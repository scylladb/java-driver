"""Queue artifact reuse tests: no network, credentials, or live test clusters."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location(
    'reuse', Path(__file__).resolve().parents[1] / 'scripts/reuse-queue-coverage.py')
reuse = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reuse)


class Fixture:
    def __init__(self):
        self.run = dict(id=42, run_attempt=2, repository={'full_name': reuse.REPOSITORY},
                        head_repository={'full_name': reuse.REPOSITORY},
                        workflow_id=7, path=reuse.WORKFLOW, event='merge_group',
                        status='completed', conclusion='success', head_sha='a' * 40,
                        head_branch='gh-readonly-queue/scylla-4.x/pr-100-abc')
        self.candidate = dict(tree={'sha': 'b' * 40}, parents=[{'sha': 'c' * 40}])
        self.merged = copy.deepcopy(self.candidate)
        self.manifests = {}
        self.archives = {}
        self.artifacts = []
        for profile, artifact_id in [('unit', 10), ('reduced', 11)]:
            self.manifests[profile] = dict(
                repository=reuse.REPOSITORY, run_id=42, run_attempt=2,
                tested_sha=self.run['head_sha'], workflow_sha=self.run['head_sha'],
                workflow_ref=f"{reuse.REPOSITORY}/{reuse.WORKFLOW}@refs/heads/{self.run['head_branch']}",
                event='merge_group', profile=profile, jdk='11', tree='b' * 40,
                reports={'jacoco.xml': hashlib.sha256(b'<report/>').hexdigest()})
            self.artifacts.append(dict(id=artifact_id, name=f'coverage-verified-{profile}-2',
                                       expired=False, workflow_run={'id': 42}))
        self.repack()
        self.runs = [self.run]
        self.calls = []

    def repack(self, report=b'<report/>', extra=False):
        for profile, artifact in zip(['unit', 'reduced'], self.artifacts):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w') as z:
                z.writestr('jacoco.xml', report)
                z.writestr('manifest.json', json.dumps(self.manifests[profile]))
                if extra:
                    z.writestr('../untrusted', 'no')
            archive = buffer.getvalue()
            self.archives[artifact['id']] = archive
            artifact['digest'] = 'sha256:' + hashlib.sha256(archive).hexdigest()

    def get(self, path):
        self.calls.append(path)
        if path == 'actions/workflows/tests.yml':
            return dict(id=7, path=reuse.WORKFLOW)
        if path.startswith('actions/workflows/tests.yml/runs?'):
            return dict(workflow_runs=copy.deepcopy(self.runs))
        if path == 'git/commits/' + 'd' * 40:
            return self.merged
        if path == 'git/commits/' + 'a' * 40:
            return self.candidate
        if path == 'actions/runs/42':
            return copy.deepcopy(self.run)
        if path.startswith('actions/runs/42/artifacts?'):
            return dict(total_count=len(self.artifacts), artifacts=self.artifacts)
        raise AssertionError(path)

    def raw(self, path):
        self.calls.append(path)
        return self.archives[int(path.split('/')[2])]

    def find(self):
        return reuse.find_reports(self, 'd' * 40)


class CoverageReuseTests(unittest.TestCase):
    def test_matching_tree_and_parent_reuse_both_profiles(self):
        f = Fixture()
        self.assertEqual(f.find(), {'unit': b'<report/>', 'reduced': b'<report/>'})

    def test_tree_and_parent_mismatches_do_not_download_reports(self):
        for field in ['tree', 'parents']:
            f = Fixture()
            if field == 'tree':
                f.merged['tree']['sha'] = 'e' * 40
            else:
                f.merged['parents'][0]['sha'] = 'e' * 40
            self.assertIsNone(f.find())
            self.assertFalse(any('/artifacts' in call for call in f.calls))

    def test_unsuccessful_or_foreign_queue_runs_are_ignored(self):
        for field, value in [('conclusion', 'failure'), ('conclusion', 'cancelled'),
                             ('status', 'in_progress'), ('event', 'push'),
                             ('path', '.github/workflows/other.yml'), ('workflow_id', 8),
                             ('head_branch', 'scylla-4.x'), ('head_sha', 'bad'),
                             ('repository', {'full_name': 'fork/java-driver'}),
                             ('head_repository', {'full_name': 'fork/java-driver'})]:
            with self.subTest(field=field, value=value):
                f = Fixture()
                f.run[field] = value
                self.assertIsNone(f.find())

    def test_direct_push_has_no_queue_evidence(self):
        f = Fixture()
        f.runs = []
        self.assertIsNone(f.find())

    def test_missing_or_duplicate_report_is_refused(self):
        for change in ['missing', 'duplicate']:
            f = Fixture()
            if change == 'missing':
                f.artifacts.pop()
            else:
                f.artifacts.append(copy.deepcopy(f.artifacts[0]))
            with self.assertRaises(reuse.Unusable):
                f.find()

    def test_provenance_fields_are_all_checked(self):
        for field in Fixture().manifests['unit']:
            with self.subTest(field=field):
                f = Fixture()
                f.manifests['unit'][field] = 'altered'
                f.repack()
                with self.assertRaises(reuse.Unusable):
                    f.find()

    def test_report_content_and_archive_digest_are_checked(self):
        f = Fixture()
        f.repack(report=b'<altered/>')
        with self.assertRaisesRegex(reuse.Unusable, 'Report checksum'):
            f.find()
        f = Fixture()
        f.artifacts[0]['digest'] = 'sha256:altered'
        with self.assertRaisesRegex(reuse.Unusable, 'Artifact checksum'):
            f.find()
        f = Fixture()
        f.repack(extra=True)
        with self.assertRaisesRegex(reuse.Unusable, 'Unexpected report files'):
            f.find()

    def test_expired_artifact_and_wrong_run_are_refused(self):
        for field, value in [('expired', True), ('workflow_run', {'id': 99})]:
            f = Fixture()
            f.artifacts[0][field] = value
            with self.assertRaises(reuse.Unusable):
                f.find()

    def test_attempt_changes_during_verification_are_refused(self):
        f = Fixture()
        original = f.get
        reads = []
        def get(path):
            value = original(path)
            if path == 'actions/runs/42':
                reads.append(path)
                if len(reads) == 2:
                    value['run_attempt'] += 1
            return value
        f.get = get
        with self.assertRaisesRegex(reuse.Unusable, 'changed'):
            f.find()

    def test_no_evidence_retries_for_at_most_two_minutes_and_skips(self):
        elapsed = [0]
        def sleep(seconds):
            elapsed[0] += seconds
        with tempfile.TemporaryDirectory() as directory:
            env = dict(GITHUB_REPOSITORY=reuse.REPOSITORY, MERGED_SHA='d' * 40,
                       GITHUB_OUTPUT=directory + '/output', GITHUB_STEP_SUMMARY=directory + '/summary')
            with patch.dict(os.environ, env), patch.object(reuse.time, 'monotonic', side_effect=lambda: elapsed[0]), \
                    patch.object(reuse.time, 'sleep', side_effect=sleep), \
                    patch.object(reuse, 'find_reports', return_value=None) as find:
                reuse.main()
                self.assertEqual(elapsed[0], 120)
                self.assertEqual(find.call_count, 12)
            self.assertEqual(Path(env['GITHUB_OUTPUT']).read_text(), 'found=false\n')
            self.assertIn('skipping upload', Path(env['GITHUB_STEP_SUMMARY']).read_text())

    def test_each_api_command_is_bounded_by_remaining_deadline(self):
        with patch.object(reuse.time, 'monotonic', return_value=115), \
                patch.object(reuse.subprocess, 'check_output', return_value=b'{}') as command:
            reuse.GitHub(120).get('actions/runs/42')
            self.assertEqual(command.call_args.kwargs['timeout'], 5)


if __name__ == '__main__':
    unittest.main()
