"""Exercise GitHub publication with a disposable Git remote and a fake GitHub CLI."""

from contextlib import contextmanager, redirect_stderr
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import github_release
import guard
import signing

ROOT = Path(__file__).resolve().parents[2]


class GitHubReleaseTests(unittest.TestCase):
    @contextmanager
    def fixture(self, major='4'):
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'gh'
            binary.write_text(
                f'#!{sys.executable}\n'
                'import json, os, sys\n'
                'from pathlib import Path\n'
                'args = sys.argv[1:]\n'
                'with open(os.environ["TRACE"], "a") as trace:\n'
                ' trace.write(json.dumps(args) + "\\n")\n'
                'if os.environ.get("CLI_FAIL"):\n'
                ' print(os.environ["GH_TOKEN"], file=sys.stderr)\n'
                ' raise SystemExit(1)\n'
                'state = Path(os.environ["STATE"])\n'
                'if args[0] == "api":\n'
                ' if not state.exists():\n'
                '  print("HTTP/2.0 404 Not Found\\nContent-Type: application/json\\n")\n'
                '  print(json.dumps({"message": "Not Found"}))\n'
                '  print("gh: Not Found (HTTP 404)", file=sys.stderr)\n'
                '  raise SystemExit(1)\n'
                ' print("HTTP/2.0 200 OK\\nContent-Type: application/json\\n")\n'
                ' print(state.read_text())\n'
                'elif args[:2] == ["release", "create"]:\n'
                ' if "--notes-from-tag" in args and "--repo" in args:\n'
                '  raise SystemExit(1)\n'
                ' state.write_text(json.dumps({"tag_name": args[2], "draft": False,\n'
                '  "immutable": os.environ.get("MUTABLE") != "true"}))\n'
                'else:\n'
                ' raise SystemExit(2)\n'
            )
            binary.chmod(0o700)
            repo = root / 'source'
            repo.mkdir()
            def git(*args):
                return subprocess.check_output([
                    'git', '-c', 'commit.gpgsign=false', '-c', 'tag.gpgSign=false',
                    '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', *args,
                ], cwd=repo, text=True, stderr=subprocess.PIPE).strip()
            git('init', '-q')
            git('commit', '--allow-empty', '-qm', 'Previous release')
            git('tag', '-a', major + '.1.2.2', '-m', 'Previous release')
            git('commit', '--allow-empty', '-qm', 'Upstream release')
            git('tag', '-a', major + '.1.3', '-m', 'Upstream three-part tag')
            git('commit', '--allow-empty', '-qm', 'Fixture')
            version = major + '.1.2.3'
            git('tag', '-a', version, '-m', 'Prepared release')
            remote = root / 'remote.git'
            git('clone', '-q', '--bare', str(repo), str(remote))
            git('remote', 'add', 'origin', str(remote))
            with patch.dict(os.environ, {
                    'PATH': str(root) + os.pathsep + os.environ['PATH'],
                    'RELEASE_MAJOR': major, 'RELEASE_VERSION': version,
                    'RELEASE_TARGET_TAG': f'scylla-{major}.x', 'RELEASE_DRY_RUN': 'false',
                    'GITHUB_REPOSITORY': 'scylladb/java-driver',
                    'GH_TOKEN': 'test-only-github-token', 'STATE': str(root / 'state.json'),
                    'TRACE': str(root / 'trace.jsonl'),
                    'CLI_FAIL': '', 'MUTABLE': '',
            }), patch.object(signing, 'verify_release') as verify:
                os.chdir(repo)
                try:
                    yield root, git, verify
                finally:
                    os.chdir(previous)

    def calls(self, root):
        path = root / 'trace.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_both_lines_publish_once_and_accept_matching_immutable_release_on_recovery(self):
        for major in ('3', '4'):
            with self.subTest(major=major), self.fixture(major) as (root, _, verify):
                github_release.publish()
                github_release.publish()
                self.assertEqual(verify.call_count, 2)
                verify.assert_called_with(major + '.1.2.3')
                creates = [call for call in self.calls(root) if call[:2] == ['release', 'create']]
                self.assertEqual(len(creates), 1)
                self.assertIn('--verify-tag', creates[0])
                self.assertIn('--generate-notes', creates[0])
                self.assertEqual(creates[0][creates[0].index('--notes-start-tag') + 1], major + '.1.2.2')
                self.assertNotIn('--title', creates[0])
                self.assertIn('github.com/scylladb/java-driver', creates[0])
                self.assertIn('--latest' if major == '4' else '--latest=false', creates[0])
                self.assertNotIn('--draft', creates[0])

    def test_unsigned_or_foreign_preparation_stops_before_any_github_request(self):
        with self.fixture() as (root, _, verify):
            verify.side_effect = signing.SigningError('Invalid signature')
            with self.assertRaises(signing.SigningError):
                github_release.publish()
            self.assertEqual(self.calls(root), [])

    def test_missing_remote_tag_stops_before_github_request(self):
        with self.fixture() as (root, git, _):
            git('push', '-q', 'origin', ':refs/tags/4.1.2.3')
            with self.assertRaises(subprocess.CalledProcessError):
                github_release.publish()
            self.assertEqual(self.calls(root), [])

    def test_different_tag_object_on_same_commit_stops_before_github_request(self):
        with self.fixture() as (root, git, _):
            # Matching peeled commits are insufficient: require the signed tag object.
            git('tag', '-fa', '4.1.2.3', '-m', 'Different annotation')
            with self.assertRaisesRegex(github_release.Invalid, 'Remote release tag differs'):
                github_release.publish()
            self.assertEqual(self.calls(root), [])

    def test_remote_tag_moving_during_publication_is_rejected(self):
        with self.fixture() as (root, git, _):
            original = github_release.gh
            def move_tag(*args, **kwargs):
                result = original(*args, **kwargs)
                if args[:2] == ('release', 'create'):
                    git('--git-dir=' + str(root / 'remote.git'), 'tag', '-fa',
                        '4.1.2.3', '-m', 'Unexpected remote replacement')
                return result
            with patch.object(github_release, 'gh', side_effect=move_tag):
                with self.assertRaisesRegex(github_release.Invalid, 'Remote release tag differs'):
                    github_release.publish()

    def test_existing_mutable_draft_or_wrong_version_is_never_modified(self):
        for extra in ({'immutable': False}, {'draft': True}, {'tag_name': '4.9.9.9'}):
            with self.subTest(extra=extra), self.fixture() as (root, _, _):
                value = {'tag_name': '4.1.2.3', 'draft': False, 'immutable': True, **extra}
                (root / 'state.json').write_text(json.dumps(value))
                with self.assertRaisesRegex(github_release.Invalid, 'published and immutable'):
                    github_release.publish()
                self.assertTrue(all(call[0] == 'api' for call in self.calls(root)))

    def test_new_mutable_release_fails_without_deleting_or_overwriting_it(self):
        with self.fixture() as (root, _, _), patch.dict(os.environ, {'MUTABLE': 'true'}):
            with self.assertRaisesRegex(github_release.Invalid, 'published and immutable'):
                github_release.publish()
            self.assertEqual([call[:2] for call in self.calls(root)],
                             [['api', '--hostname'], ['release', 'create'], ['api', '--hostname']])

    def test_dry_runs_historical_targets_wrong_lines_and_forks_cannot_publish(self):
        for extra in ({'RELEASE_DRY_RUN': 'true'}, {'RELEASE_DRY_RUN': ''},
                      {'RELEASE_TARGET_TAG': '4.1.2.3'}, {'RELEASE_MAJOR': '3'},
                      {'RELEASE_MAJOR': '5'}, {'RELEASE_VERSION': '3.1.2.3'},
                      {'GITHUB_REPOSITORY': 'someone/java-driver'}):
            with self.subTest(extra=extra), self.fixture() as (root, _, verify), \
                    patch.dict(os.environ, extra):
                with self.assertRaises((github_release.Invalid, github_release.target.Invalid)):
                    github_release.publish()
                verify.assert_not_called()
                self.assertEqual(self.calls(root), [])

    def test_api_failure_does_not_create_release_or_print_credentials(self):
        with self.fixture() as (root, _, _), patch.dict(os.environ, {'CLI_FAIL': 'true'}):
            errors = StringIO()
            with redirect_stderr(errors):
                self.assertEqual(github_release.main(), 1)
            self.assertNotIn(os.environ['GH_TOKEN'], errors.getvalue())
            self.assertEqual(len(self.calls(root)), 1)
            self.assertEqual(self.calls(root)[0][0], 'api')

    def test_only_an_explicit_http_404_is_absence(self):
        for output in ('', '{"status":"404"}',
                       'HTTP/2.0 403 Forbidden\n\n{"status":"404"}',
                       'HTTP/2.0 429 Too Many Requests\n\n{}',
                       'HTTP/2.0 500 Internal Server Error\n\n{}'):
            with self.subTest(output=output), patch.object(
                    subprocess, 'run', return_value=subprocess.CompletedProcess(
                        [], 1, stdout=output, stderr='test-only-token')):
                with self.assertRaises(github_release.Invalid):
                    github_release.release('scylladb/java-driver', '4.1.2.3')

    def test_creation_failure_stops_without_printing_credentials(self):
        with self.fixture() as (root, _, _):
            original = github_release.gh
            def fail_create(*args, **kwargs):
                if args[:2] == ('release', 'create'):
                    with patch.dict(os.environ, {'CLI_FAIL': 'true'}):
                        return original(*args, **kwargs)
                return original(*args, **kwargs)
            errors = StringIO()
            with patch.object(github_release, 'gh', side_effect=fail_create), redirect_stderr(errors):
                self.assertEqual(github_release.main(), 1)
            self.assertNotIn(os.environ['GH_TOKEN'], errors.getvalue())
            self.assertFalse((root / 'state.json').exists())
            self.assertEqual(len(self.calls(root)), 2)

    def test_workflow_publishes_after_push_inside_protected_release_job_only(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        validation, publishing = workflow.split('\n  release:\n', 1)
        publishing, docs = publishing.split('\n  publish-docs:\n', 1)
        self.assertNotIn('github_release.py', validation)
        self.assertIn('environment: release', publishing)
        major = '3' if "RELEASE_MAJOR: '3'" in workflow else '4'
        step = publishing.split('- name: Publish immutable GitHub Release', 1)[1].split(
            '\n      - name:', 1)[0]
        self.assertIn(f"inputs.dry-run == false && inputs.target-tag == 'scylla-{major}.x'", step)
        self.assertIn('GH_TOKEN: ${{ github.token }}', step)
        self.assertIn('RELEASE_DRY_RUN: ${{ inputs.dry-run }}', step)
        self.assertIn('python3 "$RUNNER_TEMP/release-safety/github_release.py"', step)
        self.assertLess(publishing.index('HEAD:refs/heads/'), publishing.index('github_release.py'))
        self.assertIn("needs.release.result == 'success'", docs)
        guard.check_policy(ROOT)


if __name__ == '__main__':
    unittest.main()
