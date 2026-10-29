"""Driver release policy and workflow tests, without production credentials."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import guard
import target

ROOT = Path(__file__).resolve().parents[2]


class TargetTests(unittest.TestCase):
    def git(self, directory, *args):
        return subprocess.check_output([
            'git', '-c', 'commit.gpgsign=false', '-c', 'tag.gpgSign=false',
            '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
            '-C', str(directory), *args,
        ], text=True, stderr=subprocess.PIPE).strip()

    @contextmanager
    def fixture(self, major):
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            self.git(source, 'init', '-q', '--initial-branch=scylla-' + major + '.x')
            commits = []
            for value in (major + '.1.2.3-SNAPSHOT', major + '.1.2.3', major + '.1.2.4-SNAPSHOT'):
                (source / 'pom.xml').write_text(
                    '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                    '<version>' + value + '</version></project>')
                self.git(source, 'add', '.')
                self.git(source, 'commit', '-qm', 'Version ' + value)
                commits.append(self.git(source, 'rev-parse', 'HEAD'))
                if value == major + '.1.2.3':
                    self.git(source, 'tag', value)
            self.git(root, 'clone', '-q', str(source), str(root / 'checkout'))
            os.chdir(root / 'checkout')
            try:
                yield root, source, commits
            finally:
                os.chdir(previous)

    def test_both_branch_versions_overrides_and_historical_matching(self):
        for major in ('3', '4'):
            with self.subTest(major=major), self.fixture(major) as (_, _, commits):
                branch = f'scylla-{major}.x'
                ref = 'refs/heads/' + branch
                self.assertEqual(target.resolve(major, branch, '', commits[-1], ref),
                                 (commits[-1], major + '.1.2.4'))
                self.assertEqual(target.resolve(major, branch, major + '.9.8.7', commits[-1], ref),
                                 (commits[-1], major + '.9.8.7'))
                self.assertEqual(target.resolve(major, major + '.1.2.3', '', commits[-1], ref),
                                 (commits[0], major + '.1.2.3'))
                for invalid in ('feature', 'scylla-5.x', 'v' + major + '.1.2.3', '--help',
                                major + '.1.2', '../other'):
                    with self.subTest(invalid=invalid), self.assertRaises(target.Invalid):
                        target.resolve(major, invalid, '', commits[-1], ref)
                for invalid in ('2.1.2.3', '3.1.2' if major == '3' else '4.1.2',
                                major + '.1.2.3-SNAPSHOT', major + '.1.2.3\n'):
                    with self.assertRaises(target.Invalid):
                        target.resolve(major, branch, invalid, commits[-1], ref)
                with self.assertRaises(target.Invalid):
                    target.resolve(major, branch, '', commits[-1], 'refs/heads/feature')
                with self.assertRaises(target.Invalid):
                    target.resolve(major, major + '.1.2.3', major + '.9.8.7', commits[-1], ref)
                self.git(Path.cwd(), 'tag', major + '.1.2.5', commits[-1])
                with self.assertRaises(target.Invalid):
                    target.resolve(major, major + '.1.2.5', '', commits[-1], ref)

    def test_branch_moving_during_approval_is_rejected_for_both_lines(self):
        for major in ('3', '4'):
            with self.subTest(major=major), self.fixture(major) as (_, source, commits):
                target.checkout(major, f'scylla-{major}.x', commits[-1])
                self.assertEqual(target.git('rev-parse', 'HEAD'), commits[-1])
                self.git(source, 'commit', '--allow-empty', '-qm', 'Advance')
                with self.assertRaisesRegex(target.Invalid, 'advanced'):
                    target.checkout(major, f'scylla-{major}.x', commits[-1])

    def test_historical_checkout_uses_validated_sha_even_when_tag_moves(self):
        for major in ('3', '4'):
            with self.subTest(major=major), self.fixture(major) as (_, source, commits):
                self.git(source, 'tag', '-f', major + '.1.2.3', commits[-1])
                target.checkout(major, major + '.1.2.3', commits[0])
                self.assertEqual(target.git('rev-parse', 'HEAD'), commits[0])
                self.assertEqual(target.git('tag', '--list', major + '.1.2.3'), '')

    def test_explicit_push_from_detached_checkout_has_no_upstream_dependency(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        push = next(script for script in guard.workflow_commands(workflow) if 'git push origin' in script)
        major = '3' if 'HEAD:refs/heads/scylla-3.x' in push else '4'
        with self.fixture(major) as (root, source, _):
            bare = root / 'remote.git'
            self.git(root, 'clone', '-q', '--bare', str(source), str(bare))
            self.git(Path.cwd(), 'remote', 'set-url', 'origin', str(bare))
            self.git(Path.cwd(), 'checkout', '--detach')
            self.git(Path.cwd(), 'commit', '--allow-empty', '-qm', 'Release')
            self.git(Path.cwd(), 'tag', '-a', 'release-fixture', '-m', 'Release')
            prepared = target.git('rev-parse', 'HEAD')
            result = subprocess.run(['bash', '-eo', 'pipefail', '-c', push], capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(bare, 'rev-parse', f'refs/heads/scylla-{major}.x'), prepared)
            self.assertEqual(self.git(bare, 'rev-parse', 'refs/tags/release-fixture^{}'), prepared)

    def test_placeholder_or_empty_central_credentials_fail_before_preparation(self):
        for name in ('SONATYPE_TOKEN_USERNAME', 'SONATYPE_TOKEN_PASSWORD'):
            for value in ('', 'REPLACE_ME', '  REPLACE_ME  ', ' \n'):
                with self.subTest(name=name, value=value), patch.dict(os.environ, {
                        'SONATYPE_TOKEN_USERNAME': 'synthetic-user',
                        'SONATYPE_TOKEN_PASSWORD': 'synthetic-password', name: value}, clear=True):
                    with self.assertRaisesRegex(target.Invalid, name):
                        target.preflight()

    def test_blocker_scope_and_pagination_for_both_lines(self):
        for major in ('3', '4'):
            other = '4' if major == '3' else '3'
            for lines, blocks in (([], True), ([major], True), ([other], False),
                                  ([major, other], True)):
                issue = {'html_url': 'https://example.invalid/issue',
                         'labels': [{'name': f'area/Driver_-_java-driver-{line}.x'} for line in lines]}
                payload = json.dumps([[], [issue]])
                with patch.dict(os.environ, {'GITHUB_REPOSITORY': 'test/repo'}), \
                        patch.object(subprocess, 'check_output', return_value=payload) as query:
                    if blocks:
                        with self.assertRaises(target.Invalid):
                            target.blockers(major)
                    else:
                        target.blockers(major)
                    self.assertIn('--paginate', query.call_args.args[0])

    def test_blocker_message_keeps_links_bare(self):
        urls = ['https://example.invalid/issues/1', 'https://example.invalid/issues/2']
        payload = json.dumps([[{'html_url': url, 'labels': []} for url in urls]])
        with patch.object(subprocess, 'check_output', return_value=payload):
            with self.assertRaises(target.Invalid) as raised:
                target.blockers('3')
        self.assertEqual(str(raised.exception).splitlines()[1:], urls)

    def stamp(self, readme, release):
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                Path('README.md').write_text(readme)
                with patch.dict(os.environ, {'RELEASE_VERSION': release}), patch.object(target, 'git'):
                    target.stamp_readme()
                return Path('README.md').read_text()
            finally:
                os.chdir(previous)

    def test_readme_stamps_effective_version(self):
        self.assertEqual(self.stamp('Use 3.11.5.20 and 3.11.5.20, with Java 8.\n', '3.11.6.1'),
                         'Use 3.11.6.1 and 3.11.6.1, with Java 8.\n')

    def test_readme_stamp_moves_badge_to_release_line(self):
        readme = ('[badge](https://img.shields.io/maven-central/v/x?versionPrefix=3.11.5)\n'
                  'Use 3.11.5.20; 4.x starts at 4.7.2.0.\n')
        self.assertEqual(self.stamp(readme, '3.12.1.0'),
                         '[badge](https://img.shields.io/maven-central/v/x?versionPrefix=3.12.1)\n'
                         'Use 3.12.1.0; 4.x starts at 4.7.2.0.\n')

    def test_approval_is_after_validation_and_matrix_and_has_no_shared_cache(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        validate, publish = workflow.split('\n  release:\n', 1)
        self.assertIn('needs: [validate, pre-release-integration]', publish)
        self.assertIn("needs.validate.result == 'success'", publish)
        self.assertIn("inputs.skip-integration-matrix && needs.pre-release-integration.result == 'skipped'", publish)
        self.assertNotIn('secrets.', validate)
        self.assertNotIn('environment:', validate)
        self.assertNotIn('contents: write', validate)
        self.assertIn('python3 -m unittest discover', validate)
        self.assertIn('driver_ref: ${{ needs.validate.outputs.ref }}', validate)
        self.assertIn('driver_version: ${{ needs.validate.outputs.version }}', validate)
        self.assertIn('    environment: release\n', publish)
        self.assertNotIn('cache:', publish)
        self.assertNotIn('download-artifact', publish)
        self.assertIn('ref: ${{ github.sha }}', publish)
        self.assertEqual(publish.count("RELEASE_SKIP_TESTS: 'true'"), 3)
        self.assertLess(publish.index('Preserve release guard'), publish.index('Checkout release target'))
        self.assertLess(publish.index('Check Central credentials'), publish.index('Prepare release'))
        self.assertLess(publish.index('Check release credential safety'), publish.index('secrets.'))
        self.assertNotIn('gpg-private-key:', publish)
        self.assertNotIn('signing.py" configure', publish)
        self.assertIn('verify-prepared', publish)
        self.assertLess(publish.index('Recheck for open release blockers'), publish.index('Perform release'))
        self.assertIn('inputs.dry-run == false', publish.split('Push changes to SCM')[1])
        self.assertIn("inputs.dry-run == false", publish.split('publish-docs:')[1])


if __name__ == '__main__':
    unittest.main()
