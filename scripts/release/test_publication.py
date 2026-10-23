"""Verify bundle contents and fail closed before any public Central operation."""

import base64
from contextlib import redirect_stderr
from io import StringIO
import json
import os
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import zipfile

import guard
import publication
import signing
import test_signing as signing_fixtures

VERSION = '4.1.2.3'
IDENTIFIER = '01234567-89ab-cdef-0123-456789abcdef'
PREFIX = f'com/scylladb/driver-core/{VERSION}/driver-core-{VERSION}'


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        signing_fixtures.SigningTests.setUpClass()
        cls.signer = signing_fixtures.SigningTests()

    @classmethod
    def tearDownClass(cls):
        signing_fixtures.SigningTests.tearDownClass()

    def bundle(self, directory):
        archive = Path('target/checkout/target/central-publishing/central-bundle.zip')
        archive.parent.mkdir(parents=True)
        files = [Path('component.pom'), Path('component.jar')]
        for file in files:
            file.write_bytes(b'fixture artifact')
        with self.signer.session() as env:
            signing.sign_files(files, env)
        entries = {}
        for file in files:
            name = PREFIX + file.suffix
            entries[name] = file.read_bytes()
            entries[name + '.asc'] = Path(str(file) + '.asc').read_bytes()
        self.write_archive(archive, entries)
        logs = directory / 'logs'
        logs.mkdir()
        os.environ['RELEASE_LOG_DIR'] = str(logs)
        Path('pom.xml').write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<artifactId>driver-core</artifactId><version>4.1.2.4-SNAPSHOT</version></project>')
        Path('release.properties').write_text('scm.tag=' + VERSION + '\n')
        (logs / 'stdout.log').write_text('deploymentId: ffffffff-ffff-ffff-ffff-ffffffffffff\n')
        publication.begin()
        with (logs / 'stdout.log').open('a') as stream:
            stream.write('Uploaded bundle successfully, deploymentId: ' + IDENTIFIER + '\n')
        (logs / 'stderr.log').write_text('')
        return archive, entries, logs

    def write_archive(self, archive, entries):
        with zipfile.ZipFile(archive, 'w') as output:
            for name, content in entries.items():
                output.writestr(name, content)

    def test_all_signatures_and_exact_version_and_modules_are_required(self):
        with self.signer.repository() as directory:
            archive, entries, _ = self.bundle(directory)
            self.assertEqual(publication.verify_bundle(archive, VERSION, {'driver-core'}), 2)
            invalid_cases = [
                {name: content for name, content in entries.items() if name != PREFIX + '.jar.asc'},
                {**entries, PREFIX + '.jar': b'tampered'},
                {name.replace(VERSION + '.jar', VERSION + '0.jar'): content for name, content in entries.items()},
                {name.replace('com/scylladb/', '../scylladb/'): content for name, content in entries.items()},
            ]
            for invalid in invalid_cases:
                self.write_archive(archive, invalid)
                with self.assertRaises((publication.Invalid, signing.SigningError)):
                    publication.verify_bundle(archive, VERSION, {'driver-core'})
            self.write_archive(archive, entries)
            for version, expected in [('4.1.2.4', {'driver-core'}), (VERSION, {'foreign'}),
                                      (VERSION, {'driver-core', 'missing'})]:
                with self.assertRaises(publication.Invalid):
                    publication.verify_bundle(archive, version, expected)
            with patch.object(signing, 'SIGNING', 'A' * 40), self.assertRaises(signing.SigningError):
                publication.verify_bundle(archive, VERSION, {'driver-core'})

    def test_real_publication_waits_for_published_after_bundle_verification(self):
        with self.signer.repository() as directory:
            _, _, logs = self.bundle(directory)
            statuses = [self.status('VALIDATED'), None, self.status('PUBLISHING'), self.status('PUBLISHED')]
            with patch.object(publication, 'api', side_effect=statuses) as api, \
                    patch.object(publication.target, 'blockers'), patch.object(publication.time, 'sleep'):
                publication.finish('publish')
            self.assertEqual([call.args for call in api.call_args_list], [
                ('POST', 'status?id=' + IDENTIFIER), ('POST', 'deployment/' + IDENTIFIER),
                ('POST', 'status?id=' + IDENTIFIER), ('POST', 'status?id=' + IDENTIFIER),
            ])
            report = json.loads((logs / 'publication-report.log').read_text())
            self.assertEqual(report['outcome'], 'published')
            self.assertEqual(report['signed_files'], 2)

    def test_blockers_and_moved_branch_are_rechecked_after_upload_before_publication(self):
        with self.signer.repository() as directory:
            self.bundle(directory)
            with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true', 'RELEASE_TARGET_TAG': 'scylla-4.x',
                                         'RELEASE_BASE_COMMIT': 'a' * 40}):
                for gate in ('blockers', 'check_branch'):
                    with patch.object(publication, 'api', return_value=self.status('VALIDATED')) as api, \
                            patch.object(publication.target, 'blockers') as blockers, \
                            patch.object(publication.target, 'check_branch') as branch:
                        (blockers if gate == 'blockers' else branch).side_effect = publication.target.Invalid('Gate closed')
                        with self.assertRaises(publication.target.Invalid):
                            publication.finish('publish')
                        self.assertEqual(len(api.call_args_list), 1)
                        blockers.assert_called_once_with('4')
                        if gate == 'check_branch':
                            branch.assert_called_once_with('4', 'a' * 40)

    def test_dry_run_deletes_only_its_validated_unpublished_deployment(self):
        with self.signer.repository() as directory:
            _, _, logs = self.bundle(directory)
            status = self.status('VALIDATED')
            status['purls'] = [status['purls'][0] + '?type=jar', status['purls'][0] + '?type=pom']
            with patch.object(publication, 'api', side_effect=[status, None]) as api:
                publication.finish('dry-run')
            self.assertEqual(api.call_args_list[-1].args, ('DELETE', 'deployment/' + IDENTIFIER))
            self.assertFalse(any(call.args == ('POST', 'deployment/' + IDENTIFIER) for call in api.call_args_list))
            self.assertIn('deleted', json.loads((logs / 'publication-report.log').read_text())['outcome'])

    def test_unsigned_bundle_wrong_components_or_published_state_never_trigger_changes(self):
        with self.signer.repository() as directory:
            archive, entries, _ = self.bundle(directory)
            self.write_archive(archive, {name: value for name, value in entries.items() if not name.endswith('.asc')})
            with patch.object(publication, 'api') as api, self.assertRaises(publication.Invalid):
                publication.finish('publish')
            api.assert_not_called()
            self.write_archive(archive, entries)
            for status in [self.status('PUBLISHED'), self.status('FAILED'),
                           {**self.status('VALIDATED'), 'purls': ['pkg:maven/foreign/module@1.0']}]:
                with patch.object(publication, 'api', return_value=status) as api, self.assertRaises(publication.Invalid):
                    publication.finish('dry-run')
                self.assertEqual(len(api.call_args_list), 1)

    def status(self, state):
        return {'deploymentId': IDENTIFIER, 'deploymentState': state,
                'purls': [f'pkg:maven/com.scylladb/driver-core@{VERSION}']}

    def test_recovery_bundle_retains_the_exact_signed_commits_and_tag(self):
        with self.signer.repository() as directory:
            baseline = signing.run(['git', 'rev-parse', 'HEAD']).strip()
            recovered = directory / 'recovery'
            signing.run(['git', 'clone', '--quiet', '.', str(recovered)])
            with self.signer.session():
                signing.run(['git', 'commit', '--allow-empty', '-qm', 'Release'])
                signing.run(['git', 'tag', '-s', '-m', 'Release', VERSION])
                signing.run(['git', 'commit', '--allow-empty', '-qm', 'Development'])
            expected = signing.run(['git', 'rev-parse', 'HEAD']).strip()
            workflow = (Path(__file__).resolve().parents[2] / '.github/workflows/release.yml').read_text()
            script = next(command for command in guard.workflow_commands(workflow) if 'git bundle create' in command)
            subprocess.run(['bash', '-eo', 'pipefail', '-c', script], check=True, capture_output=True,
                           env={**os.environ, 'RELEASE_VERSION': VERSION, 'RELEASE_BASE_COMMIT': baseline})
            bundle = directory / 'prepared-release.bundle'
            signing.run(['git', '-C', str(recovered), 'bundle', 'verify', str(bundle)])
            signing.run(['git', '-C', str(recovered), 'fetch', str(bundle),
                         'HEAD:refs/heads/recovered', f'refs/tags/{VERSION}:refs/tags/{VERSION}'])
            self.assertEqual(signing.run(['git', '-C', str(recovered), 'rev-parse', 'recovered']).strip(), expected)
            with signing.public_environment() as env:
                for kind, ref in [('commit', 'recovered'), ('tag', VERSION)]:
                    signing.verify_status(signing.run(['git', '-C', str(recovered), '-c', 'gpg.program=gpg',
                                                      'verify-' + kind, '--raw', ref], env))

    def test_api_keeps_credentials_out_of_urls_and_refuses_redirects(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{}'
        opener = MagicMock()
        opener.open.return_value = response
        with patch.dict(os.environ, {'SONATYPE_TOKEN_USERNAME': 'test-user',
                                     'SONATYPE_TOKEN_PASSWORD': 'test-value'}, clear=True), \
                patch.object(publication.urllib.request, 'build_opener', return_value=opener):
            publication.api('POST', 'status?id=' + IDENTIFIER)
        request = opener.open.call_args.args[0]
        self.assertEqual(request.full_url, publication.BASE_URL + 'status?id=' + IDENTIFIER)
        self.assertEqual(request.get_header('Authorization'),
                         'Bearer ' + base64.b64encode(b'test-user:test-value').decode())
        with self.assertRaises(publication.Invalid):
            publication.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://foreign.invalid/')

    def test_unexpected_errors_withhold_values(self):
        output = StringIO()
        with patch.object(publication, 'finish', side_effect=OSError('synthetic-sensitive-value')), \
                patch('sys.argv', ['publication.py', 'publish']), redirect_stderr(output):
            self.assertEqual(publication.main(), 1)
        self.assertNotIn('synthetic-sensitive-value', output.getvalue())

    def test_reactor_exclusions_and_literal_auto_publication_guard(self):
        root = Path(__file__).resolve().parents[2]
        modules = publication.expected_modules(root)
        self.assertTrue(modules)
        self.assertFalse(any('distribution' in name or name.endswith('-dist') for name in modules))
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            (fixture / 'pom.xml').write_text((root / 'pom.xml').read_text().replace(
                '<autoPublish>${release.autopublish}</autoPublish>', '<autoPublish>true</autoPublish>'))
            with self.assertRaisesRegex(guard.Unsafe, 'unpublished'):
                guard.check_policy(fixture)


if __name__ == '__main__':
    unittest.main()
