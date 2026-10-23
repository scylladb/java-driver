#!/usr/bin/env python3
"""Verify the uploaded bundle before publishing, or remove a verified dry-run deployment."""

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

import signing
import target

UUID = r'[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}'
BASE_URL = 'https://central.sonatype.com/api/v1/publisher/'
NAMESPACE = {'m': 'http://maven.apache.org/POM/4.0.0'}
WAIT_SECONDS = 1800


class Invalid(ValueError):
    pass


def log_directory():
    return Path(os.environ.get('RELEASE_LOG_DIR', '/tmp/release-logs'))


def expected_modules(root):
    root = root.resolve()
    pom = ET.parse(root / 'pom.xml').getroot()
    exclusions = pom.findtext(
        ".//m:plugin[m:artifactId='central-publishing-maven-plugin']/m:configuration/m:excludeArtifacts",
        namespaces=NAMESPACE) or ''
    excluded = set(filter(None, (item.strip() for item in exclusions.split(','))))
    modules, seen = set(), set()

    def visit(directory):
        directory = directory.resolve()
        if not directory.is_relative_to(root) or directory in seen:
            raise Invalid('Unexpected reactor module path.')
        seen.add(directory)
        project = ET.parse(directory / 'pom.xml').getroot()
        artifact = project.findtext('m:artifactId', namespaces=NAMESPACE)
        if not artifact or not re.fullmatch(r'[a-z0-9][a-z0-9_.-]*', artifact):
            raise Invalid('Unexpected reactor artifact identifier.')
        if artifact not in excluded:
            modules.add(artifact)
        for module in project.findall('m:modules/m:module', NAMESPACE):
            visit(directory / module.text)

    visit(root)
    return modules


def begin():
    tag = [line.removeprefix('scm.tag=') for line in Path('release.properties').read_text().splitlines()
           if line.startswith('scm.tag=')]
    if len(tag) != 1:
        raise Invalid('Expected one prepared release version.')
    major = ET.parse('pom.xml').getroot().findtext('m:version', namespaces=NAMESPACE).split('.')[0]
    version = target.version(major, tag[0])
    if (os.environ.get('RELEASE_VERSION') or version) != version:
        raise Invalid('Prepared release version does not match validation.')
    directory = log_directory()
    directory.mkdir(parents=True, exist_ok=True)
    offsets = {name: (directory / (name + '.log')).stat().st_size
               if (directory / (name + '.log')).exists() else 0 for name in ('stdout', 'stderr')}
    (directory / 'publication-state.json').write_text(json.dumps({
        'version': version, 'modules': sorted(expected_modules(Path.cwd())), 'offsets': offsets,
    }))


def deployment_id(state):
    identifiers = set()
    for name, offset in state['offsets'].items():
        with (log_directory() / (name + '.log')).open('rb') as stream:
            stream.seek(offset)
            text = stream.read().decode('utf-8', errors='replace')
        identifiers.update(re.findall(r'deploymentId\s*[:=]\s*(' + UUID + ')', text, re.I))
    if len(identifiers) != 1:
        raise Invalid('Expected exactly one deployment ID in this Maven invocation; inspect sanitized logs.')
    return identifiers.pop().lower()


def verify_bundle(path, version, expected):
    modules, count = set(), 0
    with signing.session_signals() as interrupted, zipfile.ZipFile(path) as bundle, \
            signing.public_environment() as env, \
            tempfile.TemporaryDirectory(prefix='release-bundle-') as directory:
        names = bundle.namelist()
        if len(names) != len(set(names)):
            raise Invalid('Duplicate bundle entries.')
        payloads = {name for name in names if not name.endswith(('/', '.asc', '.md5', '.sha1', '.sha256', '.sha512'))}
        if not payloads or {name[:-4] for name in names if name.endswith('.asc')} != payloads:
            raise Invalid('Every bundle artifact must have exactly one detached signature.')
        for name in sorted(payloads):
            interrupted.check()
            parts = name.split('/')
            if (len(parts) != 5 or parts[:2] != ['com', 'scylladb'] or parts[3] != version
                    or parts[2] not in expected
                    or not parts[4].startswith((parts[2] + '-' + version + '.', parts[2] + '-' + version + '-'))
                    or not name.endswith(('.pom', '.jar'))):
                raise Invalid('Unexpected bundle coordinates, version, or module.')
            modules.add(parts[2])
            artifact = Path(directory) / 'artifact'
            signature = Path(directory) / 'artifact.asc'
            artifact.write_bytes(bundle.read(name))
            signature.write_bytes(bundle.read(name + '.asc'))
            signing.verify_status(signing.gpg('--status-fd', '1', '--verify', signature, artifact, env=env))
            count += 1
            interrupted.check()
        if modules != set(expected):
            raise Invalid('The Central bundle does not contain the expected reactor modules.')
        # Each component must include a signed POM, even if its packaging is pom.
        for module in modules:
            if f'com/scylladb/{module}/{version}/{module}-{version}.pom' not in payloads:
                raise Invalid('A bundle component is missing its POM.')
    return count


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, url):
        raise Invalid('Central redirected a credential-bearing request.')


def api(method, endpoint):
    target.preflight()
    credentials = (os.environ['SONATYPE_TOKEN_USERNAME'] + ':' +
                   os.environ['SONATYPE_TOKEN_PASSWORD']).encode()
    request = urllib.request.Request(BASE_URL + endpoint, method=method, data=b'' if method == 'POST' else None,
                                     headers={'Authorization': 'Bearer ' + base64.b64encode(credentials).decode()})
    with urllib.request.build_opener(NoRedirect).open(request, timeout=60) as response:
        data = response.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            raise Invalid('Central status response exceeded the limit.')
        return json.loads(data) if data else None


def wait_for(identifier, wanted):
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        status = api('POST', 'status?id=' + identifier)
        if not isinstance(status, dict) or status.get('deploymentId') != identifier:
            raise Invalid('Central returned an unexpected deployment identity.')
        state = status.get('deploymentState')
        if state == wanted:
            return status
        if state == 'FAILED' or state not in ('PENDING', 'VALIDATING', 'VALIDATED', 'PUBLISHING', 'PUBLISHED'):
            raise Invalid('Central deployment did not reach the required state; inspect Portal.')
        if wanted == 'VALIDATED' and state in ('PUBLISHING', 'PUBLISHED'):
            raise Invalid('Expected an unpublished deployment; refusing further changes.')
        if time.monotonic() >= deadline:
            raise Invalid('Timed out waiting for Central; inspect the deployment before retrying.')
        time.sleep(5)


def finish(mode):
    if mode not in ('publish', 'dry-run'):
        raise Invalid('Unsupported publication mode.')
    state = json.loads((log_directory() / 'publication-state.json').read_text())
    identifier = deployment_id(state)
    paths = list(Path('target/checkout').glob('**/target/central-publishing/central-bundle.zip'))
    if len(paths) != 1:
        raise Invalid('Expected exactly one Central bundle in the Maven release checkout.')
    count = verify_bundle(paths[0], state['version'], set(state['modules']))
    status = wait_for(identifier, 'VALIDATED')
    expected_purls = {f'pkg:maven/com.scylladb/{module}@{state["version"]}' for module in state['modules']}
    if status.get('purls') and set(status['purls']) != expected_purls:
        raise Invalid('Central components do not match the verified bundle.')
    if mode == 'publish':
        api('POST', 'deployment/' + identifier)
        wait_for(identifier, 'PUBLISHED')
        outcome = 'published'
    elif mode == 'dry-run':
        api('DELETE', 'deployment/' + identifier)
        outcome = 'deleted unpublished dry-run deployment'
    else:
        raise Invalid('Unsupported publication mode.')
    report = {**{key: state[key] for key in ('version', 'modules')}, 'deployment_id': identifier,
              'outcome': outcome, 'signed_files': count, 'primary_fingerprint': signing.PRIMARY,
              'signing_fingerprint': signing.SIGNING,
              'bundle_sha256': hashlib.sha256(paths[0].read_bytes()).hexdigest()}
    (log_directory() / 'publication-report.log').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


def main():
    try:
        if sys.argv[1:] == ['begin']:
            begin()
        elif sys.argv[1:] in (['publish'], ['dry-run']):
            finish(sys.argv[1])
        else:
            raise Invalid('Expected begin, publish, or dry-run.')
        return 0
    except (Invalid, signing.SigningError, target.Invalid) as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print('Central verification or operation failed; values withheld. Inspect sanitized logs and Portal.', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
