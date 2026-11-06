"""Reuse immutable queue coverage only for the exact tree merged on scylla-4.x."""
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import time
import zipfile

REPOSITORY = 'scylladb/java-driver'
WORKFLOW = '.github/workflows/tests.yml'
SHA = re.compile(r'^[0-9a-f]{40}$')


class Unusable(ValueError):
    """A candidate is not trustworthy or does not cover the merged tree."""


class GitHub:
    def __init__(self, deadline):
        self.deadline = deadline

    def raw(self, path):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Coverage lookup deadline reached')
        return subprocess.check_output(
            ['gh', 'api', f'repos/{REPOSITORY}/{path}'],
            timeout=min(10, remaining), stderr=subprocess.PIPE)

    def get(self, path):
        return json.loads(self.raw(path))


def eligible(run, workflow_id):
    return (run.get('repository', {}).get('full_name') == REPOSITORY
            and run.get('head_repository', {}).get('full_name') == REPOSITORY
            and run.get('event') == 'merge_group'
            and run.get('path') == WORKFLOW
            and run.get('workflow_id') == workflow_id
            and run.get('status') == 'completed'
            and run.get('conclusion') == 'success'
            and run.get('head_branch', '').startswith('gh-readonly-queue/scylla-4.x/')
            and SHA.fullmatch(run.get('head_sha', '')) is not None)


def verify_archive(archive, artifact, run, candidate, profile):
    """Validate archive digest, provenance and every report before returning bytes."""
    digest = 'sha256:' + hashlib.sha256(archive).hexdigest()
    if artifact.get('digest') != digest or artifact.get('expired'):
        raise Unusable('Artifact checksum mismatch or expired artifact')
    if artifact.get('workflow_run', {}).get('id') != run['id']:
        raise Unusable('Artifact belongs to another run')
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        if sorted(z.namelist()) != ['jacoco.xml', 'manifest.json']:
            raise Unusable('Unexpected report files')
        if sum(i.file_size for i in z.infolist()) > 128 * 1024 * 1024:
            raise Unusable('Report archive too large')
        manifest = json.loads(z.read('manifest.json'))
        expected = dict(repository=REPOSITORY, run_id=run['id'],
                        run_attempt=run['run_attempt'], tested_sha=run['head_sha'],
                        workflow_sha=run['head_sha'], event='merge_group',
                        profile=profile, jdk='11', tree=candidate['tree']['sha'])
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise Unusable('Report provenance mismatch')
        ref = f"{REPOSITORY}/{WORKFLOW}@refs/heads/{run['head_branch']}"
        if manifest.get('workflow_ref') != ref:
            raise Unusable('Report workflow ref mismatch')
        report = z.read('jacoco.xml')
        if manifest.get('reports') != {'jacoco.xml': hashlib.sha256(report).hexdigest()}:
            raise Unusable('Report checksum mismatch')
        return report


def find_reports(api, merged_sha):
    workflow = api.get('actions/workflows/tests.yml')
    if workflow.get('path') != WORKFLOW:
        raise Unusable('Unexpected coverage workflow')
    merged = api.get(f'git/commits/{merged_sha}')
    runs = api.get('actions/workflows/tests.yml/runs?event=merge_group&per_page=100')['workflow_runs']
    for summary in runs:
        if not eligible(summary, workflow['id']):
            continue
        run = api.get(f"actions/runs/{summary['id']}")
        if not eligible(run, workflow['id']):
            continue
        candidate = api.get(f"git/commits/{run['head_sha']}")
        # A squash merge replaces the queue commit's identity, but not its tree
        # or first parent. Checking both also excludes an old unrelated queue run.
        if (candidate['tree']['sha'] != merged['tree']['sha']
                or not candidate.get('parents') or not merged.get('parents')
                or candidate['parents'][0]['sha'] != merged['parents'][0]['sha']):
            continue
        artifacts = api.get(f"actions/runs/{run['id']}/artifacts?per_page=100")
        if artifacts['total_count'] > 100:
            raise Unusable('Unexpectedly many queue artifacts')
        reports = {}
        for profile in ['unit', 'reduced']:
            name = f"coverage-verified-{profile}-{run['run_attempt']}"
            matches = [a for a in artifacts['artifacts'] if a['name'] == name]
            if len(matches) != 1:
                raise Unusable(f'Missing or ambiguous {profile} report')
            artifact = matches[0]
            archive = api.raw(f"actions/artifacts/{artifact['id']}/zip")
            reports[profile] = verify_archive(archive, artifact, run, candidate, profile)
        # A rerun must not race provenance verification.
        current = api.get(f"actions/runs/{run['id']}")
        if not eligible(current, workflow['id']) or current['run_attempt'] != run['run_attempt']:
            raise Unusable('Queue run changed during verification')
        return reports
    return None


def main():
    if os.environ['GITHUB_REPOSITORY'] != REPOSITORY:
        raise SystemExit('Coverage reuse is restricted to the upstream repository')
    merged_sha = os.environ['MERGED_SHA']
    if not SHA.fullmatch(merged_sha):
        raise SystemExit('Invalid merged SHA')
    deadline = time.monotonic() + 120
    api = GitHub(deadline)
    reports = None
    while time.monotonic() < deadline:
        try:
            reports = find_reports(api, merged_sha)
            if reports:
                break
        except (Unusable, subprocess.SubprocessError, TimeoutError, ValueError,
                KeyError, zipfile.BadZipFile) as error:
            print(f'Coverage evidence unavailable: {error}')
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(10, remaining))
    if reports:
        dest = Path('queue-coverage')
        dest.mkdir(exist_ok=True)
        for profile, data in reports.items():
            (dest / f'{profile}.xml').write_bytes(data)
        message = f'Verified queue unit and reduced coverage for merged commit {merged_sha}.'
    else:
        message = ('No matching successful queue coverage within two minutes; '
                   'skipping upload. Direct and bypass pushes do not rerun tests.')
    print(message)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        print(f'found={str(bool(reports)).lower()}', file=output)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        print(message, file=summary)


if __name__ == '__main__':
    main()
