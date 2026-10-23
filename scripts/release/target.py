#!/usr/bin/env python3
"""Resolve immutable driver release inputs and enforce the two release-line policies."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


class Invalid(ValueError):
    pass


def git(*args):
    return subprocess.check_output(['git', *args], text=True, stderr=subprocess.PIPE).strip()


def version(major, value):
    if not re.fullmatch(rf'{major}\.[0-9]+\.[0-9]+\.[0-9]+', value):
        raise Invalid('Expected a four-component release version for this driver line.')
    return value


def pom_version(commit):
    pom = ET.fromstring(git('show', commit + ':pom.xml'))
    return pom.findtext('{http://maven.apache.org/POM/4.0.0}version') or ''


def resolve(major, target, override, launch_sha, launch_ref):
    branch = f'scylla-{major}.x'
    if launch_ref != 'refs/heads/' + branch:
        raise Invalid('Dispatch this workflow from its release branch.')
    if target == branch:
        commit = git('rev-parse', launch_sha + '^{commit}')
    else:
        version(major, target)
        commit = git('rev-parse', 'refs/tags/' + target + '^1')
    effective = version(major, override or pom_version(commit).removesuffix('-SNAPSHOT'))
    if target != branch and (effective != target or
                             pom_version(commit).removesuffix('-SNAPSHOT') != target or
                             pom_version('refs/tags/' + target).removesuffix('-SNAPSHOT') != target):
        raise Invalid('Historical target and preparation POM must match the tag version.')
    return commit, effective


def checkout(major, target, commit):
    if not re.fullmatch('[0-9a-f]{40}', commit):
        raise Invalid('Invalid validated commit.')
    branch = f'scylla-{major}.x'
    if target != branch:
        version(major, target)
    git('fetch', '--no-tags', 'origin', commit)
    if target == branch:
        git('fetch', '--no-tags', 'origin', 'refs/heads/' + branch)
        if git('rev-parse', 'FETCH_HEAD') != commit:
            raise Invalid('Release branch advanced after validation; dispatch again.')
        git('checkout', '-B', branch, commit)
    else:
        git('checkout', '--detach', commit)
        # release:prepare creates this tag again, locally only for historical targets.
        if git('tag', '--list', target):
            git('tag', '-d', target)


def preflight():
    override = os.environ.get('RELEASE_VERSION', '')
    if override:
        current = ET.parse('pom.xml').getroot().findtext(
            '{http://maven.apache.org/POM/4.0.0}version') or ''
        version(current.split('.')[0], override)
    for name in ('SONATYPE_TOKEN_USERNAME', 'SONATYPE_TOKEN_PASSWORD'):
        value = os.environ.get(name, '')
        if not value.strip() or value.strip() == 'REPLACE_ME':
            raise Invalid(name + ' must contain a fresh Central Portal token, not a placeholder.')


def blockers(major):
    # Paginate: a relevant blocker beyond the first 100 must still stop publication.
    payload = subprocess.check_output([
        'gh', 'api', '--paginate', '--slurp',
        f'repos/{os.environ["GITHUB_REPOSITORY"]}/issues?state=open&labels=release-blocker&per_page=100',
    ], text=True)
    other = '3' if major == '4' else '4'
    matches = []
    for page in json.loads(payload):
        for issue in page:
            if 'pull_request' in issue:
                continue
            labels = {item['name'] for item in issue['labels']}
            if (f'area/Driver_-_java-driver-{other}.x' not in labels or
                    f'area/Driver_-_java-driver-{major}.x' in labels):
                matches.append(issue['html_url'])
    if matches:
        raise Invalid('Release blocked by open issues: ' + ', '.join(matches))


def stamp_readme():
    release = version('3', os.environ['RELEASE_VERSION'])
    path = Path('README.md')
    text, count = re.subn(r'(?<![\d.])3\.[0-9]+\.[0-9]+\.[0-9]+(?![\d.])', release, path.read_text())
    if not count:
        raise Invalid('README does not contain a driver version to stamp.')
    path.write_text(text)
    git('add', 'README.md')


def main():
    try:
        command = sys.argv[1]
        if command == 'resolve':
            commit, effective = resolve(os.environ['RELEASE_MAJOR'], os.environ['RELEASE_TARGET_TAG'],
                                        os.environ.get('RELEASE_VERSION', ''),
                                        os.environ['GITHUB_SHA'], os.environ['GITHUB_REF'])
            with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
                output.write(f'ref={commit}\nversion={effective}\n')
        elif command == 'checkout':
            checkout(os.environ['RELEASE_MAJOR'], os.environ['RELEASE_TARGET_TAG'],
                     os.environ['RELEASE_TARGET_COMMIT'])
        elif command == 'check-branch':
            branch = 'refs/heads/scylla-' + os.environ['RELEASE_MAJOR'] + '.x'
            git('fetch', '--no-tags', 'origin', branch)
            if git('rev-parse', 'FETCH_HEAD') != os.environ['RELEASE_TARGET_COMMIT']:
                raise Invalid('Release branch advanced after validation; dispatch again.')
        elif command == 'preflight':
            preflight()
        elif command == 'blockers':
            blockers(os.environ['RELEASE_MAJOR'])
        elif command == 'stamp-readme':
            stamp_readme()
        else:
            raise Invalid('Unsupported release target command.')
        return 0
    except Invalid as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print('Release target check failed; values withheld.', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
