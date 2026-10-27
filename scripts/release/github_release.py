#!/usr/bin/env python3
"""Publish an immutable GitHub Release for the exact prepared and pushed signed tag."""

import json
import os
import subprocess
import sys

import signing
import target


class Invalid(ValueError):
    pass


def gh(*arguments, missing_ok=False):
    result = subprocess.run(
        ['gh', *arguments], capture_output=True, text=True, timeout=60,
    )
    output = result.stdout
    if missing_ok:
        # gh api --include supplies the HTTP status independently of error JSON
        # or stderr diagnostics. Denied access and network errors are not absence.
        headers, separator, output = output.partition('\n\n')
        status = headers.splitlines()[0].split() if headers else []
        if not separator or len(status) < 2 or not status[0].startswith('HTTP/'):
            raise Invalid('GitHub release request failed; response withheld.')
        if result.returncode and status[1] == '404':
            return None
        if status[1] != '200':
            raise Invalid('GitHub release request failed; response withheld.')
    if result.returncode:
        raise Invalid('GitHub release request failed; response withheld.')
    return output


def release(repository, version):
    value = gh('api', '--hostname', 'github.com', '--include',
               f'repos/{repository}/releases/tags/{version}', missing_ok=True)
    return json.loads(value) if value is not None else None


def verify_remote_tag(version):
    ref = 'refs/tags/' + version
    expected = target.git('rev-parse', ref)
    if target.git('ls-remote', '--exit-code', 'origin', ref) != expected + '\t' + ref:
        raise Invalid('Remote release tag differs from the verified signed tag.')


def verify_immutable(value, version):
    if (not isinstance(value, dict) or value.get('tag_name') != version
            or value.get('draft') is not False or value.get('immutable') is not True):
        raise Invalid('GitHub Release must be published and immutable; inspect it before recovery.')


def publish():
    major = os.environ['RELEASE_MAJOR']
    if (major not in ('3', '4') or os.environ.get('RELEASE_DRY_RUN') != 'false'
            or os.environ.get('RELEASE_TARGET_TAG') != f'scylla-{major}.x'):
        raise Invalid('GitHub Releases require a real release from the corresponding branch.')
    repository = os.environ['GITHUB_REPOSITORY']
    if repository != 'scylladb/java-driver':
        raise Invalid('Unexpected driver release repository.')
    version = target.version(major, os.environ['RELEASE_VERSION'])
    # This uses only the public certificate and the original preparation baseline.
    signing.verify_prepared()
    verify_remote_tag(version)
    existing = release(repository, version)
    if existing is None:
        gh('release', 'create', version, '--repo', 'github.com/' + repository,
           '--verify-tag', '--generate-notes', '--title', f'ScyllaDB Java Driver {version}',
           '--latest' if major == '4' else '--latest=false')
        existing = release(repository, version)
    # Accept a matching immutable release on recovery without editing it. Refuse
    # drafts and mutable releases rather than silently changing published state.
    verify_immutable(existing, version)
    verify_remote_tag(version)
    print(f'Verified immutable GitHub Release and signed tag: {version}')


def main():
    try:
        publish()
        return 0
    except Invalid as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print('GitHub release publication failed; values withheld.', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
