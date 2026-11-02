#!/usr/bin/env python3
"""Points a local tag named after each version in conf.py's RELEASE_LINES at that line's newest
release, so scylla-4.19.2.x builds from the newest 4.19.2.N tag and a patch release
needs no conf.py change. A new line does: create fails while a major has releases of a
newer line than RELEASE_LINES lists. The aliases are scratch refs: never push them.

  alias-tags.py create   (re)create one alias per RELEASE_LINES entry
  alias-tags.py delete   remove them again
"""

import ast
import os
import re
import subprocess
import sys

ALIAS = re.compile(r"scylla-(\d+\.\d+\.\d+)\.x")


def fail(message):
    print("::error::%s" % message, file=sys.stderr)
    sys.exit(1)


def read_lines(conf_py):
    with open(conf_py) as conf:
        tree = ast.parse(conf.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "RELEASE_LINES" for target in node.targets):
            lines = ast.literal_eval(node.value)
            if not isinstance(lines, list):
                fail("RELEASE_LINES in %s is not a literal list" % conf_py)
            return lines
    fail("RELEASE_LINES is missing from %s" % conf_py)


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def newest_release(line):
    # X.Y.Z.N only: release candidates (-rc) never match.
    release = re.compile(re.escape(line) + r"\.(\d+)")
    patches = [int(m.group(1)) for m in map(release.fullmatch, git("tag", "-l", line + ".*").split()) if m]
    return "%s.%d" % (line, max(patches)) if patches else None


def newest_line(major):
    # X.Y.Z.N only: release candidates and upstream's three-part tags never match.
    release = re.compile(re.escape(major) + r"\.(\d+)\.(\d+)\.\d+")
    lines = [(int(m.group(1)), int(m.group(2)))
             for m in map(release.fullmatch, git("tag", "-l", major + ".*").split()) if m]
    return (int(major),) + max(lines) if lines else None


def check_registered(lines):
    # An upstream sync starts a new line; without this its docs silently never publish.
    registered = {}
    for line in lines:
        key = tuple(int(part) for part in line.split("."))
        registered[key[0]] = max(registered.get(key[0], key), key)
    for major, current in sorted(registered.items()):
        newest = newest_line(str(major))
        if newest and newest > current:
            fail("%s has releases, but RELEASE_LINES lists scylla-%s.x for %d.x: add "
                 "scylla-%s.x to RELEASE_LINES, move scylla-%s.x to BRANCHES, and update "
                 "LATEST_VERSION, DEPRECATED_VERSIONS and javadoc-multiversion.sh to match"
                 % ("%d.%d.%d.N" % newest, "%d.%d.%d" % current, major,
                    "%d.%d.%d" % newest, "%d.%d.%d" % current))


def create(versions):
    # Resolve every entry before tagging, so a failure leaves no alias behind.
    aliases = []
    for version in versions:
        match = ALIAS.fullmatch(version)
        if not match:
            fail("RELEASE_LINES entry '%s' is not of the form scylla-X.Y.Z.x" % version)
        newest = newest_release(match.group(1))
        if not newest:
            fail("No release tag matches %s.N for %s" % (match.group(1), version))
        aliases.append((version, newest))
    check_registered([ALIAS.fullmatch(version).group(1) for version in versions])
    for version, newest in aliases:
        git("tag", "-f", version, newest + "^{commit}")
        print("%s -> %s" % (version, newest))


def delete(versions):
    for version in versions:
        # Only ever an alias: a malformed entry must not delete a release tag.
        if ALIAS.fullmatch(version):
            subprocess.run(["git", "tag", "-d", version], capture_output=True)


def main():
    commands = {"create": create, "delete": delete}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print("usage: %s create|delete" % sys.argv[0], file=sys.stderr)
        sys.exit(2)
    commands[sys.argv[1]](read_lines(os.environ.get("CONF_PY", "docs/source/conf.py")))


if __name__ == "__main__":
    main()
