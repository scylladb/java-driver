#!/bin/bash
#
# Points a local tag named after each version in conf.py's TAGS at that line's newest
# release, so scylla-4.19.2.x builds from the newest 4.19.2.N tag and a patch release
# needs no conf.py change. The aliases are scratch refs: never push them.
#
#   alias-tags.sh create   (re)create one alias per TAGS entry
#   alias-tags.sh delete   remove them again

set -euo pipefail

CONF_PY="${CONF_PY:-docs/source/conf.py}"

if ! versions="$(python3 - "$CONF_PY" <<'PY'
import ast
import sys

with open(sys.argv[1]) as conf:
    tree = ast.parse(conf.read())

for node in ast.walk(tree):
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "TAGS":
                tags = ast.literal_eval(node.value)
                if not isinstance(tags, list):
                    sys.exit("TAGS is not a literal list")
                print("\n".join(tags))
                sys.exit(0)
sys.exit("TAGS is missing from %s" % sys.argv[1])
PY
)"; then
    echo "::error::Cannot read TAGS from $CONF_PY" >&2
    exit 1
fi

case "${1:-}" in
  create)
    for version in $versions; do
        if [[ ! "$version" =~ ^scylla-([0-9]+\.[0-9]+\.[0-9]+)\.x$ ]]; then
            echo "::error::TAGS entry '$version' is not of the form scylla-X.Y.Z.x" >&2
            exit 1
        fi
        line="${BASH_REMATCH[1]}"
        newest="$(git tag -l "$line.*" --sort=-v:refname | grep -E "^${line//./\\.}\.[0-9]+$" | head -n 1 || true)"
        if [[ -z "$newest" ]]; then
            echo "::error::No release tag matches $line.N for $version" >&2
            exit 1
        fi
        git tag -f "$version" "$newest^{commit}" > /dev/null
        echo "$version -> $newest"
    done
    ;;
  delete)
    for version in $versions; do
        git tag -d "$version" > /dev/null 2>&1 || true
    done
    ;;
  *)
    echo "usage: $0 create|delete" >&2
    exit 2
    ;;
esac
