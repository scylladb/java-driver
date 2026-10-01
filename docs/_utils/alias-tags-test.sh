#!/bin/bash
#
# Fixture tests for alias-tags.sh, run by Docs / Build PR.

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/alias-tags.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cd "$TMP" || exit 1

git init -q .
git -c user.name=t -c user.email=t@t commit -q --allow-empty -m one
first="$(git rev-parse HEAD)"
git -c user.name=t -c user.email=t@t commit -q --allow-empty -m two
second="$(git rev-parse HEAD)"
git tag 4.19.2.1 "$first"
git tag -a -m release 4.19.2.10 "$second"
git tag 4.19.2.9 "$first"
git tag 4.19.2.11-rc1 "$first"
git tag 3.11.5.19 "$first"

failures=0
check() {
    if [[ "$2" == "$3" ]]; then
        echo "ok   $1"
    else
        echo "FAIL $1: expected '$2', got '$3'"
        failures=$((failures + 1))
    fi
}

cat > conf.py <<'PY'
TAGS = ['scylla-4.19.2.x', 'scylla-3.11.5.x']
BRANCHES = []
scylladb_markdown_recommonmark_versions = ['decoy-should-not-be-read']
PY

CONF_PY=conf.py "$SCRIPT" create > /dev/null
check "newest patch wins by version order, annotated tags resolve to the commit" \
    "$second" "$(git rev-parse -q --verify 'refs/tags/scylla-4.19.2.x^{commit}')"
check "each line gets its own alias" \
    "$first" "$(git rev-parse -q --verify 'refs/tags/scylla-3.11.5.x^{commit}')"

CONF_PY=conf.py "$SCRIPT" create > /dev/null
check "create is repeatable" "0" "$?"

CONF_PY=conf.py "$SCRIPT" delete
check "delete removes every alias" "" "$(git tag -l 'scylla-*')"

cat > conf.py <<'PY'
TAGS = ['scylla-4.20.0.x']
PY
CONF_PY=conf.py "$SCRIPT" create > /dev/null 2>&1
check "a line with no release fails" "1" "$?"

cat > conf.py <<'PY'
TAGS = ['stable']
PY
CONF_PY=conf.py "$SCRIPT" create > /dev/null 2>&1
check "a malformed TAGS entry fails" "1" "$?"

cat > conf.py <<'PY'
BRANCHES = []
PY
CONF_PY=conf.py "$SCRIPT" create > /dev/null 2>&1
check "missing TAGS fails" "1" "$?"

cat > conf.py <<'PY'
TAGS = []
PY
CONF_PY=conf.py "$SCRIPT" create > /dev/null
check "empty TAGS is a no-op" "0" "$?"

exit "$failures"
