#!/bin/bash
#
# Fixture tests for check-javadoc-output.sh. The docs publish never runs before merge,
# so this is the only thing that exercises the guard; Docs / Build PR runs it.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/check-javadoc-output.sh"
REAL_CONF="$HERE/../source/conf.py"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

STUB_CONF="$TMP/conf.py"
cat > "$STUB_CONF" <<'PY'
TAGS = []
BRANCHES = [
    'scylla-4.17.0.x',
    'scylla-4.18.1.x',
    'scylla-4.19.0.x'
]
LATEST_VERSION = 'scylla-4.19.0.x'
scylladb_markdown_recommonmark_versions = [
    'decoy-should-not-be-read'
]
smv_rename_latest_version = 'stable'
PY

ALL="scylla-4.17.0.x scylla-4.18.1.x scylla-4.19.0.x stable"

# What the real conf.py documents, written out rather than re-derived: deriving it
# with a copy of the guard's own parser makes this case pass when that parser breaks.
# BRANCHES order, then the smv_rename_latest_version alias. Changing the published set
# is meant to land here too.
REAL_ALL="scylla-3.7.2.x scylla-3.10.2.x scylla-3.11.0.x scylla-3.11.2.x \
scylla-3.11.4.x scylla-3.11.5.x scylla-3.x scylla-4.7.2.x scylla-4.10.0.x \
scylla-4.11.1.x scylla-4.12.0.x scylla-4.13.0.x scylla-4.14.1.x \
scylla-4.15.0.x scylla-4.17.0.x scylla-4.18.1.x scylla-4.19.0.x stable"
REAL_ALL="$(echo $REAL_ALL)"

failures=0

# make_version <build-dir> <version> ok|empty|noindex|emptyindex|dirindex|nodocs|absent
make_version() {
    local dir="$1/$2"
    case "$3" in
        ok) mkdir -p "$dir/api" && echo "javadoc" > "$dir/api/index.html" ;;
        empty) mkdir -p "$dir/api" ;;
        noindex) mkdir -p "$dir/api" && echo "list" > "$dir/api/element-list" ;;
        emptyindex) mkdir -p "$dir/api" && : > "$dir/api/index.html" ;;
        dirindex) mkdir -p "$dir/api/index.html" ;;
        nodocs) mkdir -p "$dir" ;;
        absent) : ;;
        *) echo "make_version: unknown state '$3'" >&2; exit 1 ;;
    esac
}

# run_case <name> <want-exit> <want-not-built> <want-missing-api> <build-dir> [conf]
# Asserting the two buckets separately is what keeps a fixture from passing out of the
# wrong one.
run_case() {
    local name="$1" want_exit="$2" want_nb="$3" want_ma="$4" build="$5" conf="${6:-$STUB_CONF}"
    local out="$TMP/output" summary="$TMP/summary" log="$TMP/log"
    : > "$out"
    : > "$summary"
    GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="$summary" \
        "$SCRIPT" "$build" "$conf" > "$log" 2>&1
    local got_exit=$?
    local got_nb got_ma
    got_nb="$(sed -n 's/^not-built=//p' "$out")"
    got_ma="$(sed -n 's/^missing-api=//p' "$out")"
    if [ "$got_exit" != "$want_exit" ] || [ "$got_nb" != "$want_nb" ] || [ "$got_ma" != "$want_ma" ]; then
        printf 'FAIL %s\n  exit        %s (want %s)\n  not-built   "%s"\n       want   "%s"\n  missing-api "%s"\n       want   "%s"\n' \
            "$name" "$got_exit" "$want_exit" "$got_nb" "$want_nb" "$got_ma" "$want_ma"
        sed 's/^/  | /' "$log"
        failures=$((failures + 1))
    else
        printf 'ok   %s\n' "$name"
    fi
}

# assert_summary <name> <grep-pattern> — checks the report left by the last run_case
assert_summary() {
    if ! grep -q -- "$2" "$TMP/summary"; then
        printf 'FAIL %s: summary does not mention "%s"\n' "$1" "$2"
        sed 's/^/  | /' "$TMP/summary"
        failures=$((failures + 1))
    fi
}

# refute_summary <name> <grep-pattern> — the report must NOT say this
refute_summary() {
    if grep -q -- "$2" "$TMP/summary"; then
        printf 'FAIL %s: summary should not mention "%s"\n' "$1" "$2"
        sed 's/^/  | /' "$TMP/summary"
        failures=$((failures + 1))
    fi
}

# assert_log <name> <grep-pattern> — checks the annotations from the last run_case.
# Without this a case that refuses to publish passes whatever reason it gives.
assert_log() {
    if ! grep -q -- "$2" "$TMP/log"; then
        printf 'FAIL %s: output does not mention "%s"\n' "$1" "$2"
        sed 's/^/  | /' "$TMP/log"
        failures=$((failures + 1))
    fi
}

# build_tree <name> <state-for-4.17> <4.18> <4.19> <stable>
build_tree() {
    local build="$TMP/$1"
    mkdir -p "$build"
    make_version "$build" scylla-4.17.0.x "$2"
    make_version "$build" scylla-4.18.1.x "$3"
    make_version "$build" scylla-4.19.0.x "$4"
    make_version "$build" stable "$5"
    echo "$build"
}

run_case "complete build" 0 "" "" "$(build_tree complete ok ok ok ok)"
# stable is a copy of the latest version, so it is a directory but not a documented one.
assert_summary "complete build" "All 3 documented versions"
assert_summary "complete build" "4 directories, including .stable."

run_case "empty api/" 0 "" "scylla-4.18.1.x" "$(build_tree empty-api ok empty ok ok)"
assert_summary "empty api/" "Built without javadoc"

run_case "version not built" 0 "scylla-4.18.1.x" "" "$(build_tree absent-dir ok absent ok ok)"
assert_summary "version not built" "Not built at all"

run_case "api/ without index.html" 0 "" "scylla-4.18.1.x" "$(build_tree no-index ok noindex ok ok)"
assert_summary "api/ without index.html" "Built without javadoc"

# -f alone accepts a zero-byte index.html; -s alone accepts a directory named one.
run_case "zero-byte index.html" 0 "" "scylla-4.18.1.x" "$(build_tree zero-index ok emptyindex ok ok)"
run_case "index.html is a directory" 0 "" "scylla-4.18.1.x" "$(build_tree dir-index ok dirindex ok ok)"

run_case "version built without api/" 0 "" "scylla-4.18.1.x" "$(build_tree no-api ok nodocs ok ok)"
assert_summary "version built without api/" "Built without javadoc"

# stable is a copy of the latest version: one failure, two directories.
run_case "latest and its stable copy" 0 "" "scylla-4.19.0.x stable" "$(build_tree mirrored ok ok empty empty)"
assert_summary "latest and its stable copy" "also published as .stable."
assert_summary "latest and its stable copy" "copy of .scylla-4.19.0.x."

# Nothing survives, so the guard stops before deploy and the site keeps what it has.
# The report must not describe deletions that never happen.
run_case "every version lost" 1 "" "$ALL" "$(build_tree total-loss empty empty empty empty)"
assert_summary "every version lost" "the published site is unchanged"
refute_summary "every version lost" "deploy removes"

run_case "nothing built" 1 "$ALL" "" "$(build_tree nothing absent absent absent absent)"
assert_summary "nothing built" "Not built at all"
assert_summary "nothing built" "the published site is unchanged"
refute_summary "nothing built" "deploy removes"

# Directories that are not versions must not be mistaken for one: redirects-cli writes
# top-level stubs into the same tree, and the theme ships _static.
extras="$(build_tree extras ok ok ok ok)"
mkdir -p "$extras/_static" "$extras/old-page"
: > "$extras/old-page/index.html"
run_case "non-version directories ignored" 0 "" "" "$extras"

# A conf.py this script cannot read must stop the publish, not quietly check a
# shorter list: BRANCHES is what the whole guard is derived from. The annotation has
# to name that cause, or the operator is sent after an empty version list instead.
cat > "$TMP/unreadable-conf.py" <<'PY'
TAGS = []
BRANCHES = [v for v in ('scylla-4.19.0.x',)]
LATEST_VERSION = 'scylla-4.19.0.x'
smv_rename_latest_version = 'stable'
PY
run_case "BRANCHES is not a literal" 1 "" "" "$(build_tree unreadable ok ok ok ok)" \
    "$TMP/unreadable-conf.py"
assert_log "BRANCHES is not a literal" "::error::Cannot read the documented versions"

run_case "conf.py does not exist" 1 "" "" "$(build_tree no-conf ok ok ok ok)" \
    "$TMP/no-such-conf.py"
assert_log "conf.py does not exist" "::error::Cannot read the documented versions"

# A build directory that is not there is a wrong path, not a build that produced
# nothing, and saying so is the difference between fixing one and hunting the other.
run_case "build directory does not exist" 1 "" "" "$TMP/no-such-build"
assert_log "build directory does not exist" "does not exist, so there is no build output"

# The real conf.py, so a reshuffle there cannot silently empty the expected list.
mkdir -p "$TMP/real-empty"
run_case "real conf.py, nothing built" 1 "$REAL_ALL" "" "$TMP/real-empty" "$REAL_CONF"

real_full="$TMP/real-full"
mkdir -p "$real_full"
for version in $REAL_ALL; do make_version "$real_full" "$version" ok; done
run_case "real conf.py, everything built" 0 "" "" "$real_full" "$REAL_CONF"
assert_summary "real conf.py, everything built" "All 17 documented versions"
assert_summary "real conf.py, everything built" "18 directories, including .stable."

# Run by hand, neither variable exists and the report goes to stdout.
plain="$(build_tree plain ok ok ok ok)"
if plain_out="$(env -u GITHUB_OUTPUT -u GITHUB_STEP_SUMMARY "$SCRIPT" "$plain" "$STUB_CONF" 2>&1)"; then
    case "$plain_out" in
        *"All 3 documented versions"*) printf 'ok   %s\n' "no GITHUB_ environment" ;;
        *) printf 'FAIL no GITHUB_ environment: report missing from stdout\n%s\n' "$plain_out"
           failures=$((failures + 1)) ;;
    esac
else
    printf 'FAIL no GITHUB_ environment: exit %s\n%s\n' "$?" "$plain_out"
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    printf '\n%s case(s) failed\n' "$failures"
    exit 1
fi
printf '\nall cases passed\n'
