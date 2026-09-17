#!/bin/bash
#
# Reports documented versions whose javadoc did not survive the multiversion build.
#
# deploy.sh rebuilds gh-pages from this run's output and force-pushes, so a version
# that produced no api/ has its live one deleted rather than left stale. Exits
# non-zero only when nothing usable would publish; a partial loss is reported and
# left for the caller to fail on after deploying.

set -euo pipefail

BUILD_DIR="${1:-docs/_build/dirhtml}"
CONF_PY="${2:-docs/source/conf.py}"

# conf.py declares the versions as literals; read them rather than trusting whatever
# the build happened to produce.
if ! conf_dump="$(python3 - "$CONF_PY" <<'PY'
import ast
import sys

with open(sys.argv[1]) as conf:
    tree = ast.parse(conf.read())

# Keyed by target name: conf.py holds a second, identical-looking version list under
# scylladb_markdown_recommonmark_versions.
values = {}
for node in ast.walk(tree):
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name):
                try:
                    values[target.id] = ast.literal_eval(node.value)
                except (ValueError, SyntaxError):
                    pass

if not isinstance(values.get("BRANCHES"), list):
    sys.exit("BRANCHES is missing from %s, or is not a literal list" % sys.argv[1])

alias = values.get("smv_rename_latest_version", "")
for version in list(values.get("TAGS", [])) + list(values["BRANCHES"]):
    print("VERSION\t%s" % version)
if alias:
    print("VERSION\t%s" % alias)
print("LATEST\t%s" % values.get("LATEST_VERSION", ""))
print("ALIAS\t%s" % alias)
PY
)"; then
    # set -e does not fire on a failed substitution feeding a here-string, so without
    # this an unreadable conf.py would surface as "no documented versions" instead.
    echo "::error::Cannot read the documented versions from $CONF_PY" >&2
    exit 1
fi

latest=""
alias_name=""
expected=()
while IFS=$'\t' read -r key value; do
    case "$key" in
        VERSION) expected+=("$value") ;;
        LATEST) latest="$value" ;;
        ALIAS) alias_name="$value" ;;
    esac
done <<< "$conf_dump"

if [ "${#expected[@]}" -eq 0 ]; then
    echo "::error::No documented versions found in $CONF_PY" >&2
    exit 1
fi

if [ ! -d "$BUILD_DIR" ]; then
    # Distinct from "every version is missing": the build always creates this tree, so
    # its absence means a wrong path rather than a build that produced nothing.
    echo "::error::$BUILD_DIR does not exist, so there is no build output to check" >&2
    exit 1
fi

# An absent directory loses the whole version, not just its javadoc. api/index.html
# rather than a non-empty api/, because that file is what the site links to. The
# alias's own api.html redirects to the latest version's path, since conf.py's
# redirect_api_page_to_javadoc keys off SPHINX_MULTIVERSION_NAME - so this checks each
# directory's content, not where its redirect points.
absent=()
noapi=()
for version in "${expected[@]}"; do
    index="$BUILD_DIR/$version/api/index.html"
    if [ ! -d "$BUILD_DIR/$version" ]; then
        absent+=("$version")
    elif [ ! -f "$index" ] || [ ! -s "$index" ]; then
        noapi+=("$version")
    fi
done
lost=$(( ${#absent[@]} + ${#noapi[@]} ))

# Nothing survived, so this stops before deploy and the live site keeps what it has.
# The report has to say that rather than describe deletions that never happen.
total_loss=0
[ "$lost" -ne "${#expected[@]}" ] || total_loss=1

# stable is a copy of the latest version rather than a version of its own: a directory
# to check, but not one of the versions conf.py documents.
documented=${#expected[@]}
[ -z "$alias_name" ] || documented=$(( documented - 1 ))

# stable is a copy of the latest version, so one javadoc failure shows up as two
# directories. Say so rather than reporting two independent losses.
mirror_note() {
    if [ -n "$alias_name" ] && [ -n "$latest" ]; then
        case "$1" in
            "$alias_name") printf ' (copy of `%s`)' "$latest" ;;
            "$latest") printf ' (also published as `%s`)' "$alias_name" ;;
        esac
    fi
}

list_versions() {
    local version
    for version in "$@"; do
        printf -- '- `%s`%s\n' "$version" "$(mirror_note "$version")"
    done
}

report() {
    echo "### Javadoc output"
    echo
    if [ "$lost" -eq 0 ]; then
        if [ "$documented" -eq "${#expected[@]}" ]; then
            echo "All $documented documented versions built with a javadoc \`api/\`."
        else
            echo "All $documented documented versions built with a javadoc \`api/\` (${#expected[@]} directories, including \`$alias_name\`)."
        fi
        return
    fi
    if [ "${#absent[@]}" -gt 0 ]; then
        if [ "$total_loss" -eq 1 ]; then
            echo "Not built at all:"
        else
            echo "Not built at all — deploy removes these from the site entirely:"
        fi
        echo
        list_versions "${absent[@]}"
        echo
    fi
    if [ "${#noapi[@]}" -gt 0 ]; then
        if [ "$total_loss" -eq 1 ]; then
            echo "Built without javadoc:"
        else
            echo "Built without javadoc — deploy removes their \`/api/\`:"
        fi
        echo
        list_versions "${noapi[@]}"
        echo
    fi
    if [ "$total_loss" -eq 1 ]; then
        echo "No documented version survived, so nothing is deployed and the published site is unchanged."
    fi
}

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    report >> "$GITHUB_STEP_SUMMARY"
else
    report
fi

# Two outputs rather than one list: a version that never built is gone from the site
# entirely, which is not the same loss as one that published without its javadoc.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
        echo "not-built=${absent[*]:-}"
        echo "missing-api=${noapi[*]:-}"
    } >> "$GITHUB_OUTPUT"
fi

# Deploying a tree with no javadoc at all would delete every published api/ for no
# benefit, so stop before deploy. A partial loss still deploys: the versions that did
# build are worth publishing, and the caller fails the job afterwards.
if [ "$total_loss" -eq 1 ]; then
    echo "::error::No documented version has javadoc, refusing to deploy"
    exit 1
fi
