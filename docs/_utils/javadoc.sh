#!/bin/bash
set -euo pipefail

# Install dependencies
mvn install -DskipTests -Dmaven.javadoc.skip=true -T 1C

# Define output folder
OUTPUT_DIR="docs/_build/dirhtml/api"
if [[ "${SPHINX_MULTIVERSION_OUTPUTDIR:-}" != "" ]]; then
    OUTPUT_DIR="$SPHINX_MULTIVERSION_OUTPUTDIR/api"
    echo "HTML_OUTPUT = $OUTPUT_DIR" >> doxyfile
fi

# Generate javadoc. Restricting the reactor to the modules that are published also keeps
# coverage-report out of it: it needs integration-tests, whose install is skipped, and
# javadoc:javadoc is a standalone goal so it resolves from the repository, not the reactor.
JAVADOC_MODULES=(core query-builder mapper-runtime)
# The api/ package each module contributes. check-javadoc-output.sh only checks that
# api/index.html is non-empty, and the copy below always takes that file from the first
# module, so a module that produced nothing would otherwise be invisible downstream.
JAVADOC_API_PACKAGES=(core querybuilder mapper)

# Nothing here runs clean, so drop the previous run's output: it is indistinguishable from
# this run's, and an empty run would otherwise republish it as current.
for module in "${JAVADOC_MODULES[@]}"; do
    rm -rf "$module/target/reports/apidocs" "$module/target/site/apidocs"
done

mvn javadoc:javadoc -pl "$(IFS=,; echo "${JAVADOC_MODULES[*]}")" -T 1C

# maven-javadoc-plugin writes to target/reports from 3.11 on, and to target/site before it.
apidocs_dir() {
    local module="$1" package="$2" candidate index
    for candidate in "$module/target/reports/apidocs" "$module/target/site/apidocs"; do
        index="$candidate/index.html"
        # -f as well as -s: -s alone is true for a directory named index.html, as
        # check-javadoc-output.sh does for the same file.
        if [[ -f "$index" && -s "$index" && -d "$candidate/com/datastax/oss/driver/api/$package" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "No javadoc output holding com/datastax/oss/driver/api/$package for $module" >&2
    return 1
}

# Resolve every module before touching $OUTPUT_DIR: javadoc-multiversion.sh downgrades our
# exit code to a warning, so whatever api/ holds by then is what gets deployed.
source_dirs=()
for i in "${!JAVADOC_MODULES[@]}"; do
    source_dir="$(apidocs_dir "${JAVADOC_MODULES[$i]}" "${JAVADOC_API_PACKAGES[$i]}")" || exit 1
    source_dirs+=("$source_dir")
done

# Assemble alongside and swap in, so a failed copy cannot leave a partial api/. The first
# module provides the base tree and keeps the files every module generates, so index.html,
# element-list and the search index cover core only (#157). Copy rather than move, so the
# sources survive a second run.
staging_dir="$OUTPUT_DIR.new"
rm -rf "$staging_dir"
# Otherwise a failed copy leaves api.new next to api/, and deploy.sh publishes it.
trap 'rm -rf "$staging_dir"' EXIT
mkdir -p "$staging_dir"
for source_dir in "${source_dirs[@]}"; do
    cp -an "$source_dir/." "$staging_dir"
done
rm -rf "$OUTPUT_DIR"
mv "$staging_dir" "$OUTPUT_DIR"
