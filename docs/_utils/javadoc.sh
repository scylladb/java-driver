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

# Generate javadoc. driver-core is the only module the published api/ contains, and restricting
# the reactor keeps a javadoc failure in driver-mapping or driver-extras from costing it;
# javadoc:javadoc is a standalone goal so it resolves from the repository, not the reactor.
JAVADOC_MODULE=driver-core
# The api/ package the module contributes. The publish only checks that api/index.html is
# non-empty, so a run that generated nothing else would be invisible downstream.
JAVADOC_API_PACKAGE=com/datastax/driver/core

# Nothing here runs clean, so drop the previous run's output: it is indistinguishable from this
# run's, and an empty run would otherwise republish it as current.
rm -rf "$JAVADOC_MODULE/target/reports/apidocs" "$JAVADOC_MODULE/target/site/apidocs"

mvn javadoc:javadoc -pl "$JAVADOC_MODULE" -T 1C

# maven-javadoc-plugin writes to target/reports from 3.11 on, and to target/site before it.
apidocs_dir() {
    local module="$1" package="$2" candidate
    for candidate in "$module/target/reports/apidocs" "$module/target/site/apidocs"; do
        if [[ -f "$candidate/index.html" && -s "$candidate/index.html" && -d "$candidate/$package" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "No javadoc output holding $package for $module" >&2
    return 1
}

# Resolve before touching $OUTPUT_DIR: the default branch's javadoc-multiversion.sh downgrades our
# exit code to a warning, so whatever api/ holds by then is what gets deployed.
source_dir="$(apidocs_dir "$JAVADOC_MODULE" "$JAVADOC_API_PACKAGE")" || exit 1

# Assemble alongside and swap in, so a failed copy cannot leave a partial api/, and clean the
# staging tree up on any exit so a failure cannot deploy it next to the real one.
staging_dir="$OUTPUT_DIR.new"
trap 'rm -rf "$staging_dir"' EXIT
rm -rf "$staging_dir"
mkdir -p "$staging_dir"
# Copy rather than move, so the source survives a second run.
cp -a "$source_dir/." "$staging_dir"
rm -rf "$OUTPUT_DIR"
mv "$staging_dir" "$OUTPUT_DIR"
