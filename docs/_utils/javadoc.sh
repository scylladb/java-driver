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

# Generate javadoc
mvn javadoc:javadoc -T 1C
JAVADOC_SOURCE_DIR="core/target/site/apidocs"
if [[ ! -d "$JAVADOC_SOURCE_DIR" && -d "core/target/reports/apidocs" ]]; then
    JAVADOC_SOURCE_DIR="core/target/reports/apidocs"
fi
if [[ ! -d "$JAVADOC_SOURCE_DIR" ]]; then
    echo "Javadoc output directory was not generated" >&2
    exit 1
fi
shopt -s nullglob
JAVADOC_FILES=("$JAVADOC_SOURCE_DIR"/*)
if (( ${#JAVADOC_FILES[@]} == 0 )); then
    echo "Javadoc output directory is empty" >&2
    exit 1
fi
if [[ -d "$OUTPUT_DIR" ]]; then
    rm -r "$OUTPUT_DIR"
fi
mkdir -p "$OUTPUT_DIR"
mv -f "${JAVADOC_FILES[@]}" "$OUTPUT_DIR"
