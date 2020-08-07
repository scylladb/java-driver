#!/bin/bash

GITHUB_VERSION=${GITHUB_REF##*/}

OUTPUT_DIR="docs/_build/dirhtml/api"
if [[ "$GITHUB_VERSION" != "" ]]; then
    OUTPUT_DIR="docs/_build/dirhtml/$GITHUB_VERSION/api"
    echo "HTML_OUTPUT = $OUTPUT_DIR" >> doxyfile
fi
echo $OUTPUT_DIR
mkdir -p "$OUTPUT_DIR"
doxygen doxyfile