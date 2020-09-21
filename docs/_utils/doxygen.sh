#!/bin/bash

OUTPUT_DIR="docs/_build/dirhtml/api"
if [[ "$SPHINX_MULTIVERSION_OUTPUTDIR" != "" ]]; then
    OUTPUT_DIR="$SPHINX_MULTIVERSION_OUTPUTDIR/api"
    echo "HTML_OUTPUT = $OUTPUT_DIR" >> doxyfile
fi
mkdir -p "$OUTPUT_DIR"
doxygen doxyfile