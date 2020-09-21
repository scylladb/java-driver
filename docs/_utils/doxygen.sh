#!/bin/bash

OUTPUT_DIR="docs/_build/dirhtml/api"
if [[ "$SPHINX_MULTIVERSION_OUTPUTDIR" != "" ]]; then
    OUTPUT_DIR="docs/$SPHINX_MULTIVERSION_OUTPUTDIR/api"
fi
mkdir -p "$OUTPUT_DIR"
( cat doxyfile | echo "HTML_OUTPUT = $OUTPUT_DIR" )  | doxygen doxyfile
