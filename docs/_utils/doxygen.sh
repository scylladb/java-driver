#!/bin/bash

OUTPUT_DIR="_build/dirhtml/api"
if [[ "$SPHINX_MULTIVERSION_OUTPUTDIR" != "" ]]; then
    echo $SPHINX_MULTIVERSION_OUTPUTDIR
    OUTPUT_DIR="$SPHINX_MULTIVERSION_OUTPUTDIR/api"
fi
echo $OUTPUT_DIR
mkdir -p "$OUTPUT_DIR"
cd .. && ( cat Doxyfile | echo "HTML_OUTPUT = $OUTPUT_DIR" )  | doxygen doxyfile
