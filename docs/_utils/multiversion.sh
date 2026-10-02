#! /bin/bash

cd .. || exit 1

# TAGS versions build from their line's newest release, through local alias tags.
./docs/_utils/alias-tags.sh create || exit 1
trap './docs/_utils/alias-tags.sh delete' EXIT

sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "bash -c \"(find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'; find . -mindepth 2 -name README.rst -execdir mv '{}' index.rst ';')\"" \
    --post-build "$(pwd)/docs/_utils/javadoc-multiversion.sh"
