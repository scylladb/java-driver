#! /bin/bash

cd .. || exit 1

# RELEASE_LINES versions build from their line's newest release, through local alias tags.
trap 'python3 ./docs/_utils/alias-tags.py delete' EXIT
python3 ./docs/_utils/alias-tags.py create || exit 1

sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "bash -c \"(find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'; find . -mindepth 2 -name README.rst -execdir mv '{}' index.rst ';')\"" \
    --post-build "$(pwd)/docs/_utils/javadoc-multiversion.sh"
