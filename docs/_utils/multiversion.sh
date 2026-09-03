#! /bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT" && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "bash -c \"(find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'; find . -mindepth 2 -name README.rst -execdir mv '{}' index.rst ';')\"" \
    --post-build "bash '$SCRIPT_DIR/multiversion-javadoc.sh'"
