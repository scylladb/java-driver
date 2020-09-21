#! /bin/bash	

cd .. && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build './docs/_utils/doxygen.sh' \
    --pre-build "find docs/source -name README.md -execdir mv '{}' index.md ';'"