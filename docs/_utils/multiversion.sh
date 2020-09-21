#! /bin/bash	

cd .. && sphinx-multiversion docs/_source docs/_build/dirhtml \
    --pre-build="./_utils/doxygen.sh" \
    --pre-build="cp -Tr source _source" \
    --pre-build="cd _source && find -name README.md -execdir mv '{}' index.md ';'"