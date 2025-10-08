#! /bin/bash	

cd .. && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "bash -c \"(find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'; find . -mindepth 2 -name README.rst -execdir mv '{}' index.rst ';')\"" \
    --post-build './docs/_utils/javadoc.sh'
