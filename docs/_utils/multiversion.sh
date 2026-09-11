#! /bin/bash	

# javadoc.sh comes from each version's own tree and runs a full Maven build, so
# it can break on a frozen release branch for reasons unrelated to the docs.
# sphinx-multiversion's run_commands() only rescues OSError, so an exit code
# there aborts the whole multiversion run and the site publishes nothing -- that
# is how every build between 2026-05-26 and 2026-08-31 was lost. Degrade to a
# missing /api/ for the one version instead, and say so in the log.
cd .. && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "bash -c \"(find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'; find . -mindepth 2 -name README.rst -execdir mv '{}' index.rst ';')\"" \
    --post-build "bash -c './docs/_utils/javadoc.sh || echo \"::warning::javadoc build failed for \$SPHINX_MULTIVERSION_NAME - its api pages will be missing\"'"
