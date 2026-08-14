#!/bin/bash
#
# Runs each documented version's javadoc.sh with the JDK that version needs.

case "${SPHINX_MULTIVERSION_NAME:-}" in
  scylla-4.7.2.x | \
  scylla-4.10.0.x | \
  scylla-4.11.1.x | \
  scylla-4.12.0.x | \
  scylla-4.13.0.x | \
  scylla-4.14.1.x | \
  scylla-4.15.0.x | \
  scylla-4.17.0.x | \
  scylla-4.18.1.x | \
  scylla-4.19.0.x)
    JDK_VERSION=8
    ;;
  *)
    JDK_VERSION=11
    ;;
esac

JDK_HOME_VAR="JAVA_HOME_${JDK_VERSION}_X64"
SELECTED_JDK="${!JDK_HOME_VAR:-}"

if [[ -n "$SELECTED_JDK" ]]; then
    echo "Building javadoc for '${SPHINX_MULTIVERSION_NAME:-?}' with JDK ${JDK_VERSION} (${SELECTED_JDK})"
    export JAVA_HOME="$SELECTED_JDK"
    export PATH="$JAVA_HOME/bin:$PATH"
elif [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "${JDK_HOME_VAR} is not set: add JDK ${JDK_VERSION} to the setup-java step in docs-pages.yml" >&2
    exit 1
else
    echo "Building javadoc for '${SPHINX_MULTIVERSION_NAME:-?}' with the default JDK (${JDK_HOME_VAR} is not set)"
fi

exec ./docs/_utils/javadoc.sh
