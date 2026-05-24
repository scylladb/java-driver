# Building the docs

## Prerequisites

To build the documentation of this project, you need a UNIX-based operating system. Windows is not fully supported as it does not support symlinks.

You also need the following software installed to generate the reference documentation of the driver:

- Java JDK 11 or higher
- Maven

Once you have installed the above software, you can build and preview the documentation by following the steps outlined in the `Quickstart guide <https://sphinx-theme.scylladb.com/stable/getting-started/quickstart.html>`_.

## Custom commands

To generate the reference documentation of the driver, run the command `make javadoc`. This command generates the reference documentation using the Javadoc tool in the `_build/dirhtml/<VERSION>/api` directory.

## Using the Makefile

Most day-to-day tasks are wrapped in the top-level `Makefile` so you do not have to remember long Maven invocations. Common targets include:

- `make download-all-dependencies` pre-fetches all Maven artifacts to warm local caches.
- `make compile-all` compiles main and test sources without running tests, skipping format, clirr, and animal-sniffer checks for speed.
- `make test-unit` runs the fast unit-test suite; use `MVNCMD` to tweak the underlying Maven command if needed.
- `make test-integration-scylla` and `make test-integration-cassandra` execute CCM-backed integration suites. Export `SCYLLA_VERSION` or `CASSANDRA_VERSION` to pin specific server versions before invoking.
- `make check` executes `mvn verify -DskipTests` for static analysis, while `make fix` applies code formatters via `mvn fmt:format`.
- `make fix` executes `mvn fmt:format` to format the code.
- `make clean` removes Maven targets, shaded artifacts, and release backups to reset the tree.

The Makefile automatically installs the shaded Guava dependency and, for integration tests, bootstraps the appropriate CCM toolchain and raises kernel `aio-max-nr` when required. If a target fails because the toolchain is missing, rerun after installing the prerequisites highlighted in the target output.
