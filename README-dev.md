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

### Measuring code coverage

Coverage is measured with [JaCoCo](https://www.jacoco.org/jacoco/) and is off by default: the agent
slows every forked test JVM down, so it is opt-in through the `coverage` Maven profile. Pass
`COVERAGE=true` to any of the `test-*` Make targets to enable it, then aggregate:

```
make test-unit COVERAGE=true
make coverage-report
```

`make coverage-report` reads whatever execution data is already on disk, so several lanes can be
combined into one number -- which is the point of the separate `coverage-report` module: it
attributes the coverage `core` gets *through* the integration suite back to `core`'s own source,
which each module's own report cannot see. A `COVERAGE=true` run truncates `jacoco.exec` before it
starts, so rename the previous lane's data out of the way to keep it:

```
make test-unit COVERAGE=true
find . -name jacoco.exec -execdir mv jacoco.exec jacoco-unit.exec \;
make test-integration-scylla COVERAGE=true
make coverage-report
```

The report lands in `coverage-report/target/site/jacoco-aggregate` (HTML, XML and CSV), and
`make clean-coverage` removes it along with the execution data. `make coverage-report` fails rather
than rendering a confident-looking but empty report if it finds no execution data, or if the data
matches none of the classes.

In CI, the unit and integration jobs in `tests@v1.yml` run with `COVERAGE=true` and upload their
execution data; the "Coverage report" job aggregates it, prints the percentage to its job summary
and attaches the HTML report as an artifact. That job is `continue-on-error`, so a flaky
integration test costs the metric some data rather than adding a second failure to the pull
request. Collecting from the existing lanes rather than a dedicated workflow keeps the Scylla suite
from being run twice.

JaCoCo matches execution data to classes by checksum, so the data has to come from the same build
of the classes the report is rendered against. If a report shows code you know was exercised as
uncovered, look for `Execution data for class ... does not match` in the Maven log; the usual cause
is stale execution data from before a recompile, which `make clean-coverage` clears.

Note: the surefire/failsafe configs in `core` and `integration-tests` previously set `<argLine>` to
just their own JVM flags (e.g. `${mockitoopens.argline}`), which silently discarded the
`-javaagent` flag `jacoco:prepare-agent` injects into the `argLine` property -- coverage was being
collected for every *other* module, but not these two. They now combine both via Maven's
deferred-property syntax: `<argLine>@{argLine} ${mockitoopens.argline}</argLine>` (`@{...}` is
necessary rather than `${...}` because `jacoco:prepare-agent` sets `argLine` at build-execution
time, after the POM's own `${...}` references would already have been resolved). `argLine` itself
is declared, empty, as a root `pom.xml` property so that combination resolves to something even
outside the `coverage` profile, where `jacoco:prepare-agent` never runs to give it a real value.
(`distribution-tests` has no `src` of its own, so surefire never forks there either way; it was
left out of this.)

The Makefile automatically installs the shaded Guava dependency and, for integration tests, bootstraps the appropriate CCM toolchain and raises kernel `aio-max-nr` when required. If a target fails because the toolchain is missing, rerun after installing the prerequisites highlighted in the target output.
