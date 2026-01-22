# Copilot instructions (Scylla Java Driver)

## Big picture
- This repo builds the **Scylla Java Driver (3.x line)**, a fork of the DataStax Java Driver.
- Maven coordinates use `com.scylladb:*`, but most public API packages remain under `com.datastax.driver.*` (e.g. [driver-core/src/main/java/com/datastax/driver/core](../driver-core/src/main/java/com/datastax/driver/core)).

## Modules & where to change code
- **driver-core**: main driver implementation + public API (`com.datastax.driver.core.*`), load balancing policies, protocol, pooling.
-   - Shard/tablet-aware routing lives around `TabletMap` and `TokenAwarePolicy` in `com.datastax.driver.core.*`.
- **driver-mapping**: object mapper layer (annotation/mapper APIs).
- **driver-extras**: optional add-ons.
- **driver-tests**: integration + stress + OSGi/shading tests.
- **driver-dist**: distribution/assembly.
- Docs live in [docs](../docs) (Sphinx + generated Javadoc).

## Local workflows (prefer these)
- Format (required; build fails if unformatted): `mvn fmt:format` (Google Java Style via fmt-maven-plugin).
- Fast compile: `make compile-all` (skips fmt/clirr/animal-sniffer).
- Full verify (what CI runs): `make check` (runs `mvn verify -DskipTests`).
- Unit tests only: `make test-unit` (TestNG group `unit`).

## Test conventions
- Tests use **TestNG groups** controlled by Maven profiles:
  - default: `unit`
  - integration: `mvn test -Pshort` or `mvn test -Plong` (these do **not** include unit tests).
- Integration tests bootstrap clusters via **CCM** (Scylla CCM recommended). Useful targets:
  - `make install-scylla-ccm` / `make install-cassandra-ccm`
  - `make test-integration-scylla` / `make test-integration-cassandra`
- Some ITs require system deps (see [CONTRIBUTING.md](../CONTRIBUTING.md)) e.g. `libssl1.0.0` for SSL tests.

## Compatibility constraints to keep in mind
- **Java 8 API compatibility is enforced** (animal-sniffer); avoid using APIs not available on Java 8.
- **Binary/source compatibility matters** (clirr); avoid breaking public signatures in `com.datastax.driver.*` unless the change is intentional and build tooling is updated.

## Docs
- Docs build is driven by [docs/Makefile](../docs/Makefile):
  - `make -C docs setupenv && make -C docs test` (fails on warnings)
  - `make -C docs preview` (local server)
  - `make -C docs javadoc` (generates API reference)
