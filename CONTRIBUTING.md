# Contributing guidelines

## Code formatting

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
https://github.com/google/google-java-format for IDE plugins. The rules are not configurable.

The build will fail if the code is not formatted. To format all files from the command line, run:

```
mvn fmt:format
```

Some aspects are not covered by the formatter:

* braces must be used with `if`, `else`, `for`, `do` and `while` statements, even when the body is
  empty or contains only a single statement.

Also, if your IDE sorts import statements automatically, make sure it follows the same order as the
formatter: all static imports in ASCII sort order, followed by a blank line, followed by all regular
imports in ASCII sort order.  In addition, please avoid using wildcard imports.

## Working on an issue

We use [github issues](https://github.com/scylladb/java-driver/issues) to track ongoing issues. 
Before starting to work on something, please check the issues list to make sure nobody else is working on it.
It's also a good idea to get in contact through [ScyllaDB-Users Slack](https://scylladb-users.slack.com/) 
to make your intentions known and clear.

If your fix applies to multiple branches, base your work on the lowest active branch. Most of the time if you want to implement a feature for driver version 3, then you'll base your work on `scylla-3.x` (and `scylla-4.x` for version 4). Since version 3 of the driver,
we've adopted [semantic versioning](http://semver.org/) and our branches use the following scheme:

```
            3.0.1      3.0.2 ...                3.1.1 ...
         -----*----------*------> 3.0.x      -----*------> 3.1.x
        /                                   /
       /                                   /
      /                                   /
-----*-----------------------------------*-------------------------> 3.x
   3.0.0                               3.1.0        ...

Legend:
 > branch
 * tag
```

- new features are developed on "minor" branches such as `3.x`, where minor releases (ending in `.0`) happen.
- bugfixes go to "patch" branches such as `3.0.x` and `3.1.x`, where patch releases (ending in `.1`, `.2`...) happen.
- patch branches are regularly merged to the right (`3.0.x` to `3.1.x`) and to the bottom (`3.1.x` to `3.x`) so that
  bugfixes are applied to newer versions too.

The current active versions are 3.0 and 3.1. Therefore:

- if you're fixing a bug on a feature that existed since 3.0, target `3.0.x`. Your changes will be available in future
  3.0 and 3.1 patch versions.
- if you're fixing a bug on a 3.1-only feature, target `3.1.x`. Your changes will be available in a future 3.1 patch
  version.
- if you're adding a new feature, target `3.x`. Your changes will be available in the upcoming 3.2.0.

Before you send your pull request, make sure that:

- you have a unit test that failed before the fix and succeeds after.
- the fix is mentioned in `changelog/README.md`.
- the commit message includes the reference of the github issue for 
  <a href="https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue">automatic linking</a>
  (example: `Fixes #1234`).

As long as your pull request is not merged, it's OK to rebase your branch and push with
`--force`. Commit history should be as flat as reasonably possible. Multiple commits where each one represents a single logical piece of pull request are fine.
If you want to contribute but don't have a specific issue in mind, it's best to reach out through users slack.

## Editor configuration

For IntelliJ IDEA users, a suite of run configurations is bundled with the project (`.run` directory).
The run configurations should appear automatically in Run > Run... after loading the project in IntelliJ. They provide
a convenient way to build and test the project.

We use IntelliJ IDEA with the default formatting options, with one exception: check
"Enable formatter markers in comments" in Preferences > Editor > Code Style.

Please format your code and organize imports before submitting your changes.

## Running the tests

We use TestNG. There are 3 test categories:

- "unit": pure Java unit tests.
- "short" and "long": integration tests that launch Cassandra instances.

The Maven build uses profiles named after the categories to choose which tests to run:

```
mvn test -Pshort
```

The default is "unit". Each profile runs only their own category ("short" will *not* run "unit").

Integration tests use [CCM](https://github.com/pcmanus/ccm) to bootstrap Cassandra instances. It is recommended to 
setup [Scylla CCM](https://github.com/scylladb/scylla-ccm) in its place:
```
pip3 install https://github.com/scylladb/scylla-ccm/archive/master.zip
```

The SSL tests use `libssl.1.0.0.so`. Before starting the tests, make sure it is installed on your system
(`compat-openssl10` on Fedora and `libssl1.0.0` on Ubuntu, `xenial-security` repository source).

Two Maven properties control its execution:

- `cassandra.version`: the Cassandra version. This has a default value in the root POM,
  you can override it on the command line (`-Dcassandra.version=...`).
- `ipprefix`: the prefix of the IP addresses that the Cassandra instances will bind to (see
  below). This defaults to `127.0.1.`.

Additionally `-Dscylla.version=${{ matrix.scylla-version }}` can be used instead with Scylla CCM to test against Scylla.

Examples:
- `mvn test -Pshort -Dcassandra.version=3.11.11`
- `mvn test -Plong -Dcassandra.version=3.11.11`
- `mvn verify -Plong -Dscylla.version=4.3.6`


CCM launches multiple Cassandra instances on localhost by binding to different addresses. The
driver uses up to 10 different instances (127.0.1.1 to 127.0.1.10 with the default prefix).
You'll need to define loopback aliases for this to work, on Mac OS X your can do it with:

```
sudo ifconfig lo0 alias 127.0.1.1 up
sudo ifconfig lo0 alias 127.0.1.2 up
...
```

### Code coverage

Coverage is measured with [JaCoCo](https://www.jacoco.org/jacoco/) and is off by default: the agent
slows every forked test JVM down, so it is opt-in through the `coverage` Maven profile. Pass
`COVERAGE=true` to any of the `test-*` Make targets to enable it, then aggregate:

```sh
make test-unit COVERAGE=true
make coverage-report
```

`make coverage-report` reads whatever execution data is already on disk, so several lanes can be
combined into one number:

```sh
make test-unit COVERAGE=true
make test-integration-scylla COVERAGE=true
make coverage-report
```

Nothing has to be moved out of the way between the two: each lane writes its execution data under a
name of its own (`jacoco-unit.exec`, `jacoco-scylla-LATEST.exec`, ...) and truncates only that file
before it starts, so one lane can never overwrite another's results or read stale ones as its own.

The report lands in `driver-coverage-report/target/site/jacoco-aggregate` (HTML, XML and CSV), and
`make clean-coverage` removes it along with the execution data. In CI, the unit and integration jobs
upload their execution data and the "Coverage report" job aggregates it; the percentage shows up in
that job's summary and the HTML report is attached as an artifact.

Two things to know about the scope of the report:

- It covers the modules that `driver-coverage-report` depends on: `driver-core`,
  `driver-mapping` and `driver-extras`. `driver-examples`, `driver-tests/**` and `driver-dist`
  are out of scope, and the agent is not attached to them at all: the data would only be recorded
  for nobody to read.
- Only Surefire is instrumented. That covers the unit tests and, because 3.x runs its integration
  tests as TestNG `short`-group tests, the integration tests as well. The Failsafe-run tests in
  `driver-tests/**` (OSGi, shading) are left out: the OSGi ones load the driver inside their own
  Pax Exam container, and those modules are outside the report's scope in any case.

JaCoCo matches execution data to classes by checksum, so the data has to come from the same build of
the classes the report is rendered against. If a report shows code you know was exercised as
uncovered, look for `Execution data for class ... does not match` in the Maven log; the usual cause
is stale execution data from before a recompile, which `make clean-coverage` clears.

## Updating GitHub Actions workflows

GitHub Actions workflows in this repository pin all third-party actions to specific commit SHAs
instead of mutable version tags (e.g. `@v5`). This is a supply chain security measure: tags can be
moved to point to different commits, but a SHA is immutable.

The format used is:

```yaml
uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1
```

There is no need to update workflow action versions on every release. Only do so when the current
version has a known vulnerability or when a new feature is needed.

### How to update a pinned action

1. Go to the action's GitHub repository (e.g. `github.com/actions/checkout`).
2. Navigate to the desired release tag (e.g. `v5.0.2`) via the Tags page.
3. Copy the full 40-character commit SHA from that tag's commit page.
4. Verify the commit is not an [impostor commit](https://www.chainguard.dev/unchained/what-the-fork-imposter-commits-in-github-actions-and-ci-cd):
   open the commit on GitHub and ensure there is **no** banner saying
   "This commit does not belong to any branch on this repository".
5. Replace the SHA and version comment in all workflow files.
6. Update the repository allowlist under
   `Settings -> Actions -> General -> Allow or block specified actions and reusable workflows`
   to include the new SHA.
