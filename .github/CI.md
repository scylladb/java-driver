# CI and merge queue rollout for scylla-4.x

- PR opened, reopened, or updated: JDK 11 build, full verification, unit tests, unit coverage,
  release credential and release note safeguards.
- Merge queue candidate: the PR checks plus two complete JDK 11 Scylla suites, `LTS-PRIOR` and
  `LATEST`; unit and combined reduced coverage.
- Push after merge: reuse verified queue coverage under the merged SHA; no test execution.
- Pre-release: JDK 11/17 build, verification and unit tests; six Cassandra and nine Scylla
  integration jobs; unit and combined full coverage; existing external compatibility matrix.
- Manual dispatch: full suite by default; select `full-suite: false` for the queue suites.

`tests.yml` implements the reusable workflow. Build and test checkouts use `test-ref`, falling
back to the event SHA, and each Maven job applies `driver-version` when supplied. Safeguard jobs
check the workflow launch SHA so historical release targets need not contain current CI tooling.
Codecov uploads identify the tested source SHA, including historical release targets.
`tests@v1.yml` preserves the existing manual entrypoint and forwards reusable inputs. Its filename
contains `@`, so release callers use the implementation's filename without that ref delimiter.

`CI gate` fails if any expected job fails, is cancelled, is absent, or is unexpectedly skipped.
It also waits for coverage packaging so successful queue artifacts exist before merging.
PR-specific concurrency cancels superseded PR runs. Each queue SHA has its own concurrency group.
All PRs and queue candidates targeting `scylla-4.x` run the gate, including documentation changes.
Metadata automation and documentation deployment retain their existing triggers.

The two reduced integration jobs run all three Failsafe categories together (parallelizable,
serial, isolated), with a 90-minute timeout. They keep the existing CCM installation and image
caches, database logs, JUnit reports, and source-port reservation for isolated shard-aware tests.
Full runs retain the original database/JDK/category combinations and timeout.

## Coverage profiles and reuse

Reduced runs collect and compile coverage on JDK 11; full runs use JDK 17 exclusively. The report
jobs reject a missing lane, a missing expected module, an empty execution file, or a JDK mismatch.
Unit coverage is reported separately from the combined `reduced` and `full` profiles. Codecov's
PR comparisons use the `unit` flag; broader project statuses are separately labelled and
informational, with carryforward disabled. No coverage percentage is a merge threshold.

Raw execution data and rendered diagnostic reports remain available after failures. A verified
report is published only if every test required by that profile passed and all execution data is
present. Fork PRs and Dependabot retain tokenless, nonfatal Codecov handling. Upstream uploads
fail visibly if refused. Pre-release CI receives only the Codecov token; publishing credentials
remain confined to the protected release job.

A push runs only the coverage uploader (and independently applicable documentation/metadata
workflows). It searches successful `merge_group` runs of `tests.yml` for at most two minutes.
Before uploading, it verifies repository, workflow identity/ref, run and latest attempt, tested
SHA, JDK, profile, artifact archive digest, and XML report checksums. The candidate's Git tree
and first parent must match the merged commit's tree and first parent. This supports squash
merges without attributing another candidate's coverage to the merged SHA. Missing, altered,
failed or expired evidence causes an explained skip. Bypass/direct pushes never run tests or
substitute coverage from another tree.

## Release inputs

Release validation resolves the immutable source SHA and effective version once, then passes
both to full internal CI and the external compatibility matrix before publishing approval.
`skip-full-ci` defaults to false. Only selecting it permits a skipped internal suite; failure or
cancellation still blocks publishing. `skip-tests` retains its meaning for release validation
project tests and does not skip full internal CI. `skip-integration-matrix` still controls only
the external matrix. Release helper tests always run. Release authentication is unchanged.

## Disabled ruleset and activation

The separate ruleset **scylla-4.x merge queue CI** must remain **disabled** until rollout approval.
It targets only `refs/heads/scylla-4.x`, requires `CI gate` from the GitHub Actions app, and uses:

- Squash merging and `ALLGREEN`.
- Build concurrency 1; minimum and maximum merge group size 1.
- Required check timeout 120 minutes; no redundant up-to-date branch requirement.
- The existing `scylla-4.x/scylla-3.x` ruleset's bypass actors, copied without modification.
- No additional review requirements; queue entry remains manual.

Rollout is a separate follow-up:

1. Merge the workflow PR and confirm the new workflow is registered on `scylla-4.x`.
2. Verify a publisher bypass for the release workflow's direct signed-history push to
   `scylla-4.x`. The existing built-in Actions token must not be assumed to bypass the queue.
   Resolve this before activation, without changing release authentication in this PR.
3. Activate the new ruleset while preserving all existing active protections.
4. Verify one successful queue candidate (including both full Scylla suites and coverage reuse)
   and one deliberately failing candidate that is prevented from merging.

Local regression checks:

```sh
actionlint
python3 scripts/release/guard.py check
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
python3 -m unittest discover -s .github/tests -p 'test_*.py' -v
# Use the pinned labeler bundle, fetched as in release-notes-guard.yml:
LABELER_ACTION_PATH=/path/to/pinned/labeler.mjs node --test .github/tests/*.test.cjs
```
