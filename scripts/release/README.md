# Driver releases and publisher signing

Both release lines follow the
[Alternator release process](https://github.com/scylladb/alternator-client-java/blob/96839d9c1aba6cea23c5065515e6ce1d9a05df8e/.github/workflows/release.yml).
Java APIs, coordinates, module exclusions, and compilation targets are unchanged.

## Protected environment

The shared GitHub `release` environment must require `scylladb/drivers` review, permit
self-review, disable administrator bypass, and allow deployment from exactly the branches
`scylla-4.x` and `scylla-3.x`. Dispatch from the corresponding release branch, including
historical releases. Validation and the integration matrix must finish before approval.

Environment secrets:

| Secret | Source |
| --- | --- |
| `GPG_PRIVATE_KEY` | Protected `~/.repos-gpg/scylladb/java-driver/signing-private.asc` |
| `GPG_PASSPHRASE` | Matching `~/.repos-gpg/scylladb/java-driver/passphrase.txt` |
| `SONATYPE_TOKEN_USERNAME` | Fresh Central Portal user token username |
| `SONATYPE_TOKEN_PASSWORD` | Fresh Central Portal user token password |

The initial Central values are `REPLACE_ME`; both must be replaced before a dry run or
release can prepare commits. Generate a token with driver namespace publishing rights
using [Central's token instructions](https://central.sonatype.org/publish/generate-portal-token/).
Upload secret files through stdin (`gh secret set NAME --repo scylladb/java-driver
--env release < FILE`); never place their contents in command arguments or Git.

Token expiration: **none** (configured during cutover). Keep the full
primary backup and revocation certificate offline. Retain `CODECOV_TOKEN`.

## Release inputs and validation

`target-tag` accepts the current release branch or a matching four-component version tag
(`4.MINOR.PATCH.DRIVER` or `3.MINOR.PATCH.DRIVER`). Other refs are rejected. An optional
`release-version` sets the effective version for branch releases. Historical releases
require both the tag POM and its preceding commit's POM to match the tag version; they
cannot use an override to publish under another version. Unsafe historical builds fail
credential-safety checks and must be ported to a supported target before release.

The workflow resolves the target SHA and version once. It runs release-helper tests and
an unsigned `-Prelease -Dgpg.skip=true verify` at the effective version without release
secrets, then passes that
same SHA and effective version to the driver integration matrix. `skip-tests` skips only
project tests; helper tests always run. As in 4.x release preparation, validation skips
CCM integration tests (`-DskipITs=true`); integration gates run in CI and the separate
matrix. The matrix must pass unless
`skip-integration-matrix` is explicitly selected. Open `release-blocker` issues apply to
both lines unless labelled solely for the other line. Blockers are checked before
validation and again immediately before publication, including all result pages.

Approval starts a fresh runner with no validation workspace or Maven cache. The runner
preserves the launch SHA's Makefile, guard, signing helper, target helper, and public
certificate before checking out the validated SHA. A branch that advanced during approval
is rejected. Publication checks the branch again after preparation. Preparation and
publication skip project tests already covered by validation. The 3.x README is stamped
with the effective version; 4.x retains its POM formatting preparation/completion goals. Its release profile skips
archive assembly: the unpublished distribution archives have no assembly descriptors.

## Signing and verification

The driver retains its existing publisher certificate (`publisher.asc`):

- Primary: `FA5BF24FD656DB6172B6DEDA92CDA600C75CA820`
- Signing subkey: `6633DD12AC5A67675513E2EA998E34B10468CB5B`
- Git identity: `ScyllaDB Publisher <publish.code@scylladb.com>`

Each signing session imports only the protected signing-subkey export into a fresh
keyring, checks both fingerprints and primary-secret absence, tests the passphrase with
a fresh agent, and signs a preflight payload. Credentials travel through environment and
pipes. Git configuration is scoped to the command and inherited by Maven child checkouts.
Maven artifacts, release/development commits, and annotated tags use the signing subkey.
The session stops children, kills its agent, and deletes its keyring on success, failure,
SIGTERM, SIGHUP, or SIGINT. No private key is imported by `setup-java`.

After preparation, public-only verification checks every commit since the original
`RELEASE_BASE_COMMIT`, requires an annotated signed tag on a newly prepared commit, and
requires the prepared tag to equal the effective release version. Publication rechecks
this history before importing signing credentials. Never substitute a newer baseline to
hide an unsigned or unexpected preparation commit.

Maven output is captured and sanitized before console or artifact output, including
encoded credentials. Both stdout and stderr remain available on failure or cancellation.
Do not enable Maven debug output, redirect raw output, pass credential properties on the
command line, or disable `gpg.bestPractices`.

Consumers can import the public certificate into an isolated GnuPG keyring and run
`gpg --verify artifact.jar.asc artifact.jar`, `git verify-tag VERSION`, or
`git verify-commit COMMIT`. Independently compare both fingerprints above, including the
signing subkey in GnuPG's `VALIDSIG` status. Keep historical certificates available.

For local releases, authenticate `gh` as `scylladb-publisher`; protected Actions operators
are authorized by environment review. Export the Central token pair and:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE="$HOME/.repos-gpg/scylladb/java-driver/signing-private.asc"
export RELEASE_GPG_PASSPHRASE_FILE="$HOME/.repos-gpg/scylladb/java-driver/passphrase.txt"
export RELEASE_VERSION=4.19.2.4 # use the appropriate four-component driver version
export RELEASE_BASE_COMMIT="$(git rev-parse HEAD)"
make release-prepare
python3 scripts/release/signing.py verify-prepared
make release-dry-run
```

Run credential-free helper verification with:

```sh
python3 scripts/release/guard.py check
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
```

## Cutover and dry runs

Each branch serializes its own releases; the two lines can validate independently.
Pause release dispatches during cutover. Provision the protected environment, merge both
line-specific workflow changes, and replace the Central placeholders. Run an approved
`dry-run` on each branch. Dry runs **upload unpublished Central deployments**. The workflow
verifies every bundle signature and the exact module set, confirms Central validation, and
deletes the unpublished deployment. Download `verified-central-bundle` and the publication
report in the Maven logs for independent inspection. Dry runs push no Git refs and publish
no docs. See
[Central publishing behavior](https://central.sonatype.org/publish/publish-portal-maven/).

After both dry runs pass, remove repository-scoped `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`,
`SONATYPE_TOKEN_USERNAME`, and `SONATYPE_TOKEN_PASSWORD`. Audit all workflow and external
consumers of `OSSRH_USERNAME`/`OSSRH_PASSWORD` before deleting them. Keep `CODECOV_TOKEN`.
Revoke the retired Central token only after checking other repositories and consumers.
Resume release dispatches. Confirm the first real release publishes the expected module
set, signed commits and tag, and docs. Branch releases push explicitly to
`HEAD:refs/heads/scylla-4.x` or `HEAD:refs/heads/scylla-3.x` only after the Portal API
confirms `PUBLISHED`. Maven always uploads in manual mode;
`publication.py` verifies signatures, module coordinates, and deployment identity before
requesting a real publication. It rechecks blockers and branch movement after upload,
then waits for completion before the SCM push and docs job.
Historical targets never push refs or publish docs.

## Partial release recovery and key rotation

If validation or approval fails, fix the cause and start a new dispatch. If preparation
fails, inspect sanitized logs and the baseline/commits/tag before retrying; discard only
unpublished local state in a disposable checkout. Never move a published release tag.

If Central upload/validation fails, inspect the deployment status and module set before
retrying. Delete an unpublished deployment when appropriate. If some artifacts are already
public, Maven Central versions are immutable: do not blindly rerun or overwrite them.
Download `prepared-release-state`: its Git bundle retains the original signed commits
and tag, with the matching public certificate. In a clone containing the original
baseline, run `git bundle verify prepared-release.bundle` and fetch its HEAD and tag
into recovery refs. Verify against the original baseline and
reconcile the actual Central state first. If publication succeeded but SCM push failed,
verify and push that exact signed history after reconciling branch advancement. Do not
prepare another release under the same version. Run docs publication only after the SCM
release state is correct. Retain sanitized logs and deployment identifiers for recovery.

For rotation, pause releases, export a new protected signing subkey without the primary
secret, update both pinned fingerprints and the public certificate on both branches, and
replace the environment key/passphrase together. Register the publisher identity on GitHub
and publish the certificate to the public keyserver required by Central. Repeat disposable
key tests and approved dry runs before resuming. Preserve old certificates, published tags,
and revocation records so consumers can verify historical artifacts.
