# Release signing

The release workflow uses the Java driver's repository-specific publisher certificate:

- Primary: `FA5BF24FD656DB6172B6DEDA92CDA600C75CA820`
- Signing subkey: `6633DD12AC5A67675513E2EA998E34B10468CB5B`
- Identity: `ScyllaDB Publisher (scylladb/java-driver) <publish.code@scylladb.com>`

Both `scylla-4.x` and `scylla-3.x` use this signing subkey for Maven artifacts, release
commits, development-version commits, and annotated release tags. `publisher.asc` is the
public certificate; `signing.py` pins both fingerprints and rejects a different signer.

The existing repository secrets must be updated as a matching pair during activation:

- `GPG_PRIVATE_KEY`: the protected ASCII-armored **signing-subkey export**, with an
  unavailable primary secret, from `signing-private.asc`.
- `GPG_PASSPHRASE`: the corresponding nonempty passphrase.

Keep the full certification-primary backup and revocation certificate out of CI. Register
the public certificate with `scylladb-publisher` and verify the publisher email there before
creating release commits or tags. Publish the public certificate and fingerprints for
package verification, including the public keyserver required by Maven Central. Keep old
public certificates available to verify existing releases; do not replace existing tags.

`setup-java` imports the key into its temporary keyring. Before Maven runs, `signing.py`
checks the exact protected subkey and makes a real test signature with a fresh agent. An
empty or incorrect passphrase stops the job. The script writes a temporary 0600 passphrase
file and creates a Git GPG wrapper using loopback pinentry. A temporary job-specific Git
configuration supplies the publisher identity and signing settings to Maven's separate
`release:perform` checkout too. It does not modify a developer's Git configuration.

The workflow exports the non-secret `gpg.keyname` selection through `MAVEN_OPTS`, which is
inherited by Maven's forked builds. Maven GPG receives its passphrase through
`MAVEN_GPG_PASSPHRASE`, matching the `setup-java` input. Do not put passphrases in Maven
command-line properties or disable `gpg.bestPractices`.

After `release:prepare`, every new commit and the expected release tag are verified in a
public-only keyring, including the exact signing and primary fingerprints. Publication
and SCM push remain behind the existing release gates. The signing passphrase file and
configuration are removed by an `always()` step; `setup-java` cleans up its keyring.

Run the signing and credential-safety tests without production secrets:

```sh
python3 scripts/release/guard.py check
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
```

The tests create disposable protected keys, commits, tags, and a separate clone. They cover
correct, empty, and incorrect passphrases, rejection of a full primary secret, unsigned
commits, the wrong tag target, foreign signers, and ambiguous verification status.
