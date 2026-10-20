"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import signing


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="java-signing-fixture-")
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.password = "test-only-release-passphrase"
        def gpg(*args):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=cls.password + "\n", capture_output=True, text=True, check=True,
            )
            return result.stdout
        gpg("--quick-generate-key", "Release Test <release@example.invalid>", "rsa2048", "cert", "1d")
        cls.primary = [line.split(":")[9] for line in
                       gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.primary, "rsa2048", "sign", "1d")
        cls.subkey = [line.split(":")[9] for line in
                      gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][1]
        cls.public = cls.directory / "public.asc"
        cls.public.write_text(gpg("--armor", "--export", cls.primary))
        cls.private = cls.directory / "subkey.asc"
        cls.private.write_text(gpg("--armor", "--export-secret-subkeys", cls.primary))
        cls.full = cls.directory / "full.asc"
        cls.full.write_text(gpg("--armor", "--export-secret-keys", cls.primary))

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["gpgconf", "--homedir", str(cls.keyring), "--kill", "gpg-agent"],
                       capture_output=True, check=True)
        cls.fixture.cleanup()

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="java-signing-test-") as temp:
            directory = Path(temp)
            keyring = directory / "gnupg"
            keyring.mkdir(mode=0o700)
            env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
            env.update(GNUPGHOME=str(keyring), RUNNER_TEMP=str(directory),
                       GITHUB_ENV=str(directory / "github-env"), GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=str(directory / "initial-gitconfig"),
                       MAVEN_GPG_PASSPHRASE=self.password if password is None else password)
            (directory / "initial-gitconfig").write_text("")
            subprocess.run(["gpg", "--no-options", "--homedir", str(keyring), "--batch",
                            "--pinentry-mode", "loopback", "--passphrase-fd", "0", "--import",
                            str(self.full if full else self.private)], input=self.password + "\n",
                           capture_output=True, text=True, env=env, check=True)
            try:
                with patch.dict(os.environ, env, clear=True), \
                        patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                       PUBLIC_KEY=self.public):
                    repo = directory / "source"
                    signing.run(["git", "init", "--quiet", str(repo)])
                    os.chdir(repo)
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    yield directory
            finally:
                os.chdir(original)
                subprocess.run(["gpgconf", "--homedir", str(keyring), "--kill", "gpg-agent"],
                               capture_output=True, check=True)

    def configured(self):
        values = signing.configure()
        os.environ.update(values)
        self.assertEqual(Path(values["RELEASE_GPG_PASSPHRASE_FILE"]).stat().st_mode & 0o777, 0o600)
        self.assertNotIn(self.password, Path(os.environ["GITHUB_ENV"]).read_text())
        return values

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        # Maven gitexe creates annotated tags with -F rather than an explicit -s.
        Path("message").write_text("Release 1.0\n")
        signing.run(["git", "tag", "-F", "message", "1.0"])
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Next development"])

    def test_signed_release_and_cloned_checkout(self):
        with self.repository() as directory:
            values = self.configured()
            self.assertIn("-Dgpg.keyname=" + self.subkey + "!", values["MAVEN_OPTS"])
            self.release()
            signing.verify_release("1.0")
            # release:perform makes a separate clone: signing must survive that boundary.
            clone = directory / "perform-clone"
            signing.run(["git", "clone", "--quiet", "--local", ".", str(clone)])
            signing.run(["git", "-C", str(clone), "commit", "--quiet", "--allow-empty",
                         "-m", "Clone signing"])
            status = signing.run(["git", "-C", str(clone), "-c", "gpg.program=gpg",
                                  "verify-commit", "--raw", "HEAD"],
                                 signing.public_environment(Path(values["RELEASE_SIGNING_DIR"])))
            signing.verify_status(status)
            signing.cleanup()
            self.assertFalse(Path(values["RELEASE_SIGNING_DIR"]).exists())

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"):
                signing.configure()

    def test_unsigned_release_commit_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                         "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_unsigned_release_tag_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "tag", "-d", "1.0"])
            signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m", "Unsigned",
                         "1.0", "HEAD~1"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_tag_at_unrelated_commit_rejected(self):
        with self.repository():
            self.configured()
            signing.run(["git", "tag", "-m", "Old commit", "1.0"])
            signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
            with self.assertRaisesRegex(signing.SigningError, "newly signed"):
                signing.verify_release("1.0")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        valid = "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY
        signing.verify_status(valid)
        for status in [valid.replace(signing.SIGNING, "0" * 40),
                       valid.replace(signing.PRIMARY, "0" * 40), valid + "\n" + valid]:
            with self.assertRaises(signing.SigningError):
                signing.verify_status(status)


if __name__ == "__main__":
    unittest.main()
