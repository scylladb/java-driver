"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager, redirect_stderr
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import signing

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_PRIMARY = signing.PRIMARY
REPOSITORY_SIGNING = signing.SIGNING


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="repo-signing-fixture-")
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.password = "test-only-release-passphrase"
        def gpg(*args):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=cls.password + "\n", capture_output=True, text=True, check=True, timeout=60,
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
                       capture_output=True, check=True, timeout=60)
        cls.fixture.cleanup()

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-test-") as temp:
            directory = Path(temp)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "GITHUB_", "RELEASE_", "MAKE"))
                   and key not in ("MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE")}
            env.update(RUNNER_TEMP=str(directory), GITHUB_ENV=str(directory / "github-env"),
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(directory / "gitconfig"),
                       RELEASE_GPG_PRIVATE_KEY_FILE=str(self.full if full else self.private),
                       RELEASE_GPG_PASSPHRASE=self.password if password is None else password)
            Path(env["GIT_CONFIG_GLOBAL"]).write_text("")
            with patch.dict(os.environ, env, clear=True), \
                    patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                   PUBLIC_KEY=self.public):
                repo = directory / "source"
                signing.run(["git", "init", "--quiet", str(repo)])
                os.chdir(repo)
                try:
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    yield directory
                finally:
                    os.chdir(original)

    @contextmanager
    def session(self):
        with signing.signing_session() as env, patch.dict(os.environ, env, clear=True):
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY", env)
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY_FILE", env)
            yield env

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        signing.run(["git", "tag", "-s", "-m", "Release 1.0", "v1.0.0"])

    def test_repository_setup_failure_restores_working_directory(self):
        original = Path.cwd()
        real_run = signing.run
        def fail_commit(command, env=None):
            if "commit" in command:
                raise signing.SigningError("fixture commit failed")
            return real_run(command, env)
        with patch.object(signing, "run", side_effect=fail_commit):
            with self.assertRaisesRegex(signing.SigningError, "fixture commit failed"):
                with self.repository():
                    self.fail("Initial commit failure accepted")
        self.assertEqual(Path.cwd(), original)
        with self.repository():
            self.assertTrue(Path(".git").is_dir())

    def test_signed_commit_and_tag_verify_after_session(self):
        with self.repository() as directory:
            # Existing checkout settings must not override the session.
            signing.run(["git", "config", "user.signingkey", "invalid-key"])
            config = Path(".git/config").read_bytes()
            with self.session():
                self.release()
            signing.verify_commit("HEAD")
            signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"), self.session():
                self.fail("Empty passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError), self.session():
                self.fail("Wrong passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"), self.session():
                self.fail("Primary private key accepted")

    def test_unprotected_export_rejected_after_native_import(self):
        with self.repository() as directory:
            home = directory / "unprotected"
            home.mkdir(mode=0o700)
            env = {**os.environ, "GNUPGHOME": str(home)}
            def unprotected_gpg(*args):
                return signing.gpg("--pinentry-mode", "loopback", "--passphrase", "", *args, env=env)
            try:
                unprotected_gpg("--quick-generate-key", "Unprotected Test <test@example.invalid>",
                                "rsa2048", "cert", "1d")
                primary = [row.split(":")[9] for row in
                           unprotected_gpg("--with-colons", "--list-keys").splitlines()
                           if row.startswith("fpr:")][0]
                unprotected_gpg("--quick-add-key", primary, "rsa2048", "sign", "1d")
                subkey = [row.split(":")[9] for row in
                          unprotected_gpg("--with-colons", "--list-keys").splitlines()
                          if row.startswith("fpr:")][1]
                private = unprotected_gpg("--armor", "--export-secret-subkeys", primary)
                public = directory / "unprotected-public.asc"
                public.write_text(unprotected_gpg("--armor", "--export", primary))
            finally:
                signing.run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"])
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            os.environ["RELEASE_GPG_PRIVATE_KEY"] = private
            # Any nonempty passphrase would sign this export without the post-use check.
            os.environ["RELEASE_GPG_PASSPHRASE"] = "unrelated-test-passphrase"
            with patch.multiple(signing, PRIMARY=primary, SIGNING=subkey, PUBLIC_KEY=public), \
                    self.assertRaisesRegex(signing.SigningError, "protected subkey"), self.session():
                self.fail("Unprotected subkey export accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_unsigned_commit_and_tag_rejected(self):
        with self.repository():
            with self.session():
                signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                             "--allow-empty", "-m", "Unsigned"])
                signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m",
                             "Unsigned", "v1.0.0"])
            with self.assertRaises(signing.SigningError):
                signing.verify_commit("HEAD")
            with self.assertRaises(signing.SigningError):
                signing.verify_tag("v1.0.0")

    def test_tag_at_unexpected_commit_rejected(self):
        with self.repository():
            with self.session():
                self.release()
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Later"])
            with self.assertRaisesRegex(signing.SigningError, "unexpected commit"):
                signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    def test_batch_signing_reuse_and_tamper_rejection(self):
        with self.repository(), self.session():
            files = [Path("package.jar"), Path("package.pom"), Path("SHA256SUMS")]
            for artifact in files:
                artifact.write_bytes(b"release package")
            signing.sign_files(files)
            signatures = [Path(str(artifact) + ".asc").read_bytes() for artifact in files]
            signing.sign_files(files)
            self.assertEqual([Path(str(artifact) + ".asc").read_bytes() for artifact in files], signatures)
            files[0].write_bytes(b"changed package")
            with self.assertRaises(signing.SigningError):
                signing.sign_files(files)

    def test_direct_sign_files_imports_credentials_and_preserves_verification_only_use(self):
        with self.make_repository() as directory:
            default_home = directory / "empty-default-keyring"
            default_home.mkdir(mode=0o700)
            os.environ["GNUPGHOME"] = str(default_home)
            files = [Path("package.jar"), Path("package.pom")]
            for artifact in files:
                artifact.write_bytes(b"release package")
            command = [sys.executable, "scripts/release/signing.py", "sign-files"]
            def invoke(*artifacts):
                result = subprocess.run(command + list(map(str, artifacts)), capture_output=True,
                                        text=True, timeout=60)
                self.assertNotIn(self.password, result.stdout + result.stderr)
                self.assertNotIn("PRIVATE KEY", result.stdout + result.stderr)
                return result
            result = invoke(files[0])
            self.assertEqual(result.returncode, 0, result.stderr)
            first_signature = Path(str(files[0]) + ".asc").read_bytes()
            # Inline export input also opens a session, preserving existing signatures.
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            os.environ["RELEASE_GPG_PRIVATE_KEY"] = self.private.read_text()
            result = invoke(*files)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(Path(str(files[0]) + ".asc").read_bytes(), first_signature)
            signatures = [Path(str(artifact) + ".asc").read_bytes() for artifact in files]
            del os.environ["RELEASE_GPG_PRIVATE_KEY"]
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            result = invoke(*files)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual([Path(str(artifact) + ".asc").read_bytes() for artifact in files], signatures)
            files[0].write_bytes(b"tampered release")
            self.assertNotEqual(invoke(*files).returncode, 0)
            self.assertFalse(list(default_home.iterdir()))
            self.assertFalse(list(directory.glob("release-signing-*")))

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process observation required")
    def test_exec_termination_stops_child_group_before_keyring_and_agent_cleanup(self):
        self.check_exec_termination("active")

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process observation required")
    def test_exec_startup_interruption_keeps_child_owned_until_cleanup(self):
        self.check_exec_termination("startup")

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process observation required")
    def test_normal_teardown_interruption_finishes_agent_and_keyring_cleanup(self):
        self.check_exec_termination("teardown")

    def check_exec_termination(self, window):
        def running(pid):
            try:
                return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
            except FileNotFoundError:
                return False
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            with self.subTest(signum=signum), self.make_repository() as directory:
                ready, stopped = directory / "ready", directory / "stopped"
                started = directory / "started"
                agent_shutdown = directory / "agent-shutdown"
                descendant_ready = directory / "descendant-ready"
                descendant = (
                    "import os,signal,time\nfrom pathlib import Path\n"
                    "for s in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT): signal.signal(s,signal.SIG_IGN)\n"
                    f"Path({str(descendant_ready)!r}).write_text(str(os.getpid()))\n"
                    "time.sleep(60)\n")
                script = (
                    "import json,os,signal,subprocess,sys,time\nfrom pathlib import Path\n"
                    "os.close(1)\nos.close(2)\n"
                    f"Path({str(started)!r}).write_text(str(os.getpid()))\n"
                    f"child=subprocess.Popen([sys.executable,'-c',{descendant!r}])\n"
                    "home=Path(os.environ['GNUPGHOME'])\n"
                    "agent=subprocess.run(['gpg-connect-agent','--no-autostart','--homedir',str(home),"
                    "'GETINFO pid','/bye'],capture_output=True,text=True,check=True).stdout\n"
                    "agent=int(next(line[2:] for line in agent.splitlines() if line.startswith('D ')))\n"
                    "def interrupted(s,frame):\n"
                    f" Path({str(stopped)!r}).write_text(json.dumps({{'home_exists':home.exists(),"
                    "'private_exists':(home/'private-keys-v1.d').is_dir()}))\n"
                    " raise SystemExit(128+s)\n"
                    "for s in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT): signal.signal(s,interrupted)\n"
                    "deadline=time.monotonic()+3\n"
                    f"while not Path({str(descendant_ready)!r}).exists() and time.monotonic()<deadline: time.sleep(0.01)\n"
                    f"Path({str(ready)+'.tmp'!r}).write_text(json.dumps({{'pid':os.getpid(),'child':child.pid,"
                    "'home':str(home),'agent':agent,'blocked':sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK,[]))}))\n"
                    f"os.rename({str(ready)+'.tmp'!r},{str(ready)!r})\n"
                    + ("child.kill()\nchild.wait()\n"
                       f"Path({str(stopped)!r}).write_text(json.dumps({{'home_exists':home.exists(),"
                       "'private_exists':(home/'private-keys-v1.d').is_dir()}))\n" if window == "teardown"
                       else "time.sleep(60)\n"))
                runner = (
                    "import os,signal,sys,time\n"
                    "sys.path.insert(0,'scripts/release')\nimport signing\n"
                    f"sys.argv=['signing.py','exec',sys.executable,'-c',{script!r}]\n")
                if window == "startup":
                    runner += (
                        "original=signing.subprocess.Popen\n"
                        "def launching(command,*args,**kwargs):\n"
                        " process=original(command,*args,**kwargs)\n"
                        " if command[0]==sys.executable and command[1]=='-c':\n"
                        "  deadline=time.monotonic()+5\n"
                        f"  while not os.path.exists({str(ready)!r}) and time.monotonic()<deadline: time.sleep(0.01)\n"
                        f"  os.kill(os.getpid(),{int(signum)})\n"
                        " return process\n"
                        "signing.subprocess.Popen=launching\n")
                elif window == "teardown":
                    runner += (
                        "finished=False\noriginal_execute=signing.execute\noriginal_run=signing.run\n"
                        "def executing(*args):\n"
                        " global finished\n"
                        " status=original_execute(*args)\n finished=True\n return status\n"
                        "def shutdown(command,*args,**kwargs):\n"
                        " if finished and command[0]=='gpgconf':\n"
                        f"  os.kill(os.getpid(),{int(signum)})\n"
                        " result=original_run(command,*args,**kwargs)\n"
                        f" if finished and command[0]=='gpgconf': open({str(agent_shutdown)!r},'w').write('completed')\n"
                        " return result\n"
                        "signing.execute=executing\nsigning.run=shutdown\n")
                runner += "raise SystemExit(signing.main())\n"
                process = subprocess.Popen(
                    [sys.executable, "-c", runner],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                observation = None
                try:
                    deadline = time.monotonic() + 10
                    while not ready.exists() and time.monotonic() < deadline and process.poll() is None:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists(), "Signing child did not become ready")
                    self.assertTrue(descendant_ready.exists())
                    observation = json.loads(ready.read_text())
                    self.assertEqual(observation["blocked"],
                                     sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])))
                    if window == "active":
                        self.assertTrue(running(observation["agent"]))
                        self.assertTrue(Path(observation["home"]).exists())
                        process.send_signal(signum)
                    stdout, stderr = process.communicate(timeout=8)
                    self.assertEqual(process.returncode, 128 + signum, {
                        "child_running": running(observation["pid"]),
                        "descendant_running": running(observation["child"]),
                        "agent_running": running(observation["agent"]),
                        "keyring_exists": Path(observation["home"]).exists()})
                    self.assertNotIn(self.password.encode(), stdout + stderr)
                    self.assertTrue(stopped.exists(), {
                        "child_running": running(observation["pid"]),
                        "descendant_running": running(observation["child"]),
                        "agent_running": running(observation["agent"]),
                        "keyring_exists": Path(observation["home"]).exists()})
                    self.assertEqual(json.loads(stopped.read_text()),
                                     {"home_exists": True, "private_exists": True})
                    self.assertFalse(running(observation["pid"]))
                    self.assertFalse(running(observation["child"]))
                    if window == "teardown":
                        self.assertTrue(agent_shutdown.exists(), "Interruption skipped normal agent shutdown")
                    deadline = time.monotonic() + 3
                    while running(observation["agent"]) and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertFalse(running(observation["agent"]))
                    self.assertFalse(list(directory.glob("release-signing-*")))
                finally:
                    # Also stop children and agents if readiness or any assertion fails.
                    if observation is None and ready.exists():
                        observation = json.loads(ready.read_text())
                    child_pid = observation["pid"] if observation else int(started.read_text()) if started.exists() else None
                    groups = [process.pid] + ([child_pid] if child_pid else [])
                    for group in groups:
                        try:
                            os.killpg(group, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    pids = ([child_pid] if child_pid else []) + (
                        [int(descendant_ready.read_text())] if descendant_ready.exists() else [])
                    for pid in pids:
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    for home in directory.glob("release-signing-*/keyring"):
                        subprocess.run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"],
                                       capture_output=True, timeout=10)
                    if observation and running(observation["agent"]):
                        os.kill(observation["agent"], signal.SIGKILL)
                    process.communicate(timeout=5)

    def test_exec_cleans_up_after_command_failure(self):
        with self.repository() as directory:
            config = Path(".git/config").read_bytes()
            with patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", "raise SystemExit(7)"]):
                self.assertEqual(signing.main(), 7)
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_exec_accepts_passphrase_file(self):
        with self.repository() as directory:
            password = directory / "passphrase"
            # Literal fixture data belongs only to the disposable test key.
            signing.write_private(password, "test-only-release-passphrase\n")
            os.environ["RELEASE_GPG_PASSPHRASE_FILE"] = str(password)
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            with patch("sys.argv", ["signing.py", "exec", "git", "commit", "--quiet",
                                    "--allow-empty", "-m", "Signed from file credentials"]):
                self.assertEqual(signing.main(), 0)
            signing.verify_commit("HEAD")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_fixture_isolates_inherited_credentials_and_fallback_precedence(self):
        inherited = {"RELEASE_GPG_PASSPHRASE_FILE": "/nonexistent/inherited-passphrase",
                     "RELEASE_GPG_PRIVATE_KEY": "inherited-private-key",
                     "RELEASE_GPG_PASSPHRASE": "inherited-passphrase",
                     "MAVEN_GPG_PASSPHRASE": "inherited-maven-passphrase",
                     "PGP_PASSPHRASE": "inherited-pgp-passphrase"}
        with patch.dict(os.environ, inherited), self.repository():
            for key in ("RELEASE_GPG_PASSPHRASE_FILE", "RELEASE_GPG_PRIVATE_KEY",
                        "MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE"):
                self.assertNotIn(key, os.environ)
            self.assertEqual(signing.passphrase_value(os.environ), self.password)
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            os.environ["PGP_PASSPHRASE"] = self.password
            with self.session():
                signing.run(["git", "commit", "--allow-empty", "-qm", "PGP fallback"])
            signing.verify_commit("HEAD")
        for values, expected in [({"PGP_PASSPHRASE": "pgp"}, "pgp"),
                                 ({"PGP_PASSPHRASE": "pgp", "MAVEN_GPG_PASSPHRASE": "maven"}, "maven"),
                                 ({"PGP_PASSPHRASE": "pgp", "MAVEN_GPG_PASSPHRASE": "maven",
                                   "RELEASE_GPG_PASSPHRASE": "release"}, "release")]:
            self.assertEqual(signing.passphrase_value(values), expected)

    def test_cleanup_removes_files_even_when_agent_shutdown_fails(self):
        with self.repository() as directory:
            real_run = signing.run
            def fail_shutdown(command, env=None):
                real_run(command, env)
                raise signing.SigningError("shutdown failed")
            with self.assertRaisesRegex(signing.SigningError, "shutdown"), \
                    patch.object(signing, "run", wraps=real_run) as command:
                with self.session():
                    command.side_effect = fail_shutdown
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_git_wrapper_reads_current_passphrase_after_agent_reset(self):
        with self.repository(), self.session():
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = "incorrect-fixture-passphrase"
            with self.assertRaises(signing.SigningError):
                signing.run(["git", "commit", "--allow-empty", "-qm", "Must fail"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = self.password
            signing.run(["git", "commit", "--allow-empty", "-qm", "Correct passphrase"])
            signing.verify_commit("HEAD")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        valid = "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY
        good = "[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Release Test"
        accepted = good + "\n" + valid
        signing.verify_status(accepted)
        for status in [valid, good, accepted.replace(signing.SIGNING, "0" * 40),
                       accepted.replace(signing.PRIMARY, "0" * 40), accepted + "\n" + valid,
                       accepted + "\n" + good, good.replace(signing.SIGNING[-16:], "0" * 16) + "\n" + valid]:
            with self.subTest(status=status), self.assertRaises(signing.SigningError):
                signing.verify_status(status)
        for rejected in ["EXPKEYSIG", "REVKEYSIG", "EXPSIG", "BADSIG", "ERRSIG",
                         "FAILURE", "ERROR", "KEYEXPIRED", "KEYREVOKED", "SIGEXPIRED"]:
            with self.subTest(rejected=rejected), self.assertRaises(signing.SigningError):
                signing.verify_status(accepted + "\n[GNUPG:] " + rejected + " " + signing.SIGNING[-16:])

    def test_signed_maven_history_and_cloned_checkout(self):
        with self.repository() as directory:
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            os.environ["MAVEN_OPTS"] = "-Xmx512m"
            os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="core.quotePath",
                              GIT_CONFIG_VALUE_0="false")
            with self.session() as env:
                self.assertIn("-Dgpg.keyname=" + self.subkey + "!", env["MAVEN_OPTS"])
                self.assertIn("-Xmx512m", env["MAVEN_OPTS"])
                self.assertEqual(env["MAVEN_GPG_PASSPHRASE"], self.password)
                self.assertEqual(signing.run(["git", "config", "core.quotePath"]).strip(), "false")
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
                # Maven gitexe uses an annotation file without an explicit -s.
                Path("message").write_text("Release 1.0.0\n")
                signing.run(["git", "tag", "-F", "message", "1.0.0"])
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Next development"])
                clone = directory / "perform clone"
                signing.run(["git", "clone", "--quiet", "--local", ".", str(clone)])
                signing.run(["git", "-C", str(clone), "commit", "--quiet", "--allow-empty",
                             "-m", "Clone signing"])
                with signing.public_environment() as public:
                    signing.verify_status(signing.run(
                        ["git", "-C", str(clone), "-c", "gpg.program=gpg",
                         "verify-commit", "--raw", "HEAD"], public))
            Path("release.properties").write_text("scm.tagNameFormat=@{project.version}\nscm.tag=1.0.0\n")
            signing.verify_prepared(baseline)
            with patch.dict(os.environ, RELEASE_BASE_COMMIT=baseline):
                signing.verify_prepared()
                with patch("sys.argv", ["signing.py", "verify-release", "1.0.0"]):
                    self.assertEqual(signing.main(), 0)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())
            # Development commits belong to the verified release history too.
            signing.run(["git", "-c", "user.name=Initial", "-c", "user.email=initial@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_prepared(baseline)

    def test_prepared_metadata_requires_exactly_one_tag(self):
        with self.repository():
            for metadata in ["scm.tagNameFormat=@{project.version}\n",
                             "scm.tag=1.0.0\nscm.tag=2.0.0\n"]:
                with self.subTest(metadata=metadata):
                    Path("release.properties").write_text(metadata)
                    with self.assertRaisesRegex(signing.SigningError, "exactly one prepared Maven release tag"):
                        signing.verify_prepared("HEAD")

    def test_prepared_verification_requires_original_boundary(self):
        with self.repository():
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            with self.session():
                signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--allow-empty",
                             "-qm", "Unsigned preparation commit"])
                self.release()
                signing.run(["git", "commit", "--allow-empty", "-qm", "Next development"])
            Path("release.properties").write_text("scm.tag=v1.0.0\n")
            with self.assertRaisesRegex(signing.SigningError, "pre-prepare RELEASE_BASE_COMMIT"):
                signing.verify_prepared()
            with patch.dict(os.environ, RELEASE_BASE_COMMIT=baseline), self.assertRaises(signing.SigningError):
                signing.verify_prepared()

    def test_maven_history_rejects_unsigned_release_commit(self):
        with self.repository():
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            with self.session():
                signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                             "--allow-empty", "-m", "Unsigned release"])
                signing.run(["git", "tag", "-s", "-m", "Release", "1.0.0"])
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Next development"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0.0", baseline)

    def test_maven_history_rejects_unsigned_tag_and_old_tag(self):
        with self.repository():
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            with self.session():
                signing.run(["git", "tag", "-s", "-m", "Old commit", "old"])
                self.release()
                signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m", "Unsigned", "1.0.0"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0.0", baseline)
            with self.assertRaisesRegex(signing.SigningError, "newly signed"):
                signing.verify_release("old", baseline)
            with self.assertRaisesRegex(signing.SigningError, "No release commits"):
                signing.verify_release("old", "HEAD")

    @contextmanager
    def make_repository(self):
        with self.repository() as directory:
            for name in ["Makefile", "pom.xml", ".github/workflows/release.yml",
                         "scripts/release/signing.py", "scripts/release/guard.py", "scripts/release/target.py"]:
                target = Path(name)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / name, target)
            helper = Path("scripts/release/signing.py")
            helper.write_text(helper.read_text().replace(REPOSITORY_PRIMARY, self.primary)
                              .replace(REPOSITORY_SIGNING, self.subkey))
            shutil.copyfile(self.public, "scripts/release/publisher.asc")
            tools = directory / "bin"
            tools.mkdir()
            maven = tools / "mvn"
            maven.write_text("#!/usr/bin/env python3\n" + "\n".join([
                "import os, pathlib, subprocess, sys",
                "assert '-Dgpg.keyname=' + os.environ['TEST_SIGNING_KEY'] + '!' in os.environ['MAVEN_OPTS']",
                "assert 'RELEASE_GPG_PRIVATE_KEY' not in os.environ",
                "assert 'RELEASE_GPG_PRIVATE_KEY_FILE' not in os.environ",
                "assert os.environ.get('GIT_CONFIG_COUNT')",
                "if 'TEST_MAVEN_OPTS' in os.environ:",
                "    assert os.environ['MAVEN_OPTS'] == os.environ['TEST_MAVEN_OPTS'] + ' -Dgpg.keyname=' + os.environ['TEST_SIGNING_KEY'] + '!'",
                "def git(*args): subprocess.run(['git', *args], check=True, capture_output=True)",
                "print('test credential: ' + os.environ['MAVEN_GPG_PASSPHRASE'])",
                "if os.environ.get('FAIL_MAVEN'): sys.exit(17)",
                "if 'release:prepare' in sys.argv:",
                "    git('commit', '--allow-empty', '-qm', 'Release')",
                "    pathlib.Path('tag-message').write_text('Release 1.0.0')",
                "    git('tag', '-F', 'tag-message', '1.0.0')",
                "    git('commit', '--allow-empty', '-qm', 'Next development')",
                "    pathlib.Path('release.properties').write_text('scm.tag=1.0.0\\n')",
                "else:",
                "    git('clone', '--quiet', '--local', '.', 'perform-clone')",
                "    git('-C', 'perform-clone', 'commit', '--allow-empty', '-qm', 'Clone signing')",
            ]) + "\n")
            maven.chmod(0o700)
            os.environ.update(PATH=str(tools) + os.pathsep + os.environ["PATH"],
                              GITHUB_ACTIONS="true", GITHUB_ACTOR="release-operator",
                              GITHUB_TRIGGERING_ACTOR="rerun-operator",
                              SONATYPE_TOKEN_USERNAME="synthetic-user", SONATYPE_TOKEN_PASSWORD="synthetic-password",
                              TEST_SIGNING_KEY=self.subkey, RELEASE_LOG_DIR=str(directory / "logs"))
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Initial", "-c", "user.email=initial@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Fixture"])
            yield directory

    def test_make_prepares_verifies_and_performs_in_separate_sessions(self):
        with self.make_repository() as directory:
            config = Path(".git/config").read_bytes()
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            options = "-Xmx512m -Dliteral=$HOME -Dexpr=$(shell touch make-expression-ran) -Dbrace=${HOME}"
            os.environ.update(MAVEN_OPTS=options, TEST_MAVEN_OPTS=options)
            signing.run(["make", "release-prepare",
                         "RELEASE_GPG_PRIVATE_KEY_FILE=" + str(self.private)])
            self.assertFalse(list(directory.glob("release-signing-*")))
            signing.verify_prepared(baseline)
            os.environ["RELEASE_BASE_COMMIT"] = baseline
            signing.run(["make", "release-dry-run"])
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertEqual(Path(".git/config").read_bytes(), config)
            with signing.public_environment() as public:
                signing.verify_status(signing.run(
                    ["git", "-C", "perform-clone", "-c", "gpg.program=gpg",
                     "verify-commit", "--raw", "HEAD"], public))
            logs = (directory / "logs/stdout.log").read_text()
            self.assertNotIn(self.password, logs)
            self.assertIn("[REDACTED]", logs)
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())
            self.assertFalse(Path("make-expression-ran").exists())

    def test_make_cleans_up_failed_maven_session(self):
        with self.make_repository() as directory:
            os.environ["FAIL_MAVEN"] = "1"
            result = subprocess.run(["make", "release-prepare"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Error 17", result.stderr)
            self.assertNotIn(self.password, result.stdout + result.stderr)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_make_perform_rejects_invalid_history_before_importing_key(self):
        with self.make_repository() as directory:
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            result = subprocess.run(["make", "release"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("verify-prepared", result.stderr)
            self.assertNotIn("protected signing-subkey export", result.stderr)
            self.assertFalse(list(directory.glob("release-signing-*")))

    @unittest.skipUnless(shutil.which("mvn"), "Maven required")
    def test_maven_plugin_signs_without_cached_passphrase(self):
        with self.repository(), self.session():
            Path("pom.xml").write_text(
                '<project><modelVersion>4.0.0</modelVersion><groupId>test</groupId>'
                '<artifactId>release-signing-test</artifactId><version>1.0.0</version>'
                '<packaging>pom</packaging></project>')
            # Maven must supply its environment passphrase after agent expiry.
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            result = subprocess.run(
                ["mvn", "-B", "-ntp", "org.apache.maven.plugins:maven-gpg-plugin:3.2.4:sign",
                 "-Dgpg.bestPractices=true"], capture_output=True, text=True, timeout=180)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            signatures = list(Path("target").rglob("*.asc"))
            self.assertEqual(len(signatures), 1)
            with signing.public_environment() as public:
                signing.verify_status(signing.gpg("--status-fd", "1", "--verify", signatures[0],
                                                  "pom.xml", env=public))


class DiagnosticTests(unittest.TestCase):
    def test_pending_interruption_does_not_launch_command(self):
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            with self.subTest(signum=signum), patch.object(signing.subprocess, "Popen") as spawn:
                with self.assertRaises(SystemExit) as stopped:
                    with signing.session_signals() as interrupted:
                        interrupted(signum, None)
                        signing.execute(["test-command"], {})
                self.assertEqual(stopped.exception.code, 128 + signum)
                spawn.assert_not_called()

    def test_already_exited_child_preserves_interruption_status(self):
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            with self.subTest(signum=signum), patch.object(signing.subprocess, "Popen") as spawn, \
                    patch.object(signing.os, "killpg", side_effect=ProcessLookupError):
                spawn.return_value.wait.side_effect = [SystemExit(128 + signum), 0]
                with self.assertRaises(SystemExit) as interrupted:
                    signing.execute(["test-command"], {})
                self.assertEqual(interrupted.exception.code, 128 + signum)

    def test_parsed_output_excludes_warnings_but_raw_git_keeps_status(self):
        result = subprocess.CompletedProcess([], 0, "parsed-value\n", "warning\n")
        with patch.object(subprocess, "run", return_value=result):
            for command in [["git", "rev-parse", "HEAD"], ["git", "rev-list", "HEAD"],
                            ["git", "cat-file", "-t", "HEAD"],
                            ["gpg", "--status-fd", "1", "--verify", "artifact.sig"]]:
                with self.subTest(command=command):
                    self.assertEqual(signing.run(command), result.stdout)
            for kind in ("commit", "tag"):
                self.assertEqual(signing.run(["git", "verify-" + kind, "--raw", "HEAD"]),
                                 result.stdout + result.stderr)

    def test_failed_commands_identify_operations_without_unsafe_values(self):
        sentinel = "DO-NOT-DISCLOSE"
        result = subprocess.CompletedProcess([], 17, sentinel, sentinel)
        with patch.object(subprocess, "run", return_value=result):
            with self.assertRaisesRegex(signing.SigningError, r"Git verify-commit failed \(exit status 17\)") as error:
                signing.run(["git", "verify-commit", sentinel])
            self.assertNotIn(sentinel, str(error.exception))
            with self.assertRaisesRegex(signing.SigningError, r"GPG key import failed \(exit status 17\)") as error:
                signing.credential_gpg(["--import", sentinel], payload=sentinel,
                                       env={"RELEASE_GPG_PASSPHRASE": sentinel})
            self.assertNotIn(sentinel, str(error.exception))

    def test_command_start_and_timeout_errors_withhold_payloads(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (subprocess.TimeoutExpired("DO-NOT-DISCLOSE", 60,
                                                             output="DO-NOT-DISCLOSE"), "timed out")]:
            with self.subTest(category=category), patch.object(subprocess, "run", side_effect=failure):
                with self.assertRaises(signing.SigningError) as error:
                    signing.gpg("--verify", "DO-NOT-DISCLOSE")
                self.assertIn("GPG signature verification", str(error.exception))
                self.assertIn(category, str(error.exception))
                self.assertNotIn("DO-NOT-DISCLOSE", str(error.exception))

    def test_unexpected_failure_identifies_operation_and_safe_category(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (RuntimeError("DO-NOT-DISCLOSE"), "internal error")]:
            output = StringIO()
            with patch("sys.argv", ["signing.py", "verify-commit", "HEAD"]), \
                    patch.object(signing, "verify_commit", side_effect=failure), redirect_stderr(output):
                self.assertEqual(signing.main(), 1)
            self.assertIn("verify-commit failed (" + category + ")", output.getvalue())
            self.assertNotIn("DO-NOT-DISCLOSE", output.getvalue())


class ReleaseUserTests(unittest.TestCase):
    def test_actions_accepts_workflow_actors_without_local_gh_authentication(self):
        with tempfile.TemporaryDirectory() as temp:
            gh = Path(temp) / "gh"
            gh.write_text("#!/bin/sh\nexit 99\n")
            gh.chmod(0o700)
            for actor, triggering_actor in [("release-operator", "release-operator"),
                                            ("scylladb-publisher", "rerun-operator"),
                                            ("release-operator", "")]:
                with self.subTest(actor=actor, triggering_actor=triggering_actor):
                    result = subprocess.run(
                        ["make", "release-check-user"], cwd=ROOT, capture_output=True, text=True, timeout=60,
                        env={**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                             "GITHUB_ACTIONS": "true", "GITHUB_ACTOR": actor,
                             "GITHUB_TRIGGERING_ACTOR": triggering_actor},
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_local_release_commands_automatically_reject_other_users(self):
        with tempfile.TemporaryDirectory() as temp:
            gh = Path(temp) / "gh"
            gh.write_text("#!/bin/sh\nprintf '%s\\n' someone-else\n")
            gh.chmod(0o700)
            for target in ["release-prepare", "release", "release-dry-run", "release-preflight",
                           "_release-prepare", "_release", "_release-dry-run"]:
                with self.subTest(target=target):
                    result = subprocess.run(
                        ["make", target], cwd=ROOT, capture_output=True, text=True, timeout=60,
                        env={**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                             "GITHUB_ACTIONS": "false"},
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Release requires GitHub user scylladb-publisher", result.stderr)
                    self.assertNotIn("signing.py exec", result.stdout)

    def test_local_release_uses_authenticated_gh_user(self):
        with tempfile.TemporaryDirectory() as temp:
            gh = Path(temp) / "gh"
            gh.write_text('#!/bin/sh\nprintf "%s\\n" "$TEST_GH_USER"\n')
            gh.chmod(0o700)
            env = {**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                   "GITHUB_ACTIONS": "false", "TEST_GH_USER": "someone-else"}
            result = subprocess.run(["make", "release-prepare"], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("current: someone-else", result.stderr)
            env["TEST_GH_USER"] = "scylladb-publisher"
            subprocess.run(["make", "release-check-user"], cwd=ROOT, env=env, check=True,
                           capture_output=True, timeout=60)
            gh.write_text("#!/bin/sh\nexit 1\n")
            result = subprocess.run(["make", "release-prepare"], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Run gh auth login or gh auth switch", result.stderr)


if __name__ == "__main__":
    unittest.main()
