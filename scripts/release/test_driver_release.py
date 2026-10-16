"""Exercise the 3.x release recipes with a fake Maven executable and dummy secrets."""

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DriverReleaseTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        shutil.copyfile(ROOT / "Makefile", root / "Makefile")
        scripts = root / "scripts/release"
        scripts.mkdir(parents=True)
        shutil.copyfile(ROOT / "scripts/release/guard.py", scripts / "guard.py")
        fake = root / "mvn"
        fake.write_text(
            f"#!{sys.executable}\n"
            "import base64, json, os, sys\n"
            "from pathlib import Path\n"
            "secret = os.environ['MAVEN_GPG_PASSPHRASE']\n"
            "credentials = os.environ['SONATYPE_TOKEN_USERNAME'] + ':' + os.environ['SONATYPE_TOKEN_PASSWORD']\n"
            "encoded = base64.b64encode(credentials.encode()).decode()\n"
            "os.write(1, ('-Dgpg.passphrase=' + secret + '\\n').encode())\n"
            "os.write(2, ('Authorization: Basic ' + encoded + '\\n').encode())\n"
            "Path('invocation.json').write_text(json.dumps({'args': sys.argv[1:], 'options': os.environ.get('MAVEN_OPTS', '')}))\n"
            "sys.exit(int(os.environ.get('FAKE_MAVEN_STATUS', '0')))\n"
        )
        fake.chmod(0o755)
        env = {key: value for key, value in os.environ.items() if key not in (
            "MAVEN_ARGS", "MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS",
            "_JAVA_OPTIONS", "MAVEN_DEBUG_OPTS", "MAVEN_CONFIG", "MAKEFLAGS", "MFLAGS",
        )}
        env.update(
            PATH=str(root) + os.pathsep + os.environ["PATH"],
            MAVEN_GPG_PASSPHRASE="driver-dummy-passphrase",
            SONATYPE_TOKEN_USERNAME="driver-dummy-user",
            SONATYPE_TOKEN_PASSWORD="driver-dummy-password",
            RELEASE_LOG_DIR=str(root / "logs"),
            RELEASE_SKIP_TESTS="false",
        )
        return root, env

    def test_every_release_recipe_captures_both_streams_on_success_and_failure(self):
        for target in ("release-prepare", "release", "release-dry-run"):
            for status in (0, 19):
                with self.subTest(target=target, status=status), tempfile.TemporaryDirectory() as directory:
                    root, env = self.fixture(directory)
                    env["FAKE_MAVEN_STATUS"] = str(status)
                    process = subprocess.run(["make", target], cwd=root, env=env, capture_output=True, timeout=10)
                    self.assertEqual(process.returncode == 0, status == 0)
                    stdout = (root / "logs/stdout.log").read_bytes()
                    stderr = (root / "logs/stderr.log").read_bytes()
                    self.assertIn(b"[REDACTED]", stdout)
                    self.assertIn(b"[REDACTED]", stderr)
                    exposed = process.stdout + process.stderr + stdout + stderr
                    for secret in (
                        env["MAVEN_GPG_PASSPHRASE"], env["SONATYPE_TOKEN_PASSWORD"],
                        base64.b64encode((env["SONATYPE_TOKEN_USERNAME"] + ":" + env["SONATYPE_TOKEN_PASSWORD"]).encode()).decode(),
                    ):
                        self.assertNotIn(secret.encode(), exposed)
                    invocation = json.loads((root / "invocation.json").read_text())
                    self.assertNotIn("-X", invocation["args"])
                    if target == "release-prepare":
                        self.assertIn("-DcheckModificationExcludeList=README.md", invocation["args"])
                    elif target == "release":
                        self.assertIn("-Drelease.autopublish=true", invocation["args"])
                    else:
                        self.assertNotIn("-Drelease.autopublish=true", invocation["args"])

    def test_dry_run_respects_the_explicit_skip_tests_flag(self):
        for value in ("false", "true", "1"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                root, env = self.fixture(directory)
                env["RELEASE_SKIP_TESTS"] = value
                process = subprocess.run(["make", "release-dry-run"], cwd=root, env=env, capture_output=True, timeout=10)
                self.assertEqual(process.returncode, 0, process.stderr.decode())
                invocation = json.loads((root / "invocation.json").read_text())
                self.assertEqual("-DskipTests=true" in invocation["options"], value != "false")

    def test_secret_environment_values_are_not_interpolated_as_shell_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fixture(directory)
            marker = root / "injected"
            env["MAVEN_GPG_PASSPHRASE"] = f"$(touch {marker})"
            process = subprocess.run(["make", "release"], cwd=root, env=env, capture_output=True, timeout=10)
            self.assertEqual(process.returncode, 0, process.stderr.decode())
            self.assertFalse(marker.exists())
            self.assertNotIn(str(marker).encode(), process.stdout + process.stderr)

    def test_target_check_precedes_signing_and_artifacts_follow_scm_push(self):
        text = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(text.index("Preserve release guard"), text.index("Checkout Code One Commit Before"))
        self.assertLess(text.index("Checkout Code One Commit Before"), text.index("Check release credential safety"))
        self.assertLess(text.index("Check release credential safety"), text.index("gpg-private-key:"))
        self.assertLess(text.index("Push changes to SCM"), text.index("Upload sanitized release logs"))
        self.assertIn("if: always()", text)
        self.assertIn("gpg-passphrase-env-var: MAVEN_GPG_PASSPHRASE", text)
        self.assertIn('version="$(python3 "$RELEASE_GUARD" run mvn', text)

    def test_unsupported_checkout_target_fails_before_running_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fixture(directory)
            fake_git = root / "git"
            fake_git.write_text("#!/bin/sh\ntouch unexpected-git-call\nexit 0\n")
            fake_git.chmod(0o755)
            env["RELEASE_TARGET_TAG"] = "scylla-4.x"
            process = subprocess.run(["make", "checkout-one-commit-before"], cwd=root, env=env, capture_output=True, timeout=10)
            self.assertNotEqual(process.returncode, 0)
            self.assertFalse((root / "unexpected-git-call").exists())

    def test_resolver_validates_dispatch_ref_and_preserves_tag_checks(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        step = workflow.split("      - name: Resolve release commit and version\n", 1)[1]
        script = textwrap.dedent(step.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        for target, ref, version, succeeds in (
            ("scylla-3.x", "refs/heads/scylla-3.x", "3.11.5.99", True),
            ("scylla-3.x", "refs/heads/another-3x-branch", "3.11.5.99", False),
            ("3.11.5.99", "refs/heads/scylla-3.x", "3.11.5.99", True),
            ("3.11.5.99", "refs/heads/scylla-3.x", "3.11.5.98", False),
        ):
            with self.subTest(target=target, ref=ref, version=version), tempfile.TemporaryDirectory() as directory:
                root, env = self.fixture(directory)
                fake_git = root / "git"
                fake_git.write_text(
                    f"#!{sys.executable}\nimport sys\nfrom pathlib import Path\n"
                    "with Path('git-calls').open('a') as out: out.write(' '.join(sys.argv[1:]) + '\\n')\n"
                    "if sys.argv[1] == 'rev-parse': print('tag-parent')\n"
                    f"else: print('<project xmlns=\"http://maven.apache.org/POM/4.0.0\"><version>{version}-SNAPSHOT</version></project>')\n"
                )
                fake_git.chmod(0o755)
                output = root / "output"
                env.update(TARGET_TAG=target, GITHUB_REF=ref, GITHUB_SHA="dispatch-sha", GITHUB_OUTPUT=str(output))
                result = subprocess.run(["bash", "-c", script], cwd=root, env=env, capture_output=True, timeout=10)
                self.assertEqual(result.returncode == 0, succeeds, result.stderr.decode())
                if succeeds:
                    expected = "dispatch-sha" if target == "scylla-3.x" else "tag-parent"
                    self.assertIn(f"ref={expected}\n", output.read_text())
                elif target == "scylla-3.x":
                    self.assertFalse((root / "git-calls").exists())
                    self.assertFalse(output.exists())

    def test_historical_checkout_keeps_the_current_guarded_makefile(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fixture(directory)
            original = (root / "Makefile").read_bytes()
            fake_git = root / "git"
            fake_git.write_text("#!/bin/sh\nif [ \"$1\" = checkout ]; then printf '%s\\n' 'MVNCMD = mvn -X' > Makefile; fi\nexit 0\n")
            fake_git.chmod(0o755)
            env["RELEASE_TARGET_TAG"] = "3.11.5.99"
            process = subprocess.run(["make", "checkout-one-commit-before"], cwd=root, env=env, capture_output=True, timeout=10)
            self.assertEqual(process.returncode, 0, process.stderr.decode())
            self.assertEqual((root / "Makefile").read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
