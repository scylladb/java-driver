"""Exercise actual release recipes with a fake Maven executable and synthetic secrets."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import guard

ROOT = Path(__file__).resolve().parents[2]


class ReleaseCommandTests(unittest.TestCase):
    def fake_environment(self, directory):
        root = Path(directory)
        binary = root / "mvn"
        binary.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "with open(os.environ['FAKE_MAVEN_TRACE'], 'a') as trace:\n"
            " trace.write(json.dumps({'args': sys.argv[1:], "
            "'passphrase': os.environ['MAVEN_GPG_PASSPHRASE'], "
            "'opts': os.environ.get('MAVEN_OPTS', ''), 'cwd': os.getcwd(), "
            "'baseline': os.environ.get('RELEASE_BASE_COMMIT', '')}) + '\\n')\n"
            "print('stdout: ' + os.environ['MAVEN_GPG_PASSPHRASE'])\n"
            "print('stderr: ' + os.environ['MAVEN_GPG_PASSPHRASE'], file=sys.stderr)\n"
            "raise SystemExit(int(os.environ.get('FAKE_MAVEN_STATUS', '0')))\n"
        )
        binary.chmod(0o755)
        helper = root / "signing.py"
        helper.write_text(
            "import os, pathlib, subprocess, sys\n"
            "if 'FAKE_PUBLISHER_CERT' in os.environ:\n"
            " assert pathlib.Path(__file__).with_name('publisher.asc').read_text() "
            "== os.environ['FAKE_PUBLISHER_CERT']\n"
            "if 'FAKE_RELEASE_BASE' in os.environ:\n"
            " assert os.environ.get('RELEASE_BASE_COMMIT') == os.environ['FAKE_RELEASE_BASE']\n"
            "if sys.argv[1] == 'exec':\n"
            " raise SystemExit(subprocess.run(sys.argv[2:]).returncode)\n"
        )
        publisher = root / "publication.py"
        publisher.write_text("pass\n")
        env = {key: value for key, value in os.environ.items() if key not in guard.ENV_OPTIONS}
        env.update(
            PATH=str(root) + os.pathsep + env.get("PATH", ""),
            PYTHONDONTWRITEBYTECODE="1",
            MAVEN_GPG_PASSPHRASE="synthetic-passphrase",
            SONATYPE_TOKEN_USERNAME="synthetic-user",
            SONATYPE_TOKEN_PASSWORD="synthetic-password",
            RELEASE_LOG_DIR=str(root / "logs"),
            FAKE_MAVEN_TRACE=str(root / "trace.jsonl"),
            RELEASE_SKIP_TESTS="false",
            RELEASE_VERSION="",
            IS_CICD="true",
            GITHUB_ACTIONS="true",
            GITHUB_ACTOR="release-operator",
            GITHUB_TRIGGERING_ACTOR="rerun-operator",
            RELEASE_SIGNING_HELPER=str(helper),
            RELEASE_PUBLICATION_HELPER=str(publisher),
        )
        return root, env

    def run_make(self, target, env):
        return subprocess.run(
            ["make", "--no-print-directory", target], cwd=ROOT, env=env,
            capture_output=True, text=True, timeout=10,
        )

    def test_all_release_phases_capture_both_streams_and_keep_credentials_in_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            for target in ("release-prepare", "release", "release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertIn("release:prepare", calls[0]["args"])
            self.assertIn("-DcheckModificationExcludeList=README.md", calls[0]["args"])
            self.assertIn("release:perform", calls[1]["args"])
            self.assertIn("release:perform", calls[2]["args"])
            self.assertIn("-Drelease.autopublish=false", calls[1]["args"])
            self.assertNotIn("-Drelease.autopublish=true", calls[2]["args"])
            for call in calls:
                self.assertEqual(call["passphrase"], env["MAVEN_GPG_PASSPHRASE"])
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], " ".join(call["args"]))
                self.assertNotIn("-DskipTests=true", call["opts"])
            self.assertEqual((root / "logs/stdout.log").read_text(), "stdout: [REDACTED]\n" * 3)
            self.assertEqual((root / "logs/stderr.log").read_text(), "stderr: [REDACTED]\n" * 3)

    def test_public_key_selection_preserves_literal_jvm_options_for_every_release_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            key_option = "-Dgpg.keyname=" + "A" * 40 + "!"
            marker = root / "must-not-exist"
            env["MAVEN_OPTS"] = (
                "-Xmx512m -Dhome=$HOME -Dbraced=${HOME} "
                f"'-Dmake=$(shell touch {marker})' " + key_option
            )
            for target in ("release-prepare", "release", "release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            for call in calls:
                self.assertEqual(call["opts"], env["MAVEN_OPTS"])
            self.assertFalse(marker.exists())


    def test_workflow_uses_preserved_recipes_in_target_checkout(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        commands = [script.strip() for script in guard.workflow_commands(workflow)
                    if 'make -f "$RUNNER_TEMP/release-safety/Makefile"' in script
                    or script.strip().startswith(
                        'python3 "$RUNNER_TEMP/release-safety/signing.py" verify-prepared')]
        self.assertEqual(len(commands), 4)
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            target = root / "target-checkout"
            target.mkdir()
            shutil.copy2(ROOT / "pom.xml", target / "pom.xml")
            shutil.copytree(ROOT / ".github", target / ".github")
            original_pom = (target / "pom.xml").read_text()
            marker = target / "unsafe-target-executed"
            # Any read of target Makefiles, including parse-time shell execution,
            # would create the marker and expose the passphrase before capture.
            unsafe = (f"$(shell touch {marker})\nSHELL := bash\n.SHELLFLAGS := -x -c\n"
                      "override RELEASE_GUARD := unsafe.py\n"
                      "release-prepare release release-dry-run:\n"
                      "\techo $${MAVEN_GPG_PASSPHRASE}\n")
            (target / "GNUmakefile").write_text(unsafe)
            (target / "Makefile").write_text(
                'release-prepare release release-dry-run:\n\ttouch "' + str(marker) + '"\n')
            (target / "scripts/release").mkdir(parents=True)
            (target / "scripts/release/signing.py").write_text(
                f"from pathlib import Path\nPath({str(marker)!r}).touch()\nraise SystemExit(1)\n")
            (target / "scripts/release/publisher.asc").write_text("unsafe-target-certificate\n")
            subprocess.run(["git", "init", "--quiet", str(target)], check=True, capture_output=True)
            subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                            "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "Fixture"],
                           cwd=target, check=True, capture_output=True)
            baseline = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
            preserved = root / "release-safety"
            preserved.mkdir()
            shutil.copy2(ROOT / "Makefile", preserved / "Makefile")
            shutil.copy2(ROOT / "scripts/release/guard.py", preserved / "guard.py")
            shutil.copy2(env.pop("RELEASE_SIGNING_HELPER"), preserved / "signing.py")
            shutil.copy2(ROOT / "scripts/release/publisher.asc", preserved / "publisher.asc")
            shutil.copy2(ROOT / "scripts/release/target.py", preserved / "target.py")
            shutil.copy2(env.pop("RELEASE_PUBLICATION_HELPER"), preserved / "publication.py")
            env["FAKE_PUBLISHER_CERT"] = (preserved / "publisher.asc").read_text()
            env.update(RUNNER_TEMP=str(root), RELEASE_GUARD=str(target / "unsafe.py"),
                       GITHUB_ENV=str(root / "github-env"), FAKE_RELEASE_BASE=baseline)
            for command in commands:
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", command], cwd=target, env=env,
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)
                self.assertFalse(marker.exists())
                # Actions makes public GITHUB_ENV values available to later steps.
                public_state = (root / "github-env").read_text()
                self.assertEqual(public_state, f"RELEASE_BASE_COMMIT={baseline}\n")
                env["RELEASE_BASE_COMMIT"] = public_state.strip().split("=", 1)[1]
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertTrue(all(call["cwd"] == str(target) for call in calls))
            self.assertTrue(all(call["baseline"] == baseline for call in calls))
            self.assertEqual((target / "pom.xml").read_text(), original_pom)
            for name in ("stdout", "stderr"):
                output = (root / f"logs/{name}.log").read_text()
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], output)
                self.assertEqual(output.count("[REDACTED]"), 3)

    def test_passphrase_quotes_and_shell_syntax_never_become_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            marker = root / "must-not-exist"
            env["MAVEN_GPG_PASSPHRASE"] = f'synthetic "quoted" $(touch {marker}) secret'
            result = self.run_make("release-prepare", env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(marker.exists())
            call = json.loads((root / "trace.jsonl").read_text())
            self.assertEqual(call["passphrase"], env["MAVEN_GPG_PASSPHRASE"])
            self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)

    def test_requested_skip_flags_apply_to_prepare_perform_and_dry_run(self):
        for target in ("release-prepare", "release", "release-dry-run"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root, env = self.fake_environment(directory)
                env["RELEASE_SKIP_TESTS"] = "true"
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                call = json.loads((root / "trace.jsonl").read_text())
                self.assertIn("-DskipTests=true", call["opts"])
                self.assertIn("-DskipITs=true", call["opts"])

    def test_version_override_remains_one_argument(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["RELEASE_VERSION"] = "3.0.8.1"
            result = self.run_make("release-prepare", env)
            self.assertEqual(result.returncode, 0, result.stderr)
            call = json.loads((root / "trace.jsonl").read_text())
            self.assertIn("-DreleaseVersion=3.0.8.1", call["args"])

    def test_maven_failure_keeps_sanitized_artifacts_and_fails_make(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["FAKE_MAVEN_STATUS"] = "17"
            result = self.run_make("release", env)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Error 17", result.stderr)
            for name in ("stdout", "stderr"):
                output = (root / f"logs/{name}.log").read_text()
                self.assertIn("[REDACTED]", output)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], output)

    def test_debug_environment_is_blocked_before_fake_maven_starts(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["MAVEN_ARGS"] = "-B -X"
            result = self.run_make("release", env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / "trace.jsonl").exists())

    def test_unsafe_environment_is_rejected_during_precredential_check(self):
        with patch.dict(os.environ, {"MAVEN_ARGS": "-B -X"}, clear=True), patch.object(
            sys, "argv", ["guard.py", "check"]
        ), patch.object(guard, "check_policy") as policy:
            self.assertEqual(guard.main(), 2)
            policy.assert_not_called()

    def test_guard_covers_lowercase_release_commands_but_allows_shell_executable_test(self):
        guard.check_text('[[ -x "$executable" ]]', maven_options=False)
        for command in ("${mvn}", "$(mvn)", "$(MVNCMD)"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copy(ROOT / "pom.xml", root / "pom.xml")
                shutil.copytree(ROOT / ".github", root / ".github")
                (root / "Makefile").write_text(f"\t{command} release:perform\n")
                with self.assertRaisesRegex(guard.Unsafe, "sanitized log capture"):
                    guard.check_policy(root)

    def test_json_escaped_credentials_are_removed_before_logs(self):
        secret = 'synthetic "quote" \\ slash\nline é'
        forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
        for ascii_only in (True, False):
            serialized = json.dumps(secret, ensure_ascii=ascii_only)
            with self.subTest(ascii_only=ascii_only):
                cleaned = guard.redact(serialized.encode(), forms)
                self.assertEqual(cleaned, b'"[REDACTED]"')



    def test_validation_builds_unsigned_release_artifacts_without_deployment(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        command = next(script for script in guard.workflow_commands(workflow)
                       if "-Dgpg.skip=true verify" in script)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            maven = root / "mvn"
            maven.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "assert not any(os.environ.get(key) for key in "
                "('RELEASE_GPG_PRIVATE_KEY', 'MAVEN_GPG_PASSPHRASE', "
                "'SONATYPE_TOKEN_USERNAME', 'SONATYPE_TOKEN_PASSWORD'))\n"
                "Path(os.environ['TRACE']).write_text(json.dumps("
                "{'args': sys.argv[1:], 'opts': os.environ.get('MAVEN_OPTS', '')}))\n"
            )
            maven.chmod(0o700)
            safety = root / "release-safety"
            safety.mkdir()
            (safety / "target.py").write_text("pass\n")
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith("RELEASE_") and key not in guard.ENV_OPTIONS
                   and key not in ("MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_USERNAME",
                                   "SONATYPE_TOKEN_PASSWORD")}
            env.update(PATH=str(root) + os.pathsep + os.environ["PATH"],
                       RELEASE_GUARD=str(ROOT / "scripts/release/guard.py"),
                       RELEASE_LOG_DIR=str(root / "logs"), TRACE=str(root / "trace"), RUNNER_TEMP=str(root), RELEASE_VERSION="3.1.2.3")
            for skip in ("false", "true"):
                with self.subTest(skip=skip):
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c", command], cwd=ROOT,
                        env={**env, "RELEASE_SKIP_TESTS": skip},
                        capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    call = json.loads((root / "trace").read_text())
                    self.assertEqual(call["args"], ["-B", "--no-transfer-progress", "-Prelease",
                                                    "-Dgpg.skip=true", "verify"])
                    self.assertEqual("-DskipTests=true" in call["opts"], skip == "true")








if __name__ == "__main__":
    unittest.main()
