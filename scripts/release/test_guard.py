"""Focused checks for release configuration and sanitized output."""

import base64
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.parse

import guard

ROOT = Path(__file__).resolve().parents[2]
CENTRAL_VERSION = re.compile(
    r"(<artifactId>central-publishing-maven-plugin</artifactId>\s*<version>)[^<]*(</version>)"
)


def with_central_version(pom, version):
    # Locate the pin by plugin, not by its current value, so dependency bumps can't
    # turn these tests into silent no-ops.
    updated, count = CENTRAL_VERSION.subn(lambda match: match[1] + version + match[2], pom)
    if count != 1:
        raise AssertionError(f"expected one central-publishing version pin, found {count}")
    return updated



class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

    def test_release_account_and_baseline_lookups_allow_only_fixed_commands(self):
        guard.check_maven_capture('if ! user=$(gh api user --jq .login); then')
        guard.check_maven_capture('baseline=$(git rev-parse HEAD)')
        for command in (
            'if ! user=$(gh api user --jq "$(mvn release:perform)"); then',
            'if ! user=$(gh api user --jq .login; mvn release:perform); then',
            'baseline=$(git rev-parse HEAD; mvn release:perform)',
        ):
            with self.subTest(command=command), self.assertRaises(guard.Unsafe):
                guard.check_maven_capture(command)

    def test_internal_release_recipes_require_sanitized_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / "Makefile").write_text('_release:\n\tmvn deploy\n')
            with self.assertRaisesRegex(guard.Unsafe, "sanitized log capture"):
                guard.check_policy(root)

    def test_sourced_release_scripts_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / "release.sh").write_text("mvn release:perform\n")
            workflow = root / ".github/workflows/release.yml"
            original = workflow.read_text()
            for launcher in ("source", "."):
                with self.subTest(launcher=launcher):
                    (root / "Makefile").write_text(f"release:\n\t{launcher} ./release.sh\n")
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
                    (root / "Makefile").unlink()
                    workflow.write_text(original.replace(
                        'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run',
                        f"run: {launcher} ./release.sh",
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
                    workflow.write_text(original)

    def test_make_output_functions_cannot_print_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for function in ("info", "warning", "error"):
                with self.subTest(function=function):
                    (root / "Makefile").write_text(
                        f"LEAK := $({function} $(SONATYPE_TOKEN_PASSWORD))\n"
                        'release:\n\tpython3 "$(RELEASE_GUARD)" run $(MVNCMD) release:perform\n'
                    )
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_unsafe_options(self):
        for value in (
            "-X",
            "-l raw.log",
            "-lraw.log",
            "-l=raw.log",
            "--log-file raw.log",
            "--log-file=raw.log",
            "-Dgpg.bestPractices=false",
            "-Dgpg.bestPractices=no",
            "-Dgpg.bestPractices=0",
            "-Dgpg.bestPractices=",
            "--define=gpg.bestPractices=maybe",
            "-define=gpg.bestPractices=false",
            "-define=javax.net.debug=all",
            "-define=http.authorization=dummy",
            "-define password=dummy",
            "-Djavax.net.debug=all",
            "-Djavax.net.debug=ssl:record:plaintext",
            "-Djavax.net.debug",
            '--define=javax.net.debug="all"',
            '-Dgpg.bestPractices="false"',
            "-Dgpg.bestPractices='false'",
            "-Dorg.slf4j.simpleLogger.logFile=raw.log",
            "-Dorg.slf4j.simpleLogger.logFile=SYSTEM.OUT",
            "-Dorg.slf4j.simpleLogger.logFile=System.out",
            "-Dorg.slf4j.simpleLogger.logFile=System.err",
            "-Dorg.slf4j.simpleLogger.logFile=System.out raw.log",
            "--debug",
            "-debug",
            '-Dorg.slf4j.simpleLogger.log.org.apache.http="debug"',
            '-Dorg.slf4j.simpleLogger."logFile"=raw.log',
            "-Dorg.slf4j.simpleLogger.log.org.apache.http='trace'",
            '-Darguments="-X"',
            "-Dgpg.passphrase=dummy",
            "-Dhttp.authorization=dummy",
            "--define=gpg.passphrase=dummy",
            "-D gpg.passphrase=dummy",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=trace",
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value)
        guard.check_text("mvn -B -ntp release:perform -DskipTests=true")
        guard.check_text('-Dgpg.bestPractices="true"')
        guard.check_text("if [[ $? -lt 127 ]]", "Makefile", maven_options=False)

    def test_complete_gpg_boolean_values(self):
        for value in (
            '-Dgpg.bestPractices="true false"',
            "-Dgpg.bestPractices='true false'",
            '"-Dgpg.bestPractices=true false"',
            '-Dgpg."bestPractices"="true false"',
            '-Dgpg.bestPractices="true "',
            '-Dgpg.bestPractices',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices=true',
        ):
            guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices="true"',
            "-Dgpg.bestPractices='true'",
            '"-Dgpg.bestPractices=true"',
            '-Dgpg."bestPractices"="true"',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        guard.check_text("-Dgpg.bestPractices")  # Maven CLI bare properties mean true.
        for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            for value in ('-Dgpg.bestPractices="true false"', "-Dgpg.bestPractices"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(
                    sys, "argv", ["guard.py", "run", "mvn", "validate"]
                ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
        for name in guard.ENV_OPTIONS:
            for value in ("-Dorg.slf4j.simpleLogger.logFile=raw.log",
                          "-Dorg.slf4j.simpleLogger.logFile"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                    guard, "capture"
                ) as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_direct_gpg_argument_boundaries(self):
        for value, expected in (("true false", 2), ("true ", 2), ("true", 0)):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "-Dgpg.bestPractices=" + value]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(capture.called, expected == 0)

    def test_direct_gpg_aliases_and_nested_arguments(self):
        for option in ("-D", "-define=", "--define="):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                argument = option + "gpg.bestPractices=true false"
                guard.check_text(argument, maven_arguments=[argument])
        with self.assertRaises(guard.Unsafe):
            guard.check_text("-D gpg.bestPractices=true false",
                             maven_arguments=["-D", "gpg.bestPractices=true false"])
        nested = "-Darguments=-Dgpg.bestPractices=true -DskipTests=true"
        guard.check_text(nested, maven_arguments=[nested])

    def test_direct_debug_options(self):
        guard.check_text("-Xmx1g -Xms256m")
        for option in ("-Xq", "-debug"):
            with self.subTest(option=option), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()

    def test_maven_argument_sources_reject_debug_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                with patch.dict(os.environ, {"MAVEN_ARGS": "-Xvalidate"}, clear=True):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path", side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    (root / ".mvn/maven.config").write_text("-Xvalidate")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-Dgpg.bestPractices=true false")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-DargLine=-Xmx512m -XX:+UseG1GC")
                    self.assertEqual(guard.main(), 0)
                    (root / ".mvn/maven.config").unlink()
                    (root / ".mvn/jvm.config").write_text("-Dgpg.bestPractices")
                    self.assertEqual(guard.main(), 2)
                    (root / ".mvn/jvm.config").write_text("-Xmx1g")
                    self.assertEqual(guard.main(), 0)
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_ARGS": "-DargLine='-Xmx512m -XX:+UseG1GC'"}, clear=True):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}, clear=True):
                    self.assertEqual(guard.main(), 0)
                    capture.assert_called_once()

    def test_maven_aliases_are_rejected_across_argument_sources(self):
        for value in ("--X", "--l raw.log", "--l=raw.log",
                      "--Dgpg.bestPractices=false", "--D=gpg.bestPractices=false",
                      "--Dorg.slf4j.simpleLogger.logFile=raw.log"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copytree(ROOT / ".github", root / ".github")
                shutil.copy(ROOT / "pom.xml", root / "pom.xml")
                config = root / ".mvn/maven.config"
                config.parent.mkdir()
                config.write_text(value.replace(" ", "\n"))
                with self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
                with patch.object(guard, "capture") as capture, \
                     patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True), \
                     patch.object(sys, "argv", ["guard.py", "run", "mvn"]):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                with patch.object(guard, "capture") as capture, \
                     patch.dict(os.environ, {}, clear=True), \
                     patch.object(sys, "argv", ["guard.py", "run", "mvn", *value.split()]):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_unwrapped_release_goals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for goal in ("prepare", "perform"):
                for options in ("", "-B ", "-B -ntp "):
                    (root / "Makefile").write_text(f"\t$(MVNCMD) {options}release:{goal}\n")
                    with self.subTest(goal=goal, options=options), self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_make_capture_continuations_and_variable_goals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for executable in ("mvn", "/usr/bin/mvn", "./mvnw", "$(MVNCMD)",
                               "${MVNCMD}", "$${MVNCMD}", "$$MVNCMD"):
                for arguments in ("-B release:perform", "-B \\\n\t release:prepare",
                                  "$(GOAL)", "${GOAL}", "clean"):
                    for prefix in ("", "echo '$(RELEASE_GUARD)'; "):
                        for target in ("compile", "release"):
                            (root / "Makefile").write_text(
                                "GOAL = release:perform\n" + target + ":\n\t" + prefix
                                + executable + " " + arguments + " # $(RELEASE_GUARD)\n"
                            )
                            with self.subTest(executable=executable, arguments=arguments,
                                              prefix=prefix, target=target):
                                if target == "compile" and arguments == "clean":
                                    guard.check_policy(root)
                                else:
                                    with self.assertRaises(guard.Unsafe):
                                        guard.check_policy(root)
                (root / "Makefile").write_text(
                    "compile:\n\t" + executable + " compile test-compile -Dfmt.skip=true\n"
                    "release:\n\tpython3 \"$(RELEASE_GUARD)\" run " + executable
                    + " \\\n\t $(GOAL)\n"
                )
                guard.check_policy(root)

    def test_workflow_requires_capture_for_each_maven_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_path = root / ".github/workflows/release.yml"
            workflow_path.parent.mkdir(parents=True)
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow = (ROOT / ".github/workflows/release.yml").read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for executable in ("mvn", "/usr/bin/mvn", "./mvnw", "${MVNCMD}", "$MVNCMD"):
                for script in (
                    executable + " clean # RELEASE_GUARD",
                    json.dumps(executable + ' clean # RELEASE_GUARD'),
                    "'" + executable + " clean # RELEASE_GUARD'",
                    '|\n          ' + executable + " clean # RELEASE_GUARD",
                    '>\n          ' + executable + "\n          clean # RELEASE_GUARD",
                    'python3 "$RELEASE_GUARD" run $MVNCMD clean; ' + executable + ' release:clean',
                    '|\n          python3 "$RELEASE_GUARD" run $MVNCMD clean\n          '
                    + executable + ' release:clean',
                ):
                    workflow_path.write_text(workflow.replace(original, "run: " + script))
                    with self.subTest(script=script), self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
                workflow_path.write_text(workflow.replace(
                    original, 'run: python3 "${RELEASE_GUARD}" run ' + executable + " clean"
                ))
                guard.check_policy(root)
            workflow_path.write_text(workflow.replace(
                original, 'run: >\n          python3 "$RELEASE_GUARD" run\n          $MVNCMD clean'
            ))
            guard.check_policy(root)

    def test_unresolved_executables_require_guard_routing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow_path = root / ".github/workflows/release.yml"
            workflow = workflow_path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            for executable in ('$(printf mvn)', '"$(printf mvn)"', '`printf mvn`',
                               '$TOOL', '${TOOLS}/mvn', './mv*'):
                for prefix in ('', 'SAFE=true ', 'exec ', 'env '):
                    command = prefix + executable + ' release:perform'
                    with self.subTest(command=command):
                        (root / "Makefile").write_text('release:\n\t' + command + '\n')
                        with self.assertRaisesRegex(guard.Unsafe, 'unresolved release executables'):
                            guard.check_policy(root)
                        (root / "Makefile").unlink()
                        workflow_path.write_text(workflow.replace(original, 'run: |\n          ' + command))
                        with self.assertRaisesRegex(guard.Unsafe, 'unresolved release executables'):
                            guard.check_policy(root)
                        workflow_path.write_text(workflow)
                guarded = 'SAFE=true python3 "$RELEASE_GUARD" run ' + executable + ' release:perform'
                if "$(" in executable or "`" in executable:
                    # Shell expansion happens before the guard can capture it.
                    with self.assertRaises(guard.Unsafe):
                        guard.check_maven_capture(guarded, require_capture=True)
                else:
                    guard.check_maven_capture(guarded, require_capture=True)
            for executable in ('/usr/bin/mvn', './mvnw', 'custom-maven'):
                guard.check_maven_capture(
                    'python3 "$RELEASE_GUARD" run ' + executable + ' release:perform',
                    require_capture=True,
                )
            # Real workflow includes multiline gh command substitutions in assignments.
            guard.check_policy(root)

    def test_workflow_quoted_and_spaced_run_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            path = root / ".github/workflows/release.yml"
            workflow = path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            for key in ('run :', '"run":', "'run':", '"run" :', "'run'  :"):
                for value in ('mvn release:perform', '"mvn release:perform"',
                              '|\n          mvn release:perform',
                              '>\n          mvn\n          release:perform'):
                    with self.subTest(key=key, value=value):
                        path.write_text(workflow.replace(original, key + ' ' + value))
                        with self.assertRaisesRegex(guard.Unsafe, 'sanitized log capture'):
                            guard.check_policy(root)
                path.write_text(workflow.replace('run:', key))
                guard.check_policy(root)
                self.assertEqual(list(guard.workflow_commands('- ' + key + ' mvn clean')),
                                 ['mvn clean'])

    def test_assignment_substitutions_and_eval_cannot_bypass_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for command in ('result=$$(mvn release:perform)',
                            'result="$$(mvn release:perform)"',
                            'result=`mvn release:perform`',
                            "eval 'mvn release:perform'",
                            "command eval 'mvn release:perform'",
                            "env -- eval 'mvn release:perform'"):
                with self.subTest(command=command):
                    (root / "Makefile").write_text('release:\n\t' + command + '\n')
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_blocker_lookup_allows_only_existing_fixed_command(self):
        script = "blocker=$(gh issue list --repo \"$GITHUB_REPOSITORY\" --state open --label release-blocker --limit 1 --json title,url --jq '.[0] // empty')"
        guard.check_maven_capture(script, require_capture=True)
        for extra in ('; mvn release:perform', ' $(mvn release:perform)',
                      ' `mvn release:perform`', '\n mvn release:perform'):
            with self.subTest(extra=extra), self.assertRaises(guard.Unsafe):
                guard.check_maven_capture(script.replace('gh issue list', 'gh issue list' + extra),
                                          require_capture=True)

    def test_env_launcher_options_cannot_hide_executables(self):
        for command in ('env -- "$$(command -v mvn)" -B release:perform',
                        'env -- SAFE=true "$$(command -v mvn)" release:perform',
                        'env -- mvn release:perform',
                        'env -- /bin/bash -c "mvn release:perform"',
                        'env -S "mvn release:perform"',
                        'env --split-string="mvn release:perform"'):
            with self.subTest(command=command), self.assertRaises(guard.Unsafe):
                guard.check_maven_capture(command, require_capture=True)
        guard.check_maven_capture(
            'env -- SAFE=true python3 "$RELEASE_GUARD" run mvn release:perform',
            require_capture=True,
        )

    def test_guarded_maven_arguments_are_not_shell_launchers(self):
        for argument in ('-Dsafe.path=/tmp/bash', '/tmp/bash', 'eval', 'sh'):
            with self.subTest(argument=argument):
                guard.check_maven_capture(
                    'python3 "$RELEASE_GUARD" run mvn release:perform ' + argument,
                    require_capture=True, make_variables=True,
                )

    def test_quoted_workflow_run_comments_preserve_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            path = root / '.github/workflows/release.yml'
            workflow = path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            for command in ('python3 "$RELEASE_GUARD" run mvn clean -Dsafe.path=/tmp/#bash',
                            "python3 '$RELEASE_GUARD' run mvn clean -Dsafe.path=/tmp/#bash",
                            'mvn release:perform # RELEASE_GUARD'):
                for scalar in (json.dumps(command), "'" + command.replace("'", "''") + "'"):
                    value = scalar + ' # YAML comment with "quotes" and # hashes'
                    with self.subTest(value=value):
                        self.assertEqual(list(guard.workflow_commands('run: ' + value)), [command])
                        path.write_text(workflow.replace(original, 'run: ' + value))
                        if command.startswith('mvn'):
                            with self.assertRaisesRegex(guard.Unsafe, 'sanitized log capture'):
                                guard.check_policy(root)
                        else:
                            guard.check_policy(root)

    def test_make_rejects_custom_recipe_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for setting in ('.RECIPEPREFIX := >', '.RECIPEPREFIX = >',
                            'override .RECIPEPREFIX := >', 'export .RECIPEPREFIX := >'):
                with self.subTest(setting=setting):
                    (root / "Makefile").write_text(setting + '\nrelease:\n>mvn release:perform\n')
                    with self.assertRaisesRegex(guard.Unsafe, r'\.RECIPEPREFIX.*unsupported'):
                        guard.check_policy(root)
            (root / "Makefile").write_text('# .RECIPEPREFIX := >\nrelease:\n\t@echo safe\n')
            guard.check_policy(root)

    def test_make_rejects_nested_shell_launchers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for launcher in ("bash -c", "/bin/sh -c", "env bash -lc", "exec zsh -c"):
                for executable in ("mvn", "$(MVNCMD)", "$$MVNCMD"):
                    for target in ("compile", ".release", "release-prepare"):
                        for separator in ("\n\t", " ; "):
                            (root / "Makefile").write_text(
                                target + ":" + separator + "@" + launcher + " '"
                                + executable + " -B release:perform' # $(RELEASE_GUARD)\n"
                            )
                            with self.subTest(launcher=launcher, executable=executable,
                                              target=target, separator=separator), \
                                 self.assertRaisesRegex(guard.Unsafe, "nested shell launchers"):
                                guard.check_policy(root)

    def test_workflow_rejects_nested_shell_launchers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_path = root / ".github/workflows/release.yml"
            workflow_path.parent.mkdir(parents=True)
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow = (ROOT / ".github/workflows/release.yml").read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for launcher in ("sh", "bash", "dash", "ash", "ksh", "zsh", "fish", "/bin/bash"):
                for script in ("mvn -B release:perform", "$MVNCMD clean"):
                    command = launcher + " -c '" + script + "' # RELEASE_GUARD"
                    for value in (command, json.dumps(command), "|\n          " + command):
                        workflow_path.write_text(workflow.replace(original, "run: " + value))
                        with self.subTest(launcher=launcher, script=script, value=value), \
                             self.assertRaisesRegex(guard.Unsafe, "nested shell launchers"):
                            guard.check_policy(root)

    def test_make_inline_recipes_require_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for target in (".release", "release-prepare", "compile", "compile .release"):
                for executable in ("mvn", "$(MVNCMD)", "${MVNCMD}"):
                    for arguments in ("-B release:perform", "$(GOAL)", "clean"):
                        for prefix in ("", "@", "+", "-", "echo capture; "):
                            (root / "Makefile").write_text(
                                "GOAL = release:perform\n" + target + ": ; "
                                + prefix + executable + " " + arguments
                                + " # $(RELEASE_GUARD)\n"
                            )
                            with self.subTest(target=target, executable=executable,
                                              arguments=arguments, prefix=prefix):
                                if target == "compile" and arguments == "clean":
                                    guard.check_policy(root)
                                else:
                                    with self.assertRaises(guard.Unsafe):
                                        guard.check_policy(root)
                    (root / "Makefile").write_text(
                        "compile: ; " + executable + " compile test-compile -Dfmt.skip=true\n"
                        + target + ': ; @python3 "$(RELEASE_GUARD)" run ' + executable
                        + " -B \\\n\t release:perform\n"
                    )
                    guard.check_policy(root)

    def test_release_prerequisites_and_executable_make_directives_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for text in (
                'release: helper\nhelper:\n\tbash -c "mvn release:perform"\n',
                'include unsafe.mk\nrelease:\n\techo safe\n',
                'X := $(shell mvn release:perform)\nrelease:\n\techo safe\n',
                '$(eval release: ; mvn release:perform)\n',
                'release:\n\tcustom-maven release:perform\n',
            ):
                with self.subTest(text=text):
                    (root / "Makefile").write_text(text)
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_argument_substitutions_and_opaque_launchers_fail_full_policy(self):
        commands = (
            'echo "$(mvn release:perform)"',
            'printf "%s\\n" "$(mvn release:perform)"',
            'echo "`mvn release:perform`"',
            'echo "$(mvn)"',
            'python3 "$RELEASE_GUARD" run mvn "$(mvn release:perform)"',
            'python3 "$RELEASE_GUARD" run mvn "`mvn release:perform`"',
            "builtin eval 'mvn release:perform'",
            "command -p bash -c 'mvn release:perform'",
            "command -- builtin eval 'mvn release:perform'",
            "exec -a safe bash -c 'mvn release:perform'",
            "env -S 'mvn release:perform'",
            'xargs mvn clean',
            'xargs custom-maven release:perform',
            'timeout 10 mvn clean',
            'nice mvn clean',
            'nohup mvn clean',
            'custom-wrapper mvn clean',
            'custom-maven release:perform',
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow_path = root / ".github/workflows/release.yml"
            workflow = workflow_path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for command in commands:
                with self.subTest(command=command, source="workflow"):
                    workflow_path.write_text(workflow.replace(original, 'run: ' + json.dumps(command)))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
                workflow_path.write_text(workflow)
                # Make escapes shell dollars; the rejection must survive that layer.
                for target in ('release', 'compile'):
                    with self.subTest(command=command, source="Makefile", target=target):
                        (root / "Makefile").write_text(
                            target + ':\n\t' + command.replace('$', '$$') + '\n'
                        )
                        with self.assertRaises(guard.Unsafe):
                            guard.check_policy(root)
                (root / "Makefile").unlink()

    def test_diagnostic_arguments_and_supported_launchers_pass_full_policy(self):
        commands = (
            'echo mvn release:perform',
            'printf "%s\\n" mvn release:perform',
            'echo bash eval custom-maven release:perform',
            'command -- echo mvn release:perform',
            'env -- SAFE=true printf "%s\\n" mvn release:perform',
            'command -- python3 "$RELEASE_GUARD" run custom-maven release:perform',
            'exec -- python3 "$RELEASE_GUARD" run mvn release:perform',
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow_path = root / ".github/workflows/release.yml"
            workflow = workflow_path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            for command in commands:
                with self.subTest(command=command):
                    workflow_path.write_text(workflow.replace(original, 'run: ' + json.dumps(command)))
                    (root / "Makefile").write_text('release:\n\t' + command + '\n')
                    guard.check_policy(root)

    def test_workflow_aliases_and_tags_are_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            path = root / ".github/workflows/release.yml"
            workflow = path.read_text()
            for value in ("*release", "&release mvn release:perform", "!!str mvn release:perform"):
                with self.subTest(value=value):
                    path.write_text("command: &release mvn release:perform\n" +
                                    workflow.replace('run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run', "run: " + value))
                    with self.assertRaisesRegex(guard.Unsafe, "aliased or tagged"):
                        guard.check_policy(root)

    def test_process_substitutions_and_subshells_fail_full_policy(self):
        commands = (
            'cat <(mvn deploy)',
            'cat >(/usr/bin/mvn deploy)',
            'python3 "$RELEASE_GUARD" run mvn clean <(mvn deploy)',
            'python3 "$RELEASE_GUARD" run mvn clean >(mvn deploy)',
            '(mvn $GOAL)',
            '( mvn $GOAL )',
            'echo safe; (mvn $GOAL)',
            'if true; then (mvn $GOAL); fi',
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / '.github', root / '.github')
            shutil.copy(ROOT / 'pom.xml', root / 'pom.xml')
            workflow_path = root / '.github/workflows/release.yml'
            workflow = workflow_path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for command in commands:
                with self.subTest(command=command, source='workflow'):
                    workflow_path.write_text(workflow.replace(original, 'run: ' + json.dumps(command)))
                    with self.assertRaisesRegex(guard.Unsafe, 'grouping and process substitutions'):
                        guard.check_policy(root)
                workflow_path.write_text(workflow)
                with self.subTest(command=command, source='Makefile'):
                    (root / 'Makefile').write_text(
                        'GOAL = release:perform\n.release:\n\t'
                        + command.replace('$GOAL', '$(GOAL)') + '\n'
                    )
                    with self.assertRaisesRegex(guard.Unsafe, 'grouping and process substitutions'):
                        guard.check_policy(root)
                (root / 'Makefile').unlink()

    def test_shell_comments_preserve_command_boundaries_full_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / '.github', root / '.github')
            shutil.copy(ROOT / 'pom.xml', root / 'pom.xml')
            workflow_path = root / '.github/workflows/release.yml'
            workflow = workflow_path.read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for executable in ('mvn', '/usr/bin/mvn', './mvnw', '$MVNCMD'):
                for comment in ('# captured', '# ignored (mvn deploy) <(mvn deploy)'):
                    script = ('python3 "$RELEASE_GUARD" run $MVNCMD clean '
                              + comment + '\n' + executable + ' release:perform')
                    with self.subTest(executable=executable, comment=comment):
                        workflow_path.write_text(workflow.replace(
                            original, 'run: |\n          ' + script.replace('\n', '\n          ')
                        ))
                        with self.assertRaisesRegex(guard.Unsafe, 'sanitized log capture'):
                            guard.check_policy(root)

    def test_comments_and_literal_parentheses_pass_full_policy(self):
        commands = (
            'python3 "$RELEASE_GUARD" run mvn clean # captured',
            '# ignored (mvn deploy) <(mvn deploy) >(mvn deploy) $(mvn deploy)\n'
            'python3 "$RELEASE_GUARD" run mvn clean',
            'python3 "$RELEASE_GUARD" run mvn clean # captured\necho safe # done',
            'python3 "$RELEASE_GUARD" run mvn clean "-Dlabel=(ordinary)"',
            "python3 \"$RELEASE_GUARD\" run mvn clean '-Dlabel=<(ordinary)>(text)'",
            r'python3 "$RELEASE_GUARD" run mvn clean -Dlabel=\(ordinary\)',
            'python3 "$RELEASE_GUARD" run mvn clean -Dlabel=hash#text',
            'python3 "$RELEASE_GUARD" run mvn clean "-Dlabel=#(text)"',
            'if test -n "$GOAL"; then python3 "$RELEASE_GUARD" run mvn clean; fi',
            'if [[ -n "$GOAL" ]]; then python3 "$RELEASE_GUARD" run mvn clean; fi',
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / '.github', root / '.github')
            shutil.copy(ROOT / 'pom.xml', root / 'pom.xml')
            workflow_path = root / '.github/workflows/release.yml'
            workflow = workflow_path.read_text()
            makefile = (ROOT / 'Makefile').read_text()
            original = 'run: make -f "$RUNNER_TEMP/release-safety/Makefile" RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" release-dry-run'
            self.assertIn(original, workflow)
            for command in commands:
                with self.subTest(command=command):
                    workflow_path.write_text(workflow.replace(original, 'run: ' + json.dumps(command)))
                    # Keep all native credential gates while adding a safe recipe.
                    (root / 'Makefile').write_text(
                        makefile + '\nrelease-lexical:\n\t'
                        + command.replace('$', '$$').replace('\n', '\n\t') + '\n'
                    )
                    guard.check_policy(root)

    def test_old_release_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for case, text, message in (
                ("bestPractices", pom.replace("<gpg.bestPractices>true</gpg.bestPractices>", ""),
                 "enable GPG best practices"),
                ("central 0.8.0", with_central_version(pom, "0.8.0"), "update Central publishing"),
                ("central 0.10.0", with_central_version(pom, "0.10.0"),
                 "update Central publishing"),
            ):
                (root / "pom.xml").write_text(text)
                with self.subTest(case=case), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)

    def test_disabled_gpg_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            for value in ("false", "no", "0", "", "true"):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<artifactId>maven-gpg-plugin</artifactId>",
                    "<artifactId>maven-gpg-plugin</artifactId>"
                    f"<configuration><bestPractices>{value}</bestPractices></configuration>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value):
                    if value == "true":
                        guard.check_policy(root)
                    else:
                        with self.assertRaises(guard.Unsafe):
                            guard.check_policy(root)

    def test_disabled_gpg_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("false", "no", "0", ""):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<profiles>",
                    "<profiles><profile><id>unsafe</id><properties>"
                    f"<gpg.bestPractices>{value}</gpg.bestPractices></properties></profile>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_newer_central_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for version in ("0.11.0", "0.11.1", "0.12.0", "1.0.0"):
                (root / "pom.xml").write_text(with_central_version(pom, version))
                with self.subTest(version=version):
                    guard.check_policy(root)

    def test_workflow_policy_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow = root / ".github/workflows/release.yml"
            original = workflow.read_text()
            workflow.write_text(original.replace("if: always()", "if: ${{ always() }}"))
            guard.check_policy(root)
            for text, message in ((original.replace("RELEASE_GUARD", "GUARD"), "RELEASE_GUARD"),
                                  (original.replace("if: always()", "if: success()"), "always()")):
                workflow.write_text(text)
                with self.assertRaisesRegex(guard.Unsafe, re.escape(message)):
                    guard.check_policy(root)
            workflow.unlink()
            with self.assertRaisesRegex(guard.Unsafe, "workflow.*missing"):
                guard.check_policy(root)
            with patch.object(sys, "argv", ["guard.py", "check"]), patch.object(
                guard, "check_policy", side_effect=guard.Unsafe("release workflow requires RELEASE_GUARD")
            ), patch("sys.stderr", io.StringIO()) as output:
                self.assertEqual(guard.main(), 2)
                self.assertIn("RELEASE_GUARD", output.getvalue())

    def test_option_locations_hide_values(self):
        for text in ("safe\ncurl -X POST secret", "safe\n-X", "safe\n--define\npassword=secret"):
            with patch("sys.stderr", new_callable=io.StringIO) as output:
                with self.assertRaises(guard.Unsafe):
                    guard.check_text(text, "Makefile")
                self.assertIn("Makefile:2", output.getvalue())
                self.assertNotIn("secret", output.getvalue())
        for text in ("-Dmaven.repo.local=/x/repo-f", "-Dfoo=a-s", "-fae -ff -llr"):
            guard.check_text(text)

    def test_termination_signals_stop_maven(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready"
                mvn = root / "mvn"
                mvn.write_text(f"#!{sys.executable}\nimport os, time\n"
                               "os.write(1, b'-Dpassword=\"first\\nsecret tail')\n"
                               f"open({str(ready) + '.tmp'!r}, 'w').write(str(os.getpid()))\n"
                               f"os.rename({str(ready) + '.tmp'!r}, {str(ready)!r})\n"
                               "time.sleep(30)\n")
                mvn.chmod(0o755)
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                    env=dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                             RELEASE_LOG_DIR=str(root / "logs")),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child = int(ready.read_text())
                    process.send_signal(signum)
                    stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 128 + signum)
                    for name, console in (("stdout", stdout), ("stderr", stderr)):
                        self.assertEqual(console, guard.CANCELLED_LOG)
                        self.assertEqual((root / "logs" / (name + ".log")).read_bytes(), console)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()

    def test_encoded_and_multiline_credentials(self):
        env = {
            "MAVEN_GPG_PASSPHRASE": "first line\nsecond line+/=",
            "SONATYPE_TOKEN_USERNAME": "dummy-user",
            "SONATYPE_TOKEN_PASSWORD": "dummy-password",
        }
        for value in [*env.values(), "dummy-user:dummy-password"]:
            for encoded in (
                value,
                base64.b64encode(value.encode()).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote(urllib.parse.quote(value)),
                urllib.parse.quote(urllib.parse.quote(value), safe=""),
                urllib.parse.quote_plus(value),
            ):
                with self.subTest(encoded=encoded):
                    self.assertEqual(
                        b"[REDACTED]", guard.redact(encoded.encode(), guard.secret_forms(env))
                    )
        self.assertEqual(
            guard.redact(b'Authorization: Bearer unknown\n-Dpassword="a\nb"', []),
            b"Authorization: [REDACTED]\n-Dpassword=[REDACTED]",
        )

    def test_xml_escaped_credentials_in_both_streams(self):
        secret = "xml&<tag>\"'secret"
        forms = (
            "xml&amp;&lt;tag&gt;\"'secret",
            "xml&amp;&lt;tag&gt;&quot;&apos;secret",
            "xml&amp;&lt;tag&gt;&quot;&#x27;secret",
            "xml&amp;&lt;tag&gt;&#34;&#39;secret",
            "xml&amp;&lt;tag&gt;&#x22;&#x27;secret",
        )
        output = ("\n".join(forms) + "\n").encode()
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       f"import os; os.write(1, {output!r}); os.write(2, {output!r})"]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script], capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret),
            )
            self.assertEqual(result.returncode, 0)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b"[REDACTED]\n" * len(forms))
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_quoted_authorization_headers(self):
        for header in ("Authorization", "Proxy-Authorization"):
            for quote in ("", "'", '"'):
                with self.subTest(header=header, quote=quote):
                    data = f"{quote}{header}{quote}: {quote}Bearer unknown{quote}\n".encode()
                    self.assertNotIn(b"unknown", guard.redact(data, []))

    def test_quoted_credential_arguments(self):
        for quote in (b"'", b'"'):
            for option in (b"-Dpassword=", b"--define=token=", b"--define secret=",
                           b"-define=token=", b"-define secret="):
                with self.subTest(quote=quote, option=option):
                    result = guard.redact(quote + option + b"two words\nlast" + quote, [])
                    self.assertNotIn(b"words", result)
                    self.assertNotIn(b"last", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_escaped_quotes_in_credential_arguments(self):
        for quote in (b"'", b'"'):
            secret = b"alpha" + b"\\" + quote + b"beta gamma"
            for value in (
                b"-Dpassword=" + quote + secret + quote,
                quote + b"-Dpassword=" + secret + quote,
            ):
                with self.subTest(value=value):
                    result = guard.redact(value, [])
                    self.assertNotIn(b"beta", result)
                    self.assertNotIn(b"gamma", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_secrets_matching_credential_markers(self):
        for marker, output in (
            ("password", b"-Ddb.password=unregistered-value"),
            ("Authorization", b"Authorization: Bearer unregistered-value"),
        ):
            with self.subTest(marker=marker):
                result = guard.redact(output, guard.secret_forms({"TOKEN": marker}))
                self.assertNotIn(b"unregistered-value", result)
                self.assertNotIn(marker.encode(), result)

    def test_overlapping_structured_and_literal_secrets(self):
        for secret in ("prefix-Authorization: suffix", "prefix-Dpassword=value tail"):
            with self.subTest(secret=secret):
                result = guard.redact(secret.encode(), guard.secret_forms({"TOKEN": secret}))
                self.assertEqual(result, b"[REDACTED]")
        self.assertEqual(guard.redact(b"ababa", [b"aba"]), b"[REDACTED]")

    def test_split_output_artifacts_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            # Use a real subprocess to verify both console streams and the resulting files.
            command = [
                sys.executable,
                "-c",
                "import os\nfor fd in (1, 2):\n"
                ' for part in (b"diagnostic: ", b"dummy-", b"secret\\n"):\n'
                "  os.write(fd, part)\nraise SystemExit(17)",
            ]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE="dummy-secret"),
            )
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, (Path(directory) / (name + ".log")).read_bytes())
                self.assertEqual(console, b"diagnostic: [REDACTED]\n")

    def test_capture_read_failure_preserves_completed_maven_status(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                process = unittest.mock.Mock()
                process.poll.return_value = status
                process.wait.return_value = status
                with patch.object(guard.subprocess, "Popen", return_value=process), \
                     patch.object(guard.selectors, "DefaultSelector", side_effect=OSError("secret")), \
                     patch.object(guard.os, "killpg"), patch.object(guard, "write_logs") as logs:
                    self.assertEqual(guard.capture(["mvn"], {}, [], Path(directory)), status)
                    logs.assert_called_once_with([guard.FAILED_LOG, guard.FAILED_LOG], Path(directory))

    def test_configured_maven_executable(self):
        for executable in ("/usr/bin/mvn", "./mvnw", "custom-maven"):
            with self.subTest(executable=executable), \
                 patch.object(sys, "argv", ["guard.py", "run", executable, "release:prepare"]), \
                 patch.object(guard, "capture", return_value=0) as capture, \
                 patch.object(guard, "check_runtime_options"):
                self.assertEqual(guard.main(), 0)
                self.assertEqual(capture.call_args.args[0], [executable, "release:prepare"])

    def test_output_failures_preserve_maven_status(self):
        for status in (0, 17):
            for failure in ("directory", "artifact", "stdout", "stderr"):
                with self.subTest(status=status, failure=failure), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    if failure == "directory":
                        logs.write_text("blocked")
                    elif failure == "artifact":
                        logs.mkdir()
                        (logs / "stdout.log").mkdir()
                    command = [sys.executable, "-c",
                               f"import os; os.write(1, b'out\\n'); os.write(2, b'err\\n'); raise SystemExit({status})"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    read_fd, write_fd = os.pipe()
                    os.close(read_fd)
                    try:
                        result = subprocess.run(
                            [sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                            stdout=write_fd if failure == "stdout" else subprocess.PIPE,
                            stderr=write_fd if failure == "stderr" else subprocess.PIPE,
                        )
                    finally:
                        os.close(write_fd)
                    self.assertEqual(result.returncode, status)
                    if failure != "stdout":
                        self.assertEqual(result.stdout, b"out\n")
                    if failure != "stderr":
                        self.assertIn(b"err\n", result.stderr)
                        self.assertIn(b"values withheld", result.stderr)
                    if failure in ("stdout", "stderr"):
                        self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if failure != "directory":
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_sanitization_failure_writes_fixed_logs(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    "guard.redact = lambda *_: (_ for _ in ()).throw(ValueError('secret'))\n"
                    f"raise SystemExit(guard.capture([{sys.executable!r}, '-c', "
                    f"'print(\"secret\"); raise SystemExit({status})'], os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release")
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, guard.FAILED_LOG)
                self.assertEqual(result.stderr, guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stdout.log").read_bytes(), guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stderr.log").read_bytes(), guard.FAILED_LOG)

    def test_overflow_drains_without_interrupting_maven(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                marker = Path(directory) / "completed"
                command = [sys.executable, "-c",
                           "import os\nfor fd in (1, 2):\n"
                           " os.write(fd, b'-Dpassword=\"first\\nsecret tail' * 100)\n"
                           f"open({str(marker)!r}, 'w').write('done')\nraise SystemExit({status})"]
                script = (
                    "import guard, os\nfrom pathlib import Path\nguard.LIMIT = 64\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5)
                self.assertEqual(result.returncode, status)
                self.assertEqual(marker.read_text(), "done")
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, guard.OVERFLOW_LOG)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_interruption_during_log_writes_does_not_replay_output(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            for partial in (False, True):
                with self.subTest(signum=signum, partial=partial), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    calls = Path(directory) / "writes"
                    command = [sys.executable, "-c",
                               "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n')"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        "original = guard.write_logs\n"
                        "def interrupted_write(clean, logs):\n"
                        f" with Path({str(calls)!r}).open('a') as out: out.write('called\\n')\n"
                        + (" logs.mkdir(parents=True, exist_ok=True)\n"
                           " with (logs / 'stdout.log').open('ab') as out: out.write(clean[0])\n"
                           if partial else " original(clean, logs)\n")
                        + f" guard.interrupted({int(signum)}, None)\n"
                        "guard.write_logs = interrupted_write\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertEqual(result.returncode, 128 + signum)
                    self.assertEqual(calls.read_text(), "called\n")
                    self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if partial:
                        self.assertFalse((logs / "stderr.log").exists())
                    else:
                        self.assertEqual(result.stdout, b"out\n")
                        self.assertEqual(result.stderr, b"err\n")
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_inherited_pipes_have_bounded_safe_capture(self):
        for complete_stderr in (False, True):
            with self.subTest(complete_stderr=complete_stderr), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "child"
                command = [sys.executable, "-c",
                           "import os, time\nchild = os.fork()\nif child == 0:\n"
                           + (" os.close(2)\n" if complete_stderr else "")
                           + " os.write(1, b'known-secret-prefix')\n"
                           " time.sleep(30)\n os._exit(0)\n"
                           f"open({str(pid_file)!r}, 'w').write(str(child))\n"
                           + ("os.write(2, b'-Dpassword=\"first\\nunknown secret tail\"')\n"
                              if complete_stderr else "os.write(2, b'-Dpassword=\"first\\nunknown secret tail')\n")
                           + "raise SystemExit(17)"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [b'known-secret-prefix-tail'], Path({directory!r})))"
                )
                started = time.monotonic()
                try:
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertLess(time.monotonic() - started, 4)
                    self.assertEqual(result.returncode, 17)
                    self.assertEqual(result.stdout, guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stdout.log").read_bytes(), result.stdout)
                    if complete_stderr:
                        self.assertEqual(result.stderr, b'-Dpassword=[REDACTED]')
                    else:
                        self.assertEqual(result.stderr, guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stderr.log").read_bytes(), result.stderr)
                finally:
                    if pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass


if __name__ == "__main__":
    unittest.main()
