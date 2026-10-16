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
from unittest.mock import Mock, patch
import urllib.parse
import xml.etree.ElementTree as ET

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


def policy_pom():
    # Root-only policy fixtures omit modules; reactor tests declare their own small tree.
    return re.sub(r"<modules>.*?</modules>", "",
                  (ROOT / "pom.xml").read_text(), flags=re.S)


class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

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
        for text in ("set -x", "use the -x switch", "-Debug"):
            guard.check_text(text, maven_options=False)

    def test_malformed_quoted_gpg_value_check_is_bounded(self):
        script = (
            "import guard\ntry:\n"
            " guard.check_text('-Dgpg.bestPractices=\"' + '\\\\a' * 64)\n"
            "except guard.Unsafe:\n raise SystemExit(0)\nraise SystemExit(1)"
        )
        result = subprocess.run([sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0)

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
            '-Dgpg.bestPractices="true"',
            "-Dgpg.bestPractices='true'",
            '"-Dgpg.bestPractices=true"',
            '-Dgpg."bestPractices"="true"',
        ):
            guard.check_text(value, jvm_options=True)
        guard.check_text("-Dgpg.bestPractices")  # Maven CLI bare properties mean true.
        for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            for value in ('-Dgpg.bestPractices="true false"', "-Dgpg.bestPractices"):
                with self.subTest(name=name, value=value), patch.dict(os.environ, {name: value}, clear=True), patch.object(
                    sys, "argv", ["guard.py", "run", "mvn", "validate"]
                ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_prose_quotes_do_not_hide_unsafe_gpg_options(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
            (root / "Makefile").write_text(
                "# isn't\nrelease-prepare:\n"
                '\tpython3 "$(RELEASE_GUARD)" run $(MVNCMD) release:prepare '
                "-D gpg.bestPractices=false\n# it's\n"
            )
            with self.assertRaisesRegex(guard.Unsafe, "GPG best practices"):
                guard.check_policy(root)

    def test_safe_gpg_option_in_shell_assignments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
            for prefix in ("", "${MAVEN_OPTS} "):
                (root / "Makefile").write_text(
                    'export MAVEN_OPTS="' + prefix
                    + '-Dgpg.bestPractices=true -DskipTests=true"\n'
                )
                with self.subTest(prefix=prefix):
                    guard.check_policy(root)

    def test_gpg_boolean_quotes_follow_the_jvm_option_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
            (root / ".mvn").mkdir()
            with patch.object(guard, "Path", side_effect=lambda value: root / value), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                for name, quoted_status in (
                    ("MAVEN_OPTS", 2), ("MAVEN_DEBUG_OPTS", 2), ("JAVA_TOOL_OPTIONS", 0),
                    ("JDK_JAVA_OPTIONS", 0), ("_JAVA_OPTIONS", 0),
                ):
                    for value, expected in (
                        ('-Dgpg.bestPractices="true"', quoted_status),
                        ("-Dgpg.bestPractices='true'", quoted_status),
                        ("-Dgpg.bestPractices=true", 0),
                        ("-D=gpg.bestPractices=false", 0),
                    ):
                        capture.reset_mock()
                        with self.subTest(source=name, value=value), patch.dict(os.environ, {name: value}, clear=True):
                            self.assertEqual(guard.main(), expected)
                            self.assertEqual(capture.called, expected == 0)
                for value, expected in (
                    ('-Dgpg.bestPractices="true"', 2), ("-Dgpg.bestPractices='true'", 2),
                    ("-Dgpg.bestPractices=true", 0), ("-D=gpg.bestPractices=false", 0),
                ):
                    (root / ".mvn/jvm.config").write_text(value)
                    capture.reset_mock()
                    with self.subTest(source="jvm.config", value=value), patch.dict(os.environ, {}, clear=True):
                        if expected == 0:
                            guard.check_policy(root)
                            self.assertEqual(guard.main(), 0)
                            capture.assert_called_once()
                        else:
                            with self.assertRaisesRegex(guard.Unsafe, "GPG best practices"):
                                guard.check_policy(root)
                            self.assertEqual(guard.main(), 2)
                            capture.assert_not_called()

    def test_log_file_checks_apply_only_to_maven_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = re.sub(r"<description>.*?</description>",
                         "<description>Use -l for locale</description>",
                         policy_pom(), count=1, flags=re.S)
            (root / "pom.xml").write_text(pom)
            (root / ".mvn").mkdir()
            (root / ".mvn/jvm.config").write_text("-Dfoo=-l")
            guard.check_policy(root)
            with patch.object(guard, "Path", side_effect=lambda value: root / value):
                for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
                    with self.subTest(source=name), patch.dict(os.environ, {name: "-Dfoo=-l"}, clear=True):
                        guard.check_runtime_options()
                for value in ("-lraw.log", "--log-file=raw.log"):
                    with self.subTest(value=value), patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True):
                        with self.assertRaises(guard.Unsafe):
                            guard.check_runtime_options()
                    (root / ".mvn/maven.config").write_text(value)
                    with patch.dict(os.environ, {}, clear=True), self.assertRaises(guard.Unsafe):
                        guard.check_runtime_options()
                    (root / ".mvn/maven.config").unlink()

    def test_known_release_properties_from_jvm_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
            (root / ".mvn").mkdir()
            config = root / ".mvn/jvm.config"
            with patch.object(guard, "Path", side_effect=lambda value: root / value), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                for source, value, expected in (
                    ("MAVEN_OPTS", "-Darguments=-Xvalidate", 2),
                    ("JAVA_TOOL_OPTIONS", "-Darguments=-Xvalidate", 2),
                    ("jvm.config", "-Darguments=-Xvalidate", 2),
                    ("MAVEN_OPTS", "-Dgoals=deploy,-X", 2),
                    ("_JAVA_OPTIONS", "-DpreparationGoals=clean,verify,-X", 2),
                    ("jvm.config", "-DcompletionGoals=verify,--l,raw.log", 2),
                    ("JAVA_TOOL_OPTIONS", "-Darguments=-D=password=dummy-value", 2),
                    ("MAVEN_OPTS", "-Darguments=--D=javax.net.debug=all", 2),
                    ("jvm.config", "-Dgoals=deploy,-D=gpg.bestPractices=false", 2),
                    ("MAVEN_OPTS", "-Dgoals=deploy -Dfoo=-l -Xmx512m", 0),
                    ("JAVA_TOOL_OPTIONS", "-Dgoals=deploy", 0),
                    ("JAVA_TOOL_OPTIONS", '-Darguments="-Dfoo=-l"', 0),
                    ("MAVEN_OPTS", "-Darguments=-Dfoo=-X", 0),
                    ("jvm.config", "-D=goals=deploy,-X", 0),
                    ("MAVEN_OPTS", '-Dgoals="deploy,-X"', 0),
                    ("JAVA_TOOL_OPTIONS", '-Dgoals="deploy,-X"', 2),
                ):
                    config.unlink(missing_ok=True)
                    if source == "jvm.config":
                        config.write_text(value)
                    env = {} if source == "jvm.config" else {source: value}
                    capture.reset_mock()
                    with self.subTest(source=source, value=value), patch.dict(os.environ, env, clear=True):
                        self.assertEqual(guard.main(), expected)
                        self.assertEqual(capture.called, expected == 0)
                        if expected == 0:
                            self.assertEqual(capture.call_args.args[0], ["mvn", "validate"])
                            if source != "jvm.config":
                                self.assertEqual(capture.call_args.args[1][source], value)
                        if source == "jvm.config":
                            self.assertEqual(config.read_text(), value)
                            if expected:
                                with self.assertRaises(guard.Unsafe):
                                    guard.check_policy(root)
                            else:
                                guard.check_policy(root)

    def test_direct_gpg_argument_boundaries(self):
        for value, expected in (("true false", 2), ("true ", 2), ("true", 0)):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "-Dgpg.bestPractices=" + value]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(capture.called, expected == 0)

    def test_direct_gpg_aliases_and_nested_arguments(self):
        for option in ("-D", "-D=", "--D=", "-define=", "--define="):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                argument = option + "gpg.bestPractices=true false"
                guard.check_text(argument, maven_arguments=[argument])
        with self.assertRaises(guard.Unsafe):
            guard.check_text("-D gpg.bestPractices=true false",
                             maven_arguments=["-D", "gpg.bestPractices=true false"])
        for nested in (
            "-Darguments=-Dgpg.bestPractices=true -DskipTests=true",
            '-Darguments="-Dgpg.bestPractices=true"',
        ):
            guard.check_text(nested, maven_arguments=[nested])
        for nested in (
            "-Darguments=\"-Dgpg.bestPractices='true false'\"",
            '-Darguments="-Dgpg.bestPractices=true -DskipTests=true"',
        ):
            with self.assertRaises(guard.Unsafe):
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

    def test_maven_argument_sources_reject_attached_debug_and_log_options(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            config = root / ".mvn/maven.config"
            with patch.object(
                guard, "Path", side_effect=lambda value: root / value if value == ".mvn" else Path(value)
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                for arguments, expected in (
                    (["-B-X"], 2), (["-e-l", "raw.log"], 2),
                    (["--X"], 2), (["--l", "raw.log"], 2),
                    (["-B--debug"], 2), (["-B--log-file", "raw.log"], 2),
                    (["-B--X"], 2), (["-B--l", "raw.log"], 2),
                    (["-B-log-file", "raw.log"], 2), (["-ntp=--l", "raw.log"], 2),
                    (["-llr"], 0), (["--llr"], 0), (["-B-llr"], 0),
                    (["--legacy-local-repository"], 0),
                    (["-pl", "driver-core"], 0), (["-B-pl", "driver-core"], 0),
                    (["-Dfoo=pre-load"], 0),
                    (["-Dfoo=-l"], 0), (["-Dfoo=-X"], 0),
                    (["--define=foo=-l"], 0), (["-define", "foo=-l"], 0),
                    (["-D", "foo=-l"], 0), (["-D=foo=-l"], 0), (["--D=foo=-l"], 0),
                    (["-Dfoo=--l"], 0), (["-Dfoo=-B-l"], 0),
                    (["-D=gpg.bestPractices=false"], 2), (["--D=gpg.bestPractices=false"], 2),
                    (["-D=gpg.bestPractices=true"], 0), (["--D=gpg.bestPractices=true"], 0),
                    (["--D=gpg.bestPractices="], 2),
                    (["-D=password=dummy-value"], 2), (["--D=javax.net.debug=all"], 2),
                    (["--D=org.slf4j.simpleLogger.logFile=raw.log"], 2),
                    (["-Darguments=-X"], 2), (["-Darguments=-l raw.log"], 2),
                    (["--D=arguments=-l raw.log"], 2),
                    (["-Darguments=-Xvalidate"], 2), (["--D=arguments=-Xvalidate"], 2),
                    (['"-Xvalidate"'], 2), (['"-Darguments=-Xvalidate"'], 2),
                    (['"-Dfoo=-l"'], 0),
                    (['"-Dfoo=some', "-l", "raw.log", '-X"'], 0),
                    (["-Darguments=-Dfoo=-l"], 0), (["-Darguments=-Dfoo=-X"], 0),
                    (["-Darguments=-Dfoo=some -X"], 2),
                    (["-Darguments=-Dfoo=some -l raw.log"], 2),
                    (["-Darguments=-Dfoo=some --l raw.log"], 2),
                    (["-Darguments=-Dfoo=some -B-l raw.log"], 2),
                    (["-Darguments=-Dfoo=some -ntp=--l raw.log"], 2),
                    (["-Dgoals=deploy,-X"], 2),
                    (["-DpreparationGoals=clean,-Xvalidate"], 2),
                    (["-DcompletionGoals=verify,--l,raw.log"], 2),
                    (['-Dgoals=deploy,-Dfoo="some -l raw.log"'], 2),
                    (["-Dgoals=deploy,-Dfoo=-l"], 0), (["-Dgoals=deploy,-Dfoo=-X"], 0),
                    (["-Dgoals=deploy"], 0), (['"-DpreparationGoals=clean verify"'], 0),
                    (["-DGoals=deploy,-X"], 0),
                    (["-Darguments=-Dgoals=deploy,-X"], 0),
                ):
                    for source in ("cli", "MAVEN_ARGS", "maven.config"):
                        config.unlink(missing_ok=True)
                        if source == "maven.config":
                            config.write_text("\n".join(arguments))
                        env = {"MAVEN_ARGS": " ".join(arguments)} if source == "MAVEN_ARGS" else {}
                        argv = ["guard.py", "run", "mvn"] + (arguments if source == "cli" else ["validate"])
                        capture.reset_mock()
                        with self.subTest(source=source, arguments=arguments), patch.dict(os.environ, env, clear=True), patch.object(
                            sys, "argv", argv
                        ):
                            self.assertEqual(guard.main(), expected)
                            self.assertEqual(capture.called, expected == 0)

    def test_maven_property_values_preserve_argument_boundaries(self):
        for arguments, expected in (
            (["-Dfoo=some -l raw.log -X"], 0),
            (["-Dfoo=some", "-l", "raw.log"], 2),
            (['-Darguments=-Dfoo="some -l raw.log -X"'], 0),
            (["-Darguments=-Dfoo='some -l raw.log -X'"], 0),
            (['-Darguments=-Dfoo="back\\slash -l raw.log"'], 0),
            (['-Darguments=-Dfoo="trailing\\"'], 0),
        ):
            with self.subTest(arguments=arguments), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn"] + arguments
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(capture.called, expected == 0)
                if expected == 0:
                    self.assertEqual(capture.call_args.args[0], ["mvn"] + arguments)

    def test_unwrapped_release_goals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
            for maven in ("$(MVNCMD)", "${MVNCMD}", "mvn"):
                for goal in ("prepare", "perform"):
                    for options in ("", "-B ", "-B -ntp "):
                        for separator in (" ", " \\\n\t"):
                            command = f"{maven}{separator}{options}release:{goal}\n"
                            (root / "Makefile").write_text("\t" + command)
                            with self.subTest(maven=maven, goal=goal, options=options, separator=separator):
                                with self.assertRaises(guard.Unsafe):
                                    guard.check_policy(root)
                                (root / "Makefile").write_text('\tpython3 "$(RELEASE_GUARD)" run ' + command)
                                guard.check_policy(root)

    def test_root_gpg_boolean_normalization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            for value, enabled in (("true", True), ("TRUE", True), ("\n true\n", True),
                                   ("false", False), ("", False)):
                (root / "pom.xml").write_text(policy_pom().replace(
                    "<gpg.bestPractices>true</gpg.bestPractices>",
                    f"<gpg.bestPractices>{value}</gpg.bestPractices>", 1))
                with self.subTest(value=value):
                    if enabled:
                        guard.check_policy(root)
                    else:
                        with self.assertRaisesRegex(guard.Unsafe, "enable GPG best practices"):
                            guard.check_policy(root)

    def test_profile_and_plugin_management_declarations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            namespace = "{http://maven.apache.org/POM/4.0.0}"
            for location in (
                ("build", "pluginManagement", "plugins"),
                ("profiles", "profile", "build", "plugins"),
                ("profiles", "profile", "build", "pluginManagement", "plugins"),
            ):
                pom = ET.fromstring(policy_pom())
                plugins = pom.find(f"{namespace}build/{namespace}plugins")
                central = next(plugin for plugin in plugins
                               if plugin.findtext(namespace + "artifactId") == "central-publishing-maven-plugin")
                plugins.remove(central)
                target = pom
                for tag in location:
                    child = target.find(namespace + tag)
                    if child is None:
                        child = ET.SubElement(target, namespace + tag)
                    target = child
                target.append(central)
                ET.ElementTree(pom).write(root / "pom.xml")
                with self.subTest(location=location):
                    guard.check_policy(root)
                    version = central.find(namespace + "version")
                    safe_version = version.text
                    version.text = "0.10.0"
                    ET.ElementTree(pom).write(root / "pom.xml")
                    with self.assertRaisesRegex(guard.Unsafe, "update Central publishing"):
                        guard.check_policy(root)
                    version.text = safe_version
                    gpg = ET.SubElement(target, namespace + "plugin")
                    ET.SubElement(gpg, namespace + "artifactId").text = "maven-gpg-plugin"
                    config = ET.SubElement(gpg, namespace + "configuration")
                    ET.SubElement(config, namespace + "bestPractices").text = "false"
                    ET.ElementTree(pom).write(root / "pom.xml")
                    with self.assertRaisesRegex(guard.Unsafe, "GPG best practices"):
                        guard.check_policy(root)

    def test_central_version_must_be_pinned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            original = policy_pom()
            for version in ("", " \n ", None):
                pom = with_central_version(original, version or "")
                if version is None:
                    pom = CENTRAL_VERSION.sub(lambda match: match[1][:-len("<version>")], original)
                (root / "pom.xml").write_text(pom)
                with self.subTest(version=version), self.assertRaisesRegex(guard.Unsafe, "pin Central publishing"):
                    guard.check_policy(root)

    def test_root_pom_must_declare_central_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = ET.fromstring(policy_pom())
            ns = {"m": "http://maven.apache.org/POM/4.0.0"}
            plugins = pom.find("m:build/m:plugins", ns)
            central = next(plugin for plugin in plugins
                           if plugin.findtext("m:artifactId", namespaces=ns) == "central-publishing-maven-plugin")
            plugins.remove(central)
            ET.ElementTree(pom).write(root / "pom.xml")
            with self.assertRaisesRegex(guard.Unsafe, "pin Central publishing"):
                guard.check_policy(root)

    def test_reactor_poms_inherit_defaults_but_reject_unsafe_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom().replace(
                "</project>", "<modules><module>child</module><module>child</module></modules></project>"))
            nested = root / "child/nested"
            nested.mkdir(parents=True)
            project = '<project xmlns="http://maven.apache.org/POM/4.0.0">{}</project>'
            (root / "child/pom.xml").write_text(project.format("<modules><module>nested</module></modules>"))
            for settings in (
                "",
                "<properties><gpg.bestPractices>false</gpg.bestPractices></properties>",
                "<build><plugins><plugin><artifactId>maven-gpg-plugin</artifactId>"
                "<configuration><bestPractices>false</bestPractices></configuration></plugin></plugins></build>",
                "<build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId>"
                "<version>0.8.0</version></plugin></plugins></build>",
            ):
                (nested / "pom.xml").write_text(project.format(settings))
                with self.subTest(settings=settings):
                    if settings:
                        with self.assertRaises(guard.Unsafe):
                            guard.check_policy(root)
                    else:
                        guard.check_policy(root)
            (nested / "pom.xml").unlink()
            with self.assertRaises(OSError):
                guard.check_policy(root)

    def test_reactor_policy_checks_do_not_depend_on_xml_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = policy_pom().replace("</project>", "<modules><module>child</module></modules></project>")
            (root / "pom.xml").write_text(pom)
            nested = root / "child/nested"
            nested.mkdir(parents=True)
            qualified = '<project xmlns="http://maven.apache.org/POM/4.0.0">{}</project>'
            unsafe = "<properties><gpg.bestPractices>false</gpg.bestPractices></properties>"
            for namespace in ("", ' xmlns="urn:alternate-pom"'):
                project = "<project" + namespace + ">{}</project>"
                modules = "<modules><module>nested</module></modules>"
                (nested / "pom.xml").write_text(qualified.format(""))
                for settings in (
                    "", unsafe,
                    "<build><plugins><plugin><artifactId>maven-gpg-plugin</artifactId>"
                    "<configuration><bestPractices>false</bestPractices></configuration></plugin></plugins></build>",
                    "<build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId>"
                    "<version>0.8.0</version></plugin></plugins></build>",
                ):
                    (root / "child/pom.xml").write_text(project.format(settings + modules))
                    with self.subTest(namespace=namespace, settings=settings):
                        if settings:
                            with self.assertRaises(guard.Unsafe):
                                guard.check_policy(root)
                        else:
                            guard.check_policy(root)
                (root / "child/pom.xml").write_text(project.format(modules))
                (nested / "pom.xml").write_text(qualified.format(unsafe))
                with self.subTest(namespace=namespace), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
            (nested / "pom.xml").write_text(qualified.format(""))
            (root / "pom.xml").write_text(pom.replace('xmlns="http://maven.apache.org/POM/4.0.0"', ""))
            guard.check_policy(root)
            (root / "child/pom.xml").write_text('<document xmlns="urn:alternate-pom"/>')
            with self.assertRaisesRegex(guard.Unsafe, "project POM"):
                guard.check_policy(root)

    def test_plugin_payload_is_not_a_maven_declaration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            payload = (
                '<custom xmlns="urn:payload"><gpg.bestPractices>false</gpg.bestPractices>'
                '<modules><module>not-a-reactor-module</module></modules>'
                '<plugin><artifactId>central-publishing-maven-plugin</artifactId>'
                '<version>0.8.0</version></plugin></custom>'
            )
            pom = policy_pom().replace(
                "</plugins>",
                "<plugin><artifactId>maven-release-plugin</artifactId><configuration>"
                + payload + "</configuration></plugin></plugins>", 1,
            )
            (root / "pom.xml").write_text(pom)
            guard.check_policy(root)

    def test_old_release_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = policy_pom()
            for case, text, message in (
                ("bestPractices", pom.replace("<gpg.bestPractices>true</gpg.bestPractices>", ""),
                 "enable GPG best practices"),
                ("central 0.8.0", with_central_version(pom, "0.8.0"), "update Central publishing"),
                ("central 0.10.0", with_central_version(pom, "0.10.0"),
                 "update Central publishing"),
                ("central property", with_central_version(pom, "${central.version}"), "update Central publishing"),
                ("central incomplete", with_central_version(pom, "0.12"), "update Central publishing"),
                ("central snapshot", with_central_version(pom, "0.12.0-SNAPSHOT"), "update Central publishing"),
            ):
                (root / "pom.xml").write_text(text)
                with self.subTest(case=case), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)

    def test_disabled_gpg_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            for value in ("false", "no", "0", "", "true"):
                pom = policy_pom().replace(
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
            shutil.copytree(ROOT / ".github", root / ".github")
            for value in ("false", "no", "0", "", "true"):
                pom = policy_pom().replace(
                    "<profiles>",
                    "<profiles><profile><id>unsafe</id><properties>"
                    f"<gpg.bestPractices>{value}</gpg.bestPractices></properties></profile>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value):
                    if value == "true":
                        guard.check_policy(root)
                    else:
                        with self.assertRaisesRegex(guard.Unsafe, "GPG best practices must not be disabled"):
                            guard.check_policy(root)

    def test_newer_central_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = policy_pom()
            for version in ("0.11.0", "0.11.1", "0.12.0", "1.0.0", " \n 0.12.0 \n "):
                (root / "pom.xml").write_text(with_central_version(pom, version))
                with self.subTest(version=version):
                    guard.check_policy(root)

    def test_workflow_policy_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            (root / "pom.xml").write_text(policy_pom())
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
        arguments = ["-B", "-Darguments=-Dfoo=some -l raw.log", "-ntp"]
        with patch("sys.stderr", new_callable=io.StringIO) as output, self.assertRaises(guard.Unsafe):
            guard.check_text("\n".join(arguments), ".mvn/maven.config", maven_arguments=arguments)
        self.assertIn("at .mvn/maven.config;", output.getvalue())
        self.assertNotRegex(output.getvalue(), r"maven\.config:\d")

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
                child = None
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
                        process.terminate()
                    if child is None and ready.exists():
                        child = int(ready.read_text())
                    if child is not None:
                        try:
                            os.killpg(child, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)

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

    def test_quoted_authorization_headers(self):
        for header in ("Authorization", "Proxy-Authorization"):
            for quote in ("", "'", '"'):
                with self.subTest(header=header, quote=quote):
                    data = f"{quote}{header}{quote}: {quote}Bearer unknown{quote}\n".encode()
                    self.assertNotIn(b"unknown", guard.redact(data, []))

    def test_quoted_credential_arguments(self):
        for option in (b"-Dpassword=", b"-D=password=", b"--D=token=",
                       b"--define=token=", b"--define secret=", b"-define=token=", b"-define secret="):
            with self.subTest(option=option):
                self.assertEqual(guard.redact(option + b"unregistered-value", []), option + b"[REDACTED]")
            for quote in (b"'", b'"'):
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

    def test_unquoted_escaped_whitespace_credentials(self):
        for whitespace in (b" ", b"\t", b"\n", b"\r", b"\r\n", b"\v", b"\f"):
            for option in (b"-Dpassword=", b"--define=token=", b"-define secret="):
                for ending in (b"", b"\\"):
                    with self.subTest(whitespace=whitespace, option=option, ending=ending):
                        value = b"first\\" + whitespace + b"secret-tail" + ending
                        diagnostic = b"\nordinary following diagnostic\n"
                        self.assertEqual(guard.redact(option + value + diagnostic, []),
                                         option + b"[REDACTED]" + diagnostic if not ending
                                         else option + b"[REDACTED] following diagnostic\n")
                        self.assertEqual(guard.redact(option + value, []), option + b"[REDACTED]")
        self.assertEqual(guard.redact(b"-Dpassword=\\\n", []), b"-Dpassword=[REDACTED]")
        self.assertEqual(guard.redact(b"-Dpassword=\\", []), b"-Dpassword=[REDACTED]")

    def test_json_url_secret_variants_in_all_capture_outputs(self):
        secret = "pa/ss<&word +é"
        forms = guard.secret_forms({"SONATYPE_TOKEN_PASSWORD": secret})
        variants = {urllib.parse.quote(secret, safe=""), urllib.parse.quote(secret),
                    urllib.parse.quote_plus(secret, safe="")}
        variants |= {value.lower() for value in variants}
        payloads = []
        for value in sorted(variants):
            for slash_escape in (False, True):
                serialized = json.dumps(value)
                if slash_escape:
                    serialized = serialized.replace("/", "\\/")
                for payload in (serialized, serialized[1:-1]):
                    with self.subTest(payload=payload):
                        self.assertIn(b"[REDACTED]", guard.redact(payload.encode(), forms))
                        self.assertNotIn(b"word", guard.redact(payload.encode(), forms))
                    payloads.append(payload.encode())
        # Both fixes traverse the real capture path to both consoles and artifacts.
        payloads.extend(b"-Dpassword=first\\" + whitespace + b"secret-tail"
                        for whitespace in (b" ", b"\t", b"\n", b"\r\n"))
        data = b"\n".join(payloads) + b"\nordinary following diagnostic\n"
        expected = guard.redact(data, forms)
        self.assertNotIn(b"secret-tail", expected)
        self.assertNotIn(b"pa", expected.replace(b"-Dpassword=", b""))
        self.assertTrue(expected.endswith(b"\nordinary following diagnostic\n"))
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c", "import os\nfor fd in (1, 2):\n"
                       f" os.write(fd, {data!r})\nraise SystemExit(17)"]
            script = ("import guard, os\nfrom pathlib import Path\n"
                      f"raise SystemExit(guard.capture({command!r}, os.environ, "
                      f"guard.secret_forms({dict(SONATYPE_TOKEN_PASSWORD=secret)!r}), Path({directory!r})))")
            result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                    cwd=ROOT / "scripts/release", timeout=5)
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, expected)
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), expected)

    def test_unclosed_quoted_credentials_are_redacted_through_eof(self):
        for quote in (b"'", b'"'):
            for prefix in (b"-Dpassword=" + quote, quote + b"-Dpassword="):
                for payload in (b"first\nsecret tail", b"first\\" + quote + b"\nsecret tail"):
                    for ending in (b"", b"\\"):
                        with self.subTest(prefix=prefix, payload=payload, ending=ending):
                            result = guard.redact(prefix + payload + ending, [])
                            self.assertNotIn(b"first", result)
                            self.assertNotIn(b"secret tail", result)
                            self.assertTrue(result.endswith(b"[REDACTED]"))
                    diagnostic = b"\nordinary following diagnostic\n"
                    result = guard.redact(prefix + payload + quote + diagnostic, [])
                    self.assertNotIn(b"secret tail", result)
                    self.assertTrue(result.endswith(diagnostic))

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
            expected_artifact = b""
            for marker, status in (("first", 0), ("second", 17)):
                # Real captures share logs so earlier sanitized invocations must remain.
                command = [
                    sys.executable, "-c",
                    "import os\nfor fd in (1, 2):\n"
                    f" for part in ({(marker + ': ').encode()!r}, b'dummy-', b'secret\\n'):\n"
                    f"  os.write(fd, part)\nraise SystemExit({status})",
                ]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, "
                    f"guard.secret_forms(os.environ), Path({directory!r})))"
                )
                result = subprocess.run(
                    [sys.executable, "-c", script], capture_output=True,
                    cwd=ROOT / "scripts/release",
                    env=dict(os.environ, MAVEN_GPG_PASSPHRASE="dummy-secret"), timeout=5,
                )
                self.assertEqual(result.returncode, status)
                expected_console = marker.encode() + b": [REDACTED]\n"
                expected_artifact += expected_console
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, expected_console)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), expected_artifact)

    def test_completed_eof_redacts_unclosed_credentials_and_preserves_status(self):
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       "import os\nfor fd in (1, 2):\n"
                       " os.write(fd, b'-Dpassword=\"first\\nsecret tail')\nraise SystemExit(17)"]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({directory!r})))"
            )
            result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                    cwd=ROOT / "scripts/release", timeout=5)
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b"-Dpassword=[REDACTED]")
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

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
                            timeout=5,
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

    def test_capture_io_failure_preserves_only_completed_maven_status(self):
        for operation in ("select", "read"):
            for status in (0, 17, -signal.SIGTERM, None):
                for error in (OSError("synthetic-secret"), ValueError("synthetic-secret"),
                              KeyboardInterrupt(), SystemExit(143)):
                    with self.subTest(operation=operation, status=status, error=type(error).__name__):
                        process = Mock()
                        process.poll.return_value = status
                        process.wait.return_value = status if status is not None else -signal.SIGTERM
                        selector = Mock()
                        selector.get_map.return_value = {1: object()}
                        key = Mock(data=0, fileobj=process.stdout)
                        selector.select.return_value = [(key, 1)]
                        if operation == "select":
                            selector.select.side_effect = error
                        with patch.object(guard.subprocess, "Popen", return_value=process), patch.object(
                            guard.selectors, "DefaultSelector"
                        ) as factory, patch.object(guard.os, "read", side_effect=error), patch.object(
                            guard.os, "killpg"
                        ) as killpg, patch.object(guard, "write_logs") as write_logs:
                            factory.return_value.__enter__.return_value = selector
                            write_logs.side_effect = lambda *_: self.assertEqual(killpg.call_count, 2)
                            if status is not None and isinstance(error, Exception):
                                self.assertEqual(guard.capture(["mvn"], {}, [], Path("unused")),
                                                 status if status >= 0 else 128 - status)
                            else:
                                with self.assertRaises(type(error)):
                                    guard.capture(["mvn"], {}, [], Path("unused"))
                            self.assertEqual([call.args for call in killpg.call_args_list], [
                                (process.pid, signal.SIGTERM), (process.pid, signal.SIGKILL)])
                            diagnostic = (guard.CANCELLED_LOG if isinstance(
                                error, (KeyboardInterrupt, SystemExit)) else guard.FAILED_LOG)
                            write_logs.assert_called_once_with([diagnostic, diagnostic], Path("unused"))

    def test_missing_or_detached_console_keeps_both_artifacts(self):
        for name in ("stdout", "stderr"):
            for failure in ("missing", "detached"):
                with self.subTest(stream=name, failure=failure), tempfile.TemporaryDirectory() as directory:
                    broken = None
                    if failure == "detached":
                        broken = io.TextIOWrapper(io.BytesIO())
                        broken.detach()
                    other = "stderr" if name == "stdout" else "stdout"
                    good = Mock()
                    good.buffer = io.BytesIO()
                    logs = Path(directory)
                    with patch.object(sys, name, broken), patch.object(sys, other, good), patch.object(
                        guard.os, "write"
                    ) as notice:
                        guard.write_logs([b"out\n", b"err\n"], logs)
                    self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")
                    self.assertEqual(good.buffer.getvalue(), b"err\n" if other == "stderr" else b"out\n")
                    notice.assert_called_once_with(
                        2, b"Sanitized release output could not be fully written; values withheld.\n")

    def test_sanitization_failure_writes_fixed_logs(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                child = f"import sys; print('secret'); sys.exit({status})"
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    "guard.redact = lambda *_: (_ for _ in ()).throw(ValueError('secret'))\n"
                    f"raise SystemExit(guard.capture([{sys.executable!r}, '-c', {child!r}], os.environ, [], Path({directory!r})))\n"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5)
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, guard.FAILED_LOG)
                self.assertEqual(result.stderr, guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stdout.log").read_bytes(), guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stderr.log").read_bytes(), guard.FAILED_LOG)

    def test_json_escaped_environment_secrets(self):
        secret = 'synthetic "quote" \\ slash/\nline é'
        forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
        for ensure_ascii in (True, False):
            value = json.dumps({"message": secret}, ensure_ascii=ensure_ascii)
            for escaped in (value, value.replace("/", "\\/")):
                with self.subTest(ensure_ascii=ensure_ascii, escaped_slash="\\/" in escaped):
                    sanitized = guard.redact(escaped.encode(), forms)
                    self.assertIn(b"[REDACTED]", sanitized)
                    self.assertNotIn(b"synthetic", sanitized)

    def test_precredential_check_rejects_unsafe_runtime_environment(self):
        for name, value in (
            ("MAVEN_ARGS", "-X"), ("MAVEN_OPTS", "-Dgpg.bestPractices=false"),
            ("JAVA_TOOL_OPTIONS", "-Djavax.net.debug=all"),
            ("JDK_JAVA_OPTIONS", "-Dgpg.bestPractices=false"),
            ("_JAVA_OPTIONS", "-Dgpg.bestPractices=false"),
            ("MAVEN_DEBUG_OPTS", "--debug"), ("MAVEN_CONFIG", "--debug"),
        ):
            with self.subTest(name=name), patch.dict(os.environ, {name: value}, clear=True), patch.object(
                sys, "argv", ["guard.py", "check"]
            ), patch.object(guard, "check_policy"), patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)

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
