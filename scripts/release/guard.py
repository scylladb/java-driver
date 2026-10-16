#!/usr/bin/env python3
"""Check release defaults and capture Maven output without writing raw credentials.

Output is buffered until Maven exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Overflow and incomplete streams produce fixed diagnostics.
Child Maven processes run normally; they are not intercepted.
"""

import base64
import heapq
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import signal
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

LIMIT = 64 * 1024 * 1024
DRAIN_GRACE = 1
OVERFLOW_LOG = b"Maven output exceeded the capture limit; invocation output withheld.\n"
INCOMPLETE_LOG = b"Maven output stream did not close; incomplete output withheld.\n"
CANCELLED_LOG = b"Maven was interrupted; incomplete output withheld.\n"
FAILED_LOG = b"Sanitized Maven output capture failed; invocation output withheld.\n"
ENV_OPTIONS = (
    "MAVEN_ARGS",
    "MAVEN_OPTS",
    "JAVA_TOOL_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS",
    "MAVEN_DEBUG_OPTS",
    "MAVEN_CONFIG",
)
MAVEN_PROPERTY_PREFIX = r"(?:--?define(?:=|\s+)|--?D(?:=|\s*))"


class Unsafe(ValueError):
    pass


def _clean_maven_arguments(arguments):
    # Maven CleanArgument groups leading double quotes over existing argv units.
    # A closing quote on a later unit remains literal; this is not shell parsing.
    cleaned = []
    quoted = None
    for argument in arguments:
        if argument.startswith('"'):
            if quoted is not None:
                cleaned.append(quoted)
            quoted = argument[1:]
            if argument.endswith('"'):
                if not quoted:
                    raise Unsafe("invalid Maven argument quoting")
                cleaned.append(quoted[:-1])
                quoted = None
        elif quoted is not None:
            quoted += " " + argument
        else:
            cleaned.append(argument)
    if quoted is not None:
        cleaned.append(quoted)
    return cleaned


def _release_maven_arguments(value):
    # The known release arguments carrier uses Plexus setLine quote grouping.
    lexer = shlex.shlex(value, posix=True)
    lexer.whitespace = " "
    lexer.whitespace_split = True
    lexer.commenters = ""
    lexer.escape = ""
    try:
        return _clean_maven_arguments(list(lexer))
    except ValueError:
        raise Unsafe("invalid release argument quoting") from None


def _release_property_arguments(name, value):
    if name == "arguments":
        return _release_maven_arguments(value)
    if name in ("goals", "preparationGoals", "completionGoals"):
        # forked-path adds split goals with setValue, not Plexus setLine.
        goals = re.split(r"[, \n\r\t]+", value)
        return _clean_maven_arguments([goal for goal in goals if goal])
    return None


def check_text(text, source=None, maven_options=True, maven_arguments=(), jvm_options=False,
               literal_jvm_options=False):
    option_arguments = []
    jvm_option_arguments = []
    views = [(_clean_maven_arguments(maven_arguments), True)] if maven_arguments else []
    if jvm_options:
        # The Maven shell launcher preserves quotes in these property values;
        # Java's own option environment variables interpret quotes as delimiters.
        try:
            arguments = text.split() if literal_jvm_options else shlex.split(text)
        except ValueError:
            raise Unsafe("invalid JVM option quoting") from None
        for argument in arguments:
            setting = re.fullmatch(r"-Dgpg\.bestPractices(?:=(.*))?", argument, re.I | re.S)
            if setting and (setting[1] or "").lower() != "true":
                raise Unsafe("GPG best practices must not be disabled")
            # Java defines exact keys with -D, without Maven's aliases or empty-key form.
            setting = re.fullmatch(r"-D([^=]+)(?:=(.*))?", argument, re.S)
            child = _release_property_arguments(setting[1], setting[2] or "") if setting else None
            if child is not None:
                views.append((child, False))
                jvm_option_arguments.append("")
            else:
                jvm_option_arguments.append(argument)
    for arguments, forward_release in views:
        if any(arg.strip().startswith("-X") for arg in arguments):
            raise Unsafe("Maven debug options are not allowed")
        for index, argument in enumerate(arguments):
            if index and arguments[index - 1] in ("-D", "--D", "-define", "--define"):
                argument = arguments[index - 1] + " " + argument
            setting = re.fullmatch(
                MAVEN_PROPERTY_PREFIX + r"([^=]+)(?:=(.*))?", argument, re.S
            )
            if setting:
                name = setting[1]
                if (name.translate(str.maketrans("", "", "\"'")).lower() == "gpg.bestpractices"
                        and setting[2] is not None and setting[2].lower() != "true"):
                    raise Unsafe("GPG best practices must not be disabled")
                if forward_release:
                    child = _release_property_arguments(name, setting[2] or "")
                    if child is not None:
                        views.append((child, False))
                option_arguments.append("")
            else:
                option_arguments.append(argument if argument.startswith("-") else "")
    # Preserve whitespace only inside quoted GPG values. Quotes around shell
    # assignments or unrelated prose must not hide later option boundaries.
    property_prefix = r"-D\s*" if jvm_options else MAVEN_PROPERTY_PREFIX
    gpg_name = r"[\"']*" + r"[\"']*".join(re.escape(char) for char in "gpg.bestPractices") + r"[\"']*"
    gpg_text = re.sub(
        r"(" + property_prefix + gpg_name + r"=)([\"'])((?:\\[\s\S]|(?!\2)[^\\])*)\2",
        lambda match: match[1] + match[2] + re.sub(r"\s", "\0", match[3]) + match[2],
        text, flags=re.I,
    ).translate(str.maketrans("", "", "\"'"))
    if re.search(property_prefix + r"gpg\.bestPractices=(?!true(?:\s|$))",
                 gpg_text, re.I) or (jvm_options and re.search(
                     r"(?:^|\s)-Dgpg\.bestPractices(?:\s|$)", gpg_text, re.I)):
        raise Unsafe("GPG best practices must not be disabled")
    # Ignore quoting for conservative debug and credential-argument detection.
    text = text.translate(str.maketrans("", "", "\"'"))
    maven_option_text = "\n".join(option_arguments).translate(str.maketrans("", "", "\"'"))
    option_text = text
    if maven_arguments:
        option_text = maven_option_text
    elif jvm_options:
        option_text = "\n".join(jvm_option_arguments).translate(str.maketrans("", "", "\"'"))
    # Dedicated property checks still inspect the full input, including values.
    property_sources = [(text, property_prefix, True)]
    if jvm_options and views:
        # Only extracted child argv uses Maven property syntax, not the Java parent.
        child_text = "\n".join(argument for arguments, _ in views for argument in arguments)
        property_sources.append((child_text.translate(str.maketrans("", "", "\"'")),
                                 MAVEN_PROPERTY_PREFIX, False))
    match = None
    for diagnostic_text, prefix, physical_line in property_sources:
        match = re.search(
            prefix + r"javax\.net\.debug\b"
            r"|simpleLogger\.logFile="
            r"|simpleLogger[^\s<>]*=(?:debug|trace)\b"
            r"|" + prefix + r"[\w.-]*(?:passphrase|password|token|secret|authorization)[\w.-]*=",
            diagnostic_text,
            re.I,
        )
        if match:
            break
    if not match:
        diagnostic_text = option_text
        physical_line = not (maven_arguments or jvm_options)
        match = re.search(r"(?:^|[\s=\"'])(?:-X|--?debug)\b", option_text)
    if (maven_options or views) and not match:
        # Catch Maven aliases and one attached boolean prefix, such as -B-l.
        if views:
            diagnostic_text = maven_option_text
            physical_line = False
        match = re.search(
            r"(?:^|[\s=\"'])(?:-[BCNUVXcehoqv])?"
            r"(?:--?X\b|--?debug\b|--?log-file(?:[=\s]|$)"
            r"|--l(?:[=\s]|$)|-l(?!lr(?:[\s\"']|$)))", diagnostic_text
        )
    if match:
        if source is not None:
            location = str(source)
            if physical_line:
                start = match.start() + len(match[0]) - len(match[0].lstrip())
                location += ":" + str(diagnostic_text.count("\n", 0, start) + 1)
            print(f"Unsafe release options at {location}; values withheld.", file=sys.stderr)
        raise Unsafe("debug logging or credential arguments are not allowed")


def check_pom(pom, is_root):
    if pom.tag.rsplit("}", 1)[-1] != "project":
        raise Unsafe("expected a Maven project POM")
    # Maven accepts qualified and unqualified model elements. QName wildcards
    # inspect either form without rewriting the parsed XML names.
    if is_root and (pom.findtext("{*}properties/{*}gpg.bestPractices") or "").strip().lower() != "true":
        raise Unsafe("enable GPG best practices before releasing this target")
    # Plugin configuration can contain arbitrary XML, including model-like names.
    # Inspect only declaration positions in the project and its profiles.
    sections = (pom, *pom.findall("{*}profiles/{*}profile"))
    plugins = [plugin for section in sections for location in (
        "{*}build/{*}plugins/{*}plugin",
        "{*}build/{*}pluginManagement/{*}plugins/{*}plugin",
    ) for plugin in section.findall(location)]
    if is_root and not any(plugin.findtext("{*}artifactId") ==
                           "central-publishing-maven-plugin" for plugin in plugins):
        raise Unsafe("pin Central publishing before releasing this target")
    for plugin in plugins:
        if plugin.findtext("{*}artifactId") == "maven-gpg-plugin":
            settings = plugin.findall("{*}configuration/{*}bestPractices") + plugin.findall(
                "{*}executions/{*}execution/{*}configuration/{*}bestPractices"
            )
            for setting in settings:
                if (setting.text or "").strip().lower() != "true":
                    raise Unsafe("GPG best practices must not be disabled")
        if plugin.findtext("{*}artifactId") == "central-publishing-maven-plugin":
            version = (plugin.findtext("{*}version") or "").strip()
            if not version:
                raise Unsafe("pin Central publishing before releasing this target")
            if (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (0, 11, 0)
            ):
                raise Unsafe("update Central publishing before releasing this target")
    for section in sections:
        for element in section.findall("{*}properties/{*}gpg.bestPractices"):
            if (element.text or "").strip().lower() != "true":
                raise Unsafe("GPG best practices must not be disabled")
    for element in pom.iter():
        check_text(element.text or "", maven_options=False)


def check_policy(root):
    root_pom = (root / "pom.xml").resolve()
    pending = [root_pom]
    visited = set()
    while pending:
        path = pending.pop().resolve()
        if path in visited:
            continue
        visited.add(path)
        pom = ET.parse(path).getroot()
        check_pom(pom, path == root_pom)
        for section in (pom, *pom.findall("{*}profiles/{*}profile")):
            for module in section.findall("{*}modules/{*}module"):
                pending.append(path.parent / (module.text or "").strip() / "pom.xml")
    paths = [
        root / "Makefile",
        root / ".github/workflows/release.yml",
        root / ".mvn/maven.config",
        root / ".mvn/jvm.config",
    ]
    for path in paths:
        if path.exists():
            check_text(
                path.read_text(), path.relative_to(root), maven_options=path.name == "maven.config",
                maven_arguments=path.read_text().splitlines() if path.name == "maven.config" else (),
                jvm_options=path.name == "jvm.config",
                literal_jvm_options=path.name == "jvm.config",
            )
    workflow_path = root / ".github/workflows/release.yml"
    if not workflow_path.exists():
        raise Unsafe("release workflow .github/workflows/release.yml is missing")
    workflow = workflow_path.read_text()
    if "RELEASE_GUARD" not in workflow:
        raise Unsafe("release workflow requires the RELEASE_GUARD marker")
    if not re.search(r"if:\s*(?:always\(\)|\$\{\{\s*always\(\)\s*\}\})", workflow):
        raise Unsafe("release workflow requires an always() artifact upload condition")
    makefile = root / "Makefile"
    if makefile.exists():
        commands = re.sub(r"\\\r?\n[\t ]*", " ", makefile.read_text())
        for line in commands.splitlines():
            if re.search(r"(?:\$\(MVNCMD\)|\$\{MVNCMD\}|(?<![\w/.-])mvn(?=\s|$)).*\brelease:", line) and "$(RELEASE_GUARD)" not in line:
                raise Unsafe("release target must use sanitized log capture")


def secret_forms(env):
    values = {
        v
        for k, v in env.items()
        if v and re.search(r"PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION", k, re.I)
    }
    user, password = env.get("SONATYPE_TOKEN_USERNAME"), env.get("SONATYPE_TOKEN_PASSWORD")
    if user and password:
        values.add(user + ":" + password)
    # Include multiline components, escaped newlines, URL and both Base64 alphabets.
    values |= {line for value in values for line in value.splitlines() if line}
    values |= {value.replace("\n", "\\n").replace("\r", "\\r") for value in values}
    forms = set(values)
    for value in values:
        raw = value.encode()
        forms.update(
            (
                base64.b64encode(raw).decode(),
                base64.urlsafe_b64encode(raw).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote_plus(value, safe=""),
            )
        )
        for ensure_ascii in (True, False):
            serialized = json.dumps(value, ensure_ascii=ensure_ascii)
            forms.update((serialized, serialized[1:-1]))
            forms.update((serialized.replace("/", "\\/"), serialized[1:-1].replace("/", "\\/")))
    url_forms = set()
    for value in tuple(forms):
        url_forms.update((urllib.parse.quote(value, safe=""), urllib.parse.quote(value)))
        forms.add(value.rstrip("="))
    forms.update(url_forms)
    # Diagnostics can JSON-serialize URL-encoded credentials, including slashes.
    for value in url_forms:
        serialized = json.dumps(value)
        forms.update((serialized, serialized[1:-1]))
        forms.update((serialized.replace("/", "\\/"), serialized[1:-1].replace("/", "\\/")))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    property_prefix = MAVEN_PROPERTY_PREFIX.encode()
    matches = [
        spans(
            rb"(?i)([\"'])(" + property_prefix + rb"[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*(?:\\)?)(?:\1|\Z)", 3,
        ),
        spans(
            rb"(?i)(" + property_prefix + rb"[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"(\"(?:\\[\s\S]|[^\"\\])*(?:\\)?(?:\"|\Z)"
            rb"|'(?:\\[\s\S]|[^'\\])*(?:\\)?(?:'|\Z)"
            rb"|(?:\\(?:\r\n|[\s\S])|[^\s\\])+(?:\\)?|\\)", 2,
        ),
        spans(rb"(?im)((?:proxy-)?authorization[\"']?\s*[:=]\s*)([^\r\n]*)", 2),
    ]
    for value in forms:
        # Lookahead includes overlapping occurrences; percent escapes ignore case.
        matches.append(spans(b"(?=(" + re.escape(value) + b"))", 1, re.I if b"%" in value else 0))
    output = bytearray()
    start = end = 0
    for left, right in heapq.merge(*matches):
        if left > end:
            if end > start:
                output.extend(b"[REDACTED]")
            output.extend(data[end:left])
            start = left
        end = max(end, right)
    if end > start:
        output.extend(b"[REDACTED]")
    output.extend(data[end:])
    return bytes(output)


def write_logs(clean, logs):
    """Write only sanitized content or fixed diagnostics, tolerating broken outputs."""
    output_failed = False
    for name, data, stream in zip(("stdout", "stderr"), clean, (sys.stdout, sys.stderr)):
        try:
            logs.mkdir(parents=True, exist_ok=True)
            with (logs / (name + ".log")).open("ab") as output:
                output.write(data)
        except OSError:
            output_failed = True
        try:
            stream.buffer.write(data)
            stream.flush()
        except (OSError, AttributeError, ValueError):
            output_failed = True
            # Prevent a failed shutdown flush from replacing Maven's exit status.
            try:
                with open(os.devnull, "wb") as sink:
                    os.dup2(sink.fileno(), stream.fileno())
            except (OSError, AttributeError, ValueError):
                setattr(sys, name, None)
    if output_failed:
        try:
            os.write(2, b"Sanitized release output could not be fully written; values withheld.\n")
        except OSError:
            pass


def capture(command, env, forms, logs):
    """Never spool raw output or sanitize a stream cut off before EOF."""
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    )
    buffers = [bytearray(), bytearray()]
    complete = [False, False]
    overflow = False
    logs_started = False
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            exited_at = None
            while selector.get_map():
                if process.poll() is not None:
                    if exited_at is None:
                        exited_at = time.monotonic()
                    elif time.monotonic() - exited_at >= DRAIN_GRACE:
                        break
                for key, _ in selector.select(timeout=0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        complete[key.data] = True
                        selector.unregister(key.fileobj)
                    elif not overflow:
                        if sum(map(len, buffers)) + len(chunk) > LIMIT:
                            overflow = True
                            buffers = [bytearray(), bytearray()]
                            continue
                        buffers[key.data].extend(chunk)
        status = process.wait()
        # Sanitize both streams completely before opening artifacts or emitting output.
        try:
            clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                redact(bytes(buffer), forms) if ended else INCOMPLETE_LOG
                for buffer, ended in zip(buffers, complete)
            ]
        except Exception:
            # Publishing may have finished. Withhold output without replacing its status.
            clean = [FAILED_LOG, FAILED_LOG]
        logs_started = True
        write_logs(clean, logs)
        return status if status >= 0 else 128 - status
    except BaseException as error:
        completed_status = process.poll()
        signum = signal.SIGTERM
        if isinstance(error, SystemExit) and isinstance(error.code, int):
            signum = error.code - 128
        if signum not in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            signum = signal.SIGTERM
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=DRAIN_GRACE)
        except subprocess.TimeoutExpired:
            pass
        # The Maven parent may exit before a descendant responds to cancellation.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        # Cleanup precedes fallback output, which can itself fail. Keep the status
        # observed before cleanup; cancellation still keeps its own exit status.
        if isinstance(error, Exception) and completed_status is not None:
            if not logs_started:
                write_logs([FAILED_LOG, FAILED_LOG], logs)
            return completed_status if completed_status >= 0 else 128 - completed_status
        clean = [FAILED_LOG, FAILED_LOG]
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            try:
                clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                    redact(bytes(buffer), forms) if ended else CANCELLED_LOG
                    for buffer, ended in zip(buffers, complete)
                ]
            except Exception:
                pass
        if not logs_started:
            write_logs(clean, logs)
        raise
    finally:
        process.stdout.close()
        process.stderr.close()


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def check_runtime_options():
    for name in ENV_OPTIONS:
        check_text(
            os.environ.get(name, ""),
            maven_options=name == "MAVEN_ARGS",
            maven_arguments=os.environ.get(name, "").split() if name == "MAVEN_ARGS" else (),
            jvm_options=name not in ("MAVEN_ARGS", "MAVEN_CONFIG"),
            literal_jvm_options=name in ("MAVEN_OPTS", "MAVEN_DEBUG_OPTS"),
        )
    for name in ("maven.config", "jvm.config"):
        path = Path(".mvn") / name
        if path.exists():
            check_text(
                path.read_text(),
                maven_options=name == "maven.config",
                maven_arguments=path.read_text().splitlines() if name == "maven.config" else (),
                jvm_options=name == "jvm.config",
                literal_jvm_options=name == "jvm.config",
            )


def main():
    try:
        if sys.argv[1:] == ["check"]:
            check_policy(Path.cwd())
            check_runtime_options()
            print("Release credential safety checks passed.")
            return 0
        if sys.argv[1:3] != ["run", "mvn"]:
            raise Unsafe("expected check or run mvn ...")
        check_text(" ".join(sys.argv[3:]), maven_arguments=sys.argv[3:])
        check_runtime_options()
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        return capture(
            sys.argv[2:],
            os.environ,
            secret_forms(os.environ),
            Path(os.environ.get("RELEASE_LOG_DIR", "/tmp/release-logs")),
        )
    except Unsafe as error:
        # Unsafe messages name only fixed policy checks, never their input values.
        print(f"Release safety check failed: {error}; values withheld.", file=sys.stderr)
        return 2
    except Exception:
        # Exception values can contain credentials. Do not echo them.
        print(
            "Release safety check or sanitized log capture failed; values withheld.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
