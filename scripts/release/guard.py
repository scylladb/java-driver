#!/usr/bin/env python3
"""Check release defaults and capture Maven output without writing raw credentials.

Output is buffered until Maven exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Child Maven processes run normally; they are not intercepted.
"""

import base64
import heapq
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import urllib.parse
import xml.etree.ElementTree as ET

LIMIT = 64 * 1024 * 1024
ENV_OPTIONS = (
    "MAVEN_ARGS",
    "MAVEN_OPTS",
    "JAVA_TOOL_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS",
    "MAVEN_DEBUG_OPTS",
    "MAVEN_CONFIG",
)


class Unsafe(ValueError):
    pass


def check_text(text, source=None, maven_options=True, maven_arguments=(), jvm_options=False):
    if any(arg.strip().startswith("-X") for arg in maven_arguments):
        raise Unsafe("Maven debug options are not allowed")
    for index, argument in enumerate(maven_arguments):
        if index and maven_arguments[index - 1] in ("-D", "-define", "--define"):
            argument = maven_arguments[index - 1] + " " + argument
        setting = re.fullmatch(
            r"(?:-D\s*|--?define(?:=|\s+))gpg\.bestPractices=(.*)", argument, re.I | re.S
        )
        if setting and setting[1].lower() != "true":
            raise Unsafe("GPG best practices must not be disabled")
    # Preserve whitespace inside quoted GPG values when checking the full boolean.
    gpg_text = re.sub(
        r"([\"'])((?:\\.|(?!\1)[\s\S])*)\1",
        lambda match: re.sub(r"\s", "\0", match[2]), text,
    ).translate(str.maketrans("", "", "\"'"))
    if re.search(r"(?:-D\s*|--?define(?:=|\s+))gpg\.bestPractices=(?!true(?:\s|$))",
                 gpg_text, re.I) or (jvm_options and re.search(
                     r"(?:^|\s)-Dgpg\.bestPractices(?:\s|$)", gpg_text, re.I)):
        raise Unsafe("GPG best practices must not be disabled")
    # Java and Maven option parsing remove quotes from property names and values.
    text = text.translate(str.maketrans("", "", "\"'"))
    # Catch accidental debug flags and credential properties, including release arguments.
    match = re.search(
        r"(?:^|[\s=\"'])(?:-X|--?debug)\b"
        r"|(?:-D\s*|--?define(?:=|\s+))javax\.net\.debug\b"
        r"|simpleLogger\.logFile="
        r"|simpleLogger[^\s<>]*=(?:debug|trace)\b"
        r"|(?:-D\s*|--?define(?:=|\s+))[\w.-]*"
        r"(?:passphrase|password|token|secret|authorization)[\w.-]*=",
        text,
        re.I,
    )
    if maven_options and not match:
        match = re.search(
            r"(?:^|[\s=\"'])(?:--log-file(?:[=\s]|$)|-l(?!lr(?:[\s\"']|$)))", text
        )
    if match:
        if source is not None:
            start = match.start() + len(match[0]) - len(match[0].lstrip())
            line = text.count("\n", 0, start) + 1
            print(f"Unsafe release options at {source}:{line}; values withheld.", file=sys.stderr)
        raise Unsafe("debug logging or credential arguments are not allowed")


def check_policy(root):
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(root / "pom.xml").getroot()
    if pom.findtext("m:properties/m:gpg.bestPractices", namespaces=ns) != "true":
        raise Unsafe("enable GPG best practices before releasing this target")
    for plugin in pom.findall(".//m:plugin", ns):
        if plugin.findtext("m:artifactId", namespaces=ns) == "maven-gpg-plugin":
            for setting in plugin.findall(".//m:bestPractices", ns):
                if (setting.text or "").strip().lower() != "true":
                    raise Unsafe("GPG best practices must not be disabled")
        if plugin.findtext("m:artifactId", namespaces=ns) == "central-publishing-maven-plugin":
            version = plugin.findtext("m:version", namespaces=ns)
            if version and (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (0, 11, 0)
            ):
                raise Unsafe("update Central publishing before releasing this target")
    for element in pom.iter():
        if (element.tag == "{" + ns["m"] + "}gpg.bestPractices"
                and (element.text or "").strip().lower() != "true"):
            raise Unsafe("GPG best practices must not be disabled")
        check_text(element.text or "")
    paths = [
        root / "Makefile",
        root / ".github/workflows/release.yml",
        root / ".mvn/maven.config",
        root / ".mvn/jvm.config",
    ]
    for path in paths:
        if path.exists():
            check_text(
                path.read_text(), path.relative_to(root), maven_options=path.parent.name == ".mvn",
                maven_arguments=(
                    path.read_text().splitlines() if path.name == "maven.config" else ()
                ),
                jvm_options=path.name == "jvm.config",
            )
    workflow = (root / ".github/workflows/release.yml").read_text()
    if "RELEASE_GUARD" not in workflow or "if: always()" not in workflow:
        raise Unsafe(
            "release workflow sanity check requires RELEASE_GUARD and if: always() markers"
        )
    makefile = root / "Makefile"
    if makefile.exists():
        for line in makefile.read_text().splitlines():
            if re.search(r"\$\(MVNCMD\).*\brelease:", line) and "$(RELEASE_GUARD)" not in line:
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
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    matches = [
        spans(
            rb"(?i)([\"'])((?:-D\s*|--?define[=\s]+)[\w.-]*"
            rb"(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*)\1", 3,
        ),
        spans(
            rb"(?i)((?:-D\s*|--?define[=\s]+)[\w.-]*"
            rb"(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"(\"(?:\\[\s\S]|[^\"\\])*\"|'(?:\\[\s\S]|[^'\\])*'|[^\s]+)", 2,
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


def capture(command, env, forms, logs):
    """Never spool raw output. Limit memory and terminate the process group on failure."""
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    )
    buffers = [bytearray(), bytearray()]
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            while selector.get_map():
                for key, _ in selector.select():
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        buffers[key.data].extend(chunk)
                        if sum(map(len, buffers)) > LIMIT:
                            raise Unsafe("release output exceeds memory limit")
        status = process.wait()
        # Sanitize both streams completely before opening artifacts or emitting output.
        clean = [redact(bytes(buffer), forms) for buffer in buffers]
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
            except OSError:
                output_failed = True
                # Prevent a failed shutdown flush from replacing Maven's exit status.
                try:
                    with open(os.devnull, "wb") as sink:
                        os.dup2(sink.fileno(), stream.fileno())
                except OSError:
                    pass
        if output_failed:
            try:
                os.write(
                    2, b"Sanitized release output could not be fully written; values withheld.\n"
                )
            except OSError:
                pass
        return status if status >= 0 else 128 - status
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    finally:
        process.stdout.close()
        process.stderr.close()


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def main():
    try:
        if sys.argv[1:] == ["check"]:
            check_policy(Path.cwd())
            print("Release credential safety checks passed.")
            return 0
        if sys.argv[1:3] != ["run", "mvn"]:
            raise Unsafe("expected check or run mvn ...")
        if any(arg.startswith("-X") for arg in sys.argv[3:]):
            raise Unsafe("Maven debug options are not allowed")
        check_text(" ".join(sys.argv[3:]), maven_arguments=sys.argv[3:])
        for name in ENV_OPTIONS:
            check_text(
                os.environ.get(name, ""),
                maven_arguments=os.environ.get(name, "").split() if name == "MAVEN_ARGS" else (),
                jvm_options=name not in ("MAVEN_ARGS", "MAVEN_CONFIG"),
            )
        for name in ("maven.config", "jvm.config"):
            path = Path(".mvn") / name
            if path.exists():
                check_text(
                    path.read_text(),
                    maven_arguments=path.read_text().splitlines() if name == "maven.config" else (),
                    jvm_options=name == "jvm.config",
                )
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        return capture(
            sys.argv[2:],
            os.environ,
            secret_forms(os.environ),
            Path(os.environ.get("RELEASE_LOG_DIR", "/tmp/release-logs")),
        )
    except Exception:
        # Exception values can contain credentials. Do not echo them.
        print(
            "Release safety check or sanitized log capture failed; values withheld.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
