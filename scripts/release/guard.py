#!/usr/bin/env python3
"""Check release defaults and capture Maven output without writing raw credentials.

Output is buffered until Maven exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Overflow and incomplete streams produce fixed diagnostics.
Child Maven processes run normally; they are not intercepted.
"""

import base64
import heapq
import html
import json
import os
from pathlib import Path
import re
import selectors
import signal
import shlex
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


CHILD_OPTIONS = r"arguments|prepareVerifyArgs|goals|preparationGoals|completionGoals"


class Unsafe(ValueError):
    pass


def check_text(text, source=None, maven_options=True, maven_arguments=(), jvm_options=False):
    # Maven accepts an equals sign after -D/--D; treat it as the ordinary
    # property form before every safety check.
    def normalize_property(value):
        value = re.sub(r"(?<![\w-])--?D=", "-D", value, flags=re.I)
        value = re.sub(r"(?i)(?<![\w-])(--?define=)\s+", r"\1", value)
        value = re.sub(
            r"(?i)(?<![\w-])((?:--?D\s*|--?define(?:=|\s+)))([\"'])([\w.-]+)\s+=",
            r"\1\2\3=", value,
        )
        return re.sub(
            r"(?i)(?<![\w-])((?:--?D\s*|--?define(?:=|\s+))[\w.-]+)\s+=",
            r"\1=", value,
        )

    def normalize_outer_property(value):
        return re.sub(
            r"(?i)^(--?D\s*|--?define(?:=|\s+))([\"'])(.*)\2$",
            lambda match: match[1] + match[3], normalize_property(value),
        )

    text = normalize_property(text)
    # Maven removes quotes before interpreting property names and values.
    original_maven_arguments = tuple(maven_arguments)
    maven_arguments = tuple(
        normalize_property(argument.translate(str.maketrans("", "", "\"'")))
        for argument in original_maven_arguments
    )
    if re.search(r"(?<![\w-])-[A-Za-z](?:=\s*)?-(?=[A-Za-z-])", text):
        raise Unsafe("combined Maven options are not allowed")
    def check_autopublish(argument):
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))release\.autopublish=(.*)",
            argument, re.I | re.S,
        )
        if setting and setting[1] not in ("true", "false", "${release.autopublish}"):
            raise Unsafe("release.autopublish must be a Boolean")

    try:
        arguments = [argument.strip().strip("\"'") for argument in maven_arguments]
        jvm_arguments = shlex.split(text) if jvm_options else []
    except ValueError as error:
        raise Unsafe("malformed release options are not allowed") from error
    if any(re.match(r"-XX:VMOptionsFile(?:=|$)", argument) for argument in jvm_arguments):
        raise Unsafe("unverified JVM option files are not allowed")
    def check_pom_option(argument):
        # Maven can split an equals-separated option into a second flag.
        # Keep property values such as -Dsafe=-f intact.
        if (re.match(r"^--?[A-Za-z][\w-]*=--?[A-Za-z]", argument)
            and not re.match(r"^--?D[\w.-]*=|^--?define=", argument, re.I)):
            raise Unsafe("combined Maven options are not allowed")
        if (argument.startswith("-f") and argument not in ("-fae", "-ff", "-fn")) or (
            argument.startswith("--f")
            and argument not in ("--fail-at-end", "--fail-fast", "--fail-never")
        ):
            raise Unsafe("alternate Maven POMs are not allowed")
        if (argument.startswith(("-s", "-gs")) and not argument.startswith("--")) or (
            argument in ("--s", "--gs", "--settings", "--global-settings", "-global-settings")
            or argument.startswith(("--s=", "--gs=", "--settings=", "--global-settings=",
                                    "-global-settings="))
        ):
            raise Unsafe("alternate Maven settings are not allowed")

    def check_release_arguments(value):
        if "${" in value:
            raise Unsafe("release arguments must not contain unverified properties")
        if "\\" in value:
            raise Unsafe("backslash escapes in release arguments are not allowed")
        try:
            lexer = shlex.shlex(value, posix=True)
            lexer.whitespace_split = True
            lexer.escape = ""  # Maven's child tokenizer keeps backslashes literal.
            lexer.commenters = ""  # Maven does not treat # as a comment here.
            child_arguments = list(lexer)
        except ValueError as error:
            raise Unsafe("malformed release options are not allowed") from error
        for child_argument in child_arguments:
            check_pom_option(child_argument)
        check_text(value, maven_arguments=child_arguments)

    for argument in arguments:
        if argument.startswith(("-X", "--X")):
            raise Unsafe("Maven debug options are not allowed")
        check_pom_option(argument)
    # Release-plugin properties are passed on to child Maven invocations.
    option_arguments = arguments + jvm_arguments
    for index, argument in enumerate(option_arguments):
        if index and option_arguments[index - 1] in ("-D", "--D", "-define", "--define"):
            argument = normalize_property(option_arguments[index - 1] + " " + argument)
        check_autopublish(argument)
        setting = re.fullmatch(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=(.*)",
            argument, re.I | re.S,
        )
        if setting and index >= len(arguments):
            check_release_arguments(setting[1])
    if not option_arguments:
        for setting in re.finditer(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=([^\r\n]*)",
            text, re.I,
        ):
            check_release_arguments(setting[1])
    if any(argument.startswith("@") for argument in arguments + jvm_arguments):
        raise Unsafe("unverified argument files are not allowed")
    if jvm_options and re.search(r"-Dgpg\.[^\s]*bestPractices[^\s]*[\"']", text, re.I):
        raise Unsafe("GPG best practices must not be disabled")
    for argument in arguments + jvm_arguments:
        check_autopublish(argument)
    for index, argument in enumerate(maven_arguments):
        previous = maven_arguments[index - 1].strip().strip("\"'") if index else ""
        raw = original_maven_arguments[index].strip()
        if len(raw) > 1 and raw[0] in "\"'" and raw[-1] == raw[0]:
            raw = raw[1:-1]
        if previous in ("-D", "--D", "-define", "--define"):
            argument = normalize_property(previous + " " + argument.strip("\"'"))
        if re.fullmatch(r"(?:--?D\s*|--?define(?:=|\s+))release\.autopublish=(?:true|false)",
                        argument, re.I | re.S):
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            raw_option = normalize_outer_property(raw_option)
            raw_option = re.sub(
                r"(?i)^(--?D\s*|--?define(?:=|\s+))([\"'])(.*)\2$",
                lambda match: match[1] + match[3], raw_option,
            )
            raw_value = raw_option.rsplit("=", 1)[-1]
            if raw_value.lower() not in ("true", "false"):
                raise Unsafe("release.autopublish must be a Boolean")
        # Quote deletion is useful for finding quoted property names, but it
        # must not turn a literal value such as tr'ue into a valid Boolean.
        if re.fullmatch(r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=true",
                        argument, re.I | re.S):
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            value = normalize_outer_property(raw_option).rsplit("=", 1)[-1]
            if value.lower() != "true":
                raise Unsafe("GPG best practices must not be disabled")
        check_autopublish(argument)
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(.*)", argument, re.I | re.S
        )
        if setting and setting[1].lower() != "true":
            raise Unsafe("GPG best practices must not be disabled")
        child_setting = re.fullmatch(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=(.*)",
            argument, re.I | re.S,
        )
        if child_setting:
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            raw_option = normalize_outer_property(raw_option)
            raw_setting = re.fullmatch(
                rf"(?:--?D\s*|--?define(?:=|\s+))[\"']?(?:{CHILD_OPTIONS})[\"']?=(.*)",
                raw_option, re.I | re.S,
            )
            if not raw_setting:
                raise Unsafe("malformed release options are not allowed")
            check_release_arguments(raw_setting[1])
    # Preserve whitespace inside quoted GPG values when checking the full boolean.
    gpg_text = re.sub(
        r"([\"'])((?:\\.|(?!\1)[\s\S])*)\1",
        lambda match: re.sub(r"\s", "\0", match[2]), text,
    ).translate(str.maketrans("", "", "\"'"))
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(?!true(?:\s|$))",
                 gpg_text, re.I) or (jvm_options and re.search(
                     r"(?:^|\s)-Dgpg\.bestPractices(?:\s|$)", gpg_text, re.I)):
        raise Unsafe("GPG best practices must not be disabled")
    # Java and Maven option parsing remove quotes from property names and values.
    text = normalize_property(text.translate(str.maketrans("", "", "\"'")))
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))pomFileName\b", text, re.I):
        raise Unsafe("alternate Maven POMs are not allowed")
    if maven_options and re.search(
        r"(?<![\w-])(?:--?s|--?gs|--settings|--?global-settings)(?=\s|=|$)", text
    ):
        raise Unsafe("alternate Maven settings are not allowed")
    # Catch accidental debug flags and credential properties, including release arguments.
    match = re.search(
        r"(?:^|[\s=\"'])(?:(?-i:--?X)|--?debug)\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))(?:arguments|prepareVerifyArgs)="
        r"(?:-X|[^\r\n]*?[=\s\"']-X)"
        r"|(?:--?D\s*|--?define(?:=|\s+))javax\.net\.debug\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))"
        r"(?:user\.dir|file\.encoding|sun\.std(?:out|err)\.encoding)\b"
        # These properties can redirect Maven to unchecked configuration.
        r"|(?:--?D\s*|--?define(?:=|\s+))"
        r"(?:maven\.multiModuleProjectDirectory|maven\.(?:conf|home)|user\.home)\b"
        r"|simpleLogger\.logFile\b"
        r"|simpleLogger[^\s<>]*=(?:debug|trace)\b"
        r"|simpleLogger[^\s<>]*=[^\s<>]*\$\{"
        r"|(?:--?D\s*|--?define(?:=|\s+))[\w.-]*"
        r"(?:passphrase|password|token|secret|credentials?|authorization)[\w.-]*=",
        text,
        re.I,
    )
    if maven_options and not match:
        match = re.search(
            r"(?:^|[\s=\"'])(?:--log-file(?:[=\s]|$)|-l(?!lr(?:[\s\"']|$))|--l)", text
        )
    if match:
        if source is not None:
            start = match.start() + len(match[0]) - len(match[0].lstrip())
            line = text.count("\n", 0, start) + 1
            print(f"Unsafe release options at {source}:{line}; values withheld.", file=sys.stderr)
        raise Unsafe("debug logging or credential arguments are not allowed")


def check_maven_capture(command, require_capture=False, make_variables=False):
    # Inspect each shell command separately: a marker in a comment or another
    # command does not establish that Maven runs under the guard.
    command = re.sub(r"\\\r?\n\s*", " ", command)
    if make_variables:
        # Only Make expands $(NAME) as a variable. $$(NAME) reaches the shell
        # as a command substitution and must not be normalized away.
        command = re.sub(r"(?<!\$)\$\((\w+)\)", r"${\1}", command)
    command = command.replace("$$", "$")
    # Allow only these fixed Git/blocker lookups, with fixed arguments and jq text.
    # Matching the whole assignment prevents nested shell commands hiding in it.
    for assignment in (
        'baseline=$(git rev-parse HEAD)',
        'blocker=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open '
        '--label release-blocker --limit 1 --json title,url --jq \'.[0] // empty\')',
        'blocker_title=$(jq -r \'.title\' <<< "$blocker")',
        'blocker_url=$(jq -r \'.url\' <<< "$blocker")',
    ):
        command = re.sub(r'(?m)^[ \t]*' + re.escape(assignment).replace(r'\ ', r'[ \t]+') + r'[ \t]*$',
                         'blocker=""', command)
    command = re.sub(
        r'(?m)^[ \t]*if ! user=\$\(gh api user --jq \.login\); then[ \t]*$',
        'if ! user=; then', command,
    )
    # These fixed, quoted array assignments belong to the current prepare recipe.
    # No target-supplied shell fragment is evaluated while checking them.
    command = re.sub(r'(?m)^[ \t]*NEXT_RELEASE=\(\)[ \t]*$', 'NEXT_RELEASE=', command)
    command = re.sub(
        r'(?m)^[ \t]*NEXT_RELEASE=\("-DreleaseVersion=\$\{RELEASE_VERSION\}"\)[ \t]*$',
        'NEXT_RELEASE=', command,
    )
    # shlex consumes comment newlines, merging separate shell commands. Strip
    # comments ourselves, retaining their newlines and quoted/embedded hashes.
    # Reject unquoted parentheses conservatively: subshells and Bash process
    # substitutions execute commands that this token-based checker cannot parse.
    characters = []
    quote = None
    grouping = False
    word_start = True
    index = 0
    while index < len(command):
        character = command[index]
        if character == "\\" and quote != "'":
            characters.append(command[index:index + 2])
            index += 2
            word_start = False
            continue
        if quote:
            if character == quote:
                quote = None
        elif character in "\"'":
            quote = character
        elif character == "#" and word_start:
            newline = command.find("\n", index)
            index = len(command) if newline < 0 else newline
            continue
        elif character in "()":
            grouping = True
        characters.append(character)
        word_start = quote is None and character in " \t\r\n;&|<>"
        index += 1
    command = "".join(characters)
    # Check the original script before shlex removes quotes and escapes. Even
    # guarded arguments are expanded by the shell before capture starts.
    if "$(" in command or "`" in command:
        raise Unsafe("unresolved release executables or command substitutions are unsupported")
    if grouping:
        raise Unsafe("shell grouping and process substitutions are unsupported")
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|\n")
        lexer.whitespace = " \t\r"
        lexer.whitespace_split = True
        lexer.commenters = ""
        tokens = list(lexer)
    except ValueError as error:
        raise Unsafe("malformed release commands are not allowed") from error
    if not require_capture:
        # These driver test/build parameters never select a Maven goal. Unknown
        # variable goals remain rejected; release recipes allow none of these.
        for index, token in enumerate(tokens):
            for name in ("MVN_COVERAGE", "MAVEN_EXTRA_ARGS", "SCYLLA_VERSION_RESOLVED",
                         "CASSANDRA_VERSION_RESOLVED"):
                token = token.replace("${" + name + "}", "").replace("$" + name, "")
            tokens[index] = token
    parts = [[]]
    for token in tokens:
        if token and all(character in ";&|\n" for character in token):
            parts.append([])
        else:
            parts[-1].append(token)
    for part in parts:
        while part and (re.match(r"^[A-Za-z_][A-Za-z_0-9]*=", part[0])
                        or part[0] in ("!", "if", "then", "elif", "else", "do",
                                       "exec", "command", "env")):
            if part[0] == "env":
                part = part[1:]
                if part and part[0] == "--":
                    part = part[1:]
                elif part and part[0].startswith("-"):
                    raise Unsafe("unsupported env launcher options in release commands")
                continue
            if part[0] in ("command", "exec"):
                part = part[1:]
                if part and part[0] == "--":
                    part = part[1:]
                elif part and part[0].startswith("-"):
                    raise Unsafe("unsupported launcher options in release commands")
                continue
            part = part[1:]
        # Resolve neither substitutions nor variable/glob executable names.
        if (part and part[0] not in ("$MVNCMD", "${MVNCMD}", "$mvn", "${mvn}", "$MAKE", "${MAKE}", "[", "[[")
                and re.search(r"[$`*?\[]", part[0])):
            raise Unsafe("unresolved release executables require sanitized log capture")
        # Quoted shell scripts are opaque; inspect the executable, not Maven args.
        if part and Path(part[0]).name in (
            "sh", "bash", "dash", "ash", "ksh", "zsh", "fish", "eval"
        ):
            raise Unsafe("nested shell launchers are not allowed in release commands")
        if not part:
            continue
        executable = Path(part[0]).name
        if part[0] == "." or executable in (
            "source", "builtin", "xargs", "timeout", "nice", "nohup", "sudo", "doas",
            "time", "busybox", "parallel", "watch", "setsid", "stdbuf", "chroot", "flock", "find"
        ):
            raise Unsafe("unsupported command launchers in release commands")
        if part[:3] in (["python3", "$RELEASE_GUARD", "run"],
                        ["python3", "${RELEASE_GUARD}", "run"]) and len(part) > 3:
            continue
        if executable in ("echo", "printf"):
            continue
        is_maven = executable in ("mvn", "mvnw") or part[0] in ("$MVNCMD", "${MVNCMD}", "$mvn", "${mvn}")
        # An unknown wrapper can also dispatch Maven. Do not infer safe shell
        # semantics from an arbitrary executable's arguments.
        if any(Path(token).name in ("mvn", "mvnw") or token in ("$MVNCMD", "${MVNCMD}", "$mvn", "${mvn}")
               for token in part[1:]):
            raise Unsafe("unsupported command launchers require sanitized log capture")
        # Static non-release Maven recipes remain usable without release secrets.
        if (is_maven and (require_capture or any("$" in argument for argument in part[1:]))) or any(
            "release:" in argument for argument in part
        ):
            raise Unsafe("release target must use sanitized log capture")


def workflow_commands(workflow):
    """Read inline and indented run scripts without executing workflow content."""
    lines = iter(workflow.splitlines())
    pending = None
    while True:
        line = pending if pending is not None else next(lines, None)
        pending = None
        if line is None:
            return
        match = re.match(r"^(\s*)(?:-\s+)?(?:run|\"run\"|'run')\s*:\s*(.*)$", line)
        if not match:
            continue
        value = match[2]
        if value.startswith(("*", "&", "!")):
            raise Unsafe("aliased or tagged release scripts are unsupported")
        if not value.startswith(("|", ">")):
            if value.startswith(('"', "'")):
                # Find the closing YAML quote before accepting a trailing comment.
                quoted = re.match(
                    r'''^("(?:\\.|[^"\\])*"|'(?:''|[^'])*')(?:[ \t]+#.*)?[ \t]*$''',
                    value,
                )
                if not quoted:
                    raise Unsafe("unsupported quoted release script")
                value = quoted[1]
            if value.startswith('"'):
                try:
                    value = json.loads(value)
                except ValueError as error:
                    raise Unsafe("unsupported quoted release script") from error
            elif value.startswith("'"):
                if not value.endswith("'"):
                    raise Unsafe("unsupported quoted release script")
                value = value[1:-1].replace("''", "'")
            yield value
            continue
        script = []
        for following in lines:
            if following.strip() and len(following) - len(following.lstrip()) <= len(match[1]):
                pending = following
                break
            script.append(following)
        yield (" " if value.startswith(">") else "\n").join(script)


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
    # Pax provision parameters configure Felix, not Maven's logger/CLI.
    provision_params = set(pom.findall(
        ".//m:plugin[m:artifactId='maven-pax-plugin']/m:configuration/m:provision/m:param", ns))
    for element in pom.iter():
        if (element.tag == "{" + ns["m"] + "}gpg.bestPractices"
                and (element.text or "").strip().lower() != "true"):
            raise Unsafe("GPG best practices must not be disabled")
        check_text((element.text or "").replace("${release.autopublish}", "false"),
                   maven_options=element not in provision_params)
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
                maven_arguments=path.read_text().splitlines() if path.name == "maven.config" else (),
                jvm_options=path.name == "jvm.config",
            )
    workflow_path = root / ".github/workflows/release.yml"
    if not workflow_path.exists():
        raise Unsafe("release workflow .github/workflows/release.yml is missing")
    workflow = workflow_path.read_text()
    if "RELEASE_GUARD" not in workflow:
        raise Unsafe("release workflow requires the RELEASE_GUARD marker")
    for command in workflow_commands(workflow):
        check_maven_capture(command, require_capture=True)
    if not re.search(r"if:\s*(?:always\(\)|\$\{\{\s*always\(\)\s*\}\})", workflow):
        raise Unsafe("release workflow requires an always() artifact upload condition")
    makefile = root / "Makefile"
    if makefile.exists():
        logical = re.sub(r"\\\r?\n[ \t]*", " ", makefile.read_text())
        if re.search(r"(?m)^[ \t]*(?:(?:override|export|private)\s+)*\.RECIPEPREFIX\b", logical):
            raise Unsafe("custom .RECIPEPREFIX settings are unsupported")
        # Existing driver build macros are unrelated to release execution.
        checked_directives = logical
        for allowed in ["COVERAGE_LOWER := $(shell printf '%s' '$(COVERAGE)' | tr '[:upper:]' '[:lower:]')", "$(error COVERAGE must be true or false, got '$(COVERAGE)')", 'CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})', 'define LOAD_CACHED_VERSION', 'define REQUIRE_FULLY_QUALIFIED_VERSION']:
            checked_directives = checked_directives.replace(allowed + "\n", "\n")
        if re.search(r"(?m)^\s*(?:-?include|sinclude|define)\s|\$[({]\s*(?:shell|eval|file|info|warning|error)\b", checked_directives):
            raise Unsafe("executable Make directives are unsupported for release targets")
        release_target = False
        for line in logical.splitlines():
            if line.startswith("\t"):
                if release_target or re.search(r"(?:\bmvnw?\b|MVNCMD|\$[({]mvn[)}]|release:)", line):
                    check_maven_capture(line.lstrip().lstrip("@+-"), require_capture=release_target, make_variables=True)
            elif re.match(r"^[^#:=\s][^:=]*:(?!=)", line):
                release_target = any(
                    re.fullmatch(r"[._]?release(?:-[\w-]+)?", target)
                    for target in line.split(":", 1)[0].split()
                )
                prerequisites, separator, recipe = line.split(":", 1)[1].partition(";")
                if release_target and prerequisites.strip() not in ("", "release-check-user"):
                    raise Unsafe("release target prerequisites are unsupported")
                if separator:
                    check_maven_capture(recipe.lstrip().lstrip("@+-"),
                                        require_capture=release_target, make_variables=True)


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
    # JSON/logging serializers also escape quotes, backslashes and Unicode secrets.
    values |= {json.dumps(value, ensure_ascii=ascii_only)[1:-1]
               for value in tuple(values) for ascii_only in (True, False)}
    # Maven XML serializers escape punctuation and may use named or numeric quotes.
    for value in tuple(values):
        escaped = html.escape(value, quote=False)
        for apostrophe in ("'", "&apos;", "&#39;", "&#x27;"):
            for quote in ('"', "&quot;", "&#34;", "&#x22;"):
                values.add(escaped.replace("'", apostrophe).replace('"', quote))
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
            rb"(?i)([\"'])((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*)\1", 3,
        ),
        spans(
            rb"(?i)((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
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
            # Publishing has already exited. Withhold output without changing its
            # status or skipping the workflow's successful SCM push.
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
        if isinstance(error, Exception) and completed_status is not None:
            return completed_status if completed_status >= 0 else 128 - completed_status
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


def main():
    try:
        if sys.argv[1:] == ["check"]:
            check_runtime_options()
            check_policy(Path.cwd())
            print("Release credential safety checks passed.")
            return 0
        if len(sys.argv) < 3 or sys.argv[1] != "run" or not sys.argv[2]:
            raise Unsafe("expected check or run Maven executable ...")
        if any(arg.startswith("-X") for arg in sys.argv[3:]):
            raise Unsafe("Maven debug options are not allowed")
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
