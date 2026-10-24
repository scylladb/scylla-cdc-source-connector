#!/usr/bin/env python3
"""Validate release configuration and retain only sanitized Maven output."""

import base64
import heapq
import html
import json
import selectors
import time
import urllib.parse
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
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
        normalize_property(argument.lstrip().translate(str.maketrans("", "", "\"'")))
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
        r"(?:^|[\s=\"'])(?:--?X|--?debug)\b"
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


def check_ancestor_maven_config(root):
    # Without a local .mvn directory Maven searches parents for one.
    if (root / ".mvn").is_dir():
        return
    for parent in root.parents:
        if (parent / ".mvn").is_dir():
            if any((parent / ".mvn" / name).exists()
                   for name in ("maven.config", "jvm.config")):
                raise Unsafe("ancestor Maven configuration is not allowed")
            return


def check_policy(root):
    check_ancestor_maven_config(root)
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(root / "pom.xml").getroot()
    if pom.find("m:parent", ns) is not None:
        raise Unsafe("parent Maven POMs are not verified for release")
    if pom.findtext("m:properties/m:gpg.bestPractices", namespaces=ns) != "true":
        raise Unsafe("enable GPG best practices before releasing this target")
    root_plugins = pom.findall("m:build/m:plugins/m:plugin", ns)
    release_plugins = [
        plugin
        for profile in pom.findall("m:profiles/m:profile", ns)
        if profile.findtext("m:id", namespaces=ns) == "release"
        for plugin in profile.findall("m:build/m:plugins/m:plugin", ns)
    ]
    supported_gpg = False
    supported_central = False
    release_configuration = set()
    property_values = set(pom.findall(".//m:properties/*", ns))
    for plugin in pom.findall(".//m:plugin", ns):
        if plugin.findtext("m:artifactId", namespaces=ns) == "maven-release-plugin":
            release_configuration.update(plugin.findall(".//m:configuration//*", ns))
        if plugin.findtext("m:artifactId", namespaces=ns) == "maven-gpg-plugin":
            version = plugin.findtext("m:version", namespaces=ns)
            if version is not None and (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (3, 2, 0)
            ):
                raise Unsafe("update the GPG plugin before releasing this target")
            if plugin in root_plugins and version is not None:
                supported_gpg = True
            for setting in plugin.findall(".//m:bestPractices", ns):
                if (setting.text or "").strip().lower() != "true":
                    raise Unsafe("GPG best practices must not be disabled")
        if plugin.findtext("m:artifactId", namespaces=ns) == "central-publishing-maven-plugin":
            version = plugin.findtext("m:version", namespaces=ns)
            if version is not None and (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (0, 11, 0)
            ):
                raise Unsafe("update Central publishing before releasing this target")
            if plugin in release_plugins and version is not None:
                supported_central = True
    if not supported_gpg:
        raise Unsafe("pin a GPG plugin supporting best practices before releasing this target")
    if not supported_central:
        raise Unsafe("pin Central publishing before releasing this target")
    for element in pom.iter():
        name = element.tag.rsplit("}", 1)[-1]
        if name == "pomFileName":
            raise Unsafe("alternate Maven POMs are not allowed")
        if (element.tag == "{" + ns["m"] + "}gpg.bestPractices"
                and (element.text or "").strip().lower() != "true"):
            raise Unsafe("GPG best practices must not be disabled")
        if (element.tag == "{" + ns["m"] + "}release.autopublish"
                and (element.text or "").strip() not in ("true", "false")):
            raise Unsafe("release.autopublish must be a Boolean")
        child_arguments = name == "arguments" or (
            name in ("prepareVerifyArgs", "goals", "preparationGoals", "completionGoals")
            and (element in release_configuration or element in property_values)
        )
        if child_arguments and "${" in (element.text or "").replace(
            "-Drelease.autopublish=${release.autopublish}", ""
        ):
            raise Unsafe("release arguments must not contain unverified properties")
        check_text(
            element.text or "",
            maven_options=child_arguments,
            maven_arguments=(element.text or "").split() if child_arguments else (),
        )
    paths = [
        root / "Makefile",
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
    makefile = root / "Makefile"
    if makefile.exists():
        for line in makefile.read_text().splitlines():
            if re.search(r"(?:\$[({](?:MVNCMD|mvn)[)}]|\bmvnw?\b).*\brelease:", line) and "$(RELEASE_GUARD)" not in line:
                raise Unsafe("release target must use guarded output suppression")


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
    controls = {"\a": r"\a", "\b": r"\b", "\x1b": r"\E", "\f": r"\f",
                "\n": r"\n", "\r": r"\r", "\t": r"\t", "\v": r"\v"}
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
        # POSIX quoting may split apostrophes, or escape special characters
        # inside double quotes. Retain these spellings for each derived value.
        if "'" in value:
            forms.add(shlex.quote(value))
        forms.add(re.sub(r'([\\"$`])', r'\\\1', value))
        # Ordinary shell words escape metacharacters with a backslash. Keep a
        # whole literal form so repeated backslashes cannot cause backtracking.
        shell = re.sub(r'''([ \\"'$`&;|<>(){}\[\]*?!,^])''', r'\\\1', value)
        forms.add("\\" + shell if shell.startswith(("#", "~")) else shell)
        # Bash uses ANSI-C spelling for controls, retaining printable Unicode.
        forms.add("".join(
            controls.get(char, rf"\{ord(char):03o}") if ord(char) < 32 or ord(char) == 127
            else "\\" + char if char in "\\'" else char
            for char in value
        ))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def escaped_pattern(value):
    # Match partial URL, numeric XML/HTML and shell/JSON byte escapes in place.
    # Keeping original offsets lets redaction cover the encoded bytes themselves.
    patterns = []
    spellings = {"&": b"&amp;", "<": b"&lt;", ">": b"&gt;", '"': b"&quot;", "'": b"&apos;", "/": b"\\/"}
    # Read code points from credential forms only; child output remains raw bytes.
    for token in re.findall(r"\\u[0-9a-fA-F]{4}|[\s\S]", value.decode("utf-8", "surrogateescape")):
        # JSON Unicode escapes, including surrogate pairs, allow either hex case.
        if len(token) == 6:
            patterns.append(re.escape(token[:2].encode()) + b"(?i:" + token[2:].encode() + b")")
            continue
        raw = token.encode("utf-8", "surrogateescape")
        # Bash emits fixed-width octal for control and non-ASCII bytes.
        pattern = b"".join(
            b"(?:" + re.escape(bytes([byte])) + (
                rf"|(?i:%{byte:02x}|&#x0*{byte:x};|\\x{byte:02x}|\\u00{byte:02x})"
                rf"|&#0*{byte};"
                + (rf"|\\{byte:03o}" if byte < 32 or byte >= 127 else "") + ")"
            ).encode()
            for byte in raw
        )
        if len(raw) > 1:
            # XML numeric references represent Unicode code points, not UTF-8 bytes.
            pattern = b"(?:" + pattern + (
                rf"|&#0*{ord(token)};|(?i:&#x0*{ord(token):x};))"
            ).encode()
        if token in spellings:
            pattern = b"(?:" + pattern + b"|" + re.escape(spellings[token]) + b")"
        patterns.append(pattern)
    return b"".join(patterns)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    folded = None

    def literal_spans(value):
        nonlocal folded
        if not value:
            return
        source = data
        if b"%" in value:
            # Preserve the existing case-insensitive percent-form matching.
            if folded is None:
                folded = data.lower()
            source, value = folded, value.lower()
        failure = [0] * len(value)
        for index in range(1, len(value)):
            prefix = failure[index - 1]
            while prefix and value[index] != value[prefix]:
                prefix = failure[prefix - 1]
            if value[index] == value[prefix]:
                prefix += 1
            failure[index] = prefix
        period = len(value) - failure[-1]
        del failure
        # Coalesce periodic overlaps and adjacent hits instead of every offset.
        extension = None
        start = 0
        while True:
            left = source.find(value, start)
            if left < 0:
                return
            if extension is None:
                extension = re.compile(re.escape(value) + b"(?:" + re.escape(value[-period:]) + b")*")
            right = extension.match(source, left).end()
            yield left, right
            start = right - len(value) + period

    def credential_spans():
        # Consume each quoted value once, even when its closing quote is missing.
        consumed = 0
        property_name = re.compile(rb"[\w.-]*")
        for match in re.finditer(
            rb"(?i)([\"']?)(?:--?define[=\s]+|-D\s*)",
            data,
        ):
            if match.end() <= consumed:
                continue
            # A preceding value's closing quote may touch the next option.
            outer = match[1] if match.start() >= consumed else b""
            consumed = property_name.match(data, match.end()).end()
            if (data[consumed:consumed + 1] != b"=" or not re.search(
                    rb"passphrase|password|username|token|secret|authorization",
                    data[match.end():consumed], re.I)):
                continue
            left = right = consumed + 1
            quote = outer or data[left:left + 1]
            if quote in (b'"', b"'"):
                closed = False
                if not outer:
                    right += 1
                while right < len(data):
                    if data[right] == 92:
                        right = min(right + 2, len(data))
                    elif data[right] == quote[0]:
                        right += 1
                        closed = True
                        break
                    else:
                        right += 1
                consumed = right
                yield left, right - 1 if outer and closed else right
            else:
                while right < len(data) and data[right] not in b" \t\n\r\v\f":
                    right += 1
                consumed = right
                yield left, right

    matches = [
        credential_spans(),
        spans(rb"((?:proxy-)?" + escaped_pattern(b"authorization")
              + rb"[\"']?\s*[:=]\s*)([^\r\n]*)", 2, re.I),
    ]
    has_escapes = re.search(rb"%[0-9a-f]{2}|&(?:#|amp;|lt;|gt;|quot;|apos;)|\\(?:[0-7]{3}|x|u|/)", data, re.I)
    for value in forms:
        matches.append(literal_spans(value))
        if has_escapes:
            matches.append(spans(b"(?=(" + escaped_pattern(value) + b"))", 1))
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
                # The console is already broken; keep the completed Maven status.
                pass
    if output_failed:
        try:
            os.write(2, b"Sanitized release output could not be fully written; values withheld.\n")
        except OSError:
            # Stderr is unavailable and there is no further safe reporting channel.
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

    def stop_maven(signum):
        pending = None

        def record(signum, frame):
            nonlocal pending
            if pending is None:
                pending = signum

        signals = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
        previous = {number: signal.getsignal(number) for number in signals}
        try:
            # Cancellation cannot interrupt the owned group's grace or escalation.
            for number in signals:
                signal.signal(number, record)
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                # The entire owned group has already exited.
                pass
            else:
                deadline = time.monotonic() + DRAIN_GRACE
                try:
                    process.wait(timeout=DRAIN_GRACE)
                except subprocess.TimeoutExpired:
                    # Descendants receive the same grace period before escalation.
                    pass
                time.sleep(max(0, deadline - time.monotonic()))
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    # The process group exited during the grace period.
                    pass
                process.wait()
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
        return pending

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
        pending = stop_maven(signal.SIGTERM)
        if pending is not None:
            interrupted(pending, None)
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
    except (Exception, SystemExit, KeyboardInterrupt) as error:
        completed_status = process.poll()
        signum = signal.SIGTERM
        if isinstance(error, SystemExit) and isinstance(error.code, int):
            signum = error.code - 128
        if signum not in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            signum = signal.SIGTERM
        pending = stop_maven(signum)
        clean = [FAILED_LOG, FAILED_LOG]
        cancelling = isinstance(error, (SystemExit, KeyboardInterrupt))
        incomplete = CANCELLED_LOG if cancelling or pending is not None else INCOMPLETE_LOG
        try:
            clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                redact(bytes(buffer), forms) if ended else incomplete
                for buffer, ended in zip(buffers, complete)
            ]
        except Exception:
            # Keep the safe fallback without masking the original failure.
            pass
        if not logs_started:
            write_logs(clean, logs)
        if pending is not None and not cancelling:
            interrupted(pending, None)
        if isinstance(error, Exception) and completed_status is not None:
            return completed_status if completed_status >= 0 else 128 - completed_status
        raise
    finally:
        process.stdout.close()
        process.stderr.close()



def run_maven(command, env):
    return capture(command, env, secret_forms(env),
                   Path(env.get("RELEASE_LOG_DIR", "/tmp/release-logs")))


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def main():
    try:
        # The Maven launcher reads .mvn/jvm.config from MAVEN_BASEDIR when set,
        # which can bypass the configuration checked in this checkout.
        if os.environ.get("MAVEN_BASEDIR"):
            raise Unsafe("alternate Maven base directories are not allowed")
        for name in ENV_OPTIONS:
            check_text(
                os.environ.get(name, ""),
                maven_arguments=(
                    os.environ.get(name, "").split()
                    if name in ("MAVEN_ARGS", "MAVEN_CONFIG") else ()
                ),
                jvm_options=name not in ("MAVEN_ARGS", "MAVEN_CONFIG"),
            )
        if sys.argv[1:] == ["check"]:
            try:
                check_policy(Path.cwd())
            except Unsafe as error:
                print(
                    "Selected release target fails credential safety checks. "
                    "Release only supported targets with the release guard and "
                    f"safe GPG/Central configuration: {error}; values withheld.",
                    file=sys.stderr,
                )
                return 2
            print("Release credential safety checks passed.")
            return 0
        if sys.argv[1:3] != ["run", "mvn"]:
            raise Unsafe("expected check or run mvn ...")
        check_ancestor_maven_config(Path.cwd())
        if any(arg.startswith("-X") for arg in sys.argv[3:]):
            raise Unsafe("Maven debug options are not allowed")
        check_text(" ".join(sys.argv[3:]), maven_arguments=sys.argv[3:])
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
        # The Maven launcher otherwise sources unchecked system and user mavenrc files.
        maven_env = dict(os.environ, MAVEN_SKIP_RC="1")
        return run_maven(sys.argv[2:], maven_env)
    except Exception:
        # Exception values can contain credentials. Do not echo them.
        print(
            "Release safety check or Maven execution failed; values withheld.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
