#!/usr/bin/env python3
"""Validate release configuration and discard credential-bearing Maven output."""

import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import xml.etree.ElementTree as ET

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
            if re.search(r"\$\(MVNCMD\).*\brelease:", line) and "$(RELEASE_GUARD)" not in line:
                raise Unsafe("release target must use guarded output suppression")


def run_maven(command, env):
    """Discard child output without buffering, decoding, or writing log artifacts."""
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        status = process.wait()
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    status = status if status >= 0 else 128 - status
    message = (b"Release Maven command succeeded; output withheld.\n" if status == 0
               else b"Release Maven command failed; output withheld.\n")
    try:
        os.write(1 if status == 0 else 2, message)
    except OSError:
        # A closed console must not turn a completed release into a failed release.
        pass
    return status


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
