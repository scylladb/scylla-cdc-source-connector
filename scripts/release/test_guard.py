"""Focused checks for release configuration and sanitized output."""

import base64
import html
import io
import json
import os
from pathlib import Path
import re
import shutil
import shlex
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
    updated, count = CENTRAL_VERSION.subn(rf"\g<1>{version}\g<2>", pom)
    if count != 1:
        raise AssertionError(f"expected one central-publishing version pin, found {count}")
    return updated


class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

    def test_check_rejects_unsafe_environment_before_credentials(self):
        with patch.dict(os.environ, {"MAVEN_OPTS": "-Dgpg.bestPractices=false"}, clear=True), \
             patch.object(sys, "argv", ["guard.py", "check"]), \
             patch("sys.stderr", io.StringIO()), patch("sys.stdout", io.StringIO()):
            self.assertEqual(guard.main(), 2)

    def test_maven_launcher_skips_unchecked_rc_files(self):
        with patch.dict(os.environ, {"MAVEN_SKIP_RC": ""}, clear=True), \
             patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), \
             patch.object(guard, "capture", return_value=0) as capture:
            self.assertEqual(guard.main(), 0)
            self.assertEqual(capture.call_args.args[1]["MAVEN_SKIP_RC"], "1")

    def test_run_redacts_and_appends_sanitized_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                "#!/bin/sh\n"
                "printf 'stdout:%s\\n' \"$MAVEN_GPG_PASSPHRASE\"\n"
                "printf 'stderr:%s\\n' \"$MAVEN_GPG_PASSPHRASE\" >&2\n"
            )
            binary.chmod(0o755)
            logs = root / "logs"
            env = {key: value for key, value in os.environ.items()
                   if key not in guard.ENV_OPTIONS}
            env.update(
                PATH=str(root) + os.pathsep + env.get("PATH", ""),
                RELEASE_LOG_DIR=str(logs),
                MAVEN_GPG_PASSPHRASE="dummy-secret",
            )
            command = [sys.executable, str(ROOT / "scripts/release/guard.py"),
                       "run", "mvn", "-B", "-ntp", "release:perform",
                       "-Drelease.autopublish=true"]
            for _ in range(2):
                result = subprocess.run(command, cwd=root, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "stdout:[REDACTED]\n")
                self.assertEqual(result.stderr, "stderr:[REDACTED]\n")
            self.assertEqual(
                (logs / "stdout.log").read_text(), "stdout:[REDACTED]\n" * 2
            )
            self.assertEqual(
                (logs / "stderr.log").read_text(), "stderr:[REDACTED]\n" * 2
            )

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
            "-Duser.dir=/tmp/unchecked",
            "-Dfile.encoding=UTF-16LE",
            "-Dsun.stdout.encoding=UTF-16LE",
            "-Dsun.stderr.encoding=UTF-16LE",
            "-Dcredentials=dummy-value",
            '--define=javax.net.debug="all"',
            '-Dgpg.bestPractices="false"',
            "-Dgpg.bestPractices='false'",
            "-Dorg.slf4j.simpleLogger.logFile=raw.log",
            "-Dorg.slf4j.simpleLogger.logFile",
            "-Dorg.slf4j.simpleLogger.logFile=SYSTEM.OUT",
            "-Dorg.slf4j.simpleLogger.logFile=System.out",
            "-Dorg.slf4j.simpleLogger.logFile=System.err",
            "-Dorg.slf4j.simpleLogger.logFile=System.out raw.log",
            "--debug",
            "--X",
            "--l=raw.log",
            "-debug",
            '-Dorg.slf4j.simpleLogger.log.org.apache.http="debug"',
            '-Dorg.slf4j.simpleLogger."logFile"=raw.log',
            "-Dorg.slf4j.simpleLogger.log.org.apache.http='trace'",
            '-Darguments="-X"',
            '-Darguments=-Xverify',
            "-Dgpg.passphrase=dummy",
            "-Dhttp.authorization=dummy",
            "--define=gpg.passphrase=dummy",
            "-D gpg.passphrase=dummy",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=trace",
            '-Dorg.slf4j.simpleLogger.log.org.apache.maven="debug"',
            "-Dorg.slf4j.simpleLogger.defaultLogLevel='trace'",
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

    def test_maven_configuration_redirects_are_rejected(self):
        for name in ("maven.multiModuleProjectDirectory", "maven.conf", "maven.home",
                     "user.home"):
            for option in (f"-D{name}=/tmp/unchecked",
                           f"--define={name}=/tmp/unchecked"):
                for jvm_options in (False, True):
                    with self.subTest(option=option, jvm_options=jvm_options), self.assertRaises(
                        guard.Unsafe
                    ):
                        guard.check_text(option, jvm_options=jvm_options)

    def test_direct_gpg_argument_boundaries(self):
        for value, expected in (("true false", 2), ("true ", 2), ("true", 0)):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "-Dgpg.bestPractices=" + value]
            ), patch.object(guard, "capture", return_value=0) as capture, patch(
                "sys.stderr", io.StringIO()
            ):
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
        with self.assertRaises(guard.Unsafe):
            guard.check_text('--define\n"gpg.bestPractices =false"',
                             maven_arguments=["--define", '"gpg.bestPractices =false"'])
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
            ), patch.object(guard, "capture", return_value=0) as capture, patch(
                "sys.stderr", io.StringIO()
            ):
                for value in ("-Xvalidate", '"-Xverify"'):
                    with patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True):
                        self.assertEqual(guard.main(), 2)
                        capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
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
                with patch.dict(
                    os.environ, {"MAVEN_ARGS": "-DargLine='-Xmx512m -XX:+UseG1GC'"}, clear=True
                ):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}, clear=True):
                    self.assertEqual(guard.main(), 0)
                    capture.assert_called_once()
                with patch.dict(os.environ, {"MAVEN_OPTS": "@.mvn/extra.options"}, clear=True):
                    self.assertEqual(guard.main(), 2)

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

    def test_old_release_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pom = (ROOT / "pom.xml").read_text()
            for old, text in (
                ("bestPractices", pom.replace("<gpg.bestPractices>true</gpg.bestPractices>",
                                              "<version>0.8.0</version>")),
                ("central", with_central_version(pom, "0.8.0")),
            ):
                (root / "pom.xml").write_text(text)
                with self.subTest(old=old), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
                    cwd=root, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("Release only supported targets", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_target_validation_uses_checked_out_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow = root / ".github/workflows/release.yml"
            original = workflow.read_text()
            workflow.write_text("name: Old workflow without a release guard\n")
            guard.check_policy(root)
            self.assertIn("RELEASE_GUARD", original)

    def test_alternate_maven_base_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / ".mvn").mkdir()
            (base / ".mvn/jvm.config").write_text(
                "-Dorg.slf4j.simpleLogger.logFile=raw.log\n"
            )
            for arguments in (["check"], ["run", "mvn", "validate"]):
                with self.subTest(arguments=arguments), patch.dict(
                    os.environ, {"MAVEN_BASEDIR": str(base)}, clear=True
                ), patch.object(sys, "argv", ["guard.py", *arguments]), patch.object(
                    guard, "capture"
                ) as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_release_target_requires_pinned_central_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            start = pom.index("                    <plugin>\n"
                              "                        <groupId>org.sonatype.central</groupId>")
            end = pom.index("                    </plugin>", start) + len("                    </plugin>")
            central_plugin = pom[start:end]
            self.assertIn(central_plugin, pom)
            (root / "pom.xml").write_text(pom.replace(central_plugin, ""))
            with self.assertRaisesRegex(guard.Unsafe, "pin Central publishing"):
                guard.check_policy(root)
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
                cwd=root, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("pin Central publishing", result.stderr)
            (root / "pom.xml").write_text(with_central_version(pom, ""))
            with self.assertRaisesRegex(guard.Unsafe, "update Central publishing"):
                guard.check_policy(root)

    def test_alternate_maven_settings_are_rejected(self):
        for option in ("-s", "-gs", "--s", "--gs", "--settings",
                       "-global-settings", "--global-settings"):
            with self.subTest(option=option), self.assertRaisesRegex(
                guard.Unsafe, "alternate Maven settings"
            ):
                guard.check_text(option + " unsafe.xml", maven_arguments=[option, "unsafe.xml"])
            with self.subTest(nested=option), self.assertRaises(guard.Unsafe):
                guard.check_text(
                    "-Darguments=" + option + " unsafe.xml",
                    maven_arguments=["-Darguments=" + option + " unsafe.xml"],
                )
        for option in ("--s=unsafe.xml", "--gs=unsafe.xml",
                       "-global-settings=unsafe.xml"):
            with self.subTest(option=option), self.assertRaisesRegex(
                guard.Unsafe, "alternate Maven settings"
            ):
                guard.check_text(option, maven_arguments=[option])
        guard.check_text("--show-version", maven_arguments=["--show-version"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("-s\nunsafe.xml\n", "--s=unsafe.xml\n", "--gs=unsafe.xml\n",
                           "-global-settings=unsafe.xml\n"):
                with self.subTest(config=option):
                    (root / ".mvn/maven.config").write_text(option)
                    with self.assertRaisesRegex(guard.Unsafe, "alternate Maven settings"):
                        guard.check_policy(root)
            (root / ".mvn/maven.config").unlink()
            workflow = root / ".github/workflows/release.yml"
            workflow.write_text(workflow.read_text() + "\n# curl -s is not a Maven setting\n")
            guard.check_policy(root)

    def test_combined_short_options_are_rejected(self):
        for value in ("-B-X", "-B--settings=unsafe.xml", "-B-lraw.log",
                      "-B=-f", "-B=--settings=unsafe.xml"):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, maven_arguments=[value])
            with self.subTest(nested=value), self.assertRaises(guard.Unsafe):
                guard.check_text("-Darguments=" + value,
                                 maven_arguments=["-Darguments=" + value])
        with self.assertRaises(guard.Unsafe):
            guard.check_text("-B=-f unsafe.xml", maven_arguments=["-B=-f", "unsafe.xml"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for value in ("-B-X", "-B=-f\nunsafe.xml"):
                (root / ".mvn/maven.config").write_text(value + "\n")
                with self.subTest(config=value), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_equals_combined_long_options_are_rejected(self):
        for option in ("--B=-f", "--batch-mode=-f", "--batch-mode=-X",
                       "--batch-mode=--settings"):
            with self.subTest(direct=option), self.assertRaises(guard.Unsafe):
                guard.check_text(option + " unsafe.xml", maven_arguments=[option, "unsafe.xml"])
            with self.subTest(child=option), self.assertRaises(guard.Unsafe):
                guard.check_text("-Darguments=" + option + " unsafe.xml",
                                 maven_arguments=["-Darguments=" + option + " unsafe.xml"])
        for option in ("-Dsafe=-f", "--define=safe=-f"):
            with self.subTest(property=option):
                guard.check_text(option, maven_arguments=[option])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("--B=-f", "--batch-mode=-f"):
                with self.subTest(config=option):
                    (root / ".mvn/maven.config").write_text(option + "\nunsafe.xml\n")
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_logger_level_interpolation_is_rejected(self):
        for option in ("-Dorg.slf4j.simpleLogger.defaultLogLevel=${level}",
                       "-Dorg.slf4j.simpleLogger.defaultLogLevel=INFO${level}"):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                guard.check_text(option + " -Dlevel=debug",
                                 maven_arguments=[option, "-Dlevel=debug"])
            with self.subTest(child=option), self.assertRaises(guard.Unsafe):
                guard.check_text("-Darguments=" + option,
                                 maven_arguments=["-Darguments=" + option])

    def test_whitespace_before_maven_property_equals_is_rejected(self):
        for value in ("-Dgpg.bestPractices =false", "--D gpg.bestPractices =false",
                      "-Dpassword =unknown", "-Darguments =-Xverify",
                      "-DpomFileName =unsafe.xml", "-Drelease.autopublish =maybe",
                      "--define= gpg.bestPractices=false", "--define= password=unknown",
                      "--define= arguments=-Xverify", "--define= pomFileName=unsafe.xml",
                      '--define "gpg.passphrase =dummy"',
                      '--define "org.slf4j.simpleLogger.log.org.apache.http =debug"',
                      '--define "gpg.bestPractices =false"',
                      '--define "gpg.bestPractices =true false"'):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, maven_arguments=[value])

    def test_quoted_maven_property_is_rejected_before_capture(self):
        for value in ('--define "gpg.passphrase =dummy"',
                      '--define "org.slf4j.simpleLogger.log.org.apache.http =debug"'):
            with self.subTest(value=value), patch.dict(
                os.environ, {"MAVEN_ARGS": value}, clear=True
            ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "capture"
            ) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()
        with self.assertRaises(guard.Unsafe):
            guard.check_text("--D gpg.bestPractices =false",
                             maven_arguments=["--D", "gpg.bestPractices =false"])

    def test_old_gpg_plugin_cannot_pass_target_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for version in ("1.6", "3.1.0", "${gpg.version}"):
                (root / "pom.xml").write_text(pom.replace("<version>3.2.8</version>",
                                                        f"<version>{version}</version>"))
                with self.subTest(version=version), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace("<version>3.2.8</version>",
                                                    "<version>3.2.0</version>"))
            guard.check_policy(root)

    def test_pom_nested_release_debug_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text().replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<arguments>-Xverify</arguments>",
            )
            (root / "pom.xml").write_text(pom)
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)

    def test_pom_nested_release_property_cannot_hide_debug(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text().replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<arguments>${release.extraArgs}</arguments>",
            ).replace("<properties>", "<properties><release.extraArgs>-Xverify</release.extraArgs>")
            (root / "pom.xml").write_text(pom)
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)

    def test_release_child_argument_aliases_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for element, value in (
                ("prepareVerifyArgs", "-Xverify"),
                ("prepareVerifyArgs", "${release.extraArgs}"),
                ("goals", "deploy -Xverify"),
                ("preparationGoals", "verify -Xverify"),
                ("completionGoals", "deploy -f unsafe.xml"),
            ):
                with self.subTest(element=element, value=value):
                    (root / "pom.xml").write_text(pom.replace(
                        "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                        f"<{element}>{value}</{element}>",
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            for element in ("goals", "preparationGoals", "completionGoals"):
                with self.subTest(property=element):
                    (root / "pom.xml").write_text(pom.replace(
                        "<properties>",
                        f"<properties><{element}>deploy -f unsafe.xml</{element}>",
                        1,
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<prepareVerifyArgs>-DskipTests=true</prepareVerifyArgs>",
            ))
            guard.check_policy(root)

    def test_release_pom_file_name_override_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for value in ("unsafe-pom.xml", "pom.xml", "${release.pom}"):
                with self.subTest(value=value):
                    (root / "pom.xml").write_text(pom.replace(
                        "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                        f"<pomFileName>{value}</pomFileName>",
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace(
                "<properties>", "<properties><pomFileName>unsafe.xml</pomFileName>", 1
            ))
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)
            (root / "pom.xml").write_text(pom)
            (root / ".mvn").mkdir()
            for option in ("-DpomFileName=unsafe-pom.xml", "--define=pomFileName=unsafe-pom.xml"):
                with self.subTest(option=option):
                    (root / ".mvn/maven.config").write_text(option)
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_release_child_properties_are_rejected_before_run(self):
        for option in (
            "-DpomFileName=unsafe-pom.xml",
            "-DprepareVerifyArgs=-Xverify",
            "-DprepareVerifyArgs=-f unsafe.xml",
            "-Darguments=--file=unsafe.xml",
            "-DprepareVerifyArgs=${release.extraArgs}",
            "-Darguments=-l raw.log",
            "-Dgoals=deploy -f unsafe.xml",
            "-Dgoals=deploy -Xverify",
            "-DpreparationGoals=verify -Xverify",
            "-DcompletionGoals=deploy -Xverify",
            "-DcompletionGoals=deploy -f unsafe.xml",
            "-D=arguments=-funsafe.xml",
            "--D=arguments=-funsafe.xml",
            "-D=gpg.bestPractices=false",
            "--D=gpg.bestPractices=false",
        ):
            with self.subTest(option=option), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()
        for option in ("-Darguments=-f unsafe.xml", "--define=prepareVerifyArgs=${extra}"):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                guard.check_text(option)
        for name, value in (("arguments", "-funsafe.xml"),
                            ("goals", "-Xverify"),
                            ("release.autopublish", "maybe"),
                            ("gpg.bestPractices", "false")):
            with self.subTest(split_property=name), self.assertRaises(guard.Unsafe):
                guard.check_text("--D " + name + "=" + value,
                                 maven_arguments=["--D", name + "=" + value])
        guard.check_text("-Darguments=-DskipTests=true")
        guard.check_text("-DprepareVerifyArgs=-fae")

    def test_nested_release_properties_in_environment_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "capture"
            ) as capture, patch("sys.stderr", io.StringIO()):
                for name, option in (
                    ("MAVEN_ARGS", "-Darguments=-funsafe.xml"),
                    ("MAVEN_OPTS", "-DprepareVerifyArgs='-f unsafe.xml'"),
                    ("MAVEN_CONFIG", "-Darguments=${release.extraArgs}"),
                    ("MAVEN_CONFIG", "-D=gpg.bestPractices=false"),
                ):
                    with self.subTest(name=name), patch.dict(
                        os.environ, {name: option}, clear=True
                    ):
                        self.assertEqual(guard.main(), 2)
                        capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    for name, option in (
                        ("maven.config", "-DprepareVerifyArgs=--file unsafe.xml"),
                        ("maven.config", "-D=gpg.bestPractices=false"),
                        ("maven.config", "--D\narguments=-funsafe.xml\n"),
                        ("maven.config", "--D\ngoals=-Xverify\n"),
                        ("jvm.config", "-Darguments='-f unsafe.xml'"),
                    ):
                        with self.subTest(name=name):
                            (root / ".mvn" / name).write_text(option)
                            self.assertEqual(guard.main(), 2)
                            capture.assert_not_called()
                            (root / ".mvn" / name).unlink()

    def test_release_autopublish_property_must_be_boolean(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            (root / "pom.xml").write_text(pom.replace(
                "<release.autopublish>false</release.autopublish>",
                "<release.autopublish>false -Xverify</release.autopublish>",
            ))
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)
            for option in ("-Drelease.autopublish=-Xverify", "-Drelease.autopublish=maybe",
                           "--define=release.autopublish=false -Xverify",
                           "--Drelease.autopublish=false -Xverify"):
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    guard.check_text(option, maven_arguments=[option])

    def test_alternate_maven_pom_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("--file=unsafe-pom.xml", "--file unsafe-pom.xml",
                           "-f unsafe-pom.xml", "-funsafe-pom.xml", "-ff.xml",
                           "--f=unsafe-pom.xml"):
                (root / ".mvn/maven.config").write_text(option)
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_jvm_argument_file_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            (root / ".mvn/extra.options").write_text(
                "-Dorg.slf4j.simpleLogger.logFile=raw.log"
            )
            (root / ".mvn/jvm.config").write_text("@.mvn/extra.options")
            with self.assertRaises(guard.Unsafe):
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

    def test_option_locations_hide_values(self):
        for text in ("safe\ncurl -X POST secret", "safe\n-X", "safe\n--define\npassword=secret"):
            with patch("sys.stderr", new_callable=io.StringIO) as output:
                with self.assertRaises(guard.Unsafe):
                    guard.check_text(text, "Makefile")
                self.assertIn("Makefile:2", output.getvalue())
                self.assertNotIn("secret", output.getvalue())
        for text in ("-Dmaven.repo.local=/x/repo-f", "-Dfoo=a-s", "-fae -ff -fn -llr"):
            guard.check_text(text)

    def test_termination_signals_stop_maven(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready"
                mvn = root / "mvn"
                mvn.write_text(f"#!{sys.executable}\nimport os, time\n"
                               f"open({str(ready) + '.tmp'!r}, 'w').write(str(os.getpid()))\n"
                               f"os.rename({str(ready) + '.tmp'!r}, {str(ready)!r})\n"
                               "time.sleep(30)\n")
                mvn.chmod(0o755)
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                    env=dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"]),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child = int(ready.read_text())
                    process.send_signal(signum)
                    process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 128 + signum)
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
        self.assertEqual(
            guard.redact(b"Authorization:\nordinary log line\n", []),
            b"Authorization:\nordinary log line\n",
        )

    def test_escaped_credential_forms_and_short_values(self):
        secret = 'a"b\\c&d\'e'
        forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
        for escaped in (json.dumps(secret), json.dumps(secret)[1:-1],
                        html.escape(secret, quote=True), shlex.quote(secret),
                        secret.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")):
            with self.subTest(escaped=escaped):
                self.assertEqual(guard.redact(escaped.encode(), forms), b"[REDACTED]")
        short = guard.secret_forms({"SKIP_TOKEN": "1"})
        self.assertEqual(guard.redact(b"1.2.3", short), b"[REDACTED].2.3")

    def test_xml_credential_forms(self):
        for secret, escaped in (
            ("a'b&c", "a'b&amp;c"),
            ("a'b&c", "a&apos;b&amp;c"),
            ("a'b&c", "a&#39;b&amp;c"),
            ('alpha"<&beta', 'alpha"&lt;&amp;beta'),
            ('alpha"<&beta', 'alpha&quot;&lt;&amp;beta'),
            ("a'\"&b", "a&#39;&quot;&amp;b"),
            ("alpha>&beta", "alpha>&amp;beta"),
            ("alpha\tbeta", "alpha&#9;beta"),
            ("alpha\tbeta", "alpha&#09;beta"),
            ("alpha\nbeta", "alpha&#10;beta"),
            ("alpha\rbeta", "alpha&#13;beta"),
        ):
            with self.subTest(escaped=escaped):
                self.assertEqual(
                    guard.redact(escaped.encode(), guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )

    def test_shell_backslash_credential_forms(self):
        for secret, escaped in (
            ("alpha beta", r"alpha\ beta"),
            ("alpha&beta", r"alpha\&beta"),
            ("alpha'beta", r"alpha\'beta"),
        ):
            with self.subTest(escaped=escaped):
                self.assertEqual(
                    guard.redact(escaped.encode(), guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )

    def test_quoted_property_fallback_redaction(self):
        output = b'--define "gpg.passphrase =dummy"'
        self.assertNotIn(b"dummy", guard.redact(output, []))

    def test_inherited_output_pipe_fails_closed_after_maven_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                f"#!{sys.executable}\nimport os, subprocess, sys\n"
                "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
                "print(os.environ['MAVEN_GPG_PASSPHRASE'])\n"
            )
            binary.chmod(0o755)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                       MAVEN_GPG_PASSPHRASE="dummy-secret", RELEASE_LOG_DIR=str(root / "logs"))
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                cwd=root, env=env, capture_output=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertNotIn(b"dummy-secret", result.stdout + result.stderr)
            self.assertFalse((root / "logs").exists())

    def test_quoted_credential_arguments(self):
        for quote in (b"'", b'"'):
            for option in (b"-Dpassword=", b"-D=password=", b"--D=password=",
                           b"--define=token=", b"--define secret=",
                           b"-define=token=", b"-define secret="):
                with self.subTest(quote=quote, option=option):
                    result = guard.redact(quote + option + b"two words\nlast" + quote, [])
                    self.assertNotIn(b"words", result)
                    self.assertNotIn(b"last", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_equals_prefixed_credential_arguments(self):
        for option in (b"-D=password=", b"--D=password=",
                       b"--define= password=", b"--define= password =",
                       b"-Dcredentials=", b"--define=credential="):
            with self.subTest(option=option):
                self.assertEqual(
                    guard.redact(option + b"unregistered-value", []),
                    option + b"[REDACTED]",
                )

    def test_whitespace_after_credential_equals_is_redacted(self):
        for prefix in (b"-Dpassword=", b"--define=token=", b'"-Dpassword='):
            for value in (b"unregistered-value", b'"unregistered value"'):
                with self.subTest(prefix=prefix, value=value):
                    raw = prefix + b" \t" + value
                    if prefix.startswith(b'"'):
                        raw += b'"'
                    result = guard.redact(raw, [])
                    self.assertNotIn(b"unregistered", result)
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

    def test_quoted_authorization_headers(self):
        for header in (
            b'Authorization: Bearer unknown',
            b'"Authorization": "Bearer unknown"',
            b"'Proxy-Authorization': 'Basic unknown'",
        ):
            with self.subTest(header=header):
                self.assertEqual(
                    guard.redact(header, []), header.split(b":", 1)[0] + b": [REDACTED]"
                )

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

    def test_output_failures_preserve_maven_status(self):
        for status in (0, 17):
            for failure in ("directory", "artifact", "stdout", "stderr"):
                with self.subTest(
                    status=status, failure=failure
                ), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    if failure == "directory":
                        logs.write_text("blocked")
                    elif failure == "artifact":
                        logs.mkdir()
                        (logs / "stdout.log").mkdir()
                    command = [
                        sys.executable, "-c",
                        "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n'); "
                        f"raise SystemExit({status})",
                    ]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], "
                        f"Path({str(logs)!r})))"
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

    def test_sanitization_and_memory_failures_write_no_logs(self):
        for failure in (
            patch.object(guard, "redact", side_effect=ValueError("secret")),
            patch.object(guard, "LIMIT", 1),
        ):
            with tempfile.TemporaryDirectory() as directory, failure:
                logs = Path(directory) / "logs"
                with self.assertRaises((ValueError, guard.Unsafe)):
                    guard.capture([sys.executable, "-c", 'print("secret")'], os.environ, [], logs)
                self.assertFalse(logs.exists())


if __name__ == "__main__":
    unittest.main()
