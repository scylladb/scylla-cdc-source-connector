"""Focused checks for release configuration and output suppression."""

import io
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

import guard

ROOT = Path(__file__).resolve().parents[2]
CENTRAL = "central-publishing-maven-plugin"
GPG = "maven-gpg-plugin"


def with_plugin_version(pom, artifact_id, version):
    # Locate the pin by plugin, not by its current value, so dependency bumps can't
    # turn these tests into silent no-ops.
    updated, count = re.subn(
        rf"(<artifactId>{re.escape(artifact_id)}</artifactId>\s*<version>)[^<]*(</version>)",
        lambda match: match[1] + version + match[2],
        pom,
    )
    if count != 1:
        raise AssertionError(f"expected one {artifact_id} version pin, found {count}")
    return updated


class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

    def test_ancestor_maven_configuration_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "target"
            root.mkdir()
            (parent / ".mvn").mkdir()
            (parent / ".mvn/maven.config").write_text("-X\n--log-file=/tmp/raw.log\n")
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            with self.assertRaisesRegex(guard.Unsafe, "ancestor Maven configuration"):
                guard.check_policy(root)
            with patch.object(guard, "run_maven", return_value=0) as run_maven, \
                 patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), \
                 patch("pathlib.Path.cwd", return_value=root), \
                 patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                run_maven.assert_not_called()
            (root / ".mvn").mkdir()
            guard.check_policy(root)

    def test_nearest_empty_maven_directory_stops_ancestor_search(self):
        with tempfile.TemporaryDirectory() as directory:
            top = Path(directory)
            (top / ".mvn").mkdir()
            (top / ".mvn/maven.config").write_text("-X\n")
            middle = top / "middle"
            (middle / ".mvn").mkdir(parents=True)
            root = middle / "target"
            root.mkdir()
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            guard.check_policy(root)

    def test_parent_pom_cannot_supply_unchecked_gpg_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "child"
            root.mkdir()
            parent = Path(directory) / "parent"
            parent.mkdir()
            (parent / "pom.xml").write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                '<modelVersion>4.0.0</modelVersion><groupId>example</groupId>'
                '<artifactId>parent</artifactId><version>1</version><build><plugins>'
                '<plugin><artifactId>maven-gpg-plugin</artifactId><configuration>'
                '<bestPractices>false</bestPractices></configuration></plugin>'
                '</plugins></build></project>'
            )
            pom = (ROOT / "pom.xml").read_text().replace(
                "<modelVersion>4.0.0</modelVersion>",
                "<modelVersion>4.0.0</modelVersion>"
                "<parent><groupId>example</groupId><artifactId>parent</artifactId>"
                "<version>1</version><relativePath>../parent/pom.xml</relativePath></parent>",
                1,
            )
            (root / "pom.xml").write_text(pom)
            with self.assertRaisesRegex(guard.Unsafe, "parent Maven POMs"):
                guard.check_policy(root)

    def test_check_rejects_unsafe_environment_before_credentials(self):
        with patch.dict(os.environ, {"MAVEN_OPTS": "-Dgpg.bestPractices=false"}, clear=True), \
             patch.object(sys, "argv", ["guard.py", "check"]), \
             patch("sys.stderr", io.StringIO()), patch("sys.stdout", io.StringIO()):
            self.assertEqual(guard.main(), 2)

    def test_jvm_options_file_cannot_hide_release_properties(self):
        for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            with self.subTest(name=name), patch.dict(
                os.environ, {name: "-XX:VMOptionsFile=/tmp/unchecked.options"}, clear=True
            ), patch.object(sys, "argv", ["guard.py", "check"]), \
                 patch("sys.stderr", io.StringIO()), patch("sys.stdout", io.StringIO()):
                self.assertEqual(guard.main(), 2)

    def test_maven_launcher_skips_unchecked_rc_files(self):
        with patch.dict(os.environ, {"MAVEN_SKIP_RC": ""}, clear=True), \
             patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), \
             patch.object(guard, "run_maven", return_value=0) as run_maven:
            self.assertEqual(guard.main(), 0)
            self.assertEqual(run_maven.call_args.args[1]["MAVEN_SKIP_RC"], "1")

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
                ), patch.object(guard, "run_maven") as run_maven, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()
        for name in guard.ENV_OPTIONS:
            for value in ("-Dorg.slf4j.simpleLogger.logFile=raw.log",
                          "-Dorg.slf4j.simpleLogger.logFile"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                    guard, "run_maven"
                ) as run_maven, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()

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
            ), patch.object(guard, "run_maven", return_value=0) as run_maven, patch(
                "sys.stderr", io.StringIO()
            ):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(run_maven.called, expected == 0)

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
        for nested in (
            "-Darguments=-Dsafe=ok# -funsafe.xml",
            r'-Darguments=-Dsafe=a\" -funsafe.xml -Dother=b\"',
        ):
            with self.subTest(nested=nested), self.assertRaises(guard.Unsafe):
                guard.check_text(nested, maven_arguments=[nested])

    def test_literal_quotes_do_not_make_gpg_boolean_true(self):
        for arguments in (
            ["-Dgpg.bestPractices=tr'ue"],
            ["-D", "gpg.bestPractices=tr'ue"],
            ['-D"gpg.bestPractices=tr\'ue"'],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(guard.Unsafe):
                guard.check_text(" ".join(arguments), maven_arguments=arguments)
        for argument in ('-Dgpg.bestPractices="true"',
                         "-Dgpg.bestPractices='true'"):
            with self.subTest(argument=argument), self.assertRaises(guard.Unsafe):
                guard.check_text(argument, maven_arguments=[argument])
        for argument in ("-Dgpg.bestPractices=true", '-D"gpg.bestPractices=true"'):
            guard.check_text(argument, maven_arguments=[argument])
        for argument in (
            '-Darguments=-Dgpg.bestPractices="tr\'ue"',
            '-DprepareVerifyArgs=-Dgpg.bestPractices="tr\'ue"',
        ):
            with self.subTest(argument=argument), self.assertRaises(guard.Unsafe):
                guard.check_text(argument, maven_arguments=[argument])
        for value in ("TRUE", "True"):
            argument = "-Dgpg.bestPractices=" + value
            guard.check_text(argument, maven_arguments=[argument])
        for argument in ('-D=arguments=-DskipTests=true',
                         '-D"arguments=-DskipTests=true"',
                         '-D=prepareVerifyArgs=-DskipTests=true'):
            with self.subTest(argument=argument):
                guard.check_text(argument, maven_arguments=[argument])
        for argument in ('-D"arguments=-Dsafe=bar\\ -funsafe.xml"',
                         '-Darguments=-Dsafe=bar\\ -funsafe.xml'):
            with self.subTest(argument=argument), self.assertRaises(guard.Unsafe):
                guard.check_text(argument, maven_arguments=[argument])
        safe = '-Darguments=-Dsafe="hello -fvalue" -DskipTests=true'
        guard.check_text(safe, maven_arguments=[safe])

    def test_direct_debug_options(self):
        guard.check_text("-Xmx1g -Xms256m")
        for option in ("-Xq", "-debug"):
            with self.subTest(option=option), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "run_maven") as run_maven, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                run_maven.assert_not_called()

    def test_maven_argument_sources_reject_debug_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "run_maven", return_value=0) as run_maven, patch(
                "sys.stderr", io.StringIO()
            ):
                for value in ("-Xvalidate", '"-Xverify"'):
                    with patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True):
                        self.assertEqual(guard.main(), 2)
                        run_maven.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    (root / ".mvn/maven.config").write_text("-Xvalidate")
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-Dgpg.bestPractices=true false")
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-DargLine=-Xmx512m -XX:+UseG1GC")
                    self.assertEqual(guard.main(), 0)
                    (root / ".mvn/maven.config").unlink()
                    (root / ".mvn/jvm.config").write_text("-Dgpg.bestPractices")
                    self.assertEqual(guard.main(), 2)
                    (root / ".mvn/jvm.config").write_text("-Xmx1g")
                    self.assertEqual(guard.main(), 0)
                run_maven.reset_mock()
                with patch.dict(
                    os.environ, {"MAVEN_ARGS": "-DargLine='-Xmx512m -XX:+UseG1GC'"}, clear=True
                ):
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()
                run_maven.reset_mock()
                with patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}, clear=True):
                    self.assertEqual(guard.main(), 0)
                    run_maven.assert_called_once()
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
            for case, edit, message in (
                ("bestPractices",
                 lambda: pom.replace("<gpg.bestPractices>true</gpg.bestPractices>", ""),
                 "enable GPG best practices"),
                ("central 0.8.0", lambda: with_plugin_version(pom, CENTRAL, "0.8.0"),
                 "update Central publishing"),
                ("central 0.10.0", lambda: with_plugin_version(pom, CENTRAL, "0.10.0"),
                 "update Central publishing"),
            ):
                with self.subTest(case=case):
                    (root / "pom.xml").write_text(edit())
                    with self.assertRaisesRegex(guard.Unsafe, message):
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
                    guard, "run_maven"
                ) as run_maven, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    run_maven.assert_not_called()

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
            (root / "pom.xml").write_text(with_plugin_version(pom, CENTRAL, ""))
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

    def test_quoted_maven_property_is_rejected_before_execution(self):
        for value in ('--define "gpg.passphrase =dummy"',
                      '--define "org.slf4j.simpleLogger.log.org.apache.http =debug"'):
            with self.subTest(value=value), patch.dict(
                os.environ, {"MAVEN_ARGS": value}, clear=True
            ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "run_maven"
            ) as run_maven, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                run_maven.assert_not_called()
        with self.assertRaises(guard.Unsafe):
            guard.check_text("--D gpg.bestPractices =false",
                             maven_arguments=["--D", "gpg.bestPractices =false"])

    def test_old_gpg_plugin_cannot_pass_target_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for version in ("1.6", "3.1.0", "${gpg.version}"):
                (root / "pom.xml").write_text(with_plugin_version(pom, GPG, version))
                with self.subTest(version=version), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
            (root / "pom.xml").write_text(with_plugin_version(pom, GPG, "3.2.0"))
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
            '-D"arguments=-Dsafe=bar\\ -funsafe.xml"',
            "-D=arguments=-funsafe.xml",
            "--D=arguments=-funsafe.xml",
            "-D=gpg.bestPractices=false",
            "--D=gpg.bestPractices=false",
        ):
            with self.subTest(option=option), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "run_maven") as run_maven, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                run_maven.assert_not_called()
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
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            with patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "run_maven"
            ) as run_maven, patch("sys.stderr", io.StringIO()):
                for name, option in (
                    ("MAVEN_ARGS", "-Darguments=-funsafe.xml"),
                    ("MAVEN_ARGS", '-D"arguments=-funsafe.xml"'),
                    ("MAVEN_OPTS", "-DprepareVerifyArgs='-f unsafe.xml'"),
                    ("MAVEN_CONFIG", "-Darguments=${release.extraArgs}"),
                    ("MAVEN_CONFIG", "-D=gpg.bestPractices=false"),
                ):
                    with self.subTest(name=name), patch.dict(
                        os.environ, {name: option}, clear=True
                    ):
                        self.assertEqual(guard.main(), 2)
                        run_maven.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    for name, option in (
                        ("maven.config", "-DprepareVerifyArgs=--file unsafe.xml"),
                        ("maven.config", '-D"arguments=-funsafe.xml"'),
                        ("maven.config", "-D=gpg.bestPractices=false"),
                        ("maven.config", "--D\narguments=-funsafe.xml\n"),
                        ("maven.config", "--D\ngoals=-Xverify\n"),
                        ("maven.config", "  -Darguments=-f unsafe.xml  \n"),
                        ("maven.config", "\t-DprepareVerifyArgs=--settings unsafe.xml\n"),
                        ("maven.config", "  -Dgpg.bestPractices=tr'ue'\n"),
                        ("maven.config", "  -Drelease.autopublish=fal'se'\n"),
                        ("jvm.config", "-Darguments='-f unsafe.xml'"),
                    ):
                        with self.subTest(name=name, option=option):
                            (root / ".mvn" / name).write_text(option)
                            with self.assertRaises(guard.Unsafe):
                                guard.check_policy(root)
                            self.assertEqual(guard.main(), 2)
                            run_maven.assert_not_called()
                            (root / ".mvn" / name).unlink()
                    for option in ("  -Darguments=-DskipTests=true  \n",
                                   "\t-Dgpg.bestPractices=true\n",
                                   "  -Drelease.autopublish=false\n"):
                        with self.subTest(safe_option=option):
                            (root / ".mvn/maven.config").write_text(option)
                            guard.check_policy(root)
                            run_maven.return_value = 0
                            self.assertEqual(guard.main(), 0)
                            run_maven.assert_called_once()
                            run_maven.reset_mock()

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
                           "--Drelease.autopublish=false -Xverify",
                           "-Drelease.autopublish=tr'ue",
                           "-Drelease.autopublish=fal'se"):
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    guard.check_text(option, maven_arguments=[option])
            for option in ("-Drelease.autopublish=true",
                           '-D"release.autopublish=true"'):
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
                (root / "pom.xml").write_text(with_plugin_version(pom, CENTRAL, version))
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


class MavenOutputTests(unittest.TestCase):
    def run_guard(self, script):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(f"#!{sys.executable}\n" + script)
            binary.chmod(0o755)
            env = {key: value for key, value in os.environ.items()
                   if key not in guard.ENV_OPTIONS}
            env.update(
                PATH=str(root) + os.pathsep + env.get("PATH", ""),
                MAVEN_GPG_PASSPHRASE="dummy-secret",
                RELEASE_LOG_DIR=str(root / "logs"),
            )
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"),
                 "run", "mvn", "-B", "-ntp", "release:perform"],
                cwd=root, env=env, capture_output=True, timeout=30,
            )
            self.assertEqual(sorted(path.name for path in root.iterdir()), ["logs", "mvn"])
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual((root / f"logs/{name}.log").read_bytes(), console)
                self.assertNotIn(b"dummy-secret", console)
            return result

    def test_raw_encoded_and_unknown_output_is_withheld_on_success_and_failure(self):
        payload = (b"dummy-secret\nAuthorization: Bearer unknown-token\n"
                   b"$'dummy\\x2dsecret'\nAuthoriz\\u0061tion: unknown-token\n"
                   b"dummy&#45;secret\ndummy%2dsecret\n"
                   b"%64%75%6d%6d%79%2dsecret\n\xff\x00")
        for status in (0, 17):
            with self.subTest(status=status):
                result = self.run_guard(
                    f"import os\nfor fd in (1, 2): os.write(fd, {payload!r})\n"
                    f"raise SystemExit({status})\n"
                )
                self.assertEqual(result.returncode, status)
                expected = (b"[REDACTED]\nAuthorization: [REDACTED]\n"
                            b"$'[REDACTED]'\nAuthoriz\\u0061tion: [REDACTED]\n"
                            b"[REDACTED]\n[REDACTED]\n[REDACTED]\n\xff\x00")
                # run_guard also compares each console stream to its uploaded log.
                self.assertEqual(result.stdout, expected)
                self.assertEqual(result.stderr, expected)

    def test_output_larger_than_previous_capture_limit_is_discarded(self):
        result = self.run_guard(
            "import os\n"
            "for _ in range(80):\n"
            " for fd in (1, 2): os.write(fd, b'x' * (1024 * 1024))\n"
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, guard.OVERFLOW_LOG)
        self.assertEqual(result.stderr, guard.OVERFLOW_LOG)

    def test_signal_exit_status_is_preserved(self):
        result = self.run_guard(
            "import os, signal\nos.kill(os.getpid(), signal.SIGTERM)\n"
        )
        self.assertEqual(result.returncode, 128 + signal.SIGTERM)
        self.assertEqual(result.stderr, b"")

    def test_closed_console_does_not_change_completed_maven_status(self):
        for status in (0, 17):
            with self.subTest(status=status):
                command = [sys.executable, "-c", f"raise SystemExit({status})"]
                script = ("import guard, os\n"
                          f"raise SystemExit(guard.run_maven({command!r}, os.environ))")
                read_fd, write_fd = os.pipe()
                os.close(read_fd)
                try:
                    result = subprocess.run(
                        [sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                        stdout=write_fd, stderr=write_fd, timeout=30,
                    )
                finally:
                    os.close(write_fd)
                self.assertEqual(result.returncode, status)

    def test_launcher_failure_withholds_exception_values(self):
        with patch.object(guard.subprocess, "Popen", side_effect=OSError("dummy-secret")), \
             patch.object(sys, "argv", ["guard.py", "run", "mvn"]), \
             patch.dict(os.environ, {}, clear=True), \
             patch("sys.stderr", io.StringIO()) as output:
            self.assertEqual(guard.main(), 2)
            self.assertNotIn("dummy-secret", output.getvalue())
            self.assertIn("values withheld", output.getvalue())


if __name__ == '__main__':
    unittest.main()
