"""Exercise actual release recipes with a fake Maven executable and synthetic secrets."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import guard

ROOT = Path(__file__).resolve().parents[2]


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
            raise AssertionError("aliased or tagged release scripts are unsupported")
        if not value.startswith(("|", ">")):
            if value.startswith(('"', "'")):
                # Find the closing YAML quote before accepting a trailing comment.
                quoted = re.match(
                    r'''^("(?:\\.|[^"\\])*"|'(?:''|[^'])*')(?:[ \t]+#.*)?[ \t]*$''',
                    value,
                )
                if not quoted:
                    raise AssertionError("unsupported quoted release script")
                value = quoted[1]
            if value.startswith('"'):
                try:
                    value = json.loads(value)
                except ValueError as error:
                    raise AssertionError("unsupported quoted release script") from error
            elif value.startswith("'"):
                if not value.endswith("'"):
                    raise AssertionError("unsupported quoted release script")
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
            self.assertIn("release:perform", calls[1]["args"])
            self.assertIn("release:perform", calls[2]["args"])
            self.assertIn("-Drelease.autopublish=true", calls[1]["args"])
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

    def test_master_release_rejects_other_dispatch_refs_before_preserving_guard(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        command = next(script for script in workflow_commands(workflow)
                       if "preserve-tools.sh" in script)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for target, ref, accepted in (
                ("master", "refs/heads/master", True),
                ("master", "refs/heads/feature", False),
                ("master", "refs/tags/master", False),
                ("v1.0.0", "refs/heads/master", True),
            ):
                with self.subTest(target=target, ref=ref):
                    runner = root / str(len(list(root.iterdir())))
                    runner.mkdir()
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c", command], cwd=ROOT,
                        env={**os.environ, "RELEASE_TARGET_TAG": target, "GITHUB_REF": ref,
                             "RUNNER_TEMP": str(runner), "GITHUB_ENV": str(runner / "env")},
                        capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)
                    self.assertEqual((runner / "release-safety").exists(), accepted)
                    if accepted:
                        runtime_files = {
                            "guard.py", "signing.py", "assets.py", "publisher.asc",
                            "check-blockers.sh", "setup-maven.sh", "Makefile",
                        }
                        self.assertEqual(
                            {path.name for path in (runner / "release-safety").iterdir()},
                            runtime_files,
                        )
                        for filename in runtime_files - {"Makefile"}:
                            self.assertEqual(
                                (runner / "release-safety" / filename).read_bytes(),
                                (ROOT / "scripts/release" / filename).read_bytes(),
                            )

    def test_blocker_checks_use_launch_tools_and_fail_closed(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        commands = [script for script in workflow_commands(workflow) if "check-blockers.sh" in script]
        self.assertEqual(len(commands), 3)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            preserved = root / "release-safety"
            preserved.mkdir()
            shutil.copy2(ROOT / "scripts/release/check-blockers.sh", preserved / "check-blockers.sh")
            target = root / "historical-target"
            (target / "scripts/release").mkdir(parents=True)
            (target / "scripts/release/check-blockers.sh").write_text("exit 99\n")
            gh = root / "gh"
            gh.write_text(
                f"#!{sys.executable}\n"
                "import os, sys\n"
                "print(os.environ['BLOCKER_RESULT'])\n"
                "sys.exit(int(os.environ['BLOCKER_STATUS']))\n"
            )
            gh.chmod(0o700)
            for command in commands:
                for output, status, expected in (
                    ("", 0, 0),
                    (json.dumps({"title": "Release blocked", "url": "https://example.invalid/blocker"}), 0, 1),
                    ("", 7, 7),
                ):
                    with self.subTest(command=command, status=status, output=output):
                        result = subprocess.run(
                            ["bash", "-eo", "pipefail", "-c", command],
                            cwd=ROOT if "bash scripts/" in command else target,
                            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                 "RUNNER_TEMP": str(root), "GITHUB_REPOSITORY": "fixture/connector",
                                 "BLOCKER_RESULT": output, "BLOCKER_STATUS": str(status)},
                            capture_output=True, text=True, timeout=10,
                        )
                        self.assertEqual(result.returncode, expected, result.stderr)

    def test_workflow_uses_preserved_recipes_in_target_checkout(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        commands = [script.strip() for script in workflow_commands(workflow)
                    if ('make -f "$RUNNER_TEMP/release-safety/Makefile"' in script and 'github-release' not in script)
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
            shutil.copy2(ROOT / "scripts/release/assets.py", preserved / "assets.py")
            shutil.copy2(env.pop("RELEASE_SIGNING_HELPER"), preserved / "signing.py")
            shutil.copy2(ROOT / "scripts/release/publisher.asc", preserved / "publisher.asc")
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
            env["RELEASE_VERSION"] = "2.0.8"
            result = self.run_make("release-prepare", env)
            self.assertEqual(result.returncode, 0, result.stderr)
            call = json.loads((root / "trace.jsonl").read_text())
            self.assertIn("-DreleaseVersion=2.0.8", call["args"])

    def test_placeholder_tokens_stop_upload_commands_before_maven(self):
        for target in ("release", "release-dry-run", "_release", "_release-dry-run"):
            for name in ("SONATYPE_TOKEN_USERNAME", "SONATYPE_TOKEN_PASSWORD"):
                for value in ("", "PLACEHOLDER_NOT_CONFIGURED"):
                    with self.subTest(target=target, name=name, value=value), \
                         tempfile.TemporaryDirectory() as directory:
                        root, env = self.fake_environment(directory)
                        env[name] = value
                        result = self.run_make(target, env)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("Configure real Sonatype tokens", result.stderr)
                        self.assertFalse((root / "trace.jsonl").exists())

    def test_invalid_target_rejected_before_source_selection(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        resolve = next(script for script in workflow_commands(workflow)
                       if "sed 's/^/sha=/'" in script)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            for tag in ("feature", "v1.0", "--help", "v1.0.0; echo unsafe"):
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", resolve], cwd=ROOT,
                    env={**os.environ, "RELEASE_TARGET_TAG": tag, "GITHUB_OUTPUT": str(output)},
                    capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Unsupported target tag", result.stdout)
                self.assertFalse(output.exists())

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

    def test_guard_covers_lowercase_release_commands(self):
        for command in ("${mvn}", "$(mvn)", "$(MVNCMD)"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copy(ROOT / "pom.xml", root / "pom.xml")
                shutil.copytree(ROOT / ".github", root / ".github")
                (root / "Makefile").write_text(f"\t{command} release:perform\n")
                with self.assertRaisesRegex(guard.Unsafe, "guarded output suppression"):
                    guard.check_policy(root)

    def test_json_escaped_credentials_are_removed_before_logs(self):
        secret = 'synthetic "quote" \\ slash\nline é'
        forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
        for ascii_only in (True, False):
            serialized = json.dumps(secret, ensure_ascii=ascii_only)
            with self.subTest(ascii_only=ascii_only):
                cleaned = guard.redact(serialized.encode(), forms)
                self.assertEqual(cleaned, b'"[REDACTED]"')

    def test_workflow_preserves_helper_and_checks_target_before_credentials(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(workflow.index("Preserve release guard"), workflow.index("Checkout release target"))
        self.assertLess(workflow.index("Checkout release target"), workflow.index("Check release credential safety"))
        self.assertLess(workflow.index("Check release credential safety"), workflow.index("RELEASE_GPG_PRIVATE_KEY:"))
        self.assertNotIn("gpg-private-key:", workflow)
        self.assertLess(workflow.index("Push changes to SCM"), workflow.index("Upload sanitized stdout"))
        self.assertIn('git checkout --detach "refs/tags/$RELEASE_TARGET_TAG~1" --', workflow)
        self.assertIn("default: master", workflow)
        self.assertEqual(workflow.count("if: always()"), 4)
        self.assertRegex(workflow, r"(?m)^    environment: release$")

    def test_asset_signing_failure_leaves_prepared_objects_on_remote(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        names = ("Perform release", "Push changes to SCM", "Sign and verify GitHub assets")
        steps = [step for step in re.split(r"^      - name: ", workflow, flags=re.M)[1:]
                 if step.splitlines()[0] in names]
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            preserved = root / "release-safety"
            preserved.mkdir()
            for source in (ROOT / "Makefile", ROOT / "scripts/release/guard.py"):
                shutil.copy2(source, preserved / source.name)
            # Central perform uses fake Maven; only Git pushes to a local bare repo.
            (preserved / "assets.py").write_text(
                "import sys\nraise SystemExit(7 if sys.argv[1] == 'sign' else 0)\n")
            shutil.copy2(ROOT / "pom.xml", root / "pom.xml")
            shutil.copytree(ROOT / ".github", root / ".github")
            env.update(RUNNER_TEMP=str(root), GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=os.devnull)
            remote = root / "remote.git"
            for args in (["init", "--quiet", "--initial-branch=master"],
                         ["init", "--quiet", "--bare", str(remote)],
                         ["remote", "add", "origin", str(remote)],
                         ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                          "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "Prepared"],
                         ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                          "-c", "tag.gpgSign=false", "tag", "-a", "v1.0.0", "-m", "Prepared"]):
                subprocess.run(["git", *args], cwd=root, env=env, check=True, capture_output=True)
            results = []
            for step in steps:
                command = next(workflow_commands("      - name: " + step))
                result = subprocess.run(["bash", "-eo", "pipefail", "-c", command],
                                        cwd=root, env=env, capture_output=True, timeout=10)
                results.append(result.returncode)
                if result.returncode:
                    break
            self.assertEqual(results, [0, 0, 7], result.stderr)
            for ref in ("refs/heads/master", "refs/tags/v1.0.0"):
                local = subprocess.check_output(["git", "rev-parse", ref], cwd=root, env=env)
                pushed = subprocess.check_output(["git", "--git-dir", str(remote), "rev-parse", ref], env=env)
                self.assertEqual(pushed, local)

    def test_validation_precedes_approval_on_a_separate_runner_without_release_secrets(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        validation, release = workflow.split("\n  release:\n", 1)
        release_header = release.split("    steps:", 1)[0]
        self.assertIn("\n  validate:\n", validation)
        self.assertIn("    runs-on: ubuntu-latest\n", validation)
        self.assertNotIn("environment:", validation)
        self.assertNotIn("secrets.", validation)
        self.assertIn("      contents: read\n", validation)
        self.assertNotIn("contents: write", validation)
        self.assertIn("    needs: validate\n", release_header)
        self.assertIn("    runs-on: ubuntu-latest\n", release_header)
        self.assertIn("    environment: release\n", release_header)
        self.assertNotRegex(release_header, r"(?m)^    if:")
        self.assertIn("RELEASE_TARGET_COMMIT: ${{ needs.validate.outputs.target-sha }}", release)
        self.assertNotIn("cache: maven", release)
        self.assertEqual(release.count("RELEASE_SKIP_TESTS: 'true'"), 3)
        self.assertIn("python3 -m unittest discover", validation)

    def test_validation_builds_unsigned_release_artifacts_without_deployment(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        command = next(script for script in workflow_commands(workflow)
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
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith("RELEASE_") and key not in guard.ENV_OPTIONS
                   and key not in ("MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_USERNAME",
                                   "SONATYPE_TOKEN_PASSWORD")}
            env.update(PATH=str(root) + os.pathsep + os.environ["PATH"],
                       RELEASE_GUARD=str(ROOT / "scripts/release/guard.py"),
                       RELEASE_LOG_DIR=str(root / "logs"), TRACE=str(root / "trace"))
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

    def target_fixture(self, root):
        source = root / "source"
        source.mkdir()
        self.git(source, "init", "--quiet", "--initial-branch=master")
        commits = []
        for version in ("initial", "release", "development"):
            (source / "version").write_text(version)
            self.git(source, "add", "version")
            self.git(source, "commit", "--quiet", "-m", version)
            commits.append(self.git(source, "rev-parse", "HEAD"))
            if version == "release":
                self.git(source, "tag", "v1.0.0")
        return source, commits

    def git(self, directory, *args):
        return subprocess.check_output(
            ["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgSign=false",
             "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
             "-C", str(directory), *args], text=True, stderr=subprocess.PIPE,
        ).strip()

    def checkout_validated_target(self, root, source, tag, expected):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        resolve = next(script for script in workflow_commands(workflow)
                       if "sed 's/^/sha=/'" in script)
        checkout = next(script for script in workflow_commands(workflow)
                        if 'git fetch --no-tags origin "$RELEASE_TARGET_COMMIT"' in script)
        validation = root / "validation"
        self.git(root, "clone", "--quiet", str(source), str(validation))
        output = root / "output"
        result = subprocess.run(
            ["bash", "-eo", "pipefail", "-c", resolve], cwd=validation,
            env={**os.environ, "RELEASE_TARGET_TAG": tag, "GITHUB_OUTPUT": str(output)},
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text(), f"sha={expected}\n")
        publisher = root / "publisher"
        self.git(root, "clone", "--quiet", "--no-tags", str(source), str(publisher))
        return checkout, publisher

    def test_publishing_master_uses_validated_commit_and_rejects_an_advanced_branch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            checkout, publisher = self.checkout_validated_target(root, source, "master", commits[-1])
            env = {**os.environ, "RELEASE_TARGET_TAG": "master", "RELEASE_TARGET_COMMIT": commits[-1]}
            result = subprocess.run(["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), commits[-1])
            self.assertEqual(self.git(publisher, "branch", "--show-current"), "master")
            self.git(source, "commit", "--quiet", "--allow-empty", "-m", "master advanced")
            result = subprocess.run(["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("master advanced after validation", result.stdout)

    def test_publishing_historical_target_does_not_resolve_a_moved_tag_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            checkout, publisher = self.checkout_validated_target(root, source, "v1.0.0", commits[0])
            self.git(source, "tag", "--force", "v1.0.0", commits[-1])
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                env={**os.environ, "RELEASE_TARGET_TAG": "v1.0.0", "RELEASE_TARGET_COMMIT": commits[0]},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), commits[0])
            self.assertEqual(self.git(publisher, "tag", "--list"), "")
            self.assertEqual((publisher / "version").read_text(), "initial")

    def test_publishing_master_pushes_from_a_sha_checkout_without_an_upstream(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        push = next(script for script in workflow_commands(workflow)
                    if "git push origin --follow-tags" in script)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            remote = root / "remote.git"
            self.git(root, "clone", "--quiet", "--bare", str(source), str(remote))
            checkout, publisher = self.checkout_validated_target(root, source, "master", commits[-1])
            self.git(publisher, "remote", "set-url", "origin", str(remote))
            # A full-SHA checkout starts detached and creates no tracking branch.
            self.git(publisher, "checkout", "--detach")
            self.git(publisher, "branch", "-D", "master")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                env={**os.environ, "RELEASE_TARGET_TAG": "master",
                     "RELEASE_TARGET_COMMIT": commits[-1]},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.git(publisher, "commit", "--quiet", "--allow-empty", "-m", "prepared release")
            self.git(publisher, "tag", "-a", "release-fixture", "-m", "prepared release")
            prepared = self.git(publisher, "rev-parse", "HEAD")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", push], cwd=publisher,
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(remote, "rev-parse", "refs/heads/master"), prepared)
            self.assertEqual(self.git(remote, "rev-parse", "refs/tags/release-fixture^{}"), prepared)


if __name__ == "__main__":
    unittest.main()
