"""Check the release workflow's credential boundaries without release credentials."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest

from workflow_helpers import workflow_source

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = workflow_source()
        self.steps = re.split(r'^      - name: ', self.workflow, flags=re.M)[1:]

    def test_separate_jobs_and_scoped_secrets(self):
        validate, release = self.workflow.split("\n  release:\n", 1)
        self.assertNotIn("secrets.", validate)
        self.assertNotIn("environment:", validate)
        self.assertIn("needs: validate", release)
        self.assertIn("environment: release", release)
        self.assertNotIn("gpg-private-key:", release)
        self.assertNotIn("cache: maven", release)
        self.assertLess(release.index("Check release credential safety"),
                        release.index("RELEASE_GPG_PRIVATE_KEY:"))
        self.assertLess(release.index("check-tokens"), release.index("Prepare release"))

    def test_only_credentialed_work_is_serialized(self):
        validate, release = self.workflow.split("\n  release:\n", 1)
        self.assertNotRegex(validate, r"(?m)^\s*concurrency:")
        self.assertIn("    concurrency:\n      group: connector-release\n"
                      "      cancel-in-progress: false\n", release)

    def run_shell_step(self, name, env, cwd=None):
        step = next(step for step in self.steps if step.splitlines()[0] == name)
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        return subprocess.run(["bash", "-c", script], cwd=cwd,
                              env={**os.environ, **env}, capture_output=True, text=True)

    def test_invalid_inputs_fail_before_build_and_approval(self):
        names = [step.splitlines()[0] for step in self.steps]
        self.assertLess(names.index("Validate release inputs"),
                        names.index("Set up validation Java"))
        defaults = {"GITHUB_REF": "refs/heads/master", "RELEASE_TARGET_TAG": "master",
                    "RELEASE_VERSION": ""}
        cases = [({}, True), ({"RELEASE_TARGET_TAG": "v1.2.3"}, True),
                 ({"RELEASE_VERSION": "3.0.1"}, True),
                 ({"GITHUB_REF": "refs/heads/other"}, False),
                 ({"RELEASE_TARGET_TAG": "other"}, False),
                 ({"RELEASE_TARGET_TAG": "v1.2.3~1"}, False),
                 ({"RELEASE_VERSION": "v3.0.1"}, False),
                 ({"RELEASE_VERSION": "3.0.1-SNAPSHOT"}, False),
                 ({"RELEASE_VERSION": "3.0.1\n"}, False)]
        for overrides, valid in cases:
            with self.subTest(overrides=overrides):
                result = self.run_shell_step("Validate release inputs", {**defaults, **overrides})
                self.assertEqual(result.returncode == 0, valid, result.stdout + result.stderr)

    def test_master_advance_fails_before_approval(self):
        with tempfile.TemporaryDirectory() as temp:
            origin = Path(temp) / "origin.git"
            checkout = Path(temp) / "checkout"

            def git(*args, cwd=temp):
                return subprocess.run(["git", *map(str, args)], cwd=cwd, check=True,
                                      capture_output=True, text=True).stdout.strip()

            git("init", "--bare", "--initial-branch=master", origin)
            git("clone", origin, checkout)
            git("config", "user.name", "Release tests", cwd=checkout)
            git("config", "user.email", "release-tests@example.com", cwd=checkout)
            git("commit", "--allow-empty", "--no-gpg-sign", "-m", "Initial target", cwd=checkout)
            git("push", "origin", "master", cwd=checkout)
            env = {"RELEASE_TARGET_COMMIT": git("rev-parse", "HEAD", cwd=checkout)}
            result = self.run_shell_step("Recheck master before approval", env, cwd=checkout)
            self.assertEqual(result.returncode, 0, result.stderr)
            git("commit", "--allow-empty", "--no-gpg-sign", "-m", "Advance master", cwd=checkout)
            git("push", "origin", "master", cwd=checkout)
            result = self.run_shell_step("Recheck master before approval", env, cwd=checkout)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("master advanced during validation", result.stdout)

    def test_readiness_is_rechecked_after_build_before_approval(self):
        validate, release = self.workflow.split("\n  release:\n", 1)
        build = validate.index("Validate target without release credentials")
        self.assertLess(build, validate.index("Recheck master before approval"))
        self.assertLess(build, validate.index("Recheck for open release blockers before approval"))
        self.assertIn("if: inputs.target-tag == 'master'", validate)
        blockers = next(step for step in self.steps
                        if step.startswith("Recheck for open release blockers before approval\n"))
        self.assertNotIn("if:", blockers)
        # Approval and the publishing queue can outlast validation; retain fresh checks.
        self.assertIn("master advanced after validation", release)
        self.assertIn("Check for open release blockers", release)
        self.assertIn("Recheck master before publication", release)

    def test_release_output_uploads_only_sanitized_maven_streams(self):
        uploads = [step for step in self.steps
                   if re.search(r"uses: actions/upload-artifact@", step)]
        self.assertEqual(len(uploads), 4)
        paths = []
        for step in uploads:
            self.assertIn("if: always()", step)
            selected = re.findall(r"^\s+path: (.+)$", step, re.M)
            self.assertEqual(len(selected), 1)
            self.assertIn(selected[0], ("${{ runner.temp }}/release-logs/stdout.log",
                                        "${{ runner.temp }}/release-logs/stderr.log"))
            paths.extend(selected)
        self.assertEqual(paths.count("${{ runner.temp }}/release-logs/stdout.log"), 2)
        self.assertEqual(paths.count("${{ runner.temp }}/release-logs/stderr.log"), 2)
        for action in re.findall(r"uses: (\S+)", self.workflow):
            if action != "./.github/workflows/release-credential-safety.yml":
                self.assertRegex(action, r"@[0-9a-f]{40}$")

    def test_dry_run_excludes_publication_and_notifications(self):
        dry_run = workflow_source("release-dry-run")
        for name in ("Push changes to SCM", "Create GitHub Release", "Notify Confluent Hub",
                     "Open fallback issue if Confluent was not notified"):
            self.assertNotIn(name, dry_run)
        self.assertNotIn("release blockers", dry_run)
        self.assertNotIn(": write", dry_run)
        self.assertNotIn("inputs.dry-run", self.workflow + dry_run)
        self.assertNotIn("Perform release dry-run", self.workflow)
        self.assertIn("-Drelease.autopublish=false", (ROOT / "Makefile").read_text())
        self.assertNotIn("git describe", (ROOT / "Makefile").read_text())

    def test_inputs_are_not_interpolated_into_shell(self):
        for step in self.steps:
            if '        run:' in step:
                self.assertNotIn('${{ inputs.', step.split('        run:', 1)[1])

    def test_master_recheck_runs_after_blockers_immediately_before_publication(self):
        names = [step.splitlines()[0] for step in self.steps]
        index = names.index("Recheck master before publication")
        self.assertEqual(names[index - 1], "Recheck for open release blockers")
        self.assertEqual(names[index + 1], "Perform release")
        self.assertIn("if: inputs.target-tag == 'master'",
                      self.steps[index])

    def test_policy_check_has_read_only_permissions_and_pinned_actions(self):
        policy = (ROOT / '.github/workflows/release-credential-safety.yml').read_text()
        self.assertIn('contents: read', policy)
        self.assertNotIn(': write', policy)
        self.assertIn('branches: [master]', policy)
        self.assertIn('python3 scripts/release/guard.py check', policy)
        for action in re.findall(r'uses: (\S+)', policy):
            self.assertRegex(action, r'@[0-9a-f]{40}$')


if __name__ == '__main__':
    unittest.main()
