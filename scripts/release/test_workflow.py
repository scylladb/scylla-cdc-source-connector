"""Check the release workflow's credential boundaries without release credentials."""

from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / '.github/workflows/release.yml').read_text()
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

    def test_release_output_uploads_only_sanitized_maven_streams(self):
        self.assertEqual(self.workflow.count("if: always()"), 4)
        self.assertEqual(self.workflow.count("path: ${{ runner.temp }}/release-logs/"), 4)
        for action in re.findall(r"uses: (\S+)", self.workflow):
            self.assertRegex(action, r"@[0-9a-f]{40}$")

    def test_dry_run_excludes_publication_and_notifications(self):
        for name in ("Push changes to SCM", "Create GitHub Release", "Notify Confluent Hub",
                     "Open fallback issue if Confluent was not notified"):
            step = next(step for step in self.steps if step.startswith(name))
            self.assertIn("inputs.dry-run == false", step)
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
        self.assertIn("if: inputs.dry-run == false && inputs.target-tag == 'master'",
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
