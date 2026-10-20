"""Check the release workflow's credential boundaries without release credentials."""

from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / '.github/workflows/release.yml').read_text()
        self.steps = re.split(r'^      - name: ', self.workflow, flags=re.M)[1:]

    def test_guard_survives_checkout_and_validates_before_key_import(self):
        text = self.workflow
        preserve = text.index('cp scripts/release/guard.py "$RUNNER_TEMP/release-guard.py"')
        checkout = text.index('git checkout')
        validate = text.index('python3 "$RELEASE_GUARD" check')
        credentials = text.index('gpg-private-key:')
        self.assertLess(preserve, checkout)
        self.assertLess(checkout, validate)
        self.assertLess(validate, credentials)
        self.assertIn('echo "RELEASE_GUARD=$RUNNER_TEMP/release-guard.py" >> "$GITHUB_ENV"', text)
        self.assertNotRegex(text, r'(?m)^    env:\n(?:.*\n)*?\s+RELEASE_GUARD:')

    def test_all_four_maven_invocations_are_guarded(self):
        commands = [line.strip() for line in self.workflow.splitlines()
                    if re.search(r'\bmvn\b', line) and not line.lstrip().startswith('#')]
        # The fallback notification contains a prose reminder about release:prepare.
        commands = [line for line in commands if not line.startswith('**Do not re-run')]
        self.assertEqual(len(commands), 4)
        for goal in ('clean', 'release:clean', 'release:prepare', 'release:perform'):
            self.assertTrue(any('python3 "$RELEASE_GUARD" run mvn -B -ntp '
                                in command and goal in command for command in commands), goal)

    def test_release_output_has_no_log_artifacts(self):
        self.assertNotIn('RELEASE_LOG_DIR', self.workflow)
        self.assertNotIn('release-logs', self.workflow)
        self.assertNotIn('actions/upload-artifact@', self.workflow)

    def test_invalid_target_tag_stops_before_credentials(self):
        checkout = next(step for step in self.steps if step.startswith('Checkout historical release target\n'))
        self.assertIn('if: inputs.target-tag != \'master\'', checkout)
        self.assertIn('Unsupported target tag', checkout)
        self.assertRegex(checkout, r'else\n\s+echo [^\n]+\n\s+exit 1')

    def test_inputs_are_not_interpolated_into_shell(self):
        for step in self.steps:
            if '        run:' in step:
                self.assertNotIn('${{ inputs.', step.split('        run:', 1)[1])

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
