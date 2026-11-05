"""Check release-note configuration and its signed publication integration.

Signed asset publication, draft rejection and failure recovery are exercised by
test_assets.py against the production assets helper.
"""

from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class GitHubReleaseTests(unittest.TestCase):
    def test_github_release_still_follows_authoritative_publication(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        publication = 'RELEASE_GUARD="$RUNNER_TEMP/release-safety/guard.py" github-release'
        self.assertLess(workflow.index('- name: Perform release'),
                        workflow.index('- name: Push changes to SCM'))
        self.assertLess(workflow.index('- name: Push changes to SCM'),
                        workflow.index(publication))
        self.assertLess(workflow.index('- name: Sign and verify GitHub assets'),
                        workflow.index(publication))
        self.assertLess(workflow.index(publication),
                        workflow.index('- name: Notify Confluent Hub'))
        steps = re.split(r'^      - name: ', workflow, flags=re.M)[1:]
        create = next(step for step in steps if step.startswith('Create GitHub Release\n'))
        self.assertIn(publication, create)
        self.assertRegex(create,
                         r"(?m)^        if: inputs\.dry-run == false && inputs\.target-tag == 'master'$")


if __name__ == '__main__':
    unittest.main()
