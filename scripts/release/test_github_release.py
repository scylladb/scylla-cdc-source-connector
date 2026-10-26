"""Exercise the release Make target with a fake GitHub CLI, without publication."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class GitHubReleaseTests(unittest.TestCase):
    def run_release(self, mode, tag='v2.0.8'):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            (temp / 'gh').write_text('''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['CALL_LOG'], 'a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\\n')
mode = os.environ['RELEASE_MODE']
if sys.argv[1] == 'api' and mode == 'missing-tag':
    sys.exit(1)
if sys.argv[1:3] == ['release', 'view']:
    if mode == 'new':
        sys.exit(1)
    print('true' if mode == 'draft' else 'false')
if sys.argv[1:3] == ['release', 'upload'] and mode == 'upload-failure':
    sys.exit(1)
''')
            (temp / 'gh').chmod(0o755)
            log = temp / 'calls.jsonl'
            result = subprocess.run(
                ['make', 'github-release', f'RELEASE_TAG={tag}'], cwd=ROOT,
                env={**os.environ, 'PATH': f"{temp}:{os.environ['PATH']}",
                     'CALL_LOG': str(log), 'RELEASE_MODE': mode},
                text=True, capture_output=True,
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_new_release_verifies_tag_and_generates_notes_with_both_artifacts(self):
        result, calls = self.run_release('new')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[0][0], 'api')
        create = calls[-1]
        self.assertEqual(create[:3], ['release', 'create', 'v2.0.8'])
        self.assertIn('--verify-tag', create)
        self.assertIn('--generate-notes', create)
        self.assertTrue(any(arg.endswith('.jar') for arg in create))
        self.assertTrue(any(arg.endswith('.zip') for arg in create))
        config = (ROOT / '.github/release.yml').read_text()
        self.assertIn('omit-from-release-notes', config)

    def test_published_release_preserves_notes_and_replaces_artifacts(self):
        result, calls = self.run_release('published')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[-1][:3], ['release', 'upload', 'v2.0.8'])
        self.assertIn('--clobber', calls[-1])
        self.assertFalse(any(call[:2] == ['release', 'create'] for call in calls))

    def test_draft_fails_without_publishing_or_uploading(self):
        result, calls = self.run_release('draft')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Existing release is a draft', result.stderr)
        self.assertEqual(len(calls), 2)

    def test_missing_tag_invalid_tag_and_upload_errors_fail_visibly(self):
        result, calls = self.run_release('missing-tag')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        result, calls = self.run_release('new', 'unknown')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [])
        result, _ = self.run_release('upload-failure')
        self.assertNotEqual(result.returncode, 0)

    def test_github_release_still_follows_authoritative_publication(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        self.assertLess(workflow.index('- name: Perform release'), workflow.index('- name: Push changes to SCM'))
        self.assertLess(workflow.index('- name: Push changes to SCM'), workflow.index('run: make github-release'))
        self.assertLess(workflow.index('run: make github-release'), workflow.index('- name: Notify Confluent Hub'))
        self.assertIn("if: inputs.dry-run == false && inputs.target-tag == 'master'", workflow)


if __name__ == '__main__':
    unittest.main()
