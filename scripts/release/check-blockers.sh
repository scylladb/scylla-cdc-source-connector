#!/usr/bin/env bash
set -euo pipefail
# The issues endpoint includes pull requests. Unlike gh issue list, this checks
# both kinds of blocker, including any beyond the first page.
pages=$(gh api --paginate --slurp \
  "repos/${GITHUB_REPOSITORY}/issues?state=open&labels=release-blocker&per_page=100")
blockers=$(jq -r '.[][] | .html_url' <<< "${pages}")
if [[ -n "${blockers}" ]]; then
  echo "::error title=Release blocked::Open issues or pull requests are labelled release-blocker."
  printf 'Open release blockers:\n%s\n' "${blockers}"
  exit 1
fi
echo "No open release blockers found."
