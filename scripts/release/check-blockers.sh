#!/usr/bin/env bash
set -euo pipefail
blocker=$(gh issue list \
  --repo "${GITHUB_REPOSITORY}" \
  --state open \
  --label release-blocker \
  --limit 1 \
  --json title,url \
  --jq '.[0] // empty')
if [[ -n "${blocker}" ]]; then
  blocker_title=$(jq -r '.title' <<< "${blocker}")
  blocker_url=$(jq -r '.url' <<< "${blocker}")
  echo "::error title=Release blocked::${blocker_title} (${blocker_url})"
  echo "Open release blocker: ${blocker_title} (${blocker_url})"
  exit 1
fi
echo "No open release blockers found."
