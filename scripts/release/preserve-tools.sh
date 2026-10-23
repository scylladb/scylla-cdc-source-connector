#!/usr/bin/env bash
set -euo pipefail
if [[ "$GITHUB_REF" != refs/heads/master ]]; then
  echo "Releases must be dispatched from master."
  exit 1
fi
mkdir -p "$RUNNER_TEMP/release-safety"
# Preserve only runtime tools from the launch revision before changing source.
cp scripts/release/{guard.py,signing.py,assets.py,publisher.asc,check-blockers.sh,setup-maven.sh} \
  Makefile "$RUNNER_TEMP/release-safety/"
echo "RELEASE_GUARD=$RUNNER_TEMP/release-safety/guard.py" >> "$GITHUB_ENV"
echo "RELEASE_LOG_DIR=$RUNNER_TEMP/release-logs" >> "$GITHUB_ENV"
