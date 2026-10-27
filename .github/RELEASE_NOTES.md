# Release notes

The Release workflow publishes to Maven Central, pushes the release tag, then
runs `make github-release` to create generated GitHub release notes and upload
the JAR and Confluent ZIP. Existing published notes are preserved on retries;
artifacts are uploaded with `--clobber`. Missing remote tags fail, and an existing
draft fails visibly so a maintainer can deliberately finish publication.
Signing, credential checks, release blockers, and Confluent notifications remain
part of the existing release process. GitHub release publication remains limited
to the non-dry `master` path. Do not rerun the entire Release workflow to retry an
upload: it prepares a new Maven version.

`.github/release.yml` excludes PRs labeled `omit-from-release-notes`. The label is
for GitHub release presentation. Local Jira label-event callers ignore its manual
addition/removal while preserving other Jira events. Existing Jira copies are not
cleaned; other synchronization paths can restore previously copied metadata.

`actions/labeler` uses `.github/labeler.yml` as the canonical filename policy.
Every current and previous filename must qualify. Documentation, `src/test/`,
CI and development housekeeping, the listed release tests, and the ten guide
screenshots qualify, including mixtures of these paths. Documentation contents
still qualify when packaged with the connector. Runtime sources/resources,
`pom.xml`, `Makefile`, `scripts/release/guard.py`, executable examples/quickstarts,
`.github/workflows/release.yml`, packaged logos `images/connector.png` and
`images/scylla.png`, and unknown paths remain visible. Author, title, `chore`, and
`dependencies` labels never determine exclusion.

Classification runs on open, synchronize, reopen, edit, and close. Runs for one PR
are serialized, including manual runs. The workflow checks out repository-controlled
event tooling, never PR head code or an old PR base revision. Manual dispatch must
select the repository default branch before checkout; this precondition does not
make arbitrary modified workflow versions trustworthy.

Before checkout or dependency installation, the workflow confirms the exact PR
number from a matching trusted PR event or a successful manual PR lookup. Invalid,
ordinary-issue, unavailable, or mismatched targets cause no label writes. The
confirmed number is retained independently of helpers, so later setup or API
failures still attempt cleanup for that confirmed PR.

The guard requires a complete, unique file listing with valid rename metadata,
compares head/base revisions and counts around collection, then checks the same
snapshot and actual label after the stock action. A rename from a nonqualifying
path skips the action and removes the omission label. `sync-labels: true` handles
ordinary matching changes. Failed or unexpectedly skipped setup/classification
attempts omission-label removal without requiring installed helper dependencies,
and failures remain visible. API outages can prevent cleanup. Snapshot comparisons
and serialized runs are bounded checks, not atomicity or race-freedom guarantees.

## Before generating notes

Publication stays asynchronous. No automatic release/classification gate or
follow-up synchronization is scheduled. Before starting a new Release workflow
or `make github-release` publication:

1. Identify every PR in the intended release range since the previous release,
   including merged PRs that predate this workflow. Inspect the complete paginated
   file listing for each PR, checking both `filename` and `previous_filename`.
   Confirm that the listing is unique and matches the PR's changed-file count.
2. Classify each candidate through Actions → Classify release notes → Run workflow.
   Select the default branch and enter the PR number, or run:

   ```sh
   gh workflow run label-release-notes.yml --ref master -f pull-request=123
   ```

3. Wait for each run to succeed. In its final verification log, compare the printed
   `head`, `base`, `count`, and snapshot hash with collection and the candidate PR's
   current head/base/count. Confirm `omit` agrees with the current logical
   `omit-from-release-notes` label and with every current/previous filename.
4. Repeat classification and inspection for changed revisions, failed runs, missing
   verification logs, or stale labels. Recheck every candidate immediately before
   generating notes. If the merged comparison no longer supplies complete metadata,
   keep the PR visible and inspect it manually before generating notes.

Maintainers may correct the label manually after inspecting the same evidence;
subsequent classification reconciles it with the canonical policy. Manual checks
cannot prevent every concurrent change. If verification is missed, newly published
notes can retain omissions or unwanted support entries. Already published notes
remain unchanged by classification and upload retries.
