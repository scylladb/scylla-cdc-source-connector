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

`.github/release.yml` excludes PRs labeled `omit-from-release-notes`.
