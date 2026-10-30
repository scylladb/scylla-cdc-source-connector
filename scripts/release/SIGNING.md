# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `F18AF4DD8C57C3227814158320256FBE9F33D0CA`
* Signing fingerprint: `F421F14EFAF125CBC3FC587507AA6EFF1829511B`
* Identity: `ScyllaDB Publisher (scylladb/scylla-cdc-source-connector) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:46:37+00:00`

The workflow first runs a `validate` job on its own runner without production
signing or package-publishing credentials. It checks the release tooling and
target policy, then builds the release profile with signing disabled. Project
tests run here unless `skip-tests` is selected; release helper tests always run.
Validation records the exact target commit as a job output.

The `release` job depends on successful validation and uses a fresh runner. Only
this job uses the GitHub deployment environment `release`, so approval is requested
after validation finishes. It checks out the validated commit, rejects `master` if
it has advanced, and independently checks target policy before importing keys.
It does not restore the validation workspace or Maven cache. Signed preparation,
signature verification, and publication happen after approval; project tests are
not repeated with production credentials. Historical tags are resolved only by
validation, so moving a tag during the approval wait cannot change the source.

Before running it, a repository owner must create and protect that environment,
require approval by `scylladb/drivers` (self-review allowed, administrator bypass
disabled), allow deployments only from `master`, and store
`GPG_PRIVATE_KEY`
(ASCII-armored protected signing-subkey export) and `GPG_PASSPHRASE` (single-line
passphrase) as environment secrets. Migrate any repository-level copies to this
environment and remove those copies after validating the migration. The workflow
binding does not configure protection or migrate secrets; owner setup remains
required, including before dry runs and reruns.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

Locally, the `release-prepare`, `release`, and `release-dry-run` Make targets
require the active `gh` account to be `scylladb-publisher` before importing
credentials or changing the checkout; switch accounts with `gh auth switch` if
needed. Local dry runs and internal release recipes enforce the same account
requirement. In Actions, the protected `release` environment authorizes operators,
including dry runs and reruns. The triggering actor need not be the publisher
account: signing still uses the repository's pinned publisher subkey and identity.

Each target runs its Maven commands in a scoped `signing.py exec` session. The
session imports only the protected signing-subkey export, checks that primary
private material is absent, and verifies a fresh-agent signing preflight against
the pinned public certificate. Anonymous pipes supply the key import and passphrase;
neither credential is written to a temporary file or included in command-line
arguments. Runtime Git settings apply only to the command and its children,
including Maven's local checkout clones. Sessions leave checkout Git configuration
unchanged and remove their temporary keyrings on success or failure. No credentials
or signing-session state are written to `GITHUB_ENV` or retained between workflow
steps. The workflow records the public pre-preparation commit SHA there as
`RELEASE_BASE_COMMIT` for later verification.

Maven's forked builds inherit the complete signing fingerprint with `!` through
`MAVEN_OPTS`; the GPG plugin receives `MAVEN_GPG_PASSPHRASE` through its environment.
Every GPG invocation reads the current passphrase, so checks and builds can outlive
the agent's passphrase cache. The protected signing-subkey export is consumed by
the session and removed from the child environment. Maven release builds need
access to the session keyring and passphrase while signing artifacts.

For maintainer-driven release preparation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
export RELEASE_VERSION=X.Y.Z
export RELEASE_BASE_COMMIT=$(git rev-parse HEAD)
make release-prepare
# Run additional checks against the prepared release.
make release-dry-run
# Use make release when ready for automatic publication.
```

Maven owns version changes, release and development commits, annotated tags, and
artifact signing. The Make recipes retain sanitized Maven stdout and stderr.
Preparation verifies every new commit and the release tag against the pinned
certificate before closing its session. The workflow repeats verification with
public material only. Perform and dry-run commands verify prepared history before
opening a new signing session. Keep `RELEASE_BASE_COMMIT` set to the original
pre-preparation commit for those commands; verification fails without that trusted
boundary. Do not derive it from the release tag's parent or replace it after
preparation. Do not change or replace existing release tags. A failed verification
blocks publication.

Both jobs check out the workflow's launch SHA and preserve its current Makefile,
guard, signing and asset helpers, public certificate, and shared blocker/Maven
setup scripts before selecting the target source. Test files are not preserved.
The release job uses the validated commit SHA instead of resolving the input ref
again. Explicit recursive Makefile selection keeps every phase on those preserved
recipes. Target POM and source files still come from the selected checkout and
must pass credential safety checks before any signing key is imported.

Complex checks can run before preparation or against signed outputs after a session
closes. Checks that need additional signatures can use their own scoped invocation:

```sh
python3 scripts/release/signing.py exec make YOUR_CHECK_TARGET
```

The low-level helper does not impose release-account policy. Wrapped commands have
no overall time limit; individual GPG operations have their own timeout. A failed
command propagates its exit status and still cleans up its session. Helper errors
identify a fixed operation and status or category without printing command arguments,
captured output, or exception payloads. `sign-files FILE...` signs and verifies a
batch of additional artifacts using one shared public keyring, preserving existing
signatures on reruns. Run `make test-release-signing` to test with disposable keys.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys F18AF4DD8C57C3227814158320256FBE9F33D0CA
gpg --fingerprint F18AF4DD8C57C3227814158320256FBE9F33D0CA
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require exactly one `GOODSIG` and one `VALIDSIG` record. The `GOODSIG` key ID must
match the signing fingerprint's final 16 hexadecimal digits, and `VALIDSIG` must
contain the complete signing fingerprint above and the expected primary
fingerprint. Reject `EXPKEYSIG`, `REVKEYSIG`, `EXPSIG`, `BADSIG`, or `ERRSIG`, even
when a matching `VALIDSIG` is also present. `git verify-tag --raw TAG` and
`git verify-commit --raw COMMIT` provide the equivalent check for Git objects.
Keyserver presence alone establishes neither trust nor the expected repository
identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.

## Token migration and release pause

The protected `release` environment was provisioned during this change. Its four
secrets are `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`, `SONATYPE_TOKEN_USERNAME`, and
`SONATYPE_TOKEN_PASSWORD`. The repository-level copies of those four secrets were
removed immediately after verifying protection settings and environment secret
names. `EMAIL_USERNAME` and `EMAIL_PASSWORD` remain at repository scope.

**Releases are paused:** both Sonatype secrets contain
`PLACEHOLDER_NOT_CONFIGURED`. Replace them with real Central Portal user tokens
and merge this workflow before resuming. Missing or placeholder tokens fail before
workflow preparation and before the upload-capable `make release` and
`make release-dry-run` commands. Local signing preflight and preparation remain
available without Sonatype credentials. Upload secrets with `gh secret set` using
stdin or input redirection; never put credentials in arguments or commit private
material. Check local file checksums against the offline `metadata.json` first.

Local Maven needs a `central` server in `~/.m2/settings.xml`, with username
`${env.SONATYPE_TOKEN_USERNAME}` and password `${env.SONATYPE_TOKEN_PASSWORD}`.
Export the real tokens in the command environment. Keep passphrases in environment
variables or private input files, and avoid Maven debug logging and CLI credential
properties. The workflow generates the same server configuration with setup-java.

`release-dry-run` performs a real, signed upload to Central with
`release.autopublish=false`, leaving an unpublished deployment for manual inspection.
It requires real Sonatype credentials. It does not push SCM changes, create a GitHub
release, or send Confluent email. Delete unused deployments in the Central Portal.
See [Sonatype's Maven publishing documentation](https://central.sonatype.org/publish/publish-portal-maven/).

## GitHub assets and recovery

The fat JAR and connector ZIP are signed and verified with the same exact subkey
after `release:perform`. GitHub receives both files and their detached `.asc`
signatures. Tag, version and filenames come from `release.properties` and the tagged
Maven POM, so unrelated or newer tags cannot select another release's assets.
Preparation saves verified metadata in `target/release-prepared.properties` because
Maven removes `release.properties` after a successful perform.
`make github-release` verifies the existing signatures with the public certificate
before checking GitHub. It refuses an existing release or draft and never
regenerates signatures. Discovery errors stop publication. For a new release,
it creates a draft with all four files and an existing remote tag, then publishes
only after all uploads succeed.
New published releases have immutable assets and tags. All `v*` tags are also
protected against updates and deletion without bypass.
Enabling release immutability does not make already-published releases immutable.
The separate [tag rules](https://github.com/scylladb/scylla-cdc-source-connector/rules/25759576)
protect existing `v*` tags too.
This follows [GitHub's draft, attach, publish sequence](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases#best-practices-for-publishing-immutable-releases).
Maven stdout and stderr artifacts contain sanitized output only. Complete streams
remain available after capture errors; unfinished streams, overflowing captures
and sanitization failures produce fixed diagnostics. No raw output is spooled.
After Central publication succeeds for `master`, the workflow pushes the signed
commits and tag before signing GitHub assets so an asset-signing failure leaves
the prepared Git objects available for recovery.

After a partial release, **do not rerun preparation or the entire workflow**. Record
the original launch SHA, `RELEASE_BASE_COMMIT`, prepared tag and signed commit IDs,
Central deployment ID and any published assets before choosing recovery:

1. If preparation failed before upload, inspect the local commits/tag and sanitized
   logs. Keep a recoverable copy. Restart from a clean checkout of the original
   baseline only after confirming no deployment or remote tag exists.
2. If Central accepted an upload, inspect its status in the Portal. Publish the
   already validated deployment or drop an unpublished dry-run deployment as
   appropriate. Never upload another release version to hide an uncertain result.
   Published coordinates are immutable.
3. If publication succeeded but SCM push failed, recover the exact signed release
   and development commits plus annotated tag from the original runner or a trusted
   retained checkout. Verify them with the public certificate and original baseline,
   check the remote branch and tag, then reconcile and push those exact objects.
   Sanitized log artifacts alone cannot reconstruct Git objects. If the objects
   are unavailable, stop and investigate rather than recreate or move the tag.
4. If GitHub publication stopped, inspect whether the release is still a draft
   and verify its exact prepared tag. For a draft, recover matching metadata and
   trusted local assets, compare the hashes of existing draft files, and verify detached
   signatures with the public certificate. Upload only missing files without
   `--clobber`, then publish the draft after all four files are complete and
   verified (`gh release edit TAG --draft=false`). If an existing file differs,
   stop and investigate instead of overwriting it. Do not rerun the automated
   `github-release` target on an existing draft. Published immutable assets and
   tags cannot be repaired in place; corrections require a new release version.
5. If only Confluent notification failed, send the message in the workflow summary
   or fallback issue manually. Do not run Maven or publish again.

Historical releases continue to use the predecessor of the requested `vX.Y.Z` tag,
subject to the same credential safety checks. Only the validated SHA is handed to
the release runner. As before, historical targets do not push changes, create a
GitHub release or send email. Old POMs with unsupported signing/publishing plugins
are rejected before importing credentials.
