# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `F18AF4DD8C57C3227814158320256FBE9F33D0CA`
* Signing fingerprint: `F421F14EFAF125CBC3FC587507AA6EFF1829511B`
* Identity: `ScyllaDB Publisher (scylladb/scylla-cdc-source-connector) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:46:37+00:00`

Repository Actions secrets `GPG_PRIVATE_KEY` (ASCII-armored protected signing-subkey
export) and `GPG_PASSPHRASE` (single-line passphrase) supply release signing.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

`signing.py configure` checks that the imported private key is the protected signing
subkey without usable primary private material. It performs a fresh-agent signing
preflight and verifies the exact signing and certification fingerprints against a
separate public-only keyring. Anonymous pipes supply the key import and passphrase; neither credential is
written to a temporary file or included in command-line arguments. Each signing
step receives its secret explicitly; setup does not write it into `GITHUB_ENV`. Git configuration is job-specific
and inherited by release-tool clones. Always run `signing.py cleanup` after a CI job.

For maintainer-driven release preparation, use an isolated invocation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
python3 scripts/release/signing.py exec git tag -s vX.Y.Z -m 'Release vX.Y.Z'
python3 scripts/release/signing.py verify-tag vX.Y.Z
```

Use the same `exec` prefix with the repository's release-commit preparation command
where applicable. It restores checkout signing configuration on exit. Do not change
or replace existing release tags. A failed verification blocks publication.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys F18AF4DD8C57C3227814158320256FBE9F33D0CA
gpg --fingerprint F18AF4DD8C57C3227814158320256FBE9F33D0CA
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require one `VALIDSIG` record with the signing fingerprint above and the expected
primary fingerprint. `git verify-tag --raw TAG` and `git verify-commit --raw COMMIT`
provide the equivalent check for Git objects. Keyserver presence alone establishes
neither trust nor the expected repository identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.
