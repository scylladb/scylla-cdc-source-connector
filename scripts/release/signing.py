#!/usr/bin/env python3
"""Configure and verify the repository's protected release signing subkey."""

import os
from pathlib import Path
import shutil
import shlex
import subprocess
import sys
import tempfile
import uuid

PRIMARY = "F18AF4DD8C57C3227814158320256FBE9F33D0CA"
SIGNING = "F421F14EFAF125CBC3FC587507AA6EFF1829511B"
PUBLIC_KEY = Path(__file__).with_name("publisher.asc")


class SigningError(ValueError):
    pass


def run(command, env=None):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise SigningError("Release signing command failed; values withheld.")
    return result.stdout + result.stderr


def gpg(*args, env=None):
    return run(["gpg", "--no-options", "--batch", "--no-tty", "--no-auto-key-retrieve",
                *map(str, args)], env)


def verify_status(status):
    signatures = [line.split() for line in status.splitlines()
                  if line.startswith("[GNUPG:] VALIDSIG ")]
    if (len(signatures) != 1 or len(signatures[0]) != 12
            or signatures[0][2] != SIGNING or signatures[0][-1] != PRIMARY):
        raise SigningError("Signature does not match the repository's release subkey.")


def protected_subkey():
    rows = [line.split(":") for line in
            gpg("--with-colons", "--with-keygrip", "--list-secret-keys").splitlines()]
    fingerprints = [row[9] for row in rows if row[0] == "fpr"]
    keys = [row for row in rows if row[0] in ("sec", "ssb")]
    grips = [row[9] for row in rows if row[0] == "grp"]
    if (fingerprints != [PRIMARY, SIGNING] or len(keys) != 2 or len(grips) != 2
            or keys[0][14] != "#" or keys[1][14] != "+"):
        raise SigningError("Import only this repository's signing-private.asc release subkey.")
    keydir = Path(os.environ["GNUPGHOME"]) / "private-keys-v1.d"
    if ((keydir / (grips[0] + ".key")).exists()
            or b"(protected-private-key" not in (keydir / (grips[1] + ".key")).read_bytes()):
        raise SigningError("Release signing requires a protected subkey without the primary secret.")


def write_private(path, text, executable=False):
    with open(path, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
        output.write(text)
    if executable:
        path.chmod(0o700)


def public_environment(directory):
    return {**os.environ, "GNUPGHOME": str(directory / "public")}


def passphrase_value():
    value = os.environ.get("RELEASE_GPG_PASSPHRASE",
                           os.environ.get("MAVEN_GPG_PASSPHRASE",
                                          os.environ.get("PGP_PASSPHRASE", "")))
    if "RELEASE_GPG_PASSPHRASE_FILE" in os.environ:
        value = Path(os.environ["RELEASE_GPG_PASSPHRASE_FILE"]).read_text().removesuffix("\n")
    if not value or "\n" in value or "\r" in value or len(value.encode()) > 1024:
        raise SigningError("Release signing requires a nonempty single-line passphrase.")
    return value


def credential_gpg(args, payload=None, capture=True):
    # A separate pipe preserves stdin for Git's payload. Its descriptor, never
    # its contents, appears in argv; pass_fds deliberately survives only to gpg.
    reader, writer = os.pipe()
    try:
        try:
            os.write(writer, (passphrase_value() + "\n").encode())
        finally:
            os.close(writer)
        result = subprocess.run(
            ["gpg", "--no-options", "--batch", "--no-tty", "--no-auto-key-retrieve",
             "--pinentry-mode", "loopback", "--passphrase-fd", str(reader), *map(str, args)],
            input=payload, capture_output=capture, text=capture, pass_fds=(reader,), timeout=60)
        if capture:
            if result.returncode:
                raise SigningError("Release signing command failed; values withheld.")
            return result.stdout + result.stderr
        return result.returncode
    finally:
        os.close(reader)


def configure():
    passphrase_value()
    os.environ.setdefault("RUNNER_TEMP", tempfile.gettempdir())
    directory = Path(tempfile.mkdtemp(prefix="release-signing-", dir=os.environ["RUNNER_TEMP"]))
    try:
        private = os.environ.get("RELEASE_GPG_PRIVATE_KEY")
        if "RELEASE_GPG_PRIVATE_KEY_FILE" in os.environ:
            private = Path(os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]).read_text()
        if private:
            signing_home = directory / "keyring"
            signing_home.mkdir(mode=0o700)
            os.environ["GNUPGHOME"] = str(signing_home)
            credential_gpg(["--import"], payload=private)
        protected_subkey()
        wrapper = directory / "gpg-wrapper"
        write_private(wrapper, '#!/bin/sh\nexec python3 '
                      + shlex.quote(str(Path(__file__).resolve()))
                      + ' gpg-program "$@"\n', executable=True)
        public = directory / "public"
        public.mkdir(mode=0o700)
        verify_env = public_environment(directory)
        gpg("--import", PUBLIC_KEY, env=verify_env)
        artifact = directory / "preflight"
        write_private(artifact, "Repository release signing preflight\n")
        # Kill the dedicated signing agent: cached credentials cannot satisfy this check.
        run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
        credential_gpg(["--local-user", SIGNING + "!", "--detach-sign", artifact])
        verify_status(gpg("--status-fd", "1", "--verify", str(artifact) + ".sig", artifact,
                          env=verify_env))
        config = directory / "gitconfig"
        # A job-specific global file is inherited by Maven's localCheckout clones too.
        write_private(config, "")
        settings = {"user.name": "ScyllaDB Publisher", "user.email": "publish.code@scylladb.com",
                    "user.signingkey": SIGNING + "!", "commit.gpgsign": "true",
                    "tag.gpgSign": "true", "gpg.format": "openpgp", "gpg.program": str(wrapper)}
        for name, value in settings.items():
            run(["git", "config", "--file", str(config), name, value])
            run(["git", "config", "--local", name, value])
        values = {
            "RELEASE_SIGNING_DIR": str(directory),
            "RELEASE_GPG_WRAPPER": str(wrapper),
            "GIT_CONFIG_GLOBAL": str(config),
            "RELEASE_MAVEN_OPTS": "-Dgpg.keyname=" + SIGNING + "!",
            "RELEASE_BASE_COMMIT": run(["git", "rev-parse", "HEAD"]).strip(),
        }
        if private:
            values["GNUPGHOME"] = str(signing_home)
        with Path(os.environ.get("GITHUB_ENV", os.devnull)).open("a") as output:
            for name, value in values.items():
                marker = "SIGNING_" + uuid.uuid4().hex
                output.write(f"{name}<<{marker}\n{value}\n{marker}\n")
        return values
    except BaseException:
        for home in (directory / "keyring", directory / "public"):
            if home.exists():
                subprocess.run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"],
                               capture_output=True)
        shutil.rmtree(directory)
        raise


def verify_release(tag):
    directory = Path(os.environ["RELEASE_SIGNING_DIR"])
    env = public_environment(directory)
    baseline = os.environ["RELEASE_BASE_COMMIT"]
    run(["git", "merge-base", "--is-ancestor", baseline, "HEAD"])
    commits = run(["git", "rev-list", baseline + "..HEAD"]).splitlines()
    if not commits:
        raise SigningError("No release commits were created.")
    for commit in commits:
        verify_status(run(["git", "-c", "gpg.program=gpg", "verify-commit", "--raw", commit], env))
    ref = "refs/tags/" + tag
    run(["git", "check-ref-format", ref])
    if run(["git", "rev-parse", ref + "^{commit}"]).strip() not in commits:
        raise SigningError("Release tag does not point to a newly signed release commit.")
    verify_status(run(["git", "-c", "gpg.program=gpg", "verify-tag", "--raw", ref], env))



def verify_commit(commit):
    with tempfile.TemporaryDirectory(prefix="release-public-") as temp:
        public = Path(temp)
        public.chmod(0o700)
        env = {**os.environ, "GNUPGHOME": str(public)}
        try:
            gpg("--import", PUBLIC_KEY, env=env)
            verify_status(run(["git", "-c", "gpg.program=gpg", "verify-commit", "--raw", commit], env))
        finally:
            run(["gpgconf", "--homedir", str(public), "--kill", "gpg-agent"])


def verify_tag(tag, commit=None):
    ref = "refs/tags/" + tag
    run(["git", "check-ref-format", ref])
    if run(["git", "cat-file", "-t", ref]).strip() != "tag":
        raise SigningError("Release tags must be signed annotated tags.")
    if commit is not None and run(["git", "rev-parse", ref + "^{commit}"]).strip() != commit:
        raise SigningError("Release tag points to an unexpected commit.")
    with tempfile.TemporaryDirectory(prefix="release-public-") as temp:
        public = Path(temp)
        public.chmod(0o700)
        env = {**os.environ, "GNUPGHOME": str(public)}
        try:
            gpg("--import", PUBLIC_KEY, env=env)
            verify_status(run(["git", "-c", "gpg.program=gpg", "verify-tag", "--raw", ref], env))
        finally:
            run(["gpgconf", "--homedir", str(public), "--kill", "gpg-agent"])


def sign_files(files):
    if not files:
        raise SigningError("At least one release artifact is required.")
    directory = Path(os.environ["RELEASE_SIGNING_DIR"])
    for filename in files:
        artifact = Path(filename).resolve()
        if not artifact.is_file() or artifact.suffix == ".asc":
            raise SigningError("Expected an unsigned release artifact.")
        # Existing signatures are immutable: never overwrite them on a rerun.
        signature = Path(str(artifact) + ".asc")
        if not signature.exists():
            credential_gpg(["--local-user", SIGNING + "!", "--armor",
                            "--detach-sign", "--output", signature, artifact])
        verify_status(gpg("--status-fd", "1", "--verify", signature, artifact,
                          env=public_environment(directory)))


def verify_prepared():
    properties = Path("release.properties").read_text().splitlines()
    tags = [line.removeprefix("scm.tag=") for line in properties if line.startswith("scm.tag=")]
    if len(tags) != 1:
        raise SigningError("Expected exactly one prepared Maven release tag.")
    verify_release(tags[0])


def cleanup():
    value = os.environ.get("RELEASE_SIGNING_DIR")
    if not value:
        return
    directory = Path(value).resolve()
    if (directory.parent != Path(os.environ["RUNNER_TEMP"]).resolve()
            or not directory.name.startswith("release-signing-")):
        raise SigningError("Unexpected release signing directory.")
    if directory.exists():
        for home in (directory / "keyring", directory / "public"):
            if home.exists():
                run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"])
        shutil.rmtree(directory)


def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "gpg-program":
            # Git requires stdout/stderr byte-for-byte; do not print a success banner.
            return credential_gpg(sys.argv[2:], capture=False)
        if sys.argv[1:] == ["configure"]:
            configure()
        elif len(sys.argv) == 3 and sys.argv[1] == "verify-release":
            verify_release(sys.argv[2])
        elif sys.argv[1:] == ["verify-prepared"]:
            verify_prepared()
        elif len(sys.argv) == 3 and sys.argv[1] == "verify-commit":
            verify_commit(sys.argv[2])
        elif len(sys.argv) == 3 and sys.argv[1] == "verify-tag":
            verify_tag(sys.argv[2])
        elif len(sys.argv) > 2 and sys.argv[1] == "sign-files":
            sign_files(sys.argv[2:])
        elif len(sys.argv) > 2 and sys.argv[1] == "exec":
            config = Path(run(["git", "rev-parse", "--git-path", "config"]).strip())
            previous = config.read_bytes()
            values = {}
            try:
                values = configure()
                env = {**os.environ, **values}
                env["MAVEN_OPTS"] = (env.get("MAVEN_OPTS", "") + " "
                                     + values["RELEASE_MAVEN_OPTS"]).strip()
                result = subprocess.run(sys.argv[2:], env=env)
                if result.returncode:
                    raise SigningError("Release command failed.")
            finally:
                try:
                    if values:
                        os.environ.update(values)
                        cleanup()
                finally:
                    config.write_bytes(previous)
        elif sys.argv[1:] == ["cleanup"]:
            cleanup()
        else:
            raise SigningError("Expected configure, verify-prepared, verify-release TAG, verify-tag TAG, sign-files FILE..., exec COMMAND..., or cleanup.")
        print("Release signing " + sys.argv[1] + " passed.")
        return 0
    except SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release signing failed; values withheld.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
