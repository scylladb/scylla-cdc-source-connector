"""Adversarial output capture tests adapted from Alternator 671e7eb."""
import base64
import json
import os
from pathlib import Path
import signal
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.parse
import guard
ROOT = Path(__file__).resolve().parents[2]


def running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


class CaptureTests(unittest.TestCase):
    def deliver_during_stopped_teardown(self, process, signum):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            self.assertIsNone(process.poll(), "Guard exited before the repeated signal")
            status = dict(line.split(":", 1) for line in Path(f"/proc/{process.pid}/status").read_text().splitlines())
            if status["State"].strip().startswith("T"):
                break
            time.sleep(0.01)
        else:
            self.fail("Maven did not stop guard during teardown")
        # A stopped, unreaped process cannot exit between this kill and observation.
        os.kill(process.pid, signum)
        status = dict(line.split(":", 1) for line in Path(f"/proc/{process.pid}/status").read_text().splitlines())
        pending = int(status["SigPnd"], 16) | int(status["ShdPnd"], 16)
        self.assertNotEqual(pending & (1 << (signum - 1)), 0, "Repeated signal was not queued")
        os.kill(process.pid, signal.SIGCONT)

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux signal observation required")
    def test_repeated_termination_signals_stop_maven(self):
        for signum, repeated in ((signal.SIGTERM, signal.SIGTERM), (signal.SIGHUP, signal.SIGINT)):
            with self.subTest(signum=signum, repeated=repeated), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready"
                mvn = root / "mvn"
                mvn.write_text(f"#!{sys.executable}\nimport os, signal, time\n"
                               "def stop_guard(signum,frame): os.kill(os.getppid(),signal.SIGSTOP)\n"
                               "signal.signal(signal.SIGTERM, stop_guard)\n"
                               "signal.signal(signal.SIGHUP, stop_guard)\n"
                               "os.write(1, b'-Dpassword=\"first\\nsecret tail')\n"
                               f"open({str(ready) + '.tmp'!r}, 'w').write(str(os.getpid()))\n"
                               f"os.rename({str(ready) + '.tmp'!r}, {str(ready)!r})\n"
                               "time.sleep(30)\n")
                mvn.chmod(0o755)
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                    env=dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                             RELEASE_LOG_DIR=str(root / "logs")),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child = int(ready.read_text())
                    os.kill(process.pid, signum)
                    self.deliver_during_stopped_teardown(process, repeated)
                    stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 128 + signum)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child, 0)
                    for name, console in (("stdout", stdout), ("stderr", stderr)):
                        self.assertEqual(console, guard.CANCELLED_LOG)
                        self.assertEqual((root / "logs" / (name + ".log")).read_bytes(), console)
                finally:
                    if process.poll() is None:
                        process.kill()
                    if ready.exists():
                        try:
                            os.killpg(int(ready.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            # Successful guard cleanup has already reaped Maven.
                            pass
                    process.communicate(timeout=5)

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process observation required")
    def test_signal_during_completed_maven_cleanup_preserves_interrupt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "child"
            descendant = ("import os,signal,time\nfrom pathlib import Path\n"
                          "def stop_guard(signum,frame): os.kill(int(os.environ['TEST_GUARD_PID']),signal.SIGSTOP)\n"
                          "signal.signal(signal.SIGTERM,stop_guard)\n"
                          f"Path({str(ready)!r}).write_text(str(os.getpid()))\n"
                          "os.close(1)\nos.close(2)\ntime.sleep(30)\n")
            command = [sys.executable, "-c",
                       "import os,subprocess,sys,time\nfrom pathlib import Path\n"
                       + f"subprocess.Popen([sys.executable,'-c',{descendant!r}])\n"
                       + "deadline=time.monotonic()+3\n"
                       + f"while not Path({str(ready)!r}).exists() and time.monotonic()<deadline: time.sleep(0.01)\n"
                       + f"assert Path({str(ready)!r}).exists()\n"
                       + "os.write(1,b'completed: dummy-secret\\n')\n"]
            script = (
                "import guard,os,signal\nfrom pathlib import Path\n"
                "signal.signal(signal.SIGTERM,guard.interrupted)\n"
                "signal.signal(signal.SIGHUP,guard.interrupted)\n"
                "env=dict(os.environ,TEST_GUARD_PID=str(os.getpid()))\n"
                f"raise SystemExit(guard.capture({command!r},env,[b'dummy-secret'],Path({str(root / 'logs')!r})))"
            )
            process = subprocess.Popen([sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.deliver_during_stopped_teardown(process, signal.SIGINT)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 128 + signal.SIGINT)
                self.assertEqual(stdout, b"completed: [REDACTED]\n")
                self.assertEqual(stderr, b"")
                for name, console in (("stdout", stdout), ("stderr", stderr)):
                    self.assertEqual((root / "logs" / (name + ".log")).read_bytes(), console)
                deadline = time.monotonic() + 1
                while running(int(ready.read_text())) and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertFalse(running(int(ready.read_text())))
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate()
                if ready.exists() and running(int(ready.read_text())):
                    os.kill(int(ready.read_text()), signal.SIGKILL)

    def test_encoded_and_multiline_credentials(self):
        env = {
            "MAVEN_GPG_PASSPHRASE": "first line\nsecond line+/=",
            "SONATYPE_TOKEN_USERNAME": "dummy-user",
            "SONATYPE_TOKEN_PASSWORD": "dummy-password",
        }
        for value in [*env.values(), "dummy-user:dummy-password"]:
            for encoded in (
                value,
                base64.b64encode(value.encode()).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote(urllib.parse.quote(value)),
                urllib.parse.quote(urllib.parse.quote(value), safe=""),
                urllib.parse.quote_plus(value),
            ):
                with self.subTest(encoded=encoded):
                    self.assertEqual(
                        b"[REDACTED]", guard.redact(encoded.encode(), guard.secret_forms(env))
                    )
        self.assertEqual(
            guard.redact(b'Authorization: Bearer unknown\n-Dpassword="a\nb"', []),
            b"Authorization: [REDACTED]\n-Dpassword=[REDACTED]",
        )

    def test_json_and_xml_escapes_preserve_literal_case(self):
        for secret, spellings in (
            ("A&B>C", (b"A&amp;B>C", b"A&#38;B&gt;C", b"A&amp;B&#62;C")),
            ("alpha/beta/gamma", (b"alpha\\/beta/gamma", b"alpha/beta\\/gamma", b"alpha\\/beta\\/gamma")),
            ("café", (rb"caf\u00e9", rb"caf\u00E9", b"caf&#233;", b"caf&#xe9;", b"caf&#xE9;", rb"caf\303\251")),
            ("Id字", (rb"Id\u5b57", rb"Id\u5B57", b"Id&#23383;", b"Id&#x5b57;", b"Id&#x5B57;")),
            ("Token💯", (rb"Token\ud83d\udcaf", rb"Token\uD83D\uDCAF",
                        rb"Token\ud83D\uDcAf", b"Token&#128175;", b"Token&#x1F4AF;",
                        b"Token&#x1f4Af;", rb"Token\360\237\222\257")),
        ):
            forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
            for spelling in spellings:
                with self.subTest(secret=secret, spelling=spelling):
                    self.assertEqual(guard.redact(spelling, forms), b"[REDACTED]")
                    different_case = spelling[:1].swapcase() + spelling[1:]
                    self.assertEqual(guard.redact(different_case, forms), different_case)
            different_case = secret.swapcase().encode()
            self.assertEqual(guard.redact(different_case, forms), different_case)

    def test_xml_and_json_escaped_credentials_in_both_streams(self):
        suffix = " café 💯 /path/leaf"
        secret = "xml&<tag>\"'secret" + suffix
        forms = (
            "xml&amp;&lt;tag&gt;\"'secret" + suffix,
            "xml&amp;&lt;tag&gt;&quot;&apos;secret" + suffix,
            "xml&amp;&lt;tag&gt;&quot;&#x27;secret" + suffix,
            "xml&amp;&lt;tag&gt;&#34;&#39;secret" + suffix,
            "xml&amp;&lt;tag&gt;&#x22;&#x27;secret" + suffix,
            "xml&amp;&lt;tag>\"'secret" + suffix,
            "xml&#38;&lt;tag>&quot;'secret" + suffix,
            json.dumps(secret)[1:-1].replace("00e9", "00E9").replace("d83d", "D83D").replace("dcaf", "DCAF"),
            json.dumps(secret)[1:-1].replace("/", r"\/", 1),
            "xml&amp;&lt;tag&gt;&quot;&apos;secret caf&#233; &#128175; /path/leaf",
            "xml&amp;&lt;tag&gt;&quot;&apos;secret caf&#xE9; &#x1F4AF; /path/leaf",
        )
        output = ("\n".join(forms) + "\n").encode()
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       f"import os; os.write(1, {output!r}); os.write(2, {output!r})"]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script], capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret),
            )
            self.assertEqual(result.returncode, 0)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b"[REDACTED]\n" * len(forms))
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_quoted_authorization_headers(self):
        for header in ("Authorization", "Proxy-Authorization"):
            for quote in ("", "'", '"'):
                with self.subTest(header=header, quote=quote):
                    data = f"{quote}{header}{quote}: {quote}Bearer unknown{quote}\n".encode()
                    self.assertNotIn(b"unknown", guard.redact(data, []))

    def test_quoted_credential_arguments(self):
        for quote in (b"'", b'"'):
            for option in (b"-Dpassword=", b"--define=token=", b"--define secret=",
                           b"-define=token=", b"-define secret="):
                with self.subTest(quote=quote, option=option):
                    result = guard.redact(quote + option + b"two words\nlast" + quote, [])
                    self.assertNotIn(b"words", result)
                    self.assertNotIn(b"last", result)
                    self.assertIn(b"[REDACTED]", result)
        self.assertEqual(guard.redact(b"'-Dpassword=first'-Dtoken=last", []),
                         b"'-Dpassword=[REDACTED]'-Dtoken=[REDACTED]")

    def test_escaped_quotes_in_credential_arguments(self):
        for quote in (b"'", b'"'):
            secret = b"alpha" + b"\\" + quote + b"beta gamma"
            for value in (
                b"-Dpassword=" + quote + secret + quote,
                quote + b"-Dpassword=" + secret + quote,
            ):
                with self.subTest(value=value):
                    result = guard.redact(value, [])
                    self.assertNotIn(b"beta", result)
                    self.assertNotIn(b"gamma", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_malformed_quoted_credentials_have_bounded_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       "import os; data=(bytes([92,34])+b'-Dpassword=')*20000; os.write(1,data); os.write(2,data); raise SystemExit(17)"]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({directory!r})))"
            )
            result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                    cwd=ROOT / "scripts/release", timeout=5)
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b'\\"-Dpassword=[REDACTED]')
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_secrets_matching_credential_markers(self):
        for marker, output in (
            ("password", b"-Ddb.password=unregistered-value"),
            ("Authorization", b"Authorization: Bearer unregistered-value"),
        ):
            with self.subTest(marker=marker):
                result = guard.redact(output, guard.secret_forms({"TOKEN": marker}))
                self.assertNotIn(b"unregistered-value", result)
                self.assertNotIn(marker.encode(), result)

    def test_overlapping_structured_and_literal_secrets(self):
        for secret in ("prefix-Authorization: suffix", "prefix-Dpassword=value tail"):
            with self.subTest(secret=secret):
                result = guard.redact(secret.encode(), guard.secret_forms({"TOKEN": secret}))
                self.assertEqual(result, b"[REDACTED]")
        for data, value, expected in (
            (b"ababa", b"aba", b"[REDACTED]"),
            (b"abacababacaba", b"abacaba", b"[REDACTED]"),
            (b"aaaaa", b"aaa", b"[REDACTED]"),
            (b"aaaaa", b"a", b"[REDACTED]"),
            (b"prefix abababab suffix", b"abab", b"prefix [REDACTED] suffix"),
            (b"prefix ALPHA%2fbeta suffix", b"alpha%2Fbeta", b"prefix [REDACTED] suffix"),
        ):
            with self.subTest(data=data, value=value):
                self.assertEqual(guard.redact(data, [value]), expected)

    def test_repetitive_literal_credentials_have_bounded_capture(self):
        for secret, expected in (("a" * 1023 + "b", b"a" * (8 * 1024 * 1024)),
                                 ("a" * 1024, b"[REDACTED]")):
            with self.subTest(dense_overlap=secret.endswith("a")), tempfile.TemporaryDirectory() as directory:
                command = [sys.executable, "-c",
                           "import os; data=b'a'*(8*1024*1024); os.write(1,data); os.write(2,data); raise SystemExit(17)"]
                script = (
                    "import guard,os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r},os.environ,guard.secret_forms(os.environ),Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5,
                                        env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret))
                self.assertEqual(result.returncode, 17)
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, expected)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_split_output_artifacts_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            # Use a real subprocess to verify both console streams and the resulting files.
            command = [
                sys.executable,
                "-c",
                "import os\nfor fd in (1, 2):\n"
                + ' for part in (b"diagnostic: ", b"dummy-", b"secret\\n"):\n'
                + "  os.write(fd, part)\nraise SystemExit(17)",
            ]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE="dummy-secret"),
            )
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, (Path(directory) / (name + ".log")).read_bytes())
                self.assertEqual(console, b"diagnostic: [REDACTED]\n")

    def test_capture_read_failure_preserves_completed_maven_status(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                process = unittest.mock.Mock()
                process.poll.return_value = status
                process.wait.return_value = status
                with patch.object(guard.subprocess, "Popen", return_value=process), \
                     patch.object(guard.selectors, "DefaultSelector", side_effect=OSError("secret")), \
                     patch.object(guard.os, "killpg"), patch.object(guard, "write_logs") as logs:
                    self.assertEqual(guard.capture(["mvn"], {}, [], Path(directory)), status)
                    logs.assert_called_once_with([guard.INCOMPLETE_LOG, guard.INCOMPLETE_LOG], Path(directory))

    def test_capture_failure_preserves_only_complete_sanitized_streams(self):
        for sanitization_fails in (False, True):
            with self.subTest(sanitization_fails=sanitization_fails), tempfile.TemporaryDirectory() as directory:
                command = [sys.executable, "-c",
                           "import os, time\nos.write(1, b'completed: dummy-secret\\n')\n"
                           + "os.close(1)\nos.write(2, b'unfinished: dummy-')\ntime.sleep(30)"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    "class FailingSelector(guard.selectors.DefaultSelector):\n"
                    " def unregister(self, fileobj):\n"
                    "  key = super().unregister(fileobj)\n"
                    "  if key.data == 0: raise OSError('synthetic capture failure')\n"
                    "  return key\n"
                    "guard.selectors.DefaultSelector = FailingSelector\n"
                    + ("guard.redact = lambda *_: (_ for _ in ()).throw(ValueError('secret'))\n"
                       if sanitization_fails else "")
                    + "try:\n"
                    f" guard.capture({command!r}, os.environ, [b'dummy-secret'], Path({directory!r}))\n"
                    "except OSError:\n raise SystemExit(23)\n"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5)
                self.assertEqual(result.returncode, 23)
                expected = ([guard.FAILED_LOG, guard.FAILED_LOG] if sanitization_fails else
                            [b"completed: [REDACTED]\n", guard.INCOMPLETE_LOG])
                for name, console, clean in zip(("stdout", "stderr"),
                                                (result.stdout, result.stderr), expected):
                    self.assertEqual(console, clean)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), clean)

    def test_posix_quoted_secrets_in_both_streams_and_artifacts(self):
        secret = "quote'and space \\\"$`tail"
        single_quoted = shlex.quote(secret)
        double_quoted = '"' + ''.join('\\' + c if c in '\\"$`' else c for c in secret) + '"'
        output = (single_quoted + "\n" + double_quoted + "\n").encode()
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                command = [sys.executable, "-c",
                           f"import os; os.write(1, {output!r}); os.write(2, {output!r}); raise SystemExit({status})"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, "
                    f"guard.secret_forms(os.environ), Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release",
                                        env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret), timeout=10)
                self.assertEqual(result.returncode, status)
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, b'[REDACTED]\n"[REDACTED]"\n')
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_bash_printf_quoted_secrets_in_console_and_artifacts(self):
        for secret, quoted_locales in (
            ("alpha beta", ()),
            ("café 💯 A&B>C", ("C",)),
            ("quote'\"$`\\;[]*?,^~#", ()),
            ("tab\tquote'\\value", ("C", "C.UTF-8")),
            ("\u0085", ("C", "C.UTF-8")),
            ("é\u0085", ("C", "C.UTF-8")),
        ):
            for locale, status in (("C", 0), ("C", 17), ("C.UTF-8", 0), ("C.UTF-8", 17)):
                with self.subTest(secret=secret, locale=locale, status=status), tempfile.TemporaryDirectory() as directory:
                    expected = b"argument: $'[REDACTED]'\n" if locale in quoted_locales else b"argument: [REDACTED]\n"
                    command = ["bash", "--noprofile", "--norc", "-c",
                               "printf 'argument: %q\\n' \"$MAVEN_GPG_PASSPHRASE\"; "
                               + "printf 'argument: %q\\n' \"$MAVEN_GPG_PASSPHRASE\" >&2; "
                               + "exit \"$SYNTHETIC_STATUS\""]
                    script = (
                        "import guard,os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r},os.environ,guard.secret_forms(os.environ),Path({directory!r})))"
                    )
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=10,
                                            env=dict(os.environ, LC_ALL=locale, MAVEN_GPG_PASSPHRASE=secret,
                                                     SYNTHETIC_STATUS=str(status)))
                    self.assertEqual(result.returncode, status)
                    for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                        self.assertEqual(console, expected)
                        self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_output_failures_preserve_maven_status(self):
        for status in (0, 17):
            for failure in ("directory", "artifact", "stdout", "stderr"):
                with self.subTest(status=status, failure=failure), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    if failure == "directory":
                        logs.write_text("blocked")
                    elif failure == "artifact":
                        logs.mkdir()
                        (logs / "stdout.log").mkdir()
                    command = [sys.executable, "-c",
                               f"import os; os.write(1, b'out\\n'); os.write(2, b'err\\n'); raise SystemExit({status})"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    read_fd, write_fd = os.pipe()
                    os.close(read_fd)
                    try:
                        result = subprocess.run(
                            [sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                            stdout=write_fd if failure == "stdout" else subprocess.PIPE,
                            stderr=write_fd if failure == "stderr" else subprocess.PIPE,
                        )
                    finally:
                        os.close(write_fd)
                    self.assertEqual(result.returncode, status)
                    if failure != "stdout":
                        self.assertEqual(result.stdout, b"out\n")
                    if failure != "stderr":
                        self.assertIn(b"err\n", result.stderr)
                        self.assertIn(b"values withheld", result.stderr)
                    if failure in ("stdout", "stderr"):
                        self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if failure != "directory":
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_sanitization_failure_writes_fixed_logs(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    "guard.redact = lambda *_: (_ for _ in ()).throw(ValueError('secret'))\n"
                    f"raise SystemExit(guard.capture([{sys.executable!r}, '-c', "
                    f"'print(\"secret\"); raise SystemExit({status})'], os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release")
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, guard.FAILED_LOG)
                self.assertEqual(result.stderr, guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stdout.log").read_bytes(), guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stderr.log").read_bytes(), guard.FAILED_LOG)

    def test_overflow_drains_without_interrupting_maven(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                marker = Path(directory) / "completed"
                command = [sys.executable, "-c",
                           "import os\nfor fd in (1, 2):\n"
                           " os.write(fd, b'-Dpassword=\"first\\nsecret tail' * 100)\n"
                           f"open({str(marker)!r}, 'w').write('done')\nraise SystemExit({status})"]
                script = (
                    "import guard, os\nfrom pathlib import Path\nguard.LIMIT = 64\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5)
                self.assertEqual(result.returncode, status)
                self.assertEqual(marker.read_text(), "done")
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, guard.OVERFLOW_LOG)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_interruption_during_log_writes_does_not_replay_output(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            for partial in (False, True):
                with self.subTest(signum=signum, partial=partial), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    calls = Path(directory) / "writes"
                    command = [sys.executable, "-c",
                               "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n')"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        "original = guard.write_logs\n"
                        "def interrupted_write(clean, logs):\n"
                        f" with Path({str(calls)!r}).open('a') as out: out.write('called\\n')\n"
                        + (" logs.mkdir(parents=True, exist_ok=True)\n"
                           " with (logs / 'stdout.log').open('ab') as out: out.write(clean[0])\n"
                           if partial else " original(clean, logs)\n")
                        + f" guard.interrupted({int(signum)}, None)\n"
                        "guard.write_logs = interrupted_write\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertEqual(result.returncode, 128 + signum)
                    self.assertEqual(calls.read_text(), "called\n")
                    self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if partial:
                        self.assertFalse((logs / "stderr.log").exists())
                    else:
                        self.assertEqual(result.stdout, b"out\n")
                        self.assertEqual(result.stderr, b"err\n")
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process observation required")
    def test_inherited_pipes_have_bounded_safe_capture(self):
        for complete_stderr, complete_stdout, status in ((False, False, 17), (True, False, 17), (True, True, 0)):
            with self.subTest(complete_stderr=complete_stderr, complete_stdout=complete_stdout), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "child"
                command = [sys.executable, "-c",
                           "import os, signal, time\nchild = os.fork()\nif child == 0:\n"
                           + " signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                           + (" os.close(2)\n" if complete_stderr else "")
                           + (" os.close(1)\n" if complete_stdout else " os.write(1, b'known-secret-prefix')\n")
                           + " time.sleep(30)\n os._exit(0)\n"
                           + f"open({str(pid_file)!r}, 'w').write(str(child))\n"
                           + ("os.write(2, b'-Dpassword=\"first\\nunknown secret tail\"')\n"
                              if complete_stderr else "os.write(2, b'-Dpassword=\"first\\nunknown secret tail')\n")
                           + f"raise SystemExit({status})"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [b'known-secret-prefix-tail'], Path({directory!r})))"
                )
                started = time.monotonic()
                try:
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertLess(time.monotonic() - started, 4)
                    self.assertEqual(result.returncode, status)
                    self.assertEqual(result.stdout, b"" if complete_stdout else guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stdout.log").read_bytes(), result.stdout)
                    if complete_stderr:
                        self.assertEqual(result.stderr, b'-Dpassword=[REDACTED]')
                    else:
                        self.assertEqual(result.stderr, guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stderr.log").read_bytes(), result.stderr)
                    pid = int(pid_file.read_text())
                    deadline = time.monotonic() + 1
                    while running(pid):
                        self.assertLess(time.monotonic(), deadline, "Maven descendant survived capture")
                        time.sleep(0.01)
                finally:
                    if pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            # The fixture child has already exited; cleanup is complete.
                            pass
