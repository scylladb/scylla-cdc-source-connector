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
    def test_default_logs_stay_in_checkout_and_explicit_directory_is_preserved(self):
        for override in (False, True):
            with self.subTest(override=override), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                checkout = root / "checkout"
                checkout.mkdir()
                shared = root / "untrusted"
                shared.mkdir()
                victim = root / "victim"
                victim.write_bytes(b"original\n")
                (shared / "stdout.log").symlink_to(victim)
                logs = root / "chosen-logs" if override else checkout / "target/release-logs"
                env = {"TEST_SHARED_LOG_DIR": str(shared)}
                if override:
                    env["RELEASE_LOG_DIR"] = str(logs)
                command = [sys.executable, "-c",
                           "import sys; print('local stdout'); print('local stderr', file=sys.stderr)"]
                script = (
                    "import os, sys\nfrom pathlib import Path\n"
                    f"sys.path.insert(0, {str(ROOT / 'scripts/release')!r})\nimport guard\n"
                    "original_capture = guard.capture\n"
                    "def capture(command, env, forms, logs):\n"
                    # Model an attacker-owned default without writing to the host's /tmp.
                    "    if 'RELEASE_LOG_DIR' not in env and not logs.resolve().is_relative_to(Path.cwd()):\n"
                    "        logs = Path(os.environ['TEST_SHARED_LOG_DIR'])\n"
                    "    return original_capture(command, env, forms, logs)\n"
                    "guard.capture = capture\n"
                    f"raise SystemExit(guard.run_maven({command!r}, dict(os.environ)))\n"
                )
                result = subprocess.run([sys.executable, "-c", script], cwd=checkout, env=env,
                                        capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(victim.read_bytes(), b"original\n")
                self.assertEqual((logs / "stdout.log").read_bytes(), b"local stdout\n")
                self.assertEqual((logs / "stderr.log").read_bytes(), b"local stderr\n")

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
                cwd=ROOT / "scripts/release", timeout=30,
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret),
            )
            self.assertEqual(result.returncode, 0)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b"[REDACTED]\n" * len(forms))
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_escaped_regions_preserve_full_and_overlapping_byte_spans(self):
        for value, encoded in (
            (b"aba", b"%61b%61b%61"),
            (b"aabbaa", b"%61a&#" + b"0" * 10000 + b"98;b%61a"),
            (b"aabbaa", b"a&#x" + b"0" * 10000 + b"61;bb&#97;a"),
            ("aé💯/z".encode(), rb"a%c3\xa9&#x1f4Af;\/z"),
            (rb"a\ud83d\udcafz", rb"a\uD83D\uDcAfz"),
        ):
            with self.subTest(value=value):
                suffix = b" preserved\xfe" * 40
                for prefix in (b"unchanged\xff " * 40, b"%20." * 129,
                               b" ".join(f"&#{n};".encode() for n in range(129)) + b" "):
                    self.assertEqual(guard.redact(prefix + encoded + suffix, [value]),
                                     prefix + b"[REDACTED]" + suffix)

    def test_escape_vocabulary_preserves_all_byte_and_codepoint_encodings(self):
        for byte in range(256):
            value = b"[" + bytes([byte]) + b"]"
            encodings = [f"%{byte:02X}", f"&#000{byte};", f"&#x000{byte:X};",
                         rf"\x{byte:02X}", rf"\u00{byte:02X}"]
            if byte < 32 or byte >= 127:
                encodings.append(rf"\{byte:03o}")
            for encoded in encodings:
                with self.subTest(byte=byte, encoded=encoded):
                    self.assertEqual(guard.redact(b"[" + encoded.encode() + b"]", [value]),
                                     b"[REDACTED]")
        for value, encoded in (
            ("é", "&#000233;"), ("é", "&#x000E9;"),
            ("💯", "&#000128175;"), ("💯", "&#x0001F4AF;"),
            (r"\ud83d\udcaf", r"\uD83D\uDcAf"),
            ("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"),
            ('"', "&quot;"), ("'", "&apos;"), ("/", r"\/"),
        ):
            with self.subTest(value=value, encoded=encoded):
                self.assertEqual(guard.redact(b"[" + encoded.encode() + b"]",
                                             [b"[" + value.encode() + b"]"]), b"[REDACTED]")

    def test_unrelated_escapes_have_bounded_sanitization(self):
        script = (
            "import guard\n"
            "forms=guard.secret_forms({'TOKEN': 'syntheticé-pass&\"quoted\\\''})\n"
            "for data in ((b'ordinary build diagnostic line\\n'*100000)[:2*1024*1024]+b'%20\\n',\n"
            "             (b'ordinary build diagnostic line %20\\n'*100000)[:2*1024*1024]):\n"
            "    assert guard.redact(data,forms)==data\n"
            "for marker in (b'&#', b'&#x'):\n"
            "    data=marker+b'0'*(1024*1024)+b'invalid'\n"
            "    assert guard.redact(data,forms)==data\n"
            "for secret in ('a'*1023+'b', 'a'*1024):\n"
            "    data=b'a'*(8*1024*1024)+b'%20'\n"
            "    expected=data if secret.endswith('b') else b'[REDACTED]%20'\n"
            "    assert guard.redact(data,guard.secret_forms({'TOKEN':secret}))==expected\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_repeated_unrelated_escapes_do_not_expand_near_match_searches(self):
        script = (
            "import guard\n"
            "forms=guard.secret_forms({'TOKEN': 'a'*1023+'b'})\n"
            "for escape in (b'%20', b'&#32;', b'&#x20;', br'\\x20', br'\\u0020', b'&amp;'):\n"
            "    data=escape+(b'a'*2048+escape)*1024\n"
            "    assert guard.redact(data,forms)==data\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_relevant_escaped_near_matches_have_bounded_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       "import os; data=(b'%61'+b'a'*2048)*4096; " +
                       "os.write(1,data); os.write(2,data); raise SystemExit(17)"]
            script = (
                "import guard,os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r},os.environ,"
                f"guard.secret_forms(os.environ),Path({directory!r})))"
            )
            result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                    cwd=ROOT / "scripts/release", timeout=10,
                                    env=dict(os.environ, MAVEN_GPG_PASSPHRASE="a" * 1023 + "b"))
            self.assertEqual(result.returncode, 17, result.stderr[:1000])
            expected = (b"%61" + b"a" * 2048) * 4096
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, expected)
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), expected)

    def test_encoded_near_matches_do_not_hide_the_rare_credential_token(self):
        script = (
            "import guard\n"
            "data=b'b'+b'%61'*(8*1024*1024//3)\n"
            "forms=guard.secret_forms({'TOKEN':'a'*1023+'b'})\n"
            "assert guard.redact(data,forms)==data\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_dense_multi_token_near_matches_have_bounded_work(self):
        script = (
            "import guard\n"
            "data=(b'%61'+b'a'*511+b'b'*511+b'.')*8192\n"
            "assert guard.redact(data,[b'a'*512+b'b'*512])==data\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_dense_irrelevant_derived_forms_use_shared_escape_counts(self):
        script = (
            "import guard\n"
            "forms=guard.secret_forms({'TOKEN':'''A&B'\"/ +=é'''})\n"
            "data=b'%20%2F'*(8*1024*1024//6)\n"
            "assert guard.redact(data,forms)==data\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mixed_byte_and_codepoint_paths_keep_the_earliest_source_start(self):
        # The numeric escape can finish a UTF-8 byte sequence or encode the whole
        # code point. Both paths reach the same state with different source starts.
        value = "\u0080".encode() + b"a" * 64
        data = b"prefix!\xc2&#128;" + b"a" * 64
        self.assertEqual(guard.redact(data, [value]), b"prefix![REDACTED]")

    def test_matching_forms_release_their_state_before_the_next_form(self):
        script = (
            "import guard,tracemalloc\n"
            "forms=[('é'*511+chr(256+i)).encode() for i in range(16)]\n"
            "data=b'|'.join(b'&#233;'*511+('&#%d;'%(256+i)).encode() for i in range(16))\n"
            "tracemalloc.start()\n"
            "assert guard.redact(data,list(reversed(forms)))==b'|'.join([b'[REDACTED]']*16)\n"
            "assert tracemalloc.get_traced_memory()[1]<8*1024*1024\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=45)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_repeated_token_near_matches_are_checked_once_per_run(self):
        script = (
            "import guard\n"
            "cases=[(b'a'*1024,(b'%61'+b'a'*1022+b'.')*8192),\n"
            "       (('é'*512).encode(),(b'&#233;'*511+b'.')*1024),\n"
            "       (br'\\u0061'*170,(br'\\u0061'*169+b'.')*4096)]\n"
            "for value,data in cases:\n"
            "    assert guard.redact(data,[value])==data\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_literal_periodic_matches_have_bounded_regex_storage(self):
        script = (
            "import guard,resource,tracemalloc\n"
            "resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))\n"
            "for period in (b'ab',b'abc'):\n"
            "    data=period*(32*1024*1024//len(period))\n"
            "    value=period*(1024//len(period))\n"
            "    tracemalloc.start()\n"
            "    assert guard.redact(data,[value])==b'[REDACTED]'\n"
            "    assert tracemalloc.get_traced_memory()[1]<16*1024*1024\n"
            "    tracemalloc.stop()\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_long_encoded_credentials_preserve_complete_overlapping_matches(self):
        for value, data in (
            (b"1" * 65, b"%31" + b"1" * 130),
            (b"1" * 64 + b"b", b"%31" + b"1" * 63 + b"b"),
            (b"\\" * 65, br"\x5c" * 130),
            (b"a" + b"\\" * 64, b"a" + br"\u005c" * 64),
            (b"ababa" * 16, b"%61b%61b%61" * 32),
            (("é1" * 40).encode(), b"&#233;%31" * 80),
            (br"\u00e9" * 12, br"\u00E9" * 24),
            (b"0" * 65, b"&#" + b"0" * 100000 + b"48;" + b"%30" * 65),
            (b"a" * 65, b"&#" + b"0" * 100000 + b"97;" + b"a" * 64),
        ):
            with self.subTest(value=value[:20]):
                self.assertEqual(guard.redact(b"<" + data + b">", [value]), b"<[REDACTED]>")
        value = br"\u00e9" * 12
        data = br"\U00E9" * 24
        self.assertEqual(guard.redact(data, [value]), data)
        self.assertEqual(guard.redact(b"%33" * 64, [b"3" * 65]), b"%[REDACTED]")

    def test_dense_long_escaped_matches_coalesce_without_per_offset_retries(self):
        script = (
            "import guard,tracemalloc\n"
            "data=b'%61'*(2*1024*1024//3)\n"
            "tracemalloc.start()\n"
            "assert guard.redact(data,[b'a'*1024])==b'[REDACTED]'\n"
            "assert tracemalloc.get_traced_memory()[1] < 8*len(data)\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_sparse_escape_windows_match_with_bounded_memory(self):
        script = (
            "import guard, tracemalloc\n"
            # Exactly 128 escapes retain sparse mode. A complete encoded secret
            # forces matching; the other windows remain disjoint near-misses.
            "data=(b'%61'+b'.'*(65536-3))*128\n"
            "data=b'%61bc12345'+data[10:]\n"
            "expected=b'[REDACTED]'+data[10:]\n"
            "tracemalloc.start()\n"
            "assert guard.redact(data,[b'abc12345'])==expected\n"
            "_, peak=tracemalloc.get_traced_memory()\n"
            # Allow input-sized output buffers, but no tuple per source window.
            "assert peak < 3*len(data), peak\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                cwd=ROOT / "scripts/release", timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

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
                cwd=ROOT / "scripts/release", timeout=30,
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
                           "import os\nos.write(1, b'completed: dummy-secret\\n')\n"
                           + "os.close(1)\nos.write(2, b'unfinished: dummy-')\n"
                           + "os.read(int(os.environ['FIXTURE_LIFETIME_FD']), 1)"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    # Only the wrapper owns the writer. Even a deadline SIGKILL
                    # closes it, so the separate-session child gets EOF and exits.
                    "reader, writer = os.pipe()\n"
                    "os.environ['FIXTURE_LIFETIME_FD'] = str(reader)\n"
                    "real_popen = guard.subprocess.Popen\n"
                    "def launch(*args, **kwargs):\n"
                    " kwargs['pass_fds'] = (*kwargs.get('pass_fds', ()), reader)\n"
                    " return real_popen(*args, **kwargs)\n"
                    "guard.subprocess.Popen = launch\n"
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
                    "finally:\n os.close(writer)\n os.close(reader)\n"
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
                            timeout=30,
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
                                        cwd=ROOT / "scripts/release", timeout=30)
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
