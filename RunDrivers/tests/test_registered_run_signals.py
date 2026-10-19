# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A signal to a registered run is recorded `killed`, and nothing it then does can say otherwise
(rrk-adoption prompt 03, W4, W5 and W6 test 4).

The subjects are `make_killed_handler`, `install_killed_handler` and `finish_after_exec` in
`RunDrivers/registered_run.py`, SecondaryGWKit's design at `031bcb4`. The handler is called
directly, with a stand-in run or a real `Run` in a temporary runs root; `ray.shutdown` and
`signal.alarm` are stand-ins. **One test runs a subprocess**: the wrapper with Ray stubbed
(`python -u -m RunDrivers.tests.stubbed_run`) on a stand-in that waits, sent a real `SIGTERM` once
its banner is on stdout. The child is reaped by a cleanup registered before its first read. No Ray
is initialised, no store is opened, and nothing under `var/` is read or written.
"""

import io
import os
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path

import ray
import runregistrykit

from RunDrivers import registered_run
from RunDrivers.tests.stubbed_run import (
    REPO_ROOT,
    TITLE,
    Recorder,
    only_run,
    read_json,
    reap,
    standin_program,
    stubbed,
    subprocess_env,
    wrapper_argv,
)

SIGTERM = int(signal.SIGTERM)
SIGINT = int(signal.SIGINT)

FINGERPRINT_KEYS = ("fingerprint", "fingerprint_error", "fingerprint_sidecar")


class StandInRun:
    """Records every `finish` call as `(state, exit_code, fingerprint)`."""

    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail

    def finish(self, state, exit_code=None, *, fingerprint=False):
        if self.fail is not None:
            raise self.fail
        self.calls.append((state, exit_code, fingerprint))
        return {"state": state, "exit_code": exit_code}


class SignalCase(unittest.TestCase):
    def setUp(self):
        self.latch = registered_run.SignalLatch()
        self.out = io.StringIO()
        tmp = tempfile.TemporaryDirectory(prefix="champbh-registered-run-")
        self.addCleanup(tmp.cleanup)
        self.top = Path(tmp.name).resolve()
        self.runs_root = self.top / "runs"
        self.runs_root.mkdir()
        self.database = self.top / "store.db"  # never created

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    def handler(self, run):
        return registered_run.make_killed_handler(run, self.latch, stream=self.out)

    def a_real_run(self):
        return runregistrykit.begin(
            campaign="rrk-adoption",
            prompt="03",
            slug="killed-handler",
            purpose="a unit test of the killed handler, in a temporary directory",
            argv=["unittest"],
            results=str(self.database),
            root=str(self.runs_root),
            client_root=str(REPO_ROOT),
        )

    def fired(self, run, signum=SIGTERM):
        """The handler, fired as a signal during the program would fire it."""
        with self.assertRaises(SystemExit) as raised:
            self.handler(run)(signum, None)
        return raised.exception

    def after(self, run, exc, shutdown=None, alarm=None):
        """`finish_after_exec` with stand-ins: `(the SystemExit it raised or None, shutdown,
        alarm)`."""
        shutdown = Recorder() if shutdown is None else shutdown
        alarm = Recorder(0) if alarm is None else alarm
        try:
            registered_run.finish_after_exec(
                run, exc, self.latch, shutdown, alarm=alarm, stream=self.out
            )
        except SystemExit as e:
            return e, shutdown, alarm
        return None, shutdown, alarm


class TestTheHandler(SignalCase):
    def test_on_a_real_run_it_records_killed_143_at_once_without_a_fingerprint(self):
        run = self.a_real_run()
        raised = self.fired(run)
        self.assertEqual(raised.code, 143)
        status = read_json(run.status_path)
        self.assertEqual((status["state"], status["exit_code"]), ("killed", 143))
        for key in FINGERPRINT_KEYS:
            self.assertNotIn(key, status)
        self.assertEqual(self.latch.signum, SIGTERM)
        lines = self.out.getvalue().splitlines()
        self.assertEqual(len(lines), 1, lines)
        self.assertIn("SIGTERM", lines[0])
        self.assertIn("recorded killed, exit code 143", lines[0])

    def test_a_second_signal_is_ignored_with_a_line(self):
        run = StandInRun()
        handler = self.handler(run)
        with self.assertRaises(SystemExit):
            handler(SIGTERM, None)
        self.assertIsNone(handler(SIGTERM, None))
        self.assertIsNone(handler(SIGINT, None))
        self.assertEqual(run.calls, [("killed", 143, False)])
        self.assertEqual(self.latch.signum, SIGTERM)
        lines = self.out.getvalue().splitlines()
        self.assertEqual(len(lines), 3, lines)
        self.assertIn("ignored", lines[1])
        self.assertIn("SIGINT", lines[2])
        self.assertIn("ignored", lines[2])

    def test_sigint_gives_130(self):
        run = StandInRun()
        self.assertEqual(self.fired(run, SIGINT).code, 130)
        self.assertEqual(run.calls, [("killed", 130, False)])

    def test_a_write_that_fails_does_not_stop_the_raise(self):
        run = StandInRun(fail=OSError("no space left on device"))
        self.assertEqual(self.fired(run).code, 143)
        self.assertEqual(self.latch.signum, SIGTERM)
        self.assertIn("could not record killed", self.out.getvalue())

    def test_once_the_outcome_is_decided_a_signal_is_only_noted(self):
        run = StandInRun()
        self.assertIsNone(self.latch.decide())
        self.assertIsNone(self.handler(run)(SIGTERM, None))
        self.assertEqual(run.calls, [])
        self.assertIsNone(self.latch.signum)
        self.assertIn("ignored", self.out.getvalue())


class TestInstall(SignalCase):
    def setUp(self):
        super().setUp()
        saved = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}

        def restore():
            for number, handler in saved.items():
                signal.signal(number, handler)

        self.addCleanup(restore)

    def test_it_takes_sigterm_and_sigint(self):
        handler = registered_run.install_killed_handler(
            None, self.latch, stream=self.out
        )
        self.assertIs(signal.getsignal(signal.SIGTERM), handler)
        self.assertIs(signal.getsignal(signal.SIGINT), handler)


class TestAfterASignal(SignalCase):
    """The handler fired during the program: `killed`, whatever then reaches the driver, with
    the fingerprint added."""

    WHAT_REACHES_THE_DRIVER = (
        ("SystemExit(1), out of ray.wait", SystemExit(1)),
        ("SystemExit(143)", SystemExit(143)),
        ("SystemExit(None)", SystemExit(None)),
        ("KeyboardInterrupt", KeyboardInterrupt()),
        ("RuntimeError", RuntimeError("interrupted")),
        ("no exception: the program returned", None),
    )

    def test_it_is_killed_143_with_the_fingerprint_whatever_reaches_the_driver(self):
        for label, exc in self.WHAT_REACHES_THE_DRIVER:
            with self.subTest(label):
                self.latch = registered_run.SignalLatch()
                run = StandInRun()
                self.fired(run)
                raised, shutdown, alarm = self.after(run, exc)
                self.assertEqual(
                    run.calls, [("killed", 143, False), ("killed", 143, True)]
                )
                self.assertIsNotNone(raised, "it did not exit")
                self.assertEqual(raised.code, 143)
                self.assertEqual(alarm.calls, [((0,), {})])
                self.assertEqual(len(shutdown.calls), 1)

    def test_on_a_real_run_the_fingerprint_is_taken_and_the_state_stays_killed(self):
        run = self.a_real_run()
        self.fired(run)
        raised, _, _ = self.after(run, SystemExit(1))
        self.assertEqual(raised.code, 143)
        status = read_json(run.status_path)
        self.assertEqual((status["state"], status["exit_code"]), ("killed", 143))
        # the fingerprint was taken: the store does not exist, and the attempt is recorded
        self.assertIn("does not exist", status["fingerprint_error"])

    def test_the_alarm_is_cancelled_first(self):
        order = []
        run = StandInRun()
        self.fired(run)

        def alarm(seconds):
            order.append(("alarm", seconds))
            return 3

        raised, _, _ = self.after(
            run, SystemExit(1), shutdown=lambda: order.append("shutdown"), alarm=alarm
        )
        self.assertEqual(raised.code, 143)
        self.assertEqual(order, [("alarm", 0), "shutdown"])
        self.assertIn("cancelled an alarm with 3 s left", self.out.getvalue())


class TestWithNoSignal(SignalCase):
    """No signal: the program's own outcome, each with the fingerprint, then Ray shut down, and a
    return so that the caller re-raises."""

    OUTCOMES = (
        ("returned", None, ("done", 0)),
        ("SystemExit(None)", SystemExit(None), ("done", None)),
        ("SystemExit(0)", SystemExit(0), ("done", 0)),
        ("SystemExit(1)", SystemExit(1), ("failed", 1)),
        ("SystemExit(2)", SystemExit(2), ("failed", 2)),
        ("SystemExit(15), Ray's own handler", SystemExit(15), ("killed", 15)),
        ("RuntimeError", RuntimeError("a real failure"), ("failed", 1)),
        ("KeyboardInterrupt", KeyboardInterrupt(), ("failed", 1)),
    )

    def test_every_outcome_is_recorded_with_the_fingerprint(self):
        for label, exc, expected in self.OUTCOMES:
            with self.subTest(label):
                self.latch = registered_run.SignalLatch()
                run = StandInRun()
                raised, shutdown, alarm = self.after(run, exc)
                self.assertIsNone(raised)
                self.assertEqual(run.calls, [expected + (True,)])
                self.assertEqual(len(shutdown.calls), 1)
                self.assertEqual(alarm.calls, [((0,), {})])
                self.assertTrue(self.latch.decided)

    def test_a_shutdown_that_fails_does_not_replace_the_exception(self):
        def failing():
            raise RuntimeError("the raylet is gone")

        raised, _, _ = self.after(StandInRun(), SystemExit(1), shutdown=failing)
        self.assertIsNone(raised)
        self.assertIn("ray.shutdown() raised", self.out.getvalue())


class TestARealSigterm(SignalCase):
    """The wrapper with Ray stubbed, on a stand-in that waits, sent a real `SIGTERM` once its
    banner is on stdout. The stand-in `ray.init` installs a Ray-like handler, which the wrapper's
    must replace."""

    def test_it_is_killed_143_and_exits_143_with_the_marker_last(self):
        program = standin_program(self.top, "wait", self.runs_root)
        child = subprocess.Popen(
            stubbed(
                self.runs_root, program, wrapper_argv(self.database, slug="sigterm")
            ),
            cwd=str(REPO_ROOT),
            env=subprocess_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.addCleanup(reap, child)
        seen = []
        while True:
            line = child.stdout.readline()
            if not line:
                child.kill()
                self.fail(f"no banner: {''.join(seen)}{child.stderr.read()}")
            seen.append(line)
            if line.rstrip("\n") == f"** {TITLE}":
                break
        os.kill(child.pid, signal.SIGTERM)
        out, err = child.communicate(timeout=120)
        self.assertEqual(child.returncode, 143, err)
        stdout = "".join(seen) + out
        self.assertEqual(stdout.splitlines()[-1], "** driver exit status 143")
        run = only_run(self.runs_root)
        self.assertEqual(
            (run / "stdout.log").read_text().splitlines()[-1],
            "** driver exit status 143",
        )
        status = read_json(run / "status.json")
        self.assertEqual((status["state"], status["exit_code"]), ("killed", 143))
        self.assertIn("fingerprint_error", status)  # taken after the program unwound
        self.assertIn("SIGTERM", err)
        self.assertIn("recorded killed, exit code 143", err)
        self.assertNotIn("Traceback", err)


if __name__ == "__main__":
    unittest.main()
