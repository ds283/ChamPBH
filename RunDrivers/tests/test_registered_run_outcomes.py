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

"""How a registered run ends, and what the wrapper refuses (rrk-adoption prompt 03, W2, W5 and W6
tests 3 and 6).

These need the script's own entry, `run`: the marker, the exit status, the refusal print and the
traceback exist only there (the orchestrator's note for 03, ruling 8). So:

- **the outcomes** and **`begin`'s refusal** run the wrapper with Ray stubbed, `python -u -m
  RunDrivers.tests.stubbed_run`, on a stand-in program, into a temporary runs root;
- **the refusals before `begin`**, and `--help`, run the real script, `python
  RunDrivers/registered_run.py`, with no stub and no `PYTHONPATH`: they reach neither `begin` nor
  Ray. `var/` is not created by them, and where it already exists (it is the user's once a first
  registered run is made) the listing of `var/runs` is unchanged.

Every child is reaped by a cleanup registered before its first read. The dead run of `begin`'s
refusal is made by a real child that begins a run and is sent `SIGKILL`, as SecondaryGWKit's
`test_hard_killed_predecessor.py` builds one. No Ray is initialised; no store is created.
"""

import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import ray

from RunDrivers.tests.stubbed_run import (
    ERROR_TEXT,
    MID_LINE,
    REPO_ROOT,
    TITLE,
    only_run,
    read_json,
    reap,
    run_stubbed,
    run_the_script,
    standin_program,
    subprocess_env,
    var_state,
    wrapper_argv,
)

MARKER = "** driver exit status"

# a child that begins a run on the store, heartbeats once, says where its run is, and waits to be
# killed (SecondaryGWKit's test_hard_killed_predecessor.CHILD)
CHILD = """\
import sys, time

import runregistrykit

run = runregistrykit.begin(
    campaign="example",
    prompt="01",
    slug="hard-killed",
    purpose="a run that is killed outright",
    root=sys.argv[1],
    client_root=sys.argv[2],
    argv=["a-child"],
    results=sys.argv[3],
)
run.heartbeat(units_done=1, stage="working")
print(run.path, flush=True)
time.sleep(600)
"""


class SubprocessCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-registered-run-")
        self.addCleanup(tmp.cleanup)
        self.top = Path(tmp.name).resolve()
        self.runs_root = self.top / "runs"
        self.runs_root.mkdir()
        self.database = self.top / "store.db"  # never created

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    def assertMarkerLast(self, stdout, status):
        lines = stdout.splitlines()
        self.assertTrue(lines, "nothing on stdout")
        self.assertEqual(lines[-1], f"{MARKER} {status}")
        self.assertEqual(
            sum(line.startswith(MARKER) for line in lines), 1, "the marker twice"
        )

    def assertOneRefusalLine(self, done, text=None):
        self.assertEqual(done.returncode, 2, done.stdout + done.stderr)
        self.assertNotIn("Traceback", done.stderr)
        lines = done.stderr.splitlines()
        self.assertEqual(len(lines), 1, done.stderr)
        self.assertTrue(lines[0].startswith("!! "), done.stderr)
        self.assertFalse(lines[0].startswith("!! !!"), done.stderr)
        if text is not None:
            self.assertIn(text, lines[0])
        self.assertMarkerLast(done.stdout, 2)


class TestTheOutcomes(SubprocessCase):
    """A return, `sys.exit(2)`, `sys.exit(1)` and a `RuntimeError`: the state and code recorded,
    the wrapper's own exit status, and the marker last on stdout and in `stdout.log`."""

    OUTCOMES = (
        ("return", "done", 0),
        ("exit-2", "failed", 2),
        ("exit-1", "failed", 1),
        ("raise", "failed", 1),
    )

    def test_each_outcome(self):
        for behaviour, state, code in self.OUTCOMES:
            with self.subTest(behaviour):
                runs_root = self.top / f"runs-{behaviour}"
                runs_root.mkdir()
                program = standin_program(self.top, behaviour, runs_root)
                done = run_stubbed(
                    runs_root, program, wrapper_argv(self.database, slug=behaviour)
                )
                self.assertEqual(done.returncode, code, done.stdout + done.stderr)
                run = only_run(runs_root)
                status = read_json(run / "status.json")
                self.assertEqual((status["state"], status["exit_code"]), (state, code))
                # the marker is never the stage
                self.assertEqual(status["stage"], f"{TITLE}, all work items complete")
                self.assertMarkerLast(done.stdout, code)
                self.assertMarkerLast((run / "stdout.log").read_text(), code)
                stderr_log = (run / "stderr.log").read_text()
                if behaviour == "raise":
                    self.assertIn("Traceback", stderr_log)
                    self.assertIn(f"RuntimeError: {ERROR_TEXT}", stderr_log)
                    self.assertIn("Traceback", done.stderr)
                else:
                    self.assertNotIn("Traceback", done.stderr)
                self.assertFalse(self.database.exists())


class TestTheMarkerIsALineOfItsOwn(SubprocessCase):
    """A program whose last write ends without a newline, as a signal handler raising between
    `print`'s text and its newline leaves it: the marker still starts a line of its own, so that
    `interrupt_when.py`'s `startswith` finds it."""

    def test_after_an_unfinished_line(self):
        program = standin_program(self.top, "mid-line", self.runs_root)
        done = run_stubbed(
            self.runs_root, program, wrapper_argv(self.database, slug="mid-line")
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        for text in (
            done.stdout,
            (only_run(self.runs_root) / "stdout.log").read_text(),
        ):
            self.assertEqual(text.splitlines()[-2], MID_LINE)
            self.assertMarkerLast(text, 0)


class TestBeginsRefusal(SubprocessCase):
    """A store that an unfinished run in the same runs root names is refused by `begin`: exit 2,
    one `!! ` line naming `python -m runregistrykit end`, and nothing created. Once that run is
    ended, the same registration is accepted."""

    def a_hard_killed_run(self):
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                CHILD,
                str(self.runs_root),
                str(REPO_ROOT),
                str(self.database),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(REPO_ROOT),
            env=subprocess_env(),
        )
        self.addCleanup(reap, child)
        path = child.stdout.readline().strip()
        if not path:
            child.kill()
            child.wait(timeout=60)
            self.fail(f"the child began no run: {child.stderr.read()}")
        child.kill()
        self.assertEqual(child.wait(timeout=60), -signal.SIGKILL)
        self.assertEqual(read_json(Path(path) / "status.json")["state"], "running")
        return Path(path)

    def end(self, run_id):
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "runregistrykit",
                "end",
                run_id,
                "--reason",
                "a dead child in a test; ended by the test",
                "--root",
                str(self.runs_root),
            ],
            cwd=str(REPO_ROOT),
            env=subprocess_env(),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def test_refused_then_accepted_after_end(self):
        dead = self.a_hard_killed_run()
        listing = sorted(os.listdir(self.runs_root))
        program = standin_program(self.top, "return", self.runs_root)
        argv = wrapper_argv(self.database, slug="resume")

        refused = run_stubbed(self.runs_root, program, argv)
        self.assertOneRefusalLine(refused, "python -m runregistrykit end")
        self.assertIn(dead.name, refused.stderr)
        self.assertTrue(
            refused.stderr.rstrip().endswith("No run directory was created"),
            refused.stderr,
        )
        self.assertEqual(sorted(os.listdir(self.runs_root)), listing)
        self.assertFalse(program.with_suffix(".json").exists(), "the program ran")

        ended = self.end(dead.name)
        self.assertEqual(ended.returncode, 0, ended.stdout + ended.stderr)
        self.assertEqual(read_json(dead / "status.json")["state"], "lost")

        accepted = run_stubbed(self.runs_root, program, argv)
        self.assertEqual(accepted.returncode, 0, accepted.stdout + accepted.stderr)
        made = set(os.listdir(self.runs_root)) - set(listing)
        self.assertEqual(len(made), 1)
        status = read_json(self.runs_root / made.pop() / "status.json")
        self.assertEqual((status["state"], status["exit_code"]), ("done", 0))
        self.assertMarkerLast(accepted.stdout, 0)


class TestTheRefusalsBeforeBegin(SubprocessCase):
    """The real script, no stub: each refusal is exit 2, one `!! ` line on stderr, no traceback,
    the marker last on stdout, and `var/` as it was."""

    def refuse(self, argv, text):
        before = var_state()
        done = run_the_script(argv)
        self.assertOneRefusalLine(done, text)
        self.assertEqual(var_state(), before, "a refused run changed var/")
        self.assertFalse(self.database.exists())

    def argv(self, main_args=None, drop=None, **replace):
        argv = wrapper_argv(self.database, main_args=main_args)
        if drop is not None:
            at = argv.index(f"--{drop}")
            del argv[at : at + 2]
        for name, value in replace.items():
            argv[argv.index(f"--{name}") + 1] = value
        return argv

    def test_each_of_the_four_flags_missing(self):
        for name in ("campaign", "prompt", "register", "purpose"):
            with self.subTest(name):
                self.refuse(
                    self.argv(drop=name),
                    f"the following arguments are required: --{name}",
                )

    def test_an_empty_flag(self):
        self.refuse(self.argv(purpose="  "), "--purpose is empty")

    def test_no_bare_double_dash(self):
        argv = self.argv()
        del argv[argv.index("--")]
        self.refuse(argv, "main.py's arguments follow a bare --")

    def test_no_database(self):
        self.refuse(
            self.argv(main_args=["--ray-address", "127.0.0.1:6380"]),
            "main.py's arguments: the following arguments are required: --database",
        )

    def test_an_empty_database(self):
        self.refuse(
            self.argv(main_args=["--database", "", "--ray-address", "127.0.0.1:6380"]),
            "--database is empty",
        )

    def test_an_unrecognised_main_py_option(self):
        self.refuse(
            self.argv(
                main_args=[
                    "--database",
                    str(self.database),
                    "--ray-address",
                    "127.0.0.1:6380",
                    "--no-such-option",
                ]
            ),
            "main.py's arguments: unrecognized arguments: --no-such-option",
        )

    def test_inventory(self):
        self.refuse(
            self.argv(main_args=["--database", str(self.database), "--inventory"]),
            "--inventory is not registered",
        )

    def test_ray_address_auto(self):
        self.refuse(
            self.argv(
                main_args=["--database", str(self.database), "--ray-address", "auto"]
            ),
            "--ray-address is required whenever Ray starts",
        )

    def test_no_ray_address(self):
        self.refuse(
            self.argv(main_args=["--database", str(self.database)]),
            "'auto' is refused",
        )


class TestHelp(SubprocessCase):
    def test_the_wrappers_help(self):
        before = var_state()
        done = run_the_script(["--help"])
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("usage: registered_run.py", done.stdout)
        self.assertEqual(done.stderr, "")
        self.assertMarkerLast(done.stdout, 0)
        self.assertEqual(var_state(), before)

    def test_main_pys_help_registers_nothing(self):
        before = var_state()
        done = run_the_script(self.argv_help())
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("--ray-address", done.stdout)
        self.assertNotIn("registered as", done.stdout)
        self.assertMarkerLast(done.stdout, 0)
        self.assertEqual(var_state(), before)

    def argv_help(self):
        return wrapper_argv(self.database, main_args=["--help"])


if __name__ == "__main__":
    unittest.main()
