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

"""What `RunDrivers/registered_run.py` records when it registers a run (rrk-adoption prompt 03,
W3 and W6 test 1), and what it hands the program it runs.

Each test runs the wrapper's `main` in this process on a stand-in program
(`stubbed_run.standin_program`), into a temporary runs root, with `ray.init` and `ray.shutdown`
replaced by stand-ins before the wrapper wraps them (`stubbed_run.in_process`). The database is a
path in the temporary directory that no test here creates. One test calls `register` alone with
`program=MAIN_PY`, which reads no `main.py` text and runs nothing (the orchestrator's note for 03,
ruling 4). No Ray is initialised, and nothing under `var/` is read or written.
"""

import hashlib
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ray
import runregistrykit

from RunDrivers import registered_run
from RunDrivers.tests.stubbed_run import (
    REPO_ROOT,
    TITLE,
    WRAPPER,
    Recorder,
    TakeOver,
    StringWriter,
    in_process,
    install_ray_like_handler,
    only_run,
    ray_like_sigterm_handler,
    read_json,
    read_record,
    standin_program,
    wrapper_argv,
)

ADDRESS = "127.0.0.1:6380"

# rrk-adoption prompt 04's pilot, as build.sh runs it, with 04's job name
PILOT = [
    "--config",
    "exponential.yaml",
    "--database",
    "var/datastores/rrk-adoption-04-reference.db",
    "--shards",
    "16",
    "--job-name",
    "rrk-adoption-04-reference",
    "--M-values-Mp",
    "1e-3",
    "--beta-values",
    "1.1",
    "1.15",
    "1.2",
    "10",
    "25",
    "--ray-address",
    ADDRESS,
]
PILOT_SCOPE = (
    "config exponential.yaml; potential Exponential; β 1.1, 1.15, 1.2, 10, 25; "
    "M 0.001 Mp; φ* 5 Mp; 16 shards; job rrk-adoption-04-reference"
)


def sha256(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class WrapperCase(unittest.TestCase):
    """A temporary directory holding the stand-in, the database path (never created here) and the
    runs root. Every test ends with Ray not initialised."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-registered-run-")
        self.addCleanup(tmp.cleanup)
        # resolved: on macOS the temporary directory is reached through /var -> /private/var
        self.top = Path(tmp.name).resolve()
        self.runs_root = self.top / "runs"
        self.runs_root.mkdir()
        self.database = self.top / "store.db"

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    def program(self, behaviour="return"):
        return standin_program(self.top, behaviour, self.runs_root)

    def run_it(self, behaviour="return", argv=None, **kwargs):
        program = self.program(behaviour)
        argv = wrapper_argv(self.database) if argv is None else argv
        return program, argv, in_process(self, program, argv, self.runs_root, **kwargs)


class TestRegistration(WrapperCase):
    def test_the_manifest_is_written_before_the_programs_first_line(self):
        program, _, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        run = only_run(self.runs_root)
        self.assertEqual(
            read_record(program)["manifests_at_start"],
            [str(run / "manifest.json")],
        )

    def test_the_manifest_records_the_program_the_store_the_scope_and_the_packages(
        self,
    ):
        program, _, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        manifest = read_json(only_run(self.runs_root) / "manifest.json")

        # U3: the program run, with its own hash. A stand-in outside the client root is recorded
        # by its absolute path, as is a store outside it (begin's _repo_path)
        self.assertEqual(manifest["script"], str(program))
        self.assertEqual(manifest["script_sha256"], sha256(program))
        self.assertEqual(manifest["results"], str(self.database))
        self.assertIsNone(manifest["checkpoint"])
        # begin takes git_head and git_dirty from the directory of `script` (runregistrykit
        # v0.2.0, begin's `start`): the stand-in's temporary directory is in no repository.
        # TestRegisterInProduction shows the production form
        self.assertEqual(manifest["git_head"], "unknown")
        self.assertIs(manifest["git_dirty"], False)
        self.assertEqual(
            manifest["scope"],
            "no config file; potential none; β 0.1–3.0, 5 per unit; "
            "M 10^25–10^26.5 eV, 6 per decade; φ* 5 Mp; "
            "20 shards; job ChamPBH-test",
        )
        self.assertEqual(
            (manifest["campaign"], manifest["prompt"]), ("rrk-adoption", "03")
        )
        self.assertTrue(
            os.path.basename(only_run(self.runs_root)).startswith(
                "rrk-adoption-03-unit-"
            )
        )
        for name in ("runregistrykit", "datastorekit", "raypoolkit"):
            self.assertIn(name, manifest["packages"])
            self.assertIsNotNone(manifest["packages"][name], name)

    def test_heartbeat_means_states_the_floor_the_throttle_the_workers_and_stale(self):
        _, _, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        said = read_json(only_run(self.runs_root) / "manifest.json")["heartbeat_means"]
        self.assertEqual(
            said,
            registered_run.HEARTBEAT_MEANS.format(
                interval=registered_run.BEAT_MIN_INTERVAL,
                notify=registered_run.NOTIFY_FLOOR,
                stale=runregistrykit.DEFAULT_STALE_AFTER,
            ),
        )
        for fragment in (
            "at most once per 30 s",
            "no more often than every 300 s",
            "MIN_NOTIFY_INTERVAL",
            "within about five minutes",
            "a worker's line beats too",
            "does NOT mean any Ray actor is progressing",
            "(900 s, `list --stale-after`)",
            "Stale means go and look",
        ):
            self.assertIn(fragment, said)

    def test_the_manifests_argv_is_the_wrappers(self):
        _, argv, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        manifest = read_json(only_run(self.runs_root) / "manifest.json")
        self.assertEqual(manifest["argv"][0], str(WRAPPER))
        self.assertEqual(manifest["argv"][1:], argv)

    def test_it_prints_the_run_id_the_manifest_and_the_list_command(self):
        _, _, done = self.run_it()
        run = only_run(self.runs_root)
        lines = done.out.text().splitlines()
        self.assertEqual(lines[0], f"** registered_run: registered as {run.name}")
        self.assertEqual(lines[1], f"**   manifest {run / 'manifest.json'}")
        self.assertIn("python -m runregistrykit list", lines[2])


class TestTheProgram(WrapperCase):
    """What the program sees: its own path as `sys.argv[0]`, `main.py`'s arguments verbatim,
    `__name__ == "__main__"`, and `ray.init` called with its arguments unchanged (the
    orchestrator's note for 03, ruling 5)."""

    def test_it_runs_as_main_from_its_own_file_with_main_pys_arguments(self):
        program, argv, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        record = read_record(program)
        self.assertEqual(record["argv"][0], str(program))
        self.assertEqual(record["argv"][1:], argv[argv.index("--") + 1 :])
        self.assertEqual(record["name"], "__main__")
        self.assertEqual(record["file"], str(program))

    def test_ray_init_gets_the_programs_arguments_unchanged(self):
        _, _, done = self.run_it()
        self.assertEqual(done.init.calls, [((), {"address": ADDRESS})])
        self.assertEqual(len(done.shutdown.calls), 1)

    def test_the_wrappers_handler_replaces_rays_after_ray_init(self):
        seen = []

        def like_ray(*args, **kwargs):
            install_ray_like_handler()
            seen.append(signal.getsignal(signal.SIGTERM))

        program, _, done = self.run_it(init=Recorder(side_effect=like_ray))
        self.assertIsNone(done.outcome, repr(done.outcome))
        self.assertEqual(seen, [ray_like_sigterm_handler])
        record = read_record(program)
        self.assertEqual(record["sigterm_before_init"], "register.<locals>.terminal")
        self.assertEqual(
            record["sigterm_after_init"], "make_killed_handler.<locals>.killed_handler"
        )

    def test_a_returning_program_is_done_at_its_last_stage(self):
        _, _, done = self.run_it()
        self.assertIsNone(done.outcome, repr(done.outcome))
        run = only_run(self.runs_root)
        status = read_json(run / "status.json")
        self.assertEqual((status["state"], status["exit_code"]), ("done", 0))
        self.assertEqual(status["stage"], f"{TITLE}, all work items complete")
        # there is no store, so no fingerprint: recorded, and the run is still done
        self.assertIn("does not exist", status["fingerprint_error"])
        self.assertFalse(self.database.exists())
        # the real stream and the copy hold the same text, byte for byte
        self.assertEqual((run / "stdout.log").read_text(), done.out.text())
        self.assertEqual(done.taken, {"stdout": True, "stderr": True, "handlers": True})


class TestRegisterInProduction(WrapperCase):
    """U3 in its production form: `register` with `program=MAIN_PY` records `main.py`, its hash
    and this repository's `HEAD`. Nothing runs: `register` reads no `main.py` text and starts
    nothing (the orchestrator's note for 03, ruling 4)."""

    def test_the_script_is_main_py_with_its_hash_and_this_repositorys_head(self):
        argv = wrapper_argv(self.database, slug="production")
        args, _, parsed = registered_run.parse_arguments(argv)
        saved = TakeOver()
        self.addCleanup(saved.restore)
        installed = (StringWriter(), StringWriter())
        sys.stdout, sys.stderr = installed
        try:
            run = registered_run.register(
                args,
                parsed,
                argv,
                program=registered_run.MAIN_PY,
                runs_root=self.runs_root,
            )
            # observed before restoring
            taken = (
                isinstance(sys.stdout, registered_run.RegisteredStream),
                isinstance(sys.stderr, registered_run.RegisteredStream),
                signal.getsignal(signal.SIGTERM).__qualname__,
                signal.getsignal(signal.SIGINT).__qualname__,
            )
        finally:
            saved.restore()
        self.assertEqual(
            taken,
            (True, True, "register.<locals>.terminal", "register.<locals>.terminal"),
        )
        manifest = read_json(run.manifest_path)
        self.assertEqual(manifest["script"], "main.py")
        self.assertEqual(manifest["script_sha256"], sha256(REPO_ROOT / "main.py"))
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        self.assertEqual(manifest["git_head"], head)
        self.assertEqual(manifest["results"], str(self.database))
        self.assertEqual(Path(run.path).parent, self.runs_root)
        self.assertEqual(read_json(run.status_path)["state"], "running")


class TestScope(WrapperCase):
    """`scope` is one line from `main.py`'s parsed arguments as given, never its grid (the
    orchestrator's note for 03, ruling 11). Parsing reads `exponential.yaml` at the root.
    """

    def scope(self, main_args):
        _, _, parsed = registered_run.parse_arguments(
            wrapper_argv(None, main_args=main_args)
        )
        return registered_run.describe_scope(parsed)

    def test_the_pilots_scope(self):
        self.assertEqual(self.scope(PILOT), PILOT_SCOPE)

    def test_more_than_six_values_read_as_a_count_and_a_range(self):
        scope = self.scope(
            [
                "--database",
                "x.db",
                "--ray-address",
                ADDRESS,
                "--potential-type",
                "Exponential",
                "--beta-values",
                *["0.1", "0.5", "1", "1.5", "2", "2.5", "3"],
                "--M-values-eV",
                "1e24",
                "--phi-init-Mp",
                "2.5",
            ]
        )
        self.assertEqual(
            scope,
            "no config file; potential Exponential; β 7 values, 0.1–3; M 1e+24 eV; "
            "φ* 2.5 Mp; 20 shards; job ChamPBH-test",
        )


class TestBeginsErrors(WrapperCase):
    """A `ConfigError` from `begin` keeps its type and its traceback; any other `RuntimeError`
    from it becomes a `Refusal` with its text. Either way nothing is created and nothing is
    taken over, and the program does not run."""

    def test_a_config_error_is_not_a_refusal(self):
        error = runregistrykit.ConfigError("no runregistrykit.toml was found")
        self.assertIsInstance(error, RuntimeError)
        with mock.patch.object(runregistrykit, "begin", side_effect=error):
            program, _, done = self.run_it()
        self.assertIs(done.outcome, error)
        self.assertNotIsInstance(done.outcome, registered_run.Refusal)
        self.assertEqual(list(self.runs_root.iterdir()), [])
        self.assertFalse(program.with_suffix(".json").exists(), "the program ran")
        self.assertEqual(
            done.taken, {"stdout": False, "stderr": False, "handlers": False}
        )
        self.assertEqual(done.init.calls, [])

    def test_a_runtime_error_is_a_refusal_with_its_text(self):
        error = RuntimeError("Cannot begin run x: ... No run directory was created")
        with mock.patch.object(runregistrykit, "begin", side_effect=error):
            program, _, done = self.run_it()
        self.assertIsInstance(done.outcome, registered_run.Refusal)
        self.assertEqual(str(done.outcome), str(error))
        self.assertIs(done.outcome.__cause__, error)
        self.assertEqual(list(self.runs_root.iterdir()), [])
        self.assertFalse(program.with_suffix(".json").exists(), "the program ran")
        self.assertEqual(
            done.taken, {"stdout": False, "stderr": False, "handlers": False}
        )


if __name__ == "__main__":
    unittest.main()
