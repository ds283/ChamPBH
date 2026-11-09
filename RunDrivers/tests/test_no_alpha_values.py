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

"""
The wrapper and `--no-alpha-values` (no-alpha-values, 2026-11-07).

- `describe_scope` reads `α none` under the flag, followed by the A part; a run without the flag has the
  scope it had (`test_scope_alpha` and `test_scope_a_values` pin those strings, and are untouched);
- the flag without `--A-values`, and the flag beside `--alpha-values`, are each refused by the wrapper with
  the parser's message, before anything is registered and with no `ray.init`;
- a run given the flag and `--A-values` is registered, its manifest's scope reads `α none; A …`, and the
  program is handed the arguments verbatim.

Each wrapper test runs the wrapper's `main` in this process on a stand-in program
(`stubbed_run.standin_program`), into a temporary runs root, with `ray.init` and `ray.shutdown`
replaced by stand-ins (`stubbed_run.in_process`). No Ray, no store, nothing under `var/`. Instant.
"""

import tempfile
import unittest
from pathlib import Path

import ray

from RunDrivers import registered_run
from RunDrivers.tests.stubbed_run import (
    in_process,
    only_run,
    read_json,
    read_record,
    standin_program,
    wrapper_argv,
)
from config.argument_parser import (
    NO_ALPHA_WITH_ALPHA,
    NO_ALPHA_WITHOUT_A,
    create_argument_parser,
)

ADDRESS = "127.0.0.1:6380"
BASE = ["--database", "x.db", "--ray-address", ADDRESS]


def scope(*extra):
    parsed = create_argument_parser().parse_args(BASE + list(extra))
    return registered_run.describe_scope(parsed)


class TestTheScopeUnderTheFlag(unittest.TestCase):
    def test_alpha_reads_none_and_A_follows(self):
        self.assertEqual(
            scope("--no-alpha-values", "--A-values", "0.1", "1"),
            "no config file; β 0.1–3.0, 5 per unit; M 10^25–10^26.5 eV, 6 per decade; "
            "n 1; φ* 5 Mp; tuning tolerance 1e-05; α none; A 0.1, 1; 20 shards; job ChamPBH-test",
        )

    def test_without_the_flag_the_scope_is_as_before(self):
        self.assertIn("; α 0; A 1; ", scope("--A-values", "1"))
        self.assertIn("; α 0; 20 shards; ", scope())
        self.assertNotIn("α none", scope("--alpha-values", "0.1", "--A-values", "1"))


class _WrapperCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-no-alpha-values-")
        self.addCleanup(tmp.cleanup)
        self.top = Path(tmp.name).resolve()
        self.runs_root = self.top / "runs"
        self.runs_root.mkdir()
        self.database = self.top / "store.db"

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    def run_it(self, extra):
        program = standin_program(self.top, "return", self.runs_root)
        main_args = ["--database", str(self.database), "--ray-address", ADDRESS]
        main_args += list(extra)
        argv = wrapper_argv(None, main_args=main_args)
        return program, main_args, in_process(self, program, argv, self.runs_root)


class TestTheWrapperRefuses(_WrapperCase):
    def test_a_run_with_no_row_and_a_contradiction_are_refused(self):
        for extra, text in (
            (["--no-alpha-values"], NO_ALPHA_WITHOUT_A),
            (
                ["--no-alpha-values", "--alpha-values", "0", "--A-values", "1"],
                NO_ALPHA_WITH_ALPHA,
            ),
            (["--alpha-values", "0.1", "--no-alpha-values"], NO_ALPHA_WITH_ALPHA),
        ):
            with self.subTest(extra=extra):
                _, _, done = self.run_it(extra)
                self.assertIsInstance(done.outcome, registered_run.Refusal)
                self.assertEqual(
                    str(done.outcome), f"main.py's arguments: {text}", done.outcome
                )
                self.assertEqual(list(self.runs_root.iterdir()), [])
                self.assertEqual(done.init.calls, [])


class TestTheWrapperRegisters(_WrapperCase):
    def test_the_manifest_records_alpha_none_and_the_program_gets_its_arguments(self):
        extra = ["--no-alpha-values", "--A-values", "0.5", "2"]
        program, main_args, done = self.run_it(extra)
        self.assertIsNone(done.outcome, repr(done.outcome))
        manifest = read_json(only_run(self.runs_root) / "manifest.json")
        self.assertIn("; α none; A 0.5, 2; ", manifest["scope"])
        self.assertEqual(read_record(program)["argv"][1:], main_args)


if __name__ == "__main__":
    unittest.main()
