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
`--no-alpha-values`: a run of one-loop rows alone, with no fixed-law row (no-alpha-values, 2026-11-07).

(1) the parser: without the flag every command parses as it did (`alpha_values` is [0.0] by default, no
    helper attribute is left on the namespace); with the flag and `--A-values`, `alpha_values` is the empty
    list and `A_values` is as given, on the command line and in a `--config` YAML file; the flag without
    `--A-values`, and the flag beside `--alpha-values` (given on the command line or in the YAML file, the
    default's own value 0 included), are each refused with exit 2 and the message on stderr; the help names
    the flag;
(2) the drivers: `main.py`, `plot_by_beta.py` and `plot_ScalarHistory.py`, each run as a subprocess from the
    repository root with the flag and no `--A-values`, exit 2 with the parser's message, before the
    provenance line they print after the parse and before the Ray address is checked (no address is given,
    so a parse that let the run through would end on the address's message instead). `main.py --inventory`
    is under the same rule, and with the flag and `--A-values` it reaches the inventory, which needs no Ray;
(3) the plot inputs: on a temporary store holding alpha = 0 and 0.5 under the fixed model and A = 0.5 and 1
    under the one-loop model, `plot_inputs.parameters` given the drivers' lists under the flag returns the A
    rows alone, and writes nothing; without the flag it returns alpha = 0 first, as before;
(4) the inventory: a store of one-loop rows alone, which is what a build under the flag makes, lists them
    as `A=<A> (<drain_model>)` and names no alpha.

No Ray cluster and no persistent store: (3) and (4) build temporary SQLite stores in the stand-in cluster.
The subprocesses of (2) import the drivers and stop at their parse, about 3 s each.
"""

import contextlib
import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import ray

import plot_inputs
from ComputeTargets.tests import test_plot_inputs as tpi
from ComputeTargets.tests.test_a_values_plot_inputs import _build_store
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED, DRAIN_MODEL_ONE_LOOP
from config.argument_parser import (
    NO_ALPHA_WITH_ALPHA,
    NO_ALPHA_WITHOUT_A,
    RAY_ADDRESS_PROBLEM,
    create_argument_parser,
    distinct_values,
)
from tools.tests import test_inventory_alpha as tia

ROOT = Path(__file__).resolve().parents[2]
BASE = ["--database", "x.db", "--ray-address", "127.0.0.1:6380"]
STORE_KEY = "no_alpha_values"


def _parse(argv):
    """`argv` through the shared parser: (namespace or None, exit code or None, stderr)."""
    err = io.StringIO()
    with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
        try:
            return create_argument_parser().parse_args(argv), None, err.getvalue()
        except SystemExit as e:
            return None, e.code, err.getvalue()


class TestTheParser(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)

    def yaml(self, text: str) -> str:
        path = self.dir / "config.yaml"
        path.write_text(text)
        return str(path)

    def test_without_the_flag_every_command_parses_as_it_did(self):
        for extra, alphas, As in (
            ([], [0.0], None),
            (["--alpha-values", "0", "0.1"], [0.0, 0.1], None),
            (["--A-values", "1"], [0.0], [1.0]),
            (["--alpha-values", "0.1", "--A-values", "1", "2"], [0.1], [1.0, 2.0]),
        ):
            with self.subTest(extra=extra):
                args, code, _ = _parse(BASE + extra)
                self.assertIsNone(code)
                self.assertFalse(args.no_alpha_values)
                self.assertEqual(args.alpha_values, alphas)
                self.assertEqual(args.A_values, As)
                self.assertEqual(
                    [k for k in vars(args) if k.startswith("_")], [], "no helper left"
                )

    def test_the_flag_empties_alpha_values_and_keeps_A_values(self):
        args, code, _ = _parse(
            BASE + ["--no-alpha-values", "--A-values", "0.1", "1", "0.1"]
        )
        self.assertIsNone(code)
        self.assertTrue(args.no_alpha_values)
        self.assertEqual(args.alpha_values, [])
        self.assertEqual(args.A_values, [0.1, 1.0, 0.1])
        # the drivers' own expressions give no fixed row and the A rows in order
        self.assertEqual(distinct_values(args.alpha_values), [])
        self.assertEqual(distinct_values(args.A_values or []), [0.1, 1.0])
        self.assertEqual([k for k in vars(args) if k.startswith("_")], [])

    def test_the_flag_in_a_config_file(self):
        path = self.yaml("no-alpha-values: true\nA-values: [0.5, 2]\n")
        args, code, _ = _parse(BASE + ["--config", path])
        self.assertIsNone(code)
        self.assertEqual(args.alpha_values, [])
        self.assertEqual(args.A_values, [0.5, 2.0])

    def test_a_run_with_no_row_is_refused(self):
        for extra in (
            ["--no-alpha-values"],
            ["--no-alpha-values", "--inventory"],
            ["--config", self.yaml("no-alpha-values: true\n")],
        ):
            with self.subTest(extra=extra):
                args, code, err = _parse(BASE + extra)
                self.assertIsNone(args)
                self.assertEqual(code, 2)
                self.assertIn(f"error: {NO_ALPHA_WITHOUT_A}", err)

    def test_the_flag_beside_alpha_values_is_refused(self):
        alpha_yaml = self.yaml("alpha-values: [0.1, 0.03]\n")
        for extra in (
            ["--no-alpha-values", "--alpha-values", "0.1", "--A-values", "1"],
            # the default's own value, given, is still given
            ["--alpha-values", "0", "--no-alpha-values", "--A-values", "1"],
            ["--config", alpha_yaml, "--no-alpha-values", "--A-values", "1"],
        ):
            with self.subTest(extra=extra):
                args, code, err = _parse(BASE + extra)
                self.assertIsNone(args)
                self.assertEqual(code, 2)
                self.assertIn(f"error: {NO_ALPHA_WITH_ALPHA}", err)
        # without the flag the YAML list is read as before
        args, code, _ = _parse(BASE + ["--config", alpha_yaml])
        self.assertIsNone(code)
        self.assertEqual(args.alpha_values, [0.1, 0.03])

    def test_the_help_names_the_flag(self):
        text = " ".join(create_argument_parser().format_help().split())
        self.assertIn("[--no-alpha-values]", text)
        self.assertIn("build, or read, no fixed-law row", text)
        self.assertIn("It needs --A-values and refuses --alpha-values", text)
        self.assertIn("unless --no-alpha-values is given", text)
        # the help of the two options of before still says what it said
        self.assertIn("0 is natural mode (the default [0.0])", text)
        self.assertIn("also builds, or reads, alpha = 0, natural mode", text)


def _driver(script, *extra):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    with tempfile.TemporaryDirectory() as tmp:
        return subprocess.run(
            [sys.executable, script, "--database", str(Path(tmp) / "store.db"), *extra],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )


class TestTheDrivers(unittest.TestCase):
    """Each driver as a subprocess; nothing reaches Ray (no address is given)."""

    def test_each_driver_refuses_a_run_with_no_row_at_its_parse(self):
        for script in ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=script):
                done = _driver(script, "--no-alpha-values")
                self.assertEqual(done.returncode, 2, done.stderr)
                self.assertIn(NO_ALPHA_WITHOUT_A, done.stderr)
                self.assertNotIn(RAY_ADDRESS_PROBLEM[3:40], done.stderr)
                # the provenance line is printed after the parse
                self.assertNotIn(">> packages:", done.stdout)

    def test_main_refuses_the_flag_beside_alpha_values(self):
        done = _driver(
            "main.py", "--no-alpha-values", "--alpha-values", "0.1", "--A-values", "1"
        )
        self.assertEqual(done.returncode, 2, done.stderr)
        self.assertIn(NO_ALPHA_WITH_ALPHA, done.stderr)

    def test_main_inventory_is_reached_under_the_flag(self):
        # the path is not a store, so the inventory refuses it (exit 1): the run got past the parse to
        # the inventory, which starts no Ray
        done = _driver("main.py", "--inventory", "--no-alpha-values", "--A-values", "1")
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("!! --inventory:", done.stderr)
        self.assertNotIn(NO_ALPHA_WITHOUT_A, done.stderr)


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    tpi._MODULE[STORE_KEY + "_tmp"] = tmp
    # alpha = 0 and 0.5 under the fixed model, A = 0.5 and 1 under the one-loop model
    tpi._MODULE[STORE_KEY] = _build_store(Path(tmp.name) / "both")


def tearDownModule():
    tpi._MODULE.pop(STORE_KEY, None)
    tpi._MODULE.pop(STORE_KEY + "_tmp").cleanup()
    if ray.is_initialized():
        raise AssertionError("Ray was initialised by test_no_alpha_values")


def _digests(primary: Path):
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(primary.parent.iterdir())
        if p.is_file()
    }


class TestThePlotInputs(tpi._ReadOnlyCase):
    def rows(self, primary, extra):
        args = tpi._args(*extra)
        with self.read_only(primary) as pool:
            with tpi.quiet():
                return plot_inputs.parameters(
                    pool,
                    abs_tol=args.abs_tol,
                    rel_tol=args.rel_tol,
                    T_init_GeV=args.T_init_GeV,
                    phi_init_Mp=args.phi_init_Mp,
                    units=tpi.UNITS,
                    tuning_tol=args.tuning_tolerance,
                    alpha_values=distinct_values(args.alpha_values),
                    A_values=distinct_values(args.A_values or []),
                )

    def test_under_the_flag_only_the_A_rows_are_read(self):
        primary = self.copy(STORE_KEY)
        before = _digests(primary)
        rows = self.rows(primary, ["--no-alpha-values", "--A-values", "1", "0.5"])
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows.alphas],
            [(1.0, DRAIN_MODEL_ONE_LOOP), (0.5, DRAIN_MODEL_ONE_LOOP)],
        )
        self.assertEqual(_digests(primary), before)

    def test_without_the_flag_alpha_0_is_read_first_as_before(self):
        primary = self.copy(STORE_KEY)
        rows = self.rows(primary, ["--A-values", "1"])
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows.alphas],
            [(0.0, DRAIN_MODEL_FIXED), (1.0, DRAIN_MODEL_ONE_LOOP)],
        )

    def test_an_A_not_stored_is_still_refused_under_the_flag(self):
        primary = self.copy(STORE_KEY)
        with self.assertRaises(plot_inputs.PlotOptionNotStored) as caught:
            self.rows(primary, ["--no-alpha-values", "--A-values", "2"])
        self.assertIn("--A-values", str(caught.exception))


class TestTheInventoryOfOneLoopRowsAlone(tia._AlphaStoreCase):
    def test_the_rows_read_as_A_and_no_alpha_is_named(self):
        _, lines = self.report([(0.1, tia.ONE_LOOP), (1.0, tia.ONE_LOOP)])
        self.assertIn(
            f"values = [ A=0.1 ({tia.ONE_LOOP}), A=1 ({tia.ONE_LOOP}) ]", lines
        )
        histories = [l for l in lines if l.startswith("- T_init=")]
        self.assertEqual(len(histories), 2)
        for l in histories:
            self.assertNotIn("alpha=", l)
            self.assertEqual(l.count("A="), 1)


if __name__ == "__main__":
    unittest.main()
