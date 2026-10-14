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
tools/surfing_scan.py (surfing-temperature prompt 02, T2-T4.2).

- `summarise` on hand-built crossing lists (U1-U3), with stub kick windows and root records;
- the beta grid, built by index;
- the CSV and its resume, in a `tempfile` directory, through `main(argv)` with `run_history`
  replaced by a stub, so that no integration runs;
- one real history: beta = 2, phi* = 5 M_P, M = 1e-3 M_P, from T* = 2e4 GeV to T_stop = 0.5 GeV,
  below its first bounce (0.7467 GeV), through the tool's own `run_history`. 0.35 s at load 7
  (0.77 s to T_stop = 1 MeV, measured by prompt 02). No PRyMordial, no Ray, no store.

Every test runs from the repository root, as the suites do; the main() tests build the production
cosmology and find_kicks once per call (about a second each).
"""

import contextlib
import csv
import io
import json
import os
import tempfile
import unittest
from math import exp, log
from pathlib import Path

import tools.surfing_scan as ss
from ComputeTargets.ScalarModel import SurfingCrossing, first_bounce
from Units import Planck_units

ROOT = Path(__file__).resolve().parents[2]

_units = Planck_units()
GeV = _units.GeV

# stub kick windows and root records, in the cosmology's units, as find_kicks and
# reachability_roots return them
KICKS = [
    {"label": "EW", "T_window_hot": 1e4 * GeV, "T_window_cold": 10.0 * GeV},
    {"label": "QCD", "T_window_hot": 10.0 * GeV, "T_window_cold": 0.07 * GeV},
    {"label": "ee", "T_window_hot": 0.07 * GeV, "T_window_cold": 1e-5 * GeV},
]
ROOTS = [
    {"kick": "EW", "T_hot": 100.0 * GeV, "T_cold": 30.0 * GeV, "merged": None},
    {"kick": "QCD", "T_hot": 0.75 * GeV, "T_cold": 0.1 * GeV, "merged": None},
    {"kick": "ee", "T_hot": None, "T_cold": None, "merged": None},
]


def crossing(N: float, T_GeV: float, direction: str) -> SurfingCrossing:
    return SurfingCrossing(
        N=N,
        phi_Einstein=1.0,
        pi_Einstein=-0.5,
        log_T_Jordan=log(T_GeV * GeV),
        direction=direction,
    )


def bounce(N: float, T_GeV: float, reflected: bool = False):
    return ss.FirstBounce(
        N=N, phi_Einstein=1e-4, log_T_Jordan=log(T_GeV * GeV), reflected=reflected
    )


class TestSummarise(unittest.TestCase):
    def check_close(self, value, expected):
        self.assertIsNotNone(value)
        self.assertLessEqual(abs(value - expected), 1e-12 * abs(expected))

    def test_a_not_captured(self):
        """(a) no crossing: not captured, every temperature blank but delivery."""
        row = ss.summarise([], bounce(20.0, 1.5), KICKS, None, _units)
        self.assertFalse(row["captured"])
        self.assertEqual(row["n_crossings"], 0)
        for key in (
            "capture_kick",
            "T_capture_GeV",
            "N_capture",
            "T_max_GeV",
            "N_max",
            "T_second_dip_GeV",
            "N_second_dip",
            "T_hot_GeV",
            "T_cold_GeV",
        ):
            self.assertIsNone(row[key], key)
        self.check_close(row["T_deliver_GeV"], 1.5)
        self.assertEqual(row["N_deliver"], 20.0)
        self.assertIs(row["deliver_reflected"], False)

    def test_b_min_only(self):
        """(b) one "min" and no bounce above T_stop: capture only, delivery blank."""
        row = ss.summarise([crossing(14.0, 0.65, "min")], None, KICKS, ROOTS, _units)
        self.assertTrue(row["captured"])
        self.assertEqual(row["n_crossings"], 1)
        self.assertEqual(row["capture_kick"], "QCD")
        self.check_close(row["T_capture_GeV"], 0.65)
        self.assertEqual(row["N_capture"], 14.0)
        self.assertIsNone(row["T_max_GeV"])
        self.assertIsNone(row["T_second_dip_GeV"])
        self.assertIsNone(row["T_deliver_GeV"])
        self.assertIsNone(row["deliver_reflected"])
        self.check_close(row["T_hot_GeV"], 0.75)
        self.check_close(row["T_cold_GeV"], 0.1)

    def test_c_min_max_then_the_wall(self):
        """(c) min, max, then the bounce before a second dip: no second dip."""
        row = ss.summarise(
            [crossing(14.0, 0.65, "min"), crossing(18.9, 0.75, "max")],
            bounce(20.35, 0.7467),
            KICKS,
            ROOTS,
            _units,
        )
        self.assertEqual(row["n_crossings"], 2)
        self.check_close(row["T_capture_GeV"], 0.65)
        self.check_close(row["T_max_GeV"], 0.75)
        self.assertEqual(row["N_max"], 18.9)
        self.assertIsNone(row["T_second_dip_GeV"])
        self.assertIsNone(row["N_second_dip"])
        self.check_close(row["T_deliver_GeV"], 0.7467)
        self.assertEqual(row["capture_kick"], "QCD")

    def test_d_min_max_min_max(self):
        """(d) min, max, min, max: the second dip is the second min; largest is the larger max."""
        row = ss.summarise(
            [
                crossing(10.0, 77.0, "min"),
                crossing(12.0, 100.0, "max"),
                crossing(14.0, 97.0, "min"),
                crossing(16.0, 101.0, "max"),
            ],
            bounce(17.0, 97.6),
            KICKS,
            ROOTS,
            _units,
        )
        self.assertEqual(row["n_crossings"], 4)
        self.assertEqual(row["capture_kick"], "EW")
        self.check_close(row["T_capture_GeV"], 77.0)
        self.assertEqual(row["N_capture"], 10.0)
        self.check_close(row["T_max_GeV"], 101.0)
        self.assertEqual(row["N_max"], 16.0)
        self.check_close(row["T_second_dip_GeV"], 97.0)
        self.assertEqual(row["N_second_dip"], 14.0)
        self.check_close(row["T_deliver_GeV"], 97.6)
        self.check_close(row["T_hot_GeV"], 100.0)
        self.check_close(row["T_cold_GeV"], 30.0)

    def test_e_capture_kick_without_roots(self):
        """(e) a capture in a window whose kick has no interval at this beta: roots blank."""
        row = ss.summarise(
            [crossing(30.0, 1e-3, "min")], bounce(31.0, 9e-4), KICKS, ROOTS, _units
        )
        self.assertEqual(row["capture_kick"], "ee")
        self.assertIsNone(row["T_hot_GeV"])
        self.assertIsNone(row["T_cold_GeV"])


class TestBetaGrid(unittest.TestCase):
    def test_grid_1p10_to_5p00(self):
        """79 values, each the decimal it names, with no drift."""
        grid = ss.beta_grid(1.10, 5.00, 0.05)
        self.assertEqual(len(grid), 79)
        self.assertEqual(grid[0], 1.1)
        self.assertEqual(grid[-1], 5.0)
        expected = [float(f"{110 + 5 * k}e-2") for k in range(79)]
        self.assertEqual(grid, expected)
        self.assertEqual([repr(b) for b in grid][:3], ["1.1", "1.15", "1.2"])

    def test_format_field(self):
        """blank, booleans, repr of floats."""
        self.assertEqual(ss.format_field(None), "")
        self.assertEqual(ss.format_field(True), "true")
        self.assertEqual(ss.format_field(False), "false")
        self.assertEqual(ss.format_field(0.1 + 0.2), "0.30000000000000004")
        self.assertEqual(ss.format_field(4), "4")


class _StubRuns:
    """A stand-in for run_history that records the beta it is asked for; no integration."""

    def __init__(self):
        self.calls = []

    def __call__(self, setup, beta, phi_init_Mp, M_Mp, T_stop_GeV):
        self.calls.append(beta)
        if beta == 1.3:
            crossings = [
                crossing(14.0, 0.65, "min"),
                crossing(18.9, 0.75, "max"),
            ]
            b = bounce(20.35, 0.7467)
        elif beta == 1.4:
            return ss.HistoryOutcome(
                failure_reason="stub: an integration failure, with, commas",
                crossings=None,
                bounce=None,
                accepted_steps=None,
                reflections=None,
                wall_s=0.25,
                result=None,
            )
        else:
            crossings = []
            b = bounce(20.0, 0.14)
        return ss.HistoryOutcome(
            failure_reason=None,
            crossings=crossings,
            bounce=b,
            accepted_steps=1000,
            reflections=0,
            wall_s=0.5,
            result=None,
        )


class TestResume(unittest.TestCase):
    def setUp(self):
        self.assertTrue((Path(os.getcwd()) / ss.EOS_TABLE).is_file())
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.out = self.tmp / "scan.csv"
        self.stub = _StubRuns()
        self._saved = ss.run_history
        ss.run_history = self.stub

    def tearDown(self):
        ss.run_history = self._saved
        self._tmp.cleanup()

    def run_main(self, *argv) -> int:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = ss.main(["--out", str(self.out), *argv])
        self.stdout = buffer.getvalue()
        return code

    def rows(self):
        with open(self.out, newline="") as f:
            return list(csv.reader(f))

    def test_a_write_then_skip(self):
        """(a) two rows written; a second run over three beta runs only the third."""
        self.assertEqual(self.run_main("--beta-values", "1.2", "1.3"), 0)
        rows = self.rows()
        self.assertEqual(rows[0], ss.COLUMNS)
        self.assertEqual(len(rows), 3)
        self.assertEqual(self.stub.calls, [1.2, 1.3])

        first = dict(zip(ss.COLUMNS, rows[1]))
        self.assertEqual(first["beta"], "1.2")
        self.assertEqual(first["phi_init_Mp"], "5.0")
        self.assertEqual(first["M_Mp"], "0.001")
        self.assertEqual(first["Lambda_eV"], "0.001")
        self.assertEqual(first["T_stop_GeV"], "0.001")
        self.assertEqual(first["status"], "ok")
        self.assertEqual(first["failure_reason"], "")
        self.assertEqual(first["captured"], "false")
        self.assertEqual(first["capture_kick"], "")
        self.assertEqual(first["T_capture_GeV"], "")
        self.assertEqual(first["n_crossings"], "0")
        self.assertEqual(first["deliver_reflected"], "false")
        self.assertEqual(float(first["T_deliver_GeV"]), exp(log(0.14 * GeV)) / GeV)

        second = dict(zip(ss.COLUMNS, rows[2]))
        self.assertEqual(second["captured"], "true")
        self.assertEqual(second["capture_kick"], "QCD")
        self.assertEqual(second["n_crossings"], "2")
        self.assertEqual(second["T_second_dip_GeV"], "")
        T = exp(log(0.65 * GeV)) / GeV
        self.assertEqual(second["T_capture_GeV"], repr(T))
        # the production roots of the QCD kick at beta = 1.3, which exist
        self.assertNotEqual(second["T_hot_GeV"], "")
        self.assertNotEqual(second["T_cold_GeV"], "")

        self.stub.calls.clear()
        self.assertEqual(self.run_main("--beta-values", "1.2", "1.3", "1.5"), 0)
        self.assertEqual(self.stub.calls, [1.5])
        rows = self.rows()
        self.assertEqual(len(rows), 4)
        self.assertEqual([r[0] for r in rows[1:]], ["1.2", "1.3", "1.5"])

        meta = json.loads(Path(str(self.out) + ".meta.json").read_text())
        self.assertEqual(len(meta["runs"]), 2)
        self.assertFalse(meta["runs"][0]["resumed"])
        self.assertTrue(meta["runs"][1]["resumed"])
        self.assertEqual(meta["runs"][1]["skipped"], 2)
        for key in (
            "start_time",
            "git_head",
            "git_tracked_changes",
            "version_label",
            "options",
            "python",
            "scipy",
        ):
            self.assertIn(key, meta["runs"][0])
        self.assertEqual(meta["runs"][0]["version_label"], ss.VERSION_LABEL)
        self.assertEqual(meta["runs"][0]["options"]["phi_init_Mp"], 5.0)

    def test_b_phi_init_mismatch_refused(self):
        """(b) a resumed CSV whose phi* differs is refused, exit 2, and left as it was."""
        self.assertEqual(self.run_main("--beta-values", "1.2", "1.3"), 0)
        before = self.out.read_bytes()
        self.stub.calls.clear()
        self.assertEqual(
            self.run_main("--beta-values", "1.5", "--phi-init-Mp", "4.0"), 2
        )
        self.assertEqual(self.stub.calls, [])
        self.assertEqual(self.out.read_bytes(), before)

    def test_c_T_stop_and_M_mismatch_refused(self):
        """(c) likewise for T_stop and for M."""
        self.assertEqual(self.run_main("--beta-values", "1.2"), 0)
        self.assertEqual(
            self.run_main("--beta-values", "1.5", "--T-stop-GeV", "2e-3"), 2
        )
        self.assertEqual(self.run_main("--beta-values", "1.5", "--M", "1e-2"), 2)
        self.assertEqual(self.stub.calls, [1.2])

    def test_d_header_mismatch_refused(self):
        """(d) a file with another header is refused, exit 2."""
        self.out.write_text("beta,something_else\n")
        self.assertEqual(self.run_main("--beta-values", "1.2"), 2)
        self.assertEqual(self.stub.calls, [])

    def test_e_failure_row(self):
        """(e) a failure is a row with its reason, exit 1, and it is not retried on resume."""
        self.assertEqual(self.run_main("--beta-values", "1.4"), 1)
        row = dict(zip(ss.COLUMNS, self.rows()[1]))
        self.assertEqual(row["status"], "failure")
        self.assertEqual(
            row["failure_reason"], "stub: an integration failure, with, commas"
        )
        self.assertEqual(row["captured"], "")
        self.assertEqual(row["T_deliver_GeV"], "")
        self.stub.calls.clear()
        self.assertEqual(self.run_main("--beta-values", "1.4"), 0)
        self.assertEqual(self.stub.calls, [])

    def test_f_grid_options(self):
        """(f) --beta-min/--beta-max/--beta-step build the grid by index."""
        self.assertEqual(
            self.run_main(
                "--beta-min", "1.10", "--beta-max", "1.30", "--beta-step", "0.05"
            ),
            0,
        )
        self.assertEqual(self.stub.calls, [1.1, 1.15, 1.2, 1.25, 1.3])

    def test_g_refuses_outside_the_root(self):
        """(g) run from another directory: refused, exit 2, nothing written."""
        cwd = os.getcwd()
        try:
            os.chdir(self.tmp)
            self.assertEqual(self.run_main("--beta-values", "1.2"), 2)
        finally:
            os.chdir(cwd)
        self.assertFalse(self.out.exists())
        self.assertEqual(self.stub.calls, [])


class TestRealHistory(unittest.TestCase):
    def test_beta_2(self):
        """
        beta = 2, phi* = 5, M = 1e-3, from T* to T_stop = 0.5 GeV, through run_history: 0.35 s
        at load 7. No PRyMordial runs.
        """
        SM = ss.importlib.import_module("ComputeTargets.ScalarModel")
        original = SM.integrate_scalar_history

        setup = ss.make_setup()
        units = setup.units
        with contextlib.redirect_stdout(io.StringIO()):
            outcome = ss.run_history(setup, 2.0, 5.0, 1e-3, 0.5)

        # the wrapper is removed
        self.assertIs(SM.integrate_scalar_history, original)

        self.assertIsNone(outcome.failure_reason)
        result = outcome.result
        crossings = outcome.crossings
        self.assertGreaterEqual(len(crossings), 1)

        bounce = first_bounce(result)
        self.assertIsNotNone(bounce)
        self.assertLess(0.5 * units.GeV, exp(bounce.log_T_Jordan))

        M_P = units.PlanckMass
        for c in crossings:
            self.assertLess(c.N, bounce.N)
            y = result.solution(c.N)
            self.assertLess(abs(1.0 + 2.0 * y[1] / M_P), 1e-8)
            self.assertEqual(c.pi_Einstein, y[1])

            log_T = c.log_T_Jordan
            before = result.solution(c.N - 1e-3)[4]
            after = result.solution(c.N + 1e-3)[4]
            if c.direction == "min":
                self.assertGreater(before, log_T)
                self.assertGreater(after, log_T)
            else:
                self.assertEqual(c.direction, "max")
                self.assertLess(before, log_T)
                self.assertLess(after, log_T)

        # delivery is first_bounce's, exactly
        self.assertEqual(outcome.bounce.log_T_Jordan, bounce.log_T_Jordan)
        row = ss.summarise(crossings, outcome.bounce, [], None, units)
        self.assertEqual(row["T_deliver_GeV"], exp(bounce.log_T_Jordan) / units.GeV)
        self.assertEqual(row["N_deliver"], bounce.N)


if __name__ == "__main__":
    unittest.main()
