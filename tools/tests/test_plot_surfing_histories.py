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
tools/plot_surfing_histories.py (standalone, 2026-10-14).

- `build_figure` on synthetic traces: one line per history in each panel, dashed if captured,
  the turning points and bounces marked, the axes' scales, the kick peaks inside the T_J range
  only, and no more than six histories;
- `main(argv)` with `make_setup`, `find_kicks` and `trace_history` stubbed: both files written,
  `--arrays`, a failed history, the tuning options passed on, the title, and the refusals
  (--T-stop-GeV among them, jordan-normalization prompt 08);
- `trace_history` on one real history, beta = 3.0, tuned to T_CMB (jordan-normalization prompt
  08: four integrations, 33 s at load 5, 40 s in the probe at load 6-9; no PRyMordial): not
  captured, and its first bounce within 1e-6 relative of the committed fine scan's row (that
  scan was untuned, at Lambda = 1 meV, stopped at 1 MeV, and on the units before prompt 05; the
  orchestrator's probe measured 1.1e-7).

No Ray and no store. Every test runs from the repository root, as the suites do.
"""

import contextlib
import csv
import io
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np

import tools.plot_surfing_histories as psh

ROOT = Path(__file__).resolve().parents[2]
NEAR3_CSV = (
    ROOT / ".documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv"
)


def make_trace(beta, kick=None, bounce_N=20.0, turning_points=()):
    """A synthetic history to its first bounce: T_J falling from 100 to 2 GeV, phi from 5 to 0."""
    N = np.linspace(0.0, bounce_N, 201)
    T = 100.0 * np.exp(-N / bounce_N * np.log(50.0))
    phi = 5.0 * (1.0 - N / bounce_N)
    pi = -np.ones_like(N) / beta * (1.0 - 0.5 * N / bounce_N)
    return psh.HistoryTrace(
        beta=beta,
        N=N,
        phi_Mp=phi,
        pi_Mp=pi,
        T_J_GeV=T,
        g=1.0 + beta * pi,
        turning_points=list(turning_points),
        bounce_N=bounce_N,
        bounce_T_GeV=float(T[-1]),
        capture_kick=kick,
    )


def _lines(ax, gid):
    return [line for line in ax.get_lines() if line.get_gid() == gid]


@contextlib.contextmanager
def _in_directory(path):
    here = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(here)


class TestBuildFigure(unittest.TestCase):
    def setUp(self):
        self.traces = [
            make_trace(3.0),
            make_trace(3.02, kick="EW", turning_points=[(12.0, 50.0, "min")]),
        ]
        self.fig = psh.build_figure(
            self.traces, [("EW", 53.0), ("QCD", 0.18)], "title", N_min=1.0
        )
        self.ax_T, self.ax_phi, self.ax_g = self.fig.axes

    def test_a_one_line_per_history_in_each_panel(self):
        for ax, gid in (
            (self.ax_T, "history-T"),
            (self.ax_phi, "history-phi"),
            (self.ax_g, "history-g"),
        ):
            lines = _lines(ax, gid)
            self.assertEqual(len(lines), 2)
            np.testing.assert_array_equal(lines[0].get_xdata(), self.traces[0].N)
        np.testing.assert_array_equal(
            _lines(self.ax_T, "history-T")[1].get_ydata(), self.traces[1].T_J_GeV
        )
        np.testing.assert_array_equal(
            _lines(self.ax_g, "history-g")[0].get_ydata(), self.traces[0].g
        )

    def test_b_captured_is_dashed_and_not_captured_is_solid(self):
        styles = [line.get_linestyle() for line in _lines(self.ax_T, "history-T")]
        self.assertEqual(styles, ["-", "--"])
        labels = [t.get_text() for t in self.fig.legends[0].get_texts()]
        self.assertEqual(
            labels, ["β = 3.000, not captured", "β = 3.020, captured (EW)"]
        )

    def test_c_turning_points_and_bounces_are_marked(self):
        marks = _lines(self.ax_T, "turning-point")
        self.assertEqual(len(marks), 1)
        self.assertEqual(marks[0].get_marker(), "v")
        self.assertEqual(list(marks[0].get_xdata()), [12.0])
        for ax in (self.ax_T, self.ax_phi, self.ax_g):
            bounces = _lines(ax, "bounce")
            self.assertEqual(len(bounces), 2)
            self.assertEqual(list(bounces[0].get_xdata()), [20.0])

    def test_d_scales_limits_and_kick_peaks(self):
        self.assertEqual(self.ax_T.get_yscale(), "log")
        self.assertEqual(self.ax_g.get_yscale(), "symlog")
        self.assertEqual(self.ax_T.get_xlim()[0], 1.0)
        lo, hi = self.ax_T.get_ylim()
        self.assertLess(lo, 2.0)
        self.assertGreater(hi, 100.0 * np.exp(-np.log(50.0) / 20.0))
        # EW's 53 GeV is inside the range and drawn; QCD's 0.18 GeV is not
        peaks = _lines(self.ax_T, "kick-peak")
        self.assertEqual(len(peaks), 1)
        self.assertEqual(list(peaks[0].get_ydata()), [53.0, 53.0])

    def test_e_more_than_six_histories_are_refused(self):
        with self.assertRaises(ValueError):
            psh.build_figure([make_trace(3.0 + k) for k in range(7)], [], "title")


class TestMain(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        GeV = 2.0
        self.setup = SimpleNamespace(
            cosmology=SimpleNamespace(
                _eos=SimpleNamespace(_T_min=1.0e-5, _T_max=2.5e4)
            ),
            units=SimpleNamespace(GeV=GeV),
        )
        self.kicks = [{"label": "EW", "T_peak": 53.0 * GeV}]

    def _patches(self, fake_trace):
        return (
            mock.patch.object(psh, "make_setup", return_value=self.setup),
            mock.patch.object(psh, "find_kicks", return_value=self.kicks),
            mock.patch.object(psh, "trace_history", side_effect=fake_trace),
        )

    def test_a_writes_both_files_and_the_arrays(self):
        calls = []

        def fake(setup, kicks, beta, phi, M, T_window, **tuning):
            calls.append((T_window, tuning))
            return make_trace(beta), None

        out = self.dir / "near3"
        p1, p2, p3 = self._patches(fake)
        with _in_directory(ROOT), p1, p2, p3, contextlib.redirect_stdout(io.StringIO()):
            status = psh.main(
                ["--beta-values", "3.0", "3.01", "--out", str(out), "--arrays"]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            sorted(os.listdir(self.dir)),
            [
                "near3.pdf",
                "near3.png",
                "near3_beta3.000.npz",
                "near3_beta3.010.npz",
            ],
        )
        arrays = np.load(self.dir / "near3_beta3.000.npz")
        self.assertEqual(sorted(arrays.files), ["N", "T_J_GeV", "phi_Mp", "pi_Mp"])
        np.testing.assert_array_equal(arrays["phi_Mp"], make_trace(3.0).phi_Mp)
        # the window's edge and the tuning options reach trace_history (jordan-normalization
        # prompt 08)
        self.assertEqual(
            calls, [(1e-3, {"tolerance": 1e-5, "max_integrations": 30})] * 2
        )

    def test_b_a_failed_history_is_reported_and_the_rest_drawn(self):
        def fake(setup, kicks, beta, phi, M, T_window, **tuning):
            if beta == 3.01:
                return None, "stub failure"
            return make_trace(beta), None

        out = self.dir / "partial"
        p1, p2, p3 = self._patches(fake)
        printed = io.StringIO()
        with _in_directory(ROOT), p1, p2, p3, contextlib.redirect_stdout(printed):
            status = psh.main(["--beta-values", "3.0", "3.01", "--out", str(out)])
        self.assertEqual(status, 1)
        self.assertIn("stub failure", printed.getvalue())
        self.assertTrue(Path(str(out) + ".png").exists())

    def test_c_refused_outside_the_root_and_beyond_six(self):
        out = self.dir / "refused"
        with _in_directory(self.dir), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(psh.main(["--out", str(out)]), 2)
        betas = [str(3.0 + 0.001 * k) for k in range(7)]
        with _in_directory(ROOT), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(psh.main(["--beta-values", *betas, "--out", str(out)]), 2)
        # --T-stop-GeV, removed by jordan-normalization prompt 08, is refused by name
        err = io.StringIO()
        with _in_directory(ROOT), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as ctx:
                psh.main(["--T-stop-GeV", "1e-3", "--out", str(out)])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(
            "--T-stop-GeV was removed by jordan-normalization prompt 08", err.getvalue()
        )
        self.assertIn("--T-window-low-GeV", err.getvalue())
        self.assertEqual(os.listdir(self.dir), [])

    def test_d_the_title_gives_the_tuning_and_no_Lambda(self):
        title = psh._title(5.0, 1e-3, 1e-5)
        self.assertIn("tuned to H0 at T_CMB, tolerance 1e-05", title)
        self.assertNotIn("Λ", title)
        self.assertIn("φ* = 5 M_P, M = 0.001 M_P", title)


class TestRealHistory(unittest.TestCase):
    def test_beta_3_is_not_captured_and_bounces_as_the_committed_scan(self):
        """
        One real history at beta = 3.0, tuned to T_CMB (four integrations, 33 s at load 5, 40 s
        in the probe at load 6-9; no PRyMordial). Its first bounce is within 1e-6 relative of the
        committed scan's row: the scan was untuned, at Lambda = 1 meV, stopped at 1 MeV, on the
        units before jordan-normalization prompt 05. Before the window V0 is negligible, and a
        constant phi_ref rescales A, to which the dynamics there are blind. The orchestrator's
        probe for prompt 08 measured 1.1e-7 (T_deliver). Before prompt 08: one untuned
        integration, within 1e-7.
        """
        with open(NEAR3_CSV, newline="") as f:
            row = next(r for r in csv.DictReader(f) if float(r["beta"]) == 3.0)
        with _in_directory(ROOT):
            setup = psh.make_setup()
            units = setup.units
            kicks = psh.find_kicks(
                setup.cosmology,
                setup.cosmology._eos._T_min * units.GeV,
                setup.cosmology._eos._T_max * units.GeV,
            )
            trace, reason = psh.trace_history(
                setup, kicks, 3.0, 5.0, 1e-3, 1e-3, n_samples=2001
            )
        self.assertIsNone(reason)
        self.assertIsNone(trace.capture_kick)
        self.assertEqual(row["captured"], "false")
        self.assertEqual(trace.turning_points, [])
        self.assertLessEqual(
            abs(trace.bounce_T_GeV / float(row["T_deliver_GeV"]) - 1.0), 1e-6
        )
        self.assertLessEqual(abs(trace.bounce_N / float(row["N_deliver"]) - 1.0), 1e-6)
        self.assertEqual(trace.N[-1], trace.bounce_N)
        self.assertAlmostEqual(trace.T_J_GeV[-1], trace.bounce_T_GeV, delta=1e-9)
        self.assertAlmostEqual(trace.phi_Mp[0], 5.0, delta=1e-12)
        # the stall: g stays positive, and its minimum is the scan's near miss
        self.assertGreater(float(np.min(trace.g)), 0.0)
        self.assertLess(float(np.min(trace.g)), 0.03)


if __name__ == "__main__":
    unittest.main()
