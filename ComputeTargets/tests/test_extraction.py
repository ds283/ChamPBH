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
Extraction and the science figures (science-readiness prompt 07, README sections 2 (k), 6.8).

(a) relative_shift; (b) running_band; (d) kick_threshold_curve; (e) the four figures and
histories.csv, built from synthetic records into a temporary directory, with the fixed-T
values read from ScalarModel.fixed_T_values and no sample loaded; (f) plot_by_beta.py reads
the stored columns, keeps _do_not_populate on its lookups, and the parser has
--band-half-width; the max |Q| caveat.

Test (c) of the original prompt, value_at_T_Jordan, is withdrawn: the fixed-T values are
stored on the ScalarModel row (prompt 06b) and are not interpolated from samples.

No Ray cluster, no datastore, no PRyMordial solve. A few seconds (matplotlib).
"""

import ast
import csv
import tempfile
import unittest
from math import exp, isnan, log, sqrt
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from ComputeTargets.ScalarModel import FixedTValues
from extract_common import (
    CSV_COLUMNS,
    adiabatic_Q_caption,
    _fixed_T_figure,
    build_history_record,
    kick_threshold_curve,
    plot_abundance_shifts,
    plot_convergence_in_M,
    plot_fixed_T,
    plot_T_deliver,
    relative_shift,
    running_band,
    write_histories_csv,
)

ROOT = Path(__file__).resolve().parents[2]


class TestRelativeShift(unittest.TestCase):
    def test_a_relative_shift(self):
        self.assertAlmostEqual(relative_shift(1.1, 1.0), 0.1, places=14)
        self.assertAlmostEqual(relative_shift(0.9, 2.0), -0.55, places=14)
        self.assertEqual(relative_shift(2.5, 2.5), 0.0)
        for value, baseline in [
            (None, 1.0),
            (1.0, None),
            (1.0, 0.0),
            (float("nan"), 1),
        ]:
            self.assertTrue(isnan(relative_shift(value, baseline)))


class TestRunningBand(unittest.TestCase):
    def test_b_known_distribution_at_the_centre(self):
        # 101 points, y = x on [0, 1]: a window covering everything, centred on x = 0.5,
        # has median 0.5 and, by linear interpolation of 101 uniform points, percentiles
        # 0.16 and 0.84
        x = np.linspace(0.0, 1.0, 101)
        median, lower, upper = running_band(x, x, 0.5)
        self.assertAlmostEqual(median[50], 0.5, delta=1e-12)
        self.assertAlmostEqual(lower[50], 0.16, delta=1e-12)
        self.assertAlmostEqual(upper[50], 0.84, delta=1e-12)

    def test_b_empty_window_is_nan(self):
        median, lower, upper = running_band([0.0, 1.0], [np.nan, 2.0], 0.25)
        # the first point's window holds only a non-finite y
        for out in (median, lower, upper):
            self.assertTrue(isnan(out[0]))
        self.assertEqual(median[1], 2.0)

        median, _, _ = running_band([], [], 0.25)
        self.assertEqual(len(median), 0)

    def test_b_window_edges_are_inclusive(self):
        # dyadic x so that x +- h is exact: at x = 0.5 and h = 0.25 the window is [0.25, 0.75],
        # holding the points at 0.25, 0.5 and 0.75 with y = 0, 10, 100 (not the neighbours)
        x = [0.0, 0.25, 0.5, 0.75, 1.0]
        y = [1000.0, 0.0, 10.0, 100.0, 2000.0]
        median, lower, upper = running_band(x, y, 0.25)
        self.assertEqual(median[2], 10.0)
        self.assertAlmostEqual(lower[2], np.percentile([0.0, 10.0, 100.0], 16), 12)
        self.assertAlmostEqual(upper[2], np.percentile([0.0, 10.0, 100.0], 84), 12)
        # just inside the window excludes both edges
        median_narrow, _, _ = running_band(x, y, 0.2499)
        self.assertEqual(median_narrow[2], 10.0)
        _, lower_narrow, upper_narrow = running_band(x, y, 0.2499)
        self.assertEqual(lower_narrow[2], 10.0)
        self.assertEqual(upper_narrow[2], 10.0)


class TestKickThresholdCurve(unittest.TestCase):
    def test_d_beta_th_and_omitted_points(self):
        # Sigma = 1 - 3w: w = 11/36 at T = 1 gives Sigma = 1/12 and beta_th = 1/sqrt(3/12) = 2;
        # at T = 2 w = 0.4 gives Sigma < 0; at T = 3 w = 1/3 gives Sigma = 0. Both omitted.
        table = {1.0: 11.0 / 36.0, 2.0: 0.4, 3.0: 1.0 / 3.0, 4.0: 0.0}
        cosmology = SimpleNamespace(w=lambda T: table[T])
        T, beta_th = kick_threshold_curve(cosmology, [1.0, 2.0, 3.0, 4.0])
        self.assertEqual(T, [1.0, 4.0])
        self.assertAlmostEqual(beta_th[0], 2.0, delta=1e-14)
        self.assertAlmostEqual(beta_th[1], 1.0 / sqrt(3.0), delta=1e-14)


def _units():
    from Units import Planck_units

    return Planck_units()


class _NoSamples(SimpleNamespace):
    """
    A stand-in stored object whose sample values cannot be read, as a ScalarModel or BBNData
    built with _do_not_populate: touching `values` raises. The record builder must not.
    """

    @property
    def values(self):
        raise RuntimeError("values have not yet been populated")


def _synthetic_records(units):
    """Two (M, Lambda) sets of histories from stand-in stored objects."""
    baseline = {"Yp_BBN": 0.2470, "DOverH": 2.55}
    records = []
    for M_Mp in (1e-3, 0.5):
        for k in range(40):
            beta = 1.0 + 0.05 * k
            scalar = _NoSamples(
                extra_metadata={"reflections": k % 3} if k % 3 else None,
                first_bounce=(
                    None
                    if k == 7
                    else SimpleNamespace(
                        log_T_Jordan=log(0.7 * units.GeV), reflected=(k % 5 == 0)
                    )
                ),
                # as ScalarModel.fixed_T_values: phi in the cosmology's units, a pair None
                # where the temperature is not reached
                fixed_T_values=FixedTValues(
                    phi_Einstein_1MeV=1e-3 * beta * units.PlanckMass,
                    density_NP_ratio_1MeV=2e-3 * beta,
                    phi_Einstein_70keV=(
                        None if k == 11 else 1e-4 * beta * units.PlanckMass
                    ),
                    density_NP_ratio_70keV=None if k == 11 else 3e-3 * beta,
                ),
            )
            bbn = _NoSamples(
                Yp_BBN=0.2470 * (1.0 + 0.01 * beta),
                DOverH=2.55 * (1.0 - 0.02 * beta + 0.002 * ((-1) ** k)),
                He3OverH=1.04,
                Li7OverH=5.3,
            )
            records.append(
                build_history_record(
                    beta=beta,
                    M_Mp=M_Mp,
                    Lambda_eV=1e-3,
                    phi_init_Mp=5.0,
                    scalar=scalar,
                    bbn=bbn,
                    baseline=baseline,
                    units=units,
                )
            )
    # a history whose ScalarModel failed: no values, a failure reason
    records.append(
        build_history_record(
            beta=9.0,
            M_Mp=0.5,
            Lambda_eV=1e-3,
            phi_init_Mp=5.0,
            scalar=None,
            bbn=None,
            baseline=baseline,
            units=units,
            failure_reasons=["integration: step size underflow"],
        )
    )
    return records


class TestFigures(unittest.TestCase):
    def test_e_records_carry_the_fixed_T_values(self):
        units = _units()
        records = _synthetic_records(units)
        r = records[3]
        beta = r["beta"]
        # read from fixed_T_values, with no sample loaded (the stand-ins raise on `values`)
        self.assertAlmostEqual(r["phi_1MeV_Mp"], 1e-3 * beta, delta=1e-15)
        self.assertAlmostEqual(r["rho_ratio_1MeV"], 2e-3 * beta, delta=1e-15)
        self.assertAlmostEqual(r["phi_70keV_Mp"], 1e-4 * beta, delta=1e-16)
        self.assertAlmostEqual(r["rho_ratio_70keV"], 3e-3 * beta, delta=1e-15)
        self.assertAlmostEqual(r["T_deliver_GeV"], 0.7, delta=1e-12)
        self.assertAlmostEqual(
            r["delta_Yp"], 0.2470 * (1.0 + 0.01 * beta) / 0.2470 - 1.0, delta=1e-12
        )
        # a temperature the history did not reach is NaN in both of its fields only
        unreached = records[11]
        self.assertTrue(isnan(unreached["phi_70keV_Mp"]))
        self.assertTrue(isnan(unreached["rho_ratio_70keV"]))
        self.assertFalse(isnan(unreached["phi_1MeV_Mp"]))
        self.assertFalse(isnan(unreached["rho_ratio_1MeV"]))
        failed = records[-1]
        self.assertEqual(failed["failure_reasons"], "integration: step size underflow")
        self.assertTrue(isnan(failed["Yp_BBN"]))
        self.assertTrue(isnan(failed["T_deliver_GeV"]))

    def test_e_the_four_figures_and_the_csv(self):
        units = _units()
        records = _synthetic_records(units)
        kick = ([0.5, 0.7, 1.0], [3.0, 2.2, 1.9])
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self.assertTrue(
                plot_abundance_shifts(records, out / "shifts.pdf", 0.025, "synthetic")
            )
            self.assertTrue(
                plot_convergence_in_M(records, out / "conv.pdf", 0.025, "synthetic")
            )
            self.assertTrue(
                plot_T_deliver(records, out / "T_deliver.pdf", kick, "synthetic")
            )
            self.assertTrue(plot_fixed_T(records, out / "fixed_T.pdf", "synthetic"))
            write_histories_csv(records, out / "histories.csv")

            for stem in ("shifts", "conv", "T_deliver", "fixed_T"):
                for suffix in (".pdf", ".png"):
                    path = out / f"{stem}{suffix}"
                    self.assertTrue(path.is_file(), path)
                    self.assertGreater(path.stat().st_size, 1000)

            with open(out / "histories.csv", newline="") as f:
                rows = list(csv.reader(f))
            self.assertEqual(rows[0], CSV_COLUMNS)
            self.assertEqual(len(rows) - 1, len(records))
            last = dict(zip(rows[0], rows[-1]))
            self.assertEqual(
                last["failure_reasons"], "integration: step size underflow"
            )
            self.assertEqual(last["Yp_BBN"], "")

    def test_e_figure_4_keeps_negative_ratios(self):
        # rho_NP/rho_R,J is negative at 1 MeV on real histories (log 06b: -4.8e-2 at
        # beta = 2, M = 0.5): every finite point must reach the axes, on a signed scale
        units = _units()
        records = _synthetic_records(units)
        for r in records:
            if np.isfinite(r["rho_ratio_1MeV"]):
                r["rho_ratio_1MeV"] = -r["rho_ratio_1MeV"]
        n_finite = sum(1 for r in records if np.isfinite(r["rho_ratio_1MeV"]))
        fig = _fixed_T_figure(records, "synthetic")
        ratio_axes = fig.axes[1]
        self.assertEqual(ratio_axes.get_yscale(), "symlog")
        line = [ln for ln in ratio_axes.lines if ln.get_label() == "$T_J$ = 1MeV"][0]
        y = np.asarray(line.get_ydata())
        self.assertEqual(len(y), n_finite)
        self.assertTrue(np.all(y < 0.0))

    def test_e_a_figure_with_nothing_to_plot_is_skipped(self):
        units = _units()
        records = [
            build_history_record(
                beta=1.0,
                M_Mp=0.5,
                Lambda_eV=1e-3,
                phi_init_Mp=5.0,
                scalar=None,
                bbn=None,
                baseline=None,
                units=units,
            )
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self.assertFalse(plot_abundance_shifts(records, out / "a.pdf"))
            self.assertFalse(plot_T_deliver(records, out / "b.pdf"))
            self.assertFalse(plot_fixed_T(records, out / "c.pdf"))
            self.assertEqual(list(out.iterdir()), [])


class TestDriverReadsTheColumns(unittest.TestCase):
    def test_f_parser_option(self):
        from config.argument_parser import create_argument_parser

        parser = create_argument_parser()
        args = parser.parse_args(["--database", "unused.db"])
        self.assertEqual(args.band_half_width, 0.025)
        args = parser.parse_args(
            ["--database", "unused.db", "--band-half-width", "0.1"]
        )
        self.assertEqual(args.band_half_width, 0.1)

    def test_f_plot_by_beta_uses_the_new_functions(self):
        # plot_by_beta.py parses sys.argv and starts Ray at import: read it with ast
        tree = ast.parse((ROOT / "plot_by_beta.py").read_text())
        called = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        for name in (
            "build_history_record",
            "plot_abundance_shifts",
            "plot_convergence_in_M",
            "plot_T_deliver",
            "plot_fixed_T",
            "write_histories_csv",
            "kick_threshold_curve",
            "adiabatic_Q_caption",
        ):
            self.assertIn(name, called)

        attributes = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        self.assertIn("band_half_width", attributes)

        # every lookup keeps _do_not_populate: the same four literals as on the commit before
        # this prompt (git show HEAD~1:plot_by_beta.py | grep -c _do_not_populate); the
        # fixed-T values are read from the ScalarModel row, not from the samples
        source = (ROOT / "plot_by_beta.py").read_text()
        self.assertEqual(source.count('"_do_not_populate": True'), 4)
        self.assertNotIn("value_at_T_Jordan", source)

    def test_f_adiabatic_caveat(self):
        for M in (1e-3, 1e-5):
            self.assertIn(
                "post-adiabatic-Q-reads-aliased-late-samples", adiabatic_Q_caption(M)
            )
            self.assertIn("aliased late samples", adiabatic_Q_caption(M))
        for M in (1e-2, 0.5):
            self.assertIsNone(adiabatic_Q_caption(M))


if __name__ == "__main__":
    unittest.main()
