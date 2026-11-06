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
The two figures of inelastic-reflection prompt 10 and histories.csv (tests 5, 6 and 7; R27, R28).

(5) `plot_Delta_N_eff` on constructed records: alpha = 0.03 and 0.1 at three beta each, one with a
    NULL at 1 MeV, and a record at alpha = 0 among them: two curves per panel, in increasing alpha,
    the NULL left out of its panel only, the alpha = 0 record never drawn, log axes, the labels of
    R22 and P7, the file written; a value that is not positive is left out and counted; alpha = 0
    alone writes nothing and returns False;
(6) `histories.csv` has `alpha`, `Delta_N_eff_today` and `Delta_N_eff_1MeV` in its header, sorts by
    (alpha, M, n, beta), and writes a missing value empty;
(7) `plot_dark_radiation` on a dissipation history stored and read back on a temporary store (09's
    harness and fixture; M = 1e-3 M_P, beta = 1.2, alpha = 0.1, which parks at N = 28.37 and is
    released at N = 45.91): the file is written, the plotted Delta g* equals
    `gstar_rho * rho_chi_over_rho_rad` per sample bitwise (read back from the axes), samples with a
    ratio of 0.0 are left out, two vertical lines stand at the parking and the release (exactly, in
    e-folds; at the first sample at or after each N, in redshift), and at alpha = 0 nothing is
    written and the function returns False;
(8) the record of that history carries the further dissipation columns (added 2026-11-01): the
    parking and release temperatures as exp(ln T_J) in GeV, the injections, the injected fraction
    and the failing shares as the record has them, in the header just before failure_reasons.

Tests 7 and 8 integrate two histories (about 4 s each unloaded, 8-10 s at a load of 8) and draws the
figure three times; no Ray, no PRyMordial. Tests 5 and 6 are instant.
"""

import contextlib
import csv
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import extract_common as ec
from ComputeTargets.tests import dissipation_histories as dh
from ObjectFactories.tests import test_dissipation_store as tds


def _record(alpha, beta, today, one_MeV, M_Mp=0.5, n=1):
    """A record as build_history_record makes it for a history that is read back."""
    record = ec.build_history_record(
        beta=beta,
        M_Mp=M_Mp,
        n=n,
        phi_init_Mp=5.0,
        alpha=alpha,
        drain_model="fixed",
        scalar=None,
        bbn=None,
        baseline=None,
        units=None,
    )
    record["Delta_N_eff_today"] = today
    record["Delta_N_eff_1MeV"] = one_MeV
    return record


def _draw(function, *args):
    """(the function's value, the figure it saved, the lines it printed), the figure kept."""
    figures = []
    real_save = ec._save

    def spy(fig, path):
        figures.append(fig)
        return real_save(fig, path)

    out = io.StringIO()
    with mock.patch.object(ec, "_save", spy), contextlib.redirect_stdout(out):
        value = function(*args)
    return value, (figures[0] if figures else None), out.getvalue().splitlines()


def _curves(ax):
    return {
        line.get_label(): (list(line.get_xdata()), list(line.get_ydata()))
        for line in ax.lines
    }


class TestDeltaNEffFigure(unittest.TestCase):
    def records(self):
        return [
            _record(0.1, 1.0, 0.060, 0.056),
            _record(0.1, 2.0, 0.050, 0.047),
            _record(0.1, 5.0, 0.040, float("nan")),  # did not reach 1 MeV: NULL
            _record(0.03, 5.0, 0.012, 0.011),  # out of order on purpose
            _record(0.03, 1.0, 0.020, 0.019),
            _record(0.03, 2.0, 0.015, 0.014),
            _record(0.0, 1.0, 0.0, 0.0),  # natural mode: never a curve
            _record(0.0, 2.0, 0.0, 0.0),
        ]

    def test_5_two_curves_per_panel_the_null_left_out_the_file_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plots" / "Delta_N_eff.pdf"
            wrote, fig, lines = _draw(
                ec.plot_Delta_N_eff, self.records(), path, "title"
            )
            self.assertIs(wrote, True)
            self.assertTrue(path.is_file())
            self.assertTrue(path.with_suffix(".png").is_file())

        upper, lower = fig.axes
        self.assertEqual(upper.get_yscale(), "log")
        self.assertEqual(lower.get_yscale(), "log")
        # one curve per alpha > 0, in increasing alpha; alpha = 0 is not one
        self.assertEqual(len(upper.lines), 2)
        self.assertEqual(len(lower.lines), 2)
        self.assertEqual(
            [l.get_label() for l in upper.lines],
            [r"$\alpha$ = 0.03", r"$\alpha$ = 0.1"],
        )
        for line in (*upper.lines, *lower.lines):
            self.assertNotEqual(line.get_marker(), "None")
        # each curve in increasing beta; the NULL left out of the lower panel only
        up = _curves(upper)
        lo = _curves(lower)
        self.assertEqual(
            up[r"$\alpha$ = 0.03"], ([1.0, 2.0, 5.0], [0.020, 0.015, 0.012])
        )
        self.assertEqual(
            up[r"$\alpha$ = 0.1"], ([1.0, 2.0, 5.0], [0.060, 0.050, 0.040])
        )
        self.assertEqual(
            lo[r"$\alpha$ = 0.03"], ([1.0, 2.0, 5.0], [0.019, 0.014, 0.011])
        )
        self.assertEqual(lo[r"$\alpha$ = 0.1"], ([1.0, 2.0], [0.056, 0.047]))
        # the labels: P7's (11/4)^{4/3} above, R22's T_nu = T_gamma below
        self.assertIn("(11/4)^{4/3}", upper.get_ylabel())
        self.assertIn(r"T_\nu = T_\gamma", lower.get_ylabel())
        self.assertEqual(lower.get_xlabel(), r"coupling $\beta$")
        # the shared beta axis
        self.assertIs(upper.get_shared_x_axes().joined(upper, lower), True)
        # one printed line says how many were left out of each panel
        counted = [l for l in lines if l.startswith("@@ plot_Delta_N_eff: left out")]
        self.assertEqual(len(counted), 1)
        self.assertIn("0 from the T_CMB panel, 1 from the 1 MeV panel", counted[0])

    def test_5_a_value_that_is_not_positive_is_left_out_and_counted(self):
        records = [
            _record(0.1, 1.0, 0.06, 0.0),
            _record(0.1, 2.0, -1.0e-3, 0.04),
            _record(0.1, 3.0, 0.05, 0.03),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            wrote, fig, lines = _draw(
                ec.plot_Delta_N_eff, records, Path(tmp) / "d.pdf", ""
            )
        self.assertTrue(wrote)
        upper, lower = fig.axes
        self.assertEqual(_curves(upper)[r"$\alpha$ = 0.1"][0], [1.0, 3.0])
        self.assertEqual(_curves(lower)[r"$\alpha$ = 0.1"][0], [2.0, 3.0])
        self.assertTrue(
            any("1 from the T_CMB panel, 1 from the 1 MeV" in l for l in lines)
        )

    def test_5_alpha_zero_alone_writes_nothing(self):
        records = [r for r in self.records() if r["alpha"] == 0.0]
        self.assertEqual(len(records), 2)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            wrote, fig, lines = _draw(ec.plot_Delta_N_eff, records, out / "d.pdf", "")
            self.assertIs(wrote, False)
            self.assertIsNone(fig)
            self.assertEqual(list(out.iterdir()), [])
        self.assertEqual(len(lines), 1)
        self.assertIn("no record has alpha > 0", lines[0])


class TestHistoriesCsv(unittest.TestCase):
    def test_6_the_three_columns_and_the_sort(self):
        self.assertEqual(
            ec.CSV_COLUMNS[ec.CSV_COLUMNS.index("phi_init_Mp") + 1], "alpha"
        )
        self.assertEqual(
            ec.CSV_COLUMNS[
                ec.CSV_COLUMNS.index("rho_ratio_70keV")
                + 1 : ec.CSV_COLUMNS.index("rho_ratio_70keV")
                + 3
            ],
            ["Delta_N_eff_today", "Delta_N_eff_1MeV"],
        )
        records = [
            _record(0.1, 2.0, 0.05, 0.04, M_Mp=0.5, n=1),
            _record(0.0, 3.0, 0.0, 0.0, M_Mp=0.5, n=1),
            _record(0.1, 1.0, 0.06, float("nan"), M_Mp=0.5, n=1),
            _record(0.03, 5.0, 0.02, 0.01, M_Mp=1e-3, n=1),
            _record(0.0, 1.0, 0.0, 0.0, M_Mp=1e-3, n=2),
            _record(0.0, 1.0, 0.0, 0.0, M_Mp=1e-3, n=1),
            _record(0.0, 2.0, 0.0, 0.0, M_Mp=0.5, n=1),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "histories.csv"
            ec.write_histories_csv(records, path)
            with open(path, newline="") as f:
                rows = list(csv.reader(f))
        header = rows[0]
        self.assertEqual(header, ec.CSV_COLUMNS)
        for name in ("alpha", "Delta_N_eff_today", "Delta_N_eff_1MeV"):
            self.assertIn(name, header)
        cells = [dict(zip(header, row)) for row in rows[1:]]
        # sorted by (alpha, M, n, beta)
        self.assertEqual(
            [(c["alpha"], c["M_Mp"], c["n"], c["beta"]) for c in cells],
            [
                ("0.0", "0.001", "1", "1.0"),
                ("0.0", "0.001", "2", "1.0"),
                ("0.0", "0.5", "1", "2.0"),
                ("0.0", "0.5", "1", "3.0"),
                ("0.03", "0.001", "1", "5.0"),
                ("0.1", "0.5", "1", "1.0"),
                ("0.1", "0.5", "1", "2.0"),
            ],
        )
        by = {(c["alpha"], c["beta"], c["M_Mp"]): c for c in cells}
        self.assertEqual(by[("0.1", "2.0", "0.5")]["Delta_N_eff_today"], "0.05")
        self.assertEqual(by[("0.1", "2.0", "0.5")]["Delta_N_eff_1MeV"], "0.04")
        # a NULL is an empty cell
        self.assertEqual(by[("0.1", "1.0", "0.5")]["Delta_N_eff_1MeV"], "")
        self.assertEqual(by[("0.1", "1.0", "0.5")]["Delta_N_eff_today"], "0.06")

    def test_6_the_record_takes_alpha_from_the_caller_and_delta_N_eff_from_the_history(
        self,
    ):
        from types import SimpleNamespace

        dissipation = SimpleNamespace(Delta_N_eff_today=0.07, Delta_N_eff_1MeV=None)
        for scalar, today, one_MeV in (
            (None, None, None),
            (SimpleNamespace(dissipation=None), None, None),
            (SimpleNamespace(dissipation=dissipation), 0.07, None),
        ):
            with self.subTest(scalar=scalar):
                if scalar is not None:
                    scalar.tuning = mock.MagicMock()
                    scalar.extra_metadata = None
                    scalar.first_bounce = None
                    scalar.fixed_T_values = mock.MagicMock(
                        phi_Einstein_1MeV=None,
                        density_NP_ratio_1MeV=None,
                        phi_Einstein_70keV=None,
                        density_NP_ratio_70keV=None,
                    )
                units = mock.MagicMock()
                record = ec.build_history_record(
                    beta=1.0,
                    M_Mp=0.5,
                    n=1,
                    phi_init_Mp=5.0,
                    alpha=0.1,
                    drain_model="fixed",
                    scalar=scalar,
                    bbn=None,
                    baseline=None,
                    units=units,
                )
                self.assertEqual(record["alpha"], 0.1)
                for key, expected in (
                    ("Delta_N_eff_today", today),
                    ("Delta_N_eff_1MeV", one_MeV),
                ):
                    if expected is None:
                        self.assertTrue(np.isnan(record[key]), key)
                    else:
                        self.assertEqual(record[key], expected)


class TestDarkRadiationFigure(tds._DissipationStoreCase):
    KEY = (1.2, 1e-3, 0.1)

    def stored(self, key):
        self.write(dh.payload(*key), *key)
        return self.read(*key)

    def test_7_the_file_the_delta_gstar_the_lines(self):
        got = self.stored(self.KEY)
        dissipation = got.dissipation
        # the history parks and is released, so both lines are drawn
        self.assertIsNotNone(dissipation.parking_N)
        self.assertIsNotNone(dissipation.release_N)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plots" / "dark_radiation.pdf"
            wrote, fig, _ = _draw(
                ec.plot_dark_radiation, got, "QCD_Cosmology", path, "efolds"
            )
            self.assertIs(wrote, True)
            self.assertTrue(path.is_file())
            self.assertTrue(path.with_suffix(".png").is_file())

        gstar_ax, ratio_ax = fig.axes
        self.assertEqual(gstar_ax.get_yscale(), "log")
        self.assertEqual(ratio_ax.get_yscale(), "log")

        values = got.values
        positive = [v for v in values if v.rho_chi_over_rho_rad > 0.0]
        # the ratio is 0.0 before the first drain: those samples are left out
        self.assertGreater(len(positive), 0)
        self.assertLess(len(positive), len(values))

        # the plotted Delta g* is gstar_rho * rho_chi_over_rho_rad, bitwise, sample by sample
        expected = [v.gstar_rho * v.rho_chi_over_rho_rad for v in positive]
        plotted = list(gstar_ax.lines[0].get_ydata())
        self.assertEqual(len(plotted), len(expected))
        for k, (got_y, want_y) in enumerate(zip(plotted, expected)):
            self.assertTrue(got_y == want_y, f"sample {k}: {got_y!r} != {want_y!r}")
        # ... and rho_chi/rho_R below
        ratios = list(ratio_ax.lines[0].get_ydata())
        self.assertEqual(len(ratios), len(positive))
        for k, (got_y, v) in enumerate(zip(ratios, positive)):
            self.assertTrue(got_y == v.rho_chi_over_rho_rad, f"sample {k}")
        # the x of each point is the sample's N
        self.assertEqual(
            list(gstar_ax.lines[0].get_xdata()), [v.raw_N for v in positive]
        )
        self.assertNotEqual(
            list(gstar_ax.lines[0].get_ydata()), list(ratio_ax.lines[0].get_ydata())
        )

        # two vertical lines, at the parking and the release, exactly, on both panels
        marked = {
            l.get_label().split(",")[0]: list(l.get_xdata())
            for l in gstar_ax.lines
            if l.get_label().startswith(("parking", "release"))
        }
        self.assertEqual(set(marked), {"parking", "release"})
        self.assertEqual(marked["parking"], [dissipation.parking_N] * 2)
        self.assertEqual(marked["release"], [dissipation.release_N] * 2)
        for ax in (gstar_ax, ratio_ax):
            at = [
                list(l.get_xdata())
                for l in ax.lines
                if l.get_linestyle() == "--"
                and list(l.get_xdata())
                in ([dissipation.parking_N] * 2, [dissipation.release_N] * 2)
            ]
            self.assertEqual(len(at), 2)
        self.assertLess(dissipation.parking_N, dissipation.release_N)

        # the title says alpha
        text = " ".join(t.get_text() for t in fig.texts)
        self.assertIn("Cosmology: QCD_Cosmology, α = 0.1", text)

    def test_7_in_redshift_a_line_is_at_the_first_sample_at_or_after_its_n(self):
        got = self.stored(self.KEY)
        dissipation = got.dissipation
        with tempfile.TemporaryDirectory() as tmp:
            wrote, fig, _ = _draw(
                ec.plot_dark_radiation,
                got,
                "QCD_Cosmology",
                Path(tmp) / "d.pdf",
                "redshift",
            )
        self.assertTrue(wrote)
        gstar_ax = fig.axes[0]
        by_N = sorted(got.values, key=lambda v: v.raw_N)
        for name, N in (
            ("parking", dissipation.parking_N),
            ("release", dissipation.release_N),
        ):
            line = [l for l in gstar_ax.lines if l.get_label().startswith(name)]
            self.assertEqual(len(line), 1, name)
            first = next(v for v in by_N if v.raw_N >= N)
            self.assertEqual(list(line[0].get_xdata()), [1.0 + first.z.z] * 2, name)
            self.assertGreaterEqual(first.raw_N, N)
            earlier = [v for v in by_N if v.raw_N < N]
            self.assertTrue(len(earlier) > 0)
            self.assertLess(earlier[-1].raw_N, N)
        self.assertEqual(fig.axes[1].get_xscale(), "log")

    def test_7_at_alpha_zero_nothing_is_written(self):
        got = self.stored((1.2, 1e-3, 0.0))
        self.assertEqual(got.alpha.value, 0.0)
        for x_coord in ("efolds", "redshift"):
            with self.subTest(x_coord=x_coord), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp)
                wrote, fig, lines = _draw(
                    ec.plot_dark_radiation, got, "QCD_Cosmology", out / "d.pdf", x_coord
                )
                self.assertIs(wrote, False)
                self.assertIsNone(fig)
                self.assertEqual(list(out.iterdir()), [])
            self.assertIn("alpha = 0", lines[0])

    def test_8_the_dissipation_columns_of_the_record(self):
        got = self.stored(self.KEY)
        dissipation = got.dissipation
        record = ec.build_history_record(
            beta=1.2,
            M_Mp=1e-3,
            n=1,
            phi_init_Mp=5.0,
            alpha=0.1,
            drain_model="fixed",
            scalar=got,
            bbn=None,
            baseline=None,
            units=self.units,
        )
        # the temperatures, in GeV, from the stored ln T_J in the cosmology's units
        for key, source in (
            ("parking_T_GeV", dissipation.parking_log_T_Jordan),
            ("release_T_GeV", dissipation.release_log_T_Jordan),
        ):
            self.assertIsNotNone(source, key)
            self.assertEqual(record[key], np.exp(source) / self.units.GeV, key)
        # the others as the record has them
        for key in (
            "injections",
            "injected_fraction",
            "log_R_first_impact",
            "birth_failing_share",
            "reflection_failing_share",
            "tracker_failing_share",
            "unassessed_share",
        ):
            expected = getattr(dissipation, key)
            if expected is None:
                self.assertTrue(np.isnan(record[key]), key)
            else:
                self.assertEqual(record[key], expected, key)
        # in the header just before the drain block, which one-loop-drain prompt 04 put between them and
        # failure_reasons
        first = ec.CSV_COLUMNS.index(ec.DISSIPATION_CSV_COLUMNS[0])
        self.assertEqual(
            ec.CSV_COLUMNS[first : first + len(ec.DISSIPATION_CSV_COLUMNS)],
            ec.DISSIPATION_CSV_COLUMNS,
        )
        self.assertEqual(
            ec.CSV_COLUMNS[first + len(ec.DISSIPATION_CSV_COLUMNS) : -1],
            ec.DRAIN_CSV_COLUMNS,
        )
        self.assertEqual(ec.CSV_COLUMNS[-1], "failure_reasons")


if __name__ == "__main__":
    unittest.main()
