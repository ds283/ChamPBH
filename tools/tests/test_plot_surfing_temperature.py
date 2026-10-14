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
tools/plot_surfing_temperature.py (surfing-temperature prompt 03, F1-F3).

- a synthetic CSV in tools.surfing_scan.COLUMNS' header, with a sidecar: a not-captured row, a QCD
  capture with no second dip, a QCD capture with all four temperatures, an EW capture and a failure
  row (whose `captured` is blank);
- `build_figure` on those rows with stub roots on a stub grid;
- `main(argv)` with the cosmology, `find_kicks` and `reachability_roots` replaced by stubs;
- the fine grid, by `fine_grid` and (TestFineGridCalls) by a wrapper counting the calls of the real
  `reachability_roots` on the production cosmology.

No Ray, no store, no history and no PRyMordial. Every test runs from the repository root, as the
suites do.
"""

import contextlib
import csv
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np

import tools.plot_surfing_temperature as psp
import tools.surfing_scan as ss

ROOT = Path(__file__).resolve().parents[2]


def make_row(**fields):
    """A CSV row as a dict over ss.COLUMNS: blank unless given, with the inputs of the campaign."""
    row = {name: "" for name in ss.COLUMNS}
    row.update(
        phi_init_Mp="5.0",
        M_Mp="0.001",
        Lambda_eV="0.001",
        T_stop_GeV="0.001",
        status="ok",
    )
    row.update({k: str(v) for k, v in fields.items()})
    return row


SYNTHETIC_ROWS = [
    # not captured: the delivery is kept in the file, and is not a surfing temperature
    make_row(
        beta=1.2,
        captured="false",
        T_deliver_GeV=0.14,
        N_deliver=5.0,
        deliver_reflected="false",
        n_crossings=0,
    ),
    # a QCD capture with no second dip
    make_row(
        beta=1.5,
        captured="true",
        capture_kick="QCD",
        T_capture_GeV=0.32,
        N_capture=14.0,
        T_max_GeV=0.36,
        N_max=18.0,
        T_deliver_GeV=0.3599,
        N_deliver=18.1,
        deliver_reflected="false",
        n_crossings=2,
        T_hot_GeV=0.357,
        T_cold_GeV=0.13,
    ),
    # a QCD capture with all four temperatures
    make_row(
        beta=2.5,
        captured="true",
        capture_kick="QCD",
        T_capture_GeV=1.1,
        N_capture=14.0,
        T_max_GeV=1.4,
        N_max=18.0,
        T_second_dip_GeV=1.25,
        N_second_dip=20.0,
        T_deliver_GeV=1.3,
        N_deliver=22.0,
        deliver_reflected="false",
        n_crossings=4,
        T_hot_GeV=1.35,
        T_cold_GeV=0.0257,
    ),
    # an EW capture
    make_row(
        beta=3.5,
        captured="true",
        capture_kick="EW",
        T_capture_GeV=77.0,
        N_capture=7.0,
        T_max_GeV=100.7,
        N_max=11.0,
        T_second_dip_GeV=97.2,
        N_second_dip=15.0,
        T_deliver_GeV=97.6,
        N_deliver=22.8,
        deliver_reflected="false",
        n_crossings=4,
        T_hot_GeV=97.63,
        T_cold_GeV=30.0,
    ),
    # a failure: every field but the inputs, status, reason and wall_s is blank, captured too
    make_row(
        beta=4.0, status="failure", failure_reason="step size underflow", wall_s=0.5
    ),
]

SIDECAR = {
    "runs": [
        {"git_head": "61e0c42" + "0" * 33, "git_tracked_changes": False},
        {"git_head": "f6440d1" + "0" * 33, "git_tracked_changes": True},
    ]
}

STUB_BETAS = [round(1.1 + 0.1 * k, 10) for k in range(40)]  # 1.1 ... 5.0
THRESHOLDS = {"EW": 3.01, "QCD": 1.107, "mu-pi": 2.018, "ee": 1.864}
SERIES_COLUMNS = {
    "capture": "T_capture_GeV",
    "largest": "T_max_GeV",
    "second dip": "T_second_dip_GeV",
    "delivery": "T_deliver_GeV",
}


def stub_records(beta, GeV=1.0):
    """reachability_roots-shaped records (hot to cold) for the stub: QCD merges with mu-pi from 2.2."""

    def rec(kick, hot, cold, merged=None):
        if hot is None:
            return {"kick": kick, "T_hot": None, "T_cold": None, "merged": None}
        return {
            "kick": kick,
            "T_hot": hot * GeV,
            "T_cold": cold * GeV,
            "merged": merged,
        }

    merged = beta >= 2.2
    return [
        rec("EW", 20.0 * beta, 5.0 * beta) if beta >= 3.0 else rec("EW", None, None),
        rec(
            "QCD",
            0.2 * beta,
            0.03 if merged else 0.1 * beta,
            "mu-pi" if merged else None,
        ),
        (
            rec("mu-pi", 0.2 * beta, 0.03, "QCD")
            if merged
            else (rec("mu-pi", 0.06, 0.04) if beta >= 2.0 else rec("mu-pi", None, None))
        ),
        rec("ee", 1.0e-3, 6.0e-4) if beta >= 1.8 else rec("ee", None, None),
    ]


STUB_ROOTS = psp.RootGrid(STUB_BETAS, [stub_records(b) for b in STUB_BETAS], THRESHOLDS)


def write_csv(directory, rows=SYNTHETIC_ROWS, sidecar=SIDECAR, name="synthetic.csv"):
    path = Path(directory, name)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ss.COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    if sidecar is not None:
        with open(str(path) + ".meta.json", "w") as handle:
            json.dump(sidecar, handle)
    return path


def gid_artists(ax, gid):
    return [a for a in list(ax.lines) + list(ax.texts) if a.get_gid() == gid]


def one(ax, gid):
    found = gid_artists(ax, gid)
    assert len(found) == 1, f"{gid}: {len(found)} artists"
    return found[0]


def legend_texts(fig):
    return [t.get_text() for t in fig.legends[0].get_texts()]


class _TempDirCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        self.rows = psp.parse_rows(write_csv(self.dir))
        self.fig = psp.build_figure(self.rows, STUB_ROOTS, SIDECAR)
        self.upper, self.lower = self.fig.axes


class TestParse(_TempDirCase):
    def test_a_rows_are_classified_by_status_first(self):
        """A failure row's blank `captured` is neither false nor an error; an ok row's must be read."""
        self.assertEqual(
            [r["kind"] for r in self.rows],
            [psp.UNCAPTURED, psp.CAPTURED, psp.CAPTURED, psp.CAPTURED, psp.FAILURE],
        )
        failure = self.rows[-1]
        self.assertIsNone(failure["T_deliver_GeV"])
        self.assertIsNone(failure["capture_kick"])
        # an ok row with a blank captured is refused
        bad = write_csv(
            self.dir, [make_row(beta=1.5, captured="")], name="bad.csv", sidecar=None
        )
        with self.assertRaises(ValueError):
            psp.parse_rows(bad)

    def test_b_the_header_must_be_the_scans(self):
        path = self.dir / "foreign.csv"
        path.write_text("beta,T_deliver_GeV\n1.5,0.3\n")
        with self.assertRaises(ValueError):
            psp.parse_rows(path)


class TestBuildFigure(_TempDirCase):
    def test_a_one_marker_per_existing_value_and_none_at_a_blank(self):
        """Markers only for captured rows, only where the column has a value, never at zero."""
        for key, column in SERIES_COLUMNS.items():
            expected = [
                (float(r["beta"]), float(r[column]))
                for r in SYNTHETIC_ROWS
                if r["captured"] == "true" and r[column] != ""
            ]
            line = one(self.upper, f"marker-{key}")
            got = list(zip(line.get_xdata(), line.get_ydata()))
            self.assertEqual(got, expected, key)
            self.assertTrue(all(y > 0.0 for _, y in got))
        # the second dip is blank at beta = 1.5, and the uncaptured row's delivery is not drawn
        self.assertEqual(
            list(one(self.upper, "marker-second dip").get_xdata()), [2.5, 3.5]
        )
        self.assertNotIn(1.2, list(one(self.upper, "marker-delivery").get_xdata()))
        # the lower panel has the same markers, less any row with no T_hot
        for key in SERIES_COLUMNS:
            self.assertEqual(
                list(one(self.lower, f"ratio-{key}").get_xdata()),
                list(one(self.upper, f"marker-{key}").get_xdata()),
            )

    def test_b_ratios_equal_the_quotients_of_the_csv(self):
        for key, column in SERIES_COLUMNS.items():
            expected = [
                float(r[column]) / float(r["T_hot_GeV"])
                for r in SYNTHETIC_ROWS
                if r["captured"] == "true" and r[column] != ""
            ]
            got = one(self.lower, f"ratio-{key}").get_ydata()
            self.assertEqual(len(got), len(expected))
            for g, e in zip(got, expected):
                self.assertAlmostEqual(g, e, delta=1e-12)

    def test_c_rows_not_drawn_have_edge_marks_and_legend_entries(self):
        texts = legend_texts(self.fig)
        self.assertIn("not captured", texts)
        self.assertIn("failure", texts)
        for panel, suffix in ((self.upper, "upper"), (self.lower, "lower")):
            self.assertEqual(
                list(one(panel, f"edge-uncaptured-{suffix}").get_xdata()), [1.2]
            )
            self.assertEqual(
                list(one(panel, f"edge-failure-{suffix}").get_xdata()), [4.0]
            )
        # and nothing else: no legend entry for what the file does not contain
        rows = [r for r in self.rows if r["kind"] == psp.CAPTURED]
        fig = psp.build_figure(rows, STUB_ROOTS, SIDECAR)
        self.assertNotIn("not captured", legend_texts(fig))
        self.assertNotIn("failure", legend_texts(fig))

    def test_d_roots_are_drawn_on_the_stub_grid(self):
        betas = np.array(STUB_BETAS)
        for kick, kicks_records in (("EW", 0), ("QCD", 1), ("ee", 3)):
            for edge, key in (("hot", "T_hot"), ("cold", "T_cold")):
                line = one(self.upper, f"root-{edge}-{kick}")
                np.testing.assert_array_equal(line.get_xdata(), betas)
                expected = np.array(
                    [
                        (
                            r[kicks_records][key]
                            if r[kicks_records]["T_hot"] is not None
                            and r[kicks_records]["merged"] is None
                            else np.nan
                        )
                        for r in STUB_ROOTS.records
                    ]
                )
                np.testing.assert_array_equal(line.get_ydata(), expected)
        # the thresholds
        for kick, beta_th in THRESHOLDS.items():
            self.assertIn(
                f"{beta_th:.3f}", one(self.upper, f"threshold-{kick}").get_text()
            )

    def test_e_a_merged_interval_is_one_band_and_the_lines_break(self):
        """Drawn once, hot edge in QCD's colour and cold edge in mu-pi's; none of its beta on the
        separate lines; one filled polygon set for the merged run, not two."""
        hot = one(self.upper, "merged-hot-QCD+mu-pi")
        cold = one(self.upper, "merged-cold-QCD+mu-pi")
        merged = np.array([b >= 2.2 for b in STUB_BETAS])
        self.assertTrue(np.all(np.isfinite(hot.get_ydata()[merged])))
        self.assertTrue(np.all(np.isnan(hot.get_ydata()[~merged])))
        self.assertEqual(hot.get_color(), psp.KICK_COLOURS["QCD"])
        self.assertEqual(cold.get_color(), psp.KICK_COLOURS["mu-pi"])
        self.assertEqual(len(gid_artists(self.upper, "merged-hot-mu-pi+QCD")), 0)
        # the separate lines are broken: nothing at a merged beta
        for kick in ("QCD", "mu-pi"):
            for edge in ("hot", "cold"):
                y = one(self.upper, f"root-{edge}-{kick}").get_ydata()
                self.assertTrue(np.all(np.isnan(y[merged])), (kick, edge))
        # one fill for the merged run, plus the separate bands: QCD's, mu-pi's, EW's and ee's
        self.assertEqual(len(self.upper.collections), 5)

    def test_f_reachable_not_surfed_on_every_kick_no_row_captures(self):
        self.assertIn(
            "reachable, not surfed", one(self.upper, "band-label-ee").get_text()
        )
        self.assertIn(
            "reachable, not surfed", one(self.upper, "band-label-mu-pi").get_text()
        )
        self.assertNotIn(
            "reachable, not surfed", one(self.upper, "band-label-QCD").get_text()
        )
        self.assertNotIn(
            "reachable, not surfed", one(self.upper, "band-label-EW").get_text()
        )

    def test_g_the_title_names_every_head_and_marks_tracked_changes(self):
        title = self.fig.get_suptitle()
        self.assertIn("61e0c42", title)
        self.assertIn("f6440d1 + tracked changes", title)
        self.assertNotIn("61e0c42 + tracked", title)
        for text in ("5", "0.001"):
            self.assertIn(text, title)
        fig = psp.build_figure(self.rows, STUB_ROOTS, None)
        self.assertIn("HEAD unknown", fig.get_suptitle())

    def test_h_coincident_maxima_and_deliveries_both_stay_visible(self):
        """Delivery is the larger, lower disc and the largest a smaller triangle above it."""
        delivery, largest = one(self.upper, "marker-delivery"), one(
            self.upper, "marker-largest"
        )
        self.assertGreater(delivery.get_markersize(), largest.get_markersize())
        self.assertLess(delivery.get_zorder(), largest.get_zorder())
        self.assertNotEqual(
            delivery.get_markerfacecolor(), largest.get_markerfacecolor()
        )
        self.assertEqual(
            one(self.upper, "marker-second dip").get_markerfacecolor(), "none"
        )


class TestFineGrid(unittest.TestCase):
    def test_a_the_range_is_rounded_outwards_with_a_tolerance(self):
        for lo, hi, first, last, count in (
            (
                1.15,
                5.0,
                1.15,
                5.0,
                771,
            ),  # 1.15/0.05 is 22.999999999999996: not moved to 1.10
            (1.10, 5.0, 1.10, 5.0, 781),
            (1.17, 1.23, 1.15, 1.25, 21),
            (1.5, 1.5, 1.5, 1.5, 1),
        ):
            grid = psp.fine_grid(lo, hi)
            self.assertEqual(
                (grid[0], grid[-1], len(grid)), (first, last, count), (lo, hi)
            )
        grid = psp.fine_grid(1.10, 5.0)
        self.assertEqual(grid[1], 1.105)
        self.assertEqual(grid[-1], 5.0)


@contextlib.contextmanager
def _in_directory(path):
    here = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(here)


def _stubs(GeV=2.0):
    """Patches for the cosmology and the two functions of extract_common that main calls."""
    setup = SimpleNamespace(
        cosmology=SimpleNamespace(_eos=SimpleNamespace(_T_min=1.0e-5, _T_max=2.5e4)),
        units=SimpleNamespace(GeV=GeV),
    )
    kicks = [
        {"label": label, "Sigma_peak": s}
        for label, s in (("EW", 0.037), ("QCD", 0.31), ("mu-pi", 0.085), ("ee", 0.1))
    ]
    calls = []

    def fake_roots(cosmology, beta, kicks_arg):
        calls.append(beta)
        return stub_records(beta, GeV=GeV)

    return (
        mock.patch.object(psp, "make_setup", return_value=setup),
        mock.patch.object(psp, "find_kicks", return_value=kicks),
        mock.patch.object(psp, "reachability_roots", side_effect=fake_roots),
        calls,
    )


class TestMain(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        self.csv = write_csv(self.dir)

    def test_a_writes_both_files_and_changes_nothing_else(self):
        """With the cosmology, find_kicks and reachability_roots stubbed; the prefix holds dots."""
        before = sorted(os.listdir(self.dir))
        p_setup, p_kicks, p_roots, calls = _stubs()
        out = self.dir / "run-2026.6.0"
        with _in_directory(ROOT), p_setup, p_kicks, p_roots, contextlib.redirect_stdout(
            io.StringIO()
        ):
            status = psp.main(["--csv", str(self.csv), "--out", str(out)])
        self.assertEqual(status, 0)
        after = sorted(os.listdir(self.dir))
        self.assertEqual(
            after, sorted(before + ["run-2026.6.0.pdf", "run-2026.6.0.png"])
        )
        for suffix in (".pdf", ".png"):
            self.assertGreater(Path(str(out) + suffix).stat().st_size, 0)
        # the stub is called at every beta of the rounded range (1.20-4.00 -> 1.20-4.00)
        self.assertEqual(calls[0], 1.2)
        self.assertEqual(calls[-1], 4.0)
        self.assertEqual(len(calls), 561)

    def test_b_refused_outside_the_root_and_on_a_bad_csv(self):
        out = self.dir / "refused"
        with _in_directory(self.dir), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(psp.main(["--csv", str(self.csv), "--out", str(out)]), 2)
        self.assertEqual(
            sorted(os.listdir(self.dir)),
            sorted(["synthetic.csv", "synthetic.csv.meta.json"]),
        )
        bad = self.dir / "bad.csv"
        bad.write_text("beta\n1.0\n")
        with _in_directory(ROOT), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(psp.main(["--csv", str(bad), "--out", str(out)]), 2)
        self.assertFalse(Path(str(out) + ".pdf").exists())

    def test_c_a_missing_sidecar_is_headed_unknown(self):
        Path(str(self.csv) + ".meta.json").unlink()
        p_setup, p_kicks, p_roots, _ = _stubs()
        out = self.dir / "nosidecar"
        with _in_directory(ROOT), p_setup, p_kicks, p_roots, contextlib.redirect_stdout(
            io.StringIO()
        ), contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(psp.main(["--csv", str(self.csv), "--out", str(out)]), 0)
        self.assertIn("HEAD unknown", err.getvalue())


class TestFineGridCalls(unittest.TestCase):
    def test_the_real_reachability_roots_is_called_at_the_fine_grid(self):
        """
        Builds the production cosmology and runs the real find_kicks and reachability_roots, so it
        takes about 1.3 s (measured alone at load 15: the cosmology, find_kicks and 11 calls). The
        CSV has two rows, beta = 1.15 and 1.20, so the rounded range is 1.15-1.20 and the grid has
        11 values at step 0.005. Counts the calls with wrappers.
        """
        with tempfile.TemporaryDirectory() as tmp:
            rows = [
                make_row(
                    beta=1.15, captured="false", T_deliver_GeV=0.14, n_crossings=0
                ),
                make_row(beta=1.2, captured="false", T_deliver_GeV=0.15, n_crossings=0),
            ]
            path = write_csv(tmp, rows)
            roots_calls, kicks_calls = [], []
            real_roots, real_kicks = psp.reachability_roots, psp.find_kicks

            def roots(cosmology, beta, kicks):
                roots_calls.append(beta)
                return real_roots(cosmology, beta, kicks)

            def kicks(*args, **kw):
                kicks_calls.append(1)
                return real_kicks(*args, **kw)

            out = Path(tmp, "fine")
            with _in_directory(ROOT), mock.patch.object(
                psp, "reachability_roots", side_effect=roots
            ), mock.patch.object(
                psp, "find_kicks", side_effect=kicks
            ), contextlib.redirect_stdout(
                io.StringIO()
            ):
                status = psp.main(["--csv", str(path), "--out", str(out)])
            self.assertEqual(status, 0)
            self.assertEqual(len(kicks_calls), 1)
            self.assertEqual(len(roots_calls), 11)
            self.assertEqual((roots_calls[0], roots_calls[-1]), (1.15, 1.2))
            np.testing.assert_allclose(np.diff(roots_calls), 0.005, atol=1e-9)
            self.assertGreater(Path(str(out) + ".pdf").stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
