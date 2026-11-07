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
tools/quanta_figures.py, the pieces shared by the two figure tools (one-loop-drain prompt 07 §1.1, §1.5; orchestrator note
07 rulings 1-3, 7-9), and `ExportMixin`, the constructed exports that the tests of both tools build on.

**The mixin** writes, in a temporary directory, the exports of 04's writers (tools/tests/quanta_exports.py) for histories
constructed with the five-regime candidate rows of `spec`, runs 05's tool on them (`quanta_measures.run`), and so leaves a
tree with histories.csv, quanta_histories.csv and each history's four files. Its histories at A = 1: two M and two beta fully
measured (one with a blank ell_min and no parking, one with a zero non-one-loop share), a drawn history whose bounces.csv is
missing (05 counts it missing: no quanta_histories row), a failed history, a marker row; at A = 2 two measured histories; and
a fixed-law row and a fixed-law marker row.

**Tested here:** the classification of the rows in ruling 1's order, the selectors' matching, the model check (another model
and two models refused; the scale compared as a float), `to_output`, `plots_dir`, the regime refusal, and saving both files.

No Ray, no store, no PRyMordial solve. The Agg backend is used: the tools draw with `matplotlib.figure.Figure`, never pyplot.
"""

import contextlib
import csv
import io
import shutil
import tempfile
import unittest
from math import log
from pathlib import Path

import numpy as np

import extract_common as ec
from ComputeTargets import quanta_spectrum as qs
from ComputeTargets.dark_radiation import DarkRadiationLedger
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED, DRAIN_MODEL_ONE_LOOP
from Units import Planck_units
from tools import quanta_figures as qf
from tools import quanta_measures as qm
from tools.tests import quanta_exports as qe

ONE_LOOP = DRAIN_MODEL_ONE_LOOP
UNITS = Planck_units()
K_INITIAL = 1.0e-4

# (N_turn, N, dK, ln q, ln k_ex, ln m, ln E, ln H, regime): five generations, one of them (the unassessed drain) not one,
# a drain of nothing (classical), and all four regimes
SPEC = [
    (1.0, 1.05, 1.0e-3, log(2.0), log(2.0) - 1.0, -3.0, -20.0, -8.0, "one_loop"),
    (2.0, 2.1, 2.0e-3, log(5.0), log(5.0) - 2.0, -2.5, -22.0, -9.0, "gap"),
    (2.5, 2.6, 5.0e-4, None, None, -2.0, None, -9.5, "unassessed"),
    (3.1, 3.2, 1.0e-3, 0.0, -3.1, -2.0, -23.0, -10.0, "one_loop"),
    (4.0, 4.1, 0.0, None, -3.0, -1.5, -24.0, -11.0, "classical"),
    (5.0, 5.1, 4.0e-3, 6.0, 6.0 - 5.0, -1.0, -25.0, -12.0, "classical"),
    (6.0, 6.13, 3.0e-3, -5.0, -5.0 - 6.0, -0.5, -26.0, -13.0, "one_loop"),
]
GENERATION_REGIMES = ["one_loop", "gap", "one_loop", "classical", "one_loop"]


def constructed(spec=SPEC, parking=0.0):
    """(rows, ledger, parking_share): the candidate rows with a plateau and a wall event, K_before from a ledger as the
    integrator makes it, and a parking leftover of the share asked entered after the last candidate (as 05's tests do).
    """
    ledger = DarkRadiationLedger(K_INITIAL)
    rows = []
    ordinal = 0
    for index, (Nt, N, dK, lq, lk, lm, lE, lH, regime) in enumerate(spec):
        K_before = ledger.cumulative_K[-1] if ledger.cumulative_K else K_INITIAL
        if dK > 0.0:
            ledger.add(N, dK)
        if parking > 0.0 and index == len(spec) - 1:
            total = K_INITIAL + sum(s[2] for s in spec)
            ledger.add(N, parking * total / (1.0 - parking))
        if index == 1:
            rows.append(qe.event(ordinal, 1.5, -2.0))
            ordinal += 1
        if index == 4:
            rows.append(qe.event(ordinal, 3.6, -1.8, source="wall"))
            ordinal += 1
        rows.append(
            qe.candidate(
                ordinal,
                index,
                Nt,
                N,
                dK,
                K_before,
                log_q=lq,
                log_k_ex=lk,
                log_m=lm,
                log_E=lE,
                log_H=lH,
                regime=regime,
            )
        )
        ordinal += 1
    if parking > 0.0:
        K_final = ledger.K(1e9)
        parking = (K_final - K_INITIAL - sum(s[2] for s in spec)) / K_final
    return rows, ledger, parking


def samples_for(ledger):
    out = []
    for k, N in enumerate(np.linspace(0.5, 8.0, 31)):
        N = float(N)
        log_rho = -30.0 - 4.0 * N
        r = ledger.ratio(N, log_rho) if not ledger.is_zero(N) else 0.0
        out.append(
            qe.sample_value(
                k + 1, N, log_rho, r, -9.0 + 1.6 * N, -10.0 + 1.8 * N, UNITS
            )
        )
    return out


def scaled(f):
    """SPEC with every drain scaled by f: a history whose measures differ from the others'."""
    return [
        (Nt, N, dK * f, lq, lk, lm, lE, lH, r)
        for Nt, N, dK, lq, lk, lm, lE, lH, r in SPEC
    ]


class ExportMixin:
    """setUp/tearDown and the constructed tree; mixed into a unittest.TestCase."""

    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="quanta-figures-"))
        self.root = self._dir / "out" / qe.LABEL
        self.root.mkdir(parents=True)
        self.records = []
        self.dirs = {}

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def add(self, beta, M_Mp, A, shares=(0.7, 0.1, 0.1, 0.1), spec=SPEC, **kw):
        rows, ledger, ps = constructed(spec, kw.pop("parking", 0.0))
        record = qe.history_record(
            beta, M_Mp, 1, A, ONE_LOOP, parking_share=ps, bounce_rows=len(rows)
        )
        record.update(
            drained_share_one_loop=shares[0],
            drained_share_gap=shares[1],
            drained_share_classical=shares[2],
            drained_share_unassessed=shares[3],
            ell_min=kw.pop("ell_min", 3.5 + beta),
            all_worst_birth_margin=kw.pop("birth", beta - 2.5),
            all_worst_reflection_margin=kw.pop("reflection", 4.0 - beta),
            all_worst_tracker_margin=kw.pop("tracker", 1.5),
        )
        record.update(kw.pop("record", {}))
        self.records.append(record)
        d = qe.write_constructed(
            self.root, UNITS, record, rows, samples_for(ledger), **kw
        )
        self.dirs[(A, beta, M_Mp)] = d
        return d

    def build(self):
        """The constructed tree, histories.csv written and 05's tool run at c_a = 1 (and 0 and 10 for the scale tests)."""
        self.add(2.0, 1e-20, 1.0)
        self.add(3.0, 1e-20, 1.0, ell_min=float("nan"), shares=(1.0, 0.0, 0.0, 0.0))
        self.add(
            2.0, 2e-20, 1.0, birth=0.0, reflection=0.0, tracker=0.0, spec=scaled(3.0)
        )
        # drawn, but 05 finds no bounces.csv: no quanta_histories row
        self.add(3.0, 2e-20, 1.0, write_bounces=False)
        # a failed history (blank normalization, another reason than the marker's)
        self.records.append(
            qe.history_record(4.0, 1e-20, 1, 1.0, ONE_LOOP, normalization=None)
        )
        self.records[-1]["failure_reasons"] = "tuning: did not converge"
        # a marker row, a fixed-law row and a fixed-law marker row
        marker = qe.history_record(5.0, 1e-20, 1, 1.0, ONE_LOOP, normalization=None)
        marker["failure_reasons"] = qf.MARKER_REASON
        self.records.append(marker)
        self.records.append(qe.history_record(2.0, 1e-20, 1, 0.1, DRAIN_MODEL_FIXED))
        fm = qe.history_record(
            2.5, 1e-20, 1, 0.1, DRAIN_MODEL_FIXED, normalization=None
        )
        fm["failure_reasons"] = qf.MARKER_REASON
        self.records.append(fm)
        # A = 2
        self.add(2.0, 1e-20, 2.0, parking=0.25)
        self.add(2.5, 1e-20, 2.0, parking=0.25)
        ec.write_histories_csv(self.records, self.root / "histories.csv")
        for c in (1.0, 0.0, 10.0):
            rc = qm.run(self.root, c, units=UNITS, out=io.StringIO())
            self.assertEqual(
                rc, 0
            )  # the history with no bounces.csv is counted missing, which is not a refusal
        return self.root

    def read(self, path):
        with open(path, newline="") as f:
            return list(csv.DictReader(f))

    def rewrite(self, path, edit):
        rows = self.read(path)
        with open(path, newline="") as f:
            fields = csv.DictReader(f).fieldnames
        edit(rows)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)


def capturing(module):
    """Patch `module.qf.save_figure` (the shared one) to keep the figures and write nothing; returns (patcher, figures)."""
    from unittest import mock

    figures = []

    def fake(fig, stem):
        figures.append((fig, Path(stem)))
        return Path(str(stem) + ".pdf"), Path(str(stem) + ".png")

    return mock.patch.object(qf, "save_figure", side_effect=fake), figures


class TestTheRowsToDraw(ExportMixin, unittest.TestCase):
    """Ruling 1: a row is counted in the first of fixed, marker, failed, drawn that it meets, and only there."""

    def test_classes_in_order(self):
        self.build()
        rows = qf.read_histories(self.root)
        by = qf.walk(rows)
        # fixed: the fixed row and the fixed marker row (both, and not also as markers)
        self.assertEqual(len(by[qf.FIXED]), 2)
        self.assertEqual(len(by[qf.MARKER]), 1)
        self.assertEqual(len(by[qf.FAILED]), 1)
        self.assertEqual(len(by[qf.DRAWN]), 6)
        self.assertEqual(sum(len(v) for v in by.values()), len(rows))
        self.assertEqual(by[qf.MARKER][0]["beta"], "5.0")
        self.assertEqual(
            by[qf.FAILED][0]["failure_reasons"], "tuning: did not converge"
        )

    def test_the_marker_needs_both_cells(self):
        row = dict(
            drain_model=ONE_LOOP, normalization="", failure_reasons=qf.MARKER_REASON
        )
        self.assertEqual(qf.classify(row), qf.MARKER)
        self.assertEqual(
            qf.classify(dict(row, failure_reasons=qf.MARKER_REASON + " (x)")), qf.FAILED
        )
        self.assertEqual(qf.classify(dict(row, normalization="-3.0")), qf.DRAWN)
        self.assertEqual(
            qf.classify(dict(row, drain_model=DRAIN_MODEL_FIXED)), qf.FIXED
        )

    def test_is_close_matches_a_typed_value(self):
        self.assertTrue(qf.is_close(0.1, "0.1"))
        self.assertTrue(qf.is_close(1.0, "1.0000000001"))
        self.assertFalse(qf.is_close(1.0, "1.001"))
        self.assertFalse(qf.is_close(1.0, "not a number"))

    def test_key_cells_are_strings(self):
        self.build()
        rows = qf.read_histories(self.root)
        q = qf.read_checked(self.root / "quanta_histories.csv", 1.0)
        keys = {qf.key_of(r) for r in rows}
        for r in q:
            self.assertIn(qf.key_of(r), keys)
            self.assertTrue(all(isinstance(c, str) for c in qf.key_of(r)))


class TestTheModelCheck(ExportMixin, unittest.TestCase):
    def test_the_expected_model_and_scale_pass(self):
        self.build()
        for c, name in (
            (1.0, "quanta_histories.csv"),
            (0.0, "quanta_histories-assoc0.csv"),
            (10.0, "quanta_histories-assoc10.csv"),
        ):
            rows = qf.read_checked(self.root / name, c)
            self.assertTrue(rows)
            self.assertEqual({r["spectrum_model"] for r in rows}, {qs.SPECTRUM_MODEL})
            self.assertEqual({float(r["assoc_scale"]) for r in rows}, {c})

    def test_a_file_of_another_scale_is_refused(self):
        self.build()
        with self.assertRaises(qf.FigureRefused) as cm:
            qf.read_checked(self.root / "quanta_histories-assoc0.csv", 1.0)
        self.assertIn("assoc_scale", str(cm.exception))

    def test_another_model_and_two_models_are_refused(self):
        self.build()
        p = self.root / "quanta_histories.csv"

        def other(rows):
            for r in rows:
                r["spectrum_model"] = "U4-v0"

        self.rewrite(p, other)
        with self.assertRaises(qf.FigureRefused) as cm:
            qf.read_checked(p, 1.0)
        self.assertIn("U4-v0", str(cm.exception))
        self.build_again_for_two_models(p)

    def build_again_for_two_models(self, p):
        def one_other(rows):
            for r in rows:
                r["spectrum_model"] = qs.SPECTRUM_MODEL
            rows[-1]["spectrum_model"] = "U4-v0"

        self.rewrite(p, one_other)
        with self.assertRaises(qf.FigureRefused):
            qf.read_checked(p, 1.0)

    def test_an_unreadable_file_is_refused(self):
        with self.assertRaises(qf.FigureRefused):
            qf.read_checked(self._dir / "absent.csv", 1.0)


class TestWhereThingsGo(ExportMixin, unittest.TestCase):
    def test_to_output_replaces_the_root(self):
        root = Path("/x/out/QCD")
        p = root / "one-loop-measured-v1/A=1/plots/quanta_grid"
        self.assertEqual(qf.to_output(root, p, None), p)
        self.assertEqual(
            qf.to_output(root, p, Path("/y/QCD")),
            Path("/y/QCD/one-loop-measured-v1/A=1/plots/quanta_grid"),
        )

    def test_plots_dir_replaces_csv_and_nothing_else(self):
        self.build()
        d = self.dirs[(1.0, 2.0, 1e-20)]
        p = qf.plots_dir(self.root, d)
        self.assertEqual(
            p.relative_to(self.root).parts[:3], ("one-loop-measured-v1", "A=1", "plots")
        )
        self.assertEqual(
            p.relative_to(self.root).parts[3:], d.relative_to(self.root).parts[3:]
        )
        self.assertNotIn("csv", p.relative_to(self.root).parts)

    def test_save_writes_both_files_and_creates_directories(self):
        from matplotlib.figure import Figure

        fig = Figure()
        fig.subplots().plot([0, 1], [0, 1])
        pdf, png = qf.save_figure(fig, self._dir / "a" / "b" / "fig")
        self.assertTrue(pdf.is_file() and png.is_file())
        self.assertEqual(pdf.name, "fig.pdf")
        self.assertEqual(png.name, "fig.png")
        self.assertEqual(png.read_bytes()[:4], b"\x89PNG")
        # 150 dpi: a 6.4 x 4.8 inch figure
        self.assertEqual(int.from_bytes(png.read_bytes()[16:20], "big"), 960)


class TestTheRegimes(unittest.TestCase):
    def test_four_regimes_four_distinct_colours(self):
        self.assertEqual(qf.REGIMES, ("one_loop", "gap", "classical", "unassessed"))
        self.assertEqual(len(set(qf.REGIME_COLOURS.values())), 4)
        self.assertEqual(set(qf.REGIME_COLOURS), set(qf.REGIMES))
        self.assertEqual(set(qf.REGIME_LABELS), set(qf.REGIMES))


if __name__ == "__main__":
    unittest.main()
