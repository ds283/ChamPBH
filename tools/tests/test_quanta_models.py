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
The quanta tools take each row's wall from its potential (power-law-wall prompt 06 §1.8, §1.10; README §6.10 P51-P53).

On a power-law root (`<output>/<label>/inverse-power/`) written by 04's writers from constructed rows
(tools/tests/quanta_exports.py), with one history at n = 1 and one at n = 2 under `one-loop-measured-pl-v1`:
- `quanta_measures` measures each with its own wall's spectrum (`R8-power-n1-v1`, `R8-power-n2-v1`): every output row
  names it, and ln n_g is the one that wall's spectrum gives; the summary names both, in the order met;
- a row whose potential has no wall at its n (`inverse-power` at n = 3), or names no potential the code knows, is refused
  and counted in the refusals, with the reason, and nothing of it is written; the others are measured;
- a histories.csv without `model` is refused as an unreadable one (exit 2), naming the column, by quanta_measures and by
  both figure tools;
- `quanta_figures`' check expects, row by row, the spectrum model of the row's wall: the root's quanta_histories.csv
  passes, and a row naming the other n's model is refused; the two figure tools draw both histories and name both
  models.

No Ray, no store. About 3 s (the spectra's tables are built once per wall).
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
from Units import Planck_units
from tools import plot_quanta_grid as pg
from tools import plot_quanta_history as ph
from tools import quanta_figures as qf
from tools import quanta_measures as qm
from tools.tests import quanta_exports as qe
from tools.tests.test_quanta_measures import constructed, samples_for

UNITS = Planck_units()
PL = "one-loop-measured-pl-v1"
POWER = "inverse-power"


class _PowerRoot(unittest.TestCase):
    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="quanta-models-"))
        self.root = ec.export_root(self._dir / "out", qe.LABEL, POWER)
        self.root.mkdir(parents=True)
        self.records = []
        self.dirs = {}

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def add(self, n, beta=2.0, model=POWER, drain_model=PL):
        rows, ledger, ps, _ = constructed("after", 0.25)
        record = qe.history_record(
            beta,
            1e-20,
            n,
            1.0,
            drain_model,
            parking_share=ps,
            bounce_rows=len(rows),
            model=model,
        )
        record.update(
            drained_share_one_loop=0.7,
            drained_share_gap=0.1,
            drained_share_classical=0.1,
            drained_share_unassessed=0.1,
            ell_min=3.5,
            all_worst_birth_margin=0.5,
            all_worst_reflection_margin=1.0,
            all_worst_tracker_margin=1.5,
        )
        self.records.append(record)
        d = qe.write_constructed(self.root, UNITS, record, rows, samples_for(ledger))
        self.dirs[(n, beta)] = d
        return d

    def write_histories(self, drop_model=False, raw=False):
        """histories.csv by write_histories_csv; or, raw, by the csv module, which writes records of more than one
        `model` (write_histories_csv refuses them) or, with drop_model, no `model` column.
        """
        path = self.root / "histories.csv"
        if drop_model or raw:
            columns = [c for c in ec.CSV_COLUMNS if c != "model" or not drop_model]
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(columns)
                for r in self.records:
                    w.writerow(["" if r[c] != r[c] else r[c] for c in columns])
        else:
            ec.write_histories_csv(self.records, path)

    def measure(self, c=1.0):
        out = io.StringIO()
        rc = qm.run(self.root, c, units=UNITS, out=out)
        return rc, out.getvalue()

    def read(self, path):
        with open(path, newline="") as f:
            return list(csv.DictReader(f))


class TestEachRowItsWall(_PowerRoot):
    def test_n_1_and_n_2_in_one_root(self):
        self.add(1)
        self.add(2)
        self.write_histories()
        rc, text = self.measure()
        self.assertEqual(rc, 0, text)
        self.assertIn("measured 2,", text)
        self.assertIn(
            "quanta_measures (R8-power-n1-v1, R8-power-n2-v1, assoc scale 1)", text
        )
        expected = {1: qs.WALL_POWER_N1, 2: qs.WALL_POWER_N2}
        by_n = {int(r["n"]): r for r in self.read(self.root / "quanta_histories.csv")}
        self.assertEqual(sorted(by_n), [1, 2])
        for n, wall in expected.items():
            with self.subTest(n=n):
                model = qs.spectrum_model(wall)
                self.assertEqual(by_n[n]["spectrum_model"], model)
                d = self.dirs[(n, 2.0)]
                bounces = self.read(d / "quanta_bounces.csv")
                samples = self.read(d / "quanta_samples.csv")
                self.assertEqual({r["spectrum_model"] for r in bounces}, {model})
                self.assertEqual({r["spectrum_model"] for r in samples}, {model})
                # ln n_g is the one this wall's spectrum gives
                sp = qs.spectrum(wall)
                gens = [
                    r
                    for r in self.read(d / "bounces.csv")
                    if r["source"] == "candidate"
                    and r["log_q_Mp"] != ""
                    and float(r["delta_K_Mp4"]) > 0.0
                ]
                out = {int(r["ordinal"]): r for r in bounces}
                for g in gens:
                    expect = (
                        qs.LOG_TWO_PI_SQUARED
                        + log(float(g["delta_K_Mp4"]))
                        - 4.0 * float(g["log_q_Mp"])
                        - sp.log_I
                        + sp.log_s_peak
                    )
                    self.assertAlmostEqual(
                        float(out[int(g["ordinal"])]["ln_n_g"]), expect, places=12
                    )
        # the two walls give two different spectra, so the rows do differ
        a = self.read(self.dirs[(1, 2.0)] / "quanta_bounces.csv")
        b = self.read(self.dirs[(2, 2.0)] / "quanta_bounces.csv")
        self.assertNotEqual(
            [r["ln_n_g"] for r in a if r["ln_n_g"]],
            [r["ln_n_g"] for r in b if r["ln_n_g"]],
        )

    def test_a_row_with_no_wall_is_refused_and_counted(self):
        self.add(1)
        self.add(3, beta=2.5)
        self.add(1, beta=3.0, model="power-law")
        self.write_histories(raw=True)
        rc, text = self.measure()
        self.assertEqual(rc, 1, text)
        self.assertIn("measured 1,", text)
        self.assertIn("refused 2,", text)
        refusals = [l for l in text.splitlines() if l.startswith("REFUSED")]
        self.assertEqual(len(refusals), 2)
        self.assertTrue(
            any("n = 3" in l and "spectrum_wall" in l for l in refusals), refusals
        )
        self.assertTrue(any("power-law" in l for l in refusals), refusals)
        self.assertFalse((self.dirs[(3, 2.5)] / "quanta_bounces.csv").exists())
        self.assertTrue((self.dirs[(1, 2.0)] / "quanta_bounces.csv").exists())

    def test_a_histories_csv_without_model_is_refused_by_every_tool(self):
        self.add(1)
        self.write_histories(drop_model=True)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(self.measure()[0], 2)
            self.assertEqual(pg.run(self.root, out=io.StringIO()), 2)
            self.assertEqual(ph.run(self.root, units=UNITS, out=io.StringIO()), 2)
        text = err.getvalue()
        self.assertEqual(text.count("'model'"), 3, text)
        self.assertFalse((self.root / "quanta_histories.csv").exists())


class TestTheFigureTools(_PowerRoot):
    def build(self):
        # n = 1 and n = 2 at two beta: plot_quanta_grid groups by (drain model, A) alone, and refuses two rows in one
        # (beta, M) cell (log 06, "Observations not acted on")
        self.add(1, beta=2.0)
        self.add(2, beta=3.0)
        self.write_histories()
        self.assertEqual(self.measure()[0], 0)

    def test_the_check_expects_each_rows_own_model(self):
        self.build()
        histories = qf.read_histories(self.root)
        expected = qf.expected_models(histories)
        rows = qf.read_checked(self.root / "quanta_histories.csv", 1.0, expected)
        self.assertEqual(
            sorted(r["spectrum_model"] for r in rows),
            ["R8-power-n1-v1", "R8-power-n2-v1"],
        )
        # a row that names the other n's model is refused
        path = self.root / "quanta_histories.csv"
        rows = self.read(path)
        with open(path, newline="") as f:
            fields = csv.DictReader(f).fieldnames
        for r in rows:
            r["spectrum_model"] = "R8-power-n1-v1"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        with self.assertRaises(qf.FigureRefused) as ctx:
            qf.read_checked(path, 1.0, expected)
        self.assertIn("R8-power-n2-v1", str(ctx.exception))

    def test_both_figure_tools_draw_both_and_name_both_models(self):
        self.build()
        out = io.StringIO()
        rc = pg.run(self.root, output=self._dir / "figs", out=out)
        self.assertEqual(rc, 0, out.getvalue())
        self.assertIn(
            "plot_quanta_grid (R8-power-n1-v1, R8-power-n2-v1, assoc scale 1)",
            out.getvalue(),
        )
        out = io.StringIO()
        rc = ph.run(self.root, output=self._dir / "figs", units=UNITS, out=out)
        self.assertEqual(rc, 0, out.getvalue())
        self.assertIn("figures written 2", out.getvalue())
        self.assertIn(
            "plot_quanta_history (R8-power-n1-v1, R8-power-n2-v1, assoc scale 1)",
            out.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
