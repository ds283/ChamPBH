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
tools/build_w_table.py (electroweak-eos prompt 01, §1.3): the production w table is the build's
output, composed as the campaign's U2 (b) says, and the build's assertions fire on input made to
break them.

- the build regenerates the installed CSV byte for byte, and `--check` says so (exit 0), and says
  otherwise (exit 1) when one byte of a copy of it changes;
- the composition: each source's rows and their count, and the boundaries;
- each assertion of the build fires on input made to break it: a perturbed shared row at T0, a
  perturbed 100 GeV point, an out-of-order row, and a left-out row moved off the points' spline;
- `Xav_EOS_spline` on the installed table: Sigma = 0 at and above its top row, 386.7085 GeV, and
  its electroweak peak.

No Ray, no store, no integration. Run from the repository root, as the suites do. Each test but
the last class's takes well under a second; the last class builds `Xav_EOS_spline` once (about a
second) and evaluates Sigma on 3000 points per decade from 20 GeV to 1 TeV (well under a second).
"""

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import tools.build_w_table as bw
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

T0_GEV = 5.011872336272725e-05
TOP_T_GEV = 386.7085411060535


class TestRegeneration(unittest.TestCase):

    def test_the_build_is_the_installed_csv_byte_for_byte(self):
        """The table built in memory equals the installed CSV byte for byte. Under 0.1 s."""
        self.assertEqual(bw.build_text().encode(), bw.OUTPUT.read_bytes())

    def test_check_exits_0_on_the_installed_csv_and_1_when_one_byte_changes(self):
        """
        `--check` exits 0 on the installed CSV and writes nothing; on a copy with one byte changed
        it exits 1. Under 0.1 s.
        """
        before = bw.OUTPUT.read_bytes()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(bw.main(["--check"]), 0)
        self.assertEqual(bw.OUTPUT.read_bytes(), before)

        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp) / "Xav_EOS_data.csv"
            data = bytearray(before)
            i = (
                data.index(b"0.3", len(bw.HEADER) + 1) + 2
            )  # a digit of a w, in a data row
            data[i] = ord("1") if data[i] != ord("1") else ord("2")
            copy.write_bytes(bytes(data))
            with mock.patch.object(bw, "OUTPUT", copy), contextlib.redirect_stdout(
                io.StringIO()
            ):
                self.assertEqual(bw.main(["--check"]), 1)

    def test_every_number_reads_back_to_the_float_written(self):
        """Each row of the installed CSV, parsed by float(), is the built row. Under 0.1 s."""
        inputs = bw.load_inputs()
        rows = bw.table_rows(
            bw.compose(inputs["old"], inputs["notebook"], inputs["points"])
        )
        self.assertEqual(bw.read_rows(bw.OUTPUT), rows)
        self.assertNotIn("np.float64", bw.OUTPUT.read_text())


class TestComposition(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.inputs = bw.load_inputs()
        cls.parts = bw.compose(
            cls.inputs["old"], cls.inputs["notebook"], cls.inputs["points"]
        )
        cls.installed = bw.read_rows(bw.OUTPUT)

    def test_the_counts(self):
        """14 + 126 + 29 + 1 = 170 rows. Under 0.1 s."""
        self.assertEqual(
            {k: len(self.parts[k]) for k in bw.EXPECTED_COUNTS}, bw.EXPECTED_COUNTS
        )
        self.assertEqual(len(self.installed), 170)
        self.assertEqual(self.installed[0][0], 1e-5)
        self.assertEqual(self.installed[-1], (TOP_T_GEV, 1.0 / 3.0))

    def test_each_source_supplies_its_rows_unchanged(self):
        """
        Below T0 the rows are the old table's; from T0 to 100 GeV the notebook's; 100-380 GeV the
        points'; above, the notebook's 386.7 GeV row. Each is the source's row bitwise. Under 0.1 s.
        """
        old, nb, pts = (self.inputs[k] for k in ("old", "notebook", "points"))
        self.assertEqual(self.installed[:14], [r for r in old if r[0] < T0_GEV])
        self.assertEqual(
            self.installed[14:140], [r for r in nb if T0_GEV <= r[0] < 100.0]
        )
        self.assertEqual(self.installed[140:169], pts)
        self.assertEqual(self.installed[169:], [nb[-1]])

    def test_the_boundaries(self):
        """
        The first notebook row is T0, the notebook's second row; the last of the old rows is below
        it; the notebook's first row (36.5 keV) and its rows at 158.5, 251.2 and 316.2 GeV are left
        out. Under 0.1 s.
        """
        self.assertEqual(self.parts["notebook_low"][0][0], T0_GEV)
        self.assertEqual(self.inputs["notebook"][1][0], T0_GEV)
        self.assertLess(self.parts["old"][-1][0], T0_GEV)
        self.assertAlmostEqual(self.parts["old"][-1][0], 4.466835921509635e-05)
        self.assertEqual(self.parts["notebook_low"][-1][0], 89.12509381337459)
        self.assertEqual(self.parts["points"][0][0], 100.0)
        self.assertEqual(self.parts["points"][-1][0], 380.0)
        T_installed = {T for T, _ in self.installed}
        self.assertNotIn(self.inputs["notebook"][0][0], T_installed)
        left_out = [T for T, _, _ in self.parts["left_out"]]
        self.assertEqual(len(left_out), 3)
        for T, expected in zip(left_out, (158.4893, 251.1886, 316.2278)):
            self.assertAlmostEqual(T, expected, places=3)
            self.assertNotIn(T, T_installed)


class TestAssertionsFire(unittest.TestCase):
    """Each assertion of the build, on input made to break it. Under 0.1 s each."""

    @classmethod
    def setUpClass(cls):
        cls.inputs = bw.load_inputs()

    def _compose(self, old=None, notebook=None, points=None):
        return bw.compose(
            old if old is not None else list(self.inputs["old"]),
            notebook if notebook is not None else list(self.inputs["notebook"]),
            points if points is not None else list(self.inputs["points"]),
        )

    def test_unbroken_input_composes(self):
        self._compose()

    def test_a_perturbed_shared_row_at_T0(self):
        old = list(self.inputs["old"])
        i = [T for T, _ in old].index(T0_GEV)
        old[i] = (old[i][0], old[i][1] + 1e-11)
        with self.assertRaisesRegex(bw.BuildError, "at T0"):
            self._compose(old=old)

    def test_a_missing_shared_row_at_T0(self):
        old = [r for r in self.inputs["old"] if r[0] != T0_GEV]
        with self.assertRaisesRegex(bw.BuildError, "not exactly one row of the old"):
            self._compose(old=old)

    def test_a_perturbed_100_GeV_point(self):
        points = list(self.inputs["points"])
        T, w = points[0]
        self.assertEqual(T, 100.0)
        points[0] = (T, float(np.nextafter(w, 1.0)))  # one ulp
        with self.assertRaisesRegex(bw.BuildError, "100 GeV row"):
            self._compose(points=points)

    def test_an_out_of_order_row(self):
        notebook = list(self.inputs["notebook"])
        notebook[20], notebook[21] = notebook[21], notebook[20]
        with self.assertRaisesRegex(bw.BuildError, "strictly increasing"):
            self._compose(notebook=notebook)

    def test_a_left_out_row_moved_off_the_spline(self):
        notebook = list(self.inputs["notebook"])
        i = [round(T, 1) for T, _ in notebook].index(251.2)
        notebook[i] = (notebook[i][0], notebook[i][1] + 1e-5)  # 3e-5 in Sigma
        with self.assertRaisesRegex(bw.BuildError, "from the points' spline"):
            self._compose(notebook=notebook)


class TestTheClassOnTheInstalledTable(unittest.TestCase):
    """
    `Xav_EOS_spline`, the production class, on the installed table. It is built once (about a
    second).
    """

    # The electroweak peak of the spline on this table, measured by electroweak-eos prompt 01 with
    # scipy's minimize_scalar on the class's spline: Sigma = 0.0377570 at 54.019 GeV (the audit's
    # addendum, on the same rows above 50.1 keV: 0.037757 at 54.0 GeV). This test's grid, 3000 per
    # decade from 20 GeV, puts the argmax within 0.04 % in T of the peak (measured 54.037 GeV,
    # 0.033 %), and moves Sigma there by under 1e-8 (measured 3e-9), so the tolerances hold the
    # grid and the rounding of the pinned values with room: 2e-6 in Sigma and 0.2 % in T.
    EW_PEAK_SIGMA = 0.0377570
    EW_PEAK_T_GEV = 54.019
    EW_PEAK_SIGMA_TOLERANCE = 2e-6  # absolute
    EW_PEAK_T_TOLERANCE = 2e-3  # relative

    @classmethod
    def setUpClass(cls):
        cls.units = GeV_units()
        cls.eos = Xav_EOS_spline(cls.units)
        cls.GeV = cls.units.GeV

    def _sigma(self, T_GeV: float) -> float:
        return 1.0 - 3.0 * float(self.eos.w(T_GeV * self.GeV))

    def test_the_ends(self):
        """The class's T_min and T_max are the table's first and last rows."""
        self.assertEqual(self.eos._T_min, 1e-5)
        self.assertAlmostEqual(self.eos._T_max, TOP_T_GEV, delta=1e-9 * TOP_T_GEV)

    def test_sigma_is_zero_at_and_above_the_top_row(self):
        """Sigma = 0 exactly at T_max and above it, and dw/d ln T = 0 there."""
        for T in (self.eos._T_max, 386.71, 400.0, 1e3, 2e4, 1e16):
            with self.subTest(T_GeV=T):
                self.assertEqual(self._sigma(T), 0.0)
                self.assertEqual(self.eos.dw_dlogT(T * self.GeV), 0.0)

    def test_the_electroweak_peak(self):
        """The one maximum of Sigma between 20 GeV and 1 TeV, at about 54.0 GeV."""
        n = int(round(3000 * np.log10(1e3 / 20.0))) + 1
        Ts = np.logspace(np.log10(20.0), np.log10(1e3), n)
        S = np.array([self._sigma(T) for T in Ts])
        k = int(np.argmax(S))
        self.assertAlmostEqual(
            S[k], self.EW_PEAK_SIGMA, delta=self.EW_PEAK_SIGMA_TOLERANCE
        )
        self.assertAlmostEqual(
            Ts[k],
            self.EW_PEAK_T_GEV,
            delta=self.EW_PEAK_T_TOLERANCE * self.EW_PEAK_T_GEV,
        )
        # one maximum: Sigma rises to the peak and falls after it, on this grid
        self.assertTrue(np.all(np.diff(S[: k + 1]) >= 0.0))
        self.assertTrue(np.all(np.diff(S[k:]) <= 0.0))


if __name__ == "__main__":
    unittest.main()
