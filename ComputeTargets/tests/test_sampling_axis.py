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
The sampling axis (jordan-normalization prompt 03, Z1, Z3, Z4 and Z5).

A history is sampled on points ln(1 + z_E) = k * DELTA, DELTA = ln 10 / S, counted back from its
end (`CosmologyConcepts.redshift.log_one_plus_z_grid`); `compute_scalar_history` reads N_backward
from the axis, and each sample carries ln(1 + z_J) = ln(1 + z_E) + ln A(phi_0) - ln A(phi).

- `TestTheGrid`: the helper's points are k * DELTA bit for bit, and a grid of 400 decades
  (N about 921) cut at N = 800 by `truncate_to_history` reads `z` on every point (inf past float
  range) without raising. No integration.
- `TestTheGridTooShort`: the configuration error when the grid ends before the history does,
  through `compute_scalar_history._function` on the golden beta = 1.5 setup with a 5-decade grid
  (N = 11.5 against the history's 25.1). It integrates: about 0.6 s.
- `TestTheJordanRedshift`: on the golden beta = 1.5 history (golden_history.py), run once in
  setUpClass (about 0.6 s), ln(1 + z_J) is the identity exactly, and agrees with the redshift of the
  Jordan-frame temperature, ln(1 + cosmology.z(T_J)) - ln(1 + cosmology.z(T_J,end)), to 1e-6 at every
  sample, bounces included (entropy conservation in the Jordan frame).

No Ray cluster, no datastore, no PRyMordial solve.
"""

import contextlib
import io
import math
import unittest
from math import exp, log
from unittest import mock

from ComputeTargets.ScalarHistory import SampleValues, truncate_to_history
from ComputeTargets.tests import golden_history as gh
from CosmologyConcepts import redshift, redshift_array
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.redshift import log_one_plus_z_grid
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

# the largest ln(1 + z) whose z is a finite float: ln(DBL_MAX) = 709.78
LOG_DBL_MAX = math.log(1.7976931348623157e308)

# the largest difference allowed between ln(1 + z_J) and the redshift of the Jordan-frame
# temperature (the orchestrator's measurement before the change: 3.3e-7 at beta = 1.5)
JORDAN_REDSHIFT_TOLERANCE = 1.0e-6


def axis(samples_per_log10_z: int, log10_one_plus_z_high: float) -> redshift_array:
    xs = log_one_plus_z_grid(samples_per_log10_z, log10_one_plus_z_high)
    return redshift_array(z_array=[redshift(k, x) for k, x in enumerate(xs)])


class TestTheGrid(unittest.TestCase):
    def test_the_points_are_integer_multiples_of_delta(self):
        """Each point is k * DELTA, computed from the integer k, never accumulated: equal bit for
        bit, so that two runs (and main.py and the tests) build the same floats."""
        for S, H in ((250, 85), (250, 300), (250, 35), (100, 7.5)):
            DELTA = math.log(10.0) / S
            K = int(round(H * S))
            xs = log_one_plus_z_grid(S, H)
            self.assertEqual(len(xs), K + 1, (S, H))
            self.assertEqual(xs[0], 0.0)
            for k, x in enumerate(xs):
                self.assertEqual(x.hex(), (k * DELTA).hex(), (S, H, k))

    def test_the_production_grid(self):
        """At 250 per decade and 85 decades (exponential.yaml): K = 21250, the top point
        ln(1 + z) = 195.7; at the parser's default of 300 decades, 75 001 points to 690.8.
        """
        xs = log_one_plus_z_grid(250, 85)
        self.assertEqual(len(xs), 21251)
        self.assertEqual(xs[-1], 21250 * (math.log(10.0) / 250))
        self.assertAlmostEqual(xs[-1], 85 * math.log(10.0), places=10)
        self.assertEqual(len(log_one_plus_z_grid(250, 300)), 75001)

    def test_a_grid_past_float_range_cuts_and_reads_without_overflow(self):
        """400 decades (N = 921), cut at a synthetic final_N = 800 by the sampling's helper: the
        points kept are those with ln(1 + z) <= 800, and z reads as a float on every point, inf
        beyond ln(DBL_MAX) = 709.78, without raising."""
        grid = axis(250, 400)
        self.assertGreater(grid.max.log_one_plus_z, 921.0)
        with self.assertRaises(OverflowError):
            math.exp(grid.max.log_one_plus_z)

        final_N = 800.0
        cut = truncate_to_history(grid, final_N)

        DELTA = math.log(10.0) / 250
        self.assertEqual(len(cut), int(final_N // DELTA) + 1)
        self.assertLessEqual(cut.max.log_one_plus_z, final_N)
        self.assertEqual(cut.min.log_one_plus_z, 0.0)
        # the cut is descending in ln(1 + z), as the sample loop reads it
        labels = [p.log_one_plus_z for p in cut]
        self.assertEqual(labels, sorted(labels, reverse=True))

        n_inf = 0
        for p in grid:
            z = p.z
            self.assertIsInstance(z, float)
            if p.log_one_plus_z > LOG_DBL_MAX:
                self.assertEqual(z, math.inf)
                n_inf += 1
            else:
                self.assertTrue(math.isfinite(z), p.log_one_plus_z)
                self.assertEqual(z, math.expm1(p.log_one_plus_z))
        self.assertGreater(n_inf, 0)
        # on the cut too: N_backward is the axis value itself, never a log of z
        self.assertTrue(any(p.z == math.inf for p in cut))

    def test_a_point_just_past_the_end_is_kept(self):
        """The truncation keeps a point at most 1e-10 beyond final_N, and no further."""
        grid = axis(250, 5)
        DELTA = math.log(10.0) / 250
        x = 100 * DELTA
        self.assertEqual(len(truncate_to_history(grid, x - 0.5e-10)), 101)
        self.assertEqual(len(truncate_to_history(grid, x - 2.0e-10)), 100)

    def test_the_label_is_not_negative_and_float_is_the_label(self):
        """A redshift refuses a negative or NaN ln(1 + z); float() of one is its ln(1 + z)."""
        with self.assertRaises(ValueError):
            redshift(0, -1e-12)
        with self.assertRaises(ValueError):
            redshift(0, math.nan)
        p = redshift(7, 2.5)
        self.assertEqual(float(p), 2.5)
        self.assertEqual(p.z, math.expm1(2.5))
        self.assertEqual(redshift(1, 800.0).z, math.inf)


class TestTheGridTooShort(unittest.TestCase):
    def test_a_grid_that_ends_before_the_history_is_a_configuration_error(self):
        """The golden beta = 1.5 history (final_N = 25.1) on a 5-decade grid (to N = 11.5): the
        RuntimeError of compute_scalar_history, naming the largest supplied redshift. It runs the
        integration first: about 0.6 s."""
        with mock.patch.object(
            gh, "make_z_grid", lambda: axis(gh.SAMPLES_PER_LOG10_Z, 5)
        ), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as ctx:
                gh.run_history(1.5)
        message = str(ctx.exception)
        self.assertIn("largest supplied redshift", message)
        self.assertIn(f"ln(1 + z)={5 * math.log(10.0):.6g}", message)
        self.assertIn("solution required N=25.07", message)


class TestTheJordanRedshift(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with contextlib.redirect_stdout(io.StringIO()):
            cls.data = gh.run_history(1.5)
        cls.units = Planck_units()
        cls.cosmology = QCD_Cosmology(0, cls.units, Planck2018())
        # the realized model the history was computed with (jordan-normalization prompt 04)
        cls.model = ExponentialModel.from_rows(*gh.rows(1.5, cls.units))
        cls.samples = cls.data["sample"]
        cls.points = list(cls.data["z_grid"])

    def test_the_new_field_is_the_last(self):
        self.assertEqual(SampleValues._fields[-1], "log_one_plus_z_Jordan")
        self.assertEqual(len(SampleValues._fields), 18)

    def test_the_identity_holds_exactly(self):
        """ln(1 + z_J) = ln(1 + z_E) + (ln A(phi_0) - ln A(phi)) at every sample, as computed, with
        phi_0 the phi of the k = 0 sample (the end of the history); 0.0 exactly there.
        """
        self.assertEqual(self.points[-1].log_one_plus_z, 0.0)
        end = self.samples[-1]
        log_A_0 = self.model.log_Omega(end.phi_Einstein)

        self.assertEqual(end.log_one_plus_z_Jordan, 0.0)
        for i, (p, s) in enumerate(zip(self.points, self.samples)):
            expected = p.log_one_plus_z + (
                log_A_0 - self.model.log_Omega(s.phi_Einstein)
            )
            self.assertEqual(s.log_one_plus_z_Jordan, expected, i)

    def test_the_jordan_redshift_is_that_of_the_jordan_temperature(self):
        """|ln(1 + z_J) - (ln(1 + z(T_J)) - ln(1 + z(T_J,end)))| < 1e-6 at every sample, bounces
        (H_J < 0) included, with z(T) the cosmology's redshift of a Jordan-frame temperature
        (entropy conservation in the Jordan frame) and T_J,end that of the z = 0 sample.
        """
        cosmology = self.cosmology
        log_end = log(1.0 + cosmology.z(exp(self.samples[-1].log_T_Jordan)))
        worst = (0.0, None)
        for i, s in enumerate(self.samples):
            from_T = log(1.0 + cosmology.z(exp(s.log_T_Jordan))) - log_end
            diff = abs(s.log_one_plus_z_Jordan - from_T)
            if diff > worst[0]:
                worst = (diff, i)
        self.assertLess(worst[0], JORDAN_REDSHIFT_TOLERANCE, worst)
        # the history has a bounce: some samples have H_J < 0, where a_J falls
        self.assertTrue(any(s.H_Jordan < 0.0 for s in self.samples))


if __name__ == "__main__":
    unittest.main()
