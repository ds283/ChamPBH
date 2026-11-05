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
The measured drain law (one-loop-drain prompt 02c §1.1, §1.6; README §8.10; orchestrator note 02c, rulings 1-3
and 9).

Under the one-loop law each candidate drains alpha_eff = min(DRAIN_ALPHA_CAP, A f) w_gate, with f
(drain_fraction) by the candidate's regime, L the model's (RealizedModel.drain_L; ExponentialModel's is
ln(1 + E/V0), U15 (b)) and ln R the assessment's:

  one_loop, ell >= 1:  d = ell + b0 - c ln L; min(f_cap, 1 - exp(-a/d)) if d > 0, else f_cap;
  one_loop, ell < 1:   f_plateau;
  gap:                 0 if ln R <= r0 or 1 + q ln L <= 0, else 1 - exp(-g (ln R - r0)^p (1 + q ln L));
  classical:           exp(ln R) S(L), S tabulated (drain_S_of_L);
  unassessed:          0.

The items are prompt §1.6's:
  - the law alone, each row at synthetic inputs, with ell = 1 exactly, the cap, d <= 0, ln R = r0 exactly,
    ln R <= 0, the unassessed case, A f >= alpha_cap and w_gate's two cases;
  - S(L) at every node, between nodes against an independent log-linear interpolation, below 0.3 and above
    134;
  - the reference: f equals fraction_adopted, copied verbatim below from
    prompts/one-loop-drain/source/crossover/measured_law_map.py at 1b16c62 with its table loading (ruling 3),
    on a grid covering every branch, bitwise but in two places where the production law departs from the
    reference: above L = 134 in the classical regime, where S is the ruled continuation (README §8.10), and in
    the gap where the reference is negative, where f is 0 (the user's ruling of 2026-11-05);
  - L computed stably (ruling 2), and U15: at n = 2, L = ln(1 + E/V0); at n = 1, L = M/phi_t to 1e-10
    relative (ruling 3), now ExponentialModel's _drain_L (the user's ruling of 2026-11-05: L in the model class);
  - the model hook: AbstractModel._drain_L raises, and the three entry points refuse a model without it under the
    one-loop law before integrating, while the same model runs under the fixed law;
  - the gap where 1 + q ln L <= 0 (L <= e^{-1/q} = 0.304): f = 0, by the user's ruling of 2026-11-05 on
    orchestrator note 02c's ruling 9, "I think probably we set `f = 0` there";
  - the removed prescriptions and constants.

Pure functions, the model's own classical_turning_point, and the P1 floor harness at M = 1e-10
(test_injection.integrate_floor, about 0.2 s a run, three runs): no Ray, no store, no PRyMordial solve.
"""

import contextlib
import importlib
import io
import unittest
from math import exp, log, log1p, nextafter
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_injection as ti
from ComputeTargets.tests.test_model_interface import _MinimalModel
from ComputeTargets.tests.test_wall_model import _PowerLawWall
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.AbstractModel import AbstractModel, ClassicalTurningPoint
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Units import Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")

# ---- the reference, verbatim (ruling 3) ----------------------------------------------------------------------
#
# The rows of prompts/one-loop-drain/source/crossover/out/stage1a_S_of_L.txt (at 1b16c62), copied as text, and
# loaded as measured_law_map.py loads that file. fraction_adopted and S_of are measured_law_map.py's at 1b16c62,
# copied verbatim; M_MP is its module global, set by each comparison below

_STAGE1A_S_OF_L_TXT = """\
# stage 1a: classical bounce off U = c(e^{1/phi} - 1), M = omega_t = 1; k/omega_t in [1e-3, 10], 121 points
# EPS = 1e-05, rtol = 1e-11, atol = 1e-14
#     L   ln(E/V0)  |beta|^2_max  k_peak   L^2|b|^2max       S(L)     L^2 S   note L^2S   S(EPS/100)  wall(s)
    0.3    -1.050   7.5486e-02   0.171       0.0068   1.0041e-02   0.0009        nan   1.0041e-02      8.3
    0.5    -0.433   7.3579e-02   0.171       0.0184   9.7145e-03   0.0024        nan   9.7145e-03      8.1
    0.7     0.014   7.1095e-02   0.171       0.0348   9.3185e-03   0.0046        nan   9.3185e-03      8.2
    1.0     0.541   6.6703e-02   0.171       0.0667   8.6688e-03   0.0087        nan   8.6688e-03      8.1
    2.0     1.855   5.0922e-02   0.171       0.2037   6.6167e-03   0.0265        nan   6.6167e-03      8.1
    3.0     2.949   3.8029e-02   0.185       0.3423   5.0735e-03   0.0457        nan   5.0735e-03      8.5
    4.0     3.982   2.8751e-02   0.200       0.4600   3.9750e-03   0.0636        nan   3.9751e-03      7.4
    5.0     4.993   2.2208e-02   0.215       0.5552   3.1852e-03   0.0796        nan   3.1852e-03      7.2
    6.0     5.998   1.7557e-02   0.215       0.6320   2.6041e-03   0.0937        nan   2.6042e-03      6.6
    7.0     6.999   1.4176e-02   0.233       0.6946   2.1664e-03   0.1062        nan   2.1664e-03      6.6
    8.0     8.000   1.1653e-02   0.233       0.7458   1.8292e-03   0.1171        nan   1.8292e-03      6.3
   10.0    10.000   8.2445e-03   0.251       0.8245   1.3529e-03   0.1353        nan   1.3529e-03      6.0
   13.0    13.000   5.3422e-03   0.271       0.9028   9.2294e-04   0.1560        nan   9.2292e-04      6.0
   16.0    16.000   3.7361e-03   0.271       0.9564   6.6916e-04   0.1713        nan   6.6913e-04      6.1
   20.0    20.000   2.5058e-03   0.293       1.0023   4.6603e-04   0.1864        nan   4.6603e-04      6.6
   30.0    30.000   1.1873e-03   0.293       1.0685   2.3302e-04   0.2097        nan   2.3302e-04      8.2
   40.0    40.000   6.8748e-04   0.316       1.1000   1.3936e-04   0.2230       0.22   1.3936e-04      9.6
   70.0    70.000   2.3359e-04   0.316       1.1446   4.9353e-05   0.2418       0.24   4.9353e-05     14.7
   90.0    90.000   1.4283e-04   0.316       1.1570   3.0600e-05   0.2479       0.25   3.0600e-05     17.8
  110.0   110.000   9.6245e-05   0.316       1.1646   2.0810e-05   0.2518       0.25   2.0810e-05     21.7
  134.0   134.000   6.5190e-05   0.316       1.1706   1.4204e-05   0.2551       0.26   1.4204e-05     26.3
# check: L = 70, k = 0.3: |beta|^2 = 2.325252e-04 (note: 2.32525e-4)
"""

_a = np.loadtxt(io.StringIO(_STAGE1A_S_OF_L_TXT), comments="#", usecols=(0, 5))
_lL, _lS = np.log(_a[:, 0]), np.log(_a[:, 1])

M_MP = None


def S_of(L):
    return float(np.exp(np.interp(log(L), _lL, _lS)))


def fraction_adopted(regime, ell, turning_point):
    """The law the user adopted on 2026-11-05 (README "Other M"; fit_law.py): one-loop
    min(0.95, 1 - exp(-2.69/(ell + 0.969 - 0.41 ln L))) for ell >= 1 and 0.9 below; gap
    1 - exp(-0.0198 (ln R - 1.5601)^1.5472 (1 + 0.841 ln L)) for ln R > 1.5601, else 0; classical R S(L).
    """
    if turning_point is None:
        return 0.9 if regime == SM.REGIME_ONE_LOOP and ell < 1.0 else 0.0
    L = M_MP / turning_point.phi_t
    lnR = turning_point.log_R
    if regime == SM.REGIME_ONE_LOOP:
        if ell < 1.0:
            return 0.9
        d = ell + 0.9691 - 0.41 * log(L)
        return min(0.95, 1.0 - exp(-2.69 / d)) if d > 0.0 else 0.95
    if regime == SM.REGIME_GAP:
        if lnR <= 1.5601:
            return 0.0
        return 1.0 - exp(-0.0198 * (lnR - 1.5601) ** 1.5472 * (1.0 + 0.841 * log(L)))
    if regime == SM.REGIME_CLASSICAL:
        return exp(lnR) * S_of(L)
    return 0.0


# ---- end of the verbatim copy ---------------------------------------------------------------------------------


def _ct(phi_t: float, log_R: float) -> ClassicalTurningPoint:
    """A synthetic turning point carrying phi_t and ln R; the reference reads nothing else."""
    return ClassicalTurningPoint(
        phi_t=phi_t,
        log_omega_t_squared=-150.0,
        log_R=log_R,
        lambda_t=1.0e20,
        log_k_ex=-70.0,
        ell_t=log(0.5),
    )


def _reference_with_departures(regime, ell, turning_point):
    """
    fraction_adopted with the production law's two departures: above L = 134 in the classical regime, S is the
    ruled continuation S(134) (134/L)^2 (README §8.10); in the gap, max(0.0, reference), by the user's ruling of
    2026-11-05 ("I think probably we set `f = 0` there"), which acts where 1 + q ln L < 0 and ln R > r0.
    """
    L = M_MP / turning_point.phi_t
    if regime == SM.REGIME_CLASSICAL and L > 134.0:
        return exp(turning_point.log_R) * (1.4204e-05 * (134.0 / L) ** 2)
    if regime == SM.REGIME_GAP:
        return max(0.0, fraction_adopted(regime, ell, turning_point))
    return fraction_adopted(regime, ell, turning_point)


# the reference grid (ruling 3): every branch of every regime
GRID_ELL = (
    1.0e-6,
    0.31,
    0.5,
    nextafter(1.0, 0.0),
    1.0,
    nextafter(1.0, 2.0),
    1.27,
    2.0,
    4.9,
    9.5,
    17.75,
    32.2579,
    50.0,
)
GRID_L = (
    1.0e-6,
    0.01,
    0.1,
    0.25,
    0.3,
    exp(-1.0 / 0.841),
    0.31,
    0.5,
    0.678,
    1.0,
    2.578,
    3.556,
    6.0,
    10.0,
    25.0,
    54.52,
    68.8,
    100.0,
    102.1,
    106.0,
    122.0,
    133.9,
    134.0,
    134.1,
    200.0,
    1.0e3,
    1.0e6,
)
GRID_LOG_R = (
    -100.0,
    -16.645,
    -1.0,
    -1.0e-12,
    0.0,
    1.0e-12,
    1.0,
    1.5601,
    nextafter(1.5601, 2.0),
    2.0,
    3.0,
    10.358,
    13.601,
    40.161,
    138.07,
)
GRID_M_MP = (4.0e-31, 5.0e-20, 1.0e-12)
REGIMES_ASSESSED = (SM.REGIME_ONE_LOOP, SM.REGIME_GAP, SM.REGIME_CLASSICAL)


class TestTheConstants(unittest.TestCase):
    def test_c_the_constants_and_their_sources(self):
        """README §8.10's constants, as fraction_adopted writes them (fit_law.txt's fits, rounded)."""
        self.assertEqual(SM.DRAIN_ALPHA_CAP, 0.99)
        self.assertEqual(SM.DRAIN_ONE_LOOP_A, 2.69)
        self.assertEqual(SM.DRAIN_ONE_LOOP_B0, 0.9691)
        self.assertEqual(SM.DRAIN_ONE_LOOP_C, 0.41)
        self.assertEqual(SM.DRAIN_ELL_PLATEAU, 1.0)
        self.assertEqual(SM.DRAIN_F_CAP, 0.95)
        self.assertEqual(SM.DRAIN_F_PLATEAU, 0.9)
        self.assertEqual(SM.DRAIN_GAP_G, 0.0198)
        self.assertEqual(SM.DRAIN_GAP_R0, 1.5601)
        self.assertEqual(SM.DRAIN_GAP_P, 1.5472)
        self.assertEqual(SM.DRAIN_GAP_Q, 0.841)
        self.assertEqual(SM.DRAIN_MODEL_FIXED, "fixed")
        self.assertEqual(SM.DRAIN_MODEL_ONE_LOOP, "one-loop-measured-v1")

    def test_c_the_S_table_is_the_files(self):
        """The 21 (L, S) literals equal stage1a_S_of_L.txt's columns L and S(L), read as the reference reads them."""
        self.assertEqual(len(SM.DRAIN_S_TABLE_L), 21)
        self.assertEqual(len(SM.DRAIN_S_TABLE_S), 21)
        self.assertEqual(tuple(_a[:, 0]), SM.DRAIN_S_TABLE_L)
        self.assertEqual(tuple(_a[:, 1]), SM.DRAIN_S_TABLE_S)

    def test_c_the_prescriptions_and_the_old_constants_are_gone(self):
        """Decision 2 (README §8.10): zero and edge are removed, and 2.7/ell with them."""
        for name in (
            "_GAP_PRESCRIPTION",
            "GAP_PRESCRIPTION_ZERO",
            "GAP_PRESCRIPTION_EDGE",
            "GAP_PRESCRIPTIONS",
            "DRAIN_ELL_EDGE",
            "DRAIN_C1",
        ):
            with self.subTest(name=name):
                self.assertFalse(hasattr(SM, name))
        self.assertNotIn("gap_prescription", SM.DrainRecord._fields)
        self.assertEqual(
            SM.DrainRecord._fields,
            (
                "law",
                "A",
                "model",
                "unresolvable_assessments",
                "candidates",
                "unresolved_declined",
            ),
        )
        self.assertEqual(
            SM.DrainCandidate._fields,
            ("regime", "ell", "alpha_eff", "w_gate", "log_E", "log_H", "L", "log_R"),
        )


class TestTheSTable(unittest.TestCase):
    def test_s_at_every_node(self):
        for L, S in zip(SM.DRAIN_S_TABLE_L, SM.DRAIN_S_TABLE_S):
            with self.subTest(L=L):
                # exp(ln S) need not round-trip: ln S (|ln S| <= 11.2) is rounded to within 1.8e-15
                # absolute, which exp carries into S as a relative error; the tolerance is 1e-14 relative
                self.assertAlmostEqual(SM.drain_S_of_L(L) / S, 1.0, delta=1.0e-14)
                self.assertEqual(SM.drain_S_of_L(L), S_of(L))

    def test_s_between_nodes_is_log_linear(self):
        """
        Against an independent evaluation of the log-linear interpolant, ln S = ln S_i + (ln L - ln L_i) (ln S_j
        - ln S_i)/(ln L_j - ln L_i), at three points in every interval. np.interp evaluates the same line in
        another order, so the two differ by rounding: |ln S| <= 11.2 here, so a few ulp of ln S is below 1e-14 in
        S; the tolerance is 1e-13 relative, ten times that.
        """
        Ls, Ss = SM.DRAIN_S_TABLE_L, SM.DRAIN_S_TABLE_S
        for i in range(len(Ls) - 1):
            lo, hi = log(Ls[i]), log(Ls[i + 1])
            slo, shi = log(Ss[i]), log(Ss[i + 1])
            for t in (0.1, 0.5, 0.9):
                lnL = lo + t * (hi - lo)
                L = exp(lnL)
                expected = exp(slo + (log(L) - lo) * (shi - slo) / (hi - lo))
                with self.subTest(L=L):
                    self.assertAlmostEqual(
                        SM.drain_S_of_L(L) / expected, 1.0, delta=1.0e-13
                    )
                    self.assertGreater(SM.drain_S_of_L(L), min(Ss[i], Ss[i + 1]))
                    self.assertLess(SM.drain_S_of_L(L), max(Ss[i], Ss[i + 1]))

    def test_s_below_the_table_holds_S_at_0_3(self):
        for L in (0.3, 0.2999, 0.1, 1.0e-3, 1.0e-12):
            with self.subTest(L=L):
                self.assertEqual(SM.drain_S_of_L(L), SM.drain_S_of_L(0.3))
                self.assertEqual(SM.drain_S_of_L(L), S_of(L))

    def test_s_above_the_table_falls_as_L_squared(self):
        """S(134) (134/L)^2 above 134: L^2 S tends to a constant (production_energy_ratio.tex; README §8.10)."""
        for L in (nextafter(134.0, 200.0), 134.1, 200.0, 1.0e3, 1.0e6):
            with self.subTest(L=L):
                self.assertEqual(SM.drain_S_of_L(L), 1.4204e-05 * (134.0 / L) ** 2)
                self.assertAlmostEqual(
                    L * L * SM.drain_S_of_L(L) / (134.0 * 134.0 * 1.4204e-05),
                    1.0,
                    delta=1.0e-15,
                )
        # continuous at 134 to rounding
        self.assertAlmostEqual(
            SM.drain_S_of_L(nextafter(134.0, 200.0)) / SM.drain_S_of_L(134.0),
            1.0,
            delta=1.0e-14,
        )
        # where the reference (np.interp) instead holds S(134)
        self.assertEqual(S_of(200.0), S_of(134.0))


class TestTheLaw(unittest.TestCase):
    """Each row of prompt §1.1's table at synthetic inputs."""

    def f(self, regime, ell=None, L=None, log_R=None):
        return SM.drain_fraction(regime, ell, L, log_R)

    def test_l_one_loop_at_and_below_ell_one(self):
        L = 6.0
        # ell = 1 exactly takes the exponential, not the plateau
        d = 1.0 + 0.9691 - 0.41 * log(L)
        self.assertEqual(self.f("one_loop", 1.0, L, 3.0), 1.0 - exp(-2.69 / d))
        self.assertNotEqual(self.f("one_loop", 1.0, L, 3.0), 0.9)
        # just below, the plateau, which reads neither L nor ln R
        self.assertEqual(self.f("one_loop", nextafter(1.0, 0.0), None, None), 0.9)
        self.assertEqual(self.f("one_loop", 0.31, None, None), 0.9)

    def test_l_one_loop_the_cap_and_d_at_or_below_zero(self):
        # d = 1.9691 - 1.23 = 0.7391: 1 - exp(-2.69/d) = 0.974, capped at 0.95
        L = exp(3.0)
        d = 1.0 + 0.9691 - 0.41 * log(L)
        self.assertGreater(1.0 - exp(-2.69 / d), 0.95)
        self.assertEqual(self.f("one_loop", 1.0, L, 3.0), 0.95)
        # d <= 0: ln L >= (ell + 0.9691)/0.41
        for L in (1.0e3, 1.0e6, exp((1.0 + 0.9691) / 0.41) * 1.0000001):
            with self.subTest(L=L):
                self.assertLessEqual(1.0 + 0.9691 - 0.41 * log(L), 0.0)
                self.assertEqual(self.f("one_loop", 1.0, L, 3.0), 0.95)
        # an uncapped value
        self.assertEqual(
            self.f("one_loop", 9.5, 26.74, 30.0),
            1.0 - exp(-2.69 / (9.5 + 0.9691 - 0.41 * log(26.74))),
        )

    def test_l_gap(self):
        L = 3.556
        self.assertEqual(self.f("gap", None, L, 1.5601), 0.0)
        for log_R in (1.0, 0.0, -1.0, -50.0):
            with self.subTest(log_R=log_R):
                self.assertEqual(self.f("gap", None, L, log_R), 0.0)
        # just above r0 the fraction is positive and small (at the next float above r0 it is 0.0 in floats,
        # since 1 - exp(-y) rounds to 0 for y ~ 1e-25)
        above = 1.5601 + 1.0e-6
        self.assertGreater(self.f("gap", None, L, above), 0.0)
        self.assertLess(self.f("gap", None, L, above), 1.0e-9)
        self.assertEqual(self.f("gap", None, L, nextafter(1.5601, 2.0)), 0.0)
        self.assertEqual(
            self.f("gap", None, L, 13.601),
            1.0 - exp(-0.0198 * (13.601 - 1.5601) ** 1.5472 * (1.0 + 0.841 * log(L))),
        )

    def test_l_classical(self):
        for L, log_R in ((54.52, -16.645), (0.678, 0.0), (6.0, -1.0e-12)):
            with self.subTest(L=L, log_R=log_R):
                self.assertEqual(
                    self.f("classical", None, L, log_R),
                    exp(log_R) * SM.drain_S_of_L(L),
                )

    def test_l_unassessed(self):
        self.assertEqual(self.f("unassessed"), 0.0)
        for w in (1.0, 0.5):
            with self.subTest(w=w):
                value = SM.drain_alpha_effective("unassessed", None, None, None, 1.0, w)
                self.assertEqual(value, 0.0)
                self.assertEqual(str(value), "0.0")

    def test_l_alpha_eff_the_cap_and_the_gate(self):
        f = self.f("one_loop", 9.5, 26.74, 30.0)
        for A, w in ((1.0, 1.0), (0.05, 1.0), (1.0, 0.37), (0.05, 0.0)):
            with self.subTest(A=A, w=w):
                self.assertEqual(
                    SM.drain_alpha_effective("one_loop", 9.5, 26.74, 30.0, A, w),
                    min(0.99, A * f) * w,
                )
        # A f >= alpha_cap: the cap, then the gate
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 10.0, 1.0), 0.99
        )
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 1.1, 1.0), 0.99
        )
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 10.0, 0.5),
            0.99 * 0.5,
        )
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 1.0, 1.0), 0.9
        )
        # nothing drained in the gap below r0 and with w_gate = 0: the caller's alpha_eff = 0 branch
        for args in (
            ("gap", None, 3.556, 1.0, 1.0, 1.0),
            ("one_loop", 9.5, 26.74, 30.0, 1.0, 0.0),
        ):
            with self.subTest(args=args):
                value = SM.drain_alpha_effective(*args)
                self.assertEqual(value, 0.0)
                self.assertFalse(value > 0.0)
        # the classical regime drains: R S(L), however small
        value = SM.drain_alpha_effective("classical", None, 54.52, -16.645, 0.2, 1.0)
        self.assertEqual(
            value, min(0.99, 0.2 * (exp(-16.645) * SM.drain_S_of_L(54.52)))
        )
        self.assertGreater(value, 0.0)

    def test_l_an_unknown_regime_is_refused(self):
        with self.assertRaises(ValueError):
            SM.drain_fraction("elastic", None, 1.0, 1.0)
        with self.assertRaises(ValueError):
            SM.drain_alpha_effective("elastic", None, 1.0, 1.0, 1.0, 1.0)


class TestTheReference(unittest.TestCase):
    """
    f and alpha_eff against the verbatim fraction_adopted (ruling 3), on GRID_ELL x GRID_L x GRID_LOG_R x
    GRID_M_MP for every assessed regime: L = M_MP/phi_t by the reference's own expression, given to the
    production law as a number. Bitwise everywhere but at the two departures, where it is bitwise against
    _reference_with_departures: the classical regime above L = 134 (the ruled continuation) and the gap where the
    reference is negative (the user's ruling of 2026-11-05: 0.0).
    """

    def test_r_bitwise_on_the_grid(self):
        global M_MP
        saved = M_MP
        compared = 0
        departed_by = {regime: 0 for regime in REGIMES_ASSESSED}
        try:
            for M_MP in GRID_M_MP:
                for L_target in GRID_L:
                    phi_t = M_MP / L_target
                    L = M_MP / phi_t
                    for log_R in GRID_LOG_R:
                        tp = _ct(phi_t, log_R)
                        for regime in REGIMES_ASSESSED:
                            ells = GRID_ELL if regime == SM.REGIME_ONE_LOOP else (None,)
                            for ell in ells:
                                f = SM.drain_fraction(regime, ell, L, log_R)
                                expected = _reference_with_departures(regime, ell, tp)
                                if expected != fraction_adopted(regime, ell, tp):
                                    departed_by[regime] += 1
                                compared += 1
                                if f != expected:
                                    self.fail(
                                        f"f differs at regime={regime} ell={ell!r} L={L!r} "
                                        f"log_R={log_R!r} M={M_MP!r}: {f!r} != {expected!r}"
                                    )
                                for A, w in ((0.05, 1.0), (1.0, 0.37), (10.0, 1.0)):
                                    alpha = SM.drain_alpha_effective(
                                        regime, ell, L, log_R, A, w
                                    )
                                    if (
                                        alpha
                                        != min(SM.DRAIN_ALPHA_CAP, A * expected) * w
                                    ):
                                        self.fail(
                                            f"alpha_eff differs at regime={regime} ell={ell!r} L={L!r} "
                                            f"log_R={log_R!r} A={A} w={w}"
                                        )
        finally:
            M_MP = saved
        self.assertEqual(
            compared,
            len(GRID_M_MP) * len(GRID_L) * len(GRID_LOG_R) * (len(GRID_ELL) + 2),
        )
        # both departures are exercised, and nothing else departs
        self.assertGreater(departed_by[SM.REGIME_CLASSICAL], 0)
        self.assertGreater(departed_by[SM.REGIME_GAP], 0)
        self.assertEqual(departed_by[SM.REGIME_ONE_LOOP], 0)

    def test_r_the_departure_is_the_continuation_only(self):
        """Above 134 in the classical regime the reference holds S(134); the production law falls as L^-2."""
        global M_MP
        saved = M_MP
        try:
            M_MP = 4.0e-31
            tp = _ct(M_MP / 200.0, -1.0)
            L = M_MP / tp.phi_t
            self.assertNotEqual(
                SM.drain_fraction("classical", None, L, -1.0),
                fraction_adopted("classical", None, tp),
            )
            self.assertEqual(
                fraction_adopted("classical", None, tp), exp(-1.0) * S_of(134.0)
            )
        finally:
            M_MP = saved


class TestTheGapBelowLZero(unittest.TestCase):
    """
    The user's ruling of 2026-11-05 on orchestrator note 02c's ruling 9, "I think probably we set `f = 0` there":
    with q = 0.841, 1 + q ln L <= 0 for L <= e^{-1/q} = 0.3045, where README §8.10's gap expression (and
    fraction_adopted) is negative for ln R > r0. There f is 0.0 exactly.
    """

    def test_g_zero_below_the_boundary_and_positive_above(self):
        L0 = exp(-1.0 / SM.DRAIN_GAP_Q)
        self.assertAlmostEqual(L0, 0.30451, places=5)
        below = above = L0
        while 1.0 + SM.DRAIN_GAP_Q * log(below) > 0.0:
            below = nextafter(below, 0.0)
        while 1.0 + SM.DRAIN_GAP_Q * log(above) <= 0.0:
            above = nextafter(above, 1.0)
        for log_R in (1.6, 2.0, 13.601, 40.0):
            with self.subTest(log_R=log_R):
                for L in (below, 0.99 * L0, 0.01, 1.0e-6):
                    self.assertEqual(SM.drain_fraction("gap", None, L, log_R), 0.0)
                    self.assertEqual(
                        str(SM.drain_alpha_effective("gap", None, L, log_R, 1.0, 1.0)),
                        "0.0",
                    )
                # at the first float above the boundary s ~ 1e-16 and 1 - exp(-y) can round to 0.0, but
                # the expression is taken; a relative step of 1e-6 above it is positive at every ln R here
                self.assertGreaterEqual(
                    SM.drain_fraction("gap", None, above, log_R), 0.0
                )
                self.assertGreater(
                    SM.drain_fraction("gap", None, (1.0 + 1.0e-6) * L0, log_R), 0.0
                )
                self.assertGreater(
                    SM.drain_fraction("gap", None, 1.01 * L0, log_R), 0.0
                )

    def test_g_the_reference_is_negative_there(self):
        """The departure from fraction_adopted that the ruling makes: production is max(0.0, reference)."""
        global M_MP
        saved = M_MP
        try:
            M_MP = 4.0e-31
            tp = _ct(M_MP / 0.01, 13.601)
            self.assertLess(fraction_adopted("gap", None, tp), 0.0)
            self.assertEqual(
                SM.drain_fraction("gap", None, M_MP / tp.phi_t, 13.601), 0.0
            )
        finally:
            M_MP = saved


def _exponential(n=1, M_Mp=4.0e-31):
    units = Planck_units()
    return (
        ExponentialModel(
            0, M_value(0, M_Mp * units.PlanckMass), n, beta_value(0, 1.185), units
        ),
        units,
    )


class TestLAndU15(unittest.TestCase):
    """
    Ruling 2 (L computed stably) and U15 (b): ExponentialModel's L = ln(1 + E/V0) at every n, the model's hook
    _drain_L and its public route RealizedModel.drain_L (the user's ruling of 2026-11-05: L in the model class).
    """

    NORMALIZATION = -276.63072859401797

    def _realized(self, n, M_Mp=4.0e-31):
        model, units = _exponential(n, M_Mp)
        return (
            model.realize(self.NORMALIZATION, 8.645101631245453e-16 * units.PlanckMass),
            units,
        )

    def test_u_L_is_stable_for_every_finite_x(self):
        model, _ = _exponential()
        for x in (-30.0, 0.0, 30.0):
            with self.subTest(x=x):
                self.assertAlmostEqual(
                    model._drain_L(-276.6, x - 276.6) / log1p(exp(x)),
                    1.0,
                    delta=1.0e-15,
                )
        # log1p(exp(800)) overflows; the stable form returns 800
        with self.assertRaises(OverflowError):
            log1p(exp(800.0))
        self.assertEqual(model._drain_L(0.0, 800.0), 800.0)

    def test_u_L_at_the_maps_x(self):
        """At the four maps' ln(E/V0) (prompt §2.3), against log1p(exp(x)) to 2 ulp."""
        model, _ = _exponential()
        for x in (
            106.0437,
            102.0518,
            91.4068,
            54.5244,
            24.9951,
            3.4802,
            2.4992,
            0.5,
            -1.05,
        ):
            with self.subTest(x=x):
                expected = log1p(exp(x))
                self.assertLessEqual(
                    abs(model._drain_L(0.0, x) - expected), 2.0 * np.spacing(expected)
                )

    def test_u_the_public_route_is_the_hook(self):
        realized, _ = self._realized(1)
        for x in (-1.05, 3.0, 106.0):
            with self.subTest(x=x):
                log_E = self.NORMALIZATION + x
                self.assertEqual(
                    realized.drain_L(log_E),
                    realized.model._drain_L(self.NORMALIZATION, log_E),
                )
        self.assertTrue(realized.drain_L_implemented)
        self.assertTrue(realized.model.drain_L_implemented)

    def test_u_n2_takes_ln_one_plus_E_over_V0(self):
        realized, units = self._realized(2)
        M = 4.0e-31 * units.PlanckMass
        for x in (0.5, 3.0, 25.0, 100.0):
            with self.subTest(x=x):
                log_E = realized.log_V_floor + x
                phi = 1.0e-20 * units.PlanckMass
                ct = realized.classical_turning_point(phi, log_E)
                L, log_R = SM.drain_law_inputs(realized, (phi, log_E, ct))
                self.assertEqual(L, realized.drain_L(log_E))
                self.assertAlmostEqual(L / log1p(exp(x)), 1.0, delta=1.0e-15)
                self.assertEqual(log_R, ct.log_R)
                # at n = 2, (M/phi_t)^2 = L, so M/phi_t is not L
                self.assertAlmostEqual((M / ct.phi_t) ** 2 / L, 1.0, delta=1.0e-10)
                self.assertGreater(abs(M / ct.phi_t - L) / L, 1.0e-3)

    def test_u_n1_equals_M_over_phi_t(self):
        """At n = 1, ln(1 + E/V0) = M/phi_t exactly; phi_t is a root to about 1e-12 relative (ruling 3: 1e-10)."""
        for M_Mp in (4.0e-31, 5.0e-20, 1.0e-12):
            realized, units = self._realized(1, M_Mp)
            M = M_Mp * units.PlanckMass
            for x in (0.5, 3.0, 25.0, 100.0, 130.0):
                with self.subTest(M=M_Mp, x=x):
                    log_E = realized.log_V_floor + x
                    phi = 1.0e3 * M
                    ct = realized.classical_turning_point(phi, log_E)
                    L, _ = SM.drain_law_inputs(realized, (phi, log_E, ct))
                    self.assertAlmostEqual(L / (M / ct.phi_t), 1.0, delta=1.0e-10)

    def test_u_unassessed_has_no_inputs(self):
        realized, _ = self._realized(1)
        self.assertEqual(SM.drain_law_inputs(realized, None), (None, None))


class _NoDrainL(ExponentialModel):
    """ExponentialModel with AbstractModel's _drain_L restored: a model that does not define the law's L."""

    _drain_L = AbstractModel._drain_L


def _floor_harness(model_cls):
    """test_injection.build_with_ledger's P1 harness at M = 1e-10, with the model's class given."""
    model = model_cls(
        0,
        M_value(0, 1e-10 * kcl._units.PlanckMass),
        1,
        beta_value(0, kcl.P1[0]),
        kcl._units,
    ).realize(4.0 * log(1e-3 * kcl._units.eV), 0.0)
    ledger = DarkRadiationLedger()
    policy = SM.ODEPolicy("test", kcl._cosmology, model, dark_radiation=ledger)
    rhs = SM.ODERHS("test", policy)
    supervisor = ti.ScalarFieldIntegrationSupervisor(
        kcl._units,
        kcl._T_init,
        kcl._T_stop,
        label="test",
        collect_full_statistics=False,
    )
    return policy, rhs, supervisor


def _integrate_floor(model_cls, alpha, drain_law):
    """P1 to N = 21 (test_injection.integrate_floor), about 0.2 s."""
    _, N0, state = kcl.P1
    policy, rhs, supervisor = _floor_harness(model_cls)
    with contextlib.redirect_stdout(io.StringIO()), supervisor:
        return SM.integrate_scalar_history(
            rhs,
            supervisor,
            ti.StateVector._make(state),
            N0,
            kcl._log_T_stop,
            SM.StepControl(),
            policy=policy,
            N_stop=21.0,
            alpha=alpha,
            drain_law=drain_law,
        )


class TestTheModelHook(unittest.TestCase):
    """
    The user's ruling of 2026-11-05: "I suggest we move `L` into the model class, with no default. Then when a new
    potential is implemented, breakage is loud, and the implementer has to think about what they are changing."
    AbstractModel._drain_L raises; under the one-loop law the three entry points refuse a model without it before
    integrating (tune_history before the seed); under the fixed law such a model runs as before.
    """

    def _assert_names_the_hook(self, caught, context):
        message = str(caught.exception)
        self.assertTrue(message.startswith(f"{context}: "), message)
        self.assertIn("_NoDrainL", message)
        self.assertIn("_drain_L", message)

    def test_h_the_base_hook_raises_for_a_model_lacking_it(self):
        for model in (
            _PowerLawWall(1.0e-20),
            _MinimalModel(0.5),
            _NoDrainL(0, M_value(0, 1.0e-10), 1, beta_value(0, 2.0), Planck_units()),
        ):
            with self.subTest(model=type(model).__name__):
                self.assertFalse(model.drain_L_implemented)
                self.assertFalse(model.realize(0.0, 1.0).drain_L_implemented)
                with self.assertRaises(NotImplementedError) as caught:
                    model._drain_L(0.0, 1.0)
                self.assertIn(type(model).__name__, str(caught.exception))
                self.assertIn("one-loop drain law", str(caught.exception))
                with self.assertRaises(NotImplementedError):
                    model.realize(0.0, 1.0).drain_L(1.0)

    def test_h_integrate_scalar_history_refuses_before_any_step(self):
        with mock.patch.object(SM, "Radau") as radau:
            with self.assertRaises(ValueError) as caught:
                _integrate_floor(_NoDrainL, 1.0, SM.DRAIN_LAW_ONE_LOOP)
        radau.assert_not_called()
        self._assert_names_the_hook(caught, "integrate_scalar_history")

    def test_h_integrate_history_refuses_before_any_integration(self):
        cosmology = gth.make_cosmology()
        units = cosmology.units
        model = gth.make_model(units, cls=_NoDrainL).realize(0.0, 0.0)
        with mock.patch.object(SM, "integrate_scalar_history") as integrate:
            with self.assertRaises(ValueError) as caught:
                SM.integrate_history(
                    cosmology,
                    temperature(0, 2.0e4 * units.GeV),
                    temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                    phi_value(0, 5.0 * units.PlanckMass),
                    pi_value(0, 0.0),
                    gth.make_z_grid(),
                    model,
                    alpha=1.0,
                    drain_law=SM.DRAIN_LAW_ONE_LOOP,
                )
        integrate.assert_not_called()
        self._assert_names_the_hook(caught, "integrate_history")

    def test_h_tune_history_refuses_before_the_seed(self):
        cosmology = gth.make_cosmology()
        units = cosmology.units
        model = gth.make_model(units, cls=_NoDrainL)
        with mock.patch.object(_NoDrainL, "seed") as seed, mock.patch.object(
            SM, "integrate_history"
        ) as integrate:
            with self.assertRaises(ValueError) as caught:
                SM.tune_history(
                    cosmology,
                    model,
                    temperature(0, 2.0e4 * units.GeV),
                    phi_value(0, 5.0 * units.PlanckMass),
                    pi_value(0, 0.0),
                    gth.make_z_grid(),
                    alpha=1.0,
                    drain_law=SM.DRAIN_LAW_ONE_LOOP,
                )
        seed.assert_not_called()
        integrate.assert_not_called()
        self._assert_names_the_hook(caught, "tune_history")

    def test_h_the_same_model_runs_under_the_fixed_law(self):
        """At alpha = 0.1, the fixed law: the run equals ExponentialModel's, bit for bit."""
        ours = _integrate_floor(_NoDrainL, 0.1, SM.DRAIN_LAW_FIXED)
        theirs = _integrate_floor(ExponentialModel, 0.1, SM.DRAIN_LAW_FIXED)
        self.assertGreater(len(ours.injections), 0)
        self.assertEqual(ours.reflections, theirs.reflections)
        self.assertEqual(ours.nfev, theirs.nfev)
        self.assertEqual(tuple(ours.final_state), tuple(theirs.final_state))
        self.assertEqual(ours.drain, theirs.drain)
        # and the fixed-law check passes it at each entry point
        for context in (
            "integrate_scalar_history",
            "integrate_history",
            "tune_history",
        ):
            SM.check_drain_law_model(
                SM.DRAIN_LAW_FIXED, _NoDrainL.__new__(_NoDrainL), context
            )


if __name__ == "__main__":
    unittest.main()
