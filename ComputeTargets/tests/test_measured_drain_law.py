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
The exponential's one-loop drain law, one-loop-exponential-v2 (power-law-wall prompt 07c §1.3, §1.7; README §6.13 U13,
§6.14 P99, P100, U14). Until 07c this module tested v1, the measured law of one-loop-drain prompt 02c in
ComputeTargets/ScalarHistory.py; v1 read ell in the old k_ex, which R13-R15 retire, and its tests become v2's here, by
the same structure where it fits.

Under the one-loop law each candidate drains alpha_eff = min(DRAIN_ALPHA_CAP, A f) w_gate (drain_alpha_effective),
with f the model's own fraction (ExponentialModel._drain_fraction, one_loop_fraction) by the candidate's regime, L the
model's (ln(1 + E/V0), U15 (b)) and ln R the assessment's:

  classical (and any ln R <= 0):  exp(ln R) S(L), S tabulated (drain_S_of_L), as v1;
  one_loop and gap:               1 - exp(-min(y_rise, y_fall)),
                                  y_rise = G_n (ln R - R0_n)^P_n (1 + Q_n ln L), 0 where ln R <= R0_n or 1 + Q_n ln L <= 0,
                                  y_fall = A_f/(ell - ell0) at a one-loop candidate with ell > ell0, +inf otherwise;
  unassessed:                     0.

The items are prompt §1.7's:
  - the constants: equal, bit for bit, to 07b2's joint fit (.documents/power-law-wall/kex-07b2/minform.json, with_new,
    n12/S3/v-J), read here from that file; v1's constants and module-level drain_fraction are gone;
  - S(L) at every node, between nodes against an independent log-linear interpolation, below 0.3 and above 134 (moved
    with the table into ExponentialModel.py, unchanged in value, P99);
  - the law alone at synthetic inputs covering every branch: the rise below R0, the rise with 1 + Q ln L <= 0, the rise
    active at a one-loop candidate, the fall active, ell <= ell0, the gap, ln R -> 0+, classical, unassessed, and
    alpha_eff's cap and gate;
  - the reference: one_loop_fraction equals minform.fraction (07b2's reference implementation,
    prompts/power-law-wall/source/kex/minform.py at 2f638e0, copied verbatim below with y_rise, y_fall and y_of) with
    the file's constants, bit for bit, at the 146 points the joint fit was made on (and equals the fit's own values
    there) and on a grid covering every branch at n = 1 and 2;
  - L computed stably (ruling 2 of one-loop-drain note 02c), and U15: at n = 2, L = ln(1 + E/V0); at n = 1, L = M/phi_t;
  - the model hook: under the one-loop law _drain_fraction is required and _drain_L optional (P99); AbstractModel's
    _drain_fraction raises, the three entry points refuse a model without it before integrating, and the same model runs
    under the fixed law;
  - the rise where 1 + Q ln L <= 0 (L <= e^{-1/Q_n}): f = 0, at each n, as v1's gap by the user's ruling of 2026-11-05;
  - U14: at n other than 1 and 2 the exponential has no one-loop law.

Pure functions, the model's own classical_turning_point, and the P1 floor harness at M = 1e-10
(test_injection.integrate_floor, about 0.2 s a run, three runs): no Ray, no store, no PRyMordial solve.
"""

import contextlib
import importlib
import io
import json
import os
import unittest
from math import exp, inf, log, log1p, nextafter
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
from CosmologyConcepts.Models.AbstractModel import AbstractModel
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Units import Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")
EM = importlib.import_module("CosmologyConcepts.Models.ExponentialModel")

# ---- the reference, verbatim -----------------------------------------------------------------------------------------
#
# y_rise, y_fall, y_of and fraction of prompts/power-law-wall/source/kex/minform.py at 2f638e0 (07b2's reference
# implementation, §1.8), copied verbatim; the constants are read from minform.json, as minform_run.py `ref` reads them


def y_rise(L, ln_R, q):
    """G (ln R - R0)_+^P (1 + Q ln L)_+."""
    d = ln_R - q["R0"]
    if not d > 0.0:
        return 0.0
    s = 1.0 + q["Q"] * log(L)
    if not s > 0.0:
        return 0.0
    return q["G"] * d ** q["P"] * s


def y_fall(ell, L, q, variant):
    """A/(ell - ell0) ("v-L": A/(ell - ell0 - C ln L)) where the chain exists and the denominator is positive; else
    +inf."""
    if ell is None:
        return inf
    den = ell - q["ell0"]
    if variant == "v-L":
        den = den - q["C"] * log(L)
    if not den > 0.0:
        return inf
    return q["A"] / den


def y_of(ell, L, ln_R, n, params):
    """(y, y_rise, y_fall) under params."""
    variant = params["variant"]
    q = params[f"n{n}"]
    r = y_rise(L, ln_R, q)
    fl = y_fall(ell, L, q, variant)
    m = r if r <= fl else fl
    if variant == "vi" and fl != inf and r > 0.0:
        big = fl if r <= fl else r
        m = m * (1.0 + (m / big) ** q["s"]) ** (-1.0 / q["s"])
    return m, r, fl


def fraction(ell, L, ln_R, n, params):
    """f = 1 - exp(-y) for ln R > 0 under the variant and constants of params (module docstring). ell is the new
    chain's, None where it has no solution."""
    if not ln_R > 0.0:
        raise ValueError(
            "fraction: ln R <= 0 is the classical region (R S(L)), not this law's"
        )
    return 1.0 - exp(-y_of(ell, L, ln_R, n, params)[0])


# ---- end of the verbatim copy ----------------------------------------------------------------------------------------

_MINFORM_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    ".documents",
    "power-law-wall",
    "kex-07b2",
    "minform.json",
)
with open(_MINFORM_JSON) as _f:
    JOINT = json.load(_f)["with_new"]["n12/S3/v-J"]
PARAMS = JOINT["params"]
POINTS = JOINT["points"]

# The rows of prompts/one-loop-drain/source/crossover/out/stage1a_S_of_L.txt (at 1b16c62), columns L and S(L), copied as
# v1's test copied them; S_of is measured_law_map.py's interpolation at 1b16c62, verbatim
_S_ROWS = (
    (0.3, 1.0041e-02),
    (0.5, 9.7145e-03),
    (0.7, 9.3185e-03),
    (1.0, 8.6688e-03),
    (2.0, 6.6167e-03),
    (3.0, 5.0735e-03),
    (4.0, 3.9750e-03),
    (5.0, 3.1852e-03),
    (6.0, 2.6041e-03),
    (7.0, 2.1664e-03),
    (8.0, 1.8292e-03),
    (10.0, 1.3529e-03),
    (13.0, 9.2294e-04),
    (16.0, 6.6916e-04),
    (20.0, 4.6603e-04),
    (30.0, 2.3302e-04),
    (40.0, 1.3936e-04),
    (70.0, 4.9353e-05),
    (90.0, 3.0600e-05),
    (110.0, 2.0810e-05),
    (134.0, 1.4204e-05),
)
_a = np.array(_S_ROWS)
_lL, _lS = np.log(_a[:, 0]), np.log(_a[:, 1])


def S_of(L):
    return float(np.exp(np.interp(log(L), _lL, _lS)))


def reference(n, regime, ell, L, log_R):
    """The production law as the reference gives it: R S(L) at ln R <= 0 or classical (above L = 134 the ruled
    continuation S(134) (134/L)^2, README §8.10 of one-loop-drain), 0 unassessed, else minform.fraction with the joint
    fit's constants (ell None at a gap candidate)."""
    if regime == "unassessed":
        return 0.0
    if regime == "classical" or log_R <= 0.0:
        S = 1.4204e-05 * (134.0 / L) ** 2 if L > 134.0 else S_of(L)
        return exp(log_R) * S
    return fraction(ell if regime == "one_loop" else None, L, log_R, n, PARAMS)


# the reference grid: every branch of every regime at each n
GRID_ELL = (
    1.0e-6,
    0.3,
    1.0,
    nextafter(PARAMS["n1"]["ell0"], 0.0),
    PARAMS["n1"]["ell0"],
    nextafter(PARAMS["n1"]["ell0"], 2.0),
    1.2,
    1.5,
    2.2,
    4.9,
    9.5,
    32.2579,
)
GRID_L = (
    1.0e-6,
    0.1,
    exp(-1.0 / PARAMS["n1"]["Q"]),
    exp(-1.0 / PARAMS["n2"]["Q"]),
    0.4,
    1.0,
    1.378,
    3.556,
    10.0,
    54.52,
    106.0,
    133.9,
    134.0,
    200.0,
    1.0e3,
)
GRID_LOG_R = (
    -16.645,
    -1.0e-12,
    0.0,
    5e-324,
    1.0e-12,
    1.0,
    PARAMS["n2"]["R0"],
    nextafter(PARAMS["n2"]["R0"], 2.0),
    PARAMS["n1"]["R0"],
    nextafter(PARAMS["n1"]["R0"], 2.0),
    2.0,
    5.0,
    13.601,
    40.161,
    138.07,
)
REGIMES_ASSESSED = (SM.REGIME_ONE_LOOP, SM.REGIME_GAP, SM.REGIME_CLASSICAL)


def law(n, regime, ell=None, L=None, log_R=None):
    return EM.one_loop_fraction(n, regime, ell, L, log_R)


class TestTheConstants(unittest.TestCase):
    def test_c_the_constants_are_the_joint_fit(self):
        """P100: 07b2's joint fit (v-J), the fall shared, the rise per n, bit for bit as minform.json holds them."""
        self.assertEqual(PARAMS["variant"], "v-J")
        self.assertEqual(len(POINTS), 146)
        for n in (1, 2):
            q = PARAMS[f"n{n}"]
            with self.subTest(n=n):
                self.assertEqual(EM.DRAIN_EXP_FALL_COEFFICIENT, q["A"])
                self.assertEqual(EM.DRAIN_EXP_FALL_ELL0, q["ell0"])
                self.assertEqual(EM.DRAIN_EXP_RISE_G[n], q["G"])
                self.assertEqual(EM.DRAIN_EXP_RISE_R0[n], q["R0"])
                self.assertEqual(EM.DRAIN_EXP_RISE_P[n], q["P"])
                self.assertEqual(EM.DRAIN_EXP_RISE_Q[n], q["Q"])
        self.assertEqual(tuple(EM.DRAIN_EXP_RISE_G), (1, 2))
        # the README's values (P100), as printed there
        self.assertEqual(EM.DRAIN_EXP_FALL_COEFFICIENT, 2.5143466540073995)
        self.assertEqual(EM.DRAIN_EXP_FALL_ELL0, 1.1859055953178366)
        self.assertEqual(SM.DRAIN_ALPHA_CAP, 0.99)
        self.assertEqual(SM.DRAIN_MODEL_FIXED, "fixed")
        self.assertEqual(SM.DRAIN_MODEL_ONE_LOOP_EXPONENTIAL, "one-loop-exponential-v2")

    def test_c_the_S_table_is_the_files(self):
        """The 21 (L, S) literals equal stage1a_S_of_L.txt's columns L and S(L) (P99: moved, unchanged in value)."""
        self.assertEqual(len(EM.DRAIN_EXP_S_TABLE_L), 21)
        self.assertEqual(len(EM.DRAIN_EXP_S_TABLE_S), 21)
        self.assertEqual(tuple(_a[:, 0]), EM.DRAIN_EXP_S_TABLE_L)
        self.assertEqual(tuple(_a[:, 1]), EM.DRAIN_EXP_S_TABLE_S)

    def test_c_v1_is_gone(self):
        """P99: v1's module-level law and constants left ComputeTargets/ScalarHistory.py, and v2 has no plateau or cap."""
        for name in (
            "drain_fraction",
            "drain_S_of_L",
            "DRAIN_ONE_LOOP_A",
            "DRAIN_ONE_LOOP_B0",
            "DRAIN_ONE_LOOP_C",
            "DRAIN_ELL_PLATEAU",
            "DRAIN_F_CAP",
            "DRAIN_F_PLATEAU",
            "DRAIN_GAP_G",
            "DRAIN_GAP_R0",
            "DRAIN_GAP_P",
            "DRAIN_GAP_Q",
            "DRAIN_S_TABLE_L",
            "DRAIN_S_TABLE_S",
            "DRAIN_MODEL_ONE_LOOP",
            "_GAP_PRESCRIPTION",
            "GAP_PRESCRIPTIONS",
            "DRAIN_C1",
        ):
            with self.subTest(name=name):
                self.assertFalse(hasattr(SM, name))
        for name in ("DRAIN_F_CAP", "DRAIN_F_PLATEAU", "DRAIN_EXP_FALL_A"):
            self.assertFalse(hasattr(EM, name), name)
        self.assertEqual(
            SM.DrainCandidate._fields,
            ("regime", "ell", "alpha_eff", "w_gate", "log_E", "log_H", "L", "log_R"),
        )


class TestTheSTable(unittest.TestCase):
    def test_s_at_every_node(self):
        for L, S in zip(EM.DRAIN_EXP_S_TABLE_L, EM.DRAIN_EXP_S_TABLE_S):
            with self.subTest(L=L):
                # exp(ln S) need not round-trip: ln S (|ln S| <= 11.2) is rounded to within 1.8e-15
                # absolute, which exp carries into S as a relative error; the tolerance is 1e-14 relative
                self.assertAlmostEqual(EM.drain_S_of_L(L) / S, 1.0, delta=1.0e-14)
                self.assertEqual(EM.drain_S_of_L(L), S_of(L))

    def test_s_between_nodes_is_log_linear(self):
        """
        Against an independent evaluation of the log-linear interpolant at three points in every interval; the two
        differ by rounding, a few ulp of ln S (|ln S| <= 11.2), below 1e-14 in S; the tolerance is 1e-13 relative.
        """
        Ls, Ss = EM.DRAIN_EXP_S_TABLE_L, EM.DRAIN_EXP_S_TABLE_S
        for i in range(len(Ls) - 1):
            lo, hi = log(Ls[i]), log(Ls[i + 1])
            slo, shi = log(Ss[i]), log(Ss[i + 1])
            for t in (0.1, 0.5, 0.9):
                L = exp(lo + t * (hi - lo))
                expected = exp(slo + (log(L) - lo) * (shi - slo) / (hi - lo))
                with self.subTest(L=L):
                    self.assertAlmostEqual(
                        EM.drain_S_of_L(L) / expected, 1.0, delta=1.0e-13
                    )
                    self.assertGreater(EM.drain_S_of_L(L), min(Ss[i], Ss[i + 1]))
                    self.assertLess(EM.drain_S_of_L(L), max(Ss[i], Ss[i + 1]))

    def test_s_below_the_table_holds_S_at_0_3(self):
        for L in (0.3, 0.2999, 0.1, 1.0e-3, 1.0e-12):
            with self.subTest(L=L):
                self.assertEqual(EM.drain_S_of_L(L), EM.drain_S_of_L(0.3))
                self.assertEqual(EM.drain_S_of_L(L), S_of(L))

    def test_s_above_the_table_falls_as_L_squared(self):
        """S(134) (134/L)^2 above 134: L^2 S tends to a constant (production_energy_ratio.tex)."""
        for L in (nextafter(134.0, 200.0), 134.1, 200.0, 1.0e3, 1.0e6):
            with self.subTest(L=L):
                self.assertEqual(EM.drain_S_of_L(L), 1.4204e-05 * (134.0 / L) ** 2)
        self.assertAlmostEqual(
            EM.drain_S_of_L(nextafter(134.0, 200.0)) / EM.drain_S_of_L(134.0),
            1.0,
            delta=1.0e-14,
        )
        # where the interpolation alone would hold S(134)
        self.assertEqual(S_of(200.0), S_of(134.0))


class TestTheLaw(unittest.TestCase):
    """Each branch at synthetic inputs, at n = 1 and 2."""

    def q(self, n):
        return PARAMS[f"n{n}"]

    def test_l_the_rise_below_R0_is_zero(self):
        for n in (1, 2):
            R0 = self.q(n)["R0"]
            for log_R in (1.0e-12, 0.5 * R0, R0):
                for regime, ell in (
                    ("gap", None),
                    ("one_loop", 1.5),
                    ("one_loop", 9.5),
                ):
                    with self.subTest(n=n, log_R=log_R, regime=regime, ell=ell):
                        # min(0, y_fall) = 0: f = 0.0 exactly, however small the fall
                        self.assertEqual(law(n, regime, ell, 3.556, log_R), 0.0)
                        self.assertEqual(str(law(n, regime, ell, 3.556, log_R)), "0.0")

    def test_l_ln_R_to_zero_from_above(self):
        """f = 0 for 0 < ln R <= R0_n, where R S(L) at ln R = 0 is up to 1e-2: the step v1's gap had (07b2 §8.5)."""
        for n in (1, 2):
            for log_R in (5e-324, 1.0e-300, 1.0e-12, 1.0e-3):
                self.assertEqual(law(n, "gap", None, 1.0, log_R), 0.0)
            self.assertEqual(law(n, "classical", None, 1.0, 0.0), S_of(1.0))
            self.assertGreater(S_of(1.0), 8.0e-3)

    def test_l_the_rise_where_one_plus_Q_ln_L_is_not_positive(self):
        for n in (1, 2):
            Q = self.q(n)["Q"]
            L0 = exp(-1.0 / Q)
            for L in (0.5 * L0, 0.01, 1.0e-6):
                with self.subTest(n=n, L=L):
                    self.assertLessEqual(1.0 + Q * log(L), 0.0)
                    self.assertEqual(law(n, "gap", None, L, 13.601), 0.0)
                    # at a one-loop candidate the rise is 0, so f = 0 whatever the fall
                    self.assertEqual(law(n, "one_loop", 2.0, L, 13.601), 0.0)

    def test_l_the_rise_active_at_a_one_loop_candidate(self):
        """Near the chain's end (ell just above ell0) the fall is large and the rise is the smaller."""
        for n in (1, 2):
            q = self.q(n)
            L, log_R, ell = 1.378, 5.0, q["ell0"] + 0.05
            r = q["G"] * (log_R - q["R0"]) ** q["P"] * (1.0 + q["Q"] * log(L))
            fl = q["A"] / (ell - q["ell0"])
            self.assertLess(r, fl)
            self.assertEqual(law(n, "one_loop", ell, L, log_R), 1.0 - exp(-r))

    def test_l_the_fall_active(self):
        for n in (1, 2):
            q = self.q(n)
            L, log_R, ell = 106.0, 142.368, 33.46
            r = q["G"] * (log_R - q["R0"]) ** q["P"] * (1.0 + q["Q"] * log(L))
            fl = q["A"] / (ell - q["ell0"])
            self.assertLess(fl, r)
            self.assertEqual(law(n, "one_loop", ell, L, log_R), 1.0 - exp(-fl))

    def test_l_ell_at_or_below_ell0_takes_the_rise(self):
        for n in (1, 2):
            q = self.q(n)
            L, log_R = 3.556, 13.601
            r = q["G"] * (log_R - q["R0"]) ** q["P"] * (1.0 + q["Q"] * log(L))
            for ell in (q["ell0"], nextafter(q["ell0"], 0.0), 0.3, 1.0e-6):
                with self.subTest(n=n, ell=ell):
                    self.assertEqual(law(n, "one_loop", ell, L, log_R), 1.0 - exp(-r))
                    self.assertEqual(
                        law(n, "one_loop", ell, L, log_R), law(n, "gap", None, L, log_R)
                    )

    def test_l_the_gap_takes_the_rise(self):
        for n in (1, 2):
            q = self.q(n)
            for L, log_R in ((3.556, 13.601), (1.0, 2.0), (54.52, 40.161)):
                r = q["G"] * (log_R - q["R0"]) ** q["P"] * (1.0 + q["Q"] * log(L))
                self.assertEqual(law(n, "gap", None, L, log_R), 1.0 - exp(-r))

    def test_l_no_plateau_and_no_cap(self):
        """v1's plateau (0.9 below ell = 1) and cap (0.95) go (U13): f follows the form, up to 1 - exp(-y)."""
        q = self.q(1)
        self.assertNotEqual(law(1, "one_loop", 0.5, 3.556, 13.601), 0.9)
        f = law(1, "one_loop", q["ell0"] + 0.1, 3.556, 40.0)
        self.assertGreater(f, 0.95)

    def test_l_classical(self):
        for n in (1, 2):
            for L, log_R in (
                (54.52, -16.645),
                (0.678, 0.0),
                (6.0, -1.0e-12),
                (200.0, -1.0),
            ):
                with self.subTest(n=n, L=L, log_R=log_R):
                    self.assertEqual(
                        law(n, "classical", None, L, log_R),
                        exp(log_R) * EM.drain_S_of_L(L),
                    )

    def test_l_unassessed_reads_nothing(self):
        for n in (1, 2):
            self.assertEqual(law(n, "unassessed"), 0.0)
            self.assertEqual(str(law(n, "unassessed")), "0.0")

    def test_l_an_unknown_regime_is_refused(self):
        with self.assertRaises(ValueError):
            law(1, "elastic", None, 1.0, 1.0)

    def test_l_alpha_eff_the_cap_and_the_gate(self):
        realized = exponential_realized(1)
        log_E = TestLAndU15.NORMALIZATION + 3.0
        L = realized.drain_L(log_E)
        f = law(1, "one_loop", 9.5, L, 30.0)
        for A, w in ((1.0, 1.0), (0.05, 1.0), (1.0, 0.37), (0.05, 0.0)):
            with self.subTest(A=A, w=w):
                self.assertEqual(
                    SM.drain_alpha_effective(
                        "one_loop", 9.5, 30.0, A, w, model=realized, log_E=log_E
                    ),
                    min(0.99, A * f) * w,
                )
        # A f >= alpha_cap: the cap, then the gate
        for A, w in ((100.0, 1.0), (100.0, 0.5)):
            self.assertEqual(
                SM.drain_alpha_effective(
                    "one_loop", 9.5, 30.0, A, w, model=realized, log_E=log_E
                ),
                0.99 * w,
            )
        # nothing drained below R0 and with w_gate = 0: the caller's alpha_eff = 0 branch
        for args in (("gap", None, 1.0, 1.0, 1.0), ("one_loop", 9.5, 30.0, 1.0, 0.0)):
            with self.subTest(args=args):
                value = SM.drain_alpha_effective(*args, model=realized, log_E=log_E)
                self.assertEqual(value, 0.0)
                self.assertFalse(value > 0.0)
        # the classical regime drains R S(L), however small
        value = SM.drain_alpha_effective(
            "classical", None, -16.645, 0.2, 1.0, model=realized, log_E=log_E
        )
        self.assertEqual(value, min(0.99, 0.2 * (exp(-16.645) * EM.drain_S_of_L(L))))
        self.assertGreater(value, 0.0)
        # unassessed: 0.0, reading neither ln E nor ln R
        value = SM.drain_alpha_effective(
            "unassessed", None, None, 1.0, 1.0, model=realized, log_E=None
        )
        self.assertEqual(str(value), "0.0")
        with self.assertRaises(ValueError):
            SM.drain_alpha_effective(
                "elastic", None, 1.0, 1.0, 1.0, model=realized, log_E=log_E
            )


class TestTheReference(unittest.TestCase):
    """
    one_loop_fraction against the verbatim minform.fraction with minform.json's constants (P100, the gate Consistent),
    bit for bit: at the joint fit's 146 points, where it also equals the fit's own value f_fit, and on GRID_ELL x GRID_L x
    GRID_LOG_R at n = 1 and 2 for every assessed regime. Through the model too: ExponentialModel._drain_fraction at the
    ln E whose L the point gives.
    """

    def test_r_bitwise_at_the_fitted_points(self):
        counts = {"gap": 0, "one_loop": 0}
        for p in POINTS:
            regime = "gap" if p["ell"] is None else "one_loop"
            counts[regime] += 1
            got = law(p["n"], regime, p["ell"], p["L"], p["ln_R"])
            with self.subTest(point=p):
                self.assertEqual(
                    got, fraction(p["ell"], p["L"], p["ln_R"], p["n"], PARAMS)
                )
                self.assertEqual(got, p["f_fit"])
        self.assertEqual(counts, {"gap": 37, "one_loop": 109})
        self.assertEqual({p["n"] for p in POINTS}, {1, 2})

    def test_r_bitwise_on_the_grid(self):
        compared = 0
        for n in (1, 2):
            for L in GRID_L:
                for log_R in GRID_LOG_R:
                    for regime in REGIMES_ASSESSED:
                        ells = GRID_ELL if regime == SM.REGIME_ONE_LOOP else (None,)
                        for ell in ells:
                            f = law(n, regime, ell, L, log_R)
                            expected = reference(n, regime, ell, L, log_R)
                            compared += 1
                            if f != expected:
                                self.fail(
                                    f"f differs at n={n} regime={regime} ell={ell!r} L={L!r} log_R={log_R!r}: "
                                    f"{f!r} != {expected!r}"
                                )
        self.assertEqual(
            compared, 2 * len(GRID_L) * len(GRID_LOG_R) * (len(GRID_ELL) + 2)
        )

    def test_r_through_the_model(self):
        """_drain_fraction at ln E reads L = _drain_L(ln E) and gives one_loop_fraction there."""
        for n in (1, 2):
            realized = exponential_realized(n)
            for x in (0.5, 3.0, 25.0, 106.0):
                log_E = TestLAndU15.NORMALIZATION + x
                L = realized.drain_L(log_E)
                for regime, ell, log_R in (
                    ("one_loop", 9.5, 30.0),
                    ("one_loop", 1.2, 6.0),
                    ("gap", None, 13.601),
                    ("classical", None, -2.0),
                ):
                    with self.subTest(n=n, x=x, regime=regime):
                        self.assertEqual(
                            realized.drain_fraction(regime, ell, log_E, log_R),
                            law(n, regime, ell, L, log_R),
                        )
                self.assertEqual(
                    realized.drain_fraction("unassessed", None, None, None), 0.0
                )


class TestTheRiseBelowLZero(unittest.TestCase):
    """
    The rise's 1 + Q_n ln L <= 0 (L <= e^{-1/Q_n}): f = 0.0 exactly, at each n, as v1's gap had it by the user's ruling
    of 2026-11-05 ("I think probably we set `f = 0` there"); the reference's y_rise is 0 there too.
    """

    def test_g_zero_below_the_boundary_and_positive_above(self):
        for n in (1, 2):
            Q = PARAMS[f"n{n}"]["Q"]
            L0 = exp(-1.0 / Q)
            below = above = L0
            while 1.0 + Q * log(below) > 0.0:
                below = nextafter(below, 0.0)
            while 1.0 + Q * log(above) <= 0.0:
                above = nextafter(above, 1.0)
            for log_R in (2.0, 13.601, 40.0):
                with self.subTest(n=n, log_R=log_R):
                    for L in (below, 0.99 * L0, 0.01, 1.0e-6):
                        self.assertEqual(law(n, "gap", None, L, log_R), 0.0)
                    self.assertGreaterEqual(law(n, "gap", None, above, log_R), 0.0)
                    self.assertGreater(
                        law(n, "gap", None, (1.0 + 1.0e-6) * L0, log_R), 0.0
                    )
                    self.assertGreater(law(n, "gap", None, 1.01 * L0, log_R), 0.0)


def _exponential(n=1, M_Mp=4.0e-31):
    units = Planck_units()
    return (
        ExponentialModel(
            0, M_value(0, M_Mp * units.PlanckMass), n, beta_value(0, 1.185), units
        ),
        units,
    )


def exponential_realized(n=1, M_Mp=4.0e-31):
    model, units = _exponential(n, M_Mp)
    return model.realize(
        TestLAndU15.NORMALIZATION, 8.645101631245453e-16 * units.PlanckMass
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
        """At the four maps' ln(E/V0) (one-loop-drain prompt 02c §2.3), against log1p(exp(x)) to 2 ulp."""
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
        self.assertTrue(realized.drain_fraction_implemented)

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


class TestU14(unittest.TestCase):
    """
    U14, ruled (a) by the user, 2026-11-18: "U14: yes, for now, refuse `n >= 3`." At n other than 1 and 2 the
    exponential's one_loop_drain_model and _drain_fraction raise ValueError naming the class, the hook and U14, so
    check_drain_law_model refuses the model under the one-loop law; the fixed law accepts it at every n.
    """

    def test_u14_the_hooks_raise(self):
        for n in (0, 3, 4):
            model, _ = _exponential(n)
            realized = model.realize(TestLAndU15.NORMALIZATION, 0.0)
            for subject in (model, realized):
                with self.subTest(n=n, subject=type(subject).__name__):
                    with self.assertRaises(ValueError) as ctx:
                        subject.one_loop_drain_model
                    for needle in (
                        "ExponentialModel",
                        f"n={n}",
                        "one_loop_drain_model",
                        "U14",
                    ):
                        self.assertIn(needle, str(ctx.exception))
            with self.assertRaises(ValueError) as ctx:
                realized.drain_fraction(
                    "gap", None, TestLAndU15.NORMALIZATION + 3.0, 10.0
                )
            for needle in ("ExponentialModel", f"n={n}", "_drain_fraction", "U14"):
                self.assertIn(needle, str(ctx.exception))

    def test_u14_refused_under_the_one_loop_law_accepted_under_the_fixed(self):
        for n in (1, 2, 3):
            model, _ = _exponential(n)
            for subject in (model, model.realize(TestLAndU15.NORMALIZATION, 0.0)):
                self.assertIsNone(
                    SM.check_drain_law_model(SM.DRAIN_LAW_FIXED, subject, "ctx")
                )
                if n == 3:
                    with self.assertRaises(ValueError) as ctx:
                        SM.check_drain_law_model(SM.DRAIN_LAW_ONE_LOOP, subject, "ctx")
                    self.assertTrue(str(ctx.exception).startswith("ctx: "))
                    self.assertIn("U14", str(ctx.exception))
                else:
                    self.assertIsNone(
                        SM.check_drain_law_model(SM.DRAIN_LAW_ONE_LOOP, subject, "ctx")
                    )


class _NoDrainFraction(ExponentialModel):
    """ExponentialModel with AbstractModel's _drain_fraction restored: a model with no one-loop fraction of its own."""

    _drain_fraction = AbstractModel._drain_fraction


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
    P99 (power-law-wall prompt 07c): under the one-loop law the model's own fraction, _drain_fraction, is required and
    _drain_L optional. AbstractModel._drain_fraction raises, naming the class (the user's ruling of 2026-11-05: "when a
    new potential is implemented, breakage is loud"); the three entry points refuse a model without it before
    integrating (tune_history before the seed); under the fixed law such a model runs as before. AbstractModel._drain_L
    still raises for a model lacking it.
    """

    def _assert_names_the_hook(self, caught, context):
        message = str(caught.exception)
        self.assertTrue(message.startswith(f"{context}: "), message)
        self.assertIn("_NoDrainFraction", message)
        self.assertIn("_drain_fraction", message)

    def test_h_the_base_hooks_raise_for_a_model_lacking_them(self):
        for model in (_PowerLawWall(1.0e-20), _MinimalModel(0.5)):
            with self.subTest(model=type(model).__name__):
                self.assertFalse(model.drain_L_implemented)
                self.assertFalse(model.realize(0.0, 1.0).drain_L_implemented)
                with self.assertRaises(NotImplementedError) as caught:
                    model._drain_L(0.0, 1.0)
                self.assertIn(type(model).__name__, str(caught.exception))
                self.assertIn("one-loop drain law", str(caught.exception))
                with self.assertRaises(NotImplementedError):
                    model.realize(0.0, 1.0).drain_L(1.0)
        model = _NoDrainFraction(
            0, M_value(0, 1.0e-10), 1, beta_value(0, 2.0), Planck_units()
        )
        self.assertFalse(model.drain_fraction_implemented)
        self.assertTrue(model.drain_L_implemented)
        with self.assertRaises(NotImplementedError) as caught:
            model.realize(0.0, 1.0).drain_fraction("gap", None, 1.0, 10.0)
        self.assertIn("_NoDrainFraction", str(caught.exception))
        self.assertIn("_drain_fraction", str(caught.exception))

    def test_h_integrate_scalar_history_refuses_before_any_step(self):
        with mock.patch.object(SM, "Radau") as radau:
            with self.assertRaises(ValueError) as caught:
                _integrate_floor(_NoDrainFraction, 1.0, SM.DRAIN_LAW_ONE_LOOP)
        radau.assert_not_called()
        self._assert_names_the_hook(caught, "integrate_scalar_history")

    def test_h_integrate_history_refuses_before_any_integration(self):
        cosmology = gth.make_cosmology()
        units = cosmology.units
        model = gth.make_model(units, cls=_NoDrainFraction).realize(0.0, 0.0)
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
        model = gth.make_model(units, cls=_NoDrainFraction)
        with mock.patch.object(_NoDrainFraction, "seed") as seed, mock.patch.object(
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
        ours = _integrate_floor(_NoDrainFraction, 0.1, SM.DRAIN_LAW_FIXED)
        theirs = _integrate_floor(ExponentialModel, 0.1, SM.DRAIN_LAW_FIXED)
        self.assertGreater(len(ours.injections), 0)
        self.assertEqual(ours.reflections, theirs.reflections)
        self.assertEqual(ours.nfev, theirs.nfev)
        self.assertEqual(tuple(ours.final_state), tuple(theirs.final_state))
        self.assertEqual(ours.drain, theirs.drain)
        for context in (
            "integrate_scalar_history",
            "integrate_history",
            "tune_history",
        ):
            SM.check_drain_law_model(
                SM.DRAIN_LAW_FIXED, _NoDrainFraction.__new__(_NoDrainFraction), context
            )


if __name__ == "__main__":
    unittest.main()
