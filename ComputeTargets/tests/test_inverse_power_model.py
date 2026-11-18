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
The inverse-power-law model (power-law-wall prompt 01 §1.3): `InversePowerModel`
(CosmologyConcepts/Models/InversePowerModel.py), V = V_0 [1 + (M/phi)^n], A = exp(beta phi/M_P).

- (a) `TestTheNames`: name, type id, shard key, the floor, reflects_at_origin, n >= 1.
- (b) `TestTheHooks`: each hook against its closed form, and against a centred difference of the hook
  below it, at n = 1 and 2, M = 4e-31 and 1e-3 M_P, and phi from 1e-90 M to 10 M_P (_PHIS); no value
  overflows. The closed forms: ln V against ln V_0 + ln(1 + (M/phi)^n) evaluated in 60-digit decimal
  arithmetic; V - V_0 = V_0 (M/phi)^n; V' = -n V_0 M^n/phi^{n+1}; V'' = n(n + 1) V_0 M^n/phi^{n+2}, the
  last two through RealizedModel's V' = V (ln V)' and V'' = V[(ln V)'' + (ln V)'^2], compared in logs.
- (c) `TestNotPositive`: phi <= 0 (and NaN, inf) raises ComputationFailureError from each hook, naming
  the model and the hook.
- (d) `TestTheWallAgainstClosedForms`: classical_turning_point, minimum_on_trace and
  d_minimum_d_log_trace against their closed forms, and one_loop_turnaround against the chain iterated
  by the test's own loop with this wall's closed forms, at n = 1 and 2. (`test_wall_model.py`'s generic
  tests (3) run on this model too, through its _NO_CLOSED_FORM.)
- (e) `TestTheSeed`: the seed's kind at four points of test_model_interface (g)'s, and its phi_ref and
  normalization against an independent brentq of prompt §1.2's equation.
- (f) `TestRenormalize`: renormalize reaches its target, refuses V_target <= 0; the coordinate
  round-trips exactly.
- (g) `TestTheDrainLaw`: check_drain_law_model refuses the model under the one-loop law at n = 3, naming the
  class, the hook and n, and accepts it under the fixed law (since power-law-wall prompt 03 the one-loop law is the
  model's own at n = 1 and 2; test_power_wall_law tests it).
- (h) `TestShortIntegrations`: one untuned integration under natural mode and one under the fixed law
  (alpha = 0.1), at M = 4e-31 M_P, beta = 2, n = 2 and n = 1, from T* = 2e4 GeV to T_J = 100 MeV, past
  the first bounce.
- (i) `TestPickle`: a realized model survives a pickle round trip.

Every test but (h) takes well under a second; (h) is timed in its docstring. No Ray, no store, no
PRyMordial.
"""

import contextlib
import io
import pickle
import sys
import time
import unittest
from decimal import Decimal, getcontext
from math import exp, expm1, isfinite, log, log1p, pi

from scipy.optimize import brentq

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.ScalarHistory import (
    DRAIN_LAW_FIXED,
    DRAIN_LAW_ONE_LOOP,
    check_drain_law_model,
    integrate_history,
)
from ComputeTargets.tests.test_model_interface import cosmology, rel
from ComputeTargets.tests.test_wall_model import (
    _minimum_residual,
    _root_tolerance,
)
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models import (
    INVERSE_POWER_MODEL,
    InversePowerModel,
    RealizedModel,
    Seed,
)
from CosmologyConcepts.Models.AbstractModel import ONE_LOOP_ELL_RTOL
from Units import Planck_units

_units = Planck_units()
Mp = _units.PlanckMass
EPS = sys.float_info.epsilon
LOG_8_PI_SQUARED = log(8.0 * pi * pi)
LOG_32_PI_SQUARED = log(32.0 * pi * pi)

NS = (1, 2)
MASSES_OVER_MP = (4.0e-31, 1.0e-3)
# a normalization near the tuned ones (ln V_0 ~ -276.6, V_0 in M_P^4)
NORMALIZATION = -276.63


def model(n: int, M_over_Mp: float, beta: float = 2.0) -> InversePowerModel:
    return InversePowerModel(
        0, M_value(0, M_over_Mp * Mp), n, beta_value(0, beta), _units
    )


def _phis(M_over_Mp: float):
    """phi/M_P from 1e-90 M to 10 M_P: 31 points evenly spaced in ln phi, the ends included."""
    lo, hi = log(1.0e-90 * M_over_Mp), log(10.0)
    return [exp(lo + (hi - lo) * k / 30) for k in range(31)]


getcontext().prec = 60


def _decimal_log1p_u(n: int, M: float, phi: float) -> float:
    """ln(1 + (M/phi)^n) in 60-digit decimal arithmetic, from the floats M and phi exactly."""
    u = (Decimal(M) / Decimal(phi)) ** n
    return float((Decimal(1) + u).ln())


class TestTheNames(unittest.TestCase):
    """(a)"""

    def test_a_names_ids_and_the_wall(self):
        m = model(2, 1.0e-3, beta=1.5)
        self.assertEqual(m.name, "InversePowerModel(M=0.001Mp,n=2,beta=1.5)")
        self.assertEqual(m.type_id, INVERSE_POWER_MODEL)
        self.assertEqual(INVERSE_POWER_MODEL, 2)
        self.assertEqual(float(m.shard_key), 1.5)
        self.assertTrue(m.reflects_at_origin)
        self.assertEqual(m._log_V_floor(NORMALIZATION), NORMALIZATION)
        rm = m.realize(NORMALIZATION, 0.0)
        self.assertTrue(rm.name.startswith(m.name), rm.name)
        self.assertEqual(rm.log_V_floor, NORMALIZATION)

    def test_a_n_must_be_a_positive_integer(self):
        for n in (0, -1, 1.0, 2.5):
            with self.subTest(n=n), self.assertRaises(AssertionError):
                model(n, 1.0e-3)


class TestTheHooks(unittest.TestCase):
    """
    (b) The hooks at every (n, M, phi) of _phis. Tolerances, with their reasons:
    - ln V against the decimal ln V_0 + ln(1 + u): 4 eps (|ln V_0| + |ln V|) absolute. ln V is the sum of
      the normalization and ln(1 + u), each correctly rounded or within an ulp or two of it (log1p, exp
      and log), so the sum carries at most about 2 eps of the larger of the two.
    - V - V_0 = V_0 u at normalization 0, where ln V = ln(1 + u) is not swamped by ln V_0 (at
      ln V_0 = -276.6 the sum cannot resolve u below about 6e-14): expm1(ln V)/u - 1 within
      4 eps (|ln V| + 2). expm1 at x has relative condition |x|, and ln(M/phi) n is formed with
      about eps |ln u| absolute.
    - ln(-V') = ln V + ln(-(ln V)') against ln V_0 + ln n + n ln M - (n + 1) ln phi, and
      ln V'' = ln V + ln((ln V)'' + (ln V)'^2) against ln V_0 + ln n(n + 1) + n ln M - (n + 2) ln phi:
      8 eps (|ln V| + |ln V_0| + (n + 2)|ln phi| + n |ln M| + 4) absolute (relative in V' and V''):
      each side is a sum of logarithms of that size, and (ln V)'' + (ln V)'^2 = n(n + 1) s/phi^2 cancels
      n^2 s^2, which costs at most a few eps n^2 s^2/(n(n + 1) s) <= 2 eps relative.
    - (ln V)' phi against the centred difference in x = ln phi of ln V at normalization 0, and
      (ln V)'' phi against that of (ln V)', with h = 1e-5: 1e-7 relative. The truncation is h^2/6 of the
      third derivative over the first, at most about (n + 1)^2 h^2 ~ 1e-9; the rounding is eps |f|/h
      over |f'|, at most about 2e-16 x 210/(1e-5 x 1) ~ 4e-9 for ln V at the deepest phi (|f| ~ 210 n,
      |f'| ~ n), and eps/h ~ 2e-11 for (ln V)'. The normalization is an additive constant that no
      derivative reads; at 0 it adds no rounding to the difference.
    - every value is finite.
    """

    def cases(self):
        for n in NS:
            for M_over_Mp in MASSES_OVER_MP:
                m = model(n, M_over_Mp)
                for x in _phis(M_over_Mp):
                    yield (n, M_over_Mp, x), n, M_over_Mp * Mp, m, x * Mp

    def test_b_ln_V_against_the_closed_form(self):
        worst = 0.0
        for where, n, M, m, phi in self.cases():
            log_V = m._log_V(NORMALIZATION, phi)
            self.assertTrue(isfinite(log_V), where)
            expected = NORMALIZATION + _decimal_log1p_u(n, M, phi)
            d = abs(log_V - expected)
            worst = max(worst, d / (EPS * (abs(NORMALIZATION) + abs(log_V))))
            self.assertLessEqual(
                d, 4.0 * EPS * (abs(NORMALIZATION) + abs(log_V)), where
            )
        print(
            f"\n[test_inverse_power_model (b)] ln V: worst {worst:.3g} eps (|ln V_0| + |ln V|)"
        )

    def test_b_V_minus_V0_is_V0_u(self):
        for where, n, M, m, phi in self.cases():
            log_V = m._log_V(0.0, phi)
            log_u = n * (log(M) - log(phi))
            # V/V_0 - 1 = expm1(ln V - ln V_0) at ln V_0 = 0, against u, compared as a ratio in logs
            ratio = log(expm1(log_V)) - log_u
            self.assertLessEqual(abs(ratio), 4.0 * EPS * (abs(log_V) + 2.0), where)

    def test_b_V_prime_and_V_double_prime_against_the_closed_forms(self):
        worst = [0.0, 0.0]
        for where, n, M, m, phi in self.cases():
            rm = m.realize(NORMALIZATION, 0.0)
            log_V = rm.log_V(phi)
            d1 = rm.d_logV_dphi(phi)
            d2 = rm.d2_logV_dphi2(phi)
            self.assertTrue(isfinite(d1) and isfinite(d2), where)
            self.assertLess(d1, 0.0, where)
            scale = (
                8.0
                * EPS
                * (
                    abs(log_V)
                    + abs(NORMALIZATION)
                    + (n + 2) * abs(log(phi))
                    + n * abs(log(M))
                    + 4.0
                )
            )

            log_minus_V1 = log_V + log(-d1)
            expected = NORMALIZATION + log(n) + n * log(M) - (n + 1) * log(phi)
            worst[0] = max(worst[0], abs(log_minus_V1 - expected) / scale)
            self.assertLessEqual(abs(log_minus_V1 - expected), scale, where)

            log_V2 = log_V + log(d2 + d1 * d1)
            expected = (
                NORMALIZATION + log(n * (n + 1)) + n * log(M) - (n + 2) * log(phi)
            )
            worst[1] = max(worst[1], abs(log_V2 - expected) / scale)
            self.assertLessEqual(abs(log_V2 - expected), scale, where)
        print(
            f"\n[test_inverse_power_model (b)] ln(-V'): worst {worst[0]:.3g} of its bound; "
            f"ln V'': worst {worst[1]:.3g} of its bound"
        )

    def test_b_each_derivative_is_the_difference_of_the_hook_below(self):
        h = 1.0e-5
        worst = [0.0, 0.0]
        for where, n, M, m, phi in self.cases():
            x = log(phi)
            up, down = exp(x + h), exp(x - h)

            diff = (m._log_V(0.0, up) - m._log_V(0.0, down)) / (2.0 * h)
            d1 = m._d_logV_dphi(0.0, phi) * phi
            worst[0] = max(worst[0], rel(d1, diff))
            self.assertLessEqual(rel(d1, diff), 1.0e-7, where)

            diff = (m._d_logV_dphi(0.0, up) - m._d_logV_dphi(0.0, down)) / (2.0 * h)
            d2 = m._d2_logV_dphi2(0.0, phi) * phi
            worst[1] = max(worst[1], rel(d2, diff))
            self.assertLessEqual(rel(d2, diff), 1.0e-7, where)

            # (ln V)''' (power-law-wall prompt 07c), where it is within float range: ~ (n + 2)! s/phi^3 overflows
            # below phi ~ 1e-103 M_P, where the hook raises (test_k_ex (a) tests both)
            if phi > 1.0e-100 * Mp:
                diff = (m._d2_logV_dphi2(0.0, up) - m._d2_logV_dphi2(0.0, down)) / (
                    2.0 * h
                )
                d3 = m._d3_logV_dphi3(0.0, phi) * phi
                worst.append(rel(d3, diff))
                self.assertLessEqual(rel(d3, diff), 1.0e-7, where)
        print(
            f"\n[test_inverse_power_model (b)] centred differences: (ln V)' worst {worst[0]:.3g}, "
            f"(ln V)'' worst {worst[1]:.3g}, (ln V)''' worst {max(worst[2:]):.3g} relative"
        )


class TestNotPositive(unittest.TestCase):
    """(c) phi <= 0, NaN or inf: a ComputationFailureError naming the model and the hook."""

    HOOKS = ("_log_V", "_d_logV_dphi", "_d2_logV_dphi2", "_d3_logV_dphi3")

    def test_c_each_hook_raises(self):
        for n in NS:
            m = model(n, 1.0e-3)
            for phi in (
                0.0,
                -0.0,
                -1.0e-30 * Mp,
                -0.1 * Mp,
                float("nan"),
                float("inf"),
            ):
                for hook in self.HOOKS:
                    with self.subTest(n=n, phi=phi, hook=hook):
                        with contextlib.redirect_stdout(io.StringIO()):
                            with self.assertRaises(ComputationFailureError) as ctx:
                                getattr(m, hook)(NORMALIZATION, phi)
                        message = str(ctx.exception)
                        self.assertIn("InversePowerModel", message)
                        self.assertIn(hook.lstrip("_"), message)

    def test_c_through_the_realized_model(self):
        rm = model(2, 4.0e-31).realize(NORMALIZATION, 0.0)
        for method in ("log_V", "d_logV_dphi", "d2_logV_dphi2", "d3_logV_dphi3"):
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(ComputationFailureError, msg=method):
                    getattr(rm, method)(-1.0e-40 * Mp)


# the wall's points: the field at 0.1 M_P, the normalization of test_wall_model, ln E as there
FIELD = 0.1 * Mp
WALL_NORMALIZATION = -279.87
WALL_MASSES_OVER_MP = (4.0e-31, 1.0e-16, 1.0e-3)
LOG_ENERGIES = (-180.0, -120.0, -60.0)


def _third_in_range(n: int, phi: float) -> bool:
    """True iff the third derivative of ln V at phi, about n (n + 1)(n + 2)/phi^3 near the wall, is within float range (in logs)."""
    return log(n * (n + 1) * (n + 2)) - 3.0 * log(phi) < log(sys.float_info.max) - 1.0


def _wall_points():
    for n in NS:
        for M_over_Mp in WALL_MASSES_OVER_MP:
            rm = model(n, M_over_Mp).realize(WALL_NORMALIZATION, 0.0)
            for log_E in LOG_ENERGIES:
                yield (n, M_over_Mp, log_E), n, M_over_Mp * Mp, rm, log_E


def _the_chain(n: int, M: float, log_V0: float, log_E: float):
    """
    The one-loop chain on this wall, iterated by the test's own loop with the wall's closed forms, never
    by the methods: m_ta^4 = 32 pi^2 E/ell; V''(phi_ta) = n(n + 1) V_0 M^n/phi_ta^{n+2} = m_ta^2 gives
    ln phi_ta; the pulse rate |V'''/V''| = (n + 2)/phi_ta exactly on this wall (V'' ~ phi^-(n+2)); k_ex =
    (n + 2) E^{1/2}/(2 phi_ta) (power-law-wall prompt 07c, R13-R15; until then lambda_ta = n s/phi_ta, s = u/(1 + u));
    ell = ln(k_ex/m_ta). From ell = 32 until ell changes by at most 1e-14 relative. Returns
    (phi_ta, ln m_ta, |V'''/V''|, ln k_ex, ell).
    """
    ell = 32.0
    for _ in range(200):
        log_m = 0.25 * (LOG_32_PI_SQUARED + log_E - log(ell))
        log_phi = (log(n * (n + 1)) + log_V0 + n * log(M) - 2.0 * log_m) / (n + 2)
        lam = (n + 2) / exp(log_phi)
        log_k = log(lam) + 0.5 * log_E - log(2.0)
        ell_next = log_k - log_m
        if abs(ell_next - ell) <= 1.0e-14 * ell_next:
            return exp(log_phi), log_m, lam, log_k, ell_next
        ell = ell_next
    raise AssertionError("the chain did not converge")


class TestTheWallAgainstClosedForms(unittest.TestCase):
    """
    (d) The generic wall searches of AbstractModel on this model, at n = 1 and 2, M = 4e-31, 1e-16 and
    1e-3 M_P and ln E = -180, -120, -60 (E/V_0 = e^100 to e^220), the field at 0.1 M_P.

    Classical: V(phi_t) - V_0 = V_0 u_t = E, so phi_t = M (V_0/E)^{1/n}; V'' = n(n + 1) E/phi_t^2;
    the pulse rate |V'''/V''| = (n + 2)/phi_t exactly (power-law-wall prompt 07c; before it lambda_t = n s_t/phi_t,
    s_t = (E/V_0)/(1 + E/V_0)); ln R, ln k_ex and ell_t from the docstring's
    definitions. The root in x = ln phi lies within brentq's tolerance (_root_tolerance) plus the
    residual's rounding over its slope, and the slope d ln(V - V_0)/dx is exactly -n here: the bound dx is
    _root_tolerance(x) + 6 eps (|ln V| + |ln V_0| + |ln E| + 1)/n (test_wall_model (3)'s rounding term).
    phi_t then agrees to dx relative (plus 2 eps), the pulse rate (n + 2)/phi_t to dx (the bound kept at (n + 1) dx, as
    for lambda_t before), ln omega_t^2 = ln V'' to (n + 2) dx, since the method evaluates V'' at its
    root, and ln R to 2 (n + 2) dx, each plus 8 eps of the logarithms that enter.

    The minimum: V' = -n V_0 M^n phi^{-n-1} exactly and (ln A)' = beta/M_P, so phi_min =
    (n V_0 M^n M_P/(beta T))^{1/(n+1)}, at ln T = ln rho_m0 and -200. Its root lies within
    _root_tolerance + R/S of the true one (test_wall_model's _minimum_residual, S = n + 1 here).
    d phi_min/d ln T = -phi_min/(n + 1) at the true root; at the method's root the formula gives
    -phi e^{-g}/(n + 1), g the residual, so the two agree to |g| <= S _root_tolerance + R, plus 8 eps.

    One-loop: the method's phi_ta, ln m_ta, pulse rate, ln k_ex and ell against _the_chain, to 1e-8
    relative (test_wall_model (2)'s tolerance; the iteration stops at 1e-12 in ell, and phi_ta moves by
    1/(4(n + 2)) of the relative change in ell).

    Since power-law-wall prompt 07c both read the third hook for k_ex. (ln V)''' ~ n(n + 1)(n + 2) s/phi^3 leaves
    float range below phi ~ 1e-103 M_P: at E/V_0 = e^220 with n = 1, phi_t is 1.3e-126 M_P (M = 4e-31 M_P) and
    1.3e-112 M_P (M = 1e-16 M_P), where the methods raise ComputationFailureError naming the hook
    (_third_in_range; issue [07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]). Those points assert the
    raise; every other point is compared as before.
    """

    def test_d_the_classical_turning_point(self):
        worst = 0.0
        for where, n, M, rm, log_E in _wall_points():
            log_phi_t = log(M) - (log_E - WALL_NORMALIZATION) / n
            if not _third_in_range(n, exp(log_phi_t)):
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(ComputationFailureError, msg=where) as ctx:
                        rm.classical_turning_point(FIELD, log_E)
                self.assertIn("d3_logV_dphi3", str(ctx.exception), where)
                continue
            c = rm.classical_turning_point(FIELD, log_E)
            self.assertIsNotNone(c, where)
            log_V_t = rm.log_V(c.phi_t)
            dx = (
                _root_tolerance(log_phi_t)
                + 6.0
                * EPS
                * (abs(log_V_t) + abs(WALL_NORMALIZATION) + abs(log_E) + 1.0)
                / n
            )
            worst = max(worst, abs(log(c.phi_t) - log_phi_t) / dx)
            self.assertLessEqual(rel(c.phi_t, exp(log_phi_t)), dx + 2.0 * EPS, where)

            log_omega2 = log(n * (n + 1)) + log_E - 2.0 * log_phi_t
            lam = (n + 2) / exp(log_phi_t)
            log_R = 2.0 * log_omega2 - LOG_8_PI_SQUARED - log_E
            log_k = log(lam) + 0.5 * log_E - log(2.0)
            logs = abs(log_omega2) + abs(log_E) + abs(log_phi_t) + 4.0
            self.assertLessEqual(
                abs(c.log_omega_t_squared - log_omega2),
                (n + 2) * dx + 8.0 * EPS * logs,
                where,
            )
            self.assertLessEqual(
                abs(c.log_R - log_R),
                2.0 * (n + 2) * dx + 8.0 * EPS * (2.0 * logs + abs(log_R)),
                where,
            )
            self.assertLessEqual(
                rel(c.pulse_rate_t, lam), (n + 1) * dx + 8.0 * EPS, where
            )
            self.assertLessEqual(
                abs(c.log_k_ex - log_k),
                (n + 1) * dx + 8.0 * EPS * (abs(log_k) + logs),
                where,
            )
            self.assertEqual(c.ell_t, c.log_k_ex - 0.5 * c.log_omega_t_squared, where)
        print(
            f"\n[test_inverse_power_model (d)] classical: worst |ln phi_t error| {worst:.3g} of dx"
        )

    def test_d_the_one_loop_turnaround_is_the_chain(self):
        worst = 0.0
        for where, n, M, rm, log_E in _wall_points():
            phi, log_m, lam, log_k, ell = _the_chain(n, M, WALL_NORMALIZATION, log_E)
            if not _third_in_range(n, phi):
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(ComputationFailureError, msg=where) as ctx:
                        rm.one_loop_turnaround(FIELD, log_E)
                self.assertIn("d3_logV_dphi3", str(ctx.exception), where)
                continue
            o = rm.one_loop_turnaround(FIELD, log_E)
            self.assertIsNotNone(o, where)
            for got, expected in (
                (o.phi_ta, phi),
                (o.log_m_ta, log_m),
                (o.pulse_rate_ta, lam),
                (o.log_k_ex, log_k),
                (o.ell, ell),
            ):
                worst = max(worst, rel(got, expected))
                self.assertLessEqual(rel(got, expected), 1.0e-8, where)
            self.assertTrue(o.outside_classical, where)
        print(
            f"\n[test_inverse_power_model (d)] one-loop against the chain: worst {worst:.3g}"
        )

    def test_d_the_minimum_and_its_derivative(self):
        log_T0 = log(cosmology().rho_m0)
        beta = 2.0
        for n in NS:
            for M_over_Mp in MASSES_OVER_MP:
                M = M_over_Mp * Mp
                rm = model(n, M_over_Mp, beta).realize(WALL_NORMALIZATION, 0.0)
                for log_T in (log_T0, -200.0):
                    where = (n, M_over_Mp, log_T)
                    phi = rm.minimum_on_trace(FIELD, log_T)
                    log_phi_min = (
                        log(n)
                        + WALL_NORMALIZATION
                        + n * log(M)
                        + log(Mp)
                        - log(beta)
                        - log_T
                    ) / (n + 1)
                    g, S, R = _minimum_residual(rm, phi, log_T)
                    self.assertLessEqual(
                        abs(log(phi) - log_phi_min),
                        _root_tolerance(log(phi))
                        + R / S
                        + 4.0 * EPS * abs(log_phi_min),
                        where,
                    )
                    derivative = rm.d_minimum_d_log_trace(phi, log_T)
                    self.assertLessEqual(
                        rel(derivative, -phi / (n + 1)),
                        S * _root_tolerance(log(phi)) + R + 8.0 * EPS,
                        where,
                    )


def _friedmann(phi: float, kind: str, beta: float):
    """(V, G, 1 + beta phi'/M_P) at phi for the seed's kind, from Eq. 7 today as prompt §1.2 has it."""
    c = cosmology()
    pi_ = (1.5 * phi) if kind == "tracking" else 0.0
    bp = 1.0 + beta * pi_ / Mp
    G = 1.0 - pi_ * pi_ / (6.0 * Mp * Mp)
    V = 3.0 * Mp**2 * c.H0**2 * G / bp**2 - c.rho_m0 - c.rho_r0
    return V, G, bp


def _independent_seed(n: int, M: float, beta: float, kind: str):
    """
    prompt §1.2's equation n V u/((1 + u) phi) = beta rho_m0/M_P, by brentq in ln phi to xtol 1e-15 and
    SciPy's least rtol, with V from _friedmann; the normalization ln V - ln(1 + u). The test's own
    expressions, not the method's. The bracket is [1e-60 M_P, 2 M_P] for the static kind and
    [1e-60 M_P, 0.25 M_P/beta] for the tracking kind, where 1 + beta phi'/M_P = 1.375 keeps V > 0.
    """
    c = cosmology()

    def F(x):
        phi = exp(x)
        V, _, _ = _friedmann(phi, kind, beta)
        log_u = n * (log(M) - x)
        log_s = -log1p(exp(-log_u)) if log_u > 0.0 else log_u - log1p(exp(log_u))
        return log(n) + log(V) + log_s - x - log(beta * c.rho_m0 / Mp)

    high = 2.0 * Mp if kind == "static" else 0.25 * Mp / beta
    x = brentq(F, log(1.0e-60 * Mp), log(high), xtol=1.0e-15, rtol=4.0 * EPS)
    phi = exp(x)
    V, _, _ = _friedmann(phi, kind, beta)
    log_u = n * (log(M) - x)
    log1p_u = log_u + log1p(exp(-log_u)) if log_u > 0.0 else log1p(exp(log_u))
    return log(V) - log1p_u, phi


class TestTheSeed(unittest.TestCase):
    """
    (e) The seed at n = 1 and 2, at test_model_interface (g)'s points: beta = 1.5 at M = 1e-3 M_P
    (tracking), beta = 1.5 at M = 0.5 M_P (static: the tracking kind has no solution there, as on the
    exponential), and beta = -1.5 and 0 (plain); and at laboratory M, 4e-31 M_P with beta = 2.
    phi_ref against the independent root to 1e-12 relative and the normalization to 1e-12 absolute
    (|normalization| ~ 277: about 16 eps). The fixed point stops at |phi_new/phi - 1| < 1e-14, and the
    two roots carry the rounding of ln V, which is formed from V by the same Friedmann expression.
    """

    POINTS = (
        (1.5, 1.0e-3, "tracking"),
        (1.5, 0.5, "static"),
        (2.0, 4.0e-31, "tracking"),
    )

    def test_e_analytic_kinds_against_an_independent_root(self):
        for n in NS:
            for beta, M_over_Mp, kind in self.POINTS:
                with self.subTest(n=n, beta=beta, M=M_over_Mp):
                    seed = model(n, M_over_Mp, beta).seed(cosmology())
                    self.assertIsInstance(seed, Seed)
                    self.assertEqual(seed.kind, kind)
                    normalization, phi = _independent_seed(
                        n, M_over_Mp * Mp, beta, kind
                    )
                    self.assertLessEqual(rel(seed.phi_ref, phi), 1.0e-12)
                    self.assertLessEqual(
                        abs(seed.normalization - normalization), 1.0e-12
                    )
                    # V(phi_ref) at the seed's normalization is the Friedmann V of its kind
                    V, G, bp = _friedmann(seed.phi_ref, kind, beta)
                    self.assertTrue(V > 0.0 and G > 0.0 and bp > 0.0)
                    log_V = model(n, M_over_Mp, beta)._log_V(
                        seed.normalization, seed.phi_ref
                    )
                    self.assertLessEqual(abs(log_V - log(V)), 1.0e-12)

    def test_e_plain_where_neither_kind_has_a_solution(self):
        plain = float.fromhex("-0x1.14a16ebc655f3p+8")  # test_model_interface (g)'s
        for n in NS:
            for beta in (-1.5, 0.0):
                with self.subTest(n=n, beta=beta):
                    seed = model(n, 1.0e-3, beta).seed(cosmology())
                    self.assertEqual(
                        seed, Seed(kind="plain", normalization=plain, phi_ref=0.0)
                    )


class TestRenormalize(unittest.TestCase):
    """(f) As test_model_interface (e), on this model."""

    def test_f_renormalize_reaches_the_target(self):
        for n in NS:
            m = model(n, 1.0e-3, 1.5)
            for x in (1.0e-40, 1.0e-4, 0.0349624, 2.0):
                phi = x * Mp
                for V_target in (1.0e-121, 3.0e-120):
                    new = m.renormalize(NORMALIZATION, phi, V_target)
                    V = exp(m.realize(new, 0.0).log_V(phi))
                    self.assertLessEqual(rel(V, V_target), 1e-12, (n, x, V_target))

    def test_f_a_target_that_is_not_positive_is_refused(self):
        m = model(2, 1.0e-3, 1.5)
        for V_target in (0.0, -1.0e-121):
            with self.assertRaises(ValueError):
                m.renormalize(NORMALIZATION, 0.03 * Mp, V_target)

    def test_f_the_coordinate_round_trips_exactly(self):
        m = model(1, 1.0e-3, 1.5)
        for normalization in (-279.87, NORMALIZATION, 0.0, 12.5):
            x = m.normalization_coordinate(normalization)
            self.assertEqual(x, normalization)
            self.assertEqual(m.normalization_from_coordinate(x), normalization)


class TestTheDrainLaw(unittest.TestCase):
    """(g) No _drain_L (README §6.6 P16, replacing §6.3 P4): the model's own one-loop law is defined at n = 1 and 2
    only (power-law-wall prompt 03), so the one-loop law refuses it at n = 3; the fixed law accepts it.
    """

    def test_g_the_one_loop_law_refuses_it(self):
        m = model(3, 4.0e-31)
        self.assertFalse(m.drain_L_implemented)
        for subject in (m, m.realize(NORMALIZATION, 0.0)):
            with self.subTest(subject=type(subject).__name__):
                with self.assertRaises(ValueError) as ctx:
                    check_drain_law_model(DRAIN_LAW_ONE_LOOP, subject, "test")
                self.assertIn("InversePowerModel", str(ctx.exception))
                self.assertIn("one_loop_drain_model", str(ctx.exception))
                self.assertIn("n=3", str(ctx.exception))
        with self.assertRaises(NotImplementedError) as ctx:
            m.realize(NORMALIZATION, 0.0).drain_L(-120.0)
        self.assertIn("InversePowerModel", str(ctx.exception))

    def test_g_the_fixed_law_accepts_it(self):
        m = model(1, 4.0e-31)
        self.assertIsNone(check_drain_law_model(DRAIN_LAW_FIXED, m, "test"))
        self.assertIsNone(
            check_drain_law_model(
                DRAIN_LAW_FIXED, m.realize(NORMALIZATION, 0.0), "test"
            )
        )


class TestShortIntegrations(unittest.TestCase):
    """
    (h) One untuned integrate_history under natural mode (n = 2) and one under the fixed law at
    alpha = 0.1 (n = 1), at M = 4e-31 M_P, beta = 2, realized at the seed's normalization and phi_ref,
    from phi* = 5 M_P, pi* = 0, T* = 2e4 GeV (history_and_bbn's initial data) to T_J = 100 MeV, past the
    first bounce near 750 MeV, on a z grid of 20 samples per decade to 300 decades. Each succeeds, with
    reflections and a first bounce. Measured 2026-11-14 at a load of about 10: natural n = 2, 4
    reflections, 1212 accepted steps, 198 samples, first bounce N = 20.352098 at 746.7 MeV, 0.5 s; fixed
    alpha = 0.1 n = 1, 5 reflections, 1407 steps, 199 samples, the same first bounce, 0.9 s.
    """

    T_STOP_GEV = 0.1

    def run_one(self, n: int, alpha: float):
        from CosmologyConcepts import redshift, redshift_array
        from CosmologyConcepts.redshift import log_one_plus_z_grid

        m = model(n, 4.0e-31, 2.0)
        seed = m.seed(cosmology())
        xs = log_one_plus_z_grid(20, 300.0)
        z = redshift_array(z_array=[redshift(i, x) for i, x in enumerate(xs)])
        start = time.perf_counter()
        with contextlib.redirect_stdout(io.StringIO()):
            data = integrate_history(
                cosmology(),
                temperature(0, 2.0e4 * _units.GeV),
                temperature(1, self.T_STOP_GEV * _units.GeV),
                phi_value(0, 5.0 * Mp),
                pi_value(0, 0.0),
                z,
                m.realize(seed.normalization, seed.phi_ref),
                task_label=f"test_inverse_power_model-n{n}-alpha{alpha:g}",
                alpha=alpha,
            )
        wall = time.perf_counter() - start
        return data, wall

    def check(self, data, wall, label):
        self.assertFalse(data.get("failure", False), data.get("failure_reason"))
        self.assertGreater(data["reflections"], 0)
        self.assertIsNotNone(data["first_bounce"])
        self.assertGreater(len(data["sample"]), 0)
        print(
            f"\n[test_inverse_power_model (h)] {label}: reflections={data['reflections']} "
            f"samples={len(data['sample'])} accepted_steps={data['accepted_steps']} "
            f"first bounce N={data['first_bounce'].N:.6f} "
            f"T_J={exp(data['first_bounce'].log_T_Jordan) / _units.MeV:.3f} MeV wall={wall:.1f} s"
        )

    def test_h_natural_mode(self):
        data, wall = self.run_one(2, 0.0)
        self.check(data, wall, "natural, n = 2")

    def test_h_the_fixed_law(self):
        data, wall = self.run_one(1, 0.1)
        self.check(data, wall, "fixed alpha = 0.1, n = 1")
        self.assertGreater(len(data["injections"]), 0)


class TestPickle(unittest.TestCase):
    """(i) As test_model_interface (h)."""

    def test_i_round_trip(self):
        for n in NS:
            plain = model(n, 1.0e-3, 5.5).realize(NORMALIZATION, 0.0)
            for rm in (plain, plain.model.realize(NORMALIZATION, 0.03 * Mp)):
                loaded = pickle.loads(pickle.dumps(rm))
                self.assertIsInstance(loaded, RealizedModel)
                self.assertIsInstance(loaded.model, InversePowerModel)
                self.assertEqual(loaded.name, rm.name)
                self.assertEqual(loaded.normalization, rm.normalization)
                self.assertEqual(loaded.phi_ref, rm.phi_ref)
                self.assertEqual(loaded.log_V_floor, rm.log_V_floor)
                self.assertEqual(loaded.reflects_at_origin, rm.reflects_at_origin)
                for phi in (1.0e-40 * Mp, 1.0e-4 * Mp, 0.03 * Mp, 2.0 * Mp):
                    for method in (
                        "log_V",
                        "d_logV_dphi",
                        "d2_logV_dphi2",
                        "log_Omega",
                        "Omega",
                        "d_logOmega_dphi",
                        "d2_logOmega_dphi2",
                    ):
                        self.assertEqual(
                            getattr(loaded, method)(phi), getattr(rm, method)(phi)
                        )


if __name__ == "__main__":
    unittest.main()
