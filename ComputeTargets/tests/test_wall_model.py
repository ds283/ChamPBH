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
The wall and the minimum through the model (inelastic-reflection prompt 04 §1.6): the generic
`classical_turning_point`, `one_loop_turnaround`, `minimum_on_trace` and `d_minimum_d_log_trace` of
`AbstractModel` (CosmologyConcepts/Models/AbstractModel.py), which use the hooks alone (README §8.1
R9). `ExponentialModel` overrides none of them, so every test here runs the generic path.

- (1) `TestClassicalAgainstTheClosedForm`: phi_t, ln R and k_ex against the exponential n = 1 closed
  form, at four M and three E each (orchestrator note 04, ruling 2's points).
- (2) `TestTheWorkedExample`: wall_backreaction.tex §"The logarithm in terms of the parameters of the
  capture" (sec:params), M = 1 meV, V_0 = 1e-48 GeV^4, E = (c_beta T_s)^4 with c_beta = 1.18 and
  T_s = 0.23 GeV. The test evaluates the notes' chain by its own loop (ruling 10) and compares the
  method with it, and both with the notes' printed y = 39.1, Lambda = 53.8, k_ex = 5.6e13 GeV.
- (3) `TestTheDefiningEquations`: on the exponential with n = 2 and on a test-only power-law wall
  V = V_0 (1 + (M/phi)^2), which no n = 1 closed form describes, every defining equation holds at the
  returned values to a rounding bound derived from the operations (ruling 8; see `_root_tolerance`
  and each test).
- (4) `TestTheMinimum`: its residual, its agreement with `effective_minimum` at T = rho_m,E today with
  Sigma = 0, and d phi_min/d ln T against a centred finite difference, at M = 1e-30 and 0.5 M_P, on
  the exponential and on the power-law wall with (ln A)'' != 0 (ruling 6).
- (5) `TestTheCases`: a model that does not reflect, no log_V_floor, E <= 0, bad fields, a field
  inside its turning points (None), a one-loop call with no solution, a search that leaves float
  range, and a wall with a bump that the monotonicity check refuses (ruling 3).
- (6) `TestRealizedModelDelegates`: each RealizedModel method calls the model's with the
  normalization and returns its result unchanged (ruling 5).
- (7) `TestTheImport`: `import CosmologyConcepts.Models` alone in a fresh interpreter, and after
  `import ComputeTargets` (ruling 1), in subprocesses from the repository root (about 3 s each).

No integration, no store, no Ray, and no file outside the tree is read: the notes' numbers are
written here with their source. The whole module takes about 2 s plus the subprocesses.
"""

import os
import subprocess
import sys
import unittest
from math import exp, expm1, isfinite, log, log1p, pi, sqrt
from unittest import mock

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.test_model_interface import cosmology, exponential_model, rel
from CosmologyConcepts.Models import AbstractModel, ExponentialModel
from CosmologyConcepts.Models.AbstractModel import (
    ClassicalTurningPoint,
    OneLoopTurnaround,
    ONE_LOOP_ELL_RTOL,
    WALL_ROOT_RTOL,
    WALL_ROOT_XTOL,
)
from Units import Planck_units

_units = Planck_units()
Mp = _units.PlanckMass
GeV = _units.GeV

EPS = sys.float_info.epsilon
LOG_8_PI_SQUARED = log(8.0 * pi * pi)
LOG_32_PI_SQUARED = log(32.0 * pi * pi)

# orchestrator note 04, ruling 2: the planner's probe points (field at 0.1 M_P, normalization
# -279.87, ln E in {-180, -120, -60} at each of four masses)
NORMALIZATION = -279.87
FIELD = 0.1 * Mp
MASSES_OVER_MP = (1.0e-30, 1.0e-16, 1.0e-10, 1.0e-7)
LOG_ENERGIES = (-180.0, -120.0, -60.0)

REPOSITORY_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def _root_tolerance(x: float) -> float:
    """
    How far brentq's root may lie from the true one in x = ln phi: SciPy stops when the sign change
    is bracketed within xtol + rtol |x| (ruling 2).
    """
    return WALL_ROOT_XTOL + WALL_ROOT_RTOL * abs(x)


def _log_W(log_V: float, log_V_floor: float) -> float:
    """ln(V - V_floor), formed as the test's own expression."""
    return log_V + log(-expm1(log_V_floor - log_V))


def _log_V2(rm, phi: float) -> float:
    """ln V'' = ln V + ln((ln V)'' + (ln V)'^2), from the hooks."""
    d1 = rm.d_logV_dphi(phi)
    return rm.log_V(phi) + log(rm.d2_logV_dphi2(phi) + d1 * d1)


class _PowerLawWall(AbstractModel):
    """
    A test-only reflecting wall, V = V_0 (1 + (M/phi)^p) with V_0 = e^normalization, and a coupling
    ln A = beta phi/M_P + gamma phi^2/M_P^2, so that (ln A)'' = 2 gamma/M_P^2 != 0 (ruling 6). Its
    hooks are written here, with nothing from ExponentialModel. With s = u/(1 + u), u = (M/phi)^p:
    (ln V)' = -p s/phi and (ln V)'' = [p (p + 1) s - p^2 s^2]/phi^2.
    """

    def __init__(
        self,
        M: float,
        p: float = 2.0,
        beta: float = 2.0,
        gamma: float = 0.0,
        reflects: bool = True,
        floor: bool = True,
    ):
        super().__init__(0, _units)
        self._M, self._p, self._beta, self._gamma = M, p, beta, gamma
        self._reflects, self._floor = reflects, floor

    @property
    def name(self) -> str:
        return (
            f"PowerLawWall(M={self._M:.6g},p={self._p:g},beta={self._beta:g},"
            f"gamma={self._gamma:.6g})"
        )

    @property
    def type_id(self) -> int:
        return -2

    @property
    def shard_key(self):
        return None

    @property
    def reflects_at_origin(self) -> bool:
        return self._reflects

    def _log_V_floor(self, normalization):
        return normalization if self._floor else None

    def _s(self, phi: float) -> float:
        log_u = self._p * log(self._M / phi)
        return 1.0 / (1.0 + exp(-log_u)) if log_u > -700.0 else exp(log_u)

    def _log_V(self, normalization, phi) -> float:
        log_u = self._p * log(self._M / phi)
        if log_u > 0.0:
            return normalization + log_u + log1p(exp(-log_u))
        return normalization + log1p(exp(log_u))

    def _d_logV_dphi(self, normalization, phi) -> float:
        return -self._p * self._s(phi) / phi

    def _d2_logV_dphi2(self, normalization, phi) -> float:
        s = self._s(phi)
        p = self._p
        return (p * (p + 1.0) * s - p * p * s * s) / (phi * phi)

    def _log_Omega(self, phi) -> float:
        return self._beta * phi / Mp + self._gamma * phi * phi / (Mp * Mp)

    def _d_logOmega_dphi(self, phi) -> float:
        return self._beta / Mp + 2.0 * self._gamma * phi / (Mp * Mp)

    def _d2_logOmega_dphi2(self, phi) -> float:
        return 2.0 * self._gamma / (Mp * Mp)


class _BumpWall(_PowerLawWall):
    """
    A test-only wall that claims to reflect, V = V_0 (1 + (M/phi)^2 + b), with a bump
    b = B exp(-((ln phi - c)/w)^2) between the field and its turning point (ruling 3). With the field at
    1 M_P, M = 1e-10 M_P, E = e^50 V_0, B = e^40, c = -7, w = 0.5, the doubling probes sit at
    ln phi = -1, -3, -7, -15, ...: the probe at -7 is on the bump, at ln(V - V_floor) = ln V_0 + 40, and
    the probe at -15 is past it, at about ln V_0 - 16, so the searched quantity moves away from its
    target. The turning point is at ln phi ~ -48.
    """

    B, C, W = exp(40.0), -7.0, 0.5

    def _b(self, phi: float):
        g = (log(phi) - self.C) / self.W
        return self.B * exp(-g * g), -2.0 * g / self.W

    def _log_V(self, normalization, phi) -> float:
        b, _ = self._b(phi)
        return normalization + log1p((self._M / phi) ** 2 + b)

    def _d_logV_dphi(self, normalization, phi) -> float:
        u = (self._M / phi) ** 2
        b, g = self._b(phi)
        return (-2.0 * u + b * g) / (phi * (1.0 + u + b))

    def _d2_logV_dphi2(self, normalization, phi) -> float:
        u = (self._M / phi) ** 2
        b, g = self._b(phi)
        d1 = (-2.0 * u + b * g) / (phi * (1.0 + u + b))
        second = (6.0 * u + b * (g * g - g - 2.0 / (self.W * self.W))) / (phi * phi)
        return second / (1.0 + u + b) - d1 * d1


def _exponential(M_over_Mp: float, n: int = 1, beta: float = 2.0) -> ExponentialModel:
    return exponential_model(beta, M_over_Mp, n)


class TestClassicalAgainstTheClosedForm(unittest.TestCase):
    """
    (1) The classical turning point of ExponentialModel, n = 1, against its closed form, which this
    test computes: V_0 (e^{M/phi_t} - 1) = E gives y_t = M/phi_t = ln(1 + E/V_0); V'' = V (y^4 + 2y^3)/M^2
    with V = E + V_0 at phi_t, so omega_t^2 = (E + V_0)(y^4 + 2 y^3)/M^2; lambda_t = y^2/M and
    k_ex = (y^2/M) E^{1/2}/2.

    The tolerances are the prompt's: 1e-12 relative in phi_t and k_ex, 1e-10 absolute in ln R. The hooks
    round ln V = normalization + M/phi at eps |normalization| ~ 6e-14, so y_t carries that absolute
    error and phi_t a relative error of about 6e-14/ln(1 + E/V_0) (ruling 2): 6e-16 to 1e-15 here,
    where E/V_0 = e^100 to e^220, which the test asserts is above 1. ln R inherits brentq's tolerance
    in ln phi, at most 1e-14 + 4 eps |ln phi_t| = 7.6e-14, times 2(y + 4) <= 450: 3.4e-11.
    """

    def test_1_phi_t_ln_R_and_k_ex_equal_the_closed_form(self):
        for M_over_Mp in MASSES_OVER_MP:
            M = M_over_Mp * Mp
            rm = _exponential(M_over_Mp).realize(NORMALIZATION, 0.0)
            for log_E in LOG_ENERGIES:
                where = (M_over_Mp, log_E)
                self.assertGreater(log_E - NORMALIZATION, 0.0, where)  # E/V_0 > 1

                c = rm.classical_turning_point(FIELD, log_E)
                self.assertIsInstance(c, ClassicalTurningPoint)

                y = log1p(exp(log_E - NORMALIZATION))
                log_E_plus_V0 = log_E + log1p(exp(NORMALIZATION - log_E))
                log_omega2 = log_E_plus_V0 + log(y**4 + 2.0 * y**3) - 2.0 * log(M)
                log_R = 2.0 * log_omega2 - LOG_8_PI_SQUARED - log_E
                log_k_ex = log(y * y / M) + 0.5 * log_E - log(2.0)

                self.assertLessEqual(rel(c.phi_t, M / y), 1.0e-12, where)
                self.assertLessEqual(abs(c.log_R - log_R), 1.0e-10, where)
                self.assertLessEqual(
                    rel(exp(c.log_k_ex), exp(log_k_ex)), 1.0e-12, where
                )
                self.assertEqual(c.ell_t, c.log_k_ex - 0.5 * c.log_omega_t_squared)


def _worked_example():
    """
    The notes' worked example (wall_backreaction.tex sec:params), in Planck units through units.GeV:
    M = 1 meV, V_0 = 1e-48 GeV^4, E = (c_beta T_s)^4 with c_beta = 1.18, T_s = 0.23 GeV (beta = 1.2).
    """
    M = 1.0e-12 * GeV
    V_0 = 1.0e-48 * GeV**4
    log_E = 4.0 * log(1.18 * 0.23 * GeV)
    return M, V_0, log_E


def _the_notes_chain(M: float, V_0: float, log_E: float):
    """
    sec:params' equations, iterated to convergence by the test's own loop, never by the methods
    (ruling 10): m_ta^4 = 32 pi^2 E/ell (eq:mta-Ts); y = Lambda - 4 ln y - ln(1 + 2/y) with
    Lambda = ln(m_ta^2 M^2/V_0) (eq:y-Lambda); k_ex = y^2 E^{1/2}/2M (eq:kex-Ts); ell = ln(k_ex/m_ta)
    (eq:ell-exact). From ell = 32, until ell changes by at most 1e-14 relative. Returns
    (y, Lambda, log k_ex, log m_ta, ell, the number of outer iterations).
    """
    ell = 32.0
    for outer in range(1, 101):
        log_m = 0.25 * (LOG_32_PI_SQUARED + log_E - log(ell))
        Lam = 2.0 * log_m + 2.0 * log(M) - log(V_0)
        y = Lam
        for _ in range(200):
            y_next = Lam - 4.0 * log(y) - log1p(2.0 / y)
            converged = abs(y_next - y) <= 1.0e-15 * y_next
            y = y_next
            if converged:
                break
        log_k = 2.0 * log(y) + 0.5 * log_E - log(2.0 * M)
        ell_next = log_k - log_m
        if abs(ell_next - ell) <= 1.0e-14 * ell_next:
            return y, Lam, log_k, log_m, ell_next, outer
        ell = ell_next
    raise AssertionError("the notes' chain did not converge")


class TestTheWorkedExample(unittest.TestCase):
    """
    (2) The notes' worked example. The field is at 1e-3 M_P, far outside the wall (M = 4.1e-31 M_P).
    - Classical: y_t = M/phi_t equals ln(1 + E/V_0) to 1e-12 relative (= 105.3; the notes print 105).
    - One-loop: the method's y = M/phi_ta, ell, ln k_ex and ln m_ta agree with the test's own chain to
      1e-8 relative (the prompt's tolerance; measured about 4e-15).
    - The notes' printed numbers, from the test's chain: y = 39.1 +- 0.05, Lambda = 53.8 +- 0.05,
      k_ex = 5.6e13 GeV +- 3% (prompt §1.6 item 2). The notes' m_ta^2 ~ 0.25 GeV^2 at ell = 32 is not
      asserted; log 04 reports it beside the chain's own value.
    """

    def setUp(self):
        self.M, self.V_0, self.log_E = _worked_example()
        model = exponential_model(1.2, self.M / Mp, 1)
        self.rm = model.realize(log(self.V_0), 0.0)
        self.field = 1.0e-3 * Mp

    def test_2_the_classical_wall(self):
        c = self.rm.classical_turning_point(self.field, self.log_E)
        y_t = self.M / c.phi_t
        self.assertLessEqual(rel(y_t, log1p(exp(self.log_E) / self.V_0)), 1.0e-12)
        self.assertEqual(round(y_t), 105)

    def test_2_the_one_loop_turnaround_is_the_notes_chain(self):
        y, Lam, log_k, log_m, ell, _ = _the_notes_chain(self.M, self.V_0, self.log_E)
        o = self.rm.one_loop_turnaround(self.field, self.log_E)
        self.assertIsInstance(o, OneLoopTurnaround)
        self.assertLessEqual(rel(self.M / o.phi_ta, y), 1.0e-8)
        self.assertLessEqual(rel(o.ell, ell), 1.0e-8)
        self.assertLessEqual(rel(o.log_k_ex, log_k), 1.0e-8)
        self.assertLessEqual(rel(o.log_m_ta, log_m), 1.0e-8)
        self.assertTrue(o.outside_classical)
        self.assertGreater(o.iterations, 1)

    def test_2_the_notes_printed_values(self):
        y, Lam, log_k, _, _, _ = _the_notes_chain(self.M, self.V_0, self.log_E)
        self.assertLessEqual(abs(y - 39.1), 0.05)
        self.assertLessEqual(abs(Lam - 53.8), 0.05)
        self.assertLessEqual(abs(exp(log_k) / GeV / 5.6e13 - 1.0), 0.03)


# (3) the models on which no n = 1 closed form holds, and their points: the exponential with n = 2,
# and the power-law wall with p = 2, each at two M and two E (field at 0.1 M_P, normalization -279.87).
# Every point has a one-loop turnaround: on n = 2 at ln E = -180 the chain has none at M = 1e-16 M_P,
# where R = 10^-2.9, so the second mass is 1e-20 M_P (R = 10^13)
_NO_CLOSED_FORM = (
    ("exponential n = 2", lambda M: _exponential(M, n=2), (1.0e-30, 1.0e-20)),
    ("power-law p = 2", lambda M: _PowerLawWall(M * Mp, p=2.0), (1.0e-30, 1.0e-10)),
)
_NO_CLOSED_FORM_LOG_E = (-180.0, -120.0)


class TestTheDefiningEquations(unittest.TestCase):
    """
    (3) No closed form is used: on the exponential with n = 2 and the power-law wall, every defining
    equation holds at the returned values to rounding (ruling 8). Each bound is the residual's
    sensitivity to x = ln phi times the root's tolerance (_root_tolerance), plus the rounding of the
    residual's own evaluation, plus, for m_ta^4 ell = 32 pi^2 E, the iteration's stopping tolerance.
    The rounding terms count, for each logarithm the residual adds, eps/2 of its size per operation:
    - ln V from the hooks: at most 2.5 eps (|ln V| + |normalization|) on both models (the power or the
      log1p, the division and the sum); ln(V - V_floor) adds the expm1 and log, whose condition in
      ln V - ln V_floor is V_floor/W < 1 since E > V_0, and eps |ln W|/2; the subtraction of ln E adds
      eps |ln E|/2. 6 eps (|ln V| + |normalization| + |ln E| + 1) covers it with room.
    - ln V'' adds ln((ln V)'' + (ln V)'^2), a few eps absolute, and its own size: 6 eps (... + |ln q| + 4).
    - Recomputed identities (k_ex, ell, ln R) differ only by the rounding of their operations:
      4 eps times the sum of the sizes of the logs that enter.
    """

    def points(self):
        for label, make, masses in _NO_CLOSED_FORM:
            for M_over_Mp in masses:
                rm = make(M_over_Mp).realize(NORMALIZATION, 0.0)
                for log_E in _NO_CLOSED_FORM_LOG_E:
                    yield (label, M_over_Mp, log_E), rm, log_E

    def test_3_the_classical_turning_point(self):
        for where, rm, log_E in self.points():
            c = rm.classical_turning_point(FIELD, log_E)
            phi = c.phi_t
            x = log(phi)
            log_V = rm.log_V(phi)
            log_W = _log_W(log_V, rm.log_V_floor)

            # V(phi_t) - V_floor = E: d ln W/dx = phi (ln V)' V/W
            S = abs(phi * rm.d_logV_dphi(phi)) * exp(log_V - log_W)
            bound = S * _root_tolerance(x) + 6.0 * EPS * (
                abs(log_V) + abs(NORMALIZATION) + abs(log_E) + 1.0
            )
            self.assertLessEqual(abs(log_W - log_E), bound, where)

            # ln omega_t^2 = ln V'', ln R = 2 ln omega_t^2 - ln 8 pi^2 - ln E, lambda_t, k_ex
            log_V2 = _log_V2(rm, phi)
            self.assertLessEqual(
                abs(c.log_omega_t_squared - log_V2), 4.0 * EPS * abs(log_V2), where
            )
            log_R = 2.0 * log_V2 - LOG_8_PI_SQUARED - log_E
            self.assertLessEqual(
                abs(c.log_R - log_R),
                4.0 * EPS * (2.0 * abs(log_V2) + abs(log_E) + abs(log_R) + 3.0),
                where,
            )
            self.assertEqual(c.lambda_t, -rm.d_logV_dphi(phi), where)
            log_k = log(-rm.d_logV_dphi(phi)) + 0.5 * log_E - log(2.0)
            self.assertLessEqual(
                abs(c.log_k_ex - log_k),
                4.0 * EPS * (abs(log_k) + abs(log(c.lambda_t)) + abs(log_E) + 1.0),
                where,
            )

    def test_3_the_one_loop_turnaround(self):
        for where, rm, log_E in self.points():
            o = rm.one_loop_turnaround(FIELD, log_E)
            self.assertIsInstance(o, OneLoopTurnaround, where)
            phi = o.phi_ta
            x = log(phi)

            # V''(phi_ta) = m_ta^2. d ln V''/dx needs (ln V)''', which no hook gives: a centred
            # difference with h = 1e-3 in ln phi, accurate to O(h^2) relative, is enough for a bound
            h = 1.0e-3
            S = abs(_log_V2(rm, exp(x + h)) - _log_V2(rm, exp(x - h))) / (2.0 * h)
            log_V = rm.log_V(phi)
            d1 = rm.d_logV_dphi(phi)
            log_q = log(rm.d2_logV_dphi2(phi) + d1 * d1)
            residual = _log_V2(rm, phi) - 2.0 * o.log_m_ta
            bound = S * _root_tolerance(x) + 6.0 * EPS * (
                abs(log_V) + abs(NORMALIZATION) + abs(log_E) + abs(log_q) + 4.0
            )
            self.assertLessEqual(abs(residual), bound, where)

            # ell = ln(k_ex/m_ta), recomputed from the returned logs (ruling 9)
            ell = o.log_k_ex - o.log_m_ta
            self.assertLessEqual(
                abs(o.ell - ell), EPS * (abs(o.log_k_ex) + abs(o.log_m_ta)), where
            )

            # m_ta^4 ell = 32 pi^2 E, to the iteration's stopping tolerance: the method stops when
            # |ell_new - ell| <= r ell_new and returns ell_new with the m_ta of ell, so
            # |ln(ell_new/ell)| <= -ln(1 - r) <= r (1 + r)
            r = ONE_LOOP_ELL_RTOL
            residual = 4.0 * o.log_m_ta + log(ell) - LOG_32_PI_SQUARED - log_E
            bound = r * (1.0 + r) + 4.0 * EPS * (
                4.0 * abs(o.log_m_ta) + abs(log(ell)) + abs(log_E) + 8.0
            )
            self.assertLessEqual(abs(residual), bound, where)

            # k_ex = lambda_ta E^{1/2}/2, lambda_ta = -d ln V/dphi from the hook
            self.assertEqual(o.lambda_ta, -d1, where)
            log_k = log(-d1) + 0.5 * log_E - log(2.0)
            self.assertLessEqual(
                abs(o.log_k_ex - log_k),
                4.0 * EPS * (abs(log_k) + abs(log(-d1)) + abs(log_E) + 1.0),
                where,
            )

            # outside_classical is phi_ta > phi_t
            c = rm.classical_turning_point(FIELD, log_E)
            self.assertEqual(o.outside_classical, o.phi_ta > c.phi_t, where)

    def test_3_the_minimum(self):
        for label, make, masses in _NO_CLOSED_FORM:
            for M_over_Mp in masses:
                rm = make(M_over_Mp).realize(NORMALIZATION, 0.0)
                for log_T in (log(cosmology().rho_m0), -200.0):
                    where = (label, M_over_Mp, log_T)
                    phi = rm.minimum_on_trace(FIELD, log_T)
                    self._check_minimum_residual(rm, phi, log_T, where)

    def _check_minimum_residual(self, rm, phi, log_T, where):
        _check_minimum_residual(self, rm, phi, log_T, where)


def _minimum_residual(rm, phi, log_T):
    """
    The minimum's residual g = ln(-V') - ln((ln A)' T) at phi, its sensitivity S = |dg/dx| and its
    rounding R. dg/dx = phi [V''/V' - (ln A)''/(ln A)'] = phi [q/(ln V)' - (ln A)''/(ln A)'],
    q = (ln V)'' + (ln V)'^2. R: ln V as in (3), ln(-(ln V)'), ln (ln A)' and ln T each eps/2 of their
    size per operation, 6 eps of the sum with room.
    """
    d1_V = rm.d_logV_dphi(phi)
    q = rm.d2_logV_dphi2(phi) + d1_V * d1_V
    a1, a2 = rm.d_logOmega_dphi(phi), rm.d2_logOmega_dphi2(phi)
    log_V = rm.log_V(phi)
    g = log_V + log(-d1_V) - log(a1) - log_T
    S = abs(phi * (q / d1_V - a2 / a1))
    R = (
        6.0
        * EPS
        * (
            abs(log_V)
            + abs(rm.normalization)
            + abs(log(-d1_V))
            + abs(log(a1))
            + abs(log_T)
            + 2.0
        )
    )
    return g, S, R


def _check_minimum_residual(case, rm, phi, log_T, where):
    """V'(phi_min) + (ln A)'(phi_min) T = 0, to S times the root's tolerance plus R."""
    g, S, R = _minimum_residual(rm, phi, log_T)
    case.assertLessEqual(abs(g), S * _root_tolerance(log(phi)) + R, where)


def _gamma_for(M: float, log_T: float, beta: float = 2.0) -> float:
    """
    A gamma that makes (ln A)'' T comparable with V'' at the power-law wall's minimum (ruling 6): the
    root with gamma = 0 is near phi_c = (2 V_0 M^2/(beta T))^{1/3} (phi << M), and gamma = beta/phi_c
    makes 2 gamma phi ~ 2 beta there. A test choice only; the method does not see it.
    """
    phi_c = exp((log(2.0) + NORMALIZATION + 2.0 * log(M) - log(beta) - log_T) / 3.0)
    return beta * Mp * Mp / phi_c


class TestTheMinimum(unittest.TestCase):
    """
    (4) The minimum on a given trace, at M = 1e-30 and 0.5 M_P, on the exponential (n = 1,
    (ln A)'' = 0) and on the power-law wall with (ln A)'' != 0 (ruling 6), at T = rho_m,E today
    (Planck2018's rho_m0) and at ln T = -200 (a radiation-era trace); the field starts at 0.1 M_P.
    - The residual, to the bound of _check_minimum_residual.
    - At T = rho_m,E today with Sigma = 0 it is effective_minimum(normalization, phi_ref, phi_min, T)'s
      root: there A(x)/A(phi_0) = 1 at x = phi_0 = phi_min, so the two equations agree. To
      effective_minimum's tolerance, xtol 1e-14 phi_0 + rtol 1e-13, plus this method's,
      1e-14 + 4 eps |ln phi_min| in ln phi.
    - d phi_min/d ln T against a centred difference in ln T with h = 1e-3, to 1e-6 relative. The
      truncation is h^2/6 of phi''' /phi', about 2e-8 for phi_min ~ T^(-1/3) or T^(-1/2), and the
      roots' rounding about 1e-14/2h = 5e-12.
    Also on effective_minimum's three states of test_model_interface (f) (M = 1e-3 M_P).
    """

    def models(self):
        log_T0 = log(cosmology().rho_m0)
        for M_over_Mp in (1.0e-30, 0.5):
            yield ("exponential n = 1", M_over_Mp), _exponential(M_over_Mp)
            yield ("power-law p = 2", M_over_Mp), _PowerLawWall(
                M_over_Mp * Mp, gamma=_gamma_for(M_over_Mp * Mp, log_T0)
            )

    def log_traces(self):
        return (log(cosmology().rho_m0), -200.0)

    def test_4_the_residual(self):
        for where, model in self.models():
            rm = model.realize(NORMALIZATION, 0.0)
            for log_T in self.log_traces():
                phi = rm.minimum_on_trace(FIELD, log_T)
                _check_minimum_residual(self, rm, phi, log_T, where + (log_T,))

    def test_4_the_root_does_not_depend_on_the_start(self):
        """
        Started inside the root and outside it, the search finds the same root: each lies within the
        root's tolerance of the true root plus R/S, where the residual's rounding R hides its sign
        (_minimum_residual), so the two differ by at most twice that in ln phi.
        """
        for where, model in self.models():
            rm = model.realize(NORMALIZATION, 0.0)
            log_T = self.log_traces()[0]
            phi = rm.minimum_on_trace(FIELD, log_T)
            _, S, R = _minimum_residual(rm, phi, log_T)
            for start in (phi * 1.0e-6, phi * 0.5, phi * 3.0, phi * 1.0e4):
                other = rm.minimum_on_trace(start, log_T)
                self.assertLessEqual(
                    abs(log(other) - log(phi)),
                    2.0 * (_root_tolerance(log(phi)) + R / S),
                    where + (start,),
                )

    def test_4_effective_minimum_agrees_today(self):
        rho_m0 = cosmology().rho_m0
        cases = [
            (model, NORMALIZATION, 0.0, log(rho_m0), where)
            for where, model in self.models()
        ]
        # the three states of test_model_interface (f): beta, Lambda in meV, phi_ref/M_P, phi_min/phi_0
        for beta, Lambda_meV, phi_ref_over_Mp, ratio in (
            (1.5, 2.125913, 0.0349624, 1.046),
            (5.5, 2.021177, 0.0167195, 1.006),
            (25.0, 1.843465, 0.0068090, 1.000),
        ):
            model = exponential_model(beta)
            normalization = 4.0 * log(Lambda_meV * 1.0e-3 * _units.eV)
            phi_ref = phi_ref_over_Mp * Mp
            rm = model.realize(normalization, phi_ref)
            log_T = 4.0 * rm.log_Omega(phi_ref / ratio) + log(rho_m0)
            cases.append((model, normalization, phi_ref, log_T, ("state", beta)))

        for model, normalization, phi_ref, log_T, where in cases:
            rm = model.realize(normalization, phi_ref)
            phi_min = rm.minimum_on_trace(FIELD, log_T)
            other = model.effective_minimum(normalization, phi_ref, phi_min, exp(log_T))
            tolerance = 1.0e-13 + 1.0e-14 + _root_tolerance(log(phi_min))
            self.assertLessEqual(rel(other, phi_min), tolerance, where)

    def test_4_the_derivative_against_a_finite_difference(self):
        h = 1.0e-3
        for where, model in self.models():
            rm = model.realize(NORMALIZATION, 0.0)
            for log_T in self.log_traces():
                phi = rm.minimum_on_trace(FIELD, log_T)
                plus = rm.minimum_on_trace(phi, log_T + h)
                minus = rm.minimum_on_trace(phi, log_T - h)
                difference = (plus - minus) / (2.0 * h)
                derivative = rm.d_minimum_d_log_trace(phi, log_T)
                self.assertLess(derivative, 0.0, where)
                self.assertLessEqual(
                    rel(derivative, difference), 1.0e-6, where + (log_T,)
                )


class TestTheCases(unittest.TestCase):
    """(5) The cases of prompt §1.1 and orchestrator note 04, rulings 3, 4 and 7."""

    METHODS = (
        "classical_turning_point",
        "one_loop_turnaround",
        "minimum_on_trace",
        "d_minimum_d_log_trace",
    )

    def test_5_a_model_that_does_not_reflect_raises(self):
        rm = _PowerLawWall(1.0e-10 * Mp, reflects=False).realize(NORMALIZATION, 0.0)
        for method in self.METHODS:
            with self.assertRaises(ComputationFailureError, msg=method) as ctx:
                getattr(rm, method)(FIELD, -120.0)
            self.assertIn("does not reflect", str(ctx.exception))
            self.assertIn("PowerLawWall", str(ctx.exception))

    def test_5_a_model_without_a_floor_raises_at_the_turning_points(self):
        rm = _PowerLawWall(1.0e-10 * Mp, floor=False).realize(NORMALIZATION, 0.0)
        for method in self.METHODS[:2]:
            with self.assertRaises(ComputationFailureError, msg=method) as ctx:
                getattr(rm, method)(FIELD, -120.0)
            self.assertIn("log_V_floor", str(ctx.exception))
        # the minimum does not read the floor (ruling 7)
        self.assertGreater(rm.minimum_on_trace(FIELD, -200.0), 0.0)

    def test_5_E_not_positive_raises(self):
        rm = _exponential(1.0e-10).realize(NORMALIZATION, 0.0)
        for method in self.METHODS:
            for bad in (float("-inf"), float("nan"), float("inf")):
                with self.assertRaises(ComputationFailureError, msg=(method, bad)):
                    getattr(rm, method)(FIELD, bad)

    def test_5_a_field_not_finite_and_positive_raises(self):
        rm = _exponential(1.0e-10).realize(NORMALIZATION, 0.0)
        for method in self.METHODS:
            for bad in (0.0, -0.1, float("nan"), float("inf")):
                with self.assertRaises(
                    ComputationFailureError, msg=(method, bad)
                ) as ctx:
                    getattr(rm, method)(bad, -120.0)
                self.assertIn("finite and > 0", str(ctx.exception))

    def test_5_a_field_inside_its_turning_points_returns_None(self):
        M, V_0, log_E = _worked_example()
        rm = exponential_model(1.2, M / Mp, 1).realize(log(V_0), 0.0)
        c = rm.classical_turning_point(1.0e-3 * Mp, log_E)
        o = rm.one_loop_turnaround(1.0e-3 * Mp, log_E)
        self.assertGreater(o.phi_ta, c.phi_t)

        # between the walls: inside the one-loop turnaround, outside the classical one (ruling 4)
        between = sqrt(c.phi_t * o.phi_ta)
        self.assertIsNone(rm.one_loop_turnaround(between, log_E))
        self.assertLessEqual(
            rel(rm.classical_turning_point(between, log_E).phi_t, c.phi_t), 1.0e-13
        )

        # inside both (prompt §1.1)
        inside = 0.5 * c.phi_t
        self.assertIsNone(rm.classical_turning_point(inside, log_E))
        self.assertIsNone(rm.one_loop_turnaround(inside, log_E))

    def test_5_a_one_loop_call_without_a_solution_raises(self):
        """
        At the worked example's E with M = 1e-16 M_P: R = 10^2.49 > 1, yet the chain has no solution,
        since ell falls to -0.19 at the 4th iterate (log 04 §5.2: the chain stops at M = 10^-16.11 M_P,
        R = 10^2.92, at this E).
        """
        _, V_0, log_E = _worked_example()
        rm = exponential_model(1.2, 1.0e-16, 1).realize(log(V_0), 0.0)
        self.assertGreater(rm.classical_turning_point(FIELD, log_E).log_R, 0.0)
        with self.assertRaises(ComputationFailureError) as ctx:
            rm.one_loop_turnaround(FIELD, log_E)
        self.assertIn("no one-loop turnaround", str(ctx.exception))
        self.assertIn("ell from ell_0", str(ctx.exception))

    def test_5_a_search_that_leaves_float_range_raises(self):
        """
        A power-law wall with p = 0.1: (M/phi_t)^0.1 = E/V_0 = e^160 puts phi_t at e^-1600 M, below the
        smallest float; the search stops at the first probe whose phi is not a normal float.
        """
        rm = _PowerLawWall(1.0e-10 * Mp, p=0.1).realize(NORMALIZATION, 0.0)
        with self.assertRaises(ComputationFailureError) as ctx:
            rm.classical_turning_point(FIELD, -120.0)
        self.assertIn("float range", str(ctx.exception))

    def test_5_a_bump_between_the_field_and_the_wall_is_refused(self):
        """Ruling 3: a probe that sees the searched quantity move away from its target raises."""
        rm = _BumpWall(1.0e-10 * Mp).realize(NORMALIZATION, 0.0)
        with self.assertRaises(ComputationFailureError) as ctx:
            rm.classical_turning_point(1.0 * Mp, NORMALIZATION + 50.0)
        self.assertIn("not monotone", str(ctx.exception))
        # without the bump the same search succeeds
        plain = _PowerLawWall(1.0e-10 * Mp).realize(NORMALIZATION, 0.0)
        self.assertIsNotNone(
            plain.classical_turning_point(1.0 * Mp, NORMALIZATION + 50.0)
        )


class TestRealizedModelDelegates(unittest.TestCase):
    """
    (6) Each RealizedModel method calls the model's with the normalization and the caller's arguments,
    and returns its result unchanged (ruling 5): a unittest.mock spy on the model's class.
    """

    def test_6_each_method_delegates(self):
        model = _exponential(1.0e-10)
        normalization, phi_ref = NORMALIZATION, 0.25 * Mp
        rm = model.realize(normalization, phi_ref)
        for method in TestTheCases.METHODS:
            sentinel = object()
            with mock.patch.object(
                ExponentialModel, method, autospec=True, return_value=sentinel
            ) as spy:
                result = getattr(rm, method)(0.3, -123.0)
            self.assertIs(result, sentinel, method)
            spy.assert_called_once_with(model, normalization, 0.3, -123.0)


class TestTheImport(unittest.TestCase):
    """
    (7) Ruling 1: ComputationFailureError is imported inside the methods, so that
    `import CosmologyConcepts.Models` works in a fresh interpreter on its own, as well as after
    `import ComputeTargets`. Each runs in a subprocess from the repository root (about 3 s).
    """

    def run_python(self, code: str):
        env = dict(os.environ)
        env["PYTHONPATH"] = "."
        return subprocess.run(
            [sys.executable, "-c", code],
            cwd=REPOSITORY_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )

    def test_7_the_models_import_alone(self):
        done = self.run_python(
            "import CosmologyConcepts.Models as m; "
            "from CosmologyConcepts.Models.AbstractModel import ClassicalTurningPoint"
        )
        self.assertEqual(done.returncode, 0, done.stderr)

    def test_7_the_models_import_after_ComputeTargets(self):
        done = self.run_python("import ComputeTargets; import CosmologyConcepts.Models")
        self.assertEqual(done.returncode, 0, done.stderr)


if __name__ == "__main__":
    unittest.main()
