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
One k_ex (power-law-wall prompt 07c §1.2, §1.7; README §6.12 R13-R16, §6.14 P102, P103): k_ex = E^{1/2}|V'''/V''|/2 at
the point the estimate is taken, through the third hook _d3_logV_dphi3.

- (a) `TestTheThirdHook`: ExponentialModel's and InversePowerModel's _d3_logV_dphi3 against their closed forms in
  60-digit decimal arithmetic, and phi (ln V)''' against the centred difference in ln phi of _d2_logV_dphi2, at n = 1
  and 2, M = 4e-31 and 1e-3 M_P, phi from 1e-90 M to 10 M_P (the gate Model's range; test_inverse_power_model's
  _phis). Where the closed form is beyond float range the hook raises ComputationFailureError naming the model and the
  hook. AbstractModel's hook raises NotImplementedError naming the class (no generic default); RealizedModel delegates.
- (b) `TestThePulseRate`: the classical turning point's and the one-loop turnaround's pulse rate |V'''/V''| against the
  closed forms: (n + 2)/phi exactly on the power wall (README §6.12), and [n^2 u^2 + 3n(n + 1)u + (n + 1)(n + 2)] /
  [phi (n u + n + 1)] on the exponential (u = (M/phi)^n), each at the method's own phi; ln k_ex = ln|V'''/V''| +
  ln E/2 - ln 2 recomputed bit for bit; on the power wall k_ex/omega_t = (n + 2)/(2 sqrt(n(n + 1))) at the classical
  turning point; R16: phi_t, ln omega_t^2 and ln R read no k_ex. A pulse rate that is zero or not finite raises
  ComputationFailureError naming the hook; the refusal of a wall that does not repel stays on -d ln V/dphi <= 0.
- (c) `TestTheIterationCap`: ONE_LOOP_MAX_ITERATIONS is 400 (P103), and at 07b's slowest point (the laboratory curve,
  n = 1, L = 1.378072, ln R = 5.003334, where 07b's chain took 195 iterations) production converges in more than 100 and
  at most 400, and with the cap patched to 100 it raises, as 07b's issue measured.

No integration, no store, no Ray. Instant.
"""

import importlib
import sys
import unittest
from decimal import Decimal, getcontext
from math import exp, expm1, isfinite, log, pi, sqrt
from unittest import mock

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.test_inverse_power_model import _phis
from ComputeTargets.tests.test_model_interface import rel
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import (
    AbstractModel,
    ExponentialModel,
    InversePowerModel,
)
from Units import Planck_units

# the module, not the class CosmologyConcepts.Models re-exports under its name
AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

_units = Planck_units()
Mp = _units.PlanckMass
EPS = sys.float_info.epsilon
FLOAT_MAX = Decimal(sys.float_info.max)
LOG_8_PI_SQUARED = log(8.0 * pi * pi)

NS = (1, 2)
MASSES_OVER_MP = (4.0e-31, 1.0e-3)
NORMALIZATION = -276.63

getcontext().prec = 60

MODELS = (("exponential", ExponentialModel), ("inverse-power", InversePowerModel))


def model(cls, n: int, M_over_Mp: float, beta: float = 2.0):
    return cls(0, M_value(0, M_over_Mp * Mp), n, beta_value(0, beta), _units)


def closed_d3(name: str, n: int, M: float, phi: float) -> Decimal:
    """(ln V)''' in 60-digit decimal arithmetic from the floats M and phi exactly."""
    u = (Decimal(M) / Decimal(phi)) ** n
    p3 = Decimal(phi) ** 3
    if name == "exponential":
        # ln V = ln V0 + u, du/dphi = -n u/phi
        return -n * (n + 1) * (n + 2) * u / p3
    # ln V = ln V0 + ln(1 + u), s = u/(1 + u)
    s = u / (1 + u)
    return (
        -s * (n * (n + 1) * (n + 2) - 3 * n * n * (n + 1) * s + 2 * n**3 * s * s) / p3
    )


class TestTheThirdHook(unittest.TestCase):
    """
    (a) Tolerances, with their reasons:
    - against the closed form: 16 eps (n + 2) (|ln M| + |ln phi| + 4) relative. The closed form is exact in M and phi;
      the hook forms u = (M/phi)^n (exponential: the quotient and the power, (n + 1) eps; the power wall: ln u =
      n (ln M - ln phi), absolute error eps n (|ln M| + |ln phi|), which enters s only as (1 - s) times it), then a
      bracket of at most three terms (on the power wall n(n + 1)(n + 2) - 3n^2(n + 1) s + 2n^3 s^2, whose cancellation at
      s = 1 is at most a factor 9, at n = 2) and three divisions by phi: a few tens of eps beside the ln u term.
    - phi (ln V)''' against the centred difference in x = ln phi of (ln V)'', h = 1e-5: 1e-7 relative. The truncation is
      h^2/6 of d^3(ln V)''/dx^3 over d(ln V)''/dx, at most about (n + 4)^2 h^2/6 ~ 6e-10; the rounding is eps/h of
      (ln V)'' over phi (ln V)''' ~ (n + 2) eps/h ~ 1e-10.
    The comparisons run where the closed form (and, for the difference, (ln V)'' at x +- h) is within float range; beyond
    it the hook raises ComputationFailureError (the third derivative is ~ phi^-3 and leaves float range above about
    1e103 M_P^-3, deep in the wall at laboratory M, where no turning point of a history lies).
    """

    def cases(self):
        for name, cls in MODELS:
            for n in NS:
                for M_over_Mp in MASSES_OVER_MP:
                    m = model(cls, n, M_over_Mp)
                    for x in _phis(M_over_Mp):
                        yield (
                            name,
                            n,
                            M_over_Mp,
                            x,
                        ), name, n, M_over_Mp * Mp, m, x * Mp

    def test_a_against_the_closed_form(self):
        worst = 0.0
        compared = raised = 0
        for where, name, n, M, m, phi in self.cases():
            exact = closed_d3(name, n, M, phi)
            if abs(exact) > FLOAT_MAX:
                with self.assertRaises(ComputationFailureError, msg=where) as ctx:
                    with mock.patch("builtins.print"):
                        m._d3_logV_dphi3(NORMALIZATION, phi)
                self.assertIn(type(m).__name__, str(ctx.exception), where)
                self.assertIn("d3_logV_dphi3", str(ctx.exception), where)
                raised += 1
                continue
            got = m._d3_logV_dphi3(NORMALIZATION, phi)
            self.assertTrue(isfinite(got), where)
            self.assertLess(got, 0.0, where)
            tol = 16.0 * EPS * (n + 2) * (abs(log(M)) + abs(log(phi)) + 4.0)
            r = abs(float(Decimal(got) / exact - 1))
            worst = max(worst, r / tol)
            self.assertLessEqual(r, tol, where)
            compared += 1
        self.assertGreater(compared, 150)
        self.assertGreater(raised, 0)
        print(
            f"\n[test_k_ex (a)] (ln V)''' against the closed form: {compared} compared, worst {worst:.3g} of the "
            f"tolerance; {raised} beyond float range, raised"
        )

    def test_a_against_the_centred_difference(self):
        h = 1.0e-5
        worst = 0.0
        compared = 0
        for where, name, n, M, m, phi in self.cases():
            x = log(phi)
            up, down = exp(x + h), exp(x - h)
            if any(
                abs(closed_d3(name, n, M, p)) > FLOAT_MAX / 10 for p in (phi, up, down)
            ):
                continue
            with mock.patch("builtins.print"):
                try:
                    d2_up, d2_down = m._d2_logV_dphi2(0.0, up), m._d2_logV_dphi2(
                        0.0, down
                    )
                except (ComputationFailureError, ZeroDivisionError):
                    continue
            if not (isfinite(d2_up) and isfinite(d2_down)):
                continue
            diff = (d2_up - d2_down) / (2.0 * h)
            d3 = m._d3_logV_dphi3(0.0, phi) * phi
            worst = max(worst, rel(d3, diff))
            self.assertLessEqual(rel(d3, diff), 1.0e-7, where)
            compared += 1
        self.assertGreater(compared, 150)
        print(
            f"\n[test_k_ex (a)] phi (ln V)''' against the centred difference of (ln V)'': {compared} compared, "
            f"worst {worst:.3g} relative"
        )

    def test_a_the_base_has_no_default(self):
        class _NoThird(ExponentialModel):
            _d3_logV_dphi3 = AbstractModel._d3_logV_dphi3

        m = _NoThird(0, M_value(0, 1e-10 * Mp), 1, beta_value(0, 2.0), _units)
        with self.assertRaises(NotImplementedError) as ctx:
            m._d3_logV_dphi3(NORMALIZATION, 1e-12 * Mp)
        self.assertIn("_NoThird", str(ctx.exception))
        self.assertIn("_d3_logV_dphi3", str(ctx.exception))
        # the turning points read it, so a model without it is refused there, loudly
        with self.assertRaises(NotImplementedError):
            m.classical_turning_point(NORMALIZATION, 1.0e-3 * Mp, NORMALIZATION + 30.0)

    def test_a_the_realized_model_delegates(self):
        for name, cls in MODELS:
            m = model(cls, 2, 1.0e-3)
            rm = m.realize(NORMALIZATION, 0.25 * Mp)
            for phi in (1.0e-5 * Mp, 1.0e-3 * Mp, 0.3 * Mp):
                self.assertEqual(
                    rm.d3_logV_dphi3(phi), m._d3_logV_dphi3(NORMALIZATION, phi)
                )

    def test_a_phi_not_positive_raises_on_the_power_wall(self):
        m = model(InversePowerModel, 1, 1.0e-3)
        for phi in (0.0, -1.0e-30 * Mp, float("nan"), float("inf")):
            with mock.patch("builtins.print"):
                with self.assertRaises(ComputationFailureError) as ctx:
                    m._d3_logV_dphi3(NORMALIZATION, phi)
            self.assertIn("d3_logV_dphi3", str(ctx.exception))


# the wall's points, as test_inverse_power_model (d) and test_wall_model (3) take them: the field at 0.1 M_P, the
# normalization -279.87, ln E = -180, -120, -60 (E/V_0 = e^100 to e^220); the masses test_inverse_power_model's on the
# power wall and test_wall_model (3)'s on the exponential, where every point has a one-loop turnaround
FIELD = 0.1 * Mp
WALL_NORMALIZATION = -279.87
WALL_MASSES_OVER_MP = {
    "exponential": (1.0e-30, 1.0e-20),
    "inverse-power": (4.0e-31, 1.0e-16, 1.0e-3),
}
LOG_ENERGIES = (-180.0, -120.0, -60.0)


def third_in_range(n: int, phi: float) -> bool:
    """True iff n (n + 1)(n + 2)/phi^3, the power wall's third derivative of ln V near the wall, is within float range."""
    return log(n * (n + 1) * (n + 2)) - 3.0 * log(phi) < log(sys.float_info.max) - 1.0


def pulse_closed(name: str, n: int, M: float, phi: float) -> float:
    """|V'''/V''| at phi: (n + 2)/phi on the power wall; [n^2 u^2 + 3n(n+1)u + (n+1)(n+2)]/[phi (nu + n + 1)] on the
    exponential, in 60-digit decimals."""
    if name == "inverse-power":
        return float((n + 2) / Decimal(phi))
    u = (Decimal(M) / Decimal(phi)) ** n
    return float(
        (n * n * u * u + 3 * n * (n + 1) * u + (n + 1) * (n + 2))
        / (Decimal(phi) * (n * u + n + 1))
    )


class TestThePulseRate(unittest.TestCase):
    """
    (b) Tolerance on the pulse rate against its closed form at the method's own phi: 32 eps (n + 2) (|ln M| + |ln phi| +
    4) relative, twice the hook's (a): the rate is |(ln V)''' + 3 (ln V)'(ln V)'' + (ln V)'^3| / ((ln V)'' + (ln V)'^2),
    every term of each sum of one sign on a repulsive convex wall, so no cancellation beyond the hooks' own. On the
    power wall at n = 1, ln E = -60 and M = 4e-31 or 1e-16 M_P, phi_t is 1e-126 or 1e-112 M_P, where (ln V)''' is
    beyond float range: the methods raise there, naming the hook (test_inverse_power_model (d) asserts it), and those
    points are left out here.
    """

    def points(self):
        for name, cls in MODELS:
            for n in NS:
                for M_over_Mp in WALL_MASSES_OVER_MP[name]:
                    rm = model(cls, n, M_over_Mp).realize(WALL_NORMALIZATION, 0.0)
                    for log_E in LOG_ENERGIES:
                        phi_t = M_over_Mp * Mp * exp(-(log_E - WALL_NORMALIZATION) / n)
                        if name == "inverse-power" and not third_in_range(n, phi_t):
                            continue
                        yield (
                            name,
                            n,
                            M_over_Mp,
                            log_E,
                        ), name, n, M_over_Mp * Mp, rm, log_E

    def test_b_the_classical_turning_point(self):
        worst = 0.0
        for where, name, n, M, rm, log_E in self.points():
            c = rm.classical_turning_point(FIELD, log_E)
            self.assertEqual(
                c._fields,
                (
                    "phi_t",
                    "log_omega_t_squared",
                    "log_R",
                    "pulse_rate_t",
                    "log_k_ex",
                    "ell_t",
                ),
            )
            tol = 32.0 * EPS * (n + 2) * (abs(log(M)) + abs(log(c.phi_t)) + 4.0)
            r = rel(c.pulse_rate_t, pulse_closed(name, n, M, c.phi_t))
            worst = max(worst, r / tol)
            self.assertLessEqual(r, tol, where)
            self.assertEqual(
                c.log_k_ex, log(c.pulse_rate_t) + 0.5 * log_E - log(2.0), where
            )
            self.assertEqual(c.ell_t, c.log_k_ex - 0.5 * c.log_omega_t_squared, where)
            if name == "inverse-power":
                # k_ex/omega_t = (n + 2)/(2 sqrt(n(n + 1))) at every E/V_0 (README §6.12), to the root's and the
                # hooks' rounding (1e-12)
                self.assertLessEqual(
                    abs(exp(c.ell_t) / ((n + 2) / (2.0 * sqrt(n * (n + 1)))) - 1.0),
                    1.0e-12,
                    where,
                )
        print(
            f"\n[test_k_ex (b)] classical pulse rate: worst {worst:.3g} of the tolerance"
        )

    def test_b_the_one_loop_turnaround(self):
        worst = 0.0
        for where, name, n, M, rm, log_E in self.points():
            o = rm.one_loop_turnaround(FIELD, log_E)
            self.assertIsNotNone(o, where)
            self.assertEqual(
                o._fields,
                (
                    "phi_ta",
                    "log_m_ta",
                    "pulse_rate_ta",
                    "log_k_ex",
                    "ell",
                    "iterations",
                    "outside_classical",
                ),
            )
            tol = 32.0 * EPS * (n + 2) * (abs(log(M)) + abs(log(o.phi_ta)) + 4.0)
            r = rel(o.pulse_rate_ta, pulse_closed(name, n, M, o.phi_ta))
            worst = max(worst, r / tol)
            self.assertLessEqual(r, tol, where)
            self.assertEqual(
                o.log_k_ex, log(o.pulse_rate_ta) + 0.5 * log_E - log(2.0), where
            )
            self.assertEqual(o.ell, o.log_k_ex - o.log_m_ta, where)
        print(
            f"\n[test_k_ex (b)] one-loop pulse rate: worst {worst:.3g} of the tolerance"
        )

    def test_b_R16_phi_t_omega_t_and_R_read_no_k_ex(self):
        """With the third hook patched to any finite negative value, phi_t, ln omega_t^2 and ln R do not move."""
        for name, cls in MODELS:
            m = model(cls, 1, 1.0e-16)
            c = m.classical_turning_point(WALL_NORMALIZATION, FIELD, -120.0)
            with mock.patch.object(cls, "_d3_logV_dphi3", return_value=-1.0e60):
                d = m.classical_turning_point(WALL_NORMALIZATION, FIELD, -120.0)
            self.assertEqual(
                (c.phi_t, c.log_omega_t_squared, c.log_R),
                (d.phi_t, d.log_omega_t_squared, d.log_R),
            )
            self.assertNotEqual(c.pulse_rate_t, d.pulse_rate_t)

    def test_b_a_pulse_rate_zero_or_not_finite_raises(self):
        m = model(ExponentialModel, 1, 1.0e-16)

        def q3_zero(self, normalization, phi):
            # V'''/V = 0 exactly, in the order AbstractModel._pulse_rate forms it
            d1 = self._d_logV_dphi(normalization, phi)
            d2 = self._d2_logV_dphi2(normalization, phi)
            return -(d1 * (3.0 * d2 + d1 * d1))

        for patch, label in (
            (q3_zero, "zero"),
            (lambda self, normalization, phi: float("-inf"), "inf"),
            (lambda self, normalization, phi: float("nan"), "nan"),
        ):
            with mock.patch.object(ExponentialModel, "_d3_logV_dphi3", patch):
                for method in ("classical_turning_point", "one_loop_turnaround"):
                    with self.subTest(label=label, method=method):
                        with self.assertRaises(ComputationFailureError) as ctx:
                            getattr(m, method)(WALL_NORMALIZATION, FIELD, -120.0)
                        self.assertIn("_d3_logV_dphi3", str(ctx.exception))
                        self.assertIn("pulse rate", str(ctx.exception))

    def test_b_a_wall_that_does_not_repel_is_refused_on_the_slope(self):
        m = model(ExponentialModel, 1, 1.0e-16)
        with mock.patch.object(ExponentialModel, "_d_logV_dphi", return_value=0.0):
            with self.assertRaises(ComputationFailureError) as ctx:
                m.classical_turning_point(WALL_NORMALIZATION, FIELD, -120.0)
        self.assertIn("-d ln V/dphi", str(ctx.exception))
        self.assertIn("not repulsive", str(ctx.exception))


def ln_v0m4(L: float, ln_R: float) -> float:
    """ln(V0/M^4) of the exponential at n = 1 with wall variable L and ln R (07b's fits.ln_v0m4, n = 1)."""
    return ln_R + log(8 * pi**2) + log(expm1(L)) - 2 * L - 2 * log(L**4 + 2 * L**3)


class TestTheIterationCap(unittest.TestCase):
    """
    (c) P103. 07b's slowest point (its issue [07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-
    chain-end]): the laboratory curve at n = 1, L = 1.378072, ln R = 5.003334 (07b's stage2_new.txt), realized at
    M = 4e-31 M_P with ln V0 = ln(V0/M^4) + 4 ln M and ln E = ln V0 + ln(e^L - 1), the search from 100 M.
    """

    L, LN_R = 1.378072, 5.003334

    def setUp(self):
        M = 4.0e-31 * Mp
        self.m = ExponentialModel(0, M_value(0, M), 1, beta_value(0, 2.0), _units)
        self.norm = ln_v0m4(self.L, self.LN_R) + 4.0 * log(M)
        self.log_E = self.norm + log(expm1(self.L))
        self.phi = 100.0 * M

    def test_c_the_cap(self):
        self.assertEqual(AM.ONE_LOOP_MAX_ITERATIONS, 400)

    def test_c_the_slowest_point_converges(self):
        c = self.m.classical_turning_point(self.norm, self.phi, self.log_E)
        self.assertLessEqual(abs(c.log_R - self.LN_R), 1.0e-9)
        o = self.m.one_loop_turnaround(self.norm, self.phi, self.log_E)
        self.assertIsNotNone(o)
        self.assertGreater(o.iterations, 100)
        self.assertLessEqual(o.iterations, 400)
        self.assertLess(abs(o.ell - 0.12), 0.01)
        with mock.patch.object(AM, "ONE_LOOP_MAX_ITERATIONS", 100):
            with self.assertRaises(ComputationFailureError) as ctx:
                self.m.one_loop_turnaround(self.norm, self.phi, self.log_E)
        self.assertIn("did not converge in 100 iterations", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
