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
Unresolved turnarounds (inelastic-reflection prompt 05; README §8.4, gate "Unresolved").

At alpha > 0, at the start of every step with pi < 0, integrate_scalar_history assesses the wall
ahead: assess_infall gives ln E = ln(H_E^2 pi^2/2 + V - V_floor) and the model's classical turning
point. If phi < K phi_t, with phi_t < 10^-8 M_P or ln R > 0 (unresolved_turnaround; K = 3), the field
is reflected there, through the floor branch, as a reflection of kind "unresolved". Every reflection,
the floor rule's included, drains with w_gate = 1.0 exactly (README §8.1 R10, §8.2 P20). At alpha = 0
nothing is assessed (P22); natural mode makes one model call, at its first floor reflection, for its
warning. The items are those of prompt §1.6, with orchestrator note 05's additions:

  (1) alpha = 0 is the old code: no model call on a golden history, exactly one on the P1 harness at
      M = 1e-10, and that run bitwise kcl.integrate's;
  (2) the decision at each of its edges, on synthetic values; R8's values as literals (ruling 3);
      the assessment's ln E and phi_t on test_wall_model's power-law wall;
  (3) an unresolved reflection at 3 phi_t: M = 1e-8 M_P, beta = 2, alpha = 0.1, whose first wall
      approach is judged unresolved before the floor rule fires (log 05 §5.3 says why);
  (5) the probe's band: M = 1e-9 M_P, the rule against the same history with the phi_t threshold
      patched to 0, to a window's end before rho_kin/rho_R falls below 1e-6;
  (6) M = 0.5 M_P, beta = 2, alpha = 0.1: every candidate resolved, no reflection, and the run
      bitwise the same run with the decision patched never to fire;
  (7) the warnings, and their survival of a failure (orchestrator note 05, rulings 1, 2, 8, 9);
  (8) the records' new fields.
Item (4), the floor rule's w_gate = 1, is test_injection's and test_gate_and_taper's item (8).

Every history uses test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB,
atol = rtol = 1e-8, main.py's axis, the model's seed realization, untuned) unless it says otherwise;
a window is driven by passing N_stop to integrate_scalar_history and leaving integrate_history by a
private exception before it samples (ruling 10). No Ray, no store, no PRyMordial solve. Each class
that integrates gives its cost and the load in its docstring.
"""

import contextlib
import importlib
import io
import unittest
from bisect import bisect_right
from math import exp, expm1, isfinite, log, nextafter, sqrt
from unittest import mock

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_injection as ti
from ComputeTargets.tests.test_wall_model import _PowerLawWall
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.AbstractModel import (
    AbstractModel,
    ClassicalTurningPoint,
    WALL_ROOT_XTOL,
)
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1
F_CAP = SM.StepControl().cap_fraction

# the step loop as this module was imported, before any test patches the module's name
_INTEGRATE = SM.integrate_scalar_history


class _LeaveHistory(Exception):
    """Raised by the window's wrapper to leave integrate_history before it samples (ruling 10)."""


def model_spy():
    """A spy on the model's classical_turning_point, on the class every model inherits it from."""
    return mock.patch.object(
        AbstractModel,
        "classical_turning_point",
        autospec=True,
        side_effect=AbstractModel.classical_turning_point,
    )


def window(beta: float, M: float, alpha: float, N_stop: float, patches=()):
    """
    One history at test_injection's setup, integrated to N_stop and not sampled: returns
    (IntegrationResult, the ODEPolicy, which holds the ledger, the realized model).
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    Mp = units.PlanckMass
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)
    captured = []

    def windowed(RHS, *args, **kwargs):
        kwargs["N_stop"] = N_stop
        captured.append((_INTEGRATE(RHS, *args, **kwargs), RHS.policy))
        raise _LeaveHistory()

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(SM, "integrate_scalar_history", windowed))
        for patch in patches:
            stack.enter_context(patch)
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        try:
            SM.integrate_history(
                cosmology,
                temperature(0, 2.0e4 * units.GeV),
                temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                phi_value(0, 5.0 * Mp),
                pi_value(0, 0.0),
                gth.make_z_grid(),
                realized,
                task_label="test_unresolved_turnarounds",
                alpha=alpha,
            )
        except _LeaveHistory:
            pass
    if len(captured) != 1:
        raise RuntimeError("test_unresolved_turnarounds: the window did not complete")
    result, policy = captured[-1]
    return result, policy, realized


def assessment_at(policy, model, N: float, y):
    """The loop's assessment at a step start (N, y), recomputed: (ln E, ClassicalTurningPoint)."""
    H = ti.H_Einstein(policy, N, y)
    log_rho_kin = 2.0 * log(H) + 2.0 * log(-float(y[1])) - SM.LOG_2
    return SM.assess_infall(model, float(y[0]), log_rho_kin)


# ---- (2) the constants, the decision and the assessment ----


class TestTheConstants(unittest.TestCase):
    """R8's values, as literals (orchestrator note 05, ruling 3). Microseconds."""

    def test_2_K_is_3_and_the_threshold_is_1e_minus_8_M_P(self):
        self.assertEqual(SM.UNRESOLVED_REFLECTION_FACTOR, 3.0)
        self.assertEqual(SM.UNRESOLVED_PHI_T_THRESHOLD_MP, 1.0e-8)


def turning_point(phi_t: float, log_R: float) -> ClassicalTurningPoint:
    """A synthetic classical turning point: only phi_t and ln R are read by the decision."""
    return ClassicalTurningPoint(
        phi_t=phi_t,
        log_omega_t_squared=0.0,
        log_R=log_R,
        lambda_t=1.0,
        log_k_ex=0.0,
        ell_t=0.0,
    )


class TestTheDecision(unittest.TestCase):
    """
    (2) unresolved_turnaround on synthetic values, each strict comparison at its edge: the edge
    does not fire, the next float on the other side does. M_P = 1 and M_P = 2.435e18 (GeV units'
    size) both. Microseconds.
    """

    def test_2_phi_equal_to_K_phi_t_does_not_fire(self):
        for Mp in (1.0, 2.435e18):
            phi_t = 1.0e-12 * Mp
            tp = turning_point(phi_t, -10.0)
            edge = 3.0 * phi_t
            self.assertFalse(SM.unresolved_turnaround(edge, tp, Mp))
            self.assertTrue(SM.unresolved_turnaround(nextafter(edge, 0.0), tp, Mp))

    def test_2_phi_t_equal_to_the_threshold_does_not_fire(self):
        for Mp in (1.0, 2.435e18):
            edge = 1.0e-8 * Mp
            self.assertFalse(
                SM.unresolved_turnaround(2.0 * edge, turning_point(edge, -10.0), Mp)
            )
            below = nextafter(edge, 0.0)
            self.assertTrue(
                SM.unresolved_turnaround(2.0 * below, turning_point(below, -10.0), Mp)
            )

    def test_2_ln_R_equal_to_zero_does_not_fire(self):
        for Mp in (1.0, 2.435e18):
            phi_t = 1.0e-3 * Mp
            for zero in (0.0, -0.0):
                self.assertFalse(
                    SM.unresolved_turnaround(
                        2.0 * phi_t, turning_point(phi_t, zero), Mp
                    )
                )
            above = nextafter(0.0, 1.0)
            self.assertTrue(
                SM.unresolved_turnaround(2.0 * phi_t, turning_point(phi_t, above), Mp)
            )

    def test_2_outside_K_phi_t_nothing_fires(self):
        tp = turning_point(1.0e-20, 50.0)
        self.assertFalse(SM.unresolved_turnaround(3.0e-20, tp, 1.0))
        self.assertFalse(SM.unresolved_turnaround(1.0, tp, 1.0))

    def test_2_the_rule_reads_its_constants_at_call_time(self):
        tp = turning_point(1.0e-12, -10.0)
        self.assertFalse(SM.unresolved_turnaround(5.0e-12, tp, 1.0))
        with mock.patch.object(SM, "UNRESOLVED_REFLECTION_FACTOR", 10.0):
            self.assertTrue(SM.unresolved_turnaround(5.0e-12, tp, 1.0))
        self.assertTrue(SM.unresolved_turnaround(2.0e-12, tp, 1.0))
        with mock.patch.object(SM, "UNRESOLVED_PHI_T_THRESHOLD_MP", 0.0):
            self.assertFalse(SM.unresolved_turnaround(2.0e-12, tp, 1.0))


class TestTheAssessment(unittest.TestCase):
    """
    (2) assess_infall on test_wall_model's power-law wall, V = V_0 (1 + (M/phi)^2), V_floor = V_0,
    realized at ln V_0 = -120 (Planck units), M = 1e-6 M_P, at phi = M, where V - V_floor = V_0. With
    rho_kin = 8 (V - V_floor), E = 9 (V - V_floor): V - V_floor is E/9 at the field, and the
    turning point is phi_t = phi/3 exactly. Microseconds.
    """

    NORMALIZATION = -120.0
    M = 1.0e-6

    def setUp(self):
        self.model = _PowerLawWall(self.M, p=2.0).realize(self.NORMALIZATION, 0.1)

    def test_2_ln_E_is_rho_kin_plus_the_wall(self):
        """
        ln E against ln(rho_kin + (V - V_floor)) formed here from the closed form V - V_floor =
        V_0 (M/phi)^2. The bound is log 04 §1.5's R for ln(V - V_floor), 6 EPS (|ln V| + |norm| +
        |ln E| + 1): the hooks' ln V is within 2.5 EPS (|ln V| + |norm|), ln(V - V_floor) adds an
        expm1 and a log whose condition is V_floor/W = 1 here, and the sum in logs (an exp, a log1p
        and an addition, with weight W/E = 1/9 on the first) adds a few EPS of |ln E|.
        """
        phi = self.M
        W = exp(self.NORMALIZATION) * (self.M / phi) ** 2
        log_rho_kin = log(8.0 * W)
        log_E, tp = SM.assess_infall(self.model, phi, log_rho_kin)
        direct = log(exp(log_rho_kin) + W)
        log_V = self.model.log_V(phi)
        bound = 6.0 * EPS * (abs(log_V) + abs(self.NORMALIZATION) + abs(direct) + 1.0)
        self.assertLessEqual(abs(log_E - direct), bound, (log_E, direct))
        self.assertLessEqual(abs(log_E - log(9.0 * W)), bound)

    def test_2_phi_t_is_the_root_for_that_E(self):
        """
        phi_t = phi/3, to the root's tolerance: |ln phi_t - ln(phi/3)| <= tau + R/S (log 04 §1.5),
        with tau = 1e-14 + 4 EPS |ln phi_t|, S = |phi (ln V)'| V/W = 2 V/W at phi_t, and R the
        bound above. And V(phi_t) - V_floor = E, to R.
        """
        phi = self.M
        W = exp(self.NORMALIZATION) * (self.M / phi) ** 2
        log_E, tp = SM.assess_infall(self.model, phi, log(8.0 * W))
        self.assertIsNotNone(tp)
        log_V_t = self.model.log_V(tp.phi_t)
        log_W_t = log_V_t + log(-expm1(self.NORMALIZATION - log_V_t))
        R = 6.0 * EPS * (abs(log_V_t) + abs(self.NORMALIZATION) + abs(log_E) + 1.0)
        S = abs(tp.phi_t * self.model.d_logV_dphi(tp.phi_t)) * exp(log_V_t - log_W_t)
        tau = WALL_ROOT_XTOL + 4.0 * EPS * abs(log(tp.phi_t))
        self.assertLessEqual(abs(log(tp.phi_t) - log(phi / 3.0)), tau + R / S, tp)
        self.assertLessEqual(abs(log_W_t - log_E), R + S * tau)

    def test_2_a_field_with_no_kinetic_energy_beyond_rounding_has_no_turning_point(
        self,
    ):
        """rho_kin far below the rounding of V - V_floor: ln E rounds to ln W, and the point is None."""
        phi = self.M
        W = exp(self.NORMALIZATION) * (self.M / phi) ** 2
        log_E, tp = SM.assess_infall(self.model, phi, log(W) - 60.0)
        self.assertIsNone(tp)


# ---- (1) alpha = 0 is the old code ----


class TestAlphaZeroIsTheOldCode(unittest.TestCase):
    """
    (1) and (7) at alpha = 0. The golden histories (beta = 1.5 and 5.5, about 1-2 s each) and the P1
    harness at M = 1e-10 (about 0.3 s per run) at a load of about 5-10. The golden tests,
    test_first_bounce, test_surfing_turning_points and test_kinematic_cap_loop are the bitwise
    check, unedited.
    """

    def test_1_no_model_call_and_no_warning_on_the_golden_histories(self):
        for beta in gh.BETAS:
            with model_spy() as spy:
                data = gh.run_history(beta)
            self.assertEqual(spy.call_count, 0, beta)
            self.assertEqual(data["warnings"], (), beta)
            self.assertEqual(data["reflections"], 0, beta)

    def test_1_one_model_call_on_the_floor_harness_and_the_run_is_todays(self):
        """
        The P1 harness at alpha = 0 with a ledger (test_injection's integrate_floor): one floor
        reflection, one model call, for natural mode's warning; and kcl.integrate's run bitwise.
        """
        with model_spy() as spy:
            result, policy, ledger = ti.integrate_floor(0.0)
        self.assertEqual(spy.call_count, 1)
        today, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(len(result.reflections), 1)
        self.assertEqual(result.reflections[0].kind, "floor")
        self.assertEqual(result.reflections, today.reflections)
        self.assertEqual(result.nfev, today.nfev)
        self.assertEqual(result.accepted_steps, today.accepted_steps)
        self.assertEqual(tuple(result.final_state), tuple(today.final_state))
        self.assertEqual(result.injections, [])
        self.assertTrue(ledger.is_empty)

    def test_7_natural_mode_warns_once_on_the_floor_harness(self):
        """phi_t ~ 9e-13 M_P at the floor reflection: one line, of the "below" kind."""
        result, _, _ = ti.integrate_floor(0.0)
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(
            SM.warning_kind(result.warnings[0]), SM.WARNING_TURNING_POINT_BELOW
        )
        self.assertNotIn("M = ", result.warnings[0])

    def test_7_a_failing_model_call_is_a_warning_and_the_run_is_todays(self):
        """
        With the model's classical_turning_point raising ComputationFailureError, the run succeeds,
        bitwise kcl.integrate's, with one "could not be assessed" line naming the reason.
        """

        def fails(*args, **kwargs):
            raise ComputationFailureError("test: the wall cannot be assessed")

        with mock.patch.object(
            AbstractModel, "classical_turning_point", autospec=True, side_effect=fails
        ) as raising:
            result, _, _ = ti.integrate_floor(0.0)
        self.assertEqual(raising.call_count, 1)
        today, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(result.reflections, today.reflections)
        self.assertEqual(result.nfev, today.nfev)
        self.assertEqual(tuple(result.final_state), tuple(today.final_state))
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(
            SM.warning_kind(result.warnings[0]), SM.WARNING_WALL_UNASSESSED
        )
        self.assertIn("test: the wall cannot be assessed", result.warnings[0])

    def test_5_the_reflection_records_carry_their_kind(self):
        """No Reflection of a real integration holds the namedtuple default (ruling 5)."""
        for alpha in (0.0, ALPHA):
            result, _, _ = ti.integrate_floor(alpha)
            for r in result.reflections:
                self.assertIn(r.kind, ("floor", "unresolved"), (alpha, r))


# ---- (3) an unresolved reflection at 3 phi_t ----


class TestAnUnresolvedReflection(unittest.TestCase):
    """
    (3) M = 1e-8 M_P, beta = 2, alpha = 0.1, to N = 20.6. The field falls from 5 M_P with
    |pi| ~ 0.5 M_P, and the floor rule fires only where phi/|pi| < h_floor/f = 1e-10, so near
    3 phi_t ~ 2.8e-10 M_P the assessment fires first: the first wall approach is an unresolved
    reflection at N = 20.3521, phi/phi_t = 2.959, before any turning point (log 05 §5.3). One
    window, about 0.5 s at a load of 5-10.
    """

    @classmethod
    def setUpClass(cls):
        cls.result, cls.policy, cls.model = window(2.0, 1.0e-8, ALPHA, 20.6)
        cls.reflection = cls.result.reflections[0]
        cls.injection = cls.result.injections[0]
        cls.ts = list(cls.result.solution.ts)

    def test_3_the_first_wall_approach_is_an_unresolved_reflection(self):
        r, i = self.reflection, self.injection
        self.assertEqual(r.kind, "unresolved")
        self.assertEqual(i.kind, "unresolved")
        self.assertFalse(i.resolved)
        self.assertEqual(i.N, r.N)
        self.assertEqual(i.phi_turn, r.phi_Einstein)
        self.assertEqual(i.pi_before, r.pi_Einstein_in)
        for t in self.result.turning_points:
            self.assertGreater(t.N, r.N, t)
        for x in self.result.reflections:
            self.assertIn(x.kind, ("floor", "unresolved"), x)

    def test_3_w_gate_is_one_and_alpha_eff_is_alpha(self):
        i = self.injection
        self.assertEqual(i.w_gate, 1.0)
        self.assertEqual(i.alpha_eff, ALPHA)
        self.assertTrue(isfinite(i.nonadiabaticity), i)

    def test_3_phi_lies_below_three_phi_t(self):
        """The upper bound is the decision itself, with R8's literals (ruling 3)."""
        i = self.injection
        tp = i.classical_turning_point
        self.assertEqual(i.phi_assessed, float(self.reflection.phi_Einstein))
        self.assertLess(i.phi_assessed, 3.0 * tp.phi_t)
        self.assertLess(tp.phi_t, 1.0e-8 * self.policy.units.PlanckMass)
        self.assertTrue(
            SM.unresolved_turnaround(i.phi_assessed, tp, self.policy.units.PlanckMass)
        )

    def test_3_phi_lies_above_the_bound_the_step_before_allowed(self):
        """
        Ruling 4. At the step start before the reflection (recomputed: its state is the dense
        output there, and the ledger is empty before the first candidate) the decision did not fire,
        so phi_prev >= 3 phi_t,prev. That step was capped by the velocity cap, h <= f phi_prev/|pi|,
        and the field was decelerating (pi' > 0), so it moved inwards by at most |pi| h <= f phi_prev,
        and phi >= (1 - f) 3 phi_t,prev = (1 - f) 3 phi_t (1 - delta), delta = 1 - phi_t,prev/phi_t,
        the change in phi_t between the two step starts (measured 3.4e-12; log 05 §5.3). Radau's
        step and the cap each round within a few EPS, so 8 EPS more is allowed.
        """
        i = self.injection
        k = self.ts.index(self.reflection.N)
        N_prev = self.ts[k - 1]
        y_prev = self.result.solution(N_prev)
        _, tp_prev = assessment_at(self.policy, self.model, N_prev, y_prev)
        phi_prev, pi_prev = float(y_prev[0]), float(y_prev[1])
        self.assertFalse(
            SM.unresolved_turnaround(phi_prev, tp_prev, self.policy.units.PlanckMass)
        )
        self.assertGreaterEqual(phi_prev, 3.0 * tp_prev.phi_t)

        h = self.ts[k] - N_prev
        self.assertLessEqual(h, F_CAP * phi_prev / (-pi_prev) * (1.0 + 4.0 * EPS))
        self.assertGreater(ti.pi_prime(self.policy, N_prev, y_prev), 0.0)

        phi_t = i.classical_turning_point.phi_t
        delta = 1.0 - tp_prev.phi_t / phi_t
        self.assertLess(abs(delta), 1e-10)
        bound = (1.0 - F_CAP) * 3.0 * phi_t * (1.0 - delta) * (1.0 - 8.0 * EPS)
        self.assertGreaterEqual(i.phi_assessed, bound, (i.phi_assessed / phi_t, delta))

    def test_3_pi_out_and_the_dense_output(self):
        i, r = self.injection, self.reflection
        self.assertEqual(i.pi_after, -(sqrt(1.0 - ALPHA) * r.pi_Einstein_in))
        self.assertEqual(self.result.solution(i.N)[1], i.pi_after)

    def test_3_the_ledger_equals_the_records(self):
        self.assertEqual(
            self.policy.dark_radiation.injections,
            tuple(
                (x.N, x.delta_K) for x in self.result.injections if x.alpha_eff > 0.0
            ),
        )

    def test_3_H_Einstein_is_continuous_across_it(self):
        """To test_injection's H_bound (log 02 §1.2)."""
        i = self.injection
        bound, c = ti.H_bound(i, self.policy, self.result.solution(i.N))
        ratio = abs(i.H_Einstein_after - i.H_Einstein_before) / i.H_Einstein_before
        self.assertLessEqual(ratio, bound, (ratio, bound, c))

    def test_3_first_bounce_reports_the_reflection(self):
        bounce = SM.first_bounce(self.result)
        self.assertTrue(bounce.reflected)
        self.assertEqual(bounce.N, self.reflection.N)

    def test_8_the_reflection_records_its_own_assessment(self):
        """
        Its step start's: phi, and ln E from ln rho_kin,E = 2 ln H_before + 2 ln|pi_in| - ln 2,
        which the candidate formed from the same state and ledger, so exactly. At 3 phi_t on this
        wall V - V_floor is about 1e-37 of E, below rounding, so ln E is ln rho_kin,E there.
        """
        i = self.injection
        log_E, tp = SM.assess_infall(self.model, i.phi_assessed, i.log_rho_kin)
        self.assertEqual(i.log_E_assessed, log_E)
        self.assertEqual(i.classical_turning_point, tp)
        self.assertGreaterEqual(i.log_E_assessed, i.log_rho_kin)

    def test_7_dissipation_mode_warns_nearly_elastic_with_the_first_impacts_R(self):
        self.assertEqual(len(self.result.warnings), 1)
        line = self.result.warnings[0]
        self.assertEqual(SM.warning_kind(line), SM.WARNING_NEARLY_ELASTIC)
        log10_R = self.injection.classical_turning_point.log_R / log(10.0)
        self.assertIn(f"10^{log10_R:.3f}", line)


# ---- (5) the probe's band ----

BAND_N_STOP = 30.3


class TestTheProbesBand(unittest.TestCase):
    """
    (5) M = 1e-9 M_P, beta = 2, alpha = 0.1, to N = 30.3: 05's rule against the same history with
    UNRESOLVED_PHI_T_THRESHOLD_MP patched to 0, so that every wall is resolved (ln R < 0 there,
    about -24 to -47 in log10). The planner's probes parked where rho_kin/rho_R first fell below
    1e-6; here that is at N = 30.34933 (rule) and 30.34939 (resolved) (log 05 §5.4), so the window
    ends before both. Two windows in setUpClass, about 5 + 7 s at a load of 5-10. It does not
    claim to catch K (ruling 3).
    """

    @classmethod
    def setUpClass(cls):
        cls.rule, _, _ = window(2.0, 1.0e-9, ALPHA, BAND_N_STOP)
        cls.resolved, _, _ = window(
            2.0,
            1.0e-9,
            ALPHA,
            BAND_N_STOP,
            patches=[mock.patch.object(SM, "UNRESOLVED_PHI_T_THRESHOLD_MP", 0.0)],
        )

    @staticmethod
    def approaches(result):
        walls = [t for t in result.turning_points if t.kind == "wall"]
        return len(walls) + len(result.reflections)

    def test_5_the_same_wall_approaches_and_candidates(self):
        self.assertEqual(self.approaches(self.rule), self.approaches(self.resolved))
        self.assertEqual(len(self.rule.injections), len(self.resolved.injections))
        self.assertGreater(
            sum(1 for i in self.rule.injections if i.kind == "unresolved"), 0
        )
        self.assertEqual(
            sum(1 for i in self.resolved.injections if i.kind == "unresolved"), 0
        )

    def test_5_the_drained_energy_agrees(self):
        """
        Sum of Delta rho/rho_R over the window, to 1e-5 relative. Measured (log 05 §5.4): 7.49e-7
        here (46 candidates each), and at most 1.41e-6 over the band where both runs are sound
        (M = 1e-9, 3e-10, 1e-10); the tolerance is 7 times that, far inside the prompt's 1 % stop.
        The difference comes from the crossing below 3 phi_t that the rule does not integrate: the
        energy that the wall holds at the reflection, (V - V_floor)/E, is below 4e-16 at every
        unresolved reflection before the cut, so the field leaves with the same energy, and the
        two runs differ only by where along the bounce each drain is read.
        """
        s_rule = sum(i.delta_rho_over_rho_rad for i in self.rule.injections)
        s_resolved = sum(i.delta_rho_over_rho_rad for i in self.resolved.injections)
        self.assertLessEqual(abs(s_rule / s_resolved - 1.0), 1e-5, (s_rule, s_resolved))


# ---- (6), (7) and (8) at M = 0.5 M_P ----


class TestAtHalfThePlanckMass(unittest.TestCase):
    """
    (6), (7) and (8): M = 0.5 M_P, beta = 2, alpha = 0.1 (03's item 3 history), to T_CMB, sampled,
    integrated twice: as shipped, and with unresolved_turnaround patched to return False. About
    3 + 3 s at a load of 5-10.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, recorder = ti.integrate_captured(
            2.0, 0.5, ALPHA, record_rhs=True
        )
        cls.policy = recorder.policy
        with mock.patch.object(SM, "unresolved_turnaround", return_value=False):
            cls.never_data, cls.never, _ = ti.integrate_captured(
                2.0, 0.5, ALPHA, record_rhs=True
            )
        cls.ts = list(cls.result.solution.ts)

    def test_6_every_candidate_resolved_with_an_assessment_and_no_reflection(self):
        self.assertEqual(self.result.reflections, [])
        self.assertGreater(len(self.result.injections), 0)
        for i in self.result.injections:
            self.assertEqual(i.kind, "turning_point", i)
            self.assertTrue(i.resolved, i)
            self.assertIsNotNone(i.classical_turning_point, i)
            self.assertLess(i.classical_turning_point.log_R, 0.0, i)
            self.assertEqual(i.w_gate, SM.gate_weight(i.nonadiabaticity))

    def test_6_bitwise_the_run_whose_decision_never_fires(self):
        a, b = self.result, self.never
        self.assertEqual(a.turning_points, b.turning_points)
        self.assertEqual(a.injections, b.injections)
        self.assertEqual(tuple(a.final_state), tuple(b.final_state))
        self.assertEqual(a.N_final, b.N_final)
        self.assertEqual(a.nfev, b.nfev)
        self.assertEqual(
            [tuple(s) for s in self.data["sample"]],
            [tuple(s) for s in self.never_data["sample"]],
        )
        self.assertEqual(
            self.data["dark_radiation"].injections,
            self.never_data["dark_radiation"].injections,
        )

    def test_7_one_nearly_elastic_warning_with_R_below_one(self):
        self.assertEqual(self.data["warnings"], self.result.warnings)
        self.assertEqual(len(self.result.warnings), 1)
        self.assertEqual(
            SM.warning_kind(self.result.warnings[0]), SM.WARNING_NEARLY_ELASTIC
        )
        first = self.result.injections[0]
        log10_R = first.classical_turning_point.log_R / log(10.0)
        self.assertLess(log10_R, 0.0)
        self.assertIn(f"10^{log10_R:.3f}", self.result.warnings[0])

    def test_8_a_drain_points_assessment_is_its_infalls_last(self):
        """
        Each drain point records the assessment made at the start of the step on which its
        wall-side turning point lies (the last step start of that infall with pi < 0), recomputed
        here from the dense output there (right-continuous, so the state the step started from)
        and the ledger as it stood (the drains at or before that N), exactly.
        """
        model = self.policy.model
        ledger = self.data["dark_radiation"]
        drain_Ns = [N for N, _ in ledger.injections]
        for i in self.result.injections:
            k = bisect_right(self.ts, i.N_turn) - 1
            y = self.result.solution(self.ts[k])
            self.assertLess(y[1], 0.0, i)
            view = ti.ledger_before(ledger, bisect_right(drain_Ns, self.ts[k]))
            view_policy = SM.ODEPolicy(
                "test", self.policy.cosmology, model, dark_radiation=view
            )
            log_E, tp = assessment_at(view_policy, model, self.ts[k], y)
            self.assertEqual(i.phi_assessed, float(y[0]), i)
            self.assertEqual(i.log_E_assessed, log_E, i)
            self.assertEqual(i.classical_turning_point, tp, i)


# ---- (7) the warnings in the tuning layer, and through a failure ----


class _PlainSteps:
    """TuningSecant's interface, with plain steps only."""

    def __init__(self, secant=True):
        self.reset = False

    def step(self, x, G):
        return (G[0], G[1]), "plain"

    def guard(self):
        pass


class TestTheWarningsInTheTuning(unittest.TestCase):
    """
    (7) tune_history with integrate_history and _tuning_state patched to scripted results (as
    test_tuning's wiring tests): no integration, milliseconds. Three unconverged integrations, the
    third ending in the cap's failure.
    """

    def _tune(self, payloads):
        cosmology = gth.make_cosmology()
        Mp = cosmology.units.PlanckMass
        state = SM.TuningState(
            phi_0=5.0 * Mp,
            pi_0=0.0,
            log_V=-10.0,
            V_target=Mp**4,
            phi_min=4.0 * Mp,
            residual_H=1.0e-2,
            residual_phi=1.0e-2,
        )
        out = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(SM, "integrate_history", side_effect=payloads)
            )
            stack.enter_context(
                mock.patch.object(SM, "_tuning_state", return_value=state)
            )
            stack.enter_context(mock.patch.object(SM, "TuningSecant", _PlainSteps))
            stack.enter_context(contextlib.redirect_stdout(out))
            data = gth.run_tuning(
                cosmology=cosmology, max_integrations=len(payloads), clock=lambda: 0.0
            )
        return data, [l for l in out.getvalue().splitlines() if l.startswith("!! ")]

    def test_7_each_kind_is_printed_once_from_the_first_integration_that_returns_it(
        self,
    ):
        elastic = [
            f"{SM.WARNING_NEARLY_ELASTIC}: R = 10^-{k}.000 < 1" for k in (3, 4, 5)
        ]
        below = [
            f"{SM.WARNING_TURNING_POINT_BELOW} 1e-08 M_P (phi_t = {k})" for k in (1, 2)
        ]
        payloads = [
            {"sample": [], "warnings": (elastic[0],)},
            {"sample": [], "warnings": (elastic[1], below[0])},
            {"sample": [], "warnings": (elastic[2], below[1])},
        ]
        data, lines = self._tune(payloads)
        self.assertTrue(data["failure"])
        prefix = f"!! tune_history ({gth.TASK_LABEL}): "
        self.assertEqual(lines[:2], [prefix + elastic[0], prefix + below[0]])
        self.assertEqual(len(lines), 3)
        self.assertIn("the normalization did not converge", lines[2])

    def test_7_no_warning_no_line(self):
        data, lines = self._tune([{"sample": []}, {"sample": [], "warnings": ()}])
        self.assertEqual(len(lines), 1)
        self.assertIn("the normalization did not converge", lines[0])


def failing_floor_harness(*args, **kwargs):
    """
    Stands in for integrate_scalar_history inside integrate_history, ignoring its arguments: the
    P1 harness at M = 1e-10, alpha = 0, with a step budget three steps past its floor reflection
    (at accepted step 210), so that the loop fails after natural mode's warning.
    """
    beta, N0, state = kcl.P1
    policy, rhs, supervisor = kcl.build(beta, 1e-10)
    with supervisor:
        return _INTEGRATE(
            rhs,
            supervisor,
            StateVector._make(state),
            N0,
            kcl._log_T_stop,
            SM.StepControl(step_budget=213),
            policy=policy,
            N_stop=21.0,
            task_label=kwargs.get("task_label"),
        )


class TestTheWarningsSurviveAFailure(unittest.TestCase):
    """
    (7), orchestrator note 05, ruling 2: a natural-mode history that fails after its warning. The
    P1 harness (failing_floor_harness), driven through integrate_history and through tune_history
    with gth's model; about 0.3 s per run at a load of 5-10.
    """

    def test_7_the_loop_raises_with_its_warnings(self):
        result, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        k = list(result.solution.ts).index(result.reflections[0].N)
        self.assertEqual(k, 210)
        with self.assertRaises(ComputationFailureError) as caught:
            with contextlib.redirect_stdout(io.StringIO()):
                failing_floor_harness()
        self.assertIn("step budget exhausted", caught.exception.message)
        self.assertEqual(len(caught.exception.warnings), 1)
        self.assertEqual(
            SM.warning_kind(caught.exception.warnings[0]),
            SM.WARNING_TURNING_POINT_BELOW,
        )

    def test_7_the_failure_payload_carries_them(self):
        cosmology = gth.make_cosmology()
        units = cosmology.units
        with mock.patch.object(
            SM, "integrate_scalar_history", failing_floor_harness
        ), contextlib.redirect_stdout(io.StringIO()):
            data = SM.integrate_history(
                cosmology,
                temperature(0, 2.0e4 * units.GeV),
                temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                phi_value(0, 5.0 * units.PlanckMass),
                pi_value(0, 0.0),
                gth.make_z_grid(),
                gth.make_model(units).realize(0.0, 0.0),
                task_label="test_unresolved_turnarounds",
            )
        self.assertTrue(data["failure"])
        self.assertTrue(data["failure_reason"].startswith("step budget exhausted"))
        self.assertEqual(len(data["warnings"]), 1)
        self.assertEqual(
            SM.warning_kind(data["warnings"][0]), SM.WARNING_TURNING_POINT_BELOW
        )

    def test_7_tune_history_prints_them_before_its_failure_line(self):
        out = io.StringIO()
        with mock.patch.object(
            SM, "integrate_scalar_history", failing_floor_harness
        ), contextlib.redirect_stdout(out):
            data = gth.run_tuning()
        self.assertTrue(data["failure"])
        lines = [
            l for l in out.getvalue().splitlines() if l.startswith("!! tune_history")
        ]
        prefix = f"!! tune_history ({gth.TASK_LABEL}): "
        self.assertEqual(len(lines), 2, lines)
        self.assertTrue(lines[0].startswith(prefix + SM.WARNING_TURNING_POINT_BELOW))
        self.assertTrue(lines[1].startswith(prefix + "step budget exhausted"))


if __name__ == "__main__":
    unittest.main()
