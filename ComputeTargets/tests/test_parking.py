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
Parking (inelastic-reflection prompt 06; README §8.4, gate "Park").

At alpha > 0, on a model with a wall, integrate_scalar_history tests every candidate after the
history's first: if rho_kin,E/rho_R,E < PARKING_EPSILON, and the zero-lag tracker there has
m_eff/H_E >= PARKING_ADIABATICITY_THRESHOLD, the leftover kinetic energy goes into the ledger and
the history continues in the parked phase: a reduced integration of (ln rho_R,E, ln f_m, ln T_J) at
phi = phi_min, pi = d phi_min/dN, whose segments return the full state through the dense output.
At a reflection where the cut did not fire, |pi_in| < PARKING_PI_GUARD_MP M_P fails the history.
The items are those of prompt §1.7, with orchestrator note 06's additions:

  (1) alpha = 0 is the old code: no call of minimum_on_trace or d_minimum_d_log_trace on a golden
      history, on the P1 harness, or on the P1 harness with two floor reflections with the cut
      opened wide (ruling 10);
  (2) the decisions at their edges, on synthetic values; and in the loop, a cut opened wide parks
      at the history's second candidate, never its first (ruling 1);
  (3) the tracker's velocity: against a finite difference of phi_min along a constructed trace, on
      synthetic states at small and large M (ruling 5); the reduced right-hand side's d ln T/dN
      against a + b pi_min (ruling 4); and on a parked history's samples;
  (4) M = 1e-30 M_P, beta = 2, alpha = 0.1 reaches T_CMB, parking where log 05 §5.5 found the cut,
      and is bitwise the cut-disabled history up to the switch;
  (5) the switch: H_E continuous, the ledger, the dense output at the switch;
  (6) the parked samples on phi_min, pi on them, the segments deterministic, integrate_history's
      readers;
  (7) M = 0.5 M_P: no parking at beta = 2; at beta = 5 a park at a drain point and, where the
      tracker loses adiabaticity, a release (inelastic-reflection prompt 07, which replaced 06's
      failure there; test_release tests the release itself);
  (8) N_stop inside the parked phase;
  (9) the guard (P21).
Item (10) is the setup of test_injection's TestTwoTurningPointsOnOneStep and test_gate_and_taper's
TestTheNearStallIsGated, which run with the cut disabled.

Every history uses test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB,
atol = rtol = 1e-8, main.py's axis, the model's seed realization, untuned) unless it says otherwise.
"The cut disabled" is PARKING_EPSILON patched to 0.0, so that the ratio test never passes; the
guard stays. No Ray, no store, no PRyMordial solve. Each class that integrates gives its cost and
the load in its docstring.
"""

import contextlib
import importlib
import io
import unittest
from bisect import bisect_right
from math import exp, isfinite, log, nextafter, pi
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_injection as ti
from ComputeTargets.tests import test_unresolved_turnarounds as tut
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.AbstractModel import (
    AbstractModel,
    WALL_ROOT_RTOL,
    WALL_ROOT_XTOL,
)
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import (
    ScalarFieldIntegrationSupervisor,
    StateVector,
)

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1

# the step loop as this module was imported, before any test patches the module's name
_INTEGRATE = SM.integrate_scalar_history

# the lower end of the equation of state's table: below 10 keV w = 1/3 exactly and dw/d ln T = 0,
# above it dw/d ln T = -3.6e-8, so pi_min, which reads dw/d ln T, jumps there (log 06 §5)
EOS_TABLE_LOWEST_T_GEV = 1.0e-5


def no_cut():
    """The cut disabled: PARKING_EPSILON = 0.0, so that rho_kin/rho_R < epsilon never holds."""
    return mock.patch.object(SM, "PARKING_EPSILON", 0.0)


def wide_open_cut():
    """Every candidate after the first passes both tests: epsilon = inf, threshold 0.0."""
    return (
        mock.patch.object(SM, "PARKING_EPSILON", float("inf")),
        mock.patch.object(SM, "PARKING_ADIABATICITY_THRESHOLD", 0.0),
    )


@contextlib.contextmanager
def minimum_spy():
    """Spies on the model's minimum_on_trace and d_minimum_d_log_trace, on the class (ruling 10)."""
    with mock.patch.object(
        AbstractModel,
        "minimum_on_trace",
        autospec=True,
        side_effect=AbstractModel.minimum_on_trace,
    ) as a, mock.patch.object(
        AbstractModel,
        "d_minimum_d_log_trace",
        autospec=True,
        side_effect=AbstractModel.d_minimum_d_log_trace,
    ) as b:
        yield a, b


def history(beta: float, M: float, alpha: float, patches=()):
    """
    integrate_history at test_injection's setup, with the IntegrationResult captured, under the
    given patches. Returns (payload, IntegrationResult or None, the ODEPolicy or None); a failure's
    payload is returned, not raised.
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    Mp = units.PlanckMass
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)
    captured = []

    def capturing(RHS, *args, **kwargs):
        captured.append(RHS.policy)
        result = _INTEGRATE(RHS, *args, **kwargs)
        captured.append(result)
        return result

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(SM, "integrate_scalar_history", capturing)
        )
        for patch in patches:
            stack.enter_context(patch)
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        data = SM.integrate_history(
            cosmology,
            temperature(0, 2.0e4 * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, 5.0 * Mp),
            pi_value(0, 0.0),
            gth.make_z_grid(),
            realized,
            task_label="test_parking",
            alpha=alpha,
        )
    policy = captured[0] if captured else None
    result = captured[1] if len(captured) > 1 else None
    return data, result, policy


def floor_harness(alpha: float, N_stop: float, patches=()):
    """
    The P1 harness at M = 1e-10 (test_injection's integrate_floor) to N_stop, under the patches.
    To N = 21.5 it holds two floor reflections: N = 20.352 and 21.408 at alpha = 0, 20.352 and
    21.381 at alpha = 0.1. About 0.2 s.
    """
    beta, N0, state = kcl.P1
    policy, rhs, supervisor, ledger = ti.build_with_ledger(beta, 1e-10)
    with contextlib.ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        stack.enter_context(supervisor)
        result = SM.integrate_scalar_history(
            rhs,
            supervisor,
            StateVector._make(state),
            N0,
            kcl._log_T_stop,
            SM.StepControl(),
            policy=policy,
            N_stop=N_stop,
            alpha=alpha,
        )
    return result, policy, ledger


def root_tolerance(phi: float) -> float:
    """
    The largest difference in ln phi between two roots of minimum_on_trace on one trace from
    different starts: each lies within xtol + rtol |ln phi| of the root (brentq's bound on its
    bracket, WALL_ROOT_XTOL and WALL_ROOT_RTOL), so two lie within twice that.
    """
    return 2.0 * (WALL_ROOT_XTOL + WALL_ROOT_RTOL * abs(log(phi)))


def H_change_and_bound(result, model, Mp: float):
    """
    (|H_parked/H_before - 1|, its bound) at a history's switch (item 5). With the leftover in the
    ledger, 3 M_P^2 H^2 - H^2 pi^2/2 = rho_R (1 + f_m) + rho_chi + V, and the ledger gains
    alpha_eff H_before^2 pi_in^2/2 + (1 - alpha_eff) H_after^2 pi_in^2/2, which is the candidate's
    kinetic energy but for (1 - alpha_eff) pi_in^2 (H_after^2 - H_before^2)/2. So

        3 M_P^2 (H_parked^2 - H_before^2) = V(phi_min) - V(phi) + H_parked^2 pi_min^2/2
                                           + (1 - alpha_eff) pi_in^2 (H_after^2 - H_before^2)/2,

    and |H_parked/H_before - 1| is at most the sum of their magnitudes over 6 M_P^2 H_before^2, plus
    the rounding of two H_E: each is exp(ln V - ln(V/3H^2 M_P^2))^(1/2), relative error at most
    eps (|ln V| + |ln 3H^2 M_P^2| + 4), so 2 eps of that for the ratio.
    """
    p = result.parking
    x = result.injections[p.candidate_index]
    log_V = model.log_V(p.phi_Einstein)
    dV = exp(model.log_V(p.phi_min)) - exp(log_V)
    kinetic = 0.5 * p.H_Einstein_parked**2 * p.pi_min**2
    drain = (
        0.5
        * (1.0 - x.alpha_eff)
        * x.pi_before**2
        * abs(x.H_Einstein_after**2 - x.H_Einstein_before**2)
    )
    three_H2_Mp2 = 3.0 * Mp**2 * p.H_Einstein_before**2
    rounding = 2.0 * EPS * (abs(log_V) + abs(log(three_H2_Mp2)) + 4.0)
    bound = (abs(dV) + kinetic + drain) / (2.0 * three_H2_Mp2) + rounding
    return abs(p.H_Einstein_parked / p.H_Einstein_before - 1.0), bound


# ---- the constants and the decisions (2) ----


class TestTheConstants(unittest.TestCase):
    def test_2_the_constants_are_the_rulings(self):
        """R14's epsilon, R16's m/H = 10 and P21's 1e-9 M_P, as literals (P18)."""
        self.assertEqual(SM.PARKING_EPSILON, 1.0e-6)
        self.assertEqual(SM.PARKING_ADIABATICITY_THRESHOLD, 10.0)
        self.assertEqual(SM.PARKING_PI_GUARD_MP, 1.0e-9)


class TestTheDecisions(unittest.TestCase):
    """(2) On synthetic floats (orchestrator note 06, ruling 1)."""

    def test_2_epsilon_itself_does_not_fire_and_the_next_float_below_does(self):
        eps = SM.PARKING_EPSILON
        self.assertFalse(SM.parking_ratio_test(1, eps))
        self.assertTrue(SM.parking_ratio_test(1, nextafter(eps, 0.0)))
        self.assertTrue(SM.parking_ratio_test(7, 0.0))
        self.assertFalse(SM.parking_ratio_test(1, float("nan")))

    def test_2_the_first_candidate_never_fires(self):
        for ratio in (0.0, 1e-300, nextafter(SM.PARKING_EPSILON, 0.0)):
            self.assertFalse(SM.parking_ratio_test(0, ratio), ratio)
        with mock.patch.object(SM, "PARKING_EPSILON", float("inf")):
            self.assertFalse(SM.parking_ratio_test(0, 1.0))
            self.assertTrue(SM.parking_ratio_test(1, 1.0))

    def test_2_m_over_H_of_ten_fires_and_the_next_float_below_does_not(self):
        threshold = SM.PARKING_ADIABATICITY_THRESHOLD
        self.assertTrue(SM.tracker_adiabatic(threshold))
        self.assertFalse(SM.tracker_adiabatic(nextafter(threshold, 0.0)))
        self.assertTrue(SM.tracker_adiabatic(1e20))
        self.assertFalse(SM.tracker_adiabatic(float("nan")))

    def test_9_the_guard_at_its_edge(self):
        """|pi_in| = 1e-9 M_P does not fail, the next float below does, of either sign."""
        for Mp in (1.0, 2.435e18):
            edge = SM.PARKING_PI_GUARD_MP * Mp
            self.assertFalse(SM.parking_guard(edge, Mp))
            self.assertFalse(SM.parking_guard(-edge, Mp))
            self.assertTrue(SM.parking_guard(nextafter(edge, 0.0), Mp))
            self.assertTrue(SM.parking_guard(-nextafter(edge, 0.0), Mp))

    def test_2_the_decisions_read_their_constants_at_call_time(self):
        with mock.patch.object(SM, "PARKING_EPSILON", 1e-3):
            self.assertTrue(SM.parking_ratio_test(1, 1e-4))
        with mock.patch.object(SM, "PARKING_ADIABATICITY_THRESHOLD", 100.0):
            self.assertFalse(SM.tracker_adiabatic(50.0))
        with mock.patch.object(SM, "PARKING_PI_GUARD_MP", 1.0):
            self.assertTrue(SM.parking_guard(0.5, 1.0))


class TestTheCutInTheLoop(unittest.TestCase):
    """
    (2), ruling 1's loop-level test, and (9): the P1 harness at M = 1e-10, alpha = 0.1, to N = 21.5,
    with its two floor reflections (about 0.3 s per run at a load of about 10).
    """

    def test_2_a_wide_open_cut_parks_at_the_second_candidate(self):
        result, policy, ledger = floor_harness(ALPHA, 21.5, patches=wide_open_cut())
        self.assertIsNotNone(result.parking)
        self.assertEqual(result.parking.candidate_index, 1)
        self.assertEqual(len(result.injections), 2)
        self.assertEqual(len(result.reflections), 2)
        self.assertEqual(result.parking.N, result.reflections[1].N)
        self.assertEqual(result.N_final, 21.5)

    def test_2_with_the_shipped_constants_neither_candidate_parks(self):
        result, _, _ = floor_harness(ALPHA, 21.5)
        self.assertIsNone(result.parking)
        self.assertEqual(len(result.injections), 2)

    def test_9_the_guard_fails_the_second_reflection(self):
        """
        No history reaches |pi_in| < 1e-9 M_P at a reflection before another failure (log 06 §5.4:
        guard G2 refuses the reflection first at M = 1e-7 to 1e-12, and the model's wall search fails
        first at 1e-30), so the threshold is raised by a patch to 0.45 M_P, between the harness's
        |pi_in| = 0.4977 and 0.4232 M_P. The first reflection passes; the second fails, its reason
        naming |pi_in| and rho_kin/rho_R; the ratio test did not pass there, so m/H was not computed.
        """
        with self.assertRaises(SM.ComputationFailureError) as caught:
            floor_harness(
                ALPHA,
                21.5,
                patches=(mock.patch.object(SM, "PARKING_PI_GUARD_MP", 0.45),),
            )
        message = caught.exception.message
        self.assertIn("the |pi| guard (P21)", message)
        pi_in = float(message.split("|pi_in| = ")[1].split(" ")[0])
        ratio = float(message.split("rho_kin/rho_R,E = ")[1].split(",")[0])
        N = float(message.split(" at N = ")[1].split(" ")[0])
        self.assertAlmostEqual(pi_in, 0.4232, 4)
        self.assertAlmostEqual(ratio, 0.03088, 5)
        self.assertAlmostEqual(N, 21.38082, 5)
        self.assertIn("m_eff/H_E = not computed", message)

    def test_9_the_guards_reason_gives_m_over_H_when_the_tracker_ran(self):
        """With epsilon = inf and the threshold at inf, the tracker runs and does not park."""
        with self.assertRaises(SM.ComputationFailureError) as caught:
            floor_harness(
                ALPHA,
                21.5,
                patches=(
                    mock.patch.object(SM, "PARKING_PI_GUARD_MP", 0.45),
                    mock.patch.object(SM, "PARKING_EPSILON", float("inf")),
                    mock.patch.object(
                        SM, "PARKING_ADIABATICITY_THRESHOLD", float("inf")
                    ),
                ),
            )
        message = caught.exception.message
        self.assertIn("the |pi| guard (P21)", message)
        self.assertNotIn("not computed", message)
        m_over_H = float(message.split("m_eff/H_E = ")[1].rstrip(")"))
        self.assertTrue(isfinite(m_over_H) and m_over_H > 0.0, message)


# ---- (1) alpha = 0 ----


class TestAlphaZeroIsTheOldCode(unittest.TestCase):
    """
    (1) The golden histories (beta = 1.5 and 5.5, about 1-2 s each) and the P1 harness (about 0.2 s
    per run), at a load of about 10. The golden tests, test_first_bounce,
    test_surfing_turning_points, test_kinematic_cap_loop and test_unresolved_turnarounds are the
    bitwise check, unedited.
    """

    def test_1_no_minimum_on_a_golden_history(self):
        for beta in gh.BETAS:
            with minimum_spy() as (spy_min, spy_d):
                data = gh.run_history(beta)
            self.assertEqual(spy_min.call_count, 0, beta)
            self.assertEqual(spy_d.call_count, 0, beta)
            self.assertIsNone(data["parking"], beta)

    def test_1_no_minimum_on_the_floor_harness(self):
        with minimum_spy() as (spy_min, spy_d):
            result, _, ledger = ti.integrate_floor(0.0)
        self.assertEqual(spy_min.call_count, 0)
        self.assertEqual(spy_d.call_count, 0)
        self.assertIsNone(result.parking)
        self.assertTrue(ledger.is_empty)

    def test_1_none_with_two_reflections_and_the_cut_wide_open(self):
        """
        Ruling 10: the golden histories have no reflection, and the P1 harness to N = 21 one, so the
        ratio test could never pass on them at alpha = 0 even if it ran. To N = 21.5 the harness has
        two floor reflections; with epsilon = inf and the threshold 0.0 the second would park if the
        cut ran at alpha = 0. It does not, and the run is the unpatched one bitwise.
        """
        failure = None
        with minimum_spy() as (spy_min, spy_d):
            try:
                result, _, ledger = floor_harness(0.0, 21.5, patches=wide_open_cut())
            except Exception as e:  # the spies are read first, whatever the run did
                failure = e
        self.assertEqual(spy_min.call_count, 0)
        self.assertEqual(spy_d.call_count, 0)
        self.assertIsNone(failure)
        self.assertEqual(len(result.reflections), 2)
        self.assertIsNone(result.parking)
        self.assertTrue(ledger.is_empty)
        plain, _, _ = floor_harness(0.0, 21.5)
        self.assertEqual(result.reflections, plain.reflections)
        self.assertEqual(result.nfev, plain.nfev)
        self.assertEqual(tuple(result.final_state), tuple(plain.final_state))


# ---- (3) the tracker's velocity, on synthetic states ----


def synthetic_setup(beta: float, M: float):
    """(realized model, ODEPolicy, ODERHS, supervisor) at the seed realization."""
    cosmology = gth.make_cosmology()
    units = cosmology.units
    model = ExponentialModel(
        0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
    )
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)
    policy = SM.ODEPolicy("test_parking", cosmology, realized)
    rhs = SM.ODERHS("test_parking", policy)
    supervisor = ScalarFieldIntegrationSupervisor(
        units,
        temperature(0, 2.0e4 * units.GeV),
        temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
        label="test_parking",
        collect_full_statistics=False,
    )
    return realized, policy, rhs, supervisor


def synthetic_reduced_state(cosmology, T_Jordan: float) -> np.ndarray:
    """
    (ln rho_R,E, ln f_m, ln T_J) at T_J, built as integrate_history builds its initial state, with
    A = 1: rho_R = (pi^2/30) g_rho T^4, f_m = rho_m0 (1 + z)^3/rho_R.
    """
    log_rho = log(pi * pi / 30.0) + 4.0 * log(T_Jordan) + log(cosmology.G_rho(T_Jordan))
    log_fm = log(cosmology.rho_m0) + 3.0 * log(1.0 + cosmology.z(T_Jordan)) - log_rho
    return np.array([log_rho, log_fm, log(T_Jordan)])


# the synthetic states of item 3 (ruling 5): small M, and M = 0.5 M_P at beta = 2 and 5, where B5's
# extra (ln A)' in the denominator of s = d phi_min/d ln T moves s, and so pi_min, by about |(ln A)' s|
# relative: 1.0e-4 to 6.3e-4 at these states (log 06 §1.2), against 1e-11 at M = 1e-7
SYNTHETIC_MODELS = ((2.0, 1e-7), (2.0, 1e-30), (2.0, 0.5), (5.0, 0.5))
SYNTHETIC_T_MEV = (1.0e3, 10.0, 0.1)


class TestTheTrackersVelocity(unittest.TestCase):
    """
    (3) On synthetic states (no integration; about 1 s in all).

    The constructed trace through a state y0 is the straight line y(N) = y0 + (N - N0) y0', with
    y0' = ParkedRHS at y0, so that d ln T/dN there is the right-hand side's at (phi_min, pi_min).
    Along it phi_min(N) = minimum_on_trace(ln T(y(N))), from one fixed start, and its derivative at
    N0 is s d ln T/dN = pi_min exactly if pi_min solves pi = s (a + b pi).

    The finite difference is the five-point central one, D(h), at h = 1e-3, compared with D(2h).
    Its truncation error falls at least as h (measured: as h^3 to h^4 down to h = 1e-4), so
    |D(h) - pi| <= |D(2h) - D(h)|; the roots' own error, at most root_tolerance in ln phi per root,
    adds 18/12 of it times phi/h. The tolerance is the sum of the two, and at every large-M state it
    lies below B5's change of pi_min (log 06 §1.2 tabulates both).
    """

    @classmethod
    def setUpClass(cls):
        cls.cases = []
        for beta, M in SYNTHETIC_MODELS:
            realized, policy, rhs, supervisor = synthetic_setup(beta, M)
            cosmology = policy.cosmology
            units = cosmology.units
            with contextlib.redirect_stdout(io.StringIO()), supervisor:

                def fun(N, y, rhs=rhs, supervisor=supervisor):
                    return rhs(N, y, supervisor)

                for T_MeV in SYNTHETIC_T_MEV:
                    y0 = synthetic_reduced_state(cosmology, T_MeV * units.MeV)
                    log_T, _, _ = SM.log_trace(cosmology, y0)
                    start = 1e-3 * units.PlanckMass if M > 1e-3 else M
                    anchor = realized.minimum_on_trace(start, log_T)
                    parked_rhs = SM.ParkedRHS(fun, realized, cosmology, anchor)
                    phi_min, pi_min = SM.parked_tracker(realized, cosmology, anchor, y0)
                    terms = SM.tracker_velocity_terms(realized, cosmology, phi_min, y0)
                    y_dot = parked_rhs(0.0, y0)

                    def phi_at(h, y0=y0, y_dot=y_dot, anchor=anchor):
                        y = y0 + h * y_dot
                        return realized.minimum_on_trace(
                            anchor, SM.log_trace(cosmology, y)[0]
                        )

                    def D(h):
                        return (
                            -phi_at(2 * h)
                            + 8 * phi_at(h)
                            - 8 * phi_at(-h)
                            + phi_at(-2 * h)
                        ) / (12.0 * h)

                    cls.cases.append(
                        dict(
                            label=(beta, M, T_MeV),
                            beta=beta,
                            M=M,
                            phi_min=phi_min,
                            pi_min=pi_min,
                            terms=terms,
                            y0=y0,
                            y_dot=y_dot,
                            D1=D(1e-3),
                            D2=D(2e-3),
                            w_prime=float(cosmology.dw_dlogT(exp(y0[2]))),
                            fm=exp(y0[1]),
                            Sigma=1.0 - 3.0 * float(cosmology.w(exp(y0[2]))),
                            B5=abs(realized.d_logOmega_dphi(phi_min) * terms.s),
                        )
                    )

    def test_3_pi_min_against_a_finite_difference_of_phi_min(self):
        for c in self.cases:
            h = 1e-3
            tolerance = (
                abs(c["D2"] - c["D1"])
                + 1.5 * root_tolerance(c["phi_min"]) * abs(c["phi_min"]) / h
            )
            self.assertLessEqual(
                abs(c["D1"] - c["pi_min"]),
                tolerance,
                (c["label"], c["D1"], c["pi_min"], tolerance),
            )
            if c["M"] > 1e-3:
                # B5 would move pi_min by about |(ln A)' s| relative: the test must see it
                self.assertLess(tolerance, 0.1 * c["B5"] * abs(c["pi_min"]), c["label"])

    def test_3_the_reduced_right_hand_side_is_a_plus_b_pi_min(self):
        """
        d ln T/dN formed from ParkedRHS's three derivatives (ODERHS's components 2-4 at
        (phi_min, pi_min)) equals a + b pi_min from tracker_velocity_terms, to rounding: about ten
        operations on terms of order |a|, so 16 eps |a| + 16 eps |b pi_min|. B6 (pi left out of the
        right-hand side) moves it by |b pi_min|, which is 1.5e-10 to 3.9e-10 relative at M = 1e-7 and
        4e-4 to 3e-3 at M = 0.5 (log 06 §1.2).
        """
        for c in self.cases:
            t = c["terms"]
            d_rho, d_fm, d_T = c["y_dot"]
            d_Sigma = -3.0 * c["w_prime"] * d_T
            d_log_T = d_rho + (d_Sigma + c["fm"] * d_fm) / (c["Sigma"] + c["fm"])
            expected = t.a + t.b * t.pi_min
            tolerance = 16.0 * EPS * (abs(t.a) + abs(t.b * t.pi_min))
            self.assertLessEqual(
                abs(d_log_T - expected), tolerance, (c["label"], d_log_T, expected)
            )
            self.assertEqual(t.pi_min, c["pi_min"], c["label"])

    def test_3_the_parked_state_is_the_trackers(self):
        """parked_tracker's pi is tracker_velocity's at its phi_min, bitwise."""
        for c in self.cases:
            realized, policy, _, _ = synthetic_setup(c["beta"], c["M"])
            self.assertEqual(
                SM.tracker_velocity(realized, policy.cosmology, c["phi_min"], c["y0"]),
                c["pi_min"],
                c["label"],
            )


# ---- (4), (5), (6), (3) on a parked history at M = 1e-30 ----

# item (4)'s reference window ends here (ruling 8): after the switch at N = 30.349, before log 05 §5.5's
# failure at N = 31.086
REFERENCE_N_STOP = 30.9


class TestADrainedHistoryAtLaboratoryM(unittest.TestCase):
    """
    (3), (4), (5) and (6): M = 1e-30 M_P, beta = 2, alpha = 0.1, to T_CMB and sampled (about 8 s),
    and the same history with the cut disabled to N = 30.9 (ruling 8; about 10 s), at a load of
    about 10. At 9c2483b the full history fails at N = 31.086 in the model's wall search (log 05
    §5.5); log 05 put the cut at candidate 48, N = 30.349.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, cls.policy = history(2.0, 1e-30, ALPHA)
        cls.reference, cls.reference_policy, _ = tut.window(
            2.0, 1e-30, ALPHA, REFERENCE_N_STOP, patches=(no_cut(),)
        )
        cls.model = cls.policy.model
        cls.cosmology = cls.policy.cosmology
        cls.units = cls.cosmology.units
        cls.parking = cls.result.parking
        cls.ts = list(cls.result.solution.ts)
        cls.k_switch = cls.ts.index(cls.parking.N)
        cls.ledger = cls.data["dark_radiation"]

    def test_4_it_reaches_T_CMB(self):
        self.assertFalse(self.data.get("failure", False), self.data)
        T_CMB = self.cosmology.T_CMB_Kelvin * self.units.Kelvin
        self.assertAlmostEqual(self.result.final_state.log_T_Jordan, log(T_CMB), 9)
        self.assertGreater(self.result.N_final, 48.0)

    def test_4_it_parks_where_log_05_found_the_cut(self):
        p = self.parking
        self.assertEqual(p.candidate_index, 48)
        self.assertEqual(len(self.result.injections), 49)
        self.assertEqual(self.result.injections[48].kind, "floor")
        self.assertAlmostEqual(p.N, 30.349, 3)
        self.assertLess(p.rho_kin_over_rho_rad, SM.PARKING_EPSILON)
        for i in self.result.injections[1:48]:
            self.assertGreaterEqual(
                exp(i.log_rho_kin - self.reference.solution(i.N)[2]),
                SM.PARKING_EPSILON,
                i,
            )

    def test_4_bitwise_the_cut_disabled_history_up_to_the_switch(self):
        """The records and the dense output on every step before the switch's N (ruling 8)."""
        a, b = self.result, self.reference
        N_switch = self.parking.N
        self.assertEqual(a.injections, b.injections[: len(a.injections)])
        self.assertEqual(a.reflections, [r for r in b.reflections if r.N <= N_switch])
        self.assertEqual(
            a.turning_points, [t for t in b.turning_points if t.N <= N_switch]
        )
        b_ts = list(b.solution.ts)
        self.assertEqual(self.ts[: self.k_switch + 1], b_ts[: self.k_switch + 1])
        for k in range(self.k_switch):
            lo, hi = self.ts[k], self.ts[k + 1]
            for N in (lo, 0.5 * (lo + hi), hi):
                self.assertEqual(
                    tuple(a.solution.interpolants[k](N)),
                    tuple(b.solution.interpolants[k](N)),
                    (k, N),
                )
        self.assertEqual(
            self.ledger.injections[:49],
            self.reference_policy.dark_radiation.injections[:49],
        )
        self.assertEqual(a.accepted_steps - self.parking.parked_steps, self.k_switch)

    def test_4_the_parking_record_by_construction(self):
        p = self.parking
        x = self.result.injections[p.candidate_index]
        # the drained state's ln rho_R,E, ln f_m and ln T_J are the first parked segment's at its left
        # node (DOP853's interpolant returns y_old there exactly)
        y_post = self.result.solution(p.N)
        self.assertEqual(p.N, x.N)
        self.assertEqual(p.phi_Einstein, x.phi_turn)
        self.assertEqual(p.pi_Einstein, x.pi_after)
        self.assertEqual(p.rho_kin_over_rho_rad, exp(x.log_rho_kin - float(y_post[2])))
        dK, drho = SM.parking_leftover(x.H_Einstein_after, x.pi_after, p.N, y_post[2])
        self.assertEqual((p.delta_K, p.delta_rho_over_rho_rad), (dK, drho))
        self.assertEqual(p.H_Einstein_before, x.H_Einstein_before)
        self.assertEqual(p.log_T_Jordan, float(y_post[4]))
        self.assertEqual(
            p.delta_log_A,
            self.model.log_Omega(p.phi_min) - self.model.log_Omega(p.phi_Einstein),
        )
        self.assertGreaterEqual(p.m_eff_over_H, SM.PARKING_ADIABATICITY_THRESHOLD)
        self.assertLessEqual(p.min_m_eff_over_H, p.m_eff_over_H)
        self.assertGreaterEqual(p.min_m_eff_over_H, SM.PARKING_ADIABATICITY_THRESHOLD)
        self.assertEqual(p.parked_steps, len(self.ts) - 1 - self.k_switch)
        self.assertEqual(self.data["parking"], p)
        for value in p:
            self.assertIsInstance(value, (float, int), p)

    def test_5_the_ledger_holds_the_leftover(self):
        """
        Two entries at the switch's N: the candidate's drain and the leftover; K after the switch is
        K before the candidate plus both, and K at the history's end is the same: nothing after.
        """
        p = self.parking
        x = self.result.injections[p.candidate_index]
        entries = self.ledger.injections
        self.assertEqual(entries[-2], (p.N, x.delta_K))
        self.assertEqual(entries[-1], (p.N, p.delta_K))
        K_before = self.ledger.K(nextafter(p.N, 0.0))
        self.assertEqual(self.ledger.K(p.N), K_before + x.delta_K + p.delta_K)
        self.assertEqual(self.ledger.K(self.result.N_final), self.ledger.K(p.N))

    def test_5_H_is_continuous_at_the_switch(self):
        """
        H_change_and_bound's bound (which see); and far below the leftover fraction, which is what
        H would move by without the leftover in the ledger (P19). Here V(phi_min) - V(phi) and the
        tracker's kinetic energy round to nothing, and H_E is continuous bitwise.
        """
        change, bound = H_change_and_bound(
            self.result, self.model, self.units.PlanckMass
        )
        self.assertLessEqual(change, bound, (change, bound))
        self.assertLess(change, 1e-3 * self.parking.delta_rho_over_rho_rad)

    def test_5_the_dense_output_at_the_switch_is_the_parked_state(self):
        p = self.parking
        y = self.result.solution(p.N)
        self.assertEqual((y[0], y[1]), (p.phi_min, p.pi_min))
        self.assertEqual(y[4], p.log_T_Jordan)
        # the last oscillating step's interpolant at its right end: the same state to its rounding
        y_osc = self.result.solution.interpolants[self.k_switch - 1](p.N)
        for j in (2, 3, 4):
            self.assertLessEqual(abs(y[j] - y_osc[j]), 64.0 * EPS * abs(y[j]), j)

    def parked_samples(self):
        return [s for s in self.data["sample"] if s.raw_N > self.parking.N]

    def test_6_phi_on_every_parked_sample_is_phi_min(self):
        """minimum_on_trace from the sample's own phi, on its own trace: within root_tolerance."""
        samples = self.parked_samples()
        self.assertGreater(len(samples), 1000)
        for s in samples:
            reduced = (s.log_rhorad_Einstein, s.log_fm, s.log_T_Jordan)
            log_T, _, _ = SM.log_trace(self.cosmology, reduced)
            phi_root = self.model.minimum_on_trace(s.phi_Einstein, log_T)
            self.assertLessEqual(
                abs(log(phi_root) - log(s.phi_Einstein)),
                root_tolerance(s.phi_Einstein),
                s.raw_N,
            )

    def test_6_pi_on_every_parked_sample_is_pi_min(self):
        for s in self.parked_samples():
            reduced = (s.log_rhorad_Einstein, s.log_fm, s.log_T_Jordan)
            self.assertEqual(
                s.pi_Einstein,
                SM.tracker_velocity(
                    self.model, self.cosmology, s.phi_Einstein, reduced
                ),
                s.raw_N,
            )

    def test_3_pi_on_the_samples_against_a_finite_difference_of_phi(self):
        """
        The central difference of phi over the samples (spacing h = 0.00921 e-folds) differs from pi
        by h^2 pi''/6, with pi'' estimated as the largest |second difference of pi| over the five
        samples around (taken twice), plus the roots' error, 2 root_tolerance phi/(2h), plus 8 eps.
        A stencil across T_J = 10 keV is left out: there the equation of state's table ends, dw/d ln T
        jumps from -3.6e-8 to 0, and pi_min with it (log 06 §5).
        """
        samples = self.parked_samples()
        N = np.array([s.raw_N for s in samples])
        phi = np.array([s.phi_Einstein for s in samples])
        pis = np.array([s.pi_Einstein for s in samples])
        log_T = np.array([s.log_T_Jordan for s in samples])
        log_T_edge = log(EOS_TABLE_LOWEST_T_GEV * self.units.GeV)
        d2 = np.gradient(np.gradient(pis, N), N)
        checked = 0
        for j in range(2, len(samples) - 2):
            if (log_T[j - 2] - log_T_edge) * (log_T[j + 2] - log_T_edge) <= 0.0:
                continue
            h = 0.5 * (N[j + 1] - N[j - 1])
            fd = (phi[j + 1] - phi[j - 1]) / (N[j + 1] - N[j - 1])
            tolerance = (
                2.0 * h * h / 6.0 * np.max(np.abs(d2[j - 2 : j + 3]))
                + root_tolerance(phi[j]) * abs(phi[j]) / h
                + 8.0 * EPS * abs(pis[j])
            )
            self.assertLessEqual(abs(fd - pis[j]), tolerance, (N[j], fd, pis[j]))
            checked += 1
        self.assertGreater(checked, 1000)

    def test_6_a_parked_segment_is_deterministic(self):
        """
        Three parked segments, at their nodes and five interior points: forward, backward, shuffled
        scalar calls and one array call give the same floats (ruling 3).
        """
        rng = np.random.default_rng(6)
        for k in (self.k_switch, self.k_switch + 7, len(self.ts) - 2):
            segment = self.result.solution.interpolants[k]
            self.assertIsInstance(segment, SM.ParkedSegment)
            lo, hi = self.ts[k], self.ts[k + 1]
            points = [lo, hi] + list(np.linspace(lo, hi, 7)[1:-1])
            forward = [tuple(segment(N)) for N in points]
            backward = [tuple(segment(N)) for N in reversed(points)][::-1]
            order = rng.permutation(len(points))
            shuffled = {j: tuple(segment(points[j])) for j in order}
            array = segment(np.array(points))
            self.assertEqual(array.shape, (5, len(points)))
            for j in range(len(points)):
                self.assertEqual(forward[j], backward[j], (k, j))
                self.assertEqual(forward[j], shuffled[j], (k, j))
                self.assertEqual(forward[j], tuple(array[:, j]), (k, j))
            self.assertEqual(segment(lo).shape, (5,))

    def test_6_the_readers_read_the_parked_phase(self):
        """first_bounce before the switch; T_J = 1 MeV before it and 70 keV after it, on phi_min."""
        bounce = self.data["first_bounce"]
        self.assertIsNotNone(bounce)
        self.assertLess(bounce.N, self.parking.N)
        fixed = self.data["fixed_T_values"]
        for value in fixed:
            self.assertIsNotNone(value)
        N_70 = SM.T_Jordan_crossing(
            self.result, log(SM.FIXED_T_JORDAN_LOW_MEV * self.units.MeV)
        )
        N_1 = SM.T_Jordan_crossing(
            self.result, log(SM.FIXED_T_JORDAN_HIGH_MEV * self.units.MeV)
        )
        self.assertLess(N_1, self.parking.N)
        self.assertGreater(N_70, self.parking.N)
        y = self.result.solution(N_70)
        self.assertEqual(fixed.phi_Einstein_70keV, float(y[0]))
        log_T, _, _ = SM.log_trace(self.cosmology, y[2:])
        phi_root = self.model.minimum_on_trace(float(y[0]), log_T)
        self.assertLessEqual(abs(log(phi_root / y[0])), root_tolerance(y[0]))


# ---- (8) the window ----


class TestAWindowEndingInTheParkedPhase(unittest.TestCase):
    """
    (8), and (5) at a second M: M = 1e-7 M_P, beta = 2, alpha = 0.1, to N_stop = 30.9, after the
    switch at candidate 48, N = 30.349 (an unresolved reflection). About 4 s at a load of about 10.
    """

    @classmethod
    def setUpClass(cls):
        cls.result, cls.policy, cls.model = tut.window(
            2.0, 1e-7, ALPHA, REFERENCE_N_STOP
        )

    def test_8_N_stop_ends_it_successfully_in_the_parked_phase(self):
        r = self.result
        self.assertIsNotNone(r.parking)
        self.assertEqual(r.parking.candidate_index, 48)
        self.assertEqual(r.injections[48].kind, "unresolved")
        self.assertEqual(r.N_final, REFERENCE_N_STOP)
        self.assertEqual(r.solution.ts[-1], REFERENCE_N_STOP)
        self.assertGreater(r.parking.parked_steps, 0)
        self.assertIsInstance(r.solution.interpolants[-1], SM.ParkedSegment)
        # the final state is the tracker's at N_stop, on phi_min
        log_T, _, _ = SM.log_trace(self.policy.cosmology, tuple(r.final_state)[2:])
        phi_root = self.model.minimum_on_trace(r.final_state.phi_Einstein, log_T)
        self.assertLessEqual(
            abs(log(phi_root / r.final_state.phi_Einstein)),
            root_tolerance(r.final_state.phi_Einstein),
        )

    def test_5_H_is_continuous_at_the_switch_here_too(self):
        """As item (5)'s bound, at a state where V(phi_min) - V(phi) does not round to zero."""
        change, bound = H_change_and_bound(
            self.result, self.model, self.policy.units.PlanckMass
        )
        self.assertLessEqual(change, bound, (change, bound))
        self.assertLess(change, 1e-3 * self.result.parking.delta_rho_over_rho_rad)


# ---- (7) large M ----

# the oscillating history's tracker crosses m/H = 10 at N = 40.25469 at M = 0.5, beta = 5, and the
# parked one at 40.25454 (log 06 §5.2); the parked phase checks at its step ends, at most
# global_max_step apart, so the failure lies in (crossing - 1e-3, crossing + 1e-3 + 0.1]
BETA_5_CROSSING_N = 40.2547
BETA_5_CROSSING_SLACK = 1e-3


class TestAtHalfThePlanckMass(unittest.TestCase):
    """
    (7) M = 0.5 M_P, alpha = 0.1. beta = 2 with the cut on and disabled (about 3 + 3 s); beta = 5 to
    T_CMB (it is released and reaches T_CMB since prompt 07, about 6 s; at 06 it failed) and to
    N = 40.0 (after its switch, about 4 s); at a load of about 10.
    """

    @classmethod
    def setUpClass(cls):
        cls.data2, cls.result2, _ = history(2.0, 0.5, ALPHA)
        cls.never_data2, cls.never2, _ = history(2.0, 0.5, ALPHA, patches=(no_cut(),))
        cls.data5, _, _ = history(5.0, 0.5, ALPHA)
        cls.window5, cls.policy5, _ = tut.window(5.0, 0.5, ALPHA, 40.0)

    def test_7_no_parking_at_beta_2_and_the_history_is_the_cut_disabled_one(self):
        self.assertFalse(self.data2.get("failure", False), self.data2)
        a, b = self.result2, self.never2
        self.assertIsNone(a.parking)
        self.assertIsNone(self.data2["parking"])
        self.assertEqual(a.turning_points, b.turning_points)
        self.assertEqual(a.injections, b.injections)
        self.assertEqual(tuple(a.final_state), tuple(b.final_state))
        self.assertEqual(a.N_final, b.N_final)
        self.assertEqual(a.nfev, b.nfev)
        self.assertEqual(a.accepted_steps, b.accepted_steps)
        self.assertEqual(
            [tuple(s) for s in self.data2["sample"]],
            [tuple(s) for s in self.never_data2["sample"]],
        )
        self.assertEqual(
            self.data2["dark_radiation"].injections,
            self.never_data2["dark_radiation"].injections,
        )

    def test_7_at_beta_2_a_candidate_passes_the_ratio_test_but_not_the_tracker(self):
        """Orchestrator note 06 §0: the ratio passes at #27, where m/H = 3.1."""
        self.assertFalse(self.data2.get("failure", False), self.data2)
        passing = []
        for index, x in enumerate(self.result2.injections):
            y = self.result2.solution(x.N)
            if SM.parking_ratio_test(index, exp(x.log_rho_kin - y[2])):
                passing.append(index)
        self.assertIn(27, passing)

    def test_7_at_beta_5_it_parks_at_a_drain_point(self):
        p = self.window5.parking
        self.assertIsNotNone(p)
        self.assertEqual(
            self.window5.injections[p.candidate_index].kind, "turning_point"
        )
        self.assertGreaterEqual(p.m_eff_over_H, SM.PARKING_ADIABATICITY_THRESHOLD)
        self.assertEqual(self.window5.N_final, 40.0)

    def test_7_at_beta_5_it_is_released_where_the_tracker_loses_adiabaticity(self):
        """
        Prompt 07 §1.6 item 9: where 06 failed the history, the field is released, inside the window
        that 06's failure lay in, and the history succeeds.
        """
        data = self.data5
        self.assertFalse(data.get("failure", False), data)
        release = data["release"]
        self.assertIsNotNone(release)
        self.assertEqual(
            data["parking"],
            self.window5.parking._replace(
                parked_steps=data["parking"].parked_steps,
                min_m_eff_over_H=data["parking"].min_m_eff_over_H,
            ),
        )
        self.assertLess(release.m_eff_over_H_check, SM.PARKING_ADIABATICITY_THRESHOLD)
        for N in (release.N, release.N_check):
            self.assertGreater(N, BETA_5_CROSSING_N - BETA_5_CROSSING_SLACK)
            self.assertLessEqual(
                N,
                BETA_5_CROSSING_N
                + BETA_5_CROSSING_SLACK
                + SM.StepControl().global_max_step,
            )
        self.assertEqual(len(data["warnings"]), 1)


if __name__ == "__main__":
    unittest.main()
