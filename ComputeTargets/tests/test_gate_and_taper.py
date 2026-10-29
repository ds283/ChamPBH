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
The non-adiabaticity gate (inelastic-reflection prompt 03; README §2.2, gate "Taper").

At alpha > 0 every drain point and every floor reflection is a candidate, drained at
alpha_eff = alpha * w_gate, with w_gate = A_k^2/(A_k^2 + A_0^2), A_0 = 1, on the non-adiabaticity
A_k = max H_E |d ln m_eff^2/dN|/(2 m_eff) over the half-oscillation that ends at the candidate.
The prompt's stopping rule (w_rate) was withdrawn by the user (orchestrator note 03, ruling 16), and
with it the prompt's items 4-6. The items here are those of prompt §1.6 that remain, with orchestrator
note 03's additions:

  (1) alpha = 0 is the old code: on the golden beta = 1.5 history neither the mass function, the
      non-adiabaticity nor the gate is called, and the TurningPoints' new fields are None;
  (2) the gate against hand values (exact rationals), and its edges (ruling 8); the mass against a
      hand value on a stand-in with (ln A)'' != 0 (ruling 2);
  (3) A_k against a reference that recomputes it post hoc, at 33 points per step, with its own
      expression for m_eff^2 (ruling 2), under the same attribution rule (ruling 3);
  (7) the records on a real history;
  (8) a floor-rule candidate at M = 1e-10;
  (9) the near-stall at beta = 5: a wall-side turning point with A_k < 1e-2 is gated off.

Items (3) and (7) share one history, integrated once in setUpClass: M = 0.5 M_P, n = 1, beta = 2, at
the model's seed realization (untuned), phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB, atol = rtol
= 1e-8, main.py's axis, alpha = 0.1 (test_injection's setup). No Ray, no store, no PRyMordial solve.

"To rounding" is a number: each such assertion states its bound and where it comes from.
"""

import contextlib
import importlib
import sys
import unittest
from bisect import bisect_left, bisect_right
from fractions import Fraction
from math import exp, isfinite, log, sqrt
from unittest import mock

import numpy as np

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import test_injection as ti
from Quadrature.supervisors.ScalarField import StateVector

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1

# the functions of prompt 03 that the loop calls by their module names (orchestrator note 03,
# ruling 1)
PROMPT_03_FUNCTIONS = ("log_m_eff_squared", "step_nonadiabaticity", "gate_weight")


def exact_gate(A: float) -> Fraction:
    """A^2/(A^2 + A_0^2), in exact rational arithmetic on the two floats."""
    X, A0 = Fraction(A), Fraction(SM.NONADIABATICITY_SCALE)
    return X * X / (X * X + A0 * A0)


def w_gate_of(A: float) -> float:
    """The gate, by this file's own expression, A^2/(A^2 + 1), for the reference of item (3)."""
    return A * A / (A * A + 1.0) if A <= 1.0 else 1.0 / (1.0 + 1.0 / (A * A))


# ---- (2) the gate and the mass ----


class TestTheGate(unittest.TestCase):
    """(2) w_gate against hand values, in exact rationals, and at its edges. Milliseconds."""

    # A <= A_0 forms A*A, A*A + A_0*A_0 and the quotient, each rounded once, and the quotient's
    # condition in A*A is 1/(1 + A^2) <= 1: at most 1.5 EPS relative. Above A_0 it forms r = A_0/A
    # (EPS/2), r*r (1.5 EPS), 1 + r*r (r^2/(1 + r^2) <= 1/2 of that, plus one rounding: 1.25 EPS) and
    # 1/(...) (EPS/2): at most 1.75 EPS. So 2 EPS relative covers both, to first order (ruling 8)
    BOUND = 2.0 * EPS

    def test_2_gate_weight_against_hand_values(self):
        self.assertEqual(SM.NONADIABATICITY_SCALE, 1.0)
        for A in (0.0, 1e-3, 1.0, 10.0, 1e3, 1e200):
            w = SM.gate_weight(A)
            self.assertTrue(0.0 <= w <= 1.0, (A, w))
            exact = exact_gate(A)
            if exact == 0:
                self.assertEqual(w, 0.0)
            else:
                self.assertLessEqual(
                    abs(Fraction(w) - exact) / exact, Fraction(self.BOUND), (A, w)
                )
        # the values exact in binary, and the far edge, where A^2 would overflow
        self.assertEqual(SM.gate_weight(0.0), 0.0)
        self.assertEqual(SM.gate_weight(1.0), 0.5)
        self.assertEqual(SM.gate_weight(1e200), 1.0)
        self.assertEqual(SM.gate_weight(5e-324), 0.0)

    def test_2_the_gate_refuses_a_non_finite_or_negative_value(self):
        for bad in (-1e-3, float("nan"), float("inf")):
            with self.assertRaises(ComputationFailureError):
                SM.gate_weight(bad)


class TestTheMass(unittest.TestCase):
    """m_eff^2 against a hand value (orchestrator note 03, ruling 2). Milliseconds."""

    class StandIn:
        """
        log V = v1 phi + v2 phi^2 and ln A = beta phi + gamma phi^2 (M_P = 1): (ln A)'' = 2 gamma != 0,
        the term no exponential coupling exercises (ExponentialModel has (ln A)'' = 0).
        """

        name = "stand-in for test_gate_and_taper"

        def __init__(self, beta=2.0, gamma=1.5, v1=1.0, v2=0.0):
            self.beta, self.gamma, self.v1, self.v2 = beta, gamma, v1, v2

        def log_V(self, phi):
            return self.v1 * phi + self.v2 * phi * phi

        def d_logV_dphi(self, phi):
            return self.v1 + 2.0 * self.v2 * phi

        def d2_logV_dphi2(self, phi):
            return 2.0 * self.v2

        def d_logOmega_dphi(self, phi):
            return self.beta + 2.0 * self.gamma * phi

        def d2_logOmega_dphi2(self, phi):
            return 2.0 * self.gamma

    def test_2_the_mass_against_a_hand_value(self):
        """
        At phi = 0, rho_R,E = 1, Sigma = 1/4, f_m = 3/4: V'' = V [(ln V)'' + (ln V)'^2] = 1, and
        [(ln A)'' + (ln A)'^2] rho_R,E (Sigma + f_m) = (3 + 4)(1)(1) = 7, so m_eff^2 = 8. Without the
        (ln A)'' term it would be 5, and without the trace term (the breakage B10) 1. The bound is
        8 EPS on ln m_eff^2 (about 2.08): f_m = exp(ln 0.75) is within 1.5 EPS of 3/4, so the trace
        term's log is within about 2 EPS of ln 7; the sum relative to the larger term and its log add
        about 2 EPS.
        """
        model = self.StandIn()
        log_m2 = SM.log_m_eff_squared(model, 0.0, 0.0, log(0.75), 0.25)
        self.assertLessEqual(abs(log_m2 - log(8.0)), 8.0 * EPS, log_m2)

    def test_2_the_mass_at_a_generic_state(self):
        """
        At phi = 0.3, with log V quadratic, against the expression formed directly in floats. Each
        side is a few roundings of terms of order 1, and the logs and exps are of arguments below 3:
        32 EPS relative on m_eff^2.
        """
        model = self.StandIn(v2=0.4)
        phi, log_rho, log_fm, Sigma = 0.3, -2.0, -1.0, 0.1
        V = exp(model.log_V(phi))
        d1 = model.d_logV_dphi(phi)
        a1 = model.d_logOmega_dphi(phi)
        direct = V * (model.d2_logV_dphi2(phi) + d1 * d1) + (
            model.d2_logOmega_dphi2(phi) + a1 * a1
        ) * exp(log_rho) * (Sigma + exp(log_fm))
        m2 = exp(SM.log_m_eff_squared(model, phi, log_rho, log_fm, Sigma))
        self.assertLessEqual(abs(m2 / direct - 1.0), 32.0 * EPS)

    def test_2_a_non_positive_mass_is_a_failure(self):
        """A concave V with no coupling has m_eff^2 < 0; no V'' and no coupling has m_eff^2 = 0."""
        concave = self.StandIn(beta=0.0, gamma=0.0, v1=0.0, v2=-1.0)
        with self.assertRaises(ComputationFailureError):
            SM.log_m_eff_squared(concave, 0.0, 0.0, 0.0, 0.5)
        flat = self.StandIn(beta=0.0, gamma=0.0, v1=0.0, v2=0.0)
        with self.assertRaises(ComputationFailureError):
            SM.log_m_eff_squared(flat, 0.0, 0.0, 0.0, 0.5)


# ---- (1) alpha = 0 ----


class TestAlphaZeroIsTheOldCode(unittest.TestCase):
    """
    (1) The golden beta = 1.5 history at alpha = 0, with every function of prompt 03 patched (by a
    wrapper, so that the history would still run if one were called): none is called. One
    integration, about 1.5 s at a load of 6. test_golden_history.py, the tuned golden test and
    test_first_bounce.py are the bitwise check, unedited.
    """

    def test_1_nothing_of_prompt_03_runs_at_alpha_zero(self):
        captured = []
        integrate = SM.integrate_scalar_history

        def capturing(*args, **kwargs):
            result = integrate(*args, **kwargs)
            captured.append(result)
            return result

        with contextlib.ExitStack() as stack:
            mocks = {
                name: stack.enter_context(
                    mock.patch.object(SM, name, wraps=getattr(SM, name))
                )
                for name in PROMPT_03_FUNCTIONS
            }
            stack.enter_context(
                mock.patch.object(SM, "integrate_scalar_history", capturing)
            )
            data = gh.run_history(1.5)
        result = captured[-1]

        for name, m in mocks.items():
            self.assertEqual(m.call_count, 0, name)
        self.assertGreaterEqual(len(result.turning_points), 2)
        for t in result.turning_points:
            self.assertIsNone(t.nonadiabaticity, t)
            self.assertIsNone(t.m_eff_over_H, t)
        self.assertEqual(result.injections, [])
        self.assertTrue(data["dark_radiation"].is_empty)
        self.assertIsInstance(result.dropped_drains, int)
        self.assertEqual(data["dropped_drains"], result.dropped_drains)


# ---- (3) and (7): a real history ----


def reference_nonadiabaticity(result, data, policy, samples_per_step: int = 33):
    """
    A_k recomputed post hoc (prompt §1.6 item 3), for every turning point and floor reflection of
    `result`, as {N_k: A_k}. Per accepted step, `samples_per_step` points, ends included:
      - m_eff^2 by this function's own expression (orchestrator note 03, ruling 2), never by
        SM.log_m_eff_squared: V''(phi) = V [(ln V)'' + (ln V)'^2] from log_V, d_logV_dphi and
        d2_logV_dphi2, plus [(ln A)'' + (ln A)'^2] rho_R,E (Sigma + f_m), with Sigma = 1 - 3 w(T_J)
        from the cosmology;
      - H_E from HubblePolicy with the ledger as the loop saw it on that step: the drains at or
        before the step's start (a drain is made at a step end, after that step's samples);
      - A = H_E |d ln m_eff^2/dN|/(2 m_eff), the derivative np.gradient over the step's own
        samples, in N;
      - the attribution of ruling 3: a sample belongs to the half-oscillation (N_{k-1}, N_k] that
        contains its N; the first begins at N_start. A half-oscillation with no sample has 0.0.
    It differs from the loop only in the number of samples.
    """
    model = policy.model
    cosmology = policy.cosmology
    ledger = data["dark_radiation"]
    hubble = SM.HubblePolicy(model, policy.units)
    ts = result.solution.ts
    interpolants = result.solution.interpolants

    ends = sorted(
        [t.N for t in result.turning_points] + [r.N for r in result.reflections]
    )
    boundaries = [float(ts[0])] + ends
    A_k = [None] * len(boundaries)

    drain_Ns = [N for N, _ in ledger.injections]
    for k, interpolant in enumerate(interpolants):
        if ts[k] > boundaries[-1]:
            break
        view = ti.ledger_before(ledger, bisect_right(drain_Ns, ts[k]))
        view_policy = SM.ODEPolicy("reference", cosmology, model, dark_radiation=view)
        Ns = np.linspace(ts[k], ts[k + 1], samples_per_step)
        Y = interpolant(Ns)
        m2 = np.empty(samples_per_step)
        H = np.empty(samples_per_step)
        for j in range(samples_per_step):
            state = StateVector._make(Y[:, j])
            phi = state.phi_Einstein
            d1 = model.d_logV_dphi(phi)
            Vpp = exp(model.log_V(phi)) * (model.d2_logV_dphi2(phi) + d1 * d1)
            a1 = model.d_logOmega_dphi(phi)
            Sigma = 1.0 - 3.0 * float(cosmology.w(exp(state.log_T_Jordan)))
            trace = exp(state.log_rhorad_Einstein) * (Sigma + exp(state.log_fm))
            m2[j] = Vpp + (model.d2_logOmega_dphi2(phi) + a1 * a1) * trace
            H[j] = hubble(view_policy(Ns[j], state), state).H_Einstein
        A = H * np.abs(np.gradient(np.log(m2), Ns)) / (2.0 * np.sqrt(m2))
        for N, a in zip(Ns, A):
            i = bisect_left(boundaries, N)
            if i == 0 or i >= len(boundaries):
                continue
            A_k[i] = float(a) if A_k[i] is None else max(A_k[i], float(a))
    return {N: (0.0 if a is None else a) for N, a in zip(boundaries[1:], A_k[1:])}


class TestARealHistoryAtAlpha(unittest.TestCase):
    """
    (3) and (7) on the beta = 2, M = 0.5 history at alpha = 0.1 (module docstring), integrated once:
    about 5 s with the reference of (3), at a load of 12.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, cls.recorder = ti.integrate_captured(
            2.0, 0.5, ALPHA, record_rhs=True
        )
        cls.policy = cls.recorder.policy
        cls.ledger = cls.data["dark_radiation"]

    # ---- (3) A_k against the reference ----

    def test_3_the_nonadiabaticity_against_a_reference(self):
        """
        At every wall-side turning point the loop's w_gate (10 samples per step) agrees with the
        reference's (33 per step) to 0.05 absolute; since dw/d ln A <= 1/2, that is a 10% tolerance
        on A where the gate is sensitive. Plateau turnarounds are checked the same way. Both gates
        are this file's expression. The largest |dw_gate| and the largest relative difference in
        A_k, at each kind, are written to stderr.

        Each turning point's m_eff/H_E is also checked against the reference's own expression at
        the turning point's state, sqrt(V'' + [(ln A)'' + (ln A)'^2] rho_R,E (Sigma + f_m))/H_E, to
        EPS (|ln V| + |ln rho_R,E| + |ln H_E| + 16): each side exponentiates logs of those sizes
        once or twice, which carries their absolute rounding into a relative one, and adds a few
        roundings. It measured 1.4e-14. The gate's 0.05 alone does not see a wrong trace term (the
        breakage B10): where it matters, on the plateau side, A_k is far above A_0 either way and
        both gates are 1; m_eff/H_E at a plateau turnaround, where the trace term is up to 1e39
        times V'', does.
        """
        reference = reference_nonadiabaticity(self.result, self.data, self.policy)
        model = self.policy.model
        cosmology = self.policy.cosmology
        hubble = SM.HubblePolicy(model, self.policy.units)
        worst = {"wall": (0.0, 0.0), "plateau": (0.0, 0.0)}
        for t in self.result.turning_points:
            A_ref = reference[t.N]
            self.assertIsNotNone(t.nonadiabaticity, t)
            dw = abs(w_gate_of(t.nonadiabaticity) - w_gate_of(A_ref))
            self.assertLessEqual(dw, 0.05, (t, A_ref))

            state = StateVector._make(self.result.solution(t.N))
            phi = state.phi_Einstein
            d1 = model.d_logV_dphi(phi)
            a1 = model.d_logOmega_dphi(phi)
            Sigma = 1.0 - 3.0 * float(cosmology.w(exp(state.log_T_Jordan)))
            m2 = exp(model.log_V(phi)) * (model.d2_logV_dphi2(phi) + d1 * d1) + (
                model.d2_logOmega_dphi2(phi) + a1 * a1
            ) * exp(state.log_rhorad_Einstein) * (Sigma + exp(state.log_fm))
            H = hubble(self.policy(t.N, state), state).H_Einstein
            bound = (
                abs(model.log_V(phi))
                + abs(state.log_rhorad_Einstein)
                + abs(log(H))
                + 16.0
            ) * EPS
            self.assertLessEqual(abs(t.m_eff_over_H / (sqrt(m2) / H) - 1.0), bound, t)
            rel = abs(t.nonadiabaticity - A_ref) / A_ref
            dw_worst, rel_worst = worst[t.kind]
            worst[t.kind] = (max(dw_worst, dw), max(rel_worst, rel))
        for kind, (dw, rel) in worst.items():
            sys.stderr.write(
                f"\n  test_3: {kind}: largest |dw_gate| {dw:.3e}, largest relative "
                f"difference in A_k {rel:.3e}\n"
            )

    # ---- (7) the records ----

    def test_7_alpha_eff_is_alpha_times_the_gate(self):
        """
        Every candidate: alpha_eff == alpha * w_gate exactly, as floats; w_gate == gate_weight(A_k);
        A_k is its wall-side turning point's, exactly. Every turning point carries A_k and m_eff/H_E.
        """
        walls = {t.N: t for t in self.result.turning_points if t.kind == "wall"}
        for t in self.result.turning_points:
            self.assertIsNotNone(t.nonadiabaticity, t)
            self.assertIsNotNone(t.m_eff_over_H, t)
            self.assertGreater(t.m_eff_over_H, 0.0, t)
        injections = self.result.injections
        self.assertGreaterEqual(len(injections), 10)
        for i in injections:
            self.assertEqual(i.kind, "turning_point")
            self.assertEqual(i.alpha_eff, ALPHA * i.w_gate, i)
            self.assertEqual(i.w_gate, SM.gate_weight(i.nonadiabaticity), i)
            self.assertEqual(i.nonadiabaticity, walls[i.N_turn].nonadiabaticity, i)

    def test_7_delta_rho_over_rho_rad_from_the_record(self):
        """
        delta_rho/rho_R against alpha_eff rho_kin,E/rho_R,E recomputed from the record by products,
        alpha_eff H_before^2 pi_before^2/(2 rho_R,E), with rho_R,E = exp(ln rho_R,E) read from the
        dense output at the candidate (a drain changes pi only). The loop forms it as
        exp(ln alpha_eff + ln rho_kin,E - ln rho_R,E), with ln rho_kin,E = 2 ln H + 2 ln|pi| - ln 2,
        so the bound is EPS times the sum of the magnitudes of the logs and their partial sums, plus
        8 for the products, the exps and ln 2 (test_injection.delta_K_bound's argument, with
        ln rho_R,E in place of 4N).
        """
        for i in self.result.injections:
            self.assertGreater(i.alpha_eff, 0.0, i)
            log_rho_rad = float(self.result.solution(i.N)[2])
            direct = (
                i.alpha_eff
                * 0.5
                * i.H_Einstein_before**2
                * i.pi_before**2
                / exp(log_rho_rad)
            )
            bound = (
                2.0 * abs(log(i.H_Einstein_before))
                + 2.0 * abs(log(abs(i.pi_before)))
                + abs(i.log_rho_kin)
                + abs(log(i.alpha_eff))
                + abs(log_rho_rad)
                + abs(log(i.delta_rho_over_rho_rad))
                + 8.0
            ) * EPS
            self.assertLessEqual(abs(i.delta_rho_over_rho_rad / direct - 1.0), bound, i)

    def test_7_the_ledger_holds_exactly_the_drains_made(self):
        """
        The ledger's injections are the records with alpha_eff > 0.0, as the same floats; a record
        with alpha_eff == 0.0 (none on this history) would have delta_K = delta_rho/rho_R = 0.0,
        pi_after == pi_before and H_after == H_before. dropped_drains is the wall-side turning
        points less the "turning_point" candidates, which is also the count of wall-side turning
        points that no record names.
        """
        injections = self.result.injections
        self.assertEqual(
            self.ledger.injections,
            tuple((i.N, i.delta_K) for i in injections if i.alpha_eff > 0.0),
        )
        for i in injections:
            if i.alpha_eff == 0.0:
                self.assertEqual(i.delta_K, 0.0)
                self.assertEqual(i.delta_rho_over_rho_rad, 0.0)
                self.assertEqual(i.pi_after, i.pi_before)
                self.assertEqual(i.H_Einstein_after, i.H_Einstein_before)
        walls = [t for t in self.result.turning_points if t.kind == "wall"]
        named = {i.N_turn for i in injections if i.kind == "turning_point"}
        candidates = sum(1 for i in injections if i.kind == "turning_point")
        self.assertEqual(self.result.dropped_drains, len(walls) - candidates)
        self.assertEqual(
            self.result.dropped_drains, sum(1 for t in walls if t.N not in named)
        )
        self.assertEqual(self.data["dropped_drains"], self.result.dropped_drains)


# ---- (8) a floor-rule candidate ----


class TestAFloorRuleCandidate(unittest.TestCase):
    """
    (8) test_injection's P1 harness at M = 1e-10, with a ledger, at alpha = 0.1. About 0.3 s. Since
    inelastic-reflection prompt 05 (README §8.2 P20) a floor reflection is unresolved: it is not
    gated, and drains with w_gate = 1.0 exactly, its A_k still recorded (log 05 lists the edit).
    """

    def test_8_the_floor_reflection_is_an_unresolved_candidate(self):
        """
        The one floor reflection is a candidate whose record carries A_k (the infall's, up to the
        reflection; a lower bound, since the wall is thinner than a step) and w_gate = 1.0, with
        resolved False; alpha_eff = alpha; the ledger holds it if and only if alpha_eff > 0; and
        pi_out = -sqrt(1 - alpha_eff) pi_in exactly. The A_k and w_gate it gets are written to
        stderr.
        """
        result, policy, ledger = ti.integrate_floor(ALPHA)
        self.assertEqual(len(result.reflections), 1)
        floors = [i for i in result.injections if i.kind == "floor"]
        self.assertEqual(len(floors), 1)
        i = floors[0]
        reflection = result.reflections[0]
        self.assertEqual(reflection.kind, "floor")
        self.assertEqual(i.N, reflection.N)
        for value in (i.nonadiabaticity, i.w_gate):
            self.assertTrue(value is not None and isfinite(value), i)
        self.assertFalse(i.resolved)
        self.assertEqual(i.w_gate, 1.0)
        self.assertEqual(i.alpha_eff, ALPHA)
        self.assertEqual(
            i.pi_after, -(sqrt(1.0 - i.alpha_eff) * reflection.pi_Einstein_in)
        )
        self.assertEqual(result.solution(i.N)[1], i.pi_after)
        self.assertEqual(
            ledger.injections,
            tuple((x.N, x.delta_K) for x in result.injections if x.alpha_eff > 0.0),
        )
        sys.stderr.write(
            f"\n  test_8: floor A_k = {i.nonadiabaticity:.4e}, w_gate = {i.w_gate:.4e}, "
            f"alpha_eff = {i.alpha_eff:.4e}\n"
        )


# ---- (9) the near-stall ----


class TestTheNearStallIsGated(unittest.TestCase):
    """
    (9) M = 0.5 M_P, beta = 5, alpha = 0.1 (the module's setup otherwise). One integration, about
    6 s at a load of 13.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, _ = ti.integrate_captured(5.0, 0.5, ALPHA)

    def test_9_a_wall_side_turning_point_with_small_A_k_is_gated_off(self):
        """
        Every candidate after a wall-side turning point with A_k < 1e-2 has w_gate < 1e-4, and there
        is at least one. Under the gate this history holds no interior plateau-and-wall pair: the
        near-stall pair prompt 02 found here (N = 42.889, 42.893) is gone, because the gated drains
        change the trajectory (log 03 §2). test_injection's item (11) has moved to beta = 10, where
        the pair's wall point has A_k = 2.1e-5.
        """
        small = [
            i
            for i in self.result.injections
            if i.kind == "turning_point" and i.nonadiabaticity < 1e-2
        ]
        self.assertGreaterEqual(len(small), 1)
        for i in small:
            self.assertLess(i.w_gate, 1e-4, i)


if __name__ == "__main__":
    unittest.main()
