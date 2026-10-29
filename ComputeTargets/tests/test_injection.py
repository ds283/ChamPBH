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
The injection at turning points (inelastic-reflection prompt 02; README §2.2, gate "Inject").

At alpha > 0, integrate_scalar_history drains a fraction alpha of the condensate's kinetic energy
rho_kin,E = H_E^2 pi^2/2 into the dark-radiation ledger at the drain point that follows each
wall-side turning point of phi (pi from - to +), and at each floor-rule reflection, and restarts
Radau from the drained state. The items are those of prompt §1.5:

  (1) alpha = 0 is the old code: on the golden beta = 1.5 history the ledger and the injections are
      empty, and the first wall-side TurningPoint is first_bounce's, field by field.
  (2) the classification of the turning points, and one injection per wall-side turning point;
  (3) the drain point: pi > 0 and pi' <= 0 there, and at no step end before it after the turn;
  (4) H_E continuous across every injection, to a stated rounding bound;
  (5) the ledger is the sum of the drains; Delta K and pi_after against a recomputation;
  (6) the dense output at a drain node is the drained state (right-continuous);
  (7) every RHS evaluation of the new Radau instance sees the new K;
  (8) a floor-rule drain at M = 1e-10, and the same run at alpha = 0 is today's;
  (9) the parameter: its range, the ledger it needs, and tune_history raising before the seed;
  (10) the payload at alpha = 0.1 pickles;
  (11) a plateau and wall-side pair inside one step is found by the interior sampling of pi
       (orchestrator note 02, ruling 15), and no step shows more sign changes at 17 points than
       the loop recorded on it. Since inelastic-reflection prompt 03 gated the drains, the history
       is beta = 10 (log 03 §2).

Since prompt 03 a drain's fraction is alpha_eff = alpha * w_gate, not alpha: items (2), (5) and (8)
check alpha_eff (log 03 §2 lists the edits).

Items (2)-(7) and (10) share one history, integrated once in setUpClass: M = 0.5 M_P, n = 1,
beta = 2, at the model's seed realization (untuned), phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to
T_CMB, atol = rtol = 1e-8, main.py's axis, alpha = 0.1. It is the planner's measurement setup
(prompt §0), and about 2 s at a load of 12. No Ray, no store, no PRyMordial solve.

"To rounding" is a number (orchestrator note 01 ruling 8, note 02 ruling 8): each such assertion
states its bound, and the docstring says why it is rounding.
"""

import contextlib
import importlib
import io
import pickle
import sys
import unittest
from bisect import bisect_right
from math import exp, isnan, log, sqrt
from unittest import mock

import numpy as np
from scipy.integrate import OdeSolution

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import (
    ScalarFieldIntegrationSupervisor,
    StateVector,
)

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1


class RecordingRHS:
    """
    Wraps an ODERHS and records, for every evaluation in call order, (N, pi, K(N)): K read from the
    ledger the policy holds at the moment of the call, which is what the policy reads in that call.
    It carries the wrapped RHS's policy, so integrate_scalar_history's check that the RHS and the
    drain share one ledger (orchestrator note 02, ruling 5) sees the real one.
    """

    def __init__(self, rhs):
        self.rhs = rhs
        self.policy = rhs.policy
        self.calls = []

    def __call__(self, N, s, supervisor):
        ledger = self.policy.dark_radiation
        self.calls.append((float(N), float(s[1]), ledger.K(N)))
        return self.rhs(N, s, supervisor)


def integrate_captured(beta: float, M: float, alpha: float, record_rhs: bool = False):
    """
    integrate_history at the planner's setup (module docstring), with the IntegrationResult
    captured by wrapping integrate_scalar_history, as test_fixed_T_values.full_history does.
    If record_rhs, the RHS is wrapped in a RecordingRHS. Returns (data, result, recorder or None).
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    Mp = units.PlanckMass
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)

    captured = []
    recorders = []
    integrate = SM.integrate_scalar_history

    def capturing(RHS, *args, **kwargs):
        if record_rhs:
            RHS = RecordingRHS(RHS)
            recorders.append(RHS)
        result = integrate(RHS, *args, **kwargs)
        captured.append(result)
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, 2.0e4 * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, 5.0 * Mp),
            pi_value(0, 0.0),
            gth.make_z_grid(),
            realized,
            task_label="test_injection",
            alpha=alpha,
        )
    if data.get("failure", False):
        raise RuntimeError(f"test_injection: history failed: {data['failure_reason']}")
    return data, captured[-1], (recorders[-1] if record_rhs else None)


def pi_prime(policy, N: float, y) -> float:
    """pi' = dpi/dN at (N, y), as ODERHS computes it: friction + reflecting + kicking."""
    data = policy(N, StateVector._make(y))
    return data.friction_term + data.reflecting_term + data.kicking_term


def ledger_before(ledger: DarkRadiationLedger, j: int) -> DarkRadiationLedger:
    """The ledger as it stood before its j-th injection: K_0 and the first j injections, replayed."""
    replay = DarkRadiationLedger(ledger.K_initial)
    for N, dK in ledger.injections[:j]:
        replay.add(N, dK)
    return replay


def H_Einstein(policy, N: float, y) -> float:
    state = StateVector._make(y)
    return SM.HubblePolicy(policy.model, policy.units)(
        policy(N, state), state
    ).H_Einstein


def delta_K_bound(i) -> float:
    """
    The rounding bound on |Delta K/direct - 1|, with direct = alpha H_before^2 pi_before^2 e^{4N}/2
    computed here by products (about 4 EPS). The loop computes Delta K = exp(ln alpha + ln rho_kin
    + 4N), with ln rho_kin = 2 ln H + 2 ln|pi| - ln 2: each log carries an absolute rounding of
    EPS/2 times its size, doubled by the factor 2, each of the four sums EPS/2 times its partial
    result, and exp turns the absolute error of its argument into a relative one. So the bound is
    EPS times the sum of the magnitudes of the logs and partial sums, plus 8 for the products,
    the exps and the ln 2 (log 02 §1.2).
    """
    log_H, log_pi = log(i.H_Einstein_before), log(abs(i.pi_before))
    return (
        2.0 * abs(log_H)
        + 2.0 * abs(log_pi)
        + abs(i.log_rho_kin)
        + abs(log(i.alpha_eff))
        + 4.0 * abs(i.N)
        + abs(log(i.delta_K))
        + 8.0
    ) * EPS


def H_bound(i, policy, y_after):
    """
    The rounding bound on |H_after - H_before|/H_before at one injection (prompt §1.5 item 4;
    orchestrator note 02, ruling 8), derived in log 02 §1.2. `policy` holds the ledger with the
    drain in it, and y_after is the drained state. Returns (bound, c).

    In exact arithmetic H_E is unchanged: the constraint 3 M_P^2 H^2 G = V + rho_R (1 + f_m) +
    rho_chi holds at the same H for (pi, rho_chi) and (sqrt(1 - alpha) pi, rho_chi + alpha
    rho_kin). HubblePolicy rebuilds H^2 = exp(x)/3 M_P^2, with x = ln V - ln(G T) and
    T = 1/(1 + (rho_R/V)(1 + f_m + r_chi)). ln V, rho_R/V and f_m are the same floats before and
    after (phi and ln rho_R,E are not touched), so the two H differ only through G, through r_chi
    and through the roundings after them. On H^2, between the two:
      - G = 1 - pi^2/6 M_P^2. pi_after carries 1.25 EPS from sqrt(1 - alpha) and the product, its
        square and the division EPS more, and 1 - s costs EPS/2 of G. With
        c = (pi_before^2/6 M_P^2)/G_before, G differs from its exact ratio by at most EPS (4.5 c + 1);
      - T: its sum, product and division, 4 EPS; and r_chi's relative error, EPS (|ln K| + 4|N| +
        |ln rho_R,E| + 2) from exp(ln K - 4N - ln rho_R,E), times its weight in T,
        omega = rho_chi/(3 M_P^2 H^2 G) <= 1;
      - the drain's own Delta K enters with weight alpha rho_kin/(3 M_P^2 H^2 G) = alpha c, and is
        wrong by delta_K_bound plus twice H_before's own error, EPS |x|;
      - log(G T): EPS |ln(G T)| over the two. x is rounded to its float each time, half an ulp of
        x, at most EPS |x|/2, so EPS |x| between the two.
    H is half of H^2, and exp, the division by 3 M_P^2 and sqrt add 2 EPS to H. The factor (1 + c)
    is the condition of G (note 02, ruling 8). The term EPS |x|/2, an ulp of the exponent, is not of
    that form and dominates: |x| is about 180 at M_P = 1.
    """
    Mp = policy.units.PlanckMass
    s = i.pi_before * i.pi_before / (6.0 * Mp * Mp)
    c = s / (1.0 - s)
    H = i.H_Einstein_before
    x = log(3.0 * Mp * Mp * H * H)

    state = StateVector._make(y_after)
    data = policy(i.N, state)
    G_after = 1.0 - state.pi_Einstein * state.pi_Einstein / (6.0 * Mp * Mp)
    omega = (
        data.rho_chi_over_rho_rad
        * exp(state.log_rhorad_Einstein)
        / (3.0 * Mp * Mp * H * H * G_after)
    )
    K = policy.dark_radiation.K(i.N)
    log_K = log(K) if K > 0.0 else 0.0
    r_chi_error = abs(log_K) + 4.0 * abs(i.N) + abs(state.log_rhorad_Einstein) + 2.0
    on_H2 = (
        (4.5 * c + 1.0)
        + 4.0
        + omega * r_chi_error
        + i.alpha_eff * c * (delta_K_bound(i) / EPS + abs(x))
        + abs(log(data.V_over_3H2Mp2))
        + abs(x)
    )
    return (0.5 * on_H2 + 2.0) * EPS, c


class TestAlphaZeroIsTheOldCode(unittest.TestCase):
    """(1) The golden beta = 1.5 history at alpha = 0. One integration, about 1 s at a load of 12."""

    def test_1_no_injection_and_the_first_wall_turning_point_is_first_bounce(self):
        captured = []
        integrate = SM.integrate_scalar_history

        def capturing(*args, **kwargs):
            result = integrate(*args, **kwargs)
            captured.append(result)
            return result

        with mock.patch.object(SM, "integrate_scalar_history", capturing):
            data = gh.run_history(1.5)
        result = captured[-1]

        self.assertTrue(data["dark_radiation"].is_empty)
        self.assertEqual(result.injections, [])
        self.assertEqual(data["injections"], [])
        self.assertEqual(data["turning_points"], result.turning_points)
        self.assertEqual(len(result.reflections), 0)

        walls = [t for t in result.turning_points if t.kind == "wall"]
        self.assertGreaterEqual(len(walls), 1)
        bounce = SM.first_bounce(result)
        self.assertIsNotNone(bounce)
        self.assertFalse(bounce.reflected)
        self.assertEqual(walls[0].N, bounce.N)
        self.assertEqual(walls[0].phi_Einstein, bounce.phi_Einstein)
        self.assertEqual(walls[0].log_T_Jordan, bounce.log_T_Jordan)


class TestARealHistoryAtAlpha(unittest.TestCase):
    """
    (2)-(7) and (10) on the beta = 2, M = 0.5 history at alpha = 0.1 (module docstring), integrated
    once with its RHS recorded: about 2-3 s at a load of 12.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, cls.recorder = integrate_captured(
            2.0, 0.5, ALPHA, record_rhs=True
        )
        cls.policy = cls.recorder.policy
        cls.ledger = cls.data["dark_radiation"]
        cls.Mp = cls.policy.units.PlanckMass
        cls.ts = list(cls.result.solution.ts)
        cls.interpolants = cls.result.solution.interpolants

    def step_of(self, N: float) -> int:
        """The index k of the accepted step with ts[k] <= N < ts[k + 1] (the last if N = N_final)."""
        return min(bisect_right(self.ts, N) - 1, len(self.interpolants) - 1)

    def node(self, N: float) -> int:
        """The index of N in ts; N must be a node."""
        k = self.ts.index(N)
        return k

    # ---- (2) the classification ----

    def test_2_the_turning_points_are_classified(self):
        """
        Each wall-side turning point has pi(t_old) < 0 < pi(t) on its step's interpolant, and each
        plateau one the reverse; the kinds alternate.
        """
        tps = self.result.turning_points
        self.assertGreaterEqual(len([t for t in tps if t.kind == "wall"]), 10)
        for t in tps:
            k = self.step_of(t.N)
            a, b = self.ts[k], self.ts[k + 1]
            self.assertLess(a, t.N)
            self.assertLess(t.N, b)
            pa, pb = self.interpolants[k](a)[1], self.interpolants[k](b)[1]
            if t.kind == "wall":
                self.assertTrue(pa < 0.0 < pb, t)
            else:
                self.assertEqual(t.kind, "plateau")
                self.assertTrue(pa > 0.0 > pb, t)
        for t0, t1 in zip(tps, tps[1:]):
            self.assertNotEqual(t0.kind, t1.kind)
            self.assertLess(t0.N, t1.N)

    def test_2_one_injection_per_wall_side_turning_point_and_none_after_a_plateau(self):
        """
        Each wall-side turning point is followed by exactly one injection of kind "turning_point",
        before the next turning point (the history's last turning point, if wall-side, by at most
        one); every injection follows a wall-side turning point, at alpha_eff = alpha * w_gate
        (inelastic-reflection prompt 03).
        """
        tps = self.result.turning_points
        injections = self.result.injections
        self.assertGreaterEqual(len(injections), 10)
        for i in injections:
            self.assertEqual(i.kind, "turning_point")
            self.assertEqual(i.alpha_eff, ALPHA * i.w_gate)

        walls = {t.N: t for t in tps if t.kind == "wall"}
        for i in injections:
            self.assertIn(i.N_turn, walls)
            self.assertEqual(i.phi_turn, walls[i.N_turn].phi_Einstein)

        for n, t in enumerate(tps):
            N_next = tps[n + 1].N if n + 1 < len(tps) else float("inf")
            after = [i for i in injections if t.N < i.N < N_next]
            if t.kind == "plateau":
                self.assertEqual(after, [], t)
            elif n + 1 < len(tps):
                self.assertEqual(len(after), 1, t)
                self.assertEqual(after[0].N_turn, t.N)
            else:
                self.assertLessEqual(len(after), 1, t)

    # ---- (3) the drain point ----

    def test_3_the_drain_point_is_the_first_step_end_past_the_maximum(self):
        """
        At each drain node pi > 0 and pi' <= 0; at every step end from the turning point's own
        step's up to the drain node (exclusive), pi > 0 and pi' > 0, so in particular at the end of
        the step before the drain step, unless that step is the turning point's own. pi' is
        ODERHS's (friction + reflecting + kicking) at the accepted state, rebuilt exactly (at the
        drain node, the drained state with pi_before put back; before it, the start of the next
        interpolant), with the ledger as it stood then: the loop's solver.f[1] bitwise. Under the
        breakage B4 (a drain at the end of the turning point's own step, whatever pi') pi' > 0 at
        some drain node. Microseconds per point, on setUpClass's history.
        """
        own_step_drains = 0
        for j, i in enumerate(self.result.injections):
            policy = SM.ODEPolicy(
                "t",
                self.policy.cosmology,
                self.policy.model,
                ledger_before(self.ledger, j),
            )
            k_drain = self.node(i.N)
            y = list(self.result.solution(i.N))
            y[1] = i.pi_before
            self.assertGreater(i.pi_before, 0.0)
            self.assertLessEqual(pi_prime(policy, i.N, y), 0.0, (j, i))

            k_turn = self.step_of(i.N_turn)
            self.assertLessEqual(k_turn + 1, k_drain)
            if k_drain == k_turn + 1:
                own_step_drains += 1
            for m in range(k_turn + 1, k_drain):
                # the end of step m - 1 is the start of interpolant m: the accepted state, exactly
                y_m = self.interpolants[m](self.ts[m])
                self.assertGreater(y_m[1], 0.0, (j, m, i))
                self.assertGreater(pi_prime(policy, self.ts[m], y_m), 0.0, (j, m, i))
        # the check before the drain step is exercised: most drains are not on the turn's own step
        self.assertLess(own_step_drains, len(self.result.injections))

    # ---- (4) H_E continuous ----

    def test_4_H_Einstein_is_continuous_across_every_injection(self):
        """
        |H_after - H_before|/H_before <= H_bound (its docstring derives it; log 02 §1.2) at every
        injection, and the largest ratio, with the c and the bound at it, is written to stderr
        (orchestrator note 02, ruling 8a). Under prompt 02's drains every ratio on this history
        measured 0.0, at c up to 0.043 against bounds of 99-120 EPS; the one nonzero ratio of log
        02's measurement is at beta = 25, 1.414e-14 (one ulp of the exponent, halved) at c = 9.7e-8
        against 97 EPS. Under prompt 03's gate one ratio here is 1.43e-14, against a bound of
        115 EPS (log 03 §7).
        Microseconds per injection.
        """
        worst = None
        for i in self.result.injections:
            bound, c = H_bound(i, self.policy, self.result.solution(i.N))
            ratio = abs(i.H_Einstein_after - i.H_Einstein_before) / i.H_Einstein_before
            self.assertLessEqual(ratio, bound, (i, c))
            if worst is None or ratio / bound > worst[0] / worst[2]:
                worst = (ratio, c, bound)
        sys.stderr.write(
            f"\n  test_4: largest |dH/H| = {worst[0]:.3e} at c = {worst[1]:.3e}, "
            f"bound {worst[2]:.3e} ({worst[2] / EPS:.0f} EPS)\n"
        )

    # ---- (5) the ledger is the sum of the drains ----

    def test_5_the_ledger_is_the_sum_of_the_drains(self):
        """
        ledger.injections equals [(N, delta_K)] exactly, the same floats; pi_after is
        sqrt(1 - alpha_eff) pi_before exactly, computed the same way; and each delta_K is
        alpha_eff H_before^2 pi_before^2 e^{4N}/2, computed here by products, to delta_K_bound.
        Microseconds.
        """
        self.assertEqual(
            self.ledger.injections,
            tuple((i.N, i.delta_K) for i in self.result.injections),
        )
        for i in self.result.injections:
            self.assertEqual(i.pi_after, sqrt(1.0 - i.alpha_eff) * i.pi_before)
            direct = (
                i.alpha_eff
                * 0.5
                * i.H_Einstein_before**2
                * i.pi_before**2
                * exp(4.0 * i.N)
            )
            self.assertLessEqual(abs(i.delta_K / direct - 1.0), delta_K_bound(i), i)

    # ---- (6) the dense output at a drain node ----

    def test_6_the_dense_output_at_a_drain_node_is_the_drained_state(self):
        """
        result.solution(N_j) is the drained state bitwise, pi included: with alt_segment=True it
        is interpolant k + 1 at its start, which is the state the new Radau instance started from.
        H_E rebuilt there, with the ledger, is H_Einstein_after exactly. SciPy's default
        (alt_segment=False) would return interpolant k at its end: the state before the drain.
        """
        default = OdeSolution(self.result.solution.ts, self.interpolants)
        for i in self.result.injections:
            k = self.node(i.N)
            y = self.result.solution(i.N)
            self.assertEqual(y[1], i.pi_after)
            self.assertTrue((y == self.interpolants[k](i.N)).all())
            self.assertEqual(H_Einstein(self.policy, i.N, y), i.H_Einstein_after)

            pi_default = default(i.N)[1]
            self.assertNotEqual(pi_default, i.pi_after)
            self.assertLessEqual(
                abs(pi_default - i.pi_before),
                1e-6 * abs(i.pi_before - i.pi_after),
            )

    # ---- (7) the new Radau instance sees the new K ----

    def test_7_every_RHS_evaluation_after_a_drain_sees_the_new_K(self):
        """
        The recorded evaluations, in call order. The new Radau instance's first evaluation is at
        (N_j, pi_after); from it until the next one's, every evaluation is at N >= N_j and sees
        K(N_j) with the drain in it. Under B6 (ledger.add after new_solver) the first does not.
        """
        calls = self.recorder.calls
        starts = []
        c = 0
        for i in self.result.injections:
            while not (calls[c][0] == i.N and calls[c][1] == i.pi_after):
                c += 1
            starts.append(c)
        self.assertEqual(len(starts), len(self.result.injections))
        bounds = starts + [len(calls)]
        for j, i in enumerate(self.result.injections):
            K_after = self.ledger.K(i.N)
            for N, _, K in calls[bounds[j] : bounds[j + 1]]:
                self.assertGreaterEqual(N, i.N)
                self.assertEqual(K, K_after, (j, N))

    # ---- (10) pickling ----

    def test_10_the_payload_pickles_with_its_records(self):
        copy = pickle.loads(pickle.dumps(self.data))
        self.assertEqual(copy["injections"], self.data["injections"])
        self.assertEqual(copy["turning_points"], self.data["turning_points"])
        self.assertEqual(
            copy["dark_radiation"].injections, self.data["dark_radiation"].injections
        )
        self.assertEqual(copy["reflections"], 0)


class TestTwoTurningPointsOnOneStep(unittest.TestCase):
    """
    (11) The interior sampling of pi (orchestrator note 02, ruling 15). Prompt 02 found the pair at
    M = 0.5 M_P, beta = 5, alpha = 0.1, at T_J ~ 0.7 MeV. Under prompt 03's gate that trajectory
    holds none, and neither do beta = 4, 6 and 8 at alpha = 0.1 nor beta = 5 at alpha = 0.05,
    0.2 and 0.3 (log 03 §2). At beta = 10, alpha = 0.1 (the module's setup otherwise) the field
    nearly stalls at T_J ~ 36 MeV, N ~ 63.853: one accepted step holds a plateau turnaround and a
    wall-side turning point, with pi > 0 at both ends. Detection at the step ends only (the
    breakage B9) misses both. One integration, about 8 s at a load of 13. Since
    inelastic-reflection prompt 06 the parking cut would end this history's oscillation at 358 MeV,
    before the near-stall, so it runs with the cut disabled (PARKING_EPSILON = 0.0).
    """

    @classmethod
    def setUpClass(cls):
        with mock.patch.object(SM, "PARKING_EPSILON", 0.0):
            cls.data, cls.result, _ = integrate_captured(10.0, 0.5, ALPHA)
        cls.ts = list(cls.result.solution.ts)
        cls.interpolants = cls.result.solution.interpolants

    def step_of(self, N: float) -> int:
        return min(bisect_right(self.ts, N) - 1, len(self.interpolants) - 1)

    def test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded(self):
        """
        Some step holds two turning points, a plateau turnaround and then a wall-side one, both
        strictly inside it, with pi > 0 at both of the step's ends; the records still alternate,
        and every wall-side one is drained (so the pair's wall point is too).
        """
        tps = self.result.turning_points
        per_step = {}
        for t in tps:
            per_step.setdefault(self.step_of(t.N), []).append(t)
        multi = {k: v for k, v in per_step.items() if len(v) >= 2}
        self.assertGreaterEqual(len(multi), 1)
        for k, pair in multi.items():
            self.assertEqual([t.kind for t in pair], ["plateau", "wall"], k)
            a, b = self.ts[k], self.ts[k + 1]
            for t in pair:
                self.assertTrue(a < t.N < b, (k, t))
            self.assertGreater(self.interpolants[k](a)[1], 0.0)
            self.assertGreater(self.interpolants[k](b)[1], 0.0)
            # the field is turned at each: pi changes sign across a small neighbourhood
            for t in pair:
                h = 1e-6 * (b - a)
                before, after = (
                    self.interpolants[k](t.N - h)[1],
                    self.interpolants[k](t.N + h)[1],
                )
                if t.kind == "wall":
                    self.assertTrue(before < 0.0 < after, t)
                else:
                    self.assertTrue(before > 0.0 > after, t)

        for t0, t1 in zip(tps, tps[1:]):
            self.assertNotEqual(t0.kind, t1.kind)
            self.assertLess(t0.N, t1.N)
        walls = [t for t in tps if t.kind == "wall"]
        self.assertEqual(
            sorted(i.N_turn for i in self.result.injections), [t.N for t in walls]
        )

    def test_11_no_step_holds_more_sign_changes_than_the_loop_recorded(self):
        """
        The narrowed stop of ruling 15: sampled at 17 points, no step's interpolant shows more sign
        changes of pi (a sample exactly 0.0 is not a sign) than the loop recorded on that step.
        """
        counts = {}
        for t in self.result.turning_points:
            k = self.step_of(t.N)
            counts[k] = counts.get(k, 0) + 1
        for k, interpolant in enumerate(self.interpolants):
            p = interpolant(np.linspace(self.ts[k], self.ts[k + 1], 17))[1]
            signs = [x > 0.0 for x in p if x != 0.0]
            changes = sum(1 for u, v in zip(signs, signs[1:]) if u != v)
            self.assertLessEqual(changes, counts.get(k, 0), k)


def build_with_ledger(beta: float, M: float):
    """kcl.build, with a DarkRadiationLedger given to the policy."""
    model = ExponentialModel(
        0, M_value(0, M * kcl._units.PlanckMass), 1, beta_value(0, beta), kcl._units
    ).realize(4.0 * log(1e-3 * kcl._units.eV), 0.0)
    ledger = DarkRadiationLedger()
    policy = SM.ODEPolicy("test", kcl._cosmology, model, dark_radiation=ledger)
    rhs = SM.ODERHS("test", policy)
    supervisor = ScalarFieldIntegrationSupervisor(
        kcl._units,
        kcl._T_init,
        kcl._T_stop,
        label="test",
        collect_full_statistics=False,
    )
    return policy, rhs, supervisor, ledger


def integrate_floor(alpha: float):
    """P1 at M = 1e-10 to N = 21 (kcl.check_reflected's run), with a ledger. About 0.2 s."""
    beta, N0, state = kcl.P1
    policy, rhs, supervisor, ledger = build_with_ledger(beta, 1e-10)
    with contextlib.redirect_stdout(io.StringIO()), supervisor:
        result = SM.integrate_scalar_history(
            rhs,
            supervisor,
            StateVector._make(state),
            N0,
            kcl._log_T_stop,
            SM.StepControl(),
            policy=policy,
            N_stop=21.0,
            alpha=alpha,
        )
    return result, policy, ledger


class TestAFloorRuleDrain(unittest.TestCase):
    """
    (8) The P1 harness at M = 1e-10: one floor reflection. Two runs, about 0.5 s. Since
    inelastic-reflection prompt 05 (README §8.2 P20) a floor reflection is unresolved: it drains
    with w_gate = 1.0 exactly, so alpha_eff = alpha (log 05 lists the edit).
    """

    def test_8_the_floor_reflection_drains(self):
        result, policy, ledger = integrate_floor(ALPHA)
        self.assertEqual(len(result.reflections), 1)
        floors = [i for i in result.injections if i.kind == "floor"]
        self.assertEqual(len(floors), 1)
        i = floors[0]
        reflection = result.reflections[0]
        pi_in = reflection.pi_Einstein_in

        self.assertEqual(reflection.kind, "floor")
        self.assertEqual(i.N, reflection.N)
        self.assertEqual(i.N_turn, reflection.N)
        self.assertEqual(i.phi_turn, reflection.phi_Einstein)
        self.assertEqual(i.pi_before, pi_in)
        self.assertEqual(i.pi_after, -(sqrt(1.0 - i.alpha_eff) * pi_in))
        self.assertEqual(result.solution(i.N)[1], i.pi_after)
        self.assertFalse(i.resolved)
        self.assertEqual(i.w_gate, 1.0)
        self.assertEqual(i.alpha_eff, ALPHA)
        self.assertTrue(i.nonadiabaticity is not None and i.nonadiabaticity > 0.0, i)

        self.assertEqual(
            ledger.injections, tuple((x.N, x.delta_K) for x in result.injections)
        )
        self.assertEqual(ledger.injections[0], (i.N, i.delta_K))
        direct = i.alpha_eff * 0.5 * i.H_Einstein_before**2 * pi_in**2 * exp(4.0 * i.N)
        self.assertLessEqual(abs(i.delta_K / direct - 1.0), delta_K_bound(i))

        bound, c = H_bound(i, policy, result.solution(i.N))
        self.assertLessEqual(
            abs(i.H_Einstein_after - i.H_Einstein_before) / i.H_Einstein_before, bound
        )

    def test_8_at_alpha_zero_the_run_is_todays(self):
        """With a ledger and alpha = 0: no injection, an empty ledger, and kcl's run bitwise."""
        result, policy, ledger = integrate_floor(0.0)
        today, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(result.injections, [])
        self.assertTrue(ledger.is_empty)
        self.assertEqual(len(result.reflections), 1)
        reflection = result.reflections[0]
        self.assertEqual(result.solution(reflection.N)[1], -reflection.pi_Einstein_in)
        self.assertEqual(result.reflections, today.reflections)
        self.assertEqual(result.nfev, today.nfev)
        self.assertEqual(result.accepted_steps, today.accepted_steps)
        self.assertEqual(tuple(result.final_state), tuple(today.final_state))


class TestTheParameter(unittest.TestCase):
    """(9) alpha's range and the ledger it needs. Milliseconds: nothing integrates."""

    BAD = (-0.1, 1.0, 1.5, float("nan"))

    def test_9_integrate_scalar_history_refuses_a_bad_alpha_before_any_solver(self):
        beta, N0, state = kcl.P1
        policy, rhs, supervisor, _ = build_with_ledger(beta, 0.5)
        for alpha in self.BAD:
            with mock.patch.object(SM, "Radau") as radau:
                with self.assertRaises(ValueError):
                    SM.integrate_scalar_history(
                        rhs,
                        supervisor,
                        StateVector._make(state),
                        N0,
                        kcl._log_T_stop,
                        policy=policy,
                        alpha=alpha,
                    )
                radau.assert_not_called()

    def test_9_alpha_needs_the_ledger_the_RHS_reads(self):
        beta, N0, state = kcl.P1
        no_ledger_policy, no_ledger_rhs, supervisor = kcl.build(beta, 0.5)
        policy, rhs, _, _ = build_with_ledger(beta, 0.5)
        other, _, _, _ = build_with_ledger(beta, 0.5)
        for RHS, pol in ((no_ledger_rhs, no_ledger_policy), (rhs, other)):
            with mock.patch.object(SM, "Radau") as radau:
                with self.assertRaises(ValueError):
                    SM.integrate_scalar_history(
                        RHS,
                        supervisor,
                        StateVector._make(state),
                        N0,
                        kcl._log_T_stop,
                        policy=pol,
                        alpha=ALPHA,
                    )
                radau.assert_not_called()

    def test_9_integrate_history_refuses_before_any_integration(self):
        cosmology = gth.make_cosmology()
        for alpha in self.BAD:
            with mock.patch.object(SM, "integrate_scalar_history") as integrate:
                with self.assertRaises(ValueError):
                    SM.integrate_history(
                        cosmology,
                        None,
                        None,
                        None,
                        None,
                        None,
                        mock.Mock(),
                        alpha=alpha,
                    )
                integrate.assert_not_called()

    def test_9_tune_history_refuses_before_the_seed(self):
        cosmology = gth.make_cosmology()
        for alpha in self.BAD:
            model = mock.Mock()
            with mock.patch.object(SM, "integrate_history") as integrate:
                with self.assertRaises(ValueError):
                    SM.tune_history(
                        cosmology, model, None, None, None, None, alpha=alpha
                    )
                integrate.assert_not_called()
            model.seed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
