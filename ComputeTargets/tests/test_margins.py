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
The spectrum's peak and the relativistic margins (inelastic-reflection prompt 08; README §8.4, gate
"Margin").

After the integration, at alpha > 0 on a model with a wall, integrate_history computes each generation's
peak by its R (classical, one-loop, or classical in the one-loop gap, flagged), and three margins
ln(p/m) per generation: at birth, at every later event (candidates, plateau turning points, wall-side
turning points with no candidate), and on the late tracker after N_late, today included. The worst of
each over the generations carrying at least 10^-3 of K, the failing shares and the first failures are
the payload's "margins"; the generations are its "generations"; the warning lines are its
"margin_warnings" (orchestrator note 08, ruling 1). The items are those of prompt §1.8:

  (1) a constructed history, through the pure core, against a hand computation;
  (2) the energy weight at its edge, and the leftover counted only as unassessed;
  (3) the estimate by R on a history that crosses the one-loop gap (M = 1e-16 M_P, beta = 2);
  (4) the classical birth margin, ell_t bitwise and negative, on resolved turnarounds (M = 0.5 M_P);
  (5) the tracker margin on a parked (M = 1e-30 M_P) and on a released (M = 0.5 M_P, beta = 5) history;
  (6) alpha = 0: nothing computed, on the golden histories' setups;
  (7) the warnings, in the payload and in tune_history;
  (8) a failure in the margins is a warning, never the history's failure.
Item (9), that the existing tests pass unedited, is the suites'.

Every history uses test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB,
atol = rtol = 1e-8, main.py's axis, the model's seed realization, untuned) at alpha = 0.1, unless it
says otherwise. Each is integrated once per module (history()). No Ray, no store, no PRyMordial solve.
Each class that integrates gives its cost and the load in its docstring.
"""

import contextlib
import importlib
import io
import unittest
from math import log, log10, nextafter
from unittest import mock

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1
LN10 = log(10.0)

# the step loop as this module was imported, before any test patches the module's name
_INTEGRATE = SM.integrate_scalar_history

_HISTORIES = {}


def run(beta: float, M: float, alpha: float, patches=()):
    """
    integrate_history at test_injection's setup under the patches, with the IntegrationResult and the
    ODEPolicy captured. Returns (payload, IntegrationResult, ODEPolicy, the realized model).
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    Mp = units.PlanckMass
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)
    captured = []

    def capturing(RHS, *args, **kwargs):
        result = _INTEGRATE(RHS, *args, **kwargs)
        captured.append((result, RHS.policy))
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
            task_label="test_margins",
            alpha=alpha,
        )
    result, policy = captured[-1]
    return data, result, policy, realized


def history(beta: float, M: float, alpha: float = ALPHA):
    """run(beta, M, alpha) without patches, once per module."""
    key = (beta, M, alpha)
    if key not in _HISTORIES:
        _HISTORIES[key] = run(beta, M, alpha)
    return _HISTORIES[key]


def candidate(index, N_turn, delta_K, estimate, log_k_ex, log_m, log_R=None):
    return SM.MarginCandidate(
        index=index,
        kind="turning_point" if estimate is not None else "floor",
        N_turn=N_turn,
        delta_K=delta_K,
        estimate=estimate,
        log_k_ex=log_k_ex,
        log_m=log_m,
        log_R=log_R,
        gap_reason="raised" if estimate == SM.ESTIMATE_CLASSICAL_GAP else None,
    )


# ---- (1) a constructed history, through the pure core ----


class TestAConstructedHistory(unittest.TestCase):
    """
    (1) Three generations: classical (#0), one-loop (#2), in the gap (#3); a later candidate with
    delta_K = 0 (#1); an unassessed candidate (#4); two plateau turning points, of which the one at
    N = 11.5 binds the reflection margins of #0 and #2; a wall-side turning point with no candidate
    (N = 12.2), which binds #3's; samples before N_late, at it, and after it; today; a leftover and a
    K_0. K_final = 1, so that a share is its delta_K exactly. Every expected value is formed in the test
    by the same operations as the core's (the margins are one sum and one or two differences), so the
    comparisons are equalities; the partition of K is a sum of six terms in another order, checked to
    6 eps. Microseconds.
    """

    @classmethod
    def setUpClass(cls):
        cls.candidates = [
            candidate(0, 10.0, 0.3, SM.ESTIMATE_CLASSICAL, 1.0, 1.7, -5.0),
            candidate(1, 10.5, 0.0, SM.ESTIMATE_CLASSICAL, 0.5, 2.0, -4.0),
            candidate(2, 11.0, 0.4, SM.ESTIMATE_ONE_LOOP, 5.0, -10.0, 9.0),
            candidate(3, 12.0, 0.2, SM.ESTIMATE_CLASSICAL_GAP, 3.0, 2.5, 2.0),
            candidate(4, 12.5, 0.05, None, None, 0.0),
        ]
        cls.events = [
            SM.MarginEvent(10.25, 1.0, "plateau"),
            SM.MarginEvent(11.5, 9.0, "plateau"),
            SM.MarginEvent(12.2, 3.0, "wall"),
        ]
        cls.N_late = 12.5
        # the first two lie before N_late and at it, and must not be read
        cls.samples = [(12.0, 50.0), (12.5, 50.0), (13.0, 1.0), (14.0, -1.0)]
        cls.today = (20.0, -8.0)
        cls.leftover, cls.K_0 = 0.04, 0.01
        cls.generations, cls.margins = SM.relativistic_margins(
            cls.candidates,
            cls.events,
            cls.N_late,
            cls.samples,
            cls.today,
            1.0,
            cls.leftover + cls.K_0,
        )
        cls.by_index = {g.index: g for g in cls.generations}

    def test_1_the_generations_are_the_assessed_candidates_with_delta_K(self):
        self.assertEqual([g.index for g in self.generations], [0, 2, 3])
        self.assertEqual(
            [g.estimate for g in self.generations],
            [SM.ESTIMATE_CLASSICAL, SM.ESTIMATE_ONE_LOOP, SM.ESTIMATE_CLASSICAL_GAP],
        )
        for g in self.generations:
            c = self.candidates[g.index]
            self.assertEqual(g.log_q, c.log_k_ex + c.N_turn)
            self.assertEqual(g.share, c.delta_K)
            self.assertTrue(g.weighted)

    def test_1_the_birth_margins(self):
        for g in self.generations:
            c = self.candidates[g.index]
            self.assertEqual(g.birth_margin, c.log_k_ex - c.log_m)

    def test_1_the_reflection_margins_and_the_plateau_that_binds(self):
        # (N_k, ln m_k) of every event, the delta_K = 0 candidate and the unassessed one included
        g0, g2, g3 = (self.by_index[k] for k in (0, 2, 3))
        self.assertEqual(g0.reflection_margin, (1.0 + 10.0) - 11.5 - 9.0)
        self.assertEqual(g0.reflection_margin_N, 11.5)
        self.assertEqual(g2.reflection_margin, (5.0 + 11.0) - 11.5 - 9.0)
        self.assertEqual(g2.reflection_margin_N, 11.5)
        self.assertEqual(g3.reflection_margin, (3.0 + 12.0) - 12.2 - 3.0)
        self.assertEqual(g3.reflection_margin_N, 12.2)
        # every value the core minimized over, by hand, for #0
        values = [
            (1.0 + 10.0) - N - lm
            for N, lm in [
                (10.25, 1.0),
                (10.5, 2.0),
                (11.0, -10.0),
                (11.5, 9.0),
                (12.0, 2.5),
                (12.2, 3.0),
                (12.5, 0.0),
            ]
        ]
        self.assertEqual(g0.reflection_margin, min(values))

    def test_1_without_the_plateau_the_margin_would_be_another(self):
        """The plateau at 11.5 binds: with it left out, #0's margin is the wall event's."""
        _, margins = SM.relativistic_margins(
            self.candidates,
            [self.events[0], self.events[2]],
            self.N_late,
            self.samples,
            self.today,
            1.0,
            self.leftover + self.K_0,
        )
        self.assertEqual(margins.worst_reflection_margin, (1.0 + 10.0) - 12.2 - 3.0)

    def test_1_the_tracker_margins_read_the_late_samples_and_today_only(self):
        late = [(13.0, 1.0), (14.0, -1.0), (20.0, -8.0)]
        for g in self.generations:
            values = [g.log_q - N - lm for N, lm in late]
            self.assertEqual(g.tracker_margin, min(values))
            self.assertEqual(g.tracker_margin_N, late[values.index(min(values))][0])
        self.assertEqual(self.margins.late_points, 3)

    def test_1_the_worst_margins(self):
        m = self.margins
        self.assertEqual(
            (m.worst_birth_margin, m.worst_birth_margin_N, m.worst_birth_margin_index),
            (1.0 - 1.7, 10.0, 0),
        )
        self.assertEqual(
            (
                m.worst_reflection_margin,
                m.worst_reflection_margin_N,
                m.worst_reflection_margin_index,
            ),
            ((1.0 + 10.0) - 11.5 - 9.0, 11.5, 0),
        )
        self.assertEqual(
            (
                m.worst_tracker_margin,
                m.worst_tracker_margin_N,
                m.worst_tracker_margin_index,
            ),
            ((1.0 + 10.0) - 13.0 - 1.0, 13.0, 0),
        )

    def test_1_the_failing_shares(self):
        m = self.margins
        # birth: #0 (-0.7) and #3 (0.5) fail, #2 (15) passes
        self.assertEqual(m.birth_failing_share, 0.0 + 0.3 + 0.2)
        # reflection and tracker: all three fail (#2's tracker margin is 2 < ln 10)
        self.assertLess((5.0 + 11.0) - 13.0 - 1.0, LN10)
        self.assertEqual(m.reflection_failing_share, 0.0 + 0.3 + 0.4 + 0.2)
        self.assertEqual(m.tracker_failing_share, 0.0 + 0.3 + 0.4 + 0.2)

    def test_1_the_shares_and_the_counts(self):
        m = self.margins
        self.assertEqual(m.weighted_share, 0.0 + 0.3 + 0.4 + 0.2)
        self.assertEqual(m.below_weight_share, 0.0)
        self.assertEqual(m.unassessed_share, (0.04 + 0.01) / 1.0 + 0.05 / 1.0)
        # six terms summed: within 6 eps of 1
        total = m.weighted_share + m.below_weight_share + m.unassessed_share
        self.assertLessEqual(abs(total - 1.0), 6 * EPS)
        self.assertEqual(
            (
                m.classical_generations,
                m.one_loop_generations,
                m.classical_gap_generations,
                m.weighted_generations,
                m.unassessed_candidates,
                m.plateau_events,
                m.wall_events,
            ),
            (1, 1, 1, 3, 1, 2, 1),
        )
        self.assertEqual(m.N_late, 12.5)

    def test_1_the_first_failures(self):
        m = self.margins
        # birth: the earliest failing N_turn
        self.assertEqual(
            m.first_birth_failure,
            SM.MarginFailure(
                0, 10.0, 1.0 - 1.7, 1.0 + 10.0, 1.7, SM.ESTIMATE_CLASSICAL
            ),
        )
        # reflection: #0's first event below ln 10 is the plateau at 10.25, before #2's and #3's
        self.assertEqual(
            m.first_reflection_failure,
            SM.MarginFailure(
                0,
                10.25,
                (1.0 + 10.0) - 10.25 - 1.0,
                1.0 + 10.0,
                1.0,
                SM.ESTIMATE_CLASSICAL,
            ),
        )
        # tracker: all three fail first at N = 13; the smallest margin there is #0's (ruling 8)
        self.assertEqual(
            m.first_tracker_failure,
            SM.MarginFailure(
                0,
                13.0,
                (1.0 + 10.0) - 13.0 - 1.0,
                1.0 + 10.0,
                1.0,
                SM.ESTIMATE_CLASSICAL,
            ),
        )

    def test_1_a_history_that_drained_nothing_has_no_generation(self):
        """K_final = 0 (every candidate with delta_K = 0, no leftover): no generation, every share 0."""
        generations, margins = SM.relativistic_margins(
            [candidate(0, 10.0, 0.0, SM.ESTIMATE_CLASSICAL, 1.0, 1.7)],
            [],
            10.0,
            [],
            (20.0, -8.0),
            0.0,
            0.0,
        )
        self.assertEqual(generations, ())
        self.assertEqual(
            (
                margins.weighted_share,
                margins.below_weight_share,
                margins.unassessed_share,
            ),
            (0.0, 0.0, 0.0),
        )
        for kind in SM.MARGIN_KINDS:
            self.assertIsNone(getattr(margins, f"worst_{kind}_margin"))
            self.assertIsNone(getattr(margins, f"first_{kind}_failure"))

    def test_1_no_later_event_is_a_margin_of_None_and_not_failing(self):
        """A lone generation has no later event: its reflection margin is None and fails nothing."""
        generations, margins = SM.relativistic_margins(
            [candidate(0, 10.0, 1.0, SM.ESTIMATE_CLASSICAL, 1.0, 1.7)],
            [],
            10.0,
            [],
            (20.0, -8.0),
            1.0,
            0.0,
        )
        self.assertIsNone(generations[0].reflection_margin)
        self.assertIsNone(generations[0].reflection_margin_N)
        self.assertIsNone(margins.worst_reflection_margin)
        self.assertIsNone(margins.first_reflection_failure)
        self.assertEqual(margins.reflection_failing_share, 0.0)


# ---- (2) the energy weight ----


class TestTheEnergyWeight(unittest.TestCase):
    """
    (2) A generation with the worst margins at a share one float below MARGIN_WEIGHT_SHARE is left out of
    the worst and the failing shares, and counted below the weight; at MARGIN_WEIGHT_SHARE exactly it
    is in. K_final = 1, so that a share is its delta_K. Microseconds. Then, on the parked history at
    M = 1e-30 M_P, beta = 2 (about 6 s at a load of 10-20, shared with (5)): the leftover is counted
    only as unassessed, and no generation stands for it.
    """

    def margins_at(self, small_share):
        big = 1.0 - small_share
        return SM.relativistic_margins(
            [
                candidate(0, 10.0, big, SM.ESTIMATE_CLASSICAL, 5.0, 1.0),
                candidate(1, 11.0, small_share, SM.ESTIMATE_CLASSICAL, -5.0, 1.0),
            ],
            [],
            11.0,
            [],
            (20.0, -20.0),
            1.0,
            0.0,
        )

    def test_2_the_threshold_is_1e_minus_3(self):
        self.assertEqual(SM.MARGIN_WEIGHT_SHARE, 1.0e-3)
        self.assertEqual(SM.MARGIN_THRESHOLD, log(10.0))

    def test_2_just_below_the_weight_it_is_left_out(self):
        share = nextafter(1.0e-3, 0.0)
        generations, m = self.margins_at(share)
        self.assertFalse(generations[1].weighted)
        self.assertEqual(m.worst_birth_margin, 5.0 - 1.0)
        self.assertEqual(m.worst_birth_margin_index, 0)
        self.assertEqual(m.birth_failing_share, 0.0)
        self.assertIsNone(m.first_birth_failure)
        self.assertEqual(m.weighted_generations, 1)
        self.assertEqual(m.below_weight_share, share)

    def test_2_at_the_weight_it_is_included(self):
        generations, m = self.margins_at(1.0e-3)
        self.assertTrue(generations[1].weighted)
        self.assertEqual(m.worst_birth_margin, -5.0 - 1.0)
        self.assertEqual(m.worst_birth_margin_index, 1)
        self.assertEqual(m.birth_failing_share, 1.0e-3)
        self.assertEqual(m.first_birth_failure.index, 1)
        self.assertEqual(m.below_weight_share, 0.0)

    def test_2_the_leftover_is_counted_only_as_unassessed(self):
        data, result, policy, _ = history(2.0, 1.0e-30)
        self.assertIsNotNone(result.parking)
        self.assertGreater(result.parking.delta_K, 0.0)
        expected = [
            k
            for k, x in enumerate(result.injections)
            if x.delta_K > 0.0 and x.classical_turning_point is not None
        ]
        self.assertEqual([g.index for g in data["generations"]], expected)
        K = policy.dark_radiation.K(result.N_final)
        m = data["margins"]
        self.assertEqual(m.unassessed_candidates, 0)
        self.assertEqual(m.unassessed_share, result.parking.delta_K / K)
        # the leftover's candidate is a generation with its own share, not the leftover's
        cut = data["generations"][-1]
        self.assertEqual(cut.index, result.parking.candidate_index)
        self.assertEqual(cut.share, result.injections[cut.index].delta_K / K)
        # the partition: one term per generation and the leftover, summed in index order
        n = len(data["generations"]) + 1
        total = m.weighted_share + m.below_weight_share + m.unassessed_share
        self.assertLessEqual(abs(total - 1.0), n * 4 * EPS)


# ---- (3) the estimate by R, across the one-loop gap ----


class TestTheEstimateByR(unittest.TestCase):
    """
    (3) M = 1e-16 M_P, beta = 2: the history of log 07b §5.3 that crosses the gap. Its generations with
    ln R <= 0 are classical; the three with 0 < log10 R <= 2.92 are "classical_gap" (the chain raises:
    "no one-loop turnaround"), carrying 0.5877 of K; the one at log10 R >= 3.00 is one-loop, carrying
    0.2812, with birth margin its ell > 0 bitwise. The shares to log 07b's four digits and their rounding,
    5e-5 (orchestrator note 08, ruling 9). About 6 s at a load of 10-20.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, cls.policy, cls.model = history(2.0, 1.0e-16)

    def test_3_the_history_succeeds(self):
        self.assertFalse(self.data.get("failure", False), self.data)
        self.assertIsNotNone(self.data["margins"])

    def test_3_the_estimate_follows_R(self):
        for g in self.data["generations"]:
            log10_R = g.log_R / LN10
            if g.log_R <= 0.0:
                self.assertEqual(g.estimate, SM.ESTIMATE_CLASSICAL, g)
            elif log10_R <= 2.92:
                self.assertEqual(g.estimate, SM.ESTIMATE_CLASSICAL_GAP, g)
                self.assertTrue(g.gap_reason.startswith("one_loop_turnaround"), g)
                self.assertIn("no one-loop turnaround", g.gap_reason)
            else:
                self.assertGreaterEqual(log10_R, 3.00)
                self.assertEqual(g.estimate, SM.ESTIMATE_ONE_LOOP, g)

    def test_3_the_counts_and_shares_of_log_07b(self):
        gens = self.data["generations"]
        gap = [g for g in gens if g.estimate == SM.ESTIMATE_CLASSICAL_GAP]
        one_loop = [g for g in gens if g.estimate == SM.ESTIMATE_ONE_LOOP]
        self.assertEqual(len(gap), 3)
        self.assertEqual(len(one_loop), 1)
        self.assertLessEqual(abs(sum(g.share for g in gap) - 0.5877), 5.0e-5)
        self.assertLessEqual(abs(one_loop[0].share - 0.2812), 5.0e-5)
        m = self.data["margins"]
        self.assertEqual(m.classical_gap_generations, 3)
        self.assertEqual(m.one_loop_generations, 1)
        self.assertEqual(m.classical_generations, len(gens) - 4)

    def test_3_the_one_loop_birth_margin_is_its_ell(self):
        (g,) = [
            g for g in self.data["generations"] if g.estimate == SM.ESTIMATE_ONE_LOOP
        ]
        x = self.result.injections[g.index]
        turnaround = self.model.one_loop_turnaround(x.phi_assessed, x.log_E_assessed)
        self.assertEqual(g.birth_margin, turnaround.ell)
        self.assertEqual(g.log_k_ex, turnaround.log_k_ex)
        self.assertEqual(g.log_m, turnaround.log_m_ta)
        self.assertGreater(g.birth_margin, 0.0)
        self.assertEqual(g.log_q, turnaround.log_k_ex + x.N_turn)

    def test_3_a_gap_generation_is_born_classical(self):
        for g in self.data["generations"]:
            if g.estimate != SM.ESTIMATE_ONE_LOOP:
                x = self.result.injections[g.index]
                ct = x.classical_turning_point
                self.assertEqual(g.birth_margin, ct.ell_t)
                self.assertEqual(g.log_q, ct.log_k_ex + x.N_turn)


# ---- (4) the classical birth margin on resolved turnarounds ----


class TestTheClassicalBirthMargin(unittest.TestCase):
    """
    (4) M = 0.5 M_P, beta = 2: every generation is classical, and its birth margin is its ell_t
    bitwise, and negative (on the exponential wall k_ex/omega_t -> 1/2). About 3 s at a load of 10-20,
    shared with (7) and (8).
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, _, _ = history(2.0, 0.5)

    def test_4_every_generation_is_classical_and_born_below_zero(self):
        gens = self.data["generations"]
        self.assertGreater(len(gens), 0)
        for g in gens:
            ct = self.result.injections[g.index].classical_turning_point
            self.assertEqual(g.estimate, SM.ESTIMATE_CLASSICAL)
            self.assertLessEqual(g.log_R, 0.0)
            self.assertEqual(g.birth_margin, ct.ell_t)
            self.assertLess(g.birth_margin, 0.0)
            x = self.result.injections[g.index]
            # the turning point's N, not the drain point's (they differ at a resolved candidate)
            self.assertLess(x.N_turn, x.N)
            self.assertEqual(g.N_turn, x.N_turn)
            self.assertEqual(g.log_q, ct.log_k_ex + x.N_turn)

    def test_4_the_events_are_the_plateau_and_the_undrained_wall_turning_points(self):
        """Every plateau turning point is an event, and so is a wall-side one with no candidate (ruling 5)."""
        m = self.data["margins"]
        drained = {
            x.N_turn for x in self.result.injections if x.kind == "turning_point"
        }
        tps = self.result.turning_points
        self.assertEqual(m.plateau_events, sum(1 for tp in tps if tp.kind == "plateau"))
        self.assertGreater(m.plateau_events, 0)
        self.assertEqual(
            m.wall_events,
            sum(1 for tp in tps if tp.kind == "wall" and tp.N not in drained),
        )


# ---- (5) the tracker margin ----


def tracker_recomputed(model, s):
    """ln m_tr at a stored sample, by the test: the minimum from the sample's field, the mass there."""
    log_T = s.log_rhorad_Einstein + log(s.Sigma + SM.exp(s.log_fm))
    phi_min = model.minimum_on_trace(s.phi_Einstein, log_T)
    return 0.5 * SM.log_m_eff_squared(
        model, phi_min, s.log_rhorad_Einstein, s.log_fm, s.Sigma
    )


class TestTheTrackerMargin(unittest.TestCase):
    """
    (5) The parked history, M = 1e-30 M_P, beta = 2 (about 6 s): N_late is the last candidate's N_turn
    (there is no turning point after it), the tracker margin reads only the samples after it and today,
    and each generation's equals the test's recomputation from the samples, bitwise (the same calls).
    The released history, M = 0.5 M_P, beta = 5 (about 6 s): its generations after the release are in
    the list, and N_late lies after the release. Load 10-20.
    """

    @classmethod
    def setUpClass(cls):
        cls.parked = history(2.0, 1.0e-30)
        cls.released = history(5.0, 0.5)

    def test_5_N_late_on_the_parked_history(self):
        data, result, _, _ = self.parked
        self.assertIsNone(result.release)
        N_late = max(
            [tp.N for tp in result.turning_points]
            + [x.N_turn for x in result.injections]
        )
        self.assertEqual(data["margins"].N_late, N_late)
        self.assertLessEqual(N_late, result.parking.N)

    def test_5_the_parked_tracker_margin_is_the_recomputation(self):
        data, result, policy, model = self.parked
        m = data["margins"]
        late = [s for s in data["sample"] if s.raw_N > m.N_late]
        self.assertEqual(m.late_points, len(late) + 1)
        points = [(s.raw_N, tracker_recomputed(model, s)) for s in late]
        f = result.final_state
        _, Sigma, _ = SM.log_trace(
            policy.cosmology, (f.log_rhorad_Einstein, f.log_fm, f.log_T_Jordan)
        )
        log_T = f.log_rhorad_Einstein + log(Sigma + SM.exp(f.log_fm))
        phi_min = model.minimum_on_trace(f.phi_Einstein, log_T)
        points.append(
            (
                result.N_final,
                0.5
                * SM.log_m_eff_squared(
                    model, phi_min, f.log_rhorad_Einstein, f.log_fm, Sigma
                ),
            )
        )
        for g in data["generations"]:
            values = [g.log_q - N - lm for N, lm in points]
            k = min(range(len(values)), key=lambda i: (values[i], i))
            self.assertEqual(g.tracker_margin, values[k], g.index)
            self.assertEqual(g.tracker_margin_N, points[k][0], g.index)
            self.assertGreater(g.tracker_margin_N, m.N_late)

    def test_5_the_released_history_includes_the_generations_after_the_release(self):
        data, result, _, _ = self.released
        self.assertFalse(data.get("failure", False), data)
        release = result.release
        self.assertIsNotNone(release)
        after = [
            k
            for k, x in enumerate(result.injections)
            if k >= release.candidate_index and x.delta_K > 0.0
        ]
        self.assertGreater(len(after), 0)
        indices = [g.index for g in data["generations"]]
        for k in after:
            self.assertIn(k, indices)
        m = data["margins"]
        self.assertGreater(m.N_late, release.N)
        self.assertEqual(
            m.N_late,
            max(
                [tp.N for tp in result.turning_points]
                + [x.N_turn for x in result.injections]
            ),
        )
        for g in data["generations"]:
            self.assertGreater(g.tracker_margin_N, m.N_late)


# ---- (6) alpha = 0 ----


class TestNothingAtAlphaZero(unittest.TestCase):
    """
    (6) On the golden histories' setups (gh: beta = 1.5 and 5.5, to 1 MeV; gth's model untuned at its
    seed, to T_CMB), alpha = 0: "generations" and "margins" are None, "margin_warnings" is (), and
    "warnings" is the loop's. That every other value is bitwise the dispatch commit's is the golden
    tests' (unedited) and log 08 §2.5's (orchestrator note 08, ruling 11). About 10 s at a load of
    10-20.
    """

    def check(self, data, result):
        self.assertFalse(data.get("failure", False), data)
        self.assertIsNone(data["generations"])
        self.assertIsNone(data["margins"])
        self.assertEqual(data["margin_warnings"], ())
        self.assertEqual(data["warnings"], result.warnings)

    def test_6_the_golden_histories(self):
        for beta in gh.BETAS:
            captured = []

            def capturing(RHS, *args, **kwargs):
                result = _INTEGRATE(RHS, *args, **kwargs)
                captured.append(result)
                return result

            with mock.patch.object(
                SM, "integrate_scalar_history", capturing
            ), contextlib.redirect_stdout(io.StringIO()):
                data = gh.run_history(beta)
            with self.subTest(beta=beta):
                self.check(data, captured[-1])

    def test_6_natural_mode_at_large_M(self):
        data, result, _, _ = history(2.0, 0.5, alpha=0.0)
        self.check(data, result)


# ---- (7) the warnings ----


class TestTheWarnings(unittest.TestCase):
    """
    (7) M = 0.5 M_P, beta = 2 (shared with (4)) carries the birth and reflection lines on
    "margin_warnings", whose kinds warning_kind returns, and "warnings" is the loop's. Then tune_history
    with integrate_history and _tuning_state patched to scripted payloads (test_unresolved_turnarounds'
    wiring): each kind, of either key, is printed once, from the first integration that returns it.
    """

    def test_7_the_phrases_are_kinds(self):
        for phrase in (
            SM.WARNING_BIRTH_MARGIN,
            SM.WARNING_REFLECTION_MARGIN,
            SM.WARNING_TRACKER_MARGIN,
            SM.WARNING_MARGINS_FAILED,
        ):
            self.assertIn(phrase, SM.WARNING_KINDS)
            self.assertEqual(SM.warning_kind(phrase + ": x"), phrase)

    def test_7_birth_and_reflection_lines_at_large_M(self):
        data, result, _, _ = history(2.0, 0.5)
        self.assertEqual(data["warnings"], result.warnings)
        kinds = [SM.warning_kind(line) for line in data["margin_warnings"]]
        self.assertIn(SM.WARNING_BIRTH_MARGIN, kinds)
        self.assertIn(SM.WARNING_REFLECTION_MARGIN, kinds)
        self.assertEqual(len(kinds), len(set(kinds)))
        m = data["margins"]
        self.assertEqual(
            len(kinds),
            sum(
                1
                for kind in SM.MARGIN_KINDS
                if getattr(m, f"worst_{kind}_margin") is not None
                and getattr(m, f"worst_{kind}_margin") < LN10
            ),
        )
        (birth,) = [
            l for l in data["margin_warnings"] if l.startswith(SM.WARNING_BIRTH_MARGIN)
        ]
        self.assertIn(f"{m.worst_birth_margin:.4f}", birth)
        self.assertIn(f"{m.birth_failing_share:.4g}", birth)

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

        class PlainSteps:
            reset = False

            def __init__(self, *args, **kwargs):
                pass

            def step(self, x, G):
                return (G[0], G[1]), "plain"

            def guard(self):
                pass

        out = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(SM, "integrate_history", side_effect=payloads)
            )
            stack.enter_context(
                mock.patch.object(SM, "_tuning_state", return_value=state)
            )
            stack.enter_context(mock.patch.object(SM, "TuningSecant", PlainSteps))
            stack.enter_context(contextlib.redirect_stdout(out))
            data = gth.run_tuning(
                cosmology=cosmology, max_integrations=len(payloads), clock=lambda: 0.0
            )
        return data, [l for l in out.getvalue().splitlines() if l.startswith("!! ")]

    def test_7_tune_history_prints_each_kind_once(self):
        birth = [f"{SM.WARNING_BIRTH_MARGIN}: {k}" for k in (1, 2, 3)]
        tracker = [f"{SM.WARNING_TRACKER_MARGIN}: {k}" for k in (1, 2)]
        elastic = f"{SM.WARNING_NEARLY_ELASTIC}: R = 10^-3.000 < 1"
        payloads = [
            {"sample": [], "warnings": (elastic,), "margin_warnings": (birth[0],)},
            {
                "sample": [],
                "warnings": (elastic,),
                "margin_warnings": (birth[1], tracker[0]),
            },
            {"sample": [], "margin_warnings": (birth[2], tracker[1])},
        ]
        data, lines = self._tune(payloads)
        self.assertTrue(data["failure"])
        prefix = f"!! tune_history ({gth.TASK_LABEL}): "
        self.assertEqual(
            lines[:3], [prefix + elastic, prefix + birth[0], prefix + tracker[0]]
        )
        self.assertEqual(len(lines), 4)
        self.assertIn("the normalization did not converge", lines[3])


# ---- (8) a failure in the margins ----


def failing_tracker(*args, **kwargs):
    raise ComputationFailureError("tracker_log_m: a stub that raises (test_margins)")


class TestAFailureInTheMargins(unittest.TestCase):
    """
    (8) M = 0.5 M_P, beta = 2, with tracker_log_m patched through the module's name to a stub that raises
    ComputationFailureError (the model's minimum_on_trace is not patched, so the integration is untouched;
    orchestrator note 08, ruling 4): the history succeeds, "generations" and "margins" are None,
    "margin_warnings" is the one line of its kind, and every other key is bitwise the unpatched run's
    (by repr; metadata's times and the z grid's object aside). About 6 s at a load of 10-20, the
    unpatched run shared with (4).
    """

    @classmethod
    def setUpClass(cls):
        cls.patched = run(
            2.0,
            0.5,
            ALPHA,
            patches=(mock.patch.object(SM, "tracker_log_m", failing_tracker),),
        )
        cls.unpatched = history(2.0, 0.5)

    def test_8_the_history_succeeds_with_the_failure_as_its_warning(self):
        data = self.patched[0]
        self.assertFalse(data.get("failure", False), data)
        self.assertIsNone(data["generations"])
        self.assertIsNone(data["margins"])
        self.assertEqual(len(data["margin_warnings"]), 1)
        line = data["margin_warnings"][0]
        self.assertEqual(SM.warning_kind(line), SM.WARNING_MARGINS_FAILED)
        self.assertIn("a stub that raises", line)

    def test_8_every_other_key_is_the_unpatched_runs(self):
        patched, unpatched = self.patched[0], self.unpatched[0]
        # "log_m_tracker" (one-loop-drain prompt 03, the sample masses) calls tracker_log_m too, so the patched
        # run's is None at every sample
        skip = {
            "metadata",
            "z_grid",
            "generations",
            "margins",
            "margin_warnings",
            "log_m_tracker",
        }
        self.assertEqual(set(patched), set(unpatched))
        for key in sorted(set(patched) - skip):
            with self.subTest(key=key):
                self.assertEqual(repr(patched[key]), repr(unpatched[key]))
        self.assertEqual(
            [float(z.log_one_plus_z) for z in patched["z_grid"]],
            [float(z.log_one_plus_z) for z in unpatched["z_grid"]],
        )
        for field in ("compute_steps", "RHS_evaluations"):
            self.assertEqual(
                getattr(patched["metadata"], field),
                getattr(unpatched["metadata"], field),
            )


if __name__ == "__main__":
    unittest.main()
