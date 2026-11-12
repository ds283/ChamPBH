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
Turning points resolved above the solver's tolerance (electroweak-eos prompt 01a; README §8.3 U8, U9;
gate "Resolved").

The rule. On an accepted step, pi is read at the two nodes and at TURNING_POINT_INTERIOR_SAMPLES
interior points, and the sign changes between consecutive samples are found as before 01a. If the
samples show exactly one change and pi at the two nodes is of opposite signs, it is a turning point,
whatever its size. Otherwise the changes are walked in order from the step's start: for a change i
with a successor i + 1, the excursion is the largest |pi| over the samples between the two crossings;
if it is not greater than tau = atol + rtol max(|pi_low|, |pi_high|) (turning_point_tolerance, the
tolerance to which Radau holds pi = y[1] on the step), both changes are dropped and the walk goes on
at i + 2; otherwise change i is kept and the walk goes on at i + 1. A dropped pair is as if it were
not there (no TurningPoint, no drain armed or dropped, no boundary of the half-oscillation), and is
counted in IntegrationResult.unresolved_turning_point_pairs, which is not stored.

The cases and their sources:
  - On constructed samples, through resolved_sign_changes and turning_point_tolerance (no
    integration; each well under a millisecond): an unresolved interior pair shaped like the one
    prompt 01 measured on the golden beta = 1.5 history on its new w table (README §8.1: pi at the
    nodes -3.97e-12 and -1.82e-9, an interior swing to +6.74e-10, atol = rtol = 1e-8) is dropped and
    counted; the same pair scaled above tau is kept; an excursion exactly tau is dropped and the next
    float above it kept; a pair between atol and tau that only the rtol term drops; a single change
    across the nodes kept below tau; three changes on one step; samples exactly 0.0, against the
    change finder of the code before 01a (copied here as reference_changes) on fixed cases and on 2000
    random sign patterns.
  - On integrations:
      - test_injection's item (11) (beta = 10, M = 0.5 M_P, alpha = 0.1, the parking cut off,
        PARKING_EPSILON = 0.0; on the w table before electroweak-eos prompt 01 since that prompt):
        its plateau and wall-side pair inside one step, with pi > 0 at both
        nodes, is real and is kept. Measured on 2026-11-12 before 01a's change (atol = rtol = 1e-8):
        the step [63.852649, 63.854452], pi at its nodes +5.14e-8 and +8.29e-6, the excursion (the
        dip) 1.1761e-6, tau 1.000008e-8, so the excursion is 117.6 tau. One integration, about 9 s at
        a load of 8.
      - a history started at rest with Sigma = 1 - 3w zero above T_cut = 386.7 GeV, as on prompt
        01's table, built without installing a table: the cosmology's w is patched inside the test
        (mock.patch.object on the instance), so that Sigma is the production table's less a ramp,
        linear in ln T, from 0 at 100 GeV to Sigma(T_cut) at T_cut, and zero above T_cut. Sigma is
        continuous, with a kink at T_cut, as the new table's spline is at its top row. The golden
        normalization, beta = 1.5, M = 1e-3 M_P, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to 100 GeV. Before
        01a's change this history records a wall at N = 3.9451 and a plateau at N = 3.9460 (T_J 387.1 and
        386.7 GeV): the step [3.944422, 3.946330] has pi -4.00e-12 and -1.68e-9 at its nodes and
        swings to +6.17e-10 between them, with tau 1.0000000017e-8. With 01a the pair is dropped and
        counted. At alpha = 0 and at alpha = 0.1; about 0.1 s each.
  - The field: appended last to IntegrationResult with default 0, and not a payload key.

No Ray, no store, no PRyMordial solve.
"""

import contextlib
import importlib
import io
import random
import unittest
from bisect import bisect_right
from math import exp, log, nextafter
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_injection as ti
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")

ATOL = 1e-8
RTOL = 1e-8

# ten samples on a unit step, as observe_turning_points lays them out: the two nodes and eight
# equally spaced interior points
NS = [float(x) for x in np.linspace(0.0, 1.0, SM.TURNING_POINT_INTERIOR_SAMPLES + 2)]


def reference_changes(Ns, pis):
    """
    The sign changes as observe_turning_points found them before 01a (its loop at 0479587, copied),
    as (N_before, N_after, N_zero, kind).
    """
    changes = []
    last_sign = 0
    last_j = None
    zero_j = None
    for j, pi_j in enumerate(pis):
        if pi_j == 0.0:
            if zero_j is None:
                zero_j = j
            continue
        sign = 1 if pi_j > 0.0 else -1
        if last_sign != 0 and sign != last_sign:
            changes.append((last_j, j, zero_j, "wall" if last_sign < 0 else "plateau"))
        last_sign = sign
        last_j = j
        zero_j = None
    return [
        (Ns[b], Ns[a], None if z is None else Ns[z], kind) for b, a, z, kind in changes
    ]


def as_tuples(kept):
    return [(c.N_before, c.N_after, c.N_zero, c.kind) for c in kept]


# the pair prompt 01 measured, in shape: pi < 0 at both nodes, a swing above zero over samples 3-5
EARLY_PAIR = [
    -3.97e-12,
    -4.0e-11,
    -1.1e-10,
    2.1e-10,
    6.74e-10,
    3.3e-10,
    -2.4e-10,
    -7.9e-10,
    -1.3e-9,
    -1.82e-9,
]


class TestTheRuleOnConstructedSamples(unittest.TestCase):

    def test_an_unresolved_interior_pair_is_dropped_and_counted(self):
        tau = SM.turning_point_tolerance(EARLY_PAIR[0], EARLY_PAIR[-1], ATOL, RTOL)
        self.assertLess(max(EARLY_PAIR), tau)
        self.assertEqual(len(reference_changes(NS, EARLY_PAIR)), 2)
        kept, dropped = SM.resolved_sign_changes(NS, EARLY_PAIR, tau)
        self.assertEqual(kept, [])
        self.assertEqual(dropped, 1)

    def test_the_same_pair_above_tau_is_kept(self):
        pis = [p * 100.0 if p > 0.0 else p for p in EARLY_PAIR]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        self.assertGreater(max(pis), tau)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 0)
        self.assertEqual(
            as_tuples(kept),
            [(NS[2], NS[3], None, "wall"), (NS[5], NS[6], None, "plateau")],
        )
        self.assertEqual(as_tuples(kept), reference_changes(NS, pis))

    def test_an_excursion_exactly_tau_is_dropped_and_the_next_float_kept(self):
        tau = SM.turning_point_tolerance(EARLY_PAIR[0], EARLY_PAIR[-1], ATOL, RTOL)
        pis = list(EARLY_PAIR)
        pis[4] = tau
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual((kept, dropped), ([], 1))
        pis[4] = nextafter(tau, 1.0)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 0)
        self.assertEqual([c.kind for c in kept], ["wall", "plateau"])

    def test_a_pair_between_atol_and_tau_is_dropped_by_the_rtol_term(self):
        """
        Nodes of -0.5 and -1.0 give tau = 1e-8 + 1e-8 * 1.0 = 2e-8; an excursion of 1.5e-8 lies
        between atol and tau, so only the rtol term drops it.
        """
        pis = [-0.5, -0.3, -1e-3, 1.0e-8, 1.5e-8, 1.2e-8, -1e-3, -0.4, -0.8, -1.0]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        self.assertEqual(tau, ATOL + RTOL * 1.0)
        self.assertTrue(ATOL < max(pis) < tau)
        self.assertEqual(SM.resolved_sign_changes(NS, pis, tau), ([], 1))
        # with atol alone the pair would be kept
        kept, dropped = SM.resolved_sign_changes(NS, pis, ATOL)
        self.assertEqual((len(kept), dropped), (2, 0))

    def test_the_tolerance_is_atol_plus_rtol_times_the_larger_node(self):
        self.assertEqual(
            SM.turning_point_tolerance(-3.0, 2.0, 1e-8, 1e-6), 1e-8 + 1e-6 * 3.0
        )
        self.assertEqual(
            SM.turning_point_tolerance(2.0, -3.0, 1e-8, 1e-6), 1e-8 + 1e-6 * 3.0
        )
        self.assertEqual(SM.turning_point_tolerance(0.0, 0.0, 1e-8, 1e-6), 1e-8)

    def test_a_single_change_across_the_nodes_is_kept_below_tau(self):
        pis = [
            -1e-12,
            -8e-13,
            -5e-13,
            -2e-13,
            -1e-14,
            1e-14,
            2e-13,
            5e-13,
            8e-13,
            1e-12,
        ]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        self.assertLess(max(abs(p) for p in pis), tau)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 0)
        self.assertEqual(as_tuples(kept), [(NS[4], NS[5], None, "wall")])

    def test_three_changes_with_the_first_pair_unresolved_keep_only_the_third(self):
        pis = [-1e-3, -1e-3, 1e-10, 1e-10, -1e-3, -1e-3, -1e-3, 1e-3, 1e-3, 1e-3]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        self.assertEqual(len(reference_changes(NS, pis)), 3)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 1)
        self.assertEqual(as_tuples(kept), [(NS[6], NS[7], None, "wall")])

    def test_three_changes_with_the_first_pair_resolved_keep_all_three(self):
        pis = [-1e-3, -1e-3, 1e-5, 1e-5, -1e-3, -1e-3, -1e-3, 1e-3, 1e-3, 1e-3]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 0)
        self.assertEqual(as_tuples(kept), reference_changes(NS, pis))
        self.assertEqual([c.kind for c in kept], ["wall", "plateau", "wall"])

    def test_a_kept_change_is_followed_by_the_test_of_the_next_pair(self):
        """A resolved first change, then an unresolved dip: the walk goes on at i + 1 and drops 1, 2."""
        pis = [-1e-3, 1e-3, 1e-3, -1e-10, 1e-3, 1e-3, 1e-3, 1e-3, 1e-3, 1e-3]
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual(dropped, 1)
        self.assertEqual(as_tuples(kept), [(NS[0], NS[1], None, "wall")])

    def test_samples_exactly_zero_are_treated_as_before(self):
        cases = [
            # a change through one zero sample: the zero is the turning point
            [-1.0, -1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            # through two: the first of them
            [-1.0, -1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            # a touch: no change
            [-1.0, -1.0, 0.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
            # zero at a node
            [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1.0],
            [-1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
            # all zero
            [0.0] * 10,
        ]
        for pis in cases:
            with self.subTest(pis=pis):
                kept, dropped = SM.resolved_sign_changes(NS, pis, 0.0)
                self.assertEqual(dropped, 0)
                self.assertEqual(as_tuples(kept), reference_changes(NS, pis))
        self.assertEqual(
            as_tuples(SM.resolved_sign_changes(NS, cases[1], 0.0)[0]),
            [(NS[1], NS[4], NS[2], "wall")],
        )
        self.assertEqual(SM.resolved_sign_changes(NS, cases[2], 0.0), ([], 0))

    def test_a_pair_through_zero_samples_is_tested_like_any_other(self):
        """The excursion is over the samples of the other sign; zero samples carry none."""
        pis = [-1e-3, 0.0, 1e-12, 0.0, -1e-3, -1e-3, -1e-3, -1e-3, -1e-3, -1e-3]
        self.assertEqual(
            reference_changes(NS, pis),
            [(NS[0], NS[2], NS[1], "wall"), (NS[2], NS[4], NS[3], "plateau")],
        )
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        self.assertEqual(SM.resolved_sign_changes(NS, pis, tau), ([], 1))
        pis[2] = 1e-6
        kept, dropped = SM.resolved_sign_changes(NS, pis, tau)
        self.assertEqual((as_tuples(kept), dropped), (reference_changes(NS, pis), 0))

    def test_with_no_pair_dropped_the_changes_are_the_old_codes(self):
        """
        2000 random sign patterns (seed 20261112) of samples in {-, 0, +} with |pi| >= 1, at
        tau = 0.0, where no excursion can be dropped: the changes are reference_changes', exactly.
        """
        rng = random.Random(20261112)
        for case in range(2000):
            pis = [
                rng.choice((-1.0, 0.0, 1.0)) * (1.0 + rng.random()) for _ in range(10)
            ]
            kept, dropped = SM.resolved_sign_changes(NS, pis, 0.0)
            self.assertEqual(dropped, 0, case)
            self.assertEqual(as_tuples(kept), reference_changes(NS, pis), case)


class TestTheField(unittest.TestCase):

    def test_it_is_appended_last_with_default_zero(self):
        self.assertEqual(
            SM.IntegrationResult._fields[-1], "unresolved_turning_point_pairs"
        )
        standin = SM.IntegrationResult(
            *[None]
            * (
                len(SM.IntegrationResult._fields)
                - len(SM.IntegrationResult._field_defaults)
            )
        )
        self.assertEqual(standin.unresolved_turning_point_pairs, 0)


class TestTheInteriorPairOfItem11IsKept(unittest.TestCase):
    """
    test_injection's item (11): the plateau and wall-side pair inside one step, at beta = 10,
    M = 0.5 M_P, alpha = 0.1, with the parking cut off. One integration, about 9 s.

    Since electroweak-eos prompt 01 it is integrated, as test_injection's item (11) is, on the w table
    before that prompt (ti.on_the_old_w_table), by the user's ruling of 2026-11-12. On the new table
    the history holds no interior pair, a table effect (log 01a §6), so the case is kept on the table
    where it exists.
    """

    @classmethod
    def setUpClass(cls):
        with mock.patch.object(SM, "PARKING_EPSILON", 0.0), ti.on_the_old_w_table():
            cls.data, cls.result, _ = ti.integrate_captured(10.0, 0.5, ti.ALPHA)
        cls.ts = list(cls.result.solution.ts)
        cls.interpolants = cls.result.solution.interpolants

    def test_the_pair_is_kept_with_its_excursion_above_tau(self):
        per_step = {}
        for t in self.result.turning_points:
            k = min(bisect_right(self.ts, t.N) - 1, len(self.interpolants) - 1)
            per_step.setdefault(k, []).append(t)
        pairs = {k: v for k, v in per_step.items() if len(v) == 2}
        self.assertEqual(len(pairs), 1)
        ((k, pair),) = pairs.items()
        self.assertEqual([t.kind for t in pair], ["plateau", "wall"])

        interpolant = self.interpolants[k]
        t_low, t_high = self.ts[k], self.ts[k + 1]
        N_interior = np.linspace(t_low, t_high, SM.TURNING_POINT_INTERIOR_SAMPLES + 2)[
            1:-1
        ]
        Ns = [t_low] + [float(x) for x in N_interior] + [t_high]
        pis = (
            [interpolant(t_low)[1]]
            + [float(x) for x in interpolant(N_interior)[1]]
            + [interpolant(t_high)[1]]
        )
        self.assertGreater(pis[0], 0.0)
        self.assertGreater(pis[-1], 0.0)
        tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
        excursion = max(-p for p in pis if p < 0.0)
        # measured 1.1761e-6 against 1.000008e-8 (module docstring)
        self.assertGreater(excursion, 100.0 * tau)
        kept, dropped = SM.resolved_sign_changes(Ns, pis, tau)
        self.assertEqual(dropped, 0)
        self.assertEqual([c.kind for c in kept], ["plateau", "wall"])

    def test_no_pair_is_dropped_on_this_history(self):
        self.assertEqual(self.result.unresolved_turning_point_pairs, 0)


T_CUT_GEV = 386.7
T_RAMP_GEV = 100.0
BETA_AT_REST = 1.5
M_AT_REST = 1e-3


def integrate_with_sigma_switched_off(alpha: float):
    """
    The module docstring's history: Sigma zero above T_CUT_GEV, through a patch of the cosmology's w.
    Returns (payload, IntegrationResult, the step's (Ns, pis) where Sigma switches on).
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    GeV, Mp = units.GeV, units.PlanckMass
    w = cosmology.w
    T_cut, T_ramp = T_CUT_GEV * GeV, T_RAMP_GEV * GeV
    sigma_cut = 1.0 - 3.0 * w(T_cut)

    def w_patched(T):
        T = float(T)
        if T >= T_cut:
            return 1.0 / 3.0
        if T <= T_ramp:
            return w(T)
        x = (log(T) - log(T_ramp)) / (log(T_cut) - log(T_ramp))
        sigma = (1.0 - 3.0 * w(T)) - sigma_cut * x
        return (1.0 - sigma) / 3.0

    model = ExponentialModel(
        0, M_value(0, M_AT_REST * Mp), 1, beta_value(0, BETA_AT_REST), units
    )
    realized = model.realize(gh.normalization(units), 0.0)
    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        result = integrate(*args, **kwargs)
        captured.append(result)
        return result

    with mock.patch.object(cosmology, "w", w_patched), mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, 2.0e4 * GeV),
            temperature(1, T_ramp),
            phi_value(0, 5.0 * Mp),
            pi_value(0, 0.0),
            gh.make_z_grid(),
            realized,
            task_label="test_resolved_turning_points",
            alpha=alpha,
            compute_sample_masses=False,
        )
    result = captured[-1]

    # the step on which ln T_J crosses ln T_cut
    ts = list(result.solution.ts)
    step = None
    for k, interpolant in enumerate(result.solution.interpolants):
        if interpolant(ts[k])[4] >= log(T_cut) > interpolant(ts[k + 1])[4]:
            N_interior = np.linspace(
                ts[k], ts[k + 1], SM.TURNING_POINT_INTERIOR_SAMPLES + 2
            )[1:-1]
            Ns = [ts[k]] + [float(x) for x in N_interior] + [ts[k + 1]]
            pis = (
                [interpolant(ts[k])[1]]
                + [float(x) for x in interpolant(N_interior)[1]]
                + [interpolant(ts[k + 1])[1]]
            )
            step = (Ns, pis)
            break
    return data, result, step


class TestAHistoryStartedAtRestWithSigmaSwitchedOff(unittest.TestCase):
    """
    The early pair of prompt 01's table, made on the old table by a patched Sigma (module docstring).
    Two integrations to 100 GeV, about 0.1 s each.
    """

    @classmethod
    def setUpClass(cls):
        cls.runs = {
            alpha: integrate_with_sigma_switched_off(alpha) for alpha in (0.0, 0.1)
        }

    def test_the_step_where_sigma_switches_on_shows_an_unresolved_pair(self):
        for alpha, (data, result, step) in self.runs.items():
            with self.subTest(alpha=alpha):
                self.assertFalse(data.get("failure", False))
                self.assertIsNotNone(step)
                Ns, pis = step
                self.assertLess(pis[0], 0.0)
                self.assertLess(pis[-1], 0.0)
                self.assertEqual(
                    [c[3] for c in reference_changes(Ns, pis)], ["wall", "plateau"]
                )
                tau = SM.turning_point_tolerance(pis[0], pis[-1], ATOL, RTOL)
                self.assertLess(max(pis), tau)

    def test_it_gives_no_turning_point_and_counts_the_pair(self):
        for alpha, (data, result, step) in self.runs.items():
            with self.subTest(alpha=alpha):
                self.assertEqual(result.turning_points, [])
                self.assertEqual(result.unresolved_turning_point_pairs, 1)
                self.assertIsNone(data["first_bounce"])

    def test_it_arms_and_drops_no_drain(self):
        for alpha, (data, result, step) in self.runs.items():
            with self.subTest(alpha=alpha):
                self.assertEqual(result.dropped_drains, 0)
                self.assertEqual(result.injections, [])

    def test_the_count_is_not_a_payload_key(self):
        data, _, _ = self.runs[0.0]
        self.assertNotIn("unresolved_turning_point_pairs", data)


if __name__ == "__main__":
    unittest.main()
