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
The tuning layer (jordan-normalization prompt 06, T1-T7; orchestrator note 06, ruling 11).

tune_history integrates a history to T_J = T_CMB and adjusts (normalization, phi_ref) until
H_J(T_CMB) = H0 and phi_ref = phi_min(T_CMB). These tests drive it with no Ray and no store, at
the tests' point (golden_tuned_history.py's setup: beta = 3, M = 0.01 M_P, phi* = 5 M_P, pi* = 0,
T* = 2e4 GeV, atol = rtol = 1e-8, main.py's default sampling axis), or at a subclass of the model
there. Each integration to T_CMB costs about 5 s on an unloaded machine (12-19 s at a load of
60-90); each test's docstring gives its count.

  (a) tuning_converged: both residuals inside the tolerance; each outside alone; the boundary.
  (b) TuningSecant, the step, on synthetic maps with no integration.
  (c) a converged history (one tuning in setUpClass with progress="every": five integrations,
      about 25 s unloaded): the residuals, A(phi_ref) = 1, V_ref, the residuals recomputed from
      the last sample, the seed, the step counts, and the progress lines against RunDrivers'
      StageTracker.
  (d) determinism: a second tuning, with the default progress and clock, equals (c)'s bitwise
      (about 25 s unloaded), and prints no tune line unless it ran 120 s or more (then it skips).
  (e) the tuned golden history: (c)'s output against data/golden_history_06_tuned.json.
  (f)-(j) the failures: the cap, no normalization reaching H0, the seed being the model's own,
      the effective minimum not found, and an integration's own failure (f)-(h) with
      progress="every".
  (k)-(n) the quiet tune lines (tuning-followups prompt 02; README §6.0 R2):
      (k) tune_line_due, on numbers (milliseconds);
      (l) TuningSecant.reset, on synthetic maps (milliseconds);
      (m) tune_history's wiring with the integration and the state scripted (milliseconds);
      (n) a real tuning with a fake clock advancing 200 s per call (about 25 s unloaded): a line
          after every integration, in (c)'s form, the outputs bitwise (c)'s.
"""

import contextlib
import importlib
import io
import json
import re
import unittest
from time import perf_counter
from math import exp, sqrt
from unittest import mock

from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_TOLERANCE,
    TUNE_LINE_INTERVAL_S,
    TUNE_LINE_SLOW_INTEGRATIONS,
    TUNING_OUTPUT_KEYS,
    SampleValues,
    TuningSecant,
    TuningState,
    tune_line_due,
    tuning_converged,
)
from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests.golden_history import record_of
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from RunDrivers.registered_run import StageTracker
from config.defaults import DEFAULT_STRING_LENGTH
from pipeline_selection import summarise_failure_reasons

# the module (ComputeTargets' package exports the class of the same name)
SM = importlib.import_module("ComputeTargets.ScalarHistory")

# ruling 9's progress line
PROGRESS = re.compile(
    r"^-- tune (?P<label>\S+) it=(?P<it>\d+) kind=(?P<kind>seed|plain|secant|guard) "
    r"H/H0-1=(?P<rH>[+-]\d\.\d{3}e[+-]\d{2}) "
    r"phi_ref/phi_min-1=(?P<rphi>[+-]\d\.\d{3}e[+-]\d{2}) "
    r"norm=(?P<norm>\S+) phi_ref=(?P<phi_ref>[+-]?\d\.\d{10}e[+-]\d{2}) "
    r"wall=(?P<wall>\d+\.\d)$"
)

MINUS_I = [[-1.0, 0.0], [0.0, -1.0]]


def _tune(**kwargs):
    """Run golden_tuned_history.run_tuning with stdout captured; (payload, progress lines)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        data = gth.run_tuning(**kwargs)
    lines = [l for l in out.getvalue().splitlines() if l.startswith("-- tune ")]
    return data, lines


def _first_clause(reason: str) -> str:
    groups = summarise_failure_reasons([reason])
    assert len(groups) == 1
    return groups[0][0]


class _NoMinimumModel(ExponentialModel):
    """
    note 06's §0: effective_minimum returns phi_0 - M_P/beta, so that after the first iterate
    A(phi_0) = e and Eq. 7's V_target is negative at every iterate.
    """

    def effective_minimum(self, normalization, phi_ref, phi_0, rho_m_E_0, bracket=None):
        return phi_0 - self.units.PlanckMass / float(self._beta)


class _RaisingMinimumModel(ExponentialModel):
    """effective_minimum raises the ValueError that a bracket with no sign change raises."""

    def effective_minimum(self, normalization, phi_ref, phi_0, rho_m_E_0, bracket=None):
        raise ValueError("a test model's effective minimum has no bracket")


class TestConvergenceTest(unittest.TestCase):
    """(a) tuning_converged, on numbers (no integration)."""

    def test_a_both_inside(self):
        self.assertTrue(tuning_converged(0.0, 0.0, 1.0e-5))
        self.assertTrue(tuning_converged(-9.9e-6, 9.9e-6, 1.0e-5))

    def test_a_each_outside_alone(self):
        self.assertFalse(tuning_converged(2.0e-5, 0.0, 1.0e-5))
        self.assertFalse(tuning_converged(-2.0e-5, 0.0, 1.0e-5))
        self.assertFalse(tuning_converged(0.0, 2.0e-5, 1.0e-5))
        self.assertFalse(tuning_converged(0.0, -2.0e-5, 1.0e-5))

    def test_a_the_tolerance_itself_is_outside(self):
        self.assertFalse(tuning_converged(1.0e-5, 0.0, 1.0e-5))
        self.assertFalse(tuning_converged(0.0, -1.0e-5, 1.0e-5))
        self.assertFalse(tuning_converged(1.0e-5, 1.0e-5, 1.0e-5))


class TestTheStepper(unittest.TestCase):
    """(b) TuningSecant on synthetic maps: no integration, no model (milliseconds)."""

    X0 = (0.75, 1.25)
    G0 = (0.5, 1.0)
    G1 = (0.45, 0.99)

    def _plain_then_secant(self):
        s = TuningSecant()
        x1, kind0 = s.step(self.X0, self.G0)
        x2, kind1 = s.step(x1, self.G1)
        return s, x1, kind0, x2, kind1

    def test_b_the_first_step_is_the_plain_map_bitwise(self):
        s = TuningSecant()
        self.assertIsNone(s.B)
        x1, kind = s.step(self.X0, self.G0)
        self.assertEqual(kind, "plain")
        self.assertEqual([v.hex() for v in x1], [v.hex() for v in self.G0])
        self.assertEqual(s.B, MINUS_I)

    def test_b_the_second_step_is_a_secant(self):
        s, x1, _, x2, kind = self._plain_then_secant()
        self.assertEqual(kind, "secant")
        B = s.B
        # Broyden's update satisfies the secant condition B dx = dF
        F0 = (self.G0[0] - self.X0[0], self.G0[1] - self.X0[1])
        F1 = (self.G1[0] - x1[0], self.G1[1] - x1[1])
        dx = (x1[0] - self.X0[0], x1[1] - self.X0[1])
        dF = (F1[0] - F0[0], F1[1] - F0[1])
        for i in range(2):
            self.assertAlmostEqual(B[i][0] * dx[0] + B[i][1] * dx[1], dF[i], places=14)
        # and the step solves B d = -F
        d = (x2[0] - x1[0], x2[1] - x1[1])
        for i in range(2):
            self.assertAlmostEqual(B[i][0] * d[0] + B[i][1] * d[1], -F1[i], places=14)
        self.assertNotEqual(x2, self.G1)

    def test_b_after_guard_the_next_step_is_plain_and_B_is_minus_identity(self):
        s, _, _, x2, _ = self._plain_then_secant()
        self.assertNotEqual(s.B, MINUS_I)
        s.guard()
        self.assertIsNone(s.B)
        G2 = (0.44, 0.985)
        x3, kind = s.step(x2, G2)
        self.assertEqual(kind, "plain")
        self.assertEqual(x3, G2)
        self.assertEqual(s.B, MINUS_I)

    def test_b_the_twice_rule(self):
        s, x1, _, x2, _ = self._plain_then_secant()
        F1 = sqrt((self.G1[0] - x1[0]) ** 2 + (self.G1[1] - x1[1]) ** 2)
        # at the point the secant step produced, a residual three times the last
        G2 = (x2[0] + 3.0 * F1, x2[1])
        x3, kind = s.step(x2, G2)
        self.assertEqual(kind, "plain")
        self.assertEqual(x3, G2)
        self.assertEqual(s.B, MINUS_I)

    def test_b_the_secant_needs_fewer_steps_on_an_affine_contraction(self):
        A = ((0.6, 0.3), (-0.2, 0.7))
        b = (0.1, -0.4)

        def G(x):
            return (
                A[0][0] * x[0] + A[0][1] * x[1] + b[0],
                A[1][0] * x[0] + A[1][1] * x[1] + b[1],
            )

        def steps(secant):
            s = TuningSecant(secant=secant)
            x = (1.0, -1.0)
            for n in range(1000):
                g = G(x)
                if sqrt((g[0] - x[0]) ** 2 + (g[1] - x[1]) ** 2) < 1.0e-12:
                    return n, x
                x, _ = s.step(x, g)
            self.fail("no convergence in 1000 steps")

        n_plain, x_plain = steps(False)
        n_secant, x_secant = steps(True)
        self.assertLess(n_secant, n_plain)
        for i in range(2):
            self.assertAlmostEqual(x_secant[i], x_plain[i], places=10)


class TestTheResetReport(unittest.TestCase):
    """(l) TuningSecant.reset, on (b)'s synthetic maps: no integration (milliseconds)."""

    X0 = TestTheStepper.X0
    G0 = TestTheStepper.G0
    G1 = TestTheStepper.G1

    def _through_the_reset(self):
        """Steps 1 (plain), 2 (secant), 3 (the twice rule's reset); the stepper and x3."""
        s = TuningSecant()
        x1, _ = s.step(self.X0, self.G0)
        x2, _ = s.step(x1, self.G1)
        F1 = sqrt((self.G1[0] - x1[0]) ** 2 + (self.G1[1] - x1[1]) ** 2)
        x3, kind = s.step(x2, (x2[0] + 3.0 * F1, x2[1]))
        return s, x3, kind

    def test_l_false_before_any_step_and_after_plain_and_secant_steps(self):
        s = TuningSecant()
        self.assertFalse(s.reset)
        x1, kind0 = s.step(self.X0, self.G0)
        self.assertEqual(kind0, "plain")
        self.assertFalse(s.reset)
        _, kind1 = s.step(x1, self.G1)
        self.assertEqual(kind1, "secant")
        self.assertFalse(s.reset)

    def test_l_true_after_the_twice_rule_fires_and_false_again_after(self):
        s, x3, kind = self._through_the_reset()
        # a reset step is still a plain step: the stored step counts do not change
        self.assertEqual(kind, "plain")
        self.assertTrue(s.reset)
        # the next step (a plain one from the reset B = -I, then a secant) is not a reset
        x4, kind4 = s.step(x3, (x3[0] - 0.01, x3[1]))
        self.assertEqual(kind4, "secant")
        self.assertFalse(s.reset)

    def test_l_false_after_guard(self):
        s, _, _ = self._through_the_reset()
        self.assertTrue(s.reset)
        s.guard()
        self.assertFalse(s.reset)
        _, kind = s.step(self.X0, self.G0)
        self.assertEqual(kind, "plain")
        self.assertFalse(s.reset)


class TestTheTuneLineRule(unittest.TestCase):
    """(k) tune_line_due, on numbers (milliseconds); and (m) tune_history's wiring."""

    def due(self, progress="quiet", k=0, kind="plain", reset=False, now=0.0, last=0.0):
        return tune_line_due(progress, k, kind, reset, now, last)

    def test_k_no_trigger_prints_nothing(self):
        self.assertFalse(self.due(k=0, kind="seed", now=0.0, last=0.0))
        self.assertFalse(self.due(k=3, kind="secant", now=119.0, last=0.0))

    def test_k_the_interval_from_the_start(self):
        self.assertFalse(self.due(now=TUNE_LINE_INTERVAL_S - 0.001, last=0.0))
        self.assertTrue(self.due(now=TUNE_LINE_INTERVAL_S, last=0.0))
        self.assertTrue(self.due(now=500.0, last=0.0))

    def test_k_the_interval_from_a_later_last_line(self):
        self.assertFalse(
            self.due(now=1000.0 + TUNE_LINE_INTERVAL_S - 0.001, last=1000.0)
        )
        self.assertTrue(self.due(now=1000.0 + TUNE_LINE_INTERVAL_S, last=1000.0))
        # a long time since the start is not a long time since the last line
        self.assertFalse(self.due(now=1050.0, last=1000.0))

    def test_k_the_slow_integrations(self):
        n = TUNE_LINE_SLOW_INTEGRATIONS
        self.assertFalse(self.due(k=n - 2))
        self.assertTrue(self.due(k=n - 1))
        self.assertTrue(self.due(k=n))
        self.assertTrue(self.due(k=n + 5))

    def test_k_a_guard_step_alone(self):
        self.assertTrue(self.due(kind="guard"))
        self.assertFalse(self.due(kind="plain"))

    def test_k_a_reset_alone(self):
        self.assertTrue(self.due(kind="plain", reset=True))
        self.assertFalse(self.due(kind="plain", reset=False))

    def test_k_every_always_prints(self):
        self.assertTrue(self.due(progress="every", k=0, kind="seed", now=0.0, last=0.0))
        self.assertTrue(
            self.due(progress="every", k=2, kind="secant", now=1.0, last=0.0)
        )

    def test_k_an_unknown_progress_raises(self):
        for bad in ("loud", "", None, "QUIET"):
            with self.assertRaises(ValueError):
                self.due(progress=bad)


class _FakeClock:
    """A clock advancing `step` seconds per call, the first call returning 0."""

    def __init__(self, step):
        self.step, self.calls = step, 0

    def __call__(self):
        t = self.calls * self.step
        self.calls += 1
        return t


class _StubStepper:
    """
    TuningSecant's interface with plain steps only (the scripted states are constants, which
    would give the real secant a zero step), reporting a reset after its `reset_after`-th step
    (never, by default).
    """

    reset_after = None

    def __init__(self, secant=True):
        self.n, self.reset = 0, False

    def step(self, x, G):
        self.n += 1
        self.reset = self.n == self.reset_after
        return (G[0], G[1]), "plain"

    def guard(self):
        self.reset = False


class _ResetsAfterTwoSteps(_StubStepper):
    reset_after = 2


class TestTheWiring(unittest.TestCase):
    """
    (m) tune_history with integrate_history and _tuning_state patched to scripted results: no
    integration (milliseconds). The model and the cosmology are the real ones; the states are
    unconverged (residuals 1e-2) with a positive V_target unless a case says otherwise, TuningSecant
    is a stub of plain steps (which reports a reset where a case says so), so a
    tuning of N integrations ends in the cap's failure after the N-th line decision, and the
    tune lines are those that began "-- tune ".
    """

    @classmethod
    def setUpClass(cls):
        cls.cosmology = gth.make_cosmology()
        cls.units = cls.cosmology.units
        cls.z_grid = gth.make_z_grid()

    def _state(self, V_target=1.0):
        Mp = self.units.PlanckMass
        return TuningState(
            phi_0=5.0 * Mp,
            pi_0=0.0,
            log_V=-10.0,
            V_target=V_target * Mp**4,
            phi_min=4.0 * Mp,
            residual_H=1.0e-2,
            residual_phi=1.0e-2,
        )

    def _run(self, n, bad=(), stepper=_StubStepper, **kwargs):
        """
        n integrations; V_target <= 0 at the integrations in `bad`. Returns (lines, calls to
        integrate_history).
        """
        states = [self._state(-1.0 if k in bad else 1.0) for k in range(n)]
        with contextlib.ExitStack() as stack:
            integrate = stack.enter_context(
                mock.patch.object(SM, "integrate_history", return_value={"sample": []})
            )
            stack.enter_context(
                mock.patch.object(SM, "_tuning_state", side_effect=states)
            )
            stack.enter_context(mock.patch.object(SM, "TuningSecant", stepper))
            data, lines = _tune(
                cosmology=self.cosmology,
                z_grid=self.z_grid,
                max_integrations=n,
                **kwargs,
            )
        self.assertTrue(data["failure"])
        self.assertEqual(
            _first_clause(data["failure_reason"]), "the normalization did not converge"
        )
        return lines, integrate.call_count

    @staticmethod
    def _its(lines):
        return [int(PROGRESS.match(l).group("it")) for l in lines]

    def test_m_the_default_is_quiet_five_integrations_no_line(self):
        """The claim that main.py's path (the default progress) prints nothing when fast."""
        lines, calls = self._run(5, clock=lambda: 0.0)
        self.assertEqual(calls, 5)
        self.assertEqual(lines, [])

    def test_m_a_guard_step_prints_its_integrations_line_alone(self):
        lines, _ = self._run(5, bad=(2,), clock=lambda: 0.0)
        self.assertEqual(self._its(lines), [3])
        self.assertEqual(PROGRESS.match(lines[0]).group("kind"), "guard")

    def test_m_a_reset_prints_its_integrations_line_alone(self):
        # the stub reports a reset after its second step, which reaches integration 2
        lines, _ = self._run(5, stepper=_ResetsAfterTwoSteps, clock=lambda: 0.0)
        self.assertEqual(self._its(lines), [2])
        self.assertEqual(PROGRESS.match(lines[0]).group("kind"), "plain")

    def test_m_twelve_integrations_frozen_clock_lines_from_the_tenth(self):
        lines, calls = self._run(12, clock=lambda: 0.0)
        self.assertEqual(calls, 12)
        self.assertEqual(self._its(lines), [9, 10, 11])

    def test_m_a_clock_advancing_50_s_per_call(self):
        """
        The clock is called once before the loop and once before and after each integration, so
        integration k ends at 50 (2k + 2) s and wall = 50.0. The tuning starts at 0 s. Integration
        0 ends at 100 s (100 < 120, no line); 1 at 200 s (200 s since the start: line, last = 200);
        2 at 300 s (100 since: no); 3 at 400 s (200: line); and so on, a line at every odd k. From
        k = 9 the slow rule prints k = 10 and 11 as well: lines at 1, 3, 5, 7, 9, 10, 11.
        """
        lines, _ = self._run(12, clock=_FakeClock(50.0))
        self.assertEqual(self._its(lines), [1, 3, 5, 7, 9, 10, 11])
        self.assertEqual({PROGRESS.match(l).group("wall") for l in lines}, {"50.0"})

    def test_m_every_prints_a_line_per_integration(self):
        lines, _ = self._run(5, progress="every", clock=lambda: 0.0)
        self.assertEqual(self._its(lines), [0, 1, 2, 3, 4])

    def test_m_an_unknown_progress_raises_before_the_seed(self):
        with mock.patch.object(SM, "integrate_history") as integrate:
            with self.assertRaises(ValueError):
                _tune(cosmology=self.cosmology, z_grid=self.z_grid, progress="loud")
        integrate.assert_not_called()


class TestAConvergedHistory(unittest.TestCase):
    """
    (c), (d), (e), (n). setUpClass tunes the tests' point once, with progress="every": five
    integrations to T_CMB, about 25 s unloaded. (d) and (n) tune it again (another 25 s each).
    """

    @classmethod
    def setUpClass(cls):
        cls.cosmology = gth.make_cosmology()
        cls.units = cls.cosmology.units
        cls.model = gth.make_model(cls.units)
        cls.data, cls.lines = _tune(
            cosmology=cls.cosmology, model=cls.model, progress="every"
        )
        if cls.data.get("failure", False):
            raise AssertionError(f"the tuning failed: {cls.data['failure_reason']}")

    def test_c_both_stored_residuals_are_inside_the_tolerance(self):
        d = self.data
        self.assertLess(abs(d["residual_H"]), DEFAULT_TUNING_TOLERANCE)
        self.assertLess(abs(d["residual_phi"]), DEFAULT_TUNING_TOLERANCE)
        self.assertTrue(
            tuning_converged(
                d["residual_H"], d["residual_phi"], DEFAULT_TUNING_TOLERANCE
            )
        )

    def test_c_A_of_phi_ref_is_one_exactly_and_V_ref_is_the_realizations(self):
        d = self.data
        rm = self.model.realize(d["normalization"], d["phi_ref"])
        self.assertEqual(rm.log_Omega(d["phi_ref"]), 0.0)
        self.assertEqual(d["V_ref"], exp(rm.log_V(d["phi_ref"])))

    def test_c_the_residuals_recomputed_from_the_last_sample_are_the_stored_ones(self):
        d = self.data
        rm = self.model.realize(d["normalization"], d["phi_ref"])
        Mp = self.units.PlanckMass
        H0 = self.cosmology.H0
        s = SampleValues._make(tuple(d["sample"][-1]))
        phi0, pi0 = s.phi_Einstein, s.pi_Einstein
        A0 = rm.Omega(phi0)
        rho_RJ = exp(s.log_rhorad_Jordan)
        fm = exp(s.log_fm)
        rho_mE0 = A0**4 * fm * rho_RJ
        phi_min = self.model.effective_minimum(
            d["normalization"], d["phi_ref"], phi0, rho_mE0
        )
        self.assertEqual((s.H_Jordan / H0 - 1.0).hex(), d["residual_H"].hex())
        self.assertEqual((d["phi_ref"] / phi_min - 1.0).hex(), d["residual_phi"].hex())
        self.assertEqual(float(phi0), d["phi_0"])
        self.assertEqual(float(pi0), d["pi_0"])
        self.assertEqual(float(pi0 / (1.5 * phi0)), d["tracking"])

    def test_c_the_seed_is_the_models(self):
        seed = self.model.seed(self.cosmology)
        self.assertEqual(self.data["seed_kind"], seed.kind)
        self.assertEqual(self.data["seed_normalization"], seed.normalization)
        self.assertEqual(self.data["seed_phi_ref"], seed.phi_ref)

    def test_c_the_outputs_and_the_step_counts(self):
        d = self.data
        for key in TUNING_OUTPUT_KEYS:
            self.assertIn(type(d[key]), (float, int, str), key)
        self.assertEqual(
            d["tuning_iterations"],
            1 + d["plain_steps"] + d["secant_steps"] + d["guard_steps"],
        )
        self.assertFalse(d.get("failure", False))
        self.assertGreater(len(d["sample"]), 0)

    def test_c_one_progress_line_per_integration_that_the_tracker_ignores(self):
        self.assertEqual(len(self.lines), self.data["tuning_iterations"])
        tracker = StageTracker()
        for k, line in enumerate(self.lines):
            m = PROGRESS.match(line)
            self.assertIsNotNone(m, line)
            self.assertEqual(int(m.group("it")), k)
            self.assertEqual(m.group("label"), gth.TASK_LABEL)
            self.assertEqual(m.group("kind") == "seed", k == 0)
            self.assertFalse(tracker.feed(line), line)
            self.assertIsNone(tracker.stage)
        # the last line is the converged integration's
        last = PROGRESS.match(self.lines[-1])
        self.assertEqual(last.group("rH"), f"{self.data['residual_H']:+.3e}")
        self.assertEqual(last.group("rphi"), f"{self.data['residual_phi']:+.3e}")
        self.assertEqual(last.group("norm"), f"{self.data['normalization']:.12g}")
        self.assertEqual(
            last.group("phi_ref"),
            f"{self.data['phi_ref'] / self.units.PlanckMass:.10e}",
        )

    def _assert_bitwise_the_same(self, again):
        self.assertEqual(record_of(again), record_of(self.data))
        for key in TUNING_OUTPUT_KEYS:
            a, b = again[key], self.data[key]
            self.assertIs(type(a), type(b), key)
            if isinstance(a, float):
                self.assertEqual(a.hex(), b.hex(), key)
            else:
                self.assertEqual(a, b, key)

    def test_d_a_second_tuning_is_bitwise_the_same(self):
        """
        (d) Five more integrations, about 25 s unloaded, with the default progress and clock
        (tuning-followups prompt 02): the bitwise assertions come first, and show that progress
        moves no value. Then, if the tuning ran under TUNE_LINE_INTERVAL_S by the test's own
        clock, it printed no tune line; if not, the interval was genuinely due, a line is right,
        and the test skips (the bitwise check has already passed).
        """
        start = perf_counter()
        again, lines = _tune(cosmology=self.cosmology, model=self.model)
        elapsed = perf_counter() - start
        self._assert_bitwise_the_same(again)
        if elapsed >= TUNE_LINE_INTERVAL_S:
            self.skipTest(
                f"the tuning took {elapsed:.0f} s, past the {TUNE_LINE_INTERVAL_S:.0f} s "
                f"interval, so a line is due; the bitwise check passed"
            )
        self.assertEqual(lines, [])

    def test_n_a_fake_clock_prints_every_line_and_moves_no_value(self):
        """
        (n) Five more integrations, about 25 s unloaded, with the default progress and a clock
        advancing 200 s per call: every integration ends at least 120 s after the last line, so
        five lines, it 0-4, in (c)'s form with wall=200.0, the kinds of the golden record's counts,
        and the outputs bitwise (c)'s.
        """
        again, lines = _tune(
            cosmology=self.cosmology, model=self.model, clock=_FakeClock(200.0)
        )
        self._assert_bitwise_the_same(again)
        self.assertEqual(len(lines), 5)
        ms = [PROGRESS.match(l) for l in lines]
        self.assertTrue(all(m is not None for m in ms), lines)
        self.assertEqual([int(m.group("it")) for m in ms], [0, 1, 2, 3, 4])
        self.assertEqual(
            [m.group("kind") for m in ms],
            ["seed", "plain", "secant", "secant", "secant"],
        )
        self.assertEqual({m.group("wall") for m in ms}, {"200.0"})
        # the lines are (c)'s but for the wall
        strip = lambda l: l.rsplit(" wall=", 1)[0]
        self.assertEqual([strip(l) for l in lines], [strip(l) for l in self.lines])

    def test_e_the_tuned_golden_history(self):
        """(e) (c)'s output against data/golden_history_06_tuned.json (no integration)."""
        golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
        messages = gth.differences(golden, gth.build_record(self.data))
        if messages:
            self.fail(
                f"the tuned golden history differs from {gth.GOLDEN_TUNED_PATH.name}:\n  "
                + "\n  ".join(messages)
                + f"\nrecorded environment: {golden['provenance']['environment']}"
                + f"\ncurrent environment:  {gth.environment()}"
                + f"\n(generated at {golden['provenance']['generated_at_commit']}; "
                f"regenerate only deliberately: {gth.REGENERATE_COMMAND})"
            )
        self.assertEqual(sorted(golden["tuning"]), sorted(TUNING_OUTPUT_KEYS))


class TestTheFailures(unittest.TestCase):
    """(f)-(j): the failure reasons, each with its own first clause."""

    @classmethod
    def setUpClass(cls):
        cls.cosmology = gth.make_cosmology()
        cls.units = cls.cosmology.units
        cls.z_grid = gth.make_z_grid()

    def test_f_the_cap(self):
        """(f) max_integrations=2: two integrations, about 10 s unloaded."""
        data, lines = _tune(
            cosmology=self.cosmology,
            z_grid=self.z_grid,
            max_integrations=2,
            progress="every",
        )
        self.assertTrue(data["failure"])
        reason = data["failure_reason"]
        self.assertEqual(_first_clause(reason), "the normalization did not converge")
        self.assertIn("after 2 integrations", reason)
        self.assertEqual(len(lines), 2)

    def test_g_no_normalization_reaches_H0(self):
        """
        (g) note 06's §0 subclass: V_target <= 0 at integrations 1-4, and the failure after the
        fifth. Five integrations, about 25 s unloaded.
        """
        model = gth.make_model(self.units, cls=_NoMinimumModel)
        data, lines = _tune(
            cosmology=self.cosmology,
            model=model,
            z_grid=self.z_grid,
            progress="every",
        )
        self.assertTrue(data["failure"])
        reason = data["failure_reason"]
        self.assertEqual(_first_clause(reason), "no normalization reaches H0")
        self.assertTrue(reason.endswith("the last at integration 4"), reason)
        self.assertEqual(len(lines), 5)
        kinds = [PROGRESS.match(l).group("kind") for l in lines]
        self.assertEqual(kinds, ["seed", "plain", "guard", "guard", "guard"])

    def test_h_the_seed_is_each_models_own(self):
        """
        (h) beta = 3 at M = 0.01 and at M = 0.1 M_P, in that order, each with max_integrations=1:
        one integration each, about 10 s unloaded.
        """
        Mp = self.units.PlanckMass
        for M_over_Mp in (0.01, 0.1):
            model = gth.make_model(self.units, M_over_Mp=M_over_Mp)
            seed = model.seed(self.cosmology)
            data, lines = _tune(
                cosmology=self.cosmology,
                model=model,
                z_grid=self.z_grid,
                max_integrations=1,
                progress="every",
            )
            self.assertTrue(data["failure"])
            self.assertEqual(len(lines), 1)
            m = PROGRESS.match(lines[0])
            self.assertEqual(m.group("it"), "0")
            self.assertEqual(m.group("kind"), "seed")
            self.assertEqual(m.group("norm"), f"{seed.normalization:.12g}", M_over_Mp)
            self.assertEqual(m.group("phi_ref"), f"{seed.phi_ref / Mp:.10e}", M_over_Mp)

    def test_i_the_effective_minimum_is_not_found(self):
        """(i) effective_minimum raises ValueError: one integration, about 5 s unloaded."""
        model = gth.make_model(self.units, cls=_RaisingMinimumModel)
        data, lines = _tune(cosmology=self.cosmology, model=model, z_grid=self.z_grid)
        self.assertTrue(data["failure"])
        reason = data["failure_reason"]
        self.assertEqual(_first_clause(reason), "the effective minimum was not found")
        self.assertIn("a test model's effective minimum has no bracket", reason)
        self.assertTrue(reason.endswith("(tuning integration 0)"), reason)
        self.assertEqual(lines, [])

    def test_j_an_integrations_own_failure(self):
        """
        (j) StepControl patched to a step budget of 50, as test_scalarhistory_failure_reason.py
        does: the first integration fails at its 51st step, well under a second.
        """
        real = SM.StepControl
        with mock.patch.object(
            SM, "StepControl", lambda **kw: real(**kw, step_budget=50)
        ):
            data, lines = _tune(cosmology=self.cosmology, z_grid=self.z_grid)
        self.assertTrue(data["failure"])
        reason = data["failure_reason"]
        self.assertTrue(reason.startswith("step budget exhausted"), reason)
        self.assertEqual(_first_clause(reason), "step budget exhausted")
        self.assertTrue(
            reason.endswith("(tuning integration 0)")
            or len(reason) == DEFAULT_STRING_LENGTH,
            reason,
        )
        self.assertLessEqual(len(reason), DEFAULT_STRING_LENGTH)
        self.assertEqual(lines, [])


if __name__ == "__main__":
    unittest.main()
