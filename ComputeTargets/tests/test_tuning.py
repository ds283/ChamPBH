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
  (c) a converged history (one tuning in setUpClass: five integrations, about 25 s unloaded):
      the residuals, A(phi_ref) = 1, V_ref, the residuals recomputed from the last sample, the
      seed, the step counts, and the progress lines against RunDrivers' StageTracker.
  (d) determinism: a second tuning equals (c)'s bitwise.
  (e) the tuned golden history: (c)'s output against data/golden_history_06_tuned.json.
  (f)-(j) the failures: the cap, no normalization reaching H0, the seed being the model's own,
      the effective minimum not found, and an integration's own failure.
"""

import contextlib
import importlib
import io
import json
import re
import unittest
from math import exp, sqrt
from unittest import mock

from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_TOLERANCE,
    TUNING_OUTPUT_KEYS,
    SampleValues,
    TuningSecant,
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


class TestAConvergedHistory(unittest.TestCase):
    """
    (c), (d), (e). setUpClass tunes the tests' point once: five integrations to T_CMB, about 25 s
    unloaded. (d) tunes it again (another 25 s).
    """

    @classmethod
    def setUpClass(cls):
        cls.cosmology = gth.make_cosmology()
        cls.units = cls.cosmology.units
        cls.model = gth.make_model(cls.units)
        cls.data, cls.lines = _tune(cosmology=cls.cosmology, model=cls.model)
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

    def test_d_a_second_tuning_is_bitwise_the_same(self):
        """(d) Five more integrations, about 25 s unloaded."""
        again, _ = _tune(cosmology=self.cosmology, model=self.model)
        self.assertEqual(record_of(again), record_of(self.data))
        for key in TUNING_OUTPUT_KEYS:
            a, b = again[key], self.data[key]
            self.assertIs(type(a), type(b), key)
            if isinstance(a, float):
                self.assertEqual(a.hex(), b.hex(), key)
            else:
                self.assertEqual(a, b, key)

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
            cosmology=self.cosmology, z_grid=self.z_grid, max_integrations=2
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
        data, lines = _tune(cosmology=self.cosmology, model=model, z_grid=self.z_grid)
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
