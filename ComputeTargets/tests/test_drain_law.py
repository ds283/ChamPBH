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
The drain law (one-loop-drain prompt 01 §1.1-§1.4, §1.6's count, §1.8; README §8.2, §8.8 P18-P24;
orchestrator note 01, rulings 3, 4 and 5).

integrate_scalar_history, integrate_history and tune_history take `drain_law`: "fixed" (the default,
alpha as before) or "one_loop" (alpha carries A > 0, finite). Under the one-loop law each candidate is
classified in the loop (drain_regime, peak_estimate's classification computed once) and drains
alpha_eff = min(DRAIN_ALPHA_CAP, A f) w_gate, f the measured law of one-loop-drain prompt 02c (README §8.10),
which replaced prompt 01's A DRAIN_C1/ell and its gap prescriptions; the law itself is tested in
test_measured_drain_law. The payload gains "drain", a DrainRecord. The items are those of prompt 01 §1.8, as
prompt 02c §1.6 rewrote them:

  (1) the law's inputs at each of the three entry points, under both laws;
  (2) the regime on synthetic assessments: each row of §1.2's table, ell <= 0; alpha_eff by the measured law
      at a one-loop turnaround, the cap, w_gate's two cases, and the unassessed case;
  (3) the fixed law is unchanged: the payload's keys are the merged tree's and "drain", whose fixed-law
      form is DrainRecord("fixed", None, "fixed", count, None, declined); alpha_eff = alpha w_gate at every
      candidate;
      a failure's payload carries "drain";
  (4) P18's reuse: under the one-loop law the loop's PeakEstimate equals a fresh peak_estimate for each
      estimate kind, the margins equal those computed without the reuse, and one_loop_turnaround runs
      once per assessed candidate with R > 1;
  (5) the count of unresolvable assessments, and natural mode's case (§1.6): a constructed floor
      reflection at one of the 19 failing inputs;
  (6) the user's ruling (b) on the inside-wall stop (§1.7): the unresolved turnaround's decision is declined
      where G2 would refuse its reflection, by G2's own expression and comparison; a window of 3873 where
      it is declined; the count.

Histories: the P1 floor harness at M = 1e-10 (test_injection.integrate_floor, about 0.2 s a run), the
merged tree's reflecting golden setup untuned at its seed realization (beta = 1.5, M = 1e-12 M_P,
alpha = 0.05; one integration, about 5-10 s), and 2197 of the 2026.8.0 store at its stored
normalization under the one-loop law at A = 1 (about 2-4 s), all at a load of about 4-8. No Ray, no
store, no PRyMordial solve.
"""

import contextlib
import importlib
import io
import unittest
from math import exp, inf, log, nan, nextafter
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_injection as ti
from ComputeTargets.tests.test_unresolvable_turning_points import (
    FAILING_INPUTS,
    classical_turning_point_before_fix,
)
from ComputeTargets.tests.test_unresolved_turnarounds import failing_floor_harness
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.AbstractModel import (
    AbstractModel,
    ClassicalTurningPoint,
    OneLoopTurnaround,
)
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")
AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

# the keys of integrate_history's success payload on the merged tree (1561b60), before this prompt
MERGED_TREE_PAYLOAD_KEYS = frozenset(
    (
        "metadata",
        "z_grid",
        "sample",
        "reflections",
        "first_bounce",
        "fixed_T_values",
        "cap_fraction",
        "cap_floor",
        "cap_global_max_step",
        "jacobian_factor_max",
        "accepted_steps",
        "steps_rejected_by_exception",
        "restart_retakes",
        "largest_RHS_values",
        "smallest_RHS_values",
        "mean_RHS_values",
        "solver_label",
        "dark_radiation",
        "turning_points",
        "injections",
        "dropped_drains",
        "warnings",
        "parking",
        "release",
        "generations",
        "margins",
        "margin_warnings",
        "rho_chi_over_rho_rad",
        "dark_radiation_values",
    )
)

# 2197 of the 2026.8.0 store (shard0005), read by repr: M in eV, beta, the stored normalization and
# phi_ref in M_P, built as the drain probe builds it (source/probe/drain/drain_probe.py)
H2197 = dict(
    M_eV=0.0009741294382105218,
    beta=1.185,
    normalization=-276.63072859401797,
    phi_ref_Mp=8.645101631245453e-16,
)

BAD_A = (0.0, -0.0, -0.1, -1.0, nan, inf, -inf)


def _ct(log_R: float) -> ClassicalTurningPoint:
    return ClassicalTurningPoint(
        phi_t=1.0e-20,
        log_omega_t_squared=-150.0,
        log_R=log_R,
        lambda_t=1.0e20,
        log_k_ex=-70.0,
        ell_t=log(0.5),
    )


def _turnaround(ell: float) -> OneLoopTurnaround:
    return OneLoopTurnaround(
        phi_ta=2.0e-20,
        log_m_ta=-80.0,
        lambda_ta=3.0e19,
        log_k_ex=-80.0 + ell,
        ell=ell,
        iterations=7,
        outside_classical=True,
    )


def _injection(assessment):
    """An Injection carrying an assessment (phi, ln E, ClassicalTurningPoint) or None."""
    phi_a, log_E_a, ct = (None, None, None) if assessment is None else assessment
    return SM.Injection(
        N=1.0,
        kind="floor",
        N_turn=1.0,
        phi_turn=1.0e-20,
        pi_before=-1.0,
        pi_after=1.0,
        log_rho_kin=-200.0,
        alpha_eff=0.0,
        delta_K=0.0,
        delta_rho_over_rho_rad=0.0,
        H_Einstein_before=1.0,
        H_Einstein_after=1.0,
        nonadiabaticity=0.0,
        w_gate=1.0,
        resolved=False,
        phi_assessed=phi_a,
        log_E_assessed=log_E_a,
        classical_turning_point=ct,
    )


def run_2197(A: float):
    """
    2197 at its stored normalization, untuned, to T_CMB, under the one-loop law at A, as the drain probe
    builds it. Returns (data, result, policy, realized).
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    model = ExponentialModel(
        0, M_value(0, H2197["M_eV"] * units.eV), 1, beta_value(0, H2197["beta"]), units
    )
    realized = model.realize(
        H2197["normalization"], H2197["phi_ref_Mp"] * units.PlanckMass
    )
    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        result = integrate(*args, **kwargs)
        captured.append((result, kwargs["policy"]))
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, 2.0e4 * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, 5.0 * units.PlanckMass),
            pi_value(0, 0.0),
            gth.make_z_grid(),
            realized,
            task_label="test_drain_law-2197",
            alpha=A,
            drain_law=SM.DRAIN_LAW_ONE_LOOP,
        )
    result, policy = captured[-1]
    return data, result, policy, realized


class TestTheConstants(unittest.TestCase):
    def test_2_the_constants_and_their_names(self):
        self.assertEqual(SM.DRAIN_LAW_FIXED, "fixed")
        self.assertEqual(SM.DRAIN_LAW_ONE_LOOP, "one_loop")
        self.assertEqual(SM.DRAIN_ALPHA_CAP, 0.99)
        # prompt 02c removed DRAIN_C1, DRAIN_ELL_EDGE and the gap prescriptions; the measured law's
        # constants are test_measured_drain_law's
        for name in (
            "DRAIN_C1",
            "DRAIN_ELL_EDGE",
            "_GAP_PRESCRIPTION",
            "GAP_PRESCRIPTIONS",
        ):
            self.assertFalse(hasattr(SM, name), name)
        self.assertEqual(SM.DRAIN_MODEL_FIXED, "fixed")
        self.assertEqual(SM.DRAIN_MODEL_ONE_LOOP, "one-loop-measured-v1")
        self.assertEqual(
            SM.DRAIN_REGIMES, ("one_loop", "gap", "classical", "unassessed")
        )


class TestTheLawsInputs(unittest.TestCase):
    """(1): each entry point refuses before any integration; the fixed law's check is today's."""

    def test_1_integrate_scalar_history_refuses(self):
        for law, alpha in [("one_loop", a) for a in BAD_A] + [
            ("fixed", 1.0),
            ("fixed", -0.1),
            ("bogus", 0.1),
            ("Fixed", 0.1),
        ]:
            with self.subTest(law=law, alpha=alpha):
                with mock.patch.object(SM, "Radau") as radau:
                    with self.assertRaises(ValueError) as caught:
                        SM.integrate_scalar_history(
                            mock.Mock(),
                            mock.Mock(),
                            None,
                            0.0,
                            0.0,
                            alpha=alpha,
                            drain_law=law,
                        )
                radau.assert_not_called()
                self.assertTrue(
                    str(caught.exception).startswith("integrate_scalar_history: ")
                )

    def test_1_the_fixed_laws_message_is_todays(self):
        for entry, call in (
            (
                "integrate_scalar_history",
                lambda: SM.integrate_scalar_history(
                    mock.Mock(), mock.Mock(), None, 0.0, 0.0, alpha=1.0
                ),
            ),
            (
                "integrate_history",
                lambda: SM.integrate_history(
                    gth.make_cosmology(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    mock.Mock(),
                    alpha=1.0,
                ),
            ),
            (
                "tune_history",
                lambda: SM.tune_history(
                    gth.make_cosmology(), mock.Mock(), None, None, None, None, alpha=1.0
                ),
            ),
        ):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError) as caught:
                    call()
                self.assertEqual(
                    str(caught.exception),
                    f"{entry}: alpha must satisfy 0 <= alpha < 1, not 1.0",
                )

    def test_1_integrate_history_refuses_before_any_integration(self):
        cosmology = gth.make_cosmology()
        for law, alpha in [("one_loop", a) for a in BAD_A] + [("bogus", 0.1)]:
            with self.subTest(law=law, alpha=alpha):
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
                            drain_law=law,
                        )
                integrate.assert_not_called()

    def test_1_tune_history_refuses_before_the_seed(self):
        cosmology = gth.make_cosmology()
        for law, alpha in [("one_loop", a) for a in BAD_A] + [("bogus", 0.1)]:
            with self.subTest(law=law, alpha=alpha):
                model = mock.Mock()
                with mock.patch.object(SM, "integrate_history") as integrate:
                    with self.assertRaises(ValueError):
                        SM.tune_history(
                            cosmology,
                            model,
                            None,
                            None,
                            None,
                            None,
                            alpha=alpha,
                            drain_law=law,
                        )
                integrate.assert_not_called()
                model.seed.assert_not_called()

    def _integrate_history_with(self, stand_in, **kwargs):
        cosmology = gth.make_cosmology()
        units = cosmology.units
        with mock.patch.object(
            SM, "integrate_scalar_history", stand_in
        ), contextlib.redirect_stdout(io.StringIO()):
            return SM.integrate_history(
                cosmology,
                temperature(0, 2.0e4 * units.GeV),
                temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                phi_value(0, 5.0 * units.PlanckMass),
                pi_value(0, 0.0),
                gth.make_z_grid(),
                gth.make_model(units).realize(0.0, 0.0),
                task_label="test_drain_law",
                **kwargs,
            )

    def test_1_A_above_one_is_accepted_and_handed_on(self):
        """No upper bound on A (DRAIN_ALPHA_CAP clamps); integrate_history hands A and the law on."""
        seen = []

        def stand_in(*args, **kwargs):
            seen.append(kwargs)
            raise ComputationFailureError("stand-in: before the loop")

        data = self._integrate_history_with(stand_in, alpha=1.5, drain_law="one_loop")
        self.assertEqual(seen[0]["alpha"], 1.5)
        self.assertEqual(seen[0]["drain_law"], "one_loop")
        # a failure before the loop: "drain" is None (§1.4)
        self.assertTrue(data["failure"])
        self.assertIn("drain", data)
        self.assertIsNone(data["drain"])

    def test_1_the_default_is_the_fixed_law(self):
        seen = []

        def stand_in(*args, **kwargs):
            seen.append(kwargs)
            raise ComputationFailureError("stand-in")

        self._integrate_history_with(stand_in, alpha=0.1)
        self.assertEqual(seen[0]["drain_law"], "fixed")

    def test_1_tune_history_hands_the_law_on_and_returns_the_integrations_drain(self):
        sentinel = SM.DrainRecord("one_loop", 1.5, "one-loop-measured-v1", 0, (), 0)
        failure = {"failure": True, "failure_reason": "stand-in", "drain": sentinel}
        cosmology = gth.make_cosmology()
        with mock.patch.object(
            SM, "integrate_history", return_value=failure
        ) as integrate, contextlib.redirect_stdout(io.StringIO()):
            data = SM.tune_history(
                cosmology,
                gth.make_model(cosmology.units),
                temperature(0, 2.0e4 * cosmology.units.GeV),
                phi_value(0, 5.0),
                pi_value(0, 0.0),
                gth.make_z_grid(),
                alpha=1.5,
                drain_law="one_loop",
            )
        self.assertEqual(integrate.call_args.kwargs["alpha"], 1.5)
        self.assertEqual(integrate.call_args.kwargs["drain_law"], "one_loop")
        self.assertTrue(data["failure"])
        self.assertIs(data["drain"], sentinel)

    def test_1_the_one_loop_law_runs_on_the_floor_harness(self):
        """integrate_scalar_history accepts A = 1.5 (the floor harness, about 0.2 s)."""
        beta, N0, state = kcl.P1
        policy, rhs, supervisor, ledger = ti.build_with_ledger(beta, 1e-10)
        with contextlib.redirect_stdout(io.StringIO()), supervisor:
            result = SM.integrate_scalar_history(
                rhs,
                supervisor,
                ti.StateVector._make(state),
                N0,
                kcl._log_T_stop,
                SM.StepControl(),
                policy=policy,
                N_stop=21.0,
                alpha=1.5,
                drain_law="one_loop",
            )
        self.assertEqual(result.drain.law, "one_loop")
        self.assertEqual(result.drain.A, 1.5)
        self.assertEqual(len(result.drain.candidates), len(result.injections))
        self.assertEqual(len(result.peak_estimates), len(result.injections))
        self.assertGreater(len(result.injections), 0)
        for record, injection in zip(result.drain.candidates, result.injections):
            self.assertLess(record.alpha_eff, 1.0)
            self.assertEqual(record.alpha_eff, injection.alpha_eff)


class TestTheRegimeAndAlphaEff(unittest.TestCase):
    """(2), on synthetic assessments and a model whose one_loop_turnaround is a mock."""

    PHI, LOG_E = 1.0e-19, -300.0

    def _model(self, **kwargs):
        model = mock.Mock()
        model.one_loop_turnaround = mock.Mock(**kwargs)
        return model

    def _check_against_peak_estimate(self, model, assessment, estimate):
        """P18: the loop's estimate equals peak_estimate's on an Injection with the same assessment."""
        fresh = SM.peak_estimate(model, _injection(assessment))
        self.assertEqual(estimate, fresh)

    def test_2_unassessed(self):
        model = self._model()
        self.assertEqual(SM.drain_regime(model, None), ("unassessed", None, None))
        model.one_loop_turnaround.assert_not_called()
        self.assertIsNone(SM.peak_estimate(model, _injection(None)))

    def test_2_classical_at_and_below_R_equal_to_one(self):
        for log_R in (0.0, -0.0, -1e-300, -50.0):
            with self.subTest(log_R=log_R):
                model = self._model()
                assessment = (self.PHI, self.LOG_E, _ct(log_R))
                regime, ell, estimate = SM.drain_regime(model, assessment)
                self.assertEqual((regime, ell), ("classical", None))
                self.assertEqual(estimate.estimate, SM.ESTIMATE_CLASSICAL)
                model.one_loop_turnaround.assert_not_called()
                self._check_against_peak_estimate(model, assessment, estimate)

    def test_2_one_loop(self):
        model = self._model(return_value=_turnaround(10.0))
        assessment = (self.PHI, self.LOG_E, _ct(5e-324))
        regime, ell, estimate = SM.drain_regime(model, assessment)
        self.assertEqual((regime, ell), ("one_loop", 10.0))
        model.one_loop_turnaround.assert_called_once_with(self.PHI, self.LOG_E)
        self.assertEqual(estimate.estimate, SM.ESTIMATE_ONE_LOOP)
        self._check_against_peak_estimate(model, assessment, estimate)

    def test_2_gap_where_the_turnaround_raises_or_returns_None(self):
        for kwargs, reason in (
            (dict(side_effect=ComputationFailureError("no chain")), "no chain"),
            (dict(return_value=None), "one_loop_turnaround returned None"),
        ):
            with self.subTest(reason=reason):
                model = self._model(**kwargs)
                assessment = (self.PHI, self.LOG_E, _ct(3.0))
                regime, ell, estimate = SM.drain_regime(model, assessment)
                self.assertEqual((regime, ell), ("gap", None))
                self.assertEqual(estimate.estimate, SM.ESTIMATE_CLASSICAL_GAP)
                self.assertEqual(estimate.gap_reason, reason)
                self._check_against_peak_estimate(model, assessment, estimate)

    def test_2_ell_at_or_below_zero_is_gap_and_peak_estimate_is_unchanged(self):
        """P24 with ruling 4: the regime is gap, the PeakEstimate is peak_estimate's (one-loop)."""
        for value in (0.0, -0.0, -1.0):
            with self.subTest(ell=value):
                model = self._model(return_value=_turnaround(value))
                assessment = (self.PHI, self.LOG_E, _ct(3.0))
                regime, ell, estimate = SM.drain_regime(model, assessment)
                self.assertEqual((regime, ell), ("gap", None))
                self.assertEqual(estimate.estimate, SM.ESTIMATE_ONE_LOOP)
                self._check_against_peak_estimate(model, assessment, estimate)

    def test_2_another_exception_propagates(self):
        model = self._model(side_effect=RuntimeError("not ours"))
        with self.assertRaises(RuntimeError):
            SM.drain_regime(model, (self.PHI, self.LOG_E, _ct(3.0)))

    def test_2_alpha_eff_at_a_one_loop_turnaround(self):
        """The measured law (prompt 02c): min(0.99, A f) w_gate, f = 1 - exp(-2.69/(ell + 0.9691 - 0.41 ln L))."""
        for A, ell, L, w in (
            (1.0, 10.0, 30.0, 1.0),
            (0.1, 1.8, 7.0, 1.0),
            (0.05, 32.2579, 102.1, 0.37),
            (1.0, 2.7, 10.0, 1.0),
        ):
            with self.subTest(A=A, ell=ell, L=L, w=w):
                f = 1.0 - exp(-2.69 / (ell + 0.9691 - 0.41 * log(L)))
                self.assertLess(f, 0.95)
                self.assertEqual(
                    SM.drain_alpha_effective("one_loop", ell, L, 50.0, A, w),
                    min(0.99, A * f) * w,
                )

    def test_2_the_cap(self):
        """alpha_cap binds once A f >= 0.99: at the plateau (f = 0.9) from A = 1.1."""
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 1.1, 1.0), 0.99
        )
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 0.5, None, None, 100.0, 0.5),
            0.99 * 0.5,
        )
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 1e-300, None, None, 10.0, 1.0), 0.99
        )

    def test_2_w_gate_zero_drains_nothing(self):
        self.assertEqual(
            SM.drain_alpha_effective("one_loop", 10.0, 30.0, 50.0, 1.0, 0.0), 0.0
        )

    def test_2_nothing_where_unassessed(self):
        """Unassessed drains 0.0 exactly; classical now drains A R S(L) (prompt 02c; README §8.10, decision 3)."""
        for w in (1.0, 0.5):
            with self.subTest(w=w):
                value = SM.drain_alpha_effective("unassessed", None, None, None, 1.0, w)
                self.assertEqual(value, 0.0)
                self.assertEqual(str(value), "0.0")
        self.assertGreater(
            SM.drain_alpha_effective("classical", None, 54.52, -16.6, 1.0, 1.0), 0.0
        )

    def test_2_an_unknown_regime_is_refused(self):
        with self.assertRaises(ValueError):
            SM.drain_alpha_effective("elastic", None, 1.0, 1.0, 1.0, 1.0)


class TestTheFixedLawIsUnchanged(unittest.TestCase):
    """
    (3): the reflecting golden setup untuned at its seed realization (beta = 1.5, M = 1e-12 M_P,
    alpha = 0.05), one integration to T_CMB through integrate_history (test_injection.integrate_captured),
    about 5-10 s at a load of 4-8; and the failing floor harness (about 0.3 s).
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, _ = ti.integrate_captured(1.5, 1.0e-12, 0.05)

    def test_3_the_payloads_keys_are_the_merged_trees_and_drain(self):
        self.assertFalse(self.data.get("failure", False))
        # one-loop-drain prompt 03 adds "bounces" (None under the fixed law) and the sample masses
        self.assertEqual(
            set(self.data),
            MERGED_TREE_PAYLOAD_KEYS
            | {"drain", "bounces", "log_m_eff", "log_m_tracker"},
        )
        self.assertIsNone(self.data["bounces"])

    def test_3_drain_is_the_fixed_laws_record(self):
        self.assertEqual(
            self.data["drain"], SM.DrainRecord("fixed", None, "fixed", 0, None, 0)
        )
        self.assertIs(self.data["drain"], self.result.drain)
        self.assertIsNone(self.result.peak_estimates)

    def test_3_alpha_eff_is_alpha_times_the_gate(self):
        self.assertGreater(len(self.result.injections), 0)
        for injection in self.result.injections:
            w = SM.gate_weight(injection.nonadiabaticity) if injection.resolved else 1.0
            self.assertEqual(injection.w_gate, w)
            self.assertEqual(injection.alpha_eff, 0.05 * w)

    def test_3_a_failures_payload_carries_drain(self):
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
                task_label="test_drain_law",
            )
        self.assertTrue(data["failure"])
        self.assertEqual(set(data), {"failure", "failure_reason", "warnings", "drain"})
        self.assertEqual(
            data["drain"], SM.DrainRecord("fixed", None, "fixed", 0, None, 0)
        )


class TestTheOneLoopLawOn2197(unittest.TestCase):
    """
    (4) and the "drain" key: 2197 at its stored normalization under the one-loop law at A = 1. Under prompt
    01's law this was the drain probe's 2197-A1 (ten candidates, all one-loop); under the measured law of
    prompt 02c it is five candidates, all one-loop, parking at N = 29.035 (re-pinned by prompt 02c and
    cross-checked against prompts/one-loop-drain/source/crossover/measured_law_map.py --law=adopted on the
    same inputs; log 02c). One integration to T_CMB, about 2-4 s at a load of 4-8, with one_loop_turnaround
    counted. electroweak-eos prompt 01 re-pinned the values on its w table: still five candidates, all
    one-loop, now parking at N = 28.876.
    """

    # prompt 02c's pins: each candidate's alpha_eff as the reference map prints it (:.4e), and the parking N.
    # Re-pinned by electroweak-eos prompt 01 when the w table changed, from this tree's history (not
    # cross-checked against the reference map); 02c's were ("8.2276e-02", "9.1886e-02", "1.1431e-01",
    # "1.2001e-01", "1.6459e-01") and 29.035264723467392
    PINNED_ALPHA_EFF = (
        "8.2277e-02",
        "9.1883e-02",
        "1.1431e-01",
        "1.2001e-01",
        "1.6185e-01",
    )
    PINNED_PARKING_N = 28.876391476457353

    @classmethod
    def setUpClass(cls):
        original = AbstractModel.one_loop_turnaround
        cls.turnaround_calls = []

        def counting(self, *args, **kwargs):
            cls.turnaround_calls.append(args)
            return original(self, *args, **kwargs)

        with mock.patch.object(AbstractModel, "one_loop_turnaround", counting):
            cls.data, cls.result, cls.policy, cls.model = run_2197(1.0)

    def test_4_the_record(self):
        drain = self.data["drain"]
        self.assertIs(drain, self.result.drain)
        self.assertEqual(drain.law, "one_loop")
        self.assertEqual(drain.A, 1.0)
        self.assertEqual(drain.model, "one-loop-measured-v1")
        self.assertIsInstance(drain.unresolvable_assessments, int)
        self.assertIsInstance(drain.candidates, tuple)
        self.assertEqual(len(drain.candidates), len(self.result.injections))
        self.assertGreater(len(drain.candidates), 0)
        for record, injection in zip(drain.candidates, self.result.injections):
            self.assertIn(record.regime, SM.DRAIN_REGIMES)
            self.assertEqual(record.alpha_eff, injection.alpha_eff)
            self.assertEqual(record.w_gate, injection.w_gate)
            self.assertEqual(record.log_E, injection.log_E_assessed)
            self.assertEqual(record.log_H, log(injection.H_Einstein_before))
            self.assertEqual(
                record.w_gate,
                (
                    SM.gate_weight(injection.nonadiabaticity)
                    if injection.resolved
                    else 1.0
                ),
            )
            self.assertEqual(
                record.alpha_eff,
                SM.drain_alpha_effective(
                    record.regime,
                    record.ell,
                    record.L,
                    record.log_R,
                    1.0,
                    record.w_gate,
                ),
            )
            # the law's inputs, as given (prompt 02c §1.3; orchestrator note 02c ruling 1)
            ct = injection.classical_turning_point
            if record.regime == "unassessed":
                self.assertIsNone(record.L)
                self.assertIsNone(record.log_R)
            else:
                self.assertEqual(record.L, self.model.drain_L(record.log_E))
                self.assertEqual(record.log_R, ct.log_R)
                # at n = 1, L = M/phi_t to the root's accuracy (ruling 3: 1e-10)
                M = H2197["M_eV"] * self.model.model.units.eV
                self.assertAlmostEqual(record.L / (M / ct.phi_t), 1.0, delta=1e-10)
            if record.regime == "one_loop":
                self.assertGreater(record.ell, 0.0)
            else:
                self.assertIsNone(record.ell)

    def test_4_the_values_under_the_measured_law(self):
        """Re-pinned by prompt 02c: the regimes, alpha_eff and the parking N under the measured law at A = 1."""
        candidates = self.data["drain"].candidates
        self.assertEqual(tuple(c.regime for c in candidates), ("one_loop",) * 5)
        self.assertEqual(
            tuple(format(c.alpha_eff, ".4e") for c in candidates), self.PINNED_ALPHA_EFF
        )
        self.assertAlmostEqual(
            self.result.parking.N, self.PINNED_PARKING_N, delta=1.0e-9
        )

    def test_4_the_record_is_plain_values(self):
        import pickle

        self.assertEqual(
            pickle.loads(pickle.dumps(self.data["drain"])), self.data["drain"]
        )

    def test_4_the_reuse_equals_a_fresh_peak_estimate(self):
        self.assertEqual(len(self.result.peak_estimates), len(self.result.injections))
        for estimate, injection in zip(
            self.result.peak_estimates, self.result.injections
        ):
            self.assertEqual(estimate, SM.peak_estimate(self.model, injection))

    def test_4_the_one_loop_solve_runs_once_per_candidate(self):
        """Once per assessed candidate with ln R > 0, in the loop; history_margins does not call it."""
        expected = sum(
            1
            for i in self.result.injections
            if i.classical_turning_point is not None
            and i.classical_turning_point.log_R > 0.0
        )
        self.assertEqual(len(self.turnaround_calls), expected)

    def test_4_the_margins_equal_those_without_the_reuse(self):
        self.assertIsNotNone(self.data["margins"])
        fresh = SM.history_margins(
            self.result._replace(peak_estimates=None), self.data["sample"], self.policy
        )
        self.assertEqual(fresh, (self.data["generations"], self.data["margins"]))

    def test_4_the_regime_agrees_with_peak_estimates(self):
        """§2.2.5 with ruling 4: one_loop <-> ESTIMATE_ONE_LOOP, gap <-> CLASSICAL_GAP (but ell <= 0)."""
        names = {
            SM.ESTIMATE_ONE_LOOP: "one_loop",
            SM.ESTIMATE_CLASSICAL_GAP: "gap",
            SM.ESTIMATE_CLASSICAL: "classical",
        }
        for record, injection in zip(
            self.data["drain"].candidates, self.result.injections
        ):
            estimate = SM.peak_estimate(self.model, injection)
            expected = "unassessed" if estimate is None else names[estimate.estimate]
            self.assertEqual(record.regime, expected)


class TestTheUnresolvableCount(unittest.TestCase):
    """
    (5): §1.6's count, and natural mode's case on a constructed floor reflection. The P1 floor harness
    at M = 1e-10 (one floor reflection, about 0.2 s a run) with assess_infall replaced by the real
    function at a constructed input on the harness's own model: 2197's failing input (the probe's) moved
    to the harness's ln V_floor, ln E as far below the least finite ln(V - V_floor) as 2197's is below
    its own (2.42 in ln), and phi = 1e5 M_P, far out in the tail, where ln V rounds to ln V_floor and
    assess_infall's ln E is ln rho_kin itself. The pre-fix copy raises there, as at 2197's input.
    """

    PHI_TAIL_MP = 1.0e5

    def _substituted(self):
        source, beta, M, normalization, phi, log_E = FAILING_INPUTS[0]
        self.assertEqual(source, "probe 2197")
        below = AM._least_finite_log_V_minus_floor(normalization) - log_E
        self.assertGreater(below, 2.4)
        real = SM.assess_infall
        calls = []
        inputs = []

        def substituted(model_, phi_, log_rho_kin_):
            calls.append((phi_, log_rho_kin_))
            log_E_c = AM._least_finite_log_V_minus_floor(model_.log_V_floor) - below
            phi_c = self.PHI_TAIL_MP * model_.model.units.PlanckMass
            inputs.append((model_, phi_c, log_E_c))
            return real(model_, phi_c, log_E_c)

        return substituted, calls, inputs

    def test_5_the_constructed_input_raised_before_the_fix(self):
        substituted, _, inputs = self._substituted()
        with mock.patch.object(SM, "assess_infall", substituted):
            ti.integrate_floor(0.0)
        model_, phi_c, log_E_c = inputs[0]
        self.assertTrue(AM._turning_point_unresolvable(model_.log_V_floor, log_E_c))
        self.assertEqual(SM.assess_infall(model_, phi_c, log_E_c), (log_E_c, None))
        with self.assertRaises(ComputationFailureError) as caught:
            classical_turning_point_before_fix(
                model_.model, model_.normalization, phi_c, log_E_c
            )
        self.assertIn("classical_turning_point (", caught.exception.message)

    def test_5_natural_mode_counts_one_and_takes_phi_for_phi_t(self):
        substituted, calls, _ = self._substituted()
        with mock.patch.object(SM, "assess_infall", substituted):
            result, _, _ = ti.integrate_floor(0.0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            result.drain, SM.DrainRecord("fixed", None, "fixed", 1, None, 0)
        )
        today, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(result.reflections, today.reflections)
        self.assertEqual(result.nfev, today.nfev)
        self.assertEqual(tuple(result.final_state), tuple(today.final_state))
        # the floor harness's phi at its reflection is below 1e-8 M_P, so phi taken for phi_t warns
        # "below"
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(
            SM.warning_kind(result.warnings[0]), SM.WARNING_TURNING_POINT_BELOW
        )

    def test_5_before_the_fix_the_same_call_was_unassessed(self):
        substituted, calls, _ = self._substituted()

        def before(self_, normalization, phi, log_E):
            return classical_turning_point_before_fix(self_, normalization, phi, log_E)

        with mock.patch.object(SM, "assess_infall", substituted), mock.patch.object(
            AbstractModel, "classical_turning_point", before
        ):
            result, _, _ = ti.integrate_floor(0.0)
        self.assertEqual(result.drain.unresolvable_assessments, 0)
        today, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(result.reflections, today.reflections)
        self.assertEqual(tuple(result.final_state), tuple(today.final_state))
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(
            SM.warning_kind(result.warnings[0]), SM.WARNING_WALL_UNASSESSED
        )
        self.assertIn("not monotone", result.warnings[0])

    def test_5_the_loop_counts_at_dissipation_mode(self):
        """At alpha > 0 every step start with pi < 0 is assessed: each None with the predicate holding counts."""
        substituted, calls, _ = self._substituted()
        with mock.patch.object(SM, "assess_infall", substituted):
            result, _, _ = ti.integrate_floor(0.1)
        self.assertGreater(len(calls), 0)
        self.assertEqual(result.drain.unresolvable_assessments, len(calls))
        self.assertEqual(result.drain.law, "fixed")


if __name__ == "__main__":
    unittest.main()


# ---- the user's ruling (b) on the inside-wall stop (one-loop-drain prompt 01 §1.7, 2026-11-03) ----

# 3873 of the 2026.8.0 store (shard0007; M in eV, beta, the stored normalization and phi_ref in M_P, by repr),
# and the state just after its reflection at N = 43.763255440356026 under the one-loop law at A = 1 (pi
# flipped; nothing drained, so the ledger is empty), captured on this tree; the next infall is the one on which
# the history stopped before the ruling ("reflection requested inside the wall", W/(pi^2/2) = 1.485)
H3873 = dict(
    M_eV=2435323595526304.5,
    beta=1.11,
    normalization=-276.63073612988643,
    phi_ref_Mp=1.4123304109747024e-06,
)
SEED_3873_N = 43.763255440356026
SEED_3873_STATE = (
    2.129448817919489e-08,
    0.001618019309971909,
    -278.10970626736423,
    6.271286404846784,
    -69.55398791014242,
)


class _SyntheticPolicy:
    """A policy stand-in for G2: one ODEPolicyData-like record, CONST_MP_SQ and a model with a wall."""

    def __init__(self, V_over_3H2Mp2, log_V, log_V_floor, Mp_sq=1.0):
        self._data = mock.Mock(V_over_3H2Mp2=V_over_3H2Mp2, log_V=log_V)
        self.CONST_MP_SQ = Mp_sq
        self.model = mock.Mock(reflects_at_origin=True, log_V_floor=log_V_floor)
        self.model.name = "synthetic"

    def __call__(self, N, state):
        return self._data


class TestTheDeclinedUnresolvedReflection(unittest.TestCase):
    """
    The user's ruling (b): where unresolved_turnaround fires but G2 would refuse the reflection, no
    unresolved decision is made at that step start. (6a) the decision on synthetic states either side of
    G2's boundary, against _reflection_guards on the same states; (6b) a window of 3873 under the one-loop
    law at A = 1, from SEED_3873_STATE to N + 0.0025 (119 accepted steps, under a second at a load of 4-8),
    where the rule is declined and the infall goes on to a resolved turning point, and where the code
    before the ruling raised; (6c) the count.
    """

    # W = -3 v expm1(ln V_floor - ln V) with expm1 = -1.0 exactly, against pi^2/2 = 2.0 at pi = 2, Mp^2 = 1
    LOG_V, LOG_V_FLOOR = 0.0, -1000.0
    Y = (1.0e-8, 2.0, 0.0, 0.0, 0.0)

    def _policy(self, v):
        return _SyntheticPolicy(v, self.LOG_V, self.LOG_V_FLOOR)

    def test_6a_the_comparison_is_G2s(self):
        self.assertFalse(SM._g2_refuses(2.0, 2.0))
        self.assertTrue(SM._g2_refuses(nextafter(2.0, inf), 2.0))
        self.assertFalse(SM._g2_refuses(nextafter(2.0, -inf), 2.0))
        self.assertTrue(SM._g2_refuses(nan, 2.0))

    def test_6a_either_side_of_the_boundary(self):
        at = 2.0 / 3.0
        state = SM.StateVector._make(self.Y)
        W, kinetic = SM._wall_and_kinetic(
            self._policy(at), 0.0, state, self.LOG_V_FLOOR
        )
        self.assertEqual((W, kinetic), (2.0, 2.0))
        # the first v above `at` whose W exceeds pi^2/2 (W rounds to 2.0 for a few floats above it)
        up = at
        while (
            SM._wall_and_kinetic(self._policy(up), 0.0, state, self.LOG_V_FLOOR)[0]
            <= 2.0
        ):
            up = nextafter(up, inf)
        for v, refused in (
            (at, False),
            (nextafter(up, -inf), False),
            (up, True),
            (0.1, False),
            (5.0, True),
        ):
            with self.subTest(v=v):
                policy = self._policy(v)
                self.assertEqual(
                    SM.unresolved_reflection_refused(policy, 0.0, np.array(self.Y)),
                    refused,
                )
                # _reflection_guards raises exactly where the decision is declined, with its message
                if refused:
                    with self.assertRaises(ComputationFailureError) as caught:
                        SM._reflection_guards(policy, 0.0, np.array(self.Y), None)
                    self.assertIn(
                        "reflection requested inside the wall", caught.exception.message
                    )
                else:
                    W, kinetic = SM._wall_and_kinetic(
                        policy, 0.0, state, self.LOG_V_FLOOR
                    )
                    self.assertEqual(
                        SM._reflection_guards(policy, 0.0, np.array(self.Y), None),
                        W / kinetic,
                    )

    def _window(self, refused=None):
        cosmology = kcl._cosmology
        units = kcl._units
        model = ExponentialModel(
            0,
            M_value(0, H3873["M_eV"] * units.eV),
            1,
            beta_value(0, H3873["beta"]),
            units,
        ).realize(H3873["normalization"], H3873["phi_ref_Mp"] * units.PlanckMass)
        ledger = DarkRadiationLedger()
        policy = SM.ODEPolicy("test", cosmology, model, dark_radiation=ledger)
        rhs = SM.ODERHS("test", policy)
        supervisor = ti.ScalarFieldIntegrationSupervisor(
            units, kcl._T_init, kcl._T_stop, label="test", collect_full_statistics=False
        )
        calls = []
        real = SM.unresolved_reflection_refused

        def counting(*args):
            value = real(*args) if refused is None else refused
            calls.append(value)
            return value

        with mock.patch.object(
            SM, "unresolved_reflection_refused", counting
        ), contextlib.redirect_stdout(io.StringIO()), supervisor:
            result = SM.integrate_scalar_history(
                rhs,
                supervisor,
                ti.StateVector._make(SEED_3873_STATE),
                SEED_3873_N,
                kcl._log_T_stop,
                SM.StepControl(),
                policy=policy,
                N_stop=SEED_3873_N + 0.0025,
                alpha=1.0,
                drain_law="one_loop",
            )
        return result, calls

    def test_6b_the_infall_goes_on_to_a_resolved_turning_point(self):
        result, calls = self._window()
        self.assertGreater(result.drain.unresolved_declined, 0)
        self.assertEqual(result.reflections, [])
        walls = [tp for tp in result.turning_points if tp.kind == "wall"]
        self.assertEqual(len(walls), 1)
        self.assertEqual(len(result.injections), 1)
        candidate = result.injections[0]
        self.assertEqual(candidate.kind, "turning_point")
        self.assertTrue(candidate.resolved)
        self.assertEqual(candidate.N_turn, walls[0].N)
        self.assertGreater(candidate.pi_before, 0.0)
        self.assertEqual(result.drain.candidates[0].regime, "classical")

    def test_6b_before_the_ruling_the_window_raised(self):
        with self.assertRaises(ComputationFailureError) as caught:
            self._window(refused=False)
        self.assertIn("reflection requested inside the wall", caught.exception.message)
        self.assertEqual(caught.exception.drain.unresolved_declined, 0)

    def test_6c_the_count_is_the_declined_step_starts(self):
        result, calls = self._window()
        self.assertEqual(result.drain.unresolved_declined, sum(1 for c in calls if c))
        self.assertEqual(len(calls), sum(1 for c in calls if c))
