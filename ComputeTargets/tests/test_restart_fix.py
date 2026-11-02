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
F3 and the guard at a restart with pi > 0 (reflection-restart prompt 03 §1.1, §1.2, §1.4; README §6.2 P7,
P8, §6.3; orchestrator note 03, rulings 5 and 6), in `integrate_scalar_history`:

- **F3**: a Radau restarted with pi > 0 (after a reflection, a drain or a release) is given
  first_step = min(global_max_step, max(h_floor, f phi/pi)) by `_restart_first_step`; the history's start,
  and a restart with pi <= 0, get SciPy's own choice.
- **the guard**: the first accepted step of such an instance is tested by `_outward_step_flagged` (P1's
  flag, either condition) and, if flagged, discarded and retaken from its start with first_step = h/2;
  below h_floor the history fails. No other step is tested. `IntegrationResult.restart_retakes` counts
  the retakes, and `build_extra_data` stores the count only when it is above 0.

**The window** (from 01's test_probe_outward_fix.py, deleted with the probe; ruling 6). The diagnosis's
first failure (beta = 1.5, M = 1e-12 M_P, alpha = 0.05, tuned) takes its bad step from the state of a
floor reflection at N = 35.914291 (diagnosis §3 step 3). The state the reflection was made from, the
realized model and the ledger before the reflection's drain are recorded below as float.hex, from
.documents/reflection-restart/measure/restart_state.py (its output is
.documents/reflection-restart/out/restart_state.txt). A window starts there: the loop makes the
reflection and its drain itself (the ledger's new entry and the restarted state are checked bitwise
against the history's), restarts Radau, and runs until ln T_J has fallen by WINDOW (2e-3 e-folds, three
times the bad step, short of the apex of the outward flight) or by LONG_WINDOW (1e-2, past the step at
N0 + 0.009 that P1's ratio condition flags, the field's legitimate deceleration before the apex). The
window's Radau bound is the history's (N_failsafe), so that SciPy chooses the restarted solver's first
step as it did in the history.

Breakages each test is shown to catch are in the log of prompt 03 (§6). No Ray, no store. Costs, unloaded
(01's measure, about twice that at a load of 10): the cosmology about 1 s; each window 0.05-0.3 s; the
cap-loop probe P1 at M = 0.5 about 0.3 s; the history beta = 5, M = 0.5 M_P, alpha = 0.1 about 6 s; the
golden history beta = 1.5 at M = 1e-3 about 0.6 s.
"""

import contextlib
import functools
import importlib
import io
import unittest
from unittest import mock

import numpy as np

from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import test_kinematic_cap_loop as cap_loop
from ComputeTargets.tests import test_release as rel
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Quadrature.supervisors.ScalarField import (
    ScalarFieldIntegrationSupervisor,
    StateVector,
)
from Units import Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")

BETA = 1.5
M_OVER_MP = 1.0e-12
ALPHA = 0.05
WINDOW = 2.0e-3
LONG_WINDOW = 1.0e-2

# the diagnosis's resolved re-integration of the bad step (out/trace-1.5-1e-12-0.05.txt), at N0 + h
RESOLVED_PHI = 1.523361e-06
RESOLVED_PI = 2.351192e-03

# from .documents/reflection-restart/out/restart_state.txt (restart_state.py)
RESTART_N = float.fromhex("0x1.1f5077d3b9b9fp+5")
RESTART_STATE = tuple(
    float.fromhex(x)
    for x in (
        "0x1.0aef551c04d40p-42",
        "0x1.3f0da5ec8aaa8p-9",
        "-0x1.ddd3e415f9ab8p+7",
        "-0x1.c38af5a2f4350p+1",
        "-0x1.de0a48f74dca2p+5",
    )
)
NORMALIZATION = float.fromhex("-0x1.14a17800d5024p+8")
PHI_REF = float.fromhex("0x1.46217bbc6b0dfp-20")
K_INITIAL = float.fromhex("0x0.0p+0")
LEDGER = tuple(
    (float.fromhex(n), float.fromhex(k))
    for n, k in (
        ("0x1.2a4dc2d7d10a8p+4", "0x1.8bdfbd5d5c584p-147"),
        ("0x1.3a56df29ba380p+4", "0x1.3c2d92021356bp-147"),
        ("0x1.52ef04d3e2582p+4", "0x1.d4443ed06322ap-150"),
        ("0x1.680431c31d694p+4", "0x1.3ae98863404c1p-153"),
        ("0x1.a053a8faba3c9p+4", "0x1.5451ae3c62f40p-152"),
        ("0x1.a96a63ef38951p+4", "0x1.48cca43b6dfdcp-152"),
        ("0x1.af732b4894e9cp+4", "0x1.166dc249745edp-152"),
        ("0x1.b47577de92353p+4", "0x1.ac9a9ec3c0f69p-153"),
        ("0x1.b948ccd84fe14p+4", "0x1.1bfc6282117bdp-153"),
        ("0x1.bf16aa5260595p+4", "0x1.f374e774f85a8p-155"),
        ("0x1.09202e45d3407p+5", "0x1.c722d0801b0b2p-161"),
        ("0x1.0e9644b344ef5p+5", "0x1.b04e69c8f2791p-161"),
        ("0x1.11bcf1ddd7a4bp+5", "0x1.9aab0f353f0bcp-161"),
        ("0x1.13f1d916d19f9p+5", "0x1.861fd6e165ef0p-161"),
        ("0x1.15a264970fa1bp+5", "0x1.729ccc51be4d2p-161"),
        ("0x1.16ff48386a756p+5", "0x1.601415c70e90ap-161"),
        ("0x1.18227be2852dbp+5", "0x1.4e78eaf338303p-161"),
        ("0x1.191b7ea49215fp+5", "0x1.3dbf4a72d6f54p-161"),
        ("0x1.19f44ecbfa0edp+5", "0x1.2ddbdbe3f5eafp-161"),
        ("0x1.1ab3bede2a80fp+5", "0x1.1ec3df85dc9ffp-161"),
        ("0x1.1b5eac6ef3a6fp+5", "0x1.106d231eef9b0p-161"),
        ("0x1.1bf8af19c15c1p+5", "0x1.02cdf9338925bp-161"),
        ("0x1.1c84812ec9642p+5", "0x1.ebba62ef0704cp-162"),
        ("0x1.1d0441768aa5cp+5", "0x1.d3242422e581ep-162"),
        ("0x1.1d799e358423bp+5", "0x1.bbc8a2bd30f5cp-162"),
        ("0x1.1de5f245f7ecep+5", "0x1.a598205697b30p-162"),
        ("0x1.1e4a595ab9f69p+5", "0x1.9083a88a2e4c0p-162"),
        ("0x1.1ea7be74e15fep+5", "0x1.7c7d06a96add7p-162"),
        ("0x1.1efee66f1d81ap+5", "0x1.6976bc229f4b0p-162"),
        ("0x1.1f5077d3b9b9fp+5", "0x1.5763f742e9d8dp-162"),
    )
)
BAD_STEP_H = float.fromhex("0x1.4dcce0f410000p-11")
PRE_REFLECTION_STATE = tuple(
    float.fromhex(x)
    for x in (
        "0x1.0aef551c04d40p-42",
        "-0x1.47577fb413242p-9",
        "-0x1.ddd3e415f9ab8p+7",
        "-0x1.c38af5a2f4350p+1",
        "-0x1.de0a48f74dca2p+5",
    )
)


_units = Planck_units()
_params = Planck2018()
_cosmology = QCD_Cosmology(0, _units, _params)
_T_init = temperature(0, 2.0e4 * _units.GeV)
_T_stop = temperature(1, _params.T_CMB_Kelvin * _units.Kelvin)


def expected_first_step(y) -> float:
    """F3's first step at a restarted state y with pi > 0, from StepControl's defaults."""
    params = SM.StepControl()
    return min(
        params.global_max_step,
        max(params.cap_floor, params.cap_fraction * y[0] / y[1]),
    )


def scipys_own_choice(y, global_max_step, h_floor, cap_fraction):
    """A stand-in for _restart_first_step: SciPy chooses the first step, as before this prompt."""
    return None


def never_flagged(*args, **kwargs):
    """A stand-in for _outward_step_flagged that passes every step."""
    return False


# the guard's predicate as the module defines it, held here so that the recorder and the spy read it
# even while a test replaces the module's name
_OUTWARD_STEP_FLAGGED = SM._outward_step_flagged


def _flagged(interpolant, t0, t1, y0, y1) -> bool:
    return _OUTWARD_STEP_FLAGGED(
        interpolant, t0, t1, y0[0], y0[1], y1[1], SM.StepControl().cap_fraction
    )


class _Recorder:
    """The Radau instances of an integration and their steps, read without changing them."""

    def __init__(self):
        self.instances = []

    def radau(self):
        recorder = self
        base = SM.Radau

        class Recording(base):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.record = {
                    "t0": self.t,
                    "y0": self.y.copy(),
                    "first_step": kwargs.get("first_step"),
                    "steps": [],
                }
                recorder.instances.append(self.record)

            def step(self):
                t0, y0 = self.t, self.y.copy()
                message = super().step()
                if message is None and self.t != t0:
                    flagged = y0[1] > 0.0 and _flagged(
                        self.dense_output(), t0, self.t, y0, self.y
                    )
                    self.record["steps"].append((t0, self.t - t0, flagged))
                return message

        return Recording


class _Spy:
    """_outward_step_flagged, called through and counted (or replaced by `replacement`)."""

    def __init__(self, replacement=None):
        self.calls = []
        self.replacement = replacement

    def __call__(self, interpolant, N_low, N_high, *args):
        self.calls.append((N_low, N_high))
        target = _OUTWARD_STEP_FLAGGED if self.replacement is None else self.replacement
        return target(interpolant, N_low, N_high, *args)


def window(patches=(), log_T_drop=WINDOW):
    """
    The window under the patches. Returns (result or the ComputationFailureError, the recorder, the
    ledger).
    """
    model = ExponentialModel(
        0, M_value(0, M_OVER_MP * _units.PlanckMass), 1, beta_value(0, BETA), _units
    ).realize(NORMALIZATION, PHI_REF)
    ledger = DarkRadiationLedger(K_INITIAL)
    for N_j, delta_K in LEDGER[:-1]:
        ledger.add(N_j, delta_K)
    policy = SM.ODEPolicy("test", _cosmology, model, dark_radiation=ledger)
    rhs = SM.ODERHS("test", policy)
    supervisor = ScalarFieldIntegrationSupervisor(
        _units, _T_init, _T_stop, label="test", collect_full_statistics=False
    )
    recorder = _Recorder()
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(SM, "Radau", recorder.radau()))
        for p in patches:
            stack.enter_context(p)
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        stack.enter_context(supervisor)
        try:
            result = SM.integrate_scalar_history(
                rhs,
                supervisor,
                StateVector._make(PRE_REFLECTION_STATE),
                RESTART_N,
                RESTART_STATE[4] - log_T_drop,
                SM.StepControl(),
                policy=policy,
                alpha=ALPHA,
            )
        except ComputationFailureError as e:
            result = e
    return result, recorder, ledger


def without_F3():
    return mock.patch.object(SM, "_restart_first_step", scipys_own_choice)


def without_the_guard():
    return mock.patch.object(SM, "_outward_step_flagged", never_flagged)


class _WindowChecks(unittest.TestCase):
    def assert_the_reflection(self, recorder, ledger):
        """The loop made the history's reflection: its drain and its restarted state, bitwise."""
        self.assertEqual(ledger.injections[-1], LEDGER[-1])
        restart = recorder.instances[1]
        self.assertEqual(restart["t0"], RESTART_N)
        self.assertEqual(
            [float(v).hex() for v in restart["y0"]],
            [float(v).hex() for v in RESTART_STATE],
        )

    def assert_no_bad_step_kept(self, result, recorder):
        """The window succeeded; no kept step carries the flag; pi > 0 over the bad step's interval;
        phi and pi at N0 + BAD_STEP_H agree with the resolved re-integration to 1e-6."""
        self.assertNotIsInstance(result, ComputationFailureError, result)
        sol = result.solution
        kept = list(zip(sol.ts[:-1], np.diff(sol.ts)))
        flagged = {
            (t0, h) for inst in recorder.instances for t0, h, f in inst["steps"] if f
        }
        for t0, h in kept:
            self.assertNotIn((t0, h), flagged, f"a kept step at N = {t0!r}")
        end = RESTART_N + BAD_STEP_H
        for k, (t0, t1) in enumerate(zip(sol.ts[:-1], sol.ts[1:])):
            if t0 >= end:
                break
            y = sol.interpolants[k](np.linspace(t0, min(t1, end), 50))
            self.assertTrue(np.all(y[1] > 0.0), f"pi <= 0 on the step at N = {t0!r}")
            self.assertTrue(np.all(y[0] > 0.0), f"phi <= 0 on the step at N = {t0!r}")
        y = sol(end)
        self.assertLess(abs(y[0] / RESOLVED_PHI - 1.0), 1e-6)
        self.assertLess(abs(y[1] / RESOLVED_PI - 1.0), 1e-6)


# ---- F3 ---------------------------------------------------------------------------------------------


class TestTheRestartFirstStep(unittest.TestCase):
    """_restart_first_step on its own: the formula, its clamps, and None where pi <= 0. Microseconds."""

    def test_the_formula_and_its_clamps(self):
        f, h_floor, h_max = 0.1, 1e-11, 0.1
        self.assertEqual(SM._restart_first_step((2.0, 4.0), h_max, h_floor, f), 0.05)
        # above global_max_step: clamped to it
        self.assertEqual(SM._restart_first_step((2.0, 1.0), h_max, h_floor, f), h_max)
        # below h_floor: clamped to it
        self.assertEqual(
            SM._restart_first_step((1e-15, 1.0), h_max, h_floor, f), h_floor
        )

    def test_none_where_pi_is_not_positive(self):
        for pi in (0.0, -1.0, -1e-300):
            with self.subTest(pi=pi):
                self.assertIsNone(SM._restart_first_step((1.0, pi), 0.1, 1e-11, 0.1))


class TestF3AtAReflection(_WindowChecks):
    """F3 at the reflection of the window. About 0.05 s."""

    def test_the_restarted_first_step(self):
        spy = _Spy()
        result, recorder, ledger = window(
            (mock.patch.object(SM, "_outward_step_flagged", spy),)
        )
        self.assert_the_reflection(recorder, ledger)
        self.assert_no_bad_step_kept(result, recorder)
        self.assertEqual(
            recorder.instances[1]["first_step"], expected_first_step(RESTART_STATE)
        )
        # the start of the integration is not a restart: SciPy chooses its first step
        self.assertIsNone(recorder.instances[0]["first_step"])
        # under F3 the guard tests the restarted instance's first step, once, and passes it
        self.assertEqual(result.restart_retakes, 0)
        self.assertEqual(len(recorder.instances), 2)
        first = recorder.instances[1]["steps"][0]
        self.assertEqual(spy.calls, [(first[0], first[0] + first[1])])


@functools.lru_cache(maxsize=None)
def drain_and_release_history():
    """M = 0.5 M_P, beta = 5, alpha = 0.1 to T_CMB (test_release's history; about 6 s at a load of
    5-15), with every Radau instance recorded."""
    recorder = _Recorder()
    history = rel.integrate(
        5.0, 0.5, patches=(mock.patch.object(SM, "Radau", recorder.radau()),)
    )
    return history, recorder


class TestF3AtADrainAndAtARelease(unittest.TestCase):
    """
    F3 at the restarts after a drain and after the release: M = 0.5 M_P, beta = 5, alpha = 0.1, to T_CMB
    (test_release's history, which drains at turning points before it parks, parks at #105, is released,
    and drains again after the release). Every instance but the first is a restart; each restart with
    pi > 0 has F3's first step, and any with pi <= 0 SciPy's. About 6 s at a load of 5-15.
    """

    @classmethod
    def setUpClass(cls):
        cls.history, cls.recorder = drain_and_release_history()
        cls.result = cls.history.result

    def test_every_restart(self):
        self.assertFalse(self.history.data.get("failure", False), self.history.data)
        instances = self.recorder.instances
        self.assertIsNone(instances[0]["first_step"])
        for inst in instances[1:]:
            y0 = inst["y0"]
            if y0[1] > 0.0:
                self.assertEqual(
                    inst["first_step"], expected_first_step(y0), inst["t0"]
                )
            else:
                self.assertIsNone(inst["first_step"], inst["t0"])
        self.assertEqual(self.result.restart_retakes, 0)

    def test_the_drains_and_the_release_are_restarts_with_pi_positive(self):
        starts = {inst["t0"]: inst for inst in self.recorder.instances[1:]}
        # every drain but the parking candidate's, where the history switches to the parked phase and
        # makes no restart
        parking = self.result.parking
        drains = [
            x.N
            for k, x in enumerate(self.result.injections)
            if x.kind == "turning_point"
            and x.alpha_eff > 0.0
            and k != parking.candidate_index
        ]
        self.assertGreater(len(drains), 100)
        self.assertGreater(len([N for N in drains if N > self.result.release.N]), 0)
        # every drain is a restart (no reflection here: every candidate is a turning point)
        self.assertEqual(len(self.result.reflections), 0)
        for N in drains:
            self.assertIn(N, set(starts), f"no restart at the drain at N = {N!r}")
            self.assertGreater(starts[N]["y0"][1], 0.0)
            self.assertEqual(
                starts[N]["first_step"], expected_first_step(starts[N]["y0"])
            )
        release = self.result.release
        self.assertIsNotNone(release)
        self.assertIn(release.N, set(starts), "no restart at the release")
        self.assertGreater(starts[release.N]["y0"][1], 0.0)
        self.assertEqual(
            starts[release.N]["first_step"],
            expected_first_step(starts[release.N]["y0"]),
        )


class TestF3IsClampedToTheBound(unittest.TestCase):
    """
    A restart closer to the solver's bound than F3's step: test_release's history (M = 0.5 M_P, beta = 5,
    alpha = 0.1) with N_stop = N_rel + 0.01, 0.01 e-folds after the release, where F3's step is 0.1
    (f phi/pi is about 0.66 there). SciPy refuses a first step past the bound (ValueError), so the
    restart's first step is the distance to the bound, N_stop - N_rel (ruled by the user, "Please allow
    both"). N_rel is drain_and_release_history's; the window's own release lies within 1e-3 of it. About
    7 s at a load of 5-15, beside drain_and_release_history's (cached).
    """

    def test_a_restart_near_n_stop(self):
        history, _ = drain_and_release_history()
        N_rel = history.result.release.N
        N_stop = N_rel + 0.01
        recorder = _Recorder()
        near = rel.integrate(
            5.0,
            0.5,
            patches=(mock.patch.object(SM, "Radau", recorder.radau()),),
            N_stop=N_stop,
        )
        self.assertFalse(near.data.get("failure", False), near.data)
        self.assertEqual(near.result.N_final, N_stop)
        # the bound changes SciPy's own steps from the start, so this history's release is its own,
        # near N_rel and before N_stop
        N_near = near.result.release.N
        self.assertLess(abs(N_near - N_rel), 1e-3)
        self.assertLess(N_near, N_stop)
        starts = {inst["t0"]: inst for inst in recorder.instances[1:]}
        self.assertIn(N_near, set(starts), "no restart at the release")
        y0 = starts[N_near]["y0"]
        self.assertGreater(y0[1], 0.0)
        # F3's own step would pass the bound; the restart's is the distance to it
        self.assertGreater(expected_first_step(y0), N_stop - N_near)
        self.assertEqual(starts[N_near]["first_step"], N_stop - N_near)


# ---- the guard --------------------------------------------------------------------------------------


class TestTheGuardCatchesTheBadStep(_WindowChecks):
    """The guard on its own, with _restart_first_step replaced by SciPy's own choice. About 0.1 s."""

    def test_the_bad_step_is_discarded_and_retaken(self):
        result, recorder, ledger = window((without_F3(),))
        self.assert_the_reflection(recorder, ledger)
        # SciPy chose the restarted solver's first step, as in the history, and took the bad step
        self.assertIsNone(recorder.instances[1]["first_step"])
        discarded = recorder.instances[1]["steps"][0]
        self.assertEqual(discarded[1], BAD_STEP_H)
        self.assertTrue(discarded[2])
        # retaken from the same start at half its h
        retake = recorder.instances[2]
        self.assertEqual(retake["t0"], RESTART_N)
        self.assertEqual(
            [float(v).hex() for v in retake["y0"]],
            [float(v).hex() for v in RESTART_STATE],
        )
        self.assertEqual(retake["first_step"], 0.5 * BAD_STEP_H)
        self.assertGreaterEqual(result.restart_retakes, 1)
        self.assertEqual(result.restart_retakes, len(recorder.instances) - 2)
        # the window succeeds, and the field never crosses phi = 0
        self.assert_no_bad_step_kept(result, recorder)
        sol = result.solution
        for k, (t0, t1) in enumerate(zip(sol.ts[:-1], sol.ts[1:])):
            phi = sol.interpolants[k](np.linspace(t0, t1, 50))[0]
            self.assertTrue(np.all(phi > 0.0), f"phi <= 0 on the step at N = {t0!r}")
        # the discarded step is not in the solution
        self.assertNotIn(RESTART_N + BAD_STEP_H, list(sol.ts))

    def test_without_the_guard_the_bad_step_is_kept(self):
        """The breakage the guard exists to catch: the history's m_eff^2 failure."""
        result, recorder, ledger = window((without_F3(), without_the_guard()))
        self.assertIsInstance(result, ComputationFailureError)
        self.assertIn("log_m_eff_squared", result.message)
        self.assert_the_reflection(recorder, ledger)
        first = recorder.instances[1]["steps"][0]
        self.assertEqual(first[1], BAD_STEP_H)
        self.assertTrue(first[2])
        self.assertEqual(len(recorder.instances), 2)

    def test_a_retake_below_h_floor_fails_the_history(self):
        """Every first step flagged: F3's first step here is h_floor, so its half is below it."""
        always = mock.patch.object(SM, "_outward_step_flagged", lambda *a: True)
        result, recorder, _ = window((always,))
        self.assertIsInstance(result, ComputationFailureError)
        self.assertIn("guard", result.message)
        self.assertIn("h_floor", result.message)
        self.assertIn(f"N={RESTART_N:.10g}", result.message)
        # no retake was built
        self.assertEqual(len(recorder.instances), 2)


class TestTheGuardLooksAtTheFirstStepOnly(unittest.TestCase):
    def test_a_later_flagged_step_of_a_restarted_instance_is_kept(self):
        """The window to LONG_WINDOW under F3: the restarted instance's step at N0 + 0.009 carries P1's
        ratio condition (the deceleration before the apex) and is kept; the guard tests the first step
        only. About 0.1 s."""
        spy = _Spy()
        result, recorder, _ = window(
            (mock.patch.object(SM, "_outward_step_flagged", spy),),
            log_T_drop=LONG_WINDOW,
        )
        self.assertNotIsInstance(result, ComputationFailureError, result)
        self.assertEqual(result.restart_retakes, 0)
        # one restart, no retake
        self.assertEqual(len(recorder.instances), 2)
        steps = recorder.instances[1]["steps"]
        later_flagged = [s for s in steps[1:] if s[2]]
        self.assertGreaterEqual(len(later_flagged), 1)
        # each is kept: its start is a node of the solution (the last may be cut at the window's end)
        nodes = list(result.solution.ts)
        for t0, h, _ in later_flagged:
            self.assertIn(t0, nodes)
            k = nodes.index(t0)
            self.assertTrue(t0 < nodes[k + 1] <= t0 + h)
        self.assertEqual(len(spy.calls), 1)

    def test_no_restart_no_guard(self):
        """The cap-loop probe P1 at M = 0.5 to N = 21: one Radau instance, no restart, and a flagged
        outward step after N = 20.8, which is kept; the guard is never consulted. About 0.3 s.
        """
        spy = _Spy()
        recorder = _Recorder()
        with mock.patch.object(SM, "_outward_step_flagged", spy), mock.patch.object(
            SM, "Radau", recorder.radau()
        ):
            result, _ = cap_loop.integrate(cap_loop.P1, 0.5, 21.0)
        self.assertEqual(len(recorder.instances), 1)
        flagged = [s for s in recorder.instances[0]["steps"] if s[2]]
        self.assertGreaterEqual(len(flagged), 1)
        self.assertGreater(flagged[0][0], 20.8)
        kept = set(zip(result.solution.ts[:-1], np.diff(result.solution.ts)))
        for t0, h, _ in flagged:
            self.assertIn((t0, h), kept)
        self.assertEqual(result.restart_retakes, 0)
        self.assertEqual(spy.calls, [])


# ---- the count --------------------------------------------------------------------------------------


class TestTheCount(unittest.TestCase):
    @staticmethod
    def stand_in(**overrides) -> dict:
        data = {
            "reflections": 2,
            "cap_fraction": 0.1,
            "cap_floor": 1e-11,
            "cap_global_max_step": 0.1,
            "jacobian_factor_max": 1e-4,
            "accepted_steps": 4476,
            "steps_rejected_by_exception": 0,
            "restart_retakes": 0,
            "largest_RHS_values": None,
            "smallest_RHS_values": None,
            "mean_RHS_values": None,
        }
        data.update(overrides)
        return data

    def test_extra_data_holds_the_count_only_above_0(self):
        """build_extra_data: restart_retakes stored when above 0, absent at 0. Microseconds."""
        self.assertEqual(
            SM.build_extra_data(self.stand_in(restart_retakes=3))["restart_retakes"], 3
        )
        self.assertNotIn("restart_retakes", SM.build_extra_data(self.stand_in()))
        # a payload from before this prompt, without the key, stores what it stored before
        old = self.stand_in()
        del old["restart_retakes"]
        self.assertEqual(SM.build_extra_data(old), SM.build_extra_data(self.stand_in()))

    def test_the_payload_carries_the_count(self):
        """integrate_history's payload has "restart_retakes" (0 here: the golden history beta = 1.5,
        M = 1e-3, which does not reflect). About 0.6 s."""
        with contextlib.redirect_stdout(io.StringIO()):
            data = gh.run_history(1.5)
        self.assertEqual(data["restart_retakes"], 0)

    def test_the_count_is_that_of_the_retakes(self):
        """The window's count is the payload's source: IntegrationResult.restart_retakes. About 0.1 s."""
        result, recorder, _ = window((without_F3(),))
        self.assertEqual(result.restart_retakes, len(recorder.instances) - 2)
        self.assertGreaterEqual(result.restart_retakes, 1)


if __name__ == "__main__":
    unittest.main()
