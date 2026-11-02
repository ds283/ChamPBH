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
The probe switch of reflection-restart prompt 01 (§1.2; README §5, §6.2 P8 in its probe form):
`ComputeTargets.ScalarHistory._PROBE_OUTWARD_FIX`, read by integrate_scalar_history at call time.

The window. The diagnosis's first failure (beta = 1.5, M = 1e-12 M_P, alpha = 0.05, tuned; it fails in
its seed integration) takes its bad step from the state of a floor reflection at N = 35.914291
(diagnosis §3 step 3). The state the reflection was made from, the realized model and the ledger before
the reflection's drain are recorded below as float.hex, from
.documents/reflection-restart/measure/restart_state.py (its output is
.documents/reflection-restart/out/restart_state.txt). A window starts there: the loop makes the
reflection and its drain itself (the ledger's new entry and the restarted state are checked bitwise
against the history's), restarts Radau, and runs until ln T_J has fallen by WINDOW (about 2e-3
e-folds, three times the bad step, and short of the apex of the outward flight, where P1's ratio
condition fires on the field's legitimate deceleration). The window's Radau bound is the history's
(N_failsafe), so that SciPy chooses the restarted solver's first step as it did in the history.

- **None** reproduces the bad step: the restarted Radau's first step is the history's, h = BAD_STEP_H
  bitwise, carrying the flag (its interpolant falls below zero), and the gate fails the window on it with
  the history's m_eff^2 failure.
- **Each fix** (F1; F2 at c = 1, 0.3 and 3; F3; F4; F3+F4): the window succeeds; no step that the loop
  keeps carries the flag; pi > 0 at every accepted node and on every step's interpolant up to
  N0 + BAD_STEP_H; and phi and pi at N0 + BAD_STEP_H agree with the step re-integrated resolved by the
  diagnosis (phi = 1.523361e-06 M_P, pi = 2.351192e-03, printed to seven digits) to 1e-6. F4 discards the
  bad step (h = BAD_STEP_H) and retakes it.
- **Bitwise where a fix should not act**: F3 on a window with no restart (the cap-loop probe P1 at
  M = 0.5 to N = 21), and F4 on a window with no flagged step (P1 at M = 0.5 to N = 20.8, which ends before
  its first flagged step; checked under None): ts, the final state and the RHS count equal None's.
- **Any other value** raises ValueError at the start of the integration, and the default is None.

No Ray, no store. Cost: the cosmology about 1 s; each window 0.05-0.3 s unloaded; the P1 windows about
0.2 s each; about 5 s in all.
"""

import contextlib
import importlib
import io
import unittest
from math import log
from unittest import mock

import numpy as np

from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import test_kinematic_cap_loop as cap_loop
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


def _flagged(interpolant, t0, t1, y0, y1) -> bool:
    return SM._probe_outward_step_flagged(
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


def window(fix, c=None):
    """
    The window under _PROBE_OUTWARD_FIX = fix (and _PROBE_STIFFNESS_C = c if given). Returns
    (result or the ComputationFailureError, the recorder, the ledger).
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
    patches = [
        mock.patch.object(SM, "Radau", recorder.radau()),
        mock.patch.object(SM, "_PROBE_OUTWARD_FIX", fix),
    ]
    if c is not None:
        patches.append(mock.patch.object(SM, "_PROBE_STIFFNESS_C", c))
    with contextlib.ExitStack() as stack:
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
                RESTART_STATE[4] - WINDOW,
                SM.StepControl(),
                policy=policy,
                alpha=ALPHA,
            )
        except ComputationFailureError as e:
            result = e
    return result, recorder, ledger


class TestTheWindow(unittest.TestCase):
    def assert_the_reflection(self, recorder, ledger):
        """The loop made the history's reflection: its drain and its restarted state, bitwise."""
        self.assertEqual(ledger.injections[-1], LEDGER[-1])
        restart = recorder.instances[1]
        self.assertEqual(restart["t0"], RESTART_N)
        self.assertEqual(
            [float(v).hex() for v in restart["y0"]],
            [float(v).hex() for v in RESTART_STATE],
        )

    def test_None_reproduces_the_bad_step(self):
        """None: the history's bad step and its failure; about 0.05 s."""
        result, recorder, ledger = window(None)
        self.assertIsInstance(result, ComputationFailureError)
        self.assertIn("log_m_eff_squared", result.message)
        self.assert_the_reflection(recorder, ledger)
        first = recorder.instances[1]["steps"][0]
        self.assertEqual(first[1], BAD_STEP_H)
        self.assertTrue(first[2])
        # SciPy chose the restarted solver's first step, as in the history
        self.assertIsNone(recorder.instances[1]["first_step"])

    def check_fix(self, fix, c=None):
        result, recorder, ledger = window(fix, c)
        self.assertNotIsInstance(result, ComputationFailureError, fix)
        self.assert_the_reflection(recorder, ledger)
        sol = result.solution
        kept = list(zip(sol.ts[:-1], np.diff(sol.ts)))
        flagged = {
            (t0, h) for inst in recorder.instances for t0, h, f in inst["steps"] if f
        }
        # no step the loop kept carries the flag
        for t0, h in kept:
            self.assertNotIn((t0, h), flagged, f"{fix}: a kept step at N = {t0!r}")
        # pi keeps the sign of the resolved re-integration over the bad step's interval
        end = RESTART_N + BAD_STEP_H
        for k, (t0, t1) in enumerate(zip(sol.ts[:-1], sol.ts[1:])):
            if t0 >= end:
                break
            pi = sol.interpolants[k](np.linspace(t0, min(t1, end), 50))[1]
            self.assertTrue(
                np.all(pi > 0.0), f"{fix}: pi <= 0 on the step at N = {t0!r}"
            )
        y = sol(end)
        self.assertLess(abs(y[0] / RESOLVED_PHI - 1.0), 1e-6, fix)
        self.assertLess(abs(y[1] / RESOLVED_PI - 1.0), 1e-6, fix)
        return result, recorder

    def test_F1(self):
        """F1: about 0.1 s (about 190 steps, each outward one capped at f phi/pi)."""
        result, _ = self.check_fix("F1")
        self.assertEqual(result.probe_retakes, 0)

    def test_F2(self):
        """F2 at c = 1, 0.3 and 3: about 0.05 s each."""
        for c in (1.0, 0.3, 3.0):
            with self.subTest(c=c):
                result, _ = self.check_fix("F2", c)
                self.assertEqual(result.probe_retakes, 0)

    def test_F3(self):
        """F3: the restarted Radau's first_step is min(0.1, max(h_floor, f phi/pi)); about 0.05 s."""
        result, recorder = self.check_fix("F3")
        self.assertEqual(result.probe_retakes, 0)
        params = SM.StepControl()
        phi, pi = RESTART_STATE[0], RESTART_STATE[1]
        expected = min(
            params.global_max_step,
            max(params.cap_floor, params.cap_fraction * phi / pi),
        )
        self.assertEqual(recorder.instances[1]["first_step"], expected)
        # the start of the window is not a restart: SciPy chooses its first step
        self.assertIsNone(recorder.instances[0]["first_step"])

    def test_F4(self):
        """F4: the bad step discarded and retaken; about 0.05 s."""
        result, recorder = self.check_fix("F4")
        self.assertGreaterEqual(result.probe_retakes, 1)
        discarded = recorder.instances[1]["steps"][0]
        self.assertEqual(discarded[1], BAD_STEP_H)
        self.assertTrue(discarded[2])
        self.assertEqual(recorder.instances[2]["t0"], RESTART_N)

    def test_F3_and_F4(self):
        """F3+F4: both act. In the window F3's first step (so no step is discarded there); on the
        cap-loop probe P1 at M = 0.5 to N = 21, whose one flagged step None keeps, F4 retakes it.
        About 0.4 s."""
        result, recorder = self.check_fix("F3+F4")
        params = SM.StepControl()
        phi, pi = RESTART_STATE[0], RESTART_STATE[1]
        self.assertEqual(
            recorder.instances[1]["first_step"],
            min(
                params.global_max_step,
                max(params.cap_floor, params.cap_fraction * phi / pi),
            ),
        )
        with mock.patch.object(SM, "_PROBE_OUTWARD_FIX", "F3+F4"):
            probe, _ = cap_loop.integrate(cap_loop.P1, 0.5, 21.0)
        self.assertGreaterEqual(probe.probe_retakes, 1)

    def test_the_defaults(self):
        """The switch's default is None, today's code, and c's is 1; instant."""
        self.assertIsNone(SM._PROBE_OUTWARD_FIX)
        self.assertEqual(SM._PROBE_STIFFNESS_C, 1.0)

    def test_an_unknown_value_raises(self):
        """Any other value is a ValueError before anything is integrated; well under 0.1 s."""
        for fix in ("F5", "f1", "F4+F3", ""):
            with self.subTest(fix=fix):
                with self.assertRaises(ValueError):
                    window(fix)


class TestBitwiseWhereAFixShouldNotAct(unittest.TestCase):
    def run_probe(self, fix, N_stop):
        with mock.patch.object(SM, "_PROBE_OUTWARD_FIX", fix):
            recorder = _Recorder()
            with mock.patch.object(SM, "Radau", recorder.radau()):
                result, _ = cap_loop.integrate(cap_loop.P1, 0.5, N_stop)
        return result, recorder

    def assert_bitwise(self, a, b):
        self.assertTrue(
            np.array_equal(np.asarray(a.solution.ts), np.asarray(b.solution.ts))
        )
        self.assertEqual(
            [float(v).hex() for v in a.final_state],
            [float(v).hex() for v in b.final_state],
        )
        self.assertEqual((a.nfev, a.accepted_steps), (b.nfev, b.accepted_steps))

    def test_F3_without_an_outward_restart(self):
        """P1 at M = 0.5 to N = 21: no reflection, one Radau instance; about 0.3 s."""
        base, recorder = self.run_probe(None, 21.0)
        self.assertEqual(len(recorder.instances), 1)
        fixed, _ = self.run_probe("F3", 21.0)
        self.assert_bitwise(base, fixed)

    def test_F4_where_nothing_is_flagged(self):
        """P1 at M = 0.5 to N = 20.8: outward steps, none flagged; about 0.3 s."""
        base, recorder = self.run_probe(None, 20.8)
        steps = recorder.instances[0]["steps"]
        self.assertGreater(len(steps), 50)
        self.assertFalse(any(f for _, _, f in steps))
        fixed, _ = self.run_probe("F4", 20.8)
        self.assertEqual(fixed.probe_retakes, 0)
        self.assert_bitwise(base, fixed)


if __name__ == "__main__":
    unittest.main()
