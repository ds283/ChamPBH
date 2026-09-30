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
PRyMordial's solve_ivp failures are detected, the PRyMordial call is the
boundary at which an exception becomes a failure payload, and non-finite
new-physics samples are refused before any solve.

Written for run-integrity prompt 02 (item F, README section 6.2). Before that
prompt none of the eight `solve_ivp` calls in `PRyM/PRyM_main.py` checked
`.success`, so a solve that gave up returned plausible abundances; only
`(OverflowError, ValueError, ComputationFailureError)` became a failure
payload; and `build_NP_callbacks` never checked its samples for finiteness.

**This module runs partial PRyMordial solves, about 40 s in all.** A failure is
forced as `prompts/run-integrity/planning-probes/prymordial_solver_probe.py`
forces it: the name `PRyM.PRyM_main.solve_ivp`, which PRyMordial looks up at
call time, is replaced by a wrapper that integrates the first 1 % of the k-th
call's span and then marks the result failed, as `solve_ivp` marks a solver
that gives up. The call is selected by its order, not by line number. Every
PRyMordial module global touched is restored, and so is `solve_ivp`.

Tests (d) and (e) run no solve. The new exception class and the new helper
are looked up inside the test bodies, so that on the tree before prompt 02
each test fails on its own assertion rather than the module failing to import.

Nothing here needs a Ray cluster or a datastore. Run from the repository root,
since PRyMordial reads `PRyMrates/` from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import unittest
from math import exp
from unittest import mock

import numpy as np

import PRyM.PRyM_main as PRyMmain
from ComputeTargets.BBNData import (
    NPCallbacks,
    PRYM_VERSION,
    build_NP_callbacks,
    compute_SM_baseline,
)
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.prym_fixtures import (
    RES_D_OVER_H_E5,
    RES_LI7_OVER_H_E10,
    RES_YP_BBN,
    ZERO,
    run_prym,
)
from ComputeTargets.tests.test_network_flag import _SavedPRyMGlobals

# the exception class PRyM/PRyM_main.py raises for a failed solve_ivp
FAILURE_CLASS_NAME = "PRyMSolverFailureError"

# the number of solve_ivp calls PRyMordial makes under production's flags
# (NP_thermo_flag, aTid_flag, compute_bckg_flag true; julia_flag false), per
# network: thermodynamics, a(T), high-T n <-> p, mid-T, low-T
N_CALLS = 5

# with small_network=True, the calls that differ from the full network's
SMALL_NETWORK_CALLS = (4, 5)

FORCED_FRACTION = 0.01
FORCED_MESSAGE = "forced failure (test_bbn_solver_failures)"


class _ForcedFailure:
    """
    Stands in for PRyM_main.solve_ivp. The k-th call (1-based) integrates the
    first 1 % of its span and is then marked failed; every other call is passed
    through unchanged. t_eval, where given, is cut to the shortened span.
    """

    def __init__(self, fail_at: int):
        self._solve_ivp = PRyMmain.solve_ivp
        self.fail_at = fail_at
        self.n_calls = 0
        self.forced = False

    def __call__(self, fun, t_span, y0, **kwargs):
        self.n_calls += 1
        if self.n_calls != self.fail_at:
            return self._solve_ivp(fun, t_span, y0, **kwargs)

        t0, t1 = float(t_span[0]), float(t_span[1])
        t_stop = t0 + FORCED_FRACTION * (t1 - t0)
        if kwargs.get("t_eval") is not None:
            t_eval = np.asarray(kwargs["t_eval"], dtype=float)
            kwargs["t_eval"] = t_eval[(t_eval >= t0) & (t_eval <= t_stop)]
        sol = self._solve_ivp(fun, [t0, t_stop], y0, **kwargs)
        sol.status, sol.success = -1, False
        sol.message = FORCED_MESSAGE
        self.forced = True
        return sol


def _run_forced(fail_at: int, small_network: bool):
    """
    The SM callbacks (all three zero) through run_prym, with the fail_at-th
    solve_ivp call forced to fail. Returns (the spy, the abundances or None,
    the exception or None).
    """
    spy = _ForcedFailure(fail_at)
    res, raised = None, None
    with _SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
        try:
            res = run_prym(ZERO.rho, ZERO.p, ZERO.drho_dT, small_network=small_network)
        except Exception as e:
            raised = e
    return spy, res, raised


class TestBBNSolverFailures(unittest.TestCase):
    def _check_stages(self, calls, small_network: bool, label: str):
        """
        For each k in `calls`, force the k-th solve_ivp call to fail and require
        the new class, a named stage, the status and the message; return the
        stage names.
        """
        stages = {}
        for k in calls:
            with self.subTest(k=k, small_network=small_network):
                spy, res, raised = _run_forced(k, small_network)
                self.assertTrue(spy.forced, f"call {k} was never made")
                if raised is None:
                    print(
                        f"\n[test_bbn_solver_failures ({label})] k={k}: no exception; "
                        f"PRyMordial returned Yp {res[RES_YP_BBN]:.10g}, "
                        f"D/H x1e5 {res[RES_D_OVER_H_E5]:.10g}, "
                        f"7Li/H x1e10 {res[RES_LI7_OVER_H_E10]:.10g}"
                    )
                else:
                    print(
                        f"\n[test_bbn_solver_failures ({label})] k={k}: "
                        f"{type(raised).__name__}: {raised}"
                    )
                self.assertIsNotNone(raised, f"call {k} failed and nothing raised")
                self.assertEqual(type(raised).__name__, FAILURE_CLASS_NAME)
                self.assertEqual(type(raised).__module__, "PRyM.PRyM_main")
                stage = getattr(raised, "stage", None)
                self.assertIsInstance(stage, str)
                self.assertTrue(stage)
                message = str(raised)
                self.assertIn(stage, message)
                self.assertIn("status=-1", message)
                self.assertIn(FORCED_MESSAGE, message)
                stages[k] = stage
        return stages

    def test_a_every_production_stage_is_checked(self):
        """(a) Full network (production): each of the five solve_ivp calls,
        forced to fail, raises the new class naming a stage, and the five stage
        names are distinct. **Five partial PRyMordial solves.**"""
        stages = self._check_stages(range(1, N_CALLS + 1), False, "a")
        self.assertEqual(len(stages), N_CALLS)
        self.assertEqual(len(set(stages.values())), N_CALLS, stages)

    def test_b_small_network_stages_are_checked(self):
        """(b) small_network=True: the mid-T and low-T calls, which differ from
        the full network's, forced to fail, raise the new class naming a stage,
        distinct from each other. **Two partial PRyMordial solves.**"""
        stages = self._check_stages(SMALL_NETWORK_CALLS, True, "b")
        self.assertEqual(len(stages), len(SMALL_NETWORK_CALLS))
        self.assertEqual(len(set(stages.values())), len(SMALL_NETWORK_CALLS), stages)

    def test_c_the_prymordial_boundary(self):
        """(c) Through the PRyMordial helper: a forced failure (k = 1) and a
        callback raising RuntimeError inside PRyMordial each give a failure
        payload whose reason begins "PRyMordial: " and names the class.
        compute_SM_baseline does not use the helper, and still raises.
        **Two partial PRyMordial solves.**"""
        from ComputeTargets.BBNData import _run_PRyMordial

        zero = NPCallbacks(rho_NP=ZERO.rho, P_NP=ZERO.p, drho_NP_dT=ZERO.drho_dT)

        with self.subTest("forced solve_ivp failure"):
            spy = _ForcedFailure(1)
            with _SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
                payload = _run_PRyMordial(zero, small_network=False)
            self.assertTrue(spy.forced)
            self.assertIs(payload.get("failure"), True, payload)
            reason = payload["failure_reason"]
            print(f"\n[test_bbn_solver_failures (c)] forced: {reason}")
            self.assertTrue(reason.startswith(f"PRyMordial: {FAILURE_CLASS_NAME}: "))

        def raising(T_in_MeV: float) -> float:
            raise RuntimeError("synthetic callback failure")

        with self.subTest("RuntimeError from a callback"):
            with _SavedPRyMGlobals():
                payload = _run_PRyMordial(
                    NPCallbacks(rho_NP=raising, P_NP=raising, drho_NP_dT=raising),
                    small_network=False,
                )
            self.assertIs(payload.get("failure"), True, payload)
            reason = payload["failure_reason"]
            print(f"[test_bbn_solver_failures (c)] RuntimeError: {reason}")
            self.assertEqual(
                reason, "PRyMordial: RuntimeError: synthetic callback failure"
            )

        with self.subTest("compute_SM_baseline raises"):
            spy = _ForcedFailure(1)
            with _SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
                with self.assertRaises(Exception) as ctx:
                    compute_SM_baseline(False)
            self.assertEqual(type(ctx.exception).__name__, FAILURE_CLASS_NAME)

    def test_d_non_finite_samples_are_refused(self):
        """(d) build_NP_callbacks refuses a NaN in density_ratio and an infinite
        value in pressure_ratio with ComputationFailureError naming the array,
        the index and T; a callback built from finite samples raises
        ComputationFailureError for T = NaN. **No solve**: before prompt 02 a
        NaN new-physics value hangs PRyMordial."""
        n, k = 40, 17
        log_T = np.linspace(np.log(100.0), np.log(1.0e-7), n)
        r = np.full(n, 0.08)
        s = r / 3.0

        def rho_SM(T):
            return T**4

        def drho_SM_dT(T):
            return 4.0 * T**3

        def build(x, dr, pr):
            return build_NP_callbacks(
                x,
                dr,
                pr,
                rho_SM,
                drho_SM_dT,
                T_min_MeV=1.0e-7,
                T_max_MeV=100.0,
                task_label="test-finite",
            )

        cases = (
            ("density_ratio", float("nan")),
            ("pressure_ratio", float("inf")),
            ("log_T_MeV", float("nan")),
        )
        for name, bad in cases:
            with self.subTest(name):
                arrays = {"log_T_MeV": log_T, "density_ratio": r, "pressure_ratio": s}
                arrays = {a: v.copy() for a, v in arrays.items()}
                arrays[name][k] = bad
                with self.assertRaises(ComputationFailureError) as ctx:
                    build(
                        arrays["log_T_MeV"],
                        arrays["density_ratio"],
                        arrays["pressure_ratio"],
                    )
                message = str(ctx.exception)
                print(f"\n[test_bbn_solver_failures (d)] {name}: {message}")
                self.assertIn(name, message)
                self.assertIn(f"index {k}", message)
                self.assertIn("test-finite", message)
                if name != "log_T_MeV":
                    self.assertIn(f"T={exp(log_T[k]):.6g} MeV", message)

        callbacks = build(log_T, r, s)
        for name, fn in callbacks._asdict().items():
            for T in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(callback=name, T=T):
                    with self.assertRaises(ComputationFailureError):
                        fn(T)
            with self.subTest(callback=name, T="finite"):
                # finite input is unchanged: a negative T still returns 0
                self.assertEqual(fn(-1.0), 0.0)
                self.assertTrue(np.isfinite(fn(1.0)))

    def test_e_prym_version(self):
        """(e) The PRyMordial version string names the run-integrity patch."""
        self.assertEqual(PRYM_VERSION, "bf24c3d+cham03+ri02")


if __name__ == "__main__":
    unittest.main()
