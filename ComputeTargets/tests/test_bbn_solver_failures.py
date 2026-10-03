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
new-physics samples are refused before any solve. Since science-readiness
prompt 01, also: the thermodynamic solve has two components and rho_NP reaches
PRyMordial only through Hubble(); a solve has an optional wall-clock limit; a
successful return is checked before it is stored; and the callback builder
refuses a short sample grid and a non-finite value.

Written for run-integrity prompt 02 (item F, README section 6.2). Before that
prompt none of the eight `solve_ivp` calls in `PRyM/PRyM_main.py` checked
`.success`, so a solve that gave up returned plausible abundances; only
`(OverflowError, ValueError, ComputationFailureError)` became a failure
payload; and the callback builder never checked its samples for finiteness.
Science-readiness prompt 01 rewrote tests (c)-(e) for the one rho_NP callback
and added (f)-(i) (its README section 6.2).

**This module runs partial PRyMordial solves, about 50 s in all.** A failure is
forced as `prompts/run-integrity/planning-probes/prymordial_solver_probe.py`
forces it: the name `PRyM.PRyM_main.solve_ivp`, which PRyMordial looks up at
call time, is replaced by a wrapper that integrates the first 1 % of the k-th
call's span and then marks the result failed, as `solve_ivp` marks a solver
that gives up. The call is selected by its order, not by line number. Every
PRyMordial module global touched is restored, and so is `solve_ivp`.

Tests (d), (e), (h) and (i) run no solve; (f) runs one small-network solve and
(g) a solve cut short by its wall-clock limit. New names are looked up inside
the test bodies, so that on the tree before the prompt that added them each
test fails on its own assertion where it can rather than the module failing to
import.

Nothing here needs a Ray cluster or a datastore. Run from the repository root,
since PRyMordial reads `PRyMrates/` from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import sys
import time
import unittest
from math import exp, isnan
from unittest import mock

import numpy as np

import PRyM.PRyM_main as PRyMmain
from ComputeTargets.BBNData import (
    PRYM_VERSION,
    compute_SM_baseline,
)
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.prym_fixtures import (
    CONSTANT,
    RES_D_OVER_H_E5,
    RES_LI7_OVER_H_E10,
    RES_YP_BBN,
    ZERO,
    SavedPRyMGlobals,
    run_prym,
)

# the exception class PRyM/PRyM_main.py raises for a failed solve_ivp
FAILURE_CLASS_NAME = "PRyMSolverFailureError"

# the number of solve_ivp calls PRyMordial makes under production's flags
# (NP_hubble_flag, aTid_flag, compute_bckg_flag true; julia_flag false; until
# science-readiness prompt 01 NP_thermo_flag rather than NP_hubble_flag), per
# network: thermodynamics, a(T), high-T n <-> p, mid-T, low-T
N_CALLS = 5

# with small_network=True, the calls that differ from the full network's
SMALL_NETWORK_CALLS = (4, 5)

FORCED_FRACTION = 0.01
FORCED_MESSAGE = "forced failure (test_bbn_solver_failures)"

# (g) the wall-clock limit on the constant family, and the bound on how long
# the cut-short solve may take (science-readiness README section 6.2)
WALL_CLOCK_TEST_LIMIT = 1e-3
WALL_CLOCK_RETURN_BOUND_S = 5.0
WALL_CLOCK_FAILURE_PREFIX = "PRyMordial: PRyMWallClockLimitError"

# (h) the stubbed results: [N_eff, ., ., Yp (CMB), Yp (BBN), D/H x1e5,
# 3He/H x1e5, 7Li/H x1e10], inside the output checks
GOOD_RESULTS = [3.04, 0.0, 0.0, 0.245, 0.247, 2.46, 1.04, 5.42]
OUTPUT_FAILURE_PREFIX = "PRyMordial output:"

# (i) a temperature at which the stand-in EOS returns a NaN g_rho
NAN_G_RHO_T_MEV = 0.5


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
    rho_NP = 0 through run_prym, with the fail_at-th solve_ivp call forced to
    fail. Returns (the spy, the abundances or None, the exception or None).
    """
    spy = _ForcedFailure(fail_at)
    res, raised = None, None
    with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
        try:
            res = run_prym(ZERO.rho, small_network=small_network)
        except Exception as e:
            raised = e
    return spy, res, raised


class _Recorder:
    """
    Stands in for PRyM_main.solve_ivp in test (f): records each call's y0 and
    passes it through.
    """

    def __init__(self):
        self._solve_ivp = PRyMmain.solve_ivp
        self.y0s = []

    def __call__(self, fun, t_span, y0, **kwargs):
        self.y0s.append(list(y0))
        return self._solve_ivp(fun, t_span, y0, **kwargs)


class _StubPRyMclass:
    """Stands in for PRyMclass in test (h): no solve, fixed results."""

    results = GOOD_RESULTS

    def __init__(self, *args, **kwargs):
        pass

    def PRyMresults(self):
        return list(type(self).results)


class _NaNAtOneT:
    """
    An EOS stand-in for test (i): the real Saikawa-Shirai G_rho, except NaN
    within 1e-9 relative of NAN_G_RHO_T_MEV.
    """

    def __init__(self, units):
        from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
            SaikawaShirai_EOS_spline,
        )

        self._eos = SaikawaShirai_EOS_spline(units)
        self._MeV = units.MeV

    def G_rho(self, T):
        if abs(T / self._MeV - NAN_G_RHO_T_MEV) <= 1e-9 * NAN_G_RHO_T_MEV:
            return float("nan")
        return self._eos.G_rho(T)


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
        **Two partial PRyMordial solves.** (Rewritten for the one rho_NP
        callback by science-readiness prompt 01; the RuntimeError case shows
        that an exception raised by a callback inside an LSODA right-hand side
        propagates out of PRyMclass, on which the wall-clock limit relies.)"""
        from ComputeTargets.BBNData import _run_PRyMordial

        with self.subTest("forced solve_ivp failure"):
            spy = _ForcedFailure(1)
            with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
                payload = _run_PRyMordial(
                    ZERO.rho, small_network=False, wall_clock_limit=None
                )
            self.assertTrue(spy.forced)
            self.assertIs(payload.get("failure"), True, payload)
            reason = payload["failure_reason"]
            print(f"\n[test_bbn_solver_failures (c)] forced: {reason}")
            self.assertTrue(reason.startswith(f"PRyMordial: {FAILURE_CLASS_NAME}: "))

        def raising(T_in_MeV: float) -> float:
            raise RuntimeError("synthetic callback failure")

        with self.subTest("RuntimeError from a callback"):
            with SavedPRyMGlobals():
                payload = _run_PRyMordial(
                    raising, small_network=False, wall_clock_limit=None
                )
            self.assertIs(payload.get("failure"), True, payload)
            reason = payload["failure_reason"]
            print(f"[test_bbn_solver_failures (c)] RuntimeError: {reason}")
            self.assertEqual(
                reason, "PRyMordial: RuntimeError: synthetic callback failure"
            )

        with self.subTest("compute_SM_baseline raises"):
            spy = _ForcedFailure(1)
            with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", spy):
                with self.assertRaises(Exception) as ctx:
                    compute_SM_baseline(False)
            self.assertEqual(type(ctx.exception).__name__, FAILURE_CLASS_NAME)

    def test_d_non_finite_samples_are_refused(self):
        """(d) build_rho_NP_callback refuses a NaN and an infinite value in
        density_ratio and a NaN in log_T_MeV with ComputationFailureError naming
        the array, the index and T; a callback built from finite samples raises
        ComputationFailureError for a non-finite T. **No solve**: before
        run-integrity prompt 02 a NaN new-physics value hangs PRyMordial.
        (Rewritten for the one callback by science-readiness prompt 01, which
        removed the pressure ratio array; its infinite case moved to
        density_ratio.)"""
        from ComputeTargets.BBNData import build_rho_NP_callback

        n, k = 40, 17
        log_T = np.linspace(np.log(100.0), np.log(1.0e-7), n)
        r = np.full(n, 0.08)

        def rho_SM(T):
            return T**4

        def build(x, dr):
            return build_rho_NP_callback(
                x,
                dr,
                rho_SM,
                T_min_MeV=1.0e-7,
                T_max_MeV=100.0,
                task_label="test-finite",
            )

        cases = (
            ("density_ratio", float("nan")),
            ("density_ratio", float("inf")),
            ("log_T_MeV", float("nan")),
        )
        for name, bad in cases:
            with self.subTest(name=name, bad=bad):
                arrays = {"log_T_MeV": log_T.copy(), "density_ratio": r.copy()}
                arrays[name][k] = bad
                with self.assertRaises(ComputationFailureError) as ctx:
                    build(arrays["log_T_MeV"], arrays["density_ratio"])
                message = str(ctx.exception)
                print(f"\n[test_bbn_solver_failures (d)] {name}: {message}")
                self.assertIn(name, message)
                self.assertIn(f"index {k}", message)
                self.assertIn("test-finite", message)
                if name != "log_T_MeV":
                    self.assertIn(f"T={exp(log_T[k]):.6g} MeV", message)

        rho_NP = build(log_T, r)
        for T in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(T=T):
                with self.assertRaises(ComputationFailureError):
                    rho_NP(T)
        with self.subTest(T="finite"):
            # finite input is unchanged: a negative T still returns 0
            self.assertEqual(rho_NP(-1.0), 0.0)
            self.assertTrue(np.isfinite(rho_NP(1.0)))

    def test_e_prym_version(self):
        """(e) The PRyMordial version string names the run-integrity,
        science-readiness and bbn-tolerance patches, and no longer the reverted
        cham03. Re-pinned 2026-10-03 by bbn-tolerance prompt 02 (the low-T rtol,
        "+bt02"); it was "bf24c3d+ri02+sr01"."""
        self.assertEqual(PRYM_VERSION, "bf24c3d+ri02+sr01+bt02")

    def test_f_thermodynamics_has_two_components(self):
        """(f) rho_NP through the production flags (_configure_PRyMordial), small
        network: the thermodynamic solve_ivp (the first call) is given a
        two-component y0, (T_gamma, T_nu), and the solve completes. A recording
        rho_NP callback is called, and only from Hubble(). Fails before
        science-readiness prompt 01, where y0 was (T_gamma, T_nu, T_NP) and
        rho_NP was also read by the plasma equation and N_eff.
        **Runs one small-network PRyMordial solve, about 6 s.**"""
        from ComputeTargets.BBNData import _configure_PRyMordial

        callers = {}

        def recording(T_in_MeV: float) -> float:
            name = sys._getframe(1).f_code.co_name
            callers[name] = callers.get(name, 0) + 1
            return CONSTANT.rho(T_in_MeV)

        recorder = _Recorder()
        with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", recorder):
            PRyMmain_ = _configure_PRyMordial(True)
            res = PRyMmain_.PRyMclass(recording).PRyMresults()

        print(
            f"\n[test_bbn_solver_failures (f)] solve_ivp y0 lengths "
            f"{[len(y) for y in recorder.y0s]}; rho_NP callers {callers}; "
            f"Yp {res[RES_YP_BBN]:.10g}, D/H x1e5 {res[RES_D_OVER_H_E5]:.10g}"
        )
        self.assertEqual(len(recorder.y0s), N_CALLS)
        self.assertEqual(len(recorder.y0s[0]), 2)
        self.assertGreater(callers.get("Hubble", 0), 0)
        self.assertEqual(set(callers), {"Hubble"})

    def test_g_wall_clock_limit(self):
        """(g) The constant family through the PRyMordial helper with
        wall_clock_limit=1e-3 s returns, in under 5 s, a failure payload whose
        reason begins "PRyMordial: PRyMWallClockLimitError" and names a stage
        that _check_solve_ivp names. **One PRyMordial solve, cut short.**
        (wall_clock_limit=None changing nothing is test_prym_passenger (c),
        which passes None.)"""
        from ComputeTargets.BBNData import _run_PRyMordial

        stages = (
            "thermodynamics (no NP)",
            "a(T)",
            "high-T n <-> p",
            "mid-T nuclear network (full)",
            "low-T nuclear network (full)",
        )

        start = time.perf_counter()
        with SavedPRyMGlobals():
            payload = _run_PRyMordial(
                CONSTANT.rho,
                small_network=False,
                wall_clock_limit=WALL_CLOCK_TEST_LIMIT,
            )
        wall = time.perf_counter() - start

        print(f"\n[test_bbn_solver_failures (g)] {wall:.3f} s: {payload}")
        self.assertIs(payload.get("failure"), True, payload)
        reason = payload["failure_reason"]
        self.assertTrue(reason.startswith(WALL_CLOCK_FAILURE_PREFIX), reason)
        self.assertTrue(any(f"'{stage}'" in reason for stage in stages), reason)
        self.assertLess(wall, WALL_CLOCK_RETURN_BOUND_S)

    def test_h_output_checks(self):
        """(h) With PRyMclass stubbed (no solve), a result with Yp = 0.7, then
        one with D/H = NaN, then one with 7Li/H = 0, each gives a failure
        payload beginning "PRyMordial output:" and naming the value; a result
        inside the checks passes through unchanged. Fails before
        science-readiness prompt 01, which stored all four as results."""
        from ComputeTargets.BBNData import _run_PRyMordial

        def with_result(index, value):
            results = list(GOOD_RESULTS)
            results[index] = value
            return results

        cases = (
            ("Yp_BBN", with_result(RES_YP_BBN, 0.7)),
            ("DOverH", with_result(RES_D_OVER_H_E5, float("nan"))),
            ("Li7OverH", with_result(RES_LI7_OVER_H_E10, 0.0)),
        )
        for name, results in cases:
            with self.subTest(name):
                with SavedPRyMGlobals(), mock.patch.object(
                    PRyMmain, "PRyMclass", _StubPRyMclass
                ), mock.patch.object(_StubPRyMclass, "results", results):
                    payload = _run_PRyMordial(
                        ZERO.rho, small_network=False, wall_clock_limit=None
                    )
                print(f"\n[test_bbn_solver_failures (h)] {name}: {payload}")
                self.assertIs(payload.get("failure"), True, payload)
                reason = payload["failure_reason"]
                self.assertTrue(reason.startswith(OUTPUT_FAILURE_PREFIX), reason)
                self.assertIn(f"{name}=", reason)

        with self.subTest("inside the checks"):
            with SavedPRyMGlobals(), mock.patch.object(
                PRyMmain, "PRyMclass", _StubPRyMclass
            ):
                payload = _run_PRyMordial(
                    ZERO.rho, small_network=False, wall_clock_limit=None
                )
            self.assertEqual(
                payload,
                {
                    "Yp_BBN": GOOD_RESULTS[RES_YP_BBN],
                    "DOverH": GOOD_RESULTS[RES_D_OVER_H_E5],
                    "He3OverH": GOOD_RESULTS[6],
                    "Li7OverH": GOOD_RESULTS[RES_LI7_OVER_H_E10],
                },
            )

    def test_i_callback_builder_refusals(self):
        """(i) The callback builder raises ComputationFailureError for three
        samples, naming the count; and the callback raises
        ComputationFailureError at a T where an EOS stand-in's G_rho is NaN.
        No solve. Fails before science-readiness prompt 01, where three samples
        raised ValueError (from make_interp_spline) and the NaN was returned."""
        from ComputeTargets.BBNData import build_rho_NP_callback, thermodynamic_rho_SM
        from Units import GeV_units

        with self.subTest("three samples"):
            log_T = np.log([10.0, 1.0, 0.1])
            with self.assertRaises(ComputationFailureError) as ctx:
                build_rho_NP_callback(
                    log_T,
                    np.full(3, 0.08),
                    lambda T: T**4,
                    T_min_MeV=1.0e-7,
                    T_max_MeV=100.0,
                    task_label="test-short",
                )
            message = str(ctx.exception)
            print(f"\n[test_bbn_solver_failures (i)] three samples: {message}")
            self.assertIn("3", message)
            self.assertIn("test-short", message)

        with self.subTest("NaN rho_SM"):
            units = GeV_units()
            rho_SM = thermodynamic_rho_SM(_NaNAtOneT(units), units)
            self.assertTrue(isnan(rho_SM(NAN_G_RHO_T_MEV)))
            log_T = np.linspace(np.log(100.0), np.log(1.0e-7), 40)
            rho_NP = build_rho_NP_callback(
                log_T,
                np.full(40, 0.08),
                rho_SM,
                T_min_MeV=1.0e-7,
                T_max_MeV=100.0,
                task_label="test-nan-eos",
            )
            self.assertTrue(np.isfinite(rho_NP(1.0)))
            with self.assertRaises(ComputationFailureError) as ctx:
                rho_NP(NAN_G_RHO_T_MEV)
            message = str(ctx.exception)
            print(f"[test_bbn_solver_failures (i)] NaN rho_SM: {message}")
            self.assertIn("test-nan-eos", message)
            self.assertIn(f"T={NAN_G_RHO_T_MEV:.6g} MeV", message)


if __name__ == "__main__":
    unittest.main()
