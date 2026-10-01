"""
Prompt 01 (integrator-remediation): the kinematic-cap step loop, integrate_scalar_history.

Each test drives the pure loop from a mid-history state of the integrator audit
(.documents/integrator-audit-2026-09-30/README.md §2.2), with the objects built as the audit's
harness.build does: QCD_Cosmology, Planck2018, Planck_units, ExponentialPotential(n = 1,
Lambda = 1e-3 eV), ExponentialCoupling, ODEPolicy, ODERHS and a ScalarFieldIntegrationSupervisor.
No Ray cluster and no datastore are used. Methods that integrate say how long they take.

Reference values are those of integrator-remediation README §6.1, measured by the audit's probes
on b1f64d8; where more digits were needed (pi at N = 21 from P1 at M = 0.5) they were measured
on 918590e with the audit's harness.run_fragment_loop(strategy="regions") at atol = rtol = 1e-8
and 1e-12, which agree to 1e-9 relative: phi(21) = 1.2203834e-1, pi(21) = -1.2971527e-1.
"""

import ast
import contextlib
import io
import os
import unittest
from math import inf, log
from typing import List, Optional, Tuple

import numpy as np
from scipy.optimize import brentq

import importlib

# ComputeTargets re-exports the ScalarModel class under the module's name, so import the module
SM = importlib.import_module("ComputeTargets.ScalarModel")

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts import beta_value, M_value, Lambda_value, temperature
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import ExponentialCoupling
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Quadrature.supervisors.ScalarField import (
    ScalarFieldIntegrationSupervisor,
    StateVector,
)
from Units import Planck_units

# the audit's probe states (integrator audit README §2.2; harness.py P1, P2, P3), in Planck units:
# (beta, N0, (phi_E, pi_E, ln rho_rad_E, ln f_m, ln T_J))
P1 = (
    2.0,
    20.0016270506,
    (
        0.174417541178,
        -0.497741179951,
        -166.040957866,
        -20.8145686243,
        -42.6274655299,
    ),
)
P2 = (
    2.0,
    25.003077235,
    (
        0.0242924190654,
        0.00184365317107,
        -185.456771851,
        -16.7033554358,
        -46.7288490575,
    ),
)
P3 = (
    1.2,
    32.8965084954,
    (
        0.00437706545159,
        -0.0116641233111,
        -232.843822983,
        -5.0399304446,
        -58.2427706427,
    ),
)

# the audit's step-over state at M = 0.01 (inside the wall, phi_wall ~ 9.2e-5): README §6.1 (e)
INSIDE_WALL_STATE = (5e-5, -0.4976, -166.04, -20.81, -42.63)

_units = Planck_units()
_params = Planck2018()
_cosmology = QCD_Cosmology(0, _units, _params)
_T_init = temperature(0, 2.0e4 * _units.GeV)
_T_stop = temperature(1, _params.T_CMB_Kelvin * _units.Kelvin)
_log_T_stop = log(float(_params.T_CMB_Kelvin * _units.Kelvin))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


class NonReflectingExponentialPotential(ExponentialPotential):
    """A stand-in that does not declare the wall (guard G1)."""

    @property
    def reflects_at_origin(self) -> bool:
        return False


def build(beta: float, M: float, potential_class=ExponentialPotential):
    potential = potential_class(
        0,
        M_value(0, M * _units.PlanckMass),
        Lambda_value(0, 1e-3 * _units.eV),
        1,
        _units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), _units)
    policy = SM.ODEPolicy("test", _cosmology, potential, coupling)
    rhs = SM.ODERHS("test", policy)
    supervisor = ScalarFieldIntegrationSupervisor(
        _units, _T_init, _T_stop, label="test", collect_full_statistics=False
    )
    return policy, rhs, supervisor


def integrate(
    probe,
    M: float,
    N_stop: float,
    params=SM.StepControl(),
    rhs_wrapper=None,
    potential_class=ExponentialPotential,
    N0: Optional[float] = None,
    state: Optional[tuple] = None,
) -> Tuple[SM.IntegrationResult, str]:
    """Run integrate_scalar_history from a probe state to N_stop; return (result, stdout)."""
    beta, probe_N0, probe_state = probe
    policy, rhs, supervisor = build(beta, M, potential_class)
    if rhs_wrapper is not None:
        rhs = rhs_wrapper(rhs)

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), supervisor:
        result = SM.integrate_scalar_history(
            rhs,
            supervisor,
            StateVector._make(probe_state if state is None else state),
            probe_N0 if N0 is None else N0,
            _log_T_stop,
            params,
            policy=policy,
            N_stop=N_stop,
        )
    return result, buffer.getvalue()


def turning_points(result: SM.IntegrationResult) -> List[tuple]:
    """
    Sign changes of pi between consecutive accepted states (and across a reflection), as the
    audit's harness records them: (N, phi, pi_before, pi_after), phi taken at the accepted state
    after the change.
    """
    sol = result.solution
    turns = []
    last_pi = sol.interpolants[0](sol.ts[0])[1]
    for k, interpolant in enumerate(sol.interpolants):
        for N in (sol.ts[k], sol.ts[k + 1]):
            y = interpolant(N)
            if (y[1] < 0.0) != (last_pi < 0.0):
                turns.append((N, y[0], last_pi, y[1]))
            last_pi = y[1]
    return turns


def wall_bounces(result: SM.IntegrationResult, M: float) -> List[tuple]:
    """harness.wall_bounces: turning points with pi going - to + and phi < 1.5 M."""
    return [t for t in turning_points(result) if t[1] < 1.5 * M and t[2] < 0.0 < t[3]]


def interpolated_minima(result: SM.IntegrationResult, M: float) -> List[tuple]:
    """
    (N, phi) at the root of pi on the interpolant of every accepted step on which pi goes from
    negative to positive with phi < 1.5 M: the minimum of phi along the dense output.
    """
    sol = result.solution
    minima = []
    for k, interpolant in enumerate(sol.interpolants):
        a, b = sol.ts[k], sol.ts[k + 1]
        if interpolant(a)[1] < 0.0 < interpolant(b)[1]:
            N = brentq(lambda t: interpolant(t)[1], a, b, xtol=1e-15)
            phi = interpolant(N)[0]
            if phi < 1.5 * M:
                minima.append((N, phi))
    return minima


class FailFirstStepCalls:
    """
    Wrap an ODERHS so that it raises ComputationFailureError on its first three calls once the
    loop has begun stepping (the supervisor holds a step cap from the first step onwards), i.e.
    on trial states inside solver.step(), not on Radau's start-up evaluations.
    """

    def __init__(self, rhs, failures: int = 3):
        self.rhs = rhs
        self.policy = rhs.policy
        self.remaining = failures

    def __call__(self, N, s, supervisor):
        if supervisor._current_step_cap is not None and self.remaining > 0:
            self.remaining -= 1
            raise ComputationFailureError("test: injected trial-state failure")
        return self.rhs(N, s, supervisor)


def rel(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


class TestKinematicCapLoopP1(unittest.TestCase):
    """README §6.1 (a)-(c): the first reflection from the P1 state, to N = 21."""

    def check_common(self, result: SM.IntegrationResult, phi_21: float):
        self.assertEqual(result.N_final, 21.0)
        self.assertLessEqual(rel(result.final_state.phi_Einstein, phi_21), 1e-5)
        if result.max_wall_to_kinetic_ratio is not None:
            self.assertLessEqual(result.max_wall_to_kinetic_ratio, 1e-3)

    def test_a_M_0p5(self):
        """P1 at M = 0.5, 1945 RHS; about 0.2 s. The RHS bound fails on HEAD~1 (17 092 RHS)."""
        result, _ = integrate(P1, 0.5, 21.0)
        self.assertLessEqual(result.nfev, 3000)
        bounces = wall_bounces(result, 0.5)
        self.assertGreaterEqual(len(bounces), 1)
        N_b, phi_min = bounces[0][0], bounces[0][1]
        self.assertLessEqual(abs(N_b - 20.343028), 1e-5)
        self.assertLessEqual(rel(phi_min, 4.57371e-3), 1e-4)
        self.check_common(result, 1.2203834e-1)
        self.assertLessEqual(rel(result.final_state.pi_Einstein, -1.2971527e-1), 1e-5)
        self.assertEqual(len(result.reflections), 0)

    def test_a_M_0p01(self):
        """P1 at M = 0.01, 2120 RHS; about 0.2 s."""
        result, _ = integrate(P1, 0.01, 21.0)
        self.assertLessEqual(result.nfev, 3500)
        bounces = wall_bounces(result, 0.01)
        self.assertGreaterEqual(len(bounces), 1)
        self.assertLessEqual(rel(bounces[0][1], 9.1505e-5), 1e-4)
        self.check_common(result, 1.185154e-1)
        self.assertEqual(len(result.reflections), 0)

    def test_a_M_0p001(self):
        """P1 at M = 0.001, 2275 RHS; about 0.2 s."""
        result, _ = integrate(P1, 0.001, 21.0)
        self.assertLessEqual(result.nfev, 3500)
        bounces = wall_bounces(result, 0.001)
        self.assertGreaterEqual(len(bounces), 1)
        self.assertLessEqual(rel(bounces[0][1], 9.1505e-6), 1e-4)
        self.check_common(result, 1.184501e-1)
        self.assertEqual(len(result.reflections), 0)

    def check_reflected(self, M: float):
        result, _ = integrate(P1, M, 21.0)
        self.assertEqual(len(result.reflections), 1)
        self.assertGreaterEqual(result.reflections[0].phi_Einstein, 1e-11)
        self.assertLessEqual(result.reflections[0].phi_Einstein, 1e-10)
        self.assertLess(result.reflections[0].pi_Einstein_in, 0.0)
        self.assertIsNotNone(result.max_wall_to_kinetic_ratio)
        self.check_common(result, 1.184428e-1)

    def test_b_M_1em10(self):
        """P1 at M = 1e-10: one elastic reflection, about 0.2 s. Fails on HEAD~1 ("Required step
        size is less than spacing between numbers", audit p_smallM_scan.py regions 1e-10).
        """
        self.check_reflected(1e-10)

    def test_b_M_4p1em28(self):
        """P1 at M = 4.1e-28 (M = 1 eV): one elastic reflection, about 0.2 s."""
        self.check_reflected(4.1e-28)

    def test_c_convergence_in_f(self):
        """P1 at M = 0.5 with cap_fraction = 0.02 meets the f = 0.1 first-bounce tolerances;
        about 0.3 s."""
        result, _ = integrate(P1, 0.5, 21.0, params=SM.StepControl(cap_fraction=0.02))
        bounces = wall_bounces(result, 0.5)
        self.assertGreaterEqual(len(bounces), 1)
        self.assertLessEqual(abs(bounces[0][0] - 20.343028), 1e-5)
        self.assertLessEqual(rel(bounces[0][1], 4.57371e-3), 1e-4)
        self.check_common(result, 1.2203834e-1)


class TestKinematicCapLoopWindows(unittest.TestCase):
    """README §6.1 (b), (c): the P3 grazing window and the P2 parked window."""

    def test_d_P3_window(self):
        """P3 (beta = 1.2, M = 0.01) to N = 37.5: 38 547 RHS, about 2.5 s."""
        result, _ = integrate(P3, 0.01, 37.5)
        self.assertLessEqual(result.nfev, 60000)
        self.assertEqual(len(result.reflections), 0)
        self.assertEqual(len(wall_bounces(result, 0.01)), 51)
        self.assertLessEqual(rel(result.final_state.phi_Einstein, 5.8078e-4), 1e-4)

        # phi_min of bounces 1, 2, 8: the minimum of phi on the dense output (see the log of
        # prompt 01 for why the value at the accepted step after the turn is not used here)
        minima = interpolated_minima(result, 0.01)
        self.assertEqual(len(minima), 51)
        for index, reference in ((0, 2.79886e-4), (1, 3.03650e-4), (7, 3.66368e-4)):
            self.assertLessEqual(rel(minima[index][1], reference), 2e-4)

    def test_e_P2_window_and_jacobian_clamp(self):
        """P2 (beta = 2, M = 0.5) to N = 40: 18 880 RHS, about 2.5 s. No T_Jordan = 0
        substitution is printed (35 without the clamp and the cap, audit §8 F2)."""
        result, captured = integrate(P2, 0.5, 40.0)
        self.assertLessEqual(result.nfev, 25000)
        self.assertNotIn("T_Jordan = 0", captured)
        self.assertEqual(len(wall_bounces(result, 0.5)), 19)
        self.assertLessEqual(rel(result.final_state.phi_Einstein, 1.909693e-2), 1e-5)
        self.assertEqual(len(result.reflections), 0)


class TestKinematicCapLoopFailurePaths(unittest.TestCase):
    """README §6.1 (e)."""

    def test_f_cap_disabled_raises_on_phi_nonpositive(self):
        """cap_fraction = inf from P1 at M = 0.01 steps over the wall; about 0.1 s."""
        with self.assertRaises(ComputationFailureError) as ctx:
            integrate(P1, 0.01, 21.0, params=SM.StepControl(cap_fraction=inf))
        self.assertIn("phi <= 0", ctx.exception.message)

    def test_f_trial_state_exceptions_are_rejected_steps(self):
        """Three injected RHS failures on trial states from P1 at M = 0.5; about 0.2 s."""
        result, _ = integrate(P1, 0.5, 21.0, rhs_wrapper=FailFirstStepCalls)
        self.assertGreaterEqual(result.steps_rejected_by_exception, 1)
        self.assertLessEqual(rel(result.final_state.phi_Einstein, 1.2203834e-1), 1e-5)

    def test_h_G1_undeclared_wall_fails_at_the_floor(self):
        """P1 at M = 1e-10 under a potential that does not declare reflects_at_origin; about 0.1 s."""
        with self.assertRaises(ComputationFailureError) as ctx:
            integrate(
                P1,
                1e-10,
                21.0,
                potential_class=NonReflectingExponentialPotential,
            )
        self.assertIn("reflects_at_origin", ctx.exception.message)
        self.assertIn("ExponentialPotential(", ctx.exception.message)

    def test_h_G2_reflection_inside_the_wall_fails(self):
        """The audit's step-over state at M = 0.01 with h_floor = 1e-3, so that the floor fires at
        once; W/(pi^2/2) is about 23. No step is taken."""
        with self.assertRaises(ComputationFailureError) as ctx:
            integrate(
                P1,
                0.01,
                21.0,
                params=SM.StepControl(cap_floor=1e-3),
                N0=20.35,
                state=INSIDE_WALL_STATE,
            )
        message = ctx.exception.message
        self.assertIn("inside the wall", message)
        ratio = float(message.split("W/(pi^2/2) = ")[1].split(" ")[0])
        self.assertGreater(ratio, 20.0)
        self.assertLess(ratio, 26.0)


class TestStoredMetadataAndLabel(unittest.TestCase):
    """README §6.1 (f): the stored keys and the stepper label."""

    @staticmethod
    def stand_in(**overrides) -> dict:
        data = {
            "reflections": 2,
            "cap_fraction": 0.1,
            "cap_floor": 1e-11,
            "cap_global_max_step": 0.1,
            "jacobian_factor_max": 1e-4,
            "accepted_steps": 4476,
            "steps_rejected_by_exception": 3,
            "largest_RHS_values": None,
            "smallest_RHS_values": None,
            "mean_RHS_values": None,
        }
        data.update(overrides)
        return data

    def test_g_extra_data_keys(self):
        self.assertEqual(
            set(SM.build_extra_data(self.stand_in()).keys()),
            {
                SM.REFLECTIONS_KEY,
                "cap_fraction",
                "cap_floor",
                "cap_global_max_step",
                "jacobian_factor_max",
                "accepted_steps",
                "steps_rejected_by_exception",
            },
        )
        self.assertEqual(SM.REFLECTIONS_KEY, "number_reflections")

    def test_g_zero_counts_are_absent(self):
        keys = set(
            SM.build_extra_data(
                self.stand_in(reflections=0, steps_rejected_by_exception=0)
            ).keys()
        )
        self.assertNotIn(SM.REFLECTIONS_KEY, keys)
        self.assertNotIn("steps_rejected_by_exception", keys)
        self.assertEqual(
            keys,
            {
                "cap_fraction",
                "cap_floor",
                "cap_global_max_step",
                "jacobian_factor_max",
                "accepted_steps",
            },
        )

    def test_g_stepper_label_registered_in_the_scripts(self):
        self.assertEqual(SM.SCALAR_MODEL_STEPPER_LABEL, "Radau+kinematic-cap-stepping0")
        for script in ("main.py", "plot_by_beta.py", "plot_ScalarModel.py"):
            with open(os.path.join(REPO_ROOT, script)) as f:
                tree = ast.parse(f.read(), filename=script)
            strings = {
                node.value
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
            }
            self.assertIn("Radau+kinematic-cap-stepping0", strings, script)
            if script == "main.py":
                self.assertIn("Radau+kinematic-cap", strings, script)


if __name__ == "__main__":
    unittest.main()
