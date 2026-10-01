"""
Prompt 02 (integrator-remediation): the solver fallback is gone and the exception taxonomy of
README §2 (h) is settled.

The states, the builder and the helpers are those of prompt 01's test module
(test_kinematic_cap_loop.py), imported rather than duplicated. No Ray cluster and no datastore
are used. Methods that integrate say how long they take.

The reference values of (c) are the floats prompt 01's tree (fc97233) gives for P1 at M = 0.5 to
N = 21, printed with repr() (log 01 "State handed to the next prompt" for phi and pi; the
first-bounce pair measured on an export of fc97233 by prompt 02, with the same helpers).
"""

import ast
import contextlib
import io
import os
import unittest
from math import nan

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.exceptions import ComputationFailureError

# the names are imported one by one, not the TestCase classes, so that discovery does not
# collect prompt 01's tests a second time from this module
from ComputeTargets.tests.test_kinematic_cap_loop import (
    P1,
    P2,
    REPO_ROOT,
    SM,
    _log_T_stop,
    build,
    integrate,
    rel,
    wall_bounces,
)
from Quadrature.supervisors.ScalarField import StateVector

# P1 at M = 0.5 to N = 21 on prompt 01's tree (fc97233), repr() of the floats
P1_M0p5_PHI_21 = 0.12203833994225839
P1_M0p5_PI_21 = -0.12971527073457434
P1_M0p5_FIRST_BOUNCE_N = 20.343034651924082
P1_M0p5_FIRST_BOUNCE_PHI = 0.004573794758933947

# text that the traceback prints removed by prompt 02 produced: print(f"type={exc_type}, ...")
# writes "type=<class ...", and traceback.print_tb writes '  File "...", line N, in ...' lines.
# print_tb never writes the word "Traceback"; it is checked as well, as the prompt asks.
TRACEBACK_MARKERS = ("Traceback", "type=<class", 'File "')


class NaNFrictionPolicy(SM.ODEPolicy):
    """A stand-in ODEPolicy whose friction_term is NaN, so that ODERHS's output is non-finite."""

    def __call__(self, N, state):
        return super().__call__(N, state)._replace(friction_term=nan)


class RaisingPolicy(SM.ODEPolicy):
    """
    A stand-in ODEPolicy that raises ComputationFailureError on its first `failures` calls once
    armed. The exception is raised inside ODERHS.__call__, i.e. inside RHS_timer, which is where
    the traceback print removed by prompt 02 used to fire.
    """

    def __init__(self, *args, failures: int = 3, **kwargs):
        super().__init__(*args, **kwargs)
        self.armed = False
        self.remaining = failures

    def __call__(self, N, state):
        if self.armed and self.remaining > 0:
            self.remaining -= 1
            raise ComputationFailureError(
                "test: injected trial-state failure in ODEPolicy"
            )
        return super().__call__(N, state)


class ArmOnFirstStep:
    """Wrap an ODERHS so that its RaisingPolicy is armed once the loop has begun stepping."""

    def __init__(self, rhs):
        self.rhs = rhs
        self.policy = rhs.policy

    def __call__(self, N, s, supervisor):
        if supervisor._current_step_cap is not None:
            self.policy.armed = True
        return self.rhs(N, s, supervisor)


def run_loop(probe, M, params=SM.StepControl(), N_failsafe=1000.0, N_stop=None):
    """integrate_scalar_history from a probe state, with stdout and stderr captured."""
    beta, N0, state = probe
    policy, rhs, supervisor = build(beta, M)
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            with supervisor:
                result = SM.integrate_scalar_history(
                    rhs,
                    supervisor,
                    StateVector._make(state),
                    N0,
                    _log_T_stop,
                    params,
                    N_failsafe=N_failsafe,
                    policy=policy,
                    N_stop=N_stop,
                )
            error = None
        except Exception as e:  # returned to the test, which asserts on its type
            result, error = None, e
    return result, error, out.getvalue() + err.getvalue()


class TestTrialStatePolicy(unittest.TestCase):
    """README §6.2: the RHS's diagnostic branch and _get_T_Jordan."""

    def test_a_nan_output_raises_computation_failure(self):
        """A NaN friction_term makes ODERHS raise ComputationFailureError. Fails on HEAD~1, where
        the diagnostic branch reads data.d_logV_dphi and raises AttributeError."""
        _, rhs, supervisor = build(P1[0], 0.5)
        policy = NaNFrictionPolicy(
            "test", rhs.policy.cosmology, rhs.policy.potential, rhs.policy.coupling
        )
        nan_rhs = SM.ODERHS("test", policy)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer), supervisor:
            with self.assertRaises(ComputationFailureError) as ctx:
                nan_rhs(P1[1], list(P1[2]), supervisor)
        self.assertIn("infinity or NaN", ctx.exception.message)
        self.assertIn("output from ODE RHS has infinity or NaN", buffer.getvalue())

    def test_b_nonpositive_T_Jordan_raises(self):
        """log_T_Jordan = -1e4 underflows exp() to 0: ODEPolicy raises ComputationFailureError.
        Fails on HEAD~1, where it prints and substitutes T_Jordan = 1 K."""
        policy, _, _ = build(P1[0], 0.5)
        state = StateVector._make(P1[2])._replace(log_T_Jordan=-1e4)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            with self.assertRaises(ComputationFailureError) as ctx:
                policy(P1[1], state)
        self.assertIn("T_Jordan = 0", ctx.exception.message)

    def test_c_physical_values_unchanged(self):
        """P1 at M = 0.5 to N = 21 reproduces prompt 01's tree to 1e-10 relative; about 0.2 s."""
        result, captured = integrate(P1, 0.5, 21.0)
        self.assertEqual(result.N_final, 21.0)
        self.assertLessEqual(
            rel(result.final_state.phi_Einstein, P1_M0p5_PHI_21), 1e-10
        )
        self.assertLessEqual(rel(result.final_state.pi_Einstein, P1_M0p5_PI_21), 1e-10)
        N_b, phi_min = wall_bounces(result, 0.5)[0][0:2]
        self.assertLessEqual(rel(N_b, P1_M0p5_FIRST_BOUNCE_N), 1e-10)
        self.assertLessEqual(rel(phi_min, P1_M0p5_FIRST_BOUNCE_PHI), 1e-10)
        self.assertEqual(result.nfev, 1945)
        self.assertEqual(result.accepted_steps, 224)
        self.assertEqual(result.steps_rejected_by_exception, 0)
        self.assertNotIn("T_Jordan = 0", captured)


class TestLoopFailures(unittest.TestCase):
    """README §6.2: the failsafe and the step budget are ComputationFailureError."""

    def test_d_failsafe_is_computation_failure(self):
        """From P1 at M = 0.5 with N_failsafe = N0 + 0.1 and no N_stop; about 0.1 s. The
        supervisor's exit prints no traceback."""
        result, error, captured = run_loop(P1, 0.5, N_failsafe=P1[1] + 0.1)
        self.assertIsNone(result)
        self.assertIsInstance(error, ComputationFailureError)
        self.assertNotIsInstance(error, RuntimeError)
        self.assertIn("failsafe", error.message)
        for marker in TRACEBACK_MARKERS:
            self.assertNotIn(marker, captured)

    def test_e_step_budget(self):
        """From P2 with step_budget = 50: fails at the 51st accepted step; about 0.1 s."""
        result, error, captured = run_loop(
            P2, 0.5, params=SM.StepControl(step_budget=50), N_stop=40.0
        )
        self.assertIsNone(result)
        self.assertIsInstance(error, ComputationFailureError)
        message = error.message
        self.assertTrue(message.startswith("step budget exhausted"), message)
        self.assertIn("took 51 accepted steps", message)
        self.assertIn("budget 50", message)
        self.assertIn("N=", message)
        self.assertIn("T_J=", message)
        self.assertIn("GeV", message)
        self.assertIn("0 reflection(s)", message)
        for marker in TRACEBACK_MARKERS:
            self.assertNotIn(marker, captured)

    def test_e_default_step_budget(self):
        self.assertEqual(SM.StepControl().step_budget, 2_000_000)
        self.assertEqual(SM.StepControl._fields[-1], "step_budget")


class TestQuietTimer(unittest.TestCase):
    """README §6.2: RHS_timer.__exit__ (and the supervisor's __exit__) print nothing."""

    def test_f_prompt_01_rejection_test_is_quiet(self):
        """Prompt 01's exception-rejection test, run with stdout and stderr captured; about 0.2 s."""
        out, err = io.StringIO(), io.StringIO()
        suite = unittest.TestSuite(
            [
                kcl.TestKinematicCapLoopFailurePaths(
                    "test_f_trial_state_exceptions_are_rejected_steps"
                )
            ]
        )
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            outcome = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(
                suite
            )
        self.assertTrue(outcome.wasSuccessful())
        captured = out.getvalue() + err.getvalue()
        for marker in TRACEBACK_MARKERS:
            self.assertNotIn(marker, captured)

    def test_f_rejection_inside_the_timer_is_quiet(self):
        """
        Three ComputationFailureErrors raised by the policy inside ODERHS (so inside RHS_timer)
        from P1 at M = 0.5: rejected steps, the same phi(21), and no traceback text on stdout or
        stderr; about 0.2 s. Fails on HEAD~1, where RHS_timer prints type= and print_tb lines
        for each. (Prompt 01's FailFirstStepCalls raises before ODERHS is entered, so it does not
        reach the timer.)
        """
        _, rhs, supervisor = build(P1[0], 0.5)
        policy = RaisingPolicy(
            "test", rhs.policy.cosmology, rhs.policy.potential, rhs.policy.coupling
        )
        wrapped = ArmOnFirstStep(SM.ODERHS("test", policy))
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with supervisor:
                result = SM.integrate_scalar_history(
                    wrapped,
                    supervisor,
                    StateVector._make(P1[2]),
                    P1[1],
                    _log_T_stop,
                    SM.StepControl(),
                    policy=policy,
                    N_stop=21.0,
                )
        self.assertEqual(policy.remaining, 0)
        self.assertGreaterEqual(result.steps_rejected_by_exception, 1)
        self.assertLessEqual(rel(result.final_state.phi_Einstein, P1_M0p5_PHI_21), 1e-5)
        captured = out.getvalue() + err.getvalue()
        for marker in TRACEBACK_MARKERS:
            self.assertNotIn(marker, captured)


class TestFallbackGone(unittest.TestCase):
    """README §6.2: one stepper, and one RuntimeError (the z grid) in the integration path."""

    @staticmethod
    def module_tree():
        path = os.path.join(REPO_ROOT, "ComputeTargets", "ScalarModel.py")
        with open(path) as f:
            return ast.parse(f.read(), filename=path)

    @staticmethod
    def function(tree, name):
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        raise AssertionError(f"{name} not found")

    def test_g_no_fallback(self):
        tree = self.module_tree()
        names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        self.assertNotIn("solver_list", names)

        # (ScalarModel's constructor keeps its solver_labels argument: the label -> IntegrationSolver
        # dictionary that store() reads. Only compute_scalar_model's dictionary of old names goes.)
        body = self.function(tree, "compute_scalar_model")
        body_names = {node.id for node in ast.walk(body) if isinstance(node, ast.Name)}
        self.assertNotIn("solver_labels", body_names)
        self.assertNotIn("success", body_names)
        strings = {
            node.value
            for node in ast.walk(body)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        for name in ("BDF", "LSODA", "DOP853"):
            self.assertFalse(any(name in s for s in strings), name)
        self.assertFalse(any(isinstance(node, ast.While) for node in ast.walk(body)))

    def test_g_one_runtime_error_in_the_integration_path(self):
        tree = self.module_tree()

        def runtime_errors(fn):
            sites = []
            for node in ast.walk(fn):
                if isinstance(node, ast.Raise) and node.exc is not None:
                    exc = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
                    if isinstance(exc, ast.Name) and exc.id == "RuntimeError":
                        sites.append(node)
            return sites

        self.assertEqual(
            len(runtime_errors(self.function(tree, "integrate_scalar_history"))), 0
        )
        sites = runtime_errors(self.function(tree, "compute_scalar_model"))
        self.assertEqual(len(sites), 1)
        message = ast.unparse(sites[0].exc)
        self.assertIn("largest supplied redshift", message)


if __name__ == "__main__":
    unittest.main()
