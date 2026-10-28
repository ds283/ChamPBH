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
The dark-radiation component rho_chi (inelastic-reflection prompt 01; README §2.2, gate "DR").

  (a) DarkRadiationLedger: K piecewise constant, the rule N_j <= N at an injection's own N, the
      a^{-4} scaling between injections, an empty ledger zero everywhere, add() out of order
      raising, and K_0 present below N_start (orchestrator note 01, ruling 1). Microseconds.
  (b) The constraint and E at constructed states, through ODEPolicy and HubblePolicy: both
      branches of the log-safe T, the V = 0 branch of HubblePolicy, r_chi from 1e-6 to 10.
      Milliseconds.
  (c) Hdot/H^2 at the same states, from rho + p and the constraint written out here, against
      PotentialDerivativePolicy.Hdot_over_H2_plus_3 - 3. Milliseconds.
  (d) A real history with an initial rho_chi: the constraint on the samples, and d ln H_E/dN on
      the dense output against the policy's Hdot/H^2. One integration, a few seconds.
  (e) The tuning: _tuning_state on a constructed last sample (milliseconds), and tune_history at
      the tuned golden point with an initial rho_chi (several integrations to T_CMB).

The golden histories (test_golden_history.py and test_tuning.py (e)) are the arbiter of K = 0
being the old code bitwise; they are not repeated here.

"To rounding" is a number (ruling 8 of the orchestrator note): each such assertion states its
bound as a relative tolerance, and the docstring says why it is rounding.
"""

import contextlib
import io
import json
import pickle
import unittest
from math import exp, log
from unittest import mock

import importlib

from ComputeTargets.dark_radiation import DarkRadiationLedger

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Quadrature.supervisors.ScalarField import StateVector
from Units import Planck_units

EPS = 2.0**-52

_units = Planck_units()
_cosmology = QCD_Cosmology(0, _units, Planck2018())
_Mp = _units.PlanckMass


def rounding_bound(*logs: float) -> float:
    """
    The rounding bound of the constructed-state checks, as a relative tolerance:
    (sum |ln x| + 64) EPS over the logarithms the policy exponentiates. exp(x) of an argument that
    carries an absolute rounding of |x| EPS/2 has a relative error of |x| EPS/2, and H^2 is rebuilt
    from exp(ln V - ln(V/3H^2 M_P^2)) and V/3H^2 M_P^2 from exp(ln rho_R - ln V), so each
    log of size |x| costs up to |x| EPS; the 64 covers the two dozen products, quotients and sums
    of positive terms in between, none of which cancels.
    """
    return (sum(abs(x) for x in logs) + 64.0) * EPS


# ---------------------------------------------------------------------------------------------
# (a) the ledger
# ---------------------------------------------------------------------------------------------


class TestTheLedger(unittest.TestCase):
    """(a) DarkRadiationLedger on numbers. Microseconds."""

    def test_a_K_is_piecewise_constant(self):
        led = DarkRadiationLedger()
        led.add(1.0, 2.0)
        led.add(3.0, 0.5)
        for N, K in ((0.0, 0.0), (0.999, 0.0), (1.0, 2.0), (2.0, 2.0), (2.999, 2.0)):
            self.assertEqual(led.K(N), K, N)
        for N in (3.0, 4.0, 100.0):
            self.assertEqual(led.K(N), 2.5, N)
        self.assertEqual(led.injections, ((1.0, 2.0), (3.0, 0.5)))

    def test_a_an_injection_counts_at_its_own_N(self):
        """The rule N_j <= N: K at N_j is the new K, and just below it the old."""
        led = DarkRadiationLedger(1.0)
        led.add(2.0, 3.0)
        self.assertEqual(led.K(2.0), 4.0)
        self.assertEqual(led.K(2.0 - 1e-12), 1.0)
        self.assertFalse(led.is_zero(2.0))
        self.assertEqual(led.rho_chi(2.0), exp(log(4.0) - 8.0))

    def test_a_rho_chi_scales_as_a_to_the_minus_4_between_injections(self):
        """
        rho_chi(N + dN)/rho_chi(N) = e^{-4 dN} on [N_j, N_{j+1}), to rounding: three exps, of
        arguments x_1 = ln rho_chi(N + dN), x_2 = ln rho_chi(N) and -4 dN, each with a relative error
        up to |x| EPS/2, so the bound is (|x_1| + |x_2| + 4 dN + 8) EPS.
        """
        led = DarkRadiationLedger(3.0e-40)
        led.add(5.0, 7.0e-30)
        for N, dN in ((0.0, 2.0), (5.0, 1.5), (6.0, 10.0)):
            ratio = led.rho_chi(N + dN) / led.rho_chi(N)
            bound = (
                abs(log(led.rho_chi(N + dN))) + abs(log(led.rho_chi(N))) + 4 * dN + 8
            ) * EPS
            self.assertLessEqual(abs(ratio / exp(-4.0 * dN) - 1.0), bound, (N, dN))
        # and r_chi carries the same law against a fixed rho_R
        x1, x2 = log(led.ratio(7.0, -100.0)), log(led.ratio(6.0, -100.0))
        self.assertLessEqual(
            abs(led.ratio(7.0, -100.0) / led.ratio(6.0, -100.0) / exp(-4.0) - 1.0),
            (abs(x1) + abs(x2) + 4 + 8) * EPS,
        )

    def test_a_an_empty_ledger_is_zero_everywhere(self):
        led = DarkRadiationLedger()
        self.assertTrue(led.is_empty)
        for N in (-10.0, 0.0, 1e-300, 50.0, 1e3):
            self.assertTrue(led.is_zero(N))
            self.assertEqual(led.K(N), 0.0)
            self.assertEqual(led.rho_chi(N), 0.0)
            self.assertEqual(led.ratio(N, -150.0), 0.0)

    def test_a_add_out_of_order_raises(self):
        led = DarkRadiationLedger()
        led.add(2.0, 1.0)
        led.add(2.0, 1.0)  # equal N is allowed
        with self.assertRaises(ValueError):
            led.add(1.999, 1.0)
        self.assertEqual(led.injections, ((2.0, 1.0), (2.0, 1.0)))

    def test_a_K_initial_is_present_below_N_start(self):
        """Ruling 1: K_0 is the same at N_start - 1e-9 as at N_start (N_start = 0)."""
        led = DarkRadiationLedger(5.0e-43)
        self.assertEqual(led.K(-1e-9), led.K(0.0))
        self.assertEqual(led.K(0.0), 5.0e-43)
        self.assertFalse(led.is_zero(-1e-9))
        self.assertFalse(led.is_empty)

    def test_a_it_pickles(self):
        """Ray returns the payload, which carries the ledger."""
        led = DarkRadiationLedger(2.0)
        led.add(1.0, 3.0)
        copy = pickle.loads(pickle.dumps(led))
        self.assertEqual(copy.K_initial, 2.0)
        self.assertEqual(copy.injections, led.injections)
        self.assertEqual(copy.ratio(1.5, -3.0), led.ratio(1.5, -3.0))


# ---------------------------------------------------------------------------------------------
# (b), (c) constructed states
# ---------------------------------------------------------------------------------------------

_BASE = ExponentialModel(0, M_value(0, 0.01 * _Mp), 1, beta_value(0, 3.0), _units)

# ln(rho_R/V) at the states: -8 and -2 (V dominates; the branch ln(rho_R/V) <= 2), 2.5 and 10 (the
# branch ln(rho_R/V) > 2), and 746 (V/3H^2 M_P^2 underflows to 0.0: HubblePolicy's V = 0 branch;
# exp(-745.13) is still a subnormal, ruling 2)
LOG_RHO_OVER_V = (-8.0, -2.0, 2.5, 10.0, 746.0)
R_CHI = (1e-6, 1e-2, 1.0, 10.0)
# (ln f_m, pi/M_P): a bath with f_m ~ 2e-9 and an inward velocity; f_m = 1.65 and an outward one
MATTER = ((-20.0, -0.4), (0.5, 0.05))
LOG_RHO = -166.0  # ln rho_R,E in Planck units, the integrator audit's P1 state
PHI = 0.2
LOG_T = -42.6


def constructed(L: float, r: float, log_fm: float, pi_over_Mp: float):
    """
    A state with ln(rho_R/V) = L at phi = 0.2 M_P (the model realized at the normalization that
    gives it), N = 0, and a ledger whose K_0 gives r_chi = r there. Returns
    (model, ledger, policy, state, data, H_E).
    """
    norm = LOG_RHO - L - 0.01 * _Mp / (PHI * _Mp)
    rm = _BASE.realize(norm, 0.0)
    ledger = DarkRadiationLedger(r * exp(LOG_RHO))
    policy = SM.ODEPolicy("test_dark_radiation", _cosmology, rm, dark_radiation=ledger)
    state = StateVector(PHI * _Mp, pi_over_Mp * _Mp, LOG_RHO, log_fm, LOG_T)
    data = policy(0.0, state)
    H = SM.HubblePolicy(rm, _units)(data, state).H_Einstein
    return rm, ledger, policy, state, data, H


def _R(Sigma: float, fm: float) -> float:
    """R = (Sigma + f_m)/(1 + f_m), in ODEPolicy's two forms."""
    return (
        (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        if fm > 10.0
        else (Sigma + fm) / (1.0 + fm)
    )


class TestTheConstraint(unittest.TestCase):
    """
    (b) At 5 x 4 x 2 constructed states: ODEPolicy and HubblePolicy satisfy
    3 M_P^2 H_E^2 G = V + rho_R (1 + f_m) + rho_chi, and the E that reaches the kick satisfies
    E 3H^2 M_P^2 = rho_R (1 + f_m). Milliseconds.
    """

    def test_b_the_states_take_the_branches_they_are_meant_to(self):
        for L in LOG_RHO_OVER_V:
            rm, ledger, policy, state, data, H = constructed(L, 1.0, *MATTER[0])
            self.assertEqual(data.rho_chi_over_rho_rad > 0.0, True)
            self.assertLessEqual(abs(data.rho_chi_over_rho_rad - 1.0), 64 * EPS)
            log_rho_over_V = state.log_rhorad_Einstein - rm.log_V(state.phi_Einstein)
            self.assertLessEqual(abs(log_rho_over_V - L), 1e-12)
            if L >= 746.0:
                # ruling 2: the V = 0 branch is taken
                self.assertEqual(data.V_over_3H2Mp2, 0.0)
            else:
                self.assertGreater(data.V_over_3H2Mp2, 0.0)

    def test_b_the_constraint_to_rounding(self):
        """
        |LHS - RHS|/RHS <= rounding_bound(ln V, ln rho_R): no term of the constraint cancels (all
        are positive), so the bound is that of the exps (about 400 EPS here; the largest measured
        was 68 EPS). Leaving r_chi out of the V = 0 branch puts an error r/(1 + f_m + r) >= 3.8e-7 there.
        """
        for L in LOG_RHO_OVER_V:
            for r in R_CHI:
                for log_fm, pi in MATTER:
                    rm, ledger, policy, state, data, H = constructed(L, r, log_fm, pi)
                    G = 1.0 - state.pi_Einstein**2 / (6.0 * _Mp**2)
                    log_V = rm.log_V(state.phi_Einstein)
                    rhs = (
                        exp(log_V)
                        + exp(LOG_RHO) * (1.0 + exp(log_fm))
                        + ledger.rho_chi(0.0)
                    )
                    lhs = 3.0 * _Mp**2 * H**2 * G
                    self.assertLessEqual(
                        abs(lhs - rhs) / rhs,
                        rounding_bound(log_V, LOG_RHO),
                        (L, r, log_fm),
                    )

    def test_b_E_is_the_matter_and_bath_only(self):
        """
        E is read back from the kick, E = -kicking/(3 M_P^2 (d ln A/dphi) R), and
        |E 3H^2 M_P^2 - rho_R (1 + f_m)|/(rho_R (1 + f_m)) <= rounding_bound(...) G/(G - U),
        U = V/3H^2 M_P^2: G - U cancels when V dominates, and multiplies the rounding of U by
        G/(G - U) (up to 3e3 at ln(rho_R/V) = -8; the largest measured error was 841 EPS against a
        bound of about 1e6 EPS there). Leaving rho_chi in E gives an error r/(1 + f_m) >= 3.8e-7.
        """
        for L in LOG_RHO_OVER_V:
            for r in R_CHI:
                for log_fm, pi in MATTER:
                    rm, ledger, policy, state, data, H = constructed(L, r, log_fm, pi)
                    fm = exp(log_fm)
                    G = 1.0 - state.pi_Einstein**2 / (6.0 * _Mp**2)
                    U = data.V_over_3H2Mp2
                    E = -data.kicking_term / (
                        3.0
                        * _Mp**2
                        * rm.d_logOmega_dphi(state.phi_Einstein)
                        * _R(data.Sigma, fm)
                    )
                    expected = exp(LOG_RHO) * (1.0 + fm)
                    error = abs(E * 3.0 * _Mp**2 * H**2 - expected) / expected
                    bound = rounding_bound(rm.log_V(state.phi_Einstein), LOG_RHO) * (
                        G / (G - U)
                    )
                    self.assertLessEqual(error, bound, (L, r, log_fm))
                    self.assertLess(bound, 1e-3 * r / (1.0 + fm + r))


class TestHdot(unittest.TestCase):
    """
    (c) Hdot/H^2 at the constructed states, from Hdot = -(rho + p)/2 M_P^2 with
    rho + p = H^2 pi^2 + rho_R (1 + f_m) + p_R + (4/3) rho_chi, p_R = rho_R (1 - Sigma)/3, and H^2
    from the constraint, all written out here, against Hdot_over_H2_plus_3 - 3. Milliseconds.
    """

    def _definitions(self, rm, state, Sigma, fm, r):
        V = exp(rm.log_V(state.phi_Einstein))
        rho = exp(state.log_rhorad_Einstein)
        rho_chi = r * rho
        pi = state.pi_Einstein
        G = 1.0 - pi * pi / (6.0 * _Mp**2)
        H2 = (V + rho * (1.0 + fm) + rho_chi) / (3.0 * _Mp**2 * G)
        rho_plus_p = (
            H2 * pi * pi
            + rho * (1.0 + fm)
            + rho * (1.0 - Sigma) / 3.0
            + 4.0 * rho_chi / 3.0
        )
        return -rho_plus_p / (2.0 * _Mp**2 * H2)

    def test_c_Hdot_from_the_definitions(self):
        """
        |policy - definitions| <= 3 rounding_bound(ln V, ln rho_R), an absolute bound: Hdot/H^2 lies
        in [-3, 0], and the policy returns Hdot/H^2 + 3, a sum G A1 + C A2 - Y R/2 in [1, 3] that
        subtracts Y R/2 <= 1/2 from a sum >= 1. Its rounding is relative to that sum, so once the 3
        is taken off it is absolute on the scale 3; Hdot/H^2 itself is a cancelled difference when V
        dominates (-7e-4 at ln(rho_R/V) = -8), and its relative error there is not rounding-sized.
        Sigma = 0.3 so that R > 0 at f_m ~ 2e-9 too; dropping -Y R/2 shifts the result by
        Y R/2 >= 4e-11 at these states, above the bound (about 2.6e-13).
        """
        Sigma = 0.3
        for L in LOG_RHO_OVER_V:
            for r in R_CHI:
                for log_fm, pi in MATTER:
                    rm, ledger, policy, state, data, H = constructed(L, r, log_fm, pi)
                    fm = exp(log_fm)
                    code = (
                        policy.V_policy.Hdot_over_H2_plus_3(
                            state.phi_Einstein,
                            state.pi_Einstein,
                            state.log_rhorad_Einstein,
                            Sigma,
                            fm,
                            r_chi=data.rho_chi_over_rho_rad,
                        )
                        - 3.0
                    )
                    expected = self._definitions(rm, state, Sigma, fm, r)
                    self.assertLessEqual(
                        abs(code - expected),
                        3.0 * rounding_bound(rm.log_V(state.phi_Einstein), LOG_RHO),
                        (L, r, log_fm),
                    )

    def test_c_with_R_zero_the_term_vanishes(self):
        """
        Sigma = 0 and f_m = 0, so R = 0: at V = 0 and pi = 0, Hdot/H^2 = -2 exactly for every r_chi
        (G A1 = 1, C = 0, Y R/2 = 0), and at the other states the definitions agree to the absolute
        bound of test_c_Hdot_from_the_definitions.
        """
        for L in LOG_RHO_OVER_V:
            for r in R_CHI:
                rm, ledger, policy, state, data, H = constructed(L, r, -20.0, 0.0)
                code = (
                    policy.V_policy.Hdot_over_H2_plus_3(
                        state.phi_Einstein,
                        state.pi_Einstein,
                        state.log_rhorad_Einstein,
                        0.0,
                        0.0,
                        r_chi=data.rho_chi_over_rho_rad,
                    )
                    - 3.0
                )
                if L >= 746.0:
                    self.assertEqual(code, -2.0, r)
                expected = self._definitions(rm, state, 0.0, 0.0, r)
                self.assertLessEqual(
                    abs(code - expected),
                    3.0 * rounding_bound(rm.log_V(state.phi_Einstein), LOG_RHO),
                    (L, r),
                )

    def test_c_with_matter_it_is_the_dust_and_radiation_law(self):
        """
        V = 0, pi = 0, Sigma = 1 (R = 1): Hdot/H^2 = -(3/2)(rho_m + (4/3) rho_chi)/(rho_m + rho_chi),
        with rho_R (1 + f_m) the dust. To 16 EPS: a handful of operations on numbers of order 1.
        """
        for r in R_CHI:
            rm, ledger, policy, state, data, H = constructed(746.0, r, 0.5, 0.0)
            fm = exp(0.5)
            code = (
                policy.V_policy.Hdot_over_H2_plus_3(
                    state.phi_Einstein,
                    state.pi_Einstein,
                    state.log_rhorad_Einstein,
                    1.0,
                    fm,
                    r_chi=data.rho_chi_over_rho_rad,
                )
                - 3.0
            )
            rho_m = 1.0 + fm  # in units of rho_R
            expected = -1.5 * (rho_m + 4.0 * r / 3.0) / (rho_m + r)
            self.assertLessEqual(abs(code / expected - 1.0), 16 * EPS, r)


# ---------------------------------------------------------------------------------------------
# (d) a real history
# ---------------------------------------------------------------------------------------------

# r_0 of (d): Y R/2 peaks at about 9.5e-3 at T_J = 0.19 GeV, on the QCD transition, where Sigma is
# largest (the prompt's log); well above the solver's rtol = 1e-8 and the H check's tolerance
D_RHO_CHI_RATIO = 0.1
D_BETA = 1.5

# the H check's tolerance: rtol = 1e-8, and the derivative of Radau's collocation polynomial between
# its nodes carries the local error over the step, ~1e-8/1e-2 = 1e-6 at the steps of the QCD
# transition; the centred difference with h <= 1e-4 adds h^2/6 |(ln H)'''| < 1e-8 and a rounding of
# 1e-14/h = 1e-10. The same check on the same history with r_0 = 0 has its worst residual 4.8e-7
# (the prompt's log), so this bound is the integrator's, not the dark radiation's
H_CHECK_TOLERANCE = 2.0e-6
H_CHECK_STEP = 1.0e-4


def _history_with_dark_radiation(r0: float, beta: float):
    """
    The golden histories' setup (golden_history.py: M = 1e-3 M_P, Lambda = 1 meV, phi* = 5 M_P,
    T* = 2e4 GeV, T_stop = 1 MeV, atol = rtol = 1e-8, the production axis) through integrate_history
    with rho_chi_ratio_init = r0. Returns (payload, IntegrationResult, realized model); the result
    is captured by wrapping integrate_scalar_history, as test_fixed_T_values does (ruling 4).
    """
    rm = gh.model(beta, _units).realize(gh.normalization(_units), 0.0)
    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        result = integrate(*args, **kwargs)
        captured.append(result)
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            _cosmology,
            temperature(0, gh.T_INIT_GEV * _units.GeV),
            temperature(1, gh.T_STOP_GEV * _units.GeV),
            phi_value(0, gh.PHI_INIT_OVER_MP * _Mp),
            pi_value(0, gh.PI_INIT),
            gh.make_z_grid(),
            rm,
            task_label="test_dark_radiation",
            atol=gh.ATOL,
            rtol=gh.RTOL,
            rho_chi_ratio_init=r0,
        )
    return data, captured[-1], rm


class TestARealHistory(unittest.TestCase):
    """
    (d) The golden beta = 1.5 history with r_0 = 0.1 (setUpClass: one integration, under 1 s
    unloaded, a few seconds at a load of 20). The H check reads the dense output through an
    ODEPolicy that holds the payload's own ledger.
    """

    @classmethod
    def setUpClass(cls):
        cls.data, cls.result, cls.rm = _history_with_dark_radiation(
            D_RHO_CHI_RATIO, D_BETA
        )
        if cls.data.get("failure", False):
            raise AssertionError(f"the history failed: {cls.data['failure_reason']}")
        cls.ledger = cls.data["dark_radiation"]
        cls.policy = SM.ODEPolicy(
            "test_dark_radiation", _cosmology, cls.rm, dark_radiation=cls.ledger
        )
        cls.hubble = SM.HubblePolicy(cls.rm, _units)

    def test_d_the_payload_carries_the_ledger_it_was_built_with(self):
        """K_0 = r_0 rho_R,E,init e^{4 N_start}, N_start = 0; r_chi at N = 0 is r_0 to 64 EPS."""
        self.assertIsInstance(self.ledger, DarkRadiationLedger)
        self.assertEqual(self.ledger.injections, ())
        first = self.result.solution(0.0)
        self.assertLessEqual(
            abs(self.ledger.ratio(0.0, first[2]) / D_RHO_CHI_RATIO - 1.0), 64 * EPS
        )
        self.assertNotIn("dark_radiation", SM.build_extra_data(self.data) or {})

    def test_d_the_constraint_on_the_samples(self):
        """
        At every sample, |3 M_P^2 H_E^2 G - rho_R (1 + f_m) - V - rho_chi,E(N)| relative to the total
        3 M_P^2 H_E^2 G is within rounding_bound(ln V, ln rho_R,E) (about 540 EPS at the late
        samples; the largest measured was 129 EPS): H_E was rebuilt from exps of those logs, and
        the subtraction of positive parts is measured on the scale of their sum.
        """
        for s in self.data["sample"]:
            G = 1.0 - s.pi_Einstein**2 / (6.0 * _Mp**2)
            total = 3.0 * _Mp**2 * s.H_Einstein**2 * G
            log_V = self.rm.log_V(s.phi_Einstein)
            rest = (
                total - exp(s.log_rhorad_Einstein) * (1.0 + exp(s.log_fm)) - exp(log_V)
            )
            error = abs(rest - self.ledger.rho_chi(s.raw_N)) / total
            self.assertLessEqual(
                error, rounding_bound(log_V, s.log_rhorad_Einstein), s.raw_N
            )
        self.assertGreater(len(self.data["sample"]), 1000)

    def _log_H(self, N):
        state = StateVector._make(self.result.solution(N))
        data = self.policy(N, state)
        return log(self.hubble(data, state).H_Einstein), state, data

    def test_d_d_ln_H_dN_on_the_dense_output(self):
        """
        At the midpoint of every accepted step longer than 1e-3 e-folds, the centred difference of
        ln H_E (step min(1e-4, step/8)) equals Hdot/H^2 = Hdot_over_H2_plus_3(..., r_chi) - 3 at the
        interpolated state, to H_CHECK_TOLERANCE = 2e-6. On this history Y R/2 reaches 9.5e-3, so
        dropping it, or the dark radiation from H, fails by more than 1e3 times the tolerance.
        """
        ts = self.result.solution.ts
        largest_YR = 0.0
        checked = 0
        for k in range(len(ts) - 1):
            a, b = ts[k], ts[k + 1]
            if b - a < 1e-3:
                continue
            N = 0.5 * (a + b)
            h = min(H_CHECK_STEP, (b - a) / 8.0)
            difference = (self._log_H(N + h)[0] - self._log_H(N - h)[0]) / (2.0 * h)
            _, state, data = self._log_H(N)
            Hdot_over_H2 = (
                self.policy.V_policy.Hdot_over_H2_plus_3(
                    state.phi_Einstein,
                    state.pi_Einstein,
                    state.log_rhorad_Einstein,
                    data.Sigma,
                    data.fm,
                    r_chi=data.rho_chi_over_rho_rad,
                )
                - 3.0
            )
            self.assertLessEqual(abs(difference - Hdot_over_H2), H_CHECK_TOLERANCE, N)
            G = 1.0 - state.pi_Einstein**2 / (6.0 * _Mp**2)
            _, Y = SM.PotentialDerivativePolicy.matter_and_dark_radiation_fractions(
                G, data.V_over_3H2Mp2, data.fm, data.rho_chi_over_rho_rad
            )
            largest_YR = max(largest_YR, Y * _R(data.Sigma, data.fm) / 2.0)
            checked += 1
        self.assertGreater(checked, 400)
        # the check is sensitive: the dark radiation's own term is far above its tolerance
        self.assertGreater(largest_YR, 1e3 * H_CHECK_TOLERANCE)


class TestTheEmptyLedger(unittest.TestCase):
    """The golden beta = 1.5 history with r_0 = 0: its payload's ledger is empty and pickles. 1 s."""

    def test_d_r0_zero_gives_an_empty_ledger_and_a_payload_that_pickles(self):
        data, result, rm = _history_with_dark_radiation(0.0, D_BETA)
        self.assertTrue(data["dark_radiation"].is_empty)
        copy = pickle.loads(pickle.dumps(data))
        self.assertTrue(copy["dark_radiation"].is_empty)
        self.assertEqual(copy["sample"], data["sample"])


# ---------------------------------------------------------------------------------------------
# (e) the tuning
# ---------------------------------------------------------------------------------------------

# r_0 of (e): r_chi today is 0.39 r_0 (rho_chi a^4 is constant while the bath is heated by
# g_s^{4/3}), so leaving rho_chi out of V_target misplaces H_J/H0 by about 2.6e-5 at r_0 = 1, 2.6
# times the tolerance 1e-5 (the prompt's log), and the tuning cannot converge
E_RHO_CHI_RATIO = 1.0


def _last_golden_sample():
    """The tuned golden history's last sample, from its JSON excerpt, and its realized model."""
    golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
    excerpt = golden["history"]["excerpts"][-1]
    fields = {k: float.fromhex(v) for k, v in excerpt["fields"].items()}
    s = SM.SampleValues(**fields)
    norm = float.fromhex(golden["tuning"]["normalization"])
    phi_ref = float.fromhex(golden["tuning"]["phi_ref"])
    model = gth.make_model(_units)
    return s, model, model.realize(norm, phi_ref)


class TestTheTuningState(unittest.TestCase):
    """
    (e) _tuning_state on the tuned golden history's last sample (from the JSON, no integration)
    with a ledger that gives r_chi = 0.39 there. Milliseconds.
    """

    def test_e_V_target_puts_H_J_at_H0(self):
        """
        With V_target in the Einstein-frame constraint, 3 M_P^2 H_E^2 G = V_target + rho_R,E (1 + f_m)
        + rho_chi,E and H_J = H_E (1 + (d ln A/dphi) pi)/A, |H_J/H0 - 1| <= 4 rounding_bound(ln rho_R,E):
        V_target subtracts the matter (Omega_m ~ 0.3 of the total) from the total, which cancels
        by at most a factor 1/0.7, and rho_R,E is exp(-285); the 4 covers the square root and the
        factor. Leaving r_chi out of V_target puts H_J/H0 - 1 at about r_chi Omega_R/2 ~ 2e-5.
        """
        s, model, rm = _last_golden_sample()
        r = 0.39
        ledger = DarkRadiationLedger(r * exp(s.log_rhorad_Einstein + 4.0 * s.raw_N))
        cosmology = gth.make_cosmology()
        state = SM._tuning_state(
            cosmology, model, rm, {"sample": [s], "dark_radiation": ledger}
        )

        phi, pi = s.phi_Einstein, s.pi_Einstein
        G = 1.0 - pi * pi / (6.0 * _Mp**2)
        rho_E = exp(s.log_rhorad_Einstein)
        H2_E = (
            state.V_target + rho_E * (1.0 + exp(s.log_fm)) + ledger.rho_chi(s.raw_N)
        ) / (3.0 * _Mp**2 * G)
        H_J = H2_E**0.5 * (1.0 + rm.d_logOmega_dphi(phi) * pi) / rm.Omega(phi)
        self.assertLessEqual(
            abs(H_J / cosmology.H0 - 1.0), 4.0 * rounding_bound(s.log_rhorad_Einstein)
        )
        self.assertLessEqual(
            abs(ledger.ratio(s.raw_N, s.log_rhorad_Einstein) / r - 1.0), 1e-12
        )


class TestATunedHistory(unittest.TestCase):
    """
    (e) tune_history at the tuned golden point (golden_tuned_history.py: beta = 3, M = 0.01 M_P)
    with rho_chi_ratio_init = 1.0: five integrations to T_CMB, about 20 s unloaded.
    """

    @classmethod
    def setUpClass(cls):
        with contextlib.redirect_stdout(io.StringIO()):
            cls.data = gth.run_tuning(rho_chi_ratio_init=E_RHO_CHI_RATIO)
        cls.cosmology = gth.make_cosmology()
        cls.model = gth.make_model(_units)

    def test_e_it_converges_with_H_J_at_H0_and_phi_ref_at_phi_min(self):
        d = self.data
        self.assertFalse(d.get("failure", False), d.get("failure_reason"))
        self.assertTrue(
            SM.tuning_converged(d["residual_H"], d["residual_phi"], gth.TOLERANCE)
        )

        # both conditions, recomputed from the last sample
        rm = self.model.realize(d["normalization"], d["phi_ref"])
        s = SM.SampleValues._make(tuple(d["sample"][-1]))
        A0 = rm.Omega(s.phi_Einstein)
        rho_mE0 = A0**4 * exp(s.log_fm) * exp(s.log_rhorad_Jordan)
        phi_min = self.model.effective_minimum(
            d["normalization"], d["phi_ref"], s.phi_Einstein, rho_mE0
        )
        self.assertLess(abs(s.H_Jordan / self.cosmology.H0 - 1.0), gth.TOLERANCE)
        self.assertLess(abs(d["phi_ref"] / phi_min - 1.0), gth.TOLERANCE)

        # the history carried its rho_chi to today: r_chi(T_CMB) is r_0 diluted by the heating
        # of the bath, 0.39 r_0 (the prompt's log)
        ledger = d["dark_radiation"]
        self.assertGreater(ledger.K_initial, 0.0)
        r_today = ledger.ratio(s.raw_N, s.log_rhorad_Einstein)
        self.assertGreater(r_today, 0.3 * E_RHO_CHI_RATIO)
        self.assertLess(r_today, 0.5 * E_RHO_CHI_RATIO)


if __name__ == "__main__":
    unittest.main()
