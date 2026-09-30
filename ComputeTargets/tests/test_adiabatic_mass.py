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
The adiabatic mass and Q's numerator in `ComputeTargets/AdiabaticHistory.py`.

Written for production-readiness prompt 03 (README §6.3; review H5). No Ray
cluster, no datastore, no PRyMordial solve. About 3 s.

**The reference** (test (a)). The conformal part of M^2_eff is the phi-derivative
of the source term in the force the ODE integrates,

    F(phi) = (ln Omega)'(phi) [Sigma(T_J) rho_R,E + rho_m,E],

at fixed Einstein-frame scale factor and fixed comoving entropy. The reference
takes that derivative by a central difference in phi, built from the defining
relations only:

- T_J(phi) solves T_J Omega(phi) g_s(T_J)^{1/3} = const, root-found with `G_s`
  alone (never `dG_s_dlogT` or `dw_dlogT`);
- rho_R,E(phi) = rho_R,E(phi_0) exp(integral of Sigma d ln Omega), the integral by
  Simpson's rule across the step (Sigma = 1 - 3 w(T_J), from `w`);
- rho_m,E proportional to Omega.

Nothing in it uses the closed form under test. It converges as h^2 (test (a)).

**What is compared** (tests (b)-(d)). `AdiabaticComputePolicy.M2eff_over_H2` itself,
minus the self mass 3 M_P^2 V''/(3 H^2 M_P^2) and the gravitational mass
1 - (Hdot/H^2 + 3), both taken from the same `PotentialDerivativePolicy`. The
potential is a stand-in with log V linear in phi and V far below rho_R, so that it
does not enter the comparison except through E, which is common to both sides.

The call path passes `T_Jordan` only if the method accepts it, so that on the tree
before prompt 03 (`HEAD~1`) test (b) runs and fails because the term is missing
(max |bracket| 0.41), not because the signature changed.

**Q's numerator** (test (e)) is reached two ways: through
`compute_adiabatic_values._function` with a stand-in model and a stand-in policy
(this is the path that fails on `HEAD~1`), and through the factored pure helper
`Q_numerator` for the signed value. Both are imported from the module; the helper
is imported inside the test that uses it so that the module still loads on
`HEAD~1`.

Run from the repository root. Set CHAMPBH_TEST_REPORT=1 to print the measured
values: the reference's convergence table, the probe's Table 1 temperatures
through the code and the reference, the bracket's extremes, and the audit form's
distance from the reference (test (f), reported, never asserted).
"""

import importlib
import inspect
import os
import sys
import unittest
from math import exp, log, pi
from types import SimpleNamespace
from unittest import mock

import numpy as np
from scipy.interpolate import make_interp_spline
from scipy.optimize import brentq

from ComputeTargets.AdiabaticHistory import (
    AdiabaticComputePolicy,
    compute_adiabatic_values,
)
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
    ExponentialCoupling,
)
from CosmologyConcepts import beta_value
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import GeV_units
from constants import RadiationConstant

# the module itself: ComputeTargets/__init__.py re-exports the class of the same name,
# which shadows the submodule as an attribute of the package
adiabatic_module = importlib.import_module("ComputeTargets.AdiabaticHistory")

_REPORT = os.environ.get("CHAMPBH_TEST_REPORT", "") not in ("", "0")

# README §6.3
BRACKET_TOLERANCE = 1e-6
MATTER_LIMIT_TOLERANCE = 1e-6
Q_NUMERATOR_TOLERANCE = 1e-4
EXACT_ZERO_TOLERANCE = 1e-4

# the grid of Jordan-frame temperatures, in GeV
GRID_POINTS = 1000
GRID_T_LO_GEV = 1.2e-5
GRID_T_HI_GEV = 2.0e4

# the step of the reference, as a change in ln Omega
REFERENCE_STEP = 1e-4
CONVERGENCE_STEPS = (1e-3, 1e-4, 1e-5)

FM_VALUES = (0.0, 1.0, 100.0)
FM_MATTER_LIMIT = 1e6

BETA = 2.0
PHI0_MP = 0.3
PI0_MP = 0.2

# stand-in coupling with (ln Omega)'' != 0: ln Omega = beta phi/M_P + gamma (phi/M_P)^2
STANDIN_BETA = 2.0
STANDIN_GAMMA = 1.5
STANDIN_PHI0_MP = 0.5

# the probe's Table 1 temperatures (planning-probes/h5_bracket_probe.py), in GeV
TABLE_1_T_GEV = (3e-4, 1.6e-4, 1e-4, 0.25, 0.18, 0.14, 0.1, 80.0, 53.0, 40.0)

# synthetic histories for Q's numerator (README §2 (e)): production sampling
Q_DN = log(10.0) / 250.0
Q_N_MAX = 12.0
# the accuracy window, as in planning-probes/q_sign_change_probe.py: 0.5 e-folds clear of
# each end of the history, where the spline's end condition sets the derivative
Q_WINDOW = (0.5, 11.5)
Q_KP_OVER_H = 10.0
SPIKE_CENTRES = (2.03, 4.51, 7.27, 9.88)


def _report(msg: str):
    if _REPORT:
        print(msg, file=sys.stderr)


def _T_grid_GeV() -> np.ndarray:
    return np.exp(np.linspace(log(GRID_T_LO_GEV), log(GRID_T_HI_GEV), GRID_POINTS))


class _StandInPotential:
    """log V = log V0 + lam phi/M_P, with V far below rho_R at every grid temperature."""

    name = "stand-in potential for test_adiabatic_mass"

    def __init__(self, MP: float, log_V0: float, lam: float):
        self._MP = MP
        self._log_V0 = log_V0
        self._lam = lam

    def log_V(self, phi):
        return self._log_V0 + self._lam * phi / self._MP

    def d_logV_dphi(self, phi):
        return self._lam / self._MP

    def d2_logV_dphi2(self, phi):
        return 0.0


class _QuadraticCoupling:
    """ln Omega = beta phi/M_P + gamma (phi/M_P)^2, a stand-in with (ln Omega)'' != 0."""

    name = "stand-in quadratic coupling"

    def __init__(self, MP: float, beta: float, gamma: float):
        self._MP = MP
        self._beta = beta
        self._gamma = gamma

    def log_Omega(self, phi):
        y = phi / self._MP
        return self._beta * y + self._gamma * y * y

    def Omega(self, phi):
        return exp(self.log_Omega(phi))

    def d_logOmega_dphi(self, phi):
        return (self._beta + 2.0 * self._gamma * phi / self._MP) / self._MP

    def d2_logOmega_dphi2(self, phi):
        return 2.0 * self._gamma / (self._MP * self._MP)


class _Reference:
    """
    The reference of test (a): the central difference in phi of the source term
    F = (ln Omega)' (Sigma rho_R,E + rho_m,E), from the defining relations only.
    Temperatures are plain floats in GeV.
    """

    def __init__(self, cosmology):
        self.cosmology = cosmology
        self.GeV = cosmology.units.GeV

    def Sigma(self, T_GeV: float) -> float:
        return 1.0 - 3.0 * float(self.cosmology.w(T_GeV * self.GeV))

    def log_G_s(self, T_GeV: float) -> float:
        return log(float(self.cosmology.G_s(T_GeV * self.GeV)))

    def T_of(self, T0_GeV: float, u: float) -> float:
        """
        T_J after ln Omega changes by u at fixed a_E: the root delta = ln(T/T0) of
            delta + u + (1/3) [ln g_s(T0 e^delta) - ln g_s(T0)] = 0,
        which is T Omega g_s^{1/3} = const. G_s only.
        """
        if u == 0.0:
            return T0_GeV
        lg0 = self.log_G_s(T0_GeV)

        def f(delta):
            return delta + u + (self.log_G_s(T0_GeV * exp(delta)) - lg0) / 3.0

        width = 10.0 * abs(u)
        delta = brentq(f, -width, width, xtol=1e-300, rtol=8.9e-16, maxiter=200)
        return T0_GeV * exp(delta)

    def source_over_rho_R0(self, T0_GeV: float, u: float, fm: float) -> float:
        """
        (Sigma rho_R,E + rho_m,E) / rho_R,E(0) after ln Omega changes by u. rho_R,E from
        d ln rho_R,E = Sigma d ln Omega by Simpson's rule; rho_m,E proportional to Omega.
        """
        S0 = self.Sigma(T0_GeV)
        S_half = self.Sigma(self.T_of(T0_GeV, 0.5 * u))
        S1 = self.Sigma(self.T_of(T0_GeV, u))
        log_rho_R_ratio = (u / 6.0) * (S0 + 4.0 * S_half + S1)
        return S1 * exp(log_rho_R_ratio) + fm * exp(u)

    def dF_dphi_over_rho_R0(
        self, coupling, phi0: float, T0_GeV: float, fm: float, h: float
    ) -> float:
        """
        Central difference of F/rho_R,E(0) in phi, with a step in phi giving
        |Delta ln Omega| ~ h.
        """
        dphi = h / abs(coupling.d_logOmega_dphi(phi0))
        lO0 = coupling.log_Omega(phi0)

        def F(phi):
            u = coupling.log_Omega(phi) - lO0
            return coupling.d_logOmega_dphi(phi) * self.source_over_rho_R0(
                T0_GeV, u, fm
            )

        return (F(phi0 + dphi) - F(phi0 - dphi)) / (2.0 * dphi)


class _State(SimpleNamespace):
    pass


def _M2eff(policy, phi, pi_E, log_rho, Sigma, fm, T_Jordan):
    """
    Call M2eff_over_H2 with T_Jordan if it takes it. On the tree before prompt 03 it
    does not, and the old formula is scored (module docstring).
    """
    if "T_Jordan" in inspect.signature(policy.M2eff_over_H2).parameters:
        return policy.M2eff_over_H2(phi, pi_E, log_rho, Sigma, fm, T_Jordan)
    return policy.M2eff_over_H2(phi, pi_E, log_rho, Sigma, fm)


class TestAdiabaticMass(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.units = GeV_units()
        cls.GeV = cls.units.GeV
        cls.MP = cls.units.PlanckMass
        cls.cosmology = QCD_Cosmology(0, cls.units, Planck2018())
        cls.reference = _Reference(cls.cosmology)

        # V0 = 1e-30 GeV^4: V/rho_R < 1e-9 at 12 keV, where rho_R is smallest
        cls.potential = _StandInPotential(cls.MP, log(1e-30 * cls.GeV**4), lam=1.0)
        cls.exponential = ExponentialCoupling(0, beta_value(0, BETA), cls.units)
        cls.quadratic = _QuadraticCoupling(cls.MP, STANDIN_BETA, STANDIN_GAMMA)

        cls.Ts = _T_grid_GeV()

    # ---- helpers ------------------------------------------------------------------

    def _state(self, T_GeV: float, phi: float, fm: float) -> _State:
        rho_R = (
            RadiationConstant
            * float(self.cosmology.G_rho(T_GeV * self.GeV))
            * (T_GeV * self.GeV) ** 4
        )
        return _State(
            phi=phi,
            pi=PI0_MP * self.MP,
            log_rho=log(rho_R),
            rho_R=rho_R,
            fm=fm,
            T=T_GeV * self.GeV,
            Sigma=1.0 - 3.0 * float(self.cosmology.w(T_GeV * self.GeV)),
        )

    def _code_conformal(self, policy, s: _State) -> float:
        """M2eff_over_H2 minus the self and gravitational masses."""
        total = _M2eff(policy, s.phi, s.pi, s.log_rho, s.Sigma, s.fm, s.T)
        self_mass = (
            3.0
            * self.MP**2
            * policy.V_policy.Vprimeprime_over_3H2Mp2(s.phi, s.pi, s.log_rho, s.fm)
        )
        gravitational_mass = 1.0 - policy.V_policy.Hdot_over_H2_plus_3(
            s.phi, s.pi, s.log_rho, s.Sigma, s.fm
        )
        return total - self_mass - gravitational_mass

    def _rho_R_over_H2(self, s: _State) -> float:
        """rho_R,E/H^2 from 3 M_P^2 H^2 = (V + rho_R (1 + f_m))/G, independently of E."""
        V = exp(self.potential.log_V(s.phi))
        G = 1.0 - s.pi * s.pi / (6.0 * self.MP**2)
        H2 = (V + s.rho_R * (1.0 + s.fm)) / G / (3.0 * self.MP**2)
        return s.rho_R / H2

    def _policy(self, coupling) -> AdiabaticComputePolicy:
        return AdiabaticComputePolicy(
            "test_adiabatic_mass", self.cosmology, self.potential, coupling
        )

    def _bracket_errors(self, coupling, phi0: float, fm: float, h: float):
        """
        Per grid point: (code - reference) in M^2/H^2, divided by the norm
        3 M_P^2 E [(ln Omega)'^2 + |(ln Omega)''|], where 3 M_P^2 E = rho_R (1+f_m)/H^2.
        For the exponential coupling this is README §6.3's bracket norm.
        Also returns the reference bracket per point, in the same norm.
        """
        policy = self._policy(coupling)
        d1 = coupling.d_logOmega_dphi(phi0)
        d2 = coupling.d2_logOmega_dphi2(phi0)
        errors, ref_brackets = [], []
        for T in self.Ts:
            s = self._state(T, phi0, fm)
            rho_R_over_H2 = self._rho_R_over_H2(s)
            three_MP2_E = rho_R_over_H2 * (1.0 + fm)
            norm = three_MP2_E * (d1 * d1 + abs(d2))

            ref = rho_R_over_H2 * self.reference.dF_dphi_over_rho_R0(
                coupling, phi0, T, fm, h
            )
            code = self._code_conformal(policy, s)
            errors.append((code - ref) / norm)
            ref_brackets.append(ref / norm)
        return np.asarray(errors), np.asarray(ref_brackets)

    # ---- (a) the reference converges ----------------------------------------------

    def test_a_reference_converges_as_h_squared(self):
        """
        The reference's self-convergence, independent of the code: successive
        differences between h = 1e-3, 1e-4 and 1e-5 (in ln Omega) shrink by 100,
        the h^2 of a central difference. Exponential coupling, f_m = 0, on the grid.
        """
        phi0 = PHI0_MP * self.MP
        B = {}
        for h in CONVERGENCE_STEPS:
            B[h] = np.asarray(
                [
                    self.reference.dF_dphi_over_rho_R0(
                        self.exponential, phi0, T, 0.0, h
                    )
                    * self.MP**2
                    / BETA**2
                    for T in self.Ts
                ]
            )
        d1 = np.abs(B[1e-3] - B[1e-4]).max()
        d2 = np.abs(B[1e-4] - B[1e-5]).max()
        ratio = d1 / d2
        _report(
            f"[test_adiabatic_mass (a)] max |B(1e-3) - B(1e-4)| = {d1:.3e}, max |B(1e-4) - B(1e-5)| = {d2:.3e}, ratio {ratio:.2f} (h^2: 100)"
        )

        # the same table against the closed form, as the probe's Table 2
        if (
            _REPORT
            and "T_Jordan"
            in inspect.signature(AdiabaticComputePolicy.M2eff_over_H2).parameters
        ):
            policy = self._policy(self.exponential)
            code = []
            for T in self.Ts:
                s = self._state(T, phi0, 0.0)
                code.append(
                    self._code_conformal(policy, s)
                    / (self._rho_R_over_H2(s) * BETA**2 / self.MP**2)
                )
            code = np.asarray(code)
            for h in CONVERGENCE_STEPS:
                d = np.abs(B[h] - code)
                i = int(np.argmax(d))
                _report(
                    f"[test_adiabatic_mass (a)] h = {h:g}: max |B_reference - B_code| = {d[i]:.3e} at T = {self.Ts[i]:.4g} GeV"
                )

        self.assertGreater(ratio, 50.0)
        self.assertLess(ratio, 200.0)

    # ---- (b) exponential coupling -------------------------------------------------

    def test_b_exponential_coupling_against_reference(self):
        """
        M2eff_over_H2's conformal part against the reference, exponential coupling
        beta = 2, f_m in {0, 1, 100}, 1000 points over [12 keV, 20 TeV], h = 1e-4 in
        ln Omega. Norm: |Delta(M^2/H^2)| / (3 (ln Omega)'^2 M_P^2 E) <= 1e-6.
        Fails on HEAD~1, where the conformal part is 0 against a bracket of 0.41.
        """
        phi0 = PHI0_MP * self.MP
        for fm in FM_VALUES:
            with self.subTest(fm=fm):
                errors, ref = self._bracket_errors(
                    self.exponential, phi0, fm, REFERENCE_STEP
                )
                i = int(np.argmax(np.abs(errors)))
                _report(
                    f"[test_adiabatic_mass (b)] f_m = {fm:g}: max |error| in the bracket norm {abs(errors[i]):.3e} at T = {self.Ts[i]:.4g} GeV"
                )
                if fm == 0.0:
                    j, k = int(np.argmin(ref)), int(np.argmax(ref))
                    _report(
                        f"[test_adiabatic_mass (b)] reference bracket, f_m = 0: min {ref[j]:.4f} at {self.Ts[j]:.4g} GeV, max {ref[k]:.4f} at {self.Ts[k]:.4g} GeV"
                    )
                self.assertLessEqual(
                    abs(errors[i]),
                    BRACKET_TOLERANCE,
                    msg=f"f_m = {fm}: {errors[i]:.3e} at T = {self.Ts[i]:.5g} GeV",
                )

    # ---- (c) a coupling with (ln Omega)'' != 0 ------------------------------------

    def test_c_quadratic_coupling_against_reference(self):
        """
        The stand-in ln Omega = 2 phi/M_P + 1.5 (phi/M_P)^2 at phi = 0.5 M_P, where
        (ln Omega)' = 3.5/M_P and (ln Omega)'' = 3/M_P^2. Same grid, f_m and step.
        Norm: |Delta(M^2/H^2)| / (3 M_P^2 E [(ln Omega)'^2 + |(ln Omega)''|]) <= 1e-6.
        """
        phi0 = STANDIN_PHI0_MP * self.MP
        for fm in FM_VALUES:
            with self.subTest(fm=fm):
                errors, _ = self._bracket_errors(
                    self.quadratic, phi0, fm, REFERENCE_STEP
                )
                i = int(np.argmax(np.abs(errors)))
                _report(
                    f"[test_adiabatic_mass (c)] f_m = {fm:g}: max |error| in the norm {abs(errors[i]):.3e} at T = {self.Ts[i]:.4g} GeV"
                )
                self.assertLessEqual(
                    abs(errors[i]),
                    BRACKET_TOLERANCE,
                    msg=f"f_m = {fm}: {errors[i]:.3e} at T = {self.Ts[i]:.5g} GeV",
                )

    # ---- (d) the matter limit -----------------------------------------------------

    def test_d_matter_limit(self):
        """
        Exponential coupling, f_m = 1e6: the conformal part equals
        beta^2 rho_m,E / (M_P^2 H^2) to 1e-6 relative, with H^2 from the Friedmann
        equation, not from E. (Exactly, the ratio is 1 + B/f_m.)
        """
        policy = self._policy(self.exponential)
        phi0 = PHI0_MP * self.MP
        worst, worst_T = 0.0, None
        for T in self.Ts:
            s = self._state(T, phi0, FM_MATTER_LIMIT)
            expected = BETA**2 * FM_MATTER_LIMIT * self._rho_R_over_H2(s) / self.MP**2
            rel = abs(self._code_conformal(policy, s) / expected - 1.0)
            if rel > worst:
                worst, worst_T = rel, T
        _report(
            f"[test_adiabatic_mass (d)] f_m = 1e6: max relative difference from beta^2 rho_m/(M_P^2 H^2) = {worst:.3e} at T = {worst_T:.4g} GeV"
        )
        self.assertLessEqual(worst, MATTER_LIMIT_TOLERANCE)

    # ---- (f) the audit's form, measured -------------------------------------------

    def test_f_audit_form_is_measured(self):
        """
        Audit §5's bracket, Sigma (4 - d ln(Sigma rho_J)/d ln T_J) with
        rho_J = (pi^2/30) g_rho T^4, i.e. -Sigma_T - Sigma d ln g_rho/d ln T, against
        the reference. Reported with CHAMPBH_TEST_REPORT=1; not asserted either way.
        Also prints the probe's Table 1 temperatures through the code and the
        reference.
        """
        phi0 = PHI0_MP * self.MP
        policy = self._policy(self.exponential)

        def brackets(T):
            GeVT = T * self.GeV
            Sigma = 1.0 - 3.0 * float(self.cosmology.w(GeVT))
            Sigma_T = -3.0 * float(self.cosmology.dw_dlogT(GeVT))
            x = (
                float(self.cosmology.dG_s_dlogT(GeVT))
                / float(self.cosmology.G_s(GeVT))
                / 3.0
            )
            dlng_rho = float(self.cosmology.dG_rho_dlogT(GeVT)) / float(
                self.cosmology.G_rho(GeVT)
            )
            B_audit = -Sigma_T - Sigma * dlng_rho
            B_ref = (
                self.reference.dF_dphi_over_rho_R0(
                    self.exponential, phi0, T, 0.0, REFERENCE_STEP
                )
                * self.MP**2
                / BETA**2
            )
            s = self._state(T, phi0, 0.0)
            B_code = self._code_conformal(policy, s) / (
                self._rho_R_over_H2(s) * BETA**2 / self.MP**2
            )
            return Sigma, Sigma_T, x, B_audit, B_code, B_ref

        rows = [brackets(T) for T in self.Ts]
        diff = np.asarray([abs(r[3] - r[5]) for r in rows])
        i = int(np.argmax(diff))
        _report(
            f"[test_adiabatic_mass (f)] audit form: max |B_audit - B_reference| = {diff[i]:.4f} at T = {self.Ts[i]:.4g} GeV "
            f"(B_audit {rows[i][3]:.4f}, B_reference {rows[i][5]:.4f})"
        )
        _report(
            f"[test_adiabatic_mass (f)] {'T/GeV':>9} {'Sigma':>8} {'Sigma_T':>8} {'x':>8} {'B_audit':>9} {'B_code':>9} {'B_ref':>9}"
        )
        for T in TABLE_1_T_GEV:
            S, ST, x, Ba, Bc, Br = brackets(T)
            _report(
                f"[test_adiabatic_mass (f)] {T:9.4g} {S:8.4f} {ST:8.4f} {x:8.4f} {Ba:9.4f} {Bc:9.4f} {Br:9.4f}"
            )
        self.assertTrue(np.all(np.isfinite(diff)))


# ---- (e) Q's numerator ----------------------------------------------------------------


def _crossing(N: np.ndarray):
    """m = 5 sin(2 pi N/3) + 0.5, which changes sign every 1.5 e-folds."""
    w = 2.0 * pi / 3.0
    return 5.0 * np.sin(w * N) + 0.5, 5.0 * w * np.cos(w * N)


def _spikes(N: np.ndarray):
    """m = 0.5 + four 1e4 spikes of width 0.05: a bounce's dynamic range, no sign change."""
    m, dm = 0.5 + 0.0 * N, 0.0 * N
    for Nk in SPIKE_CENTRES:
        g = 1e4 * np.exp(-(((N - Nk) / 0.05) ** 2))
        m, dm = m + g, dm - 2.0 * (N - Nk) / 0.05**2 * g
    return m, dm


def _N_grid() -> np.ndarray:
    return np.arange(0.0, Q_N_MAX + 1e-12, Q_DN)


class _StandInPolicy:
    """
    Replaces AdiabaticComputePolicy inside compute_adiabatic_values: M2eff_over_H2
    returns the sample's phi_Einstein, which the stand-in history sets to m, and
    Hdot/H^2 = -2 (radiation, H^2 proportional to e^{-4N}).
    """

    def __init__(self, task_label, cosmology, potential, coupling):
        pass

    def M2eff_over_H2(self, phi_Einstein, *args):
        return phi_Einstein

    def Hdot_over_H2(self, *args):
        return -2.0


class _StandInProxy:
    def __init__(self, model):
        self._model = model

    def get(self):
        return self._model


def _run_compute_adiabatic_values(N: np.ndarray, m: np.ndarray):
    values = [
        SimpleNamespace(
            raw_N=float(Ni),
            z=None,
            phi_Einstein=float(mi),
            pi_Einstein=0.0,
            Sigma=0.0,
            log_fm=0.0,
            log_rhorad_Einstein=0.0,
            log_T_Jordan=0.0,
            H_Einstein=exp(-2.0 * float(Ni)),
        )
        for Ni, mi in zip(N, m)
    ]
    model = SimpleNamespace(
        _cosmology=SimpleNamespace(units=GeV_units()),
        potential=None,
        coupling=None,
        values=values,
    )
    labels = {"kp_over_H_1E1": Q_KP_OVER_H}
    with mock.patch.object(adiabatic_module, "AdiabaticComputePolicy", _StandInPolicy):
        data = compute_adiabatic_values._function(
            _StandInProxy(model), labels, "test_adiabatic_mass (e)"
        )
    return np.asarray(data["abs_Q_samples"]["kp_over_H_1E1"])


def _old_log_route(N: np.ndarray, m: np.ndarray) -> np.ndarray:
    """A copy of the route before prompt 03: m (1 + (1/2) d ln|H^2 m|/dN), log|M^2| splined."""
    d_log = make_interp_spline(N, np.log(np.abs(np.exp(-4.0 * N) * m))).derivative()
    return m * (1.0 + 0.5 * d_log(N))


class TestQNumerator(unittest.TestCase):
    """
    Test (e): the two synthetic histories of README §2 (e) at Delta N = ln 10/250,
    H^2 proportional to e^{-4N}, so Hdot/H^2 = -2 and exactly A*C = -m + (1/2) dm/dN.
    Errors are measured at the samples, where Q is evaluated, relative to
    max |A*C| over the samples. They are asserted at the samples with N in
    [0.5, 11.5], the window of the planning probe the targets come from. The error
    at every sample, including the two ends where the cubic spline's not-a-knot end
    condition sets the derivative, is reported (log 03: 2.3e-4 at the last sample of
    the crossing history).
    """

    def _score_through_compute(self, name, fn):
        N = _N_grid()
        m, dm = fn(N)
        exact = -m + 0.5 * dm
        scale = np.abs(exact).max()
        abs_Q = _run_compute_adiabatic_values(N, m)
        abs_AC = abs_Q * np.abs(m + Q_KP_OVER_H**2) ** 1.5
        e = np.abs(abs_AC - np.abs(exact)) / scale
        inside = (N >= Q_WINDOW[0]) & (N <= Q_WINDOW[1])
        err = e[inside].max()
        _report(
            f"[test_adiabatic_mass (e)] {name}: through compute_adiabatic_values, max ||A*C| - |exact|| / max|A*C| = {err:.3e} in [0.5, 11.5], "
            f"{e.max():.3e} at all samples (max |A*C| {scale:.4g})"
        )
        return err

    def test_e_crossing_history_through_compute(self):
        """The sign-changing history through compute_adiabatic_values. Fails on HEAD~1."""
        err = self._score_through_compute("crossing", _crossing)
        self.assertLessEqual(err, Q_NUMERATOR_TOLERANCE)

    def test_e_spike_history_through_compute(self):
        """The bounce-like history through compute_adiabatic_values."""
        err = self._score_through_compute("spikes", _spikes)
        self.assertLessEqual(err, Q_NUMERATOR_TOLERANCE)

    def test_e_exact_zero(self):
        """
        m = 5 sin(2 pi (N - N_z)/3) with N_z a sample, so m = 0 exactly there. Nothing
        raises, every |Q| is finite, and |Q| at N_z equals (1/2)(dm/dN)/(k_p/H)^3 to
        1e-4 relative. Fails on HEAD~1, where log(0) raises.
        """
        N = _N_grid()
        k = len(N) // 2
        N_z = N[k]
        w = 2.0 * pi / 3.0
        m = 5.0 * np.sin(w * (N - N_z))
        self.assertEqual(m[k], 0.0)

        abs_Q = _run_compute_adiabatic_values(N, m)
        self.assertTrue(np.all(np.isfinite(abs_Q)))

        expected = 0.5 * 5.0 * w / Q_KP_OVER_H**3
        rel = abs(abs_Q[k] / expected - 1.0)
        _report(
            f"[test_adiabatic_mass (e)] exact zero at N = {N_z:.6g}: |Q| = {abs_Q[k]:.10g}, (1/2)(dm/dN)/(k_p/H)^3 = {expected:.10g}, relative {rel:.3e}"
        )
        self.assertLessEqual(rel, EXACT_ZERO_TOLERANCE)

    def test_e_signed_numerator_and_the_old_route(self):
        """
        Through the pure helper Q_numerator, the signed A*C on both histories, <= 1e-4
        of max |A*C|. On the spike history (no sign change) it must also agree with a
        copy of the old log|M^2| route to 1e-4 of max |A*C| (prompt 03, §6's stop
        condition). The old route's error on the crossing history is reported.
        """
        from ComputeTargets.AdiabaticHistory import Q_numerator

        N = _N_grid()
        for name, fn in (("crossing", _crossing), ("spikes", _spikes)):
            with self.subTest(history=name):
                m, dm = fn(N)
                exact = -m + 0.5 * dm
                scale = np.abs(exact).max()
                smooth = np.asarray(Q_numerator(N, m, -2.0 + 0.0 * N))
                old = _old_log_route(N, m)
                inside = (N >= Q_WINDOW[0]) & (N <= Q_WINDOW[1])
                e = np.abs(smooth - exact) / scale
                err = e[inside].max()
                err_old = (np.abs(old - exact) / scale)[inside].max()
                smooth_vs_old = (np.abs(smooth - old) / scale)[inside].max()
                _report(
                    f"[test_adiabatic_mass (e)] {name}: in [0.5, 11.5], Q_numerator error {err:.3e}, old log route error {err_old:.3e}, "
                    f"Q_numerator vs old route {smooth_vs_old:.3e}; Q_numerator error at all samples {e.max():.3e} "
                    f"(all / max|A*C| = {scale:.4g})"
                )
                self.assertLessEqual(err, Q_NUMERATOR_TOLERANCE)
                if name == "spikes":
                    self.assertLessEqual(smooth_vs_old, Q_NUMERATOR_TOLERANCE)


if __name__ == "__main__":
    unittest.main()
