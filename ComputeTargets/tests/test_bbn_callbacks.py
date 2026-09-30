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
The PRyMordial new-physics callbacks built by `build_NP_callbacks`, the
Hdot_J/H_J^2 expression, and the Standard-Model baseline.

Written for review-remediation prompt 04 (item R3). Before that prompt the
callbacks splined asinh(rho_NP / MeV^4) and asinh(p_NP / MeV^4) against ln T;
they now spline the ratios r = rho_NP / rho_R,J and s = p_NP / rho_R,J and
multiply back by rho_SM(T). The synthetic geometry is that of
`.documents/audit-2026-09-29/spline_test.py`: knots at 250 per decade of T over
[1e-7, 1e2] MeV (the pipeline's spline domain), errors measured on 3,000 points
in [0.02, 5] MeV.

rho_SM is the Saikawa-Shirai g_rho through `SaikawaShirai_EOS_spline` (the
class whose G_rho and dG_rho_dlogT the production EOS inherits), passed through
`thermodynamic_rho_SM`, so the derivative the callbacks receive is the exact
derivative of the rho_SM they multiply by. No cosmology object is needed.

Errors are reported as in spline_test.py:
- values: |X_callback - X_true| / rho_SM(T), the spurious fractional change of H^2;
- the derivative: |drho_callback - drho_true| T / (4 rho_SM(T)), that is the
  error in d rho_NP / d ln T relative to d rho_SM / d ln T ~ 4 rho_SM.

**Tests (h) and (i) run PRyMordial, one solve each, about 10 s each.** Nothing
here needs a Ray cluster or a datastore. Run from the repository root, since
PRyMordial reads `PRyMrates/` from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import os
import unittest
from math import log, log10, pi

import numpy as np
from scipy.interpolate import make_interp_spline

from ComputeTargets.BBNData import (
    PRYM_VERSION,
    build_NP_callbacks,
    compute_SM_baseline,
    jordan_Hdot_over_H2,
    thermodynamic_rho_SM,
)
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.prym_fixtures import (
    RES_D_OVER_H_E5,
    RES_YP_BBN,
    run_prym,
)
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from Units import GeV_units, Planck_units

REPORT = bool(os.environ.get("CHAMPBH_TEST_REPORT"))

# the pipeline's spline domain, compute_BBN_data's defaults: 1e-4 keV to 100 MeV
T_MIN_MEV = 1e-7
T_MAX_MEV = 100.0
KNOTS_PER_DECADE = 250

# the window PRyMordial cares about, and the evaluation density
T_EVAL_LO_MEV = 0.02
T_EVAL_HI_MEV = 5.0
N_EVAL = 3000

# (a) constant ratio
CONSTANT_RATIO = 0.08
CONSTANT_VALUE_TOLERANCE = 1e-12
CONSTANT_DERIVATIVE_TOLERANCE = 1e-9

# (b) oscillating ratio: README section 6.3 (spline_test.py measured 9.7e-9 and 7.4e-7)
OSCILLATING_VALUE_BOUND = 2e-8
OSCILLATING_DERIVATIVE_BOUND = 1.5e-6

# (f) rho_SM at 1 MeV is about 3.5 MeV^4 (g_rho about 10.5)
RHO_SM_1MEV_LO = 3.4
RHO_SM_1MEV_HI = 3.6
UNITS_AGREEMENT_RTOL = 1e-10

# (h) prompt 03's constant family on the patched tree, full ten figures
# (log 03, "State handed to the next prompt"; `python -m
# ComputeTargets.tests.prym_fixtures constant`). README section 6.3 asks for
# 1e-4 relative. PRyMordial's D/H moves by up to 7e-4 when rho_NP moves by 1e-8
# (log 04, Verification), so this comparison sits near PRyMordial's own noise.
PROMPT_03_CONSTANT_YP = 0.2540937879
PROMPT_03_CONSTANT_D_OVER_H_E5 = 2.671499971
END_TO_END_RTOL = 1e-4

# (i) README section 2 (f) row 1, the SM baseline, to its quoted figures
README_BASELINE = {
    "Yp_BBN": 0.24689,
    "DOverH": 2.4623,
    "He3OverH": 1.042,
    "Li7OverH": 5.423,
}
BASELINE_RTOL = 1e-4


def ratio_constant(T_MeV):
    return CONSTANT_RATIO + 0.0 * np.log(T_MeV)


def dratio_constant_dlogT(T_MeV):
    return 0.0 * np.log(T_MeV)


def ratio_oscillating(T_MeV):
    """README section 2 (d): 0.08 + 0.3 sin(2 pi x) exp(-(x/1.5)^2), x = ln(T/0.3 MeV)."""
    x = np.log(T_MeV / 0.3)
    return 0.08 + 0.3 * np.sin(2.0 * pi * x) * np.exp(-((x / 1.5) ** 2))


def dratio_oscillating_dlogT(T_MeV):
    """The analytic d r / d ln T of `ratio_oscillating`."""
    x = np.log(T_MeV / 0.3)
    return (
        0.3
        * np.exp(-((x / 1.5) ** 2))
        * (2.0 * pi * np.cos(2.0 * pi * x) - (2.0 * x / 2.25) * np.sin(2.0 * pi * x))
    )


FAMILIES = {
    "constant": (ratio_constant, dratio_constant_dlogT),
    "oscillating": (ratio_oscillating, dratio_oscillating_dlogT),
}


def _old_HJdot_over_HJ2(HEdot_over_HE2, Omega_prime, Omega_primeprime, pi_, pi_prime):
    """The expression at BBNData.py before prompt 04, with Omega'' pi."""
    A1 = 1.0 + Omega_prime * pi_
    return (HEdot_over_HE2 - Omega_prime * pi_) / A1 + (
        Omega_primeprime * pi_ + Omega_prime * pi_prime
    ) / (A1 * A1)


class QuadraticStandIn:
    """
    A non-exponential stand-in coupling, ln Omega = phi^2 / (2 mu^2), for which
    d^2 ln Omega / d phi^2 = 1/mu^2 is not zero. It has only the two methods
    the Hdot_J/H_J^2 expression reads.
    """

    def __init__(self, mu: float):
        self._mu2 = mu * mu

    def d_logOmega_dphi(self, phi: float) -> float:
        return phi / self._mu2

    def d2_logOmega_dphi2(self, phi: float) -> float:
        return 1.0 / self._mu2


class _SavedPRyMGlobals:
    """
    compute_SM_baseline sets PRyMordial's module flags and NP callbacks, as
    compute_BBN_data does, and leaves them set. Restore them so that the tests
    in this package do not depend on order.
    """

    _init_names = ("NP_thermo_flag", "Tstart_NP", "verbose_flag", "smallnet_flag")
    _thermo_names = ("rho_NP", "p_NP", "drho_NP_dT", "delta_rho_NP")
    _missing = object()

    def __enter__(self):
        import PRyM.PRyM_init as PRyMini
        import PRyM.PRyM_thermo as PRyMthermo

        self._ini, self._thermo = PRyMini, PRyMthermo
        self._saved_init = {
            n: getattr(PRyMini, n, self._missing) for n in self._init_names
        }
        self._saved_thermo = {n: getattr(PRyMthermo, n) for n in self._thermo_names}
        return self

    def __exit__(self, *exc):
        for n, v in self._saved_init.items():
            if v is self._missing:
                if hasattr(self._ini, n):
                    delattr(self._ini, n)
            else:
                setattr(self._ini, n, v)
        for n, v in self._saved_thermo.items():
            setattr(self._thermo, n, v)
        return False


class TestBBNCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.units = GeV_units()
        cls.eos = SaikawaShirai_EOS_spline(cls.units)
        rho_SM, drho_SM_dT = thermodynamic_rho_SM(cls.eos, cls.units)
        # staticmethod, so that self.rho_SM(T) is not called as a bound method
        cls.rho_SM = staticmethod(rho_SM)
        cls.drho_SM_dT = staticmethod(drho_SM_dT)

        # knots, in the order the solver produces them (decreasing T)
        n_knots = int(round(KNOTS_PER_DECADE * log10(T_MAX_MEV / T_MIN_MEV))) + 1
        x_increasing = np.linspace(log(T_MIN_MEV), log(T_MAX_MEV), n_knots)
        cls.x_knots = x_increasing[::-1].copy()
        cls.T_knots = np.exp(cls.x_knots)
        cls.rho_SM_knots = np.array([cls.rho_SM(T) for T in cls.T_knots])

        # the evaluation grid
        cls.x_eval = np.linspace(log(T_EVAL_LO_MEV), log(T_EVAL_HI_MEV), N_EVAL)
        cls.T_eval = np.exp(cls.x_eval)
        cls.rho_SM_eval = np.array([cls.rho_SM(T) for T in cls.T_eval])
        cls.drho_SM_dT_eval = np.array([cls.drho_SM_dT(T) for T in cls.T_eval])

    # helpers

    def _ratio_callbacks(self, family: str):
        ratio, _ = FAMILIES[family]
        r = ratio(self.T_knots)
        return build_NP_callbacks(
            self.x_knots,
            r,
            r / 3.0,
            self.rho_SM,
            self.drho_SM_dT,
            T_min_MeV=T_MIN_MEV,
            T_max_MeV=T_MAX_MEV,
            task_label=f"test-{family}",
        )

    def _asinh_callbacks(self, family: str):
        """
        The representation before prompt 04, rebuilt here from its three-line
        construction (BBNData.py at ec206a3): asinh(rho / MeV^4) splined against
        ln(T/MeV), inverted with sinh, and drho/dT = sqrt(1 + rho^2)/T * spline'.
        """
        ratio, _ = FAMILIES[family]
        rho_knots = ratio(self.T_knots) * self.rho_SM_knots
        x = self.x_knots[::-1]
        rho_spline = make_interp_spline(x, np.arcsinh(rho_knots[::-1]), k=3)
        P_spline = make_interp_spline(x, np.arcsinh(rho_knots[::-1] / 3.0), k=3)
        rho_derivative_spline = rho_spline.derivative()

        def rho(T):
            return float(np.sinh(rho_spline(log(T))))

        def P(T):
            return float(np.sinh(P_spline(log(T))))

        def drho(T):
            value = np.sinh(rho_spline(log(T)))
            return float(
                np.sqrt(1.0 + value * value) / T * rho_derivative_spline(log(T))
            )

        return rho, P, drho

    def _truth(self, family: str):
        ratio, dratio_dlogT = FAMILIES[family]
        r = ratio(self.T_eval)
        rho = r * self.rho_SM_eval
        drho = (
            dratio_dlogT(self.T_eval) * self.rho_SM_eval / self.T_eval
            + r * self.drho_SM_dT_eval
        )
        return rho, rho / 3.0, drho

    def _errors(self, rho, P, drho, family: str):
        """Maximum value and derivative errors of three callables on the eval grid."""
        rho_true, P_true, drho_true = self._truth(family)
        rho_c = np.array([rho(T) for T in self.T_eval])
        P_c = np.array([P(T) for T in self.T_eval])
        drho_c = np.array([drho(T) for T in self.T_eval])
        return {
            "rho": float(np.max(np.abs(rho_c - rho_true) / self.rho_SM_eval)),
            "P": float(np.max(np.abs(P_c - P_true) / self.rho_SM_eval)),
            "drho": float(
                np.max(
                    np.abs(drho_c - drho_true) * self.T_eval / (4.0 * self.rho_SM_eval)
                )
            ),
        }

    def _report(self, label, errors):
        if REPORT:
            print(
                f"\n[{label}] max rho {errors['rho']:.3e}, P {errors['P']:.3e}, "
                f"drho {errors['drho']:.3e}"
            )

    # the cases

    def test_a_constant_ratio_is_exact(self):
        """(a) r = 0.08, s = r/3: rho_NP and P_NP are r rho_SM and s rho_SM to 1e-12
        of rho_SM; drho_NP_dT is r drho_SM/dT to 1e-9 of 4 rho_SM / T."""
        cb = self._ratio_callbacks("constant")
        errors = self._errors(cb.rho_NP, cb.P_NP, cb.drho_NP_dT, "constant")
        self._report("constant, ratio", errors)

        self.assertLessEqual(errors["rho"], CONSTANT_VALUE_TOLERANCE)
        self.assertLessEqual(errors["P"], CONSTANT_VALUE_TOLERANCE)
        self.assertLessEqual(errors["drho"], CONSTANT_DERIVATIVE_TOLERANCE)

    def test_b_oscillating_ratio_bounds(self):
        """(b) README section 2 (d)'s oscillating ratio: <= 2e-8 in rho_NP/rho_SM
        (and P_NP/rho_SM), <= 1.5e-6 in the derivative measure. Prints the maxima."""
        cb = self._ratio_callbacks("oscillating")
        errors = self._errors(cb.rho_NP, cb.P_NP, cb.drho_NP_dT, "oscillating")
        print(
            f"\n[test_bbn_callbacks (b)] oscillating ratio: max rho_NP/rho_SM error "
            f"{errors['rho']:.3e}, P_NP {errors['P']:.3e}, derivative {errors['drho']:.3e}"
        )

        self.assertLessEqual(errors["rho"], OSCILLATING_VALUE_BOUND)
        self.assertLessEqual(errors["P"], OSCILLATING_VALUE_BOUND)
        self.assertLessEqual(errors["drho"], OSCILLATING_DERIVATIVE_BOUND)

    def test_c_no_worse_than_asinh(self):
        """(c) On both families the ratio representation's maximum error is no
        larger than the asinh representation's, for rho, P and the derivative."""
        for family in FAMILIES:
            cb = self._ratio_callbacks(family)
            ratio_errors = self._errors(cb.rho_NP, cb.P_NP, cb.drho_NP_dT, family)
            asinh_errors = self._errors(*self._asinh_callbacks(family), family)
            self._report(f"{family}, ratio", ratio_errors)
            self._report(f"{family}, asinh", asinh_errors)
            for key in ("rho", "P", "drho"):
                with self.subTest(family=family, quantity=key):
                    self.assertLessEqual(ratio_errors[key], asinh_errors[key])

    def test_d_non_monotonic_input_is_refused(self):
        """(d) log_T_MeV not strictly decreasing raises ComputationFailureError
        naming the first offending pair; an equal pair is refused too."""
        r = ratio_constant(self.T_knots)
        k = 1000

        swapped = self.x_knots.copy()
        swapped[k], swapped[k + 1] = swapped[k + 1], swapped[k]

        repeated = self.x_knots.copy()
        repeated[k + 1] = repeated[k]

        for label, x in (("swapped", swapped), ("repeated", repeated)):
            with self.subTest(label):
                with self.assertRaises(ComputationFailureError) as ctx:
                    build_NP_callbacks(
                        x,
                        r,
                        r / 3.0,
                        self.rho_SM,
                        self.drho_SM_dT,
                        T_min_MeV=T_MIN_MEV,
                        T_max_MeV=T_MAX_MEV,
                        task_label="test-non-monotonic",
                    )
                message = str(ctx.exception)
                self.assertIn(f"sample {k} has log(T/MeV)={x[k]:.10g}", message)
                self.assertIn(f"sample {k + 1} has log(T/MeV)={x[k + 1]:.10g}", message)
                self.assertIn("test-non-monotonic", message)

    def test_e_domain_guards(self):
        """(e) Below T_min and above T_max every callback raises
        ComputationFailureError; a negative T returns 0."""
        cb = self._ratio_callbacks("constant")
        for name, fn in cb._asdict().items():
            with self.subTest(name):
                self.assertEqual(fn(-1.0), 0.0)
                with self.assertRaises(ComputationFailureError):
                    fn(0.5 * T_MIN_MEV)
                with self.assertRaises(ComputationFailureError):
                    fn(2.0 * T_MAX_MEV)
                # the endpoints themselves are inside the domain
                fn(T_MIN_MEV)
                fn(T_MAX_MEV)

    def test_f_units(self):
        """(f) T in MeV gives MeV^4, MeV^4, MeV^3. rho_SM(1 MeV) is about
        3.5 MeV^4 and is the same whatever units the EOS was built in; and
        T drho_NP_dT / rho_NP = 4 + d ln g_rho / d ln T, which is wrong by a
        factor T at T != 1 MeV if the derivative were per ln T."""
        rho_SM_1 = self.rho_SM(1.0)
        self.assertGreater(rho_SM_1, RHO_SM_1MEV_LO)
        self.assertLess(rho_SM_1, RHO_SM_1MEV_HI)

        planck = Planck_units()
        rho_SM_planck, drho_SM_dT_planck = thermodynamic_rho_SM(
            SaikawaShirai_EOS_spline(planck), planck
        )
        for T in (0.02, 0.3, 1.0, 5.0):
            with self.subTest(T=T):
                self.assertAlmostEqual(
                    rho_SM_planck(T) / self.rho_SM(T), 1.0, delta=UNITS_AGREEMENT_RTOL
                )
                self.assertAlmostEqual(
                    drho_SM_dT_planck(T) / self.drho_SM_dT(T),
                    1.0,
                    delta=UNITS_AGREEMENT_RTOL,
                )

        cb = self._ratio_callbacks("constant")
        self.assertAlmostEqual(cb.rho_NP(1.0), CONSTANT_RATIO * rho_SM_1, delta=1e-12)
        self.assertAlmostEqual(
            cb.P_NP(1.0), CONSTANT_RATIO * rho_SM_1 / 3.0, delta=1e-12
        )
        for T in (0.1, 2.0):
            with self.subTest(T=T):
                T_GeV = T * 1e-3 * self.units.GeV
                dlng = float(self.eos.dG_rho_dlogT(T_GeV)) / float(
                    self.eos.G_rho(T_GeV)
                )
                self.assertAlmostEqual(
                    T * cb.drho_NP_dT(T) / cb.rho_NP(T), 4.0 + dlng, delta=1e-9
                )

    def test_g_Hdot_over_H2_Omega_primeprime_term(self):
        """(g) With Omega'' != 0 the new expression differs from the old by
        Omega'' pi (pi - 1) / A1^2; with Omega'' = 0 they are equal. Along a
        trajectory phi(N) with a non-exponential stand-in coupling, the A1'/A1^2
        term of the new expression is dA1/dN / A1^2, dA1/dN taken by a central
        difference in N of A1 = 1 + Omega'(phi(N)) pi(N); the old one is not."""
        HE, Op, Opp, p, pp = -2.0, 2.0, 5.0, 0.3, 0.1
        A1 = 1.0 + Op * p
        new = jordan_Hdot_over_H2(HE, Op, Opp, p, pp)
        old = _old_HJdot_over_HJ2(HE, Op, Opp, p, pp)
        self.assertAlmostEqual(new - old, Opp * p * (p - 1.0) / A1**2, delta=1e-14)

        self.assertEqual(
            jordan_Hdot_over_H2(HE, Op, 0.0, p, pp),
            _old_HJdot_over_HJ2(HE, Op, 0.0, p, pp),
        )

        coupling = QuadraticStandIn(mu=0.7)
        phi0, a, b = 0.4, 0.25, -0.15

        def phi(N):
            return phi0 + a * N + b * N * N

        def pi_(N):
            return a + 2.0 * b * N

        def A1_of(N):
            return 1.0 + coupling.d_logOmega_dphi(phi(N)) * pi_(N)

        N0, h = 0.3, 1e-5
        dA1_dN = (A1_of(N0 + h) - A1_of(N0 - h)) / (2.0 * h)

        Op0 = coupling.d_logOmega_dphi(phi(N0))
        Opp0 = coupling.d2_logOmega_dphi2(phi(N0))
        p0, pp0 = pi_(N0), 2.0 * b
        A10 = A1_of(N0)
        first = (HE - Op0 * p0) / A10

        new_A1_term = jordan_Hdot_over_H2(HE, Op0, Opp0, p0, pp0) - first
        old_A1_term = _old_HJdot_over_HJ2(HE, Op0, Opp0, p0, pp0) - first
        expected = dA1_dN / A10**2

        self.assertAlmostEqual(new_A1_term, expected, delta=1e-8)
        self.assertGreater(abs(old_A1_term - expected), 1e-2)

    def test_h_end_to_end_constant_ratio(self):
        """(h) The constant ratio 0.08 through build_NP_callbacks into PRyMordial
        reproduces prompt 03's Yp and D/H to 1e-4 relative.
        **Runs one PRyMordial solve, about 10 s.**"""
        cb = self._ratio_callbacks("constant")
        res = run_prym(cb.rho_NP, cb.P_NP, cb.drho_NP_dT)

        Yp, DoH = res[RES_YP_BBN], res[RES_D_OVER_H_E5]
        dYp = abs(Yp - PROMPT_03_CONSTANT_YP) / PROMPT_03_CONSTANT_YP
        dDoH = (
            abs(DoH - PROMPT_03_CONSTANT_D_OVER_H_E5) / PROMPT_03_CONSTANT_D_OVER_H_E5
        )
        print(
            f"\n[test_bbn_callbacks (h)] Yp {Yp:.10g} ({dYp:.2e}), "
            f"D/H x1e5 {DoH:.10g} ({dDoH:.2e}) against prompt 03"
        )
        with self.subTest("Yp"):
            self.assertLessEqual(dYp, END_TO_END_RTOL)
        with self.subTest("D/H"):
            self.assertLessEqual(dDoH, END_TO_END_RTOL)

    def test_i_SM_baseline(self):
        """(i) compute_SM_baseline(False) reproduces README section 2 (f) row 1 to
        1e-4 relative in all four abundances, and names the PRyMordial version.
        **Runs one PRyMordial solve, about 10 s.**"""
        with _SavedPRyMGlobals():
            baseline = compute_SM_baseline(False)

        print(
            "\n[test_bbn_callbacks (i)] baseline "
            + ", ".join(f"{k} {baseline[k]:.10g}" for k in README_BASELINE)
        )
        self.assertFalse(baseline["small_network"])
        self.assertEqual(baseline["PRyM_version"], PRYM_VERSION)
        for key, reference in README_BASELINE.items():
            with self.subTest(key):
                self.assertLessEqual(
                    abs(baseline[key] - reference) / reference, BASELINE_RTOL
                )


if __name__ == "__main__":
    unittest.main()
