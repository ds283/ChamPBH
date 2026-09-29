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
The Jordan-frame temperature law, scored against exact entropy conservation.

Written for review-remediation prompt 01, which *characterised* items R1 (the
ln 10 in dG_s_dlogT) and R5 (the 10 keV join) on the unfixed tree. Prompt 02
fixed both and flipped the named constants below: the tests now guard the
corrected law, and fail on the tree before prompt 02 with the characterised
values (offset +1.422 e-folds, ratio 2.303, rho_R ratio 0.0041; +1.465e-4 at
the join with R1 fixed but not R5). Every constant's comment says which prompt
set it.

Run from the repository root; Xav_EOS_spline reads its CSV by a relative path.
No Ray cluster, no datastore, no PRyMordial. The whole module takes about five
seconds, most of it building and evaluating the jax class.

Set CHAMPBH_TEST_REPORT=1 to print the measured values as well.
"""

import importlib.util
import os
import sys
import unittest
from math import log

from CosmologyModels.GenericEOS.SaikawaShirai_common import (
    HIGH_T_GSTAR,
    LOW_T_GSTAR,
    LOW_T_G_S_STAR,
    SAIKAWA_SHIRAI_T_HI,
    SAIKAWA_SHIRAI_T_LO,
)
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from CosmologyModels.tests.eos_reference import (
    T_INIT_GEV,
    T_CMB_GeV,
    central_dG_s_dlogT,
    derivative_test_grid_GeV,
    exact_efolds,
    integrate_temperature_law,
    production_eos,
)
from Units import GeV_units

LN10 = log(10.0)

# kappa multiplies dG_s_dlogT in the law. Prompt 01 used 1/ln 10 to convert the
# shipped d/d log10 T to d/d ln T. Prompt 02 moved that conversion into the EOS
# class, so both are now 1 and case 2 does not divide twice.
KAPPA_SHIPPED = 1.0
KAPPA_CORRECTED = 1.0

HAVE_JAX = importlib.util.find_spec("jax") is not None

REPORT = bool(os.environ.get("CHAMPBH_TEST_REPORT"))

# ---------------------------------------------------------------------------
# Case 1: the guard (kappa = 1, the law as shipped)
# ---------------------------------------------------------------------------

# T1 in GeV (None stands for T_CMB) -> N_code - N_exact from T_INIT_GEV.
# Before prompt 02 these were the audit's offsets 0.180, 0.779, 0.987, 0.994,
# 1.336, 1.403, 1.422, 1.422 (tlaw_check.py on f5896bb), with tolerance 2e-3.
# Prompt 02 set every one to 0.0 with tolerance 1e-5 (README §6.1).
EXPECTED_EFOLD_OFFSET_SHIPPED = {
    1.0: 0.0,
    0.1: 0.0,
    5e-3: 0.0,
    1e-3: 0.0,
    1e-4: 0.0,
    7e-5: 0.0,
    1e-5: 0.0,
    None: 0.0,
}

TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE = 1e-5

# ---------------------------------------------------------------------------
# Case 2: the guard, split at the 10 keV join (kappa = KAPPA_CORRECTED = 1)
# ---------------------------------------------------------------------------

# Prompt 01 ran this case with kappa = 1/ln 10 to show that dividing by ln 10
# is the whole of R1 above the join. Since prompt 02 it runs the same law as
# case 1; it is kept for the split at the join and the step accounting, which
# see R5 independently of R1.
T1_ABOVE_JOIN_GEV = (1.0, 0.1, 5e-3, 1e-3, 1e-4, 7e-5)
CORRECTED_LAW_EFOLD_TOLERANCE = 1e-5

# At and below the join (10 keV, T_CMB) the residual N_code - N_exact is R5's
# step, -(1/3) ln[G_s(T_LO+)/G_s(T_LO)]. With LOW_T_G_S_STAR = 3.94 it was
# +1.465e-4 (low_t_join_probe.py on 41b410d; tolerance 5e-6). Prompt 02 set
# LOW_T_G_S_STAR to the fit's own limit 3.931, and this to 0.0 with tolerance 1e-5.
T1_AT_OR_BELOW_JOIN_GEV = (1e-5, None)
LOW_T_JOIN_EFOLD_RESIDUAL = 0.0
LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE = 1e-5

# The residual is accounted for by -(1/3) ln[G_s(T_LO+)/G_s(T_LO)] to this
# accuracy. What is left is the ~4e-8 seen at every other point. Prompt 02 kept it.
LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE = 1e-6

# "just above" the join, as in low_t_join_probe.py
T_LO_PLUS_GEV = SAIKAWA_SHIRAI_T_LO * (1.0 + 1e-9)

# ---------------------------------------------------------------------------
# Case 3: the derivative convention is natural log
# ---------------------------------------------------------------------------

# |dG_s_dlogT - central_dG_s_dlogT| / G_s, that is the error in
# d ln g_s / d ln T, the quantity the temperature law consumes, at every point
# of derivative_test_grid_GeV(). Prompt 01 asserted the ratio, ln 10 to 1e-3
# relative. Prompt 02 changed the form of the assertion (README §6.1, amended
# 2026-09-29): a ratio test fails in the e+- tail, where the derivative is ~1e-6,
# even with an exact fix. The ratio is still printed in the failure message.
DERIVATIVE_CONVENTION_TOLERANCE = 1e-6  # absolute, in d ln g_s / d ln T

# ---------------------------------------------------------------------------
# Case 4: the spline and jax implementations agree
# ---------------------------------------------------------------------------

# |spline.dG_s_dlogT - jax.dG_s_dlogT| / spline.G_s on the same grid. Prompt 01
# asserted the ratio, ln 10 to 1e-3 relative; prompt 02 changed the form as
# for case 3.
IMPLEMENTATION_AGREEMENT_TOLERANCE = 1e-6  # absolute, in d ln g_s / d ln T

# ---------------------------------------------------------------------------
# Case 5: the rho_R witness
# ---------------------------------------------------------------------------

# Integrated rho_R / (pi^2/30) g_rho T^4 from 5 MeV to 10 keV (README §2 (c)).
# Before prompt 02: 0.18184 with the law as shipped and 1.00471 with ln 10
# divided out but R5 not fixed (low_t_join_probe.py ship, on 41b410d). Prompt 02
# removed the shipped-law half and re-centred this on the value measured after
# R1 and R5, 1.00135 (the audit §11 probe gives the same). Prompt 05 tightens it.
RHO_R_WITNESS_T0_GEV = 5e-3
RHO_R_WITNESS_T1_GEV = 1e-5
RHO_R_WITNESS_CORRECTED = 1.00135
RHO_R_WITNESS_CORRECTED_TOLERANCE = 5e-3

# The same witness from T_INIT_GEV, read off the case-2 runs at 1 MeV, 70 keV
# and 10 keV. Before prompt 02 the shipped law gave 0.0216 / 0.0044 / 0.0041
# (README §6.1's "now" column). The centres are README §6.1's 0.998 / 0.999
# and, at 10 keV, the value prompt 02 measured after R5, 0.99922 (it was 1.0026
# with R1 fixed alone). Prompt 05 tightens it.
RHO_R_WITNESS_FROM_INIT_CORRECTED = {1e-3: 0.998, 7e-5: 0.999, 1e-5: 0.99922}
RHO_R_WITNESS_FROM_INIT_CORRECTED_TOLERANCE = 3e-3

# ---------------------------------------------------------------------------
# Case 6: clamps
# ---------------------------------------------------------------------------

# Temperatures (GeV) at or beyond the clamps. The values hold before and after
# prompt 02; the constants they are compared with are imported, not copied.
CLAMP_LOW_T_GEV = (SAIKAWA_SHIRAI_T_LO, 1e-6, 1e-9, 2.35e-13)
CLAMP_HIGH_T_GEV = (SAIKAWA_SHIRAI_T_HI, 1e17, 1e19)


def _T1(eos, key):
    return T_CMB_GeV(eos) if key is None else key


def _label(key):
    return "T_CMB" if key is None else f"{key:.3g} GeV"


class TestTemperatureLaw(unittest.TestCase):
    """
    Guard for R1 and R5. All runs start at T_INIT_GEV = 2e4 GeV, except the
    5 MeV rho_R witness. They are made once, here, and shared by the cases.
    """

    @classmethod
    def setUpClass(cls):
        cls.eos = production_eos()
        cls.GeV = cls.eos._units.GeV

        cls.exact = {}
        cls.shipped = {}
        cls.corrected = {}
        for key in EXPECTED_EFOLD_OFFSET_SHIPPED:
            T1 = _T1(cls.eos, key)
            cls.exact[key] = exact_efolds(cls.eos, T_INIT_GEV, T1)
            cls.shipped[key] = integrate_temperature_law(
                cls.eos, T_INIT_GEV, T1, kappa=KAPPA_SHIPPED, with_rho=True
            )
            cls.corrected[key] = integrate_temperature_law(
                cls.eos, T_INIT_GEV, T1, kappa=KAPPA_CORRECTED, with_rho=True
            )

        cls.rho_witness_corrected = integrate_temperature_law(
            cls.eos,
            RHO_R_WITNESS_T0_GEV,
            RHO_R_WITNESS_T1_GEV,
            kappa=KAPPA_CORRECTED,
            with_rho=True,
        )

        cls.G_s_above_join = float(cls.eos.G_s(T_LO_PLUS_GEV * cls.GeV))
        cls.G_s_at_join = float(cls.eos.G_s(SAIKAWA_SHIRAI_T_LO * cls.GeV))
        cls.join_step = log(cls.G_s_above_join / cls.G_s_at_join) / 3.0

        cls.grid = derivative_test_grid_GeV()

        if REPORT:
            cls._report()

    @classmethod
    def _report(cls):
        out = sys.stderr
        print("\n[test_temperature_law] from 2e4 GeV:", file=out)
        print(
            f"{'T1':>12s} {'N exact':>10s} {'N code':>10s} {'N code - N exact':>17s}"
            f" {'rho ratio':>10s}",
            file=out,
        )
        for key in EXPECTED_EFOLD_OFFSET_SHIPPED:
            a, ex = cls.shipped[key], cls.exact[key]
            print(
                f"{_label(key):>12s} {ex:10.4f} {a.efolds:10.4f} {a.efolds - ex:+17.3e}"
                f" {a.rho_R_ratio:10.5f}",
                file=out,
            )
        print(
            f"rho_R witness 5 MeV -> 10 keV: {cls.rho_witness_corrected.rho_R_ratio:.5f}",
            file=out,
        )
        print(
            f"join: G_s(T_LO+) = {cls.G_s_above_join:.6f}, G_s(T_LO) = {cls.G_s_at_join:.6f},"
            f" (1/3) ln ratio = {cls.join_step:+.4e}",
            file=out,
        )

    # Case 1
    def test_guard_temperature_law_matches_entropy_conservation(self):
        """
        N from 2e4 GeV with the law as shipped agrees with exact entropy
        conservation to 1e-5 at every T1, down to T_CMB (R1 and R5).
        """
        for key, expected in EXPECTED_EFOLD_OFFSET_SHIPPED.items():
            with self.subTest(T1=_label(key)):
                offset = self.shipped[key].efolds - self.exact[key]
                self.assertAlmostEqual(
                    offset,
                    expected,
                    delta=TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE,
                    msg=f"T1 = {_label(key)}: N_code - N_exact = {offset:+.6e}, expected {expected:+.3f}"
                    f" (N_code = {self.shipped[key].efolds:.6f}, N_exact = {self.exact[key]:.6f})",
                )

    # Case 2
    def test_guard_corrected_convention(self):
        """
        With kappa = KAPPA_CORRECTED the law agrees with exact entropy
        conservation to 1e-5 above the 10 keV join and at and below it. The
        residual at and below the join is R5's step, and
        -(1/3) ln[G_s(T_LO+)/G_s(T_LO)] accounts for it; since prompt 02 both are zero.
        """
        for key in T1_ABOVE_JOIN_GEV:
            with self.subTest(T1=_label(key), region="above join"):
                resid = self.corrected[key].efolds - self.exact[key]
                self.assertLessEqual(
                    abs(resid),
                    CORRECTED_LAW_EFOLD_TOLERANCE,
                    msg=f"T1 = {_label(key)}: corrected-law residual {resid:+.3e}"
                    f" exceeds {CORRECTED_LAW_EFOLD_TOLERANCE:g}",
                )

        step_msg = (
            f"G_s(T_LO+) = {self.G_s_above_join:.6f}, G_s(T_LO) = {self.G_s_at_join:.6f},"
            f" (1/3) ln ratio = {self.join_step:+.4e}"
        )
        for key in T1_AT_OR_BELOW_JOIN_GEV:
            resid = self.corrected[key].efolds - self.exact[key]
            with self.subTest(T1=_label(key), region="at/below join"):
                self.assertAlmostEqual(
                    resid,
                    LOW_T_JOIN_EFOLD_RESIDUAL,
                    delta=LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE,
                    msg=f"T1 = {_label(key)}: corrected-law residual {resid:+.4e},"
                    f" expected {LOW_T_JOIN_EFOLD_RESIDUAL:+.4e}; {step_msg}",
                )
            with self.subTest(T1=_label(key), region="step accounting"):
                self.assertLessEqual(
                    abs(resid + self.join_step),
                    LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE,
                    msg=f"T1 = {_label(key)}: residual {resid:+.4e} is not accounted for by"
                    f" the join step; {step_msg}",
                )

    # Case 3
    def test_derivative_convention_is_natural_log(self):
        """
        dG_s_dlogT against a central difference of G_s in ln T, on the 60-point
        grid of derivative_test_grid_GeV(): the error in d ln g_s / d ln T is at
        most 1e-6 at every point (R1).
        """
        self.assertEqual(len(self.grid), 60)
        for T in self.grid:
            with self.subTest(T_GeV=f"{T:.4g}"):
                T_units = T * self.GeV
                deriv = float(self.eos.dG_s_dlogT(T_units))
                central = central_dG_s_dlogT(self.eos, T)
                G_s = float(self.eos.G_s(T_units))
                err = abs(deriv - central) / G_s
                self.assertLessEqual(
                    err,
                    DERIVATIVE_CONVENTION_TOLERANCE,
                    msg=f"T = {T:.5g} GeV: |d ln g_s/d ln T - central| = {err:.3e}"
                    f" (dG_s_dlogT {deriv:.8e}, central {central:.8e}, ratio"
                    f" {deriv / central:.8f}; ln 10 = {LN10:.8f})",
                )

    # Case 4
    @unittest.skipUnless(HAVE_JAX, "jax is not importable")
    def test_spline_and_jax_derivatives_agree(self):
        """
        SaikawaShirai_EOS_spline.dG_s_dlogT against SaikawaShirai_EOS_jax_autodiff.dG_s_dlogT
        on the same grid: they differ by at most 1e-6 in d ln g_s / d ln T (R1).
        The jax class returns T dg_s/dT.
        """
        from CosmologyModels.GenericEOS.SaikawaShirai_EOS_jax_autodiff import (
            SaikawaShirai_EOS_jax_autodiff,
        )

        units = GeV_units()
        spline = SaikawaShirai_EOS_spline(units)
        jax_eos = SaikawaShirai_EOS_jax_autodiff(units)
        for T in self.grid:
            with self.subTest(T_GeV=f"{T:.4g}"):
                T_units = T * units.GeV
                d_spline = float(spline.dG_s_dlogT(T_units))
                d_jax = float(jax_eos.dG_s_dlogT(T_units))
                err = abs(d_spline - d_jax) / float(spline.G_s(T_units))
                self.assertLessEqual(
                    err,
                    IMPLEMENTATION_AGREEMENT_TOLERANCE,
                    msg=f"T = {T:.5g} GeV: |d ln g_s/d ln T, spline - jax| = {err:.3e}"
                    f" (spline {d_spline:.8e}, jax {d_jax:.8e}, ratio"
                    f" {d_spline / d_jax:.8f}; ln 10 = {LN10:.8f})",
                )

    # Case 5
    def test_rho_R_witness(self):
        """
        The radiation density carried with d ln rho_R/dN = Sigma - 4 against
        (pi^2/30) g_rho T^4. Asserted from 5 MeV to 10 keV (prompt 01's range),
        and from 2e4 GeV at 1 MeV, 70 keV and 10 keV (README §6.1).
        """
        label = "5 MeV -> 10 keV"
        result = self.rho_witness_corrected
        expected, tol = RHO_R_WITNESS_CORRECTED, RHO_R_WITNESS_CORRECTED_TOLERANCE
        with self.subTest(case=label):
            self.assertAlmostEqual(
                result.rho_R_ratio,
                expected,
                delta=tol,
                msg=f"{label}: rho_R ratio {result.rho_R_ratio:.5f}, expected {expected} +/- {tol:g}",
            )

        tol = RHO_R_WITNESS_FROM_INIT_CORRECTED_TOLERANCE
        for key, expected in RHO_R_WITNESS_FROM_INIT_CORRECTED.items():
            label = f"2e4 GeV -> {_label(key)}"
            with self.subTest(case=label):
                ratio = self.corrected[key].rho_R_ratio
                self.assertAlmostEqual(
                    ratio,
                    expected,
                    delta=tol,
                    msg=f"{label}: rho_R ratio {ratio:.5f}, expected {expected} +/- {tol:g}",
                )

    # Case 6
    def test_clamps(self):
        """
        At and below SAIKAWA_SHIRAI_T_LO and at and above SAIKAWA_SHIRAI_T_HI the
        derivatives are exactly zero and the g's take the plateau constants.
        """
        for T in CLAMP_LOW_T_GEV:
            with self.subTest(T_GeV=f"{T:.3g}", side="low"):
                T_units = T * self.GeV
                self.assertEqual(float(self.eos.dG_s_dlogT(T_units)), 0.0)
                self.assertEqual(float(self.eos.dG_rho_dlogT(T_units)), 0.0)
                self.assertEqual(float(self.eos.G_s(T_units)), LOW_T_G_S_STAR)
                self.assertEqual(float(self.eos.G_rho(T_units)), LOW_T_GSTAR)
        for T in CLAMP_HIGH_T_GEV:
            with self.subTest(T_GeV=f"{T:.3g}", side="high"):
                T_units = T * self.GeV
                self.assertEqual(float(self.eos.dG_s_dlogT(T_units)), 0.0)
                self.assertEqual(float(self.eos.dG_rho_dlogT(T_units)), 0.0)
                self.assertEqual(float(self.eos.G_s(T_units)), HIGH_T_GSTAR)
                self.assertEqual(float(self.eos.G_rho(T_units)), HIGH_T_GSTAR)


if __name__ == "__main__":
    unittest.main()
