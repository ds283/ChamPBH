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

Written for review-remediation prompt 01. Items R1 (the ln 10 in dG_s_dlogT) and
R5 (the 10 keV join) are *characterised* here: these tests pass on the unfixed
tree and record the defect's size. Prompt 02 fixes both and changes the named
constants below. Every constant's comment says which prompt changes it.

Run from the repository root; Xav_EOS_spline reads its CSV by a relative path.
No Ray cluster, no datastore, no PRyMordial. The whole module takes about a
second.

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
    derivative_convention_ratio,
    derivative_test_grid_GeV,
    exact_efolds,
    integrate_temperature_law,
    production_eos,
)
from Units import GeV_units

LN10 = log(10.0)

# the corrected convention: kappa converts d/d log10 T to d/d ln T
KAPPA_SHIPPED = 1.0
KAPPA_CORRECTED = 1.0 / LN10

HAVE_JAX = importlib.util.find_spec("jax") is not None

REPORT = bool(os.environ.get("CHAMPBH_TEST_REPORT"))

# ---------------------------------------------------------------------------
# Case 1: the guard, characterised (kappa = 1, the law as shipped)
# ---------------------------------------------------------------------------

# T1 in GeV (None stands for T_CMB) -> N_code - N_exact from T_INIT_GEV, as
# measured by .documents/audit-2026-09-29/tlaw_check.py on f5896bb.
# Prompt 02 replaces every expected offset by 0.0.
EXPECTED_EFOLD_OFFSET_SHIPPED = {
    1.0: 0.180,
    0.1: 0.779,
    5e-3: 0.987,
    1e-3: 0.994,
    1e-4: 1.336,
    7e-5: 1.403,
    1e-5: 1.422,
    None: 1.422,
}

# Prompt 02 tightens this to 1e-5 when it sets the expected offsets to zero.
TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE = 2e-3

# ---------------------------------------------------------------------------
# Case 2: the guard, corrected convention (kappa = 1/ln 10)
# ---------------------------------------------------------------------------

# Above the 10 keV join the corrected law must agree with exact entropy
# conservation. This holds today: it shows that dividing by ln 10 is the whole
# of R1. Prompt 02 keeps it at 1e-5, with kappa = 1.
T1_ABOVE_JOIN_GEV = (1.0, 0.1, 5e-3, 1e-3, 1e-4, 7e-5)
CORRECTED_LAW_EFOLD_TOLERANCE = 1e-5

# At and below the join (10 keV, T_CMB) the residual N_code - N_exact is R5's step:
# G_s is the spline just above SAIKAWA_SHIRAI_T_LO and LOW_T_G_S_STAR = 3.94 at
# and below it. low_t_join_probe.py measures +1.465e-4 on 41b410d.
# Prompt 02 sets this to 0.0 with tolerance 1e-5 when it corrects LOW_T_G_S_STAR.
T1_AT_OR_BELOW_JOIN_GEV = (1e-5, None)
LOW_T_JOIN_EFOLD_RESIDUAL = 1.465e-4
LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE = 5e-6

# The residual is accounted for by -(1/3) ln[G_s(T_LO+)/G_s(T_LO)] to this
# accuracy. What is left is the ~4e-8 seen at every other point. Prompt 02 keeps it.
LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE = 1e-6

# "just above" the join, as in low_t_join_probe.py
T_LO_PLUS_GEV = SAIKAWA_SHIRAI_T_LO * (1.0 + 1e-9)

# ---------------------------------------------------------------------------
# Case 3: the derivative convention, characterised
# ---------------------------------------------------------------------------

# dG_s_dlogT / (central difference of G_s in ln T) on derivative_test_grid_GeV().
# It is ln 10 while the spline class returns d/d log10 T.
# Prompt 02 sets the expected ratio to 1.0 and the tolerance to 1e-6.
EXPECTED_DERIVATIVE_CONVENTION_RATIO = LN10
DERIVATIVE_CONVENTION_RATIO_TOLERANCE = 1e-3  # relative

# ---------------------------------------------------------------------------
# Case 4: the spline and jax implementations disagree by ln 10
# ---------------------------------------------------------------------------

# SaikawaShirai_EOS_spline.dG_s_dlogT / SaikawaShirai_EOS_jax_autodiff.dG_s_dlogT
# on the same grid. Prompt 02 sets the expected ratio to 1.0 and the tolerance to 1e-6.
EXPECTED_IMPLEMENTATION_RATIO = LN10
IMPLEMENTATION_RATIO_TOLERANCE = 1e-3  # relative

# ---------------------------------------------------------------------------
# Case 5: the rho_R witness, characterised
# ---------------------------------------------------------------------------

# Integrated rho_R / (pi^2/30) g_rho T^4 from 5 MeV to 10 keV (README §2 (c)).
# The event values are 0.18184 and 1.00471 (low_t_join_probe.py ship, on 41b410d).
# Prompt 02 removes the kappa = 1 half and re-centres the corrected half on the
# value it measures after R5; the audit §11 probe gives 1.00135. Prompt 05
# tightens it.
RHO_R_WITNESS_T0_GEV = 5e-3
RHO_R_WITNESS_T1_GEV = 1e-5
RHO_R_WITNESS_SHIPPED = 0.182
RHO_R_WITNESS_SHIPPED_TOLERANCE = 2e-3
RHO_R_WITNESS_CORRECTED = 1.005
RHO_R_WITNESS_CORRECTED_TOLERANCE = 5e-3

# The same witness from T_INIT_GEV, read off the case-1 and case-2 runs at 1 MeV,
# 70 keV and 10 keV. These are README §6.1's "now" column (0.022 / 0.0044 / 0.0041)
# and the corrected-law row (0.998 / 0.999 / 1.0026). The shipped-law centres
# are the measured values to two significant figures (1 MeV is 0.0216).
# Prompt 02 removes the kappa = 1 half and re-centres 10 keV on the value it
# measures after R5; the probe gives 0.99922. Prompt 05 tightens it.
RHO_R_WITNESS_FROM_INIT_SHIPPED = {1e-3: 0.0216, 7e-5: 0.0044, 1e-5: 0.0041}
RHO_R_WITNESS_FROM_INIT_SHIPPED_TOLERANCE = 2e-4
RHO_R_WITNESS_FROM_INIT_CORRECTED = {1e-3: 0.998, 7e-5: 0.999, 1e-5: 1.0026}
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

        cls.rho_witness_shipped = integrate_temperature_law(
            cls.eos,
            RHO_R_WITNESS_T0_GEV,
            RHO_R_WITNESS_T1_GEV,
            kappa=KAPPA_SHIPPED,
            with_rho=True,
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
            f"{'T1':>12s} {'N exact':>10s} {'offset k=1':>11s} {'resid k=1/ln10':>15s}"
            f" {'rho k=1':>9s} {'rho k=1/ln10':>13s}",
            file=out,
        )
        for key in EXPECTED_EFOLD_OFFSET_SHIPPED:
            a, b, ex = cls.shipped[key], cls.corrected[key], cls.exact[key]
            print(
                f"{_label(key):>12s} {ex:10.4f} {a.efolds - ex:+11.4f} {b.efolds - ex:+15.3e}"
                f" {a.rho_R_ratio:9.5f} {b.rho_R_ratio:13.5f}",
                file=out,
            )
        print(
            f"rho_R witness 5 MeV -> 10 keV: k=1 {cls.rho_witness_shipped.rho_R_ratio:.5f},"
            f" k=1/ln10 {cls.rho_witness_corrected.rho_R_ratio:.5f}",
            file=out,
        )
        print(
            f"join: G_s(T_LO+) = {cls.G_s_above_join:.6f}, G_s(T_LO) = {cls.G_s_at_join:.6f},"
            f" (1/3) ln ratio = {cls.join_step:+.4e}",
            file=out,
        )

    # Case 1
    def test_guard_shipped_convention_offset_is_characterised(self):
        """
        N from 2e4 GeV with the law as shipped overshoots exact entropy
        conservation by the audit's offsets (R1, characterised).
        """
        for key, expected in EXPECTED_EFOLD_OFFSET_SHIPPED.items():
            with self.subTest(T1=_label(key)):
                offset = self.shipped[key].efolds - self.exact[key]
                self.assertAlmostEqual(
                    offset,
                    expected,
                    delta=TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE,
                    msg=f"T1 = {_label(key)}: N_code - N_exact = {offset:+.6f}, expected {expected:+.3f}"
                    f" (N_code = {self.shipped[key].efolds:.6f}, N_exact = {self.exact[key]:.6f})",
                )

    # Case 2
    def test_guard_corrected_convention(self):
        """
        With kappa = 1/ln 10 the law agrees with exact entropy conservation to
        1e-5 above the 10 keV join. At and below the join the residual is R5's
        step, and -(1/3) ln[G_s(T_LO+)/G_s(T_LO)] accounts for it.
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
    def test_derivative_convention_is_characterised(self):
        """
        dG_s_dlogT against a central difference of G_s in ln T, on the 60-point
        grid of derivative_test_grid_GeV() (R1, characterised).
        """
        self.assertEqual(len(self.grid), 60)
        for T in self.grid:
            with self.subTest(T_GeV=f"{T:.4g}"):
                ratio = derivative_convention_ratio(self.eos, T)
                rel = ratio / EXPECTED_DERIVATIVE_CONVENTION_RATIO - 1.0
                self.assertLessEqual(
                    abs(rel),
                    DERIVATIVE_CONVENTION_RATIO_TOLERANCE,
                    msg=f"T = {T:.5g} GeV: ratio {ratio:.8f}, expected"
                    f" {EXPECTED_DERIVATIVE_CONVENTION_RATIO:.8f} (relative {rel:+.2e})",
                )

    # Case 4
    @unittest.skipUnless(HAVE_JAX, "jax is not importable")
    def test_spline_and_jax_implementations_disagree_by_ln10(self):
        """
        SaikawaShirai_EOS_spline.dG_s_dlogT over SaikawaShirai_EOS_jax_autodiff.dG_s_dlogT
        on the same grid (R1, characterised). The jax class returns T dg_s/dT.
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
                ratio = float(spline.dG_s_dlogT(T_units)) / float(
                    jax_eos.dG_s_dlogT(T_units)
                )
                rel = ratio / EXPECTED_IMPLEMENTATION_RATIO - 1.0
                self.assertLessEqual(
                    abs(rel),
                    IMPLEMENTATION_RATIO_TOLERANCE,
                    msg=f"T = {T:.5g} GeV: spline/jax {ratio:.8f}, expected"
                    f" {EXPECTED_IMPLEMENTATION_RATIO:.8f} (relative {rel:+.2e})",
                )

    # Case 5
    def test_rho_R_witness_is_characterised(self):
        """
        The radiation density carried with d ln rho_R/dN = Sigma - 4 against
        (pi^2/30) g_rho T^4. Asserted from 5 MeV to 10 keV (the prompt's range),
        and from 2e4 GeV at 1 MeV, 70 keV and 10 keV (README §6.1).
        """
        for label, result, expected, tol in (
            (
                "5 MeV -> 10 keV, kappa = 1",
                self.rho_witness_shipped,
                RHO_R_WITNESS_SHIPPED,
                RHO_R_WITNESS_SHIPPED_TOLERANCE,
            ),
            (
                "5 MeV -> 10 keV, kappa = 1/ln 10",
                self.rho_witness_corrected,
                RHO_R_WITNESS_CORRECTED,
                RHO_R_WITNESS_CORRECTED_TOLERANCE,
            ),
        ):
            with self.subTest(case=label):
                self.assertAlmostEqual(
                    result.rho_R_ratio,
                    expected,
                    delta=tol,
                    msg=f"{label}: rho_R ratio {result.rho_R_ratio:.5f}, expected {expected} +/- {tol:g}",
                )

        for runs, table, tol, kappa_label in (
            (
                self.shipped,
                RHO_R_WITNESS_FROM_INIT_SHIPPED,
                RHO_R_WITNESS_FROM_INIT_SHIPPED_TOLERANCE,
                "kappa = 1",
            ),
            (
                self.corrected,
                RHO_R_WITNESS_FROM_INIT_CORRECTED,
                RHO_R_WITNESS_FROM_INIT_CORRECTED_TOLERANCE,
                "kappa = 1/ln 10",
            ),
        ):
            for key, expected in table.items():
                label = f"2e4 GeV -> {_label(key)}, {kappa_label}"
                with self.subTest(case=label):
                    ratio = runs[key].rho_R_ratio
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
