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
The kicking function Sigma(T_J) = 1 - 3 w(T_J) as the pipeline evaluates it.

Written for review-remediation prompt 05 (item R4, the pins). Every value comes
from `Xav_EOS_spline.w`, the production class (`QCD_Cosmology` wraps it), and
never from `Xav_EOS_data.csv` directly. The targets are README §6.4's, with the
peak temperatures restated by the user's decision of 2026-09-29 ("Option 1",
IMPLEMENTATION_STATE.md, Decisions): they are the peaks of the spline `w`
evaluates, not the table rows with the largest Sigma.

These are characterisation tests. They pin what the tree does, so that a change
to the CSV, to the spline, or to which class production uses announces itself.

Case 7 of the prompt (the spline and jax derivatives agree to 1e-6 in
d ln g_s / d ln T) already lives in test_temperature_law.py as
`TestTemperatureLaw.test_spline_and_jax_derivatives_agree` (prompt 02's case 4)
and is not duplicated here.

Run from the repository root; Xav_EOS_spline reads its CSV by a relative path.
No Ray cluster, no datastore, no PRyMordial. The module takes about two seconds.

Set CHAMPBH_TEST_REPORT=1 to print the measured values as well.
"""

import os
import sys
import unittest

import numpy as np
from scipy.integrate import simpson

from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from CosmologyModels.LambdaCDM import Planck2018
from CosmologyModels.model_ids import XAV_IMPROVED_EOS_IDENTIFIER
from CosmologyModels.tests.eos_reference import (
    T_INIT_GEV,
    integrate_temperature_law,
    production_eos,
)
from Units import GeV_units

REPORT = bool(os.environ.get("CHAMPBH_TEST_REPORT"))

# ---------------------------------------------------------------------------
# The evaluation grid
# ---------------------------------------------------------------------------

# Sigma is evaluated on a grid log-spaced in T over [10 keV, 30 TeV]. The prompt
# asks for at least 200 points per decade; 1000 puts the argmax within 0.23 % in
# T of the spline's peak, so the +/- 5 % temperature tolerance is not spent on
# grid spacing.
GRID_T_LO_GEV = 1.0e-5
GRID_T_HI_GEV = 3.0e4
GRID_POINTS_PER_DECADE = 1000

# ---------------------------------------------------------------------------
# Case 1: the three peaks
# ---------------------------------------------------------------------------

# name -> (window lo GeV, window hi GeV, Sigma at peak, T at peak in GeV).
# Sigma: README §6.4 (the audit's §4 table, read off the CSV's rows on f5896bb).
# T: the user's decision of 2026-09-29, the argmax of 1 - 3 Xav_EOS_spline.w on a
# 5000-per-decade grid, measured by the orchestrator on eba4473. README §6.4
# gave the table rows 0.1585 MeV, 0.1778 GeV and 56.23 GeV, which the spline
# peaks between; the electroweak one is 5.3 % away.
# Measured by this module on 89bd52e + prompt 05 at 1000 per decade:
# 0.100732 at 0.16033 MeV, 0.314532 at 0.181993 GeV, 0.037436 at 53.2214 GeV.
PEAKS = {
    "e+e-": (1.0e-5, 5.0e-3, 0.1007, 0.1605e-3),
    "QCD": (5.0e-2, 1.0, 0.3138, 0.1819),
    "electroweak": (20.0, 1.0e3, 0.03733, 53.25),
}
PEAK_SIGMA_TOLERANCE = 1e-3  # absolute
PEAK_T_TOLERANCE = 0.05  # relative

# ---------------------------------------------------------------------------
# Case 2: the e+e- profile
# ---------------------------------------------------------------------------

# T in GeV -> Sigma (README §6.4; audit §4 and eos_consistency.py on f5896bb).
# Measured by this module on 89bd52e + prompt 05: 0.002946, 0.034450, 0.094564,
# 0.067984, 0.002858.
EE_PROFILE = {
    2.0e-3: 0.0030,
    5.0e-4: 0.0345,
    2.0e-4: 0.0946,
    1.0e-4: 0.0680,
    5.0e-5: 0.0029,
}
EE_PROFILE_TOLERANCE = 1e-3  # absolute

# Sigma(20 keV) is below this (measured 5.2e-8).
EE_TAIL_T_GEV = 2.0e-5
EE_TAIL_BOUND = 1e-6

# ---------------------------------------------------------------------------
# Case 3: the integrated e+e- kick
# ---------------------------------------------------------------------------

# int Sigma d ln T over [10 keV, 3 MeV] (README §6.4). Measured by this module
# on 89bd52e + prompt 05: 0.161813 (Simpson, 1000 per decade; scipy quad on the
# same function gives 0.161813).
EE_INTEGRAL_T_LO_GEV = 1.0e-5
EE_INTEGRAL_T_HI_GEV = 3.0e-3
EE_INTEGRAL = 0.1617
EE_INTEGRAL_TOLERANCE = 2e-3

# ---------------------------------------------------------------------------
# Case 4: outside the table
# ---------------------------------------------------------------------------

# Temperatures (GeV) at or beyond the table's ends, 1e-5 GeV and 25118.86 GeV,
# where Xav_EOS_spline.w returns 1.0 / 3.0 without consulting the spline.
# T_min and T_max themselves are added in the test, read from the class.
OUTSIDE_TABLE_LOW_T_GEV = (9.99e-6, 5.0e-6, 1.0e-6, 1.0e-9, 2.35e-13)
OUTSIDE_TABLE_HIGH_T_GEV = (2.6e4, 3.0e4, 1.0e6, 1.0e16)

# ---------------------------------------------------------------------------
# Case 5: the table is consistent with the g's
# ---------------------------------------------------------------------------

# Integrated rho_R / (pi^2/30) g_rho T^4 at 10 keV, the law as it now ships
# (d ln T/dN with d ln g_s / d ln T), d ln rho_R/dN = Sigma - 4.
# Start temperature (GeV) -> centre. The prompt's centres (1.005 from 5 MeV,
# 1.003 from 100 MeV and from 2e4 GeV) predate R5; README §6.4 says prompt 05
# takes the values prompt 02 recorded after R5 (log 02, "State handed to the
# next prompt", on 47c50ae): 1.00135, 1.00005 and 0.99922. The tolerance is the
# prompt's +/- 5e-3. Measured by this module on 89bd52e + prompt 05: 1.001346,
# 1.000053, 0.999223.
TABLE_G_WITNESS_T1_GEV = 1.0e-5
TABLE_G_WITNESS = {
    5.0e-3: 1.00135,
    0.1: 1.00005,
    T_INIT_GEV: 0.99922,
}
TABLE_G_WITNESS_TOLERANCE = 5e-3

# ---------------------------------------------------------------------------
# Case 6: the 2 MeV freeze is not the production path
# ---------------------------------------------------------------------------

FREEZE_T_GEV = 2.0e-3
BELOW_FREEZE_T_GEV = 1.0e-3


def _grid_GeV(T_lo: float, T_hi: float, per_decade: int) -> np.ndarray:
    n = int(round(per_decade * np.log10(T_hi / T_lo))) + 1
    return np.logspace(np.log10(T_lo), np.log10(T_hi), n)


def _label(T_GeV: float) -> str:
    if T_GeV < 1e-3:
        return f"{T_GeV * 1e6:.4g} keV"
    if T_GeV < 1.0:
        return f"{T_GeV * 1e3:.4g} MeV"
    return f"{T_GeV:.4g} GeV"


class TestKickingFunction(unittest.TestCase):
    """
    R4's pins: the peaks, the e+e- profile and its integral, the ends of the
    table, the table-g consistency, and which w() production uses.
    """

    @classmethod
    def setUpClass(cls):
        cls.eos = production_eos()
        cls.GeV = cls.eos._units.GeV

        cls.grid = _grid_GeV(GRID_T_LO_GEV, GRID_T_HI_GEV, GRID_POINTS_PER_DECADE)
        cls.Sigma_grid = np.array([cls.Sigma(T) for T in cls.grid])

        cls.peaks = {}
        for name, (lo, hi, _, _) in PEAKS.items():
            mask = (cls.grid >= lo) & (cls.grid <= hi)
            i = int(np.argmax(cls.Sigma_grid[mask]))
            cls.peaks[name] = (cls.Sigma_grid[mask][i], cls.grid[mask][i])

        cls.integral_grid = _grid_GeV(
            EE_INTEGRAL_T_LO_GEV, EE_INTEGRAL_T_HI_GEV, GRID_POINTS_PER_DECADE
        )
        cls.ee_integral = float(
            simpson(
                [cls.Sigma(T) for T in cls.integral_grid],
                x=np.log(cls.integral_grid),
            )
        )

        cls.witness = {
            T0: integrate_temperature_law(
                cls.eos, T0, TABLE_G_WITNESS_T1_GEV, with_rho=True
            ).rho_R_ratio
            for T0 in TABLE_G_WITNESS
        }

        if REPORT:
            cls._report()

    @classmethod
    def Sigma(cls, T_GeV: float) -> float:
        return 1.0 - 3.0 * float(cls.eos.w(T_GeV * cls.GeV))

    @classmethod
    def _report(cls):
        out = sys.stderr
        print("\n[test_kicking_function]", file=out)
        for name, (S, T) in cls.peaks.items():
            print(f"  peak {name:>11s}: Sigma = {S:.6f} at T = {_label(T)}", file=out)
        for T in list(EE_PROFILE) + [EE_TAIL_T_GEV]:
            print(f"  Sigma({_label(T)}) = {cls.Sigma(T):.6g}", file=out)
        print(f"  int Sigma d ln T, [10 keV, 3 MeV] = {cls.ee_integral:.6f}", file=out)
        for T0, r in cls.witness.items():
            print(f"  rho_R witness {_label(T0)} -> 10 keV: {r:.6f}", file=out)

    # Case 1
    def test_three_peaks(self):
        """
        The e+e-, QCD and electroweak peaks of 1 - 3 w, by argmax in
        [10 keV, 5 MeV], [50 MeV, 1 GeV] and [20 GeV, 1 TeV]: Sigma to 1e-3,
        T to 5 % (README §6.4; peak temperatures per the 2026-09-29 decision).
        """
        for name, (_, _, Sigma_expected, T_expected) in PEAKS.items():
            Sigma_peak, T_peak = self.peaks[name]
            with self.subTest(peak=name, quantity="Sigma"):
                self.assertAlmostEqual(
                    Sigma_peak,
                    Sigma_expected,
                    delta=PEAK_SIGMA_TOLERANCE,
                    msg=f"{name}: peak Sigma {Sigma_peak:.6f} at {_label(T_peak)},"
                    f" expected {Sigma_expected} +/- {PEAK_SIGMA_TOLERANCE:g}",
                )
            with self.subTest(peak=name, quantity="T"):
                self.assertLessEqual(
                    abs(T_peak / T_expected - 1.0),
                    PEAK_T_TOLERANCE,
                    msg=f"{name}: peak at {_label(T_peak)}, expected {_label(T_expected)}"
                    f" +/- {PEAK_T_TOLERANCE:.0%}",
                )

    # Case 2
    def test_ee_profile(self):
        """
        Sigma through e+e- annihilation at 2, 0.5, 0.2, 0.1 and 0.05 MeV to 1e-3,
        and below 1e-6 at 20 keV.
        """
        for T, expected in EE_PROFILE.items():
            with self.subTest(T=_label(T)):
                S = self.Sigma(T)
                self.assertAlmostEqual(
                    S,
                    expected,
                    delta=EE_PROFILE_TOLERANCE,
                    msg=f"Sigma({_label(T)}) = {S:.6f}, expected {expected} +/- {EE_PROFILE_TOLERANCE:g}",
                )
        with self.subTest(T=_label(EE_TAIL_T_GEV)):
            S = self.Sigma(EE_TAIL_T_GEV)
            self.assertLess(
                abs(S),
                EE_TAIL_BOUND,
                msg=f"Sigma({_label(EE_TAIL_T_GEV)}) = {S:.3e}, expected |Sigma| < {EE_TAIL_BOUND:g}",
            )

    # Case 3
    def test_ee_integral(self):
        """
        int Sigma d ln T over [10 keV, 3 MeV] = 0.1617 +/- 2e-3.
        """
        self.assertAlmostEqual(
            self.ee_integral,
            EE_INTEGRAL,
            delta=EE_INTEGRAL_TOLERANCE,
            msg=f"int Sigma d ln T = {self.ee_integral:.6f}, expected {EE_INTEGRAL} +/- {EE_INTEGRAL_TOLERANCE:g}",
        )

    # Case 4
    def test_w_is_one_third_outside_the_table(self):
        """
        At and below the table's lowest temperature (10 keV) and at and above
        its highest (25.1 TeV), w is exactly 1/3.
        """
        low = (self.eos._T_min,) + OUTSIDE_TABLE_LOW_T_GEV
        high = (self.eos._T_max,) + OUTSIDE_TABLE_HIGH_T_GEV
        for side, temps in (("below", low), ("above", high)):
            for T in temps:
                with self.subTest(side=side, T_GeV=f"{T:.6g}"):
                    self.assertEqual(float(self.eos.w(T * self.GeV)), 1.0 / 3.0)

    # Case 5
    def test_table_is_consistent_with_the_gs(self):
        """
        rho_R carried with d ln rho_R/dN = Sigma - 4 (Sigma from the table)
        against (pi^2/30) g_rho T^4 at 10 keV, from 5 MeV, 100 MeV and 2e4 GeV,
        each within 5e-3 of the value prompt 02 recorded.
        """
        for T0, expected in TABLE_G_WITNESS.items():
            ratio = self.witness[T0]
            label = f"{_label(T0)} -> 10 keV"
            with self.subTest(case=label):
                self.assertAlmostEqual(
                    ratio,
                    expected,
                    delta=TABLE_G_WITNESS_TOLERANCE,
                    msg=f"{label}: rho_R ratio {ratio:.6f}, expected {expected} +/- {TABLE_G_WITNESS_TOLERANCE:g}",
                )

    # Case 6
    def test_the_2_MeV_freeze_is_not_the_production_path(self):
        """
        QCD_Cosmology's EOS is an Xav_EOS_spline, whose tabulated w varies below
        2 MeV, while SaikawaShirai_EOS_spline.w freezes its argument at 2 MeV.
        The paper's numerical section describes the latter.
        """
        units = GeV_units()
        GeV = units.GeV
        cosmology = QCD_Cosmology(0, units, Planck2018())

        with self.subTest(check="QCD_Cosmology uses Xav_EOS_spline"):
            self.assertIsInstance(cosmology._eos, Xav_EOS_spline)
            self.assertEqual(cosmology.type_id, XAV_IMPROVED_EOS_IDENTIFIER)

        with self.subTest(check="the base spline class freezes w at 2 MeV"):
            base = SaikawaShirai_EOS_spline(units)
            w_below = float(base.w(BELOW_FREEZE_T_GEV * GeV))
            w_freeze = float(base.w(FREEZE_T_GEV * GeV))
            self.assertEqual(
                w_below,
                w_freeze,
                msg=f"SaikawaShirai_EOS_spline.w: 1 MeV {w_below!r}, 2 MeV {w_freeze!r}",
            )

        with self.subTest(check="the production w is not frozen"):
            w_below = float(cosmology.w(BELOW_FREEZE_T_GEV * GeV))
            w_freeze = float(cosmology.w(FREEZE_T_GEV * GeV))
            self.assertNotEqual(
                w_below,
                w_freeze,
                msg=f"QCD_Cosmology.w: 1 MeV {w_below!r}, 2 MeV {w_freeze!r}",
            )


if __name__ == "__main__":
    unittest.main()
