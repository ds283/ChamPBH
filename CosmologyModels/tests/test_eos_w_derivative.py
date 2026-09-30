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
`dw_dlogT` = d w / d ln T on every EOS class, against a central difference of that
class's own `w` in ln T with half-step h = 1e-4.

Written for production-readiness prompt 03 (README §6.3, rows 1-3). The adiabatic
mass needs Sigma_T = d Sigma / d ln T = -3 dw_dlogT; production code never
finite-differences w, so the central difference lives only here.

The grid: 1010 points log-spaced over [12 keV, 20 TeV], then

- **clamps.** A grid point is dropped for a class if it lies within two half-steps
  (|ln T - ln T_c| <= 2h) of a temperature T_c where that class's w, or its
  derivative, is not smooth, so that no central-difference stencil straddles one:
  the 2 MeV freeze of the Saikawa-Shirai classes, and for the jax class also the
  120 MeV branch join, where its raw fits (and so its w) jump. Xav's table ends
  (10 keV, 25.1 TeV) and the g clamps (10 keV, 1e16 GeV) lie outside the grid, a
  factor >= 1.2 away. At least 1000 points remain for every class.
- **the 120 MeV join of the spline class.** SaikawaShirai_EOS_spline's g splines
  are fitted across the raw fits' jump at 120 MeV and ring there. The spline is
  smooth, but within a factor 1.03 of 120 MeV its third derivative is large enough
  that the h = 1e-4 central difference itself is in error by up to 1.12e-6
  (measured on a 200001-point grid; it scales as h^2, 1.12e-8 at h = 1e-5). At the
  grid points in that window the witness uses h = 1e-5 instead. The h = 1e-4 value
  there is reported, not asserted (log 03, Deviations).

Run from the repository root; Xav_EOS_spline reads its CSV by a relative path.
No Ray cluster, no datastore, no PRyMordial. The spline classes take about two
seconds. **The jax case takes about 100 s**, because each jax evaluation of w and
of its gradient is dispatched op by op; it is skipped if jax does not import.

Set CHAMPBH_TEST_REPORT=1 to print the measured values as well.
"""

import importlib.util
import os
import sys
import unittest
from math import exp, log

import numpy as np

from CosmologyModels.GenericEOS.GenericEOS import GenericEOSBase
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

HALF_STEP = 1e-4
FINE_HALF_STEP = 1e-5
TOLERANCE = 1e-6

GRID_POINTS = 1010
GRID_T_LO_GEV = 1.2e-5
GRID_T_HI_GEV = 2.0e4
MIN_POINTS = 1000

FREEZE_T_GEV = 2e-3
JOIN_T_GEV = 0.12
JOIN_WINDOW_FACTOR = 1.03

_REPORT = os.environ.get("CHAMPBH_TEST_REPORT", "") not in ("", "0")

_HAS_JAX = importlib.util.find_spec("jax") is not None


def _report(msg: str):
    if _REPORT:
        print(msg, file=sys.stderr)


def _grid_GeV(exclude=()) -> np.ndarray:
    Ts = np.exp(np.linspace(log(GRID_T_LO_GEV), log(GRID_T_HI_GEV), GRID_POINTS))
    keep = np.ones(len(Ts), dtype=bool)
    for T_c in exclude:
        keep &= np.abs(np.log(Ts / T_c)) > 2.0 * HALF_STEP
    return Ts[keep]


def _central_dw(w, GeV: float, T_GeV: float, h: float) -> float:
    return (float(w(T_GeV * exp(h) * GeV)) - float(w(T_GeV * exp(-h) * GeV))) / (
        2.0 * h
    )


def _in_join_window(T_GeV: float) -> bool:
    return abs(log(T_GeV / JOIN_T_GEV)) <= log(JOIN_WINDOW_FACTOR)


class TestEOSwDerivative(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.units = GeV_units()
        cls.GeV = cls.units.GeV
        cls.xav = Xav_EOS_spline(cls.units)
        cls.spline = SaikawaShirai_EOS_spline(cls.units)

    def _score(self, label, dw, w, Ts, join_window=False):
        """
        max |dw(T) - central difference of w| over Ts. With join_window, points inside
        the 120 MeV window use the h = 1e-5 witness (module docstring).
        """
        self.assertGreaterEqual(len(Ts), MIN_POINTS, msg=f"{label}: too few points")
        worst, worst_T = 0.0, None
        worst_outside = 0.0
        worst_window_coarse = 0.0
        n_window = 0
        for T in Ts:
            h = HALF_STEP
            if join_window and _in_join_window(T):
                h = FINE_HALF_STEP
                n_window += 1
                worst_window_coarse = max(
                    worst_window_coarse,
                    abs(dw(T * self.GeV) - _central_dw(w, self.GeV, T, HALF_STEP)),
                )
            d = abs(dw(T * self.GeV) - _central_dw(w, self.GeV, T, h))
            if h == HALF_STEP:
                worst_outside = max(worst_outside, d)
            if d > worst:
                worst, worst_T = d, T
        _report(
            f"[test_eos_w_derivative] {label}: {len(Ts)} points, max |dw_dlogT - central| = {worst:.3e} at {worst_T:.5g} GeV"
            + (
                f"; {len(Ts) - n_window} points at h = 1e-4: max {worst_outside:.3e}; {n_window} points in the 120 MeV window at h = 1e-5,"
                f" where h = 1e-4 gives {worst_window_coarse:.3e} (reported, not asserted)"
                if join_window
                else ""
            )
        )
        self.assertLessEqual(
            worst,
            TOLERANCE,
            msg=f"{label}: max |dw_dlogT - central difference| = {worst:.3e} at {worst_T:.5g} GeV",
        )

    def test_xav_spline(self):
        """
        The production class: the analytic derivative of its ln T spline, over the
        whole grid (probe: 1.0e-7 at 150 MeV).
        """
        self._score("Xav_EOS_spline", self.xav.dw_dlogT, self.xav.w, _grid_GeV())

    def test_xav_spline_is_zero_beyond_the_table(self):
        """
        w returns 1/3 without the spline at and beyond the table's ends, so dw_dlogT is
        exactly 0 there.
        """
        for T in [self.xav._T_min, 0.5 * self.xav._T_min, self.xav._T_max, 2e5]:
            self.assertEqual(self.xav.dw_dlogT(T * self.GeV), 0.0, msg=f"T = {T} GeV")

    def test_saikawa_shirai_spline(self):
        """
        Above 2 MeV, the base-class formula from dG_s_dlogT and dG_rho_dlogT; at and
        below 2 MeV exactly 0, where w freezes its argument.
        """
        Ts = _grid_GeV(exclude=(FREEZE_T_GEV,))
        above = Ts[Ts > FREEZE_T_GEV]
        at_or_below = Ts[Ts <= FREEZE_T_GEV]

        self._score(
            "SaikawaShirai_EOS_spline, full grid",
            self.spline.dw_dlogT,
            self.spline.w,
            Ts,
            join_window=True,
        )
        _report(
            f"[test_eos_w_derivative] SaikawaShirai_EOS_spline: {len(above)} points above 2 MeV, {len(at_or_below)} at or below"
        )

        for T in list(at_or_below) + [FREEZE_T_GEV]:
            self.assertEqual(
                self.spline.dw_dlogT(T * self.GeV), 0.0, msg=f"T = {T} GeV"
            )

    def test_base_class_formula(self):
        """
        GenericEOSBase.dw_dlogT, which has no freeze, against GenericEOSBase.w, both on
        the spline class's g's. Asserted over the whole grid, which includes the range
        above 2 MeV that README §6.3 names.
        """

        def w(T):
            return GenericEOSBase.w(self.spline, T)

        def dw(T):
            return GenericEOSBase.dw_dlogT(self.spline, T)

        self._score("GenericEOSBase formula", dw, w, _grid_GeV(), join_window=True)

    @unittest.skipUnless(_HAS_JAX, "jax is not importable")
    def test_jax_autodiff(self):
        """
        The jax class: autodiff of its own w above 2 MeV, exactly 0 at and below it.
        About 100 s (module docstring). Points within 2h of 2 MeV and of the 120 MeV
        branch join, where this class's w jumps, are dropped.
        """
        from CosmologyModels.GenericEOS.SaikawaShirai_EOS_jax_autodiff import (
            SaikawaShirai_EOS_jax_autodiff,
        )

        eos = SaikawaShirai_EOS_jax_autodiff(self.units)
        Ts = _grid_GeV(exclude=(FREEZE_T_GEV, JOIN_T_GEV))
        self._score("SaikawaShirai_EOS_jax_autodiff", eos.dw_dlogT, eos.w, Ts)

        for T in list(Ts[Ts <= FREEZE_T_GEV]) + [FREEZE_T_GEV]:
            self.assertEqual(eos.dw_dlogT(T * self.GeV), 0.0, msg=f"T = {T} GeV")

    def test_cosmology_forwards(self):
        """
        LambdaCDM_GenericEOS forwards dw_dlogT to its EOS.
        """
        from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
        from CosmologyModels.LambdaCDM import Planck2018

        cosmology = QCD_Cosmology(0, self.units, Planck2018())
        for T in [1e-4, 0.15, 53.0]:
            self.assertEqual(
                cosmology.dw_dlogT(T * self.GeV),
                cosmology._eos.dw_dlogT(T * self.GeV),
            )


if __name__ == "__main__":
    unittest.main()
