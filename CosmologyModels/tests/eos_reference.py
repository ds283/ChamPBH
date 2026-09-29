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
Reference measurements for the Jordan-frame temperature law.

Pure functions, no unittest, so that tests and campaign scripts can share them.
Written for review-remediation prompt 01. The geometry and the numbers are those
of `.documents/audit-2026-09-29/tlaw_check.py` and `low_t_join_probe.py`.

Every temperature argument here is a plain float in GeV. The EOS is called with
T * units.GeV in whatever units it was built with.

The three witnesses:

1. `exact_efolds` -- exact entropy conservation, T a g_s^{1/3} = const. It reads
   `G_s` only.
2. `derivative_convention_ratio` -- the shipped `dG_s_dlogT` against a central
   difference of `G_s` in ln T.
3. `integrate_temperature_law(..., with_rho=True)` against `thermodynamic_rho_R`
   -- the radiation density carried with d ln rho_R/dN = Sigma - 4, Sigma from
   the production class's w(T), against (pi^2/30) g_rho(T) T^4.

`integrate_temperature_law` is the thing being measured. It is the only function
here that calls `dG_s_dlogT` in the ODE. No reference calls it.
"""

from math import exp, log
from typing import NamedTuple, Optional

import numpy as np
from scipy.integrate import solve_ivp

from constants import RadiationConstant
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

# the CMB temperature today, as in CosmologyModels/LambdaCDM/Planck.py and tlaw_check.py
T_CMB_KELVIN = 2.7255

# the starting temperature of the audit's geometry, and the default --T-init-GeV
T_INIT_GEV = 2.0e4

# the upper limit of the N integration; the event at ln T1 terminates it long before
_N_MAX = 200.0


def _GeV(eos) -> float:
    return eos._units.GeV


def T_CMB_GeV(eos) -> float:
    """
    T_CMB = 2.7255 K expressed in GeV, in the units the EOS was built with.
    """
    units = eos._units
    return T_CMB_KELVIN * units.Kelvin / units.GeV


def production_eos(units=None) -> Xav_EOS_spline:
    """
    The EOS class the pipeline uses (`QCD_Cosmology.py`), built in GeV units
    unless told otherwise. It reads its CSV by a relative path, so the caller
    must be at the repository root. It takes about a second to build, so a test
    class should build it once in setUpClass.
    """
    if units is None:
        units = GeV_units()
    return Xav_EOS_spline(units)


def exact_efolds(eos, T0_GeV: float, T1_GeV: float) -> float:
    """
    Witness 1: e-folds from T0 to T1 at fixed field under exact entropy
    conservation, T a g_s^{1/3} = const, so that
        N = ln(T0/T1) + (1/3) ln[g_s(T0)/g_s(T1)].
    It reads `eos.G_s` at the two endpoints and nothing else. It does not call
    `dG_s_dlogT`, which is one of the things being measured.
    """
    GeV = _GeV(eos)
    G_s0 = float(eos.G_s(T0_GeV * GeV))
    G_s1 = float(eos.G_s(T1_GeV * GeV))
    return log(T0_GeV / T1_GeV) + log(G_s0 / G_s1) / 3.0


def thermodynamic_rho_R(eos, T_GeV: float) -> float:
    """
    The thermodynamic radiation density (pi^2/30) g_rho(T) T^4, in GeV^4.
    Reads `eos.G_rho` only. It is the reference for witness 3.
    """
    GeV = _GeV(eos)
    return RadiationConstant * float(eos.G_rho(T_GeV * GeV)) * T_GeV**4


class TemperatureLawResult(NamedTuple):
    # e-folds elapsed when ln T reached ln T1 (the event, not the last solver sample)
    efolds: float

    # the integrated radiation density at T1, in GeV^4; None unless with_rho=True
    rho_R: Optional[float]

    # the integrated rho_R divided by thermodynamic_rho_R(eos, T1); None unless with_rho=True
    rho_R_ratio: Optional[float]


def integrate_temperature_law(
    eos,
    T0_GeV: float,
    T1_GeV: float,
    kappa: float = 1.0,
    with_rho: bool = False,
    rtol: float = 1e-10,
    atol: float = 1e-12,
    max_step: float = 0.05,
) -> TemperatureLawResult:
    """
    Integrate the Jordan-frame temperature law exactly as `ODERHS.__call__` writes
    it (`ComputeTargets/ScalarModel.py`), at fixed field (A' phi' = 0):
        d ln T/dN = -1 / (1 + kappa * dG_s_dlogT / G_s / 3)
    from T0 until ln T = ln T1, terminated by an event there.

    This is the quantity under test, not a reference. `kappa` multiplies
    `dG_s_dlogT`: kappa = 1 scores the convention as shipped, and
    kappa = 1/ln 10 converts a d/d log10 T derivative to d/d ln T. It lets the
    corrected convention be scored before the EOS class is changed.

    With `with_rho=True`, ln rho_R is carried alongside with
        d ln rho_R/dN = Sigma - 4,   Sigma = 1 - 3 w(T),
    which is the RHS's `log_rhorad_Einstein` equation at fixed field. It starts
    from thermodynamic_rho_R(eos, T0).
    """
    GeV = _GeV(eos)

    def rhs(N, y):
        T = exp(y[0]) * GeV
        G_s = float(eos.G_s(T))
        dG_s_dlogT = float(eos.dG_s_dlogT(T))
        d_log_T = -1.0 / (1.0 + kappa * dG_s_dlogT / G_s / 3.0)
        if not with_rho:
            return [d_log_T]
        Sigma = 1.0 - 3.0 * float(eos.w(T))
        return [d_log_T, Sigma - 4.0]

    log_T1 = log(T1_GeV)

    def reached_T1(N, y):
        return y[0] - log_T1

    reached_T1.terminal = True
    reached_T1.direction = -1

    y0 = [log(T0_GeV)]
    if with_rho:
        y0.append(log(thermodynamic_rho_R(eos, T0_GeV)))

    sol = solve_ivp(
        rhs,
        [0.0, _N_MAX],
        y0,
        events=reached_T1,
        rtol=rtol,
        atol=atol,
        max_step=max_step,
    )
    if sol.status != 1 or len(sol.t_events[0]) != 1:
        raise RuntimeError(
            f"integrate_temperature_law: did not reach T1 = {T1_GeV:.5g} GeV from T0 = {T0_GeV:.5g} GeV "
            f"(status {sol.status}: {sol.message})"
        )

    N = float(sol.t_events[0][0])
    if not with_rho:
        return TemperatureLawResult(efolds=N, rho_R=None, rho_R_ratio=None)

    rho_R = exp(float(sol.y_events[0][0][1]))
    return TemperatureLawResult(
        efolds=N,
        rho_R=rho_R,
        rho_R_ratio=rho_R / thermodynamic_rho_R(eos, T1_GeV),
    )


def derivative_convention_ratio(eos, T_GeV: float, h: float = 1e-4) -> float:
    """
    Witness 2: eos.dG_s_dlogT(T) divided by the central difference of `eos.G_s`
    in ln T with half-step h,
        [G_s(T e^h) - G_s(T e^-h)] / (2h).
    The reference is built from `G_s` alone. The ratio is 1 if dG_s_dlogT is a
    natural-log derivative and ln 10 if it is a log10 one.
    """
    GeV = _GeV(eos)
    T = T_GeV * GeV
    central = (float(eos.G_s(T * exp(h))) - float(eos.G_s(T * exp(-h)))) / (2.0 * h)
    return float(eos.dG_s_dlogT(T)) / central


# The Saikawa-Shirai branch boundaries inside the test range: G_s and G_rho switch
# fits at 120 MeV, where the raw G_s is discontinuous (19.1000 just below, 19.0929
# at and above). The spline is fitted across the jump and rings nearby. The clamps sit at 10 keV and 1e16 GeV, and
# Xav's table ends at 25 TeV.
DERIVATIVE_GRID_POINTS = 60
DERIVATIVE_GRID_T_LO_GEV = 2.0e-5
DERIVATIVE_GRID_T_HI_GEV = 5.0e3
DERIVATIVE_GRID_JOIN_GEV = 0.12
DERIVATIVE_GRID_JOIN_CLEARANCE = 1.5


def derivative_test_grid_GeV() -> np.ndarray:
    """
    The 60-point grid for the derivative-convention tests. It lies in
    [20 keV, 5 TeV], a factor 2 above the low clamp at 10 keV and a factor 5
    below the top of Xav's table at 25 TeV. It is log-spaced in two segments,
    [20 keV, 80 MeV] and [180 MeV, 5 TeV], so that every point is at least a
    factor 1.5 in T from the 120 MeV branch join. The points are split between
    the segments in proportion to their length in log T (27 and 33), which
    makes the spacing nearly the same in both (0.1385 and 0.1389 decades).
    """
    lo = log(DERIVATIVE_GRID_T_LO_GEV)
    hi = log(DERIVATIVE_GRID_T_HI_GEV)
    join_lo = log(DERIVATIVE_GRID_JOIN_GEV / DERIVATIVE_GRID_JOIN_CLEARANCE)
    join_hi = log(DERIVATIVE_GRID_JOIN_GEV * DERIVATIVE_GRID_JOIN_CLEARANCE)

    n_lo = int(
        round(
            DERIVATIVE_GRID_POINTS * (join_lo - lo) / ((join_lo - lo) + (hi - join_hi))
        )
    )
    n_hi = DERIVATIVE_GRID_POINTS - n_lo

    return np.concatenate(
        (
            np.exp(np.linspace(lo, join_lo, n_lo)),
            np.exp(np.linspace(join_hi, hi, n_hi)),
        )
    )
