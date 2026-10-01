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
Synthetic new-physics densities for PRyMordial, and runners that set
PRyMordial's flags exactly as `compute_BBN_data` does, or with no new physics
at all.

Written for review-remediation prompt 03 (item R2). Since science-readiness
prompt 01 PRyMordial takes rho_NP alone: the new physics reaches it through the
expansion rate (`NP_hubble_flag`), and nothing reads a pressure or a density
derivative, so the families carry rho_NP only. It is also a measurement
script. Run it from the repository root, one case at a time, so that each case
can sit under an external `timeout`:

    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.prym_fixtures zero
    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.prym_fixtures reference
    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.prym_fixtures constant
    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.prym_fixtures oscillating

Every case runs one PRyMordial solve, about 10 s (full network). PRyMordial
reads `PRyMrates/` from the working directory, so nothing here works from
anywhere else.

The three families are README section 2 (d) and (f) of the review-remediation
campaign, and the geometry of `.documents/audit-2026-09-29/spline_test.py`:

- `ZERO`: rho_NP = 0.
- `CONSTANT`: rho_NP = 0.08 rho_SM(T).
- `OSCILLATING`: rho_NP = r(T) rho_SM(T), with
  r(T) = 0.08 + 0.3 sin(2 pi x) exp(-(x/1.5)^2), x = ln(T/0.3 MeV). dr/d ln T
  changes sign repeatedly in [0.02, 5] MeV.

Here rho_SM(T) = (pi^2/30) g_rho(T) T^4 with the Saikawa-Shirai g_rho (see
`g_rho`).

All temperatures are in MeV and all densities in MeV^4, as PRyMordial expects.
"""

import sys
import time
import warnings
from math import exp, log, pi, sin
from typing import Callable, NamedTuple

from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from Units import GeV_units

# indices into the array returned by PRyMclass.PRyMresults() (PRyM_main.py)
RES_NEFF = 0
RES_YP_BBN = 4  # the Yp that compute_BBN_data stores
RES_D_OVER_H_E5 = 5  # D/H x 1e5
RES_HE3_OVER_H_E5 = 6  # 3He/H x 1e5
RES_LI7_OVER_H_E10 = 7  # 7Li/H x 1e10

_EOS = None
_GeV = None


def g_rho(T_MeV: float) -> float:
    """
    The Saikawa-Shirai g_rho through the splined class the pipeline's EOS
    package provides, `SaikawaShirai_EOS_spline.G_rho`, with its clamps (the
    fit's own limit 3.383 below 10 keV since prompt 02). The class is built on
    first use, in GeV units.

    The user chose this over the raw fit `_raw_G_rho` (2026-09-29, option C;
    log 03). The two agree to 2.2e-10 on [10 keV, 10 MeV], but PRyMordial's
    output moves by ~1e-5 under perturbations that small, and this is the
    construction that reproduces README section 2 (f) row 2.
    """
    global _EOS, _GeV
    if _EOS is None:
        units = GeV_units()
        _EOS = SaikawaShirai_EOS_spline(units)
        _GeV = units.GeV
    return float(_EOS.G_rho(T_MeV / 1e3 * _GeV))


def rho_SM(T_MeV: float) -> float:
    """(pi^2/30) g_rho(T) T^4, in MeV^4."""
    return (pi * pi / 30.0) * g_rho(T_MeV) * T_MeV**4


def ratio_zero(T_MeV: float) -> float:
    return 0.0


def ratio_constant(T_MeV: float) -> float:
    return 0.08


def ratio_oscillating(T_MeV: float) -> float:
    x = log(T_MeV / 0.3)
    return 0.08 + 0.3 * sin(2.0 * pi * x) * exp(-((x / 1.5) ** 2))


class SyntheticNP(NamedTuple):
    label: str
    rho: Callable[[float], float]


def make_family(label: str, ratio: Callable[[float], float]) -> SyntheticNP:
    """
    rho_NP = ratio(T) rho_SM(T). PRyMordial has been seen to pass negative
    temperatures to the callbacks (the guard in BBNData.py says so); those get
    zero, as in compute_BBN_data.

    The product is written out, left to right, as
    ratio * (pi^2/30) * g_rho * T^4, rather than as ratio(T) * rho_SM(T). That
    is the floating-point order in which the constant family was measured
    when the user chose its reference (2026-09-29; review-remediation log 03).
    Re-associating the product changes rho_NP by about one ulp and moves
    PRyMordial's D/H by 1.0e-4 relative (that log), so the order is part of the
    fixture. Do not "simplify" it.

    Until science-readiness prompt 01 a family also carried p_NP = rho_NP/3 and
    a central-difference density derivative, for the NP_thermo_flag route.
    """

    def rho(T_MeV: float) -> float:
        if T_MeV <= 0.0:
            return 0.0
        return ratio(T_MeV) * (pi * pi / 30.0) * g_rho(T_MeV) * T_MeV**4

    return SyntheticNP(label=label, rho=rho)


def _zero(T_MeV: float) -> float:
    return 0.0


# rho_NP identically zero: an exact zero, not a family built from ratio_zero
ZERO = SyntheticNP(label="zero", rho=_zero)
CONSTANT = make_family("constant", ratio_constant)
OSCILLATING = make_family("oscillating", ratio_oscillating)

FAMILIES = {f.label: f for f in (ZERO, CONSTANT, OSCILLATING)}

_MISSING = object()


# every PRyM_init flag the runners set, and the PRyM_thermo callback PRyMclass
# installs; module globals that persist between solves
INIT_NAMES = (
    "NP_thermo_flag",
    "NP_hubble_flag",
    "verbose_flag",
    "smallnet_flag",
)
THERMO_NAMES = ("rho_NP",)


class SavedPRyMGlobals:
    """
    Save PRyM_init's INIT_NAMES and PRyM_thermo's THERMO_NAMES on entry and
    restore them on exit, whether or not the body raised. A name that did not
    exist on entry is deleted.
    """

    def __enter__(self):
        import PRyM.PRyM_init as PRyMini
        import PRyM.PRyM_thermo as PRyMthermo

        self._ini, self._thermo = PRyMini, PRyMthermo
        self._saved_init = {n: getattr(PRyMini, n, _MISSING) for n in INIT_NAMES}
        self._saved_thermo = {n: getattr(PRyMthermo, n) for n in THERMO_NAMES}
        return self

    def __exit__(self, *exc):
        for n, v in self._saved_init.items():
            if v is _MISSING:
                if hasattr(self._ini, n):
                    delattr(self._ini, n)
            else:
                setattr(self._ini, n, v)
        for n, v in self._saved_thermo.items():
            setattr(self._thermo, n, v)
        return False


def run_prym(
    rho: Callable[[float], float],
    small_network: bool = False,
    wall_clock_limit=None,
):
    """
    Run PRyMordial once on the rho_NP callback and return
    `PRyMclass(rho, wall_clock_limit=wall_clock_limit).PRyMresults()`, an array
    [N_eff, Omega_nu,rel h^2 x 1e6, 1/(Omega_nu,nr h^2 x 1e-6), Yp (CMB),
     Yp (BBN), D/H x 1e5, 3He/H x 1e5, 7Li/H x 1e10]; see the RES_* indices.

    The flags are set by `_configure_PRyMordial` (ComputeTargets/BBNData.py),
    the function `compute_BBN_data` and `compute_SM_baseline` use, so the
    fixture and the pipeline cannot drift apart. Since science-readiness
    prompt 01 that is the Hubble-only route: `NP_thermo_flag = False`,
    `NP_hubble_flag = True`, `verbose_flag = False`, `smallnet_flag =
    small_network`.

    `small_network = False` (the default, and production's) is PRyMordial's
    full reaction network; `True` is its 12-reaction network. Before
    production-readiness prompt 02 this function set `small_network_flag`,
    which PRyMordial never reads, so every solve it ran used the full network
    whatever the argument; every abundance pinned from it is a full-network
    value.

    PRyM_init's flags and PRyM_thermo's rho_NP are module globals that persist
    between runs, so every one this function touches is restored on exit,
    whether or not the solve raised.
    """
    from ComputeTargets.BBNData import _configure_PRyMordial

    with SavedPRyMGlobals():
        PRyMmain = _configure_PRyMordial(small_network)
        return PRyMmain.PRyMclass(rho, wall_clock_limit=wall_clock_limit).PRyMresults()


def run_prym_without_new_physics(small_network: bool = False):
    """
    PRyMordial with every new-physics flag off and no callback installed: the
    reference with no new physics at all. `NP_thermo_flag`, `NP_hubble_flag`,
    `NP_nu_flag` and `NP_e_flag` are False (the last two are PRyM_init's
    defaults and are checked, not set), `verbose_flag = False` and
    `smallnet_flag = small_network`. Every global touched is restored.
    (science-readiness prompt 01)
    """
    import PRyM.PRyM_init as PRyMini
    import PRyM.PRyM_main as PRyMmain

    with SavedPRyMGlobals():
        PRyMini.NP_thermo_flag = False
        PRyMini.NP_hubble_flag = False
        PRyMini.verbose_flag = False
        PRyMini.smallnet_flag = small_network
        if PRyMini.NP_nu_flag or PRyMini.NP_e_flag:
            raise AssertionError("PRyM_init.NP_nu_flag and NP_e_flag must be False")
        return PRyMmain.PRyMclass().PRyMresults()


def _main(argv) -> int:
    """
    Driver: one case per invocation, printing wall-clock, the RuntimeWarning
    count and the abundances. `reference` is PRyMordial with no new physics at
    all (`run_prym_without_new_physics`); the families go through `run_prym`.
    """
    if len(argv) != 2 or argv[1] not in (*FAMILIES, "reference"):
        print(f"usage: {argv[0]} {{{','.join((*FAMILIES, 'reference'))}}}")
        return 2

    case = argv[1]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        start = time.perf_counter()
        if case == "reference":
            res = run_prym_without_new_physics()
        else:
            res = run_prym(FAMILIES[case].rho)
        wall = time.perf_counter() - start

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    sites = sorted(
        {f"{w.filename.split('/')[-1]}:{w.lineno}" for w in runtime_warnings}
    )
    print(f"case={case}")
    print(f"wall={wall:.2f} s")
    print(f"RuntimeWarnings={len(runtime_warnings)} at {sites}")
    print(
        f"Neff={res[RES_NEFF]:.6g} Yp={res[RES_YP_BBN]:.10g} "
        f"DoH_e5={res[RES_D_OVER_H_E5]:.10g} He3oH_e5={res[RES_HE3_OVER_H_E5]:.6g} "
        f"Li7oH_e10={res[RES_LI7_OVER_H_E10]:.6g}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
