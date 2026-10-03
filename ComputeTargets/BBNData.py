from collections import namedtuple
from math import exp, isfinite, log
from typing import Optional, List, Any, Callable, Sequence

import numpy as np
import ray
from scipy.interpolate import make_interp_spline

from CosmologyConcepts import redshift, redshift_array
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore import DatastoreObject
from MetadataConcepts import store_tag
from Units.base import UnitsLike
from config.defaults import DEFAULT_STRING_LENGTH
from config.sharding import ShardKeyType
from constants import RadiationConstant
from utilities import WallclockTimer, energy_formatter
from .ScalarModel import ScalarModelProxy, ScalarModel, ScalarModelValue
from .exceptions import ComputationFailureError

SampleValues = namedtuple(
    "SampleValues",
    [
        "raw_N",
        "log_T_Jordan",
        "density_NP",
        "density_NP_ratio",
    ],
)


# The PRyMordial that produced a row: the pinned upstream hash, plus a suffix
# naming the ChamPBH patches applied to the vendored copy. "ri02" is
# run-integrity prompt 02: every solve_ivp result is checked, and a solve that
# did not succeed raises PRyMSolverFailureError (PRyM/PRyM_main.py). "sr01" is
# science-readiness prompt 01: NP_hubble_flag (PRyM/PRyM_init.py) adds rho_NP
# to the expansion rate in Hubble() and nowhere else, and PRyMclass takes an
# optional wall_clock_limit, past which it raises PRyMWallClockLimitError
# (PRyM/PRyM_main.py). The "cham03" patch (review-remediation prompt 03, an
# inert dTNPdt) was reverted by science-readiness prompt 01: with
# PRyMordial's thermodynamic NP flag off, dTNPdt is never called. "bt02"
# (2026-10-03) is bbn-tolerance prompt 02: the low-T nuclear network's
# solve_ivp call, which upstream gives no rtol (so SciPy's 1e-3 applied), runs
# at rtol 1e-6 on the small network and 1e-5 on the full one, atol unchanged
# (PRyM/PRyM_main.py; bbn-tolerance logs 01c and 01).
PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"

# The wall-clock limit on a production PRyMordial solve, in seconds
# (science-readiness README section 0.2, P3). An unloaded full-network solve
# takes about 10 s. main.py's --bbn-wall-clock-limit defaults to this value.
DEFAULT_BBN_WALL_CLOCK_LIMIT = 600.0

# The fewest samples the cubic ratio spline can be built on
# (science-readiness prompt 01; run-integrity board,
# [02-a-short-bbn-sample-grid-escapes-compute-bbn-data])
MIN_BBN_SAMPLES = 4

# The abundances _run_PRyMordial returns, and the range each must lie in for
# the result to be stored as a success (science-readiness prompt 01, item O).
# These classify our own failures; they are not accuracy bounds on PRyMordial.
YP_UPPER_BOUND = 0.5
ABUNDANCE_NAMES = ("Yp_BBN", "DOverH", "He3OverH", "Li7OverH")


def _failure_payload(reason: str) -> dict:
    """
    The value compute_BBN_data returns from every failure path. The reason is
    truncated to DEFAULT_STRING_LENGTH, the width of the BBNData.failure_reason
    column. (review-remediation prompt 03)
    """
    return {"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}


def thermodynamic_rho_SM(eos, units: UnitsLike) -> Callable[[float], float]:
    """
    The Standard-Model radiation density rho_SM(T) = (pi^2/30) g_rho(T) T^4 in
    PRyMordial's units: it takes T in MeV and returns MeV^4. `eos` is anything
    with `G_rho` taking a dimensionful temperature in `units` (a
    LambdaCDM_GenericEOS cosmology or an EOS class). Nothing is splined here.
    (review-remediation prompt 04. Until science-readiness prompt 01 it also
    returned d rho_SM / dT, for the density-derivative callback that prompt
    removed.)
    """
    MeV = units.MeV

    def rho_SM_MeV4(T_in_MeV: float) -> float:
        g = float(eos.G_rho(T_in_MeV * MeV))
        return RadiationConstant * g * T_in_MeV**4

    return rho_SM_MeV4


def build_rho_NP_callback(
    log_T_MeV: Sequence[float],
    density_ratio: Sequence[float],
    rho_SM_MeV4: Callable[[float], float],
    T_min_MeV: float,
    T_max_MeV: float,
    task_label: str,
) -> Callable[[float], float]:
    """
    Build PRyMordial's rho_NP callback from samples of the ratio
    r = rho_NP / rho_R,J against ln(T_J / MeV). The callback takes T in MeV and
    returns rho_NP(T) = r(T) rho_SM(T) in MeV^4.

    The arrays are in the order the solver produced them, so `log_T_MeV` must be
    strictly decreasing. If it is not, ComputationFailureError is raised naming
    the first offending pair; nothing is sorted. The ratio is splined (cubic,
    no transform) on the reversed arrays.

    Checks, each raising ComputationFailureError:
    - fewer than MIN_BBN_SAMPLES (4) samples, naming the count
      (science-readiness prompt 01; before it, IndexError or ValueError escaped
      compute_BBN_data with no failure row);
    - a non-finite sample of log_T_MeV or density_ratio, naming the array, the
      first index and its T (run-integrity prompt 02);
    - log_T_MeV not strictly decreasing (review-remediation prompt 04);
    - in the callback: a non-finite T (run-integrity prompt 02); T above
      T_max_MeV or below T_min_MeV; an OverflowError or ValueError while
      evaluating; and a non-finite value returned for a finite T in the domain,
      for example from a non-finite g_rho (science-readiness prompt 01; before
      it, the NaN was handed to PRyMordial).
    A negative T returns 0 (PRyMordial sometimes produces one). For finite
    input in the domain no value differs from the rho_NP callback that
    build_NP_callbacks returned before science-readiness prompt 01.

    Until science-readiness prompt 01 this was build_NP_callbacks, which also
    built the pressure and density-derivative callbacks of PRyMordial's
    thermodynamic new-physics route. The Hubble-only route reads rho_NP alone.
    """
    log_T = np.asarray(log_T_MeV, dtype=float)
    r = np.asarray(density_ratio, dtype=float)

    # a cubic spline needs four samples; with fewer, make_interp_spline raises
    # ValueError and an empty grid raises IndexError, neither of which
    # compute_BBN_data catches (science-readiness prompt 01)
    if len(log_T) < MIN_BBN_SAMPLES:
        raise ComputationFailureError(
            f"too few samples for the rho_NP spline: {len(log_T)} in the window, "
            f"at least {MIN_BBN_SAMPLES} needed [{task_label}]"
        )

    # refuse non-finite samples before the monotonicity check, which a NaN in
    # log_T_MeV would otherwise fail with a misleading message
    # (run-integrity prompt 02)
    for name, samples in (
        ("log_T_MeV", log_T),
        ("density_ratio", r),
    ):
        bad = np.flatnonzero(~np.isfinite(samples))
        if len(bad) > 0:
            i = int(bad[0])
            T_at_i = exp(log_T[i]) if i < len(log_T) else float("nan")
            raise ComputationFailureError(
                f"{name} is not finite: index {i} has {name}={float(samples[i])} "
                f"at T={T_at_i:.6g} MeV [{task_label}]"
            )

    for i in range(len(log_T) - 1):
        if not log_T[i + 1] < log_T[i]:
            raise ComputationFailureError(
                f"T_Jordan is not strictly decreasing: sample {i} has "
                f"log(T/MeV)={log_T[i]:.10g} (T={exp(log_T[i]):.6g} MeV) and sample {i + 1} has "
                f"log(T/MeV)={log_T[i + 1]:.10g} (T={exp(log_T[i + 1]):.6g} MeV) [{task_label}]"
            )

    # the spline wants increasing abscissae: reverse, do not sort
    x = log_T[::-1]
    density_ratio_spline = make_interp_spline(x, r[::-1], k=3)

    def _wrap(e: Exception, kind: str, T_in_MeV: float) -> ComputationFailureError:
        msg = f"!! compute_BBN_data {task_label}: {kind} error at T = {T_in_MeV:.5g} MeV: {e}"
        print(msg)
        return ComputationFailureError(msg)

    def rho_NP(T_in_MeV: float) -> float:
        # a NaN T passes both the negative-T guard and the domain guard
        # (run-integrity prompt 02)
        if not isfinite(T_in_MeV):
            raise ComputationFailureError(
                f"rho_NP was called with a non-finite T_in_MeV={T_in_MeV!r} [{task_label}]"
            )

        # PRyMordial sometimes produces negative temperatures
        if T_in_MeV < 0:
            return 0.0

        if T_in_MeV > T_max_MeV:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is larger than T_BBN_spline_max={T_max_MeV:.5g} MeV"
            )

        if T_in_MeV < T_min_MeV:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is smaller than T_BBN_spline_min={T_min_MeV:.5g} MeV"
            )

        log_T_in_MeV = log(T_in_MeV)
        try:
            ratio = float(density_ratio_spline(log_T_in_MeV))
            rho_SM = rho_SM_MeV4(T_in_MeV)
            value = ratio * rho_SM
        except OverflowError as e:
            raise _wrap(e, "overflow", T_in_MeV) from e
        except ValueError as e:
            raise _wrap(e, "value", T_in_MeV) from e

        # a non-finite value for a finite T in the domain would be handed to
        # PRyMordial, which can hang on one (science-readiness prompt 01;
        # run-integrity board, [02-the-bbn-callbacks-do-not-check-their-values-for-finiteness])
        if not isfinite(value):
            raise ComputationFailureError(
                f"rho_NP is not finite at T={T_in_MeV:.6g} MeV: ratio={ratio!r}, "
                f"rho_SM={rho_SM!r} [{task_label}]"
            )

        return value

    return rho_NP


def _configure_PRyMordial(small_network: bool):
    """
    Set the PRyMordial flags that compute_BBN_data and compute_SM_baseline both
    use, check the ones this route relies on, and return the PRyM_main module.
    (Factored out in review-remediation prompt 04 so that the baseline goes
    through exactly the same settings.)

    Since science-readiness prompt 01 the new physics reaches PRyMordial through
    the expansion rate alone: NP_hubble_flag adds rho_NP(T_gamma) to H in
    Hubble(), and the thermodynamic NP flag is off, so the plasma obeys the
    Standard-Model dT_gamma/dt, the neutrinos theirs, and no NP temperature is
    integrated.
    The route also needs NP_nu_flag and NP_e_flag off (each would put rho_NP
    into the thermodynamics a second way), julia_flag off (the Julia branches
    carry no wall-clock limit), and compute_bckg_flag on (a cached background,
    thermo/Tgamma_Tnu.txt, would silently ignore rho_NP). Those are checked,
    and an AssertionError is raised if one is not as required.
    """
    # import locally so that global variable in PRyMini don't leak between threads
    # (not sure if this is possible or not, but worth being defensive)
    import PRyM.PRyM_init as PRyMini
    import PRyM.PRyM_main as PRyMmain

    # the new physics enters the Hubble rate only (science-readiness prompt 01).
    # Until then this set the thermodynamic NP flag and the NP start temperature.
    PRyMini.NP_thermo_flag = False
    PRyMini.NP_hubble_flag = True

    # disable verbose output
    PRyMini.verbose_flag = False

    # select the reaction network: True restricts PRyMordial to its 12-reaction network, which is
    # faster but unreliable for Li7; False (production) runs the full network. PRyMordial reads
    # smallnet_flag when the solve runs. Until production-readiness prompt 02 this set
    # small_network_flag, which PRyMordial never reads, so every solve ran the full network.
    PRyMini.smallnet_flag = small_network

    # the flags the Hubble-only route relies on (science-readiness prompt 01). An
    # explicit raise rather than `assert`, so that the check survives python -O.
    for name, required in (
        ("NP_thermo_flag", False),
        ("NP_hubble_flag", True),
        ("NP_nu_flag", False),
        ("NP_e_flag", False),
        ("julia_flag", False),
        ("compute_bckg_flag", True),
    ):
        value = getattr(PRyMini, name)
        if value is not required:
            raise AssertionError(
                f"_configure_PRyMordial: PRyM_init.{name} is {value!r}; the Hubble-only "
                f"BBN route requires {required!r}"
            )

    return PRyMmain


def _zero_NP(T_in_MeV: float) -> float:
    return 0.0


def _check_abundances(abundances: dict) -> Optional[str]:
    """
    The output checks (science-readiness prompt 01, item O). Return None if all
    four abundances are finite, 0 < Yp_BBN < 0.5, and DOverH, He3OverH and
    Li7OverH are positive; otherwise a reason naming each value that fails.
    These classify our own failures, not PRyMordial's accuracy.
    """
    problems = []
    for name in ABUNDANCE_NAMES:
        value = float(abundances[name])
        if not isfinite(value):
            problems.append(f"{name}={value} is not finite")
        elif name == "Yp_BBN":
            if not 0.0 < value < YP_UPPER_BOUND:
                problems.append(
                    f"{name}={value:.10g} is outside (0, {YP_UPPER_BOUND:g})"
                )
        elif not value > 0.0:
            problems.append(f"{name}={value:.10g} is not positive")

    if len(problems) == 0:
        return None
    return "; ".join(problems)


def _run_PRyMordial(
    rho_NP: Callable[[float], float],
    small_network: bool,
    wall_clock_limit: Optional[float],
) -> dict:
    """
    The PRyMordial boundary (run-integrity prompt 02). Configure PRyMordial
    with `_configure_PRyMordial(small_network)`, run it on the rho_NP callback
    with the given wall-clock limit (seconds; None for no limit), and return
    {"Yp_BBN", "DOverH", "He3OverH", "Li7OverH"} (D/H and 3He/H x 1e5,
    7Li/H x 1e10).

    Any Exception raised inside the PRyMordial call is returned as
    `_failure_payload("PRyMordial: <Type>: <message>")`. That covers
    PRyMSolverFailureError (a solve_ivp that did not succeed),
    PRyMWallClockLimitError (the limit passed; science-readiness prompt 01), a
    ComputationFailureError raised by the callback, which PRyMordial calls,
    and anything else PRyMordial or the callback raise. It does not cover a
    BaseException such as KeyboardInterrupt, nor anything raised outside the
    call: an exception from ChamPBH's own code outside PRyMordial is a bug and
    propagates. compute_SM_baseline does not use this helper.

    A successful return is then checked by `_check_abundances`; one outside
    the checks is returned as `_failure_payload("PRyMordial output: ...")`
    (science-readiness prompt 01).
    """
    PRyMmain = _configure_PRyMordial(small_network)

    try:
        res = PRyMmain.PRyMclass(
            rho_NP, wall_clock_limit=wall_clock_limit
        ).PRyMresults()
    except Exception as e:
        return _failure_payload(f"PRyMordial: {type(e).__name__}: {e}")

    abundances = {
        "Yp_BBN": res[4],
        "DOverH": res[5],
        "He3OverH": res[6],
        "Li7OverH": res[7],
    }

    problems = _check_abundances(abundances)
    if problems is not None:
        return _failure_payload(f"PRyMordial output: {problems}")

    return abundances


def compute_SM_baseline(small_network: bool) -> dict:
    """
    The Standard-Model abundances through the same PRyMordial path as
    compute_BBN_data: the same flags (the Hubble-only route since
    science-readiness prompt 01), with rho_NP = 0, which is PRyMordial with
    every new-physics contribution zero. Returns Yp_BBN, DOverH (x 1e5),
    He3OverH (x 1e5), Li7OverH (x 1e10), PRyM_version and small_network. One
    PRyMordial solve, about 10 s, with no wall-clock limit; must be called from
    the repository root. Not stored.
    (review-remediation prompt 04, item R3)
    """
    # this does not go through _run_PRyMordial: a failed baseline raises (for
    # example PRyMSolverFailureError), since it is not stored and should be
    # loud (run-integrity prompt 02)
    PRyMmain = _configure_PRyMordial(small_network)
    res = PRyMmain.PRyMclass(_zero_NP).PRyMresults()

    return {
        "Yp_BBN": res[4],
        "DOverH": res[5],
        "He3OverH": res[6],
        "Li7OverH": res[7],
        "PRyM_version": PRYM_VERSION,
        "small_network": small_network,
    }


@ray.remote
def compute_BBN_data(
    model_proxy: ScalarModelProxy,
    task_label: str,
    T_BBN_MeV_spline_max: float = 100,  # PRyMordial default begins at 10 MeV
    # PRyMordial's lowest query of the callback is 0.363 keV (measured, small
    # network, planning-probes/prym_callback_domain.py; its thermodynamic solve
    # runs past T_end = 1 keV). The floor is 0.2 keV, below that; the pre-check
    # below then makes a history reach 0.1 * 0.2 keV = 20 eV (science-readiness
    # prompt 06; the floor was 1e-4 keV = 0.1 eV, needing 0.01 eV).
    T_BBN_keV_spline_min: float = 0.2,
    small_network: bool = True,
    wall_clock_limit: Optional[float] = DEFAULT_BBN_WALL_CLOCK_LIMIT,
):
    """
    The BBN abundances of one stored history. For each sample in
    [T_BBN_keV_spline_min, T_BBN_MeV_spline_max] in T_Jordan,
        rho_NP = 3 M_P^2 H_J^2 - rho_R,J (1 + f_m),   r = rho_NP / rho_R,J,
    and PRyMordial is run on the callback rho_NP(T) = r(T) rho_SM(T) of
    build_rho_NP_callback, through the Hubble-only route of
    _configure_PRyMordial, with `wall_clock_limit` seconds (None for no limit).

    Since science-readiness prompt 01 nothing computes p_NP: the Hubble-only
    route reads rho_NP alone. Every failure returns `_failure_payload`.
    """
    model: ScalarModel = model_proxy.get()
    cosmology: BaseCosmology = model._cosmology
    units: UnitsLike = cosmology.units

    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_3_MP_SQ = 3.0 * CONST_MP_SQ

    z_grid: List[redshift] = []
    samples: List[SampleValues] = []

    raw_N_grid: List[float] = []

    log_T_Jordan_grid: List[float] = []
    density_NP_grid: List[float] = []
    rhorad_Jordan_grid: List[float] = []

    # PRyMordial expects energies to be in units of MeV
    log_T_Jordan_MeV_grid: List[float] = []
    density_NP_ratio_grid: List[float] = []

    T_BBN_spline_max = T_BBN_MeV_spline_max * units.MeV
    T_BBN_spline_min = T_BBN_keV_spline_min * units.keV

    if model.T_Jordan_stop.as_float > 0.1 * T_BBN_spline_min:
        formatter: energy_formatter = energy_formatter(units)
        print(
            f"!! compute_BBN_data {task_label}: T_Jordan_stop={formatter(model.T_Jordan_stop)} is more than 0.1*T_BBN_spline_min={formatter(0.1*T_BBN_spline_min)}, so cannot compute BBN abundances"
        )
        return _failure_payload(
            f"pre-check: T_Jordan_stop={formatter(model.T_Jordan_stop)} is more than 0.1*T_BBN_spline_min={formatter(0.1*T_BBN_spline_min)}"
        )

    log_MeV = log(units.MeV)

    with WallclockTimer() as NP_timer:
        # first, build the "new physics" density needed by PRyMordial
        for value in model.values:
            value: ScalarModelValue

            T_Jordan: float = exp(value.log_T_Jordan)

            if T_BBN_spline_min <= T_Jordan <= T_BBN_spline_max:
                z_grid.append(value.z)
                raw_N_grid.append(value.raw_N)
                log_T_Jordan_grid.append(value.log_T_Jordan)

                log_T_Jordan_MeV_grid.append(value.log_T_Jordan - log_MeV)

                rhorad_Jordan: float = exp(value.log_rhorad_Jordan)
                rhorad_Jordan_grid.append(rhorad_Jordan)

                H2_Jordan: float = value.H_Jordan * value.H_Jordan
                fm: float = exp(value.log_fm)

                LHS: float = H2_Jordan * CONST_3_MP_SQ
                density_NP: float = LHS - rhorad_Jordan * (1.0 + fm)

                density_NP_grid.append(density_NP)
                density_NP_ratio_grid.append(density_NP / rhorad_Jordan)

        # the ratio is multiplied back by the thermodynamic rho_SM(T_J), not by a
        # spline of the stored rho_R,J (review-remediation prompt 04; see its log)
        rho_SM_MeV4 = thermodynamic_rho_SM(cosmology, units)

        try:
            rho_NP = build_rho_NP_callback(
                log_T_Jordan_MeV_grid,
                density_NP_ratio_grid,
                rho_SM_MeV4,
                T_min_MeV=T_BBN_spline_min / units.MeV,
                T_max_MeV=T_BBN_spline_max / units.MeV,
                task_label=task_label,
            )
        except ComputationFailureError as e:
            print(f"!! compute_BBN_data {task_label}: {e}")
            return _failure_payload(f"BBN callbacks: {e}")

    with WallclockTimer() as BBN_timer:
        # run PRyMordial; any exception inside the call, and an output outside
        # the checks, comes back as a failure payload (run-integrity prompt 02;
        # science-readiness prompt 01)
        abundances: dict = _run_PRyMordial(rho_NP, small_network, wall_clock_limit)

    if abundances.get("failure", False):
        return abundances

    for i, z in enumerate(z_grid):
        samples.append(
            SampleValues(
                raw_N=raw_N_grid[i],
                log_T_Jordan=log_T_Jordan_grid[i],
                density_NP=density_NP_grid[i],
                density_NP_ratio=density_NP_grid[i] / rhorad_Jordan_grid[i],
            )
        )

    return {
        "Yp_BBN": abundances["Yp_BBN"],
        "DOverH": abundances["DOverH"],
        "He3OverH": abundances["He3OverH"],
        "Li7OverH": abundances["Li7OverH"],
        "z_grid": z_grid,
        "samples": samples,
        "BBN_compute_time": BBN_timer.elapsed,
        "NP_compute_time": NP_timer.elapsed,
        "small_network": small_network,
        "PRyM_version": PRYM_VERSION,  # PRyMordial seems not to have a proper versioning scheme
    }


class BBNData(DatastoreObject):
    def __init__(
        self,
        payload,
        model_proxy: ScalarModelProxy,
        label: Optional[str] = None,
        tags: Optional[List[store_tag]] = None,
    ):
        self._model_proxy: ScalarModelProxy = model_proxy
        model: ScalarModel = model_proxy.get()

        self._coupling = model.coupling
        self._potential = model.potential

        self._label: str = label
        self._tags: Optional[List[store_tag]] = tags if tags is not None else []

        if payload is None:
            DatastoreObject.__init__(self, None)

            self._small_network: Optional[bool] = None
            self._PRyM_version: Optional[str] = None

            self._Yp_BBN: Optional[float] = None
            self._DOverH: Optional[float] = None
            self._He3OverH: Optional[float] = None
            self._Li7OverH: Optional[float] = None

            self._values: Optional[List[BBNDataValue]] = None

            self._BBN_compute_time: Optional[float] = None
            self._NP_compute_time: Optional[float] = None

            self._failure: bool = None
            self._failure_reason: Optional[str] = None

            # we don't want to use self._values as an indicator of whether we contain
            # useful, readable information, because we might read with "_do_not_populate",
            # but still want to read the summary Yp, D/H, Li7/H, etc. data
            self._queryable: bool = False

        else:
            DatastoreObject.__init__(self, payload["store_id"])

            self._small_network = payload["small_network"]
            self._PRyM_version = payload["PRyM_version"]

            self._Yp_BBN = payload["Yp_BBN"]
            self._DOverH = payload["DOverH"]
            self._He3OverH = payload["He3OverH"]
            self._Li7OverH = payload["Li7OverH"]

            self._values = payload["values"]

            self._BBN_compute_time = payload["BBN_compute_time"]
            self._NP_compute_time = payload["NP_compute_time"]

            self._failure: Optional[bool] = payload["failure"]
            self._failure_reason: Optional[str] = payload["failure_reason"]

            # see above for explanation of this flag
            self._queryable = True

        self._compute_ref: Optional[ray.ObjectRef] = None

    @property
    def shard_key(self) -> ShardKeyType:
        return self._coupling.shard_key

    @property
    def failure(self) -> Optional[bool]:
        return self._failure

    @property
    def failure_reason(self) -> Optional[str]:
        """
        Why the BBN computation failed, or None if it did not. Unlike the other
        properties this is readable when `failure` is true; that is its purpose.
        """
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): failure_reason has not yet been populated"
            )
        return self._failure_reason

    @property
    def label(self) -> str:
        return self._label

    @property
    def tags(self) -> List[store_tag]:
        return self._tags

    @property
    def potential(self) -> AbstractPotential:
        return self._potential

    @property
    def coupling(self) -> AbstractCoupling:
        return self._coupling

    @property
    def small_network(self) -> Optional[bool]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): small_network has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._small_network

    @property
    def PRyM_version(self) -> Optional[str]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): PRyM_version has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._PRyM_version

    @property
    def Yp_BBN(self) -> Optional[float]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): Yp_BBN has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._Yp_BBN

    @property
    def DOverH(self) -> Optional[float]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): DOverH has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._DOverH

    @property
    def He3OverH(self) -> Optional[float]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): He3OverH has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._He3OverH

    @property
    def Li7OverH(self) -> Optional[float]:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): Li7OverH has not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._Li7OverH

    @property
    def values(self) -> List:
        if self._values is None:
            raise RuntimeError(
                f"BBNData ({self._label}): values have not yet been populated"
            )

        if self._failure:
            raise RuntimeError(
                f"BBNData ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._values

    @property
    def BBN_compute_time(self) -> float:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): BBN_compute_time has not yet been populated"
            )
        return self._BBN_compute_time

    @property
    def NP_compute_time(self) -> float:
        if self._queryable is False:
            raise RuntimeError(
                f"BBNData ({self._label}): NP_compute_time has not yet been populated"
            )
        return self._NP_compute_time

    def compute(
        self, label: Optional[str] = None, payload: Optional[dict[str, Any]] = None
    ) -> ray.ObjectRef:
        if self._queryable:
            raise RuntimeError("values have already been populated")

        if label is not None:
            self._label = label

        if payload is not None:
            small_network = payload.get("small_network", True)
            # seconds, or None for no limit (science-readiness prompt 01)
            wall_clock_limit = payload.get(
                "wall_clock_limit", DEFAULT_BBN_WALL_CLOCK_LIMIT
            )
        else:
            small_network = True
            wall_clock_limit = DEFAULT_BBN_WALL_CLOCK_LIMIT

        self._compute_ref = compute_BBN_data.remote(
            self._model_proxy,
            task_label=(
                self._label
                if self._label is not None
                else f"{self._potential.name}-{self._coupling.name}"
            ),
            small_network=small_network,
            wall_clock_limit=wall_clock_limit,
        )
        return self._compute_ref

    def store(self) -> Optional[bool]:
        if self._compute_ref is None:
            raise RuntimeError(
                "BBNData: store() called, but no compute() is in progress"
            )

        # check whether the computation has actually resolved
        resolved, unresolved = ray.wait([self._compute_ref], timeout=0)

        if len(resolved) == 0:
            return None

        # retrieve result and populate ourselves
        data = ray.get(self._compute_ref)
        self._compute_ref = None

        self._queryable = True

        failure: bool = data.get("failure", False)
        if failure:
            self._failure = True
            self._failure_reason = data.get("failure_reason", None)
            self._values = []
            return True

        self._failure = False
        self._failure_reason = None

        self._small_network = data["small_network"]
        self._PRyM_version = data["PRyM_version"]

        self._Yp_BBN = data["Yp_BBN"]
        self._DOverH = data["DOverH"]
        self._He3OverH = data["He3OverH"]
        self._Li7OverH = data["Li7OverH"]

        self._BBN_compute_time = data["BBN_compute_time"]
        self._NP_compute_time = data["NP_compute_time"]

        z_grid: redshift_array = data["z_grid"]
        samples = data["samples"]

        self._values = []
        for i in range(len(z_grid)):
            self._values.append(
                BBNDataValue(
                    None,
                    z=z_grid[i],
                    raw_N=samples[i].raw_N,
                    log_T_Jordan=samples[i].log_T_Jordan,
                    density_NP=samples[i].density_NP,
                    density_NP_ratio=samples[i].density_NP_ratio,
                )
            )

        return True


class BBNDataValue(DatastoreObject):
    def __init__(
        self,
        store_id: int,
        z: redshift,
        raw_N: float,
        log_T_Jordan: float,
        density_NP: float,
        density_NP_ratio: float,
    ):
        DatastoreObject.__init__(self, store_id)

        self._z: redshift = z
        self._raw_N: float = raw_N
        self._log_T_Jordan: float = log_T_Jordan

        self._density_NP: float = density_NP
        self._density_NP_ratio: float = density_NP_ratio

    @property
    def shard_key(self) -> ShardKeyType:
        return NotImplementedError

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def raw_N(self) -> float:
        return self._raw_N

    @property
    def log_T_Jordan(self) -> float:
        return self._log_T_Jordan

    @property
    def density_NP(self) -> float:
        return self._density_NP

    @property
    def density_NP_ratio(self) -> float:
        return self._density_NP_ratio
