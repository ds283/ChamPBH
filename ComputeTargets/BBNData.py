from collections import namedtuple
from math import exp, isfinite, log
from typing import Optional, List, Any, Callable, NamedTuple, Sequence

import numpy as np
import ray
from scipy.interpolate import make_interp_spline

from CosmologyConcepts import redshift, redshift_array
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore import DatastoreObject
from MetadataConcepts import store_tag
from Quadrature.supervisors.ScalarField import StateVector
from Units.base import UnitsLike
from config.defaults import DEFAULT_STRING_LENGTH
from config.sharding import ShardKeyType
from constants import RadiationConstant
from utilities import WallclockTimer, energy_formatter
from .Policies import PotentialDerivativePolicy
from .ScalarModel import ScalarModelProxy, ScalarModel, ScalarModelValue, ODEPolicy
from .exceptions import ComputationFailureError

SampleValues = namedtuple(
    "SampleValues",
    [
        "raw_N",
        "log_T_Jordan",
        "density_NP",
        "pressure_NP",
        "density_NP_ratio",
    ],
)


# The PRyMordial that produced a row: the pinned upstream hash, plus a suffix
# naming the ChamPBH patches applied to the vendored copy. "cham03" is
# review-remediation prompt 03: dTNPdt returns 0 (PRyM/PRyM_main.py). "ri02" is
# run-integrity prompt 02: every solve_ivp result is checked, and a solve that
# did not succeed raises PRyMSolverFailureError (PRyM/PRyM_main.py).
PRYM_VERSION = "bf24c3d+cham03+ri02"


def _failure_payload(reason: str) -> dict:
    """
    The value compute_BBN_data returns from every failure path. The reason is
    truncated to DEFAULT_STRING_LENGTH, the width of the BBNData.failure_reason
    column. (review-remediation prompt 03)
    """
    return {"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}


class NPCallbacks(NamedTuple):
    """
    The three new-physics callbacks PRyMordial takes. Each takes T in MeV;
    rho_NP and P_NP return MeV^4 and drho_NP_dT returns MeV^3.
    """

    rho_NP: Callable[[float], float]
    P_NP: Callable[[float], float]
    drho_NP_dT: Callable[[float], float]


def thermodynamic_rho_SM(
    eos, units: UnitsLike
) -> tuple[Callable[[float], float], Callable[[float], float]]:
    """
    The Standard-Model radiation density rho_SM(T) = (pi^2/30) g_rho(T) T^4 and
    its T-derivative, in PRyMordial's units: both take T in MeV and return MeV^4
    and MeV^3 respectively. `eos` is anything with `G_rho` and `dG_rho_dlogT`
    taking a dimensionful temperature in `units` (a LambdaCDM_GenericEOS
    cosmology or an EOS class). dG_rho_dlogT is d g_rho / d ln T (correct since
    review-remediation prompt 02), so
        d rho_SM / dT = (pi^2/30) T^3 [4 g_rho(T) + d g_rho / d ln T].
    Nothing is splined here and nothing is finite-differenced.
    (review-remediation prompt 04)
    """
    MeV = units.MeV

    def rho_SM_MeV4(T_in_MeV: float) -> float:
        g = float(eos.G_rho(T_in_MeV * MeV))
        return RadiationConstant * g * T_in_MeV**4

    def drho_SM_dT_MeV3(T_in_MeV: float) -> float:
        T = T_in_MeV * MeV
        g = float(eos.G_rho(T))
        dg_dlogT = float(eos.dG_rho_dlogT(T))
        return RadiationConstant * T_in_MeV**3 * (4.0 * g + dg_dlogT)

    return rho_SM_MeV4, drho_SM_dT_MeV3


def build_NP_callbacks(
    log_T_MeV: Sequence[float],
    density_ratio: Sequence[float],
    pressure_ratio: Sequence[float],
    rho_SM_MeV4: Callable[[float], float],
    drho_SM_dT_MeV3: Callable[[float], float],
    T_min_MeV: float,
    T_max_MeV: float,
    task_label: str,
) -> NPCallbacks:
    """
    Build PRyMordial's rho_NP, P_NP and drho_NP_dT from samples of the ratios
    r = rho_NP / rho_R,J and s = p_NP / rho_R,J against ln(T_J / MeV).

    The arrays are in the order the solver produced them, so `log_T_MeV` must be
    strictly decreasing. If it is not, ComputationFailureError is raised naming
    the first offending pair; nothing is sorted. The ratios are splined (cubic,
    no transform) on the reversed arrays, and
        rho_NP(T)     = r(T) rho_SM(T),
        P_NP(T)       = s(T) rho_SM(T),
        drho_NP_dT(T) = r'(ln T) rho_SM(T) / T + r(T) drho_SM_dT(T),
    where r' is the analytic derivative of the ratio spline in ln T. The
    derivative callback is differentiated from the interpolant, never
    finite-differenced (campaign README section 2 (g)).

    Guards, as before prompt 04: a negative T returns 0 (PRyMordial sometimes
    produces one); T above T_max_MeV or below T_min_MeV raises
    ComputationFailureError; an OverflowError or ValueError while evaluating is
    wrapped in ComputationFailureError.
    (review-remediation prompt 04, item R3)

    Finiteness (run-integrity prompt 02): a non-finite sample of log_T_MeV,
    density_ratio or pressure_ratio raises ComputationFailureError before any
    spline is built, naming the array, the first index and its T; and each
    callback raises ComputationFailureError for a non-finite T, before the
    negative-T guard. A NaN new-physics value made PRyMordial's high-T solve
    hang, and a NaN T passes both the negative-T and the domain guards.
    For finite input no value changes.
    """
    log_T = np.asarray(log_T_MeV, dtype=float)
    r = np.asarray(density_ratio, dtype=float)
    s = np.asarray(pressure_ratio, dtype=float)

    # refuse non-finite samples before the monotonicity check, which a NaN in
    # log_T_MeV would otherwise fail with a misleading message
    # (run-integrity prompt 02)
    for name, samples in (
        ("log_T_MeV", log_T),
        ("density_ratio", r),
        ("pressure_ratio", s),
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
    pressure_ratio_spline = make_interp_spline(x, s[::-1], k=3)
    density_ratio_derivative_spline = density_ratio_spline.derivative()

    def _check_finite(T_in_MeV: float, callback: str):
        # a NaN T passes both the negative-T guard and _check_domain
        # (run-integrity prompt 02)
        if not isfinite(T_in_MeV):
            raise ComputationFailureError(
                f"{callback} was called with a non-finite T_in_MeV={T_in_MeV!r} [{task_label}]"
            )

    def _check_domain(T_in_MeV: float):
        if T_in_MeV > T_max_MeV:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is larger than T_BBN_spline_max={T_max_MeV:.5g} MeV"
            )

        if T_in_MeV < T_min_MeV:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is smaller than T_BBN_spline_min={T_min_MeV:.5g} MeV"
            )

    def _wrap(e: Exception, kind: str, T_in_MeV: float) -> ComputationFailureError:
        msg = f"!! compute_BBN_data {task_label}: {kind} error at T = {T_in_MeV:.5g} MeV: {e}"
        print(msg)
        return ComputationFailureError(msg)

    def rho_NP(T_in_MeV: float) -> float:
        _check_finite(T_in_MeV, "rho_NP")

        # PRyMordial sometimes produces negative temperatures
        if T_in_MeV < 0:
            return 0.0

        _check_domain(T_in_MeV)

        log_T_in_MeV = log(T_in_MeV)
        try:
            value = float(density_ratio_spline(log_T_in_MeV)) * rho_SM_MeV4(T_in_MeV)
        except OverflowError as e:
            raise _wrap(e, "overflow", T_in_MeV) from e
        except ValueError as e:
            raise _wrap(e, "value", T_in_MeV) from e
        else:
            return value

    def P_NP(T_in_MeV: float) -> float:
        _check_finite(T_in_MeV, "P_NP")

        # PRyMordial sometimes produces negative temperatures
        if T_in_MeV < 0:
            return 0.0

        _check_domain(T_in_MeV)

        log_T_in_MeV = log(T_in_MeV)
        try:
            value = float(pressure_ratio_spline(log_T_in_MeV)) * rho_SM_MeV4(T_in_MeV)
        except OverflowError as e:
            raise _wrap(e, "overflow", T_in_MeV) from e
        except ValueError as e:
            raise _wrap(e, "value", T_in_MeV) from e
        else:
            return value

    def drho_NP_dT(T_in_MeV: float) -> float:
        _check_finite(T_in_MeV, "drho_NP_dT")

        # PRyMordial sometimes produces negative temperatures
        if T_in_MeV < 0:
            return 0.0

        _check_domain(T_in_MeV)

        log_T_in_MeV = log(T_in_MeV)
        try:
            ratio = float(density_ratio_spline(log_T_in_MeV))
            dratio_dlogT = float(density_ratio_derivative_spline(log_T_in_MeV))
            rho_SM = rho_SM_MeV4(T_in_MeV)
            drho_SM_dT = drho_SM_dT_MeV3(T_in_MeV)
            value = dratio_dlogT * rho_SM / T_in_MeV + ratio * drho_SM_dT
        except OverflowError as e:
            raise _wrap(e, "overflow", T_in_MeV) from e
        except ValueError as e:
            raise _wrap(e, "value", T_in_MeV) from e
        else:
            return value

    return NPCallbacks(rho_NP=rho_NP, P_NP=P_NP, drho_NP_dT=drho_NP_dT)


def jordan_Hdot_over_H2(
    HEdot_over_HE2: float,
    Omega_prime: float,
    Omega_primeprime: float,
    pi: float,
    pi_prime: float,
) -> float:
    """
    Hdot_J / H_J^2 from the Einstein-frame Hdot_E / H_E^2. Here Omega_prime and
    Omega_primeprime are d ln Omega / d phi and d^2 ln Omega / d phi^2, pi is
    d phi / dN and pi_prime is d pi / dN, with N the Einstein-frame e-fold number.
    With A1 = 1 + Omega' pi, H_J = (H_E / Omega) A1, so
        Hdot_J / H_J^2 = (Hdot_E / H_E^2 - Omega' pi) / A1 + A1' / A1^2,
        A1' = Omega'' pi^2 + Omega' pi'.
    Before review-remediation prompt 04 (item R3) the first term of A1' was
    Omega'' pi, which is also dimensionally inconsistent. It is zero for the
    exponential coupling, so no result has depended on it.
    """
    A1 = 1.0 + Omega_prime * pi
    return (HEdot_over_HE2 - Omega_prime * pi) / A1 + (
        Omega_primeprime * pi**2 + Omega_prime * pi_prime
    ) / (A1 * A1)


def _configure_PRyMordial(small_network: bool):
    """
    Set the PRyMordial flags that compute_BBN_data and compute_SM_baseline both
    use, and return the PRyM_main module. (Factored out in review-remediation
    prompt 04 so that the baseline goes through exactly the same settings.)
    """
    # import locally so that global variable in PRyMini don't leak between threads
    # (not sure if this is possible or not, but worth being defensive)
    import PRyM.PRyM_init as PRyMini
    import PRyM.PRyM_main as PRyMmain

    # ask PRyMordial to include new physics ("NP") contributions to the thermodynamics
    PRyMini.NP_thermo_flag = True

    # PryMordial seems to require the temperature in the NP sector to be set separately
    # note PRyMini.T_start seems to be in Kelvin whereas all other energies are measured in MeV
    PRyMini.Tstart_NP = PRyMini.T_start / PRyMini.MeV_to_Kelvin

    # disable verbose output
    PRyMini.verbose_flag = False

    # select the reaction network: True restricts PRyMordial to its 12-reaction network, which is
    # faster but unreliable for Li7; False (production) runs the full network. PRyMordial reads
    # smallnet_flag when the solve runs. Until production-readiness prompt 02 this set
    # small_network_flag, which PRyMordial never reads, so every solve ran the full network.
    PRyMini.smallnet_flag = small_network

    return PRyMmain


def _zero_NP(T_in_MeV: float) -> float:
    return 0.0


def _run_PRyMordial(callbacks: NPCallbacks, small_network: bool) -> dict:
    """
    The PRyMordial boundary (run-integrity prompt 02). Configure PRyMordial
    with `_configure_PRyMordial(small_network)`, run it on the three
    new-physics callbacks, and return {"Yp_BBN", "DOverH", "He3OverH",
    "Li7OverH"} (D/H and 3He/H x 1e5, 7Li/H x 1e10).

    Any Exception raised inside the PRyMordial call is returned as
    `_failure_payload("PRyMordial: <Type>: <message>")`. That covers
    PRyMSolverFailureError (a solve_ivp that did not succeed), a
    ComputationFailureError raised by the callbacks, which PRyMordial calls,
    and anything else PRyMordial or the callbacks raise. It does not cover a
    BaseException such as KeyboardInterrupt, nor anything raised outside the
    call: an exception from ChamPBH's own code outside PRyMordial is a bug and
    propagates. compute_SM_baseline does not use this helper.
    """
    PRyMmain = _configure_PRyMordial(small_network)

    try:
        res = PRyMmain.PRyMclass(
            callbacks.rho_NP, callbacks.P_NP, callbacks.drho_NP_dT
        ).PRyMresults()
    except Exception as e:
        return _failure_payload(f"PRyMordial: {type(e).__name__}: {e}")

    return {
        "Yp_BBN": res[4],
        "DOverH": res[5],
        "He3OverH": res[6],
        "Li7OverH": res[7],
    }


def compute_SM_baseline(small_network: bool) -> dict:
    """
    The Standard-Model abundances through the same PRyMordial path as
    compute_BBN_data: the same flags (NP_thermo_flag = True and the rest), with
    rho_NP = p_NP = drho_NP/dT = 0. Returns Yp_BBN, DOverH (x 1e5), He3OverH
    (x 1e5), Li7OverH (x 1e10), PRyM_version and small_network. One PRyMordial
    solve, about 10 s; must be called from the repository root. Not stored.
    (review-remediation prompt 04, item R3)
    """
    # this does not go through _run_PRyMordial: a failed baseline raises (for
    # example PRyMSolverFailureError), since it is not stored and should be
    # loud (run-integrity prompt 02)
    PRyMmain = _configure_PRyMordial(small_network)
    res = PRyMmain.PRyMclass(_zero_NP, _zero_NP, _zero_NP).PRyMresults()

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
    T_BBN_keV_spline_min: float = 1e-4,  # PRyMordial default ends at 1 keV, but samples at later times
    small_network: bool = False,
):
    model: ScalarModel = model_proxy.get()
    cosmology: BaseCosmology = model._cosmology
    units: UnitsLike = cosmology.units

    potential: AbstractPotential = model.potential
    coupling: AbstractCoupling = model.coupling

    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_3_MP_SQ = 3.0 * CONST_MP_SQ

    z_grid: List[redshift] = []
    samples: List[SampleValues] = []

    raw_N_grid: List[float] = []

    log_T_Jordan_grid: List[float] = []
    pressure_NP_grid: List[float] = []
    density_NP_grid: List[float] = []
    rhorad_Jordan_grid: List[float] = []

    # PRyMordial expects energies to be in units of MeV
    log_T_Jordan_MeV_grid: List[float] = []
    density_NP_ratio_grid: List[float] = []
    pressure_NP_ratio_grid: List[float] = []

    T_BBN_spline_max = T_BBN_MeV_spline_max * units.MeV
    T_BBN_spline_min = T_BBN_keV_spline_min * units.keV

    if model.T_Jordan_stop.as_float > 0.1 * T_BBN_spline_min:
        formatter: energy_formatter = energy_formatter(units)
        print(
            f"!! compute_BBN_data {task_label}: T_Jordan_stop={formatter(model.T_Jordan_stop)} is more than than 0.1*T_BBN_spline_min={formatter(0.1*T_BBN_spline_min)}, so cannot compute BBN abundances"
        )
        return _failure_payload(
            f"pre-check: T_Jordan_stop={formatter(model.T_Jordan_stop)} is more than 0.1*T_BBN_spline_min={formatter(0.1*T_BBN_spline_min)}"
        )

    log_MeV = log(units.MeV)

    V_policy: PotentialDerivativePolicy = PotentialDerivativePolicy(
        task_label, cosmology, potential
    )
    ODE_policy: ODEPolicy = ODEPolicy(task_label, cosmology, potential, coupling)

    with WallclockTimer() as NP_timer:
        # first, build estimates for the "new physics" density and pressure needed by PRyMordial
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

                Sigma: float = value.Sigma
                fm: float = exp(value.log_fm)
                w: float = (1.0 - Sigma) / 3.0

                LHS: float = H2_Jordan * CONST_3_MP_SQ
                density_NP: float = LHS - rhorad_Jordan * (1.0 + fm)

                density_NP_grid.append(density_NP)
                density_NP_ratio_grid.append(density_NP / rhorad_Jordan)

                HEdot_over_HE2: float = (
                    V_policy.Hdot_over_H2_plus_3(
                        value.phi_Einstein,
                        value.pi_Einstein,
                        value.log_rhorad_Einstein,
                        Sigma,
                        fm,
                    )
                    - 3.0
                )
                log_Omega_prime: float = coupling.d_logOmega_dphi(value.phi_Einstein)
                log_Omega_primeprime: float = coupling.d2_logOmega_dphi2(
                    value.phi_Einstein
                )

                state: StateVector = StateVector(
                    phi_Einstein=value.phi_Einstein,
                    pi_Einstein=value.pi_Einstein,
                    log_rhorad_Einstein=value.log_rhorad_Einstein,
                    log_fm=value.log_fm,
                    log_T_Jordan=value.log_T_Jordan,
                )
                data = ODE_policy(value.raw_N, state)
                pi_Einstein_prime: float = (
                    data.friction_term + data.reflecting_term + data.kicking_term
                )

                HJdot_over_HJ2: float = jordan_Hdot_over_H2(
                    HEdot_over_HE2,
                    log_Omega_prime,
                    log_Omega_primeprime,
                    value.pi_Einstein,
                    pi_Einstein_prime,
                )

                pressure_NP: float = (
                    -LHS * (1.0 + 2.0 * HJdot_over_HJ2 / 3.0) - w * rhorad_Jordan
                )

                pressure_NP_grid.append(pressure_NP)
                pressure_NP_ratio_grid.append(pressure_NP / rhorad_Jordan)

        # the ratios are multiplied back by the thermodynamic rho_SM(T_J), not by a
        # spline of the stored rho_R,J (review-remediation prompt 04; see its log)
        rho_SM_MeV4, drho_SM_dT_MeV3 = thermodynamic_rho_SM(cosmology, units)

        try:
            callbacks: NPCallbacks = build_NP_callbacks(
                log_T_Jordan_MeV_grid,
                density_NP_ratio_grid,
                pressure_NP_ratio_grid,
                rho_SM_MeV4,
                drho_SM_dT_MeV3,
                T_min_MeV=T_BBN_spline_min / units.MeV,
                T_max_MeV=T_BBN_spline_max / units.MeV,
                task_label=task_label,
            )
        except ComputationFailureError as e:
            print(f"!! compute_BBN_data {task_label}: {e}")
            return _failure_payload(f"BBN callbacks: {e}")

    with WallclockTimer() as BBN_timer:
        # run PRyMordial; any exception inside the call comes back as a failure
        # payload (run-integrity prompt 02)
        abundances: dict = _run_PRyMordial(callbacks, small_network)

    if abundances.get("failure", False):
        return abundances

    for i, z in enumerate(z_grid):
        samples.append(
            SampleValues(
                raw_N=raw_N_grid[i],
                log_T_Jordan=log_T_Jordan_grid[i],
                density_NP=density_NP_grid[i],
                density_NP_ratio=density_NP_grid[i] / rhorad_Jordan_grid[i],
                pressure_NP=pressure_NP_grid[i],
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
            small_network = payload.get("small_network", False)
        else:
            small_network = False

        self._compute_ref = compute_BBN_data.remote(
            self._model_proxy,
            task_label=(
                self._label
                if self._label is not None
                else f"{self._potential.name}-{self._coupling.name}"
            ),
            small_network=small_network,
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
                    pressure_NP=samples[i].pressure_NP,
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
        pressure_NP: float,
        density_NP_ratio: float,
    ):
        DatastoreObject.__init__(self, store_id)

        self._z: redshift = z
        self._raw_N: float = raw_N
        self._log_T_Jordan: float = log_T_Jordan

        self._density_NP: float = density_NP
        self._pressure_NP: float = pressure_NP
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
    def pressure_NP(self) -> float:
        return self._pressure_NP

    @property
    def density_NP_ratio(self) -> float:
        return self._density_NP_ratio
