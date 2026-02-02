from collections import namedtuple
from math import exp, log
from typing import Optional, List

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
from config.sharding import ShardKeyType
from utilities import WallclockTimer
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


def _make_spline(x_grid, y_grid):
    paired_data = list(zip(x_grid, y_grid))
    paired_data.sort(key=lambda pair: pair[0])

    sorted_x_grid, sorted_y_grid = zip(*paired_data)
    return make_interp_spline(sorted_x_grid, sorted_y_grid, k=3)


@ray.remote
def compute_BBN_data(
    model_proxy: ScalarModelProxy,
    task_label: str,
    T_BBN_MeV_spline_max: float = 100,  # PRyMordial default begins at 10 MeV
    T_BBN_keV_spline_min: float = 1e-4,  # PRyMordial default ends at 1 keV, but samples at later times
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
    pressure_NP_MeV4_grid: List[float] = []
    density_NP_MeV4_grid: List[float] = []

    T_BBN_spline_max = T_BBN_MeV_spline_max * units.MeV
    T_BBN_spline_min = T_BBN_keV_spline_min * units.keV

    log_MeV = log(units.MeV)
    MeV2 = units.MeV * units.MeV
    MeV4 = MeV2 * MeV2

    last_log_T_Jordan_MeV = None

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

                log_T_Jordan_MeV = value.log_T_Jordan - log_MeV
                if last_log_T_Jordan_MeV is not None:
                    if log_T_Jordan_MeV > last_log_T_Jordan_MeV:
                        print(
                            f"!! compute_BBN_data {task_label}: T_Jordan values are not monotonically decreasing: this log(T_Jordan/MeV) {log_T_Jordan_MeV:.5g} (this) > {log_T_Jordan_MeV:.5g} (last) = {last_log_T_Jordan_MeV:.5g} (last)"
                        )

                log_T_Jordan_MeV_grid.append(log_T_Jordan_MeV)
                last_log_T_Jordan_MeV = log_T_Jordan_MeV

                rhorad_Jordan: float = exp(value.log_rhorad_Jordan)
                rhorad_Jordan_grid.append(rhorad_Jordan)

                H2_Jordan: float = value.H_Jordan * value.H_Jordan

                Sigma: float = value.Sigma
                fm: float = exp(value.log_fm)
                w: float = (1.0 - Sigma) / 3.0

                LHS: float = H2_Jordan * CONST_3_MP_SQ
                density_NP: float = LHS - rhorad_Jordan * (1.0 + fm)

                density_NP_grid.append(density_NP)
                density_NP_MeV4_grid.append(density_NP / MeV4)

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

                A1: float = 1.0 + log_Omega_prime * value.pi_Einstein

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

                HJdot_over_HJ2: float = (
                    HEdot_over_HE2 - log_Omega_prime * value.pi_Einstein
                ) / A1 + (
                    log_Omega_primeprime * value.pi_Einstein
                    + log_Omega_prime * pi_Einstein_prime
                ) / (
                    A1 * A1
                )

                pressure_NP: float = (
                    -LHS * (1.0 + 2.0 * HJdot_over_HJ2 / 3.0) - w * rhorad_Jordan
                )

                pressure_NP_grid.append(pressure_NP)
                pressure_NP_MeV4_grid.append(pressure_NP / MeV4)

        density_NP_MeV4_spline = _make_spline(
            log_T_Jordan_MeV_grid,
            density_NP_MeV4_grid,
        )
        pressure_NP_MeV4_spline = _make_spline(
            log_T_Jordan_MeV_grid,
            pressure_NP_MeV4_grid,
        )
        density_NP_MeV4_derivative_spline = density_NP_MeV4_spline.derivative()

    def rho_NP(T_in_MeV: float):
        T = T_in_MeV * units.MeV

        # PRyMordial sometimes produces negative temperatures
        if T < 0:
            return 0.0

        if T > T_BBN_spline_max:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is larger than T_BBN_spline_max={T_BBN_spline_max/units.MeV:.5g} MeV"
            )

        if T < T_BBN_spline_min:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is smaller than T_BBN_spline_min={T_BBN_spline_min/units.MeV:.5g} MeV"
            )

        log_T_in_MeV = log(T_in_MeV)
        return density_NP_MeV4_spline(log_T_in_MeV)

    def P_NP(T_in_MeV: float):
        T = T_in_MeV * units.MeV

        # PRyMordial sometimes produces negative temperatures
        if T < 0:
            return 0.0

        if T > T_BBN_spline_max:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is larger than T_BBN_spline_max={T_BBN_spline_max/units.MeV:.5g} MeV"
            )

        if T < T_BBN_spline_min:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is smaller than T_BBN_spline_min={T_BBN_spline_min/units.MeV:.5g} MeV"
            )

        log_T_in_MeV = log(T_in_MeV)
        return pressure_NP_MeV4_spline(log_T_in_MeV)

    def drho_NP_dT(T_in_MeV: float):
        T = T_in_MeV * units.MeV

        # PRyMordial sometimes produces negative temperatures
        if T < 0:
            return 0.0

        if T > T_BBN_spline_max:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is larger than T_BBN_spline_max={T_BBN_spline_max/units.MeV:.5g} MeV"
            )

        if T < T_BBN_spline_min:
            raise ComputationFailureError(
                f"T_in_MeV={T_in_MeV:.5g} MeV is smaller than T_BBN_spline_min={T_BBN_spline_min/units.MeV:.5g} MeV"
            )

        log_T_in_MeV = log(T_in_MeV)
        return density_NP_MeV4_derivative_spline(log_T_in_MeV)

    with WallclockTimer() as BBN_timer:
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

        # speed up calculation using small reaction network, at cost of Li7 accuracy
        PRyMini.small_network_flag = True

        # run PRyMordial
        res = PRyMmain.PRyMclass(rho_NP, P_NP, drho_NP_dT).PRyMresults()

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
        "Yp_BBN": res[4],
        "DOverH": res[5],
        "HeOverH": res[6],
        "LiOverH": res[7],
        "z_grid": z_grid,
        "samples": samples,
        "BBN_compute_time": BBN_timer.elapsed,
        "NP_compute_time": NP_timer.elapsed,
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
            self._Yp_BBN: Optional[float] = None
            self._DOverH: Optional[float] = None
            self._HeOverH: Optional[float] = None
            self._LiOverH: Optional[float] = None

            self._values: Optional[List[BBNDataValue]] = None

            self._BBN_compute_time: Optional[float] = None
            self._NP_compute_time: Optional[float] = None

            self._populated: bool = False
        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._Yp_BBN = payload["Yp_BBN"]
            self._DOverH = payload["DOverH"]
            self._HeOverH = payload["HeOverH"]
            self._LiOverH = payload["LiOverH"]

            self._values = payload["values"]

            self._BBN_compute_time = payload["BBN_compute_time"]
            self._NP_compute_time = payload["NP_compute_time"]

            self._populated = True

        self._compute_ref: Optional[ray.ObjectRef] = None

    @property
    def shard_key(self) -> ShardKeyType:
        return self._coupling.shard_key

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
    def Yp_BBN(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("Yp_BBN has not yet been populated")

        return self._Yp_BBN

    @property
    def DOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("DOverH has not yet been populated")

        return self._DOverH

    @property
    def HeOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("HeOverH has not yet been populated")

        return self._HeOverH

    @property
    def LiOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("LiOverH has not yet been populated")

        return self._LiOverH

    @property
    def values(self) -> List:
        if self._values is None:
            raise RuntimeError("values have not yet been populated")

        return self._values

    @property
    def BBN_compute_time(self) -> float:
        if self._populated is False:
            raise RuntimeError("BBN_compute_time has not yet been populated")
        return self._BBN_compute_time

    @property
    def NP_compute_time(self) -> float:
        if self._populated is False:
            raise RuntimeError("NP_compute_time has not yet been populated")
        return self._NP_compute_time

    def compute(self, label: Optional[str] = None) -> ray.ObjectRef:
        if self._populated:
            raise RuntimeError("values have already been populated")

        if label is not None:
            self._label = label

        self._compute_ref = compute_BBN_data.remote(
            self._model_proxy,
            task_label=(
                self._label
                if self._label is not None
                else f"{self._potential.name}-{self._coupling.name}"
            ),
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

        self._Yp_BBN = data["Yp_BBN"]
        self._DOverH = data["DOverH"]
        self._HeOverH = data["HeOverH"]
        self._LiOverH = data["LiOverH"]

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

        self._populated = True
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
