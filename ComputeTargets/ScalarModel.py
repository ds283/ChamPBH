from collections import namedtuple
from math import fabs, log
from typing import Optional, List

import ray
from ray import ObjectRef
from scipy.integrate import solve_ivp
from scipy.interpolate import make_interp_spline

from ComputeTargets.spline_wrappers import ZSplineWrapper
from CosmologyConcepts import (
    redshift,
    temperature,
    redshift_array,
    AbstractPotential,
    AbstractCoupling,
)
from CosmologyModels import BaseCosmology
from Datastore import DatastoreObject
from MetadataConcepts import tolerance, store_tag
from Quadrature.integration_metadata import IntegrationSolver, IntegrationData
from Quadrature.supervisors.base import RHS_timer, IntegrationSupervisor
from Units.base import UnitsLike
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE

PHI_EINSTEIN_INDEX = 0
PI_EINSTEIN_INDEX = 1
LOG_RHORAD_EINSTEIN_INDEX = 2
LOG_FM_INDEX = 3
EXPECTED_SOL_LENGTH = 4

ModelFunctions = namedtuple(
    "ModelFunctions",
    [
        "H_Einstein",
        "H_Jordan",
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_rhorad_Jordan",
        "log_fm",
        "T_Jordan",
    ],
)


@ray.remote
def compute_scalar_model(
    cosmology: BaseCosmology,
    T_init: temperature,
    T_stop: temperature,
    phi_init: float,
    pi_init: float,
    z_grid: redshift_array,
    potential: AbstractPotential,
    coupling: AbstractCoupling,
    atol: float = DEFAULT_ABS_TOLERANCE,
    rtol: float = DEFAULT_REL_TOLERANCE,
) -> dict:
    """
    :param cosmology:
    :param T_init: initial radiation temperature in Jordan frame
    :param T_stop: final radiation temperature in Jordan frame (usually T_CMB)
    :param phi_init: initial phi value
    :param pi_init: initial dphi/dN value
    :param z_grid:
    :param potential:
    :param coupling:
    :param atol:
    :param rtol:
    :return:
    """
    T_init_float: float = float(T_init)
    T_stop_float: float = float(T_stop)

    def RHS(z, state, supervisor) -> List[float]:
        with RHS_timer(supervisor) as timer:
            H = cosmology.Hubble(z)

            return [da0_tau_dz]

    with IntegrationSupervisor() as supervisor:
        rho_init = cosmology.rho(z_init)

        initial_state = [tau_init]

        sol = solve_ivp(
            RHS,
            method="RK45",
            t_span=(z_init, z_stop),
            y0=initial_state,
            t_eval=z_sample.as_float_list(),
            atol=atol,
            rtol=rtol,
            args=(supervisor,),
            dense_output=True,
        )

    if not sol.success:
        raise RuntimeError(
            f'compute_scalar_model: integration did not terminate successfully (z_init={z_init:.5g}, z_stop={z_stop:.5g}, error at z={sol.t[-1]:.5g}, "{sol.message}")'
        )

    sampled_z = sol.t
    sampled_values = sol.y
    if len(sampled_values) != EXPECTED_SOL_LENGTH:
        raise RuntimeError(
            f"compute_scalar_model: solution does not have expected number of members (expected {EXPECTED_SOL_LENGTH}, found {len(sampled_values)}; length of sol.t={len(z_sample)})"
        )
    a0_tau_sample = sampled_values[A0_TAU_INDEX]

    returned_values = sampled_z.size
    expected_values = len(z_sample)

    if returned_values != expected_values:
        raise RuntimeError(
            f"compute_scalar_model: solve_ivp returned {returned_values} samples, but expected {expected_values}"
        )

    # validate that the samples of the solution correspond to the z-sample points that we specified.
    # This really should be true, but there is no harm in being defensive.
    for i in range(returned_values):
        diff = sampled_z[i] - z_sample[i].z
        if fabs(diff) > DEFAULT_ABS_TOLERANCE:
            raise RuntimeError(
                f"compute_scalar_model: solve_ivp returned sample points that differ from those requested (difference={diff} at i={i})"
            )

    # each BaseCosmology instance provides methods to evaluate H(z), rho(z), and the value of the equation of state
    # for the background and perturbations
    H_sample = [cosmology.Hubble(z.z) for z in z_sample]
    rho_sample = [cosmology.rho(z.z) for z in z_sample]
    T_photon_sample = [cosmology.T_photon(z.z) for z in z_sample]
    wBackground_sample = [cosmology.wBackground(z.z) for z in z_sample]
    wPerturbations_sample = [cosmology.wPerturbations(z.z) for z in z_sample]

    # further, each BaseCosmology instance may provide methods to evaluate the derivatives of H(z) and w(z), but if it doesn't,
    # we estimate these derivatives using a spline

    def _build_derivative(attr: str, f_to_diff=None, sample_to_diff=None):
        if f_to_diff is None and sample_to_diff is None:
            raise RuntimeError(
                "compute_scalar_model._build_derivative: f_to_diff and sample_to_diff cannot both be None"
            )

        if hasattr(cosmology, attr):
            return [getattr(cosmology, attr)(z.z) for z in z_sample]

        if f_to_diff is not None:
            data = [(log(1.0 + z.z), f_to_diff(z.z)) for z in z_sample]
        else:
            data = [(log(1.0 + z.z), s) for z, s in zip(z_sample, sample_to_diff)]

        data.sort(key=lambda pair: pair[0])
        x_data, y_data = zip(*data)

        raw = make_interp_spline(x_data, y_data)
        deriv = raw.derivative()

        spline = ZSplineWrapper(
            deriv,
            label=attr,
            min_z=z_sample.min.z,
            max_z=z_sample.max.z,
            log_z=True,
            deriv=True,
        )

        return [spline(z.z) for z in z_sample]

    d_lnH_dz_sample = _build_derivative(
        "d_lnH_dz", f_to_diff=lambda z: log(cosmology.Hubble(z))
    )
    d2_lnH_dz2_sample = _build_derivative("d2_lnH_dz2", sample_to_diff=d_lnH_dz_sample)
    d3_lnH_dz3_sample = _build_derivative(
        "d3_lnH_dz3", sample_to_diff=d2_lnH_dz2_sample
    )
    d_wPerturbations_dz_sample = _build_derivative(
        "d_wPerturbations_dz", sample_to_diff=wPerturbations_sample
    )
    d2_wPerturbations_dz2_sample = _build_derivative(
        "d2_wPerturbations_dz2", sample_to_diff=d_wPerturbations_dz_sample
    )

    return {
        "metadata": IntegrationData(
            compute_time=supervisor.integration_time,
            compute_steps=int(sol.nfev),
            RHS_evaluations=supervisor.RHS_evaluations,
            mean_RHS_time=supervisor.mean_RHS_time,
            max_RHS_time=supervisor.max_RHS_time,
            min_RHS_time=supervisor.min_RHS_time,
        ),
        "H_Einstein_sample": H_Einstein_sample,
        "phi_Einstein_sample": phi_Einstein_sample,
        "rho_sample": rho_sample,
        "T_Jordan_sample": T_Jordan_sample,
        "fm_sample": fm_sample,
        "solver_label": "solve_ivp+RK45-stepping0",
    }


class ScalarModel(DatastoreObject):
    """
    Encapsulates the time history of a cosmological model.
    This bakes-in all the quantities we need such as the conformal time \tau (for analytic
    approximations to the transfer functions and Green's functions).
    It also means we have an explicit record in the database of the values of H(z), w(z), etc.,
    that yielded a particular set of results
    """

    def __init__(
        self,
        payload,
        solver_labels: dict,
        cosmology: BaseCosmology,
        T_init: temperature,  # initial Jordan-frame temperature
        T_stop: temperature,  # Jordan-frame temperature at which to terminate the calculation
        phi_init: float,  # initial value of Einstein-frame scalar phi
        pi_init: float,  # initial value of dphi/dN
        potential: AbstractPotential,
        coupling: AbstractCoupling,
        atol: tolerance,
        rtol: tolerance,
        z_grid: Optional[redshift_array] = None,
        label: Optional[str] = None,
        tags: Optional[List[store_tag]] = None,
    ):
        self._solver_labels = solver_labels

        self._T_init: temperature = T_init
        self._T_stop: temperature = T_stop

        self._phi_init: float = phi_init
        self._pi_init: float = pi_init

        self._potential: AbstractPotential = potential
        self._coupling: AbstractCoupling = coupling

        self._z_grid: Optional[redshift_array] = z_grid

        if payload is None:
            DatastoreObject.__init__(self, None)
            self._metadata = None
            self._solver = None
            self._values = None

        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._metadata: Optional[IntegrationData] = payload["metadata"]
            self._solver: Optional[IntegrationSolver] = payload["solver"]
            self._values: Optional[List[ScalarModelValue]] = payload["values"]

        # store parameters
        self._label = label
        self._tags = tags if tags is not None else []

        self._cosmology = cosmology
        self._units = cosmology.units

        self._functions = None

        self._compute_ref = None

        self._atol = atol
        self._rtol = rtol

    @property
    def cosmology(self) -> BaseCosmology:
        return self._cosmology

    @property
    def label(self) -> Optional[str]:
        return self._label

    @property
    def tags(self) -> List[store_tag]:
        return self._tags

    @property
    def T_init(self) -> temperature:
        return self._T_init

    @property
    def T_stop(self) -> temperature:
        return self._T_stop

    @property
    def potential(self) -> AbstractPotential:
        return self._potential

    @property
    def coupling(self) -> AbstractCoupling:
        return self._coupling

    @property
    def metadata(self) -> IntegrationData:
        if self.values is None:
            raise RuntimeError("values have not yet been populated")

        return self._data

    @property
    def solver(self) -> IntegrationSolver:
        if self._solver is None:
            raise RuntimeError("solver has not yet been populated")
        return self._solver

    @property
    def values(self) -> List:
        if self._values is None:
            raise RuntimeError("values has not yet been populated")
        return self._values

    @property
    def functions(self) -> ModelFunctions:
        if self._values is None:
            raise RuntimeError("values has not yet been populated")

        if self._functions is None:
            self._create_functions()

        return self._functions

    def _create_functions(self):
        def _build_func(attr: str):
            data = [(v.p, getattr(v, attr)) for v in self.values]
            data.sort(key=lambda pair: pair[0])

            x_data, y_data = zip(*data)
            spline = make_interp_spline(x_data, y_data)
            return ZSplineWrapper(
                spline,
                label=attr,
                min_z=self.z_sample.min.z,
                max_z=self.z_sample.max.z,
                log_z=True,
            )

        # build splines for those functions that are stored directly as part of the integration output
        H_Einstein = _build_func("H_Einstein")
        H_Jordan = _build_func("H_Jordan")
        phi_Einstein = _build_func("phi_Einstein")
        pi_Einstein = _build_func("pi_Einstein")
        log_rhorad_Einstein = _build_func("log_rhorad_Einstein")
        log_rhorad_Jordan = _build_func("log_rhorad_Jordan")
        log_fm = _build_func("log_fm")
        T_Jordan = _build_func("T_Jordan")

        self._functions = ModelFunctions(
            H_Einstein=H_Einstein,
            H_Jordan=H_Jordan,
            phi_Einstein=phi_Einstein,
            pi_Einstein=pi_Einstein,
            log_rhorad_Einstein=log_rhorad_Einstein,
            log_rhorad_Jordan=log_rhorad_Jordan,
            log_fm=log_fm,
            T_Jordan=T_Jordan,
        )

    def compute(self, label: Optional[str] = None):
        if self._values is not None:
            raise RuntimeError("values have already been populated")

        def check_required_parameter(attr: str):
            if not hasattr(self, attr):
                raise RuntimeError(
                    f'Object has not been configured correctly for a concrete calcuation ("{attr}" is missing). This object can only represent a Datastore query.'
                )

            if getattr(self, attr) is None:
                raise RuntimeError(
                    f'Object has not been configured correctly for a concrete calcuation ("{attr}" is set to None). This object can only represent a Datastore query.'
                )

        check_required_parameter("_T_high")
        check_required_parameter("_T_low")
        check_required_parameter("_phi_init")
        check_required_parameter("_pi_init")
        check_required_parameter("_z_grid")

        # replace label if specified
        if label is not None:
            self._label = label

        self._compute_ref = compute_scalar_model.remote(
            self.cosmology,
            self.T_init,
            self.T_stop,
            self.phi_init,
            self.pi_init,
            self.z_grid,
            self.potential,
            self.coupling,
            atol=self._atol.tol,
            rtol=self._rtol.tol,
        )
        return self._compute_ref

    def store(self) -> Optional[bool]:
        if self._compute_ref is None:
            raise RuntimeError(
                "ScalarModel: store() called, but no compute() is in progress"
            )

        # check whether the computation has actually resolved
        resolved, unresolved = ray.wait([self._compute_ref], timeout=0)

        # if not, return None
        if len(resolved) == 0:
            return None

        # retrieve result and populate ourselves
        data = ray.get(self._compute_ref)
        self._compute_ref = None

        self._data = data["metadata"]

        H_sample = data["H_sample"]
        wB_sample = data["wBackground_sample"]
        wP_sample = data["wPerturbations_sample"]
        rho_sample = data["rho_sample"]
        T_photon_sample = data["T_photon_sample"]
        tau_sample = data["a0_tau_sample"]

        d_lnH_ds_sample = data["d_lnH_dz_sample"]
        d2_lnH_dz2_sample = data["d2_lnH_dz2_sample"]
        d3_lnH_dz3_sample = data["d3_lnH_dz3_sample"]

        d_wPerturbations_dz_sample = data["d_wPerturbations_dz_sample"]
        d2_wPerturbations_dz2_sample = data["d2_wPerturbations_dz2_sample"]

        self._values = []
        for i in range(len(H_sample)):
            self._values.append(
                ScalarModelValue(
                    None,
                    self._z_sample[i],
                    Hubble=H_sample[i],
                    wBackground=wB_sample[i],
                    wPerturbations=wP_sample[i],
                    rho=rho_sample[i],
                    tau=tau_sample[i],
                    T_photon=T_photon_sample[i],
                    d_lnH_dz=d_lnH_ds_sample[i],
                    d2_lnH_dz2=d2_lnH_dz2_sample[i],
                    d3_lnH_dz3=d3_lnH_dz3_sample[i],
                    d_wPerturbations_dz=d_wPerturbations_dz_sample[i],
                    d2_wPerturbations_dz2=d2_wPerturbations_dz2_sample[i],
                )
            )

        self._solver = self._solver_labels[data["solver_label"]]

        return True


class ScalarModelValue(DatastoreObject):
    def __init__(
        self,
        store_id: int,
        z: redshift,
        Hubble: float,
        wBackground: float,
        wPerturbations: float,
        rho: float,
        tau: float,
        T_photon: float,
        d_lnH_dz: float,
        d2_lnH_dz2: Optional[float] = None,
        d3_lnH_dz3: Optional[float] = None,
        d_wPerturbations_dz: Optional[float] = None,
        d2_wPerturbations_dz2: Optional[float] = None,
    ):
        DatastoreObject.__init__(self, store_id)

        self._z = z

        self._Hubble: float = Hubble
        self._wBackground: float = wBackground
        self._wPerturbations: float = wPerturbations

        self._rho: float = rho
        self._tau: float = tau
        self._T_photon: float = T_photon

        self._d_lnH_dz: float = d_lnH_dz
        self._d2_lnH_dz2: float = d2_lnH_dz2
        self._d3_lnH_dz3: float = d3_lnH_dz3

        self._d_wPerturbations_dz: float = d_wPerturbations_dz
        self._d2_wPerturbations_dz2: float = d2_wPerturbations_dz2

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def Hubble(self) -> float:
        return self._Hubble

    @property
    def wBackground(self) -> float:
        return self._wBackground

    @property
    def wPerturbations(self) -> float:
        return self._wPerturbations

    @property
    def rho(self) -> float:
        return self._rho

    @property
    def tau(self) -> float:
        return self._tau

    @property
    def T_photon(self) -> float:
        return self._T_photon

    @property
    def d_lnH_dz(self) -> float:
        return self._d_lnH_dz

    @property
    def d2_lnH_dz2(self) -> Optional[float]:
        return self._d2_lnH_dz2

    @property
    def d3_lnH_dz3(self) -> Optional[float]:
        return self._d3_lnH_dz3

    @property
    def d_wPerturbations_dz(self) -> Optional[float]:
        return self._d_wPerturbations_dz

    @property
    def d2_wPerturbations_dz2(self) -> Optional[float]:
        return self._d2_wPerturbations_dz2


class ScalarModelProxy:
    def __init__(self, model: ScalarModel):
        self._ref: ObjectRef = ray.put(model)

        self._store_id: int = model.store_id if model.available else None

        self._units: UnitsLike = model.cosmology.units
        self._cosmology: BaseCosmology = model.cosmology

    @property
    def store_id(self) -> int:
        return self._store_id

    @property
    def available(self) -> bool:
        return self._store_id is not None

    @property
    def units(self) -> UnitsLike:
        return self._units

    @property
    def cosmology(self) -> BaseCosmology:
        return self._cosmology

    def get(self) -> ScalarModel:
        """
        The return value should only be held locally and not persisted, otherwise the entire
        ScalarModel instance may be serialized when it is passed around by Ray.
        That would defeat the purpose of the proxy.
        :return:
        """
        return ray.get(self._ref)
