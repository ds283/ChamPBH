from collections import namedtuple
from math import log, pi, exp, sqrt
from typing import Optional, List

import ray
from ray import ObjectRef
from scipy.integrate import solve_ivp
from scipy.interpolate import make_interp_spline

from ComputeTargets.spline_wrappers import ZSplineWrapper
from CosmologyConcepts import (
    redshift,
    temperature,
    TemperatureLike,
    redshift_array,
    GetTemperature,
    M_value,
    phi_value,
    pi_value,
    GetFieldValue,
    FieldLike,
)
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from CosmologyModels.GenericEOS.LambdaCDM_GenericEOS import LambdaCDM_GenericEOS
from Datastore import DatastoreObject
from MetadataConcepts import tolerance, store_tag
from Quadrature.integration_metadata import IntegrationSolver, IntegrationData
from Quadrature.supervisors.ScalarField import ScalarFieldIntegrationSupervisor
from Quadrature.supervisors.base import RHS_timer
from Units.base import UnitsLike
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE

# useful constants, calculated once and cached to speed up the numerical integration
PISQ_OVER_30 = pi * pi / 30.0
LOG_PISQ_OVER_30 = log(PISQ_OVER_30)

# use of named tuples ensures that we never get the fields of the state vector in the wrong order
StateVector = namedtuple(
    "StateVector",
    [
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_fm",
        "log_T_Jordan",
    ],
)
EXPECTED_SOL_LENGTH = 5

SampleValues = namedtuple(
    "SampleValues",
    [
        "raw_N",
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_rhorad_Jordan",
        "log_fm",
        "log_H_Einstein",
        "log_H_Jordan",
        "log_T_Jordan",
        "gstar_rho",
        "gstar_s",
        "Sigma",
    ],
)

ModelFunctions = namedtuple(
    "ModelFunctions",
    [
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_rhorad_Jordan",
        "log_fm",
        "log_H_Einstein",
        "log_H_Jordan",
        "log_T_Jordan",
        "gstar_rho",
        "gstar_s",
        "Sigma",
    ],
)


@ray.remote
def compute_scalar_model(
    cosmology: LambdaCDM_GenericEOS,
    T_init: TemperatureLike,
    T_stop: TemperatureLike,
    phi_init: FieldLike,
    pi_init: FieldLike,
    z_grid: redshift_array,
    potential: AbstractPotential,
    coupling: AbstractCoupling,
    task_label: str = "compute_scalar_model",
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
    units: UnitsLike = cosmology.units

    log_T_init: float = log(GetTemperature(T_init))
    log_T_stop: float = log(GetTemperature(T_stop))

    phi_init_float: float = GetFieldValue(phi_init)
    pi_init_float: float = GetFieldValue(pi_init)

    # compute initial Jordan frame radiation density at T_J = T_Jordan_init
    # rho = (pi^2 / 30) g* T^4
    log_rhorad_Jordan_init: float = (
        LOG_PISQ_OVER_30 + 4.0 * log_T_init + log(cosmology.G_rho(T_init))
    )

    # convert Jordan frame radiation density at T_J = T_Jordan_init to Einstein frame radiation density
    offset: float = 4.0 * coupling.log_Omega(phi_init_float)
    log_rhorad_Einstein_init: float = log_rhorad_Jordan_init + offset

    # estimate initial matter fraction at T_J = T_Jordan_init
    # f_m = rho_m
    z_init_estimate = cosmology.z(T_init)
    log_rho_m0: float = log(cosmology.rho_m0)
    log_rho_m_init: float = log_rho_m0 - 3.0 * log(1.0 + z_init_estimate)
    log_fm_init = log_rho_m_init - log_rhorad_Jordan_init
    assert log_fm_init < 0.0

    rhorad_Jordan_init: float = exp(log_rhorad_Einstein_init)
    rhorad_Jordan_init_14: float = pow(rhorad_Jordan_init, 1.0 / 4.0)

    rhomat_Jordan_init: float = exp(log_rho_m_init)
    rhomat_Jordan_init_14: float = pow(rhomat_Jordan_init, 1.0 / 4.0)

    fm_init = exp(log_fm_init)

    print(f"-- compute_scalar_model ({task_label}): initial data")
    print(
        f"    - T_Jordan_init = {GetTemperature(T_init)/units.GeV:.5g} GeV = {GetTemperature(T_init)/units.Kelvin:.5g} K"
    )
    print(f"    - rho_r_Jordan_init = ({rhorad_Jordan_init_14/units.GeV:.5g} GeV)^4")
    print(f"    - rho_m_Jordan_init = ({rhomat_Jordan_init_14/units.GeV:.5g} GeV)^4")
    print(f"    - f_m_init = {fm_init:.5g} | log(f_m_init) = {log_fm_init:.5g}")
    print(
        f"    - log_rho_r_Jordan_init = {log_rhorad_Jordan_init:.5g}, log_rho_r_Einstein_init = {log_rhorad_Einstein_init:.5g}"
    )

    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_6_MP_SQ = 6.0 * CONST_MP_SQ

    def RHS(N, s, supervisor) -> StateVector:
        with RHS_timer(supervisor) as timer:
            state: StateVector = StateVector._make(s)

            phi_Einstein: float = state.phi_Einstein
            pi_Einstein: float = state.pi_Einstein
            log_rhorad_Einstein: float = state.log_rhorad_Einstein
            log_fm: float = state.log_fm
            log_T_Jordan: float = state.log_T_Jordan

            rhorad_Einstein: float = exp(log_rhorad_Einstein)
            fm: float = exp(log_fm)
            T_Jordan: float = exp(log_T_Jordan)

            if supervisor.notify_available:
                supervisor.message(
                    T_Jordan,
                    f"current state: N={N:.3g}, T_Jordan = {T_Jordan/units.GeV:.5g} GeV = {T_Jordan/units.Kelvin:.5g} K, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp",
                )
                supervisor.reset_notify_time()

            V: float = potential.V(phi_Einstein)
            Vprime: float = potential.Vprime(phi_Einstein)

            log_Omega_prime: float = coupling.log_Omega_prime(phi_Einstein)

            G: float = 1.0 - pi_Einstein * pi_Einstein / CONST_6_MP_SQ

            H2_Mp2_Einstein: float = (rhorad_Einstein * (1.0 + fm) + V) / (G) / 3.0

            H2_Einstein: float = H2_Mp2_Einstein / CONST_MP_SQ

            Sigma: float = 1.0 - 3.0 * cosmology.w(T_Jordan)

            d_phi_Einstein: float = pi_Einstein
            d_log_rhorad_Einstein: float = Sigma - 4.0
            d_log_fm: float = 1.0 - Sigma

            A1: float = (2.0 + 3.0 * fm + Sigma) / (2.0 * (1.0 + fm))
            A2: float = (4.0 + 3.0 * fm - Sigma) / (1.0 + fm)
            A3: float = (Sigma + fm) / (1.0 + fm)
            C: float = V / (6.0 * H2_Mp2_Einstein)
            D: float = Vprime / H2_Einstein
            E: float = (
                1.0
                - pi_Einstein * pi_Einstein / CONST_6_MP_SQ
                - V / (3.0 * H2_Mp2_Einstein)
            )

            d_pi_Einstein: float = (
                -pi_Einstein * (G * A1 + C * A2)
                - D
                - 3.0 * CONST_MP_SQ * G * E * log_Omega_prime * A3
            )

            G_s: float = cosmology.G_s(T_Jordan)
            dG_s: float = cosmology.dG_s_dT(T_Jordan)
            d_log_T_Jordan: float = -(1.0 + log_Omega_prime * pi_Einstein) / (
                1.0 + (T_Jordan / G_s) * dG_s / 3.0
            )

            return StateVector(
                phi_Einstein=d_phi_Einstein,
                pi_Einstein=d_pi_Einstein,
                log_rhorad_Einstein=d_log_rhorad_Einstein,
                log_fm=d_log_fm,
                log_T_Jordan=d_log_T_Jordan,
            )

    # termination occurs when the Jordan frame temperature hits T_Jordan_stop, usually equal to T_CMB,
    # so the actual stop value given in t_span is mostly irrelevant, just
    # to ensure that the integration terminates
    def terminate_at_T_stop(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.log_T_Jordan - log_T_stop

    terminate_at_T_stop.terminal = True
    terminate_at_T_stop.direction = (
        -1.0
    )  # only trigger when going from positive to negative, i.e., when the temperature dips *below* T_Jordan_stop

    with ScalarFieldIntegrationSupervisor(
        units, T_init, T_stop, label=task_label
    ) as supervisor:
        initial_state = StateVector(
            phi_Einstein=phi_init_float,
            pi_Einstein=pi_init_float,
            log_rhorad_Einstein=log_rhorad_Einstein_init,
            log_fm=log_fm_init,
            log_T_Jordan=log_T_init,
        )

        sol = solve_ivp(
            RHS,
            method="Radau",
            t_span=(0.0, 1000.0),
            y0=initial_state,
            atol=atol,
            rtol=rtol,
            args=(supervisor,),
            events=(terminate_at_T_stop,),
            dense_output=True,
        )

    if not sol.success:
        raise RuntimeError(
            f'compute_scalar_model ({task_label}): integration did not terminate successfully (log_T_init={log_T_init:.5g}, log_T_stop={log_T_stop:.5g}, error at N={sol.t[-1]:.5g}, "{sol.message}")'
        )

    if not sol.status == 1:
        raise RuntimeError(
            f'compute_scalar_model ({task_label}): expected termination to occur at T_Jordan_stop (log_T_init={log_T_init:.5g}, log_T_stop={log_T_stop:.5g}, last sample at N={sol.t[-1]:.5g}, "{sol.message}")'
        )

    sampled_N = sol.t
    sampled_values = StateVector._make(sol.y)
    if len(sampled_values) != EXPECTED_SOL_LENGTH:
        raise RuntimeError(
            f"compute_scalar_model ({task_label}): solution does not have expected number of members (expected {EXPECTED_SOL_LENGTH}, found {len(sampled_values)}; length of sol.t={len(sampled_N)})"
        )

    # the integration should have terminated when T_Jordan = T_CMB, which ought to correspond to z = 0
    # we now work backwards and sample the integration output on the supplied z grid, using the e-fold number
    # to assign a value of log(1 + z).
    final_N = sol.t[-1]
    largest_z = exp(final_N) - 1.0
    z_grid_cut = z_grid.truncate(largest_z, keep="lower")

    max_z: redshift = z_grid.max
    max_N: float = log(1.0 + max_z.z)
    if max_N < final_N:
        raise RuntimeError(
            f"compute_scalar_model: ({task_label}): largest supplied redshift z={max_z.z:.3g} is equivalent to maximum e-fold number N={max_N:.3g}, but solution required N={final_N:.3g} e-folds"
        )

    sample = []

    for z in z_grid_cut:
        z: redshift
        N_backward = log(1.0 + z.z)
        N_forward = final_N - N_backward

        state: StateVector = StateVector._make(sol.sol(N_forward))

        log_Omega: float = coupling.log_Omega(state.phi_Einstein)
        log_Omega_prime: float = coupling.log_Omega_prime(state.phi_Einstein)
        offset: float = 4.0 * log_Omega
        log_rhorad_Jordan: float = state.log_rhorad_Einstein - offset

        V: float = potential.V(state.phi_Einstein)
        G: float = 1.0 - state.pi_Einstein * state.pi_Einstein / CONST_6_MP_SQ
        rhorad_Einstein: float = exp(state.log_rhorad_Einstein)
        fm: float = exp(state.log_fm)

        H2_Mp2_Einstein: float = (rhorad_Einstein * (1.0 + fm) + V) / (G) / 3.0
        H2_Einstein: float = H2_Mp2_Einstein / CONST_MP_SQ
        log_H_Einstein: float = log(sqrt(H2_Einstein))
        log_H_Jordan: float = (
            log_H_Einstein - log_Omega + log(1.0 + log_Omega_prime * state.pi_Einstein)
        )

        T_Jordan: float = exp(state.log_T_Jordan)

        sample.append(
            SampleValues(
                raw_N=N_forward,
                phi_Einstein=state.phi_Einstein,
                pi_Einstein=state.pi_Einstein,
                log_rhorad_Einstein=state.log_rhorad_Einstein,
                log_rhorad_Jordan=log_rhorad_Jordan,
                log_fm=state.log_fm,
                log_T_Jordan=state.log_T_Jordan,
                log_H_Einstein=log_H_Einstein,
                log_H_Jordan=log_H_Jordan,
                gstar_rho=cosmology.G_rho(T_Jordan),
                gstar_s=cosmology.G_s(T_Jordan),
                Sigma=1.0 - 3.0 * cosmology.w(T_Jordan),
            )
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
        "z_grid": z_grid_cut,
        "sample": sample,
        "solver_label": "solve_ivp+Radau-stepping0",
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
        T_Jordan_init: temperature,  # initial Jordan-frame temperature
        T_Jordan_stop: temperature,  # Jordan-frame temperature at which to terminate the calculation
        phi_Einstein_init: phi_value,  # initial value of Einstein-frame scalar phi
        pi_Einstein_init: pi_value,  # initial value of dphi/dN
        potential: AbstractPotential,
        coupling: AbstractCoupling,
        atol: tolerance,
        rtol: tolerance,
        z_grid: Optional[redshift_array] = None,
        label: Optional[str] = None,
        tags: Optional[List[store_tag]] = None,
    ):
        self._solver_labels = solver_labels

        self._T_Jordan_init: temperature = T_Jordan_init
        self._T_Jordan_stop: temperature = T_Jordan_stop

        self._phi_Einstein_init: phi_value = phi_Einstein_init
        self._pi_Einstein_init: pi_value = pi_Einstein_init

        self._potential: AbstractPotential = potential
        self._coupling: AbstractCoupling = coupling

        self._target_z_grid: Optional[redshift_array] = z_grid

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
    def shard_key(self) -> M_value:
        return self._potential.shard_key

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
    def T_Jordan_init(self) -> temperature:
        return self._T_Jordan_init

    @property
    def T_Jordan_stop(self) -> temperature:
        return self._T_Jordan_stop

    @property
    def phi_Einstein_init(self) -> phi_value:
        return self._phi_Einstein_init

    @property
    def pi_Einstein_init(self) -> pi_value:
        return self._pi_Einstein_init

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
            data = [(v.z.z, getattr(v, attr)) for v in self.values]
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
        self._functions = ModelFunctions(
            phi_Einstein=_build_func("phi_Einstein"),
            pi_Einstein=_build_func("pi_Einstein"),
            log_rhorad_Einstein=_build_func("log_rhorad_Einstein"),
            log_rhorad_Jordan=_build_func("log_rhorad_Jordan"),
            log_fm=_build_func("log_fm"),
            log_H_Einstein=_build_func("log_H_Einstein"),
            log_H_Jordan=_build_func("log_H_Jordan"),
            log_T_Jordan=_build_func("log_T_Jordan"),
            gstar_rho=_build_func("gstar_rho"),
            gstar_s=_build_func("gstar_s"),
            Sigma=_build_func("Sigma"),
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

        check_required_parameter("_T_Jordan_init")
        check_required_parameter("_T_Jordan_stop")
        check_required_parameter("_phi_Einstein_init")
        check_required_parameter("_pi_Einstein_init")
        check_required_parameter("_potential")
        check_required_parameter("_coupling")
        check_required_parameter("_target_z_grid")

        # replace label if specified
        if label is not None:
            self._label = label

        self._compute_ref = compute_scalar_model.remote(
            self.cosmology,
            self.T_Jordan_init,
            self.T_Jordan_stop,
            self.phi_Einstein_init,
            self.pi_Einstein_init,
            self._target_z_grid,
            self.potential,
            self.coupling,
            task_label=(
                self._label
                if self._label is not None
                else f"{self.potential.name}-{self.coupling.name}"
            ),
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

        sample: List[SampleValues] = data["sample"]
        z_grid: redshift_array = data["z_grid"]

        self._values = []
        for i in range(len(sample)):
            self._values.append(
                ScalarModelValue(
                    None,
                    z_grid[i],
                    raw_N=sample[i].N,
                    phi_Einstein=sample[i].phi_Einstein,
                    pi_Einstein=sample[i].pi_Einstein,
                    log_rhorad_Einstein=sample[i].log_rhorad_Einstein,
                    log_rhorad_Jordan=sample[i].log_rhorad_Jordan,
                    log_fm=sample[i].log_fm,
                    log_T_Jordan=sample[i].log_T_Jordan,
                    log_H_Einstein=sample[i].log_H_Einstein,
                    log_H_Jordan=sample[i].log_H_Jordan,
                    gstar_rho=sample[i].gstar_rho,
                    gstar_s=sample[i].gstar_s,
                    Sigma=sample[i].Sigma,
                )
            )

        self._solver = self._solver_labels[data["solver_label"]]

        return True


class ScalarModelValue(DatastoreObject):
    def __init__(
        self,
        store_id: int,
        z: redshift,
        raw_N: float,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        log_rhorad_Jordan: float,
        log_fm: float,
        log_T_Jordan: float,
        log_H_Einstein: float,
        log_H_Jordan: float,
        gstar_rho: float,
        gstar_s: float,
        Sigma: float,
    ):
        DatastoreObject.__init__(self, store_id)

        self._z: float = z
        self._raw_N: float = raw_N

        self._phi_Einstein: float = phi_Einstein
        self._pi_Einstein: float = pi_Einstein

        self._log_H_Einstein: float = log_H_Einstein
        self._log_H_Jordan: float = log_H_Jordan

        self._log_rhorad_Einstein: float = log_rhorad_Einstein
        self._log_rhorad_Jordan: float = log_rhorad_Jordan
        self._log_fm: float = log_fm
        self._log_T_Jordan: float = log_T_Jordan

        self._gstar_rho: float = gstar_rho
        self._gstar_s: float = gstar_s
        self._Sigma: float = Sigma

    @property
    def shard_key(self) -> M_value:
        # should not get called individually, since serialization is handled by the parent ScalarModel
        return NotImplementedError

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def raw_N(self) -> float:
        return self._raw_N

    @property
    def log_H_Einstein(self) -> float:
        return self._log_H_Einstein

    @property
    def log_H_Jordan(self) -> float:
        return self._log_H_Jordan

    @property
    def phi_Einstein(self) -> float:
        return self._phi_Einstein

    @property
    def pi_Einstein(self) -> float:
        return self._pi_Einstein

    @property
    def log_rhorad_Einstein(self) -> float:
        return self._log_rhorad_Einstein

    @property
    def log_rhorad_Jordan(self) -> float:
        return self._log_rhorad_Jordan

    @property
    def log_fm(self) -> float:
        return self._log_fm

    @property
    def log_T_Jordan(self) -> float:
        return self._log_T_Jordan

    @property
    def gstar_rho(self) -> float:
        return self._gstar_rho

    @property
    def gstar_s(self) -> float:
        return self._gstar_s

    @property
    def Sigma(self) -> float:
        return self._Sigma


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
