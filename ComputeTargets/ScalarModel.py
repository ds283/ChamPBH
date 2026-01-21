from collections import namedtuple
from math import log, pi, exp, sqrt, isinf, isnan
from typing import Optional, List, Dict, Any

import ray
from numpy import inf as np_inf
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
from Quadrature.supervisors.ScalarField import (
    ScalarFieldIntegrationSupervisor,
    StateVector,
)
from Quadrature.supervisors.base import RHS_timer
from Units.base import UnitsLike
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE
from config.sharding import ShardKeyType

# useful constants, calculated once and cached to speed up the numerical integration
PISQ_OVER_30 = pi * pi / 30.0
LOG_PISQ_OVER_30 = log(PISQ_OVER_30)

EXPECTED_SOL_LENGTH = 5

# using named tuples ensures that we never get the fields in the wrong order
ODE_data = namedtuple(
    "ODE_data",
    [
        "fm",
        "T_Jordan",
        "Sigma",
        "log_V",
        "d_logV_dphi",
        "V_over_3H2Mp2",
        "Vprime_over_3H2Mp2",
        "log_Omega_prime",
        "friction_term",
        "reflecting_term",
        "kicking_term",
    ],
)

SolutionFragment = namedtuple(
    "SolutionFragment",
    [
        "N_low",
        "N_high",
        "sol",
    ],
)

SampleValues = namedtuple(
    "SampleValues",
    [
        "raw_N",
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_rhorad_Jordan",
        "log_fm",
        "H_Einstein",
        "H_Jordan",
        "log_T_Jordan",
        "gstar_rho",
        "gstar_s",
        "dgstar_s_dlogT",
        "dgstar_rho_dlogT",
        "Sigma",
        "friction_term",
        "reflecting_term",
        "kicking_term",
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
        "H_Einstein",
        "H_Jordan",
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
    verbose: bool = False,
) -> dict:
    """
    :param cosmology: background cosmology
    :param T_init: initial radiation temperature in Jordan frame
    :param T_stop: final radiation temperature in Jordan frame (usually T_CMB)
    :param phi_init: initial Einstein-frame scalar field value phi
    :param pi_init: initial Einstein-frame scalar field derivative dphi/dN
    :param z_grid: grid of redshifts at which to sample the solution
    :param potential: the scalar field potential
    :param coupling: the conformal coupling
    :param task_label: label for the ray task
    :param atol: absolute tolerance for the ODE solver
    :param rtol: relative tolerance for the ODE solver
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
    log_rhorad_Einstein_init: float = log_rhorad_Jordan_init + 4.0 * coupling.log_Omega(
        phi_init_float
    )

    # estimate initial matter fraction at T_J = T_Jordan_init
    # f_m = rho_m
    z_init_estimate = cosmology.z(T_init)
    log_rho_m0: float = log(cosmology.rho_m0)
    log_rho_m_init: float = log_rho_m0 + 3.0 * log(1.0 + z_init_estimate)
    log_fm_init = log_rho_m_init - log_rhorad_Jordan_init
    assert log_fm_init < 0.0

    # rhorad_Jordan_init: float = exp(log_rhorad_Einstein_init)
    # rhorad_Jordan_init_14: float = pow(rhorad_Jordan_init, 1.0 / 4.0)

    # rhomat_Jordan_init: float = exp(log_rho_m_init)
    # rhomat_Jordan_init_14: float = pow(rhomat_Jordan_init, 1.0 / 4.0)

    # fm_init = exp(log_fm_init)

    # print(f"-- compute_scalar_model ({task_label}): initial data")
    # print(
    #     f"    - T_Jordan_init = {GetTemperature(T_init)/units.GeV:.5g} GeV = {GetTemperature(T_init)/units.Kelvin:.5g} K"
    # )
    # print(f"    - rho_r_Jordan_init = ({rhorad_Jordan_init_14/units.GeV:.5g} GeV)^4")
    # print(f"    - rho_m_Jordan_init = ({rhomat_Jordan_init_14/units.GeV:.5g} GeV)^4")
    # print(f"    - f_m_init = {fm_init:.5g} | log(f_m_init) = {log_fm_init:.5g}")
    # print(
    #     f"    - log_rho_r_Jordan_init = {log_rhorad_Jordan_init:.5g}, log_rho_r_Einstein_init = {log_rhorad_Einstein_init:.5g}"
    # )

    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_6_MP_SQ = 6.0 * CONST_MP_SQ

    def RHS_impl(N: float, state: StateVector):
        if any((isnan(x) or isinf(x)) for x in state):
            print(
                f"!! compute_scalar_model ({task_label}): input to ODE RHS has infinity or NaN values at N={N:.8g}"
            )
            print(f"     - state={state}")
            raise RuntimeError(
                f"compute_scalar_model ({task_label}): input to ODE RHS has infinity or NaN values"
            )

        phi_Einstein: float = state.phi_Einstein
        pi_Einstein: float = state.pi_Einstein
        log_rhorad_Einstein: float = state.log_rhorad_Einstein
        log_fm: float = state.log_fm
        log_T_Jordan: float = state.log_T_Jordan

        try:
            fm: float = exp(log_fm)
        except OverflowError as e:
            print(
                f"!! compute_scalar_model ({task_label}): math overflow in exp(log_fm), log_fm = {log_fm:.5g} | N = {N:.5g}, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp, pi_Einstein = {pi_Einstein / units.PlanckMass:.5g} Mp"
            )
            raise e

        try:
            T_Jordan: float = exp(log_T_Jordan)
        except OverflowError as e:
            print(
                f"!! compute_scalar_model ({task_label}): math overflow in exp(log_T_Jordan), log_T_Jordan = {log_T_Jordan:.5g} | N = {N:.5g}, f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp, pi_Einstein = {pi_Einstein / units.PlanckMass:.5g} Mp"
            )
            raise e

        if T_Jordan <= 0.0:
            print(
                f"!! compute_scalar_model ({task_label}): T_Jordan = {T_Jordan:.5g}, log_T_Jordan = {log_T_Jordan:.5g} at N={N:.8g}"
            )
            T_Jordan = 1 * units.Kelvin

        log_Omega_prime: float = coupling.log_Omega_prime(phi_Einstein)

        G: float = 1.0 - pi_Einstein * pi_Einstein / CONST_6_MP_SQ
        # G must be positive in order the H_Einstein^2 is also positive
        if G < 0.0:
            print(
                f"!! compute_scalar_model ({task_label}): negative value of G = {G:.5g} detected at N={N:.8g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp, pi_Einstein = {pi_Einstein / units.PlanckMass:.5g} Mp, log(rho_rad/GeV^4) = {log_rhorad_Einstein - 4.0*log(units.GeV):.5g}, T_Jordan = {T_Jordan/units.GeV:.5g} GeV = {T_Jordan/units.Kelvin:.5g} K"
            )
            raise RuntimeError(
                f"compute_scalar_model ({task_label}): negative value of G = {G:.5g} detected"
            )

        Sigma: float = 1.0 - 3.0 * cosmology.w(T_Jordan)

        R: float
        if fm > 10.0:
            R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        else:
            R = (Sigma + fm) / (1.0 + fm)
        A1: float = 1.0 + R / 2.0
        A2: float = 4.0 - R

        log_V: float = potential.log_V(phi_Einstein)
        d_logV_dphi: float = potential.d_logV_dphi(phi_Einstein)

        log_rhorad_over_V: float = log_rhorad_Einstein - log_V
        T: float
        if log_rhorad_over_V > 2.0:
            V_over_rhorad: float = exp(-log_rhorad_over_V)
            T = V_over_rhorad / (V_over_rhorad + 1.0 + fm)
        else:
            rhorad_over_V: float = exp(log_rhorad_over_V)
            T = 1.0 / (1.0 + rhorad_over_V * (1.0 + fm))

        V_over_3H2Mp2: float = G * T
        Vprime_over_3H2Mp2: float = G * d_logV_dphi * T

        C: float = V_over_3H2Mp2 / 2.0
        D: float = 3.0 * CONST_MP_SQ * Vprime_over_3H2Mp2
        E: float = G - V_over_3H2Mp2
        # must be positive, because it is proportional to rho_R/H^2
        if E < 0.0:
            print(
                f"!! compute_scalar_model ({task_label}): negative value of E = {E:.5g} detected at N={N:.8g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp, pi_Einstein = {pi_Einstein / units.PlanckMass:.5g} Mp, log(rho_rad/GeV^4) = {log_rhorad_Einstein - 4.0*log(units.GeV):.5g}, T_Jordan = {T_Jordan/units.GeV:.5g} GeV = {T_Jordan/units.Kelvin:.5g} K, G = {G:.5g}, T = {T:.5g}"
            )
            raise RuntimeError(
                f"compute_scalar_model ({task_label}): negative value of E = {E:.5g} detected"
            )

        friction_term = -pi_Einstein * (G * A1 + C * A2)
        reflecting_term = -D
        kicking_term = -3.0 * CONST_MP_SQ * E * log_Omega_prime * R
        return ODE_data(
            fm=fm,
            T_Jordan=T_Jordan,
            Sigma=Sigma,
            log_V=log_V,
            d_logV_dphi=d_logV_dphi,
            V_over_3H2Mp2=V_over_3H2Mp2,
            Vprime_over_3H2Mp2=Vprime_over_3H2Mp2,
            log_Omega_prime=log_Omega_prime,
            friction_term=friction_term,
            reflecting_term=reflecting_term,
            kicking_term=kicking_term,
        )

    def RHS(N, s, supervisor) -> StateVector:
        with RHS_timer(supervisor) as timer:
            state: StateVector = StateVector._make(s)
            data: ODE_data = RHS_impl(N, state)

            phi_Einstein: float = state.phi_Einstein
            pi_Einstein: float = state.pi_Einstein
            log_rhorad_Einstein: float = state.log_rhorad_Einstein

            fm: float = data.fm
            T_Jordan: float = data.T_Jordan

            if supervisor.notify_available:
                supervisor.message(
                    T_Jordan,
                    f"current state: N={N:.5g}, f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / units.PlanckMass:.5g} Mp, pi_Einstein = {pi_Einstein / units.PlanckMass:.5g} Mp, log(rho_rad/GeV^4) = {log_rhorad_Einstein - 4.0*log(units.GeV):.5g}, T_Jordan = {T_Jordan/units.GeV:.5g} GeV = {T_Jordan/units.Kelvin:.5g} K",
                )
                supervisor.reset_notify_time(T_Jordan)

            d_phi_Einstein: float = pi_Einstein
            d_pi_Einstein: float = (
                data.friction_term + data.reflecting_term + data.kicking_term
            )

            d_log_rhorad_Einstein: float = (
                data.Sigma - 4.0 + data.Sigma * data.log_Omega_prime * pi_Einstein
            )
            d_log_fm: float = (1.0 - data.Sigma) * (
                1.0 + data.log_Omega_prime * pi_Einstein
            )

            G_s: float = cosmology.G_s(T_Jordan)
            dG_s_dlogT: float = cosmology.dG_s_dlogT(T_Jordan)

            d_log_T_Jordan: float = -(1.0 + data.log_Omega_prime * pi_Einstein) / (
                1.0 + dG_s_dlogT / G_s / 3.0
            )

            return_state = StateVector(
                phi_Einstein=d_phi_Einstein,
                pi_Einstein=d_pi_Einstein,
                log_rhorad_Einstein=d_log_rhorad_Einstein,
                log_fm=d_log_fm,
                log_T_Jordan=d_log_T_Jordan,
            )

            if any((isnan(x) or isinf(x)) for x in return_state):
                log_fm: float = state.log_fm
                log_T_Jordan: float = state.log_T_Jordan

                Sigma: float = data.Sigma

                log_V: float = data.log_V
                d_logV_dphi: float = data.d_logV_dphi

                V_over_3H2Mp2: float = data.V_over_3H2Mp2
                Vprime_over_3H2Mp2: float = data.Vprime_over_3H2Mp2

                print(
                    f"!! compute_scalar_model ({task_label}): output from ODE RHS has infinity or NaN values at N={N:.8g}"
                )
                print(
                    f"     - inputs/states: phi_E={phi_Einstein/units.PlanckMass:.5g} Mp, pi_E={pi_Einstein/units.PlanckMass:.5g} Mp, log_rhorad_E={log_rhorad_Einstein:.5g}, log_fm={log_fm:.5g}, log_T_J={log_T_Jordan:.5g}"
                )
                print(
                    f"     - physical: log(rhorad_E/GeV^4)={log_rhorad_Einstein - 4.0*log(units.GeV):.5g}, fm={fm:.5g}, T_J={T_Jordan/units.GeV:.5g} GeV = {T_Jordan/units.Kelvin:.5g} K"
                )
                print(
                    f"     - potential: log(V/GeV^4)={log_V - 4.0*log(units.GeV):.5g}, V'/V={d_logV_dphi*units.GeV:.5g} GeV^(-1), log_Omega'={data.log_Omega_prime:.5g}"
                )
                print(
                    f"     - cosmology: V/3H2Mp2={V_over_3H2Mp2:.5g}, V'/3H2Mp2={Vprime_over_3H2Mp2:.5g}, Sigma={Sigma:.5g}"
                )
                # print(
                #     f"     - intermediates: G={G:.5g}, T={T:.5g}, A1={A1:.5g}, A2={A2:.5g}, R={R:.5g}, C={C:.5g}, D={D:.5g}, E={E:.5g}"
                # )
                print(
                    f"     - derivatives: d_phi_E={d_phi_Einstein:.5g}, d_pi_E={d_pi_Einstein:.5g}, d_log_rhorad_E={d_log_rhorad_Einstein:.5g}, d_log_fm={d_log_fm:.5g}, d_log_T_J={d_log_T_Jordan:.5g}"
                )
                print(f"     - thermodynamics: G_s={G_s:.5g}, dG_s={dG_s_dlogT:.5g}")
                print(f"     - state={state}")
                print(f"     - return_state={return_state}")
                raise RuntimeError(
                    f"compute_scalar_model ({task_label}): output from ODE RHS has infinity or NaN values"
                )

            supervisor.notify_new_RHS(return_state)

            return return_state

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

    hard_reflection_point: float = potential.hard_reflection_point

    # detect failures to reflect at the chameleon "brick wall" at the origin
    def reflection_failure_detector(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.phi_Einstein - hard_reflection_point

    reflection_failure_detector.terminal = True
    reflection_failure_detector.direction = (
        -1.0
    )  # only trigger when phi_Einstein crosses from positive to negative values

    # detect entry/exit from region close a bounce
    bounce_region_level1_boundary = potential.bounce_region_level1_boundary
    bounce_region_level2_boundary = potential.bounce_region_level2_boundary

    def enter_bounce_region_level1(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.phi_Einstein - bounce_region_level1_boundary

    enter_bounce_region_level1.terminal = True
    enter_bounce_region_level1.direction = -1.0

    def exit_bounce_region_level1(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.phi_Einstein - bounce_region_level1_boundary

    exit_bounce_region_level1.terminal = True
    exit_bounce_region_level1.direction = +1.0

    def enter_bounce_region_level2(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.phi_Einstein - bounce_region_level2_boundary

    enter_bounce_region_level2.terminal = True
    enter_bounce_region_level2.direction = -1.0

    def exit_bounce_region_level2(N, s, supervisor) -> float:
        state: StateVector = StateVector._make(s)

        return state.phi_Einstein - bounce_region_level2_boundary

    exit_bounce_region_level2.terminal = True
    exit_bounce_region_level2.direction = +1.0

    in_level1 = False
    in_level2 = False

    def dummy_event_handler(N, s, supervisor):
        return 1.0

    N_failsafe = 1000.0  # terminate after 1000 e-folds as a failsafe
    N_start = 0.0

    # track number of function evaluations reported by the integrator
    compute_steps = 0

    # track the solution fragments generated between reflection events
    solution_fragments = []
    solution_complete = False

    # prepare the first initial state
    initial_state = StateVector(
        phi_Einstein=phi_init_float,
        pi_Einstein=pi_init_float,
        log_rhorad_Einstein=log_rhorad_Einstein_init,
        log_fm=log_fm_init,
        log_T_Jordan=log_T_init,
    )

    # track maximum step size
    # usually we want this to be unbounded, so the stepper can choose as large a value as it wishes
    # but close to a bounce we want to dramatically shorten the step size, so that we resolve the bounce accurately
    max_step_size = np_inf

    with ScalarFieldIntegrationSupervisor(
        units,
        T_init,
        T_stop,
        max_step_size,
        label=task_label,
        collect_full_statistics=True,
    ) as supervisor:
        while not solution_complete:
            sol = solve_ivp(
                RHS,
                method="Radau",
                t_span=(N_start, N_failsafe),
                y0=initial_state,
                atol=atol,
                rtol=rtol,
                args=(supervisor,),
                events=(
                    terminate_at_T_stop,
                    reflection_failure_detector,
                    (
                        enter_bounce_region_level1
                        if not in_level1
                        else dummy_event_handler
                    ),
                    exit_bounce_region_level1 if in_level1 else dummy_event_handler,
                    (
                        enter_bounce_region_level2
                        if not in_level2
                        else dummy_event_handler
                    ),
                    exit_bounce_region_level2 if in_level2 else dummy_event_handler,
                ),
                dense_output=True,
                max_step=max_step_size,
            )

            # check that termination occurred due to reaching the end of the integration domain, or because of a termination event
            if not sol.success:
                raise RuntimeError(
                    f'compute_scalar_model ({task_label}): integration did not terminate successfully (log_T_init={log_T_init:.5g}, log_T_stop={log_T_stop:.5g}, error at N={sol.t[-1]:.5g}, "{sol.message}")'
                )

            # check that termination was not due to reaching the end of the integraton domain (that is supposed to be just a failsafe)
            if not sol.status == 1:
                raise RuntimeError(
                    f'compute_scalar_model ({task_label}): integration concluded without a termination event => failsafe activated (log_T_init={log_T_init:.5g}, log_T_stop={log_T_stop:.5g}, last sample at N={sol.t[-1]:.5g}, "{sol.message}")'
                )

            # check that the solution has the expected number of elements
            sampled_N = sol.t
            sampled_values = StateVector._make(sol.y)
            if len(sampled_values) != EXPECTED_SOL_LENGTH:
                raise RuntimeError(
                    f"compute_scalar_model ({task_label}): solution does not have expected number of members (expected {EXPECTED_SOL_LENGTH}, found {len(sampled_values)}; length of sol.t={len(sampled_N)})"
                )

            # update running number of function evaluations (recorded by integrator)
            compute_steps += int(sol.nfev)

            # at this stage, we know that one of the event handlers fired to terminate the evolution
            # now we decide which one

            termination_times = sol.t_events[0]
            reflection_times = sol.t_events[1]
            enter_bounce_region_level1_times = sol.t_events[2]
            exit_bounce_region_level1_times = sol.t_events[3]
            enter_bounce_region_level2_times = sol.t_events[4]
            exit_bounce_region_level2_times = sol.t_events[5]

            num_termination_events = len(termination_times)
            num_reflection_events = len(reflection_times)
            num_enter_level1_events = len(enter_bounce_region_level1_times)
            num_exit_level1_events = len(exit_bounce_region_level1_times)
            num_enter_level2_events = len(enter_bounce_region_level2_times)
            num_exit_level2_events = len(exit_bounce_region_level2_times)

            if (
                num_termination_events
                + num_reflection_events
                + num_enter_level1_events
                + num_exit_level1_events
                + num_enter_level2_events
                + num_exit_level2_events
                != 1
            ):
                raise RuntimeError(
                    f"compute_scalar_model ({task_label}): integration terminated with multiple termination events (N_start={N_start:.5g}, N_failsafe={N_failsafe:.5g}, num_termination_events={num_termination_events}, num_reflection_events={num_reflection_events})"
                )

            # append this solution fragment to this list
            solution_fragments.append(
                SolutionFragment(
                    N_low=N_start,
                    N_high=sol.t[-1],
                    sol=sol.sol,
                )
            )

            # if the integration terminated, break out
            if num_termination_events == 1:
                solution_complete = True
                continue

            # if we need to restart the integration, prepare a new state
            if (
                num_reflection_events == 1
                or num_enter_level1_events == 1
                or num_exit_level1_events == 1
                or num_enter_level2_events == 1
                or num_exit_level2_events == 1
            ):
                # prepare to restart the integration
                N_start = sol.t[-1]
                initial_state = StateVector._make(sol.y[:, -1])

                if num_enter_level1_events == 1:
                    max_step_size = potential.bounce_region_level1_max_step
                    supervisor.notify_level_1_entry(
                        enter_bounce_region_level1_times[0], max_step_size
                    )
                    in_level1 = True
                    in_level2 = False
                    if verbose:
                        print(
                            f"-- compute_scalar_model ({task_label}): enter level-1 bounce region at N={enter_bounce_region_level1_times[0]:.5g}, reducing max step size to {max_step_size:.5g}"
                        )

                if num_exit_level1_events == 1:
                    max_step_size = np_inf
                    supervisor.notify_level_1_exit(
                        exit_bounce_region_level1_times[0], max_step_size
                    )
                    in_level1 = False
                    in_level2 = False
                    if verbose:
                        print(
                            f"-- compute_scalar_model ({task_label}): exit level-1 bounce region at N={exit_bounce_region_level1_times[0]:.5g}, increasing max step size to {max_step_size:.5g}"
                        )

                if num_enter_level2_events == 1:
                    max_step_size = potential.bounce_region_level2_max_step
                    supervisor.notify_level_2_entry(
                        enter_bounce_region_level2_times[0], max_step_size
                    )
                    in_level1 = True
                    in_level2 = True
                    if verbose:
                        print(
                            f"-- compute_scalar_model ({task_label}): enter level-2 bounce region at N={enter_bounce_region_level2_times[0]:.5g}, reducing max step size to {max_step_size:.5g}"
                        )

                if num_exit_level2_events == 1:
                    max_step_size = potential.bounce_region_level1_max_step
                    supervisor.notify_level_2_exit(
                        exit_bounce_region_level2_times[0], max_step_size
                    )
                    in_level1 = True
                    in_level2 = False
                    if verbose:
                        print(
                            f"-- compute_scalar_model ({task_label}): exit level-2 bounce region at N={exit_bounce_region_level2_times[0]:.5g}, increasing max step size to {max_step_size:.5g}"
                        )

                if num_reflection_events == 1:
                    # record that a hard reflection occurred and prepare for the next integration step
                    supervisor.notify_hard_reflection(reflection_times[0])

                    # reverse direction of travel for the scalar field
                    if initial_state.pi_Einstein < 0.0:
                        initial_state = initial_state._replace(
                            pi_Einstein=-initial_state.pi_Einstein
                        )
                    assert initial_state.pi_Einstein >= 0.0

                    if verbose:
                        print(
                            f"-- compute_scalar_model ({task_label}): hard reflection detected at N={reflection_times[0]:.5g}"
                        )

                continue

            # otherwise, we don't know what happened, so we should never reach this point
            raise RuntimeError(
                f"compute_scalar_model ({task_label}): integration terminated with unknown events (N_start={N_start:.5g}, N_failsafe={N_failsafe:.5g}"
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

    # loop over the required z sample grid.
    # Note that we will work from high z to low z.

    num_fragments = len(solution_fragments)
    current_fragment = solution_fragments.pop(0)
    for z in z_grid_cut:
        z: redshift
        N_backward = log(1.0 + z.z)
        N_forward = final_N - N_backward

        if N_forward > current_fragment.N_high:
            while N_forward > current_fragment.N_high:
                current_fragment = solution_fragments.pop(0)

        if N_forward < current_fragment.N_low:
            raise RuntimeError(
                f"compute_scalar_model: ({task_label}): z={z.z:.3g} appears to be out-of-order relative to the solution fragments"
            )

        state: StateVector = StateVector._make(current_fragment.sol(N_forward))
        data: ODE_data = RHS_impl(N_forward, state)

        T_Jordan: float = data.T_Jordan

        log_V_over_3H2Mp2: float = log(data.V_over_3H2Mp2)
        log_3H2Mp2: float = -1.0 * (log_V_over_3H2Mp2 - data.log_V)
        H2_Einstein: float = exp(log_3H2Mp2) / 3.0 / CONST_MP_SQ
        H_Einstein: float = sqrt(H2_Einstein)

        log_Omega: float = coupling.log_Omega(state.phi_Einstein)
        log_rhorad_Jordan: float = state.log_rhorad_Einstein - 4.0 * log_Omega

        Omega: float = coupling.Omega(state.phi_Einstein)

        # H_Jordan can even be negative, so there is no use trying to store its logarithm
        H_Jordan: float = (
            H_Einstein * (1.0 + data.log_Omega_prime * state.pi_Einstein) / Omega
        )

        sample.append(
            SampleValues(
                raw_N=N_forward,
                phi_Einstein=state.phi_Einstein,
                pi_Einstein=state.pi_Einstein,
                log_rhorad_Einstein=state.log_rhorad_Einstein,
                log_rhorad_Jordan=log_rhorad_Jordan,
                log_fm=state.log_fm,
                log_T_Jordan=state.log_T_Jordan,
                H_Einstein=H_Einstein,
                H_Jordan=H_Jordan,
                gstar_rho=cosmology.G_rho(T_Jordan),
                gstar_s=cosmology.G_s(T_Jordan),
                dgstar_rho_dlogT=cosmology.dG_rho_dlogT(T_Jordan),
                dgstar_s_dlogT=cosmology.dG_s_dlogT(T_Jordan),
                Sigma=1.0 - 3.0 * cosmology.w(T_Jordan),
                friction_term=data.friction_term,
                reflecting_term=data.reflecting_term,
                kicking_term=data.kicking_term,
            )
        )

    collected_full_statistics = supervisor.collect_full_statistics
    return {
        "metadata": IntegrationData(
            compute_time=supervisor.integration_time,
            compute_steps=compute_steps,
            RHS_evaluations=supervisor.RHS_evaluations,
            mean_RHS_time=supervisor.mean_RHS_time,
            max_RHS_time=supervisor.max_RHS_time,
            min_RHS_time=supervisor.min_RHS_time,
        ),
        "z_grid": z_grid_cut,
        "sample": sample,
        "hard_reflections": supervisor.number_hard_reflections,
        "level_1_entries": supervisor.number_level_1_entries,
        "level_1_exits": supervisor.number_level_1_exits,
        "level_2_entries": supervisor.number_level_2_entries,
        "level_2_exits": supervisor.number_level_2_exits,
        "number_fragments": num_fragments,
        "largest_RHS_values": (
            supervisor.largest_RHS_values if collected_full_statistics else None
        ),
        "smallest_RHS_values": (
            supervisor.smallest_RHS_values if collected_full_statistics else None
        ),
        "mean_RHS_values": (
            supervisor.mean_RHS_values if collected_full_statistics else None
        ),
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
        """
        :param payload: data dictionary for initializing from the datastore (may be None)
        :param solver_labels: dictionary mapping solver labels to IntegrationSolver objects
        :param cosmology: background cosmology
        :param T_Jordan_init: initial radiation temperature in the Jordan frame
        :param T_Jordan_stop: final radiation temperature in the Jordan frame
        :param phi_Einstein_init: initial Einstein-frame scalar field value
        :param pi_Einstein_init: initial Einstein-frame scalar field derivative dphi/dN
        :param potential: the scalar field potential
        :param coupling: the conformal coupling
        :param atol: absolute tolerance for the ODE solver
        :param rtol: relative tolerance for the ODE solver
        :param z_grid: grid of redshifts at which to sample the solution (optional)
        :param label: optional label for the model
        :param tags: optional list of tags for the model
        """
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
            self._extra_data = None

        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._metadata: Optional[IntegrationData] = payload["metadata"]
            self._solver: Optional[IntegrationSolver] = payload["solver"]
            self._values: Optional[List[ScalarModelValue]] = payload["values"]
            self._extra_data: Optional[Dict[str, Any]] = payload["extra_data"]

        # store parameters
        self._label: str = label
        self._tags: Optional[List[store_tag]] = tags if tags is not None else []

        self._cosmology: BaseCosmology = cosmology
        self._units: UnitsLike = cosmology.units

        self._functions: Optional[ModelFunctions] = None

        self._compute_ref: Optional[ray.ObjectRef] = None

        self._atol: tolerance = atol
        self._rtol: tolerance = rtol

    @property
    def shard_key(self) -> ShardKeyType:
        return self._coupling.shard_key

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

        return self._metadata

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
            H_Einstein=_build_func("H_Einstein"),
            H_Jordan=_build_func("H_Jordan"),
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

        self._metadata = data["metadata"]

        sample: List[SampleValues] = data["sample"]
        z_grid: redshift_array = data["z_grid"]

        extra_data = {}

        def store_attr(src_attr: str, dest_attr: str, min_value: Optional[int] = None):
            value = data[src_attr]

            if min_value is None or value > min_value:
                extra_data[dest_attr] = value

        store_attr("hard_reflections", "number_hard_reflections", 0)
        store_attr("level_1_entries", "number_level_1_entries", 0)
        store_attr("level_1_exits", "number_level_1_exits", 0)
        store_attr("level_2_entries", "number_level_2_entries", 0)
        store_attr("level_2_exits", "number_level_2_exits", 0)
        store_attr("number_fragments", "number_fragments", 1)

        largest_RHS_values = data["largest_RHS_values"]
        smallest_RHS_values = data["smallest_RHS_values"]
        mean_RHS_values = data["mean_RHS_values"]

        if largest_RHS_values is not None:
            extra_data["largest_RHS_values"] = largest_RHS_values._asdict()
        if smallest_RHS_values is not None:
            extra_data["smallest_RHS_values"] = smallest_RHS_values._asdict()
        if mean_RHS_values is not None:
            extra_data["mean_RHS_values"] = mean_RHS_values._asdict()

        if len(extra_data) > 0:
            self._extra_data = extra_data

        self._values = []
        for i in range(len(sample)):
            self._values.append(
                ScalarModelValue(
                    None,
                    z_grid[i],
                    raw_N=sample[i].raw_N,
                    phi_Einstein=sample[i].phi_Einstein,
                    pi_Einstein=sample[i].pi_Einstein,
                    log_rhorad_Einstein=sample[i].log_rhorad_Einstein,
                    log_rhorad_Jordan=sample[i].log_rhorad_Jordan,
                    log_fm=sample[i].log_fm,
                    log_T_Jordan=sample[i].log_T_Jordan,
                    H_Einstein=sample[i].H_Einstein,
                    H_Jordan=sample[i].H_Jordan,
                    gstar_rho=sample[i].gstar_rho,
                    gstar_s=sample[i].gstar_s,
                    dgstar_rho_dlogT=sample[i].dgstar_rho_dlogT,
                    dgstar_s_dlogT=sample[i].dgstar_s_dlogT,
                    Sigma=sample[i].Sigma,
                    friction_term=sample[i].friction_term,
                    reflecting_term=sample[i].reflecting_term,
                    kicking_term=sample[i].kicking_term,
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
        H_Einstein: float,
        H_Jordan: float,
        gstar_rho: float,
        gstar_s: float,
        dgstar_rho_dlogT: float,
        dgstar_s_dlogT: float,
        Sigma: float,
        friction_term: float,
        reflecting_term: float,
        kicking_term: float,
    ):
        """
        :param store_id: ID in the datastore
        :param z: redshift at this point
        :param raw_N: the raw e-folding time N from the ODE solver
        :param phi_Einstein: Einstein-frame scalar field value
        :param pi_Einstein: Einstein-frame scalar field derivative dphi/dN
        :param log_rhorad_Einstein: natural logarithm of the radiation density in the Einstein frame
        :param log_rhorad_Jordan: natural logarithm of the radiation density in the Jordan frame
        :param log_fm: natural logarithm of the matter fraction
        :param log_T_Jordan: natural logarithm of the Jordan-frame radiation temperature
        :param H_Einstein: Hubble parameter in the Einstein frame
        :param H_Jordan: Hubble parameter in the Jordan frame
        :param gstar_rho: effective number of degrees of freedom for energy density
        :param gstar_s: effective number of degrees of freedom for entropy density
        :param dgstar_rho_dlogT: logarithmic derivative of gstar_rho with respect to temperature
        :param dgstar_s_dlogT: logarithmic derivative of gstar_s with respect to temperature
        :param Sigma: equation of state parameter (1 - 3w)
        """
        DatastoreObject.__init__(self, store_id)

        self._z: redshift = z
        self._raw_N: float = raw_N

        self._phi_Einstein: float = phi_Einstein
        self._pi_Einstein: float = pi_Einstein

        self._H_Einstein: float = H_Einstein
        self._H_Jordan: float = H_Jordan

        self._log_rhorad_Einstein: float = log_rhorad_Einstein
        self._log_rhorad_Jordan: float = log_rhorad_Jordan
        self._log_fm: float = log_fm
        self._log_T_Jordan: float = log_T_Jordan

        self._gstar_rho: float = gstar_rho
        self._gstar_s: float = gstar_s
        self._dgstar_rho_dlogT: float = dgstar_rho_dlogT
        self._dgstar_s_dlogT: float = dgstar_s_dlogT

        self._Sigma: float = Sigma

        self._friction_term: float = friction_term
        self._reflecting_term: float = reflecting_term
        self._kicking_term: float = kicking_term

    @property
    def shard_key(self) -> ShardKeyType:
        # should not get called individually, since serialization is handled by the parent ScalarModel
        return NotImplementedError

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def raw_N(self) -> float:
        return self._raw_N

    @property
    def H_Einstein(self) -> float:
        return self._H_Einstein

    @property
    def H_Jordan(self) -> float:
        return self._H_Jordan

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
    def dgstar_rho_dlogT(self) -> float:
        return self._dgstar_rho_dlogT

    @property
    def dgstar_s_dlogT(self) -> float:
        return self._dgstar_s_dlogT

    @property
    def Sigma(self) -> float:
        return self._Sigma

    @property
    def friction_term(self) -> float:
        return self._friction_term

    @property
    def reflecting_term(self) -> float:
        return self._reflecting_term

    @property
    def kicking_term(self) -> float:
        return self._kicking_term


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
