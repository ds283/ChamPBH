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

from bisect import bisect_left
from collections import namedtuple
from math import log, pi, exp, expm1, sqrt, isinf, isnan
from typing import Optional, List, Dict, Any

import numpy as np
import ray
from ray import ObjectRef
from scipy.integrate import OdeSolution, Radau
from scipy.interpolate import make_interp_spline
from scipy.optimize import brentq

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
from config.defaults import (
    DEFAULT_ABS_TOLERANCE,
    DEFAULT_REL_TOLERANCE,
    DEFAULT_STRING_LENGTH,
)
from config.sharding import ShardKeyType
from utilities import energy_formatter
from .Policies import PotentialDerivativePolicy
from .exceptions import ComputationFailureError

# useful constants, calculated once and cached to speed up the numerical integration
PISQ_OVER_30 = pi * pi / 30.0
LOG_PISQ_OVER_30 = log(PISQ_OVER_30)

# key under which the number of elastic reflections (the floor-triggered reflection model of
# integrate_scalar_history) is stored in ScalarModel.extra_metadata.
# A count of zero is never stored, so an absent key means there were none.
REFLECTIONS_KEY = "number_reflections"

EXPECTED_SOL_LENGTH = 5

# the stepper label returned by compute_scalar_model; main.py pre-registers it as
# IntegrationSolver(label="Radau+kinematic-cap", stepping=0)
SCALAR_MODEL_STEPPER_LABEL = "Radau+kinematic-cap-stepping0"

# a trial-state exception inside a Radau step is treated as a rejected step and the step halved;
# below this step size (in e-folds) it becomes a failure of the history
MIN_STEP_AFTER_TRIAL_EXCEPTION = 1e-13

# the parameters of the step loop in integrate_scalar_history (integrator-remediation prompt 01)
#   cap_fraction:        f; a step may move the field inwards by at most f*phi
#   cap_floor:           h_floor in e-folds; the cap never falls below it, and an inward-moving
#                        field whose velocity cap f*phi/|pi| would fall below it is reflected
#   global_max_step:     the cap applied everywhere, in e-folds
#   jacobian_factor_max: upper clamp on scipy's finite-difference Jacobian perturbation factor
#   atol, rtol:          tolerances passed to Radau
#   step_budget:         the largest number of accepted steps; exceeding it is a failure of the
#                        history (integrator-remediation prompt 02). The default, 2e6 steps, is
#                        the planner's (README §0.2): about an hour, and far above the 3.4e4
#                        steps of the costliest history measured (beta = 3, M = 0.001)
StepControl = namedtuple(
    "StepControl",
    [
        "cap_fraction",
        "cap_floor",
        "global_max_step",
        "jacobian_factor_max",
        "atol",
        "rtol",
        "step_budget",
    ],
    defaults=[
        0.1,
        1e-11,
        0.1,
        1e-4,
        DEFAULT_ABS_TOLERANCE,
        DEFAULT_REL_TOLERANCE,
        2_000_000,
    ],
)

# one elastic reflection performed by integrate_scalar_history: the e-fold number, the field
# value and the (negative) inward velocity at which it was performed
Reflection = namedtuple(
    "Reflection",
    [
        "N",
        "phi_Einstein",
        "pi_Einstein_in",
    ],
)

# what integrate_scalar_history returns
#   solution:                    scipy OdeSolution over [N_start, N_final]
#   N_final:                     e-fold number at which ln T_J crossed log_T_stop
#   final_state:                 StateVector at N_final
#   nfev:                        RHS evaluations requested by the loop (including Jacobian probes)
#   accepted_steps:              accepted Radau steps
#   steps_rejected_by_exception: step attempts abandoned because the RHS raised on a trial state
#   reflections:                 list of Reflection
#   max_wall_to_kinetic_ratio:   largest W/(pi^2/2) at a reflection (guard G2), or None
IntegrationResult = namedtuple(
    "IntegrationResult",
    [
        "solution",
        "N_final",
        "final_state",
        "nfev",
        "accepted_steps",
        "steps_rejected_by_exception",
        "reflections",
        "max_wall_to_kinetic_ratio",
    ],
)

# using named tuples ensures that we never get the fields in the wrong order
ODEPolicyData = namedtuple(
    "ODEPolicyData",
    [
        "fm",
        "T_Jordan",
        "Sigma",
        "log_V",
        "V_over_3H2Mp2",
        "Vprime_over_3H2Mp2",
        "d_logOmega_dphi",
        "friction_term",
        "reflecting_term",
        "kicking_term",
    ],
)

HubblePolicyData = namedtuple(
    "HubblePolicyData",
    [
        "H_Einstein",
        "H_Jordan",
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


class ODEPolicy:
    def __init__(
        self,
        task_label: str,
        cosmology: BaseCosmology,
        potential: AbstractPotential,
        coupling: AbstractCoupling,
    ):
        self.task_label: str = task_label
        self.units: UnitsLike = cosmology.units

        self.cosmology: BaseCosmology = cosmology
        self.potential: AbstractPotential = potential
        self.coupling: AbstractCoupling = coupling

        self.V_policy: PotentialDerivativePolicy = PotentialDerivativePolicy(
            task_label, cosmology, potential
        )

        self.MP = self.units.PlanckMass
        self.CONST_MP_SQ = self.MP * self.MP
        self.CONST_3_MP_SQ = 3.0 * self.CONST_MP_SQ
        self.CONST_6_MP_SQ = 6.0 * self.CONST_MP_SQ

        self.GeV = self.units.GeV
        self.Kelvin = self.units.Kelvin

    def _get_fm(self, N: float, state: StateVector) -> float:
        try:
            fm: float = exp(state.log_fm)
        except OverflowError as e:
            msg = f"!! ODEPolicy ({self.task_label}): math overflow in exp(log_fm), log_fm = {state.log_fm:.5g} | N = {N:.5g}, phi_Einstein = {state.phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {state.pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg) from e

        return fm

    def _get_T_Jordan(self, N: float, state: StateVector) -> float:
        try:
            T_Jordan: float = exp(state.log_T_Jordan)
        except OverflowError as e:
            msg = f"!! ODEPolicy ({self.task_label}): math overflow in exp(log_T_Jordan), log_T_Jordan = {state.log_T_Jordan:.5g} | N = {N:.5g}, log_fm = {state.log_fm:.5g}, phi_Einstein = {state.phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {state.pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg) from e

        if T_Jordan <= 0.0:
            # exp(log_T_Jordan) has underflowed: an unphysical trial state (a Newton iterate or a
            # Jacobian probe). Raise like the other unphysical states; integrate_scalar_history
            # treats the exception as a rejected step. (integrator-remediation prompt 02: this
            # replaces a silent substitution of T_Jordan = 1 K.)
            msg = f"!! ODEPolicy ({self.task_label}): T_Jordan = {T_Jordan:.5g}, log_T_Jordan = {state.log_T_Jordan:.5g} at N={N:.8g}"
            print(msg)
            raise ComputationFailureError(msg)

        return T_Jordan

    def __call__(self, N: float, state: StateVector) -> ODEPolicyData:
        if any((isnan(x) or isinf(x)) for x in state):
            msg = f"!! ODEPolicy ({self.task_label}): input to ODE RHS has infinity or NaN values at N={N:.8g}"
            print(msg)
            print(f"     - state={state}")
            raise ComputationFailureError(msg)

        phi_Einstein: float = state.phi_Einstein
        pi_Einstein: float = state.pi_Einstein
        log_rhorad_Einstein: float = state.log_rhorad_Einstein

        fm = self._get_fm(N, state)
        T_Jordan = self._get_T_Jordan(N, state)

        Sigma: float = 1.0 - 3.0 * self.cosmology.w(T_Jordan)

        # compute R = (Sigma + fm)/(1 + fm)
        # try to avoid any problems with overflow
        R: float
        if fm > 10.0:
            R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        else:
            R = (Sigma + fm) / (1.0 + fm)

        d_logOmega_dphi: float = self.coupling.d_logOmega_dphi(phi_Einstein)

        log_V = self.V_policy.log_V(phi_Einstein)
        V_over_3H2Mp2 = self.V_policy.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        Vprime_over_3H2Mp2 = self.V_policy.Vprime_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        D: float = self.CONST_3_MP_SQ * Vprime_over_3H2Mp2

        # G must be positive in order that H_Einstein^2 is also positive
        # this gives a limit pi_Einstein < sqrt(6) Mp
        # equality only happens if the scalar field KE dominates the energy budget of the universe,
        # which we should not encounter
        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        E: float = G - V_over_3H2Mp2
        # E must be positive, because it is proportional to rho_R/H^2
        if E < 0.0:
            # unclear whether we should treat this as a genuine computational error, or whether it just means that rho_r is very small
            # (e.g. at the end of the integration) and should harmlessly be treated as zero
            msg = f"!! ODEPolicy ({self.task_label}): negative value of E = {E:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            # raise ComputationFailureError(msg)
            E = 0.0

        dotH_over_H2_plus_3: float = self.V_policy.Hdot_over_H2_plus_3(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
        )

        friction_term = -pi_Einstein * dotH_over_H2_plus_3
        reflecting_term = -D
        kicking_term = -self.CONST_3_MP_SQ * E * d_logOmega_dphi * R

        return ODEPolicyData(
            fm=fm,
            T_Jordan=T_Jordan,
            Sigma=Sigma,
            log_V=log_V,
            V_over_3H2Mp2=V_over_3H2Mp2,
            Vprime_over_3H2Mp2=Vprime_over_3H2Mp2,
            d_logOmega_dphi=d_logOmega_dphi,
            friction_term=friction_term,
            reflecting_term=reflecting_term,
            kicking_term=kicking_term,
        )


class HubblePolicy:
    def __init__(self, coupling: AbstractCoupling, units: UnitsLike):
        self.coupling: AbstractCoupling = coupling
        self.units: UnitsLike = units

        self.MP = units.PlanckMass
        self.CONST_MP_SQ = self.MP * self.MP
        self.CONST_3_MP_SQ = 3.0 * self.CONST_MP_SQ
        self.CONST_6_MP_SQ = 6.0 * self.CONST_MP_SQ

    def __call__(self, data: ODEPolicyData, state: StateVector) -> HubblePolicyData:
        if data.V_over_3H2Mp2 > 0.0:
            # V should not be zero
            assert not isinf(data.log_V)

            log_V_over_3H2Mp2: float = log(data.V_over_3H2Mp2)
            log_3H2Mp2: float = -1.0 * (log_V_over_3H2Mp2 - data.log_V)
            H2_Einstein: float = exp(log_3H2Mp2) / self.CONST_3_MP_SQ
        else:
            # can assume that V = 0, so we can't use log(V/3H^2 Mp^2); we have to compute H^2 directly

            rho_rad: float = exp(state.log_rhorad_Einstein)
            fm: float = exp(state.log_fm)
            pi_Einstein: float = state.pi_Einstein
            G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
            ThreeH2Mp2: float = rho_rad * (1.0 + fm) / G
            H2_Einstein: float = ThreeH2Mp2 / self.CONST_3_MP_SQ

        H_Einstein: float = sqrt(H2_Einstein)

        Omega: float = self.coupling.Omega(state.phi_Einstein)

        # H_Jordan can even be negative, so there is no use trying to store its logarithm
        H_Jordan: float = (
            H_Einstein * (1.0 + data.d_logOmega_dphi * state.pi_Einstein) / Omega
        )

        return HubblePolicyData(H_Einstein=H_Einstein, H_Jordan=H_Jordan)


class ODERHS:
    def __init__(self, task_label: str, policy: ODEPolicy):
        self.task_label = task_label
        self.policy = policy

        self.cosmology = policy.cosmology
        self.units = self.cosmology.units
        self._formatter: energy_formatter = energy_formatter(self.units)

        self.MP = self.units.PlanckMass
        self.GeV = self.units.GeV

    def __call__(self, N: float, s: StateVector, supervisor):
        with RHS_timer(supervisor) as timer:
            state: StateVector = StateVector._make(s)
            data: ODEPolicyData = self.policy(N, state)

            phi_Einstein: float = state.phi_Einstein
            pi_Einstein: float = state.pi_Einstein
            log_rhorad_Einstein: float = state.log_rhorad_Einstein

            fm: float = data.fm
            T_Jordan: float = data.T_Jordan

            if supervisor.notify_available:
                supervisor.message(
                    N,
                    T_Jordan,
                    f"current state: phi_E = {self._formatter(phi_Einstein)}, pi_E = {self._formatter(pi_Einstein)}, f_m = {fm:.5g}, log(rho_rad/GeV^4) = {log_rhorad_Einstein - 4.0 * log(self.GeV):.5g}, T_J = {self._formatter(T_Jordan)}",
                )
                supervisor.reset_notify_time(T_Jordan)

            d_phi_Einstein: float = pi_Einstein
            d_pi_Einstein: float = (
                data.friction_term + data.reflecting_term + data.kicking_term
            )

            d_log_rhorad_Einstein: float = (
                data.Sigma - 4.0 + data.Sigma * data.d_logOmega_dphi * pi_Einstein
            )
            d_log_fm: float = (1.0 - data.Sigma) * (
                1.0 + data.d_logOmega_dphi * pi_Einstein
            )

            G_s: float = self.cosmology.G_s(T_Jordan)
            dG_s_dlogT: float = self.cosmology.dG_s_dlogT(T_Jordan)

            d_log_T_Jordan: float = -(1.0 + data.d_logOmega_dphi * pi_Einstein) / (
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

                # (integrator-remediation prompt 02) this branch used to read data.d_logV_dphi, a
                # field ODEPolicyData does not have, so it raised AttributeError instead of the
                # ComputationFailureError below. V'/V is no longer printed: not every potential
                # implements d_logV_dphi, and a diagnostic must not raise on its own account.
                # V/3H^2Mp^2 and V'/3H^2Mp^2 are printed on the "cosmology" line.
                log_V: float = data.log_V

                V_over_3H2Mp2: float = data.V_over_3H2Mp2
                Vprime_over_3H2Mp2: float = data.Vprime_over_3H2Mp2

                print(
                    f"!! compute_scalar_model ({self.task_label}): output from ODE RHS has infinity or NaN values at N={N:.8g}"
                )
                print(
                    f"     - inputs/states: phi_E={self._formatter(phi_Einstein)}, pi_E={self._formatter(pi_Einstein)}, log_rhorad_E={log_rhorad_Einstein:.5g}, log_fm={log_fm:.5g}, log_T_J={log_T_Jordan:.5g}"
                )
                print(
                    f"     - physical: log(rhorad_E/GeV^4)={log_rhorad_Einstein - 4.0*log(self.GeV):.5g}, fm={fm:.5g}, T_J={self._formatter(T_Jordan)}"
                )
                print(
                    f"     - potential: log(V/GeV^4)={log_V - 4.0*log(self.GeV):.5g}, d_logOmega_dphi'={data.d_logOmega_dphi:.5g}"
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
                raise ComputationFailureError(
                    f"compute_scalar_model ({self.task_label}): output from ODE RHS has infinity or NaN values"
                )

            # print(
            #     f"@   N={N:.6g}, phi_E={phi_Einstein/self.MP:.6g} Mp, pi_E={pi_Einstein/self.MP:.6g} Mp, T_J={T_Jordan/self.GeV:.6g} GeV, friction={data.friction_term/self.MP:.6g} Mp, kicking={data.kicking_term/self.MP:.6g} Mp, reflecting={data.reflecting_term/self.MP} Mp, phi_E/M={phi_Einstein/self.policy.potential._M_float:.6g}, max_step={supervisor._max_step_size:.6g}"
            # )

            supervisor.notify_new_RHS(return_state)

            return return_state


def _reflection_guards(
    policy: ODEPolicy, N: float, y: np.ndarray, task_label: Optional[str]
) -> float:
    """
    The two guards on an elastic reflection (integrator-remediation README §2 (b')), checked at
    the state where the floor rule asks for a reflection. Returns W/(pi^2/2); raises
    ComputationFailureError if either guard fails.

    G1: the potential must declare a repulsive wall between the origin and the field
        (reflects_at_origin). The floor rule is a time-scale test, not a wall detector, and the
        reflection is exact only if a wall exists in (0, phi).
    G2: the wall part of the potential fraction, W = 3 (V - V_floor)/(3H^2 Mp^2), must not exceed
        the kinetic fraction pi^2/(2 Mp^2). A field that arrived from outside the wall satisfies
        this; one that has already been stepped into the wall does not.
    """
    potential = policy.potential
    state = StateVector._make(y)
    label = f" ({task_label})" if task_label is not None else ""

    if not potential.reflects_at_origin:
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: the representable-step floor was reached "
            f"(N={N:.10g}, phi_E={state.phi_Einstein:.5g}, pi_E={state.pi_Einstein:.5g}), but the "
            f"potential {potential.name} does not declare reflects_at_origin, so the elastic "
            f"reflection model cannot be applied"
        )

    log_V_floor = potential.log_V_floor
    if log_V_floor is None:
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: the potential {potential.name} declares "
            f"reflects_at_origin but provides no log_V_floor, so the reflection cannot be checked"
        )

    data: ODEPolicyData = policy(N, state)
    W = -3.0 * data.V_over_3H2Mp2 * expm1(log_V_floor - data.log_V)
    kinetic = 0.5 * state.pi_Einstein * state.pi_Einstein / policy.CONST_MP_SQ
    ratio = W / kinetic

    if not (W <= kinetic):
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: reflection requested inside the wall: "
            f"W/(pi^2/2) = {ratio:.5g} at N={N:.10g}, phi_E={state.phi_Einstein:.5g}, "
            f"pi_E={state.pi_Einstein:.5g} (W = {W:.5g}, pi^2/2 = {kinetic:.5g})"
        )

    return ratio


def integrate_scalar_history(
    RHS,
    supervisor: ScalarFieldIntegrationSupervisor,
    initial_state: StateVector,
    N_start: float,
    log_T_stop: float,
    params: StepControl = StepControl(),
    N_failsafe: float = 1000.0,
    policy: Optional[ODEPolicy] = None,
    task_label: Optional[str] = None,
    N_stop: Optional[float] = None,
) -> IntegrationResult:
    """
    Integrate the scalar-field history from (N_start, initial_state) until ln T_J falls below
    log_T_stop, with one Radau step loop under a kinematic step cap
    (integrator-remediation prompt 01; README §2 (a)-(c)).

    Before every step, from the current state (phi, pi) and the RHS pi' that Radau holds:
      - if pi < 0 and f*phi/|pi| < h_floor, the wall is thinner than a representable step: after
        the guards G1 and G2, pi -> -pi and a new Radau instance is started from the reflected
        state at the same N (an instantaneous elastic reflection);
      - otherwise the step is capped at min(global_max_step, f*phi/|pi| if pi < 0,
        sqrt(2 f phi/|pi'|) if pi' < 0), never below h_floor.
    A ComputationFailureError raised by the RHS on a trial state is a rejected step: the step is
    halved and retried, and below MIN_STEP_AFTER_TRIAL_EXCEPTION it is the history's failure.
    After every accepted step SciPy's Jacobian perturbation factor is clamped, and phi <= 0 is a
    failure (under the cap it can only mean the cap was violated). The history ends at the first
    accepted step with ln T_J < log_T_stop, at the root of ln T_J - log_T_stop on that step's
    interpolant. Reaching N_failsafe is a failure, and so is taking more than params.step_budget
    accepted steps.

    If N_stop is given (used to drive a window of a history, e.g. from a mid-history state in a
    test), Radau's bound is min(N_stop, N_failsafe) and reaching N_stop ends the integration
    successfully at N_stop, unless ln T_J has fallen below log_T_stop first.

    :param RHS: callable RHS(N, state, supervisor), normally an ODERHS
    :param supervisor: the ScalarFieldIntegrationSupervisor (already entered)
    :param initial_state: the state at N_start
    :param N_start: the initial e-fold number
    :param log_T_stop: the log Jordan-frame temperature at which to stop
    :param params: the StepControl parameters
    :param N_failsafe: the Radau bound; reaching it is a failure
    :param policy: the ODEPolicy used for the reflection guard; defaults to RHS.policy
    :param task_label: label used in failure messages
    :param N_stop: optional e-fold number at which to end the integration successfully
    :return: IntegrationResult
    """
    if policy is None:
        policy = RHS.policy

    f_cap: float = params.cap_fraction
    h_floor: float = params.cap_floor
    global_max_step: float = params.global_max_step
    jacobian_factor_max: float = params.jacobian_factor_max
    step_budget: int = params.step_budget

    label = f" ({task_label})" if task_label is not None else ""

    nfev = 0

    def fun(N, y):
        nonlocal nfev
        nfev += 1
        return RHS(N, y, supervisor)

    N_bound: float = N_failsafe if N_stop is None else min(N_stop, N_failsafe)
    stop_at_bound: bool = N_stop is not None and N_stop <= N_failsafe

    def new_solver(N, y):
        return Radau(
            fun,
            N,
            y,
            N_bound,
            max_step=global_max_step,
            rtol=params.rtol,
            atol=params.atol,
        )

    solver = new_solver(float(N_start), np.array(initial_state, dtype=float))

    ts = [solver.t]
    interpolants = []
    reflections = []
    max_ratio: Optional[float] = None
    accepted_steps = 0
    steps_rejected_by_exception = 0

    while True:
        N: float = solver.t
        phi: float = solver.y[0]
        pi_: float = solver.y[1]

        # the floor rule: an inward-moving field whose velocity cap would fall below the
        # representable step is reflected elastically (README §2 (b))
        if pi_ < 0.0 and f_cap * phi / (-pi_) < h_floor:
            ratio = _reflection_guards(policy, N, solver.y, task_label)
            max_ratio = ratio if max_ratio is None else max(max_ratio, ratio)

            reflections.append(Reflection(N=N, phi_Einstein=phi, pi_Einstein_in=pi_))
            supervisor.notify_reflection(N)

            y_reflected = solver.y.copy()
            y_reflected[1] = -pi_
            solver = new_solver(N, y_reflected)
            continue

        # the kinematic cap (README §2 (a)): the inward displacement in a step is at most
        # max(-pi, 0) h + 1/2 max(-pi', 0) h^2; require it to be at most f*phi
        cap: float = global_max_step
        if pi_ < 0.0:
            cap = min(cap, f_cap * phi / (-pi_))
        a_in: float = -solver.f[1]
        if a_in > 0.0:
            cap = min(cap, sqrt(2.0 * f_cap * phi / a_in))
        cap = max(cap, h_floor)

        solver.max_step = cap
        if solver.h_abs > cap:
            solver.h_abs = cap
        supervisor.notify_step_cap(cap)

        try:
            message = solver.step()
        except ComputationFailureError as e:
            # the RHS raised on a trial state (a Newton iterate or a Jacobian probe): reject the step
            steps_rejected_by_exception += 1
            solver.h_abs *= 0.5
            if solver.h_abs < MIN_STEP_AFTER_TRIAL_EXCEPTION:
                raise ComputationFailureError(
                    f"integrate_scalar_history{label}: step size fell below "
                    f"{MIN_STEP_AFTER_TRIAL_EXCEPTION:.3g} e-folds after repeated trial-state "
                    f"exceptions at N={N:.10g}, phi_E={phi:.5g}, pi_E={pi_:.5g}: {e.message}"
                ) from e
            continue

        if message is not None:
            raise ComputationFailureError(
                f'integrate_scalar_history{label}: Radau step failed at N={solver.t:.10g}, phi_E={solver.y[0]:.5g}, pi_E={solver.y[1]:.5g}: "{message}"'
            )

        accepted_steps += 1

        # the step budget (README §2 (h)): without a parked-tracking model a history whose
        # bounces are unresolvable (physical M, beta >= 1.2) would run for days; fail it cleanly
        if accepted_steps > step_budget:
            raise ComputationFailureError(
                f"step budget exhausted: integrate_scalar_history{label} took {accepted_steps} "
                f"accepted steps (budget {step_budget}) at N={solver.t:.10g}, "
                f"T_J={exp(solver.y[4]) / policy.GeV:.5g} GeV, with {len(reflections)} "
                f"reflection(s)"
            )

        # scipy's num_jac multiplies a component's perturbation factor by 10 whenever its
        # Jacobian column is (nearly) zero, with no upper clamp; clamp it (README §2 (f))
        if solver.jac_factor is not None:
            np.minimum(solver.jac_factor, jacobian_factor_max, out=solver.jac_factor)

        if solver.y[0] <= 0.0:
            raise ComputationFailureError(
                f"integrate_scalar_history{label}: phi <= 0 in an accepted state (the step cap was "
                f"violated) at N={solver.t:.10g}, phi_E={solver.y[0]:.5g}, pi_E={solver.y[1]:.5g}"
            )

        interpolant = solver.dense_output()

        if solver.y[4] < log_T_stop:
            N_low: float = solver.t_old
            N_high: float = solver.t

            def crossing(N_):
                return interpolant(N_)[4] - log_T_stop

            g_low = crossing(N_low)
            g_high = crossing(N_high)
            if not (g_low >= 0.0 and g_high < 0.0):
                raise ComputationFailureError(
                    f"integrate_scalar_history{label}: cannot bracket the termination root on the "
                    f"last step [{N_low:.10g}, {N_high:.10g}] (ln T_J - log_T_stop = {g_low:.5g}, {g_high:.5g})"
                )

            if g_low == 0.0 and len(interpolants) > 0:
                # the previous accepted state sits exactly on log_T_stop: it is the final node
                N_final = N_low
            else:
                N_final = brentq(crossing, N_low, N_high) if g_low > 0.0 else N_low
                ts.append(N_final)
                interpolants.append(interpolant)
            final_state = StateVector._make(interpolant(N_final))
            break

        ts.append(solver.t)
        interpolants.append(interpolant)

        if solver.status == "finished":
            if stop_at_bound:
                N_final = solver.t
                final_state = StateVector._make(solver.y)
                break

            raise ComputationFailureError(
                f"integrate_scalar_history{label}: the failsafe N={N_failsafe:.5g} was reached "
                f"without ln T_J falling below log_T_stop={log_T_stop:.5g} (ln T_J={solver.y[4]:.5g})"
            )

    return IntegrationResult(
        solution=OdeSolution(ts, interpolants),
        N_final=N_final,
        final_state=final_state,
        nfev=nfev,
        accepted_steps=accepted_steps,
        steps_rejected_by_exception=steps_rejected_by_exception,
        reflections=reflections,
        max_wall_to_kinetic_ratio=max_ratio,
    )


# the first bounce of a history (science-readiness prompt 03; README §0.2 P5, §2 (g))
#   N:             e-fold number of the bounce (the same N as Reflection.N and SampleValues.raw_N)
#   phi_Einstein:  the Einstein-frame field there, phi_min for a turning point
#   log_T_Jordan:  ln of the Jordan-frame temperature there, in the cosmology's units
#   reflected:     True if the bounce is an elastic reflection, False if it is a turning point
FirstBounce = namedtuple(
    "FirstBounce",
    [
        "N",
        "phi_Einstein",
        "log_T_Jordan",
        "reflected",
    ],
)


def first_bounce(result: IntegrationResult) -> Optional[FirstBounce]:
    """
    The first bounce of an integrated history (science-readiness prompt 03; README §0.2 P5,
    §2 (g)), or None if there is none.

    The accepted steps are walked in order, on their own interpolants. The first bounce is the
    first step [t_k, t_{k+1}] on whose interpolant pi(t_k) < 0 < pi(t_{k+1}); it is located at
    the root of pi on that interpolant (brentq, xtol = 1e-15), and the state is read there. This
    is the dense-output turning point the user ruled for (integrator-remediation board,
    Decisions), so phi_Einstein is the minimum of phi along the dense output.

    An elastic reflection (Reflection) happens at a step boundary: the step that ends at it has
    pi < 0 at its end and the step after it starts with pi > 0, so it is never a sign change
    inside one interpolant. If the first reflection comes before the first turning point, it is
    the first bounce, with reflected=True and phi and ln T_J read from the step that ends at it
    (from the step that starts at it if the reflection is at the history's first N).

    The inequalities are strict, so a history that starts from pi = 0 (main.py's pi* = 0) and
    then falls inwards does not count its start as a bounce. The last step's interpolant is read
    only up to the history's end (ts[-1] = N_final). No phi < 1.5 M filter is applied (P5).

    :param result: the IntegrationResult of integrate_scalar_history
    :return: FirstBounce, or None if pi never turns from negative to positive and there was no
             reflection
    """
    ts = result.solution.ts
    interpolants = result.solution.interpolants

    N_reflection: Optional[float] = (
        result.reflections[0].N if len(result.reflections) > 0 else None
    )

    for k, interpolant in enumerate(interpolants):
        t_low: float = ts[k]
        t_high: float = ts[k + 1]

        # a reflection at or before the start of this step comes before any turning point on it
        if N_reflection is not None and N_reflection <= t_low:
            break

        if interpolant(t_low)[1] < 0.0 < interpolant(t_high)[1]:
            N_bounce: float = brentq(
                lambda t: interpolant(t)[1], t_low, t_high, xtol=1e-15
            )
            y = interpolant(N_bounce)
            return FirstBounce(
                N=float(N_bounce),
                phi_Einstein=float(y[0]),
                log_T_Jordan=float(y[4]),
                reflected=False,
            )

    if N_reflection is None:
        return None

    # the state at the reflection, from the step that ends at it. A reflection is made at the
    # solver's current N, which is always a node of ts (N_start, or the end of an accepted step)
    k_end: int = bisect_left(ts, N_reflection) - 1
    if k_end < 0:
        # the reflection was made at the history's first N, before any step
        y = interpolants[0](N_reflection)
    else:
        y = interpolants[k_end](N_reflection)
    return FirstBounce(
        N=float(N_reflection),
        phi_Einstein=float(y[0]),
        log_T_Jordan=float(y[4]),
        reflected=True,
    )


def _failure_payload(reason: str) -> dict:
    """
    The payload compute_scalar_model returns for a failed history: the failure flag and
    the reason, truncated to DEFAULT_STRING_LENGTH, the width of the
    ScalarModel.failure_reason column. (science-readiness prompt 02; BBNData's pattern)
    """
    return {"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}


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

    policy = ODEPolicy(task_label, cosmology, potential, coupling)
    RHS = ODERHS(task_label, policy)

    # the step loop's parameters: the defaults of StepControl, with the tolerances requested
    step_control = StepControl(atol=atol, rtol=rtol)

    N_failsafe = 1000.0  # terminate after 1000 e-folds as a failsafe
    N_start = 0.0

    # prepare the initial state
    initial_state = StateVector(
        phi_Einstein=phi_init_float,
        pi_Einstein=pi_init_float,
        log_rhorad_Einstein=log_rhorad_Einstein_init,
        log_fm=log_fm_init,
        log_T_Jordan=log_T_init,
    )

    # one stepper (integrator-remediation prompt 02): a ComputationFailureError from the step loop
    # or from the sampling is a failure of this history, recorded as a failure row.
    # The "z grid too short" RuntimeError below is a configuration error and is not caught.
    try:
        with ScalarFieldIntegrationSupervisor(
            units,
            T_init,
            T_stop,
            label=task_label,
            collect_full_statistics=False,
        ) as supervisor:
            result: IntegrationResult = integrate_scalar_history(
                RHS,
                supervisor,
                initial_state,
                N_start,
                log_T_stop,
                step_control,
                N_failsafe=N_failsafe,
                policy=policy,
                task_label=task_label,
            )

        # the solution must carry the five components of StateVector; anything else is a bug
        assert (
            len(result.solution(result.N_final)) == EXPECTED_SOL_LENGTH
        ), f"compute_scalar_model ({task_label}): solution does not have {EXPECTED_SOL_LENGTH} components"

        if verbose and len(result.reflections) > 0:
            print(
                f"-- compute_scalar_model ({task_label}): {len(result.reflections)} elastic reflection(s), first at N={result.reflections[0].N:.5g}"
            )

        # the integration should have terminated when T_Jordan = T_CMB, which ought to correspond to z = 0
        # we now work backwards and sample the integration output on the supplied z grid, using the e-fold number
        # to assign a value of log(1 + z).
        final_N = result.N_final
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

        solution: OdeSolution = result.solution

        hubble: HubblePolicy = HubblePolicy(coupling, units)
        try:
            for z in z_grid_cut:
                z: redshift
                N_backward = log(1.0 + z.z)
                N_forward = final_N - N_backward

                state: StateVector = StateVector._make(solution(N_forward))
                data: ODEPolicyData = policy(N_forward, state)
                hubble_data: HubblePolicyData = hubble(data, state)

                T_Jordan: float = data.T_Jordan

                log_Omega: float = coupling.log_Omega(state.phi_Einstein)
                log_rhorad_Jordan: float = state.log_rhorad_Einstein - 4.0 * log_Omega

                sample.append(
                    SampleValues(
                        raw_N=N_forward,
                        phi_Einstein=state.phi_Einstein,
                        pi_Einstein=state.pi_Einstein,
                        log_rhorad_Einstein=state.log_rhorad_Einstein,
                        log_rhorad_Jordan=log_rhorad_Jordan,
                        log_fm=state.log_fm,
                        log_T_Jordan=state.log_T_Jordan,
                        H_Einstein=hubble_data.H_Einstein,
                        H_Jordan=hubble_data.H_Jordan,
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
        except OverflowError as e:
            print(
                f"!! compute_scalar_model ({task_label}): overflow when assembling sample values; marked as total integration failure"
            )
            return _failure_payload(
                f"sampling: overflow when assembling sample values: {e}"
            )
    except ComputationFailureError as e:
        print(f"-- compute_scalar_model ({task_label}): integration failure")
        print(f"   {e.message}")
        print(
            f"!! compute_scalar_model ({task_label}): marked as total integration failure"
        )
        return _failure_payload(e.message)

    # the first bounce, on the dense output (science-readiness prompt 03); None if there is none
    bounce: Optional[FirstBounce] = first_bounce(result)

    collected_full_statistics = supervisor.collect_full_statistics
    return {
        "metadata": IntegrationData(
            compute_time=supervisor.integration_time,
            compute_steps=result.nfev,
            RHS_evaluations=supervisor.RHS_evaluations,
            mean_RHS_time=supervisor.mean_RHS_time,
            max_RHS_time=supervisor.max_RHS_time,
            min_RHS_time=supervisor.min_RHS_time,
        ),
        "z_grid": z_grid_cut,
        "sample": sample,
        "reflections": len(result.reflections),
        "first_bounce": bounce,
        "cap_fraction": step_control.cap_fraction,
        "cap_floor": step_control.cap_floor,
        "cap_global_max_step": step_control.global_max_step,
        "jacobian_factor_max": step_control.jacobian_factor_max,
        "accepted_steps": result.accepted_steps,
        "steps_rejected_by_exception": result.steps_rejected_by_exception,
        "largest_RHS_values": (
            supervisor.largest_RHS_values if collected_full_statistics else None
        ),
        "smallest_RHS_values": (
            supervisor.smallest_RHS_values if collected_full_statistics else None
        ),
        "mean_RHS_values": (
            supervisor.mean_RHS_values if collected_full_statistics else None
        ),
        "solver_label": SCALAR_MODEL_STEPPER_LABEL,
    }


def build_extra_data(data: dict) -> Optional[dict]:
    """
    Assemble the ScalarModel's extra_data dictionary from the dictionary that
    compute_scalar_model returns. Returns None if there is nothing to store.
    """
    extra_data = {}

    def store_attr(src_attr: str, dest_attr: str, min_value: Optional[int] = None):
        value = data[src_attr]

        if min_value is None or value > min_value:
            extra_data[dest_attr] = value

    store_attr("reflections", REFLECTIONS_KEY, 0)
    store_attr("cap_fraction", "cap_fraction")
    store_attr("cap_floor", "cap_floor")
    store_attr("cap_global_max_step", "cap_global_max_step")
    store_attr("jacobian_factor_max", "jacobian_factor_max")
    store_attr("accepted_steps", "accepted_steps")
    store_attr("steps_rejected_by_exception", "steps_rejected_by_exception", 0)

    largest_RHS_values = data["largest_RHS_values"]
    smallest_RHS_values = data["smallest_RHS_values"]
    mean_RHS_values = data["mean_RHS_values"]

    if largest_RHS_values is not None:
        extra_data["largest_RHS_values"] = largest_RHS_values._asdict()
    if smallest_RHS_values is not None:
        extra_data["smallest_RHS_values"] = smallest_RHS_values._asdict()
    if mean_RHS_values is not None:
        extra_data["mean_RHS_values"] = mean_RHS_values._asdict()

    return extra_data if len(extra_data) > 0 else None


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
            self._failure = None
            self._failure_reason = None
            self._first_bounce = None

        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._metadata: Optional[IntegrationData] = payload["metadata"]
            self._solver: Optional[IntegrationSolver] = payload["solver"]
            self._values: Optional[List[ScalarModelValue]] = payload["values"]
            self._extra_data: Optional[Dict[str, Any]] = payload["extra_data"]
            self._failure: Optional[bool] = payload["failure"]
            self._failure_reason: Optional[str] = payload["failure_reason"]
            self._first_bounce: Optional[FirstBounce] = payload["first_bounce"]

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
    def failure(self) -> Optional[bool]:
        return self._failure

    @property
    def failure_reason(self) -> Optional[str]:
        """
        Why the history failed, or None if it did not. Unlike the other properties this is
        readable when `failure` is true; that is its purpose. (science-readiness prompt 02)
        """
        if self._failure is None:
            raise RuntimeError(
                f"ScalarModel ({self._label}): failure_reason has not yet been populated"
            )
        return self._failure_reason

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
        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._metadata is None:
            raise RuntimeError("metadata values have not yet been populated")

        return self._metadata

    @property
    def extra_metadata(self) -> Optional[Dict[str, Any]]:
        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        return self._extra_data

    @property
    def first_bounce(self) -> Optional[FirstBounce]:
        """
        The history's first bounce (first_bounce()), or None if it had none.
        Raises on a failure row, as metadata does. (science-readiness prompt 03)
        """
        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._failure is None:
            raise RuntimeError("first_bounce has not yet been populated")

        return self._first_bounce

    @property
    def solver(self) -> IntegrationSolver:
        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._solver is None:
            raise RuntimeError("solver has not yet been populated")
        return self._solver

    @property
    def values(self) -> List:
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarModel: attempt to read values, but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._values is None:
            raise RuntimeError("values has not yet been populated")
        return self._values

    @property
    def functions(self) -> ModelFunctions:
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarModel: attempt to call functions(), but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._values is None:
            raise RuntimeError("values has not yet been populated")

        if self._functions is None:
            self._create_functions()

        return self._functions

    def _create_functions(self):
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarModel: attempt to call _create_functions(), but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarModel ({self._label}): this object had an integration failure and cannot be used"
            )

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
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarModel: attempt to call compute(), but _do_not_populate is set"
            )

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

        failure: bool = data.get("failure", False)
        if failure:
            self._failure = True
            self._failure_reason = (
                str(data.get("failure_reason", ""))[:DEFAULT_STRING_LENGTH] or None
            )
            self._first_bounce = None
            self._values = []
            return True

        self._failure = False
        self._failure_reason = None
        self._first_bounce = data["first_bounce"]
        self._metadata = data["metadata"]

        sample: List[SampleValues] = data["sample"]
        z_grid: redshift_array = data["z_grid"]

        extra_data = build_extra_data(data)
        if extra_data is not None:
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
        self._shard_key: ShardKeyType = model.shard_key if model.available else None

        self._units: UnitsLike = model.cosmology.units
        self._cosmology: BaseCosmology = model.cosmology

    @property
    def store_id(self) -> int:
        return self._store_id

    @property
    def shard_key(self) -> ShardKeyType:
        return self._shard_key

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
