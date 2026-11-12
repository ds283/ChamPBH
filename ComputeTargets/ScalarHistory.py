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
from math import log, log1p, pi, exp, expm1, sqrt, isinf, isnan, isfinite
from time import perf_counter
from typing import Optional, List, Dict, Any, Tuple

import numpy as np
import ray
from ray import ObjectRef
from scipy.integrate import DenseOutput, DOP853, OdeSolution, Radau
from scipy.interpolate import make_interp_spline
from scipy.optimize import brentq

from ComputeTargets.spline_wrappers import ZSplineWrapper
from CosmologyConcepts import (
    alpha_value,
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

# the drain law and the drain model, and their rules (one-loop-drain prompt 03; orchestrator note 03, ruling 1):
# defined in the leaf module CosmologyConcepts/drain_model.py, which alpha_value imports too, and imported here
# so that every SM.<name> resolves to the same object as before
from CosmologyConcepts.drain_model import (
    DRAIN_LAW_FIXED,
    DRAIN_LAW_ONE_LOOP,
    DRAIN_LAWS,
    DRAIN_MODEL_FIXED,
    DRAIN_MODEL_ONE_LOOP,
    DRAIN_MODELS,
    check_drain_law,
    law_of,
)
from CosmologyConcepts.Models.AbstractModel import (
    AbstractModel,
    ClassicalTurningPoint,
    OneLoopTurnaround,
    RealizedModel,
    Seed,
    _turning_point_unresolvable,
)
from CosmologyModels import BaseCosmology
from CosmologyModels.GenericEOS.LambdaCDM_GenericEOS import LambdaCDM_GenericEOS
from datastorekit import DatastoreObject
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
from .dark_radiation import DarkRadiationLedger
from .exceptions import ComputationFailureError

# useful constants, calculated once and cached to speed up the numerical integration
PISQ_OVER_30 = pi * pi / 30.0
LOG_PISQ_OVER_30 = log(PISQ_OVER_30)
LOG_2 = log(2.0)

# key under which the number of reflections of integrate_scalar_history (floor-rule reflections and
# unresolved turnarounds, inelastic-reflection prompt 05) is stored in ScalarHistory.extra_metadata.
# They are elastic at alpha = 0; at alpha > 0 each drains.
# A count of zero is never stored, so an absent key means there were none.
REFLECTIONS_KEY = "number_reflections"

EXPECTED_SOL_LENGTH = 5

# the two Jordan-frame temperatures, in MeV, at which fixed_T_values reads phi and
# rho_NP/rho_R,J for the ScalarHistory row (science-readiness prompt 06b; README §2 (n)).
# Fixed constants: not a run option and not part of the lookup key
FIXED_T_JORDAN_HIGH_MEV = 1.0
FIXED_T_JORDAN_LOW_MEV = 0.07

# the interior points at which integrate_scalar_history samples pi on each accepted step, besides
# its two ends, to find turning points (inelastic-reflection prompt 02; orchestrator note 02,
# ruling 15, as surfing_turning_points samples g): two sign changes on one step are both found
# unless they are closer than a ninth of it.
# Since electroweak-eos prompt 01a (its README §8, U8) an interior pair of changes is kept only if
# the solver resolves it: unless the samples show exactly one change running end to end, two
# consecutive changes whose excursion (the largest |pi| over the samples between them) is not
# greater than the step's tolerance on pi, atol + rtol max(|pi_low|, |pi_high|), are both dropped
# (resolved_sign_changes). The interpolant's swing below that tolerance is not the field turning
TURNING_POINT_INTERIOR_SAMPLES = 8

# the gate (inelastic-reflection prompt 03; README §6.1 U4 (b'), §6.2 P6), applied at alpha > 0 only:
# A_0 of w_gate = A_k^2/(A_k^2 + A_0^2), on the non-adiabaticity A_k = max |dm_eff/dt|/m_eff^2 over the
# half-oscillation ending at the candidate. The user ruled A_0 = 1 on 2026-10-28 (README §6.1), the
# physical threshold |m'|/m^2 = 1 below which production is adiabatically suppressed. There is no
# stopping rule: the user withdrew w_rate the same day (orchestrator note 03, ruling 16)
NONADIABATICITY_SCALE = 1.0

# the unresolved turnaround (inelastic-reflection prompt 05; README §8.1 R8, §8.2 P18), at alpha > 0
# only. The user ruled "with `K=3` and `phi_t = 1E-8 Mp` as you propose": an infalling field whose
# classical turning point phi_t lies below the threshold, or whose test-field ratio R exceeds 1, is
# reflected as soon as phi < K phi_t, without its turnaround being resolved. Neither is a key.
#   UNRESOLVED_REFLECTION_FACTOR:      K
#   UNRESOLVED_PHI_T_THRESHOLD_MP:     the threshold on phi_t, in units of M_P; multiplied by the
#                                      units' PlanckMass where it is used
# unresolved_turnaround reads both at call time, so that a patch of either changes the rule
UNRESOLVED_REFLECTION_FACTOR = 3.0
UNRESOLVED_PHI_T_THRESHOLD_MP = 1.0e-8

# the parking cut (inelastic-reflection prompt 06; README §8.1 R14, R16, §8.2 P18, P21), at alpha > 0 on
# a model with a wall only (P22: natural mode runs none of it). None is a key. Each is read where it is
# used, at call time, so that a patch of any of them changes it
#   PARKING_EPSILON:                 epsilon of the cut rho_kin,E/rho_R,E < epsilon, tested at every
#                                    candidate after the history's first (R14: "relative to `rho_R`,
#                                    although the exact value of `epsilon` used may need to be tuned")
#   PARKING_ADIABATICITY_THRESHOLD:  m_eff/H_E of the zero-lag tracker: the cut parks only where it is at
#                                    least this (R14). A parked step end below it was a failure of the
#                                    history in prompt 06; since prompt 07 the field is released there,
#                                    at the root of m_eff/H_E = this on the step (R16: "I suggest
#                                    `m/H < 10`")
#   PARKING_PI_GUARD_MP:             P21's guard: |pi_in| below this, in units of M_P, at a reflection
#                                    where the cut did not fire is a failure of the history ("a sign of
#                                    a bug", R14); multiplied by the units' PlanckMass where it is used
PARKING_EPSILON = 1.0e-6
PARKING_ADIABATICITY_THRESHOLD = 10.0
PARKING_PI_GUARD_MP = 1.0e-9

# the release (inelastic-reflection prompt 07; README §8.1 R16): brentq's tolerances for the root of
# m_eff/H_E = PARKING_ADIABATICITY_THRESHOLD on a parked step's segment, in N (orchestrator note 07,
# ruling 2). Neither is a key. RELEASE_ROOT_RTOL is the smallest brentq accepts; at N ~ 40-150 it and
# RELEASE_ROOT_XTOL put the root within about 1e-13 e-folds, so that m_eff/H_E there is the threshold
# to |d(m_eff/H_E)/dN| times that, about 1e-12 (log 07 §1.2 derives it)
RELEASE_ROOT_XTOL = 1.0e-14
RELEASE_ROOT_RTOL = 4.0 * float(np.finfo(float).eps)

# the relativistic margins (inelastic-reflection prompt 08; README §6.2 P6, §8.1 R13, §8.3 U9), computed by
# integrate_history after the integration, at alpha > 0 on a model with a wall only. Neither is a key
#   MARGIN_THRESHOLD:     ln 10 (P6): a margin ln(p/m) below it is a failure of its kind, warned of and never
#                         refused (R13)
#   MARGIN_WEIGHT_SHARE:  U9's energy weight: the worst margins and the failing shares run over the
#                         generations with Delta K_j/K(N_final) >= this
MARGIN_THRESHOLD = log(10.0)
MARGIN_WEIGHT_SHARE = 1.0e-3

# the drain law (one-loop-drain prompt 01; README §8.2, §8.8 P22, U9, U10; the measured law of prompt 02c,
# README §8.10), an input of integrate_scalar_history, integrate_history and tune_history; since prompt 03 a key
# of every stored history, through alpha_value's drain_model (README §8.11 P27). DRAIN_LAW_FIXED,
# DRAIN_LAW_ONE_LOOP and DRAIN_LAWS are defined in CosmologyConcepts/drain_model.py and imported above
# (orchestrator note 03, ruling 1); this comment and the constants below stay here
#   DRAIN_LAW_FIXED:     alpha_eff = alpha w_gate at every candidate, 0 <= alpha < 1: the law to
#                        VERSION_LABEL 2026.9.0, the default, unchanged
#   DRAIN_LAW_ONE_LOOP:  alpha carries the law's strength A, finite and > 0 (A = 0 is natural mode, asked
#                        for as the fixed law at alpha = 0); each candidate is classified in the loop
#                        (drain_regime) and drains (drain_alpha_effective)
#                          alpha_eff = min(DRAIN_ALPHA_CAP, A f) w_gate,
#                        with f = drain_fraction(regime, ell, L, ln R) the measured law that the user adopted on
#                        2026-11-05 (README §8.10, U13 re-ruled), in place of A 2.7/ell and the "zero" and "edge"
#                        gap prescriptions, which are removed (decision 2):
#                          one_loop, ell >= 1:  d = ell + DRAIN_ONE_LOOP_B0 - DRAIN_ONE_LOOP_C ln L;
#                                               min(DRAIN_F_CAP, 1 - exp(-DRAIN_ONE_LOOP_A/d)) if d > 0, else
#                                               DRAIN_F_CAP;
#                          one_loop, ell < 1:   DRAIN_F_PLATEAU;
#                          gap:                 0 if ln R <= DRAIN_GAP_R0 or s = 1 + DRAIN_GAP_Q ln L <= 0, else
#                                               1 - exp(-DRAIN_GAP_G (ln R - DRAIN_GAP_R0)^DRAIN_GAP_P s);
#                          classical:           exp(ln R) S(L) (drain_S_of_L; decision 3, "if it's cheap, keep the
#                                               table");
#                          unassessed:          0, counted (P19)
#                        ell is the turnaround's, ln R the assessment's ClassicalTurningPoint.log_R, and
#                        L the model's wall variable at the assessment's ln E (RealizedModel.drain_L; the model
#                        defines it, with no default, by the user's ruling of 2026-11-05, and a model without it
#                        is refused under this law: check_drain_law_model). For ExponentialModel it is
#                        ln(1 + E/V0) at every n: U15 (b), ruled 2026-11-05, "OK, let's take option (b) for now".
#                        At n = 1 it is M/phi_t, the variable stage 2 measured; at n = 2 crossover's measurement
#                        supports it
# and the law's constants (P17), each with its source. The fitted ones are
# prompts/one-loop-drain/source/crossover/out/fit_law.txt's fits to every stage-2 point, rounded as the reference
# implementation (fraction_adopted in prompts/one-loop-drain/source/crossover/measured_law_map.py) rounds them:
#   DRAIN_ALPHA_CAP:    0.99 (U10, "0.99 is OK"), the probe's cap; the drain refuses alpha_eff >= 1
#   DRAIN_ONE_LOOP_A:   2.69, the Born coefficient of the one-loop fraction (BriefingNotes/wall_backreaction.tex,
#                       "The inelastic fraction"), held fixed in the fit
#   DRAIN_ONE_LOOP_B0:  0.9691, the fitted shift of ell (fit_law.txt: within 0.074 of 27 one-loop points)
#   DRAIN_ONE_LOOP_C:   0.41, the fitted coefficient of ln L in that shift
#   DRAIN_ELL_PLATEAU:  1, the ell below which the plateau holds (README §8.10)
#   DRAIN_F_CAP:        0.95, the one-loop cap, and DRAIN_F_PLATEAU, 0.9, the plateau below ell = 1: the user's
#                       adopted judgement, qualitative behaviour (README §8.10; stage 2 measured 0.88-0.98 and
#                       0.80-0.97 there)
#   DRAIN_GAP_G, DRAIN_GAP_R0, DRAIN_GAP_P, DRAIN_GAP_Q:  0.0198, 1.5601, 1.5472 and 0.841, the gap fit's
#                       amplitude, threshold in ln R, power and coefficient of ln L (fit_law.txt: within 0.062 of
#                       34 gap points)
# The gap's s <= 0 branch (L <= e^{-1/DRAIN_GAP_Q} = 0.304, where README §8.10's expression is negative) is the
# user's ruling of 2026-11-05 on orchestrator note 02c's ruling 9: "I think probably we set `f = 0` there"
DRAIN_ALPHA_CAP = 0.99
DRAIN_ONE_LOOP_A = 2.69
DRAIN_ONE_LOOP_B0 = 0.9691
DRAIN_ONE_LOOP_C = 0.41
DRAIN_ELL_PLATEAU = 1.0
DRAIN_F_CAP = 0.95
DRAIN_F_PLATEAU = 0.9
DRAIN_GAP_G = 0.0198
DRAIN_GAP_R0 = 1.5601
DRAIN_GAP_P = 1.5472
DRAIN_GAP_Q = 0.841

# S(L), the classical wall's production energy ratio (README §8.10, decision 3): the 21 values of
# prompts/one-loop-drain/source/crossover/out/stage1a_S_of_L.txt (stage 1a, the classical bounce off
# V0 (e^{M/phi} - 1); columns L and S(L)), as the same decimal literals. drain_S_of_L interpolates linearly in
# (ln L, ln S), by np.interp on their np.log as the reference implementation's S_of does; holds S(0.3) below
# L = 0.3; and above L = 134 continues as S(134) (134/L)^2, since L^2 S tends to a constant
# (production_energy_ratio.tex). The table is 0.1% high from its quadrature (crossover stage 2, V2q), which is
# immaterial here
DRAIN_S_TABLE_L = (
    0.3,
    0.5,
    0.7,
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    10.0,
    13.0,
    16.0,
    20.0,
    30.0,
    40.0,
    70.0,
    90.0,
    110.0,
    134.0,
)
DRAIN_S_TABLE_S = (
    1.0041e-02,
    9.7145e-03,
    9.3185e-03,
    8.6688e-03,
    6.6167e-03,
    5.0735e-03,
    3.9750e-03,
    3.1852e-03,
    2.6041e-03,
    2.1664e-03,
    1.8292e-03,
    1.3529e-03,
    9.2294e-04,
    6.6916e-04,
    4.6603e-04,
    2.3302e-04,
    1.3936e-04,
    4.9353e-05,
    3.0600e-05,
    2.0810e-05,
    1.4204e-05,
)
_DRAIN_S_LOG_L = np.log(np.array(DRAIN_S_TABLE_L))
_DRAIN_S_LOG_S = np.log(np.array(DRAIN_S_TABLE_S))

# the drain model, named in the "drain" record (README §8.10, decision 1) and, since prompt 03, stored as
# alpha_value's drain_model, a key of every history (README §8.11 P27). DRAIN_MODEL_FIXED and DRAIN_MODEL_ONE_LOOP
# ("one-loop-measured-v1") are defined in CosmologyConcepts/drain_model.py, with law_of, the one mapping from a
# model to its law, and imported above. DRAIN_MODEL_ONE_LOOP names the measured law and its version: THE VERSION
# MOVES whenever drain_fraction, its constants above or the S table change, so that a new version is a new key value

# the regimes of a candidate under the one-loop law (README §8.2): peak_estimate's classification,
# computed once, in the loop (P18), with a one-loop solution of ell <= 0 called gap (P24)
REGIME_ONE_LOOP = "one_loop"
REGIME_GAP = "gap"
REGIME_CLASSICAL = "classical"
REGIME_UNASSESSED = "unassessed"
DRAIN_REGIMES = (REGIME_ONE_LOOP, REGIME_GAP, REGIME_CLASSICAL, REGIME_UNASSESSED)

# the warnings of a history (inelastic-reflection prompt 05; README §8.1 R6, §8.2 P23), returned by
# integrate_scalar_history and printed once per history by tune_history. Each line begins with one
# of these fixed phrases, one per kind, and warning_kind returns it; tune_history prints the first
# line of each kind it sees and no other of that kind (orchestrator note 05, ruling 1). None names
# an M: where the regimes meet depends on the potential
#   WARNING_NEARLY_ELASTIC:       dissipation mode, R < 1 at the first impact
#   WARNING_REGIME_UNASSESSED:    dissipation mode, the first impact has no assessment
#   WARNING_TURNING_POINT_BELOW:  natural mode, a classical turning point below the threshold
#   WARNING_WALL_UNASSESSED:      natural mode, the model call at the first floor reflection failed
# and the margins' kinds (inelastic-reflection prompt 08 §1.5; README §8.1 R13), whose lines are on the
# payload's "margin_warnings", never on "warnings" (orchestrator note 08, ruling 1)
#   WARNING_BIRTH_MARGIN:         dissipation mode, a worst birth margin below ln 10
#   WARNING_REFLECTION_MARGIN:    dissipation mode, a worst reflection margin below ln 10
#   WARNING_TRACKER_MARGIN:       dissipation mode, a worst tracker margin below ln 10
#   WARNING_MARGINS_FAILED:       dissipation mode, the margins could not be computed
WARNING_NEARLY_ELASTIC = (
    "dissipation mode: the reflections are expected to be nearly elastic in this regime"
)
WARNING_REGIME_UNASSESSED = (
    "dissipation mode: the regime could not be assessed at the first impact"
)
WARNING_TURNING_POINT_BELOW = "natural mode: the classical turning point lies below"
WARNING_WALL_UNASSESSED = "natural mode: the wall could not be assessed"
WARNING_BIRTH_MARGIN = (
    "dissipation mode: the spectrum's peak is not relativistic at its birth"
)
WARNING_REFLECTION_MARGIN = (
    "dissipation mode: the spectrum's peak is not relativistic at a later bounce"
)
WARNING_TRACKER_MARGIN = (
    "dissipation mode: the spectrum's peak is not relativistic on the late tracker"
)
WARNING_MARGINS_FAILED = (
    "dissipation mode: the relativistic margins could not be computed"
)
WARNING_KINDS = (
    WARNING_NEARLY_ELASTIC,
    WARNING_REGIME_UNASSESSED,
    WARNING_TURNING_POINT_BELOW,
    WARNING_WALL_UNASSESSED,
    WARNING_BIRTH_MARGIN,
    WARNING_REFLECTION_MARGIN,
    WARNING_TRACKER_MARGIN,
    WARNING_MARGINS_FAILED,
)

# the stepper label returned by compute_scalar_history; main.py pre-registers it as
# IntegrationSolver(label="Radau+kinematic-cap", stepping=1). Stepping 1 since reflection-restart
# prompt 03 (README §6.2 P6): the step loop gives a Radau restarted with pi > 0 F3's first step and
# guards that step (_restart_first_step, _outward_step_flagged), so every history that reflects, drains
# or is released may change value. Stepping 0 is the loop of integrator-remediation prompt 01 to
# VERSION_LABEL 2026.8.0, which main.py still registers
SCALAR_HISTORY_STEPPER_LABEL = "Radau+kinematic-cap-stepping1"

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

# The restart of a Radau instance with pi > 0 (reflection-restart prompt 03; README §6.3, the user's
# ruling U6 "F3 plus the guard"; .documents/reflection-restart-verification.md §1.5). After a floor
# reflection close to the wall the field moves outwards and the wall's push beats the matter pull, so
# the kinematic cap does not bind and a new Radau's first attempt was SciPy's own initial step, h*omega
# of order 10^3-10^4 of the oscillation the restart's Jacobian sees. SciPy rejected it on its ordinary
# error estimate, then accepted the cut attempt on its refined estimate, which under-reads there by
# 10^5: an endpoint near the wall and an interpolant through phi < 0 (the diagnosis,
# .documents/post-reflection-step-2026-11-02/). Two things answer it, at every restart with pi > 0
# (after a reflection, a drain or a release; P7), and nowhere else:
#   F3:        the restarted Radau's first_step is _restart_first_step's, f phi/pi clamped to
#              [h_floor, global_max_step], so its first attempt never reaches that h*omega;
#   the guard: the instance's first accepted step is tested by _outward_step_flagged (P1's flag,
#              either condition) before it is used for anything; a flagged step is discarded and
#              retaken from its start with first_step = h/2, and a retake below h_floor is a failure
#              of the history. IntegrationResult.restart_retakes counts the retakes.
# The history's start, a restart with pi <= 0 and the parked phase (DOP853) are untouched.

# the flag of P1 (reflection-restart README §6.2), the guard's test: a step's interpolant is read at
# this many equally spaced points, its two ends included
OUTWARD_FLAG_SAMPLES = 50


def _restart_first_step(
    y, global_max_step: float, h_floor: float, cap_fraction: float
) -> Optional[float]:
    """
    F3 (reflection-restart prompt 03 §1.1): the first step of a Radau restarted at the state y, f phi/pi
    clamped to [h_floor, global_max_step], where pi > 0; None (SciPy's own choice) where pi <= 0.
    integrate_scalar_history looks it up at call time, so that a test may replace it.
    """
    if not y[1] > 0.0:
        return None
    return min(global_max_step, max(h_floor, cap_fraction * y[0] / y[1]))


def _outward_step_flagged(
    interpolant,
    N_low: float,
    N_high: float,
    phi_start: float,
    pi_start: float,
    pi_end: float,
    cap_fraction: float,
) -> bool:
    """
    The guard's test (reflection-restart prompt 03 §1.2; P1's flag, README §6.2, ruled as the signature
    in §6.3): a step that began with pi_start > 0 is flagged if pi_end/pi_start < 1/2 while pi > 0 at
    every sample of its interpolant (the ratio condition), or if the interpolant's least phi is below
    (1 - f) phi_start (the phi condition). OUTWARD_FLAG_SAMPLES samples, both ends included.
    integrate_scalar_history applies it to the first accepted step of a Radau restarted with pi > 0
    only, and looks it up at call time. tools/outward_step_scan.py (classify_step) holds the same
    definition.
    """
    y = interpolant(np.linspace(N_low, N_high, OUTWARD_FLAG_SAMPLES))
    sign_change = bool(np.any(y[1] <= 0.0))
    ratio = (pi_end / pi_start) < 0.5 and not sign_change
    return bool(ratio or np.min(y[0]) < (1.0 - cap_fraction) * phi_start)


# one reflection performed by integrate_scalar_history: the e-fold number, the field value and the
# (negative) inward velocity at which it was performed. Elastic at alpha = 0; at alpha > 0 it drains
# (inelastic-reflection prompt 02)
#   kind:  "floor" (the floor rule: the wall is thinner than a representable step) or "unresolved"
#          (inelastic-reflection prompt 05: at alpha > 0, phi < K phi_t with phi_t below the
#          threshold or R > 1). Every reflection at alpha = 0 is "floor". The loop always passes it;
#          the default None exists only so that a Reflection built by keyword without it, as a test's
#          stub does, still builds (orchestrator note 05, ruling 5)
Reflection = namedtuple(
    "Reflection",
    [
        "N",
        "phi_Einstein",
        "pi_Einstein_in",
        "kind",
    ],
    defaults=[None],
)

# one turning point of phi seen by integrate_scalar_history (inelastic-reflection prompt 02): a sign
# change of pi on an accepted step's interpolant, sampled at its two ends and at
# TURNING_POINT_INTERIOR_SAMPLES interior points, located at the root of pi (brentq,
# xtol = 1e-15); a single end-to-end change is located exactly as first_bounce locates one
#   N:             e-fold number of the turning point
#   phi_Einstein:  the Einstein-frame field there
#   log_T_Jordan:  ln of the Jordan-frame temperature there, in the cosmology's units
#   kind:          "wall" where pi goes from - to + (turned back by the potential), "plateau" where
#                  it goes from + to - (turned back by the matter coupling)
#   nonadiabaticity: A_k, the largest non-adiabaticity H_E |d ln m_eff^2/dN|/(2 m_eff) over the
#                  samples of the half-oscillation that ends here (inelastic-reflection prompt 03);
#                  None at alpha = 0
#   m_eff_over_H:  m_eff/H_E at the turning point itself (prompt 03); None at alpha = 0. With
#                  nonadiabaticity, the plateau diagnostic of README §6.1 U4 (b')
TurningPoint = namedtuple(
    "TurningPoint",
    [
        "N",
        "phi_Einstein",
        "log_T_Jordan",
        "kind",
        "nonadiabaticity",
        "m_eff_over_H",
    ],
)

# one sign change of pi between two consecutive samples of an accepted step (electroweak-eos
# prompt 01a), as observe_turning_points finds them
#   N_before:  N of the last sample before the change at which pi is not 0.0
#   N_after:   N of the first sample after it at which pi is not 0.0, of the other sign
#   N_zero:    N of the first sample exactly 0.0 between those two, or None; if not None it is the
#              turning point
#   kind:      "wall" where pi goes from - to +, "plateau" where it goes from + to -
SignChange = namedtuple("SignChange", ["N_before", "N_after", "N_zero", "kind"])


def turning_point_tolerance(
    pi_low: float, pi_high: float, atol: float, rtol: float
) -> float:
    """
    The tolerance to which Radau holds pi on an accepted step (electroweak-eos prompt 01a; README
    §8.3 U8): atol + rtol max(|pi_low|, |pi_high|), with pi_low and pi_high pi at the step's two
    nodes and atol, rtol the scalars its Radau instance was built with. SciPy's Radau scales each
    component's error estimate by atol + rtol max(|y_old|, |y_new|) and accepts a step whose RMS
    over the components is at most 1; pi is the state's component y[1], unscaled.
    """
    return atol + rtol * max(abs(pi_low), abs(pi_high))


def resolved_sign_changes(
    N_samples, pi_samples, tolerance: float
) -> Tuple[List[SignChange], int]:
    """
    The sign changes of pi that a step's samples resolve, in order, and the number of pairs dropped
    (electroweak-eos prompt 01a; README §8.3 U8 and U9).

    The sign changes are found between consecutive samples, as observe_turning_points found them
    before 01a: a sample exactly 0.0 carries no sign, and a change across one or more such samples is
    recorded with the first of them (N_zero); a touch of zero, with the same sign on either side, is
    no change. Then the changes are walked in order from the step's start. For a change i that has a
    successor i + 1, the excursion between them is the largest |pi| over the samples strictly
    between the two crossings (from change i's N_after to change i + 1's N_before, all of the
    opposite sign to pi before change i). If the excursion is not greater than `tolerance`, both
    changes are dropped and the walk goes on at i + 2; otherwise change i is kept and the walk goes
    on at i + 1. A change with no successor is kept, so a single change is always kept, whatever
    its size.

    When no pair is dropped, the changes returned are those found before 01a, in the same order and
    with the same N.

    :param N_samples: the samples' N, in increasing order, the step's two nodes first and last
    :param pi_samples: pi at each sample, plain floats
    :param tolerance: the step's tolerance on pi (turning_point_tolerance)
    :return: (the kept SignChanges in order, the number of pairs dropped)
    """
    # the changes as indices: (j_before, j_after, j_zero or None, kind)
    changes = []
    last_sign: int = 0
    last_j: Optional[int] = None
    zero_j: Optional[int] = None
    for j, pi_j in enumerate(pi_samples):
        if pi_j == 0.0:
            if zero_j is None:
                zero_j = j
            continue
        sign: int = 1 if pi_j > 0.0 else -1
        if last_sign != 0 and sign != last_sign:
            changes.append((last_j, j, zero_j, "wall" if last_sign < 0 else "plateau"))
        last_sign = sign
        last_j = j
        zero_j = None

    kept: List[SignChange] = []
    dropped: int = 0
    i: int = 0
    while i < len(changes):
        j_before, j_after, j_zero, kind = changes[i]
        if i + 1 < len(changes):
            j_end: int = changes[i + 1][0]
            excursion: float = max(
                abs(pi_samples[j]) for j in range(j_after, j_end + 1)
            )
            if excursion <= tolerance:
                dropped += 1
                i += 2
                continue
        kept.append(
            SignChange(
                N_before=N_samples[j_before],
                N_after=N_samples[j_after],
                N_zero=None if j_zero is None else N_samples[j_zero],
                kind=kind,
            )
        )
        i += 1
    return kept, dropped


# one candidate for a drain of the condensate's kinetic energy into the dark-radiation ledger
# (inelastic-reflection prompts 02, 03 and 05; README §6.2 P4, the fields prompts 02 and 03 can
# fill). At alpha > 0 every drain point and every reflection is one, alpha_eff = 0.0 included
# (prompt 03); the ledger holds exactly those with alpha_eff > 0.0
#   N:                       the drain point: the end of the accepted step at which the drain was
#                            made, or the reflection's N
#   kind:                    "turning_point" (after a wall-side TurningPoint), "floor" (a floor-rule
#                            reflection) or "unresolved" (a reflection at phi < K phi_t, prompt 05)
#   N_turn:                  the turning point's N, or N for a reflection
#   phi_turn:                phi at the turning point, or at the reflection
#   pi_before, pi_after:     pi at the drain, before and after it; pi_after is pi_before (-pi_before
#                            at a reflection) when alpha_eff == 0.0
#   log_rho_kin:             ln rho_kin,E = ln(H_E^2 pi_before^2/2) before the drain
#   alpha_eff:               the fraction drained, alpha * w_gate (prompt 03)
#   delta_K:                 the ledger's entry, alpha_eff rho_kin,E e^{4N}; 0.0 if alpha_eff == 0.0
#   delta_rho_over_rho_rad:  alpha_eff rho_kin,E/rho_R,E; 0.0 if alpha_eff == 0.0
#   H_Einstein_before:       H_E from the state and the ledger before the drain
#   H_Einstein_after:        H_E from the state and the ledger after it (the same float as
#                            H_Einstein_before when alpha_eff == 0.0)
#   nonadiabaticity:         A_k of the half-oscillation that ends at the candidate: the wall-side
#                            TurningPoint's, or the infall's up to a reflection (prompt 03). Recorded
#                            at an unresolved candidate too, where the gate does not read it
#   w_gate:                  A_k^2/(A_k^2 + NONADIABATICITY_SCALE^2) (gate_weight) at a resolved
#                            candidate; exactly 1.0 at an unresolved one, floor reflections included
#                            (prompt 05; README §8.1 R10, §8.2 P20), where gate_weight is not called
#   resolved:                True for a drain point after a wall-side TurningPoint, False for a
#                            reflection of either kind (prompt 05)
#   phi_assessed:            phi at the step start where the assessment the candidate was judged on
#                            was made (prompt 05): a reflection's own step start, or, if the
#                            assessment there returned None, the infall's latest; for a drain point
#                            the last assessment of the infall that ended at its wall-side turning
#                            point. None if the infall was never assessed (a model without a wall,
#                            or None at every step of it); then log_E_assessed and
#                            classical_turning_point are None too
#   log_E_assessed:          ln E there, E = H_E^2 pi^2/2 + V(phi) - V_floor, the energy that reaches
#                            the wall (prompt 05 §0.2)
#   classical_turning_point: the model's ClassicalTurningPoint for (phi_assessed, E): phi_t,
#                            ln omega_t^2, ln R, lambda_t, ln k_ex and ell_t (inelastic-reflection
#                            prompt 04)
Injection = namedtuple(
    "Injection",
    [
        "N",
        "kind",
        "N_turn",
        "phi_turn",
        "pi_before",
        "pi_after",
        "log_rho_kin",
        "alpha_eff",
        "delta_K",
        "delta_rho_over_rho_rad",
        "H_Einstein_before",
        "H_Einstein_after",
        "nonadiabaticity",
        "w_gate",
        "resolved",
        "phi_assessed",
        "log_E_assessed",
        "classical_turning_point",
    ],
)

# the switch to the parked phase (inelastic-reflection prompt 06; README §8.1 R14, R15, §8.2 P19), on
# IntegrationResult.parking; None if the history did not park. Floats and ints only (orchestrator note
# 06, ruling 6): nothing callable, so that it crosses Ray in integrate_history's payload
#   N, log_T_Jordan:           the switch: the cut candidate's N, and ln T_J there
#   candidate_index:           the cut candidate's index in IntegrationResult.injections (never 0)
#   phi_Einstein, pi_Einstein: the field just before the switch: the candidate's state after its drain
#                              (pi is the candidate's pi_after)
#   phi_min, pi_min:           the parked state's field: phi_min on the trace there (the model's
#                              minimum_on_trace) and pi_min = d phi_min/dN (tracker_velocity). They are
#                              the parked phase's own value at N, which its dense output returns there
#   rho_kin_over_rho_rad:      rho_kin,E/rho_R,E at the test, rho_kin,E the candidate's log_rho_kin
#   delta_K:                   the leftover's ledger entry (P19): the drained state's rho_kin,E e^{4N},
#                              rho_kin,E = H_E^2 pi^2/2 from the candidate's H_Einstein_after, pi_after
#   delta_rho_over_rho_rad:    the leftover's rho_kin,E/rho_R,E
#   m_eff_over_H:              m_eff/H_E of the tracker at the switch, the value the cut was decided on
#                              (the ledger after the candidate's drain, before the leftover)
#   H_Einstein_before:         H_E before the candidate (its H_Einstein_before)
#   H_Einstein_parked:         H_E at the parked state, with the leftover in the ledger
#   delta_log_A:               ln A(phi_min) - ln A(phi_Einstein), the switch's move of the coupling
#   parked_steps:              the parked phase's accepted steps (IntegrationResult.accepted_steps
#                              counts both phases). For a released history (prompt 07) the parked
#                              steps up to and including the one the release falls in
#   min_m_eff_over_H:          the smallest m_eff/H_E of the tracker: the switch's and that of every
#                              parked step end, the history's end included. For a released history
#                              (prompt 07) the switch's, the checks that passed and the release's own
#                              (Release.m_eff_over_H); the check that failed lies after the release,
#                              is not part of the parked phase, and is in Release.m_eff_over_H_check
Parking = namedtuple(
    "Parking",
    [
        "N",
        "log_T_Jordan",
        "candidate_index",
        "phi_Einstein",
        "pi_Einstein",
        "phi_min",
        "pi_min",
        "rho_kin_over_rho_rad",
        "delta_K",
        "delta_rho_over_rho_rad",
        "m_eff_over_H",
        "H_Einstein_before",
        "H_Einstein_parked",
        "delta_log_A",
        "parked_steps",
        "min_m_eff_over_H",
    ],
)

# the release of a parked field (inelastic-reflection prompt 07; README §8.1 R16, §8.2 P24), on
# IntegrationResult.release; None if the history was not released. Floats and ints only (orchestrator
# note 07, ruling 11): nothing callable, so that it crosses Ray in integrate_history's payload. The
# release is at the root of m_eff/H_E = PARKING_ADIABATICITY_THRESHOLD on the parked step whose end
# check found the tracker below it; the oscillating loop resumes there with a new Radau instance, and
# the field never parks again (P24)
#   N, log_T_Jordan:           the release, N_rel, the root; and ln T_J there. It is the last node of
#                              the parked phase in the solution's ts, and the first of the oscillating
#                              phase that resumes
#   phi_Einstein, pi_Einstein: the tracker's (phi_min, pi_min) at N_rel: the released state's field,
#                              the parked segment's value there, from which the new Radau starts
#   m_eff_over_H:              m_eff/H_E of the tracker at N_rel, the root's value (the threshold to the
#                              root's tolerance)
#   N_check:                   the N of the check that failed: the parked step's end, or N_final on
#                              the step that ends the history
#   m_eff_over_H_check:        m_eff/H_E there, below the threshold
#   H_Einstein:                H_E at N_rel, with the ledger as it stands (nothing is drained or added
#                              at the release)
#   candidate_index:           the index in IntegrationResult.injections of the first candidate after
#                              the release, which is the number of candidates before it; equal to
#                              len(injections) if no candidate follows
#   steps_after:               the accepted steps after the release, all oscillating
#                              (IntegrationResult.accepted_steps counts every phase)
Release = namedtuple(
    "Release",
    [
        "N",
        "log_T_Jordan",
        "phi_Einstein",
        "pi_Einstein",
        "m_eff_over_H",
        "N_check",
        "m_eff_over_H_check",
        "H_Einstein",
        "candidate_index",
        "steps_after",
    ],
)

# the end of a parked phase, handed back from the parked phase to the loop (prompt 07; orchestrator note
# 07, ruling 1): a return value, never an exception
#   N, state:  the history's end (released False), or the release's N_rel and its state, the parked
#              segment's value there, from which the oscillating loop resumes (released True)
#   released:  whether the field was released
_ParkedPhaseEnd = namedtuple("_ParkedPhaseEnd", ["N", "state", "released"])

# what integrate_scalar_history returns
#   solution:                    scipy OdeSolution over [N_start, N_final], right-continuous at its
#                                nodes (alt_segment=True; inelastic-reflection prompt 02): at a
#                                drain or a reflection it returns the state after it
#   N_final:                     e-fold number at which ln T_J crossed log_T_stop
#   final_state:                 StateVector at N_final
#   nfev:                        RHS evaluations requested by the loop (including Jacobian probes)
#   accepted_steps:              accepted Radau steps
#   steps_rejected_by_exception: step attempts abandoned because the RHS raised on a trial state
#   reflections:                 list of Reflection, of both kinds: floor-rule events and, at
#                                alpha > 0, unresolved reflections (inelastic-reflection prompt 05)
#   max_wall_to_kinetic_ratio:   largest W/(pi^2/2) at a reflection (guard G2), or None
#   turning_points:              list of TurningPoint, in order, at every alpha
#                                (inelastic-reflection prompt 02)
#   injections:                  list of Injection, in order; empty at alpha = 0
#                                (inelastic-reflection prompt 02). At alpha > 0, one per candidate,
#                                alpha_eff = 0.0 included (prompt 03)
#   dropped_drains:              the drains armed by a wall-side TurningPoint that never reached a
#                                drain point: dropped by a later turning point, a floor reflection
#                                or the history's end (inelastic-reflection prompt 03). Counted at
#                                every alpha, alpha = 0 included; at alpha > 0 it is the wall-side
#                                turning points less the "turning_point" candidates
#   warnings:                    tuple of the history's warning lines (inelastic-reflection prompt
#                                05), each beginning with one of WARNING_KINDS; empty when there are
#                                none. A ComputationFailureError raised by the loop carries the
#                                warnings collected before it as its attribute `warnings`
#   parking:                     the Parking record of the switch to the parked phase
#                                (inelastic-reflection prompt 06), or None if the history did not
#                                park. After the switch the solution's segments are ParkedSegments
#   release:                     the Release record of the release of the parked field (inelastic-
#                                reflection prompt 07), or None if the history was not released. Its
#                                segments from Release.N on are Radau's again
#   restart_retakes:             the first steps of Radau instances restarted with pi > 0 that the guard
#                                discarded and retook (reflection-restart prompt 03 §1.2). The default
#                                0 exists only so that a stand-in built by keyword without it still
#                                builds; the loop always passes it
#   drain:                       the DrainRecord of the drain law (one-loop-drain prompt 01 §1.4), the
#                                payload's "drain"; the default None exists for stand-ins, as
#                                restart_retakes' 0 does, and the loop always passes it
#   peak_estimates:              under the one-loop law, the PeakEstimate (or None, unassessed) of each
#                                candidate, parallel to injections, as the loop's classification computed
#                                it, which history_margins reuses (P18; orchestrator note 01, ruling 5);
#                                None under the fixed law, where history_margins calls peak_estimate as
#                                before. Neither field is carried by an existing payload key
#   unresolved_turning_point_pairs: the interior pairs of sign changes of pi that observe_turning_points
#                                dropped over the history because the solver does not resolve them
#                                (resolved_sign_changes; electroweak-eos prompt 01a, README §8.3 U8, U9).
#                                Counted at every alpha, alpha = 0 included. A dropped pair is as if it
#                                were not there: no TurningPoint, no drain armed or dropped, no boundary
#                                of the half-oscillation, no bounce row. It is not a payload key, not a
#                                column and not part of any record or fingerprint. The default 0 exists
#                                for stand-ins, as restart_retakes' does, and the loop always passes it
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
        "turning_points",
        "injections",
        "dropped_drains",
        "warnings",
        "parking",
        "release",
        "restart_retakes",
        "drain",
        "peak_estimates",
        "unresolved_turning_point_pairs",
    ],
    defaults=[0, None, None, 0],
)

# the drain law's record of one candidate under the one-loop law (one-loop-drain prompt 01 §1.4), in the
# order of IntegrationResult.injections. Plain values only, so that it crosses Ray and pickles
#   regime:     REGIME_ONE_LOOP, REGIME_GAP, REGIME_CLASSICAL or REGIME_UNASSESSED (drain_regime)
#   ell:        ln(k_ex/m_ta) of the one-loop turnaround (regime one_loop only), else None
#   alpha_eff:  the fraction drained (drain_alpha_effective), the Injection's alpha_eff
#   w_gate:     the gate, the Injection's w_gate: gate_weight(A_k) at a resolved candidate, 1.0 exactly at
#               an unresolved one
#   log_E:      ln E of the assessment the candidate was judged on (the Injection's log_E_assessed), the
#               impact energy; None if unassessed
#   log_H:      ln H_E before the candidate (ln of the Injection's H_Einstein_before)
#   L:          the law's L, the model's (drain_law_inputs), the number drain_alpha_effective was given;
#               None if unassessed (one-loop-drain prompt 02c §1.3; appended, orchestrator note 02c ruling 7)
#   log_R:      the law's ln R, the assessment's ClassicalTurningPoint.log_R, as given; None if unassessed
DrainCandidate = namedtuple(
    "DrainCandidate",
    ["regime", "ell", "alpha_eff", "w_gate", "log_E", "log_H", "L", "log_R"],
)

# the payload's "drain" (one-loop-drain prompt 01 §1.4): the drain law of one integration, and what it did.
# Plain values only (strings, floats, ints, None and tuples of DrainCandidate), so that it crosses Ray
#   law:                       DRAIN_LAW_FIXED or DRAIN_LAW_ONE_LOOP
#   A:                         the one-loop law's A (the alpha argument); None under the fixed law
#   model:                     the drain model and its version, DRAIN_MODEL_ONE_LOOP under the one-loop law and
#                              DRAIN_MODEL_FIXED under the fixed law (one-loop-drain prompt 02c §1.3; README §8.10,
#                              decision 1), where the gap prescription stood (orchestrator note 02c ruling 7)
#   unresolvable_assessments:  the assessments whose classical turning point was None with
#                              _turning_point_unresolvable holding (§1.6): the calls the wall-search fix
#                              answers. Counted under either law: the loop's assessments at every step
#                              start with pi < 0 (alpha > 0) and natural mode's one assessment (alpha = 0)
#   candidates:                under the one-loop law a tuple of DrainCandidate, one per Injection, in
#                              order; None under the fixed law (no instrumentation there: README §6.3
#                              ruling 2)
#   unresolved_declined:       the step starts at which unresolved_turnaround fired but the reflection
#                              would have failed G2, so that no unresolved decision was made there
#                              (unresolved_reflection_refused; one-loop-drain prompt 01 §1.7, the user's
#                              ruling (b)). Counted under either law
DrainRecord = namedtuple(
    "DrainRecord",
    [
        "law",
        "A",
        "model",
        "unresolvable_assessments",
        "candidates",
        "unresolved_declined",
    ],
)

# using named tuples ensures that we never get the fields in the wrong order
#   rho_chi_over_rho_rad: r_chi = rho_chi,E/rho_R,E, the ratio of the dark-radiation component to the
#                         bath that the policy used at this (N, state); 0.0 when it had none
#                         (inelastic-reflection prompt 01; appended last, orchestrator note 01
#                         ruling 6). HubblePolicy reads it in its V = 0 branch
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
        "rho_chi_over_rho_rad",
    ],
)

HubblePolicyData = namedtuple(
    "HubblePolicyData",
    [
        "H_Einstein",
        "H_Jordan",
    ],
)

# one sample of a history, at a point of the sampling axis ln(1 + z_E) (jordan-normalization prompt 03).
#   log_one_plus_z_Jordan: ln(1 + z_J), the Jordan-frame redshift counted back from the end of the
#                          history, computed at sampling as ln(1 + z_E) + ln A(phi_0) - ln A(phi),
#                          with phi_0 the field at the end (a_J = A a_E). It is a column, never a
#                          key: H_J < 0 during a bounce, so a_J is not monotonic and z_J is not a
#                          coordinate. "The end" is wherever the history stopped; once
#                          jordan-normalization prompt 07 lands it is T_J = T_CMB for every stored
#                          history (today a history may stop at another T_stop)
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
        "log_one_plus_z_Jordan",
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
        model: RealizedModel,
        dark_radiation: Optional[DarkRadiationLedger] = None,
    ):
        """
        :param dark_radiation: the ledger of the dark-radiation component rho_chi,E = K(N) e^{-4N}
                               (inelastic-reflection prompt 01), read at the N each call is made
                               with. None (the default), or a ledger with K(N) = 0, takes exactly the
                               old operations. integrate_history always passes its ledger, empty
                               when the history has no rho_chi, so that the one policy it builds sees
                               an injection that prompt 02 adds while Radau runs.
        """
        self.task_label: str = task_label
        self.units: UnitsLike = cosmology.units

        self.cosmology: BaseCosmology = cosmology
        self.model: RealizedModel = model
        self.dark_radiation: Optional[DarkRadiationLedger] = dark_radiation

        self.V_policy: PotentialDerivativePolicy = PotentialDerivativePolicy(
            task_label, cosmology, model
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

    def _get_r_chi(self, N: float, state: StateVector) -> float:
        """
        r_chi = rho_chi,E/rho_R,E at (N, state), from the ledger; exactly 0.0, with no exp or log, when
        there is no ledger or K(N) = 0.
        """
        ledger = self.dark_radiation
        if ledger is None or ledger.is_zero(N):
            return 0.0

        try:
            return ledger.ratio(N, state.log_rhorad_Einstein)
        except OverflowError as e:
            msg = f"!! ODEPolicy ({self.task_label}): math overflow in rho_chi/rho_rad, log_rhorad_Einstein = {state.log_rhorad_Einstein:.5g} | N = {N:.5g}, phi_Einstein = {state.phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {state.pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg) from e

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

        # the dark-radiation component (inelastic-reflection prompt 01): 0.0 when there is none, and
        # then every expression below takes exactly the old operations
        r_chi: float = self._get_r_chi(N, state)

        Sigma: float = 1.0 - 3.0 * self.cosmology.w(T_Jordan)

        # compute R = (Sigma + fm)/(1 + fm)
        # try to avoid any problems with overflow
        R: float
        if fm > 10.0:
            R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        else:
            R = (Sigma + fm) / (1.0 + fm)

        d_logOmega_dphi: float = self.model.d_logOmega_dphi(phi_Einstein)

        log_V = self.V_policy.log_V(phi_Einstein)
        V_over_3H2Mp2 = self.V_policy.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )
        Vprime_over_3H2Mp2 = self.V_policy.Vprime_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
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

        # E = rho_R (1 + f_m)/3H^2 M_P^2, the matter-and-bath factor of the kick. With dark radiation
        # (inelastic-reflection prompt 01) G - V/3H^2 M_P^2 holds rho_chi/3H^2 M_P^2 as well, which is
        # taken out: the quanta are traceless, and must not reach the kick
        E: float
        if r_chi == 0.0:
            E = G - V_over_3H2Mp2
        else:
            E, _ = self.V_policy.matter_and_dark_radiation_fractions(
                G, V_over_3H2Mp2, fm, r_chi
            )
        # E must be positive, because it is proportional to rho_R/H^2
        if E < 0.0:
            # unclear whether we should treat this as a genuine computational error, or whether it just means that rho_r is very small
            # (e.g. at the end of the integration) and should harmlessly be treated as zero
            msg = f"!! ODEPolicy ({self.task_label}): negative value of E = {E:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            # raise ComputationFailureError(msg)
            E = 0.0

        dotH_over_H2_plus_3: float = self.V_policy.Hdot_over_H2_plus_3(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm, r_chi=r_chi
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
            rho_chi_over_rho_rad=r_chi,
        )


class HubblePolicy:
    def __init__(self, model: RealizedModel, units: UnitsLike):
        self.model: RealizedModel = model
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
            # 3 H^2 M_P^2 G = rho_R (1 + f_m) + rho_chi, with rho_chi = r_chi rho_R
            # (inelastic-reflection prompt 01); r_chi is the one ODEPolicy used for this state
            r_chi: float = data.rho_chi_over_rho_rad
            ThreeH2Mp2: float
            if r_chi == 0.0:
                ThreeH2Mp2 = rho_rad * (1.0 + fm) / G
            else:
                ThreeH2Mp2 = rho_rad * (1.0 + fm + r_chi) / G
            H2_Einstein: float = ThreeH2Mp2 / self.CONST_3_MP_SQ

        H_Einstein: float = sqrt(H2_Einstein)

        Omega: float = self.model.Omega(state.phi_Einstein)

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
                    f"!! compute_scalar_history ({self.task_label}): output from ODE RHS has infinity or NaN values at N={N:.8g}"
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
                    f"compute_scalar_history ({self.task_label}): output from ODE RHS has infinity or NaN values"
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

    G1: the model must declare a repulsive wall between the origin and the field
        (reflects_at_origin). The floor rule is a time-scale test, not a wall detector, and the
        reflection is exact only if a wall exists in (0, phi).
    G2: the wall part of the potential fraction, W = 3 (V - V_floor)/(3H^2 Mp^2), must not exceed
        the kinetic fraction pi^2/(2 Mp^2). A field that arrived from outside the wall satisfies
        this; one that has already been stepped into the wall does not.
    """
    model = policy.model
    state = StateVector._make(y)
    label = f" ({task_label})" if task_label is not None else ""

    if not model.reflects_at_origin:
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: the representable-step floor was reached "
            f"(N={N:.10g}, phi_E={state.phi_Einstein:.5g}, pi_E={state.pi_Einstein:.5g}), but the "
            f"model {model.name} does not declare reflects_at_origin, so the elastic "
            f"reflection model cannot be applied"
        )

    log_V_floor = model.log_V_floor
    if log_V_floor is None:
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: the model {model.name} declares "
            f"reflects_at_origin but provides no log_V_floor, so the reflection cannot be checked"
        )

    W, kinetic = _wall_and_kinetic(policy, N, state, log_V_floor)
    ratio = W / kinetic

    if _g2_refuses(W, kinetic):
        raise ComputationFailureError(
            f"integrate_scalar_history{label}: reflection requested inside the wall: "
            f"W/(pi^2/2) = {ratio:.5g} at N={N:.10g}, phi_E={state.phi_Einstein:.5g}, "
            f"pi_E={state.pi_Einstein:.5g} (W = {W:.5g}, pi^2/2 = {kinetic:.5g})"
        )

    return ratio


def _wall_and_kinetic(
    policy: ODEPolicy, N: float, state: StateVector, log_V_floor: float
) -> Tuple[float, float]:
    """
    G2's two sides at one state (one-loop-drain prompt 01 §1.7: factored out of _reflection_guards, the
    expressions unchanged, so that the unresolved turnaround's decision reads the same numbers): the wall
    part of the potential fraction, W = -3 V/(3H^2 Mp^2) expm1(ln V_floor - ln V) = 3(V - V_floor)/(3H^2 Mp^2),
    and the kinetic fraction pi^2/(2 Mp^2), from one policy call at (N, state).
    """
    data: ODEPolicyData = policy(N, state)
    W = -3.0 * data.V_over_3H2Mp2 * expm1(log_V_floor - data.log_V)
    kinetic = 0.5 * state.pi_Einstein * state.pi_Einstein / policy.CONST_MP_SQ
    return W, kinetic


def _g2_refuses(W: float, kinetic: float) -> bool:
    """G2's comparison: a reflection is refused iff not (W <= kinetic), so a NaN refuses."""
    return not (W <= kinetic)


def unresolved_reflection_refused(policy: ODEPolicy, N: float, y: np.ndarray) -> bool:
    """
    Whether G2 would refuse a reflection at (N, y) (one-loop-drain prompt 01 §1.7; the user's ruling (b),
    2026-11-03): _reflection_guards' own expressions (_wall_and_kinetic) and comparison (_g2_refuses).
    The step loop asks it only where unresolved_turnaround has fired, on a model with a wall (G1 and a
    log_V_floor hold there, since the loop assesses the wall only on such a model). Where it is True the
    loop makes no unresolved decision at that step start: the floor rule is tested as usual, with G2
    applied to its reflection as before, and otherwise Radau integrates the infall on to its turning point.

    Why nothing that succeeds today moves: where it is True, the unresolved branch went on to
    _reflection_guards at this same state, which raised "reflection requested inside the wall", and the
    history failed; a failed integration ends a tuning as a failure. So every state at which the decision
    is declined is one that raised before this ruling, and no history or tuning that succeeds today moves.
    """
    W, kinetic = _wall_and_kinetic(
        policy, N, StateVector._make(y), policy.model.log_V_floor
    )
    return _g2_refuses(W, kinetic)


# ---- the gate (inelastic-reflection prompt 03) ----
#
# Module-level, so that they can be tested on synthetic values without an integration, and so that
# integrate_scalar_history, which calls each of them by its module name at call time, can be
# intercepted by unittest.mock.patch (orchestrator note 03, ruling 1)


def log_m_eff_squared(
    model: RealizedModel,
    phi_Einstein: float,
    log_rhorad_Einstein: float,
    log_fm: float,
    Sigma: float,
) -> float:
    """
    ln m_eff^2, the local mass of a chameleon quantum at one state (inelastic-reflection prompt 03
    §1.1; log 03 §1.1 derives it):

        m_eff^2 = V''(phi) + [(ln A)'' + (ln A)'^2] rho_R,E (Sigma + f_m),

    the curvature of V_eff = V + A rho~_c at fixed rho~_c = rho_R,E (Sigma + f_m)/A, the Einstein-frame
    trace that ODERHS couples to. V'' = V [(ln V)'' + (ln V)'^2]. It is not
    AdiabaticComputePolicy.M2eff_over_H2, the perturbation's mass, which carries the source's response
    and -(2 + Hdot/H^2).

    It takes no cosmology and no N, so that a stored sample (phi, ln rho_R,E, ln f_m, Sigma) is
    enough: inelastic-reflection prompt 04 calls it for m_ta. It returns the logarithm: the loop
    needs ln m_eff^2 for d ln m_eff^2/dN, and each term is formed as a sign and the log of its
    magnitude, ln V + ln|(ln V)'' + (ln V)'^2| and ln rho_R,E + ln|[...] (Sigma + f_m)|, summed
    relative to the larger, so that neither V nor rho_R,E is exponentiated and nothing overflows.

    A non-positive or non-finite m_eff^2 is a ComputationFailureError naming the state.
    """

    def failure(reason: str) -> ComputationFailureError:
        return ComputationFailureError(
            f"log_m_eff_squared: {reason} at phi_E = {phi_Einstein:.10g}, "
            f"ln rho_R,E = {log_rhorad_Einstein:.10g}, ln f_m = {log_fm:.10g}, "
            f"Sigma = {Sigma:.10g} ({model.name})"
        )

    d1_V: float = model.d_logV_dphi(phi_Einstein)
    q_V: float = model.d2_logV_dphi2(phi_Einstein) + d1_V * d1_V

    d1_A: float = model.d_logOmega_dphi(phi_Einstein)
    q_A: float = model.d2_logOmega_dphi2(phi_Einstein) + d1_A * d1_A

    try:
        q_trace: float = q_A * (Sigma + exp(log_fm))
    except OverflowError as e:
        raise failure("overflow in f_m") from e

    # (ln|term|, sign) of V'' and of the trace term; a term exactly 0.0 is left out
    terms = []
    if q_V != 0.0:
        terms.append(
            (model.log_V(phi_Einstein) + log(abs(q_V)), 1.0 if q_V > 0.0 else -1.0)
        )
    if q_trace != 0.0:
        terms.append(
            (log_rhorad_Einstein + log(abs(q_trace)), 1.0 if q_trace > 0.0 else -1.0)
        )
    if len(terms) == 0:
        raise failure("m_eff^2 = 0")

    L: float = max(t[0] for t in terms)
    if not isfinite(L):
        raise failure(f"a non-finite term of m_eff^2 (ln = {L!r})")
    total: float = sum(sign * exp(t - L) for t, sign in terms)
    if not (total > 0.0):
        raise failure(f"m_eff^2 <= 0 (m_eff^2 = {total:.5g} e^{L:.10g})")

    return L + log(total)


def step_nonadiabaticity(
    N_samples: np.ndarray, log_m2_samples: np.ndarray, H_samples: np.ndarray
) -> np.ndarray:
    """
    The non-adiabaticity A = |dm_eff/dt|/m_eff^2 = H_E |d ln m_eff^2/dN|/(2 m_eff) at the samples of
    one accepted step (inelastic-reflection prompt 03 §1.1), given their N, ln m_eff^2 and H_E.
    d ln m_eff^2/dN is np.gradient over these samples, in N: nothing is differenced across a step
    boundary (orchestrator note 03, ruling 3). A non-finite value is a ComputationFailureError
    (ruling 8).
    """
    d_log_m2 = np.gradient(log_m2_samples, N_samples)
    with np.errstate(over="ignore", invalid="ignore"):
        A = 0.5 * H_samples * np.abs(d_log_m2) * np.exp(-0.5 * log_m2_samples)
    if not np.isfinite(A).all():
        raise ComputationFailureError(
            f"step_nonadiabaticity: a non-finite non-adiabaticity on the step "
            f"[{N_samples[0]:.10g}, {N_samples[-1]:.10g}]: {A!r}"
        )
    return A


def gate_weight(nonadiabaticity: float) -> float:
    """
    The gate w_gate = A^2/(A^2 + A_0^2), A_0 = NONADIABATICITY_SCALE (inelastic-reflection prompt 03
    §1.2). Above A_0 it is computed as 1/(1 + (A_0/A)^2), which overflows nowhere, so that it is
    finite and in [0, 1] for every finite A >= 0 (orchestrator note 03, ruling 8). A negative or
    non-finite A is a ComputationFailureError.
    """
    A: float = nonadiabaticity
    if not (A >= 0.0 and isfinite(A)):
        raise ComputationFailureError(
            f"gate_weight: the non-adiabaticity must be finite and >= 0, not {A!r}"
        )
    A0: float = NONADIABATICITY_SCALE
    if A > A0:
        r: float = A0 / A
        return 1.0 / (1.0 + r * r)
    return (A * A) / (A * A + A0 * A0)


# ---- the unresolved turnaround (inelastic-reflection prompt 05) ----
#
# Module-level, called by integrate_scalar_history through their module names at call time, so that
# they can be tested on synthetic values and patched (orchestrator note 05, ruling 6), as the gate's
# are


def assess_infall(
    model: RealizedModel, phi_Einstein: float, log_rho_kin: float
) -> Tuple[float, Optional[ClassicalTurningPoint]]:
    """
    The assessment of the wall ahead of an infalling field (inelastic-reflection prompt 05 §1.2):
    (ln E, the model's classical_turning_point(phi, ln E)).

    E is the energy that reaches the wall, rho_kin,E + V(phi) - V_floor (prompt §0.2): across the
    crossing rho_kin + V is conserved (expansion is neglected, as the model's methods neglect it),
    and far out V = V_floor, so the turning point where V(phi_t) - V_floor = E is the same from every
    point of the infall. With rho_kin alone, phi_t would drift outwards as the field climbs the wall.
    ln E is formed in logs from ln rho_kin,E (given), and the model's log_V and log_V_floor, with
    ln(V - V_floor) = -inf where V rounds to V_floor or below; then ln E = ln rho_kin,E exactly.

    The classical turning point is None if V(phi) - V_floor >= E, which with this E means rho_kin,E
    is below the rounding of V - V_floor: the field is at its turning point to rounding. A
    ComputationFailureError from the model propagates. The model must have a wall
    (reflects_at_origin) and a log_V_floor; the caller does not assess one without.
    """
    log_V_floor: float = model.log_V_floor
    log_V: float = model.log_V(phi_Einstein)

    if log_V > log_V_floor:
        log_W: float = log_V + log(-expm1(log_V_floor - log_V))
        if log_W > log_rho_kin:
            log_E: float = log_W + log1p(exp(log_rho_kin - log_W))
        else:
            log_E = log_rho_kin + log1p(exp(log_W - log_rho_kin))
    else:
        log_E = log_rho_kin

    return log_E, model.classical_turning_point(phi_Einstein, log_E)


def unresolved_turnaround(
    phi_Einstein: float, turning_point: ClassicalTurningPoint, PlanckMass: float
) -> bool:
    """
    The decision (inelastic-reflection prompt 05 §1.3; README §8.1 R8): True iff
    phi < K phi_t, and either phi_t < UNRESOLVED_PHI_T_THRESHOLD_MP M_P or ln R > 0. All three
    comparisons are strict. K and the threshold are read here, at call time.
    """
    phi_t: float = turning_point.phi_t
    if not phi_Einstein < UNRESOLVED_REFLECTION_FACTOR * phi_t:
        return False
    return (
        phi_t < UNRESOLVED_PHI_T_THRESHOLD_MP * PlanckMass or turning_point.log_R > 0.0
    )


def warning_kind(line: str) -> str:
    """
    The kind of a warning line: the fixed phrase of WARNING_KINDS it begins with (orchestrator note
    05, ruling 1). A line that begins with none of them is a ValueError.
    """
    for kind in WARNING_KINDS:
        if line.startswith(kind):
            return kind
    raise ValueError(f"warning_kind: not a history warning: {line!r}")


# ---- parking (inelastic-reflection prompt 06; README §8.1 R14, R15, §8.2 P18, P19, P21) ----
#
# Module-level, called by integrate_scalar_history through their module names at call time, so that
# they can be tested on synthetic values and patched (orchestrator note 06, rulings 1 and 4), as the
# gate's and the unresolved turnaround's are


def parking_ratio_test(candidate_index: int, ratio: float) -> bool:
    """
    The cut's ratio test (prompt 06 §1.2; README §8.1 R14): True iff the candidate is not the
    history's first (its first impact, index 0 in IntegrationResult.injections) and
    rho_kin,E/rho_R,E < PARKING_EPSILON, strictly. The ratio is the candidate's, rho_kin,E from
    before its drain. PARKING_EPSILON is read here, at call time (orchestrator note 06, ruling 1).
    """
    if candidate_index == 0:
        return False
    return ratio < PARKING_EPSILON


def tracker_adiabatic(m_eff_over_H: float) -> bool:
    """
    The zero-lag tracker's adiabaticity (prompt 06 §1.2, §1.3; README §8.1 R14, R16): True iff
    m_eff/H_E >= PARKING_ADIABATICITY_THRESHOLD. The cut parks only where it is True, and a parked
    step end where it is False releases the field (prompt 07; it was a failure of the history in
    prompt 06, orchestrator note 06, ruling 1). A NaN is False. The threshold is read at call time.
    """
    return m_eff_over_H >= PARKING_ADIABATICITY_THRESHOLD


def parking_guard(pi_in: float, PlanckMass: float) -> bool:
    """
    P21's guard (README §8.2): True iff |pi_in| < PARKING_PI_GUARD_MP M_P, strictly, at a reflection
    where the cut did not fire; the loop then fails the history. The threshold is read at call time.
    """
    return abs(pi_in) < PARKING_PI_GUARD_MP * PlanckMass


def _log_trace(
    log_rhorad_Einstein: float, Sigma: float, fm: float, context: str
) -> float:
    """
    ln T, T = rho_R,E (Sigma + f_m), the Einstein-frame trace that ODERHS couples to and that the
    model's minimum_on_trace reads (inelastic-reflection prompt 04). Sigma + f_m <= 0, or a ln T that
    is not finite, is a ComputationFailureError (orchestrator note 06, ruling 2).
    """
    total: float = Sigma + fm
    if not (total > 0.0):
        raise ComputationFailureError(
            f"{context}: the trace is not positive: Sigma + f_m = {total!r} "
            f"(Sigma = {Sigma!r}, f_m = {fm!r})"
        )
    log_T: float = log_rhorad_Einstein + log(total)
    if not isfinite(log_T):
        raise ComputationFailureError(f"{context}: ln T = {log_T!r} is not finite")
    return log_T


def log_trace(cosmology: BaseCosmology, reduced_state) -> Tuple[float, float, float]:
    """
    (ln T, Sigma, f_m) at a reduced state (ln rho_R,E, ln f_m, ln T_J) (prompt 06 §1.3), with
    Sigma = 1 - 3w(T_J) and f_m formed exactly as ODEPolicy forms them, so that at one state they are
    the policy's floats.
    """
    log_rhorad_Einstein, log_fm, log_T_Jordan = (float(x) for x in reduced_state)
    try:
        T_Jordan: float = exp(log_T_Jordan)
        fm: float = exp(log_fm)
    except OverflowError as e:
        raise ComputationFailureError(
            f"log_trace: overflow at ln f_m = {log_fm!r}, ln T_J = {log_T_Jordan!r}"
        ) from e
    Sigma: float = 1.0 - 3.0 * float(cosmology.w(T_Jordan))
    return _log_trace(log_rhorad_Einstein, Sigma, fm, "log_trace"), Sigma, fm


# the tracker's velocity at one state (prompt 06 §1.4): pi_min = s a/(1 - s b), with d ln T/dN = a + b pi
# the right-hand side's d ln T/dN at that state and s = d phi_min/d ln T (the model's
# d_minimum_d_log_trace); log_T is ln T there. The parts are returned so that a test can form
# a + b pi_min against the right-hand side
TrackerVelocity = namedtuple("TrackerVelocity", ["pi_min", "a", "b", "s", "log_T"])


def tracker_velocity_terms(
    model: RealizedModel, cosmology: BaseCosmology, phi_min: float, reduced_state
) -> TrackerVelocity:
    """
    pi_min = d phi_min/dN of the zero-lag tracker at (phi_min, the reduced state
    (ln rho_R,E, ln f_m, ln T_J)), solved as linear algebra, never by differencing phi_min
    (inelastic-reflection prompt 06 §1.4; log 06 §1.2 derives it). With g = 1 + (ln A)' pi,
    D = 1 + (d ln g_s/d ln T_J)/3 and w' = dw/d ln T_J, ODERHS gives

        d ln rho_R,E/dN = Sigma g - 4,   d ln f_m/dN = (1 - Sigma) g,   d ln T_J/dN = -g/D,

    and dSigma/dN = -3 w' d ln T_J/dN = 3 w' g/D. With ln T = ln rho_R,E + ln(Sigma + f_m),

        d ln T/dN = -4 + c g = a + b pi,  c = Sigma + (3 w'/D + f_m (1 - Sigma))/(Sigma + f_m),
        a = c - 4,  b = c (ln A)'(phi_min),

    and pi = s (a + b pi), s = d phi_min/d ln T from the model's d_minimum_d_log_trace (no (ln A)'^2
    term in its denominator), so pi_min = s a/(1 - s b). A non-finite pi_min, or 1 - s b = 0, is a
    ComputationFailureError; so is any the model raises.
    """
    log_T, Sigma, fm = log_trace(cosmology, reduced_state)
    T_Jordan: float = exp(float(reduced_state[2]))

    D: float = (
        1.0
        + float(cosmology.dG_s_dlogT(T_Jordan)) / float(cosmology.G_s(T_Jordan)) / 3.0
    )
    w_prime: float = float(cosmology.dw_dlogT(T_Jordan))
    c: float = Sigma + (3.0 * w_prime / D + fm * (1.0 - Sigma)) / (Sigma + fm)
    a: float = c - 4.0
    b: float = c * float(model.d_logOmega_dphi(phi_min))

    s: float = float(model.d_minimum_d_log_trace(phi_min, log_T))
    denominator: float = 1.0 - s * b
    if denominator == 0.0 or not isfinite(denominator):
        raise ComputationFailureError(
            f"tracker_velocity: 1 - s b = {denominator!r} at phi_min = {phi_min!r}, "
            f"ln T = {log_T!r} (s = {s!r}, b = {b!r})"
        )
    pi_min: float = s * a / denominator
    if not isfinite(pi_min):
        raise ComputationFailureError(
            f"tracker_velocity: pi_min = {pi_min!r} is not finite at phi_min = {phi_min!r}, "
            f"ln T = {log_T!r} (s = {s!r}, a = {a!r}, b = {b!r})"
        )
    return TrackerVelocity(pi_min=pi_min, a=a, b=b, s=s, log_T=log_T)


def tracker_velocity(
    model: RealizedModel, cosmology: BaseCosmology, phi_min: float, reduced_state
) -> float:
    """pi_min = d phi_min/dN at (phi_min, the reduced state); tracker_velocity_terms's."""
    return tracker_velocity_terms(model, cosmology, phi_min, reduced_state).pi_min


def parked_tracker(
    model: RealizedModel, cosmology: BaseCosmology, phi_start: float, reduced_state
) -> Tuple[float, float]:
    """
    (phi_min, pi_min) of the zero-lag tracker at a reduced state (prompt 06 §1.3): phi_min the
    model's minimum_on_trace on the state's trace, searched from phi_start, and pi_min
    tracker_velocity's there. The parked phase passes a phi_start that is fixed per step (orchestrator
    note 06, ruling 3), so that the value depends on the state alone.
    """
    log_T, _, _ = log_trace(cosmology, reduced_state)
    phi_min: float = model.minimum_on_trace(phi_start, log_T)
    return phi_min, tracker_velocity(model, cosmology, phi_min, reduced_state)


def tracker_m_eff_over_H(
    policy: "ODEPolicy", hubble: "HubblePolicy", N: float, y: np.ndarray
) -> Tuple[float, float]:
    """
    (m_eff/H_E, H_E) at a full state (N, y) (prompt 06 §1.2 step 3, §1.3): m_eff from
    log_m_eff_squared at (phi, ln rho_R,E, ln f_m, Sigma), H_E from HubblePolicy with the ledger as it
    stands, from one policy call. Never through the right-hand side.
    """
    state = StateVector._make(y)
    data: ODEPolicyData = policy(N, state)
    H_Einstein: float = hubble(data, state).H_Einstein
    log_m2: float = log_m_eff_squared(
        policy.model,
        state.phi_Einstein,
        state.log_rhorad_Einstein,
        state.log_fm,
        data.Sigma,
    )
    return exp(0.5 * log_m2 - log(H_Einstein)), H_Einstein


# the tracker at a cut candidate (prompt 06 §1.2 step 3; orchestrator note 06, ruling 2)
#   phi_root:         minimum_on_trace on the candidate's trace, searched from the field; the parked
#                     phase's first start
#   phi_min, pi_min:  the parked state, parked_tracker from phi_root: the parked phase's own value at
#                     the switch's N, so that the dense output there returns it bitwise
#   log_T:            ln T at the candidate, from the policy's Sigma and f_m
#   m_eff_over_H:     m_eff/H_E at (phi_min, pi_min), the value the cut is decided on
#   H_Einstein:       H_E there, with the ledger after the candidate's drain and before the leftover
CutTracker = namedtuple(
    "CutTracker",
    ["phi_root", "phi_min", "pi_min", "log_T", "m_eff_over_H", "H_Einstein"],
)


def cut_tracker(
    policy: "ODEPolicy", hubble: "HubblePolicy", N: float, y: np.ndarray
) -> CutTracker:
    """
    The zero-lag tracker at a cut candidate's state after its drain (prompt 06 §1.2 step 3;
    orchestrator note 06, ruling 2): Sigma and f_m from one policy call, ln T = ln rho_R,E +
    ln(Sigma + f_m); phi_min by the model's minimum_on_trace from the field; pi_min (§1.4); and
    m_eff/H_E there, with the ledger as it stands. pi in y is not used. A ComputationFailureError from
    the policy or the model propagates.
    """
    state = StateVector._make(y)
    model: RealizedModel = policy.model
    data: ODEPolicyData = policy(N, state)
    log_T: float = _log_trace(
        state.log_rhorad_Einstein, data.Sigma, data.fm, "cut_tracker"
    )
    phi_root: float = model.minimum_on_trace(float(state.phi_Einstein), log_T)
    reduced = (state.log_rhorad_Einstein, state.log_fm, state.log_T_Jordan)
    phi_min, pi_min = parked_tracker(model, policy.cosmology, phi_root, reduced)
    y_parked = np.array([phi_min, pi_min, *reduced], dtype=float)
    m_eff_over_H, H_Einstein = tracker_m_eff_over_H(policy, hubble, N, y_parked)
    return CutTracker(
        phi_root=phi_root,
        phi_min=phi_min,
        pi_min=pi_min,
        log_T=log_T,
        m_eff_over_H=m_eff_over_H,
        H_Einstein=H_Einstein,
    )


def parking_leftover(
    H_Einstein: float, pi_Einstein: float, N: float, log_rhorad_Einstein: float
) -> Tuple[float, float]:
    """
    The leftover kinetic energy at the switch (prompt 06 §1.2 step 4; README §8.2 P19): the drained
    state's rho_kin,E = H_E^2 pi^2/2, with H_E and pi the candidate's H_Einstein_after and pi_after.
    Returns (Delta K, Delta rho/rho_R,E) = (rho_kin,E e^{4N}, rho_kin,E/rho_R,E), formed in logs as the
    drain forms them, so that nothing overflows (orchestrator note 06, ruling 2).
    """
    log_rho_kin: float = 2.0 * log(H_Einstein) + 2.0 * log(abs(pi_Einstein)) - LOG_2
    return exp(log_rho_kin + 4.0 * N), exp(log_rho_kin - log_rhorad_Einstein)


class ParkedRHS:
    """
    The parked phase's right-hand side (prompt 06 §1.3; orchestrator note 06, ruling 4): the
    derivatives of the reduced state (ln rho_R,E, ln f_m, ln T_J), ODERHS's components 2-4, evaluated
    through `fun` (the loop's, so that nfev and the supervisor count them) at the full state
    (phi_min, pi_min, the reduced state), with phi_min and pi_min solved by parked_tracker from
    phi_anchor. The loop sets phi_anchor to the step's left-node phi_min before each step (ruling 3),
    so that every evaluation of one step searches from the same float.
    """

    def __init__(self, fun, model: RealizedModel, cosmology, phi_anchor: float):
        self.fun = fun
        self.model: RealizedModel = model
        self.cosmology = cosmology
        self.phi_anchor: float = float(phi_anchor)

    def full_state(self, N: float, reduced_state) -> np.ndarray:
        """The full state (phi_min, pi_min, ln rho_R,E, ln f_m, ln T_J) of the tracker at (N, reduced)."""
        phi_min, pi_min = parked_tracker(
            self.model, self.cosmology, self.phi_anchor, reduced_state
        )
        return np.array(
            [phi_min, pi_min, reduced_state[0], reduced_state[1], reduced_state[2]],
            dtype=float,
        )

    def __call__(self, N, reduced_state) -> np.ndarray:
        y = self.full_state(float(N), reduced_state)
        derivatives = self.fun(N, y)
        return np.array([derivatives[2], derivatives[3], derivatives[4]], dtype=float)


class ParkedSegment(DenseOutput):
    """
    One accepted step of the parked phase, as a segment of the history's OdeSolution (prompt 06
    §1.3): for each N it reads the step's interpolant of the reduced state by a scalar call and
    returns the full state (phi_min, pi_min, ln rho_R,E, ln f_m, ln T_J), phi_min and pi_min solved
    by parked_tracker from phi_anchor, the phi_min at the step's left node, fixed for the segment.
    Shape (5,) for a scalar N and (5, n) for an array, which is a loop of the scalar evaluations, so
    that the value at N depends on N alone: not on the order of calls, and not on whether N came in
    an array (orchestrator note 06, ruling 3; SciPy's interpolants do not give that by themselves).
    It holds no state that a call changes.
    """

    def __init__(
        self, reduced_interpolant, phi_anchor: float, model: RealizedModel, cosmology
    ):
        super().__init__(reduced_interpolant.t_old, reduced_interpolant.t)
        self.reduced_interpolant = reduced_interpolant
        self.phi_anchor: float = float(phi_anchor)
        self.model: RealizedModel = model
        self.cosmology = cosmology

    def state_at(self, N: float) -> np.ndarray:
        reduced = self.reduced_interpolant(N)
        phi_min, pi_min = parked_tracker(
            self.model, self.cosmology, self.phi_anchor, reduced
        )
        return np.array(
            [phi_min, pi_min, reduced[0], reduced[1], reduced[2]], dtype=float
        )

    def _call_impl(self, t):
        if t.ndim == 0:
            return self.state_at(float(t))
        values = np.empty((EXPECTED_SOL_LENGTH, t.shape[0]))
        for j in range(t.shape[0]):
            values[:, j] = self.state_at(float(t[j]))
        return values


# ---- the drain law (one-loop-drain prompt 01; README §8.2, §8.8 P18, P22-P24) ----
#
# Module-level, so that they can be tested on synthetic values without an integration; the loop calls
# each by its module name at call time, as it calls the gate's


# check_drain_law, the law's inputs (one-loop-drain prompt 01 §1.1; README §8.8 P22), is defined in
# CosmologyConcepts/drain_model.py and imported above (one-loop-drain prompt 03; orchestrator note 03, ruling 1),
# so that alpha_value checks its pair (value, drain_model) by the same statement of the rules; its messages are
# unchanged


def check_drain_law_model(drain_law: str, model, context: str) -> None:
    """
    The model under the drain law (one-loop-drain prompt 02c; the user's ruling of 2026-11-05, "I suggest we move
    `L` into the model class, with no default"), checked at entry by integrate_scalar_history, integrate_history
    and tune_history, beside check_drain_law and before any integration (and, in tune_history, before the seed).
    Under DRAIN_LAW_ONE_LOOP a model, realized or not, whose class does not override AbstractModel._drain_L
    (drain_L_implemented) is a ValueError naming its class and the hook. Under the fixed law nothing is checked.
    """
    if drain_law != DRAIN_LAW_ONE_LOOP:
        return
    if not model.drain_L_implemented:
        cls = type(model.model if isinstance(model, RealizedModel) else model)
        raise ValueError(
            f"{context}: the one-loop drain law needs the model's wall variable L, "
            f"_drain_L(normalization, log_E), which {cls.__name__} does not implement"
        )


def drain_regime(model: RealizedModel, assessment):
    """
    The regime of one candidate under the one-loop law (one-loop-drain prompt 01 §1.2; README §8.2, P18,
    P24), from the assessment it is judged on, (phi, ln E, ClassicalTurningPoint) or None. Returns
    (regime, ell, estimate), estimate the PeakEstimate that peak_estimate computes for a candidate with
    this assessment (None if unassessed), from the same expressions (_assessed_peak), so that
    history_margins can reuse it and one_loop_turnaround runs once per candidate:
      - unassessed:  no assessment; (REGIME_UNASSESSED, None, None);
      - classical:   ln R <= 0;
      - one_loop:    ln R > 0, one_loop_turnaround(phi, ln E) returns a solution with ell > 0; ell is its;
      - gap:         ln R > 0, and it raises ComputationFailureError or returns None, or returns a
                     solution with ell <= 0 (P24, for which estimate is peak_estimate's ESTIMATE_ONE_LOOP:
                     orchestrator note 01, ruling 4). ell is None.
    Any other exception from the model propagates, as it does from peak_estimate.
    """
    if assessment is None:
        return REGIME_UNASSESSED, None, None
    phi_assessed, log_E_assessed, turning_point = assessment
    estimate, turnaround = _assessed_peak(
        model, turning_point, phi_assessed, log_E_assessed
    )
    if estimate.estimate == ESTIMATE_CLASSICAL:
        return REGIME_CLASSICAL, None, estimate
    if estimate.estimate == ESTIMATE_CLASSICAL_GAP:
        return REGIME_GAP, None, estimate
    if not turnaround.ell > 0.0:
        return REGIME_GAP, None, estimate
    return REGIME_ONE_LOOP, turnaround.ell, estimate


def drain_law_inputs(model: RealizedModel, assessment):
    """
    The law's inputs at one candidate (one-loop-drain prompt 02c §1.1; orchestrator note 02c ruling 1), from the
    assessment it is judged on, (phi, ln E, ClassicalTurningPoint) or None: (L, ln R), L the model's
    (model.drain_L(ln E); AbstractModel._drain_L, which the model defines, by the user's ruling of 2026-11-05)
    and ln R the turning point's log_R; (None, None) if unassessed. Computed once per candidate in the loop; the
    same two numbers go to drain_alpha_effective and to the DrainCandidate. An assessment always carries a
    turning point (drain_regime reads its log_R).
    """
    if assessment is None:
        return None, None
    _, log_E, turning_point = assessment
    return model.drain_L(log_E), turning_point.log_R


def drain_S_of_L(L: float) -> float:
    """
    S(L), the classical wall's production energy ratio (README §8.10, decision 3), from DRAIN_S_TABLE_*:
    linear in (ln L, ln S) by np.interp on the table's np.log, as the reference implementation's S_of
    computes it, so that at L <= 134 the two are bitwise equal; S(0.3) below L = 0.3 (np.interp holds the end
    value); S(134) (134/L)^2 above L = 134, where the reference instead holds S(134) (README §8.10 rules the
    continuation).
    """
    if L > DRAIN_S_TABLE_L[-1]:
        return DRAIN_S_TABLE_S[-1] * (DRAIN_S_TABLE_L[-1] / L) ** 2
    return float(np.exp(np.interp(log(L), _DRAIN_S_LOG_L, _DRAIN_S_LOG_S)))


def drain_fraction(
    regime: str, ell: Optional[float], L: Optional[float], log_R: Optional[float]
) -> float:
    """
    f, the fraction of the kinetic energy one bounce gives to quanta under the measured law (one-loop-drain
    prompt 02c §1.1; README §8.10), by regime, with the constants above, term for term as the reference
    implementation (fraction_adopted, prompts/one-loop-drain/source/crossover/measured_law_map.py) writes it,
    so that the values are bitwise equal to it but in two places:
      - one_loop, ell >= DRAIN_ELL_PLATEAU: d = ell + B0 - C ln L; min(F_CAP, 1 - exp(-A/d)) if d > 0, else F_CAP;
      - one_loop, ell < DRAIN_ELL_PLATEAU: F_PLATEAU, reading neither L nor ln R;
      - gap: 0.0 if ln R <= R0, or if s = 1 + Q ln L <= 0 (the user's ruling of 2026-11-05: "I think probably
        we set `f = 0` there"; the reference is negative there); else 1 - exp(-G (ln R - R0)^P s);
      - classical: exp(ln R) drain_S_of_L(L) (above L = 134 the reference holds S(134));
      - unassessed: 0.0.
    An unknown regime is a ValueError.
    """
    if regime == REGIME_ONE_LOOP:
        if ell < DRAIN_ELL_PLATEAU:
            return DRAIN_F_PLATEAU
        d: float = ell + DRAIN_ONE_LOOP_B0 - DRAIN_ONE_LOOP_C * log(L)
        return (
            min(DRAIN_F_CAP, 1.0 - exp(-DRAIN_ONE_LOOP_A / d))
            if d > 0.0
            else DRAIN_F_CAP
        )
    if regime == REGIME_GAP:
        if log_R <= DRAIN_GAP_R0:
            return 0.0
        s: float = 1.0 + DRAIN_GAP_Q * log(L)
        if s <= 0.0:
            return 0.0
        return 1.0 - exp(-DRAIN_GAP_G * (log_R - DRAIN_GAP_R0) ** DRAIN_GAP_P * s)
    if regime == REGIME_CLASSICAL:
        return exp(log_R) * drain_S_of_L(L)
    if regime == REGIME_UNASSESSED:
        return 0.0
    raise ValueError(
        f"drain_fraction: the regime must be one of {DRAIN_REGIMES}, not {regime!r}"
    )


def drain_alpha_effective(
    regime: str,
    ell: Optional[float],
    L: Optional[float],
    log_R: Optional[float],
    A: float,
    w_gate: float,
) -> float:
    """
    The fraction drained at one candidate under the one-loop law (one-loop-drain prompt 02c §1.1; README §8.10;
    orchestrator note 02c ruling 1): min(DRAIN_ALPHA_CAP, A * drain_fraction(regime, ell, L, ln R)) * w_gate,
    evaluated as the reference implementation's patch evaluates it. L and ln R are drain_law_inputs', computed
    once per candidate by the caller. Unassessed: 0.0 exactly, without reading L or ln R, and the caller takes
    the alpha_eff = 0 branch it takes at the fixed law; so it does wherever f = 0 (the gap below its
    threshold, or where 1 + Q ln L <= 0) or w_gate = 0. An unknown regime is a ValueError.
    """
    if regime == REGIME_UNASSESSED:
        return 0.0
    if regime not in DRAIN_REGIMES:
        raise ValueError(
            f"drain_alpha_effective: the regime must be one of {DRAIN_REGIMES}, not {regime!r}"
        )
    return min(DRAIN_ALPHA_CAP, A * drain_fraction(regime, ell, L, log_R)) * w_gate


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
    alpha: float = 0.0,
    drain_law: str = DRAIN_LAW_FIXED,
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

    Turning points and the drain (inelastic-reflection prompt 02). After every accepted step that
    is appended to the dense output, pi is read on the step's interpolant at its two ends (the
    last step's end is N_final) and at TURNING_POINT_INTERIOR_SAMPLES interior points, and each
    sign change is a TurningPoint, located by brentq (xtol = 1e-15): "wall" for pi from - to +,
    "plateau" for + to -. A single change running end to end is located exactly as first_bounce
    locates one; two or more on one step are each refined on their own sub-interval
    (orchestrator note 02, ruling 15). Since electroweak-eos prompt 01a (its README §8, U8 and U9),
    unless the samples show exactly one change running end to end, an interior pair is kept only if
    the solver resolves it: walking the changes from the step's start, two consecutive changes whose
    excursion (the largest |pi| over the samples between their crossings) is not greater than
    tau = atol + rtol max(|pi_low|, |pi_high|), the tolerance to which Radau holds pi = y[1] on the
    step, are both dropped (resolved_sign_changes). A dropped pair is as if it were not there: no
    TurningPoint, no drain armed or dropped, no boundary of the half-oscillation; it is counted in
    IntegrationResult.unresolved_turning_point_pairs. The case that led to it: on the w table of
    electroweak-eos prompt 01, with Sigma = 0 above 386.7 GeV, a history started at rest has
    pi < 0 at both nodes of the step where Sigma switches on, and the interpolant swings above zero
    between them by less than tau; the field does not turn there.
    This runs at every alpha and does not touch the solver.
    At alpha > 0:
      - a wall-side turning point arms a drain. The drain point is the end of the first accepted
        step, counting from the turning point's own, at which the accepted state has pi > 0 and
        pi' = solver.f[1] <= 0: the first step end past the maximum of |pi| (README §6.2 P3).
        There, with alpha_eff = alpha, rho_kin,E = H_E^2 pi^2/2 (H_E from HubblePolicy with the
        ledger before the drain), Delta K = alpha_eff rho_kin,E e^{4N} is added to the ledger,
        pi -> sqrt(1 - alpha_eff) pi, and Radau is restarted from the drained state. The ledger is
        added to before the new Radau instance is built, so its first RHS evaluation sees the new
        K. A plateau turnaround, a floor reflection or the history's end before the drain point
        drops the armed drain. The step that ends the history never drains;
      - a floor-rule reflection drains in its own restart: pi_out = -sqrt(1 - alpha_eff) pi_in,
        with Delta K = alpha_eff H_E^2 pi_in^2 e^{4N}/2 from the state and ledger before it.
    Each drain is an Injection. At alpha = 0 nothing is drained, the ledger is not touched, no
    restart is made and every value is the old code's. The dense output is right-continuous at its
    nodes (OdeSolution with alt_segment=True), so at a drain node it returns the drained state,
    which the ledger's rule N_j <= N pairs with the new K.

    The gate (inelastic-reflection prompt 03), at alpha > 0 only. Every drain point and every floor
    reflection is a candidate, recorded as an Injection whatever its alpha_eff, and
    alpha_eff = alpha * w_gate replaces alpha above:
      - on every appended step, ln m_eff^2 (log_m_eff_squared) and H_E are computed at the ten
        samples of pi, and the non-adiabaticity A = H_E |d ln m_eff^2/dN|/(2 m_eff)
        (step_nonadiabaticity) is folded into a running maximum over the half-oscillation in
        progress. A turning point or a floor reflection ends one, at its N, and its maximum is
        that turning point's A_k (0.0 if it holds no sample); each TurningPoint records it with
        m_eff/H_E at the turning point;
      - w_gate = gate_weight(A_k), with A_k the wall-side turning point's, for a drain point. Since
        prompt 05 a reflection is unresolved and drains with w_gate = 1.0 (below); the infall's A_k
        is still recorded on it. Plateau turnarounds never drain;
      - a candidate with alpha_eff == 0.0 is recorded with no ledger entry, no change to pi and
        no restart (a floor reflection's elastic restart stays).
    None of this runs at alpha = 0: no mass, H_E or A is computed and no candidate is recorded,
    and the TurningPoints' two new fields are None. dropped_drains counts the armed drains that
    never reached a drain point, at every alpha.

    Unresolved turnarounds (inelastic-reflection prompt 05; README §8.1 R8, R10, §8.2 P20, P22), at
    alpha > 0 only, and only for a model with a wall (reflects_at_origin and a log_V_floor):
      - at the start of every step with pi < 0, before the floor rule, the wall ahead is assessed:
        assess_infall(model, phi, ln rho_kin,E), with rho_kin,E = H_E^2 pi^2/2 from policy and
        hubble (never fun), gives ln E = ln(rho_kin,E + V - V_floor) and the classical turning point.
        A None turning point makes no decision at that step, and the infall's latest assessment
        stands. A ComputationFailureError from the model is the history's failure;
      - unresolved_turnaround(phi, turning point, M_P) decides: phi < K phi_t, and phi_t below the
        threshold or ln R > 0. If it fires the field is reflected there, through the floor branch
        (the guards, a Reflection of kind "unresolved", the drop of an armed drain, the close of the
        half-oscillation, the candidate, the restart from -sqrt(1 - alpha_eff) pi_in); otherwise the
        floor rule is tested as before, and its reflection is of kind "floor";
      - every reflection, of either kind, is an unresolved candidate: it drains with w_gate = 1.0
        exactly, alpha_eff = alpha, and gate_weight is not called for it; its A_k is recorded. A
        drain point after a wall-side turning point keeps the gate;
      - each candidate records whether it was resolved and the assessment it was judged on: a
        reflection its own step start's (or the infall's latest, if that was None), a drain point
        the last of the infall that ended at its wall-side turning point. A plateau turnaround or a
        reflection starts a new infall.
    At alpha = 0 no assessment is made and the floor rule is the old code.

    The warnings (README §8.1 R6, §8.2 P23), returned in IntegrationResult.warnings, one line each,
    at most one per kind:
      - at alpha > 0, at the first candidate: WARNING_NEARLY_ELASTIC with R if its assessment has
        ln R < 0, WARNING_REGIME_UNASSESSED if it has none;
      - at alpha = 0, at the first of: a wall-side TurningPoint with phi below the threshold (there
        phi is phi_t, and nothing is computed), or the first floor reflection, at which the model's
        classical turning point is computed once, from the E above, and found below the threshold.
        A ComputationFailureError from that one call gives WARNING_WALL_UNASSESSED instead and the
        history goes on. It changes no value of the history.
    A ComputationFailureError that the loop raises carries the warnings collected before it as its
    attribute `warnings` (orchestrator note 05, ruling 2).

    Parking (inelastic-reflection prompt 06; README §8.1 R14, R15, R17, §8.2 P19, P21), at alpha > 0
    on a model with a wall only, at every M. After each candidate is recorded and drained:
      - the ratio test (parking_ratio_test): not the history's first candidate, and
        rho_kin,E/rho_R,E < PARKING_EPSILON, rho_kin,E the candidate's log_rho_kin;
      - if it passes, the zero-lag tracker at the drained state (cut_tracker): phi_min on the trace
        T = rho_R,E (Sigma + f_m), pi_min = d phi_min/dN (tracker_velocity) and m_eff/H_E there, and
        the decision tracker_adiabatic (m_eff/H_E >= PARKING_ADIABATICITY_THRESHOLD);
      - if both hold, the switch: the leftover kinetic energy of the drained state goes into the
        ledger at the same N (parking_leftover; P19), and the history continues in the parked phase
        from (phi_min, pi_min) and the drained state's ln rho_R,E, ln f_m and ln T_J, recorded in a
        Parking. The parked phase integrates (ln rho_R,E, ln f_m, ln T_J) by DOP853 on ParkedRHS, the
        right-hand side's components 2-4 at the tracker, to the history's end, and appends one
        ParkedSegment per accepted step to the dense output, each returning the full five-component
        state. Nothing else runs there: no turning points, candidates or assessments;
      - at a reflection where the cut did not fire, |pi_in| < PARKING_PI_GUARD_MP M_P is a failure of
        the history (parking_guard; P21).
    At alpha = 0 none of this runs (P22).

    The release (inelastic-reflection prompt 07; README §8.1 R16, §8.2 P24). A parked step end (or
    N_final on the step that ends the history) at which the tracker's m_eff/H_E is below
    PARKING_ADIABATICITY_THRESHOLD releases the field: at N_rel, the root of m_eff/H_E = the threshold
    on that step's ParkedSegment (brentq, RELEASE_ROOT_XTOL, RELEASE_ROOT_RTOL), which becomes the
    step's node in ts. The released state is the segment's at N_rel; nothing is drained or added to
    the ledger. The parked phase returns it to the loop, which starts a new Radau instance there, as
    after a drain, with no drain armed, no infall assessment, and the gate's half-oscillation begun
    at N_rel with no sample. The history then goes on to its end in the oscillating phase, draining
    at its candidates, and never parks again: after the release no candidate makes the ratio test or
    calls the tracker (P24). The guard still runs at every reflection. The Release record is
    IntegrationResult.release.

    The restart (reflection-restart prompt 03; see the comment above _restart_first_step). Every Radau
    instance begun after a reflection, a drain or a release with pi > 0 is given F3's first step,
    _restart_first_step, and its first accepted step is guarded: if _outward_step_flagged flags it, the
    step is discarded before it is used for anything (no change to the nodes, the turning points, the
    gate's samples, the ledger, a drain's arming, the step count or the budget; its RHS evaluations
    stay counted) and retaken from the same (N, state) by a new Radau with first_step = h/2, itself
    guarded; half an h below h_floor is a failure of the history. No other step is guarded: not the
    history's start, not a later step of an instance, not a restart with pi <= 0, not the parked phase.
    The retakes are IntegrationResult.restart_retakes.

    The drain law (one-loop-drain prompt 01 §1.1-§1.4; README §8.2, §8.8 P18, P22-P24). drain_law is
    DRAIN_LAW_FIXED (the default: everything above, unchanged) or DRAIN_LAW_ONE_LOOP, under which alpha
    carries A > 0 and every alpha > 0 path above runs unchanged (assessments, candidates, the gate,
    parking, the release) but for alpha_eff: at each candidate, before alpha_eff, drain_regime classifies
    it from the assessment it is given, once, and drain_alpha_effective gives alpha_eff from the regime,
    ell, A and the gate's w_gate (gate_weight at a resolved candidate, 1.0 at an unresolved one, as
    above). Each candidate's DrainCandidate and PeakEstimate are kept, in IntegrationResult.drain and
    IntegrationResult.peak_estimates. Under either law, each assessment whose classical turning point
    is None with _turning_point_unresolvable holding is counted (DrainRecord.unresolvable_assessments);
    nothing else changes at it, since a None turning point already makes no decision. A
    ComputationFailureError that the loop raises carries the DrainRecord as far as the loop reached as
    its attribute `drain`, beside `warnings`.

    The declined unresolved reflection (one-loop-drain prompt 01 §1.7; the user's ruling (b), 2026-11-03),
    under either law: at a step start where unresolved_turnaround fires but unresolved_reflection_refused
    says G2 would refuse the reflection there (W > pi^2/2, G2's own expression and comparison), no
    unresolved decision is made at that step start, and the floor rule is tested as usual (its reflection
    is guarded by G2 as before). The step starts declined are DrainRecord.unresolved_declined. Every
    state where this acts raised in _reflection_guards before the ruling, so no history or tuning that
    succeeded before it moves.

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
    :param alpha: the fraction of the condensate's kinetic energy drained at each drain point and
                  floor reflection (inelastic-reflection prompt 02), 0 <= alpha < 1, before the
                  gate w_gate of prompt 03. At alpha > 0 policy.dark_radiation
                  receives the drains, and must be the ledger the RHS reads. Under the one-loop
                  drain law it is A instead, finite and > 0 (one-loop-drain prompt 01)
    :param drain_law: DRAIN_LAW_FIXED (the default) or DRAIN_LAW_ONE_LOOP (check_drain_law)
    :return: IntegrationResult
    """
    # the guard (reflection-restart prompt 03 §1.2): guard_first_step is True from a restart with pi > 0
    # (or a retake) until that instance's first accepted step; restart_retakes counts the retakes
    guard_first_step: bool = False
    restart_retakes: int = 0

    check_drain_law(drain_law, alpha, "integrate_scalar_history")

    # the drain law (one-loop-drain prompt 01): under the one-loop law the candidates' records and
    # PeakEstimates, and the drain model's name (prompt 02c); under either law the count of the
    # assessments the wall-search fix answers
    one_loop: bool = drain_law == DRAIN_LAW_ONE_LOOP
    drain_model: str = DRAIN_MODEL_ONE_LOOP if one_loop else DRAIN_MODEL_FIXED
    drain_candidates: List[DrainCandidate] = []
    peak_estimates: List[Optional[PeakEstimate]] = []
    unresolvable_assessments: int = 0
    unresolved_declined: int = 0

    def drain_record() -> DrainRecord:
        return DrainRecord(
            law=drain_law,
            A=alpha if one_loop else None,
            model=drain_model,
            unresolvable_assessments=unresolvable_assessments,
            candidates=tuple(drain_candidates) if one_loop else None,
            unresolved_declined=unresolved_declined,
        )

    if policy is None:
        policy = RHS.policy

    # the model must define the law's L under the one-loop law (prompt 02c), before any step
    check_drain_law_model(drain_law, policy.model, "integrate_scalar_history")

    # the ledger the drains go to (orchestrator note 02, ruling 5): at alpha > 0 it must exist, and
    # it must be the one the RHS reads, or a drain would change pi without changing the expansion
    ledger: Optional[DarkRadiationLedger] = None
    if alpha > 0.0:
        ledger = policy.dark_radiation
        if ledger is None:
            raise ValueError(
                "integrate_scalar_history: alpha > 0 needs a policy with a DarkRadiationLedger"
            )
        RHS_policy = getattr(RHS, "policy", None)
        if (
            RHS_policy is not None
            and getattr(RHS_policy, "dark_radiation", None) is not ledger
        ):
            raise ValueError(
                "integrate_scalar_history: the policy's DarkRadiationLedger is not the one the "
                "RHS reads"
            )

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

    def new_solver(N, y, restart: bool = False, retake_step: Optional[float] = None):
        """
        A new Radau instance at (N, y). At the history's start (restart False) SciPy chooses the first
        step and nothing is guarded. At a restart (after a reflection, a drain or a release) with
        pi > 0, F3's first step (at most the distance to the bound) and the guard on the instance's
        first accepted step; with pi <= 0, SciPy's choice and no guard. A retake (retake_step, half the discarded step) starts there and
        is guarded (reflection-restart prompt 03; orchestrator note 03, ruling 5).
        """
        nonlocal guard_first_step
        first_step: Optional[float] = None
        guard_first_step = False
        if retake_step is not None:
            first_step = retake_step
            guard_first_step = True
        elif restart and y[1] > 0.0:
            first_step = _restart_first_step(y, global_max_step, h_floor, f_cap)
            # SciPy refuses a first step past the solver's bound: at a restart closer to N_bound than
            # F3's step (a window's N_stop) the step is the distance to the bound. Where F3's step fits,
            # this changes nothing (reflection-restart prompt 03; ruled by the user, "Please allow both")
            if first_step is not None:
                first_step = min(first_step, N_bound - N)
            guard_first_step = True
        return Radau(
            fun,
            N,
            y,
            N_bound,
            max_step=global_max_step,
            rtol=params.rtol,
            atol=params.atol,
            first_step=first_step,
        )

    solver = new_solver(float(N_start), np.array(initial_state, dtype=float))

    ts = [solver.t]
    interpolants = []
    reflections = []
    turning_points = []
    injections = []
    max_ratio: Optional[float] = None
    accepted_steps = 0
    steps_rejected_by_exception = 0

    # the wall-side TurningPoint whose drain is armed, or None (inelastic-reflection prompt 02)
    armed: Optional[TurningPoint] = None

    hubble = HubblePolicy(policy.model, policy.units)
    model: RealizedModel = policy.model

    # the gate (inelastic-reflection prompt 03), at alpha > 0 only (orchestrator note 03, ruling
    # 1); neither of these is touched at alpha = 0
    #   running_max: the largest non-adiabaticity over the samples of the half-oscillation in
    #                progress, or None while it holds none (then A_k = 0.0; ruling 6)
    #   boundary_N:  the N at which that half-oscillation began: N_start, a turning point or a floor
    #                reflection. It holds the samples with boundary_N < N <= N_k (ruling 3)
    running_max: Optional[float] = None
    boundary_N: float = float(N_start)

    # the drains armed by a wall-side turning point and dropped before their drain point
    # (orchestrator note 03, ruling 7). Counted at every alpha, alpha = 0 included
    dropped_drains = 0

    # the interior pairs of sign changes of pi dropped as unresolved by the solver (electroweak-eos
    # prompt 01a). Counted at every alpha, alpha = 0 included
    unresolved_pairs: int = 0

    # the unresolved turnaround (inelastic-reflection prompt 05), at alpha > 0 on a model with a wall
    # only; at alpha = 0, or without a wall, nothing is assessed and the floor rule is the old code
    #   infall_assessment:  the infall's latest assessment (phi, ln E, ClassicalTurningPoint), or None;
    #                       a turning point or a reflection starts a new infall
    #   armed_assessment:   the infall's assessment, captured by the wall-side TurningPoint that armed
    #                       the drain in `armed`
    Mp: float = policy.units.PlanckMass
    assess_wall: bool = (
        alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None
    )
    infall_assessment: Optional[Tuple[float, float, ClassicalTurningPoint]] = None
    armed_assessment: Optional[Tuple[float, float, ClassicalTurningPoint]] = None

    # the warnings (inelastic-reflection prompt 05; README §8.1 R6, §8.2 P23), at most one per kind
    #   first_candidate_seen: dissipation mode; its warning is made at the history's first candidate
    #   natural_warned:       natural mode; a warning has been made, so neither test runs again
    #   natural_floor_called: natural mode; the one model call, at the first floor reflection, is made
    warnings: List[str] = []
    first_candidate_seen = False
    natural_warned = False
    natural_floor_called = False

    def H_Einstein_at(N_: float, y_: np.ndarray) -> float:
        state_ = StateVector._make(y_)
        return hubble(policy(N_, state_), state_).H_Einstein

    def mass_and_H_at(N_: float, y_: np.ndarray):
        """
        (ln m_eff^2, H_E) at (N_, y_), from one policy call: Sigma from its ODEPolicyData, H_E from
        HubblePolicy on the same data with the ledger as it stands (orchestrator note 03, rulings 2
        and 5). Never through fun or the RHS, so nfev and the supervisor's records are untouched.
        """
        state_ = StateVector._make(y_)
        data_ = policy(N_, state_)
        H_: float = hubble(data_, state_).H_Einstein
        log_m2_: float = log_m_eff_squared(
            model,
            state_.phi_Einstein,
            state_.log_rhorad_Einstein,
            state_.log_fm,
            data_.Sigma,
        )
        return log_m2_, H_

    def fold(N_samples: np.ndarray, A_samples: np.ndarray, upto: float) -> None:
        """Fold the samples with boundary_N < N <= upto into the running maximum (ruling 3)."""
        nonlocal running_max
        for N_j, A_j in zip(N_samples, A_samples):
            if boundary_N < N_j <= upto:
                A_j = float(A_j)
                running_max = A_j if running_max is None else max(running_max, A_j)

    def close_half_oscillation(N_end: float) -> float:
        """
        End the half-oscillation in progress at N_end (a turning point or a floor reflection) and
        return its A_k, 0.0 if it holds no sample (ruling 6); the next begins at N_end.
        """
        nonlocal running_max, boundary_N
        A_k: float = 0.0 if running_max is None else running_max
        running_max = None
        boundary_N = float(N_end)
        return A_k

    def alpha_effective(nonadiabaticity: float, resolved: bool):
        """
        The fraction drained at one candidate, alpha_eff = alpha * w_gate (inelastic-reflection
        prompt 03; orchestrator note 03, ruling 4 as amended by ruling 16): the gate on the
        half-oscillation's A_k at a resolved candidate, and w_gate = 1.0 exactly at an unresolved
        one, where gate_weight is not called (prompt 05; README §8.1 R10, §8.2 P20). Returns
        (alpha_eff, w_gate). At alpha = 0 it returns 0.0 before the weight is computed (ruling 1);
        the loop does not call it there.
        """
        if alpha == 0.0:
            return 0.0, None

        w_gate: float = gate_weight(nonadiabaticity) if resolved else 1.0
        alpha_eff: float = alpha * w_gate
        return alpha_eff, w_gate

    def first_candidate_warning(assessment) -> None:
        """
        Dissipation mode's warning (prompt 05 §1.5), at the history's first candidate, its first
        impact: nearly elastic if its assessment has ln R < 0, unassessed if it has none.
        """
        nonlocal first_candidate_seen
        if first_candidate_seen:
            return
        first_candidate_seen = True
        if assessment is None:
            warnings.append(
                f"{WARNING_REGIME_UNASSESSED}: the first impact has no assessment of the wall "
                f"(the model has no wall, or every assessment of the infall returned None)"
            )
            return
        phi_a, log_E_a, turning_point_a = assessment
        if turning_point_a.log_R < 0.0:
            warnings.append(
                f"{WARNING_NEARLY_ELASTIC}: the test-field ratio R = 10^"
                f"{turning_point_a.log_R / log(10.0):.3f} < 1 at the first impact "
                f"(phi_t = {turning_point_a.phi_t / Mp:.4g} M_P)"
            )

    def candidate(
        N_: float,
        y_: np.ndarray,
        nonadiabaticity: float,
        kind: str,
        N_turn: float,
        phi_turn: float,
        sign: float,
        resolved: bool,
        assessment,
    ) -> Optional[np.ndarray]:
        """
        One candidate at alpha > 0, a drain point or a reflection, in ruling 4's order, with
        the state and the ledger before any change: H_E and ln rho_kin,E (prompt 02's
        expressions); then alpha_effective; then, if alpha_eff > 0.0, prompt 02's drain, which
        records it and returns the drained state. If alpha_eff == 0.0 the candidate is recorded
        with pi_after = sign pi_before, delta_K = delta_rho/rho_R = 0.0 and H_after = H_before,
        nothing is added to the ledger, and None is returned: the caller makes no restart beyond
        what alpha = 0 makes there. resolved and the assessment (phi, ln E, ClassicalTurningPoint),
        or None, are recorded (prompt 05); the first candidate of the history makes dissipation
        mode's warning.
        """
        first_candidate_warning(assessment)

        pi_before: float = float(y_[1])
        H_before: float = H_Einstein_at(N_, y_)

        # rho_kin,E = H_E^2 pi^2/2, in logs (prompt 02)
        log_rho_kin: float = 2.0 * log(H_before) + 2.0 * log(abs(pi_before)) - LOG_2

        if one_loop:
            # the one-loop law (one-loop-drain prompt 01 §1.2): the gate as alpha_effective forms it,
            # the regime from the assessment, once (P18), the law's inputs L and ln R, once (prompt 02c,
            # orchestrator note 02c ruling 1), and alpha_eff by the measured law
            w_gate: float = gate_weight(nonadiabaticity) if resolved else 1.0
            regime, ell, estimate = drain_regime(model, assessment)
            law_L, law_log_R = drain_law_inputs(model, assessment)
            alpha_eff: float = drain_alpha_effective(
                regime, ell, law_L, law_log_R, alpha, w_gate
            )
        else:
            alpha_eff, w_gate = alpha_effective(nonadiabaticity, resolved)
        phi_a, log_E_a, turning_point_a = (
            (None, None, None) if assessment is None else assessment
        )
        weights = dict(
            nonadiabaticity=float(nonadiabaticity),
            w_gate=w_gate,
            resolved=resolved,
            phi_assessed=phi_a,
            log_E_assessed=log_E_a,
            classical_turning_point=turning_point_a,
        )

        y_out: Optional[np.ndarray] = None
        if alpha_eff > 0.0:
            y_out = drain(
                N_,
                y_,
                H_before,
                log_rho_kin,
                alpha_eff,
                kind,
                N_turn,
                phi_turn,
                sign,
                weights,
            )
        else:
            injections.append(
                Injection(
                    N=float(N_),
                    kind=kind,
                    N_turn=float(N_turn),
                    phi_turn=float(phi_turn),
                    pi_before=pi_before,
                    pi_after=sign * pi_before,
                    log_rho_kin=log_rho_kin,
                    alpha_eff=alpha_eff,
                    delta_K=0.0,
                    delta_rho_over_rho_rad=0.0,
                    H_Einstein_before=H_before,
                    H_Einstein_after=H_before,
                    **weights,
                )
            )

        # the drain law's record of the candidate, beside its Injection (one-loop-drain prompt 01 §1.4)
        if one_loop:
            drain_candidates.append(
                DrainCandidate(
                    regime=regime,
                    ell=ell,
                    alpha_eff=alpha_eff,
                    w_gate=w_gate,
                    log_E=log_E_a,
                    log_H=log(H_before),
                    L=law_L,
                    log_R=law_log_R,
                )
            )
            peak_estimates.append(estimate)
        return y_out

    def drain(
        N_: float,
        y_: np.ndarray,
        H_before: float,
        log_rho_kin: float,
        alpha_eff: float,
        kind: str,
        N_turn: float,
        phi_turn: float,
        sign: float,
        weights: dict,
    ) -> np.ndarray:
        """
        Drain alpha_eff of rho_kin,E at (N_, y_) into the ledger, and return the drained state, with
        pi -> sign sqrt(1 - alpha_eff) pi (sign = -1 for a floor reflection). The ledger is added
        to here, before the caller builds the new Radau instance. H_before and ln rho_kin,E are the
        candidate's, from the state and the ledger before the drain (inelastic-reflection prompt 03
        moved their computation, unchanged, to candidate(), which records them when alpha_eff is
        0.0 as well).
        """
        pi_before: float = float(y_[1])

        # Delta K = alpha_eff rho_kin,E e^{4N}, in logs, so that no intermediate overflows or
        # underflows
        log_alpha: float = log(alpha_eff)
        delta_K: float = exp(log_alpha + log_rho_kin + 4.0 * N_)
        delta_rho_over_rho_rad: float = exp(log_alpha + log_rho_kin - float(y_[2]))

        ledger.add(N_, delta_K)

        y_drained = y_.copy()
        y_drained[1] = sign * (sqrt(1.0 - alpha_eff) * pi_before)
        H_after: float = H_Einstein_at(N_, y_drained)

        injections.append(
            Injection(
                N=float(N_),
                kind=kind,
                N_turn=float(N_turn),
                phi_turn=float(phi_turn),
                pi_before=pi_before,
                pi_after=float(y_drained[1]),
                log_rho_kin=log_rho_kin,
                alpha_eff=alpha_eff,
                delta_K=delta_K,
                delta_rho_over_rho_rad=delta_rho_over_rho_rad,
                H_Einstein_before=H_before,
                H_Einstein_after=H_after,
                **weights,
            )
        )
        return y_drained

    def observe_turning_points() -> List[TurningPoint]:
        """
        The turning points on the step just appended, in order (orchestrator note 02, rulings 1
        and 15). pi is read at the step's two ends by scalar calls, interpolant(ts[k]) and
        interpolant(ts[k + 1]) (ts[k + 1] = N_final on the last step), as first_bounce reads it,
        and at TURNING_POINT_INTERIOR_SAMPLES equally spaced interior points in one vectorized
        call, as surfing_turning_points samples g.
          - One sign change across the samples, running end to end (pi_low and pi_high of opposite
            signs): the root is bracketed by the whole step, brentq(xtol = 1e-15), exactly as
            first_bounce brackets it.
          - Otherwise each sign change between consecutive samples that resolved_sign_changes keeps
            is refined by brentq on its sub-interval. A pair of consecutive changes whose excursion
            is not greater than the step's tolerance on pi (turning_point_tolerance, with
            params.atol and params.rtol) is dropped and counted in unresolved_pairs
            (electroweak-eos prompt 01a); its samples are folded as if the step held no turning
            point. The end-to-end bracket is used only when the samples show exactly one change,
            even if dropping a pair leaves one. A sample at which pi is exactly 0.0 is the turning
            point if the signs on either side of it (within the step) differ, counted once; a
            touch is not one.
        "wall" where pi goes from - to +, "plateau" where it goes from + to -. Records and returns
        the list, possibly empty.

        At alpha > 0 (inelastic-reflection prompt 03; orchestrator note 03, rulings 3 and 5) the
        same ten samples are read as full states, and ln m_eff^2 and H_E are computed at each
        (mass_and_H_at), before any drain at the step's end; step_nonadiabaticity gives A at each.
        The samples are folded into the running maximum of the half-oscillation in progress: those
        at or before a turning point found on the step into the half-oscillation that ends there,
        whose A_k the TurningPoint records with m_eff/H_E at the turning point itself, and those
        after it into the next.
        """
        nonlocal unresolved_pairs
        k: int = len(interpolants) - 1
        interpolant_ = interpolants[k]
        t_low: float = ts[k]
        t_high: float = ts[k + 1]

        y_low = interpolant_(t_low)
        y_high = interpolant_(t_high)
        pi_low: float = y_low[1]
        pi_high: float = y_high[1]
        N_interior = np.linspace(t_low, t_high, TURNING_POINT_INTERIOR_SAMPLES + 2)[
            1:-1
        ]
        y_interior = interpolant_(N_interior)
        pi_interior = y_interior[1]

        # the non-adiabaticity at the step's ten samples (prompt 03), at alpha > 0 only
        N_samples = None
        A_samples = None
        if alpha > 0.0:
            n_samples: int = TURNING_POINT_INTERIOR_SAMPLES + 2
            N_samples = np.empty(n_samples)
            N_samples[0] = t_low
            N_samples[1:-1] = N_interior
            N_samples[-1] = t_high
            log_m2_samples = np.empty(n_samples)
            H_samples = np.empty(n_samples)
            for j in range(n_samples):
                if j == 0:
                    y_j = y_low
                elif j == n_samples - 1:
                    y_j = y_high
                else:
                    y_j = y_interior[:, j - 1]
                log_m2_samples[j], H_samples[j] = mass_and_H_at(N_samples[j], y_j)
            A_samples = step_nonadiabaticity(N_samples, log_m2_samples, H_samples)

        # the common case, decided in numpy: every sample strictly of one sign, so no change
        if (pi_low > 0.0 and pi_high > 0.0 and (pi_interior > 0.0).all()) or (
            pi_low < 0.0 and pi_high < 0.0 and (pi_interior < 0.0).all()
        ):
            if alpha > 0.0:
                fold(N_samples, A_samples, float("inf"))
            return []

        Ns = [t_low] + [float(x) for x in N_interior] + [t_high]
        pis = [pi_low] + [float(x) for x in pi_interior] + [pi_high]

        # the sign changes between consecutive samples, less the pairs the solver does not resolve
        # (electroweak-eos prompt 01a): with none dropped, exactly the changes found before 01a
        changes, dropped = resolved_sign_changes(
            Ns,
            pis,
            turning_point_tolerance(pi_low, pi_high, params.atol, params.rtol),
        )
        unresolved_pairs += dropped

        def pi_of(t: float) -> float:
            return interpolant_(t)[1]

        roots = []
        if (
            len(changes) == 1
            and dropped == 0
            and (pi_low < 0.0 < pi_high or pi_low > 0.0 > pi_high)
        ):
            # end to end: the whole step is the bracket, as first_bounce takes it
            roots.append((brentq(pi_of, t_low, t_high, xtol=1e-15), changes[0].kind))
        else:
            for change in changes:
                kind = change.kind
                if change.N_zero is not None:
                    roots.append((change.N_zero, kind))
                    continue
                a, b = change.N_before, change.N_after
                # re-read the sub-interval's ends by scalar calls: a vectorized interior sample
                # can differ from the scalar one by rounding
                pi_a, pi_b = pi_of(a), pi_of(b)
                if pi_a == 0.0:
                    root = a
                elif pi_b == 0.0:
                    root = b
                elif (pi_a < 0.0) == (pi_b < 0.0):
                    root = a if abs(pi_a) <= abs(pi_b) else b
                else:
                    root = brentq(pi_of, a, b, xtol=1e-15)
                roots.append((root, kind))

        found = []
        for N_turn, kind in roots:
            y_turn = interpolant_(N_turn)
            A_k: Optional[float] = None
            m_eff_over_H: Optional[float] = None
            if alpha > 0.0:
                fold(N_samples, A_samples, N_turn)
                A_k = close_half_oscillation(N_turn)
                log_m2_turn, H_turn = mass_and_H_at(N_turn, y_turn)
                m_eff_over_H = exp(0.5 * log_m2_turn - log(H_turn))
            found.append(
                TurningPoint(
                    N=float(N_turn),
                    phi_Einstein=float(y_turn[0]),
                    log_T_Jordan=float(y_turn[4]),
                    kind=kind,
                    nonadiabaticity=A_k,
                    m_eff_over_H=m_eff_over_H,
                )
            )
        if alpha > 0.0:
            fold(N_samples, A_samples, float("inf"))
        turning_points.extend(found)
        return found

    def handle_turning_point(turning_point: TurningPoint) -> None:
        """
        One turning point, in order along its step (prompts 02 and 03): a wall-side one arms a drain
        and any turning point drops one still armed (counted; orchestrator note 03, ruling 7). Since
        prompt 05 the armed wall-side point captures the infall's latest assessment, and every
        turning point starts a new infall. At alpha = 0, a wall-side turning point below the
        threshold makes natural mode's warning: there the field is at rest, so phi is phi_t.
        """
        nonlocal armed, armed_assessment, infall_assessment, dropped_drains
        nonlocal natural_warned
        if armed is not None:
            dropped_drains += 1
        if turning_point.kind == "wall":
            armed = turning_point
            armed_assessment = infall_assessment
        else:
            armed = None
            armed_assessment = None
        infall_assessment = None

        if (
            alpha == 0.0
            and not natural_warned
            and turning_point.kind == "wall"
            and turning_point.phi_Einstein < UNRESOLVED_PHI_T_THRESHOLD_MP * Mp
        ):
            natural_warned = True
            warnings.append(
                f"{WARNING_TURNING_POINT_BELOW} {UNRESOLVED_PHI_T_THRESHOLD_MP:.3g} M_P "
                f"(phi_t = {turning_point.phi_Einstein / Mp:.4g} M_P at the wall-side turning "
                f"point at N = {turning_point.N:.10g}), where dissipation mode would reflect "
                f"without resolving it; this history may fail on Radau's float spacing or the "
                f"step budget"
            )

    def natural_floor_warning(N_: float, y_: np.ndarray) -> None:
        """
        Natural mode's warning at its first floor reflection (prompt 05 §1.5; orchestrator note 05,
        ruling 8): the model's classical turning point, from E = H_E^2 pi^2/2 + V - V_floor, is
        computed once, and below the threshold makes the warning. A None turning point means the
        field is at its turning point to rounding, and phi is taken for phi_t. A
        ComputationFailureError from H_E or the model makes the "could not be assessed" warning,
        and the history goes on; any other exception propagates. It changes no value.
        """
        nonlocal natural_warned, natural_floor_called, unresolvable_assessments
        if natural_warned or natural_floor_called:
            return
        natural_floor_called = True

        phi_: float = float(y_[0])
        try:
            H_: float = H_Einstein_at(N_, y_)
            log_rho_kin_: float = 2.0 * log(H_) + 2.0 * log(abs(float(y_[1]))) - LOG_2
            log_E_, turning_point_ = assess_infall(model, phi_, log_rho_kin_)
            # the wall-search fix's count (one-loop-drain prompt 01 §1.6): before it, this call raised
            # and made the "could not be assessed" warning; now phi is taken for phi_t, below
            if turning_point_ is None and _turning_point_unresolvable(
                model.log_V_floor, log_E_
            ):
                unresolvable_assessments += 1
        except ComputationFailureError as e:
            natural_warned = True
            warnings.append(
                f"{WARNING_WALL_UNASSESSED} at the first floor reflection (N = {N_:.10g}, "
                f"phi = {phi_ / Mp:.4g} M_P): {e.message}; the history goes on"
            )
            return

        phi_t_: float = phi_ if turning_point_ is None else turning_point_.phi_t
        if phi_t_ < UNRESOLVED_PHI_T_THRESHOLD_MP * Mp:
            natural_warned = True
            warnings.append(
                f"{WARNING_TURNING_POINT_BELOW} {UNRESOLVED_PHI_T_THRESHOLD_MP:.3g} M_P "
                f"(phi_t = {phi_t_ / Mp:.4g} M_P at the floor reflection at N = {N_:.10g}), "
                f"where dissipation mode would reflect without resolving it; this history may "
                f"fail on Radau's float spacing or the step budget"
            )

    # parking (inelastic-reflection prompt 06; README §8.1 R14, R15, §8.2 P19, P21), at alpha > 0 on a
    # model with a wall only (assess_wall); at alpha = 0 none of it runs (P22)
    #   parking:  the Parking record of the switch, or None; the switch is made at most once, and the
    #             history then ends in the parked phase, or is released (prompt 07)
    #   release:  the Release record of the release (inelastic-reflection prompt 07; README §8.1 R16,
    #             §8.2 P24), or None; once it is set no candidate makes the ratio test or calls the
    #             tracker, so the field never parks again
    #   accepted_at_release: accepted_steps at the release, from which Release.steps_after is counted
    parking: Optional[Parking] = None
    release: Optional[Release] = None
    accepted_at_release: int = 0

    def run_parked_phase(record: Parking, phi_anchor: float, y_parked: np.ndarray):
        """
        The parked phase (prompt 06 §1.3; orchestrator note 06, rulings 3, 7, 9 and 11), from the
        switch's N and the parked state y_parked to the history's end or to the release (prompt 07).
        Returns (_ParkedPhaseEnd, the parked steps, the smallest m_eff/H_E of the tracker); at a
        release it also sets `release`.

        The reduced state (ln rho_R,E, ln f_m, ln T_J) is integrated by DOP853 with params.rtol,
        params.atol and params.global_max_step as its largest step, on ParkedRHS, which evaluates
        ODERHS's components 2-4 through fun at the tracker (phi_min, pi_min). Before each step after
        the first, the start of minimum_on_trace is set to the phi_min at the step's left node, carried
        from the previous step's end, and the solver's derivative there is re-evaluated with it, so
        that every evaluation of one step, and its segment, searches from the same float. Each
        accepted step appends a ParkedSegment to ts and interpolants. At the end of every accepted step
        (at N_final on the step that ends the history) the tracker's m_eff/H_E is checked; below the
        threshold the field is released on that step (prompt 07; README §8.1 R16): at the root N_rel
        of m_eff/H_E = PARKING_ADIABATICITY_THRESHOLD on the step's ParkedSegment, between its left
        node and the point checked (release_on_step). Otherwise the end is the oscillating phase's: the
        first step with ln T_J < log_T_stop, at the root on its interpolant; N_stop; or N_failsafe, a
        failure. On the step that ends the history the check at N_final comes first, so a tracker below
        the threshold there is released, and the oscillating phase ends the history. The step budget
        counts both phases. A ComputationFailureError from the right-hand side, the model or the
        tracker is the history's failure, with no step halving or retry: every failure reason here
        names the switch.
        """
        nonlocal accepted_steps
        cosmology_ = policy.cosmology
        MeV: float = policy.units.MeV
        switch_text: str = (
            f"parked at N = {record.N:.10g}, T_J = {exp(record.log_T_Jordan) / MeV:.5g} MeV, "
            f"at candidate #{record.candidate_index}"
        )

        def failure(reason: str) -> ComputationFailureError:
            return ComputationFailureError(
                f"integrate_scalar_history{label}: in the parked phase ({switch_text}): {reason}"
            )

        def guarded(f, *args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ComputationFailureError as e:
                raise failure(e.message) from e

        parked_steps = 0
        min_m_eff_over_H: float = record.m_eff_over_H

        def fold_minimum(m_eff_over_H_: float) -> None:
            nonlocal min_m_eff_over_H
            if not (m_eff_over_H_ >= min_m_eff_over_H):
                min_m_eff_over_H = m_eff_over_H_

        def check(N_: float, y_: np.ndarray) -> Optional[float]:
            """
            The tracker's adiabaticity at a step end (ruling 11), with the ledger as it stands.
            Returns None where it holds, its m_eff/H_E folded into the minimum; and m_eff/H_E where it
            does not, unfolded: that check lies after the release, and is not part of the parked
            phase (orchestrator note 07, ruling 4).
            """
            m_eff_over_H_, _ = guarded(tracker_m_eff_over_H, policy, hubble, N_, y_)
            if not tracker_adiabatic(m_eff_over_H_):
                return m_eff_over_H_
            fold_minimum(m_eff_over_H_)
            return None

        def release_on_step(
            segment_: ParkedSegment,
            appended: bool,
            N_check: float,
            m_eff_over_H_check: float,
        ) -> _ParkedPhaseEnd:
            """
            The release (prompt 07 §1.1; orchestrator note 07, rulings 2-4), on the step whose check at
            N_check gave m_eff/H_E = m_eff_over_H_check below the threshold. g(N) = m_eff/H_E of the
            tracker at segment_(N), a scalar call, less PARKING_ADIABATICITY_THRESHOLD, with the ledger
            as it stands; evaluated at the step's left node on the segment, not taken from the check
            there. If g > 0 there, N_rel is brentq's root on [left node, N_check] with
            RELEASE_ROOT_XTOL and RELEASE_ROOT_RTOL, and it replaces the step's node in ts (N_check,
            or N_end on the step that ends the history: never appended twice); the segment stays. If
            g <= 0 there, the release is at the left node, and the step's segment, if appended, is
            removed: no zero-length segment. The released state is segment_(N_rel), one scalar call,
            the floats the root's last evaluation there saw. Nothing is drained and nothing is added
            to the ledger. The step stays counted in accepted_steps and parked_steps.
            """
            nonlocal release, accepted_at_release
            N_left: float = float(segment_.t_min)

            def g(N__: float) -> float:
                y__ = guarded(segment_, N__)
                m__, _ = guarded(tracker_m_eff_over_H, policy, hubble, N__, y__)
                return m__ - PARKING_ADIABATICITY_THRESHOLD

            g_left: float = g(N_left)
            if g_left > 0.0:
                g_check: float = g(N_check)
                if not (g_check < 0.0):
                    raise failure(
                        f"cannot bracket the release on the step [{N_left:.10g}, {N_check:.10g}] "
                        f"(m_eff/H_E - {PARKING_ADIABATICITY_THRESHOLD:.3g} = {g_left:.5g}, "
                        f"{g_check:.5g} on the segment; {m_eff_over_H_check:.5g} at the check)"
                    )
                N_rel: float = float(
                    brentq(
                        g,
                        N_left,
                        N_check,
                        xtol=RELEASE_ROOT_XTOL,
                        rtol=RELEASE_ROOT_RTOL,
                    )
                )
                ts[-1] = N_rel
            else:
                N_rel = N_left
                if appended:
                    ts.pop()
                    interpolants.pop()

            y_rel = guarded(segment_, N_rel)
            m_rel, H_rel = guarded(tracker_m_eff_over_H, policy, hubble, N_rel, y_rel)
            fold_minimum(m_rel)
            release = Release(
                N=N_rel,
                log_T_Jordan=float(y_rel[4]),
                phi_Einstein=float(y_rel[0]),
                pi_Einstein=float(y_rel[1]),
                m_eff_over_H=m_rel,
                N_check=float(N_check),
                m_eff_over_H_check=float(m_eff_over_H_check),
                H_Einstein=H_rel,
                candidate_index=len(injections),
                steps_after=0,
            )
            accepted_at_release = accepted_steps
            return _ParkedPhaseEnd(N=N_rel, state=y_rel, released=True)

        anchor: float = float(phi_anchor)
        rhs_parked = ParkedRHS(fun, model, cosmology_, anchor)
        psolver = guarded(
            DOP853,
            rhs_parked,
            record.N,
            np.array(y_parked[2:], dtype=float),
            N_bound,
            max_step=global_max_step,
            rtol=params.rtol,
            atol=params.atol,
        )

        while True:
            if parked_steps > 0:
                rhs_parked.phi_anchor = anchor
                psolver.f = guarded(psolver.fun, psolver.t, psolver.y)

            message = guarded(psolver.step)
            if message is not None:
                raise failure(
                    f'DOP853 step failed at N = {psolver.t:.10g}: "{message}"'
                )

            accepted_steps += 1
            parked_steps += 1
            if accepted_steps > step_budget:
                raise ComputationFailureError(
                    f"step budget exhausted: integrate_scalar_history{label} took {accepted_steps} "
                    f"accepted steps (budget {step_budget}) at N={psolver.t:.10g}, "
                    f"T_J={exp(psolver.y[2]) / policy.GeV:.5g} GeV, with {len(reflections)} "
                    f"reflection(s), {parked_steps} of the steps in the parked phase ({switch_text})"
                )

            reduced_interpolant = guarded(psolver.dense_output)
            segment = ParkedSegment(reduced_interpolant, anchor, model, cosmology_)

            if psolver.y[2] < log_T_stop:
                N_low: float = psolver.t_old
                N_high: float = psolver.t

                def crossing(N_):
                    return reduced_interpolant(N_)[2] - log_T_stop

                g_low = crossing(N_low)
                g_high = crossing(N_high)
                if not (g_low >= 0.0 and g_high < 0.0):
                    raise failure(
                        f"cannot bracket the termination root on the last step [{N_low:.10g}, "
                        f"{N_high:.10g}] (ln T_J - log_T_stop = {g_low:.5g}, {g_high:.5g})"
                    )
                appended: bool = g_low != 0.0
                if not appended:
                    # the previous accepted state sits exactly on log_T_stop: it is the final node
                    N_end: float = N_low
                else:
                    N_end = brentq(crossing, N_low, N_high)
                    ts.append(N_end)
                    interpolants.append(segment)
                y_end = guarded(segment, N_end)
                m_failed = check(N_end, y_end)
                if m_failed is not None:
                    # the release comes first, and the oscillating phase ends the history (prompt 07
                    # §1.1 item 5)
                    released_ = release_on_step(segment, appended, N_end, m_failed)
                    return released_, parked_steps, min_m_eff_over_H
                return (
                    _ParkedPhaseEnd(
                        N=N_end, state=StateVector._make(y_end), released=False
                    ),
                    parked_steps,
                    min_m_eff_over_H,
                )

            ts.append(psolver.t)
            interpolants.append(segment)

            phi_end, pi_end = guarded(
                parked_tracker, model, cosmology_, anchor, psolver.y
            )
            y_end = np.array([phi_end, pi_end, *psolver.y], dtype=float)
            m_failed = check(psolver.t, y_end)
            if m_failed is not None:
                released_ = release_on_step(segment, True, psolver.t, m_failed)
                return released_, parked_steps, min_m_eff_over_H
            anchor = phi_end

            if psolver.status == "finished":
                if stop_at_bound:
                    return (
                        _ParkedPhaseEnd(
                            N=psolver.t, state=StateVector._make(y_end), released=False
                        ),
                        parked_steps,
                        min_m_eff_over_H,
                    )
                raise failure(
                    f"the failsafe N={N_failsafe:.5g} was reached without ln T_J falling below "
                    f"log_T_stop={log_T_stop:.5g} (ln T_J={psolver.y[2]:.5g})"
                )

    def cut(N_: float, y_post: np.ndarray, kind: str, pi_in: Optional[float]):
        """
        The cut at a candidate (prompt 06 §1.2; orchestrator note 06, ruling 2), after candidate()
        has recorded it and made its drain; y_post is the state after it (pi is not read). Returns
        None if the history goes on, or the parked phase's _ParkedPhaseEnd: the history's end
        (released False), or the release (released True), from which the loop resumes (prompt 07;
        orchestrator note 07, ruling 1).
          - the ratio test on the candidate's index and rho_kin,E/rho_R,E, rho_kin,E its log_rho_kin
            (from before its drain), rho_R,E the state's. After a release (prompt 07; README §8.2
            P24) neither the ratio test nor the tracker is called: the field never parks again;
          - if it passes, the tracker at y_post (cut_tracker), and the decision on its m_eff/H_E;
          - if both hold, the switch: the leftover (the drained state's rho_kin,E, from the candidate's
            H_Einstein_after and pi_after) into the ledger at the same N (P19); H_E at the parked state
            with it; the record; armed cleared; the parked phase;
          - at a reflection (pi_in given) where the cut did not fire, the guard (P21), after a
            release too; its reason then says that the field was released, and where.
        """
        nonlocal parking, armed, armed_assessment
        index: int = len(injections) - 1
        record_: Injection = injections[index]
        ratio: float = exp(record_.log_rho_kin - float(y_post[2]))

        tracker: Optional[CutTracker] = None
        if release is None and parking_ratio_test(index, ratio):
            tracker = cut_tracker(policy, hubble, N_, y_post)
            if tracker_adiabatic(tracker.m_eff_over_H):
                delta_K, delta_rho_over_rho_rad = parking_leftover(
                    record_.H_Einstein_after, record_.pi_after, N_, float(y_post[2])
                )
                ledger.add(N_, delta_K)

                y_parked = np.array(
                    [tracker.phi_min, tracker.pi_min, y_post[2], y_post[3], y_post[4]],
                    dtype=float,
                )
                H_parked: float = H_Einstein_at(N_, y_parked)
                armed = None
                armed_assessment = None
                parking = Parking(
                    N=float(N_),
                    log_T_Jordan=float(y_post[4]),
                    candidate_index=index,
                    phi_Einstein=float(y_post[0]),
                    pi_Einstein=float(y_post[1]),
                    phi_min=tracker.phi_min,
                    pi_min=tracker.pi_min,
                    rho_kin_over_rho_rad=ratio,
                    delta_K=delta_K,
                    delta_rho_over_rho_rad=delta_rho_over_rho_rad,
                    m_eff_over_H=tracker.m_eff_over_H,
                    H_Einstein_before=record_.H_Einstein_before,
                    H_Einstein_parked=H_parked,
                    delta_log_A=model.log_Omega(tracker.phi_min)
                    - model.log_Omega(float(y_post[0])),
                    parked_steps=0,
                    min_m_eff_over_H=tracker.m_eff_over_H,
                )
                end_, parked_steps, min_m_eff_over_H = run_parked_phase(
                    parking, tracker.phi_root, y_parked
                )
                parking = parking._replace(
                    parked_steps=parked_steps, min_m_eff_over_H=min_m_eff_over_H
                )
                return end_

        if pi_in is not None and parking_guard(pi_in, Mp):
            if tracker is not None:
                m_text: str = f"{tracker.m_eff_over_H:.5g}"
            elif release is not None:
                m_text = (
                    f"not computed (the field was released at N = {release.N:.10g})"
                )
            else:
                m_text = "not computed (the ratio test did not pass)"
            raise ComputationFailureError(
                f"integrate_scalar_history{label}: the |pi| guard (P21): a reflection of kind "
                f"{kind} at N = {N_:.10g} with |pi_in| = {abs(pi_in) / Mp:.5g} M_P < "
                f"{PARKING_PI_GUARD_MP:.3g} M_P, where the cut did not fire "
                f"(rho_kin/rho_R,E = {ratio:.5g}, m_eff/H_E = {m_text})"
            )
        return None

    def resume_after_release(end_: _ParkedPhaseEnd):
        """
        The hand-back of a release to the oscillating loop (prompt 07 §1.1 items 5 and 6; orchestrator
        note 07, ruling 1): a new Radau instance from the released state at N_rel, as after a drain,
        and the state the loop carries reset: no drain armed, no infall assessment, and the gate's
        half-oscillation begins at N_rel with no sample, so that no sample from before the switch
        reaches a candidate after the release. The warnings already made stand.
        """
        nonlocal armed, armed_assessment, infall_assessment, running_max, boundary_N
        armed = None
        armed_assessment = None
        infall_assessment = None
        running_max = None
        boundary_N = float(end_.N)
        y_released = np.array(end_.state, dtype=float)
        return new_solver(float(end_.N), y_released, restart=True)

    try:
        while True:
            N: float = solver.t
            phi: float = solver.y[0]
            pi_: float = solver.y[1]

            # the unresolved turnaround (inelastic-reflection prompt 05): at alpha > 0, on a model
            # with a wall, every step start with pi < 0 assesses the wall ahead before the floor
            # rule is tested. A None turning point makes no decision, and the infall's latest
            # assessment stands (orchestrator note 05, ruling 7)
            unresolved: bool = False
            if assess_wall and pi_ < 0.0:
                H_start: float = H_Einstein_at(N, solver.y)
                log_rho_kin_start: float = 2.0 * log(H_start) + 2.0 * log(-pi_) - LOG_2
                log_E_start, turning_point_start = assess_infall(
                    model, float(phi), log_rho_kin_start
                )
                if turning_point_start is not None:
                    infall_assessment = (
                        float(phi),
                        log_E_start,
                        turning_point_start,
                    )
                    unresolved = unresolved_turnaround(phi, turning_point_start, Mp)
                    # the user's ruling (b) (one-loop-drain prompt 01 §1.7, 2026-11-03): where the
                    # unresolved reflection would fail G2 (the field already within the wall's
                    # mirror-invalid region, as in the linear tail when phi_t crosses the threshold
                    # mid-infall), no unresolved decision is made at this step start; the floor rule is
                    # tested as usual. Every such state raised before, so nothing that succeeds moves
                    if unresolved and unresolved_reflection_refused(
                        policy, N, solver.y
                    ):
                        unresolved = False
                        unresolved_declined += 1
                elif _turning_point_unresolvable(model.log_V_floor, log_E_start):
                    # the wall-search fix answered this assessment (one-loop-drain prompt 01 §1.6):
                    # counted, and nothing else changes, as at any None. assess_infall's ln E is at
                    # least the finite ln(V - V_floor) at phi wherever that is finite, so where the
                    # predicate holds the None is the fix's, never the r_0 >= 0 test's
                    unresolvable_assessments += 1

            # the floor rule: an inward-moving field whose velocity cap would fall below the
            # representable step is reflected elastically (README §2 (b)); at alpha > 0 the
            # reflection is also the drain point (inelastic-reflection prompt 02; README §6.2 P3).
            # An unresolved turnaround (prompt 05) is reflected through the same branch, first
            if unresolved or (pi_ < 0.0 and f_cap * phi / (-pi_) < h_floor):
                kind: str = "unresolved" if unresolved else "floor"
                ratio = _reflection_guards(policy, N, solver.y, task_label)
                max_ratio = ratio if max_ratio is None else max(max_ratio, ratio)

                reflections.append(
                    Reflection(N=N, phi_Einstein=phi, pi_Einstein_in=pi_, kind=kind)
                )
                supervisor.notify_reflection(N)

                # a drain still armed is dropped: the field turned inwards before its drain point
                if armed is not None:
                    dropped_drains += 1
                armed = None
                armed_assessment = None

                # at alpha > 0 the reflection is a candidate (prompt 03): it ends the
                # half-oscillation of the infall, whose A_k it records. Since prompt 05 it is
                # unresolved, with w_gate = 1.0, and records the infall's assessment. If its
                # alpha_eff is 0.0, or at alpha = 0, the restart is the elastic one; at alpha = 0
                # the first floor reflection makes natural mode's warning, changing nothing
                y_reflected: Optional[np.ndarray] = None
                if alpha > 0.0:
                    A_k: float = close_half_oscillation(N)
                    y_reflected = candidate(
                        N,
                        solver.y,
                        A_k,
                        kind,
                        N,
                        phi,
                        -1.0,
                        False,
                        infall_assessment,
                    )
                else:
                    natural_floor_warning(N, solver.y)
                infall_assessment = None
                if y_reflected is None:
                    y_reflected = solver.y.copy()
                    y_reflected[1] = -pi_

                # the cut (inelastic-reflection prompt 06), after the candidate; where it does not
                # fire, the |pi| guard (P21). At alpha = 0, or without a wall, neither runs
                if assess_wall:
                    parked = cut(N, y_reflected, kind, pi_)
                    if parked is not None:
                        if parked.released:
                            # the release (prompt 07): the loop resumes from the released state,
                            # not from y_reflected
                            solver = resume_after_release(parked)
                            continue
                        N_final, final_state = parked.N, parked.state
                        break

                solver = new_solver(N, y_reflected, restart=True)
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

            # the guard keeps the guarded step's start state, to retake a discarded step from it
            y_step_start: Optional[np.ndarray] = (
                solver.y.copy() if guard_first_step else None
            )

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

            # the guard (reflection-restart prompt 03 §1.2; orchestrator note 03, ruling 5): the first
            # accepted step of a Radau restarted with pi > 0 is tested before it is used for anything.
            # The mark is cleared whether the step passes or not; a retake marks its own instance.
            # A flagged step is discarded, leaving nothing but the count and its RHS evaluations, and
            # retaken from its start (N, the state) at half its h
            if guard_first_step:
                guard_first_step = False
                if _outward_step_flagged(
                    solver.dense_output(),
                    solver.t_old,
                    solver.t,
                    y_step_start[0],
                    y_step_start[1],
                    solver.y[1],
                    f_cap,
                ):
                    h_discarded: float = solver.t - solver.t_old
                    h_retake: float = 0.5 * h_discarded
                    if h_retake < h_floor:
                        raise ComputationFailureError(
                            f"integrate_scalar_history{label}: the restart guard (reflection-restart "
                            f"prompt 03): the first step of a Radau restarted at N={N:.10g} with "
                            f"phi_E={y_step_start[0]:.5g}, pi_E={y_step_start[1]:.5g} is flagged at "
                            f"h={h_discarded:.5g}, and half of it is below h_floor={h_floor:.3g}"
                        )
                    restart_retakes += 1
                    y_retake: np.ndarray = y_step_start
                    solver = new_solver(N, y_retake, retake_step=h_retake)
                    continue

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
                np.minimum(
                    solver.jac_factor, jacobian_factor_max, out=solver.jac_factor
                )

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
                    # the step that ends the history is classified up to N_final, and never drains: a
                    # wall-side turning point on it arms a drain that is dropped below (ruling 7)
                    for turning_point in observe_turning_points():
                        handle_turning_point(turning_point)
                final_state = StateVector._make(interpolant(N_final))
                break

            ts.append(solver.t)
            interpolants.append(interpolant)

            # turning points (inelastic-reflection prompt 02; orchestrator note 02, rulings 1, 3 and
            # 15), in order along the step: a wall-side one arms a drain, and any turning point drops
            # one still armed (counted; orchestrator note 03, ruling 7)
            for turning_point in observe_turning_points():
                handle_turning_point(turning_point)

            if solver.status == "finished":
                if stop_at_bound:
                    N_final = solver.t
                    final_state = StateVector._make(solver.y)
                    break

                raise ComputationFailureError(
                    f"integrate_scalar_history{label}: the failsafe N={N_failsafe:.5g} was reached "
                    f"without ln T_J falling below log_T_stop={log_T_stop:.5g} (ln T_J={solver.y[4]:.5g})"
                )

            # the drain point (README §6.2 P3; orchestrator note 02, ruling 2): the first accepted state
            # since the wall-side turning point with pi > 0 and pi' <= 0, read before any restart
            # At alpha > 0 it is a candidate (prompt 03), gated on the wall-side turning point's A_k;
            # at alpha = 0, or if its alpha_eff is 0.0, nothing is drained and no restart is made
            if armed is not None and solver.y[1] > 0.0 and solver.f[1] <= 0.0:
                if alpha > 0.0:
                    y_drained = candidate(
                        solver.t,
                        solver.y,
                        armed.nonadiabaticity,
                        "turning_point",
                        armed.N,
                        armed.phi_Einstein,
                        1.0,
                        True,
                        armed_assessment,
                    )
                    # the cut (inelastic-reflection prompt 06), after the candidate, whether or
                    # not it drained; a switch clears armed and ends the history in the parked phase
                    if assess_wall:
                        parked = cut(
                            solver.t,
                            solver.y if y_drained is None else y_drained,
                            "turning_point",
                            None,
                        )
                        if parked is not None:
                            if parked.released:
                                # the release (prompt 07): the loop resumes from the released
                                # state, and makes no restart from y_drained
                                solver = resume_after_release(parked)
                                y_drained = None
                            else:
                                N_final, final_state = parked.N, parked.state
                                break
                    if y_drained is not None:
                        solver = new_solver(solver.t, y_drained, restart=True)
                armed = None
                armed_assessment = None
    except ComputationFailureError as e:
        # the warnings survive a failure (orchestrator note 05, ruling 2): the same exception,
        # carrying the warnings collected before it, so that integrate_history's failure payload and
        # tune_history can report them; and the drain law's record as far as the loop reached
        # (one-loop-drain prompt 01 §1.4)
        e.warnings = tuple(warnings)
        e.drain = drain_record()
        raise

    # a drain still armed when the history ends is dropped (ruling 7)
    if armed is not None:
        dropped_drains += 1

    return IntegrationResult(
        # right-continuous at the nodes (inelastic-reflection prompt 02, §1.4): at a drain or a
        # reflection it returns the state after it, which the ledger's N_j <= N pairs with the new
        # K. At alpha = 0 only a query at an interior node exactly could see a difference
        solution=OdeSolution(ts, interpolants, alt_segment=True),
        N_final=N_final,
        final_state=final_state,
        nfev=nfev,
        accepted_steps=accepted_steps,
        steps_rejected_by_exception=steps_rejected_by_exception,
        reflections=reflections,
        max_wall_to_kinetic_ratio=max_ratio,
        turning_points=turning_points,
        injections=injections,
        dropped_drains=dropped_drains,
        warnings=tuple(warnings),
        parking=parking,
        release=(
            None
            if release is None
            else release._replace(steps_after=accepted_steps - accepted_at_release)
        ),
        restart_retakes=restart_retakes,
        drain=drain_record(),
        peak_estimates=tuple(peak_estimates) if one_loop else None,
        unresolved_turning_point_pairs=unresolved_pairs,
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


# a turning point of the Jordan-frame temperature before the first bounce (surfing-temperature
# prompt 02, T1; README §0.2). T_J is stationary exactly where g = 1 + (d ln Omega/dphi) pi = 0.
#   N:             e-fold number of the crossing
#   phi_Einstein:  the Einstein-frame field there
#   pi_Einstein:   the Einstein-frame field velocity dphi/dN there
#   log_T_Jordan:  ln of the Jordan-frame temperature there, in the cosmology's units
#   direction:     "min" where g falls through zero (T_J has a minimum), "max" where it rises
SurfingCrossing = namedtuple(
    "SurfingCrossing",
    [
        "N",
        "phi_Einstein",
        "pi_Einstein",
        "log_T_Jordan",
        "direction",
    ],
)


def surfing_turning_points(
    result: IntegrationResult,
    model: RealizedModel,
    interior_samples: int = 8,
) -> List[SurfingCrossing]:
    """
    The turning points of the Jordan-frame temperature before the first bounce (surfing-temperature
    prompt 02, T1), in order: the zeros of g(N) = 1 + (d ln Omega/dphi)(phi(N)) pi(N), the
    numerator of the temperature law (ODERHS), read on the accepted steps' interpolants through
    the realized model. For the exponential model g = 1 + beta pi/M_P.

    The accepted steps are walked in order, as first_bounce walks them, up to the first bounce:
    first_bounce(result).N, on the bounce's own step, or the history's end if there is none. A
    reflection is a step boundary, and the first reflection is at or after the first bounce, so
    the walk never crosses one. On each step g is read at its two ends and at `interior_samples`
    equally spaced interior points, so that two zeros on one step are found rather than cancelled.
    On SciPy's Radau dense output each component is a cubic in N on each step, so g has at most
    three zeros on a step; the default of eight interior points splits the step into nine parts,
    and two zeros on one step are both found unless they are closer than a ninth of it. The step
    boundaries are read once, from the step that ends at them. Each sign change of g between
    consecutive samples is refined by brentq (xtol = 1e-15, first_bounce's) on that step's
    interpolant; if the two ends' signs agree when re-evaluated there (a zero on an end, seen
    through rounding), the end where |g| is smaller is taken. A sample at which g is exactly zero
    is a crossing there if the signs before and after it differ, and is counted once.

    `direction` is "min" where g goes from positive to negative (beta pi/M_P falls through -1, and
    T_J has a minimum) and "max" where it goes the other way. This function is not called by
    compute_scalar_history, and nothing is stored (surfing-temperature README §5 rule 9).

    :param result: the IntegrationResult of integrate_scalar_history
    :param model: the history's realized model
    :param interior_samples: interior sample points per step
    :return: list of SurfingCrossing, possibly empty
    """
    ts = result.solution.ts
    interpolants = result.solution.interpolants

    bounce: Optional[FirstBounce] = first_bounce(result)
    N_end: float = bounce.N if bounce is not None else float(ts[-1])

    def refine(g, a: float, b: float) -> float:
        g_a: float = g(a)
        g_b: float = g(b)
        if g_a == 0.0:
            return a
        if g_b == 0.0:
            return b
        if (g_a < 0.0) == (g_b < 0.0):
            return a if abs(g_a) <= abs(g_b) else b
        return float(brentq(g, a, b, xtol=1e-15))

    def crossing_at(interpolant, N: float, direction: str) -> SurfingCrossing:
        y = interpolant(N)
        return SurfingCrossing(
            N=float(N),
            phi_Einstein=float(y[0]),
            pi_Einstein=float(y[1]),
            log_T_Jordan=float(y[4]),
            direction=direction,
        )

    crossings: List[SurfingCrossing] = []

    # the sign of g at the last sample where it was non-zero, and the first sample since then at
    # which g was exactly zero, as (interpolant, N)
    last_sign: int = 0
    zero_at = None

    for k, interpolant in enumerate(interpolants):
        t_low: float = float(ts[k])
        if t_low >= N_end:
            break
        t_high: float = min(float(ts[k + 1]), N_end)

        def g(t: float, interpolant=interpolant) -> float:
            y = interpolant(t)
            return 1.0 + model.d_logOmega_dphi(float(y[0])) * float(y[1])

        Ns = np.linspace(t_low, t_high, interior_samples + 2)
        ys = interpolant(Ns)
        gs = [
            1.0 + model.d_logOmega_dphi(float(phi)) * float(pi_)
            for phi, pi_ in zip(ys[0], ys[1])
        ]

        # the step's start was read as the previous step's end
        for j in range(0 if k == 0 else 1, len(Ns)):
            g_j: float = gs[j]
            if g_j == 0.0:
                if zero_at is None:
                    zero_at = (interpolant, float(Ns[j]))
                continue

            sign: int = 1 if g_j > 0.0 else -1
            if last_sign != 0 and sign != last_sign:
                direction: str = "min" if last_sign > 0 else "max"
                if zero_at is not None:
                    crossings.append(crossing_at(zero_at[0], zero_at[1], direction))
                else:
                    # no zero sample since the last non-zero one, so it is the previous sample,
                    # on this step (its start, if j = 1)
                    N_cross: float = refine(g, float(Ns[j - 1]), float(Ns[j]))
                    crossings.append(crossing_at(interpolant, N_cross, direction))

            last_sign = sign
            zero_at = None

    return crossings


# phi and rho_NP/rho_R,J at the history's first crossing of T_J = 1 MeV and of T_J = 70 keV
# (science-readiness prompt 06b; README §2 (n)). phi is in the cosmology's units; the ratio is
# dimensionless. A field is None where the history does not reach that temperature.
FixedTValues = namedtuple(
    "FixedTValues",
    [
        "phi_Einstein_1MeV",
        "density_NP_ratio_1MeV",
        "phi_Einstein_70keV",
        "density_NP_ratio_70keV",
    ],
)


def _T_Jordan_crossing_step(result: IntegrationResult, log_T: float):
    """
    (k, N) for the first crossing of ln T_J = log_T, where k indexes the accepted step it lies
    on; or None. The body of T_Jordan_crossing, which see.
    """
    ts = result.solution.ts
    interpolants = result.solution.interpolants

    for k, interpolant in enumerate(interpolants):
        t_low: float = ts[k]
        t_high: float = ts[k + 1]

        g_low: float = interpolant(t_low)[4] - log_T
        g_high: float = interpolant(t_high)[4] - log_T

        if g_low == 0.0:
            return k, float(t_low)
        if g_high == 0.0:
            return k, float(t_high)
        if (g_low < 0.0) != (g_high < 0.0):
            N_cross: float = brentq(
                lambda t: interpolant(t)[4] - log_T, t_low, t_high, xtol=1e-15
            )
            return k, float(N_cross)

    return None


def T_Jordan_crossing(result: IntegrationResult, log_T: float) -> Optional[float]:
    """
    The e-fold number N of the history's first crossing of ln T_J = log_T, or None if the
    history never reaches it (science-readiness prompt 06b; README §2 (n)).

    The accepted steps are walked in order, on their own interpolants, as first_bounce walks
    them. The crossing is on the first step [t_k, t_{k+1}] on which ln T_J - log_T changes sign
    or reaches zero at an end, and is located by brentq (xtol = 1e-15) on that step's
    interpolant. ln T_J is continuous across an elastic reflection, which flips pi only, so a
    reflection does not open a gap in which a crossing could be missed. The last step's
    interpolant is read only up to the history's end (ts[-1] = N_final).

    ln T_J is not monotonic in principle (d ln T_J/dN carries a factor 1 + (d ln Omega/dphi) pi);
    only the first crossing is returned.

    :param result: the IntegrationResult of integrate_scalar_history
    :param log_T: ln of the target Jordan-frame temperature, in the cosmology's units
    :return: N of the first crossing, or None
    """
    found = _T_Jordan_crossing_step(result, log_T)
    return found[1] if found is not None else None


def fixed_T_value_at(
    result: IntegrationResult,
    k: int,
    N: float,
    policy: ODEPolicy,
    model: RealizedModel,
    units: UnitsLike,
) -> tuple:
    """
    (phi_Einstein, rho_NP/rho_R,J) on the history at e-fold number N, read from the interpolant
    of accepted step k (science-readiness prompt 06b; README §2 (n)).

    H_J, ln rho_R,J and f_m are built from the state exactly as compute_scalar_history's sampling
    loop builds a SampleValues, and the ratio from them exactly as compute_BBN_data builds
    density_NP_ratio: (3 M_P^2 H_J^2 - rho_R,J (1 + f_m)) / rho_R,J. At a stored sample's own N
    it is therefore the ratio BBN sees there.
    """
    state: StateVector = StateVector._make(result.solution.interpolants[k](N))
    data: ODEPolicyData = policy(N, state)
    hubble_data: HubblePolicyData = HubblePolicy(model, units)(data, state)

    log_Omega: float = model.log_Omega(state.phi_Einstein)
    log_rhorad_Jordan: float = state.log_rhorad_Einstein - 4.0 * log_Omega

    CONST_3_MP_SQ = 3.0 * units.PlanckMass * units.PlanckMass
    rhorad_Jordan: float = exp(log_rhorad_Jordan)
    H2_Jordan: float = hubble_data.H_Jordan * hubble_data.H_Jordan
    fm: float = exp(state.log_fm)
    density_NP: float = H2_Jordan * CONST_3_MP_SQ - rhorad_Jordan * (1.0 + fm)

    return float(state.phi_Einstein), float(density_NP / rhorad_Jordan)


def fixed_T_values(
    result: IntegrationResult,
    policy: ODEPolicy,
    model: RealizedModel,
    units: UnitsLike,
) -> FixedTValues:
    """
    phi and rho_NP/rho_R,J at the history's first crossing of T_J = FIXED_T_JORDAN_HIGH_MEV and
    of T_J = FIXED_T_JORDAN_LOW_MEV, on the dense output (science-readiness prompt 06b;
    README §2 (n)). The crossing is T_Jordan_crossing's, and the values fixed_T_value_at's, on
    the step the crossing lies on. A temperature the history does not reach gives None in both
    of its fields.
    """
    values = []
    for T_MeV in (FIXED_T_JORDAN_HIGH_MEV, FIXED_T_JORDAN_LOW_MEV):
        found = _T_Jordan_crossing_step(result, log(T_MeV * units.MeV))
        if found is None:
            values.extend([None, None])
        else:
            k, N = found
            values.extend(fixed_T_value_at(result, k, N, policy, model, units))

    return FixedTValues._make(values)


# ---- the dark radiation and the whole-history values of dissipation mode (inelastic-reflection prompt
# 09) ----
#
# README §8.1 R12, R22-R25, §8.2 P26, P27, P31; orchestrator note 09, rulings 3, 5, 6 and 8. Computed after
# the integration, from the IntegrationResult and the payload; they read the history and never change it

# Delta N_eff = NEFF_PER_G_RHO g_rho r_chi at T_J = 1 MeV, taking T_nu = T_gamma there (R22), and
# NEFF_PER_G_RHO g_rho NEFF_NEUTRINO_HEATING r_chi today, where T_nu/T_gamma = (4/11)^(1/3) (P7): one
# neutrino species carries (7/8) 2 (pi^2/30) T_nu^4, so rho_chi = Delta N_eff (7/4) (pi^2/30) T_nu^4
# against rho_R = g_rho (pi^2/30) T_gamma^4
NEFF_PER_G_RHO = 4.0 / 7.0
NEFF_NEUTRINO_HEATING = (11.0 / 4.0) ** (4.0 / 3.0)

# the dark radiation at the history's first crossing of T_J = FIXED_T_JORDAN_HIGH_MEV (1 MeV) and
# FIXED_T_JORDAN_LOW_MEV (70 keV), and at N_final (today), on the payload's "dark_radiation_values"
# (P31; orchestrator note 09, ruling 3):
#   rho_chi_ratio_1MeV, rho_chi_ratio_70keV, rho_chi_ratio_today:
#       r_chi = rho_chi/rho_R, the policy's at the dense output's state there; None at a temperature
#       the history does not reach
#   Delta_N_eff_1MeV:  NEFF_PER_G_RHO g_rho r_chi at 1 MeV (R22), None if not reached
#   Delta_N_eff_today: NEFF_PER_G_RHO g_rho NEFF_NEUTRINO_HEATING r_chi at N_final (P7)
# g_rho is the cosmology's G_rho at the T_J the policy reads there. At alpha = 0 the ledger is empty, so
# every ratio is exactly 0.0, and each Delta N_eff is too
DarkRadiationValues = namedtuple(
    "DarkRadiationValues",
    [
        "rho_chi_ratio_1MeV",
        "rho_chi_ratio_70keV",
        "rho_chi_ratio_today",
        "Delta_N_eff_1MeV",
        "Delta_N_eff_today",
    ],
)


def dark_radiation_values(
    result: IntegrationResult, policy: ODEPolicy, cosmology: BaseCosmology
) -> DarkRadiationValues:
    """
    r_chi and Delta N_eff at the history's first crossing of T_J = 1 MeV and 70 keV, and at N_final
    (inelastic-reflection prompt 09; README §8.1 R22, §8.2 P31; orchestrator note 09, ruling 3).

    The crossing is fixed_T_values', _T_Jordan_crossing_step on the dense output, so its N is
    fixed_T_values' bitwise; the state there is the step's interpolant's, as fixed_T_value_at reads it.
    Today's state is result.solution(result.N_final), as the sampling loop reads the last sample's.
    r_chi is policy(N, state).rho_chi_over_rho_rad, and g_rho cosmology.G_rho at that call's T_J, as
    the sampling loop takes gstar_rho. Nothing branches on alpha.
    """
    units: UnitsLike = cosmology.units

    def r_and_g(N: float, state: StateVector) -> Tuple[float, float]:
        data: ODEPolicyData = policy(N, state)
        return data.rho_chi_over_rho_rad, cosmology.G_rho(data.T_Jordan)

    ratios = []
    g_rho_1MeV = None
    for T_MeV in (FIXED_T_JORDAN_HIGH_MEV, FIXED_T_JORDAN_LOW_MEV):
        found = _T_Jordan_crossing_step(result, log(T_MeV * units.MeV))
        if found is None:
            ratios.append(None)
            continue
        k, N = found
        r, g = r_and_g(N, StateVector._make(result.solution.interpolants[k](N)))
        ratios.append(r)
        if T_MeV == FIXED_T_JORDAN_HIGH_MEV:
            g_rho_1MeV = g

    r_today, g_today = r_and_g(
        result.N_final, StateVector._make(result.solution(result.N_final))
    )

    r_1MeV = ratios[0]
    return DarkRadiationValues(
        rho_chi_ratio_1MeV=r_1MeV,
        rho_chi_ratio_70keV=ratios[1],
        rho_chi_ratio_today=r_today,
        Delta_N_eff_1MeV=(
            NEFF_PER_G_RHO * g_rho_1MeV * r_1MeV if r_1MeV is not None else None
        ),
        Delta_N_eff_today=NEFF_PER_G_RHO * g_today * NEFF_NEUTRINO_HEATING * r_today,
    )


# the whole-history values of dissipation mode, as ScalarHistory exposes them (ScalarHistory.dissipation)
# and stores them, one column each, in this order (prompt 09 §1.3; orchestrator note 09, rulings 6 and 8).
# Formed by dissipation_values from the payload; None on a failure:
#   injections:            the candidates with delta_K > 0
#   resolved_candidates, unresolved_candidates: every candidate, by Injection.resolved
#   injected_fraction:     the sum, left to right from 0.0, of every candidate's delta_rho_over_rho_rad,
#                          then of the parking leftover's (Parking.delta_rho_over_rho_rad) if parked
#   rho_chi_ratio_1MeV, rho_chi_ratio_70keV, rho_chi_ratio_today, Delta_N_eff_1MeV, Delta_N_eff_today:
#                          DarkRadiationValues'
#   log_R_first_impact:    ln R of the first candidate's ClassicalTurningPoint; None if there is no
#                          candidate or it has no assessment
#   parking_N, parking_log_T_Jordan, release_N, release_log_T_Jordan:
#                          Parking's and Release's N and ln T_J (the cosmology's units); None if the
#                          history did not park, or was not released
#   worst_<kind>_margin, worst_<kind>_margin_N, <kind>_failing_share for birth, reflection and tracker,
#   unassessed_share:      Margins' fields of the same names; None where the payload's margins is None
# Each is None where its payload key is absent (a stand-in payload): zeros are never inferred from an
# absent key (ruling 5). A real payload carries every key, so at alpha = 0 the counts are 0 and the
# fraction, ratios and Delta N_eff 0.0 (R24)
DissipationValues = namedtuple(
    "DissipationValues",
    [
        "injections",
        "resolved_candidates",
        "unresolved_candidates",
        "injected_fraction",
        "rho_chi_ratio_1MeV",
        "rho_chi_ratio_70keV",
        "rho_chi_ratio_today",
        "Delta_N_eff_1MeV",
        "Delta_N_eff_today",
        "log_R_first_impact",
        "parking_N",
        "parking_log_T_Jordan",
        "release_N",
        "release_log_T_Jordan",
        "worst_birth_margin",
        "worst_birth_margin_N",
        "birth_failing_share",
        "worst_reflection_margin",
        "worst_reflection_margin_N",
        "reflection_failing_share",
        "worst_tracker_margin",
        "worst_tracker_margin_N",
        "tracker_failing_share",
        "unassessed_share",
    ],
)

# the fields of DissipationValues read from the payload's Margins record, by the same names
_DISSIPATION_MARGIN_FIELDS = (
    "worst_birth_margin",
    "worst_birth_margin_N",
    "birth_failing_share",
    "worst_reflection_margin",
    "worst_reflection_margin_N",
    "reflection_failing_share",
    "worst_tracker_margin",
    "worst_tracker_margin_N",
    "tracker_failing_share",
    "unassessed_share",
)


def dissipation_values(data: dict) -> DissipationValues:
    """
    The whole-history values of a success payload (DissipationValues; orchestrator note 09, rulings 5
    and 6), each read with .get: a key that is absent gives None in its fields.
    """
    values = dict.fromkeys(DissipationValues._fields)

    injections = data.get("injections")
    if injections is not None:
        values["injections"] = sum(1 for c in injections if c.delta_K > 0.0)
        values["resolved_candidates"] = sum(1 for c in injections if c.resolved)
        values["unresolved_candidates"] = sum(1 for c in injections if not c.resolved)
        if len(injections) > 0 and injections[0].classical_turning_point is not None:
            values["log_R_first_impact"] = injections[0].classical_turning_point.log_R

    if "parking" in data:
        parking: Optional[Parking] = data["parking"]
        if injections is not None:
            injected = 0.0
            for c in injections:
                injected = injected + c.delta_rho_over_rho_rad
            if parking is not None:
                injected = injected + parking.delta_rho_over_rho_rad
            values["injected_fraction"] = injected
        if parking is not None:
            values["parking_N"] = parking.N
            values["parking_log_T_Jordan"] = parking.log_T_Jordan

    release: Optional[Release] = data.get("release")
    if release is not None:
        values["release_N"] = release.N
        values["release_log_T_Jordan"] = release.log_T_Jordan

    dark: Optional[DarkRadiationValues] = data.get("dark_radiation_values")
    if dark is not None:
        values.update(dark._asdict())

    margins = data.get("margins")
    if margins is not None:
        for key in _DISSIPATION_MARGIN_FIELDS:
            values[key] = getattr(margins, key)

    return DissipationValues(**values)


# ---- the spectrum's peak and the relativistic margins (inelastic-reflection prompt 08) ----
#
# README §8.1 R7, R13, R19, R20, R21, §8.2 P25, §8.3 U9; log 08 §1 gives the definitions and the coverage
# bound. Computed by integrate_history after the integration, outside the step loop (README §6.0 R3),
# at alpha > 0 on a model with a wall only, from the IntegrationResult, the samples and the dense output;
# they read the history and never change it. Module-level, so that the core can be tested on constructed
# values and the masses patched through their module names (orchestrator note 08, rulings 4 and 8).
# Every momentum and mass is Einstein-frame (a_E = e^N); p/m is frame-independent. Nothing is stored
# (prompt 09 stores the record)

# the estimates of a candidate's peak (R7, R19): by its ln R, with the one-loop chain's gap flagged
ESTIMATE_CLASSICAL = "classical"
ESTIMATE_ONE_LOOP = "one_loop"
ESTIMATE_CLASSICAL_GAP = "classical_gap"

# a candidate's peak (peak_estimate): the estimate, ln k_ex and ln m of it, the candidate's ln R, and, for
# "classical_gap", why the one-loop estimate was not used (the ComputationFailureError's message, or the
# phrase for a None); None otherwise
PeakEstimate = namedtuple(
    "PeakEstimate", ["estimate", "log_k_ex", "log_m", "log_R", "gap_reason"]
)

# one candidate as the core reads it (relativistic_margins): its index in IntegrationResult.injections, its
# kind, N_turn and delta_K, and its peak. estimate is None for a candidate with no assessment; then log_m
# is the field's own m_eff at its state at N_turn (field_log_m), and log_k_ex, log_R and gap_reason are
# None
MarginCandidate = namedtuple(
    "MarginCandidate",
    [
        "index",
        "kind",
        "N_turn",
        "delta_K",
        "estimate",
        "log_k_ex",
        "log_m",
        "log_R",
        "gap_reason",
    ],
)

# one event of the reflection margin that is not a candidate (orchestrator note 08, ruling 5): a plateau
# turning point ("plateau"), or a wall-side turning point that has no candidate ("wall", a dropped
# drain); log_m is the field's own m_eff at its state on the dense output
MarginEvent = namedtuple("MarginEvent", ["N", "log_m", "source"])

# one generation (prompt 08 §1.1, §1.2): a candidate with delta_K > 0 and an assessment
#   index:                the candidate's index in IntegrationResult.injections
#   kind:                 the candidate's kind ("turning_point", "floor" or "unresolved")
#   estimate:             ESTIMATE_CLASSICAL, ESTIMATE_ONE_LOOP or ESTIMATE_CLASSICAL_GAP
#   N_turn:               its birth
#   log_q:                ln q = ln k_ex + N_turn, the comoving peak momentum
#   log_k_ex, log_m:      the estimate's ln k_ex and ln m (ln m = ln omega_t, or ln m_ta)
#   log_R:                the candidate's ln R
#   share:                delta_K/K(N_final)
#   weighted:             share >= MARGIN_WEIGHT_SHARE (U9)
#   birth_margin:         ln k_ex - ln m, at N_turn (ell_t or ell, bitwise)
#   reflection_margin:    the smallest ln q - N_k - ln m_k over the events with N_k > N_turn, or None if
#                         there is none; reflection_margin_N its N_k
#   tracker_margin:       the smallest ln q - N - ln m_tr(N) over the late samples (N > N_late) and today;
#                         tracker_margin_N its N
#   gap_reason:           the PeakEstimate's
Generation = namedtuple(
    "Generation",
    [
        "index",
        "kind",
        "estimate",
        "N_turn",
        "log_q",
        "log_k_ex",
        "log_m",
        "log_R",
        "share",
        "weighted",
        "birth_margin",
        "reflection_margin",
        "reflection_margin_N",
        "tracker_margin",
        "tracker_margin_N",
        "gap_reason",
    ],
)

# the first failure of one kind (R13: "we can store details of the first encountered failure in the JSON
# blob"): the earliest N at which a weighted generation's margin of that kind falls below
# MARGIN_THRESHOLD (birth: its N_turn; reflection: the first event N_k; tracker: the first late sample, or
# today); the generation's index, that N, the margin there, the generation's ln q, the ln m there, and
# the generation's estimate. Prompt 09 stores it as JSON (_asdict)
MarginFailure = namedtuple(
    "MarginFailure", ["index", "N", "margin", "log_q", "log_m", "estimate"]
)

# the history's margins (prompt 08 §1.3), returned by history_margins with the generations, on the
# payload's "margins" (not stored; prompt 09 stores the worst margins, their N and the failing shares as
# columns, and the first failures as JSON). Floats, ints, strings, None and MarginFailure: nothing
# callable, so that it crosses Ray. For each kind (birth, reflection, tracker):
#   worst_<kind>_margin, worst_<kind>_margin_N, worst_<kind>_margin_index:
#                          the smallest margin over the weighted generations, its N and its generation's
#                          index (the lowest index on a tie); None if no weighted generation has one
#   <kind>_failing_share:  the sum of the shares of the weighted generations whose margin of that kind is
#                          below MARGIN_THRESHOLD (a margin of None is not failing)
#   first_<kind>_failure:  MarginFailure, or None
# and
#   weighted_share, below_weight_share, unassessed_share:
#                          the shares of K(N_final) in the weighted generations, in the generations below
#                          the weight, and unassessed: the candidates with delta_K > 0 and no
#                          assessment, the parking leftover (R21) and the initial K_0 (a test harness;
#                          orchestrator note 08, ruling 9). They partition K
#   classical_generations, one_loop_generations, classical_gap_generations:
#                          the generations per estimate, weighted or not
#   weighted_generations:  the generations with share >= MARGIN_WEIGHT_SHARE
#   unassessed_candidates: the candidates with delta_K > 0 and no assessment
#   plateau_events, wall_events:
#                          the events of the reflection margin that are not candidates: plateau turning
#                          points, and wall-side turning points without a candidate
#   N_late:                the largest N among the turning points and the candidates' N_turn (R20); -inf
#                          if there are none
#   late_points:           the points of the tracker margin: the samples with N > N_late, and today
Margins = namedtuple(
    "Margins",
    [
        "worst_birth_margin",
        "worst_birth_margin_N",
        "worst_birth_margin_index",
        "birth_failing_share",
        "first_birth_failure",
        "worst_reflection_margin",
        "worst_reflection_margin_N",
        "worst_reflection_margin_index",
        "reflection_failing_share",
        "first_reflection_failure",
        "worst_tracker_margin",
        "worst_tracker_margin_N",
        "worst_tracker_margin_index",
        "tracker_failing_share",
        "first_tracker_failure",
        "weighted_share",
        "below_weight_share",
        "unassessed_share",
        "classical_generations",
        "one_loop_generations",
        "classical_gap_generations",
        "weighted_generations",
        "unassessed_candidates",
        "plateau_events",
        "wall_events",
        "N_late",
        "late_points",
    ],
)

# the three kinds, in the order of the record and of the warning lines
MARGIN_KINDS = ("birth", "reflection", "tracker")


def peak_estimate(model: RealizedModel, injection: Injection) -> Optional[PeakEstimate]:
    """
    The peak of a candidate's spectrum, by its ln R (prompt 08 §1.1; README §8.1 R7, R19, §8.2 P25). None
    if the candidate has no assessment (classical_turning_point None).

    - ln R <= 0: the classical estimate, ln k_ex and ln m = ln omega_t = ln omega_t^2/2 of the
      candidate's ClassicalTurningPoint.
    - ln R > 0: the model's one_loop_turnaround at (phi_assessed, ln E_assessed): its ln k_ex and ln m_ta.
      If it raises ComputationFailureError (the chain has no solution, the gap of 1 < R < 10^2.9) or
      returns None, the classical estimate, flagged ESTIMATE_CLASSICAL_GAP with the reason ("Classical,
      flagged", R19). Any other exception propagates.
    """
    ct: Optional[ClassicalTurningPoint] = injection.classical_turning_point
    if ct is None:
        return None
    return _assessed_peak(model, ct, injection.phi_assessed, injection.log_E_assessed)[
        0
    ]


def _assessed_peak(
    model: RealizedModel,
    ct: ClassicalTurningPoint,
    phi_assessed: float,
    log_E_assessed: float,
) -> Tuple[PeakEstimate, Optional[OneLoopTurnaround]]:
    """
    peak_estimate's body for an assessed candidate (its expressions moved here unchanged by one-loop-drain
    prompt 01, so that the one-loop law's drain_regime computes the same PeakEstimate, once; P18):
    (the PeakEstimate, the OneLoopTurnaround where one_loop_turnaround returned one, else None).
    """
    log_m_classical: float = 0.5 * ct.log_omega_t_squared
    if ct.log_R <= 0.0:
        return (
            PeakEstimate(
                ESTIMATE_CLASSICAL, ct.log_k_ex, log_m_classical, ct.log_R, None
            ),
            None,
        )
    try:
        turnaround = model.one_loop_turnaround(phi_assessed, log_E_assessed)
    except ComputationFailureError as e:
        return (
            PeakEstimate(
                ESTIMATE_CLASSICAL_GAP,
                ct.log_k_ex,
                log_m_classical,
                ct.log_R,
                e.message,
            ),
            None,
        )
    if turnaround is None:
        return (
            PeakEstimate(
                ESTIMATE_CLASSICAL_GAP,
                ct.log_k_ex,
                log_m_classical,
                ct.log_R,
                "one_loop_turnaround returned None",
            ),
            None,
        )
    return (
        PeakEstimate(
            ESTIMATE_ONE_LOOP, turnaround.log_k_ex, turnaround.log_m_ta, ct.log_R, None
        ),
        turnaround,
    )


def tracker_log_m(
    model: RealizedModel,
    phi_start: float,
    log_rhorad_Einstein: float,
    log_fm: float,
    Sigma: float,
) -> float:
    """
    ln m_tr, the zero-lag tracker's m_eff at one state (prompt 08 §1.2; README §8.1 R20): phi_min the
    model's minimum_on_trace on the trace T = rho_R,E (Sigma + f_m), searched from phi_start, and
    ln m_eff^2/2 (log_m_eff_squared) at phi_min on the same state. history_margins passes the state's
    own field as phi_start (orchestrator note 08, ruling 3), so that the value depends on the state alone.
    A ComputationFailureError from the model propagates.
    """
    try:
        fm: float = exp(log_fm)
    except OverflowError as e:
        raise ComputationFailureError(
            f"tracker_log_m: overflow at ln f_m = {log_fm!r}"
        ) from e
    log_T: float = _log_trace(log_rhorad_Einstein, Sigma, fm, "tracker_log_m")
    phi_min: float = model.minimum_on_trace(phi_start, log_T)
    return 0.5 * log_m_eff_squared(model, phi_min, log_rhorad_Einstein, log_fm, Sigma)


def field_log_m(model: RealizedModel, cosmology: BaseCosmology, state) -> float:
    """
    ln m_eff of the field itself at a full state (phi, pi, ln rho_R,E, ln f_m, ln T_J) of the dense output
    (prompt 08 §1.2): ln m_eff^2/2 (log_m_eff_squared) at its own phi, with Sigma from log_trace, as the
    policy forms it. The mass of a plateau turning point, of a wall-side turning point with no candidate,
    and of a candidate with no assessment.
    """
    _, Sigma, _ = log_trace(cosmology, (state[2], state[3], state[4]))
    return 0.5 * log_m_eff_squared(
        model, float(state[0]), float(state[2]), float(state[3]), Sigma
    )


def _earliest_failure(failures: List[MarginFailure]) -> Optional[MarginFailure]:
    """The earliest N; on a tie the smallest margin there, then the lowest index (orchestrator note 08, ruling 8)."""
    if len(failures) == 0:
        return None
    return min(failures, key=lambda f: (f.N, f.margin, f.index))


def relativistic_margins(
    candidates,
    events,
    N_late: float,
    samples,
    today: Tuple[float, float],
    K_final: float,
    K_unassessed: float,
) -> Tuple[Tuple[Generation, ...], Margins]:
    """
    The pure core of the margins (prompt 08 §1.2, §1.3; orchestrator note 08, rulings 5, 8, 9 and 10): plain
    values in, the generations and the record out. No model, cosmology or dense output is read.

    :param candidates: every candidate, in the order of IntegrationResult.injections, as MarginCandidate
    :param events: the events of the reflection margin that are not candidates, as MarginEvent
    :param N_late: the late tracker's start (R20)
    :param samples: (N, ln m_tr) of the stored samples, in N order; those with N > N_late are the tracker
                    margin's points
    :param today: (N_final, ln m_tr) at the final state, always a point of the tracker margin
    :param K_final: K(N_final), the ledger's, the parking leftover and K_0 included
    :param K_unassessed: the parking leftover's delta_K plus K_0

    A generation is a candidate with delta_K > 0 and an estimate; its share is delta_K/K_final, and it is
    weighted if share >= MARGIN_WEIGHT_SHARE. Its margins:
      - birth: ln k_ex - ln m;
      - reflection: the minimum of ln q - N_k - ln m_k over every candidate (delta_K = 0 included, at its
        N_turn with its log_m) and every event with N_k > N_turn; None if there is none;
      - tracker: the minimum of ln q - N - ln m_tr over the samples with N > N_late, and today.
    Minima are taken in N order with a strict comparison, so a tie keeps the earliest; the worst margin
    of a kind is taken over the weighted generations in index order, so a tie keeps the lowest index.
    """
    threshold: float = MARGIN_THRESHOLD
    weight: float = MARGIN_WEIGHT_SHARE

    # the events of the reflection margin, in N order (a stable sort: a candidate before an event at the
    # same N)
    all_events = sorted(
        [(float(c.N_turn), float(c.log_m)) for c in candidates]
        + [(float(e.N), float(e.log_m)) for e in events],
        key=lambda e: e[0],
    )
    late = [(float(N), float(lm)) for N, lm in samples if N > N_late]
    late.append((float(today[0]), float(today[1])))

    generations: List[Generation] = []
    failures: Dict[str, List[MarginFailure]] = {kind: [] for kind in MARGIN_KINDS}
    # a history that drained nothing has K_final = 0 and no generation: every share is then 0.0 (a
    # candidate with delta_K > 0 implies K_final > 0)
    unassessed_share: float = K_unassessed / K_final if K_final > 0.0 else 0.0
    unassessed_candidates = 0
    counts = {
        ESTIMATE_CLASSICAL: 0,
        ESTIMATE_ONE_LOOP: 0,
        ESTIMATE_CLASSICAL_GAP: 0,
    }

    def minimum(log_q: float, points, after: float):
        """(the smallest ln q - N - ln m, its N, the first point below the threshold or None)."""
        smallest, smallest_N, first = None, None, None
        for N, lm in points:
            if not N > after:
                continue
            value: float = log_q - N - lm
            if smallest is None or value < smallest:
                smallest, smallest_N = value, N
            if first is None and value < threshold:
                first = (N, value, lm)
        return smallest, smallest_N, first

    for c in candidates:
        if not c.delta_K > 0.0:
            continue
        share: float = c.delta_K / K_final
        if c.estimate is None:
            unassessed_share += share
            unassessed_candidates += 1
            continue
        counts[c.estimate] += 1
        log_q: float = c.log_k_ex + c.N_turn
        birth: float = c.log_k_ex - c.log_m
        reflection, reflection_N, reflection_first = minimum(
            log_q, all_events, c.N_turn
        )
        tracker, tracker_N, tracker_first = minimum(log_q, late, float("-inf"))
        weighted: bool = share >= weight
        generations.append(
            Generation(
                index=c.index,
                kind=c.kind,
                estimate=c.estimate,
                N_turn=c.N_turn,
                log_q=log_q,
                log_k_ex=c.log_k_ex,
                log_m=c.log_m,
                log_R=c.log_R,
                share=share,
                weighted=weighted,
                birth_margin=birth,
                reflection_margin=reflection,
                reflection_margin_N=reflection_N,
                tracker_margin=tracker,
                tracker_margin_N=tracker_N,
                gap_reason=c.gap_reason,
            )
        )
        if not weighted:
            continue
        if birth < threshold:
            failures["birth"].append(
                MarginFailure(c.index, c.N_turn, birth, log_q, c.log_m, c.estimate)
            )
        for kind, first in (
            ("reflection", reflection_first),
            ("tracker", tracker_first),
        ):
            if first is not None:
                failures[kind].append(
                    MarginFailure(
                        c.index, first[0], first[1], log_q, first[2], c.estimate
                    )
                )

    weighted_generations = [g for g in generations if g.weighted]
    values: Dict[str, Any] = {}
    for kind in MARGIN_KINDS:
        worst: Optional[Generation] = None
        failing_share: float = 0.0
        for g in weighted_generations:
            margin = getattr(g, f"{kind}_margin")
            if margin is None:
                continue
            if worst is None or margin < getattr(worst, f"{kind}_margin"):
                worst = g
            if margin < threshold:
                failing_share += g.share
        if worst is None:
            values[f"worst_{kind}_margin"] = None
            values[f"worst_{kind}_margin_N"] = None
            values[f"worst_{kind}_margin_index"] = None
        else:
            values[f"worst_{kind}_margin"] = getattr(worst, f"{kind}_margin")
            values[f"worst_{kind}_margin_N"] = (
                worst.N_turn if kind == "birth" else getattr(worst, f"{kind}_margin_N")
            )
            values[f"worst_{kind}_margin_index"] = worst.index
        values[f"{kind}_failing_share"] = failing_share
        values[f"first_{kind}_failure"] = _earliest_failure(failures[kind])

    weighted_share: float = 0.0
    below_weight_share: float = 0.0
    for g in generations:
        if g.weighted:
            weighted_share += g.share
        else:
            below_weight_share += g.share

    margins = Margins(
        weighted_share=weighted_share,
        below_weight_share=below_weight_share,
        unassessed_share=unassessed_share,
        classical_generations=counts[ESTIMATE_CLASSICAL],
        one_loop_generations=counts[ESTIMATE_ONE_LOOP],
        classical_gap_generations=counts[ESTIMATE_CLASSICAL_GAP],
        weighted_generations=len(weighted_generations),
        unassessed_candidates=unassessed_candidates,
        plateau_events=sum(1 for e in events if e.source == "plateau"),
        wall_events=sum(1 for e in events if e.source == "wall"),
        N_late=float(N_late),
        late_points=len(late),
        **values,
    )
    return tuple(generations), margins


# the core's inputs, as the gatherer returns them (one-loop-drain prompt 03; orchestrator note 03, ruling 4):
# relativistic_margins' arguments in its order, so that relativistic_margins(*inputs) is history_margins' result.
# integrate_history keeps them for the bounce rows (bounce_record)
#   candidates:    every candidate, as MarginCandidate, in the order of IntegrationResult.injections
#   events:        the events of the reflection margin that are not candidates, as MarginEvent, in the order of
#                  IntegrationResult.turning_points
#   N_late:        the late tracker's start (R20)
#   samples:       (N, ln m_tr) of the stored samples with N > N_late, in N order
#   today:         (N_final, ln m_tr) at the final state
#   K_final:       K(N_final), the policy's ledger's
#   K_unassessed:  the parking leftover's delta_K plus K_0
MarginInputs = namedtuple(
    "MarginInputs",
    ["candidates", "events", "N_late", "samples", "today", "K_final", "K_unassessed"],
)


def history_margins(
    result: IntegrationResult, sample, policy: ODEPolicy
) -> Tuple[Tuple[Generation, ...], Margins]:
    """
    The margins of a history (prompt 08 §1.4): the core (relativistic_margins) on the gatherer's values
    (margin_inputs). Since one-loop-drain prompt 03 the gathering is margin_inputs, its body moved there
    unchanged (orchestrator note 03, ruling 4), so that integrate_history can keep the core's inputs; this
    returns what it returned before, bitwise.

    A ComputationFailureError (a model call, the tracker's minimum) propagates to the caller, which
    reports it as a warning (§1.4).
    """
    return relativistic_margins(*margin_inputs(result, sample, policy))


def margin_inputs(result: IntegrationResult, sample, policy: ODEPolicy) -> MarginInputs:
    """
    The gatherer (prompt 08 §1.4; one-loop-drain prompt 03, ruling 4): the core's values from a history.

    - Each candidate's peak, once (peak_estimate), used both for its generation and as an event
      (orchestrator note 08, ruling 5); a candidate with no assessment takes the field's own mass at its
      state at N_turn on the dense output (field_log_m).
    - The events that are not candidates: every plateau turning point, and every wall-side turning point
      whose N is no "turning_point" candidate's N_turn, with the field's own mass on the dense output.
    - N_late, the largest N among the turning points and the candidates' N_turn (R20).
    - The tracker's mass (tracker_log_m) at every stored sample with N > N_late, searched from the
      sample's own field, with the trace from its stored ln rho_R,E, ln f_m and Sigma; and today's, at the
      final state, with Sigma from log_trace (ruling 3).
    - K(N_final) from the policy's ledger; the unassessed K is the parking leftover's delta_K plus K_0.

    A ComputationFailureError (a model call, the tracker's minimum) propagates to the caller, which
    reports it as a warning (§1.4).
    """
    model: RealizedModel = policy.model
    cosmology = policy.cosmology
    ledger: DarkRadiationLedger = policy.dark_radiation
    solution = result.solution

    # under the one-loop law the loop's PeakEstimates are reused (one-loop-drain prompt 01, P18;
    # orchestrator note 01, ruling 5): drain_regime computed each as peak_estimate would. Under the fixed
    # law (peak_estimates None) peak_estimate is called as before
    reused: Optional[Tuple[Optional[PeakEstimate], ...]] = result.peak_estimates
    candidates: List[MarginCandidate] = []
    for index, injection in enumerate(result.injections):
        if reused is None:
            estimate = peak_estimate(model, injection)
        else:
            estimate = reused[index]
        if estimate is None:
            candidates.append(
                MarginCandidate(
                    index=index,
                    kind=injection.kind,
                    N_turn=injection.N_turn,
                    delta_K=injection.delta_K,
                    estimate=None,
                    log_k_ex=None,
                    log_m=field_log_m(model, cosmology, solution(injection.N_turn)),
                    log_R=None,
                    gap_reason=None,
                )
            )
        else:
            candidates.append(
                MarginCandidate(
                    index=index,
                    kind=injection.kind,
                    N_turn=injection.N_turn,
                    delta_K=injection.delta_K,
                    estimate=estimate.estimate,
                    log_k_ex=estimate.log_k_ex,
                    log_m=estimate.log_m,
                    log_R=estimate.log_R,
                    gap_reason=estimate.gap_reason,
                )
            )

    drained_turns = {
        injection.N_turn
        for injection in result.injections
        if injection.kind == "turning_point"
    }
    events: List[MarginEvent] = []
    for tp in result.turning_points:
        if tp.kind == "plateau":
            source = "plateau"
        elif tp.N not in drained_turns:
            source = "wall"
        else:
            continue
        events.append(
            MarginEvent(tp.N, field_log_m(model, cosmology, solution(tp.N)), source)
        )

    Ns = [tp.N for tp in result.turning_points] + [
        injection.N_turn for injection in result.injections
    ]
    N_late: float = max(Ns) if len(Ns) > 0 else float("-inf")

    late_samples = []
    for s in sorted(sample, key=lambda s: s.raw_N):
        if s.raw_N > N_late:
            late_samples.append(
                (
                    s.raw_N,
                    tracker_log_m(
                        model, s.phi_Einstein, s.log_rhorad_Einstein, s.log_fm, s.Sigma
                    ),
                )
            )

    final: StateVector = result.final_state
    _, Sigma_final, _ = log_trace(
        cosmology, (final.log_rhorad_Einstein, final.log_fm, final.log_T_Jordan)
    )
    today = (
        result.N_final,
        tracker_log_m(
            model,
            final.phi_Einstein,
            final.log_rhorad_Einstein,
            final.log_fm,
            Sigma_final,
        ),
    )

    K_unassessed: float = ledger.K_initial
    if result.parking is not None:
        K_unassessed += result.parking.delta_K

    return MarginInputs(
        candidates,
        events,
        N_late,
        late_samples,
        today,
        ledger.K(result.N_final),
        K_unassessed,
    )


def margin_warning_lines(margins: Margins) -> Tuple[str, ...]:
    """
    The margins' warning lines (prompt 08 §1.5; orchestrator note 08, ruling 1): one per kind whose worst
    margin is below MARGIN_THRESHOLD, in the order birth, reflection, tracker, each beginning with its
    phrase of WARNING_KINDS and giving the worst margin, its N, its generation and the failing share. They
    go on the payload's "margin_warnings", never on "warnings".
    """
    phrases = {
        "birth": WARNING_BIRTH_MARGIN,
        "reflection": WARNING_REFLECTION_MARGIN,
        "tracker": WARNING_TRACKER_MARGIN,
    }
    lines = []
    for kind in MARGIN_KINDS:
        worst = getattr(margins, f"worst_{kind}_margin")
        if worst is None or not worst < MARGIN_THRESHOLD:
            continue
        lines.append(
            f"{phrases[kind]}: the worst margin ln(p/m) = {worst:.4f} < ln 10 at "
            f"N = {getattr(margins, f'worst_{kind}_margin_N'):.10g} (generation "
            f"{getattr(margins, f'worst_{kind}_margin_index')}); the generations below ln 10 "
            f"carry {getattr(margins, f'{kind}_failing_share'):.4g} of K"
        )
    return tuple(lines)


# ---- the store's instrumentation (one-loop-drain prompt 03; README §8.3, §8.11 P29-P31) ----
#
# Model-free, computed from records the integration already holds, outside the step loop: the sample masses at
# every law (U5, P31), and, under the one-loop law only, a row per bounce (the payload's "bounces") and the
# history's drain record (DrainHistoryValues, formed by ScalarHistory.store from the payload). None of them moves
# a value of the integration


def sample_masses(
    model: RealizedModel, cosmology: BaseCosmology, sample
) -> Tuple[Tuple[Optional[float], ...], Tuple[Optional[float], ...]]:
    """
    The sample masses (one-loop-drain prompt 03 §1.5; README §8.3 U5, P31; orchestrator note 03, ruling 2):
    (ln m_eff, ln m_tr) at each sample, two tuples parallel to sample, in the cosmology's units.
      - ln m_eff: the field's own mass, field_log_m on the sample's stored state (phi, pi, ln rho_R,E, ln f_m,
        ln T_J), Sigma from log_trace as the policy forms it;
      - ln m_tr: the zero-lag tracker's, tracker_log_m from the sample's stored phi, ln rho_R,E, ln f_m and
        Sigma, searched from the sample's own field, as history_margins calls it.
    Each is None where its call raises ComputationFailureError (m_eff^2 <= 0, no tracker minimum); any other
    exception propagates. Each value is a Python float (float() of the model's result, exact: a realization
    whose parameters are numpy floats returns numpy floats), so that a payload's repr does not depend on how the
    model was realized. The model is the realized model of the integration that made the samples.
    integrate_history calls it by default; tune_history once, on the converged payload.
    """
    field: List[Optional[float]] = []
    tracker: List[Optional[float]] = []
    for s in sample:
        try:
            field.append(
                float(
                    field_log_m(
                        model,
                        cosmology,
                        (
                            s.phi_Einstein,
                            s.pi_Einstein,
                            s.log_rhorad_Einstein,
                            s.log_fm,
                            s.log_T_Jordan,
                        ),
                    )
                )
            )
        except ComputationFailureError:
            field.append(None)
        try:
            tracker.append(
                float(
                    tracker_log_m(
                        model, s.phi_Einstein, s.log_rhorad_Einstein, s.log_fm, s.Sigma
                    )
                )
            )
        except ComputationFailureError:
            tracker.append(None)
    return tuple(field), tuple(tracker)


# the sources of a bounce row (prompt 03 §1.3): a candidate (an Injection), or an event of the reflection margin
# that is not one (MarginEvent.source: a plateau turning point, or a wall-side turning point with no candidate)
BOUNCE_SOURCE_CANDIDATE = "candidate"
BOUNCE_SOURCE_PLATEAU = "plateau"
BOUNCE_SOURCE_WALL = "wall"
BOUNCE_SOURCES = (BOUNCE_SOURCE_CANDIDATE, BOUNCE_SOURCE_PLATEAU, BOUNCE_SOURCE_WALL)

# one bounce row (one-loop-drain prompt 03 §1.3; README §8.3, P7 as revised, P29), a row of ScalarHistoryBounce,
# under the one-loop law only; in the cosmology's units (the store converts each to the unit of its column's
# suffix). Plain values only, so that it crosses Ray. None where a field does not apply
#   ordinal:             0, 1, ... in relativistic_margins' event order: by N (a candidate at its N_turn, an event
#                        at its N), stable, a candidate before an event at the same N
#   source:              BOUNCE_SOURCE_CANDIDATE, _PLATEAU or _WALL
#   candidate_index:     the index in IntegrationResult.injections; None for an event
#   N:                   a candidate's drain N (Injection.N), or an event's N
#   N_turn, kind, resolved:
#                        the Injection's; None for an event
#   log_T_Jordan:        ln T_J at N_turn, or at the event: the TurningPoint's own log_T_Jordan for a
#                        "turning_point" candidate and for an event, the dense output's (solution(N_turn)[4]) for
#                        a reflection candidate (orchestrator note 03, ruling 7)
#   delta_K, log_rho_kin, nonadiabaticity, alpha_eff, w_gate:
#                        the Injection's; None for an event
#   K_before:            the ledger's K in force just before the candidate: its cumulative K after the entries
#                        made before it, read by position (ledger_K_before; ruling 6); None for an event
#   regime, ell, L, log_R, log_E, log_H:
#                        the DrainCandidate's; None for an event. log_R is PeakEstimate.log_R too, bitwise (P18)
#   log_k_ex, log_m:     the PeakEstimate's; for an unassessed candidate log_k_ex is None and log_m the field's own
#                        mass that the margins' gatherer used (MarginCandidate.log_m), and for an event the
#                        event's (MarginEvent.log_m); None where the margins were not computed
#   log_q, share, weighted, birth_margin, reflection_margin, reflection_margin_N, tracker_margin,
#   tracker_margin_N:    the Generation's, where the candidate is one (delta_K > 0 and assessed) and the margins
#                        were computed; None otherwise
BounceRow = namedtuple(
    "BounceRow",
    [
        "ordinal",
        "source",
        "candidate_index",
        "N",
        "N_turn",
        "kind",
        "resolved",
        "log_T_Jordan",
        "delta_K",
        "log_rho_kin",
        "nonadiabaticity",
        "alpha_eff",
        "w_gate",
        "K_before",
        "regime",
        "ell",
        "L",
        "log_R",
        "log_E",
        "log_H",
        "log_k_ex",
        "log_m",
        "log_q",
        "share",
        "weighted",
        "birth_margin",
        "reflection_margin",
        "reflection_margin_N",
        "tracker_margin",
        "tracker_margin_N",
    ],
)

# the payload's "bounces" (one-loop-drain prompt 03 §1.3; orchestrator note 03, ruling 4): None under the fixed
# law and at alpha = 0, absent from a failure's payload
#   rows:     a tuple of BounceRow, in ordinal order
#   K_final:  K(N_final), the ledger's, the parking leftover and K_0 included: the denominator of the shares
#             (Generation.share's, and DrainHistoryValues')
BounceRecord = namedtuple("BounceRecord", ["rows", "K_final"])


def ledger_K_before(
    injections, parking: Optional[Parking], ledger: DarkRadiationLedger
) -> Tuple[float, ...]:
    """
    K just before each candidate (orchestrator note 03, ruling 6), parallel to injections: the ledger's
    cumulative K after the entries made before the candidate, read by position. The ledger holds one entry per
    candidate with alpha_eff > 0 (the drain's add, made iff alpha_eff > 0) and the parking leftover, added at the
    cut candidate's own N after its drain, so the walk goes over the candidates in index order and takes an entry
    for each that drained and the leftover after the cut candidate. It is never read by K(N) at a candidate's N
    (right-continuous, so it would include the candidate's own entry) and never re-summed. Each entry's N and
    delta_K are checked against the candidate's, and every entry must be consumed; a mismatch is a RuntimeError.
    """
    entries = ledger.injections
    cumulative = ledger.cumulative_K
    K: float = ledger.K_initial
    j = 0
    out: List[float] = []

    def take(N: float, delta_K: float, what: str) -> float:
        nonlocal j
        if j >= len(entries) or entries[j] != (N, delta_K):
            raise RuntimeError(
                f"ledger_K_before: {what} at N = {N!r} with delta_K = {delta_K!r} does not match the "
                f"ledger's entry {j} ({entries[j] if j < len(entries) else 'none'})"
            )
        value = cumulative[j]
        j += 1
        return value

    for index, injection in enumerate(injections):
        out.append(K)
        if injection.alpha_eff > 0.0:
            K = take(injection.N, injection.delta_K, f"candidate {index}")
        if parking is not None and parking.candidate_index == index:
            K = take(parking.N, parking.delta_K, "the parking leftover")
    if j != len(entries):
        raise RuntimeError(
            f"ledger_K_before: {len(entries) - j} of the ledger's {len(entries)} entries are not candidates' "
            f"or the parking leftover's"
        )
    return tuple(out)


def bounce_rows(
    injections,
    drain_candidates,
    estimates,
    candidate_log_T,
    K_before,
    margin_candidates,
    events,
    event_log_T,
    generations,
) -> Tuple[BounceRow, ...]:
    """
    The bounce rows (one-loop-drain prompt 03 §1.3; orchestrator note 03, rulings 4, 5 and 7): pure, on plain
    values, so that it is tested on constructed records. No model, cosmology, dense output or ledger is read:
    every value from them is computed by the caller (bounce_record) and handed in.

    :param injections: the candidates, as Injection, in order
    :param drain_candidates: the DrainRecord's candidates, parallel to injections
    :param estimates: the PeakEstimate (or None, unassessed) of each candidate, parallel (the loop's, reused)
    :param candidate_log_T: ln T_J of each candidate's row, parallel (ruling 7)
    :param K_before: the ledger's K just before each candidate, parallel (ledger_K_before)
    :param margin_candidates: the margins' gatherer's MarginCandidates, parallel, or None where the margins were
                              not computed
    :param events: the gatherer's MarginEvents, or None where the margins were not computed (then no event row
                   is written; ruling 5)
    :param event_log_T: ln T_J of each event, its TurningPoint's, parallel to events (None with events)
    :param generations: the core's Generations, or None where the margins were not computed

    One row per candidate (delta_K = 0 included) and one per event; ordered by N (a candidate at its N_turn) by
    a stable sort of the candidates in index order followed by the events in their order, so that a candidate
    comes before an event at the same N, as relativistic_margins orders them; ordinals 0, 1, ...
    """
    if not (
        len(injections)
        == len(drain_candidates)
        == len(estimates)
        == len(candidate_log_T)
        == len(K_before)
    ):
        raise ValueError(
            "bounce_rows: injections, drain_candidates, estimates, candidate_log_T and K_before must be "
            "parallel"
        )
    by_index = {} if generations is None else {g.index: g for g in generations}

    unordered = []
    for index, injection in enumerate(injections):
        c = drain_candidates[index]
        estimate = estimates[index]
        if estimate is not None:
            log_k_ex, log_m = estimate.log_k_ex, estimate.log_m
        elif margin_candidates is not None:
            log_k_ex, log_m = None, margin_candidates[index].log_m
        else:
            log_k_ex, log_m = None, None
        g: Optional[Generation] = by_index.get(index)
        unordered.append(
            (
                injection.N_turn,
                dict(
                    source=BOUNCE_SOURCE_CANDIDATE,
                    candidate_index=index,
                    N=injection.N,
                    N_turn=injection.N_turn,
                    kind=injection.kind,
                    resolved=injection.resolved,
                    log_T_Jordan=candidate_log_T[index],
                    delta_K=injection.delta_K,
                    log_rho_kin=injection.log_rho_kin,
                    nonadiabaticity=injection.nonadiabaticity,
                    alpha_eff=injection.alpha_eff,
                    w_gate=injection.w_gate,
                    K_before=K_before[index],
                    regime=c.regime,
                    ell=c.ell,
                    L=c.L,
                    log_R=c.log_R,
                    log_E=c.log_E,
                    log_H=c.log_H,
                    log_k_ex=log_k_ex,
                    log_m=log_m,
                    log_q=None if g is None else g.log_q,
                    share=None if g is None else g.share,
                    weighted=None if g is None else g.weighted,
                    birth_margin=None if g is None else g.birth_margin,
                    reflection_margin=None if g is None else g.reflection_margin,
                    reflection_margin_N=None if g is None else g.reflection_margin_N,
                    tracker_margin=None if g is None else g.tracker_margin,
                    tracker_margin_N=None if g is None else g.tracker_margin_N,
                ),
            )
        )

    if events is not None:
        if event_log_T is None or len(event_log_T) != len(events):
            raise ValueError("bounce_rows: event_log_T must be parallel to events")
        for event, log_T in zip(events, event_log_T):
            row = dict.fromkeys(BounceRow._fields)
            row.update(
                source=event.source, N=event.N, log_T_Jordan=log_T, log_m=event.log_m
            )
            del row["ordinal"]
            unordered.append((event.N, row))

    ordered = sorted(unordered, key=lambda item: float(item[0]))
    return tuple(
        BounceRow(ordinal=ordinal, **row) for ordinal, (_, row) in enumerate(ordered)
    )


def bounce_record(
    result: IntegrationResult,
    inputs: Optional[MarginInputs],
    generations: Optional[Tuple[Generation, ...]],
    ledger: DarkRadiationLedger,
) -> BounceRecord:
    """
    The payload's "bounces" under the one-loop law (one-loop-drain prompt 03 §1.3; orchestrator note 03, rulings
    4-7): the values the builder needs from the dense output and the ledger, then bounce_rows on them.
      - ln T_J of each candidate (ruling 7): a "turning_point" candidate's wall-side TurningPoint, matched by
        exact N equality as the margins' gatherer matches it; a reflection's solution(N_turn)[4];
      - ln T_J of each event: its TurningPoint's, the turning points the gatherer kept as events, in the
        gatherer's order (checked by N against the events);
      - K just before each candidate (ledger_K_before);
      - inputs and generations are the margins' (None where they were not computed, ruling 5).
    No model call is made. A turning point that cannot be matched is a RuntimeError, a bug.
    """
    injections = result.injections
    drain: DrainRecord = result.drain

    wall_log_T = {
        tp.N: tp.log_T_Jordan for tp in result.turning_points if tp.kind == "wall"
    }
    candidate_log_T: List[float] = []
    for index, injection in enumerate(injections):
        if injection.kind == "turning_point":
            if injection.N_turn not in wall_log_T:
                raise RuntimeError(
                    f"bounce_record: candidate {index} (turning_point) at N_turn = {injection.N_turn!r} has "
                    f"no wall-side turning point"
                )
            candidate_log_T.append(wall_log_T[injection.N_turn])
        else:
            candidate_log_T.append(float(result.solution(injection.N_turn)[4]))

    events = None
    event_log_T = None
    margin_candidates = None
    if inputs is not None:
        margin_candidates = inputs.candidates
        events = inputs.events
        drained_turns = {
            injection.N_turn
            for injection in injections
            if injection.kind == "turning_point"
        }
        event_tps = [
            tp
            for tp in result.turning_points
            if tp.kind == "plateau" or tp.N not in drained_turns
        ]
        if [tp.N for tp in event_tps] != [e.N for e in events]:
            raise RuntimeError(
                "bounce_record: the turning points kept as events do not match the margins' events"
            )
        event_log_T = [tp.log_T_Jordan for tp in event_tps]

    rows = bounce_rows(
        injections,
        drain.candidates,
        result.peak_estimates,
        candidate_log_T,
        ledger_K_before(injections, result.parking, ledger),
        margin_candidates,
        events,
        event_log_T,
        generations,
    )
    return BounceRecord(rows=rows, K_final=ledger.K(result.N_final))


# the history's drain record (one-loop-drain prompt 03 §1.4; README §8.3, P9 as revised, P30), as ScalarHistory
# exposes it (ScalarHistory.drain_history) and stores it, one column each, in this order. Formed by
# drain_history_values from the payload, under the one-loop law only: None under the fixed law, at alpha = 0 and
# on a failure. Every value is dimensionless
#   drained_share_<regime>:  for one_loop, gap, classical and unassessed, the sum of the candidates' delta_K in the
#                            regime, in index order, over K_final (BounceRecord.K_final), as the tool's drain line
#                            (tools/history_and_bbn.drain_values) forms it, bitwise; None where K_final = 0
#   parking_share:           the parking leftover's delta_K over K_final; 0.0 if the history did not park; None
#                            where K_final = 0. With the four shares and K_0/K_final it sums to 1 to rounding
#   candidates_<regime>:     the candidates per regime, delta_K = 0 included
#   ell_min:                 the least ell over the one-loop candidates; None if there is none
#   log_R_min_drain:         the least ln R over the candidates with delta_K > 0 and an assessment; None if none
#   unresolvable_assessments, unresolved_declined:
#                            the DrainRecord's counts
#   all_worst_<kind>_margin, all_worst_<kind>_margin_N, all_<kind>_failing_share, for birth, reflection and tracker:
#                            Margins' worst margin, its N and the failing share over every generation, the weight
#                            ignored (P3, P30): the core's values with MARGIN_WEIGHT_SHARE 0 (all_generation_margins);
#                            None where the margins were not computed
#   bounce_rows:             the number of bounce rows
DrainHistoryValues = namedtuple(
    "DrainHistoryValues",
    [f"drained_share_{regime}" for regime in DRAIN_REGIMES]
    + ["parking_share"]
    + [f"candidates_{regime}" for regime in DRAIN_REGIMES]
    + ["ell_min", "log_R_min_drain", "unresolvable_assessments", "unresolved_declined"]
    + [
        f"{prefix}{kind}{suffix}"
        for kind in MARGIN_KINDS
        for prefix, suffix in (
            ("all_worst_", "_margin"),
            ("all_worst_", "_margin_N"),
            ("all_", "_failing_share"),
        )
    ]
    + ["bounce_rows"],
)


def all_generation_margins(generations) -> Dict[str, Any]:
    """
    The all-generation margins (one-loop-drain prompt 03 §1.4; README §8.3 P3, P30): for each kind, the worst
    margin over every generation, its N and the failing share, taken from the generations relativistic_margins
    returns by the core's own rules with the weight ignored (every generation counted), so that each value equals
    the core's with MARGIN_WEIGHT_SHARE 0: generations in index order, a strict comparison (a tie keeps the
    lowest index), the birth margin's N its N_turn, the failing share summed in order. One O(G) pass; no second
    run of the core.
    """
    threshold: float = MARGIN_THRESHOLD
    out: Dict[str, Any] = {}
    for kind in MARGIN_KINDS:
        worst: Optional[Generation] = None
        failing_share: float = 0.0
        for g in generations:
            margin = getattr(g, f"{kind}_margin")
            if margin is None:
                continue
            if worst is None or margin < getattr(worst, f"{kind}_margin"):
                worst = g
            if margin < threshold:
                failing_share += g.share
        if worst is None:
            out[f"all_worst_{kind}_margin"] = None
            out[f"all_worst_{kind}_margin_N"] = None
        else:
            out[f"all_worst_{kind}_margin"] = getattr(worst, f"{kind}_margin")
            out[f"all_worst_{kind}_margin_N"] = (
                worst.N_turn if kind == "birth" else getattr(worst, f"{kind}_margin_N")
            )
        out[f"all_{kind}_failing_share"] = failing_share
    return out


def drain_history_values(data: dict) -> Optional[DrainHistoryValues]:
    """
    The history's drain record of a success payload (one-loop-drain prompt 03 §1.4; orchestrator note 03, ruling
    8), read with .get as dissipation_values reads its own: None where the payload's "bounces" is None or absent
    (the fixed law, alpha = 0, a stand-in payload). Otherwise each field as DrainHistoryValues says, None where a
    key it is read from is absent.
    """
    record: Optional[BounceRecord] = data.get("bounces")
    if record is None:
        return None
    values = dict.fromkeys(DrainHistoryValues._fields)
    values["bounce_rows"] = len(record.rows)
    K_final: float = record.K_final

    drain: Optional[DrainRecord] = data.get("drain")
    injections = data.get("injections")
    if drain is not None:
        values["unresolvable_assessments"] = drain.unresolvable_assessments
        values["unresolved_declined"] = drain.unresolved_declined
    if drain is not None and drain.candidates is not None and injections is not None:
        candidates = drain.candidates
        for regime in DRAIN_REGIMES:
            values[f"candidates_{regime}"] = sum(
                1 for c in candidates if c.regime == regime
            )
        if K_final > 0.0:
            # as tools/history_and_bbn.drain_values forms them (ruling 8), so that they are its bitwise
            for regime in DRAIN_REGIMES:
                K = sum(
                    i.delta_K
                    for c, i in zip(candidates, injections)
                    if c.regime == regime
                )
                values[f"drained_share_{regime}"] = K / K_final
            if "parking" in data:
                parking: Optional[Parking] = data["parking"]
                K_parking = 0.0 if parking is None else parking.delta_K
                values["parking_share"] = K_parking / K_final
        ells = [c.ell for c in candidates if c.regime == REGIME_ONE_LOOP]
        values["ell_min"] = min(ells) if len(ells) > 0 else None
        log_Rs = [
            c.log_R
            for c, i in zip(candidates, injections)
            if i.delta_K > 0.0 and c.log_R is not None
        ]
        values["log_R_min_drain"] = min(log_Rs) if len(log_Rs) > 0 else None

    generations = data.get("generations")
    if data.get("margins") is not None and generations is not None:
        values.update(all_generation_margins(generations))

    return DrainHistoryValues(**values)


def truncate_to_history(z_grid: redshift_array, final_N: float) -> redshift_array:
    """
    The points of the sampling axis that a history ending at final_N e-folds reaches: those with
    ln(1 + z) <= final_N, to redshift_array's tolerance (1e-10), so a point at most 1e-10 beyond
    final_N is kept and sampled at N_forward >= -1e-10. (jordan-normalization prompt 03, Z4; a
    helper so that a test can drive it with a synthetic final_N.)
    """
    return z_grid.truncate(final_N, keep="lower")


def _failure_payload(reason: str, warnings: Optional[Tuple[str, ...]] = None) -> dict:
    """
    The payload compute_scalar_history returns for a failed history: the failure flag and
    the reason, truncated to DEFAULT_STRING_LENGTH, the width of the
    ScalarHistory.failure_reason column. (science-readiness prompt 02; BBNData's pattern)

    If warnings is given (integrate_history's failures; inelastic-reflection prompt 05,
    orchestrator note 05 ruling 2), the payload carries them under "warnings", which is not stored.
    """
    payload = {"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}
    if warnings is not None:
        payload["warnings"] = tuple(warnings)
    return payload


def integrate_history(
    cosmology: LambdaCDM_GenericEOS,
    T_init: TemperatureLike,
    T_stop: TemperatureLike,
    phi_init: FieldLike,
    pi_init: FieldLike,
    z_grid: redshift_array,
    model: RealizedModel,
    task_label: str = "integrate_history",
    atol: float = DEFAULT_ABS_TOLERANCE,
    rtol: float = DEFAULT_REL_TOLERANCE,
    verbose: bool = False,
    rho_chi_ratio_init: float = 0.0,
    alpha: float = 0.0,
    drain_law: str = DRAIN_LAW_FIXED,
    compute_sample_masses: bool = True,
) -> dict:
    """
    The primitive (jordan-normalization prompt 06, T1; README §6.0 R8): integrate one history of
    one realized model, from T_init to any T_stop, and sample it on the axis. It has no
    normalization logic: the model's normalization and phi_ref are whatever `model` was realized
    with. tune_history calls it once per iterate; tests, diagnostics and the tools that run their
    own histories call it directly. (Until prompt 06 this was the body of compute_scalar_history,
    which since prompt 07 tunes the history with tune_history.)

    :param cosmology: background cosmology
    :param T_init: initial radiation temperature in Jordan frame
    :param T_stop: final radiation temperature in Jordan frame (usually T_CMB)
    :param phi_init: initial Einstein-frame scalar field value phi
    :param pi_init: initial Einstein-frame scalar field derivative dphi/dN
    :param z_grid: the sampling axis: points ln(1 + z_E), counted back from the end of the history
    :param model: the realized model: V, the conformal factor A and their derivatives
                  (jordan-normalization prompt 04)
    :param task_label: label for the ray task
    :param atol: absolute tolerance for the ODE solver
    :param rtol: relative tolerance for the ODE solver
    :param rho_chi_ratio_init: a TEST HARNESS (inelastic-reflection prompt 01): r_0, the ratio
                               rho_chi,E/rho_R,E of a dark-radiation component at T_init. The
                               history's DarkRadiationLedger then holds K_0 = r_0 rho_R,E,init
                               e^{4 N_start}, present at every N. 0.0 (the default) gives an empty
                               ledger, and every value is the old code's bitwise.
                               compute_scalar_history does not take it, so no history a driver
                               launches, and none that is stored, has an initial rho_chi.
    :param alpha: the fraction of the condensate's kinetic energy drained at each drain point and
                  floor reflection (inelastic-reflection prompt 02; see integrate_scalar_history),
                  0 <= alpha < 1, else a ValueError before any integration. 0.0 (the default) is
                  the old code bitwise. compute_scalar_history passes it through tune_history since
                  inelastic-reflection prompt 09, which makes it a key of every stored history.
                  Under the one-loop drain law it is A, finite and > 0
    :param drain_law: DRAIN_LAW_FIXED (the default) or DRAIN_LAW_ONE_LOOP (one-loop-drain prompt 01;
                      check_drain_law), passed to integrate_scalar_history. Since one-loop-drain prompt 03
                      compute_scalar_history takes it, from the stored key (alpha_value's drain_model); no
                      driver asks for the one-loop law until prompt 04 gives it the option
    :param compute_sample_masses: whether the payload carries "log_m_eff" and "log_m_tracker", the sample
                      masses (sample_masses; one-loop-drain prompt 03 §1.5, orchestrator note 03 ruling 2).
                      True, the default, for every caller but tune_history, which passes False on each of its
                      integrations and computes them once, on the converged payload (P31); with False the two
                      keys are absent
    :return: the payload; on success it carries "dark_radiation", the history's
             DarkRadiationLedger (inelastic-reflection prompt 01), and "turning_points" and
             "injections", the IntegrationResult's lists of TurningPoint and Injection
             (inelastic-reflection prompt 02), and "dropped_drains", its count of armed drains
             that never reached a drain point (prompt 03), and "warnings", its warning lines
             (prompt 05), which a failure's payload carries too, and "parking", its Parking record
             or None (prompt 06), and "generations", "margins" and "margin_warnings", the spectrum's
             peaks and the relativistic margins (prompt 08; None, None and () at alpha = 0), which a
             failure's payload does not carry, and "rho_chi_over_rho_rad", r_chi at each sample,
             and "dark_radiation_values", a DarkRadiationValues (prompt 09), which a failure's
             payload does not carry either. ScalarHistory.store reads "injections", "parking",
             "release", "margins" and the last two (prompt 09); no store reads the others. Every
             payload, a failure's included, carries "drain", the DrainRecord of the drain law
             (one-loop-drain prompt 01 §1.4), as far as the integration reached, or None if it failed
             before the step loop; no store reads it, and build_extra_data does not. Since
             one-loop-drain prompt 03 a success's payload carries "bounces" too, a BounceRecord under the
             one-loop law (bounce_record) and None under the fixed law and at alpha = 0, and, unless
             compute_sample_masses is False, "log_m_eff" and "log_m_tracker", tuples parallel to "sample"
             (sample_masses). ScalarHistory.store reads all three, and "drain"; a failure's payload carries
             none of them
    """
    check_drain_law(drain_law, alpha, "integrate_history")
    check_drain_law_model(drain_law, model, "integrate_history")

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
    log_rhorad_Einstein_init: float = log_rhorad_Jordan_init + 4.0 * model.log_Omega(
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

    # print(f"-- compute_scalar_history ({task_label}): initial data")
    # print(
    #     f"    - T_Jordan_init = {GetTemperature(T_init)/units.GeV:.5g} GeV = {GetTemperature(T_init)/units.Kelvin:.5g} K"
    # )
    # print(f"    - rho_r_Jordan_init = ({rhorad_Jordan_init_14/units.GeV:.5g} GeV)^4")
    # print(f"    - rho_m_Jordan_init = ({rhomat_Jordan_init_14/units.GeV:.5g} GeV)^4")
    # print(f"    - f_m_init = {fm_init:.5g} | log(f_m_init) = {log_fm_init:.5g}")
    # print(
    #     f"    - log_rho_r_Jordan_init = {log_rhorad_Jordan_init:.5g}, log_rho_r_Einstein_init = {log_rhorad_Einstein_init:.5g}"
    # )

    N_failsafe = 1000.0  # terminate after 1000 e-folds as a failsafe
    N_start = 0.0

    # the dark-radiation component (inelastic-reflection prompt 01; orchestrator note 01, rulings 1
    # and 3): always a ledger, held by the one ODEPolicy below, empty unless the test harness
    # rho_chi_ratio_init gives an initial K_0 = r_0 rho_R,E,init e^{4 N_start}
    K_initial: float = 0.0
    if rho_chi_ratio_init != 0.0:
        K_initial = rho_chi_ratio_init * exp(log_rhorad_Einstein_init + 4.0 * N_start)
    dark_radiation = DarkRadiationLedger(K_initial)

    policy = ODEPolicy(task_label, cosmology, model, dark_radiation=dark_radiation)
    RHS = ODERHS(task_label, policy)

    # the step loop's parameters: the defaults of StepControl, with the tolerances requested
    step_control = StepControl(atol=atol, rtol=rtol)

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
                alpha=alpha,
                drain_law=drain_law,
            )

        # the solution must carry the five components of StateVector; anything else is a bug
        assert (
            len(result.solution(result.N_final)) == EXPECTED_SOL_LENGTH
        ), f"integrate_history ({task_label}): solution does not have {EXPECTED_SOL_LENGTH} components"

        if verbose and len(result.reflections) > 0:
            print(
                f"-- integrate_history ({task_label}): {len(result.reflections)} reflection(s), first at N={result.reflections[0].N:.5g}"
            )

        # the integration terminated at T_Jordan = T_stop, which is ln(1 + z_E) = 0 on the sampling
        # axis. We work backwards and sample the integration output on the axis: a point's
        # ln(1 + z_E) is the number of e-folds back from the end (jordan-normalization prompt 03)
        final_N = result.N_final
        z_grid_cut = truncate_to_history(z_grid, final_N)

        max_z: redshift = z_grid.max
        if max_z.log_one_plus_z < final_N:
            raise RuntimeError(
                f"integrate_history: ({task_label}): largest supplied redshift has ln(1 + z)={max_z.log_one_plus_z:.6g}, but solution required N={final_N:.6g} e-folds"
            )

        sample = []
        # r_chi = rho_chi/rho_R at each sample, parallel to sample: the policy's own float, exactly
        # 0.0 at alpha = 0 (inelastic-reflection prompt 09, P27). SampleValues does not change, since
        # both golden histories hash its fields
        rho_chi_ratios = []

        # loop over the required z sample grid.
        # Note that we will work from high z to low z.

        solution: OdeSolution = result.solution

        hubble: HubblePolicy = HubblePolicy(model, units)

        # ln A(phi_0), with phi_0 the field at the end of the history, for ln(1 + z_J)
        phi_0: float = StateVector._make(solution(final_N)).phi_Einstein
        log_A_0: float = model.log_Omega(phi_0)

        try:
            for z in z_grid_cut:
                z: redshift
                N_backward = z.log_one_plus_z
                N_forward = final_N - N_backward

                state: StateVector = StateVector._make(solution(N_forward))
                data: ODEPolicyData = policy(N_forward, state)
                hubble_data: HubblePolicyData = hubble(data, state)

                T_Jordan: float = data.T_Jordan

                log_Omega: float = model.log_Omega(state.phi_Einstein)
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
                        log_one_plus_z_Jordan=N_backward + (log_A_0 - log_Omega),
                    )
                )
                rho_chi_ratios.append(data.rho_chi_over_rho_rad)
        except OverflowError as e:
            print(
                f"!! integrate_history ({task_label}): overflow when assembling sample values; marked as total integration failure"
            )
            payload = _failure_payload(
                f"sampling: overflow when assembling sample values: {e}",
                warnings=result.warnings,
            )
            payload["drain"] = result.drain
            return payload
    except ComputationFailureError as e:
        print(f"-- integrate_history ({task_label}): integration failure")
        print(f"   {e.message}")
        print(
            f"!! integrate_history ({task_label}): marked as total integration failure"
        )
        # the warnings collected before the failure (inelastic-reflection prompt 05; orchestrator
        # note 05, ruling 2); none if the failure came from outside the step loop. The drain law's
        # record likewise (one-loop-drain prompt 01 §1.4): None from outside the step loop
        payload = _failure_payload(e.message, warnings=getattr(e, "warnings", ()))
        payload["drain"] = getattr(e, "drain", None)
        return payload

    # the first bounce, on the dense output (science-readiness prompt 03); None if there is none
    bounce: Optional[FirstBounce] = first_bounce(result)

    # phi and rho_NP/rho_R,J at T_J = 1 MeV and 70 keV, on the dense output (science-readiness
    # prompt 06b); a field is None where the history does not reach the temperature
    fixed_T: FixedTValues = fixed_T_values(result, policy, model, units)

    # r_chi and Delta N_eff at T_J = 1 MeV, 70 keV and today, on the dense output (inelastic-reflection
    # prompt 09; README §8.1 R22, §8.2 P31); computed at every alpha, 0.0 at alpha = 0
    dark_values: DarkRadiationValues = dark_radiation_values(result, policy, cosmology)

    # the spectrum's peak and the relativistic margins (inelastic-reflection prompt 08), after the
    # integration and outside its loop, under the step loop's own condition for assessing the wall
    # (orchestrator note 08, ruling 2): at alpha = 0, or on a model without a wall, nothing is computed
    # (P22). A ComputationFailureError inside them is one warning line, never the history's failure
    # Since one-loop-drain prompt 03 the gatherer and the core are called here, as history_margins calls them,
    # so that the core's inputs are kept for the bounce rows (orchestrator note 03, ruling 4): the core runs
    # once and no model call is added
    generations: Optional[Tuple[Generation, ...]] = None
    margins: Optional[Margins] = None
    margin_warnings: Tuple[str, ...] = ()
    inputs: Optional[MarginInputs] = None
    if alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None:
        try:
            inputs = margin_inputs(result, sample, policy)
            generations, margins = relativistic_margins(*inputs)
            margin_warnings = margin_warning_lines(margins)
        except ComputationFailureError as e:
            inputs = None
            generations, margins = None, None
            margin_warnings = (f"{WARNING_MARGINS_FAILED}: {e.message}",)

    # the bounce rows (one-loop-drain prompt 03 §1.3; README §8.11 P29), under the one-loop law only: None under
    # the fixed law and at alpha = 0 (no instrumentation there, README §6.3 ruling 2). Where the margins failed
    # the candidate rows are written without what depends on them, and no event row (ruling 5)
    bounces: Optional[BounceRecord] = None
    if drain_law == DRAIN_LAW_ONE_LOOP:
        bounces = bounce_record(result, inputs, generations, dark_radiation)

    collected_full_statistics = supervisor.collect_full_statistics
    payload = {
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
        "fixed_T_values": fixed_T,
        "cap_fraction": step_control.cap_fraction,
        "cap_floor": step_control.cap_floor,
        "cap_global_max_step": step_control.global_max_step,
        "jacobian_factor_max": step_control.jacobian_factor_max,
        "accepted_steps": result.accepted_steps,
        "steps_rejected_by_exception": result.steps_rejected_by_exception,
        # the guard's retakes (reflection-restart prompt 03 §1.2); stored in extra_data only when
        # above 0
        "restart_retakes": result.restart_retakes,
        "largest_RHS_values": (
            supervisor.largest_RHS_values if collected_full_statistics else None
        ),
        "smallest_RHS_values": (
            supervisor.smallest_RHS_values if collected_full_statistics else None
        ),
        "mean_RHS_values": (
            supervisor.mean_RHS_values if collected_full_statistics else None
        ),
        "solver_label": SCALAR_HISTORY_STEPPER_LABEL,
        # the history's dark-radiation ledger (inelastic-reflection prompt 01): _tuning_state reads
        # it; ScalarHistory.store and build_extra_data read named keys only, so it is not stored
        "dark_radiation": dark_radiation,
        # the turning points and the drains (inelastic-reflection prompt 02); not stored either.
        # "reflections" above counts every reflection, of both kinds (prompt 05): at alpha = 0 every
        # reflection is a floor-rule one, so the count there is unchanged
        "turning_points": result.turning_points,
        "injections": result.injections,
        # the armed drains that never reached a drain point (inelastic-reflection prompt 03); not
        # stored either
        "dropped_drains": result.dropped_drains,
        # the history's warnings, a tuple of lines (inelastic-reflection prompt 05); printed by
        # tune_history, never here, and not stored
        "warnings": result.warnings,
        # the switch to the parked phase, a Parking record of floats and ints, or None if the history
        # did not park (inelastic-reflection prompt 06); not stored
        "parking": result.parking,
        # the release of the parked field, a Release record of floats and ints, or None if the
        # history was not released (inelastic-reflection prompt 07); not stored
        "release": result.release,
        # the generations, a tuple of Generation, and the history's Margins record, or None at alpha = 0,
        # on a model without a wall, or where the margins failed; and the margins' warning lines, a
        # tuple, () where none fires, printed by tune_history beside "warnings" (inelastic-reflection
        # prompt 08; orchestrator note 08, rulings 1 and 2). None is stored (prompt 09's)
        "generations": generations,
        "margins": margins,
        "margin_warnings": margin_warnings,
        # r_chi at each sample, a tuple parallel to "sample", and the dark radiation at 1 MeV, 70 keV
        # and today, a DarkRadiationValues (inelastic-reflection prompt 09; P27, R22, P31). Both are
        # stored by ScalarHistory.store, as are the records above
        "rho_chi_over_rho_rad": tuple(rho_chi_ratios),
        "dark_radiation_values": dark_values,
        # the drain law's record, a DrainRecord (one-loop-drain prompt 01 §1.4): the law, A, the drain
        # model (prompt 02c), the unresolvable assessments and, under the one-loop law, each candidate's
        # regime, ell, alpha_eff, w_gate, ln E, ln H_E, L and ln R. ScalarHistory.store checks its model against
        # the key's (one-loop-drain prompt 03, P28) and forms the history's drain record from it
        "drain": result.drain,
        # the bounce rows, a BounceRecord under the one-loop law, None under the fixed law (one-loop-drain prompt
        # 03 §1.3); stored by ScalarHistory.store as rows of ScalarHistoryBounce
        "bounces": bounces,
    }
    # the sample masses (one-loop-drain prompt 03 §1.5; orchestrator note 03, ruling 2): every law and every
    # alpha, unless tune_history turned them off, which computes them once on the converged payload (P31)
    if compute_sample_masses:
        payload["log_m_eff"], payload["log_m_tracker"] = sample_masses(
            model, cosmology, sample
        )
    return payload


# ---- the tuning layer (jordan-normalization prompt 06; README §6.0 R1, R2, R8, R10, R13) ----

# the defaults of tune_history: the tolerance on both residuals and the largest number of
# integrations, the seed's included (README §6.2 P4). jordan-normalization prompt 08 gives them
# options
DEFAULT_TUNING_TOLERANCE = 1.0e-5
DEFAULT_TUNING_MAX_INTEGRATIONS = 30

# Eq. 7's V_target <= 0 at this many integrations running is a failure of the tuning
TUNING_GUARD_LIMIT = 4

# when a quiet tuning prints a tune line (tuning-followups prompt 02; README §6.0 R2, P6): at least
# this many seconds since its last line (or its start), or from this integration on (k + 1 >= it),
# or on a guard or reset step. The user's ruling: "120s and 10 integrations ... we can adjust later"
TUNE_LINE_INTERVAL_S = 120.0
TUNE_LINE_SLOW_INTEGRATIONS = 10

# the values of tune_history's `progress` (tuning-followups prompt 02, R2): "quiet" prints by the
# rule above, "every" prints one line per integration (the standalone tools, U2)
TUNE_LINE_PROGRESS = ("quiet", "every")

# the keys tune_history adds to the final integration's payload (README §6.2 P6); each is a
# Python float, int or str. jordan-normalization prompt 07 stores exactly these
TUNING_OUTPUT_KEYS = (
    "normalization",
    "V_ref",
    "phi_ref",
    "phi_0",
    "pi_0",
    "tracking",
    "residual_H",
    "residual_phi",
    "tuning_iterations",
    "plain_steps",
    "secant_steps",
    "guard_steps",
    "seed_kind",
    "seed_normalization",
    "seed_phi_ref",
)

# what a ScalarHistory holds of its tuning (jordan-normalization prompt 07, S3): the keys of
# TUNING_OUTPUT_KEYS, in that order, as tune_history returned them (after a round trip through
# the store, each in the cosmology's units again)
TuningValues = namedtuple("TuningValues", TUNING_OUTPUT_KEYS)


def tuning_converged(residual_H: float, residual_phi: float, tolerance: float) -> bool:
    """
    The tuning's convergence test (jordan-normalization prompt 06, T2): true iff both residuals,
    residual_H = H_J/H0 - 1 and residual_phi = phi_ref/phi_min - 1 at T_J = T_CMB, are inside
    the tolerance. The tolerance itself is outside.
    """
    return abs(residual_H) < tolerance and abs(residual_phi) < tolerance


def _solve2(B, F):
    """d with B d = -F, for a 2x2 B, by Cramer's rule."""
    (a, b), (c, d) = B
    det = a * d - b * c
    return ((-F[0] * d + F[1] * b) / det, (-a * F[1] + c * F[0]) / det)


class TuningSecant:
    """
    The step of the tuning (jordan-normalization prompt 06, T3; README §6.0 R10, §6.2 P4): Broyden's
    ("good") method on the fixed-point residual F(x) = G(x) - x of the plain map G, in two
    variables. It holds the Broyden matrix B, the last point stepped from and its F, and nothing
    else: no integration, no model, no count.

    The caller supplies x = (normalization_coordinate, phi_ref/scale) and G(x), the plain map at
    x: the frozen-state renormalization to Eq. 7's V_target and phi_ref <- phi_min, in the same
    coordinates. step(x, G) returns the next point and the kind of step, "plain" or "secant":
      - a plain step sets B = -I and returns G(x) itself, so the first step is the plain update;
      - a secant step, taken when the secant is on and B is set, updates B by Broyden's rule,
        B += (dF - B dx) dx^T / |dx|^2, with dx and dF the changes in x and F since the last
        step, solves B d = -F by Cramer's rule, and returns x + d;
      - the twice rule: if x came from a secant step and |F| > 2 |F_prev| (Euclidean, in x's
        coordinates), B is reset and the step is plain, from x.
    guard() resets B and the last point, so that the next step is plain; the tuning calls it when
    Eq. 7 has no positive V_target and it moves the iterate itself.

    With secant=False every step is plain (the comparison of prompt 06 §2, and diagnostics).
    """

    def __init__(self, secant: bool = True):
        self._secant = bool(secant)
        self._B = None
        self._x_prev = None
        self._F_prev = None
        self._last_kind = None
        self._reset = False

    @property
    def B(self):
        """A copy of the Broyden matrix, as a list of two rows, or None when it is not set."""
        return None if self._B is None else [list(row) for row in self._B]

    @property
    def reset(self) -> bool:
        """
        True iff the last step() was the twice rule's reset (a plain step that discarded B because
        |F| more than doubled after a secant step); false before any step, after any other step
        and after guard(). Read-only; it changes no return and no count (tuning-followups prompt 02,
        ruling 2 of its note).
        """
        return self._reset

    def guard(self):
        """Forget B and the last point: the next step is plain."""
        self._B = None
        self._x_prev = None
        self._F_prev = None
        self._last_kind = None
        self._reset = False

    def step(self, x, G):
        """The next point from x, given G = G(x), and the kind of step: (x_new, kind)."""
        F = (G[0] - x[0], G[1] - x[1])
        nF = sqrt(F[0] ** 2 + F[1] ** 2)

        self._reset = False
        kind = "plain"
        if self._secant and self._B is not None:
            Fp = self._F_prev
            if self._last_kind == "secant" and nF > 2.0 * sqrt(Fp[0] ** 2 + Fp[1] ** 2):
                self._B = None
                self._reset = True
            else:
                B, xp = self._B, self._x_prev
                dx = (x[0] - xp[0], x[1] - xp[1])
                dF = (F[0] - Fp[0], F[1] - Fp[1])
                Bdx = (
                    B[0][0] * dx[0] + B[0][1] * dx[1],
                    B[1][0] * dx[0] + B[1][1] * dx[1],
                )
                u = (dF[0] - Bdx[0], dF[1] - Bdx[1])
                dd = dx[0] ** 2 + dx[1] ** 2
                self._B = [
                    [B[0][0] + u[0] * dx[0] / dd, B[0][1] + u[0] * dx[1] / dd],
                    [B[1][0] + u[1] * dx[0] / dd, B[1][1] + u[1] * dx[1] / dd],
                ]
                kind = "secant"

        if kind == "plain":
            self._B = [[-1.0, 0.0], [0.0, -1.0]]
            x_new = (G[0], G[1])
        else:
            d = _solve2(self._B, F)
            x_new = (x[0] + d[0], x[1] + d[1])

        self._x_prev, self._F_prev = (x[0], x[1]), F
        self._last_kind = kind
        return x_new, kind


# today's state of one integration, and the residuals (jordan-normalization prompt 06, T2)
TuningState = namedtuple(
    "TuningState",
    [
        "phi_0",
        "pi_0",
        "log_V",
        "V_target",
        "phi_min",
        "residual_H",
        "residual_phi",
    ],
)


def _tuning_state(
    cosmology, model: AbstractModel, rm: RealizedModel, data: dict
) -> TuningState:
    """
    Today's state from the last sample of an integration (ln(1 + z) = 0, T_J = T_CMB), Eq. 7's
    V_target (the potential at which H_J = H0 at that state), the effective minimum at the
    history's own rho_m,E today, and the two residuals. The expressions are fixed by orchestrator
    note 06, ruling 5. effective_minimum's ValueError propagates.

    The bracket of Eq. 7 subtracts the dark-radiation component as well (inelastic-reflection
    prompt 01): rho_chi,J = r_chi rho_R,J, with r_chi = rho_chi,E/rho_R,E read from the payload's
    ledger at the last sample's N and ln rho_R,E (r_chi is the same in both frames). With no dark
    radiation the expression is exactly the old one. effective_minimum reads rho_m,E only: massless
    quanta are traceless and do not move the minimum.
    """
    Mp = cosmology.units.PlanckMass
    H0 = cosmology.H0
    norm, phi_ref = rm.normalization, rm.phi_ref

    s = SampleValues._make(tuple(data["sample"][-1]))
    phi0, pi0 = s.phi_Einstein, s.pi_Einstein
    A0 = rm.Omega(phi0)
    rho_RJ = exp(s.log_rhorad_Jordan)
    fm = exp(s.log_fm)
    logV = rm.log_V(phi0)
    bp = 1.0 + rm.d_logOmega_dphi(phi0) * pi0
    G = 1.0 - pi0 * pi0 / (6.0 * Mp * Mp)
    ledger: DarkRadiationLedger = data["dark_radiation"]
    if ledger.is_zero(s.raw_N):
        V_target = A0**4 * (
            3.0 * Mp**2 * H0**2 * G / (A0**2 * bp**2) - rho_RJ * (1.0 + fm)
        )
    else:
        r_chi = ledger.ratio(s.raw_N, s.log_rhorad_Einstein)
        V_target = A0**4 * (
            3.0 * Mp**2 * H0**2 * G / (A0**2 * bp**2) - rho_RJ * (1.0 + fm + r_chi)
        )
    rho_mE0 = A0**4 * fm * rho_RJ
    phi_min = model.effective_minimum(norm, phi_ref, phi0, rho_mE0)

    return TuningState(
        phi_0=phi0,
        pi_0=pi0,
        log_V=logV,
        V_target=V_target,
        phi_min=phi_min,
        residual_H=s.H_Jordan / H0 - 1.0,
        residual_phi=phi_ref / phi_min - 1.0,
    )


def _tuning_failure(task_label: str, reason: str, drain=None) -> dict:
    """
    A failed tuning: the line, and _failure_payload's payload, with "drain", the DrainRecord of the
    integration that ended the tuning (one-loop-drain prompt 01 §1.4), or None.
    """
    print(f"!! tune_history ({task_label}): {reason}", flush=True)
    payload = _failure_payload(reason)
    payload["drain"] = drain
    return payload


def tune_line_due(
    progress: str,
    k: int,
    kind: str,
    reset: bool,
    now: float,
    last_line: float,
) -> bool:
    """
    Whether integration k of a tuning prints its tune line (tuning-followups prompt 02; README §6.0
    R2, P6). kind is the step that reached the integration ("seed", "plain", "secant" or "guard"),
    reset whether the twice rule's reset reached it, now the clock at its end and last_line the
    clock when the tuning last printed a line (its start, if none).
      - "every": always;
      - "quiet": iff now - last_line >= TUNE_LINE_INTERVAL_S, or k + 1 >= TUNE_LINE_SLOW_INTEGRATIONS,
        or kind == "guard", or reset;
      - anything else: ValueError.
    """
    if progress == "every":
        return True
    if progress == "quiet":
        return (
            now - last_line >= TUNE_LINE_INTERVAL_S
            or k + 1 >= TUNE_LINE_SLOW_INTEGRATIONS
            or kind == "guard"
            or bool(reset)
        )
    raise ValueError(
        f"tune_history: progress must be one of {TUNE_LINE_PROGRESS}, not {progress!r}"
    )


def tune_history(
    cosmology: LambdaCDM_GenericEOS,
    model: AbstractModel,
    T_init: TemperatureLike,
    phi_init: FieldLike,
    pi_init: FieldLike,
    z_grid: redshift_array,
    task_label: str = "tune_history",
    atol: float = DEFAULT_ABS_TOLERANCE,
    rtol: float = DEFAULT_REL_TOLERANCE,
    tolerance: float = DEFAULT_TUNING_TOLERANCE,
    max_integrations: int = DEFAULT_TUNING_MAX_INTEGRATIONS,
    secant: bool = True,
    verbose: bool = False,
    progress: str = "quiet",
    clock=perf_counter,
    rho_chi_ratio_init: float = 0.0,
    alpha: float = 0.0,
    drain_law: str = DRAIN_LAW_FIXED,
) -> dict:
    """
    The tuning layer (jordan-normalization prompt 06; README §6.0 R1, R2, R8, R10, R13): integrate
    the history to T_J = T_CMB with integrate_history and adjust the model's normalization and
    phi_ref until H_J(T_CMB) = H0 and phi_ref = phi_min(T_CMB), the minimum of the Einstein-frame
    effective potential at the history's own state today.

    The model is read only through realize, renormalize, normalization_coordinate,
    normalization_from_coordinate, effective_minimum, seed and the RealizedModel's methods; the
    tuning never reads the model's parameters.

    - It starts from model.seed(cosmology) (R13: the history's own inputs, no cache) and stops at
      T_stop = cosmology.T_CMB_Kelvin * cosmology.units.Kelvin, the float main.py uses.
    - After each integration k (k = 0 is the seed's) it forms today's state and the residuals
      residual_H = H_J/H0 - 1 and residual_phi = phi_ref/phi_min - 1, and, when tune_line_due
      says so, prints one line
          -- tune <task_label> it=<k> kind=<seed|plain|secant|guard> H/H0-1=... phi_ref/phi_min-1=... norm=... phi_ref=<phi_ref/M_P> wall=<s>
      With progress="quiet" (the default; compute_scalar_history, so main.py, passes nothing else)
      the line is printed only if clock() has advanced TUNE_LINE_INTERVAL_S (120 s) since the
      tuning's last line (or its start), or from the TUNE_LINE_SLOW_INTEGRATIONS-th (10th)
      integration on, or if the integration was reached by a guard step or by the twice rule's reset
      of the secant. With progress="every" every integration prints (the standalone tools). Any other
      progress is a ValueError at entry, before the seed. The lines exist for the heartbeat of a
      registered run, which beats only on a stdout line (jordan-normalization P12); the rule keeps
      them to the slow or unusual tunings, so while a tuning runs its longest silence is at most
      TUNE_LINE_INTERVAL_S plus one integration. A failure's "!!" line is printed whatever the rule
      says. clock (default perf_counter) is called once before the loop and once before and once
      after each integration, and times both the rule and each line's wall=.
    - The history's warnings (inelastic-reflection prompt 05): each integration's payload, failed or
      not, carries its warning lines under "warnings". The first line of each kind (warning_kind)
      is printed, as "!! tune_history (<task_label>): <line>", from the first integration that
      returns it, whatever progress is, and no later line of that kind; a failure's warnings are
      printed before its "!!" failure line. The lines of "margin_warnings" (inelastic-reflection
      prompt 08) follow those of "warnings", through the same set of printed kinds.
      If tuning_converged(residual_H, residual_phi, tolerance), it returns.
    - If Eq. 7's V_target <= 0 (no positive potential gives H0 at today's state), the next iterate
      halves V(phi_0) through renormalize and sets phi_ref = phi_min, and the step's matrix is
      reset (a "guard" step). V_target <= 0 at TUNING_GUARD_LIMIT integrations running fails.
    - Otherwise TuningSecant takes the step, "plain" or "secant", on
      x = (normalization_coordinate, phi_ref/scale), scale being phi_min of integration 0.
    - After max_integrations integrations, the seed's included, it fails.

    secant=False takes the plain step at every iterate. It exists for the comparison of prompt 06
    §2 and for diagnostics; no driver passes it.

    rho_chi_ratio_init is a TEST HARNESS (inelastic-reflection prompt 01): r_0 = rho_chi,E/rho_R,E
    at T_init, passed to every integration, each of which builds its own ledger from it (see
    integrate_history). Eq. 7's V_target subtracts the component today (_tuning_state). 0.0 (the
    default) is the old tuning bitwise. compute_scalar_history does not take it.

    alpha (inelastic-reflection prompt 02) is passed to every integration, each of which drains into
    its own ledger (see integrate_scalar_history). A value outside 0 <= alpha < 1 is a ValueError
    at entry, before the seed. 0.0 (the default) is the old tuning bitwise. compute_scalar_history
    passes its own (inelastic-reflection prompt 09).

    drain_law (one-loop-drain prompt 01 §1.1) is passed to every integration with alpha, and checked at
    entry with it (check_drain_law): under DRAIN_LAW_ONE_LOOP alpha is A, finite and > 0. The default is
    DRAIN_LAW_FIXED, the old tuning bitwise; compute_scalar_history passes the stored key's law since
    one-loop-drain prompt 03. The payload's "drain" is the accepted integration's; a failure's payload
    carries the "drain" of the integration that ended the tuning.

    The sample masses (one-loop-drain prompt 03 §1.5, P31): every integration is made with
    compute_sample_masses=False, and the success's payload gains "log_m_eff" and "log_m_tracker"
    (sample_masses) from its samples and the accepted integration's realized model, once.

    :return: on success, the final integration's payload (every key integrate_history returns),
             with the keys of TUNING_OUTPUT_KEYS added: normalization, V_ref = V(phi_ref) on the
             final realization, phi_ref, phi_0, pi_0, tracking = pi_0/(1.5 phi_0), residual_H,
             residual_phi, tuning_iterations (the integrations, the seed's included),
             plain_steps, secant_steps, guard_steps, seed_kind, seed_normalization and
             seed_phi_ref. On failure, _failure_payload's payload, whose reason's first clause
             is one of "the normalization did not converge", "no normalization reaches H0",
             "the effective minimum was not found", or an integration's own reason with
             " (tuning integration k)" appended.
    """
    if progress not in TUNE_LINE_PROGRESS:
        raise ValueError(
            f"tune_history: progress must be one of {TUNE_LINE_PROGRESS}, not {progress!r}"
        )
    check_drain_law(drain_law, alpha, "tune_history")
    check_drain_law_model(drain_law, model, "tune_history")

    units: UnitsLike = cosmology.units
    Mp: float = units.PlanckMass
    T_stop: float = cosmology.T_CMB_Kelvin * units.Kelvin

    seed: Seed = model.seed(cosmology)
    norm, phi_ref = seed.normalization, seed.phi_ref

    stepper = TuningSecant(secant=secant)
    steps = {"plain": 0, "secant": 0, "guard": 0}
    kind = "seed"
    scale: Optional[float] = None
    guards_running = 0
    reset = False
    last_line = clock()

    # the kinds of warning already printed for this history (inelastic-reflection prompt 05;
    # orchestrator note 05, ruling 1)
    warned_kinds = set()

    k = 0
    while True:
        rm: RealizedModel = model.realize(norm, phi_ref)

        start = clock()
        data = integrate_history(
            cosmology,
            T_init,
            T_stop,
            phi_init,
            pi_init,
            z_grid,
            rm,
            task_label=task_label,
            atol=atol,
            rtol=rtol,
            verbose=verbose,
            rho_chi_ratio_init=rho_chi_ratio_init,
            alpha=alpha,
            drain_law=drain_law,
            # the sample masses are computed once, on the converged payload, below (one-loop-drain prompt 03,
            # P31; orchestrator note 03, ruling 2)
            compute_sample_masses=False,
        )
        end = clock()
        wall = end - start

        # the history's warnings (inelastic-reflection prompt 05; README §8.1 R6, §8.2 P23): the first
        # line of each kind, from the first integration that returns it, whatever progress is, and
        # before a failure's line (orchestrator note 05, rulings 1, 2 and 9); then the margins' lines,
        # through the same set of printed kinds (inelastic-reflection prompt 08; orchestrator note 08,
        # ruling 1)
        for line in (*data.get("warnings", ()), *data.get("margin_warnings", ())):
            line_kind = warning_kind(line)
            if line_kind not in warned_kinds:
                warned_kinds.add(line_kind)
                print(f"!! tune_history ({task_label}): {line}", flush=True)

        if data.get("failure", False):
            return _tuning_failure(
                task_label,
                f"{data['failure_reason']} (tuning integration {k})",
                drain=data.get("drain"),
            )

        try:
            state: TuningState = _tuning_state(cosmology, model, rm, data)
        except ValueError as e:
            return _tuning_failure(
                task_label,
                f"the effective minimum was not found: {e} (tuning integration {k})",
                drain=data.get("drain"),
            )

        if tune_line_due(progress, k, kind, reset, end, last_line):
            print(
                f"-- tune {task_label} it={k} kind={kind} H/H0-1={state.residual_H:+.3e} "
                f"phi_ref/phi_min-1={state.residual_phi:+.3e} norm={norm:.12g} "
                f"phi_ref={phi_ref / Mp:.10e} wall={wall:.1f}",
                flush=True,
            )
            last_line = end

        if tuning_converged(state.residual_H, state.residual_phi, tolerance):
            payload = dict(data)
            payload.update(
                {
                    "normalization": float(norm),
                    "V_ref": float(exp(rm.log_V(phi_ref))),
                    "phi_ref": float(phi_ref),
                    "phi_0": float(state.phi_0),
                    "pi_0": float(state.pi_0),
                    "tracking": float(state.pi_0 / (1.5 * state.phi_0)),
                    "residual_H": float(state.residual_H),
                    "residual_phi": float(state.residual_phi),
                    "tuning_iterations": k + 1,
                    "plain_steps": steps["plain"],
                    "secant_steps": steps["secant"],
                    "guard_steps": steps["guard"],
                    "seed_kind": str(seed.kind),
                    "seed_normalization": float(seed.normalization),
                    "seed_phi_ref": float(seed.phi_ref),
                }
            )
            # the sample masses of the accepted integration, with its realization (one-loop-drain prompt 03
            # §1.5, P31): once per stored history, never on a tuning integration
            payload["log_m_eff"], payload["log_m_tracker"] = sample_masses(
                rm, cosmology, payload["sample"]
            )
            return payload

        if scale is None:
            scale = state.phi_min

        # Eq. 7 has no positive V_target: halve V(phi_0) and move phi_ref to phi_min (a guard
        # step), and forget the step's matrix. Ruling 7 of orchestrator note 06 fixes the order
        if state.V_target <= 0.0:
            guards_running += 1
            if guards_running >= TUNING_GUARD_LIMIT:
                return _tuning_failure(
                    task_label,
                    f"no normalization reaches H0: V_target <= 0 at {TUNING_GUARD_LIMIT} "
                    f"integrations running, the last at integration {k}",
                    drain=data.get("drain"),
                )
            next_norm = model.renormalize(norm, state.phi_0, 0.5 * exp(state.log_V))
            next_phi_ref = state.phi_min
            stepper.guard()
            next_kind = "guard"
            next_reset = False
        else:
            guards_running = 0
            next_kind = None

        if k + 1 >= max_integrations:
            return _tuning_failure(
                task_label,
                f"the normalization did not converge: after {k + 1} integrations, "
                f"H/H0-1={state.residual_H:+.3e}, "
                f"phi_ref/phi_min-1={state.residual_phi:+.3e}",
                drain=data.get("drain"),
            )

        if next_kind is None:
            x = (model.normalization_coordinate(norm), phi_ref / scale)
            G = (
                model.normalization_coordinate(
                    model.renormalize(norm, state.phi_0, state.V_target)
                ),
                state.phi_min / scale,
            )
            x_new, next_kind = stepper.step(x, G)
            next_reset = stepper.reset
            next_norm = model.normalization_from_coordinate(x_new[0])
            next_phi_ref = x_new[1] * scale

        norm, phi_ref, kind, reset = next_norm, next_phi_ref, next_kind, next_reset
        steps[kind] += 1
        k += 1


@ray.remote
def compute_scalar_history(
    cosmology: LambdaCDM_GenericEOS,
    model: AbstractModel,
    T_init: TemperatureLike,
    phi_init: FieldLike,
    pi_init: FieldLike,
    z_grid: redshift_array,
    task_label: str = "compute_scalar_history",
    atol: float = DEFAULT_ABS_TOLERANCE,
    rtol: float = DEFAULT_REL_TOLERANCE,
    tolerance: float = DEFAULT_TUNING_TOLERANCE,
    max_integrations: int = DEFAULT_TUNING_MAX_INTEGRATIONS,
    verbose: bool = False,
    alpha: float = 0.0,
    drain_law: str = DRAIN_LAW_FIXED,
) -> dict:
    """
    The stored path (jordan-normalization prompt 07, S4): the Ray task that ScalarHistory.compute
    submits, and so the one that computes every history main.py stores. It tunes the history with
    tune_history (prompt 06), which integrates it to T_J = T_CMB with integrate_history until
    H_J(T_CMB) = H0 and phi_ref = phi_min(T_CMB), and returns tune_history's payload: the final
    integration's, with the keys of TUNING_OUTPUT_KEYS added, or a failure.

    A caller that wants one integration of a realized model to a given T_stop, untuned, calls
    integrate_history, which has this function's former signature.

    :param cosmology: background cosmology
    :param model: the model (an AbstractModel); its normalization and phi_ref are the tuning's
    :param T_init: initial radiation temperature in Jordan frame
    :param phi_init: initial Einstein-frame scalar field value phi
    :param pi_init: initial Einstein-frame scalar field derivative dphi/dN
    :param z_grid: the sampling axis: points ln(1 + z_E), counted back from the end of the history
    :param task_label: label for the ray task
    :param atol: absolute tolerance for the ODE solver
    :param rtol: relative tolerance for the ODE solver
    :param tolerance: the tolerance on both residuals of the tuning
    :param max_integrations: the cap on integrations, the seed's included
    :param alpha: the fraction of the condensate's kinetic energy drained at each candidate, passed to
                  tune_history (inelastic-reflection prompt 09); 0.0, the default, is natural mode,
                  the old code bitwise. Under the one-loop law it is A
    :param drain_law: the drain law, passed to tune_history (one-loop-drain prompt 03 §1.2): the law of the
                      history's alpha_value's drain_model, which ScalarHistory.compute passes. The default,
                      DRAIN_LAW_FIXED, is the call of before, bitwise
    :return: tune_history's payload
    """
    return tune_history(
        cosmology=cosmology,
        model=model,
        T_init=T_init,
        phi_init=phi_init,
        pi_init=pi_init,
        z_grid=z_grid,
        task_label=task_label,
        atol=atol,
        rtol=rtol,
        tolerance=tolerance,
        max_integrations=max_integrations,
        verbose=verbose,
        alpha=alpha,
        drain_law=drain_law,
    )


def build_extra_data(data: dict) -> Optional[dict]:
    """
    Assemble the ScalarHistory's extra_data dictionary from the dictionary that
    compute_scalar_history returns. Returns None if there is nothing to store.
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

    # the guard's retakes (reflection-restart prompt 03 §1.2), only when above 0, so that a history the
    # guard never touches stores what it stored before; read with .get, so that a payload without the
    # key does too
    restart_retakes = data.get("restart_retakes", 0)
    if restart_retakes > 0:
        extra_data["restart_retakes"] = restart_retakes

    largest_RHS_values = data["largest_RHS_values"]
    smallest_RHS_values = data["smallest_RHS_values"]
    mean_RHS_values = data["mean_RHS_values"]

    if largest_RHS_values is not None:
        extra_data["largest_RHS_values"] = largest_RHS_values._asdict()
    if smallest_RHS_values is not None:
        extra_data["smallest_RHS_values"] = smallest_RHS_values._asdict()
    if mean_RHS_values is not None:
        extra_data["mean_RHS_values"] = mean_RHS_values._asdict()

    # what of the relativistic margins is not a column (inelastic-reflection prompt 09; README §8.1
    # R13, R25): each kind's first failure, as MarginFailure._asdict() or None, the worst generations'
    # indices, the counts per estimate and the weighted and below-weight shares. Only where the payload
    # carries a Margins record (dissipation mode on a model with a wall, the margins computed); read
    # with .get, so that a payload without the key stores what it stored before
    margins = data.get("margins")
    if margins is not None:

        def failure(record: Optional[MarginFailure]) -> Optional[dict]:
            return dict(record._asdict()) if record is not None else None

        extra_data["margins"] = {
            "first_birth_failure": failure(margins.first_birth_failure),
            "first_reflection_failure": failure(margins.first_reflection_failure),
            "first_tracker_failure": failure(margins.first_tracker_failure),
            "worst_birth_margin_index": margins.worst_birth_margin_index,
            "worst_reflection_margin_index": margins.worst_reflection_margin_index,
            "worst_tracker_margin_index": margins.worst_tracker_margin_index,
            "classical_generations": margins.classical_generations,
            "one_loop_generations": margins.one_loop_generations,
            "classical_gap_generations": margins.classical_gap_generations,
            "weighted_generations": margins.weighted_generations,
            "unassessed_candidates": margins.unassessed_candidates,
            "weighted_share": margins.weighted_share,
            "below_weight_share": margins.below_weight_share,
        }

    return extra_data if len(extra_data) > 0 else None


class ScalarHistory(DatastoreObject):
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
        phi_Einstein_init: phi_value,  # initial value of Einstein-frame scalar phi
        pi_Einstein_init: pi_value,  # initial value of dphi/dN
        model: AbstractModel,
        tuning_tol: tolerance,
        alpha: alpha_value,
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
        :param phi_Einstein_init: initial Einstein-frame scalar field value
        :param pi_Einstein_init: initial Einstein-frame scalar field derivative dphi/dN
        :param model: the model (jordan-normalization prompt 07): V and A with their parameters;
                      the history's normalization and phi_ref are found by tuning
        :param tuning_tol: the tolerance on both residuals of the tuning
        :param alpha: the fraction of the condensate's kinetic energy drained at each candidate, a
                      key of the history (inelastic-reflection prompt 09; README §6.1 U3); its
                      value 0 is natural mode
        :param atol: absolute tolerance for the ODE solver
        :param rtol: relative tolerance for the ODE solver
        :param z_grid: grid of redshifts at which to sample the solution (optional)
        :param label: optional label for the model
        :param tags: optional list of tags for the model
        """
        self._solver_labels = solver_labels

        self._T_Jordan_init: temperature = T_Jordan_init

        self._phi_Einstein_init: phi_value = phi_Einstein_init
        self._pi_Einstein_init: pi_value = pi_Einstein_init

        self._model: AbstractModel = model
        self._tuning_tol: tolerance = tuning_tol
        self._alpha: alpha_value = alpha

        # built from the model and the tuning on first access to realized_model
        # (jordan-normalization prompt 07)
        self._realized_model: Optional[RealizedModel] = None

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
            self._fixed_T_values = None
            self._tuning = None
            self._dissipation = None
            self._drain_history = None
            self._bounces = None

        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._metadata: Optional[IntegrationData] = payload["metadata"]
            self._solver: Optional[IntegrationSolver] = payload["solver"]
            self._values: Optional[List[ScalarHistoryValue]] = payload["values"]
            self._extra_data: Optional[Dict[str, Any]] = payload["extra_data"]
            self._failure: Optional[bool] = payload["failure"]
            self._failure_reason: Optional[str] = payload["failure_reason"]
            self._first_bounce: Optional[FirstBounce] = payload["first_bounce"]
            self._fixed_T_values: Optional[FixedTValues] = payload["fixed_T_values"]
            self._tuning: Optional[TuningValues] = payload["tuning"]
            self._dissipation: Optional[DissipationValues] = payload["dissipation"]
            # the history's drain record and its bounce rows (one-loop-drain prompt 03 §1.4, §1.6); the rows are
            # None under _do_not_populate and on a failure, as the values are
            self._drain_history: Optional[DrainHistoryValues] = payload["drain_history"]
            self._bounces: Optional[Tuple[BounceRow, ...]] = payload["bounces"]

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
        return self._model.shard_key

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
                f"ScalarHistory ({self._label}): failure_reason has not yet been populated"
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
        """
        The Jordan-frame temperature at which the history ends: the cosmology's T_CMB, the end of
        every stored history (README §6.0 R8) and the float tune_history stops at. It is neither a
        parameter, a column nor part of the key (jordan-normalization prompt 07, S2), and is not
        a stored row (store id 0). It exists because compute_BBN_data's pre-check reads it.
        """
        units: UnitsLike = self._cosmology.units
        return temperature(0, self._cosmology.T_CMB_Kelvin * units.Kelvin)

    @property
    def phi_Einstein_init(self) -> phi_value:
        return self._phi_Einstein_init

    @property
    def pi_Einstein_init(self) -> pi_value:
        return self._pi_Einstein_init

    @property
    def model(self) -> AbstractModel:
        return self._model

    @property
    def tuning_tol(self) -> tolerance:
        return self._tuning_tol

    @property
    def alpha(self) -> alpha_value:
        """The drained fraction alpha, a key of the history (inelastic-reflection prompt 09)."""
        return self._alpha

    @property
    def dissipation(self) -> Optional[DissipationValues]:
        """
        The whole-history values of dissipation mode (inelastic-reflection prompt 09, §1.3), set by
        store() from the payload, or by the factory's build from the row's columns, so that they are
        readable on an object built with _do_not_populate, as `tuning` is. ln T_J is in the
        cosmology's units. None on a failure row; raises when not populated.
        """
        if self._failure is None:
            raise RuntimeError("dissipation has not yet been populated")

        return self._dissipation

    @property
    def drain_history(self) -> Optional[DrainHistoryValues]:
        """
        The history's drain record (one-loop-drain prompt 03 §1.4): the regime shares and counts, ell_min, the
        least ln R at a drain, the DrainRecord's counts, the all-generation margins and the number of bounce
        rows; set by store() from the payload (drain_history_values), or by the factory's build from the row's
        columns, so that it is readable on an object built with _do_not_populate, as `dissipation` is. Under
        the one-loop law only: None under the fixed law, at alpha = 0 and on a failure row; raises when not
        populated.
        """
        if self._failure is None:
            raise RuntimeError("drain_history has not yet been populated")

        return self._drain_history

    @property
    def bounces(self) -> Tuple[BounceRow, ...]:
        """
        The history's bounce rows (one-loop-drain prompt 03 §1.3, §1.6), a tuple of BounceRow in ordinal order,
        in the cosmology's units; empty under the fixed law and at alpha = 0. Read with the samples, so it
        raises under _do_not_populate, on a failure row and when not populated, as `values` does.
        """
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarHistory: attempt to read bounces, but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._bounces is None:
            raise RuntimeError("bounces has not yet been populated")
        return self._bounces

    @property
    def tuning(self) -> TuningValues:
        """
        What the tuning found (jordan-normalization prompt 07, S3): the keys of
        TUNING_OUTPUT_KEYS, set by store() from tune_history's payload, or by the factory's build
        from the row. Stored on the ScalarHistory row, so it is readable on an object built with
        _do_not_populate. Raises on a failure row, as first_bounce does, and when not populated.
        """
        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._failure is None or self._tuning is None:
            raise RuntimeError("tuning has not yet been populated")

        return self._tuning

    @property
    def realized_model(self) -> RealizedModel:
        """
        The realized model the history was computed with (jordan-normalization prompt 07, S3):
        model.realize(normalization, phi_ref), at the normalization and phi_ref the tuning found.
        Built on first access and cached, not in __init__: an object built for a lookup may never
        need it. Raises where `tuning` does.
        """
        if self._realized_model is None:
            tuning: TuningValues = self.tuning
            self._realized_model = self._model.realize(
                tuning.normalization, tuning.phi_ref
            )
        return self._realized_model

    @property
    def metadata(self) -> IntegrationData:
        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._metadata is None:
            raise RuntimeError("metadata values have not yet been populated")

        return self._metadata

    @property
    def extra_metadata(self) -> Optional[Dict[str, Any]]:
        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
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
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._failure is None:
            raise RuntimeError("first_bounce has not yet been populated")

        return self._first_bounce

    @property
    def fixed_T_values(self) -> FixedTValues:
        """
        phi and rho_NP/rho_R,J at T_J = 1 MeV and 70 keV (fixed_T_values()); a field is None
        where the history did not reach that temperature. Stored on the ScalarHistory row, so it
        is readable on an object built with _do_not_populate. Raises on a failure row, as
        first_bounce does. (science-readiness prompt 06b)
        """
        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._failure is None:
            raise RuntimeError("fixed_T_values has not yet been populated")

        return self._fixed_T_values

    @property
    def solver(self) -> IntegrationSolver:
        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._solver is None:
            raise RuntimeError("solver has not yet been populated")
        return self._solver

    @property
    def values(self) -> List:
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarHistory: attempt to read values, but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._values is None:
            raise RuntimeError("values has not yet been populated")
        return self._values

    @property
    def functions(self) -> ModelFunctions:
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarHistory: attempt to call functions(), but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
            )

        if self._values is None:
            raise RuntimeError("values has not yet been populated")

        if self._functions is None:
            self._create_functions()

        return self._functions

    def _create_functions(self):
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarHistory: attempt to call _create_functions(), but _do_not_populate is set"
            )

        if self._failure:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): this object had an integration failure and cannot be used"
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

    def compute(
        self,
        label: Optional[str] = None,
        max_integrations: int = DEFAULT_TUNING_MAX_INTEGRATIONS,
    ):
        """
        Submit compute_scalar_history for this history and return its reference.

        :param label: the task's label, replacing the history's own if given
        :param max_integrations: the cap on the tuning's integrations, the seed's included. It is
            not a key of the history (the tuning tolerance is): a history that does not converge
            within it is stored as a failure under the same key, whatever the cap
            (jordan-normalization prompt 08)
        """
        if hasattr(self, "_do_not_populate"):
            raise RuntimeError(
                "ScalarHistory: attempt to call compute(), but _do_not_populate is set"
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
        check_required_parameter("_phi_Einstein_init")
        check_required_parameter("_pi_Einstein_init")
        check_required_parameter("_model")
        check_required_parameter("_tuning_tol")
        check_required_parameter("_alpha")
        check_required_parameter("_target_z_grid")

        # replace label if specified
        if label is not None:
            self._label = label

        self._compute_ref = compute_scalar_history.remote(
            self.cosmology,
            self._model,
            self.T_Jordan_init,
            self.phi_Einstein_init,
            self.pi_Einstein_init,
            self._target_z_grid,
            task_label=(self._label if self._label is not None else self._model.name),
            atol=self._atol.tol,
            rtol=self._rtol.tol,
            tolerance=self._tuning_tol.tol,
            max_integrations=max_integrations,
            alpha=self._alpha.value,
            # the law of the key's drain model (one-loop-drain prompt 03 §1.2): the key is on alpha, so the
            # history takes no argument of its own
            drain_law=self._alpha.drain_law,
        )
        return self._compute_ref

    def store(self) -> Optional[bool]:
        if self._compute_ref is None:
            raise RuntimeError(
                "ScalarHistory: store() called, but no compute() is in progress"
            )

        # check whether the computation has actually resolved
        resolved, unresolved = ray.wait([self._compute_ref], timeout=0)

        # if not, return None
        if len(resolved) == 0:
            return None

        # retrieve result and populate ourselves
        data = ray.get(self._compute_ref)

        failure: bool = data.get("failure", False)

        # the guard (one-loop-drain prompt 03 §1.2; README §8.11 P28; orchestrator note 03, ruling 3): a success's
        # drain model must be its key's, checked before anything is stored or set on the object. A "drain"
        # record whose model differs is refused; a missing or None record is accepted under the fixed model
        # (a stand-in payload) and refused under the one-loop model. A failure is stored as before
        if not failure:
            key_model: str = self._alpha.drain_model
            drain: Optional[DrainRecord] = data.get("drain")
            if drain is None:
                if key_model != DRAIN_MODEL_FIXED:
                    raise RuntimeError(
                        f"ScalarHistory ({self._label}): the payload carries no drain record, but the "
                        f"history's key names the drain model {key_model!r}"
                    )
            elif drain.model != key_model:
                raise RuntimeError(
                    f"ScalarHistory ({self._label}): the payload was computed under the drain model "
                    f"{drain.model!r}, but the history's key names {key_model!r}"
                )

        self._compute_ref = None

        if failure:
            self._failure = True
            self._failure_reason = (
                str(data.get("failure_reason", ""))[:DEFAULT_STRING_LENGTH] or None
            )
            self._first_bounce = None
            self._fixed_T_values = None
            self._tuning = None
            self._dissipation = None
            self._drain_history = None
            self._bounces = ()
            self._values = []
            return True

        self._failure = False
        self._failure_reason = None
        self._first_bounce = data["first_bounce"]
        self._fixed_T_values = data["fixed_T_values"]
        # what the tuning found (jordan-normalization prompt 07, S3)
        self._tuning = TuningValues(**{key: data[key] for key in TUNING_OUTPUT_KEYS})
        self._metadata = data["metadata"]

        # the whole-history values of dissipation mode (inelastic-reflection prompt 09); each read
        # with .get, so that a key the payload does not carry stores NULL (orchestrator note 09,
        # ruling 5)
        self._dissipation = dissipation_values(data)

        # the history's drain record and its bounce rows (one-loop-drain prompt 03 §1.3, §1.4): both from the
        # payload's "bounces", None and () where it is None or absent (the fixed law, alpha = 0)
        self._drain_history = drain_history_values(data)
        record: Optional[BounceRecord] = data.get("bounces")
        self._bounces = () if record is None else tuple(record.rows)

        sample: List[SampleValues] = data["sample"]
        z_grid: redshift_array = data["z_grid"]

        # r_chi at each sample (P27): a payload with samples must carry it, and no default is taken
        ratios = data.get("rho_chi_over_rho_rad")
        if ratios is None and len(sample) > 0:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): the payload has {len(sample)} samples but no "
                f'"rho_chi_over_rho_rad"'
            )

        # the sample masses (one-loop-drain prompt 03 §1.5): a payload with samples must carry both, as it must
        # carry the ratio; an entry may be None (the model call failed there)
        masses_eff = data.get("log_m_eff")
        masses_tracker = data.get("log_m_tracker")
        if (masses_eff is None or masses_tracker is None) and len(sample) > 0:
            raise RuntimeError(
                f"ScalarHistory ({self._label}): the payload has {len(sample)} samples but no "
                f'"log_m_eff" or no "log_m_tracker"'
            )

        extra_data = build_extra_data(data)
        if extra_data is not None:
            self._extra_data = extra_data

        self._values = []
        for i in range(len(sample)):
            self._values.append(
                ScalarHistoryValue(
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
                    log_one_plus_z_Jordan=sample[i].log_one_plus_z_Jordan,
                    rho_chi_over_rho_rad=ratios[i],
                    log_m_eff=masses_eff[i],
                    log_m_tracker=masses_tracker[i],
                )
            )

        self._solver = self._solver_labels[data["solver_label"]]

        return True


class ScalarHistoryValue(DatastoreObject):
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
        log_one_plus_z_Jordan: float,
        *,
        rho_chi_over_rho_rad: float,
        log_m_eff: Optional[float] = None,
        log_m_tracker: Optional[float] = None,
    ):
        """
        :param store_id: ID in the datastore
        :param z: the point of the sampling axis, ln(1 + z_E) counted back from the end
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
        :param log_one_plus_z_Jordan: ln(1 + z_J), the Jordan-frame redshift counted back from the
            end of the history, ln(1 + z_E) + ln A(phi_0) - ln A(phi) (jordan-normalization
            prompt 03, R4). A column, never a key: H_J < 0 during a bounce, so a_J is not
            monotonic and z_J is not a coordinate. Once prompt 07 lands, "the end" is
            T_J = T_CMB for every stored history
        :param rho_chi_over_rho_rad: r_chi = rho_chi/rho_R, the ratio of the dark radiation to the
            bath that the policy used at the sample, exactly 0.0 at alpha = 0
            (inelastic-reflection prompt 09, P27). A required keyword with no default, so that no
            caller passes a dissipation history's samples with the ratio silently zero
        :param log_m_eff: ln m_eff of the field itself at the sample, in the cosmology's units, or None where
            the model call failed (m_eff^2 <= 0); sample_masses (one-loop-drain prompt 03 §1.5)
        :param log_m_tracker: ln m_tr of the zero-lag tracker at the sample, or None where the model call
            failed (no tracker minimum). Both default to None, for the objects that tools build from a payload
            and never store; ScalarHistory.store passes both, and refuses a payload without them
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

        self._log_one_plus_z_Jordan: float = log_one_plus_z_Jordan

        self._rho_chi_over_rho_rad: float = rho_chi_over_rho_rad

        self._log_m_eff: Optional[float] = log_m_eff
        self._log_m_tracker: Optional[float] = log_m_tracker

    @property
    def shard_key(self) -> ShardKeyType:
        # should not get called individually, since serialization is handled by the parent ScalarHistory
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

    @property
    def log_one_plus_z_Jordan(self) -> float:
        return self._log_one_plus_z_Jordan

    @property
    def rho_chi_over_rho_rad(self) -> float:
        return self._rho_chi_over_rho_rad

    @property
    def log_m_eff(self) -> Optional[float]:
        """ln m_eff of the field at the sample (one-loop-drain prompt 03 §1.5); None where it is not defined."""
        return self._log_m_eff

    @property
    def log_m_tracker(self) -> Optional[float]:
        """ln m_tr of the zero-lag tracker at the sample (one-loop-drain prompt 03 §1.5); None where it is not
        defined."""
        return self._log_m_tracker


class ScalarHistoryProxy:
    def __init__(self, model: ScalarHistory):
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

    def get(self) -> ScalarHistory:
        """
        The return value should only be held locally and not persisted, otherwise the entire
        ScalarHistory instance may be serialized when it is passed around by Ray.
        That would defeat the purpose of the proxy.
        :return:
        """
        return ray.get(self._ref)
