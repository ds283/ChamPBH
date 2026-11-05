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
The model interface (jordan-normalization prompt 04, M1 and M2; README §6.0 R6, R7).

A model is a Lagrangian: the potential V(phi) and the conformal factor A(phi) of g_J = A^2 g_E,
with the parameters that define them. It is one stored object (jordan-normalization prompt 07
gives each model type its table). Two quantities are not parameters of the model but of each
history, which finds them by tuning (prompt 06):
  - the normalization of the potential, which the model interprets as it wishes (R7): the
    exponential model reads it as ln V_0, a multiplicative normalization;
  - the reference point phi_ref at which A(phi_ref) = 1, so that M_P is the laboratory Planck
    mass.

`AbstractModel.realize(normalization, phi_ref)` binds the two and returns a `RealizedModel`, the
one object the integrator and everything downstream of it read. A(phi_ref) = 1 is the base
class's: `RealizedModel.log_Omega(phi)` is `model._log_Omega(phi) - model._log_Omega(phi_ref)`, so
no model interprets phi_ref, and a model that forgets it cannot break it.

A model implements hooks that take no phi_ref:
  - `_log_V(normalization, phi)`, `_d_logV_dphi(normalization, phi)`,
    `_d2_logV_dphi2(normalization, phi)`;
  - `_log_Omega(phi)`, `_d_logOmega_dphi(phi)`, `_d2_logOmega_dphi2(phi)`;
  - optionally `_log_V_floor(normalization)` and the property `reflects_at_origin`, which the
    integrator's elastic reflection reads (defaults: None and False, as on the potentials).
A model that the tuning layer drives also implements `renormalize`, `normalization_coordinate` and
`normalization_from_coordinate`; the base class raises NotImplementedError for them.

The wall and the minimum (inelastic-reflection prompt 04; README §8.1 R9): `classical_turning_point`,
`one_loop_turnaround`, `minimum_on_trace` and `d_minimum_d_log_trace` are generic defaults built on
the hooks alone, which a model may override with a closed form, as `effective_minimum` is built.
None assumes a form of the potential.
"""

import sys
from abc import ABC, abstractmethod
from collections import namedtuple
from math import exp, expm1, isfinite, isnan, log, nextafter, pi, ulp
from typing import Callable, List, Optional, Tuple

from scipy.optimize import brentq

from datastorekit import DatastoreObject
from Units.base import UnitsLike
from config.sharding import ShardKeyType

# the seed of a history's tuning (README §6.2 P5): which analytic estimate it came from, the
# normalization and phi_ref. kind is one of SEED_KINDS
Seed = namedtuple("Seed", ["kind", "normalization", "phi_ref"])

SEED_KINDS = ("tracking", "static", "plain")

# the classical turning point of a field falling into a reflecting wall with kinetic energy E
# (inelastic-reflection prompt 04 §1.2; wall_backreaction.tex §"Where the field is stopped" and
# §"The chain of estimates"): phi_t, where V(phi_t) - V_floor = E; ln omega_t^2 = ln V''(phi_t);
# ln R, the test-field ratio omega_t^4/8 pi^2 E; lambda_t = -d ln V/dphi at phi_t; ln k_ex, the
# classical peak lambda_t E^{1/2}/2; and ell_t = ln(k_ex/omega_t)
ClassicalTurningPoint = namedtuple(
    "ClassicalTurningPoint",
    ["phi_t", "log_omega_t_squared", "log_R", "lambda_t", "log_k_ex", "ell_t"],
)

# the one-loop turnaround (prompt 04 §1.3; eq:chain-turn): phi_ta, where V''(phi_ta) = m_ta^2 with
# m_ta^4 ell = 32 pi^2 E; ln m_ta; lambda_ta = -d ln V/dphi at phi_ta; ln k_ex = ln(lambda_ta E^{1/2}/2);
# ell = ln(k_ex/m_ta) = ln k_ex - ln m_ta; the number of iterations of the fixed point; and
# outside_classical, phi_ta > phi_t
OneLoopTurnaround = namedtuple(
    "OneLoopTurnaround",
    [
        "phi_ta",
        "log_m_ta",
        "lambda_ta",
        "log_k_ex",
        "ell",
        "iterations",
        "outside_classical",
    ],
)

# the root-finding of the wall and the minimum (prompt 04 §1.5, orchestrator note 04 rulings 2, 3).
# Every brentq in x = ln phi uses rtol = 4 eps (SciPy's floor) and xtol = 1e-14. Prompt §1.5's
# rtol of 1e-13 allows about 7e-12 in ln phi at phi ~ 1e-32 M_P (|ln phi| ~ 74), and ln R inherits
# that times 2(y + 4), which the orchestrator measured at 1.3e-10 against the exponential closed
# form, above the test's 1e-10 (ruling 2)
WALL_ROOT_RTOL = 4.0 * sys.float_info.epsilon
WALL_ROOT_XTOL = 1.0e-14
# the search doubles its step in ln phi from the field, at most this many times (a backstop: a probe
# leaves float range long before), then halves the last bracket WALL_SEARCH_HALVINGS times, each
# midpoint checked for monotonicity, and further, at most WALL_SEARCH_MAX_DOUBLINGS times, while the
# outer end's value is -inf, which brentq's interpolation cannot use
WALL_SEARCH_MAX_DOUBLINGS = 64
WALL_SEARCH_HALVINGS = 4
# the width of ln V's rounding (inelastic-reflection prompt 07b; log 07b §1): _log_V_minus_floor is
# -inf where ln V - ln V_floor <= K ulp(max(|ln V|, |ln V_floor|)). Two finite probes 2^-H e-folds
# apart (H = WALL_SEARCH_HALVINGS, a halving midpoint against an end, the closest the search
# compares) are ordered if (e^{s 2^-H} - 1)(K + 1 - rho) > 2 rho, for a model whose ln V - ln V_floor
# is rounded within rho quanta and has |d ln(ln V - ln V_floor)/d ln phi| >= s. The exponential
# (s = n = 1, rho = 1/2) needs K > 15.005; K = 16 is the next integer, with no margin beyond its 6 %
WALL_ROUNDING_QUANTA = 16
# the one-loop fixed point: from ell_0 = 32 (eq:chain-turn) until |delta ell| <= 1e-12 ell
ONE_LOOP_ELL_START = 32.0
ONE_LOOP_ELL_RTOL = 1.0e-12
ONE_LOOP_MAX_ITERATIONS = 100

_LOG_2 = log(2.0)
_LOG_8_PI_SQUARED = log(8.0 * pi * pi)
_LOG_32_PI_SQUARED = log(32.0 * pi * pi)
_SMALLEST_NORMAL = sys.float_info.min
_NEG_INF = float("-inf")


def _failure(message: str):
    """
    A ComputationFailureError with the message. It is imported here, not at the top of the module:
    ComputeTargets.exceptions initializes the ComputeTargets package, whose AdiabaticHistory and
    ScalarHistory import RealizedModel from this module, so a top-level import would make
    `import CosmologyConcepts.Models` fail in a fresh interpreter (orchestrator note 04, ruling 1).
    """
    from ComputeTargets.exceptions import ComputationFailureError

    return ComputationFailureError(message)


def _log_V_minus_floor(log_V: float, log_V_floor: float) -> float:
    """
    ln(V - V_floor) from ln V and ln V_floor; -inf where V is below V_floor, or V rounds to V_floor,
    or lies within K quanta of it: ln V - ln V_floor <= K ulp(max(|ln V|, |ln V_floor|)), K =
    WALL_ROUNDING_QUANTA (inelastic-reflection prompt 07b). A difference that small is not resolved
    by ln V's rounding. ln V = +inf has an infinite width and is not within it: it returns +inf.
    """
    if not log_V > log_V_floor:
        return _NEG_INF
    width = WALL_ROUNDING_QUANTA * ulp(max(abs(log_V), abs(log_V_floor)))
    if log_V - log_V_floor <= width < float("inf"):
        return _NEG_INF
    return log_V + log(-expm1(log_V_floor - log_V))


# the least finite value of _log_V_minus_floor at one ln V_floor, cached per (ln V_floor,
# WALL_ROUNDING_QUANTA) (one-loop-drain prompt 01 §1.6): a history has one ln V_floor, a tuning a few.
# The walk up from ln V_floor takes WALL_ROUNDING_QUANTA + 1 steps where the floats above ln V_floor
# have its ulp (every ln V_floor of the grid, near -276.6), and about twice that where ln V_floor is
# minus a power of two, whose upper neighbours have half its ulp; it is bounded at
# _LEAST_FINITE_MAX_STEPS. The cache is cleared when it reaches _LEAST_FINITE_CACHE_SIZE entries
_LEAST_FINITE_MAX_STEPS = 4 * (WALL_ROUNDING_QUANTA + 2)
_LEAST_FINITE_CACHE_SIZE = 4096
_LEAST_FINITE_CACHE: dict = {}


def _least_finite_log_V_minus_floor(log_V_floor: float) -> float:
    """
    The least finite value that _log_V_minus_floor(., log_V_floor) can return, for a finite ln V_floor
    (one-loop-drain prompt 01 §1.6; README §8.8 P21): its value at the first float above ln V_floor at
    which it is finite, found by stepping up from ln V_floor through the representable floats
    (nextafter), and computed by _log_V_minus_floor itself, so that it is exact by construction.

    It is the least value, not only the first: above the first finite argument ln V - ln V_floor grows
    by at least one ulp of ln V_floor a step, ln(-expm1(ln V_floor - ln V)) with it, and the sum
    ln V + ln(-expm1(...)) is rounded monotonically, so _log_V_minus_floor does not decrease (given
    monotone expm1 and log, which the test of this function checks over 10^4 floats). Below the first
    finite argument it is -inf.

    Raises ComputationFailureError if no finite value is found within _LEAST_FINITE_MAX_STEPS steps.
    """
    key = (log_V_floor, WALL_ROUNDING_QUANTA)
    least = _LEAST_FINITE_CACHE.get(key)
    if least is not None:
        return least

    log_V = log_V_floor
    for _ in range(_LEAST_FINITE_MAX_STEPS):
        log_V = nextafter(log_V, float("inf"))
        value = _log_V_minus_floor(log_V, log_V_floor)
        if value > _NEG_INF:
            least = value
            break
    else:
        raise _failure(
            f"_least_finite_log_V_minus_floor: no finite ln(V - V_floor) within "
            f"{_LEAST_FINITE_MAX_STEPS} floats above ln V_floor = {log_V_floor!r}"
        )

    if len(_LEAST_FINITE_CACHE) >= _LEAST_FINITE_CACHE_SIZE:
        _LEAST_FINITE_CACHE.clear()
    _LEAST_FINITE_CACHE[key] = least
    return least


def _turning_point_unresolvable(log_V_floor: float, log_E: float) -> bool:
    """
    True iff ln E is below the least finite value that _log_V_minus_floor(., log_V_floor) can return
    (one-loop-drain prompt 01 §1.6; README §8.8 P21, the user's ruling "Add the wall-search fix to
    one-loop-drain 01"): the classical turning point, the root of V(phi_t) - V_floor = E, lies inside
    the region where _log_V_minus_floor returns -inf (within WALL_ROUNDING_QUANTA quanta of ln V_floor),
    below the resolution of ln V, and the search cannot resolve it. ln V_floor and ln E are finite (the
    caller's input checks).

    Why every input it accepts raised in classical_turning_point's search before this prompt. The
    search's residual is _log_V_minus_floor(ln V(phi), ln V_floor) - ln E. Where the predicate holds,
    every finite value of _log_V_minus_floor is at least its least value, which is above ln E, so every
    finite residual is strictly positive (a float difference a - b with a > b is positive), and every
    other residual is -inf (+inf, at ln V = +inf, is refused by _evaluate). classical_turning_point
    returns None first where r_0 >= 0, so the search starts with r_0 = -inf. Then in _search_log_field:
      - the doubling loop keeps the outer end at a probe whose residual is -inf, and breaks at the first
        positive one (or raises: no bracket in WALL_SEARCH_MAX_DOUBLINGS, a probe out of float range);
      - every halving midpoint is -inf, which becomes the outer end, or positive, which becomes the inner
        one if it is below the inner end's value and otherwise raises "not monotone"; the outer end's
        value therefore stays -inf, no residual is ever exactly 0.0, and the halvings run to their limit;
      - after them the outer end is -inf, and the search raises "the bracket's outer end is still -inf".
    No path returns. So classical_turning_point's return of None where this holds changes only calls
    that raised, and none that returned.
    """
    return log_E < _least_finite_log_V_minus_floor(log_V_floor)


def _evaluate(residual: Callable[[float], float], x: float, context: str) -> float:
    """
    residual(phi) at phi = e^x. A phi that is not a positive normal float, a hook that raises, or a
    value that is NaN or +inf ends the search as a ComputationFailureError naming the probe, with the
    hook's own exception chained (orchestrator note 04, ruling 3). -inf is allowed: it is
    ln(V - V_floor) where V rounds to V_floor, or lies within K quanta of it, or the minimum's
    residual where V' >= 0.
    """
    try:
        phi = exp(x)
    except OverflowError as e:
        raise _failure(
            f"{context}: the search left float range at ln phi = {x!r}"
        ) from e
    if not (_SMALLEST_NORMAL <= phi < float("inf")):
        raise _failure(
            f"{context}: the search left float range at ln phi = {x!r} (phi = {phi!r})"
        )
    try:
        r = residual(phi)
    except Exception as e:
        raise _failure(
            f"{context}: the model raised at phi = {phi!r} (ln phi = {x!r}): {e!r}"
        ) from e
    if isnan(r) or r == float("inf"):
        raise _failure(
            f"{context}: a non-finite value {r!r} at phi = {phi!r} (ln phi = {x!r})"
        )
    return r


def _search_log_field(
    residual: Callable[[float], float],
    x_0: float,
    r_0: float,
    direction: float,
    context: str,
) -> float:
    """
    The root in x = ln phi of residual(e^x) = 0, searched from x_0 in the given direction (-1.0
    inwards, +1.0 outwards), where residual(e^{x_0}) = r_0 < 0 and the residual rises towards 0
    along the search (prompt 04 §1.2, §1.5; orchestrator note 04, rulings 2 and 3).

    - The step starts at one e-fold and doubles until the first probe with residual >= 0, at most
      WALL_SEARCH_MAX_DOUBLINGS times.
    - Monotonicity is checked on values: along the probes from x_0, and at every halving midpoint,
      the residual must rise strictly (repeated -inf allowed). A violation raises, naming the probes.
      Nothing is searched exhaustively.
    - The last bracket is halved WALL_SEARCH_HALVINGS times, and further while its outer end is -inf,
      then brentq with WALL_ROOT_RTOL and WALL_ROOT_XTOL.
    """
    probes: List[Tuple[float, float]] = [(x_0, r_0)]

    def not_monotone(reason: str):
        shown = ", ".join(f"(ln phi = {x:.17g}, {r:.17g})" for x, r in probes)
        return _failure(
            f"{context}: the searched quantity is not monotone along the search ({reason}); "
            f"probes (ln phi, residual) from the start: {shown}"
        )

    x_out, r_out = x_0, r_0
    x_in, r_in = x_0, r_0
    step = 1.0
    for _ in range(WALL_SEARCH_MAX_DOUBLINGS):
        x_in = x_out + direction * step
        r_in = _evaluate(residual, x_in, context)
        probes.append((x_in, r_in))
        if not (r_in > r_out or (r_in == _NEG_INF and r_out == _NEG_INF)):
            raise not_monotone("a probe moved away from the target")
        if r_in >= 0.0:
            break
        x_out, r_out = x_in, r_in
        step *= 2.0
    else:
        raise _failure(
            f"{context}: no bracket within {WALL_SEARCH_MAX_DOUBLINGS} doublings "
            f"(last probe ln phi = {x_in!r}, residual {r_in!r})"
        )

    halvings = 0
    while halvings < WALL_SEARCH_HALVINGS or (
        r_out == _NEG_INF
        and halvings < WALL_SEARCH_HALVINGS + WALL_SEARCH_MAX_DOUBLINGS
    ):
        if r_in == 0.0:
            break
        x_mid = 0.5 * (x_out + x_in)
        r_mid = _evaluate(residual, x_mid, context)
        probes.append((x_mid, r_mid))
        if not ((r_out < r_mid < r_in) or (r_mid == _NEG_INF and r_out == _NEG_INF)):
            raise not_monotone(
                f"a midpoint of the bracket [{x_out!r}, {x_in!r}] is not between its ends"
            )
        if r_mid >= 0.0:
            x_in, r_in = x_mid, r_mid
        else:
            x_out, r_out = x_mid, r_mid
        halvings += 1

    if r_in == 0.0:
        return x_in
    if r_out == _NEG_INF:
        raise _failure(
            f"{context}: the bracket's outer end is still -inf after {halvings} halvings "
            f"(bracket ln phi in [{x_out!r}, {x_in!r}])"
        )

    try:
        return float(
            brentq(
                lambda x: _evaluate(residual, x, context),
                min(x_out, x_in),
                max(x_out, x_in),
                xtol=WALL_ROOT_XTOL,
                rtol=WALL_ROOT_RTOL,
            )
        )
    except (RuntimeError, ValueError) as e:
        raise _failure(
            f"{context}: brentq failed on the bracket ln phi in [{x_out!r}, {x_in!r}]: {e!r}"
        ) from e


class RealizedModel:
    """
    A model with its normalization and its reference point bound (jordan-normalization prompt 04,
    M2): V(phi), the conformal factor A(phi) with A(phi_ref) = 1, and their derivatives. Ω is the
    conformal factor A: the methods keep the integrator's names (log_Omega, Omega, d_logOmega_dphi,
    d2_logOmega_dphi2), so that its expressions are unchanged.

    log_Omega(phi) is model._log_Omega(phi) - model._log_Omega(phi_ref), the latter computed once,
    here; Omega(phi) is exp(log_Omega(phi)). Every other method delegates to the model with nothing
    added. Immutable: the attributes are set in __init__ and read through properties.
    """

    def __init__(self, model: "AbstractModel", normalization, phi_ref: float):
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "_normalization", normalization)
        object.__setattr__(self, "_phi_ref", phi_ref)
        object.__setattr__(self, "_log_Omega_ref", model._log_Omega(phi_ref))

    def __setattr__(self, name, value):
        raise AttributeError(f"RealizedModel is immutable (cannot set {name})")

    def __delattr__(self, name):
        raise AttributeError(f"RealizedModel is immutable (cannot delete {name})")

    @property
    def model(self) -> "AbstractModel":
        return self._model

    @property
    def normalization(self):
        return self._normalization

    @property
    def phi_ref(self) -> float:
        return self._phi_ref

    @property
    def name(self) -> str:
        Mp = self._model.units.PlanckMass
        return (
            f"{self._model.name}[normalization={self._normalization:.6g},"
            f"phi_ref={self._phi_ref / Mp:.6g}Mp]"
        )

    def log_V(self, phi) -> float:
        return self._model._log_V(self._normalization, phi)

    def d_logV_dphi(self, phi) -> float:
        return self._model._d_logV_dphi(self._normalization, phi)

    def d2_logV_dphi2(self, phi) -> float:
        return self._model._d2_logV_dphi2(self._normalization, phi)

    def log_Omega(self, phi) -> float:
        return self._model._log_Omega(phi) - self._log_Omega_ref

    def Omega(self, phi) -> float:
        return exp(self.log_Omega(phi))

    def d_logOmega_dphi(self, phi) -> float:
        return self._model._d_logOmega_dphi(phi)

    def d2_logOmega_dphi2(self, phi) -> float:
        return self._model._d2_logOmega_dphi2(phi)

    @property
    def log_V_floor(self) -> Optional[float]:
        return self._model._log_V_floor(self._normalization)

    def drain_L(self, log_E: float) -> float:
        return self._model._drain_L(self._normalization, log_E)

    @property
    def drain_L_implemented(self) -> bool:
        return self._model.drain_L_implemented

    @property
    def reflects_at_origin(self) -> bool:
        return self._model.reflects_at_origin

    # the wall and the minimum (inelastic-reflection prompt 04): the model's methods at this
    # normalization; phi_ref does not enter (orchestrator note 04, ruling 5)

    def classical_turning_point(
        self, phi: float, log_E: float
    ) -> Optional["ClassicalTurningPoint"]:
        return self._model.classical_turning_point(self._normalization, phi, log_E)

    def one_loop_turnaround(
        self, phi: float, log_E: float
    ) -> Optional["OneLoopTurnaround"]:
        return self._model.one_loop_turnaround(self._normalization, phi, log_E)

    def minimum_on_trace(self, phi_start: float, log_T: float) -> float:
        return self._model.minimum_on_trace(self._normalization, phi_start, log_T)

    def d_minimum_d_log_trace(self, phi_min: float, log_T: float) -> float:
        return self._model.d_minimum_d_log_trace(self._normalization, phi_min, log_T)


class AbstractModel(DatastoreObject, ABC):
    def __init__(self, store_id: int, units: UnitsLike):
        DatastoreObject.__init__(self, store_id)
        self._units: UnitsLike = units

    @property
    def units(self) -> UnitsLike:
        return self._units

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def type_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def shard_key(self) -> ShardKeyType:
        raise NotImplementedError

    # ---- the hooks: V with the normalization, ln A with no phi_ref ----

    @abstractmethod
    def _log_V(self, normalization, phi) -> float:
        raise NotImplementedError

    @abstractmethod
    def _d_logV_dphi(self, normalization, phi) -> float:
        raise NotImplementedError

    @abstractmethod
    def _d2_logV_dphi2(self, normalization, phi) -> float:
        raise NotImplementedError

    @abstractmethod
    def _log_Omega(self, phi) -> float:
        raise NotImplementedError

    @abstractmethod
    def _d_logOmega_dphi(self, phi) -> float:
        raise NotImplementedError

    @abstractmethod
    def _d2_logOmega_dphi2(self, phi) -> float:
        raise NotImplementedError

    def _log_V_floor(self, normalization) -> Optional[float]:
        """
        log of the potential's value far from the wall, or None if not defined; the integrator's
        guard G2 reads it at a reflection (RealizedModel.log_V_floor). Default None.
        """
        return None

    @property
    def reflects_at_origin(self) -> bool:
        """
        True if the potential has a purely repulsive wall in (0, phi) for every phi > 0
        (RealizedModel.reflects_at_origin); the integrator's guard G1 reads it. Default False.
        """
        return False

    def _drain_L(self, normalization, log_E: float) -> float:
        """
        L, the wall variable of the one-loop drain law (one-loop-drain prompt 02c; README §8.10, U15 (b)), for a
        field reaching the wall with energy E (ln E given): RealizedModel.drain_L. The law's fraction was measured,
        and its constants fitted, as a function of L on one family of walls, so L is the model's to define.

        No default, by the user's ruling of 2026-11-05: "I suggest we move `L` into the model class, with no
        default. Then when a new potential is implemented, breakage is loud, and the implementer has to think about
        what they are changing." This base raises, naming the class. It is not abstract, so that a model without
        it is still built and runs under the fixed law; under the one-loop law integrate_scalar_history,
        integrate_history and tune_history refuse such a model at entry (drain_L_implemented).
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement _drain_L(normalization, log_E), the wall variable L "
            f"that the one-loop drain law needs"
        )

    @property
    def drain_L_implemented(self) -> bool:
        """True iff this model's class overrides _drain_L (RealizedModel.drain_L_implemented)."""
        return type(self)._drain_L is not AbstractModel._drain_L

    # ---- the interface ----

    def realize(self, normalization, phi_ref: float) -> RealizedModel:
        """The model with its normalization and reference point bound, A(phi_ref) = 1."""
        return RealizedModel(self, normalization, phi_ref)

    def renormalize(self, normalization, phi: float, V_target: float):
        """
        The frozen-state step (R7): the normalization at which V(phi) = V_target, however the
        model interprets it.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement renormalize"
        )

    def normalization_coordinate(self, normalization) -> float:
        """The scalar on which the tuning layer's secant acts (README §6.2 P4)."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement normalization_coordinate"
        )

    def normalization_from_coordinate(self, x: float):
        """The inverse of normalization_coordinate."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement normalization_from_coordinate"
        )

    def effective_minimum(
        self,
        normalization,
        phi_ref: float,
        phi_0: float,
        rho_m_E_0: float,
        bracket: Optional[Tuple[float, float]] = None,
    ) -> float:
        """
        The minimum of the Einstein-frame effective potential at a history's state today
        (README §6.0 R2; the brief's §3 R2): the root of

            V'(phi) + (d ln A/dphi) rho_m,E(phi_0) A(phi)/A(phi_0),

        with rho_m,E proportional to A at fixed Einstein-frame number density, on the model
        realized at (normalization, phi_ref). The radiation term Sigma rho_R,E is left out: at
        T_CMB, Sigma = 1 - 3 w is 0.0 exactly on QCD_Cosmology(Planck2018).

        brentq on `bracket`, by default (0.05 phi_0, 20 phi_0), with xtol = 1e-14 phi_0 and
        rtol = 1e-13. A bracket on which V_eff' does not change sign raises ValueError naming
        it, and so does one on which V_eff' overflows, as it does at 20 phi_0 once beta phi_0
        is a few M_P: the tuning layer records either as a failed history. A model may override
        this with a closed form.
        """
        rm: RealizedModel = self.realize(normalization, phi_ref)
        log_Omega_0: float = rm.log_Omega(phi_0)

        def dV_eff(x: float) -> float:
            return exp(rm.log_V(x)) * rm.d_logV_dphi(x) + rm.d_logOmega_dphi(
                x
            ) * rho_m_E_0 * exp(rm.log_Omega(x) - log_Omega_0)

        low, high = bracket if bracket is not None else (0.05 * phi_0, 20.0 * phi_0)
        try:
            f_low, f_high = dV_eff(low), dV_eff(high)
            if not ((f_low <= 0.0 <= f_high) or (f_high <= 0.0 <= f_low)):
                raise ValueError(
                    f"effective_minimum ({self.name}): V_eff' does not change sign on the bracket "
                    f"[{low:.6g}, {high:.6g}] (values {f_low:.6g}, {f_high:.6g})"
                )

            return float(brentq(dV_eff, low, high, xtol=1e-14 * phi_0, rtol=1e-13))
        except OverflowError as e:
            raise ValueError(
                f"effective_minimum ({self.name}): V_eff' overflows on the bracket "
                f"[{low:.6g}, {high:.6g}] ({e})"
            ) from e

    # ---- the wall and the minimum (inelastic-reflection prompt 04; README §8.1 R7-R9) ----
    #
    # Generic defaults through the hooks alone, for any potential; a model may override any of them
    # with a closed form. Every quantity that can overflow is carried as a logarithm. E is the
    # condensate's kinetic energy density in the Einstein frame at the infall, |phi_dot_in| =
    # (2E)^{1/2}, given as ln E; expansion during the crossing is neglected, as the briefing notes
    # neglect it (wall_backreaction.tex). They take the normalization, as the hooks do, and no
    # phi_ref: none reads ln A itself (orchestrator note 04, ruling 5).

    def _wall_context(self, method: str, normalization, **inputs) -> str:
        shown = ", ".join(f"{k} = {v!r}" for k, v in inputs.items())
        return f"{method} ({self.name}, normalization = {normalization!r}, {shown})"

    def _check_wall_inputs(self, context: str, phi, log_X, log_X_name: str):
        """
        The checks of prompt 04 §1.1 and orchestrator note 04, ruling 7: the model reflects at the
        origin; phi is a finite float > 0; ln E or ln T is finite (E <= 0 is ln E = -inf, or NaN).
        """
        if not self.reflects_at_origin:
            raise _failure(f"{context}: the model does not reflect at the origin")
        if not (isinstance(phi, (float, int)) and isfinite(phi) and phi > 0.0):
            raise _failure(f"{context}: the field must be finite and > 0")
        if not (isinstance(log_X, (float, int)) and isfinite(log_X)):
            raise _failure(
                f"{context}: {log_X_name} must be finite (-inf is a zero density, and a "
                f"negative density has no logarithm)"
            )

    def _wall_floor(self, context: str, normalization, phi, log_E) -> float:
        """The input checks, and ln V_floor, which the turning points need (ruling 7)."""
        self._check_wall_inputs(context, phi, log_E, "ln E")
        log_V_floor = self._log_V_floor(normalization)
        if log_V_floor is None:
            raise _failure(f"{context}: the model has no log_V_floor")
        if not isfinite(log_V_floor):
            raise _failure(f"{context}: log_V_floor = {log_V_floor!r} is not finite")
        return log_V_floor

    def classical_turning_point(
        self, normalization, phi: float, log_E: float
    ) -> Optional[ClassicalTurningPoint]:
        """
        The classical turning point of a field at phi falling into the wall with kinetic energy E
        (inelastic-reflection prompt 04 §1.2; wall_backreaction.tex §"Where the field is stopped",
        §"The chain of estimates"):

        - phi_t, the root of V(phi_t) - V_floor = E, searched inwards from phi in ln phi;
        - ln omega_t^2 = ln V''(phi_t), V'' = V[(ln V)'' + (ln V)'^2]; V'' <= 0 there raises, since a
          reflecting wall is convex at its turning point;
        - ln R = 2 ln omega_t^2 - ln 8 pi^2 - ln E, the test-field ratio omega_t^4/8 pi^2 E; R > 1 says
          the field cannot reach the classical wall (README §8.1 R7, R8);
        - lambda_t = -d ln V/dphi at phi_t, the notes' wall steepness;
        - ln k_ex, k_ex = lambda_t |phi_dot_in|/2 sqrt 2 = lambda_t E^{1/2}/2 (eq:kex-over-wt with y = L);
        - ell_t = ln(k_ex/omega_t), about ln 1/2 for the exponential wall.

        Returns None if V(phi) - V_floor >= E already: the field is at or inside its classical turning
        point, which is behind it. That is not a failure. It also returns None, after that test and
        before the search, if ln E is below the least finite value of ln(V - V_floor)
        (_turning_point_unresolvable; one-loop-drain prompt 01 §1.6, README §8.8 P21): the turning point
        lies inside the region the search reads as -inf, below the resolution of ln V, and cannot be
        resolved. Every such call raised in the search before that prompt.

        Raises ComputationFailureError, naming the model, the inputs and the reason, for a model that
        does not reflect at the origin or has no log_V_floor; phi not finite and positive; ln E not
        finite (E <= 0); a bracket not found; a search that leaves float range; a quantity that is not
        monotone along the search; V'' <= 0 or lambda_t <= 0 at phi_t.
        """
        context = self._wall_context(
            "classical_turning_point", normalization, phi=phi, log_E=log_E
        )
        log_V_floor = self._wall_floor(context, normalization, phi, log_E)

        def residual(p: float) -> float:
            return (
                _log_V_minus_floor(self._log_V(normalization, p), log_V_floor) - log_E
            )

        x_0 = log(phi)
        r_0 = _evaluate(residual, x_0, context)
        if r_0 >= 0.0:
            return None

        # the second None (one-loop-drain prompt 01 §1.6): a root inside the -inf region, which the search
        # cannot find; only calls that raised in the search reach it
        if _turning_point_unresolvable(log_V_floor, log_E):
            return None

        phi_t = exp(_search_log_field(residual, x_0, r_0, -1.0, context))

        d1 = self._d_logV_dphi(normalization, phi_t)
        q = self._d2_logV_dphi2(normalization, phi_t) + d1 * d1
        if not q > 0.0:
            raise _failure(
                f"{context}: V'' <= 0 at the classical turning point phi_t = {phi_t!r} "
                f"((ln V)'' + (ln V)'^2 = {q!r}); a reflecting wall is convex there"
            )
        lambda_t = -d1
        if not lambda_t > 0.0:
            raise _failure(
                f"{context}: -d ln V/dphi = {lambda_t!r} <= 0 at phi_t = {phi_t!r}; "
                f"the wall is not repulsive there"
            )
        log_omega_t_squared = self._log_V(normalization, phi_t) + log(q)
        log_k_ex = log(lambda_t) + 0.5 * log_E - _LOG_2
        result = ClassicalTurningPoint(
            phi_t=phi_t,
            log_omega_t_squared=log_omega_t_squared,
            log_R=2.0 * log_omega_t_squared - _LOG_8_PI_SQUARED - log_E,
            lambda_t=lambda_t,
            log_k_ex=log_k_ex,
            ell_t=log_k_ex - 0.5 * log_omega_t_squared,
        )
        if not all(isfinite(v) for v in result):
            raise _failure(f"{context}: a non-finite value in {result!r}")
        return result

    def one_loop_turnaround(
        self, normalization, phi: float, log_E: float
    ) -> Optional[OneLoopTurnaround]:
        """
        The one-loop turnaround of a field at phi falling into the wall with kinetic energy E
        (inelastic-reflection prompt 04 §1.3; wall_backreaction.tex §"The chain of estimates",
        eq:chain-dress, eq:chain-turn, eq:kex-over-wt, and §"The logarithm in terms of the parameters of
        the capture"). The field stops where the dressing m^4 ln(k_ex/m)/32 pi^2, m^2 = V'', reaches E:

            m_ta^4 = 32 pi^2 E/ell,   ell = ln(k_ex/m_ta),   V''(phi_ta) = m_ta^2,
            k_ex = lambda_ta E^{1/2}/2,   lambda_ta = -d ln V/dphi at phi_ta.

        There is no renormalization scale (README §8.1 R7). ell enters m_ta, which fixes phi_ta, which
        fixes k_ex and so ell: the fixed point is iterated from ell_0 = 32 until |delta ell| <= 1e-12
        ell, at most ONE_LOOP_MAX_ITERATIONS times. Each iterate finds phi_ta as the root of
        ln V'' = 2 ln m_ta, searched inwards from phi in ln phi; on the approach V'' rises
        monotonically (wall_backreaction.tex before eq:peak-is-ta), and the search checks it.

        Returns OneLoopTurnaround(phi_ta, log_m_ta, lambda_ta, log_k_ex, ell, iterations,
        outside_classical), with ell = log_k_ex - log_m_ta of the returned values. outside_classical
        is phi_ta > phi_t, decided as V(phi_ta) - V_floor < E, so that no classical root is found;
        where the classical point is behind the field it is False (then phi_t >= phi >= phi_ta). It
        is reported, not enforced.

        Returns None if V'' at phi is already at or above m_ta^2 at an iterate: the field is inside
        its one-loop turnaround to the accuracy of the iteration (orchestrator note 04, ruling 4).

        Raises ComputationFailureError as classical_turning_point does, and where the chain has no
        solution: an iterate with ell <= 0, or no convergence, with ell's last values in the message.
        """
        context = self._wall_context(
            "one_loop_turnaround", normalization, phi=phi, log_E=log_E
        )
        log_V_floor = self._wall_floor(context, normalization, phi, log_E)

        def log_V2(p: float) -> float:
            d1 = self._d_logV_dphi(normalization, p)
            q = self._d2_logV_dphi2(normalization, p) + d1 * d1
            if not q > 0.0:
                raise ValueError(f"V'' <= 0 ((ln V)'' + (ln V)'^2 = {q!r})")
            return self._log_V(normalization, p) + log(q)

        x_0 = log(phi)
        log_V2_0 = _evaluate(log_V2, x_0, context)

        ell = ONE_LOOP_ELL_START
        ells: List[float] = [ell]
        for iteration in range(1, ONE_LOOP_MAX_ITERATIONS + 1):
            log_m_ta = 0.25 * (_LOG_32_PI_SQUARED + log_E - log(ell))
            target = 2.0 * log_m_ta
            r_0 = log_V2_0 - target
            if r_0 >= 0.0:
                return None

            def residual(p: float) -> float:
                return log_V2(p) - target

            phi_ta = exp(_search_log_field(residual, x_0, r_0, -1.0, context))
            lambda_ta = -self._d_logV_dphi(normalization, phi_ta)
            if not lambda_ta > 0.0:
                raise _failure(
                    f"{context}: -d ln V/dphi = {lambda_ta!r} <= 0 at phi_ta = {phi_ta!r}; "
                    f"the wall is not repulsive there"
                )
            log_k_ex = log(lambda_ta) + 0.5 * log_E - _LOG_2
            ell_new = log_k_ex - log_m_ta
            ells.append(ell_new)
            if not ell_new > 0.0:
                raise _failure(
                    f"{context}: no one-loop turnaround: ell = ln(k_ex/m_ta) fell to {ell_new!r} "
                    f"at iteration {iteration} (ell from ell_0: {ells!r})"
                )
            if abs(ell_new - ell) <= ONE_LOOP_ELL_RTOL * ell_new:
                outside = (
                    _log_V_minus_floor(self._log_V(normalization, phi_ta), log_V_floor)
                    < log_E
                )
                return OneLoopTurnaround(
                    phi_ta=phi_ta,
                    log_m_ta=log_m_ta,
                    lambda_ta=lambda_ta,
                    log_k_ex=log_k_ex,
                    ell=ell_new,
                    iterations=iteration,
                    outside_classical=outside,
                )
            ell = ell_new

        raise _failure(
            f"{context}: no one-loop turnaround: the fixed point did not converge in "
            f"{ONE_LOOP_MAX_ITERATIONS} iterations (last values of ell: {ells[-5:]!r})"
        )

    def minimum_on_trace(self, normalization, phi_start: float, log_T: float) -> float:
        """
        The minimum of the effective potential on a given trace (inelastic-reflection prompt 04
        §1.4): the root phi_min of

            V'(x) + (ln A)'(x) T = 0,

        with T = rho_R,E (Sigma + f_m) the Einstein-frame trace that ODERHS couples to, given as ln T.
        It is the parked field's condition: a field at its minimum has rho~_c = T/A(phi_min), and
        V' + (ln A)' A rho~_c = 0 at x = phi_min is this equation; it is effective_minimum's with the
        radiation term included and phi_0 = phi_min.

        Solved in logs, ln(-V') = ln V + ln(-(ln V)') against ln((ln A)' T), searched in ln x from
        phi_start, outwards or inwards until the sign changes. There is no default start: the caller
        gives one (the field, or the last phi_min). For a reflecting wall with (ln A)' > 0 the root
        is unique, since V' < 0 rises to 0 as x grows.

        phi_ref does not enter: the equation reads (ln A)' and never ln A, and RealizedModel passes
        (ln A)' through unshifted (orchestrator note 04, ruling 5).

        Raises ComputationFailureError for a model that does not reflect at the origin; phi_start not
        finite and positive; ln T not finite; (ln A)' <= 0 at a point of the search (the root
        included); and the search's failures (classical_turning_point).
        """
        context = self._wall_context(
            "minimum_on_trace", normalization, phi_start=phi_start, log_T=log_T
        )
        self._check_wall_inputs(context, phi_start, log_T, "ln T")

        def g(p: float) -> float:
            # ln(-V') - ln((ln A)' T), -inf where V' >= 0; it falls as x grows
            d1_A = self._d_logOmega_dphi(p)
            if not d1_A > 0.0:
                raise ValueError(f"(ln A)' = {d1_A!r} <= 0")
            d1_V = self._d_logV_dphi(normalization, p)
            if not d1_V < 0.0:
                return _NEG_INF
            return self._log_V(normalization, p) + log(-d1_V) - log(d1_A) - log_T

        x_0 = log(phi_start)
        g_0 = _evaluate(g, x_0, context)
        if g_0 == 0.0:
            return exp(x_0)
        if g_0 > 0.0:
            # V' + (ln A)' T < 0: the minimum is outwards, where -g rises to 0
            def minus_g(p: float) -> float:
                return -g(p)

            return exp(_search_log_field(minus_g, x_0, -g_0, 1.0, context))
        return exp(_search_log_field(g, x_0, g_0, -1.0, context))

    def d_minimum_d_log_trace(
        self, normalization, phi_min: float, log_T: float
    ) -> float:
        """
        d phi_min/d ln T of minimum_on_trace's root at phi_min, by implicit differentiation of
        V'(x) + (ln A)'(x) T = 0 (inelastic-reflection prompt 04 §1.4):

            d phi_min/d ln T = -(ln A)' T/(V'' + (ln A)'' T)
                             = -1/(V''/((ln A)' T) + (ln A)''/(ln A)'),

        the second form computed, with V''/((ln A)' T) formed in logs. phi_min must be the root; this
        method does not find it.

        The denominator has no (ln A)'^2 term: T is held fixed. The fluctuation's mass at fixed
        rho~_c (ScalarHistory.log_m_eff_squared, V'' + [(ln A)'' + (ln A)'^2] T) has one, because
        there A rho~_c varies with x. Both are right for what they describe; the parked tracker's
        derivative (prompt 06) composes this one with d ln T/dN of the right-hand side, and must not
        use the mass's denominator.

        Raises ComputationFailureError for a model that does not reflect at the origin; phi_min not
        finite and positive; ln T not finite; (ln A)' <= 0; a denominator that is not positive (no
        minimum) or overflows.
        """
        context = self._wall_context(
            "d_minimum_d_log_trace", normalization, phi_min=phi_min, log_T=log_T
        )
        self._check_wall_inputs(context, phi_min, log_T, "ln T")

        d1_A = self._d_logOmega_dphi(phi_min)
        if not d1_A > 0.0:
            raise _failure(f"{context}: (ln A)' = {d1_A!r} <= 0")
        d2_A = self._d2_logOmega_dphi2(phi_min)
        d1_V = self._d_logV_dphi(normalization, phi_min)
        q_V = self._d2_logV_dphi2(normalization, phi_min) + d1_V * d1_V

        try:
            ratio = 0.0
            if q_V != 0.0:
                ratio = (1.0 if q_V > 0.0 else -1.0) * exp(
                    self._log_V(normalization, phi_min)
                    + log(abs(q_V))
                    - log(d1_A)
                    - log_T
                )
        except OverflowError as e:
            raise _failure(f"{context}: V''/((ln A)' T) overflows") from e
        denominator = ratio + d2_A / d1_A
        if not (denominator > 0.0 and isfinite(denominator)):
            raise _failure(
                f"{context}: V'' + (ln A)'' T is not positive ((V'' + (ln A)'' T)/((ln A)' T) = "
                f"{denominator!r}); phi_min is not a minimum"
            )
        return -1.0 / denominator

    def seed(self, cosmology) -> Seed:
        """
        The start of a history's tuning (README §6.2 P5). The base class gives the plain start:
        the normalization ln(3 M_P^2 H0^2 (1 - Omega_m)), which is ln rho_Lambda on a flat
        cosmology, with phi_ref = 0. It is a normalization in the exponential model's sense
        (ln V_0); a model that interprets the normalization otherwise overrides this.
        """
        Mp = cosmology.units.PlanckMass
        H0 = cosmology.H0
        return Seed(
            kind="plain",
            normalization=log(3.0 * Mp**2 * H0**2 * (1.0 - cosmology.omega_m)),
            phi_ref=0.0,
        )
