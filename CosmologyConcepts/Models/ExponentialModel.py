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
The exponential model (jordan-normalization prompt 04, M3; README §6.0 R6, R7, R14):

    V(phi) = V_0 exp((M/phi)^n),    A(phi) = exp(beta (phi - phi_ref)/M_P).

The normalization is ln V_0 (multiplicative in V), and its secant coordinate is itself. The hooks
are the expressions of the former ExponentialPotential and ExponentialCoupling, with ln Lambda^4
replaced by the normalization, so that at the normalization 4 ln Lambda and phi_ref = 0 every value
is bitwise what those rows gave (jordan-normalization prompt 04). Prompt 07 deleted the rows, their
classes and the adapter from_rows that realized a model from them; the model is stored in its own
table (ObjectFactories/ExponentialModel.py), keyed by (M, n, beta).

The one-loop drain law on this wall (power-law-wall prompt 07c; README §6.13 U13, §6.14 P99, P100, U14) is the model's
own, _drain_fraction, stored under the drain model one_loop_drain_model = "one-loop-exponential-v2", at n = 1 and 2
only, by candidate regime (one_loop_fraction):

    f = R S(L)                                  at ln R <= 0 (the classical branch, unchanged from v1),
    f = 1 - exp(-min(y_rise, y_fall))           above, with
        y_rise = G_n (ln R - R0_n)^P_n (1 + Q_n ln L),   0 where ln R <= R0_n or 1 + Q_n ln L <= 0,
        y_fall = A_f/(ell - ell_0)                       at a one-loop candidate with ell > ell_0, +inf otherwise,

and 0 for an unassessed candidate. L = ln(1 + E/V_0) is the model's _drain_L, ell the one-loop turnaround's in k_ex =
E^{1/2}|V'''/V''|/2 (R13-R15). A gap candidate (no chain) takes the rise alone. The constants are power-law-wall prompt
07b2's joint fit (v-J: the fall shared by n = 1 and 2, the rise per n), fitted on ln(V_0/M^4) <= 10 (P109; every history
of record has ln(V_0/M^4) <= 3.3, crossover's U15). At any other n the one-loop law is undefined (U14) and
check_drain_law_model refuses the model under it; the fixed law and natural mode run it at every n.
"""

from math import exp, inf, isfinite, log, log1p, sqrt
from typing import Optional

import numpy as np

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.quanta_spectrum import WALL_EXPONENTIAL
from CosmologyConcepts import M_value, beta_value, FieldLike, GetFieldValue
from CosmologyConcepts.drain_model import (
    DRAIN_MODEL_ONE_LOOP_EXPONENTIAL,
    DRAIN_REGIMES,
    REGIME_CLASSICAL,
    REGIME_ONE_LOOP,
    REGIME_UNASSESSED,
)
from CosmologyConcepts.Models.AbstractModel import AbstractModel, RealizedModel, Seed
from CosmologyConcepts.Models.model_ids import EXPONENTIAL_MODEL
from Units.base import UnitsLike
from config.sharding import ShardKeyType
from utilities import energy_formatter

# the one-loop drain law on this wall, one-loop-exponential-v2 (power-law-wall prompt 07c; README §6.13 U13, §6.14 P100),
# keyed by n (defined at n = 1 and 2 only, U14). The constants are power-law-wall prompt 07b2's joint fit (v-J) on the
# selection S3, ln(V_0/M^4) <= 10, with its new points: .documents/power-law-wall/kex-07b2/minform.json, key with_new,
# n12/S3/v-J (07b2's commit 2f638e0), at full precision, so that one_loop_fraction equals 07b2's reference
# implementation (prompts/power-law-wall/source/kex/minform.py, fraction) bit for bit:
#   DRAIN_EXP_FALL_COEFFICIENT, DRAIN_EXP_FALL_ELL0:  the falling branch y_fall = A_f/(ell - ell_0), shared by n = 1 and 2
#                     (U13: "falling branch shared, rising branch fitted per-n"); A_f = 2.514 +- 0.052, ell_0 = 1.186 +-
#                     0.047. A_f is not called A, which names the law's strength (--A-values)
#   DRAIN_EXP_RISE_G, _R0, _P, _Q:  the rising branch y_rise = G (ln R - R0)^P (1 + Q ln L), per n (v1's gap form)
DRAIN_EXP_FALL_COEFFICIENT = 2.5143466540073995
DRAIN_EXP_FALL_ELL0 = 1.1859055953178366
DRAIN_EXP_RISE_G = {1: 0.020236734836939167, 2: 0.014077566206585982}
DRAIN_EXP_RISE_R0 = {1: 1.7292636698176154, 2: 1.2044868612303559}
DRAIN_EXP_RISE_P = {1: 1.4945632951339265, 2: 1.7311233766914242}
DRAIN_EXP_RISE_Q = {1: 1.0354234565531648, 2: 0.9084029819604703}

# S(L), the classical wall's production energy ratio, the law's classical branch R S(L) (one-loop-drain README §8.10,
# decision 3), moved here from ComputeTargets/ScalarHistory.py by power-law-wall prompt 07c (README §6.14 P99), unchanged
# in value: the 21 values of prompts/one-loop-drain/source/crossover/out/stage1a_S_of_L.txt (stage 1a, the classical
# bounce off V0 (e^{M/phi} - 1); columns L and S(L)), as the same decimal literals. drain_S_of_L interpolates linearly in
# (ln L, ln S), by np.interp on their np.log; holds S(0.3) below L = 0.3; and above L = 134 continues as
# S(134) (134/L)^2, since L^2 S tends to a constant (production_energy_ratio.tex). The table is 0.1% high from its
# quadrature (crossover stage 2, V2q), which is immaterial here. It reads no k_ex (R16), so v2 keeps it
DRAIN_EXP_S_TABLE_L = (
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
DRAIN_EXP_S_TABLE_S = (
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
_DRAIN_EXP_S_LOG_L = np.log(np.array(DRAIN_EXP_S_TABLE_L))
_DRAIN_EXP_S_LOG_S = np.log(np.array(DRAIN_EXP_S_TABLE_S))


def drain_S_of_L(L: float) -> float:
    """
    S(L), the classical wall's production energy ratio (one-loop-drain README §8.10, decision 3), from
    DRAIN_EXP_S_TABLE_*: linear in (ln L, ln S) by np.interp on the table's np.log; S(0.3) below L = 0.3 (np.interp holds
    the end value); S(134) (134/L)^2 above L = 134 (the ruled continuation). Moved unchanged from
    ComputeTargets/ScalarHistory.py by power-law-wall prompt 07c (P99).
    """
    if L > DRAIN_EXP_S_TABLE_L[-1]:
        return DRAIN_EXP_S_TABLE_S[-1] * (DRAIN_EXP_S_TABLE_L[-1] / L) ** 2
    return float(np.exp(np.interp(log(L), _DRAIN_EXP_S_LOG_L, _DRAIN_EXP_S_LOG_S)))


def one_loop_fraction(
    n: int,
    regime: str,
    ell: Optional[float],
    L: Optional[float],
    log_R: Optional[float],
) -> float:
    """
    f under one-loop-exponential-v2 at one candidate (power-law-wall prompt 07c §1.3; README §6.13 U13, §6.14 P100), by
    regime, at n = 1 or 2 (the caller, ExponentialModel._drain_fraction, refuses any other n, U14), with L the model's
    wall variable ln(1 + E/V_0) and ell the one-loop turnaround's:
      - unassessed: 0.0, reading nothing else;
      - classical, and any ln R <= 0: exp(ln R) S(L) (drain_S_of_L), as v1;
      - one_loop and gap: f = 1 - exp(-min(y_rise, y_fall)) with
          y_rise = G_n (ln R - R0_n)^P_n (1 + Q_n ln L), 0 where ln R <= R0_n or 1 + Q_n ln L <= 0 (so f = 0.0 exactly
                   there, as v1's gap had it by the user's ruling of 2026-11-05),
          y_fall = A_f/(ell - ell_0) at a one-loop candidate with ell > ell_0, +inf otherwise (a gap candidate takes
                   the rise alone).
    No plateau and no cap (U13). Term for term as 07b2's reference, minform.fraction (with y_rise, y_fall and y_of),
    writes it, so that the two are equal bit for bit. The constants were fitted on ln(V_0/M^4) <= 10 (P109; every
    history of record has ln(V_0/M^4) <= 3.3); that domain is stated, not checked. An unknown regime is a ValueError.
    """
    if regime == REGIME_UNASSESSED:
        return 0.0
    if regime not in DRAIN_REGIMES:
        raise ValueError(
            f"ExponentialModel one_loop_fraction: the regime must be one of {DRAIN_REGIMES}, not {regime!r}"
        )
    if regime == REGIME_CLASSICAL or log_R <= 0.0:
        return exp(log_R) * drain_S_of_L(L)
    # the rise
    r = 0.0
    d = log_R - DRAIN_EXP_RISE_R0[n]
    if d > 0.0:
        s = 1.0 + DRAIN_EXP_RISE_Q[n] * log(L)
        if s > 0.0:
            r = DRAIN_EXP_RISE_G[n] * d ** DRAIN_EXP_RISE_P[n] * s
    # the fall, where the chain has a solution with ell > ell_0
    fl = inf
    if regime == REGIME_ONE_LOOP and ell is not None:
        den = ell - DRAIN_EXP_FALL_ELL0
        if den > 0.0:
            fl = DRAIN_EXP_FALL_COEFFICIENT / den
    m = r if r <= fl else fl
    return 1.0 - exp(-m)


class ExponentialModel(AbstractModel):
    def __init__(
        self, store_id: int, M: M_value, n: int, beta: beta_value, units: UnitsLike
    ):
        super().__init__(store_id, units)

        self._formatter: energy_formatter = energy_formatter(units, include_space=False)

        assert n >= 0

        self._M: M_value = M
        self._n: int = n
        self._beta: beta_value = beta

        self._M_float: float = float(M)
        self._beta_float: float = float(beta)

        self._Mp: float = units.PlanckMass

    @property
    def name(self) -> str:
        return f"ExponentialModel(M={self._formatter(self._M)},n={self._n},beta={self._beta_float:.5g})"

    @property
    def type_id(self) -> int:
        return EXPONENTIAL_MODEL

    @property
    def shard_key(self) -> ShardKeyType:
        return self._beta

    @property
    def reflects_at_origin(self) -> bool:
        # V = V_0 exp((M/phi)^n) rises without bound as phi -> 0+, and its slope is negative
        # for every phi > 0, so the wall between the field and the origin is purely repulsive
        return True

    def _log_V_floor(self, normalization) -> float:
        # far from the wall (M/phi -> 0) the potential tends to V_0
        return normalization

    def _drain_L(self, normalization, log_E: float) -> float:
        # the one-loop drain law's L (one-loop-drain prompt 02c; AbstractModel._drain_L): L = ln(1 + E/V_0). For
        # V = V_0 e^{(M/phi)^n} this is the wall exponent (M/phi_t)^n at the classical turning point, where
        # V(phi_t) - V_0 = E; at n = 1 it is M/phi_t, the variable crossover's stage 2 measured on. It is U15 (b)
        # as ruled 2026-11-05, "OK, let's take option (b) for now", and the law is fitted on this family. Since
        # power-law-wall prompt 07c it is the input of this model's own fraction (_drain_fraction), and is recorded
        # per candidate (README §6.14 P99). Through log1p of an exponential, which cannot overflow for any finite
        # x = ln E - ln V_0 (orchestrator note 02c ruling 2): x + log1p(e^{-x}) for x > 0, log1p(e^x) otherwise
        x: float = log_E - self._log_V_floor(normalization)
        if x > 0.0:
            return x + log1p(exp(-x))
        return log1p(exp(x))

    def _undefined_at_n(self, hook: str) -> ValueError:
        return ValueError(
            f"ExponentialModel (n={self._n}) has no one-loop drain law: {hook} is defined at n = 1 and 2 only "
            f"(power-law-wall README §6.14 U14) ({self.name})"
        )

    @property
    def one_loop_drain_model(self) -> str:
        # the drain model the one-loop law is stored under (AbstractModel.one_loop_drain_model; power-law-wall README
        # §6.6 P16, §6.14 P101): one-loop-exponential-v2, this model's _drain_fraction, at n = 1 and 2; a ValueError at
        # any other n, where the law is undefined (U14, ruled (a): "for now, refuse `n >= 3`")
        if self._n not in DRAIN_EXP_RISE_G:
            raise self._undefined_at_n("one_loop_drain_model")
        return DRAIN_MODEL_ONE_LOOP_EXPONENTIAL

    def _drain_fraction(self, normalization, regime, ell, log_E, log_R) -> float:
        """
        f on this wall at one candidate under one-loop-exponential-v2 (AbstractModel._drain_fraction; power-law-wall
        README §6.13 U13, §6.14 P99, P100): one_loop_fraction at this n with L = _drain_L(normalization, ln E), read
        only where the regime needs it (not for an unassessed candidate, nor an unknown regime, which one_loop_fraction
        refuses). At n other than 1 and 2 a ValueError naming the class, the hook and U14.
        """
        if self._n not in DRAIN_EXP_RISE_G:
            raise self._undefined_at_n("_drain_fraction")
        if regime == REGIME_UNASSESSED or regime not in DRAIN_REGIMES:
            return one_loop_fraction(self._n, regime, ell, None, log_R)
        return one_loop_fraction(
            self._n, regime, ell, self._drain_L(normalization, log_E), log_R
        )

    @property
    def spectrum_wall(self) -> str:
        # the wall of the quanta spectrum (power-law-wall prompt 06 §1.7; AbstractModel.spectrum_wall, README §6.10 P52):
        # the exponential wall's table at every n, which serves the exponential family (README §6.6 P19)
        return WALL_EXPONENTIAL

    def _log_V(self, normalization, phi: FieldLike) -> float:
        phi_float = GetFieldValue(phi)

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return normalization + arg
        except OverflowError as e:
            msg = f"!! Overflow in ExponentialModel log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in ExponentialModel log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e

    def _d_logV_dphi(self, normalization, phi: FieldLike) -> float:
        phi_float = GetFieldValue(phi)

        arg: float = pow(self._M_float / phi_float, self._n)

        try:
            return -self._n * arg / phi
        except OverflowError as e:
            msg = f"! Overflow in ExponentialModel d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in ExponentialModel d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e

    def _d2_logV_dphi2(self, normalization, phi: FieldLike) -> float:
        phi_float = GetFieldValue(phi)

        arg: float = pow(self._M_float / phi_float, self._n)

        try:
            return self._n * (self._n + 1) * arg / (phi * phi)
        except OverflowError as e:
            msg = f"! Overflow in ExponentialModel d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in ExponentialModel d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e

    def _d3_logV_dphi3(self, normalization, phi: FieldLike) -> float:
        # (ln V)''' = -n (n + 1) (n + 2) (M/phi)^n/phi^3 (power-law-wall prompt 07c; AbstractModel._d3_logV_dphi3),
        # divided by phi three times so that phi^3 cannot underflow; an overflow, of the power or of the value, raises
        phi_float = GetFieldValue(phi)
        n = self._n
        try:
            arg: float = pow(self._M_float / phi_float, n)
            value = -n * (n + 1) * (n + 2) * arg / phi_float / phi_float / phi_float
        except (OverflowError, ValueError, ZeroDivisionError) as e:
            msg = f"!! {type(e).__name__} in ExponentialModel d3_logV_dphi3 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV ({e})"
            print(msg)
            raise ComputationFailureError(msg) from e
        if not isfinite(value):
            msg = f"!! Overflow in ExponentialModel d3_logV_dphi3 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (ln V)''' = {value!r}"
            print(msg)
            raise ComputationFailureError(msg)
        return value

    def _pulse_rate_hook(self, normalization, phi: FieldLike) -> float:
        # |V'''/V''| (power-law-wall prompt 07c2; README §6.16 P126; AbstractModel._pulse_rate_hook): with ln V = ln V_0 + u,
        # u = (M/phi)^n, V''/V = n u [(n + 1) + n u]/phi^2 and V'''/V = -n u [(n + 1)(n + 2) + 3 n (n + 1) u + n^2 u^2]/phi^3,
        # so the rate is [(n + 1)(n + 2) + 3 n (n + 1) u + n^2 u^2]/([(n + 1) + n u] phi), every term positive, with no
        # third derivative and no product of large hooks. An overflow, of the power or of the value, raises naming the hook
        phi_float = GetFieldValue(phi)
        n = self._n
        try:
            u: float = pow(self._M_float / phi_float, n)
            value = (
                ((n + 1) * (n + 2) + 3 * n * (n + 1) * u + n * n * u * u)
                / ((n + 1) + n * u)
                / phi_float
            )
        except (OverflowError, ValueError, ZeroDivisionError) as e:
            msg = f"!! {type(e).__name__} in ExponentialModel pulse_rate_hook at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV ({e})"
            print(msg)
            raise ComputationFailureError(msg) from e
        if not isfinite(value):
            msg = f"!! Overflow in ExponentialModel pulse_rate_hook at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, |V'''/V''| = {value!r}"
            print(msg)
            raise ComputationFailureError(msg)
        return value

    def _log_Omega(self, phi: FieldLike) -> float:
        phi_float = GetFieldValue(phi)
        return self._beta_float * phi_float / self._Mp

    def _d_logOmega_dphi(self, phi: FieldLike) -> float:
        return self._beta_float / self._Mp

    def _d2_logOmega_dphi2(self, phi: FieldLike) -> float:
        return 0.0

    def renormalize(self, normalization, phi: float, V_target: float) -> float:
        """
        The frozen-state step (R7): ln V_0 + ln V_target - ln V(phi), at which V(phi) = V_target.
        V_target <= 0 raises ValueError; the tuning layer's guard deals with it before calling.
        """
        if not V_target > 0.0:
            raise ValueError(
                f"ExponentialModel.renormalize: V_target = {V_target:.6g} is not positive"
            )
        return normalization + log(V_target) - self._log_V(normalization, phi)

    def normalization_coordinate(self, normalization) -> float:
        # the normalization ln V_0 is its own coordinate
        return normalization

    def normalization_from_coordinate(self, x: float) -> float:
        return x

    def _analytic_seed(self, cosmology, kind: str):
        """
        (normalization, phi_ref) of the analytic minimum of V_eff = V + rho_m0 A at A(phi_ref) = 1
        on the cosmology's densities today, for n = 1; or None where the kind has no solution.
        kind "static" is the field at rest; "tracking" moves at the matter-era tracking speed
        phi' = 1.5 phi in Eq. 7. The loop is analytic() of
        prompts/jordan-normalization/source/planning-probes-2026-10-20/analytic_seed.py,
        transcribed; the normalization is ln(V e^{-M/phi}), the probe's ln Lambda^4.
        """
        Mp = cosmology.units.PlanckMass
        H0 = cosmology.H0
        rho_m0, rho_r0 = cosmology.rho_m0, cosmology.rho_r0
        beta = self._beta_float
        Mf = self._M_float

        track = 0.0 if kind == "static" else 1.0
        converged = False
        try:
            phi = sqrt(Mf * Mp * (3 * Mp**2 * H0**2 - rho_m0) / (beta * rho_m0))
            for _ in range(200):
                pi = 1.5 * track * phi
                bp = 1.0 + beta * pi / Mp
                G = 1.0 - pi * pi / (6.0 * Mp * Mp)
                V = 3.0 * Mp**2 * H0**2 * G / bp**2 - rho_m0 - rho_r0
                new = sqrt(
                    Mf * Mp * V / (beta * rho_m0)
                )  # (M/phi^2) V = (beta/Mp) rho_m0
                if abs(new / phi - 1.0) < 1e-14:
                    converged = True
                    break
                phi = new
        except (ValueError, ZeroDivisionError):
            return None

        if not converged or not (V > 0.0 and G > 0.0 and bp > 0.0):
            return None

        return log(V) - Mf / phi, phi

    def seed(self, cosmology) -> Seed:
        """
        The start of a history's tuning (README §6.2 P5): the tracking analytic minimum, else the
        static one, else the base class's plain start. The analytic seeds assume n = 1
        (V' = -V M/phi^2), so for n != 1 the seed is plain. A kind has no solution where its loop
        raises ValueError or ZeroDivisionError, does not converge within 200 iterations, or ends
        with V, G or 1 + beta phi'/M_P not positive.
        """
        if self._n == 1:
            for kind in ("tracking", "static"):
                found = self._analytic_seed(cosmology, kind)
                if found is not None:
                    return Seed(kind=kind, normalization=found[0], phi_ref=found[1])

        return super().seed(cosmology)
