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
The inverse-power-law model (power-law-wall prompt 01; README §0.2, §6.1 R1, R2, §6.3 P1, P3, P4):

    V(phi) = V_0 [1 + (M/phi)^n],    A(phi) = exp(beta (phi - phi_ref)/M_P),

for any integer n >= 1. In the user's notation the potential is V_0 + Lambda^{4+n}/phi^n with
Lambda^{4+n} = V_0 M^n and V_0 a cosmological constant. The user's ruling R1 (2026-11-13): "Power law:
yes, the intention to is add a cosmological constant. We can write it in the form you propose, to
maintain the distinction between `V_0` and `M` from the exponential potential."

For phi >> M it equals the exponential model's V_0 e^{(M/phi)^n} to O((M/phi)^{2n}), so today's minimum,
the tuning of V_0 to H_0 and the meaning of M carry over; the two models differ at the wall, phi <~ M.
The normalization is ln V_0, multiplicative in V as the exponential's is, and its secant coordinate is
itself. The coupling is the exponential's, written again here (no exponential code path is shared or
moved; README §6.3 P2).

Every hook is computed from ln u = n ln(M/phi), u = (M/phi)^n, so that nothing overflows across the
ninety-odd decades of phi a history at laboratory M reaches: ln(1 + u) as ln u + log1p(e^{-ln u}) for
ln u > 0 and log1p(e^{ln u}) otherwise, and the derivatives through s = u/(1 + u), formed from ln u, as
the test-only _PowerLawWall of ComputeTargets/tests/test_wall_model.py forms them. phi <= 0, where
ln(M/phi) does not exist, is a ComputationFailureError naming the model, phi and the hook (orchestrator
note 01, ruling 2); the integrator treats one raised on a trial state as a rejected step. Whether some
continuation to phi <= 0 is wanted is the user's to rule (prompt 01 §1.6).

_drain_L is not implemented: the power wall has no large parameter L (its local index is n), and the one-loop
law of the exponential family does not describe it (power-law-wall prompt 02). Its one-loop law is its own
(power-law-wall prompt 03; README §6.5 R6, R7, §6.6 P15, P16), _drain_fraction, stored under the drain model
one_loop_drain_model = "one-loop-measured-pl-v1", at n = 1 and 2 only:

    f = R S_n                     at ln R <= 0 (the classical branch, R6),
    f = 1 - exp(-y)               above, with y = G_n (ln R - R0_n)^P_n below the switch ln R_x,n and
                                  y = A_n/(ln R - c_n) at and above it (f_n(ln R), R7),

for one-loop and gap candidates alike (the measured fraction depends on ln R and n alone; it reads neither ell
nor an L), and 0 for an unassessed one. At any other n the one-loop law is undefined, and check_drain_law_model
refuses the model under it; the fixed law and natural mode run it at every n.
"""

from math import exp, log, log1p, sqrt

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.quanta_spectrum import WALL_POWER_N1, WALL_POWER_N2
from CosmologyConcepts import M_value, beta_value, FieldLike, GetFieldValue
from CosmologyConcepts.drain_model import (
    DRAIN_MODEL_ONE_LOOP_PL,
    DRAIN_REGIMES,
    REGIME_CLASSICAL,
    REGIME_UNASSESSED,
)
from CosmologyConcepts.Models.AbstractModel import AbstractModel, Seed
from CosmologyConcepts.Models.model_ids import INVERSE_POWER_MODEL
from Units.base import UnitsLike
from config.sharding import ShardKeyType
from utilities import energy_formatter

# below this ln u, 1/(1 + e^{-ln u}) would overflow e^{-ln u}; s = u/(1 + u) is then u = e^{ln u} to
# float precision (_PowerLawWall._s's branch)
_LOG_U_SMALL = -700.0

# the analytic seed's fixed point (seed, _analytic_seed): the exponential's stopping rule and cap
_SEED_RTOL = 1.0e-14
_SEED_MAX_ITERATIONS = 200

# the one-loop drain law on this wall (power-law-wall prompt 03 §1.2; README §6.5 R6, R7, §6.6 P15, P18), stored under
# DRAIN_MODEL_ONE_LOOP_PL, keyed by n (defined at n = 1 and 2 only). Each constant with its source; the same decimal
# literals as the reference implementation, fraction_power in prompts/power-law-wall/source/wall/fit_fn.py
# (orchestrator note 03, ruling 3), so that the two are equal bit for bit:
#   DRAIN_PL_S:       S_n, the classical wall's production energy ratio, constant in E/V_0 (R6, "yes, just use the
#                     constants"): .documents/power-law-wall/wall-02/stage1a_S_n.txt (stage 1a, the 241-point ln-k
#                     trapezoid), 1.0262017e-2 and 5.2161510e-3, as the user ruled them, 1.02620e-2 and 5.21615e-3
#   DRAIN_PL_G, DRAIN_PL_R0, DRAIN_PL_P:  the rising branch y = G (ln R - R0)^P, y = -ln(1 - f), fitted on stage 2's
#                     points with 0.3 < ln R < 20 (R7, "yes, I'd like to use the `f_n(ln R)` fit");
#   DRAIN_PL_A, DRAIN_PL_C:  the falling branch y = A/(ln R - c), fitted on the points with ln R > 28;
#                     both: .documents/power-law-wall/law-03/fit_fn.txt (fit_fn.py, from 02's stage2_scan.txt, the
#                     curve points with the field back out and not classical; unweighted least squares in f), rounded
#                     to six significant figures. Over those 62 | 60 points the law is within 0.054 | 0.056 (rms
#                     0.019 | 0.022), and within 0.0042 | 0.0030 of the mapped histories' points (n = 1 | 2)
#   DRAIN_PL_SWITCH:  ln R_x, where the two branches' y are equal at the literals above (fit_fn.py's brentq, xtol
#                     1e-14), so that f is continuous to rounding there; the rising branch below it, the falling at
#                     and above it
DRAIN_PL_S = {1: 1.02620e-2, 2: 5.21615e-3}
DRAIN_PL_G = {1: 2.86769e-3, 2: 2.85913e-3}
DRAIN_PL_R0 = {1: -1.27414, 2: -1.10903}
DRAIN_PL_P = {1: 2.22126, 2: 2.45613}
DRAIN_PL_A = {1: 27.9671, 2: 19.6100}
DRAIN_PL_C = {1: 14.9835, 2: 10.9039}
DRAIN_PL_SWITCH = {1: 23.08943252571177, 2: 16.710187997698853}


# the wall of the quanta spectrum at each n where the power wall's spectrum was measured (power-law-wall prompt 06 §1.7)
_SPECTRUM_WALLS = {1: WALL_POWER_N1, 2: WALL_POWER_N2}


def _log1p_exp(x: float) -> float:
    """ln(1 + e^x) for any finite x, without overflow: x + log1p(e^{-x}) for x > 0, log1p(e^x) otherwise."""
    if x > 0.0:
        return x + log1p(exp(-x))
    return log1p(exp(x))


class InversePowerModel(AbstractModel):
    def __init__(
        self, store_id: int, M: M_value, n: int, beta: beta_value, units: UnitsLike
    ):
        super().__init__(store_id, units)

        self._formatter: energy_formatter = energy_formatter(units, include_space=False)

        assert isinstance(n, int) and n >= 1

        self._M: M_value = M
        self._n: int = n
        self._beta: beta_value = beta

        self._M_float: float = float(M)
        self._beta_float: float = float(beta)
        self._log_M: float = log(self._M_float)

        self._Mp: float = units.PlanckMass

    @property
    def name(self) -> str:
        return f"InversePowerModel(M={self._formatter(self._M)},n={self._n},beta={self._beta_float:.5g})"

    @property
    def type_id(self) -> int:
        return INVERSE_POWER_MODEL

    @property
    def shard_key(self) -> ShardKeyType:
        return self._beta

    @property
    def reflects_at_origin(self) -> bool:
        # V = V_0 [1 + (M/phi)^n] rises without bound as phi -> 0+, and its slope V' = -n (V - V_0)/phi is
        # negative for every phi > 0, so the wall between the field and the origin is purely repulsive
        return True

    def _log_V_floor(self, normalization) -> float:
        # far from the wall (M/phi -> 0) the potential tends to V_0
        return normalization

    # no _drain_L (README §6.6 P16, replacing §6.3 P4): the one-loop law's L was defined and fitted on the exponential
    # family; this wall has no large parameter, and its measured fraction depends on ln R and n alone (prompt 02,
    # validate V6). Its one-loop law is its own fraction below (R7)

    def _undefined_at_n(self, hook: str) -> ValueError:
        return ValueError(
            f"InversePowerModel (n={self._n}) has no one-loop drain law: {hook} is defined at n = 1 and 2 only "
            f"(power-law-wall README §6.5 R7) ({self.name})"
        )

    @property
    def one_loop_drain_model(self) -> str:
        # the drain model the one-loop law is stored under (AbstractModel.one_loop_drain_model; README §6.6 P16, P17):
        # DRAIN_MODEL_ONE_LOOP_PL at n = 1 and 2; a ValueError at any other n, where the law is undefined (R7)
        if self._n not in DRAIN_PL_S:
            raise self._undefined_at_n("one_loop_drain_model")
        return DRAIN_MODEL_ONE_LOOP_PL

    @property
    def spectrum_wall(self) -> str:
        # the wall of the quanta spectrum (power-law-wall prompt 06 §1.7; AbstractModel.spectrum_wall, README §6.10 P52):
        # the power wall's table at n = 1 and 2 (README §6.5 R8); a ValueError at any other n, which has none
        wall = _SPECTRUM_WALLS.get(self._n)
        if wall is None:
            raise ValueError(
                f"InversePowerModel (n={self._n}) has no quanta spectrum: spectrum_wall is defined at n = 1 and 2 only "
                f"(power-law-wall README §6.5 R8) ({self.name})"
            )
        return wall

    def _drain_fraction(self, normalization, regime, ell, log_E, log_R) -> float:
        """
        f on this wall at one candidate (AbstractModel._drain_fraction; README §6.5 R6, R7, §6.6 P15, P18), by regime,
        term for term as fraction_power (prompts/power-law-wall/source/wall/fit_fn.py) writes it, so that the two are
        equal bit for bit:
          - unassessed: 0.0, reading nothing else;
          - classical (ln R <= 0): exp(ln R) S_n;
          - one_loop and gap: exp(ln R) S_n at ln R <= 0 (which the regimes' classification does not give, ln R > 0
            being what makes a candidate one-loop or gap), else 1 - exp(-y), y = G (ln R - R0)^P below the switch
            and A/(ln R - c) at and above it.
        It reads neither ell nor ln E (the fraction measured on this wall depends on ln R and n alone), nor the
        normalization. An unknown regime is a ValueError, and so is n other than 1 or 2.
        """
        n = self._n
        if n not in DRAIN_PL_S:
            raise self._undefined_at_n("_drain_fraction")
        if regime == REGIME_UNASSESSED:
            return 0.0
        if regime not in DRAIN_REGIMES:
            raise ValueError(
                f"InversePowerModel._drain_fraction: the regime must be one of {DRAIN_REGIMES}, not {regime!r}"
            )
        if regime == REGIME_CLASSICAL or log_R <= 0.0:
            return exp(log_R) * DRAIN_PL_S[n]
        if log_R < DRAIN_PL_SWITCH[n]:
            y = DRAIN_PL_G[n] * (log_R - DRAIN_PL_R0[n]) ** DRAIN_PL_P[n]
        else:
            y = DRAIN_PL_A[n] / (log_R - DRAIN_PL_C[n])
        return 1.0 - exp(-y)

    def _failure(
        self, hook: str, phi_float: float, reason: str
    ) -> ComputationFailureError:
        msg = (
            f"!! {reason} in InversePowerModel {hook} at phi={phi_float / self._Mp:.5g} Mp, "
            f"M={self._M_float / self._units.eV:.5g} eV, n={self._n} ({self.name})"
        )
        print(msg)
        return ComputationFailureError(msg)

    def _log_u(self, hook: str, phi: FieldLike):
        """(phi as a float, ln u = n ln(M/phi)); phi <= 0 or not finite raises, naming the hook."""
        phi_float = GetFieldValue(phi)
        if not phi_float > 0.0:
            raise self._failure(hook, phi_float, "phi is not positive")
        try:
            log_u = self._n * (self._log_M - log(phi_float))
        except (ValueError, OverflowError) as e:
            raise self._failure(hook, phi_float, f"{type(e).__name__} ({e})") from e
        if not (log_u - log_u == 0.0):
            # inf or NaN: phi = inf, or NaN
            raise self._failure(hook, phi_float, f"ln(M/phi)^n = {log_u!r}")
        return phi_float, log_u

    @staticmethod
    def _s(log_u: float) -> float:
        # s = u/(1 + u), from ln u
        if log_u > _LOG_U_SMALL:
            return 1.0 / (1.0 + exp(-log_u))
        return exp(log_u)

    def _log_V(self, normalization, phi: FieldLike) -> float:
        _, log_u = self._log_u("log_V", phi)
        return normalization + _log1p_exp(log_u)

    def _d_logV_dphi(self, normalization, phi: FieldLike) -> float:
        phi_float, log_u = self._log_u("d_logV_dphi", phi)
        return -self._n * self._s(log_u) / phi_float

    def _d2_logV_dphi2(self, normalization, phi: FieldLike) -> float:
        phi_float, log_u = self._log_u("d2_logV_dphi2", phi)
        s = self._s(log_u)
        n = self._n
        # divided by phi twice, not by phi^2, which underflows below phi ~ 1e-154
        value = (n * (n + 1) * s - n * n * s * s) / phi_float / phi_float
        if value == float("inf"):
            raise self._failure(
                "d2_logV_dphi2", phi_float, f"overflow ((ln V)'' = {value!r})"
            )
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
        The frozen-state step (R7): ln V_0 + ln V_target - ln V(phi), at which V(phi) = V_target, since
        the normalization multiplies V. V_target <= 0 raises ValueError; the tuning layer's guard deals
        with it before calling.
        """
        if not V_target > 0.0:
            raise ValueError(
                f"InversePowerModel.renormalize: V_target = {V_target:.6g} is not positive"
            )
        return normalization + log(V_target) - self._log_V(normalization, phi)

    def normalization_coordinate(self, normalization) -> float:
        # the normalization ln V_0 is its own coordinate
        return normalization

    def normalization_from_coordinate(self, x: float) -> float:
        return x

    def _analytic_seed(self, cosmology, kind: str):
        """
        (normalization, phi_ref) of the analytic minimum of V_eff = V + rho_m0 A at A(phi_ref) = 1 on the
        cosmology's densities today, at any n; or None where the kind has no solution. kind "static" is
        the field at rest; "tracking" moves at the matter-era tracking speed phi' = 1.5 phi in Eq. 7, as
        in ExponentialModel._analytic_seed, whose loop this follows.

        V' = -n (V - V_0)/phi exactly, and V - V_0 = V u/(1 + u), so the minimum satisfies

            n V u/((1 + u) phi) = beta rho_m0/M_P,    u = (M/phi)^n,

        with V = 3 M_P^2 H_0^2 G/(1 + beta phi'/M_P)^2 - rho_m0 - rho_r0 from the Friedmann condition at
        phi' (G = 1 - phi'^2/6 M_P^2), as the exponential's loop has it. Since u/(1 + u) = M^n/(phi^n + M^n),
        this is phi^{n+1} (1 + u) = C, C = n V M^n M_P/(beta rho_m0), which is solved by the fixed point

            ln phi <- [ln C(phi) - ln(1 + u(phi))]/(n + 1),

        in logs, so that M^n cannot underflow at laboratory M. Its derivative in ln phi is
        n s/(n + 1) < 1 (s = u/(1 + u)) where V does not depend on phi, and it converges from the start
        phi^{n+1} = n M^n M_P (3 M_P^2 H_0^2 - rho_m0)/(beta rho_m0), the static value with u = 0 and
        rho_r0 left out (the exponential's start at n = 1). It stops at the first iterate with
        |phi_new/phi - 1| < 1e-14, at most 200 iterations. The normalization is ln V - ln(1 + u), at which
        V(phi_ref) = V.

        No solution, as the exponential's: the loop raises ValueError (a logarithm of a quantity that is
        not positive: V <= 0, or beta <= 0) or ZeroDivisionError, does not converge within 200 iterations,
        or ends with V, G or 1 + beta phi'/M_P not positive.
        """
        Mp = cosmology.units.PlanckMass
        H0 = cosmology.H0
        rho_m0, rho_r0 = cosmology.rho_m0, cosmology.rho_r0
        beta = self._beta_float
        n = self._n
        log_Mn = n * self._log_M

        track = 0.0 if kind == "static" else 1.0
        converged = False
        try:
            # ln(n M^n M_P/(beta rho_m0)), the part of ln C that does not depend on phi
            log_K = log(n) + log_Mn + log(Mp) - log(beta) - log(rho_m0)
            log_phi = (log_K + log(3.0 * Mp**2 * H0**2 - rho_m0)) / (n + 1)
            for _ in range(_SEED_MAX_ITERATIONS):
                phi = exp(log_phi)
                pi = 1.5 * track * phi
                bp = 1.0 + beta * pi / Mp
                G = 1.0 - pi * pi / (6.0 * Mp * Mp)
                V = 3.0 * Mp**2 * H0**2 * G / bp**2 - rho_m0 - rho_r0
                log_one_plus_u = _log1p_exp(log_Mn - n * log_phi)
                new = (log_K + log(V) - log_one_plus_u) / (n + 1)
                if abs(exp(new - log_phi) - 1.0) < _SEED_RTOL:
                    # V is that of this iterate's phi, which is returned with it, as the exponential's
                    # loop returns the phi its last V was computed at
                    converged = True
                    break
                log_phi = new
        except (ValueError, ZeroDivisionError, OverflowError):
            return None

        if not converged or not (V > 0.0 and G > 0.0 and bp > 0.0):
            return None

        phi = exp(log_phi)
        return log(V) - _log1p_exp(log_Mn - n * log_phi), phi

    def seed(self, cosmology) -> Seed:
        """
        The start of a history's tuning (README §6.2 P5 of jordan-normalization; power-law-wall README
        §6.3 P3): the tracking analytic minimum, else the static one, else the base class's plain start,
        as ExponentialModel.seed, but at every n (_analytic_seed states the method, its stopping rule and
        when a kind has no solution).
        """
        for kind in ("tracking", "static"):
            found = self._analytic_seed(cosmology, kind)
            if found is not None:
                return Seed(kind=kind, normalization=found[0], phi_ref=found[1])

        return super().seed(cosmology)
