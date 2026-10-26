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
"""

from math import log, sqrt

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts import M_value, beta_value, FieldLike, GetFieldValue
from CosmologyConcepts.Models.AbstractModel import AbstractModel, RealizedModel, Seed
from CosmologyConcepts.Models.model_ids import EXPONENTIAL_MODEL
from Units.base import UnitsLike
from config.sharding import ShardKeyType
from utilities import energy_formatter


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
