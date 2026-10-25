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
"""

from abc import ABC, abstractmethod
from collections import namedtuple
from math import exp, log
from typing import Optional, Tuple

from scipy.optimize import brentq

from datastorekit import DatastoreObject
from Units.base import UnitsLike
from config.sharding import ShardKeyType

# the seed of a history's tuning (README §6.2 P5): which analytic estimate it came from, the
# normalization and phi_ref. kind is one of SEED_KINDS
Seed = namedtuple("Seed", ["kind", "normalization", "phi_ref"])

SEED_KINDS = ("tracking", "static", "plain")


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

    @property
    def reflects_at_origin(self) -> bool:
        return self._model.reflects_at_origin


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
        guard G2 reads it at a reflection (see AbstractPotential.log_V_floor). Default None.
        """
        return None

    @property
    def reflects_at_origin(self) -> bool:
        """
        True if the potential has a purely repulsive wall in (0, phi) for every phi > 0 (see
        AbstractPotential.reflects_at_origin); the integrator's guard G1 reads it. Default False.
        """
        return False

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
        it. A model may override this with a closed form.
        """
        rm: RealizedModel = self.realize(normalization, phi_ref)
        log_Omega_0: float = rm.log_Omega(phi_0)

        def dV_eff(x: float) -> float:
            return exp(rm.log_V(x)) * rm.d_logV_dphi(x) + rm.d_logOmega_dphi(
                x
            ) * rho_m_E_0 * exp(rm.log_Omega(x) - log_Omega_0)

        low, high = bracket if bracket is not None else (0.05 * phi_0, 20.0 * phi_0)
        f_low, f_high = dV_eff(low), dV_eff(high)
        if not ((f_low <= 0.0 <= f_high) or (f_high <= 0.0 <= f_low)):
            raise ValueError(
                f"effective_minimum ({self.name}): V_eff' does not change sign on the bracket "
                f"[{low:.6g}, {high:.6g}] (values {f_low:.6g}, {f_high:.6g})"
            )

        return float(brentq(dV_eff, low, high, xtol=1e-14 * phi_0, rtol=1e-13))

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
