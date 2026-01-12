from math import exp

from CosmologyConcepts import beta_value, FieldLike, GetFieldValue
from CosmologyConcepts.ConformalCouplings.AbstractCoupling import AbstractCoupling
from CosmologyConcepts.ConformalCouplings.model_ids import EXPONENTIAL_COUPLING
from Units.base import UnitsLike
from config.sharding import ShardKeyType


class ExponentialCoupling(AbstractCoupling):
    def __init__(self, store_id: int, beta: beta_value, units: UnitsLike):
        super().__init__(store_id)

        self._units: UnitsLike = units

        self._beta: beta_value = beta
        self._beta_float: float = float(beta)

        self._Mp: float = units.PlanckMass

    @property
    def name(self):
        return f"ExponentialCoupling(beta={self._beta_float:.5g})"

    @property
    def type_id(self) -> int:
        return EXPONENTIAL_COUPLING

    @property
    def shard_key(self) -> ShardKeyType:
        return self._beta

    def log_Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the logarithm of the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        return self._beta_float * phi_float / self._Mp

    def Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        return exp(self.log_Omega(phi))

    def log_Omega_prime(self, phi: FieldLike) -> float:
        """
        Evluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        return self._beta_float / self._Mp
