from math import exp

from CosmologyConcepts import beta_value
from CosmologyConcepts.ConformalCouplings.AbstractCoupling import AbstractCoupling
from CosmologyConcepts.ConformalCouplings.model_ids import EXPONENTIAL_COUPLING
from Units.base import UnitsLike


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

    def _raw_log_Omega(self, phi):
        """
        Evaluate the logarithm of the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        return self._beta_float * phi / self._Mp

    def _raw_log_Omega_prime(self, phi):
        """
        Evluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        return self._beta_float / self._Mp

    def _raw_Omega(self, phi):
        """
        Evaluate the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        return exp(self._raw_log_Omega(phi))
