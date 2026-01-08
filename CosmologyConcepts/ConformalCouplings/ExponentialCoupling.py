from math import exp

from CosmologyConcepts import beta_value
from CosmologyConcepts.ConformalCouplings.AbstractCoupling import AbstractCoupling
from Units.base import UnitsLike


class ExponentialCoupling(AbstractCoupling):
    def __init__(self, store_id: int, beta: beta_value, units: UnitsLike):
        super().__init__(store_id)

        self._units: UnitsLike = units

        self._beta: beta_value = beta
        self._beta_float: float = float(beta)

        self._Mp: float = units.PlanckMass

    def log_Omega(self, phi: float) -> float:
        """
        Evaluate the logarithm of the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        return self._beta_float * phi / self._Mp

    def Omega(self, phi: float) -> float:
        """
        Evaluate the conformal coupling Omega at field value phi
        :param phi:
        :return:
        """
        return exp(self.log_Omega(phi))

    def log_Omega_prime(self, phi: float) -> float:
        """
        Evluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        return self._beta_float / self._Mp
