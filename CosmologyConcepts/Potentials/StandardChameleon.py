from math import exp

from CosmologyConcepts import M_value, Lambda_value
from CosmologyConcepts.Potentials import AbstractPotential
from Units.base import UnitsLike


class StandardChameleon(AbstractPotential):
    def __init__(
        self, store_id: int, M: M_value, Lambda: Lambda_value, n: int, units: UnitsLike
    ):
        super().__init__(store_id)

        self._units: UnitsLike = units

        self._M: M_value = M
        self._Lambda: Lambda_value = Lambda
        self._n: int = n

        # pre-evaluated Lambda^4, which we don't need to recompute each time
        _Lambda_as_float = float(Lambda)
        _Lambda_2 = _Lambda_as_float * _Lambda_as_float
        self._Lambda_4 = _Lambda_2 * _Lambda_2

        self._M_float = float(M)

    @property
    def shard_key(self) -> M_value:
        return self._M

    def V(self, phi: float) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        arg: float = pow(phi / self._M_float, self._n)
        return self._Lambda_4 * exp(arg)

    def Vprime(self, phi: float) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        arg: float = pow(phi / self._M_float, self._n)
        return -self._Lambda_4 * self._n * exp(arg) * arg / phi
