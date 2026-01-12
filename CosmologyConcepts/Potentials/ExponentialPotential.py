from math import exp

from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import (
    EXPONENTIAL_POTENTIAL,
)
from Units.base import UnitsLike


class ExponentialPotential(AbstractPotential):
    def __init__(
        self, store_id: int, M: M_value, Lambda: Lambda_value, n: int, units: UnitsLike
    ):
        super().__init__(store_id)

        self._units: UnitsLike = units

        assert n >= 0

        self._M: M_value = M
        self._Lambda: Lambda_value = Lambda
        self._n: int = n

        # pre-evaluated Lambda^4, which we don't need to recompute each time
        _Lambda_as_float = float(Lambda)
        _Lambda_2 = _Lambda_as_float * _Lambda_as_float
        self._Lambda_4 = _Lambda_2 * _Lambda_2

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

    @property
    def name(self):
        return f"ExponentialPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return EXPONENTIAL_POTENTIAL

    def V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return self._Lambda_4 * exp(arg)
        except OverflowError as e:
            print(
                f"Overflow in ExponentialPotential potential V() at phi={phi_float / self._units.GeV:.5g} GeV, M={self._M_float / self._units.GeV:.5g} GeV [(M/phi)^n = {arg:.5g}]"
            )
            raise e

    def Vprime(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return -self._Lambda_4 * self._n * exp(arg) * arg / phi_float
        except OverflowError as e:
            print(
                f"Overflow in ExponentialPotential potential Vprime() at phi={phi_float / self._units.GeV:.5g} GeV, M={self._M_float / self._units.GeV:.5g} GeV [(M/phi)^n = {arg:.5g}]"
            )
            raise e
