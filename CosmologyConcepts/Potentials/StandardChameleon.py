from CosmologyConcepts import M_value, Lambda_value
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import STANDARD_CHAMELEON
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
        self._Lambda_float = float(Lambda)

    @property
    def name(self):
        return f"StandardChameleon(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return STANDARD_CHAMELEON

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
        try:
            return self._Lambda_4 * (1.0 + arg)
        except OverflowError as e:
            print(
                f"Overflow in StandardChameleon potential V() at phi={phi / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV [(phi/M)^n = {arg:.5g}]"
            )
            raise e

    def Vprime(self, phi: float) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        arg: float = pow(phi / self._M_float, self._n)
        try:
            return -self._Lambda_4 * self._n * arg / phi
        except OverflowError as e:
            print(
                f"Overflow in StandardChameleon potential Vprime() at phi={phi / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV [(phi/M)^n = {arg:.5g}]"
            )
            raise e
