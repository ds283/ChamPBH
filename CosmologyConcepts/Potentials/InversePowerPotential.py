from math import log, fabs

from numpy import inf

from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import INVERSE_POWER_POTENTIAL
from Units.base import UnitsLike


class InversePowerPotential(AbstractPotential):
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
        self._log_Lambda_4 = 4.0 * log(_Lambda_as_float)

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

    @property
    def name(self):
        return f"InversePowerPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return INVERSE_POWER_POTENTIAL

    @property
    def bounce_region_level1_boundary(self) -> float:
        return self._M_float / 1e2

    @property
    def bounce_region_level2_boundary(self) -> float:
        return self._M_float / 1e4

    @property
    def bounce_region_level1_max_step(self) -> float:
        return self.bounce_region_level1_boundary / 1e2

    @property
    def bounce_region_level2_max_step(self) -> float:
        return self.bounce_region_level2_boundary / 1e4

    @property
    def hard_reflection_point(self) -> float:
        return 0.0

    def log_V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)

        if phi_float < 0.0:
            return inf

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return self._log_Lambda_4 + log(1.0 + arg)
        except OverflowError as e:
            print(
                f"!! Overflow in InversePowerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in InversePowerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            )
            raise e

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)

        if phi_float < 0.0:
            return inf

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            if fabs(arg) < 1.0:
                arginv = pow(phi_float / self._M_float, 1.0 / self._n)
                return -(self._n / phi) / (1.0 + arginv)
            else:
                return -(self._n * arg / phi) / (1.0 + arg)
        except OverflowError as e:
            print(
                f"! Overflow in InversePowerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in InversePowerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            )
            raise e
