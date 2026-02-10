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

from math import log

from ComputeTargets.exceptions import ComputationFailureError
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

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

        # pre-evaluated Lambda^4, which we don't need to recompute each time
        self._log_Lambda_4 = 4.0 * log(self._Lambda_float)

    @property
    def name(self):
        return f"ExponentialPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return EXPONENTIAL_POTENTIAL

    @property
    def bounce_region_level1_boundary(self) -> float:
        return 1.5 * self._M_float

    @property
    def bounce_region_level2_boundary(self) -> float:
        return 1.5 * self._M_float / 30.0

    @property
    def bounce_region_level1_max_step(self) -> float:
        return self.bounce_region_level1_boundary / 5e2

    @property
    def bounce_region_level2_max_step(self) -> float:
        return self.bounce_region_level2_boundary / 5e2

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

        # if phi_float < 0.0:
        #     return inf

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return self._log_Lambda_4 + arg
        except OverflowError:
            msg = f"!! Overflow in ExponentialPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in ExponentialPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)

        # if phi_float < 0.0:
        #     return inf

        arg: float = pow(self._M_float / phi_float, self._n)

        try:
            return -self._n * arg / phi
        except OverflowError:
            msg = f"! Overflow in ExponentialPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in ExponentialPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)

        arg: float = pow(self._M_float / phi_float, self._n)

        try:
            return self._n * (self._n + 1) * arg / (phi * phi)
        except OverflowError:
            msg = f"! Overflow in ExponentialPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in ExponentialPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
