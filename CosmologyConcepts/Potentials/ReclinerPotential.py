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
from math import exp, log, fabs

from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import (
    RECLINER_POTENTIAL,
)
from Units.base import UnitsLike


class ReclinerPotential(AbstractPotential):
    def __init__(
        self, store_id: int, M: M_value, Lambda: Lambda_value, units: UnitsLike
    ):
        super().__init__(store_id)

        self._units: UnitsLike = units

        self._M: M_value = M
        self._Lambda: Lambda_value = Lambda

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

        self._log_Lambda_4 = 4.0 * log(self._Lambda_float)

    @property
    def name(self):
        return f"ReclinerPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return RECLINER_POTENTIAL

    @property
    def bounce_region_level1_boundary(self) -> float:
        return -10.0 * self._M_float

    @property
    def bounce_region_level2_boundary(self) -> float:
        return -30.0 * self._M_float

    @property
    def bounce_region_level1_max_step(self) -> float:
        return fabs(self.bounce_region_level1_boundary) / 5e2

    @property
    def bounce_region_level2_max_step(self) -> float:
        return fabs(self.bounce_region_level2_boundary) / 5e2

    @property
    def hard_reflection_point(self) -> float:
        return -50.0 * self._M_float

    def log_V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        arg: float = phi_float / self._M_float

        try:
            if arg > 1.0:
                A: float = exp(-arg)
                return self._log_Lambda_4 + log(1.0 + A)

            else:
                A: float = exp(arg)
                return self._log_Lambda_4 * (log(1.0 + A) - arg)

        except OverflowError as e:
            print(
                f"! Overflow in ReclinerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in ReclinerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        arg: float = phi_float / self._M_float

        try:
            if arg > 1.0:
                A: float = exp(-arg)
                B: float = A / (1.0 + A)
                return -B / self._M_float

            else:
                A: float = exp(arg)
                B: float = 1.0 / (1.0 + A)
                return -B / self._M_float

        except OverflowError as e:
            print(
                f"! Overflow in ReclinerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in ReclinerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        arg: float = phi_float / self._M_float

        M2: float = self._M_float * self._M_float
        try:
            if arg > 1.0:
                A: float = exp(-arg)
                B: float = A / (1.0 + A)
                C: float = 1.0 / (1.0 + A)
                return B * C / M2

            else:
                A: float = exp(arg)
                B: float = 1.0 / (1.0 + A)
                C: float = A / (1.0 + A)
                return B * C / M2

        except OverflowError as e:
            print(
                f"! Overflow in ReclinerPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in ReclinerPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e
