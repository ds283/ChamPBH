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

from math import exp, fabs

from numpy import inf

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import (
    STAROBINSKY_POTENTIAL,
)
from Units.base import UnitsLike


class StarobinskyPotential(AbstractPotential):
    def __init__(
        self, store_id: int, M: M_value, Lambda: Lambda_value, units: UnitsLike
    ):
        super().__init__(store_id)

        self._units: UnitsLike = units

        self._M: M_value = M
        self._Lambda: Lambda_value = Lambda
        # pre-evaluated Lambda^4, which we don't need to recompute each time
        _Lambda_as_float = float(Lambda)
        self._Lambda2_float = _Lambda_as_float * _Lambda_as_float
        self._Lambda4_float = self._Lambda2_float * self._Lambda2_float

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

    @property
    def name(self):
        return f"StarobinskyPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return STAROBINSKY_POTENTIAL

    @property
    def default_max_step(self) -> float:
        # seems no need to limit initial maximum step size for this potential
        return inf

    @property
    def bounce_region_level1_boundary(self) -> float:
        return -self._M_float

    @property
    def bounce_region_level2_boundary(self) -> float:
        return -3.0 * self._M_float

    @property
    def bounce_region_level1_max_step(self) -> float:
        return fabs(self.bounce_region_level1_boundary) / 5e2

    @property
    def bounce_region_level2_max_step(self) -> float:
        return fabs(self.bounce_region_level2_boundary) / 5e2

    @property
    def hard_reflection_point(self) -> float:
        return -15.0 * self._M_float

    def V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float: float = GetFieldValue(phi)

        try:
            arg: float = phi_float / self._M_float
            B: float = 1.0 - exp(-arg)

            return self._Lambda4_float * B * B

        except OverflowError:
            msg = f"!! Overflow in StarobinskyPotential V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in StarobinskyPotential V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)

    def dV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float: float = GetFieldValue(phi)

        try:
            arg: float = phi_float / self._M_float
            B1: float = exp(-arg)
            B2: float = exp(-2.0 * arg)
            B: float = B2 - B1

            return -2.0 * self._Lambda4_float * B / self._M_float

        except OverflowError:
            msg = f"! Overflow in StarobinskyPotential V' at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in StarobinskyPotential V'' at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)

    def d2V_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """

        phi_float: float = GetFieldValue(phi)

        try:
            arg: float = phi_float / self._M_float
            B1: float = exp(-arg)
            B2: float = exp(-2.0 * arg)
            B: float = 2.0 * B2 - B1

            return 2.0 * self._Lambda4_float * B / (self._M_float * self._M_float)

        except OverflowError:
            msg = f"! Overflow in StarobinskyPotential V'' at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
        except ValueError:
            msg = f"!! ValueError in StarobinskyPotential V'' at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg)
