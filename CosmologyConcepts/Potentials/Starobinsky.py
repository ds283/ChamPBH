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

from math import log, exp, fabs, copysign, inf

from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import (
    STAROBINSKY_POTENTIAL,
)
from Units.base import UnitsLike

_DENOMINATOR_REGULATOR = 1e-7


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
        self._log_Lambda_4 = 4.0 * log(_Lambda_as_float)

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

    @property
    def name(self):
        return f"StarobinskyPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return STAROBINSKY_POTENTIAL

    @property
    def bounce_region_level1_boundary(self) -> float:
        return -0.5 * self._M_float

    @property
    def bounce_region_level2_boundary(self) -> float:
        return -self._M_float

    @property
    def bounce_region_level1_max_step(self) -> float:
        return fabs(self.bounce_region_level1_boundary) / 5e2

    @property
    def bounce_region_level2_max_step(self) -> float:
        return fabs(self.bounce_region_level2_boundary) / 5e2

    @property
    def hard_reflection_point(self) -> float:
        return -5.0 * self._M_float

    def log_V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float: float = GetFieldValue(phi)

        # if phi_float < 0.0:
        #     return inf

        try:
            arg: float = phi_float / self._M_float
            exp_arg: float = exp(arg)
            B: float = 1.0 - exp_arg
            if B > 0.0:
                A: float = log(B)
            else:
                A: float = -inf

            return self._log_Lambda_4 + 2.0 * A
        except OverflowError as e:
            print(
                f"!! Overflow in StarobinskyPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in StarobinskyPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, phi/M = {arg:.5g}"
            )
            raise e

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float: float = GetFieldValue(phi)

        # if phi_float < 0.0:
        #     return inf

        try:
            arg: float = phi_float / self._M_float
            A: float = 2.0 / self._M_float

            if arg > 2.0:
                exp_marg: float = exp(-arg)

                den: float = exp_marg - 1.0
                if fabs(den) < _DENOMINATOR_REGULATOR:
                    den = copysign(1.0, den) * _DENOMINATOR_REGULATOR

                B: float = 1.0 / den

            else:
                exp_arg: float = exp(arg)

                den: float = 1.0 - exp_arg
                if fabs(den) < _DENOMINATOR_REGULATOR:
                    den = copysign(1.0, den) * _DENOMINATOR_REGULATOR

                B: float = exp_arg / den

            return -A * B

        except OverflowError as e:
            print(
                f"! Overflow in StarobinskyPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, M/phi = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in StarobinskyPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, M/phi = {arg:.5g}"
            )
            raise e

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float: float = GetFieldValue(phi)

        try:
            arg: float = phi_float / self._M_float
            M2: float = self._M_float * self._M_float
            A: float = 2.0 / M2

            if arg > 2.0:
                exp_marg: float = exp(-arg)

                den: float = exp_marg - 1.0
                if fabs(den) < _DENOMINATOR_REGULATOR:
                    den = copysign(1.0, den) * _DENOMINATOR_REGULATOR

                B: float = 1.0 / den
                C: float = exp_marg / den
            else:
                exp_arg: float = exp(arg)

                den: float = 1.0 - exp_arg
                if fabs(den) < _DENOMINATOR_REGULATOR:
                    den = copysign(1.0, den) * _DENOMINATOR_REGULATOR

                B: float = exp_arg / den
                C: float = 1.0 / den

            return -A * B * C

        except OverflowError as e:
            print(
                f"! Overflow in StarobinskyPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, M/phi = {arg:.5g}"
            )
            raise e
        except ValueError as e:
            print(
                f"!! ValueError in StarobinskyPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, M/phi = {arg:.5g}"
            )
            raise e
