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

from numpy import inf

from CosmologyConcepts import M_value, Lambda_value, FieldLike
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import (
    REFLECTING_POTENTIAL,
)
from Units.base import UnitsLike
from utilities import energy_formatter


class ReflectingPotential(AbstractPotential):
    def __init__(
        self, store_id: int, M: M_value, Lambda: Lambda_value, units: UnitsLike
    ):
        super().__init__(store_id)

        self._units: UnitsLike = units
        self._formatter: energy_formatter = energy_formatter(units, include_space=False)

        self._M: M_value = M
        self._Lambda: Lambda_value = Lambda

        # pre-evaluated Lambda^4, which we don't need to recompute each time
        _Lambda_as_float = float(Lambda)
        self._log_Lambda_4 = 4.0 * log(_Lambda_as_float)

        self._M_float = float(M)
        self._Lambda_float = float(Lambda)

        self._reflection_point = self._M_float / 1e3

    @property
    def name(self):
        return f"ReflectingPotential(M={self._formatter(self._M)},Lambda={self._formatter(self._Lambda)})"

    @property
    def type_id(self) -> int:
        return REFLECTING_POTENTIAL

    @property
    def default_max_step(self) -> float:
        # seems no need to limit initial maximum step size for this potential
        return inf

    @property
    def bounce_region_level1_boundary(self) -> float:
        return 5 * self._reflection_point

    @property
    def bounce_region_level2_boundary(self) -> float:
        return 2 * self._reflection_point

    @property
    def bounce_region_level1_max_step(self) -> float:
        return self.bounce_region_level1_boundary / 10

    @property
    def bounce_region_level2_max_step(self) -> float:
        return self.bounce_region_level2_boundary / 10

    @property
    def hard_reflection_point(self) -> float:
        return self._reflection_point

    def log_V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        return self._log_Lambda_4

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        return 0.0

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        return 0.0
