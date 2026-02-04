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

    @property
    def name(self):
        return f"ReclinerPotential(M={self._M_float / self._units.eV:.5g}eV,Lambda={self._Lambda_float / self._units.eV:.5g}eV)"

    @property
    def type_id(self) -> int:
        return RECLINER_POTENTIAL

    @property
    def bounce_region_level1_boundary(self) -> float:
        return 0.0

    @property
    def bounce_region_level2_boundary(self) -> float:
        return 0.0

    @property
    def bounce_region_level1_max_step(self) -> float:
        return 0.0

    @property
    def bounce_region_level2_max_step(self) -> float:
        return 0.0

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
        # Skeleton implementation
        return 0.0

    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        # Skeleton implementation
        return 0.0

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)
        # Skeleton implementation
        return 0.0
