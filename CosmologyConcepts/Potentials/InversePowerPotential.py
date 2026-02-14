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

from math import log, fabs

from numpy import inf

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts import M_value, Lambda_value, FieldLike, GetFieldValue
from CosmologyConcepts.Potentials.AbstractPotential import AbstractPotential
from CosmologyConcepts.Potentials.model_ids import INVERSE_POWER_POTENTIAL
from Units.base import UnitsLike
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE


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
    def default_abs_tol(self) -> float:
        return DEFAULT_ABS_TOLERANCE

    @property
    def default_rel_tol(self) -> float:
        return DEFAULT_REL_TOLERANCE

    @property
    def default_max_step(self) -> float:
        # seems no need to limit initial maximum step size for this potential
        return inf

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

        # if phi_float < 0.0:
        #     return inf

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            return self._log_Lambda_4 + log(1.0 + arg)
        except OverflowError as e:
            msg = f"!! Overflow in InversePowerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in InversePowerPotential log_V at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e

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
            if fabs(arg) < 1.0:
                arginv = pow(phi_float / self._M_float, 1.0 / self._n)
                return -(self._n / phi) / (1.0 + arginv)
            else:
                return -(self._n * arg / phi) / (1.0 + arg)
        except OverflowError as e:
            msg = f"! Overflow in InversePowerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in InversePowerPotential d_logV_dphi at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e

    def d2_logV_dphi2(self, phi: FieldLike) -> float:
        """
        Evaluate the second derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        phi_float = GetFieldValue(phi)

        arg: float = pow(self._M_float / phi_float, self._n)
        try:
            phi2: float = phi * phi
            A: float = 1.0 + arg
            A2: float = A * A
            return self._n * (self._n + 1.0 + arg) * arg / phi2 / A2
        except OverflowError as e:
            msg = f"! Overflow in InversePowerPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
        except ValueError as e:
            msg = f"!! ValueError in InversePowerPotential d2_logV_dphi2 at phi={phi_float / self._units.PlanckMass:.5g} Mp, M={self._M_float / self._units.eV:.5g} eV, (M/phi)^n = {arg:.5g}"
            print(msg)
            raise ComputationFailureError(msg) from e
