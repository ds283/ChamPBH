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

from abc import ABC, abstractmethod
from typing import Optional

from Datastore import DatastoreObject


class AbstractPotential(DatastoreObject, ABC):
    def __init__(self, store_id: int):
        DatastoreObject.__init__(self, store_id)

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def type_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def bounce_region_level1_boundary(self) -> float:
        raise NotImplementedError

    @property
    @abstractmethod
    def bounce_region_level2_boundary(self) -> float:
        raise NotImplementedError

    @property
    @abstractmethod
    def bounce_region_level1_max_step(self) -> float:
        raise NotImplementedError

    @property
    @abstractmethod
    def bounce_region_level2_max_step(self) -> float:
        raise NotImplementedError

    @property
    @abstractmethod
    def hard_reflection_point(self) -> float:
        raise NotImplementedError

    @property
    def reflects_at_origin(self) -> bool:
        """
        True if the potential has a purely repulsive wall somewhere in (0, phi) for every phi > 0
        at which the field can arrive moving inwards, so that nothing but the wall can turn an
        inward-moving field. The scalar-field step loop (ComputeTargets.ScalarModel.
        integrate_scalar_history) replaces a bounce that is thinner than the representable step
        by an instantaneous elastic reflection only if this is True (guard G1, integrator-
        remediation README §2 (b')); otherwise reaching the representable-step floor is a
        failure of the history. Default False.
        """
        return False

    @property
    def log_V_floor(self) -> Optional[float]:
        """
        log of the potential's value far from the wall (its constant part), or None if not
        defined. The step loop uses it at a reflection to form the wall part of the potential
        fraction, W = 3 (V - V_floor)/(3H^2 Mp^2), and requires W <= pi^2/2 (guard G2,
        integrator-remediation README §2 (b')): a field that arrived from outside the wall
        satisfies it, one that has been stepped into the wall does not. Default None.
        """
        return None

    # in addition, each potential should implement functions
    # log_V(), d_logV_dphi(), and d2_logV_dphi2()
    # or V(), d_V_dphi(), and d2_V_dphi2()
    # or both
