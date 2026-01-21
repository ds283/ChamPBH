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

from Datastore import DatastoreObject
from Units.base import UnitsLike


class BaseCosmology(DatastoreObject, ABC):
    def __init__(self, store_id: int):
        DatastoreObject.__init__(self, store_id)
        # no constructor for ABC

    @property
    @abstractmethod
    def type_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def units(self) -> UnitsLike:
        raise NotImplementedError

    @property
    @abstractmethod
    def H0(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def z(self, T: float) -> float:
        raise NotImplementedError


def check_cosmology(A, B):
    """
    Check that object A and B are defined with the same cosmology
    Assumes that both provide a .cosmology property that returns a BaseCosmology object
    :param A:
    :param B:
    :return:
    """
    A_cosmology: BaseCosmology = A if isinstance(A, BaseCosmology) else A.cosmology
    B_cosmology: BaseCosmology = B if isinstance(A, BaseCosmology) else B.cosmology

    if A_cosmology.store_id != B_cosmology.store_id:
        raise RuntimeError("Cosmology store_ids are different")
