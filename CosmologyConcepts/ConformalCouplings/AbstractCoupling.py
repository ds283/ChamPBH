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

from CosmologyConcepts import FieldLike
from Datastore import DatastoreObject


class AbstractCoupling(DatastoreObject, ABC):
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

    @abstractmethod
    def log_Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the logarithm of the conformal coupling function at field value phi
        :param phi:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the conformal coupling function at field value phi
        :param phi:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def log_Omega_prime(self, phi: FieldLike) -> float:
        """
        Evaluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        raise NotImplementedError
