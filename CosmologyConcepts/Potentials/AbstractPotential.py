from abc import ABC, abstractmethod

from CosmologyConcepts import FieldLike, GetFieldValue
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

    @abstractmethod
    def _raw_V(self, phi):
        raise NotImplementedError

    def V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        return self._raw_V(GetFieldValue(phi))

    @abstractmethod
    def _raw_Vprime(self, phi):
        raise NotImplementedError

    def Vprime(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        return self._raw_Vprime(GetFieldValue(phi))
