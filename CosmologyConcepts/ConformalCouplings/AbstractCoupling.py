from abc import ABC, abstractmethod

from CosmologyConcepts import FieldLike, GetFieldValue
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
    def _raw_log_Omega(self, phi):
        raise NotImplementedError

    def log_Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the logarithm of the conformal coupling function at field value phi
        :param phi:
        :return:
        """
        return self._raw_log_Omega(GetFieldValue(phi))

    @abstractmethod
    def _raw_log_Omega_prime(self, phi):
        raise NotImplementedError

    def log_Omega_prime(self, phi: FieldLike) -> float:
        """
        Evaluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        return self._raw_log_Omega_prime(GetFieldValue(phi))

    @abstractmethod
    def _raw_Omega(self, phi):
        raise NotImplementedError

    def Omega(self, phi: FieldLike) -> float:
        """
        Evaluate the conformal coupling function at field value phi
        :param phi:
        :return:
        """
        return self._raw_Omega(GetFieldValue(phi))
