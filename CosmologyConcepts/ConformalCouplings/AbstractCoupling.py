from abc import ABC, abstractmethod

from Datastore import DatastoreObject


class AbstractCoupling(DatastoreObject, ABC):
    def __init__(self, store_id: int):
        DatastoreObject.__init__(self, store_id)

    @abstractmethod
    def Omega(self, phi: float) -> float:
        """
        Evaluate the conformal coupling function at field value phi
        :param phi:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def log_Omega_prime(self, phi: float) -> float:
        """
        Evaluate the logarithmic derivative Omega'/Omega at field value phi
        :param phi:
        :return:
        """
        raise NotImplementedError
