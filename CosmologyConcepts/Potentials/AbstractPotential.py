from abc import ABC, abstractmethod

from Datastore import DatastoreObject


class AbstractPotential(DatastoreObject, ABC):
    def __init__(self, store_id: int):
        DatastoreObject.__init__(self, store_id)

    @abstractmethod
    def V(self, phi: float) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def Vprime(self, phi: float) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        raise NotImplementedError
