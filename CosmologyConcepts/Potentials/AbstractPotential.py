from abc import ABC, abstractmethod

from CosmologyConcepts import FieldLike
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
    def bounce_region_boundary(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def log_V(self, phi: FieldLike) -> float:
        """
        Evaluate the potential at a given value of phi
        :param phi:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def d_logV_dphi(self, phi: FieldLike) -> float:
        """
        Evaluate the derivative of the potential at a given value of phi
        :param phi:
        :return:
        """
        raise NotImplementedError
