from functools import total_ordering
from typing import Iterable

from Datastore import DatastoreObject


@total_ordering
class DimensionlessQuantity(DatastoreObject):
    def __init__(self, store_id: int, value: float, name: str):
        """
        Represents a value of beta that can be used in the conformal coupling
        """
        if store_id is None:
            raise ValueError("Store ID cannot be None")
        DatastoreObject.__init__(self, store_id)

        self.value = value
        self.name = name

    def __float__(self):
        """
        Cast to float. Returns numerical value.
        """
        return float(self.value)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplementedError

        return self.store_id == other.store_id

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplementedError

        return self.value < other.value

    def __hash__(self):
        return (self.name, self.store_id).__hash__()


class DimensionlessQuantityArray:
    def __init__(self, value_array: Iterable[DimensionlessQuantity]):
        """
        Represents an array of dimensionless quantity objects.
        """
        # store array, sorted into ascending order
        self._value_array = sorted(set(value_array), key=lambda x: x.value)
