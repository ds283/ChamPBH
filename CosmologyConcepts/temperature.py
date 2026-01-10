from typing import Union

from CosmologyConcepts.DimensionfulQuantity import (
    DimensionfulQuantity,
)


class temperature(DimensionfulQuantity):
    default_unit = "GeV"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "temperature")


TemperatureLike = Union[temperature, float]


def GetTemperature(T: TemperatureLike) -> float:
    if isinstance(T, temperature):
        return T.as_float

    if isinstance(T, float):
        return T

    # attempt conversion to float, allowing an exception to be raised if it fails
    raise ValueError("Expected a TemperatureLike object")
