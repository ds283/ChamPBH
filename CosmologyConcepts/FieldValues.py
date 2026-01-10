from typing import Union

from CosmologyConcepts import DimensionfulQuantity


class phi_value(DimensionfulQuantity):
    default_unit = "PlanckMass"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "phi_value")


class pi_value(DimensionfulQuantity):
    default_unit = "PlanckMass"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "pi_value")


FieldLike = Union[phi_value, pi_value, float]


def GetFieldValue(value: FieldLike) -> float:
    if isinstance(value, phi_value):
        return value.as_float

    if isinstance(value, pi_value):
        return value.as_float

    if isinstance(value, float):
        return value

    raise ValueError("Expected a FieldLike object")
