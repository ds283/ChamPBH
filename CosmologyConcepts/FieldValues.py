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


EnergyLike = Union[phi_value, pi_value, float]


def GetFieldValue(value: EnergyLike) -> float:
    if isinstance(value, phi_value):
        return float(value)

    if isinstance(value, pi_value):
        return float(value)

    if isinstance(value, float):
        return value

    # attempt conversion to float, allowing an exception to be raised if it fails
    return float(value)
