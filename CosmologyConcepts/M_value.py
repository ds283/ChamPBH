from CosmologyConcepts.DimensionfulQuantity import (
    DimensionfulQuantity,
)


class M_value(DimensionfulQuantity):
    default_unit = "eV"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "M_value")
