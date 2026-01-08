from CosmologyConcepts.DimensionfulQuantity import (
    DimensionfulQuantity,
)


class Lambda_value(DimensionfulQuantity):
    default_unit = "eV"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "Lambda_value")
