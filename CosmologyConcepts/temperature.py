from CosmologyConcepts.DimensionfulQuantity import (
    DimensionfulQuantity,
)


class temperature(DimensionfulQuantity):
    default_unit = "GeV"

    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "temperature")
