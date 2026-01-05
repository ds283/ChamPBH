from CosmologyConcepts import DimensionlessQuantity


class beta_value(DimensionlessQuantity):
    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "beta_value")
