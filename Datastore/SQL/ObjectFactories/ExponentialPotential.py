import sqlalchemy as sqla
from sqlalchemy.exc import MultipleResultsFound

from CosmologyConcepts import M_value, Lambda_value
from CosmologyConcepts.Potentials import ExponentialPotential
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from Units.base import UnitsLike


class sqla_ExponentialPotential_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": True,
            "timestamp": True,
            "columns": [
                sqla.Column(
                    "M_serial",
                    sqla.Integer,
                    sqla.ForeignKey("M_value.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "Lambda_serial",
                    sqla.Integer,
                    sqla.ForeignKey("Lambda_value.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column("n_value", sqla.Integer, nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        M: M_value = payload["M"]
        Lambda: Lambda_value = payload["Lambda"]
        n: int = payload["n"]

        units: UnitsLike = payload["units"]

        query = sqla.select(
            table.c.serial,
        ).filter(
            table.c.M_serial == M.store_id,
            table.c.Lambda_serial == Lambda.store_id,
            table.c.n_value == n,
        )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! ExponentialPotential.build(): multiple results found when querying for ExponentialPotential"
            )
            raise e

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {
                "M_serial": M.store_id,
                "Lambda_serial": Lambda.store_id,
                "n_value": n,
            }
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        # return the constructed object
        obj = ExponentialPotential(
            store_id=store_id,
            M=M,
            Lambda=Lambda,
            n=n,
            units=units,
        )
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj
