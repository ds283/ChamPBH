from math import fabs

import sqlalchemy as sqla

from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from Units.base import UnitsLike
from config.defaults import (
    DEFAULT_DIMENSIONFUL_QUANTITY_PRECISION,
    DEFAULT_DIMENSIONFUL_QUANTITY_RELATIVE_PRECISION,
)


class sqla_dimensionful_quantity_factory(SQLAFactoryBase):
    def __init__(self, ObjectType):
        self.ObjectType = ObjectType

        self.value_col: str = f"value_{ObjectType.default_unit}"

    def register(self):
        return {
            "version": False,
            "timestamp": True,
            "columns": [
                sqla.Column(self.value_col, sqla.Float(64), index=True),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        value = payload["value"]
        units = payload["units"]

        try:
            unit = getattr(units, self.ObjectType.default_unit)
        except TypeError as e:
            print(
                f'TypeError encountered in sqla_dimensionful_quantity_factory.build(): self.ObjectType="{self.ObjectType.__name__}", self.ObjectType.default_unit="{self.ObjectType.default_unit}"'
            )
            raise e

        if unit is None:
            raise RuntimeError(
                f'default_unit must be a class attribute of specified object type "{self.ObjectType.__name__}"'
            )
        value_in_units = value / unit

        if fabs(value_in_units) == 0:
            query = sqla.select(
                table.c.serial,
            ).filter(
                sqla.func.abs(table.c[self.value_col] - value_in_units)
                < DEFAULT_DIMENSIONFUL_QUANTITY_PRECISION
            )
        else:
            query = sqla.select(
                table.c.serial,
            ).filter(
                sqla.func.abs(
                    (table.c[self.value_col] - value_in_units) / value_in_units
                )
                < DEFAULT_DIMENSIONFUL_QUANTITY_RELATIVE_PRECISION
            )
        row_data = conn.execute(query).one_or_none()

        # if this quantity is not already present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {self.value_col: value_in_units}
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        # return the constructed object
        obj = self.ObjectType(
            store_id=store_id,
            value=value,
        )
        for k, v in attribute_set.items():
            setattr(obj, k, v)
        return obj

    def read_table(
        self,
        conn,
        table,
        units: UnitsLike,
    ):
        unit = getattr(units, self.ObjectType.default_unit)

        # query for all value records in the table
        query = sqla.select(
            table.c.serial,
            table.c[self.value_col],
        )

        rows = conn.execute(query.order_by(table.c[self.value_col]))

        return [
            self.ObjectType(
                store_id=row.serial, value=row._mapping[self.value_col] * unit
            )
            for row in rows
        ]
