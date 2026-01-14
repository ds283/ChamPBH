from math import fabs

import sqlalchemy as sqla

from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from config.defaults import (
    DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION,
    DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION,
)


class sqla_dimensionless_quantity_factory(SQLAFactoryBase):
    def __init__(self, ObjectType):
        self.ObjectType = ObjectType

    def register(self):
        return {
            "version": False,
            "timestamp": True,
            "columns": [
                sqla.Column("value", sqla.Float(64), index=True),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        value = payload["value"]

        if fabs(value) == 0:
            query = sqla.select(
                table.c.serial,
            ).filter(
                sqla.func.abs(table.c.value - value)
                < DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION
            )
        else:
            query = sqla.select(
                table.c.serial,
            ).filter(
                sqla.func.abs((table.c.value - value) / value)
                < DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION
            )
        row_data = conn.execute(query).one_or_none()

        # if this quantity is not already present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {"value": value}
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
    ):
        # query for all value records in the table
        query = sqla.select(
            table.c.serial,
            table.c.value,
        )

        rows = conn.execute(query.order_by(table.c.value))

        return [self.ObjectType(store_id=row.serial, value=row.value) for row in rows]
