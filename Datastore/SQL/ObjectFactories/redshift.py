from typing import Optional

import sqlalchemy as sqla
from ComputeTargets import ModelProxy, BackgroundModel
from sqlalchemy import and_

from CosmologyConcepts import redshift
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from defaults import DEFAULT_REDSHIFT_RELATIVE_PRECISION


class sqla_redshift_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    @staticmethod
    def register():
        return {
            "version": False,
            "timestamp": True,
            "columns": [
                sqla.Column("z", sqla.Float(64), index=True),
            ],
        }

    @staticmethod
    def build(payload, conn, table, inserter, tables, inserters):
        z = payload["z"]
        is_source = payload["is_source"]
        is_response = payload["is_response"]

        # query for this redshift in the datastore
        query = sqla.select(
            table.c.serial,
            table.c.source,
            table.c.response,
        ).filter(
            sqla.func.abs((table.c.z - z) / z) < DEFAULT_REDSHIFT_RELATIVE_PRECISION
        )
        row_data = conn.execute(query).one_or_none()

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {"z": z, "source": is_source, "response": is_response}
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        # return the constructed object
        obj = redshift(
            store_id=store_id,
            z=z,
        )
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj

    @staticmethod
    def read_table(
        conn,
        table,
        tables,
        model_proxy: Optional[ModelProxy] = None,
    ):
        # query for all redshift records in the table
        query = sqla.select(
            table.c.serial,
            table.c.z,
        )

        if model_proxy is not None:
            bgv_table = tables["BackgroundModelValue"]
            model: BackgroundModel = model_proxy.get()

            # force a join to the BackgroundModelValue table, on redshift serial and matching model number
            # the net result will filter out only those redshifts that are used for the intended model
            query = query.join(
                bgv_table,
                and_(
                    bgv_table.c.model_serial == model.store_id,
                    bgv_table.c.z_serial == table.c.serial,
                ),
            )

        rows = conn.execute(query.order_by(table.c.z))

        return [
            redshift(
                store_id=row.serial,
                z=row.z,
            )
            for row in rows
        ]
