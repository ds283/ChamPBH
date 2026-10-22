# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Optional

import sqlalchemy as sqla
from sqlalchemy import and_

from ComputeTargets import ScalarHistoryProxy, ScalarHistory
from CosmologyConcepts import redshift
from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.store_inventory import InventorySpec
from config.defaults import DEFAULT_LOG_ONE_PLUS_Z_PRECISION


class sqla_redshift_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": False,
            "timestamp": True,
            "columns": [
                sqla.Column("log_one_plus_z", sqla.Float(64), index=True),
            ],
        }

    def inventory_spec(self):
        # build() matches log_one_plus_z alone, by absolute difference (dk-and-rpk-migration
        # prompt 09; the leaf was z, matched relatively, until jordan-normalization prompt 03)
        return InventorySpec(leaves=("log_one_plus_z",))

    def build(self, payload, conn, table, inserter, tables, inserters):
        x = payload["log_one_plus_z"]

        # query for this point of the axis: |stored - x| < tol, in one branch with no special
        # case at zero (jordan-normalization prompt 03, P8). It is written as a range, not as
        # abs(column - x), so that SQLite can search the column's index rather than scan the table
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        query = sqla.select(table.c.serial).filter(
            (table.c.log_one_plus_z > x - tol) & (table.c.log_one_plus_z < x + tol)
        )

        row_data = conn.execute(query).one_or_none()

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {"log_one_plus_z": x}
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
            log_one_plus_z=x,
        )
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj

    def read_table(
        self,
        conn,
        table,
        tables,
        model_proxy: Optional[ScalarHistoryProxy] = None,
    ):
        # query for all redshift records in the table
        query = sqla.select(
            table.c.serial,
            table.c.log_one_plus_z,
        )

        if model_proxy is not None:
            bgv_table = tables["ScalarHistoryValue"]
            model: ScalarHistory = model_proxy.get()

            # force a join to the ScalarHistoryValue table, on redshift serial and matching model number
            # the net result will filter out only those redshifts that are used for the intended model
            query = query.join(
                bgv_table,
                and_(
                    bgv_table.c.history_serial == model.store_id,
                    bgv_table.c.z_serial == table.c.serial,
                ),
            )

        rows = conn.execute(query.order_by(table.c.log_one_plus_z))

        return [
            redshift(
                store_id=row.serial,
                log_one_plus_z=row.log_one_plus_z,
            )
            for row in rows
        ]
