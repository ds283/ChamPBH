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

"""
The factory of alpha_value (one-loop-drain prompt 03 §1.1; README §8.10 decision 1, §8.11 P27; orchestrator
note 03, ruling 1).

Until prompt 03 alpha_value shared sqla_dimensionless_quantity_factory with beta_value. A row is now the pair
(value, drain_model), unique: the value is alpha under DRAIN_MODEL_FIXED and the one-loop law's A under
DRAIN_MODEL_ONE_LOOP. build() checks the pair (check_drain_model, the one statement of the rules) before it
queries or inserts, so that an unknown model, A = 0 under the one-loop model, or alpha >= 1 under the fixed one is
a ValueError with no row written. It matches the value as the dimensionless factory does (relatively, or
absolutely at 0) and drain_model exactly. A payload that names no drain_model asks for DRAIN_MODEL_FIXED, so that
object_get("alpha_value", value=a) is the lookup of before. The table stays replicated.
"""

from math import fabs

import sqlalchemy as sqla

from CosmologyConcepts import alpha_value
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED, check_drain_model
from config.defaults import (
    DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION,
    DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION,
    DEFAULT_STRING_LENGTH,
)
from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.store_inventory import InventorySpec


class sqla_alpha_value_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": False,
            "timestamp": True,
            "columns": [
                sqla.Column("value", sqla.Float(64), index=True),
                # the drain model, a string from CosmologyConcepts.drain_model.DRAIN_MODELS (one-loop-drain
                # prompt 03): with value, the key of the row
                sqla.Column(
                    "drain_model",
                    sqla.String(DEFAULT_STRING_LENGTH),
                    index=True,
                    nullable=False,
                ),
            ],
        }

    def inventory_spec(self):
        # build() matches the pair: its value column, relatively, and drain_model exactly
        return InventorySpec(leaves=("value", "drain_model"))

    def build(self, payload, conn, table, inserter, tables, inserters):
        value = payload["value"]
        drain_model = payload.get("drain_model", DRAIN_MODEL_FIXED)

        # the pair, before any query or insert
        check_drain_model(drain_model, value, "alpha_value")

        if fabs(value) == 0:
            value_match = (
                sqla.func.abs(table.c.value - value)
                < DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION
            )
        else:
            value_match = (
                sqla.func.abs((table.c.value - value) / value)
                < DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION
            )
        query = sqla.select(table.c.serial).filter(
            value_match, table.c.drain_model == drain_model
        )
        row_data = conn.execute(query).one_or_none()

        # if this pair is not already present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {"value": value, "drain_model": drain_model}
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        obj = alpha_value(store_id=store_id, value=value, drain_model=drain_model)
        for k, v in attribute_set.items():
            setattr(obj, k, v)
        return obj

    def read_table(
        self,
        conn,
        table,
    ):
        # every row, the pair of each
        query = sqla.select(
            table.c.serial,
            table.c.value,
            table.c.drain_model,
        )

        rows = conn.execute(query.order_by(table.c.value, table.c.drain_model))

        return [
            alpha_value(
                store_id=row.serial, value=row.value, drain_model=row.drain_model
            )
            for row in rows
        ]
