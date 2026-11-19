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
The inverse-power-law model's table (power-law-wall prompt 05, README §6.3 P1, §6.9 P33): one row per (M, n, beta),
every Lagrangian parameter of InversePowerModel (CosmologyConcepts/Models/InversePowerModel.py, V_0 [1 + (M/phi)^n])
in one row. It is written as ExponentialModel's table is (ObjectFactories/ExponentialModel.py), with the same
columns, the same key and the same inventory spec, so that the two specs' digests are equal; only the table's name
and the class it builds differ. As for the exponential, the normalization V_0 is not a parameter of the model but of
each history, which finds it by tuning and stores it, so the table holds no V_0.

A ScalarHistory names its model by (model_type, model_serial): this table's rows are those of model_type
INVERSE_POWER_MODEL = 2 (CosmologyConcepts/Models/model_ids.py). Its serials are its own, so the first row of this
table and the first row of ExponentialModel's both take serial 1; anything that joins a model table on model_serial
must also match model_type (README §6.9 P37).

The table is sharded by beta, its shard key the beta_value row, as ExponentialModel's is (config/sharding.py); a
ScalarHistory takes the same shard key from its model.
"""

import sqlalchemy as sqla
from sqlalchemy.exc import MultipleResultsFound

from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import InversePowerModel
from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.store_inventory import InventorySpec, Parent
from Units.base import UnitsLike


class sqla_InversePowerModel_factory(SQLAFactoryBase):
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
                sqla.Column("n_value", sqla.Integer, nullable=False),
                sqla.Column(
                    "beta_serial",
                    sqla.Integer,
                    sqla.ForeignKey("beta_value.serial"),
                    index=True,
                    nullable=False,
                ),
            ],
        }

    def inventory_spec(self):
        # ExponentialModel's spec (P33): build() matches M_serial, n_value and beta_serial, and not version.
        # M_value and beta_value are replicated, so both are on this class's shard
        return InventorySpec(
            leaves=("n_value",),
            parents={
                "M": Parent("M_serial", "M_value"),
                "beta": Parent("beta_serial", "beta_value"),
            },
        )

    def build(self, payload, conn, table, inserter, tables, inserters):
        M: M_value = payload["M"]
        n: int = payload["n"]
        beta: beta_value = payload["beta"]

        units: UnitsLike = payload["units"]

        query = sqla.select(
            table.c.serial,
        ).filter(
            table.c.M_serial == M.store_id,
            table.c.n_value == n,
            table.c.beta_serial == beta.store_id,
        )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! InversePowerModel.build(): multiple results found when querying for InversePowerModel"
            )
            raise e

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {
                "M_serial": M.store_id,
                "n_value": n,
                "beta_serial": beta.store_id,
            }
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        # return the constructed object
        obj = InversePowerModel(store_id, M, n, beta, units)
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj
