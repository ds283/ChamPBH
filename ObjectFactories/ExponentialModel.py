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
The exponential model's table (jordan-normalization prompt 07, S1; README §6.0 R6, R14): one row
per (M, n, beta), every Lagrangian parameter of the model in one row. It replaces the tables of
ExponentialPotential (M, Lambda, n) and ExponentialCoupling (beta). The normalization of the
potential is not a parameter of the model but of each history, which finds it by tuning and stores
it (ScalarHistory, R7), so the table holds no Lambda.

The table is sharded by beta, its shard key the beta_value row, as ExponentialCoupling's was
(config/sharding.py); a ScalarHistory takes the same shard key from its model.
"""

import sqlalchemy as sqla
from sqlalchemy.exc import MultipleResultsFound

from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import ExponentialModel
from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.store_inventory import InventorySpec, Parent
from Units.base import UnitsLike


class sqla_ExponentialModel_factory(SQLAFactoryBase):
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
        # build() matches M_serial, n_value and beta_serial, and not version, as the factories of
        # ExponentialPotential and ExponentialCoupling did (dk-and-rpk-migration prompt 09).
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
                f"!! ExponentialModel.build(): multiple results found when querying for ExponentialModel"
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
        obj = ExponentialModel(store_id, M, n, beta, units)
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj
