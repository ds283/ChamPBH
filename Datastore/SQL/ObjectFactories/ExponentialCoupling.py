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

import sqlalchemy as sqla
from sqlalchemy.exc import MultipleResultsFound

from CosmologyConcepts import beta_value
from CosmologyConcepts.ConformalCouplings import ExponentialCoupling
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from Units.base import UnitsLike


class sqla_ExponentialCoupling_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": True,
            "timestamp": True,
            "columns": [
                sqla.Column(
                    "beta_serial",
                    sqla.Integer,
                    sqla.ForeignKey("beta_value.serial"),
                    index=True,
                    nullable=False,
                ),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        beta: beta_value = payload["beta"]

        units: UnitsLike = payload["units"]

        query = sqla.select(
            table.c.serial,
        ).filter(
            table.c.beta_serial == beta.store_id,
        )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! ExponentialCoupling.build(): multiple results found when querying for ExponentialCoupling"
            )
            raise e

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {
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
        obj = ExponentialCoupling(
            store_id=store_id,
            beta=beta,
            units=units,
        )
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj
