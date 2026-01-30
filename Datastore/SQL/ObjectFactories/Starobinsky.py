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

from CosmologyConcepts import M_value, Lambda_value
from CosmologyConcepts.Potentials import StarobinskyPotential
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from Units.base import UnitsLike


class sqla_StarobinskyPotential_factory(SQLAFactoryBase):
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
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        M: M_value = payload["M"]
        Lambda: Lambda_value = payload["Lambda"]

        units: UnitsLike = payload["units"]

        query = sqla.select(
            table.c.serial,
        ).filter(
            table.c.M_serial == M.store_id,
            table.c.Lambda_serial == Lambda.store_id,
        )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! StarobinskyPotential.build(): multiple results found when querying for StarobinskyPotential"
            )
            raise e

        # if not present, create a new id using the provided inserter
        if row_data is None:
            insert_data = {
                "M_serial": M.store_id,
                "Lambda_serial": Lambda.store_id,
            }
            if "serial" in payload:
                insert_data["serial"] = payload["serial"]
            store_id = inserter(conn, insert_data)
            attribute_set = {"_new_insert": True}
        else:
            store_id = row_data.serial
            attribute_set = {"_deserialized": True}

        # return the constructed object
        obj = StarobinskyPotential(
            store_id=store_id,
            M=M,
            Lambda=Lambda,
            units=units,
        )
        for key, value in attribute_set.items():
            setattr(obj, key, value)
        return obj
