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

from typing import Optional, List, Mapping

import sqlalchemy as sqla
from sqlalchemy import and_, or_
from sqlalchemy.exc import MultipleResultsFound, SQLAlchemyError

from ComputeTargets import (
    AdiabaticHistory,
    AdiabaticHistoryValue,
    ScalarModelProxy,
)
from CosmologyConcepts import redshift, redshift_array
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from MetadataConcepts import store_tag
from config.defaults import DEFAULT_STRING_LENGTH


class sqla_AdiabaticHistoryTagAssociation_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "serial": False,
            "version": False,
            "stepping": False,
            "timestamp": True,
            "columns": [
                sqla.Column(
                    "history_serial",
                    sqla.Integer,
                    sqla.ForeignKey("AdiabaticHistory.serial"),
                    index=True,
                    nullable=False,
                    primary_key=True,
                ),
                sqla.Column(
                    "tag_serial",
                    sqla.Integer,
                    sqla.ForeignKey("store_tag.serial"),
                    index=True,
                    nullable=False,
                    primary_key=True,
                ),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        raise NotImplementedError

    @staticmethod
    def add_tag(conn, inserter, history: AdiabaticHistory, tag: store_tag):
        inserter(
            conn,
            {
                "history_serial": history.store_id,
                "tag_serial": tag.store_id,
            },
        )

    @staticmethod
    def remove_tag(conn, table, history: AdiabaticHistory, tag: store_tag):
        conn.execute(
            sqla.delete(table).where(
                and_(
                    table.c.history_serial == history.store_id,
                    table.c.tag_serial == tag.store_id,
                )
            )
        )


class sqla_AdiabaticHistoryFactory(SQLAFactoryBase):
    _Q_labels = AdiabaticHistory.Q_labels

    def __init__(self):
        pass

    def register(self):
        return {
            "version": True,
            "stepping": False,
            "timestamp": True,
            "validate_on_startup": True,
            "columns": [
                sqla.Column(
                    "model_serial",
                    sqla.Integer,
                    sqla.ForeignKey("ScalarModel.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column("compute_time", sqla.Float(64)),
            ]
            + [
                sqla.Column(f"max_abs_Q_{label}", sqla.Float(64))
                for label in self._Q_labels
            ]
            + [
                sqla.Column("label", sqla.String(DEFAULT_STRING_LENGTH), nullable=True),
                sqla.Column("z_samples", sqla.Integer, nullable=False),
                sqla.Column("validated", sqla.Boolean, default=False, nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        label: Optional[str] = payload.get("label", None)
        tags: List[store_tag] = payload.get("tags", [])

        model_proxy: ScalarModelProxy = payload["model_proxy"]

        max_Q_value_cols = [table.c[f"max_abs_Q_{label}"] for label in self._Q_labels]

        # find if there is an existing record for this model
        query = sqla.select(
            table.c.serial,
            *max_Q_value_cols,
            table.c.z_samples,
            table.c.label,
            table.c.compute_time,
        ).filter(
            table.c.model_serial == model_proxy.store_id,
        )

        # require that the integration we search for has the specified list of tags
        tag_table = tables["AdiabaticHistory_tags"]
        count = 0
        for tag in tags:
            tag: store_tag
            tab = tag_table.alias(f"tag_{count}")
            count += 1
            query = query.join(
                tab,
                and_(
                    tab.c.history_serial == table.c.serial,
                    tab.c.tag_serial == tag.store_id,
                ),
            )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! AdiabaticHistory.build(): multiple results found when querying for AdiabaticHistoryValue"
            )
            raise e

        if row_data is None:
            # build and return an unpopulated object
            return AdiabaticHistory(
                None, model_proxy=model_proxy, label=label, tags=tags
            )

        store_id = row_data.serial
        store_label = row_data.label

        # found an existing record, so retrieve its values
        num_expected_samples = row_data.z_samples
        value_table = tables["AdiabaticHistoryValue"]
        redshift_table = tables["redshift"]
        value_columns = [value_table.c[f"abs_Q_{label}"] for label in self._Q_labels]

        value_query = (
            sqla.select(
                value_table.c.serial,
                value_table.c.z_serial == value_table.c.z_serial,
                redshift_table.c.z,
                value_table.c.raw_N,
                *value_columns,
            )
            .select_from(
                value_table.join(
                    redshift_table, redshift_table.c.serial == value_table.c.z_serial
                )
            )
            .filter(value_table.c.history_serial == store_id)
            .order_by(redshift_table.c.z.desc())
        )

        value_rows = conn.execute(value_query).fetchall()

        z_points = []
        values = []
        for v_row in value_rows:
            z_value = redshift(
                store_id=v_row.z_serial,
                z=v_row.z,
            )
            z_points.append(z_value)
            values.append(
                AdiabaticHistoryValue(
                    store_id=v_row.serial,
                    z=z_value,
                    raw_N=v_row.raw_N,
                    values={
                        label: v_row._mapping[f"abs_Q_{label}"]
                        for label in self._Q_labels
                    },
                )
            )
        imported_z_sample = redshift_array(z_points)

        if num_expected_samples is not None:
            if len(imported_z_sample) != num_expected_samples:
                raise RuntimeError(
                    f'Fewer z-samples than expected were recovered from the validated AdiabaticHistory "{store_label}"'
                )
        obj = AdiabaticHistory(
            {
                "store_id": store_id,
                "values": values,
                "max_abs_Q_values": {
                    label: row_data._mapping[f"Q_{label}"] for label in self._Q_labels
                },
                "compute_time": row_data.compute_time,
            },
            model_proxy,
            label=store_label,
            tags=tags,
        )
        obj._deserialized = True
        return obj

    def store(self, obj: AdiabaticHistory, conn, table, inserter, tables, inserters):
        payload = {
            "model_serial": obj._model_proxy.store_id,
            "label": obj.label,
            "z_samples": len(obj.values),
            "compute_time": obj._compute_time,
            "validated": False,
        }
        for label in self._Q_labels:
            payload[f"max_abs_Q_{label}"] = obj.max_abs_Q(label)

        store_id = inserter(conn, payload)

        # set store_id on behalf of the ScalarModel instance
        obj._my_id = store_id

        # add any tags that have been specified
        tag_inserter = inserters["AdiabaticHistory_tags"]
        for tag in obj.tags:
            sqla_AdiabaticHistoryTagAssociation_factory.add_tag(
                conn, tag_inserter, obj, tag
            )

        value_inserter = inserters["AdiabaticHistoryValue"]
        for value in obj.values:
            value: AdiabaticHistoryValue
            payload = {
                "history_serial": store_id,
                "z_serial": value.z.store_id,
                "raw_N": value.raw_N,
            }
            for label in self._Q_labels:
                payload[f"abs_Q_{label}"] = value.value(label)

            value_id = value_inserter(
                conn,
                payload,
            )

            # set store_id on behalf of the AdiabaticHistoryValue instance
            value._my_id = value_id

        return obj

    def validate(self, obj: AdiabaticHistory, conn, table, tables):
        # check if this object is present in the database and validated
        if not obj.available:
            raise RuntimeError(
                "Attempt to validate a datastore object that has not yet been serialized"
            )

        expected_samples = conn.execute(
            sqla.select(table.c.z_samples).filter(table.c.serial == obj.store_id)
        ).scalar()

        value_table = tables["AdiabaticHistoryValue"]
        num_samples = conn.execute(
            sqla.select(sqla.func.count(value_table.c.serial)).filter(
                value_table.c.history_serial == obj.store_id
            )
        ).scalar()

        # check if we counted as many rows as we expected
        validated: bool = num_samples == expected_samples
        if not validated:
            print(
                f'!! WARNING: AdiabaticHistory "{obj.label}" did not validate after serialization (expected samples={expected_samples}, number stored={num_samples})'
            )

        conn.execute(
            sqla.update(table)
            .where(table.c.serial == obj.store_id)
            .values(validated=validated)
        )

        return validated

    def validate_on_startup(self, conn, table, tables, prune=False):
        # check all records in the table and ensure they are valid
        # for AdiabaticHistory, we could check if the associated ScalarModel still exists

        value_table = tables["AdiabaticHistoryValue"]
        tags_table = tables["AdiabaticHistory_tags"]

        not_validated = list(
            conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.label,
                    table.c.z_samples,
                ).filter(or_(table.c.validated == False, table.c.validated == None))
            )
        )

        if len(not_validated) == 0:
            return []

        msgs = [
            ">> AdiabaticHistory instances",
            "     The following unvalidated models were detected in the datastore:",
        ]
        for history in not_validated:
            msgs.append(f'       -- "{history.label}" (store_id={history.serial})')
            rows = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.history_serial == history.serial,
                )
            ).scalar()
            msgs.append(
                f"          contains {rows} z-sample values | expected={history.z_samples}"
            )

        if prune:
            invalid_serials = [nv.serial for nv in not_validated]
            try:
                conn.execute(
                    sqla.delete(value_table).where(
                        value_table.c.history_serial.in_(invalid_serials)
                    )
                )
                conn.execute(
                    sqla.delete(tags_table).where(
                        tags_table.c.history_serial.in_(invalid_serials)
                    )
                )
                conn.execute(
                    sqla.delete(table).where(table.c.serial.in_(invalid_serials))
                )
            except SQLAlchemyError:
                msgs.append(
                    f"!!        DATABASE ERROR encountered when pruning these values"
                )
                pass
            else:
                msgs.append(
                    f"     ** Note: these values have been pruned from the datastore."
                )

        return msgs


class sqla_AdiabaticHistoryValue_factory(SQLAFactoryBase):
    _Q_labels = AdiabaticHistory.Q_labels

    def __init__(self):
        pass

    def register(self):
        columns = [
            sqla.Column(
                "history_serial",
                sqla.Integer,
                sqla.ForeignKey("AdiabaticHistory.serial"),
                index=True,
                nullable=False,
            ),
            sqla.Column(
                "z_serial",
                sqla.Integer,
                sqla.ForeignKey("redshift.serial"),
                index=True,
                nullable=False,
            ),
        ]

        # add columns for each adiabatic history label
        for label in self._Q_labels:
            columns.append(
                sqla.Column(f"abs_Q_{label}", sqla.Float(64), nullable=False)
            )

        return {
            "version": False,
            "timestamp": False,
            "stepping": False,
            "columns": columns,
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        history_serial = payload["history_serial"]
        z: redshift = payload["z"]
        raw_N: float = payload["raw_N"]
        values: Mapping[str, float] = payload["values"]

        # ensure redshift is stored
        z_serial = inserters["redshift"](conn, z, tables, inserters)
        try:
            columns = [table.c[f"abs_Q_{label}"] for label in self._Q_labels]
            row_data = conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.raw_N,
                    *columns,
                ).filter(
                    table.c.history_serial == history_serial,
                    table.c.z_serial == z_serial,
                )
            ).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! AdiabaticHistoryValue.build(): multiple results found when querying for AdiabaticaHistoryValue"
            )
            raise e

        if row_data is None:
            payload = {
                "history_serial": history_serial,
                "z_serial": z_serial,
                "raw_N": row_data.raw_N,
            }
            for label in self._Q_labels:
                payload[f"abs_Q_{label}"] = values[label]

            store_id = inserter(conn, payload)

        else:
            store_id = row_data.serial

            raw_N = row_data.raw_N

        obj = AdiabaticHistoryValue(
            store_id=store_id,
            z=z,
            raw_N=raw_N,
            values=values,
        )
        obj._deserialized = True
        return obj
