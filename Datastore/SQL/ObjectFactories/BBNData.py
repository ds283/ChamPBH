from math import log
from typing import Optional, List

import sqlalchemy as sqla
from sqlalchemy import and_, or_
from sqlalchemy.exc import MultipleResultsFound, SQLAlchemyError

from ComputeTargets import (
    BBNData,
    BBNDataValue,
    ScalarModelProxy,
    ScalarModel,
)
from CosmologyConcepts import redshift, redshift_array
from CosmologyModels import BaseCosmology
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from MetadataConcepts import store_tag
from Units.base import UnitsLike
from config.defaults import DEFAULT_STRING_LENGTH


class sqla_BBNDataTagAssociation_factory(SQLAFactoryBase):
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
                    "bbn_serial",
                    sqla.Integer,
                    sqla.ForeignKey("BBNData.serial"),
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
    def add_tag(conn, inserter, bbn: BBNData, tag: store_tag):
        inserter(
            conn,
            {
                "bbn_serial": bbn.store_id,
                "tag_serial": tag.store_id,
            },
        )

    @staticmethod
    def remove_tag(conn, table, bbn: BBNData, tag: store_tag):
        conn.execute(
            sqla.delete(table).where(
                and_(
                    table.c.bbn_serial == bbn.store_id,
                    table.c.tag_serial == tag.store_id,
                )
            )
        )


class sqla_BBNDataFactory(SQLAFactoryBase):
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
                sqla.Column("failure", sqla.Boolean, default=False, nullable=False),
                sqla.Column("Yp_BBN", sqla.Float(64), nullable=True),
                sqla.Column("DOverH", sqla.Float(64), nullable=True),
                sqla.Column("He3OverH", sqla.Float(64), nullable=True),
                sqla.Column("Li7OverH", sqla.Float(64), nullable=True),
                sqla.Column("label", sqla.String(DEFAULT_STRING_LENGTH), nullable=True),
                sqla.Column("small_network", sqla.Boolean, nullable=True),
                sqla.Column(
                    "PryM_version", sqla.String(DEFAULT_STRING_LENGTH), nullable=True
                ),
                sqla.Column("z_samples", sqla.Integer, nullable=True),
                sqla.Column("NP_compute_time", sqla.Float(64), nullable=True),
                sqla.Column("BBN_compute_time", sqla.Float(64), nullable=True),
                sqla.Column("validated", sqla.Boolean, default=False, nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        label: Optional[str] = payload.get("label", None)
        tags: List[store_tag] = payload.get("tags", [])

        model_proxy: ScalarModelProxy = payload["model_proxy"]
        model: ScalarModel = model_proxy.get()
        cosmology: BaseCosmology = model.cosmology

        failure: Optional[bool] = payload.get("failure", False)

        redshift_table = tables["redshift"]

        # find if there is an existing record for this model
        query = sqla.select(
            table.c.serial,
            table.c.failure,
            table.c.Yp_BBN,
            table.c.DOverH,
            table.c.He3OverH,
            table.c.Li7OverH,
            table.c.label,
            table.c.small_network,
            table.c.PryM_version,
            table.c.z_samples,
            table.c.NP_compute_time,
            table.c.BBN_compute_time,
        ).filter(
            table.c.model_serial == model_proxy.store_id,
        )

        # filter by failure flag if provided
        if failure is not None:
            query = query.filter(table.c.failure == failure)

        # require that the integration we search for has the specified list of tags
        tag_table = tables["BBNData_tags"]
        count = 0
        for tag in tags:
            tag: store_tag
            tab = tag_table.alias(f"tag_{count}")
            count += 1
            query = query.join(
                tab,
                and_(
                    tab.c.bbn_serial == table.c.serial,
                    tab.c.tag_serial == tag.store_id,
                ),
            )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(f"!! BBNData.build(): multiple results found")
            raise e

        if row_data is None:
            return BBNData(None, model_proxy=model_proxy, label=label, tags=tags)

        store_id = row_data.serial
        store_label = row_data.label

        num_expected_samples = row_data.z_samples

        do_not_populate = payload.get("_do_not_populate", False)
        if not do_not_populate:
            # read out sample values associated with this BBN data block
            value_table = tables["BBNDataValue"]

            # load values
            value_query = (
                sqla.select(
                    value_table.c.serial,
                    value_table.c.z_serial,
                    redshift_table.c.z,
                    value_table.c.raw_N,
                    value_table.c.log_T_Jordan_MeV,
                    value_table.c.density_NP_MeV4,
                    value_table.c.pressure_NP_MeV4,
                    value_table.c.density_NP_ratio,
                )
                .select_from(
                    value_table.join(
                        redshift_table,
                        value_table.c.z_serial == redshift_table.c.serial,
                    )
                )
                .filter(value_table.c.bbn_serial == store_id)
                .order_by(redshift_table.c.z.desc())
            )
            value_rows = conn.execute(value_query).all()

            z_points = []
            values = []

            units: UnitsLike = cosmology.units
            MeV2 = units.MeV * units.MeV
            MeV4 = MeV2 * MeV2
            log_MeV = log(units.MeV)

            for row in value_rows:
                z_value = redshift(
                    store_id=row.z_serial,
                    z=row.z,
                )
                z_points.append(z_value)
                values.append(
                    BBNDataValue(
                        row.serial,
                        z=z_value,
                        raw_N=row.raw_N,
                        log_T_Jordan=row.log_T_Jordan_MeV + log_MeV,
                        density_NP=row.density_NP_MeV4 * MeV4,
                        pressure_NP=row.pressure_NP_MeV4 * MeV4,
                        density_NP_ratio=row.density_NP_ratio,
                    )
                )
            imported_z_sample = redshift_array(z_points)

            if num_expected_samples is not None:
                if len(imported_z_sample) != num_expected_samples:
                    raise RuntimeError(
                        f'Fewer z-samples than expected were recovered from the validated BBNData "{store_label}"'
                    )

            attributes = {"_deserialized": True}
        else:
            values = None

            attributes = {
                "_do_not_populate": True,
                "_deserialized": True,
            }

        obj = BBNData(
            {
                "store_id": store_id,
                "failure": row_data.failure,
                "Yp_BBN": row_data.Yp_BBN,
                "small_network": row_data.small_network,
                "PryM_version": row_data.PryM_version,
                "DOverH": row_data.DOverH,
                "He3OverH": row_data.He3OverH,
                "Li7OverH": row_data.Li7OverH,
                "NP_compute_time": row_data.NP_compute_time,
                "BBN_compute_time": row_data.BBN_compute_time,
                "values": values,
            },
            model_proxy,
            label=store_label,
            tags=tags,
        )
        for key, value in attributes.items():
            setattr(obj, key, value)
        return obj

    def store(self, obj: BBNData, conn, table, inserter, tables, inserters):
        payload = {
            "model_serial": obj._model_proxy.store_id,
            "label": obj.label,
            "failure": obj._failure,
            "small_network": obj.small_network if not obj._failure else None,
            "PryM_version": obj.PRyM_version if not obj._failure else None,
            "Yp_BBN": obj.Yp_BBN if not obj._failure else None,
            "DOverH": obj.DOverH if not obj._failure else None,
            "He3OverH": obj.He3OverH if not obj._failure else None,
            "Li7OverH": obj.Li7OverH if not obj._failure else None,
            "z_samples": len(obj.values) if not obj._failure else None,
            "NP_compute_time": obj.NP_compute_time if not obj._failure else None,
            "BBN_compute_time": obj.BBN_compute_time if not obj._failure else None,
            "validated": False,
        }

        # set store_id on behalf of the BBNData instance
        store_id = inserter(conn, payload)
        obj._my_id = store_id

        # add any tags that have been specified
        tag_inserter = inserters["BBNData_tags"]
        for tag in obj.tags:
            sqla_BBNDataTagAssociation_factory.add_tag(conn, tag_inserter, obj, tag)

        model: ScalarModel = obj._model_proxy.get()
        cosmology: BaseCosmology = model.cosmology
        units: UnitsLike = cosmology.units

        log_MeV = log(units.MeV)
        MeV2 = units.MeV * units.MeV
        MeV4 = MeV2 * MeV2

        value_inserter = inserters["BBNDataValue"]
        for val in obj._values:
            val: BBNDataValue
            v_payload = {
                "bbn_serial": store_id,
                "z_serial": val.z.store_id,
                "raw_N": val._raw_N,
                "log_T_Jordan_MeV": val._log_T_Jordan - log_MeV,
                "density_NP_MeV4": val._density_NP / MeV4,
                "pressure_NP_MeV4": val._pressure_NP / MeV4,
                "density_NP_ratio": val._density_NP_ratio,
            }

            # set store_id on behalf of the BBNDataValue instance
            v_id = value_inserter(conn, v_payload)
            val._my_id = v_id

        return obj

    def validate(self, obj: BBNData, conn, table, tables):
        # check if this object is present in the database and validated
        if not obj.available:
            raise RuntimeError(
                "Attempt to validate a datastore object that has not yet been serialized"
            )

        # treat integration failures as validated
        if obj._failure:
            validated: bool = True

        else:
            expected_samples = conn.execute(
                sqla.select(table.c.z_samples).filter(table.c.serial == obj.store_id)
            ).scalar()

            value_table = tables["BBNDataValue"]
            num_samples = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.bbn_serial == obj.store_id
                )
            ).scalar()

            # check if we counted as many rows as we expected
            validated: bool = num_samples == expected_samples

        if not validated:
            print(
                f'!! WARNING: BBNData "{obj.label}" did not validate after serialization (expected samples={expected_samples}, number stored={num_samples})'
            )

        conn.execute(
            sqla.update(table)
            .where(table.c.serial == obj.store_id)
            .values(validated=validated)
        )

        return validated

    def validate_on_startup(self, conn, table, tables, prune=False):
        # check all records in the table and ensure they are valid

        value_table = tables["BBNDataValue"]
        tags_table = tables["BBNData_tags"]

        not_validated = list(
            conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.label,
                    table.c.z_samples,
                ).filter(
                    and_(
                        table.c.failure == False,
                        or_(table.c.validated == False, table.c.validated == None),
                    )
                )
            )
        )

        if len(not_validated) == 0:
            return []

        msgs = [
            ">> BBNData instances",
            "     The following unvalidated models were detected in the datastore:",
        ]
        for bbn_data in not_validated:
            msgs.append(f'       -- "{bbn_data.label}" (store_id={bbn_data.serial})')
            rows = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.bbn_serial == bbn_data.serial,
                )
            ).scalar()
            msgs.append(
                f"          contains {rows} z-sample values | expected={bbn_data.z_samples}"
            )

        if prune:
            invalid_serials = [nv.serial for nv in not_validated]
            try:
                conn.execute(
                    sqla.delete(value_table).where(
                        value_table.c.bbn_serial.in_(invalid_serials)
                    )
                )
                conn.execute(
                    sqla.delete(tags_table).where(
                        tags_table.c.bbn_serial.in_(invalid_serials)
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

    def inventory(self, conn, table, tables):
        version_table = tables["version"]

        query = sqla.select(
            version_table.c.label.label("version_label"),
            table.c.timestamp,
            table.c.label,
            table.c.validated,
        ).join(version_table, table.c.version == version_table.c.serial)

        rows = conn.execute(query)

        data = {
            "validated": {
                "earliest_timestamp": None,
                "latest_timestamp": None,
                "versions": set(),
                "labels": [],
            },
            "unvalidated": {
                "earliest_timestamp": None,
                "latest_timestamp": None,
                "versions": set(),
                "labels": [],
            },
        }

        for item in rows:
            if item.validated:
                group = data["validated"]
            else:
                group = data["unvalidated"]

            if (
                group["latest_timestamp"] is None
                or item.timestamp > group["latest_timestamp"]
            ):
                group["latest_timestamp"] = item.timestamp

            if (
                group["earliest_timestamp"] is None
                or item.timestamp < group["earliest_timestamp"]
            ):
                group["earliest_timestamp"] = item.timestamp

            if item.version_label not in group["versions"]:
                group["versions"].add(item.version_label)

            group["labels"].append(item.label)

        return data


class sqla_BBNDataValue_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": False,
            "timestamp": False,
            "stepping": False,
            "columns": [
                sqla.Column(
                    "bbn_serial",
                    sqla.Integer,
                    sqla.ForeignKey("BBNData.serial"),
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
                sqla.Column("raw_N", sqla.Float(64), nullable=False),
                sqla.Column("log_T_Jordan_MeV", sqla.Float(64), nullable=False),
                sqla.Column("density_NP_MeV4", sqla.Float(64), nullable=False),
                sqla.Column("pressure_NP_MeV4", sqla.Float(64), nullable=False),
                sqla.Column("density_NP_ratio", sqla.Float(64), nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        BBN_serial = payload["BBN_serial"]
        units: UnitsLike = payload["units"]

        z: redshift = payload["z"]
        raw_N: float = payload["raw_N"]

        log_T_Jordan: float = payload["log_T_Jordan"]
        density_NP: float = payload["density_NP"]
        pressure_NP: float = payload["pressure_NP"]
        density_NP_ratio: float = payload["density_NP_ratio"]

        # define quantities in explicit units
        MeV2 = units.MeV * units.MeV
        MeV4 = MeV2 * MeV2
        log_MeV = log(units.MeV)

        log_T_Jordan_MeV: float = log_T_Jordan - log_MeV
        density_NP_MeV4: float = density_NP / MeV4
        pressure_NP_MeV4: float = pressure_NP / MeV4

        try:
            row_data = conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.raw_N,
                    table.c.log_T_Jordan_MeV,
                    table.c.density_NP_MeV4,
                    table.c.pressure_NP_MeV4,
                    table.c.density_NP_ratio,
                ).filter(
                    table.c.bbn_serial == BBN_serial,
                    table.c.z_serial == z.store_id,
                )
            ).one_or_none()
        except MultipleResultsFound as e:
            print(f"!! BBNDataValue.build(): multiple results found")
            raise e

        if row_data is None:
            store_id = inserter(
                conn,
                {
                    "bbn_serial": BBN_serial,
                    "z_serial": z.store_id,
                    "raw_N": raw_N,
                    "log_T_Jordan_MeV": log_T_Jordan_MeV,
                    "density_NP_MeV4": density_NP_MeV4,
                    "pressure_NP_MeV4": pressure_NP_MeV4,
                    "density_NP_ratio": density_NP_ratio,
                },
            )
        else:
            store_id = row_data.serial

            # replace supplied values with those read from the dataabse
            raw_N = row_data.raw_N

            log_T_Jordan = row_data.log_T_Jordan_MeV + log_MeV
            density_NP = row_data.density_NP_MeV4 * MeV4
            pressure_NP = row_data.pressure_NP_MeV4 * MeV4
            density_NP_ratio = row_data.density_NP_ratio

        obj = BBNDataValue(
            store_id,
            z=z,
            raw_N=raw_N,
            log_T_Jordan=log_T_Jordan,
            density_NP=density_NP,
            pressure_NP=pressure_NP,
            density_NP_ratio=density_NP_ratio,
        )
        obj._deserialized = True
        return obj
