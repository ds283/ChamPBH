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

import json
from math import fabs, log
from typing import Optional, List

import sqlalchemy as sqla
from sqlalchemy import and_, or_
from sqlalchemy.exc import MultipleResultsFound, SQLAlchemyError

from ComputeTargets import (
    ScalarModel,
    ScalarModelValue,
)
from CosmologyConcepts import (
    redshift_array,
    redshift,
    temperature,
    phi_value,
    pi_value,
)
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
from MetadataConcepts import store_tag, tolerance
from Quadrature.integration_metadata import IntegrationData, IntegrationSolver
from Units.base import UnitsLike
from config.defaults import DEFAULT_STRING_LENGTH, DEFAULT_FLOAT_PRECISION


class sqla_ScalarModelTagAssociation_factory(SQLAFactoryBase):
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
                    "model_serial",
                    sqla.Integer,
                    sqla.ForeignKey("ScalarModel.serial"),
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
    def add_tag(conn, inserter, model: ScalarModel, tag: store_tag):
        inserter(
            conn,
            {
                "model_serial": model.store_id,
                "tag_serial": tag.store_id,
            },
        )

    @staticmethod
    def remove_tag(conn, table, model: ScalarModel, tag: store_tag):
        conn.execute(
            sqla.delete(table).where(
                and_(
                    table.c.model_serial == model.store_id,
                    table.c.tag_serial == tag.store_id,
                )
            )
        )


class sqla_ScalarModelFactory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": True,
            "stepping": False,
            "timestamp": True,
            "validate_on_startup": True,
            "columns": [
                sqla.Column("label", sqla.String(DEFAULT_STRING_LENGTH), nullable=True),
                sqla.Column("cosmology_type", sqla.Integer, index=True, nullable=False),
                sqla.Column(
                    "cosmology_serial", sqla.Integer, index=True, nullable=False
                ),
                sqla.Column("potential_type", sqla.Integer, index=True, nullable=False),
                sqla.Column(
                    "potential_serial", sqla.Integer, index=True, nullable=False
                ),
                sqla.Column("coupling_type", sqla.Integer, index=True, nullable=False),
                sqla.Column(
                    "coupling_serial", sqla.Integer, index=True, nullable=False
                ),
                sqla.Column(
                    "atol_serial",
                    sqla.Integer,
                    sqla.ForeignKey("tolerance.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "rtol_serial",
                    sqla.Integer,
                    sqla.ForeignKey("tolerance.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "phi_Einstein_init_serial",
                    sqla.Integer,
                    sqla.ForeignKey("phi_value.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "pi_Einstein_init_serial",
                    sqla.Integer,
                    sqla.ForeignKey("pi_value.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "T_Jordan_init_serial",
                    sqla.Integer,
                    sqla.ForeignKey("temperature.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column(
                    "T_Jordan_stop_serial",
                    sqla.Integer,
                    sqla.ForeignKey("temperature.serial"),
                    index=True,
                    nullable=False,
                ),
                sqla.Column("failure", sqla.Boolean, default=False, nullable=False),
                sqla.Column(
                    "solver_serial",
                    sqla.Integer,
                    sqla.ForeignKey("IntegrationSolver.serial"),
                    index=True,
                    nullable=True,
                ),
                sqla.Column("z_samples", sqla.Integer, nullable=True),
                sqla.Column("compute_time", sqla.Float(64), nullable=True),
                sqla.Column("compute_steps", sqla.Integer, nullable=True),
                sqla.Column("RHS_evaluations", sqla.Integer, nullable=True),
                sqla.Column("mean_RHS_time", sqla.Float(64), nullable=True),
                sqla.Column("max_RHS_time", sqla.Float(64), nullable=True),
                sqla.Column("min_RHS_time", sqla.Float(64), nullable=True),
                sqla.Column("validated", sqla.Boolean, default=False, nullable=False),
                sqla.Column(
                    "extra_data", sqla.String(DEFAULT_STRING_LENGTH), nullable=True
                ),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        label: Optional[str] = payload.get("label", None)
        tags: List[store_tag] = payload.get("tags", [])

        cosmology: BaseCosmology = payload["cosmology"]

        T_init: temperature = payload.get("T_Jordan_init")
        T_stop: temperature = payload.get("T_Jordan_stop")

        phi_Einstein_init: phi_value = payload.get("phi_Einstein_init")
        pi_Einstein_init: pi_value = payload.get("pi_Einstein_init")

        potential: AbstractPotential = payload.get("potential")
        coupling: AbstractCoupling = payload.get("coupling")

        z_grid: Optional[redshift_array] = payload.get("z_grid", None)

        failure: Optional[bool] = payload.get("failure", None)
        solver_labels = payload["solver_labels"]

        atol: tolerance = payload["atol"]
        rtol: tolerance = payload["rtol"]

        atol_table = tables["tolerance"].alias("atol")
        rtol_table = tables["tolerance"].alias("rtol")
        solver_table = tables["IntegrationSolver"]
        tag_table = tables["ScalarModel_tags"]
        redshift_table = tables["redshift"]

        # notice that we query only for validated data
        query = (
            sqla.select(
                table.c.serial,
                table.c.compute_time,
                table.c.compute_steps,
                table.c.RHS_evaluations,
                table.c.mean_RHS_time,
                table.c.max_RHS_time,
                table.c.min_RHS_time,
                table.c.failure,
                table.c.solver_serial,
                table.c.label,
                table.c.z_samples,
                table.c.extra_data,
                solver_table.c.label.label("solver_label"),
                solver_table.c.stepping.label("solver_stepping"),
                atol_table.c.log10_tol.label("log10_atol"),
                rtol_table.c.log10_tol.label("log10_rtol"),
            )
            .select_from(
                table.join(
                    solver_table,
                    solver_table.c.serial == table.c.solver_serial,
                    isouter=True,  # outer join needed to allow matching failed instances
                )
                .join(atol_table, atol_table.c.serial == table.c.atol_serial)
                .join(rtol_table, rtol_table.c.serial == table.c.rtol_serial)
            )
            .filter(
                table.c.validated == True,
                table.c.cosmology_type == cosmology.type_id,
                table.c.cosmology_serial == cosmology.store_id,
                table.c.potential_type == potential.type_id,
                table.c.potential_serial == potential.store_id,
                table.c.coupling_type == coupling.type_id,
                table.c.coupling_serial == coupling.store_id,
                table.c.T_Jordan_init_serial == T_init.store_id,
                table.c.T_Jordan_stop_serial == T_stop.store_id,
                table.c.phi_Einstein_init_serial == phi_Einstein_init.store_id,
                table.c.pi_Einstein_init_serial == pi_Einstein_init.store_id,
                table.c.atol_serial == atol.store_id,
                table.c.rtol_serial == rtol.store_id,
            )
        )

        # filter by failure flag if provided
        if failure is not None:
            query = query.filter(table.c.failure == failure)

        # require that the integration we search for has the specified list of tags
        count = 0
        for tag in tags:
            tag: store_tag
            tab = tag_table.alias(f"tag_{count}")
            count += 1
            query = query.join(
                tab,
                and_(
                    tab.c.model_serial == table.c.serial,
                    tab.c.tag_serial == tag.store_id,
                ),
            )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! ScalarModel.build(): multiple results found when querying for ScalarModel"
            )
            raise e

        # we did an outer join on the solver table to allow for failed instances, so enforce that we have non-null values
        # if the failure flag is not set
        if not row_data.failure:
            if row_data.solver_label is None:
                raise RuntimeError(f"ScalarModel {label} has no solver label")

        if row_data is None:
            # build and return an unpopulated object
            return ScalarModel(
                payload=None,
                solver_labels=solver_labels,
                cosmology=cosmology,
                T_Jordan_init=T_init,
                T_Jordan_stop=T_stop,
                phi_Einstein_init=phi_Einstein_init,
                pi_Einstein_init=pi_Einstein_init,
                potential=potential,
                coupling=coupling,
                z_grid=z_grid,
                atol=atol,
                rtol=rtol,
                label=label,
                tags=tags,
            )

        store_id = row_data.serial
        store_label = row_data.label

        num_expected_samples = row_data.z_samples

        do_not_populate = payload.get("_do_not_populate", False)
        if not do_not_populate:
            # read out sample values associated with this integration
            value_table = tables["ScalarModelValue"]

            sample_rows = conn.execute(
                sqla.select(
                    value_table.c.serial,
                    value_table.c.z_serial,
                    redshift_table.c.z,
                    value_table.c.raw_N,
                    value_table.c.phi_Einstein_Mp,
                    value_table.c.pi_Einstein_Mp,
                    value_table.c.log_rhorad_Einstein_Mp4,
                    value_table.c.log_rhorad_Jordan_Mp4,
                    value_table.c.log_fm,
                    value_table.c.log_T_Jordan_GeV,
                    value_table.c.H_Einstein_Mp,
                    value_table.c.H_Jordan_Mp,
                    value_table.c.gstar_rho,
                    value_table.c.gstar_s,
                    value_table.c.dgstar_rho_dlogT,
                    value_table.c.dgstar_s_dlogT,
                    value_table.c.Sigma,
                    value_table.c.friction_term_Mp,
                    value_table.c.reflecting_term_Mp,
                    value_table.c.kicking_term_Mp,
                )
                .select_from(
                    value_table.join(
                        redshift_table,
                        redshift_table.c.serial == value_table.c.z_serial,
                    )
                )
                .filter(value_table.c.model_serial == store_id)
                .order_by(redshift_table.c.z.desc())
            )

            z_points = []
            values = []

            units: UnitsLike = cosmology.units
            log_Mp = log(units.PlanckMass)
            log_GeV = log(units.GeV)

            for row in sample_rows:
                z_value = redshift(
                    store_id=row.z_serial,
                    z=row.z,
                )
                z_points.append(z_value)
                values.append(
                    ScalarModelValue(
                        store_id=row.serial,
                        z=z_value,
                        raw_N=row.raw_N,
                        phi_Einstein=row.phi_Einstein_Mp * units.PlanckMass,
                        pi_Einstein=row.pi_Einstein_Mp * units.PlanckMass,
                        log_rhorad_Einstein=row.log_rhorad_Einstein_Mp4 + 4.0 * log_Mp,
                        log_rhorad_Jordan=row.log_rhorad_Jordan_Mp4 + 4.0 * log_Mp,
                        log_fm=row.log_fm,
                        log_T_Jordan=row.log_T_Jordan_GeV + log_GeV,
                        H_Einstein=row.H_Einstein_Mp * units.PlanckMass,
                        H_Jordan=row.H_Jordan_Mp * units.PlanckMass,
                        gstar_rho=row.gstar_rho,
                        gstar_s=row.gstar_s,
                        dgstar_rho_dlogT=row.dgstar_rho_dlogT,
                        dgstar_s_dlogT=row.dgstar_s_dlogT,
                        Sigma=row.Sigma,
                        friction_term=row.friction_term_Mp * units.PlanckMass,
                        reflecting_term=row.reflecting_term_Mp * units.PlanckMass,
                        kicking_term=row.kicking_term_Mp * units.PlanckMass,
                    )
                )
            imported_z_sample = redshift_array(z_points)

            if num_expected_samples is not None:
                if len(imported_z_sample) != num_expected_samples:
                    raise RuntimeError(
                        f'Fewer z-samples than expected were recovered from the validated ScalarModel "{store_label}"'
                    )

            attributes = {"_deserialized": True}
        else:
            values = None

            attributes = {"_do_not_populate": True, "_deserialized": True}

        # z_grid not supplied since this object has already been computed/populated
        failed = row_data.failure
        obj = ScalarModel(
            payload={
                "store_id": store_id,
                "failure": failed,
                "metadata": (
                    IntegrationData(
                        compute_time=row_data.compute_time,
                        compute_steps=row_data.compute_steps,
                        RHS_evaluations=row_data.RHS_evaluations,
                        mean_RHS_time=row_data.mean_RHS_time,
                        max_RHS_time=row_data.max_RHS_time,
                        min_RHS_time=row_data.min_RHS_time,
                    )
                    if not failed
                    else None
                ),
                "solver": (
                    (
                        IntegrationSolver(
                            store_id=row_data.solver_serial,
                            label=row_data.solver_label,
                            stepping=row_data.solver_stepping,
                        )
                    )
                    if not failed
                    else None
                ),
                "extra_data": (
                    json.loads(row_data.extra_data)
                    if row_data.extra_data is not None
                    else None
                ),
                "values": values,
            },
            solver_labels=solver_labels,
            cosmology=cosmology,
            T_Jordan_init=T_init,
            T_Jordan_stop=T_stop,
            phi_Einstein_init=phi_Einstein_init,
            pi_Einstein_init=pi_Einstein_init,
            potential=potential,
            coupling=coupling,
            atol=atol,
            rtol=rtol,
            label=store_label,
            tags=tags,
        )
        for key, value in attributes.items():
            setattr(obj, key, value)
        return obj

    def store(
        self,
        obj: ScalarModel,
        conn,
        table,
        inserter,
        tables,
        inserters,
    ):
        payload = {
            "label": obj.label,
            "cosmology_type": obj.cosmology.type_id,
            "cosmology_serial": obj.cosmology.store_id,
            "potential_type": obj.potential.type_id,
            "potential_serial": obj.potential.store_id,
            "coupling_type": obj.coupling.type_id,
            "coupling_serial": obj.coupling.store_id,
            "T_Jordan_init_serial": obj.T_Jordan_init.store_id,
            "T_Jordan_stop_serial": obj.T_Jordan_stop.store_id,
            "phi_Einstein_init_serial": obj.phi_Einstein_init.store_id,
            "pi_Einstein_init_serial": obj.pi_Einstein_init.store_id,
            "atol_serial": obj._atol.store_id,
            "rtol_serial": obj._rtol.store_id,
            "failure": obj._failure,
            "solver_serial": obj.solver.store_id if not obj._failure else None,
            "z_samples": len(obj._values) if not obj._failure else None,
            "compute_time": obj.metadata.compute_time if not obj._failure else None,
            "compute_steps": obj.metadata.compute_steps if not obj._failure else None,
            "RHS_evaluations": (
                obj.metadata.RHS_evaluations if not obj._failure else None
            ),
            "mean_RHS_time": obj.metadata.mean_RHS_time if not obj._failure else None,
            "max_RHS_time": obj.metadata.max_RHS_time if not obj._failure else None,
            "min_RHS_time": obj.metadata.min_RHS_time if not obj._failure else None,
            "validated": False,
            "extra_data": (
                json.dumps(obj._extra_data) if obj._extra_data is not None else None
            ),
        }

        # # because ScalarModel is a replicated table, we need to allow for the possibility that this object
        # # is a replica, rather than a fresh insert. If so, it's _my_id field will be set.
        # if hasattr(obj, "_my_id") and obj._my_id is not None:
        #     payload.update({"serial": obj._my_id})

        store_id = inserter(conn, payload)

        # set store_id on behalf of the ScalarModel instance
        obj._my_id = store_id

        # add any tags that have been specified
        tag_inserter = inserters["ScalarModel_tags"]
        for tag in obj.tags:
            sqla_ScalarModelTagAssociation_factory.add_tag(conn, tag_inserter, obj, tag)

        # now serialize the sampled output points
        units: UnitsLike = obj._units
        log_Mp = log(units.PlanckMass)
        log_GeV = log(units.GeV)

        value_inserter = inserters["ScalarModelValue"]
        for value in obj._values:
            value: ScalarModelValue
            value_id = value_inserter(
                conn,
                {
                    "model_serial": store_id,
                    "z_serial": value.z.store_id,
                    "raw_N": value.raw_N,
                    "phi_Einstein_Mp": value.phi_Einstein / units.PlanckMass,
                    "pi_Einstein_Mp": value.pi_Einstein / units.PlanckMass,
                    "log_rhorad_Einstein_Mp4": value.log_rhorad_Einstein - 4.0 * log_Mp,
                    "log_rhorad_Jordan_Mp4": value.log_rhorad_Jordan - 4.0 * log_Mp,
                    "log_fm": value.log_fm,
                    "log_T_Jordan_GeV": value.log_T_Jordan - log_GeV,
                    "H_Einstein_Mp": value.H_Einstein / units.PlanckMass,
                    "H_Jordan_Mp": value.H_Jordan / units.PlanckMass,
                    "gstar_rho": value.gstar_rho,
                    "gstar_s": value.gstar_s,
                    "dgstar_rho_dlogT": value.dgstar_rho_dlogT,
                    "dgstar_s_dlogT": value.dgstar_s_dlogT,
                    "Sigma": value.Sigma,
                    "friction_term_Mp": value.friction_term / units.PlanckMass,
                    "reflecting_term_Mp": value.reflecting_term / units.PlanckMass,
                    "kicking_term_Mp": value.kicking_term / units.PlanckMass,
                },
            )

            # set store_id on behalf of the ScalarModelValue instance
            value._my_id = value_id

        return obj

    def validate(
        self,
        obj: ScalarModel,
        conn,
        table,
        tables,
    ):
        # query the row in ScalarModel corresponding to this object
        if not obj.available:
            raise RuntimeError(
                "Attempt to validate a datastore object that has not yet been serialized"
            )

        # treat integration failures as validated
        if obj._failure:
            validated = True

        else:
            expected_samples = conn.execute(
                sqla.select(table.c.z_samples).filter(table.c.serial == obj.store_id)
            ).scalar()

            value_table = tables["ScalarModelValue"]
            num_samples = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.model_serial == obj.store_id
                )
            ).scalar()

            # check if we counted as many rows as we expected
            validated: bool = num_samples == expected_samples

        if not validated:
            print(
                f'!! WARNING: ScalarModel "{obj.label}" did not validate after serialization (expected samples={expected_samples}, number stored={num_samples})'
            )

        conn.execute(
            sqla.update(table)
            .where(table.c.serial == obj.store_id)
            .values(validated=validated)
        )

        return validated

    def validate_on_startup(self, conn, table, tables, prune=False):
        # query the datastore for any integrations that are not validated

        atol_table = tables["tolerance"].alias("atol")
        rtol_table = tables["tolerance"].alias("rtol")
        solver_table = tables["IntegrationSolver"]
        value_table = tables["ScalarModelValue"]
        tags_table = tables["ScalarModel_tags"]

        # bake results into a list so that we can close this query; we are going to want to run
        # another one as we process the rows from this one
        not_validated = list(
            conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.label,
                    table.c.z_samples,
                    solver_table.c.label.label("solver_label"),
                    atol_table.c.log10_tol.label("log10_atol"),
                    rtol_table.c.log10_tol.label("log10_rtol"),
                )
                .select_from(
                    table.join(
                        solver_table,
                        solver_table.c.serial == table.c.solver_serial,
                        isouter=True,  # outer join needed to allow matching failed instances
                    )
                    .join(atol_table, atol_table.c.serial == table.c.atol_serial)
                    .join(rtol_table, rtol_table.c.serial == table.c.rtol_serial)
                )
                .filter(
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
            ">> ScalarModel instances",
            "     The following unvalidated models were detected in the datastore:",
        ]
        for model in not_validated:
            msgs.append(
                f'       -- "{model.label}" (store_id={model.serial}, log10_atol={model.log10_atol}, log10_rtol={model.log10_rtol})'
            )
            rows = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.model_serial == model.serial,
                )
            ).scalar()
            msgs.append(
                f"          contains {rows} z-sample values | expected={model.z_samples}"
            )

        if prune:
            invalid_serials = [nv.serial for nv in not_validated]
            try:
                conn.execute(
                    sqla.delete(value_table).where(
                        value_table.c.model_serial.in_(invalid_serials)
                    )
                )
                conn.execute(
                    sqla.delete(tags_table).where(
                        tags_table.c.model_serial.in_(invalid_serials)
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


class sqla_ScalarModelValue_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": False,
            "timestamp": False,
            "stepping": False,
            "columns": [
                sqla.Column(
                    "model_serial",
                    sqla.Integer,
                    sqla.ForeignKey("ScalarModel.serial"),
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
                sqla.Column("phi_Einstein_Mp", sqla.Float(64), nullable=False),
                sqla.Column("pi_Einstein_Mp", sqla.Float(64), nullable=False),
                sqla.Column("log_rhorad_Einstein_Mp4", sqla.Float(64), nullable=False),
                sqla.Column("log_rhorad_Jordan_Mp4", sqla.Float(64), nullable=False),
                sqla.Column("log_fm", sqla.Float(64), nullable=False),
                sqla.Column("log_T_Jordan_GeV", sqla.Float(64), nullable=False),
                sqla.Column("H_Einstein_Mp", sqla.Float(64), nullable=False),
                sqla.Column("H_Jordan_Mp", sqla.Float(64), nullable=False),
                sqla.Column("gstar_rho", sqla.Float(64), nullable=False),
                sqla.Column("gstar_s", sqla.Float(64), nullable=False),
                sqla.Column("dgstar_rho_dlogT", sqla.Float(64), nullable=False),
                sqla.Column("dgstar_s_dlogT", sqla.Float(64), nullable=False),
                sqla.Column("Sigma", sqla.Float(64), nullable=False),
                sqla.Column("friction_term_Mp", sqla.Float(64), nullable=False),
                sqla.Column("reflecting_term_Mp", sqla.Float(64), nullable=False),
                sqla.Column("kicking_term_Mp", sqla.Float(64), nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        model_serial = payload["model_serial"]
        units: UnitsLike = payload["units"]

        z: redshift = payload["z"]
        raw_N: float = payload["raw_N"]

        phi_Einstein: float = payload["phi_Einstein"]
        pi_Einstein: float = payload["pi_Einstein"]

        log_rhorad_Einstein: float = payload["log_rhorad_Einstein"]
        log_rhorad_Jordan: float = payload["log_rhorad_Jordan"]
        log_fm: float = payload["log_fm"]
        log_T_Jordan: float = payload["log_T_Jordan"]

        H_Einstein: float = payload["H_Einstein"]
        H_Jordan: float = payload["H_Jordan"]

        gstar_rho: float = payload["gstar_rho"]
        gstar_s: float = payload["gstar_s"]
        dgstar_rho_dlogT: float = payload["dgstar_rho_dlogT"]
        dgstar_s_dlogT: float = payload["dgstar_s_dlogT"]

        Sigma: float = payload["Sigma"]

        friction_term: float = payload["friction_term"]
        reflecting_term: float = payload["reflecting_term"]
        kicking_term: float = payload["kicking_term"]

        # define quantities in explicit units
        log_Mp = log(units.PlanckMass)
        log_GeV = log(units.GeV)

        phi_Einstein_Mp: float = phi_Einstein / units.PlanckMass
        pi_Einstein_Mp: float = pi_Einstein / units.PlanckMass

        log_rhorad_Einstein_Mp4: float = log_rhorad_Einstein - 4.0 * log_Mp
        log_rhorad_Jordan_Mp4: float = log_rhorad_Jordan - 4.0 * log_Mp
        log_T_Jordan_GeV: float = log_T_Jordan - log_GeV

        H_Einstein_Mp: float = H_Einstein / units.PlanckMass
        H_Jordan_Mp: float = H_Jordan / units.PlanckMass

        friction_term_Mp: float = friction_term / units.PlanckMass
        reflecting_term_Mp: float = reflecting_term / units.PlanckMass
        kicking_term_Mp: float = kicking_term / units.PlanckMass

        try:
            row_data = conn.execute(
                sqla.select(
                    table.c.serial,
                    table.c.raw_N,
                    table.c.phi_Einstein_Mp,
                    table.c.pi_Einstein_Mp,
                    table.c.log_rhorad_Einstein_Mp4,
                    table.c.log_rhorad_Jordan_Mp4,
                    table.c.log_fm,
                    table.c.log_T_Jordan_GeV,
                    table.c.H_Einstein_Mp,
                    table.c.H_Jordan_Mp,
                    table.c.gstar_rho,
                    table.c.gstar_s,
                    table.c.Sigma,
                    table.c.dgstar_rho_dlogT,
                    table.c.dgstar_s_dlogT,
                    table.c.friction_term_Mp,
                    table.c.reflecting_term_Mp,
                    table.c.kicking_term_Mp,
                ).filter(
                    table.c.model_serial == model_serial,
                    table.c.z_serial == z.store_id,
                )
            ).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! ScalarModelValue.build(): multiple results found when querying for ScalarModelValue"
            )
            raise e

        if row_data is None:
            store_id = inserter(
                conn,
                {
                    "model_serial": model_serial,
                    "z_serial": z.store_id,
                    "raw_N": raw_N,
                    "phi_Einstein_Mp": phi_Einstein_Mp,
                    "pi_Einstein_Mp": pi_Einstein_Mp,
                    "log_rhorad_Einstein_Mp4": log_rhorad_Einstein_Mp4,
                    "log_rhorad_Jordan_Mp4": log_rhorad_Jordan_Mp4,
                    "log_fm": log_fm,
                    "log_T_Jordan_GeV": log_T_Jordan_GeV,
                    "H_Einstein_Mp": H_Einstein_Mp,
                    "H_Jordan_Mp": H_Jordan_Mp,
                    "gstar_rho": gstar_rho,
                    "gstar_s": gstar_s,
                    "dgstar_rho_dlogT": dgstar_rho_dlogT,
                    "dgstar_s_dlogT": dgstar_s_dlogT,
                    "Sigma": Sigma,
                    "friction_term_Mp": friction_term_Mp,
                    "reflecting_term_Mp": reflecting_term_Mp,
                    "kicking_term_Mp": kicking_term_Mp,
                },
            )
        else:
            store_id = row_data.serial

            # phi_Einstein = row_data.phi_Einstein_Mp * units.PlanckMass
            # pi_Einstein = row_data.pi_Einstein_Mp * units.PlanckMass

            # replace supplied values with those read from the database
            raw_N = row_data.raw_N

            log_rhorad_Einstein = row_data.log_rhorad_Einstein_Mp4 + 4.0 * log_Mp
            log_rhorad_Jordan = row_data.log_rhorad_Jordan_Mp4 + 4.0 * log_Mp
            log_fm = row_data.log_fm
            log_T_Jordan = row_data.log_T_Jordan_GeV + log_GeV

            H_Einstein = row_data.H_Einstein_Mp * units.PlanckMass
            H_Jordan = row_data.H_Jordan_Mp * units.PlanckMass

            gstar_rho = row_data.gstar_rho
            gstar_s = row_data.gstar_s
            dgstar_rho_dlogT = row_data.dgstar_rho_dlogT
            dgstar_s_dlogT = row_data.dgstar_s_dlogT

            Sigma = row_data.Sigma

            friction_term = row_data.friction_term_Mp * units.PlanckMass
            reflecting_term = row_data.reflecting_term_Mp * units.PlanckMass
            kicking_term = row_data.kicking_term_Mp * units.PlanckMass

            # validate that recovered values match the supplied values, at least for phi and pi
            # (we could do this for all of the values but it is laborious)
            if (
                fabs(row_data.phi_Einstein_Mp - phi_Einstein_Mp)
                > DEFAULT_FLOAT_PRECISION
            ):
                raise ValueError(
                    f"Stored ScalarModel phi(Einstein)/Mp value (model store_id={model_serial}, z={z.store_id}) = {row_data.phi_Einstein_Mp}/Mp differs from expected value = {phi_Einstein_Mp}/Mp"
                )

            if (
                fabs(row_data.phi_Einstein_Mp - pi_Einstein_Mp)
                > DEFAULT_FLOAT_PRECISION
            ):
                raise ValueError(
                    f"Stored ScalarModel pi(Einstein)/Mp value (model store_id={model_serial}, z={z.store_id}) = {row_data.pi_Einstein_Mp}/Mp differs from expected value = {pi_Einstein_Mp}/Mp"
                )

        obj = ScalarModelValue(
            store_id=store_id,
            z=z,
            raw_N=raw_N,
            phi_Einstein=phi_Einstein,
            pi_Einstein=pi_Einstein,
            log_rhorad_Einstein=log_rhorad_Einstein,
            log_rhorad_Jordan=log_rhorad_Jordan,
            log_fm=log_fm,
            log_T_Jordan=log_T_Jordan,
            H_Einstein=H_Einstein,
            H_Jordan=H_Jordan,
            gstar_rho=gstar_rho,
            gstar_s=gstar_s,
            Sigma=Sigma,
            dgstar_rho_dlogT=dgstar_rho_dlogT,
            dgstar_s_dlogT=dgstar_s_dlogT,
            friction_term=friction_term,
            reflecting_term=reflecting_term,
            kicking_term=kicking_term,
        )
        obj._deserialized = True
        return obj
