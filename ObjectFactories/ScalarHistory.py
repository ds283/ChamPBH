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
from math import log
from typing import Optional, List

import sqlalchemy as sqla
from sqlalchemy import and_, or_
from sqlalchemy.exc import MultipleResultsFound, SQLAlchemyError

from ComputeTargets import (
    ScalarHistory,
    ScalarHistoryValue,
)
from ComputeTargets.ScalarHistory import (
    FirstBounce,
    FixedTValues,
    TUNING_OUTPUT_KEYS,
    TuningValues,
)
from CosmologyConcepts import (
    redshift_array,
    redshift,
    temperature,
    phi_value,
    pi_value,
)
from CosmologyConcepts.Models import AbstractModel
from CosmologyConcepts.Models.model_ids import EXPONENTIAL_MODEL
from CosmologyModels import BaseCosmology
from CosmologyModels.model_ids import XAV_IMPROVED_EOS_IDENTIFIER
from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.store_inventory import InventorySpec, Parent
from MetadataConcepts import store_tag, tolerance
from Quadrature.integration_metadata import IntegrationData, IntegrationSolver
from Units.base import UnitsLike
from config.defaults import DEFAULT_STRING_LENGTH
from datastorekit.contract import require_version_serial

# the tuning's outputs stored in units of the Planck mass, and in units of M_P^4
# (jordan-normalization prompt 07, S3); the others are stored as tune_history returns them
_TUNING_KEYS_IN_MP = ("phi_ref", "phi_0", "pi_0", "seed_phi_ref")
_TUNING_KEYS_IN_MP4 = ("V_ref",)


def _tuning_unit(key: str, units: UnitsLike) -> Optional[float]:
    """The unit a tuning output is stored in: M_P, M_P^4, or None (stored as it is)."""
    if key in _TUNING_KEYS_IN_MP:
        return units.PlanckMass
    if key in _TUNING_KEYS_IN_MP4:
        return units.PlanckMass**4
    return None


def _tuning_columns(tuning: Optional[TuningValues], units: UnitsLike) -> dict:
    """The columns of TUNING_OUTPUT_KEYS for a row: each in its stored unit, or all None."""
    columns = {}
    for key in TUNING_OUTPUT_KEYS:
        value = None if tuning is None else getattr(tuning, key)
        unit = _tuning_unit(key, units)
        columns[key] = value / unit if value is not None and unit is not None else value
    return columns


def _tuning_from_row(row_data, units: UnitsLike) -> TuningValues:
    """The TuningValues of a row read back: each column times its stored unit."""
    values = {}
    for key in TUNING_OUTPUT_KEYS:
        value = getattr(row_data, key)
        unit = _tuning_unit(key, units)
        values[key] = value * unit if value is not None and unit is not None else value
    return TuningValues(**values)


class sqla_ScalarHistoryTagAssociation_factory(SQLAFactoryBase):
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
                    sqla.ForeignKey("ScalarHistory.serial"),
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
    def add_tag(conn, inserter, model: ScalarHistory, tag: store_tag):
        inserter(
            conn,
            {
                "history_serial": model.store_id,
                "tag_serial": tag.store_id,
            },
        )

    @staticmethod
    def remove_tag(conn, table, model: ScalarHistory, tag: store_tag):
        conn.execute(
            sqla.delete(table).where(
                and_(
                    table.c.history_serial == model.store_id,
                    table.c.tag_serial == tag.store_id,
                )
            )
        )


class sqla_ScalarHistoryFactory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": True,
            # lookups return only rows made under the current version label (run-integrity prompt 01)
            "key_on_version": True,
            "stepping": False,
            "timestamp": True,
            "validate_on_startup": True,
            "columns": [
                sqla.Column("label", sqla.String(DEFAULT_STRING_LENGTH), nullable=True),
                sqla.Column("cosmology_type", sqla.Integer, index=True, nullable=False),
                sqla.Column(
                    "cosmology_serial", sqla.Integer, index=True, nullable=False
                ),
                # the model, a polymorphic reference with no foreign key, as the cosmology
                # (jordan-normalization prompt 07, S2): model_type is the model's type_id
                # (CosmologyConcepts/Models/model_ids.py), which names its table
                sqla.Column("model_type", sqla.Integer, index=True, nullable=False),
                sqla.Column("model_serial", sqla.Integer, index=True, nullable=False),
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
                # the tolerance on both residuals of the tuning (jordan-normalization prompt 07,
                # S2; README §6.2 P7): part of the key, since a history tuned to another tolerance
                # is another history
                sqla.Column(
                    "tuning_tol_serial",
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
                sqla.Column("failure", sqla.Boolean, default=False, nullable=False),
                # why the history failed; NULL on a success (science-readiness prompt 02)
                sqla.Column(
                    "failure_reason", sqla.String(DEFAULT_STRING_LENGTH), nullable=True
                ),
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
                # the first bounce (science-readiness prompt 03; README §2 (g)). All four are
                # NULL when the history had no bounce, and on a failure row. The e-fold number
                # is dimensionless; phi is stored in units of the Planck mass and ln T_J with T_J
                # in GeV, as the ScalarHistoryValue columns phi_Einstein_Mp and log_T_Jordan_GeV
                sqla.Column("first_bounce_N", sqla.Float(64), nullable=True),
                sqla.Column("first_bounce_log_T_Jordan", sqla.Float(64), nullable=True),
                sqla.Column("first_bounce_phi_Einstein", sqla.Float(64), nullable=True),
                sqla.Column("first_bounce_reflected", sqla.Boolean, nullable=True),
                # phi and rho_NP/rho_R,J at T_J = 1 MeV and 70 keV (science-readiness prompt 06b;
                # README §2 (n)). phi is stored in units of the Planck mass, as
                # first_bounce_phi_Einstein; the ratio is dimensionless. A pair is NULL when the
                # history did not reach that temperature, and all four are NULL on a failure row
                sqla.Column("phi_Einstein_1MeV", sqla.Float(64), nullable=True),
                sqla.Column("density_NP_ratio_1MeV", sqla.Float(64), nullable=True),
                sqla.Column("phi_Einstein_70keV", sqla.Float(64), nullable=True),
                sqla.Column("density_NP_ratio_70keV", sqla.Float(64), nullable=True),
                # what the tuning found (jordan-normalization prompt 07, S3; README §6.2 P6): the
                # keys of TUNING_OUTPUT_KEYS, as tune_history returns them. All are NULL on a
                # failure row. normalization and seed_normalization are stored unconverted, since
                # only the model can interpret them (P6): for ExponentialModel each is ln V_0 in
                # the units of the run that stored it, which for every driver is Planck_units.
                # V_ref = V(phi_ref) is stored in units of M_P^4; phi_ref, phi_0 (the field
                # today), pi_0 (its velocity dphi/dN today) and seed_phi_ref in units of the
                # Planck mass, as first_bounce_phi_Einstein. tracking = pi_0/(1.5 phi_0),
                # residual_H = H_J/H0 - 1 and residual_phi = phi_ref/phi_min - 1 at T_CMB are
                # dimensionless. tuning_iterations counts the integrations, the seed's included;
                # plain_steps, secant_steps and guard_steps count the steps of each kind;
                # seed_kind is the seed's kind (tracking, static or plain)
                sqla.Column("normalization", sqla.Float(64), nullable=True),
                sqla.Column("V_ref", sqla.Float(64), nullable=True),
                sqla.Column("phi_ref", sqla.Float(64), nullable=True),
                sqla.Column("phi_0", sqla.Float(64), nullable=True),
                sqla.Column("pi_0", sqla.Float(64), nullable=True),
                sqla.Column("tracking", sqla.Float(64), nullable=True),
                sqla.Column("residual_H", sqla.Float(64), nullable=True),
                sqla.Column("residual_phi", sqla.Float(64), nullable=True),
                sqla.Column("tuning_iterations", sqla.Integer, nullable=True),
                sqla.Column("plain_steps", sqla.Integer, nullable=True),
                sqla.Column("secant_steps", sqla.Integer, nullable=True),
                sqla.Column("guard_steps", sqla.Integer, nullable=True),
                sqla.Column(
                    "seed_kind", sqla.String(DEFAULT_STRING_LENGTH), nullable=True
                ),
                sqla.Column("seed_normalization", sqla.Float(64), nullable=True),
                sqla.Column("seed_phi_ref", sqla.Float(64), nullable=True),
                sqla.Column("validated", sqla.Boolean, default=False, nullable=False),
                sqla.Column(
                    "extra_data", sqla.String(DEFAULT_STRING_LENGTH), nullable=True
                ),
            ],
        }

    def inventory_spec(self):
        # the key is what build() matches: the two type/serial pairs (the cosmology and the
        # model), the initial temperature, phi and pi, the three tolerances (atol, rtol and the
        # tuning's) and the version (jordan-normalization prompt 07, S2). failure is matched only
        # when the payload gives it, and is a leaf so that the inventory shows it (campaign README
        # P15); the stored label, a job name and a time, is not. The polymorphic maps are those of
        # the two model_ids modules: only XAV_IMPROVED_EOS_IDENTIFIER names a stored cosmology.
        # The model is on this row's shard, since both take their shard key from beta
        # (dk-and-rpk-migration prompt 09; jordan-normalization prompt 07)
        return InventorySpec(
            leaves=("cosmology_type", "model_type", "failure"),
            parents={
                "version": Parent("version", "version"),
                "cosmology": Parent(
                    "cosmology_serial",
                    type_column="cosmology_type",
                    types={XAV_IMPROVED_EOS_IDENTIFIER: "QCD_Cosmology"},
                ),
                "model": Parent(
                    "model_serial",
                    type_column="model_type",
                    types={EXPONENTIAL_MODEL: "ExponentialModel"},
                ),
                "T_init": Parent("T_Jordan_init_serial", "temperature"),
                "phi_init": Parent("phi_Einstein_init_serial", "phi_value"),
                "pi_init": Parent("pi_Einstein_init_serial", "pi_value"),
                "atol": Parent("atol_serial", "tolerance"),
                "rtol": Parent("rtol_serial", "tolerance"),
                "tuning_tol": Parent("tuning_tol_serial", "tolerance"),
            },
            tags=("ScalarHistory_tags", "history_serial"),
            values=("ScalarHistoryValue", "history_serial"),
            validated="validated",
        )

    def build(self, payload, conn, table, inserter, tables, inserters):
        label: Optional[str] = payload.get("label", None)
        tags: List[store_tag] = payload.get("tags", [])

        cosmology: BaseCosmology = payload["cosmology"]

        T_init: temperature = payload.get("T_Jordan_init")

        phi_Einstein_init: phi_value = payload.get("phi_Einstein_init")
        pi_Einstein_init: pi_value = payload.get("pi_Einstein_init")

        model: AbstractModel = payload.get("model")

        z_grid: Optional[redshift_array] = payload.get("z_grid", None)

        failure: Optional[bool] = payload.get("failure", None)
        solver_labels = payload["solver_labels"]

        # the lookup is keyed on the version label; it may never fall back to unfiltered
        version_serial: int = require_version_serial(payload, "ScalarHistory")

        atol: tolerance = payload["atol"]
        rtol: tolerance = payload["rtol"]
        tuning_tol: tolerance = payload["tuning_tol"]

        atol_table = tables["tolerance"].alias("atol")
        rtol_table = tables["tolerance"].alias("rtol")
        solver_table = tables["IntegrationSolver"]
        tag_table = tables["ScalarHistory_tags"]
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
                table.c.failure_reason,
                table.c.solver_serial,
                table.c.label,
                table.c.z_samples,
                table.c.extra_data,
                table.c.first_bounce_N,
                table.c.first_bounce_log_T_Jordan,
                table.c.first_bounce_phi_Einstein,
                table.c.first_bounce_reflected,
                table.c.phi_Einstein_1MeV,
                table.c.density_NP_ratio_1MeV,
                table.c.phi_Einstein_70keV,
                table.c.density_NP_ratio_70keV,
                *(table.c[key] for key in TUNING_OUTPUT_KEYS),
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
                table.c.version == version_serial,
                table.c.cosmology_type == cosmology.type_id,
                table.c.cosmology_serial == cosmology.store_id,
                table.c.model_type == model.type_id,
                table.c.model_serial == model.store_id,
                table.c.T_Jordan_init_serial == T_init.store_id,
                table.c.phi_Einstein_init_serial == phi_Einstein_init.store_id,
                table.c.pi_Einstein_init_serial == pi_Einstein_init.store_id,
                table.c.atol_serial == atol.store_id,
                table.c.rtol_serial == rtol.store_id,
                table.c.tuning_tol_serial == tuning_tol.store_id,
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
                    tab.c.history_serial == table.c.serial,
                    tab.c.tag_serial == tag.store_id,
                ),
            )

        try:
            row_data = conn.execute(query).one_or_none()
        except MultipleResultsFound as e:
            print(
                f"!! ScalarHistory.build(): multiple results found when querying for ScalarHistory"
            )
            raise e

        # we did an outer join on the solver table to allow for failed instances, so enforce that we have non-null values
        # if the failure flag is not set
        if (
            row_data is not None
            and not row_data.failure
            and row_data.solver_label is None
        ):
            raise RuntimeError(f"ScalarHistory {label} has no solver label")

        if row_data is None:
            # build and return an unpopulated object
            return ScalarHistory(
                payload=None,
                solver_labels=solver_labels,
                cosmology=cosmology,
                T_Jordan_init=T_init,
                phi_Einstein_init=phi_Einstein_init,
                pi_Einstein_init=pi_Einstein_init,
                model=model,
                tuning_tol=tuning_tol,
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
            value_table = tables["ScalarHistoryValue"]

            sample_rows = conn.execute(
                sqla.select(
                    value_table.c.serial,
                    value_table.c.z_serial,
                    redshift_table.c.log_one_plus_z,
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
                    value_table.c.log_one_plus_z_Jordan,
                )
                .select_from(
                    value_table.join(
                        redshift_table,
                        redshift_table.c.serial == value_table.c.z_serial,
                    )
                )
                .filter(value_table.c.history_serial == store_id)
                .order_by(redshift_table.c.log_one_plus_z.desc())
            )

            z_points = []
            values = []

            units: UnitsLike = cosmology.units
            log_Mp = log(units.PlanckMass)
            log_GeV = log(units.GeV)

            for row in sample_rows:
                z_value = redshift(
                    store_id=row.z_serial,
                    log_one_plus_z=row.log_one_plus_z,
                )
                z_points.append(z_value)
                values.append(
                    ScalarHistoryValue(
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
                        log_one_plus_z_Jordan=row.log_one_plus_z_Jordan,
                    )
                )
            imported_z_sample = redshift_array(z_points)

            if num_expected_samples is not None:
                if len(imported_z_sample) != num_expected_samples:
                    raise RuntimeError(
                        f'Fewer z-samples than expected were recovered from the validated ScalarHistory "{store_label}"'
                    )

            attributes = {"_deserialized": True}
        else:
            values = None

            attributes = {"_do_not_populate": True, "_deserialized": True}

        # z_grid not supplied since this object has already been computed/populated
        failed = row_data.failure
        obj = ScalarHistory(
            payload={
                "store_id": store_id,
                "failure": failed,
                "failure_reason": row_data.failure_reason,
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
                "first_bounce": (
                    FirstBounce(
                        N=row_data.first_bounce_N,
                        phi_Einstein=row_data.first_bounce_phi_Einstein
                        * cosmology.units.PlanckMass,
                        log_T_Jordan=row_data.first_bounce_log_T_Jordan
                        + log(cosmology.units.GeV),
                        reflected=bool(row_data.first_bounce_reflected),
                    )
                    if not failed and row_data.first_bounce_N is not None
                    else None
                ),
                "fixed_T_values": (
                    FixedTValues(
                        phi_Einstein_1MeV=(
                            row_data.phi_Einstein_1MeV * cosmology.units.PlanckMass
                            if row_data.phi_Einstein_1MeV is not None
                            else None
                        ),
                        density_NP_ratio_1MeV=row_data.density_NP_ratio_1MeV,
                        phi_Einstein_70keV=(
                            row_data.phi_Einstein_70keV * cosmology.units.PlanckMass
                            if row_data.phi_Einstein_70keV is not None
                            else None
                        ),
                        density_NP_ratio_70keV=row_data.density_NP_ratio_70keV,
                    )
                    if not failed
                    else None
                ),
                # what the tuning found, from the row's own columns, so that it is read with
                # _do_not_populate too (jordan-normalization prompt 07, S3)
                "tuning": (
                    _tuning_from_row(row_data, cosmology.units) if not failed else None
                ),
                "values": values,
            },
            solver_labels=solver_labels,
            cosmology=cosmology,
            T_Jordan_init=T_init,
            phi_Einstein_init=phi_Einstein_init,
            pi_Einstein_init=pi_Einstein_init,
            model=model,
            tuning_tol=tuning_tol,
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
        obj: ScalarHistory,
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
            "model_type": obj.model.type_id,
            "model_serial": obj.model.store_id,
            "T_Jordan_init_serial": obj.T_Jordan_init.store_id,
            "phi_Einstein_init_serial": obj.phi_Einstein_init.store_id,
            "pi_Einstein_init_serial": obj.pi_Einstein_init.store_id,
            "atol_serial": obj._atol.store_id,
            "rtol_serial": obj._rtol.store_id,
            "tuning_tol_serial": obj.tuning_tol.store_id,
            "failure": obj._failure,
            "failure_reason": obj._failure_reason if obj._failure else None,
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

        # the first bounce, if there was one (science-readiness prompt 03); NULL otherwise
        bounce: Optional[FirstBounce] = obj._first_bounce if not obj._failure else None
        payload.update(
            {
                "first_bounce_N": bounce.N if bounce is not None else None,
                "first_bounce_log_T_Jordan": (
                    bounce.log_T_Jordan - log(obj._units.GeV)
                    if bounce is not None
                    else None
                ),
                "first_bounce_phi_Einstein": (
                    bounce.phi_Einstein / obj._units.PlanckMass
                    if bounce is not None
                    else None
                ),
                "first_bounce_reflected": (
                    bool(bounce.reflected) if bounce is not None else None
                ),
            }
        )

        # phi and the ratio at the two fixed temperatures (science-readiness prompt 06b); NULL
        # where the history did not reach a temperature, and on a failure row
        fixed_T: Optional[FixedTValues] = (
            obj._fixed_T_values if not obj._failure else None
        )

        def _phi_Mp(phi: Optional[float]) -> Optional[float]:
            return phi / obj._units.PlanckMass if phi is not None else None

        payload.update(
            {
                "phi_Einstein_1MeV": (
                    _phi_Mp(fixed_T.phi_Einstein_1MeV) if fixed_T is not None else None
                ),
                "density_NP_ratio_1MeV": (
                    fixed_T.density_NP_ratio_1MeV if fixed_T is not None else None
                ),
                "phi_Einstein_70keV": (
                    _phi_Mp(fixed_T.phi_Einstein_70keV) if fixed_T is not None else None
                ),
                "density_NP_ratio_70keV": (
                    fixed_T.density_NP_ratio_70keV if fixed_T is not None else None
                ),
            }
        )

        # what the tuning found (jordan-normalization prompt 07, S3); NULL on a failure row
        tuning: Optional[TuningValues] = obj._tuning if not obj._failure else None
        payload.update(_tuning_columns(tuning, obj._units))

        # # because ScalarHistory is a replicated table, we need to allow for the possibility that this object
        # # is a replica, rather than a fresh insert. If so, it's _my_id field will be set.
        # if hasattr(obj, "_my_id") and obj._my_id is not None:
        #     payload.update({"serial": obj._my_id})

        store_id = inserter(conn, payload)

        # set store_id on behalf of the ScalarHistory instance
        obj._my_id = store_id

        # add any tags that have been specified
        tag_inserter = inserters["ScalarHistory_tags"]
        for tag in obj.tags:
            sqla_ScalarHistoryTagAssociation_factory.add_tag(
                conn, tag_inserter, obj, tag
            )

        # now serialize the sampled output points
        units: UnitsLike = obj._units
        log_Mp = log(units.PlanckMass)
        log_GeV = log(units.GeV)

        value_inserter = inserters["ScalarHistoryValue"]
        for value in obj._values:
            value: ScalarHistoryValue
            value_id = value_inserter(
                conn,
                {
                    "history_serial": store_id,
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
                    "log_one_plus_z_Jordan": value.log_one_plus_z_Jordan,
                },
            )

            # set store_id on behalf of the ScalarHistoryValue instance
            value._my_id = value_id

        return obj

    def validate(
        self,
        obj: ScalarHistory,
        conn,
        table,
        tables,
    ):
        # query the row in ScalarHistory corresponding to this object
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

            value_table = tables["ScalarHistoryValue"]
            num_samples = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.history_serial == obj.store_id
                )
            ).scalar()

            # check if we counted as many rows as we expected
            validated: bool = num_samples == expected_samples

        if not validated:
            print(
                f'!! WARNING: ScalarHistory "{obj.label}" did not validate after serialization (expected samples={expected_samples}, number stored={num_samples})'
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
        value_table = tables["ScalarHistoryValue"]
        tags_table = tables["ScalarHistory_tags"]

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
            ">> ScalarHistory instances",
            "     The following unvalidated models were detected in the datastore:",
        ]
        for model in not_validated:
            msgs.append(
                f'       -- "{model.label}" (store_id={model.serial}, log10_atol={model.log10_atol}, log10_rtol={model.log10_rtol})'
            )
            rows = conn.execute(
                sqla.select(sqla.func.count(value_table.c.serial)).filter(
                    value_table.c.history_serial == model.serial,
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


class sqla_ScalarHistoryValue_factory(SQLAFactoryBase):
    def __init__(self):
        pass

    def register(self):
        return {
            "version": False,
            "timestamp": False,
            "stepping": False,
            "columns": [
                sqla.Column(
                    "history_serial",
                    sqla.Integer,
                    sqla.ForeignKey("ScalarHistory.serial"),
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
                # ln(1 + z_J), the Jordan-frame redshift from the end of the history
                # (jordan-normalization prompt 03, R4): a column, never a key, since a_J is not
                # monotonic through a bounce
                sqla.Column("log_one_plus_z_Jordan", sqla.Float(64), nullable=False),
            ],
        }

    def build(self, payload, conn, table, inserter, tables, inserters):
        # ScalarHistoryValue rows are written by sqla_ScalarHistoryFactory.store and read by its
        # build, with their parent; nothing reaches this path (object_get("ScalarHistoryValue", ...)
        # has no caller). Its stored-against-expected check compared the stored phi with the
        # expected pi, so it went in jordan-normalization prompt 03 (Z7). build is abstract in
        # SQLAFactoryBase, so it stays, and refuses
        raise NotImplementedError(
            "ScalarHistoryValue rows are written and read only through "
            "sqla_ScalarHistoryFactory (its store and build), never on their own"
        )
