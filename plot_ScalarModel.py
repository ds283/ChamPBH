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

import argparse
import itertools
import sys
from datetime import datetime
from math import exp, log
from pathlib import Path
from typing import List, Any

import pandas as pd
import ray
import seaborn as sns
from matplotlib import pyplot as plt

from ComputeTargets import ScalarModel, ScalarModelValue
from CosmologyConcepts import temperature, phi_value, pi_value
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ProfileAgent import ProfileAgent
from Datastore.SQL.ShardedPool import ShardedPool
from MetadataConcepts import tolerance
from RayTools.RayWorkPool import RayWorkPool
from Units import Planck_units
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE
from config.model_list import build_model_list
from config.sharding import (
    ShardKeyType,
    get_shard_key_store_id,
    replicated_tables,
    sharded_tables,
    read_table_config,
)
from extract_common import (
    safe_fabs,
    add_plot_labels,
    add_redshift_xaxis_labels,
    add_temperature_yaxis_labels,
    safe_fabs_positive,
    safe_fabs_negative,
    get_x_coord,
)

DEFAULT_TIMEOUT = 60

DEFAULT_T_INIT_GEV = 20000

parser = argparse.ArgumentParser()
parser.add_argument
parser.add_argument(
    "--database",
    type=str,
    default=None,
    help="read/write work items using the specified database cache",
)
parser.add_argument(
    "--db-timeout",
    type=int,
    default=DEFAULT_TIMEOUT,
    help="specify connection timeout for database layer",
)
parser.add_argument(
    "--profile-db",
    type=str,
    default=None,
    help="write profiling and performance data to the specified database",
)
parser.add_argument(
    "--ray-address", default="auto", type=str, help="specify address of Ray cluster"
)
parser.add_argument(
    "--output",
    default="ScalarModel-out",
    type=str,
    help="specify folder for output files",
)
args = parser.parse_args()

if args.database is None:
    parser.print_help()
    sys.exit()

# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
ray.init(address=args.ray_address)

VERSION_LABEL = "2026.1.1"

# instantiate a Datastore actor: this runs on its own node, and acts as a broker between
# ourselves and the database.
# For performance reasons, we want all database activity to run on this node.
# For one thing, this lets us use transactions efficiently.

profile_agent = None
if args.profile_db is not None:
    label = f'{VERSION_LABEL}--plot_ScalarModel-primarydb-"{args.database}"-{datetime.now().replace(microsecond=0).isoformat()}'

    profile_agent = ProfileAgent.options(name="ProfileAgent").remote(
        db_name=args.profile_db,
        timeout=args.db_timeout,
        label=label,
    )


@ray.remote
def plot_ScalarModel(model_label: str, model: ScalarModel, x_coord: str = "redshift"):
    if not model.available:
        return

    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    coupling: AbstractCoupling = model.coupling
    potential: AbstractPotential = model.potential

    beta = coupling._beta.as_float
    M = potential._M.as_float
    Lambda = potential._Lambda.as_float

    base_path = Path(args.output).resolve()
    base_path = base_path / f"{model_label}"

    values: List[ScalarModelValue] = model.values
    units = model._units

    def _get_x_coord(value: ScalarModelValue) -> float:
        return get_x_coord(value, x_coord)

    def x_axis_label() -> str:
        if x_coord == "efolds":
            return r"e-folds $N$"

        return r"redshift $1+z$"

    # max and min BBN temperatures chosen to match the PRyMordial defaults
    LOG_T_BBN_MAX = log(10 * units.MeV)
    LOG_T_BBN_MIN = log(1e-3 * units.MeV)

    def is_in_BBN_era(value: ScalarModelValue) -> bool:
        return LOG_T_BBN_MIN <= value.log_T_Jordan <= LOG_T_BBN_MAX

    def SigmaFm(value: ScalarModelValue) -> float:
        Sigma = value.Sigma
        fm = exp(value.log_fm)

        if fm > 10.0:
            return (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)

        return (Sigma + fm) / (1.0 + fm)

    def KE(value: ScalarModelValue) -> float:
        H_Einstein2 = value.H_Einstein * value.H_Einstein
        return H_Einstein2 * value.pi_Einstein * value.pi_Einstein / 2.0

    def PE(value: ScalarModelValue) -> float:
        return exp(potential.log_V(value.phi_Einstein))

    def TotalEnergy(value: ScalarModelValue) -> float:
        return KE(value) + PE(value)

    abs_phi_Einstein_points = [
        (_get_x_coord(value), safe_fabs(value.phi_Einstein / units.PlanckMass))
        for value in values
    ]
    pi_Einstein_points = [
        (_get_x_coord(value), value.pi_Einstein / units.PlanckMass) for value in values
    ]
    T_Jordan_points = [
        (_get_x_coord(value), exp(value.log_T_Jordan) / units.GeV) for value in values
    ]
    Sigma_points = [(_get_x_coord(value), value.Sigma) for value in values]
    positive_Sigma_points = [
        (_get_x_coord(value), safe_fabs_positive(value.Sigma)) for value in values
    ]
    negative_Sigma_points = [
        (_get_x_coord(value), safe_fabs_negative(value.Sigma)) for value in values
    ]
    positive_SigmaFm_points = [
        (_get_x_coord(value), safe_fabs_positive(SigmaFm(value))) for value in values
    ]
    negative_SigmaFm_points = [
        (_get_x_coord(value), safe_fabs_negative(SigmaFm(value))) for value in values
    ]
    w_points = [(_get_x_coord(value), (1.0 - value.Sigma) / 3.0) for value in values]
    gstar_rho_points = [(_get_x_coord(value), value.gstar_rho) for value in values]
    gstar_s_points = [(_get_x_coord(value), value.gstar_s) for value in values]
    dgstar_rho_points = [
        (_get_x_coord(value), value.dgstar_rho_dlogT) for value in values
    ]
    dgstar_s_points = [(_get_x_coord(value), value.dgstar_s_dlogT) for value in values]

    positive_friction_term_points = [
        (
            _get_x_coord(value),
            safe_fabs_positive(value.friction_term / units.PlanckMass),
        )
        for value in values
    ]
    negative_friction_term_points = [
        (
            _get_x_coord(value),
            safe_fabs_negative(value.friction_term / units.PlanckMass),
        )
        for value in values
    ]
    positive_reflecting_term_points = [
        (
            _get_x_coord(value),
            safe_fabs_positive(value.reflecting_term / units.PlanckMass),
        )
        for value in values
    ]
    negative_reflecting_term_points = [
        (
            _get_x_coord(value),
            safe_fabs_negative(value.reflecting_term / units.PlanckMass),
        )
        for value in values
    ]
    positive_kicking_term_points = [
        (_get_x_coord(value), safe_fabs_positive(value.kicking_term / units.PlanckMass))
        for value in values
    ]
    negative_kicking_term_points = [
        (_get_x_coord(value), safe_fabs_negative(value.kicking_term / units.PlanckMass))
        for value in values
    ]

    positive_abs_H_Einstein_points = [
        (_get_x_coord(value), safe_fabs_positive(value.H_Einstein / units.GeV))
        for value in values
    ]
    negative_abs_H_Einstein_points = [
        (_get_x_coord(value), safe_fabs_negative(value.H_Einstein / units.GeV))
        for value in values
    ]
    positive_abs_H_Jordan_points = [
        (_get_x_coord(value), safe_fabs_positive(value.H_Jordan / units.GeV))
        for value in values
    ]
    negative_abs_H_Jordan_points = [
        (_get_x_coord(value), safe_fabs_negative(value.H_Jordan / units.GeV))
        for value in values
    ]

    Mp2 = units.PlanckMass * units.PlanckMass
    Mp4 = Mp2 * Mp2
    KE_points = [(_get_x_coord(value), KE(value) / Mp4) for value in values]
    PE_points = [(_get_x_coord(value), PE(value) / Mp4) for value in values]
    TotalEnergy_points = [
        (_get_x_coord(value), TotalEnergy(value) / Mp4) for value in values
    ]

    abs_phi_Einstein_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs(value.phi_Einstein / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]
    positive_Sigma_BBN = [
        (T_Jordan_MeV(value), safe_fabs_positive(value.Sigma))
        for value in values
        if is_in_BBN_era(value)
    ]
    negative_Sigma_BBN = [
        (T_Jordan_MeV(value), safe_fabs_negative(value.Sigma))
        for value in values
        if is_in_BBN_era(value)
    ]
    positive_SigmaFm_BBN = [
        (T_Jordan_MeV(value), safe_fabs_positive(SigmaFm(value)))
        for value in values
        if is_in_BBN_era(value)
    ]
    negative_SigmaFm_BBN = [
        (T_Jordan_MeV(value), safe_fabs_negative(SigmaFm(value)))
        for value in values
        if is_in_BBN_era(value)
    ]
    positive_kicking_term_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs_positive(value.kicking_term / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]
    negative_kicking_term_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs_negative(value.kicking_term / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]
    positive_reflecting_term_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs_positive(value.reflecting_term / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]
    negative_reflecting_term_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs_negative(value.reflecting_term / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]

    abs_phi_Einstein_x, abs_phi_Einstein_y = zip(*abs_phi_Einstein_points)
    pi_Einstein_x, pi_Einstein_y = zip(*pi_Einstein_points)
    T_Jordan_x, T_Jordan_y = zip(*T_Jordan_points)
    Sigma_x, Sigma_y = zip(*Sigma_points)
    positive_Sigma_x, positive_Sigma_y = zip(*positive_Sigma_points)
    negative_Sigma_x, negative_Sigma_y = zip(*negative_Sigma_points)
    positive_SigmaFm_x, positive_SigmaFm_y = zip(*positive_SigmaFm_points)
    negative_SigmaFm_x, negative_SigmaFm_y = zip(*negative_SigmaFm_points)
    w_x, w_y = zip(*w_points)
    gstar_rho_x, gstar_rho_y = zip(*gstar_rho_points)
    gstar_s_x, gstar_s_y = zip(*gstar_s_points)
    dgstar_rho_x, dgstar_rho_y = zip(*dgstar_rho_points)
    dgstar_s_x, dgstar_s_y = zip(*dgstar_s_points)

    positive_friction_term_x, positive_friction_term_y = zip(
        *positive_friction_term_points
    )
    negative_friction_term_x, negative_friction_term_y = zip(
        *negative_friction_term_points
    )
    positive_reflecting_term_x, positive_reflecting_term_y = zip(
        *positive_reflecting_term_points
    )
    negative_reflecting_term_x, negative_reflecting_term_y = zip(
        *negative_reflecting_term_points
    )
    positive_kicking_term_x, positive_kicking_term_y = zip(
        *positive_kicking_term_points
    )
    negative_kicking_term_x, negative_kicking_term_y = zip(
        *negative_kicking_term_points
    )

    positive_abs_H_Einstein_x, positive_abs_H_Einstein_y = zip(
        *positive_abs_H_Einstein_points
    )
    negative_abs_H_Einstein_x, negative_abs_H_Einstein_y = zip(
        *negative_abs_H_Einstein_points
    )
    positive_abs_H_Jordan_x, positive_abs_H_Jordan_y = zip(
        *positive_abs_H_Jordan_points
    )
    negative_abs_H_Jordan_x, negative_abs_H_Jordan_y = zip(
        *negative_abs_H_Jordan_points
    )

    KE_x, KE_y = zip(*KE_points)
    PE_x, PE_y = zip(*PE_points)
    TotalEnergy_x, TotalEnergy_y = zip(*TotalEnergy_points)

    abs_phi_Einstein_BBN_x, abs_phi_Einstein_BBN_y = zip(*abs_phi_Einstein_BBN)
    positive_Sigma_BBN_x, positive_Sigma_BBN_y = zip(*positive_Sigma_BBN)
    negative_Sigma_BBN_x, negative_Sigma_BBN_y = zip(*negative_Sigma_BBN)
    positive_SigmaFm_BBN_x, positive_SigmaFm_BBN_y = zip(*positive_SigmaFm_BBN)
    negative_SigmaFm_BBN_x, negative_SigmaFm_BBN_y = zip(*negative_SigmaFm_BBN)
    positive_kicking_term_BBN_x, positive_kicking_term_BBN_y = zip(
        *positive_kicking_term_BBN
    )
    negative_kicking_term_BBN_x, negative_kicking_term_BBN_y = zip(
        *negative_kicking_term_BBN
    )
    positive_reflecting_term_BBN_x, positive_reflecting_term_BBN_y = zip(
        *positive_reflecting_term_BBN
    )
    negative_reflecting_term_BBN_x, negative_reflecting_term_BBN_y = zip(
        *negative_reflecting_term_BBN
    )

    sns.set_theme()

    if len(abs_phi_Einstein_x) > 0 and any(
        y is not None and y > 0 for y in abs_phi_Einstein_y
    ):
        fig = plt.figure()
        fig.set_size_inches(8.0, 10.0)

        axs = fig.subplots(nrows=3, ncols=1, sharex=True, sharey=False)

        phi_ax = axs[2]
        pi_ax = axs[1]
        T_ax = axs[0]

        phi_ax.plot(
            abs_phi_Einstein_x,
            abs_phi_Einstein_y,
            label=r"$\phi_{\text{E}}$ [$M_{\text{P}}$]",
            color="r",
            linestyle="solid",
        )
        if x_coord == "redshift":
            phi_ax.set_xscale("log")
            phi_ax.xaxis.set_inverted(True)
        phi_ax.set_yscale("log")

        phi_ax.set_xlabel(x_axis_label())
        phi_ax.grid(True)

        pi_ax.plot(
            pi_Einstein_x,
            pi_Einstein_y,
            label=r"$\pi_{\text{E}}$ [$M_{\text{P}}$]",
            color="b",
            linestyle="solid",
        )
        pi_ax.grid(True)

        T_ax.plot(
            T_Jordan_x,
            T_Jordan_y,
            label=r"$T_{\text{Jordan}}$ [GeV]",
            color="g",
            linestyle="solid",
        )
        T_ax.set_yscale("log")
        T_ax.grid(True)

        add_plot_labels(T_ax, model, model_label, shift=0.05)
        add_temperature_yaxis_labels(T_ax, model, temp_unit="GeV")

        h, l = add_redshift_xaxis_labels(
            phi_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )
        add_redshift_xaxis_labels(pi_ax, model, text_labels=False, x_coord=x_coord)

        T_ax.legend(loc="best")
        pi_ax.legend(loc="best")

        handles, labels = phi_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        phi_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/fields.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        plt.close()

        fig = plt.figure()
        fig.set_size_inches(8.0, 13.0)

        axs = fig.subplots(nrows=4, ncols=1, sharex=True, sharey=False)

        gstar_ax = axs[0]
        dgstar_ax = axs[1]
        w_ax = axs[2]
        Sigma_ax = axs[3]

        Sigma_ax.plot(
            Sigma_x,
            Sigma_y,
            label=r"$\Sigma$",
            color="r",
            linestyle="solid",
        )
        if x_coord == "redshift":
            Sigma_ax.set_xscale("log")
            Sigma_ax.xaxis.set_inverted(True)

        Sigma_ax.set_xlabel(x_axis_label())
        Sigma_ax.grid(True)

        w_ax.plot(
            w_x,
            w_y,
            label=r"$w = (1-\Sigma)/3$",
            color="b",
            linestyle="solid",
        )
        w_ax.grid(True)

        gstar_ax.plot(
            gstar_rho_x,
            gstar_rho_y,
            label=r"$g_{*\rho}$",
            color="g",
            linestyle="solid",
        )
        gstar_ax.plot(
            gstar_s_x,
            gstar_s_y,
            label=r"$g_{*s}$",
            color="m",
            linestyle="solid",
        )
        gstar_ax.grid(True)

        dgstar_ax.plot(
            dgstar_rho_x,
            dgstar_rho_y,
            label=r"$\mathrm{d} g_{*\rho}/\mathrm{d}(\log T)$",
            color="g",
            linestyle="solid",
        )
        dgstar_ax.plot(
            dgstar_s_x,
            dgstar_s_y,
            label=r"$\mathrm{d} g_{*s}/\mathrm{d}(\log T)$",
            color="m",
            linestyle="solid",
        )

        add_plot_labels(gstar_ax, model, model_label, shift=0.05)

        h, l = add_redshift_xaxis_labels(
            gstar_ax, model, text_labels=False, x_coord=x_coord
        )
        add_redshift_xaxis_labels(dgstar_ax, model, text_labels=False, x_coord=x_coord)
        add_redshift_xaxis_labels(w_ax, model, text_labels=False, x_coord=x_coord)
        add_redshift_xaxis_labels(
            Sigma_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )

        w_ax.legend(loc="best")
        gstar_ax.legend(loc="best")
        dgstar_ax.legend(loc="best")

        handles, labels = Sigma_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        Sigma_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/thermo.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        plt.close()

        fig = plt.figure()
        fig.set_size_inches(8.0, 5.0)
        H_ax = fig.gca()

        H_ax.plot(
            positive_abs_H_Einstein_x,
            positive_abs_H_Einstein_y,
            label=r"$H_{\text{Einstein}}$ [GeV]",
            color="b",
            linestyle="solid",
        )
        H_ax.plot(
            negative_abs_H_Einstein_x,
            negative_abs_H_Einstein_y,
            color="b",
            linestyle="dashed",
        )
        H_ax.plot(
            positive_abs_H_Jordan_x,
            positive_abs_H_Jordan_y,
            label=r"$H_{\text{Jordan}}$ [GeV]",
            color="g",
            linestyle="solid",
        )
        H_ax.plot(
            negative_abs_H_Jordan_x,
            negative_abs_H_Jordan_y,
            color="g",
            linestyle="dashed",
        )

        if x_coord == "redshift":
            H_ax.set_xscale("log")
            H_ax.xaxis.set_inverted(True)
        H_ax.set_yscale("log")

        H_ax.set_xlabel(x_axis_label())

        H_ax.grid(True)

        add_plot_labels(H_ax, model, model_label)
        h, l = add_redshift_xaxis_labels(
            H_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )

        handles, labels = H_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        H_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/Hubble.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        plt.close()

        fig = plt.figure()
        fig.set_size_inches(8.0, 5.0)
        energy_ax = fig.gca()

        energy_ax.plot(
            KE_x,
            KE_y,
            label=r"KE [$M_{\mathrm{P}}^4$]",
            color="r",
            linestyle="solid",
        )
        energy_ax.plot(
            PE_x,
            PE_y,
            label=r"PE [$M_{\mathrm{P}}^4$]",
            color="g",
            linestyle="solid",
        )
        energy_ax.plot(
            TotalEnergy_x,
            TotalEnergy_y,
            label=r"Total energy [$M_{\mathrm{P}}^4$]",
            color="b",
            linestyle="dashed",
        )

        if x_coord == "redshift":
            energy_ax.set_xscale("log")
            energy_ax.xaxis.set_inverted(True)
        energy_ax.set_yscale("log")

        energy_ax.set_xlabel(x_axis_label())

        energy_ax.grid(True)

        add_plot_labels(energy_ax, model, model_label)
        h, l = add_redshift_xaxis_labels(
            energy_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )

        handles, labels = energy_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        energy_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/energy.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        fig = plt.figure()
        fig.set_size_inches(8.0, 10.0)

        axs = fig.subplots(nrows=4, ncols=1, sharex=True, sharey=False)

        f_ax = axs[0]
        r_ax = axs[1]
        k_ax = axs[2]
        Sigma_ax = axs[3]

        f_ax.plot(
            positive_friction_term_x,
            positive_friction_term_y,
            label=r"friction term [$M_{\text{P}}$]",
            color="r",
            linestyle="solid",
        )
        f_ax.plot(
            negative_friction_term_x,
            negative_friction_term_y,
            color="r",
            linestyle="dashed",
        )
        f_ax.set_yscale("log")
        f_ax.grid(True)

        r_ax.plot(
            positive_reflecting_term_x,
            positive_reflecting_term_y,
            label=r"reflecting term [$M_{\text{P}}$]",
            color="g",
            linestyle="solid",
        )
        r_ax.plot(
            negative_reflecting_term_x,
            negative_reflecting_term_y,
            color="g",
            linestyle="dashed",
        )
        r_ax.set_yscale("log")
        r_ax.grid(True)

        k_ax.plot(
            positive_kicking_term_x,
            positive_kicking_term_y,
            label=r"kicking term [$M_{\text{P}}$]",
            color="b",
            linestyle="solid",
        )
        k_ax.plot(
            negative_kicking_term_x,
            negative_kicking_term_y,
            color="b",
            linestyle="dashed",
        )
        k_ax.set_yscale("log")
        k_ax.grid(True)

        Sigma_ax.plot(
            positive_Sigma_x,
            positive_Sigma_y,
            label=r"$\Sigma$",
            color="m",
            linestyle="solid",
        )
        Sigma_ax.plot(
            negative_Sigma_x,
            negative_Sigma_y,
            color="m",
            linestyle="dashed",
        )
        Sigma_ax.plot(
            positive_SigmaFm_x,
            positive_SigmaFm_y,
            label=r"$(\Sigma + f_{\mathrm{m}})/(1 + f_{\mathrm{m}})$",
            color="c",
            linestyle="solid",
        )
        Sigma_ax.plot(
            negative_SigmaFm_x,
            negative_SigmaFm_y,
            color="c",
            linestyle="dashed",
        )
        Sigma_ax.set_yscale("log")
        Sigma_ax.grid(True)

        if x_coord == "redshift":
            Sigma_ax.set_xscale("log")
            Sigma_ax.xaxis.set_inverted(True)
        Sigma_ax.set_xlabel(x_axis_label())

        add_plot_labels(f_ax, model, model_label, shift=0.05)
        add_redshift_xaxis_labels(f_ax, model, text_labels=False, x_coord=x_coord)
        add_redshift_xaxis_labels(r_ax, model, text_labels=False, x_coord=x_coord)
        add_redshift_xaxis_labels(k_ax, model, text_labels=False, x_coord=x_coord)
        h, l = add_redshift_xaxis_labels(
            Sigma_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )

        f_ax.legend(loc="best")
        r_ax.legend(loc="best")
        k_ax.legend(loc="best")

        handles, labels = Sigma_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        Sigma_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/ODE_terms.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        plt.close()

        fig = plt.figure()
        fig.set_size_inches(8.0, 10.0)

        axs = fig.subplots(nrows=3, ncols=1, sharex=True, sharey=False)

        ODE_terms_BBN_ax = axs[0]
        Sigma_BBN_ax = axs[1]
        phi_BBN_ax = axs[2]

        phi_BBN_ax.plot(
            abs_phi_Einstein_BBN_x,
            abs_phi_Einstein_BBN_y,
            label=r"$|\phi_{\text{E}}|$ [$M_{\text{P}}$]",
            color="r",
            linestyle="solid",
        )
        phi_BBN_ax.set_yscale("log")
        phi_BBN_ax.grid(True)

        Sigma_BBN_ax.plot(
            positive_Sigma_BBN_x,
            positive_Sigma_BBN_y,
            label=r"$\Sigma$",
            color="m",
            linestyle="solid",
        )
        Sigma_BBN_ax.plot(
            negative_Sigma_BBN_x,
            negative_Sigma_BBN_y,
            color="m",
            linestyle="dashed",
        )
        Sigma_BBN_ax.plot(
            positive_SigmaFm_BBN_x,
            positive_SigmaFm_BBN_y,
            label=r"$(\Sigma + f_{\mathrm{m}})/(1 + f_{\mathrm{m}})$",
            color="c",
            linestyle="solid",
        )
        Sigma_BBN_ax.plot(
            negative_SigmaFm_BBN_x,
            negative_SigmaFm_BBN_y,
            color="c",
            linestyle="dashed",
        )
        Sigma_BBN_ax.set_yscale("log")
        Sigma_BBN_ax.grid(True)

        ODE_terms_BBN_ax.plot(
            positive_kicking_term_BBN_x,
            positive_kicking_term_BBN_y,
            label=r"kicking term [$M_{\text{P}}$]",
            color="b",
            linestyle="solid",
        )
        ODE_terms_BBN_ax.plot(
            negative_kicking_term_BBN_x,
            negative_kicking_term_BBN_y,
            color="b",
            linestyle="dashed",
        )
        ODE_terms_BBN_ax.plot(
            positive_reflecting_term_BBN_x,
            positive_reflecting_term_BBN_y,
            label=r"reflecting term [$M_{\text{P}}$]",
            color="g",
            linestyle="solid",
        )
        ODE_terms_BBN_ax.plot(
            negative_reflecting_term_BBN_x,
            negative_reflecting_term_BBN_y,
            color="g",
            linestyle="dashed",
        )
        ODE_terms_BBN_ax.set_yscale("log")
        ODE_terms_BBN_ax.grid(True)

        phi_BBN_ax.set_xlabel("Temperature $T$ [MeV]")
        phi_BBN_ax.set_xscale("log")
        phi_BBN_ax.xaxis.set_inverted(True)

        add_plot_labels(ODE_terms_BBN_ax, model, model_label, shift=0.05)

        phi_BBN_ax.legend(loc="best")
        Sigma_BBN_ax.legend(loc="best")
        ODE_terms_BBN_ax.legend(loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/BBN_era.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))

        plt.close()

        data = []
        log_GeV = log(units.GeV)
        for val in values:
            data.append(
                {
                    "z": float(val.z),
                    "raw_N": val.raw_N,
                    "phi_Einstein_Mp": val.phi_Einstein / units.PlanckMass,
                    "pi_Einstein_Mp": val.pi_Einstein / units.PlanckMass,
                    "H_Einstein_Mp": val.H_Einstein / units.PlanckMass,
                    "H_Jordan_Mp": val.H_Jordan / units.PlanckMass,
                    "log_rhorad_Einstein_GeV4": val.log_rhorad_Einstein - 4.0 * log_GeV,
                    "log_rhorad_Jordan_GeV4": val.log_rhorad_Jordan - 4.0 * log_GeV,
                    "log_fm": val.log_fm,
                    "fm": exp(val.log_fm),
                    "log_T_Jordan_GeV": val.log_T_Jordan - log_GeV,
                    "T_Jordan_GeV": exp(val.log_T_Jordan) / units.GeV,
                    "T_Jordan_Kelvin": exp(val.log_T_Jordan) / units.Kelvin,
                    "gstar_rho": val.gstar_rho,
                    "gstar_s": val.gstar_s,
                    "dgstar_rho_dlogT": val.dgstar_rho_dlogT,
                    "dgstar_s_dlogT": val.dgstar_s_dlogT,
                    "Sigma": val.Sigma,
                    "w": (1.0 - val.Sigma) / 3.0,
                    "friction_term_Mp": val.friction_term / units.PlanckMass,
                    "reflecting_term_Mp": val.reflecting_term / units.PlanckMass,
                    "kicking_term_Mp": val.kicking_term / units.PlanckMass,
                }
            )

        df = pd.DataFrame(data)
        df.sort_values(by="z", inplace=True, ascending=False, ignore_index=True)

        csv_path = (
            base_path
            / f"csv/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/fields.csv"
        )
        csv_path.parents[0].mkdir(exist_ok=True, parents=True)

        df.to_csv(csv_path, header=True, index=False)


def T_Jordan_MeV(value: ScalarModelValue) -> float | Any:
    return exp(value.log_T_Jordan) / units.MeV


def run_pipeline(
    model_data,
    Potential_array: List[AbstractPotential],
    Coupling_array: List[AbstractCoupling],
    T_init: temperature,
    T_stop: temperature,
    phi_init: phi_value,
    pi_init: pi_value,
    atol: tolerance,
    rtol: tolerance,
):
    model_label = model_data["label"]
    model_cosmology = model_data["cosmology"]

    print(f"\n>> RUNNING PIPELINE FOR MODEL {model_label}")

    def build_plot_work(item):
        potential, coupling = item

        query_payload = {
            "shard_key": coupling.shard_key,
            "solver_labels": [],
            "cosmology": model_cosmology,
            "T_Jordan_init": T_init,
            "T_Jordan_stop": T_stop,
            "phi_Einstein_init": phi_init,  # currently using fixed initial value of phi_Einstein
            "pi_Einstein_init": pi_init,  # currently all integrations begin with the field at rest
            "z_grid": None,  # don't check which values of z we have sampled
            "potential": potential,
            "coupling": coupling,
            "atol": atol,
            "rtol": rtol,
        }

        ref = pool.object_get("ScalarModel", **query_payload)

        return plot_ScalarModel.remote(model_label, ref, x_coord="efolds")

    work_grid = itertools.product(Potential_array, Coupling_array)

    work_queue = RayWorkPool(
        pool,
        work_grid,
        task_builder=build_plot_work,
        compute_handler=None,
        store_handler=None,
        available_handler=None,
        validation_handler=None,
        post_handler=None,
        label_builder=None,
        create_batch_size=10,
        process_batch_size=10,
        notify_batch_size=50,
        notify_time_interval=120,
        title="GENERATING ScalarModel DATA PRODUCTS",
        store_results=False,
    )
    work_queue.run()


# establish a ShardedPool to orchestrate database access
with ShardedPool(
    version_label=VERSION_LABEL,
    db_name=args.database,
    ShardKeyType=ShardKeyType,
    ShardKeyStoreIdGetter=get_shard_key_store_id,
    replicated_tables=replicated_tables,
    sharded_tables=sharded_tables,
    timeout=args.db_timeout,
    profile_agent=profile_agent,
    job_name="plot_ScalarModel",
    prune_unvalidated=False,
    read_table_config=read_table_config,
) as pool:
    # build absolute and relative tolerances
    atol, rtol = ray.get(
        [
            pool.object_get(tolerance, tol=DEFAULT_ABS_TOLERANCE),
            pool.object_get(tolerance, tol=DEFAULT_REL_TOLERANCE),
        ]
    )

    # get list of models we want to extract transfer functions for
    units = Planck_units()

    T_init = ray.get(
        pool.object_get(
            "temperature", value=DEFAULT_T_INIT_GEV * units.GeV, units=units
        )
    )

    phi_init, pi_init = ray.get(
        [
            pool.object_get("phi_value", value=5.0 * units.PlanckMass, units=units),
            pool.object_get("pi_value", value=0.0, units=units),
        ]
    )

    def convert_to_potential(M_lambda_set):
        return pool.object_get(
            "ExponentialPotential",
            payload_data=[
                {"M": M, "Lambda": Lambda, "n": 1, "units": units}
                for M, Lambda in M_lambda_set
            ],
        )

    def convert_to_coupling(beta_set):
        # ExponentialCoupling is a sharded table and needs a "shard_key" field
        # TODO: find a better way to implement/handle
        return pool.object_get(
            "ExponentialCoupling",
            payload_data=[
                {"shard_key": beta, "beta": beta, "units": units} for beta in beta_set
            ],
        )

    # read in the stored tables of beta, M, and Lambda
    beta_table = ray.get(pool.read_table("beta_value"))
    M_table = ray.get(pool.read_table("M_value", units))
    Lambda_table = ray.get(pool.read_table("Lambda_value", units))

    M_Lambda_grid = itertools.product(M_table, Lambda_table)
    Potential_array = ray.get(convert_to_potential(M_Lambda_grid))

    Coupling_array = ray.get(convert_to_coupling(beta_table))

    model_list = build_model_list(pool, units)

    for model_data in model_list:
        cosmology: BaseCosmology = model_data["cosmology"]

        T_CMB = cosmology._params.T_CMB_Kelvin * units.Kelvin
        T_stop = ray.get(pool.object_get("temperature", value=T_CMB, units=units))

        run_pipeline(
            model_data,
            Potential_array,
            Coupling_array,
            T_init,
            T_stop,
            phi_init,
            pi_init,
            atol,
            rtol,
        )
