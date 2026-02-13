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
from math import exp, log, sqrt
from pathlib import Path
from typing import List, Union, Dict

import pandas as pd
import ray
import seaborn as sns
from matplotlib import pyplot as plt
from numpy import nan

from ComputeTargets import (
    ScalarModel,
    ScalarModelValue,
    ScalarModelProxy,
    AdiabaticHistory,
    BBNData,
    BBNDataValue,
)
from CosmologyConcepts import temperature, phi_value, pi_value
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ProfileAgent import ProfileAgent
from Datastore.SQL.ShardedPool import ShardedPool
from MetadataConcepts import tolerance
from RayTools.RayWorkPool import RayWorkPool
from Units import Planck_units
from Units.base import UnitsLike
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
    add_ScalarModel_labels,
    add_redshift_xaxis_labels,
    add_temperature_yaxis_labels,
    safe_fabs_positive,
    safe_fabs_negative,
    nice_Q_labels,
)

DEFAULT_TIMEOUT = 60

DEFAULT_T_INIT_GEV = 20000

parser = argparse.ArgumentParser()
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


def history_plot(
    plot_path,
    model: ScalarModel,
    Q: AdiabaticHistory,
    model_label: str,
    get_x_coord,
    x_axis_label,
    x_coord: str = "redshift",
):
    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    values = model.values
    units: UnitsLike = model._units

    positive_abs_phi_Einstein_points = [
        (get_x_coord(value), safe_fabs_positive(value.phi_Einstein / units.PlanckMass))
        for value in values
    ]
    negative_abs_phi_Einstein_points = [
        (get_x_coord(value), safe_fabs_negative(value.phi_Einstein / units.PlanckMass))
        for value in values
    ]
    pi_Einstein_points = [
        (get_x_coord(value), value.pi_Einstein / units.PlanckMass) for value in values
    ]
    T_Jordan_points = [
        (get_x_coord(value), exp(value.log_T_Jordan) / units.GeV) for value in values
    ]

    positive_abs_phi_Einstein_x, positive_abs_phi_Einstein_y = zip(
        *positive_abs_phi_Einstein_points
    )
    negative_abs_phi_Einstein_x, negative_abs_phi_Einstein_y = zip(
        *negative_abs_phi_Einstein_points
    )
    pi_Einstein_x, pi_Einstein_y = zip(*pi_Einstein_points)
    T_Jordan_x, T_Jordan_y = zip(*T_Jordan_points)

    fig = plt.figure()
    adiabatic_xy = {}

    if Q.available:
        adiabatic_points = [
            (
                get_x_coord(value),
                {label: value.value(label) for label in AdiabaticHistory.Q_labels},
            )
            for value in Q.values
        ]

        for label in AdiabaticHistory.Q_labels:
            _points = [(x, y[label]) for x, y in adiabatic_points]
            adiabatic_xy[label] = zip(*_points)

        fig.set_size_inches(8.0, 13.0)
        axs = fig.subplots(nrows=4, ncols=1, sharex=True, sharey=False)

        phi_ax = axs[3]
        pi_ax = axs[2]
        Q_ax = axs[1]
        T_ax = axs[0]
    else:
        fig.set_size_inches(8.0, 10.0)
        axs = fig.subplots(nrows=3, ncols=1, sharex=True, sharey=False)

        phi_ax = axs[2]
        pi_ax = axs[1]
        Q_ax = None
        T_ax = axs[0]

    phi_ax.plot(
        positive_abs_phi_Einstein_x,
        positive_abs_phi_Einstein_y,
        label=r"$\phi_{\text{E}}$ [$M_{\text{P}}$]",
        color="r",
        linestyle="solid",
    )
    phi_ax.plot(
        negative_abs_phi_Einstein_x,
        negative_abs_phi_Einstein_y,
        color="r",
        linestyle="dashed",
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

    if Q_ax is not None and len(adiabatic_xy) > 0:
        ytrans = Q_ax.get_yaxis_transform()

        Q_ax.axhline(y=1.0, color="r", linestyle="dashed")
        Q_ax.text(
            0.15,
            5.0,
            r"$|Q| = 1$",
            color="r",
            transform=ytrans,
            fontsize="x-small",
        )

        for label in adiabatic_xy:
            x, y = adiabatic_xy[label]
            Q_ax.plot(x, y, label=nice_Q_labels[label])

        Q_ax.set_yscale("log")
        Q_ax.grid(True)
        Q_ax.set_ylabel(r"$|Q| = |\omega_k'/\omega_k^2|$")
        Q_ax.legend(loc="best")

    add_temperature_yaxis_labels(T_ax, model, temp_unit="GeV")
    T_ax.plot(
        T_Jordan_x,
        T_Jordan_y,
        label=r"$T_{\text{Jordan}}$ [GeV]",
        color="g",
        linestyle="solid",
    )
    T_ax.set_yscale("log")
    T_ax.grid(True)

    add_ScalarModel_labels(fig, model, model_label)

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

    fig_path = plot_path / "full_history.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a history plot for ScalarModel '{model_label}'"
        )

    plt.close()


def thermo_plot(
    plot_path,
    model: ScalarModel,
    model_label: str,
    get_x_coord,
    x_axis_label,
    x_coord: str = "redshift",
):
    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    values = model.values
    units: UnitsLike = model._units

    Sigma_points = [(get_x_coord(value), value.Sigma) for value in values]
    w_points = [(get_x_coord(value), (1.0 - value.Sigma) / 3.0) for value in values]
    gstar_rho_points = [(get_x_coord(value), value.gstar_rho) for value in values]
    gstar_s_points = [(get_x_coord(value), value.gstar_s) for value in values]
    dgstar_rho_points = [
        (get_x_coord(value), value.dgstar_rho_dlogT) for value in values
    ]
    dgstar_s_points = [(get_x_coord(value), value.dgstar_s_dlogT) for value in values]

    Sigma_x, Sigma_y = zip(*Sigma_points)
    w_x, w_y = zip(*w_points)
    gstar_rho_x, gstar_rho_y = zip(*gstar_rho_points)
    gstar_s_x, gstar_s_y = zip(*gstar_s_points)
    dgstar_rho_x, dgstar_rho_y = zip(*dgstar_rho_points)
    dgstar_s_x, dgstar_s_y = zip(*dgstar_s_points)

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

    add_ScalarModel_labels(fig, model, model_label)

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

    fig_path = plot_path / "thermo.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a thermodynamics plot for ScalarModel '{model_label}'"
        )

    plt.close()


def Hubble_plot(
    plot_path,
    model: ScalarModel,
    BBN: BBNData,
    model_label: str,
    _get_x_coord,
    _x_axis_label,
    H_Standard,
    x_coord: str = "redshift",
):
    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    values = model.values
    units: UnitsLike = model._units

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

    abs_H_standard_points = [
        (_get_x_coord(value), safe_fabs(H_Standard(value) / units.GeV))
        for value in values
    ]

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
    abs_H_standard_x, abs_H_standard_y = zip(*abs_H_standard_points)

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
    H_ax.plot(
        abs_H_standard_x,
        abs_H_standard_y,
        label=r"$H_{\mathrm{standard}}$ [GeV]",
        color="r",
        linestyle="solid",
    )

    if x_coord == "redshift":
        H_ax.set_xscale("log")
        H_ax.xaxis.set_inverted(True)
    H_ax.set_yscale("log")

    H_ax.set_xlabel(_x_axis_label())

    H_ax.grid(True)

    add_ScalarModel_labels(fig, model, model_label)
    h, l = add_redshift_xaxis_labels(
        H_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
    )

    handles, labels = H_ax.get_legend_handles_labels()
    handles.extend(h)
    labels.extend(l)
    H_ax.legend(handles, labels, loc="best")

    fig_path = plot_path / "Hubble.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a Hubble rate plot for ScalarModel '{model_label}'"
        )

    plt.close()


def energy_plot(
    plot_path,
    model: ScalarModel,
    model_label: str,
    get_x_coord,
    x_axis_label,
    KE,
    PE,
    TotalEnergy,
    x_coord: str = "redshift",
):
    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    values = model.values
    units: UnitsLike = model._units

    Mp2 = units.PlanckMass * units.PlanckMass
    Mp4 = Mp2 * Mp2
    KE_points = [(get_x_coord(value), KE(value) / Mp4) for value in values]
    PE_points = [(get_x_coord(value), PE(value) / Mp4) for value in values]
    TotalEnergy_points = [
        (get_x_coord(value), TotalEnergy(value) / Mp4) for value in values
    ]

    KE_x, KE_y = zip(*KE_points)
    PE_x, PE_y = zip(*PE_points)
    TotalEnergy_x, TotalEnergy_y = zip(*TotalEnergy_points)
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

    add_ScalarModel_labels(fig, model, model_label)
    h, l = add_redshift_xaxis_labels(
        energy_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
    )

    handles, labels = energy_ax.get_legend_handles_labels()
    handles.extend(h)
    labels.extend(l)
    energy_ax.legend(handles, labels, loc="best")

    fig_path = plot_path / "energy.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating an energy history plot for ScalarModel '{model_label}'"
        )

    plt.close()


def ODE_terms_plot(
    plot_path,
    model: ScalarModel,
    model_label: str,
    get_x_coord,
    x_axis_label,
    SigmaFm,
    x_coord: str = "redshift",
):
    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    values = model.values
    units: UnitsLike = model._units

    positive_Sigma_points = [
        (get_x_coord(value), safe_fabs_positive(value.Sigma)) for value in values
    ]
    negative_Sigma_points = [
        (get_x_coord(value), safe_fabs_negative(value.Sigma)) for value in values
    ]
    positive_SigmaFm_points = [
        (get_x_coord(value), safe_fabs_positive(SigmaFm(value))) for value in values
    ]
    negative_SigmaFm_points = [
        (get_x_coord(value), safe_fabs_negative(SigmaFm(value))) for value in values
    ]

    positive_friction_term_points = [
        (
            get_x_coord(value),
            safe_fabs_positive(value.friction_term / units.PlanckMass),
        )
        for value in values
    ]
    negative_friction_term_points = [
        (
            get_x_coord(value),
            safe_fabs_negative(value.friction_term / units.PlanckMass),
        )
        for value in values
    ]
    positive_reflecting_term_points = [
        (
            get_x_coord(value),
            safe_fabs_positive(value.reflecting_term / units.PlanckMass),
        )
        for value in values
    ]
    negative_reflecting_term_points = [
        (
            get_x_coord(value),
            safe_fabs_negative(value.reflecting_term / units.PlanckMass),
        )
        for value in values
    ]
    positive_kicking_term_points = [
        (get_x_coord(value), safe_fabs_positive(value.kicking_term / units.PlanckMass))
        for value in values
    ]
    negative_kicking_term_points = [
        (get_x_coord(value), safe_fabs_negative(value.kicking_term / units.PlanckMass))
        for value in values
    ]

    positive_Sigma_x, positive_Sigma_y = zip(*positive_Sigma_points)
    negative_Sigma_x, negative_Sigma_y = zip(*negative_Sigma_points)
    positive_SigmaFm_x, positive_SigmaFm_y = zip(*positive_SigmaFm_points)
    negative_SigmaFm_x, negative_SigmaFm_y = zip(*negative_SigmaFm_points)

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

    add_ScalarModel_labels(fig, model, model_label)
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

    fig_path = plot_path / "ODE_terms.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a plot of the ODE terms for ScalarModel '{model_label}'"
        )

    plt.close()


def BBN_era_plot(
    plot_path, model: ScalarModel, model_label: str, T_Jordan_MeV, SigmaFm, H_Standard
):
    values = model.values
    units: UnitsLike = model._units

    # max and min BBN temperatures chosen to match the PRyMordial defaults
    LOG_T_BBN_MAX = log(10 * units.MeV)
    LOG_T_BBN_MIN = log(1e-3 * units.MeV)

    def is_in_BBN_era(value: ScalarModelValue) -> bool:
        return LOG_T_BBN_MIN <= value.log_T_Jordan <= LOG_T_BBN_MAX

    abs_phi_Einstein_BBN = [
        (
            T_Jordan_MeV(value),
            safe_fabs(value.phi_Einstein / units.PlanckMass),
        )
        for value in values
        if is_in_BBN_era(value)
    ]
    positive_abs_H_Jordan_BBN = [
        (T_Jordan_MeV(value), safe_fabs_positive(value.H_Jordan / units.GeV))
        for value in values
        if is_in_BBN_era(value)
    ]
    negative_abs_H_Jordan_BBN = [
        (T_Jordan_MeV(value), safe_fabs_negative(value.H_Jordan / units.GeV))
        for value in values
        if is_in_BBN_era(value)
    ]
    abs_H_Standard_BBN = [
        (T_Jordan_MeV(value), safe_fabs(H_Standard(value) / units.GeV))
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

    abs_phi_Einstein_BBN_x, abs_phi_Einstein_BBN_y = zip(*abs_phi_Einstein_BBN)
    positive_abs_H_Jordan_BBN_x, positive_abs_H_Jordan_BBN_y = zip(
        *positive_abs_H_Jordan_BBN
    )
    negative_abs_H_Jordan_BBN_x, negative_abs_H_Jordan_BBN_y = zip(
        *negative_abs_H_Jordan_BBN
    )
    abs_H_Standard_BBN_x, abs_H_Standard_BBN_y = zip(*abs_H_Standard_BBN)
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

    fig = plt.figure()
    fig.set_size_inches(8.0, 13.0)

    axs = fig.subplots(nrows=4, ncols=1, sharex=True, sharey=False)

    ODE_terms_BBN_ax = axs[0]
    Sigma_BBN_ax = axs[1]
    HJordan_BBN_ax = axs[2]
    phi_BBN_ax = axs[3]

    phi_BBN_ax.plot(
        abs_phi_Einstein_BBN_x,
        abs_phi_Einstein_BBN_y,
        label=r"$|\phi_{\text{E}}|$ [$M_{\text{P}}$]",
        color="r",
        linestyle="solid",
    )
    phi_BBN_ax.set_yscale("log")
    phi_BBN_ax.grid(True)

    HJordan_BBN_ax.plot(
        positive_abs_H_Jordan_BBN_x,
        positive_abs_H_Jordan_BBN_y,
        label=r"$H_{\text{Jordan}}$ [GeV]",
        color="g",
        linestyle="solid",
    )
    HJordan_BBN_ax.plot(
        negative_abs_H_Jordan_BBN_x,
        negative_abs_H_Jordan_BBN_y,
        color="g",
        linestyle="dashed",
    )
    HJordan_BBN_ax.plot(
        abs_H_Standard_BBN_x,
        abs_H_Standard_BBN_y,
        label=r"$H_{\text{standard}}$ [GeV]",
        color="b",
        linestyle="solid",
    )
    HJordan_BBN_ax.set_yscale("log")
    HJordan_BBN_ax.grid(True)

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

    add_ScalarModel_labels(fig, model, model_label)

    phi_BBN_ax.legend(loc="best")
    HJordan_BBN_ax.legend(loc="best")
    Sigma_BBN_ax.legend(loc="best")
    ODE_terms_BBN_ax.legend(loc="best")

    fig_path = plot_path / "BBN_era.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a BBN era plot for ScalarModel '{model_label}'"
        )

    plt.close()


def BBN_era_NP_plot(
    plot_path,
    model: ScalarModel,
    BBN: BBNData,
    model_label: str,
    T_Jordan_MeV,
    H_Standard,
):
    units: UnitsLike = model._units

    if not BBN.available:
        return

    # can assume model is available
    assert model.available
    assert not model.failure

    # pair up BBNDataValue instances with corresponding ScalarModelValue instances
    model_values: List[ScalarModelValue] = model.values
    num_model_values: int = len(model_values)
    current_model_index: int = 0

    BBN_value_partners: Dict[int, ScalarModelValue] = {}
    for BBN_value in BBN.values:
        while model_values[current_model_index].z.store_id != BBN_value.z.store_id:
            current_model_index += 1
            if current_model_index >= num_model_values:
                raise IndexError("Could not match BBN values to model values")

        BBN_value_partners[BBN_value.store_id] = model_values[current_model_index]

    GeV2 = units.GeV * units.GeV
    GeV4 = GeV2 * GeV2

    positive_abs_density_NP_points = [
        (
            T_Jordan_MeV(value),
            safe_fabs_positive(value.density_NP / GeV4),
        )
        for value in BBN.values
    ]
    positive_abs_pressure_NP_points = [
        (T_Jordan_MeV(value), safe_fabs_positive(value.pressure_NP / GeV4))
        for value in BBN.values
    ]

    negative_abs_density_NP_points = [
        (
            T_Jordan_MeV(value),
            safe_fabs_negative(value.density_NP / GeV4),
        )
        for value in BBN.values
    ]
    negative_abs_pressure_NP_points = [
        (T_Jordan_MeV(value), safe_fabs_negative(value.pressure_NP / GeV4))
        for value in BBN.values
    ]

    positive_density_NP_ratio = [
        (
            T_Jordan_MeV(value),
            safe_fabs_positive(value.density_NP_ratio),
        )
        for value in BBN.values
    ]
    negative_density_NP_ratio = [
        (
            T_Jordan_MeV(value),
            safe_fabs_negative(value.density_NP_ratio),
        )
        for value in BBN.values
    ]

    w_NP_points = [
        (T_Jordan_MeV(value), value.pressure_NP / value.density_NP)
        for value in BBN.values
    ]

    positive_abs_H_Jordan_points = [
        (
            T_Jordan_MeV(value),
            safe_fabs_positive(BBN_value_partners[value.store_id].H_Jordan / units.GeV),
        )
        for value in BBN.values
    ]
    negative_abs_H_Jordan_points = [
        (
            T_Jordan_MeV(value),
            safe_fabs_negative(BBN_value_partners[value.store_id].H_Jordan / units.GeV),
        )
        for value in BBN.values
    ]
    abs_H_standard_points = [
        (
            T_Jordan_MeV(value),
            safe_fabs(H_Standard(BBN_value_partners[value.store_id]) / units.GeV),
        )
        for value in BBN.values
    ]

    positive_abs_density_NP_x, positive_abs_density_NP_y = zip(
        *positive_abs_density_NP_points
    )
    negative_abs_density_NP_x, negative_abs_density_NP_y = zip(
        *negative_abs_density_NP_points
    )
    positive_abs_pressure_NP_x, positive_abs_pressure_NP_y = zip(
        *positive_abs_pressure_NP_points
    )
    negative_abs_pressure_NP_x, negative_abs_pressure_NP_y = zip(
        *negative_abs_pressure_NP_points
    )

    positive_density_NP_ratio_x, positive_density_NP_ratio_y = zip(
        *positive_density_NP_ratio
    )
    negative_density_NP_ratio_x, negative_density_NP_ratio_y = zip(
        *negative_density_NP_ratio
    )

    w_NP_x, w_NP_y = zip(*w_NP_points)

    positive_abs_H_Jordan_x, positive_abs_H_Jordan_y = zip(
        *positive_abs_H_Jordan_points
    )
    negative_abs_H_Jordan_x, negative_abs_H_Jordan_y = zip(
        *negative_abs_H_Jordan_points
    )
    abs_H_standard_x, abs_H_standard_y = zip(*abs_H_standard_points)

    fig = plt.figure()
    fig.set_size_inches(8.0, 13.0)

    axs = fig.subplots(nrows=4, ncols=1, sharex=True, sharey=False)

    rhoP_NP_ax = axs[3]
    H_ax = axs[2]
    ratio_NP_ax = axs[1]
    w_NP_ax = axs[0]

    rhoP_NP_ax.plot(
        positive_abs_density_NP_x,
        positive_abs_density_NP_y,
        label=r"$\rho_{\mathrm{NP}}$ [GeV$^4$]",
        color="b",
        linestyle="solid",
    )
    rhoP_NP_ax.plot(
        negative_abs_density_NP_x,
        negative_abs_density_NP_y,
        color="b",
        linestyle="dashed",
    )
    rhoP_NP_ax.plot(
        positive_abs_pressure_NP_x,
        positive_abs_pressure_NP_y,
        label=r"$P_{\mathrm{NP}}$ [GeV$^4$]",
        color="r",
        linestyle="solid",
    )
    rhoP_NP_ax.plot(
        negative_abs_pressure_NP_x,
        negative_abs_pressure_NP_y,
        color="r",
        linestyle="dashed",
    )
    rhoP_NP_ax.set_yscale("log")
    rhoP_NP_ax.grid(True)

    ratio_NP_ax.plot(
        positive_density_NP_ratio_x,
        positive_density_NP_ratio_y,
        label=r"$\rho_{\mathrm{NP}}/\rho_{\mathrm{SM}}$",
        color="b",
        linestyle="solid",
    )
    ratio_NP_ax.plot(
        negative_density_NP_ratio_x,
        negative_density_NP_ratio_y,
        color="b",
        linestyle="dashed",
    )
    ratio_NP_ax.set_yscale("log")
    ratio_NP_ax.grid(True)

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
    H_ax.plot(
        abs_H_standard_x,
        abs_H_standard_y,
        label=r"$H_{\text{standard}}$ [GeV]",
        color="r",
        linestyle="solid",
    )
    H_ax.set_yscale("log")
    H_ax.grid(True)

    w_NP_ax.axhline(y=1.0 / 3.0, color="r", linestyle="dashed")
    w_NP_ax.axhline(y=-1.0, color="c", linestyle="dashed")

    y_trans = w_NP_ax.get_yaxis_transform()
    w_NP_ax.text(
        0.15,
        0.4,
        r"radiation",
        color="r",
        transform=y_trans,
        fontsize="x-small",
    )
    w_NP_ax.text(
        0.15,
        -1.2,
        r"cosmological constant",
        color="c",
        transform=y_trans,
        fontsize="x-small",
    )

    w_NP_ax.plot(
        w_NP_x, w_NP_y, label=r"$w_{\mathrm{NP}}$", color="g", linestyle="solid"
    )
    w_NP_ax.set_ylim(-1.5, 1.2)
    w_NP_ax.grid(True)

    rhoP_NP_ax.set_xscale("log")
    rhoP_NP_ax.set_xlabel("Temperature $T$ [MeV]")
    rhoP_NP_ax.xaxis.set_inverted(True)

    add_ScalarModel_labels(fig, model, model_label)

    rhoP_NP_ax.legend(loc="best")
    ratio_NP_ax.legend(loc="best")
    H_ax.legend(loc="best")
    w_NP_ax.legend(loc="best")

    fig_path = plot_path / "BBN_era_NP.pdf"
    fig_path.parents[0].mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(fig_path)
        fig.savefig(fig_path.with_suffix(".png"))
    except OverflowError:
        print(
            f"@@ plot_ScalarModel: error occurred when generating a BBN NP plot for ScalarModel '{model_label}'"
        )

    plt.close()


def write_csv_file(
    BBN: BBNData,
    Q: AdiabaticHistory,
    csv_path: Path,
    model: ScalarModel,
    units: UnitsLike,
):
    model_values = (
        {value.z.store_id: value for value in model.values} if model.available else {}
    )
    Q_values = {value.z.store_id: value for value in Q.values} if Q.available else {}
    BBN_values = (
        {value.z.store_id: value for value in BBN.values} if BBN.available else {}
    )

    z_keys = set(model_values.keys()) | set(Q_values.keys()) | set(BBN_values.keys())

    log_GeV = log(units.GeV)

    def model_columns(store_id):
        if store_id in model_values:
            value = model_values[store_id]
            return {
                "z": float(value.z),
                "raw_N": value.raw_N,
                "phi_Einstein_Mp": value.phi_Einstein / units.PlanckMass,
                "pi_Einstein_Mp": value.pi_Einstein / units.PlanckMass,
                "H_Einstein_Mp": value.H_Einstein / units.PlanckMass,
                "H_Jordan_Mp": value.H_Jordan / units.PlanckMass,
                "log_rhorad_Einstein_GeV4": value.log_rhorad_Einstein - 4.0 * log_GeV,
                "log_rhorad_Jordan_GeV4": value.log_rhorad_Jordan - 4.0 * log_GeV,
                "log_fm": value.log_fm,
                "fm": exp(value.log_fm),
                "log_T_Jordan_GeV": value.log_T_Jordan - log_GeV,
                "T_Jordan_GeV": exp(value.log_T_Jordan) / units.GeV,
                "T_Jordan_Kelvin": exp(value.log_T_Jordan) / units.Kelvin,
                "gstar_rho": value.gstar_rho,
                "gstar_s": value.gstar_s,
                "dgstar_rho_dlogT": value.dgstar_rho_dlogT,
                "dgstar_s_dlogT": value.dgstar_s_dlogT,
                "Sigma": value.Sigma,
                "w": (1.0 - value.Sigma) / 3.0,
                "friction_term_Mp": value.friction_term / units.PlanckMass,
                "reflecting_term_Mp": value.reflecting_term / units.PlanckMass,
                "kicking_term_Mp": value.kicking_term / units.PlanckMass,
            }

        return {
            "z": nan,
            "raw_N": nan,
            "phi_Einstein_Mp": nan,
            "pi_Einstein_Mp": nan,
            "H_Einstein_Mp": nan,
            "H_Jordan_Mp": nan,
            "log_rhorad_Einstein_GeV4": nan,
            "log_rhorad_Jordan_GeV4": nan,
            "log_fm": nan,
            "fm": nan,
            "log_T_Jordan_GeV": nan,
            "T_Jordan_GeV": nan,
            "T_Jordan_Kelvin": nan,
            "gstar_rho": nan,
            "gstar_s": nan,
            "dgstar_rho_dlogT": nan,
            "dgstar_s_dlogT": nan,
            "Sigma": nan,
            "w": nan,
            "friction_term_Mp": nan,
            "reflecting_term_Mp": nan,
            "kicking_term_Mp": nan,
        }

    def Q_columns(store_id):
        if store_id in Q_values:
            return {}

        return {}

    def BBN_columns(store_id):
        if store_id in BBN_values:
            return {}

        return {}

    data = [
        model_columns(store_id) | Q_columns(store_id) | BBN_columns(store_id)
        for store_id in z_keys
    ]

    df = pd.DataFrame(data)
    df.sort_values(by="z", inplace=True, ascending=False, ignore_index=True)

    csv_path = csv_path / "fields.csv"
    csv_path.parents[0].mkdir(exist_ok=True, parents=True)

    df.to_csv(csv_path, header=True, index=False)


@ray.remote
def plot_ScalarModel(
    model_label: str,
    model: ScalarModel,
    Q: AdiabaticHistory,
    BBN: BBNData,
    x_coord: str = "redshift",
):
    coupling: AbstractCoupling = model.coupling
    potential: AbstractPotential = model.potential

    if not model.available:
        print(
            f"@@ plot_ScalarModel: scalar field history for Potential={potential.name}, Coupling={coupling.name} is not available; skipping this item"
        )
        return

    if model.failure:
        print(
            f"@@ plot_ScalarModel: scalar field history for Potential={potential.name}, Coupling={coupling.name} had an integration failure; skipping this item"
        )
        return

    if not Q.available:
        print(
            f"@@ plot_ScalarModel: adiabatic Q history for Potential={potential.name}, Coupling={coupling.name} is not available, and will not be plotted"
        )

    if not BBN.available:
        print(
            f"@@ plot_ScalarModel: BBN data for Potential={potential.name}, Coupling={coupling.name} is not available, and will not be plotted"
        )

    if x_coord not in ["redshift", "efolds"]:
        raise RuntimeError(f"Invalid x_coord: {x_coord}")

    beta = coupling._beta.as_float
    M = potential._M.as_float
    Lambda = potential._Lambda.as_float

    units: UnitsLike = model._units
    CONST_MP: float = units.PlanckMass
    CONST_MP2: float = CONST_MP * CONST_MP
    CONST_3_MP2: float = 3.0 * CONST_MP2

    base_path = Path(args.output).resolve()
    base_path = base_path / f"{model_label}"
    plot_path = (
        base_path
        / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV"
    )
    csv_path = (
        base_path
        / f"csv/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV"
    )

    def get_x_coord(value: ScalarModelValue) -> float:
        if x_coord == "efolds":
            return value.raw_N

        return value.z.z

    def x_axis_label() -> str:
        if x_coord == "efolds":
            return r"e-folds $N$"

        return r"redshift $1+z$"

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
        if hasattr(potential, "V"):
            return potential.V(value.phi_Einstein)
        elif hasattr(potential, "log_V"):
            return exp(potential.log_V(value.phi_Einstein))
        else:
            raise RuntimeError(
                f"Potential {potential.name} has no V or log_V implementation"
            )

    def TotalEnergy(value: ScalarModelValue) -> float:
        return KE(value) + PE(value)

    def T_Jordan_MeV(value: Union[ScalarModelValue, BBNDataValue]) -> float:
        return exp(value.log_T_Jordan) / units.MeV

    def H_Standard(value: ScalarModelValue) -> float:
        rhorad_Jordan: float = exp(value.log_rhorad_Jordan)
        fm: float = exp(value.log_fm)
        H2: float = rhorad_Jordan * (1.0 + fm) / CONST_3_MP2
        H: float = sqrt(H2)
        return H

    sns.set_theme()

    history_plot(
        plot_path, model, Q, model_label, get_x_coord, x_axis_label, x_coord=x_coord
    )
    thermo_plot(
        plot_path, model, model_label, get_x_coord, x_axis_label, x_coord=x_coord
    )
    Hubble_plot(
        plot_path,
        model,
        BBN,
        model_label,
        get_x_coord,
        x_axis_label,
        H_Standard,
        x_coord=x_coord,
    )
    energy_plot(
        plot_path,
        model,
        model_label,
        get_x_coord,
        x_axis_label,
        KE,
        PE,
        TotalEnergy,
        x_coord=x_coord,
    )
    ODE_terms_plot(
        plot_path,
        model,
        model_label,
        get_x_coord,
        x_axis_label,
        SigmaFm,
        x_coord=x_coord,
    )
    BBN_era_plot(plot_path, model, model_label, T_Jordan_MeV, SigmaFm, H_Standard)
    BBN_era_NP_plot(plot_path, model, BBN, model_label, T_Jordan_MeV, H_Standard)

    write_csv_file(BBN, Q, csv_path, model, units)


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

        model_query_payload = {
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

        model: ScalarModel = ray.get(
            pool.object_get("ScalarModel", **model_query_payload)
        )
        model_proxy: ScalarModelProxy = ScalarModelProxy(model)

        adiabatic_query_payload = {
            "shard_key": coupling.shard_key,
            "model_proxy": model_proxy,
        }

        Q: AdiabaticHistory = pool.object_get(
            "AdiabaticHistory", **adiabatic_query_payload
        )

        BBN_query_payload = {
            "shard_key": coupling.shard_key,
            "model_proxy": model_proxy,
        }

        BBN: BBNData = pool.object_get("BBNData", **BBN_query_payload)

        return plot_ScalarModel.remote(model_label, model, Q, BBN, x_coord="efolds")

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
