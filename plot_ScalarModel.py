import argparse
import itertools
import sys
from datetime import datetime
from math import exp, log
from pathlib import Path
from typing import List

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
    w_points = [(_get_x_coord(value), (1.0 - value.Sigma) / 3.0) for value in values]
    gstar_rho_points = [(_get_x_coord(value), value.gstar_rho) for value in values]
    gstar_s_points = [(_get_x_coord(value), value.gstar_s) for value in values]
    dgstar_rho_points = [
        (_get_x_coord(value), value.dgstar_rho_dT * units.GeV) for value in values
    ]
    dgstar_s_points = [
        (_get_x_coord(value), value.dgstar_s_dT * units.GeV) for value in values
    ]

    friction_term_points = [
        (_get_x_coord(value), value.friction_term / units.PlanckMass)
        for value in values
    ]
    reflecting_term_points = [
        (_get_x_coord(value), value.reflecting_term / units.PlanckMass)
        for value in values
    ]
    kicking_term_points = [
        (_get_x_coord(value), value.kicking_term / units.PlanckMass) for value in values
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

    abs_phi_Einstein_x, abs_phi_Einstein_y = zip(*abs_phi_Einstein_points)
    pi_Einstein_x, pi_Einstein_y = zip(*pi_Einstein_points)
    T_Jordan_x, T_Jordan_y = zip(*T_Jordan_points)
    Sigma_x, Sigma_y = zip(*Sigma_points)
    w_x, w_y = zip(*w_points)
    gstar_rho_x, gstar_rho_y = zip(*gstar_rho_points)
    gstar_s_x, gstar_s_y = zip(*gstar_s_points)
    dgstar_rho_x, dgstar_rho_y = zip(*dgstar_rho_points)
    dgstar_s_x, dgstar_s_y = zip(*dgstar_s_points)

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

    friction_term_x, friction_term_y = zip(*friction_term_points)
    reflecting_term_x, reflecting_term_y = zip(*reflecting_term_points)
    kicking_term_x, kicking_term_y = zip(*kicking_term_points)

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
            label=r"$\mathrm{d} g_{*\rho}/\mathrm{d} T$ [GeV$^{-1}$]",
            color="g",
            linestyle="solid",
        )
        dgstar_ax.plot(
            dgstar_s_x,
            dgstar_s_y,
            label=r"$\mathrm{d} g_{*s}/\mathrm{d} T$ [GeV$^{-1}$]",
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
        fig.set_size_inches(8.0, 10.0)

        axs = fig.subplots(nrows=3, ncols=1, sharex=True, sharey=False)

        f_ax = axs[0]
        r_ax = axs[1]
        k_ax = axs[2]

        f_ax.plot(
            friction_term_x,
            friction_term_y,
            label=r"friction term [$M_{\text{P}}$]",
            color="r",
            linestyle="solid",
        )
        f_ax.grid(True)

        r_ax.plot(
            reflecting_term_x,
            reflecting_term_y,
            label=r"reflecting term [$M_{\text{P}}$]",
            color="g",
            linestyle="solid",
        )
        r_ax.grid(True)

        k_ax.plot(
            kicking_term_x,
            kicking_term_y,
            label=r"kicking term [$M_{\text{P}}$]",
            color="b",
            linestyle="solid",
        )
        k_ax.grid(True)

        if x_coord == "redshift":
            k_ax.set_xscale("log")
            k_ax.xaxis.set_inverted(True)
        k_ax.set_xlabel(x_axis_label())

        add_plot_labels(f_ax, model, model_label, shift=0.05)
        add_redshift_xaxis_labels(f_ax, model, text_labels=False, x_coord=x_coord)
        add_redshift_xaxis_labels(r_ax, model, text_labels=False, x_coord=x_coord)
        h, l = add_redshift_xaxis_labels(
            k_ax, model, temp_unit="GeV", text_labels=True, x_coord=x_coord
        )

        f_ax.legend(loc="best")
        r_ax.legend(loc="best")

        handles, labels = k_ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)
        k_ax.legend(handles, labels, loc="best")

        fig_path = (
            base_path
            / f"plots/beta={beta:.5g}/M={M/units.eV:.5g}eV_Lambda={Lambda/units.eV:.5g}eV/ODE_terms.pdf"
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
                    "dgstar_rho_dT_GeVinv": val.dgstar_rho_dT * units.GeV,
                    "dgstar_s_dT_GeVinv": val.dgstar_s_dT * units.GeV,
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
