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

import itertools
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import ray
import seaborn as sns
from matplotlib import pyplot as plt
from numpy import nan

from ComputeTargets import ScalarModelProxy, AdiabaticHistory, BBNData, ScalarModel
from ComputeTargets.BBNData import compute_SM_baseline
from CosmologyConcepts import temperature, phi_value, pi_value
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ProfileAgent import ProfileAgent
from Datastore.SQL.ShardedPool import ShardedPool
from MetadataConcepts import tolerance, store_tag
from RayTools.RayWorkPool import RayWorkPool
from Units import Planck_units
from config.argument_parser import create_argument_parser
from config.model_list import build_model_list
from config.sharding import (
    ShardKeyType,
    get_shard_key_store_id,
    replicated_tables,
    sharded_tables,
    read_table_config,
    inventory_config,
)
from config.version import VERSION_LABEL
from extract_common import (
    add_beta_summary_labels,
    nice_Q_labels,
    add_BBN_info_labels,
    reflection_count,
)

DEFAULT_TIMEOUT = 60

DEFAULT_T_INIT_GEV = 20000

potential_types = ["Exponential", "InversePower", "Starobinsky", "Recliner"]


parser = create_argument_parser()
parser.add_argument(
    "--no-baseline",
    action="store_true",
    default=False,
    help="do not compute the Standard-Model BBN baseline (rho_NP = 0, one PRyMordial solve) or draw it on the abundance panels",
)
args = parser.parse_args()

if args.database is None:
    parser.print_help()
    sys.exit()

# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
ray.init(address=args.ray_address)

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
def build_beta_plot(
    model_label,
    potential: AbstractPotential,
    Q_data: List[AdiabaticHistory],
    bbn_data: list[BBNData],
    scalar_data: list[ScalarModel],
    SM_baseline: Optional[dict] = None,
):
    base_path = Path(args.output).resolve()
    base_path = base_path / f"{model_label}"

    PRyM_versions = set(
        [d.PRyM_version for d in bbn_data if d.available and not d.failure]
    )
    small_networks = set(
        [d.small_network for d in bbn_data if d.available and not d.failure]
    )
    bbn_labels = {
        "small_network": (
            "Multiple"
            if len(small_networks) > 1
            else "True" if True in small_networks else "False"
        ),
        "PRyM_version": (
            PRyM_versions.pop() if len(PRyM_versions) == 1 else "Multiple"
        ),
    }

    solver_time_points = [
        (m.coupling._beta.as_float, m.metadata.compute_time)
        for m in scalar_data
        if m.available and not m.failure
    ]
    bbn_time_points = [
        (d.coupling._beta.as_float, d.BBN_compute_time)
        for d in bbn_data
        if d.available and not d.failure
    ]

    adiabatic_maxQ_points = [
        (
            q.coupling._beta.as_float,
            {label: q.max_abs_Q(label) for label in AdiabaticHistory.Q_labels},
        )
        for q in Q_data
        if q.available
    ]

    # ignore negative values which must represent a PRyMordial integration failure
    Yp_points = [
        (d.coupling._beta.as_float, d.Yp_BBN)
        for d in bbn_data
        if d.available and not d.failure and d.Yp_BBN > 0
    ]
    DOverH_points = [
        (d.coupling._beta.as_float, d.DOverH)
        for d in bbn_data
        if d.available and not d.failure and d.DOverH > 0
    ]
    Li7_points = [
        (d.coupling._beta_float, d.Li7OverH)
        for d in bbn_data
        if d.available and not d.failure and d.Li7OverH > 0
    ]

    if len(Yp_points) == 0:
        print("!! build_beta_plot: no valid BBN data to plot for '{model_label}'")
        return

    solver_time_x, solver_time_y = zip(*solver_time_points)
    bbn_time_x, bbn_time_y = zip(*bbn_time_points)
    Yp_x, Yp_y = zip(*Yp_points)
    DOverH_x, DOverH_y = zip(*DOverH_points)
    Li7_x, Li7_y = zip(*Li7_points)

    adiabtic_maxQ_xy = {}
    for label in AdiabaticHistory.Q_labels:
        _points = [(x, y[label]) for x, y in adiabatic_maxQ_points]
        adiabtic_maxQ_xy[label] = zip(*_points)

    sns.set_theme()

    if len(solver_time_x) > 0 or len(bbn_time_x) > 0:
        # TIMINGS

        fig = plt.figure()
        fig.set_size_inches(8.0, 8.0)

        axs = fig.subplots(nrows=2, ncols=1, sharex=True, sharey=False)

        solver_ax = axs[1]
        bbn_ax = axs[0]

        solver_ax.plot(
            solver_time_x,
            solver_time_y,
            label=r"scalar field",
            color="r",
            marker="o" if len(solver_time_x) <= 20.0 else None,
        )
        solver_ax.set_yscale("log")

        bbn_ax.plot(
            bbn_time_x,
            bbn_time_y,
            label=r"PRyMordial",
            color="b",
            marker="o" if len(bbn_time_x) <= 20.0 else None,
        )
        bbn_ax.set_yscale("log")

        bbn_ax.set_ylabel(r"time [sec]")

        solver_ax.set_xlabel(r"coupling $\beta$")
        solver_ax.set_ylabel(r"time [sec]")
        solver_ax.grid(True)

        add_beta_summary_labels(fig, model_label, potential)
        add_BBN_info_labels(fig, labels=bbn_labels)

        solver_ax.legend(loc="best")
        bbn_ax.legend(loc="best")

        fig_path = (
            base_path
            / f"plots/M={potential._M.as_float / units.eV:.5g}eV_Lambda={potential._Lambda.as_float / units.eV:.5g}eV/timings.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        try:
            fig.savefig(fig_path)
            fig.savefig(fig_path.with_suffix(".png"))
        except OverflowError:
            print(
                f"!! build_beta_plot: error occurred when generating a timing plot for '{model_label}'"
            )

        plt.close()

    if len(Yp_x) > 0 or len(DOverH_x) > 0 or len(Li7_x) > 0:
        # BBN OUTPUT

        fig = plt.figure()
        fig.set_size_inches(8.0, 10.0)

        axs = fig.subplots(nrows=3, ncols=1, sharex=True, sharey=False)

        Yp_ax = axs[2]
        D_ax = axs[1]
        Li7_ax = axs[0]

        Yp_data = {
            "Aver+2017": {
                "label": "Aver+2015",
                "central_value": 0.2449,
                "sigma": 0.0040,
                "colour": "g",
            },
            # Eq. (24.3), page 5 of PDG 2024 review
            "PDG2022": {
                "label": "PDG2024",
                "central_value": 0.245,
                "sigma": 0.003,
                "colour": "r",
            },
        }

        D_data = {
            "Cooke+2017": {
                "label": "Cooke+2017",
                "central_value": 2.527,
                "sigma": 0.030,
                "colour": "g",
            },
            # Eq. (24.2), page 3 of PDG 2024 review
            "PDG2022": {
                "label": "PDG2024",
                "central_value": 2.547,
                "sigma": 0.029,
                "colour": "r",
            },
        }

        Li7_data = {
            # Eq. (24.4), page 5 of PDG 2024 review
            "PDG2024": {
                "label": "PDG2024",
                "central_value": 1.6,
                "sigma": 0.3,
                "colour": "r",
            }
        }

        def add_data_to_axis(ax, data):
            for key, config in data.items():
                ax.axhline(
                    config["central_value"],
                    color=config["colour"],
                    linestyle="dashed",
                    label=config["label"],
                )
                ax.axhspan(
                    ymin=config["central_value"] - config["sigma"],
                    ymax=config["central_value"] + config["sigma"],
                    color=config["colour"],
                    alpha=0.25,
                    label=None,
                )
                ax.axhspan(
                    ymin=config["central_value"] - 3.0 * config["sigma"],
                    ymax=config["central_value"] + 3.0 * config["sigma"],
                    color=config["colour"],
                    alpha=0.15,
                    label=None,
                )

        add_data_to_axis(Yp_ax, Yp_data)
        add_data_to_axis(D_ax, D_data)
        add_data_to_axis(Li7_ax, Li7_data)

        # the Standard-Model baseline through the same PRyMordial path (rho_NP = 0),
        # computed once at plot time and not stored (review-remediation prompt 04)
        if SM_baseline is not None:
            for ax, key in (
                (Yp_ax, "Yp_BBN"),
                (D_ax, "DOverH"),
                (Li7_ax, "Li7OverH"),
            ):
                ax.axhline(
                    SM_baseline[key],
                    color="k",
                    linestyle="dotted",
                    label=r"SM baseline ($\rho_{\mathrm{NP}} = 0$)",
                )

        Yp_ax.plot(
            Yp_x,
            Yp_y,
            label=r"$Y_p$ (BBN)",
            color="b",
            marker="o" if len(Yp_x) <= 20.0 else None,
        )
        Yp_ax.grid(True)

        D_ax.plot(
            DOverH_x,
            DOverH_y,
            label=r"$10^5 \; \mathrm{D}/\mathrm{H}$",
            color="m",
            marker="o" if len(DOverH_x) <= 20.0 else None,
        )
        D_ax.grid(True)

        Li7_ax.plot(
            Li7_x,
            Li7_y,
            label=r"$10^{10} \; \mathrm{Li}^7/\mathrm{H}$",
            color="c",
            marker="o" if len(Li7_x) <= 20.0 else None,
        )

        Yp_ax.set_xlabel(r"coupling $\beta$")

        add_beta_summary_labels(fig, model_label, potential)
        add_BBN_info_labels(fig, labels=bbn_labels)

        def get_max_min(data):
            max_value = None
            min_value = None

            for key, config in data.items():
                this_max = config["central_value"] + 5.5 * config["sigma"]
                this_min = config["central_value"] - 5.5 * config["sigma"]

                if max_value is None or this_max > max_value:
                    max_value = this_max
                if min_value is None or this_min < min_value:
                    min_value = this_min

            return max_value, min_value

        if args.Yp_axis_limits:
            # max_Yp = 1.05 * max(max(Yp_y), Yp_central_value + 3.0 * Yp_sigma)
            # min_Yp = 0.95 * min(min(Yp_y), Yp_central_value - 3.0 * Yp_sigma)
            max_Yp, min_Yp = get_max_min(Yp_data)
            Yp_ax.set_ylim(min_Yp, max_Yp)

        if args.D_axis_limits:
            # max_DOverH = max(max(DOverH_y), DOverH_central_value + 3.0 * DOverH_sigma)
            # min_DOverH = min(min(DOverH_y), DOverH_central_value - 3.0 * DOverH_sigma)
            # if max_DOverH / min_DOverH > 50.0:
            #     D_ax.set_yscale("log")
            #     D_ax.set_ylim(min_DOverH / 10.0, 10.0 * max_DOverH)
            # else:
            #     D_ax.set_ylim(0.95 * min_DOverH, 1.06 * max_DOverH)
            max_D, min_D = get_max_min(D_data)
            D_ax.set_ylim(min_D, max_D)

        if args.Li7_axis_limits:
            max_Li7, min_Li7 = get_max_min(Li7_data)
            Li7_ax.set_ylim(min_Li7, max_Li7)

        Yp_ax.legend(loc="best")
        D_ax.legend(loc="best")
        Li7_ax.legend(loc="best")

        fig_path = (
            base_path
            / f"plots/M={potential._M.as_float / units.eV:.5g}eV_Lambda={potential._Lambda.as_float / units.eV:.5g}eV/BBN.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        try:
            fig.savefig(fig_path)
            fig.savefig(fig_path.with_suffix(".png"))
        except OverflowError:
            print(
                f"!! build_beta_plot: error occurred when generating a BBN abundance plot for '{model_label}'"
            )

        plt.close()

    if len(adiabtic_maxQ_xy) > 0:
        # BBN OUTPUT

        fig = plt.figure()
        fig.set_size_inches(8.0, 8.0)

        ax = fig.gca()

        for label in AdiabaticHistory.Q_labels:
            x, y = adiabtic_maxQ_xy[label]
            ax.plot(
                x,
                y,
                label=nice_Q_labels[label],
                marker="o" if len(x) <= 20.0 else None,
            )
        ax.set_yscale("log")

        ax.set_xlabel(r"coupling $\beta$")
        ax.set_ylabel(r"maximum $|Q| = |\omega_k'/\omega_k^2|$")
        ax.grid(True)

        add_beta_summary_labels(fig, model_label, potential)

        ax.legend(loc="best")

        fig_path = (
            base_path
            / f"plots/M={potential._M.as_float / units.eV:.5g}eV_Lambda={potential._Lambda.as_float / units.eV:.5g}eV/max_Q.pdf"
        )
        fig_path.parents[0].mkdir(exist_ok=True, parents=True)
        try:
            fig.savefig(fig_path)
            fig.savefig(fig_path.with_suffix(".png"))
        except OverflowError:
            print(
                f"@@ build_beta_plot: error occurred when generating a max(Q) plot for ScalarModel '{model_label}'"
            )

        plt.close()

        beta_to_models = {
            m.coupling._beta.store_id: m
            for m in scalar_data
            if m.available and not m.failure
        }
        beta_to_Q = {Q.coupling._beta.store_id: Q for Q in Q_data if Q.available}
        beta_to_BBN = {
            B.coupling._beta.store_id: B
            for B in bbn_data
            if B.available and not B.failure
        }

        beta_keys = (
            set(beta_to_models.keys()) | set(beta_to_Q.keys()) | set(beta_to_BBN.keys())
        )
        data = [
            {
                "beta": (
                    beta_to_models[store_id].coupling._beta.as_float
                    if store_id in beta_to_models
                    else (
                        beta_to_Q[store_id].coupling._beta.as_float
                        if store_id in beta_to_Q
                        else beta_to_BBN[store_id].coupling._beta.as_float
                    )
                ),
                "scalar_compute_time": (
                    beta_to_models[store_id].metadata.compute_time
                    if store_id in beta_to_models
                    else nan
                ),
                "BBN_compute_time": (
                    beta_to_BBN[store_id].BBN_compute_time
                    if store_id in beta_to_BBN
                    else nan
                ),
                "NP_compute_time": (
                    beta_to_BBN[store_id].NP_compute_time
                    if store_id in beta_to_BBN
                    else nan
                ),
                "reflections": (
                    reflection_count(beta_to_models[store_id].extra_metadata)
                    if store_id in beta_to_models
                    else nan
                ),
                "Yp_BBN": (
                    beta_to_BBN[store_id].Yp_BBN if store_id in beta_to_BBN else nan
                ),
                "D_over_H": (
                    beta_to_BBN[store_id].DOverH if store_id in beta_to_BBN else nan
                ),
                "He3_over_H": (
                    beta_to_BBN[store_id].He3OverH if store_id in beta_to_BBN else nan
                ),
                "Li7_over_H": (
                    beta_to_BBN[store_id].Li7OverH if store_id in beta_to_BBN else nan
                ),
            }
            | (
                {
                    label: beta_to_Q[store_id].max_abs_Q(label)
                    for label in AdiabaticHistory.Q_labels
                }
                if store_id in beta_to_Q
                else {label: nan for label in AdiabaticHistory.Q_labels}
            )
            for store_id in beta_keys
        ]

        # report how many models had bounces modelled by the elastic reflection
        M_eV = potential._M.as_float / units.eV
        Lambda_eV = potential._Lambda.as_float / units.eV
        reflected = sorted(
            (
                (m.coupling._beta.as_float, reflection_count(m.extra_metadata))
                for m in beta_to_models.values()
            ),
            key=lambda x: x[0],
        )
        reflected = [(beta, count) for beta, count in reflected if count > 0]
        if len(reflected) > 0:
            print(
                f"!! build_beta_plot '{model_label}', M={M_eV:.5g} eV, Lambda={Lambda_eV:.5g} eV: {len(reflected)} of {len(beta_to_models)} model(s) used elastic reflections"
            )
            for beta, count in reflected:
                print(
                    f"     -- beta={beta:.5g}, M={M_eV:.5g} eV, Lambda={Lambda_eV:.5g} eV: {count} reflection(s)"
                )
        else:
            print(
                f"@@ build_beta_plot '{model_label}', M={M_eV:.5g} eV, Lambda={Lambda_eV:.5g} eV: no models used elastic reflections ({len(beta_to_models)} model(s) checked)"
            )

        df = pd.DataFrame(data)
        df.sort_values(by="beta", inplace=True, ascending=True, ignore_index=True)

        csv_path = (
            base_path
            / f"csv/M={potential._M.as_float / units.eV:.5g}eV_Lambda={potential._Lambda.as_float / units.eV:.5g}eV/data.csv"
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
    tags: List[store_tag],
    SM_baseline: Optional[dict],
):
    model_label = model_data["label"]
    model_cosmology = model_data["cosmology"]

    print(f"\n>> RUNNING PIPELINE FOR MODEL {model_label}")

    def report_dropped_bbn_models(
        model_label: str,
        potential: AbstractPotential,
        model_proxies: List[ScalarModelProxy],
        bbn_results: List[BBNData],
    ):
        """
        Print, once per potential, every model the BBN plots drop, with its
        (beta, M, Lambda) and the reason, and the count. Nothing plotted depends
        on this. (review-remediation prompt 03)

        Two filters drop a model. The lookup asks for successful rows only, so a
        model whose BBN computation failed comes back unavailable; its failed row,
        if there is one, is looked up here for its failure_reason (the newest, if
        there are several). build_beta_plot then drops a stored non-positive
        abundance from the panel it belongs to.
        """
        missing = [
            proxy for proxy, B in zip(model_proxies, bbn_results) if not B.available
        ]

        dropped = []

        if len(missing) > 0:
            failed_query_queue = RayWorkPool(
                pool,
                [
                    {
                        "shard_key": m.shard_key,
                        "model_proxy": m,
                        "failure": True,
                        "tags": tags,
                        "_do_not_populate": True,
                    }
                    for m in missing
                ],
                task_builder=lambda x: pool.object_get("BBNData", **x),
                available_handler=None,
                compute_handler=None,
                store_handler=None,
                validation_handler=None,
                label_builder=None,
                title=None,
                store_results=True,
                create_batch_size=20,
                process_batch_size=20,
            )
            failed_query_queue.run()

            for F in failed_query_queue.results:
                if F.available:
                    reason = F.failure_reason
                    if reason is None:
                        reason = "failed, no failure_reason stored"
                else:
                    reason = "no BBNData row in the store"
                dropped.append((F.coupling._beta.as_float, reason))

        for B in bbn_results:
            if B.available and not B.failure:
                abundances = {"Yp": B.Yp_BBN, "D/H": B.DOverH, "7Li/H": B.Li7OverH}
                non_positive = [
                    name for name, value in abundances.items() if not value > 0
                ]
                if len(non_positive) > 0:
                    values = ", ".join(
                        f"{name}={value:.5g}" for name, value in abundances.items()
                    )
                    dropped.append(
                        (
                            B.coupling._beta.as_float,
                            f"non-positive {'/'.join(non_positive)} ({values}); dropped from those panels",
                        )
                    )

        if len(dropped) == 0:
            return

        M_eV = potential._M.as_float / units.eV
        Lambda_eV = potential._Lambda.as_float / units.eV
        print(
            f"!! plot_by_beta '{model_label}', M={M_eV:.5g} eV, Lambda={Lambda_eV:.5g} eV: {len(dropped)} model(s) dropped from the BBN plots"
        )
        for beta, reason in sorted(dropped, key=lambda x: x[0]):
            print(
                f"     -- beta={beta:.5g}, M={M_eV:.5g} eV, Lambda={Lambda_eV:.5g} eV: {reason}"
            )

    def build_plot_work(potential: AbstractPotential) -> ray.ObjectRef:
        # build a work queue to read in all ScalarModel instances with this potential, for the
        # couplings in Coupling_array
        model_query_batch = [
            {
                "shard_key": coupling.shard_key,
                "solver_labels": ["Radau+kinematic-cap-stepping0"],
                "failure": False,
                "cosmology": model_cosmology,
                "T_Jordan_init": T_init,
                "T_Jordan_stop": T_stop,
                "phi_Einstein_init": phi_init,
                "pi_Einstein_init": pi_init,
                "z_grid": None,
                "potential": potential,
                "coupling": coupling,
                "atol": atol,
                "rtol": rtol,
                "tags": tags,
                "_do_not_populate": True,
            }
            for coupling in Coupling_array
        ]
        model_query_queue = RayWorkPool(
            pool,
            model_query_batch,
            task_builder=lambda x: pool.object_get("ScalarModel", **x),
            available_handler=None,
            compute_handler=None,
            store_handler=None,
            validation_handler=None,
            label_builder=None,
            title=None,
            store_results=True,
            create_batch_size=20,
            process_batch_size=20,
        )
        model_query_queue.run()

        available_models = [
            m for m in model_query_queue.results if m.available and not m.failure
        ]
        model_proxies = [ScalarModelProxy(m) for m in available_models]

        adiabatic_query_batch = [
            {
                "shard_key": m.shard_key,
                "model_proxy": m,
                "tags": tags,
                "_do_not_populate": True,
            }
            for m in model_proxies
        ]
        adiabatic_query_queue = RayWorkPool(
            pool,
            adiabatic_query_batch,
            task_builder=lambda x: pool.object_get("AdiabaticHistory", **x),
            available_handler=None,
            compute_handler=None,
            store_handler=None,
            validation_handler=None,
            label_builder=None,
            title=None,
            store_results=True,
            create_batch_size=20,
            process_batch_size=20,
        )
        adiabatic_query_queue.run()
        available_adiabatic = [Q for Q in adiabatic_query_queue.results if Q.available]

        bbn_query_batch = [
            {
                "shard_key": m.shard_key,
                "model_proxy": m,
                "failure": False,
                "tags": tags,
                "_do_not_populate": True,
            }
            for m in model_proxies
        ]
        bbn_query_queue = RayWorkPool(
            pool,
            bbn_query_batch,
            task_builder=lambda x: pool.object_get("BBNData", **x),
            available_handler=None,
            compute_handler=None,
            store_handler=None,
            validation_handler=None,
            label_builder=None,
            title=None,
            store_results=True,
            create_batch_size=20,
            process_batch_size=20,
        )
        bbn_query_queue.run()
        available_bbn = [B for B in bbn_query_queue.results if B.available]

        report_dropped_bbn_models(
            model_label, potential, model_proxies, bbn_query_queue.results
        )

        return build_beta_plot.remote(
            model_label,
            potential,
            available_adiabatic,
            available_bbn,
            available_models,
            SM_baseline,
        )

    work_queue = RayWorkPool(
        pool,
        Potential_array,
        task_builder=build_plot_work,
        compute_handler=None,
        store_handler=None,
        available_handler=None,
        validation_handler=None,
        label_builder=None,
        create_batch_size=10,
        notify_batch_size=10,
        max_task_queue=10,
        notify_min_time_interval=120,
        title="GENERATING SUMMARY PLOTS BY BETA",
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
    inventory_config=inventory_config,
) as pool:
    # build absolute and relative tolerances
    atol, rtol = ray.get(
        [
            pool.object_get(tolerance, tol=args.abs_tol),
            pool.object_get(tolerance, tol=args.rel_tol),
        ]
    )

    # get list of models we want to extract transfer functions for
    units = Planck_units()

    T_init_GeV: float = args.T_init_GeV
    T_stop_GeV: float = args.T_stop_GeV

    T_init = ray.get(
        pool.object_get("temperature", value=T_init_GeV * units.GeV, units=units)
    )

    phi_init, pi_init = ray.get(
        [
            pool.object_get("phi_value", value=5.0 * units.PlanckMass, units=units),
            pool.object_get("pi_value", value=0.0, units=units),
        ]
    )

    def convert_to_potential(M_lambda_set):
        if args.potential_type == "Exponential":
            return pool.object_get(
                "ExponentialPotential",
                payload_data=[
                    {"M": M, "Lambda": Lambda, "n": 1, "units": units}
                    for M, Lambda in M_lambda_set
                ],
            )
        elif args.potential_type == "InversePower":
            return pool.object_get(
                "InversePowerPotential",
                payload_data=[
                    {"M": M, "Lambda": Lambda, "n": 1, "units": units}
                    for M, Lambda in M_lambda_set
                ],
            )
        elif args.potential_type == "Starobinsky":
            return pool.object_get(
                "StarobinskyPotential",
                payload_data=[
                    {"M": M, "Lambda": Lambda, "units": units}
                    for M, Lambda in M_lambda_set
                ],
            )
        elif args.potential_type == "Recliner":
            return pool.object_get(
                "ReclinerPotential",
                payload_data=[
                    {"M": M, "Lambda": Lambda, "units": units}
                    for M, Lambda in M_lambda_set
                ],
            )
        else:
            raise ValueError(f"Unknown potential type: {args.potential_type}")

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

    # The Standard-Model baseline: rho_NP = 0 through compute_BBN_data's PRyMordial
    # settings, one solve, not stored. small_network=False is what main.py passes.
    # (review-remediation prompt 04)
    SM_baseline = None
    if not args.no_baseline:
        SM_baseline = compute_SM_baseline(small_network=False)
        print(
            f"@@ plot_by_beta: SM baseline (rho_NP = 0, PRyM_version={SM_baseline['PRyM_version']}, small_network={SM_baseline['small_network']}): "
            f"Yp={SM_baseline['Yp_BBN']:.6g}, D/H x1e5={SM_baseline['DOverH']:.6g}, 3He/H x1e5={SM_baseline['He3OverH']:.6g}, 7Li/H x1e10={SM_baseline['Li7OverH']:.6g}"
        )

    for model_data in model_list:
        cosmology: BaseCosmology = model_data["cosmology"]

        if T_stop_GeV is None:
            T_CMB = cosmology._params.T_CMB_Kelvin * units.Kelvin
            T_stop = ray.get(pool.object_get("temperature", value=T_CMB, units=units))
        else:
            T_stop = ray.get(
                pool.object_get(
                    "temperature", value=T_stop_GeV * units.GeV, units=units
                )
            )

        tags = []

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
            tags,
            SM_baseline,
        )
