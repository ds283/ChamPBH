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
from math import fabs
from typing import List, Tuple, Any

import configargparse
import numpy as np
import ray

from ComputeTargets import (
    ScalarModel,
    AdiabaticHistory,
    ScalarModelProxy,
    BBNData,
)
from CosmologyConcepts import (
    DimensionlessQuantityArray,
    DimensionfulQuantityArray,
    temperature,
    redshift_array,
    phi_value,
    pi_value,
    beta_value,
    M_value,
    Lambda_value,
)
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ObjectFactories import tolerance
from Datastore.SQL.ProfileAgent import ProfileAgent
from Datastore.SQL.ShardedPool import ShardedPool
from Quadrature.integration_metadata import IntegrationSolver
from RayTools.RayWorkPool import RayWorkPool
from Units import Planck_units
from Units.base import UnitsLike
from config.defaults import (
    DEFAULT_ABS_TOLERANCE,
    DEFAULT_REL_TOLERANCE,
    DEFAULT_FLOAT_PRECISION,
)
from config.model_list import build_model_list
from config.sharding import (
    replicated_tables,
    sharded_tables,
    get_shard_key_store_id,
    ShardKeyType,
    read_table_config,
    inventory_config,
)
from utilities import grouper

DEFAULT_LABEL = "ChamPBH-test"
DEFAULT_TIMEOUT = 60
DEFAULT_SHARDS = 20
DEFAULT_RAY_ADDRESS = "auto"

DEFAULT_Z_END = 0.1
DEFAULT_T_INIT_GEV = 20000

DEFAULT_LOG10_ONE_PLUS_Z_HIGH = 35
DEFAULT_LOG10_ONE_PLUS_Z_LOW = 0
DEFAULT_SAMPLES_PER_LOG10_Z = 250

DEFAULT_BETA_LOW = 0.1
DEFAULT_BETA_HIGH = 3.0
DEFAULT_SAMPLES_PER_BETA = 5

DEFAULT_LOG10_M_LOW_EV = 25
DEFAULT_LOG10_M_HIGH_EV = 26.5
DEFAULT_SAMPLES_PER_LOG10_M_EV = 6

DEFAULT_LOG10_LAMBDA_LOW_EV = -2
DEFAULT_LOG10_LAMBDA_HIGH_EV = 1
DEFAULT_SAMPLES_PER_LOG10_LAMBDA_EV = 6

MIN_NOTIFY_INTERVAL = 5 * 60

allowed_drop_actions = ["scalar-model", "adiabatic-history", "bbn-data"]
potential_types = ["Exponential", "InversePower", "Starobinsky", "Recliner"]

parser = configargparse.ArgumentParser()

parser.add_argument(
    "--database",
    type=str,
    default=None,
    help="read/write work items using the specified database cache",
)
parser.add_argument(
    "--inventory",
    default=False,
    action=configargparse.BooleanOptionalAction,
    help="show an inventory of the datastore content",
)
parser.add_argument(
    "--show-all",
    default=False,
    action=configargparse.BooleanOptionalAction,
    help="do not truncate long lists of inventory items",
)
parser.add_argument(
    "--job-name",
    default=DEFAULT_LABEL,
    help="specify a label for this job (used to identify integrations and other numerical products)",
)
parser.add_argument(
    "--shards",
    type=int,
    default=DEFAULT_SHARDS,
    help="specify number of shards to be used when creating a new datastore (if used)",
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
    "--potential-type",
    type=str,
    default="Exponential",
    choices=potential_types,
    help="specify potential type to use",
)
parser.add_argument(
    "--samples-log10-z",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_Z,
    help="specify number of z-sample points per log10(z)",
)
parser.add_argument(
    "--T-init-GeV",
    type=float,
    default=DEFAULT_T_INIT_GEV,
    help="set initial conditions at temperature T_Jordan_init, specified in GeV",
)
parser.add_argument(
    "--beta-low",
    type=float,
    default=DEFAULT_BETA_LOW,
    help="minimum value of beta to sample",
)
parser.add_argument(
    "--beta-high",
    type=float,
    default=DEFAULT_BETA_HIGH,
    help="maximum value of beta to sample",
)
parser.add_argument(
    "--samples-per-beta",
    type=int,
    default=DEFAULT_SAMPLES_PER_BETA,
    help="number of samples per beta",
)
parser.add_argument(
    "--log10-M-low-eV",
    type=float,
    default=DEFAULT_LOG10_M_LOW_EV,
    help="minimum value of log10(M/eV) to sample",
)
parser.add_argument(
    "--log10-M-high-eV",
    type=float,
    default=DEFAULT_LOG10_M_HIGH_EV,
    help="maximum value of log10(M/eV) to sample",
)
parser.add_argument(
    "--samples-per-log10-M-eV",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_M_EV,
    help="number of samples per log10(M/eV)",
)
parser.add_argument(
    "--log10-Lambda-low-eV",
    type=float,
    default=DEFAULT_LOG10_LAMBDA_LOW_EV,
    help="minimum value of log10(Lambda/eV) to sample",
)
parser.add_argument(
    "--log10-Lambda-high-eV",
    type=float,
    default=DEFAULT_LOG10_LAMBDA_HIGH_EV,
    help="maximum value of log10(Lambda/eV) to sample",
)
parser.add_argument(
    "--log10-one-plus-z-high",
    type=float,
    default=DEFAULT_LOG10_ONE_PLUS_Z_HIGH,
    help="maximum value of log10(1+z) to sample",
)
parser.add_argument(
    "--log10-one-plus-z-low",
    type=float,
    default=DEFAULT_LOG10_ONE_PLUS_Z_LOW,
    help="minimum value of log10(1+z) to sample",
)
parser.add_argument(
    "--samples-per-log10-Lambda-eV",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_LAMBDA_EV,
    help="number of samples per log10(Lambda/eV)",
)
parser.add_argument(
    "--prune-unvalidated",
    action=configargparse.BooleanOptionalAction,
    default=True,
    help="prune unvalidated data from the datastore during startup",
)
parser.add_argument(
    "--drop",
    type=str,
    nargs="+",
    default=[],
    choices=allowed_drop_actions,
    help="drop one or more data categories",
    action="extend",
)
parser.add_argument(
    "--ray-address",
    default=DEFAULT_RAY_ADDRESS,
    type=str,
    help="specify address of Ray cluster",
)
args = parser.parse_args()


if args.database is None:
    parser.print_help()
    sys.exit()

# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
ray.init(address=args.ray_address)

VERSION_LABEL = "2026.1.1"

specified_drop_actions = [x.lower() for x in args.drop]
drop_actions = [x for x in specified_drop_actions if x in allowed_drop_actions]

# instantiate a ProfileAgent to profile database operations; this is passed as an argument to ShardedPool below
profile_agent = None
if args.profile_db is not None:
    if args.job_name is not None:
        label = f'{VERSION_LABEL}-jobname-"{args.job_name}"-primarydb-"{args.database}"-shards-{args.shards}-{datetime.now().replace(microsecond=0).isoformat()}'
    else:
        label = f'{VERSION_LABEL}-primarydb-"{args.database}"-shards-{args.shards}-{datetime.now().replace(microsecond=0).isoformat()}'

    profile_agent = ProfileAgent.options(name="ProfileAgent").remote(
        db_name=args.profile_db,
        timeout=args.db_timeout,
        label=label,
    )


def run_pipeline(
    pool: ShardedPool,
    model_data: dict,
    Potential_array: List[AbstractPotential],
    Coupling_array: List[AbstractCoupling],
    T_init: temperature,
    T_stop: temperature,
    phi_init: phi_value,
    pi_init: pi_value,
    z_grid: redshift_array,
    atol: tolerance,
    rtol: tolerance,
    solvers: dict[str, IntegrationSolver],
    metadata: dict[str, Any],
):
    model_label = model_data["label"]
    model_cosmology = model_data["cosmology"]

    print(f"\n>> RUNNING PIPELINE FOR MODEL {model_label}")

    # build tags and other labels, based on these sample grids
    (
        SamplesPerLog10ZTag,  # labels number of sampled redshifts per log10 interval of 1+z in the source grid
        SamplesPerBetaTag,  # labels number of sampled beta values per log10 in beta
        SamplesPerLog10LambdaTag,  # labels number of sampled Lambda values per log10 in Lambda
        SamplesPerLog10MTag,  # labels number of sampled M values per log10 in M
    ) = ray.get(
        [
            pool.object_get(
                "store_tag", label=f"SamplesPerLog10Z_{metadata["samples_per_log10_z"]}"
            ),
            pool.object_get(
                "store_tag", label=f"SamplesPerBeta_{metadata["samples_per_beta"]}"
            ),
            pool.object_get(
                "store_tag",
                label=f"SamplesPerLog10Lambda_eV_{metadata["samples_per_log10_Lambda_eV"]}",
            ),
            pool.object_get(
                "store_tag",
                label=f"SamplesPerLog10M_eV_{metadata["samples_per_log10_M_eV"]}",
            ),
        ]
    )

    ## STEP 1
    ## BUILD SCALAR FIELD HISTORIES FOR SAMPLE GRID

    # Sharding is done on beta value, so put the coupling on the right hand side in this tensor product.
    # The intention is that each batch will have as nearly an equal distribution of beta values as we can,
    # which helps balance the load on each shard
    model_sample_grid = itertools.product(
        Potential_array,
        Coupling_array,
    )
    solver_work_batches = list(grouper(model_sample_grid, n=8, incomplete="fill"))

    def build_solver_batch(batch: List[Tuple[AbstractPotential, AbstractCoupling]]):
        # grouper may fill with None values, which we want to strip out
        batch = [x for x in batch if x is not None]

        # to allow vectorized object_get() calls to each shard, we need to bin the batch by shard
        binned_batch = {}
        for potential, coupling in batch:
            shard_key = coupling.shard_key
            binned_batch.setdefault(shard_key, []).append((potential, coupling))

        # freeze the shard keys into a well-defined order, so we know the order in which they are returned
        batch_keys = list(binned_batch.keys())

        # query whether a stored result exists for all potential/coupling combinations
        # ScalarModel is a sharded table and needs a "shard_key" field
        # TODO: find a better way to implement/handle
        query_batch = [
            {
                "shard_key": key,
                "payload": [
                    {
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
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        "_do_not_populate": True,
                    }
                    for potential, coupling in binned_batch[key]
                ],
            }
            for key in batch_keys
        ]

        query_queue = RayWorkPool(
            pool,
            query_batch,
            task_builder=lambda x: pool.object_get_vectorized(
                "ScalarModel", x["shard_key"], payload_data=x["payload"]
            ),
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
        query_queue.run()

        # which models are missing?
        missing = [
            {
                "shard_key": key,
                "missing": [m for obj, m in zip(query_outcomes, binned_batch[key])],
            }
            for key, query_outcomes in zip(batch_keys, query_queue.results)
        ]

        num_missing = sum(len(x["missing"]) for x in missing)
        if num_missing == 0:
            return []

        work_refs = []

        for key, data in zip(batch_keys, missing):
            work_refs.extend(
                [
                    pool.object_get(
                        "ScalarModel",
                        shard_key=coupling.shard_key,
                        solver_labels=solvers,
                        cosmology=model_cosmology,
                        T_Jordan_init=T_init,
                        T_Jordan_stop=T_stop,
                        phi_Einstein_init=phi_init,
                        pi_Einstein_init=pi_init,
                        potential=potential,
                        coupling=coupling,
                        z_grid=z_grid,
                        atol=atol,
                        rtol=rtol,
                        tags=[
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        _do_not_populate=True,  # ignored if object does not already exist in database, so does not spoil work scheduling
                    )
                    for potential, coupling in data["missing"]
                ]
            )

        return work_refs

    def build_solver_batch_label(m: ScalarModel):
        potential: AbstractPotential = m.potential
        coupling: AbstractCoupling = m.coupling
        return f"{args.job_name}-ScalarModel-{potential.name}-{coupling.name}-{datetime.now().replace(microsecond=0).isoformat()}"

    def compute_solver_batch(m: ScalarModel, label: str):
        return m.compute(label=label)

    def validate_solver_batch(m: ScalarModel):
        if not m.available:
            raise RuntimeError(
                "ScalarModel object passed for validation, but is not yet available"
            )

        return pool.object_validate(m)

    solver_queue = RayWorkPool(
        pool,
        solver_work_batches,
        task_builder=build_solver_batch,
        compute_handler=compute_solver_batch,
        validation_handler=validate_solver_batch,
        label_builder=build_solver_batch_label,
        title="CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID",
        store_results=False,
        create_batch_size=2,
        notify_batch_size=2,
        max_task_queue=24,
        process_batch_size=3,
        notify_min_time_interval=MIN_NOTIFY_INTERVAL,
    )
    solver_queue.run()

    ## STEP 2
    ## CALCULATE THE ADIABATIC TRANSGRESSION PARAMETER Q FOR EACH MODEL IN THE GRID
    adiabatic_sample_grid = itertools.product(
        Potential_array,
        Coupling_array,
    )
    adiabatic_work_batches = list(
        grouper(adiabatic_sample_grid, n=8, incomplete="fill")
    )

    def build_adiabatic_batch(batch: List[Tuple[AbstractPotential, AbstractCoupling]]):
        # grouper may fill with None values which must be filtered out
        batch = [x for x in batch if x is not None]

        # STEP 1. PULL MODEL INSTANCES FROM THE DATABASE

        # to allow vectorized object_get() calls to each shard, we need to bin the batch by shard
        binned_batch = {}
        for potential, coupling in batch:
            shard_key = coupling.shard_key
            binned_batch.setdefault(shard_key, []).append((potential, coupling))

        # freeze the shard keys into a well-defined order, so we know the order in which they are returned
        batch_keys = list(binned_batch.keys())

        # find which instances are missing
        model_query_batch = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "solver_labels": [],
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
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        "_do_not_populate": True,
                    }
                    for potential, coupling in binned_batch[key]
                ],
            }
            for key in batch_keys
        ]

        model_query_queue = RayWorkPool(
            pool,
            model_query_batch,
            task_builder=lambda x: pool.object_get_vectorized(
                "ScalarModel", x["shard_key"], payload_data=x["payload"]
            ),
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

        missing_models = [
            {
                "shard_key": key,
                "missing": [
                    m
                    for obj, m in zip(query_outcomes, binned_batch[key])
                    if not obj.available
                ],
            }
            for key, query_outcomes in zip(batch_keys, model_query_queue.results)
        ]
        num_missing_models = sum(len(x["missing"]) for x in missing_models)
        if num_missing_models > 0:
            raise RuntimeError(
                f"Some ScalarModel instances needed for AdiabaticHistory computation are missing ({num_missing_models} missing in this batch)"
            )

        adiabatic_query_batch = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "model_proxy": ScalarModelProxy(obj),
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        "_do_not_populate": True,
                    }
                    for obj in query_outcomes
                    if not obj.failure
                ],
            }
            for key, query_outcomes in zip(batch_keys, model_query_queue.results)
        ]

        adiabatic_query_queue = RayWorkPool(
            pool,
            adiabatic_query_batch,
            task_builder=lambda x: pool.object_get_vectorized(
                "AdiabaticHistory", x["shard_key"], payload_data=x["payload"]
            ),
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

        missing_adiabatic = [
            {
                "shard_key": key,
                "missing": [
                    (potential, coupling)
                    for obj, (potential, coupling) in zip(
                        query_outcomes, binned_batch[key]
                    )
                    if not obj.available
                ],
            }
            for key, query_outcomes in zip(batch_keys, adiabatic_query_queue.results)
        ]

        num_missing = sum(len(x["missing"]) for x in missing_adiabatic)
        if num_missing == 0:
            return []

        # now we need to re-lookup ScalarModel insatnces for the missing elements, this time
        # *not* with _do_not_populate
        required_models_payload = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "solver_labels": [],
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
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                    }
                    for potential, coupling in x["missing"]
                ],
            }
            for key, x in zip(batch_keys, missing_adiabatic)
        ]

        required_models_queue = RayWorkPool(
            pool,
            required_models_payload,
            task_builder=lambda x: pool.object_get_vectorized(
                "ScalarModel", x["shard_key"], payload_data=x["payload"]
            ),
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
        required_models_queue.run()
        required_models_proxies = [
            {
                "shard_key": key,
                "missing_proxies": [ScalarModelProxy(obj) for obj in lookup_data],
            }
            for key, lookup_data in zip(batch_keys, required_models_queue.results)
        ]

        work_refs = []

        for key, proxy_data in zip(batch_keys, required_models_proxies):
            work_refs.extend(
                [
                    pool.object_get(
                        "AdiabaticHistory",
                        shard_key=key,
                        model_proxy=proxy,
                        tags=[
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                    )
                    for proxy in proxy_data["missing_proxies"]
                ]
            )

        return work_refs

    def build_adiabatic_batch_label(q: AdiabaticHistory):
        potential: AbstractPotential = q.potential
        coupling: AbstractCoupling = q.coupling
        return f"{args.job_name}-AdiabaticHistory-{potential.name}-{coupling.name}-{datetime.now().replace(microsecond=0).isoformat()}"

    def compute_adiabatic_batch(q: AdiabaticHistory, label: str):
        return q.compute(label=label)

    def validate_adiabatic_batch(q: AdiabaticHistory):
        if not q.available:
            raise RuntimeError(
                "AdiabaticHistory object passed for validation, but is not yet available"
            )

        return pool.object_validate(q)

    adiabatic_queue = RayWorkPool(
        pool,
        adiabatic_work_batches,
        task_builder=build_adiabatic_batch,
        compute_handler=compute_adiabatic_batch,
        validation_handler=validate_adiabatic_batch,
        label_builder=build_adiabatic_batch_label,
        title="CALCULATE ADIABATIC TRANSGRESSION PARAMETERS",
        store_results=False,
        create_batch_size=2,
        notify_batch_size=2,
        max_task_queue=24,
        process_batch_size=3,
        notify_min_time_interval=MIN_NOTIFY_INTERVAL,
    )
    adiabatic_queue.run()

    ## STEP 3
    ## COMPUTE BBN DATA FOR EACG MODEL IN THE GRID
    BBN_sample_grid = itertools.product(
        Potential_array,
        Coupling_array,
    )

    bbn_data_work_batches = list(grouper(BBN_sample_grid, n=8, incomplete="fill"))

    def build_bbn_data_batch(batch: List[Tuple[AbstractPotential, AbstractCoupling]]):
        # grouper may fill with None values which must be filtered out
        batch = [x for x in batch if x is not None]

        # STEP 1. PULL MODEL INSTANCES FROM THE DATABASE

        # to allow vectorized object_get() calls to each shard, we need to bin the batch by shard
        binned_batch = {}
        for potential, coupling in batch:
            shard_key = coupling.shard_key
            binned_batch.setdefault(shard_key, []).append((potential, coupling))

        # freeze the shard keys into a well-defined order, so we know the order in which they are returned
        batch_keys = list(binned_batch.keys())

        # find which instances are missing
        model_query_batch = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "solver_labels": [],
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
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        "_do_not_populate": True,
                    }
                    for potential, coupling in binned_batch[key]
                ],
            }
            for key in batch_keys
        ]

        model_query_queue = RayWorkPool(
            pool,
            model_query_batch,
            task_builder=lambda x: pool.object_get_vectorized(
                "ScalarModel", x["shard_key"], payload_data=x["payload"]
            ),
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

        missing_models = [
            {
                "shard_key": key,
                "missing": [
                    m
                    for obj, m in zip(query_outcomes, binned_batch[key])
                    if not obj.available
                ],
            }
            for key, query_outcomes in zip(batch_keys, model_query_queue.results)
        ]
        num_missing_models = sum(len(x["missing"]) for x in missing_models)
        if num_missing_models > 0:
            raise RuntimeError(
                f"Some ScalarModel instances needed for BBN computations are missing ({num_missing_models} missing in this batch)"
            )

        bbn_query_batch = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "model_proxy": ScalarModelProxy(obj),
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                        "_do_not_populate": True,
                    }
                    for obj in query_outcomes
                    if not obj.failure
                ],
            }
            for key, query_outcomes in zip(batch_keys, model_query_queue.results)
        ]

        bbn_query_queue = RayWorkPool(
            pool,
            bbn_query_batch,
            task_builder=lambda x: pool.object_get_vectorized(
                "BBNData", x["shard_key"], payload_data=x["payload"]
            ),
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

        missing_bbn = [
            {
                "shard_key": key,
                "missing": [
                    (potential, coupling)
                    for obj, (potential, coupling) in zip(
                        query_outcomes, binned_batch[key]
                    )
                    if not obj.available
                ],
            }
            for key, query_outcomes in zip(batch_keys, bbn_query_queue.results)
        ]

        num_missing = sum(len(x["missing"]) for x in missing_bbn)
        if num_missing == 0:
            return []

        # now we need to re-lookup ScalarModel instances for the missing elements, this time
        # *not* with _do_not_populate
        required_models_payload = [
            {
                "shard_key": key,
                "payload": [
                    {
                        "solver_labels": [],
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
                        "tags": [
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                    }
                    for potential, coupling in x["missing"]
                ],
            }
            for key, x in zip(batch_keys, missing_bbn)
        ]

        required_models_queue = RayWorkPool(
            pool,
            required_models_payload,
            task_builder=lambda x: pool.object_get_vectorized(
                "ScalarModel", x["shard_key"], payload_data=x["payload"]
            ),
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
        required_models_queue.run()
        required_models_proxies = [
            {
                "shard_key": key,
                "missing_proxies": [ScalarModelProxy(obj) for obj in lookup_data],
            }
            for key, lookup_data in zip(batch_keys, required_models_queue.results)
        ]

        work_refs = []

        for key, proxy_data in zip(batch_keys, required_models_proxies):
            work_refs.extend(
                [
                    pool.object_get(
                        "BBNData",
                        shard_key=key,
                        model_proxy=proxy,
                        tags=[
                            SamplesPerLog10ZTag,
                            SamplesPerBetaTag,
                            SamplesPerLog10MTag,
                            SamplesPerLog10LambdaTag,
                        ],
                    )
                    for proxy in proxy_data["missing_proxies"]
                ]
            )

        return work_refs

    def build_bbn_data_batch_label(q: BBNData):
        potential: AbstractPotential = q.potential
        coupling: AbstractCoupling = q.coupling
        return f"{args.job_name}-BBNData-{potential.name}-{coupling.name}-{datetime.now().replace(microsecond=0).isoformat()}"

    def compute_bbn_data_batch(data: BBNData, label: str):
        return data.compute(label=label, payload={"small_network": True})

    def validate_bbn_data_batch(q: BBNData):
        if not q.available:
            raise RuntimeError(
                "BBNData object passed for validation, but is not yet available"
            )

        return pool.object_validate(q)

    bbn_data_queue = RayWorkPool(
        pool,
        bbn_data_work_batches,
        task_builder=build_bbn_data_batch,
        compute_handler=compute_bbn_data_batch,
        validation_handler=validate_bbn_data_batch,
        label_builder=build_bbn_data_batch_label,
        title="CALCULATE BBN DATA",
        store_results=False,
        create_batch_size=2,
        notify_batch_size=2,
        max_task_queue=24,
        process_batch_size=3,
        notify_min_time_interval=MIN_NOTIFY_INTERVAL,
    )
    bbn_data_queue.run()


def execute(pool, units: UnitsLike):
    log10_one_plus_z_low = args.log10_one_plus_z_low
    log10_one_plus_z_high = args.log10_one_plus_z_high
    samples_per_log10_z: int = args.samples_log10_z

    beta_low: float = args.beta_low
    beta_high: float = args.beta_high
    samples_per_beta: int = args.samples_per_beta

    log10_M_low_eV: float = args.log10_M_low_eV
    log10_M_high_eV: float = args.log10_M_high_eV
    samples_per_log10_M_eV: int = args.samples_per_log10_M_eV

    log10_Lambda_low_eV: float = args.log10_Lambda_low_eV
    log10_Lambda_high_eV: float = args.log10_Lambda_high_eV
    samples_per_log10_Lambda_eV: int = args.samples_per_log10_Lambda_eV

    T_init_GeV: float = args.T_init_GeV

    T_init = ray.get(
        pool.object_get("temperature", value=T_init_GeV * units.GeV, units=units)
    )

    # pick initial conditions that are equivalent to Xav's
    phi_init, pi_init = ray.get(
        [
            pool.object_get("phi_value", value=5.0 * units.PlanckMass, units=units),
            pool.object_get("pi_value", value=0.0, units=units),
        ]
    )

    def convert_to_redshift(z_array):
        return pool.object_get(
            "redshift",
            payload_data=[{"z": z} for z in z_array],
        )

    def convert_to_betas(beta_sample_set):
        return pool.object_get(
            "beta_value",
            payload_data=[{"value": beta} for beta in beta_sample_set],
        )

    def convert_to_Ms(M_sample_set):
        return pool.object_get(
            "M_value",
            payload_data=[{"value": M, "units": units} for M in M_sample_set],
        )

    def convert_to_Lambdas(lambda_sample_set):
        return pool.object_get(
            "Lambda_value",
            payload_data=[
                {"value": Lambda, "units": units} for Lambda in lambda_sample_set
            ],
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

    ## DATASTORE OBJECTS

    # build absolute and relative tolerances
    atol, rtol = ray.get(
        [
            pool.object_get("tolerance", tol=DEFAULT_ABS_TOLERANCE),
            pool.object_get("tolerance", tol=DEFAULT_REL_TOLERANCE),
        ]
    )

    # build stepper labels; we have to query these up-front from the pool in order to be
    # certain that they get the same serial number in each database shard.
    # So we can no longer construct these on-the-fly in the integration classes, as used to be done
    (
        solve_ivp_RK45,
        solve_ivp_DOP853,
        solve_ivp_Radau,
        solve_ivp_BDF,
        solve_icp_LSODA,
    ) = ray.get(
        [
            pool.object_get("IntegrationSolver", label="solve_ivp+RK45", stepping=0),
            pool.object_get("IntegrationSolver", label="solve_ivp+DOP853", stepping=0),
            pool.object_get("IntegrationSolver", label="solve_ivp+Radau", stepping=0),
            pool.object_get("IntegrationSolver", label="solve_ivp+BDF", stepping=0),
            pool.object_get("IntegrationSolver", label="solve_ivp+LSODA", stepping=0),
        ]
    )
    solvers = {
        "solve_ivp+RK45-stepping0": solve_ivp_RK45,
        "solve_ivp+DOP853-stepping0": solve_ivp_DOP853,
        "solve_ivp+Radau-stepping0": solve_ivp_Radau,
        "solve_ivp+BDF-stepping0": solve_ivp_BDF,
        "solve_ivp+LSODA-stepping0": solve_icp_LSODA,
    }

    # the redshift z corresponding to T = 20,000 GeV is about 6E35 in a LambdaCDM-like cosmology
    # we set up a redshift sampling grid that covers this range
    # we use this grid to store the scalar field histories
    num_z_samples = int(
        round(
            samples_per_log10_z * (log10_one_plus_z_high - log10_one_plus_z_low) + 0.5,
            0,
        )
    )
    z_array = ray.get(
        convert_to_redshift(
            np.logspace(log10_one_plus_z_low, log10_one_plus_z_high, num_z_samples)
            - 1.0,
        )
    )
    z_grid = redshift_array(z_array=z_array)

    ## STEP 1
    ## BUILD A GRID OF beta, M, Lambda VALUES AT WHICH TO SAMPLE, AND USE THIS
    ## TO BUILD A GRID OF POTENTIAL AND COUPLING FUNCTIONS

    print("\n** BUILDING GRID OF MODELS TO SAMPLE")
    print(f'   -- using potential type "{args.potential_type}"')

    num_beta_sample = int(round(samples_per_beta * (beta_high - beta_low) + 0.5, 0))

    beta_blacklist = []

    def beta_blacklisted(beta: float) -> bool:
        if (
            len(
                [
                    b
                    for b in beta_blacklist
                    if fabs((beta - b) / b) < DEFAULT_FLOAT_PRECISION
                ]
            )
            > 0
        ):
            print(
                f"   @@ Note: beta = {beta:.8g} is blacklisted and has been removed from the sample"
            )
            return True

        return False

    beta_pre_sample = np.linspace(beta_low, beta_high, num_beta_sample, endpoint=True)
    # beta_pre_sample = [0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    beta_sample = [beta for beta in beta_pre_sample if not beta_blacklisted(beta)]

    beta_array = ray.get(convert_to_betas(beta_sample))
    beta_grid = DimensionlessQuantityArray(value_array=beta_array)
    print(f"   -- populated beta sample grid with {len(beta_sample)} values")

    # num_M_sample = int(
    #     round(samples_per_log10_M_eV * (log10_M_high_eV - log10_M_low_eV) + 0.5, 0)
    # )
    #
    # M_array = ray.get(
    #     convert_to_Ms(
    #         np.logspace(log10_M_low_eV, log10_M_high_eV, num_M_sample, endpoint=True)
    #         * units.eV
    #     )
    # )
    M_array = ray.get(convert_to_Ms([0.5 * units.PlanckMass]))
    M_grid = DimensionfulQuantityArray(value_array=M_array)
    print(f"   -- populated M sample grid with {len(M_array)} values")

    # num_Lambda_sample = int(
    #     round(
    #         samples_per_log10_Lambda_eV * (log10_Lambda_high_eV - log10_Lambda_low_eV)
    #         + 0.5,
    #         0,
    #     )
    # )
    #
    # Lambda_array = ray.get(
    #     convert_to_Lambdas(
    #         np.logspace(
    #             log10_Lambda_low_eV,
    #             log10_Lambda_high_eV,
    #             num_Lambda_sample,
    #             endpoint=True,
    #         )
    #         * units.eV
    #     )
    # )
    Lambda_array = ray.get(convert_to_Lambdas([1e-3 * units.eV]))
    Lambda_grid = DimensionfulQuantityArray(value_array=Lambda_array)
    print(f"   -- populated Lambda sample grid with {len(Lambda_array)} values")

    M_lambda_grid = itertools.product(M_grid, Lambda_grid)
    Potential_array = ray.get(convert_to_potential(M_lambda_grid))

    Coupling_array = ray.get(convert_to_coupling(beta_grid))

    print(
        f"   -- total number of models to integrate: {len(Potential_array) * len(Coupling_array)}"
    )

    model_list = build_model_list(pool, units)
    for model_data in model_list:
        cosmology: BaseCosmology = model_data["cosmology"]

        T_CMB = cosmology._params.T_CMB_Kelvin * units.Kelvin
        # T_CMB = 50 * units.keV
        T_stop = ray.get(pool.object_get("temperature", value=T_CMB, units=units))

        run_pipeline(
            pool,
            model_data,
            Potential_array,
            Coupling_array,
            T_init,
            T_stop,
            phi_init,
            pi_init,
            z_grid,
            atol,
            rtol,
            solvers,
            {
                "samples_per_log10_z": samples_per_log10_z,
                "samples_per_beta": samples_per_beta,
                "samples_per_log10_M_eV": samples_per_log10_M_eV,
                "samples_per_log10_Lambda_eV": samples_per_log10_Lambda_eV,
            },
        )


def redshift_inventory(pool: ShardedPool):
    print("\n   -- Redshifts")
    data = pool.inventory("redshift")
    sorted_values = sorted(data["values"])
    num = len(sorted_values)

    if num == 0:
        print(f"      no values committed")
    elif num == 1:
        print(
            f'      1 value committed at {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )
    else:
        print(
            f'      {num} values committed between {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")} and {data["latest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )

    if num < 20:
        formatted_values = [f"{v:.5g}" for v in sorted_values]
    else:
        low_formatted_values = [f"{v:.5g}" for v in sorted_values[:10]]
        high_formatted_values = [f"{v:.5g}" for v in sorted_values[-10:]]
        formatted_values = low_formatted_values + ["..."] + high_formatted_values

    print(f'      values = [ {", ".join(formatted_values)} ]')


def dimensionless_value_inventory(pool: ShardedPool, type: str, label: str):
    print(f"\n   -- {label}")
    data = pool.inventory(type)
    sorted_values = sorted(data["values"])
    num = len(sorted_values)

    if num == 0:
        print(f"      no values committed")
    elif num == 1:
        print(
            f'      1 value committed at {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )
    else:
        print(
            f'      {num} values committed between {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")} and {data["latest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )

    if num < 20:
        formatted_values = [f"{v:.5g}" for v in sorted_values]
    else:
        low_formatted_values = [f"{v:.5g}" for v in sorted_values[:10]]
        high_formatted_values = [f"{v:.5g}" for v in sorted_values[-10:]]
        formatted_values = low_formatted_values + ["..."] + high_formatted_values

    print(f'      values = [ {", ".join(formatted_values)} ]')


def dimensionful_value_inventory(
    pool: ShardedPool, type: str, label: str, units: UnitsLike
):
    print(f"\n   -- {label}")
    data = pool.inventory(type, units)
    sorted_values = sorted(data["values"])
    num = len(sorted_values)

    if num == 0:
        print(f"      no values committed")
    elif num == 1:
        print(
            f'      1 value committed at {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )
    else:
        print(
            f'      {num} values committed between {data["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")} and {data["latest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
        )

    unit = data["unit"]
    unit_value = getattr(units, unit)

    if num < 20:
        formatted_values = [f"{v/unit_value:.5g} {unit}" for v in sorted_values]
    else:
        low_formatted_values = [
            f"{v/unit_value:.5g} {unit}" for v in sorted_values[:10]
        ]
        high_formatted_values = [
            f"{v/unit_value:.5g} {unit}" for v in sorted_values[-10:]
        ]
        formatted_values = low_formatted_values + ["..."] + high_formatted_values

    print(f'      values = [ {", ".join(formatted_values)} ]')


def object_inventory(pool: ShardedPool, cls, label):
    print(f"\n   -- {label}")
    data = pool.inventory(cls)

    def print_data(group):
        sorted_labels = sorted(group["labels"])
        num = len(sorted_labels)

        versions = group["versions"]
        sorted_versions = sorted(versions)
        num_versions = len(versions)

        if num == 0:
            print(f"        no values committed")
        elif num == 1:
            print(
                f'        1 value committed at {group["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}, version = {versions.pop()}'
            )
        else:
            print(
                f'        {num} values committed between {group["earliest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")} and {group["latest_timestamp"].strftime("%a %d %b %Y %H:%M:%S")}'
            )
            if num_versions == 1:
                print(f"        version = {versions.pop()}")
            else:
                print(f'        versions = [ {", ".join(sorted_versions)} ]')

        if num < 20 or args.show_all:
            for value in sorted_labels:
                print(f"        :: {value}")

        else:
            for value in sorted_labels[:10]:
                print(f"        :: {value}")
            print(f"      ...")
            for value in sorted_labels[-10:]:
                print(f"        :: {value}")

    print(f"      @@ validated models")
    print_data(data["validated"])
    print(f"      @@ unvalidated models")
    print_data(data["unvalidated"])


def inventory(pool: ShardedPool, units: UnitsLike):
    print("\n@@ DATASTORE INVENTORY")

    redshift_inventory(pool)

    dimensionless_value_inventory(pool, beta_value, "Beta Values")
    dimensionful_value_inventory(pool, temperature, "Temperature Values", units)
    dimensionful_value_inventory(pool, M_value, "M Values", units)
    dimensionful_value_inventory(pool, Lambda_value, "Lambda Values", units)
    dimensionful_value_inventory(pool, phi_value, "Phi Values", units)
    dimensionful_value_inventory(pool, pi_value, "Pi Values", units)

    object_inventory(pool, "ScalarModel", "Scalar Models")
    object_inventory(pool, "AdiabaticHistory", "Adiabatic Histories")
    object_inventory(pool, "BBNData", "BBN Data")


# construct a ShardedPool to orchestrate database access
with ShardedPool(
    version_label=VERSION_LABEL,
    db_name=args.database,
    ShardKeyType=ShardKeyType,
    ShardKeyStoreIdGetter=get_shard_key_store_id,
    replicated_tables=replicated_tables,
    sharded_tables=sharded_tables,
    timeout=args.db_timeout,
    shards=args.shards,
    profile_agent=profile_agent,
    job_name=args.job_name,
    prune_unvalidated=args.prune_unvalidated,
    drop_actions=drop_actions,
    read_table_config=read_table_config,
    inventory_config=inventory_config,
) as pool:

    units = Planck_units()

    if args.inventory:
        inventory(pool, units)

    else:
        execute(pool, units)
