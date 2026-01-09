import argparse
import itertools
import sys
from datetime import datetime
from typing import List, Tuple

import numpy as np
import ray

from ComputeTargets import ScalarModel
from CosmologyConcepts import (
    DimensionlessQuantityArray,
    DimensionfulQuantityArray,
    temperature,
    redshift_array,
    phi_value,
    pi_value,
)
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore.SQL.ObjectFactories import tolerance
from Datastore.SQL.ProfileAgent import ProfileAgent
from Datastore.SQL.ShardedPool import ShardedPool
from Quadrature.integration_metadata import IntegrationSolver
from RayTools.RayWorkPool import RayWorkPool
from Units import GeV_units
from config.defaults import (
    DEFAULT_ABS_TOLERANCE,
    DEFAULT_REL_TOLERANCE,
    DEFAULT_QUADRATURE_ATOL,
    DEFAULT_QUADRATURE_RTOL,
)
from config.model_list import build_model_list
from config.sharding import (
    replicated_tables,
    sharded_tables,
    get_shard_key_store_id,
    shard_key_type,
    read_table_config,
)
from utilities import grouper

DEFAULT_LABEL = "ChamPBH-test"
DEFAULT_TIMEOUT = 60
DEFAULT_SHARDS = 20
DEFAULT_RAY_ADDRESS = "auto"

DEFAULT_Z_END = 0.1
DEFAULT_T_HIGH_GEV = 20000
DEFAULT_SAMPLES_PER_LOG10_Z = 100

DEFAULT_BETA_LOW = 0.1
DEFAULT_BETA_HIGH = 3.0
DEFAULT_SAMPLES_PER_BETA = 10

DEFAULT_LOG10_M_LOW_EV = -33
DEFAULT_LOG10_M_HIGH_EV = -25
DEFAULT_SAMPLES_PER_LOG10_M_EV = 50

DEFAULT_LOG10_LAMBDA_LOW_EV = -33
DEFAULT_LOG10_LAMBDA_HIGH_EV = -25
DEFAULT_SAMPLES_PER_LOG10_LAMBDA_EV = 50

MIN_NOTIFY_INTERVAL = 5 * 60

allowed_drop_actions = []

parser = argparse.ArgumentParser()
parser.add_argument(
    "--database",
    type=str,
    default=None,
    help="read/write work items using the specified database cache",
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
    "--samples-log10-z",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_Z,
    help="specify number of z-sample points per log10(z)",
)
parser.add_argument(
    "--T-high-GeV",
    type=float,
    default=DEFAULT_T_HIGH_GEV,
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
    "--samples-per-log10-Lambda-eV",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_LAMBDA_EV,
    help="number of samples per log10(Lambda/eV)",
)
parser.add_argument(
    "--prune-unvalidated",
    action=argparse.BooleanOptionalAction,
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
                "store_tag", label=f"SamplesPerLog10Z_{samples_per_log10_z}"
            ),
            pool.object_get("store_tag", label=f"SamplesPerBeta_{samples_per_beta}"),
            pool.object_get(
                "store_tag",
                label=f"SamplesPerLog10Lambda_eV_{samples_per_log10_Lambda_eV}",
            ),
            pool.object_get(
                "store_tag", label=f"SamplesPerLog10M_eV_{samples_per_log10_M_eV}"
            ),
        ]
    )

    ## STEP 1
    ## BAKE THE BACKGROUND COSMOLOGY INTO A BACKGROUND MODEL OBJECT

    # sharding is done on M value, so put it on the right hand side
    # this means that each batch will have as nearly an equal distribution of M values as we can,
    # which helps balance the load on each shard
    solver_work_items = itertools.product(
        Potential_array,
        Coupling_array,
    )
    solver_work_batches = list(grouper(solver_work_items, n=50, incomplete="fill"))

    def build_solver_work(batch: List[Tuple[AbstractPotential, AbstractCoupling]]):
        # grouper may fill with None values, which we want to strip out
        batch = [x for x in batch if x is not None]

        # query whether a stored result exists for all potential/coupling combinations
        # ScalarModel is a sharded table and needs a "shard_key" field
        # TODO: find a better way to implement/handle
        query_batch = [
            {
                "shard_key": potential.shard_key,
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
            for potential, coupling in batch
        ]

        query_queue = RayWorkPool(
            pool,
            query_batch,
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
        query_queue.run()

        # which models are missing?
        missing = [m for obj, m in zip(query_queue.results, batch) if not obj.available]

        if len(missing) == 0:
            return []

        work_refs = []

        for potential, coupling in missing:
            work_refs.append(
                pool.object_get(
                    "ScalarModel",
                    shard_key=potential.shard_key,
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
            )

        return work_refs

    def build_solver_work_label(m: ScalarModel):
        potential: AbstractPotential = m.potential
        coupling: AbstractCoupling = m.coupling
        return f"{args.job_name}-ScalarModel-{potential.name}-{coupling.name}-{datetime.now().replace(microsecond=0).isoformat()}"

    def compute_solver_work(m: ScalarModel, label: str):
        return m.compute(label=label)

    def validate_solver_work(m: ScalarModel):
        if not m.available:
            raise RuntimeError(
                "ScalarModel object passed for validation, but is not yet available"
            )

        return pool.object_validate(m)

    solver_queue = RayWorkPool(
        pool,
        solver_work_batches,
        task_builder=build_solver_work,
        compute_handler=compute_solver_work,
        validation_handler=validate_solver_work,
        label_builder=build_solver_work_label,
        title="CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID",
        store_results=False,
        create_batch_size=10,
        notify_batch_size=20,
        max_task_queue=20,
        process_batch_size=10,
        notify_min_time_interval=MIN_NOTIFY_INTERVAL,
    )
    solver_queue.run()


# construct a ShardedPool to orchestrate database access
with ShardedPool(
    version_label=VERSION_LABEL,
    db_name=args.database,
    ShardKeyType=shard_key_type,
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
) as pool:

    # set up LambdaCDM object representing a basic Planck2018 cosmology in Mpc units

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

    T_init_GeV: float = args.T_high_GeV

    units = GeV_units()

    T_init = ray.get(
        pool.object_get("temperature", value=T_init_GeV * units.GeV, units=units)
    )

    # think Xav is using phi_init=5 Mp, picking a slightly different comparison to check stability of evolutions
    phi_init, pi_init = ray.get(
        [
            pool.object_get("phi_value", value=7.0 * units.PlanckMass, units=units),
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
        # InversePowerPotential is a sharded table and needs a "shard_key" field
        # TODO: find a better way to implement/handle
        return pool.object_get(
            "InversePowerPotential",
            payload_data=[
                {"shard_key": M, "M": M, "Lambda": Lambda, "n": 1, "units": units}
                for M, Lambda in M_lambda_set
            ],
        )

    def convert_to_coupling(beta_set):
        return pool.object_get(
            "ExponentialCoupling",
            payload_data=[{"beta": beta, "units": units} for beta in beta_set],
        )

    ## DATASTORE OBJECTS

    # build absolute and relative tolerances
    atol, rtol, quad_atol, quad_rtol = ray.get(
        [
            pool.object_get("tolerance", tol=DEFAULT_ABS_TOLERANCE),
            pool.object_get("tolerance", tol=DEFAULT_REL_TOLERANCE),
            pool.object_get("tolerance", tol=DEFAULT_QUADRATURE_ATOL),
            pool.object_get("tolerance", tol=DEFAULT_QUADRATURE_RTOL),
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
    log10_one_plus_z_high = 36
    log10_one_plus_z_low = 0
    num_z_samples = samples_per_log10_z * (log10_one_plus_z_high - log10_one_plus_z_low)
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

    num_beta_sample = int(round(samples_per_beta * (beta_high - beta_low) + 0.5, 0))

    beta_array = ray.get(
        convert_to_betas(
            np.linspace(beta_low, beta_high, num_beta_sample, endpoint=True)
        )
    )
    beta_grid = DimensionlessQuantityArray(value_array=beta_array)

    num_M_sample = int(
        round(samples_per_log10_M_eV * (log10_M_high_eV - log10_M_low_eV) + 0.5, 0)
    )

    M_array = ray.get(
        convert_to_Ms(
            np.logspace(log10_M_low_eV, log10_M_high_eV, num_M_sample, endpoint=True)
            * units.eV
        )
    )
    M_grid = DimensionfulQuantityArray(value_array=M_array)

    num_Lambda_sample = int(
        round(
            samples_per_log10_Lambda_eV * (log10_Lambda_high_eV - log10_Lambda_low_eV)
            + 0.5,
            0,
        )
    )

    Lambda_array = ray.get(
        convert_to_Lambdas(
            np.logspace(
                log10_Lambda_low_eV,
                log10_Lambda_high_eV,
                num_Lambda_sample,
                endpoint=True,
            )
            * units.eV
        )
    )
    Lambda_grid = DimensionfulQuantityArray(value_array=Lambda_array)

    M_lambda_grid = itertools.product(M_grid, Lambda_grid)
    Potential_array = ray.get(convert_to_potential(M_lambda_grid))

    Coupling_array = ray.get(convert_to_coupling(beta_grid))

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
            z_grid,
            atol,
            rtol,
            solvers,
        )
