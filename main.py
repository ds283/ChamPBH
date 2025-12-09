import argparse
import datetime
import sys
from math import log10

import numpy as np
import ray

from ComputeTargets import BackgroundModel, ModelProxy
from CosmologyConcepts import redshift_array
from Datastore.SQL import ProfileAgent, Datastore
from Datastore.SQL.ObjectFactories import tolerance
from Quadrature.integration_metadata import IntegrationSolver
from Units import Mpc_units
from defaults import (
    DEFAULT_ABS_TOLERANCE,
    DEFAULT_REL_TOLERANCE,
    DEFAULT_QUADRATURE_ATOL,
    DEFAULT_QUADRATURE_RTOL,
)
from model_list import build_model_list
from utilities import WallclockTimer, format_time

DEFAULT_LABEL = "ChamPBH-test"
DEFAULT_TIMEOUT = 60
DEFAULT_SHARDS = 20
DEFAULT_RAY_ADDRESS = "auto"
DEFAULT_SAMPLES_PER_LOG10_Z = 100
DEFAULT_ZEND = 0.1

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
    "--samples-log10z",
    type=int,
    default=DEFAULT_SAMPLES_PER_LOG10_Z,
    help="specify number of z-sample points per log10(z)",
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

VERSION_LABEL = "2025.1.1"

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
    z_sample: redshift_array,
    atol: tolerance,
    rtol: tolerance,
    solvers: dict[str, IntegrationSolver],
):
    model_label = model_data["label"]
    model_cosmology = model_data["cosmology"]

    print(f"\n>> RUNNING PIPELINE FOR MODEL {model_label}")

    # build tags and other labels, based on these sample grids
    (
        ZGridSizeTag,  # labels size of the z sample grid
        LargestZTag,  # labels largest z in the global grid
        SmallestZTag,  # labels smallest z in the global grid
        SamplesPerLog10ZTag,  # labels number of redshifts per log10 interval of 1+z in the source grid
    ) = ray.get(
        [
            pool.object_get(
                "store_tag", label=f"RedshiftGrid_{len(z_sample)}"
            ),
            pool.object_get(
                "store_tag", label=f"LargestSourceRedshift_{z_sample.max.z:.5g}"
            ),
            pool.object_get(
                "store_tag", label=f"SmallestSourceRedshift_{z_sample.min.z:.5g}"
            ),
            pool.object_get(
                "store_tag", label=f"SourceSamplesPerLog10Z_{samples_per_log10z}"
            ),
        ]
    )

    ## STEP 1
    ## BAKE THE BACKGROUND COSMOLOGY INTO A BACKGROUND MODEL OBJECT

    bg_model: BackgroundModel = ray.get(
        pool.object_get(
            "BackgroundModel",
            solver_labels=solvers,
            cosmology=model_cosmology,
            z_sample=z_sample,
            atol=atol,
            rtol=rtol,
            tags=[ZGridSizeTag, LargestZTag, SmallestZTag, SamplesPerLog10ZTag],
        )
    )
    if not bg_model.available:
        print(f"\n** CALCULATING BACKGROUND {model_label} MODEL")
        with WallclockTimer() as timer:
            data = ray.get(bg_model.compute(label=model_cosmology.name))
            bg_model.store()
            bg_model = ray.get(pool.object_store(bg_model))
            outcome = ray.get(pool.object_validate(bg_model))
        print(
            f"   @@ computed and stored new background solution in time {format_time(timer.elapsed)}"
        )
    else:
        print(
            f'\n** FOUND EXISTING {model_label} BACKGROUND MODEL "{bg_model.label}" (store_id={bg_model.store_id})'
        )

    model_proxy = ModelProxy(bg_model)


# construct a ShardedPool to orchestrate database access
with Datastore(
    version_label=VERSION_LABEL,
    db_name=args.database,
    timeout=args.db_timeout,
    profile_agent=profile_agent,
    prune_unvalidated=args.prune_unvalidated,
    drop_actions=drop_actions,
) as pool:

    # set up LambdaCDM object representing a basic Planck2018 cosmology in Mpc units

    zend = args.zend
    samples_per_log10z = args.samples_log10z

    units = Mpc_units()

    def convert_to_redshifts(z_sample_set):
        return pool.object_get(
            "redshift",
            payload_data=[{"z": z} for z in z_sample_set],
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
        solve_ivp_DOP852,
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
        "solve_ivp+DOP853-stepping0": solve_ivp_DOP852,
        "solve_ivp+Radau-stepping0": solve_ivp_Radau,
        "solve_ivp+BDF-stepping0": solve_ivp_BDF,
        "solve_ivp+LSODA-stepping0": solve_icp_LSODA,
    }

    ## STEP 1
    ## BUILD A GRID OF Z-VALUES AT WHICH TO SAMPLE

    print("\n** BUILDING ARRAY OF Z-VALUES AT WHICH TO SAMPLE")

    # now we want to build a set of sample points for redshifts between z_init and
    # the final point z = z_final, using the specified number of redshift sample points
    num_z_sample = int(
        round(samples_per_log10z * (log10(zstart) - log10(zend)) + 0.5, 0)
    )

    z_array = ray.get(
        convert_to_redshifts(
            np.logspace(log10(zstart), log10(zend), num_z_sample),
        )
    )
    z_sample = redshift_array(z_array=z_array)

    model_list = build_model_list(pool, units)
    for model_data in model_list:
        run_pipeline(
            model_data,
            z_sample,
            atol,
            rtol,
            solvers,
        )
