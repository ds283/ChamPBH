import argparse
from typing import Optional

import configargparse

from ComputeTargets.BBNData import DEFAULT_BBN_WALL_CLOCK_LIMIT
from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
)
from config.defaults import DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE

DEFAULT_LABEL = "ChamPBH-test"
DEFAULT_TIMEOUT = 60
DEFAULT_SHARDS = 20

DEFAULT_Z_END = 0.1
DEFAULT_T_INIT_GEV = 20000
DEFAULT_PHI_INIT_MP = 5.0

# the sampling axis (jordan-normalization prompt 03, P8): 300 decades of 1 + z, about 691 e-folds,
# against histories that need 80-200; the default was 35 before prompt 03. The axis is anchored at
# the end of the history, so its lower limit is 0 and nothing else
DEFAULT_LOG10_ONE_PLUS_Z_HIGH = 300
DEFAULT_LOG10_ONE_PLUS_Z_LOW = 0
DEFAULT_SAMPLES_PER_LOG10_Z = 250

DEFAULT_BETA_LOW = 0.1
DEFAULT_BETA_HIGH = 3.0
DEFAULT_SAMPLES_PER_BETA = 5

DEFAULT_LOG10_M_LOW_EV = 25
DEFAULT_LOG10_M_HIGH_EV = 26.5
DEFAULT_SAMPLES_PER_LOG10_M_EV = 6


allowed_drop_actions = ["scalar-history", "adiabatic-history", "bbn-data"]

# the options removed by jordan-normalization, each with what replaces it. They stay registered,
# hidden, so that a command line or a --config YAML file that still gives one is refused with this
# message (exit 2) rather than configargparse's "unrecognized arguments" (prompt 08, D1, D7)
_LAMBDA_REMOVED = (
    "each history's normalization is tuned so that H_J(T_CMB) = H0 and is stored with the "
    "history, so no Lambda is given; --tuning-tolerance sets the tuning"
)
REMOVED_OPTIONS = {
    "--log10-Lambda-low-eV": ("07", _LAMBDA_REMOVED),
    "--log10-Lambda-high-eV": ("07", _LAMBDA_REMOVED),
    "--samples-per-log10-Lambda-eV": ("07", _LAMBDA_REMOVED),
    "--Lambda-values-eV": ("07", _LAMBDA_REMOVED),
    "--Lambda-values-Mp": ("07", _LAMBDA_REMOVED),
    "--T-stop-GeV": (
        "07",
        "every stored history ends at T_CMB; tools/history_and_bbn.py --fixed-normalization "
        "NORM PHI_REF_MP --T-stop-GeV T runs an untuned history to any T",
    ),
    "--potential-type": (
        "08",
        "there is one model, ExponentialModel, V = V0 exp((M/phi)^n) with "
        "A = exp(beta (phi - phi_ref)/M_P); --n-values gives its exponent n",
    ),
}


class RemovedOption(argparse.Action):
    """
    An option that jordan-normalization removed (REMOVED_OPTIONS): giving it, with any number of
    values, calls ``parser.error`` with a message naming its replacement, so the parse ends with
    exit 2. ``parser.error`` rather than a print and an exit, so that a parser whose ``error`` is
    replaced (RunDrivers/registered_run.py's) refuses with the same message (prompt 08, D1, D7).
    """

    def __init__(self, option_strings, dest, removed_by: str, replacement: str, **kw):
        self.removed_by = removed_by
        self.replacement = replacement
        kw.setdefault("nargs", "*")
        kw.setdefault("help", argparse.SUPPRESS)
        kw.setdefault("default", argparse.SUPPRESS)
        super().__init__(option_strings, dest, **kw)

    def __call__(self, parser, namespace, values, option_string=None):
        parser.error(
            f"{option_string} was removed by jordan-normalization prompt "
            f"{self.removed_by}: {self.replacement}"
        )


def non_negative_float(text: str) -> float:
    """An argparse type: a float that is not negative (science-readiness prompt 01)."""
    value = float(text)
    if not value >= 0.0:
        raise argparse.ArgumentTypeError(f"must be a non-negative number, not {text}")
    return value


def positive_float(text: str) -> float:
    """An argparse type: a float that is greater than zero (jordan-normalization prompt 08)."""
    value = float(text)
    if not value > 0.0:
        raise argparse.ArgumentTypeError(f"must be a positive number, not {text}")
    return value


def positive_int(text: str) -> int:
    """An argparse type: an int that is greater than zero (jordan-normalization prompt 08)."""
    value = int(text)
    if not value > 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, not {text}")
    return value


def anchored_log10_one_plus_z_low(text: str) -> float:
    """
    An argparse type for --log10-one-plus-z-low: only 0 is accepted, since the sampling axis
    ln(1 + z) is anchored at the end of the history (jordan-normalization prompt 03, Z3).
    """
    value = float(text)
    if value != 0.0:
        raise argparse.ArgumentTypeError(
            f"must be 0, not {text}: the redshift axis is anchored at the end of the history"
        )
    return value


def create_argument_parser() -> configargparse.ArgumentParser:
    parser = configargparse.ArgumentParser(
        config_file_parser_class=configargparse.YAMLConfigFileParser
    )

    parser.add_argument(
        "-c",
        "--config",
        is_config_file=True,
        help="read options from the specified configuration file",
    )
    parser.add_argument(
        "--database",
        required=True,
        type=str,
        default=None,
        help="read/write work items using the specified database cache",
    )
    parser.add_argument(
        "--inventory",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="show an inventory of the datastore content",
    )
    parser.add_argument(
        "--show-all",
        default=False,
        action=argparse.BooleanOptionalAction,
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
        "--samples-log10-z",
        type=int,
        default=DEFAULT_SAMPLES_PER_LOG10_Z,
        help="specify number of z-sample points per log10(z)",
    )
    parser.add_argument(
        "--tuning-tolerance",
        type=positive_float,
        default=DEFAULT_TUNING_TOLERANCE,
        help="the tolerance of each history's tuning on both of its residuals, "
        "|H_J(T_CMB)/H0 - 1| and |phi_ref/phi_min - 1| (default %(default)s). It is a key of "
        "every stored history, so a run at another tolerance computes histories of its own "
        "(jordan-normalization prompt 08)",
    )
    parser.add_argument(
        "--tuning-max-integrations",
        type=positive_int,
        default=DEFAULT_TUNING_MAX_INTEGRATIONS,
        help="the cap on the integrations of one history's tuning, the seed's included "
        "(default %(default)s). It is not a key of the history: a history that does not "
        "converge within it is stored as a failure, and a later run with a larger cap does "
        "not recompute it (only --drop scalar-history, which drops every history, does) "
        "(jordan-normalization prompt 08)",
    )
    parser.add_argument(
        "--abs-tol",
        type=float,
        default=DEFAULT_ABS_TOLERANCE,
        help="specify absolute tolerance used during integration",
    )
    parser.add_argument(
        "--rel-tol",
        type=float,
        default=DEFAULT_REL_TOLERANCE,
        help="specify relative tolerance used during integration",
    )
    parser.add_argument(
        "--T-init-GeV",
        type=float,
        default=DEFAULT_T_INIT_GEV,
        help="set initial temperature in Jordan frame, specified in GeV",
    )
    parser.add_argument(
        "--phi-init-Mp",
        type=float,
        default=DEFAULT_PHI_INIT_MP,
        help="set the initial value of the scalar field phi* in the Einstein frame, in units of "
        "the (reduced) Planck mass M_P (science-readiness prompt 04). The initial momentum is zero. "
        "A start with Omega(phi*) T* > M_P is warned about and computed",
    )
    parser.add_argument(
        "--band-half-width",
        type=float,
        default=0.025,
        help="half-width h, in the coupling beta, of the window [beta - h, beta + h] over which "
        "plot_by_beta.py takes the running median and the 16th-84th percentile band of the "
        "abundance shifts (science-readiness prompt 07)",
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
        "--beta-values",
        type=float,
        nargs="*",
        action="extend",
        default=[],
        help="specify one or more beta values to sample",
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
        "--M-values-eV",
        type=float,
        nargs="*",
        action="extend",
        default=[],
        help="specify one or more values of M/eV to sample",
    )
    parser.add_argument(
        "--M-values-Mp",
        type=float,
        nargs="*",
        action="extend",
        default=[],
        help="specify one or more values of M/Mp to sample",
    )
    parser.add_argument(
        "--n-values",
        type=positive_int,
        nargs="+",
        default=[1],
        help="one or more values of the model's exponent n, V = V0 exp((M/phi)^n); n is part of "
        "the model's key (default %(default)s; jordan-normalization prompt 08)",
    )
    parser.add_argument(
        "--log10-one-plus-z-high",
        type=float,
        default=DEFAULT_LOG10_ONE_PLUS_Z_HIGH,
        help="maximum value of log10(1+z) to sample (default %(default)s)",
    )
    parser.add_argument(
        "--log10-one-plus-z-low",
        type=anchored_log10_one_plus_z_low,
        default=DEFAULT_LOG10_ONE_PLUS_Z_LOW,
        help="minimum value of log10(1+z) to sample; only 0 is accepted, since the redshift axis is anchored at the end of the history",
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
        help="drop one or more data categories (scalar-history replaced scalar-model in jordan-normalization prompt 02)",
        action="extend",
    )
    parser.add_argument(
        "--retry-failed-bbn",
        action="store_true",
        default=False,
        help="retry BBN computations that failed under the current version label; without "
        "this flag a stored BBN failure counts as done and is not retried",
    )
    parser.add_argument(
        "--bbn-wall-clock-limit",
        type=non_negative_float,
        default=DEFAULT_BBN_WALL_CLOCK_LIMIT,
        metavar="SECS",
        help="wall-clock limit on each PRyMordial solve, in seconds; a solve that exceeds it is "
        f"stored as a failure (default {DEFAULT_BBN_WALL_CLOCK_LIMIT:g}; 0 disables the limit)",
    )
    parser.add_argument(
        "--output",
        default="data-out",
        type=str,
        help="specify folder for output files",
    )
    parser.add_argument(
        "--Li7-axis-limits",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="limit the y-axis on Li7 plots to near the observationally-allowed region",
    )
    parser.add_argument(
        "--D-axis-limits",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="limit the y-axis on D/H plots to near the observationally-allowed region",
    )
    parser.add_argument(
        "--Yp-axis-limits",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="limit the y-axis on Y_p plots to near the observationally-allowed region",
    )
    parser.add_argument(
        "--ray-address",
        default=None,
        type=str,
        help="address of the Ray cluster; required whenever Ray starts (main.py --inventory starts no "
        "Ray and needs none). On this machine ChamPBH's cluster is 127.0.0.1:6380; 'local' starts a "
        "private instance. 'auto' is refused",
    )

    for option, (removed_by, replacement) in REMOVED_OPTIONS.items():
        parser.add_argument(
            option,
            action=RemovedOption,
            removed_by=removed_by,
            replacement=replacement,
        )

    return parser


RAY_ADDRESS_PROBLEM = (
    "!! --ray-address is required whenever Ray starts. Give the address of a running cluster "
    "(ChamPBH's on this machine is 127.0.0.1:6380, started by "
    ".documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh), or 'local' for a private instance. "
    "'auto' is refused: with the projects' clusters isolated, it attaches to whichever cluster it "
    "finds first."
)


def ray_address_problem(address: Optional[str]) -> Optional[str]:
    """Return None if ``address`` may be passed to ``ray.init``, and otherwise the message to print.

    A missing or empty address is refused, and so is 'auto' (any case, surrounding whitespace
    ignored). Anything else is accepted unchanged; Ray checks the address itself.
    """
    if address is None or address.strip().lower() in ("", "auto"):
        return RAY_ADDRESS_PROBLEM
    return None
