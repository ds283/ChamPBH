import argparse
from typing import Optional

import configargparse

from ComputeTargets.BBNData import DEFAULT_BBN_WALL_CLOCK_LIMIT
from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
)
from CosmologyConcepts.drain_model import DRAIN_MODEL_ONE_LOOP, check_drain_model
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


# set on the namespace by AlphaValues when --alpha-values is given, and removed again by
# DriverArgumentParser once it has been read
_ALPHA_VALUES_GIVEN = "_alpha_values_given"


class AlphaValues(argparse._StoreAction):
    """
    --alpha-values: argparse's own store, which also records that the option was given, on the
    command line or in a --config file, which its default [0.0] alone cannot tell; so that
    --no-alpha-values can refuse it (no-alpha-values, 2026-11-07). It is a subclass of
    ``argparse._StoreAction`` because configargparse expands a YAML list into the option's values
    only for that class.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        super().__call__(parser, namespace, values, option_string)
        setattr(namespace, _ALPHA_VALUES_GIVEN, True)


NO_ALPHA_WITH_ALPHA = (
    "--no-alpha-values refuses --alpha-values: the first asks for no fixed-law row, the second "
    "names some. Give one or the other"
)
NO_ALPHA_WITHOUT_A = (
    "--no-alpha-values needs --A-values: with no fixed-law row and no one-loop row there is "
    "nothing to build or read"
)


def drain_rows_problem(namespace) -> Optional[str]:
    """
    None if the parsed drain-law options name at least one row, and otherwise the message that
    refuses them (no-alpha-values, 2026-11-07). Only --no-alpha-values can leave a run without a
    row, since --alpha-values takes one value or more and has the default [0.0]: it is refused
    together with --alpha-values, and without --A-values.
    """
    if not getattr(namespace, "no_alpha_values", False):
        return None
    if getattr(namespace, _ALPHA_VALUES_GIVEN, False):
        return NO_ALPHA_WITH_ALPHA
    if not getattr(namespace, "A_values", None):
        return NO_ALPHA_WITHOUT_A
    return None


class DriverArgumentParser(configargparse.ArgumentParser):
    """
    The drivers' parser. After configargparse's parse it checks the drain-law options with
    `drain_rows_problem`, calling ``self.error`` on a refusal (exit 2, before any Ray; a parser
    whose ``error`` is replaced, RunDrivers/registered_run.py's, refuses with the same message), and
    under --no-alpha-values sets ``alpha_values`` to the empty list. Every reader of
    ``alpha_values`` then sees the fixed-law rows the run has, none under the flag, and a command
    without the flag parses as it did (no-alpha-values, 2026-11-07).
    """

    def parse_known_args(self, *args, **kwargs):
        namespace, unknown = super().parse_known_args(*args, **kwargs)
        problem = drain_rows_problem(namespace)
        if problem is not None:
            self.error(problem)
        if hasattr(namespace, _ALPHA_VALUES_GIVEN):
            delattr(namespace, _ALPHA_VALUES_GIVEN)
        if getattr(namespace, "no_alpha_values", False):
            namespace.alpha_values = []
        return namespace, unknown


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


def alpha_fraction(text: str) -> float:
    """
    An argparse type for --alpha-values: the drained fraction alpha, 0 <= alpha < 1
    (inelastic-reflection prompt 10, P32). A value out of range is refused here, at parse and
    before Ray, since integrate_history refuses it inside a Ray task. ``-0`` is returned as 0.0, so
    that the alpha row, and the path built from it, have one spelling of zero.
    """
    value = float(text) + 0.0
    if not (0.0 <= value < 1.0):
        raise argparse.ArgumentTypeError(
            f"must be a fraction with 0 <= alpha < 1, not {text}"
        )
    return value


def drain_strength_A(text: str) -> float:
    """
    An argparse type for --A-values: the strength A of the one-loop drain law, finite and > 0, with no
    upper bound (one-loop-drain prompt 04, README §8.12 P33). The rules are CosmologyConcepts.drain_model's
    (check_drain_model under the code's current one-loop model), stated once; its ValueError is re-raised as
    an ArgumentTypeError so that argparse prints the message, and the option is named by argparse (exit 2,
    before any Ray). ``-0`` is refused as 0 is.
    """
    value = float(text) + 0.0
    try:
        check_drain_model(DRAIN_MODEL_ONE_LOOP, value, "--A-values")
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None
    return value


def distinct_values(values) -> list:
    """
    ``values`` without exact duplicates, in the order of first appearance. The drivers apply it once
    to --alpha-values and use only its result (inelastic-reflection prompt 10, P32).
    """
    seen = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return seen


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
    parser = DriverArgumentParser(
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
        help="specify number of sample points per decade of 1 + z (the axis is ln(1 + z) at "
        "multiples of ln(10)/N)",
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
        "--alpha-values",
        type=alpha_fraction,
        nargs="+",
        default=[0.0],
        action=AlphaValues,
        help="one or more values of the drained fraction alpha, 0 <= alpha < 1: the fraction of the "
        "condensate's kinetic energy that dissipation mode drains at each candidate. It is a key of "
        "every stored history; 0 is natural mode (the default %(default)s), and a given list "
        "replaces the default (inelastic-reflection prompt 10). --no-alpha-values gives no alpha",
    )
    parser.add_argument(
        "--no-alpha-values",
        action="store_true",
        default=False,
        help="build, or read, no fixed-law row: no alpha, not even the default alpha = 0 (natural "
        "mode), so that the run builds, or plots, its --A-values rows alone. It needs --A-values and "
        "refuses --alpha-values (exit 2, before any Ray). No stored value changes: a fixed-law row "
        "already in the store is left as it is, and is not read (no-alpha-values, 2026-11-07)",
    )
    parser.add_argument(
        "--A-values",
        type=drain_strength_A,
        nargs="+",
        default=None,
        help="one or more values of A, the strength of the one-loop drain law, finite and > 0 with no upper "
        "bound: each names a history key (A, the code's current one-loop drain model) beside the "
        "--alpha-values rows, which are looked up first and are unchanged. A is a key of every stored "
        "history, as alpha is. The default is none, so a run without it is the fixed-law run; --alpha-values "
        "keeps its default, so a run given only --A-values also builds, or reads, alpha = 0, natural mode "
        "(one-loop-drain prompt 04), unless --no-alpha-values is given",
    )
    parser.add_argument(
        "--tuning-max-integrations",
        type=positive_int,
        default=DEFAULT_TUNING_MAX_INTEGRATIONS,
        help="the cap on the integrations of one history's tuning, the seed's included "
        "(default %(default)s). It is not a key of the history: a history that does not "
        "converge within it is stored as a failure, and a later run with a larger cap does "
        "not recompute it unless given --retry-failed-scalar, which stores the new history "
        "beside the failure (jordan-normalization prompt 08; tuning-followups prompt 01)",
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
        "--retry-failed-scalar",
        action="store_true",
        default=False,
        help="retry scalar histories that failed under the current version label (for example "
        "after raising --tuning-max-integrations); the new history is stored beside the failure. "
        "Without this flag a stored scalar failure counts as done and is not retried. Not a key "
        "(tuning-followups prompt 01)",
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
    "tools/ray_cluster.sh), or 'local' for a private instance. "
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
