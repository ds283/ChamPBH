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

"""
Tune one scalar history per beta to T_CMB and write one CSV row each, holding the Jordan-frame
temperatures a surfing trajectory reaches before its first bounce inside an analysis window, with
no Ray cluster, no datastore and no BBN.

Written for surfing-temperature prompt 02 (T2, T3; README §2.2, §4). Since jordan-normalization
prompt 08 (U4, D5) each history is tuned as main.py's are: `tune_history` integrates it to
T_J = T_CMB with `integrate_history` and adjusts the model's normalization and phi_ref until
H_J(T_CMB) = H0 and phi_ref = phi_min(T_CMB), within --tuning-tolerance on both residuals and
--tuning-max-integrations integrations (main.py's options and defaults, 1e-5 and 30). The setup
is tools/history_and_bbn.py's, whose constants and z grid are imported from it: QCD_Cosmology on
Planck2018, ExponentialModel with n = 1 and the given M and beta, pi* = 0, T* = 2e4 GeV, the
default tolerances and step control, and 250 samples per decade of 1 + z to main.py's 300
decades. The final integration's IntegrationResult is kept by wrapping integrate_scalar_history
for the duration of the call, as history_and_bbn.py does, and its turning points of T_J are
`surfing_turning_points` (ComputeTargets/ScalarHistory.py) on the tuned realization. Nothing is
stored. (Until prompt 08 each history was one integration at the fixed normalization
4 ln(1e-3 eV), phi_ref = 0, stopped at --T-stop-GeV: [05-surfing-scan-runs-at-a-fixed-lambda].)

The analysis window. A tuned history runs to T_CMB, so the analysis is restricted to a window
that ends at the history's first crossing of T_J = --T-window-low-GeV (default 1e-3 GeV, the old
stopping temperature), found by `T_Jordan_crossing` on the dense output, at e-fold N_window. Only
the turning points with N <= N_window are kept, and the first bounce only if its N <= N_window
(`in_window`). This reproduces the old stop exactly: an integration stopped at T_stop ended at
that same first crossing. A filter on T_J would not, since T_J is not monotonic. A history that
never reaches the edge is analysed whole.

Run from the repository root, since the EOS reads its table from the working directory:

    ./venv/bin/python tools/surfing_scan.py --out PATH.csv \\
        (--beta-min B0 --beta-max B1 --beta-step DB | --beta-values B [B ...]) \\
        [--phi-init-Mp X] [--M M] [--T-window-low-GeV T] \\
        [--tuning-tolerance TOL] [--tuning-max-integrations K]

The beta grid is beta_k = beta_min + k * beta_step, rounded to 10 decimal places, for every k with
beta_k <= beta_max. M is in units of the (reduced) Planck mass. --T-stop-GeV was removed by
prompt 08 and is refused (exit 2).

The CSV (README §4) has one row per beta, appended and flushed as each history ends: the inputs
(beta, phi_init_Mp and M_Mp in M_P, tuning_tol, T_window_low_GeV in GeV), status and
failure_reason, the tuning's results (normalization: the model's own, ln V0 with V0 in M_P^4,
unconverted; V_ref_eV4: V(phi_ref) in eV^4; phi_ref_Mp in M_P; residual_H, residual_phi and
tuning_iterations, the integrations, the seed's included), then the measured columns
(temperatures in GeV, N in e-folds from T*), reflections and accepted_steps of the final
integration, and wall_s, the whole tuning's. The six tuned columns are blank on a failure row. A
blank field is empty; a boolean is `true` or `false`; a float is Python's repr of the value, so a
row depends on nothing but the history (wall_s apart). If PATH.csv exists the run resumes: a file
whose header holds Lambda_eV or T_stop_GeV (written before prompt 08, at a fixed Lambda) is
refused with its own message; any other header that differs is refused; a row whose phi*, M,
tuning tolerance or window edge differs from the options is refused (all exit 2); and every beta
already in the file (to 1e-9) is skipped. A failed history is a row with status=failure and the
payload's reason, so it is not retried. PATH.csv.meta.json records the git HEAD, whether a
tracked file differs from it, VERSION_LABEL, the options (the tuning tolerance, the cap and the
window's edge among them), the start time and the Python and SciPy versions; it is written at the
start of a run, and a resumed run adds an entry to it.

One console line per beta:

    beta=... status=ok capture=... max=... second_dip=... deliver=... GeV crossings=... \\
        iterations=... phi_ref=... M_P V_ref=... eV^4 wall=... s

with `-` for a blank, and a flag where a captured history has no bounce above T_window.
tune_history's `-- tune` lines, one per integration, and integrate_history's own printing
(failures only, by default) are left on the console.

Costs (jordan-normalization prompt 08, measured at a load falling from 8 to 5): one integration
to T_CMB takes about 5.0 s at beta = 1.5, 7.9 s at 3.0 and 13 s at 4.0 (against 0.5, 1.7 and
4.8 s to 1 MeV, untuned, before prompt 08), and a tuning four or five of them: 20, 32 and 65 s a
beta. Interpolated over the grid, at 4.3 integrations a beta, the 79-beta scan of 1.10-5.00
(S1) takes about 55 minutes and the 61-beta scan of 2.98-3.04 (S5) about 35, against 280 s and
150 s before.

Exit status: 0 if every history ran and none failed, 1 if a failure row was written, 2 if the run
was refused.
"""

import argparse
import csv
import importlib
import json
import os
import platform
import subprocess
import sys
import time
from collections import namedtuple
from datetime import datetime, timezone
from math import exp, floor, isclose, log
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional, Sequence, Tuple

import scipy

# make the repository root importable when run as `python tools/surfing_scan.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ComputeTargets.ScalarHistory import (  # noqa: E402
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
    TUNING_OUTPUT_KEYS,
    FirstBounce,
    SurfingCrossing,
    T_Jordan_crossing,
    surfing_turning_points,
    tune_history,
)
from config.argument_parser import (  # noqa: E402
    RemovedOption,
    positive_float,
    positive_int,
)
from config.version import VERSION_LABEL  # noqa: E402
from CosmologyConcepts import (  # noqa: E402
    M_value,
    beta_value,
    temperature,
)
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from extract_common import find_kicks, reachability_roots  # noqa: E402
from tools.history_and_bbn import (  # noqa: E402
    PHI_INIT_MP,
    PI_INIT,
    T_INIT_GEV,
    _z_grid,
)
from Units import Planck_units  # noqa: E402

# the file the EOS reads by a path relative to the working directory
EOS_TABLE = Path("CosmologyModels", "GenericEOS", "Xav_EOS_data.csv")

# the default lower edge of the analysis window, in GeV: the first crossing of T_J = 1 MeV, which
# was the stopping temperature (P3) until jordan-normalization prompt 08 tuned each history to
# T_CMB
T_WINDOW_LOW_GEV_DEFAULT = 1.0e-3
M_MP_DEFAULT = 1.0e-3

# --T-stop-GeV, refused by name (jordan-normalization prompt 08)
T_STOP_REMOVED = (
    "each history is tuned to T_CMB; --T-window-low-GeV sets the lower edge of the analysis "
    "window, the history's first crossing of that T_J"
)

# README §4, in order; jordan-normalization prompt 08 replaced Lambda_eV and T_stop_GeV by the
# tuning tolerance and the window's edge, and added the tuning's six results
COLUMNS = [
    "beta",
    "phi_init_Mp",
    "M_Mp",
    "tuning_tol",
    "T_window_low_GeV",
    "status",
    "failure_reason",
    "normalization",
    "V_ref_eV4",
    "phi_ref_Mp",
    "residual_H",
    "residual_phi",
    "tuning_iterations",
    "captured",
    "capture_kick",
    "T_capture_GeV",
    "N_capture",
    "T_max_GeV",
    "N_max",
    "T_second_dip_GeV",
    "N_second_dip",
    "T_deliver_GeV",
    "N_deliver",
    "deliver_reflected",
    "n_crossings",
    "reflections",
    "accepted_steps",
    "wall_s",
    "T_hot_GeV",
    "T_cold_GeV",
]

# beta values closer than this are the same point of the grid
BETA_MATCH = 1e-9

# the columns a resumed run must agree with the options on, and the relative tolerance (the file
# holds repr of each value, which reads back exactly)
_INPUT_MATCH = ("phi_init_Mp", "M_Mp", "tuning_tol", "T_window_low_GeV")
_INPUT_RTOL = 1e-12

# the columns of a CSV written before jordan-normalization prompt 08, at a fixed Lambda
_OLD_FORMAT_COLUMNS = ("Lambda_eV", "T_stop_GeV")

# what one history returns to main
#   failure_reason:  None, or the payload's failure_reason
#   crossings:       surfing_turning_points(result, model) inside the window, or None on a failure
#   bounce:          the payload's first_bounce (first_bounce(result)) if inside the window, or None
#   accepted_steps:  the final integration's accepted_steps, or None on a failure
#   reflections:     the final integration's reflection count, or None on a failure
#   wall_s:          the wall-clock time of the tuning and the walk, seconds
#   result:          the captured IntegrationResult of the final integration, or None
#   tuning:          {key: value} of TUNING_OUTPUT_KEYS from the payload, or None on a failure
#   N_window:        the e-fold of the window's lower edge (T_Jordan_crossing), or None if the
#                    history never reaches it (or failed)
HistoryOutcome = namedtuple(
    "HistoryOutcome",
    [
        "failure_reason",
        "crossings",
        "bounce",
        "accepted_steps",
        "reflections",
        "wall_s",
        "result",
        "tuning",
        "N_window",
    ],
)


def beta_grid(beta_min: float, beta_max: float, beta_step: float) -> List[float]:
    """beta_min + k beta_step, rounded to 10 decimal places, for k = 0, 1, ... up to beta_max."""
    if beta_step <= 0.0:
        raise ValueError("beta_grid: the step must be positive")
    n = int(floor((beta_max - beta_min) / beta_step + 1e-9))
    return [round(beta_min + k * beta_step, 10) for k in range(n + 1)]


def make_setup() -> SimpleNamespace:
    """The units, parameters and cosmology of history_and_bbn.py, built once per run."""
    units = Planck_units()
    params = Planck2018()
    cosmology = QCD_Cosmology(0, units, params)
    return SimpleNamespace(units=units, params=params, cosmology=cosmology)


def in_window(
    crossings: Sequence[SurfingCrossing],
    bounce: Optional[FirstBounce],
    N_edge: Optional[float],
) -> Tuple[List[SurfingCrossing], Optional[FirstBounce]]:
    """
    The analysis window (jordan-normalization prompt 08): the crossings with N <= N_edge, in
    order, and the bounce if its N <= N_edge (else None). N_edge None is no edge: everything is
    kept. Pure.
    """
    if N_edge is None:
        return list(crossings), bounce
    kept = [c for c in crossings if c.N <= N_edge]
    if bounce is not None and not bounce.N <= N_edge:
        bounce = None
    return kept, bounce


def run_history(
    setup: SimpleNamespace,
    beta: float,
    phi_init_Mp: float,
    M_Mp: float,
    T_window_low_GeV: float,
    tolerance: float = DEFAULT_TUNING_TOLERANCE,
    max_integrations: int = DEFAULT_TUNING_MAX_INTEGRATIONS,
) -> HistoryOutcome:
    """
    One history tuned to T_CMB through tune_history (jordan-normalization prompt 08), with the
    final integration's IntegrationResult captured by wrapping integrate_scalar_history for the
    duration of the call (history_and_bbn.py's idiom; only the last is kept, and the wrapper is
    removed in a finally), and its turning points of T_J on the tuned realization, restricted to
    the window that ends at the first crossing of T_J = T_window_low_GeV (`in_window`).
    """
    units = setup.units

    model = ExponentialModel(
        0, M_value(0, M_Mp * units.PlanckMass), 1, beta_value(0, beta), units
    )
    T_init = temperature(0, T_INIT_GEV * units.GeV)

    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    captured = []
    integrate = SM.integrate_scalar_history

    def _capturing_integrate(*a, **kw):
        result = integrate(*a, **kw)
        captured[:] = [result]
        return result

    SM.integrate_scalar_history = _capturing_integrate
    start = time.perf_counter()
    try:
        data = tune_history(
            setup.cosmology,
            model,
            T_init,
            phi_value(0, phi_init_Mp * units.PlanckMass),
            pi_value(0, PI_INIT),
            _z_grid(),
            task_label=f"surfing_scan-beta{beta:g}-M{M_Mp:g}",
            tolerance=tolerance,
            max_integrations=max_integrations,
        )
    finally:
        SM.integrate_scalar_history = integrate

    result = captured[-1] if captured else None

    if data.get("failure", False):
        wall = time.perf_counter() - start
        return HistoryOutcome(
            failure_reason=data.get("failure_reason", "(no reason stored)"),
            crossings=None,
            bounce=None,
            accepted_steps=None,
            reflections=None,
            wall_s=wall,
            result=result,
            tuning=None,
            N_window=None,
        )

    realized = model.realize(data["normalization"], data["phi_ref"])
    N_window = T_Jordan_crossing(result, log(T_window_low_GeV * units.GeV))
    crossings, bounce = in_window(
        surfing_turning_points(result, realized), data["first_bounce"], N_window
    )
    wall = time.perf_counter() - start
    return HistoryOutcome(
        failure_reason=None,
        crossings=crossings,
        bounce=bounce,
        accepted_steps=data["accepted_steps"],
        reflections=data["reflections"],
        wall_s=wall,
        result=result,
        tuning={key: data[key] for key in TUNING_OUTPUT_KEYS},
        N_window=N_window,
    )


def tuned_columns(tuning: dict, units) -> dict:
    """The six tuned columns of a row, from a successful outcome's `tuning`: the normalization
    unconverted, V_ref in eV^4, phi_ref in M_P, the residuals and the integrations."""
    return {
        "normalization": tuning["normalization"],
        "V_ref_eV4": tuning["V_ref"] / units.eV**4,
        "phi_ref_Mp": tuning["phi_ref"] / units.PlanckMass,
        "residual_H": tuning["residual_H"],
        "residual_phi": tuning["residual_phi"],
        "tuning_iterations": tuning["tuning_iterations"],
    }


def summarise(
    crossings: Sequence[SurfingCrossing],
    bounce: Optional[FirstBounce],
    kicks,
    roots,
    units,
) -> dict:
    """
    The measured columns of one successful history's row (T3; README §0.2, §6.1 U1-U3).

    - capture: the first "min" crossing;
    - largest: the "max" crossing of largest T_J;
    - second dip: the first "min" crossing after the first "max";
    - delivery: the first bounce (first_bounce's T_J), whether or not the history was captured;
    - captured: True if there is at least one crossing; n_crossings: their number;
    - capture_kick: the label of the find_kicks window that contains T_capture;
    - T_hot_GeV, T_cold_GeV: that kick's record in `roots` (reachability_roots at this beta),
      the merged interval's ends where it is merged.

    `kicks` is find_kicks's list. `roots` is reachability_roots's list, and is read only if there
    is a capture (None will do otherwise). A field that does not exist is None. Temperatures are
    in GeV, N is the e-fold number.
    """
    GeV = units.GeV

    def T_GeV(log_T: float) -> float:
        return exp(log_T) / GeV

    row = {
        "captured": len(crossings) > 0,
        "capture_kick": None,
        "T_capture_GeV": None,
        "N_capture": None,
        "T_max_GeV": None,
        "N_max": None,
        "T_second_dip_GeV": None,
        "N_second_dip": None,
        "T_deliver_GeV": None,
        "N_deliver": None,
        "deliver_reflected": None,
        "n_crossings": len(crossings),
        "T_hot_GeV": None,
        "T_cold_GeV": None,
    }

    minima = [c for c in crossings if c.direction == "min"]
    maxima = [c for c in crossings if c.direction == "max"]

    if minima:
        capture = minima[0]
        row["T_capture_GeV"] = T_GeV(capture.log_T_Jordan)
        row["N_capture"] = capture.N

        T_capture = exp(capture.log_T_Jordan)
        for kick in kicks:
            if kick["T_window_cold"] <= T_capture <= kick["T_window_hot"]:
                row["capture_kick"] = kick["label"]
                break

        if row["capture_kick"] is not None:
            for record in roots:
                if record["kick"] == row["capture_kick"]:
                    if record["T_hot"] is not None:
                        row["T_hot_GeV"] = record["T_hot"] / GeV
                    if record["T_cold"] is not None:
                        row["T_cold_GeV"] = record["T_cold"] / GeV
                    break

    if maxima:
        largest = max(maxima, key=lambda c: c.log_T_Jordan)
        row["T_max_GeV"] = T_GeV(largest.log_T_Jordan)
        row["N_max"] = largest.N

        N_first_max = maxima[0].N
        later = [c for c in minima if c.N > N_first_max]
        if later:
            row["T_second_dip_GeV"] = T_GeV(later[0].log_T_Jordan)
            row["N_second_dip"] = later[0].N

    if bounce is not None:
        row["T_deliver_GeV"] = T_GeV(bounce.log_T_Jordan)
        row["N_deliver"] = bounce.N
        row["deliver_reflected"] = bool(bounce.reflected)

    return row


def format_field(value) -> str:
    """A CSV field: blank for None, true/false for a bool, repr for a float, str otherwise."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _git(*args: str) -> Optional[str]:
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout


def _provenance(args, betas: Sequence[float], skipped: int, resumed: bool) -> dict:
    head = _git("rev-parse", "HEAD")
    status = _git("status", "--porcelain", "--untracked-files=no")
    return {
        "start_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_head": head.strip() if head is not None else None,
        "git_tracked_changes": (status.strip() != "") if status is not None else None,
        "version_label": VERSION_LABEL,
        "options": {
            "beta_min": args.beta_min,
            "beta_max": args.beta_max,
            "beta_step": args.beta_step,
            "beta_values": args.beta_values,
            "phi_init_Mp": args.phi_init_Mp,
            "M": args.M,
            "T_window_low_GeV": args.T_window_low_GeV,
            "tuning_tolerance": args.tuning_tolerance,
            "tuning_max_integrations": args.tuning_max_integrations,
            "out": str(args.out),
        },
        "beta_grid": list(betas),
        "resumed": resumed,
        "skipped": skipped,
        "python": platform.python_version(),
        "scipy": scipy.__version__,
    }


def _write_meta(meta_path: Path, entry: dict, resumed: bool) -> None:
    runs = []
    if resumed and meta_path.exists():
        with open(meta_path) as f:
            runs = json.load(f).get("runs", [])
    runs.append(entry)
    with open(meta_path, "w") as f:
        json.dump({"runs": runs}, f, indent=2)
        f.write("\n")


def read_existing(path: Path, args) -> Optional[List[float]]:
    """
    The beta of an existing CSV's rows, or None (with a message) if the file must not be resumed:
    its header holds Lambda_eV or T_stop_GeV (a scan made before jordan-normalization prompt 08,
    at a fixed Lambda, which a tuned scan does not resume), its header differs from COLUMNS, a row
    has the wrong number of fields, or a row's phi*, M, tuning tolerance or window edge differs
    from the options.
    """
    expected = {
        "phi_init_Mp": args.phi_init_Mp,
        "M_Mp": args.M,
        "tuning_tol": args.tuning_tolerance,
        "T_window_low_GeV": args.T_window_low_GeV,
    }
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is not None and any(c in header for c in _OLD_FORMAT_COLUMNS):
            print(
                f"!! surfing_scan: {path} was written before jordan-normalization prompt 08, "
                f"by a scan of untuned histories at a fixed Lambda (its header holds Lambda_eV "
                f"or T_stop_GeV); a tuned scan does not resume it. Give a new --out"
            )
            return None
        if header != COLUMNS:
            print(f"!! surfing_scan: {path} has a different header; not resumed")
            return None
        betas = []
        for line, row in enumerate(reader, start=2):
            if len(row) != len(COLUMNS):
                print(f"!! surfing_scan: {path} line {line} has {len(row)} fields")
                return None
            fields = dict(zip(COLUMNS, row))
            for key in _INPUT_MATCH:
                if not isclose(float(fields[key]), expected[key], rel_tol=_INPUT_RTOL):
                    print(
                        f"!! surfing_scan: {path} line {line} has {key}={fields[key]}, "
                        f"but this run has {expected[key]!r}; not resumed"
                    )
                    return None
            betas.append(float(fields["beta"]))
    return betas


def _console_T(value: Optional[float]) -> str:
    return "-" if value is None else f"{value:.6g}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--beta-min", type=float, default=None)
    parser.add_argument("--beta-max", type=float, default=None)
    parser.add_argument("--beta-step", type=float, default=None)
    parser.add_argument(
        "--beta-values",
        type=float,
        nargs="+",
        default=None,
        help="explicit beta values, in place of --beta-min/--beta-max/--beta-step",
    )
    parser.add_argument(
        "--phi-init-Mp",
        type=float,
        default=PHI_INIT_MP,
        help="initial value of the scalar field in units of M_P (default 5, as main.py)",
    )
    parser.add_argument(
        "--M",
        type=float,
        default=M_MP_DEFAULT,
        help="the potential's M, in units of M_P (default 1e-3)",
    )
    parser.add_argument(
        "--T-window-low-GeV",
        type=positive_float,
        default=T_WINDOW_LOW_GEV_DEFAULT,
        help="the lower edge of the analysis window on each history tuned to T_CMB, in GeV: "
        "turning points and the first bounce are kept up to the history's first crossing of "
        "this Jordan-frame temperature (default %(default)s, where the integration stopped "
        "before jordan-normalization prompt 08)",
    )
    parser.add_argument(
        "--tuning-tolerance",
        type=positive_float,
        default=DEFAULT_TUNING_TOLERANCE,
        help="the tuning's tolerance on both residuals, |H_J(T_CMB)/H0 - 1| and "
        "|phi_ref/phi_min - 1| (default %(default)s, as main.py)",
    )
    parser.add_argument(
        "--tuning-max-integrations",
        type=positive_int,
        default=DEFAULT_TUNING_MAX_INTEGRATIONS,
        help="the cap on the tuning's integrations, the seed's included (default "
        "%(default)s, as main.py)",
    )
    parser.add_argument(
        "--T-stop-GeV",
        action=RemovedOption,
        removed_by="08",
        replacement=T_STOP_REMOVED,
    )
    parser.add_argument("--out", type=Path, required=True, help="the CSV path")
    args = parser.parse_args(argv)

    if not Path(os.getcwd(), EOS_TABLE).is_file():
        print(
            f"!! surfing_scan: run from the repository root; the EOS reads {EOS_TABLE} from "
            f"the working directory ({os.getcwd()})"
        )
        return 2

    ranged = (args.beta_min, args.beta_max, args.beta_step)
    if args.beta_values is not None:
        if any(x is not None for x in ranged):
            print(
                "!! surfing_scan: give --beta-values or --beta-min/--beta-max/--beta-step, "
                "not both"
            )
            return 2
        betas = [round(b, 10) for b in args.beta_values]
    else:
        if any(x is None for x in ranged):
            print(
                "!! surfing_scan: give --beta-values, or all of --beta-min, --beta-max and "
                "--beta-step"
            )
            return 2
        betas = beta_grid(*ranged)

    out: Path = args.out
    meta_path = Path(str(out) + ".meta.json")
    resumed = out.exists()
    done: List[float] = []
    if resumed:
        existing = read_existing(out, args)
        if existing is None:
            return 2
        done = existing

    todo = [b for b in betas if not any(abs(b - d) <= BETA_MATCH for d in done)]
    skipped = len(betas) - len(todo)

    _write_meta(meta_path, _provenance(args, betas, skipped, resumed), resumed)
    print(
        f"-- surfing_scan: {len(todo)} beta to run, {skipped} already in {out}; "
        f"phi*={args.phi_init_Mp:g} M_P, M={args.M:g} M_P, tuned to T_CMB with tolerance "
        f"{args.tuning_tolerance:g} (at most {args.tuning_max_integrations} integrations), "
        f"window to T_J={args.T_window_low_GeV:g} GeV"
    )

    setup = make_setup()
    units = setup.units
    cosmology = setup.cosmology
    kicks = find_kicks(
        cosmology,
        cosmology._eos._T_min * units.GeV,
        cosmology._eos._T_max * units.GeV,
    )

    any_failure = False
    with open(out, "a", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        if not resumed:
            writer.writerow(COLUMNS)
            f.flush()

        for beta in todo:
            outcome = run_history(
                setup,
                beta,
                args.phi_init_Mp,
                args.M,
                args.T_window_low_GeV,
                tolerance=args.tuning_tolerance,
                max_integrations=args.tuning_max_integrations,
            )

            row = {
                "beta": float(beta),
                "phi_init_Mp": float(args.phi_init_Mp),
                "M_Mp": float(args.M),
                "tuning_tol": float(args.tuning_tolerance),
                "T_window_low_GeV": float(args.T_window_low_GeV),
                "wall_s": f"{outcome.wall_s:.3f}",
            }
            flag = ""
            if outcome.failure_reason is not None:
                any_failure = True
                row["status"] = "failure"
                row["failure_reason"] = outcome.failure_reason
            else:
                row["status"] = "ok"
                row.update(tuned_columns(outcome.tuning, units))
                roots = None
                if any(c.direction == "min" for c in outcome.crossings):
                    roots = reachability_roots(cosmology, beta, kicks)
                row.update(
                    summarise(outcome.crossings, outcome.bounce, kicks, roots, units)
                )
                row["reflections"] = outcome.reflections
                row["accepted_steps"] = outcome.accepted_steps
                if row["captured"] and outcome.bounce is None:
                    flag = " [captured, but no bounce above T_window]"
                elif not row["captured"]:
                    flag = " [not captured]"

            writer.writerow([format_field(row.get(c)) for c in COLUMNS])
            f.flush()

            if outcome.failure_reason is not None:
                print(
                    f"beta={beta!r} status=failure wall={outcome.wall_s:.1f} s "
                    f"reason={outcome.failure_reason}"
                )
            else:
                print(
                    f"beta={beta!r} status=ok "
                    f"capture={_console_T(row['T_capture_GeV'])} "
                    f"max={_console_T(row['T_max_GeV'])} "
                    f"second_dip={_console_T(row['T_second_dip_GeV'])} "
                    f"deliver={_console_T(row['T_deliver_GeV'])} GeV "
                    f"crossings={row['n_crossings']} "
                    f"iterations={row['tuning_iterations']} "
                    f"phi_ref={row['phi_ref_Mp']:.10e} M_P "
                    f"V_ref={row['V_ref_eV4']:.6e} eV^4 "
                    f"wall={outcome.wall_s:.1f} s{flag}"
                )
            sys.stdout.flush()

    return 1 if any_failure else 0


if __name__ == "__main__":
    sys.exit(main())
