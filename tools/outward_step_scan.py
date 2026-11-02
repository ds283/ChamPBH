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
The outward-step scanner (reflection-restart prompt 01, §1.1; README §6.2 P1, P2, P3): run scalar
histories through the production code, with no Ray, no BBN and no store written, and record every
accepted Radau step that begins outward (pi > 0), flagged by P1's flag, of which the signature of the
bad step after a reflection (`.documents/post-reflection-step-2026-11-02/`) is a case.

**The ruled signature** (README §6.3): P1's flag, either condition, on the first accepted step of a
Radau instance begun by a restart (a reflection, a drain, a release, or the guard's retake), never on
the history's start or a later step. Since reflection-restart prompt 03 the production code gives
such a restart F3's first step and guards that step (`ComputeTargets.ScalarHistory`,
`_restart_first_step` and `_outward_step_flagged`): a flagged first step is discarded and retaken at
half its h. Each record counts the signature steps kept and discarded apart (`signature_kept`,
`signature_discarded` in "scan") and carries the payload's `restart_retakes`.

**The flag (P1).** A step that begins with pi > 0 is flagged if either
  - pi_end/pi_start < 1/2 and pi does not change sign on the step's interpolant (the "ratio"
    condition), or
  - the interpolant's least phi falls below (1 - f) phi_start, f = StepControl.cap_fraction (the
    "phi" condition).
The interpolant is read at FLAG_SAMPLES (50) equally spaced points, its two ends included, as the
diagnosis read it; "changes sign" means some sample has pi <= 0. h*omega, omega = sqrt(|J[1,0]|) of
the Jacobian Radau holds at the step's start (J[1,0] = d pi'/d phi), is recorded, never flagged on.
`ComputeTargets.ScalarHistory._outward_step_flagged`, the guard's predicate, holds the same definition
(a test holds the two equal).

**How it observes.** By a subclass of SciPy's Radau put in place of the name `Radau` in
`ComputeTargets.ScalarHistory` for the duration of a history (`observe`), and the module's
`integrate_scalar_history` wrapped for the same duration to count integrations and keep each
IntegrationResult. Nothing in SciPy is modified, no other module's `Radau` is touched, and both
names are restored when the history ends, so the patch cannot leak into another import; nothing is
patched at import. The subclass reads the solver's attributes before and after each step and calls
SciPy's own `step`, so the values are bitwise those of an unobserved run. What started each Radau
instance (start, reflection, drain, release, or the guard's retake) is read from the frame of
`integrate_scalar_history` that builds it: the closure `resume_after_release` for a release, and
otherwise the identity of the state array passed to `new_solver` against the loop's locals
`y_reflected`, `y_drained` and `y_retake`. A step the guard discards is marked "discarded" in its row
when the retake's instance is built.

**What it records**, per history (one JSON object): the outcome; the final integration's accepted
steps, RHS evaluations and restart_retakes, the wall-clock; reflections, injections, parking and
release (N and ln T_J, T_J in GeV) and the dark-radiation values (`dissipation_values` of the payload);
per integration, the IntegrationResult's counts (restart_retakes among them) or the failure; over every
integration, the counts of outward steps, first outward steps, flagged steps (first and later apart)
and signature steps (kept and discarded apart), the distribution of
pi_end/pi_start and of h*omega over the outward steps, the largest h*omega over all accepted steps;
the flagged rows in full (and, with --rows first or all, more rows); and SHA-256 digests of the
samples and of their ln(1 + z), so that two runs can be seen to be bitwise equal.

**From a store** (P3). `--from-store PATH` selects histories from a store read-only (sqlite
`mode=ro` on each shard `PATH-shard*.db`; a path that cannot be opened read-only is refused) and runs
each untuned, at its stored normalization and phi_ref, alpha, phi*, T* and tolerances, to T_CMB on
main.py's sampling axis. The new payload is converted to the stored columns by the factory's own
`store` (`ObjectFactories.ScalarHistory`), called with an inserter that captures the rows, and each
is compared with the store: every `ScalarHistoryValue` column but serial, history_serial and z_serial,
the sample matched to its stored row by ln(1 + z) through z_serial and the `redshift` table; the
`ScalarHistory` columns the history computes (not the tuning's, the timings, the serials, label,
version or timestamp); and `extra_data` key by key over the keys both hold. The result is "bitwise",
or the columns that differ with the largest relative difference and the first differing sample.

**Many histories.** `batch` runs a list of tasks (a JSON file, or a store selection) in worker
processes, one process per task, at most --jobs at a time, each with a wall-clock limit (--timeout;
a task past it is killed and recorded as "timeout" with the progress it had reached). One JSON line
per task is appended to the output and flushed; a task whose key is already there is skipped, so a
killed batch resumes. A task that raises anything is recorded with its traceback and the batch goes
on. The batch writes its pid to OUTPUT.pid and logs to OUTPUT.log, whose last line is "** scan done".

**The production code only.** Prompt 01's probe switch (`--variant`, `--stiffness-c`, a task's
"variant") was removed with the switch by reflection-restart prompt 03; a task that names a variant is
refused. 01's task lists and its measurement scripts run only at `d4703df`.

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python tools/outward_step_scan.py one BETA M [--alpha A] \\
        [--phi-init-Mp X] [--fixed-normalization NORM PHI_REF_MP [--T-stop-GeV T]] \\
        [--rows flagged|first|all]
    PYTHONPATH=. ./venv/bin/python tools/outward_step_scan.py batch --output OUT.jsonl \\
        (--tasks TASKS.json | --from-store PATH [--M M ...] [--beta B ...] [--alpha A ...] \\
         [--block A|A2|B|C|D ...] [--success-only]) [--jobs N] [--timeout S]

Written for reflection-restart prompt 01; made a tool on the production code by prompt 03.
"""

import argparse
import contextlib
import glob
import hashlib
import importlib
import importlib.util
import io
import json
import math
import multiprocessing
import os
import sqlite3
import struct
import sys
import time
import traceback
from math import log, sqrt
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import numpy as np

# make the repository root importable when run as `python tools/outward_step_scan.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# the flag (P1)
FLAG_SAMPLES = 50
PI_RATIO_THRESHOLD = 0.5

# the start reasons of a restarted Radau instance, whose first step the ruled signature looks at
# (README §6.3): every reason but "start" (and "unknown")
RESTART_REASONS = ("reflection", "drain", "release", "retake")

# main.py's initial data and sampling axis (tools/history_and_bbn.py's, which are main.py's)
PHI_INIT_MP = 5.0
PI_INIT = 0.0
T_INIT_GEV = 2.0e4
SAMPLES_PER_LOG10_Z = 250
DEFAULT_TIMEOUT = 1800.0
DEFAULT_JOBS = 6

# the blocks of the 2026.8.0 run (science-runs/2026-11-01-science-2026.8.0/README.md), by M in M_P
# and, at 4e-31, by alpha: A is alpha >= 0.01, A2 below
BLOCKS = {
    "A": ((4.0e-31,), lambda a: a >= 0.01),
    "A2": ((4.0e-31,), lambda a: a < 0.01),
    "B": ((1.0e-33, 1.0e-30), lambda a: True),
    "C": ((1.0e-15, 1.0e-13, 1.0e-12), lambda a: True),
    "D": ((1.0e-3, 0.1, 0.5), lambda a: True),
}

# the edges of the recorded distributions over outward steps: pi_end/pi_start (steps with no sign
# change) and log10(h*omega)
PI_RATIO_EDGES = (
    0.0,
    1e-4,
    1e-3,
    1e-2,
    0.1,
    0.5,
    0.9,
    0.99,
    0.999,
    0.9999,
    0.99999,
    1.0,
)
LOG10_H_OMEGA_EDGES = tuple(range(-12, 5))

# the ScalarHistory columns the comparison leaves out: the keys and serials, the tuning's, the
# timings and the bookkeeping (orchestrator note 01, ruling 2)
_EXCLUDED_HISTORY_COLUMNS = {
    "serial",
    "label",
    "version",
    "timestamp",
    "cosmology_type",
    "cosmology_serial",
    "model_type",
    "model_serial",
    "atol_serial",
    "rtol_serial",
    "tuning_tol_serial",
    "alpha_serial",
    "phi_Einstein_init_serial",
    "pi_Einstein_init_serial",
    "T_Jordan_init_serial",
    "solver_serial",
    "compute_time",
    "mean_RHS_time",
    "max_RHS_time",
    "min_RHS_time",
    "validated",
    "extra_data",
}
_EXCLUDED_VALUE_COLUMNS = {"serial", "history_serial", "z_serial"}


# ---- the flag -------------------------------------------------------------------------------------


def classify_step(
    phi_start: float,
    pi_start: float,
    pi_end: float,
    sign_change: bool,
    phi_min: float,
    cap_fraction: float,
) -> dict:
    """
    P1's flag on one step's quantities: {"ratio": ..., "phi": ..., "flagged": ...}. Only a step that
    begins with pi > 0 can be flagged.
    """
    outward = pi_start > 0.0
    ratio = bool(
        outward and (pi_end / pi_start) < PI_RATIO_THRESHOLD and not sign_change
    )
    phi = bool(outward and phi_min < (1.0 - cap_fraction) * phi_start)
    return {"ratio": ratio, "phi": phi, "flagged": ratio or phi}


def step_samples(interpolant, t0: float, t1: float, n: int = FLAG_SAMPLES):
    """phi and pi of a step's interpolant at n equally spaced points, both ends included."""
    y = interpolant(np.linspace(t0, t1, n))
    return y[0], y[1]


# ---- the observer ---------------------------------------------------------------------------------


def _start_reason(frame, y0) -> str:
    """What started a Radau instance, from the frame that called new_solver (module docstring)."""
    if frame is None or frame.f_code.co_name != "new_solver":
        return "unknown"
    caller = frame.f_back
    if caller is None:
        return "unknown"
    name = caller.f_code.co_name
    if name == "resume_after_release":
        return "release"
    if name != "integrate_scalar_history":
        return "unknown"
    local = caller.f_locals
    if "solver" not in local:
        return "start"
    for key, reason in (
        ("y_retake", "retake"),
        ("y_reflected", "reflection"),
        ("y_drained", "drain"),
    ):
        if local.get(key) is y0:
            return reason
    return "unknown"


class Observer:
    """
    What the observed Radau instances report, over every integration of one history. `rows_mode`
    is "flagged" (keep only flagged rows), "first" (also every first outward step of an instance)
    or "all" (every outward step).
    """

    def __init__(
        self, cap_fraction: float, M: float, rows_mode="flagged", progress=None
    ):
        self.f = cap_fraction
        self.M = M
        self.rows_mode = rows_mode
        self.progress = progress
        self.integration = -1
        self.instances = []
        self.integrations = []
        self.rows = []
        self.outward = 0
        self.first_outward = 0
        self.first_call_outward = 0
        self.flagged_first = 0
        self.flagged_later = 0
        self.signature = 0
        self.signature_discarded = 0
        self.flag_ratio = 0
        self.flag_phi = 0
        self.flag_both = 0
        self.discarded = 0
        self.discarded_flagged = 0
        self.accepted = 0
        self.max_h_omega = 0.0
        self.max_h_omega_N = None
        self.max_h_omega_outward = 0.0
        self.sign_change_steps = 0
        self.least_ratio_unflagged = None
        self.greatest_ratio_flagged = None
        self.ratio_hist = [0] * (len(PI_RATIO_EDGES) + 1)
        self.h_omega_hist = [0] * (len(LOG10_H_OMEGA_EDGES) + 1)
        self.last_row = None
        self.N_reached = None

    # the instances
    def new_instance(self, solver, reason: str, first_step) -> int:
        index = len(self.instances)
        self.instances.append(
            {
                "integration": self.integration,
                "reason": reason,
                "N0": float(solver.t),
                "pi0": float(solver.y[1]),
                "first_step": None if first_step is None else float(first_step),
                "calls": 0,
                "steps": 0,
            }
        )
        if reason == "retake" and self.last_row is not None:
            # the guard discarded the step it retakes: the last outward step, from the same start
            row = self.last_row
            if row["N0"] == float(solver.t) and row["integration"] == self.integration:
                row["discarded"] = True
                self.discarded += 1
                if row["flagged"]:
                    # a discarded step leaves the history: it is counted apart
                    if row["first"]:
                        self.flagged_first -= 1
                    else:
                        self.flagged_later -= 1
                    self.discarded_flagged += 1
                if row["signature"]:
                    self.signature_discarded += 1
                self._keep(row, force=True)
        return index

    def before(self, solver):
        inst = self.instances[solver._observer_index]
        inst["calls"] += 1
        J10 = float(solver.J[1, 0]) if solver.J is not None else float("nan")
        return (
            float(solver.t),
            solver.y.copy(),
            float(solver.max_step),
            float(solver.h_abs),
            J10,
            -float(solver.f[1]),
            inst["calls"] == 1,
        )

    def after(self, solver, pre, message):
        t0, y0, cap, h_in, J10, a_in, first_call = pre
        if message is not None or not (solver.t != t0):
            return
        inst = self.instances[solver._observer_index]
        inst["steps"] += 1
        first = inst["steps"] == 1
        self.accepted += 1
        h = float(solver.t) - t0
        omega = sqrt(abs(J10)) if J10 == J10 else float("nan")
        h_omega = h * omega
        if h_omega > self.max_h_omega:
            self.max_h_omega = h_omega
            self.max_h_omega_N = t0
        self.N_reached = float(solver.t)
        if self.progress is not None:
            self.progress[0] += 1.0
            self.progress[1] = float(solver.t)
            self.progress[2] = float(self.integration)

        pi_start = float(y0[1])
        if not pi_start > 0.0:
            return

        phi_s, pi_s = step_samples(solver.dense_output(), t0, float(solver.t))
        pi_end = float(solver.y[1])
        sign_change = bool(np.any(pi_s <= 0.0))
        phi_min = float(np.min(phi_s))
        phi_start = float(y0[0])
        cls = classify_step(phi_start, pi_start, pi_end, sign_change, phi_min, self.f)
        ratio = pi_end / pi_start

        self.outward += 1
        if first:
            self.first_outward += 1
        if first_call:
            self.first_call_outward += 1
        if h_omega > self.max_h_omega_outward:
            self.max_h_omega_outward = h_omega
        if sign_change:
            self.sign_change_steps += 1
        else:
            self.ratio_hist[
                int(np.searchsorted(PI_RATIO_EDGES, ratio, side="right"))
            ] += 1
        if h_omega > 0.0:
            self.h_omega_hist[
                int(
                    np.searchsorted(
                        LOG10_H_OMEGA_EDGES, math.log10(h_omega), side="right"
                    )
                )
            ] += 1
        else:
            self.h_omega_hist[0] += 1

        # the ruled signature (README §6.3): the flag on the first step of a restarted instance
        signature = bool(cls["flagged"] and first and inst["reason"] in RESTART_REASONS)
        if signature:
            self.signature += 1

        if cls["flagged"]:
            if first:
                self.flagged_first += 1
            else:
                self.flagged_later += 1
            if cls["ratio"] and cls["phi"]:
                self.flag_both += 1
            elif cls["ratio"]:
                self.flag_ratio += 1
            else:
                self.flag_phi += 1
            if not sign_change and (
                self.greatest_ratio_flagged is None
                or ratio > self.greatest_ratio_flagged
            ):
                self.greatest_ratio_flagged = ratio
        elif not sign_change and (
            self.least_ratio_unflagged is None or ratio < self.least_ratio_unflagged
        ):
            self.least_ratio_unflagged = ratio

        row = {
            "integration": self.integration,
            "instance": solver._observer_index,
            "reason": inst["reason"],
            "first": first,
            "first_call": first_call,
            "N0": t0,
            "h": h,
            "h_omega": h_omega,
            "omega": omega,
            "phi_start": phi_start,
            "phi_start_over_M": phi_start / self.M,
            "pi_start": pi_start,
            "pi_end": pi_end,
            "pi_ratio": ratio,
            "sign_change": sign_change,
            "phi_min_over_M": phi_min / self.M,
            "phi_min_over_start": phi_min / phi_start,
            "cap": cap,
            "h_abs_in": h_in,
            "a_in": a_in,
            "ratio_condition": cls["ratio"],
            "phi_condition": cls["phi"],
            "flagged": cls["flagged"],
            "signature": signature,
            "discarded": False,
        }
        self.last_row = row
        self._keep(row)

    def _keep(self, row, force=False):
        keep = (
            force
            or self.rows_mode == "all"
            or row["flagged"]
            or (self.rows_mode == "first" and row["first"])
        )
        if keep and not any(r is row for r in self.rows[-2:]):
            self.rows.append(row)

    def summary(self) -> dict:
        return {
            "outward_steps": self.outward,
            "first_outward_steps": self.first_outward,
            "first_call_outward_steps": self.first_call_outward,
            "flagged_first": self.flagged_first,
            "flagged_later": self.flagged_later,
            "flagged": self.flagged_first + self.flagged_later,
            "flag_ratio_only": self.flag_ratio,
            "flag_phi_only": self.flag_phi,
            "flag_both": self.flag_both,
            "discarded_steps": self.discarded,
            "discarded_flagged_steps": self.discarded_flagged,
            "signature_kept": self.signature - self.signature_discarded,
            "signature_discarded": self.signature_discarded,
            "radau_accepted_steps": self.accepted,
            "max_h_omega": self.max_h_omega,
            "max_h_omega_N": self.max_h_omega_N,
            "max_h_omega_outward": self.max_h_omega_outward,
            "sign_change_outward_steps": self.sign_change_steps,
            "least_pi_ratio_unflagged": self.least_ratio_unflagged,
            "greatest_pi_ratio_flagged": self.greatest_ratio_flagged,
            "pi_ratio_edges": list(PI_RATIO_EDGES),
            "pi_ratio_hist": self.ratio_hist,
            "log10_h_omega_edges": list(LOG10_H_OMEGA_EDGES),
            "h_omega_hist": self.h_omega_hist,
            "instances": len(self.instances),
            "instance_reasons": {
                r: sum(1 for i in self.instances if i["reason"] == r)
                for r in sorted({i["reason"] for i in self.instances})
            },
        }


def _observed_radau_class(base, observer: Observer):
    class ObservedRadau(base):
        def __init__(self, fun, t0, y0, t_bound, **kwargs):
            reason = _start_reason(sys._getframe(1), y0)
            super().__init__(fun, t0, y0, t_bound, **kwargs)
            self._observer_index = observer.new_instance(
                self, reason, kwargs.get("first_step")
            )

        def step(self):
            pre = observer.before(self)
            message = super().step()
            observer.after(self, pre, message)
            return message

    return ObservedRadau


@contextlib.contextmanager
def observe(M: float, rows_mode: str = "flagged", progress=None):
    """
    For the duration of the block: the observed Radau in place of ComputeTargets.ScalarHistory's
    `Radau`, and its integrate_scalar_history wrapped to count integrations and keep their results.
    Yields the Observer. Both are restored on exit.
    """
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    observer = Observer(SM.StepControl().cap_fraction, M, rows_mode, progress)

    radau = SM.Radau
    integrate = SM.integrate_scalar_history

    def wrapped(*args, **kwargs):
        observer.integration += 1
        entry = {"index": observer.integration}
        observer.integrations.append(entry)
        try:
            result = integrate(*args, **kwargs)
        except Exception as e:
            entry["failure"] = str(getattr(e, "message", e))[:500]
            raise
        entry.update(
            accepted_steps=result.accepted_steps,
            nfev=result.nfev,
            reflections=len(result.reflections),
            injections=len(result.injections),
            N_final=float(result.N_final),
            restart_retakes=int(result.restart_retakes),
            parked=result.parking is not None,
            released=result.release is not None,
        )
        return result

    SM.Radau = _observed_radau_class(radau, observer)
    SM.integrate_scalar_history = wrapped
    try:
        yield observer
    finally:
        SM.Radau = radau
        SM.integrate_scalar_history = integrate


# ---- one history ----------------------------------------------------------------------------------


def _sha256(array) -> str:
    return hashlib.sha256(np.asarray(array, dtype=np.float64).tobytes()).hexdigest()


def z_grid(log10_high: Optional[float] = None):
    """main.py's sampling axis, 250 samples per decade of 1 + z, with store ids the index k."""
    from CosmologyConcepts import redshift, redshift_array
    from CosmologyConcepts.redshift import log_one_plus_z_grid
    from config.argument_parser import DEFAULT_LOG10_ONE_PLUS_Z_HIGH

    high = DEFAULT_LOG10_ONE_PLUS_Z_HIGH if log10_high is None else log10_high
    xs = log_one_plus_z_grid(SAMPLES_PER_LOG10_Z, high)
    return redshift_array(z_array=[redshift(i, x) for i, x in enumerate(xs)])


def run_history(spec: dict):
    """
    Run one history as tools/history_and_bbn.py does, with no BBN: tuned (spec["mode"] == "tuned")
    or untuned at spec["normalization"], spec["phi_ref_Mp"] ("fixed" or "store"). Returns the
    payload and the units.
    """
    from ComputeTargets.ScalarHistory import (
        DEFAULT_ABS_TOLERANCE,
        DEFAULT_REL_TOLERANCE,
        integrate_history,
        tune_history,
    )
    from CosmologyConcepts import M_value, beta_value, temperature
    from CosmologyConcepts.FieldValues import phi_value, pi_value
    from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
    from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
    from CosmologyModels.LambdaCDM import Planck2018
    from Units import Planck_units

    units = Planck_units()
    cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0,
        M_value(0, spec["M"] * units.PlanckMass),
        1,
        beta_value(0, spec["beta"]),
        units,
    )
    T_init = temperature(0, spec.get("T_init_GeV", T_INIT_GEV) * units.GeV)
    phi_init = phi_value(0, spec.get("phi_init_Mp", PHI_INIT_MP) * units.PlanckMass)
    pi_init = pi_value(0, PI_INIT)
    alpha = float(spec.get("alpha", 0.0))
    atol = spec.get("atol", DEFAULT_ABS_TOLERANCE)
    rtol = spec.get("rtol", DEFAULT_REL_TOLERANCE)
    grid = z_grid(spec.get("log10_z_high"))
    label = f"outward_step_scan-beta{spec['beta']:g}-M{spec['M']:g}-a{alpha:g}"

    if spec["mode"] == "tuned":
        kwargs = {}
        if "tuning_tolerance" in spec:
            kwargs["tolerance"] = spec["tuning_tolerance"]
        if "tuning_max_integrations" in spec:
            kwargs["max_integrations"] = spec["tuning_max_integrations"]
        data = tune_history(
            cosmology,
            model,
            T_init,
            phi_init,
            pi_init,
            grid,
            task_label=label,
            atol=atol,
            rtol=rtol,
            alpha=alpha,
            **kwargs,
        )
    else:
        if spec.get("T_stop_GeV") is None:
            T_stop = temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin)
        else:
            T_stop = temperature(1, spec["T_stop_GeV"] * units.GeV)
        data = integrate_history(
            cosmology,
            T_init,
            T_stop,
            phi_init,
            pi_init,
            grid,
            model.realize(spec["normalization"], spec["phi_ref_Mp"] * units.PlanckMass),
            task_label=label,
            atol=atol,
            rtol=rtol,
            alpha=alpha,
        )
    return data, units


def _json_float(x):
    if x is None:
        return None
    x = float(x)
    if math.isnan(x) or math.isinf(x):
        return repr(x)
    return x


def history_summary(data: dict, units) -> dict:
    """The per-history values of a payload (success or failure)."""
    from ComputeTargets.ScalarHistory import TUNING_OUTPUT_KEYS, dissipation_values

    if data.get("failure", False):
        return {
            "outcome": "failure",
            "failure_reason": str(data.get("failure_reason", "")),
        }

    out = {
        "outcome": "success",
        "accepted_steps": int(data["accepted_steps"]),
        "restart_retakes": int(data["restart_retakes"]),
        "RHS_evaluations": int(data["metadata"].RHS_evaluations),
        "compute_steps": int(data["metadata"].compute_steps),
        "reflections": int(data["reflections"]),
        "n_samples": len(data["sample"]),
        "samples_sha256": _sha256(data["sample"]),
        "log_one_plus_z_sha256": _sha256([z.log_one_plus_z for z in data["z_grid"]]),
    }
    values = dissipation_values(data)._asdict()
    for key in ("parking_log_T_Jordan", "release_log_T_Jordan"):
        if values[key] is not None:
            values[key] = values[key] - log(units.GeV)
    out["dissipation"] = {
        (
            "parking_log_T_Jordan_GeV"
            if k == "parking_log_T_Jordan"
            else "release_log_T_Jordan_GeV" if k == "release_log_T_Jordan" else k
        ): (_json_float(v) if isinstance(v, float) else v)
        for k, v in values.items()
    }
    bounce = data.get("first_bounce")
    out["first_bounce"] = (
        None
        if bounce is None
        else {
            "N": bounce.N,
            "phi_Einstein_Mp": bounce.phi_Einstein / units.PlanckMass,
            "log_T_Jordan_GeV": bounce.log_T_Jordan - log(units.GeV),
            "reflected": bool(bounce.reflected),
        }
    )
    fixed_T = data.get("fixed_T_values")
    if fixed_T is not None:
        out["fixed_T_values"] = {
            k: _json_float(v) for k, v in fixed_T._asdict().items()
        }
    if all(k in data for k in TUNING_OUTPUT_KEYS):
        out["tuning"] = {
            k: (_json_float(data[k]) if isinstance(data[k], float) else data[k])
            for k in TUNING_OUTPUT_KEYS
        }
    return out


def scan_one(spec: dict, rows_mode: str = "flagged", progress=None, keep=None) -> dict:
    """
    Run one task (a history spec, optionally with "store" and "post") under the observer; return its
    record. `keep`, if given, is a dict that receives the payload. A spec that names a probe variant
    (prompt 01's "variant" or "stiffness_c", other than None) is refused: the switch is gone.
    """
    for name in ("variant", "stiffness_c"):
        if spec.get(name) is not None:
            raise ValueError(
                f"outward_step_scan: the task names {name}={spec[name]!r}, but the probe switch was "
                f"removed by reflection-restart prompt 03; 01's tasks run only at d4703df"
            )
    record = {"key": spec.get("key"), "spec": spec}
    if spec.get("mode") == "stub":
        # a stand-in for a history, for the tests of the batch: "module:function" called on the spec
        module, name = spec["stub"].rsplit(":", 1)
        record.update(getattr(importlib.import_module(module), name)(spec))
        return record
    with contextlib.redirect_stdout(io.StringIO()) as out:
        start = time.perf_counter()
        with observe(spec["M"], rows_mode, progress) as observer:
            data, units = run_history(spec)
        wall = time.perf_counter() - start
    record["wall"] = wall
    record.update(history_summary(data, units))
    record["integrations"] = observer.integrations
    record["scan"] = observer.summary()
    record["rows"] = observer.rows
    record["stdout_tail"] = out.getvalue().splitlines()[-5:]
    if record["outcome"] == "success" and spec.get("store") is not None:
        record["comparison"] = compare_with_store(data, units, spec["store"])
    if spec.get("post"):
        record["post"] = _call_post(spec["post"], data, spec)
    if keep is not None:
        keep["data"] = data
    return record


def _call_post(target: str, data: dict, spec: dict):
    path, name = target.rsplit(":", 1)
    module_spec = importlib.util.spec_from_file_location("_scan_post", path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return getattr(module, name)(data, spec)


# ---- the store ------------------------------------------------------------------------------------


def store_shards(path: str):
    """The shard files of a store, PATH-shard*.db, for PATH with or without its .db."""
    base = path[:-3] if path.endswith(".db") else path
    shards = sorted(glob.glob(f"{base}-shard*.db"))
    if not shards:
        raise SystemExit(f"!! outward_step_scan: no shards {base}-shard*.db")
    return shards


def connect_ro(path: str) -> sqlite3.Connection:
    """A read-only connection (sqlite mode=ro), or SystemExit if the file cannot be opened so."""
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.execute("select count(*) from sqlite_master").fetchone()
    except sqlite3.Error as e:
        raise SystemExit(f"!! outward_step_scan: cannot open {path} read-only: {e}")
    conn.row_factory = sqlite3.Row
    return conn


def _M_over_Mp(value_eV: float, units) -> float:
    """
    M in units of M_P as the run gave it (a short decimal, --M-values-Mp): the stored value in eV
    converted back and rounded to 12 significant figures, which must agree with it to 1e-12.
    """
    exact = value_eV * units.eV / units.PlanckMass
    short = float(f"{exact:.12g}")
    if abs(short - exact) > 1e-12 * abs(exact):
        raise RuntimeError(f"M = {exact!r} M_P does not round to a short decimal")
    return short


def _tolerance(log10_tol: float) -> float:
    """A stored tolerance (its log10): the default's float exactly if it is the default's."""
    from ComputeTargets.ScalarHistory import (
        DEFAULT_ABS_TOLERANCE,
        DEFAULT_REL_TOLERANCE,
    )

    value = 10.0**log10_tol
    for default in (DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE):
        if abs(value - default) <= 1e-12 * default:
            return default
    return value


def select_from_store(
    path: str,
    M=None,
    beta=None,
    alpha=None,
    blocks=None,
    success_only: bool = False,
):
    """
    The histories of a store that pass the filters (each a list, or None for any), as specs for
    run_history in mode "store", each with its "store": {"shard", "serial"}.
    """
    from Units import Planck_units

    units = Planck_units()

    def match(x, values):
        return values is None or any(abs(x - v) <= 1e-9 * abs(v) for v in values)

    specs = []
    for shard in store_shards(path):
        conn = connect_ro(shard)
        rows = conn.execute(
            """select h.serial, h.failure, h.normalization, h.phi_ref, h.z_samples,
                      m.value_eV as M_eV, b.value as beta, a.value as alpha,
                      p.value_PlanckMass as phi_init_Mp, t.value_GeV as T_init_GeV,
                      ta.log10_tol as log10_atol, tr.log10_tol as log10_rtol
               from ScalarHistory h
               join ExponentialModel e on h.model_serial = e.serial
               join M_value m on e.M_serial = m.serial
               join beta_value b on e.beta_serial = b.serial
               join alpha_value a on h.alpha_serial = a.serial
               join phi_value p on h.phi_Einstein_init_serial = p.serial
               join temperature t on h.T_Jordan_init_serial = t.serial
               join tolerance ta on h.atol_serial = ta.serial
               join tolerance tr on h.rtol_serial = tr.serial
               order by h.serial"""
        ).fetchall()
        conn.close()
        for r in rows:
            M_Mp = _M_over_Mp(r["M_eV"], units)
            if success_only and r["failure"]:
                continue
            if not (
                match(M_Mp, M) and match(r["beta"], beta) and match(r["alpha"], alpha)
            ):
                continue
            if blocks is not None and not any(
                match(M_Mp, BLOCKS[b][0]) and BLOCKS[b][1](r["alpha"]) for b in blocks
            ):
                continue
            specs.append(
                {
                    "mode": "store",
                    "beta": r["beta"],
                    "M": M_Mp,
                    "alpha": r["alpha"],
                    "phi_init_Mp": r["phi_init_Mp"],
                    "T_init_GeV": r["T_init_GeV"],
                    "atol": _tolerance(r["log10_atol"]),
                    "rtol": _tolerance(r["log10_rtol"]),
                    "normalization": r["normalization"],
                    "phi_ref_Mp": r["phi_ref"],
                    "failure": bool(r["failure"]),
                    "store": {"shard": shard, "serial": r["serial"]},
                }
            )
    return specs


def _bits(x) -> bytes:
    return struct.pack("<d", float(x))


def _same(a, b) -> bool:
    """Bitwise for floats; equal for ints, bools (sqlite holds 0/1), strings; None only to None."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    if isinstance(a, float) or isinstance(b, float):
        return _bits(a) == _bits(b)
    return a == b


def _rel(a, b) -> float:
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return float("inf")
    if a == b:
        return 0.0
    scale = max(abs(a), abs(b))
    return abs(a - b) / scale if scale > 0.0 else float("inf")


def store_rows(data: dict, units):
    """
    The rows the factory's store would write for this payload: (the ScalarHistory row, the
    ScalarHistoryValue rows, the extra_data dict). The ScalarHistory object is populated as
    ScalarHistory.store() populates it from a payload, and the factory's own `store` converts it,
    with inserters that capture the rows (orchestrator note 01, ruling 2).
    """
    from ComputeTargets.ScalarHistory import (
        SampleValues,
        ScalarHistoryValue,
        build_extra_data,
        dissipation_values,
    )
    from ObjectFactories.ScalarHistory import sqla_ScalarHistoryFactory

    stub = SimpleNamespace(store_id=None, type_id=None)
    sample = data["sample"]
    ratios = data["rho_chi_over_rho_rad"]
    values = [
        ScalarHistoryValue(
            None,
            z,
            rho_chi_over_rho_rad=ratios[i],
            **SampleValues._make(tuple(sample[i]))._asdict(),
        )
        for i, z in enumerate(data["z_grid"])
    ]
    obj = SimpleNamespace(
        label=None,
        cosmology=stub,
        model=stub,
        T_Jordan_init=stub,
        phi_Einstein_init=stub,
        pi_Einstein_init=stub,
        _atol=stub,
        _rtol=stub,
        tuning_tol=stub,
        alpha=stub,
        _failure=False,
        _failure_reason=None,
        solver=stub,
        _values=values,
        metadata=data["metadata"],
        _extra_data=build_extra_data(data),
        _first_bounce=data["first_bounce"],
        _fixed_T_values=data["fixed_T_values"],
        _tuning=None,
        _dissipation=dissipation_values(data),
        _units=units,
        tags=[],
    )
    captured = {"history": None, "values": []}

    def history_inserter(conn, row):
        captured["history"] = dict(row)
        return 0

    def value_inserter(conn, row):
        captured["values"].append(dict(row))
        return len(captured["values"])

    sqla_ScalarHistoryFactory().store(
        obj,
        None,
        None,
        history_inserter,
        None,
        {"ScalarHistory_tags": None, "ScalarHistoryValue": value_inserter},
    )
    # each value's z carries its store id, which the factory writes as z_serial
    labels = {z.store_id: z.log_one_plus_z for z in data["z_grid"]}
    for row in captured["values"]:
        row["log_one_plus_z"] = labels[row["z_serial"]]
    return captured["history"], captured["values"], obj._extra_data


def _difference(name, a, b, at, where):
    entry = where.setdefault(name, {"count": 0, "max_rel": 0.0, "first": None})
    entry["count"] += 1
    r = _rel(a, b)
    if r > entry["max_rel"]:
        entry["max_rel"] = r
    if entry["first"] is None:
        entry["first"] = dict(
            at,
            stored=_json_float(a) if isinstance(a, float) else a,
            new=_json_float(b) if isinstance(b, float) else b,
        )


def compare_with_store(data: dict, units, store: dict) -> dict:
    """
    The payload against the stored history store["serial"] in store["shard"], read-only (module
    docstring; orchestrator note 01, ruling 2). Returns {"bitwise": bool, ...}.
    """
    from ComputeTargets.ScalarHistory import TUNING_OUTPUT_KEYS

    history, values, extra = store_rows(data, units)
    conn = connect_ro(store["shard"])
    try:
        stored = dict(
            conn.execute(
                "select * from ScalarHistory where serial = ?", (store["serial"],)
            ).fetchone()
        )
        stored_values = [
            dict(r)
            for r in conn.execute(
                """select v.*, z.log_one_plus_z as log_one_plus_z
                   from ScalarHistoryValue v join redshift z on v.z_serial = z.serial
                   where v.history_serial = ? order by v.raw_N""",
                (store["serial"],),
            ).fetchall()
        ]
    finally:
        conn.close()

    excluded = _EXCLUDED_HISTORY_COLUMNS | set(TUNING_OUTPUT_KEYS)
    history_diff = {}
    for name, new in history.items():
        if name in excluded:
            continue
        if name not in stored:
            history_diff[name] = {"missing_in_store": True}
            continue
        if not _same(stored[name], new):
            _difference(name, stored[name], new, {}, history_diff)

    extra_diff = {}
    stored_extra = json.loads(stored["extra_data"]) if stored["extra_data"] else {}
    new_extra = json.loads(json.dumps(extra)) if extra else {}
    common = sorted(set(stored_extra) & set(new_extra))
    for key in common:
        if stored_extra[key] != new_extra[key]:
            extra_diff[key] = {"stored": stored_extra[key], "new": new_extra[key]}
    only = {
        "store": sorted(set(stored_extra) - set(new_extra)),
        "new": sorted(set(new_extra) - set(stored_extra)),
    }

    by_label = {}
    for row in values:
        by_label[_bits(row["log_one_plus_z"])] = row
    value_diff = {}
    unmatched_stored = 0
    matched = set()
    for srow in stored_values:
        key = _bits(srow["log_one_plus_z"])
        nrow = by_label.get(key)
        if nrow is None:
            unmatched_stored += 1
            continue
        matched.add(key)
        at = {"log_one_plus_z": srow["log_one_plus_z"], "raw_N": srow["raw_N"]}
        for name, s in srow.items():
            if name in _EXCLUDED_VALUE_COLUMNS or name == "log_one_plus_z":
                continue
            if name not in nrow:
                value_diff.setdefault(name, {"missing_in_new": True})
                continue
            if not _same(s, nrow[name]):
                _difference(name, s, nrow[name], at, value_diff)
    unmatched_new = len(values) - len(matched)

    bitwise = (
        not history_diff
        and not extra_diff
        and not value_diff
        and unmatched_stored == 0
        and unmatched_new == 0
        and len(stored_values) == len(values)
    )
    return {
        "bitwise": bitwise,
        "stored_samples": len(stored_values),
        "new_samples": len(values),
        "unmatched_stored": unmatched_stored,
        "unmatched_new": unmatched_new,
        "history_columns": history_diff,
        "value_columns": value_diff,
        "extra_data": extra_diff,
        "extra_data_only_in": only,
        "stored_accepted_steps": stored_extra.get("accepted_steps"),
        "stored_compute_time": stored.get("compute_time"),
    }


# ---- the batch ------------------------------------------------------------------------------------


def task_key(spec: dict) -> str:
    if spec.get("key"):
        return spec["key"]
    where = (
        f"store={Path(spec['store']['shard']).name}#{spec['store']['serial']}"
        if spec.get("store")
        else spec["mode"]
    )
    return f"beta={spec['beta']!r} M={spec['M']!r} alpha={spec.get('alpha', 0.0)!r} {where}"


def _worker(spec, rows_mode, progress, conn):
    try:
        record = scan_one(spec, rows_mode, progress)
        record["status"] = "done"
    except BaseException:
        record = {
            "key": spec["key"],
            "spec": spec,
            "status": "error",
            "traceback": traceback.format_exc(),
        }
    conn.send(record)
    conn.close()


def _json_default(o):
    """numpy values in a record (a 0-d array from an interpolant, a numpy scalar) as Python ones."""
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, np.generic):
        return o.item()
    raise TypeError(f"{type(o).__name__} is not JSON serializable")


def _done_keys(output: Path):
    keys = set()
    if output.exists():
        with output.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        keys.add(json.loads(line)["key"])
                    except (ValueError, KeyError):
                        pass
    return keys


def _log(log_path: Path, text: str):
    with log_path.open("a") as f:
        f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {text}\n")
        f.flush()


def run_batch(
    tasks,
    output: str,
    jobs: int = DEFAULT_JOBS,
    timeout: float = DEFAULT_TIMEOUT,
    rows_mode: str = "flagged",
    poll: float = 0.2,
):
    """Run the tasks, one worker process each, at most `jobs` at a time (module docstring)."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    log_path = output.with_name(output.name + ".log")
    output.with_name(output.name + ".pid").write_text(f"{os.getpid()}\n")

    for spec in tasks:
        spec["key"] = task_key(spec)
    keys = [s["key"] for s in tasks]
    if len(set(keys)) != len(keys):
        raise SystemExit("!! outward_step_scan: two tasks have the same key")
    done = _done_keys(output)
    pending = [s for s in tasks if s["key"] not in done]
    _log(
        log_path,
        f"** scan start: {len(tasks)} tasks, {len(tasks) - len(pending)} already done, "
        f"jobs={jobs} timeout={timeout:g} s pid={os.getpid()}",
    )

    ctx = multiprocessing.get_context("spawn")
    running = {}
    with output.open("a") as out:

        def write(record):
            try:
                text = json.dumps(record, default=_json_default)
            except (TypeError, ValueError):
                text = json.dumps(
                    {
                        "key": record.get("key"),
                        "spec": record.get("spec"),
                        "status": "error",
                        "traceback": "the record could not be written as JSON:\n"
                        + traceback.format_exc(),
                    },
                    default=repr,
                )
            out.write(text + "\n")
            out.flush()

        while pending or running:
            while pending and len(running) < jobs:
                spec = pending.pop(0)
                progress = ctx.RawArray("d", 3)
                receiver, sender = ctx.Pipe(duplex=False)
                proc = ctx.Process(
                    target=_worker,
                    args=(spec, rows_mode, progress, sender),
                    daemon=True,
                )
                proc.start()
                sender.close()
                running[spec["key"]] = (
                    proc,
                    receiver,
                    time.monotonic(),
                    progress,
                    spec,
                )

            for key in list(running):
                proc, receiver, start, progress, spec = running[key]
                record = None
                if receiver.poll():
                    try:
                        record = receiver.recv()
                    except EOFError:
                        record = None
                    proc.join()
                    if record is None:
                        record = {
                            "key": key,
                            "spec": spec,
                            "status": "error",
                            "traceback": f"worker exited with code {proc.exitcode} "
                            f"and sent nothing",
                        }
                elif not proc.is_alive():
                    proc.join()
                    record = {
                        "key": key,
                        "spec": spec,
                        "status": "error",
                        "traceback": f"worker exited with code {proc.exitcode} "
                        f"and sent nothing",
                    }
                elif time.monotonic() - start > timeout:
                    proc.terminate()
                    proc.join()
                    record = {
                        "key": key,
                        "spec": spec,
                        "status": "timeout",
                        "outcome": "timeout",
                        "wall": time.monotonic() - start,
                        "radau_accepted_steps_reached": int(progress[0]),
                        "N_reached": progress[1],
                        "integration_reached": int(progress[2]),
                    }
                if record is not None:
                    record.setdefault("wall_batch", time.monotonic() - start)
                    write(record)
                    del running[key]
                    _log(
                        log_path,
                        f"{record.get('status')} {record.get('outcome', '')} "
                        f"{time.monotonic() - start:.1f} s {key}",
                    )
            time.sleep(poll)

    _log(log_path, "** scan done")


# ---- the command line -----------------------------------------------------------------------------


def _parser():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    one = sub.add_parser("one", help="scan one history and print its record as JSON")
    one.add_argument("beta", type=float)
    one.add_argument("M", type=float, help="M in units of M_P")
    one.add_argument("--alpha", type=float, default=0.0)
    one.add_argument("--phi-init-Mp", type=float, default=PHI_INIT_MP)
    one.add_argument(
        "--fixed-normalization",
        type=float,
        nargs=2,
        default=None,
        metavar=("NORM", "PHI_REF_MP"),
    )
    one.add_argument("--T-stop-GeV", type=float, default=None)
    one.add_argument("--rows", choices=("flagged", "first", "all"), default="flagged")

    batch = sub.add_parser("batch", help="scan many histories, one JSON line each")
    batch.add_argument("--output", required=True)
    source = batch.add_mutually_exclusive_group(required=True)
    source.add_argument("--tasks", help="a JSON list of task specs")
    source.add_argument("--from-store", help="a store, PATH or PATH.db, read-only")
    batch.add_argument("--M", type=float, nargs="+", default=None)
    batch.add_argument("--beta", type=float, nargs="+", default=None)
    batch.add_argument("--alpha", type=float, nargs="+", default=None)
    batch.add_argument("--block", choices=sorted(BLOCKS), nargs="+", default=None)
    batch.add_argument("--success-only", action="store_true")
    batch.add_argument("--jobs", type=int, default=DEFAULT_JOBS)
    batch.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    batch.add_argument("--rows", choices=("flagged", "first", "all"), default="flagged")
    return parser


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    if not Path(os.getcwd(), "CosmologyModels").is_dir():
        print("!! outward_step_scan: run from the repository root", file=sys.stderr)
        return 2

    if args.command == "one":
        spec = {
            "beta": args.beta,
            "M": args.M,
            "alpha": args.alpha,
            "phi_init_Mp": args.phi_init_Mp,
        }
        if args.fixed_normalization is None:
            spec["mode"] = "tuned"
        else:
            spec.update(
                mode="fixed",
                normalization=args.fixed_normalization[0],
                phi_ref_Mp=args.fixed_normalization[1],
                T_stop_GeV=args.T_stop_GeV,
            )
        spec["key"] = task_key(spec)
        print(json.dumps(scan_one(spec, args.rows), indent=1, default=_json_default))
        return 0

    if args.tasks:
        tasks = json.loads(Path(args.tasks).read_text())
    else:
        tasks = select_from_store(
            args.from_store,
            M=args.M,
            beta=args.beta,
            alpha=args.alpha,
            blocks=args.block,
            success_only=args.success_only,
        )
    run_batch(tasks, args.output, args.jobs, args.timeout, args.rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
