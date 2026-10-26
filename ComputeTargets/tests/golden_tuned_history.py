"""
The tuned golden history (jordan-normalization prompt 06, T7; README §6.2 P3; orchestrator note
06, ruling 12): the setup and the record of one tuned history, shared by test_tuning.py and by a
regeneration. It sits beside golden_history.py, from which it takes record_of, first_difference,
environment and _hex.

One history is tuned as test_tuning.py tunes it, through tune_history with no Ray:
QCD_Cosmology on Planck2018 in Planck_units; ExponentialModel(M = 0.01 M_P, n = 1, beta = 3);
phi* = 5 M_P, pi* = 0, T* = 2e4 GeV; atol = rtol = 1e-8; the tuning tolerance 1e-5 and the cap of
30 integrations (the defaults); and main.py's default sampling axis, log_one_plus_z_grid(250, 300).
The tuning starts from the model's own seed and integrates to T_J = T_CMB until both residuals
are inside the tolerance: five integrations, about 25 s on an unloaded machine.

What is pinned:
  - "history": the record of the final integration's payload, by golden_history.record_of (the
    SHA-256 of the samples and of their ln(1 + z), the counts, the first bounce, the
    fixed-temperature values, the step-control scalars and the excerpts), compared by
    golden_history.first_difference;
  - "tuning": every key tune_history adds to the payload (ScalarHistory.TUNING_OUTPUT_KEYS),
    floats as float.hex, ints and strings as they are, compared exactly.

NOT pinned: the wall-clock fields of the payload's metadata (golden_history.EXCLUDED_METADATA),
which record_of leaves out.

Regenerate (from the repository root; the JSON is replaced, so do it only deliberately, in a prompt
that changes values, and log the change):

    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_tuned_history

This file's name does not match test*.py, so discovery does not run it.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
    TUNING_OUTPUT_KEYS,
    tune_history,
)
from ComputeTargets.tests.golden_history import (
    EXCLUDED_METADATA,
    _hex,
    environment,
    first_difference,
    record_of,
)
from CosmologyConcepts import M_value, beta_value, redshift, redshift_array, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.redshift import log_one_plus_z_grid
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

GOLDEN_TUNED_PATH = (
    Path(__file__).resolve().parent / "data" / "golden_history_06_tuned.json"
)
REGENERATE_COMMAND = (
    "PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_tuned_history"
)

BETA = 3.0
M_OVER_MP = 1.0e-2
N_EXPONENT = 1
PHI_INIT_OVER_MP = 5.0
PI_INIT = 0.0
T_INIT_GEV = 2.0e4
ATOL = 1.0e-8
RTOL = 1.0e-8
TOLERANCE = DEFAULT_TUNING_TOLERANCE
MAX_INTEGRATIONS = DEFAULT_TUNING_MAX_INTEGRATIONS
SAMPLES_PER_LOG10_Z = 250
LOG10_ONE_PLUS_Z_HIGH = 300
TASK_LABEL = "golden_tuned_history"


def make_cosmology():
    """QCD_Cosmology on Planck2018, in Planck_units."""
    return QCD_Cosmology(0, Planck_units(), Planck2018())


def make_model(units, beta: float = BETA, M_over_Mp: float = M_OVER_MP, cls=None):
    """The exponential model at (M, n = 1, beta), with store ids 0; `cls` may be a subclass."""
    cls = ExponentialModel if cls is None else cls
    return cls(
        0,
        M_value(0, M_over_Mp * units.PlanckMass),
        N_EXPONENT,
        beta_value(0, beta),
        units,
    )


def make_z_grid() -> redshift_array:
    """main.py's default sampling axis, with dummy store ids (the index k)."""
    xs = log_one_plus_z_grid(SAMPLES_PER_LOG10_Z, LOG10_ONE_PLUS_Z_HIGH)
    return redshift_array(z_array=[redshift(i, x) for i, x in enumerate(xs)])


def run_tuning(cosmology=None, model=None, z_grid=None, **kwargs) -> dict:
    """
    Tune one history at the golden setup and return tune_history's payload (which may be a
    failure). `model` defaults to make_model(units); `kwargs` go to tune_history.
    """
    cosmology = make_cosmology() if cosmology is None else cosmology
    units = cosmology.units
    model = make_model(units) if model is None else model
    z_grid = make_z_grid() if z_grid is None else z_grid

    arguments = dict(
        task_label=TASK_LABEL,
        atol=ATOL,
        rtol=RTOL,
        tolerance=TOLERANCE,
        max_integrations=MAX_INTEGRATIONS,
    )
    arguments.update(kwargs)
    return tune_history(
        cosmology,
        model,
        temperature(0, T_INIT_GEV * units.GeV),
        phi_value(0, PHI_INIT_OVER_MP * units.PlanckMass),
        pi_value(0, PI_INIT),
        z_grid,
        **arguments,
    )


def tuning_record(data: dict) -> dict:
    """The keys tune_history adds, floats as float.hex, ints and strings as they are."""
    record = {}
    for key in TUNING_OUTPUT_KEYS:
        value = data[key]
        record[key] = _hex(value) if isinstance(value, float) else value
    return record


def build_record(data: dict) -> dict:
    """The pinned record of one tuned history (its payload must not be a failure)."""
    if data.get("failure", False):
        raise RuntimeError(
            f"the tuned golden history failed: {data.get('failure_reason')}"
        )
    return {"history": record_of(data), "tuning": tuning_record(data)}


def differences(stored: dict, current: dict):
    """
    Compare a stored record with a current one: golden_history.first_difference for the history,
    and each tuning key exactly. Returns a list of messages, empty if they agree.
    """
    messages = list(first_difference(stored["history"], current["history"]))
    for key in sorted(set(stored["tuning"]) | set(current["tuning"])):
        if stored["tuning"].get(key) != current["tuning"].get(key):
            messages.append(
                f"tuning key {key} differs: recorded {stored['tuning'].get(key)!r}, "
                f"now {current['tuning'].get(key)!r}"
            )
    return messages


def main() -> int:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    start = time.perf_counter()
    record = build_record(run_tuning())
    elapsed = time.perf_counter() - start

    document = {
        "provenance": {
            "generated_at_commit": commit,
            "code_tree": (
                "jordan-normalization prompt 06's working tree, before its commit, on top "
                "of generated_at_commit"
            ),
            "generator": "ComputeTargets/tests/golden_tuned_history.py",
            "regenerate": REGENERATE_COMMAND,
            "environment": environment(),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "generation_seconds": round(elapsed, 1),
            "setup": {
                "beta": BETA,
                "M_over_M_P": M_OVER_MP,
                "n": N_EXPONENT,
                "phi_init_over_M_P": PHI_INIT_OVER_MP,
                "pi_init": PI_INIT,
                "T_init_GeV": T_INIT_GEV,
                "T_stop": "cosmology.T_CMB_Kelvin * units.Kelvin",
                "atol": ATOL,
                "rtol": RTOL,
                "tolerance": TOLERANCE,
                "max_integrations": MAX_INTEGRATIONS,
                "samples_per_log10_z": SAMPLES_PER_LOG10_Z,
                "log10_one_plus_z_high": LOG10_ONE_PLUS_Z_HIGH,
                "seed": "model.seed(cosmology)",
            },
            "not_pinned": [f"metadata.{n}" for n in EXCLUDED_METADATA],
            "not_pinned_reason": "wall-clock measurements that differ between runs",
        },
        **record,
    }
    GOLDEN_TUNED_PATH.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(f"wrote {GOLDEN_TUNED_PATH} at {commit} in {elapsed:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
