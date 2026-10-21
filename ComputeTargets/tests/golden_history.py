"""
The golden histories: the setup, and the record of one history, shared by the test
(test_golden_history.py) and by a regeneration. (jordan-normalization prompt 01, X1.)

Two histories are run as tools/history_and_bbn.py runs one, through
compute_scalar_history._function with no Ray: QCD_Cosmology on Planck2018 in Planck_units;
ExponentialPotential(M = 1e-3 M_P, Lambda = 1 meV, n = 1); ExponentialCoupling(beta) for beta = 1.5
and beta = 5.5; phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, T_stop = 1 MeV; atol = rtol = 1e-8; and the
production z grid, built as main.py builds it (250 samples per decade of 1 + z, up to 1e85,
`num = int(round(250 * 85 + 0.5, 0))`, which is 21250 points; reproduced, not corrected).

What is pinned, per history (see `record_of`):
  - samples_sha256: SHA-256 of numpy.asarray(samples, dtype=float64).tobytes(), row-major in
    SampleValues order; z_sha256: the same for the sampled z values; n_samples; the field names of
    SampleValues, in order;
  - reflections, accepted_steps, steps_rejected_by_exception, metadata.compute_steps and
    metadata.RHS_evaluations (ints);
  - first_bounce (N, phi_Einstein, log_T_Jordan as float.hex; reflected as a bool; null if None);
    fixed_T_values (four fields, float.hex or null); cap_fraction, cap_floor, cap_global_max_step,
    jacobian_factor_max (float.hex); solver_label; the three RHS-statistics blocks (null, since full
    statistics are not collected);
  - the excerpts: the samples at indices 0, 250, 500, ... and the last, each as its index, its z
    and its fields by name, all float.hex.

NOT pinned: metadata.compute_time, mean_RHS_time, max_RHS_time and min_RHS_time. They are wall-clock
measurements and differ from run to run.

Regenerate (from the repository root; the JSON is replaced, so do it only deliberately, in a prompt
that changes values, and log the change):

    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history

This file's name does not match test*.py, so discovery does not run it.
"""

import hashlib
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import scipy

from ComputeTargets.ScalarHistory import SampleValues, compute_scalar_history
from CosmologyConcepts import (
    Lambda_value,
    M_value,
    beta_value,
    redshift,
    redshift_array,
    temperature,
)
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
    ExponentialCoupling,
)
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

GOLDEN_PATH = Path(__file__).resolve().parent / "data" / "golden_history_01.json"
REGENERATE_COMMAND = (
    "PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history"
)

BETAS = (1.5, 5.5)
M_OVER_MP = 1.0e-3
LAMBDA_OVER_EV = 1.0e-3
N_EXPONENT = 1
PHI_INIT_OVER_MP = 5.0
PI_INIT = 0.0
T_INIT_GEV = 2.0e4
T_STOP_GEV = 1.0e-3
ATOL = 1.0e-8
RTOL = 1.0e-8
SAMPLES_PER_LOG10_Z = 250
LOG10_ONE_PLUS_Z_LOW = 0
LOG10_ONE_PLUS_Z_HIGH = 85
EXCERPT_STRIDE = 250

EXCLUDED_METADATA = (
    "compute_time",
    "mean_RHS_time",
    "max_RHS_time",
    "min_RHS_time",
)


def history_key(beta: float) -> str:
    return f"beta={beta:g}"


def make_z_grid() -> redshift_array:
    """main.py's arithmetic, unchanged, with dummy store ids (the index)."""
    num = int(
        round(
            SAMPLES_PER_LOG10_Z * (LOG10_ONE_PLUS_Z_HIGH - LOG10_ONE_PLUS_Z_LOW) + 0.5,
            0,
        )
    )
    zs = np.logspace(LOG10_ONE_PLUS_Z_LOW, LOG10_ONE_PLUS_Z_HIGH, num) - 1.0
    return redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])


def run_history(beta: float) -> dict:
    """Run one golden history; the payload of compute_scalar_history, which must not be a failure."""
    units = Planck_units()
    params = Planck2018()
    cosmology = QCD_Cosmology(0, units, params)

    potential = ExponentialPotential(
        0,
        M_value(0, M_OVER_MP * units.PlanckMass),
        Lambda_value(0, LAMBDA_OVER_EV * units.eV),
        N_EXPONENT,
        units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), units)

    data = compute_scalar_history._function(
        cosmology,
        temperature(0, T_INIT_GEV * units.GeV),
        temperature(1, T_STOP_GEV * units.GeV),
        phi_value(0, PHI_INIT_OVER_MP * units.PlanckMass),
        pi_value(0, PI_INIT),
        make_z_grid(),
        potential,
        coupling,
        task_label=f"golden_history-{history_key(beta)}",
        atol=ATOL,
        rtol=RTOL,
    )
    if data.get("failure", False):
        raise RuntimeError(
            f"golden history {history_key(beta)} failed: {data.get('failure_reason')}"
        )
    return data


def _hex(x):
    return None if x is None else float(x).hex()


def _sha256(array) -> str:
    return hashlib.sha256(np.asarray(array, dtype=np.float64).tobytes()).hexdigest()


def record_of(data: dict) -> dict:
    """The pinned record of one history (the payload of compute_scalar_history)."""
    samples = data["sample"]
    zs = [r.z for r in data["z_grid"]]
    names = list(SampleValues._fields)

    indices = list(range(0, len(samples), EXCERPT_STRIDE))
    if indices[-1] != len(samples) - 1:
        indices.append(len(samples) - 1)
    excerpts = [
        {
            "index": i,
            "z": _hex(zs[i]),
            "fields": {n: _hex(v) for n, v in zip(names, samples[i])},
        }
        for i in indices
    ]

    bounce = data["first_bounce"]
    fixed_T = data["fixed_T_values"]
    metadata = data["metadata"]

    for key in ("largest_RHS_values", "smallest_RHS_values", "mean_RHS_values"):
        assert data[key] is None, f"{key} is expected to be None (no full statistics)"

    return {
        "samples_sha256": _sha256(samples),
        "z_sha256": _sha256(zs),
        "n_samples": len(samples),
        "sample_fields": names,
        "reflections": int(data["reflections"]),
        "accepted_steps": int(data["accepted_steps"]),
        "steps_rejected_by_exception": int(data["steps_rejected_by_exception"]),
        "compute_steps": int(metadata.compute_steps),
        "RHS_evaluations": int(metadata.RHS_evaluations),
        "first_bounce": (
            None
            if bounce is None
            else {
                "N": _hex(bounce.N),
                "phi_Einstein": _hex(bounce.phi_Einstein),
                "log_T_Jordan": _hex(bounce.log_T_Jordan),
                "reflected": bool(bounce.reflected),
            }
        ),
        "fixed_T_values": {n: _hex(v) for n, v in zip(fixed_T._fields, fixed_T)},
        "cap_fraction": _hex(data["cap_fraction"]),
        "cap_floor": _hex(data["cap_floor"]),
        "cap_global_max_step": _hex(data["cap_global_max_step"]),
        "jacobian_factor_max": _hex(data["jacobian_factor_max"]),
        "solver_label": data["solver_label"],
        "largest_RHS_values": None,
        "smallest_RHS_values": None,
        "mean_RHS_values": None,
        "excerpts": excerpts,
    }


def environment() -> dict:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "numpy": np.__version__,
        "scipy": scipy.__version__,
    }


def build_record() -> dict:
    """Run both histories and return the whole golden record (without provenance)."""
    return {history_key(beta): record_of(run_history(beta)) for beta in BETAS}


def first_difference(stored: dict, current: dict):
    """
    Compare one history's stored record with a current one. Returns a list of messages, empty if
    they agree. Excerpts are searched first so that the first differing sample and field is named.
    """
    messages = []

    stored_ex = {e["index"]: e for e in stored["excerpts"]}
    current_ex = {e["index"]: e for e in current["excerpts"]}
    for index in sorted(set(stored_ex) | set(current_ex)):
        if index not in stored_ex or index not in current_ex:
            messages.append(f"excerpt index {index} is present in only one record")
            continue
        s, c = stored_ex[index], current_ex[index]
        if s["z"] != c["z"]:
            messages.append(
                f"sample {index}: z differs: recorded {s['z']}, now {c['z']}"
            )
        for name in sorted(set(s["fields"]) | set(c["fields"])):
            if s["fields"].get(name) != c["fields"].get(name):
                messages.append(
                    f"sample {index}: field {name} differs: recorded "
                    f"{s['fields'].get(name)}, now {c['fields'].get(name)}"
                )
                break
        if messages:
            break

    for key in stored:
        if key == "excerpts":
            continue
        if stored[key] != current.get(key):
            messages.append(
                f"{key} differs: recorded {stored[key]!r}, now {current.get(key)!r}"
            )

    if not messages:
        return messages
    if all("sample " not in m for m in messages) and any(
        "sha256" in m for m in messages
    ):
        messages.append("every excerpt agrees: the difference lies between excerpts")
    return messages


def main() -> int:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    start = time.perf_counter()
    histories = build_record()
    elapsed = time.perf_counter() - start

    record = {
        "provenance": {
            "generated_at_commit": commit,
            "code_tree": (
                "the code tree of ac187c8: git diff --stat ac187c8 HEAD -- . "
                "':!prompts' ':!.documents' was empty at generation"
            ),
            "generator": "ComputeTargets/tests/golden_history.py",
            "regenerate": REGENERATE_COMMAND,
            "environment": environment(),
            "generation_seconds": round(elapsed, 1),
            "setup": {
                "betas": list(BETAS),
                "M_over_M_P": M_OVER_MP,
                "Lambda_over_eV": LAMBDA_OVER_EV,
                "n": N_EXPONENT,
                "phi_init_over_M_P": PHI_INIT_OVER_MP,
                "pi_init": PI_INIT,
                "T_init_GeV": T_INIT_GEV,
                "T_stop_GeV": T_STOP_GEV,
                "atol": ATOL,
                "rtol": RTOL,
                "samples_per_log10_z": SAMPLES_PER_LOG10_Z,
                "log10_one_plus_z_high": LOG10_ONE_PLUS_Z_HIGH,
                "excerpt_stride": EXCERPT_STRIDE,
            },
            "not_pinned": [f"metadata.{n}" for n in EXCLUDED_METADATA],
            "not_pinned_reason": "wall-clock measurements that differ between runs",
        },
        "histories": histories,
    }
    GOLDEN_PATH.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    print(f"wrote {GOLDEN_PATH} at {commit} in {elapsed:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
