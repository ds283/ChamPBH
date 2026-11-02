"""
The reflecting golden history (reflection-restart prompt 03 §1.6; README §6.2 P5; orchestrator note 03,
ruling 3): the setup and the record of one tuned history that reflects, shared by
test_golden_reflecting_history.py and by a regeneration. It is built on golden_tuned_history's builders
(make_cosmology, make_model, make_z_grid, run_tuning, build_record, differences), with its own point.

The point is the diagnosis's first failure (.documents/post-reflection-step-2026-11-02/): beta = 1.5,
M = 1e-12 M_P, alpha = 0.05, n = 1, tuned. Everything else is golden_tuned_history's setup: QCD_Cosmology on
Planck2018 in Planck_units; phi* = 5 M_P, pi* = 0, T* = 2e4 GeV; atol = rtol = 1e-8; the tuning tolerance
1e-5 and the cap of 30 integrations; main.py's default sampling axis, log_one_plus_z_grid(250, 300). Before
reflection-restart prompt 03 the tuning's seed integration failed on the gate's m_eff^2 (the uncapped
outward step after the reflection at N = 35.914291). Since that prompt the Radau restarted after every
reflection is given F3's first step and its first step is guarded; the history succeeds in one
integration, with 32 floor reflections (the last parks it, before T_CMB), 7183 accepted steps and no
retake. About 4-6 s unloaded.

What is pinned: golden_tuned_history.build_record's record (the history's record_of, which holds the
reflection count, accepted_steps and restart_retakes, expected 0; and every tuning key), compared by
golden_tuned_history.differences. The record also states, under "reflecting", the counts a reader of this
history looks at first: reflections, restart_retakes, accepted_steps and the injections.

Regenerate (from the repository root; the JSON is replaced, so do it only deliberately, in a prompt that
changes values, and log the change):

    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_reflecting_history

This file's name does not match test*.py, so discovery does not run it.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests.golden_history import EXCLUDED_METADATA, environment

GOLDEN_REFLECTING_PATH = (
    Path(__file__).resolve().parent / "data" / "golden_history_rr03_reflecting.json"
)
REGENERATE_COMMAND = (
    "PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_reflecting_history"
)

BETA = 1.5
M_OVER_MP = 1.0e-12
ALPHA = 0.05
TASK_LABEL = "golden_reflecting_history"


def run_tuning() -> dict:
    """Tune the reflecting golden history; tune_history's payload (which may be a failure)."""
    cosmology = gth.make_cosmology()
    return gth.run_tuning(
        cosmology=cosmology,
        model=gth.make_model(cosmology.units, BETA, M_OVER_MP),
        task_label=TASK_LABEL,
        alpha=ALPHA,
    )


def build_record(data: dict) -> dict:
    """golden_tuned_history's record of the payload, with the reflecting counts beside it."""
    record = gth.build_record(data)
    record["reflecting"] = {
        "reflections": int(data["reflections"]),
        "restart_retakes": int(data["restart_retakes"]),
        "accepted_steps": int(data["accepted_steps"]),
        "injections": len(data["injections"]),
    }
    return record


def differences(stored: dict, current: dict):
    """golden_tuned_history.differences, and the reflecting counts exactly."""
    messages = list(gth.differences(stored, current))
    if stored["reflecting"] != current["reflecting"]:
        messages.append(
            f"reflecting differs: recorded {stored['reflecting']!r}, now "
            f"{current['reflecting']!r}"
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
                "reflection-restart prompt 03's working tree, before its commit, on top "
                "of generated_at_commit"
            ),
            "generator": "ComputeTargets/tests/golden_reflecting_history.py",
            "regenerate": REGENERATE_COMMAND,
            "environment": environment(),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "generation_seconds": round(elapsed, 1),
            "setup": {
                "beta": BETA,
                "M_over_M_P": M_OVER_MP,
                "alpha": ALPHA,
                "n": gth.N_EXPONENT,
                "phi_init_over_M_P": gth.PHI_INIT_OVER_MP,
                "pi_init": gth.PI_INIT,
                "T_init_GeV": gth.T_INIT_GEV,
                "T_stop": "cosmology.T_CMB_Kelvin * units.Kelvin",
                "atol": gth.ATOL,
                "rtol": gth.RTOL,
                "tolerance": gth.TOLERANCE,
                "max_integrations": gth.MAX_INTEGRATIONS,
                "samples_per_log10_z": gth.SAMPLES_PER_LOG10_Z,
                "log10_one_plus_z_high": gth.LOG10_ONE_PLUS_Z_HIGH,
                "seed": "model.seed(cosmology)",
            },
            "not_pinned": [f"metadata.{n}" for n in EXCLUDED_METADATA],
            "not_pinned_reason": "wall-clock measurements that differ between runs",
        },
        **record,
    }
    GOLDEN_REFLECTING_PATH.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    )
    print(f"wrote {GOLDEN_REFLECTING_PATH} at {commit} in {elapsed:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
