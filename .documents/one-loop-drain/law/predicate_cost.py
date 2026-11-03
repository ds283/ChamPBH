"""
one-loop-drain prompt 01 §2.1.4 and §2.1.6: one history of reflection-restart 04's tasks-pair.json (2197 or 2188
at its stored normalization, alpha = 10^-3, the fixed law, untuned, to T_CMB), run in this process by the
scanner's own run_history (tools/outward_step_scan.py), with the wall-search predicate timed.

Both names of the predicate are wrapped for the run: AbstractModel._turning_point_unresolvable (which
classical_turning_point calls, after its r_0 >= 0 test, before every search) and the name ScalarHistory
imported (which the step loop calls at a None assessment). Each call is timed by perf_counter around it, so
the total is an upper bound on the predicate's cost (it includes the wrapper's own overhead). The output
gives the calls, their total and its share of the history's wall-clock, and the run's record: the payload's
"drain" (unresolvable_assessments), reflections, the parking N, r_chi and Delta N_eff today, and the digest
of the samples, which must equal the scanner's record of the same task in var/one-loop-drain/01/pair.jsonl.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/predicate_cost.py SERIAL OUT.json
"""

import contextlib
import importlib
import io
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")

TASKS = Path(
    "/Users/ds283/Documents/Code/ChamPBH/var/reflection-restart/04/tasks-pair.json"
)


def main():
    serial, out_path = int(sys.argv[1]), sys.argv[2]
    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    scan = importlib.import_module("tools.outward_step_scan")
    spec = next(
        t for t in json.loads(TASKS.read_text()) if t["store"]["serial"] == serial
    )

    original = AM._turning_point_unresolvable
    calls = {"model": 0, "loop": 0}
    seconds = {"model": 0.0, "loop": 0.0}

    def timed(where):
        def wrapper(log_V_floor, log_E):
            t0 = time.perf_counter()
            try:
                return original(log_V_floor, log_E)
            finally:
                seconds[where] += time.perf_counter() - t0
                calls[where] += 1

        return wrapper

    AM._turning_point_unresolvable = timed("model")
    SM._turning_point_unresolvable = timed("loop")
    start = time.perf_counter()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            data, units = scan.run_history(spec)
    finally:
        AM._turning_point_unresolvable = original
        SM._turning_point_unresolvable = original
    wall = time.perf_counter() - start

    out = {
        "serial": serial,
        "spec": spec,
        "wall_s": wall,
        "predicate_calls": calls,
        "predicate_seconds": seconds,
        "predicate_share_of_wall": (seconds["model"] + seconds["loop"]) / wall,
        "failure": bool(data.get("failure", False)),
        "failure_reason": data.get("failure_reason"),
    }
    drain = data.get("drain")
    out["drain"] = None if drain is None else dict(drain._asdict())
    if not out["failure"]:
        summary = scan.history_summary(data, units)
        out["reflections"] = summary["reflections"]
        out["accepted_steps"] = summary["accepted_steps"]
        out["samples_sha256"] = summary["samples_sha256"]
        out["log_one_plus_z_sha256"] = summary["log_one_plus_z_sha256"]
        out["dissipation"] = summary["dissipation"]
        out["candidates"] = len(data["injections"])
        out["drains"] = sum(1 for i in data["injections"] if i.delta_K > 0.0)
    with open(out_path, "w") as fh:
        json.dump(out, fh, indent=1, default=str)
    print(
        "done",
        json.dumps(
            {
                k: out[k]
                for k in (
                    "serial",
                    "wall_s",
                    "predicate_calls",
                    "predicate_share_of_wall",
                    "failure",
                )
            }
        ),
    )


if __name__ == "__main__":
    main()
