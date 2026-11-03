"""
one-loop-drain prompt 01 §1.7, after the user's ruling (b): two of the 21 stored fixed-law failures "reflection
requested inside the wall" (law/out/inside_wall_store.txt) that still fail on the tree before the ruling, 3460
(beta 0.75, M 1e-12 M_P, alpha 0.1) and 4397 (beta 1.0, M 1e-12 M_P, alpha 0.01), run tuned under the fixed law
by the scanner's own run_history (tools/outward_step_scan.py), in this process. It records the outcome, the
tuning's integrations, the accepted integration's (or the failing one's) "drain" record, with its
unresolved_declined count, and the summary values.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/store_failures.py SERIAL OUT.json
"""

import contextlib
import importlib
import io
import json
import sys
import time

sys.path.insert(0, ".")

SPECS = {
    3460: {"mode": "tuned", "beta": 0.75, "M": 1e-12, "alpha": 0.1},
    4397: {"mode": "tuned", "beta": 1.0, "M": 1e-12, "alpha": 0.01},
}


def main():
    serial, out_path = int(sys.argv[1]), sys.argv[2]
    scan = importlib.import_module("tools.outward_step_scan")
    spec = dict(SPECS[serial])
    t0 = time.perf_counter()
    with contextlib.redirect_stdout(io.StringIO()) as out:
        data, units = scan.run_history(spec)
    wall = time.perf_counter() - t0
    summary = scan.history_summary(data, units)
    drain = data.get("drain")
    record = {
        "serial": serial,
        "spec": spec,
        "wall_s": wall,
        "outcome": summary["outcome"],
        "failure_reason": summary.get("failure_reason"),
        "drain": None if drain is None else dict(drain._asdict()),
        "tuning_iterations": data.get("tuning_iterations"),
        "reflections": summary.get("reflections"),
        "dissipation": summary.get("dissipation"),
        "stdout_tail": out.getvalue().splitlines()[-5:],
    }
    with open(out_path, "w") as fh:
        json.dump(record, fh, indent=1, default=str)
    print(
        "done",
        json.dumps(
            {
                k: record[k]
                for k in (
                    "serial",
                    "wall_s",
                    "outcome",
                    "failure_reason",
                    "tuning_iterations",
                )
            }
        )[:600],
    )


if __name__ == "__main__":
    main()
