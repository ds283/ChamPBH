"""
reflection-restart prompt 04 §1.2 (c): every call of the wall search in one history, recorded passively
(orchestrator note 04, ruling 5). Wraps AbstractModel.classical_turning_point in this process so that each call's
phi, ln E, normalization, its result (None, a turning point, or a raise) and the root's margin are recorded;
the wrapper calls the original with the same arguments and passes back exactly what it returns or raises. Then
runs one history of tasks-pair.json at its stored normalization through the scanner's scan_one, in this process.

**The margin** of a call is where its root lies: the target ln V - ln V_floor = ln(1 + E/V_floor), in quanta of
ulp(max(|ln V|, |ln V_floor|)) at the target, as _log_V_minus_floor measures the width it treats as -inf
(WALL_ROUNDING_QUANTA quanta). It depends on ln E and the normalization only. A searched root below
WALL_ROUNDING_QUANTA + 1 quanta lies inside the -inf region.

Runs on either tree: from the repository root for this branch's, or from the 89a0760 worktree's root with
PYTHONPATH=. and this checkout's venv, which check_tree() verifies (ruling 3). Writes one JSON line to OUTPUT
(the scanner's record, with "candidates", a summary) and every call to OUTPUT_DIR/candidates-<tree>-<serial>.npz.

    PYTHONPATH=. /Users/ds283/Documents/Code/ChamPBH/venv/bin/python \\
        /Users/ds283/Documents/Code/ChamPBH/.documents/reflection-restart/close/candidates.py \\
        SERIAL TASKS OUTPUT
"""

import math
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import check_tree, load_task, run_with_timeout, write_record  # noqa: E402

OUTCOME = {"none": 0, "point": 1, "raise": 2}


def margin_quanta(log_E: float, log_V_floor: float) -> float:
    """ln V - ln V_floor at the root of V - V_floor = E, in quanta of ulp(max(|ln V|, |ln V_floor|))."""
    d = math.log1p(math.exp(log_E - log_V_floor))
    log_V = log_V_floor + d
    return d / math.ulp(max(abs(log_V), abs(log_V_floor)))


def main():
    serial, tasks, output = int(sys.argv[1]), sys.argv[2], sys.argv[3]
    SM, scan, head = check_tree()
    import importlib

    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

    calls = []
    original = AM.AbstractModel.classical_turning_point

    def recorded(self, normalization, phi, log_E):
        log_V_floor = self._log_V_floor(normalization)
        try:
            m = margin_quanta(log_E, log_V_floor)
        except Exception:
            m = float("nan")
        try:
            result = original(self, normalization, phi, log_E)
        except BaseException:
            calls.append((phi, log_E, OUTCOME["raise"], m, float("nan")))
            raise
        calls.append(
            (
                phi,
                log_E,
                OUTCOME["none" if result is None else "point"],
                m,
                float("nan") if result is None else result.phi_t,
            )
        )
        return result

    AM.AbstractModel.classical_turning_point = recorded
    spec = load_task(tasks, serial)
    spec["key"] = scan.task_key(spec) + " candidates"
    print(f"task: {spec['key']}")
    print(
        f"AbstractModel.classical_turning_point wrapped (passive); WALL_ROUNDING_QUANTA = "
        f"{AM.WALL_ROUNDING_QUANTA}, WALL_SEARCH_HALVINGS = {AM.WALL_SEARCH_HALVINGS}"
    )
    sys.stdout.flush()
    start = time.strftime("%Y-%m-%dT%H:%M:%S")
    record = run_with_timeout(scan, spec)
    AM.AbstractModel.classical_turning_point = original

    arr = np.array(calls, dtype=float).reshape(-1, 5)
    out_dir = Path(output).parent
    npz = out_dir / f"candidates-{head[:7]}-{serial}.npz"
    np.savez_compressed(
        npz,
        phi=arr[:, 0],
        log_E=arr[:, 1],
        outcome=arr[:, 2],
        margin=arr[:, 3],
        phi_t=arr[:, 4],
    )

    summary = {"calls": int(len(arr)), "npz": str(npz)}
    K = AM.WALL_ROUNDING_QUANTA
    for name, code in OUTCOME.items():
        sel = arr[arr[:, 2] == code]
        summary[f"n_{name}"] = int(len(sel))
        summary[f"min_margin_{name}"] = float(sel[:, 3].min()) if len(sel) else None
    searched = arr[arr[:, 2] != OUTCOME["none"]]
    summary["n_searched"] = int(len(searched))
    summary["min_margin_searched"] = (
        float(searched[:, 3].min()) if len(searched) else None
    )
    summary["min_margin_all"] = float(arr[:, 3].min()) if len(arr) else None
    for bound in (K + 1, 32, 100, 1000):
        summary[f"n_searched_margin_le_{bound}"] = int(np.sum(searched[:, 3] <= bound))
    order = np.argsort(searched[:, 3])[:10] if len(searched) else []
    idx_searched = np.nonzero(arr[:, 2] != OUTCOME["none"])[0]
    summary["smallest_searched"] = [
        {
            "call": int(idx_searched[i]),
            "phi": float(searched[i, 0]),
            "log_E": float(searched[i, 1]),
            "outcome": [k for k, v in OUTCOME.items() if v == searched[i, 2]][0],
            "margin": float(searched[i, 3]),
        }
        for i in order
    ]
    if len(arr) and arr[-1, 2] == OUTCOME["raise"]:
        summary["last_call"] = {
            "call": int(len(arr) - 1),
            "phi": float(arr[-1, 0]),
            "log_E": float(arr[-1, 1]),
            "margin": float(arr[-1, 3]),
            "min_margin_searched_before": (
                float(searched[:-1, 3].min()) if len(searched) > 1 else None
            ),
        }
    record["candidates"] = summary
    record["tree"] = head
    record["started"] = start
    write_record(scan, record, output)
    print(
        f"{record.get('status')} {record.get('outcome')} wall {record.get('wall', 0):.1f} s"
    )
    print({k: v for k, v in summary.items() if k != "smallest_searched"})
    if record.get("outcome") == "failure":
        print(f"failure_reason: {record['failure_reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
