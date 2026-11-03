"""
one-loop-drain prompt 01 §2.1.2 (orchestrator note 01, ruling 2): reflection-restart 04's 42 tasks, run on this
tree by the scanner (var/one-loop-drain/01/the-42.jsonl), against 04's records (var/reflection-restart/04/
the-42.jsonl), task by task: outcome, both SHA-256 digests, n_samples, accepted_steps, RHS_evaluations,
reflections and restart_retakes; for a failure, its reason. A difference is a stop. The wall-clock of each
is printed beside it, as a measurement.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/compare_replay.py > .documents/one-loop-drain/law/out/compare_replay.txt
"""

import json
import os
import sys
from pathlib import Path

NEW = Path(
    os.environ.get(
        "ONE_LOOP_DRAIN_01_NEW",
        "/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01/the-42.jsonl",
    )
)
OLD = Path("/Users/ds283/Documents/Code/ChamPBH/var/reflection-restart/04/the-42.jsonl")
FIELDS = (
    "outcome",
    "samples_sha256",
    "log_one_plus_z_sha256",
    "n_samples",
    "accepted_steps",
    "RHS_evaluations",
    "reflections",
    "restart_retakes",
    "failure_reason",
)


def load(path):
    out = {}
    for line in path.read_text().splitlines():
        d = json.loads(line)
        out[d["key"]] = d
    return out


def main():
    old, new = load(OLD), load(NEW)
    print(
        f"04's records: {len(old)}; this tree's: {len(new)}; same keys: {set(old) == set(new)}"
    )
    print()
    print(
        "| task | outcome | samples | accepted_steps | RHS | reflections | retakes | equal | wall 04 / here (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    differing = []
    for key in sorted(old):
        a, b = old[key], new.get(key, {})
        diffs = [f for f in FIELDS if a.get(f) != b.get(f)]
        if diffs:
            differing.append((key, diffs))
        print(
            f"| {key} | {b.get('outcome')} | {b.get('n_samples')} | {b.get('accepted_steps')} | "
            f"{b.get('RHS_evaluations')} | {b.get('reflections')} | {b.get('restart_retakes')} | "
            f"{'yes' if not diffs else 'NO: ' + ', '.join(diffs)} | "
            f"{a.get('wall', float('nan')):.1f} / {b.get('wall', float('nan')):.1f} |"
        )
    print()
    failures = [k for k in sorted(new) if new[k].get("outcome") != "success"]
    for k in failures:
        print(f"failure {k}: here {new[k].get('failure_reason')!r}")
        print(f"         04:   {old[k].get('failure_reason')!r}")
    print()
    print(
        f"tasks differing in any field: {len(differing)}"
        + (
            ""
            if not differing
            else ": " + "; ".join(f"{k} ({', '.join(d)})" for k, d in differing)
        )
    )
    return 1 if differing or set(old) != set(new) else 0


if __name__ == "__main__":
    sys.exit(main())
