"""
The six histories of the demonstration (one-loop-drain prompt 07 §3; orchestrator note 07 ruling 6): in 05's export, at
c_a = 1, the history holding the largest of each of `generations`, `drained_share_gap`, `drained_share_classical`,
`n_bar_max`, `backreaction_max` and `phase_slips_below_1`, quanta_histories.csv joined to histories.csv on the five key cells
(as strings). Values are compared as floats; ties go to the row that comes first in quanta_histories.csv; blank cells are
skipped; duplicates are merged, with each pick's columns listed.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/figures-07/pick_histories.py ROOT

prints one line per picked history, `alpha beta M_Mp n | columns`, in the order of first pick. Reads files only.
"""

import csv
import sys
from pathlib import Path

COLUMNS = [
    ("generations", "quanta"),
    ("drained_share_gap", "histories"),
    ("drained_share_classical", "histories"),
    ("n_bar_max", "quanta"),
    ("backreaction_max", "quanta"),
    ("phase_slips_below_1", "quanta"),
]
KEY = ["drain_model", "alpha", "M_Mp", "n", "beta"]


def picks(root: Path):
    hist = {tuple(r[c] for c in KEY): r for r in csv.DictReader(open(root / "histories.csv", newline=""))}
    quanta = list(csv.DictReader(open(root / "quanta_histories.csv", newline="")))
    joined = []
    for q in quanta:
        k = tuple(q[c] for c in KEY)
        if k not in hist:
            raise SystemExit(f"quanta_histories.csv row {k} has no histories.csv row")
        joined.append((k, q, hist[k]))
    chosen = {}
    for name, source in COLUMNS:
        best, best_key = None, None
        for k, q, h in joined:
            cell = (q if source == "quanta" else h)[name]
            if cell == "":
                continue
            v = float(cell)
            if best is None or v > best:
                best, best_key = v, k
        chosen.setdefault(best_key, []).append(f"{name}={best!r}")
    return chosen


if __name__ == "__main__":
    for k, cols in picks(Path(sys.argv[1])).items():
        model, alpha, M, n, beta = k
        print(f"{alpha} {beta} {M} {n} | {', '.join(cols)}")
