"""
bbn-tolerance prompt 01c, section 2: T1's prod rows on log 01's S3 inputs
against log 01's S3 rows, field by field, as the CSV strings were written
(Python's repr of each float, so every printed digit).

    ./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/compare_s3.py

Compares status, failure_stage, t_reached, t_target, Yp, DOverH, He3OverH and
Li7OverH for each (input, lowT_rtol). Prints one line per cell and a verdict;
exits 1 on any difference or missing cell.
"""

import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parent / "01-probes" / "scan.csv"
NEW = HERE / "scan.csv"
FIELDS = (
    "status",
    "failure_stage",
    "t_reached",
    "t_target",
    "Yp",
    "DOverH",
    "He3OverH",
    "Li7OverH",
)


def key(r):
    return (r["input"], r["lowT_rtol"] or "default")


def main():
    old = {
        key(r): r
        for r in csv.DictReader(open(OLD))
        if r["tag"] == "S3" and r["variant"] in ("prod", "SM")
    }
    new = {
        key(r): r
        for r in csv.DictReader(open(NEW))
        if r["tag"] == "T1"
        and r["network"] == "small"
        and r["variant"] in ("prod", "SM")
        and key(r) in old
    }
    bad = 0
    for k in sorted(old):
        if k not in new:
            print(f"MISSING {k}")
            bad += 1
            continue
        diffs = [f for f in FIELDS if old[k][f] != new[k][f]]
        if old[k]["network"] != "small":
            diffs.append("network")
        print(
            f"{k[0]:20s} rtol={k[1]:8s} Yp {new[k]['Yp']:22s} D/H {new[k]['DOverH']:20s} "
            f"{'IDENTICAL' if not diffs else 'DIFFERS in ' + ', '.join(diffs)}"
        )
        bad += bool(diffs)
    print(
        f"\n{len(old)} cells of log 01's S3; {len(old) - bad} identical in every field; {bad} not"
    )
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
