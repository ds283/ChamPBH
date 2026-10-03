"""
bbn-tolerance prompt 02, README section 6.2: compare acceptance.csv (the
patched tree, no override, small network) with logs/01c-probes/scan.csv's T1
rows at lowT_rtol 1e-06 (the override at 1e-6 on 893a5b1), input by input and
variant by variant, in every outcome field, as the CSV strings were written.

    ./venv/bin/python prompts/bbn-tolerance/logs/02-probes/compare.py
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
REF = HERE.parent / "01c-probes" / "scan.csv"
NEW = HERE / "acceptance.csv"
FIELDS = [
    "status",
    "failure_stage",
    "t_reached",
    "t_target",
    "Yp",
    "DOverH",
    "He3OverH",
    "Li7OverH",
    "failure_reason",
]


def key(row):
    return (row["input"], row["variant"], row["network"])


def main():
    ref = {
        key(r): r
        for r in csv.DictReader(open(REF))
        if r["tag"] == "T1" and r["lowT_rtol"] == "1e-06"
    }
    new = [r for r in csv.DictReader(open(NEW)) if r["tag"] == "A02"]
    keys = [key(r) for r in new]
    assert len(keys) == len(set(keys)), "duplicate rows in acceptance.csv"
    print(f"reference rows {len(ref)}; acceptance rows {len(new)}")
    for r in new:
        assert r["lowT_rtol"] == "" and r["lowT_atol"] == "", "an override was passed"
    identical, differ, missing = 0, [], sorted(set(ref) - set(keys))
    for r in sorted(new, key=key):
        k = key(r)
        if k not in ref:
            differ.append((k, "no reference row"))
            continue
        bad = [f for f in FIELDS if r[f] != ref[k][f]]
        if bad:
            differ.append(
                (k, ", ".join(f"{f}: {r[f]!r} vs {ref[k][f]!r}" for f in bad))
            )
        else:
            identical += 1
    failures = [key(r) for r in new if r["status"] != "ok"]
    print(f"identical in every field: {identical} of {len(new)}")
    print(f"differing: {len(differ)}")
    for k, why in differ:
        print(f"  {k}: {why}")
    print(f"reference rows with no acceptance row: {len(missing)}")
    for k in missing:
        print(f"  {k}")
    print(f"non-ok outcomes: {len(failures)}")
    for k in failures:
        print(f"  {k}")
    sm = [r for r in new if r["input"] == "SM"]
    ctl = [
        r for r in new if r["input"] == "beta=1.6 M=0.001" and r["variant"] == "prod"
    ]
    for r in sm + ctl:
        print(
            f"  {r['input']} {r['variant']}: Yp {r['Yp']}, D/H {r['DOverH']}, "
            f"3He/H {r['He3OverH']}, 7Li/H {r['Li7OverH']}"
        )


if __name__ == "__main__":
    main()
