"""
bbn-tolerance prompt 03, README section 6.3: compare remeasure.csv (the final
tree, no override, small network, `prod`) with logs/02-probes/acceptance.csv's
`prod` rows and the SM row (log 02's figures, which equal log 01c's T1 rows at
lowT_rtol 1e-06), input by input, in every outcome field, as the CSV strings
were written. The `pert12` and `pert9` rows of acceptance.csv are not part of
the re-measure and are not compared.

    ./venv/bin/python prompts/bbn-tolerance/logs/03-probes/compare.py
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
REF = HERE.parent / "02-probes" / "acceptance.csv"
NEW = HERE / "remeasure.csv"
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
        if r["tag"] == "A02" and r["variant"] in ("prod", "SM")
    }
    new = [r for r in csv.DictReader(open(NEW)) if r["tag"] == "R03"]
    keys = [key(r) for r in new]
    assert len(keys) == len(set(keys)), "duplicate rows in remeasure.csv"
    print(f"reference rows {len(ref)}; re-measure rows {len(new)}")
    for r in new:
        assert r["lowT_rtol"] == "" and r["lowT_atol"] == "", "an override was passed"
        assert r["network"] == "small", "not the small network"
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
    print(f"reference rows with no re-measure row: {len(missing)}")
    for k in missing:
        print(f"  {k}")
    print(f"non-ok outcomes: {len(failures)}")
    for k in failures:
        print(f"  {k}")
    for r in sorted(new, key=key):
        print(
            f"  {r['input']} {r['variant']}: Yp {r['Yp']}, D/H {r['DOverH']}, "
            f"3He/H {r['He3OverH']}, 7Li/H {r['Li7OverH']}"
        )


if __name__ == "__main__":
    main()
