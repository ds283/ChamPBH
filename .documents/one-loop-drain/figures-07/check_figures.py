"""
The gate Figures' item 4 (one-loop-drain prompt 07 §2): each rendered figure's counts on its printed line against the tree's
files, by script. Reads the tools' printed output saved in out/ and quanta_histories.csv of each tree (never the figures):

- a grid line `<model> A=<A>: drawn N, ...`: N equals the rows of quanta_histories<suffix>.csv at that A (the drawn histories
  that have a quanta row; "no quanta row" is added, so drawn = rows + no quanta row);
- a history line `<label>: generations G; ...`: G equals the `generations` of that history's row of quanta_histories<suffix>.csv
  (the label's A, M_Mp, n, beta are the row's cells);
- every one-loop drawn history of 06's tree has a figure (figures written = drawn rows of histories.csv);
- every exit is 0 (the `exit` line each run's file ends with).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/figures-07/check_figures.py
"""

import csv
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
T06 = ROOT / "var/one-loop-drain/06/plot-reference/QCD_Cosmology"
T05 = ROOT / "var/one-loop-drain/05/exports/QCD_Cosmology"
OUT = HERE / "out"

failures = 0


def check(ok, text):
    global failures
    print(("ok   " if ok else "FAIL ") + text)
    if not ok:
        failures += 1


def rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def grid_run(name, tree, suffix):
    global failures
    text = (OUT / name).read_text()
    check(re.search(r"^exit 0$", text, re.M) is not None, f"{name}: exit 0")
    q = rows(tree / f"quanta_histories{suffix}.csv")
    h = rows(tree / "histories.csv")
    n = 0
    for m in re.finditer(
        r"^(\S+) A=(\S+): drawn (\d+), failed (\d+), no quanta row (\d+);", text, re.M
    ):
        model, A, drawn, failed, noq = (
            m.group(1),
            float(m.group(2)),
            *map(int, m.groups()[2:]),
        )
        n += 1
        in_q = sum(1 for r in q if r["drain_model"] == model and float(r["alpha"]) == A)
        in_h = sum(
            1
            for r in h
            if r["drain_model"] == model
            and float(r["alpha"]) == A
            and r["normalization"] != ""
        )
        check(
            drawn == in_h,
            f"{name}: A={A:g} drawn {drawn} = drawn rows of histories.csv {in_h}",
        )
        check(
            drawn - noq == in_q,
            f"{name}: A={A:g} drawn - no quanta row = {drawn - noq}; quanta_histories rows at A: {in_q}",
        )
    summary = re.search(r"figures written (\d+)", text)
    check(
        summary is not None and int(summary.group(1)) == n,
        f"{name}: {n} figure lines, summary says {summary.group(1) if summary else None}",
    )
    return n


def history_run(name, tree, suffix):
    text = (OUT / name).read_text()
    check(re.search(r"^exit 0$", text, re.M) is not None, f"{name}: exit 0")
    q = rows(tree / f"quanta_histories{suffix}.csv")
    by = {
        (
            r["drain_model"],
            float(r["alpha"]),
            float(r["M_Mp"]),
            r["n"],
            float(r["beta"]),
        ): r
        for r in q
    }
    n = 0
    total = 0
    for m in re.finditer(
        r"^(\S+) A=(\S+) M_Mp=(\S+) n=(\S+) beta=(\S+): generations (\d+); bounce rows (\d+);",
        text,
        re.M,
    ):
        model, A, M, nn, beta, gens = (
            m.group(1),
            float(m.group(2)),
            float(m.group(3)),
            m.group(4),
            float(m.group(5)),
            int(m.group(6)),
        )
        row = by.get((model, A, M, nn, beta))
        n += 1
        total += gens
        check(
            row is not None and int(row["generations"]) == gens,
            f"{name}: A={A:g} M_Mp={M:g} beta={beta:g}: generations {gens} = quanta_histories {row['generations'] if row else None}",
        )
    summary = re.search(r"figures written (\d+)", text)
    check(
        summary is not None and int(summary.group(1)) == n,
        f"{name}: {n} history lines, summary says {summary.group(1) if summary else None}",
    )
    return n


if __name__ == "__main__":
    n = grid_run("grid-06.txt", T06, "")
    check(n == 2, "grid-06.txt: two A (0.1 and 1)")
    n = history_run("history-06.txt", T06, "")
    drawn06 = sum(
        1
        for r in rows(T06 / "histories.csv")
        if r["drain_model"] != "fixed" and r["normalization"] != ""
    )
    check(
        n == drawn06 == 9,
        f"history-06.txt: {n} figures = {drawn06} drawn one-loop histories of 06's histories.csv",
    )
    n = grid_run("grid-05.txt", T05, "")
    check(n == 8, "grid-05.txt: eight A")
    grid_run("grid-05-assoc0.txt", T05, "-assoc0")
    grid_run("grid-05-assoc10.txt", T05, "-assoc10")
    for p in sorted(OUT.glob("history-05-*.txt")):
        history_run(p.name, T05, "")
    print(f"{failures} failures")
    sys.exit(1 if failures else 0)
