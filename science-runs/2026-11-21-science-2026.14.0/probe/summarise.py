"""
The probe's Part 2 summary (PLAN.md section 3). Stdlib only; reads, writes nothing but stdout.

    ./venv/bin/python science-runs/plan-science-2026.14.0/probe/summarise.py HISTORIES.txt [...] \
        --logs LOGDIR --stores STEM [STEM ...]

HISTORIES.txt is the output of .documents/power-law-wall/rehearse-07d/histories.py on a probe store; its `JSON {...}`
lines are read. It reports, per wall:
  - per (M, n, row): the histories, the failures, the tuning cap reached (a failure reason "the normalization did not
    converge: after N integrations"), and the median and largest number of tuning integrations;
  - per (M, beta, n, row), each history that failed, with its reason (the label in it masked);
  - per block, the wall time between run_probe.sh's ">>" and "<<" lines in LOGDIR/probe.log;
  - per store, its files' size on disk and the size per history (for PLAN.md section 7).
No threshold is set and nothing is judged: what it finds is for the user.
"""

import argparse
import glob
import json
import os
import re
import statistics
from collections import defaultdict
from datetime import datetime

CAP = "the normalization did not converge: after"


def val(v):
    return float(v["repr"]) if isinstance(v, dict) and "repr" in v else v


def mask(reason):
    if not reason:
        return reason
    i = reason.find("(")
    j = reason.find("): ")
    return reason if i < 0 or j < i else reason[: i + 1] + "<label>" + reason[j:]


def row_name(h):
    return (
        f"alpha {h['alpha']:g}" if h["drain_model"] == "fixed" else f"A {h['alpha']:g}"
    )


def read(path):
    out = []
    with open(path) as f:
        for line in f:
            if line.startswith("JSON "):
                out.append({k: val(v) for k, v in json.loads(line[5:]).items()})
    return out


def block_times(logdir):
    starts, times = {}, {}
    path = os.path.join(logdir, "probe.log")
    if not os.path.exists(path):
        return times
    for line in open(path):
        m = re.match(r"(>>|<<) (\S+ \S+) (\S+)$", line.strip())
        if not m:
            continue
        t = datetime.strptime(m.group(2), "%Y-%m-%d %H:%M:%S")
        if m.group(1) == ">>":
            starts[m.group(3)] = t
        elif m.group(3) in starts:
            times[m.group(3)] = (t - starts[m.group(3)]).total_seconds()
    return times


def main():
    p = argparse.ArgumentParser()
    p.add_argument("histories", nargs="+")
    p.add_argument("--logs")
    p.add_argument("--stores", nargs="*", default=[])
    a = p.parse_args()

    for path in a.histories:
        hs = read(path)
        print(f"== {path}: {len(hs)} histories")
        groups = defaultdict(list)
        for h in hs:
            groups[(h["potential"], h["M_Mp"], h["n"], row_name(h))].append(h)
        print(
            f"  {'potential':13} {'M/M_P':>9} {'n':>2} {'row':10} {'hist':>4} {'fail':>4} {'cap':>4} {'med it':>6} {'max it':>6}"
        )
        for (pot, M, n, row), g in sorted(groups.items()):
            its = [
                h["tuning_iterations"]
                for h in g
                if not h["failure"] and h["tuning_iterations"] is not None
            ]
            fails = [h for h in g if h["failure"]]
            cap = [h for h in fails if CAP in (h["failure_reason"] or "")]
            print(
                f"  {pot:13} {M:9.3g} {n:2} {row:10} {len(g):4} {len(fails):4} {len(cap):4} "
                f"{statistics.median(its) if its else '-':>6} {max(its) if its else '-':>6}"
            )
        fails = [h for h in hs if h["failure"]]
        print(f"  failures: {len(fails)}")
        for h in sorted(
            fails, key=lambda h: (h["M_Mp"], h["n"], row_name(h), h["beta"])
        ):
            print(
                f"    M {h['M_Mp']:.3g} beta {h['beta']:g} n {h['n']} {row_name(h)}: {mask(h['failure_reason'])!r}"
            )
        print()

    if a.logs:
        times = block_times(a.logs)
        print("== block wall times (run_probe.sh's log)")
        for job, s in times.items():
            print(f"  {job:28} {s / 60:8.1f} min")
        print()

    for stem in a.stores:
        files = glob.glob(glob.escape(stem) + "-shard*.db") + glob.glob(
            glob.escape(stem) + ".db"
        )
        size = sum(os.path.getsize(f) for f in files)
        n = None
        for path in a.histories:
            if os.path.basename(stem).split("-")[-1] in os.path.basename(path):
                n = len(read(path))
        per = f", {size / n / 1e6:.2f} MB per history ({n})" if n else ""
        print(f"== store {stem}: {len(files)} files, {size / 1e9:.3f} GB{per}")


if __name__ == "__main__":
    main()
