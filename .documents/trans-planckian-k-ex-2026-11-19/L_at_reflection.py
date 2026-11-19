"""
L = ln(1 + E/V_0) at the reflection events of the 2026.11.0 exports (trans-planckian-k-ex note, 2026-11-19, §4).

Reads the bounces.csv files committed under the run's record (exponential wall, n = 1). Run from the repository root:
./venv/bin/python <this file> [record directory], default science-runs/2026-11-13-science-2026.11.0 (on main).
"""

import csv
import glob
import math
import sys
from collections import defaultdict

record = (
    sys.argv[1] if len(sys.argv) > 1 else "science-runs/2026-11-13-science-2026.11.0"
)
rows = defaultdict(list)
for f in sorted(glob.glob(record + "/**/bounces.csv", recursive=True)):
    key = f.split("inputs/")[1].rsplit("/bounces.csv", 1)[0]
    for r in csv.DictReader(open(f)):
        try:
            L = float(r["L"])
        except ValueError:
            continue
        if math.isfinite(L):
            rows[key].append((L, float(r["log_T_Jordan_GeV"])))
everything = []
for k, v in rows.items():
    Ls = sorted(x[0] for x in v)
    T = [x[1] for x in v]
    everything += Ls
    print(
        f"{k:55s} N={len(Ls):4d}  L: min {Ls[0]:7.2f}  median {Ls[len(Ls) // 2]:7.2f}  max {Ls[-1]:7.2f}"
        f"   ln(T_J/GeV) {min(T):.1f}..{max(T):.1f}"
    )
print(f"all: N={len(everything)}  L {min(everything):.2f}..{max(everything):.2f}")
