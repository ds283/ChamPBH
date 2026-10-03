"""
bbn-tolerance prompt 01: summarise yp_floor.csv (README section 2 (e)): per
history and configuration, the spreads of Yp and D/H over prod, pert12, pert9.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/summarize_yp_floor.py
"""

import csv
from collections import defaultdict
from pathlib import Path

ORDER = ["base", "thermo", "aT", "highT", "midT", "all"]
rows = list(csv.DictReader(open(Path(__file__).resolve().parent / "yp_floor.csv")))
d = defaultdict(dict)
for r in rows:
    d[(r["beta"], r["M_Mp"], r["config"])][r["variant"]] = r
for key in sorted(d, key=lambda k: (k[0], k[1], ORDER.index(k[2]))):
    vs = d[key]
    st = [v["status"] for v in vs.values()]
    if len(vs) == 3 and all(s == "ok" for s in st):
        Y = sorted(float(v["Yp"]) for v in vs.values())
        D = sorted(float(v["DOverH"]) for v in vs.values())
        print(
            f"beta={key[0]} M={key[1]} {key[2]:6s}: Yp spread {(Y[2] - Y[0]) / Y[1]:.3e} "
            f"D/H spread {(D[2] - D[0]) / D[1]:.3e}; prod Yp {float(vs['prod']['Yp']):.10g} "
            f"D/H {float(vs['prod']['DOverH']):.10g}; wall(prod, loaded) {float(vs['prod']['wall_s']):.0f} s"
        )
    else:
        bad = [
            f"{k}: {v['failure_reason'][:110]}"
            for k, v in vs.items()
            if v["status"] != "ok"
        ]
        print(f"beta={key[0]} M={key[1]} {key[2]:6s}: {st} {bad}")
