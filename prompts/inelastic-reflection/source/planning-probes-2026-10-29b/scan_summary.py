# Summarise phi_t_scan.py records as grids of M (rows) against PHI_NUM (columns), one block per
# (beta, GATE): Sigma drho/rho_R against the ceiling 1/(6 beta^2 - 1), or the failure; then where it
# parked (T_J in keV), the accepted steps, and the reflections judged unresolved. The phi_t column is the
# range of the classical turning points of the approaches that came within K phi_t of the wall.
# Usage: scan_summary.py DIR
import glob
import json
import sys
from collections import defaultdict
from math import exp

recs = defaultdict(dict)
for f in glob.glob(sys.argv[1] + "/*.json"):
    r = json.load(open(f))
    recs[(r["beta"], r["gate"])][(r["M_over_Mp"], r["phi_num"])] = r

for (beta, gate), runs in sorted(recs.items()):
    Ms = sorted({k[0] for k in runs}, reverse=True)
    ths = sorted({k[1] for k in runs})
    ceil = 1.0 / (6 * beta * beta - 1)

    def cell(r, what):
        if r is None:
            return "-"
        if r["failure"] is not None:
            f = r["failure"]
            return ("F:budget" if "budget" in f else "F:spacing" if "spacing" in f else
                    "F:phi<=0" if "phi <= 0" in f else "F:wall" if "inside the wall" in f else "F:other")
        if r["parked"] is None:
            return "noPark"
        if what == "drain":
            return f"{sum(x[6] for x in r['inj']) / ceil:.4f}"
        if what == "T":
            return f"{exp(r['parked']['ln_TJ_MeV']) * 1e3:.4g}"
        if what == "steps":
            return f"{r['steps']}"
        return f"{sum(1 for d in r['decisions'] if d[3])}"

    for what, title in (("drain", "sum drho/rho_R / ceiling"), ("T", "parked at T_J (keV)"),
                        ("steps", "accepted steps"), ("unres", "reflections judged unresolved")):
        print(f"\n== beta={beta} gate={gate}: {title}")
        print(f"{'M':>7} {'phi_t range':>17} | " + " ".join(f"{('PHI ' + format(t, 'g')) if t else 'R only':>10}" for t in ths))
        for M in Ms:
            pts = [d[5] for t in ths if (M, t) in runs for d in runs[(M, t)]["decisions"]]
            rng = f"{min(pts):.1e}..{max(pts):.1e}" if pts else "-"
            print(f"{M:>7g} {rng:>17} | " + " ".join(f"{cell(runs.get((M, t)), what):>10}" for t in ths))
