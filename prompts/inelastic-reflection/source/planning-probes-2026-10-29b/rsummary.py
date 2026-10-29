# Summarise r_criterion.py records as one table. Usage: rsummary.py DIR
import glob
import json
import sys
from math import exp, log10

rows = []
for f in glob.glob(sys.argv[1] + "/*.json"):
    r = json.load(open(f))
    rows.append(r)
rows.sort(key=lambda r: (r["alpha"], r["beta"], -r["M_over_Mp"], r["mode"]))
print(f"{'a':>4} {'beta':>4} {'M':>6} {'mode':>4} | {'outcome':<26} {'steps':>7} {'wall s':>6} | "
      f"{'floor':>5} {'drain':>5} {'wallTP':>6} | {'byR':>4} {'byNum':>5} {'resolv':>6} {'log10 R range':>15} | "
      f"{'sum drho/rhoR':>13} {'xceil':>5}")
for r in rows:
    b = r["beta"]
    p = r["parked"]
    if p:
        out = f"parked {exp(p['ln_TJ_MeV']):.3g} MeV ({p['kind'][:5]})"
    else:
        fl = r["failure"] or "ran to T_CMB"
        out = ("FAIL " + ("budget" if "budget" in fl else "phi<=0" if "phi <= 0" in fl else
                          "spacing" if "spacing" in fl else "inside wall" if "inside the wall" in fl else fl[:20])
               ) if r["failure"] else "ran to T_CMB, no park"
    inj = r["inj"]
    nfl = sum(1 for x in inj if x[1] == "floor")
    ndr = sum(1 for x in inj if x[1] == "turning_point")
    nwall = sum(1 for t in r["tp"] if t[1] == "wall")
    d = r["decisions"]
    hits = [x for x in d if x[3]]
    byR = sum(1 for x in hits if x[1] > 0)
    byN = len(hits) - byR
    # approaches judged resolved: decisions with hit False, counted once per wall turning point is
    # awkward; report the number of resolved-side decisions instead
    nres = sum(1 for x in d if not x[3])
    lr = f"{min(x[1] for x in d):+.1f}..{max(x[1] for x in d):+.1f}" if d else "-"
    tot = sum(x[6] for x in inj)
    ceil = 1.0 / (6 * b * b - 1)
    print(f"{r['alpha']:>4} {b:>4} {r['M_over_Mp']:>6.0e} {r['mode']:>4} | {out:<26} {r['steps']:>7} "
          f"{r['wall_time_s']:>6.0f} | {nfl:>5} {ndr:>5} {nwall:>6} | {byR:>4} {byN:>5} {nres:>6} {lr:>15} | "
          f"{tot:>13.4e} {tot / ceil:>5.3f}")
