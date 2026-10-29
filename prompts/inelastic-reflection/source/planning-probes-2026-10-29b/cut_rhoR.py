# Where a parking cut relative to the radiation bath, rho_kin/rho_R < EPS, would fire in the records of
# floor_cascade.py: at the first floor reflection after the first impact with pi_in^2/6 < EPS (all the
# energy is kinetic at a reflection, and rho_kin/rho_R ~ pi^2/6 M_P^2 there), for EPS = 1e-6, 1e-8, 1e-10.
# Reports the step count at the cut and the share of the history's total drain that comes after it.
# Usage: cut_rhoR.py FILE...
import bisect
import json
import sys
from math import exp

for f in sys.argv[1:]:
    r = json.load(open(f))
    inj, tp = r["inj"], r["tp"]
    tpN = [t[0] for t in tp]
    tot = sum(x[6] for x in inj)
    print(f.split("/")[-1], "| total steps", r["steps"])
    for eps in (1e-6, 1e-8, 1e-10):
        i = next((k for k, x in enumerate(inj) if k > 0 and x[2] ** 2 / 6 < eps), None)
        if i is None:
            print(f"    eps={eps:g}: never")
            continue
        k = min(bisect.bisect_left(tpN, inj[i][0]), len(tp) - 1)
        rest = sum(x[6] for x in inj[i:])
        print(f"    eps={eps:g}: #{i} N={inj[i][0]:.3f} T_J~{exp(tp[k][3]) * 1e3:.4g} keV "
              f"steps={inj[i][8]} drain left {rest / tot:.1e}")
