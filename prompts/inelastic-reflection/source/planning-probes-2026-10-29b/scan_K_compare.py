# Compare, in the band of M where phi_t_scan.py's results depend on PHI_NUM, the resolved history
# (PHI_NUM = 0, "R only") with the reflected one (PHI_NUM = 1e-8, every wall reflected) at K = 10, 3 and
# 1.5: Sigma drho/rho_R against the ceiling, where it parked, and how many floor reflections drained with
# w_gate < 1 (today's floor rule, whose gate reads the infall).
# Usage: scan_K_compare.py DIR   (DIR holds scan/ and scanK/)
import json
import sys
from math import exp

base = sys.argv[1]


def load(p):
    r = json.load(open(p))
    if r["failure"]:
        return "FAIL" + (" (spacing)" if "spacing" in r["failure"] else "")
    c = 1.0 / (6 * r["beta"] ** 2 - 1)
    d = sum(x[6] for x in r["inj"]) / c
    w = sum(1 for x in r["inj"] if x[1] == "floor" and x[4] is not None and x[4] < 0.999999)
    return f"{d:.4f} @{exp(r['parked']['ln_TJ_MeV']) * 1e3:.4g} keV w<1:{w}"


for b in ("1.2", "2", "3"):
    for M in ("1e-9", "3e-10", "1e-10", "3e-11"):
        row = [f"beta={b} M={M}", "R only " + load(f"{base}/scan/b{b}_M{M}_t0_gate.json"),
               "K=10 " + load(f"{base}/scan/b{b}_M{M}_t1e-8_gate.json")]
        for K in ("3", "1.5"):
            row.append(f"K={K} " + load(f"{base}/scanK/b{b}_M{M}_t1e-8_gate_K{K}.json"))
        print(" | ".join(row))
