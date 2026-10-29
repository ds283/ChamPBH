# Summarise floor_cascade.py records. Usage: summarise.py EPS FILE...
import bisect
import json
import statistics
import sys
from math import exp, sqrt

eps = float(sys.argv[1])
for f in sys.argv[2:]:
    r = json.load(open(f))
    Mp, b = r["Mp"], r["beta"]
    inj, tp = r["inj"], r["tp"]
    tpN = [t[0] for t in tp]

    def TJ(N):  # T_J/MeV at the nearest turning point
        if not tp:
            return float("nan")
        k = min(max(bisect.bisect_left(tpN, N), 0), len(tp) - 1)
        return exp(tp[k][3])

    print(f"== {f.split('/')[-1]}: beta={b} M={r['M_over_Mp']:g} alpha={r['alpha']} "
          f"{r['wall_time_s']:.0f}s steps={r['steps']} tps={len(tp)} inj={len(inj)}")
    print(f"   failure: {r['failure']}")
    kinds = {k: sum(1 for x in inj if x[1] == k) for k in ("floor", "turning_point")}
    drained = [x for x in inj if x[3] > 0]
    tot = sum(x[6] for x in inj)
    ceil = 1.0 / (6 * b * b - 1)
    print(f"   candidates {kinds}; sum drho/rho_R = {tot:.4e} = {tot / ceil:.3f} x ceiling {ceil:.4f}")
    if not inj:
        continue
    print(f"   first candidate N={inj[0][0]:.4f} T_J~{TJ(inj[0][0]):.4g} MeV, drho/rho_R={inj[0][6]:.3e}")
    ws = [x[4] for x in inj]
    q = statistics.quantiles(ws, n=10)
    print(f"   w_gate: min {min(ws):.3f} p10 {q[0]:.3f} median {statistics.median(ws):.3f} max {max(ws):.3f}")
    # share of the total drain by the first k candidates
    acc, shares = 0.0, {}
    for i, x in enumerate(inj):
        acc += x[6]
        if i + 1 in (1, 3, 10, 30, 100):
            shares[i + 1] = acc / tot
    print("   drain share of first k:", {k: round(v, 3) for k, v in shares.items()})
    if kinds["floor"] == 0:
        continue
    # the cut of handoff §5.1: the first floor reflection whose |pi_in|^2 is below eps times the
    # largest |pi_in|^2 since the last re-excitation (a reflection with |pi_in| > 1.05 x the previous)
    fl = [x for x in inj if x[1] == "floor"]
    ref_pi, cut = abs(fl[0][2]), None
    for i, x in enumerate(fl):
        p = abs(x[2])
        if i > 0 and p > 1.05 * abs(fl[i - 1][2]):
            ref_pi = p
        ref_pi = max(ref_pi, p)
        if (p / ref_pi) ** 2 < eps:
            cut = i
            break
    if cut is None:
        print("   no cut reached")
    else:
        x = fl[cut]
        k0 = max(j for j in range(cut + 1) if abs(fl[j][2]) == ref_pi)
        tail = [abs(fl[j][2]) for j in range(k0, cut + 1)]
        aeff = statistics.median(fl[j][3] for j in range(k0, cut + 1))
        ratio = (tail[-1] / tail[0]) ** (1.0 / max(len(tail) - 1, 1))
        print(f"   cut (eps={eps:g}) at floor #{cut}: N={x[0]:.6f} T_J~{TJ(x[0]):.4g} MeV, steps={x[8]} "
              f"of {r['steps']}; cascade from #{k0} (N={fl[k0][0]:.4f}, |pi|={ref_pi / Mp:.3e}) "
              f"{cut - k0} bounces; per-bounce |pi| ratio {ratio:.4f} vs sqrt(1-median a_eff) "
              f"{sqrt(1 - aeff):.4f}")
        after = [y for y in fl[cut:]]
        print(f"   after the cut: {len(after)} more floor reflections, drho/rho_R {sum(y[6] for y in after):.2e}, "
              f"{r['steps'] - x[8]} more steps, N span {fl[-1][0] - x[0]:.2e}")
