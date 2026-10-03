"""
bbn-tolerance prompt 01c: summarise scan.csv (T1, T2) against P11's criteria
1-3 and P12's offset from the full network.

    ./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/summarize.py [--detail]

For each T1 setting (small network, low-T rtol, atol as now):
  1. failed solves, each named with its stage, t reached / t target, reason;
  2. per history the spread (max - min) / median over prod, pert12, pert9 of
     D/H and Yp (when all three completed); a history passes criterion 2 if
     its D/H spread is < 1e-4 or <= 1.5x its own D/H spread at rtol 1e-8;
  3. per input (SM, and each history's prod) the relative difference from
     the same input at rtol 1e-8; criterion 3 is D/H <= 1e-4 and Yp <= 1e-5
     on every input.
P12: for each input common to T1 and log 01's S1 (full network, atol as now),
at rtol 1e-5, 1e-6 and 1e-8, (small - full) / full in Yp and D/H, with the
median and maximum |.| over inputs. T2: count, failures, and the range of Yp
and D/H. With --detail, per-history lines.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
OLD = HERE.parent / "01-probes" / "scan.csv"
RTOLS = ["default", "0.0001", "1e-05", "1e-06", "1e-08"]
REF = "1e-08"


def spread(vals):
    v = sorted(vals)
    return (v[-1] - v[0]) / float(np.median(v))


def rel(a, b):
    return (a - b) / b


def load(path, pred):
    return [r for r in csv.DictReader(open(path)) if pred(r)]


def describe(r):
    what = "SM" if r["input"] == "SM" else f"{r['input']} {r['variant']}"
    if r["input"] != "SM" and r["phi_init_Mp"] not in ("", "5.0"):
        what += f" phi={r['phi_init_Mp']}"
    t = (
        f"t={float(r['t_reached']):.6g} / {float(r['t_target']):.6g}"
        if r["t_reached"]
        else "no failed solve_ivp"
    )
    return f"{what}: stage '{r['failure_stage']}' {t}; {r['failure_reason'][:140]}"


def main():
    detail = "--detail" in sys.argv
    rows = load(HERE / "scan.csv", lambda r: True)
    t1 = defaultdict(dict)  # rtol -> (input, variant) -> row
    for r in rows:
        if r["tag"] == "T1":
            assert r["network"] == "small" and r["lowT_atol"] == ""
            t1[r["lowT_rtol"] or "default"][(r["input"], r["variant"])] = r
    full = defaultdict(dict)
    for r in load(OLD, lambda r: r["tag"] == "S1" and r["lowT_atol"] == ""):
        full[r["lowT_rtol"] or "default"][(r["input"], r["variant"])] = r

    hist_inputs = sorted({k[0] for k in t1["default"] if k[0] != "SM"})
    print(
        f"T1: {sum(len(v) for v in t1.values())} solves over {len(t1)} settings, "
        f"{len(hist_inputs)} histories + SM"
    )

    def spreads(rtol):
        sD, sY = {}, {}
        for h in hist_inputs:
            vs = [t1[rtol].get((h, v)) for v in ("prod", "pert12", "pert9")]
            if all(v is not None and v["status"] == "ok" for v in vs):
                sD[h] = spread([float(v["DOverH"]) for v in vs])
                sY[h] = spread([float(v["Yp"]) for v in vs])
        return sD, sY

    ref_D, _ = spreads(REF)
    summary = []
    for rtol in RTOLS:
        cell = t1[rtol]
        fails = [r for r in cell.values() if r["status"] != "ok"]
        sD, sY = spreads(rtol)
        # criterion 2
        miss2 = []
        for h in hist_inputs:
            if h not in sD:
                miss2.append((h, "incomplete"))
            elif not (sD[h] < 1e-4 or (h in ref_D and sD[h] <= 1.5 * ref_D[h])):
                miss2.append(
                    (h, f"{sD[h]:.3e} (1e-8: {ref_D.get(h, float('nan')):.3e})")
                )
        # criterion 3
        conv = {}
        for inp in ["SM"] + hist_inputs:
            k = ("SM", "SM") if inp == "SM" else (inp, "prod")
            a, b = cell.get(k), t1[REF].get(k)
            if a and b and a["status"] == "ok" and b["status"] == "ok":
                conv[inp] = (
                    abs(rel(float(a["DOverH"]), float(b["DOverH"]))),
                    abs(rel(float(a["Yp"]), float(b["Yp"]))),
                )
        cD = max(conv, key=lambda i: conv[i][0])
        cY = max(conv, key=lambda i: conv[i][1])
        miss3 = [i for i, (d, y) in conv.items() if d > 1e-4 or y > 1e-5]
        sm = cell[("SM", "SM")]
        print(
            f"\n[T1] small network, low-T rtol={rtol}, atol 1e-11: {len(cell)} solves"
        )
        print(f"  1. failed solves: {len(fails)}")
        for r in fails:
            print(f"     FAILED {describe(r)}")
        D = np.array(list(sD.values()))
        Y = np.array(list(sY.values()))
        hD = max(sD, key=sD.get)
        hY = max(sY, key=sY.get)
        print(
            f"  2. D/H spread over {len(D)} histories: max {D.max():.3e} ({hD}), median "
            f"{np.median(D):.3e}, >= 1e-4: {int((D >= 1e-4).sum())}; criterion 2 misses: "
            f"{len(miss2)} {miss2 if miss2 else ''}"
        )
        print(f"     Yp spread: max {Y.max():.3e} ({hY}), median {np.median(Y):.3e}")
        print(
            f"  3. against rtol 1e-8 over {len(conv)} inputs: D/H max {conv[cD][0]:.3e} ({cD}), "
            f"median {np.median([c[0] for c in conv.values()]):.3e}; Yp max {conv[cY][1]:.3e} ({cY}), "
            f"median {np.median([c[1] for c in conv.values()]):.3e}; inputs missing 3: "
            f"{len(miss3)}{' ' + str(sorted(miss3)) if miss3 else ''}"
        )
        print(
            f"  SM: Yp {float(sm['Yp']):.10g} D/H {float(sm['DOverH']):.10g} He3/H "
            f"{float(sm['He3OverH']):.10g} Li7/H {float(sm['Li7OverH']):.10g}"
        )
        summary.append(
            (
                rtol,
                len(fails),
                D.max(),
                len(miss2),
                conv[cD][0],
                conv[cY][1],
                len(miss3),
            )
        )
        if detail:
            for h in hist_inputs:
                cells = " ".join(
                    f"{v}:{'%.10g' % float(cell[(h, v)]['DOverH']) if cell[(h, v)]['status'] == 'ok' else 'FAIL'}"
                    for v in ("prod", "pert12", "pert9")
                    if (h, v) in cell
                )
                s = f" D/H spread {sD[h]:.2e} Yp spread {sY[h]:.2e}" if h in sD else ""
                c = (
                    f" | vs 1e-8: D/H {conv[h][0]:.2e} Yp {conv[h][1]:.2e}"
                    if h in conv
                    else ""
                )
                print(f"      {h}: {cells}{s}{c}")

    print("\nP11 criteria 1-3 by setting (T1):")
    print(
        f"  {'rtol':>8s} {'failed':>6s} {'max D/H spread':>15s} {'crit-2 misses':>13s} "
        f"{'max dD/H vs 1e-8':>17s} {'max dYp vs 1e-8':>16s} {'crit-3 misses':>13s}"
    )
    for s in summary:
        print(
            f"  {s[0]:>8s} {s[1]:6d} {s[2]:15.3e} {s[3]:13d} {s[4]:17.3e} {s[5]:16.3e} {s[6]:13d}"
        )

    # P12
    print(
        "\nP12: (small - full) / full, prod (and SM), log 01's S1 for the full network"
    )
    for rtol in ("1e-05", "1e-06", "1e-08"):
        offs = {}
        missing = []
        for inp in ["SM"] + hist_inputs:
            k = ("SM", "SM") if inp == "SM" else (inp, "prod")
            a, b = t1[rtol].get(k), full[rtol].get(k)
            if a and b and a["status"] == "ok" and b["status"] == "ok":
                offs[inp] = (
                    rel(float(a["Yp"]), float(b["Yp"])),
                    rel(float(a["DOverH"]), float(b["DOverH"])),
                )
            else:
                missing.append(inp)
        Yo = np.array([o[0] for o in offs.values()])
        Do = np.array([o[1] for o in offs.values()])
        print(
            f"  rtol {rtol}: {len(offs)} inputs"
            f"{' (no full-network value: ' + ', '.join(missing) + ')' if missing else ''}"
        )
        print(
            f"    Yp:  median {np.median(Yo):+.3e}, median |.| {np.median(abs(Yo)):.3e}, "
            f"max |.| {abs(Yo).max():.3e} ({max(offs, key=lambda i: abs(offs[i][0]))}), "
            f"range [{Yo.min():+.3e}, {Yo.max():+.3e}]"
        )
        print(
            f"    D/H: median {np.median(Do):+.3e}, median |.| {np.median(abs(Do)):.3e}, "
            f"max |.| {abs(Do).max():.3e} ({max(offs, key=lambda i: abs(offs[i][1]))}), "
            f"range [{Do.min():+.3e}, {Do.max():+.3e}]"
        )
        if detail:
            for inp, (y, d) in offs.items():
                print(f"      {inp:20s} Yp {y:+.3e} D/H {d:+.3e}")

    # T2
    t2 = [r for r in rows if r["tag"] == "T2"]
    if t2:
        settings = sorted({r["lowT_rtol"] or "default" for r in t2})
        fails = [r for r in t2 if r["status"] != "ok"]
        ok = [r for r in t2 if r["status"] == "ok"]
        print(
            f"\n[T2] breadth sample, small network, prod, low-T rtol {settings}: {len(t2)} solves "
            f"(phi* = 5: {sum(1 for r in t2 if r['phi_init_Mp'] == '5.0')}, "
            f"phi* != 5: {sum(1 for r in t2 if r['phi_init_Mp'] != '5.0')}), {len(fails)} failed"
        )
        for r in fails:
            print(f"     FAILED {describe(r)}")
        if ok:
            Yv = [float(r["Yp"]) for r in ok]
            Dv = [float(r["DOverH"]) for r in ok]
            print(
                f"  completed: Yp in [{min(Yv):.6g}, {max(Yv):.6g}], D/H in [{min(Dv):.6g}, {max(Dv):.6g}]"
            )


if __name__ == "__main__":
    main()
