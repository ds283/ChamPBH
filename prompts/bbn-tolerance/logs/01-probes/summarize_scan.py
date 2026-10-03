"""
bbn-tolerance prompt 01: summarise scan.csv (blocks S1-S3) per setting.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/summarize_scan.py [CSV] [--detail]

For each (block, network, rtol, atol) setting: the number of failed solves and
which; per history the spread (max - min) / median over prod, pert12, pert9 of
D/H and Yp (only when all three completed); the maximum and median spread over
the histories; the SM baseline and its shift from the default setting of the
same network. With --detail, one line per history.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent


def spread(vals):
    v = sorted(vals)
    return (v[-1] - v[0]) / float(np.median(v))


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    detail = "--detail" in sys.argv
    path = args[0] if args else HERE / "scan.csv"
    rows = list(csv.DictReader(open(path)))

    settings = defaultdict(list)
    for r in rows:
        key = (
            r["tag"],
            r["network"],
            r["lowT_rtol"] or "default",
            r["lowT_atol"] or "default",
        )
        settings[key].append(r)

    sm_default = {}
    for key, rs in settings.items():
        for r in rs:
            if r["input"] == "SM" and key[2] == "default" and key[3] == "default":
                sm_default[key[1]] = r

    def order(k):
        rt = k[2]
        return (k[0], k[1], k[3], -1.0 if rt == "default" else -float(rt))

    for key in sorted(settings, key=order):
        rs = settings[key]
        tag, net, rtol, atol = key
        fails = [r for r in rs if r["status"] != "ok"]
        by_hist = defaultdict(dict)
        for r in rs:
            if r["input"] != "SM":
                by_hist[(float(r["beta"]), float(r["M_Mp"]))][r["variant"]] = r
        sp_D, sp_Y, incomplete = {}, {}, []
        for h, vs in by_hist.items():
            ok = [v for v in vs.values() if v["status"] == "ok"]
            if len(vs) == 3 and len(ok) == 3:
                sp_D[h] = spread([float(v["DOverH"]) for v in ok])
                sp_Y[h] = spread([float(v["Yp"]) for v in ok])
            elif len(vs) == 3:
                incomplete.append(h)
        print(
            f"\n[{tag}] {net} network, low-T rtol={rtol} atol={atol}: {len(rs)} solves, "
            f"{len(fails)} failed"
        )
        for r in fails:
            what = (
                "SM"
                if r["input"] == "SM"
                else f"beta={r['beta']} M={r['M_Mp']} {r['variant']}"
            )
            print(
                f"    FAILED {what}: {r['failure_stage']} t={r['t_reached']} / {r['t_target']} "
                f"{r['failure_reason'][:110]}"
            )
        if sp_D:
            D = np.array(list(sp_D.values()))
            Y = np.array(list(sp_Y.values()))
            hD = max(sp_D, key=sp_D.get)
            hY = max(sp_Y, key=sp_Y.get)
            print(
                f"    D/H spread over {len(D)} histories: max {D.max():.3e} (beta={hD[0]:g} M={hD[1]:g}), "
                f"median {np.median(D):.3e}, histories >= 1e-4: {int((D >= 1e-4).sum())}"
            )
            print(
                f"    Yp  spread: max {Y.max():.3e} (beta={hY[0]:g} M={hY[1]:g}), median {np.median(Y):.3e}"
            )
        if incomplete:
            print(f"    histories with a failed variant: {sorted(incomplete)}")
        for r in rs:
            if r["input"] == "SM":
                base = sm_default.get(net)
                if r["status"] == "ok" and base is not None and base["status"] == "ok":
                    dY = abs(float(r["Yp"]) - float(base["Yp"])) / float(base["Yp"])
                    dD = abs(float(r["DOverH"]) - float(base["DOverH"])) / float(
                        base["DOverH"]
                    )
                    print(
                        f"    SM: Yp {float(r['Yp']):.10g} D/H {float(r['DOverH']):.10g} "
                        f"He3/H {float(r['He3OverH']):.10g} Li7/H {float(r['Li7OverH']):.10g}; "
                        f"shift from default: Yp {dY:.3e}, D/H {dD:.3e}; wall {float(r['wall_s']):.0f} s (loaded)"
                    )
                else:
                    print(f"    SM: {r['status']} {r['failure_reason'][:100]}")
        if detail:
            for h in sorted(by_hist):
                vs = by_hist[h]
                cells = " ".join(
                    f"{k}:{'%.10g' % float(vs[k]['DOverH']) if vs[k]['status'] == 'ok' else 'FAIL'}"
                    for k in ("prod", "pert12", "pert9")
                    if k in vs
                )
                s = (
                    f" D/H spread {sp_D[h]:.2e} Yp spread {sp_Y[h]:.2e}"
                    if h in sp_D
                    else ""
                )
                print(f"      beta={h[0]:g} M={h[1]:g}: {cells}{s}")


if __name__ == "__main__":
    main()
