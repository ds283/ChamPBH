"""
power-law-wall prompt 07b2 §1.6 (P96): the crossing, the cap, ln R -> 0+ and the chain's iteration limit, under (v)'s
S3 constants, without and with the new points (minform.json). Measured, not judged.

  - **The crossing.** On each history curve of §1.5 (the laboratory curve at n = 1 and n = 2, ln(V0/M^4) = 3.345;
    L-M1e-16, -129.27) and on L-M8e-18 (-119.16), and at each `grid` (n = 1) and `n2-grid` (n = 2) L, inside S3
    (ln(V0/M^4) <= 10): the ln R where y_rise = y_fall (brentq on y_rise - y_fall, bracketed by a scan of 400 steps in
    ln R), its ell_new and the predicted f there (the predicted peak, since y_rise rises and y_fall falls with ln R at
    fixed L); beside the largest measured f within 0.5 of it in ell_new on that curve or column, with and without the
    field back out.
  - **The cap.** The largest f (v) predicts inside S3 at each n: at each of 400 L in [0.5, 134] (geometric), the f at
    the crossing where it lies inside S3, else at S3's edge ln R = ln R(L, ln(V0/M^4) = 10) (f is largest there when
    the rise is active up to the edge); the largest measured f in S3's universe with and without the points whose field
    did not come back out. Then (v) capped at v1's 0.95, min(0.95, f), on each selection of §1.3: every residual that
    the cap changes.
  - **ln R -> 0+.** f under (v) at ln R = 1e-12 beside the classical R S(L) at ln R = 0 (production's drain_S_of_L), at
    the L where each history curve has ln R = 0.
  - **The chain's limit.** kexchain.chain_reason allows 200 iterations, production's one_loop_turnaround 100
    (ONE_LOOP_MAX_ITERATIONS). At the points 07b's issue names (the laboratory point at ell_new 0.120, L-M8e-18 at
    0.305) and along each history curve near the chain's start (200 points in L between where the chain begins and
    ell_new = 1): the iterations, and f under (v) with the chain found and with it taken as absent (the gap), and the
    difference.

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/minform_edges.py \\
        > .documents/power-law-wall/kex-07b2/edges.txt
"""

import json
import os
import sys
import warnings
from math import exp, log

import numpy as np
from scipy.optimize import brentq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import minform as mf  # noqa: E402
import minform_predict as mp  # noqa: E402
import rechain  # noqa: E402

from ComputeTargets.ScalarHistory import drain_S_of_L  # noqa: E402
import importlib  # noqa: E402

AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

warnings.simplefilter("ignore")
LV = {"lab": 3.345, "L-M1e-16": -129.27, "L-M8e-18": -119.16}
CURVES = (("lab", 1), ("L-M1e-16", 1), ("L-M8e-18", 1), ("lab", 2))


def lnR_curve(n, lv, L):
    mp.LV["_"] = lv
    return mp.lnR_on(n, "_", L)


def lnR_max(n, L):
    return lnR_curve(n, mf.LV_MAX, L)


def ell(n, L, lnR):
    return mp.ell_new(n, L, lnR)


def gdiff(prm, n, L, lnR):
    e = ell(n, L, lnR)
    _, r, fl = mf.y_of(e, L, lnR, n, prm)
    return r - fl if fl != float("inf") else -1e3


def crossing(prm, n, L, lo, hi, steps=400):
    rs = np.linspace(lo, hi, steps)
    prev = gdiff(prm, n, L, rs[0])
    for a, b in zip(rs[:-1], rs[1:]):
        cur = gdiff(prm, n, L, b)
        if prev < 0.0 <= cur:
            x = brentq(lambda r: gdiff(prm, n, L, r), a, b, xtol=1e-10)
            e = ell(n, L, x)
            return x, e, mf.fraction(e, L, x, n, prm)
        prev = cur
    return None


def params(J, key):
    return {
        "variant": "v",
        "n1": J[key]["n1/S3/v"]["params"]["n1"],
        "n2": J[key]["n2/S3/v"]["params"]["n2"],
    }


def near_measured(ps, n, e_x, sel):
    q = [
        p
        for p in ps
        if sel(p) and p["ell_new"] is not None and abs(p["ell_new"] - e_x) <= 0.5
    ]
    b = [p for p in q if p["back"]]
    fm = lambda xs: (
        "-"
        if not xs
        else f"{max(p['frac'] for p in xs):.4f} (ell {max(xs, key=lambda p: p['frac'])['ell_new']:.3f})"
    )  # noqa: E731
    return f"largest measured f within 0.5 in ell: {fm(q)} all, {fm(b)} back out ({len(q)}, {len(b)} points)"


def main():
    print(f"# minform_edges.py; start: {os.popen('uptime').read().strip()}")
    J = json.load(open(os.path.join(mf.OUT, "minform.json")))
    P = {k: params(J, k) for k in ("without_new", "with_new") if k in J}
    ps = mf.all_points(with_new=True)
    print(
        "# (v) on S3: "
        + "; ".join(
            f"{k}: n = 1 "
            + ", ".join(f"{a} {b:.4f}" for a, b in prm["n1"].items())
            + "; n = 2 "
            + ", ".join(f"{a} {b:.4f}" for a, b in prm["n2"].items())
            for k, prm in P.items()
        )
    )

    print(
        "\n## the crossing (y_rise = y_fall) inside S3, and the largest measured f near it"
    )
    for key, prm in P.items():
        print(f"  constants {key}:")
        for curve, n in CURVES:
            lv = LV[curve]
            # ln R on the curve as a function of L; find the crossing in L along the curve
            Ls = (
                np.linspace(1.0, 12.0, 300)
                if curve == "lab"
                else np.linspace(95.0, 125.0, 300)
            )
            vals = [gdiff(prm, n, L, lnR_curve(n, lv, L)) for L in Ls]
            x = None
            for i in range(len(Ls) - 1):
                if vals[i] < 0.0 <= vals[i + 1]:
                    Lx = brentq(
                        lambda L: gdiff(prm, n, L, lnR_curve(n, lv, L)),
                        Ls[i],
                        Ls[i + 1],
                        xtol=1e-10,
                    )
                    r = lnR_curve(n, lv, Lx)
                    e = ell(n, Lx, r)
                    x = (Lx, r, e, mf.fraction(e, Lx, r, n, prm))
                    break
            if x is None:
                print(f"    curve {curve} n = {n}: no crossing")
                continue
            sel = (
                lambda p, n=n, lv=lv: p["n"] == n and abs(p["lv"] - lv) < 0.2
            )  # noqa: E731
            print(
                f"    curve {curve:8s} n = {n} (ln(V0/M^4) {lv}): L {x[0]:.4f}, ln R {x[1]:.4f}, ell_new {x[2]:.4f}, "
                f"predicted peak f {x[3]:.4f}; {near_measured(ps, n, x[2], sel)}"
            )
        for n, gset in ((1, "grid"), (2, "n2-grid")):
            for L in sorted(set(p["L"] for p in ps if p["set"] == gset)):
                x = crossing(prm, n, L, 0.31, lnR_max(n, L))
                if x is None:
                    print(
                        f"    {gset} n = {n} L {L:6g}: no crossing inside S3 (ln R <= {lnR_max(n, L):.3f})"
                    )
                    continue
                sel = (
                    lambda p, n=n, L=L, gset=gset: p["set"] == gset and p["L"] == L
                )  # noqa: E731
                print(
                    f"    {gset} n = {n} L {L:6g}: ln R {x[0]:.4f}, ell_new {x[1]:.4f}, predicted peak f {x[2]:.4f}; "
                    f"{near_measured(ps, n, x[1], sel)}"
                )

    print(
        "\n## the cap: the largest f (v) predicts inside S3, and the largest measured"
    )
    for key, prm in P.items():
        for n in (1, 2):
            best = None
            for L in np.geomspace(0.5, 134.0, 400):
                hi = lnR_max(n, L)
                if hi <= mf.LNR_MIN:
                    continue
                x = crossing(prm, n, L, 0.31, hi, steps=120)
                if x is None:
                    e = ell(n, L, hi)
                    x = (hi, e, mf.fraction(e, L, hi, n, prm))
                if best is None or x[2] > best[3]:
                    best = (L, x[0], x[1], x[2])
            print(
                f"  {key} n = {n}: largest predicted f {best[3]:.4f} at L {best[0]:.3f}, ln R {best[1]:.3f}, ell_new "
                f"{'none' if best[2] is None else format(best[2], '.3f')}"
            )
    for n in (1, 2):
        u = [p for p in ps if p["n"] == n and p["lnR"] > mf.LNR_MIN and mf.in_S3(p)]
        b = [p for p in u if p["back"]]
        top = lambda xs: max(xs, key=lambda p: p["frac"])  # noqa: E731
        t, tb = top(u), top(b)
        print(
            f"  measured, n = {n}, S3 (every point, new ones included): largest f {t['frac']:.4f} ({t['set']}, L "
            f"{t['L']:.3f}, ln R {t['lnR']:.3f}, back out {t['back']}); with the field back out {tb['frac']:.4f} "
            f"({tb['set']}, L {tb['L']:.3f}, ln R {tb['lnR']:.3f})"
        )
    print(
        "  (v) capped at 0.95 on §1.3's selections (constants of each selection's own (v) fit): the residuals that change"
    )
    for key in P:
        for n in (1, 2):
            for S in ("S1", "S2", "S3") if n == 1 else ("S1", "S3"):
                prm = J[key][f"n{n}/{S}/v"]["params"]
                pts, _ = mf.select(mf.all_points(with_new=(key == "with_new")), n, S)
                ch = []
                for p in pts:
                    f = mf.fraction(p["ell_new"], p["L"], p["lnR"], n, prm)
                    if f > 0.95:
                        ch.append((p, f))
                full = [
                    mf.fraction(p["ell_new"], p["L"], p["lnR"], n, prm) - p["frac"]
                    for p in pts
                ]
                cap = [
                    min(0.95, mf.fraction(p["ell_new"], p["L"], p["lnR"], n, prm))
                    - p["frac"]
                    for p in pts
                ]
                print(
                    f"    {key} n = {n} {S}: {len(ch)} of {len(pts)} points have f > 0.95"
                    + "".join(
                        f"; {p['set']} L {p['L']:.3f} ln R {p['lnR']:.3f}: f {p['frac']:.4f}, (v) {f:.4f} -> 0.95"
                        for p, f in ch
                    )
                    + f"; all points uncapped {mf.fmt_stats(mf.stats(full))}, capped {mf.fmt_stats(mf.stats(cap))}"
                )

    print(
        "\n## ln R -> 0+: f under (v) at ln R = 1e-12 beside R S(L) at ln R = 0, at the L where each curve has ln R = 0"
    )
    for curve, n in CURVES + (("L-M5e-16", 1),):
        lv = {"L-M5e-16": -135.67}.get(curve, LV.get(curve))
        lo, hi = (0.05, 10.0) if curve == "lab" else (60.0, 125.0)
        L0 = brentq(lambda L: lnR_curve(n, lv, L), lo, hi, xtol=1e-12)
        for key, prm in P.items():
            e = ell(n, L0, 1e-12)
            f = mf.fraction(e, L0, 1e-12, n, prm)
            print(
                f"  curve {curve:8s} n = {n}: L {L0:.4f}; {key}: f(v) at 0+ {f:.3e} (R0 {prm[f'n{n}']['R0']:.4f}: f = 0 "
                f"for 0 < ln R <= R0); R S(L) at ln R = 0: {drain_S_of_L(L0):.4e} (production's table, n = 1's at either n)"
            )

    print(
        f"\n## the chain's limit: 200 iterations (kexchain) against production's {AM.ONE_LOOP_MAX_ITERATIONS}"
    )
    issue = (
        ("lab n=1 ell_new 0.120 (07b)", 1, 1.378072, 5.003334),
        ("L-M8e-18 ell_new 0.305 (07b)", 1, 93.765819, 6.605531),
    )
    for key, prm in P.items():
        print(f"  constants {key}:")
        for lab, n, L, r in issue:
            ch, why, it = kc.chain_reason(rechain.wall_of(dict(n=n, L=L)), exp(r), "v3")
            e = None if ch is None else ch[3]
            f1 = mf.fraction(e, L, r, n, prm)
            f0 = mf.fraction(None, L, r, n, prm)
            print(
                f"    {lab:30s} L {L:.6f} ln R {r:.6f}: {it} iterations ({why}), ell_new {e}; f(v) with the chain "
                f"{f1:.6f}, without {f0:.6f}, difference {f1 - f0:+.2e}"
            )
        for curve, n in CURVES:
            lv = LV[curve]
            lo, hi = (1.0, 4.0) if curve == "lab" else (90.0, 110.0)
            # where the chain begins on the curve, and where ell_new = 1
            Ls = np.linspace(lo, hi, 2000)
            have = [ell(n, L, lnR_curve(n, lv, L)) for L in Ls]
            i0 = next(i for i, e in enumerate(have) if e is not None)
            i1 = next(i for i, e in enumerate(have) if e is not None and e >= 1.0)
            over, worst = [], 0.0
            for L in np.linspace(Ls[max(i0 - 1, 0)], Ls[i1], 200):
                r = lnR_curve(n, lv, L)
                ch, why, it = kc.chain_reason(
                    rechain.wall_of(dict(n=n, L=L)), exp(r), "v3"
                )
                if ch is not None and it > AM.ONE_LOOP_MAX_ITERATIONS:
                    d = mf.fraction(ch[3], L, r, n, prm) - mf.fraction(
                        None, L, r, n, prm
                    )
                    over.append((L, ch[3], it, d))
                    worst = max(worst, abs(d))
            print(
                f"    curve {curve:8s} n = {n}: the chain begins near L {Ls[i0]:.4f} (ell_new {have[i0]:.4f}); of 200 "
                f"points up to ell_new = 1, {len(over)} need more than {AM.ONE_LOOP_MAX_ITERATIONS} iterations"
                + (
                    f" (L {over[0][0]:.4f}..{over[-1][0]:.4f}, ell_new {min(o[1] for o in over):.4f}.."
                    f"{max(o[1] for o in over):.4f}, up to {max(o[2] for o in over)} iterations); largest |f with - "
                    f"f without| {worst:.2e}"
                    if over
                    else ""
                )
            )
    print(
        "  every variant at the issue's two points (S3 constants; (v-J) its n = 1 part):"
    )
    for key in P:
        for v in ("v", "v-L", "v-A", "v-J", "vi"):
            prm = J[key]["n12/S3/v-J" if v == "v-J" else f"n1/S3/{v}"]["params"]
            diffs = []
            for lab, n, L, r in issue:
                ch = kc.chain(rechain.wall_of(dict(n=n, L=L)), exp(r), "v3")
                diffs.append(
                    mf.fraction(ch[3], L, r, n, prm) - mf.fraction(None, L, r, n, prm)
                )
            print(
                f"    {key} ({v}): f with the chain - without: "
                + ", ".join(f"{d:+.2e}" for d in diffs)
            )
    print(f"# minform_edges.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
