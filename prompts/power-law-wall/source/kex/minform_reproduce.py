"""
power-law-wall prompt 07b2 §1.1: the reproduction, before any new fit.

1. 07b's candidates (iii) and (iv), re-fitted here with this module's own forms (scalar, written from fits.py's
   docstring, not its functions) and scipy.optimize.curve_fit as 07b called it (default method, maxfev 50000, the same
   starting points: (iii) one-loop (0.5, 0.3), gap from the gap fit's optimum, which starts at (0.05, 1.0, 1.5, 0.5);
   (iv) (gap fit's G, R0, P, Q, (i)'s B0, C)), on 07b's points (fits.points()), at n = 1 under fit_law's selection and
   under note 07b §0.3's, and at n = 2; each constant against .documents/power-law-wall/kex-07b/fits.json at the digits
   fits.txt prints (four decimals; ell_g two). A mismatch is a stop (prompt §1.1).
2. (v) on the orchestrator's scratch selection (n = 1, back out, ln R > 0.3, planner_ok, and the new regime one-loop at
   every ell or gap; minform.in_S2 restricted to n = 1), start (0.02, 1.6, 1.5, 1.0, 2.6, 1.1), maxfev 100000, against
   README §6.13's printed G 0.0180, R0 1.610, P 1.546, Q 1.008, A 2.42, ell0 1.243, max 0.071 one-loop / 0.046 gap. A
   mismatch there is recorded, not a stop.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_reproduce.py \\
        > ../../../../.documents/power-law-wall/kex-07b2/reproduce.txt
"""

import os
import sys
import warnings
from math import exp, inf, log

import numpy as np
from scipy.optimize import curve_fit

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fits  # noqa: E402
import minform as mf  # noqa: E402

warnings.simplefilter("ignore")
A = 2.69


def one(ell, L, B0, C):
    return 1.0 - np.exp(-A / (ell + B0 - C * log(L)))


def one_free(ell, L, A_, B0, C):
    return 1.0 - np.exp(-A_ / (ell + B0 - C * log(L)))


def gap(lnR, L, G, R0, P, Q):
    d = lnR - R0
    d = d if d > 0.0 else 0.0
    return 1.0 - np.exp(-G * d**P * (1.0 + Q * log(L)))


def iv(ell, L, lnR, G, R0, P, Q, B0, C):
    d = lnR - R0
    s = 1.0 + Q * log(L)
    rise = G * (d if d > 0.0 else 0.0) ** P * (s if s > 0.0 else 0.0)
    den = ell + B0 - C * log(L) if ell is not None else -1.0
    fall = A / den if den > 0.0 else inf
    return 1.0 - exp(-min(rise, fall))


def cf(fn, pts, p0, maxfev=50000):
    x = np.arange(len(pts), dtype=float)
    F = np.array([p["frac"] for p in pts])

    def model(idx, *q):
        return np.array([fn(pts[int(i)], *q) for i in idx])

    p, _ = curve_fit(model, x, F, p0=p0, maxfev=maxfev)
    return p, float(np.sum((model(x, *p) - F) ** 2))


def candidates(ps, n, extra=None):
    key = "new"
    keep = [
        p for p in ps if p["n"] == n and p["lnR"] > 0.0 and (extra is None or extra(p))
    ]
    one_pts = [p for p in keep if fits.sel_one(p, key)]
    gap_pts = [p for p in keep if fits.sel_gap(p, key)]
    pi1, _ = cf(lambda p, b0, c: one(p["ell_new"], p["L"], b0, c), one_pts, (0.5, 0.3))
    pg, _ = cf(lambda p, *q: gap(p["lnR"], p["L"], *q), gap_pts, (0.05, 1.0, 1.5, 0.5))
    best = None
    for ell_g in np.arange(0.0, 2.5001, 0.05):
        o = [
            p
            for p in keep
            if p["reg_new"] == "one_loop"
            and p["back"]
            and fits.n2_ok(p)
            and p["ell_new"] >= ell_g
        ]
        g = [
            p
            for p in keep
            if p["lnR"] > 0.3
            and p["back"]
            and fits.gap_ok(p)
            and (
                p["reg_new"] == "gap"
                or (p["reg_new"] == "one_loop" and p["ell_new"] < ell_g)
            )
        ]
        if len(o) < 3 or len(g) < 5:
            continue
        try:
            po, so = cf(
                lambda p, b0, c: one(p["ell_new"], p["L"], b0, c), o, (0.5, 0.3)
            )
            pgg, sg = cf(lambda p, *q: gap(p["lnR"], p["L"], *q), g, tuple(pg))
        except RuntimeError:
            continue
        if best is None or so + sg < best[0]:
            best = (so + sg, float(ell_g), po, pgg)
    _, ell_g, po, pgg = best
    iv_pts = [
        p
        for p in keep
        if (p["reg_new"] == "one_loop" and p["back"] and fits.n2_ok(p))
        or fits.sel_gap(p, key)
    ]
    p4, _ = cf(
        lambda p, *q: iv(p["ell_new"], p["L"], p["lnR"], *q),
        iv_pts,
        tuple(pg) + tuple(pi1),
    )
    return dict(
        iii=dict(
            ell_g=ell_g, B0=po[0], C=po[1], G=pgg[0], R0=pgg[1], P=pgg[2], Q=pgg[3]
        ),
        iv=dict(G=p4[0], R0=p4[1], P=p4[2], Q=p4[3], B0=p4[4], C=p4[5]),
    )


def compare(label, mine, ref):
    ok = True
    parts = []
    for k, v in mine.items():
        nd = 2 if k == "ell_g" else 4
        a, b = round(v, nd), round(ref[k], nd)
        good = a == b
        ok &= good
        parts.append(f"{k} {v:.{nd}f}/{ref[k]:.{nd}f}{'' if good else ' MISMATCH'}")
    print(
        f"  {label:34s} {'reproduced' if ok else 'NOT REPRODUCED'}: " + ", ".join(parts)
    )
    return ok


def main():
    print(f"# minform_reproduce.py; start: {os.popen('uptime').read().strip()}")
    ps = fits.points()
    print(
        f"# 07b's points: {len(ps)} (fits.points()); 07b's constants from kex-07b/fits.json"
    )
    print(
        "## 1. 07b's (iii) and (iv) re-fitted by this module's own forms (mine / fits.json, at fits.txt's digits)"
    )
    allok = True
    for label, n, extra, key in (
        ("n = 1, fit_law's selection", 1, None, "n1"),
        (
            "n = 1, note 07b §0.3's selection",
            1,
            fits.planner_ok,
            "n1_planner_selection",
        ),
        ("n = 2, crossover's U15 selection", 2, None, "n2"),
    ):
        mine = candidates(ps, n, extra)
        for c in ("iii", "iv"):
            ref = {k: v for k, v in mf.F07B[key][c].items() if k != "A"}
            allok &= compare(f"{label}: ({c})", mine[c], ref)
    print(f"  07b's constants reproduced: {allok}")

    print(
        "\n## 2. (v) on the orchestrator's scratch selection (n = 1, back out, ln R > 0.3, planner_ok, chain or gap)"
    )
    pts, _ = mf.select(ps, 1, "S2")
    start = (0.02, 1.6, 1.5, 1.0, 2.6, 1.1)
    p, cov, sse = mf.fit_once("v", pts, 1, start)
    prm = mf.params_of("v", p, 1)
    q = prm["n1"]
    res = {"one-loop": [], "gap": []}
    for pt in pts:
        r = mf.fraction(pt["ell_new"], pt["L"], pt["lnR"], 1, prm) - pt["frac"]
        res["one-loop" if pt["reg_new"] == "one_loop" else "gap"].append(r)
    printed = dict(G=0.0180, R0=1.610, P=1.546, Q=1.008, A=2.42, ell0=1.243)
    digits = dict(G=4, R0=3, P=3, Q=3, A=2, ell0=3)
    ok = True
    parts = []
    for k, v in printed.items():
        good = round(q[k], digits[k]) == v
        ok &= good
        parts.append(
            f"{k} {q[k]:.{digits[k]}f} (§6.13 {v:.{digits[k]}f}){'' if good else ' MISMATCH'}"
        )
    m1 = max(abs(x) for x in res["one-loop"])
    mg = max(abs(x) for x in res["gap"])
    ok &= round(m1, 3) == 0.071 and round(mg, 3) == 0.046
    print(
        f"  {len(pts)} points ({len(res['one-loop'])} chain, {len(res['gap'])} gap); start {start}; SSE {sse:.6f}"
    )
    print("  " + ", ".join(parts))
    print(
        f"  max |res| chain points {m1:.3f} (§6.13 0.071), gap points {mg:.3f} (§6.13 0.046); full precision: "
        + ", ".join(f"{k} {v!r}" for k, v in q.items())
    )
    print(f"  README §6.13's numbers reproduced: {ok}")
    print(f"# minform_reproduce.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
