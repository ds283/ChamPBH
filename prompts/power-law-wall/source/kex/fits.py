"""
power-law-wall prompt 07b §1.5 (P79; U13): the candidate laws for `one-loop-exponential-v2`, against every exponential
stage-2 point in its new regime, at n = 1 and separately at n = 2. Measured, not ruled.

**The points.** crossover's rows (rechain.exponential_rows: stage2_scan and stage2_mscan at n = 1, stage2_n2 at n = 2),
and the new points of §1.4 (.documents/power-law-wall/kex-07b/stage2_new.txt), each re-chained here in both forms
(kexchain). The regime is the chain's: classical at ln R <= 0 (left out: the law there is R S(L), not a candidate's),
one-loop where the chain has a solution, gap otherwise.

**The method** is the copied crossover/fit_law.py's: scipy.optimize.curve_fit (its default method, maxfev 50000),
unweighted least squares in f, the fitted forms uncapped, p0 as fit_law's; and its selection:
  - one-loop: the chain exists, ell >= 1, the field came back out;
  - gap: no chain, ln R > 0.3, back out, not a `curve` set, not a `grid` row above ln R = 15 (n = 1); at n = 2
    crossover's U15 selection, ln(V0/M^4) <= 10 (its README, "After closing");
  - the plateau (ell < 1 with a chain, back out) and the cap (the largest f) are reported as measured, not fitted.
The reproduction: run on crossover's own rows as its fit_law.scan_rows reads them (the copy's, with the recorded ell),
this module's fit returns crossover's constants to their printed digits (fit_law.txt).

**The candidates**, y = -ln(1 - f):
  (i)   v1's form refitted: one-loop (ell >= 1) y = 2.69/(ell + B0 - C ln L), B0 and C fitted; the plateau below ell = 1,
        its value the mean measured f there; the gap y = G (ln R - R0)^P (1 + Q ln L) refitted on the points that are
        gap now;
  (ii)  as (i) with A free;
  (iii) the regimes split at ell_g: the one-loop form (A = 2.69) at chain points with ell >= ell_g, the gap's form at
        chain points with ell < ell_g and at every gap point; ell_g fitted by a scan from 0 to 2.5 in steps of 0.05,
        both branches refitted at each, the least total sum of squares chosen;
  (iv)  proposed here: f continuous across the regimes, y = min(y_rise, y_fall), y_rise = G (ln R - R0)^P (1 + Q ln L)
        and y_fall = A/(ell + B0 - C ln L) (A = 2.69; infinite where there is no chain or the denominator is not
        positive), all six fitted together on the one-loop points (every ell) and the gap points. Its reason: it is the
        power wall's ruled structure (R7: a rising branch in ln R and a falling branch, switched where they cross, so f
        is continuous), with the exponential's second variable L kept in each branch; the regime then only decides
        whether the falling branch exists, and no plateau or threshold is needed.
For each candidate and each n: the parameters, and the largest residual, the rms and the mean (fit - f) over the
one-loop points, the plateau's (where the candidate has one) and the gap points; v1 (its constants, drain_fraction's
form with the cap) beside it, on the old rows in the old form, on the new points in the old form, and on every point in
the new form.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B fits.py \\
        > ../../../../.documents/power-law-wall/kex-07b/fits.txt
It also writes the fitted parameters to .documents/power-law-wall/kex-07b/fits.json, which paths.py reads.
"""

import json
import os
import sys
import warnings
from math import exp, log, pi

import numpy as np
from scipy.optimize import curve_fit

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402

warnings.simplefilter("ignore")
FL = kc.load_aside("crossover", ("fit_law",), "xo")["fit_law"]
XO_DIR = os.path.join(rechain.ROOT, "prompts/one-loop-drain/source/crossover")
OUT = os.path.join(rechain.ROOT, ".documents/power-law-wall/kex-07b")
A_BORN = 2.69

# v1: drain_fraction's constants (ComputeTargets/ScalarHistory.py, DRAIN_*; crossover's fit_law.txt)
V1 = dict(
    A=2.69,
    B0=0.9691,
    C=0.41,
    plateau=0.9,
    ell_plateau=1.0,
    cap=0.95,
    G=0.0198,
    R0=1.5601,
    P=1.5472,
    Q=0.841,
)


def v1_fraction(reg, ell, L, lnR):
    """drain_fraction (one-loop, gap) term for term with v1's constants; None for classical (not compared)."""
    if reg == "one_loop":
        if ell < V1["ell_plateau"]:
            return V1["plateau"]
        d = ell + V1["B0"] - V1["C"] * log(L)
        return min(V1["cap"], 1.0 - exp(-V1["A"] / d)) if d > 0.0 else V1["cap"]
    if reg == "gap":
        if lnR <= V1["R0"]:
            return 0.0
        s = 1.0 + V1["Q"] * log(L)
        if s <= 0.0:
            return 0.0
        return 1.0 - exp(-V1["G"] * (lnR - V1["R0"]) ** V1["P"] * s)
    return None


def ln_v0m4(L, lnR, n):
    if n == 1:
        return lnR + log(8 * pi**2) + log(exp(L) - 1) - 2 * L - 2 * log(L**4 + 2 * L**3)
    return lnR - (
        2 * L
        - log(exp(L) - 1)
        + 2 * log(4 * L**2 + 6 * L)
        + 2 * log(L)
        - log(8 * pi**2)
    )


def new_points():
    out = []
    path = os.path.join(OUT, "stage2_new.txt")
    for line in open(path):
        if line.startswith("#") or "frac" not in line or "FAILED" in line:
            continue
        head, rest = line.split(" n=", 1)
        f = rest.split()
        n = int(f[0])
        L = float(rest.split("L=")[1].split()[0])
        lnR = float(rest.split("lnR=")[1].split()[0])
        frac = float(rest.split("| frac ")[1].split()[0])
        back = rest.split(" back ")[1].split()[0] == "True"
        out.append(
            dict(
                file="new (07b)",
                set=head.strip(),
                idx=-1,
                n=n,
                L=L,
                lnR=lnR,
                frac=frac,
                back=back,
            )
        )
    return out


def points():
    """Every exponential point with its chains: dict(..., ell_old, ell_new, reg_old, reg_new, lv)."""
    pts = rechain.exponential_rows() + new_points()
    for p in pts:
        w = rechain.wall_of(p)
        R = exp(p["lnR"])
        for form, key in (("lambda", "old"), ("v3", "new")):
            ch = kc.chain(w, R, form)
            p[f"ell_{key}"] = None if ch is None else ch[3]
            p[f"reg_{key}"] = kc.regime(p["lnR"], ch)
        p["lv"] = ln_v0m4(p["L"], p["lnR"], p["n"])
    return pts


def n2_ok(p):
    """crossover's U15 selection at n = 2 (its README, "After closing"): ln(V0/M^4) <= 10, in every regime."""
    return p["n"] == 1 or p["lv"] <= 10.0


def sel_one(p, key, ell_min=1.0):
    return (
        p[f"reg_{key}"] == "one_loop"
        and p["back"]
        and p[f"ell_{key}"] >= ell_min
        and n2_ok(p)
    )


def sel_plateau(p, key):
    return (
        p[f"reg_{key}"] == "one_loop"
        and p["back"]
        and p[f"ell_{key}"] < 1.0
        and n2_ok(p)
    )


def gap_ok(p):
    if p["n"] == 1:
        return not p["set"].startswith("curve") and not (
            p["set"] == "grid" and p["lnR"] > 15.0
        )
    return n2_ok(p)


def sel_gap(p, key):
    return p[f"reg_{key}"] == "gap" and p["lnR"] > 0.3 and p["back"] and gap_ok(p)


def planner_ok(p):
    """note 07b §0.3's selection for the one-loop refit: no `curve` sets, no grid row above ln R = 15."""
    return not p["set"].startswith("curve") and not (
        p["set"] == "grid" and p["lnR"] > 15.0
    )


# ---- the forms -------------------------------------------------------------------------------------------------------


def f_one(Z, A, B0, C):
    return 1.0 - np.exp(-A / (Z[0] + B0 - C * np.log(Z[1])))


def f_gap(Z, G, R0, P, Q):
    return 1.0 - np.exp(
        -G * np.clip(Z[0] - R0, 0.0, None) ** P * (1 + Q * np.log(Z[1]))
    )


def y_iv(Z, G, R0, P, Q, B0, C):
    """Z = (ell, or -1 where there is no chain, L, ln R): min(y_rise, y_fall), y_fall infinite where there is no chain
    or the denominator is not positive."""
    ell, L, lnR = Z
    rise = G * np.clip(lnR - R0, 0.0, None) ** P * np.clip(1 + Q * np.log(L), 0.0, None)
    d = ell + B0 - C * np.log(L)
    fall = np.where((ell < 0) | (d <= 0), np.inf, A_BORN / np.where(d > 0, d, 1.0))
    return np.minimum(rise, fall)


def f_iv(Z, *p):
    return 1.0 - np.exp(-y_iv(Z, *p))


def fit(fn, X, F, p0):
    p, _ = curve_fit(fn, X, F, p0=p0, maxfev=50000)
    return p


def stats(res):
    res = np.asarray(res, dtype=float)
    if res.size == 0:
        return "no points"
    return (
        f"max |res| {np.abs(res).max():.3f}  rms {np.sqrt(np.mean(res**2)):.3f}  mean {res.mean():+.3f}  "
        f"({res.size} points)"
    )


def X1(ps, key):
    return (np.array([p[f"ell_{key}"] for p in ps]), np.array([p["L"] for p in ps]))


def X2(ps):
    return (np.array([p["lnR"] for p in ps]), np.array([p["L"] for p in ps]))


def F(ps):
    return np.array([p["frac"] for p in ps])


# ---- the reproduction of crossover's constants ---------------------------------------------------------------------


def reproduce():
    cwd = os.getcwd()
    os.chdir(XO_DIR)
    try:
        rows = FL.scan_rows()  # the copy's reader, on crossover's own out/ (read only)
    finally:
        os.chdir(cwd)
    one = [r for r in rows if r[3] is not None and r[5] and r[3] >= 1.0]
    Xo = (np.array([r[3] for r in one]), np.array([r[1] for r in one]))
    Fo = np.array([r[4] for r in one])
    b = fit(lambda Z, b: 1 - np.exp(-2.69 / (Z[0] + b)), Xo, Fo, (0.12,))
    p1 = fit(lambda Z, b0, c: f_one(Z, 2.69, b0, c), Xo, Fo, (0.5, 0.3))
    gap = [
        r
        for r in rows
        if r[3] is None
        and r[2] > 0.3
        and r[5]
        and not r[0].startswith("curve")
        and not (r[0] == "grid" and r[2] > 15.0)
    ]
    Xg = (np.array([r[2] for r in gap]), np.array([r[1] for r in gap]))
    Fg = np.array([r[4] for r in gap])
    pg = fit(f_gap, Xg, Fg, (0.05, 1.0, 1.5, 0.5))
    r1 = f_one(Xo, 2.69, *p1) - Fo
    rg = f_gap(Xg, *pg) - Fg
    print(
        "## the reproduction: this module's fit on crossover's rows as the copied fit_law.scan_rows reads them"
    )
    print(
        f"  old form, b only: {np.round(b, 4).tolist()} (fit_law.txt: [0.0505]); one-loop b0, c: "
        f"{np.round(p1, 4).tolist()} (fit_law.txt: [0.9691, 0.41]), max {np.abs(r1).max():.3f} rms "
        f"{np.sqrt(np.mean(r1**2)):.3f} ({len(Fo)} points; 0.074, 0.021, 27); gap a, c, p, b: "
        f"{np.round(pg, 4).tolist()} (fit_law.txt: [0.0198, 1.5601, 1.5472, 0.841]), max {np.abs(rg).max():.3f} rms "
        f"{np.sqrt(np.mean(rg**2)):.3f} ({len(Fg)} points; 0.062, 0.024, 34)"
    )
    ok = (
        np.round(b, 4).tolist() == [0.0505]
        and np.round(p1, 4).tolist() == [0.9691, 0.41]
        and np.round(pg, 4).tolist() == [0.0198, 1.5601, 1.5472, 0.841]
    )
    print(f"  reproduced to the printed digits: {ok}")
    return ok


# ---- the candidates --------------------------------------------------------------------------------------------------


def residuals_law(ps, law, key):
    """{group: [fit - f]} for a law(p) -> f over the groups one_loop (ell >= 1), plateau (ell < 1), gap."""
    out = {"one-loop": [], "plateau": [], "gap": []}
    for p in ps:
        if sel_one(p, key):
            out["one-loop"].append(law(p) - p["frac"])
        elif sel_plateau(p, key):
            out["plateau"].append(law(p) - p["frac"])
        elif sel_gap(p, key):
            out["gap"].append(law(p) - p["frac"])
    return out


def print_res(name, res):
    for g in ("one-loop", "plateau", "gap"):
        if res[g]:
            print(f"    {name:44s} {g:9s} {stats(res[g])}")


def candidates(ps, n, sel_extra=None, label=""):
    key = "new"
    keep = [
        p
        for p in ps
        if p["n"] == n and p["lnR"] > 0.0 and (sel_extra is None or sel_extra(p))
    ]
    one = [p for p in keep if sel_one(p, key)]
    pla = [p for p in keep if sel_plateau(p, key)]
    gap = [p for p in keep if sel_gap(p, key)]
    print(
        f"\n## n = {n}{label}: {len(one)} one-loop points (ell >= 1, back out), {len(pla)} plateau points (ell < 1), "
        f"{len(gap)} gap points"
    )
    allf = [p["frac"] for p in keep]
    back = [p for p in keep if p["back"]]
    print(
        f"  measured: the cap, largest f {max(allf):.4f} over all {len(keep)} points ({max(p['frac'] for p in back):.4f} "
        f"where the field came back out); the plateau (ell < 1, back out): "
        + (
            f"mean {np.mean(F(pla)):.4f}, min {F(pla).min():.4f}, max {F(pla).max():.4f}"
            if pla
            else "no points"
        )
    )
    params = {}
    # (i)
    pi1 = fit(
        lambda Z, b0, c: f_one(Z, A_BORN, b0, c), X1(one, key), F(one), (0.5, 0.3)
    )
    pg = fit(f_gap, X2(gap), F(gap), (0.05, 1.0, 1.5, 0.5))
    plat = float(np.mean(F(pla))) if pla else V1["plateau"]

    def law_i(p, A=A_BORN, B0=pi1[0], C=pi1[1], pg=pg, plat=plat):
        if p["reg_new"] == "one_loop":
            if p["ell_new"] < 1.0:
                return plat
            return float(f_one((p["ell_new"], p["L"]), A, B0, C))
        return float(f_gap((p["lnR"], p["L"]), *pg))

    params["i"] = dict(
        A=A_BORN, B0=pi1[0], C=pi1[1], plateau=plat, G=pg[0], R0=pg[1], P=pg[2], Q=pg[3]
    )
    print(
        f"  (i)   A 2.69 held, B0 {pi1[0]:.4f}, C {pi1[1]:.4f}; plateau {plat:.4f} (measured mean); gap G {pg[0]:.4f}, "
        f"R0 {pg[1]:.4f}, P {pg[2]:.4f}, Q {pg[3]:.4f}"
    )
    print_res("(i)", residuals_law(keep, law_i, key))
    # (ii)
    pi2 = fit(f_one, X1(one, key), F(one), (A_BORN, 0.5, 0.3))
    params["ii"] = dict(
        A=pi2[0], B0=pi2[1], C=pi2[2], plateau=plat, G=pg[0], R0=pg[1], P=pg[2], Q=pg[3]
    )
    print(
        f"  (ii)  A {pi2[0]:.4f}, B0 {pi2[1]:.4f}, C {pi2[2]:.4f}; plateau and gap as (i)"
    )
    print_res("(ii)", residuals_law(keep, lambda p: law_i(p, *pi2), key))
    # (iii)
    best = None
    for ell_g in np.arange(0.0, 2.5001, 0.05):
        o = [
            p
            for p in keep
            if p["reg_new"] == "one_loop"
            and p["back"]
            and n2_ok(p)
            and p["ell_new"] >= ell_g
        ]
        g = [
            p
            for p in keep
            if p["lnR"] > 0.3
            and p["back"]
            and gap_ok(p)
            and (
                p["reg_new"] == "gap"
                or (p["reg_new"] == "one_loop" and p["ell_new"] < ell_g)
            )
        ]
        if len(o) < 3 or len(g) < 5:
            continue
        try:
            po = fit(
                lambda Z, b0, c: f_one(Z, A_BORN, b0, c), X1(o, key), F(o), (0.5, 0.3)
            )
            pgg = fit(f_gap, X2(g), F(g), tuple(pg))
        except RuntimeError:
            continue
        sse = float(
            np.sum((f_one(X1(o, key), A_BORN, *po) - F(o)) ** 2)
            + np.sum((f_gap(X2(g), *pgg) - F(g)) ** 2)
        )
        if best is None or sse < best[0]:
            best = (sse, float(ell_g), po, pgg)
    sse, ell_g, po, pgg = best

    def law_iii(p):
        if p["reg_new"] == "one_loop" and p["ell_new"] >= ell_g:
            return float(f_one((p["ell_new"], p["L"]), A_BORN, *po))
        return float(f_gap((p["lnR"], p["L"]), *pgg))

    params["iii"] = dict(
        ell_g=ell_g,
        A=A_BORN,
        B0=po[0],
        C=po[1],
        G=pgg[0],
        R0=pgg[1],
        P=pgg[2],
        Q=pgg[3],
    )
    print(
        f"  (iii) ell_g {ell_g:.2f} (scan 0-2.5 by 0.05, least total SS {sse:.4f}); one-loop above it A 2.69, B0 "
        f"{po[0]:.4f}, C {po[1]:.4f}; gap form below it and in the gap: G {pgg[0]:.4f}, R0 {pgg[1]:.4f}, P {pgg[2]:.4f}, "
        f"Q {pgg[3]:.4f}"
    )
    res = {"one-loop": [], "plateau": [], "gap": []}
    for p in keep:
        if p["reg_new"] == "one_loop" and p["back"] and n2_ok(p):
            grp = "one-loop" if p["ell_new"] >= 1.0 else "plateau"
            res[grp].append(law_iii(p) - p["frac"])
        elif sel_gap(p, key):
            res["gap"].append(law_iii(p) - p["frac"])
    print_res("(iii) [plateau: the chain points with ell < 1]", res)
    # (iv)
    iv_pts = [
        p
        for p in keep
        if (p["reg_new"] == "one_loop" and p["back"] and n2_ok(p)) or sel_gap(p, key)
    ]
    Z = (
        np.array([-1.0 if p["ell_new"] is None else p["ell_new"] for p in iv_pts]),
        np.array([p["L"] for p in iv_pts]),
        np.array([p["lnR"] for p in iv_pts]),
    )
    p4 = fit(f_iv, Z, F(iv_pts), (pg[0], pg[1], pg[2], pg[3], pi1[0], pi1[1]))
    params["iv"] = dict(
        G=p4[0], R0=p4[1], P=p4[2], Q=p4[3], A=A_BORN, B0=p4[4], C=p4[5]
    )

    def law_iv(p):
        z = (
            np.array([-1.0 if p["ell_new"] is None else p["ell_new"]]),
            np.array([p["L"]]),
            np.array([p["lnR"]]),
        )
        return float(f_iv(z, *p4)[0])

    print(
        f"  (iv)  y = min(G (ln R - R0)^P (1 + Q ln L), 2.69/(ell + B0 - C ln L)): G {p4[0]:.4f}, R0 {p4[1]:.4f}, P "
        f"{p4[2]:.4f}, Q {p4[3]:.4f}, B0 {p4[4]:.4f}, C {p4[5]:.4f}"
    )
    res = {"one-loop": [], "plateau": [], "gap": []}
    for p in keep:
        if p["reg_new"] == "one_loop" and p["back"] and n2_ok(p):
            res["one-loop" if p["ell_new"] >= 1.0 else "plateau"].append(
                law_iv(p) - p["frac"]
            )
        elif sel_gap(p, key):
            res["gap"].append(law_iv(p) - p["frac"])
    print_res("(iv) [plateau: the chain points with ell < 1]", res)
    # v1 beside
    print("  v1 (its constants, with the cap and the plateau), beside:")
    old_rows = [p for p in keep if p["file"] != "new (07b)"]
    new_rows = [p for p in keep if p["file"] == "new (07b)"]
    for name, rows, key2 in (
        ("v1 on crossover's rows, old form", old_rows, "old"),
        ("v1 on the new points, old form", new_rows, "old"),
        ("v1 on crossover's rows, new form", old_rows, "new"),
        ("v1 on every point, new form", keep, "new"),
    ):
        res = residuals_law(
            rows,
            lambda p, k=key2: v1_fraction(
                p[f"reg_{k}"], p[f"ell_{k}"], p["L"], p["lnR"]
            ),
            key2,
        )
        print_res(name, res)
    # every point, with each law
    print(
        "  per point (back out unless marked): set, L, ln R, ell old -> new, regime old -> new, f, v1(old), (i), (ii), (iii), (iv)"
    )
    for p in keep:
        vals = [
            v1_fraction(p["reg_old"], p["ell_old"], p["L"], p["lnR"]),
            law_i(p),
            law_i(p, *pi2),
            law_iii(p),
            law_iv(p),
        ]
        e0 = "none" if p["ell_old"] is None else f"{p['ell_old']:.3f}"
        e1 = "none" if p["ell_new"] is None else f"{p['ell_new']:.3f}"
        print(
            f"    {p['set']:22s} {p['L']:8.3f} {p['lnR']:8.3f} {e0:>7s} -> {e1:<7s} {p['reg_old']:>8s} -> "
            f"{p['reg_new']:<8s} f {p['frac']:.4f} {'' if p['back'] else '(not out)'} | "
            + " ".join(f"{v:.3f}" for v in vals)
        )
    return params


def main():
    print(f"# fits.py; start: {os.popen('uptime').read().strip()}")
    ok = reproduce()
    ps = points()
    print(
        f"\n# {len(ps)} exponential points: {sum(p['file'] != 'new (07b)' for p in ps)} of crossover's, "
        f"{sum(p['file'] == 'new (07b)' for p in ps)} new (07b)"
    )
    out = {"reproduced": ok, "n1": candidates(ps, 1), "n2": candidates(ps, 2)}
    # note 07b §0.3's selection (the gap's exclusions applied to the one-loop points too) is reported beside: it leaves
    # out the off-curve grid corners (L <= 8, ln R = 20, 25) that the new form turns from gap into one-loop
    out["n1_planner_selection"] = candidates(
        ps,
        1,
        sel_extra=planner_ok,
        label=" (note 07b §0.3's selection: no curve sets, no grid row above ln R = 15)",
    )
    out["n1_planner_selection_crossover_rows"] = candidates(
        ps,
        1,
        sel_extra=lambda p: planner_ok(p) and p["file"] != "new (07b)",
        label=" (the same selection on crossover's rows alone, as note 07b §0.3 fitted them)",
    )
    with open(os.path.join(OUT, "fits.json"), "w") as fh:
        json.dump(
            {
                k: (
                    {kk: {a: float(b) for a, b in vv.items()} for kk, vv in v.items()}
                    if isinstance(v, dict)
                    else v
                )
                for k, v in out.items()
            },
            fh,
            indent=1,
        )
    print(f"\n# fits.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
