"""
power-law-wall prompt 07b2 §1.2-§1.4 (P92-P94), and §1.5's refits: the min(rise, fall) form and its variants fitted at
n = 1 and n = 2 on the selections S1-S3, scored like for like beside v1 and 07b's (i)-(iv), with errors and
leave-one-set-out. Measured, not ruled. The forms, points and selections are minform.py's.

**The fit** (prompt §1.2): unweighted least squares in f by scipy.optimize.curve_fit (its default method, maxfev 100000),
on the points the selection admits (chain points at every ell and gap points, the field back out, ln R > 0.3); the
model is minform.fraction mapped over the points. Every fit is started from the four starts of minform.starts (the
orchestrator's scratch start, v1's gap constants with A = 2.69, and two others); the least sum of squares is taken and
every start's optimum is printed, so that whether it depends on the start is shown. Standard errors are the square roots
of the covariance's diagonal as curve_fit returns it (absolute_sigma False: scaled by the residual variance).

**The like-for-like tables** (prompt §1.3): on each selection's points, every law is scored by fit - f, max |res| / rms
/ mean (count), over (a), (b), (c) and (d) all. For a variant of this prompt (a) is the chain points where its fall is
the smaller y, (b) the chain points where its rise is, (c) the gap points (no chain). For v1 and 07b's (i)-(iv), as 07b
grouped them: (a) chain points with ell >= 1 (the one-loop branch), (b) chain points with ell < 1 (v1's plateau, (iii)
and (iv)'s gap form), (c) no chain; v1 by the old chain (its own), (i)-(iv) by the new. Each selection's dropped points
(the universe U_n: back out, ln R > 0.3, at n; less the selection) are scored separately. The row "v1 by the new chain"
is the sharpness check (prompt §3): v1's constants read in the wrong chain.

**Leave one set out** (prompt §1.4): on S3 at each n, (v) refitted without each group of minform.group_of in turn (four
starts, the least SSE), each parameter's spread across the refits, and the held-out group's residuals under the refit
that left it out. A group whose removal leaves fewer points than parameters is reported as not fittable. Beside it,
leave one L out of the grids (each `grid` or `n2-grid` column of L).

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_fits.py base \\
        > ../../../../.documents/power-law-wall/kex-07b2/fits.txt
    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_fits.py new \\
        > ../../../../.documents/power-law-wall/kex-07b2/refit.txt
`base` uses 07b's points; `new` adds this prompt's (stage2_points.txt). Each writes its constants, at full precision,
with the points and the fitted values, into .documents/power-law-wall/kex-07b2/minform.json under "without_new" or
"with_new" (the other key is kept).
"""

import json
import os
import sys
import warnings

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import minform as mf  # noqa: E402

warnings.simplefilter("ignore")
SINGLE = ("v", "v-L", "v-A", "vi")
MINFORM_JSON = os.path.join(mf.OUT, "minform.json")


def sel_names(n):
    return ("S1", "S2", "S3") if n == 1 else ("S1", "S3")


def pts_of(ps, n, S):
    """S2 at n = 2 equals S1 (prompt §1.3)."""
    return mf.select(ps, n, "S1" if (n == 2 and S == "S2") else S)


def fmt_p(names, p, se=None):
    out = []
    for i, k in enumerate(names):
        s = f"{k} {p[i]:.4f}"
        if se is not None:
            s += f" ± {se[i]:.4f}"
        out.append(s)
    return ", ".join(out)


def print_fit(label, variant, r, n):
    names = mf.NAMES[variant]
    print(f"  {label}")
    for s, p, sse in r["runs"]:
        if p is None:
            print(f"      start {tuple(round(x, 4) for x in s)}: failed ({sse})")
        else:
            print(
                f"      start {tuple(round(x, 4) for x in s)}: SSE {sse:.6f}; {fmt_p(names, p)}"
            )
    p, cov, sse, s = r["best"]
    se = mf.std_errors(cov)
    # (vi)'s s is compared apart: where the soft minimum tends to the sharp one, s runs off and is not determined
    k = len(names) - 1 if variant == "vi" else len(names)
    same = [
        x
        for x in r["runs"]
        if x[1] is not None
        and np.array_equal(np.round(x[1][:k], 4), np.round(p[:k], 4))
        and round(x[2], 6) == round(sse, 6)
    ]
    other = [x for x in r["runs"] if x[1] is not None and not any(x is y for y in same)]
    print(
        f"    best SSE {sse:.6f}: {fmt_p(names, p, se)}; {len(same)} of {len(r['runs'])} starts reach it to the printed "
        f"digits{' (s apart)' if variant == 'vi' else ''}"
        + (
            ""
            if not other
            else "; others: "
            + "; ".join(
                f"SSE {x[2]:.6f} (start {tuple(round(v, 3) for v in x[0])})"
                for x in other
            )
        )
    )
    if variant == "vi":
        ss = [x[1][-1] for x in r["runs"] if x[1] is not None]
        print(
            f"    (vi)'s s at the optima reached: {min(ss):.4g} .. {max(ss):.4g} "
            f"({len(ss)} of {len(r['runs'])} starts converged)"
        )
    return mf.params_of(variant, p, n), se


def laws_for(n, fitted):
    """[(name, law, grouper)] for the like-for-like tables: v1 (old chain), v1 by the new chain (sharpness), 07b's
    (i)-(iv) at their committed constants, and this selection's fitted variants."""
    out = [
        ("v1 (old chain)", mf.v1_law("old"), mf.regime_group("old")),
        ("v1 by the NEW chain [sharpness]", mf.v1_law("new"), mf.regime_group("new")),
    ]
    sets = (
        (("fit_law", "n1"), ("note", "n1_planner_selection"))
        if n == 1
        else (("U15", "n2"),)
    )
    for tag, key in sets:
        for c in ("i", "ii", "iii", "iv"):
            out.append(
                (
                    f"07b ({c}) [{tag}]",
                    mf.law_07b(c, mf.F07B[key][c]),
                    mf.regime_group("new"),
                )
            )
    for v, prm in fitted.items():
        out.append((f"({v}) fitted here", mf.variant_law(prm), mf.branch_group(prm)))
    return out


def table(title, pts, laws):
    print(f"    {title} ({len(pts)} points)")
    print(
        f"      {'law':36s} {'(a) fall / ell>=1':>26s} {'(b) rise / ell<1':>26s} {'(c) gap':>26s} {'(d) all':>26s}"
    )
    rows = {}
    for name, law, grp in laws:
        s = mf.score(pts, law, grp)
        rows[name] = s
        print(
            f"      {name:36s} "
            + " ".join(f"{mf.fmt_stats(s[g]):>26s}" for g in "abcd")
        )
    return rows


def loso(ps, n):
    pts, _ = mf.select(ps, n, "S3")
    names = mf.NAMES["v"]
    groups = sorted(set(p["group"] for p in pts))
    print(
        f"\n## leave one set out: (v) on S3 at n = {n} ({len(pts)} points; groups {groups})"
    )
    full = mf.fit("v", pts, n)["best"][0]
    print(f"    all points: {fmt_p(names, full)}")
    refits = []
    for g in groups:
        keep = [p for p in pts if p["group"] != g]
        out = [p for p in pts if p["group"] == g]
        if len(keep) <= len(names):
            print(
                f"    without {g:12s} ({len(out)} points): {len(keep)} left, not fittable with {len(names)} parameters"
            )
            continue
        r = mf.fit("v", keep, n)
        if r["best"] is None:
            print(f"    without {g:12s}: every start failed")
            continue
        p = r["best"][0]
        prm = mf.params_of("v", p, n)
        res = [
            mf.fraction(q["ell_new"], q["L"], q["lnR"], n, prm) - q["frac"] for q in out
        ]
        refits.append(p)
        print(
            f"    without {g:12s} ({len(out):2d} points): {fmt_p(names, p)}; held out {mf.fmt_stats(mf.stats(res))}"
        )
    if refits:
        a = np.array(refits)
        print(
            "    spread across the refits (min .. max; std): "
            + ", ".join(
                f"{k} {a[:, i].min():.4f}..{a[:, i].max():.4f} ({a[:, i].std():.4f})"
                for i, k in enumerate(names)
            )
        )
    # leave one L out of the grid
    gname = "grid" if n == 1 else "n2-grid"
    Ls = sorted(set(p["L"] for p in pts if p["group"] == gname))
    print(f"  beside it, leave one L out of `{gname}` (L = {Ls}):")
    refits = []
    for L in Ls:
        keep = [p for p in pts if not (p["group"] == gname and p["L"] == L)]
        out = [p for p in pts if p["group"] == gname and p["L"] == L]
        r = mf.fit("v", keep, n)
        p = r["best"][0]
        prm = mf.params_of("v", p, n)
        res = [
            mf.fraction(q["ell_new"], q["L"], q["lnR"], n, prm) - q["frac"] for q in out
        ]
        refits.append(p)
        print(
            f"    without L = {L:<6g} ({len(out)} points): {fmt_p(names, p)}; held out {mf.fmt_stats(mf.stats(res))}"
        )
    a = np.array(refits)
    print(
        "    spread: "
        + ", ".join(
            f"{k} {a[:, i].min():.4f}..{a[:, i].max():.4f} ({a[:, i].std():.4f})"
            for i, k in enumerate(names)
        )
    )


def list_points(pts, n, fitted, title):
    print(f"\n## {title}")
    print(
        "    set, group, L, ln R, ln(V0/M^4), ell_old, ell_new, f, then v1 (old chain) and each fitted variant's f "
        f"({', '.join(fitted)}), and (v)'s active branch"
    )
    for p in pts:
        e0 = "none" if p["ell_old"] is None else f"{p['ell_old']:.3f}"
        e1 = "none" if p["ell_new"] is None else f"{p['ell_new']:.3f}"
        vals = [mf.v1_law("old")(p)] + [mf.variant_law(v)(p) for v in fitted.values()]
        br = mf.active_branch(p["ell_new"], p["L"], p["lnR"], n, fitted["v"])
        print(
            f"    {p['set']:24s} {p['group']:12s} {p['L']:9.4f} {p['lnR']:8.3f} {p['lv']:8.2f} {e0:>7s} {e1:>7s} "
            f"f {p['frac']:.4f} | " + " ".join(f"{v:.3f}" for v in vals) + f" | {br}"
        )


def record(store, key, variant, r, n, pts, se):
    """The fit's constants, errors and SSE, its points and the values the fit's own model (minform.model_fn, the
    function curve_fit minimised) gives at the optimum."""
    p, _, sse, _ = r["best"]
    prm = mf.params_of(variant, p, n)
    vals = mf.model_fn(variant, pts, n)(np.arange(len(pts), dtype=float), *p)
    store[key] = dict(
        params=prm,
        std_errors=[float(x) for x in se],
        sse=sse,
        points=[
            dict(
                set=p["set"],
                n=p["n"],
                ell=p["ell_new"],
                L=p["L"],
                ln_R=p["lnR"],
                f_measured=p["frac"],
                f_fit=float(v),
            )
            for p, v in zip(pts, vals)
        ],
    )


def main():
    mode = sys.argv[1]
    with_new = mode == "new"
    print(f"# minform_fits.py {mode}; start: {os.popen('uptime').read().strip()}")
    ps = mf.all_points(with_new=with_new)
    nn = sum(p["file"] == "new (07b2)" for p in ps)
    print(
        f"# {len(ps)} points: {len(ps) - nn} of 07b's fits.points() (crossover's and 07b's), {nn} new (07b2)"
    )
    store = {}
    fitted_by = {}
    for n in (1, 2):
        for S in sel_names(n):
            pts, drop = pts_of(ps, n, S)
            print(
                f"\n# ===== n = {n}, {S}{' (= S1 at n = 2)' if n == 2 and S == 'S2' else ''}: {len(pts)} points "
                f"({sum(p['ell_new'] is not None for p in pts)} chain, {sum(p['ell_new'] is None for p in pts)} gap); "
                f"{len(drop)} of U_{n} dropped"
            )
            if S == "S3":
                print(
                    "  the points S3 drops (U_n less S3): set, L, ln R, ln(V0/M^4), ell_new, f, regime (new)"
                )
                for p in drop:
                    e1 = "none" if p["ell_new"] is None else f"{p['ell_new']:.3f}"
                    print(
                        f"    {p['set']:14s} L {p['L']:8.3f} ln R {p['lnR']:7.3f} ln(V0/M^4) {p['lv']:7.2f} ell_new "
                        f"{e1:>6s} f {p['frac']:.4f} {p['reg_new']}"
                    )
            fitted = {}
            for v in SINGLE:
                r = mf.fit(v, pts, n)
                prm, se = print_fit(f"({v}) on {S}, n = {n}:", v, r, n)
                fitted[v] = prm
                record(store, f"n{n}/{S}/{v}", v, r, n, pts, se)
            fitted_by[(n, S)] = fitted
    # v-J: joint over n = 1 and n = 2 on each selection
    for S in ("S1", "S2", "S3"):
        p1, _ = pts_of(ps, 1, S)
        p2, _ = pts_of(ps, 2, S)
        r = mf.fit("v-J", p1 + p2)
        prm, se = print_fit(
            f"(v-J) on {S} at n = 1 and n = 2 together ({len(p1)} + {len(p2)} points):",
            "v-J",
            r,
            None,
        )
        record(store, f"n12/{S}/v-J", "v-J", r, None, p1 + p2, se)
        fitted_by[(1, S)]["v-J"] = prm
        if S != "S2":
            fitted_by[(2, S)]["v-J"] = prm
        else:
            fitted_by[(2, "S2")] = dict(fitted_by[(2, "S1")])
            fitted_by[(2, "S2")]["v-J"] = prm
    # the like-for-like tables
    print(
        "\n\n# ===== like for like (prompt §1.3): fit - f, max |res| / rms / mean (count). (a) (b) (c) per law: a variant "
        "of this prompt by its own active branch (fall, rise, gap); v1 and 07b's (i)-(iv) by regime as 07b grouped them "
        "(ell >= 1, ell < 1, no chain), v1 in the old chain, (i)-(iv) in the new"
    )
    for n in (1, 2):
        for S in sel_names(n):
            pts, drop = pts_of(ps, n, S)
            print(f"\n  n = {n}, {S}")
            laws = laws_for(n, fitted_by[(n, S)])
            table(f"{S}'s points", pts, laws)
            if drop:
                table(f"the points {S} drops", drop, laws)
            if S != "S3":
                f3 = {
                    f"{v}@S3": prm
                    for v, prm in fitted_by[(n, "S3")].items()
                    if v in ("v",)
                }
                table(
                    f"{S}'s points, (v) fitted on S3",
                    pts,
                    [
                        (f"({k}) fitted on S3", mf.variant_law(p), mf.branch_group(p))
                        for k, p in f3.items()
                    ],
                )
    for n in (1, 2):
        loso(ps, n)
    for n in (1, 2):
        pts, _ = pts_of(ps, n, "S3")
        list_points(
            pts, n, fitted_by[(n, "S3")], f"every S3 point at n = {n} with each law"
        )
    data = json.load(open(MINFORM_JSON)) if os.path.exists(MINFORM_JSON) else {}
    data["without_new" if not with_new else "with_new"] = store
    data["about"] = (
        "power-law-wall 07b2: the min(rise, fall) form's constants for every variant on every selection, without and "
        "with the new points (minform_fits.py base / new), at full precision; each fit with its points and the fitted "
        "values minform.fraction gives there. params is the argument of minform.fraction(ell, L, ln_R, n, params)."
    )
    with open(MINFORM_JSON, "w") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
    print(f"\n# minform_fits.py {mode} end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
