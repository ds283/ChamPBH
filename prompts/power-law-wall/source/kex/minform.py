"""
power-law-wall prompt 07b2 (P91-P98): the min(rise, fall) form of the exponential's one-loop law, its variants, the
points and selections it is measured on, and the reference implementation (§1.8). A library: minform_run.py runs it.
Research, not production code; nothing here is a ruling (U13 is the user's).

**The form** (README §6.13; prompt §0), for ln R > 0 (the classical region, ln R <= 0, stays R S(L) and is not fitted):
    y_rise = G (ln R - R0)_+^P (1 + Q ln L)_+            (v1's gap form, in ln R and L)
    y_fall = A / (ell - ell0)  where the chain has a solution and ell > ell0, +inf otherwise
    f      = 1 - exp(-min(y_rise, y_fall))
ell is the new chain's (k_ex = E^{1/2}|V'''/V''|/2; kexchain.chain(w, R, "v3")), None where it has no solution.

**The variants** (prompt §1.2, P92), each fitted at n = 1 and at n = 2 separately unless stated:
    "v"    G, R0, P, Q, A, ell0 fitted;
    "v-L"  the fall A/(ell - ell0 - C ln L), C fitted with the rest (+inf where that denominator is not positive);
    "v-A"  as "v" with A held at 2.69 (the Born coefficient);
    "v-J"  A and ell0 shared by n = 1 and n = 2, each n with its own G, R0, P, Q: one joint fit;
    "vi"   proposed here (prompt §1.2 (vi)): the soft minimum y = m (1 + (m/M)^s)^(-1/s), m = min(y_rise, y_fall),
           M = max, s fitted, which tends to "v" as s -> inf. Its reason: min() has a corner where the branches cross,
           and the measured f near the crossing (07b's laboratory points at ell_new 1.2-1.5 and the grid's rows beside
           them) lies below both branches' continuation there; a soft minimum tests whether the peak is rounded, with
           one parameter more and "v" nested in it.

**The reference implementation**, fraction(ell, L, ln_R, n, params), is the one function every fit here evaluates: the
fits' model is fraction mapped over the points, so the values a fit reports are fraction's at the fitted constants, and
minform_run.py `ref` shows that fraction with the constants read back from minform.json reproduces them bit for bit.
params = {"variant": name, "n1": {constant: value}, "n2": {...}}; only the n fitted need be present.

**The points** are 07b's (fits.points(): crossover's stage-2 rows and 07b's new points, each re-chained in both forms),
and, from §1.5 on, this prompt's new points (read from .documents/power-law-wall/kex-07b2/stage2_points.txt).

**The selections** (prompt §1.3, P93), each over the points with the field back out and ln R > 0.3 (the universe U_n):
    S1  07b's fit_law selection: the chain points (any ell) with n2_ok, and the gap points with gap_ok (fits.sel_gap);
    S2  the planner's (note 07b §0.3): S1 and fits.planner_ok on every point (no `curve` set, no `grid` row above
        ln R = 15) at n = 1; at n = 2 it equals S1;
    S3  the domain of histories: ln(V0/M^4) <= 10 (fits.ln_v0m4), every set and regime, at n = 1 and at n = 2.
"""

import json
import os
import sys
from math import exp, inf, log

import numpy as np
from scipy.optimize import curve_fit

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fits  # noqa: E402  (07b's; imported, never changed)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402

ROOT = rechain.ROOT
OUT = os.path.join(ROOT, ".documents/power-law-wall/kex-07b2")
A_BORN = 2.69
LV_MAX = 10.0  # S3: crossover's U15 cut, ln(V0/M^4) <= 10
LNR_MIN = 0.3

# ---- the reference implementation (§1.8) -----------------------------------------------------------------------------


def y_rise(L, ln_R, q):
    """G (ln R - R0)_+^P (1 + Q ln L)_+."""
    d = ln_R - q["R0"]
    if not d > 0.0:
        return 0.0
    s = 1.0 + q["Q"] * log(L)
    if not s > 0.0:
        return 0.0
    return q["G"] * d ** q["P"] * s


def y_fall(ell, L, q, variant):
    """A/(ell - ell0) ("v-L": A/(ell - ell0 - C ln L)) where the chain exists and the denominator is positive; else
    +inf."""
    if ell is None:
        return inf
    den = ell - q["ell0"]
    if variant == "v-L":
        den = den - q["C"] * log(L)
    if not den > 0.0:
        return inf
    return q["A"] / den


def y_of(ell, L, ln_R, n, params):
    """(y, y_rise, y_fall) under params."""
    variant = params["variant"]
    q = params[f"n{n}"]
    r = y_rise(L, ln_R, q)
    fl = y_fall(ell, L, q, variant)
    m = r if r <= fl else fl
    if variant == "vi" and fl != inf and r > 0.0:
        big = fl if r <= fl else r
        m = m * (1.0 + (m / big) ** q["s"]) ** (-1.0 / q["s"])
    return m, r, fl


def fraction(ell, L, ln_R, n, params):
    """f = 1 - exp(-y) for ln R > 0 under the variant and constants of params (module docstring). ell is the new
    chain's, None where it has no solution."""
    if not ln_R > 0.0:
        raise ValueError(
            "fraction: ln R <= 0 is the classical region (R S(L)), not this law's"
        )
    return 1.0 - exp(-y_of(ell, L, ln_R, n, params)[0])


def active_branch(ell, L, ln_R, n, params):
    """'gap' (no chain), 'fall' (the fall is the smaller y) or 'rise'."""
    if ell is None:
        return "gap"
    _, r, fl = y_of(ell, L, ln_R, n, params)
    return "fall" if fl < r else "rise"


# ---- the variants ----------------------------------------------------------------------------------------------------

NAMES = {
    "v": ("G", "R0", "P", "Q", "A", "ell0"),
    "v-L": ("G", "R0", "P", "Q", "A", "ell0", "C"),
    "v-A": ("G", "R0", "P", "Q", "ell0"),
    "vi": ("G", "R0", "P", "Q", "A", "ell0", "s"),
    "v-J": ("A", "ell0", "G1", "R01", "P1", "Q1", "G2", "R02", "P2", "Q2"),
}
VARIANTS = ("v", "v-L", "v-A", "v-J", "vi")

# starting points: the orchestrator's scratch start (prompt §1.1), one from v1's gap constants and the Born A, and one
# well away from both; the extra parameters' starts beside
_BASE_STARTS = (
    (0.02, 1.6, 1.5, 1.0, 2.6, 1.1),
    (0.0198, 1.5601, 1.5472, 0.841, 2.69, 1.0),
    (0.05, 1.0, 1.2, 0.5, 2.0, 1.5),
    (0.01, 2.0, 1.8, 1.5, 3.2, 0.8),
)


def starts(variant):
    out = []
    for i, b in enumerate(_BASE_STARTS):
        G, R0, P, Q, A, l0 = b
        if variant == "v":
            out.append(b)
        elif variant == "v-L":
            out.append(b + ((0.0, 0.2, -0.2, 0.4)[i],))
        elif variant == "v-A":
            out.append((G, R0, P, Q, l0))
        elif variant == "vi":
            out.append(b + ((8.0, 4.0, 16.0, 2.0)[i],))
        elif variant == "v-J":
            out.append((A, l0, G, R0, P, Q, G, R0, P, Q))
    return out


def params_of(variant, p, n=None):
    """The params dict of a fitted vector p (n: the n of a single-n fit)."""
    p = [float(x) for x in p]
    if variant == "v-J":
        A, l0 = p[0], p[1]
        return {
            "variant": "v-J",
            "n1": dict(G=p[2], R0=p[3], P=p[4], Q=p[5], A=A, ell0=l0),
            "n2": dict(G=p[6], R0=p[7], P=p[8], Q=p[9], A=A, ell0=l0),
        }
    q = dict(zip(NAMES[variant], p))
    if variant == "v-A":
        q["A"] = A_BORN
    return {"variant": variant, f"n{n}": q}


# ---- the points ------------------------------------------------------------------------------------------------------


def group_of(p):
    """The leave-one-set-out group (prompt §1.4): 07b's set labels grouped by curve. `2197 A0.05`/`A0.01` -> 2197;
    `R-865 A0.05` -> R-865; crossover's curve-119 and curve-129 (ln(V0/M^4) -119.16 and -129.27) are the curves of
    L-M8e-18 and L-M1e-16 and join them; 07b's new points by their curve (lab, L-M8e-18); this prompt's new points by
    theirs (07b2 lab, 07b2 L-M*, 07b2 held-out)."""
    s = p["set"]
    if s.startswith("2197"):
        return "2197"
    if s.startswith("R-865"):
        return "R-865"
    if s == "curve-119":
        return "L-M8e-18"
    if s == "curve-129":
        return "L-M1e-16"
    return s


def new_points_07b2(path=None):
    """This prompt's new points from stage2_points.txt (minform_run.py's stage-2 lines; failed and stopped points
    left out), each re-chained here as fits.points() chains 07b's."""
    path = path or os.path.join(OUT, "stage2_points.txt")
    out = []
    if not os.path.exists(path):
        return out
    for line in open(path):
        if not line.startswith("PT "):
            continue
        f = dict(kv.split("=", 1) for kv in line.split()[1:] if "=" in kv)
        out.append(
            dict(
                file="new (07b2)",
                set=f["set"],
                idx=int(f["id"]),
                n=int(f["n"]),
                L=float(f["L"]),
                lnR=float(f["lnR"]),
                frac=float(f["frac"]),
                back=f["back"] == "True",
                kind=f["kind"],
            )
        )
    for p in out:
        chain_point(p)
    return out


def chain_point(p):
    w = rechain.wall_of(p)
    R = exp(p["lnR"])
    for form, key in (("lambda", "old"), ("v3", "new")):
        ch = kc.chain(w, R, form)
        p[f"ell_{key}"] = None if ch is None else ch[3]
        p[f"reg_{key}"] = kc.regime(p["lnR"], ch)
    p["lv"] = fits.ln_v0m4(p["L"], p["lnR"], p["n"])
    return p


def all_points(with_new=False):
    ps = fits.points()
    for p in ps:
        p.setdefault("kind", "07b" if p["file"] == "new (07b)" else "crossover")
    if with_new:
        ps = ps + new_points_07b2()
    for p in ps:
        p["group"] = group_of(p)
    return ps


# ---- the selections --------------------------------------------------------------------------------------------------


def in_universe(p, n):
    return p["n"] == n and p["back"] and p["lnR"] > LNR_MIN


def in_S1(p):
    if p["reg_new"] == "one_loop":
        return fits.n2_ok(p)
    if p["reg_new"] == "gap":
        return fits.gap_ok(p)
    return False


def in_S2(p):
    return in_S1(p) and (p["n"] == 2 or fits.planner_ok(p))


def in_S3(p):
    return p["reg_new"] in ("one_loop", "gap") and p["lv"] <= LV_MAX


SELECTIONS = {"S1": in_S1, "S2": in_S2, "S3": in_S3}


def select(ps, n, name):
    sel = SELECTIONS[name]
    keep = [p for p in ps if in_universe(p, n) and sel(p)]
    drop = [p for p in ps if in_universe(p, n) and not sel(p)]
    return keep, drop


# ---- fitting ---------------------------------------------------------------------------------------------------------


def model_fn(variant, pts, n=None):
    """The fit's model: fraction mapped over pts (indexed by xdata), at the vector p."""

    def fn(idx, *p):
        prm = params_of(variant, p, n)
        return np.array(
            [
                fraction(
                    pts[int(i)]["ell_new"],
                    pts[int(i)]["L"],
                    pts[int(i)]["lnR"],
                    pts[int(i)]["n"],
                    prm,
                )
                for i in idx
            ]
        )

    return fn


def fit_once(variant, pts, n, p0, maxfev=100000):
    fn = model_fn(variant, pts, n)
    x = np.arange(len(pts), dtype=float)
    F = np.array([p["frac"] for p in pts])
    p, cov = curve_fit(fn, x, F, p0=p0, maxfev=maxfev)
    res = fn(x, *p) - F
    return p, cov, float(np.sum(res**2))


def fit(variant, pts, n=None, start_list=None, maxfev=100000):
    """Every start of the variant; returns dict(best=(p, cov, sse, start), runs=[(start, p or None, sse or msg)])."""
    runs = []
    best = None
    for s in start_list or starts(variant):
        try:
            p, cov, sse = fit_once(variant, pts, n, s, maxfev)
        except (RuntimeError, ValueError, OverflowError) as e:
            runs.append((s, None, f"{type(e).__name__}: {str(e)[:80]}"))
            continue
        runs.append((s, p, sse))
        if best is None or sse < best[2]:
            best = (p, cov, sse, s)
    return dict(best=best, runs=runs)


def std_errors(cov):
    with np.errstate(invalid="ignore"):
        return np.sqrt(np.diag(cov))


# ---- the other laws, for the like-for-like tables ------------------------------------------------------------------

F07B = json.load(
    open(os.path.join(ROOT, ".documents/power-law-wall/kex-07b/fits.json"))
)


def law_07b(name, prm):
    """07b's candidate `name` at its committed constants prm, term for term as fits.py's law_i / law_iii / law_iv
    evaluated them (so that its residuals there are reproduced): a function of a point.
    """
    G, R0, P, Q = prm["G"], prm["R0"], prm["P"], prm["Q"]

    def gap(p):
        return float(fits.f_gap((p["lnR"], p["L"]), G, R0, P, Q))

    if name in ("i", "ii"):

        def f(p):
            if p["reg_new"] == "one_loop":
                if p["ell_new"] < 1.0:
                    return prm["plateau"]
                # a non-positive denominator gives y = inf, f = 1, as 07b's paths.law takes it; fits.py's own
                # evaluation is unguarded there, which no point of 07b's reached (07b2's L-M1e-16 points do)
                if not p["ell_new"] + prm["B0"] - prm["C"] * log(p["L"]) > 0.0:
                    return 1.0
                return float(
                    fits.f_one((p["ell_new"], p["L"]), prm["A"], prm["B0"], prm["C"])
                )
            return gap(p)

        return f
    if name == "iii":

        def f(p):
            if p["reg_new"] == "one_loop" and p["ell_new"] >= prm["ell_g"]:
                return float(
                    fits.f_one((p["ell_new"], p["L"]), A_BORN, prm["B0"], prm["C"])
                )
            return gap(p)

        return f

    def f(p):
        z = (
            np.array([-1.0 if p["ell_new"] is None else p["ell_new"]]),
            np.array([p["L"]]),
            np.array([p["lnR"]]),
        )
        return float(fits.f_iv(z, G, R0, P, Q, prm["B0"], prm["C"])[0])

    return f


def v1_law(key="old"):
    def f(p):
        return fits.v1_fraction(p[f"reg_{key}"], p[f"ell_{key}"], p["L"], p["lnR"])

    return f


def regime_group(key):
    """07b's grouping: chain points ell >= 1 ('a', its one-loop branch), ell < 1 ('b', its plateau), gap ('c')."""

    def g(p):
        if p[f"reg_{key}"] == "one_loop":
            return "a" if p[f"ell_{key}"] >= 1.0 else "b"
        return "c"

    return g


def branch_group(params):
    def g(p):
        b = active_branch(p["ell_new"], p["L"], p["lnR"], p["n"], params)
        return {"fall": "a", "rise": "b", "gap": "c"}[b]

    return g


def variant_law(params):
    def f(p):
        return fraction(p["ell_new"], p["L"], p["lnR"], p["n"], params)

    return f


def stats(res):
    res = np.asarray(res, dtype=float)
    if res.size == 0:
        return None
    return (
        float(np.abs(res).max()),
        float(np.sqrt(np.mean(res**2))),
        float(res.mean()),
        int(res.size),
    )


def fmt_stats(s):
    if s is None:
        return "-"
    return f"{s[0]:.3f} / {s[1]:.3f} / {s[2]:+.3f} ({s[3]})"


def score(pts, law, grouper):
    """{group a/b/c/d: stats} of law(p) - f."""
    res = {"a": [], "b": [], "c": [], "d": []}
    for p in pts:
        r = law(p) - p["frac"]
        res[grouper(p)].append(r)
        res["d"].append(r)
    return {k: stats(v) for k, v in res.items()}
