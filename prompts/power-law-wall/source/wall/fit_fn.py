"""
power-law-wall prompt 03 §1.1 (R7, P18): the drain fraction on the power wall as a function f_n(ln R) for each n, fitted
to stage 2's points (.documents/power-law-wall/wall-02/stage2_scan.txt, read, not re-run), and its reference
implementation fraction_power, which shares production's decimal literals (orchestrator note 03, ruling 3).

**The points.** Every `curve-*` row of n with the field back out and the production's regime not classical (fit_law.rows,
which skips FAILED rows). The rows where the field did not come back out are kept aside and reported against the fit, as
02 reported them. The off-curve `grid-*` rows are not fitted; the fit is reported beside them at the rows that came back
out with |dE/E| <= 1e-8.

**The form**, per n: y = -ln(1 - f), with
  - the rising branch y = G (ln R - R0)^P, fitted on the points with 0.3 < ln R < 20;
  - the falling branch y = A/(ln R - c), fitted on the points with ln R > 28;
  - the switch at ln R_x, where the two y are equal above c, so that f is continuous;
  - f = 1 - e^{-y}.
Unweighted least squares in f (scipy.optimize.curve_fit, its default trust-region method, p0 (0.01, 0, 2) and (30, 10)).
These are the windows, weights and starting points of the orchestrator's scratch fit (note 03 §0, laws_plot.py), which
the user was shown before ruling R7; they are kept so that the fit is the one ruled on (ruling 2). The window between 20
and 28 is the peak, where neither branch's form holds and the switch sits; the points there are in the residuals. The
standard errors are curve_fit's (the square roots of the covariance's diagonal, scaled by the residual variance).

**The literals.** The fitted constants rounded to six significant figures, typed below as PRODUCTION's decimal literals and
in CosmologyConcepts/Models/InversePowerModel.py the same; the switch is the root of y_rise = y_fall at those rounded
constants (brentq, xtol 1e-14), typed as its repr. main() checks that the literals are the fit's rounding and that the
switch is the root at them, and reports f's step across it.

**fraction_power(n, ln R)**: the classical branch exp(ln R) S_n at ln R <= 0 (R6: S_1 = 1.02620e-2, S_2 = 5.21615e-3,
stage1a_S_n.txt), and 1 - exp(-y) above, with the rising y below the switch and the falling y at and above it. It is the
reference the gate Law compares production with (InversePowerModel._drain_fraction), term for term.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python fit_fn.py \\
        > ../../../../.documents/power-law-wall/law-03/fit_fn.txt

`gate` (below) runs the gate Law's first two items against production, from this directory with PYTHONPATH set to the
repository root:

    cd prompts/power-law-wall/source/wall && PYTHONPATH=../../../.. ../../../../venv/bin/python fit_fn.py gate \\
        > ../../../../.documents/power-law-wall/law-03/gate_law.txt
"""

import os
import warnings
from math import exp, log

import numpy as np
from scipy.optimize import brentq, curve_fit

warnings.simplefilter("ignore")

# ---- the reference implementation, with production's literals (ruling 3) -------------------------------------------
#
# InversePowerModel (CosmologyConcepts/Models/InversePowerModel.py) types the same literals: S_n (R6), and per n the
# rising branch's G, R0, P, the falling branch's A, c and the switch

FN_S = {1: 1.02620e-2, 2: 5.21615e-3}
FN_G = {1: 2.86769e-3, 2: 2.85913e-3}
FN_R0 = {1: -1.27414, 2: -1.10903}
FN_P = {1: 2.22126, 2: 2.45613}
FN_A = {1: 27.9671, 2: 19.6100}
FN_C = {1: 14.9835, 2: 10.9039}
FN_SWITCH = {1: 23.08943252571177, 2: 16.710187997698853}


def fraction_power(n: int, log_R: float) -> float:
    """f_n(ln R) on the power wall (R6, R7): exp(ln R) S_n at ln R <= 0; else 1 - exp(-y), y the rising branch
    G (ln R - R0)^P below the switch and the falling branch A/(ln R - c) at and above it.
    """
    if log_R <= 0.0:
        return exp(log_R) * FN_S[n]
    if log_R < FN_SWITCH[n]:
        y = FN_G[n] * (log_R - FN_R0[n]) ** FN_P[n]
    else:
        y = FN_A[n] / (log_R - FN_C[n])
    return 1.0 - exp(-y)


# ---- the fit ---------------------------------------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
SCAN = os.path.join(
    HERE, "../../../../.documents/power-law-wall/wall-02/stage2_scan.txt"
)
RISE_WINDOW = (0.3, 20.0)
FALL_FROM = 28.0
CURVES = ("4e-31", "1e-33", "1e-15")
# the exponential law's accuracy (crossover out/fit_law.txt), reported beside
EXP_ACCURACY = "one-loop max 0.074 (rms 0.021), gap max 0.062 (rms 0.024)"
# note 03 §0's fit, reported beside (worst all, rms, worst mapped)
NOTE_FIT = {
    1: dict(
        G=0.002868,
        R0=-1.2741,
        P=2.2213,
        A=27.9671,
        c=14.9835,
        switch=23.089,
        worst=0.054,
        rms=0.019,
        mapped=0.004,
        peak=0.968,
    ),
    2: dict(
        G=0.002859,
        R0=-1.1090,
        P=2.4561,
        A=19.6100,
        c=10.9039,
        switch=16.710,
        worst=0.056,
        rms=0.022,
        mapped=0.003,
        peak=0.966,
    ),
}


def rows():
    """The curve rows (as fit_law.rows reads them) and the grid rows, from stage2_scan.txt."""
    curve, grid = [], []
    for line in open(SCAN):
        f = line.split()
        if not f or f[0].startswith("#") or "FAILED" in line:
            continue
        if not (f[0].startswith("curve-") or f[0].startswith("grid-")):
            continue
        r = dict(
            set=f[0],
            n=int(f[1]),
            lnR=float(f[2]),
            lnx=float(f[3]),
            reg=f[5],
            ell=None if f[6] == "none" else float(f[6]),
            mark=f[8],
            frac=float(f[9]),
            back=f[12] == "True",
            dE=float(f[14]),
        )
        (curve if f[0].startswith("curve-") else grid).append(r)
    return curve, grid


def y_rise(z, G, R0, P):
    return G * np.clip(z - R0, 0.0, None) ** P


def y_fall(z, A, c):
    return A / (z - c)


def fit(points):
    rise = [r for r in points if RISE_WINDOW[0] < r["lnR"] < RISE_WINDOW[1]]
    fall = [r for r in points if r["lnR"] > FALL_FROM]
    X = np.array([r["lnR"] for r in rise])
    F = np.array([r["frac"] for r in rise])
    pg, cg = curve_fit(
        lambda z, G, R0, P: 1.0 - np.exp(-y_rise(z, G, R0, P)),
        X,
        F,
        p0=(0.01, 0.0, 2.0),
        maxfev=50000,
    )
    X2 = np.array([r["lnR"] for r in fall])
    F2 = np.array([r["frac"] for r in fall])
    po, co = curve_fit(
        lambda z, A, c: 1.0 - np.exp(-y_fall(z, A, c)),
        X2,
        F2,
        p0=(30.0, 10.0),
        maxfev=50000,
    )
    return pg, np.sqrt(np.diag(cg)), po, np.sqrt(np.diag(co)), len(rise), len(fall)


def switch_of(G, R0, P, A, c):
    """The root of y_rise = y_fall above c (the planner's bracket: from max(15, c + 1) to 40)."""
    g = lambda z: G * (z - R0) ** P - A / (z - c)  # noqa: E731
    return brentq(g, max(15.0, c + 1.0), 40.0, xtol=1e-14, rtol=4.0 * 2.0**-52)


def sig6(v: float) -> float:
    return float(f"{v:.6g}")


def stats(res):
    res = np.asarray(res, dtype=float)
    if res.size == 0:
        return "no points"
    return (
        f"max |res| {np.abs(res).max():.4f}  rms {np.sqrt(np.mean(res**2)):.4f}  mean {res.mean():+.4f}  "
        f"({res.size} points)"
    )


def regime_of(r):
    if r["reg"] == "one_loop":
        return "one-loop ell >= 1" if r["ell"] >= 1.0 else "one-loop ell < 1"
    return r["reg"]


def main():
    print(
        f"# fit_fn.py (power-law-wall prompt 03 §1.1); start: {os.popen('uptime').read().strip()}"
    )
    print(
        f"# from {os.path.relpath(SCAN, HERE)}; residual = fraction_power - stage 2 (at production's literals)"
    )
    print(
        f"# windows: rising branch on {RISE_WINDOW[0]} < ln R < {RISE_WINDOW[1]}, falling branch on ln R > {FALL_FROM}; "
        "unweighted least squares in f"
    )
    curve, grid = rows()
    for n in (1, 2):
        pts = [r for r in curve if r["n"] == n]
        back = [r for r in pts if r["back"]]
        nc = [r for r in back if r["reg"] != "classical"]
        pg, eg, po, eo, nr, nf = fit(nc)
        sw_fit = switch_of(*pg, *po)
        print(f"\n## n = {n}")
        print(
            f"points: {len(pts)} curve rows, {len(back)} with the field back out, {len(nc)} of them not classical; "
            f"rising branch fitted on {nr}, falling branch on {nf}"
        )
        print("the fit, with curve_fit's standard errors:")
        for name, v, e in zip(("G", "R0", "P"), pg, eg):
            print(f"  {name:2s} = {v:.10g} +- {e:.3g}")
        for name, v, e in zip(("A", "c"), po, eo):
            print(f"  {name:2s} = {v:.10g} +- {e:.3g}")
        print(f"  switch (at the unrounded constants) ln R_x = {sw_fit:.10g}")
        lit = dict(G=FN_G[n], R0=FN_R0[n], P=FN_P[n], A=FN_A[n], c=FN_C[n])
        rounded = dict(
            G=sig6(pg[0]), R0=sig6(pg[1]), P=sig6(pg[2]), A=sig6(po[0]), c=sig6(po[1])
        )
        sw_lit = switch_of(FN_G[n], FN_R0[n], FN_P[n], FN_A[n], FN_C[n])
        print(
            f"the literals (six significant figures): {lit}; the fit's rounding {'equals' if lit == rounded else 'DIFFERS FROM'} "
            f"them{'' if lit == rounded else ': ' + str(rounded)}"
        )
        print(
            f"  switch at the literals: brentq {sw_lit!r}, literal {FN_SWITCH[n]!r} "
            f"({'equal' if sw_lit == FN_SWITCH[n] else 'DIFFERENT'})"
        )
        s = FN_SWITCH[n]
        below = 1.0 - exp(-FN_G[n] * (s - FN_R0[n]) ** FN_P[n])
        above = fraction_power(n, s)
        print(
            f"  f at the switch: rising {below!r}, falling (as fraction_power) {above!r}, step {above - below:.3g}; "
            f"f_prev = fraction_power(nextafter(switch, 0)) {fraction_power(n, np.nextafter(s, 0.0))!r}"
        )
        nf_ = NOTE_FIT[n]
        print(
            f"  note 03 §0: G {nf_['G']}, R0 {nf_['R0']}, P {nf_['P']}, A {nf_['A']}, c {nf_['c']}, switch "
            f"{nf_['switch']}; worst all {nf_['worst']}, rms {nf_['rms']}, worst mapped {nf_['mapped']}, peak f "
            f"{nf_['peak']}"
        )
        zz = np.linspace(0.01, 80.0, 79991)
        fz = np.array([fraction_power(n, z) for z in zz])
        print(f"  peak f {fz.max():.4f} at ln R {zz[int(np.argmax(fz))]:.2f}")

        print("residuals at the literals:")
        res = lambda q: [fraction_power(n, r["lnR"]) - r["frac"] for r in q]  # noqa
        print(f"  all non-classical back-out points      {stats(res(nc))}")
        mp = [r for r in nc if r["mark"] == "mapped"]
        print(f"  the mapped histories' points           {stats(res(mp))}")
        for reg in ("one-loop ell >= 1", "one-loop ell < 1", "gap"):
            q = [r for r in nc if regime_of(r) == reg]
            print(f"  regime {reg:18s}              {stats(res(q))}")
            qm = [r for r in q if r["mark"] == "mapped"]
            if qm:
                print(f"  regime {reg:18s} mapped       {stats(res(qm))}")
        for c in CURVES:
            q = [r for r in nc if r["set"].startswith(f"curve-{c}-")]
            print(f"  curve {c:6s} all                     {stats(res(q))}")
            for reg in ("one-loop ell >= 1", "one-loop ell < 1", "gap"):
                qq = [r for r in q if regime_of(r) == reg]
                if qq:
                    print(f"  curve {c:6s} {reg:18s}      {stats(res(qq))}")
        print(
            "  the fit at its unrounded constants, for comparison: "
            + stats(
                [
                    1.0
                    - exp(
                        -(
                            float(y_rise(r["lnR"], *pg))
                            if r["lnR"] < sw_fit
                            else float(y_fall(r["lnR"], *po))
                        )
                    )
                    - r["frac"]
                    for r in nc
                ]
            )
        )
        nb = [r for r in pts if not r["back"]]
        print(
            f"not back out ({len(nb)}; law - measured): "
            + ", ".join(
                f"{r['set']} ln R {r['lnR']:.2f} ({regime_of(r)}): {fraction_power(n, r['lnR']) - r['frac']:+.4f}"
                for r in nb
            )
        )
        cl = [r for r in back if r["reg"] == "classical"]
        if cl:
            print(
                "classical points (f against R S_n; not fitted): "
                + ", ".join(
                    f"{r['set']} ln R {r['lnR']:.2f}: f {r['frac']:.4e}, R S_n {fraction_power(n, r['lnR']):.4e}, ratio "
                    f"{r['frac'] / fraction_power(n, r['lnR']):.3f}"
                    for r in cl
                )
            )
        gr = [r for r in grid if r["n"] == n and r["back"] and r["dE"] <= 1e-8]
        print(
            f"grid rows back out with |dE/E| <= 1e-8 ({len(gr)}), not fitted: {stats(res(gr))}"
        )
        for r in gr:
            print(
                f"  {r['set']:10s} ln R {r['lnR']:7.2f} ln x {r['lnx']:9.4f} {regime_of(r):18s} f {r['frac']:.6f}  "
                f"law {fraction_power(n, r['lnR']):.6f}  res {fraction_power(n, r['lnR']) - r['frac']:+.4f}"
            )
        f0 = fraction_power(n, 5e-324)
        print(
            f"the step at ln R = 0: f_n(0+) = {f0:.6g} (rising branch at the smallest positive ln R) against "
            f"R S_n = {fraction_power(n, 0.0):.6g} at ln R = 0; ratio {f0 / fraction_power(n, 0.0):.4f}"
        )
    print(
        f"\n# beside: the exponential law's accuracy (crossover out/fit_law.txt): {EXP_ACCURACY}"
    )
    print(f"# end: {os.popen('uptime').read().strip()}")


# ---- the gate Law (prompt §1.4): production against the reference --------------------------------------------------

MAP_DIR = os.path.join(HERE, "../../../../.documents/power-law-wall/wall-02/map")


def gate():
    """
    The gate Law's first two items (prompt §1.4), from the repository root's production modules (PYTHONPATH set to the
    repository root):
      - the law at synthetic inputs: InversePowerModel's fraction (RealizedModel.drain_fraction) against fraction_power
        at n = 1 and 2 over ln R in [-20, 600] (a uniform grid of 62001 points, 0 and its float neighbours, the switches
        and theirs), under Planck_units and GeV_units, for the regimes the one-loop law gives there (classical at
        ln R <= 0; one-loop and gap above); counted equal bit for bit, with the largest difference;
      - 02's maps candidate by candidate: at every candidate of the map/ip-*.txt files (twenty-two: the prompt's twenty
        and the two at beta = 3.05 that 02's amended map added), production's alpha_eff at
        A = 1 and w_gate = 1, through ScalarHistory.drain_alpha_effective with the model (and its hook), at the map's ln R,
        regime and ln E = ln(E/V0) + ln V0, against min(0.99, fraction_power(n, ln R)) (0 for an unassessed candidate).
    """
    import importlib
    from math import nextafter

    from CosmologyConcepts import M_value, beta_value
    from CosmologyConcepts.Models.InversePowerModel import InversePowerModel
    from Units import GeV_units, Planck_units

    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    print(
        f"# fit_fn.py gate (power-law-wall prompt 03 §1.4); start: {os.popen('uptime').read().strip()}"
    )
    grid = list(np.linspace(-20.0, 600.0, 62001))
    grid += [0.0, nextafter(0.0, 1.0), nextafter(0.0, -1.0), 5e-324]
    for sw in FN_SWITCH.values():
        grid += [sw, nextafter(sw, 0.0), nextafter(sw, 100.0)]
    print("\n## the law at synthetic inputs")
    for units in (Planck_units(), GeV_units()):
        for n in (1, 2):
            m = InversePowerModel(
                0, M_value(0, 4e-31 * units.PlanckMass), n, beta_value(0, 2.0), units
            ).realize(-276.63 + 4.0 * log(units.PlanckMass), 0.0)
            count, equal, worst = 0, 0, 0.0
            for lr in grid:
                regimes = ("classical",) if lr <= 0.0 else ("one_loop", "gap")
                for reg in regimes:
                    got = m.drain_fraction(
                        reg, 2.0 if reg == "one_loop" else None, 10.0, float(lr)
                    )
                    ref = fraction_power(n, float(lr))
                    count += 1
                    equal += got == ref
                    worst = max(worst, abs(got - ref))
            print(
                f"{type(units).__name__:12s} n = {n}: {count} evaluations, {equal} equal bit for bit, largest |difference| "
                f"{worst:.3g}"
            )
    print("\n## 02's maps, candidate by candidate (A = 1, w_gate = 1)")
    total, total_equal, nfiles = 0, 0, 0
    for name in sorted(f for f in os.listdir(MAP_DIR) if f.startswith("ip-")):
        n = int(name.split("-")[1][1:])
        lines = open(os.path.join(MAP_DIR, name)).read().splitlines()
        lnV0 = None
        for line in lines:
            if "tuned normalization ln V0 =" in line:
                lnV0 = float(line.split("ln V0 =")[1].split(",")[0])
        m = InversePowerModel(
            0,
            M_value(0, 4e-31 * Planck_units().PlanckMass),
            n,
            beta_value(0, 2.0),
            Planck_units(),
        ).realize(lnV0, 0.0)
        count, equal, worst, regs = 0, 0, 0.0, {}
        for line in lines:
            f = line.split()
            if not f or not f[0].isdigit():
                continue
            lnEV0, lnR, ell, reg = float(f[3]), float(f[4]), f[7], f[8]
            ell = None if reg != "one_loop" else float(ell)
            got = SM.drain_alpha_effective(
                reg, ell, None, lnR, 1.0, 1.0, model=m, log_E=lnEV0 + lnV0
            )
            ref = 0.0 if reg == "unassessed" else min(0.99, fraction_power(n, lnR))
            count += 1
            equal += got == ref
            worst = max(worst, abs(got - ref))
            regs[reg] = regs.get(reg, 0) + 1
        total += count
        nfiles += 1
        total_equal += equal
        print(
            f"{name:30s} n = {n}: {count:3d} candidates ({', '.join(f'{k} {v}' for k, v in sorted(regs.items()))}), "
            f"{equal} equal bit for bit, largest |difference| {worst:.3g}"
        )
    print(f"all {nfiles} files: {total} candidates, {total_equal} equal bit for bit")
    print(f"# end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "gate":
        gate()
    else:
        main()
