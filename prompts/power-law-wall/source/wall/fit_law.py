"""
power-law-wall prompt 02 §1.7 (U3): the adopted drain law against stage 2's fraction on the power wall's curves
(.documents/power-law-wall/wall-02/stage2_scan.txt), at every point where the field came back out. The regime and ell
of each point are the production's (the chain on PowerWall(x, n) with the floor; one_loop, gap or classical as
drain_regime classifies), and the law is drain_fraction's (powerscan.f_law), at each candidate L:
  (i)   L_E = ln(1 + x), what ExponentialModel._drain_L would give on this wall (U15's choice);
  (ii)  L = n, the wall's local index;
  (iii) L_M = M/phi_t = x^{1/n}, the n = 1 exponential's literal definition;
  (iv)  the best constant L for each n, fitted (least squares over a grid in ln L, then refined), separately for the
        one-loop (ell >= 1) and the gap points;
  (v)   the law's form refitted on this wall's points (U3 (b)'s option), for each n: the one-loop shift b in
        f = 1 - exp(-2.69/(ell + b)) (the Born coefficient held at 2.69, as crossover's fit held it); the gap's
        y = G (ln R - R0)^P, f = 1 - e^{-y} (its (1 + Q ln L) factor has no variable on a wall with no L; the
        fraction measured here depends on ln R alone, validate V6); the plateau (ell < 1) and the cap reported as
        measured, not fitted. A trial for the user, not a law.
For each: the largest |residual|, the rms and the mean (law - stage 2), over one-loop points with ell >= 1, over
one-loop points with ell < 1 (against the plateau, 0.9) and over gap points, per n; and the same restricted to the
points the mapped histories reached. Classical points against R S, with S = S_n of stage1a_S_n.txt and with the
exponential's table at L_E. The exponential's own accuracy (crossover's fit_law.txt): 0.074 one-loop, 0.062 gap.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python fit_law.py \\
        > ../../../../.documents/power-law-wall/wall-02/fit_law.txt
"""

import os
import warnings
from math import exp, log, log1p

import numpy as np
from scipy.optimize import curve_fit, minimize_scalar

import powerscan as ps

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
SCAN = os.path.join(
    HERE, "../../../../.documents/power-law-wall/wall-02/stage2_scan.txt"
)
S_N = {
    1: 1.0262017e-02,
    2: 5.2161510e-03,
}  # stage1a_S_n.txt, the 241-point ln-k trapezoid


def rows():
    out = []
    for line in open(SCAN):
        f = line.split()
        if not f or not f[0].startswith("curve-") or "FAILED" in line:
            continue
        out.append(
            dict(
                set=f[0],
                n=int(f[1]),
                lnR=float(f[2]),
                lnx=float(f[3]),
                LE=log1p(
                    exp(float(f[3]))
                ),  # from ln x, not the printed L_E (4 decimals)
                reg=f[5],
                ell=None if f[6] == "none" else float(f[6]),
                ell_inf=None if f[7] == "none" else float(f[7]),
                mark=f[8],
                frac=float(f[9]),
                back=f[12] == "True",
            )
        )
    return out


def L_of(option, r, Lconst=None):
    n = r["n"]
    if option == "i":
        return r["LE"]
    if option == "ii":
        return float(n)
    if option == "iii":
        return exp(r["lnx"] / n)
    return Lconst


def stats(res):
    res = np.asarray(res)
    if res.size == 0:
        return "no points"
    return (
        f"max |res| {np.abs(res).max():.3f}  rms {np.sqrt(np.mean(res**2)):.3f}  mean {res.mean():+.3f}  "
        f"({res.size} points)"
    )


def groups(pts):
    return {
        "one-loop ell >= 1": [
            r for r in pts if r["reg"] == "one_loop" and r["ell"] >= 1.0
        ],
        "one-loop ell < 1 (plateau)": [
            r for r in pts if r["reg"] == "one_loop" and r["ell"] < 1.0
        ],
        "gap": [r for r in pts if r["reg"] == "gap"],
    }


def best_L(pts):
    def sse(lnL):
        return sum(
            (ps.f_law(r["reg"], r["ell"], exp(lnL), r["lnR"]) - r["frac"]) ** 2
            for r in pts
        )

    grid = np.linspace(-6.0, 8.0, 281)
    j = int(np.argmin([sse(v) for v in grid]))
    lo, hi = grid[max(j - 1, 0)], grid[min(j + 1, grid.size - 1)]
    m = minimize_scalar(sse, bounds=(lo, hi), method="bounded")
    return exp(m.x)


def main():
    allr = rows()
    back = [r for r in allr if r["back"]]
    print(
        f"# fit_law.py: {len(allr)} curve points, {len(back)} with the field back out; residual = law - stage 2"
    )
    notback = [r for r in allr if not r["back"]]
    for r in notback:
        print(
            f"# not back out (excluded): {r['set']} ln R {r['lnR']:.3f} {r['reg']} ell {r['ell']} frac {r['frac']:.4f}"
        )
    for n in (1, 2):
        pts = [r for r in back if r["n"] == n]
        g = groups(pts)
        print(f"\n## n = {n}")
        bestL = {
            k: best_L(v) if v else None
            for k, v in g.items()
            if k != "one-loop ell < 1 (plateau)"
        }
        for option, name in (
            ("i", "(i) L_E = ln(1 + x)"),
            ("ii", "(ii) L = n"),
            ("iii", "(iii) L_M = x^(1/n)"),
            ("iv", "(iv) best constant L"),
        ):
            print(f"# {name}")
            for gname, gp in g.items():
                subsets = [
                    ("all", gp),
                    ("mapped", [r for r in gp if r["mark"] == "mapped"]),
                ]
                subsets += [
                    (c, [r for r in gp if r["set"].startswith(f"curve-{c}-")])
                    for c in ("4e-31", "1e-33", "1e-15")
                ]
                for subset, sp in subsets:
                    if not sp:
                        continue
                    Lc = bestL.get(gname) if option == "iv" else None
                    res = [
                        ps.f_law(r["reg"], r["ell"], L_of(option, r, Lc), r["lnR"])
                        - r["frac"]
                        for r in sp
                    ]
                    extra = (
                        f"  L = {Lc:.4g}" if option == "iv" and Lc is not None else ""
                    )
                    print(f"  {gname:28s} {subset:7s} {stats(res)}{extra}")
        # (v) the refit
        print("# (v) the law's form refitted on this wall (Born coefficient 2.69 held)")
        one = g["one-loop ell >= 1"]
        if one:
            ell = np.array([r["ell"] for r in one])
            F = np.array([r["frac"] for r in one])
            fn = lambda e, b: 1.0 - np.exp(-2.69 / (e + b))  # noqa: E731
            (b,), _ = curve_fit(fn, ell, F, p0=(0.5,))
            print(f"  one-loop ell >= 1: b = {b:.4f};  {stats(fn(ell, b) - F)}")
            mp = [r for r in one if r["mark"] == "mapped"]
            if mp:
                e2 = np.array([r["ell"] for r in mp])
                print(
                    f"  one-loop ell >= 1  mapped  {stats(fn(e2, b) - np.array([r['frac'] for r in mp]))}"
                )
        gp = [r for r in g["gap"] if r["lnR"] > 0.3]
        if len(gp) >= 4:
            X = np.array([r["lnR"] for r in gp])
            F = np.array([r["frac"] for r in gp])
            fg = lambda z, G, R0, P: 1.0 - np.exp(
                -G * np.clip(z - R0, 0.0, None) ** P
            )  # noqa: E731
            p, _ = curve_fit(fg, X, F, p0=(0.05, 1.0, 1.5), maxfev=50000)
            allg = g["gap"]
            Xa = np.array([r["lnR"] for r in allg])
            print(
                f"  gap: G = {p[0]:.4f}, R0 = {p[1]:.4f}, P = {p[2]:.4f} (fitted on ln R > 0.3);  "
                f"{stats(fg(Xa, *p) - np.array([r['frac'] for r in allg]))}"
            )
        pl = [r["frac"] for r in g["one-loop ell < 1 (plateau)"]]
        if pl:
            print(
                f"  plateau (ell < 1, measured): {min(pl):.3f} to {max(pl):.3f}, mean {np.mean(pl):.3f}"
            )
        allf = [r["frac"] for r in allr if r["n"] == n]
        print(
            f"  cap (largest measured fraction on the curves, back out or not): {max(allf):.4f}"
        )
        # classical
        cl = [r for r in pts if r["reg"] == "classical"]
        if cl:
            print("# classical points: f against R S_n and R S(L_E)")
            for r in cl:
                R = exp(r["lnR"])
                print(
                    f"  {r['set']:16s} ln R {r['lnR']:6.2f}  f {r['frac']:.4e}  R S_n {R * S_N[n]:.4e} "
                    f"(f/(R S_n) {r['frac'] / (R * S_N[n]):.3f})  R S(L_E = {r['LE']:.3g}) "
                    f"{R * ps.S_of_table(r['LE']):.4e} (f/(R S(L_E)) {r['frac'] / (R * ps.S_of_table(r['LE'])):.3f})"
                )
        # for reference, not one of the prompt's options: the same law read at the floor-free chain's ell (and its
        # regime), at L_E and at L = n; stage 2's fraction depends on ln R alone, and ell_inf is ell's value without the
        # floor's suppression of lambda (validate V6)
        print(
            "# for reference (not an option of §1.7): the law at ell_inf, the floor-free chain's ell and regime"
        )
        for option, name in (("i", "L_E"), ("ii", "L = n")):
            for gname, test in (
                (
                    "one-loop ell_inf >= 1",
                    lambda r: r["ell_inf"] is not None and r["ell_inf"] >= 1.0,
                ),
                (
                    "gap (no floor-free chain)",
                    lambda r: r["ell_inf"] is None and r["lnR"] > 0.0,
                ),
            ):
                sp = [r for r in pts if test(r)]
                res = []
                for r in sp:
                    reg = "one_loop" if r["ell_inf"] is not None else "gap"
                    res.append(
                        ps.f_law(reg, r["ell_inf"], L_of(option, r), r["lnR"])
                        - r["frac"]
                    )
                print(f"  {name:6s} {gname:28s} {stats(res)}")
    print(
        "\n# the exponential's accuracy (crossover out/fit_law.txt): one-loop max 0.074 (rms 0.021), gap max 0.062 "
        "(rms 0.024)"
    )


if __name__ == "__main__":
    main()
