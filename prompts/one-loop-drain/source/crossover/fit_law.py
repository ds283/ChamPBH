"""
crossover side-campaign: the qualitative drain law fitted to every stage-2 point (out/stage2_scan.txt and
out/stage2_mscan.txt), in the variables the code has at each candidate, ell, L = M/phi_t and R, with no explicit M.
The user asked for qualitative behaviour, not percent accuracy (2026-11-04).

  one-loop, ell >= 1:  f = 1 - exp(-2.69 / (ell + b0 - c ln L)), the coefficient held at its Born value;
  gap (R > 1, no chain): y = a (ln R - c)^p (1 + b ln L) for ln R > c, else 0; f = 1 - exp(-y).
Fitted on the points where the field came back out (one-loop) and on gap points with ln R > 0.3 (the grid's to
ln R <= 15, beyond which the field stops far out in the 1/phi tail, off every history's curve).

    ../../../../venv/bin/python fit_law.py > out/fit_law.txt
"""

import warnings

import numpy as np
from scipy.optimize import curve_fit

warnings.simplefilter("ignore")


def scan_rows():
    """(set, L, ln R, ell or None, frac, back) from both scans."""
    out = []
    for line in open("out/stage2_scan.txt"):
        f = line.split()
        if not f or f[0].startswith("#"):
            continue
        o = 2 if f[0] == "grid" else 3
        ell = None if f[o + 2] == "none" else float(f[o + 2])
        out.append(
            (
                f[0],
                float(f[o]),
                float(f[o + 1]),
                ell,
                float(f[o + 4]),
                f[o + 9] == "True",
            )
        )
    for line in open("out/stage2_mscan.txt"):
        f = line.split()
        if not f or f[0].startswith("#"):
            continue
        ell = None if f[6] == "none" else float(f[6])
        out.append((f[0], float(f[3]), float(f[4]), ell, float(f[8]), f[13] == "True"))
    return out


def report(name, fn, X, F, p0, labels):
    p, _ = curve_fit(fn, X, F, p0=p0, maxfev=50000)
    ff = fn(X, *p)
    print(
        f"# {name}: params {np.round(p, 4).tolist()}  max|df| {np.abs(ff - F).max():.3f}  "
        f"rms {np.sqrt(np.mean((ff - F) ** 2)):.3f}  ({len(F)} points)"
    )
    for lab, x, f_, g in zip(labels, np.array(X).T, F, ff):
        print(
            f"{lab:10s} "
            + " ".join(f"{v:8.3f}" for v in x)
            + f"   f {f_:.3f}  fit {g:.3f}"
        )
    return p


rows = scan_rows()

one = [r for r in rows if r[3] is not None and r[5] and r[3] >= 1.0]
X1 = (np.array([r[3] for r in one]), np.array([r[1] for r in one]))
F1 = np.array([r[4] for r in one])
print("# one-loop: columns ell, L")
report(
    "old form f = 1 - exp(-2.69/(ell + b)), b refitted (0.12 on the laboratory curve alone): params b",
    lambda Z, b: 1 - np.exp(-2.69 / (Z[0] + b)),
    X1,
    F1,
    (0.12,),
    [r[0] for r in one],
)
report(
    "f = 1 - exp(-2.69/(ell + b0 - c ln L)): params b0, c",
    lambda Z, b0, c: 1 - np.exp(-2.69 / (Z[0] + b0 - c * np.log(Z[1]))),
    X1,
    F1,
    (0.5, 0.3),
    [r[0] for r in one],
)

gap = [
    r
    for r in rows
    if r[3] is None
    and r[2] > 0.3
    and r[5]
    and not r[0].startswith("curve")
    and not (r[0] == "grid" and r[2] > 15.0)
]
X2 = (np.array([r[2] for r in gap]), np.array([r[1] for r in gap]))
F2 = np.array([r[4] for r in gap])
print("# gap: columns ln R, L")
report(
    "y = a (ln R - c)^p (1 + b ln L), f = 1 - exp(-y): params a, c, p, b",
    lambda Z, a, c, p, b: 1
    - np.exp(-a * np.clip(Z[0] - c, 0.0, None) ** p * (1 + b * np.log(Z[1]))),
    X2,
    F2,
    (0.05, 1.0, 1.5, 0.5),
    [r[0] for r in gap],
)
