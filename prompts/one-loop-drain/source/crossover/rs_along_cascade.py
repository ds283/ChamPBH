"""
crossover side-campaign, stage 1: the classical test-field ratio R S(L) at each bounce of a mapped cascade.

R S(L) is what a test field reflecting off the classical wall would produce, as a fraction of the bounce's kinetic
energy (production_energy_ratio.tex eq:S). Where it is >> 1 the field cannot reach the classical wall without the
production stopping it first, so the classical reflection is not the right limit there; where it is << 1 the
classical reflection is nearly elastic. S(L) is stage 1a's (out/stage1a_S_of_L.txt), interpolated linearly in
ln L, and held at its end values outside L in [1, 134].

    ../../../../venv/bin/python rs_along_cascade.py out/cascade-P-2197-A0.01.txt > out/rs-P-2197-A0.01.txt
"""

import sys
from math import exp, log

import numpy as np


def stage1a():
    L, S = [], []
    for line in open("out/stage1a_S_of_L.txt"):
        if line.startswith("#"):
            continue
        f = line.split()
        L.append(float(f[0]))
        S.append(float(f[5]))
    return np.log(L), np.log(S)


def main(path):
    lnL, lnS = stage1a()
    rows = []
    head = None
    for line in open(path):
        if (
            line.startswith("# P-")
            or line.startswith("# W-")
            or line.startswith("# R-")
        ):
            head = line.strip()
        f = line.split()
        # the integrator's progress lines (status updates) are interleaved with the table; keep table rows only
        if not f or not f[0].isdigit():
            continue
        idx, N, regime = int(f[0]), float(f[1]), f[2]
        # columns: idx N regime [ell] alpha_eff ln(E/V0) lnR L ell_t ln(E/M^4); ell is blank off one-loop
        rest = f[3:]
        if regime == "one_loop":
            ell, alpha = float(rest[0]), float(rest[1])
            rest = rest[2:]
        else:
            ell, alpha = None, float(rest[0])
            rest = rest[1:]
        if len(rest) < 5:
            continue
        lnEV0, lnR, L = float(rest[0]), float(rest[1]), float(rest[2])
        lnS_L = float(np.interp(log(L), lnL, lnS))
        rows.append((idx, N, regime, ell, alpha, lnEV0, lnR, L, lnR + lnS_L))
    print(head)
    print("# idx       N     regime      ell     ln R        L   ln(R S(L))   R S(L)")
    # every bounce where something changes, and a sample of the rest
    last = None
    for i, r in enumerate(rows):
        idx, N, regime, ell, alpha, lnEV0, lnR, L, lnRS = r
        crosses = last is not None and (
            (last[8] > 0) != (lnRS > 0) or last[2] != regime
        )
        if crosses or i % max(1, len(rows) // 40) == 0 or i == len(rows) - 1:
            print(
                f"{idx:5d} {N:8.4f} {regime:>9s} {'' if ell is None else format(ell, '8.3f'):>8s} "
                f"{lnR:8.3f} {L:8.4g} {lnRS:11.3f} {exp(min(lnRS, 700)):9.3g}"
                + ("   <- change" if crosses else "")
            )
        last = r
    first_below = next((r for r in rows if r[8] < 0), None)
    print(
        "# first bounce with R S(L) < 1: "
        + (
            "none"
            if first_below is None
            else f"idx {first_below[0]} N {first_below[1]:.4f} regime {first_below[2]} ln R {first_below[6]:.3f} L {first_below[7]:.4g}"
        )
    )


if __name__ == "__main__":
    main(sys.argv[1])
