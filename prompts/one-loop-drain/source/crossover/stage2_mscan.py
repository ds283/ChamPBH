"""
crossover side-campaign, after stage 2: stage 2's fraction on the curves of other M, against the measured law
(measured_law_map.fraction), asked by the user on 2026-11-04.

On a history, L and R are tied by V0/M^4:
    ln R = ln(V0/M^4) + 2L + 2 ln(L^4 + 2L^3) - ln(e^L - 1) - ln 8 pi^2.
The laboratory curve has ln(V0/M^4) = 3.345. Here:
  - the beta = 2 ladder near M_crit (about 1 TeV for QCD surfing, wall_backreaction.tex §"Where the field is
    stopped"): L-M8e-18 (ln(V0/M^4) = -119.2), L-M1e-16 (-129.3, one one-loop candidate) and L-M5e-16 (-135.7, none),
    from their A = 1 maps (out/cascade-L-M*-A1.txt): every one-loop and gap row (at most eight of each, evenly
    spaced), and classical rows down to ln R = -3 (at most six);
  - one-loop sweeps along the 8e-18 and 1e-16 curves, L = 96-132 (ell 0.7-9), which the cascades themselves, entering
    at L = 106, cover only at ell <= 4.1;
  - M = 1e-3 M_P (ln(V0/M^4) = -249.0; no history at this M, so points on its curve at L = 134, 105, 60, 20).

Columns as stage2_scan.py's, with ln(V0/M^4), the measured law's f, and frac/(R S(L)).

    ../../../../venv/bin/python stage2_mscan.py > out/stage2_mscan.txt
"""

import sys
import time
from math import exp, log, pi
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import born  # noqa: E402
import stage2  # noqa: E402
from stage2_scan import S, rows  # noqa: E402
from walls import Wall  # noqa: E402

REGIME_ONE_LOOP, REGIME_GAP, REGIME_CLASSICAL = "one_loop", "gap", "classical"


def law(regime, ell, L, lnR):
    """measured_law_map.fraction, restated here without importing the production stack."""
    if regime == REGIME_ONE_LOOP:
        return 1.0 - exp(-2.69 / (ell + 0.12)) if ell >= 1.0 else 0.9
    gl = np.log([9.36e-03, 1.00e-01, 9.99e-01, 8.57e00, 6.99e01, 8.15e02, 4.22e03])
    gf = np.array([0.0027, 0.0591, 0.1594, 0.2819, 0.5223, 0.7734, 0.8264])
    if regime == REGIME_GAP:
        return float(np.interp(lnR + log(S(L)), gl, gf))
    g = float(
        np.interp(
            lnR,
            [-6.0, -4.0, -2.0, 0.0],
            np.array([9.3291, 9.1634, 8.0156, 2.7275]) / 9.3552,
        )
    )
    return exp(lnR) * S(L) * g


def ln_v0m4(L, lnR):
    return lnR + log(8 * pi**2) + log(exp(L) - 1) - 2 * L - 2 * log(L**4 + 2 * L**3)


def lnR_on(L, lv):
    return lv + 2 * L + 2 * log(L**4 + 2 * L**3) - log(exp(L) - 1) - log(8 * pi**2)


def pick(rs, n):
    if len(rs) <= n:
        return rs
    idx = np.unique(np.linspace(0, len(rs) - 1, n).round().astype(int))
    return [rs[i] for i in idx]


def points():
    pts = []
    for M in ("8e-18", "1e-16", "5e-16"):
        rs = rows(f"out/cascade-L-M{M}-A1.txt")
        ol = [r for r in rs if r["reg"] == "one_loop"]
        gp = [r for r in rs if r["reg"] == "gap"]
        cl = [r for r in rs if r["reg"] == "classical" and r["lnR"] >= -3.0]
        for r in pick(ol, 8) + pick(gp, 8) + pick(cl, 6):
            pts.append((f"L-M{M}", r))
    # one-loop sweeps along the two curves near M_crit (ell 0.7-9; the histories enter at L = 106)
    for lv, Ls in (
        (-119.16, (96, 100, 104, 108, 116, 124)),
        (-129.27, (104, 108, 112, 116, 124, 132)),
    ):
        for L in Ls:
            pts.append(
                (
                    f"curve{lv:.0f}",
                    dict(
                        idx=-1,
                        reg="curve",
                        ell=None,
                        lnR=lnR_on(float(L), lv),
                        L=float(L),
                    ),
                )
            )
    for L in (134.0, 105.0, 60.0, 20.0):
        pts.append(
            (
                "M1e-3",
                dict(idx=-1, reg="classical", ell=None, lnR=lnR_on(L, -249.0), L=L),
            )
        )
    return pts


def task(pt):
    label, r = pt
    t0 = time.time()
    L, lnR = r["L"], r["lnR"]
    w = Wall(L)
    ch = born.chain(w, exp(lnR))
    ell = ch[3] if ch else None
    reg = REGIME_ONE_LOOP if ch else (REGIME_GAP if lnR > 0 else REGIME_CLASSICAL)
    rs = S(L) * exp(lnR)
    f_law = law(reg, ell, L, lnR)
    try:
        res = stage2.run_point(L, lnR)
    except Exception as e:  # noqa: BLE001
        return f"{label:9s} {r['idx']:5d} {L:8.3f} {lnR:9.3f}   failed: {e}"
    ell_s = f"{ell:7.3f}" if ell is not None else "   none"
    return (
        f"{label:9s} {r['idx']:5d} {r['reg']:>9s} {L:8.3f} {lnR:9.3f} {ln_v0m4(L, lnR):8.2f} {ell_s} {rs:9.2e} "
        f"{res['frac_modes']:9.3e} {res['frac_balance']:9.3e} {f_law:9.3e} {res['frac_modes'] / rs:8.3f} "
        f"{res['vout']:7.4f} {str(res['returned']):>5s} {1 / res['phi_turn']:8.3f} {res['eps_turn']:8.1e} "
        f"{res['drift']:7.1e} {time.time() - t0:5.0f}"
    )


if __name__ == "__main__":
    print(
        "# stage 2 on the curves of other M: fraction of E in quanta after one bounce, against the measured law"
    )
    print(
        "# set        idx    regime        L      ln R ln(V0/M4)   ell    R S(L)      frac   balance   law f  "
        "frac/RS vout/vin back? M/phi_ta eps_turn  |dE/E|  s"
    )
    with Pool(6) as pool:
        for line in pool.imap(task, points()):
            print(line, flush=True)
