"""
crossover side-campaign, after its close: stage 2 on the n = 2 wall, V = V0 exp((M/phi)^2), for U15 (README §8.10),
asked by the user on 2026-11-05.

walls.WallN carries the wall (n = 1's Wall is unchanged). Here L = ln(1 + E/V0) = (M/phi_t)^n, so that at n = 2 the
two candidate inputs of the adopted law differ: L_E = L and L_M = M/phi_t = L^{1/2}.

  V-A  the classical limit: stage 2 with the loop off against walls.py's independent test field (Bounce and
       beta_squared on WallN), S(L) at L = 5 and 20, with a 241-point ln-k trapezoid on [1e-3, 10];
  V-B  the Born limit: stage 2 cut at k_IR = 10 m_ta against born.run at the same k_IR, at two one-loop points;
  S2   S(L) on the n = 2 wall at L = 1-100, loop off;
  scan the self-consistent fraction on a grid L in {2, 4, 8, 16, 30, 60, 100} x ln R in {3, 5, 8, 12, 16, 20, 25,
       30, 40}, with the adopted law's prediction at L_E and at L_M.

    ../../../../venv/bin/python stage2_n2.py > out/stage2_n2.txt
"""

import sys
import time
from math import exp, log, sqrt
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import born  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import WallN  # noqa: E402

N = 2
LS = (2.0, 4.0, 8.0, 16.0, 30.0, 60.0, 100.0)
LNRS = (3.0, 5.0, 8.0, 12.0, 16.0, 20.0, 25.0, 30.0, 40.0)
S2_LS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 60.0, 100.0)


def f_law(regime, ell, L, lnR, S):
    """The adopted law (README §8.10) at the L given; S is the S(L) the classical row uses."""
    if regime == "one_loop":
        if ell < 1.0:
            return 0.9
        d = ell + 0.9691 - 0.41 * log(L)
        return min(0.95, 1.0 - exp(-2.69 / d)) if d > 0 else 0.95
    if regime == "gap":
        if lnR <= 1.5601:
            return 0.0
        return 1.0 - exp(-0.0198 * (lnR - 1.5601) ** 1.5472 * (1.0 + 0.841 * log(L)))
    return exp(lnR) * S


def S_classical(L):
    w = WallN(L, N)
    kref = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0
    r = stage2.solve(L, 0.0, kmin=1e-3, kmax=30 * kref, mref=1.0, loop=False, n=N)
    return r["frac_modes"]


def va(L):
    w = WallN(L, N)
    b = walls.Bounce(w, w.start_phi(1e-3))
    ks = np.geomspace(1e-3, 10.0, 241)
    b2 = np.array([walls.beta_squared(b, w.V2, w.V3, k, w.start_phi(k)) for k in ks])
    s_ind = 4.0 * np.trapezoid(ks**4 * b2, np.log(ks))
    s2 = S_classical(L)
    return f"V-A n=2 L={L:5.1f}: S stage 2 (loop off) {s2:.5e}  walls.py {s_ind:.5e}  ratio {s2 / s_ind:.5f}"


def vb(pt):
    L, lnR = pt
    w = WallN(L, N)
    R = exp(lnR)
    ch = born.chain(w, R)
    k10 = 10.0 * ch[1]
    rb = born.run(w, R, k10)
    rs = stage2.solve(L, lnR, kmin=k10, kmax=30 * ch[2], mref=ch[1], n=N)
    return (
        f"V-B n=2 L={L:5.1f} lnR={lnR:5.1f} ell={ch[3]:.3f}: k_IR = 10 m_ta  born {rb['Efl_final']:.5f}  "
        f"stage2-cut {rs['frac_modes']:.5f}  ratio {rs['frac_modes'] / rb['Efl_final']:.5f}  |dE/E| {rs['drift']:.1e}"
    )


def scan(pt):
    L, lnR = pt
    t0 = time.time()
    w = WallN(L, N)
    ch = born.chain(w, exp(lnR))
    ell = ch[3] if ch else None
    regime = "one_loop" if ch else ("gap" if lnR > 0 else "classical")
    try:
        res = stage2.run_point(L, lnR, n=N)
    except Exception as e:  # noqa: BLE001
        return f"scan {L:6.1f} {lnR:5.1f} {regime:>9s} failed: {e}"
    LM = L ** (1.0 / N)
    fE = f_law(regime, ell, L, lnR, 0.0)
    fM = f_law(regime, ell, LM, lnR, 0.0)
    ell_s = f"{ell:7.3f}" if ell is not None else "   none"
    return (
        f"scan {L:6.1f} {lnR:5.1f} {regime:>9s} {ell_s} {res['frac_modes']:8.4f} {res['frac_balance']:8.4f} "
        f"{fE:8.4f} {fM:8.4f} {res['vout']:7.4f} {str(res['returned']):>5s} {res['eps_turn']:8.1e} "
        f"{res['drift']:7.1e} {time.time() - t0:5.0f}"
    )


def task(job):
    kind, arg = job
    if kind == "s2":
        return f"S2 n=2 L={arg:6.1f}: S(L) = {S_classical(arg):.5e}"
    return {"va": va, "vb": vb, "scan": scan}[kind](arg)


def find_lnR(L, target_ell):
    """ln R on the n = 2 wall at L where the chain's ell is nearest target (coarse search)."""
    w = WallN(L, N)
    best = None
    for lnR in np.arange(0.5, 80.0, 0.25):
        ch = born.chain(w, exp(lnR))
        if ch is not None and (
            best is None or abs(ch[3] - target_ell) < abs(best[1] - target_ell)
        ):
            best = (lnR, ch[3])
    return best[0]


if __name__ == "__main__":
    vb_pts = [(20.0, find_lnR(20.0, 6.0)), (60.0, find_lnR(60.0, 5.0))]
    jobs = (
        [("va", L) for L in (5.0, 20.0)]
        + [("vb", p) for p in vb_pts]
        + [("s2", L) for L in S2_LS]
        + [("scan", (L, r)) for L in LS for r in LNRS]
    )
    print(
        "# stage 2 on the n = 2 wall (U15). L = ln(1 + E/V0); L_M = M/phi_t = L^(1/2)"
    )
    print(
        "# scan columns: L  ln R  regime  ell(chain)  frac  balance  law(L_E)  law(L_M)  vout/vin  back?  "
        "eps_turn  |dE/E|  s   (law columns for the classical regime omit R S(L))"
    )
    with Pool(6) as pool:
        for line in pool.imap(task, jobs):
            print(line, flush=True)
