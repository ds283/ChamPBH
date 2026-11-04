"""
crossover side-campaign, stage 2 scan (stage2-brief.md §7): the self-consistent fraction along the mapped cascades and
on a small (L, ln R) grid.

  2197 at A = 0.05: one-loop rows nearest ell = 9.5, 7.4, 4.9, 3.7, 3, 2.5, 2, 1.5, 1 and the last (0.52);
  2197 at A = 0.01: one-loop rows nearest ell = 3, 2, 1.5, 1, 0.5, 0.25 and the last (0.083); gap rows 183, 250, 348,
                    430, 512, 600, 690;
  R-865 at A = 0.05 (beta = 2): its first, a middle and its last one-loop rows;
  the grid L in {1.3, 2, 3, 4, 6, 8} x ln R in {5, 10, 15, 20, 25}.

Each point is stage2.run_point at the base resolution validated in out/stage2_validation.txt. Columns: the chain's
ell (none in the gap), R S(L) with S from out/stage1a_S_of_L.txt (log-interpolated in L), the fraction of E in
quanta at the end from the modes (frac) and from the background's balance (E - phi'^2/2 - U)/E, 2.7/ell,
born.py's self-consistent fraction where its chain exists and ell >= 1.4, v_out/v_in and whether the field came
back out past V'' = 1e-6 m_ref^2 within the run, M/phi at the turn, ell_turn = ln(k_turn/m_turn) with
k_turn = lambda sqrt(E)/2 and m_turn = sqrt(V'') at the turn, the loop parameter eps = V''''/16 pi^2 at the turn,
the Hartree shift K V'''' Sigma at the turn over k_turn^2, the largest |dE/E|.

    ../../../../venv/bin/python stage2_scan.py > out/stage2_scan.txt
"""

import sys
import time
from math import exp, log
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import born  # noqa: E402
import stage2  # noqa: E402
from walls import Wall  # noqa: E402


def rows(path):
    out = []
    for line in open(path):
        if line.startswith("#") or line.startswith("@@") or not line.strip():
            continue
        f = line.split()
        if not f[
            0
        ].isdigit():  # the integrator's status reports, interleaved in the map
            continue
        if f[2] == "one_loop":
            out.append(
                dict(
                    idx=int(f[0]),
                    reg=f[2],
                    ell=float(f[3]),
                    lnR=float(f[6]),
                    L=float(f[7]),
                )
            )
        else:
            out.append(
                dict(idx=int(f[0]), reg=f[2], ell=None, lnR=float(f[5]), L=float(f[6]))
            )
    return out


def nearest(rs, targets):
    ol = [r for r in rs if r["reg"] == "one_loop"]
    pts = []
    for t in targets:
        r = ol[-1] if t == "last" else min(ol, key=lambda c: abs(c["ell"] - t))
        if r not in pts:
            pts.append(r)
    return pts


def S_of_L():
    a = np.loadtxt("out/stage1a_S_of_L.txt", comments="#", usecols=(0, 5))
    lL, lS = np.log(a[:, 0]), np.log(a[:, 1])
    return lambda L: float(np.exp(np.interp(log(L), lL, lS)))


def points():
    c05 = rows("out/cascade-P-2197-A0.05.txt")
    c01 = rows("out/cascade-P-2197-A0.01.txt")
    r865 = rows("out/cascade-R-865-A0.05.txt")
    pts = []
    for r in nearest(c05, (9.5, 7.4, 4.9, 3.7, 3.0, 2.5, 2.0, 1.5, 1.0, "last")):
        pts.append(("2197 A0.05", r))
    for r in nearest(c01, (3.0, 2.0, 1.5, 1.0, 0.5, 0.25, "last")):
        pts.append(("2197 A0.01", r))
    gap = {r["idx"]: r for r in c01}
    for i in (183, 250, 348, 430, 512, 600, 690):
        pts.append(("2197 A0.01", gap[i]))
    ol = [r for r in r865 if r["reg"] == "one_loop"]
    for r in (ol[0], ol[len(ol) // 2], ol[-1]):
        pts.append(("R-865 A0.05", r))
    for L in (1.3, 2.0, 3.0, 4.0, 6.0, 8.0):
        for lnR in (5.0, 10.0, 15.0, 20.0, 25.0):
            pts.append(("grid", dict(idx=-1, reg="grid", ell=None, lnR=lnR, L=L)))
    return pts


S = S_of_L()


def task(pt):
    label, r = pt
    t0 = time.time()
    L, lnR = r["L"], r["lnR"]
    w = Wall(L)
    ch = born.chain(w, exp(lnR))
    ell = ch[3] if ch else None
    rs = S(L) * exp(lnR)
    try:
        res = stage2.run_point(L, lnR)
    except Exception as e:  # noqa: BLE001
        return f"{label:12s} {r['idx']:5d} {L:7.3f} {lnR:8.3f}   failed: {e}"
    fb = float("nan")
    if ch is not None and ell >= 1.4:
        try:
            fb = born.run_self_consistent(w, exp(lnR))["Efl_final"]
        except Exception:  # noqa: BLE001
            pass
    ell_s = f"{ell:7.3f}" if ell is not None else "   none"
    law = f"{2.7 / ell:8.4f}" if ell is not None else "     nan"
    ell_turn = log(res["kex_turn"] / res["m_turn"])
    return (
        f"{label:12s} {r['idx']:5d} {L:7.3f} {lnR:8.3f} {ell_s} {rs:9.2e} {res['frac_modes']:8.4f} "
        f"{res['frac_balance']:8.4f} {law} "
        f"{fb:8.4f} {res['vout']:7.4f} {str(res['returned']):>5s} {1 / res['phi_turn']:7.3f} {ell_turn:7.3f} "
        f"{res['eps_turn']:8.1e} {res['hartree_turn'] / res['kex_turn'] ** 2:8.1e} {res['drift']:7.1e} "
        f"{res['passes']:2d} {time.time() - t0:5.0f}"
    )


if __name__ == "__main__":
    print(
        "# stage 2 scan: fraction of E in quanta after one bounce, self-consistent one-loop scheme with massive modes"
    )
    print(
        "# set          idx       L     ln R     ell    R S(L)     frac  balance  2.7/ell  born-sc vout/vin back?  "
        "M/phi_ta ell_turn  eps_turn  H/k_turn^2  |dE/E| passes  s"
    )
    with Pool(6) as pool:
        for line in pool.imap(task, points()):
            print(line, flush=True)
