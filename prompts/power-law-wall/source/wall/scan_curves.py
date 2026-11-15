"""
power-law-wall prompt 02 §1.6 (U3): stage 2 at points on each mapped curve, and a small off-curve grid.

The curves are ln R = a + (1 + 4/n) ln x at laboratory M (4e-31 M_P), 1e-33 and 1e-15 M_P, n = 1 and 2 (powerscan).
On each curve:
  - one-loop rows: ell (the chain on PowerWall(x, n), as production computes it) at the curve's largest mapped ell,
    then at 40, 30, 20, 15, 10, 7, 5, 3, 2, 1.5, 1, 0.7, 0.4, 0.2 and 0.1 where the curve has them;
  - the gap: eight points evenly in ln R from the curve's gap entry (the least ln R with a chain) to R = 1;
  - three classical points, ln R = -0.5, -2, -5.
A point whose ell lies below the least ell any mapped history of that curve reached (map_summary.txt) is marked
"beyond", as is every gap and classical point: the map's histories all parked while one-loop.

Each point is run_point_power (powerscan): stage2.solve on PowerWall(x, n) in units of phi_t, with the grid and the
start threshold from the bounce's own scales, which the floor does not change. Columns: set, n, ln R, ln x, L_E,
regime, ell, ell_inf, mapped/beyond, frac (the modes' energy), balance (the background's), v_out/v_in, back,
eps_turn (the loop parameter V''''R E/2 at the turn), |dE/E|, passes, seconds.

The off-curve grid: x in {1e2, 1e3, 1e4, 1e10, 1e40} at four ln R per n, by the copied stage2.run_point (its grid
from the chain on PowerWall(x), which the floor suppresses), against run_point_power at the same ln R, to measure
scale invariance in the fraction itself.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python scan_curves.py \\
        > ../../../../.documents/power-law-wall/wall-02/stage2_scan.txt
"""

import glob
import os
import sys
import time
from math import exp
from multiprocessing import Pool

import numpy as np
from scipy.optimize import brentq

sys.path.insert(0, ".")
import powerscan as ps  # noqa: E402
import stage2  # noqa: E402
from walls import PowerWall  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
MAP = os.path.join(HERE, "../../../../.documents/power-law-wall/wall-02/map")
ELL_TARGETS = (
    40.0,
    30.0,
    20.0,
    15.0,
    10.0,
    7.0,
    5.0,
    3.0,
    2.0,
    1.5,
    1.0,
    0.7,
    0.4,
    0.2,
    0.1,
)
GRID_X = (1e2, 1e3, 1e4, 1e10, 1e40)
GRID_LNR = {1: (10.0, 20.0, 40.0, 100.0), 2: (8.0, 15.0, 25.0, 60.0)}


def mapped_ell_range(M: str, n: int):
    """(least, largest) production ell over the mapped candidates of the curve's histories (beta = 3.05 excluded)."""
    ells = []
    for path in glob.glob(os.path.join(MAP, f"ip-n{n}-M{M}-b*.txt")):
        if "b3.05" in path:
            continue
        for line in open(path):
            f = line.split()
            if line[:1] == " " and len(f) >= 15 and f[8] == "one_loop":
                ells.append(float(f[7]))
    return min(ells), max(ells)


def ell_on_curve(lnV0M4, n, lnR):
    x = exp(ps.lnx_on_curve(lnV0M4, n, lnR))
    return ps.chain_ell(x, n, lnR)


def curve_points(M: str, n: int):
    lnV0M4 = ps.CURVES[M]
    lo_map, hi_map = mapped_ell_range(M, n)
    # the gap's entry: the least ln R on the curve with a chain (0.01 in ln R, then bisection)
    grid = np.arange(0.01, 700.0, 0.05)
    has = [ell_on_curve(lnV0M4, n, r) is not None for r in grid]
    j = has.index(True)
    lo, hi = grid[j - 1], grid[j]
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        if ell_on_curve(lnV0M4, n, mid) is None:
            lo = mid
        else:
            hi = mid
    entry = hi
    pts = []

    def lnR_for(ell):
        g = lambda r: (ell_on_curve(lnV0M4, n, r) or 0.0) - ell  # noqa: E731
        top = entry + 1.0
        while g(top) < 0.0:
            top += 20.0
            if top > 700.0:
                return None
        return brentq(g, entry, top, xtol=1e-9)

    targets = [hi_map] + [e for e in ELL_TARGETS if e < hi_map]
    for ell in targets:
        if ell < ell_on_curve(lnV0M4, n, entry):
            continue
        r = lnR_for(ell)
        if r is not None:
            pts.append(("one_loop", r, "mapped" if ell >= lo_map - 1e-9 else "beyond"))
    for j in range(8):
        pts.append(("gap", entry * (1.0 - (j + 0.5) / 8.0), "beyond"))
    for r in (-0.5, -2.0, -5.0):
        pts.append(("classical", r, "beyond"))
    return lnV0M4, entry, (lo_map, hi_map), pts


def run(job):
    kind = job[0]
    t0 = time.time()
    if kind == "curve":
        _, M, n, lnV0M4, want, lnR, mark = job
        lnx = ps.lnx_on_curve(lnV0M4, n, lnR)
        x = exp(lnx)
        setname = f"curve-{M}-n{n}"
        try:
            res = ps.run_point_power(x, n, lnR)
        except (
            Exception
        ) as e:  # noqa: BLE001  (a point that fails is a result: ruling 4)
            return f"{setname:16s} {n} {lnR:9.4f} {lnx:10.4f} FAILED {type(e).__name__}: {e}"
    else:
        _, n, x, lnR, how = job
        lnx = float(np.log(x))
        setname = f"grid-{how}"
        if how == "power":
            res = ps.run_point_power(x, n, lnR)
        else:
            res = stage2.run_point(None, lnR, wall=PowerWall(x, n))
            res["ell_inf"] = ps.refs(n, lnR)[2]
        mark = "offcurve"
    reg, ell = ps.regime(x, n, lnR)
    L_E = PowerWall(x, n).L
    ell_s = "none" if ell is None else f"{ell:.5f}"
    ei = res.get("ell_inf")
    ei_s = "none" if ei is None else f"{ei:.5f}"
    return (
        f"{setname:16s} {n} {lnR:9.4f} {lnx:10.4f} {L_E:10.4f} {reg:>9s} {ell_s:>9s} {ei_s:>9s} {mark:>8s} "
        f"{res['frac_modes']:9.6f} {res['frac_balance']:9.6f} {res['vout']:7.4f} {str(res['returned']):>5s} "
        f"{res['eps_turn']:9.2e} {res['drift']:8.1e} {res['passes']} {time.time() - t0:5.0f}"
    )


if __name__ == "__main__":
    t0 = time.time()
    print(f"# scan_curves.py; start: {os.popen('uptime').read().strip()}")
    jobs = []
    for M in ("4e-31", "1e-33", "1e-15"):
        for n in (1, 2):
            lnV0M4, entry, (lo, hi), pts = curve_points(M, n)
            print(
                f"# curve M={M} n={n}: ln(V0/M^4) = {lnV0M4}, ln R = {ps.curve_a(lnV0M4, n):.4f} + "
                f"{1 + 4 / n:g} ln x; gap entry ln R = {entry:.4f} (ln x = {ps.lnx_on_curve(lnV0M4, n, entry):.4f}, "
                f"ell there {ell_on_curve(lnV0M4, n, entry):.4f}); mapped ell {lo:.3f} to {hi:.3f}; "
                f"R = 1 at ln x = {ps.lnx_on_curve(lnV0M4, n, 0.0):.4f}"
            )
            jobs += [("curve", M, n, lnV0M4, k, r, m) for k, r, m in pts]
    for n in (1, 2):
        for lnR in GRID_LNR[n]:
            jobs.append(("grid", n, 1e100, lnR, "power"))
            jobs += [("grid", n, x, lnR, "copy") for x in GRID_X]
    print(
        "# set             n      ln R       ln x        L_E    regime       ell   ell_inf     mark      frac   "
        "balance  vout/vin  back  eps_turn   |dE/E| passes  s"
    )
    with Pool(6) as pool:
        for line in pool.imap(run, jobs):
            print(line, flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
