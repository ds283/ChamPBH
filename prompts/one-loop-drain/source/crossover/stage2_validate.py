"""
crossover side-campaign, stage 2 validation (stage2-brief.md §6), before any scan.

  V1 the Born limit: 2197 at A = 0.05, bounces 5, 11, 23, 35 (ell = 9.5 to 4.9). born.py's self-consistent scheme
     (k_IR = sqrt(V''_max)) against stage 2 cut at the same k_IR, and both at a fixed k_IR = 10 m_ta, where every
     mode has k >> m and the Born reduction should hold; then stage 2 with the physical infrared end (the mass).
  V2 the classical limit: the loop switched off, 8 pi^2 rho = S(L), against out/stage1a_S_of_L.txt at L = 5, 10, 40.
     V2q: stage 1a's quadrature (linear-k trapezoid on 121 log-spaced points) against finer grids, at L = 10, with
     walls.py's |beta_k|^2, which accounts for the 0.1% by which V2 falls below stage 1a.
  V3 energy: the largest |E_tot(t) - E|/E along each run, in every row.
  V4 convergence at five points from ell = 4.9 to the gap: halving the mode spacing, doubling k_max, halving k_min,
     halving the step, and the start thresholds (EPS/10, F0/100), each at the base run's grid references.
  V6 the classical limit with the loop on: the last gap bounce of 2197 at A = 0.01 (L = 0.678) at ln R from -6 to 0
     (0 is the bounce's own), frac/R against S(L) with the loop off.
  V5 the start threshold at large ell (R-865's last one-loop bounce, ell = 17.75): EPS from 1e-3 to 1e-9.

    ../../../../venv/bin/python stage2_validate.py > out/stage2_validation.txt
"""

import sys
import time
from math import exp, sqrt
from multiprocessing import Pool

sys.path.insert(0, ".")
import born  # noqa: E402
import stage2  # noqa: E402
from walls import Wall  # noqa: E402

BORN_PTS = (
    (5, 26.74, 52.151),
    (11, 20.95, 44.44),
    (23, 16.89, 38.696),
    (35, 14.66, 35.379),
)
S_TABLE = {5.0: 3.1852e-03, 10.0: 1.3529e-03, 40.0: 1.3936e-04}
CONV_PTS = (
    ("A0.05 #35", 14.66, 35.379),
    ("A0.05 #107", 7.885, 23.833),
    ("A0.05 #128", 5.716, 19.242),
    ("A0.01 #183 gap", 3.619, 13.791),
    ("A0.01 #430 gap", 1.794, 7.124),
)
VARIANTS = (
    ("base", {}),
    ("ppd x2", dict(ppd=160)),
    ("kmax x2", dict(cmax=60.0)),
    ("kmin /2", dict(cmin=0.015)),
    ("h /2", dict(h=0.05)),
    ("EPS /10", dict(eps=stage2.EPS / 10)),
    ("F0 /100", dict(f0=stage2.F0 / 100)),
)


def fmt(r):
    return (
        f"{r['frac_modes']:8.5f} {r['frac_balance']:8.5f} {r['vout']:7.4f} {str(r['returned']):>5s} "
        f"{1 / r['phi_turn']:7.3f} {r['drift']:8.1e} {r['nk']:4d} {r['steps']:7d}"
    )


def v1(pt):
    idx, L, lnR = pt
    w, R = Wall(L), exp(lnR)
    out = []
    t0 = time.time()
    rb = born.run_self_consistent(w, R)
    kIR, kex, m_ta = rb["kIR"], rb["chain"]["kex"], rb["chain"]["m_ta"]
    ell = rb["chain"]["ell"]
    rs = stage2.solve(L, lnR, kmin=kIR, kmax=30 * kex, mref=m_ta)
    out.append(
        f"V1 #{idx:3d} ell={ell:6.3f} kIR=sqrt(V''max)={kIR / m_ta:5.3f} m_ta   born {rb['Efl_final']:8.5f}  "
        f"stage2-cut {fmt(rs)}  ratio {rs['frac_modes'] / rb['Efl_final']:7.4f}"
    )
    k10 = 10.0 * m_ta
    rb10 = born.run(w, R, k10)
    rs10 = stage2.solve(L, lnR, kmin=k10, kmax=30 * kex, mref=m_ta)
    out.append(
        f"V1 #{idx:3d} ell={ell:6.3f} kIR=10 m_ta                 born {rb10['Efl_final']:8.5f}  "
        f"stage2-cut {fmt(rs10)}  ratio {rs10['frac_modes'] / rb10['Efl_final']:7.4f}"
    )
    rp = stage2.run_point(L, lnR)
    out.append(
        f"V1 #{idx:3d} ell={ell:6.3f} physical (kmin = 0.03 m_ta)                  "
        f"stage2     {fmt(rp)}  vs born-sc {rp['frac_modes'] / rb['Efl_final']:7.4f}  2.7/ell {2.7 / ell:7.4f}"
        f"  ({time.time() - t0:.0f} s)"
    )
    return "\n".join(out)


def v2(L):
    w = Wall(L)
    kref = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0
    out = []
    for kmin, ppd in ((3e-3, 80), (1.5e-3, 160)):
        r = stage2.solve(
            L, 0.0, kmin=kmin, kmax=30 * kref, ppd=ppd, mref=1.0, loop=False
        )
        out.append(
            f"V2 L={L:5.1f} kmin={kmin:.1e} ppd={ppd:3d}  S = {r['frac_modes']:.5e}  stage 1a {S_TABLE[L]:.5e}  "
            f"ratio {r['frac_modes'] / S_TABLE[L]:.5f}  v_out/v_in {r['vout']:.6f}"
        )
    return "\n".join(out)


def v2q(L):
    import numpy as np
    import walls

    w = Wall(L)
    b = walls.Bounce(w, w.start_phi(1e-3))
    out = []
    for n in (121, 481):
        ks = np.geomspace(1e-3, 10.0, n)
        b2 = np.array(
            [walls.beta_squared(b, w.V2, w.V3, k, w.start_phi(k)) for k in ks]
        )
        lin = 4.0 * np.trapezoid(ks**3 * b2, ks)
        lnk = 4.0 * np.trapezoid(ks**4 * b2, np.log(ks))
        out.append(
            f"V2q L={L:5.1f} walls.py, {n:3d} points in [1e-3, 10]: linear-k trapezoid {lin:.5e} "
            f"(/stage 1a {lin / S_TABLE[L]:.5f}), ln-k trapezoid {lnk:.5e} (/stage 1a {lnk / S_TABLE[L]:.5f})"
        )
    return "\n".join(out)


def v4(pt):
    name, L, lnR = pt
    t0 = time.time()
    base = stage2.run_point(L, lnR)
    mref, kref = base["mref"], base["kref"]
    ch = base["chain"]
    ell = f"{ch['ell']:6.3f}" if ch else "  none"
    out = [
        f"V4 {name:16s} L={L:6.3f} lnR={lnR:7.3f} ell={ell} passes={base['passes']} "
        f"mref={mref:.3e} kref={kref:.3e} eps_turn={base['eps_turn']:.2e}"
    ]
    for vname, kw in VARIANTS:
        if vname == "base":
            r = base
        else:
            kw = dict(kw)
            cmin = kw.pop("cmin", 0.03)
            cmax = kw.pop("cmax", 30.0)
            ppd = kw.pop("ppd", 80)
            r = stage2.solve(
                L,
                lnR,
                kmin=cmin * mref,
                kmax=cmax * kref,
                ppd=ppd,
                mref=mref,
                **kw,
            )
        out.append(
            f"V4 {name:16s} {vname:8s} {fmt(r)}  d(frac) {r['frac_modes'] - base['frac_modes']:+.1e}"
        )
    out[-1] += f"  ({time.time() - t0:.0f} s)"
    return "\n".join(out)


def v5(eps):
    r = stage2.run_point(51.77, 82.399, eps=eps)
    return (
        f"V5 R-865 #39 ell=17.75 EPS={eps:.0e}  frac(modes) {r['frac_modes']:.5f}  frac(balance) "
        f"{r['frac_balance']:.5f}  max|dE/E| {r['drift']:.1e}  vout/vin {r['vout']:.4f}"
    )


def v6(lnR):
    L = 0.6779
    if lnR is None:
        w = Wall(L)
        kref = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0
        r = stage2.solve(L, 0.0, kmin=0.03, kmax=30 * kref, mref=1.0, loop=False)
        return f"V6 L={L} loop off           S(L) = {r['frac_modes']:.4e}"
    r = stage2.run_point(L, lnR)
    return (
        f"V6 L={L} ln R={lnR:5.1f} loop on  frac/R = {r['frac_modes'] / exp(lnR):.4e}  frac {r['frac_modes']:.3e}  "
        f"vout/vin {r['vout']:.4f}  M/phi_turn {1 / r['phi_turn']:.3f}  max|dE/E| {r['drift']:.1e}"
    )


def task(job):
    kind, arg = job
    return {"v1": v1, "v2": v2, "v2q": v2q, "v4": v4, "v5": v5, "v6": v6}[kind](arg)


if __name__ == "__main__":
    print(
        "# stage 2 validation. Columns of each run: frac(modes) = 8 pi^2 R rho, frac(balance) = (E - phi'^2/2 - U)/E,"
    )
    print("# v_out/v_in, returned, M/phi_turn, max |dE/E| along the run, modes, steps")
    jobs = (
        [("v4", p) for p in CONV_PTS]
        + [("v1", p) for p in BORN_PTS]
        + [("v2", L) for L in S_TABLE]
        + [("v2q", 10.0)]
        + [("v5", e) for e in (1e-3, 1e-5, 1e-7, 1e-9)]
        + [("v6", x) for x in (None, -6.0, -4.0, -2.0, 0.0)]
    )
    with Pool(6) as pool:
        for line in pool.imap(task, jobs):
            print(line, flush=True)
