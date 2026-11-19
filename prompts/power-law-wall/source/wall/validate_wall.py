"""
power-law-wall prompt 02 §1.2 (the wall and the units) and §1.3 (the validation, before any scan), on PowerWall at
n = 1 and 2, as crossover's stage 2 was validated (stage2_validate.py). Item 5 (the chain against production) is
chain_vs_production.py, which runs from the repository root. Each tolerance is stated in the output beside its reason.

  D   PowerWall's U, U', V'', V''' and V'''' against central differences of the one below, in both units.
  U   the units: at x = 10 and 10^3 (and 10^2 for ell), n = 1, 2, phi_t units against walls.py's M = 1 units: S (the
      test field, 241-point ln-k trapezoid on [1e-3, 10] omega_t), ell (born.chain), and one stage-2 fraction
      (stage2.run_point on the wall in each unit).
  V1  the classical limit: stage 2 with the loop off against walls.py's independent test field (Bounce, beta_squared)
      at x = 1e40 (units of phi_t) and at x = 1 and 0.1 (units of M, where phi_t = x^{-1/n} >= 1 is well scaled); then
      with the loop on, frac/R against S as R -> 0 (crossover's V6).
  V2  the Born limit: stage 2 cut at k_IR = 10 m_ta against born.run at the same k_IR, at ell_inf = 5 and 8; and
      born.py's self-consistent k_IR = sqrt(V''_max) against stage 2's physical infrared end (run_point_power) at
      ell_inf = 5, 8, 15, 25 and 40 (ruling 3's overlap).
  V3  energy: max |E_tot - E|/E along each run, in every row (column |dE/E|).
  V4  convergence at three points per n spanning the scan (ell_inf ~ 24, ell_inf ~ 1, the gap): mode spacing halved,
      k_max doubled, k_min halved, step halved, EPS/10.
  V6  scale invariance: S, ell and the stage-2 fraction at fixed R and x = 1e10 ... 1e100; and where the floor starts
      to matter, in x: ell(x) against ell_inf, and stage2.run_point's own grid (references from the chain on
      PowerWall(x), which the floor suppresses) against run_point_power's at small x.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python validate_wall.py \\
        > ../../../../.documents/power-law-wall/wall-02/validation.txt
"""

import os
import sys
import time
from math import exp, log, sqrt
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import born  # noqa: E402
import powerscan as ps  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import PowerWall  # noqa: E402

INF = float("inf")


def row(r):
    return (
        f"frac {r['frac_modes']:.6f} bal {r['frac_balance']:.6f} vout {r['vout']:.4f} back {str(r['returned']):>5s} "
        f"|dE/E| {r['drift']:.1e} modes {r['nk']} steps {r['steps']}"
    )


def S_testfield(w, npts=241, kmin=1e-3, kmax=10.0):
    """S = 4 int (k/omega_t)^3 |beta|^2 dk/omega_t by walls.py's test field, ln-k trapezoid (omega_t = 1)."""
    ks = np.geomspace(kmin, kmax, npts)
    starts = [w.start_phi(k) for k in ks]
    b = walls.Bounce(w, max(starts))
    b2 = np.array([walls.beta_squared(b, w.V2, w.V3, k, p) for k, p in zip(ks, starts)])
    return 4.0 * np.trapezoid(ks**4 * b2, np.log(ks)), ks, b2


def S_stage2(w, ppd=80):
    kref = (
        w.n * sqrt(w.E) / 2.0 / w.phi_t
    )  # the floor-free k_ex at phi_t; only the grid's top
    r = stage2.solve(
        w.L, 0.0, kmin=1e-3, kmax=30 * kref, ppd=ppd, mref=1.0, loop=False, wall=w
    )
    return r["frac_modes"], r


def lnR_at(n, ell):
    """ln R where the floor-free chain's ell is the target (bisection on ell_inf(ln R), which rises)."""
    from scipy.optimize import brentq

    def g(r):
        e = ps.chain_ell(INF, n, r)
        return (-1.0 if e is None else e) - ell

    return brentq(g, 18.0 if n == 1 else 12.0, 700.0, xtol=1e-10)


# ---- D -------------------------------------------------------------------------------------------------------------
def task_D(_):
    out = [
        "# D: PowerWall's derivatives against central differences, h = 1e-4 phi; tolerance 1e-6 relative "
        "(the central difference's own error is ~(h/phi)^2 ~ 1e-8 times the derivatives' ratio)"
    ]
    worst = 0.0
    for units, xs in (("phi_t", (10.0, 1e40)), ("M", (10.0, 1e3))):
        for n in (1, 2):
            for x in xs:
                w = PowerWall(x, n, units)
                fs = (w.U, w.dU, w.V2, w.V3, w.V4)
                for c in (0.5, 1.0, 3.0, 30.0):
                    p = c * w.phi_t
                    h = 1e-4 * p
                    for j in range(4):
                        num = (fs[j](p + h) - fs[j](p - h)) / (2 * h)
                        rel = abs(num / fs[j + 1](p) - 1.0)
                        worst = max(worst, rel)
                chk = (w.V2(w.phi_t), w.U(w.phi_t) / w.E, w.U(w.phi_t) / w.V0 / x)
                out.append(
                    f"D units={units:5s} n={n} x={x:.0e}: V''(phi_t) = {chk[0]:.15f}, U(phi_t)/E = {chk[1]:.15f}, "
                    f"U(phi_t)/(V0 x) = {chk[2]:.15f}"
                )
    out.append(
        f"D worst relative difference over every derivative, unit, n, x and phi: {worst:.2e}"
    )
    return "\n".join(out)


# ---- U -------------------------------------------------------------------------------------------------------------
def task_U_S(arg):
    n, x = arg
    t0 = time.time()
    s1 = S_testfield(PowerWall(x, n, "phi_t"))[0]
    s2 = S_testfield(PowerWall(x, n, "M"))[0]
    return (
        f"U S   n={n} x={x:.0e}: phi_t units {s1:.8e}  M units {s2:.8e}  rel {s2 / s1 - 1:+.1e}  "
        f"(tolerance 1e-6: DOP853 at rtol 1e-11 in either unit)  ({time.time() - t0:.0f} s)"
    )


def task_U_ell(arg):
    n, x, lnR = arg
    a = born.chain(PowerWall(x, n, "phi_t"), exp(lnR))
    b = born.chain(PowerWall(x, n, "M"), exp(lnR))
    if a is None or b is None:
        return f"U ell n={n} x={x:.0e} lnR={lnR}: phi_t units {a and a[3]}  M units {b and b[3]}  (no chain)"
    return (
        f"U ell n={n} x={x:.0e} lnR={lnR}: phi_t units {a[3]:.12f}  M units {b[3]:.12f}  diff {b[3] - a[3]:+.1e}  "
        f"phi_ta/phi_t {a[0]:.6f} vs {b[0] / PowerWall(x, n, 'M').phi_t:.6f}  (tolerance 1e-10: the chain's "
        f"1e-12 on ell and rtol 1e-14 on phi_ta)"
    )


def task_U_frac(arg):
    n, x, lnR = arg
    t0 = time.time()
    ra = stage2.run_point(None, lnR, wall=PowerWall(x, n, "phi_t"))
    rb = stage2.run_point(None, lnR, wall=PowerWall(x, n, "M"))
    return (
        f"U frac n={n} x={x:.0e} lnR={lnR}: phi_t units {ra['frac_modes']:.10f}  M units {rb['frac_modes']:.10f}  "
        f"diff {rb['frac_modes'] - ra['frac_modes']:+.1e}  steps {ra['steps']} vs {rb['steps']}  |dE/E| "
        f"{ra['drift']:.1e} {rb['drift']:.1e}  (tolerance 1e-8: the same fixed-step RK4 in rescaled variables, "
        f"which differ by rounding)  ({time.time() - t0:.0f} s)"
    )


# ---- V1 ------------------------------------------------------------------------------------------------------------
def task_V1(arg):
    n, x, units = arg
    t0 = time.time()
    s_ind = S_testfield(PowerWall(x, n, units), npts=481)[0]
    s2, r = S_stage2(PowerWall(x, n, "phi_t"))
    return (
        f"V1 n={n} x={x:.0e}: S stage 2 (loop off, phi_t units) {s2:.6e}  walls.py ({units} units, 481 points) "
        f"{s_ind:.6e}  ratio {s2 / s_ind:.6f}  v_out/v_in {r['vout']:.6f}  (tolerance 1e-4: crossover's V-A "
        f"agreement, 1e-5, and the two quadratures)  ({time.time() - t0:.0f} s)"
    )


def task_V1R(arg):
    n, lnR = arg
    r = ps.run_point_power(1e40, n, lnR)
    return (
        f"V1R n={n} lnR={lnR:5.1f} loop on: frac/R {r['frac_modes'] / exp(lnR):.5e}  {row(r)}  "
        f"phi_turn/phi_t {r['phi_turn']:.4f}"
    )


# ---- V2 ------------------------------------------------------------------------------------------------------------
def task_V2(arg):
    n, ell_t = arg
    t0 = time.time()
    lnR = lnR_at(n, ell_t)
    w = PowerWall(1e40, n)
    R = exp(lnR)
    ch = born.chain(w, R)
    k10 = 10.0 * ch[1]
    rb = born.run(w, R, k10)
    rs = stage2.solve(w.L, lnR, kmin=k10, kmax=30 * ch[2], mref=ch[1], wall=w)
    return (
        f"V2 n={n} ell={ch[3]:.4f} lnR={lnR:.4f} k_IR = 10 m_ta: born {rb['Efl_final']:.6f}  stage2-cut "
        f"{rs['frac_modes']:.6f}  ratio {rs['frac_modes'] / rb['Efl_final']:.5f}  |dE/E| {rs['drift']:.1e}  "
        f"(tolerance 2e-3: crossover's V1, 4e-4 to 1.0e-3, the Born reduction's own error)  "
        f"({time.time() - t0:.0f} s)"
    )


def task_V2sc(arg):
    n, ell_t = arg
    t0 = time.time()
    lnR = lnR_at(n, ell_t)
    w = PowerWall(1e40, n)
    rb = born.run_self_consistent(w, exp(lnR))
    rp = ps.run_point_power(1e40, n, lnR)
    return (
        f"V2sc n={n} ell={rb['chain']['ell']:.4f} lnR={lnR:.4f}: born-sc {rb['Efl_final']:.5f} "
        f"(back {rb['returned']}, iterations {rb['iterations']})  stage 2 physical {rp['frac_modes']:.5f}  "
        f"ratio {rp['frac_modes'] / rb['Efl_final']:.4f}  2.7/ell {2.7 / rb['chain']['ell']:.4f}  "
        f"|dE/E| {rp['drift']:.1e}  ({time.time() - t0:.0f} s)"
    )


# ---- V4 ------------------------------------------------------------------------------------------------------------
VARIANTS = (
    ("ppd x2", dict(ppd=160)),
    ("kmax x2", dict(cmax=60.0)),
    ("kmin /2", dict(cmin=0.015)),
    ("h /2", dict(h=0.05)),
    ("EPS /10", None),
)


def task_V4(arg):
    name, n, x, lnR = arg
    t0 = time.time()
    base = ps.run_point_power(x, n, lnR)
    mref, kref = base["mref"], base["kref"]
    ell = base["chain"]["ell"] if base["chain"] else None
    out = [
        f"V4 {name:14s} n={n} x={x:.0e} lnR={lnR:.3f} ell={ell} ell_inf={base['ell_inf']} passes={base['passes']} "
        f"eps_turn={base['eps_turn']:.2e}",
        f"V4 {name:14s} base     {row(base)}",
    ]
    eps0 = stage2.EPS * (
        exp(-2.0 * (base["ell_inf"] - 8.0))
        if base["ell_inf"] and base["ell_inf"] > 8
        else 1.0
    )
    worst = 0.0
    for vname, kw in VARIANTS:
        kw = dict(eps=eps0 / 10.0) if kw is None else dict(kw, eps=eps0)
        cmin = kw.pop("cmin", 0.03)
        cmax = kw.pop("cmax", 30.0)
        ppd = kw.pop("ppd", 80)
        r = stage2.solve(
            PowerWall(x, n).L,
            lnR,
            kmin=cmin * mref,
            kmax=cmax * kref,
            ppd=ppd,
            mref=mref,
            wall=PowerWall(x, n),
            **kw,
        )
        d = r["frac_modes"] - base["frac_modes"]
        worst = max(worst, abs(d))
        out.append(f"V4 {name:14s} {vname:8s} {row(r)}  d(frac) {d:+.1e}")
    out.append(
        f"V4 {name:14s} largest |d(frac)| {worst:.1e}  ({time.time() - t0:.0f} s)"
    )
    return "\n".join(out)


# ---- V6 ------------------------------------------------------------------------------------------------------------
def task_V6_S(_):
    out = []
    for n in (1, 2):
        vals = [S_testfield(PowerWall(x, n))[0] for x in (1e10, 1e20, 1e40, 1e100)]
        out.append(
            f"V6 S n={n} at x = 1e10, 1e20, 1e40, 1e100 (phi_t units): "
            + ", ".join(f"{v:.10e}" for v in vals)
            + f"  spread {max(vals) / min(vals) - 1:.1e}"
        )
    return "\n".join(out)


def task_V6_ell(_):
    out = [
        "# V6 ell: ell(x) - ell_inf at fixed ln R; 'floor' is the least x (on a grid of 0.25 decades) above which "
        "|ell(x) - ell_inf| <= 1e-3, beside (phi_ta/phi_t)^n there (the floor matters once it approaches x)"
    ]
    for n in (1, 2):
        for lnR in (20.0, 40.0, 100.0, 300.0) if n == 1 else (15.0, 25.0, 60.0, 200.0):
            ch = born.chain(PowerWall(INF, n), exp(lnR))
            e_inf = ch[3]
            vals = {x: ps.chain_ell(x, n, lnR) for x in (1e10, 1e20, 1e40, 1e100)}
            floor = None
            for lx in np.arange(0.0, 120.0, 0.25):
                e = ps.chain_ell(10.0**lx, n, lnR)
                if e is not None and abs(e - e_inf) <= 1e-3:
                    floor = lx
                    break
            out.append(
                f"V6 ell n={n} lnR={lnR:6.1f}: ell_inf {e_inf:.6f}; at x = 1e10/1e20/1e40/1e100: "
                + "/".join("none" if v is None else f"{v:.6f}" for v in vals.values())
                + f"; |ell - ell_inf| <= 1e-3 from x = 1e{floor:.2f}, (phi_ta/phi_t)^n = {ch[0] ** n:.3e}"
            )
    return "\n".join(out)


def task_V6_frac(arg):
    n, lnR, x = arg
    t0 = time.time()
    r = stage2.run_point(None, lnR, wall=PowerWall(x, n))
    ell = r["chain"]["ell"] if r["chain"] else None
    return (
        f"V6 frac n={n} lnR={lnR:5.1f} x={x:.0e}: run_point (refs from the chain on PowerWall(x)) "
        f"{r['frac_modes']:.6f}  bal {r['frac_balance']:.6f}  ell(x) {ell}  mref {r['mref']:.3e} kref "
        f"{r['kref']:.3e} passes {r['passes']}  |dE/E| {r['drift']:.1e}  ({time.time() - t0:.0f} s)"
    )


def task_V6_free(arg):
    n, lnR = arg
    r = ps.run_point_power(1e100, n, lnR)
    return (
        f"V6 frac n={n} lnR={lnR:5.1f} run_point_power (floor-free references; the same at every x): "
        f"{r['frac_modes']:.6f}  mref {r['mref']:.3e} kref {r['kref']:.3e} passes {r['passes']}"
    )


def task(job):
    kind, arg = job
    return globals()["task_" + kind](arg)


if __name__ == "__main__":
    t0 = time.time()
    print(f"# validate_wall.py; start: {os.popen('uptime').read().strip()}")
    print(
        "# stage-2 rows: frac = 8 pi^2 R rho (the modes' energy), bal = (E - phi'^2/2 - U)/E (the background's "
        "balance), v_out/v_in, back = the field came back out, |dE/E| = max |E_tot - E|/E along the run (V3)"
    )
    jobs = [("D", None)]
    jobs += [("U_S", (n, x)) for n in (1, 2) for x in (10.0, 1e3)]
    jobs += [
        ("U_ell", (1, 1e3, 40.0)),
        ("U_ell", (1, 1e2, 29.5)),
        ("U_ell", (1, 10.0, 20.0)),
        ("U_ell", (2, 1e3, 28.5)),
        ("U_ell", (2, 1e2, 20.0)),
        ("U_ell", (2, 10.0, 15.0)),
    ]
    jobs += [
        ("U_frac", (1, 10.0, 10.0)),
        ("U_frac", (1, 1e3, 40.0)),
        ("U_frac", (2, 10.0, 8.0)),
        ("U_frac", (2, 1e3, 25.0)),
    ]
    jobs += [
        ("V1", (n, x, u))
        for n in (1, 2)
        for x, u in ((1e40, "phi_t"), (1.0, "M"), (0.1, "M"))
    ]
    jobs += [("V1R", (n, r)) for n in (1, 2) for r in (-8.0, -6.0, -4.0, -2.0, 0.0)]
    jobs += [("V2", (n, e)) for n in (1, 2) for e in (5.0, 8.0)]
    jobs += [("V2sc", (n, e)) for n in (1, 2) for e in (5.0, 8.0, 15.0, 25.0, 40.0)]
    jobs += [
        ("V4", p)
        for p in (
            ("one-loop l~24", 1, 1e40, 310.6441),
            ("one-loop l~1", 1, 1e40, 25.8629),
            ("gap", 1, 1e40, 10.0),
            ("one-loop l~24", 2, 1e40, 199.0),
            ("one-loop l~1", 2, 1e40, 16.5533),
            ("gap", 2, 1e40, 8.0),
            ("gap low", 1, 1e40, 3.2542),
            ("gap low", 2, 1e40, 3.6376),
        )
    ]
    jobs += [("V6_S", None), ("V6_ell", None)]
    jobs += [
        ("V6_frac", (n, lnR, x))
        for n, lnR in ((1, 40.0), (2, 25.0))
        for x in (1e100, 1e40, 1e10, 1e4, 1e3, 1e2)
    ]
    jobs += [("V6_free", (1, 40.0)), ("V6_free", (2, 25.0))]
    with Pool(6) as pool:
        for line in pool.imap(task, jobs):
            print(line, flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
