"""
power-law-wall prompt 02 §1.8 (U5): the test field's |beta_k|^2 on PowerWall at n = 1 and 2, at a large x (1e40, in
units of phi_t) and at two x near the floor on the laboratory curve (ln R = 10, in the gap, and R = 1; in walls.py's
units of M, where they are well scaled), in units of the production's k_ex = lambda_t sqrt(E)/2 at phi_t:

    k/k_ex = (k/omega_t) / (k_ex/omega_t),   k_ex/omega_t = (s_t/2) sqrt(n/(n + 1)),   s_t = x/(1 + x),

computed here as stage2.lam(w, phi_t) sqrt(E)/2 with omega_t = 1. For each:
  - x_p, the peak in k/k_ex: on the grid (300 points, log-spaced in k/k_ex on [1e-2, 12]), and by a fit (a parabola
    in ln k through the seven grid points around the largest |beta|^2, ln |beta|^2 against ln k);
  - the slope below the peak, d ln |beta|^2 / d ln k fitted on k/k_ex in [0.02, 0.1] (where the exponential's note
    quotes "fitted slope 1.99");
  - kappa, twice: by quanta_spectrum.kappa_fit's two-point rule, ln(|beta|^2_max / |beta|^2(x = 10))/(10 - x_p), with
    |beta|^2 at k = 10 k_ex measured (not assumed 1e-20) and x_p this wall's; and by a straight-line fit of
    ln |beta|^2 against k/k_ex on [3, 10].
The exponential's values, by the same code on WallN(90, 1) (walls.py's units; L = 90 is quanta_spectrum's KAPPA_FIT_L),
in units of its exact k_ex and of omega_t/2 (the module's convention, x_p = 0.62 = 0.31/0.5), beside x_p = 0.62 and
KAPPA = ln(1.43e-4/1e-20)/(10 - 0.62). |beta|^2 at k = 10 k_ex is measured at walls.py's EPS and tolerances and again
at EPS/1000 with rtol 1e-13, atol 1e-17, since on the exponential it is near 1e-20.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python spectrum.py \\
        > ../../../../.documents/power-law-wall/wall-02/spectrum.txt
"""

import os
import sys
import time
from math import exp, log, sqrt
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import powerscan as ps  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import PowerWall, WallN  # noqa: E402

KAPPA_MODULE = log(1.43e-4 / 1e-20) / (10.0 - 0.62)


def b2_at(w, ks, eps=walls.EPS):
    starts = [w.start_phi(k, eps) for k in ks]
    b = walls.Bounce(w, max(starts))
    return np.array(
        [walls.beta_squared(b, w.V2, w.V3, k, p) for k, p in zip(ks, starts)]
    )


def b2_strict(w, k):
    """|beta|^2 at one k with EPS/1000 and walls.py's tolerances tightened (module globals, this process only)."""
    r0, a0 = walls.RTOL, walls.ATOL
    walls.RTOL, walls.ATOL = 1e-13, 1e-17
    try:
        return float(b2_at(w, np.array([k]), eps=walls.EPS / 1000.0)[0])
    finally:
        walls.RTOL, walls.ATOL = r0, a0


def measure(w, kex, label):
    t0 = time.time()
    xs = np.geomspace(1e-2, 12.0, 300)
    b2 = b2_at(w, xs * kex)
    i = int(np.argmax(b2))
    sl = slice(max(i - 3, 0), i + 4)
    c = np.polyfit(np.log(xs[sl]), np.log(b2[sl]), 2)
    xp_fit = exp(-c[1] / (2.0 * c[0]))
    lo = (xs >= 0.02) & (xs <= 0.1)
    slope = np.polyfit(np.log(xs[lo]), np.log(b2[lo]), 1)[0]
    b2_10 = float(b2_at(w, np.array([10.0 * kex]))[0])
    b2_10s = b2_strict(w, 10.0 * kex)
    xp = xs[i]
    kappa2 = log(b2[i] / b2_10) / (10.0 - xp)
    kappa2s = log(b2[i] / b2_10s) / (10.0 - xp)
    tail = (xs >= 3.0) & (xs <= 10.0)
    kappa_fit = -np.polyfit(xs[tail], np.log(b2[tail]), 1)[0]
    return (
        f"{label}: k_ex/omega_t {kex:.5f}; |beta|^2_max {b2[i]:.5e}; x_p grid {xp:.4f}, fit {xp_fit:.4f} "
        f"(k_peak/omega_t {xp_fit * kex:.4f}); slope below the peak {slope:.3f}; |beta|^2(x = 10) {b2_10:.4e} "
        f"(strict {b2_10s:.4e}); kappa two-point {kappa2:.4f} (strict {kappa2s:.4f}); kappa tail fit [3, 10] "
        f"{kappa_fit:.4f}  ({time.time() - t0:.0f} s)"
    )


def task(job):
    kind = job[0]
    if kind == "power":
        _, n, x, units, where = job
        w = PowerWall(x, n, units)
        kex = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0 / sqrt(w.V2(w.phi_t))
        s_t = x / (1.0 + x)
        return measure(
            w,
            kex,
            f"power n={n} x={x:.4g} ({where}, {units} units; s_t {s_t:.4f}, (s_t/2)sqrt(n/(n+1)) "
            f"{0.5 * s_t * sqrt(n / (n + 1)):.5f})",
        )
    w = WallN(90.0, 1)
    if job[1] == "exact":
        kex = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0
        return measure(w, kex, "exponential WallN(90, 1), k_ex exact")
    return measure(w, 0.5, "exponential WallN(90, 1), k_ex = omega_t/2")


if __name__ == "__main__":
    t0 = time.time()
    print(f"# spectrum.py; start: {os.popen('uptime').read().strip()}")
    print(
        f"# the module's constants: X_PEAK = 0.62, KAPPA = ln(1.43e-4/1e-20)/(10 - 0.62) = {KAPPA_MODULE:.4f}"
    )
    jobs = []
    for n in (1, 2):
        a = ps.curve_a(ps.CURVES["4e-31"], n)
        x10 = exp((10.0 - a) / (1.0 + 4.0 / n))
        x1 = exp((0.0 - a) / (1.0 + 4.0 / n))
        jobs += [
            ("power", n, 1e40, "phi_t", "large x"),
            ("power", n, x10, "M", "laboratory curve, ln R = 10"),
            ("power", n, x1, "M", "laboratory curve, R = 1"),
        ]
    jobs += [("exp", "exact"), ("exp", "half")]
    with Pool(6) as pool:
        for line in pool.imap(task, jobs):
            print(line, flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
