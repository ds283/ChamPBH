# power-law-wall prompt 07b (P74): a copy of prompts/power-law-wall/source/wall/stage1a_power.py as it stands at c798d02. Below
# this header the file is that one, byte for byte. It is imported from this directory by the scripts of source/kex/,
# which put it alone on sys.path; it is a record and is not edited.
"""
power-law-wall prompt 02 §1.5 (U4): S_n(x) = 4 int (k/omega_t)^3 |beta_k|^2 dk/omega_t, the classical production
energy ratio of a massless-out test field on PowerWall, at n = 1 and 2, as stage1a_classical.py computes S(L) on the
exponential wall (walls.Bounce and walls.beta_squared, unchanged).

- The table. S_n at x = 1e-3 ... 1e3 in walls.py's units (M = 1, phi_t = x^{-1/n}, well scaled there), and at
  x = 1e10 in units of phi_t. In units of phi_t the test field reads U'' and U''' only, which do not depend on x
  (walls.PowerWall), so S_n(x) is one number per n wherever it is computed; the runs in units of M at each x are the
  independent check that it is. Beside each: |beta|^2_max and its k/omega_t, and the exponential's S(L_E) at
  L_E = ln(1 + x) (drain_S_of_L's table), which the adopted law would read at L_E.
- The quadrature (as crossover's V2q checked stage 1a's): at x = 10, on [1e-3, 10] with 121, 241 and 481 log-spaced
  points, the linear-k and the ln-k trapezoid, and on [1e-4, 20] with 601 points; and stage 2 with the loop off
  (80 and 160 modes a decade). The table's S is the 241-point ln-k trapezoid on [1e-3, 10].

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python stage1a_power.py \\
        > ../../../../.documents/power-law-wall/wall-02/stage1a_S_n.txt
"""

import os
import sys
import time
from math import log1p, sqrt
from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import powerscan as ps  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import PowerWall  # noqa: E402

XS = (1e-3, 1e-2, 0.1, 0.3, 1.0, 3.0, 10.0, 100.0, 1e3)


def spectrum(w, ks, eps=walls.EPS):
    starts = [w.start_phi(k, eps) for k in ks]
    b = walls.Bounce(w, max(starts))
    return np.array(
        [walls.beta_squared(b, w.V2, w.V3, k, p) for k, p in zip(ks, starts)]
    )


def S_lnk(ks, b2):
    return 4.0 * np.trapezoid(ks**4 * b2, np.log(ks))


def S_lin(ks, b2):
    return 4.0 * np.trapezoid(ks**3 * b2, ks)


def table(arg):
    n, x, units = arg
    t0 = time.time()
    w = PowerWall(x, n, units)
    ks = np.geomspace(1e-3, 10.0, 241)
    b2 = spectrum(w, ks)
    i = int(np.argmax(b2))
    S_exp = ps.S_of_table(log1p(x))
    return (
        f"S n={n} x={x:8.1e} ({units:5s} units): S_n = {S_lnk(ks, b2):.7e}  |beta|^2_max {b2[i]:.5e} at "
        f"k/omega_t {ks[i]:.4f}  exponential S(L_E = {log1p(x):.4g}) {S_exp:.4e}  ratio S_n/S(L_E) "
        f"{S_lnk(ks, b2) / S_exp:.3f}  ({time.time() - t0:.0f} s)"
    )


def quad(n):
    t0 = time.time()
    w = PowerWall(10.0, n)
    out = []
    for npts in (121, 241, 481):
        ks = np.geomspace(1e-3, 10.0, npts)
        b2 = spectrum(w, ks)
        out.append(
            f"Q n={n} [1e-3, 10] {npts:3d} points: ln-k trapezoid {S_lnk(ks, b2):.7e}  linear-k trapezoid "
            f"{S_lin(ks, b2):.7e}"
        )
    ks = np.geomspace(1e-4, 20.0, 601)
    b2 = spectrum(w, ks)
    out.append(
        f"Q n={n} [1e-4, 20] 601 points: ln-k trapezoid {S_lnk(ks, b2):.7e}  linear-k trapezoid {S_lin(ks, b2):.7e}"
    )
    ks = np.geomspace(1e-3, 10.0, 121)
    b2 = spectrum(w, ks, eps=walls.EPS / 100.0)
    out.append(
        f"Q n={n} [1e-3, 10] 121 points at EPS/100: ln-k trapezoid {S_lnk(ks, b2):.7e}"
    )
    for ppd in (80, 160):
        kref = n * sqrt(w.E) / 2.0
        r = stage2.solve(
            w.L,
            0.0,
            kmin=1e-3,
            kmax=30 * kref,
            ppd=ppd,
            mref=1.0,
            loop=False,
            wall=w,
        )
        out.append(
            f"Q n={n} stage 2, loop off, {ppd} modes a decade on [1e-3, {30 * kref:.2f}]: S = {r['frac_modes']:.7e}"
        )
    out[-1] += f"  ({time.time() - t0:.0f} s)"
    return "\n".join(out)


def task(job):
    kind, arg = job
    return table(arg) if kind == "S" else quad(arg)


if __name__ == "__main__":
    t0 = time.time()
    print(f"# stage1a_power.py; start: {os.popen('uptime').read().strip()}")
    print(
        f"# EPS = {walls.EPS}, rtol = {walls.RTOL}, atol = {walls.ATOL}; S_n by the 241-point ln-k trapezoid on "
        "[1e-3, 10] omega_t"
    )
    jobs = [("S", (n, x, "M")) for n in (1, 2) for x in XS]
    jobs += [("S", (n, 1e10, "phi_t")) for n in (1, 2)]
    jobs += [("Q", n) for n in (1, 2)]
    with Pool(6) as pool:
        for line in pool.imap(task, jobs):
            print(line, flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
