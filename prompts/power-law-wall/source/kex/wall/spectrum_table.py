# power-law-wall prompt 07b (P74): a copy of prompts/power-law-wall/source/wall/spectrum_table.py as it stands at c798d02. Below
# this header the file is that one, byte for byte. It is imported from this directory by the scripts of source/kex/,
# which put it alone on sys.path; it is a record and is not edited.
"""
power-law-wall prompt 03 §1.3 (R8, R9, R10; P19-P21): the test field's |beta_k|^2 tabulated on three walls, for the
direct piece of ComputeTargets/quanta_spectrum.py, and the measurements beside the tables. 02's walls.Bounce and
walls.beta_squared, unchanged, through stage1a_power.spectrum.

The walls (in walls.py's units, omega_t = V''(phi_t)^{1/2} = 1):
  - "exponential": WallN(90, 1) (P21; L = 90 is the reference L of the module before this prompt, KAPPA_FIT_L);
  - "power-n1", "power-n2": PowerWall(1e40, n) in units of phi_t, the scale-free wall (R9, U8 (a)): s_t = E/(V0 + E)
    is 1 to double precision there.
x = k/k_ex, with k_ex the production's, lambda_t sqrt(E)/2 at phi_t (lambda = -d ln V/dphi on V0 + U, stage2.lam), in
units of omega_t: 0.494540 on the exponential at L = 90 (not omega_t/2, the convention of U4-U17-v1, whose x_p = 0.62 was
k_peak/(omega_t/2)), and (s_t/2) sqrt(n/(n + 1)) = 0.353553 (n = 1) and 0.408248 (n = 2) on the power wall.

**The precision.** walls.py's tolerances (rtol 1e-11, atol 1e-14, EPS 1e-5) leave |beta| with an absolute error of
order 1e-7 (02's spectrum.txt: at n = 2, x = 10, |beta|^2 = 1.52e-12 against 1.12e-12 at the strict setting), which is a
relative error of 1e-3 in |beta|^2 near 1e-8, where the tables end. The tables are therefore computed at the strict
setting of 02's spectrum.py (rtol 1e-13, atol 1e-17, EPS 1e-8; module globals of walls.py, set in each worker);
mode `lowk` measures both settings.

Modes (each prints to stdout; the outputs are .documents/power-law-wall/law-03/spectrum_tables.txt, in this order):
  lowk     the local power d ln|beta|^2 / d ln k at k/omega_t = 1e-2, 1e-3, 3e-4, 1e-4, 3e-5, 1e-5 on each wall, at
           walls.py's setting and at the strict one, by a centred difference over k (1 +- 0.02): R8's k^2 below the
           table, and the solver's precision floor, where the power stops tending to 2;
  energy   |beta|^2 (strict) on x from the table's lower edge to 16, the energy share of x^3 s above x = 3 ... 8 by the
           measured data (trapezoid in ln x), to choose x_hi: the smallest of 3, 3.5, 4, ... with share <= 1e-3 (R8's
           "well under 1%", ruling 5);
  table    the tables: ln s on a uniform grid in t = ln x from x_lo (k/omega_t = 1e-3) to x_hi, step H_T; the
           interpolant (a not-a-knot cubic spline in (t, ln s)) against direct evaluations at every midpoint; its
           monotonicity on either side of the peak; x_p (the interpolant's maximum; a parabola through the seven
           nodes around the largest beside it, as 02's spectrum.py fitted); kappa_hi = -d ln s/dx at x_hi from the
           spline's end derivative (and a centred difference of direct evaluations at x_hi (1 +- 1e-3) beside it);
           the tail's energy share under the model; the literals for quanta_spectrum.py;
  shape    the exponential's shape over L (40, 70, 90, 110, 134) and the power wall's over E/V0 (1e40, 1e3, 10, 1 and the
           laboratory curve's R = 1 point; R9), each table built as `table` builds it but on a coarser grid, with x_p,
           J = M2/M3, M1/M3 and kappa_hi under the model's three regions at c_a = 0, against the reference table (from
           ComputeTargets.quanta_spectrum); on the power wall also the table's error in x: the measured spectrum at
           E/V0 against the reference table at x and at x s_t;
  oldnew   on the exponential, I, J, M1/M3, ln s(x_p), G(y) at y = 1e-2, 1, 10 and Omega(u) at u = 0.5, 1, 2 under
           U4-U17-v1 (the module at the base, git show 7c60226:ComputeTargets/quanta_spectrum.py, written to a scratch
           path given as the second argument) and under the new table, at c_a = 1 and 0.

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python spectrum_table.py MODE [OLD_MODULE] \\
        >> ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt

`shape` and `oldnew` import ComputeTargets.quanta_spectrum, so they run with PYTHONPATH set to the repository root. At most
six worker processes (orchestrator note 03, ruling 9).
"""

import os
import sys
import time
from math import exp, log, sqrt
from multiprocessing import Pool

import numpy as np
from scipy.interpolate import CubicSpline

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import powerscan as ps  # noqa: E402
import stage1a_power as s1  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import PowerWall, WallN  # noqa: E402

WORKERS = 6
STRICT = dict(rtol=1e-13, atol=1e-17, eps=1e-8)
DEFAULT = dict(rtol=walls.RTOL, atol=walls.ATOL, eps=walls.EPS)
K_LO_OVER_OMEGA = 1.0e-3  # R8: the table's lower edge in k/omega_t
H_T = 0.05  # the table's step in t = ln x
H_T_SHAPE = 0.05  # the shape measurements' step
SHARE_TARGET = 1.0e-3  # ruling 5
X_HI_CANDIDATES = tuple(3.0 + 0.5 * j for j in range(15))
WALL_NAMES = ("exponential", "power-n1", "power-n2")


def make_wall(name: str, param=None, units=None):
    """(wall, k_ex/omega_t, omega_t) for a wall name; param is L for the exponential, x = E/V0 for the power wall."""
    if name == "exponential":
        w = WallN(90.0 if param is None else float(param), 1)
        omega = 1.0
    else:
        n = int(name[-1])
        x = 1e40 if param is None else float(param)
        w = PowerWall(x, n, units or ("phi_t" if x > 1e3 else "M"))
        omega = sqrt(float(w.V2(w.phi_t)))
    kex = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0 / omega
    return w, kex, omega


def b2(job):
    """|beta|^2 at k = k_over_omega * omega_t on a wall, at a tolerance setting (set in this worker)."""
    name, param, units, setting, ks_over_omega = job
    walls.RTOL, walls.ATOL = setting["rtol"], setting["atol"]
    w, _, omega = make_wall(name, param, units)
    ks = np.asarray(ks_over_omega, dtype=float) * omega
    return s1.spectrum(w, ks, eps=setting["eps"])


def b2_many(pool, name, param, units, setting, ks_over_omega, chunk=12):
    """|beta|^2 at many k, split into chunks over the pool (each chunk one bounce, from its own largest start)."""
    ks = np.asarray(ks_over_omega, dtype=float)
    parts = [ks[i : i + chunk] for i in range(0, len(ks), chunk)]
    out = pool.map(b2, [(name, param, units, setting, p) for p in parts])
    return np.concatenate(out)


def uptime() -> str:
    return os.popen("uptime").read().strip()


# ---- lowk ------------------------------------------------------------------------------------------------------------


def mode_lowk(pool):
    print(f"\n## lowk: the local power d ln|beta|^2/d ln k; start: {uptime()}")
    picks = (1e-2, 1e-3, 3e-4, 1e-4, 3e-5, 1e-5)
    for name in WALL_NAMES:
        for label, setting in (("walls.py's", DEFAULT), ("strict", STRICT)):
            t0 = time.time()
            ks = np.array([[p / 1.02, p * 1.02] for p in picks]).ravel()
            b = np.concatenate(
                pool.map(b2, [(name, None, None, setting, [k]) for k in ks])
            )
            pw = [
                (log(b[2 * i + 1]) - log(b[2 * i])) / (2.0 * log(1.02))
                for i in range(len(picks))
            ]
            print(
                f"{name:12s} {label:10s} (rtol {setting['rtol']:.0e}, atol {setting['atol']:.0e}, EPS {setting['eps']:.0e}): "
                + "  ".join(f"{p:g}: {v:.4f}" for p, v in zip(picks, pw))
                + f"   ({time.time() - t0:.0f} s)",
                flush=True,
            )
    print(f"# lowk end: {uptime()}")


# ---- energy ----------------------------------------------------------------------------------------------------------


def mode_energy(pool):
    print(
        f"\n## energy: where x^3 |beta|^2 sits, from strict |beta|^2 on x in [x_lo, 16]; start: {uptime()}"
    )
    for name in WALL_NAMES:
        t0 = time.time()
        _, kex, _ = make_wall(name)
        x_lo = K_LO_OVER_OMEGA / kex
        xs = np.exp(np.arange(log(x_lo), log(16.0) + 1e-12, 0.05))
        b = b2_many(pool, name, None, None, STRICT, xs * kex)
        e = xs**4 * b  # x^3 s dx = x^4 s dt
        total = np.trapezoid(e, np.log(xs))
        line = []
        chosen = None
        for xc in X_HI_CANDIDATES:
            m = xs >= xc
            share = np.trapezoid(e[m], np.log(xs[m])) / total
            line.append(f"{xc:g}: {share:.2e}")
            if chosen is None and share <= SHARE_TARGET:
                chosen = xc
        print(
            f"{name:12s} k_ex/omega_t {kex:.6f}, x_lo {x_lo:.6g}; energy share above x = "
            + ", ".join(line)
            + f"; x_hi = {chosen:g}  ({time.time() - t0:.0f} s)",
            flush=True,
        )
    print(f"# energy end: {uptime()}")


# ---- table -----------------------------------------------------------------------------------------------------------

# x_hi: the smallest of X_HI_CANDIDATES whose measured energy share is at most SHARE_TARGET (`energy`, 2026-11-15)
X_HI = {"exponential": 3.5, "power-n1": 6.5, "power-n2": 5.0}


def build(pool, name, param=None, units=None, x_hi=None, h=H_T, x_scale=1.0):
    """
    The table on one wall: t = ln x on a uniform grid of step h from x_lo to x_hi (x_hi rounded onto the grid so that
    the last node is x_hi exactly), ln s = ln |beta|^2 there (strict), and at the midpoints. x_scale multiplies k_ex
    (for the power wall's shape at E/V0, measured in its own x).
    """
    _, kex, _ = make_wall(name, param, units)
    kex *= x_scale
    x_lo = K_LO_OVER_OMEGA / kex
    t_lo, t_hi = log(x_lo), log(x_hi)
    m = int(np.ceil((t_hi - t_lo) / h))
    t = np.linspace(t_lo, t_hi, m + 1)
    tm = 0.5 * (t[1:] + t[:-1])
    xs = np.exp(np.concatenate([t, tm]))
    b = b2_many(pool, name, param, units, STRICT, xs * kex)
    return dict(
        name=name,
        kex=kex,
        t=t,
        ls=np.log(b[: m + 1]),
        tm=tm,
        lsm=np.log(b[m + 1 :]),
        param=param,
    )


def analyse(tb):
    t, ls = tb["t"], tb["ls"]
    sp = CubicSpline(t, ls)
    mid_err = np.abs(np.exp(sp(tb["tm"]) - tb["lsm"]) - 1.0)
    # the peak: the spline's maximum (a root of its derivative), and the parabola through seven nodes
    i = int(np.argmax(ls))
    roots = sp.derivative().roots(extrapolate=False)
    roots = roots[(roots > t[max(i - 2, 0)]) & (roots < t[min(i + 2, len(t) - 1)])]
    t_p = float(roots[np.argmax(sp(roots))])
    sl = slice(max(i - 3, 0), i + 4)
    c = np.polyfit(t[sl], ls[sl], 2)
    x_p_parabola = exp(-c[1] / (2.0 * c[0]))
    # monotonicity of the interpolant on either side of the peak, on a grid 40 times finer than the nodes
    tf = np.linspace(t[0], t[-1], 40 * (len(t) - 1) + 1)
    d = sp(tf, 1)
    rising_bad = int(np.sum(d[tf < t_p - 1e-9] <= 0.0))
    falling_bad = int(np.sum(d[tf > t_p + 1e-9] >= 0.0))
    x_hi = exp(t[-1])
    kappa_hi = -float(sp(t[-1], 1)) / x_hi
    return dict(
        spline=sp,
        mid_max=float(mid_err.max()),
        mid_rms=float(np.sqrt(np.mean(mid_err**2))),
        mid_where=float(exp(tb["tm"][int(np.argmax(mid_err))])),
        x_p=exp(t_p),
        x_p_parabola=x_p_parabola,
        rising_bad=rising_bad,
        falling_bad=falling_bad,
        kappa_hi=kappa_hi,
        x_hi=x_hi,
    )


def kappa_direct(pool, name, kex, x_hi):
    d = 1e-3
    b = np.concatenate(
        pool.map(
            b2,
            [
                (name, None, None, STRICT, [x_hi * (1 - d) * kex]),
                (name, None, None, STRICT, [x_hi * (1 + d) * kex]),
            ],
        )
    )
    return -(log(b[1]) - log(b[0])) / (2.0 * d * x_hi)


def model_moments(t, ls, kappa_hi, ks=(1, 2, 3, 4)):
    """M_k of the direct piece under the three regions (c x^2 below, the spline, the matched tail), by dense quadrature."""
    sp = CubicSpline(t, ls)
    x_lo, x_hi = exp(t[0]), exp(t[-1])
    c = exp(ls[0]) / x_lo**2
    s_hi = exp(ls[-1])
    tf = np.linspace(t[0], t[-1], 200 * (len(t) - 1) + 1)
    sf = np.exp(sp(tf))
    out = {}
    for k in ks:
        below = c * x_lo ** (k + 3) / (k + 3)
        table = np.trapezoid(np.exp((k + 1) * tf) * sf, tf)
        # int_{x_hi}^inf x^k e^{-kappa (x - x_hi)} dx
        tail = sum(
            __import__("math").comb(k, j)
            * x_hi ** (k - j)
            * __import__("math").factorial(j)
            / kappa_hi ** (j + 1)
            for j in range(k + 1)
        )
        out[k] = below + table + s_hi * tail
    return out


def literal_block(name, tb, an):
    lines = [f"# ---- {name}: paste into ComputeTargets/quanta_spectrum.py ----"]
    lines.append(
        f"#   k_ex/omega_t = {tb['kex']!r}; x_lo = {exp(tb['t'][0])!r}; x_hi = {an['x_hi']!r}"
    )
    lines.append(
        f"#   t_lo = {tb['t'][0]!r}, step {(tb['t'][-1] - tb['t'][0]) / (len(tb['t']) - 1)!r}, {len(tb['t'])} nodes"
    )
    lines.append(f"#   x_p = {an['x_p']!r}; kappa_hi = {an['kappa_hi']!r}")
    lines.append("LN_BETA2 = (")
    for v in tb["ls"]:
        lines.append(f"    {float(v)!r},")
    lines.append(")")
    return "\n".join(lines)


def mode_table(pool, x_hi_by_wall):
    print(f"\n## table: the three tables; start: {uptime()}")
    blocks = []
    for name in WALL_NAMES:
        t0 = time.time()
        # x_hi: from the measured data's choice (`energy`), raised in steps of 0.5 until the tail's share of the energy
        # under the model, s(x_hi) int_{x_hi} x^3 e^{-kappa_hi (x - x_hi)} dx / M3, is at most SHARE_TARGET
        tried = []
        x_try = x_hi_by_wall[name]
        while True:
            tb = build(pool, name, x_hi=x_try)
            an = analyse(tb)
            M = model_moments(tb["t"], tb["ls"], an["kappa_hi"])
            x_hi, k_hi = an["x_hi"], an["kappa_hi"]
            tail3 = exp(tb["ls"][-1]) * sum(
                __import__("math").comb(3, j)
                * x_hi ** (3 - j)
                * __import__("math").factorial(j)
                / k_hi ** (j + 1)
                for j in range(4)
            )
            tried.append(f"{x_try:g}: {tail3 / M[3]:.3e}")
            if tail3 / M[3] <= SHARE_TARGET:
                break
            x_try += 0.5
        print(
            f"{name:12s} the tail's share under the model at x_hi = " + ", ".join(tried)
        )
        kd = kappa_direct(pool, name, tb["kex"], an["x_hi"])
        x_lo = exp(tb["t"][0])
        below3 = exp(tb["ls"][0]) / x_lo**2 * x_lo**6 / 6.0
        print(
            f"{name:12s} k_ex/omega_t {tb['kex']:.6f}; edges x_lo {x_lo:.6g} (k/omega_t 1e-3), x_hi {x_hi:g}; "
            f"{len(tb['t'])} nodes, step {H_T} in ln x; |beta|^2 at x_lo {exp(tb['ls'][0]):.4e}, at x_hi "
            f"{exp(tb['ls'][-1]):.4e}, max {exp(tb['ls'].max()):.5e}",
            flush=True,
        )
        print(
            f"   interpolant (not-a-knot cubic spline in (ln x, ln s)) against {len(tb['tm'])} direct midpoints: "
            f"max rel {an['mid_max']:.2e} (at x = {an['mid_where']:.4g}), rms {an['mid_rms']:.2e}; derivative of "
            f"the wrong sign at {an['rising_bad']} points below the peak and {an['falling_bad']} above it (of "
            f"{40 * (len(tb['t']) - 1) + 1} on a 40x finer grid)"
        )
        print(
            f"   x_p {an['x_p']:.6f} (the interpolant's maximum; parabola through seven nodes {an['x_p_parabola']:.6f}); "
            f"k_peak/omega_t {an['x_p'] * tb['kex']:.6f}"
        )
        print(
            f"   kappa_hi {k_hi:.6f} (the spline's end derivative); centred difference of direct evaluations at "
            f"x_hi (1 +- 1e-3): {kd:.6f}"
        )
        print(
            f"   under the model at c_a = 0: M1/M3 {M[1] / M[3]:.6f}, J = M2/M3 {M[2] / M[3]:.6f}, M4/M3 "
            f"{M[4] / M[3]:.6f}; energy share above x_hi (the tail) {tail3 / M[3]:.3e}, below x_lo (c x^2) "
            f"{below3 / M[3]:.3e}  ({time.time() - t0:.0f} s)",
            flush=True,
        )
        blocks.append(literal_block(name, tb, an))
    print(f"# table end: {uptime()}")
    return blocks


# ---- shape -----------------------------------------------------------------------------------------------------------


def build_by_rule(pool, name, param=None, units=None, x_scale=1.0, start=3.0):
    """`table`'s x_hi rule from `start` (the model's tail share <= SHARE_TARGET), at the shape step H_T_SHAPE."""
    x_try = start
    while True:
        tb = build(pool, name, param, units, x_hi=x_try, h=H_T_SHAPE, x_scale=x_scale)
        an = analyse(tb)
        M = model_moments(tb["t"], tb["ls"], an["kappa_hi"])
        x_hi, k_hi = an["x_hi"], an["kappa_hi"]
        tail3 = exp(tb["ls"][-1]) * sum(
            __import__("math").comb(3, j)
            * x_hi ** (3 - j)
            * __import__("math").factorial(j)
            / k_hi ** (j + 1)
            for j in range(4)
        )
        if tail3 / M[3] <= SHARE_TARGET:
            return tb, an, M, tail3 / M[3]
        x_try += 0.5


def shape_line(label, tb, an, M, share, ref=None):
    line = (
        f"{label}: k_ex/omega_t {tb['kex']:.6f}, x_lo {exp(tb['t'][0]):.5g}, x_hi {an['x_hi']:g} (tail share "
        f"{share:.1e}), midpoints max {an['mid_max']:.1e}; x_p {an['x_p']:.5f}, J {M[2] / M[3]:.5f}, M1/M3 "
        f"{M[1] / M[3]:.5f}, kappa_hi {an['kappa_hi']:.4f}"
    )
    if ref is not None:
        line += (
            f"; against the reference: x_p x{an['x_p'] / ref['x_p']:.4f}, J x{(M[2] / M[3]) / ref['J']:.4f}, "
            f"M1/M3 x{(M[1] / M[3]) / ref['r1']:.4f}, kappa_hi x{an['kappa_hi'] / ref['kappa']:.4f}"
        )
    return line


def mode_shape(pool):
    from ComputeTargets import quanta_spectrum as qs

    print(
        f"\n## shape: each table built by `table`'s rule at step {H_T_SHAPE} in ln x; moments at c_a = 0 under the "
        f"three regions (trapezoid on a 200x finer grid); start: {uptime()}"
    )
    for wall in qs.WALLS:
        s = qs.spectrum(wall, 0.0)
        print(
            f"the module's {wall} (reference): x_p {s.x_p:.5f}, J {s.J:.5f}, M1/M3 {s.moment(1) / s.I:.5f}, kappa_hi "
            f"{s.kappa_hi:.4f}, x_hi {s.x_hi:g}"
        )
    s = qs.spectrum(qs.WALL_EXPONENTIAL, 0.0)
    ref = dict(x_p=s.x_p, J=s.J, r1=s.moment(1) / s.I, kappa=s.kappa_hi)
    print(
        "# the exponential's shape over L (WallN(L, 1), k_ex exact at each L), against L = 90's table"
    )
    for L in (40.0, 70.0, 90.0, 110.0, 134.0):
        t0 = time.time()
        tb, an, M, share = build_by_rule(pool, "exponential", L)
        print(
            shape_line(f"exponential L = {L:g}", tb, an, M, share, ref)
            + f"  ({time.time() - t0:.0f} s)",
            flush=True,
        )
    print(
        "# the power wall's shape over x = E/V0 (R9: measured, not acted on). In k/omega_t the bounce is the same at every "
        "x, and k_ex/omega_t = (s_t/2) sqrt(n/(n + 1)), s_t = x/(1 + x): the reference table (s_t = 1) read at x is off "
        "in x by the factor s_t. Each line: the spectrum measured at that x (walls.py's units of M below 1e40), in its "
        "own x = k/k_ex, by `table`'s rule; then the measured ln|beta|^2 at the table's nodes against the reference "
        "table read at x s_t (the scale invariance) and at x (the table's error)"
    )
    for wall in (qs.WALL_POWER_N1, qs.WALL_POWER_N2):
        n = int(wall[-1])
        s = qs.spectrum(wall, 0.0)
        refw = dict(x_p=s.x_p, J=s.J, r1=s.moment(1) / s.I, kappa=s.kappa_hi)
        tab = qs._TABLES[wall]
        t_ref = np.linspace(log(tab.x_lo), log(tab.x_hi), len(tab.ln_beta2))
        sp_ref = CubicSpline(t_ref, np.array(tab.ln_beta2))
        a = ps.curve_a(ps.CURVES["4e-31"], n)
        x_R1 = exp((0.0 - a) / (1.0 + 4.0 / n))
        for x, where in (
            (1e40, "reference"),
            (1e3, ""),
            (10.0, ""),
            (1.0, ""),
            (x_R1, "laboratory curve, R = 1"),
        ):
            t0 = time.time()
            tb, an, M, share = build_by_rule(pool, wall, x)
            s_t = x / (1.0 + x)
            # the measured nodes inside the reference table's range, in the reference's x
            xs = np.exp(tb["t"])
            inside = (xs * s_t >= tab.x_lo) & (xs * s_t <= tab.x_hi)
            d_scaled = np.abs(tb["ls"][inside] - sp_ref(np.log(xs[inside] * s_t)))
            inside2 = (xs >= tab.x_lo) & (xs <= tab.x_hi)
            d_unscaled = np.abs(tb["ls"][inside2] - sp_ref(np.log(xs[inside2])))
            label = f"{wall} x = E/V0 = {x:.4g}{' (' + where + ')' if where else ''}, s_t {s_t:.4f}"
            print(
                shape_line(label, tb, an, M, share, refw)
                + f"; |ln|beta|^2 - reference at x s_t| max {d_scaled.max():.1e}; at x (the table's error) max "
                f"{d_unscaled.max():.2f}  ({time.time() - t0:.0f} s)",
                flush=True,
            )
    print(f"# shape end: {uptime()}")


# ---- oldnew ----------------------------------------------------------------------------------------------------------


def mode_oldnew(old_path):
    import importlib.util

    from ComputeTargets import quanta_spectrum as qs

    spec = importlib.util.spec_from_file_location("quanta_spectrum_old", old_path)
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    print(
        f"\n## oldnew: the exponential under {old.SPECTRUM_MODEL} (git show 7c60226:ComputeTargets/quanta_spectrum.py) "
        f"and under {qs.spectrum_model(qs.WALL_EXPONENTIAL)}; start: {uptime()}"
    )
    print(
        "# both normalized with s_dir = x^2 at small x (below x_p in U4-U17-v1, below x_lo in the table), so I and "
        "ln s(x_p) compare; old x is k/(omega_t/2), new x is k/(0.494535 omega_t)"
    )
    for c in (1.0, 0.0):
        so, sn = old.spectrum(c), qs.spectrum(qs.WALL_EXPONENTIAL, c)
        rows = [
            ("I", so.I, sn.I),
            ("J = M2/M3", so.J, sn.J),
            ("M1/M3", so.moment(1) / so.I, sn.moment(1) / sn.I),
            ("ln s(x_p)", so.log_s_peak, sn.log_s_peak),
            ("x_p", old.X_PEAK, sn.x_p),
        ]
        rows += [
            (f"G({y:g})", float(so.G(y)), float(sn.G(y))) for y in (1e-2, 1.0, 10.0)
        ]
        rows += [
            (f"Omega({u:g})", float(so.Omega(u)), float(sn.Omega(u)))
            for u in (0.5, 1.0, 2.0)
        ]
        print(f"c_a = {c:g}:")
        for name, a, b in rows:
            print(f"  {name:10s} old {a:.6e}  new {b:.6e}  new/old {b / a:.4f}")
    print(f"# oldnew end: {uptime()}")


if __name__ == "__main__":
    mode = sys.argv[1]
    print(f"# spectrum_table.py {mode}; start: {uptime()}")
    t_start = time.time()
    with Pool(WORKERS) as pool:
        if mode == "lowk":
            mode_lowk(pool)
        elif mode == "energy":
            mode_energy(pool)
        elif mode == "shape":
            mode_shape(pool)
        elif mode == "oldnew":
            mode_oldnew(sys.argv[2])
        elif mode == "table":
            blocks = mode_table(pool, X_HI)
            with open(sys.argv[2], "w") as fh:
                fh.write("\n\n".join(blocks) + "\n")
        else:
            raise SystemExit(f"unknown mode {mode!r}")
    print(f"# spectrum_table.py {mode} end ({time.time() - t_start:.0f} s): {uptime()}")
