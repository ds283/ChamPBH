"""
power-law-wall prompt 07b §1.7 (P81, P82): the three spectrum tables, and x_*, in x = k/k_ex with the new k_ex.

spectrum_table.py's method (the copy in wall/, imported unchanged): its build, analyse, kappa_direct, model_moments and
build_by_rule, at its strict setting, its step H_T = 0.05 in ln x and its six-figure rules, with k_ex the only change.
spectrum_table.build takes x_scale, which multiplies its k_ex (stage2.lam's, -d ln V/dphi at phi_t); here x_scale =
k_ex(v3)/k_ex(lambda) at the wall's phi_t (kexchain.classical_kex), so the nodes are uniform in the new x. |beta_k|^2 is
a function of k/omega_t alone, which no k_ex enters. The pool is four processes (orchestrator note 07b, ruling 3).

Modes (PYTHONPATH set to the repository root; each prints to stdout):
  kex     k_ex/omega_t at the three references, WallN(90, 1) in walls.py's units and PowerWall(1e40, n), n = 1, 2, in
          units of phi_t, from the chain (classical_kex) and from the closed form (exponential: |V'''/V''| at phi_t =
          1/90 by walls.py's derivatives' formula, (y^6 + 6y^5 + 6y^4)/(y^4 + 2y^3) at y = 90, times E^{1/2}/2; power wall:
          (n + 2)/(2 sqrt(n(n + 1)))), tolerance 1e-14 relative; x_* = 0.0029 omega_t / k_ex at the exponential's
          reference, and on each power wall if carried in k/omega_t;
  table   the tables in the new x: X_LO = 1e-3 omega_t / k_ex; X_HI by orchestrator note 03's ruling 5 (the smallest of 3,
          3.5, 4, ... at which the matched tail carries at most 1e-3 of x^3 s_dir under the model), the tail's share at
          each candidate tried; the spline against direct evaluations at every midpoint; monotonicity either side of
          the peak; x_p; the local slope d ln s/d ln x at X_LO (the spline's end derivative, and a centred difference of
          direct evaluations at X_LO (1 +- 0.02)); kappa_hi (the spline's end derivative, and the direct centred
          difference); the tail's and the k^2 piece's shares; then the cross-check: each new table mapped to k/omega_t
          against the old (the module's literal) mapped the same way, at the nodes they share in k/omega_t (to 1e-12
          relative in k) and, at every other new node inside the old table, against the old table's spline. The
          literals, as quanta_spectrum.py writes them (WallTable's fields, the ln|beta|^2 tuples), go to the path given;
  shape   the exponential's shape over L (WallN(L, 1) at L = 40, 90, 134, P21) in its own new x against the new L = 90
          table (rebuilt here at the shape step, the reference), and R9 on the power wall: at E/V0 = 1e3, 10, 1 and the
          laboratory curve's R = 1 point, as 03 measured (verification §3.5), the spectrum measured at E/V0 in its own new
          x against the new reference table at x: the table's error in x near the floor.

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/spectrum_kex.py kex \\
        > .documents/power-law-wall/kex-07b/spectrum.txt
    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/spectrum_kex.py table \\
        .documents/power-law-wall/kex-07b/spectrum_tables.txt >> .documents/power-law-wall/kex-07b/spectrum.txt
    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/spectrum_kex.py shape \\
        >> .documents/power-law-wall/kex-07b/spectrum.txt
"""

import os
import sys
import time
from math import exp, log, sqrt

import numpy as np
from scipy.interpolate import CubicSpline

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402

MODS = kc.load(
    "wall", ("walls", "born", "stage2", "powerscan", "stage1a_power", "spectrum_table")
)
ST = MODS["spectrum_table"]
WORKERS = 4
X_STAR_Q = (
    0.0029  # the 2PI probe's momentum in omega_t (quanta_spectrum.py, X_STAR's comment)
)
REFERENCE_STRINGS = {
    "exponential": "WallN(90, 1): V_0 e^{M/phi} at L = ln(1 + E/V_0) = 90 (P21)",
    "power-n1": "PowerWall(1e40, 1): V_0 [1 + M/phi] at E/V_0 = 1e40, the scale-free reference (R9)",
    "power-n2": "PowerWall(1e40, 2): V_0 [1 + (M/phi)^2] at E/V_0 = 1e40, the scale-free reference (R9)",
}
CONST = {
    "exponential": "WALL_EXPONENTIAL",
    "power-n1": "WALL_POWER_N1",
    "power-n2": "WALL_POWER_N2",
}


def kex_both(name, param=None, units=None):
    """(k_ex(lambda)/omega_t, k_ex(v3)/omega_t, omega_t) at the wall's phi_t."""
    w, k_old, omega = ST.make_wall(name, param, units)
    k_new = float(kc.classical_kex(w, "v3") / omega)
    k_lam = kc.classical_kex(w, "lambda") / omega
    if abs(k_lam / k_old - 1.0) > 1e-15:
        raise RuntimeError(
            f"{name}: kexchain's lambda k_ex {k_lam!r} is not spectrum_table's {k_old!r}"
        )
    return k_old, k_new, omega


def closed_form(name):
    if name == "exponential":
        y = 90.0
        w = MODS["walls"].WallN(90.0, 1)
        return (y**6 + 6 * y**5 + 6 * y**4) / (y**4 + 2 * y**3) * sqrt(w.E) / 2.0
    n = int(name[-1])
    return (n + 2) / (2.0 * sqrt(n * (n + 1)))


def mode_kex():
    print(f"\n## kex: k_ex/omega_t at the three references; start: {ST.uptime()}")
    k_exp_new = None
    for name in ST.WALL_NAMES:
        k_old, k_new, _ = kex_both(name)
        cf = closed_form(name)
        d = k_new / cf - 1.0
        print(
            f"{name:12s} old (lambda) {k_old!r}; new (v3): chain {k_new!r}, closed form {cf!r}, rel {d:+.1e} "
            f"({'holds' if abs(d) <= 1e-14 else 'FAILS'} 1e-14); new/old {k_new / k_old:.12f}; X_LO = 1e-3/k_ex "
            f"{1e-3 / k_new!r} (old {1e-3 / k_old!r})"
        )
        if name == "exponential":
            k_exp_new = k_new
    print(
        f"x_* = {X_STAR_Q} omega_t / k_ex at the exponential's reference: {X_STAR_Q / k_exp_new!r} (U4's 0.0058 = "
        f"0.0029/(1/2); in the old x at L = 90, {X_STAR_Q / kex_both('exponential')[0]:.6f})"
    )
    for name in ("power-n1", "power-n2"):
        _, k_new, _ = kex_both(name)
        print(
            f"x_* carried in k/omega_t on {name}: {X_STAR_Q / k_new!r} (R10 carries it in x: "
            f"{X_STAR_Q / k_exp_new:.6f} on every wall)"
        )


def literal(name, tb, an, x_hi):
    """The module's literals. x_hi is the rule's value (3, 3.5, ...), from whose ln the nodes were built: exp(ln 3) is
    3.0000000000000004, and the module rebuilds its nodes from ln of the literal, so the literal is the rule's value.
    """
    lines = [
        f"_LN_BETA2_{CONST[name][5:]} = (",
        *[f"    {float(v)!r}," for v in tb["ls"]],
        ")",
        "",
        f"    {CONST[name]}: WallTable(",
        f"        wall={CONST[name]},",
        f'        reference="{REFERENCE_STRINGS[name]}",',
        f"        k_ex_over_omega_t={tb['kex']!r},",
        f"        x_lo={exp(tb['t'][0])!r},",
        f"        x_hi={x_hi!r},",
        f"        ln_beta2=_LN_BETA2_{CONST[name][5:]},",
        "    ),",
    ]
    return "\n".join(lines)


def tail_share(tb, an):
    M = ST.model_moments(tb["t"], tb["ls"], an["kappa_hi"])
    x_hi, k_hi = an["x_hi"], an["kappa_hi"]
    tail3 = exp(tb["ls"][-1]) * sum(
        __import__("math").comb(3, j)
        * x_hi ** (3 - j)
        * __import__("math").factorial(j)
        / k_hi ** (j + 1)
        for j in range(4)
    )
    return tail3 / M[3], M


def mode_table(pool, out_path):
    from ComputeTargets import quanta_spectrum as qs

    print(f"\n## table: the three tables in the new x; start: {ST.uptime()}")
    blocks = []
    for name in ST.WALL_NAMES:
        t0 = time.time()
        k_old, k_new, _ = kex_both(name)
        scale = k_new / k_old
        tried, x_try = [], ST.X_HI_CANDIDATES[0]
        while True:
            tb = ST.build(pool, name, x_hi=x_try, x_scale=scale)
            an = ST.analyse(tb)
            share, M = tail_share(tb, an)
            tried.append(f"{x_try:g}: {share:.3e}")
            if share <= ST.SHARE_TARGET:
                break
            x_try += 0.5
        x_lo = exp(tb["t"][0])
        below3 = exp(tb["ls"][0]) / x_lo**2 * x_lo**6 / 6.0
        kd = ST.kappa_direct(pool, name, tb["kex"], an["x_hi"])
        sl_spline = float(an["spline"](tb["t"][0], 1))
        b = np.concatenate(
            pool.map(
                ST.b2,
                [
                    (name, None, None, ST.STRICT, [x_lo * tb["kex"] * f])
                    for f in (1 / 1.02, 1.02)
                ],
            )
        )
        sl_direct = (log(b[1]) - log(b[0])) / (2.0 * log(1.02))
        print(
            f"{name:12s} the tail's share under the model at x_hi = " + ", ".join(tried)
        )
        print(
            f"{name:12s} k_ex/omega_t {tb['kex']!r} (old {k_old!r}); X_LO {x_lo!r} (k/omega_t 1e-3), X_HI "
            f"{an['x_hi']:g} (k/omega_t {an['x_hi'] * tb['kex']:.5f}); {len(tb['t'])} nodes, step "
            f"{(tb['t'][-1] - tb['t'][0]) / (len(tb['t']) - 1):.6f} in ln x; |beta|^2 at X_LO {exp(tb['ls'][0]):.4e}, "
            f"at X_HI {exp(tb['ls'][-1]):.4e}, max {exp(tb['ls'].max()):.5e}"
        )
        print(
            f"   spline against {len(tb['tm'])} direct midpoints: max rel {an['mid_max']:.2e} (at x = "
            f"{an['mid_where']:.4g}), rms {an['mid_rms']:.2e}; derivative of the wrong sign at {an['rising_bad']} points "
            f"below the peak and {an['falling_bad']} above (of {40 * (len(tb['t']) - 1) + 1})"
        )
        print(
            f"   x_p {an['x_p']:.6f} (parabola {an['x_p_parabola']:.6f}; k_peak/omega_t {an['x_p'] * tb['kex']:.6f}); "
            f"local slope d ln s/d ln x at X_LO {sl_spline:.4f} (spline), {sl_direct:.4f} (direct); kappa_hi "
            f"{an['kappa_hi']:.6f} (spline), {kd:.6f} (direct)"
        )
        print(
            f"   under the model at c_a = 0: M1/M3 {M[1] / M[3]:.6f}, J {M[2] / M[3]:.6f}; the tail's share "
            f"{share:.3e}, the k^2 piece's {below3 / M[3]:.3e}  ({time.time() - t0:.0f} s)",
            flush=True,
        )
        # the cross-check against the old table, in k/omega_t
        old = qs._TABLES[name]
        t_old = np.linspace(log(old.x_lo), log(old.x_hi), len(old.ln_beta2))
        k_old_nodes = np.exp(t_old) * old.k_ex_over_omega_t
        k_new_nodes = np.exp(tb["t"]) * tb["kex"]
        sp_old = CubicSpline(np.log(k_old_nodes), np.array(old.ln_beta2))
        shared, worst_shared = 0, 0.0
        for i, kn in enumerate(k_new_nodes):
            j = int(np.argmin(np.abs(k_old_nodes / kn - 1.0)))
            if abs(k_old_nodes[j] / kn - 1.0) <= 1e-12:
                shared += 1
                worst_shared = max(worst_shared, abs(tb["ls"][i] - old.ln_beta2[j]))
        inside = (k_new_nodes >= k_old_nodes[0]) & (k_new_nodes <= k_old_nodes[-1])
        d_sp = np.abs(tb["ls"][inside] - sp_old(np.log(k_new_nodes[inside])))
        print(
            f"   cross-check in k/omega_t: {shared} nodes shared with the old table (to 1e-12 in k), largest "
            f"|d ln|beta|^2| there {worst_shared:.1e}; at the other {int(inside.sum()) - shared} new nodes inside the old "
            f"table, against its spline, largest {d_sp.max():.1e} (the old table's spline precision is 2.8e-6 relative)"
        )
        blocks.append(
            f"# ---- {name}: k_ex/omega_t = {tb['kex']!r}; x_lo = {x_lo!r}; x_hi = {an['x_hi']!r}; "
            f"{len(tb['t'])} nodes; x_p = {an['x_p']!r}; kappa_hi = {an['kappa_hi']!r}; tail share {share:.3e}\n"
            + literal(name, tb, an, float(x_try))
        )
    with open(out_path, "w") as fh:
        fh.write(
            "# power-law-wall prompt 07b §1.7: the spectrum tables in x = k/k_ex with k_ex = (1/2) E^{1/2} |V'''/V''| at\n"
            "# phi_t, written as ComputeTargets/quanta_spectrum.py writes its literals (the ln|beta|^2 tuples, and each\n"
            "# wall's WallTable entry of _TABLES), by prompts/power-law-wall/source/kex/spectrum_kex.py table.\n"
            "# Measured, not adopted: 07c sets them.\n\n" + "\n\n".join(blocks) + "\n"
        )
    print(
        f"# literals written to {os.path.relpath(out_path)}; table end: {ST.uptime()}"
    )


def mode_shape(pool):
    print(
        f"\n## shape: each table by ruling 5's rule at step {ST.H_T_SHAPE} in the new ln x; moments at c_a = 0; start: "
        f"{ST.uptime()}"
    )
    k_old, k_new, _ = kex_both("exponential")
    tb, an, M, share = ST.build_by_rule(
        pool, "exponential", 90.0, x_scale=k_new / k_old
    )
    ref = dict(x_p=an["x_p"], J=M[2] / M[3], r1=M[1] / M[3], kappa=an["kappa_hi"])
    print(
        "# the exponential's shape over L (WallN(L, 1), k_ex(v3) exact at each L), against L = 90's new table"
    )
    for L in (40.0, 90.0, 134.0):
        t0 = time.time()
        k_old, k_new, _ = kex_both("exponential", L)
        tb, an, M, share = ST.build_by_rule(
            pool, "exponential", L, x_scale=k_new / k_old
        )
        print(
            ST.shape_line(f"exponential L = {L:g}", tb, an, M, share, ref)
            + f"  ({time.time() - t0:.0f} s)",
            flush=True,
        )
    print(
        "# R9 on the power wall: the spectrum measured at E/V0 = x in its own new x (k_ex(v3) at that x), against the new "
        "reference table (x = 1e40) at the same x, at the measured nodes inside the reference's range"
    )
    for name in ("power-n1", "power-n2"):
        n = int(name[-1])
        k_old, k_new, _ = kex_both(name)
        tbr, anr, Mr, sharer = ST.build_by_rule(pool, name, 1e40, x_scale=k_new / k_old)
        sp_ref = CubicSpline(tbr["t"], tbr["ls"])
        refw = dict(
            x_p=anr["x_p"], J=Mr[2] / Mr[3], r1=Mr[1] / Mr[3], kappa=anr["kappa_hi"]
        )
        a = MODS["powerscan"].curve_a(MODS["powerscan"].CURVES["4e-31"], n)
        x_R1 = exp((0.0 - a) / (1.0 + 4.0 / n))
        for x, where in (
            (1e3, ""),
            (10.0, ""),
            (1.0, ""),
            (x_R1, "laboratory curve, R = 1"),
        ):
            t0 = time.time()
            units = "phi_t" if x > 1e3 else "M"
            k_old, k_new, _ = kex_both(name, x, units)
            tb, an, M, share = ST.build_by_rule(
                pool, name, x, units, x_scale=k_new / k_old
            )
            s_t = x / (1.0 + x)
            inside = (tb["t"] >= tbr["t"][0]) & (tb["t"] <= tbr["t"][-1])
            d = np.abs(tb["ls"][inside] - sp_ref(tb["t"][inside]))
            label = f"{name} x = E/V0 = {x:.4g}{' (' + where + ')' if where else ''}, s_t {s_t:.4f}, k_ex/omega_t {k_new:.6f}"
            print(
                ST.shape_line(label, tb, an, M, share, refw)
                + f"; |ln|beta|^2 - reference at the same x| max {d.max():.1e} over {int(inside.sum())} nodes  "
                f"({time.time() - t0:.0f} s)",
                flush=True,
            )
    print(f"# shape end: {ST.uptime()}")


if __name__ == "__main__":
    from multiprocessing import Pool

    mode = sys.argv[1]
    print(f"# spectrum_kex.py {mode}; start: {ST.uptime()}")
    t_start = time.time()
    if mode == "kex":
        mode_kex()
    else:
        with Pool(WORKERS) as pool:
            if mode == "table":
                mode_table(pool, sys.argv[2])
            elif mode == "shape":
                mode_shape(pool)
            else:
                raise SystemExit(f"unknown mode {mode!r}")
    print(
        f"# spectrum_kex.py {mode} end ({time.time() - t_start:.0f} s): {ST.uptime()}"
    )
