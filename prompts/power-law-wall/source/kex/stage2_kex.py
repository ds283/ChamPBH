"""
power-law-wall prompt 07b §1.4 (P78): stage 2 on the exponential with its mode grid set from the new k_ex.

crossover's stage2.solve (copied, crossover/stage2.py), unchanged, run by run_point_kex: crossover's run_point with the
chain and the crossing rate taken in a form of k_ex (kexchain), every other choice unchanged: the grid from cmin m_ref to
cmax k_ref at 80 modes a decade, m_ref and k_ref the chain's m_ta and k_ex where it has a solution, else omega_t = 1 and
the classical k_ex at phi_t; EPS lowered as e^{-2(ell - 8)} above ell = 8; a second and third pass with the grid set from
the turn's mass and crossing rate (the form's rate at the turn times E^{1/2}/2) when either is off its reference by more
than a factor 1.5. With form "lambda" it is crossover's run_point.

Modes:
  reference  three recorded points of crossover's stage2_scan.txt re-run with the grid from the new k_ex: a one-loop
             point (2197 A0.05 #107), a point that moves from gap to one-loop (2197 A0.01 #183) and a gap point
             (2197 A0.01 #512). Each against its recorded fraction: the prompt's precision (about +-6e-3, §1.4) decides,
             ruling 4 (a point off by more is a stop); crossover's own stated precision (+-2e-3) is reported beside;
  new        the new points (prompt §1.4): on each n = 1 curve, between the gap's entry under the old form and R = 1, at
             target values of ell_new chosen so that every 0.5-wide bin of ell_new below 3 that the curve's mapped
             history reaches there holds at least four points (with crossover's on-curve rows); four on the n = 2
             laboratory curve in the same range;
  reference-kmax  the gap reference point in both forms at several k_max, to see whether its difference from the
             record is the label or the grid's upper end (measured after the reference, 2026-11-17);
  born-offcurve  the Born limit at the validation points' L, at the ln R where ell_new = 6 (off-curve): at the points
             themselves k_IR = 10 m_ta lies above k_ex, where the Born reduction does not hold (measured 2026-11-17);
  validate   at three of the new points: the classical limit (the loop off, 8 pi^2 rho against walls.py's independent
             test field, 241-point ln-k trapezoid on [1e-3, 10], tolerance 1e-4: crossover's V-A agreement, 1e-5, and
             the two quadratures), the Born limit (the grid cut at k_IR = 10 m_ta against born.run at the same k_IR, with
             born's chain taken in the new form, tolerance 2e-3 relative: crossover's V1 (7e-4) and n = 2 V-B (1e-3)
             agreement, doubled for the smaller ell here) and convergence (crossover's V4 variants at the base run's
             grid references, tolerance +-6e-3, the prompt's precision).

The machine (orchestrator note 07b, ruling 3): at most four points at once, one at a time while the one-minute load is
above 24; the load is printed at each point's start and end; a point not done in 30 minutes is stopped and recorded with
its state.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B stage2_kex.py MODE \\
        > ../../../../.documents/power-law-wall/kex-07b/stage2_MODE.txt
"""

import multiprocessing as mp
import os
import sys
import time
from math import exp, log, pi, sqrt

import numpy as np
from scipy.optimize import brentq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402

M = kc.load("crossover", ("walls", "born", "stage2"))
walls, born, stage2 = M["walls"], M["born"], M["stage2"]

MAX_AT_ONCE = 4
LOAD_SERIAL = 24.0
TIMEOUT = 30 * 60
PRECISION = 6e-3  # the prompt's (§1.4)
XO_PRECISION = 2e-3  # crossover's stated precision of its scan

# ln(V0/M^4) of crossover's curves (stage2_mscan.py; the laboratory curve's cascade points have 3.345 +- 0.005)
CURVES = {"lab": 3.345, "L-M8e-18": -119.16, "L-M1e-16": -129.27, "L-M5e-16": -135.67}


def lnR_n1(L, lv):
    """crossover's curve at n = 1 (stage2_mscan.lnR_on)."""
    return lv + 2 * L + 2 * log(L**4 + 2 * L**3) - log(exp(L) - 1) - log(8 * pi**2)


def lnR_n2(L, lv):
    """crossover's curve at n = 2 (its README, "After closing: the n = 2 wall")."""
    return (
        lv
        + 2 * L
        - log(exp(L) - 1)
        + 2 * log(4 * L**2 + 6 * L)
        + 2 * log(L)
        - log(8 * pi**2)
    )


def wall(L, n):
    return walls.Wall(L) if n == 1 else walls.WallN(L, n)


def run_point_kex(
    L, lnR, form="v3", n=1, cmin=0.03, cmax=30.0, ppd=80, iterate=True, **kw
):
    w = wall(L, n)
    ch = kc.chain(w, exp(lnR), form)
    if ch is not None and ch[3] > 8.0 and "eps" not in kw:
        kw = dict(kw, eps=stage2.EPS * exp(-2.0 * (ch[3] - 8.0)))
    if ch is not None:
        mref, kref = ch[1], ch[2]
    else:
        mref, kref = 1.0, kc.classical_kex(w, form)
    for it in range(3):
        res = stage2.solve(
            L, lnR, kmin=cmin * mref, kmax=cmax * kref, ppd=ppd, mref=mref, n=n, **kw
        )
        res["mref"], res["kref"], res["passes"] = mref, kref, it + 1
        res["kex_turn_form"] = kc.rate(w, res["phi_turn"], form) * sqrt(w.E) / 2.0
        if not iterate:
            break
        mt, kt = res["m_turn"], res["kex_turn_form"]
        if 1 / 1.5 <= mt / mref <= 1.5 and 1 / 1.5 <= kt / kref <= 1.5:
            break
        mref, kref = mt, kt
    return res


def chains(L, lnR, n):
    """(regime, ell, k_ex) in each form; k_ex the chain's, or the classical turning point's where there is no chain."""
    w = wall(L, n)
    out = {}
    for form in kc.FORMS:
        ch = kc.chain(w, exp(lnR), form)
        out[form] = (
            kc.regime(lnR, ch),
            None if ch is None else ch[3],
            ch[2] if ch is not None else kc.classical_kex(w, form),
        )
    return out


def describe(L, lnR, n):
    c = chains(L, lnR, n)
    parts = []
    for form in kc.FORMS:
        reg, ell, kex = c[form]
        parts.append(
            f"{form}: {reg} ell {'none' if ell is None else format(ell, '.4f')} k_ex {kex:.5e}"
        )
    return "; ".join(parts)


def load1():
    return os.getloadavg()[0]


# ---- the points ------------------------------------------------------------------------------------------------------

REFERENCE = (
    ("one-loop: 2197 A0.05 #107", 7.885, 23.833, 0.7171),
    ("gap -> one-loop: 2197 A0.01 #183", 3.619, 13.791, 0.8264),
    ("gap: 2197 A0.01 #512", 1.354, 4.865, 0.1594),
)

# target ell_new per curve (the bins and the existing rows are in the log and in stage2_new.txt's header)
NEW_TARGETS = (
    ("lab", 1, (0.12, 0.25, 0.37, 0.55, 0.65, 0.9, 1.23, 1.48)),
    ("L-M8e-18", 1, (0.305, 0.33, 0.39)),
    ("lab", 2, (0.3, 0.7, 1.1, 1.45)),
)


def on_curve(curve, n, target):
    """L on the curve where the new chain's ell equals target, between where the chain begins and the old gap entry."""
    lv = CURVES[curve]
    f = lnR_n1 if n == 1 else lnR_n2

    def g(L):
        ch = kc.chain(wall(L, n), exp(f(L, lv)), "v3")
        return (ch[3] if ch is not None else 0.0) - target

    lo, hi = (1.0, 4.5) if curve == "lab" else (93.0, 94.5)
    return brentq(g, lo, hi, xtol=1e-6)


def new_points():
    pts = []
    for curve, n, targets in NEW_TARGETS:
        for t in targets:
            L = on_curve(curve, n, t)
            lnR = (lnR_n1 if n == 1 else lnR_n2)(L, CURVES[curve])
            pts.append((f"{curve} n={n} ell_new~{t:g}", L, lnR, n))
    return pts


VALIDATE_AT = ("lab n=1 ell_new~1.48", "lab n=1 ell_new~1.23", "lab n=2 ell_new~1.45")
BORN_ELL = 6.0


# ---- the work, one point per process ---------------------------------------------------------------------------------


def task_point(job):
    label, L, lnR, n, extra = job
    t0 = time.time()
    l0 = load1()
    res = run_point_kex(L, lnR, "v3", n)
    reg_new = chains(L, lnR, n)
    line = (
        f"{label:34s} n={n} L={L:9.5f} lnR={lnR:9.4f} | frac {res['frac_modes']:.5f} balance "
        f"{res['frac_balance']:.5f} vout/vin {res['vout']:.4f} back {str(res['returned']):5s} eps_turn "
        f"{res['eps_turn']:.1e} |dE/E| {res['drift']:.1e} passes {res['passes']} modes {res['nk']} | "
        f"{describe(L, lnR, n)}"
    )
    if extra is not None:
        d = res["frac_modes"] - extra
        line += (
            f" | recorded {extra:.4f}, diff {d:+.4f}: {'within' if abs(d) <= PRECISION else 'OUTSIDE'} +-{PRECISION:g}"
            f" ({'within' if abs(d) <= XO_PRECISION else 'outside'} crossover's +-{XO_PRECISION:g})"
        )
    line += f" | {time.time() - t0:.0f} s, load {l0:.2f} -> {load1():.2f}"
    _ = reg_new
    return line


def task_kmax(job):
    label, L, lnR, n, (form, cmax) = job
    t0 = time.time()
    l0 = load1()
    r = run_point_kex(L, lnR, form, n, cmax=cmax)
    return (
        f"{label:24s} frac {r['frac_modes']:.5f} passes {r['passes']} k_ref {r['kref']:.4e} k_max {r['k'][-1]:.4e} "
        f"modes {r['nk']} steps {r['steps']} |dE/E| {r['drift']:.1e} | {time.time() - t0:.0f} s, load {l0:.2f} -> "
        f"{load1():.2f}"
    )


def test_field_S(L, n):
    """walls.py's independent S(L): 4 int k^4 |beta|^2 d ln k over [1e-3, 10] (241 points), crossover's V-A."""
    w = wall(L, n)
    b = walls.Bounce(w, w.start_phi(1e-3))
    ks = np.geomspace(1e-3, 10.0, 241)
    b2 = np.array([walls.beta_squared(b, w.V2, w.V3, k, w.start_phi(k)) for k in ks])
    return 4.0 * np.trapezoid(ks**4 * b2, np.log(ks))


def task_validate(job):
    label, L, lnR, n, kind = job
    t0 = time.time()
    l0 = load1()
    w = wall(L, n)
    out = []
    if kind == "classical":
        kref = kc.classical_kex(w, "v3")
        r = stage2.solve(L, 0.0, kmin=3e-3, kmax=30 * kref, mref=1.0, loop=False, n=n)
        s = test_field_S(L, n)
        d = r["frac_modes"] / s - 1.0
        out.append(
            f"classical {label}: L={L:.5f} loop off, grid [3e-3, 30 k_ex(v3) = {30 * kref:.4f}]: 8 pi^2 rho "
            f"{r['frac_modes']:.6e}, walls.py {s:.6e}, rel {d:+.1e} ({'holds' if abs(d) <= 1e-4 else 'FAILS'} 1e-4); "
            f"v_out/v_in {r['vout']:.6f}"
        )
    elif kind in ("born", "born-offcurve"):
        if kind == "born-offcurve":
            # the Born reduction holds only where k_IR = 10 m_ta lies well below k_ex (crossover's V1: ell = 4.9-9.5); at
            # the validation point's L, the ln R where the new chain's ell is BORN_ELL, off the curve
            lnR = brentq(
                lambda x: kc.chain(w, exp(x), "v3")[3] - BORN_ELL,
                lnR,
                lnR + 60.0,
                xtol=1e-10,
            )
            label = f"{label} at ln R = {lnR:.4f} (off-curve, ell_new = {BORN_ELL:g})"
        R = exp(lnR)
        ch = kc.chain(w, R, "v3")
        born.chain = lambda ww, RR: kc.chain(
            ww, RR, "v3"
        )  # noqa: E731  (this worker only)
        k10 = 10.0 * ch[1]
        rb = born.run(w, R, k10)
        rs = stage2.solve(L, lnR, kmin=k10, kmax=30 * ch[2], mref=ch[1], n=n)
        d = rs["frac_modes"] / rb["Efl_final"] - 1.0
        out.append(
            f"born {label}: ell_new {ch[3]:.4f}, k_IR = 10 m_ta = {k10 / ch[2]:.3f} k_ex: born.run {rb['Efl_final']:.6e}, "
            f"stage 2 cut {rs['frac_modes']:.6e}, rel {d:+.1e} ({'holds' if abs(d) <= 2e-3 else 'FAILS'} 2e-3); "
            f"|dE/E| {rs['drift']:.1e}"
        )
    else:
        base = run_point_kex(L, lnR, "v3", n)
        mref, kref = base["mref"], base["kref"]
        out.append(
            f"convergence {label}: base frac {base['frac_modes']:.6f} (passes {base['passes']}, mref {mref:.4e}, "
            f"kref {kref:.4e})"
        )
        worst = 0.0
        for vname, kw in (
            ("ppd x2", dict(ppd=160)),
            ("kmax x2", dict(cmax=60.0)),
            ("kmin /2", dict(cmin=0.015)),
            ("h /2", dict(h=0.05)),
            ("EPS /10", dict(eps=stage2.EPS / 10)),
            ("F0 /100", dict(f0=stage2.F0 / 100)),
        ):
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
                n=n,
                **kw,
            )
            d = r["frac_modes"] - base["frac_modes"]
            worst = max(worst, abs(d))
            out.append(
                f"    {vname:8s} frac {r['frac_modes']:.6f} d {d:+.1e} |dE/E| {r['drift']:.1e}"
            )
        out.append(
            f"    largest |d| {worst:.1e} ({'holds' if worst <= PRECISION else 'FAILS'} +-{PRECISION:g})"
        )
    out[-1] += f"  ({time.time() - t0:.0f} s, load {l0:.2f} -> {load1():.2f})"
    return "\n".join(out)


def _worker(fn, job, q):
    try:
        q.put(fn(job))
    except Exception as e:  # noqa: BLE001
        q.put(f"{job[0]}: FAILED {type(e).__name__}: {e}")


def schedule(fn, jobs):
    """Run jobs, each in its own process: at most MAX_AT_ONCE at once, one at a time while the one-minute load is above
    LOAD_SERIAL; a job past TIMEOUT is terminated and reported. Lines are printed in completion order.
    """
    ctx = mp.get_context("spawn")
    pending = list(jobs)
    running = []
    while pending or running:
        allowed = 1 if load1() > LOAD_SERIAL else MAX_AT_ONCE
        while pending and len(running) < allowed:
            job = pending.pop(0)
            q = ctx.Queue()
            p = ctx.Process(target=_worker, args=(fn, job, q))
            p.start()
            running.append((job, p, q, time.time()))
            print(
                f"# start {job[0]} (running {len(running)}, load {load1():.2f})",
                flush=True,
            )
            allowed = 1 if load1() > LOAD_SERIAL else MAX_AT_ONCE
        time.sleep(2.0)
        still = []
        for job, p, q, t_start in running:
            if not q.empty():
                print(q.get(), flush=True)
                p.join()
            elif not p.is_alive():
                p.join()
                print(f"{job[0]}: ended with no result (exit {p.exitcode})", flush=True)
            elif time.time() - t_start > TIMEOUT:
                p.terminate()
                p.join()
                print(
                    f"{job[0]}: STOPPED at {TIMEOUT / 60:.0f} minutes, not done (load {load1():.2f}); state: running",
                    flush=True,
                )
            else:
                still.append((job, p, q, t_start))
        running = still


def main():
    mode = sys.argv[1]
    t0 = time.time()
    print(
        f"# stage2_kex.py {mode}; start: {os.popen('uptime').read().strip()}",
        flush=True,
    )
    if mode == "reference":
        print(
            "# the grid's reference: recorded points re-run with the grid from the new k_ex (run_point_kex, form v3); "
            "per line: the result, both forms' regime, ell and k_ex (chain or classical), the recorded fraction"
        )
        schedule(task_point, [(lab, L, lnR, 1, f) for lab, L, lnR, f in REFERENCE])
    elif mode == "born-offcurve":
        print(
            "# the Born limit at the validation points' L, off the curve at the ln R where ell_new = 6: at the points "
            "themselves (ell_new <= 1.5) k_IR = 10 m_ta lies above k_ex, outside the Born reduction (stage2_validate.txt)"
        )
        pts = {lab: (L, lnR, n) for lab, L, lnR, n in new_points()}
        schedule(
            task_validate, [(lab, *pts[lab], "born-offcurve") for lab in VALIDATE_AT]
        )
    elif mode == "reference-kmax":
        print(
            "# the gap reference point (2197 A0.01 #512) in both forms at several k_max = cmax k_ref: whether its "
            "difference from the record is the label or the grid's upper end"
        )
        jobs = [
            (f"#512 {form} cmax {cmax:g}", 1.354, 4.865, 1, (form, cmax))
            for form, cmax in (
                ("lambda", 30.0),
                ("lambda", 60.0),
                ("lambda", 120.0),
                ("v3", 7.5),
                ("v3", 30.0),
                ("v3", 60.0),
            )
        ]
        schedule(task_kmax, jobs)
    elif mode == "new":
        pts = new_points()
        print(
            "# the new points (run_point_kex, form v3), at target ell_new on each curve:"
        )
        for lab, L, lnR, n in pts:
            print(f"#   {lab:34s} L={L:.6f} ln R={lnR:.6f}  {describe(L, lnR, n)}")
        schedule(task_point, [(lab, L, lnR, n, None) for lab, L, lnR, n in pts])
    elif mode == "validate":
        pts = {lab: (L, lnR, n) for lab, L, lnR, n in new_points()}
        jobs = []
        for lab in VALIDATE_AT:
            L, lnR, n = pts[lab]
            for kind in ("classical", "born", "convergence"):
                jobs.append((lab, L, lnR, n, kind))
        schedule(task_validate, jobs)
    else:
        raise SystemExit(f"unknown mode {mode!r}")
    print(
        f"# stage2_kex.py {mode} end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}",
        flush=True,
    )


if __name__ == "__main__":
    main()
