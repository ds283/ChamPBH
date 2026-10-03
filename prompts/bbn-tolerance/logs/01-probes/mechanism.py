"""
bbn-tolerance prompt 01, section 5 (the mechanism): instrument PRyMordial's
full low-T solve on one stored history, default tolerance, `prod`, and compare
with the same history's `pert12` solve.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/mechanism.py BETA M [--rtol X]

The instrument is RecordingBDF, a subclass of SciPy's BDF passed to the low-T
call through the tool's override (`methods=`); nothing in PRyM/ or SciPy is
edited. It observes each step attempt by wrapping
scipy.integrate._ivp.bdf.solve_bdf_system for the duration of its own
_step_impl, and recomputes the error test exactly as BDF._step_impl does
(error = error_const[order] * d; scale = atol + rtol |y_new|). It changes
nothing: the prod solve must fail at the stored t reached (checked below).

Prints:
  - the accepted steps over the last 5 % of the stage's span, and every attempt
    after the last accepted step (h, order, Newton convergence, error norm, the
    component dominating |err_i| / scale_i);
  - negative abundances and abundances at or below atol at the last steps;
  - the T_of_t breakpoints (the thermodynamic solve's t_eval) within the last
    accepted steps;
  - Yp and D/H at the failure time against pert12's dense output at the same t,
    and pert12's change from that t to the end of the stage.
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import scipy.integrate._ivp.bdf as bdfmod

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
import PRyM.PRyM_init as PI  # noqa: E402
from Units import Planck_units  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
SPECIES = T.SPECIES


class RecordingBDF(bdfmod.BDF):
    attempts = None  # list, set per run
    jac_calls = None
    steps = None
    probe_t = None
    probe_y = None

    def _step_impl(self):
        solver = self
        orig = bdfmod.solve_bdf_system
        t_old = self.t
        order = self.order

        def recording(fun, t_new, y_predict, c, psi, LU, solve_lu, scale, tol):
            # SciPy 1.17's solve_bdf_system, statement for statement (so the
            # arithmetic and the result are identical), recording each iteration
            iters = []
            d = 0
            y = y_predict.copy()
            dy_norm_old = None
            converged = False
            for k in range(bdfmod.NEWTON_MAXITER):
                f = fun(t_new, y)
                if not np.all(np.isfinite(f)):
                    iters.append({"k": k, "nonfinite": np.flatnonzero(~np.isfinite(f))})
                    break
                dy = solve_lu(LU, c * f - psi - d)
                dy_norm = bdfmod.norm(dy / scale)
                rate = None if dy_norm_old is None else dy_norm / dy_norm_old
                iters.append(
                    {
                        "k": k,
                        "dy_scaled": np.abs(dy / scale),
                        "dy": np.array(dy),
                        "y": np.array(y),
                        "dy_norm": float(dy_norm),
                        "rate": rate,
                        "f": np.array(f),
                    }
                )
                if rate is not None and (
                    rate >= 1
                    or rate ** (bdfmod.NEWTON_MAXITER - k) / (1 - rate) * dy_norm > tol
                ):
                    break
                y += dy
                d += dy
                if (
                    dy_norm == 0
                    or rate is not None
                    and rate / (1 - rate) * dy_norm < tol
                ):
                    converged = True
                    break
                dy_norm_old = dy_norm
            res = (converged, k + 1, y, d)
            converged, n_iter, y_new, d = res
            type(solver).last_fun = (solver, float(t_new), np.array(y_predict), c)
            rec = {
                "iters": iters,
                "tol": tol,
                "scale": np.array(scale),
                "t": t_old,
                "t_new": float(t_new),
                "h": float(t_new - t_old),
                "order": order,
                "converged": bool(converged),
                "n_iter": int(n_iter),
                "y_predict": np.array(y_predict),
            }
            if converged:
                sc = solver.atol + solver.rtol * np.abs(y_new)
                err = solver.error_const[order] * d
                ratio = np.abs(err) / sc
                rec["error_norm"] = float(np.linalg.norm(err / sc) / np.sqrt(len(sc)))
                rec["ratio"] = ratio
                rec["y_new"] = np.array(y_new)
            type(solver).attempts.append(rec)
            return res

        jac_orig = self.jac
        jac_calls = type(self).jac_calls

        def jac_recording(t, y):
            J = jac_orig(t, y)
            jac_calls.append((float(t), np.array(J)))
            return J

        bdfmod.solve_bdf_system = recording
        if jac_orig is not None:
            self.jac = jac_recording
        try:
            ok, msg = super()._step_impl()
        finally:
            bdfmod.solve_bdf_system = orig
            self.jac = jac_orig
        type(self).steps.append(
            {
                "t_old": t_old,
                "t": float(self.t),
                "h_abs": float(self.h_abs),
                "order": self.order,
                "ok": ok,
                "y": np.array(self.y),
            }
        )
        pt = type(self).probe_t
        if ok and pt is not None and t_old < pt <= self.t:
            type(self).probe_y = np.array(self.dense_output()(pt))
        return ok, msg


def run(kind, logT, r, rho_SM, Tmin, Tmax, rtol=None, probe_t=None):
    RecordingBDF.attempts, RecordingBDF.steps, RecordingBDF.jac_calls = [], [], []
    RecordingBDF.probe_t, RecordingBDF.probe_y = probe_t, None
    cb = T.make_callback(kind, logT, r, rho_SM, Tmin, Tmax, f"mechanism-{kind}")
    with T.lowT_tolerance_override(
        rtol=rtol,
        methods={T.STAGE_LOW_T_FULL: RecordingBDF},
        keep_sol=(T.STAGE_THERMO_NP, T.STAGE_THERMO_SM, T.STAGE_LOW_T_FULL),
    ) as calls:
        result = T.B._run_PRyMordial(cb, False, 600.0)
    return (
        result,
        calls,
        RecordingBDF.attempts,
        RecordingBDF.steps,
        RecordingBDF.probe_y,
    )


def yp_dh(y):
    return 4.0 * y[5], y[2] / y[1] * 1e5


def main():
    p = argparse.ArgumentParser()
    p.add_argument("beta", type=float)
    p.add_argument("M", type=float)
    p.add_argument("--rtol", type=float, default=None)
    args = p.parse_args()

    units = Planck_units()
    cosmo = QCD_Cosmology(0, units, Planck2018())
    h = T.find_model(STORE, args.beta, args.M)
    logT, r = T.ratio_grid(h.rows, units)
    Tmin, Tmax = T.callback_domain_MeV(units)
    rho_SM = T.B.thermodynamic_rho_SM(cosmo, units)
    print(
        f"beta={args.beta:g} M={args.M:g} serial={h.serial} stored: {(h.bbn['failure_reason'] or 'success')[:200]}"
    )

    res, calls, attempts, steps, _ = run("prod", logT, r, rho_SM, Tmin, Tmax, args.rtol)
    lt = [c for c in calls if c.stage == T.STAGE_LOW_T_FULL][0]
    th = [c for c in calls if c.stage in (T.STAGE_THERMO_NP, T.STAGE_THERMO_SM)][0]
    print(
        f"prod: failure={res.get('failure', False)} {res.get('failure_reason', '')[:200]}"
    )
    print(
        f"low-T call: t_span={lt.t_span} atol={lt.kwargs.get('atol')} rtol={lt.kwargs.get('rtol', '(none: 1e-3)')} "
        f"status={lt.status} t_reached={lt.t_reached!r} accepted steps={sum(1 for s in steps if s['ok'])} "
        f"attempts={len(attempts)}"
    )
    t0, t1 = lt.t_span
    atol = lt.kwargs.get("atol")
    t_fail = lt.t_reached

    # accepted steps over the last 5 % of the span
    lo = t0 + 0.95 * (t1 - t0)
    acc = [s for s in steps if s["ok"]]
    print(f"\naccepted steps with t >= {lo:.6g} (last 5 % of [{t0:.6g}, {t1:.6g}]):")
    for s in acc:
        if s["t"] >= lo:
            print(
                f"  t={s['t']:.10g} h={s['t'] - s['t_old']:.4g} next h_abs={s['h_abs']:.4g} order={s['order']}"
            )
    last_ok_t = acc[-1]["t"] if acc else t0
    tail = [a for a in attempts if a["t"] >= last_ok_t]
    print(f"\nattempts from the last accepted t={last_ok_t:.10g}: {len(tail)}")
    for i, a in enumerate(tail):
        if i < 12 or i >= len(tail) - 6:
            if a["converged"]:
                k = int(np.argmax(a["ratio"]))
                top = np.argsort(a["ratio"])[::-1][:3]
                tops = ", ".join(
                    f"{SPECIES[j]} {a['ratio'][j]:.3g} (y={a['y_new'][j]:.3e})"
                    for j in top
                )
                print(
                    f"  [{i}] h={a['h']:.4g} order={a['order']} newton=ok({a['n_iter']}) "
                    f"err_norm={a['error_norm']:.4g} dominant: {tops}"
                )
            else:
                yneg = [
                    SPECIES[j]
                    for j in range(len(a["y_predict"]))
                    if a["y_predict"][j] < 0
                ]
                print(
                    f"  [{i}] h={a['h']:.4g} order={a['order']} newton=FAILED({a['n_iter']}) "
                    f"tol={a['tol']:.3g} y_predict<0: {yneg}"
                )
                for it in a["iters"]:
                    if "nonfinite" in it:
                        print(
                            f"      k={it['k']}: f non-finite in {[SPECIES[j] for j in it['nonfinite']]}"
                        )
                    else:
                        top = np.argsort(it["dy_scaled"])[::-1][:3]
                        tops = ", ".join(
                            f"{SPECIES[j]} {it['dy_scaled'][j]:.3g}" for j in top
                        )
                        rate = "-" if it["rate"] is None else f"{it['rate']:.4g}"
                        print(
                            f"      k={it['k']}: |dy/scale| rms={it['dy_norm']:.4g} rate={rate}; largest {tops}"
                        )
        elif i == 12:
            print("  ...")
    # the dominant component over all rejected-by-error attempts in the tail
    dom = {}
    for a in tail:
        if a["converged"] and a["error_norm"] > 1:
            k = SPECIES[int(np.argmax(a["ratio"]))]
            dom[k] = dom.get(k, 0) + 1
    nconv = sum(1 for a in tail if not a["converged"])
    print(
        f"tail summary: error-test rejections by dominant component {dom}; Newton failures {nconv}"
    )

    # is f smooth along the first Newton increment of the last attempt?
    solver, t_new, y_p, c = RecordingBDF.last_fun
    it0 = tail[-1]["iters"][0]
    dy0 = it0["dy"]
    f0 = np.asarray(solver.fun(t_new, y_p))
    f0b = np.asarray(solver.fun(t_new, y_p))
    J = np.asarray(solver.jac(t_new, y_p))
    print(
        f"\nsmoothness of f at the last attempt (t_new={t_new:.10g}, c={c:.4g}): "
        f"f repeatable: {np.array_equal(f0, f0b)}"
    )
    for j in (6, 9, 11):
        print(
            f"  {SPECIES[j]}: y_p={y_p[j]:.4e} dy0={dy0[j]:.4e} dy0/c={dy0[j] / c:.4e} f0={f0[j]:.6e}"
        )
    for sfac in (1.0, 1e-1, 1e-2, 1e-4, 1e-6):
        fs = np.asarray(solver.fun(t_new, y_p + sfac * dy0))
        lin = J @ (sfac * dy0)
        print(
            f"  s={sfac:g}: (f(y_p + s dy0) - f(y_p)) / s: "
            + ", ".join(
                f"{SPECIES[j]} {(fs[j] - f0[j]) / sfac:.4e} (J dy0 {lin[j] / sfac:.4e})"
                for j in (6, 9, 11)
            )
        )
    t_J, Js = RecordingBDF.jac_calls[-1]
    print(
        f"  the Jacobian in the Newton matrix (the last jac call, at t={t_J:.10g}) against a fresh one at "
        f"t_new: d f_Li8/d Y_Li8 {Js[9, 9]:.4e} against {J[9, 9]:.4e} (ratio fresh/stale "
        f"{J[9, 9] / Js[9, 9]:.4f}); predicted stiff-limit Newton rate |1 - fresh/stale| = "
        f"{abs(1 - J[9, 9] / Js[9, 9]):.4f}"
    )
    jt = [a["t_new"] for a in tail]
    print(f"  attempt t_new from {jt[0]:.10g} (first) to {jt[-1]:.10g} (last)")
    big = np.unravel_index(np.argsort(np.abs(J), axis=None)[::-1][:5], J.shape)
    print(
        "  largest |J| entries: "
        + ", ".join(
            f"d f_{SPECIES[i]}/d Y_{SPECIES[k]} = {J[i, k]:.4e}" for i, k in zip(*big)
        )
    )

    # the dominant component over the last 5 % of accepted attempts too
    dom_acc = {}
    for a in attempts:
        if a["t"] >= lo and a["converged"]:
            k = SPECIES[int(np.argmax(a["ratio"]))]
            dom_acc[k] = dom_acc.get(k, 0) + 1
    print(f"dominant component over all converged attempts in the last 5 %: {dom_acc}")

    print("\nabundances at the last 3 accepted steps:")
    for s in acc[-3:]:
        y = s["y"]
        neg = [SPECIES[j] for j in range(len(y)) if y[j] < 0]
        low = [SPECIES[j] for j in range(len(y)) if 0 <= y[j] <= atol]
        print(
            f"  t={s['t']:.10g}: "
            + " ".join(f"{SPECIES[j]}={y[j]:.3e}" for j in range(len(y)))
        )
        print(f"    negative: {neg}; in [0, atol={atol:g}]: {low}")
    ymins = np.min([a["y_predict"] for a in tail], axis=0) if tail else None
    if ymins is not None:
        print(
            "  min y_predict over the tail attempts: "
            + " ".join(f"{SPECIES[j]}={ymins[j]:.3e}" for j in range(len(ymins)))
        )

    tv = np.asarray(th.sol.t)
    window = (
        [x for x in tv if acc[-6]["t"] <= x <= t_fail * (1 + 1e-9)]
        if len(acc) > 6
        else []
    )
    near = tv[np.argsort(np.abs(tv - t_fail))[:3]]
    print(
        f"\nT_of_t breakpoints (thermodynamic t_eval, {len(tv)} points): within the last 6 accepted "
        f"steps [{acc[-6]['t']:.10g}, {t_fail:.10g}]: {[f'{x:.10g}' for x in window]}; nearest to t_fail: "
        f"{[f'{x:.10g}' for x in sorted(near)]}"
    )

    # the temperature (T_gamma, the thermodynamic solution T_of_t interpolates linearly) at the
    # failure and at the Jacobian refresh
    t_J = RecordingBDF.jac_calls[-1][0]
    Tf = np.interp(t_fail, tv, th.sol.y[0]) * 1e3
    TJ = np.interp(t_J, tv, th.sol.y[0]) * 1e3
    print(
        f"T_gamma at t_fail: {Tf:.5f} keV (T9 {Tf * 1e-3 * PI.MeV_to_Kelvin * 1e-9:.6f}); at the Jacobian "
        f"refresh t={t_J:.10g}: {TJ:.5f} keV (T9 {TJ * 1e-3 * PI.MeV_to_Kelvin * 1e-9:.6f})"
    )

    yf = lt.sol.y[:, -1]
    Ypf, DHf = yp_dh(yf)
    res12, calls12, _, steps12, probe = run(
        "pert12", logT, r, rho_SM, Tmin, Tmax, args.rtol, probe_t=t_fail
    )
    lt12 = [c for c in calls12 if c.stage == T.STAGE_LOW_T_FULL][0]
    Yp12, DH12 = yp_dh(probe)
    Yp12e, DH12e = yp_dh(lt12.sol.y[:, -1])
    print(f"\nprod at t_fail={t_fail:.10g}: Yp={Ypf:.10g} D/H={DHf:.10g}")
    print(
        f"pert12 (status {lt12.status}) at the same t: Yp={Yp12:.10g} D/H={DH12:.10g}; "
        f"relative prod - pert12: Yp {abs(Ypf - Yp12) / Yp12:.3e}, D/H {abs(DHf - DH12) / DH12:.3e}"
    )
    print(
        f"pert12 at the stage end t={lt12.t_reached:.10g}: Yp={Yp12e:.10g} D/H={DH12e:.10g}; change from t_fail "
        f"to the end: Yp {abs(Yp12e - Yp12) / Yp12:.3e}, D/H {abs(DH12e - DH12) / DH12:.3e}; "
        f"stored-route result Yp={res12.get('Yp_BBN')} D/H={res12.get('DOverH')}"
    )


if __name__ == "__main__":
    main()
