"""
Why SciPy's Radau accepts the bad outward step (reflection-restart prompt 01 §1.4; README §6.2 P4).

For one failing history (argv: BETA M ALPHA), run tuned under the production code (the probe switch
at None) with tools/outward_step_scan.py's observer, and keep the start of the bad step (the first step of a restarted instance flagged by the phi condition;
P1's ratio condition also fires on legitimate decelerating steps before an apex): its N, its
state, the right-hand side the solver holds, the Jacobian Radau holds there (built when the instance
was, at the restart) and the solver's tolerances. Then, from that one state:

  1. single Radau attempts at h from the good range to past the bad one, each exactly as SciPy
     1.17.0's `Radau._step_impl` makes one (its private `solve_collocation_system`, `norm`, the
     constants E, MU_REAL, MU_COMPLEX; Z0 = 0 as on an instance's first step; the same J and newton
     tolerance), reproduced here rather than instrumented. For each: Newton's convergence, SciPy's
     error estimate `error_norm` (err = (MU_REAL/h - J)^-1 (f + ZE)), the refined estimate that
     Hairer & Wanner's RADAU5 makes on a first or rejected step and SciPy makes only after a
     rejection (err' = (MU_REAL/h - J)^-1 (f(y + err) + ZE)), whether SciPy would accept the step,
     and the true error of its endpoint against the step re-integrated resolved (Radau, rtol 1e-10,
     max_step = h/20000, as the diagnosis's trace_step.py), in SciPy's own scaled norm;
  1b. SciPy's own path to the accepted step, from the instance's first h_abs: the attempts it makes
     and which estimate decides each;
  2. the growth question: a fresh Radau from the same state, as F3 would start it (first_step =
     max(h_floor, f phi/pi), and at a few larger good first steps), left to grow its step under
     max_step = 0.1 with no cap, for 40 steps, to N0 + 0.02 or to the apex of the outward flight (the
     first step that ends with pi <= 0; past it the loop's inward cap would govern): each step's h, h*omega (omega from a
     Jacobian computed afresh at the step's start, not the stale one Radau holds), SciPy's accepted
     error_norm, the flag of prompt 01 §1.1, and pi.

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/why_radau_accepts.py 1.5 1e-12 0.05

Writes nothing but its stdout; the committed outputs are in .documents/reflection-restart/out/. Needs
no Ray and no store. About 1-3 minutes (the resolved re-integrations dominate).
"""

import sys

import numpy as np
from scipy.integrate import Radau, solve_ivp
from scipy.integrate._ivp.common import EPS, norm, num_jac
from scipy.integrate._ivp.radau import (
    E,
    MU_COMPLEX,
    MU_REAL,
    solve_collocation_system,
)
from scipy.linalg import lu_factor, lu_solve

sys.path.insert(0, ".")
from tools import outward_step_scan as oss  # noqa: E402

beta, M_text, alpha = sys.argv[1], sys.argv[2], sys.argv[3]
M = float(M_text)
F = 0.1
H_FLOOR = 1e-11

# 1. the history, observed; keep the flagged step's start
captured = {}


class Capture(oss.Observer):
    def before(self, solver):
        pre = super().before(solver)
        self._pending = dict(
            t0=float(solver.t),
            y0=solver.y.copy(),
            f0=solver.f.copy(),
            J=solver.J.copy(),
            fun=solver.fun,
            atol=np.array(solver.atol, dtype=float, copy=True),
            rtol=float(solver.rtol),
            newton_tol=float(solver.newton_tol),
            h_abs_in=float(solver.h_abs),
            current_jac=bool(solver.current_jac),
            first_attempt=self.instances[solver._observer_index]["steps"] == 0,
        )
        return pre

    def after(self, solver, pre, message):
        n_rows = len(self.rows)
        super().after(solver, pre, message)
        # the bad kind: the first step of a restarted instance, flagged by the phi condition (P1's
        # ratio condition also fires on the field's deceleration before an apex, which is not it)
        row = self.rows[-1] if len(self.rows) > n_rows else None
        if row is not None and row["phi_condition"] and row["first"] and not captured:
            captured.update(self._pending)
            captured.update(
                h=float(solver.t) - self._pending["t0"],
                y1=solver.y.copy(),
                error_norm=float(solver.error_norm_old),
                row=self.rows[-1],
            )


spec = {"mode": "tuned", "beta": float(beta), "M": M, "alpha": float(alpha)}
import contextlib  # noqa: E402
import io  # noqa: E402
import importlib  # noqa: E402

SM = importlib.import_module("ComputeTargets.ScalarHistory")
observer = Capture(F, M)
radau, integrate = SM.Radau, SM.integrate_scalar_history
SM.Radau = oss._observed_radau_class(radau, observer)
with contextlib.redirect_stdout(io.StringIO()):
    data, units = oss.run_history(spec)
SM.Radau = radau
print(
    f"beta={beta} M={M_text} alpha={alpha}: outcome "
    f"{'failure: ' + data['failure_reason'][:90] if data.get('failure') else 'success'}"
)
if not captured:
    print("no flagged step")
    sys.exit(0)

t0, y0, f0, J, fun = (
    captured["t0"],
    captured["y0"],
    captured["f0"],
    captured["J"],
    captured["fun"],
)
atol, rtol, newton_tol = captured["atol"], captured["rtol"], captured["newton_tol"]
row = captured["row"]
print(
    f"the flagged step: N0={t0!r} h={captured['h']:.6g} h*omega={row['h_omega']:.4g} "
    f"pi_end/pi_start={row['pi_ratio']:.4g} phi0/M={row['phi_start_over_M']:.4g} "
    f"least phi/M={row['phi_min_over_M']:.4g} SciPy error_norm={captured['error_norm']:.4g} "
    f"(first step of its instance: {row['first']}; first h_abs {captured['h_abs_in']:.4g})"
)
print(f"   state at N0 (float.hex): {[float(v).hex() for v in y0]}")
eig = np.linalg.eigvals(J)
print(
    f"   J[1,0] = {J[1, 0]:.6g}, omega = sqrt|J[1,0]| = {np.sqrt(abs(J[1, 0])):.6g}; "
    f"eigenvalues of J: {', '.join(f'{e:.4g}' for e in eig)}"
)
print(
    f"   f at N0: phi'={f0[0]:.6g} pi'={f0[1]:.6g} (inward acceleration -pi' = {-f0[1]:.6g})"
)

n = len(y0)
I = np.identity(n)


def solve_lu(LU, b):
    return lu_solve(LU, b, overwrite_b=True)


def attempt(h, J_=J, y_=y0, t_=t0, f_=f0):
    """One Radau attempt at h from (t_, y_), as _step_impl makes it on an instance's first step
    (h is first rounded as _step_impl rounds it: t_new = t + h, h = t_new - t)."""
    h = (t_ + h) - t_
    LU_real = lu_factor(MU_REAL / h * I - J_)
    LU_complex = lu_factor(MU_COMPLEX / h * I - J_)
    scale = atol + np.abs(y_) * rtol
    Z0 = np.zeros((3, n))
    converged, n_iter, Z, rate = solve_collocation_system(
        fun, t_, y_, h, Z0, scale, newton_tol, LU_real, LU_complex, solve_lu
    )
    y_new = y_ + Z[-1]
    ZE = Z.T.dot(E) / h
    error = solve_lu(LU_real, f_ + ZE)
    scale = atol + np.maximum(np.abs(y_), np.abs(y_new)) * rtol
    error_norm = norm(error / scale)
    error2 = solve_lu(LU_real, fun(t_, y_ + error) + ZE)
    error_norm2 = norm(error2 / scale)
    return converged, n_iter, rate, y_new, error_norm, error_norm2, scale


def resolved(h):
    sol = solve_ivp(
        lambda t, y: fun(t, y),
        (t0, t0 + h),
        y0,
        method="Radau",
        rtol=1e-10,
        atol=[1e-30, 1e-30, 1e-12, 1e-12, 1e-12],
        max_step=h / 20000,
    )
    return sol.y[:, -1], int(np.sum(np.diff(np.sign(sol.y[1])) != 0)), sol.y[0].min()


omega = np.sqrt(abs(J[1, 0]))
hs = sorted(
    set(list(np.geomspace(1e-8, 4e-3, 27)) + [captured["h"], captured["h_abs_in"]])
)
print("\n1. single attempts from the flagged step's start (h*omega with omega at N0)")
print(
    f"   {'h':>10} {'h*omega':>9} {'conv':>4} {'it':>2} {'est':>10} {'est_refined':>11} "
    f"{'true':>10} {'true/est':>9} {'accept':>6} {'pi_end/pi0':>10} {'true pi/pi0':>11} "
    f"{'phi_end/M':>10} {'true phi/M':>10}"
)
for h in hs:
    conv, it, rate, y_new, est, est2, scale = attempt(h)
    y_true, sign_changes, _ = resolved(h)
    true_norm = norm((y_new - y_true) / scale)
    accept = conv and est <= 1.0
    print(
        f"   {h:10.4g} {h * omega:9.4g} {str(conv)[0]:>4} {it:2d} {est:10.4g} {est2:11.4g} "
        f"{true_norm:10.4g} {true_norm / est if est > 0 else float('inf'):9.3g} "
        f"{str(accept):>6} {y_new[1] / y0[1]:10.4g} {y_true[1] / y0[1]:11.6g} "
        f"{y_new[0] / M:10.4g} {y_true[0] / M:10.4g}"
    )


# 1b. SciPy's own path to the accepted step: its first attempt at the instance's h_abs, rejected on the
# plain estimate; the step cut by max(MIN_FACTOR, safety * predict_factor); the second attempt, where
# `rejected and error_norm > 1` makes SciPy recompute the estimate with f(y + err) and accept on it
from scipy.integrate._ivp.radau import (
    MIN_FACTOR,
    NEWTON_MAXITER,
    predict_factor,
)  # noqa: E402

print("\n1b. SciPy's path from the instance's first h_abs")
h1 = captured["h_abs_in"]
conv1, it1, _, _, est1, est1r, _ = attempt(h1)
safety1 = 0.9 * (2 * NEWTON_MAXITER + 1) / (2 * NEWTON_MAXITER + it1)
h2 = h1 * max(MIN_FACTOR, safety1 * predict_factor(h1, None, est1, None))
conv2, it2, _, y2, est2, est2r, _ = attempt(h2)
print(
    f"   attempt 1: h={h1:.6g} (h*omega {h1 * omega:.4g}) Newton converged {conv1} in {it1}; "
    f"plain estimate {est1:.4g} > 1: rejected (no refinement on an attempt not yet rejected)"
)
print(
    f"   attempt 2: h={h2:.6g} = {h2 / h1:.3g} x attempt 1 (h*omega {h2 * omega:.4g}); Newton converged "
    f"{conv2} in {it2}; plain estimate {est2:.4g} > 1, so SciPy recomputes it after the rejection: "
    f"{est2r:.4g} -> {'accepted' if est2r <= 1 else 'rejected'}"
)
print(
    f"   attempt 2 is the accepted step: h equal {((t0 + h2) - t0) == captured['h']} (h rounded as SciPy rounds it), endpoint equal "
    f"{bool(np.all(y2 == captured['y1']))}, SciPy's recorded error_norm {captured['error_norm']:.4g}"
)


# 2. the growth question
def fresh_omega(t, y):
    f = fun(t, y)
    Jf, _ = num_jac(
        lambda tt, yy: np.array([fun(tt, yy[:, k]) for k in range(yy.shape[1])]).T,
        t,
        y,
        f,
        atol,
        None,
        None,
    )
    return np.sqrt(abs(Jf[1, 0]))


print("\n2. a fresh Radau from the same state, growing its step (no cap, max_step 0.1)")
for first in (min(0.1, max(H_FLOOR, F * y0[0] / y0[1])), 1e-6, 4e-6, 4e-5, 1.6e-4):
    s = Radau(
        fun,
        t0,
        y0.copy(),
        t0 + 0.02,
        max_step=0.1,
        rtol=rtol,
        atol=atol,
        first_step=first,
    )
    print(f"   first_step = {first:.4g}")
    worst = None
    for k in range(40):
        ta, ya = s.t, s.y.copy()
        w = fresh_omega(ta, ya)
        msg = s.step()
        if s.status == "failed":
            print(f"      step {k}: failed: {msg}")
            break
        h = s.t - ta
        phi_s, pi_s = oss.step_samples(s.dense_output(), ta, s.t)
        cls = oss.classify_step(
            ya[0], ya[1], s.y[1], bool(np.any(pi_s <= 0)), float(phi_s.min()), F
        )
        if worst is None or h * w > worst[1]:
            worst = (k, h * w)
        if k < 14 or cls["flagged"] or k % 5 == 0:
            print(
                f"      step {k:2d}: N-N0={s.t - t0:.4g} h={h:.4g} h*omega={h * w:.4g} "
                f"error_norm={s.error_norm_old:.3g} phi/M={s.y[0] / M:.4g} pi={s.y[1]:.6g} "
                f"flagged={cls['flagged']}"
            )
        if s.status == "finished" or s.y[1] <= 0.0:
            # the apex of the outward flight: past it the field falls inwards, which the loop's
            # kinematic cap governs and this bare Radau does not, so the experiment stops there
            break
    print(f"      largest h*omega over the steps: {worst[1]:.4g} at step {worst[0]}")
