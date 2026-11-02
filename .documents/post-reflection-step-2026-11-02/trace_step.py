"""
One failing history through tools/history_and_bbn.py, instrumented without changing the code:

  1. the last loop passes before the failure: the step cap, the solver state when the cap is set, and
     each Radau step (t_old -> t, its max_step and incoming h_abs, the state after it); reflections;
  2. the parking cut's ratio tests at the last reflections;
  3. the failing step (the last Radau step starting below 1e-11 M_P with h > 1e-6): Radau's endpoint and
     its dense output at 12 points, against the same step re-integrated from the same state, on the
     same right-hand side, with max_step = h/20000.

    PYTHONPATH=. ./venv/bin/python .documents/post-reflection-step-2026-11-02/trace_step.py 1.5 1e-12 0.05

Patches ScalarField.notify_step_cap and notify_reflection, the module's parking_ratio_test and
scipy's Radau.step, in this process only. Needs no Ray and no store.
"""

import collections
import contextlib
import io
import runpy
import sys

import numpy as np
from scipy.integrate import Radau, solve_ivp

import ComputeTargets.ScalarHistory  # noqa: F401
import Quadrature.supervisors.ScalarField as SF

SH = sys.modules["ComputeTargets.ScalarHistory"]
beta, M_text, alpha = sys.argv[1:4]
M = float(M_text)

log = collections.deque(maxlen=14)
cuts = []

cls = [
    c
    for c in vars(SF).values()
    if isinstance(c, type) and hasattr(c, "notify_step_cap")
][0]
o_cap, o_ref = cls.notify_step_cap, cls.notify_reflection


def cap_(self, cap):
    L = sys._getframe(1).f_locals
    s = L["solver"]
    log.append(
        f"cap   {cap:.6g}  N={s.t:.10g} phi/M={s.y[0] / M:.6g} pi={s.y[1]:.6g} "
        f"a_in={-s.f[1]:.6g} h_abs={s.h_abs:.6g}"
    )
    return o_cap(self, cap)


def ref_(self, N):
    L = sys._getframe(1).f_locals
    log.append(
        f"reflection N={float(N):.10g} kind={L.get('kind')} phi/M={L['phi'] / M:.6g} pi_in={L['pi_']:.6g}"
    )
    return o_ref(self, N)


cls.notify_step_cap, cls.notify_reflection = cap_, ref_

o_ratio = SH.parking_ratio_test


def ratio_(index, ratio):
    r = o_ratio(index, ratio)
    cuts.append((index, ratio, r))
    return r


SH.parking_ratio_test = ratio_

big = {}
o_step = Radau.step


def step_(self):
    t0, y0, ms, h0 = self.t, self.y.copy(), self.max_step, self.h_abs
    r = o_step(self)
    log.append(
        f"step  N {t0:.14g} -> {self.t:.14g} (h={self.t - t0:.6g}, max_step={ms:.6g}, h_abs in={h0:.6g}) "
        f"-> phi/M={self.y[0] / M:.6g} pi={self.y[1]:.6g}"
    )
    if self.t - t0 > 1e-6 and y0[0] < 1e-11:
        big.update(
            t0=t0,
            y0=y0,
            t1=self.t,
            y1=self.y.copy(),
            fun=self.fun,
            dense=self.dense_output(),
        )
    return r


Radau.step = step_

sys.argv = [
    "history_and_bbn.py",
    "--alpha",
    alpha,
    "--wall-clock-limit",
    "600",
    beta,
    M_text,
]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try:
        runpy.run_path("tools/history_and_bbn.py", run_name="__main__")
    except SystemExit:
        pass
Radau.step = o_step

print(f"beta={beta} M={M_text} alpha={alpha}")
print(
    "".join(l + "\n" for l in buf.getvalue().splitlines() if l.startswith("history ")),
    end="",
)
print("\n1. the last loop passes")
for line in log:
    print("  ", line)
print(
    "\n2. the parking cut's ratio test at the last three reflections (index, rho_kin/rho_R, passed)"
)
for c in cuts[-3:]:
    print("  ", c)
print("\n3. the failing step")
print(f"   N {big['t0']:.14g} -> {big['t1']:.14g}, h = {big['t1'] - big['t0']:.4g}")
print(
    f"   start            phi = {big['y0'][0]:.6e} M_P ({big['y0'][0] / M:.4g} M), pi = {big['y0'][1]:.6e}"
)
print(
    f"   Radau's endpoint phi = {big['y1'][0]:.6e} M_P ({big['y1'][0] / M:.4g} M), pi = {big['y1'][1]:.6e}"
)
print(
    "   Radau's dense output, phi/M at 12 points:",
    " ".join(
        f"{big['dense'](t)[0] / M:.3g}" for t in np.linspace(big["t0"], big["t1"], 12)
    ),
)
ref = solve_ivp(
    lambda t, y: big["fun"](t, y),
    (big["t0"], big["t1"]),
    big["y0"],
    method="Radau",
    rtol=1e-10,
    atol=[1e-30, 1e-30, 1e-12, 1e-12, 1e-12],
    max_step=(big["t1"] - big["t0"]) / 20000,
)
print(
    f"   resolved         phi = {ref.y[0, -1]:.6e} M_P ({ref.y[0, -1] / M:.4g} M), pi = {ref.y[1, -1]:.6e} "
    f"({ref.message})"
)
print(
    f"   resolved: phi/M from {ref.y[0].min() / M:.4g} to {ref.y[0].max() / M:.4g}; pi from {ref.y[1].min():.6g} "
    f"to {ref.y[1].max():.6g}; sign changes of pi: {int(np.sum(np.diff(np.sign(ref.y[1])) != 0))}"
)
print(
    "   the other three components, Radau minus resolved:",
    " ".join(f"{d:.3g}" for d in big["y1"][2:] - ref.y[2:, -1]),
)
