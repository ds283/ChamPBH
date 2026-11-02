"""
For one history (argv: beta M alpha), through tools/history_and_bbn.py: every first step of a new Radau
solver whose start has pi > 0 (the outward step after a reflection or a drain). For each, h, h*omega with
omega = sqrt(|d f_pi / d phi|) from the Jacobian Radau built at the solver's start (h*omega/2pi is the
number of local oscillation periods in the step), pi_end/pi_start, the least phi of the step's dense
output at 50 points, and the step's cap (max_step), the solver's first h_abs and the inward acceleration
-pi' at its start. A step with pi_end/pi_start < 0.5 is flagged.

    PYTHONPATH=. ./venv/bin/python .documents/post-reflection-step-2026-11-02/scan_first_steps.py 1.5 1e-12 0.05

Patches scipy's Radau.step in this process only. Needs no Ray and no store.
"""

import contextlib
import io
import runpy
import sys

import numpy as np
from scipy.integrate import Radau

import ComputeTargets.ScalarHistory  # noqa: F401

beta, M_text, alpha = sys.argv[1:4]
M = float(M_text)
rows = []
o_step = Radau.step


def step_(self):
    first = not getattr(self, "_seen", False)
    self._seen = True
    t0, y0, cap, h_in = self.t, self.y.copy(), self.max_step, self.h_abs
    a_in = -self.f[1]
    J = self.J.copy()  # built at the solver's start
    r = o_step(self)
    if first and y0[1] > 0 and self.t > t0:
        phi = self.dense_output()(np.linspace(t0, self.t, 50))[0]
        h = self.t - t0
        rows.append(
            (
                t0,
                h,
                h * np.sqrt(abs(J[1, 0])),
                self.y[1] / y0[1],
                y0[0] / M,
                phi.min() / M,
                cap,
                h_in,
                a_in,
            )
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

result = [l for l in buf.getvalue().splitlines() if l.startswith("history ")]
R = np.array(rows)
bad = R[R[:, 3] < 0.5]
good = R[R[:, 3] >= 0.5]
print(f"beta={beta} M={M_text} alpha={alpha}: {result[0][:120] if result else '?'}")
print(
    f"   first outward steps: {len(R)}; flagged (pi_end/pi_start < 0.5): {len(bad)}; dense phi < 0: {int((R[:, 5] < 0).sum())}"
)
print(
    f"   good steps: h max {good[:, 1].max():.3g}, h*omega median {np.median(good[:, 2]):.3g} max {good[:, 2].max():.3g}, "
    f"least pi_end/pi_start {good[:, 3].min():.6g}"
)
print(
    f"   good steps: cap median {np.median(good[:, 6]):.3g} max {good[:, 6].max():.3g}; first h_abs median {np.median(good[:, 7]):.3g} "
    f"max {good[:, 7].max():.3g}; inward acceleration > 0 at {int((good[:, 8] > 0).sum())} of {len(good)}"
)
for r in bad:
    print(
        f"   flagged: N0={r[0]:.10g} h={r[1]:.3g} h*omega={r[2]:.3g} pi_end/pi_start={r[3]:.3g} "
        f"phi_start/M={r[4]:.3g} least dense phi/M={r[5]:.3g} cap={r[6]:.3g} first h_abs={r[7]:.3g} "
        f"inward acceleration={r[8]:.3g}"
    )
