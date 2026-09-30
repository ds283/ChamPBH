"""
Scratch probe for production-readiness prompt 03 (log 03, Deviations 2 and 3). Run from the
repository root:

    ./venv/bin/python prompts/production-readiness/logs/03-probes/spline_order_probe.py

A second. Q's numerator A*C = -m + (1/2) dm/dN (H^2 ~ e^{-4N}) at the samples, error relative to
max |A*C|, for dm/dN from a cubic (k = 3, shipped) or quintic (k = 5) spline of asinh m, and for
the old log|M^2| route, at all samples and at the samples with N in [0.5, 11.5]. Histories: README
section 2 (e)'s two, and the spike history made narrower (width 0.03, 0.02) or taller (1e8).
"""

from math import log, pi

import numpy as np
from scipy.interpolate import make_interp_spline

dN = log(10) / 250
N = np.arange(0, 12 + 1e-12, dN)
w = 2 * pi / 3


def crossing(n):
    return 5 * np.sin(w * n) + 0.5, 5 * w * np.cos(w * n)


def spikes(n, width=0.05, amp=1e4):
    m, dm = 0.5 + 0 * n, 0 * n
    for Nk in [2.03, 4.51, 7.27, 9.88]:
        g = amp * np.exp(-(((n - Nk) / width) ** 2))
        m, dm = m + g, dm - 2 * (n - Nk) / width**2 * g
    return m, dm


cases = [
    ("crossing", crossing),
    ("spikes", spikes),
    ("spikes w=0.03", lambda n: spikes(n, 0.03)),
    ("spikes w=0.02", lambda n: spikes(n, 0.02)),
    ("spikes amp 1e8", lambda n: spikes(n, 0.05, 1e8)),
]
inside = (N >= 0.5) & (N <= 11.5)
for name, fn in cases:
    m, dm = fn(N)
    exact = -m + 0.5 * dm
    scale = abs(exact).max()
    out = []
    for k in (3, 5):
        d = make_interp_spline(N, np.arcsinh(m), k=k).derivative()(N)
        e = abs(-m + 0.5 * np.hypot(1, m) * d - exact) / scale
        out.append(f"asinh k={k}: all {e.max():.2e} inside {e[inside].max():.2e}")
    dl = make_interp_spline(N, np.log(np.abs(np.exp(-4 * N) * m))).derivative()(N)
    e = abs(m * (1 + 0.5 * dl) - exact) / scale
    print(
        f"{name:15s}",
        " | ".join(out),
        f"| log route: all {e.max():.2e} inside {e[inside].max():.2e}",
    )
