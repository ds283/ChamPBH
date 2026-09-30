"""
Planning probe for the production-readiness campaign, prompt 03 (the A3 amendment).
Taken on b0e46bc, 2026-09-30. Run from the repository root:

    ./venv/bin/python prompts/production-readiness/planning-probes/q_sign_change_probe.py

A few seconds. Scratch code, not a test.

Q's numerator is A*C = (M2/H2)(1 + (1/2) d ln|M2|/dN) = M2/H2 + (1/2)(dM2/dN)/H2, which
is smooth through M2 = 0. compute_adiabatic_values gets it by splining log|M2| against N
(make_interp_spline, k = 3) and differentiating, which is singular where M2 changes sign.
Two alternatives spline a function of m = M2/H2 and use
    A*C = m (1 + Hdot/H2) + (1/2) dm/dN:
  "m-spline":     spline m itself;
  "asinh-spline": spline asinh(m), which is linear near 0 and logarithmic at large |m|,
                  and recover dm/dN = sqrt(1 + m^2) d asinh(m)/dN.

Synthetic radiation-era history, H2 ~ e^{-4N} (Hdot/H2 = -2):
  case "crossing": m(N) = 5 sin(2 pi N / 3) + 0.5, which changes sign every 1.5 e-folds;
  case "spikes":   m(N) = 0.5 + 1e4 exp(-[(N - N_k)/0.05]^2) summed over rebounds, which is
                   the dynamic range of a bounce, with no sign change.
Sampling dN = ln 10 / 250, the production default of 250 samples per decade of z.
"""

import numpy as np
from scipy.interpolate import make_interp_spline

dN = np.log(10.0) / 250.0
N = np.arange(0.0, 12.0 + 1e-12, dN)
Nf = np.linspace(0.5, 11.5, 400001)


def crossing(n):
    return 5 * np.sin(2 * np.pi * n / 3) + 0.5, 5 * (2 * np.pi / 3) * np.cos(
        2 * np.pi * n / 3
    )


def spikes(n):
    m, dm = 0.5 + 0 * n, 0 * n
    for Nk in [2.03, 4.51, 7.27, 9.88]:
        g = 1e4 * np.exp(-(((n - Nk) / 0.05) ** 2))
        m, dm = m + g, dm - 2 * (n - Nk) / 0.05**2 * g
    return m, dm


for name, fn in [("crossing", crossing), ("spikes", spikes)]:
    m_s, _ = fn(N)
    m_f, dm_f = fn(Nf)
    exact = m_f * (1 - 2) + 0.5 * dm_f
    scale = np.abs(exact).max()

    d_log = make_interp_spline(N, np.log(np.abs(np.exp(-4 * N) * m_s))).derivative()
    code = m_f * (1 + 0.5 * d_log(Nf))

    d_m = make_interp_spline(N, m_s).derivative()
    smooth = m_f * (1 - 2) + 0.5 * d_m(Nf)

    d_a = make_interp_spline(N, np.arcsinh(m_s)).derivative()
    asinh = m_f * (1 - 2) + 0.5 * np.sqrt(1 + m_f**2) * d_a(Nf)

    print(
        f"{name:9s} max|A*C| {scale:9.3g} | error / max|A*C|: log-spline"
        f" {np.abs(code - exact).max() / scale:9.3g}, m-spline"
        f" {np.abs(smooth - exact).max() / scale:9.3g}, asinh-spline"
        f" {np.abs(asinh - exact).max() / scale:9.3g}"
    )
