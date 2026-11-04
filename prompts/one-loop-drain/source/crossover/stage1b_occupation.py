"""
crossover side-campaign, stage 1b: the occupation numbers n_k on the Born trajectories of stage1b_born.py (2197 at
A = 0.05), Born formula |FT V''(2k)|^2/4k^2 and [massive modes] at k = m, 3m, k_ex/3, k_ex, 3k_ex, with the
energy-weighted mean n and the k-range holding 10-90% of the produced energy. Asked by the user, 2026-11-04: do
the occupations stay O(1) or below as the dissipation approaches 1?

    ../../../../venv/bin/python stage1b_occupation.py > out/stage1b_occupation.txt
"""

import sys

sys.path.insert(0, ".")
import numpy as np
from math import exp, pi
from walls import Wall
import born

pts = [
    (20.95, 44.44),
    (11.73, 30.716),
    (9.70, 27.227),
    (8.767, 25.522),
    (7.885, 23.833),
    (6.793, 21.613),
]
print(
    "  ell  Efl/E | n(k) Born [massive] at k = m, 3m, kex/3, kex, 3kex | energy-weighted mean n (Born), k-range of energy"
)
for L, lnR in pts:
    w = Wall(L)
    R = exp(lnR)
    res = born.run_self_consistent(w, R)
    m = res["m_turn"]
    kex = res["chain"]["kex"]
    ell = res["chain"]["ell"]
    t = res["t"]
    vpp = w.V2(res["phi"])
    nB = lambda k: abs(np.trapezoid(vpp * np.exp(-2j * k * t), t)) ** 2 / (4 * k * k)
    tr = born.Trajectory(w, res)
    out = []
    for k in (m, 3 * m, kex / 3, kex, 3 * kex):
        b = nB(k)
        mm = born.massive_beta2(tr, k)[0] if res["returned"] else float("nan")
        out.append(f"{b:.2e}[{mm:.2e}]")
    ks = np.geomspace(m, 30 * kex, 400)
    n = np.array([nB(k) for k in ks])
    wgt = ks**3 * n
    nbar = np.trapezoid(wgt * n, ks) / np.trapezoid(wgt, ks)
    c = np.cumsum(wgt * np.gradient(ks))
    c /= c[-1]
    lo, hi = ks[np.searchsorted(c, 0.1)], ks[np.searchsorted(c, 0.9)]
    print(
        f"{ell:5.2f} {res['Efl_final']:6.3f} | "
        + "  ".join(out)
        + f" | {nbar:.2e}  [{lo/kex:.2f},{hi/kex:.2f}] kex"
    )
