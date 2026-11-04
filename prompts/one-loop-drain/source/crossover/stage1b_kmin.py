"""
crossover side-campaign, stage 1b check: the massive-mode production against the lower end of its k grid, on three
of stage1b_born.py's trajectories (ell = 9.5, 7.4, 3.3), with the adiabaticity of the lowest mode at the
trajectory's start. Modes far below m_ta start non-adiabatically and give spurious production; from k = 0.1 m_ta
up the result is stable to the fourth digit and equals the scheme's own E_fl.

    ../../../../venv/bin/python stage1b_kmin.py > out/stage1b_kmin.txt
"""

import sys

sys.path.insert(0, ".")
import numpy as np
from math import exp, pi, sqrt
from walls import Wall
import born

for L, lnR in ((26.74, 52.151), (20.95, 44.44), (10.83, 29.211)):
    w = Wall(L)
    R = exp(lnR)
    res = born.run_self_consistent(w, R)
    tr = born.Trajectory(w, res)
    m = res["m_turn"]
    kex = res["chain"]["kex"]
    # adiabaticity of mode k at the trajectory start
    p0, pd0 = res["phi"][0], res["pd"][0]
    for kmin in (1e-3, 1e-2, 1e-1, 0.3):
        ks = np.geomspace(kmin * m, 30 * kex, 160)
        out = [born.massive_beta2(tr, k) for k in ks]
        b2 = np.array([o[0] for o in out])
        wo = np.array([o[1] for o in out])
        f = np.trapezoid(ks**2 * wo * b2, ks) / (2 * pi**2) * 8 * pi**2 * R
        eps0 = abs(w.V3(p0) * pd0) / (2 * (kmin * m) ** 3)
        print(
            f"L={L} ell={res['chain']['ell']:.3f} kmin={kmin} m: massive={f:.4f}  Efl={res['Efl_final']:.4f}  start adiabaticity at kmin={eps0:.2e}"
        )
