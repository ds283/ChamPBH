"""
power-law-wall prompt 02 §1.2: the exponential stays reproducible. The copies of this directory, run on crossover's
Wall and WallN, give crossover's recorded numbers, to the digits recorded:
  S      stage1a_classical.spectrum and S_of at L = 10 (121 points): 1.3529e-03 (out/stage1a_S_of_L.txt);
  S      stage2.solve with the loop off on WallN(5, 2): 2.14335e-03 (out/stage2_n2.txt, V-A);
  Born   born.run on Wall(20.95) at ln R = 44.44, k_IR = 10 m_ta: 0.44982 (out/stage2_validation.txt, V1 #11);
  stage 2  stage2.run_point(7.885, 23.833): frac 0.71709, v_out/v_in 0.5319, M/phi_turn 1.678, 311 modes, 53265 steps
         (out/stage2_validation.txt, V4 A0.05 #107 base).

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python reproduce_exponential.py \\
        > ../../../../.documents/power-law-wall/wall-02/reproduce_exponential.txt
"""

import os
import sys
import time
from math import exp, sqrt
from multiprocessing import Pool

sys.path.insert(0, ".")
import born  # noqa: E402
import stage1a_classical  # noqa: E402
import stage2  # noqa: E402
import walls  # noqa: E402
from walls import Wall, WallN  # noqa: E402


def s_wall(_):
    import numpy as np

    ks = np.geomspace(1e-3, 10.0, 121)
    _, _, b2 = stage1a_classical.spectrum(10.0, ks, walls.EPS)
    return f"S Wall(10), stage 1a's 121 points: {stage1a_classical.S_of(ks, b2):.4e}  recorded 1.3529e-03"


def s_walln(_):
    w = WallN(5.0, 2)
    kref = float(stage2.lam(w, w.phi_t)) * sqrt(w.E) / 2.0
    r = stage2.solve(5.0, 0.0, kmin=1e-3, kmax=30 * kref, mref=1.0, loop=False, n=2)
    return (
        f"S WallN(5, 2), stage 2 loop off: {r['frac_modes']:.5e}  recorded 2.14335e-03"
    )


def born_pt(_):
    w, R = Wall(20.95), exp(44.44)
    m_ta = born.chain(w, R)[1]
    rb = born.run(w, R, 10.0 * m_ta)
    return f"Born Wall(20.95), ln R = 44.44, k_IR = 10 m_ta: {rb['Efl_final']:.5f}  recorded 0.44982"


def stage2_pt(_):
    r = stage2.run_point(7.885, 23.833)
    return (
        f"stage 2 run_point(7.885, 23.833): frac {r['frac_modes']:.5f} v_out/v_in {r['vout']:.4f} "
        f"M/phi_turn {1 / r['phi_turn']:.3f} modes {r['nk']} steps {r['steps']}  "
        f"recorded 0.71709 0.5319 1.678 311 53265"
    )


if __name__ == "__main__":
    t0 = time.time()
    print(f"# reproduce_exponential.py; start: {os.popen('uptime').read().strip()}")
    jobs = [s_wall, s_walln, born_pt, stage2_pt]
    with Pool(4) as pool:
        res = [pool.apply_async(j, (None,)) for j in jobs]
        for r in res:
            print(r.get(), flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
