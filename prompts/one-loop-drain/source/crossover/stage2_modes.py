"""
crossover side-campaign, stage 2: the two mode equations (stage2-brief.md §4, wall_backreaction.tex §"The infrared").

  1. The scheme's mode equation (stage2.solve's default). Its soft modes are not protected: on a pulse that is not the
     linearization of its own background, a mode with k well below the crossing rate leaves with
     n_k = S^2/4k^2, S = int V'' dt (the note's §"Comparison with the figures of Erickcek et al."). Measured here:
     n_k k^2 / (S^2/4) at the lowest modes, with S integrated on the run's own trajectory, and the shares of the final
     quanta's energy below m_turn and below 0.1 k_turn.
  2. The linearization of the background equation at fixed Sigma, which adds the Hartree term K V'''' Sigma to the
     modes' omega^2 (stage2.solve(hartree=True)), at two steps.

    ../../../../venv/bin/python stage2_modes.py > out/stage2_modes.txt
"""

import sys

from multiprocessing import Pool

import numpy as np

sys.path.insert(0, ".")
import stage2  # noqa: E402
from walls import Wall  # noqa: E402

PTS = (
    ("A0.05 #35", 14.66, 35.379),
    ("A0.05 #107", 7.885, 23.833),
    ("A0.01 #183 gap", 3.619, 13.791),
    ("A0.01 #430 gap", 1.794, 7.124),
)


def task(pt):
    name, L, lnR = pt
    w = Wall(L)
    base = stage2.run_point(L, lnR)
    rec = base["rec"]
    Sint = float(np.trapezoid(w.V2(rec[:, 1]), rec[:, 0]))
    k, n, W, om = base["k"], base["n"], base["W"], base["om"]
    e = W * om * n
    m, kt = base["m_turn"], base["kex_turn"]
    out = [
        f"{name:16s} scheme   frac {base['frac_modes']:.5f}  vout/vin {base['vout']:.4f}  m_turn {m:.3e}  "
        f"k_turn {kt:.3e}  S = int V'' dt = {Sint:.4e}"
    ]
    soft = "  n_k k^2/(S^2/4) at k/m_turn = " + ", ".join(
        f"{k[i] / m:.3f}: {n[i] * k[i] ** 2 / (Sint**2 / 4):.3f}" for i in (0, 4, 8, 16)
    )
    out.append(f"{name:16s} {soft}")
    out.append(
        f"{name:16s}   share of the quanta's energy below m_turn {e[k < m].sum() / e.sum():.2e}, "
        f"below 0.1 k_turn {e[k < 0.1 * kt].sum() / e.sum():.2e}"
    )
    for h in (0.1, 0.05):
        try:
            r = stage2.solve(
                L,
                lnR,
                kmin=0.03 * base["mref"],
                kmax=30 * base["kref"],
                mref=base["mref"],
                hartree=True,
                h=h,
            )
            out.append(
                f"{name:16s} hartree h={h:4.2f}  frac(modes) {r['frac_modes']:.4e}  frac(balance) "
                f"{r['frac_balance']:.4e}  vout/vin {r['vout']:.4e}  M/phi_turn {1 / r['phi_turn']:.3f}  "
                f"max|dE/E| {r['drift']:.2e}  steps {r['steps']}"
            )
        except Exception as ex:  # noqa: BLE001
            out.append(f"{name:16s} hartree h={h:4.2f}  failed: {ex}")
    out.append(
        f"{name:16s}   (scheme: Hartree shift at the turn {base['hartree_turn'] / kt**2:.1f} k_turn^2 = "
        f"{base['hartree_turn'] / m**2:.1e} m_turn^2; eps_turn {base['eps_turn']:.2e})"
    )
    return "\n".join(out)


if __name__ == "__main__":
    print("# stage 2: the two mode equations")
    with Pool(4) as pool:
        for block in pool.imap(task, PTS):
            print(block, flush=True)
