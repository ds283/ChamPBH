"""
crossover side-campaign, stage 1a: a massless-out test field reflecting off the classical wall, for L from the
first reflections (40-134, production_energy_ratio.tex Table tab:beta) down to the cascade's late bounces (L of a
few), with the wall measured from its floor (walls.py).

For each L: |beta_k|^2 on a log grid of k/omega_t, its peak, and

    S(L) = rho_beta / rho_sat = 4 int (k/omega_t)^3 |beta_k|^2 dk/omega_t      (eq:S)

with rho_beta = (1/2 pi^2) int k^3 |beta_k|^2 dk. A check row at L = 70 repeats the note's |beta|^2 at k = 0.3,
and the integral is repeated at a stricter EPS (the adiabaticity at which modes start and are projected).

    cd prompts/one-loop-drain/source/crossover && ../../../../venv/bin/python stage1a_classical.py > out/stage1a_S_of_L.txt
"""

import sys
import time

import numpy as np

sys.path.insert(0, ".")
import walls  # noqa: E402
from walls import Bounce, Wall, beta_squared  # noqa: E402

LS = (
    0.3,
    0.5,
    0.7,
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    10.0,
    13.0,
    16.0,
    20.0,
    30.0,
    40.0,
    70.0,
    90.0,
    110.0,
    134.0,
)
TABLE = {
    40.0: 0.22,
    70.0: 0.24,
    90.0: 0.25,
    110.0: 0.25,
    134.0: 0.26,
}  # the note's L^2 S


def spectrum(L, ks, eps):
    w = Wall(L)
    starts = [w.start_phi(k, eps) for k in ks]
    b = Bounce(w, max(starts))
    b2 = np.array([beta_squared(b, w.V2, w.V3, k, p) for k, p in zip(ks, starts)])
    return w, b, b2


def S_of(ks, b2):
    return 4.0 * np.trapezoid(ks**3 * b2, ks)


def main():
    ks = np.geomspace(1e-3, 10.0, 121)
    print(
        "# stage 1a: classical bounce off U = c(e^{1/phi} - 1), M = omega_t = 1; k/omega_t in [1e-3, 10], 121 points"
    )
    print(f"# EPS = {walls.EPS}, rtol = {walls.RTOL}, atol = {walls.ATOL}")
    print(
        "#     L   ln(E/V0)  |beta|^2_max  k_peak   L^2|b|^2max       S(L)     L^2 S   note L^2S   S(EPS/100)  wall(s)"
    )
    for L in LS:
        t = time.time()
        w, b, b2 = spectrum(L, ks, walls.EPS)
        S = S_of(ks, b2)
        i = int(np.argmax(b2))
        # the stricter EPS, on a coarser grid, as a convergence check of S
        ks2 = ks[::4]
        _, _, b2s = spectrum(L, ks2, walls.EPS / 100.0)
        S_coarse = S_of(ks2, b2[::4])
        S_strict = S_of(ks2, b2s)
        conv = S_strict / S_coarse * S  # S at the stricter EPS, scaled to the fine grid
        print(
            f"{L:7.1f} {np.log(np.expm1(L)):9.3f} {b2[i]:12.4e} {ks[i]:7.3f} {L*L*b2[i]:12.4f} {S:12.4e} "
            f"{L*L*S:8.4f} {TABLE.get(L, float('nan')):10.2f} {conv:12.4e} {time.time()-t:8.1f}",
            flush=True,
        )
    w = Wall(70.0)
    p = w.start_phi(0.3)
    print(
        f"# check: L = 70, k = 0.3: |beta|^2 = {beta_squared(Bounce(w, p), w.V2, w.V3, 0.3, p):.6e} (note: 2.32525e-4)"
    )


if __name__ == "__main__":
    main()
