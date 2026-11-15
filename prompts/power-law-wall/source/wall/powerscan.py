"""
power-law-wall prompt 02: what the power wall's scans share. Imported by the scripts of this directory, which run from
it as crossover's did.

- The curves. On a history at fixed M (prompt §1.1, checked against every mapped candidate in §1.4),
      ln R = a + (1 + 4/n) ln x,   a = ln(V0/M^4) + 2 ln(n(n + 1)) - ln 8 pi^2,   x = E/V0.
  CURVES holds ln(V0/M^4) of the three curves the map measured (the tuned ln V0 = -276.6307286 less 4 ln M).
- The chain. born.chain on PowerWall(x, n) in units of phi_t (walls.py), with the floor: ell as the production computes
  it at that (x, R) (validated against InversePowerModel.one_loop_turnaround, validate item 5). born.chain on
  PowerWall(inf, n), the wall with no floor, gives ell_inf(R), the same chain with lambda = n/phi.
- run_point_power: stage2.solve on PowerWall(x, n), as stage2.run_point runs it, but with the mode grid and the start
  threshold set from the physical scales of the bounce, which the floor does not change: the chain's m_ta and k_ex of
  the wall with no floor (or omega_t = 1 and n sqrt(E)/2, the floor-free k_ex at phi_t, where it has no chain),
  EPS lowered as e^{-2(ell_inf - 8)} above ell_inf = 8, and the second and third passes set from the turn's mass and
  floor-free crossing rate n sqrt(E)/(2 phi_turn). The equations stage 2 integrates read U and its derivatives only,
  which do not depend on x in units of phi_t (walls.PowerWall); x enters run_point's references through lambda, which
  the floor suppresses at small x, so that run_point's grid would follow the floor and not the bounce (validate item 6
  measures this).
- The adopted law, f_law: drain_fraction's formula and constants (ComputeTargets/ScalarHistory.py), term for term;
  chain_vs_production.py checks it against drain_fraction at the scan's own inputs.
"""

from math import exp, log, pi, sqrt

import born
import stage2
from walls import PowerWall

INF = float("inf")
LOG_8PI2 = log(8.0 * pi * pi)

# ln(V0/M^4) of the mapped curves (.documents/power-law-wall/wall-02/map_summary.txt, every history at that M)
CURVES = {"4e-31": 3.344645, "1e-33": 27.310504, "1e-15": -138.475623}

# the adopted law's constants (ComputeTargets/ScalarHistory.py, DRAIN_*), and the S table's (L, S)
A1, B0, C1, ELL_PLATEAU, F_CAP, F_PLATEAU = 2.69, 0.9691, 0.41, 1.0, 0.95, 0.9
GAP_G, GAP_R0, GAP_P, GAP_Q = 0.0198, 1.5601, 1.5472, 0.841
S_TABLE_L = (
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
)
S_TABLE_L += (110.0, 134.0)
S_TABLE_S = (
    1.0041e-02,
    9.7145e-03,
    9.3185e-03,
    8.6688e-03,
    6.6167e-03,
    5.0735e-03,
    3.9750e-03,
    3.1852e-03,
)
S_TABLE_S += (
    2.6041e-03,
    2.1664e-03,
    1.8292e-03,
    1.3529e-03,
    9.2294e-04,
    6.6916e-04,
    4.6603e-04,
    2.3302e-04,
)
S_TABLE_S += (1.3936e-04, 4.9353e-05, 3.0600e-05, 2.0810e-05, 1.4204e-05)


def curve_a(log_V0_M4: float, n: int) -> float:
    return log_V0_M4 + 2.0 * log(n * (n + 1)) - LOG_8PI2


def lnx_on_curve(log_V0_M4: float, n: int, lnR: float) -> float:
    return (lnR - curve_a(log_V0_M4, n)) / (1.0 + 4.0 / n)


def chain_ell(x: float, n: int, lnR: float):
    ch = born.chain(PowerWall(x, n), exp(lnR))
    return None if ch is None else ch[3]


def regime(x: float, n: int, lnR: float):
    """(regime, ell) as drain_regime classifies a candidate: classical at ln R <= 0, one_loop where the chain has a
    solution with ell > 0, else gap."""
    if lnR <= 0.0:
        return "classical", None
    ell = chain_ell(x, n, lnR)
    return ("one_loop", ell) if ell is not None else ("gap", None)


def refs(n: int, lnR: float):
    """(m_ref, k_ref, ell_inf): the floor-free chain's m_ta and k_ex, or omega_t and the floor-free k_ex at phi_t."""
    w0 = PowerWall(INF, n)
    ch = born.chain(w0, exp(lnR))
    if ch is not None:
        return ch[1], ch[2], ch[3]
    return 1.0, n * sqrt(w0.E) / 2.0, None


def run_point_power(x, n, lnR, cmin=0.03, cmax=30.0, ppd=80, iterate=True, **kw):
    w = PowerWall(x, n)
    mref, kref, ell_inf = refs(n, lnR)
    if ell_inf is not None and ell_inf > 8.0 and "eps" not in kw:
        kw = dict(kw, eps=stage2.EPS * exp(-2.0 * (ell_inf - 8.0)))
    for it in range(3):
        res = stage2.solve(
            w.L,
            lnR,
            kmin=cmin * mref,
            kmax=cmax * kref,
            ppd=ppd,
            mref=mref,
            wall=w,
            **kw,
        )
        res["mref"], res["kref"], res["passes"], res["ell_inf"] = (
            mref,
            kref,
            it + 1,
            ell_inf,
        )
        res["kex_turn_free"] = n * sqrt(w.E) / (2.0 * res["phi_turn"])
        if not iterate:
            break
        mt, kt = res["m_turn"], res["kex_turn_free"]
        if 1 / 1.5 <= mt / mref <= 1.5 and 1 / 1.5 <= kt / kref <= 1.5:
            break
        mref, kref = mt, kt
    return res


def S_of_table(L: float) -> float:
    """drain_S_of_L (the exponential's S(L), production's table and continuation)."""
    import numpy as np

    if L > S_TABLE_L[-1]:
        return S_TABLE_S[-1] * (S_TABLE_L[-1] / L) ** 2
    return float(np.exp(np.interp(log(L), np.log(S_TABLE_L), np.log(S_TABLE_S))))


def f_law(reg, ell, L, lnR, S=None):
    """drain_fraction (ComputeTargets/ScalarHistory.py), term for term; S overrides S(L) in the classical regime."""
    if reg == "one_loop":
        if ell < ELL_PLATEAU:
            return F_PLATEAU
        d = ell + B0 - C1 * log(L)
        return min(F_CAP, 1.0 - exp(-A1 / d)) if d > 0.0 else F_CAP
    if reg == "gap":
        if lnR <= GAP_R0:
            return 0.0
        s = 1.0 + GAP_Q * log(L)
        if s <= 0.0:
            return 0.0
        return 1.0 - exp(-GAP_G * (lnR - GAP_R0) ** GAP_P * s)
    if reg == "classical":
        return exp(lnR) * (S_of_table(L) if S is None else S)
    raise ValueError(reg)
