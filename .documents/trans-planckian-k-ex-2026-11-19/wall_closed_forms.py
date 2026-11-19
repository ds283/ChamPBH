"""
k_ex on the two walls, at matched (E, V_0, M, n) (trans-planckian-k-ex note, 2026-11-19, §1-§2).

Part 1: the classical turning point from the closed forms (units M = V_0 = 1), split into the phi_t part and the
steepness part c = phi |V'''/V''|. Part 2: the production models' classical_turning_point and one_loop_turnaround at the
wall tests' points (normalization -279.87, field 0.1 M_P; ComputeTargets/tests/test_wall_model.py). Run from the
repository root, on the power-law-wall branch (InversePowerModel): PYTHONPATH=. ./venv/bin/python <this file>.
No Ray, no store.
"""

from math import exp, log, log1p, sqrt

from ComputeTargets.tests.test_model_interface import exponential_model
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import InversePowerModel
from Units import Planck_units

print("Part 1: classical turning point, closed forms (M = V_0 = 1)")
for n in (1, 2):
    for lnEV in (10.0, 50.0, 90.0):
        EV = exp(lnEV)
        L = log1p(EV)
        phie = L ** (-1 / n)
        ce = ((n + 1) * (n + 2) + 3 * n * (n + 1) * L + n * n * L * L) / (
            (n + 1) + n * L
        )
        ke = 0.5 * sqrt(EV) * ce / phie
        we = sqrt((1 + EV) * (n * (n + 1) * L + n * n * L * L)) / phie
        phip = EV ** (-1 / n)
        cp = n + 2
        kp = 0.5 * sqrt(EV) * cp / phip
        wp = sqrt(n * (n + 1) * EV) / phip
        print(
            f"n={n} ln(E/V0)={lnEV:4.0f}  phi_t exp={phie:.3e} ipl={phip:.3e}  c exp={ce:7.2f} ipl={cp}  "
            f"ln(k_ipl/k_exp)={log(kp / ke):7.2f}  [phi part {log(phie / phip):7.2f}, c part {log(cp / ce):6.2f}]  "
            f"k/omega exp={ke / we:.4f} ipl={kp / wp:.4f}"
        )

print()
print(
    "Part 2: production models, classical vs one-loop (normalization -279.87, field 0.1 M_P; M in M_P, E in M_P^4)"
)
u = Planck_units()
Mp = u.PlanckMass
NORM = -279.87
FIELD = 0.1 * Mp


def ipl(M, n):
    return InversePowerModel(0, M_value(0, M * Mp), n, beta_value(0, 2.0), u)


for n in (1, 2):
    for M in (1e-30, 1e-20, 1e-10):
        for lnE in (-180.0, -120.0):
            s = f"n={n} M={M:.0e} lnE={lnE:.0f} ln(E/V0)={lnE - NORM:.1f}: "
            for name, mk in (
                ("exp", lambda: exponential_model(2.0, M, n)),
                ("ipl", lambda: ipl(M, n)),
            ):
                m = mk()
                try:
                    c = m.classical_turning_point(NORM, FIELD, lnE)
                    o = m.one_loop_turnaround(NORM, FIELD, lnE)
                except Exception:
                    c = o = None
                if c is None or o is None:
                    s += f"{name}: none  "
                    continue
                s += (
                    f"{name}: ln(M/phi_t)={log(M / c.phi_t):6.1f} lnk_t={c.log_k_ex:7.1f} | "
                    f"ln(M/phi_ta)={log(M / o.phi_ta):6.1f} lnk_ta={o.log_k_ex:7.1f} ell={o.ell:5.1f} "
                    f"c_ta={o.pulse_rate_ta * o.phi_ta:6.1f}  "
                )
            print(s)
