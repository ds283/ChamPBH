# The 10 keV join (audit addendum §11). Scores the *corrected* temperature law
# (kappa = 1/ln10) against exact entropy conservation from 2e4 GeV, with the
# low-temperature constants as shipped, or ("patch") set in memory to the fit's own
# x -> infinity limits 3.931 / 3.383. No file is modified. Run from the repository root:
#   venv/bin/python .documents/audit-2026-09-29/low_t_join_probe.py [ship|patch]
import os, sys

sys.path.insert(0, os.getcwd())
from math import pi, log, exp
from scipy.integrate import solve_ivp
import CosmologyModels.GenericEOS.SaikawaShirai_common as C
import CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline as SP

patch = len(sys.argv) > 1 and sys.argv[1] == "patch"
if patch:
    C.LOW_T_G_S_STAR = SP.LOW_T_G_S_STAR = 3.931
    C.LOW_T_GSTAR = SP.LOW_T_GSTAR = 3.383
from Units import GeV_units
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline

u = GeV_units()
eos = Xav_EOS_spline(u)
GeV = u.GeV
T_CMB = 2.7255 * u.Kelvin / GeV


def run(kappa, T0, T1):
    def rhs(N, y):
        T = exp(y[1]) * GeV
        return [
            (1 - 3 * eos.w(T)) - 4,
            -1 / (1 + kappa * eos.dG_s_dlogT(T) / eos.G_s(T) / 3),
        ]

    ev = lambda N, y: y[1] - log(T1)
    ev.terminal = True
    y0 = [log(pi**2 / 30 * eos.G_rho(T0 * GeV) * T0**4), log(T0)]
    s = solve_ivp(rhs, [0, 200], y0, events=ev, rtol=1e-10, atol=1e-12, max_step=0.05)
    rho_th = pi**2 / 30 * eos.G_rho(T1 * GeV) * T1**4
    return s.t_events[0][0], exp(s.y_events[0][0][0]) / rho_th


exact = lambda T0, T1: log(T0 / T1) + log(eos.G_s(T0 * GeV) / eos.G_s(T1 * GeV)) / 3

T_above = 1e-5 * (1 + 1e-9) * GeV
print(
    "patched" if patch else "as shipped",
    f"| G_s(T_LO+) = {float(eos.G_s(T_above)):.6f}, G_s(T_LO) = {float(eos.G_s(1e-5 * GeV)):.6f}",
    f"| G_rho(T_LO+) = {float(eos.G_rho(T_above)):.6f}, G_rho(T_LO) = {float(eos.G_rho(1e-5 * GeV)):.6f}",
)
for T1 in [1.0, 0.1, 5e-3, 1e-3, 1e-4, 7e-5, 1e-5, T_CMB]:
    N, r = run(1 / log(10), 2e4, T1)
    print(
        f"{T1:10.3g}  corrected-law residual {N - exact(2e4, T1):+.3e}   rho_R ratio {r:.5f}"
    )
print(
    "rho_R 5 MeV -> 10 keV: corrected",
    f"{run(1 / log(10), 5e-3, 1e-5)[1]:.5f}",
    " kappa=1",
    f"{run(1.0, 5e-3, 1e-5)[1]:.5f}",
)
