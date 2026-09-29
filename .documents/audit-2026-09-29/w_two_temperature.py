# Why w = 4 g_s / (3 g_rho) - 1 fails below neutrino decoupling (audit addendum §11).
# The Saikawa-Shirai low-T branch encodes T_nu/T through S(x) = 1 + (7/4) f_s(m_e/T),
# (T_nu/T)^3 = (4/11) S(x), in its neutrino terms 1.353 S^{4/3} (g_rho) and 1.923 S (g_s).
# The enthalpy is sum_i s_i T_i, so the neutrino entropy must be weighted by T_nu/T:
#   1 + w = (4/3) [ g_s,plasma + g_s,nu (T_nu/T) ] / g_rho          ("two-T")
# or, using (rho + p)_nu = (4/3) rho_nu exactly,
#   1 + w = (4/3) [ g_s,plasma + g_rho,nu ] / g_rho                  ("nu enthalpy")
# Both are compared with the naive formula and with Xav's table (production w).
# Run from the repository root:
#   venv/bin/python .documents/audit-2026-09-29/w_two_temperature.py
import os, sys

sys.path.insert(0, os.getcwd())
from CosmologyModels.GenericEOS import SaikawaShirai_common as C
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

u = GeV_units()
X = Xav_EOS_spline(u)

print(f"{'T[MeV]':>8} {'naive':>8} {'two-T':>8} {'nu-enth':>8} {'Xav':>8} {'Tnu/T':>7}")
for T in [
    1e-2,
    5e-3,
    2e-3,
    1e-3,
    5e-4,
    3e-4,
    1.58e-4,
    1e-4,
    7e-5,
    5e-5,
    3e-5,
    2e-5,
    1.0001e-5,
]:
    S = C._S_fit(C.M_e / T)
    g_rho, g_s = C._raw_G_rho(T), C._raw_G_s(T)
    g_s_nu, g_rho_nu = 1.923 * S, 1.353 * S ** (4 / 3)
    r = (4 * S / 11) ** (1 / 3)
    naive = 4 * g_s / (3 * g_rho) - 1
    two_T = 4 * ((g_s - g_s_nu) + g_s_nu * r) / (3 * g_rho) - 1
    nu_enth = 4 * ((g_s - g_s_nu) + g_rho_nu) / (3 * g_rho) - 1
    print(
        f"{T*1e3:8.4g} {naive:8.4f} {two_T:8.5f} {nu_enth:8.5f} {float(X.w(T*u.GeV)):8.5f} {r:7.4f}"
    )

print(
    f"fit limits at T_LO: g_s = {C._raw_G_s(1e-5):.6f}, g_rho = {C._raw_G_rho(1e-5):.6f}"
)
print("N_eff   g_rho   g_s with (N_eff/3)^(3/4)   g_s linear in N_eff")
for N in [3.042, 3.044, 3.046]:
    print(
        f"{N:5.3f} {2 + 1.75 * N * (4 / 11) ** (4 / 3):8.4f} "
        f"{2 + 1.75 * 3 * (4 / 11) * (N / 3) ** 0.75:14.4f} {2 + 1.75 * N * (4 / 11):18.4f}"
    )
