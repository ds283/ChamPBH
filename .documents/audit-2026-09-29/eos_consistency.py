# Does d ln rho_R/dN = Sigma_table - 4 together with the S-S entropy temperature law
# reproduce rho_R = (pi^2/30) g_rho(T) T^4 across e+e- annihilation?
import sys, numpy as np
sys.path.insert(0, '/Users/ds283/Documents/Code/ChamPBH')
from math import pi, log, exp
from scipy.integrate import solve_ivp
from Units import GeV_units
units = GeV_units()
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
eos = Xav_EOS_spline(units)
GeV = units.GeV
def rhs(N, y):
    lnrho, lnT = y
    T = exp(lnT)*GeV
    Sigma = 1.0 - 3.0*eos.w(T)
    dlnT = -1.0/(1.0 + eos.dG_s_dlogT(T)/eos.G_s(T)/3.0)
    return [Sigma - 4.0, dlnT]
for T0_MeV, T1_MeV in [(5.0, 0.01), (100.0, 0.01), (5.0, 1.0)]:
    T0 = T0_MeV*1e-3; lnrho0 = log(pi**2/30*eos.G_rho(T0*GeV)*T0**4)
    def stop(N, y): return y[1] - log(T1_MeV*1e-3)
    stop.terminal = True
    sol = solve_ivp(rhs, [0, 50], [lnrho0, log(T0)], events=stop, rtol=1e-10, atol=1e-12, dense_output=True)
    lnrho, lnT = sol.y[:, -1]; T = exp(lnT)
    thermo = pi**2/30*eos.G_rho(T*GeV)*T**4
    print(f"{T0_MeV:6g} MeV -> {T1_MeV:5g} MeV: integrated rho_R / thermodynamic rho_R = {exp(lnrho)/thermo:.5f}   (N elapsed {sol.t[-1]:.3f})")
# Also: what w would be thermodynamically consistent with the S-S g's, vs table, at a few T
print("T[MeV]   Sigma_table   Sigma_SS=4(1-gs/grho)")
for Tm in [3, 1, 0.5, 0.2, 0.1, 0.05, 0.02]:
    T = Tm*1e-3*GeV
    print(f"{Tm:6g}   {1-3*eos.w(T):9.4f}   {4*(1-eos.G_s(T)/eos.G_rho(T)):9.4f}")
