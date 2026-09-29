# Temperature law d lnT/dN = -1/(1 + kappa * dG_s_dlogT/G_s/3): kappa=1 is what ODERHS does with the
# spline EOS (derivative w.r.t. log10 T); kappa=1/ln10 converts to d/dlnT. Compare with exact
# entropy conservation  T a g_s^{1/3} = const.
import sys, numpy as np
sys.path.insert(0, '/Users/ds283/Documents/Code/ChamPBH')
from math import pi, log, exp
from scipy.integrate import solve_ivp
from Units import GeV_units
units = GeV_units()
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
eos = Xav_EOS_spline(units); GeV = units.GeV
T_init = 2e4; T_CMB = 2.7255*units.Kelvin/GeV
LN10 = log(10.)
def make_rhs(kappa):
    def rhs(N, y):
        lnrho, lnT = y; T = exp(lnT)*GeV
        Sigma = 1.0 - 3.0*eos.w(T)
        return [Sigma - 4.0, -1.0/(1.0 + kappa*eos.dG_s_dlogT(T)/eos.G_s(T)/3.0)]
    return rhs
def run(kappa, T_end, T0=T_init):
    lnrho0 = log(pi**2/30*eos.G_rho(T0*GeV)*T0**4)
    def stop(N, y): return y[1] - log(T_end)
    stop.terminal = True
    sol = solve_ivp(make_rhs(kappa), [0, 200], [lnrho0, log(T0)], events=stop, rtol=1e-10, atol=1e-12, max_step=0.05)
    return sol.t[-1], exp(sol.y[0, -1])
def exact_N(T0, T1): return log(T0/T1) + log(eos.G_s(T0*GeV)/eos.G_s(T1*GeV))/3.0
print("e-folds from T_init=2e4 GeV to various T (exact entropy conservation vs code's law vs corrected):")
print(f"{'T_end':>10s} {'exact':>9s} {'kappa=1':>9s} {'kappa=1/ln10':>13s}   rho_int/rho_thermo(kappa=1)  (kappa=1/ln10)")
for T_end in [1.0, 0.1, 5e-3, 1e-3, 1e-4, 7e-5, 1e-5, T_CMB]:
    N1, r1 = run(1.0, T_end); N2, r2 = run(1/LN10, T_end)
    th = pi**2/30*eos.G_rho(T_end*GeV)*T_end**4
    print(f"{T_end:10.3g} {exact_N(T_init,T_end):9.4f} {N1:9.4f} {N2:13.4f}   {r1/th:12.4f}  {r2/th:14.4f}")
