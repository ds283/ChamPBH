import numpy as np
from CosmologyModels.tests.eos_reference import production_eos, integrate_temperature_law, T_INIT_GEV
X = production_eos(); G = X._units.GeV
for T1 in (31.62, 0.1778):
    for ms in (0.05, 0.01):
        print(T1, ms, integrate_temperature_law(X, T_INIT_GEV, T1, with_rho=True, max_step=ms).rho_R_ratio)
def implied(T):
    t = T*G
    grho, gs = float(X.G_rho(t)), float(X.G_s(t))
    a = float(X.dG_rho_dlogT(t))/grho; b = float(X.dG_s_dlogT(t))/gs
    threew1 = (4 + a)/(1 + b/3)   # 3(1+w)
    return 1 - 3*(threew1/3 - 1)
print(f"{'T GeV':>10s} {'Sigma table':>12s} {'Sigma from g':>12s}")
for T in (300, 100, 56.23, 30, 10, 3, 1, 0.5, 0.3, 0.2, 0.178, 0.15, 0.13, 0.11, 0.1, 0.05, 0.01, 3e-3, 1e-3, 5e-4, 2e-4, 1e-4, 5e-5):
    print(f"{T:10.4g} {1-3*float(X.w(T*G)):12.5f} {implied(T):12.5f}")
Ts = np.logspace(np.log10(1e-5), np.log10(2e4), 20000)
d = np.array([1-3*float(X.w(T*G)) - implied(T) for T in Ts])
print("int (Sigma_table - Sigma_g) d lnT over [10 keV, 2e4 GeV]:", np.trapezoid(d, np.log(Ts)))
for lo, hi in ((20, 2e4), (0.13, 20), (0.11, 0.13), (1e-5, 0.11)):
    m = (Ts>=lo)&(Ts<=hi); print(lo, hi, "max |dSigma|", np.abs(d[m]).max(), "at", Ts[m][np.argmax(np.abs(d[m]))])
