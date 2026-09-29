import numpy as np
from CosmologyModels.tests.eos_reference import production_eos, integrate_temperature_law, T_INIT_GEV
X = production_eos()
Ts = np.logspace(np.log10(1e4), np.log10(1e-5), 37)
rs = []
for T1 in Ts:
    r = integrate_temperature_law(X, T_INIT_GEV, T1, with_rho=True).rho_R_ratio
    rs.append(r); print(f"{T1:10.4g} GeV  {r:.5f}")
rs = np.array(rs); i = np.argmin(rs); j = np.argmax(np.abs(rs-1))
print("min", rs[i], Ts[i], "max|dev|", rs[j]-1, Ts[j])
m = Ts <= 0.1
print("below 100 MeV: range", rs[m].min(), rs[m].max())
