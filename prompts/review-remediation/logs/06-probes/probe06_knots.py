"""Prompt 06 scratch probe: stored samples per decade of T_J at fixed field,
250 per decade of Einstein-frame 1+z, so 250 (1 + (1/3) dln g_s/dln T) per decade of T_J.
Closed form: count over [T1, T2] = 250 [log10(T2/T1) + (1/3) log10(g_s(T2)/g_s(T1))]."""

import numpy as np
from CosmologyModels.tests.eos_reference import production_eos
from Units import GeV_units

u = GeV_units()
eos = production_eos(u)
gs = lambda T: float(eos.G_s(T * u.GeV))


def count(T1, T2):
    return 250 * (np.log10(T2 / T1) + np.log10(gs(T2) / gs(T1)) / 3)


for T1, T2, lab in [
    (2e-5, 5e-3, "[0.02, 5] MeV"),
    (1e-10, 0.1, "[0.1 eV, 100 MeV]"),
    (3e-7, 1e-2, "[0.3 keV, 10 MeV]"),
]:
    print(
        f"{lab}: {count(T1, T2):.0f} samples, {count(T1,T2)/np.log10(T2/T1):.1f} per decade on average"
    )
T = np.geomspace(2e-5, 5e-3, 4000)
loc = [250 * (1 + float(eos.dG_s_dlogT(t * u.GeV)) / gs(t) / 3) for t in T]
print(f"local density on [0.02, 5] MeV: {min(loc):.1f} to {max(loc):.1f} per decade")
