# Orchestrator's probe, 2026-09-30, for prompt 05's review. Not prompt 05's work.
# Peaks of Sigma = 1 - 3w with the base-class formula w = 4 g_s / (3 g_rho) - 1,
# through SaikawaShirai_EOS_spline.w, in prompt 05's QCD and EW windows at
# 1000 points per decade. Above 2 MeV that class's w is the base formula.
import numpy as np
from Units import GeV_units
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline

u = GeV_units()
e = SaikawaShirai_EOS_spline(u)
for name, lo, hi in (("QCD", 5e-2, 1.0), ("EW", 20.0, 1e3)):
    Ts = np.logspace(np.log10(lo), np.log10(hi), int(1000 * np.log10(hi / lo)))
    S = np.array([1.0 - 3.0 * float(e.w(T * u.GeV)) for T in Ts])
    i = int(np.argmax(S))
    print(f"{name}: base-formula Sigma peak {S[i]:.5f} at {Ts[i]:.5g} GeV")
