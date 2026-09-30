"""
Scratch probe for production-readiness prompt 03 (log 03, Deviations 1). Run from the
repository root:

    PYTHONPATH=. ./venv/bin/python prompts/production-readiness/logs/03-probes/join_window_probe.py

About 20 s. Scores SaikawaShirai_EOS_spline.dw_dlogT against a central difference of its own w
near the 120 MeV branch join, on a dense grid, at half-steps 1e-4 and 1e-5 and with Richardson
extrapolation, and maps where the h = 1e-4 witness exceeds a set of thresholds above 2 MeV.
"""

import numpy as np

from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline
from Units import GeV_units

u = GeV_units()
G = u.GeV
eos = SaikawaShirai_EOS_spline(u)


def cd(T, h):
    return (float(eos.w(T * np.exp(h) * G)) - float(eos.w(T * np.exp(-h) * G))) / (
        2 * h
    )


Ts = np.exp(np.linspace(np.log(0.08), np.log(0.18), 20001))
for h in [1e-4, 1e-5]:
    d = np.array([eos.dw_dlogT(T * G) - cd(T, h) for T in Ts])
    i = np.argmax(abs(d))
    print(
        f"[80, 180] MeV, h = {h:g}: max |dw_dlogT - central| = {abs(d[i]):.3e} at {Ts[i]:.6g} GeV"
    )
d = np.array([eos.dw_dlogT(T * G) - (4 * cd(T, 5e-5) - cd(T, 1e-4)) / 3 for T in Ts])
print(f"[80, 180] MeV, Richardson (h = 1e-4, 5e-5): max {abs(d).max():.3e}")

Ts = np.exp(np.linspace(np.log(2.001e-3), np.log(2e4), 200001))
d = np.array([abs(eos.dw_dlogT(T * G) - cd(T, 1e-4)) for T in Ts])
print(
    f"(2 MeV, 20 TeV], 200001 points, h = 1e-4: max {d.max():.4e} at {Ts[np.argmax(d)]:.6g} GeV"
)
for thr in [1e-6, 1e-7]:
    s = Ts[d > thr]
    print(f"  exceeds {thr:g} on [{s.min():.5g}, {s.max():.5g}] GeV ({len(s)} points)")
