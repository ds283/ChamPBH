"""
Compare the notebook's matter fraction with the pipeline's from 2e4 GeV down to T_CMB, and split the
ratio into the notebook's (1 + w) factor and its omission of g_s != g_rho. Run from the repository root:
    ./venv/bin/python .documents/peakewsurf-field-equations/scripts/fm_late.py WORK
WORK holds nb_fm_late.csv (nb_fm_late.wls) and delta_pipe.csv (thermo_compare.py). Writes
data/fm_late.csv and prints the numbers quoted in the report.

The pipeline's f_m along a history is a function of T_J alone (report, Sec. 1.2):
    f_pipe(T) = rho_m0 (1 + z(T))^3 / rho_J(T),   rho_J = (pi^2/30) g_rho T^4 exp(Delta(T)),
with Delta from thermo_compare.py (constant below 1e-5 GeV, where w = 1/3 and the g's are clamped).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
import dkstub  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Units import GeV_units  # noqa: E402

W = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
u = GeV_units()
c = QCD_Cosmology(None, u, Planck2018())

nb = pd.read_csv(f"{W}/nb_fm_late.csv")
d = pd.read_csv(f"{W}/delta_pipe.csv")
T = nb.T_GeV.values

gr = np.array([float(c.G_rho(t)) for t in T])
gs = np.array([float(c.G_s(t)) for t in T])
w_p = np.array([float(c.w(t)) for t in T])
z = np.array([c.z(t) for t in T])
Delta = np.interp(
    np.log(T), np.log(d.T_GeV.values), d.Delta.values
)  # flat beyond the ends
rho_g = np.pi**2 / 30 * gr * T**4
f_pipe = c.rho_m0 * (1 + z) ** 3 / (rho_g * np.exp(Delta))
f_pipe_noDelta = c.rho_m0 * (1 + z) ** 3 / rho_g

# rho_m / s today, from the same parameters
s0 = 2 * np.pi**2 / 45 * c.G_s(c._T_CMB) * c._T_CMB**3
m_over_s = c.rho_m0 / s0

ratio = nb.fmTinterp.values / f_pipe
A = (16.0 / 9.0) / (1 + nb.w.values) ** 2  # the notebook's (1 + w) placement
B = ratio / A  # everything else

pd.DataFrame(
    dict(
        T_GeV=T,
        w_nb=nb.w,
        fm_nb=nb.fmTinterp,
        fm_pipe=f_pipe,
        ratio=ratio,
        w_factor=A,
        rest=B,
    )
).to_csv(
    os.path.join(HERE, "..", "data", "fm_late.csv"), index=False, float_format="%.6e"
)

print(
    f"rho_m/s today = {m_over_s:.4g} GeV;  notebook constant 2.78e-9/4 = {2.78e-9 / 4:.4g} = (4/3) x {2.78e-9 / 4 * 0.75:.4g}"
)
print(f"g_s/g_rho below 1e-5 GeV: {gs[0] / gr[0]:.5f}; g_rho/g_s = {gr[0] / gs[0]:.5f}")
print(f"notebook rho_m/s over pipeline's: {2.78e-9 / 4 * 0.75 / m_over_s:.5f}")
for Tq in [2.36e-13, 1e-9, 1e-6, 1e-4, 1.6e-4, 1e-3, 0.18, 1.0, 50.0]:
    j = np.argmin(abs(np.log(T / Tq)))
    print(
        f"T = {T[j]:.4g} GeV: f_nb = {nb.fmTinterp[j]:.5g}, f_pipe = {f_pipe[j]:.5g}, ratio = {ratio[j]:.4f}"
        f" = (1+w) factor {A[j]:.4f} x rest {B[j]:.4f}   (w_nb = {nb.w[j]:.5f}, g_s/g_rho = {gs[j] / gr[j]:.4f}, Delta = {Delta[j]:.2e})"
    )
for lo, hi in [(1e-5, 2e4), (2.36e-13, 1e-5)]:
    m = (T >= lo) & (T <= hi)
    print(
        f"[{lo:g}, {hi:g}] GeV: (1+w) factor in [{A[m].min():.4f}, {A[m].max():.4f}], rest in [{B[m].min():.4f}, {B[m].max():.4f}]"
    )

# matter-radiation equality, f = 1, and the scale factor there (a ~ 1/(T g_s^1/3))
# f falls monotonically with T, so interpolate ln T against ln f on the reversed grid
for lab, f in [("notebook", nb.fmTinterp.values), ("pipeline", f_pipe)]:
    lnT = np.interp(0.0, np.log(f[::-1]), np.log(T[::-1]))
    print(f"{lab}: T_eq = {np.exp(lnT) / 1e-9:.4f} eV")
Teq_nb = np.exp(np.interp(0.0, np.log(nb.fmTinterp.values[::-1]), np.log(T[::-1])))
Teq_p = np.exp(np.interp(0.0, np.log(f_pipe[::-1]), np.log(T[::-1])))
print(
    f"T_eq(nb)/T_eq(pipe) = {Teq_nb / Teq_p:.4f};  a_eq(nb)/a_eq(pipe) = {Teq_p / Teq_nb:.4f}"
    f" (g_s constant there);  shift in N = {np.log(Teq_p / Teq_nb):.4f} e-folds"
)
print(
    f"at T = {T[0]:.4g} GeV: f_nb = {nb.fmTinterp.values[0]:.5g}, f_pipe = {f_pipe[0]:.5g}"
    f" (without Delta {f_pipe_noDelta[0]:.5g})"
)
