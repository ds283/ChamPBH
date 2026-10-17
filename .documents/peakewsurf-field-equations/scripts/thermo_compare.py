"""Compare the notebook's g_rho, g_s, d ln g_s/d ln T, f_m with the pipeline's; and measure the
thermodynamic consistency of (w table, g_rho, g_s). Args: scratch dir. Run from the repo root.
"""

import sys, os

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.getcwd())
import dkstub  # noqa
import numpy as np, pandas as pd
from scipy.integrate import cumulative_trapezoid
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import GeV_units

W = sys.argv[1]
u = GeV_units()
c = QCD_Cosmology(None, u, Planck2018())
nb = pd.read_csv(f"{W}/nb_thermo_grid.csv")
T = nb.T_GeV.values
gr = np.array([float(c.G_rho(t)) for t in T])
gs = np.array([float(c.G_s(t)) for t in T])
dgr = np.array([float(c.dG_rho_dlogT(t)) for t in T])
dgs = np.array([float(c.dG_s_dlogT(t)) for t in T])
w = np.array([float(c.w(t)) for t in T])
Sig = 1 - 3 * w
gam_p = dgs / gs / 3
gam_n = np.abs(nb.dgs_dlnT.values / nb.gs_interp.values) / 3


def rep(name, a, b, rel=True):
    d = (a / b - 1) if rel else (a - b)
    i = np.argmax(abs(d))
    print(
        f"{name:34s} max |{'rel' if rel else 'abs'} diff| = {abs(d[i]):.3g} at T = {T[i]:.4g} GeV"
    )
    return d


print("== notebook vs pipeline, 1e-5 .. 2e4 GeV, 2000 pts/decade")
rep("g_rho (raw fit, nb) vs pipe", nb.grho_raw.values, gr)
rep("g_rho (nb interp) vs pipe", nb.grho_interp.values, gr)
rep("g_s (nb interp) vs pipe", nb.gs_interp.values, gs)
d = rep("gamma = dln g_s/3dlnT (abs)", gam_n, gam_p, rel=False)
for lo, hi in [(1e-5, 1e-3), (1e-3, 2), (2, 20), (20, 2e4)]:
    m = (T >= lo) & (T < hi)
    print(
        f"   gamma abs diff in [{lo:g},{hi:g}): {abs(d[m]).max():.3g}; gamma_pipe max {gam_p[m].max():.3g}"
    )
print("sign of pipeline dg_s/dlnT: min =", dgs.min(), "at", T[np.argmin(dgs)])
print(
    "sign of notebook dg_s/dlnT: min =",
    nb.dgs_dlnT.min(),
    "at",
    T[np.argmin(nb.dgs_dlnT.values)],
)
rep("Sigma nb vs pipe (abs)", nb.Sigma.values, Sig, rel=False)

# ---- thermodynamic consistency: continuity (w table) vs rho = pi^2/30 g_rho T^4
lnT = np.log(T)
for lab, S_, g_r, dg_r, gam in [
    ("pipeline", Sig, gr, dgr, gam_p),
    ("notebook", nb.Sigma.values, nb.grho_interp.values, nb.dgrho_dlnT.values, gam_n),
]:
    integrand = (4 + dg_r / g_r) - (4 - S_) * (1 + gam)
    cum = cumulative_trapezoid(integrand, lnT, initial=0.0)
    Delta = cum[-1] - cum  # Delta(T) = int_{lnT}^{lnT_i} [...]
    for Tq in [1e3, 100, 50, 20, 5, 1.64, 1, 0.1, 0.01, 1e-3, 1e-4]:
        j = np.argmin(abs(T - Tq))
        print(f"  {lab}: Delta(T={T[j]:.4g}) = ln(rho_cont/rho_g) = {Delta[j]:+.4e}")
    if lab == "pipeline":
        np.savetxt(
            f"{W}/delta_pipe.csv",
            np.c_[T, Delta],
            delimiter=",",
            header="T_GeV,Delta",
            comments="",
        )
# SS-consistency alone: w_SS = 4gs/3grho - 1 in continuity
SigSS = 1 - 3 * (4 * gs / (3 * gr) - 1)
integrand = (4 + dgr / gr) - (4 - SigSS) * (1 + gam_p)
cum = cumulative_trapezoid(integrand, lnT, initial=0.0)
D2 = cum[-1] - cum
for Tq in [100, 1, 0.01]:
    j = np.argmin(abs(T - Tq))
    print(f"  w_SS from g's: Delta(T={T[j]:.4g}) = {D2[j]:+.4e}")

# ---- pipeline's own f_m(T) along any history: rho_m0 (1+z)^3 / rho_cont(T)
lnrho_g = np.log(np.pi**2 / 30 * gr * T**4)
lnrho_c = lnrho_g + np.loadtxt(f"{W}/delta_pipe.csv", delimiter=",", skiprows=1)[:, 1]
z = np.array([c.z(t) for t in T])
fm_p = c.rho_m0 * (1 + z) ** 3 / np.exp(lnrho_c)
rep("f_m nb vs pipe(exact)", nb.fm.values, fm_p)
print("max f_m/Sigma, T in [0.1 MeV, 2e4]:", end=" ")
m = (T > 1e-4) & (Sig > 0)
print(f"{(fm_p[m]/Sig[m]).max():.3g}")
out = pd.DataFrame(
    dict(T_GeV=T, Sigma_pipe=Sig, gs_pipe=gs, dgs_dlnT_pipe=dgs, fm_pipe=fm_p)
)
out.to_csv(f"{W}/pipe_thermo_grid.csv", index=False)
print("rho_m0 [GeV^4] =", c.rho_m0, " Mp =", u.PlanckMass)
