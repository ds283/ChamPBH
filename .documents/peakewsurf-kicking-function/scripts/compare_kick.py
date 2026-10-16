"""Compare PeakEWsurf.nb's Sigma(T), f_m(T) with the pipeline's. Run from the repo root."""

import sys
from math import log, exp

import numpy as np
import pandas as pd
import types as _t

_dk = _t.ModuleType("datastorekit")


class DatastoreObject:
    def __init__(self, store_id=None, *a, **k):
        self._my_id = store_id

    @property
    def store_id(self):
        return self._my_id


_dk.DatastoreObject = DatastoreObject
sys.modules.setdefault("datastorekit", _dk)

from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import GeV_units

S = sys.argv[1]
units = GeV_units()
cosmo = QCD_Cosmology(None, units, Planck2018())
GeV = units.GeV

# ---- 1. raw tables at shared rows
nbt = pd.read_csv(f"{S}/nb_w_table.csv")
csv = pd.read_csv("CosmologyModels/GenericEOS/Xav_EOS_data.csv")
print("== 1. raw table rows")
print(
    f"notebook: {len(nbt)} rows, {nbt.T_GeV.min():.6g}..{nbt.T_GeV.max():.6g} GeV;"
    f" CSV: {len(csv)} rows, {csv.T_GeV.min():.6g}..{csv.T_GeV.max():.6g} GeV"
)
lc = np.log10(csv.T_GeV.values)
matched, unmatched = [], []
for T, w in zip(nbt.T_GeV, nbt.w):
    j = np.argmin(abs(lc - np.log10(T)))
    if abs(lc[j] - np.log10(T)) < 1e-9:
        matched.append((T, w, csv.w.values[j]))
    else:
        unmatched.append((T, w))
m = np.array(matched)
dS = 3.0 * (m[:, 2] - m[:, 1])  # Sigma_nb - Sigma_csv = 3 (w_csv - w_nb)
print(
    f"rows on the CSV grid: {len(m)}; max |dSigma| = {abs(dS).max():.3g}"
    f" at T = {m[np.argmax(abs(dS)), 0]:.5g} GeV"
)
for T, w in unmatched:
    print(f"  notebook row not on CSV grid: T = {T:.10g} GeV, w = {w:.10g}")
# CSV rows with T in (100, 25118) that the notebook does not have
hi = csv[(csv.T_GeV > 100.0)]
print(
    "CSV above 100 GeV (Sigma):",
    ", ".join(f"{T:.4g}:{1-3*w:.3g}" for T, w in zip(hi.T_GeV, hi.w) if True)[:900],
)

# ---- 2. Sigma on the dense grid
g = pd.read_csv(f"{S}/nb_kick_grid.csv")
T = g.T_GeV.values
Sig_pipe = np.array([1.0 - 3.0 * float(cosmo.w(t * GeV)) for t in T])
Sig_nb = g.Sigma.values
d = Sig_nb - Sig_pipe
print("\n== 2. Sigma(T) on 1000 pts/decade, 1e-5 .. 3e4 GeV")
print(
    f"max |Sig_nb - Sig_pipe| = {abs(d).max():.4g} at T = {T[np.argmax(abs(d))]:.5g} GeV"
)
for lo, hi_, name in [
    (1e-5, 2e-3, "e+e-"),
    (2e-3, 1.0, "QCD"),
    (1.0, 386.0, "EW"),
    (386.0, 3e4, "above nb table"),
]:
    sel = (T >= lo) & (T < hi_)
    ip, ii = np.argmax(Sig_pipe[sel]), np.argmax(Sig_nb[sel])
    Ts = T[sel]
    print(
        f"  {name:15s} peak pipe {Sig_pipe[sel][ip]:.5f} @ {Ts[ip]:.5g} GeV | nb {Sig_nb[sel][ii]:.5f} @ {Ts[ii]:.5g} GeV"
        f" | max|d| {abs(d[sel]).max():.3g} (rel to pipe peak {abs(d[sel]).max()/Sig_pipe[sel].max():.2%})"
        f" | int dlnT pipe {np.trapezoid(Sig_pipe[sel], np.log(Ts)):.5g} nb {np.trapezoid(Sig_nb[sel], np.log(Ts)):.5g}"
    )

# ---- 3. f_m: pipeline's rho_m(T)/rho_r(T) as compute_scalar_model initialises it
LOG_PISQ_OVER_30 = log(np.pi**2 / 30.0)


def fm_pipe(t):
    TT = t * GeV
    log_rho_r = LOG_PISQ_OVER_30 + 4.0 * log(TT) + log(cosmo.G_rho(TT))
    log_rho_m = log(cosmo.rho_m0) + 3.0 * log(1.0 + cosmo.z(TT))
    return exp(log_rho_m - log_rho_r)


fp = np.array([fm_pipe(t) for t in T])
print("\n== 3. f_m(T)")
for name, col in [
    ("fmT (closed form)", g.fmT.values),
    ("fmTinterp (in ODE)", g.fmTinterp.values),
]:
    r = col / fp
    print(
        f"  {name}: nb/pipe ratio min {r.min():.4f} @ {T[np.argmin(r)]:.4g} GeV, max {r.max():.4f} @ {T[np.argmax(r)]:.4g} GeV"
    )
for t in [1e-4, 1.6e-4, 0.18, 1.0, 55.0, 1e3]:
    i = np.argmin(abs(T - t))
    print(
        f"  T={T[i]:.4g}: pipe fm {fp[i]:.4g}, nb fmT {g.fmT.values[i]:.4g}, Sigma_pipe {Sig_pipe[i]:.4g}, fm/Sigma {fp[i]/max(Sig_pipe[i],1e-300):.3g}"
    )
# the notebook's w-dependence vs (rho_m/s)(1+w)/T: ratio should be (16/9)/(1+w)^2 if so
w_nb = g.w.values
pred = (16.0 / 9.0) / (1.0 + w_nb) ** 2
print(f"  max |nb/pipe - (16/9)/(1+w_nb)^2| = {abs(g.fmT.values/fp - pred).max():.3g}")

# ---- 4. the combined kick factor Sigma + f_m (what multiplies beta rho)
K_nb = Sig_nb + g.fmTinterp.values
K_p = Sig_pipe + fp
dk = K_nb - K_p
print("\n== 4. Sigma + f_m")
print(
    f"  max |dK| = {abs(dk).max():.4g} @ {T[np.argmax(abs(dk))]:.5g} GeV; f_m-only part max |d fm| = {abs(g.fmTinterp.values - fp).max():.3g}"
)

np.savetxt(
    f"{S}/compare.csv",
    np.c_[T, Sig_pipe, Sig_nb, fp, g.fmT.values, g.fmTinterp.values],
    delimiter=",",
    header="T_GeV,Sigma_pipe,Sigma_nb,fm_pipe,fmT_nb,fmTinterp_nb",
    comments="",
)
