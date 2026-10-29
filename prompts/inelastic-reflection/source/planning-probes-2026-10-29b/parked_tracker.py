# Probe: is the zero-lag parked tracker adiabatic from parking to today? No integration.
# Background: the cosmology's EOS with A = 1 (exact at laboratory M; at large M it neglects A - 1),
# rho_R = (pi^2/30) g_rho T^4, rho_m = rho_m0 g_s T^3/(g_s0 T0^3), Sigma = 1 - 3w, N = ln a from
# entropy conservation. phi_min(N) solves V' + (beta/M_P) trace = 0, trace = rho_R Sigma + rho_m,
# on the model's seed realization (n = 1). Along it:
#   m^2         = log_m_eff_squared (the loop's own function: exercises its overflow handling)
#   A_ad        = H |d ln m^2/dN| / (2 m), the gate's non-adiabaticity, on the tracker
#   lag         = delta phi/phi_min = -(phi_min'' + (3 + Hdot/H^2) phi_min')/(m^2/H^2)/phi_min, the
#                 first adiabatic correction (field behind a growing minimum: negative)
#   KE fraction = H^2 phi_min'^2/2 / rho_total
# Usage: parked_tracker.py  (prints a table)
from math import exp, log, pi as PI

import numpy as np
from scipy.optimize import brentq

from ComputeTargets.tests import golden_tuned_history as gth
import importlib
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

cos = gth.make_cosmology()
u = cos.units
assert u.PlanckMass == 1.0
T0 = cos.T_CMB_Kelvin * u.Kelvin
Ts = np.exp(np.linspace(log(2.0 * u.GeV), log(T0), 6000))
g_rho = np.array([cos.G_rho(T) for T in Ts])
g_s = np.array([cos.G_s(T) for T in Ts])
Sigma = np.array([1.0 - 3.0 * cos.w(T) for T in Ts])
rhoR = PI**2 / 30.0 * g_rho * Ts**4
rhom = cos.rho_m0 * (g_s * Ts**3) / (g_s[-1] * T0**3)
N = np.log((g_s[-1] / g_s) ** (1.0 / 3.0) * T0 / Ts)
N = N - N[-1]  # e-folds before today
trace = rhoR * Sigma + rhom


def row(beta, M):
    model = ExponentialModel(0, M_value(0, M), 1, beta_value(0, beta), u)
    seed = model.seed(cos)
    rm = model.realize(seed.normalization, seed.phi_ref)
    norm = seed.normalization
    phis = np.empty(len(Ts))
    for i, tr in enumerate(trace):
        # ln V' magnitude = norm + y + 2 ln y - ln M  (y = M/phi);  balance: = ln(beta trace)
        g = lambda ly: norm + exp(ly) + 2.0 * ly - log(M) - log(beta * tr)
        phis[i] = M / exp(brentq(g, -80.0, log(3000.0), xtol=1e-14))
    V = np.exp(norm + M / phis)
    lm2 = np.array([SM.log_m_eff_squared(rm, p, log(r), log(m / r), s)
                    for p, r, m, s in zip(phis, rhoR, rhom, Sigma)])
    H2 = (rhoR + rhom + V) / 3.0
    lnH = 0.5 * np.log(H2)
    mH = np.exp(0.5 * lm2 - lnH)
    A_ad = 0.5 * np.abs(np.gradient(lm2, N)) / mH
    d1 = np.gradient(phis, N)
    d2 = np.gradient(d1, N)
    eps_H = np.gradient(lnH, N)
    lag = -(d2 + (3.0 + eps_H) * d1) / mH**2 / phis
    ke = H2 * d1**2 / 2.0 / (3.0 * H2)
    T_MeV = Ts / u.MeV
    ia, im = int(np.argmax(A_ad)), int(np.argmin(mH))
    # the release point: the first temperature (from above) at which the tracker stops being adiabatic
    rel = {}
    for name, bad in (("m/H<10", mH < 10.0), ("A_ad>0.1", A_ad > 0.1), ("|lag|>1e-2", np.abs(lag) > 1e-2)):
        k = np.nonzero(bad)[0]
        rel[name] = f"{T_MeV[k[0]]:.3g}" if len(k) else "never"
    print(f"beta={beta:<4} M={M:<7g} phi_min/M today {phis[-1] / M:9.4g} | min m/H {mH[im]:9.3g} at T={T_MeV[im]:.3g} MeV"
          f" | m/H today {mH[-1]:9.3g} | max A_ad {A_ad[ia]:9.3g} at T={T_MeV[ia]:.3g} MeV | lag today {lag[-1]:+9.3e}"
          f" max|lag| {np.max(np.abs(lag)):9.3e} | KE frac max {np.max(ke):9.3e} | release(T/MeV): "
          + ", ".join(f"{k} {v}" for k, v in rel.items()))
    return phis, mH, A_ad, lag


for beta in (0.5, 1.2, 2.0, 3.0, 5.0):
    for M in (1e-30, 1e-15, 1e-7, 1e-3, 0.5):
        row(beta, M)
    print()
