"""
Planning probe for the production-readiness campaign, prompt 03 (item P3, review H5).
Taken on 204795e, 2026-09-30. Run from the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/h5_bracket_probe.py

About 60 s. Scratch code, not a test; the campaign's tests replace it.

The missing piece of the chameleon mass is the phi-derivative, at fixed a_E and
fixed comoving entropy, of the source term in the force the ODE integrates:

    d/dphi [Sigma rho_R,E + rho_m,E] = (ln Omega)' rho_R,E [B + f_m]

This compares three values of the bracket B through the production EOS
(Xav_EOS_spline):

- B_audit   = Sigma (4 - d ln(Sigma rho_J)/d ln T_J), rho_J = (pi^2/30) g_rho T^4
              (audit section 5);
- B_entropy = Sigma^2 - (dSigma/d ln T_J) / (1 + x), x = (1/3) d ln g_s / d ln T_J
              (the planner's derivation, README section 2 (b));
- B_fd      = a central difference in ln Omega of the force, where T_J(phi) solves
              T_J Omega g_s(T_J)^{1/3} = const with G_s only (no dG_s_dlogT), and
              rho_R,E responds as the ODE says, d ln rho_R,E = Sigma d ln Omega.

It then measures, on 1500 points over [12 keV, 20 TeV], how fast B_fd converges
to B_entropy, and how well the spline's analytic dSigma/d ln T matches a central
difference of Sigma.
"""

import numpy as np
from scipy.optimize import brentq

from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

u = GeV_units()
eos = Xav_EOS_spline(u)
GeV = u.GeV
dspl = eos._spline.derivative()


def Sig(T):
    return 1 - 3 * float(eos.w(T * GeV))


def dSig_an(T):
    if T <= eos._T_min or T >= eos._T_max:
        return 0.0
    return -3 * float(dspl(np.log(T)))


def dSig_cd(T, h=1e-4):
    return (Sig(T * np.exp(h)) - Sig(T * np.exp(-h))) / (2 * h)


def x(T):
    return float(eos.dG_s_dlogT(T * GeV)) / float(eos.G_s(T * GeV)) / 3


def dlng_rho(T):
    return float(eos.dG_rho_dlogT(T * GeV)) / float(eos.G_rho(T * GeV))


def T_of(dlnA, T0):
    C = np.log(T0) + np.log(float(eos.G_s(T0 * GeV))) / 3
    f = lambda lt: lt + dlnA + np.log(float(eos.G_s(np.exp(lt) * GeV))) / 3 - C
    return np.exp(brentq(f, np.log(T0) - 1, np.log(T0) + 1, xtol=1e-15))


def F(dlnA, T0):
    # force / ((ln Omega)' rho_R,E(0)) at the shifted field, f_m = 0
    T1 = T_of(dlnA, T0)
    return np.exp(0.5 * (Sig(T0) + Sig(T1)) * dlnA) * Sig(T1)


def B_fd(T, h):
    return (F(h, T) - F(-h, T)) / (2 * h)


def B_entropy(T):
    return Sig(T) ** 2 - dSig_an(T) / (1 + x(T))


def B_audit(T):
    S = Sig(T)
    if S < 1e-12:
        return 0.0
    # Sigma (4 - dln Sigma/dlnT - 4 - dln g_rho/dlnT)
    return S * (-dSig_an(T) / S - dlng_rho(T))


print("Table 1. The bracket at selected temperatures (h = 1e-4)")
print(
    f"{'T/GeV':>10} {'Sigma':>8} {'dSig':>8} {'x':>8} {'B_audit':>9} {'B_entropy':>9} {'B_fd':>9}"
)
for T in [3e-4, 1.6e-4, 1e-4, 0.25, 0.18, 0.14, 0.1, 80, 53, 40]:
    print(
        f"{T:10.4g} {Sig(T):8.4f} {dSig_an(T):8.4f} {x(T):8.4f} "
        f"{B_audit(T):9.4f} {B_entropy(T):9.4f} {B_fd(T, 1e-4):9.4f}"
    )

Ts = np.logspace(np.log10(1.2e-5), np.log10(2e4), 1500)
print("\nTable 2. Convergence of the reference")
for h in [1e-3, 1e-4, 1e-5]:
    d = [abs(B_fd(T, h) - B_entropy(T)) for T in Ts]
    i = int(np.argmax(d))
    print(f"h = {h:g}: max |B_fd - B_entropy| = {d[i]:.3e} at T = {Ts[i]:.4g} GeV")

d = [abs(dSig_an(T) - dSig_cd(T)) for T in Ts]
i = int(np.argmax(d))
print(f"\nmax |dSigma/dlnT analytic - central| = {d[i]:.3e} at {Ts[i]:.4g} GeV")
B = [B_entropy(T) for T in Ts]
i, j = int(np.argmin(B)), int(np.argmax(B))
print(
    f"B_entropy: min {B[i]:.4f} at {Ts[i]:.4g} GeV, max {B[j]:.4f} at {Ts[j]:.4g} GeV"
)
