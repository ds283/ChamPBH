# Late-time margin of the relativistic approximation (inelastic-reflection planning, 2026-10-27).
# (1) m_eff today at the tracking minimum of V0 exp(M/phi) + A rho_m, n = 1, M = 0.5 M_P, with
#     V(phi_ref) = rho_Lambda and V'(phi_ref) = -(beta/M_P) rho_m0 (A(phi_ref) = 1), in units of H0.
# (2) the comoving Hubble rate at a bounce at T_b relative to today's, (H a)_b / (H a)_0, which
#     bounds p_0/H_0 from below for a quantum born with p >= H.
# Units M_P = 1 in (1); GeV in (2). Einstein = Jordan frame here (A ~ 1 late).
from math import sqrt, pi

OmL, Omm = 0.685, 0.315
M = 0.5
print("(1) tracker mass today, m0/H0")
for beta in [0.1, 0.5, 1.0, 1.2, 2.0, 5.0, 10.0, 25.0]:
    rhoL = 3.0 * OmL  # in units of H0^2 M_P^2
    phi = sqrt(M * OmL / (beta * Omm))  # |V'|/V = M/phi^2 = beta rho_m/rho_L
    m2 = rhoL * (M**2 / phi**4 + 2.0 * M / phi**3 + beta * M / phi**2)
    print(f"  beta = {beta:5.1f}: phi_ref = {phi:.3f} M_P, m0/H0 = {sqrt(m2):.3g}")

print("(2) (H a)_b / (H a)_0 for a bounce at T_b")
MPl = 1.220890e19  # full Planck mass, GeV
T0 = 2.7255 * 8.617333e-14  # GeV
H0 = 0.674 * 2.1332e-42  # GeV
for Tb, g, gs in [
    (1e3, 106.75, 106.75),
    (1.0, 61.75, 61.75),
    (1e-3, 10.75, 10.75),
    (1e-5, 3.36, 3.91),
]:
    Hb = 1.66 * sqrt(g) * Tb**2 / MPl
    ab = (T0 / Tb) * (3.91 / gs) ** (1.0 / 3.0)
    print(f"  T_b = {Tb:g} GeV: (H a)_b / (H a)_0 = {Hb * ab / H0:.3g}")
