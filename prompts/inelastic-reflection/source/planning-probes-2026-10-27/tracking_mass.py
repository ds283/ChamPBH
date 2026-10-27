# m_eff at the tracking minimum of V0 exp(M/phi) + A(phi) rho_c, A = exp(beta (phi - phi_ref)/M_P),
# along the code's thermal history. Units: M_P = 1, densities in units of rho_m0. Einstein = Jordan here.
import numpy as np
from Units.GeV_units import GeV_units
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline

u = GeV_units()
eos = Xav_EOS_spline(u)
M = 0.5
OmL, Omm = 0.685, 0.315
T = np.logspace(np.log10(2.0e4), np.log10(2.35e-13), 30001)
inT = T >= 1.0e-5
g = np.array([eos.G_rho(t * u.GeV) if i else 3.36 for t, i in zip(T, inT)])
gs = np.array([eos.G_s(t * u.GeV) if i else 3.91 for t, i in zip(T, inT)])
Sig = np.array([1 - 3 * eos.w(t * u.GeV) if i else 0.0 for t, i in zip(T, inT)])
N = -np.log(T) - np.log(gs) / 3.0
N -= N[-1]
ln_rm = -3 * N
ln_rr = np.log(g * T**4)
ieq = np.argmin(abs(T - 0.8e-9))
ln_rr += ln_rm[ieq] - ln_rr[ieq]
ln_rc = np.logaddexp(np.log(np.maximum(Sig, 1e-300)) + ln_rr, ln_rm)

for beta in [0.1, 0.5, 1.0, 1.2, 2.0, 5.0, 10.0, 25.0]:
    rL = OmL / Omm
    phi_ref = np.sqrt(M * rL / beta)
    lnV0 = np.log(rL) - M / phi_ref
    # root of lnV0 + ln M - 2 ln phi + M/phi = ln beta + beta (phi - phi_ref) + ln rho_c, bisect in ln phi
    lo = np.full_like(ln_rc, np.log(1e-8))
    hi = np.full_like(ln_rc, np.log(50.0))
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        p = np.exp(mid)
        F = (
            lnV0
            + np.log(M)
            - 2 * mid
            + M / p
            - (np.log(beta) + beta * (p - phi_ref) + ln_rc)
        )
        lo = np.where(F > 0, mid, lo)
        hi = np.where(F > 0, hi, mid)
    phi = np.exp(0.5 * (lo + hi))
    ln_m2 = lnV0 + M / phi + np.log(M**2 / phi**4 + 2 * M / phi**3 + beta * M / phi**2)
    y = 0.5 * ln_m2 + N  # ln(m a)
    runmin = np.minimum.accumulate(y)
    rise = y - runmin
    j = np.argmax(rise)
    i0 = np.argmin(y[: j + 1])
    gam = np.gradient(ln_m2, ln_rc)
    print(
        f"beta={beta:5.1f}: phi_ref={phi_ref:.3f}, beta*phi: {beta*phi[0]:.3g} (20 TeV) -> {beta*phi[-1]:.3g} (today); "
        f"m*a falls by {np.exp(y[0]-y[-1]):.3g} overall; worst rise x{np.exp(rise[j]):.4g} "
        f"(T {T[i0]:.3g} -> {T[j]:.3g} GeV); gamma today {gam[-1]:.3f}"
    )
    late = T < 1e-9
    yl = y[late]
    rl = yl - np.minimum.accumulate(yl)
    k = np.argmax(rl)
    print(
        f"        late (T < 1 eV): worst rise x{np.exp(rl[k]):.4g}, ending at z = {T[late][k]/2.35e-13 - 1:.3g}"
    )
sel = (T > 2.5e-5) & (T < 3.3e-5)
print("\nT [keV], Sigma, g, gs, N  near the 30 keV feature:")
for t, s_, a_, b_, n_ in list(zip(T[sel], Sig[sel], g[sel], gs[sel], N[sel]))[::40]:
    print(f"  {t*1e6:.3f}  {s_:.3g}  {a_:.5g}  {b_:.5g}  {n_:.5f}")
