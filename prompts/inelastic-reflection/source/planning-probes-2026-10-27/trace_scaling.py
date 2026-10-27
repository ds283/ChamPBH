import numpy as np
from Units.GeV_units import GeV_units
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline

u = GeV_units()
eos = Xav_EOS_spline(u)
print("table range (GeV):", eos._T_min, eos._T_max)
T = np.logspace(np.log10(2.0e4), -4.9, 22001)  # GeV, 100 TeV -> 1 keV
g = np.array([eos.G_rho(t * u.GeV) for t in T])
gs = np.array([eos.G_s(t * u.GeV) for t in T])
w = np.array([eos.w(t * u.GeV) for t in T])
Sigma = 1.0 - 3.0 * w
N = -np.log(T) - np.log(gs) / 3.0  # ln a, up to a constant
rho_r = g * T**4
# matter normalised to equality at T = 0.8 eV
Teq = 0.8e-9
ieq = np.argmin(abs(T - Teq))
C = rho_r[ieq] * np.exp(3 * N[ieq])
rho_m = C * np.exp(-3 * N)
for label, rho_c in [
    ("Sigma*rho_r + rho_m", Sigma * rho_r + rho_m),
    ("Sigma*rho_r only", Sigma * rho_r),
]:
    lc = np.log(np.maximum(rho_c, 1e-300))
    s = np.gradient(lc, N)
    print(f"\n== {label}: d ln rho_c / dN")
    for lo, hi in [(1e3, 2e4), (1, 1e3), (0.1, 1), (1e-3, 0.1), (1e-5, 1e-3)]:
        m = (T >= lo) & (T <= hi)
        i = np.argmax(np.where(m, s, -np.inf))
        print(
            f"  T in [{lo:g},{hi:g}] GeV: max slope {s[i]:+.3f} at T = {T[i]:.4g} GeV (Sigma={Sigma[i]:.3g})"
        )
    # worst-case growth of m*a for gamma=1 and gamma=3/2: max over later/earlier pairs
    for gam in (1.0, 1.5):
        y = 0.5 * gam * lc + N  # ln(m a) up to const
        runmin = np.minimum.accumulate(y)  # T decreasing along the array
        rise = np.max(y - runmin)
        j = np.argmax(y - runmin)
        i0 = np.argmin(y[: j + 1])
        print(
            f"  gamma={gam}: max growth of m*a = factor {np.exp(rise):.3g}, from T={T[i0]:.4g} to T={T[j]:.4g} GeV"
        )
