import numpy as np
from Units import GeV_units
from CosmologyModels.tests.eos_reference import production_eos
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_jax_autodiff import SaikawaShirai_EOS_jax_autodiff
X = production_eos(); G = X._units.GeV
J = SaikawaShirai_EOS_jax_autodiff(GeV_units())
def implied(E, T):
    t = T*G
    grho, gs = float(E.G_rho(t)), float(E.G_s(t))
    a = float(E.dG_rho_dlogT(t))/grho; b = float(E.dG_s_dlogT(t))/gs
    return 4 - (4 + a)/(1 + b/3)
for lo, hi in ((1e-5, 5e-3), (5e-2, 1.0), (20.0, 1e3)):
    Ts = np.logspace(np.log10(lo), np.log10(hi), int(1000*np.log10(hi/lo))+1)
    for name, E in (("spline", X), ("jax", J)):
        if name == "jax": Ts2 = Ts[::5]
        else: Ts2 = Ts
        s = np.array([implied(E, T) for T in Ts2]); i = np.argmax(s)
        print(f"window [{lo:g},{hi:g}] {name}: implied peak {s[i]:.5f} at {Ts2[i]:.5g} GeV")
for T in (56.23, 53.25, 45.5, 0.2, 0.18, 0.16, 0.15, 0.14, 0.13):
    print(f"{T:8.4g}  table {1-3*float(X.w(T*G)):.5f}  spline-g {implied(X,T):.5f}  jax-g {implied(J,T):.5f}")
