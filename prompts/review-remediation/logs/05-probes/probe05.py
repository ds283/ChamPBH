import numpy as np, pandas as pd
from math import log
from scipy.integrate import simpson, quad
from Units import GeV_units
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline
from CosmologyModels.tests.eos_reference import integrate_temperature_law, T_INIT_GEV
u = GeV_units(); X = Xav_EOS_spline(u); G = u.GeV
S = lambda T: 1.0 - 3.0*float(X.w(T*G))
for ppd in (200, 1000, 5000):
    n = int(round(ppd*np.log10(3e4/1e-5)))+1
    Ts = np.logspace(np.log10(1e-5), np.log10(3e4), n)
    Sig = np.array([S(T) for T in Ts])
    for lo,hi in ((1e-5,5e-3),(5e-2,1.0),(20.0,1e3)):
        m = (Ts>=lo)&(Ts<=hi); i = np.argmax(Sig[m])
        print(ppd, lo, hi, f"peak Sigma={Sig[m][i]:.6f} at T={Ts[m][i]:.6g} GeV")
    m = (Ts>=1e-5)&(Ts<=3e-3)
    print(ppd, "int simpson", simpson(Sig[m], x=np.log(Ts[m])), "trap", np.trapezoid(Sig[m], np.log(Ts[m])))
print("quad", quad(lambda x: S(np.exp(x)), log(1e-5), log(3e-3), limit=500, epsabs=1e-12))
for T in (2e-3,5e-4,2e-4,1e-4,5e-5,2e-5): print(f"Sigma({T*1e3:g} MeV) = {S(T):.6f}", S(T))
print("min Sigma over grid", Sig.min(), Ts[np.argmin(Sig)])
print("T_min,T_max", X._T_min, X._T_max)
for T in (1e-9,1e-6,5e-6,9.99e-6,X._T_min,X._T_max,2.6e4,3e4,1e6): print(T, float(X.w(T*G)), float(X.w(T*G))==1/3)
print("just inside", float(X.w(X._T_min*1.0001*G)), float(X.w(X._T_max*0.9999*G)))
SS = SaikawaShirai_EOS_spline(u)
print("SS w 1MeV, 2MeV", SS.w(1e-3*G), SS.w(2e-3*G), SS.w(1e-5*G), SS.w(1e-3*G)==SS.w(2e-3*G))
print("Xav w 1MeV, 2MeV", float(X.w(1e-3*G)), float(X.w(2e-3*G)))
# table
df = pd.read_csv("CosmologyModels/GenericEOS/Xav_EOS_data.csv")
print(len(df), df.T_GeV.min(), df.T_GeV.max(), df.w.iloc[0], df.w.iloc[-1], 1-3*df.w.iloc[0], 1-3*df.w.iloc[-1])
print("rows per decade", (len(df)-1)/np.log10(df.T_GeV.max()/df.T_GeV.min()), "log spacing", np.unique(np.round(np.diff(np.log10(df.T_GeV)),6)))
Sg = 1-3*df.w
for lo,hi in ((1e-5,5e-3),(5e-2,1.0),(20.0,1e3)):
    m=(df.T_GeV>=lo)&(df.T_GeV<=hi); i=Sg[m].idxmax(); print("table", df.T_GeV[i], Sg[i])
print("table max |w-1/3| first/last rows", (df.w-1/3).abs().iloc[:5].tolist(), (df.w-1/3).abs().iloc[-5:].tolist())
print("table min Sigma", Sg.min(), df.T_GeV[Sg.idxmin()])
# rho witness
for T0 in (5e-3, 0.1, T_INIT_GEV):
    for ms in (0.05, 0.01):
        r = integrate_temperature_law(X, T0, 1e-5, with_rho=True, max_step=ms)
        print("rho witness", T0, "->10keV", ms, f"{r.rho_R_ratio:.6f}")
