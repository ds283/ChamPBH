# Synthetic test of the BBNData arcsinh spline vs a ratio spline.
import sys, numpy as np
sys.path.insert(0, '/Users/ds283/Documents/Code/ChamPBH')
from math import pi
from scipy.interpolate import make_interp_spline
from CosmologyModels.GenericEOS.SaikawaShirai_common import _raw_G_rho, LOW_T_GSTAR

def g_rho(T_MeV):
    T_GeV = T_MeV/1e3
    return LOW_T_GSTAR if T_GeV < 1e-5 else float(_raw_G_rho(T_GeV))

def rho_SM(T_MeV):  # MeV^4
    return (pi**2/30.)*g_rho(T_MeV)*T_MeV**4

def ratio_const(T_MeV):
    return 0.08
def ratio_osc(T_MeV):
    x = np.log(T_MeV/0.3)
    return 0.08 + 0.3*np.sin(2*pi*x/1.0)*np.exp(-(x/1.5)**2)

def run(ratio, n_per_decade, label):
    xs = np.linspace(np.log(1e-7), np.log(100.), int(n_per_decade*9)+1)  # ln(T/MeV), 0.1 eV .. 100 MeV
    Ts = np.exp(xs)
    rho_np = np.array([ratio(T)*rho_SM(T) for T in Ts])
    r = np.array([ratio(T) for T in Ts])
    s_asinh = make_interp_spline(xs, np.arcsinh(rho_np), k=3)
    s_ratio = make_interp_spline(xs, r, k=3)
    ds_asinh = s_asinh.derivative(); ds_ratio = s_ratio.derivative()
    # evaluation grid (dense) in the window that matters
    xe = np.linspace(np.log(0.02), np.log(5.0), 3000); Te = np.exp(xe)
    rsm = np.array([rho_SM(T) for T in Te]); rtrue = np.array([ratio(T) for T in Te])
    rho_true = rtrue*rsm
    rho_a = np.sinh(s_asinh(xe)); rho_r = s_ratio(xe)*rsm
    err_a = np.abs(rho_a-rho_true)/rsm; err_r = np.abs(rho_r-rho_true)/rsm
    # derivative: d rho_NP / d lnT normalized by 4 rho_SM (~ d rho_SM/dlnT)
    eps=1e-5; drho_true = np.array([(ratio(np.exp(x+eps))*rho_SM(np.exp(x+eps))-ratio(np.exp(x-eps))*rho_SM(np.exp(x-eps)))/(2*eps) for x in xe])
    drho_a = np.sqrt(1+rho_a**2)*ds_asinh(xe); drsm = np.array([(rho_SM(np.exp(x+eps))-rho_SM(np.exp(x-eps)))/(2*eps) for x in xe]); drho_r = ds_ratio(xe)*rsm + s_ratio(xe)*drsm
    derr_a = np.abs(drho_a-drho_true)/(4*rsm); derr_r = np.abs(drho_r-drho_true)/(4*rsm)
    def at(T, arr): return arr[np.argmin(np.abs(xe-np.log(T)))]
    print(f"{label:9s} n/dec={n_per_decade:3d} | asinh: max {err_a.max():.1e}  @1MeV {at(1,err_a):.1e} @0.3MeV {at(0.3,err_a):.1e} @0.07MeV {at(0.07,err_a):.1e} | dρ: max {derr_a.max():.1e}")
    print(f"{'':9s}            | ratio: max {err_r.max():.1e}  @1MeV {at(1,err_r):.1e} @0.3MeV {at(0.3,err_r):.1e} @0.07MeV {at(0.07,err_r):.1e} | dρ: max {derr_r.max():.1e}")

print("T where |rho_NP| = 1 MeV^4 for ratio 0.08: ", end="")
Ts = np.exp(np.linspace(np.log(0.1), np.log(10), 2000)); v=[abs(0.08*rho_SM(T)-1) for T in Ts]; print(f"{Ts[int(np.argmin(v))]:.3g} MeV")
print("errors are |rho_NP,spline - rho_NP,true| / rho_SM(T)  (i.e. spurious fractional change of H^2)")
for n in (20, 50, 100, 250):
    run(ratio_const, n, "constant")
for n in (20, 50, 100, 250):
    run(ratio_osc, n, "oscill.")
