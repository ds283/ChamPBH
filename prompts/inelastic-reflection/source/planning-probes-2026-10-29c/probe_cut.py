# Orchestrator's probe for writing prompt 06 (parking), at 9c2483b. Read-only: no store, no Ray.
# For one history (test_injection's setup, alpha = 0.1) run to a window past the planner's cut,
# find the cut candidate (the first candidate after the first with rho_kin/rho_R,E < EPS), and
# report the state a parked tracker would start from, then integrate a reduced (ln rho_R,E, ln f_m,
# ln T_J) system with phi = phi_min, pi = dphi_min/dN (solved algebraically) over the window and
# compare it with the oscillating history.
# Usage: probe_cut.py BETA M N_STOP
import sys
import time
from math import exp, log

import numpy as np
from scipy.integrate import solve_ivp

from ComputeTargets.tests import test_unresolved_turnarounds as tut
import importlib

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from Quadrature.supervisors.ScalarField import StateVector

EPS = 1e-6
beta, M, N_stop = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])

t0 = time.perf_counter()
result, policy, rm = tut.window(beta, M, 0.1, N_stop)
wall = time.perf_counter() - t0
cos = policy.cosmology
Mp = policy.units.PlanckMass
sol = result.solution
inj = result.injections
print(f"beta={beta} M={M:g} window to {N_stop}: {len(inj)} candidates, {result.accepted_steps} steps, {wall:.1f} s")


def lnT_of(N, y):
    s = StateVector._make(y)
    Sigma = 1.0 - 3.0 * cos.w(exp(s.log_T_Jordan))
    return s.log_rhorad_Einstein + log(Sigma + exp(s.log_fm)), Sigma


def pi_min_of(N, y, phi_min):
    """pi = s (a + b pi), s = dphi_min/d ln T, d ln T/dN = a + b pi."""
    s_ = StateVector._make(y)
    T_J = exp(s_.log_T_Jordan)
    Sigma = 1.0 - 3.0 * cos.w(T_J)
    fm = exp(s_.log_fm)
    c = rm.d_logOmega_dphi(phi_min)
    D = 1.0 + cos.dG_s_dlogT(T_J) / cos.G_s(T_J) / 3.0
    wp = float(cos.dw_dlogT(T_J))
    q = (3.0 * wp / D + fm * (1.0 - Sigma)) / (Sigma + fm)
    a = Sigma - 4.0 + q
    b = Sigma * c + q * c
    lnT = s_.log_rhorad_Einstein + log(Sigma + fm)
    sd = rm.d_minimum_d_log_trace(phi_min, lnT)
    return sd * a / (1.0 - sd * b), sd, a, b


cut = None
for i, x in enumerate(inj):
    if i == 0:
        continue
    y = sol(x.N)
    if exp(x.log_rho_kin - y[2]) < EPS:
        cut = i
        break
if cut is None:
    print("no cut in the window")
    sys.exit(0)
x = inj[cut]
prev = inj[cut - 1]
y_after = np.array(sol(x.N))
y_before = y_after.copy()
y_before[1] = x.pi_before
print(f"cut at candidate {cut} ({x.kind}), N={x.N:.6f}, T_J={exp(y_after[4]) / policy.units.MeV:.5g} MeV, "
      f"rho_kin/rho_R={exp(x.log_rho_kin - y_after[2]):.3e}, alpha_eff={x.alpha_eff:.4g}, phi={x.phi_turn / Mp:.4e} Mp")

lnT, Sigma = lnT_of(x.N, y_before)
t1 = time.perf_counter()
phi_min = rm.minimum_on_trace(float(y_before[0]), lnT)
t_min = time.perf_counter() - t1
pi_min, sd, a, b = pi_min_of(x.N, y_after, phi_min)
y_park = y_after.copy()
y_park[0], y_park[1] = phi_min, pi_min
lm2 = SM.log_m_eff_squared(rm, phi_min, y_park[2], y_park[3], Sigma)
st = StateVector._make(y_park)
H_park = SM.HubblePolicy(rm, policy.units)(policy(x.N, st), st).H_Einstein
st_b = StateVector._make(y_before)
H_before = SM.HubblePolicy(rm, policy.units)(policy(x.N, st_b), st_b).H_Einstein
print(f"  phi_min={phi_min / Mp:.4e} Mp (phi/phi_min={x.phi_turn / phi_min:.4g}), pi_min={pi_min / Mp:.4e} Mp, "
      f"dphi_min/dlnT={sd / Mp:.3e} Mp, a={a:.4g}, b*Mp={b * Mp:.4g}, minimum_on_trace {t_min * 1e6:.0f} us")
print(f"  m_eff/H_E at phi_min = {exp(0.5 * lm2) / H_park:.4g}; Sigma={Sigma:.4g}, f_m={exp(y_park[3]):.4g}")
print(f"  H_park/H_before - 1 = {H_park / H_before - 1:.3e} (ledger not given the leftover; leftover/rho_R = {exp(x.log_rho_kin - y_after[2]) * (1 - x.alpha_eff):.3e})")
print(f"  Delta ln A at the switch = {rm.log_Omega(phi_min) - rm.log_Omega(x.phi_turn):.3e}")
Ns = np.linspace(prev.N, x.N, 4001)
phis = sol(Ns)[0]
lnA = np.array([rm.log_Omega(p) for p in phis])
print(f"  phi over the oscillation since the previous candidate: [{phis.min() / Mp:.4e}, {phis.max() / Mp:.4e}] Mp; "
      f"phi_min inside: {phis.min() <= phi_min <= phis.max()}; ln A range {lnA.max() - lnA.min():.3e}")

# the reduced integration over the window
calls = [0, 0.0]
last = [phi_min]


def reduced(N, z):
    y = np.array([last[0], 0.0, z[0], z[1], z[2]])
    lnT_, _ = lnT_of(N, y)
    t = time.perf_counter()
    pm = rm.minimum_on_trace(last[0], lnT_)
    calls[1] += time.perf_counter() - t
    last[0] = pm
    y[0] = pm
    y[1] = pi_min_of(N, y, pm)[0]
    calls[0] += 1
    d = policy(N, StateVector._make(y))
    dy = SM.ODERHS("probe", policy)(N, y, _Sup())
    return [dy[2], dy[3], dy[4]]


class _Sup:
    notify_available = False

    def notify_new_RHS(self, s):
        pass


import contextlib, io
from Quadrature.supervisors.ScalarField import ScalarFieldIntegrationSupervisor

N_end = min(N_stop, result.N_final)
with contextlib.redirect_stdout(io.StringIO()):
    sup = ScalarFieldIntegrationSupervisor(policy.units, 1.0, 1.0, label="probe", collect_full_statistics=False)


def reduced2(N, z):
    y = np.array([last[0], 0.0, z[0], z[1], z[2]])
    lnT_, _ = lnT_of(N, y)
    t = time.perf_counter()
    pm = rm.minimum_on_trace(last[0], lnT_)
    calls[1] += time.perf_counter() - t
    last[0] = pm
    y[0] = pm
    y[1] = pi_min_of(N, y, pm)[0]
    calls[0] += 1
    dy = SM.ODERHS("probe", policy)(N, y, sup)
    return [dy[2], dy[3], dy[4]]


with contextlib.redirect_stdout(io.StringIO()):
    sup.__enter__()
    t2 = time.perf_counter()
    r = solve_ivp(reduced2, (x.N, N_end), y_park[2:], method="DOP853", rtol=1e-10, atol=1e-12,
                  max_step=0.1, dense_output=True)
    t_red = time.perf_counter() - t2
print(f"  reduced DOP853 over [{x.N:.4f}, {N_end:.4f}]: {r.status} {r.nfev} RHS, {len(r.t) - 1} steps, {t_red:.2f} s, "
      f"minimum_on_trace {calls[1] / max(calls[0], 1) * 1e6:.0f} us/call")
# compare with the oscillating history
Nq = np.linspace(x.N, N_end, 401)
osc = sol(Nq)
park = r.sol(Nq)
for k, name in ((0, "ln rho_R,E"), (1, "ln f_m"), (2, "ln T_J")):
    d = park[k] - osc[k + 2]
    print(f"    {name}: parked - oscillating: end {d[-1]:+.3e}, max |.| {np.max(np.abs(d)):.3e}")
lnA_osc = np.array([rm.log_Omega(p) for p in osc[0]])
phm = []
for N_, z in zip(Nq, park.T):
    y = np.array([phi_min, 0.0, *z])
    lnT_, _ = lnT_of(N_, y)
    phm.append(rm.minimum_on_trace(phm[-1] if phm else phi_min, lnT_))
phm = np.array(phm)
lnA_park = np.array([rm.log_Omega(p) for p in phm])
print(f"    ln A: oscillating mean over window {lnA_osc.mean():.6e}, parked mean {lnA_park.mean():.6e}, "
      f"diff {lnA_osc.mean() - lnA_park.mean():+.3e}")
# pi_min against a finite difference of phi_min along the parked solution
dphi = np.gradient(phm, Nq)
pis = []
for N_, z, pm in zip(Nq, park.T, phm):
    y = np.array([pm, 0.0, *z])
    pis.append(pi_min_of(N_, y, pm)[0])
pis = np.array(pis)
rel = np.abs(dphi[2:-2] - pis[2:-2]) / np.abs(pis[2:-2])
print(f"    pi_min vs finite difference of phi_min: median rel {np.median(rel):.2e}, max rel {rel.max():.2e}; "
      f"pi_min range [{pis.min() / Mp:.3e}, {pis.max() / Mp:.3e}] Mp")
