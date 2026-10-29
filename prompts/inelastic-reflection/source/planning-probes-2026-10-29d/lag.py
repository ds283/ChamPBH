# The lag after the release against the first-order adiabatic estimate. Usage: lag.py BETA M DN
# Runs the released history to N_rel + DN, then on a dense grid after N_rel:
#   phi_min(N) on the history's own trace (minimum_on_trace), pi_min = tracker_velocity;
#   delta1 = (d pi_min/dN - F_pi(phi_min, pi_min)) / (dF_pi/dphi at phi_min), F_pi the RHS's pi' (the
#   code's own equation, linearised about the zero-lag tracker);
# and per oscillation (wall to wall turning point) the mean of phi - phi_min against the mean of delta1.
import contextlib, io, os, sys
from math import exp, log
from unittest import mock
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_patch as RP
SM = RP.SM
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

beta, M, DN = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos); rm = model.realize(seed.normalization, seed.phi_ref)
out = {}
# first pass: find N_rel
def cap(RHS, sup, *a, **kw):
    if "N_stop_" in out:
        kw["N_stop"] = out["N_stop_"]
    out["RHS"], out["sup"] = RHS, sup
    r = RP.patched(RHS, sup, *a, **kw); out["r"] = r; return r
def run():
    with mock.patch.object(SM, "integrate_scalar_history", cap), contextlib.redirect_stdout(io.StringIO()):
        return SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
            phi_value(0, 5.0 * Mp), pi_value(0, 0.0), gth.make_z_grid(), rm, task_label="orch07", alpha=0.1)
run()
N_rel = RP.PROBE["release_record"][0]
RP.PROBE.clear(); RP.PROBE["release"] = True
out["N_stop_"] = N_rel + DN
run()
r, RHS, sup = out["r"], out["RHS"], out["sup"]
policy = RHS.policy
assert abs(RP.PROBE["release_record"][0] - N_rel) == 0.0
Ns = np.linspace(N_rel, r.N_final, int(DN * 2000) + 1)
Y = np.array([r.solution(N) for N in Ns])
phi_min = np.empty_like(Ns); pi_min = np.empty_like(Ns); mH = np.empty_like(Ns)
anchor = float(Y[0, 0])
for j, (N, y) in enumerate(zip(Ns, Y)):
    pm, pim = SM.parked_tracker(rm, cos, anchor, y[2:])
    phi_min[j], pi_min[j] = pm, pim
    mH[j] = SM.tracker_m_eff_over_H(policy, SM.HubblePolicy(rm, u), N, np.array([pm, pim, *y[2:]]))[0]
dpim = np.gradient(pi_min, Ns)
delta1 = np.empty_like(Ns)
for j, (N, y) in enumerate(zip(Ns, Y)):
    yt = np.array([phi_min[j], pi_min[j], *y[2:]])
    F0 = RHS(N, yt, sup)[1]
    h = 1e-6 * phi_min[j]
    Fp = RHS(N, yt + np.array([h, 0, 0, 0, 0]), sup)[1]; Fm = RHS(N, yt - np.array([h, 0, 0, 0, 0]), sup)[1]
    dF = (Fp - Fm) / (2 * h)
    delta1[j] = (dpim[j] - F0) / dF
dev = Y[:, 0] - phi_min
print(f"beta={beta} M={M}: released at N={N_rel:.6f}; window {DN} e-folds; m/H from {mH[0]:.4g} to {mH[-1]:.4g}")
print(f"   at release: delta1/phi_min = {delta1[0]/phi_min[0]:.4g}; pi_min/phi_min = {pi_min[0]/phi_min[0]:.4g}")
walls = [tp.N for tp in r.turning_points if tp.N > N_rel and tp.kind == "wall"]
if len(walls) >= 2:
    for a, b in list(zip(walls[:-1], walls[1:]))[:8]:
        m = (Ns >= a) & (Ns <= b)
        md = np.trapezoid(dev[m], Ns[m]) / (b - a); m1 = np.trapezoid(delta1[m], Ns[m]) / (b - a)
        amp = 0.5 * (dev[m].max() - dev[m].min())
        print(f"   [{a:.4f}, {b:.4f}] m/H~{np.mean(mH[m]):.3g}: mean(phi-phi_min)/phi_min={md/np.mean(phi_min[m]):.4g}, "
              f"mean delta1/phi_min={m1/np.mean(phi_min[m]):.4g}, ratio {md/m1:.4f}; half-swing/|delta1| {amp/abs(m1):.3g}")
else:
    for k in range(0, len(Ns), max(1, len(Ns) // 10)):
        print(f"   N={Ns[k]:.4f} m/H={mH[k]:.3g} (phi-phi_min)/phi_min={dev[k]/phi_min[k]:.4g} delta1/phi_min={delta1[k]/phi_min[k]:.4g}")
