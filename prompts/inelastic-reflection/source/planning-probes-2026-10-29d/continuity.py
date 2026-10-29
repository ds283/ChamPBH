# The release's continuity: the parked segment and the first oscillating segment at N_rel, H_E either side,
# and m_eff/H_E at the release. Usage: continuity.py BETA M DN
import contextlib, io, os, sys, time
from unittest import mock
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_patch as RP
SM = RP.SM
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
beta, M, DN = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos); rm = model.realize(seed.normalization, seed.phi_ref)
out = {}
def cap(RHS, sup, *a, **kw):
    if "N_stop_" in out: kw["N_stop"] = out["N_stop_"]
    out["RHS"] = RHS; r = RP.patched(RHS, sup, *a, **kw); out["r"] = r; return r
def run():
    with mock.patch.object(SM, "integrate_scalar_history", cap), contextlib.redirect_stdout(io.StringIO()):
        SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
            phi_value(0, 5.0 * Mp), pi_value(0, 0.0), gth.make_z_grid(), rm, task_label="orch07", alpha=0.1)
run(); N_rel = RP.PROBE["release_record"][0]
RP.PROBE.clear(); RP.PROBE["release"] = True; out["N_stop_"] = N_rel + DN; run()
r = out["r"]; ts = list(r.solution.ts); k = ts.index(N_rel)
left, right = r.solution.interpolants[k - 1], r.solution.interpolants[k]
yl, yr, yd = left(N_rel), right(N_rel), r.solution(N_rel)
policy = out["RHS"].policy; hub = SM.HubblePolicy(rm, u)
print(f"beta={beta} M={M}: N_rel={N_rel:.10f}; segment types {type(left).__name__} | {type(right).__name__}; ts monotone {all(a < b for a, b in zip(ts, ts[1:]))}")
print(f"   left == right bitwise: {np.array_equal(yl, yr)}; dense output == right: {np.array_equal(yd, yr)}; max |diff| {np.max(np.abs(yl - yr)):.3g}")
mH_l, H_l = SM.tracker_m_eff_over_H(policy, hub, N_rel, yl)
print(f"   m_eff/H_E at the release {mH_l!r} (10 - it = {10 - mH_l:.3g})")
t = time.perf_counter()
for _ in range(200):
    SM.tracker_m_eff_over_H(policy, hub, N_rel, left(N_rel))
print(f"   one m/H evaluation on the segment: {(time.perf_counter() - t) / 200 * 1e6:.1f} us")
