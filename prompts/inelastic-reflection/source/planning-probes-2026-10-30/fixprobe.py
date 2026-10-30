# Planning probe for prompt 07b: the wall search with ln(V - V_floor) treated as -inf within K ulps of ln V.
# Usage: fixprobe.py BETA M K   (K = 0: the shipped code). Pickles the trajectory for comparison.
import contextlib, io, os, sys, tempfile, time, pickle, importlib
from math import log, ulp, expm1
from unittest import mock
AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
beta, M, K = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
orig = AM._log_V_minus_floor
def patched(log_V, log_V_floor):
    if log_V - log_V_floor <= K * ulp(max(abs(log_V), abs(log_V_floor))):
        return float("-inf")
    return orig(log_V, log_V_floor)
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos); rm = model.realize(seed.normalization, seed.phi_ref)
out = {}; inner = SM.integrate_scalar_history
def cap(RHS, sup, *a, **kw):
    r = inner(RHS, sup, *a, **kw); out["r"] = r; return r
t0 = time.perf_counter()
with contextlib.ExitStack() as st:
    st.enter_context(mock.patch.object(SM, "integrate_scalar_history", cap))
    if K > 0: st.enter_context(mock.patch.object(AM, "_log_V_minus_floor", patched))
    st.enter_context(contextlib.redirect_stdout(io.StringIO()))
    data = SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
        phi_value(0, 5.0 * Mp), pi_value(0, 0.0), gth.make_z_grid(), rm, task_label="fix07b", alpha=0.1)
wall = time.perf_counter() - t0
tag = f"b{beta:g}_M{M:g}_K{K:g}"
if data.get("failure"):
    print(tag, f"FAILED ({wall:.1f}s):", data["failure_reason"][:400]); res = dict(fail=data["failure_reason"])
else:
    r = out["r"]; ct = [x.classical_turning_point for x in r.injections]
    res = dict(N_final=r.N_final, final=tuple(r.final_state), ts=list(r.solution.ts), nfev=r.nfev,
               inj=[x._replace(classical_turning_point=None, phi_assessed=None, log_E_assessed=None) for x in r.injections],
               ct=ct, refl=list(r.reflections), parking=r.parking, release=r.release, sample=[tuple(s) for s in data["sample"]])
    kinds = {}
    for x in r.injections: kinds[x.kind] = kinds.get(x.kind, 0) + 1
    lr = [c.log_R / log(10) for c in ct if c is not None]
    print(tag, f"ok N_final={r.N_final:.5f} steps={r.accepted_steps} wall={wall:.1f}s candidates={kinds} "
          f"parks={'#%d N=%.4f' % (r.parking.candidate_index, r.parking.N) if r.parking else None} released={r.release is not None} "
          f"log10R {min(lr):.2f}..{max(lr):.2f}" if lr else "")
pickle.dump(res, open(os.path.join(os.environ.get("PROBE_OUT", tempfile.gettempdir()), f"fix_{tag}.pkl"), "wb"))
