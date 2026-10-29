# tune_history at alpha = 0.1, main.py's defaults otherwise, with the release patch. Usage: tune.py BETA M
import contextlib, io, os, sys, time
from math import exp
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_patch as RP
SM = RP.SM
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
beta, M = float(sys.argv[1]), float(sys.argv[2])
cos = gth.make_cosmology(); u = cos.units
model = ExponentialModel(0, M_value(0, M * u.PlanckMass), 1, beta_value(0, beta), u)
recs = []
def wrapped(*a, **kw):
    RP.PROBE.clear(); RP.PROBE["release"] = True
    r = RP.patched(*a, **kw)
    rel = RP.PROBE.get("release_record")
    recs.append((None if r.parking is None else round(r.parking.N, 6), None if rel is None else (round(rel[0], 6), exp(rel[1][4]) / u.MeV)))
    return r
buf = io.StringIO(); t0 = time.perf_counter()
with mock.patch.object(SM, "integrate_scalar_history", wrapped), contextlib.redirect_stdout(buf):
    out = SM.tune_history(cos, model, temperature(0, 2.0e4 * u.GeV), phi_value(0, 5.0 * u.PlanckMass), pi_value(0, 0.0),
                          gth.make_z_grid(), task_label=f"b={beta} M={M:g}", progress="every", alpha=0.1)
print(f"== beta={beta} M={M:g} wall={time.perf_counter()-t0:.1f}s failure={out.get('failure_reason')} iterations={out.get('tuning_iterations')} "
      f"residual_H={out.get('residual_H')} residual_phi={out.get('residual_phi')}")
for line in buf.getvalue().splitlines():
    if line.startswith("-- tune") or line.startswith("!!"):
        print("   ", line)
for k, rec in enumerate(recs):
    print("   integration", k, "parked N", rec[0], "released (N, T_J MeV)", rec[1])
