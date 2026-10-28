# Planning probe for inelastic-reflection 02: turning points of phi (sign changes of pi) per history
# at M = 0.5 M_P, untuned at the model's seed, to T_CMB, alpha = 0 (code at b30f7bd). Read-only.
import sys, time, importlib
import numpy as np
from math import exp, log
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from unittest import mock

cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
z = gth.make_z_grid()
for beta in [float(b) for b in sys.argv[1:]]:
    model = ExponentialModel(0, M_value(0, 0.5 * Mp), 1, beta_value(0, beta), u)
    seed = model.seed(cos)
    rm = model.realize(seed.normalization, seed.phi_ref)
    cap = []
    orig = SM.integrate_scalar_history
    def capturing(*a, **k):
        r = orig(*a, **k); cap.append(r); return r
    t0 = time.perf_counter()
    with mock.patch.object(SM, "integrate_scalar_history", capturing):
        data = SM.integrate_history(cos, temperature(0, 2e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
                                    phi_value(0, 5 * Mp), pi_value(0, 0.0), z, rm, task_label="probe")
    wall = time.perf_counter() - t0
    if data.get("failure"):
        print(f"beta={beta} FAILED {data['failure_reason']} wall={wall:.1f}"); continue
    r = cap[-1]; sol = r.solution
    wall_side = plateau = interior_pairs = 0
    first_T = []
    for k, interp in enumerate(sol.interpolants):
        a, b = sol.ts[k], sol.ts[k + 1]
        pa, pb = interp(a)[1], interp(b)[1]
        if pa < 0 <= pb or (pa < 0 and pb > 0): wall_side += 1; first_T.append(interp(b)[4])
        elif pa > 0 >= pb: plateau += 1
        tt = np.linspace(a, b, 17); p = interp(tt)[1]
        nch = int(np.sum(np.sign(p[1:]) != np.sign(p[:-1])))
        if nch >= 2: interior_pairs += 1
    Tj = [exp(x) / u.MeV for x in first_T]
    print(f"beta={beta} wall={wall:.1f}s steps={r.accepted_steps} reflections={len(r.reflections)} "
          f"wall_side={wall_side} plateau={plateau} steps_with_2+_changes={interior_pairs} "
          f"first_wall_side_TJ_MeV={Tj[:3]} last={Tj[-3:] if Tj else None}", flush=True)
