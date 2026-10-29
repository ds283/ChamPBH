# The orchestrator's probe of the release (prompt 07), at 7b75ae7. Usage:
#   probe_release.py BETA M MODE [N_STOP]
# MODE: release (release_patch), eps0 (the cut disabled, the never-parked history), shipped (06 as is).
# One history at test_injection's setup, alpha = 0.1; pickles what the comparison needs.
import contextlib, io, os, pickle, sys, tempfile, time
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

beta, M, mode = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
N_stop = float(sys.argv[4]) if len(sys.argv) > 4 else None
cosmology = gth.make_cosmology(); u = cosmology.units; Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cosmology); rm = model.realize(seed.normalization, seed.phi_ref)

inner = RP.patched if mode == "release" else SM.integrate_scalar_history
out = {}


def capturing(RHS, supervisor, *a, **kw):
    if N_stop is not None:
        kw["N_stop"] = N_stop
    out["RHS"], out["supervisor"] = RHS, supervisor
    try:
        r = inner(RHS, supervisor, *a, **kw)
    except Exception as e:
        out["error"] = getattr(e, "message", str(e))
        raise
    out["r"] = r
    return r


t0 = time.perf_counter()
with contextlib.ExitStack() as st:
    st.enter_context(mock.patch.object(SM, "integrate_scalar_history", capturing))
    if mode == "eps0":
        st.enter_context(mock.patch.object(SM, "PARKING_EPSILON", 0.0))
    st.enter_context(contextlib.redirect_stdout(io.StringIO()))
    data = SM.integrate_history(cosmology, temperature(0, 2.0e4 * u.GeV),
        temperature(1, cosmology.T_CMB_Kelvin * u.Kelvin), phi_value(0, 5.0 * Mp), pi_value(0, 0.0),
        gth.make_z_grid(), rm, task_label="orch07", alpha=0.1)
wall = time.perf_counter() - t0
tag = f"b{beta:g}_M{M:g}_{mode}" + ("" if N_stop is None else f"_N{N_stop:g}")
if data.get("failure", False) or "r" not in out:
    print(f"{tag}: FAILED in {wall:.1f} s: {data.get('failure_reason', out.get('error'))}")
    sys.exit(0)
r = out["r"]
MeV = u.MeV
pk = r.parking
rel = RP.PROBE.get("release_record") if mode == "release" else None
print(f"{tag}: OK N_final={r.N_final:.6f} steps={r.accepted_steps} nfev={r.nfev} candidates={len(r.injections)} "
      f"reflections={len(r.reflections)} turning_points={len(r.turning_points)} wall={wall:.1f} s")
if pk is not None:
    print(f"   parked at #{pk.candidate_index} N={pk.N:.6f} T_J={exp(pk.log_T_Jordan)/MeV:.5g} MeV m/H={pk.m_eff_over_H:.4g}; "
          f"parked steps {pk.parked_steps}, min m/H {pk.min_m_eff_over_H:.4g}")
if rel is not None:
    N_rel, y_rel, psteps, minmh, acc = rel
    print(f"   released at N={N_rel:.8f} T_J={exp(y_rel[4])/MeV:.5g} MeV (step {RP.PROBE['release_step']}, m/H at its end "
          f"{RP.PROBE['release_m_over_H_at_step_end']:.5g}); phi={y_rel[0]/Mp:.6g} Mp pi={y_rel[1]/Mp:.4g} Mp; steps before release {acc}")
    after = [x for x in r.injections if x.N > N_rel]
    tps = [x for x in r.turning_points if x.N > N_rel]
    refl = [x for x in r.reflections if x.N > N_rel]
    print(f"   after release: {len(tps)} turning points ({sum(1 for x in tps if x.kind=='wall')} wall), {len(after)} candidates "
          f"({', '.join(sorted(set(x.kind for x in after)))}), {len(refl)} reflections; "
          f"sum drho/rho_R after = {sum(x.delta_rho_over_rho_rad for x in after):.4g}")
    for x in after[:6]:
        print(f"      #{r.injections.index(x)} {x.kind} N={x.N:.5f} T_J={exp(r.solution(x.N)[4])/MeV:.4g} MeV alpha_eff={x.alpha_eff:.3g} "
              f"w={x.w_gate} A={x.nonadiabaticity:.3g} drho/rho={x.delta_rho_over_rho_rad:.3g} ratio={exp(x.log_rho_kin - r.solution(x.N)[2]):.3g}")
K_end = None
try:
    K_end = data["dark_radiation"].K(r.N_final) if hasattr(data["dark_radiation"], "K") else None
except Exception:
    pass
fixed = {k: data.get(k) for k in ("T_1MeV", "T_70keV") if k in data}
with open(os.path.join(os.environ.get("PROBE_OUT", tempfile.gettempdir()), f"{tag}.pkl"), "wb") as f:
    pickle.dump(dict(ts=list(r.solution.ts), N_final=r.N_final, final_state=tuple(r.final_state),
                     injections=r.injections, release=rel, parking=pk,
                     samples={N: tuple(r.solution(N)) for N in np.linspace(r.solution.ts[0], r.N_final, 4001)}), f)
