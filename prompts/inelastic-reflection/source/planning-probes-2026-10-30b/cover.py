# Planning probe for prompt 08 (2026-10-30b), derived from ../planning-probes-2026-10-30/peaks.py: does the margin at the turnarounds bound the field-mass margin before the late tracker? Usage: cover.py BETA M [ALPHA]
import contextlib, io, os, sys, tempfile, time, pickle, importlib
from math import exp, log, sqrt, pi
from unittest import mock
import numpy as np
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

beta, M = float(sys.argv[1]), float(sys.argv[2]); alpha = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass; MeV = u.MeV
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos); rm = model.realize(seed.normalization, seed.phi_ref)
out = {}
inner = SM.integrate_scalar_history
def cap(RHS, sup, *a, **kw):
    out["RHS"] = RHS; r = inner(RHS, sup, *a, **kw); out["r"] = r; return r
t0 = time.perf_counter()
with mock.patch.object(SM, "integrate_scalar_history", cap), contextlib.redirect_stdout(io.StringIO()):
    data = SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
        phi_value(0, 5.0 * Mp), pi_value(0, 0.0), gth.make_z_grid(), rm, task_label="plan08", alpha=alpha)
wall = time.perf_counter() - t0
tag = f"b{beta:g}_M{M:g}_a{alpha:g}"
if data.get("failure"):
    print(tag, "FAILED", data.get("failure_reason")); sys.exit()
r = out["r"]; policy = out["RHS"].policy; ledger = policy.dark_radiation
K_final = ledger.K(r.N_final)
LN10 = log(10.0)
t1 = time.perf_counter()
gens = []
n_none = n_gap = n_ol = n_cl = 0
for j, x in enumerate(r.injections):
    if x.delta_K <= 0.0: continue
    ct = x.classical_turning_point
    g = dict(j=j, N=x.N, N_turn=x.N_turn, kind=x.kind, share=x.delta_K / K_final, choice=None)
    if ct is None:
        n_none += 1; g["choice"] = "none"; gens.append(g); continue
    g["log_R"] = ct.log_R; g["ell_cl"] = ct.ell_t
    if ct.log_R <= 0.0:
        g.update(choice="classical", log_k=ct.log_k_ex, log_m=0.5 * ct.log_omega_t_squared); n_cl += 1
    else:
        try:
            ol = rm.one_loop_turnaround(x.phi_assessed, x.log_E_assessed)
        except Exception as e:
            ol = e
        if isinstance(ol, Exception):
            g.update(choice="gap", log_k=ct.log_k_ex, log_m=0.5 * ct.log_omega_t_squared, err=str(ol)[:80]); n_gap += 1
        elif ol is None:
            g.update(choice="ol_none", log_k=ct.log_k_ex, log_m=0.5 * ct.log_omega_t_squared); n_gap += 1
        else:
            g.update(choice="one_loop", log_k=ol.log_k_ex, log_m=ol.log_m_ta, ell=ol.ell, dk=ol.log_k_ex - ct.log_k_ex); n_ol += 1
    g["log_q"] = g["log_k"] + g["N_turn"]
    g["birth"] = g["log_k"] - g["log_m"]
    gens.append(g)
t_peak = time.perf_counter() - t1
# later turnarounds: every generation's own (N_turn, log_m)
turns = [(g["N_turn"], g["log_m"]) for g in gens if "log_m" in g]
# tracker on samples: phi_min at each sample, m_eff^2 there
t2 = time.perf_counter()
S = [SM.SampleValues._make(tuple(s)) for s in data["sample"]]
Ns = np.array([s.raw_N for s in S]); logm_tr = np.empty(len(S)); logm_fd = np.empty(len(S)); anchor = None
for i, s in enumerate(S):
    log_T = s.log_rhorad_Einstein + log(s.Sigma + exp(s.log_fm))
    a0 = s.phi_Einstein if anchor is None else anchor
    try:
        pm = rm.minimum_on_trace(a0, log_T)
    except Exception:
        pm = rm.minimum_on_trace(s.phi_Einstein, log_T)
    anchor = pm
    logm_tr[i] = 0.5 * SM.log_m_eff_squared(rm, pm, s.log_rhorad_Einstein, s.log_fm, s.Sigma)
    logm_fd[i] = 0.5 * SM.log_m_eff_squared(rm, s.phi_Einstein, s.log_rhorad_Einstein, s.log_fm, s.Sigma)
# today: the final state
f = r.final_state
fs = policy(r.N_final, f)
log_T_f = f.log_rhorad_Einstein + log(fs.Sigma + exp(f.log_fm))
pm_f = rm.minimum_on_trace(anchor, log_T_f)
logm_today = 0.5 * SM.log_m_eff_squared(rm, pm_f, f.log_rhorad_Einstein, f.log_fm, fs.Sigma)
logm_field_today = 0.5 * SM.log_m_eff_squared(rm, f.phi_Einstein, f.log_rhorad_Einstein, f.log_fm, fs.Sigma)
t_tr = time.perf_counter() - t2
for g in gens:
    if "log_q" not in g: continue
    later = [(N, lm) for (N, lm) in turns if N > g["N_turn"]]
    if later:
        vals = [g["log_q"] - N - lm for N, lm in later]; k = int(np.argmin(vals))
        g["refl"] = vals[k]; g["refl_N"] = later[k][0]
    m = Ns > g["N_turn"]
    vals = g["log_q"] - Ns[m] - logm_tr[m]
    vt = g["log_q"] - r.N_final - logm_today
    if m.any() and vals.min() < vt:
        k = int(np.argmin(vals)); g["track"] = vals[k]; g["track_N"] = Ns[m][k]
    else:
        g["track"] = vt; g["track_N"] = r.N_final
res = dict(tag=tag, wall=wall, t_peak=t_peak, t_tr=t_tr, nsamples=len(S), gens=gens, N_final=r.N_final,
           parking=r.parking, release=r.release, leftover_share=(r.parking.delta_K / K_final if r.parking else 0.0),
           logm_today=logm_today, logm_field_today=logm_field_today)
pickle.dump(res, open(os.path.join(os.environ.get("PROBE_OUT", tempfile.gettempdir()), f"{tag}.pkl"), "wb"))
W = [g for g in gens if g["share"] >= 1e-3 and "log_q" in g]
def worst(key):
    c = [g for g in W if key in g]
    if not c: return "—"
    g = min(c, key=lambda g: g[key]); fail = sum(x["share"] for x in c if x[key] < LN10)
    return f"{g[key]:+.3f} (gen #{g['j']} at N={g.get(key + '_N', g['N_turn']):.4f}; failing share {fail:.3g})"
print(f"{tag}: ok N_final={r.N_final:.4f} wall={wall:.1f}s; generations {len(gens)} (classical {n_cl}, one-loop {n_ol}, gap {n_gap}, no assessment {n_none}); "
      f"weighted (>=1e-3) {len(W)} carrying {sum(g['share'] for g in W):.4f}; leftover share {res['leftover_share']:.3g}; parked={r.parking is not None} released={r.release is not None}")
lr = [g["log_R"] for g in gens if "log_R" in g]
if lr: print(f"   ln R / ln 10 from {min(lr)/LN10:.2f} to {max(lr)/LN10:.2f}; ell_cl {min(g['ell_cl'] for g in gens if 'ell_cl' in g):.3f}..{max(g['ell_cl'] for g in gens if 'ell_cl' in g):.3f}")
ol = [g for g in gens if g["choice"] == "one_loop"]
if ol: print(f"   one-loop ell {min(g['ell'] for g in ol):.2f}..{max(g['ell'] for g in ol):.2f}; ln k_ol - ln k_cl {min(g['dk'] for g in ol):.3f}..{max(g['dk'] for g in ol):.3f}")
gp = [g for g in gens if g["choice"] in ("gap", "ol_none")]
if gp: print(f"   gap: {[(g['j'], round(g['log_R']/LN10,2), g.get('err','None')) for g in gp][:5]}")
print(f"   worst birth {worst('birth')}\n   worst refl  {worst('refl')}\n   worst track {worst('track')}")
print(f"   today: ln m on tracker {logm_today:.4f}, on the field {logm_field_today:.4f}; cost: peaks {t_peak*1e3:.1f} ms, tracker over {len(S)} samples {t_tr*1e3:.0f} ms")
top = sorted([g for g in gens if 'log_q' in g], key=lambda g: -g["share"])[:4]
for g in top:
    print(f"   gen #{g['j']} {g['kind']} {g['choice']} N_turn={g['N_turn']:.4f} share={g['share']:.3g} lnR/ln10={g.get('log_R',float('nan'))/LN10:.2f} birth={g['birth']:+.3f} refl={g.get('refl',float('nan')):+.3f} track={g['track']:+.3f}@{g['track_N']:.3f}")


# plateau turnarounds' field mass, on the dense output
plat = []
for tp in r.turning_points:
    if tp.kind != "plateau": continue
    y = r.solution(tp.N); st = SM.StateVector._make(y); d = policy(tp.N, st)
    plat.append((tp.N, 0.5 * SM.log_m_eff_squared(rm, st.phi_Einstein, st.log_rhorad_Einstein, st.log_fm, d.Sigma)))
last_tp = max([tp.N for tp in r.turning_points] + [g["N_turn"] for g in gens])
N_late = last_tp
worst_gap = None; viol = 0; viol_noplat = 0; nchk = 0
for g in W:
    lat = [(N, lm) for (N, lm) in turns if N > g["N_turn"]]
    pl = [(N, lm) for (N, lm) in plat if N > g["N_turn"]]
    refl_w = min([g["log_q"] - N - lm for N, lm in lat], default=float("inf"))
    refl_p = min([g["log_q"] - N - lm for N, lm in pl], default=float("inf"))
    bound = min(g["birth"], refl_w, refl_p); bound_np = min(g["birth"], refl_w)
    m = (Ns > g["N_turn"]) & (Ns <= N_late)
    if not m.any(): continue
    vals = g["log_q"] - Ns[m] - logm_fd[m]; v = vals.min(); nchk += 1
    gap = v - bound; worst_gap = gap if worst_gap is None else min(worst_gap, gap)
    if v < bound: viol += 1
    if v < bound_np: viol_noplat += 1
print(f"   COVER {tag}: plateau TPs {len(plat)}; N_late {N_late:.3f}; gens checked {nchk}; "
      f"min over gens of [field-mass margin before N_late - min(birth, wall turnarounds, plateau)] = {worst_gap:+.4f}; "
      f"gens below that bound {viol}; below the bound without plateau {viol_noplat}")
