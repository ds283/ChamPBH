# Probe for inelastic-reflection (2026-10-29, the user's request): does the transition from the
# bouncing phase to late-time tracking converge as M decreases? One history per call, at alpha = 0,
# untuned at the model's seed realization, with test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0,
# T* = 2e4 GeV, to T_CMB, main.py's axis, default tolerances), code at 20bfafa. Read-only: nothing is
# stored, no Ray. Everything is computed after the run from the dense output and the TurningPoints.
#   (a) every turning point: N, kind, phi, ln T_J, and phi_min(N), the minimum of
#       V_eff = V + A rho~_c with the full trace rho_R,E (Sigma + f_m) held at fixed rho~_c;
#   (c) per e-fold of N, averages over the dense output (9 points per accepted step, trapezoid):
#       H_J/H_E = (1 + beta pi/M_P)/A, its square, pi^2/6 M_P^2, ln A, rho_kin/rho_R = H_E^2 pi^2/2 rho_R,E,
#       and BBN's r = (3 M_P^2 H_J^2 - rho_R,J (1 + f_m))/rho_R,J; the same r averaged over each cell
#       of the stored z grid, and r at the stored samples themselves (what BBN reads);
#   (d) today: phi_0, pi_0, pi_0/(1.5 phi_0), and _tuning_state's residual_H and phi_min.
# Usage: transition.py BETA M_OVER_MP OUT.json
import json
import resource
import sys
import time
from math import exp, log

import numpy as np
from scipy.optimize import brentq

from ComputeTargets.tests import test_injection as T
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

SM = T.SM
beta, M_Mp, out = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
record = {"beta": beta, "M_over_Mp": M_Mp, "alpha": 0.0, "code": "20bfafa"}

t0 = time.perf_counter()
try:
    data, r, _ = T.integrate_captured(beta, M_Mp, 0.0)
except Exception as e:  # a failed history (the step budget, say) is a result
    record.update(failure=repr(e), wall_time_s=time.perf_counter() - t0)
    json.dump(record, open(out, "w"), indent=1)
    sys.exit(0)
record["wall_time_s"] = time.perf_counter() - t0
record.update(accepted_steps=r.accepted_steps, nfev=r.nfev, N_final=r.N_final,
              reflections=len(r.reflections), turning_points=len(r.turning_points))

# the policy integrate_history used, rebuilt (no RHS recorder: it would hold every RHS call)
cos = T.gth.make_cosmology()
u = cos.units
_model = ExponentialModel(0, M_value(0, M_Mp * u.PlanckMass), 1, beta_value(0, beta), u)
_seed = _model.seed(cos)
rm = _model.realize(_seed.normalization, _seed.phi_ref)
policy = SM.ODEPolicy("probe", cos, rm, dark_radiation=data["dark_radiation"])
Mp = u.PlanckMass
M = rm._model._M_float
n = rm._model._n
lnV0 = rm.normalization
b = rm.d_logOmega_dphi(1.0)  # beta/M_P
c_Om = rm.log_Omega(Mp) - b * Mp  # ln A(phi) = b phi + c_Om
record.update(normalization=lnV0, phi_ref_over_M=rm.phi_ref / M)


def phi_min_at(N, y):
    """Root of V'(x) + (ln A)' trace A(x)/A(phi) = 0 in u = M/x (n = 1 form, in logs)."""
    st = StateVector._make(y)
    Sigma = 1.0 - 3.0 * float(cos.w(exp(st.log_T_Jordan)))
    ln_tr = st.log_rhorad_Einstein + log(Sigma + exp(st.log_fm))

    def g(uu):  # ln|V'| - ln(b trace A(x)/A(phi)), x = M/uu
        x = M / uu
        return lnV0 + uu ** n + log(n * uu ** n / x) - (log(b) + ln_tr + b * (x - st.phi_Einstein))

    return M / brentq(g, 1e-8, 2000.0, xtol=1e-14, rtol=1e-13)


tps = []
for t in r.turning_points:
    y = r.solution(t.N)
    try:
        pm = phi_min_at(t.N, y)
    except ValueError:
        pm = None
    tps.append([t.N, t.kind, t.phi_Einstein / M, t.log_T_Jordan - log(u.MeV), None if pm is None else pm / M])
record["turning_point_rows"] = "N, kind, phi/M, ln(T_J/MeV), phi_min/M"
record["turning_points_list"] = tps

# (c) per e-fold bins and per stored-z-grid cell
samples = [SM.SampleValues._make(tuple(s)) for s in data["sample"]]
sN = np.array(sorted(s.raw_N for s in samples))
nb = int(np.ceil(r.N_final)) + 1
names = ["HJ_over_HE", "HJ_over_HE_sq", "pi2_over_6Mp2", "ln_A", "rho_kin_over_rho_R", "r", "ln_TJ_MeV"]
acc = {k: np.zeros(nb) for k in names}
wsum = np.zeros(nb)
cacc = np.zeros(len(sN) + 1)
cw = np.zeros(len(sN) + 1)
P = 9
tw = np.ones(P); tw[0] = tw[-1] = 0.5
ts = r.solution.ts
for k, it in enumerate(r.solution.interpolants):
    Ns = np.linspace(ts[k], ts[k + 1], P)
    phi, pi_, lrE, lfm, lTJ = it(Ns)
    G = 1.0 - pi_ * pi_ / (6.0 * Mp * Mp)
    rhoR = np.exp(lrE); fm = np.exp(lfm)
    H2 = (np.exp(lnV0 + (M / phi) ** n) + rhoR * (1.0 + fm)) / (3.0 * Mp * Mp * G)
    lnA = b * phi + c_Om
    q = (1.0 + b * pi_) / np.exp(lnA)
    rhoRJ = rhoR * np.exp(-4.0 * lnA)
    rr = (3.0 * Mp * Mp * H2 * q * q - rhoRJ * (1.0 + fm)) / rhoRJ
    vals = {"HJ_over_HE": q, "HJ_over_HE_sq": q * q, "pi2_over_6Mp2": pi_ * pi_ / (6.0 * Mp * Mp),
            "ln_A": lnA, "rho_kin_over_rho_R": 0.5 * H2 * pi_ * pi_ / rhoR, "r": rr,
            "ln_TJ_MeV": lTJ - log(u.MeV)}
    w = tw * (ts[k + 1] - ts[k]) / (P - 1)
    bins = np.floor(Ns).astype(int)
    np.add.at(wsum, bins, w)
    for key in names:
        np.add.at(acc[key], bins, w * vals[key])
    cells = np.searchsorted(sN, Ns, side="left")
    np.add.at(cacc, cells, w * rr)
    np.add.at(cw, cells, w)
ok = wsum > 0
record["efold_bins"] = {"N": [float(i) for i in np.nonzero(ok)[0]],
                        **{key: [float(x) for x in acc[key][ok] / wsum[ok]] for key in names}}
record["efold_bins"]["HJ_over_HE_rms"] = [
    float(sqrt_) for sqrt_ in np.sqrt(np.maximum(acc["HJ_over_HE_sq"][ok] / wsum[ok] - (acc["HJ_over_HE"][ok] / wsum[ok]) ** 2, 0.0))]
okc = cw > 0
# cell i holds sN[i-1] < N <= sN[i]; report it at its upper edge's T_J
order = sorted(samples, key=lambda s: s.raw_N)
cellT = [order[min(i, len(order) - 1)].log_T_Jordan - log(u.MeV) for i in range(len(sN) + 1)]
record["cells"] = {"ln_TJ_MeV_upper": [float(cellT[i]) for i in np.nonzero(okc)[0]],
                   "r_mean": [float(x) for x in cacc[okc] / cw[okc]]}
record["stored_samples"] = {
    "ln_TJ_MeV": [s.log_T_Jordan - log(u.MeV) for s in order],
    "r": [(3.0 * Mp * Mp * s.H_Jordan ** 2 - exp(s.log_rhorad_Jordan) * (1.0 + exp(s.log_fm))) / exp(s.log_rhorad_Jordan) for s in order],
    "phi_over_M": [s.phi_Einstein / M for s in order],
}

# (d) today
model = ExponentialModel(0, M_value(0, M_Mp * Mp), 1, beta_value(0, beta), u)
st = SM._tuning_state(cos, model, rm, data)
record["today"] = {"phi_0_over_M": st.phi_0 / M, "pi_0": st.pi_0, "tracking": st.pi_0 / (1.5 * st.phi_0),
                   "residual_H": st.residual_H, "phi_min_over_M": st.phi_min / M}
record["max_rss_MB"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20
record["total_time_s"] = time.perf_counter() - t0
json.dump(record, open(out, "w"))
print(f"done beta={beta} M={M_Mp:g}: steps={r.accepted_steps} tps={len(r.turning_points)} "
      f"wall={record['wall_time_s']:.1f}s total={record['total_time_s']:.1f}s rss={record['max_rss_MB']:.0f}MB")
