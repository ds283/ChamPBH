# Would the parking cut (rho_kin/rho_R,E < EPS at a candidate after the first, with m_eff/H_E >= 10 on
# the zero-lag tracker) fire on a full history at alpha = 0.1? Prints, per history, the smallest
# rho_kin/rho_R,E over the candidates after the first, the candidates below each EPS, and m/H at
# phi_min there. Usage: probe_large_M.py BETA M
import sys
from math import exp, log

from ComputeTargets.tests import test_injection as ti
import importlib

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from Quadrature.supervisors.ScalarField import StateVector

beta, M = float(sys.argv[1]), float(sys.argv[2])
try:
    data, r, _ = ti.integrate_captured(beta, M, 0.1)
except RuntimeError as e:
    print(f"beta={beta} M={M:g}: {e}")
    sys.exit(0)
inj = r.injections
policy = None
# rebuild the realized model and a policy with the final ledger
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

cos = gth.make_cosmology()
u = cos.units
model = ExponentialModel(0, M_value(0, M * u.PlanckMass), 1, beta_value(0, beta), u)
seed = model.seed(cos)
rm = model.realize(seed.normalization, seed.phi_ref)
policy = SM.ODEPolicy("probe", cos, rm, dark_radiation=data["dark_radiation"])
rows = []
for i, x in enumerate(inj):
    if i == 0:
        continue
    y = r.solution(x.N)
    ratio = exp(x.log_rho_kin - y[2])
    rows.append((ratio, i, x, y))
rows.sort(key=lambda t: t[0])
print(f"beta={beta} M={M:g}: {len(inj)} candidates, N_final={r.N_final:.4f}; smallest rho_kin/rho_R after the first: "
      + ", ".join(f"#{i} {ratio:.2e}" for ratio, i, x, y in rows[:3]))
for eps in (1e-4, 1e-6, 1e-8):
    below = [t for t in rows if t[0] < eps]
    first = min(below, key=lambda t: t[1]) if below else None
    if first is None:
        print(f"   eps={eps:g}: never")
        continue
    ratio, i, x, y = first
    s = StateVector._make(y)
    Sigma = 1.0 - 3.0 * cos.w(exp(s.log_T_Jordan))
    lnT = s.log_rhorad_Einstein + log(Sigma + exp(s.log_fm))
    try:
        pm = rm.minimum_on_trace(float(y[0]), lnT)
        lm2 = SM.log_m_eff_squared(rm, pm, s.log_rhorad_Einstein, s.log_fm, Sigma)
        st = StateVector._make((pm, 0.0, *y[2:]))
        H = SM.HubblePolicy(rm, u)(policy(x.N, st), st).H_Einstein
        mH = exp(0.5 * lm2) / H
    except Exception as e:
        pm, mH = float("nan"), f"error {e}"
    print(f"   eps={eps:g}: first #{i} ({x.kind}) N={x.N:.4f} T_J={exp(s.log_T_Jordan) / u.MeV:.4g} MeV "
          f"ratio {ratio:.2e}, phi={y[0] / u.PlanckMass:.4g}, phi_min={pm / u.PlanckMass:.4g} Mp, m/H at phi_min {mH}")
