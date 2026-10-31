# Planning probe for prompt 09 (inelastic-reflection, 2026-10-31): r = rho_chi/rho_R and two Delta N_eff conventions
# at T_J = 1 MeV, 70 keV and today. Usage: neff.py BETA M (alpha = 0.1). Read-only: no store, no Ray.
import contextlib, io, sys, importlib
from math import exp, log
from unittest import mock
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
beta, M = float(sys.argv[1]), float(sys.argv[2])
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos); rm = model.realize(seed.normalization, seed.phi_ref)
with contextlib.redirect_stdout(io.StringIO()):
    data = SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV), temperature(1, cos.T_CMB_Kelvin * u.Kelvin),
        phi_value(0, 5.0 * Mp), pi_value(0, 0.0), gth.make_z_grid(), rm, task_label="neff", alpha=0.1)
led = data["dark_radiation"]; S = [SM.SampleValues._make(tuple(s)) for s in data["sample"]]
def at(T_MeV):
    lT = log(T_MeV * u.MeV)
    s = next(s for s in S if s.log_T_Jordan <= lT)  # first sample at or below T
    r = led.K(s.raw_N) * exp(-4.0 * s.raw_N - s.log_rhorad_Einstein)
    return s, r
c = (4.0 / 7.0); f = (11.0 / 4.0) ** (4.0 / 3.0)
print(f"b={beta} M={M} last injection N={led.injections[-1][0]:.4f} K_final={led.K(1e9):.4e}")
for T in (1.0, 0.07, None):
    s, r = (S[-1], led.K(S[-1].raw_N) * exp(-4.0 * S[-1].raw_N - S[-1].log_rhorad_Einstein)) if T is None else at(T)
    g = s.gstar_rho
    print(f"  T_J={exp(s.log_T_Jordan)/u.MeV:.4g} MeV N={s.raw_N:.4f} r={r:.5e} g_rho={g:.4f} (4/7)g r={c*g*r:.5f} (4/7)g(11/4)^(4/3) r={c*g*f*r:.5f}")
