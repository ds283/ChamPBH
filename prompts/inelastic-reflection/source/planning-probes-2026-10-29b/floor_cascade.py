# Probe for handoff 2026-10-29 §5.1 (code at 1e31aac): a history at small M with the drain on, under the
# floor rule. Read-only: no store, no Ray. test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0,
# T* = 2e4 GeV, to T_CMB, untuned seed realization). The Injection, Reflection and TurningPoint
# constructors are wrapped so that the records survive a failed history.
# Usage: floor_cascade.py BETA M_OVER_MP ALPHA BUDGET OUT.json
import contextlib
import io
import json
import sys
import time
from math import exp, log
from unittest import mock

from ComputeTargets.tests import test_injection as T
from CosmologyConcepts import M_value, beta_value, phi_value, pi_value, temperature
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = T.SM
beta, M_Mp, alpha, budget, out = (float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]),
                                  int(sys.argv[4]), sys.argv[5])

cos = T.gth.make_cosmology()
u = cos.units
Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M_Mp * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos)
rm = model.realize(seed.normalization, seed.phi_ref)

rec = {"beta": beta, "M_over_Mp": M_Mp, "alpha": alpha, "budget": budget, "seed_kind": seed.kind,
       "inj": [], "refl": [], "tp": []}
_Inj, _Ref, _TP, _SC = SM.Injection, SM.Reflection, SM.TurningPoint, SM.StepControl
steps = [0]


class CountingRadau(SM.Radau):
    def step(self):
        msg = super().step()
        if self.status != "failed":
            steps[0] += 1
        return msg


def inj(**kw):
    rec["inj"].append([kw["N"], kw["kind"], kw["pi_before"], kw["alpha_eff"], kw["w_gate"],
                       kw["nonadiabaticity"], kw["delta_rho_over_rho_rad"], kw["phi_turn"], steps[0]])
    return _Inj(**kw)


def ref(**kw):
    rec["refl"].append([kw["N"], kw["phi_Einstein"], kw["pi_Einstein_in"]])
    return _Ref(**kw)


def tp(**kw):
    rec["tp"].append([kw["N"], kw["kind"], kw["phi_Einstein"], kw["log_T_Jordan"] - log(u.MeV),
                      kw["nonadiabaticity"], kw["m_eff_over_H"]])
    return _TP(**kw)


t0 = time.perf_counter()
with mock.patch.object(SM, "Injection", inj), mock.patch.object(SM, "Reflection", ref), \
        mock.patch.object(SM, "TurningPoint", tp), mock.patch.object(SM, "Radau", CountingRadau), \
        mock.patch.object(SM, "StepControl", lambda **kw: _SC(step_budget=budget, **kw)), \
        contextlib.redirect_stdout(io.StringIO()):
    data = SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV),
                                temperature(1, cos.T_CMB_Kelvin * u.Kelvin), phi_value(0, 5.0 * Mp),
                                pi_value(0, 0.0), T.gth.make_z_grid(), rm, task_label="floor probe",
                                alpha=alpha)
rec["wall_time_s"] = time.perf_counter() - t0
rec["failure"] = data.get("failure_reason") if data.get("failure", False) else None
rec["Mp"] = Mp
rec["steps"] = steps[0]
json.dump(rec, open(out, "w"))
print(f"beta={beta} M={M_Mp:g} alpha={alpha}: {rec['wall_time_s']:.1f}s, refl={len(rec['refl'])}, "
      f"steps={steps[0]}, inj={len(rec['inj'])}, tps={len(rec['tp'])}, failure={rec['failure']}")
