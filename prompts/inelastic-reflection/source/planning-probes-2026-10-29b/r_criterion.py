# Probe: dissipation mode with the physical resolved/unresolved criterion, to the parking cut.
# A copy of integrate_scalar_history, edited by string replacement and exec'd in a copy of the
# module's namespace (nothing in the tree changes), with test_injection's setup (n = 1, phi* = 5 M_P,
# pi* = 0, T* = 2e4 GeV, untuned seed realization).
#   mode "R":   an infalling field is reflected (the floor branch, so the drain of prompt 02) when the
#               classical turnaround ahead of it is strongly coupled, R = lambda^4 E/(8 pi^2) > 1 with
#               E = H^2 pi^2/2, y_t = ln(1 + E/V_0), lambda = y_t^2/M, phi_t = M/y_t, as soon as
#               phi < K phi_t (K = 10). An unresolved reflection drains with w_gate = 1. The old floor
#               rule stays as it is. Resolved turnarounds keep prompt 03's gate.
#   mode "RN":  as R, and also unresolved when phi_t < PHI_NUM (a classical wall too thin for Radau),
#               reflected the same way at K phi_t with w_gate = 1.
#   mode "old": today's rules.
# Both modes stop at the parking cut: rho_kin/rho_R,E < EPS at a floor reflection (pi_in) or a drain
# point (the KE maximum), i.e. after the first impact.
# Usage: r_criterion.py MODE BETA M_OVER_MP ALPHA BUDGET OUT.json
import contextlib
import inspect
import io
import json
import sys
import time
from unittest import mock
from math import exp, log, pi as PI

from ComputeTargets.tests import test_injection as T
from CosmologyConcepts import M_value, beta_value, phi_value, pi_value, temperature
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

SM = T.SM
mode, beta, M_Mp, alpha, budget, out = (sys.argv[1], float(sys.argv[2]), float(sys.argv[3]),
                                        float(sys.argv[4]), int(sys.argv[5]), sys.argv[6])
EPS, K, PHI_NUM = 1e-6, 10.0, 1e-10

cos = T.gth.make_cosmology()
u = cos.units
Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M_Mp * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos)
rm = model.realize(seed.normalization, seed.phi_ref)
lnV0 = rm.normalization

rec = {"mode": mode, "beta": beta, "M_over_Mp": M_Mp, "alpha": alpha, "budget": budget, "eps": EPS,
       "K": K, "inj": [], "tp": [], "decisions": [], "parked": None}
steps = [0]
unresolved_flag = [False]


class Parked(Exception):
    pass


class CountingRadau(SM.Radau):
    def step(self):
        msg = super().step()
        if self.status != "failed":
            steps[0] += 1
        return msg


def H_E(N, y, policy, hubble):
    s = StateVector._make(y)
    return hubble(policy(N, s), s).H_Einstein


def unresolved(N, y, policy, hubble):
    """mode R: is the classical turnaround ahead strongly coupled, and is the field at K phi_t?"""
    unresolved_flag[0] = False
    if mode not in ("R", "RN"):
        return False
    phi, pi_ = float(y[0]), float(y[1])
    H = H_E(N, y, policy, hubble)
    lnE = 2.0 * log(H) + 2.0 * log(-pi_) - log(2.0)
    y_t = log1pexp(lnE - lnV0)
    phi_t = M_Mp / y_t
    if phi >= K * phi_t:
        return False
    lnR = 4.0 * (2.0 * log(y_t) - log(M_Mp)) + lnE - log(8.0 * PI * PI)
    hit = lnR > 0.0 or (mode == "RN" and phi_t < PHI_NUM)
    rec["decisions"].append([N, lnR / log(10.0), phi / phi_t, hit, steps[0], phi_t])
    unresolved_flag[0] = hit
    return hit


def log1pexp(x):
    return x if x > 50.0 else log(1.0 + exp(x))


def park(N, y, policy, hubble, kind):
    H = H_E(N, y, policy, hubble)
    r = 0.5 * H * H * float(y[1]) ** 2 / exp(float(y[2]))
    if r < EPS:
        rec["parked"] = {"N": N, "kind": kind, "rho_kin_over_rho_R": r, "pi": float(y[1]),
                         "phi": float(y[0]), "ln_TJ_MeV": float(y[4]) - log(u.MeV), "steps": steps[0]}
        raise Parked()


def reset():
    unresolved_flag[0] = False


def A_floor(A_k):
    # an unresolved reflection drains with w_gate = 1 (gate_weight(1e300) == 1.0)
    return 1e300 if unresolved_flag[0] else A_k


_Inj, _TP = SM.Injection, SM.TurningPoint


def inj(**kw):
    rec["inj"].append([kw["N"], kw["kind"], kw["pi_before"], kw["alpha_eff"], kw["w_gate"],
                       kw["nonadiabaticity"], kw["delta_rho_over_rho_rad"], kw["phi_turn"], steps[0]])
    return _Inj(**kw)


def tp(**kw):
    rec["tp"].append([kw["N"], kw["kind"], kw["phi_Einstein"], kw["log_T_Jordan"] - log(u.MeV),
                      kw["nonadiabaticity"], kw["m_eff_over_H"]])
    return _TP(**kw)


src = inspect.getsource(SM.integrate_scalar_history)
edits = [
    ("        if pi_ < 0.0 and f_cap * phi / (-pi_) < h_floor:\n",
     "        _RESET()\n"
     "        if pi_ < 0.0 and (f_cap * phi / (-pi_) < h_floor or _UNRESOLVED(N, solver.y, policy, hubble)):\n"
     "            _PARK(N, solver.y, policy, hubble, 'floor')\n"),
    ('                y_reflected = candidate(N, solver.y, A_k, "floor", N, phi, -1.0)\n',
     '                y_reflected = candidate(N, solver.y, _A_FLOOR(A_k), "floor", N, phi, -1.0)\n'),
    ("        if armed is not None and solver.y[1] > 0.0 and solver.f[1] <= 0.0:\n",
     "        if armed is not None and solver.y[1] > 0.0 and solver.f[1] <= 0.0:\n"
     "            _PARK(solver.t, solver.y, policy, hubble, 'drain')\n"),
]
for old, new in edits:
    assert src.count(old) == 1, old
    src = src.replace(old, new)
ns = dict(vars(SM))
ns.update(_RESET=reset, _UNRESOLVED=unresolved, _PARK=park, _A_FLOOR=A_floor, Radau=CountingRadau, Injection=inj,
          TurningPoint=tp)
exec(compile(src, "integrate_scalar_history(probe)", "exec"), ns)
_SC = SM.StepControl

t0 = time.perf_counter()
try:
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(SM, "integrate_scalar_history", ns["integrate_scalar_history"]))
        stack.enter_context(mock.patch.object(SM, "StepControl", lambda **kw: _SC(step_budget=budget, **kw)))
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        data = SM.integrate_history(cos, temperature(0, 2.0e4 * u.GeV),
                                    temperature(1, cos.T_CMB_Kelvin * u.Kelvin), phi_value(0, 5.0 * Mp),
                                    pi_value(0, 0.0), T.gth.make_z_grid(), rm, task_label="R probe",
                                    alpha=alpha)
    rec["failure"] = data.get("failure_reason") if data.get("failure", False) else None
except Parked:
    rec["failure"] = None
rec["wall_time_s"] = time.perf_counter() - t0
rec["steps"] = steps[0]
json.dump(rec, open(out, "w"))
p = rec["parked"]
nd = sum(1 for d in rec["decisions"] if d[3])
print(f"{mode} beta={beta} M={M_Mp:g} alpha={alpha}: {rec['wall_time_s']:.0f}s steps={steps[0]} "
      f"inj={len(rec['inj'])} R-reflections={nd} "
      + (f"PARKED N={p['N']:.3f} T_J={exp(p['ln_TJ_MeV']):.4g} MeV ({p['kind']})" if p else
         f"not parked; failure={rec['failure']}"))
