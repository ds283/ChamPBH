# Probe: scan the threshold PHI_NUM below which a classical turnaround is treated as unresolved.
# r_criterion.py's mode "RN", with two changes:
#   - the classical turning point and the test-field ratio are computed through the RealizedModel's
#     interface only (log_V, log_V_floor, d_logV_dphi, d2_logV_dphi2), with no closed form of any
#     potential: phi_t is the root of V(phi_t) - V_floor = E, E = H_E^2 pi^2/2, searched in ln phi
#     inwards from the field, and R = omega_t^4/(8 pi^2 E) with omega_t^2 = V''(phi_t) =
#     V [(ln V)'' + (ln V)'^2]. It needs reflects_at_origin (the wall lies between the origin and phi);
#   - PHI_NUM and the weight of a resolved drain are arguments. GATE "gate" keeps prompt 03's gate on
#     resolved turnarounds; "w1" drains every candidate with w_gate = 1, so that a comparison across
#     PHI_NUM sees only where the reflection is made, not a change of weight.
# An infalling field is reflected (the floor branch) as soon as phi < K phi_t, K = 10, if R > 1 or
# phi_t < PHI_NUM; that reflection drains with w_gate = 1. PHI_NUM = 0 leaves the R rule alone. The old
# floor rule is unchanged. Every run stops at the parking cut, rho_kin/rho_R,E < EPS at a floor
# reflection (pi_in) or a drain point (the KE maximum). test_injection's setup otherwise.
# Usage: phi_t_scan.py BETA M_OVER_MP ALPHA PHI_NUM GATE BUDGET OUT.json [K]   (K defaults to 10)
#        phi_t_scan.py check   (the generic phi_t and R against the exponential model's closed form)
import contextlib
import inspect
import io
import json
import sys
import time
from math import exp, expm1, log, pi as PI
from unittest import mock

from scipy.optimize import brentq

from ComputeTargets.tests import test_injection as T
from CosmologyConcepts import M_value, beta_value, phi_value, pi_value, temperature
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

SM = T.SM
EPS, K = 1e-6, 10.0
LN_8PI2 = log(8.0 * PI * PI)


def classical_turning_point(rm, phi, lnE):
    """
    (phi_t, ln R) for a field at phi falling in with kinetic energy e^lnE, from the model's interface
    alone; None if V - V_floor >= E at phi already (the field is at or inside its turning point).
    """
    lnVf = rm.log_V_floor

    def lnW(x):  # ln(V - V_floor) at phi = e^x
        lv = rm.log_V(exp(x))
        if lv <= lnVf:
            return float("-inf")
        return lv + log(-expm1(lnVf - lv))

    hi = log(phi)
    if lnW(hi) >= lnE:
        return None
    step, lo = 1.0, hi - 1.0
    while lnW(lo) < lnE:
        hi, step = lo, 2.0 * step
        lo = hi - step
    x_t = brentq(lambda x: lnW(x) - lnE, lo, hi, xtol=1e-10, rtol=1e-12)
    phi_t = exp(x_t)
    d1 = rm.d_logV_dphi(phi_t)
    q = rm.d2_logV_dphi2(phi_t) + d1 * d1
    ln_omega2 = rm.log_V(phi_t) + log(abs(q))
    return phi_t, 2.0 * ln_omega2 - LN_8PI2 - lnE


if sys.argv[1] == "check":
    cos = T.gth.make_cosmology()
    u = cos.units
    for M_Mp in (1e-30, 1e-16, 1e-10, 1e-7):
        model = ExponentialModel(0, M_value(0, M_Mp), 1, beta_value(0, 2.0), u)
        s = model.seed(cos)
        rm = model.realize(s.normalization, s.phi_ref)
        for lnE in (-180.0, -120.0, -60.0):
            y_t = log(1.0 + exp(lnE - rm.normalization))
            lnR_closed = 4.0 * (2.0 * log(y_t) - log(M_Mp)) + lnE - LN_8PI2
            phi_t, lnR = classical_turning_point(rm, 0.1, lnE)
            print(f"M={M_Mp:g} lnE={lnE}: phi_t/(M/y_t) - 1 = {phi_t / (M_Mp / y_t) - 1.0:+.2e}, "
                  f"log10 R generic {lnR / log(10):+.3f} closed (lambda = y^2/M) {lnR_closed / log(10):+.3f}")
    sys.exit(0)

beta, M_Mp, alpha, PHI_NUM, GATE, budget, out = (
    float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), sys.argv[5],
    int(sys.argv[6]), sys.argv[7])
if len(sys.argv) > 8:
    K = float(sys.argv[8])
assert GATE in ("gate", "w1")

cos = T.gth.make_cosmology()
u = cos.units
Mp = u.PlanckMass
model = ExponentialModel(0, M_value(0, M_Mp * Mp), 1, beta_value(0, beta), u)
seed = model.seed(cos)
rm = model.realize(seed.normalization, seed.phi_ref)
assert rm.reflects_at_origin

rec = {"beta": beta, "M_over_Mp": M_Mp, "alpha": alpha, "phi_num": PHI_NUM, "gate": GATE,
       "budget": budget, "eps": EPS, "K": K, "inj": [], "tp": [], "decisions": [], "parked": None}
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
    unresolved_flag[0] = False
    phi, pi_ = float(y[0]), float(y[1])
    H = H_E(N, y, policy, hubble)
    lnE = 2.0 * log(H) + 2.0 * log(-pi_) - log(2.0)
    found = classical_turning_point(rm, phi, lnE)
    if found is None:
        return False
    phi_t, lnR = found
    if phi >= K * phi_t:
        return False
    hit = lnR > 0.0 or phi_t < PHI_NUM
    rec["decisions"].append([N, lnR / log(10.0), phi / phi_t, hit, steps[0], phi_t])
    unresolved_flag[0] = hit
    return hit


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


def A_drain(A_k):
    # GATE "w1": a resolved drain point drains with w_gate = 1 as well
    return 1e300 if GATE == "w1" else A_k


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
    ("                    armed.nonadiabaticity,\n",
     "                    _A_DRAIN(armed.nonadiabaticity),\n"),
]
for old, new in edits:
    assert src.count(old) == 1, old
    src = src.replace(old, new)
ns = dict(vars(SM))
ns.update(_RESET=reset, _UNRESOLVED=unresolved, _PARK=park, _A_FLOOR=A_floor, _A_DRAIN=A_drain,
          Radau=CountingRadau, Injection=inj, TurningPoint=tp)
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
                                    pi_value(0, 0.0), T.gth.make_z_grid(), rm, task_label="phi_t scan",
                                    alpha=alpha)
    rec["failure"] = data.get("failure_reason") if data.get("failure", False) else None
except Parked:
    rec["failure"] = None
rec["wall_time_s"] = time.perf_counter() - t0
rec["steps"] = steps[0]
json.dump(rec, open(out, "w"))
p = rec["parked"]
nd = sum(1 for d in rec["decisions"] if d[3])
print(f"beta={beta} M={M_Mp:g} alpha={alpha} PHI_NUM={PHI_NUM:g} {GATE} K={K:g}: {rec['wall_time_s']:.0f}s "
      f"steps={steps[0]} inj={len(rec['inj'])} unresolved={nd} "
      + (f"PARKED N={p['N']:.3f} T_J={exp(p['ln_TJ_MeV']):.4g} MeV ({p['kind']})" if p else
         f"not parked; failure={rec['failure']}"))
