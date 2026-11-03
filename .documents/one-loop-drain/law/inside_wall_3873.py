"""
one-loop-drain prompt 01 §1.7 (`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`): 3873 of the
2026.8.0 store (M = 1e-12 M_P, beta = 1.11, stored alpha = 0.03), rerun at its stored normalization, untuned,
built as the drain probe builds it, in this process, with the step loop's module-level calls observed:
assess_infall, unresolved_turnaround and _reflection_guards are wrapped (the loop calls each by its module
name), and so is unresolved_reflection_refused where it exists (the user's ruling (b)); nothing else is
touched, so the run is bitwise an unobserved one.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/inside_wall_3873.py LAW A OUT.json

LAW is "one_loop" (A the law's strength) or "fixed" (A the fraction alpha). For every call of the guards (each
reflection asked for) it records N, phi, pi, W/(pi^2/2), H_E, rho_kin,E = H_E^2 pi^2/2, W = V - V_floor, the
floor rule's f phi/|pi| against h_floor, and which branch asked: "unresolved" if unresolved_turnaround
returned True at this step start, else "floor"; and the assessment the branch was judged on (the step start's,
or the infall's latest), with its ln E, phi_t and ln R, and E now (rho_kin + W at the guard's state) against
the assessed E; and the infall's decisions since the last reflection (phi/phi_t, phi_t against
UNRESOLVED_PHI_T_THRESHOLD_MP, ln R, fired). It writes them, and the run's outcome, as JSON.
"""

import glob
import importlib
import json
import sqlite3
import sys
import time
from math import exp, expm1, log

sys.path.insert(0, ".")

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from tools.history_and_bbn import (
    _z_grid,
    T_INIT_GEV,
    PHI_INIT_MP,
    PI_INIT,
)  # noqa: E402
from CosmologyConcepts import M_value, beta_value, temperature  # noqa: E402
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Quadrature.supervisors.ScalarField import StateVector  # noqa: E402
from Units import Planck_units  # noqa: E402

SERIAL = 3873
STORE_GLOB = (
    "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db"
)


def store_row():
    for f in sorted(glob.glob(STORE_GLOB)):
        c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        r = c.execute(
            """select m.value_eV, b.value, a.value, h.normalization, h.phi_ref
               from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
               join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
               join alpha_value a on h.alpha_serial=a.serial where h.serial=?""",
            (SERIAL,),
        ).fetchone()
        c.close()
        if r is not None:
            return r
    raise SystemExit("not found")


def main():
    law, A, out_path = sys.argv[1], float(sys.argv[2]), sys.argv[3]
    M_eV, beta, alpha_stored, norm, phi_ref_Mp = store_row()
    units = Planck_units()
    Mp = units.PlanckMass
    cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0, M_value(0, M_eV * units.eV), 1, beta_value(0, beta), units
    )
    realized = model.realize(norm, phi_ref_Mp * Mp)
    log_V_floor = realized.log_V_floor
    hubble = SM.HubblePolicy(realized, units)

    assessments = []  # (phi, ln rho_kin, ln E, turning point or None), every call
    decisions = []  # (phi, phi_t, ln R, fired)
    guards = []
    since = [0]  # the index in decisions at which the infall in progress began

    real_assess = SM.assess_infall
    real_decide = SM.unresolved_turnaround
    real_guards = SM._reflection_guards
    real_refused = getattr(SM, "unresolved_reflection_refused", None)
    declined_at = (
        []
    )  # phi of the step starts at which the user's ruling (b) declined the decision

    def refused(policy, N, y):
        value = real_refused(policy, N, y)
        if value:
            declined_at.append(float(y[0]))
        return value

    def assess(model_, phi, log_rho_kin):
        log_E, tp = real_assess(model_, phi, log_rho_kin)
        assessments.append((float(phi), float(log_rho_kin), log_E, tp))
        return log_E, tp

    def decide(phi, tp, PlanckMass):
        fired = real_decide(phi, tp, PlanckMass)
        decisions.append((float(phi), tp.phi_t, tp.log_R, fired))
        return fired

    def guard(policy, N, y, task_label):
        state = StateVector._make(y)
        data = policy(N, state)
        H = hubble(data, state).H_Einstein
        log_V = realized.log_V(state.phi_Einstein)
        W_over = -3.0 * data.V_over_3H2Mp2 * expm1(log_V_floor - data.log_V)
        kinetic = 0.5 * state.pi_Einstein * state.pi_Einstein / (Mp * Mp)
        log_rho_kin = 2.0 * log(H) + 2.0 * log(abs(state.pi_Einstein)) - SM.LOG_2
        log_W = (
            log_V + log(-expm1(log_V_floor - log_V))
            if log_V > log_V_floor
            else float("-inf")
        )
        # the branch: unresolved iff the decision at this step start (the last made at this phi) fired
        last = decisions[-1] if decisions else None
        fired_here = (
            last is not None
            and last[0] == float(state.phi_Einstein)
            and last[3]
            and not (declined_at and declined_at[-1] == float(state.phi_Einstein))
        )
        # the assessment judged on: the infall's latest with a turning point
        latest = next((a for a in reversed(assessments) if a[3] is not None), None)
        here = (
            assessments[-1]
            if assessments and assessments[-1][0] == float(state.phi_Einstein)
            else None
        )
        # the infall's decisions, from the last reflection to this one: phi/phi_t, phi_t against the
        # threshold, ln R and whether it fired
        infall = [
            {
                "phi_over_phi_t": phi_ / phi_t_,
                "phi_t_over_threshold": phi_t_
                / (SM.UNRESOLVED_PHI_T_THRESHOLD_MP * Mp),
                "log_R": log_R_,
                "fired": fired_,
            }
            for phi_, phi_t_, log_R_, fired_ in decisions[since[0] :]
        ]
        since[0] = len(decisions)
        rec = {
            "N": N,
            "infall_decisions": infall,
            "phi_Mp": state.phi_Einstein / Mp,
            "pi_Mp": state.pi_Einstein / Mp,
            "ratio": W_over / kinetic,
            "H_E": H,
            "log_rho_kin": log_rho_kin,
            "log_W": log_W,
            "floor_rule": 0.1 * state.phi_Einstein / abs(state.pi_Einstein),
            "branch": "unresolved" if fired_here else "floor",
            "step_start_assessed": here is not None,
            "step_start_turning_point": (
                None if here is None or here[3] is None else here[3].phi_t / Mp
            ),
        }
        if latest is not None:
            phi_a, log_rho_kin_a, log_E_a, tp = latest
            log_E_now = max(log_rho_kin, log_W) + log(
                1.0 + exp(min(log_rho_kin, log_W) - max(log_rho_kin, log_W))
            )
            rec.update(
                {
                    "assessed_phi_Mp": phi_a / Mp,
                    "assessed_log_E": log_E_a,
                    "assessed_log_rho_kin": log_rho_kin_a,
                    "phi_t_Mp": tp.phi_t / Mp,
                    "log_R": tp.log_R,
                    "phi_over_phi_t": state.phi_Einstein / tp.phi_t,
                    "log_E_now": log_E_now,
                    "log_E_now_minus_assessed": log_E_now - log_E_a,
                    "log_W_minus_assessed_log_E": log_W - log_E_a,
                }
            )
        try:
            ratio = real_guards(policy, N, y, task_label)
            rec["passed"] = True
            return ratio
        except Exception as e:
            rec["passed"] = False
            rec["message"] = getattr(e, "message", str(e))
            raise
        finally:
            guards.append(rec)

    SM.assess_infall, SM.unresolved_turnaround, SM._reflection_guards = (
        assess,
        decide,
        guard,
    )
    if real_refused is not None:
        SM.unresolved_reflection_refused = refused
    t0 = time.time()
    try:
        data = SM.integrate_history(
            cosmology,
            temperature(0, T_INIT_GEV * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, PHI_INIT_MP * Mp),
            pi_value(0, PI_INIT),
            _z_grid(),
            realized,
            task_label=f"inside-wall-{SERIAL}-{law}-{A:g}",
            alpha=A,
            drain_law=law,
        )
    finally:
        SM.assess_infall, SM.unresolved_turnaround, SM._reflection_guards = (
            real_assess,
            real_decide,
            real_guards,
        )
        if real_refused is not None:
            SM.unresolved_reflection_refused = real_refused
    wall = time.time() - t0

    out = {
        "serial": SERIAL,
        "law": law,
        "A": A,
        "alpha_stored": alpha_stored,
        "normalization": norm,
        "wall_s": wall,
        "failure": bool(data.get("failure", False)),
        "failure_reason": data.get("failure_reason"),
        "drain": (
            None
            if data.get("drain") is None
            else {
                "law": data["drain"].law,
                "unresolvable_assessments": data["drain"].unresolvable_assessments,
                "unresolved_declined": getattr(
                    data["drain"], "unresolved_declined", None
                ),
                "regimes": (
                    None
                    if data["drain"].candidates is None
                    else {
                        r: sum(1 for c in data["drain"].candidates if c.regime == r)
                        for r in SM.DRAIN_REGIMES
                    }
                ),
            }
        ),
        "reflections_asked": len(guards),
        "declined_step_starts": len(declined_at),
        "branches": {
            b: sum(1 for g in guards if g["branch"] == b)
            for b in ("unresolved", "floor")
        },
        "max_ratio": max((g["ratio"] for g in guards), default=None),
        "guards": guards,
    }
    if not out["failure"]:
        out["parking_N"] = None if data["parking"] is None else data["parking"].N
        out["reflections"] = data["reflections"]
    with open(out_path, "w") as fh:
        json.dump(out, fh, indent=1, default=str)
    print(
        "done",
        json.dumps(
            {
                k: out[k]
                for k in (
                    "law",
                    "A",
                    "wall_s",
                    "failure",
                    "reflections_asked",
                    "branches",
                    "max_ratio",
                )
            }
        ),
    )


if __name__ == "__main__":
    main()
