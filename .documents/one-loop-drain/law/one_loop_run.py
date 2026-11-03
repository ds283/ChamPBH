"""
one-loop-drain prompt 01 §2.2 (orchestrator note 01, ruling 7): one history of the 2026.8.0 store, rerun under
the one-loop law at its stored normalization (no tuning, no Ray, no store written), in this process, built as
the drain probe builds it (prompts/one-loop-drain/source/probe/drain/drain_probe.py): the same _z_grid(),
T_INIT_GEV, PHI_INIT_MP, PI_INIT, QCD_Cosmology on Planck2018 in Planck_units, and
ExponentialModel(0, M_value(0, M_eV eV), 1, beta_value(0, beta), units).realize(normalization, phi_ref), from
the store's row (read-only). It passes alpha=A, drain_law="one_loop" and sets no environment variable.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/one_loop_run.py SERIAL A GAP GATE OUT.json

GAP is "zero" or "edge", set as ScalarHistory._GAP_PRESCRIPTION in this process. GATE is "probe" (gate_weight
replaced by 1.0 in this process, which is the probe's law: ruling 7) or "production" (the gate kept, U9).

It writes one JSON object: the run's summary; each candidate's (N, alpha_eff, delta_K), its kind, resolved,
w_gate, nonadiabaticity and the "drain" record's regime, ell, log_E and log_H; the "drain" record's counts;
the by-regime summary computed as the probe computed it; and, for each candidate, whether the loop's
PeakEstimate equals a fresh peak_estimate and whether its regime agrees with peak_estimate's (§2.2.5,
ruling 4). Floats are written by json (repr), so they read back exactly.
"""

import glob
import importlib
import json
import sqlite3
import sys
import time
from collections import defaultdict

sys.path.insert(0, ".")

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.ScalarHistory import integrate_history  # noqa: E402
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
from Units import Planck_units  # noqa: E402

STORE_GLOB = (
    "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db"
)

ESTIMATE_REGIME = {
    SM.ESTIMATE_ONE_LOOP: SM.REGIME_ONE_LOOP,
    SM.ESTIMATE_CLASSICAL_GAP: SM.REGIME_GAP,
    SM.ESTIMATE_CLASSICAL: SM.REGIME_CLASSICAL,
}


def store_row(serial):
    for f in sorted(glob.glob(STORE_GLOB)):
        c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        r = c.execute(
            """select m.value_eV, b.value, a.value, h.normalization, h.phi_ref, h.rho_chi_ratio_today,
                      h.Delta_N_eff_today, h.parking_N, h.extra_data
               from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
               join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
               join alpha_value a on h.alpha_serial=a.serial where h.serial=?""",
            (serial,),
        ).fetchone()
        c.close()
        if r is not None:
            return f, r
    raise SystemExit(f"serial {serial} not found")


def main():
    serial, A, gap, gate, out_path = (
        int(sys.argv[1]),
        float(sys.argv[2]),
        sys.argv[3],
        sys.argv[4],
        sys.argv[5],
    )
    shard, (
        M_eV,
        beta,
        alpha_stored,
        norm,
        phi_ref_Mp,
        rchi_stored,
        dneff_stored,
        parkN_stored,
        extra,
    ) = store_row(serial)

    SM._GAP_PRESCRIPTION = gap
    if gate == "probe":
        SM.gate_weight = lambda nonadiabaticity: 1.0
    elif gate != "production":
        raise SystemExit(f"GATE must be probe or production, not {gate!r}")

    units = Planck_units()
    cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0, M_value(0, M_eV * units.eV), 1, beta_value(0, beta), units
    )
    realized = model.realize(norm, phi_ref_Mp * units.PlanckMass)

    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        try:
            result = integrate(*args, **kwargs)
        except Exception as e:
            captured.append(("raised", getattr(e, "drain", None)))
            raise
        captured.append(("result", result))
        return result

    SM.integrate_scalar_history = capturing
    label = f"A{sys.argv[2]}"
    t0 = time.time()
    try:
        data = integrate_history(
            cosmology,
            temperature(0, T_INIT_GEV * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, PHI_INIT_MP * units.PlanckMass),
            pi_value(0, PI_INIT),
            _z_grid(),
            realized,
            task_label=f"drain-probe-{serial}-{label}",
            alpha=A,
            drain_law="one_loop",
        )
    finally:
        SM.integrate_scalar_history = integrate
    wall = time.time() - t0

    drain = data.get("drain")
    out = {
        "serial": serial,
        "shard": shard,
        "M_eV": M_eV,
        "beta": beta,
        "alpha_stored": alpha_stored,
        "A": A,
        "gap": gap,
        "gate": gate,
        "wall_s": wall,
        "stored": {
            "rho_chi_ratio_today": rchi_stored,
            "Delta_N_eff_today": dneff_stored,
            "parking_N": parkN_stored,
            "extra_data": extra,
        },
        "failure": bool(data.get("failure", False)),
        "failure_reason": data.get("failure_reason"),
        "warnings": list(data.get("warnings", ())),
        "drain": (
            None
            if drain is None
            else {
                "law": drain.law,
                "A": drain.A,
                "gap_prescription": drain.gap_prescription,
                "unresolvable_assessments": drain.unresolvable_assessments,
                "unresolved_declined": getattr(drain, "unresolved_declined", None),
                "n_candidates": (
                    None if drain.candidates is None else len(drain.candidates)
                ),
                "regime_counts": (
                    None
                    if drain.candidates is None
                    else {
                        r: sum(1 for c in drain.candidates if c.regime == r)
                        for r in SM.DRAIN_REGIMES
                    }
                ),
                "candidates": (
                    None
                    if drain.candidates is None
                    else [list(c) for c in drain.candidates]
                ),
            }
        ),
    }
    if not out["failure"]:
        result = captured[-1][1]
        inj = data["injections"]
        dv = data["dark_radiation_values"]
        out["dark_radiation_values"] = dict(dv._asdict())
        out["candidates"] = len(inj)
        out["drains"] = sum(1 for i in inj if i.delta_K > 0.0)
        out["reflections"] = data["reflections"]
        out["accepted_steps"] = data["accepted_steps"]
        out["parking_N"] = None if data["parking"] is None else data["parking"].N
        out["release_N"] = None if data["release"] is None else data["release"].N
        out["K_injections"] = sum(i.delta_K for i in inj)
        out["N_final"] = float(data["sample"][-1].raw_N)
        # by regime, exactly as drain_probe.py computes it
        by = defaultdict(
            lambda: {
                "n": 0,
                "K": 0.0,
                "alpha_min": None,
                "alpha_max": None,
                "ell_min": None,
                "ell_max": None,
                "N_min": None,
                "N_max": None,
            }
        )
        K = out["K_injections"]
        for c, i in zip(drain.candidates, inj):
            b = by[c.regime]
            b["n"] += 1
            b["K"] += i.delta_K
            for key, v in (("alpha", c.alpha_eff), ("ell", c.ell), ("N", i.N)):
                if v is None:
                    continue
                b[key + "_min"] = (
                    v if b[key + "_min"] is None else min(b[key + "_min"], v)
                )
                b[key + "_max"] = (
                    v if b[key + "_max"] is None else max(b[key + "_max"], v)
                )
        for b in by.values():
            b["K_share"] = b["K"] / K if K > 0 else None
        out["by_regime"] = dict(by)
        rows = []
        for k, (c, i) in enumerate(zip(drain.candidates, inj)):
            fresh = SM.peak_estimate(realized, i)
            reused = result.peak_estimates[k]
            expected = (
                SM.REGIME_UNASSESSED
                if fresh is None
                else ESTIMATE_REGIME[fresh.estimate]
            )
            rows.append(
                {
                    "N": i.N,
                    "alpha_eff": i.alpha_eff,
                    "delta_K": i.delta_K,
                    "kind": i.kind,
                    "resolved": i.resolved,
                    "w_gate": i.w_gate,
                    "nonadiabaticity": i.nonadiabaticity,
                    "regime": c.regime,
                    "ell": c.ell,
                    "log_E": c.log_E,
                    "log_H": c.log_H,
                    "log_R": (
                        None
                        if i.classical_turning_point is None
                        else i.classical_turning_point.log_R
                    ),
                    "reuse_equals_fresh": reused == fresh,
                    "regime_agrees": c.regime == expected,
                    "ell_nonpositive": (
                        fresh is not None
                        and fresh.estimate == SM.ESTIMATE_ONE_LOOP
                        and c.regime == SM.REGIME_GAP
                    ),
                }
            )
        out["cands"] = rows
    with open(out_path, "w") as fh:
        json.dump(out, fh, indent=1, default=str)
    print(
        "done",
        json.dumps(
            {
                k: out.get(k)
                for k in (
                    "serial",
                    "A",
                    "gap",
                    "gate",
                    "wall_s",
                    "failure",
                    "candidates",
                )
            }
        ),
    )


if __name__ == "__main__":
    main()
