"""
one-loop-drain prompt 03, Store gate §2.1 (orchestrator note 03, Order 4): the fixed law's payloads, key by key.

Run from a tree's root with PYTHONPATH=. (this checkout, and a scratch worktree of the parent commit d3069cb):

    PYTHONPATH=. <venv>/bin/python <this file> OUT.json

It writes, for each of three fixed-law runs, {payload key: repr of its value}, with these normalized, since they
are not values of the integration: "metadata" (its four RHS times dropped: compute_steps and RHS_evaluations kept),
"z_grid" (its points' ln(1 + z) and store ids), "dark_radiation" (the ledger's repr, which holds its entries).
  - golden-alpha0:   integrate_history at the tuned golden history's stored normalization and phi_ref, alpha = 0
                     (ComputeTargets/tests/golden_tuned_history.py, data/golden_history_06_tuned.json);
  - reflecting-a005: integrate_history at the reflecting setup, beta = 1.5, M = 1e-12 M_P, alpha = 0.05, its seed
                     realization (test_injection.integrate_captured, as test_drain_law (3) runs it);
  - golden-tuned:    tune_history on the golden history (golden_tuned_history.run_tuning), alpha = 0.
compare_fixed_law.py compares two such files.
"""

import contextlib
import io
import json
import sys

from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_injection as ti
from CosmologyConcepts import temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value

SM = ti.SM


def normalized(data: dict) -> dict:
    out = {}
    for key, value in data.items():
        if key == "metadata":
            value = (value.compute_steps, value.RHS_evaluations)
        elif key == "z_grid":
            value = [(z.store_id, z.log_one_plus_z) for z in value]
        out[key] = repr(value)
    return out


def golden_alpha0() -> dict:
    golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
    norm = float.fromhex(golden["tuning"]["normalization"])
    phi_ref = float.fromhex(golden["tuning"]["phi_ref"])
    cosmology = gth.make_cosmology()
    units = cosmology.units
    model = gth.make_model(units).realize(norm, phi_ref)
    return SM.integrate_history(
        cosmology,
        temperature(0, gth.T_INIT_GEV * units.GeV),
        temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
        phi_value(0, gth.PHI_INIT_OVER_MP * units.PlanckMass),
        pi_value(0, gth.PI_INIT),
        gth.make_z_grid(),
        model,
        task_label="store-03-golden",
        atol=gth.ATOL,
        rtol=gth.RTOL,
    )


def main() -> int:
    out = {}
    with contextlib.redirect_stdout(io.StringIO()):
        out["golden-alpha0"] = normalized(golden_alpha0())
        out["reflecting-a005"] = normalized(
            ti.integrate_captured(1.5, 1.0e-12, 0.05)[0]
        )
        out["golden-tuned"] = normalized(gth.run_tuning())
    with open(sys.argv[1], "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
