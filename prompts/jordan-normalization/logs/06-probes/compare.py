"""
jordan-normalization prompt 06, §2 item 2 (orchestrator note 06, ruling 13): one tuning of one
point of README §0.3's grid, by ComputeTargets.ScalarHistory.tune_history, with the secant
(`secant`) or with the plain update at every iterate (`plain`, tune_history's secant=False).

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/jordan-normalization/logs/06-probes/compare.py BETA M_MP PHI_MP secant|plain

The setup is the orchestrator's prototype's: QCD_Cosmology(Planck2018) in Planck_units,
ExponentialModel(M = M_MP M_P, n = 1, beta = BETA), phi* = PHI_MP M_P, pi* = 0, T* = 2e4 GeV,
atol = rtol = 1e-8, the tuning tolerance 1e-5 and the cap of 30 (tune_history's defaults), and
main.py's default sampling axis, log_one_plus_z_grid(250, 300). tune_history starts from
model.seed(cosmology) and prints one progress line per integration; this script adds a header
line and a RESULT (or FAILURE) line. Nothing is stored. A record of the run, not imported by
anything.
"""

import sys
import time
from math import exp

from ComputeTargets.ScalarHistory import tune_history
from CosmologyConcepts import M_value, beta_value, redshift, redshift_array, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.redshift import log_one_plus_z_grid
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units


def main() -> int:
    beta, M, phi_init = map(float, sys.argv[1:4])
    method = sys.argv[4]
    if method not in ("secant", "plain"):
        raise SystemExit(f"method must be secant or plain, not {method!r}")

    units = Planck_units()
    cosmology = QCD_Cosmology(0, units, Planck2018())
    Mp = units.PlanckMass
    z_grid = redshift_array(
        z_array=[redshift(i, x) for i, x in enumerate(log_one_plus_z_grid(250, 300))]
    )
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)

    tag = f"beta={beta:g},M={M:g},phi*={phi_init:g},{method}"
    print(
        f"{tag} seed={seed.kind} norm={seed.normalization!r} phi_ref={seed.phi_ref!r}",
        flush=True,
    )

    start = time.perf_counter()
    data = tune_history(
        cosmology,
        model,
        temperature(0, 2.0e4 * units.GeV),
        phi_value(0, phi_init * Mp),
        pi_value(0, 0.0),
        z_grid,
        task_label=tag,
        secant=(method == "secant"),
    )
    total = time.perf_counter() - start

    if data.get("failure", False):
        print(f"{tag} FAILURE {data['failure_reason']} total={total:.1f}", flush=True)
        return 1

    print(
        f"{tag} RESULT integrations={data['tuning_iterations']} "
        f"plain={data['plain_steps']} secant={data['secant_steps']} guard={data['guard_steps']} "
        f"seed={data['seed_kind']} "
        f"normalization={data['normalization'].hex()} phi_ref={data['phi_ref'].hex()} "
        f"residual_H={data['residual_H']:+.6e} residual_phi={data['residual_phi']:+.6e} "
        f"Lambda_meV={exp(data['normalization'] / 4) / units.eV * 1e3:.6f} "
        f"phi_ref_Mp={data['phi_ref'] / Mp:.7e} "
        f"tracking={data['tracking']:.4f} total={total:.1f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
