"""
crossover side-campaign (one-loop-drain README §8.9), stage 1: where the cascade's bounces sit in the variables
of the crossover problem.

For one history, realized at a tuned normalization and phi_ref (read from a 02 or 02b task's result.json, so that
nothing is re-tuned), it runs one integration under the one-loop law through integrate_history, as the tool route
does, and keeps the IntegrationResult by wrapping integrate_scalar_history. For every candidate it prints:
  N, the regime, ell (one-loop only), alpha_eff, ln(E/V0), ln R, L = M/phi_t (the classical turning point), ell_t,
  and ln(E/M^4).
V0 = exp(normalization) is the floor of V = V0 exp(M/phi) (Planck units), so E/V0 says whether the bounce is on the
exponential wall (E >> V0, L = ln(1 + E/V0) large) or in the 1/phi tail (E << V0, L << 1).

    PYTHONPATH=. ./venv/bin/python prompts/one-loop-drain/source/crossover/cascade_map.py \\
        var/one-loop-drain/02b/tasks/P-2197-A0.05-zero.result.json [TUNING_FROM.result.json] > .../out/...
"""

import json
import sys
from math import exp, log

sys.path.insert(0, ".")

import importlib  # noqa: E402

SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.ScalarHistory import integrate_history  # noqa: E402
from CosmologyConcepts import M_value, beta_value, temperature  # noqa: E402
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Units import Planck_units  # noqa: E402
from tools import history_and_bbn as tool  # noqa: E402


def main(result_json: str, tuning_json=None) -> int:
    r = json.load(open(result_json))
    # a failed tuning has no tuning record: borrow the normalization and phi_ref of another task of the same
    # history (the seed is the same at every A; 02's lines show it)
    if tuning_json is not None:
        r["tuning"] = json.load(open(tuning_json))["tuning"]
    argv = r["argv"]
    beta, M = float(argv[0]), float(argv[1])
    A = float(argv[argv.index("--alpha") + 1])
    gap = argv[argv.index("--gap-prescription") + 1]
    norm, phi_ref = r["tuning"]["normalization"], r["tuning"]["phi_ref"]

    units = Planck_units()
    cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
    )
    T_init = temperature(0, tool.T_INIT_GEV * units.GeV)
    T_stop = temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin)
    phi_init = phi_value(0, 5.0 * units.PlanckMass)
    pi_init = pi_value(0, tool.PI_INIT)

    captured = []
    integrate = SM.integrate_scalar_history

    def capture(*a, **kw):
        res = integrate(*a, **kw)
        captured[:] = [res]
        return res

    SM.integrate_scalar_history = capture
    # one-loop-drain prompt 02c removed _GAP_PRESCRIPTION: set it only where it exists
    has_gap = hasattr(SM, "_GAP_PRESCRIPTION")
    if has_gap:
        gap_before = SM._GAP_PRESCRIPTION
        SM._GAP_PRESCRIPTION = gap
    try:
        data = integrate_history(
            cosmology,
            T_init,
            T_stop,
            phi_init,
            pi_init,
            tool._z_grid(),
            model.realize(norm, phi_ref),
            task_label="crossover-cascade-map",
            alpha=A,
            drain_law="one_loop",
        )
    finally:
        SM.integrate_scalar_history = integrate
        if has_gap:
            SM._GAP_PRESCRIPTION = gap_before
    res = captured[-1]
    cands = res.drain.candidates
    Mp_units = M  # M in M_P; Planck units throughout
    log_M4 = 4.0 * log(Mp_units)
    print(
        f"# {r['id']}: beta={beta} M={M} M_P A={A} gap={gap} normalization(ln V0)={norm} "
        f"ln(V0/M^4)={norm - log_M4:.4f} failure={data.get('failure', False)} "
        f"parking_N={None if res.parking is None else res.parking.N} N_final={res.N_final} "
        f"candidates={len(cands)}"
    )
    print(
        "# idx       N    regime       ell   alpha_eff   ln(E/V0)     ln R         L     ell_t   ln(E/M^4)"
    )
    for i, (inj, c) in enumerate(zip(res.injections, cands)):
        ct = inj.classical_turning_point
        logE = inj.log_E_assessed
        L = None if ct is None else M / ct.phi_t
        print(
            f"{i:5d} {inj.N:8.4f} {c.regime:>9s} "
            f"{'' if c.ell is None else format(c.ell, '9.4f'):>9s} {c.alpha_eff:11.4e} "
            f"{'' if logE is None else format(logE - norm, '10.4f'):>10s} "
            f"{'' if ct is None else format(ct.log_R, '8.3f'):>8s} "
            f"{'' if L is None else format(L, '9.4g'):>9s} "
            f"{'' if ct is None else format(ct.ell_t, '9.4f'):>9s} "
            f"{'' if logE is None else format(logE - log_M4, '10.4f'):>10s}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
