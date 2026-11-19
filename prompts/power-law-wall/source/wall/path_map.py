"""
power-law-wall prompt 02 §1.4 (P7): the paths. One history, tuned by the tool route (tools/history_and_bbn.py's setup,
its model builder and its z grid, through tune_history), under the fixed drain law, with its final IntegrationResult
kept by wrapping integrate_scalar_history in-process, as crossover's cascade_map.py and the tool itself keep it. No
production module is changed; the BBN stage of the tool is not run, since nothing here reads it.

For every candidate (Injection) it prints, after the run, at the tuned model realized at the tuned normalization:
  idx, N, kind, ln x = ln(E/V0) (E the assessment's, from the floor), ln R, phi_t/M_P, ell_t (the
  ClassicalTurningPoint's), ell of one_loop_turnaround at the candidate's assessment (or why there is none), the regime
  the one-loop law would give it (drain_regime: one_loop, gap or classical; the fixed law does not classify),
  ln(k_ex/M_P) from classical_turning_point's log_k_ex and from one_loop_turnaround's (log_k_ex - ln units.PlanckMass,
  Einstein frame), phi_ta/M_P, the path formula's ln R at the candidate's x and its residual, and P at the parking cut.

The path formula (prompt §1.1), on the power wall:  ln R = ln(V0/M^4) + 2 ln(n(n + 1)) - ln 8 pi^2 + (1 + 4/n) ln x;
on the exponential (crossover README, "The scan"), at n = 1:
    ln R = ln(V0/M^4) + 2L + 2 ln(L^4 + 2L^3) - ln(e^L - 1) - ln 8 pi^2,   L = ln(1 + x).

Run from the repository root (PRyMordial is not run, but the EOS reads its tables from the working directory):

    PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/wall/path_map.py BETA M --model MODEL --n N \\
        --alpha ALPHA > .documents/power-law-wall/wall-02/map/<name>.txt

M is in units of M_P. runs_map.sh runs the twenty-five histories of prompt §1.4.
"""

import argparse
import importlib
import os
import sys
import time
from math import exp, expm1, log, log1p, pi

sys.path.insert(0, ".")

from ComputeTargets.exceptions import ComputationFailureError  # noqa: E402
from ComputeTargets.ScalarHistory import tune_history  # noqa: E402
from CosmologyConcepts import temperature  # noqa: E402
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Units import Planck_units  # noqa: E402
from tools import history_and_bbn as tool  # noqa: E402

SM = importlib.import_module("ComputeTargets.ScalarHistory")
LOG_8PI2 = log(8.0 * pi * pi)


def path_lnR(model: str, n: int, log_V0_M4: float, log_x: float) -> float:
    """The path formula's ln R at ln x = ln(E/V0)."""
    if model == "inverse-power":
        return log_V0_M4 + 2.0 * log(n * (n + 1)) - LOG_8PI2 + (1.0 + 4.0 / n) * log_x
    # the exponential at n = 1 (crossover's curve); L = ln(1 + x), ln(e^L - 1) = ln x
    L = log_x + log1p(exp(-log_x)) if log_x > 0 else log1p(exp(log_x))
    return log_V0_M4 + 2.0 * L + 2.0 * log(L**4 + 2.0 * L**3) - log_x - LOG_8PI2


def fmt(v, spec):
    return "-" if v is None else format(v, spec)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("beta", type=float)
    p.add_argument("M", type=float, help="M in units of M_P")
    p.add_argument("--model", choices=tuple(tool.MODEL_CLASSES), required=True)
    p.add_argument("--n", type=int, default=1)
    p.add_argument("--alpha", type=float, required=True)
    a = p.parse_args()

    units = Planck_units()
    Mp = units.PlanckMass
    model = tool.build_model(a.model, a.M, a.n, a.beta, units)
    cosmology = QCD_Cosmology(0, units, Planck2018())
    T_init = temperature(0, tool.T_INIT_GEV * units.GeV)
    phi_init = phi_value(0, tool.PHI_INIT_MP * Mp)
    pi_init = pi_value(0, tool.PI_INIT)
    label = f"beta={a.beta:g} M={a.M:g} model={a.model} n={a.n} alpha={a.alpha:g}"

    captured = []
    integrate = SM.integrate_scalar_history

    def capture(*args, **kw):
        res = integrate(*args, **kw)
        captured[:] = [res]
        return res

    SM.integrate_scalar_history = capture
    t0 = time.perf_counter()
    try:
        data = tune_history(
            cosmology,
            model,
            T_init,
            phi_init,
            pi_init,
            tool._z_grid(),
            task_label=f"power-law-wall-02-path-map-{a.model}-n{a.n}",
            tolerance=tool.DEFAULT_TUNING_TOLERANCE,
            max_integrations=tool.DEFAULT_TUNING_MAX_INTEGRATIONS,
            alpha=a.alpha,
            drain_law="fixed",
            progress="every",
        )
    finally:
        SM.integrate_scalar_history = integrate
    wall = time.perf_counter() - t0
    print(f"# {label}: uptime at end: {os.popen('uptime').read().strip()}")
    if data.get("failure", False):
        print(
            f"# {label}: FAILURE wall={wall:.1f} s reason={data.get('failure_reason')}"
        )
        return 1

    res = captured[-1]
    norm, phi_ref = data["normalization"], data["phi_ref"]
    realized = model.realize(norm, phi_ref)
    log_M = log(a.M * Mp)
    log_V0_M4 = norm - 4.0 * log_M
    log_Mp = log(Mp)
    bounce = data["first_bounce"]
    park = res.parking
    print(
        f"# {label}: tuned normalization ln V0 = {norm!r}, phi_ref = {phi_ref / Mp!r} M_P, "
        f"integrations = {data['tuning_iterations']}, wall = {wall:.1f} s"
    )
    print(
        f"# ln(V0/M^4) = {log_V0_M4:.6f}; injections = {len(res.injections)}; reflections = {len(res.reflections)}; "
        f"N_final = {res.N_final:.6f}; parking: "
        + (
            "none"
            if park is None
            else f"N = {park.N:.6f} at candidate {park.candidate_index}, "
            f"ln T_J = {park.log_T_Jordan - log(units.GeV):.6f} (GeV)"
        )
    )
    if bounce is None:
        print("# first bounce: none")
    else:
        print(
            f"# first bounce: N = {bounce.N:.9f}, T_J = {exp(bounce.log_T_Jordan) / units.MeV:.6f} MeV, "
            f"phi = {bounce.phi_Einstein / Mp:.6e} M_P, reflected = {bounce.reflected}"
        )
    print(
        "# idx        N         kind   ln(E/V0)       ln R    phi_t/M_P     ell_t        ell     regime "
        "lnkex_t/MP lnkex_ta/MP  phi_ta/M_P  lnR(path) resid(path) lnA(phi_t) P"
    )
    for i, inj in enumerate(res.injections):
        ct = inj.classical_turning_point
        if ct is None:
            print(f"{i:5d} {inj.N:9.5f} {inj.kind:>12s}   unassessed")
            continue
        log_x = inj.log_E_assessed - norm
        assessment = (inj.phi_assessed, inj.log_E_assessed, ct)
        regime, ell_regime, _ = SM.drain_regime(realized, assessment)
        try:
            ta = realized.one_loop_turnaround(inj.phi_assessed, inj.log_E_assessed)
            ta_note = None if ta is not None else "None(inside)"
        except ComputationFailureError:
            ta, ta_note = None, "nochain"
        ell_s = f"{ta.ell:10.5f}" if ta is not None else f"{ta_note:>10s}"
        lnR_path = path_lnR(a.model, a.n, log_V0_M4, log_x)
        mark = (
            "P"
            if park is not None and park.candidate_index == i
            else ("p" if park is not None and i > park.candidate_index else "")
        )
        print(
            f"{i:5d} {inj.N:9.5f} {inj.kind:>12s} {log_x:11.7f} {ct.log_R:11.6f} {ct.phi_t / Mp:12.5e} "
            f"{ct.ell_t:9.5f} {ell_s} {regime:>10s} {ct.log_k_ex - log_Mp:11.6f} "
            f"{fmt(None if ta is None else ta.log_k_ex - log_Mp, '12.6f'):>12s} "
            f"{fmt(None if ta is None else ta.phi_ta / Mp, '11.4e'):>11s} "
            f"{lnR_path:10.5f} {lnR_path - ct.log_R:11.3e} {realized.log_Omega(ct.phi_t):10.3e} {mark}"
        )
        if regime == SM.REGIME_ONE_LOOP and (ta is None or ell_regime != ta.ell):
            print(
                f"# !! idx {i}: drain_regime's ell {ell_regime} differs from the turnaround's"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
