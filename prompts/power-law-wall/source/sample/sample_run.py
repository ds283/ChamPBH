"""
power-law-wall prompt 04 §1.1, §1.3 (P24): the runner. One history through tools/history_and_bbn.py's own `main`,
in-process, with two wrappers held only for the duration of the call:
  - ComputeTargets.ScalarHistory.integrate_scalar_history, to keep the tuned history's final IntegrationResult, as
    02's path_map.py and the tool itself keep it (the tool's own wrapper wraps this one);
  - the tool module's `tune_history` name, to keep the payload it returns.
No production module is changed or re-implemented: the tool's lines are printed by the tool, unchanged, and then this
runner prints its instrumentation, every line of it beginning with `sample` or `cand`, so that the tool's lines are the
file's other lines (prompt §1.1: they are checked against a plain run of the tool, timings masked).

Run from the repository root, with the tool's own arguments:

    PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/sample/sample_run.py BETA M [the tool's options]

The exit status is the tool's. The instrumentation (prompt §1.3), after the tool's lines:

    sample outcome: status=ok|failure exit=... reason=...
    sample tuning: integrations=... seed_kind=... residual_H=... residual_phi=... normalization=... phi_ref_Mp=...
    sample cascade: reflections=... candidates=... injections=... resolved=... unresolved=... N_final=...
        first_impact_log_R=... first_impact_log_x=...
    sample drain: law=... A=... model=... f_min=... f_max=... alpha_eff_min=... alpha_eff_max=... at_cap=...
        log_R_min=... log_R_max=... ell_min=... ell_max=... consistency_mismatches=...
    sample parking: parked=... N=... T_J_GeV=... candidate_index=... rho_kin_over_rho_rad=... m_eff_over_H=...
        min_m_eff_over_H=... parked_steps=... K_share_parking=... released=... release_N=... release_T_J_GeV=...
        release_m_eff_over_H=... release_N_check=... release_m_eff_over_H_check=... release_candidate_index=...
        min_ratio_before_cut=... min_ratio_before_cut_index=...
    sample r9: K_final=... share_x_lt_100=... share_x_lt_1000=... log_x_min=... inv_s_t_minus_1_at_min=...
        log_x_max=...
    sample thresholds: phi_t_max_Mp=... phi_t_ge_1e-8=... log_R_min=... log_R_le_0=... unresolvable_assessments=...
        unresolved_declined=... log1p_x_min_over_16ulp=... max_wall_to_kinetic_ratio=...
    cand idx N kind log_x phi_t_Mp log_R ell regime f alpha_eff w_gate dK_over_K resolved rho_kin_over_rho_rad

Definitions (prompt §1.3, §1.4):
  - ln x = ln(E/V0) = the Injection's log_E_assessed less the tuned normalization (P27);
  - the regime and ell: under the one-loop law the DrainCandidate's; under the fixed law the regime drain_regime
    would give, on the tuned realization, as path_map.py prints it;
  - f: under the one-loop law the law's fraction at the candidate, the model's own (RealizedModel.drain_fraction) or
    the L law (drain_fraction), as drain_alpha_effective takes it; `-` under the fixed law. consistency_mismatches
    counts the candidates where min(DRAIN_ALPHA_CAP, A f) w_gate is not the recorded alpha_eff (expected 0);
  - at_cap: the candidates with A f >= DRAIN_ALPHA_CAP (0.99) (one-loop law only);
  - dK_over_K: the Injection's delta_K over K(N_final) of the payload's ledger;
  - rho_kin_over_rho_rad: rho_kin,E/rho_R,E before the drain, as the parking cut computes it: exp(log_rho_kin less
    ln rho_R,E of the state at the candidate's N, from the solution, which is right-continuous there);
  - share_x_lt_100, share_x_lt_1000: the share of K(N_final) drained at candidates with E/V0 < 100, < 10^3 (R9's
    share); inv_s_t_minus_1_at_min: 1/s_t - 1 = 1/x at the smallest x, the table's error in x there less 1
    (verification §3.5: s_t = x/(1 + x));
  - min_ratio_before_cut: the smallest rho_kin,E/rho_R,E over the candidates the cut's ratio test examined before it
    fired (indices 1 to candidate_index - 1; over every candidate from 1 if the history did not park);
  - log1p_x_min_over_16ulp: ln(1 + x) at the smallest x, which is ln V - ln V_floor at that candidate's turning
    point, in units of WALL_ROUNDING_QUANTA ulp(|ln V0|), the width _log_V_minus_floor compares it with.
Nothing is stored. A failure prints the outcome line alone, with the payload's reason.
"""

import importlib
import sys
from math import exp, inf, log, log1p, ulp

sys.path.insert(0, ".")

from CosmologyConcepts.Models.AbstractModel import WALL_ROUNDING_QUANTA  # noqa: E402
from tools import history_and_bbn as tool  # noqa: E402

SM = importlib.import_module("ComputeTargets.ScalarHistory")


def g(v, spec=".6g"):
    return "None" if v is None else format(v, spec)


def run(argv):
    captured = []
    payload = []
    integrate = SM.integrate_scalar_history
    tune = tool.tune_history

    def keep_result(*a, **kw):
        res = integrate(*a, **kw)
        captured[:] = [res]
        return res

    def keep_payload(*a, **kw):
        data = tune(*a, **kw)
        payload[:] = [data]
        return data

    SM.integrate_scalar_history = keep_result
    tool.tune_history = keep_payload
    try:
        rc = tool.main(argv)
    finally:
        SM.integrate_scalar_history = integrate
        tool.tune_history = tune
    sys.stdout.flush()
    return rc, (payload[-1] if payload else None), (captured[-1] if captured else None)


def instrument(argv, rc, data, res):
    if data is None:
        print(f"sample outcome: status=refused exit={rc} reason=None")
        return
    if data.get("failure", False):
        print(
            f"sample outcome: status=failure exit={rc} reason={data.get('failure_reason')}"
        )
        return
    print(f"sample outcome: status=ok exit={rc} reason=None")

    # the setup, as the tool builds it from the same arguments
    beta, M = float(argv[0]), float(argv[1])
    opts = {}
    for k, word in enumerate(argv[2:-1], start=2):
        if word.startswith("--"):
            opts[word] = argv[k + 1]
    model_name = opts.get("--model", tool.MODEL_DEFAULT)
    n = int(opts.get("--n", tool.N_DEFAULT))
    units = tool.Planck_units()
    Mp = units.PlanckMass
    model = tool.build_model(model_name, M, n, beta, units)
    norm, phi_ref = data["normalization"], data["phi_ref"]
    realized = model.realize(norm, phi_ref)

    print(
        f"sample tuning: integrations={data['tuning_iterations']} seed_kind={data['seed_kind']} "
        f"residual_H={data['residual_H']!r} residual_phi={data['residual_phi']!r} normalization={norm!r} "
        f"phi_ref_Mp={phi_ref / Mp!r}"
    )

    injections = res.injections
    drain = data["drain"]
    one_loop = drain.law == SM.DRAIN_LAW_ONE_LOOP
    A = drain.A
    K_final = data["dark_radiation"].K(res.N_final)

    def log_x(inj):
        return None if inj.log_E_assessed is None else inj.log_E_assessed - norm

    first = injections[0] if injections else None
    print(
        f"sample cascade: reflections={len(res.reflections)} candidates={len(injections)} "
        f"injections={sum(1 for i in injections if i.delta_K > 0.0)} "
        f"resolved={sum(1 for i in injections if i.resolved)} "
        f"unresolved={sum(1 for i in injections if not i.resolved)} N_final={res.N_final:.9f} "
        f"first_impact_log_R="
        f"{g(None if first is None or first.classical_turning_point is None else first.classical_turning_point.log_R, '.8g')} "
        f"first_impact_log_x={g(None if first is None else log_x(first), '.8g')}"
    )

    # the per-candidate rows
    rows = []
    for k, inj in enumerate(injections):
        ct = inj.classical_turning_point
        lx = log_x(inj)
        f = None
        if one_loop:
            c = drain.candidates[k]
            regime, ell = c.regime, c.ell
            if regime == SM.REGIME_UNASSESSED:
                f = 0.0
            elif realized.drain_L_implemented:
                f = SM.drain_fraction(regime, ell, c.L, c.log_R)
            else:
                f = realized.drain_fraction(regime, ell, c.log_E, c.log_R)
        elif ct is not None:
            regime, ell, _ = SM.drain_regime(
                realized, (inj.phi_assessed, inj.log_E_assessed, ct)
            )
        else:
            regime, ell = SM.REGIME_UNASSESSED, None
        state = res.solution(inj.N)
        ratio = exp(inj.log_rho_kin - float(state[2]))
        rows.append(
            dict(
                idx=k,
                N=inj.N,
                kind=inj.kind,
                log_x=lx,
                phi_t=None if ct is None else ct.phi_t / Mp,
                log_R=None if ct is None else ct.log_R,
                ell=ell,
                regime=regime,
                f=f,
                alpha_eff=inj.alpha_eff,
                w_gate=inj.w_gate,
                dK=inj.delta_K / K_final if K_final > 0.0 else None,
                resolved=inj.resolved,
                ratio=ratio,
            )
        )

    assessed = [r for r in rows if r["log_R"] is not None]
    if one_loop:
        fs = [r["f"] for r in rows if r["regime"] != SM.REGIME_UNASSESSED]
        mismatches = sum(
            1
            for r in rows
            if r["regime"] != SM.REGIME_UNASSESSED
            and min(SM.DRAIN_ALPHA_CAP, A * r["f"]) * r["w_gate"] != r["alpha_eff"]
        )
        at_cap = sum(
            1
            for r in rows
            if r["regime"] != SM.REGIME_UNASSESSED and A * r["f"] >= SM.DRAIN_ALPHA_CAP
        )
    else:
        fs, mismatches, at_cap = [], None, None
    alphas = [r["alpha_eff"] for r in rows]
    ells = [r["ell"] for r in rows if r["ell"] is not None]
    log_Rs = [r["log_R"] for r in assessed]
    print(
        f"sample drain: law={drain.law} A={g(A)} model={drain.model} "
        f"f_min={g(min(fs) if fs else None)} f_max={g(max(fs) if fs else None)} "
        f"alpha_eff_min={g(min(alphas) if alphas else None)} alpha_eff_max={g(max(alphas) if alphas else None)} "
        f"at_cap={g(at_cap)} log_R_min={g(min(log_Rs) if log_Rs else None, '.8g')} "
        f"log_R_max={g(max(log_Rs) if log_Rs else None, '.8g')} "
        f"ell_min={g(min(ells) if ells else None)} ell_max={g(max(ells) if ells else None)} "
        f"consistency_mismatches={g(mismatches)}"
    )

    park, rel = res.parking, res.release
    cut = park.candidate_index if park is not None else len(rows)
    before = [r for r in rows if 1 <= r["idx"] < cut]
    mb = min(before, key=lambda r: r["ratio"]) if before else None
    GeV = log(units.GeV)
    print(
        "sample parking: "
        + (
            "parked=False N=None T_J_GeV=None candidate_index=None rho_kin_over_rho_rad=None m_eff_over_H=None "
            "min_m_eff_over_H=None parked_steps=None K_share_parking=None "
            if park is None
            else f"parked=True N={park.N:.9f} T_J_GeV={exp(park.log_T_Jordan - GeV):.6g} "
            f"candidate_index={park.candidate_index} rho_kin_over_rho_rad={park.rho_kin_over_rho_rad:.6g} "
            f"m_eff_over_H={park.m_eff_over_H:.6g} min_m_eff_over_H={park.min_m_eff_over_H:.6g} "
            f"parked_steps={park.parked_steps} "
            f"K_share_parking={g(park.delta_K / K_final if K_final > 0.0 else None)} "
        )
        + (
            "released=False release_N=None release_T_J_GeV=None release_m_eff_over_H=None release_N_check=None "
            "release_m_eff_over_H_check=None release_candidate_index=None "
            if rel is None
            else f"released=True release_N={rel.N:.9f} release_T_J_GeV={exp(rel.log_T_Jordan - GeV):.6g} "
            f"release_m_eff_over_H={rel.m_eff_over_H:.6g} release_N_check={rel.N_check:.9f} "
            f"release_m_eff_over_H_check={rel.m_eff_over_H_check:.6g} "
            f"release_candidate_index={rel.candidate_index} "
        )
        + f"min_ratio_before_cut={g(None if mb is None else mb['ratio'])} "
        f"min_ratio_before_cut_index={g(None if mb is None else mb['idx'])}"
    )

    xs = [r for r in rows if r["log_x"] is not None]
    xmin = min(xs, key=lambda r: r["log_x"]) if xs else None

    def share(bound):
        if K_final <= 0.0:
            return None
        return (
            sum(
                inj.delta_K
                for r, inj in zip(rows, injections)
                if r["log_x"] is not None and r["log_x"] < log(bound)
            )
            / K_final
        )

    print(
        f"sample r9: K_final={K_final!r} share_x_lt_100={g(share(100.0))} share_x_lt_1000={g(share(1000.0))} "
        f"log_x_min={g(None if xmin is None else xmin['log_x'], '.8g')} "
        f"inv_s_t_minus_1_at_min={g(None if xmin is None else exp(-xmin['log_x']))} "
        f"log_x_max={g(max(r['log_x'] for r in xs) if xs else None, '.8g')}"
    )

    phis = [r["phi_t"] for r in assessed]
    width = WALL_ROUNDING_QUANTA * ulp(abs(norm))
    print(
        f"sample thresholds: phi_t_max_Mp={g(max(phis) if phis else None)} "
        f"phi_t_ge_1e-8={sum(1 for p in phis if p >= SM.UNRESOLVED_PHI_T_THRESHOLD_MP)} "
        f"log_R_min={g(min(log_Rs) if log_Rs else None, '.8g')} "
        f"log_R_le_0={sum(1 for v in log_Rs if v <= 0.0)} "
        f"unresolvable_assessments={drain.unresolvable_assessments} "
        f"unresolved_declined={drain.unresolved_declined} "
        f"log1p_x_min_over_16ulp={g(None if xmin is None else log1p(exp(xmin['log_x'])) / width)} "
        f"max_wall_to_kinetic_ratio={g(res.max_wall_to_kinetic_ratio)}"
    )

    print(
        "cand idx N kind log_x phi_t_Mp log_R ell regime f alpha_eff w_gate dK_over_K resolved rho_kin_over_rho_rad"
    )
    for r in rows:
        print(
            f"cand {r['idx']} {r['N']:.6f} {r['kind']} {g(r['log_x'], '.8g')} {g(r['phi_t'], '.6e')} "
            f"{g(r['log_R'], '.8g')} {g(r['ell'])} {r['regime']} {g(r['f'])} {g(r['alpha_eff'])} "
            f"{g(r['w_gate'])} {g(r['dK'])} {r['resolved']} {g(r['ratio'])}"
        )


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    rc, data, res = run(argv)
    instrument(argv, rc, data, res)
    return rc


if __name__ == "__main__":
    sys.exit(main())
