# Open issues — project-wide index

**Last updated:** 2026-10-03 · **25 open**: 4 on the `review-remediation` board (closed
2026-09-30), none now assigned; 2 on the `production-readiness` board
(closed 2026-09-30); 4 on the `run-integrity` board (closed 2026-09-30); 9 on the
`integrator-remediation` board (closed 2026-10-01), none now assigned to `science-readiness`;
2 on the `science-readiness` board (planned 2026-10-01; closed 2026-10-02, 10 of 10 landed,
prompt 06b having been added 2026-10-02); 4 on the `bbn-tolerance` board (planned 2026-10-03,
2 of 5 landed (01c, 02); prompt 01 committed BLOCKED 2026-10-03 and ruled the same day: re-planned
around PRyMordial's small network, U3; prompt 01c measured the small network 2026-10-03; prompt
02 moved production to it and closed three, opening one, 2026-10-03).

This file exists so that an issue opened by one campaign is not lost when that campaign closes.
It is an **index, not a record**: one line per issue, pointing at the campaign status board that
holds the measurements, the impact statement and the next step. Never put issue content here — if
the two disagree, the board is right.

> **Maintenance rule.** Whenever you add, narrow or close an entry in a campaign board's
> §3 (Active and unresolved issues) or §4 (Resolved issues), update this file **in the same
> commit**. Add the line, move it between sections here, or delete it, and correct the count and
> the date above. See `CLAUDE.md`.

**Boards.** [`review-remediation`](../prompts/review-remediation/IMPLEMENTATION_STATE.md) ·
[`production-readiness`](../prompts/production-readiness/IMPLEMENTATION_STATE.md) ·
[`run-integrity`](../prompts/run-integrity/IMPLEMENTATION_STATE.md) ·
[`integrator-remediation`](../prompts/integrator-remediation/IMPLEMENTATION_STATE.md) ·
[`science-readiness`](../prompts/science-readiness/IMPLEMENTATION_STATE.md) ·
[`bbn-tolerance`](../prompts/bbn-tolerance/IMPLEMENTATION_STATE.md)

## 1. Open, by owning board

### 1.1 `review-remediation` — [board §3](../prompts/review-remediation/IMPLEMENTATION_STATE.md)

Seven seeded at planning on 2026-09-29 from `.documents/audit-2026-09-29/README.md`. The two
opened after that on 2026-09-29 are resolved (board §4). One opened by prompt 02, four by prompt 03,
one by prompt 04, one by prompt 05, one by prompt 06. The campaign closed on 2026-09-30; these rows
stay here until a later campaign takes them. Four were assigned on 2026-09-30; all four are
resolved (§1.2). Three more were assigned on 2026-09-30 to `run-integrity` (§1.4); all three
are resolved. Three more were assigned on 2026-10-01 to `science-readiness` (§1.7); all three since resolved (prompts 04, 06 and 08 of that campaign).
One more was assigned on 2026-10-03 to `bbn-tolerance`; it was resolved by that campaign's
prompt 02 on 2026-10-03 (§1.9).

- `[00-kicking-function-table-has-no-provenance-in-the-repository]` —
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` was added in commit `1759515` without the script
  that built it.
- `[00-two-files-are-not-black-clean]` — `Datastore/SQL/ObjectFactories/base.py` and
  `CosmologyModels/LambdaCDM/Planck.py` at `f5896bb`; housekeeping.
- `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` — three wrong comments in
  `CosmologyModels/GenericEOS/` (the base `dG_s_dlogT` docstring, the jax class's "1/GeV", the
  "600 keV" above `SAIKAWA_SHIRAI_T_LO`); comment-only.
- `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]` — the table's Σ peaks
  (QCD 0.3145, EW 0.0374) match neither the Σ the g's imply by conservation (0.299, 0.058) nor the
  4g_s/(3g_ρ) − 1 formula (0.249, 0.0374 at 46 GeV); a decision for the authors.

### 1.2 Assigned to `production-readiness` — [its board](../prompts/production-readiness/IMPLEMENTATION_STATE.md)

Assigned 2026-09-30 by the user, to clear before the production run. The entries stay on the
`review-remediation` board (§3), which carries the **Assigned** line; the prompt that closes one
deletes its row here.

None open: the last, `[00-adiabaticity-diagnostic-omits-the-source-response-term]`, was
resolved by prompt 03 on 2026-09-30.

### 1.3 `production-readiness` — [board §3](../prompts/production-readiness/IMPLEMENTATION_STATE.md)

Two opened when the plan was amended on 2026-09-30, both for the authors. The two opened by
prompt 03 were closed as accepted by the user the same day (board §4).

- `[00-adiabaticity-is-evaluated-at-fixed-k-over-H-not-for-fixed-comoving-modes]` — max |Q| is
  taken at fixed k_p/H ∈ {10 … 10⁴}, a different comoving mode at each N, with no horizon-scale
  mode; the paper's appendix fixes k = (aH) at the first rebound.
- `[00-paper-gives-two-inconsistent-adiabaticity-conditions]` — `Paper1.tex`'s main-text
  `eq:adiabaticity` (what the code does) and its appendix condition differ; the appendix also
  carries the H5 omission.

### 1.4 Assigned to `run-integrity` — [its board](../prompts/run-integrity/IMPLEMENTATION_STATE.md)

Assigned 2026-09-30 by the user, to clear before a science run. The entries stay on the
`review-remediation` board (§3), which carries the **Assigned** line; the prompt that closes one
deletes its row here.

None open: the last, `[03-main-recomputes-failed-bbn-rows-on-every-run]`, was resolved by
prompt 03 on 2026-09-30.

### 1.5 `run-integrity` — [board §3](../prompts/run-integrity/IMPLEMENTATION_STATE.md)

Two opened by the planner on 2026-09-30, both assigned to the campaign's own prompts; prompts 02
and 03 resolved them (board §4). Two opened by prompt 01, two by prompt 02, one by prompt 03 and
one by prompt 04 on 2026-09-30. Two were assigned on 2026-10-01 to `science-readiness` (§1.7);
its prompt 01 resolved both the same day.

- `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]` — unlike `ScalarModel.build`,
  the `AdiabaticHistory` and `BBNData` lookups do not filter `validated == True`, so an
  unvalidated row left by an interrupted run is served unless `--prune-unvalidated` is passed.
- `[01-plot-by-beta-profile-label-names-plot-scalarmodel]` — `plot_by_beta.py:87` names its
  profiling run `--plot_ScalarModel-`; cosmetic.
- `[03-step-1-first-pass-lookup-filters-nothing]` — `build_solver_batch`'s first-pass
  `ScalarModel` lookup keeps every pair as missing (`main.py:208`); one redundant lookup per
  batch, no wrong result.
- `[04-adiabatichistory-lookup-ignores-do-not-populate]` — `AdiabaticHistory.build` never reads
  `_do_not_populate`, which `main.py` and `plot_by_beta.py` pass, so every lookup reads every
  value row; a cost, not a wrong result.

### 1.6 `integrator-remediation` — [board §3](../prompts/integrator-remediation/IMPLEMENTATION_STATE.md)

Fourteen opened by the planner on 2026-10-01 from
[`.documents/integrator-audit-2026-09-30/README.md`](integrator-audit-2026-09-30/README.md), and
one the same day from the user's reflection guard G1. Prompt 01 (2026-10-01) closed three and
opened two (`[01-…]`); prompt 02 (2026-10-01) closed five and opened one (`[02-…]`); prompt 03
(2026-10-01) closed one; prompt 04, the close-out (2026-10-01), opened and closed none and the
campaign closed; one more was opened after the close-out (2026-10-01, `[post-…]`). Nine are open;
two were assigned on 2026-10-01 to `science-readiness` (§1.7). One was resolved (prompt 02,
2026-10-01). The other was narrowed (prompt 05, 2026-10-02) and came back here unassigned when that
campaign closed (2026-10-02). All nine are below.

- `[00-settling-at-physical-M-needs-a-parked-tracking-model]` — the field at φ_wall(ρ) as a
  passenger once its bounces are unresolvable: the switch criterion and the parked field's
  ρ_φ, p_φ; the authors' physics. Since prompt 02 its absence is a clean failure (step budget).
- `[00-atol-does-not-scale-with-phi]` — an `atol` vector scaled by M halves the first-bounce
  error at 1 % cost; tolerance-level.
- `[00-region-properties-on-the-potentials-become-unread]` — `bounce_region_*`,
  `default_max_step`, `hard_reflection_point` stay defined after prompt 01; housekeeping.
- `[00-analytic-jacobian-would-remove-num-jac]` — only if Newton failures appear in the science
  run.
- `[00-declare-reflects-at-origin-for-the-other-potentials]` — prompt 01 declares
  `reflects_at_origin` and `log_V_floor` on `ExponentialPotential` only; the other four
  potentials keep the defaults and would fail at the floor rather than reflect.
- `[01-trial-state-exception-in-radau-start-up-is-not-a-rejection]` — an RHS exception inside
  `Radau.__init__` (start or reflection restart) fails the history; never seen.
- `[02-negative-E-is-clamped-not-raised-on-trial-states]` — `E < 0` is printed and clamped to 0,
  the last silent substitution on an unphysical state; never seen.
- `[post-adiabatic-Q-reads-aliased-late-samples]` — below 1 keV the z grid misses most bounces
  at small M (0.02–0.3 samples per half-period at 1e-5, 1e-6); max |Q| may be set by aliasing.
- `[00-stored-samples-alias-the-rebounds]` — the z grid samples fast bounces at random phase below
  about 100 eV; narrowed by `science-readiness` prompt 05: no BBN half (resolved in PRyMordial's
  window); the adiabatic half stays open. Unassigned since 2026-10-02.

### 1.7 Assigned to `science-readiness` — [its board](../prompts/science-readiness/IMPLEMENTATION_STATE.md) §3.1

Assigned 2026-10-01 when the campaign was planned. Each entry stays on its own board, which
carries the **Assigned** line; the prompt that closes one deletes its row here.

None remain. Of the seven, six were resolved. The seventh, `[00-stored-samples-alias-the-rebounds]`,
was narrowed and returned to §1.6, unassigned, when the campaign closed (2026-10-02).

### 1.8 `science-readiness` — [board §3](../prompts/science-readiness/IMPLEMENTATION_STATE.md)

Seven opened by the planner on 2026-10-01, one per campaign item with no issue elsewhere.
Prompt 01 (2026-10-01) closed three, prompt 03 (2026-10-02) one, prompt 05 (2026-10-02) one,
prompt 07 (2026-10-02) one and prompt 08 (2026-10-02) one (board §4). Prompt 05 opened two (`[05-…]`). The re-plan of 2026-10-02 opened one for prompt
06b, which closed it (2026-10-02). The campaign closed on 2026-10-02 (prompt 09, the close-out)
with both `[05-…]` rows still open and unassigned.

- `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]` — the cubic spline through the
  ratio's resolved bounce jumps may overshoot between samples; unmeasured.
- `[05-the-value-factory-compares-stored-phi-against-pi]` — `ScalarModelValue_factory.build`
  checks stored φ against supplied π (`Datastore/SQL/ObjectFactories/ScalarModel.py:1008` on
  `a522005`).

### 1.9 Assigned to `bbn-tolerance` — [its board](../prompts/bbn-tolerance/IMPLEMENTATION_STATE.md) §3.1

Assigned 2026-10-03 when the campaign was planned. Each entry stays on its own board, which
carries the **Assigned** line; the prompt that closes one deletes its row here.

None open: `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` was resolved by
prompt 02 on 2026-10-03.

### 1.10 `bbn-tolerance` — [board §3](../prompts/bbn-tolerance/IMPLEMENTATION_STATE.md)

Two opened by the planner on 2026-10-03, one the same day for prompt 01b (the user's U2), two by
prompt 01 the same day, and one by prompt 02 the same day. Prompt 02 closed the planner's two.

- `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]` — PRyMordial's Li8(p,d)Li7 reverse rate is
  exp(γ/T9) times a ringing quadratic spline, ±1e39 near 1 keV; it stalls BDF's Newton iteration;
  worked around, not patched (U3); narrowed by prompt 02: production no longer runs the full
  network, which keeps the defect for anyone who selects it.
- `[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]` — B8's low-T equation reads Y in
  the old species order (upstream too); effect not measured, probably negligible.
- `[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]` — the `T_deliver` figure's threshold
  curve is 1/√(3Σ), not the paper's 1/√(3Σ_eff); its minimum is 1.0295 where the paper says 1.11.
- `[02-two-places-still-say-production-runs-the-full-network]` — `tools/history_and_bbn.py`
  still defaults to the full network "as main.py", and a comment in `_configure_PRyMordial` says
  production runs it.
