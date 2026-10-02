# Open issues — project-wide index

**Last updated:** 2026-10-02 · **24 open**: 6 on the `review-remediation` board (closed
2026-09-30), 1 of them assigned to `science-readiness`; 2 on the `production-readiness` board
(closed 2026-09-30); 4 on the `run-integrity` board (closed 2026-09-30); 9 on the
`integrator-remediation` board (closed 2026-10-01), 1 of them assigned to `science-readiness`;
3 on the `science-readiness` board (planned 2026-10-01, 8 of 10 landed; prompt 06b added 2026-10-02).

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
[`science-readiness`](../prompts/science-readiness/IMPLEMENTATION_STATE.md)

## 1. Open, by owning board

### 1.1 `review-remediation` — [board §3](../prompts/review-remediation/IMPLEMENTATION_STATE.md)

Seven seeded at planning on 2026-09-29 from `.documents/audit-2026-09-29/README.md`. The two
opened after that on 2026-09-29 are resolved (board §4). One opened by prompt 02, four by prompt 03,
one by prompt 04, one by prompt 05, one by prompt 06. The campaign closed on 2026-09-30; these rows
stay here until a later campaign takes them. Four were assigned on 2026-09-30; all four are
resolved (§1.2). Three more were assigned on 2026-09-30 to `run-integrity` (§1.4); all three
are resolved. Three more were assigned on 2026-10-01 to `science-readiness` (§1.7); one since resolved (prompt 06 of that campaign).

- `[00-kicking-function-table-has-no-provenance-in-the-repository]` —
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` was added in commit `1759515` without the script
  that built it.
- `[00-two-files-are-not-black-clean]` — `Datastore/SQL/ObjectFactories/base.py` and
  `CosmologyModels/LambdaCDM/Planck.py` at `f5896bb`; housekeeping.
- `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` — three wrong comments in
  `CosmologyModels/GenericEOS/` (the base `dG_s_dlogT` docstring, the jax class's "1/GeV", the
  "600 keV" above `SAIKAWA_SHIRAI_T_LO`); comment-only.
- `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` — PRyMordial's Yp and D/H
  move by 1e-5–7e-4 under 1e-9–1e-8 changes to ρ_NP; prompt 04's 1e-4 D/H test passes at 8.85e-5
  inside that band.
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
two were assigned on 2026-10-01 to `science-readiness` (§1.7), one of them since resolved (prompt
02, 2026-10-01), and the eight below are not.

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

### 1.7 Assigned to `science-readiness` — [its board](../prompts/science-readiness/IMPLEMENTATION_STATE.md) §3.1

Assigned 2026-10-01 when the campaign was planned. Each entry stays on its own board, which
carries the **Assigned** line; the prompt that closes one deletes its row here.

- `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` — §7.2–7.4 need a dated
  addendum. `review-remediation`; prompt 08.
- `[00-stored-samples-alias-the-rebounds]` — the z grid samples fast bounces at random phase below
  about 100 eV; narrowed by prompt 05: no BBN half (resolved in PRyMordial's window); the adiabatic
  half stays open. `integrator-remediation`.

### 1.8 `science-readiness` — [board §3](../prompts/science-readiness/IMPLEMENTATION_STATE.md)

Seven opened by the planner on 2026-10-01, one per campaign item with no issue elsewhere.
Prompt 01 (2026-10-01) closed three, prompt 03 (2026-10-02) one, prompt 05 (2026-10-02) one and
prompt 07 (2026-10-02) one (board §4). Prompt 05 opened two (`[05-…]`). The re-plan of 2026-10-02 opened one for prompt
06b, which closed it (2026-10-02).

- `[00-documents-describe-the-thermo-route]` — three documents describe the replaced route.
  Prompt 08.
- `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]` — the cubic spline through the
  ratio's resolved bounce jumps may overshoot between samples; unmeasured.
- `[05-the-value-factory-compares-stored-phi-against-pi]` — `ScalarModelValue_factory.build`
  checks stored φ against supplied π (`Datastore/SQL/ObjectFactories/ScalarModel.py:1008` on
  `a522005`).
