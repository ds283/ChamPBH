# Integrator remediation campaign — implementation state

**Last updated:** 2026-10-01 · **Status: COMPLETE — 4 of 4 landed** (prompts 01, 02, 03 and the
close-out 04; final code tree `abcc99f`, the close-out commit is the one after it, see `git log`
"Close the integrator-remediation campaign with a handover"). Suites on the final tree:
CosmologyModels 18, ComputeTargets 67, Datastore 17 (18, 41, 17 at `2b89022`). Prompt 04's
first-bounce question was ruled by the user on 2026-10-01 (Decisions): the dense-output turning
point, on which all nine histories pass. Planned on 2026-10-01 against
`main` at `2b89022`, from the audit at
[`.documents/integrator-audit-2026-09-30/README.md`](../../.documents/integrator-audit-2026-09-30/README.md).
**Target branch** `integrator-remediation`, to be cut from `2b89022`; planning and orchestration
commits land on it.
**Every store made before 2026.5.0 is invalid.** `VERSION_LABEL` is `"2026.5.0"` since prompt 01
(2026-10-01); it was `"2026.4.0"` before.

The campaign replaces the two-region, fragment-and-event step control of `compute_scalar_model`
with one Radau step loop under a kinematic step cap, keeps the elastic reflection as a deliberate
model triggered at the representable-step floor, clamps SciPy's Jacobian perturbation factor,
treats a trial-state exception as a rejected step, deletes the unwired solver fallback, settles
the exception taxonomy, adds a step budget so physical-`M` histories fail cleanly, and documents
all of it. The parked-tracking model those histories need is physics for the authors and is
recorded, not built.

**Campaign:** [`README.md`](README.md) ·
**Code (planned):**
- `ComputeTargets/ScalarModel.py` (the loop, the metadata, the taxonomy);
- `Quadrature/supervisors/ScalarField.py`, `Quadrature/supervisors/base.py` (`RHS_timer`);
- `extract_common.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, `main.py` (the reflection count
  and the stepper label);
- `config/version.py` (the bump);
- `ComputeTargets/tests/` (new tests; one rewritten);
- `.documents/numerical-strategies.md`, `.documents/architecture-summary.md`,
  `.documents/integrator-audit-2026-09-30/README.md`,
  `.documents/paper-corrections-numerical-section.md` (new),
  `.documents/review-remediation-verification.md` (additive only).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.6.

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing at the board that holds it. Where
> the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-10-01, the user: all five README §0.2 decisions accepted as proposed.** `f = 0.1`;
  `h_floor = 1e-11` with the elastic reflection at the floor; delete the fallback; tolerances stay
  `1e-8`, no `atol` vector; a step budget of `2×10⁶` accepted steps as a clean failure; one bump to
  `"2026.5.0"` in prompt 01. The orchestrator for prompt 01 may dispatch.
- **2026-10-01, the user: two guards on the reflection (README §2 (b′)), in prompt 01.** G1: the
  potential must declare `reflects_at_origin`, else reaching the floor is a failure. G2: at the
  moment of reflection the wall part of the potential fraction must not exceed the kinetic
  fraction, `3 (V − V_floor)/(3H²M_P²) ≤ ½π²`, else the step has already passed the wall and the
  history fails. The user's question was whether the floor criterion, which does not know where
  the wall is, could fire after a resolved reflection had been stepped past; the planner's
  measurement (ratio `≤ 1.6e-4` on every legitimate reflection, exactly 0 at physical `M`, `≈ 23`
  on a step-over state) is in README §2 (b′). A first formulation with the full `V` was wrong near
  `T_CMB`, where the constant `Λ⁴` is a dark-energy-sized fraction of `3H²M_P²`; the guard uses
  `V − Λ⁴`.
- **2026-10-01, the planner: the parked-tracking model is out of scope.** It decides what the
  field *is* once its bounces are unresolvable, and what it contributes to BBN and the adiabatic
  stage; that is the authors' physics, not step control (audit §3.7, §9.4). The budget makes its
  absence a clean failure (prompt 02).
- **2026-10-01, the planner: no schema change.** A `ScalarModel` failure row still carries no
  reason. Adding a column is a migration and a revert unit of its own; it is an issue (§3).
- **2026-10-01, the planner: the region properties stay on the potentials.** They become unread.
  Removing them touches six potential classes and `AbstractPotential` for no behavioural gain;
  housekeeping, as an issue (§3).
- **2026-10-01, the user: README §6.1 (b)'s per-bounce `φ_min` is the dense-output minimum.**
  For P3 bounces 1, 2, 8 the quantity compared with the shipped `2.79886e-4`, `3.03650e-4`,
  `3.66368e-4` (± 2e-4 relative) is the minimum of `φ` on the accepted step's interpolant (the
  root of `π`), as `test_d_P3_window` asserts (4e-7, 1e-6, 1e-6 on prompt 01's tree), not `φ`
  at the first accepted step after `π` turns positive (1.06e-4, 2.10e-4, 2.31e-4). The (a)
  first-bounce rows keep the accepted-step definition the prompt wrote. Prompt 04 re-measures
  (b) with the dense-output minimum. Closes
  `[01-bounce-phi-min-at-the-accepted-step-depends-on-step-placement]` (§4).
- **2026-10-01, the user: prompt 02's scope in `Quadrature/supervisors/base.py` widened to
  `IntegrationSupervisor.__exit__`** (option (A) of the implementer's stop), so that its
  traceback print and the `print_tb` import go with `RHS_timer`'s and acceptance 2's grep finds
  nothing. The same ruling approved the state-length `assert` after the loop (the old site had
  gone with prompt 01's fragment loop) and a tightened test (f) (`print_tb` never prints
  "Traceback"). Log 02 Deviations 1–3.

- **2026-10-01, prompt 04: a question for the user, not a ruling.** The prompt's stop condition
  "first bounce disagrees with the probe's by more than `1e-5` in `N`" does not say how a bounce is
  located. By the accepted step after `π` turns positive, β = 0.9, `M = 0.5` differs by 7.9×10⁻⁵
  (the straddling step is 8.5×10⁻⁵ wide) and the other eight rows by ≤ 9.0×10⁻⁶; by the dense-output
  turning point all nine agree to 9.4×10⁻¹⁰ (log 04, Deviations 1). Prompt 04 finished the work and
  committed, as one revert unit, rather than stop; the user rules which measure was meant, as for
  `φ_min` above. Nothing in the code depends on it.
- **2026-10-01, the user: the first bounce is the dense-output turning point.** The question
  above is closed in the way intended when prompt 04 was written. A bounce is located at the root
  of `π` on the accepted step's interpolant, as for `φ_min`. On that measure all nine histories of
  README §6.1 (d) agree with the probe to within `1e-5` in `N`: the largest difference is
  3.5×10⁻⁹ (β = 0.9, `M = 0.5`, log 04's table), not the 9.4×10⁻¹⁰ log 04's prose quotes. The §5
  stop condition is not met and the close-out stands.

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- a §6.1 tolerance that cannot be met at `f = 0.1`;
- a SciPy `Radau` attribute (`f`, `max_step`, `h_abs`, `jac_factor`) that is not available as the
  audit found it;
- a `RuntimeError` site that README §2 (h) does not classify.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Replace the fragment loop with the kinematic-cap step loop](01-kinematic-cap-step-loop.md) | **A**, **J**, **X** (loop), version bump | Opus | ✍️ 2026-10-01 | ✅ 2026-10-01 (with deviations) | see `git log` ("Replace the fragment loop with a kinematic-cap step loop") | [`logs/01-kinematic-cap-step-loop.md`](logs/01-kinematic-cap-step-loop.md) |
| 02 | [Remove the solver fallback and settle the exception taxonomy](02-fallback-and-exceptions.md) | **B**, **C**, **S**, **X** (RHS) | Opus | ✍️ 2026-10-01 | ✅ 2026-10-01 (with deviations) | see `git log` ("Remove the solver fallback and settle the exception taxonomy") | [`logs/02-fallback-and-exceptions.md`](logs/02-fallback-and-exceptions.md) |
| 03 | [Documents and the paper's corrections](03-documents-and-paper-corrections.md) | **D** | Sonnet | ✍️ 2026-10-01 | ✅ 2026-10-01 | see `git log` ("Document the step loop and list the paper's corrections") | [`logs/03-documents-and-paper-corrections.md`](logs/03-documents-and-paper-corrections.md) |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-10-01 | ✅ 2026-10-01 (with deviations) | see `git log` ("Close the integrator-remediation campaign with a handover") | [`logs/04-close-out-verification.md`](logs/04-close-out-verification.md) |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| A | **DEFECT, high** | The L1/L2 regions cap the step at `3e-3 M` and `1e-4 M` e-folds wherever `φ` is inside them: 2 099 582 RHS against 18 916 on the parked P2 window for the same trajectory; 85 fragments and 5 188 284 RHS on the grazing P3 window; the full β = 1.2, M = 0.01 history dies of the 100-fragment `RuntimeError` at `N = 37.165`. The hard reflection at `φ = 0` stalls or runs free for `M ≳ 1e-13`; the shipped scheme fails outright for `1e-13 ≲ M ≲ 1e-8`. Closes `[00-region-scheme-costs-100x-and-storms-fragments]`, `[00-hard-reflection-at-phi-zero-stalls-or-runs-free]`. | 01 | **done 2026-10-01** (log 01). `integrate_scalar_history` is one Radau step loop under the kinematic cap (`f = 0.1`, global `0.1`) with the elastic reflection at `h_floor = 1e-11`, guarded by G1 (`reflects_at_origin`) and G2 (`W ≤ ½π²`); `φ ≤ 0` is a `ComputationFailureError`; one `OdeSolution` is sampled. Events, fragments and regions are gone; the fallback wrapper stays for prompt 02. P1 1 945 / 2 120 / 2 275 RHS at M = 0.5 / 0.01 / 0.001 (17 092 / 26 634 / 26 422 shipped); M = 1e-10 and 4.1e-28 complete with one reflection; P3 38 547 RHS, 51 bounces; P2 18 880; all nine full histories complete with 0 reflections. Stored keys per README §2 (e); label `"Radau+kinematic-cap-stepping0"`; **`VERSION_LABEL` `"2026.5.0"`**. P3 per-bounce `φ_min` meets ± 2e-4 on the dense-output minimum but not at the accepted step (bounces 2, 8: 2.1e-4, 2.3e-4): `[01-bounce-phi-min-at-the-accepted-step-depends-on-step-placement]`. Suites 18 / 57 / 17 |
| J | **DEFECT, high** | SciPy's `num_jac` grows the `ln T_J` perturbation factor by 10 per Jacobian evaluation with no upper clamp; the probe reaches `−9.4×10³⁰⁷`, is substituted by 1 K, and the next probe raises. Two full histories died of it at `N ≈ 38`. Closes `[00-scipy-num-jac-factor-grows-without-bound]`. | 01 | **done 2026-10-01** (log 01). `solver.jac_factor` clamped to `1e-4` after every accepted step; P2 to N = 40 prints no `T_Jordan = 0` (35 without cap and clamp, audit §8); none in the nine full histories |
| X | **DEFECT, medium** | A `ComputationFailureError` raised on a Newton iterate or Jacobian probe ends the solve: from P1 with no cap at `1e-10`, zero accepted steps. The loop treats it as a rejected step (01); `_get_T_Jordan`'s silent 1 K substitution becomes a raise the loop rejects (02). Closes `[00-trial-state-exceptions-abort-the-solve]`. | 01, 02 | **done 2026-10-01** (logs 01, 02). Loop half (01): a `ComputationFailureError` from `solver.step()` halves the step and retries; below `1e-13` e-folds it is the history's failure. RHS half (02): `_get_T_Jordan` raises on `T_J ≤ 0` instead of substituting 1 K. P1 at M = 0.5 to N = 21 is bit-identical to prompt 01's tree (`φ(21) = 0.12203833994225839`, `π(21) = −0.12971527073457434`); the nine full histories identical to log 01 in RHS, steps, bounces and first bounce |
| B | **DEFECT, low** | `solver_list` is walked but `method="Radau"` is a literal; a failing history is integrated four times identically. Closes `[00-solver-fallback-is-not-wired]`. | 02 | **done 2026-10-01** (log 02). `solver_list`, `solver_labels`, `success` and the `while not success` loop are gone; one `try` around the loop and the sampling returns `{"failure": True}` on `ComputationFailureError` |
| C | **DEFECT, medium** | Six `RuntimeError` sites mix bugs, per-history failures and configuration; any of them ends `main.py`. `data.d_logV_dphi` is a latent `AttributeError`. `RHS_timer.__exit__` prints every exception's traceback. Closes `[00-runtime-errors-mix-bugs-and-failures]`. | 02 | **done 2026-10-01** (log 02). One `RuntimeError` left in the integration path (z grid too short); the solution-dimension check is an `assert`; the NaN branch no longer reads `data.d_logV_dphi` and raises `ComputationFailureError`; the failsafe raises `ComputationFailureError`; `RHS_timer.__exit__` and `IntegrationSupervisor.__exit__` print nothing (the latter by the user's ruling) |
| S | **GAP** | At `M ≲ 1e-10` with β ≥ 1.2 the settling bounces double per e-fold; no bounce-following scheme reaches `T_CMB`. A step budget makes it a clean failure. Closes `[00-physical-M-histories-run-for-days-without-a-parking-model]`; the model stays open. | 02 | **done 2026-10-01** (log 02). `StepControl.step_budget = 2_000_000` (planner's default); exceeding it raises `ComputationFailureError` "step budget exhausted: … took n accepted steps (budget b) at N=…, T_J=… GeV, with r reflection(s)" (P2 with budget 50: 51 steps). Suites 18 / 67 / 17 |
| D | documents | The two architecture documents and the paper's `NumericalSection` describe regions, fragments, a fallback and relaxed tolerances the code does not run. Closes `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]`. | 03 | **done 2026-10-01** (log 03). `numerical-strategies.md` §3.5 (new, after §3.4) describes the loop, the cap, the floor and the guarded reflection, the Jacobian clamp, the exception table, the budget and the stored keys; six dated notes in `architecture-summary.md` (at the label, the supervisor, `SolutionFragment`, the procedure, the region properties, the fallback); an "Outcome" subsection at the end of the audit README; `paper-corrections-numerical-section.md` (nine rows, three statements for the paper). Additions only: `--numstat` 184/0, 12/0, 48/0, new file |

---

## 3. Active and unresolved issues

Fourteen opened by the planner on 2026-10-01 from the audit, and one more the same day from the
user's guard G1; prompt 01 closed three (§4) and opened two; prompt 02 closed five (four of its
own and `[01-bounce-phi-min-…]` by the user's ruling) and opened one; prompt 03 closed one; prompt 04 (the close-out) opened and closed none; one more was opened
after the close-out (2026-10-01, `[post-…]`). Ten
are open and none is assigned to this campaign's prompts. Issues opened
by later prompts go here too, with an index row under §1.6 of `.documents/OPEN_ISSUES.md`. Every
measurement below is the audit's, on `b1f64d8`, by the script named in the audit README section
cited, except in the entries opened by prompts 01 and 02 (`[01-…]`, `[02-…]`), which name their
own source.

- **[00-settling-at-physical-M-needs-a-parked-tracking-model]** *(audit §3.7, §9.4)*.
  - **What.** Once the bounce amplitude is far below any scale of interest and the period far
    below the sample spacing, the field is a passenger at `φ_wall(ρ)`, the minimum of the
    effective potential. No code path models that; the integrator follows every bounce.
  - **Impact.** No physical-`M` history with β ≥ 1.2 can be produced until it exists. After
    prompt 02 such a run fails on the budget.
  - **Next step.** The authors: a switch criterion (an amplitude, or a bounce period against the
    sample spacing), the tracking solution `φ = φ_wall(ρ(N))`, `π = dφ_wall/dN`, and what the
    parked field contributes to `ρ_φ`, `p_φ` and the adiabatic diagnostic. Not assigned.
  - **Narrowed (2026-10-01):** prompt 02 (log 02) landed the step budget, so the absence of the
    model is now a clean failure: such a history fails with "step budget exhausted" after
    `2×10⁶` accepted steps and is stored as a failure row, instead of running for days. The model
    itself is still open.
- **[00-stored-samples-alias-the-rebounds]** *(audit §7)*.
  - **What.** The z grid is `ΔN ≈ 0.0092`. Turning points are 0.080 e-folds apart on P2 (8.7
    samples per half-period) and 0.022 on P3 (2.4). The brief's 1–2 samples in the 1–100 MeV
    rebounds and its 0.18 % D/H shift were not re-measured.
  - **Impact.** The BBN ratio spline and the adiabatic stage see an aliased signal in the
    fastest phases.
  - **Next step.** Record every turning point beside the z grid; the new loop sees every accepted
    step, so the cost is nil. It changes what BBN and the adiabatic stage see: the authors'
    decision. Not assigned.
  - **Assigned (2026-10-01):** its BBN half, to the [`science-readiness`](../science-readiness/IMPLEMENTATION_STATE.md) campaign, prompt 05: cell means of
    `H_J²` and `φ` by Gauss–Legendre on the dense output, which BBN reads (the user's choice,
    2026-10-01). The adiabatic half stays here as `[post-adiabatic-Q-reads-aliased-late-samples]`;
    prompt 05 narrows this entry rather than closing it.
  - **Narrowed (2026-10-02):** by `science-readiness` prompt 05 (its log 05). The BBN half does not
    exist in PRyMordial's window: the z grid resolves the bounces above about 100 eV (a median of
    0 half-periods per sample cell from 100 MeV to 100 eV at β = 1.6 and 2, M = 10⁻⁵), and the
    sub-3-keV jumps in the ratio are resolved bounces. Aliasing begins below about 100 eV. The
    cell means were measured and withdrawn by the user's ruling. The adiabatic half stays open as
    `[post-adiabatic-Q-reads-aliased-late-samples]`.
  - **Unassigned (2026-10-02):** `science-readiness` closed (its prompt 09) with this entry
    narrowed, not resolved. It is open here again and assigned to no campaign.
- **[00-atol-does-not-scale-with-phi]** *(audit §6)*.
  - **What.** `atol = 1e-8` on `φ` and `π`, whose wall-side values are `~1e-4 M`. A vector
    `atol = [1e-8 M, 1e-8 M, 1e-8, 1e-8, 1e-8]` halves the first-bounce error at 1 % cost.
  - **Impact.** Tolerance-level.
  - **Next step.** Optional, one line in the parameters object, with the (a) tests as the
    witness. Not assigned.
- **[00-scalarmodel-failure-rows-carry-no-reason]** *(audit §8; read from `ScalarModel.store()`)*.
  - **What.** `{"failure": True}` is all a `ScalarModel` failure stores; `BBNData` has a reason
    column, `ScalarModel` does not. After prompt 02 there will be four distinct reasons (step too
    small, budget, failsafe, `φ ≤ 0`), all printed and none stored.
  - **Impact.** How often each happens in a survey cannot be read from the datastore.
  - **Next step.** A reason column: a schema change and a migration, out of this campaign's
    scope (README §0.4). Not assigned.
  - **Assigned (2026-10-01):** to the [`science-readiness`](../science-readiness/IMPLEMENTATION_STATE.md) campaign, prompt 02 (a `failure_reason` column; the
    science run starts from a fresh datastore, so no migration).
  - **Resolved (2026-10-01):** by the `science-readiness` campaign, prompt 02 (commit "Store why a
    ScalarModel history failed"). A nullable `ScalarModel.failure_reason String(256)` column,
    written from `compute_scalar_model`'s two failure exits and read back through
    `ScalarModel.failure_reason`.
- **[00-region-properties-on-the-potentials-become-unread]** *(after prompt 01)*.
  - **What.** `bounce_region_level{1,2}_boundary`, `…_max_step`, `default_max_step` and
    `hard_reflection_point` are defined on `AbstractPotential` and six potentials and read only
    by the code prompt 01 deletes.
  - **Impact.** Dead interface.
  - **Next step.** Remove them in a housekeeping prompt, with `grep` as the witness. Not
    assigned.
- **[00-declare-reflects-at-origin-for-the-other-potentials]** *(the user's guard G1,
  2026-10-01)*.
  - **What.** Prompt 01 adds `reflects_at_origin` and `log_V_floor` to `AbstractPotential` with
    defaults `False` and `None`, and implements them only on `ExponentialPotential`.
    `InversePowerPotential`, `ReclinerPotential`, `ReflectingPotential`, `StarobinskyPotential`
    keep the defaults, so a history under any of them that reaches the representable-step floor
    fails rather than reflecting.
  - **Impact.** None for production, which uses `ExponentialPotential`. A future run with another
    potential would need the declaration, with the floor-part formula for that potential.
  - **Next step.** Declare the two properties on each potential whose form justifies them, with
    a one-line derivation in the docstring. Not assigned.
- **[00-analytic-jacobian-would-remove-num-jac]** *(audit §9.4)*.
  - **What.** The stiff `(φ, π)` block's Jacobian (`V''` and the kick's `φ`-dependence) is
    available in closed form; the slow components could use finite differences under the loop's
    own control. `jac=` on `Radau` would remove `num_jac` and its factor logic.
  - **Impact.** None measured; the clamp suffices in every probe.
  - **Next step.** Only if Newton failures appear in the science run. Not assigned.

---

- **[01-trial-state-exception-in-radau-start-up-is-not-a-rejection]** *(prompt 01, 2026-10-01;
  read from `scipy/integrate/_ivp/radau.py` 1.17.0)*.
  - **What.** `Radau.__init__` evaluates the RHS at the start state, at `select_initial_step`'s
    probe and at five Jacobian probes. A `ComputationFailureError` there escapes
    `integrate_scalar_history`, at the first construction or at a reflection restart, as the
    history's failure rather than a rejected step.
  - **Impact.** Not seen in any run of prompt 01 (P1–P3, the nine histories, M down to 4.1e-28).
    After a reflection the probe moves `φ` outward.
  - **Next step.** If it is ever seen: retry the construction with an explicit `first_step` at the
    cap. Belongs with prompt 02's taxonomy if taken. Not assigned.
- **[02-negative-E-is-clamped-not-raised-on-trial-states]** *(prompt 02, 2026-10-01; read from
  `ODEPolicy.__call__`, log 02 Observations 1)*.
  - **What.** `E = G − V/(3H²M_P²) < 0` is printed and clamped to `E = 0`, with the `raise`
    commented out; the comment there is undecided whether it is an error or a harmless end-of-
    integration effect. It is the same kind of silent substitution on an unphysical state that
    prompt 02 removed for `T_J ≤ 0`.
  - **Impact.** None measured: 0 "negative value of E" prints on the P1 (M = 0.5, 0.01, 1e-10),
    P2 and P3 windows and in all nine full histories on prompt 02's tree.
  - **Next step.** Decide whether `E < 0` on a trial state should raise (the loop would reject
    the step) or stay a clamp; a one-line change, witnessed by the (a) and (c) tests and the nine
    histories. Not assigned.
- **[post-adiabatic-Q-reads-aliased-late-samples]** *(opened after the close-out, 2026-10-01, at
  the user's request; measured on `1265c75`, `.documents/review-remediation-verification.md`
  §4.9)*.
  - **What.** Below `T_J ≈ 1 keV` the stored z grid (`ΔN = ln 10/250 ≈ 0.0092`) misses most
    bounces, and more of them as `M` falls. The median number of stored samples per half-period
    is 1.3–1.7 at `M = 0.01`, 0.35–0.6 at `1e-3`, and 0.02–0.3 at `1e-5` and `1e-6`, for
    β = 1.2, 2, 3. Under one sample per half-period: 0–25 % of half-periods at `M = 0.01`,
    78–93 % at `1e-3`, 87–99 % at `1e-5` and `1e-6`. At `1e-5` and `1e-6`, `φ` swings by
    14–99 % of its value between turning points there, so the samples read `V''(φ)` and
    `M²_eff` at an effectively random phase of each bounce. The kinetic fraction `π²/6` there is `≤ 4×10⁻⁵`. In
    PRyMordial's window (10 MeV–1 keV) the sampling barely depends on `M` between `0.01` and
    `1e-6`.
  - **Impact.** `compute_adiabatic_values` stores `max |Q|` over every sample, and `Q`'s
    numerator uses the derivative of a spline through `asinh(M²_eff/H²)` on the z grid. Where the
    bounces fall between samples that derivative does not describe the field. Whether that
    segment sets the stored maximum was **not measured**; if it does, the adiabaticity verdict at
    small `M` is set by aliasing rather than by the physics. A sharper form of
    `[00-stored-samples-alias-the-rebounds]` for the adiabatic stage.
  - **Next step.** For the six §4.9 histories and the `M = 0.01`, `1e-3` baselines, find where
    `max |Q|` occurs (`N`, `T_J`), for each `k/H` label. If it is below 1 keV, the remedy is
    either the turning-point sampling of `[00-stored-samples-alias-the-rebounds]` or a
    restricted window for the adiabatic diagnostic; that choice belongs to the authors. Not
    assigned.

## 4. Resolved issues

- **[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]** *(audit §1, §11)*.
  - **What.** `Paper1.tex` `NumericalSection`: outside cap `10⁻²` (code: `inf`), region caps
    `10⁻⁵`/`10⁻⁶` (code: `3e-3 M`, `1e-4 M`), relaxed tolerances for `M ≲ 10⁻³` (not in the
    production path), a BDF → LSODA → DOP853 fallback (never wired). `numerical-strategies.md`
    §2–3 and `architecture-summary.md` describe the fragment loop.
  - **Impact.** A reader of the paper or the documents cannot reproduce what the code does.
  - **Next step.** Prompt 03: dated addenda and a corrections list for the authors.
    **Assigned (2026-10-01):** prompt 03 (D).
  - **Resolved (2026-10-01):** by prompt 03 (log 03). `numerical-strategies.md` §3.5 and six dated
    notes in `architecture-summary.md` describe the loop that replaced the fragments, regions,
    fallback and hard reflection; the audit README ends with an "Outcome" subsection; and
    `.documents/paper-corrections-numerical-section.md` lists nine sentences of `NumericalSection`
    with the measured fact and a suggested replacement beside each, plus three statements the
    paper may carry. `Paper1.tex` is not edited.
- **[00-region-scheme-costs-100x-and-storms-fragments]** *(audit §3.2, §3.3; `p2_parked.py
  regions`, `p3_grazing.py regions`)*.
  - **What.** Inside L2 at M = 0.5 every step is the cap `5e-5`: 2 099 582 RHS over 15 e-folds
    for a trajectory the uncapped Radau reproduces to seven digits in 18 916. On the P3 window the
    rebounds cross the L2 boundary twice each: 85 fragments, 42 entries, 5 188 284 RHS; the full
    history trips the 100-fragment `RuntimeError`.
  - **Impact.** Essentially the whole runtime at M = 0.5; no M ≤ 0.01 history finishes.
  - **Next step.** Prompt 01: the kinematic cap. **Assigned (2026-10-01):** prompt 01 (A).
  - **Resolved (2026-10-01):** by prompt 01 (log 01). No regions, events or fragments remain;
    one Radau instance under the kinematic cap. P2 25 → 40: 18 880 RHS, 19 bounces,
    `φ(40) = 1.9096925e-2`. P3 32.9 → 37.5: 38 547 RHS, 0 restarts, 51 bounces,
    `φ(37.5) = 5.8078198e-4`. Full β = 1.2, M = 0.01 completes in 108 789 RHS; β = 2, 3 at
    M = 0.01 and β = 2, 3 at M = 0.001 complete in 98 133 – 327 046 RHS (log 01 §6.1 (d)).
- **[00-hard-reflection-at-phi-zero-stalls-or-runs-free]** *(audit §3.5, §3.7;
  `p1_sweep.py`, `p_total2.py`, `p_smallM_scan.py regions`)*.
  - **What.** For `M ≳ 1e-13` the event root at `φ = ±1e-15` lies inside the wall. On the `+`
    side Radau fails at once ("Required step size is less than spacing between numbers"); on the
    `−` side the field runs on at `φ < 0` to `T_CMB` with no wall and no event, and would be stored.
    For `M ≲ 1e-13` the same reflection is correct and works. For `1e-13 ≲ M ≲ 1e-8` the shipped
    scheme fails inside L2 (M = 1e-10).
  - **Impact.** A missed reflection at the paper's `M` is a failure row at best and a silently
    wrong history at worst.
  - **Next step.** Prompt 01: the floor-triggered elastic reflection; `φ ≤ 0` is a failure.
    **Assigned (2026-10-01):** prompt 01 (A).
  - **Resolved (2026-10-01):** by prompt 01 (log 01). The hard reflection at `φ = 0` is gone; an
    accepted `φ ≤ 0` raises `ComputationFailureError` (cap disabled from P1 at M = 0.01: raised at
    `N = 20.354`). The elastic reflection fires at the floor rule of README §2 (b), after G1 and
    G2: P1 at M = 1e-10 and 4.1e-28 each complete with one reflection at `φ = 4.70e-11`,
    `φ(21) = 1.18442804e-1` (on `918590e`, M = 1e-10 fails "Required step size is less than
    spacing between numbers"). Largest `W/(½π²)` at any reflection: 1.76e-20.
- **[00-scipy-num-jac-factor-grows-without-bound]** *(audit §8 F2; `p2_parked.py none`,
  `p_full.py` without the clamp)*.
  - **What.** `scipy/integrate/_ivp/common.py` `_dense_num_jac` multiplies a component's factor by
    10 whenever its column is below `EPS^0.75` relative, with only a lower clamp. The `ln T_J`
    column is identically zero at low T. Substituted values grow `−910, −8 627, −85 936, …` to
    `−9.4×10³⁰⁷`.
  - **Impact.** 35 substitutions on P2 without a cap; two full histories dead at `N ≈ 38`. The
    shipped `T_J = 1 K` substitution hides it until the probe is non-finite.
  - **Next step.** Prompt 01: clamp `solver.jac_factor` at `1e-4` after every step (35 → 0 on
    P2, same trajectory). **Assigned (2026-10-01):** prompt 01 (J).
  - **Resolved (2026-10-01):** by prompt 01 (log 01). `solver.jac_factor` is clamped to `1e-4`
    after every accepted step (`StepControl.jacobian_factor_max`). P2 to N = 40 prints no
    `T_Jordan = 0`; none of the nine full histories does, and none has a rejected step.
- **[00-trial-state-exceptions-abort-the-solve]** *(audit §5; `p1_sweep.py a`, `p_total2.py`)*.
  - **What.** `ODEPolicy` and `PotentialDerivativePolicy` raise on `G < 0`, overflow and
    non-finite input, on trial states `solve_ivp` would have rejected. From P1 with no cap at
    `1e-10` the solve dies with zero accepted steps. Two policies for one kind of event: `T_J ≤ 0`
    substitutes, the others raise.
  - **Impact.** Integrable histories recorded as failures, four times over.
  - **Next step.** Prompt 01: the loop rejects the step. Prompt 02: `_get_T_Jordan` raises like
    the others. **Assigned (2026-10-01):** prompts 01, 02 (X).
  - **Narrowed (2026-10-01):** prompt 01 landed the loop half (log 01): a `ComputationFailureError`
    from `solver.step()` halves the step and retries, and below `1e-13` e-folds it is the
    history's failure (three injected failures from P1 at M = 0.5: completes,
    `steps_rejected_by_exception = 3`). Open: the RHS half, prompt 02.
  - **Resolved (2026-10-01):** by prompts 01 and 02 (logs 01, 02). The loop rejects a step on
    which the RHS raises (prompt 01); `_get_T_Jordan` now raises on `T_J ≤ 0` instead of
    substituting 1 K, so the only two policies are "raise" and, for `E < 0`, the clamp recorded
    as `[02-negative-E-is-clamped-not-raised-on-trial-states]`. Three `ComputationFailureError`s
    raised inside `ODEPolicy` from P1 at M = 0.5: rejected, same `φ(21)`, nothing printed by the
    timer (`test_integrator_exceptions.py`). P1 at M = 0.5 to N = 21 is bit-identical to prompt
    01's tree; the nine full histories are identical to log 01.
- **[00-solver-fallback-is-not-wired]** *(audit §4; `git log -S'method="Radau"'`)*.
  - **What.** `method="Radau"` has been a literal since `f67bc3a`; `solver_list` is walked on
    failure and the identical integration repeated.
  - **Impact.** Four times the cost of every failure; a paper sentence that describes nothing.
    `solver_label` is nonetheless correct for stored rows.
  - **Next step.** Prompt 02: delete. **Assigned (2026-10-01):** prompt 02 (B).
  - **Resolved (2026-10-01):** by prompt 02 (log 02). `solver_list`, the `solver_labels` dict of
    `solve_ivp+…` names, `success` and the `while not success` loop are deleted; one `try` around
    the loop and the sampling returns `{"failure": True}`. `test_g_no_fallback` (`ast`): no
    `solver_list`, no `BDF`/`LSODA`/`DOP853` string, no `while` in `compute_scalar_model`.
- **[00-runtime-errors-mix-bugs-and-failures]** *(audit §1, §5; read from the code)*.
  - **What.** `RuntimeError` at six sites in `compute_scalar_model` (`:670, 678, 711, 731, 819,
    851` on `b1f64d8`); none is caught by `RayWorkPool` or `main.py`. `ODERHS`'s NaN branch reads
    `data.d_logV_dphi`, which `ODEPolicyData` lacks. `RHS_timer.__exit__` prints every exception.
  - **Impact.** One numerical failure of one history ends the run; the diagnostic branch would
    raise the wrong type; multi-megabyte logs.
  - **Next step.** Prompt 02: the table in README §2 (h). **Assigned (2026-10-01):** prompt 02 (C).
  - **Resolved (2026-10-01):** by prompts 01 and 02 (logs 01, 02). Prompt 01 removed the
    fragment-count, multiple-event, unknown-event, `status != 1` and state-length sites with the
    fragment loop and made the failsafe a `ComputationFailureError`. Prompt 02: an `assert` on
    the solution's dimension after the loop; the NaN branch no longer reads `data.d_logV_dphi`
    and raises `ComputationFailureError` (`AttributeError` on `fc97233`); `RHS_timer.__exit__`
    and, by the user's ruling, `IntegrationSupervisor.__exit__` print nothing. One `RuntimeError`
    remains in the integration path, the z grid too short (configuration); counted by `ast` in
    `test_g_one_runtime_error_in_the_integration_path`.
- **[00-physical-M-histories-run-for-days-without-a-parking-model]** *(audit §3.7;
  `p_full.py … kin reflect` at `M = 4.1e-28`, `1e-10`, `1e-15`)*.
  - **What.** For β = 1.2, 2, 3 the steps per e-fold grow ×2–2.5 per e-fold from `N ≈ 37` (β ≤ 2)
    or `41` (β = 3); 62 135 steps for `N = 40 → 41` at β = 1.2. Extrapolated to `T_CMB`: `10⁷–10⁸`
    steps. The shipped scheme stops the same history at its 100th fragment (`N = 39.04`, β = 2).
  - **Impact.** A physical-`M` survey would occupy a machine indefinitely.
  - **Next step.** Prompt 02: a step budget as a clean `ComputationFailureError`. The model itself
    is the next entry. **Assigned (2026-10-01):** prompt 02 (S).
  - **Resolved (2026-10-01), as a clean failure:** by prompt 02 (log 02).
    `StepControl.step_budget = 2_000_000` accepted steps (the planner's default, README §0.2);
    exceeding it raises `ComputationFailureError` "step budget exhausted: … took n accepted steps
    (budget b) at N=…, T_J=… GeV, with r reflection(s)", which `compute_scalar_model` turns into
    a failure row (P2 with budget 50: 51 steps; `compute_scalar_model` smoke run with budget 50:
    `{"failure": True}`). The model itself stays open as
    `[00-settling-at-physical-M-needs-a-parked-tracking-model]`.
- **[01-bounce-phi-min-at-the-accepted-step-depends-on-step-placement]** *(prompt 01, 2026-10-01;
  `ComputeTargets/tests/test_kinematic_cap_loop.py` helpers, log 01 Deviations 5)*.
  - **What.** README §6.1 (b) asks for P3 bounces 1, 2, 8 to match the shipped `φ_min`
    (2.79886e-4, 3.03650e-4, 3.66368e-4) to ± 2e-4. Taken as `φ` at the first accepted step after
    `π` turns positive (the audit harness's measure), the loop at `f = 0.1` gives 1.06e-4,
    2.10e-4, 2.31e-4; taken as the minimum of `φ` on the dense output, 4e-7, 1e-6, 1e-6. The
    shipped values are effectively true minima (steps of `1e-6`); the accepted-step value carries
    the turning-point step's placement. At `f = 0.02` the accepted-step values are within 1.9e-4.
  - **Impact.** None on the trajectory. The (b) test asserts the dense-output minimum; the row as
    the harness measures it is missed, which is why prompt 01 is `COMPLETE WITH DEVIATIONS`.
  - **Next step.** The orchestrator or the user confirms which measure §6.1 (b) means; prompt 04
    re-measures with the one chosen. Not assigned.
  - **Resolved (2026-10-01):** by the user's ruling of 2026-10-01 (Decisions): README §6.1 (b)'s
    per-bounce `φ_min` is the dense-output minimum, which `test_d_P3_window` already asserts and
    which meets ± 2e-4 (4e-7, 1e-6, 1e-6 on prompt 01's tree). No code change; prompt 04
    re-measures (b) with that measure. Recorded on the board by prompt 02.
