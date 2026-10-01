# Integrator remediation campaign — implementation state

**Last updated:** 2026-10-01 · **Status: PLANNED — 0 of 4 landed.** Planned on 2026-10-01 against
`main` at `2b89022`, from the audit at
[`.documents/integrator-audit-2026-09-30/README.md`](../../.documents/integrator-audit-2026-09-30/README.md).
**Target branch** `integrator-remediation`, to be cut from `2b89022`; planning and orchestration
commits land on it.
**The rule once prompt 01 lands: every store made before 2026.5.0 is invalid.** `VERSION_LABEL` is
`"2026.4.0"` until then.

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

- **2026-10-01, the planner (awaiting the user; README §0.2):** `f = 0.1`; `h_floor = 1e-11`
  with the elastic reflection at the floor; delete the fallback; tolerances stay `1e-8`; a step
  budget of `2×10⁶` accepted steps as a clean failure; one bump to `"2026.5.0"` in prompt 01. Each
  is a one-line constant or a one-paragraph deletion; the user may overrule any before
  orchestration starts, and the outcome is recorded here.
- **2026-10-01, the planner: the parked-tracking model is out of scope.** It decides what the
  field *is* once its bounces are unresolvable, and what it contributes to BBN and the adiabatic
  stage; that is the authors' physics, not step control (audit §3.7, §9.4). The budget makes its
  absence a clean failure (prompt 02).
- **2026-10-01, the planner: no schema change.** A `ScalarModel` failure row still carries no
  reason. Adding a column is a migration and a revert unit of its own; it is an issue (§3).
- **2026-10-01, the planner: the region properties stay on the potentials.** They become unread.
  Removing them touches six potential classes and `AbstractPotential` for no behavioural gain;
  housekeeping, as an issue (§3).

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- a §6.1 tolerance that cannot be met at `f = 0.1`;
- a SciPy `Radau` attribute (`f`, `max_step`, `h_abs`, `jac_factor`) that is not available as the
  audit found it;
- a `RuntimeError` site that README §2 (h) does not classify.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Replace the fragment loop with the kinematic-cap step loop](01-kinematic-cap-step-loop.md) | **A**, **J**, **X** (loop), version bump | Opus | ✍️ 2026-10-01 | — | — | — |
| 02 | [Remove the solver fallback and settle the exception taxonomy](02-fallback-and-exceptions.md) | **B**, **C**, **S**, **X** (RHS) | Opus | ✍️ 2026-10-01 | — | — | — |
| 03 | [Documents and the paper's corrections](03-documents-and-paper-corrections.md) | **D** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-10-01 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| A | **DEFECT, high** | The L1/L2 regions cap the step at `3e-3 M` and `1e-4 M` e-folds wherever `φ` is inside them: 2 099 582 RHS against 18 916 on the parked P2 window for the same trajectory; 85 fragments and 5 188 284 RHS on the grazing P3 window; the full β = 1.2, M = 0.01 history dies of the 100-fragment `RuntimeError` at `N = 37.165`. The hard reflection at `φ = 0` stalls or runs free for `M ≳ 1e-13`; the shipped scheme fails outright for `1e-13 ≲ M ≲ 1e-8`. Closes `[00-region-scheme-costs-100x-and-storms-fragments]`, `[00-hard-reflection-at-phi-zero-stalls-or-runs-free]`. | 01 | planned |
| J | **DEFECT, high** | SciPy's `num_jac` grows the `ln T_J` perturbation factor by 10 per Jacobian evaluation with no upper clamp; the probe reaches `−9.4×10³⁰⁷`, is substituted by 1 K, and the next probe raises. Two full histories died of it at `N ≈ 38`. Closes `[00-scipy-num-jac-factor-grows-without-bound]`. | 01 | planned |
| X | **DEFECT, medium** | A `ComputationFailureError` raised on a Newton iterate or Jacobian probe ends the solve: from P1 with no cap at `1e-10`, zero accepted steps. The loop treats it as a rejected step (01); `_get_T_Jordan`'s silent 1 K substitution becomes a raise the loop rejects (02). Closes `[00-trial-state-exceptions-abort-the-solve]`. | 01, 02 | planned |
| B | **DEFECT, low** | `solver_list` is walked but `method="Radau"` is a literal; a failing history is integrated four times identically. Closes `[00-solver-fallback-is-not-wired]`. | 02 | planned |
| C | **DEFECT, medium** | Six `RuntimeError` sites mix bugs, per-history failures and configuration; any of them ends `main.py`. `data.d_logV_dphi` is a latent `AttributeError`. `RHS_timer.__exit__` prints every exception's traceback. Closes `[00-runtime-errors-mix-bugs-and-failures]`. | 02 | planned |
| S | **GAP** | At `M ≲ 1e-10` with β ≥ 1.2 the settling bounces double per e-fold; no bounce-following scheme reaches `T_CMB`. A step budget makes it a clean failure. Closes `[00-physical-M-histories-run-for-days-without-a-parking-model]`; the model stays open. | 02 | planned |
| D | documents | The two architecture documents and the paper's `NumericalSection` describe regions, fragments, a fallback and relaxed tolerances the code does not run. Closes `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]`. | 03 | planned |

---

## 3. Active and unresolved issues

Fourteen opened by the planner on 2026-10-01 from the audit. Eight are assigned to this campaign's
prompts (the first eight); six are open and unassigned. Issues opened by later prompts go here
too, with an index row under §1.6 of `.documents/OPEN_ISSUES.md`. Every measurement below is the
audit's, on `b1f64d8`, by the script named in the audit README section cited.

- **[00-region-scheme-costs-100x-and-storms-fragments]** *(audit §3.2, §3.3; `p2_parked.py
  regions`, `p3_grazing.py regions`)*.
  - **What.** Inside L2 at M = 0.5 every step is the cap `5e-5`: 2 099 582 RHS over 15 e-folds
    for a trajectory the uncapped Radau reproduces to seven digits in 18 916. On the P3 window the
    rebounds cross the L2 boundary twice each: 85 fragments, 42 entries, 5 188 284 RHS; the full
    history trips the 100-fragment `RuntimeError`.
  - **Impact.** Essentially the whole runtime at M = 0.5; no M ≤ 0.01 history finishes.
  - **Next step.** Prompt 01: the kinematic cap. **Assigned (2026-10-01):** prompt 01 (A).
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
- **[00-trial-state-exceptions-abort-the-solve]** *(audit §5; `p1_sweep.py a`, `p_total2.py`)*.
  - **What.** `ODEPolicy` and `PotentialDerivativePolicy` raise on `G < 0`, overflow and
    non-finite input, on trial states `solve_ivp` would have rejected. From P1 with no cap at
    `1e-10` the solve dies with zero accepted steps. Two policies for one kind of event: `T_J ≤ 0`
    substitutes, the others raise.
  - **Impact.** Integrable histories recorded as failures, four times over.
  - **Next step.** Prompt 01: the loop rejects the step. Prompt 02: `_get_T_Jordan` raises like
    the others. **Assigned (2026-10-01):** prompts 01, 02 (X).
- **[00-solver-fallback-is-not-wired]** *(audit §4; `git log -S'method="Radau"'`)*.
  - **What.** `method="Radau"` has been a literal since `f67bc3a`; `solver_list` is walked on
    failure and the identical integration repeated.
  - **Impact.** Four times the cost of every failure; a paper sentence that describes nothing.
    `solver_label` is nonetheless correct for stored rows.
  - **Next step.** Prompt 02: delete. **Assigned (2026-10-01):** prompt 02 (B).
- **[00-runtime-errors-mix-bugs-and-failures]** *(audit §1, §5; read from the code)*.
  - **What.** `RuntimeError` at six sites in `compute_scalar_model` (`:670, 678, 711, 731, 819,
    851` on `b1f64d8`); none is caught by `RayWorkPool` or `main.py`. `ODERHS`'s NaN branch reads
    `data.d_logV_dphi`, which `ODEPolicyData` lacks. `RHS_timer.__exit__` prints every exception.
  - **Impact.** One numerical failure of one history ends the run; the diagnostic branch would
    raise the wrong type; multi-megabyte logs.
  - **Next step.** Prompt 02: the table in README §2 (h). **Assigned (2026-10-01):** prompt 02 (C).
- **[00-physical-M-histories-run-for-days-without-a-parking-model]** *(audit §3.7;
  `p_full.py … kin reflect` at `M = 4.1e-28`, `1e-10`, `1e-15`)*.
  - **What.** For β = 1.2, 2, 3 the steps per e-fold grow ×2–2.5 per e-fold from `N ≈ 37` (β ≤ 2)
    or `41` (β = 3); 62 135 steps for `N = 40 → 41` at β = 1.2. Extrapolated to `T_CMB`: `10⁷–10⁸`
    steps. The shipped scheme stops the same history at its 100th fragment (`N = 39.04`, β = 2).
  - **Impact.** A physical-`M` survey would occupy a machine indefinitely.
  - **Next step.** Prompt 02: a step budget as a clean `ComputationFailureError`. The model itself
    is the next entry. **Assigned (2026-10-01):** prompt 02 (S).
- **[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]** *(audit §1, §11)*.
  - **What.** `Paper1.tex` `NumericalSection`: outside cap `10⁻²` (code: `inf`), region caps
    `10⁻⁵`/`10⁻⁶` (code: `3e-3 M`, `1e-4 M`), relaxed tolerances for `M ≲ 10⁻³` (not in the
    production path), a BDF → LSODA → DOP853 fallback (never wired). `numerical-strategies.md`
    §2–3 and `architecture-summary.md` describe the fragment loop.
  - **Impact.** A reader of the paper or the documents cannot reproduce what the code does.
  - **Next step.** Prompt 03: dated addenda and a corrections list for the authors.
    **Assigned (2026-10-01):** prompt 03 (D).
- **[00-settling-at-physical-M-needs-a-parked-tracking-model]** *(audit §3.7, §9.4)*.
  - **What.** Once the bounce amplitude is far below any scale of interest and the period far
    below the sample spacing, the field is a passenger at `φ_wall(ρ)`, the minimum of the
    effective potential. No code path models that; the integrator follows every bounce.
  - **Impact.** No physical-`M` history with β ≥ 1.2 can be produced until it exists. After
    prompt 02 such a run fails on the budget.
  - **Next step.** The authors: a switch criterion (an amplitude, or a bounce period against the
    sample spacing), the tracking solution `φ = φ_wall(ρ(N))`, `π = dφ_wall/dN`, and what the
    parked field contributes to `ρ_φ`, `p_φ` and the adiabatic diagnostic. Not assigned.
- **[00-stored-samples-alias-the-rebounds]** *(audit §7)*.
  - **What.** The z grid is `ΔN ≈ 0.0092`. Turning points are 0.080 e-folds apart on P2 (8.7
    samples per half-period) and 0.022 on P3 (2.4). The brief's 1–2 samples in the 1–100 MeV
    rebounds and its 0.18 % D/H shift were not re-measured.
  - **Impact.** The BBN ratio spline and the adiabatic stage see an aliased signal in the
    fastest phases.
  - **Next step.** Record every turning point beside the z grid; the new loop sees every accepted
    step, so the cost is nil. It changes what BBN and the adiabatic stage see: the authors'
    decision. Not assigned.
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
- **[00-region-properties-on-the-potentials-become-unread]** *(after prompt 01)*.
  - **What.** `bounce_region_level{1,2}_boundary`, `…_max_step`, `default_max_step` and
    `hard_reflection_point` are defined on `AbstractPotential` and six potentials and read only
    by the code prompt 01 deletes.
  - **Impact.** Dead interface.
  - **Next step.** Remove them in a housekeeping prompt, with `grep` as the witness. Not
    assigned.
- **[00-analytic-jacobian-would-remove-num-jac]** *(audit §9.4)*.
  - **What.** The stiff `(φ, π)` block's Jacobian (`V''` and the kick's `φ`-dependence) is
    available in closed form; the slow components could use finite differences under the loop's
    own control. `jac=` on `Radau` would remove `num_jac` and its factor logic.
  - **Impact.** None measured; the clamp suffices in every probe.
  - **Next step.** Only if Newton failures appear in the science run. Not assigned.

---

## 4. Resolved issues

None yet.
