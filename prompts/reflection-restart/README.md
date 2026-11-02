# Campaign — reflection-restart

**Written:** 2026-11-02 on branch `reflection-restart`, cut from `main` at `cc7fe75`, by Claude Opus 5.5, at
the user's request: "open a campaign to test the issue and its proposed fixes, and then to implement one
of them", and, if possible, to determine whether the `2026.8.0` science run must be regenerated "without
actually rebuilding all of it". U1–U5 ruled by the user the same day; U6 (the fix) waits on prompts 01
and 02 (§6.1).

**Re-planned 2026-11-02, after 01, at the user's word (§6.3).** `quanta-diagnostics` (planned the same day,
`296be00`) rebuilds the whole store to capture data that cannot be backfilled, and the user has ruled that the
rebuild of record waits for it. A full rebuild is therefore guaranteed. Prompt 02 (the audit of the 2026.8.0
store) is **withdrawn**, since it would only decide which histories to regenerate. 04 shrinks to the 42
failures recomputed, the close and the handover. U6 can be ruled without 02. The text below is the plan as
amended; the passages that no longer hold are marked.

It takes `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]`
(`inelastic-reflection` board §3; `.documents/OPEN_ISSUES.md`), which the user said on 2026-11-02 they
would take to a campaign of its own.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the diagnosis,
[`.documents/post-reflection-step-2026-11-02/`](../../.documents/post-reflection-step-2026-11-02/README.md)
(`626b04f`), with its scripts and `out/`; the issue's board entry; `integrator-remediation` README §2
(a)–(c) (the kinematic cap and the floor rule); `inelastic-reflection` README §8 (the drain, parking,
release); the run record `science-runs/2026-11-01-science-2026.8.0/README.md`.

## 0. Why this campaign exists

### 0.1 The defect (from the diagnosis; not re-measured by the planner)

`integrate_scalar_history` starts a new Radau instance (`new_solver`, `ComputeTargets/ScalarHistory.py:1635`)
at four places: the start (`:1646`), after a reflection (`:2551`), after a drain (`:2700`) and after a
release (`:2465`). Before every step the kinematic cap (`:2553-2567`) sets

    cap = min(global_max_step, f·φ/|π| if π < 0, √(2fφ/(−π′)) if −π′ > 0), never below h_floor.

Both terms limit **inward** motion. After a floor reflection close to the wall, π > 0 and the wall's push
beats the matter pull (−π′ < 0), so neither applies and the cap is `global_max_step` = 0.1. Usually
Radau's error test then cuts the first step to ≤ 4×10⁻⁶ e-folds. In the three reproduced failures it
instead accepted one step of h = 1.6–6.4×10⁻⁴ spanning h·ω ≈ 10³ radians of the local oscillation, whose
endpoint is wrong (the field stops near the wall instead of flying outward) and whose interpolant swings
through φ < 0. At α > 0 the gate samples m_eff² on that interpolant and fails the history.

What the diagnosis left open, and this campaign must settle before a fix is chosen:
- **why Radau's error test accepts the bad step** (an under-reading of the L-stable error estimate at
  h·ω ≫ 1 is suspected, not tested). If it is that, only the first step being bounded may not be enough:
  a later step that Radau grows to the same size could be bad too;
- **whether successful histories hold a bad step** that did not reach φ < 0. That would stop the field
  near the wall, a spurious early parking, and pass silently; at α = 0 the gate does not run, so any
  would. Six of nine scanned are clean; the other 3,600-odd reflecting histories are unscanned.
  *(2026-11-02, after 01: under the signature as the user ruled it (§6.3), none of the 54 stored successes 01
  re-integrated holds a bad step; the rest are not audited, since the whole store is rebuilt.)*

### 0.2 What the 2026.8.0 store says about exposure (planner, read-only, 2026-11-02)

From `var/datastores/science-2026.8.0` (successes only, per M; `number_reflections` from `extra_data`,
`injections` and `compute_time` columns):

| M / M_P | block | successes | mean reflections | mean injections | Σ compute_time | max |
|---|---|---|---|---|---|---|
| 10⁻³³ | B | 200 | 106 | 106 | 0.8 h | 113 s |
| 4×10⁻³¹ | A, A2 | 1924 | 656 | 656 | 35.6 h | 741 s |
| 10⁻³⁰ | B | 201 | 108 | 108 | 0.8 h | 97 s |
| 10⁻¹⁵, 10⁻¹³, 10⁻¹² | C | 1211 | 103–108 | 103–110 | 5.3 h | 90 s |
| 10⁻³, 0.1, 0.5 | D | 182 | **0** | **0** | 0.35 h | 107 s |

- **Block D** (natural mode, α = 0) has no reflection and no injection, so no restart with π > 0. A fix
  that acts only at restarts leaves it bitwise unchanged; a fix that caps outward motion everywhere does
  not. So does Figure 6a.
- **Re-integrating every reflecting success** at its stored normalization costs about the stored
  compute time, ≈ 43 CPU-hours, ≈ 4.5–5 h on the 10-core machine; block C alone ≈ 35–40 min.
- The golden histories (`golden_history_05.json`, `golden_history_06_tuned.json`) are at α = 0 and M = 10⁻³
  and record **0 reflections**. They constrain what a fix may move but do not exercise it.

### 0.3 The proposed fixes (diagnosis §6; not ruled)

- **F1 — cap outward motion too**: h ≤ f·φ/|π| for either sign of π. Symmetric with the existing rule.
  Acts everywhere π > 0, so it moves the golden histories and block D. About ln(φ_end/φ_start)/ln(1 + f)
  ≈ 160 steps per outward flight; × 656 reflections at 4×10⁻³¹ this could be ~10⁵ extra steps per history.
- **F2 — cap on the local stiffness**: h·√|∂π′/∂φ| ≤ c, from the Jacobian Radau holds. Acts wherever the
  oscillation is fast; may forfeit Radau's large steps over stiff stretches.
- **F3 — a first step for the restarted solver**: `first_step` from the cap at the restart (the inward cap
  at the reflection, which the floor rule has just found below h_floor; f·φ/|π| at a drain or a release),
  then Radau grows it. Acts only at restarts with π > 0: golden histories and block D bitwise.
- **F4 (planner) — verify and retry**: after an accepted step that began outward, if its interpolant
  falls below (1 − f)·φ_start or π_end/π_start < ½, discard the step and retake it from the same state
  with h halved. A guard against the symptom, not the cause; listed so that the probe can say whether
  F1–F3 are needed at all or whether a guard is enough on top of one of them.

## 1. Scope

**In scope:**
- **Measure** the fixes (§2, prompt 01) on the reproduced failures, the clean histories the diagnosis
  scanned, the golden histories, and a cost sample at each M of the run; and establish why the bad step
  is accepted.
- ~~**Audit the 2026.8.0 store** (prompt 02)~~ — withdrawn 2026-11-02 (§6.3): the whole store is rebuilt.
- **Implement** the fix the user rules (prompt 03), with tests, a reflecting golden history, the labels,
  and the documents.
- **The close** (prompt 04): the 42 failures recomputed with the fix; the issue closed; the handover,
  which says the whole store is rebuilt.

**Out of scope:**
- **Rebuilding the run.** The rebuild of record is the user's, on the user's word, recorded in
  `science-runs/` (§7). It waits for `quanta-diagnostics` 07, so that one build carries both campaigns'
  changes.
- A verdict per block or per figure on 2026.8.0, and a comparison of its observables with the fixed
  code's beyond what 01 measured: withdrawn with 02 (§6.3).
- The other failures of the run (classical turning point at 4×10⁻³¹, normalization and overflow at
  M = 0.5, the PRyMordial and effective-minimum failures below the surfing threshold).
- The parking cut's ε, the gate's sampling (`observe_turning_points`' ten samples per step), the
  floor h_floor and the drain rule: unchanged unless a ruling says otherwise.
- The packages, SGK, SI; `thirdparty/`; `PRyM/`.
- Records: the diagnosis folder, the run record and other campaigns' boards stay as written
  (invariant 6); they gain dated notes where this campaign adds to them.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-measure-the-fixes.md` | a scanner tool (every step that starts outward, flagged by signature) with tests; F1–F4 behind a probe-only switch, measured on the three failures, the six clean histories, the golden histories and a cost sample; why Radau accepts the bad step | Opus |
| ~~02~~ | ~~`02-audit-the-2026.8.0-store.md`~~ | **withdrawn 2026-11-02** (§6.3): the whole store is rebuilt, so the audit decides nothing | — |
| — | **ruling** | the user chooses the fix from 01 (U6) | — |
| 03 | `03-the-fix.md` | the ruled fix in `integrate_scalar_history`, the probe switch removed; tests; a reflecting golden history; `VERSION_LABEL` and stepper label; documents | Opus |
| 04 | `04-close.md` | the 42 recomputed with the fix through the tool route; the issue closed; the handover (the whole store rebuilt, carried by `quanta-diagnostics`' rebuild of record; since 2026-11-02 `one-loop-drain` 08's, §6.4); the fixed-α turning-point failures of 2197 and 2188 investigated (§6.4); closes the campaign | Opus |

Written one at a time, each after the previous one lands and is reviewed (the user's practice in
`inelastic-reflection`).

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Signature** | the scanner reproduces the diagnosis's three bad steps (h, h·ω, π_end/π_start to its printed digits) and its 845-step count on the nine histories | 01 |
| **Measure** | for each of F1–F4: the three failures' outcome, the count of signature steps over **all** outward steps (not only first ones), golden digits moved or bitwise, accepted steps and wall-clock against today at each M sampled; and a stated, tested account of why Radau accepts the bad step | 01 |
| ~~**Audit**~~ | withdrawn with 02 (§6.3) | — |
| **Fix** | the three failures succeed; no signature step (as ruled, §6.3) in the nine histories; the golden histories as the ruling predicts (bitwise for a restart-only fix, re-pinned with a stated reason otherwise); the new reflecting golden pinned | 03 |
| **Close** | the 42 recomputed and their outcome stated; the issue closed; the handover written | 04 |

> **Note (2026-11-02, after 01).** The Signature gate's "845-step count" repeats a slip in the diagnosis.
> Its nine per-history counts sum to **835**, and that is the count the gate was held to (orchestrator note
> 01, ruling 1; verification §1.2). The diagnosis README carries the same note.

## 3. Stores

- 01 and 03 open none but temporary SQLite stores in `tempfile` directories; the histories they run go
  through `tools/history_and_bbn.py`'s path, as the diagnosis did, with no Ray.
- ~~02~~ (withdrawn).
- 04 reads the 2026.8.0 store read-only for the 42 failures' inputs, and runs them through the tool route
  with no store written (§6.3, replacing U5).

## 4. The interfaces

- **01 → the ruling.** 01 says what each fix costs and catches; the user rules the fix (U6).
- **03 → 04.** The fixed tree and its labels.
- **03 → `quanta-diagnostics`.** That campaign's prompts start on the fixed tree after 03 lands (its U1).
- **To the run.** 04's handover goes into the run record as a dated note (`science-runs/…/README.md`: the
  2026.8.0 store is superseded by the rebuild of record) and into `.documents/review-remediation-verification.md`
  as a handover addendum (§4.19; §4.18 is `inelastic-reflection`'s).

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", and:
- **No production store is written.** The run's stores are read-only throughout.
- **The probe switch is not a production option.** F1–F4 exist in 01 only behind a keyword of
  `integrate_scalar_history` (default today's behaviour, bitwise), or as patches in the probe's own
  process; 03 removes what was not chosen.
- **Values do not move in 01.** The golden histories pass bitwise there. In 03 they move only as the
  ruling predicts.
- **Long runs are detached and recorded** (`CLAUDE.md`, "Long-running jobs"): 04's, if it is long, is
  launched detached with its output in `var/`, and verified once.
- **Measurements, not bounds.** Integrator differences are recorded as measured; a threshold enters
  only where the user rules one.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 For the user to rule (U1–U6)

**Ruled by the user, 2026-11-02:** U1 "fine"; U2 "F1 through F4"; U3 "I'd rather not have bad data in
the datastore. If any stored successful trajectories take a bad step, I'd prefer to regenerate them
whether or not it significantly shifts their Y_p, D/H or Delta N_eff."; U4 "yes, A, A2, B, C in full. No
need for D if it cannot be affected."; U5 (prompt 04's recomputation) "that seems adequate"; U6 "fine"
(ruled after 01 and 02, as proposed). "The defaults in P1 to P10 are fine." So:
- **U3 as ruled.** Part (a) decides: **every stored success that 02 finds with a bad step is
  regenerated**, whatever 04 measures for its observables. Part (b) is still measured in 04, as the
  record of what the fix moves in a clean history, but it does not excuse a bad one. Block C is
  regenerated in any case (§7). How a regeneration is made (the affected histories, or their whole block,
  under `2026.9.0`) is the user's at the verdict.
- **U4 as ruled.** A, A2, B and C in full; D is not scanned. 02 records from the store that D has no
  reflection and no injection, which is why it cannot be affected by a restart-only fix; if the ruled fix
  (U6) acts outside restarts (F1, F2), 04 says what that means for D.

*(2026-11-02, after 01: U3, U4 and U5 lapse, and U6 is ruled from 01 alone; §6.3.)*

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name and branch | `reflection-restart`, on a branch of that name cut from `main` at `cc7fe75` (after this plan is committed there or on the branch) | another name |
| **U2** — the fixes measured | F1, F2, F3 as the diagnosis states them, and F4 (the planner's guard) | F1–F3 only |
| **U3** — the regeneration question, in principle | **two-part test**: (a) 02 finds which stored successes took a bad step; (b) 04 measures how far the chosen fix moves a clean history's observables. If (a) finds none and (b) is below what the figures resolve, the figures made from A, A2, B and D stand, and only the histories that must change are rebuilt (§7) | rebuild every block that reflects (A, A2, B, C) under the new label regardless |
| **U4** — 02's scope | **block C in full** (1211 successes, ≈ 40 min) and **A, A2, B in full** (≈ 4–5 h, detached, ten workers by `multiprocessing`, no Ray); D only checked to have no restart with π > 0 (from the store) | C in full and a stratified ~25 % of A, A2 and B (≈ 1.5 h); or C only |
| **U5** — 04's recomputation | the 42 failures, the 42 histories of the reflection store's grid, and a stratified sample of ~100 clean histories across A, A2, B, C (every M, α and the β regions), each at its stored normalization and re-tuned, compared with the store; written to `var/datastores/reflection-restart-04` | a larger sample, or block C in full under the new label |
| **U6** — the fix | **after 01 and 02**: the user rules one of F1–F4, or a combination, from their measurements. The planner's prior, to be tested: F3 alone is the smallest change (restarts only; D and the goldens bitwise) but may not stop a later grown step; F3 + F4 if 01 shows that | — |

### 6.2 Proposed by the planner (P1–P10)

Accepted as proposed unless the user rules otherwise.
- **P1 — the signature.** A step is flagged if it begins with π > 0 and either π_end/π_start < ½ with no
  sign change of π on its interpolant, or its interpolant falls below (1 − f)·φ_start; h·ω (ω from
  √|J[1,0]| of the Jacobian Radau holds) is recorded for each, not used as a flag. The diagnosis's
  bad steps have π ratios 7–9×10⁻⁵ and its good ones ≥ 0.99992, so the threshold is not delicate; 01
  states the distribution it finds.
- **P2 — the scanner** is a tool, `tools/outward_step_scan.py`, with tests in `tools/tests/`. It runs one
  history through `integrate_history` as `tools/history_and_bbn.py` does, either tuned or at a given
  normalization, with a step hook rather than a patch of SciPy where the code allows (01 decides; a
  patch confined to the tool's process is acceptable). No BBN.
- **P3 — bitwise reproduction** (02; lapsed with it, §6.3; 01 checked it on 54 histories): a stored success re-integrated at its stored normalization with the
  run's tree must give its stored samples bitwise, compared through the store's `ScalarHistoryValue`
  rows; that it does is what makes the scan's verdict about the store and not about a different
  integration. The tree is `main` before 03. Since the run's first tree (`75208cd`, blocks A–C) the
  computing code has changed only in `CosmologyConcepts/Models/AbstractModel.py` (`745c8ee`, the
  overflow refusal in `effective_minimum`, in D's resume tree `eecaf2e`), which should not move a value
  that did not overflow; the bitwise check is what confirms it. 02 states the diff again at launch.
- **P4 — why Radau accepts** (01): re-take the bad step from its stored start and compare Radau's error
  estimate (`error_norm`) with the true error from the resolved re-integration, at h from the good range
  to the bad; say whether the under-reading grows with h·ω. Bounded at a few hours of agent time; if
  unsettled, recorded as open with what was measured.
- **P5 — a reflecting golden history** (03): β = 1.5, M = 10⁻¹², α = 0.05 (the diagnosis's first failure),
  pinned after the fix in `golden_history.py`'s form, as `golden_history_rr03.json`. The two golden
  histories at α = 0 stay.
- **P6 — labels.** The fix changes the values of every history that reflects: `VERSION_LABEL` moves to
  `2026.9.0` and `SCALAR_HISTORY_STEPPER_LABEL` to `Radau+kinematic-cap-stepping1`. Whether
  `FINGERPRINT_FORMAT` moves is decided by 03 from what the fingerprint covers. A `2026.8.0` store stays
  readable.
- **P7 — the drain and the release.** Whatever is ruled applies at all three restarts with π > 0
  (`:2465`, `:2551`, `:2700`), not only after reflections.
- **P8 — tests.** Each fix's unit test is shown to catch its breakage: a state from the diagnosis's
  trace (the reflected state of §3 step 3) restarted with the fix gives no signature step and π of the
  resolved re-integration's sign over the next steps. In `ComputeTargets/tests/`, with a docstring
  stating its runtime.
- **P9 — documents.** `.documents/reflection-restart-verification.md` (new) holds 01 and 04's
  measurements (02 withdrawn); `numerical-strategies.md` gets a dated note on the cap and restart; `architecture-summary.md`
  if the stepper's interface changes. The diagnosis folder gets a dated pointer, nothing else.
- **P10 — issues.** The issue is assigned to this campaign when the plan is committed (index §1, the
  board's **Assigned** line) and closed by 04. Anything else found is recorded and opened, not fixed.

### 6.3 Re-planned after 01 (2026-11-02)

**Ruled by the user, 2026-11-02, after 01's review:** "P1 only seems to need to fire on the first step of a
restarted Radau. That's how I would write it myself." Then, of `quanta-diagnostics`: "This is going to
rebuild the whole store anyway, to capture some diagnostic data that cannot be backfilled. So we can
guarantee a full rebuild. Is the audit still needed?" and, on the restructure proposed in answer, "Yes
please, make that restructure." So:
- **The signature** (P1 as amended) is P1's flag on **the first accepted step of a restarted Radau
  instance** with π > 0 (after a reflection, a drain or a release; not the history's start, and not a later
  step of the instance). Either condition of P1 counts. In 01's measurement it holds for exactly the
  three bad steps of the diagnosis and for no other step, under None, over the 66 histories of its batch (the goldens among them);
  under F3 it holds for none. This closes `[01-p1-flag-fires-on-legitimate-steps]`.
- **02 is withdrawn.** It would decide which stored successes U3 regenerates; with the whole store
  rebuilt (by the user, after `quanta-diagnostics` 07), every one is. 01 already re-integrated 54 stored
  successes bitwise under the production code and found no bad step among them by the signature as
  ruled; the remaining successes are not audited, and no statement about them is made.
- **U3, U4 and U5 lapse.** U3's aim (no bad data in the store) is met by the rebuild; U4 was 02's scope; U5
  was 04's recomputation for a verdict that is no longer needed. 04 recomputes only the 42 failures, as
  part of the close.
- **U6 is ruled from 01 alone.** Ruled by the user, 2026-11-02: "That's fine. We'll do F3 plus the guard."
  So the fix is two things:
  - **F3:** each Radau restarted with π > 0 (after a reflection, a drain or a release; P7) is given
    `first_step = min(global_max_step, max(h_floor, f·φ/π))`.
  - **the guard:** P1's flag, with either condition, on the first accepted step of each such instance and
    on no other step. A flagged step is discarded before it is used for anything and retaken from the
    same state at half its h; below h_floor the history fails. The retakes are counted per history.
  In 01's data the guard never fires under F3, so F3 plus the guard is expected bitwise equal to the
  probe's F3 on every history 01 ran. Prompt 03 implements it.
- **The labels** (P6) stand: `VERSION_LABEL` 2026.9.0. `quanta-diagnostics`' U3 relies on no store of record
  being built at 2026.9.0 before it lands, which this re-plan keeps.

### 6.4 The handover restated, after 03 (2026-11-02)

`quanta-diagnostics` was re-planned and renamed `one-loop-drain` on 2026-11-02 (branch `one-loop-drain`,
README §8 there). Stated by the user the same day, for 04:
- **The rebuild of record** now waits for `one-loop-drain` 08, not `quanta-diagnostics` 07. It runs under a
  one-loop drain law at `VERSION_LABEL` 2026.10.0, with the fixed α kept as a selectable law. **No store of
  record is to be built at 2026.9.0.** This replaces "after `quanta-diagnostics` 07" in §1, §4 and §7.
- **`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`** (that board's §3, assigned
  here in `b306c45`) is 04's to investigate: does 03's F3 make 2197 and 2188 fail? Its logs and the probe
  driver are read on the other branch with `git show`.
- **Nothing about the drain is 04's.** The 42 are recomputed at their stored α, as planned. The 3873
  inside-wall stop belongs to `one-loop-drain` 01.
- **After 04 lands**, `reflection-restart` is merged into `one-loop-drain` (that campaign's U1), and its 01 is
  written on the merged tree. 04 merges nothing.

04 is written from this (`04-close.md`). Its §0.2 records what the planner read from the 2026.8.0 store for
it: the store already holds nine "not monotone" `classical_turning_point` failures at laboratory M, made
before anything of this campaign's, three of them 2197's β neighbours at α = 10⁻³. So whether F3 is the cause
is tested there on two trees and not assumed.

## 7. What this campaign hands to the user

- A fix, ruled and landed, with its measurements.
- **The handover for the rebuild** (§6.3): the whole store is rebuilt under `2026.9.0`, by the user, after
  `quanta-diagnostics` 07, and the figures of record are made from it. *(Restated 2026-11-02, §6.4: after
  `one-loop-drain` 08, at 2026.10.0; no store of record at 2026.9.0.)*
- ~~**A verdict on `2026.8.0`**, per block and per figure, with evidence.~~ *(Withdrawn 2026-11-02, §6.3; the
  text below is the plan as it was.)* Under U3 as ruled: every stored
  success with a bad step is listed for regeneration, whatever its observables do. The planner's
  expectation, to be tested and not assumed: D and Figure 6a stand unchanged (with a restart-only fix);
  A, A2 and B stand if 02 finds no bad step there; block C (Figure 6b, the M_crit
  comparison) needs rebuilding under `2026.9.0` in any case, since its 42 holes need the new code and
  one figure should not mix labels (≈ 1.5 h with BBN at the run's rate).
- The rebuild itself, which stays the user's.
