# Prompt 08 — the spectrum's peak and the relativistic margins

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-10-30, after 07b landed (`92f41f2`) and was reviewed (`79091b1`), at the user's word
("Please write prompt 08"). It is written from:
- README §8.4's charter for 08, and the rulings it rests on: R7, R13, R19, U9 (§8.3), P22, P23 and P25;
- the user's two rulings of the same day, R20 and R21 (README §8.1), made after the orchestrator's
  probes;
- log 07 §8 and log 07b §5.3 and §8 (the states handed on);
- the probes:
  - [`source/planning-probes-2026-10-30/`](source/planning-probes-2026-10-30/README.md) §0
    (`peaks.py`, `peaks.out`);
  - [`source/planning-probes-2026-10-30b/`](source/planning-probes-2026-10-30b/README.md)
    (`track3.py`, `cover.py`).

  They are references, not code to copy.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`. R19 ruled what is done in the gap,
and this prompt does it.

**Opens:** only what the work finds.

**Recommended model:** **Opus**. The definitions are physics and must match R7, R13, R19–R21 exactly. The
coverage bound of §1.6 is a derivation, and the integration must stay bitwise.

**Read first:**

1. [`README.md`](README.md):
   - §8.1: R7, R13, R19, R20, R21; §8.2: P22, P23, P25; §8.3: U9;
   - §8.4's charters for 08 and 09 (09 stores what 08 computes);
   - §5 (the rules); §6.2 P6 (the margin's threshold, ln 10), P11, P12.
2. The board's entry for `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`, with its ruling and
   its measurement.
3. Logs:
   - [`logs/07-unparking.md`](logs/07-unparking.md) §8 (how a reader tells the phases apart);
   - [`logs/07b-the-wall-search-at-the-rounding-of-ln-V.md`](logs/07b-the-wall-search-at-the-rounding-of-ln-V.md)
     §5.3 (the gap) and §8;
   - [`logs/04-the-wall-through-the-model.md`](logs/04-the-wall-through-the-model.md) §1 (the methods).
4. `ComputeTargets/ScalarHistory.py`:
   - the records: `TurningPoint` (:235), `Injection` (:286), `Parking` (:337), `Release` (:382),
     `IntegrationResult` (:437);
   - the warnings: `WARNING_*` and `WARNING_KINDS` (:137–:161), `warning_kind` (:1088), and
     `tune_history`'s printing of them (:3772);
   - `log_m_eff_squared` (:923), `log_trace` (:1156), `parked_tracker` (:1236);
   - `fixed_T_values` (:3020), the precedent for a value computed after the integration;
   - `integrate_history` (:3070) and its payload (:3326–:3372).
5. `CosmologyConcepts/Models/AbstractModel.py`: `classical_turning_point` (:538), `one_loop_turnaround`
   (:607) and `minimum_on_trace` (:700). None of them changes.
6. The physics:
   - `wall_backreaction.tex` in the briefing notes (README, "Sources"): §"Where the field is stopped" (k_ex),
     and §"The scheme solved in the Born regime" (the spectrum peaks at 1.0–1.07 k_ex);
   - the scoping, [`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md) §3.2 and §3.4.

---

## 0. Where this prompt sits

### 0.1 What it is for

R3 and R4 ask every history to certify that its quanta stay relativistic, at the bounces and on the late
tracker to today. R13 asks for a flag where they do not, with the worst margin and the first failure.

This prompt computes the margins, after the integration and outside its loop (R3). It records them in the
payload and warns, and never refuses a history. Storing them, as columns and as JSON, is 09's.

### 0.2 What the probes found (α = 0.1, untuned)

`peaks.out`, `track3.out`, `cover.out`:
- **M ≥ 3×10⁻¹³ M_P: every generation is classical** (ln 𝓡 ≤ −9.7 decades).
  - The birth margin is ℓ_t = −0.70 ± 0.01 on every generation. On the exponential wall k_ex/ω_t → ½, so
    it is negative by construction; R13 expects this flag to fire "by design".
  - The worst reflection margin is −0.67 to +0.005.
- **M ≤ 10⁻²⁸ M_P: every generation is one-loop** (ln 𝓡 ≥ 28 decades). ℓ is 14–33, and every margin
  is +16.8 or more.
- **In between, histories cross the one-loop gap.** At β = 1.2, 2 and 3, M = 10⁻¹⁶ M_P, the
  generations in it carry 0.88, 0.59 and 0.52 of K (log 07b §5.3).
- **The tracker margin depends on which mass is used** (`track3.out`). As the worst margin, with the
  failing share in brackets:

  | History | A: tracker, all samples | B: late tracker only | C: field's own mass |
  |---|---|---|---|
  | M = 0.5, β = 2 | +0.25 (7 %) | +8.1 (0) | −0.71 (49 %) |
  | M = 0.5, β = 5 | −0.04 (22 %) | +8.6 (0) | −0.70 (39 %) |

  At M ≤ 10⁻³ every reading passes, by 4.5 to 38.
- **The turnarounds cover the samples before the late tracker** (`cover.out`), on seven of eight
  histories. At M = 0.5, β = 25 one generation's sampled field-mass margin falls 3×10⁻⁴ below them, which
  is the momentum's redshift over one swing (§1.6).
- **The leftover's share** of K is 5×10⁻⁵ to 2.3×10⁻³.
- **The cost.** The tracker over every sample took 0.18–0.79 s, against a history of 2.6–9 s. The
  one-loop calls took 0.1–17 ms.

### 0.3 The user's rulings of 2026-10-30

- **R20 — the tracker margin is the late tracker's.** Asked which mass the tracker margin uses, the user
  chose B: "B is cleanest, provided we are sure that checking at the reflections safely covers the time
  period before the last generation event."

  So the tracker margin runs only after the history's last turning point or generation. It does not run
  between bounces. **The proviso binds this prompt.**
  - The reflection margin must bound the field's mass margin before that point. The plateau turning
    points are therefore added to the events it runs over (§1.2).
  - §1.6 derives the bound, and §2.3 measures it on every history, with a stop.
- **R21 — the leftover is unassessed.** The parking leftover (P19) has no bounce, so it gets no peak and no
  margins. Its share of ρ_χ is reported beside the failing shares as unassessed. The user chose the
  option "Unassessed, share reported".

---

## 1. What is wanted

### 1.1 Generations and their peaks

- **A generation** is a candidate, an entry of `injections`, with `delta_K > 0` and an assessment
  (`classical_turning_point` not None). A candidate with `delta_K > 0` and no assessment is unassessed.
  So is the parking leftover (R21).
- **Its birth** is at `N_turn`. Its comoving peak momentum is

      ln q_j = ln k_ex,j + N_turn,j,

  in the Einstein frame (a_E = e^N). Every momentum and mass here is Einstein-frame. The ratio p/m is
  frame-independent, since both carry A⁻¹.
- **The estimate** is chosen by 𝓡 (R7, R19, P25):
  - ln 𝓡 ≤ 0: the classical estimate, with ln k_ex, and ln m = ½ ln ω_t², from the candidate's
    `classical_turning_point`.
  - ln 𝓡 > 0: `one_loop_turnaround(phi_assessed, log_E_assessed)` on the history's realized model.
    - If it returns a turnaround, the one-loop estimate: its `log_k_ex` and `log_m_ta`.
    - If it raises `ComputationFailureError` or returns `None`, the classical estimate, flagged as the gap
      ("Classical, flagged", R19).
  - Any other exception propagates.
- **Each generation records** which estimate it got: `"classical"`, `"one_loop"` or `"classical_gap"`.
  It also records its N_turn, ln q, ln k_ex, ln m, ln 𝓡 and share.
- **The share** is s_j = ΔK_j/K(N_final). K(N_final) comes from the history's ledger and includes the
  leftover.
- **The weighted generations** are those with s_j ≥ 10⁻³ (U9). The threshold is a module constant
  beside the others, with its comment. It is not a key.

### 1.2 The three margins, per generation

Each margin is reported with the N where its minimum lies.
- **Birth:** b_j = ln k_ex,j − ln m_j. For a classical estimate it is the turning point's `ell_t`, by the
  same expression.
- **Reflection:** r_j is the minimum of ln q_j − N_k − ln m_k over every event k with N_k > N_turn,j.
  The events are:
  - **every later candidate**, `delta_K = 0` included. It sits at its `N_turn`, and its m_k comes from
    §1.1's estimate rule. A candidate with no assessment uses the field's own m_eff at its state at
    `N_turn` instead (`log_m_eff_squared` on the dense output). The log counts these;
  - **every later plateau turning point** (`TurningPoint.kind == "plateau"`). Its m_k is the field's own
    m_eff at its state on the dense output, from `log_m_eff_squared`. This is R20's proviso (§0.3). The
    charter named the wall-side turnarounds and reflections only.

  r_j is None if there is no later event.
- **Tracker (R20):** t_j is the minimum of ln q_j − N − ln m_tr(N) over two sets of points:
  - every stored sample with N > N_late;
  - today, the final state at N_final.

  N_late is the largest N among the history's turning points of both kinds and its candidates'
  `N_turn`. m_tr is the zero-lag tracker's m_eff: `log_m_eff_squared` at φ_min on that sample's trace
  (`minimum_on_trace`). On a parked sample the field is φ_min, so the agent may reuse it, provided the
  value is the tracker's.

  The anchor of each `minimum_on_trace` call follows a fixed rule (for example the previous φ_min,
  starting from the sample's field), so that the result depends on the history alone. Today's mass is
  the tracker's at the final state.

### 1.3 Per history

A record of floats, ints, strings and tuples of them, so that it crosses Ray. Nothing callable.
- **For each of the three kinds:**
  - the worst margin over the weighted generations, its N, and its generation's index in `injections`;
  - the failing share: Σ s_j over the weighted generations with a margin below ln 10. A margin of None
    is not failing.
- **The shares:**
  - the weighted share;
  - the share in generations below the weight;
  - the unassessed share (the leftover and the candidates with no assessment, R21).
- **The counts of generations** per estimate.
- **The first failure of each kind** (R13), or None. This is the earliest N at which any weighted
  generation's margin of that kind falls below ln 10:
  - for birth, the generation's N_turn;
  - for reflection, the first event N_k;
  - for tracker, the first late sample, or today.

  Its details: the generation's index, that N, the margin there, ln q, ln m, and the estimate.

The threshold is ln 10 (P6), a module constant with its comment. The record and the per-generation list
are returned by one function. 09 stores the record's worst margins, their N and the failing shares as
columns, and the first failures as JSON. Name the fields so that 09 can take them as they are.

### 1.4 Where it runs

- **A pure core**, a module-level function. It takes plain per-generation, per-event and per-sample
  values, so that a test can hand it a constructed history.
- **A gatherer.** It builds those values from the `IntegrationResult`, the samples, the realized model,
  the policy and the dense output, and calls the core.
- **The caller.** `integrate_history` calls the gatherer after `fixed_T_values`, and only where the step
  loop assessed the wall: α > 0 on a model with a wall. The payload gains `"generations"` and
  `"margins"`, which are not stored.
- **At α = 0** both keys are `None`, nothing is computed (P22), and no new warning can arise.
- **`IntegrationResult` and the step loop do not change.** So every value of the integration is bitwise
  07b's, and only the payload gains these two keys and their warning lines.
- **A failure inside the margins does not fail the history** (warn, never refuse). This covers a
  `ComputationFailureError` from the tracker's minimum or a model call. Both keys are then `None`, and
  one warning line of its own kind (§1.5) gives the reason.

### 1.5 The warnings

Four new kinds, each a fixed opening phrase in `WARNING_KINDS`, recognised by `warning_kind` (P23 names
no M):
- a worst birth margin below ln 10;
- a worst reflection margin below ln 10;
- a worst tracker margin below ln 10;
- the margins could not be computed.

Each line gives the worst margin, its N and the failing share. The payload's `"warnings"` is the loop's
tuple with these lines appended. `IntegrationResult.warnings` is not touched. `tune_history` prints the
first line of each kind, as it does now.

The birth warning fires on every history with a weighted classical generation, which R13 expects.

### 1.6 The coverage bound (R20's proviso)

Derive in the log the condition under which r_j and b_j bound the field's own mass margin at every N with
N_turn,j < N ≤ N_late:
- **A swing.** Take consecutive events N_a < N_b among the birth, the later turning points of both kinds
  and the reflections. The momentum falls as e^{−N}. So at any N between them,

      ln q − N − ln m(N) ≥ ln q − N_b − max over the swing of ln m.

- **The mass on a swing.** Show when the maximum of m_eff over the swing is at its ends.
  - On the exponential, V'' is completely monotone in φ, and (ln A)'' + (ln A)'² times the trace is
    convex. So m_eff² is convex in φ at fixed trace.
  - Say what the trace's change over the swing adds.
  - Say what a generic model must satisfy.
- **The bound.** It follows that the sampled margin can fall below min(b_j, r_j) by at most the swing's
  duration N_b − N_a, plus the trace's term. `cover.out`'s 3×10⁻⁴ at β = 25 is of that kind.
- **Using the estimate's mass.** The bound uses the estimated mass at a wall-side event (ω_t or m_ta).
  The field's own mass at a resolved turning point is close to it. At a reflection the field never
  reaches φ_t in the code, though it does physically. Say why the estimate is the right mass there.

The margin is not changed to absorb the swing term. §2.3 measures the shortfall, and the stop of §5 binds.

### 1.7 The documents (P12)

- **`.documents/architecture-summary.md`:** a dated note beside 07b's covering:
  - the generations, the estimates by 𝓡 with the gap flagged, and the three margins;
  - the record and its fields;
  - that nothing is stored yet.
- **`.documents/numerical-strategies.md`:** a dated subsection after 07b's 6.7 covering:
  - the definitions;
  - the coverage bound;
  - the weight and the unassessed share;
  - the measured margins across M.

### 1.8 The tests

A new module, `ComputeTargets/tests/test_margins.py`. No store and no Ray. Each test that integrates gives
its cost and the load in its docstring.
1. **A constructed history against a hand computation.** Three generations, through the pure core:
   - one classical and one one-loop; one in the gap (classical, flagged);
   - later candidates, one with `delta_K = 0`;
   - two plateau turning points, **one of which binds a reflection margin**;
   - late samples, and today;
   - a leftover.

   Every margin, its N, each worst, each failing share, the shares and each first failure equal the
   test's own computation. The tolerance is derived from the operations, or equality where the
   operations are the same.
2. **The energy weight.** A generation with the worst margin, at a share just below 10⁻³, is left out
   of the worst and the failing share. At 10⁻³ exactly it is included. The leftover is counted only as
   unassessed.
3. **The estimate by 𝓡 on a real history:** M = 10⁻¹⁶ M_P, β = 2, α = 0.1, `test_injection`'s setup. In
   log 07b §5.3:
   - every generation with ln 𝓡 ≤ 0 is classical;
   - the three with 0 < log₁₀ 𝓡 ≤ 2.92 are `"classical_gap"`, carrying 0.5877 of K;
   - the one at log₁₀ 𝓡 ≥ 3.00 is `"one_loop"`, carrying 0.2812. Its birth margin is its ℓ > 0.

   The shares are to the log's four digits plus their rounding.
4. **The classical birth margin is negative by construction** on resolved turnarounds: M = 0.5 M_P,
   β = 2, α = 0.1. Every generation is classical, and its birth margin equals its `ell_t` bitwise and is
   < 0.
5. **The tracker margin on a parked and on a released history.**
   - M = 10⁻³⁰, β = 2 (parked): the tracker margin runs over the samples after N_late and today only, and
     equals the test's recomputation from the samples.
   - M = 0.5, β = 5 (released): the generations after the release are included, and N_late lies after
     the release.
6. **α = 0.** On the golden histories' setup both keys are `None`, and no new warning appears. Every other
   payload key is bitwise the dispatch commit's.
7. **The warnings.** M = 0.5, β = 2 carries the birth and reflection lines, and `warning_kind` returns
   their kinds. `tune_history` prints the first of each kind, as for 05's warnings.
8. **A failure in the margins** (a patched `minimum_on_trace` that raises, through the module's name):
   - the history succeeds;
   - both keys are `None`;
   - the warning of that kind is present;
   - every other payload key is bitwise the unpatched run's.
9. **What does not change.** These pass unedited:
   - the golden tests;
   - `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop`, `test_injection`,
     `test_gate_and_taper`, `test_unresolved_turnarounds`, `test_parking`, `test_release`,
     `test_wall_model`, `test_wall_rounding`.

**Each test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: ln q from the drain point's N instead of N_turn.
- B2: a gap generation given no flag: `"classical"` in place of `"classical_gap"`.
- B3: the plateau turning points left out of the reflection margin.
- B4: the tracker margin over every sample after birth (reading A).
- B5: the leftover counted as a generation, with its candidate's peak.
- B6: the weight's comparison > in place of ≥.
- B7: the margins computed at α = 0.
- B8: a failure in the margins propagated as the history's failure.

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned), α = 0.1, unless
stated. No store, no Ray. The dispatch commit's histories are run in a temporary worktree of it, which is
removed afterwards.

1. **The margins across M**, at the final tree, on these histories:
   - M = 0.5 at β ∈ {1.2, 2, 5, 10, 25};
   - M = 10⁻³ at β ∈ {0.5, 1.2, 2, 3};
   - β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻¹⁰, 10⁻¹², 10⁻¹³, 10⁻¹⁵, 10⁻¹⁶, 10⁻¹⁷, 10⁻¹⁸, 10⁻²⁰, 10⁻²²,
     10⁻²⁵, 10⁻³⁰} M_P.

   For each, give:
   - the generations per estimate, with their shares;
   - the three worst margins with their N, and the failing shares;
   - the weighted, below-weight and unassessed shares;
   - each first failure;
   - the warnings.
2. **The gap.** For each history that crosses it:
   - the gap generations' birth margins (classical);
   - the share they carry;
   - the one-loop generations nearest to them in log₁₀ 𝓡 and their ℓ.
3. **The coverage (R20's proviso).** On every history of §2.1 and each weighted generation, form the
   field's own mass margin at every stored sample with N_turn,j < N ≤ N_late. Give:
   - its minimum against min(b_j, r_j);
   - the largest shortfall, with the swing it lies in, its duration and §1.6's bound;
   - whether the plateau turning points bind anywhere.

   This is a measurement made in a scratch script, not code.
4. **The late tracker.** Over the late samples and today, compare the tracker's mass with the field's
   own: give the largest difference in t_j on each history.
5. **What does not change**, at the final tree against the dispatch commit, bitwise:
   - every `IntegrationResult` field, `ts` and every payload key that existed;
   - the warnings, apart from the appended lines.

   On these histories:
   - log 05's grid (β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰});
   - M = 0.5 at β ∈ {1.2, 2, 5, 10, 25};
   - natural mode at M = 0.5, β = 2;
   - the golden histories' setups.
6. **The tuning.** Tune at M = 10⁻³ M_P, β = 1.2, α = 0.1 and at M = 0.5, β = 5, α = 0.1. Give the
   margins on each converged payload, the number of integrations, and the margins' cost per
   integration.
7. **The cost.** The margins per history are the minimum of three rounds, split into:
   - the one-loop calls;
   - the plateau masses;
   - the tracker's samples.

   Give them beside the history's wall time and the load. If the margins take more than 10 % of a
   history, say so, and say what dominates. This is not a stop.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 439 (96 + 343) / 103 / 115 / 64 (log 07b §8). A different count, or a failure, is a stop.
2. **After:**
   - the five suites. ComputeTargets rises by the new tests only, and the tests of §1.8 item 9 pass
     unchanged; say so;
   - `black --check` on the files you changed.
3. **The breakages** of §1.8, each with the tests it fails.

## 4. Acceptance

1. **§1 as written, and the gate "Margin"** (README §8.4, restated):
   - the three margins equal a hand computation, energy-weighted;
   - the estimate is chosen by 𝓡, with the gap flagged;
   - the classical birth margin is negative by construction;
   - the coverage is measured within §1.6's bound;
   - NULL at α = 0;
   - every value of the integration is bitwise.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/08-spectrum-peak-and-margins.md`, in the template of `dk-and-rpk-migration` README
   §5.1. It holds:
   - the definitions as built;
   - the derivation of §1.6;
   - the suite counts before and after;
   - the breakages as diffs;
   - every deviation, classified;
   - "State handed to the next prompt", for 09:
     - the record's fields, their types and which of them 09 stores as columns and which as JSON;
     - the payload keys;
     - the cost.
4. **The documents** of §1.7.
5. **The board:**
   - row 08 and the gate Margin, in §1 and §2;
   - `### 5.10 After prompt 08 (<date>)` in §5;
   - `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` moved from §3 to §4, with a "Closed" line
     naming this commit;
   - in the same commit, its row deleted from `.documents/OPEN_ISSUES.md` §1.19, with the count and the
     date corrected (`CLAUDE.md`);
   - anything opened, on §3 and in the index.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history changes, or any test of §1.8 item 9 needs an edit.
- Any value of the integration (§2.5) is not bitwise the dispatch commit's.
- **§2.3 finds a shortfall larger than §1.6's bound** on any history. Then R20's proviso does not hold as
  measured, and the choice of the tracker margin goes back to the user.
- §1.6 cannot be derived for the exponential.
- `one_loop_turnaround` raises on a generation with log₁₀ 𝓡 ≥ 3.00, or solves on one with ℓ ≤ 0.
- Item 3's shares or counts differ from log 07b §5.3.
- The work seems to need a change to:
  - the step loop, `IntegrationResult` or any record the loop builds;
  - the model's methods;
  - the store, or anything outside `ComputeTargets/ScalarHistory.py` and the new tests.

## 6. What this prompt does not do

- **Nothing is stored.** No column, no JSON, no key, no version: those are 09's.
- **No change to the integration**, the drain, the parking or the release. The margins read the
  history; they never change it.
- **No closed form** of any potential (R9). Only the coverage derivation uses the exponential, and it
  says so.
- **No threshold refuses a history.** The margins warn (R13, P6).
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`: the constants, the records, the functions, the call in
    `integrate_history`, the payload keys and the warning kinds;
  - `ComputeTargets/tests/test_margins.py` (new);
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, the board and `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
