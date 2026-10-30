# Orchestrator — prompt 08, the spectrum's peak and the margins

Read [`../README.md`](../README.md) first: §5 (the rules), §8.1 (R7, R13, R19, R20, R21), §8.2 (P22, P23,
P25), §8.3 (U9), and 08's charter in §8.4. Then the prompt, the board's entry for
`[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`, log 07 §8, log 07b §5.3 and §8, and the probes
[`source/planning-probes-2026-10-30/`](../source/planning-probes-2026-10-30/README.md) §0 and
[`source/planning-probes-2026-10-30b/`](../source/planning-probes-2026-10-30b/README.md). This note binds
08 only. The conventions of notes 02–07b bind here as well, and are restated below: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`08-spectrum-peak-and-margins.md`](../08-spectrum-peak-and-margins.md) · **Gate:**
"Margin" · **Closes:** `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` · **Opens:** only what
the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `4e6aaa2` on 2026-10-30 at 23:30 GMT, before this note was committed:
- `inelastic-reflection` is checked out in the main checkout, at `4e6aaa2`, the commit that adds prompt 08.
  That commit came after 07b landed (`92f41f2`) and was reviewed (`79091b1`).
  - `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main
    checkout alone.
  - No code has changed since 07b landed: `git diff --stat 92f41f2 4e6aaa2` lists files under
    `prompts/inelastic-reflection/` only.
  - So the prompt's line numbers hold for this tree. Checked: `ScalarHistory.py` :146–:161, :235, :286,
    :337, :382, :437, :923, :1088, :1156, :1236, :3020, :3070, :3772. The payload's dict is
    :3323–:3373, not :3326–:3372. `AbstractModel.py` :538, :607, :700.
  - `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- SciPy is 1.17.0.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 439 (96 + 343) / 103 / 115 / 64.
- The load averages were 15 / 19 / 34. Other workloads share the machine.

## 0. What makes this prompt unusual

**The definitions are short. The hard parts are a contradiction in the warnings and a part of the
history that R20's proviso covers but the swing bound does not.** For this note the orchestrator read
the records, `log_m_eff_squared`, `log_trace`, `parked_tracker`, `warning_kind`, `tune_history`'s
printing, `integrate_history` and its payload, the three model methods, `ExponentialModel`'s coupling,
and every existing test that reads the payload's `"warnings"`. It also ran a probe (`check.py`, session
scratchpad) on five histories at α = 0.1: β = 2 at M = 10⁻¹⁶ and 10⁻³⁰, and M = 0.5 at β = 2, 5 and 25.
Here is what it found where the prompt is silent, loose or wrong.

- **Prompt §1.5 contradicts §1.8 item 9 and the stop of §5.** §1.5 appends the margin lines to the
  payload's `"warnings"`, and says M = 0.5, β = 2 carries the birth and reflection lines. But two frozen
  tests read that key on dissipation histories:
  - `test_unresolved_turnarounds.py:592` asserts `data["warnings"] == result.warnings` on M = 0.5,
    β = 2, α = 0.1. It would fail.
  - `test_parking.py:933` asserts `len(data["warnings"]) == 1` on M = 0.5, β = 5, α = 0.1. Every
    generation there is classical, so the birth line fires. It would fail.

  Both modules are on item 9's list, so as written the prompt stops at its first full run. Ruling 1.
- **At small M every candidate is a floor reflection.** At β = 2, M = 10⁻¹⁶ and 10⁻³⁰, all 49
  candidates are `"floor"`. There are 48 plateau turning points and no wall-side ones. So the reflection
  margin's events there are floor reflections and plateau turning points only. In the code the field
  turns back at the floor rule, never at φ_t.
  - At M = 0.5 every candidate is `"turning_point"`, and its `N_turn` is a wall-side turning point's N
    exactly.
  - `dropped_drains` is 0 on all five histories. Every candidate has `delta_K > 0` and an assessment.
  - K_0 is 0, and Σ ΔK/K + leftover/K = 1 to 6×10⁻¹⁶.
- **Item 3's shares sit close to their tolerance.** At β = 2, M = 10⁻¹⁶ the generations with 𝓡 > 1 are
  indices 0–3. Index 0 (log₁₀ 𝓡 = 4.238) is `"one_loop"`, with share 0.28124818254154527. Indices 1–3
  (log₁₀ 𝓡 = 2.221, 1.253, 0.318) are `"classical_gap"`, with shares summing to 0.5876556460838358.
  - Against the log's 0.2812 and 0.5877, with half a unit of the fourth digit (5×10⁻⁵), the margins are
    1.8×10⁻⁶ and 5.6×10⁻⁶. That is small but deterministic.
  - The counts match log 07b §5.3: no candidate in the gap has `delta_K = 0`, so no generation is lost.
- **The parked phase of a released history lies before the last generation, and no turnaround bounds
  it.**
  - At M = 0.5, β = 5 the field parks at N = 38.004 (the cut candidate's `N_turn` is 37.991) and is
    released at 40.255. The last generation is at 43.606.
  - At β = 25 it parks at 132.551 and is released at 145.779, and the last generation is at 147.461.

  So the parked samples lie in N_turn,j < N ≤ N_late, the region R20's proviso covers. They lie inside a
  single "swing" from the cut candidate to the first event after the release, of 2.25 and 13.2 e-folds.
  §1.6's bound, the swing's duration, is then 13 e-folds at β = 25. It holds, but it covers nothing.
  `cover.out` found the samples bounded on both histories, so the margin there is in fact large. But the
  prompt's derivation does not show it, and the stop of §5 cannot fire on it. Ruling 7.
- **On the exponential, m_eff² is monotone in φ at fixed trace, which is stronger than §1.6's
  convexity.**
  - `ExponentialModel` has ln A = βφ/M_P, so (ln A)'' = 0 and (ln A)'² = β²/M_P². The trace term
    β²T/M_P² does not depend on φ.
  - V = e^{norm} exp((M/φ)ⁿ) is completely monotone, so V'' > 0 falls strictly with φ. The maximum of
    m_eff² over a half-swing at fixed trace is therefore at its inner end.
  - φ is monotone between consecutive turning points, and a reflection only flips π.
- **The classical mass ω_t omits the trace term.** ω_t² = V''(φ_t), whereas the field's own m_eff² is
  V'' + β²T/M_P² ≥ V''. So at a resolved wall-side turning point the estimate's mass lies below the
  field's own, and the reflection margin there lies above the field's own margin. E is also taken at
  `phi_assessed`, a step start before the turning point. §1.6's "close to it" needs a number. Ruling 7.
- **Item 8's patch target would break the integration.** The parked phase calls the model's
  `minimum_on_trace` (`parked_tracker`). A patch of it that raises therefore fails a parked history
  before the margins run, and the unpatched run then cannot be compared. Ruling 4.
- **The anchor.** `peaks.py` anchored each `minimum_on_trace` at the previous φ_min, and fell back to the
  sample's field when that raised. The probe anchored at each late sample's own field instead. It had
  no failures on 1 342–2 142 late samples per history, at 19–52 ms per history. Ruling 3.
- **`tune_history` prints a kind's first line from the first integration that returns it**, which is
  the seed's, not the converged one. A margin line printed from the seed integration may not describe
  the history that is stored. That is the convention for 05's warnings, and the prompt keeps it. §2.6
  measures whether it matters. Ruling 12.
- **Ties are not ruled.** The worst margin, and the first failure at one N, can be shared by several
  generations. For example, every weighted generation fails at the same late sample. Ruling 8.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The margin lines go in a key of their own, `"margin_warnings"`, never in `"warnings"`.**
   - The payload's `"warnings"` stays `result.warnings`, bitwise.
   - `"margin_warnings"` is a tuple of lines, each beginning with one of the four new phrases of
     prompt §1.5, which join `WARNING_KINDS` and are recognised by `warning_kind`:
     - `()` where nothing fires, and at α = 0 or without a wall;
     - one line, of the "could not be computed" kind, where the margins fail.
   - A failure payload (`_failure_payload`) does not gain the key.
   - `tune_history` reads `data.get("warnings", ())` and then `data.get("margin_warnings", ())`, through
     the one set of printed kinds, so that each kind is printed once per history as now.
   - §2.5 then compares `"warnings"` bitwise with no exception, and §1.8 item 7 asserts on
     `"margin_warnings"`.

   *STRUCTURALLY REQUIRED* (§0, first point: prompt §1.5 against §1.8 item 9 and §5).
2. **The payload gains three keys:** `"generations"`, `"margins"` and `"margin_warnings"`, none of them
   stored.
   - At α = 0, or on a model without a wall: `None`, `None`, `()`.
   - Where the margins fail: `None`, `None`, and the one failure line.
   - The caller's condition is the step loop's `assess_wall` expression, verbatim
     (`ScalarHistory.py:1650`). The loop is not touched.
   - The gatherer runs after `fixed_T_values`. Inside it, only `ComputationFailureError` is caught; any
     other exception propagates.

   *IMPLEMENTATION CHOICE*, ruled here.
3. **The tracker's anchor.** Each late sample, and today, is searched from its own field φ: the sample's
   `phi_Einstein`, and the final state's. Nothing is carried from one sample to the next.
   - A parked sample's field is not reused as φ_min. Every tracker mass comes from one fresh
     `minimum_on_trace`, so that the margin has one definition and test 5's recomputation is the same
     call.
   - The trace at a sample is formed from its stored ln ρ_R,E, ln f_m and Sigma, by `_log_trace`, as
     the parked phase forms it. At a dense-output state and at today it is `log_trace(cosmology, …)`.

   *IMPLEMENTATION CHOICE*, ruled here.
4. **The masses go through module-level functions of `ScalarHistory.py`, called by their module names
   at call time,** as the gate's and the parking's are.
   - One gives the tracker's ln m at a (sample) state.
   - One gives the field's own ln m at a dense-output state, used for the plateau turning points and
     the unassessed candidates.

   Test 8 patches the tracker's function with a stub that raises `ComputationFailureError`. The model's
   `minimum_on_trace` is never patched, so the integration is untouched by construction. Test 8 uses
   M = 0.5, β = 2.

   *IMPLEMENTATION CHOICE*, ruled here.
5. **The events of the reflection margin** are, for generation j, every event with N_k > N_turn,j:
   - every later candidate, at its `N_turn`, with §1.1's mass;
   - every later plateau turning point, with the field's own mass;
   - **every later wall-side turning point that has no candidate** (a dropped drain), with the field's
     own mass. The charter names "every later wall-side turnaround". The probe found none on five
     histories. The log counts them on every history of §2.1.

   A candidate's estimate (and its one-loop call) is made once and used both for its generation and as
   an event. *IMPLEMENTATION CHOICE*, ruled here.
6. **The birth margin** is formed by the method's own expression: `log_k_ex - 0.5 *
   log_omega_t_squared` for a classical estimate, and `log_k_ex - log_m_ta` for a one-loop one. It
   therefore equals `ell_t` or `ell` bitwise, and tests 3 and 4 assert that with `assertEqual`.
   *IMPLEMENTATION CHOICE*, ruled here.
7. **§1.6's derivation covers four things the prompt leaves loose.**
   - **The exponential's mass.** Use §0's monotone form, or say why convexity is enough. Say how the
     trace's fall over a swing, by e^{−4ΔN} in ρ_R,E at fixed Σ + f_m, enters the bound, as a term in
     ln m.
   - **The estimate's mass against the field's own** at a resolved wall-side turning point. Give
     ln m_eff(field, N_turn) − ½ ln ω_t² (or − ln m_ta) on every history of §2.1, its largest value,
     and its sign. Say whether it enters the bound.
   - **At a floor or unresolved reflection** the code's field turns back outside φ_t. Say why the
     estimate's mass is the physical one there, and why the code's own samples near it then lie above
     the bound.
   - **The parked segment of a released history**, from the cut candidate to the first event after
     `Release.N`, is treated as its own interval, with the tracker's mass.
     - Derive the condition under which the margin there is bounded below by its value at the start
       of the parked phase, for example d ln m_tr/dN ≤ −1. Give the bound that follows.
     - §2.3 reports the parked segment separately on every released history: its smallest
       field-own margin against min(b_j, r_j), the shortfall, the segment's duration, and the derived
       bound.
     - The swing's duration is **not** accepted as the bound there.
     - **Stop and ask** if the derivation gives no bound tighter than the segment's duration, or if
       the measured shortfall exceeds the derived bound. R20's proviso then goes back to the user.

   *STRUCTURALLY REQUIRED* (R20's proviso: "the time period before the last generation event"
   includes the parked segment; §0, fourth to sixth points).
8. **Determinism.**
   - A worst margin shared by several generations is reported with the lowest index.
   - A first failure at an N shared by several generations reports the smallest margin there, and then
     the lowest index.
   - Minima are taken over Python floats in the order of `injections`, and over samples in N order.

   *IMPLEMENTATION CHOICE*, ruled here.
9. **The shares.**
   - K is `dark_radiation.K(N_final)` from the history's ledger, the same float the probes used.
   - K_0 (the test harness `rho_chi_ratio_init`) has no bounce. It is counted in the unassessed share,
     and the log says so.
   - The weighted, below-weight and unassessed shares partition K. Test 1 asserts that they sum to 1,
     within a tolerance derived from the number of additions.
   - Item 3's tolerance is 5×10⁻⁵ on each share: half a unit of log 07b's fourth digit. Its margins are
     §0's, about 2×10⁻⁶ and 6×10⁻⁶.

   *IMPLEMENTATION CHOICE*, ruled here.
10. **The record's counts.**
    - The generations per estimate are counted over every generation, weighted or not.
    - The record also gives the number of weighted generations and the number of unassessed
      candidates.
    - The per-generation list holds generations only (`delta_K > 0`, assessed), each with its index in
      `injections`.

    *IMPLEMENTATION CHOICE*, ruled here.
11. **Item 6 in a test.** A test cannot see the dispatch commit. Item 6 asserts the three keys
    (`None`, `None`, `()`) and `data["warnings"] == result.warnings` on the golden setups. The bitwise
    comparison with the dispatch commit is the golden tests (unedited) and §2.5. *IMPLEMENTATION
    CHOICE*, ruled here.
12. **§2.6, the tuning.** On each tuned history, compare the margin lines `tune_history` printed with
    `"margin_warnings"` of the converged payload. Say whether they differ in kind or in value. Nothing
    is changed if they do; the log records it. *IMPLEMENTATION CHOICE*, ruled here.
13. **§2.2, the gap's estimate.** Tabulate, over every history of §2.1, why each `"classical_gap"`
    generation got its estimate: the raise's text by kind ("no one-loop turnaround: ell … fell to",
    "did not converge", any other), or `None`.
    - A raise of another kind, or a `None`, is recorded under "Observations not acted on" with its
      inputs.
    - It is a stop only as prompt §5 says: a raise at log₁₀ 𝓡 ≥ 3.00.

    *IMPLEMENTATION CHOICE*, ruled here.
14. **§2.5, bitwise.** Compared against the dispatch commit in one temporary worktree of this note's
    commit, removed afterwards. On every history, compare by `repr`:
    - every record, the candidates' assessments included;
    - the samples, the final state, `N_final`, `nfev`, `accepted_steps` and `ts`;
    - the parking and release records;
    - `"warnings"`;
    - every other payload key that existed, except `metadata`'s compute times and `z_grid`'s object
      address.

    §2.1's histories run at the final tree only. *IMPLEMENTATION CHOICE*, ruled here.
15. **No file but the prompt's §6 list changes.** In particular these are byte-identical:
    - `CosmologyConcepts/`;
    - every existing test module;
    - the golden modules and JSON;
    - the `integrator-remediation` board.

    In `ScalarHistory.py` the step loop (`integrate_scalar_history`), `IntegrationResult` and every
    record the loop builds are unchanged: `git diff` touches only new constants, new records, new
    functions, `WARNING_KINDS` and its comment, `integrate_history`'s call and payload, and
    `tune_history`'s printing loop.
16. **Timing and the suites.**
    - §2.7's costs are the minimum of three rounds per history, with `uptime` beside them, on at least
      the histories of §2.6 and β = 2 at M = 10⁻¹⁶ and 10⁻³⁰. The rest are single rounds.
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output to a
      file in your own scratch directory, grep the verdict from it, and record `uptime` beside each.
    - A run that may pass 600 s goes in the background. Wait for its notification without polling.
    - §2's histories run at most four at a time, in the background. One that has not ended after 15
      minutes is stopped and recorded as such.
    - A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. §1.6's derivation, with ruling 7's four parts, written down before any code.
  3. The constants, the records, the core, the gatherer, the call and the payload keys (rulings 1–6
     and 8–10).
  4. The tests of prompt §1.8.
  5. The breakages B1–B8. Then the measurement of §2.
  6. The suites and `black --check`.
  7. The records: the log, `.documents/`, the board, and `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`, not
  zsh. None is committed. Ruling 1's key is part of what B7 and B8 must be caught on.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file goes
  under the repository. §2.5's worktree of the dispatch commit is removed afterwards, so that
  `git worktree list` shows only the main checkout.
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No golden
  JSON is regenerated.
- **Stop conditions are real.** They are:
  - the prompt's §5;
  - ruling 7's;
  - an existing test failing for a reason this note does not name.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `4e6aaa2`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The frozen tests that read `"warnings"`**: `test_unresolved_turnarounds.py` :300, :592, :764 and
   `test_parking.py` :933 read. :592 and :933 are on dissipation histories that §1.5 would change. :300
   (α = 0) and :764 (a failure payload) are not affected.
3. **`check.py`** on five histories (§0, second to fifth points).
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board, logs 07 and 07b and the two probes' READMEs;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/08-spectrum-peak-and-margins.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the definitions as built, and the record's fields with their types;
  - §1.6's derivation, with ruling 7's four parts;
  - the new tests by name, each with what it asserts and its tolerance;
  - the tables of §2, the parked segment's included;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.**
   - `git show --stat <agent>` lists exactly the prompt's §6 files.
   - Its parent is this note's commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
   - Ruling 15: `git diff` on `ScalarHistory.py` leaves `integrate_scalar_history`, `IntegrationResult`
     and the loop's records alone.
2. **α = 0 and the frozen tests.** The tests of prompt §1.8 item 9 pass at `<agent>` unedited, and
   `git diff` on every existing test module is empty.
3. **The definitions** against prompt §1.1–§1.3 and rulings 5, 6, 8–10, line by line:
   - the estimate by 𝓡, with ln 𝓡 ≤ 0 classical;
   - `"classical_gap"` on a `ComputationFailureError` or `None`, and any other exception propagated;
   - ln q from `N_turn`;
   - the events;
   - N_late;
   - the tracker's anchor;
   - the weight's ≥;
   - the shares;
   - the first failures;
   - ln 10.
4. **The derivation.** Re-derive §1.6 independently for the exponential. Check ruling 7's four parts,
   the parked segment's bound in particular, against the log.
5. **An independent probe** of my own (session scratchpad, not committed):
   - recompute the margins on β = 2 at M = 10⁻¹⁶ and 10⁻³⁰, and M = 0.5 at β = 2, 5 and 25, from the
     payload's records and samples, without the agent's functions. Compare with `"generations"` and
     `"margins"`;
   - the coverage, the parked segment included, on M = 0.5, β = 5 and 25;
   - bitwise against the dispatch commit on β = 2 at M = 10⁻¹⁶, M = 0.5 at β = 5, and natural mode at
     M = 0.5, β = 2, in a temporary worktree that I remove.
6. **The breakages.** Replay each of B1–B8 with `git apply` in `bash`. Each must fail the tests the log
   names, and is then reverted.
7. **The numerics of the tests.** Check test 1's hand computation and its tolerances, item 3's 5×10⁻⁵
   (ruling 9), the weight's boundary at 10⁻³ exactly, and the partition's tolerance.
8. **The measurement and the records.**
   - **§2:** the tables are complete. §2.1 has all 45 histories. §2.2 tabulates the raise texts
     (ruling 13). §2.3 has the parked segment (ruling 7). §2.4 to §2.7 are given, and §2.6 has ruling
     12's comparison.
   - **The log:** it is in §5.1's form, with every deviation classed and §0's rulings among them.
   - **`.documents/`:** the notes are dated and additive only.
   - **The issue:** the board's row 08, the gate Margin, §5.10, the issue moved to §4 with its "Closed"
     line, and its row deleted from `.documents/OPEN_ISSUES.md` §1.19, with the count and the date
     corrected.
9. **The suites** at `<agent>`, run serially:
   - the counts the log gives, each at least the baselines;
   - ComputeTargets up by the new tests only;
   - `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it appears;
- the board's row 08 landed, the gate Margin held, and this note added to the list of orchestrator
  notes, marked "used for 08";
- under `### 5.10`, an *Orchestrator review of prompt 08* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Margin;
- the counts;
- ruling 1, the warnings' key, and why;
- the margins across M, the gap's generations, and the coverage, the parked segment's included;
- what does not change;
- the costs;
- anything opened or closed;
- that 09 is written next, from 08's "State handed to the next prompt" (README §2.1).
