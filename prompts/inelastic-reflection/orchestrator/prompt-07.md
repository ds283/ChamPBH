# Orchestrator — prompt 07, unparking

Read [`../README.md`](../README.md) first: §5 (the rules), §8 in full (R14–R17, P18, P21, P22, P24,
07's and 08's charters in §8.4, §8.5). Then the prompt, log 06 (§1.1, §1.3, §5.2, §5.3, §7, §8), the
probes of [`source/planning-probes-2026-10-29d/`](../source/planning-probes-2026-10-29d/README.md) and
the board. This note binds 07 only. The conventions of notes 02–06 (a tolerance is a number with its
reason; timing under load; the suites run serially) are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07-unparking.md`](../07-unparking.md)
· **Gate:** "Release" · **Closes:** nothing · **Narrows:**
`[00-settling-at-physical-M-needs-a-parked-tracking-model]` · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `7f80303` on 2026-10-30 at 01:00 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `7f80303` (prompt 07, after 06 landed
  `e967cb7` and was reviewed `7b75ae7`). `git status --porcelain --untracked-files=all` is empty;
  `git worktree list` shows the main checkout alone. No code has changed since 06 landed:
  `git diff --stat e967cb7 7f80303` lists files under `prompts/` only. So the prompt's line numbers
  (checked: `:226`, `:277`, `:324`, `:375`, `:1621`, `:2020`, `:2062`, `:2165`, `:2314`, `:2452`,
  `:3159`) and the probes of `7b75ae7` hold for this tree. `HEAD` is the commit that adds this note,
  which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- SciPy is 1.17.0.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 408 (96 + 312) / 103 / 115 / 64.
- The load averages were 13.6 / 17.0 / 17.4. Other workloads share the machine.

## 0. What makes this prompt unusual

**It turns the one failure that 06 built into the parked phase into a hand-back to the oscillating
loop, and must leave every history that does not release bitwise 06's.** The prompt was written from
the orchestrator's probes at `7b75ae7`, and the code has not changed since. For this note the
orchestrator read at `7f80303` the loop's state (`ScalarHistory.py:1518-1631`), `candidate` and
`drain` (`:1671-1798`), `observe_turning_points` and `handle_turning_point` (`:1800-1975`),
`run_parked_phase` and `cut` (`:2020-2238`), the loop (`:2240-2493`), the payload (`:3150-3160`),
`test_parking.py`'s `TestAtHalfThePlanckMass` (`:854-925`), SciPy 1.17.0's Radau and DOP853 dense
outputs, and the probes. What it found, where the prompt is silent, loose or wrong:

- **Continuity at N_rel is exact by construction, if the released state is the segment's scalar
  value.** SciPy's `RadauDenseOutput` at its left node computes x = 0, p = 0 and returns
  `0 + y_old`, so the first Radau segment returns the state it was built from, bitwise; DOP853's
  returns `y_old` likewise (log 06 §1.3). With `alt_segment=True` the history's dense output at the
  node N_rel selects the Radau segment. So item 4's three floats agree if, and only if, the state
  handed to the new Radau is `segment(N_rel)` by a scalar call and `ts` holds the same float N_rel.
  The probe found exactly this (`continuity.out`). Ruling 3.
- **B4 cannot be seen at the first candidate after the release.** The gate's carry-over at the
  switch is `running_max`: the samples folded since the wall-side turning point that armed the cut
  candidate, which no candidate reads (the drain point uses `armed.nonadiabaticity`, closed at that
  turning point). After a release `boundary_N` lies before N_rel, so every later sample is folded
  anyway; what B4 changes is only the A_k of the **first turning point** after the release. At
  β = 5 the field is released with π_min > 0 (`probe_release.out`: π = 0.001125 M_P), so that first
  turning point is a plateau, which arms nothing, and the first candidate's A_k comes from the
  half-oscillation after it. And where m/H has fallen to 10, the post-release samples are likely to
  dominate a maximum anyway. Prompt §1.6 item 5's third bullet, as written, therefore passes under
  B4. Ruling 6.
- **Only the drain-point branch releases on a real history.** All four released histories switch at
  a drain point (log 06 §5.2, §5.3). The histories that switch at a reflection (M ≤ 10⁻⁷) have
  m/H ≥ 72 throughout the parked phase and never release. So the reflection branch's hand-back
  (`:2314-2320`), which the probe patched separately, is exercised by no test the prompt lists.
  Ruling 7.
- **B3 needs a candidate that would park.** The spy of item 5 sees no call after the release if no
  post-release candidate passes the ratio test, with or without P24. The probe found ρ_kin/ρ_R ≈
  10⁻⁷ at every post-release candidate at β = 5, and m/H back above 10 within about an e-fold, so
  the assertion is not vacuous there; the test must show that it is not. Ruling 5.
- **`check` folds the failing value into the minimum before it decides.** At `:2066-2068` the
  smallest m/H is updated before `tracker_adiabatic` is tested, so 06's code would put the failing
  check's m/H (8.94 at β = 5) into `Parking.min_m_eff_over_H`. Prompt §1.2 says it is not part of the
  parked phase. Ruling 4.
- **The reference N of item 2 is printed to seven digits.** Log 06 §5.2 gives the parked history's
  crossing as 40.25454 (the probe: 40.254538); the root's tolerance is near 10⁻¹² in N. A slack
  "derived from the root's tolerance" alone would fail on the reference's rounding. Ruling 8.
- **Two of item 8's ends are not bitwise the full history's release.** A `T_final` between the
  release and the step end gives the termination root inside the same DOP853 step, and the release
  root is then sought on `[t_old, N_end]` rather than `[t_old, N_check]`: the same function, a
  different bracket, so brentq returns a different float within its tolerance. An `N_stop` inside
  the release step truncates that DOP853 step itself, so the segment differs. Only an `N_stop` past
  the release step's end reproduces the full history's N_rel bitwise. Ruling 8.
- **The lag test may not be able to meet both of its conditions.** The per-oscillation means are a
  residue of oscillations whose half-swing is 23–75 |δ₁| (`lag.out`), and the measured ratios run to
  1.19 at β = 5 and 1.36 at β = 10. A tolerance derived from the half-swing and the non-closure of
  one oscillation must come out between those ratios and a factor 2. If the derivation gives a bound
  that cannot fail a factor 2, the prompt's two requirements conflict. Ruling 9.
- **The four released histories fail at the dispatch commit.** So §2.5's "up to the release step"
  against the dispatch commit has no `IntegrationResult` to compare in full. But the dispatch
  commit's failure reason prints the N and m/H of the failing check (`.10g`, `.5g`), and the release
  record holds the same two numbers from the same parked step. Ruling 10.
- **The left node of the release step is a different evaluation from the check that passed there.**
  The check at a step end evaluates `parked_tracker` from the previous step's anchor; the next
  segment evaluates its left node from the new anchor, the φ_min that check produced. The two φ_min
  can differ by the root's tolerance, and so can m/H. The switch's own node is the exception (log 06
  D3: bitwise). Ruling 2.
- **Nothing else in the suites releases.** The α > 0 histories at M = 0.5 outside `test_parking` run
  with ε = 0.0 (`test_injection` 11, `test_gate_and_taper` 9) or to windows before N = 40.25
  (`TestAtHalfThePlanckMass.window5` ends at N = 40.0). The only test that changes is item 9's.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The hand-back is a return value.** `run_parked_phase` returns either the history's end, as
   now, or a release record with its state; `cut` returns to the loop a value that both branches
   (`:2314` and `:2452`) tell apart. No exception carries the release, not even one caught before
   the loop's `try` sees it. The loop then:
   - builds `new_solver(N_rel, y_rel)` from the released state, and in the drain-point branch makes
     no restart from `y_drained`;
   - clears `armed`, `armed_assessment` and `infall_assessment` (the first two are cleared at the
     switch, and the third is None there; set all three regardless);
   - sets `running_max = None` and `boundary_N = N_rel`;
   - goes on.

   *IMPLEMENTATION CHOICE*, ruled here.
2. **The root.** g(N) = `tracker_m_eff_over_H(policy, hubble, N, segment(N))[0]` − the threshold,
   with `segment` the step's `ParkedSegment` called on scalars only, so the root's state and the
   released state are the same evaluation. The bracket is [the step's left node, the N where the
   check failed]. g at the left node is evaluated on the segment, not taken from the check:
   - g(left) > 0: brentq. Its tolerances are the agent's; the log derives the tolerance on m/H at
     the root from them, from |dg/dN| there, and from g's own noise (φ_min's root tolerance through
     V″ and H_E);
   - g(left) ≤ 0: the release is at the left node, the step's segment is not kept (no zero-length
     segment), and the step is still counted in `accepted_steps` and `parked_steps`. The log says
     whether this ever arose on §2's histories. It is not tested on a real history.

   On the step that ends the history at `log_T_stop`, the release replaces the node `N_end` already
   appended; it is never appended twice. *IMPLEMENTATION CHOICE*, ruled here.
3. **The released state is `segment(N_rel)`, one scalar call, and `ts[-1]` is that same float
   N_rel.** Nothing recomputes the state from the reduced interpolant, from `parked_tracker` with
   another anchor, or by an array call. Item 4 asserts the three floats (last parked segment, first
   Radau segment, dense output) bitwise, and H_E either side bitwise. *STRUCTURALLY REQUIRED*
   (§0, first point).
4. **The minimum and the failing check.** In a released history `Parking.min_m_eff_over_H` is the
   minimum of the switch's value, the checks that passed and the root's value; the failing check's
   m/H is in the release record, not in the minimum. Item 2 asserts `min_m_eff_over_H` equal to the
   release record's m/H at the root, bitwise, unless an earlier check is smaller (the log says
   which). In a history that does not release, the minimum is 06's, bitwise. *STRUCTURALLY REQUIRED*
   (prompt §1.2 against `:2066-2068`).
5. **P24 in the loop.** After a release, no candidate calls `parking_ratio_test` or `cut_tracker`.
   The ratio may still be formed for the guard's reason. The guard runs at every reflection; its
   reason after a release says `m_eff/H_E = not computed (the field was released at N = …)`. Item 5's
   spies are on both module functions, through the module's name. Item 5 also computes, in the
   test, for each post-release candidate: the ratio from its `log_rho_kin` and the state, and the
   tracker's m/H at its post-candidate state (through `cut_tracker` called by the test, after the
   spy is removed). It asserts that at least one candidate would have passed both, so that the
   count of zero is not vacuous. B3 must fail it. *IMPLEMENTATION CHOICE*, ruled here.
6. **B4 is caught at the first turning point after the release, by a sentinel.** Item 5's third
   bullet becomes: the first **turning point** after the release, and the first candidate, carry
   A_k from samples after the release only. The test shows it by a patch of `step_nonadiabaticity`
   (looked up through the module's name) that replaces A at the samples with
   N_last_TP < N ≤ N_switch with a sentinel larger than any real A (for example 10³), where
   N_last_TP is the turning point before the switch. Those samples feed only the half-oscillation
   that the switch abandons, so with the reset the patched history is bitwise the unpatched one in
   every record (assert it); under B4 the first turning point after the release carries the
   sentinel. The run is a window to a little after the first post-release candidate (at β = 5,
   about N = 41.5). The log says whether B4 also shows on the unpatched history, and gives
   `running_max` and `boundary_N` at the switch. If the sentinel changes the history before the
   switch, stop and ask. *STRUCTURALLY REQUIRED* (§0, second point).
7. **The reflection branch is tested.** One added test: a history that switches at a reflection,
   M = 10⁻⁷ M_P, β = 1.2, α = 0.1 (log 06 §5.4: parks at #6, unresolved, m/H 1.52×10³ at the switch
   and 72.3 at its smallest), with `PARKING_ADIABATICITY_THRESHOLD` patched to a value between the
   two (for example 100), run to an `N_stop` a little after the release. It asserts: parked at a
   reflection-kind candidate; released, with m/H at the root equal to the patched threshold to item
   2's tolerance; continuity at N_rel as item 4; the history goes on in Radau segments to `N_stop`.
   A new breakage **B7**, the reflection branch's hand-back broken (for example the release taken as
   the end of the history, or the elastic restart made from `y_reflected` instead), must fail it. If
   no such patched history releases and runs on, the log says why, and the orchestrator is asked
   before another is chosen. *IMPLEMENTATION CHOICE*, ruled here.
8. **The reference N and the ends.**
   - Item 2: the release lies strictly inside the parked step on which 06 failed. Its N where the
     check failed equals 06's failure N, 40.29089 to the digits log 06 prints, and N_rel lies within
     5×10⁻⁶ (half a unit of the seventh digit of 40.25454) plus the root's tolerance of 40.25454.
     `BETA_5_CROSSING_N` may be imported from `test_parking`.
   - Item 8: `T_final` between the release and the step end, and `N_stop` inside the release step,
     each give N_rel equal to the full history's within the root's tolerance in N (the log states
     the bound and its source), not bitwise. A third end, `N_stop` past the release step's end and
     before the next oscillating step that drains, gives N_rel bitwise the full history's. The
     prompt's "N_stop just after the release" is either of the last two; the log says which, and
     there are both.

   *IMPLEMENTATION CHOICE*, ruled here.
9. **The lag test.** On the β = 5 integration of item 2, over the first eight oscillations after the
   release, wall-side turning point to wall-side turning point, as `lag.py` forms them. φ_min from
   `parked_tracker` on the history's own trace; δ₁ from the right-hand side's π′, linearised about
   the tracker, the test's own derivative grid stated with its spacing. The tolerance is derived per
   oscillation from quantities the test measures on that oscillation: its half-swing, and how far it
   is from closing (for example the change of φ − φ_min, or of the swing, from one wall-side turning
   point to the next). It must not be a constant read off `lag.out`. Three test-only checks
   (δ₁ × −1, × 2, × ½), applied in the test, must each fail it; they are recorded with the
   breakages. **If the derived tolerance cannot fail δ₁ × 2 on some oscillation, stop and ask**: the
   prompt's two requirements then conflict, and which gives way is the user's. β = 10 is measured
   (§2.4), not tested. *IMPLEMENTATION CHOICE*, ruled here, with the stop.

   **Amended 2026-10-30, at the user's word ("OK, go with (c). I am not sure this matters a
   lot."), after the agent stopped on this ruling.** The derived per-oscillation tolerance is the
   non-closure term plus the anharmonic shift from the half-swing, 1.9–3.7 |⟨δ₁⟩| on the first
   eight oscillations at β = 5. It cannot fail δ₁ × 2 or × ½. The test is therefore a
   sign-and-order test:
   - on every oscillation, ⟨φ − φ_min⟩ has the sign of ⟨δ₁⟩;
   - on every oscillation, |⟨φ − φ_min⟩ − ⟨δ₁⟩| is within that derived bound.

   δ₁ × −1 must fail it. × 2 and × ½ are recorded in the log as not catchable, with the bound
   beside them. The measured ratios (0.99–1.19 at β = 5, and β = 10's) are reported in §2.4 as a
   measurement. Neither the bound nor the ratios are fitted. *IMPLEMENTATION CHOICE*, ruled by the
   user.
10. **§2.5 against the dispatch commit**, in one temporary worktree of this note's commit, removed
    afterwards:
    - every history that does not release, whole, against the dispatch commit, bitwise: the records,
      the samples, the final state, `N_final`, `nfev`, `accepted_steps`, and `ts`;
    - each of the four released histories through a window ending inside the parked phase, on a
      parked step before the release step, against the same window at the dispatch commit, bitwise
      as above;
    - each released history's record against the dispatch commit's failure reason: the N where the
      check failed to `.10g` and its m/H to `.5g`, equal as printed.

    *IMPLEMENTATION CHOICE*, ruled here.
11. **The release record** is a namedtuple of floats and ints, appended last to `IntegrationResult`
    with its comment, `None` for a history not released, and `"release"` in the payload, not
    stored. Nothing callable in it: the payload crosses Ray. If a test that must pass unedited builds
    `IntegrationResult` positionally (none does at `7f80303`), the field takes the default `None`;
    otherwise no default. *IMPLEMENTATION CHOICE*, ruled here.
12. **Failure reasons.** 06's texts stay for every failure the parked phase can still raise. The
    adiabaticity failure's text is removed with its raise. A failure in the oscillating phase after a
    release keeps the oscillating phase's text unchanged; the log says that none of §2.2's histories
    raised one. *IMPLEMENTATION CHOICE*, ruled here.
13. **No file but the prompt's §6 list changes.** In particular `CosmologyConcepts/Models/`,
    `ComputeTargets/Policies.py`, `ComputeTargets/dark_radiation.py`,
    `ComputeTargets/exceptions.py`, `golden_history.py`, `golden_tuned_history.py`, the golden JSON,
    and every test module but `test_parking.py` (item 9's three places only) are byte-identical, as
    are the tools and drivers. The new module may import helpers from `test_parking`,
    `test_injection`, `test_gate_and_taper`, `test_kinematic_cap_loop`, `test_unresolved_turnarounds`
    and `test_wall_model` without editing them.
14. **Timing and the suites.**
    - The cost of the release (the root; the new Radau instance) is the minimum over at least 100
      repetitions each, with `uptime` beside it. §2.1's per-history costs are the minima of three
      rounds, the dispatch commit and the final tree back to back.
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output
      to a file in your own scratch directory, grep the verdict from it, and record `uptime` beside
      each.
    - A run that may pass 600 s (the second half, under this load) goes in the background; wait
      for its notification without polling.
    - §2's histories run at most four at a time, in the background. One that has not ended after 15
      minutes is stopped and recorded as such. The two tunings at β = 5 run one at a time.
    - A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. The parked phase's end: the root (ruling 2), the released state (ruling 3), the minimum
     (ruling 4), the release record (ruling 11).
  3. The hand-back in both branches and the reset (ruling 1); P24 and the guard's reason (ruling 5);
     the payload.
  4. The tests: items 2–8 of the prompt, rulings 5–9, and item 9's edit to `test_parking.py`.
  5. The breakages B1–B7 and ruling 9's three checks. The measurement of §2.
  6. The suites and `black --check`.
  7. The records: the log, `.documents/`, the board, the `integrator-remediation` board's entry, and
     `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file
  goes under the repository. §2's worktree of the dispatch commit is removed afterwards
  (`git worktree list` shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No golden
  JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §5, plus rulings 6, 7 and 9's, plus an existing
  test failing for a reason this note does not name. A stop goes to the user through the
  orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `7f80303`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **SciPy's Radau dense output** read at its left node (`RadauDenseOutput._call_impl`, 1.17.0).
   No other probe was run: the probes at `7b75ae7` hold for this tree (Gate).
3. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board, log 06 and the probes' README;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/07-unparking.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the release record's fields, and what `Parking`'s two fields now hold;
  - the order in a parked step with the release, the root and its tolerance on m/H, and the
    hand-back and what it resets;
  - the new tests by name, each with what it asserts and its tolerance;
  - the derivation of the lag test's tolerance, and the three checks of ruling 9;
  - the tables of §2;
  - each breakage, with the tests it failed and how;
  - the edit to `test_parking.py`;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files, and its parent is this
   note's commit or a later commit that is not the agent's. `git status --porcelain
   --untracked-files=all` is empty, and nothing is pushed. `git diff <dispatch> <agent>` on ruling
   13's files is empty; on `test_parking.py` it touches item 9's test, its class docstring and the
   module docstring's item (7) only.
2. **α = 0, and what does not release.** The golden tests, `test_first_bounce`,
   `test_surfing_turning_points`, `test_kinematic_cap_loop`, `test_unresolved_turnarounds`,
   `test_injection` and `test_gate_and_taper` pass at `<agent>`, unedited. I read the α = 0 path and
   the parked phase of a history that stays adiabatic: nothing new runs but the hand-back's branch
   test.
3. **The loop** against prompt §1 and rulings 1–5, 11 and 12, line by line:
   - the root: its bracket, the left-node rule, its tolerances, scalar calls only;
   - the released state and `ts[-1]`, the same floats; the termination step's node replaced, not
     appended;
   - the hand-back in both branches; no exception; the reset of the five variables; no restart from
     `y_drained`;
   - P24: no ratio test or tracker call after a release; the guard's reason;
   - the records: `Parking`'s two fields, the release record's fields, nothing callable, the payload
     key.
4. **An independent probe** of my own (session scratchpad, not committed):
   - M = 0.5, β = 5 to T_CMB: the release (N, T_J, m/H at the root, the failing check), continuity
     at N_rel bitwise, the post-release candidates, and against `probe_release.out`;
   - the root re-found by my own bisection on the segment, against the record's N_rel and the log's
     tolerance;
   - M = 10⁻³, β = 1.2 to T_CMB;
   - M = 0.5, β = 2 and M = 10⁻³⁰, β = 2 at the dispatch commit against `<agent>`, bitwise, in a
     temporary worktree that I remove.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B1, B3, B4, B5 and B7 in particular, and ruling 9's three checks.
6. **The numerics of the tests.** Each tolerance against its stated derivation: the root's tolerance
   on m/H and on N; item 2's window; item 8's bounds; the lag test's per-oscillation tolerance
   against the measured ratios and against a factor 2.
7. **The measurement.** The tables of §2 are complete. §2.2 has all four histories released inside
   the step on which 06 failed and at T_CMB, with the would-park counts. §2.3 and §2.4 against
   `compare.out` and `lag.out`. §2.5 says what is bitwise and how. §2.6 gives every tuning's
   integrations, residuals, park and release.
8. **The derivation.** The root's tolerance on m/H re-derived from the brentq settings and |dg/dN|,
   and the lag tolerance's derivation re-read against the test's code.
9. **The records.** The log is in §5.1's form, with the order, the root, the hand-back, the lag
   derivation, the breakages, the `test_parking.py` edit, every deviation classed and §0's rulings
   among them. The `.documents/` notes are dated and additive only, and give the four released
   histories. The board (row 07, gate Release, §5.8), the narrowing of
   `[00-settling-at-physical-M-needs-a-parked-tracking-model]` on this board, on the
   `integrator-remediation` board and in `.documents/OPEN_ISSUES.md` §1.20 agree, and say that Q on
   parked samples is 09's.
10. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
    with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log, the `integrator-remediation` board and
  `.documents/OPEN_ISSUES.md` wherever it appears;
- the board's row 07 landed, the gate Release held, and this note added to the list of orchestrator
  notes, marked "used for 07";
- under `### 5.8`, an *Orchestrator review of prompt 07* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Release;
- the counts;
- the four released histories: where each parks and is released, and that each reaches T_CMB;
- continuity at the release, P24 and the would-park counts;
- the lag against δ₁;
- released against never parked;
- the tuning on released histories;
- the costs;
- anything opened or narrowed;
- that 08 is written next, from 07's "State handed to the next prompt" (README §2.1), and that
  `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` is still the user's to rule before 08.
