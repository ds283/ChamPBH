# Orchestrator — prompt 06, parking

Read [`../README.md`](../README.md) first: §5 (the rules), §8 in full (R14–R17, P18, P19, P21, P22,
P24, 06's charter in §8.4, §8.5). Then the prompt, logs 04 and 05 §8, and the board. This note binds
06 only. The conventions of notes 02–05 (a tolerance is a number with its reason; timing under load;
the suites run serially) are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`06-parking.md`](../06-parking.md)
· **Gate:** "Park" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus, as
the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `2f24322` on 2026-10-29 at 19:40 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `2f24322` (the user's ruling R17,
  after prompt 06 `5346726` and 05's review `e1a1ac0`). `git status --porcelain
  --untracked-files=all` is empty; `git worktree list` shows the main checkout alone. No code has
  changed since 05 landed: `git diff --stat 9c2483b 2f24322` lists files under `prompts/` only. So
  the prompt's line numbers and the probes of
  [`source/planning-probes-2026-10-29c/`](../source/planning-probes-2026-10-29c/README.md) hold for
  this tree. `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 374 (96 + 278) / 103 / 115 / 64.
- The load averages were 5.7 / 5.7 / 5.8. Other workloads share the machine.

## 0. What makes this prompt unusual

**It adds a second phase, with a second solver, to the integration that every history runs
through, and must leave α = 0 bitwise and every α > 0 history bitwise 05's up to its switch.** The
prompt was written from the orchestrator's probes at `9c2483b`, and the code has not changed since.
For this note the orchestrator read at `2f24322` the loop (`ScalarHistory.py:970-1859`), its readers
(`:1878-2198`), `integrate_history` (`:2226-2522`), `_tuning_state`, 04's `minimum_on_trace` and
`d_minimum_d_log_trace` (`CosmologyConcepts/Models/AbstractModel.py:684-787`), the tests at α > 0,
and SciPy 1.17.0's `OdeSolution`. What it found, where the prompt is silent, loose or wrong:

- **SciPy's segments do not return the same floats for a scalar and an array call.** On a
  five-component test system (rtol 10⁻¹⁰, max step 0.1) the orchestrator compared each segment's
  scalar call with the same N in a ten-point array call: Radau differed at 1 of 38 320 points, RK45
  at 42 of 5 290, DOP853 at 0 of 1 220. The loop's own comment says the same
  (`ScalarHistory.py:1518-1519`). Prompt §1.3's "a scalar call and the same N in an array call
  return the same floats" therefore has to be built into the parked segment; no solver's
  interpolant gives it. Ruling 3.
- **B5 is invisible at small M.** For n = 1 at the minimum, V″/((ln A)′T) = 2/φ_min, so B5's extra
  (ln A)′ changes the denominator of `d_minimum_d_log_trace` by β φ_min/(2 M_P) relative. That is
  about 2×10⁻⁹ at M = 10⁻⁷ (φ_min ≈ 2×10⁻⁹ M_P), 10⁻²⁹ at M = 10⁻³⁰, and about 6×10⁻³ at M = 0.5,
  β = 2 (φ_min ≈ 6×10⁻³ M_P). A finite-difference test on small-M states cannot see it. Ruling 5.
- **B6 is nearly invisible in any integrated value at small M.** The reduced right-hand side reads
  π only through b π in d ln T/dN. In the probe, b M_P ≈ −5 and π_min ≈ 3×10⁻¹⁰ M_P at M = 10⁻⁷,
  so B6 moves d ln T/dN by about 10⁻⁹; at M = 10⁻³⁰, by about 10⁻²⁹. Item 3's self-consistency
  catches B6 only if it goes through the loop's own reduced right-hand side, not through `ODERHS`
  called by the test. Ruling 4.
- **A history that fails returns no `IntegrationResult`.** `integrate_history`'s failure payload
  carries `failure`, `failure_reason` and `warnings` (`ScalarHistory.py:2460-2468`). At M = 0.5,
  β = 5 the history parks and then fails; §2.2's "where it parks" and §2.6's "where each
  integration parked" have nothing to read but the reason. Ruling 9.
- **The leftover is on the candidate's own record.** The drain records `pi_after` and
  `H_Einstein_after` (`:1391-1411`); with α_eff = 0.0 it records `pi_after = sign·pi_before` and
  `H_Einstein_after = H_Einstein_before` (`:1342-1358`). So the drained state's ρ_kin,E is
  H_Einstein_after² pi_after²/2 in both cases, and the cut needs no second H_E call. Ruling 2.
- **The cut-disabled reference keeps the guard.** ε = 0.0 disables the cut, not P21. Log 05 §5.5's
  histories reach |π| = 7.2×10⁻¹⁰ M_P (β = 3, M = 10⁻¹²) before failing on G2, so a cut-disabled
  history run that deep meets the guard. The references of item 4 and §2.5 end inside the window
  that 05's history survives, where |π| ≥ 10⁻⁸ M_P in every case of log 05 §5.5. Ruling 8.
- **The payload crosses Ray.** `compute_scalar_history` is a Ray task, and `integrate_history`'s
  payload is returned through it. The payload holds no `OdeSolution` (checked); the parking
  record must not bring one, or any other callable, with it. Ruling 6.
- **At α = 0 there is no candidate**, so "the cut tested at α = 0" (B10) has no single obvious form,
  and a spy sees no call unless the ratio test passes somewhere. Ruling 10.
- **A failure inside the parked phase must carry the warnings.** Note 05's ruling 2 attaches them in
  the loop's `except ComputationFailureError` (`:1832-1837`). The parked phase runs inside that same
  `try`. Ruling 7.
- **R14's "only where the energy is all kinetic"** is carried by the charter (README §8.4) as "at a
  candidate". Log 05 §5.5 recorded (V − V_floor)/E up to 0.50 at unresolved reflections after the
  10⁻⁶ cut; §2.5's ε = 10⁻⁸ may put the cut there. Nothing changes; the measurement records it.
  Ruling 12.
- **Nothing else in the suites runs a parking history at α > 0**, besides the two classes of prompt
  §0.3. The other α > 0 histories are M = 0.5 at β = 2 (the cut's ratio passes at #27, m/H = 3.1, so
  it does not fire), the P1 harness at M = 10⁻¹⁰ to N = 21, and windows ending at N = 20.6 and 30.3,
  before the cut at N = 30.349. The suites confirm it (prompt §5).

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The decisions are module-level functions**, each reading its constant at call time, so that
   they can be tested on synthetic floats and patched (as 03's and 05's are):
   - **the ratio test**, on (the candidate's index in `injections`, the ratio as a float): False at
     index 0, else ratio < ε, strictly. The loop forms the ratio as
     exp(`log_rho_kin` − ln ρ_R,E) and passes the float. Item 2's edges are tested on floats: ε
     itself does not fire, `nextafter(ε, 0)` does, and index 0 never fires at any ratio.
   - **the tracker's adiabaticity**, on m_eff/H_E: True iff m/H ≥ the threshold. The cut's step 3
     and the parked phase's check call the same function: the cut parks only where it is True, and
     a parked step end where it is False fails the history. B8 removes the call from the cut; B11
     from the parked phase.
   - **the guard**, on (π_in, M_P): True iff |π_in| < the threshold × M_P, strictly.

   A loop-level test adds to item 2: a short window with ε and the adiabaticity threshold patched so
   that every candidate passes both (for example ε = `inf`, threshold 0.0) parks at index 1, never
   0. B1 must fail it as well as the synthetic test. *IMPLEMENTATION CHOICE*, ruled here.
2. **The order at a candidate**, at α > 0 on a model with a wall:
   1. the candidate as now (`candidate()`: its record, its drain if α_eff > 0.0);
   2. the ratio test on its `log_rho_kin` and the state's ln ρ_R,E (the drain does not change it);
   3. if it passes, the tracker at the post-candidate state: (φ, ln ρ_R,E, ln f_m, ln T_J), with π
      not used. Σ and f_m come from one `policy` call; ln T = ln ρ_R,E + ln(Σ + f_m), with
      Σ + f_m ≤ 0 or a non-finite ln T a `ComputationFailureError`. Then φ_min, π_min (§1.4), and
      m/H with the ledger after the drain and before the leftover. This m/H is the one the decision
      is made on, and the record's;
   4. if both hold, the switch: the leftover ΔK = exp(2 ln H_Einstein_after + 2 ln|pi_after| − ln 2
      + 4N), its Δρ/ρ_R,E likewise in logs, added to the ledger; then H_E at the parked state with
      the ledger after it; then the record; then the parked phase. `armed` and `armed_assessment`
      are cleared, as after any drain point;
   5. at a reflection where the cut did not fire, the guard (P21), after steps 1–3, so its reason
      can give m/H when step 3 ran.

   The cut runs whether or not the candidate drained (α_eff = 0.0 included); the leftover is then all
   of ρ_kin,E. *IMPLEMENTATION CHOICE*, ruled here.
3. **The parked segment builds its determinism.** For each N it evaluates the step's reduced
   interpolant by a scalar call and solves φ_min and π_min. An array input is a loop of these scalar
   evaluations, stacked into (5, n). `minimum_on_trace`'s start for every evaluation on a segment,
   and for every reduced right-hand-side evaluation of the step that made it, is the same float:
   the φ_min at the step's left node, carried from the previous step's end (the switch's φ_min for
   the first parked step). Item 6 asserts both properties bitwise, at points interior to segments
   and at nodes. *STRUCTURALLY REQUIRED* (prompt §1.3 against §0's measurement).
4. **The reduced right-hand side is a module-level callable** (for example a factory taking the
   loop's `fun`, the model, the cosmology and the anchor), returning the three derivatives from
   `fun` at (φ_min, π_min, the reduced state). The loop's parked solver calls it, and item 3's
   self-consistency calls it, at the same states. **B6** is applied inside it and must fail item 3.
   *STRUCTURALLY REQUIRED* (§0: otherwise B6 cannot fail any test).
5. **Item 3's synthetic states include large φ_min.** At least one state at M = 0.5 M_P (β = 2 and
   5, where §0 puts B5's effect at 6×10⁻³ and more), beside small-M states. The log gives, at each
   state, B5's relative change of π_min and the test's tolerance, and the tolerance is below the
   change at the large-M states. *IMPLEMENTATION CHOICE*, ruled here.
6. **The parking record** is a namedtuple of floats, ints and `None`, appended to
   `IntegrationResult` last with the default `None` only if a test that must pass unedited builds
   `IntegrationResult` (none does at `2f24322`; then no default). It holds prompt §1.5's fields and
   nothing callable. "The smallest m/H over the parked phase" is the minimum of the switch's value
   and every parked check (ruling 11). `"parking"` in the payload is the record or `None`.
   *IMPLEMENTATION CHOICE*, ruled here.
7. **The parked phase runs inside the loop's `try`**, so that its failures carry `warnings` (note 05,
   ruling 2). A `ComputationFailureError` from the reduced right-hand side, `minimum_on_trace`,
   `d_minimum_d_log_trace` or the π_min solve is the history's failure, with its reason: no step
   halving or retry, since the reduced system has no fast mode and every trial state lies within one
   step of an accepted one. `steps_rejected_by_exception` counts the oscillating phase only. The log
   says whether one was ever raised on §2's histories. *IMPLEMENTATION CHOICE*, ruled here.
8. **The references with the cut disabled.** Each is a window (`test_unresolved_turnarounds.window`,
   or the agent's equivalent) ending after the switch's N and before log 05 §5.5's failure N, at
   ε = 0.0 only. Item 4's at β = 2, M = 10⁻³⁰ ends at N = 30.9, as the probe's did. §2.5's windows
   are log 05's (29.5, 30.9, 29.6). If the guard fires in one, the log says where, and that window
   is shortened rather than the guard patched. Item 9 is the one place where the guard is meant to
   fire: with ε = 0.0, on a history that reaches |π_in| < 10⁻⁹ M_P, or with the threshold raised by a
   patch, as the prompt says. *IMPLEMENTATION CHOICE*, ruled here.
9. **The failure reasons name the switch.** A parked tracker's loss of adiabaticity gives N, T_J and
   m/H there, and the switch's N, T_J and candidate index. So does any other failure inside the
   parked phase. §2.2 and §2.6 read where a failed history parked from its reason, or from a window
   in the scratch script. Item 7's β = 5 test asserts the reason's kind, that the switch is at a
   drain point, and the N of the failure inside a window whose width the log derives from the
   parked and oscillating traces. It does not pin the probe's four digits, which were read off the
   oscillating history. *IMPLEMENTATION CHOICE*, ruled here.
10. **B10 is written concretely and must fail item 1.** For example, the α > 0 condition removed so
    that the ratio test runs at every floor reflection at α = 0 with ρ_kin from its own H_E. Item 1's
    spy (on the class, as 05's `model_spy`, on `minimum_on_trace` and `d_minimum_d_log_trace`) must
    see a call under B10. If the α = 0 histories never pass the ratio test, item 1 adds an α = 0 run
    with ε patched to `inf` and the threshold to 0.0, and the log says why. *STRUCTURALLY REQUIRED*
    (prompt §1.7's B10 against §0).
11. **The adiabaticity check** is made at the end of every accepted parked step on the tracker. On
    the step that ends the history it is made at N_final, on the interpolated state, never past it.
    H_E there is `HubblePolicy`'s at (φ_min, π_min) with the ledger as it stands, through `policy`,
    never `fun`. *IMPLEMENTATION CHOICE*, ruled here.
12. **§2.4 and §2.5 add a column**: (V(φ) − V_floor)/ρ_kin,E at the cut candidate's state, computed
    in the scratch script from the model's `log_V` and `log_V_floor`. It is R14's "energy all
    kinetic" condition, measured. The log reports it, and the largest value, with no threshold set.
    *IMPLEMENTATION CHOICE*, ruled here.
13. **"The oscillation it replaces"** (prompt §2.4, §5) is the dense output from the previous
    candidate's N to the switch's N, sampled at no fewer than 4 000 points, as the probe did. The
    range of ln A over it is set against Δ ln A at the switch. *IMPLEMENTATION CHOICE*, ruled here.
14. **The two setup changes** (prompt §1.7 item 10) wrap `integrate_captured` in a patch of ε to
    0.0. Each class docstring may gain one sentence giving the reason, citing this prompt. Nothing
    else in either file changes. *IMPLEMENTATION CHOICE*, ruled here.
15. **§2.2 against the dispatch commit**, in one temporary worktree of `2f24322` (or this note's
    commit) that is removed afterwards:
    - every history whole at ε = 0.0, against the dispatch commit, bitwise: the records, the
      samples and the final state;
    - where it parks, the run with the cut on, up to the switch: through a window that ends after
      the switch's N and before any failure, its pre-06 fields bitwise against the dispatch
      commit's on the candidates and steps before the switch.

    *IMPLEMENTATION CHOICE*, ruled here.
16. **No file but the prompt's §6 list changes.** In particular `CosmologyConcepts/Models/`
    (04's methods), `ComputeTargets/Policies.py`, `ComputeTargets/dark_radiation.py`,
    `ComputeTargets/exceptions.py`, `golden_history.py`, `golden_tuned_history.py`, the golden JSON,
    `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`,
    `test_unresolved_turnarounds.py`, `test_wall_model.py`, and every tool and driver are
    byte-identical. The new module may import helpers from `test_injection`, `test_gate_and_taper`,
    `test_kinematic_cap_loop`, `test_unresolved_turnarounds` and `test_wall_model` without editing
    them.
17. **Timing and the suites.**
    - The cost per cut test, per parked right-hand side and per parked step is the minimum of at
      least 1000 calls each (the step: of every parked step of one history), with `uptime` beside
      it.
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output
      to a file in your own scratch directory, grep the verdict from it, and record `uptime` beside
      each.
    - A run that may pass 600 s (the second half, under this load) goes in the background; wait
      for its notification without polling.
    - §2.4's 18 histories run at most four at a time, in the background. One that has not ended
      after 15 minutes is stopped and recorded as such.
    - A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. The constants and the three decisions (ruling 1), with item 2's synthetic tests.
  3. The tracker: π_min (§1.4) and its derivation, φ_min, m/H; item 3, with rulings 4 and 5.
  4. The cut and the switch in the loop (ruling 2), the record and the payload; items 1, 2's loop
     test, 4 and 5.
  5. The parked phase: the reduced right-hand side, the solver, the segment, the end, the check;
     items 6, 7 and 8. The guard, item 9. The two setup changes, item 10.
  6. The breakages B1–B11. The measurement of §2.
  7. The suites and `black --check`.
  8. The records: the log, `.documents/`, the board, and `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed. B5's diff is to `CosmologyConcepts/Models/AbstractModel.py`; that is
  expected, and it is reverted like the others.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file
  goes under the repository. §2's worktree of the dispatch commit is removed afterwards
  (`git worktree list` shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No golden
  JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §5, plus an existing test failing for a
  reason this note does not name. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `2f24322`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The scalar-against-array comparison** of §0, on a synthetic system in the session scratchpad.
   No other probe was run: the probes at `9c2483b` hold for this tree (Gate).
3. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and logs 04 and 05;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/06-parking.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the constants, the decision functions, the tracker's and the reduced right-hand side's
    signatures, and the parking record's fields;
  - the derivation of π_min and the order at a candidate, in brief;
  - the solver chosen for the parked phase, why, and its cost;
  - the parked segment's interface, and how it is made deterministic;
  - the failure reasons of the parked phase;
  - the new tests by name, each with what it asserts and its tolerance;
  - the tables of §2;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files, and its parent is this
   note's commit or a later commit that is not the agent's. `git status --porcelain
   --untracked-files=all` is empty, and nothing is pushed. `git diff <dispatch> <agent>` on ruling
   16's files is empty; on `test_injection.py` and `test_gate_and_taper.py` it touches one class's
   setup each, and its docstring by at most a sentence.
2. **α = 0.** The golden tests, `test_first_bounce`, `test_surfing_turning_points`,
   `test_kinematic_cap_loop` and `test_unresolved_turnarounds` pass at `<agent>`, unedited. I read
   the α = 0 path through the loop: nothing new runs.
3. **The loop** against prompt §1 and rulings 1–11, line by line:
   - the order at a candidate (ruling 2); the strict comparisons; the constants read at call time;
   - the leftover from the record, in logs; the ledger's two entries at one N;
   - the switch's state, the record's fields, the payload key, nothing callable in the record;
   - the parked loop: the bound, the end and its root, `N_stop`, the failsafe, the step budget over
     both phases, `nfev` through `fun`, the check at every step end and at N_final;
   - the segment: scalar evaluations, the fixed anchor, the (5,) and (5, n) shapes, `alt_segment`
     at the switch's node;
   - the failures inside the `try`, with the switch in their reasons.
4. **An independent probe** of my own (session scratchpad, not committed):
   - β = 2, M = 10⁻³⁰, α = 0.1 to T_CMB: the switch (candidate, N, φ_min, π_min, m/H), H_E across it,
     K after it, the final state, and the fixed-T values;
   - the parked segment called at 100 random N in shuffled order, scalar and array, bitwise;
   - φ on the parked samples against `minimum_on_trace` from a start of my own, and π against my own
     π_min, written from the derivation in the log;
   - M = 0.5, β = 5: where it parks and where it fails;
   - M = 0.5, β = 2 at the dispatch commit against `<agent>`, bitwise, in a temporary worktree that I
     remove.

   I compare each with the log and with the probes at `9c2483b`.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B1, B4, B5, B6, B7, B10 and B11 in particular.
6. **The numerics of the tests.** Each tolerance against its stated derivation: item 3's
   finite-difference tolerance and B5's change at the large-M states (ruling 5); item 5's H_E bound
   against B3's shift; item 6's root tolerance; item 7's window.
7. **The measurement.** The tables of §2 are complete. §2.4 has every history at T_CMB and parked,
   with ruling 12's column. §2.5 sets the implementation beside `probe_cut.out`. §2.6 gives every
   tuning's integrations and residuals. §2.2 says what is bitwise and where each history parks or
   fails.
8. **The derivation.** π_min = s a/(1 − s b) re-derived from `ODERHS` (`ScalarHistory.py:663-675`):
   a = Σ − 4 + q, b = (Σ + q)(ln A)′, q = [3w′/D + f_m(1 − Σ)]/(Σ + f_m), D = 1 + G_s′/(3 G_s),
   against the code.
9. **The records.** The log is in §5.1's form, with the derivation, the order, the solver, every
   deviation classed and §0's rulings among them. The `.documents/` notes are dated and additive
   only, and carry R17's large-M behaviour. The board (row 06, gate Park, §5.7), the narrowing of
   `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` and `.documents/OPEN_ISSUES.md`
   agree.
10. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
    with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 06 landed, the gate Park held, and this note added to the list of orchestrator
  notes, marked "used for 06";
- under `### 5.7`, an *Orchestrator review of prompt 06* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Park;
- the counts;
- where the small-M histories park, and that they reach T_CMB;
- the parked against the oscillating history, and the three ε at β = 1.2, M = 10⁻⁷ (an input to 11);
- the large-M histories that park, where, and the ones that then fail (07's);
- the tuning at small M;
- the costs;
- anything opened;
- that 07 is written next, from 06's "State handed to the next prompt" (README §2.1).
