# Orchestrator — prompt 05, unresolved turnarounds

Read [`../README.md`](../README.md) first: §5 (the rules), §8 in full (R6, R8, R10, P18, P20,
P22, P23 and 05's charter in §8.4). Then the prompt, log 04 §8 and the board. This note binds 05
only. The conventions of notes 02–04 (a tolerance is a number with its reason; timing under load;
the suites run serially) are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`05-unresolved-turnarounds.md`](../05-unresolved-turnarounds.md)
· **Gate:** "Unresolved" · **Closes:** nothing · **Opens:** only what the work finds · **Model:**
Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `0e61915` on 2026-10-29 at 16:30 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `0e61915` (prompt 05 written).
  `git status --porcelain --untracked-files=all` is empty; `git worktree list` shows the main
  checkout alone. No code has changed since 04's review `97a48d4`: `git diff --stat 97a48d4 0e61915`
  lists the prompt and the board only. `HEAD` is the commit that adds this note, which touches only
  `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 339 (96 + 243) / 103 / 115 / 64.
- The load averages were 4.6 / 8.9 / 8.9. Other workloads share the machine.

## 0. What makes this prompt unusual

**It adds a second reflection rule to the step loop that every history runs through, at α > 0
only, and must leave α = 0 and M = 0.5 M_P bitwise.** The orchestrator read at `0e61915` every
file the prompt names, and the planner's K = 3 probe records under
`var/probes/floor-cascade-2026-10-29b/`. It ran a probe from the session scratchpad
(`scratchpad/orch05/probe.py`): the assessment of prompt §1.2, with E = ρ_kin + V − V_floor and
04's `classical_turning_point`, hooked into a copy of the loop as the planner's `phi_t_scan.py`
was. In its rule mode every floor and unresolved reflection drains with w_gate = 1 (P20), and each
run stops at the planner's cut ρ_kin/ρ_R < 10⁻⁶. Line numbers are right for this tree. What it
found, where the prompt is silent, loose or wrong:

- **The prompt's B1 cannot fail item 5.** The planner's K = 10 shift (0.2902 against 0.2664 of
  the ceiling at β = 2, M = 10⁻⁹) came from the gate on floor-rule reflections, not from K. In
  `b2_M1e-9_t0_gate.json` and `…_K3.json` the first three approaches are floor-rule reflections at
  φ ≈ 4–5 φ_t, gated at w = 0.99998, 0.624 and 0.998. A K = 10 run judges them unresolved, so they
  drain with w = 1. P20 gives them w = 1 at every K. With P20 the orchestrator's probe gives
  Σ Δρ/ρ_R at β = 2, α = 0.1, to the cut:

  | M (M_P) | K = 3 | K = 10 | threshold 0 (resolved) |
  |---|---|---|---|
  | 10⁻⁹ | 0.012615975 (48 floor) | 0.012615975 (48 floor) | 0.012615965 (6 floor + 42 turning point) |
  | 3×10⁻¹⁰ | 0.012615975 (48 floor) | 0.012615975 (48 floor) | 0.012615975 (9 floor + 39 turning point) |

  At β = 3, M = 3×10⁻¹⁰, K = 3 and K = 10 give 0.00796117 each (58 floor). These sums are
  0.2902 of the ceiling 1/(6β² − 1) at β = 2 and 0.4219 at β = 3: the planner's K = 10 values,
  not its K = 3 ones. So under P20, K is invisible in Σ across the band. Ruling 3.
- **Item 5 holds by a wide margin.** At M = 10⁻⁹ the reflected and resolved runs differ in Σ by
  7.5×10⁻⁷ relative, with the same 48 wall approaches and 48 candidates. Both reach the cut at
  N ≈ 30.3493–30.3494. Each run took 5–7 s at a load of about 10. This is given so that a
  disagreement shows at once; the agent derives its own window and tolerance.
- **Item 3's histories exist.** At β = 2, α = 0.1 the first wall approach is judged unresolved
  before the floor rule fires at M = 10⁻⁸ (N = 20.352, φ/φ_t = 2.959) and at M = 10⁻⁷ (φ/φ_t =
  2.703). At M = 10⁻⁹ the first three approaches are floor-rule reflections at φ/φ_t ≥ 4.9. The
  reason is the floor rule's own scale: it fires when φ/|π| < h_floor/f = 10⁻¹⁰, and |π| ≤ √6 M_P,
  so near 3 φ_t it can fire first only where 3 φ_t ≲ 2.4×10⁻¹⁰ M_P.
- **The lower bound of item 3 is tight.** Over every unresolved reflection of the four rule runs,
  the smallest φ/φ_t was 2.70008. That bound comes from the cap. It holds only where the step
  before was capped by the velocity cap and φ_t did not change between the two step starts. As E
  falls, φ_t rises, so the bound can be crossed. Ruling 4.
- **The P1 harness at M = 10⁻¹⁰ keeps its floor reflection.** At α = 0.1, φ/φ_t ≥ 51.4 at every
  step start before it, as the prompt says.
- **The assessment never returned `None` and never raised**: 1723 assessments at M = 0.5 (β = 2,
  α = 0.1, to T_CMB, 3969 steps) and 5500–9600 per run in the band. Costs at a load of about 10
  were about 14 µs for H_E and 28–29 µs for `classical_turning_point`, about 3 % of the M = 0.5
  history's wall time.
- **ln 𝓡 > 0 occurs, far from any wall.** At M = 10⁻⁹ the first assessments of the history
  (N < 1, φ ≈ 5 M_P, T ≈ 2×10⁴ GeV) have ln 𝓡 up to +5.96 at φ/φ_t ≈ 8×10¹¹. That is the physics
  (𝓡 grows with E), and the rule cannot fire there, since it needs φ < K φ_t. The log should say
  where ln 𝓡 > 0 was seen, so that no one reads it as a defect.
- **A warning that carries a number is not "the same warning" twice.** The dissipation-mode line
  gives 𝓡 at the first impact. Each tuning integration changes the normalization, and so 𝓡, so
  "each distinct warning once" would print up to 30 lines. Ruling 1.
- **The natural-mode warning is lost on the histories it is for.** R6: "Warn and let the run
  fail". The prompt returns the warnings in `IntegrationResult`, which a history that raises never
  builds, and `_failure_payload` (`ScalarHistory.py:1851`) carries two keys. As written, a
  natural-mode history that fails never prints its warning. Ruling 2.
- **`Reflection` is built by keyword in a test that must pass unedited.**
  `test_surfing_turning_points.py:90` builds `SM.Reflection(N=…, phi_Einstein=…, pi_Einstein_in=…)`.
  A new field without a default breaks it. No other test, tool or driver builds `Reflection`,
  `Injection`, `IntegrationResult` or `TurningPoint`. Ruling 5.
- **`HubblePolicy` needs no ledger** (`ScalarHistory.py:529-536`: r_χ = 0.0 without one). So the
  natural-mode call works on `kcl.integrate`'s ledger-free policy.
- **Two consequences of P23 as accepted, which change nothing here:**
  - every dissipation-mode history above M ≈ 4×10⁻¹⁶ M_P warns "nearly elastic", U10's
    M = 10⁻¹² point included. At the first impacts of the band, log₁₀ 𝓡 ≈ −28 at M = 10⁻⁸;
  - `"reflections"` is also `REFLECTIONS_KEY` in `extra_metadata` (`ScalarHistory.py:2695`). It
    now counts unresolved reflections. No α > 0 history is stored before 09, so nothing stored
    changes. 09 decides. Record it in "Observations not acted on".

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The warnings are deduplicated by kind, not by text.** Each warning line begins with a fixed
   phrase, one per kind, held in module constants (the dissipation-mode "nearly elastic" line,
   the dissipation-mode "could not be assessed" line, the natural-mode "classical turning point
   below" line, the natural-mode "could not be assessed" line). A helper returns a line's kind.
   `tune_history` prints the first line of each kind it sees in a history, from the first
   integration that returns it, and never another of that kind. Item 7's `tune_history` test
   returns, on three integrations, lines of one kind with three different 𝓡, and asserts one
   printed line, the first. **B9** prints on every integration. **B10** deduplicates by the
   whole string. Each must fail that test. *STRUCTURALLY REQUIRED* (prompt §1.5's "distinct"
   against its own 𝓡 in the line).
2. **Warnings survive a failure.** In `integrate_scalar_history`, a `ComputationFailureError`
   raised after a warning has been collected carries the warnings collected so far as an
   attribute. Set it in the loop's own handler and re-raise the same exception, with
   `ComputeTargets/exceptions.py` unchanged. `integrate_history`'s failure path puts them in its
   payload under `"warnings"`, beside `"failure"` and `"failure_reason"`; `_failure_payload` may
   take them as an optional argument. `tune_history` prints them, by ruling 1, before its `!!`
   failure line. A test makes a natural-mode history fail after its warning: the P1 harness at
   α = 0 with a step budget a few steps past its floor reflection, through `integrate_history` or
   by driving the loop directly. It asserts that the warning reaches the failure payload and
   `tune_history`'s output. **B11** drops the warnings on failure and must fail it.
   *STRUCTURALLY REQUIRED* (R6).
3. **K is caught by item 3, not item 5.** R8's values are written as literals in the tests: a test
   asserts that the two constants equal 3 and 10⁻⁸ (in units of M_P). Item 3 asserts φ/φ_t < 3
   and the lower bound of ruling 4 with the literal 3, never through the constant. **B1** (K = 10)
   must fail item 3. Item 5 compares the rule with the resolved run and does not claim to catch K.
   §2.3 adds K = 10 against K = 3 at β = 2, M = 10⁻⁹ and 3×10⁻¹⁰ (the threshold patched to 10⁻⁸,
   K patched to 10) and reports the difference. The log says that under P20 the planner's K = 10
   shift is the floor rule's gate, citing the planner's records. *STRUCTURALLY REQUIRED* (prompt
   §1.6's B1 against the measurement in §0).
4. **Item 3's lower bound.**
   - The upper bound, φ < K φ_t with the record's φ_t, is the decision itself, and is asserted
     exactly.
   - The lower bound is asserted against (1 − f)·3·φ_t, with a slack derived from the change in
     φ_t between consecutive step starts on that infall. The slack is measured on the history,
     stated as a number in the log, and never loosened to pass.
   - If the step before was not capped by the velocity cap, the test says so and asserts the bound
     the step actually had.

   The log tabulates φ/φ_t at every unresolved reflection of §2.3 and §2.4, with the smallest.
   *IMPLEMENTATION CHOICE*, ruled here.
5. **Defaults on the new fields.** `Reflection`'s new field takes the namedtuple default `None`,
   so that `test_surfing_turning_points.py` passes unedited. The loop passes every field of every
   record explicitly, and a test asserts that no record of a real integration holds that default.
   The other records' new fields take no default. *STRUCTURALLY REQUIRED* (prompt §5: an edit to
   an existing test beyond §1.6 item 4 is a stop).
6. **The assessment's interface.** The module-level assessment takes the model, φ and ln ρ_kin,E,
   forms ln E = ln(ρ_kin + V − V_floor) itself from the model's `log_V` and `log_V_floor`, and
   returns (ln E, the `ClassicalTurningPoint` or `None`). The loop computes ln ρ_kin,E from H_E
   exactly as `candidate` does (`ScalarHistory.py:1081-1083`), through `policy` and `hubble`.
   Item 2's ln E test and **B4** then need no policy. The decision is a second module-level
   function on (φ, the `ClassicalTurningPoint`, the units' M_P), reading K and the threshold at
   call time. *IMPLEMENTATION CHOICE*, ruled here.
7. **A step whose assessment returns `None`** makes no decision, and the infall's latest assessment
   stands: a reflection made at that step records it (prompt §1.4's "the one made at its own step
   start" whenever that is not `None`). The log counts the `None` returns per history; the
   orchestrator's probe saw none. *IMPLEMENTATION CHOICE*, ruled here.
8. **The natural-mode model call** is made at the first floor-rule reflection of the integration
   only, whatever its outcome; there is no second call. It catches `ComputationFailureError` from
   the H_E computation and from the model, and nothing else: another exception type is a defect
   and propagates. **B7**'s patch raises `ComputationFailureError`. The wall-side `TurningPoint`
   test, which costs nothing, runs until a warning is made. *IMPLEMENTATION CHOICE*, ruled here.
9. **The printed line** is `!! tune_history (<task label>): ` followed by the warning, the prefix of
   `_tuning_failure`'s line (`ScalarHistory.py:2375`), with `flush=True`. *IMPLEMENTATION CHOICE*,
   ruled here.
10. **Item 5's window.** `integrate_history` takes no `N_stop`. Drive the window as the planner's
    probe did: wrap `SM.integrate_scalar_history` to pass `N_stop`, capture the result, and leave
    `integrate_history` by a private exception. Or call the loop on `integrate_history`'s initial
    state, if the test can build it without copying `integrate_history`'s code. The window ends
    before either run reaches ρ_kin/ρ_R < 10⁻⁶; the log gives that N for both. The two runs are
    made once, in `setUpClass`. *IMPLEMENTATION CHOICE*.
11. **§2.3 against the probe.** Under P20 the floor-rule reflections the planner gated now drain
    at w = 1, so 05's Σ at β = 2, M = 10⁻⁹ is expected near 0.2902 of the ceiling, not 0.2664.
    The table gives 05's value, the planner's "R only", K = 3 and K = 10 values, and the planner's
    count of w < 1 floor reflections. The log says which differences P20 explains, which the
    planner's kinetic E explains, and which its parking explains. The converged values of README
    §8.0 (0.115, 0.218 and 0.261 of the ceiling at β = 1.2, 2 and 3) were measured with gated floor
    reflections. §2.4 gives 05's beside them, and "Observations not acted on" records any change
    for 11's gate. *STRUCTURALLY REQUIRED* (prompt §2.3 asks for the comparison).
12. **No file but the prompt's §6 list changes.** In particular `CosmologyConcepts/Models/` (04's
    methods), `ComputeTargets/exceptions.py`, `ComputeTargets/Policies.py`,
    `ComputeTargets/dark_radiation.py`, `golden_history.py`, `golden_tuned_history.py`, the golden
    JSON, `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`,
    `test_wall_model.py` and every tool and driver are byte-identical. The new module may import
    helpers from `test_injection`, `test_gate_and_taper`, `test_kinematic_cap_loop` and
    `test_wall_model` without editing them.
13. **Timing and the suites.**
    - The cost per assessment is the minimum of at least 1000 calls each, with `uptime` beside it.
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
  2. The constants, the assessment and the decision, with item 2.
  3. The loop: the rule, the kinds, w_gate = 1, the records; items 1, 3, 4, 6 and 8.
  4. The warnings, their survival of a failure and the printing (rulings 1, 2, 8 and 9), with
     item 7.
  5. Item 5. The breakages B1–B11. The measurement of §2.
  6. The suites and `black --check`.
  7. The records: the log, `.documents/`, the board, and `.documents/OPEN_ISSUES.md` if anything
     was opened.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed. B1–B9 are the prompt's, in its order; B10 and B11 are ruling 1's and
  ruling 2's.
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

Done at `0e61915`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The probe** (`scratchpad/orch05/probe.py`), as in §0: M = 0.5 observed; the P1 harness at
   M = 10⁻¹⁰ observed; the rule at β = 2, M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 3×10⁻¹⁰}, threshold 0 at 10⁻⁹ and
   3×10⁻¹⁰, K = 10 at 10⁻⁹ and 3×10⁻¹⁰, and β = 3 at 3×10⁻¹⁰ (K = 3 and 10). It is not given to
   the agent as code; the agent writes its own.
3. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and logs 02–04;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/05-unresolved-turnarounds.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the constants, the assessment and decision functions with their signatures, and the records'
    new fields;
  - the energy argument and the order at a step start, in brief;
  - the warnings' kinds and texts, and the failure path;
  - the new tests by name, each with what it asserts and its tolerance;
  - item 3's history and why it fires first; item 5's window, its N and its tolerance;
  - the tables of §2;
  - the costs;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files (plus
   `.documents/OPEN_ISSUES.md` only if something was opened), and its parent is this note's commit
   or a later commit that is not the agent's. `git status --porcelain --untracked-files=all` is
   empty, and nothing is pushed. `git diff <dispatch> <agent>` on ruling 12's files is empty, and
   on `test_injection.py` and `test_gate_and_taper.py` touches item (8) only.
2. **α = 0.** The golden tests, `test_first_bounce`, `test_surfing_turning_points` and
   `test_kinematic_cap_loop` pass at `<agent>`, unedited. I read the α = 0 path through the loop:
   nothing new runs except the natural-mode warning's tests, and the one model call is at the first
   floor reflection, guarded.
3. **The loop** against prompt §1 and rulings 1–9, line by line:
   - the order at a step start; the assessment only at α > 0, π < 0, on a model with a wall;
   - the strict comparisons; the constants read at call time;
   - w_gate = 1.0 exactly at every floor and unresolved reflection, with `gate_weight` not called
     there and A_k still closed;
   - each record's new fields and which assessment it holds; the drain point's capture; the
     resets at a plateau turnaround and a reflection;
   - the warnings at the first candidate; the failure path; `tune_history`'s deduplication;
     nothing printed by the loop or `integrate_history`.
4. **An independent probe** of my own (session scratchpad, not committed), with `probe.py` adapted
   to the agent's code:
   - M = 10⁻⁸, β = 2: the first unresolved reflection's N and φ/φ_t;
   - M = 10⁻⁹: Σ for the rule against the resolved run, and K = 10;
   - M = 0.5, β = 2, α = 0.1: the pre-05 fields bitwise against the dispatch commit, in a
     temporary worktree that I remove;
   - the natural-mode warning on the P1 harness, and on a failing history.

   I compare each with the log.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B1, B3, B5, B9, B10 and B11 in particular.
6. **The numerics of the tests.** Each tolerance against its stated derivation; item 3's slack
   (ruling 4); item 5's window and tolerance; the literals of ruling 3.
7. **The measurement.** The tables of §2 are complete. §2.3 sets the planner's values beside 05's,
   with the differences explained (ruling 11). §2.4 gives every failure with its reason, and
   the `None` counts.
8. **The records.** The log is in §5.1's form, with the energy argument, every deviation classed
   and §0's rulings among them. The `.documents/` notes are dated and additive only. The board
   (row 05, gate Unresolved, §5.6) and `.documents/OPEN_ISSUES.md`, if anything was opened, agree.
9. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
   with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 05 landed, the gate Unresolved held, and this note added to the list of
  orchestrator notes, marked "used for 05";
- under `### 5.6`, an *Orchestrator review of prompt 05* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Unresolved;
- the counts;
- item 5's agreement, and the finding that under P20 K no longer shows in Σ;
- 05's drain beside README §8.0's converged values;
- how the small-M histories fail, for 06;
- the costs;
- anything opened;
- that 06 is written next, from 05's "State handed to the next prompt" (README §2.1).
