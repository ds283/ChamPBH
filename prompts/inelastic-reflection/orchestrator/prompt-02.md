# Orchestrator — prompt 02, injection at turning points

Read [`../README.md`](../README.md) first: §0.2, §1, §2.2 (02's and 03's charters), §2.3 (the gate
"Inject"), §4, §5, §6.1 U4 as ruled (b′) and §6.2 P1, P3, P4, P5, P11 and P12. Then the prompt, log
01 §7 and the board. This note binds 02 only. Note 01's rulings 8, 10 and 11 (a tolerance is a
number with its reason; timing under load; the suites run serially) are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-injection-at-turning-points.md`](../02-injection-at-turning-points.md)
· **Gate:** "Inject" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus,
as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `63918a4` on 2026-10-28 at 10:11 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `63918a4` (prompt 02 written), after
  the prompt's own gate `b30f7bd`. `git diff b30f7bd 63918a4` touches only `prompts/`.
  `git status --porcelain --untracked-files=all` is empty; `git worktree list` shows the main
  checkout alone. `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 288 (96 + 192) / 103 / 115 / 64.
- The load averages were 12.3 / 12.8 / 13.2. Other workloads share the machine.

## 0. What makes this prompt unusual

**It changes the step loop every history is integrated with, under a bitwise regression, and
adds a restart that the loop has made until now only at a floor reflection.** The orchestrator
read at `63918a4` every file the prompt names. Its line numbers are right for this tree. What it
found, where the prompt is silent, loose or wrong:

- **Where π is read for the classification.** `first_bounce` reads π as `interpolant(ts[k])[1]`
  and `interpolant(ts[k + 1])[1]` (`ComputeTargets/ScalarHistory.py:888`), and on the last step
  `ts[k + 1]` is N_final, not the solver's t. Radau's dense output returns `y_old` exactly at its
  step's start (x = 0, so `Q·p` is zero) but `y_old + Q·1` at its end, which is `solver.y` only to
  rounding. A classification read from `solver.y` could disagree with `first_bounce` in the sign
  of a π that is zero to rounding. Ruling 1.
- **Radau's π′.** In SciPy 1.17.0 `Radau._step_impl` sets `self.f = fun(t_new, y_new)` on
  acceptance, so `solver.f[1]` after a step is π′ at the accepted state, computed with the ledger
  as it stands. The kinematic cap already reads it (`:727`). Ruling 2.
- **The order in the loop.** The step's interpolant holds the state before any drain, and must be
  appended before the restart. The step that ends the history (the termination branch, `:781-804`,
  or `solver.status == "finished"` under `N_stop`, `:809-813`) must never drain. In the termination
  branch with `g_low == 0.0` (`:796-798`) the last step is not appended at all, so `first_bounce`
  never reads it. Ruling 3.
- **The restart count.** P5 says an injection's restart is counted apart from a reflection's.
  `ScalarFieldIntegrationSupervisor.notify_reflection` (`Quadrature/supervisors/ScalarField.py:207`)
  feeds the reflections' record, and the prompt's file list does not include the supervisor. The
  records count the restarts already. Ruling 4.
- **Which ledger is drained.** `integrate_scalar_history` takes `policy` apart from `RHS`, and
  defaults it to `RHS.policy` (`:662`). In `integrate_history` the two hold one ledger, but a
  caller could pass a `policy` whose ledger is not the one the RHS reads, and the drain would then
  change π without changing the expansion. Ruling 5.
- **`alt_segment=True` at α = 0.** SciPy's `OdeSolution` picks the segment by
  `searchsorted(ts, t, side)`. At `ts[0]` and `ts[-1]` both sides give the same segment (the index
  is clamped), so `solution(N_final)` and the sample at z = 0 are unchanged. At an interior node
  t_k the right side returns interpolant k's start, which is `y_old` exactly: at α = 0 that is the
  previous step's `solver.y`, not interpolant k − 1's end value, a rounding difference. So a sample
  whose N_forward equals an interior node bitwise would change. The orchestrator expects none in
  the golden histories, but this is the one way `alt_segment=True` can break them. It also means
  that at a drain node `result.solution(N_j)` is the drained state *exactly*. Ruling 6.
- **`fixed_T_value_at` and an end-of-step crossing.** `_T_Jordan_crossing_step` returns
  `(k, t_high)` when ln T_J − log_T is exactly zero at a step's end (`:1078-1079`), and
  `fixed_T_value_at` then reads `interpolants[k]` at t_high (`:1130`). At a drain node that pairs the
  pre-drain state with the post-drain K. The prompt's §1.4 says the right-continuous dense output
  makes this consistent; for this exact-equality edge it does not. Ruling 7.
- **The golden histories** are at M = 10⁻³ M_P, β = 1.5 and 5.5 (`ComputeTargets/tests/golden_history.py`),
  with no reflections and a turning point as the first bounce (`golden_history_05.json`: 1 233 and
  26 331 accepted steps). So item 1's comparison with `first_bounce` is on a real turning point, and
  B8 restarts at least once on each. The tuned golden history is β = 3 at M = 0.01 M_P.
- **The H_E bound.** `HubblePolicy`'s log branch rebuilds H² as V/(3M_P² · V/3H²M_P²), with
  V/3H²M_P² = G·T and G = 1 − π²/6M_P². The drain changes G by απ²/6M_P² and the denominator of T
  by the matching ρ_χ. The rounding in H² is then of order a few ε times the condition of G,
  (π²/6M_P²)/G, which grows towards kinetic domination. Ruling 8.
- **The planner's probe counts non-strictly at a step's end.** Its test is
  `pa < 0 <= pb or (pa < 0 and pb > 0)` (`source/planning-probes-2026-10-28/turning_points.py:35`),
  so π exactly zero at a step's end counts as wall-side there and as nothing under `first_bounce`'s
  convention. Ruling 9.
- **The load.** The machine is at a load of about 12. Timings taken now compare only with timings
  taken beside them.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The classification reads the interpolant at the nodes `first_bounce` reads**: π at
   `ts[k]` and `ts[k + 1]` of the appended step, with `ts[k + 1]` = N_final on the last step, the
   strict inequalities, and the root by `brentq(lambda t: interpolant(t)[1], t_low, t_high,
   xtol=1e-15)` with the same two floats. φ and ln T_J are read from the interpolant at the root.
   Detection runs on every appended step, the last included, and on no step that is not appended.
   *STRUCTURALLY REQUIRED* (item 1's bitwise equality).
2. **The drain-point test reads the accepted state**: `solver.y[1] > 0.0` and `solver.f[1] <= 0.0`,
   after the step and before any restart. That is the state that is drained. If the turning point's
   own step ends with the interpolant's π > 0 but `solver.y[1] <= 0.0` (rounding), the drain stays
   armed. *IMPLEMENTATION CHOICE*, ruled here.
3. **The order after an accepted step** is today's up to the append, then: classify the appended
   step; if it is the history's last (termination, or `finished` under `N_stop`), end with no
   drain; otherwise drain if one is due, then restart. The drain never touches an interpolant
   already appended. *STRUCTURALLY REQUIRED*.
4. **No change to `Quadrature/supervisors/`.** `notify_reflection` is called for floor events only,
   as today. The restarts are counted from the records: the floor reflections plus the injections
   of kind `"turning_point"`. The log's measurement reports both. *IMPLEMENTATION CHOICE*.
5. **At α > 0 the ledger drained is the one the RHS reads.** `policy.dark_radiation` is not `None`,
   and when `RHS` has a `policy` attribute, `RHS.policy.dark_radiation is policy.dark_radiation`.
   Either failing is a `ValueError` at entry, before the first solver is built. At α = 0 neither is
   checked. *IMPLEMENTATION CHOICE*, ruled here.
6. **`alt_segment=True` stays**, as the prompt says. Item 6 of the tests asserts that
   `result.solution(N_j)` equals the drained state **bitwise**, π included, not to rounding: it is
   interpolant k + 1 at x = 0. If a golden history is not bitwise at α = 0, find whether a sample
   sits on an interior node before anything else, report it, and **stop and ask**.
   *STRUCTURALLY REQUIRED*.
7. **A temperature crossing exactly at a drain node is recorded, not fixed.** The log puts it
   under "Observations not acted on": `_T_Jordan_crossing_step`'s `(k, t_high)` case at a drain node
   pairs the pre-drain state with the post-drain K. It needs ln T_J − log_T to be exactly 0.0 at a
   drain point. No issue is opened, and `fixed_T_value_at` is not changed.
8. **Item 4's bound carries the condition of G.** The test computes, for each injection,
   |H_after − H_before|/H_before and the factor c = (π_before²/6M_P²)/G_before, and asserts the
   first is at most k·ε·(1 + c) with k a small integer the log derives from the operations of
   `ODEPolicy` and `HubblePolicy` on that path. It reports the largest ratio and the c at it. A
   fixed bound picked to pass is not acceptable (note 01 ruling 8). Item 5's ΔK is equal to the
   ledger's entry **exactly** (the same float), and against the test's own recomputation it carries
   a stated bound.
9. **A count that differs from §0's table only by π exactly 0.0 at a step's end** is reported with
   the step and is not a stop. Any other difference is. The measurement also counts, on the same
   α = 0 result, with the probe's own loop, so the loop's records are checked against an
   independent count.
10. **The signatures.** `alpha` is the last parameter of `integrate_scalar_history`,
    `integrate_history` and `tune_history`, and is passed between them by keyword. The check is
    `not (0.0 <= alpha < 1.0)`, which a NaN fails too. `compute_scalar_history` is byte-for-byte
    unchanged; so are `build_extra_data` and `ScalarHistory.store`. An existing test that fails
    because of the keyword is a stop, not a reason to edit it.
11. **The records.** `TurningPoint` and `Injection` are module-level namedtuples, so the payload
    pickles, with their fields in the prompt's order. The comment block above `IntegrationResult`
    names the two new fields. `"reflections"` in the payload stays the count of floor events.
12. **The measurement setup is the planner's.** Copy `turning_points.py` into the agent's scratch
    directory and extend it: the same cosmology, z grid, seed realization, φ*, π*, T*, the default
    tolerances. Run it from the repository root. Report the load beside every wall time.
13. **Timing under load.** Prompt §3.4's cost at α = 0 is the golden tests' wall time at `63918a4`
    (or this note's commit, which changes no code) and with the change, back to back, three times
    each, with the minima and the load. A difference inside the spread is "nothing measurable";
    say so with the numbers. The `brentq` count it adds is counted, not estimated.
14. **Suites.** Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send
    output to a file in the agent's own scratch directory, grep the verdict from it, and record
    `uptime` beside each. A run that may pass 600 s goes in the background (`run_in_background`),
    and the agent waits for its notification without polling. A failure is re-run alone before it
    is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites, and the two golden tests alone with their times, before any edit.
  2. The records, the classification, and α = 0 bitwise (the golden tests and
     `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`).
  3. `alt_segment=True`, and the golden tests again.
  4. The drain, the floor-rule drain, the parameter.
  5. The tests of §1.5, then the breakages, then the measurement, then the suites and
     `black --check`.
  6. The records: log, documents, board.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No
  golden JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §5, plus an existing test failing for a
  reason this note does not name. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `63918a4`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **H_E continuity, by the orchestrator.** This is not given to the agent, which writes its own
   (prompt §1.2). With ρ_kin = ½H²π² and ρ_o = V + ρ_R(1 + f_m), the constraint is
   3M_P²H² − ½H²π² = ρ_o + ρ_χ. After the drain, 3M_P²H² − ½H²(1 − α)π² = ρ_o + ρ_χ + α·½H²π² at
   the same H: the α terms cancel. The floor rule's π → −√(1 − α)π enters through π² only, so the
   same holds there.
3. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and log 01;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/02-injection-at-turning-points.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store, no golden JSON written.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the golden tests' times before and after, and the `brentq` calls added (ruling 13);
  - its H_E argument, in brief;
  - the new tests by name, each with what it asserts, its tolerance and its cost;
  - for item 4: the largest |ΔH/H|, the c at it, and the bound;
  - the measurement tables, the α = 0 counts against §0's, and the tuning at α = 0.1;
  - each breakage, with the tests it failed and how; B8's result in particular;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files (plus
   `.documents/OPEN_ISSUES.md` only if something was opened), and its parent is this note's commit
   or a later commit that is not the agent's. `git status --porcelain --untracked-files=all` is
   empty, and nothing is pushed.
   `git diff <dispatch> <agent> -- ComputeTargets/AdiabaticHistory.py ComputeTargets/Policies ComputeTargets/dark_radiation.py Quadrature ObjectFactories config main.py tools RunDrivers plot_inputs.py ComputeTargets/tests/data ComputeTargets/tests/golden_history.py ComputeTargets/tests/golden_tuned_history.py`
   is empty, or each change is a classed deviation. `compute_scalar_history`, `build_extra_data`
   and `ScalarHistory.store` are byte-identical to `<dispatch>`'s.
2. **The golden histories.** `test_golden_history` and the tuned golden test pass alone at
   `<agent>`. The two JSONs are byte-identical to `<dispatch>`'s. `test_first_bounce.py`,
   `test_surfing_turning_points.py` and `test_kinematic_cap_loop.py` are unedited and pass.
3. **The loop.** Read every changed line of `integrate_scalar_history` against rulings 1–6: the
   classification at the nodes, the drain-point test on `solver.y`/`solver.f`, the order (append,
   classify, end, drain, restart), `ledger.add` before `new_solver`, no drain on the last step, and
   at α = 0 no restart, no ledger call and no change to y. The floor branch at α = 0 performs
   `<dispatch>`'s operations.
4. **The drain.** ΔK and π_after are computed as the prompt's §1.2 says; ρ_kin uses H_E with the
   ledger before the drain; `delta_rho_over_rho_rad` is α_eff ρ_kin,E/ρ_R,E. A probe of my own
   (session scratchpad, not committed): one β = 2, α = 0.1 history at M = 0.5 M_P, recomputing
   for each injection H_E before and after from the definitions, and the ledger's K against
   Σ ΔK.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B8's outcome agrees with the log.
6. **The numerics of the tests.** Ruling 8: the bound carries 1 + c, k is derived, and the
   observed ratio is reported against it. Ruling 6: item 6 is bitwise. Item 7 fails under B6.
7. **The measurement.** The α = 0 counts equal §0's table (ruling 9), no step has two sign changes,
   and the dropped drains are counted. The tuning at α = 0.1 is reported as measured. I re-run one
   β of the measurement at α = 0.1 myself and compare the counts.
8. **Nothing reaches the store.** Reread `ScalarHistory.store`, `build_extra_data`,
   `golden_history.record_of` and `TUNING_OUTPUT_KEYS` at `<agent>`. The ObjectFactories, tools and
   RunDrivers counts are unchanged.
9. **The records.** The log is in §5.1's form, with the H_E argument, every deviation classed and
   §0's rulings among them. The two `.documents/` notes are dated and additive only. The board (row
   02, gate Inject, §5.2) and `.documents/OPEN_ISSUES.md`, if anything was opened, agree with each
   other.
10. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
    with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 02 landed, the gate Inject held, and this note added to the list of
  orchestrator notes, marked "used for 02";
- under `### 5.2`, an *Orchestrator review of prompt 02* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Inject;
- the counts;
- the measurement, briefly: drains per history and their cost, and the tuning at α = 0.1;
- anything opened;
- the records as handed to 03;
- that 03 is written next, from 02's "State handed to the next prompt" (README §2.1).
