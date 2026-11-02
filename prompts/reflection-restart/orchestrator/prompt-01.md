# Orchestrator — prompt 01, measure the fixes

Read [`../README.md`](../README.md) first: §0–§2, §5 (the rules), §6.1 as ruled (U1–U5 of 2026-11-02),
§6.2 P1–P10. Then the prompt. This note binds 01 only. The conventions of `inelastic-reflection`'s notes
bind here as well and are restated below: a tolerance is a number with its reason, timing is taken under
load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scanner and the measurement scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-measure-the-fixes.md`](../01-measure-the-fixes.md) · **Gates:** "Signature" and
"Measure" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus, as the prompt
recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-02 ("write the orchestration note for prompt 01 and
dispatch it").

**Gate**, checked at `fa9320a` on 2026-11-02 at 10:30 GMT, before this note was committed:
- `reflection-restart` is checked out in the main checkout, at `fa9320a`, the commit that adds prompt 01;
  `git status --porcelain --untracked-files=all` is empty and `git worktree list` shows the main checkout
  alone. `HEAD` at dispatch is the commit that adds this note, which touches only `orchestrator/`.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- `var/datastores/science-2026.8.0.db` and `science-2026.8.0-reflections.db` exist, each with 16 shards.
- None of `var/reflection-restart/`, `.documents/reflection-restart/`,
  `.documents/reflection-restart-verification.md` exists.
- The suites were not re-run for this note; the agent measures the baselines (prompt §3).
- Load averages 4.47 / 6.53 / 8.50. The machine is shared.

## 0. What the orchestrator found

For this note the orchestrator read the diagnosis's `out/scan_first_steps.txt` line by line, the step loop
from the cap to the drain restart (`ComputeTargets/ScalarHistory.py` :2553–:2705), `tools/history_and_bbn.py`'s
fixed-normalization branch (:403–:466), the 2026.8.0 schemas (`ExponentialModel`, `ScalarHistory`,
`ScalarHistoryValue`), the reflection store's compute times, and which ODE solvers PRyMordial uses. It ran
one probe (scratchpad, not committed). Here is what it found where the prompt is silent, loose or wrong.

- **The diagnosis's count is 835, not 845.** `out/scan_first_steps.txt` gives 30, 270, 151, 25, 32, 62,
  174, 36 and 55 first outward steps for the nine histories: 835. The diagnosis README §4 and this
  campaign's README §2.1 and prompt §1.3 say 845. The per-history counts are what the scan printed.
  Ruling 1.
- **The bitwise reproduction holds on one history.** β = 2.0, M = 10⁻¹² M_P, α = 0.08 from the reflection
  store, at its stored `normalization` (−276.6307387094946) and `phi_ref` (1.0521612295165602×10⁻⁶, in M_P,
  as `_TUNING_KEYS_IN_MP` says), through `integrate_history` as the tool's `--fixed-normalization` branch
  calls it: 5437 samples, the stored count, and the φ_E values bitwise equal (compared sorted; ordering was
  not checked), in 7.0 s at load ~5. So P3's premise is not refuted. A full field-by-field comparison is
  the agent's. Under `Planck_units` `PlanckMass` is 1.0, so the `_Mp` columns are the values themselves;
  ruling 2 says how to compare anyway.
- **The diagnosis's patch saw no PRyMordial step.** PRyMordial solves with LSODA and BDF (`PRyM_main.py`)
  and Julia's `RadauIIA5` through `diffeqpy`, never SciPy's `Radau`, so patching `Radau.step` in a process
  that also runs BBN counts only the history's steps. The scanner runs no BBN anyway.
- **The parked phase has its own Radau** (`:2276`, on `ParkedRHS`). Its state vector's second component is
  not the oscillating field's π in the same sense. The diagnosis's patch counted any SciPy Radau instance's
  first step with y[1] > 0, so it may have counted parked-phase instances. Ruling 1.
- **Where F4 must sit.** After `solver.step()` the loop does, in order: the exception branch, the `message`
  check, `accepted_steps += 1`, the budget, the Jacobian clamp, the φ ≤ 0 check, `dense_output`, the
  termination root, `ts`/`interpolants`, `observe_turning_points` (where the gate raises the failures), the
  `finished` check and the drain. F4's test belongs between the `message` check and `accepted_steps += 1`.
  The RHS evaluations a discarded step made are counted in `nfev` and the supervisor's statistics, and
  cannot be un-counted. Ruling 3.
- **P1's second condition can fire on a legitimate step.** "The interpolant falls below (1 − f)·φ_start"
  is true of any outward step that passes a turning point and comes back more than 10 % of φ_start within
  the step. Near a wall that is the bad step. Elsewhere, in a resolved oscillation about the minimum at
  large M or in the golden histories, a long step might do it legitimately. Whether any does is a
  measurement. Ruling 4.
- **F1 and F2 may be very slow at 4×10⁻³¹.** The reflection store's 42 histories took 15.8 min of compute
  in the run, 3 of them over 200 s (β = 1.2, α = 0.001 at 10⁻¹², 4×10⁻³¹, 10⁻¹⁵). The main store's
  4×10⁻³¹ histories average 656 reflections; F1 could add ~10⁵ steps each. A variant that runs for hours
  must not stall the batch. Ruling 5.
- **What needs no ruling, checked:**
  - `Radau` in SciPy 1.17.0 takes `first_step` and holds `J` from its first step on; `solver.J` exists on
    every instance F2 reads.
  - The state is (φ, π, …, ln T_J at index 4), so J[1,0] is ∂π′/∂φ.
  - `tools/history_and_bbn.py --fixed-normalization NORM PHI_REF_MP` takes the stored `normalization` and
    `phi_ref` as they are stored.
  - `_TempStoreCase` is at `ObjectFactories/tests/test_version_keyed_lookups.py:198`.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The Signature gate's count.** The gate is met by the per-history counts of
   `out/scan_first_steps.txt` (sum **835**), the three flagged steps' N₀, h, h·ω and π_end/π_start to the
   digits printed there, and each history's outcome line. The scanner's own count of first outward steps
   is reported **beside** the diagnosis's: if the two accountings differ (parked-phase Radau instances, or
   steps the diagnosis counted that the scanner's definition excludes), say exactly which steps make the
   difference, and the gate holds if the difference is explained step by step. The 845 is recorded in the
   verification document as the diagnosis's arithmetic slip, with the nine counts; the diagnosis folder and
   the README are not edited (invariant 6; the orchestrator's review adds dated notes).
   *STRUCTURALLY REQUIRED* (§0, first and fourth points).
2. **The store comparison** (prompt §1.1, "From a store").
   - A sample is matched to its stored row by its ln(1 + z), joined through `z_serial` to the store's
     `redshift` table, not by sorting and not by `raw_N` order alone.
   - The new values are converted to the stored columns' units by the code the factory uses when it stores
     a sample (`ObjectFactories/ScalarHistory.py`), called or reproduced exactly, so that a conversion is not
     mistaken for a difference.
   - Every `ScalarHistoryValue` column but `serial`, `history_serial` and `z_serial` is compared bitwise,
     and the `ScalarHistory` columns the history computes (not the tuning's, not timings, not serials,
     `label`, `version`, `timestamp`) likewise. `extra_data` key by key over the keys both hold.
   - The result per history is "bitwise", or the columns that differ with the largest relative difference
     and the first differing sample.
   *STRUCTURALLY REQUIRED* (§0, second point).
3. **F4's discarded step.** "No trace but the count" means: no change to `ts`, `interpolants`, turning
   points, the gate's samples, the ledger, the drain's arming, `accepted_steps`, the budget or the stored
   samples. The RHS evaluations and supervisor statistics of the discarded step stay counted, and the log
   says so. The retake count is returned in the integration's result for the scanner to read, not stored.
   *IMPLEMENTATION CHOICE*, ruled here.
4. **The flag is not tuned.** P1's definition stands as written. The scanner records, for each flagged step,
   which condition fired and whether π changed sign on the interpolant. A flagged step in a history with no
   reflection (the golden histories, block D's kind, the α = 0 histories of item 4) is reported in the
   record, under each variant, and is not a stop. If it shows F4 would act on legitimate steps, say so;
   do not change the definition. *STRUCTURALLY REQUIRED* (§0, sixth point).
5. **The batch's limits.**
   - Each (history, variant) runs in its own worker with a wall-clock limit of **1800 s**. One that passes
     it is recorded as `timeout` with its wall-clock, accepted steps and simulated N reached if the scanner
     can read them, and is not retried. A timeout is a measurement of cost, not a stop.
   - `--jobs 6` (the machine has 10 cores and is shared). Record `uptime` at the batch's launch and end.
   - Run F1 and F2 at 4×10⁻³¹ last, so that a slow variant does not hold up the rest.
   *IMPLEMENTATION CHOICE*, ruled here (§0, seventh point).
6. **§1.4 (why Radau accepts)** is bounded at about three hours of the agent's time. If it is unsettled then,
   the record says what was measured, and an issue is opened on the board §3 and in the index.
   *As the prompt says; restated.*
7. **No file but these changes:** `tools/outward_step_scan.py`, `tools/tests/test_outward_step_scan.py`,
   `ComputeTargets/ScalarHistory.py` (the switch, its values, the retake count in the result, and nothing
   else), new tests in `ComputeTargets/tests/`, `.documents/reflection-restart/` (`measure/`, `out/`),
   `.documents/reflection-restart-verification.md`, the log, the board, `.documents/OPEN_ISSUES.md` if an
   issue is opened, and `CLAUDE.md`'s test counts. No golden record changes; nothing under `var/` is
   committed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else, serially, ComputeTargets in its two halves; the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The switch and its tests; the golden histories and the fingerprint bitwise at `None`.
  3. The scanner and its tests; the Signature gate (ruling 1) before any variant runs.
  4. The batch of §1.3, detached, with ruling 5's limits; meanwhile §1.4.
  5. The suites again; the records; the commit.
- **Long runs.** The batch is launched detached (`nohup`, pid recorded), checked once, and waited on by a
  background command that ends with it, not by polling.
- **Scratch.** The batch writes under `var/reflection-restart/01/`. The agent's probes and suite outputs go
  in its own subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster; no `ray` command is run.
- **Stop conditions are real.** A suite count that falls; the golden histories or fingerprint moving at
  `None`; the Signature gate failing after ruling 1; any change the prompt's §2 forbids becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `fa9320a`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The probe** of §0's second point:
   `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/41ff359a-93c3-4609-b3d5-e38f437e0a88/scratchpad/bitwise_probe.py 1e-12 2.0 0.08`.
3. **The count** of §0's first point, from `out/scan_first_steps.txt`.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board;
- `HEAD`, the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 7. Its log is `logs/01-measure-the-fixes.md`,
  in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn but the batch**, and the batch must have ended before it
  commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the Signature gate: the nine counts against the diagnosis's, the three bad steps' numbers, and any
    accounting difference explained;
  - the switch: the golden histories and fingerprint at `None`; F4 bitwise on a history with no flag;
  - per variant: the three failures' outcomes, flagged steps (first and later apart) over the nine, the
    goldens, the reflection store's 42 (bitwise at `None`; cost ratio and value changes under each fix),
    block B's six, and every timeout;
  - §1.4's account in a paragraph, or the issue opened;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.** `git show --stat <agent>` lists only ruling 7's files; its parent is this note's commit or a
   later commit that is not the agent's; `git status --porcelain --untracked-files=all` is empty; nothing is
   pushed. `git diff <note> <agent> -- ComputeTargets/ScalarHistory.py` holds only the switch, its values and
   the retake count.
2. **Nothing left running.** The batch's pid does not answer `kill -0`; `runregistrykit list` unchanged.
3. **Bitwise at `None`.** Run the golden tests and the fingerprint test myself, and the scanner on two of
   the reflection store's histories at `None` (one at 4×10⁻³¹), comparing with the store myself.
4. **The Signature gate.** Re-run the scanner on one failing and one clean history of the nine and compare
   with `out/scan_first_steps.txt`.
5. **The variants.** Read the switch's code for each value against prompt §1.2 and rulings 3–4; re-run one
   failure under F3 and under F4 myself and check the outcomes the record states.
6. **§1.4.** Check its numbers against one re-take of the bad step at two h, by the agent's script.
7. **The records.** The log in §5.1's form, the gates item by item, every deviation classed, §0's rulings
   among them; the verification document §1; `out/` text only; the board's row 01, items and gates; any
   issue on §3 and in `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 01 landed, the gates Signature and Measure held, and this note added to the list of
  orchestrator notes, marked "used for 01";
- an *Orchestrator review of prompt 01* paragraph on the board holding what each check found;
- dated notes, additive, on the 845 → 835 count: in `.documents/post-reflection-step-2026-11-02/README.md`
  and in this campaign's README §2.1.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the
board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gates Signature and Measure;
- for each fix: whether it clears the three failures, what it leaves bitwise, what it costs per M, and
  any legitimate step it would touch;
- why Radau accepts the bad step, and whether F3 alone can suffice;
- that U6 (the fix) is the user's to rule, after 02's audit, and that 02 is written next.
