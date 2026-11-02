# Log 01 — measure the fixes

**Subject:** Measure the four fixes of the uncapped outward step, with a scanner and a probe switch
**Commit:** `d4703df`, on branch `reflection-restart`, on top of `3b8a553`
**Date:** 2026-11-02, by Claude Opus 5.5
**Result:** **COMPLETE.** The scanner, the probe switch and their tests shipped; the Signature gate holds (835 = 835,
every printed line of the diagnosis reproduced); the Measure gate holds, with the proviso of one issue opened. No
stop condition was met.
- Every fix (F1, F2 at c = 0.3, 1, 3, F3, F4, F3+F4) makes the three failures succeed and leaves no kept step of
  the bad kind in 66 histories. None reproduces 54 of 54 stored histories bitwise. F3 alone leaves the golden
  histories and every non-reflecting history bitwise at ≤ 1.03× the steps; F1 costs 1.7–2.0×; F2 and F4 move the
  goldens.
- Why Radau accepts the bad step: not its ordinary error estimate, which rejects it at 1.09×10⁵, but the refined
  estimate SciPy makes only after a rejection, which reads 0.94 against a true error of 1.05×10⁵; a grown step does
  not reach the h·ω ≈ 10³ that needs, so F3 alone suffices in everything measured.
- Opened `[01-p1-flag-fires-on-legitimate-steps]`: P1's flag, applied as defined (ruling 4), fires on about one
  legitimate step per outward flight. Suites: CosmologyModels 26, ComputeTargets 508 (122 + 386), ObjectFactories
  130, tools 145, RunDrivers 68, all passing (before: 26, 498, 130, 123, 68).

The orchestrator's note for this prompt (its Gate, §0 and seven rulings, copied to the session scratchpad as
`rr01-orchestrator-section0.md`) governed where it differs from the prompt; each ruling is classified in §2.

## 1. What shipped

- **The scanner** `tools/outward_step_scan.py` (P1, P2, P3; prompt §1.1). `one` scans a history (tuned, or at a
  given normalization); `batch` runs a task list or a store selection (`--from-store PATH` with `--M`, `--beta`,
  `--alpha`, `--block`, `--success-only`, `--variant`) in worker processes, one per task, `--jobs` at a time, each
  under `--timeout`, one JSON line per task, resumable, with `OUTPUT.pid` and `OUTPUT.log` ending `** scan done`.
  It records every accepted outward Radau step (verification §1.1), flags it by P1, and with a store compares the
  re-integration with the stored rows through the factory's own `store`. **Tests:** `tools/tests/test_outward_step_scan.py`, 22.
- **The probe switch** in `ComputeTargets/ScalarHistory.py` (prompt §1.2): `_PROBE_OUTWARD_FIX` (None, F1, F2,
  F3, F4, F3+F4), `_PROBE_STIFFNESS_C` (1.0), `_PROBE_FLAG_SAMPLES` and `_probe_outward_step_flagged` (P1's flag
  for F4), `IntegrationResult.probe_retakes` (default 0), and in `integrate_scalar_history` the read at call time,
  `new_solver`'s `first_step` (F3, at the reflection, drain and release restarts), the cap's two terms (F1, F2),
  F4's test between the `message` check and `accepted_steps += 1`, and the retake cap. Nothing else in the file.
  **Tests:** `ComputeTargets/tests/test_probe_outward_fix.py`, 10.
- **The measurement** `.documents/reflection-restart/measure/`: `tasks.py` (the task lists), `signature.py` (the
  Signature gate), `golden_post.py` (the golden records, run in the worker), `restart_state.py` (the tests' state),
  `why_radau_accepts.py` (§1.4), `summarise.py` (the tables). Outputs in `.documents/reflection-restart/out/`:
  `signature.txt`, `restart_state.txt`, `why_radau_accepts-1.5-1e-12-0.05.txt`, `why_radau_accepts-1.14-1e-15-0.01.txt`,
  `measure-summary.txt`, `measure-tasks.tsv`. Raw output in `var/reflection-restart/01/` (not committed).
- **The record** `.documents/reflection-restart-verification.md` §1 (new).
- **The board** (row 01, §1.1 items, the two gates, §3's issue, §5's counts), `.documents/OPEN_ISSUES.md` (§1.22,
  the count 26 → 27), `CLAUDE.md`'s test counts, and this log.

## 2. Deviations from the prompt, and the rulings of note 01

1. **`STRUCTURALLY REQUIRED`: the Signature count is 835** (ruling 1). The gate is the diagnosis's nine counts
   (sum 835), its three bad steps and its outcome lines. The scanner reproduces every line as text, and its own
   count equals the diagnosis's accounting in every history: no step differs (verification §1.2). The parked
   phase, which the note feared the diagnosis had counted, integrates with DOP853, not Radau, so it could not have.
   The 845 is recorded in the verification document as a slip; the diagnosis and the README are not edited.
2. **`STRUCTURALLY REQUIRED`: the store comparison** (ruling 2). The new payload is turned into rows by calling
   `sqla_ScalarHistoryFactory.store` with inserters that capture them, on an object populated as
   `ScalarHistory.store()` populates it; a sample is matched to its stored row by ln(1 + z) through `z_serial` and
   the `redshift` table (bit pattern); every `ScalarHistoryValue` column but the three serials, the computed
   `ScalarHistory` columns (not the tuning's, the timings, serials, label, version, timestamp, `validated`), and
   `extra_data` key by key over the shared keys. `compute_steps`, `RHS_evaluations` and `z_samples` are compared
   (they are counts the history computes, not timings).
3. **`IMPLEMENTATION CHOICE` (ruled): F4's discarded step** (ruling 3). F4's test sits between the `message` check
   and `accepted_steps += 1`; a flagged step rebuilds Radau from the step's start (N, a copy of the state), with
   the next cap halved, and touches nothing else; `probe_retakes` is returned in the `IntegrationResult`, not stored.
   The discarded step's RHS evaluations stay in `nfev` and the supervisor.
4. **`STRUCTURALLY REQUIRED`: the flag is not tuned** (ruling 4). P1's definition stands; each row records which
   condition fired and the sign change. It fires on legitimate steps in every history (verification §1.1), F4 acts
   on them, and the golden histories move under F4: reported, and opened as `[01-p1-flag-fires-on-legitimate-steps]`
   (invariant 4), not changed.
5. **`IMPLEMENTATION CHOICE` (ruled): the batch's limits** (ruling 5): one process per task with a 1800 s limit,
   `--jobs 6`, `uptime` at launch and end, F1 and F2 at 4×10⁻³¹ last. No task reached the limit.
6. **As the prompt says: §1.4 bounded** (ruling 6). Settled in about an hour of the agent's time; no issue.
7. **`STRUCTURALLY REQUIRED`: the files** (ruling 7). The commit holds exactly the listed files; §4.6 shows the
   staged list.
8. **`IMPLEMENTATION CHOICE`: how the scanner observes.** A subclass of SciPy's `Radau` replaces the name `Radau`
   in `ComputeTargets.ScalarHistory`, and `integrate_scalar_history` is wrapped there, for the duration of a
   history (`observe`), both restored after; SciPy's class is never modified and nothing is patched at import. A
   hook in the loop was ruled out by ruling 7. What started an instance is read from the frame that calls
   `new_solver`: `resume_after_release` for a release, otherwise the identity of the state array against the
   loop's locals `y_reflected`, `y_drained` and `y_retake` (F4's, named for this). This ties the scanner to those
   local names; a test of the loop's shape guards it.
9. **`STRUCTURALLY REQUIRED`: the tests' window starts before the reflection and ends by temperature.** F3 acts
   only at a restart, so the window starts at the state the reflection was made from, with the ledger less the
   reflection's own entry, and the loop makes the reflection; the restarted state and the ledger's new entry are
   checked bitwise against the history's. It ends at ln T_J = (the restart's) − 2×10⁻³, not at `N_stop`, because
   SciPy's initial step depends on the solver's bound and `N_stop` changes it: with `N_stop` the None window did
   not reproduce the bad step (its first h was 9.4×10⁻¹¹).
10. **`STRUCTURALLY REQUIRED`: "no flagged step" in a window short of the apex, and F4's bitwise test on a probe
    window.** Under F3 and F2 at c = 3 a window of 10⁻² e-folds keeps one ratio-flagged step at N₀ + 0.009, the
    legitimate deceleration before the apex (ruling 4), so the window is 2×10⁻³. Every history measured holds such
    steps, so F4 is shown bitwise on the cap-loop probe P1 at M = 0.5 to N = 20.8 (104 outward steps, none flagged)
    rather than on a history.
11. **`IMPLEMENTATION CHOICE`: the golden histories in the batch.** They run as the scanner's tasks (`fixed` at
    4 ln(1 meV), φ_ref 0, T_stop 1 MeV, 85 decades; and `tuned` at β = 3, M = 0.01), and a `post` hook,
    `measure/golden_post.py`, compares each payload's record with its JSON in the worker. Under None all three are
    bitwise, so the tasks reproduce `golden_history.py` and `golden_tuned_history.py`.
12. **`IMPLEMENTATION CHOICE`: the store's parameters.** M is read as `value_eV` and rounded to 12 significant
    figures in M_P, the run's command-line decimals (checked to 10⁻¹²); a tolerance whose log10 gives the default
    is the default's float. φ* and T* are read as stored (5.0 and 20000.0, exact). Every stored history reproduced
    bitwise, so the reconstruction is the run's.
13. **`IMPLEMENTATION CHOICE`: two extras in the scanner**: a `stub` task mode, for the batch's tests (a function
    run in the worker), and the `post` hook of item 11.
14. **`IMPLEMENTATION CHOICE`: §1.4 reproduces `_step_impl`** with SciPy's private `solve_collocation_system`,
    `norm`, `predict_factor` and constants, rather than instrumenting it; the reproduction lands on the accepted
    step's h, endpoint and `error_norm` bitwise.
15. **`UNINTENDED DRIFT`, corrected: the batch ran in two launches.** At 11:07:07, after 73 records, the parent
    stopped on a record it could not write as JSON: `gstar_rho` and `gstar_s` of a payload's samples are 0-d numpy
    arrays, and a comparison entry carried one. The writer now converts numpy values (`_json_default`) and writes
    an unwritable record as an error rather than stopping; a test covers it. The batch resumed from its own output
    at 11:09:18, re-running the three tasks that had been in flight (their workers had died with the parent). All
    414 records are of the fixed code; the first 73 were written by the same worker code.
16. **`UNINTENDED DRIFT`: three breakage checks ran while the batch was running** (§6, B9–B11), each editing
    `ScalarHistory.py` for about three seconds, and one (T18) edited `tools/outward_step_scan.py`. A worker that
    imported `ScalarHistory` inside one of those windows would have run the mutated code. Checked: the workers
    spawned then ran F1 and F2 tasks, and B9–B11 change only how None, F3 and F4 are parsed, which F1 and F2 do
    not read; T18 changed only the parent's writer, already loaded. No record is affected. Not repeated.
17. **`NOTED`: the summary splits P1's count.** Beside P1's count the tables give its phi and ratio conditions
    apart, and the phi condition on the first step of a restarted instance ("bad kind"), which ruling 4 allows as a
    record; the flag itself is unchanged.
18. **`NOTED`: the counts in `CLAUDE.md` were already stale** before this prompt (497 and 129 against 498 and 130
    measured; `745c8ee` and `c95650a` each added a test, as the board's §5 expected). They are set to the counts
    after this prompt.

## 3. The Signature gate

Verification §1.2 and `out/signature.txt`: nine histories under None, every outcome line, count line, statistics
line and flagged-step line of `scan_first_steps.txt` identical as text; first outward steps 30, 270, 151, 25, 32,
62, 174, 36, 55 = 835 in the diagnosis, in the scanner under the diagnosis's accounting and in the scanner's own;
the three bad steps N₀ = 35.91429105, 37.67089767, 37.41894954; h = 6.37×10⁻⁴, 1.61×10⁻⁴, 1.6×10⁻⁴; h·ω =
1.24×10³, 1.07×10³, 1.06×10³; π_end/π_start = 6.58×10⁻⁵, 8.79×10⁻⁵, 8.9×10⁻⁵. The batch took 27 s at a load of 7.8.

## 4. Verification performed

### 4.1 The suites

Serially, from the repository root, ComputeTargets in its two halves, each suite's start with `uptime`
(`run_suites.sh` in the session scratchpad).

| suite | before (10:33–10:48) | s | load at start | after (11:39–11:55) | s | load at start |
|---|---|---|---|---|---|---|
| CosmologyModels | 26 OK | 66 | 8.27 | 26 OK | 66 | 7.30 |
| ComputeTargets `test_[a-f]` | 122 OK | 135 | 7.12 | 122 OK | 133 | 3.79 |
| ComputeTargets `test_[g-z]` | 376 OK | 450 | 6.25 | 386 OK | 453 | 5.78 |
| ObjectFactories | 130 OK | 82 | 4.26 | 130 OK | 87 | 5.53 |
| tools | 123 OK | 133 | 6.86 | 145 OK | 165 | 9.06 |
| RunDrivers | 68 OK | 25 | 6.16 | 68 OK | 26 | 6.23 |

No count fell. ComputeTargets gains the 10 of `test_probe_outward_fix.py` (in the `[g-z]` half), tools the 22 of
`test_outward_step_scan.py`.

### 4.2 The golden histories and the fingerprint at None

After the switch went in, before anything else: `ComputeTargets.tests.test_golden_history` and
`ObjectFactories.tests.test_store_fingerprint_golden`, 8 tests OK in 13 s; `ComputeTargets.tests.test_tuning`
(the tuned golden history, test (e)), 40 OK in 128 s. Bitwise. The batch's None runs of the three goldens are
bitwise too (verification §1.4).

### 4.3 The switch's and the scanner's tests

`ComputeTargets.tests.test_probe_outward_fix`: 10 OK in about 0.5 s. `tools.tests.test_outward_step_scan`: 22 OK in
about 30 s. Each was shown to catch a breakage (§6).

### 4.4 The measurement

The Signature run (§3), then the batch: 414 tasks, `--jobs 6 --timeout 1800`, launched 11:04:28 UTC (`uptime`
load 8.34 / 12.52 / 11.02), stopped at 11:07:07 (D15), resumed 11:09:18 (6.76 / 10.56 / 10.71), `** scan done`
at 11:38:29 (at 11:39, 8.17 / 12.82 / 13.26; about 20 at its peak, from other work on the machine). Checked once
after each launch that it had started; waited on by a background command that ended with it. 411 successes and the
three failures under None; no timeout, no error. §1.4's script ran meanwhile (two histories, about 2.5 min each).

### 4.5 `black`

`black` on every Python file changed or added, including the measurement scripts; `black --check` clean.
`ScalarHistory.py` was black-clean before.

### 4.6 The staged files

`git diff --cached --name-status` before the commit, 21 files, staged by explicit path (ruling 7's list):

    M	.documents/OPEN_ISSUES.md
    A	.documents/reflection-restart-verification.md
    A	.documents/reflection-restart/measure/golden_post.py
    A	.documents/reflection-restart/measure/restart_state.py
    A	.documents/reflection-restart/measure/signature.py
    A	.documents/reflection-restart/measure/summarise.py
    A	.documents/reflection-restart/measure/tasks.py
    A	.documents/reflection-restart/measure/why_radau_accepts.py
    A	.documents/reflection-restart/out/measure-summary.txt
    A	.documents/reflection-restart/out/measure-tasks.tsv
    A	.documents/reflection-restart/out/restart_state.txt
    A	.documents/reflection-restart/out/signature.txt
    A	.documents/reflection-restart/out/why_radau_accepts-1.14-1e-15-0.01.txt
    A	.documents/reflection-restart/out/why_radau_accepts-1.5-1e-12-0.05.txt
    M	CLAUDE.md
    M	ComputeTargets/ScalarHistory.py
    A	ComputeTargets/tests/test_probe_outward_fix.py
    M	prompts/reflection-restart/IMPLEMENTATION_STATE.md
    A	prompts/reflection-restart/logs/01-measure-the-fixes.md
    A	tools/outward_step_scan.py
    A	tools/tests/test_outward_step_scan.py

Nothing under `var/` is committed; no golden record changed.

## 5. The measurement, in short

Verification §1.4–§1.6 holds the tables. In short:
- **Every fix removes the bad step**: the three failures succeed under F1, F2 (c = 0.3, 1, 3), F3, F4 and F3+F4,
  and no kept step of the bad kind (the phi condition on the first step of a restarted instance) remains in any of
  the 66 histories (the nine, the three goldens, the reflection store's 42, block B's 12) under any fix. Under None
  there are exactly the three.
- **Bitwise:** None reproduces 54 of 54 stored histories (every column) and the three goldens. F3 leaves the goldens
  and the six non-reflecting stored histories bitwise and moves every reflecting one. F2 (c = 1) leaves 44 of 54
  stored histories bitwise and moves the goldens. F1, F4 and F3+F4 leave nothing measured bitwise.
- **Cost** (accepted steps against None): F1 1.7–2.0× where histories reflect; F2 1.00×; F3 1.00–1.03×; F4
  0.95–1.02×; F3+F4 0.96–1.03×; F2 at c = 0.3 up to 3.2×.
- **Values against the store** (items 4, 5): every fix moves ΔN_eff today by up to 2.9–11×10⁻³, parking N and T
  by up to 1.4–4.5×10⁻³, ρ_NP/ρ_R by up to 1.2×10⁻³ (F1), and the count of drains before parking by up to 9 %.
- **Why** (verification §1.5): the bad step is accepted on SciPy's refined estimate after a rejected first attempt,
  at h·ω ≈ 1.1–1.2×10³; the plain estimate is right. A step grown from F3's first step stays below h·ω = 1.5×10⁻³ on
  the outward flight. F3 alone suffices in everything measured.

## 6. The deliberate-breakage record

Each check edits one line, runs the one test, expects a failure and restores the file (sha256 compared), by the
scratchpad's `mutate.py`. B1–B8 and T1–T17 ran before the batch; B8's first form was not caught by the test it was
aimed at (that test patches the switch), so `test_the_defaults` was added and catches it; B9–B11 were added with
the strengthened None and F3+F4 tests and ran during the batch (D16); T18 with the numpy test.

| # | breakage | test | result |
|---|---|---|---|
| B1 | no `ValueError` for an unknown value | `test_an_unknown_value_raises` | FAILED (4) |
| B2 | F1's term off | `test_F1` | FAILED |
| B3 | F2's term off | `test_F2` | FAILED (3) |
| B4 | F3 returns no first step | `test_F3` | FAILED |
| B5 | F4 never flags | `test_F4` | FAILED |
| B6 | F4 rebuilds Radau after every passing outward step | `test_F4_where_nothing_is_flagged` | FAILED |
| B7 | F3 also at the history's start | `test_F3_without_an_outward_restart` | FAILED |
| B8 | the default `"F1"` | `test_the_defaults` | FAILED (not caught by `test_None_reproduces_the_bad_step`, which sets None itself) |
| B9 | None parsed as F3 | `test_None_reproduces_the_bad_step` | FAILED |
| B10 | F3+F4 without F4 | `test_F3_and_F4` | FAILED |
| B11 | F3+F4 without F3 | `test_F3_and_F4` | FAILED |
| T1 | ratio threshold `<=` | `test_the_thresholds_are_strict` | FAILED |
| T2 | the sign change ignored | `test_a_sign_change_is_not_the_ratio_condition` | FAILED |
| T3 | the phi condition off | `test_the_phi_condition_alone` | FAILED |
| T4 | the probe's copy at 0.05 | `test_agreement` | FAILED |
| T5 | a drain read as a reflection | `test_the_start_reasons` | FAILED |
| T6 | outward steps not counted | `test_the_counts` | FAILED |
| T7 | first steps not kept in `--rows first` | `test_the_rows` | FAILED |
| T8 | `observe` does not restore `Radau` | `test_observe_restores_the_module` | FAILED |
| T9 | no resume | `test_one_line_per_task_and_the_resume` | FAILED |
| T10 | a raising task sends nothing | `test_a_task_that_raises_is_recorded_and_the_batch_goes_on` | FAILED |
| T11 | no timeout | `test_a_task_past_its_limit_is_a_timeout` | FAILED |
| T12 | a sample column skipped | `test_a_changed_sample_is_reported` | FAILED |
| T13 | history columns skipped | `test_a_changed_history_column_is_reported` | FAILED |
| T14 | `extra_data` skipped | `test_a_changed_extra_data_key_is_reported` | FAILED |
| T15 | the filters ignored | `test_the_selection_and_the_store_mode` | FAILED |
| T16 | not read-only | `test_a_path_that_cannot_be_opened_read_only_is_refused` | FAILED |
| T17 | samples matched to the wrong row | `test_a_history_compares_bitwise_with_its_own_payload` | FAILED |
| T18 | records written without the numpy converter | `test_numpy_values_in_a_record_are_written` | FAILED |

## 7. Observations not acted on

- **P1's flag fires on legitimate steps** (opened: `[01-p1-flag-fires-on-legitimate-steps]`, board §3). Under None,
  14 737 ratio-only flags (every one decelerating, −π′ > 0, none a first step of an instance; about one per outward
  flight; π ratios continuous through ½) and five phi flags on an apex step that falls back by f φ (β = 2.9,
  α = 0.08, at five M). The bad steps alone are the phi condition on a first step of a restarted instance. 02's
  signature, and F4's flag if F4 is wanted, are for the user to rule.
- **h·ω from the Jacobian Radau holds is stale on later steps.** SciPy keeps an instance's Jacobian until a slow
  Newton iteration, so a later step's ω is usually the restart's, near the wall; h·ω up to 1.2×10⁴ appears on
  legitimate later steps, and 221 outward steps have h·ω ≥ 10³ under None, three of them bad. Recorded as the
  prompt asks; 02 should not read it as the local stiffness. F2 reads the same stale J.
- **SciPy differs from Hairer and Wanner's RADAU5 in when it refines the estimate**: RADAU5 refines on the first
  step of an integration and after a rejection, SciPy only after a rejection. The bad steps are accepted on the
  refined estimate (verification §1.5); a restart in RADAU5 would refine even its first attempt. Nothing in this
  campaign changes SciPy; recorded for 03's choice.
- **Newton does not converge from Z₀ = 0 at a restart near the wall for h·ω ≲ 10³**, so SciPy reaches a good first
  step only by halving from a large first attempt; with F3's small first step it never meets that range.
- **F2 at c = 1 leaves 44 of 54 stored histories bitwise** while removing the bad step, because the bad step's h·ω
  (≈ 10³) is far above c; c is not delicate there (c = 0.3 and 3 also remove it). It acts outside restarts and
  moves the goldens, and reads the stale J.
- **The parked phase is DOP853**, not Radau: the orchestrator's note expected a Radau there. The scanner sees
  every Radau of a history; none is the parked phase's.
- **`prompts/INDEX.md`** says "26 open in the whole index" in this campaign's row; the index is now at 27. The
  file is outside ruling 7's list, so it is left for the orchestrator.
- **`.documents/OPEN_ISSUES.md`'s list of boards** at its head stops at `inelastic-reflection`; the new §1.22 links
  this board. Left as it is.

## 8. State handed to the next prompt

- **The scanner's command for 02** (from the repository root; detached, as `CLAUDE.md` "Long-running jobs"):

      PYTHONPATH=. ./venv/bin/python tools/outward_step_scan.py batch \
          --from-store var/datastores/science-2026.8.0 --block A A2 B C --success-only \
          --output var/reflection-restart/02/audit.jsonl --jobs 6 --timeout 1800

  `--variant` defaults to None (the production code). `--rows flagged` (the default) keeps the flagged rows in
  full; each row has `first`, `reason`, `ratio_condition`, `phi_condition`, `sign_change`, `a_in` and `h_omega`, so
  any signature the user rules (issue `[01-p1-flag-fires-on-legitimate-steps]`) can be counted from the output
  without a second run, provided it is a subset of P1's flag. A signature outside P1's flag needs `--rows all`,
  whose lines are much longer (about 400 000 rows over the 66 histories here). A killed batch resumes from its
  output. Blocks: A is M = 4×10⁻³¹ with α ≥ 0.01, A2 the same M with α < 0.01, B M = 10⁻³³ and 10⁻³⁰, C 10⁻¹⁵,
  10⁻¹³, 10⁻¹² (`BLOCKS` in the tool).
- **Throughput per M** (None, re-integration at stored normalization, one task per history at a load of about 10,
  wall-clock per history): 3–13 s at 4×10⁻³¹, 10⁻¹⁵, 10⁻¹², 10⁻³³ and 10⁻³⁰, except β = 1.2 at α = 0.001,
  which took 236 s (10⁻¹²), 270 s (4×10⁻³¹) and 317 s (10⁻¹⁵), its histories holding about 2500–3300 outward flights;
  10⁻³ 9–12 s; 0.5 3–4 s. With six workers the batch ran 414 tasks in about 32 min of wall-clock. The run's own compute times
  (README §0.2) give A, A2, B, C at about 42 CPU-hours, so about 7–8 h at six workers, more under load.
- **The bitwise check.** Under None every one of the 54 stored histories re-integrated here (42 of the
  reflection store, 12 of block B in the main store) is bitwise in every `ScalarHistoryValue` column, every
  computed `ScalarHistory` column (`compute_steps`, `RHS_evaluations` and `z_samples` included) and every shared
  `extra_data` key. `comparison.bitwise` in each line is the verdict; a difference names the column, its largest
  relative difference and the first differing sample by ln(1 + z) and raw N. The tree was this prompt's; the probe
  switch at None is today's code, so 02 may run on `d4703df` (P3's tree is `main` before 03).
- **Do not edit `ComputeTargets/ScalarHistory.py` or the tool while a batch runs**: each task's worker imports
  them when it starts (D16).
- The probe switch stays until 03 removes it, with `probe_retakes` and `_probe_outward_step_flagged`; the scanner
  keeps its own copy of the flag (`classify_step`), which a test holds equal to the probe's.
