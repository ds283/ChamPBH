# Log 02 — quiet tune lines

**Subject:** Make the tune lines quiet for main.py's histories
**Commit:** this commit, on branch `tuning-followups`, on top of `0eabc7a`
**Date:** 2026-10-27, by Claude Sonnet 5.5
**Result:** **COMPLETE.** No stop.
- Q1-Q6 shipped under orchestrator note 02, whose twelve rulings govern where they differ from the
  prompt.
- **The gate "Quiet" holds**: `tune_history` prints integration k's line only when `tune_line_due`
  says so (120 s since the last line or the start, or k + 1 >= 10, or a guard step, or a reset);
  `progress="every"` prints all. A real tuning at the tuned golden point with the default progress
  and clock printed no line (test (d) did not skip), and with a fake clock advancing 200 s per call
  printed all five, in (c)'s form. No Ray was started (U3).
- **Nothing a build stores moved.** `FINGERPRINT_FORMAT` and `VERSION_LABEL` are unchanged, and the
  golden fingerprint, both golden histories and test (e) pass unchanged.
- Counts: CosmologyModels 26, ComputeTargets 269 (77 + 192, +19), ObjectFactories 103, tools 115,
  RunDrivers 64. All passing, before and after.
- Closed `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` as superseded by R2. Opened
  none. The index goes 25 -> 24 open, §1.17 empty.

## What shipped

### Q1 - the rule and its constants (`ComputeTargets/ScalarHistory.py`)
- Constants beside `TUNING_GUARD_LIMIT`: `TUNE_LINE_INTERVAL_S = 120.0`,
  `TUNE_LINE_SLOW_INTEGRATIONS = 10`, `TUNE_LINE_PROGRESS = ("quiet", "every")`, each with a comment
  naming this campaign and R2, the first two quoting the ruling ("120s and 10 integrations ... we can
  adjust later").
- `tune_line_due(progress, k, kind, reset, now, last_line)`: a pure module-level function, a bool;
  "every" -> true; "quiet" -> `now - last_line >= TUNE_LINE_INTERVAL_S or k + 1 >=
  TUNE_LINE_SLOW_INTEGRATIONS or kind == "guard" or bool(reset)`; anything else -> `ValueError`.
- `tune_history(..., verbose=False, progress="quiet", clock=perf_counter)`. It raises `ValueError` for
  an unknown `progress` at entry, before `model.seed`. The clock is called exactly: once before the
  loop (`last_line`), once before each `integrate_history` (`start`), once after it (`end`);
  `wall = end - start`; the decision uses `now = end`; a printed line sets `last_line = end`. The
  line's f-string is unchanged; a failure's `!!` line is unchanged and printed whatever the rule says.
  The docstring says the rule, both arguments and why the lines exist (the heartbeat), and its bullet
  "prints one line" now says when.
- `TuningSecant.reset`: a read-only property, `False` after `__init__` and after `guard()`, set `False`
  at the start of each `step` and `True` only in the twice-rule branch. `step`'s returns, `B` and the
  counts are untouched. In `tune_history` a local `reset` is carried with `kind`: `next_reset` is
  `stepper.reset` read right after `stepper.step`, and `False` on a guard round (where `step` is not
  called).
- `compute_scalar_history` passes nothing new.

### Q2 - the tools
`tools/history_and_bbn.py` and `tools/surfing_scan.py` pass `progress="every"` at their one
`tune_history` call each, with a comment naming this campaign and U2. `grep` of `tune_history(` over
the tree (`venv`, `prompts`, `var` excluded) finds only those two, `compute_scalar_history`, and
`golden_tuned_history.run_tuning` (which forwards `**kwargs` and did not change).
`tools/plot_surfing_histories.py` reaches `tune_history` only through `surfing_scan.run_history`: its
only mention of the name is in its docstring (`:29`). Its prints are `surfing_scan`'s, so it prints as
today.

Docstrings checked, each stays true:
- `tools/history_and_bbn.py:20-25` ("Its `-- tune ...` lines, one per integration, are tune_history's
  own") and `:53` (the output-lines table's `-- tune ...` row): true, the tool passes `"every"`.
- `tools/surfing_scan.py:78` ("tune_history's `-- tune` lines, one per integration, ... are left on the
  console"): true, as above.
- `tools/plot_surfing_histories.py:29` (tuned by `tune_history`): says nothing of the lines; true.

### Q3 - the tests (19 new, all in `ComputeTargets/tests/test_tuning.py`)
- **(k) `TestTheTuneLineRule`, 8, on numbers (milliseconds):** `test_k_no_trigger_prints_nothing`,
  `test_k_the_interval_from_the_start` (exactly 120 s true, just under false),
  `test_k_the_interval_from_a_later_last_line`, `test_k_the_slow_integrations` (k + 1 = 9 false, 10, 11
  and 15 true), `test_k_a_guard_step_alone`, `test_k_a_reset_alone`, `test_k_every_always_prints`,
  `test_k_an_unknown_progress_raises` (`"loud"`, `""`, `None`, `"QUIET"`).
- **(l) `TestTheResetReport`, 3, on (b)'s synthetic maps (milliseconds):**
  `test_l_false_before_any_step_and_after_plain_and_secant_steps`,
  `test_l_true_after_the_twice_rule_fires_and_false_again_after` (the step is still `"plain"`),
  `test_l_false_after_guard`. The existing (b) tests are not modified.
- **(m) `TestTheWiring`, 7, `tune_history` with `SM.integrate_history` and `SM._tuning_state` patched
  (milliseconds):** `test_m_the_default_is_quiet_five_integrations_no_line`,
  `test_m_a_guard_step_prints_its_integrations_line_alone` (it 3, kind guard),
  `test_m_a_reset_prints_its_integrations_line_alone` (it 2),
  `test_m_twelve_integrations_frozen_clock_lines_from_the_tenth` (9, 10, 11),
  `test_m_a_clock_advancing_50_s_per_call` (1, 3, 5, 7, 9, 10, 11; the arithmetic is in its docstring),
  `test_m_every_prints_a_line_per_integration`, `test_m_an_unknown_progress_raises_before_the_seed`
  (`integrate_history` not called). How the scripts are built: the integration is patched to return
  `{"sample": []}`; `_tuning_state` is a `side_effect` list of `TuningState`s (residuals 1e-2, a
  positive `V_target`, or a negative one at the integrations a case names), so every run ends at the
  cap's failure after the last line decision (the lines are those starting `-- tune `); the model and
  cosmology are the real ones; `TuningSecant` is patched to a stub of plain steps that reports a reset
  after a configured step (never, by default).
- **(n) `TestAConvergedHistory.test_n_a_fake_clock_prints_every_line_and_moves_no_value`, 1, a real
  tuning, about 25 s unloaded:** default progress, a clock advancing 200 s per call: five lines, it 0-4,
  every line matching `PROGRESS`, kinds `seed, plain, secant, secant, secant`, `wall=200.0`, the
  fifteen outputs bitwise (c)'s (the shared `_assert_bitwise_the_same`), and the lines equal (c)'s but
  for `wall`.
- The module docstring's list is updated with (k)-(n) and each cost.

**Changed existing tests**, each for the reason named:
- (c)'s `setUpClass`: passes `progress="every"`, so (c)'s line test still tests one line per
  integration and the form; (e) is unchanged.
- (d) `test_d_a_second_tuning_is_bitwise_the_same`: runs with the default progress and clock, timed by
  `perf_counter()` around the call. Its bitwise assertions (moved, unchanged, into
  `_assert_bitwise_the_same`) come first and now also show that `progress` moves no value. Then, under
  `TUNE_LINE_INTERVAL_S` elapsed it asserts no tune line; otherwise `skipTest` with the elapsed time.
  **It did not skip** (the full module run: 0 skipped, `OK`; the suite run: `OK`, no skip).
- (f), (g), (h): pass `progress="every"` (they test the line's count, kinds or form).
- (i) and (j) are not modified (they assert `lines == []`). `tools/tests/test_history_and_bbn.py` is not
  modified; its (a) stands.

### Q4 - the notes (`NOTED`, ruling 11)
Dated notes opening `> **Note, 2026-10-27 (`tuning-followups`, prompt 02)`: `numerical-strategies.md`
§3.7.6 (after its paragraph: the rule, the constants, the tools' `"every"`, both bounds, the cap below
10, the issue's closure), `architecture-summary.md` §2 (after the tools note) and §7.2 (after the
progress-line bullet). `git diff -U0 <file> | grep '^-[^-]'` is empty for all three documents.

### Q5 - §4.17 (`NOTED`)
`.documents/review-remediation-verification.md` §4.17, "Addendum 2026-10-27 - the `tuning-followups`
campaign", between §4.16's last line and `## 5. Reproduce`, in §4.16's form: the lead, what it
supersedes by statement (§4.16 point 3's every-integration line and its heartbeat bullet; point 7's
list of issues), and four points: `--retry-failed-scalar` and when to use it; the quiet lines;
RayPoolKit's report (`cf77234`; `process_batch_size=1` not taken); the two bounds (130 s per tuning,
430 s per run), read not measured. The commits named: `8b2a29c`, `8f92987`, `2c92d77`, and this commit.
Empty `-` diff, as above.

### Q6 - `CLAUDE.md`
The counts: ComputeTargets 243 -> 269 (halves 76 -> 77 and 167 -> 192), ObjectFactories 93 -> 103;
CosmologyModels 26, tools 115, RunDrivers 64 unchanged; the parenthesis now says 2026-10-27, after
`tuning-followups` 02, which added 19 tests, and that 01 had added 7 and 10. No other line was made
false.

### The records
- `jordan-normalization`'s board: `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` moved
  from §3 to §4 with a **Resolved** line ("superseded by `tuning-followups` R2"), naming this prompt
  and this commit; §3's introduction says four remain.
- `.documents/OPEN_ISSUES.md`: the §1.17 row deleted and the heading left empty with a sentence; §1.16
  "leaving four open"; the header 25 -> 24, its sentences on `jordan-normalization` (5 -> 4) and
  `tuning-followups`; the date stays 2026-10-27. 24 rows counted.
- This board: header, row 02, §1.1, the gate, §3.1, §4, `### 5.3`. `prompts/INDEX.md`: the row (2 of 2
  landed, 02 awaiting review; 24 open).

## Deviations from the prompt

Each ruling of the note, with its class.

| # | Ruling | Class |
|---|---|---|
| 1 | The decision as a pure function `tune_line_due(progress, k, kind, reset, now, last_line)` (the prompt left its signature open), `TUNE_LINE_PROGRESS`, `ValueError` at entry before the seed, the clock called exactly three ways (before the loop, before and after each integration), `now = end` | IMPLEMENTATION CHOICE |
| 2 | The reset reported by a read-only `TuningSecant.reset`, carried with `next_kind` and read after `step`, `False` on a guard round | IMPLEMENTATION CHOICE |
| 3 | Tests all in `test_tuning.py`; wiring with the integration and state scripted; (c) `progress="every"`; (d) default timed with `skipTest`; a new real fake-clock tuning (n); (f)-(h) `"every"` | IMPLEMENTATION CHOICE |
| 3a | Within ruling 3's latitude: **every** wiring case uses a stub `TuningSecant` (plain steps, a configurable reset step), not only the reset case. With scripted constant states the real secant takes a zero step (x repeats, dx = 0) and divides by zero (found at first run: `ZeroDivisionError` in `step`). The default-quiet claim is therefore tested with the stub stepper; the real stepper's reset is tested by (l) | IMPLEMENTATION CHOICE |
| 3b | (n) also asserts that its lines equal (c)'s but for `wall` (an assertion beyond the note's list) | IMPLEMENTATION CHOICE |
| 4 | Both bounds (130 s per tuning, 430 s per run) stated in the documents, read not measured, §4.16's "bounded by one integration" superseded by statement | NOTED |
| 5 | A cap below 10: a fast tuning failing at its cap prints only the `!!` line; one sentence in §3.7.6's note, no code | NOTED |
| 6 | The tools pass `"every"`; `plot_surfing_histories.py` reaches `tune_history` only through `surfing_scan` (grep); the docstrings checked and true | NOTED |
| 7 | The issue moved to `jordan-normalization`'s §4; the index, the board's §3.1, `prompts/INDEX.md` | NOTED |
| 8 | The board's header, row, items, gate, §3.1, §4, §5.3 | NOTED |
| 9 | The baselines and counts, serially, `ComputeTargets` in halves; `CLAUDE.md`'s counts set | NOTED |
| 10 | The breakages B1-B7 and B4b | STRUCTURALLY REQUIRED |
| 11 | The documents' notes and §4.17, additive only | NOTED |
| 12 | The tools' run: `test_history_and_bbn` (a) alone, 22.4 s (24.4 s wall) at load 13, OK; the tool not run by hand | NOTED |

Two further, from this prompt itself:
- §4.17 adds a sentence, not in Q5's list, that §4.16 point 3's "a later run with a larger cap does not
  recompute a failed history" holds only without `--retry-failed-scalar` (a supersession by statement
  of the same kind as the others). **IMPLEMENTATION CHOICE.**
- `CLAUDE.md`'s counts for ComputeTargets and ObjectFactories were those of before prompt 01; 01 had
  not updated them (not in its charter). They are corrected here to the after counts, as note 02
  ruling 9 says. **NOTED.**

No `UNINTENDED DRIFT`.

## Verification performed

Baselines at `0eabc7a`, before any change, serially, output in `var/tuning-followups-02/before-*.txt`
(all as the orchestrator expected). After, the same, in `after-*.txt`. `uptime` at the start of the
baselines: load 6.58 (07:03); at the start of the after run: load 1.72 (07:18); at its end: load 6.76
(07:28).

| Suite | Before | After | Time before | Time after |
|---|---|---|---|---|
| CosmologyModels | 26 OK | 26 OK | 65.1 s | 64.6 s |
| ComputeTargets `[a-f]` | 77 OK | 77 OK | 94.1 s | 94.8 s |
| ComputeTargets `[g-z]` | 173 OK | 192 OK | 215.6 s | 234.6 s |
| ObjectFactories | 103 OK | 103 OK | 14.5 s | 14.8 s |
| tools (alone) | 115 OK | 115 OK | 110.2 s | 113.6 s |
| RunDrivers | 64 OK | 64 OK | 22.5 s | 23.1 s |

- ComputeTargets 250 -> 269: +19, all in `test_tuning.py` (the `[g-z]` half). No count fell.
- Both golden histories, the golden fingerprint and test (e) pass unchanged (inside the suites).
- `test_tuning.py` alone, before the suites (load 2.4): 40 tests, 109.3 s, `OK`, 0 skipped.
- Test (d): not skipped.
- `test_history_and_bbn.TestTuned.test_a_the_tuning_line_is_the_tuned_goldens_record_bitwise` alone:
  1 test, 22.4 s, OK (`uptime` before: load 13.0).
- `black` on the four changed `.py` files: `ScalarHistory.py`, `tools/history_and_bbn.py` and
  `tools/surfing_scan.py` unchanged; `test_tuning.py` reformatted once (my additions only; its HEAD
  version was black-clean, and the removed lines of its diff are only the ones this prompt changes).
- `git diff -U0 | grep '^-[^-]'` empty for `numerical-strategies.md`, `architecture-summary.md`,
  `review-remediation-verification.md`.

## The deliberate-breakage record

Each diff is saved under `var/tuning-followups-02/` (`B1.diff` ... `B7.diff`, `B4b.diff`), made against
the tree before the commit; `git apply` replays them from this commit (`ScalarHistory.py`'s lines are
unchanged by the commit's own later edits: it was last edited before the breakages). Each was applied,
run, reverted. The fast tests are `TestTheTuneLineRule`, `TestTheResetReport` and `TestTheWiring` (18
tests, 0.1 s); no real tuning was run for B1-B6, since each named test is a millisecond one. None is
committed; the tree is clean of them.

| # | Breakage (the diff's change) | Failing tests |
|---|---|---|
| B1 | quiet always prints: `True or now - last_line >= ...` in `tune_line_due` | 11: (k) `a_guard_step_alone`, `a_reset_alone`, `no_trigger_prints_nothing`, `the_interval_from_a_later_last_line`, `the_interval_from_the_start`, `the_slow_integrations`; (m) `a_clock_advancing_50_s_per_call`, `a_guard_step_prints...`, `a_reset_prints...`, `the_default_is_quiet_five_integrations_no_line`, `twelve_integrations_frozen_clock...` |
| B2 | the interval never triggers: `now - last_line >= TUNE_LINE_INTERVAL_S` -> `False` | 3: (k) `the_interval_from_the_start`, `the_interval_from_a_later_last_line`; (m) `a_clock_advancing_50_s_per_call` |
| B3 | the slow threshold at 100: `k + 1 >= 100` | 3: (k) `the_slow_integrations`; (m) `a_clock_advancing_50_s_per_call`, `twelve_integrations_frozen_clock_lines_from_the_tenth` |
| B4 | a guard step not printing: `kind == "guard"` -> `False` | 2: (k) `a_guard_step_alone`; (m) `a_guard_step_prints_its_integrations_line_alone` |
| B4b | `tune_history` passes `"plain"` for a guard kind to the decision (function untouched) | 1: (m) `a_guard_step_prints_its_integrations_line_alone` |
| B5 | the reset never reported: `self._reset = True` -> `False` in `TuningSecant.step` | 2: (l) `true_after_the_twice_rule_fires_and_false_again_after`, `false_after_guard` |
| B6 | `tune_history` passes `reset=False` always (stepper untouched) | 1: (m) `a_reset_prints_its_integrations_line_alone` |
| B7 | `progress="every"` as `tune_history`'s default | 6, run with `TestTheWiring` and (d) alone (44 s): (m) `a_clock_advancing_50_s_per_call`, `a_guard_step...`, `a_reset...`, `the_default_is_quiet_five_integrations_no_line`, `twelve_integrations_frozen_clock...`; and (d) `test_d_a_second_tuning_is_bitwise_the_same` fails its no-line assertion (`['-- tune golden_tuned_history it=0 kind=s...'] != []`) after its bitwise assertions pass |

B7 and (d) were run for real (setUpClass tuning plus (d), 44 s at load 2.2); B1-B6 were not run on real
tunings.

## Observations not acted on

1. **The real secant on a constant map.** `TuningSecant.step`, given two steps whose x is the same
   (`dd = |dx|^2 = 0`), divides by zero. It cannot arise in a tuning, where a step that returns the
   same x means the residual is zero and the tuning has converged; it surfaced only when the wiring
   tests first used the real stepper on scripted constant states (hence deviation 3a). Not changed,
   out of scope; no issue opened.
2. **A cap below 10** prints only the `!!` line for a fast tuning (note 02 ruling 5): recorded in the
   documents, not changed.
3. **Time-dependent behaviour of (d).** On a machine loaded past about 5 x the unloaded 25 s, the
   120 s interval is genuinely due and (d) skips (after its bitwise assertions). It did not skip here
   (the full module took 109 s for all 40 tests).
4. **`CLAUDE.md`'s "about 320 s"** for `ComputeTargets/tests` in one run: the two halves took 94.8 s and
   234.6 s (329 s) here at loads 2-7. Not changed (within "longer on a loaded machine").

## State handed to the next prompt

There is no next prompt: the orchestrator's review of 02 closes the campaign (U4).
- HEAD after this commit is this commit, on top of `0eabc7a`; branch `tuning-followups`; nothing
  pushed. The tree was clean before the work and holds nothing but this commit's files after it.
- Suites at this commit, serially: CosmologyModels 26, ComputeTargets 269 (77 + 192), ObjectFactories
  103, tools 115, RunDrivers 64, all passing.
- The gate "Quiet" holds (board §2). `main.py`'s histories are quiet; the standalone tools print one
  line per integration. The heartbeat's bounds (130 s per tuning, 430 s per run) are read, not
  measured, inside the 900 s window.
- Open issues: 24 in the whole index; on this campaign's board, `[01-bbn-from-store-refuses-a-history-
  with-a-retried-bbn-row]` (§1.18).
- Scratch: `var/tuning-followups-02/` (ignored by git) holds the baselines, the after runs, the
  breakage diffs and outputs.
- For the review: replay B1-B7 from the saved diffs; check `git diff --cached`'s file list against note
  02's "What moves on purpose"; check `black --check` on the four `.py` files.
