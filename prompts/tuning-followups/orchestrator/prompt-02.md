# Orchestrator — prompt 02, quiet tune lines

Read [`../README.md`](../README.md) first: §0.3, §0.4, §1, §2.1 (the gate "Quiet"), §5 and §6.
Then the prompt, the board and 01's log. Note 01 ([`prompt-01.md`](prompt-01.md)) governed 01 and
binds nothing here beyond what is restated.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-quiet-tune-lines.md`](../02-quiet-tune-lines.md)
· **Board items:** Q1–Q6 · **Gate:** "Quiet" · **Closes:**
`[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` (`jordan-normalization`), as superseded
by R2 · **Opens:** only what the work finds · **Model:** Sonnet, as the prompt recommends ·
**Network:** none. Nothing is pushed. **The review of 02 closes the campaign** (U4).

**Gate**, checked at `2c92d77` on 2026-10-27 at 07:00 GMT, before this note was committed:
- 01 landed in `8b2a29c` and was reviewed in `8f92987`; the gate "Retry" holds (board §2). After
  the review, the housekeeping commit `2c92d77` changed `--tuning-max-integrations`' help alone
  (`config/argument_parser.py`, two lines); `ComputeTargets.tests.test_driver_options` passes on it
  (14 tests), so the counts 01 handed on stand: CosmologyModels **26**, ComputeTargets **250**
  (77 + 173), ObjectFactories **103**, tools **115**, RunDrivers **64**. The agent measures them
  again before it changes anything.
- `tuning-followups` checked out in the main checkout; `git status --porcelain
  --untracked-files=all` empty; `git worktree list` shows the main checkout alone. `HEAD` is the
  commit that adds this note, which touches only `orchestrator/`, on top of `2c92d77`.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished (`done`, `killed` or `lost`),
  none `running` or stale. No Ray is started by this prompt (README §3, U3).
- The one-minute load was 5.4 (5.41 / 7.29 / 5.51). Other workloads share the machine.

## 0. What makes this prompt unusual

**It changes what the heartbeat of a registered run is fed, and nothing a run stores.** Every
stored value, the line's format and the step counts stay as they are; what moves is which lines are
printed. The orchestrator read at `2c92d77` every file the prompt names, and RayPoolKit's
`RayWorkPool.py` at the pinned `v0.3.0` where the prompt leans on it. The prompt's line numbers are
right for this tree. What it found, where the prompt is silent, loose or wrong:

- **The run's longest silence is no longer one integration.** §4.16 point 3 says the heartbeat's
  silence "is therefore bounded by one integration", because every integration printed. Under the
  quiet rule P7's bound holds **while a tuning runs**: at most `TUNE_LINE_INTERVAL_S` plus one
  integration (about 130 s at the slowest measured). But between tunings, in the bulk of a stage,
  histories that converge in under 10 integrations and 120 s print nothing, and the only other line
  is RayWorkPool's progress line, printed only after a `ray.wait` returns and never sooner than
  `notify_min_time_interval` after the last (`RayWorkPool.py:564-595`; main.py passes
  `MIN_NOTIFY_INTERVAL = 5 * 60`, `main.py:87`, `:354`, `:616`, `:896`; the wrapper names it
  `NOTIFY_FLOOR = 300.0`, `RunDrivers/registered_run.py:129`). So the run's silence is bounded by
  that floor plus the per-tuning bound, **about 430 s**, read and not measured. That is inside the
  900 s window, so P7's conclusion stands (no `--stale-after`, the window unchanged), but the
  documents must say it (ruling 4). Not a stop: the user ruled the quiet rule knowing it thins the
  log, "We can adjust later if needed".
- **Which integration a reset reaches.** The twice rule fires inside `TuningSecant.step`
  (`ComputeTargets/ScalarHistory.py:1495-1497`), called after integration k's residuals; the plain
  step it returns is the iterate of integration k + 1, whose `kind` is `"plain"`. So "reached by a
  reset" is a fact about integration k + 1, carried across the loop as `next_kind` is (`:1730-1745`).
  On a guard round `step` is not called (`:1716-1730`), so a flag left on the stepper from an earlier
  step must not be read then. Rulings 1 and 2.
- **The clock.** Today `wall` is `perf_counter()` around `integrate_history` (`:1622-1636`). The
  prompt asks that one injected `clock` time both the rule and `wall=`. With a fake clock that
  advances a fixed amount per call, the lines a test sees depend on how many times `tune_history`
  calls it, so the calls are fixed here (ruling 1).
- **The golden tuning has no guard and no reset.** `golden_history_06_tuned.json`: five
  integrations, one plain step (the first), three secants, no guard. So under the default the tuned
  golden point prints **no** line, and it cannot exercise the guard or reset triggers. Those are
  tested on the decision function and through `tune_history`'s wiring with the integration mocked
  (ruling 3); `ExponentialModel.seed` is analytic (`CosmologyConcepts/Models/ExponentialModel.py:195-209`),
  so the real model costs nothing there.
- **The margin of the real-clock test.** `test_tuning.py`'s docstring records 5 s per integration
  unloaded and 12-19 s at a heavy load, so the golden tuning takes 25-95 s against the 120 s
  interval. On a machine loaded past that, the interval is genuinely due and a line is right. The
  test must not fail for that (ruling 3).
- **Which existing tests read lines** (README §0.3, checked): `test_tuning.py`'s (c)
  `test_c_one_progress_line_per_integration_that_the_tracker_ignores` (count and form), (f) (count),
  (g) (count and kinds), (h) (the seed line's form); (i) and (j) assert `lines == []` and are
  unaffected; `tools/tests/test_history_and_bbn.py:108-109` counts the tool's lines and, with
  `"every"`, stands. The two tool tests that mock `tune_history` (`:196`) assert only that it is not
  called. `RunDrivers/tests` feed literal lines from stubs. Nothing else in the tree reads a tune
  line (`grep`).
- **A cap below 10.** With `--tuning-max-integrations` under `TUNE_LINE_SLOW_INTEGRATIONS`, a fast
  tuning that fails at its cap prints no tune line, only the `!!` line, which carries the last
  residuals (`:1732-1738`). That is the rule as ruled; it is recorded (ruling 5), not changed.
- **`golden_tuned_history.run_tuning` needs no change**: it forwards `**kwargs` to `tune_history`
  (`ComputeTargets/tests/golden_tuned_history.py:105-130`). It does not move.
- **`prompts/INDEX.md`'s row is stale**: "0 landed" since 01. 02 corrects it (ruling 7).

**Rulings**, each recorded in the log as a deviation of the class given where it reads the prompt
otherwise, or as `NOTED` where it only fixes what the prompt leaves open.

1. **Q1's decision and its clock** (`IMPLEMENTATION CHOICE`, ruled).
   - Module constants beside the tuning's others: `TUNE_LINE_INTERVAL_S = 120.0`,
     `TUNE_LINE_SLOW_INTEGRATIONS = 10`, and `TUNE_LINE_PROGRESS = ("quiet", "every")`, each with a
     comment naming this campaign and R2.
   - One pure, module-level function (the name is the agent's; `tune_line_due` reads well) of
     `(progress, k, kind, reset, now, last_line)`, returning a bool: `"every"` → true; `"quiet"` →
     true iff `now - last_line >= TUNE_LINE_INTERVAL_S`, or `k + 1 >= TUNE_LINE_SLOW_INTEGRATIONS`,
     or `kind == "guard"`, or `reset`; anything else → `ValueError`. A docstring.
   - `tune_history(…, progress: str = "quiet", clock=perf_counter)`, after `verbose`.
     `tune_history` raises `ValueError` for an unknown `progress` **at entry**, before the seed, so a
     bad value costs no integration.
   - **The clock is called exactly here**: once before the loop (`last_line = clock()`, the tuning's
     start); once before each `integrate_history` (`start`) and once after it (`end`). `wall = end -
     start`. The decision uses `now = end`. When a line prints, `last_line = end`. No other call.
   - The line, when printed, is today's f-string unchanged. A failure's `!!` line is unchanged and
     printed whatever the rule says. `compute_scalar_history` passes nothing new.
   - The docstring of `tune_history` says the rule, the two arguments, and why the lines exist (the
     registered run's heartbeat beats only on a stdout line); its existing bullet "prints one line"
     is reworded to say when.

2. **The reset report** (`IMPLEMENTATION CHOICE`, ruled). `TuningSecant` gains a read-only property
   (the name is the agent's; `reset` reads well) that is false after `__init__` and after `guard()`,
   set false at the start of each `step` and true only in the twice-rule branch. Nothing else in the
   class changes: `step`'s returns, `B`, and the existing (b) tests stand unchanged. In
   `tune_history` a local carries it with `next_kind`: read from the stepper right after its `step`
   call, false on a guard round, and passed to the decision for the next integration (false for the
   seed's). The step counts are untouched: a reset step is still counted `plain`.

3. **The tests: placement and the shared runs** (`IMPLEMENTATION CHOICE`, ruled). All in
   `ComputeTargets/tests/test_tuning.py` (it stays in the `[g-z]` half), its module docstring's list
   updated with each new test's letter and cost.
   - **The decision function** (milliseconds): no trigger (k = 0, `now - last_line` 0) → false; each
     trigger alone → true: the interval measured from the start and from a later `last_line`, at
     exactly `TUNE_LINE_INTERVAL_S` (true) and just under (false); k + 1 = 9 (false) and 10 and 11
     (true); a guard; a reset; `"every"` with no trigger → true; an unknown `progress` → `ValueError`.
   - **The reset report** (milliseconds), on (b)'s synthetic maps: false before any step, after the
     first (plain) step and after a secant step; true after the step where the twice rule fires (the
     map of `test_b_the_twice_rule`); false again after the next step and after `guard()`. The
     existing (b) tests are not modified.
   - **The wiring** (milliseconds, no integration): `tune_history` at the golden setup with
     `SM.integrate_history` and `SM._tuning_state` patched (`mock.patch.object(SM, …)`) to scripted
     results, the real `ExponentialModel`, and, for the reset case, `SM.TuningSecant` patched to a
     scripted stub if that is simpler than steering the real one. Cases: the default `progress`
     with a frozen clock over five integrations, no guard → no line (this is the claim that
     `main.py`'s path is quiet); a guard step → that integration's line alone; a reset → that
     integration's line alone; twelve integrations, frozen clock → lines exactly at it = 9, 10, 11; a
     clock advancing 50 s per call → lines exactly where ruling 1's arithmetic puts them (worked in
     the test's docstring); `progress="every"` → a line per integration; an unknown `progress` →
     `ValueError` with the seed not taken (no integration called). How the scripts are built is the
     agent's, said in the log.
   - **(c)'s setUpClass** passes `progress="every"`, so (c)'s line test keeps testing one line per
     integration and the form, and (e) is unchanged. Listed in the log as a changed test.
   - **(d)** runs its second tuning with the default `progress` and the default `clock`, timed by
     the test's own `perf_counter()` around the call. Its bitwise assertions come first, unchanged,
     and now also show that `progress` moves no value. Then: if the elapsed time is under
     `TUNE_LINE_INTERVAL_S`, it asserts **no tune line**; otherwise it calls `skipTest` with the
     elapsed time (the interval was genuinely due; the bitwise check has already passed). Listed in
     the log.
   - **A new real tuning** (about 25 s unloaded) at the golden point with the default `progress`
     and a fake clock advancing 200 s per call: five lines, `it` 0-4, every line matching `PROGRESS`,
     the kinds `seed, plain, secant, secant, secant` (the golden record's counts), `wall=200.0`, and
     the fifteen outputs bitwise (c)'s.
   - **(f), (g), (h)** pass `progress="every"`: they test the line's count, kinds or form. Listed in
     the log. (i) and (j) are not modified.
   - Each real tuning's cost in its docstring (`CLAUDE.md`).

4. **The bound in the documents** (`NOTED`; found by this note, §0). The notes of Q4 and §4.17 state
   **both**: while a tuning runs, its longest silence is at most `TUNE_LINE_INTERVAL_S` plus one
   integration, about 130 s at the slowest measured (P7); and the run's longest silence is at most
   RayWorkPool's 300 s floor plus that, about 430 s, because quiet histories no longer fill the time
   between the pool's lines. Both **read, not measured** (U3), both inside the unchanged 900 s
   window, no `--stale-after` needed. §4.17 says by statement that §4.16 point 3's "bounded by one
   integration" and its 58 s measurement were for the every-line tree.

5. **A cap below 10** (`NOTED`). §3.7.6's note says, in one sentence, that with
   `--tuning-max-integrations` under 10 a fast tuning that fails at its cap prints only its `!!`
   line, which carries the last residuals. No code.

6. **The tools** (`NOTED`). `tools/history_and_bbn.py` and `tools/surfing_scan.py` pass
   `progress="every"` at their `tune_history` calls, each with a comment naming this campaign and U2.
   `tools/plot_surfing_histories.py` reaches `tune_history` only through `surfing_scan` (confirm by
   `grep` and say so). The docstrings that describe the lines (`history_and_bbn.py:20-25`, `:53`;
   `surfing_scan.py:78`; `plot_surfing_histories.py:29`) are checked and stay true; the log says so
   for each. `tools/tests/test_history_and_bbn.py`'s (a) is the regression guard for
   `history_and_bbn`; `surfing_scan`'s call is verified by reading.

7. **The issue, the index and `prompts/INDEX.md`** (`NOTED`).
   - `jordan-normalization`'s board: `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`
     moved from §3 to §4 with a **Resolved** line: "superseded by `tuning-followups` R2: …" (the
     quiet lines are few and spread out; the stored `tuning_iterations` is the count to read; Ray's
     deduplication stays on), naming this prompt and "this commit". Any count of open issues on that
     board's own text is corrected (five → four).
   - `.documents/OPEN_ISSUES.md`: the §1.17 row deleted and its paragraph saying both are closed and
     the heading left empty; §1.16's "leaving five open on the board" → four; the header's count
     **25 → 24**, its sentences on `jordan-normalization` (5 → 4) and `tuning-followups` corrected;
     the date.
   - This board's §3.1 row: "closed by 02 (this commit)".
   - `prompts/INDEX.md`'s row: 2 of 2 landed, 02 awaiting review; its issues cell (one opened by 01,
     §1.18; both assigned closed; 24 open in the whole index). The orchestrator's review marks it
     closed.

8. **The board** (`NOTED`). Header: "**Status: LIVE — 2 of 2 landed; 02 awaiting review, which
   closes the campaign.**" or words to that effect; §1's row 02 ✅ with "this commit" and the log;
   §1.1's Q1–Q6 marked; §2's gate "Quiet" as found; §3.1 per ruling 7; §4's sentence for the second
   assigned issue; `### 5.3 After prompt 02 (<date>)`, in §5.2's form.

9. **The baselines and counts** (`NOTED`). Before: the five suites, serially, `ComputeTargets` in
   its two halves, each Bash call at 600 000 ms, output to the agent's scratch directory. Expected
   26 / 77 + 173 / 103 / 115 / 64. After: the same, each count at least the baseline and rising only
   by ruling 3's new tests (in the `[g-z]` half). `tools/tests` runs alone. The log gives every count
   before and after, with times and `uptime`. A failure is re-run alone before it is called real. An
   existing test that fails is a stop; (d)'s skip under load is not a failure, but the log reports
   whether it skipped. **`CLAUDE.md`'s counts** (Q6) are set to the after counts, ObjectFactories'
   103 from 01 among them, with the parenthesis "(The counts are those of 2026-10-27, after
   `tuning-followups` 02, which added N tests to `test_tuning.py`.)" or words to that effect.

10. **The breakages** (`STRUCTURALLY REQUIRED`). The prompt's B1-B5, plus:
    - B4b, the guard in the wiring: `tune_history` passes `kind` to the decision as anything but
      `"guard"` on a guard round (the function untouched): the wiring's guard test fails;
    - B6, the reset in the wiring: `tune_history` passes `reset=False` always (the stepper
      untouched): the wiring's reset test fails;
    - B7, the default: `progress="every"` as `tune_history`'s default: the wiring's default test fails,
      and (d) fails its no-line assertion when it runs under 120 s.

    B1-B4 are applied in the decision function, B5 in `TuningSecant`. Each a diff `git apply` replays
    from the agent's commit, saved in its scratch directory and quoted in the log, with every test
    that fails and how. None committed. The real tunings need not be run for a breakage whose named
    test is a millisecond one; the log says which were run.

11. **The documents** (`NOTED`). Q4's notes, each opening
    `> **Note, 2026-10-27 (`tuning-followups`, prompt 02).**` (or the date it lands), additive only:
    `numerical-strategies.md` §3.7.6 (after its paragraph), `architecture-summary.md` §7.2 (where
    the line is described) and §2 (where the tools are). §4.17 goes after §4.16's last line and
    before `## 5. Reproduce`, in §4.16's form (a dated header, a lead paragraph, numbered points),
    with Q5's content, ruling 4's bounds, and the commits `8b2a29c` (01), `8f92987` (01's review),
    `2c92d77` (the help), and "this commit" for 02. `git diff -U0 <file> | grep '^-[^-]'` empty for
    each of the three documents, and the log says so. `CLAUDE.md` is not under this rule (Q6 edits
    lines).

12. **The tools' run** (`NOTED`). The prompt's §2 item 3 is `tools/tests/test_history_and_bbn.py`'s
    (a) run alone, its time given; the agent need not run the tool by hand.

**What moves on purpose:** `ComputeTargets/ScalarHistory.py`, `tools/history_and_bbn.py`,
`tools/surfing_scan.py`, `ComputeTargets/tests/test_tuning.py`; `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md`, `.documents/review-remediation-verification.md` (§4.17 only),
`CLAUDE.md`; `prompts/jordan-normalization/IMPLEMENTATION_STATE.md` (the issue's move), 
`.documents/OPEN_ISSUES.md`, `prompts/tuning-followups/IMPLEMENTATION_STATE.md`, `prompts/INDEX.md`
(the row), and the log `prompts/tuning-followups/logs/02-quiet-tune-lines.md` (new).

**What must not move:** every other file. In particular `ComputeTargets/tests/golden_tuned_history.py`,
the golden histories (`data/golden_history_05.json`, `data/golden_history_06_tuned.json`) and the
golden fingerprint, `config/` (`FINGERPRINT_FORMAT`, `VERSION_LABEL`, the parser), `main.py` and its
`process_batch_size`, `RunDrivers/` and its tests, `ObjectFactories/`, `pipeline_selection.py`, the
plot scripts, `tools/plot_surfing_histories.py`, `tools/tests/`, the README, the prompts,
`orchestrator/`, `science-runs/`, every other campaign's records but `jordan-normalization`'s board
§3-§4, the venv, the three packages, SGK and SI.

**Conventions.** All of these bind the agent.
- **Order.** The Gate; the baselines; Q1 and Q2 with their tests (the decision, the reset report, the
  wiring); the changed existing tests; the real tunings; the breakages; the suites after; `black`;
  the documents; the records; the commit.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is the list above exactly. Check `git log -1` before committing:
  `HEAD` must still be this note's commit. The message in `CLAUDE.md`'s form. A record states only
  what a command's output shows. "this commit" where the SHA goes; the orchestrator substitutes it.
- **Scratch.** Everything saved while working goes under `var/tuning-followups-02/` (ignored by git);
  never `/tmp`, the session scratchpad or the tree.
- **`black`** on the changed `.py` files only (`CLAUDE.md`), never a file not otherwise changed.
- **Stop conditions are real**: the prompt's §4, and: a golden history or the golden fingerprint
  fails; a suite's count falls; an existing test fails for a reason this note does not name (never
  edit it to pass); the reset cannot be reported without changing `step`'s returns or the counts.
  A stop goes to the user through the orchestrator; the agent resumes by message.

## 1. Before you dispatch

Done at `2c92d77`, before this note was committed: the Gate above; the reading of §0. The suites'
baselines are the agent's (ruling 9).

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout. Give it: the prompt, the README,
the board, 01's log, `HEAD`, the Gate as measured, and §0 of this note up to "## 1. Before you
dispatch", as a copy in the session scratchpad. The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly §0's list. Its log is `logs/02-quiet-tune-lines.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:** the log's path; the message as committed; `git show --stat` of its commit;
  the suites before and after, with counts, times and `uptime`; whether (d) skipped; the new tests by
  name, each with the breakage that makes it fail; B1-B7's results; the decision function and the
  reset property as written (their source); the changed existing tests with reasons; the tools'
  docstring findings; every deviation with its class; "Observations not acted on" and "State handed
  on", verbatim.

## 3. The review — nine checks

1. **Scope.** `git show --stat <agent>` lists exactly §0's files; its parent is this note's commit.
   `git status --porcelain --untracked-files=all` empty; nothing pushed. `git diff <dispatch>
   <agent> -- config/ main.py RunDrivers ObjectFactories pipeline_selection.py tools/tests
   ComputeTargets/tests/golden_tuned_history.py ComputeTargets/tests/data` empty.
2. **The rule.** Read the decision function and its constants against ruling 1, and the loop's
   diff: the three clock calls, `last_line` updated only on a printed line, the line's f-string
   byte-identical to `2c92d77`'s, the `!!` lines untouched, `progress` validated before the seed,
   `compute_scalar_history` unchanged. Replay B1, B2, B3 and see their tests fail; revert.
3. **The reset and the guard.** `TuningSecant`'s diff against ruling 2 (the (b) tests unmodified);
   the carry in `tune_history`, false on a guard round. Replay B4, B4b, B5, B6.
4. **The default.** `compute_scalar_history` and so `main.py` get `"quiet"`; replay B7. A probe of my
   own (session scratchpad, not committed): one real tuning at the golden point with the default
   `progress` and a real clock (no line) and with a clock advancing 200 s per call (five lines in
   today's form). That is the gate "Quiet" for the golden point, short of Ray.
5. **The tools.** Their two calls pass `"every"` with the comment; `plot_surfing_histories.py` has no
   call of its own; `tools/tests/test_history_and_bbn.py` (a) passes alone with five lines.
6. **The changed tests.** `git diff` of `test_tuning.py`: only (c)'s setUpClass, (d), (f), (g), (h)
   and the docstring among the existing tests change, each as ruling 3 says and listed in the log.
7. **Values do not move.** (d), (e) and the new real tuning pass bitwise; the golden fingerprint and
   `golden_history_05` tests pass in the suites.
8. **The records.** The log in §5.1's form, every deviation classed, §0's rulings among them; the
   three documents' diffs additive only; §4.17's content against Q5 and ruling 4; `CLAUDE.md`'s
   counts against the suites; the boards, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md` (24
   open; §1.17 empty) against each other.
9. **The suites** at `<agent>`, serially: the counts the log gives, each at least 26 / 77 + 173 / 103
   / 115 / 64. `black --check` on the changed `.py` files.

## 4. After the review — the campaign closes

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on both boards, in the log, in §4.17 and in `.documents/OPEN_ISSUES.md`
  wherever it appears;
- the board's list of orchestrator notes, with this one marked "used for 02";
- `### 5.3`'s *Orchestrator review of prompt 02* paragraph, holding what each check found;
- the board's header **CLOSED** (2 of 2 landed and reviewed), `prompts/INDEX.md`'s row closed, and
  the index header's sentence for `tuning-followups` saying it closed with one issue open on its
  board (§1.18) and none assigned.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:** the agent's commit and the review commit; the gate "Quiet"; the counts; the
issue closed; the run-level bound of ruling 4 (about 430 s, read, inside 900 s), which the prompt did
not foresee; that the campaign is closed and that the merge of `tuning-followups` (with
`jordan-normalization`) into `main` is theirs (U1).
