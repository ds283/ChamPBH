# Prompt 02 — quiet tune lines

**Campaign:** [`README.md`](README.md) · **Board items:** **Q1**–**Q6** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-27, after the user's rulings (README §6.0 R2; §6.1 U2–U4; P6–P12). Its
orchestrator note, written after 01's review, governs where it differs from this file.
**Gate:**
- 01 landed and reviewed, the gate "Retry" holding;
- on branch `tuning-followups`, no tracked change in `git status`;
- the suites at the counts 01 handed on.

**Closes:** `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`, as superseded by R2 (P8).
**Opens:** only what the work finds. **Recommended model:** **Sonnet** (P11).

**Read first:**

1. [`README.md`](README.md): §0.3, §1, §2.1 (the gate "Quiet"), §5, §6 (R2, U2, U3, P6–P10).
2. 01's log and this board.
3. `jordan-normalization`'s README P12 (why the lines exist), log 06 (the progress line, T6) and
   `.documents/jordan-normalization-verification.md` §1.10 (the silences measured).
4. `ComputeTargets/ScalarHistory.py`: `TuningSecant` (`:1439-1515`), `tune_history` (`:1575-1745`),
   `compute_scalar_history` (`:1748-`); `RunDrivers/registered_run.py:300-313` (the beat);
   `raypoolkit/RayWorkPool.py:354-357`, `:564-595` at the pinned `v0.3.0` (when the pool prints).
5. The callers and tests of README §0.3: `tools/history_and_bbn.py:389`, `tools/surfing_scan.py:306`
   (and, through it, `tools/plot_surfing_histories.py`), `ComputeTargets/tests/golden_tuned_history.py`
   (`run_tuning`), `ComputeTargets/tests/test_tuning.py`, `tools/tests/test_history_and_bbn.py`.
6. `.documents/review-remediation-verification.md` §4.16 (the handover §4.17 amends).

---

## 1. What is wanted

A tuning run by `main.py` prints a tune line only when the history is slow or unusual; the
standalone tools still print one per integration. The heartbeat's bound stays well inside the
900 s window, and no stored value moves.

### 1.1 The items

- **Q1 — the rule and its constants** (P6). In `ComputeTargets/ScalarHistory.py`, beside the
  tuning's other constants (`:1391-1395`):
  - `TUNE_LINE_INTERVAL_S = 120.0` and `TUNE_LINE_SLOW_INTEGRATIONS = 10`, each with a comment
    naming this campaign and the user's ruling ("120s and 10 integrations … we can adjust later");
  - `tune_history(…, progress: str = "quiet", clock=perf_counter)`; `progress` is `"quiet"` or
    `"every"`, anything else a `ValueError`; `clock` times both the rule and each line's `wall=`;
  - with `"quiet"`, integration k (k = 0 the seed's) prints its line, in today's form, when **any**
    of: `clock()` minus the time of the history's last printed line (or of the tuning's start, if
    none) is at least `TUNE_LINE_INTERVAL_S`; k + 1 ≥ `TUNE_LINE_SLOW_INTEGRATIONS`; it was reached
    by a guard step; it was reached by a reset of the secant (the twice rule). `"every"` prints every
    line, as today;
  - the reset is reported by `TuningSecant` (for example an attribute set by `step`, true when the
    twice rule reset B in that step); the stored step counts do not change (a reset step is still a
    plain step);
  - the decision is one small function, testable without an integration;
  - the docstring of `tune_history` says the rule, and why the lines exist (the heartbeat);
  - `compute_scalar_history` passes nothing new, so `main.py`'s histories are quiet. A failure's
    `!!` line is unchanged.
- **Q2 — the tools** (U2, "Keep one line per integration, initially"). `tools/history_and_bbn.py`
  and `tools/surfing_scan.py` pass `progress="every"`, so they and `tools/plot_surfing_histories.py`
  print as today; their docstrings stay true (check each, and say so in the log).
  `golden_tuned_history.run_tuning` passes `**kwargs` through, so a test may give either.
- **Q3 — the tests** (P9), each new test shown to catch a breakage of what it tests (§2):
  - the decision function: no trigger (k small, little time) prints nothing; each trigger alone
    prints (the interval, measured from the start and from the last line; the 10th integration and
    after; a guard; a reset); `"every"` always prints; an unknown `progress` raises;
  - `TuningSecant` reports a reset when the twice rule fires and not otherwise;
  - one real tuning at the tuned golden point (`golden_tuned_history.run_tuning`, five
    integrations, about 20 s) with the default `progress` and a real clock: **no** tune line, and the
    fifteen outputs bitwise the golden record's (test (e) already pins them; this test may share its
    run if the harness allows);
  - the same with a fake clock advancing 200 s per call: a line after every integration, the form
    unchanged;
  - `test_tuning.py`'s existing tests that parse or count lines (`PROGRESS`, `_tune`) run with
    `progress="every"` where they test the line's form or count; each changed test is listed in the
    log with its reason (README §5). `tools/tests/test_history_and_bbn.py:108-109` stands unchanged
    (the tool prints every line).
  - Each real tuning's cost is in its docstring (`CLAUDE.md`).
- **Q4 — the notes** (P10). Dated notes, `> **Note, <date> (`tuning-followups`, prompt 02).**`: in
  `numerical-strategies.md` §3.7.6 (the rule, the constants, the tools' `"every"`, and the bound of
  P7: while a tuning runs, its longest silence is at most `TUNE_LINE_INTERVAL_S` plus one
  integration, about 130 s at the slowest measured, inside the 900 s window, **read, not measured**,
  U3); in `architecture-summary.md` §7.2 (the line) and §2 (the tools). Additive only.
- **Q5 — §4.17** (P10). `.documents/review-remediation-verification.md` §4.17, "Addendum
  2026-10-27 — the `tuning-followups` campaign", in §4.16's form: what the campaign did (01, 02,
  with commits), what it supersedes by statement (§4.16 point 3's "Every integration of a tuning
  prints a `-- tune` line" and its heartbeat bullet; §4.16 point 7's list of issues for the user to
  rule, two of which are now done and the third reported); the run scripts' new option
  `--retry-failed-scalar` and when to use it (after a run whose summary line reports stored scalar
  failures, with a larger `--tuning-max-integrations`); the quiet lines and the bound; RayPoolKit's
  report (its `client-reports` board, RayPoolKit `cf77234`) and the client-side alternative
  `process_batch_size=1`, which this campaign did not take (`rrk-adoption` board, the issue's
  **Reported** line). Nothing above §4.17 is edited.
- **Q6 — `CLAUDE.md`.** The suite counts and their parenthesis; nothing else unless the work makes
  a line false, and then the log says which.

## 2. Verification

1. The five suites before and after, serially, `ComputeTargets` in two halves; counts rising only
   by Q3's tests. Both golden histories and the golden fingerprint pass unchanged.
2. **Deliberate breakages**, each applied, run, reverted, recorded as a diff with the failing test;
   none committed:
   - B1 the rule always prints under `"quiet"`: the no-line test fails;
   - B2 the interval never triggers: the interval tests fail;
   - B3 the slow threshold at 100: its test fails;
   - B4 a guard step not printing: its test fails;
   - B5 the reset never reported: the `TuningSecant` test fails.
3. `tools/history_and_bbn.py 3 0.01` prints one tune line per integration (five), as before, and its
   `tuning` line equals the tuned golden record's (its test, run alone, with its time).
4. `black` on every changed `.py` file.

## 3. Acceptance

1. Q1–Q6 as §1.1 says; the gate "Quiet" (README §2.1) holds.
2. The issue closed: its entry on `jordan-normalization`'s board §3 moved to that board's §4 with a
   **Resolved** line ("superseded by `tuning-followups` R2: …", this prompt's commit); its row
   deleted from `.documents/OPEN_ISSUES.md` §1.17, which is then empty; the count and date
   corrected; this board's §3.1 row marked closed.
3. **One commit**, with the log `logs/02-quiet-tune-lines.md` in `dk-and-rpk-migration` README
   §5.1's form, and the board's row 02, Q1–Q6 and the gate. The orchestrator's review closes the
   campaign (U4).

## 4. Stop conditions: stop and ask

- A stored value, a golden history or the golden fingerprint moves.
- The rule cannot be applied without changing the line's format, the stored step counts, or
  `integrate_history`.
- A test outside §0.3's list turns out to depend on one line per integration from `main.py`'s path.

## 5. What this prompt does not do

- It does not change Ray's deduplication settings, the 900 s window, `--tuning-max-integrations`, or
  `process_batch_size`.
- It starts no Ray (U3) and opens no store.
- **Files it changes:** `ComputeTargets/ScalarHistory.py`, `tools/history_and_bbn.py`,
  `tools/surfing_scan.py`, `ComputeTargets/tests/test_tuning.py` (and
  `ComputeTargets/tests/golden_tuned_history.py` if its pass-through needs it); a new test module if
  the decision's tests are clearer apart; `.documents/architecture-summary.md`,
  `.documents/numerical-strategies.md`, `.documents/review-remediation-verification.md` (§4.17 only),
  `CLAUDE.md`; `prompts/jordan-normalization/IMPLEMENTATION_STATE.md` (the issue's move to §4),
  `.documents/OPEN_ISSUES.md`, this campaign's board, `prompts/INDEX.md` (the row's status), the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
