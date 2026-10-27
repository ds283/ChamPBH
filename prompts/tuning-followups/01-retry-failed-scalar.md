# Prompt 01 — retry failed scalar histories

**Campaign:** [`README.md`](README.md) · **Board items:** **F1**–**F8** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-27, after the user's rulings (README §6.0 R1; §6.1 U1–U4; P1–P5, P9–P12). Its
orchestrator note, written before dispatch, governs where it differs from this file.
**Gate:**
- on branch `tuning-followups`, no tracked change in `git status`;
- the suites at the counts of README §0.4.

**Closes:** `[08-a-history-that-did-not-converge-is-never-retried]` (on `jordan-normalization`'s
board, which carries its **Ruled** and **Assigned** lines). **Opens:** only what the work finds.
**Recommended model:** **Opus** (P11). The lookup it changes is read by every stage and both plot
scripts.

**Read first:**

1. [`README.md`](README.md): §0.1, §0.2, §1, §2.1 (the gate "Retry"), §5, §6.
2. `jordan-normalization`'s board §3, the issue's entry; its note 08, ruling 14 (why the cap is not a
   key); `run-integrity`'s prompt 03 and log (the ordered BBN lookup this copies).
3. The code of README §0.2: `pipeline_selection.py` (`missing_models`, `select_missing`),
   `main.py`'s scalar stage (`:180-345`) and BBN stage's retry (`:700-880`),
   `ObjectFactories/ScalarHistory.py` (`sqla_ScalarHistoryFactory.build`, `:317-` ) and
   `ObjectFactories/BBNData.py:140-215`, `config/argument_parser.py:330-340`,
   `tools/bbn_from_store.py:200-290`.
4. The tests that cover them: `ComputeTargets/tests/test_pipeline_selection.py`
   (`TestScalarStageFirstPass`), `ObjectFactories/tests/test_version_keyed_lookups.py`
   (`_TempStoreCase`, the harness), `ComputeTargets/tests/test_bbn_from_store.py`.

---

## 1. What is wanted

With `--retry-failed-scalar`, a scalar history stored as a failure is recomputed, and the new row is
stored beside the failure; every lookup of that key then returns the success if one exists, else the
newest failure. Without the flag nothing changes.

### 1.1 The items

- **F1 — the flag** (P1). `--retry-failed-scalar`, `store_true`, default off, in the shared parser
  beside `--retry-failed-bbn`, with help of the same form: retry scalar histories that failed under
  the current version label (for example after raising `--tuning-max-integrations`); without it a
  stored failure counts as done and is not retried. It is not a key. The plot scripts accept it and
  do not read it, as they do `--retry-failed-bbn`; the wrapper's `scope` does not name it.
- **F2 — the first pass** (P2). `missing_models(models, lookups, retry_failed=False)`: with
  `retry_failed`, a lookup that found a stored failure counts as missing too. **Its return stays a
  list**, so that `TestScalarStageFirstPass`'s five tests stand unchanged (this narrows P2, which
  said "as `select_missing` does"); the count of stored failures, for F6, comes from a small function
  beside it, `count_stored_failures(lookups)`. `main.py` passes `args.retry_failed_scalar`.
- **F3 — the second pass** (P3). With the flag, the second lookup (`main.py:270-292`) adds
  `failure=False` to its payload, so that the factory does not find the stored failure, returns an
  unpopulated history, and the pool computes and stores it. Without the flag the payload is exactly
  today's. If a seam lets a test check the payload without Ray, the test is written; otherwise the
  log says the item is verified by reading, and why.
- **F4 — the ordered lookup** (P4). In `sqla_ScalarHistoryFactory.build`, the rule of
  `sqla_BBNDataFactory.build` (`ObjectFactories/BBNData.py:195-210`), with a comment naming this
  campaign and `run-integrity` 03:
  - `failure=None` (the payload has no `failure`, as every lookup but F3's): order by `failure`
    ascending, then `timestamp` descending and `serial` descending, and `limit 1`, so that the
    success is returned if one exists, else the newest failure;
  - `failure=True`: the newest failure, by `timestamp` and `serial` descending, `limit 1`;
  - `failure=False`: unchanged (a filter, and `one_or_none()`).

  The `MultipleResultsFound` handler stays for the `failure=False` case. Nothing in the spec, the
  schema or a stored value changes: **`FINGERPRINT_FORMAT` and `VERSION_LABEL` do not move**, and the
  golden fingerprint test passes unchanged. Check that the tag join cannot duplicate a row before the
  `limit` (a history carries each tag once) and say so in the log.
- **F5 — `bbn_from_store`** (P5). `_FIND_QUERY` (`tools/bbn_from_store.py:206-214`) gains
  `and sm.failure = 0`, with a comment: the tool reads a success's samples, and a retried history
  has a failure row beside its success.
- **F6 — the summary line** (P2). After the scalar queue (`main.py:338-345`), when the first pass
  found any stored failure: `-- ScalarHistory: N histories with a stored failure retried
  (--retry-failed-scalar)` with the flag, or `-- ScalarHistory: N histories skipped because of a
  stored failure` without it. Nothing is printed when N is 0, so a clean run prints what it prints
  today.
- **F7 — the tests** (P9), each new test shown to catch a breakage of what it tests (§2):
  - `missing_models` with `retry_failed` (a stored failure missing; a stored success not; the
    default unchanged) and `count_stored_failures`;
  - on `_TempStoreCase`, the ordered lookup by `object_get`: a failure then a success under one key
    (the success returned); two failures (the newer); a success then a failure (the success);
    `failure=True` (the newest failure); `failure=False` with a failure and a success (the success);
    and the same through `object_get_vectorized` for at least the first case;
  - `bbn_from_store`'s query on a fixture with a failure row beside a success (one history found);
  - the parser: `--retry-failed-scalar` parses, default `False`, and `main.py --help` lists it.
- **F8 — the notes** (P10). Dated notes, `> **Note, <date> (`tuning-followups`, prompt 01).**`, in
  `architecture-summary.md` (§5.1, the option; §9, the stage's first and second passes and the
  summary line; §4.3, the ordered lookup) and `numerical-strategies.md` (§3.7.5, after the
  paragraph "**The cap is not a key**": the retry route). Additive only: no existing line deleted
  or reworded (`CLAUDE.md` invariant 6).

## 2. Verification

1. The five suites before and after, serially, `ComputeTargets` in two halves; every count at least
   README §0.4's, rising only by the tests F7 adds. Both golden histories and the golden fingerprint
   pass unchanged.
2. **Deliberate breakages**, each applied, run, and reverted, the diff and the failing test recorded
   in the log; none committed:
   - B1 the ordering removed from F4 (back to `one_or_none()` with no `order_by`): the
     failure-then-success test raises `MultipleResultsFound` or returns the failure;
   - B2 `missing_models` ignoring `retry_failed`: its new test fails;
   - B3 F5's filter removed: the `bbn_from_store` test finds two histories;
   - B4 the `failure=True` branch ordering ascending: the newest-failure test fails.
3. `main.py --help` shows the flag; `PYTHONPATH=. ./venv/bin/python -c` with
   `config.argument_parser.create_argument_parser().parse_args([...])` gives
   `retry_failed_scalar` `False` without it and `True` with it.
4. `black` on every changed `.py` file.

## 3. Acceptance

1. F1–F8 as §1.1 says; the gate "Retry" (README §2.1) holds.
2. The issue closed: its entry on `jordan-normalization`'s board §3 moved to that board's §4 with a
   **Resolved** line naming this prompt's commit; its row deleted from `.documents/OPEN_ISSUES.md`
   §1.17, the count and date corrected; this board's §3.1 row marked closed.
3. **One commit**, with the log `logs/01-retry-failed-scalar.md` in `dk-and-rpk-migration` README
   §5.1's form, and the board's row 01, F1–F8 and the gate.

## 4. Stop conditions: stop and ask

- A lookup that the ordering would change for a store with **no** duplicate keys (every existing
  test must pass unchanged): the ordering must be invisible until a retry stores a second row.
- The second pass cannot be made to skip the failure without changing the factory's handling of
  `_do_not_populate` or anything in DatastoreKit.
- A reader other than `bbn_from_store` found to bypass the factory and read `ScalarHistory` rows
  without a `failure` filter: record it and ask before changing it.

## 5. What this prompt does not do

- It does not put the cap in the key, change `--tuning-max-integrations`, or touch the tune lines
  (02).
- It does not change `process_batch_size` or anything of RayPoolKit's wait.
- It starts no Ray and opens no store but temporary ones.
- **Files it changes:** `config/argument_parser.py`, `pipeline_selection.py`, `main.py`,
  `ObjectFactories/ScalarHistory.py`, `tools/bbn_from_store.py`; the tests of F7
  (`ComputeTargets/tests/test_pipeline_selection.py`, a module in `ObjectFactories/tests/`,
  `ComputeTargets/tests/test_bbn_from_store.py`, and a parser test where the existing parser tests
  are); `.documents/architecture-summary.md`, `.documents/numerical-strategies.md`;
  `prompts/jordan-normalization/IMPLEMENTATION_STATE.md` (the issue's move to §4),
  `.documents/OPEN_ISSUES.md`, this campaign's board, the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
