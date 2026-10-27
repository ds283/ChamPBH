# tuning-followups campaign — implementation state

**Last updated:** 2026-10-27 · **Status: CLOSED 2026-10-27 — 2 of 2 landed and
reviewed.** 01 landed `8b2a29c` (reviewed `8f92987`); 02 landed in `1103443` (reviewed in the
commit that closes the campaign). One issue left open on this board (§3); none assigned. Merged
into `main`, with `jordan-normalization`, and pushed 2026-10-27 at the user's request (U1; the note
after §5.3). R1–R4 and U1–U4 ruled
by the user 2026-10-27, P1–P12 standing as proposed (README §6).
The plan is [`README.md`](README.md), written on 2026-10-27 on branch `tuning-followups`, cut from
`jordan-normalization` at `4ad15fe`.

The campaign carries out two of the user's rulings of 2026-10-27, before the science rebuild:
- `--retry-failed-scalar`, so that a stored scalar failure (a tuning that hit its cap) can be
  recomputed, with the `ScalarHistory` lookup ordered as `BBNData`'s so that a success and its old
  failures coexist;
- quiet tune lines: a history prints one only after 120 s since its last, from its 10th
  integration, or on a guard or reset step.

**Campaign:** [`README.md`](README.md) ·
**Code:** `config/argument_parser.py`, `pipeline_selection.py`, `main.py`,
`ObjectFactories/ScalarHistory.py`, `tools/bbn_from_store.py`, `ComputeTargets/ScalarHistory.py`,
`tools/history_and_bbn.py`, `tools/surfing_scan.py` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.17 (assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Retry failed scalar histories](01-retry-failed-scalar.md) | `--retry-failed-scalar`; the first and second passes; the ordered lookup; `bbn_from_store`; tests; notes | Opus | ✍️ | ✅ | `8b2a29c` | [01](logs/01-retry-failed-scalar.md) |
| 02 | [Quiet tune lines](02-quiet-tune-lines.md) | the print rule (120 s, 10 integrations, guard, reset); the tools; tests; notes; §4.17; `CLAUDE.md` | Sonnet | ✍️ | ✅ | `1103443` | [02](logs/02-quiet-tune-lines.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Written 2026-10-27, after the user ruled U1–U4. The review of 02 closes the campaign (README §2,
U4).

Orchestrator notes, one prompt at a time, in [`orchestrator/`](orchestrator/):
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`f0db26c`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`0eabc7a`), used for 02.

### 1.1 Items

| Prompt | Items |
|---|---|
| 01 | ✅ F1 the flag · ✅ F2 the first pass · ✅ F3 the second pass (verified by reading; its premise tested) · ✅ F4 the ordered lookup · ✅ F5 `bbn_from_store` · ✅ F6 the summary line · ✅ F7 the tests · ✅ F8 the notes |
| 02 | ✅ Q1 the rule and its constants · ✅ Q2 the tools · ✅ Q3 the tests (19 new) · ✅ Q4 the notes · ✅ Q5 §4.17 · ✅ Q6 `CLAUDE.md` |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Retry | with the flag, a stored scalar failure is recomputed and the success is what every lookup returns; without it, nothing is recomputed; a key with a success and older failures, or two failures, looks up without raising | 01 | ✅ holds (01): the lookups on a temporary store, `object_get` and the vectorized route; the first pass by stand-ins; the second pass's wiring by reading, its premise (`failure=False` on a stored failure finds nothing) on a temporary store; no Ray (U3) |
| Quiet | a tuning under 10 integrations and 120 s prints no tune line; past either it prints by the rule; a guard or reset step prints; the stored values bitwise unchanged | 02 | ✅ holds (02): the rule by `tune_line_due` on numbers and through `tune_history` with the integration scripted (default quiet, guard, reset, 10th integration, a 50 s clock); a real tuning at the tuned golden point, default progress and clock, prints no line (or skips if it ran 120 s or more), and with a clock advancing 200 s per call prints all five; both golden histories and the golden fingerprint pass unchanged; no Ray (U3) |

## 3. Active and unresolved issues

Prompt 01 opened one.

### `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]`

**Opened:** 2026-10-27 by prompt 01 (ruling 8 of its orchestrator note, which found it).

- **What.** `tools/bbn_from_store.py`'s `_BBN_QUERY` (`:230-234` after prompt 01; `:226-230`
  before) selects a history's `BBNData` rows with no `failure` filter and no order, and
  `find_model` raises `LookupError` when it finds more than one (`:272-277`; `:268-273` before).
  `--retry-failed-bbn` stores a recomputed BBN row beside the failure, under the same history, as it
  has since `run-integrity` prompt 03. A scalar retry cannot cause it: the retried success has a new
  serial and its own BBN rows, and a failure has none.
- **Impact.** `bbn_from_store.py` stops with `LookupError` on any history whose BBN computation was
  retried with `--retry-failed-bbn` and so holds a failure and a success. The tool is a diagnostic;
  the pipeline and the plot scripts are unaffected, since they look up through
  `sqla_BBNDataFactory.build`, which orders.
- **Next step.** For the user: order `_BBN_QUERY` as the factory does (the success if one exists,
  else the newest failure), or filter it to the success. Not assigned.

### 3.1 Assigned to this campaign

Assigned 2026-10-27 at planning, from the user's rulings of that day. Each entry stays on its own
board, which carries the **Ruled** and **Assigned** lines; the prompt named closes it there, in that
board's §4, and deletes its row from `.documents/OPEN_ISSUES.md` §1.17.

| Issue | Its board | Prompt | Why here |
|---|---|---|---|
| `[08-a-history-that-did-not-converge-is-never-retried]` | `jordan-normalization` | 01 — closed by 01 (`8b2a29c`) | R1: `--retry-failed-scalar` |
| `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` | `jordan-normalization` | 02 — closed by 02 (`1103443`) | R2: quiet tune lines supersede it |

## 4. Resolved issues

None of this board's own. The assigned `[08-a-history-that-did-not-converge-is-never-retried]` was
closed by prompt 01 (`8b2a29c`) and `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`, as
superseded by R2, by prompt 02 (`1103443`); each entry, with its **Resolved** line, is in
`jordan-normalization`'s §4, and each row is deleted from `.documents/OPEN_ISSUES.md` §1.17, which is
now empty.

## 5. Dispatch record

### 5.0 Planning (2026-10-27)

Asked by the user on 2026-10-27, after `jordan-normalization` closed (10 landed `d638738`, reviewed
`d049296`), with three rulings of the same day recorded in `4ad15fe`. Read at `4ad15fe` (README
§0.2, §0.3):
- `sqla_ScalarHistoryFactory.build` looks up with `one_or_none()` and no ordering, so a retried
  history stored beside its failure would make every later lookup of that key raise
  `MultipleResultsFound`; `BBNData`'s factory orders its lookup (`run-integrity` 03), which is why
  `--retry-failed-bbn` works. Every lookup reaches the factory's `build`, the vectorized ones
  included (`datastorekit/SQL/Datastore.py:565`). `tools/bbn_from_store.py` reads `ScalarHistory`
  by SQL with no `failure` filter.
- The tune lines exist for the heartbeat (`jordan-normalization` P12): without them the silence at
  a block's end is a whole history, since RayPoolKit prints only after a `ray.wait` returns. Three
  standalone tools call `tune_history` directly and show the lines at the terminal; two tests parse
  or count them.

No issue opened; two assigned (§3.1). `.documents/OPEN_ISSUES.md`: 25 open, two of them now in
§1.17.

### 5.1 Rulings and prompts (2026-10-27)

The user ruled U1–U4 the same day: U1 "don't care. `tuning-followups` is fine"; U2 "yes, quiet by
default. Keep one line per integration, initially" (the three standalone tools pass
`progress="every"`); U3 "I think we don't need a rehearsal"; U4 "OK" (two prompts, the review of 02
closes). P1–P12 stand. Prompts 01 and 02 written.

R3 was carried out at the user's word ("If you can add the issue directly to RayPoolKit and commit
it there"): RayPoolKit `cf77234`, a board with no prompts, `client-reports`, holding
`[00-a-pass-blocks-until-process-batch-size-tasks-complete]`. The package documents the wait as
its behaviour, with a default of 1 (RayPoolKit `README.md:161`); ChamPBH passes 3 to its three
titled queues (`main.py:330`, `:581`, `:861`). Changing ChamPBH's `process_batch_size` is not in
this campaign; it is recorded on `rrk-adoption`'s board for the user.

### 5.2 After prompt 01 (2026-10-27)

Prompt 01 landed in `8b2a29c`, under orchestrator note 01 (twelve rulings), on top of `f0db26c`;
its log is [`logs/01-retry-failed-scalar.md`](logs/01-retry-failed-scalar.md).
- **What landed.** `--retry-failed-scalar` (F1); `missing_models(…, retry_failed)` and
  `count_stored_failures` (F2); the second pass's `failure=False` under the flag (F3, verified by
  reading); the ordered lookup in `sqla_ScalarHistoryFactory.build`, `BBNData`'s rule (F4);
  `bbn_from_store`'s `sm.failure = 0` (F5); the summary line, printed only when the first pass found
  a stored failure (F6); 17 new tests (F7); dated notes in `architecture-summary.md` §4.3, §5.1 and
  §9 and `numerical-strategies.md` §3.7.5 (F8). No spec, schema or stored value moved:
  `FINGERPRINT_FORMAT` and `VERSION_LABEL` are unchanged and the golden fingerprint and both golden
  histories pass.
- **Counts**, before and after, serially: CosmologyModels 26 → 26; ComputeTargets 243 → 250
  (76 + 167 → 77 + 173); ObjectFactories 93 → 103; tools 115 → 115; RunDrivers 64 → 64. All passing.
- **Breakages.** B1–B6 (the prompt's four, and the note's B5 and B6), each applied, run and
  reverted; each made its test fail, as the log records. None committed.
- **Found.** The tag join cannot duplicate a row before the `limit`: `ScalarHistory_tags`' primary
  key is (history, tag), and a second insert of one pair is refused. Two failures under one key are
  named `duplicate` by the inventory, all kept, as for `BBNData` (a probe, not committed).
- **Issues.** Closed `[08-a-history-that-did-not-converge-is-never-retried]` (`jordan-normalization`
  §4); opened `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]` (§3; index §1.18). The
  index stays at 25 open.

*Orchestrator review of prompt 01 (2026-10-27).* The note's nine checks, at `8b2a29c`:
1. **Scope.** The commit's 14 files are the note's list exactly; its parent is `f0db26c`; the tree
   clean; nothing pushed. No change to `ComputeTargets/ScalarHistory.py`, `config/fingerprint.py`,
   `RunDrivers/`, the plot scripts, `plot_inputs.py`, `CLAUDE.md`, `prompts/INDEX.md`, the README or
   `orchestrator/`. The trailer appears once in the message (the agent's report of a second was
   wrong).
2. **The factory.** The three branches are `BBNData.py:195-210`'s, with its order keys and `limit 1`;
   `failure=False` is a filter, as before. B1 replayed: (a), (a2), (b), (c), (d1) raise
   `MultipleResultsFound`; B4: (d1) fails, `1 != 2`.
3. **The passes.** The counter is incremented before the early return (ruling 1); the second
   lookup's arguments are today's without the flag, `**retry_payload` being empty (ruling 3); the two
   summary lines are ruling 2's, in `main.py:872-877`'s form. B2 replayed: `Lists differ: ['V2'] !=
   ['V1', 'V2', 'V4']`; B6: `0 != 2`.
4. **The premise, and the gate end to end.** The new module passes alone (10 tests, 0.7 s); B5
   replayed: (f) fails, (e1) and (e2) raise. A probe of the orchestrator's (session scratchpad,
   `probe_retry.py`, not committed) on a `_TempStoreCase` store, through the factory's own `store`
   and `validate`: a stored failure is found by the first pass's lookup; `failure=False` finds
   nothing; a success stored beside it is then returned by the default lookup, the vectorized one
   and `failure=False`, while `failure=True` returns the failure; a later failure leaves the success
   returned. **The gate "Retry" holds**, short of Ray (U3).
5. **`bbn_from_store`.** The filter and its comment; (d) passes, (c)'s assertions unchanged. B3
   replayed: (d) raises `LookupError: 2 ScalarHistories match …`.
6. **The flag.** `main.py --help`, `plot_by_beta.py --help` and `plot_ScalarHistory.py --help` list
   it; only `main.py` reads `args.retry_failed_scalar`; `RunDrivers/registered_run.py` does not name
   it.
7. **Values.** Under `config/` and `ComputeTargets/` (tests aside) only the parser changed; the
   golden histories and the golden fingerprint pass in the suites below.
8. **The records.** The log is in §5.1's form, every ruling classed; both documents' diffs remove no
   line; the boards and the index agree (25 open, §1.17 one row, §1.18 one row). Residue fixed in
   this review's commit: "this commit" → `8b2a29c` in the log (×5) and on this board (×4) and
   `jordan-normalization`'s (×1); the **Resolved** line's "each shown to fail with its subject
   broken" corrected to eleven of the 17 (the other six are regression guards and parser tests, as
   the log says).
9. **The suites** at `8b2a29c`, serially: CosmologyModels 26, ComputeTargets 77 + 173,
   ObjectFactories 103, tools 115, RunDrivers 64, all passing (one-minute load 4–11), the agent's
   counts. `black --check` on the eight changed
   `.py` files: unchanged.

The log's Observation 1 (`--tuning-max-integrations`' help still says a larger cap does not
recompute a failure) is cosmetic: fixed in a standalone housekeeping commit after this one, the help
naming `--retry-failed-scalar`, with no issue opened.

### 5.3 After prompt 02 (2026-10-27)

Prompt 02 landed in `1103443`, under orchestrator note 02 (twelve rulings), on top of `0eabc7a`; its
log is [`logs/02-quiet-tune-lines.md`](logs/02-quiet-tune-lines.md).
- **What landed.** `tune_line_due` and the constants `TUNE_LINE_INTERVAL_S` (120 s),
  `TUNE_LINE_SLOW_INTEGRATIONS` (10) and `TUNE_LINE_PROGRESS` (Q1); `tune_history(…, progress="quiet",
  clock=perf_counter)`, which calls the clock exactly as ruling 1 fixes, validates `progress` at entry,
  and prints integration k's line only when the rule says; `TuningSecant.reset`, set only by the twice
  rule, carried across the loop with `next_kind` (ruling 2); `progress="every"` in
  `tools/history_and_bbn.py` and `tools/surfing_scan.py` (Q2); 19 new tests in `test_tuning.py` and
  five existing ones given `progress="every"` or the default with a timed check (Q3); dated notes in
  `numerical-strategies.md` §3.7.6 and `architecture-summary.md` §2 and §7.2 (Q4); §4.17 of the
  verification document (Q5); `CLAUDE.md`'s counts (Q6). No spec, schema or stored value moved:
  `FINGERPRINT_FORMAT` and `VERSION_LABEL` are unchanged and the golden fingerprint and both golden
  histories pass.
- **The bound.** While a tuning runs, its longest silence is at most 120 s plus one integration (about
  130 s at the slowest measured); the run's, at most RayWorkPool's 300 s floor plus that, about 430 s.
  Both read, not measured (U3), both inside the 900 s window (ruling 4).
- **Counts** are in the log. **Breakages** B1–B7 and B4b, each applied, run and reverted; each made its
  named tests fail. None committed.
- **Issues.** Closed `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` as superseded by R2
  (`jordan-normalization` §4); opened none. The index goes 25 → 24 open, §1.17 empty.

*Orchestrator review of prompt 02 (2026-10-27), which closes the campaign.* The note's nine checks,
at `1103443`:
1. **Scope.** The commit's 13 files are the note's list exactly; its parent is `0eabc7a`; the tree
   clean; nothing pushed. No change under `config/`, `main.py`, `RunDrivers/`, `ObjectFactories/`,
   `pipeline_selection.py`, `tools/tests/`, `golden_tuned_history.py` or the golden data.
2. **The rule.** `tune_line_due` and the three constants are ruling 1's; the clock is called once
   before the loop and once either side of each integration, `wall = end - start`, `now = end`, and
   `last_line` moves only on a printed line; the line's f-string is `2c92d77`'s byte for byte; the
   `!!` lines untouched; `progress` refused before the seed; `compute_scalar_history` unchanged.
   B1 replayed: 11 failures (6 of (k), 5 of (m)); B2: 3; B3: 3.
3. **The reset and the guard.** `TuningSecant.reset` is ruling 2's, the (b) tests unmodified; the
   carry in `tune_history` is read after `step` and false on a guard round. B4: 2 failures; B4b: 1
   ((m) guard); B5: 2 (both (l)); B6: 1 ((m) reset).
4. **The default, and the gate end to end.** B7: 5 failures of (m). A probe of the orchestrator's
   (session scratchpad, `probe_quiet.py`, not committed) at the golden point: through
   `compute_scalar_history`'s undecorated function, with nothing new passed (`main.py`'s path short
   of Ray), **no** tune line in 21.8 s; through `tune_history`'s default with a clock advancing 200 s
   per call, five lines in today's form (`wall=200.0`, kinds seed, plain, secant ×3); both runs
   bitwise the golden record. **The gate "Quiet" holds**, short of Ray (U3).
5. **The tools.** Both calls pass `"every"` with the comment; `plot_surfing_histories.py` reaches
   `tune_history` only through `surfing_scan.run_history`. `tools/tests/test_history_and_bbn.py` (a)
   alone: OK, 22.4 s, five lines. Besides these, `tune_history` is called only by
   `compute_scalar_history`, `golden_tuned_history.run_tuning` and `jordan-normalization`'s dated probe
   `logs/06-probes/compare.py`, which would now run quiet; a record, left as written.
6. **The changed tests.** Among the existing tests only (c)'s setUpClass, (d), (f), (g), (h) and the
   module docstring changed, each as ruling 3 says and as the log lists; (d)'s bitwise assertions are
   factored into a helper unchanged and run first.
7. **Values.** (d), (e) and (n) pass bitwise, and the golden fingerprint and `golden_history_05` in
   the suites below; (d) did not skip.
8. **The records.** The log is in §5.1's form, every ruling classed; the three documents' diffs remove
   no line; §4.17 carries Q5's content and ruling 4's two bounds; `CLAUDE.md`'s counts are the suites';
   the index holds 24 rows, as its header says, §1.17 empty. Residue fixed in this review's commit:
   "this commit" → `1103443` in the log (×8), on this board (×5), on `jordan-normalization`'s (×1), in
   `prompts/INDEX.md` (×1) and in §4.17 (×1); the index's §1.17 paragraph, which read as if both
   issues had been superseded by the quiet lines, and its header's "opened and closed none, and
   closed", reworded. The commit message's "each shown to fail with its subject broken" holds for 13
   of the 19 new tests (the agent's own report): `every`, the unknown-`progress` tests, (l)'s first
   and (n) are regression guards no breakage targets. The message is not amended.
9. **The suites** at `1103443`, serially: CosmologyModels 26, ComputeTargets 77 + 192, ObjectFactories
   103, tools 115, RunDrivers 64, all passing (one-minute load 2–12), the agent's counts. `black
   --check` on the four changed `.py` files: unchanged.

**The campaign closes** with this review: 2 of 2 prompts landed and reviewed, both gates holding,
both assigned issues closed, one opened (§3, index §1.18) and left for the user. Not done here: the
merge of `tuning-followups` (which contains `jordan-normalization`) into `main` (U1), and the
client-side `process_batch_size` alternative to RayPoolKit's wait (§4.17 point 3), both the user's.

**Merged and pushed (2026-10-27).** At the user's request `main`, at `396526b` and an ancestor of
this branch, fast-forwards to the commit that adds this paragraph, which brings `jordan-normalization`
with it, and `main` is pushed to GitHub (`github/main` was `a144c83`, so the push carries
`ray-sqla-align` too). The branches `tuning-followups` and `jordan-normalization`, never pushed and
both contained in `main`, are deleted. The user does not want the client-side `process_batch_size=1`
alternative for now (§4.17 point 3); RayPoolKit's report (`cf77234`) stays in RayPoolKit.
