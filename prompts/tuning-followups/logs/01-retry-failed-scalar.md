# Log 01 — retry failed scalar histories

**Subject:** Add --retry-failed-scalar and order the ScalarHistory lookup
**Commit:** this commit, on branch `tuning-followups`, on top of `f0db26c`
**Date:** 2026-10-27, by Claude Opus 5.5
**Result:** **COMPLETE.** No stop.
- F1–F8 shipped under orchestrator note 01, whose twelve rulings govern where they differ from the
  prompt.
- **The gate "Retry" holds**: on temporary stores, a key holding a failure and a success, in either
  order, or two failures, looks up without raising, through `object_get` and the vectorized route,
  and returns the success if one exists, else the newest failure. The first pass counts a stored
  failure as missing only under the flag (stand-ins). The second pass's `failure=False` is verified
  by reading; its premise is tested on a temporary store. No Ray was started.
- **Nothing a build stores moved.** No spec, schema or stored value changed: `FINGERPRINT_FORMAT`
  and `VERSION_LABEL` are unchanged, and the golden fingerprint and both golden histories pass
  unchanged.
- Counts: CosmologyModels 26, ComputeTargets 250 (77 + 173, +7), ObjectFactories 103 (+10), tools
  115, RunDrivers 64. All passing, before and after.
- Closed: `[08-a-history-that-did-not-converge-is-never-retried]` (`jordan-normalization`). Opened:
  `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]` (ruling 8). The index stays at 25
  open.

## What shipped

### F1 — the flag (`config/argument_parser.py:340-348`)
`--retry-failed-scalar`, `store_true`, default `False`, directly after `--retry-failed-bbn`. Its
help, as `main.py --help` prints it:

```
  --retry-failed-scalar
                        retry scalar histories that failed under the current
                        version label (for example after raising --tuning-max-
                        integrations); the new history is stored beside the
                        failure. Without this flag a stored scalar failure
                        counts as done and is not retried. Not a key (tuning-
                        followups prompt 01)
```

The usage line reads `[--retry-failed-bbn] [--retry-failed-scalar]`. It is not a key. The plot
scripts accept it, from the shared parser (each `--help` lists it twice, in the usage and the
options), and nothing but `main.py` reads `args.retry_failed_scalar`. The wrapper's `scope` does
not name it: `RunDrivers/` is unchanged.

### F2 — the first pass (`pipeline_selection.py:90-126`, `main.py:253-273`)
- `missing_models(models, lookups, retry_failed=False)` returns a list, as before. With
  `retry_failed`, a lookup that is `available` with `failure` true counts as missing, beside the
  models with no row, in order. Without it, the result is as before. The docstring says what the
  flag does.
- `count_stored_failures(lookups)`, beside it, counts the lookups that are `available` with
  `failure` true.
- In `main.py`, `scalar_counts = {"stored_failures": 0}` sits beside
  `scalar_history_failure_reasons` (`:328`). `build_solver_batch` adds each bin's count to it before
  the early `return []` (`:253-258`), so a batch with nothing missing still counts. It passes
  `retry_failed=args.retry_failed_scalar` (`:266-270`).

### F3 — the second pass (`main.py:281-308`)
`retry_payload = {"failure": False} if args.retry_failed_scalar else {}` is splatted into the
second `pool.object_get("ScalarHistory", …)`. With the flag the payload gains `failure=False`.
Without it the call's keywords are exactly as before; no `failure=None` is passed. A comment names
this campaign and says why: with no `failure` in the payload the factory would return the stored
failure, and nothing would be computed.

### F4 — the ordered lookup (`ObjectFactories/ScalarHistory.py:421-435`)
Between the tag joins and `conn.execute(query).one_or_none()`, as in `sqla_BBNDataFactory.build`:
- `failure is True`: `order_by(timestamp desc, serial desc)`, `limit(1)`;
- `failure is None`: `order_by(failure asc, timestamp desc, serial desc)`, `limit(1)`;
- `failure is False`: unchanged, a filter and `one_or_none()`.

The `MultipleResultsFound` handler stays. The comment above names `tuning-followups` prompt 01 and
`run-integrity` prompt 03, says the ordering is invisible while a key holds one row, and says the
limit counts histories.

### F5 — `bbn_from_store` (`tools/bbn_from_store.py:206-218`)
`_FIND_QUERY` gains `and sm.failure = 0`, with a comment: the tool reads a success's samples, and a
retried history has its failure row beside its success. `find_model`'s docstring now says "the one
successful ScalarHistory (`failure = 0`; tuning-followups prompt 01)".

### F6 — the summary line (`main.py:363-373`)
After the queue, after the "histories failed in this run" block and separate from it, only when the
count is positive:

```python
f"-- ScalarHistory: {scalar_counts['stored_failures']} histories with a stored failure retried (--retry-failed-scalar)"
f"-- ScalarHistory: {scalar_counts['stored_failures']} histories skipped because of a stored failure"
```

The first with the flag, the second without. Nothing is printed when the count is 0. No other line
of the stage changed.

### F7 — the tests (17 new)

**Added**
- `ObjectFactories/tests/test_scalar_history_failure_lookup.py` (new, 10 tests, 0.7 s alone at
  load 5), on `_TempStoreCase`, in `test_bbn_failure_lookup.py`'s form:
  - `test_a_any_row_prefers_a_later_success`: a failure, then a success; `failure=None` returns the
    success (serial 2);
  - `test_a2_the_vectorized_lookup_prefers_the_success_too`: the same through `payload_data`;
  - `test_b_any_row_with_two_failures_returns_the_newer`: `failure=None` returns failure 2;
  - `test_c_any_row_prefers_an_earlier_success`: a success, then a failure; `failure=None` returns
    the success (serial 1);
  - `test_d1_failure_true_with_two_failures_returns_the_newer`;
  - `test_d2_failure_true_beside_a_success_returns_the_failure` (a regression guard);
  - `test_e1_failure_false_after_failure_then_success` and
    `test_e2_failure_false_after_success_then_failure` (regression guards);
  - `test_f_failure_false_on_a_stored_failure_finds_nothing`: F3's premise (a regression guard). A
    key holding only a failure, looked up with `failure=False`, is not `available`; with no
    `failure` it is the failure;
  - `test_g_one_row_per_key_is_returned_as_before` (a regression guard): one success under a key is
    returned by `failure=None` and `failure=False`, and `failure=True` finds nothing.
- `ComputeTargets/tests/test_pipeline_selection.py`, 6 tests:
  - `TestRetryFailedScalar` (4): `test_h_a_stored_failure_is_missing_under_the_flag` (`["V1", "V2",
    "V4"]`), `test_h_a_stored_success_is_not_missing_under_the_flag`,
    `test_h_the_default_is_unchanged`, `test_h_count_stored_failures`;
  - `TestRetryFailedScalarFlag` (2): `test_i_the_flag_parses` (`False` without, `True` with, and
    `retry_failed_bbn` still `False`); `test_i_main_py_help_lists_it`, which runs `main.py --help`
    by subprocess from the root (1.3 s alone; argparse exits before `ray.init`).
- `ComputeTargets/tests/test_bbn_from_store.py`, 1 test,
  `test_d_a_failure_beside_the_success_is_not_found`. Test (c)'s store has a failure row added in
  shard 1: serial 6, the target's model and φ, `failure` 1, no samples, no BBN row. `find_model`
  returns serial 7, with its 3 samples and its BBN row.

**Modified**
- `test_bbn_from_store.py`: test (c)'s fixture moved, unchanged, into the module-level
  `_write_imitation_store(tmp, Mp_eV, failed_rows=())`, which (c) calls with no failed rows. (c)'s
  assertions are unchanged. The module docstring gains a paragraph for (d).
- `test_pipeline_selection.py`: a paragraph in the module docstring. `TestScalarStageFirstPass`'s
  five tests and `TestRetryFailedBBNFlag` are unchanged.

### F8 — the notes
Four dated notes, each opening `> **Note, 2026-10-27 (`tuning-followups`, prompt 01)…**`:
- `architecture-summary.md` §4.3, at its end: the ordered lookup, the tag join, and the inventory's
  `duplicate` (ruling 7's sentence);
- §5.1, at its end: the option;
- §9, at its end: the first and second passes, the summary line, and `bbn_from_store`;
- `numerical-strategies.md` §3.7.5, after the paragraph holding "**The cap is not a key**": the
  retry route.

`git diff -U0 <file> | grep '^-[^-]'` is empty for both documents: added lines only.

### The records
- `jordan-normalization`'s board: the issue's entry moved from §3 to §4 with a **Resolved** line
  naming this prompt and "this commit". One sentence was added to §3's introduction.
- `.documents/OPEN_ISSUES.md`:
  - §1.17's row deleted, with a sentence saying so;
  - §1.16 notes the closure;
  - §1.18 added (ruling 8);
  - the header's sentence on the two boards corrected (`jordan-normalization` 6 → 5;
    `tuning-followups` none → 1);
  - the count stays 25 (25 rows) and the date 2026-10-27.
- This board: the header, row 01, §1.1, the gate, §3, §3.1, §4 and §5.2.

## Deviations from the prompt

Every ruling cited is in orchestrator note 01.

1. **`IMPLEMENTATION CHOICE` (ruling 1).** `count_stored_failures` and the closure dict are as
   ruled. `scalar_counts` is bound beside `scalar_history_failure_reasons`, after
   `build_solver_batch`'s definition and before the queue runs, which a closure allows.
2. **`NOTED` (ruling 2).** F6's two lines are exactly the ruled text (above).
3. **`IMPLEMENTATION CHOICE` (ruling 3).** The keyword is added by splatting `retry_payload`, empty
   without the flag. **Verified by reading:** the closure is reachable only through `RayWorkPool`
   and a Ray pool, so no test calls it. Ruling 4's premise test (`test_f`) shows that a
   `failure=False` lookup on a key holding only a failure returns an unavailable history. A
   `failure=False` lookup on a key with no row is unavailable as before, and the factory's `store`
   inserts unconditionally (`ObjectFactories/ScalarHistory.py:655-`; note §0).
4. **`IMPLEMENTATION CHOICE` (ruling 4).**
   - **Fixtures.** A success row carries `solver_serial` 1 and an `IntegrationSolver` row of that
     serial, since `build` raises on a success with no solver label. `z_samples` and every tuning
     column are NULL: with `_do_not_populate` no sample is read, and `_tuning_from_row` reads the
     NULLs back as None.
   - **Split tests.** F7's two `failure=True` and two `failure=False` cases are split into d1/d2 and
     e1/e2. Every `open()` of a test shares one file, so a test can insert the parameter rows only
     once. The first draft, which built two stores in one test, failed with `IntegrityError` on
     `tolerance.serial`.
   - **(g)** is added beyond F7's list, as the regression guard for §4's first stop condition.
5. **`NOTED` (ruling 5).** The tag join, below under Verification.
6. **`IMPLEMENTATION CHOICE` (ruling 6).** The fixture is factored out, not copied (above), and
   `find_model`'s docstring says "successful". (c) is otherwise unchanged and passes.
7. **`NOTED` (ruling 7).** The inventory probe, below.
8. **`NOTED` (ruling 8).** `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]` opened on
   this board's §3, with the lines before and after this commit (`:226-230` → `:230-234`,
   `:268-273` → `:272-277`), and in the index's new §1.18.
9. **`NOTED` (ruling 9).** Counts below.
10. **`STRUCTURALLY REQUIRED` (ruling 10).** B1–B6, below.
11. **`NOTED` (ruling 11).** The four notes, additive only. Each goes at the end of its section,
    and §3.7.5's after the paragraph the prompt names.
12. **`NOTED` (ruling 12).** The boards and the index as listed above.
13. **`IMPLEMENTATION CHOICE`: the parser tests** go in `test_pipeline_selection.py`, beside
    `TestRetryFailedBBNFlag`, where the existing parser test of a retry flag is. "`main.py --help`
    lists it" is a subprocess test of the real script, not of `format_help()`.
14. **`IMPLEMENTATION CHOICE`: the help text** adds two clauses to P1's form: "the new history is
    stored beside the failure" and "Not a key (tuning-followups prompt 01)".
15. **`IMPLEMENTATION CHOICE`: the order.**
    - `black` ran on the eight `.py` files after F5's test and before the breakages, so that the
      breakage diffs and the after-suites are of the formatted tree. All eight were black-clean at
      `f0db26c`. `black` reformatted the two test files; the other six were left unchanged.
    - After the suites, two docstrings were edited (the measured times of the new module and of
      `test_i_main_py_help_lists_it`). Both modules, with `test_bbn_from_store` (c) and (d), were
      re-run after the edit: 29 tests, OK.
16. **`UNINTENDED DRIFT`:** none found.

## Verification performed

**Gate** (at `f0db26c`, 01:58 GMT, load 4.74):
- `git status` clean; the main checkout alone;
- `runregistrykit list`: 11 runs, all finished (`done`, `killed` or `lost`), none `running` or
  stale.

**The suites**, serially, `ComputeTargets` in two halves, each Bash call at 600 000 ms. The load is
the one-minute average from `uptime` at each run's start.

| Suite | Before (`f0db26c`) | Time | Load | After | Time | Load |
|---|---|---|---|---|---|---|
| CosmologyModels | 26 OK | 63.7 s | 4.74 | 26 OK | 65.7 s | 9.34 |
| ComputeTargets `test_[a-f]*` | 76 OK | 97.0 s | 2.99 | 77 OK (+1) | 98.3 s | 5.48 |
| ComputeTargets `test_[g-z]*` | 167 OK | 217.4 s | 2.52 | 173 OK (+6) | 238.7 s | 8.39 |
| ObjectFactories | 93 OK | 16.6 s | 3.13 | 103 OK (+10) | 18.7 s | 40.31 |
| tools | 115 OK | 114.8 s | 15.03 | 115 OK | 112.6 s | 28.74 |
| RunDrivers | 64 OK | 23.7 s | 8.53 | 64 OK | 22.1 s | 8.00 |

- **What each count's change is.** ComputeTargets +7: `test_bbn_from_store` (d), and 4 + 2 in
  `test_pipeline_selection`. ObjectFactories +10: the new module.
- **The goldens.** `test_golden_history` and `test_tuning` (the tuned golden) ran in the second half,
  and `test_store_fingerprint_golden` in ObjectFactories. All passed; no golden file or
  `config/fingerprint.py` is touched.
- Another workload shared the machine during the after-runs (load 40 at one start).

**Show they fail first.** Before F4 was written, the new module ran on the unchanged factory:
10 tests, 5 errors, each `sqlalchemy.exc.MultipleResultsFound`. They were (a), (a2), (b), (c) and
(d1); the other five passed. B1 reproduces this. Test (d) of `test_bbn_from_store`, with F5's filter
removed (B3), raises `LookupError`.

**The tag join (ruling 5).** The joins of `build` are:
- the solver outer join on `IntegrationSolver.serial`, and the three tolerance joins on
  `tolerance.serial`: each is many-to-one, on a primary key;
- each tag join, inner on (history, tag).

`sqla_ScalarHistoryTagAssociation_factory.register` declares both `history_serial` and `tag_serial`
`primary_key=True`. The probe printed the table's DDL, `PRIMARY KEY (history_serial, tag_serial)`. A
second insert of one pair through the store's inserter was refused with `IntegrityError UNIQUE
constraint failed: ScalarHistory_tags.history_serial, ScalarHistory_tags.tag_serial`. `store`
inserts each tag of `obj.tags` once per history. So no join can duplicate a row before the `limit`,
and the `limit` counts histories. **A consequence**, as for `BBNData`: two validated successes under
one key, which no route writes, now give the newer instead of raising.

**The inventory (ruling 7).** The probe was run in the scratch directory and not committed
(`var/tuning-followups-01/probe_inventory.py`, `_GoldenStoreCase`'s harness). It built a one-shard
store with two `ScalarHistory` rows on the n = 1 model (a failure, then a success, under one key)
and two failures on the n = 2 model (under one key). Then `read_inventory`:
- `ScalarHistory records: 4`;
- one problem: `duplicate: ScalarHistory: 1 key-and-tag set(s) are held by more than one record (2
  records, all kept); e.g. shard/serial #0/3, #0/4`.

The success beside its failure is two keys (`failure` is a leaf) and no problem. The two failures
are the named `duplicate`, all kept, and nothing refuses. This confirms §0's reading.

**The parser check** (`PYTHONPATH=. ./venv/bin/python -c` with
`create_argument_parser().parse_args([...])`):

```
without: False
with:    True
```

**`main.py --help`** exits 0 and shows the flag at lines 23 and 118–124 of its output (F1, above).

**`black --check`** passes on the eight changed or new `.py` files.

**Other readers.** A `grep` of the tree outside `venv/`, `thirdparty/` and the tests found one SQL
reader of `ScalarHistory` (`from ScalarHistory`, `join ScalarHistory`, `tables["ScalarHistory"]`):
`tools/bbn_from_store.py`. The prompt's third stop condition did not arise.

## The deliberate-breakage record

Each diff is against this commit's tree, written by `var/tuning-followups-01/make_breakages.py` and
saved there as `B1.diff`–`B6.diff`. Each was checked with `git apply --check` again just before
staging, applied with `git apply`, run, and reversed with `git apply -R`. `git status` afterwards
showed only this prompt's changes. None is committed.

**B1: the ordering removed from F4** (back to `one_or_none()` with no `order_by`).
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -425,14 +425,6 @@
         # exists, else the newest failure. failure=False is a filter only, as before. While a key
         # holds one row, as every store made before tuning-followups does, the ordering is
         # invisible. Each join is many-to-one or a single tag pair, so the limit counts histories.
-        if failure is True:
-            query = query.order_by(table.c.timestamp.desc(), table.c.serial.desc())
-            query = query.limit(1)
-        elif failure is None:
-            query = query.order_by(
-                table.c.failure.asc(), table.c.timestamp.desc(), table.c.serial.desc()
-            )
-            query = query.limit(1)
 
         try:
             row_data = conn.execute(query).one_or_none()
```
`test_scalar_history_failure_lookup`: 5 errors of 10, each `sqlalchemy.exc.MultipleResultsFound:
Multiple rows were found when one or none was required`. They are (a), (a2), (b), (c) and (d1),
the same five as on the tree before F4.

**B2: `missing_models` ignoring `retry_failed`.**
```diff
--- a/pipeline_selection.py
+++ b/pipeline_selection.py
@@ -111,7 +111,7 @@
     return [
         model
         for model, history in zip(models, lookups)
-        if not history.available or (retry_failed and history.failure)
+        if not history.available
     ]
```
`test_pipeline_selection`: 1 failure of 17,
`test_h_a_stored_failure_is_missing_under_the_flag`: `Lists differ: ['V2'] != ['V1', 'V2', 'V4']`.

**B3: F5's filter removed.**
```diff
--- a/tools/bbn_from_store.py
+++ b/tools/bbn_from_store.py
@@ -214,7 +214,6 @@
     join M_value mv on mv.serial = m.M_serial
     join phi_value ph on ph.serial = sm.phi_Einstein_init_serial
     where abs(b.value - ?) < 1e-9 and abs(ph.value_PlanckMass - ?) < 1e-9
-      and sm.failure = 0
 """
```
`test_bbn_from_store` (c) and (d): 1 error of 2. (d) fails with `LookupError: 2 ScalarHistories
match beta=1.6 M=0.001 phi=5: imitation-shard0001.db:6, imitation-shard0001.db:7`. (c) passes, since
it has no failure row.

**B4: the `failure=True` branch ordering ascending.**
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -426,7 +426,7 @@
         # holds one row, as every store made before tuning-followups does, the ordering is
         # invisible. Each join is many-to-one or a single tag pair, so the limit counts histories.
         if failure is True:
-            query = query.order_by(table.c.timestamp.desc(), table.c.serial.desc())
+            query = query.order_by(table.c.timestamp.asc(), table.c.serial.asc())
             query = query.limit(1)
         elif failure is None:
             query = query.order_by(
```
`test_scalar_history_failure_lookup`: 1 failure of 10,
`test_d1_failure_true_with_two_failures_returns_the_newer`: `AssertionError: 1 != 2` (the older
failure).

**B5: the `failure` filter skipped when `failure is False`.**
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -401,7 +401,7 @@
         )
 
         # filter by failure flag if provided
-        if failure is not None:
+        if failure is not None and failure is not False:
             query = query.filter(table.c.failure == failure)
 
         # require that the integration we search for has the specified list of tags
```
`test_scalar_history_failure_lookup`: 1 failure and 2 errors of 10.
- `test_f_failure_false_on_a_stored_failure_finds_nothing` fails: `AssertionError: True is not
  false`. The premise's lookup finds the failure.
- `test_e1…` and `test_e2…` raise `MultipleResultsFound`.

**B6: `count_stored_failures` returns 0 always.**
```diff
--- a/pipeline_selection.py
+++ b/pipeline_selection.py
@@ -124,7 +124,7 @@
 
     :param lookups: the ScalarHistory lookup results of one shard bin
     """
-    return sum(1 for history in lookups if history.available and history.failure)
+    return 0
```
`test_pipeline_selection`: 1 failure of 17, `test_h_count_stored_failures`: `AssertionError: 0 != 2`.

## Observations not acted on

1. **`--tuning-max-integrations`' help is now incomplete.**
   - The help (`config/argument_parser.py:198-202`, jordan-normalization prompt 08) still says a
     later run with a larger cap "does not recompute it (only --drop scalar-history, which drops
     every history, does)". With `--retry-failed-scalar` it does.
   - `ComputeTargets/tests/test_driver_options.py:174` pins "does not recompute it", so changing the
     help would need that assertion changed with it. Neither was in this prompt's scope, so neither
     was touched.
   - The documents' notes of this date say the same thing additively.
   - `CLAUDE.md` invariant 4 would open a §3 issue for this. The note's ruling 8 has the count stay
     at 25, and the user's practice is that cosmetic leftovers are fixed in a standalone commit. So
     I have **not** opened one, and leave it to the orchestrator: open an issue, or make a
     housekeeping commit that changes the help and the one assertion.
2. **`find_model` on a key holding only a failure** now raises "no ScalarHistory with beta=… M=…
   phi=… in …". Before, it returned a `StoredHistory` with no samples. This is the intended reading
   of P5 (note §0), and no test covers it.
3. **The ordering uses `timestamp` before `serial`**, as `BBNData`'s does. A row stored later with
   an earlier `timestamp` (the actor's `datetime.now()`, if the clock stepped back) would rank as
   older; `serial` only breaks ties. A history's rows are all written on its own shard, by that
   shard's actor.
4. **F3 has no seam without Ray.** The second lookup is built inline in `build_solver_batch`'s
   closure. A test of the payload would need a pool, or the payload factored into a pure function,
   which the note ruled out (ruling 3).
5. **`[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]`** is opened (ruling 8), not
   fixed.

## State handed to the next prompt

- **Tree:** this commit, on `tuning-followups`, on top of `f0db26c`.
- **The retry.**
  - `main.py --retry-failed-scalar` recomputes a history stored as a failure under the current
    label and stores the new row beside it. Without the flag nothing changes but the summary line
    (below).
  - The flag is not a key. The plot scripts accept it and ignore it; the wrapper's scope does not
    name it.
- **The lookup.** `sqla_ScalarHistoryFactory.build` returns, with no `failure`, the success if one
  exists, else the newest failure; with `failure=True`, the newest failure; with `failure=False`, the
  success (a filter). This is invisible on stores with one row per key. `FINGERPRINT_FORMAT` and
  `VERSION_LABEL` are unchanged; a `2026.7.0` store reads as before.
- **The summary line.** `-- ScalarHistory: N histories with a stored failure retried
  (--retry-failed-scalar)` or `… N histories skipped because of a stored failure`, only when N > 0.
- **`tools/bbn_from_store.py`** reads successes only.
- **For 02:** `ComputeTargets/ScalarHistory.py` and the tune lines are untouched. The documents
  carry this prompt's notes at the ends of `architecture-summary.md` §4.3, §5.1 and §9, and in
  `numerical-strategies.md` §3.7.5. 02's notes go in §3.7.6.
- **Counts:** CosmologyModels 26, ComputeTargets 250 (77 + 173), ObjectFactories 103, tools 115,
  RunDrivers 64. `CLAUDE.md`'s counts (02's, Q6) are still those of 2026-10-26.
- **Open issues.**
  - Closed `[08-a-history-that-did-not-converge-is-never-retried]`.
  - Opened `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]` (for the user, not
    assigned).
  - The index holds 25. `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` waits for 02.
- **For the orchestrator:** Observation 1, the `--tuning-max-integrations` help.
- **Breakage replay.** `var/tuning-followups-01/B1.diff`–`B6.diff` apply to this commit's tree.
