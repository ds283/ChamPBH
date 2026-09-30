# Prompt 03 — Stop recomputing failed BBN rows; pair lookups correctly

**Campaign:** [`README.md`](README.md) · **Board item:** **R** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and R.
**Closes:**
- `[03-main-recomputes-failed-bbn-rows-on-every-run]` on the `review-remediation` board;
- `[00-main-pairs-lookup-results-against-the-unfiltered-bin]` on this board.

**Recommended model:** **Opus**. It is pipeline logic that no test can reach through `main.py`,
because `main.py` parses arguments and starts Ray at import. So the work is to lift the decision
into a pure helper that a test can reach, and to make `main.py` use it in both stages without
changing anything else.

**Read first:**

1. [`README.md`](README.md) §0.2, §2 (a), §2 (d), §5, §6.3.
2. `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3, the entry for the first issue above,
   and this campaign's `IMPLEMENTATION_STATE.md` §3, the entry for the second.
3. [`planning-probes/pairing_probe.py`](planning-probes/pairing_probe.py). Run it (instant). It
   reproduces the `HEAD~1` list logic, and it is this prompt's breakage record for the helper.
4. [`planning-probes/datastore_version_probe.py`](planning-probes/datastore_version_probe.py), for
   the SQLite pattern. Also `Datastore/tests/`, prompt 01's, for how the tests reach it with the
   version key in place.
5. `Datastore/SQL/ObjectFactories/BBNData.py:116–180`, `BBNData.build`: the failure filter and the
   newest-first order that review-remediation prompt 03 added for `failure=True`.
6. `main.py`:
   - `:299–500`, the adiabatic stage's `build_adiabatic_batch`;
   - `:545–747`, the BBN stage's `build_bbn_data_batch`;
   - the two `RayWorkPool(...).run()` calls after them.
7. `config/argument_parser.py`, `create_argument_parser`.
8. `RayTools/RayWorkPool.py:320–420`: how a lookup result that is `available` skips compute.

---

## 1. The changes

**R1 — `BBNData.build(failure=None)` returns one row.** Order:

1. the success, if one exists;
2. otherwise the newest failure, by timestamp and then serial;

and take one. `failure=True` and `failure=False` behave exactly as now.

**R2 — one pure selection helper.** Put it in a new module, importable without `main.py`, Ray or
a datastore; `pipeline_selection.py` at the root is the suggestion. It provides the two steps both
stages need:

- **Build the query entries.** From a bin's (potential, coupling) pairs and their `ScalarModel`
  lookup results, keep the entries whose model did not fail, **each with its pair and its
  model**. Also report how many were skipped because the model failed.
- **Select the missing entries.** From those entries and the lookup results for them:
  - raise if the lengths differ;
  - an entry is missing if its result is not available;
  - with `retry_failed=True`, an entry is also missing if its result is a stored failure.

Names and signatures are an IMPLEMENTATION CHOICE; the contract is not.

**R3 — both stages use it.** In `build_adiabatic_batch` and `build_bbn_data_batch`:

- the query payload is built from the entries;
- "missing" comes from the helper, paired against those same entries;
- **no zip against `binned_batch[key]` remains** in either function.

The adiabatic stage passes `retry_failed=False`: `AdiabaticHistory` stores no failure rows.

**R4 — a stored BBN failure counts as done.** The BBN query payload passes `failure=None`, and
the helper is called with `retry_failed=args.retry_failed_bbn`.

**R5 — the flag.** Add `--retry-failed-bbn` to `create_argument_parser`: `store_true`, default
`False`. The help text says that without it a BBN computation that failed under the current
label is not retried.

**R6 — a summary line per stage.** After each stage's queue has run, print one line with:

- the number of models skipped because their `ScalarModel` failed;
- for BBN, the number of computations skipped because of a stored failure, or, under the flag,
  the number retried.

Accumulate the counts in `run_pipeline`'s scope. Print nothing else new.

**R7 — documents, additively.** In `.documents/architecture-summary.md`, add a dated note where it
describes the pipeline's stages or `BBNData`. It says what a stored failure now means, what the
flag does, and that a new label retries (prompt 01).

---

## 2. Tests

**(i) `Datastore/tests/test_bbn_failure_lookup.py`.** SQLite, no Ray, rows through the inserter
with explicit increasing serials, lookups through `object_get` (so the version key is present).

- **(a) Two failed rows** for one model: `failure=None` returns the newer. **Must fail on
  `HEAD~1`**, where it raises `MultipleResultsFound`.
- **(b) A failure, then a later success;** and, separately, **a success, then a later failure.**
  `failure=None` returns the success in both.
- **(c) Unchanged behaviour.** On the (a) and (b) data, `failure=True` returns the newest
  failure, and `failure=False` returns the success or nothing.

**(ii) `ComputeTargets/tests/test_pipeline_selection.py`.** Pure. Stand-ins with `available` and
`failure`, as in the probe.

- **(d) The probe's bin.** Five pairs; model 1's `ScalarModel` failed; models 0 and 3 have
  successful BBN rows. Then:
  - missing is {V2, V4};
  - one model is reported skipped;
  - each entry carries the pair it was built from.
- **(e) The retry rule.** The same bin with V3's stored row a failure gives {V2, V4} without the
  flag and {V2, V3, V4} with it.
- **(f) Lengths.** Results one short raise; they do not truncate.
- **(g) The flag.** `create_argument_parser()`:
  - parses `--retry-failed-bbn` to `True`;
  - parses its absence to `False`.

  Supply whatever other arguments the parser requires, and read no config file.

The helper is new, so `HEAD~1` meets (d)–(f) only as an import error. **Their breakage record is
`pairing_probe.py`**, which reproduces `HEAD~1`'s `main.py` logic and gives {V1, V3}. Quote its
output in the log, and show by grep that the zip against `binned_batch[key]` is gone from both
functions. (g) fails on `HEAD~1` because the argument does not exist.

All three suites rise by the methods you add, and must pass.

---

## 3. What this prompt does not do

- No change to what is stored, and no version bump: the label stays `"2026.4.0"`, and
  `PRYM_VERSION` stays `"bf24c3d+cham03+ri02"`.
- No deletion of stored rows.
- No change to `ScalarModel.build` or `AdiabaticHistory.build`, or to step 1 of `run_pipeline`
  (the scalar histories).
- No change to `compute_BBN_data` or to `PRyM/`; those are prompt 02's.
- No change to `plot_by_beta.py`'s failure lookup, which already asks for `failure=True`.

## 4. Acceptance

1. README §6.3, every row, with measured values in the log.
2. The two functions show no remaining zip against the unfiltered bin:
   ```bash
   grep -n "binned_batch\[key\]" main.py
   ```
   The only survivors are where the pairs enter the helper.
3. All three suites pass. `black --check` is clean on the changed files.
4. The board and the index:
   - R done;
   - both issues closed:
     - their rows deleted from `.documents/OPEN_ISSUES.md`;
     - a dated **Resolved** line on the `review-remediation` entry;
     - the planning issue moved to this board's §4;
   - count and date corrected.

## 5. Stop conditions — stop and ask the user

- The helper cannot be imported without importing `main.py` or Ray.
- Making a stored failure count as done needs a change to `RayWorkPool`.
- Either stage's logic turns out to depend on the misalignment in a way the helper cannot
  reproduce correctly.

## 6. The log and the board

`logs/03-failure-caching-and-pairing.md`, in the README §5.1 template. Beyond the template:

- the helper's names and signatures;
- `pairing_probe.py`'s output, quoted as the `HEAD~1` record;
- how (a) and (g) were shown to fail on `HEAD~1`;
- the summary lines' format, verbatim;
- the consequence, on the board's header: **a failed BBN row is final within a label; a new label
  or `--retry-failed-bbn` retries it.**
