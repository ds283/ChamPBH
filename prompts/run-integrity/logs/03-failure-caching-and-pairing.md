# Log 03 — Stop recomputing failed BBN rows; pair lookups correctly

**Prompt:** prompts/run-integrity/03-failure-caching-and-pairing.md
**Commit:** the commit that adds this file ("Cache failed BBN rows and pair stage lookups by entry"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE WITH DEVIATIONS

Every README §6.3 row is met (Verification). No stop condition of prompt §5 was met: the helper
imports neither `main.py`, Ray nor a datastore; `RayWorkPool` is unchanged, because a stored
failure is taken out before any work is scheduled; and neither stage depended on the
misalignment. The deviations are all IMPLEMENTATION CHOICEs.

**Consequence: a failed BBN row is final within a label; a new label or `--retry-failed-bbn`
retries it.**

## What shipped

`VERSION_LABEL` `"2026.4.0"` before and after (`config/version.py:33`). `PRYM_VERSION`
`"bf24c3d+cham03+ri02"` before and after (`ComputeTargets/BBNData.py:42`). No schema change, no
stored row changed, and no change to `ComputeTargets/BBNData.py`, `PRyM/`, `RayTools/` or step 1
of `run_pipeline`.

**R1 — `BBNData.build(failure=None)` returns one row**
(`Datastore/SQL/ObjectFactories/BBNData.py:173–187`).

- **Before.** `failure=None` added no failure filter, no order and no limit. `one_or_none()`
  raised `MultipleResultsFound` once two rows existed for one model, whatever their flags.
- **After.** A new `elif failure is None:` branch orders by
  `failure ASC, timestamp DESC, serial DESC` and takes `LIMIT 1`. That is the success if one
  exists, and otherwise the newest failure.
- **Unchanged.** The `failure is True` branch (newest failure, review-remediation prompt 03)
  and `failure=False` (no order, no limit).
- **The comment above the `failure is True` branch** began "main.py looks up successes only",
  which stops being true in this commit. Its first sentence is removed; the rest is kept, and the
  new branch has its own comment.

**R2 — the pure helper, `pipeline_selection.py` (new, at the root).** It imports only
`dataclasses` and `typing`. The new public symbols:

- `@dataclass(frozen=True) class QueryEntry(potential, coupling, model)`: one pair of a shard
  bin, with its `ScalarModel` lookup result.
- `@dataclass(frozen=True) class QueryEntries(entries: List[QueryEntry], unavailable:
  List[Tuple[Any, Any]], skipped_failed_models: int)`.
- `@dataclass(frozen=True) class MissingSelection(missing: List[QueryEntry], stored_failures:
  int)`.
- `build_query_entries(pairs: Sequence[Tuple[Any, Any]], model_results: Sequence[Any]) ->
  QueryEntries`.
  - It raises `ValueError` if the lengths differ.
  - A model that is not `available` goes to `unavailable`, and its `failure` is not read.
  - A model with `failure` true is counted in `skipped_failed_models`.
  - Every other pair becomes a `QueryEntry`, in bin order.
- `select_missing(entries: Sequence[QueryEntry], results: Sequence[Any], retry_failed: bool =
  False) -> MissingSelection`.
  - It raises `ValueError` if the lengths differ.
  - An entry is missing if its result is not `available`.
  - An available result with `failure` true is counted in `stored_failures`. It is missing only
    with `retry_failed=True`.
  - A result with no `failure` attribute is never a stored failure. `AdiabaticHistory` has none.

**R3 — both stages use it** (`main.py`, `build_adiabatic_batch` and `build_bbn_data_batch`).

- **The import.** `from pipeline_selection import build_query_entries, select_missing` (`:66`).
- **The `ScalarModel` check** (`:354–368` and `:607–621`).
  - **Before.** `missing_models` zipped the model lookups against `binned_batch[key]` to find
    unavailable models. That zip was correct, since both had the bin's length.
  - **After.** `query_entries = {key: build_query_entries(binned_batch[key], query_outcomes)}`,
    and the same `RuntimeError`, with the same message, if any entry's `unavailable` is
    non-empty.
- **The downstream query payload** (`:370–385`, `:623–640`). It is built from
  `query_entries[key].entries`, as `ScalarModelProxy(entry.model)`. Before, it was built from
  `for obj in query_outcomes if not obj.failure`.
- **"Missing"** (`:404–420`, `:659–679`).
  - `selections = [select_missing(query_entries[key].entries, query_outcomes, retry_failed=...)]`.
  - `missing_adiabatic` / `missing_bbn` are `(entry.potential, entry.coupling)` for each
    `selection.missing`, in the same `{"shard_key", "missing"}` shape as before. Everything after
    it is unchanged: the re-lookup of the `ScalarModel`s without `_do_not_populate`, and the
    `pool.object_get(...)` work refs.
  - The adiabatic stage passes `retry_failed=False`.

**R4 — a stored BBN failure counts as done.**

- **The lookup.** The BBN query payload carries `"failure": None` (`:632`).
- **The retry rule.** `select_missing` is called with `retry_failed=args.retry_failed_bbn`
  (`:665`).
- **The work lookup.** The later `pool.object_get("BBNData", ...)` that builds the work items
  still uses the default `failure=False`. For a retried entry, whose only rows are failures, it
  returns an unavailable object, so `RayWorkPool` computes it.

**R5 — the flag** (`config/argument_parser.py:245–251`). `--retry-failed-bbn`,
`action="store_true"`, `default=False`. Help text, verbatim: "retry BBN computations that failed
under the current version label; without this flag a stored BBN failure counts as done and is not
retried".

**R6 — the summary lines.**

- **The counters.** Two dicts in `run_pipeline`'s scope: `adiabatic_counts =
  {"skipped_failed_models": 0}` (`:283`) and `bbn_counts = {"skipped_failed_models": 0,
  "stored_failures": 0}` (`:537`). The batch builders add to them; the builders are closures run
  in the driver by `RayWorkPool.run` (`RayWorkPool.py:266`).
- **The lines.** Printed straight after `adiabatic_queue.run()` (`:530`) and
  `bbn_data_queue.run()` (`:787–794`). The format, verbatim:
  ```
  -- AdiabaticHistory: {n} models skipped because their ScalarModel failed
  -- BBNData: {n} models skipped because their ScalarModel failed; {m} computations skipped because of a stored failure
  -- BBNData: {n} models skipped because their ScalarModel failed; {m} computations with a stored failure retried (--retry-failed-bbn)
  ```
  The second BBN form is used under the flag. Nothing else new is printed.

**R7 — documents.** `.documents/architecture-summary.md` §9 has a new dated note after the
production-readiness P2 note. It says what a stored failure now means, what the flag does, that a
new label retries (prompt 01), and what `pipeline_selection` does. Nothing existing was edited.

**Tests (new).**
- `Datastore/tests/test_bbn_failure_lookup.py`: six methods, (a), (b1), (b2), (c1)–(c3).
- `ComputeTargets/tests/test_pipeline_selection.py`: six methods, (d), (e), (e2), (e3), (f), (g).

## Deviations from the prompt

### 1. The helper's names, types and exception — IMPLEMENTATION CHOICE

The prompt leaves names and signatures open.

- **The shape.** Two functions returning frozen dataclasses, not tuples. A tuple return
  (`entries, skipped`) was the alternative. Named fields make each call site in `main.py` say
  which count it reads, and the summary counters read two different fields.
- **The exception.** `ValueError`, because the lengths are wrong values of the right type.
  `RuntimeError`, which `main.py` uses elsewhere, was the alternative. The tests pin
  `ValueError`.

### 2. `build_query_entries` also reports unavailable models — IMPLEMENTATION CHOICE

- **What the prompt asked for.** The first step keeps the entries whose model did not fail.
- **The problem.** The stages also check that every `ScalarModel` exists, through a zip of the
  model lookups against `binned_batch[key]`. That zip was correct, since both lists have the bin's
  length. But it would survive as a zip against the unfiltered bin, and R3 says none remains.
- **What was done.** The helper returns the unavailable pairs as `unavailable`, and `main.py`
  raises the same `RuntimeError`, with the same message and count, when any exist.
- **Why.** The alternative was to keep that one zip, as correct, and explain it in the log. That
  would leave the grep in prompt §4 item 2 with a survivor that is not where the pairs enter the
  helper.
- **A side effect.** The helper does not read `failure` on an unavailable model. The old code
  never reached `failure` on one either, because it raised first.

### 3. `failure=None` with two successes returns the newest — IMPLEMENTATION CHOICE

- **The behaviour.** One query, ordered `failure ASC, timestamp DESC, serial DESC`, `LIMIT 1`.
  With two successful rows for one model it returns the newer, where before it raised
  `MultipleResultsFound`.
- **The alternative.** Two queries: `one_or_none()` on the successes, which keeps the raise; then,
  if none, the newest failure.
- **Why one query.** The prompt specifies the order and "take one". Two successes for one model
  within one label need two completed, validated computations of the same model. `main.py` never
  schedules that, because a success counts as done. `failure=False` still raises in that case, and
  so does every other lookup path. The difference is not tested.

### 4. A result without `failure` is never a stored failure — IMPLEMENTATION CHOICE

- **The problem.** `AdiabaticHistory` has no `failure` attribute, but the adiabatic stage calls
  the same `select_missing`.
- **What was done.** `select_missing` reads `getattr(result, "failure", False)`, and test (e2)
  pins it.
- **The alternative.** A `results_can_fail` parameter. It would duplicate what `retry_failed=False`
  plus the attribute already say, and it would count nothing new.

### 5. Tests beyond (a)–(g) — IMPLEMENTATION CHOICE

- **(b) and (c) are split into methods,** (b1)/(b2) and (c1)–(c3). Each has its own temporary
  file, so there is no state shared across subtests.
- **(e2)** pins deviation 4. **(e3)** pins deviation 2: an unavailable `ScalarModel` with no
  `failure` attribute is reported, not read.
- **(f)** also checks one result too many, and the `build_query_entries` step as well as
  `select_missing`.
- **The shared stand-ins.** `test_bbn_failure_lookup` imports its stand-ins and inserters from
  prompt 01's `test_version_keyed_lookups`, rather than copying them.

### 6. What the summary counters cover — IMPLEMENTATION CHOICE

- **The scope.** The counters sit in `run_pipeline`'s scope, as the prompt says. So they cover
  one cosmology model's pass, and the summary is printed once per model in `build_model_list()`.
  There is one model today.
- **The count.** "Models skipped because their `ScalarModel` failed" counts (potential, coupling)
  pairs. The BBN count is `stored_failures`: without the flag it is the computations skipped;
  with the flag, the computations retried.

## Verification performed

All run from the repository root with `venv/bin/python`, on `765e80d` (`HEAD~1` for this commit)
plus this prompt's diff, unless stated.

**Suite counts** (I ran these):

| Suite | Before (`765e80d`) | After |
|---|---|---|
| `CosmologyModels/tests` | 18, OK | 18, OK |
| `ComputeTargets/tests` | 35, OK | 41, OK |
| `Datastore/tests` | 11, OK | 17, OK |

Every pinned abundance in `ComputeTargets/tests/` passes unchanged. This prompt touches no
function those tests reach.

**The new tests on `HEAD~1`** (I ran this). A detached worktree of `765e80d` was made in the
scratchpad, with only the two new test files copied in. Then:

```
PYTHONPATH=. venv/bin/python -m unittest Datastore.tests.test_bbn_failure_lookup ComputeTargets.tests.test_pipeline_selection
```

It printed `Ran 12 tests`, `FAILED (errors=9)`:

- **(a), (b1), (b2)** error with `sqlalchemy.exc.MultipleResultsFound: Multiple rows were found
  when one or none was required`. (a) is the one the prompt requires to fail.
- **(c1)–(c3)** pass. They are regression guards for the unchanged filters, and their module
  docstring says so.
- **(d), (e), (e2), (e3), (f)** error with `ModuleNotFoundError: No module named
  'pipeline_selection'`.
- **(g)** errors with `AttributeError: 'Namespace' object has no attribute 'retry_failed_bbn'`.

The worktree was removed afterwards.

**The breakage record for (d)–(f): `planning-probes/pairing_probe.py`** (I ran it on
`765e80d`). It reproduces `HEAD~1`'s list logic:

```
pairing: [('V0', 'BBN result of model 0'), ('V1', 'BBN result of model 2'), ('V2', 'BBN result of model 3'), ('V3', 'BBN result of model 4')]
main.py schedules BBN for: ['V1', 'V3']
the correct set:           ['V2', 'V4']
V1's ScalarModel failed; compute_BBN_data reads model.values, which raises RuntimeError for a failed model (ComputeTargets/ScalarModel.py:1156), outside every except clause.
```

**README §6.3, row by row:**

| Quantity | Target | Measured | Witness |
|---|---|---|---|
| `failure=None`, two failed rows | the newest failure | store_id 2, reason `"second failure"`; `MultipleResultsFound` on `HEAD~1` | (a), run both sides |
| same, failure then later success; success then later failure | the success, both orders | store_id 2 and store_id 1 respectively, `failure` False; both `MultipleResultsFound` on `HEAD~1` | (b1), (b2) |
| `failure=True`, `failure=False` | unchanged | `True`: the newest failure in all three data sets. `False`: nothing for two failures, the success otherwise. Same on `HEAD~1` | (c1)–(c3) |
| `main.py`'s BBN lookup | `failure=None` | `"failure": None` in the BBN query payload | `grep -n '"failure": None' main.py` → `632` |
| `--retry-failed-bbn` | present, default False; with it a failure counts as missing | parses to `False` when absent and `True` when given | (g); (e) for the rule |
| the helper on the probe's bin | {V2, V4}; with V3 a stored failure, {V2, V4} / {V2, V3, V4} | {V2, V4}, 1 model skipped, and each entry's (potential, coupling) equals `pairs[model.i]`. With V3 failed: {V2, V4} and `stored_failures` 1 without the flag; {V2, V3, V4} with it | (d), (e) |
| results of the wrong length | raises | `ValueError`, one short at both steps and one long | (f) |
| the adiabatic stage | same helper | `build_query_entries` at `:357`, `select_missing(..., retry_failed=False)` at `:406–411` | read the diff |
| summary lines | one per stage | the formats above, at `:530–532` and `:787–794` | read the diff |
| `VERSION_LABEL`, `PRYM_VERSION` | unchanged | `config/version.py:33: VERSION_LABEL = "2026.4.0"` is the only definition outside `venv/`, `thirdparty/` and `claude-context/`; `ComputeTargets/BBNData.py:42: PRYM_VERSION = "bf24c3d+cham03+ri02"` | grep |

**Prompt §4 item 2** (I ran it):

```
$ grep -n "binned_batch\[key\]" main.py
180:                    for potential, coupling in binned_batch[key]
208:                "missing": [m for obj, m in zip(query_outcomes, binned_batch[key])],
330:                    for potential, coupling in binned_batch[key]
357:            key: build_query_entries(binned_batch[key], query_outcomes)
583:                    for potential, coupling in binned_batch[key]
610:            key: build_query_entries(binned_batch[key], query_outcomes)
```

- **`:357` and `:610`** are where the pairs enter the helper.
- **`:330` and `:583`** are where the same pairs enter the `ScalarModel` lookup whose results the
  helper receives. They are comprehensions, not zips, and unchanged.
- **`:180` and `:208`** are in `build_solver_batch`, step 1, which this prompt may not touch.
  `:208`'s zip pairs lists of equal length (see Observation 1).
- **No zip against `binned_batch[key]` remains** in `build_adiabatic_batch` (`:293–496`) or
  `build_bbn_data_batch` (`:546–754`).

**The helper is pure** (I ran this). After `import pipeline_selection`, `'ray' in sys.modules`,
`'main' in sys.modules` and `'sqlalchemy' in sys.modules` are all `False`. `py_compile main.py`
succeeds.

**`black --check`** is clean on all six changed or new Python files. `PRyM/` and `base.py` are not
touched.

**Reasoned, not run.** `main.py` cannot be imported in a test, because it parses arguments and
starts Ray at import. That `main.py` wires the helper correctly is read from the diff, not
executed. What was read:
- the payload is built from the same `entries` list that `select_missing` pairs against;
- the flag reaches `retry_failed`;
- a retried entry's work lookup (`failure=False`) returns an unavailable object, so it is
  computed.

An end-to-end check needs a pipeline run, which is the user's (CLAUDE.md).

**`planning-probes/datastore_version_probe.py` on this tree** (I ran it). Its line `[6]` prints
`available=False`, not a row and not `MultipleResultsFound`. That is prompt 01's keying: the
probe stores its two failed rows under `2026.3.0` and looks them up under `2026.4.0`. The probe
no longer witnesses §6.3 row 1; test (a) does.

## Observations not acted on

1. **Step 1's first-pass lookup filters nothing** (`main.py:205–210`, `build_solver_batch`).
   - **What.** `missing` is `[m for obj, m in zip(query_outcomes, binned_batch[key])]`, with no
     `if not obj.available`. So every pair is passed to the second pass, and the
     `num_missing == 0` early return fires only for an empty batch.
   - **Why the output is right anyway.** The second pass's `object_get` returns stored models as
     `available`, and `RayWorkPool` skips them.
   - **Impact.** One redundant vectorized lookup per batch; no wrong result. Reasoned from the
     code on `765e80d`; not run.
   - **Not acted on.** Step 1 of `run_pipeline` is outside this prompt's files. Opened as
     `[03-step-1-first-pass-lookup-filters-nothing]` (board §3).
2. **Retried failures accumulate.** Under `--retry-failed-bbn`, a computation that fails again
   stores another failed row. `failure=None` and `failure=True` return the newest, so this is by
   design (README §0.2), and nothing is deleted (§0.4). Not an issue.
3. **The datastore probe's line `[6]`** no longer exercises `failure=None` on two rows (see
   Verification). The probe is a record of `27a32bc` and is left as it is. Not an issue.

## State handed to the next prompt

- **Names.**
  - `pipeline_selection.build_query_entries(pairs, model_results) -> QueryEntries(entries,
    unavailable, skipped_failed_models)`.
  - `pipeline_selection.select_missing(entries, results, retry_failed=False) ->
    MissingSelection(missing, stored_failures)`.
  - `QueryEntry(potential, coupling, model)`.
  - Both functions raise `ValueError` on a length mismatch.
- **The flag.** `--retry-failed-bbn`, which becomes `args.retry_failed_bbn` (`store_true`,
  default `False`), in `config/argument_parser.py`.
- **`BBNData.build(failure=None)`** orders by `failure ASC, timestamp DESC, serial DESC` and takes
  `LIMIT 1`. `main.py`'s BBN query payload passes `"failure": None` (`main.py:632`).
- **The summary lines**, verbatim formats in "What shipped" R6, printed at `main.py:530–532` and
  `:787–794`.
- **The witnesses for §6.3.**
  - `Datastore/tests/test_bbn_failure_lookup.py`: (a), (b1) and (b2) fail on `765e80d`; (c1)–(c3)
    pass on both.
  - `ComputeTargets/tests/test_pipeline_selection.py`: (d)–(f) are an import error on `765e80d`,
    and (g) an `AttributeError`.
  - `planning-probes/pairing_probe.py` gives {V1, V3}, the old logic.
- **Suite counts after this prompt:** 18 / 41 / 17, all OK.
- **`datastore_version_probe.py` line `[6]`** prints `available=False` on this tree, because of
  prompt 01's keying. Do not read it as a §6.3 measurement.
- **`VERSION_LABEL`** is `"2026.4.0"` and **`PRYM_VERSION`** is `"bf24c3d+cham03+ri02"`, both
  unchanged.
