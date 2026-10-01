# Log 02 — A failure reason on `ScalarModel` rows

**Prompt:** prompts/science-readiness/02-scalarmodel-failure-reasons.md
**Commit:** the commit that adds this file ("Store why a ScalarModel history failed"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-01
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `3d5aa8d`. "HEAD~1" below means the parent of this prompt's commit,
which has `3d5aa8d`'s tree. HEAD~1 measurements were made on an export of `3d5aa8d`
(`git archive HEAD | tar -x`, with the new tests copied in), run with this repository's
`venv/bin/python`.

## What shipped

`VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION` (`"bf24c3d+ri02+sr01"`) are unchanged.

**Schema.** Column **added**: `ScalarModel.failure_reason`, `sqla.String(DEFAULT_STRING_LENGTH)`
(`String(256)`), `nullable=True`, no index, placed after `failure`. No column removed.

- `ComputeTargets/ScalarModel.py`
  - New `_failure_payload(reason: str) -> dict` (before `compute_scalar_model`), returning
    `{"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}`; the pattern of
    `BBNData._failure_payload`.
  - `compute_scalar_model`'s two failure exits: the `OverflowError` exit returns
    `_failure_payload(f"sampling: overflow when assembling sample values: {e}")`; the
    `ComputationFailureError` exit returns `_failure_payload(e.message)`. The prints are unchanged.
  - `ScalarModel._failure_reason`: `None` when unpopulated, `payload["failure_reason"]` when built
    from the datastore, set in `store()` from `data.get("failure_reason")` (truncated again,
    empty -> `None`) on a failure and `None` on a success.
  - New property `ScalarModel.failure_reason -> Optional[str]`: readable on a failure row, `None`
    on a success, `RuntimeError` on an unpopulated object (`_failure is None`), as
    `BBNData.failure_reason` does.
  - Import of `DEFAULT_STRING_LENGTH`.
- `Datastore/SQL/ObjectFactories/ScalarModel.py`: the column; `table.c.failure_reason` in `build`'s
  `select`; `"failure_reason": row_data.failure_reason` in the payload handed to `ScalarModel`;
  `"failure_reason": obj._failure_reason if obj._failure else None` in `store`'s row. The lookup
  filters are unchanged.
- `pipeline_selection.py`: new `NO_FAILURE_REASON = "no failure_reason stored"` and
  `summarise_failure_reasons(reasons) -> List[Tuple[str, int]]` (groups by the text before the first
  `:`, `None`/`""` under `NO_FAILURE_REASON`, most frequent first, ties alphabetical). See deviation 1.
- `main.py`: `validate_solver_batch` (the `ScalarModel` stage's per-model handler) appends
  `m.failure_reason` to `scalar_model_failure_reasons` when `m.failure`; after `solver_queue.run()`
  the stage prints `-- ScalarModel: N histories failed in this run` and one `     -- count x clause`
  line per group.
- `plot_by_beta.py`: new `report_dropped_scalar_models(model_label, potential, query_batch,
  model_results)` inside `run_pipeline`, called after the `ScalarModel` lookup queue. For each
  coupling whose `failure=False` lookup was not an available success, it looks up the `failure=True`
  row and prints `beta, M, Lambda` and the stored reason (`"failed, no failure_reason stored"` or
  `"no ScalarModel row in the store"` otherwise), with a count, in the style of
  `report_dropped_bbn_models`.
- Tests: `ComputeTargets/tests/test_scalarmodel_failure_reason.py` (4 tests) and
  `Datastore/tests/test_scalarmodel_failure_reason.py` (4 tests).

## Deviations from the prompt

### 1. The grouping function lives in `pipeline_selection.py`, not `main.py` — STRUCTURALLY REQUIRED

The prompt (§2 (c)) wants the grouping "factored as a pure function" and tested, and lists
`main.py` as the file to edit. `main.py` parses `sys.argv` at import (`args = parser.parse_args()`,
line 72), so a test cannot import a function from it. `pipeline_selection.py` is the repository's
existing home for pure functions that `main.py` calls and tests reach (its docstring says so;
README §2 (h) puts a pure check there too). The function went there, and `main.py` imports it.
`pipeline_selection.py` is outside the prompt's stated diff list; it is the only such file.

### 2. The `main.py` summary counts this run's failures, not stored ones — IMPLEMENTATION CHOICE

The `ScalarModel` stage looks up models and computes the missing ones; it never reads a stored
failure back (a stored failure row counts as present and is not recomputed). The reasons are
therefore collected from the models that finish in this run (`validate_solver_batch`). Alternatives:
a second lookup pass over the whole grid with `failure=True` after the stage (reports earlier runs'
failures too, at the cost of a vectorised lookup per shard); not done, because the prompt asks only
for "the stage's counts" and the pass would add a schema-wide query to a stage that has none. The
summary line says "in this run" so it cannot be mistaken for the store's totals.

### 3. `README` §2 (f) says the summary groups "as it does for `BBNData`"; the BBN summary does not group — IMPLEMENTATION CHOICE

`main.py`'s BBN summary prints two counts (`skipped_failed_models`, `stored_failures`) and no
reasons. The `ScalarModel` summary is new, in that style: one header line and one line per group.

### 4. Test (a) goes through `ScalarModel.store()` and the factory's `store` — IMPLEMENTATION CHOICE

The prompt asks for a failure row with a 300-character reason to be "stored and looked up". Inserting
a row dict directly (as `_insert_failed_scalar_model` does) would not test the truncation or the
factory's write. The test builds a `ScalarModel` on the existing stand-ins, runs `store()` with
`ray.wait`/`ray.get` patched to hand it the payload, calls the factory's `store` with the
datastore's own inserter (wrapped to give an explicit serial, since there is no serial broker), and
reads the row back through `object_get`. It also covers a failure payload with no reason (`None`),
a success row (`None`) and an unpopulated object (`RuntimeError`). The truncation is tested in
`store()` and in `_failure_payload`; the column itself is `String(256)`.

### 5. Test (b) patches `StepControl` — as the prompt allows

`compute_scalar_model` builds its own `StepControl(atol=…, rtol=…)`, so the test patches the name
in the `ComputeTargets.ScalarModel` module with `mock.patch.object` to add `step_budget=50`. Said
here because the prompt asks for it. (The module is reached through
`ComputeTargets.tests.test_kinematic_cap_loop.SM`, because `ComputeTargets.ScalarModel` as an
attribute is the class.)

## Verification performed

- **Suites, before (at `3d5aa8d`, run by me before editing):** CosmologyModels 18, ComputeTargets 71,
  Datastore 17, all OK. **After:** CosmologyModels 18, **ComputeTargets 75**, **Datastore 21**, all
  OK (three commands of README §5 rule 6; 87 s, 94 s, 1.8 s). ComputeTargets and Datastore rose by
  four methods each.
- **New tests fail on HEAD~1** (export of `3d5aa8d` with the two new test files copied in):
  Datastore: 4 tests run, 4 errors, `AttributeError: 'ScalarModel' object has no attribute
  'failure_reason'`. ComputeTargets: the module fails to import (`cannot import name
  'NO_FAILURE_REASON' from 'pipeline_selection'`); with that import removed, test (b) and the
  truncation test error (`KeyError: 'failure_reason'`: the payload is `{"failure": True}`;
  `module has no attribute '_failure_payload'`). So the reason the history printed is not in the
  payload or the row on HEAD~1.
- **README §6.3, row 2 (step budget 50 from `main.py`'s initial data, β = 2, M = 0.5):** on this
  tree `failure_reason` begins `"step budget exhausted"`; the printed message was `step budget
  exhausted: integrate_scalar_history (reason-test) took 51 accepted steps (budget 50) at
  N=4.325263426, T_J=269.15 GeV, with 0 reflection(s)`. The test runs in about 0.1 s.
- **Row 1 (round trip):** `failure_reason` reads back as the first 256 characters of the
  300-character reason, through the scalar and the vectorised lookup, on a second connection to the
  same SQLite file.
- **Row 3 (`main.py` summary; `plot_by_beta.py` drop report):** the grouping is tested on a list of
  reasons. `main.py` and `plot_by_beta.py` parse (`ast.parse`) and are `black`-clean, **but I did not
  run either**: they need Ray and a datastore, which this campaign does not use (README §0.5). The
  `main.py` hook and the `plot_by_beta.py` report are verified by reading only.
- `black` run on every changed file; only my hunks changed in them (checked by `git diff`).

## Observations not acted on

- `report_dropped_bbn_models` in `plot_by_beta.py` and the new `report_dropped_scalar_models` share
  most of their body (lookup of the failed rows, the sorted print). They could be one helper; left,
  as the prompt asks for no refactor.
- The `ScalarModel` stage does not report failures stored by earlier runs (deviation 2). If the
  science run wants that, it is a second lookup pass; no issue opened, since `plot_by_beta.py` now
  reports them per potential.

## State handed to the next prompt

- **`ScalarModel` factory columns, as they now stand** (`Datastore/SQL/ObjectFactories/ScalarModel.py`
  `register()["columns"]`, in order; plus the datastore's own `serial`, `version`, `timestamp`):
  `label String(256)`, `cosmology_type`, `cosmology_serial`, `potential_type`, `potential_serial`,
  `coupling_type`, `coupling_serial`, `atol_serial`, `rtol_serial`, `phi_Einstein_init_serial`,
  `pi_Einstein_init_serial`, `T_Jordan_init_serial`, `T_Jordan_stop_serial`, `failure Boolean`,
  **`failure_reason String(256) nullable`**, `solver_serial`, `z_samples`, `compute_time`,
  `compute_steps`, `RHS_evaluations`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`,
  `validated Boolean`, `extra_data String(256) nullable`. Prompt 03 adds its four `first_bounce_*`
  columns; the version label is unchanged.
- **Where the payload and the object take a column** (the pattern prompt 03 repeats):
  `compute_scalar_model`'s return dict -> `ScalarModel.store()` (copies to `self._…`) ->
  factory `store` row dict -> factory `build` (`select` list and the payload handed to
  `ScalarModel.__init__`, read as `payload["…"]`) -> property. A new `payload[...]` key read in
  `ScalarModel.__init__` must be added to `build`, which is the only caller that passes a payload.
- **Helpers:** `ComputeTargets.ScalarModel._failure_payload(reason)`;
  `pipeline_selection.summarise_failure_reasons(reasons)`, `NO_FAILURE_REASON`.
- **Test patterns:** `Datastore/tests/test_scalarmodel_failure_reason.py` shows a write through
  `ScalarModel.store()` (patching `ray.wait`/`ray.get` on the `ComputeTargets.ScalarModel` module)
  and the factory's `store` with an explicit-serial inserter, then a read-back by `object_get`.
  `ComputeTargets/tests/test_scalarmodel_failure_reason.py::_history` runs
  `compute_scalar_model._function` from `main.py`'s initial data (via `tools/history_and_bbn.py`'s
  constants and `_z_grid`).
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 75, Datastore 21.
