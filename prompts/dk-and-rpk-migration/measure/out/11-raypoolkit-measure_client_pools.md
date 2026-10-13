# `RayWorkPool` in ChamPBH (CPBH) at `ee79455`

- **Repository:** `/Users/ds283/Documents/Code/ChamPBH`, read through `git` only.
- **Commit:** `ee7945540bd6d8c29112edbfc42b1978d2d43d08`.
- **Verdicts by:** `raypoolkit 0.2.0`, its own constructor.
- **Files:** 563 tracked; 206 `.py` files outside `RayTools/` parsed; 0 did not parse; 4 binary, not searched; 0 datastores, not read.
- **`ray.is_initialized()` after the verdicts:** False.

## `RayTools/` at the commit

| File | Blob |
|---|---|

## Totals

| Measure | Count |
|---|---|
| Imports of a `RayTools` module | 0 (module level 0, nested 0) |
| … naming a private name | 0 |
| String literals naming `RayTools` | 0 |
| `import_module` / `__import__` calls naming `RayTools` | 0 |
| `RayWorkPool(...)` calls | 17 |
| … no compute handler (`compute_handler=None`) | 14 |
| … compute handler given | 3 |
| … compute handler left at its default | 0 |
| … `store_results=True` | 13 |
| … with a `title` keyword | 17 (`title=None` 12, set 5) |
| … positional arguments | 2 at 17 |
| … with `*args` / `**kwargs` | 0 / 0 |
| Verdicts | `constructs` 17 |
| Calls with a flag / flags | 0 / 0 |
| Lines of non-`.py` files naming `RayTools` or `RayWorkPool` | 252 |
| Test modules naming `RayTools` or `RayWorkPool` | 1 |

## The handler keywords, by spelling

Each handler keyword as absent (—), `None` or set; a row per spelling, with its calls.

| `compute_handler` | `store_handler` | `persist_handler` | `available_handler` | `validation_handler` | `post_handler` | `label_builder` | Calls |
|---|---|---|---|---|---|---|---|
| None | None | — | None | None | None | None | 1 |
| None | None | — | None | None | — | None | 13 |
| set | — | — | — | set | — | set | 3 |

## Imports of a `RayTools` module (0)

None.

## String literals naming `RayTools` (0)

None.

## `import_module` / `__import__` calls naming `RayTools` (0)

None.

## `RayWorkPool(...)` calls (17)

Each call at the line of its name. Handlers: absent (—), `None` or set. Sizes: the source text of each size keyword given. `results`: `store_results`. Verdict: `v0.2.0`'s constructor, given the classes.

| File:line | Scope | Pos. | `**` | compute | store | persist | available | validation | post | label | Sizes | `results` | Title | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `main.py:227` | `run_pipeline.build_solver_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:310` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `main.py:389` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:439` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:509` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:569` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `main.py:647` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:699` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:772` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:856` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `plot_ScalarModel.py:1560` | `run_pipeline` | 2 | — | None | None | — | None | None | None | None | `create_batch_size=10`, `process_batch_size=10` | False | set | `constructs` |
| `plot_by_beta.py:719` | `run_pipeline.report_dropped_bbn_models` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:807` | `run_pipeline.report_dropped_scalar_models` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:869` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:903` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:930` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:963` | `run_pipeline` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=10`, `max_task_queue=10` | True | set | `constructs` |

## Calls with a compute path: the handlers given (3)

The source text of each handler keyword given, at each call whose compute handler is given or left at its default.

| File:line | `compute_handler` | `store_handler` | `persist_handler` | `available_handler` | `validation_handler` | `post_handler` | `label_builder` |
|---|---|---|---|---|---|---|---|
| `main.py:310` | `compute_solver_batch` | — | — | — | `validate_solver_batch` | — | `build_solver_batch_label` |
| `main.py:569` | `compute_adiabatic_batch` | — | — | — | `validate_adiabatic_batch` | — | `build_adiabatic_batch_label` |
| `main.py:856` | `compute_bbn_data_batch` | — | — | — | `validate_bbn_data_batch` | — | `build_bbn_data_batch_label` |

## Flags (0, on 0 calls)

None.

## Notification keywords, by source text (5 calls; never passed)

| File:line | Keywords |
|---|---|
| `main.py:310` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `main.py:569` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `main.py:856` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `plot_ScalarModel.py:1560` | `notify_batch_size=50`, `notify_time_interval=120` |
| `plot_by_beta.py:963` | `notify_batch_size=10`, `notify_min_time_interval=120` |

## Non-`.py` files naming `RayTools` or `RayWorkPool` (252 lines)

| Top directory | Lines | Files |
|---|---|---|
| `.documents/` | 25 | 3 |
| `prompts/` | 227 | 32 |

Each line outside the campaign records (`prompts/`, `.prompts/`), 25:

```text
.documents/architecture-summary.md:35: ├── (the work queue, `RayWorkPool`, is the `raypoolkit` package, pinned in requirements.txt)
.documents/architecture-summary.md:152: ### 3.2 `RayWorkPool` (`raypoolkit`)
.documents/architecture-summary.md:154: `RayWorkPool` is the single generic orchestrator for all computation phases. It is instantiated with a list of work items and a set of handler callables, and its `run()` method drives a state machine until every item has been processed.
.documents/architecture-summary.md:244: The `available` property is the sentinel used by `RayWorkPool` to distinguish "already in database" from "needs computation".
.documents/architecture-summary.md:355: 2. For missing items only, call `object_get(...)` without `_do_not_populate`. This creates skeleton rows and returns objects ready for `RayWorkPool` to compute and store.
.documents/architecture-summary.md:585: The `RayWorkPool` handles steps 3–4 automatically via its three defaults: `compute_handler`, `store_handler` and `persist_handler`.
.documents/architecture-summary.md:648: 5. Store sampled `SampleValues` in memory; `RayWorkPool` will later call `store()` and then `pool.object_store()`.
.documents/architecture-summary.md:829: - `store_tag`: groups related computations; used by `RayWorkPool` to tag outputs (e.g. `SamplesPerLog10Z_250`)
.documents/architecture-summary.md:855: → RayWorkPool with compute_handler = ScalarModel.compute()
.documents/architecture-summary.md:856: → RayWorkPool with validation_handler = pool.object_validate()
.documents/architecture-summary.md:862: → RayWorkPool with compute_handler = AdiabaticHistory.compute()
.documents/architecture-summary.md:868: → RayWorkPool with compute_handler = BBNData.compute(payload={"small_network": True})
.documents/architecture-summary.md:920: query_queue = RayWorkPool(
.documents/architecture-summary.md:969: **`RayWorkPool` resilience:** The work pool does not bail out if individual tasks fail — it continues processing remaining items. The `_do_not_populate` two-pass pattern means failed objects are skipped in downstream stages (checked via `obj.failure`).
.documents/architecture-summary.md:999: - `RayWorkPool` (entire file)
.documents/architecture-summary.md:1056: # Copies computation result into self (called by RayWorkPool after compute)
.documents/architecture-summary.md:1086: work_pool = RayWorkPool(
.documents/architecture-summary.md:1108: 4. **`compute()` must return an `ObjectRef`.** `RayWorkPool` expects a Ray future from `compute_handler`. If the computation is CPU-bound and cannot be a Ray task (rare), wrap it in `ray.remote`.
.documents/architecture-summary.md:1110: 5. **`store()` is called on the driver side, not in Ray.** After `ray.get(compute_ref)` completes, `RayWorkPool` calls `obj.store()` on the Python object in the driver process. This should copy data from the compute result into the object's fields. Then `pool.object_store(obj)` is called to serialize to the database.
.documents/architecture-summary.md:1116: 8. **`store_results=True` in `RayWorkPool` is only for query-only passes.** When `task_builder` returns lists of refs (the vectorized case), the indices break. Use `store_results=True` only with scalar-returning `task_builder`s.
.documents/dk-and-rpk-migration-verification.md:12: - **§1 R0** is the start tree: ChamPBH's own layer and `RayTools/` (prompt 02).
.documents/dk-and-rpk-migration-verification.md:362: `raypoolkit` pin, `RayTools/` deleted and the three drivers importing `from raypoolkit import
.documents/dk-and-rpk-migration-verification.md:363: RayWorkPool`, and ChamPBH's data moved into `config/datastore.py` with the seven vectorized gets in
.documents/dk-and-rpk-migration-verification.md:620: `/tmp/ray-cpbh`, 6380 here), two pools (`RayTools` and `raypoolkit`) and two forms of the shard key
.documents/integrator-audit-2026-09-30/README.md:69: short). Only `ComputationFailureError` is caught (`:823`). `RayTools/RayWorkPool.py` calls
```

## Test modules naming `RayTools` or `RayWorkPool` (1)

```text
Datastore/tests/test_compare_stores.py:1161: # RayWorkPool and build.sh print; not copied from any run): a resumed run in which the first
```

## Files that do not parse (0)

None.
