# `RayWorkPool` in ChamPBH (CPBH) at `52142d7`

- **Repository:** `/Users/ds283/Documents/Code/ChamPBH`, read through `git` only.
- **Commit:** `52142d75aebe00855804e2daff4f63aee50ffc3e`.
- **Verdicts by:** `raypoolkit 0.2.0`, its own constructor.
- **Files:** 478 tracked; 203 `.py` files outside `RayTools/` parsed; 0 did not parse; 4 binary, not searched; 0 datastores, not read.
- **`ray.is_initialized()` after the verdicts:** False.

## `RayTools/` at the commit

| File | Blob |
|---|---|
| `RayTools/RayWorkPool.py` | `aaa6dc9218658e1389fe3eeee5d184dcd8dc0d26` |
| `RayTools/__init__.py` | `e69de29bb2d1d6434b8b29ae775ad8c2e48c5391` |

## Totals

| Measure | Count |
|---|---|
| Imports of a `RayTools` module | 3 (module level 3, nested 0) |
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
| Lines of non-`.py` files naming `RayTools` or `RayWorkPool` | 43 |
| Test modules naming `RayTools` or `RayWorkPool` | 0 |

## The handler keywords, by spelling

Each handler keyword as absent (—), `None` or set; a row per spelling, with its calls.

| `compute_handler` | `store_handler` | `persist_handler` | `available_handler` | `validation_handler` | `post_handler` | `label_builder` | Calls |
|---|---|---|---|---|---|---|---|
| None | None | — | None | None | None | None | 1 |
| None | None | — | None | None | — | None | 13 |
| set | — | — | — | set | — | set | 3 |

## Imports of a `RayTools` module (3)

| File:line | Scope | Statement | Private names |
|---|---|---|---|
| `main.py:50` | `(module)` | `from RayTools.RayWorkPool import RayWorkPool` | — |
| `plot_ScalarModel.py:44` | `(module)` | `from RayTools.RayWorkPool import RayWorkPool` | — |
| `plot_by_beta.py:38` | `(module)` | `from RayTools.RayWorkPool import RayWorkPool` | — |

## String literals naming `RayTools` (0)

None.

## `import_module` / `__import__` calls naming `RayTools` (0)

None.

## `RayWorkPool(...)` calls (17)

Each call at the line of its name. Handlers: absent (—), `None` or set. Sizes: the source text of each size keyword given. `results`: `store_results`. Verdict: `v0.2.0`'s constructor, given the classes.

| File:line | Scope | Pos. | `**` | compute | store | persist | available | validation | post | label | Sizes | `results` | Title | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `main.py:193` | `run_pipeline.build_solver_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:276` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `main.py:355` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:405` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:473` | `run_pipeline.build_adiabatic_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:533` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `main.py:611` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:663` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:736` | `run_pipeline.build_bbn_data_batch` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `main.py:820` | `run_pipeline` | 2 | — | set | — | — | — | set | — | set | `create_batch_size=2`, `process_batch_size=3`, `max_task_queue=24` | False | set | `constructs` |
| `plot_ScalarModel.py:1556` | `run_pipeline` | 2 | — | None | None | — | None | None | None | None | `create_batch_size=10`, `process_batch_size=10` | False | set | `constructs` |
| `plot_by_beta.py:715` | `run_pipeline.report_dropped_bbn_models` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:803` | `run_pipeline.report_dropped_scalar_models` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:865` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:899` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:926` | `run_pipeline.build_plot_work` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=20`, `process_batch_size=20` | True | None | `constructs` |
| `plot_by_beta.py:959` | `run_pipeline` | 2 | — | None | None | — | None | None | — | None | `create_batch_size=10`, `max_task_queue=10` | True | set | `constructs` |

## Calls with a compute path: the handlers given (3)

The source text of each handler keyword given, at each call whose compute handler is given or left at its default.

| File:line | `compute_handler` | `store_handler` | `persist_handler` | `available_handler` | `validation_handler` | `post_handler` | `label_builder` |
|---|---|---|---|---|---|---|---|
| `main.py:276` | `compute_solver_batch` | — | — | — | `validate_solver_batch` | — | `build_solver_batch_label` |
| `main.py:533` | `compute_adiabatic_batch` | — | — | — | `validate_adiabatic_batch` | — | `build_adiabatic_batch_label` |
| `main.py:820` | `compute_bbn_data_batch` | — | — | — | `validate_bbn_data_batch` | — | `build_bbn_data_batch_label` |

## Flags (0, on 0 calls)

None.

## Notification keywords, by source text (5 calls; never passed)

| File:line | Keywords |
|---|---|
| `main.py:276` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `main.py:533` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `main.py:820` | `notify_batch_size=2`, `notify_min_time_interval=MIN_NOTIFY_INTERVAL` |
| `plot_ScalarModel.py:1556` | `notify_batch_size=50`, `notify_time_interval=120` |
| `plot_by_beta.py:959` | `notify_batch_size=10`, `notify_min_time_interval=120` |

## Non-`.py` files naming `RayTools` or `RayWorkPool` (43 lines)

| Top directory | Lines | Files |
|---|---|---|
| `.documents/` | 22 | 2 |
| `prompts/` | 21 | 11 |

Each line outside the campaign records (`prompts/`, `.prompts/`), 22:

```text
.documents/architecture-summary.md:35: ├── RayTools/
.documents/architecture-summary.md:36: │   └── RayWorkPool.py               # Generic distributed work queue
.documents/architecture-summary.md:153: ### 3.2 `RayWorkPool` (`RayTools/RayWorkPool.py`)
.documents/architecture-summary.md:155: `RayWorkPool` is the single generic orchestrator for all computation phases. It is instantiated with a list of work items and a set of handler callables, and its `run()` method drives a state machine until every item has been processed.
.documents/architecture-summary.md:244: The `available` property is the sentinel used by `RayWorkPool` to distinguish "already in database" from "needs computation".
.documents/architecture-summary.md:355: 2. For missing items only, call `object_get(...)` without `_do_not_populate`. This creates skeleton rows and returns objects ready for `RayWorkPool` to compute and store.
.documents/architecture-summary.md:585: The `RayWorkPool` handles steps 3–4 automatically via its default `compute_handler` and `store_handler`.
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
.documents/integrator-audit-2026-09-30/README.md:69: short). Only `ComputationFailureError` is caught (`:823`). `RayTools/RayWorkPool.py` calls
```

## Test modules naming `RayTools` or `RayWorkPool` (0)

None.

## Files that do not parse (0)

None.
