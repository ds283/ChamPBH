# ChamPBH Architecture Summary

**Purpose of this document:** Describe the architecture of the ChamPBH codebase at sufficient depth that a new project — targeting the computation of stochastic instantons in a scalar field inflationary model, and subsequent derivation of compaction functions and PBH mass functions — can be designed to reuse its infrastructure components.

---

## 1. Overview

ChamPBH computes the cosmological evolution of a chameleon scalar field (a dark-energy model with a screening mechanism) through the radiation-dominated epoch, then predicts observational consequences for Big Bang Nucleosynthesis (BBN). It is designed for **parameter-space surveys**: the user specifies grids of potential parameters (M, Λ), coupling strengths (β), and integration tolerances, and the system evaluates every combination, caching results persistently.

The codebase has three major layers, each cleanly separable:

1. **Infrastructure** — Ray-based distributed task execution and sharded SQLite persistence.
2. **Physics integration** — ODE supervisor framework for driving `scipy.integrate.solve_ivp`.
3. **Compute targets** — Domain-specific classes (ScalarModel, AdiabaticHistory, BBNData) that live at the intersection of infrastructure and physics.

For a new instanton project, layers 1 and 2 can be adopted wholesale, while layer 3 is replaced with instanton-specific compute targets.

---

## 2. Directory Structure

```
ChamPBH/
├── main.py                          # Top-level pipeline driver
├── utilities.py                     # Timing, formatting, iteration helpers
├── constants.py                     # Physical constants
├── extract_common.py                # Post-hoc data extraction for analysis
├── plot_inputs.py                   # What the plot scripts read from a store before any history lookup (read-only)
├── plot_by_beta.py                  # Plotting driver (Ray remote)
├── plot_ScalarModel.py              # Scalar field detailed plots
├── exponential.yaml                 # Example run configuration
├── starobinsky.yaml
├── recliner.yaml
│
├── (the work queue, `RayWorkPool`, is the `raypoolkit` package, pinned in requirements.txt)
│
├── (the datastore layer is the `datastorekit` package, pinned in requirements.txt; see §4)
│
├── ObjectFactories/                 # SQLAlchemy serialization factories (ChamPBH's; the layer is `datastorekit`)
│   ├── __init__.py
│   ├── version.py
│   ├── store_tag.py
│   ├── redshift.py
│   ├── tolerance.py
│   ├── integration_metadata.py
│   ├── DimensionlessQuantity.py
│   ├── DimensionfulQuantity.py
│   ├── ExponentialPotential.py
│   ├── InversePowerPotential.py
│   ├── Starobinsky.py
│   ├── ReclinerPotential.py
│   ├── ReflectingPotential.py
│   ├── ExponentialCoupling.py
│   ├── QCD_Cosmology.py
│   ├── ScalarModel.py               # Factory for main ODE output
│   ├── AdiabaticHistory.py
│   ├── BBNData.py
│   └── tests/                       # unittest modules of the factories, the registry and the harness
│
├── StoreFixtures/
│   └── standin_pool.py              # the package's stand-in cluster, a copy; any test root imports it
│
├── ComputeTargets/
│   ├── ScalarModel.py               # ODE solver (Ray remote class)
│   ├── AdiabaticHistory.py          # Adiabatic-mode computation
│   ├── BBNData.py                   # BBN abundance computation
│   ├── spline_wrappers.py
│   ├── exceptions.py                # ComputationFailureError
│   └── Policies/
│       └── PotentialDerivativePolicy.py
│
├── Quadrature/
│   ├── integration_metadata.py      # IntegrationSolver, IntegrationData
│   ├── simple_quadrature.py
│   └── supervisors/
│       ├── base.py                  # IntegrationSupervisor, RHS_timer
│       └── ScalarField.py           # StateVector, ScalarFieldIntegrationSupervisor
│
├── CosmologyConcepts/
│   ├── DimensionlessQuantity.py
│   ├── DimensionfulQuantity.py
│   ├── FieldValues.py               # phi_value, pi_value
│   ├── M_value.py
│   ├── Lambda_value.py
│   ├── beta_value.py                # Also the database shard key
│   ├── redshift.py                  # redshift, redshift_array
│   ├── temperature.py
│   ├── Potentials/
│   │   ├── AbstractPotential.py
│   │   ├── ExponentialPotential.py
│   │   ├── InversePowerPotential.py
│   │   ├── StarobinskyPotential.py
│   │   ├── ReclinerPotential.py
│   │   ├── ReflectingPotential.py
│   │   └── model_ids.py
│   └── ConformalCouplings/
│       ├── AbstractCoupling.py
│       ├── ExponentialCoupling.py
│       └── model_ids.py
│
├── CosmologyModels/
│   ├── base.py                      # BaseCosmology abstract class
│   ├── LambdaCDM/
│   └── GenericEOS/
│       ├── QCD_Cosmology.py
│       └── ...
│
├── MetadataConcepts/
│   ├── version.py
│   ├── tolerance.py
│   └── store_tag.py
│
├── Units/
│   ├── base.py                      # UnitsLike abstract class
│   ├── Planck_units.py
│   ├── GeV_units.py
│   └── Mpc_units.py
│
├── PRyM/                            # External BBN solver (PRyMordial)
│   └── PRyM_main.py
│
└── config/
    ├── argument_parser.py           # configargparse CLI + YAML
    ├── datastore.py                 # The registry of factories, drop groups, batch sizes
    ├── defaults.py                  # Tolerance/precision constants
    ├── model_list.py                # Cosmological model registry
    └── sharding.py                  # Shard-key type and table lists
```

> **Note, 2026-10-02 (`science-readiness`, prompts 01, 04 and 07).** The tree above omits three
> files this campaign added or gave a role. `pipeline_selection.py` (pure functions that `main.py`
> and the tests share, because `main.py` parses `sys.argv` at import and cannot be imported):
> `build_query_entries`, `select_missing`, and since this campaign `summarise_failure_reasons`,
> `super_planckian_couplings` and `warn_super_planckian`. `tools/history_and_bbn.py BETA M
> [--T-stop-GeV T] [--phi-init-Mp P] [--small-network] [--wall-clock-limit SECS]` runs one full
> history and its BBN solve through the undecorated `._function`s, with no datastore, and prints the history, first-bounce, fixed-T, ratio-window and abundance lines; `tools/bbn_baseline.py`
> is the Standard-Model baseline. `extract_common.py` is no longer only "post-hoc extraction": it
> holds the pure pieces of `plot_by_beta.py`'s science figures (`relative_shift`, `running_band`,
> `kick_threshold_curve`, `build_history_record`, the four figure functions, `write_histories_csv`,
> `CSV_COLUMNS`), so that a test can import them. `plot_by_beta.py` itself still parses `sys.argv`
> and calls `ray.init` at import and is checked by `ast`, not run, in the tests.

> **Note, 2026-10-14 (`read-only-plots`, prompts 01 to 03).** The tree above gains `plot_inputs.py`.
> It holds what `plot_by_beta.py` and `plot_ScalarModel.py` each used to carry: the lookups they make
> before any history, as functions of a pool, so that a test can drive them on a stand-in store.
> They are `parameters` (the two tolerances, `T_init`, `phi_init` and `pi_init`), `stored_grid` (the
> stored β, M and Λ, by `read_table`), `potentials`, `couplings`, `models` and `T_stop`, with the
> exception `PlotOptionNotStored`. It parses nothing and starts no Ray at import. Both scripts now
> open their pool with `ShardedPool(read_only=True)`, so a plot run changes no file of the store it
> reads (verified under real Ray, `.documents/read-only-plots-verification.md` §1). A read-only pool
> refuses a lookup that would insert, and what a refusal means depends on the lookup:
> - **a parameter** (`--abs-tol`, `--rel-tol`, `--T-init-GeV`, `--phi-init-Mp`, the constant
>   π = 0, and `--T-stop-GeV`) the build did not store stops the run: the script prints
>   `!! plot: the store holds no …`, naming the option and its value, to stderr and exits 2, before
>   the baseline and any figure (the plot run must be given the options the build used);
> - **a potential or a coupling** never stored is skipped, with one warning line each and a count;
>   every other combination is plotted, in the order the scripts used before.
>
> The history lookups cannot be refused and are unchanged: a missing history is an unavailable
> object, as before. Both scripts also refuse `--drop` (exit 2, before Ray starts), since a plot run
> drops nothing; `--prune-unvalidated` is `main.py`'s and is ignored by them. A read-only pool starts
> no `SerialPoolBroker`. Since `read-only-plots` prompt 01 every driver that starts Ray also needs
> `--ray-address` (§5.1).

---

## 3. The Ray Distributed Computing Layer

### 3.1 Design Philosophy

All expensive computations are dispatched as Ray remote tasks or actors. The driver (main.py) never blocks waiting for a single result; it maintains a bounded queue of in-flight Ray `ObjectRef`s and processes completions as they arrive. This allows the driver to pipeline database lookups, compute tasks, and store tasks concurrently.

### 3.2 `RayWorkPool` (`raypoolkit`)

`RayWorkPool` is the single generic orchestrator for all computation phases. It is instantiated with a list of work items and a set of handler callables, and its `run()` method drives a state machine until every item has been processed.

**Constructor parameters:**

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `pool` | `raypoolkit.DatastorePool` (a `typing.Protocol` with one method, `object_store(obj)`; a type hint, not checked at run time) | required | Database access |
| `task_list` | iterable | required | Items to process |
| `task_builder` | `Callable(item) → ObjectRef \| list[ObjectRef] \| None` | required | Converts each work item to a Ray DB lookup |
| `compute_handler` | `Callable(obj, label?, payload?) → ObjectRef` | `obj.compute()` | Dispatches ODE/computation |
| `store_handler` | `Callable(obj, pool) → None` | `obj.store()` | Local store step, run on the driver when the compute task has completed; its return is ignored |
| `persist_handler` | `Callable(obj, pool) → ObjectRef` | `pool.object_store(obj)` | Datastore round-trip, run straight after the store handler; the ref's value is the persisted object |
| `available_handler` | `Callable(obj) → ObjectRef` | None | Optional: handle already-computed objects |
| `validation_handler` | `Callable(obj) → ObjectRef` | None | Optional: post-store integrity check |
| `post_handler` | `Callable(obj) → obj?` | None | Optional: called at task exit |
| `label_builder` | `Callable(obj) → str` | None | Generates a log label for compute tasks |
| `create_batch_size` | int | 5 | Items to enqueue per loop iteration |
| `process_batch_size` | int | 1 | Completed refs to dequeue per iteration |
| `max_task_queue` | int | 200 | Cap on in-flight tasks |
| `notify_batch_size` | int | 500 | Progress report frequency (by count) |
| `notify_time_interval` | int | 300 s | Progress report frequency (by time) |
| `title` | str | None | Displayed in progress output; None = silent |
| `store_results` | bool | False | Accumulate results in `self.results` list |

**Task lifecycle (state machine):**

Each work item passes through states tracked by a tag, keyed on each ref's value:

```
task_builder(item)
    → ObjectRef tagged "lookup"
    
"lookup" completes:
    obj.available == True  →  "available" (if available_handler exists)
                           →  post_handler exit (otherwise)
    obj.available == False →  compute_handler(obj) → ObjectRef tagged "compute"

"available" completes:
    → post_handler exit

"compute" completes:
    store_handler(obj, pool)  (default obj.store(); mutates obj in-place)
    persist_handler(obj, pool) → ObjectRef tagged "store"

"store" completes:
    replacement_obj = ray.get(ref)  (has store_id set)
    validation_handler(replacement_obj) → ObjectRef tagged "validate"  (if present)
    → post_handler exit  (otherwise)

"validate" completes:
    → post_handler exit
```

**Key implementation detail:** When `task_builder` returns a list of `ObjectRef`s (e.g., from a vectorized batch lookup), all refs are enqueued as independent "lookup" tasks. The `store_results=True` mode is incompatible with multi-ref returns — it is only used for pure query passes.

**Progress reporting:** When `title` is set, the pool prints periodic updates showing counts and rates (per-second and average) for each queue stage. Reporting triggers on both elapsed time (`notify_time_interval`) and completion count (`notify_batch_size`), subject to a minimum interval (`notify_min_time_interval`).

---

## 4. The Database Layer

> **Note, 2026-10-13 (`dk-and-rpk-migration`, prompt 11).** This section was rewritten for the
> package. Until `857ec73` (that campaign's prompt 08) the layer was ChamPBH's own: `Datastore/object.py`
> and, under `Datastore/SQL/`, `Datastore.py`, `ShardedPool.py`, `SerialPoolBroker.py`, `ClientPool.py`
> and `ProfileAgent.py`, with the factories' base class in `ObjectFactories/base.py` and the
> registry as `_factories` in `Datastore.py`. All seven are in git before `857ec73`. The layer is
> now the `datastorekit` package, and this section says what ChamPBH gives it. What the package
> does is stated in its client contract, which this section points at and does not copy.

### 4.1 The package, and where ChamPBH meets it

The layer is [`datastorekit`](https://github.com/ds283/DatastoreKit), pinned to the tag `v0.3.0` in
`requirements.txt` (never an editable install). Its specification for a client is
`docs/client-contract.md` at that tag
([§1](https://github.com/ds283/DatastoreKit/blob/v0.3.0/docs/client-contract.md) the pool's
constructor, §2 the keys of `register()`, §3 the factory hooks, §4 the inventory's declarations,
§5 the layer's own tables, §6 what the layer reads from a stored object, §7 the other entry
points, §8 version-keyed lookups, §9 the changes after `v0.2.0`). Where this section and the
contract differ, the contract is right.

The database is a **sharded SQLite** store managed as a pool of Ray actors. Each actor owns one
SQLite file, and a separate primary SQLite file records the shard topology. The shards are
siblings of the primary and the primary records them by their bare file names, so a store stays
readable when its directory is moved or copied.

```
primary.db           ← shard topology, key→shard mapping (the package's own tables, contract §5)
primary-shard0000.db ← Datastore actor #0
primary-shard0001.db ← Datastore actor #1
...
primary-shardNNNN.db ← Datastore actor #N
```

All actors run in the same Ray cluster but are independent processes, so SQLite's single-writer
constraint is respected per shard. What the package provides, by module:

| What | Where |
|------|-------|
| The pool, run in the driver: `ShardedPool` (`object_get`, `object_get_vectorized`, `object_read_batch`, `object_store`, `object_validate`, `read_table`) | `datastorekit.SQL.ShardedPool` |
| The shard actor (`Datastore`, one per shard, named `shardNNNN-store`), the serial broker (`SerialPoolBroker`, named so) and the optional profiling actor (`ProfileAgent`) | `datastorekit.SQL.Datastore`, `.SerialPoolBroker`, `.ProfileAgent` (and `.ClientPool`, the actor's serial leases) |
| `DatastoreObject` (`store_id`, `available`), the base of every stored class; `available` is the sentinel `RayWorkPool` uses to tell "already in the database" from "needs computation" | `datastorekit.object`, also `from datastorekit import DatastoreObject` |
| `SQLAFactoryBase`, the factories' base class; the schema builder `build_schema`, `dependent_tables`, `drop_order` | `datastorekit.SQL.factory_base`, `datastorekit.SQL.schema` |
| The read-only reader and the inventory (`open_read_only`, `read_inventory`, `InventorySpec`) | `datastorekit.store_reader`, `datastorekit.store_inventory` |
| Whole-store operations: `ShardedPool.copy_store`, `move_store`, `closed_store_files`, `delete_store`, and the two tools `python -m datastorekit.tools.sharded_store` and `python -m datastorekit.tools.shard_key_audit` | `datastorekit.SQL.ShardedPool`, `datastorekit.tools` |
| The names the layer owns: the version and tag tables' names, `VERSION_SERIAL_KEY`, `require_version_serial` | `datastorekit.contract` |

### 4.2 What ChamPBH supplies

| What | Where in ChamPBH | How it reaches the package |
|------|------------------|----------------------------|
| **The registry**: every storable class's table name mapped to its factory *instance* (27 entries, in the order the schema is built; the table below) | `config/datastore.py`, `factories` | `ShardedPool(factories=…)`, and `read_inventory(path, factories)` |
| **The drop groups** behind `--drop`, each mapped to the tables it drops, and `tables_to_drop(groups)`, which refuses a drop that would leave rows naming rows it drops, naming the groups to add (U4) | `config/datastore.py`, `drop_groups`, `tables_to_drop` | `ShardedPool(drop_tables=…)`; `main.py` calls `tables_to_drop` before Ray starts |
| **The batch sizes**: how many serials an actor leases from the broker at a time, per table | `config/datastore.py`, `serial_batch_sizes` | `ShardedPool(serial_batch_sizes=…)` |
| **The tables and the shard key**: `replicated_tables`, `sharded_tables`, `read_table_config`, `ShardKeyType` (`beta_value`) and `get_shard_key_store_id` (§5.2) | `config/sharding.py` | `ShardedPool(replicated_tables=…, sharded_tables=…, read_table_config=…, ShardKeyType=…, ShardKeyStoreIdGetter=…)` |
| **The version label** | `config/version.py`, `VERSION_LABEL` | `ShardedPool(version_label=…)` |
| **The factories' own code**: for each class, `register` and `build`, and, where the class has one, `store`, `validate`, `validate_on_startup` and `inventory_spec` | `ObjectFactories/` | read from the registry's instances by the package's actors, pool and reader |

The three drivers that open a pool (`main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`) each call
`ShardedPool(…)` in a `with` block with these arguments; the constructor's sixteen parameters, and
what the package does with each, are contract §1. `main.py`'s pool writes; the two plot scripts'
pools pass `read_only=True` (and no `shards=` and no `drop_tables=`), so they read the finished
store and change nothing in it (`plot_inputs.py`). `main.py --inventory` does not open a pool: it
calls `read_inventory` on the closed store, read-only and before Ray starts, and prints it with
`tools/inventory_report.py`.

**The registry, as `config/datastore.py` holds it** (the table type is `config/sharding.py`'s).

| Key | Factory | Table type |
|-----|---------|------------|
| `version` | `sqla_version_factory` | Replicated |
| `store_tag` | `sqla_store_tag_factory` | Replicated |
| `redshift` | `sqla_redshift_factory` | Replicated |
| `tolerance` | `sqla_tolerance_factory` | Replicated |
| `beta_value` | `sqla_dimensionless_quantity_factory` | Replicated |
| `M_value` | `sqla_dimensionful_quantity_factory` | Replicated |
| `Lambda_value` | `sqla_dimensionful_quantity_factory` | Replicated |
| `temperature` | `sqla_dimensionful_quantity_factory` | Replicated |
| `phi_value` | `sqla_dimensionful_quantity_factory` | Replicated |
| `pi_value` | `sqla_dimensionful_quantity_factory` | Replicated |
| `InversePowerPotential` | `sqla_InversePowerPotential_factory` | Replicated |
| `StarobinskyPotential` | `sqla_StarobinskyPotential_factory` | Replicated |
| `ExponentialPotential` | `sqla_ExponentialPotential_factory` | Replicated |
| `ReclinerPotential` | `sqla_ReclinerPotential_factory` | Replicated |
| `ReflectingPotential` | `sqla_ReflectingPotential_factory` | Replicated |
| `ExponentialCoupling` | `sqla_ExponentialCoupling_factory` | Sharded |
| `QCD_Cosmology` | `sqla_QCDCosmology_factory` | Replicated |
| `IntegrationSolver` | `sqla_IntegrationSolver_factory` | Replicated |
| `ScalarModel` | `sqla_ScalarModelFactory` | Sharded |
| `ScalarModel_tags` | `sqla_ScalarModelTagAssociation_factory` | with its owner (not listed in `config/sharding.py`) |
| `ScalarModelValue` | `sqla_ScalarModelValue_factory` | Sharded |
| `AdiabaticHistory` | `sqla_AdiabaticHistoryFactory` | Sharded |
| `AdiabaticHistory_tags` | `sqla_AdiabaticHistoryTagAssociation_factory` | with its owner (not listed) |
| `AdiabaticHistoryValue` | `sqla_AdiabaticHistoryValue_factory` | Sharded |
| `BBNData` | `sqla_BBNDataFactory` | Sharded |
| `BBNData_tags` | `sqla_BBNDataTagAssociation_factory` | with its owner (not listed) |
| `BBNDataValue` | `sqla_BBNDataValue_factory` | Sharded |

`LambdaCDM`, which `config/sharding.py`'s `replicated_tables` and the batch sizes listed though no
factory registered it, is in neither since `dk-and-rpk-migration` P8 (prompt 08).

### 4.3 What the package does for a get, a store and a validate

The pool's `object_get` and `object_store` dispatch by the class's name; the replicated/sharded
rule, the replication of a replicated write, the check at open, the pruning of unvalidated rows
and the refusals are the package's, and are stated in contract §1 (the arguments that set them),
§3 (the hooks it calls) and §6 (what it reads from an object). The points a ChamPBH reader needs:

- **Replicated tables** (parameter types, potentials, cosmology, metadata) are identical on every
  shard, with the same `store_id`. A replicated write goes to a controlling shard and is then
  replicated to the others under the controller's serials. A replicated row carries **one
  timestamp**, the same on every shard.
- **Sharded tables** (compute results: `ScalarModel`, `AdiabaticHistory`, `BBNData` and their
  value tables, and `ExponentialCoupling`) live only on the shard of their shard key. The shard
  is assigned, load-balanced, when a `beta_value` is first inserted; a lookup table in the
  primary maps `beta_value.store_id` to the shard.
- **Serials** are leased from the one `SerialPoolBroker`, so serial numbers never collide between
  shards, which replicated tables need, since their `store_id`s must agree on every shard.
- **`object_get(class, **kwargs)`** finds or creates an object through the factory's `build`;
  **`object_store(obj)`** writes a computed object through its factory's `store` and returns it
  with its `store_id`; **`object_validate(obj)`** runs its `validate`. `store`, `validate` and
  `validate_on_startup` are defaulted since `v0.2.3` (U3): a factory that does not define one
  raises `NotImplementedError` if the layer calls it. ChamPBH's `ScalarModel`, `AdiabaticHistory`
  and `BBNData` factories define all three, and the others none.
- **`object_get_vectorized(class, shard_key, payload_data)`** is for sharded tables only. It takes the
  shard key **in mapping form**, `{"shard_key": key}` (the sharded table's field name mapped to the
  key), and sends one shard actor the list of payloads in one call.
- **At open**, the pool checks that every shard's replicated tables agree, repairs or refuses what
  an interrupted run left, and, under `prune_unvalidated`, deletes the unvalidated rows of the
  classes whose factory validates at startup (contract §1, rows 5 and 11; §2, `validate_on_startup`).

**The `_do_not_populate` payload key.** It is ChamPBH's convention, not the package's: the
`ScalarModel` and `BBNData` factories' `build` read it from the payload, and when it is set they do
not INSERT a skeleton row. This allows a fast "does this exist?" query without side effects, and
is used in the two-pass pattern: first check which items exist, then enqueue only the missing ones
for computation.

**Version-keyed lookups (added 2026-09-30, `run-integrity` prompt 01; since `dk-and-rpk-migration`
prompt 08 the keying is the package's, contract §8).** Every table registered with
`"version": True` records, in its `version` column, the serial of the label the store was opened
under (stamped by the actor's insert). The lookups of the three compute targets read it:

- **Which tables are keyed.** `ScalarModel`, `AdiabaticHistory` and `BBNData`, and only those. Each factory's `register()` declares
  `"key_on_version": True`; the package's schema builder refuses that flag on a table without
  `"version": True` (`ValueError`, contract §8). Their value and tag tables hang off the parent row's
  serial and carry no key of their own.
- **How the serial reaches `build()`.** The actor's `object_get`, the one route to every `build()`
  (scalar and `payload_data` alike, so also `ShardedPool.object_get_vectorized`), hands a keyed
  factory a *copy* of each payload with the lookup serial added under the reserved key
  `VERSION_SERIAL_KEY` (`"_version_serial"`, now `datastorekit.contract.VERSION_SERIAL_KEY`). The
  caller's dict is not mutated, and a caller that supplies the key itself is refused (`KeyError`).
  The factory's `build()` adds `table.c.version == <serial>` to its query, and raises
  (`datastorekit.contract.require_version_serial`, imported by the three factories) if the key is
  absent: a keyed lookup never falls back to an unfiltered one.
- **Parameter tables are not keyed.** Couplings, potentials, cosmologies and the value tables (β,
  M, Λ, T, φ, π, z, tolerances) keep one row per parameter across labels, even where they carry a
  `version` column.
- Only `object_get` is keyed (contract §8, "What is not keyed"). It is tested by
  `ObjectFactories/tests/test_version_keyed_lookups.py`.

A store opened under a new label therefore sees none of its old compute targets; they are
recomputed and stored beside the old rows. `--inventory` still lists every version. The label is
defined once, as `VERSION_LABEL` in `config/version.py`, and `main.py`, `plot_by_beta.py` and
`plot_ScalarModel.py` import it.

**The two-pass query pattern** used in `main.py` for all compute stages:

1. Call `object_get_vectorized(..., {"shard_key": key}, payload_data=[…])` with each payload
   carrying `_do_not_populate=True`, across all items in a batch. This returns skeleton objects;
   check `obj.available` to identify which are already computed.
2. For missing items only, call `object_get(...)` without `_do_not_populate`. This creates skeleton
   rows and returns objects ready for `RayWorkPool` to compute and store.

### 4.4 A factory, and how to add a class

A factory is a class the package's actors call for each fact of a stored class. ChamPBH's factories
subclass `datastorekit.SQL.factory_base.SQLAFactoryBase` and the registry holds an *instance* of each.
The hooks and the keys of `register()` are contract §3 and §2; in outline:

```python
class sqla_ExampleFactory(SQLAFactoryBase):
    def register(self) -> dict:
        # the table: "columns", and the keys the schema builder reads ("version", "timestamp",
        # "stepping", "serial", "validate_on_startup", "key_on_version", …); contract §2
        ...

    def build(self, payload, conn, table, inserter, tables, inserters):
        # find the object by a SELECT, or insert it through `inserter`; return the domain object.
        # Every get comes through here, scalar and vectorized.
        ...

    # defaulted since v0.2.3 (U3); define only where the class needs them:
    def store(self, obj, conn, table, inserter, tables, inserters): ...      # serialize computed data
    def validate(self, obj, conn, table, tables) -> bool: ...                # read back and check
    def validate_on_startup(self, conn, table, tables, prune=False): ...    # report / prune unvalidated rows
    def inventory_spec(self): ...                                           # an InventorySpec (contract §4)
```

To add a new persistent class to a project built on this one: (a) write the factory and, if the class
is a compute target, its three hooks; (b) add an instance to `factories` in `config/datastore.py`
(and to a group of `drop_groups`, and to `serial_batch_sizes`, where it should have one); (c) add
its table name to `replicated_tables` or `sharded_tables` in `config/sharding.py`; (d) declare an
`inventory_spec()`, or name the class as read through an owner's spec, or excluded with a reason, in
`ObjectFactories/tests/test_inventory_specs.py`, which fails on a registered class in none of the three.

---

## 5. Configuration System

### 5.1 Argument Parser (`config/argument_parser.py`)

Uses `configargparse` (a drop-in `argparse` replacement) so that every CLI argument can alternatively be set in a YAML config file. The config file is specified with `--config path/to/config.yaml`.

**Key argument groups:**

*Database:*
- `--database PATH` (required)
- `--shards N` (default 20)
- `--db-timeout SECS` (default 60)
- `--profile-db PATH`
- `--job-name LABEL`
- `--prune-unvalidated` (default True; `main.py`'s, ignored by the plot scripts)
- `--drop {scalar-model,adiabatic-history,bbn-data}` (`main.py`'s: the plot scripts refuse it, exit 2, before Ray starts)

*Integration:*
- `--abs-tol FLOAT` (default 1e-8)
- `--rel-tol FLOAT` (default 1e-8)
- `--T-init-GeV FLOAT` (default 20000)
- `--T-stop-GeV FLOAT`

*Parameter grids:*
- `--potential-type {Exponential,InversePower,Starobinsky,Recliner}`
- `--beta-low/high FLOAT`, `--samples-per-beta N`, `--beta-values FLOAT...`
- `--log10-M-low/high-eV FLOAT`, `--samples-per-log10-M-eV N`, `--M-values-eV/Mp FLOAT...`
- `--log10-Lambda-low/high-eV FLOAT`, `--Lambda-values-eV/Mp FLOAT...`
- `--samples-log10-z N` (default 250)
- `--log10-one-plus-z-high/low FLOAT`

*Output:*
- `--output DIR`
- `--inventory` (show DB contents and exit)
- `--ray-address STR` (no default; required whenever Ray starts, `auto` is refused, `main.py --inventory` needs none; `ray_address_problem()` in `config/argument_parser.py` is called before each driver's `ray.init`)

> **Note, 2026-10-02 (`science-readiness`).** Three options were added to the shared parser (so
> `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` all accept them):
>
> - `--phi-init-Mp FLOAT` (default 5.0; prompt 04): the initial field value φ\* in units of the
>   reduced Planck mass M_P, with π\* = 0. It replaces the literal `5.0 * units.PlanckMass` that
>   was in each driver. `phi_Einstein_init` was already part of the `ScalarModel` lookup key, so a
>   run and its plots must use the same value. A coupling with Ω(φ\*) T\* > M_P is warned about
>   by `main.py` before step 1 and computed; none is skipped or refused.
> - `--bbn-wall-clock-limit SECS` (default 600, `DEFAULT_BBN_WALL_CLOCK_LIMIT`; 0 disables it;
>   negative is refused; prompt 01): the wall-clock limit on each PRyMordial solve. A solve past it
>   is a stored BBN failure like any other (final within a label, retried by `--retry-failed-bbn`).
> - `--band-half-width FLOAT` (default 0.025; prompt 07): `plot_by_beta.py` only. The half-width
>   in β of the window `[β − h, β + h]` for the running median and the 16th–84th percentile band
>   of the abundance shifts.
>
> `--T-stop-GeV` now interacts with the BBN window: a history must reach 20 eV to pass
> `compute_BBN_data`'s pre-check (`--T-stop-GeV 1e-8` passes, `1e-7` fails; the default, `T_CMB`,
> passes). The same prompt-01 and prompt-06 statements are in
> [`numerical-strategies.md` §7.6](numerical-strategies.md).

### 5.2 Sharding Configuration (`config/sharding.py`)

This file defines what the `ShardedPool` constructor needs of the tables and the shard key (the registry, the drop groups and the batch sizes are in `config/datastore.py`, §4.2):

```python
ShardKeyType = beta_value   # The type used as the shard key

replicated_tables = [
    "version", "store_tag", "redshift", "tolerance",
    "beta_value", "M_value", "Lambda_value", "temperature",
    "phi_value", "pi_value",
    "ExponentialPotential", "InversePowerPotential", "ReflectingPotential",
    "ReclinerPotential", "StarobinskyPotential",
    "QCD_Cosmology",
    "IntegrationSolver"
]

sharded_tables = {
    "ExponentialCoupling": "shard_key",
    "ScalarModel": "shard_key",
    "ScalarModelValue": "shard_key",
    "AdiabaticHistory": "shard_key",
    "AdiabaticHistoryValue": "shard_key",
    "BBNData": "shard_key",
    "BBNDataValue": "shard_key",
}
```

The file also holds `read_table_config` (the replicated classes whose `read_table` the pool serves,
each with its `tables_arg`: `redshift`, `beta_value`, `M_value`, `Lambda_value`, `phi_value`,
`pi_value`) and `get_shard_key_store_id`, which is the pool's `ShardKeyStoreIdGetter`. Its companion
is `config/datastore.py` (added by `dk-and-rpk-migration` prompt 05): the registry `factories`, the
`drop_groups`, `serial_batch_sizes` and `tables_to_drop` (§4.2). A new class is named in both.

For a new project with a different shard key, this file is the primary place to make changes.

### 5.3 Defaults (`config/defaults.py`)

```python
DEFAULT_STRING_LENGTH = 256
DEFAULT_FLOAT_PRECISION = 1e-7
DEFAULT_REDSHIFT_PRECISION = 1e-7
DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION = 1e-7
DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION = 1e-5
DEFAULT_DIMENSIONFUL_QUANTITY_PRECISION = 1e-7
DEFAULT_DIMENSIONFUL_QUANTITY_RELATIVE_PRECISION = 1e-5
DEFAULT_ABS_TOLERANCE = 1e-8
DEFAULT_REL_TOLERANCE = 1e-8
```

### 5.4 Model List (`config/model_list.py`)

A registry of cosmological background models. Currently returns a single entry for `QCD_Cosmology`. The pipeline loops over all models in this list.

---

## 6. Physics / ODE Integration Framework

### 6.1 `IntegrationSolver` and `IntegrationData` (`Quadrature/integration_metadata.py`)

`IntegrationSolver` is a `DatastoreObject` identifying a solver method by label and stepping version:

```python
class IntegrationSolver(DatastoreObject):
    label: str      # e.g. "solve_ivp+Radau"
    stepping: int   # schema/stepping version (0 = current)
```

> **Note, 2026-10-01 (`integrator-remediation`, prompt 01, `fc97233`).** `ScalarModel` no longer returns `"solve_ivp+Radau"` or any of the four other `solve_ivp+…` labels: the integrator is not `solve_ivp`. It returns one label, `"Radau+kinematic-cap"` with `stepping = 0` (`SCALAR_MODEL_STEPPER_LABEL = "Radau+kinematic-cap-stepping0"`), registered in `main.py`. The five older `IntegrationSolver` registrations stay so that a stored history under an old label still loads; nothing returns them. Every `ScalarModel` store made before `VERSION_LABEL = "2026.5.0"` is invalid.

`IntegrationData` is a `namedtuple` holding solver performance statistics returned alongside the ODE solution:

```python
IntegrationData = namedtuple("IntegrationData", [
    "compute_time",      # wall-clock seconds
    "compute_steps",     # number of accepted steps
    "mean_RHS_time",     # mean time per RHS evaluation
    "max_RHS_time",
    "min_RHS_time",
    "RHS_evaluations",   # total calls to the ODE RHS function
])
```

### 6.2 Integration Supervisors (`Quadrature/supervisors/`)

**`IntegrationSupervisor` (`base.py`):** Base class wrapping an ODE integration run. Acts as a context manager; tracks RHS timing via the nested `RHS_timer` context manager.

```python
class IntegrationSupervisor:
    def __enter__(self): ...        # start timing
    def __exit__(self, ...): ...    # finalize timing
    
    def notify_new_RHS_time(self, rhs_time: float): ...   # record one RHS call time
    
    @property mean_RHS_time(self) -> float: ...
    @property min_RHS_time(self) -> float: ...
    @property max_RHS_time(self) -> float: ...
    @property RHS_evaluations(self) -> int: ...
    
    # progress reporting helpers
    def notify_available(self) -> bool: ...
    def report_notify(self): ...
    def reset_notify_time(self): ...

class RHS_timer:
    """Context manager: time one RHS evaluation and notify the supervisor."""
    def __init__(self, supervisor: IntegrationSupervisor): ...
```

**`ScalarFieldIntegrationSupervisor` (`ScalarField.py`):** Extends the base supervisor with chameleon-specific event tracking (bounce region entry/exit, hard reflections, solution fragment transitions). For a new project, a project-specific subclass would track the relevant events (e.g., turning points in the instanton trajectory).

> **Note, 2026-10-01 (`integrator-remediation`, prompt 01, `fc97233`).** The bounce-region entry/exit, hard-reflection and fragment-transition notifications are gone from `ScalarFieldIntegrationSupervisor`, along with their counters and the `max_step_size` constructor argument. It now has `notify_reflection(N)` (elastic reflections, `number_reflections`) and `notify_step_cap(cap)` (the current cap, shown in the status line). See [`numerical-strategies.md` §3.5.2–§3.5.3](numerical-strategies.md).

**`StateVector` (`ScalarField.py`):** The ODE state namedtuple. For scalar field evolution:

```python
StateVector = namedtuple("StateVector", [
    "phi_Einstein",          # scalar field value
    "pi_Einstein",           # canonical momentum ∂φ/∂N
    "log_rhorad_Einstein",   # log of radiation energy density
    "log_fm",                # log of conformal coupling strength
    "log_T_Jordan",          # log of temperature in the Jordan frame
])
```

For a new project this is replaced with the instanton's own state variables.

---

## 7. Compute Targets

### 7.1 Design Pattern

Every compute target follows the same pattern:

1. **Is a `DatastoreObject`** with an `available` property.
2. **Is decorated `@ray.remote`** so instances can be passed to Ray tasks.
3. **Has a `compute(label: str) → ObjectRef` method** that dispatches the actual ODE/numerical work as a new Ray remote task.
4. **Has a `store()` method** that populates the object's internal data fields after `ray.get(compute_ref)` completes.
5. **Has a `failure` property** to indicate that computation was attempted but failed (distinguishing "not yet computed" from "failed to compute").

The `RayWorkPool` handles steps 3–4 automatically via its three defaults: `compute_handler`, `store_handler` and `persist_handler`.

### 7.2 `ScalarModel` (`ComputeTargets/ScalarModel.py`)

The primary compute target. Solves the chameleon scalar field ODE in an FRW background.

**Key internal classes:**

`ODEPolicy` — Encodes the ODE right-hand side. Its `__call__(N, state) → ODEPolicyData` computes all quantities needed for the next step. Returns:

```python
ODEPolicyData = namedtuple("ODEPolicyData", [
    "fm",                   # conformal coupling: f_m(φ)
    "T_Jordan",             # temperature in Jordan (matter) frame
    "Sigma",                # adiabatic perturbation parameter
    "log_V",                # log of scalar potential
    "V_over_3H2Mp2",        # dimensionless potential ratio
    "Vprime_over_3H2Mp2",   # dimensionless potential gradient
    "d_logOmega_dphi",      # conformal coupling derivative
    "friction_term",        # Hubble friction in field equation
    "reflecting_term",      # chameleon screening reflection term
    "kicking_term",         # conformal kick term
])
```

`HubblePolicy` — Computes Hubble rates in both frames from the ODE policy data and state.

`ODERHS` — The function passed to `scipy.integrate.solve_ivp`. Wraps `ODEPolicy` and uses `RHS_timer` to time each evaluation.

`SolutionFragment` — A namedtuple `(N_low, N_high, sol)` where `sol` is a `scipy.integrate.OdeSolution` (a callable interpolant). The integration may produce multiple fragments if the solver must restart due to reflection events or solver failures.

> **Note, 2026-10-01 (`integrator-remediation`, prompt 01, `fc97233`).** The `SolutionFragment` type is gone, with the fragment loop that produced it; the integration yields one `scipy.integrate.OdeSolution`, inside `IntegrationResult`. `ODERHS` is still the right-hand side, but it is now called by `integrate_scalar_history` through `scipy.integrate.Radau` rather than by `solve_ivp`, and since prompt 02 `RHS_timer` no longer prints a traceback for exceptions passing through it. See [`numerical-strategies.md` §3.5.1](numerical-strategies.md).

`SampleValues` — The values stored in the database per redshift grid point:

```python
SampleValues = namedtuple("SampleValues", [
    "raw_N",                  # e-fold number at this redshift
    "phi_Einstein",
    "pi_Einstein",
    "log_rhorad_Einstein",
    "log_rhorad_Jordan",
    "log_fm",
    "H_Einstein",             # Hubble in Einstein frame
    "H_Jordan",               # Hubble in Jordan (matter) frame
    "log_T_Jordan",
    "gstar_rho",              # effective DOF for energy density
    "gstar_s",                # effective DOF for entropy
    "dgstar_s_dlogT",         # temperature derivative of g*_s
    "dgstar_rho_dlogT",
    "Sigma",
    "friction_term",
    "reflecting_term",
    "kicking_term",
])
```

**Integration procedure in `ScalarModel.compute()`:**

1. Loop over solver methods: [Radau, BDF, LSODA, DOP853]. Try each in sequence until one succeeds.
2. For each solver attempt, call `scipy.integrate.solve_ivp` with event functions that detect: (a) temperature falling below `T_stop`, (b) field hitting the hard reflection wall, (c) entry/exit of bounce regions at two nested levels.
3. If a reflection event fires, flip the field momentum and restart integration from the event point. Append each sub-integration to the `SolutionFragment` list.
4. After integration completes (or all solvers fail), resample the solution fragments onto `z_grid` by evaluating each fragment's `OdeSolution` at the matching e-fold values.
5. Store sampled `SampleValues` in memory; `RayWorkPool` will later call `store()` and then `pool.object_store()`.

> **Note, 2026-10-01 (`integrator-remediation`, prompts 01 and 02).** Steps 1–4 describe the code at `b1f64d8`. There is no solver loop, no event function, no region and no `SolutionFragment` list now. `compute_scalar_model` builds the initial state as before and calls `integrate_scalar_history`: one `scipy.integrate.Radau` instance, stepped by hand under a kinematic maximum-step cap (`h ≤ f φ/|π|` if `π < 0`, `h ≤ sqrt(2 f φ/|π̇|)` if `π̇ < 0`, `f = 0.1`, global `0.1` e-fold) set before every step. An inward-moving field whose cap would fall below `1e-11` e-folds is reflected elastically (`π ← −π`) and the solver restarted there; an accepted `φ ≤ 0` is a failure, not a reflection. The history is one `OdeSolution`, resampled onto `z_grid` in step 4's place. A step budget of `2×10⁶` accepted steps turns a history that cannot finish into a failure row. Details, parameters and numbers: [`numerical-strategies.md` §3.5](numerical-strategies.md).

> **Note, 2026-10-02 (`science-readiness`, prompts 02, 03 and 06b): columns added to the
> `ScalarModel` table, and the properties that read them.** `VERSION_LABEL` is `"2026.6.0"` since
> prompt 01; there is no migration, so a store made earlier cannot be opened by this code and the
> science run needs a fresh datastore file. Nothing was removed from the table. Nine nullable
> columns were added, all on the parent row, so that a lookup with `_do_not_populate` returns them
> without loading the sample table:
>
> | column | type | units / content | added by |
> |---|---|---|---|
> | `failure_reason` | `String(256)` | the reason on a failure row; NULL on a success | prompt 02 |
> | `first_bounce_N` | `Float(64)` | forward e-fold number of the first bounce | prompt 03 |
> | `first_bounce_log_T_Jordan` | `Float(64)` | ln of T_J in GeV at the first bounce | prompt 03 |
> | `first_bounce_phi_Einstein` | `Float(64)` | φ at the first bounce, in M_P | prompt 03 |
> | `first_bounce_reflected` | `Boolean` | the first bounce was an elastic reflection | prompt 03 |
> | `phi_Einstein_1MeV` | `Float(64)` | φ at the first crossing of T_J = 1 MeV, in M_P | prompt 06b |
> | `density_NP_ratio_1MeV` | `Float(64)` | ρ_NP/ρ_R,J there | prompt 06b |
> | `phi_Einstein_70keV` | `Float(64)` | φ at the first crossing of T_J = 70 keV, in M_P | prompt 06b |
> | `density_NP_ratio_70keV` | `Float(64)` | ρ_NP/ρ_R,J there | prompt 06b |
>
> The four `first_bounce_*` columns are NULL when there is no bounce and on a failure row; so are
> the four fixed-T columns, and each pair is NULL on its own where the history does not reach that
> temperature. The table has 33 columns of its own, plus `serial`, `version` and `timestamp`.
>
> New properties on `ScalarModel`, each raising `RuntimeError` on a failure row (except
> `failure_reason`) and on an object that was never populated: `failure_reason -> Optional[str]`,
> `first_bounce -> Optional[FirstBounce]` and `fixed_T_values -> FixedTValues`
> (`FirstBounce(N, phi_Einstein, log_T_Jordan, reflected)` and `FixedTValues(phi_Einstein_1MeV,
> density_NP_ratio_1MeV, phi_Einstein_70keV, density_NP_ratio_70keV)`, both in
> `ComputeTargets/ScalarModel.py` and not re-exported by the package). `compute_scalar_model`'s
> success payload carries `"first_bounce"` and `"fixed_T_values"` (`store()` indexes both, so a
> hand-built success payload in a test must carry them), and its failure payloads carry
> `"failure_reason"`. `SampleValues` and the `ScalarModelValue` table are **unchanged**: there is no
> cell-mean field or column, because the bounce averages were withdrawn (see
> [`numerical-strategies.md` §3.6](numerical-strategies.md)). The definitions, units and values are
> in that section.

**`ScalarModelProxy`:** A lightweight reference object (just holds the model's `store_id` and key parameters, not the full solution array). Passed to downstream compute targets (AdiabaticHistory, BBNData) so they can fetch the full `ScalarModel` from the database only when needed.

### 7.3 `AdiabaticHistory` (`ComputeTargets/AdiabaticHistory.py`)

Computes the adiabatic perturbation parameter Q (which determines whether the chameleon field is adiabatically tracking its minimum). Requires a `ScalarModelProxy` as input.

### 7.4 `BBNData` (`ComputeTargets/BBNData.py`)

Interfaces with the external PRyMordial library to compute primordial nucleosynthesis abundances (Y_p, D/H, Li-7/H). Also requires a `ScalarModelProxy`.

The function `compute_BBN_data` is a `@ray.remote` function (not a class method). It constructs splines of `ρ_NP(T)` and `p_NP(T)` from the scalar field history, then passes them to PRyMordial as "new physics" contributions to the Friedmann equations.

> **Note, 2026-10-02 (`science-readiness`, prompts 01 and 06): `compute_BBN_data` as it now is.**
> The sentence above describes the code at `eba4473` and before.
>
> - **One callable, not two.** `compute_BBN_data` builds `ρ_NP(T) = r(T) ρ_SM(T)`: `r = ρ_NP/ρ_R,J`
>   is splined (cubic, in ln T) from the stored samples with `T_J ∈ [0.2 keV, 100 MeV]` and
>   multiplied back by the thermodynamic ρ_SM (`build_rho_NP_callback`,
>   `thermodynamic_rho_SM`). `p_NP`, the derivative callback, `jordan_Hdot_over_H2` and
>   `NPCallbacks` are removed.
> - **Hubble-only.** The vendored PRyMordial is patched so that ρ_NP enters the expansion rate
>   and nothing else (`PRyM_init.NP_hubble_flag`); `NP_thermo_flag` is off, and `_configure_PRyMordial`
>   asserts the flags. The patch also adds an optional wall-clock limit to `PRyMclass`. Every
>   hunk is listed in [`numerical-strategies.md` §7.6.4](numerical-strategies.md);
>   `PRYM_VERSION = "bf24c3d+ri02+sr01"`.
> - **Signature.** `compute_BBN_data(model_proxy, task_label, T_BBN_MeV_spline_max=100,
>   T_BBN_keV_spline_min=0.2, small_network=False, wall_clock_limit=DEFAULT_BBN_WALL_CLOCK_LIMIT)`.
>   The `BBNData.compute` payload is `{"small_network": False, "wall_clock_limit": …}`
>   (`main.py` passes `None` for `--bbn-wall-clock-limit 0`).
> - **Checks.** A result is stored only if all four abundances are finite, `0 < Yp < 0.5` and the
>   other three are positive; otherwise a failure row with reason `"PRyMordial output: …"`. Fewer
>   than four in-window samples, a non-finite sample or callback value, and a wall-clock overrun are
>   failure rows with reasons too.
> - **`BBNDataValue`** loses the column `pressure_NP_MeV4` (and the `pressure_NP` field and
>   property). It keeps `density_NP` and `density_NP_ratio` per sample. `BBNData` itself gains no
>   column.
> - **Plots.** `plot_ScalarModel.py`'s BBN figure has three panels (ratio, H, ρ_NP); the |p_NP| and
>   w_NP panels are gone.

---

## 8. Domain Concepts

### 8.1 Units (`Units/`)

`UnitsLike` is an abstract base class defining a unit system. Physical quantities are always stored internally in Planck units (default), but can be input/output in any supported unit system.

```python
class UnitsLike:
    # SI base
    Metre, Kilometre, Kilogram, Second, Kelvin: float
    
    # Particle physics
    eV, keV, MeV, GeV: float
    PlanckMass: float
    c: float           # speed of light
    
    # Cosmological
    Mpc: float
```

All quantity objects (`M_value`, `temperature`, etc.) carry a reference to the `UnitsLike` instance under which they were created, for unit-safe arithmetic.

### 8.2 Quantity Classes (`CosmologyConcepts/`)

**`DimensionlessQuantity`:** Abstract base for parameters like `beta_value`. Stores a single float; equality uses a configurable relative tolerance.

**`DimensionfulQuantity`:** Abstract base for dimensionful parameters. Stores a float in Planck units internally; construction from other unit systems multiplies by the appropriate `UnitsLike` conversion factor.

**Concrete quantity classes:**
- `beta_value`: coupling exponent β (also the shard key)
- `M_value`: potential mass scale
- `Lambda_value`: potential height scale
- `temperature`: thermodynamic temperature
- `phi_value`, `pi_value`: scalar field and canonical momentum
- `redshift` / `redshift_array`: redshift values and sorted arrays thereof

### 8.3 Potentials (`CosmologyConcepts/Potentials/`)

`AbstractPotential` defines the interface:

```python
class AbstractPotential(DatastoreObject):
    # Required properties
    name: str
    type_id: int
    
    # Bounce region tuning (chameleon-specific)
    bounce_region_level1_boundary: float
    bounce_region_level2_boundary: float
    bounce_region_level1_max_step: float
    bounce_region_level2_max_step: float
    hard_reflection_point: float
    
    # Required methods
    def log_V(self, phi) -> float: ...
    def d_V_dphi(self, phi) -> float: ...
    def d2_V_dphi2(self, phi) -> float: ...
```

> **Note, 2026-10-01 (`integrator-remediation`, prompt 01, `fc97233`).** The five `bounce_region_level{1,2}_*` and `hard_reflection_point` properties are still defined on `AbstractPotential` and its potentials, but nothing reads them since `VERSION_LABEL = "2026.5.0"`: the regions are gone (see [`numerical-strategies.md` §3.5.2](numerical-strategies.md)). The same commit added two non-abstract properties, `reflects_at_origin -> bool` (default `False`) and `log_V_floor -> Optional[float]` (default `None`), implemented on `ExponentialPotential` only; they let the integrator's elastic reflection (§3.5.3 there) run for a potential that declares it. Removing the region properties is open issue `[00-region-properties-on-the-potentials-become-unread]`.

Concrete implementations: `ExponentialPotential`, `InversePowerPotential`, `StarobinskyPotential`, `ReclinerPotential`, `ReflectingPotential`.

### 8.4 Conformal Couplings (`CosmologyConcepts/ConformalCouplings/`)

`AbstractCoupling` defines:

```python
class AbstractCoupling(DatastoreObject):
    name: str
    type_id: int
    shard_key: ShardKeyType    # returns the beta_value used for sharding
    
    def log_Omega(self, phi) -> float: ...        # log of conformal factor Ω(φ)
    def Omega(self, phi) -> float: ...
    def d_logOmega_dphi(self, phi) -> float: ...
    def d2_logOmega_dphi2(self, phi) -> float: ...
```

Currently only `ExponentialCoupling` is implemented: Ω(φ) = exp(βφ/Mp).

### 8.5 Cosmological Models (`CosmologyModels/`)

`BaseCosmology` is the abstract base:

```python
class BaseCosmology(DatastoreObject):
    type_id: int
    name: str
    units: UnitsLike
    
    def z(self, T: float) -> float: ...    # redshift as function of temperature
```

Currently `QCD_Cosmology` is the main implementation, using a lattice QCD equation of state for g*(T).

### 8.6 Metadata (`MetadataConcepts/`)

- `version`: labels a computation run for reproducibility
- `tolerance`: stores ODE tolerance as `log10_tol` (avoids floating-point comparison issues)
- `store_tag`: groups related computations; used by `RayWorkPool` to tag outputs (e.g. `SamplesPerLog10Z_250`)

---

## 9. The Main Pipeline (`main.py`)

The pipeline is driven by the `execute()` and `run_pipeline()` functions.

**`execute()` — Setup:**

1. Initializes Ray (`ray.init(address=args.ray_address)`)
2. Constructs `ShardedPool`
3. Samples parameter grids (β, M, Λ) using `np.linspace`/`np.logspace`
4. Registers all grid values in the database via batched `pool.object_get()` calls
5. Builds `Potential_array` and `Coupling_array` from the registered parameter objects
6. Builds the `z_grid` (`redshift_array`) for output sampling
7. Registers solver labels (`IntegrationSolver` objects) in the database
8. Loops over cosmological models and calls `run_pipeline()` for each

**`run_pipeline()` — Three-stage computation:**

```
STAGE 1: Scalar Field Histories
   model_sample_grid = product(Potential_array, Coupling_array)
   Batched in groups of 8, binned by shard_key (β)
   → Two-pass: vectorized existence check, then enqueue missing
   → RayWorkPool with compute_handler = ScalarModel.compute()
   → RayWorkPool with validation_handler = pool.object_validate()

STAGE 2: Adiabatic Histories
   Same (Potential, Coupling) grid
   → Two-pass: check ScalarModel exists (error if not), check AdiabaticHistory exists
   → For missing: fetch ScalarModelProxy, enqueue AdiabaticHistory
   → RayWorkPool with compute_handler = AdiabaticHistory.compute()

STAGE 3: BBN Data
   Same grid
   → Two-pass: check ScalarModel exists, check BBNData exists
   → For missing: fetch ScalarModelProxy, enqueue BBNData
   → RayWorkPool with compute_handler = BBNData.compute(payload={"small_network": True})
```

**Note added 2026-09-30 (production-readiness prompt 02, item P2).** `main.py` now passes
`payload={"small_network": False}`: production runs PRyMordial's full reaction network. The
`True` above never reached PRyMordial (it set the unread `small_network_flag`, not
`smallnet_flag`), so every earlier solve also ran the full network; the switch now works. See
`prompts/production-readiness/logs/02-wire-the-network-flag.md`.

**Note added 2026-09-30 (run-integrity prompt 03, item R).** Two changes to stages 2 and 3.

- **A stored BBN failure counts as done.** Stage 3 looks up `BBNData` with `failure=None`, which
  returns the success if one exists and otherwise the newest failure. So a BBN computation that
  failed under the current `VERSION_LABEL` is final: it is not recomputed, and no new failed row
  is stored, on a later run.
  - `--retry-failed-bbn` (default off) counts a stored failure as missing, so those
    computations are retried under the same label.
  - A new `VERSION_LABEL` retries them too, since the lookups are keyed on the label (prompt
    01): rows made under an earlier label are not returned.
  - The reason stays in `BBNData.failure_reason`.
- **Lookup results are paired with the models they were asked for.** Both stages build their
  downstream query from the `ScalarModel`s that did not fail. Before this prompt, they then
  zipped the results against the unfiltered bin, so one failed model shifted every later
  result. The decision now lives in the pure module `pipeline_selection.py`:
  - `build_query_entries` keeps each (potential, coupling) with its `ScalarModel`;
  - `select_missing` pairs each downstream result with that entry, and raises if the counts
    differ.
- **Summary lines.** After each stage's queue, `run_pipeline` prints one line: the models
  skipped because their `ScalarModel` failed and, for BBN, the computations skipped because of
  a stored failure (or retried, under the flag).

See `prompts/run-integrity/logs/03-failure-caching-and-pairing.md`.

**Note added 2026-10-02 (`science-readiness`, prompts 01, 02, 04): changes to the stages.**

- **Before step 1.** `main.py` calls `pipeline_selection.warn_super_planckian` on the coupling
  array: one warning line per coupling with Ω(φ\*) T\* > M_P, then a count. It returns the array
  unchanged, so nothing is dropped.
- **Stage 1 (`ScalarModel`).** After the queue, `main.py` prints `-- ScalarModel: N histories
  failed in this run`, grouped by the first clause of the stored reason
  (`pipeline_selection.summarise_failure_reasons`). It counts only this run's failures: a stored
  failure row counts as present and is not recomputed. `plot_by_beta.py` prints the reason for each
  model it drops because its `ScalarModel` failed.
- **Stage 3 (`BBNData`).** The compute payload is `{"small_network": False, "wall_clock_limit":
  <--bbn-wall-clock-limit, None if 0>}` (the earlier note above gives `{"small_network": False}`).
  A PRyMordial solve past the limit, or one whose output fails the checks of §7.4's note, is a
  stored failure with its reason, final within a label and retried by `--retry-failed-bbn`.

**The two-pass batching pattern** (repeated at each stage) is important for efficiency:

```python
# Pass 1: vectorized existence check, _do_not_populate=True
query_queue = RayWorkPool(
    pool, query_batch,
    task_builder=lambda x: pool.object_get_vectorized(
        "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
    ),
    compute_handler=None, store_handler=None,
    store_results=True,
)
query_queue.run()

# Identify missing items
missing = [(pot, coup) for obj, (pot, coup) in zip(results, batch) if not obj.available]

# Pass 2: scalar object_get (creates skeleton rows) for missing items only
work_refs = [pool.object_get("ScalarModel", shard_key=..., ...) for pot, coup in missing]
```

---

## 10. Utilities (`utilities.py`)

```python
class WallclockTimer:
    """Context manager returning elapsed seconds in self.interval."""

def format_time(interval: float) -> str:
    """Human-readable duration: '1h 23m 45s'"""

def format_energy(value: float, units: UnitsLike) -> str:
    """Auto-scale energy to most readable unit (eV, keV, MeV, GeV, Mp)"""

class energy_formatter:
    """Callable class wrapping format_energy for repeated use with fixed units."""

def grouper(iterable, n, incomplete='fill', fillvalue=None):
    """Chunk an iterable into batches of size n (from itertools recipes)."""

def to_float(val) -> float:
    """Convert numpy scalar to Python float safely."""
```

---

## 11. Error Handling

**`ComputationFailureError`** (`ComputeTargets/exceptions.py`): raised when an ODE integration produces NaN/Inf, encounters unphysical state (negative energy density, imaginary Hubble rate), or overflows exponential bounds. Caught by the solver loop in `ScalarModel.compute()`.

**Solver fallback:** `ScalarModel.compute()` iterates through [Radau, BDF, LSODA, DOP853]. If one solver raises `ComputationFailureError` or `solve_ivp` fails to converge, the next solver is tried. If all solvers fail, the object is marked with `failure=True` and stored in the database as a failed record (so the system does not attempt to recompute it on subsequent runs).

> **Note, 2026-10-01 (`integrator-remediation`, prompt 02, `614c41a`).** The solver fallback described here does not exist and never ran: `method="Radau"` was a literal since `f67bc3a`, so a failing history was integrated four times identically. It is deleted. `ScalarModel.compute()` runs one Radau step loop (`integrate_scalar_history`), inside one `try`; a `ComputationFailureError` from the loop or from the sampling is printed and the function returns `{"failure": True}`, which is stored as a failure row (the reason is printed, not stored). The exception table, including the new step budget, is in [`numerical-strategies.md` §3.5.5–§3.5.6](numerical-strategies.md). The paragraph above is kept as written for `b1f64d8`.

**`RayWorkPool` resilience:** The work pool does not bail out if individual tasks fail — it continues processing remaining items. The `_do_not_populate` two-pass pattern means failed objects are skipped in downstream stages (checked via `obj.failure`).

**Database startup cleanup:** `prune_unvalidated=True` causes each `Datastore` actor to delete any objects created but not yet validated on the previous run. This prevents partial results from being presented as complete.

---

## 12. Adaptation Guide for an Instanton Project

The following table maps ChamPBH components to their natural counterparts in a stochastic instanton project:

| ChamPBH component | Instanton project equivalent |
|-------------------|------------------------------|
| `ScalarModel` | `InstantonSolution` — solves the instanton ODE in the inflationary background |
| `AdiabaticHistory` | `CompactionFunction` — computes the compaction function C(r) from the instanton |
| `BBNData` | `PBHMassFunction` — integrates to produce the PBH mass function β(M) |
| `ScalarModelProxy` | `InstantonProxy` — lightweight reference for downstream stages |
| `beta_value` (shard key) | Could be retained, or replaced with the relevant free parameter of the inflationary model (e.g. amplitude of perturbation, spectral index) |
| `ExponentialCoupling` | Inflationary potential parameters (if scanning a family of potentials) |
| `AbstractPotential` | Inflationary potential V(φ) |
| `StateVector` | ODE state for instanton: {φ(r), φ'(r), ...} |
| `ODEPolicy` | RHS of instanton ODE |
| `ScalarFieldIntegrationSupervisor` | `InstantonIntegrationSupervisor` — tracks turning points, horizon crossing, etc. |
| `SampleValues` | Sampled instanton profile values at a grid of radii |
| `AdiabaticHistoryValue` | Compaction function values at a grid of radii |
| `BBNDataValue` | PBH mass function values at a grid of masses |
| `QCD_Cosmology` | Background inflationary spacetime (de Sitter or quasi-de Sitter) |
| `config/sharding.py` | Updated with new shard key type and table lists |
| `config/datastore.py` | The registry of the new factories, their drop groups and batch sizes |
| `main.py` `run_pipeline()` | New pipeline: InstantonSolution → CompactionFunction → PBHMassFunction |

An instanton project adopts `datastorekit` and `raypoolkit` as packages, pinned to a tag in `requirements.txt` and never patched in place. What it writes is a registry of factories, the factories, and a sharding config (§4.2).

**Components that can be used with zero or minimal modification:**
- the work queue: the `raypoolkit` package (`RayWorkPool`)
- the datastore layer: the `datastorekit` package (`ShardedPool`, the `Datastore` actor, `SerialPoolBroker`, `ClientPool`, `ProfileAgent`, `DatastoreObject`, `SQLAFactoryBase`, the read-only reader and inventory); a project supplies the registry (`config/datastore.py`) and the sharding config
- `IntegrationSupervisor` base class and `RHS_timer`
- `IntegrationSolver`, `IntegrationData`
- `UnitsLike` and all concrete unit systems
- `DimensionlessQuantity`, `DimensionfulQuantity` and their factories
- `version`, `tolerance`, `store_tag` and their factories
- `redshift`, `redshift_array` (if radial coordinate is redshift-based)
- `utilities.py` entirely
- YAML/configargparse configuration pattern

**Components requiring replacement:**
- `ComputeTargets/ScalarModel.py` → instanton ODE and solution class
- `Quadrature/supervisors/ScalarField.py` → `StateVector` and event tracking for instanton
- `ComputeTargets/AdiabaticHistory.py` → compaction function calculation
- `ComputeTargets/BBNData.py` → PBH mass function integration
- `ObjectFactories/ScalarModel.py` → factory for `InstantonSolution`
- `ObjectFactories/AdiabaticHistory.py` → factory for `CompactionFunction`
- `ObjectFactories/BBNData.py` → factory for `PBHMassFunction`
- `CosmologyConcepts/Potentials/` → inflationary potential hierarchy
- `CosmologyConcepts/ConformalCouplings/` → may not be needed, or replaced with inflationary model parameters
- `CosmologyModels/` → inflationary background (de Sitter, quasi-de Sitter)
- `config/sharding.py` → updated shard key and table lists
- `config/datastore.py` → the registry of the new factories, their drop groups and batch sizes
- `main.py` → new pipeline driver
- `config/model_list.py` → inflationary model list

---

## 13. Example: How a New Compute Target Integrates

To add an `InstantonSolution` compute target:

**Step 1 — Domain class** (`ComputeTargets/InstantonSolution.py`):

```python
@ray.remote
class InstantonSolution(DatastoreObject):
    def __init__(self, store_id, model_params, ...):
        super().__init__(store_id)
        # ... store parameters
    
    @property
    def available(self) -> bool:
        return self._store_id is not None and not self._failure
    
    @property
    def failure(self) -> bool:
        return self._failure
    
    def compute(self, label: str) -> ObjectRef:
        # Returns ObjectRef to a Ray remote task
        return _do_instanton_integration.remote(self, label)
    
    def store(self):
        # Copies computation result into self (called by RayWorkPool after compute)
        pass
```

**Step 2 — Factory** (`ObjectFactories/InstantonSolution.py`):

Subclass `datastorekit.SQL.factory_base.SQLAFactoryBase` (§4.4) with:
- `register()` returning the SQLAlchemy column schema and the keys the schema builder reads
- `build(payload, conn, table, inserter, tables, inserters)` to SELECT / INSERT
- `store(obj, conn, table, inserter, tables, inserters)` to serialize computed results
- `validate(obj, conn, table, tables)` to verify integrity
- `validate_on_startup(...)` and `inventory_spec()`, where the class needs them

The hooks and keys, and what the package does when one is wrong or absent, are the client contract's §2–§4 (§4.1).

**Step 3 — Register** in `config/datastore.py`, as entries of `factories` (and of `drop_groups` and `serial_batch_sizes`, where wanted):

```python
factories["InstantonSolution"] = sqla_InstantonSolution_factory()
factories["InstantonSolution_tags"] = sqla_InstantonSolutionTagAssociation_factory()
factories["InstantonSolutionValue"] = sqla_InstantonSolutionValue_factory()
```

**Step 4 — Sharding** in `config/sharding.py`:

```python
sharded_tables["InstantonSolution"] = "shard_key"
sharded_tables["InstantonSolutionValue"] = "shard_key"
```

**Step 5 — Pipeline** in `main.py` or a new driver file:

```python
work_pool = RayWorkPool(
    pool,
    work_batches,
    task_builder=build_instanton_batch,
    compute_handler=lambda obj, label, **kw: obj.compute(label=label),
    validation_handler=lambda obj: pool.object_validate(obj),
    label_builder=lambda obj: f"Instanton-{obj.model_label}",
    title="COMPUTE INSTANTON SOLUTIONS",
)
work_pool.run()
```

---

## 14. Key Invariants and Gotchas

1. **Shard key must be in the database before sharded objects.** A `beta_value` (or equivalent shard key) must be inserted into the replicated tables and assigned to a shard before any `ScalarModel` (or equivalent) referencing that key can be stored. `ShardedPool` handles this automatically when `object_get` is called on the shard-key type (its `_assign_shard_keys`; client contract §1, row 3).

2. **Replicated table `store_id`s must be identical across shards.** The `SerialPoolBroker` coordinates this. Never insert a replicated-table object directly into a shard — always go through `ShardedPool.object_get()` (a replicated write carries the controlling shard's serials to the others; contract §6).

3. **`_do_not_populate` is advisory.** It is a payload key that ChamPBH's `ScalarModel` and `BBNData` factories read (§4.3), not the package's. If the object already exists in the database (from a previous run), `object_get` returns it regardless of this flag. The flag only suppresses the INSERT when the object is absent.

4. **`compute()` must return an `ObjectRef`.** `RayWorkPool` expects a Ray future from `compute_handler`. If the computation is CPU-bound and cannot be a Ray task (rare), wrap it in `ray.remote`.

5. **`store()` is called on the driver side, not in Ray.** After `ray.get(compute_ref)` completes, `RayWorkPool` calls `obj.store()` on the Python object in the driver process. This should copy data from the compute result into the object's fields. Then `pool.object_store(obj)` is called to serialize to the database.

6. **Shard count is fixed at database creation time.** The number of shards cannot be changed for an existing database (the primary's records decide; contract §1, row 8). Plan the shard count to match the expected parallelism and data volume.

7. **Ray actor naming.** Each shard `Datastore` is named `"shard{N:04d}-store"` and the `SerialPoolBroker` is named `"SerialPoolBroker"`. If reusing actor names across sessions (e.g., when `ray.init` reuses a cluster), ensure actors from previous sessions have been cleaned up.

8. **`store_results=True` in `RayWorkPool` is only for query-only passes.** When `task_builder` returns lists of refs (the vectorized case), the indices break. Use `store_results=True` only with scalar-returning `task_builder`s.

9. **The `grouper` + bin-by-shard pattern.** The main pipeline groups work items into batches of 8, then within each batch bins by `shard_key`. This minimizes round-trips by sending multiple items to the same shard in one call. For best efficiency in a new project, maintain this pattern.
