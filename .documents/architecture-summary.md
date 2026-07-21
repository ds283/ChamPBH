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
├── plot_by_beta.py                  # Plotting driver (Ray remote)
├── plot_ScalarModel.py              # Scalar field detailed plots
├── exponential.yaml                 # Example run configuration
├── starobinsky.yaml
├── recliner.yaml
│
├── RayTools/
│   └── RayWorkPool.py               # Generic distributed work queue
│
├── Datastore/
│   ├── object.py                    # DatastoreObject base class
│   └── SQL/
│       ├── Datastore.py             # Ray actor: one SQLite shard
│       ├── ShardedPool.py           # Manages pool of shard actors
│       ├── SerialPoolBroker.py      # Cross-shard serial-ID coordination
│       ├── ClientPool.py            # Serial lease management
│       ├── ProfileAgent.py          # Optional DB profiling actor
│       └── ObjectFactories/         # SQLAlchemy serialization factories
│           ├── base.py              # SQLAFactoryBase abstract class
│           ├── version.py
│           ├── store_tag.py
│           ├── redshift.py
│           ├── tolerance.py
│           ├── integration_metadata.py
│           ├── DimensionlessQuantity.py
│           ├── DimensionfulQuantity.py
│           ├── ExponentialPotential.py
│           ├── InversePowerPotential.py
│           ├── StarobinskyPotential.py
│           ├── ReclinerPotential.py
│           ├── ReflectingPotential.py
│           ├── ExponentialCoupling.py
│           ├── QCD_Cosmology.py
│           ├── ScalarModel.py       # Factory for main ODE output
│           ├── AdiabaticHistory.py
│           └── BBNData.py
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
    ├── defaults.py                  # Tolerance/precision constants
    ├── model_list.py                # Cosmological model registry
    └── sharding.py                  # Shard-key type and table lists
```

---

## 3. The Ray Distributed Computing Layer

### 3.1 Design Philosophy

All expensive computations are dispatched as Ray remote tasks or actors. The driver (main.py) never blocks waiting for a single result; it maintains a bounded queue of in-flight Ray `ObjectRef`s and processes completions as they arrive. This allows the driver to pipeline database lookups, compute tasks, and store tasks concurrently.

### 3.2 `RayWorkPool` (`RayTools/RayWorkPool.py`)

`RayWorkPool` is the single generic orchestrator for all computation phases. It is instantiated with a list of work items and a set of handler callables, and its `run()` method drives a state machine until every item has been processed.

**Constructor parameters:**

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `pool` | `ShardedPool` | required | Database access |
| `task_list` | iterable | required | Items to process |
| `task_builder` | `Callable(item) → ObjectRef \| list[ObjectRef] \| None` | required | Converts each work item to a Ray DB lookup |
| `compute_handler` | `Callable(obj, label?, payload?) → ObjectRef` | `obj.compute()` | Dispatches ODE/computation |
| `store_handler` | `Callable(obj, pool) → ObjectRef` | `pool.object_store(obj)` | Persists result |
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

Each work item passes through states tracked by the string tag in `self._data[ref.hex]`:

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
    obj.store()  (mutates obj in-place)
    store_handler(obj) → ObjectRef tagged "store"

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

### 4.1 Architecture

The database is a **sharded SQLite** system managed as a pool of Ray remote actors. Each actor owns one SQLite file. A separate primary SQLite file records the shard topology.

```
primary.db           ← shard topology, key→shard mapping
primary-shard0000.db ← Datastore actor #0
primary-shard0001.db ← Datastore actor #1
...
primary-shardNNNN.db ← Datastore actor #N
```

All actors run in the same Ray cluster but are independent processes, so SQLite's single-writer constraint is respected per-shard.

### 4.2 `DatastoreObject` (`Datastore/object.py`)

Every persistent object inherits from `DatastoreObject`:

```python
class DatastoreObject:
    def __init__(self, store_id: Optional[int])
    
    @property
    def store_id(self) -> int      # raises if None
    
    @property
    def available(self) -> bool    # True if store_id is not None
```

The `available` property is the sentinel used by `RayWorkPool` to distinguish "already in database" from "needs computation".

### 4.3 `Datastore` (`Datastore/SQL/Datastore.py`) — Ray Remote Actor

A single-shard database manager. One instance per shard file.

**Registered factories** (the complete set as of writing):

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
| `ScalarModel_tags` | `sqla_ScalarModelTagAssociation_factory` | Sharded |
| `ScalarModelValue` | `sqla_ScalarModelValue_factory` | Sharded |
| `AdiabaticHistory` | `sqla_AdiabaticHistoryFactory` | Sharded |
| `AdiabaticHistory_tags` | `sqla_AdiabaticHistoryTagAssociation_factory` | Sharded |
| `AdiabaticHistoryValue` | `sqla_AdiabaticHistoryValue_factory` | Sharded |
| `BBNData` | `sqla_BBNDataFactory` | Sharded |
| `BBNData_tags` | `sqla_BBNDataTagAssociation_factory` | Sharded |
| `BBNDataValue` | `sqla_BBNDataValue_factory` | Sharded |

**Key actor methods:**

- `object_get(class_name, **kwargs) → ObjectRef` — Looks up or creates an object. The factory's `construct()` method runs a `SELECT`; if nothing is found and `_do_not_populate` is not set, it calls `deconstruct()` to `INSERT` a skeleton row. Returns the object (possibly with `store_id=None` if not yet computed).
- `object_store(obj) → ObjectRef` — Takes an object populated with computed data, writes it to the database, and returns a copy with `store_id` assigned.
- `object_validate(obj) → ObjectRef` — Runs post-store integrity checks; returns `True`/`False`.
- `read_largest_store_ids() → dict` — Used during startup to synchronize the serial broker.
- `inventory(class_name) → dict` — Returns human-readable summary for `--inventory` mode.

**The `_do_not_populate` flag:** When passed as a keyword argument to `object_get`, it suppresses the INSERT of a skeleton row. This allows a fast "does this exist?" query without side effects. Used in the two-pass pattern: first check which items exist, then enqueue only the missing ones for computation.

**Serial ID management:** Each object class has a monotonically increasing integer serial number. The `SerialPoolBroker` actor coordinates across shards so that serial numbers never collide between shards (critical for replicated tables that must have the same `store_id` on every shard).

### 4.4 `ShardedPool` (`Datastore/SQL/ShardedPool.py`)

The user-facing database interface. Not a Ray actor itself — runs in the driver process and calls into the shard `Datastore` actors.

**Constructor parameters (key ones):**

| Parameter | Purpose |
|-----------|---------|
| `version_label` | String versioning tag for reproducibility |
| `db_name` | Path to the primary SQLite file |
| `ShardKeyType` | Python class used as the shard key (currently `beta_value`) |
| `ShardKeyStoreIdGetter` | `Callable(key) → int` extracts `store_id` from a key object |
| `replicated_tables` | List of table names that are identical on every shard |
| `sharded_tables` | Dict of `{table_name: shard_key_field}` for sharded tables |
| `shards` | Number of shards to create (fixed at creation time) |
| `prune_unvalidated` | If True, deletes objects lacking validation on startup |
| `drop_actions` | List of table names to drop and recreate on startup |
| `read_table_config` | Optional custom readers for replicated tables |
| `inventory_config` | Merge policies for cross-shard inventory aggregation |

**Sharding strategy:** The shard is determined by the `shard_key` field on an object. In ChamPBH, this is always a `beta_value` object. A lookup table in the primary database maps `beta_value.store_id → shard_id`. Shard assignment is load-balanced at the time a new `beta_value` is first inserted.

**Replicated vs. sharded tables:**

- **Replicated tables** (all parameter types, potentials, cosmology, metadata): stored identically on every shard. When a new object is created, `ShardedPool` writes it to one randomly chosen shard and then copies it (with the same `store_id`) to all remaining shards.
- **Sharded tables** (compute results: ScalarModel, AdiabaticHistory, BBNData): stored only on the shard determined by the `shard_key`. The `object_store` method routes the write to a single shard.

**Key methods:**

```python
# Scalar lookup — works for both replicated and sharded tables
pool.object_get(ObjectClass, **kwargs) → ObjectRef

# Batch lookup — only for sharded tables, sends all items for one shard_key
# to a single shard actor in one call
pool.object_get_vectorized(ObjectClass, shard_key, payload_data: List[dict]) → ObjectRef

# Batch lookup within a shard (alternative batching method)
pool.object_read_batch(ObjectClass, shard_key, **payload) → ObjectRef

# Store a single object or list of objects
pool.object_store(obj) → ObjectRef

# Validate after storage
pool.object_validate(obj) → ObjectRef

# Human-readable inventory (used by --inventory flag)
pool.inventory(cls) → dict
```

**The two-pass query pattern** used in `main.py` for all compute stages:

1. Call `object_get_vectorized(..., _do_not_populate=True)` across all items in a batch. This returns skeleton objects; check `obj.available` to identify which are already computed.
2. For missing items only, call `object_get(...)` without `_do_not_populate`. This creates skeleton rows and returns objects ready for `RayWorkPool` to compute and store.

### 4.5 `SQLAFactoryBase` (`Datastore/SQL/ObjectFactories/base.py`)

The abstract base class for all object factories. Each factory registers one object class with the `Datastore`.

**Required methods:**

```python
class SQLAFactoryBase:
    def register(self) -> dict:
        # Returns schema definition:
        # {
        #   "name": "TableName",
        #   "version": int,                     # schema version for migrations
        #   "columns": [...sqla.Column(...)],    # SQLAlchemy column definitions
        #   "values": [...sqla.Column(...)],     # columns for value sub-table (optional)
        #   "tags": bool,                        # whether to create a tags association table
        # }
    
    def build_object(self, payload: dict, conn, table, ...):
        # Called by object_get() when a new object must be created/fetched.
        # Runs SELECT; if not found and not _do_not_populate, runs INSERT.
        # Returns the Python domain object.
    
    def build_object_list(self, payload_list: List[dict], conn, table, ...):
        # Vectorized version of build_object() for batch queries.
    
    def store_object(self, obj, conn, table, ...):
        # Called by object_store(). Serializes computed data into the database.
    
    def validate_object(self, obj, conn, table, ...) -> bool:
        # Called by object_validate(). Reads back and checks stored data.
```

To add a new persistent object class to a project based on this codebase, you need to: (a) write the factory, (b) register it in the `_factories` dict in `Datastore.py`, (c) add the table name to either `replicated_tables` or `sharded_tables` in `config/sharding.py`.

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
- `--prune-unvalidated` (default True)
- `--drop {scalar-model,adiabatic-history,bbn-data}`

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
- `--ray-address STR` (default 'auto')

### 5.2 Sharding Configuration (`config/sharding.py`)

This file defines what the `ShardedPool` constructor needs:

```python
ShardKeyType = beta_value   # The type used as the shard key

replicated_tables = [
    "version", "store_tag", "redshift", "tolerance",
    "beta_value", "M_value", "Lambda_value", "temperature",
    "phi_value", "pi_value",
    "ExponentialPotential", "InversePowerPotential", "ReflectingPotential",
    "ReclinerPotential", "StarobinskyPotential",
    "LambdaCDM", "QCD_Cosmology",
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

The `RayWorkPool` handles steps 3–4 automatically via its default `compute_handler` and `store_handler`.

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

**`ScalarModelProxy`:** A lightweight reference object (just holds the model's `store_id` and key parameters, not the full solution array). Passed to downstream compute targets (AdiabaticHistory, BBNData) so they can fetch the full `ScalarModel` from the database only when needed.

### 7.3 `AdiabaticHistory` (`ComputeTargets/AdiabaticHistory.py`)

Computes the adiabatic perturbation parameter Q (which determines whether the chameleon field is adiabatically tracking its minimum). Requires a `ScalarModelProxy` as input.

### 7.4 `BBNData` (`ComputeTargets/BBNData.py`)

Interfaces with the external PRyMordial library to compute primordial nucleosynthesis abundances (Y_p, D/H, Li-7/H). Also requires a `ScalarModelProxy`.

The function `compute_BBN_data` is a `@ray.remote` function (not a class method). It constructs splines of `ρ_NP(T)` and `p_NP(T)` from the scalar field history, then passes them to PRyMordial as "new physics" contributions to the Friedmann equations.

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

**The two-pass batching pattern** (repeated at each stage) is important for efficiency:

```python
# Pass 1: vectorized existence check, _do_not_populate=True
query_queue = RayWorkPool(
    pool, query_batch,
    task_builder=lambda x: pool.object_get_vectorized("ScalarModel", x["shard_key"], x["payload"]),
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
| `main.py` `run_pipeline()` | New pipeline: InstantonSolution → CompactionFunction → PBHMassFunction |

**Components that can be used with zero or minimal modification:**
- `RayWorkPool` (entire file)
- `ShardedPool` (entire file)
- `Datastore` (entire file, just update `_factories`)
- `SerialPoolBroker`, `ClientPool`, `ProfileAgent`
- `IntegrationSupervisor` base class and `RHS_timer`
- `IntegrationSolver`, `IntegrationData`
- `DatastoreObject` base class
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
- `Datastore/SQL/ObjectFactories/ScalarModel.py` → factory for `InstantonSolution`
- `Datastore/SQL/ObjectFactories/AdiabaticHistory.py` → factory for `CompactionFunction`
- `Datastore/SQL/ObjectFactories/BBNData.py` → factory for `PBHMassFunction`
- `CosmologyConcepts/Potentials/` → inflationary potential hierarchy
- `CosmologyConcepts/ConformalCouplings/` → may not be needed, or replaced with inflationary model parameters
- `CosmologyModels/` → inflationary background (de Sitter, quasi-de Sitter)
- `config/sharding.py` → updated shard key and table lists
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

**Step 2 — Factory** (`Datastore/SQL/ObjectFactories/InstantonSolution.py`):

Implement `SQLAFactoryBase` with:
- `register()` returning the SQLAlchemy column schema
- `build_object()` to SELECT / INSERT
- `store_object()` to serialize computed results
- `validate_object()` to verify integrity

**Step 3 — Register** in `Datastore/SQL/Datastore.py`:

```python
_factories["InstantonSolution"] = sqla_InstantonSolution_factory()
_factories["InstantonSolution_tags"] = sqla_InstantonSolutionTagAssociation_factory()
_factories["InstantonSolutionValue"] = sqla_InstantonSolutionValue_factory()
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

1. **Shard key must be in the database before sharded objects.** A `beta_value` (or equivalent shard key) must be inserted into the replicated tables and assigned to a shard before any `ScalarModel` (or equivalent) referencing that key can be stored. `ShardedPool._assign_shard_keys()` handles this automatically when `object_get` is called on the shard-key type.

2. **Replicated table `store_id`s must be identical across shards.** The `SerialPoolBroker` coordinates this. Never insert a replicated-table object directly into a shard — always go through `ShardedPool.object_get()`.

3. **`_do_not_populate` is advisory.** If the object already exists in the database (from a previous run), `object_get` returns it regardless of this flag. The flag only suppresses the INSERT when the object is absent.

4. **`compute()` must return an `ObjectRef`.** `RayWorkPool` expects a Ray future from `compute_handler`. If the computation is CPU-bound and cannot be a Ray task (rare), wrap it in `ray.remote`.

5. **`store()` is called on the driver side, not in Ray.** After `ray.get(compute_ref)` completes, `RayWorkPool` calls `obj.store()` on the Python object in the driver process. This should copy data from the compute result into the object's fields. Then `pool.object_store(obj)` is called to serialize to the database.

6. **Shard count is fixed at database creation time.** The number of shards cannot be changed for an existing database. Plan the shard count to match the expected parallelism and data volume.

7. **Ray actor naming.** Each shard `Datastore` is named `"shard{N:04d}-store"` and the `SerialPoolBroker` is named `"SerialPoolBroker"`. If reusing actor names across sessions (e.g., when `ray.init` reuses a cluster), ensure actors from previous sessions have been cleaned up.

8. **`store_results=True` in `RayWorkPool` is only for query-only passes.** When `task_builder` returns lists of refs (the vectorized case), the indices break. Use `store_results=True` only with scalar-returning `task_builder`s.

9. **The `grouper` + bin-by-shard pattern.** The main pipeline groups work items into batches of 8, then within each batch bins by `shard_key`. This minimizes round-trips by sending multiple items to the same shard in one call. For best efficiency in a new project, maintain this pattern.
