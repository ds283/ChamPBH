# ChamPBH Architecture Summary

**Purpose of this document:** Describe the architecture of the ChamPBH codebase at sufficient depth that a new project — targeting the computation of stochastic instantons in a scalar field inflationary model, and subsequent derivation of compaction functions and PBH mass functions — can be designed to reuse its infrastructure components.

---

## 1. Overview

ChamPBH computes the cosmological evolution of a chameleon scalar field (a dark-energy model with a screening mechanism) through the radiation-dominated epoch, then predicts observational consequences for Big Bang Nucleosynthesis (BBN). It is designed for **parameter-space surveys**: the user specifies grids of potential parameters (M, Λ), coupling strengths (β), and integration tolerances, and the system evaluates every combination, caching results persistently.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10).** The grids are of (M, n, β), not of
> (M, Λ) and β. The model is one `ExponentialModel` per (M, n, β) (§8.4a; `main.py:1082`), with n from
> `--n-values` (default 1), and Λ is not a parameter: the normalization of the potential, which Λ was,
> is found for each history by tuning it so that H_J(T_CMB) = H0 (`numerical-strategies.md` §3.7.4)
> and is stored on the history's row (§7.2's note of 2026-10-26). The campaign's account is
> [`prompts/jordan-normalization/README.md`](../prompts/jordan-normalization/README.md) §0.1.

The codebase has three major layers, each cleanly separable:

1. **Infrastructure** — Ray-based distributed task execution and sharded SQLite persistence.
2. **Physics integration** — ODE supervisor framework for driving `scipy.integrate.solve_ivp`.
3. **Compute targets** — Domain-specific classes (ScalarHistory, AdiabaticHistory, BBNData) that live at the intersection of infrastructure and physics.

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
├── plot_ScalarHistory.py            # Scalar field detailed plots
├── exponential.yaml                 # Example run configuration
├── runregistrykit.toml              # RunRegistryKit's config: runs_root, stores_root, recorded distributions, the fingerprinter
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
│   ├── ExponentialCoupling.py
│   ├── QCD_Cosmology.py
│   ├── ScalarHistory.py             # Factory for main ODE output
│   ├── AdiabaticHistory.py
│   ├── BBNData.py
│   └── tests/                       # unittest modules of the factories, the registry and the harness
│
├── StoreFixtures/
│   └── standin_pool.py              # the package's stand-in cluster, a copy; any test root imports it
│
├── ComputeTargets/
│   ├── ScalarHistory.py             # ODE solver (Ray remote class)
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
│   ├── Models/                      # the model interface: one object holding V and A (§8.4a)
│   │   ├── AbstractModel.py         # AbstractModel, RealizedModel, Seed
│   │   ├── ExponentialModel.py
│   │   └── model_ids.py
│   ├── Potentials/
│   │   ├── AbstractPotential.py
│   │   ├── ExponentialPotential.py
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
├── RunDrivers/                      # The launchers of registered runs
│   ├── __init__.py
│   ├── registered_run.py            # Runs main.py as a registered run (RunRegistryKit); main.py itself is unchanged
│   └── tests/                       # unittest modules of the wrapper; none needs Ray
│
├── tools/
│   ├── surfing_scan.py              # One CSV row per beta: the T_J turning points of a surfing history (no Ray, no store)
│   ├── plot_surfing_temperature.py  # The two-panel surfing-temperature figure, from that CSV and its sidecar
│   ├── plot_surfing_histories.py    # T_J, phi and 1 + beta pi/M_P to the first bounce, for up to six beta (no Ray, no store)
│   └── ...
│
├── config/
│   ├── argument_parser.py           # configargparse CLI + YAML
│   ├── datastore.py                 # The registry of factories, drop groups, batch sizes
│   ├── defaults.py                  # Tolerance/precision constants
│   ├── fingerprint.py               # The store fingerprint's format number, its spec pins, and the fingerprinter RunRegistryKit loads
│   ├── model_list.py                # Cosmological model registry
│   └── sharding.py                  # Shard-key type and table lists
│
└── var/                             # Ignored by git (.gitignore); what registered runs write, never committed
    ├── runs/                        # One directory per registered run: manifest.json, status.json, stdout.log, stderr.log
    └── datastores/                  # The stores that registered builds fill, each with its sidecar <stem>.manifest.json
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
>   π = 0, the constant cosmology `QCD_Cosmology`, and `--T-stop-GeV` or, when it is not given,
>   the cosmology's `T_CMB`) the build did not store stops the run: the script prints
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

> **Note, 2026-10-14 (`surfing-temperature`, prompts 01 to 04b).** The tree above gains a `tools/`
> block that lists only the two files this campaign added; the directory holds more
> (`history_and_bbn.py` and `bbn_baseline.py`, named in the first note above, three other tools, and
> `tests/`). Nothing here is stored, and no production code path calls any of it.
> - **`find_kicks(cosmology, T_lo, T_hi, n_grid)`** (`extract_common.py:649`) locates the peaks of
>   Σ = 1 − 3w, and the troughs between them, on the spline that `cosmology.w(T)` reads, which is the
>   integrator's own w. It labels the four kicks `EW`, `QCD`, `mu-pi` and `ee`, hot to cold.
>   **`reachability_roots(cosmology, beta, kicks, n_grid)`** (`:712`) gives, per kick, the hot and
>   cold roots of Σ_eff = 1/(3β²) with Σ_eff = Σ/(1 + Σ/2), their slopes d ln Σ/d ln T, and the
>   merged intervals (QCD and `mu-pi` merge above β = 2.154).
> - **`surfing_turning_points(result, coupling)`** (`ComputeTargets/ScalarModel.py:878`, returning
>   `SurfingCrossing`, `:866`) walks the accepted steps' interpolants up to and including the first
>   bounce, and returns every turning point of T_J: where 1 + (d ln Ω/dφ) π = 0. It sits beside
>   `first_bounce` and is **not called by `compute_scalar_model`**, so production computes and stores
>   none of it.
> - **`tools/surfing_scan.py`** integrates one history per β through `compute_scalar_model._function`
>   (no PRyMordial), writes one row per β to a resumable CSV with a sidecar `.meta.json`, and
>   **`tools/plot_surfing_temperature.py`** draws the figure from that CSV and sidecar. **Neither
>   starts Ray or opens a store.** The scan uses the fixed Λ of `tools/history_and_bbn.py`
>   (`LAMBDA_EV`, 1 meV) and has no option for it (board `[05-surfing-scan-runs-at-a-fixed-lambda]`).
>   Results: [`surfing-temperature-verification.md`](surfing-temperature-verification.md); the
>   handover, with the commands that re-run it, is §4.14 of
>   [`review-remediation-verification.md`](review-remediation-verification.md).

> **Note, 2026-10-14 (standalone, after `surfing-temperature` closed).** The `tools/` block also
> lists `tools/plot_surfing_histories.py`. It integrates up to six histories through
> `tools/surfing_scan.py`'s `run_history`, samples each on the dense output from the start to its
> first bounce, and draws T_J, φ and 1 + β π/M_P against N, with the turning points of T_J and the
> first bounce marked. Like the scan, it starts no Ray, opens no store and runs no PRyMordial.
> Its default β are the edges of, and four points inside, the uncaptured window near β = 3.0
> ([`surfing-temperature-verification.md`](surfing-temperature-verification.md) §3).

> **Note, 2026-10-20 (`rrk-adoption`, prompts 01 to 04).** The tree above gains `runregistrykit.toml`,
> `RunDrivers/`, `config/fingerprint.py` and `var/`. The campaign adopted RunRegistryKit `v0.2.0`
> (`runregistrykit[stores]`, pinned in `requirements.txt`, never an editable install) and changed
> nothing a build computes or stores. `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` and
> `utilities.py` are byte-identical to `3cd2be4`, and none of them imports the package.
> - **What the registry records, and where.** A registered run gets a directory,
>   `var/runs/<campaign>-<prompt>-<slug>-<YYYYMMDDTHHMMSS>/`, holding `manifest.json`, written before
>   the work starts (the purpose, the arguments, `script` and its SHA-256, `git_head` and `git_dirty`,
>   the installed `packages` at their commits, the host, `scope`, `heartbeat_means`), and
>   `status.json` (the state, the pid, the heartbeat, the stage, the exit code and, at the end, the
>   store's fingerprint), with the run's `stdout.log` and `stderr.log`. `runregistrykit.toml` names
>   the two roots, `var/runs` and `var/datastores`, and the distributions every manifest records
>   (`datastorekit` and `raypoolkit`, beside `runregistrykit`). `var/` is ignored by git. `python -m
>   runregistrykit list` lists the runs, and the registry records only: it does not schedule,
>   supervise, restart or lock.
> - **How a run is registered.** A registered run is launched through `RunDrivers/registered_run.py`,
>   with `--campaign`, `--prompt`, `--register SLUG` and `--purpose`, and `main.py`'s own arguments
>   after a bare `--`. The wrapper parses those arguments with `main.py`'s parser, calls
>   `runregistrykit.begin` before Ray starts, runs `main.py` unchanged by `runpy.run_path`, and
>   records `done`, `failed` or `killed` when `main.py` has returned. It refuses, exit 2 and
>   before `begin`, a run with no database, an `--inventory` run, and an address `main.py` would
>   refuse. `begin` also refuses a store that an unfinished run names. `main.py` stays launchable
>   without the wrapper, and a run launched that way is recorded nowhere.
> - **What the heartbeat means.** The wrapper refreshes it when the run writes a line to stdout, at
>   most once in 30 s, or at once when the line changes the stage. The work is in Ray actors, so a
>   beat from the driver cannot say that any actor is progressing. `RayWorkPool` prints a progress
>   line only after completed work, and `main.py` sets its notification interval to 300 s, so a fresh
>   heartbeat means that work completed within about five minutes. A stage that prints nothing for
>   longer than the staleness window (900 s) is reported `stale` while it works: stale means go and
>   look. The manifest's `heartbeat_means` says this in the run's own record.
> - **The fingerprint, and its guard.** `config/fingerprint.py` holds `FINGERPRINT_FORMAT` (1, the
>   first of a history), `SPEC_DIGESTS` (the SHA-256 of each class's `inventory_spec()`, pinned for each
>   format) and `fingerprinter`, a `runregistrykit.fingerprint.DatastoreKitFingerprinter` over
>   `config.datastore.factories`, which `runregistrykit.toml` names under `[stores]`. What goes into a
>   fingerprint is ChamPBH's (the specs, the registry and the format number); how it is encoded and
>   digested is DatastoreKit's, and where it is recorded is RunRegistryKit's. A change to any
>   `inventory_spec()` without a bump of the format and a new pin fails
>   `ObjectFactories/tests/test_fingerprint_format_guard.py`, and
>   `ObjectFactories/tests/test_store_fingerprint_golden.py` checks the fingerprint of a store built
>   in a temporary directory against a golden file, which a format change adds to and never
>   overwrites.
> - **The store's sidecar is made by a person.** A registered run takes the store's fingerprint
>   after `main.py` has returned and writes it into the store's sidecar only if there is one. Nothing
>   in the pipeline makes the sidecar: `python -m runregistrykit store create STORE --purpose TEXT`
>   makes it, by a person, once the store's first build has closed, because the package refuses a
>   sidecar for a primary that does not exist yet, and the purpose is one only a person can give.
>   Until then a run's `status.json` says `fingerprint_sidecar` "not written"; from then on, `written`.
> - **The measurements, and the handover.** The rehearsal of a registered, an interrupted and a
>   hard-killed build is in [`rrk-adoption-verification.md`](rrk-adoption-verification.md); the
>   handover for the run scripts is §4.15 of
>   [`review-remediation-verification.md`](review-remediation-verification.md).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 07).** The tree above loses
> `CosmologyConcepts/Potentials/`, `CosmologyConcepts/ConformalCouplings/` and
> `CosmologyConcepts/Lambda_value.py`, and `ObjectFactories/ExponentialPotential.py` and
> `ObjectFactories/ExponentialCoupling.py`; it gains `ObjectFactories/ExponentialModel.py`, the
> factory of the model's table. The model (`CosmologyConcepts/Models/`, §8.4a) is one stored object
> keyed by (M, n, β); the normalization of the potential and φ_ref are each history's, found by
> tuning and stored on its row (§7.2's note of the same date). The text before this note describes
> the tree as it was.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 03, 07 and 08): the tools,
> the plot inputs and `histories.csv`.** What the notes above say of them, as the tree has them now
> (read at `dbfa31f`).
> - **`tools/history_and_bbn.py BETA M`** tunes the history by default (`tune_history`,
>   `ComputeTargets/ScalarHistory.py:1575`), prints a `tuning` line holding the fifteen stored outputs,
>   and runs the BBN solve. Its options are `--phi-init-Mp`, `--tuning-tolerance` (1e-5),
>   `--tuning-max-integrations` (30), `--fixed-normalization NORM PHI_REF_MP` (one untuned history,
>   by `integrate_history`, `:1137`), `--T-stop-GeV` (accepted only with `--fixed-normalization`),
>   `--small-network` and `--wall-clock-limit` (`tools/history_and_bbn.py:280-330`). The first note
>   above lists `--T-stop-GeV T` as a free option of the tool, and its history as untuned.
> - **`tools/surfing_scan.py` and `tools/plot_surfing_histories.py`** tune each β to T_CMB (`--tuning-tolerance`,
>   `--tuning-max-integrations`) and analyse a window that ends at the history's first crossing of
>   T_J = `--T-window-low-GeV` (default 1e-3 GeV). The scan's CSV holds the tuned normalization, V_ref,
>   φ_ref and both residuals beside the roots, and its inputs that a resume must match
>   (`_INPUT_MATCH`, `tools/surfing_scan.py:204`) are φ*, M, the tuning tolerance and the window's
>   edge. A CSV whose header holds `Lambda_eV` or `T_stop_GeV` is refused on resume, and `--T-stop-GeV`
>   is refused by name. So "the scan uses the fixed Λ of `tools/history_and_bbn.py`", in the
>   `surfing-temperature` note above, is of the tree before prompt 08; the board issue
>   `[05-surfing-scan-runs-at-a-fixed-lambda]` is closed.
> - **`plot_inputs.py`** (the read-only-plots note above) has `parameters(pool, abs_tol, rel_tol,
>   T_init_GeV, phi_init_Mp, units, tuning_tol)`, which takes the tuning tolerance and no `T_stop`, and
>   `exponential_models(pool, M_table, beta_table, n_values, units)` (`plot_inputs.py:183`) in place of
>   `potentials` and `couplings`. A model that was never stored is skipped with one line (`!! plot: no
>   ExponentialModel stored for M=…, n=…, β=…; no history can use it; skipped`) and a count; an n with no
>   stored model at any stored (M, β) raises `PlotOptionNotStored` naming `--n-values`.
> - **`extract_common.py`'s `histories.csv`** has the columns `n`, `normalization`, `V_ref_eV4`,
>   `phi_ref_Mp`, `phi_0_Mp`, `tracking`, `residual_H`, `residual_phi` and `tuning_iterations` where it
>   had `Lambda_eV` (`CSV_COLUMNS`, `extract_common.py:520`), and its rows are sorted by (M, n, β)
>   (`:1166`).

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

The layer is [`datastorekit`](https://github.com/ds283/DatastoreKit), pinned to the tag `v0.4.0` in
`requirements.txt` (never an editable install). Its specification for a client is
`docs/client-contract.md` at that tag
([§1](https://github.com/ds283/DatastoreKit/blob/v0.4.0/docs/client-contract.md) the pool's
constructor, §2 the keys of `register()`, §3 the factory hooks, §4 the inventory's declarations,
§5 the layer's own tables, §6 what the layer reads from a stored object, §7 the other entry
points, §8 version-keyed lookups, §9 the changes after `v0.2.0`). Where this section and the
contract differ, the contract is right.

The pin moved from `v0.3.0` to `v0.4.0` on 2026-10-14. The release adds one module,
`datastorekit.store_fingerprint` (contract §9.7), which takes, compares and lists a store's
fingerprint from what `read_inventory` returns; the layer never calls it and nothing it writes or
reads changed, so ChamPBH's stores and code are unaffected. ChamPBH uses the module through
RunRegistryKit's store layer: `config/fingerprint.py`'s `fingerprinter` is a
`runregistrykit.fingerprint.DatastoreKitFingerprinter`, whose methods import `fingerprint_of`,
`compare_fingerprints` and `listing_lines` from `datastorekit.store_fingerprint`. ChamPBH's own code
imports the module nowhere; one test, `ObjectFactories/tests/test_store_fingerprint_golden.py`,
imports its `ENCODING_FORMAT` to check the golden file against it.

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

The three drivers that open a pool (`main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`) each call
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
| `ExponentialPotential` | `sqla_ExponentialPotential_factory` | Replicated |
| `ExponentialCoupling` | `sqla_ExponentialCoupling_factory` | Sharded |
| `QCD_Cosmology` | `sqla_QCDCosmology_factory` | Replicated |
| `IntegrationSolver` | `sqla_IntegrationSolver_factory` | Replicated |
| `ScalarHistory` | `sqla_ScalarHistoryFactory` | Sharded |
| `ScalarHistory_tags` | `sqla_ScalarHistoryTagAssociation_factory` | with its owner (not listed in `config/sharding.py`) |
| `ScalarHistoryValue` | `sqla_ScalarHistoryValue_factory` | Sharded |
| `AdiabaticHistory` | `sqla_AdiabaticHistoryFactory` | Sharded |
| `AdiabaticHistory_tags` | `sqla_AdiabaticHistoryTagAssociation_factory` | with its owner (not listed) |
| `AdiabaticHistoryValue` | `sqla_AdiabaticHistoryValue_factory` | Sharded |
| `BBNData` | `sqla_BBNDataFactory` | Sharded |
| `BBNData_tags` | `sqla_BBNDataTagAssociation_factory` | with its owner (not listed) |
| `BBNDataValue` | `sqla_BBNDataValue_factory` | Sharded |

`LambdaCDM`, which `config/sharding.py`'s `replicated_tables` and the batch sizes listed though no
factory registered it, is in neither since `dk-and-rpk-migration` P8 (prompt 08).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 07).** The registry has 21 entries: `Lambda_value`,
> `ExponentialPotential` and `ExponentialCoupling` are gone, and `ExponentialModel`
> (`sqla_ExponentialModel_factory`, sharded by β, batch size 50) is in `ExponentialPotential`'s place.
> `FINGERPRINT_FORMAT` is 5 (15 spec pairs, 13 distinct digests), with the golden file
> `store_fingerprint_jordan-normalization-07.json`. `VERSION_LABEL` is `2026.7.0`: a store made before
> cannot be reused. The table above describes the registry as it was.

### 4.3 What the package does for a get, a store and a validate

The pool's `object_get` and `object_store` dispatch by the class's name; the replicated/sharded
rule, the replication of a replicated write, the check at open, the pruning of unvalidated rows
and the refusals are the package's, and are stated in contract §1 (the arguments that set them),
§3 (the hooks it calls) and §6 (what it reads from an object). The points a ChamPBH reader needs:

- **Replicated tables** (parameter types, potentials, cosmology, metadata) are identical on every
  shard, with the same `store_id`. A replicated write goes to a controlling shard and is then
  replicated to the others under the controller's serials. A replicated row carries **one
  timestamp**, the same on every shard.
- **Sharded tables** (compute results: `ScalarHistory`, `AdiabaticHistory`, `BBNData` and their
  value tables, and `ExponentialCoupling`) live only on the shard of their shard key. The shard
  is assigned, load-balanced, when a `beta_value` is first inserted; a lookup table in the
  primary maps `beta_value.store_id` to the shard.
- **Serials** are leased from the one `SerialPoolBroker`, so serial numbers never collide between
  shards, which replicated tables need, since their `store_id`s must agree on every shard.
- **`object_get(class, **kwargs)`** finds or creates an object through the factory's `build`;
  **`object_store(obj)`** writes a computed object through its factory's `store` and returns it
  with its `store_id`; **`object_validate(obj)`** runs its `validate`. `store`, `validate` and
  `validate_on_startup` are defaulted since `v0.2.3` (U3): a factory that does not define one
  raises `NotImplementedError` if the layer calls it. ChamPBH's `ScalarHistory`, `AdiabaticHistory`
  and `BBNData` factories define all three, and the others none.
- **`object_get_vectorized(class, shard_key, payload_data)`** is for sharded tables only. It takes the
  shard key **in mapping form**, `{"shard_key": key}` (the sharded table's field name mapped to the
  key), and sends one shard actor the list of payloads in one call.
- **At open**, the pool checks that every shard's replicated tables agree, repairs or refuses what
  an interrupted run left, and, under `prune_unvalidated`, deletes the unvalidated rows of the
  classes whose factory validates at startup (contract §1, rows 5 and 11; §2, `validate_on_startup`).

**The `_do_not_populate` payload key.** It is ChamPBH's convention, not the package's: the
`ScalarHistory` and `BBNData` factories' `build` read it from the payload, and when it is set they do
not INSERT a skeleton row. This allows a fast "does this exist?" query without side effects, and
is used in the two-pass pattern: first check which items exist, then enqueue only the missing ones
for computation.

**Version-keyed lookups (added 2026-09-30, `run-integrity` prompt 01; since `dk-and-rpk-migration`
prompt 08 the keying is the package's, contract §8).** Every table registered with
`"version": True` records, in its `version` column, the serial of the label the store was opened
under (stamped by the actor's insert). The lookups of the three compute targets read it:

- **Which tables are keyed.** `ScalarHistory`, `AdiabaticHistory` and `BBNData`, and only those. Each factory's `register()` declares
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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 07).** Three statements of §4.3
> are of the tree before the model's table. The replicated list names "potentials" and the sharded
> list `ExponentialCoupling` (the first bullets of §4.3): the table that holds the Lagrangian is
> `ExponentialModel`, keyed by (M, n, β) and sharded by β (`config/sharding.py:33`; the registry
> note in §4.2). The parameter tables of the third bullet above no longer include Λ, a coupling or
> a potential; `ExponentialModel` is one of them, with one row per (M, n, β) across labels. And a
> `ScalarHistory` lookup is keyed by the version, the cosmology, the model, φ*, π*, T_init, atol,
> rtol and the tuning tolerance (§7.2's note of the same date).

A store opened under a new label therefore sees none of its old compute targets; they are
recomputed and stored beside the old rows. `--inventory` still lists every version. The label is
defined once, as `VERSION_LABEL` in `config/version.py`, and `main.py`, `plot_by_beta.py` and
`plot_ScalarHistory.py` import it.

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
- `--drop {scalar-history,adiabatic-history,bbn-data}` (`main.py`'s: the plot scripts refuse it, exit 2, before Ray starts)

*Integration:*
- `--abs-tol FLOAT` (default 1e-8)
- `--rel-tol FLOAT` (default 1e-8)
- `--T-init-GeV FLOAT` (default 20000)
- `--T-stop-GeV FLOAT`

*Parameter grids:*
- `--potential-type {Exponential}`
- `--beta-low/high FLOAT`, `--samples-per-beta N`, `--beta-values FLOAT...`
- `--log10-M-low/high-eV FLOAT`, `--samples-per-log10-M-eV N`, `--M-values-eV/Mp FLOAT...`
- `--log10-Lambda-low/high-eV FLOAT`, `--Lambda-values-eV/Mp FLOAT...`
- `--samples-log10-z N` (default 250)
- `--log10-one-plus-z-high/low FLOAT`

*Output:*
- `--output DIR`
- `--inventory` (show DB contents and exit)
- `--ray-address STR` (no default; required whenever Ray starts, `auto` is refused, `main.py --inventory` needs none; `ray_address_problem()` in `config/argument_parser.py` is called before each driver's `ray.init`)

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 03, 07 and 08): the options as
> `config/argument_parser.py` has them now.**
> - **The axis.** `--samples-log10-z N` (default 250, `:26`) is the number of grid points per decade
>   of 1 + z, and the grid is exactly the multiples k ln 10/N of ln(1 + z) (`log_one_plus_z_grid`,
>   `CosmologyConcepts/redshift.py:24`; the option's help still says "per log10(z)").
>   `--log10-one-plus-z-high` defaults to 300 (`:24`; 35 before prompt 03, board §5.4), and
>   `exponential.yaml` no longer sets it. `--log10-one-plus-z-low` accepts only 0 (`:112`): the axis
>   is anchored at the end of the history.
> - **The tuning.** `--tuning-tolerance` (default 1e-5; a key of every stored history),
>   `--tuning-max-integrations` (default 30; the cap on one history's tuning, the seed's integration
>   included, and **not** a key: a history that fails to converge within it is stored as a failure
>   and a later run with a larger cap does not recompute it, board issue
>   `[08-a-history-that-did-not-converge-is-never-retried]`) and `--n-values` (default `[1]`; the
>   exponent n of V = V₀ exp((M/φ)ⁿ), part of the model's key) (`:185`, `:194`, `:298`).
> - **Removed.** `--potential-type` (prompt 08), `--T-stop-GeV`, `--log10-Lambda-low-eV`,
>   `--log10-Lambda-high-eV`, `--samples-per-log10-Lambda-eV`, `--Lambda-values-eV` and
>   `--Lambda-values-Mp` (prompt 07) are registered hidden with `RemovedOption`
>   (`config/argument_parser.py:65`), whose message names the replacement: giving one, on the command
>   line or in a `--config` YAML file, ends the parse with exit 2. The list above, and the
>   `science-readiness` note below on `--T-stop-GeV` and the BBN window, describe options that are
>   gone: every stored history ends at T_CMB (§7.2's note of 2026-10-26), so the pre-check of
>   `compute_BBN_data` passes (`numerical-strategies.md` §7.6.1's note).

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
    "ExponentialPotential",
    "QCD_Cosmology",
    "IntegrationSolver"
]

sharded_tables = {
    "ExponentialCoupling": "shard_key",
    "ScalarHistory": "shard_key",
    "ScalarHistoryValue": "shard_key",
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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 07).** `--log10-Lambda-low-eV`,
> `--log10-Lambda-high-eV`, `--samples-per-log10-Lambda-eV`, `--Lambda-values-eV`,
> `--Lambda-values-Mp` and `--T-stop-GeV` left the shared parser, so `main.py` and both plot scripts
> refuse them (exit 2): nothing can consume them, since a history's normalization is tuned and every
> stored history ends at T_CMB. `exponential.yaml` lost its `Lambda-values-eV` line. In
> `config/sharding.py`, `ExponentialModel` is sharded by `shard_key` (its β row) where
> `ExponentialCoupling` was, and `Lambda_value` and `ExponentialPotential` left `replicated_tables`
> (and `Lambda_value` `read_table_config`).

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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 03).** `DEFAULT_REDSHIFT_PRECISION` is
> not in `config/defaults.py`; prompt 03 deleted it. The redshift lookup and `redshift_array`'s
> truncation compare ln(1 + z) by absolute difference below `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10`
> (`config/defaults.py:23`), far below the grid spacing ln 10/250 = 9.2 × 10⁻³ (§8.2's note of the
> same date).

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

### 7.2 `ScalarHistory` (`ComputeTargets/ScalarHistory.py`)

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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 03).** `SampleValues` has 18 fields
> (`ComputeTargets/ScalarHistory.py:188-210`). The last, `log_one_plus_z_Jordan`, is ln(1 + z_J), the
> Jordan-frame redshift counted back from the end of the history, computed at sampling as
> ln(1 + z_E) + ln A(φ₀) − ln A(φ) with φ₀ the field at the end (`:1327`). It is a column of
> `ScalarHistoryValue` (`:2327`, `:2459`) and never a key: H_J < 0 during a bounce, so a_J is not monotonic.
> `raw_N` is the sample's e-fold number, N_forward = final_N − N_backward (`:1296-1297`), and the
> sample's label is its point on the axis ln(1 + z_E), not z (§8.2's note of the same date;
> `numerical-strategies.md` §3.7.9).

**Integration procedure in `ScalarHistory.compute()`:**

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

> **Note, 2026-10-14 (`surfing-temperature`).** `surfing_turning_points` walks the same step
> interpolants as `first_bounce`, from the start to the bounce, and returns every turning point of
> T_J. The reason T_J turns where β π/M_P = −1 is the temperature law (`ScalarModel.py:441`),
> d ln T_J/dN = −(1 + (d ln Ω/dφ) π)/(1 + (d g_s/d ln T)/(3 g_s)), whose denominator is positive, so
> T_J is stationary exactly where 1 + (d ln Ω/dφ) π = 0, and for the exponential coupling
> d ln Ω/dφ = β/M_P. Production does not call it; nothing is stored.

**`ScalarHistoryProxy`:** A lightweight reference object (just holds the model's `store_id` and key parameters, not the full solution array). Passed to downstream compute targets (AdiabaticHistory, BBNData) so they can fetch the full `ScalarHistory` from the database only when needed.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 07): the key, the outputs and the compute path.**
> - **The key.** A `ScalarHistory` row is keyed by the version, the cosmology (`cosmology_type`,
>   `cosmology_serial`), the model (`model_type`, `model_serial`: a polymorphic reference with no
>   foreign key, `model_type` the model's `type_id`, `EXPONENTIAL_MODEL = 1` →
>   `ExponentialModel`), φ*, π*, T_init, atol, rtol and the tuning tolerance (`tuning_tol_serial`, a
>   `tolerance` row), with `validated == True`. `potential_*`, `coupling_*` and
>   `T_Jordan_stop_serial` are gone: every stored history ends at the cosmology's T_CMB.
>   `ScalarHistory.T_Jordan_stop` remains as a property only (a `temperature` with store id 0 at
>   T_CMB), because `compute_BBN_data`'s pre-check reads it.
> - **The outputs.** Fifteen nullable columns hold `TUNING_OUTPUT_KEYS`, NULL on a failure row:
>   `normalization` and `seed_normalization` unconverted (ln V₀ for `ExponentialModel`, in the units
>   of the run, Planck units for every driver), `V_ref` in M_P⁴, `phi_ref`, `phi_0`, `pi_0` and
>   `seed_phi_ref` in M_P, and `tracking`, `residual_H`, `residual_phi`, the four counts and
>   `seed_kind`. The object exposes them as `tuning` (a `TuningValues` namedtuple, readable with
>   `_do_not_populate`), and `realized_model` is `model.realize(normalization, phi_ref)`. `model` and
>   `tuning_tol` replace `potential` and `coupling`; `shard_key` is the model's β.
> - **The compute path.** `compute_scalar_history` (the `@ray.remote` that `ScalarHistory.compute`
>   submits) calls `tune_history`, so every stored history is tuned to H0 and φ_min at T_CMB.
>   `integrate_history` is the untuned primitive, called by the golden histories' setup, the tests
>   and the tools that run their own histories (`tools/history_and_bbn.py`,
>   `tools/surfing_scan.py`, which keep the fixed normalization 4 ln(1 meV) until prompt 08).
>   `AdiabaticHistory` and `BBNData` hold their history's `model`; the adiabatic computation reads
>   the history's tuned `realized_model`. Their inventory specs call the parent ScalarHistory
>   `history`.
>
> The text before this note describes `ScalarHistory` as it was.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 04, 06 and 08): the two layers and
> what the tools now call.** Read at `dbfa31f`; it corrects the notes above.
> - **The layers.** `integrate_history` (`ComputeTargets/ScalarHistory.py:1137`) is the primitive: one
>   integration of one `RealizedModel` to any T_stop, with no normalization logic; it is the body
>   that `compute_scalar_history` had. `tune_history` (`:1575`) calls it, to T_J = T_CMB
>   (`:1628`), adjusting the model's normalization and φ_ref until H_J(T_CMB) = H0 and
>   φ_ref = φ_min(T_CMB). `compute_scalar_history` (`:1748`), the `@ray.remote`, calls `tune_history`.
>   The conditions, the update, the guards, the failures and the outputs are in
>   `numerical-strategies.md` §3.7.
> - **Who calls the primitive.** The golden histories' setup, the tests, `tune_history`, and
>   `tools/history_and_bbn.py --fixed-normalization`. The tools that run their own histories no
>   longer "keep the fixed normalization 4 ln(1 meV) until prompt 08", as the note above says:
>   `tools/history_and_bbn.py` tunes by default, and `tools/surfing_scan.py` and
>   `tools/plot_surfing_histories.py` tune each β to T_CMB (§2's note of the same date).
> - **The fifteen outputs and the progress line.** The columns the note above lists are the keys of
>   `TUNING_OUTPUT_KEYS` (`:1399`). Each integration of a tuning prints one `-- tune …` line
>   (`:1672-1677`), which Ray forwards to the driver and the wrapper's heartbeat reads; Ray's log
>   deduplication can hold lines back (board issue
>   `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`).
> - **The policies.** `ODEPolicy`, `HubblePolicy`, `PotentialDerivativePolicy`, `fixed_T_value_at`,
>   `fixed_T_values` and `surfing_turning_points` take one `RealizedModel` (§8.4a) in place of a
>   potential and a coupling, and `PotentialDerivativePolicy` has only the log-potential route.

### 7.3 `AdiabaticHistory` (`ComputeTargets/AdiabaticHistory.py`)

Computes the adiabatic perturbation parameter Q (which determines whether the chameleon field is adiabatically tracking its minimum). Requires a `ScalarHistoryProxy` as input.

### 7.4 `BBNData` (`ComputeTargets/BBNData.py`)

Interfaces with the external PRyMordial library to compute primordial nucleosynthesis abundances (Y_p, D/H, Li-7/H). Also requires a `ScalarHistoryProxy`.

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

> **Note, 2026-10-26 (jordan-normalization prompt 05):** the electronvolt is set from one constant
> in `Units/base.py`. `NEWTON_G_MEV_MINUS_2 = 6.70883e-45` is PRyMordial's `GN` (`PRyM/PRyM_init.py`,
> the PDG value), in MeV⁻² and written out there because `PRyM_init` reads files from the working
> directory at import; `REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8 π NEWTON_G_MEV_MINUS_2)`
> (2.4353236 × 10²⁷) is the reduced Planck mass in eV. `Planck_units.eV` and `Mpc_units.eV` are
> `PlanckMass / REDUCED_PLANCK_MASS_EV`, and `GeV_units.PlanckMass` is
> `REDUCED_PLANCK_MASS_EV * eV`. Each system's Planck length, mass, time and temperature literals
> are CODATA 2018's and are unchanged. Before this note the three systems used 2.436 × 10²⁷, which
> is 2.78 × 10⁻⁴ above this, so that 1 K, 1/eV and 1 kg disagreed with k_B, ħc, ħ and the kilogram
> in eV by that much. PRyMordial's `Mpl = 1/√G_N` is √(8π) times `REDUCED_PLANCK_MASS_EV`.

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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 03 and 07).** `Lambda_value` is
> gone (prompt 07 deleted the class, its factory and its table): the potential's height is not a
> parameter but a normalization that each history finds (`numerical-strategies.md` §3.7.1, §3.7.4).
> `redshift` holds the label ln(1 + z_E): `redshift(store_id, log_one_plus_z)`. Its `.z` is derived by
> `expm1` and is `inf` past ln(1 + z) = 709.78, not an error (`CosmologyConcepts/redshift.py:74-82`);
> `float()` of one is ln(1 + z) (`:84`); its ordering, `redshift_array`'s truncation and the
> factory's lookup compare ln(1 + z), the lookup by one range filter of half-width 1e-10 on the
> indexed column (`ObjectFactories/redshift.py:51-53`). The axis is `log_one_plus_z_grid` (`:24`): the
> points k ln 10/S, anchored at the end of the history. The details and the measurements are in
> `numerical-strategies.md` §3.7.9.

### 8.3 Potentials (`CosmologyConcepts/Potentials/`)

`AbstractPotential` defines the interface:

```python
class AbstractPotential(DatastoreObject):
    # Required properties
    name: str
    type_id: int
    
    # Required methods
    def log_V(self, phi) -> float: ...
    def d_V_dphi(self, phi) -> float: ...
    def d2_V_dphi2(self, phi) -> float: ...
```

> **Note, 2026-10-01 (`integrator-remediation`, prompt 01, `fc97233`).** The five `bounce_region_level{1,2}_*` and `hard_reflection_point` properties are still defined on `AbstractPotential` and its potentials, but nothing reads them since `VERSION_LABEL = "2026.5.0"`: the regions are gone (see [`numerical-strategies.md` §3.5.2](numerical-strategies.md)). The same commit added two non-abstract properties, `reflects_at_origin -> bool` (default `False`) and `log_V_floor -> Optional[float]` (default `None`), implemented on `ExponentialPotential` only; they let the integrator's elastic reflection (§3.5.3 there) run for a potential that declares it. Removing the region properties is open issue `[00-region-properties-on-the-potentials-become-unread]`.

Concrete implementation: `ExponentialPotential`.

> **Note, 2026-10-20 (`jordan-normalization`, prompt 01).** `InversePowerPotential`, `StarobinskyPotential`, `ReclinerPotential` and `ReflectingPotential` were removed, with their factories, their ids in `model_ids.py`, their registry, sharding and batch-size entries, `ScalarModel`'s spec entries, `starobinsky.yaml` and `recliner.yaml` (user ruling R11: for now, strip everything down to the exponential). `ExponentialPotential` is the only potential class, `--potential-type` accepts only `Exponential` (the option itself goes in prompt 08), and `FINGERPRINT_FORMAT` is 2. The table and the lists above and in §4 and §6 describe the tree after this commit; the dated notes before it describe it as it was.

> **Note, 2026-10-21 (`jordan-normalization`, prompt 02).** `ScalarModel` was renamed `ScalarHistory`, so
> that "model" is free for the Lagrangian (README R9). The classes `ScalarHistory`,
> `ScalarHistoryValue` and `ScalarHistoryProxy`, the factories `sqla_ScalarHistory*`, the tables
> `ScalarHistory`, `ScalarHistory_tags` and `ScalarHistoryValue`, `compute_scalar_history`,
> `ComputeTargets/ScalarHistory.py`, `ObjectFactories/ScalarHistory.py` and `plot_ScalarHistory.py` carry
> the new names. The column that pointed at a history, `model_serial`, is now `history_serial` on the
> value and tag tables and `scalar_history_serial` on `AdiabaticHistory` and `BBNData`
> (`AdiabaticHistoryValue` and `AdiabaticHistory_tags` already used `history_serial` for their own
> `AdiabaticHistory` row). The drop group is `scalar-history`; `--drop scalar-model` is refused. The
> parent field `model` in the inventory specs, `model_proxy` and the lowercase `model` names are
> unchanged (board issue `[02-the-scalar-history-parent-is-still-called-model]`). Fingerprint format 3.
> Nothing computed changed (the golden history is bitwise). The notes above keep the names of their day.

> **Note, 2026-10-25 (`jordan-normalization`, prompt 04).** The integrator and everything downstream
> of it no longer read a potential: they read a `RealizedModel` (§8.4a), which gives V and the
> conformal factor together. The potential rows still exist, and still key a history, and reach the
> integrator through the adapter `ExponentialModel.from_rows(potential, coupling)` (normalization
> 4 ln Λ, φ_ref = 0, bitwise the rows' values) until prompt 07 stores the model. The five
> `bounce_region_level{1,2}_*` and `hard_reflection_point` properties, and `ExponentialPotential`'s
> `default_max_step`, are deleted, and the listing above no longer shows them; this closes
> `[00-region-properties-on-the-potentials-become-unread]`.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 07).**
> `CosmologyConcepts/Potentials/` is deleted: `AbstractPotential`, `ExponentialPotential`, their
> rows and their factory are gone, and "Concrete implementation: `ExponentialPotential`" above, and
> the adapter in the note just above, are of the tree before. The potential's part of the interface is
> `AbstractModel`'s `_log_V`, `_d_logV_dphi` and `_d2_logV_dphi2`, which take the normalization as an
> argument (§8.4a); `reflects_at_origin` and `log_V_floor` are the model's
> (`CosmologyConcepts/Models/AbstractModel.py:185-198`), and `ExponentialModel._log_V_floor` is the
> normalization ln V₀ (`ExponentialModel.py:77-79`), where the listing's floor was Λ⁴.

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

> **Note, 2026-10-25 (`jordan-normalization`, prompt 04).** The integrator now reads the conformal
> factor from the `RealizedModel` (§8.4a), with A(φ_ref) = 1; the coupling rows reach it through the
> adapter `ExponentialModel.from_rows` at φ_ref = 0, where A is exactly the coupling's Ω, until prompt
> 07 stores the model. `pipeline_selection`'s super-Planckian warning still reads the coupling rows
> (prompt 08 moves it).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 07 and 08).**
> `CosmologyConcepts/ConformalCouplings/` is deleted with `AbstractCoupling` and `ExponentialCoupling`,
> and "Currently only `ExponentialCoupling` is implemented: Ω(φ) = exp(βφ/Mp)", and the adapter in
> the note just above, are of the tree before. The conformal factor is the model's:
> A(φ) = exp(β(φ − φ_ref)/M_P), with `ExponentialModel._log_Omega` = βφ/M_P and
> `RealizedModel.log_Omega` subtracting its value at φ_ref (`AbstractModel.py:114-115`); d ln A/dφ = β/M_P is
> unchanged. The super-Planckian warning no longer reads coupling rows: `warn_super_planckian`
> (`pipeline_selection.py:341`) takes the models and evaluates ln A(φ*) at the φ_ref of each model's
> seed (§9's note of the same date).

### 8.4a Models (`CosmologyConcepts/Models/`)

*Added 2026-10-25 by `jordan-normalization` prompt 04.* A model is one Lagrangian: the potential
V(φ) and the conformal factor A(φ) of g_J = A² g_E (README R6). Two quantities belong to each history,
not to the model: the potential's **normalization**, which the model interprets (R7), and the
**reference point** φ_ref at which A(φ_ref) = 1.

```python
class AbstractModel(DatastoreObject):
    name: str
    type_id: int
    shard_key: ShardKeyType
    units: UnitsLike

    # hooks a model implements; none takes phi_ref
    def _log_V(self, normalization, phi) -> float: ...
    def _d_logV_dphi(self, normalization, phi) -> float: ...
    def _d2_logV_dphi2(self, normalization, phi) -> float: ...
    def _log_Omega(self, phi) -> float: ...
    def _d_logOmega_dphi(self, phi) -> float: ...
    def _d2_logOmega_dphi2(self, phi) -> float: ...
    def _log_V_floor(self, normalization) -> Optional[float]: ...   # default None
    reflects_at_origin: bool                                         # default False

    def realize(self, normalization, phi_ref) -> RealizedModel: ...
    def renormalize(self, normalization, phi, V_target): ...         # V(phi) = V_target
    def normalization_coordinate(self, normalization) -> float: ...
    def normalization_from_coordinate(self, x) -> normalization: ...
    def effective_minimum(self, normalization, phi_ref, phi_0, rho_m_E_0, bracket=None) -> float: ...
    def seed(self, cosmology) -> Seed: ...                           # Seed(kind, normalization, phi_ref)
```

- **`RealizedModel`** is what the integrator, `HubblePolicy`, `PotentialDerivativePolicy`,
  `fixed_T_value_at`, `surfing_turning_points` and `AdiabaticComputePolicy` take. It is immutable
  and exposes `log_V`, `d_logV_dphi`, `d2_logV_dphi2`, `log_Omega`, `Omega`, `d_logOmega_dphi`,
  `d2_logOmega_dphi2`, `log_V_floor`, `reflects_at_origin`, `name`, `normalization`, `phi_ref` and
  `model`. Ω is the conformal factor A; the names are the integrator's.
- **A(φ_ref) = 1 is the base class's.** `log_Omega(φ)` is `model._log_Omega(φ) − model._log_Omega(φ_ref)`,
  the latter computed once at realization, so no model interprets φ_ref.
- **`effective_minimum`** is the root of V′ + (d ln A/dφ) ρ_m,E(φ₀) A(φ)/A(φ₀), by `brentq` on
  (0.05 φ₀, 20 φ₀) by default; a bracket without a sign change raises `ValueError`. The radiation term
  Σ ρ_R,E is left out, since Σ(T_CMB) is 0.0 exactly.
- **`seed`** is the start of a history's tuning (P5). The base class gives `plain`:
  normalization ln(3 M_P² H0² (1 − Ω_m)), φ_ref = 0.
- **`ExponentialModel(store_id, M, n, beta, units)`**: V = V₀ exp((M/φ)ⁿ), A = exp(β(φ − φ_ref)/M_P).
  - The normalization is ln V₀, and its coordinate is itself.
  - `type_id` is `EXPONENTIAL_MODEL = 1`, and `shard_key` is the `beta_value`.
  - Its `seed` is the tracking analytic minimum, else the static one, else `plain`, for n = 1, and
    `plain` otherwise.
  - The classmethod `from_rows(potential, coupling)` is the adapter (prompt 07 removes it).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompts 07 and 08).** `from_rows`
> is deleted, with the rows it adapted. A model is a stored row of its own table:
> `ExponentialModel(store_id, M, n, beta, units)` read back by `object_get("ExponentialModel", …)`
> (`ObjectFactories/ExponentialModel.py`; `main.py:938-952`), and a history realizes it at the
> normalization and φ_ref that it stored (`ScalarHistory.realized_model`,
> `ComputeTargets/ScalarHistory.py:2014`). `ExponentialModel._log_V_floor(normalization)` is the
> normalization ln V₀ (`ExponentialModel.py:77`). Its `seed` (`:195`) tries the tracking seed and then
> the static one for n = 1, each a loop that may have no solution (`:156-193`), and otherwise gives
> the base class's plain seed (`AbstractModel.py:267`). The model has no closed-form
> `effective_minimum`: the generic `brentq` of `AbstractModel.effective_minimum` (`:227`) is the one
> in use. The conditions that `tune_history` imposes on a realized model are in
> `numerical-strategies.md` §3.7.3.

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
   → RayWorkPool with compute_handler = ScalarHistory.compute()
   → RayWorkPool with validation_handler = pool.object_validate()

STAGE 2: Adiabatic Histories
   Same (Potential, Coupling) grid
   → Two-pass: check ScalarHistory exists (error if not), check AdiabaticHistory exists
   → For missing: fetch ScalarHistoryProxy, enqueue AdiabaticHistory
   → RayWorkPool with compute_handler = AdiabaticHistory.compute()

STAGE 3: BBN Data
   Same grid
   → Two-pass: check ScalarHistory exists, check BBNData exists
   → For missing: fetch ScalarHistoryProxy, enqueue BBNData
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
        "ScalarHistory", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
    ),
    compute_handler=None, store_handler=None,
    store_results=True,
)
query_queue.run()

# Identify missing items
missing = [(pot, coup) for obj, (pot, coup) in zip(results, batch) if not obj.available]

# Pass 2: scalar object_get (creates skeleton rows) for missing items only
work_refs = [pool.object_get("ScalarHistory", shard_key=..., ...) for pot, coup in missing]
```

**Note added 2026-10-26 (`jordan-normalization` prompt 07): the grid and the stages.** The
grid is one `ExponentialModel` per (M, n = 1, β), M outer and β inner, looked up by one vectorized
`object_get("ExponentialModel", …)` (`convert_to_model`); there is no Λ grid and no T_stop. The
three stages batch the models (`grouper(…, n=8)`), bin them by the model's β and look up and compute
each history by (model, initial data, tuning tolerance); the tuning tolerance is
`DEFAULT_TUNING_TOLERANCE` (10⁻⁵), with no option yet (prompt 08). The scalar stage's first pass now
reads its lookup results (`pipeline_selection.missing_models`): a fully stored batch schedules no
work. Before, the "which are missing?" filter kept every model, and only the second lookup stopped
the work (`[02-scalar-stage-missing-filter-keeps-every-model]`,
`[03-step-1-first-pass-lookup-filters-nothing]`). `pipeline_selection.QueryEntry` is
`(model, history)`. The super-Planckian warning takes the models and evaluates ln A(φ*) at φ_ref = 0,
as before; prompt 08 moves it to the seed's φ_ref.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 08): the options, the warning and
> the scope.** Read at `dbfa31f`. It replaces step 3's "(β, M, Λ)", step 5's `Potential_array` and
> `Coupling_array` and the stages' `product(Potential_array, Coupling_array)` above (the note just
> above has the grid).
> - **The grid and the options.** `Model_array` is `itertools.product(M_grid, n_values, beta_grid)`
>   realized as `ExponentialModel` rows (`main.py:1082-1083`), with n from `--n-values`. The tuning
>   tolerance is `--tuning-tolerance` (`main.py:955-960`), not the fixed `DEFAULT_TUNING_TOLERANCE`
>   that the note just above gives, and `--tuning-max-integrations` reaches the remote as the argument
>   `max_integrations` of `ScalarHistory.compute` (`main.py:302`), not through the store.
> - **The warning.** `warn_super_planckian(Model_array, phi_init, T_init, units, cosmology)` runs once
>   per cosmology before the stages (`main.py:1094`; `pipeline_selection.py:341`). For each model whose
>   Ω(φ*) T* exceeds M_P, with Ω taken at the φ_ref of the model's seed on that cosmology (the φ_ref
>   that the history is tuned to differs from it), it prints one line naming β, M, n, φ*, the seed's
>   φ_ref and its kind, and then a count; it returns the array unchanged and nothing is skipped. The
>   `science-readiness` paragraph above, which has it called on the coupling array with Ω at
>   φ_ref = 0, is of the tree before. At M = 10⁻³ M_P the thresholds are β > 6.506 at φ* = 5 M_P and
>   β > 32.61 at φ* = 1 M_P (the docstring of `super_planckian_couplings`, `pipeline_selection.py:205`;
>   board §5.9).
> - **The scope.** The wrapper's manifest `scope` reads `config …; β …; M …; n …; φ* … Mp; tuning
>   tolerance …; N shards; job …` (`RunDrivers/registered_run.py:672-701`): the potential type and the
>   Λ fields are gone, and n and the tolerance are added.

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

**`ComputationFailureError`** (`ComputeTargets/exceptions.py`): raised when an ODE integration produces NaN/Inf, encounters unphysical state (negative energy density, imaginary Hubble rate), or overflows exponential bounds. Caught by the solver loop in `ScalarHistory.compute()`.

**Solver fallback:** `ScalarHistory.compute()` iterates through [Radau, BDF, LSODA, DOP853]. If one solver raises `ComputationFailureError` or `solve_ivp` fails to converge, the next solver is tried. If all solvers fail, the object is marked with `failure=True` and stored in the database as a failed record (so the system does not attempt to recompute it on subsequent runs).

> **Note, 2026-10-01 (`integrator-remediation`, prompt 02, `614c41a`).** The solver fallback described here does not exist and never ran: `method="Radau"` was a literal since `f67bc3a`, so a failing history was integrated four times identically. It is deleted. `ScalarHistory.compute()` runs one Radau step loop (`integrate_scalar_history`), inside one `try`; a `ComputationFailureError` from the loop or from the sampling is printed and the function returns `{"failure": True}`, which is stored as a failure row (the reason is printed, not stored). The exception table, including the new step budget, is in [`numerical-strategies.md` §3.5.5–§3.5.6](numerical-strategies.md). The paragraph above is kept as written for `b1f64d8`.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 06).** Since prompt 07 a
> stored history can fail inside `tune_history` as well as inside the integration. A failed tuning is
> a failure row whose reason's first clause is one of "the normalization did not converge", "no
> normalization reaches H0", "the effective minimum was not found", or an integration's own reason
> with " (tuning integration k)" appended (`ComputeTargets/ScalarHistory.py:1575-1745`);
> `summarise_failure_reasons` groups them by first clause, unchanged. `_tuning_state` catches only the
> `ValueError` of the effective minimum (`:1666`), so any other exception from its arithmetic would
> reach the driver and not become a failure row (log 07, Observation 2; none has arisen at any
> point measured). See `numerical-strategies.md` §3.7.5.

**`RayWorkPool` resilience:** The work pool does not bail out if individual tasks fail — it continues processing remaining items. The `_do_not_populate` two-pass pattern means failed objects are skipped in downstream stages (checked via `obj.failure`).

**Database startup cleanup:** `prune_unvalidated=True` causes each `Datastore` actor to delete any objects created but not yet validated on the previous run. This prevents partial results from being presented as complete. The lookups do not depend on it: `ScalarHistory.build`, `AdiabaticHistory.build` and `BBNData.build` return only validated rows (the last two since 2026-10-14), so a pool that does not prune, such as the plot scripts' read-only pools or `main.py --no-prune-unvalidated`, does not present one either.

---

## 12. Adaptation Guide for an Instanton Project

The following table maps ChamPBH components to their natural counterparts in a stochastic instanton project:

| ChamPBH component | Instanton project equivalent |
|-------------------|------------------------------|
| `ScalarHistory` | `InstantonSolution` — solves the instanton ODE in the inflationary background |
| `AdiabaticHistory` | `CompactionFunction` — computes the compaction function C(r) from the instanton |
| `BBNData` | `PBHMassFunction` — integrates to produce the PBH mass function β(M) |
| `ScalarHistoryProxy` | `InstantonProxy` — lightweight reference for downstream stages |
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

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 07).** The rows for
> `ExponentialCoupling` and `AbstractPotential` name classes that prompt 07 deleted. Their counterparts
> are `ExponentialModel`, a stored row keyed by (M, n, β), and `AbstractModel`, the interface
> (§8.4a).

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
- `ComputeTargets/ScalarHistory.py` → instanton ODE and solution class
- `Quadrature/supervisors/ScalarField.py` → `StateVector` and event tracking for instanton
- `ComputeTargets/AdiabaticHistory.py` → compaction function calculation
- `ComputeTargets/BBNData.py` → PBH mass function integration
- `ObjectFactories/ScalarHistory.py` → factory for `InstantonSolution`
- `ObjectFactories/AdiabaticHistory.py` → factory for `CompactionFunction`
- `ObjectFactories/BBNData.py` → factory for `PBHMassFunction`
- `CosmologyConcepts/Potentials/` → inflationary potential hierarchy
- `CosmologyConcepts/ConformalCouplings/` → may not be needed, or replaced with inflationary model parameters
- `CosmologyModels/` → inflationary background (de Sitter, quasi-de Sitter)
- `config/sharding.py` → updated shard key and table lists
- `config/datastore.py` → the registry of the new factories, their drop groups and batch sizes
- `main.py` → new pipeline driver
- `config/model_list.py` → inflationary model list

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; prompt 07).**
> `CosmologyConcepts/Potentials/` and `CosmologyConcepts/ConformalCouplings/` are deleted; the interface
> they held is `CosmologyConcepts/Models/` (§8.4a), and an inflationary project's counterpart is a new
> `AbstractModel` class and its table.

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

1. **Shard key must be in the database before sharded objects.** A `beta_value` (or equivalent shard key) must be inserted into the replicated tables and assigned to a shard before any `ScalarHistory` (or equivalent) referencing that key can be stored. `ShardedPool` handles this automatically when `object_get` is called on the shard-key type (its `_assign_shard_keys`; client contract §1, row 3).

2. **Replicated table `store_id`s must be identical across shards.** The `SerialPoolBroker` coordinates this. Never insert a replicated-table object directly into a shard — always go through `ShardedPool.object_get()` (a replicated write carries the controlling shard's serials to the others; contract §6).

3. **`_do_not_populate` is advisory.** It is a payload key that ChamPBH's `ScalarHistory` and `BBNData` factories read (§4.3), not the package's. If the object already exists in the database (from a previous run), `object_get` returns it regardless of this flag. The flag only suppresses the INSERT when the object is absent.

4. **`compute()` must return an `ObjectRef`.** `RayWorkPool` expects a Ray future from `compute_handler`. If the computation is CPU-bound and cannot be a Ray task (rare), wrap it in `ray.remote`.

5. **`store()` is called on the driver side, not in Ray.** After `ray.get(compute_ref)` completes, `RayWorkPool` calls `obj.store()` on the Python object in the driver process. This should copy data from the compute result into the object's fields. Then `pool.object_store(obj)` is called to serialize to the database.

6. **Shard count is fixed at database creation time.** The number of shards cannot be changed for an existing database (the primary's records decide; contract §1, row 8). Plan the shard count to match the expected parallelism and data volume.

7. **Ray actor naming.** Each shard `Datastore` is named `"shard{N:04d}-store"` and the `SerialPoolBroker` is named `"SerialPoolBroker"`. If reusing actor names across sessions (e.g., when `ray.init` reuses a cluster), ensure actors from previous sessions have been cleaned up.

8. **`store_results=True` in `RayWorkPool` is only for query-only passes.** When `task_builder` returns lists of refs (the vectorized case), the indices break. Use `store_results=True` only with scalar-returning `task_builder`s.

9. **The `grouper` + bin-by-shard pattern.** The main pipeline groups work items into batches of 8, then within each batch bins by `shard_key`. This minimizes round-trips by sending multiple items to the same shard in one call. For best efficiency in a new project, maintain this pattern.
