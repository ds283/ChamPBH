# Prompt 07 — the store

**Campaign:** [`README.md`](README.md) · **Board items:** **S1**–**S8** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R6, one table per model type with all
its Lagrangian parameters in one row; R7, the history holds its own normalization; R8, a stored
history always ends at T_CMB; R14, n in the key; P6, P7, P16, P17). Its orchestrator note, written
after 06's review, governs where it differs from this file.
**Gate:**
- 06 landed and reviewed, the gate "Tuning" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none, and no test of yours may start one.

**Closes:** `[02-scalar-stage-missing-filter-keeps-every-model]` (`dk-and-rpk-migration` board) and
`[03-step-1-first-pass-lookup-filters-nothing]` (`run-integrity` board). **Opens:** only what the
work finds.
**Recommended model:** **Opus**. The schema change of the campaign: a new model table, a new history
key, every writer and reader moved at once, and the fingerprint.

**Read first:**

1. [`README.md`](README.md): §2.2 (07's and 08's charters, and P17's line between them), §3, §4,
   §5, §6.0 R6–R8, R14, §6.2 P2, P6, P7, P16, P17.
2. `CLAUDE.md`: "The packages" (DatastoreKit's client contract, `docs/client-contract.md` at the
   pinned tag), the test rules (`_TempStoreCase`), and "Long-running jobs" (the wrapper).
3. 04's and 06's logs: the model interface, the adapter, and what `tune_history` returns.
4. `ObjectFactories/ExponentialPotential.py`, `ExponentialCoupling.py`, `ScalarHistory.py`
   (`register`, `inventory_spec`, `build`, `store`), `AdiabaticHistory.py`, `BBNData.py`;
   `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, `config/version.py`.
5. `main.py`: the grid construction (`convert_to_potential`, `convert_to_coupling`,
   `M_lambda_grid`), the scalar stage (`build_solver_batch` and its first-pass lookup), the adiabatic
   and BBN stages (`pipeline_selection.build_query_entries`, `select_missing`), the options it
   reads.
6. The readers: `plot_inputs.py` (the M × Λ product, the option matching), `plot_by_beta.py`,
   `plot_ScalarHistory.py`, `extract_common.py` (records and `CSV_COLUMNS`),
   `tools/compare_stores.py`, `tools/inventory_report.py`, `tools/bbn_from_store.py`;
   `RunDrivers/registered_run.py`'s `scope` (it reads the Λ options, `:699-703` before 02).
7. The two issues' entries on their boards.

---

## 1. What is wanted

A stored history is keyed by its model and its initial data, ends at T_CMB, and holds the
normalization and φ_ref it found. The store knows nothing of Λ. Every program that writes or reads a
store does so through the new tables, with behaviour changed only where the schema forces it (P17;
08 does the rest).

### 1.1 The items

- **S1 — the model's table.** `ObjectFactories/ExponentialModel.py`: `ExponentialModel` keyed by
  `M_serial` (`M_value`), `n_value` and `beta_serial` (`beta_value`), with `version` and
  `timestamp` as the other factories have; sharded by β (`config/sharding.py`), its `shard_key`
  the β row as `ExponentialCoupling`'s was. Its `build` returns 04's `ExponentialModel` with its
  store id. `ExponentialPotential`, `ExponentialCoupling` and `Lambda_value` leave the registry,
  the sharding and the batch sizes; their factory modules are deleted, and so are the
  `CosmologyConcepts` potential and coupling classes once nothing imports them (04's model
  replaced them); `Lambda_value` goes from `CosmologyConcepts` if unused.
- **S2 — the history's key** (P7): version, cosmology (type, serial), model (type, serial),
  φ*, π*, T_init, atol, rtol, the tuning tolerance (a `tolerance` row), solver. `T_Jordan_stop`
  leaves the key and the table: a stored history ends at the cosmology's T_CMB (R8). The lookup
  filters on exactly these, `validated == True` and the version as today.
- **S3 — the history's outputs** (P6): `normalization`, `V_ref`, `phi_ref`, `phi_0`, `pi_0`,
  `tracking`, `residual_H`, `residual_phi`, `tuning_iterations`, the step counts by kind,
  `seed_kind`, `seed_normalization`, `seed_phi_ref`, beside today's columns; all nullable for a
  failure row, as today's outputs are. The `ScalarHistory` object exposes `model` and
  `realized_model` (`model.realize(normalization, phi_ref)`), and drops `potential`, `coupling` and
  04's adapter.
- **S4 — the compute path.** `compute_scalar_history` (`@ray.remote`) calls 06's `tune_history`;
  `ScalarHistory.compute` passes the model and the tolerance; `store` writes S3's columns. The
  adiabatic and BBN computations build their realized model from their history.
- **S5 — `main.py`, as far as the schema forces** (P17):
  - the grid becomes models over (M, n = 1, β): `ExponentialModel` rows by `pool.object_get`; the
    Λ grid and its options (`--log10-Lambda-*`, `--Lambda-values-*`,
    `--samples-per-log10-Lambda-eV`) go, from the parser too, since nothing can consume them
    (`STRUCTURALLY REQUIRED`); `--T-stop-GeV` goes likewise. A run script that passes one is
    refused by `configargparse`. `exponential.yaml`'s `Lambda-values-eV` line goes;
  - the scalar stage queries and computes by (model, initial data, tolerance), sharded by the
    model's β;
  - **the two issues**: the first-pass "which are missing?" filter reads its lookup result, through
    `pipeline_selection.select_missing` or a pure helper beside it, tested there (no Ray): a fully
    stored batch yields no work. That closes both;
  - the tuning tolerance has a default (10⁻⁵) here; its option is 08's;
  - `RunDrivers/registered_run.py`'s `scope` stops reading the removed options (it would raise);
    adding n and the tolerance to it is 08's.
- **S6 — the readers**, as far as the schema forces: `plot_inputs.py` enumerates the stored
  `ExponentialModel` rows instead of the M × Λ product, and its option matching drops Λ and T_stop;
  `plot_by_beta.py`, `plot_ScalarHistory.py` and `extract_common.py` take M, n and β from the model
  and V_ref and φ_ref from the history where they took Λ; `tools/compare_stores.py`,
  `tools/inventory_report.py` and `tools/bbn_from_store.py` follow the tables. New CSV columns and
  sort orders are 08's; if a reader cannot run without one, add it here and log it. The tools that
  run their own histories (`tools/history_and_bbn.py`, `tools/surfing_scan.py`,
  `tools/plot_surfing_histories.py`) lose 04's adapter with everyone else: they build
  `ExponentialModel` directly and realize it with the normalization they use today (4 ln(1 meV),
  φ_ref = 0), so that their behaviour does not change here; moving them to tuned histories is 08's.
- **S7 — fingerprint format 5 and the version.** `FINGERPRINT_FORMAT` 5, its pin and a golden file
  whose store holds an `ExponentialModel`, a tuned `ScalarHistory` with every S3 column set, and its
  value, adiabatic and BBN rows; `VERSION_LABEL = "2026.7.0"` (P16) with its history line.
- **S8 — the tests** (no Ray; stores only through `_TempStoreCase`):
  - a tuned history (06's cheap point, or a stored payload from 06's tuned golden data to avoid
    re-tuning) stored and read back: every S3 column bitwise, the samples bitwise, and its
    `realized_model` equal to the one it was computed with (same normalization, φ_ref and
    parameters);
  - the lookup finds it by (model, initial data, tolerance), and does not with another φ*,
    tolerance or model;
  - a failure row stores no S3 values and is read back as a failure;
  - `tables_to_drop` refuses and accepts as before for the renamed groups;
  - `plot_inputs` on a temporary store lists its models;
  - the wrapper's tests pass with the options gone;
  - the tuned golden history passes bitwise.
- **The prose this commit makes false:** `.documents/architecture-summary.md` (the tables, the key,
  the compute path); `CLAUDE.md` where it says "potentials, couplings" (the plot-scripts bullet) or
  names `--T-stop-GeV`.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 06 handed on; both golden histories pass.
2. **After:** the five suites, each count's change explained; both golden histories; `black
   --check`; `main.py --help`, `plot_by_beta.py --help`, `plot_ScalarHistory.py --help`,
   `RunDrivers/registered_run.py --help` exit 0; `main.py --inventory` on a temporary store built
   by a test prints the new tables (no Ray).
3. `grep` outside the records for `Lambda_value`, `ExponentialPotential`, `ExponentialCoupling`,
   `T_Jordan_stop`, `T_stop_GeV`, `Lambda_values`: what remains, each with its reason (the surfing
   tools and `history_and_bbn.py` keep a fixed normalization until 08; say so).
4. **Deliberate breakages**, each failing a named test: the tolerance left out of the key (a lookup
   with another tolerance then finds the row); `normalization` stored as float32; the first-pass
   filter reverted.

## 3. Acceptance

1. S1–S8 as §1.1 says.
2. §2's checks in the log.
3. **One commit**, with the log `logs/07-the-store.md`. The board: row 07, S1–S8, the gate "Store",
   the two issues closed on their boards with their index rows deleted and the count corrected,
   `### 5.8 After prompt 07 (<date>)`.

## 4. Stop conditions: stop and ask

- The store needs a design choice that R6–R14 and P6–P8 do not make (the brief's §5).
- DatastoreKit cannot express the key or the sharding as S1–S2 say (a package question, never a
  workaround here).
- A reader cannot be kept working without a behaviour change larger than a column (P17).
- A round trip is not bitwise.

## 5. What this prompt does not do

- It does not add `main.py`'s tuning options, change the super-Planckian warning, remove
  `--potential-type`, or move the tools that run their own histories (08).
- It builds no store but the tests' temporary ones, and starts no Ray.
- **Files it changes:** `ObjectFactories/` (the new factory; the deleted ones; `ScalarHistory.py`,
  `AdiabaticHistory.py`, `BBNData.py`); `CosmologyConcepts/` (the deleted classes; the model's
  store id); `ComputeTargets/` (the compute path); `config/`; `main.py`; `exponential.yaml`;
  `pipeline_selection.py`; the readers of S6; `RunDrivers/registered_run.py` (the scope only); the
  tests and the golden data; `CLAUDE.md`; `.documents/architecture-summary.md`; the boards and the
  index; the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
