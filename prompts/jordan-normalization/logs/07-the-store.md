# Log 07 — the store

**Subject:** Key each history on its model and store what its tuning found
**Commit:** `223afc6`, on branch `jordan-normalization`, on top of `83569ce`
**Date:** 2026-10-26, by Claude Opus 5.5
**Result:** **COMPLETE**, with no stop.
- S1–S8 shipped under orchestrator note 07, whose sixteen rulings govern where they differ from
  the prompt.
- **Both golden histories pass unchanged and bitwise**: `golden_history_05.json` through
  `integrate_history` on the model built directly, and `golden_history_06_tuned.json` through
  `tune_history`. No existing golden JSON was written.
- **A tuned history round-trips bitwise** (every tuning column, raw and on the object; all 5960
  samples, every field), and its `realized_model` is rebuilt equal to the one it was computed
  with. The new module is `ObjectFactories/tests/test_tuned_history_round_trip.py` (6 tests).
- `FINGERPRINT_FORMAT` is 5 (15 pairs, 13 distinct digests), with the golden file
  `store_fingerprint_jordan-normalization-07.json`; `VERSION_LABEL` is `2026.7.0`.
- Counts: CosmologyModels 26, ComputeTargets 225 (61 + 164, −1), ObjectFactories 92 (+6), tools
  102, RunDrivers 61. Every change of count is a named addition or a removal ruling 14 names.
- Closed: `[02-scalar-stage-missing-filter-keeps-every-model]` (`dk-and-rpk-migration`) and
  `[03-step-1-first-pass-lookup-filters-nothing]` (`run-integrity`). Narrowed:
  `[02-the-scalar-history-parent-is-still-called-model]`. Opened: none. The index goes from 24 to
  22 open.
- The four breakages are recorded below. As note 07 §0 found, the column as `Float(32)` fails
  nothing on SQLite; rounding the value to float32 fails (a) and (d).

The run was cut off once by an API rate limit, after the format-5 golden store and file were
written and every suite had passed; it resumed on the same working tree, and nothing was redone.

## What shipped

### S1 — the model's table (ruling 1)

- `ObjectFactories/ExponentialModel.py`, `sqla_ExponentialModel_factory`: `version` and
  `timestamp`; columns `M_serial` (FK `M_value.serial`), `n_value` (Integer) and `beta_serial` (FK
  `beta_value.serial`), all not null, the serials indexed. `inventory_spec`: leaves `n_value`,
  parents `M` and `beta`. `build` matches the three columns and not the version, and returns
  `ExponentialModel(store_id, M, n, beta, units)` with `_new_insert` or `_deserialized`.
- `config/datastore.py`: `ExponentialModel` in `ExponentialPotential`'s place, batch size 50;
  `ExponentialCoupling` and `Lambda_value` gone (21 entries). `config/sharding.py`:
  `"ExponentialModel": "shard_key"` in `sharded_tables` where `ExponentialCoupling` was;
  `Lambda_value` and `ExponentialPotential` out of `replicated_tables`, `Lambda_value` out of
  `read_table_config`.
- Deleted by `git rm`: `ObjectFactories/ExponentialPotential.py`, `ObjectFactories/ExponentialCoupling.py`,
  `CosmologyConcepts/Potentials/` (4 files), `CosmologyConcepts/ConformalCouplings/` (4 files),
  `CosmologyConcepts/Lambda_value.py` and its export; `ExponentialModel.from_rows`. The comment in
  `CosmologyConcepts/Models/model_ids.py`, `ExponentialModel`'s module docstring and two docstrings
  of `AbstractModel` that pointed at `AbstractPotential` are brought up to date.

### S2, S3 — the history's key and outputs (rulings 2, 3)

- `ScalarHistory`'s table loses `potential_type`, `potential_serial`, `coupling_type`,
  `coupling_serial` and `T_Jordan_stop_serial`; gains `model_type`, `model_serial` (Integer,
  indexed, not null, no FK) after `cosmology_serial`, and `tuning_tol_serial` (FK
  `tolerance.serial`) after `rtol_serial`.
- The spec: leaves `cosmology_type`, `model_type`, `failure`; parents `version`, `cosmology`,
  `model` (`{EXPONENTIAL_MODEL: "ExponentialModel"}`), `T_init`, `phi_init`, `pi_init`, `atol`,
  `rtol`, `tuning_tol`.
- `build`'s filter is the key, `validated == True` and the version, and nothing else. The payload
  keys are `model` and `tuning_tol`.
- Fifteen nullable columns after `density_NP_ratio_70keV`, the names of `TUNING_OUTPUT_KEYS`, with
  a comment giving each unit: `normalization` and `seed_normalization` unconverted (Float(64)),
  `V_ref` in M_P⁴, `phi_ref`, `phi_0`, `pi_0`, `seed_phi_ref` in M_P, `tracking`, `residual_H`,
  `residual_phi` (Float(64)), the four counts (Integer), `seed_kind` (String). Three module helpers
  (`_tuning_unit`, `_tuning_columns`, `_tuning_from_row`) do the conversions. `build` reads them in
  its main query, so they are there with `_do_not_populate`.

### S3, S4 — the object and the compute path (rulings 4, 5, 6)

- `ScalarHistory(…)` takes `model: AbstractModel` and `tuning_tol: tolerance` in place of
  `potential`, `coupling` and `T_Jordan_stop`; the other parameters keep their order.
- `TuningValues = namedtuple("TuningValues", TUNING_OUTPUT_KEYS)`. `ScalarHistory.tuning` raises on
  a failure row and when not populated; it is set by `store()` from the payload and by `build` from
  the row (`payload["tuning"]`).
- `realized_model` is `self._model.realize(tuning.normalization, tuning.phi_ref)`, built on first
  access and cached, and raises where `tuning` does. `model` and `tuning_tol` are properties;
  `shard_key` is `self._model.shard_key`.
- `T_Jordan_stop` stays as a property only: `temperature(0, cosmology.T_CMB_Kelvin * units.Kelvin)`,
  with a docstring saying `compute_BBN_data`'s pre-check reads it.
- `compute_scalar_history` is `(cosmology, model, T_init, phi_init, pi_init, z_grid, task_label,
  atol, rtol, tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS,
  verbose)`, its body `return tune_history(...)` by name, with no `secant`. It moved below
  `tune_history` so that its defaults are the module constants. `ScalarHistory.compute` passes
  `self._model` and `tolerance=self._tuning_tol.tol`, with `self._model.name` as the default label.
- The callers that want the primitive call `integrate_history` with the arguments they passed
  before: `golden_history.py`, `test_fixed_T_values.py`, `test_sampling_axis.py`,
  `ComputeTargets/tests/test_scalarhistory_failure_reason.py`, `tools/history_and_bbn.py`,
  `tools/surfing_scan.py`. The adapter's users build `ExponentialModel(0, M_value(0, M), n,
  beta_value(0, β), units)` and realize it at `4.0 * log(Λ)`, `0.0`.
  `golden_history.rows` became `golden_history.model(beta, units)` and
  `golden_history.normalization(units)`.
- `AdiabaticHistory` and `BBNData` hold `model` (property) in place of `_potential`, `_coupling`;
  `shard_key` and the default task label are the model's. `compute_adiabatic_values` reads
  `model.realized_model`, the tuned one; its comment says so. `compute_BBN_data` and its pre-check
  are not changed.

### S5 — `main.py`, `pipeline_selection.py`, the parser (rulings 7–10)

- `convert_to_model`: one vectorized `object_get("ExponentialModel", payload_data=[{"shard_key":
  beta, "M": M, "n": 1, "beta": beta, "units": units} …])` over `itertools.product(M_grid,
  beta_grid)`; `--potential-type Exponential` maps to it, anything else raises as before.
  `convert_to_Lambdas`, `convert_to_potential`, `convert_to_coupling`, the Λ grid, its printing, the
  T_stop lookup and every `"T_Jordan_stop"` key are gone. `tuning_tol` is
  `object_get("tolerance", tol=DEFAULT_TUNING_TOLERANCE)` beside atol and rtol.
- `run_pipeline(pool, model_data, Model_array, T_init, phi_init, pi_init, z_grid, atol, rtol,
  tuning_tol, solvers, metadata)`; `metadata` loses `samples_per_log10_Lambda_eV`. The three stages
  batch `Model_array` with `grouper(…, n=8)`, bin by `model.shard_key`, and key every lookup by
  `model` and `tuning_tol`. Batch labels are `{job}-{Class}-{model.name}-{time}`.
- The first pass: `missing_models(binned_batch[key], query_outcomes)` in place of the
  comprehension.
- `pipeline_selection`: `QueryEntry(model, history)`; `build_query_entries(models,
  history_results)` (`unavailable` holds models); new pure `missing_models(models, lookups)` (not
  `available`, in order, `_check_lengths`; a stored failure is not missing). The super-Planckian
  warning keeps its names and signatures, takes models, evaluates `model.realize(None,
  0.0).log_Omega(phi)` (a helper, `_log_Omega_at_zero_phi_ref`), reads `_beta_float`; the count line
  says "models"; the docstrings say φ_ref = 0 and that 08 changes it. `main.py` passes
  `Model_array` and keeps it.
- `config/argument_parser.py` loses `--log10-Lambda-low-eV`, `--log10-Lambda-high-eV`,
  `--samples-per-log10-Lambda-eV`, `--Lambda-values-eV`, `--Lambda-values-Mp`, `--T-stop-GeV` and
  their three defaults. `exponential.yaml` loses `Lambda-values-eV`.
- `RunDrivers/registered_run.py`'s `describe_scope` drops the Λ part and its docstring's Λ.

### S6 — the readers (rulings 11, 13)

- `plot_inputs.py`: `stored_grid` returns `beta`, `M`. New `exponential_models(pool,
  potential_type, M_table, beta_table, units)`: M outer, β inner, n = 1, each by
  `object_get("ExponentialModel", shard_key=beta, M=M, n=1, beta=beta, units=units)` on its own; a
  `ReadOnlyWrite` "an insert" of `ExponentialModel` is skipped with one warning line and a count.
  `potentials`, `couplings`, `T_stop`, `POTENTIAL_CLASSES`, `COUPLING_CLASS` gone (`MODEL_CLASSES`,
  `MODEL_N` in their place). `parameters(…, units, tuning_tol)` gains the tuning tolerance (miss:
  `PlotOptionNotStored("tolerance", "the constant tuning tolerance", …)`), and `PlotParameters`
  `tuning_tol`. The module docstring follows.
- `plot_by_beta.py`: the models are grouped by `(M.store_id, n)`, β within each group;
  `build_beta_plot(model_label, group_model, …)` (one model of the group gives M and n); paths
  `plots/M={M/eV:.5g}eV_n={n}/…` and `csv/M=…eV_n={n}/…`; `run_pipeline(model_data, Model_array,
  T_init, phi_init, pi_init, atol, rtol, tuning_tol, tags, SM_baseline)`.
  `plot_ScalarHistory.py`: paths `beta=…/M=…eV_n={n}`; V along the history is
  `exp(history.realized_model.log_V(φ))`; work items are the models. Both pass
  `tuning_tol=DEFAULT_TUNING_TOLERANCE` to `plot_inputs.parameters`.
- `extract_common.py`: `CSV_COLUMNS`' `Lambda_eV` is `n`; `build_history_record(…, n, …)`;
  figure 2 groups by (M, n) with the legend `$M = …\,M_P$, $n = …$`; `histories.csv` sorts by
  (M, n, β). `add_beta_summary_labels(fig, model_label, group_model)` and `add_ScalarHistory_labels`
  name the model and give V_ref and φ_ref (below).
- `tools/compare_stores.py`: `POLYMORPHIC_REFERENCES["ScalarHistory"]` is cosmology and
  `"model_serial": ("model_type", {1: "ExponentialModel"})`, the comment naming
  `CosmologyConcepts/Models/model_ids.py`. `tools/bbn_from_store.py`'s `_FIND_QUERY` joins
  `ExponentialModel m on m.serial = sm.model_serial` for β and M.
- `tools/inventory_report.py` (rulings 11, 13): the group "Models" (`ExponentialModel`) replaces
  "Potentials and couplings", `Lambda_value` leaves its category; `HISTORY_FIELDS` (T_init,
  phiE_init, piE_init, model, failure), `HISTORY_EXTRA_FIELDS` (cosmology, atol, rtol,
  `tuning_tol` as `log10_tuning_tol`, version), `HISTORY_TYPE_LEAVES`, `history_extras`,
  `history_parts`, `history_sort`, `history_text_of`; the printed `history: {…}` and "common to every
  history". The `AdiabaticHistory` and `BBNData` specs' parent is `"history"`.

**The text of every changed title, label and warning** (ruling 11):
- `plot_inputs`: `!! plot: no ExponentialModel stored for M=<M> eV, n=1, β=<β>; no history can use
  it; skipped` and `!! plot: <k> of <N> ExponentialModel combinations of the stored M and β are not
  stored, and were skipped`; the tuning tolerance's miss: `!! plot: the store holds no tolerance for
  the constant tuning tolerance <tol>; …` (the existing template).
- `plot_by_beta`: titles `<label>: M = <M> M_P, n = <n>, phi* = <φ*> M_P`; the reflection and
  dropped-model lines say `M=<M> eV, n=<n>` where they said `M=… eV, Lambda=… eV`; docstrings say
  "(beta, M, n)" and "once per (M, n) group".
- `extract_common`: `add_beta_summary_labels`: `Model: ExponentialModel(M=<M>,n=<n>)` (where it said
  `Potential: <name>`); `add_ScalarHistory_labels`: `Model: <model.name>` (where `Coupling: …`) and
  `V_ref = <V_ref/M_P⁴> M_P^4, phi_ref = <φ_ref/M_P> M_P` (where `Potential: …`).
- `plot_ScalarHistory`: the five skip lines say `Model=<model.name>` where they said
  `Potential=…, Coupling=…`.
- `main.py`: `-- total number of models to integrate = <len(Model_array)>`; the super-Planckian count
  line `!! warning: <k> of <N> models start super-Planckian; …`.
- `inventory_report`: as above. A history's records now sort by the model's key (M, β, n), where
  they sorted by the coupling's β first.

### S7 — format 5 and the version (ruling 12)

`FINGERPRINT_FORMAT = 5` with its history paragraph; `SPEC_DIGESTS[5]` generated by
`test_fingerprint_format_guard.spec_digests(factories)` and pasted by script, beside `[1]`–`[4]`
(unchanged), and the comment's counts: 15 pairs (12 definitions), 13 distinct digests; format 4's
14 lose `ExponentialPotential`'s and `ExponentialCoupling`'s and gain `ExponentialModel`'s
(`M_value` keeps the digest it shared with `Lambda_value`); `ScalarHistory`, `AdiabaticHistory`
and `BBNData` moved; the other eleven pairs equal format 4's. The golden store keeps `LABEL` and
`T0` and holds two `ExponentialModel` rows (n = 1, 2), a third tolerance (the tuning tolerance,
−5), no `Lambda_value` or final temperature, and two `ScalarHistory` rows with every tuning column
set to a fixed literal (`TUNING_COLUMNS`). The golden file
`store_fingerprint_jordan-normalization-07.json` was written once from
`config.fingerprint.fingerprinter.take(primary, None)` by a scratch script that refuses to
overwrite (digest `3ceaa632c0050cde992482b11e7d662ae89e25f8df5d4f8bfd3b736edc556a59`), `taken`
null, no problems. `VERSION_LABEL = "2026.7.0"` with a dated paragraph in `config/version.py`.

### S8 — the tests

**Added.**
- `ObjectFactories/tests/test_tuned_history_round_trip.py` (ruling 14), 6 tests. setUpModule runs
  `integrate_history` once at the tuned golden's `normalization` and `phi_ref` (`float.fromhex`),
  with `golden_tuned_history`'s setup, to `cosmology.T_CMB_Kelvin * units.Kelvin`, and adds the
  JSON's `tuning` keys. The parameter rows and the model's row are made by the factories; the
  history is written by `ScalarHistory.store()` (with `ray.wait`/`ray.get` patched) and the
  factory's `store`, and read back by `object_get`.
  - `test_a_the_payload_is_the_tuned_golden_history`: `first_difference(JSON history,
    record_of(payload)) == []`.
  - `test_a_every_tuning_column_and_sample_round_trips_bitwise`: each of the 15 columns, raw and on
    `tuning`, by type and `float.hex`; all 5960 samples, every `SampleValues` field, matched by
    redshift serial, by `float.hex`.
  - `test_a_the_realized_model_is_rebuilt_equal`: `RealizedModel`; normalization and φ_ref by
    `float.hex`; `ExponentialModel` with the stored M, β, model store ids and the M, n, β floats;
    `log_V`, `d_logV_dphi`, `log_Omega` equal at five φ; cached.
  - `test_b_found_by_its_key_and_not_by_another`: found with the same key; not found with φ* = 4
    M_P, tuning tolerance 1e-6, n = 2, or β = 1.5 (subtests).
  - `test_c_a_failure_row_stores_no_tuning`: all 15 columns NULL; reads back as a failure with its
    reason; `tuning` and `realized_model` raise.
  - `test_d_tuning_and_the_realized_model_without_the_samples`: `_do_not_populate` gives `tuning`
    and `realized_model`; `values` raises.
- `ComputeTargets/tests/test_pipeline_selection.py::TestScalarStageFirstPass` (ruling 8), 5 tests:
  a fully stored batch gives `[]`; one unavailable gives that one; a stored failure is not missing;
  the order is kept; lookups one short or one long raise `ValueError`.
- `ComputeTargets/tests/test_plot_inputs.py::TestAParameterNotStored::test_a_tuning_tolerance_the_build_did_not_use_names_the_constant`
  (ruling 14): an unstored tuning tolerance (1e-6) is `PlotOptionNotStored` with its message.

**Removed** (ruling 14; each tested code `223afc6` deletes):
- `test_model_interface.TestTheAdapterIsBitwise.test_a_every_value_equals_the_rows` (the adapter
  against the potential and coupling rows, eight methods at twelve φ, bitwise) and
  `test_a_the_adapter_takes_only_the_exponential_rows` (the adapter's `TypeError` for rows of the
  wrong class).
- `test_plot_inputs`: `TestAStoreMainBuilt.test_a_given_T_stop_the_build_used_is_found`
  (`plot_inputs.T_stop` found a `--T-stop-GeV` the build used), `TestThePayloads.test_T_stop`
  (`T_stop`'s two payloads), `TestAParameterNotStored.test_a_T_stop_the_build_did_not_use` and
  `test_T_CMB_not_stored_names_the_constant` (the T_stop rows of the miss table), and
  `TestThePayloads.test_the_couplings` (`couplings`' payloads, replaced by `test_the_models`).

**Modified** (each to the new key, model or text, and nothing else):
- `ComputeTargets/tests/golden_history.py` (ruling 5): `model`, `normalization`, `run_history`
  through `integrate_history`, docstrings.
- `test_fixed_T_values.py`, `test_sampling_axis.py`, `ComputeTargets/tests/test_scalarhistory_failure_reason.py`,
  `test_kinematic_cap_loop.py` (ruling 5): the model built directly and realized at 4 log Λ, 0.0;
  `integrate_history`; docstrings and one comment that named `compute_scalar_history`.
- `test_model_interface.py` (ruling 5): `TestTheAdapterIsBitwise` → `TestTheNames`, its
  `test_a_the_names` on `golden_realized_model(1.5)`; `TestTheGoldenHistories` and `TestPickle`
  use `golden_realized_model`; module docstring.
- `test_adiabatic_mass.py`: `cls.exponential` is `ExponentialModel(…).realize(0.0, 0.0)`, whose
  conformal factor is the deleted `ExponentialCoupling`'s bit for bit (β φ/M_P − 0.0).
- `test_initial_field_option.py` (ruling 9): `_setup` builds `ExponentialModel` objects (M = 1e-3
  M_P, n = 1); assertions unchanged.
- `test_pipeline_selection.py` (ruling 8): stand-ins `(model, history)`, the models the strings
  "V<i>"; `unavailable == ["V4"]`; assertions otherwise unchanged.
- `test_plot_inputs.py` (ruling 14): `_build_run` makes main.py's lookups (three tolerances, β, M,
  `ExponentialModel` over M × β, the cosmology); stores FULL (2 β × 2 M), INCOMPLETE ({M1, M2} ×
  {b1, b2} then {M3} × {b1, b2, b3}: two missing), NO_MODEL (β = 1 without a model); tests
  rewritten for `exponential_models` (`test_the_potentials` → `test_the_models`,
  `TestABetaWithoutACoupling` → `TestABetaWithoutAModel`, the two refusal tests renamed for the
  model), `parameters` given `tuning_tol`, the script test checks `exponential_models` and that no
  `convert_to_model` is defined.
- `test_extraction.py` (ruling 11): `Lambda_eV=1e-3` → `n=1` in the three records.
- `test_reflection_reporting.py`: its stand-in history carries `model`, `tuning` (V_ref, φ_ref) and
  `_units`, which `add_ScalarHistory_labels` reads (ruling 11's labels).
- `test_bbn_from_store.py`: the imitation shards' SQL has `ExponentialModel(serial, M_serial,
  beta_serial)` and `ScalarHistory.model_serial`, for `_FIND_QUERY`.
- `ObjectFactories/tests/test_version_keyed_lookups.py`: `_stand_in_model` and `_tuning_tol` (serial
  16) for `_potential`, `_coupling`, `_T_stop`; the proxy's history carries `model`; the inserted
  row has the new key; (e) uses `ExponentialModel` (with `Planck_units()`, since the model's
  constructor formats M).
- `test_first_bounce_round_trip.py`, `ObjectFactories/tests/test_scalarhistory_failure_reason.py`:
  the new key; `_success_payload` carries `STAND_IN_TUNING`, since `store()` reads the tuning keys
  of a success.
- `test_datastore_config.py`, `test_inventory_specs.py` (`test_thirteen_definitions_serve_seventeen_classes`
  → `test_twelve_definitions_serve_fifteen_classes`), `test_fingerprint_format_guard.py` (15 pairs,
  docstring), `test_store_fingerprint_golden.py` (ruling 12): the registry, order, counts, maps and
  golden store.
- `tools/tests/test_compare_stores.py`: the fixture's `ExponentialModel` row (sharded), tuning
  tolerance and key; test 13's type spaces; `test_13b_the_potential_and_coupling_maps_follow_type_id`
  → `test_13b_the_model_map_follows_type_id`; test 14's assertions on the model.
- `tools/tests/test_inventory_report.py` (ruling 13): the store's models and tuning tolerance; the
  expected lines (44 records in 15 classes, `log10_tol = [ -10, -8, -5 ]`, "common to every
  history", `model=ExponentialModel(…)`, `history: {…}`).
- `RunDrivers/tests/test_registered_run.py` (ruling 10): `PILOT_SCOPE` and the default scope lose
  their Λ parts; the second scope test loses `--Lambda-values-Mp 1e-30`.

## Deviations from the prompt

Every ruling cited is in orchestrator note 07, §0.

1. **`STRUCTURALLY REQUIRED` (ruling 1)**: the model's table, registry, sharding and the deleted
   classes, as ruled.
2. **`IMPLEMENTATION CHOICE` (ruling 2)**: the key's columns, spec and the name `tuning_tol`.
3. **`IMPLEMENTATION CHOICE` (ruling 3)**: the fifteen columns, their units, NULL on a failure.
   The conversions are three module helpers rather than inline code.
4. **`IMPLEMENTATION CHOICE` (ruling 4)**: the object's `model`, `tuning_tol`, `tuning`
   (`TuningValues`), `realized_model`; **`STRUCTURALLY REQUIRED`**: `T_Jordan_stop` as a property.
5. **`IMPLEMENTATION CHOICE`**: `ScalarHistory.store()` reads the tuning keys of a success payload
   strictly (`data[key]`), so a success without them is an error rather than a row with NULL
   tuning. The stand-in success payload of `test_first_bounce_round_trip` (`_success_payload`,
   which `test_fixed_T_values_round_trip` imports) gained `STAND_IN_TUNING`. The factory's `store` writes NULLs for an object whose `_tuning` is None
   (`test_a3` sets the attributes by hand), so it does not refuse.
6. **`STRUCTURALLY REQUIRED` (ruling 5)**: `compute_scalar_history` tunes; it moved below
   `tune_history` so that its defaults are the constants themselves.
7. **`STRUCTURALLY REQUIRED` (rulings 5, 6)**: the primitive's callers, the adapter's users,
   `AdiabaticHistory` and `BBNData`.
8. **`IMPLEMENTATION CHOICE`**: `test_golden_history.py`'s docstring still names
   `compute_scalar_history._function` as what runs the golden histories. Ruling 5 names it among
   the docstrings to correct, but the dispatch brief lists `test_golden_history.py` as a file not
   to change, and I followed the brief. `golden_history.py`, which the test imports, says
   `integrate_history`. For the orchestrator.
9. **`STRUCTURALLY REQUIRED` (rulings 7, 8, 9, 10)**: `main.py`, `pipeline_selection.py`, the
   parser, `exponential.yaml` and the wrapper's scope. The super-Planckian evaluation is a small
   helper, `_log_Omega_at_zero_phi_ref`, used by both functions.
10. **`STRUCTURALLY REQUIRED` (ruling 10)**: besides `PILOT_SCOPE` and the second scope test, the
    wrapper test of the default scope
    (`TestRegistration.test_the_manifest_records_the_program_the_store_the_scope_and_the_packages`,
    `"M 10^25–10^26.5 eV, 6 per decade; Λ 10^-2–10^1 eV, 6 per decade; …"`) also printed Λ, and
    lost it; ruling 10 does not name it.
11. **`STRUCTURALLY REQUIRED` (ruling 11)**: the readers, with the column `n` the one a reader
    could not run without. `plot_inputs.parameters` takes `tuning_tol` as a required last
    argument (no default), so every caller says which tolerance it reads.
12. **`IMPLEMENTATION CHOICE` (ruling 11)**: `plot_by_beta` groups by `(M.store_id, n)` and passes
    the group's first model as `group_model`; `extract_common`'s beta-summary label prints
    `ExponentialModel(M=…,n=…)` from the model's own formatter.
13. **`STRUCTURALLY REQUIRED` (ruling 13)**: the spec parent `"history"` and the report's names and
    text. The report's history records now sort by the model's key (M, β, n); before, by the
    coupling's β, then the potential.
14. **`STRUCTURALLY REQUIRED` (ruling 12)**: format 5, its pin, the golden store and file, and
    2026.7.0.
15. **`STRUCTURALLY REQUIRED` (ruling 14)**: the tests above. Two choices inside it, `IMPLEMENTATION
    CHOICE`: the round trip's sampling axis is the golden one with store ids k + 1 (so that no
    redshift row has serial 0; ids do not enter the integration, and test (a)'s record check
    passes); and samples are compared by the `float.hex` of their values, since the payload holds
    numpy `float64` and the row reads back Python `float`. Renamed tests (same counts):
    `TestTheAdapterIsBitwise` → `TestTheNames`, `test_thirteen…` → `test_twelve…`, the test_plot_inputs
    and `test_13b` renames listed above.
16. **`STRUCTURALLY REQUIRED`**: `test_reflection_reporting`'s stand-in and `test_bbn_from_store`'s
    SQL fixture, which ruling 14's list does not name, change because the labels (ruling 11) and
    `_FIND_QUERY` (ruling 11) read the new attributes and tables.
17. **`STRUCTURALLY REQUIRED` (ruling 15)**: the breakages, with the prompt's `Float(32)` form
    tried first.
18. **`IMPLEMENTATION CHOICE` (ruling 16)**: `architecture-summary.md` gets five dated notes (§2's
    tree, §4.2's registry, §5.1–5.2's options and sharding, §7.2's key, outputs and compute path,
    §9's grid and stages), and no earlier text is edited. `CLAUDE.md` is not edited: `grep -n -i
    "potential\|coupling\|T-stop\|T_stop\|Lambda" CLAUDE.md` finds nothing.

No `UNINTENDED DRIFT`.

## Verification performed

**Before any edit** (at `83569ce`, code tree `1a66efc`), serially:

| suite | count | result | load (1-min) |
|---|---|---|---|
| CosmologyModels | 26 | OK | 3.7 |
| ComputeTargets `[a-f]` | 61 | OK (115.9 s) | 5.2 → 3.8 |
| ComputeTargets `[g-z]` | 165 | OK (265.7 s) | 3.5 → 11.9 |
| ObjectFactories | 86 | OK | 2.8 |
| tools | 102 | OK | 2.7 |
| RunDrivers | 61 | OK | 2.8 |

Golden tests alone: `test_golden_history` 3 tests, 12.16 s (load 10.5);
`test_tuning…test_e_the_tuned_golden_history` 30.14 s (load 9.0).

**After rulings 1–6** (the golden histories alone, the stop point of the order): `test_golden_history`
OK, 12.38 s (load 7.0); `test_e_the_tuned_golden_history` OK, 27.55 s (load 12.0).

**After** (this tree), serially:

| suite | count | change | result | load (1-min) |
|---|---|---|---|---|
| CosmologyModels | 26 | 0 | OK | 5.9 → 4.4 |
| ComputeTargets `[a-f]` | 61 | 0 | OK (164.8 s) | 16.8 → 149.8 |
| ComputeTargets `[g-z]` | 164 | −1 | OK (321.8 s) | 140.9 → 10.7 |
| ObjectFactories | 92 | +6 | OK | 33.1 |
| tools | 102 | 0 | OK | 41.6 |
| RunDrivers | 61 | 0 | OK | 24.0 |

- ComputeTargets −1: −2 (`test_model_interface`, the adapter's two tests), +5
  (`TestScalarStageFirstPass`), −5 +1 (`test_plot_inputs`: the T_stop and couplings tests out, the
  tuning tolerance's miss in).
- ObjectFactories +6: `test_tuned_history_round_trip.py`.
- The load of 140–150 was another session's; `test_prym_passenger`'s wall-clock test passed in it.

Times alone, after (load 5.9–6.9): `test_golden_history` 11.61 s; `test_e_the_tuned_golden_history`
26.82 s; `test_tuned_history_round_trip` 6 tests, 7.64 s (8.2 s at load 60).

- `black --check` on the 53 Python files of the commit: unchanged. Their `HEAD` versions were
  black-clean, so black changed only my edits.
- `main.py --help`, `plot_by_beta.py --help`, `plot_ScalarHistory.py --help`,
  `RunDrivers/registered_run.py --help`: exit 0, and none prints Λ or T-stop.
- `main.py --database <scratch>/never.db --ray-address 127.0.0.1:1 <option>` for each of the six
  removed options: exit 2, `main.py: error: unrecognized arguments: …`; no file created.
- `main.py --inventory` on the store `test_inventory_report._InventoryStoreCase.build_store` builds
  (temporary directory, no Ray): exit 0; it prints `44 records in 15 classes`, the group
  `-- Models` with `@@ ExponentialModel (sharded): 2 records` (`- M=2.436e+24 eV, beta=1.1,
  n_value=1`, `- M=2.436e+24 eV, beta=25, n_value=1`), `log10_tol = [ -10, -8, -5 ]`, `common to
  every history: …, log10_tuning_tol=-5`, the records `- T_init=20000 GeV, phiE_init=5 PlanckMass,
  piE_init=0 PlanckMass, model=ExponentialModel(M=2.436e+24 eV, beta=25, n_value=1),
  failure=False  tags: pilot  | values: 3`, and `history: {…}` for AdiabaticHistory and BBNData.
- **§2 item 3's grep**, `git grep -n -P
  "Lambda_value|ExponentialPotential|ExponentialCoupling|T_Jordan_stop|T_stop_GeV|Lambda_values"`
  outside `prompts/`, `.documents/`, `science-runs/`, `source/`. Every remaining hit:
  - `ComputeTargets/BBNData.py:434-440`: the pre-check (ruling 4, not edited);
    `ComputeTargets/ScalarHistory.py:1969`: the `T_Jordan_stop` property it reads (ruling 4).
  - Stand-ins carrying `T_Jordan_stop` for that pre-check (ruling 4, not edited):
    `test_bbn_callbacks.py:401`, `test_bbn_from_store.py:180`, `test_bbn_spline_floor.py:20, 87,
    96, 123`, `test_prym_passenger.py:185, 196`.
  - The tools that run their own histories, which keep a fixed normalization and their own
    `--T-stop-GeV` until 08: `tools/history_and_bbn.py:125, 285, 288` (the BBN stand-in's
    `T_Jordan_stop` and its option), `tools/surfing_scan.py` (9: its option, CSV column
    `T_stop_GeV`, `_INPUT_MATCH`), `tools/plot_surfing_histories.py` (3), `tools/plot_surfing_temperature.py:123,
    281` (the scan CSV's columns), `tools/tests/test_surfing_scan.py:201, 271`,
    `tools/tests/test_plot_surfing_temperature.py:58`.
  - `Quadrature/supervisors/ScalarField.py:67-145`: the supervisor's own `_T_stop_GeV` (the
    integration's end, not the key).
  - `ComputeTargets/tests/golden_history.py:317`: the setup's `T_stop_GeV` in the provenance it
    writes (the golden histories still stop at 1 MeV).
  - History in comments and docstrings, each saying what was deleted: `golden_history.py:19`,
    `test_adiabatic_mass.py:309`, `test_initial_field_option.py:23`, `test_kinematic_cap_loop.py:6-7`,
    `CosmologyConcepts/Models/ExponentialModel.py:22`, `test_datastore_config.py:23-24`,
    `test_inventory_specs.py:157`, `test_store_fingerprint_golden.py:40, 60, 64, 116`,
    `test_version_keyed_lookups.py:424`, `config/version.py:54`, `pipeline_selection.py:197`,
    `tools/compare_stores.py:176`, `tools/tests/test_compare_stores.py:893, 1080`.
  - `config/fingerprint.py` (20): the format 1–4 pins and their comments.
  - `test_fingerprint_format_guard.py:135, 140`: stand-in `Parent`s whose type map names
    `"ExponentialCoupling"` as a string (`test_a_parent_keyed_by_an_int_keeps_its_type`).
  - JSON records: `golden_history_01/03/05.json` (their setup's `T_stop_GeV`) and the four older
    fingerprint golden files, never written.

## The deliberate-breakage record

Each diff is relative to `223afc6`'s tree; each was checked with `git apply --check` and `git
apply -R --check`, replayed in `bash`, and reverted, with the tree as before (`black --check` clean).

**Breakage 1: the tuning tolerance left out of the key.**
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -396,7 +396,6 @@
                 table.c.pi_Einstein_init_serial == pi_Einstein_init.store_id,
                 table.c.atol_serial == atol.store_id,
                 table.c.rtol_serial == rtol.store_id,
-                table.c.tuning_tol_serial == tuning_tol.store_id,
             )
         )
 
```
`test_tuned_history_round_trip`: 1 failure of 6, `TestTheKey.test_b_found_by_its_key_and_not_by_another
[another tuning tolerance]`: `AssertionError: True is not false : another tuning tolerance`. (With
one row stored, the lookup finds one row, not two.)

**Breakage 2a: the prompt's form, the column as `Float(32)`.**
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -255,7 +255,7 @@
                 # dimensionless. tuning_iterations counts the integrations, the seed's included;
                 # plain_steps, secant_steps and guard_steps count the steps of each kind;
                 # seed_kind is the seed's kind (tracking, static or plain)
-                sqla.Column("normalization", sqla.Float(64), nullable=True),
+                sqla.Column("normalization", sqla.Float(32), nullable=True),
                 sqla.Column("V_ref", sqla.Float(64), nullable=True),
                 sqla.Column("phi_ref", sqla.Float(64), nullable=True),
                 sqla.Column("phi_0", sqla.Float(64), nullable=True),
```
`test_tuned_history_round_trip`: 6 tests, **OK**: it fails nothing, as note 07 §0 found (SQLite
stores every `REAL` as an 8-byte double and `Float(32)` does not round on the way in).

**Breakage 2b: `normalization` rounded to float32 on the way in** (ruling 15).
```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -14,6 +14,8 @@
 # limitations under the License.
 
 import json
+
+import numpy
 from math import log
 from typing import Optional, List
 
@@ -72,6 +74,8 @@
         value = None if tuning is None else getattr(tuning, key)
         unit = _tuning_unit(key, units)
         columns[key] = value / unit if value is not None and unit is not None else value
+        if key == "normalization" and value is not None:
+            columns[key] = float(numpy.float32(value))
     return columns
 
 
```
`test_tuned_history_round_trip`: 3 failures of 6, `test_a_every_tuning_column_and_sample_round_trips_bitwise`,
`test_a_the_realized_model_is_rebuilt_equal` and `test_d_tuning_and_the_realized_model_without_the_samples`,
each `AssertionError: '-0x1.1573400000000p+8' != '-0x1.1573404f5900fp+8'`.

**Breakage 3: the first-pass filter reverted** (the old comprehension's logic).
```diff
--- a/pipeline_selection.py
+++ b/pipeline_selection.py
@@ -99,7 +99,7 @@
     :raises ValueError: if the two lengths differ
     """
     _check_lengths("ScalarHistory lookup", len(models), len(lookups))
-    return [model for model, history in zip(models, lookups) if not history.available]
+    return [model for history, model in zip(lookups, models)]
 
 
 def build_query_entries(
```
`test_pipeline_selection`: 4 failures of 11, `test_a_fully_stored_batch_gives_no_work` (`['V0',
'V1', 'V2', 'V3', 'V4'] != []`), `test_a_batch_with_one_unavailable_gives_that_one`,
`test_a_stored_failure_is_not_missing`, `test_the_order_is_kept`.

## Observations not acted on

1. `test_golden_history.py`'s docstring still says the histories run through
   `compute_scalar_history._function` (Deviation 8). A one-line correction for whoever may edit the
   file.
2. Note 06's review observed that `_tuning_state` catches only `ValueError`, so an exception that
   escapes it (a `ComputationFailureError` from `log_V`, a `ZeroDivisionError`) now reaches the
   driver through `compute_scalar_history` instead of a failure row. 07 does not change it. None
   was met in any test.
3. `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]` stays open (ruling 4): its
   "Next step" says "For 07"; `functions`, `ModelFunctions` and `ZSplineWrapper` are unchanged.
4. `bbn_from_store`'s `_FIND_QUERY` joins `ExponentialModel` on `model_serial` without checking
   `model_type` (as it did not check `potential_type`); there is one model type.
5. `config/sharding.py`'s comment on the shard key still says the phenomenology is "mostly
   independent of (M, Lambda)": a statement of physics, left.

## State handed to the next prompt

- **Tree:** `223afc6`, on `jordan-normalization`, on top of `83569ce`.
- **The model's table.** `ExponentialModel` (M_serial, n_value, beta_serial), sharded by β, batch 50;
  `object_get("ExponentialModel", shard_key=β, M=…, n=…, beta=…, units=…)`. No `Lambda_value`,
  `ExponentialPotential`, `ExponentialCoupling`, `CosmologyConcepts/Potentials/` or
  `ConformalCouplings/`, and no `ExponentialModel.from_rows`.
- **The history.** Keyed by version, cosmology, model (`model_type`, `model_serial`), φ*, π*, T_init,
  atol, rtol, `tuning_tol`; payload keys `model`, `tuning_tol`. Fifteen tuning columns
  (`TUNING_OUTPUT_KEYS`), `ScalarHistory.tuning` (`TuningValues`), `realized_model` (tuned, cached),
  `T_Jordan_stop` a property at T_CMB. `compute_scalar_history(cosmology, model, T_init, phi_init,
  pi_init, z_grid, …, tolerance, max_integrations)` tunes; `integrate_history` is the primitive.
- **The driver.** `main.py` builds `Model_array` over (M, n = 1, β), the tuning tolerance fixed at
  `DEFAULT_TUNING_TOLERANCE` with no option, and the first pass uses
  `pipeline_selection.missing_models`. `--potential-type` stays. **08 adds the tuning options,
  moves the super-Planckian warning (still ln A at φ_ref = 0, models in), removes
  `--potential-type`, adds n and the tolerance to the wrapper's scope.**
- **The readers.** `plot_inputs.exponential_models`, `parameters(…, tuning_tol)`; the plot scripts
  pass `DEFAULT_TUNING_TOLERANCE`. `histories.csv` has `n` where `Lambda_eV` was; **08 adds V_ref,
  the normalization, φ_ref and the tuning diagnostics.**
- **The tools that run their own histories** (`history_and_bbn.py`, `surfing_scan.py`,
  `plot_surfing_histories.py`) call `integrate_history` at the fixed normalization 4 ln(1 meV),
  φ_ref = 0, with their own `--T-stop-GeV`, bitwise as before. **08 moves them to tuned histories**
  (`[05-surfing-scan-runs-at-a-fixed-lambda]`).
- **Format and version.** `FINGERPRINT_FORMAT` 5 (golden `store_fingerprint_jordan-normalization-07.json`),
  `VERSION_LABEL` `2026.7.0`. A store made before cannot be reused.
- **The golden histories.** `golden_history_05.json` (through `integrate_history`, the model built
  by `golden_history.model`/`normalization`) and `golden_history_06_tuned.json` pass bitwise.
- **Counts:** CosmologyModels 26, ComputeTargets 225 (61 + 164), ObjectFactories 92, tools 102,
  RunDrivers 61.
- **Open issues.** Closed: `[02-scalar-stage-missing-filter-keeps-every-model]`,
  `[03-step-1-first-pass-lookup-filters-nothing]`. Narrowed:
  `[02-the-scalar-history-parent-is-still-called-model]` (to `model_proxy`, `_model_proxy` and
  locals). Index: 22 open.
- **Breakage replay.** The four diffs above apply to `223afc6`'s tree, checked in `bash`.
