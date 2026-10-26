# Orchestrator — prompt 07, the store

Read [`../README.md`](../README.md) first: §2.2 (07's and 08's charters), §3, §4, §5, §6.0 R6–R8 and
R14, and §6.2 P2, P3, P6, P7, P16 and P17. Read 04's and 06's logs too, in particular "State handed
to the next prompt". The conventions of [`prompt-06.md`](prompt-06.md) bind this prompt as well.
They are restated below where 07 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07-the-store.md`](../07-the-store.md)
**Board items:** S1–S8 · **Gate:** "Store" · **Closes:** `[02-scalar-stage-missing-filter-keeps-every-model]`
(`dk-and-rpk-migration`) and `[03-step-1-first-pass-lookup-filters-nothing]` (`run-integrity`) ·
**Narrows:** `[02-the-scalar-history-parent-is-still-called-model]` (this board; ruling 13) ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none.

**Gate:**
- 06 landed (`1b83bbd`) and reviewed (`1a66efc`); the gate "Tuning" holds (board §2).
- `HEAD` is the commit that adds this note, on `jordan-normalization`, on top of `1a66efc`, with
  `git status` clean. No code has changed since `1a66efc`.
- The prompt's own gate: no tracked change, the five suites at the baselines below, and no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 08 is not dispatched until 07 is reviewed.

## 0. What makes this prompt unusual

**The schema change of the campaign, and the widest diff of it.** One commit replaces two tables
with one, re-keys the history, adds fifteen output columns, and moves every writer and reader at
once: `main.py`, the three object factories, the compute path, `pipeline_selection.py`, the plot
scripts and `plot_inputs.py`, `extract_common.py`, four tools, the wrapper's scope, and about
twenty test modules that build potential and coupling rows. The orchestrator read all of them at
`1a66efc` before writing this note. What it found, where the prompt is silent or would go wrong:

- **Six callers of `compute_scalar_history._function` want the primitive.**
  `ComputeTargets/tests/golden_history.py`, `test_fixed_T_values.py`, `test_sampling_axis.py`,
  `test_scalarhistory_failure_reason.py`, `tools/history_and_bbn.py` and `tools/surfing_scan.py`
  call it with a `RealizedModel` and a `T_stop` (`tools/plot_surfing_histories.py` through
  `surfing_scan.run_history`); the docstrings of `test_golden_history.py` and
  `plot_surfing_histories.py` name it as what runs the history. `integrate_history` has exactly
  that signature (06, ruling 1), so once the remote tunes, each calls `integrate_history` with the
  same arguments, and its output is bitwise unchanged (ruling 5).
- **The adapter is called in nine files.** `ExponentialModel.from_rows` is called by
  `ScalarHistory.realized_model`, `golden_history.py`, `test_fixed_T_values.py`,
  `test_kinematic_cap_loop.py`, `test_model_interface.py` (six sites), `test_sampling_axis.py`,
  `test_scalarhistory_failure_reason.py`, `tools/history_and_bbn.py` and `tools/surfing_scan.py`;
  and `golden_history.rows` builds the two row classes that 07 deletes. Each builds
  `ExponentialModel` directly and realizes it at `4.0 * log(Λ)`, φ_ref = `0.0`, where Λ is the
  same float the row held, so every value is bitwise the adapter's (ruling 5).
- **The BBN pre-check reads `T_Jordan_stop`.** `compute_BBN_data` (`ComputeTargets/BBNData.py:435`)
  refuses a history whose `T_Jordan_stop` is above 0.1 × `T_BBN_spline_min`. Two tests drive that
  check with stand-ins (`test_bbn_spline_floor` (a), `test_prym_passenger` (d)), and the stand-ins
  of `test_bbn_callbacks` and `test_bbn_from_store` carry the attribute. After R8 every stored
  history ends at T_CMB, so the check always passes; ruling 4 keeps it and its tests unchanged.
- **The super-Planckian warning takes couplings.** `pipeline_selection.super_planckian_couplings` and
  `warn_super_planckian` read `c.log_Omega(φ*)` and `c._beta_float` off `ExponentialCoupling`
  objects, which 07 deletes, and `test_initial_field_option` builds them. Changing what the warning
  evaluates is 08's (P11). Ruling 9 keeps its value (ln A at φ_ref = 0) and moves only what it
  iterates.
- **`pipeline_selection`'s `QueryEntry` is a (potential, coupling, model) triple**, where "model"
  is the `ScalarHistory`. Ruling 8 makes it (model, history).
- **Λ is in the readers' output paths, titles and CSV.** `plot_by_beta.py` writes
  `plots/M=…eV_Lambda=…eV/` and groups by potential; `plot_ScalarHistory.py` writes
  `plots/beta=…/M=…eV_Lambda=…eV/` and evaluates V along the history from the potential row;
  `extract_common.CSV_COLUMNS` has `Lambda_eV`, by which figure 2 groups and `histories.csv` sorts.
  None can keep Λ. Ruling 11 puts n where Λ was; 08 adds V_ref, the normalization and φ_ref.
- **The wrapper's scope test pins Λ.** `RunDrivers/tests/test_registered_run.py`'s `PILOT_SCOPE`
  reads `Λ 0.001 eV` (from `exponential.yaml`), and its second scope test passes
  `--Lambda-values-Mp`, which the parser will refuse (ruling 10).
- **`model` would name two things in one report line.** In the inventory specs of
  `AdiabaticHistory` and `BBNData`, and in `tools/inventory_report.py`, the parent `"model"` is the
  `ScalarHistory` (the open issue `[02-the-scalar-history-parent-is-still-called-model]`, whose
  next step names 07). Once `ScalarHistory`'s spec gains the parent `"model"`, the
  `ExponentialModel`, the report prints a history's `model: {…}` holding a model's `model: …`.
  Ruling 13 renames the spec parent and the report's names; the payload key `model_proxy` and the
  locals stay, and the issue narrows to them.
- **Two facts make the round trips bitwise.** `Planck_units().PlanckMass` is `1.0`, so a column
  stored as x/M_P (or x/M_P⁴) and read back as x·M_P is bitwise x in every driver's units. And
  `tolerance(tol=1e-5).tol` is `pow(10.0, log10(1e-5))`, which is `1e-05` exactly (as are 1e-4,
  1e-6 and 1e-8), so the tuning tolerance read back from its row is the float `tune_history` was
  given.
- **One integration reproduces the tuned golden history.** The orchestrator ran
  `integrate_history` once at `golden_history_06_tuned.json`'s stored normalization and φ_ref, with
  the generator's setup, to T_CMB: its `record_of` equals the JSON's `history` record with no
  difference (the probe `probe_final_integration.py`, in the session scratchpad; ruling 14 (a)).
  The round-trip test therefore needs one integration, not a tuning of five.
- **The prompt's float32 breakage would fail nothing.** SQLite stores every `REAL` as an 8-byte
  double, and SQLAlchemy's `Float(32)` does not round on the way in: a `Float(32)` column given
  −277.4502000419897 reads back −277.4502000419897 (checked in memory). Ruling 15 rounds the value
  instead.
- **`T_Jordan_stop` remains, with reasons.** Beyond the BBN pre-check (ruling 4), `tools/`' own
  `--T-stop-GeV` options and `Quadrature/supervisors/ScalarField.py`'s `T_stop_GeV` are not the
  history's key and stay (08 moves the surfing tools).

**Baselines** at `1a66efc`, serially, at load 5–13: CosmologyModels **26**, ComputeTargets
**61 + 165** (115 s and 254 s), ObjectFactories **86**, tools **102**, RunDrivers **61**, all
passing. These are 06's counts. `test_prym_passenger`'s wall-clock test passed. The probe of the
final integration took 5.3 s at load 2.

**These sixteen points, where the prompt is loose, silent or wrong for this tree, are ruled here.**

1. **The model's table (S1).** `ObjectFactories/ExponentialModel.py`, `sqla_ExponentialModel_factory`,
   in the form of the two factories it replaces.
   - `register`: `version` and `timestamp` True; columns `M_serial` (FK `M_value.serial`, indexed,
     not null), `n_value` (Integer, not null) and `beta_serial` (FK `beta_value.serial`, indexed,
     not null), in that order.
   - `inventory_spec`: `leaves=("n_value",)`, parents `M` (`M_serial`, `M_value`) and `beta`
     (`beta_serial`, `beta_value`). `build` matches the three columns and not the version, as the
     old factories did, and the comment says so.
   - `build`'s payload: `M`, `n`, `beta`, `units`, and `serial` if given. It returns
     `ExponentialModel(store_id, M, n, beta, units)` with `_new_insert` or `_deserialized`.
   - **The registry** (`config/datastore.py`): `"ExponentialModel"` takes `"ExponentialPotential"`'s
     place; `"ExponentialCoupling"` and `"Lambda_value"` go. Batch size 50. **Sharding**
     (`config/sharding.py`): `"ExponentialModel": "shard_key"` in `sharded_tables` where
     `"ExponentialCoupling"` was; `Lambda_value` and `ExponentialPotential` leave
     `replicated_tables`, and `Lambda_value` leaves `read_table_config`. The model's `shard_key` is
     its `beta_value` (04).
   - **Deleted**: `ObjectFactories/ExponentialPotential.py` and `ObjectFactories/ExponentialCoupling.py`;
     the packages `CosmologyConcepts/Potentials/` and `CosmologyConcepts/ConformalCouplings/`
     (their abstract and exponential classes and `model_ids.py`) once nothing imports them;
     `CosmologyConcepts/Lambda_value.py` and its export; `ExponentialModel.from_rows`. Files go by
     `git rm`. The comment in `CosmologyConcepts/Models/model_ids.py` that says 07 gives each
     model type its table is brought up to date.

   Classify as `STRUCTURALLY REQUIRED`.

2. **The history's key (S2).** In `ObjectFactories/ScalarHistory.py`:
   - columns `potential_type`, `potential_serial`, `coupling_type`, `coupling_serial` and
     `T_Jordan_stop_serial` go. `model_type` and `model_serial` (Integer, indexed, not null, no
     foreign key, as `cosmology_*`) follow `cosmology_serial`; `tuning_tol_serial` (FK
     `tolerance.serial`, indexed, not null) follows `rtol_serial`;
   - the spec: `leaves=("cosmology_type", "model_type", "failure")`; parents `version`,
     `cosmology`, `model` (`Parent("model_serial", type_column="model_type", types={EXPONENTIAL_MODEL: "ExponentialModel"})`),
     `T_init`, `phi_init`, `pi_init`, `atol`, `rtol` and `tuning_tol`
     (`Parent("tuning_tol_serial", "tolerance")`); its comment says the model is on this row's
     shard, both taking their shard key from β;
   - the payload keys are `model` and `tuning_tol`; `potential`, `coupling` and `T_Jordan_stop`
     go. `build`'s filter is the key, `validated == True` and the version, and nothing else.

   The name `tuning_tol` follows `atol` and `rtol`. Classify as `IMPLEMENTATION CHOICE`, citing
   this note.

3. **The outputs (S3), their columns and their units.** After `density_NP_ratio_70keV` and before
   `validated`, all nullable, NULL on a failure row, with a comment in the form of the first-bounce
   columns' saying what each holds and in what unit:
   - `normalization` (Float(64)): the model's value as `tune_history` returns it, unconverted,
     since only the model can interpret it (P6, "exact reconstruction"). For `ExponentialModel` it
     is ln V₀ in the units of the run that stored it, which for every driver is `Planck_units`;
     the comment says so;
   - `V_ref` (Float(64)) in M_P⁴; `phi_ref`, `phi_0`, `pi_0` and `seed_phi_ref` (Float(64)) in
     M_P, as `first_bounce_phi_Einstein` and `pi_Einstein_Mp` are;
   - `tracking`, `residual_H`, `residual_phi` (Float(64)), dimensionless;
   - `tuning_iterations`, `plain_steps`, `secant_steps`, `guard_steps` (Integer);
   - `seed_kind` (String(`DEFAULT_STRING_LENGTH`)); `seed_normalization` (Float(64)), unconverted.

   These are `TUNING_OUTPUT_KEYS`, under the same names and nothing more. `build` reads them in
   its main query, so they are present when `_do_not_populate` is set. Classify as `IMPLEMENTATION
   CHOICE`, citing this note.

4. **The `ScalarHistory` object (S3, S4).** In `ComputeTargets/ScalarHistory.py`:
   - the constructor takes `model: AbstractModel` and `tuning_tol: tolerance` in place of
     `potential`, `coupling` and `T_Jordan_stop`; its other parameters and their order are
     unchanged;
   - **`tuning`**: a namedtuple `TuningValues`, its fields `TUNING_OUTPUT_KEYS` in that order, set
     by `store()` from the payload and by the factory's `build` from the row. It raises on a
     failure row and when not populated, as `first_bounce` does, and is readable with
     `_do_not_populate`;
   - **`realized_model`**: `self._model.realize(self.tuning.normalization, self.tuning.phi_ref)`,
     built on first access and cached; it raises where `tuning` does. The adapter and its comment
     go;
   - `model` and `tuning_tol` are properties; `potential` and `coupling` go; `shard_key` is
     `self._model.shard_key`;
   - **`T_Jordan_stop` stays as a property and nothing else**: neither a parameter, a column nor a
     key. It is `temperature(0, cosmology.T_CMB_Kelvin * units.Kelvin)` (store id 0: not a row, as
     the stand-ins use), the end of every stored history (R8), the float `tune_history` stops at,
     with a comment saying why it exists: `compute_BBN_data`'s pre-check reads it. `BBNData.py`'s
     pre-check, and `test_bbn_spline_floor`, `test_prym_passenger` and `test_bbn_callbacks`, are not
     edited;
   - `functions`, `ModelFunctions` and `ZSplineWrapper` are left as they are
     (`[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]` stays open; the prompt
     does not take it).

   Classify the `T_Jordan_stop` property as `STRUCTURALLY REQUIRED` and the rest as
   `IMPLEMENTATION CHOICE`, citing this note.

5. **The compute path (S4).**
   - **`compute_scalar_history`** stays the `@ray.remote` and becomes
     `compute_scalar_history(cosmology, model, T_init, phi_init, pi_init, z_grid, task_label="compute_scalar_history", atol=…, rtol=…, tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS, verbose=False)`,
     `tune_history`'s order, with `model` an `AbstractModel`, its body `return tune_history(...)`
     with every argument by name, and no `secant`. Its docstring says it is the stored path.
   - `ScalarHistory.compute` passes `self._model` and `tolerance=self._tuning_tol.tol`; the
     default task label is `self._model.name`. `store()` sets `tuning` from the payload's
     `TUNING_OUTPUT_KEYS` on success and leaves it unset on a failure.
   - **Every caller that wants the primitive calls `integrate_history`** with the arguments it
     passes today: `golden_history.py`, `test_fixed_T_values.py`, `test_sampling_axis.py`,
     `test_scalarhistory_failure_reason.py`, `tools/history_and_bbn.py` and `tools/surfing_scan.py`.
     A docstring or comment that names `compute_scalar_history` as what runs an untuned history is
     corrected (those two files', `test_golden_history.py`'s and `plot_surfing_histories.py`'s).
   - **The adapter's users** build `ExponentialModel(0, M_value(0, M), n, beta_value(0, β), units)`
     and realize it at `4.0 * log(Λ)` and `0.0`, Λ the float the row was given.
     `golden_history.rows` becomes `golden_history.model(beta, units)` (the model) beside a
     module constant `NORMALIZATION = 4.0 * log(LAMBDA_OVER_EV * units.eV)`, or a function of
     units if the agent prefers; `run_history` realizes it. **`golden_history_05.json` passes
     bitwise and is not written.** `test_model_interface`'s `TestTheAdapterIsBitwise`: its first
     two tests go (they test the adapter against the rows, and both are deleted); `test_a_the_names`
     stays, on the model built directly.
   - `AdiabaticHistory`'s compute is unchanged: it reads `model.realized_model`, now the tuned one.
     Its comment at `:253-254` is corrected. `compute_BBN_data` reads no realized model and is not
     changed.

   Classify as `STRUCTURALLY REQUIRED`.

6. **`AdiabaticHistory` and `BBNData` (objects and factories).** Each object holds its history's
   `model` (`ScalarHistory.model`) in place of `_potential` and `_coupling`, with a property
   `model`; `potential` and `coupling` go; `shard_key` is the model's; the default task label is
   the model's name. Their factories' comments that say "the coupling's beta" say "the model's
   beta". Classify as `STRUCTURALLY REQUIRED`.

7. **`main.py` (S5).**
   - **The grid**: `convert_to_model` replaces `convert_to_potential` and `convert_to_coupling`:
     one vectorized `pool.object_get("ExponentialModel", payload_data=[{"shard_key": beta, "M": M, "n": 1, "beta": beta, "units": units} …])`,
     M outer and β inner (today's product order, potentials outer). `--potential-type` stays (08's)
     and its dispatch maps `Exponential` to `ExponentialModel`; any other value raises as today.
     `convert_to_Lambdas`, the Λ grid and its printing go. The "total number of models" line
     counts the models.
   - **The tuning tolerance**: `tuning_tol = pool.object_get("tolerance", tol=DEFAULT_TUNING_TOLERANCE)`,
     beside `atol` and `rtol`, imported from `ComputeTargets.ScalarHistory`. No option (08's).
   - **T_stop goes**: the per-cosmology `T_stop` lookup, `run_pipeline`'s `T_stop` parameter and
     every `"T_Jordan_stop"` payload key. `run_pipeline` takes `Model_array` in place of
     `Potential_array` and `Coupling_array`, and its `metadata` loses
     `samples_per_log10_Lambda_eV`.
   - **The three stages** batch the models with `grouper(…, n=8)`, bin by `model.shard_key`, and
     look up and compute by `(model, initial data, tuning_tol)`. Batch labels are
     `{job}-ScalarHistory-{model.name}-{time}` (likewise the other two).
   - **The two issues**: ruling 8's helper decides the first pass's missing models.
   - **The options**: `--log10-Lambda-low-eV`, `--log10-Lambda-high-eV`,
     `--samples-per-log10-Lambda-eV`, `--Lambda-values-eV`, `--Lambda-values-Mp` and `--T-stop-GeV`
     leave `config/argument_parser.py`, so `main.py` and both plot scripts refuse them.
     `exponential.yaml` loses `Lambda-values-eV`.
   - The super-Planckian warning: ruling 9.

   Classify as `STRUCTURALLY REQUIRED`.

8. **`pipeline_selection.py` and the two issues.**
   - **`QueryEntry`** becomes `(model, history)`: `model` the `ExponentialModel`, `history` its
     `ScalarHistory` lookup result. `build_query_entries(models, history_results)`; `unavailable`
     holds models. `MissingSelection` and `select_missing` are unchanged except for the word
     "model" in their docstrings. `main.py` reads `entry.model` and `entry.history`.
   - **A pure helper** `missing_models(models, lookups) -> list`: the models whose `ScalarHistory`
     lookup is not `available`, in order, using `_check_lengths`. A stored failure is not missing:
     today the second lookup finds it and nothing recomputes it, and the helper keeps that.
     `build_solver_batch` calls it on each bin's results in place of the comprehension at
     `main.py:260-266`; its `num_missing == 0` return then fires for a fully stored batch.
   - **Tests** in `ComputeTargets/tests/test_pipeline_selection.py` (no Ray): a fully stored batch
     gives `[]`; a batch with one unavailable gives that one; a stored failure is not missing; the
     order is kept; unequal lengths raise `ValueError`. The module's stand-ins move to (model,
     history).

   Classify as `STRUCTURALLY REQUIRED`. The two issues close on their boards' §4 with a
   **Resolved** line naming this commit and the helper, and their rows leave
   `.documents/OPEN_ISSUES.md` §1.15, with the count and the date corrected.

9. **The super-Planckian warning** keeps its value, its lines and its place before Ray (08 moves it
   to the seed's φ_ref, P11). `super_planckian_couplings` and `warn_super_planckian` keep their
   names and signatures, take models in place of couplings, and evaluate ln A(φ*) as
   `model.realize(None, 0.0).log_Omega(phi)`, which is bitwise today's βφ*/M_P (ln A at
   φ_ref = 0). β is read from `_beta_float`, as today. `main.py` passes `Model_array` and keeps it.
   Since the grid is now (M, β), a flagged β prints once per M; the count line says "models" where
   it said "couplings". The docstrings say φ_ref = 0 and that 08 changes it.
   `test_initial_field_option`'s (a) and (d) build `ExponentialModel` objects in place of
   couplings; their assertions on β are unchanged. Classify as `STRUCTURALLY REQUIRED`.

10. **The wrapper's scope (S5).** `describe_scope` drops the Λ part, and its docstring Λ; the
    potential type stays (08 removes it and adds n and the tolerance). `PILOT_SCOPE` loses
    `; Λ 0.001 eV`; the second scope test loses `--Lambda-values-Mp 1e-30` and its `Λ 1e-30 Mp`.
    `main.py --help`, `RunDrivers/registered_run.py --help` and both plot scripts' `--help` exit 0.
    Classify as `STRUCTURALLY REQUIRED`.

11. **The readers (S6)**, as far as the schema forces:
    - **`plot_inputs.py`**: `stored_grid` returns `beta` and `M` (no Λ); a new
      `exponential_models(pool, potential_type, M_table, beta_table, units)` looks up each stored M
      with each stored β at n = 1 (M outer, β inner) by `object_get("ExponentialModel", shard_key=beta, …)`,
      each on its own; a read-only miss is `ReadOnlyWrite` ("an insert" of `ExponentialModel`) and
      is skipped with one warning line and a count, as `couplings` does. `potentials`, `couplings`,
      `T_stop`, `POTENTIAL_CLASSES` and `COUPLING_CLASS` go (`models`, the cosmologies, keeps its
      name). `parameters` gains the tuning tolerance: an argument `tuning_tol`, which the scripts
      pass as `DEFAULT_TUNING_TOLERANCE`, looked up as the others, its miss
      `PlotOptionNotStored("tolerance", "the constant tuning tolerance", …)`; `PlotParameters`
      gains `tuning_tol`. The module docstring follows.
    - **`plot_by_beta.py`** groups the models by (M, n) where it grouped by potential, β within
      each group; paths `plots/M={M/eV:.5g}eV_n={n}/…` and `csv/M=…eV_n={n}/…`; titles and
      diagnostic lines say `n` where they said Λ. **`plot_ScalarHistory.py`**: paths
      `beta=…/M=…eV_n={n}`; V along the history is `exp(history.realized_model.log_V(φ))`, the
      tuned potential. `extract_common`'s labels name the model and give the history's V_ref and
      φ_ref where they named the potential and the coupling.
    - **`extract_common.py`**: `CSV_COLUMNS`' `Lambda_eV` becomes `n`, `build_history_record`
      takes `n`, figure 2 groups by (M, n) with the legend "M = …, n = …", and `histories.csv` sorts
      by (M, n, β). 08 adds V_ref, the normalization, φ_ref and the tuning diagnostics. Log this as
      the column a reader could not run without.
    - **`tools/compare_stores.py`**: `POLYMORPHIC_REFERENCES["ScalarHistory"]` maps
      `model_serial` to `("model_type", {1: "ExponentialModel"})`; its comment names
      `CosmologyConcepts/Models/model_ids.py`. **`tools/inventory_report.py`**: the "Potentials and
      couplings" group becomes the models; the history's key fields are T_init, phi_init, pi_init
      and model, with the three tolerances among the extras (ruling 13 for its "model"
      text). **`tools/bbn_from_store.py`**: `_FIND_QUERY` joins `ExponentialModel` for M and β.
    - **The three tools that run histories** keep today's behaviour bitwise: ruling 5's model and
      realization at their constant Λ, and `integrate_history`. Their tests pass unchanged.

    The text of every changed title, label and warning is the agent's, listed in the log. Classify
    as `STRUCTURALLY REQUIRED`.

12. **Fingerprint format 5 and the version (S7).** `FINGERPRINT_FORMAT = 5` with its history line;
    `SPEC_DIGESTS[5]` generated by `test_fingerprint_format_guard.spec_digests`, never typed, beside
    the old entries, and the comment's counts (pairs, distinct digests, which moved). The golden
    store keeps `LABEL` and `T0`, and holds two `ExponentialModel` rows (n = 1 and n = 2) where it
    held two potential rows, each with its own `ScalarHistory`, both with every S3 column set to a
    fixed literal; the golden file is `store_fingerprint_jordan-normalization-07.json`, beside the
    old ones, and `GOLDEN` names it. `VERSION_LABEL = "2026.7.0"` with a dated paragraph in
    `config/version.py`'s history: from 2026.7.0 each history is tuned to H0 and φ_min at T_CMB,
    keyed by its model, its initial data and the tuning tolerance, and stores the tuned values;
    a store made before cannot be reused. Classify as `STRUCTURALLY REQUIRED`.

13. **`[02-the-scalar-history-parent-is-still-called-model]`, the part 07 forces.** The parent
    field `"model"` in the `AdiabaticHistory` and `BBNData` specs becomes `"history"` (the column
    `scalar_history_serial` is unchanged), and `tools/inventory_report.py`'s helpers and printed
    text that call a history "model" say "history" (`model_text_of`, `model_parts`,
    `model_sort`, `MODEL_FIELDS`, `MODEL_EXTRA_FIELDS`, `model: {…}`, "common to every model").
    The payload key `model_proxy`, the attribute `_model_proxy` and locals that name a history
    `model` are not renamed. The issue is narrowed on this board's §3 to those, with a line saying
    what 07 renamed, and its index row in `.documents/OPEN_ISSUES.md` §1.16 is narrowed to match
    (the count does not change). Classify as `STRUCTURALLY REQUIRED` (the report would print one
    word for both parents).

14. **The tests (S8)**, no Ray, stores only through `_TempStoreCase`. A new module
    `ObjectFactories/tests/test_tuned_history_round_trip.py`, in the form of
    `test_first_bounce_round_trip.py` (`ScalarHistory.store()` with `ray.wait` and `ray.get`
    patched, the factory's `store` with the datastore's inserter, read back by `object_get`). Its
    docstring gives the cost.
    - **(a) The tuned history, round trip.** The payload is `integrate_history` once, with
      `golden_tuned_history`'s setup, at the JSON's normalization and φ_ref (read with
      `float.fromhex`), plus the JSON's `tuning` keys (one integration, about 5 s unloaded). The
      test first asserts that `record_of` of it has no `first_difference` from the JSON's
      `history` (so it is the tuned history), then stores and reads it back: every S3 column
      bitwise, raw (the row) and on the object (`tuning`); every sample field bitwise against the
      payload's samples; `realized_model` of the read-back object of type `RealizedModel`, its
      `normalization` and `phi_ref` equal to those it was computed with, its model an
      `ExponentialModel` with the same M, n and β (floats and store ids), and `log_V`,
      `d_logV_dphi` and `log_Omega` equal at five field values.
    - **(b) The key.** A lookup with the same (model, initial data, tuning_tol) finds the row; one
      with another φ*, another tuning tolerance (10⁻⁶), another model (n = 2, or another β) does
      not (an unpopulated object).
    - **(c) A failure row** stores every S3 column NULL, reads back as a failure, and its `tuning`
      and `realized_model` raise.
    - **(d) `_do_not_populate`**: the row read so gives `tuning` and `realized_model`, and its
      `values` raises.
    - Elsewhere: `tables_to_drop` refuses and accepts as before (`test_datastore_config`, its
      table lists updated); `plot_inputs` on a temporary store lists its models, skips a missing
      combination with its warning, and refuses an unstored tuning tolerance with
      `PlotOptionNotStored` (`test_plot_inputs`); the wrapper's tests pass (ruling 10); both golden
      histories pass bitwise (`golden_history_05.json`, `golden_history_06_tuned.json`).
    - **Test modules that build the deleted rows** (`ObjectFactories/tests/*`, `tools/tests/*`,
      `ComputeTargets/tests/*`) move to `ExponentialModel` and the new key, and nothing else. Each
      modification is listed in the log with its reason (README §5).
    - **Tests removed**, each named in the log with what tested it before: only tests of code this
      commit deletes (the adapter's two in `test_model_interface`; `plot_inputs.T_stop`'s and the
      T_stop rows of `test_plot_inputs`' miss table; any test of `potentials` or `couplings` that
      `exponential_models`' tests replace). **A suite's count may fall only by those.** Any other
      fall is a stop.

    Classify as `STRUCTURALLY REQUIRED`.

15. **The breakages** (§2 item 4), each a diff `git apply` replays from the agent's commit:
    1. `tuning_tol_serial` left out of `build`'s filter: (b)'s "another tolerance" fails (and the
       lookup may find two rows).
    2. **`normalization` rounded to float32 on the way in** (`float(numpy.float32(…))` in the
       factory's `store`): (a) fails. First try the prompt's form, the column as `Float(32)`, and
       record that it fails nothing on SQLite, as §0 found.
    3. **The first-pass filter reverted**: `missing_models` returning every model (the old
       comprehension's logic): the fully-stored-batch test fails.

    For each, the log names every test that fails and how. Classify as `STRUCTURALLY REQUIRED`.

16. **The records.**
    - **This board:** row 07, S1–S8 and the gate "Store"; `### 5.8 After prompt 07 (<date>)`, in
      §5.7's form; §1.1's items for 07 marked as landed; §3 for ruling 13; §3.1's two rows marked
      closed; §4 for the two issues.
    - **`dk-and-rpk-migration` and `run-integrity` boards:** the two issues moved to §4 with their
      **Resolved** lines.
    - **`.documents/OPEN_ISSUES.md`:** the two rows deleted, the narrowed row, the count and the
      date.
    - **The prose this commit makes false:** `.documents/architecture-summary.md` (the file tree,
      the factory table, the sharding lists, the key, the compute path), as a dated note in the
      form of 01's (the text before it is left as it was); `CLAUDE.md` only where it is now false
      (the orchestrator found no "potentials, couplings" or `--T-stop-GeV` in it at `1a66efc`; the
      log says what the agent's grep found).
    - **Not edited:** `prompts/INDEX.md`, the README, `numerical-strategies.md` (10's).

**The stop conditions.** The prompt's §4, with these added:
- `golden_history_05.json` or `golden_history_06_tuned.json` fails, or ruling 14 (a)'s record
  check differs;
- a suite's count falls by more than ruling 14's named removals;
- an existing test fails for a reason this note does not name. Never edit such a test to make it
  pass;
- a reader cannot keep working without a change larger than ruling 11 allows (P17).

**No driver runs** beyond the `--help`s and `main.py --inventory` on a test-built temporary store.
No Ray is started, and no store is opened except the tests' temporary ones.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only (`git rm` for the deleted files), and show
  `git diff --cached --name-status` before committing. Count, then paste. A log states only what the
  diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Order.**
  1. The five suites and both golden tests alone (with their times), before any edit.
  2. Rulings 1–6 (the factories, the object, the compute path, the deleted classes) with the tests
     that build rows; the golden histories alone; stop here if either fails.
  3. Rulings 7–13 (`main.py`, `pipeline_selection`, the readers, the fingerprint).
  4. Ruling 14's new tests; the suites; the breakages; §2 item 3's grep.
  5. The records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its two
  halves. Send output to a file in the agent's scratch directory, grep the verdict from it, and
  record `uptime` beside each. A failure is re-run alone before it is called real.
  `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall-clock bound and fails
  under load; re-run it alone before calling it real.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. No worktree is needed.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `1a66efc`, before this note was committed:
1. **The tree.** `jordan-normalization`, with `git status --short` empty.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: three runs, all finished
   (`ray-sqla-align` 02's). None is `running` or stale.
4. **The probes** of §0: the final integration at the tuned golden's values; SQLite's `Float(32)`.
5. **The baselines** above.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README, this board, and 04's and 06's logs;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad,
  with the probe `probe_final_integration.py` beside it. The agent does not read `orchestrator/` or
  prompts 08–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/07-the-store.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **The files it may add, change or delete** are the prompt's §5, with these additions:
  - the test modules that build the deleted rows or call `compute_scalar_history._function` or
    `from_rows` (ruling 14), `ComputeTargets/tests/golden_history.py` (ruling 5) and
    `test_pipeline_selection.py`, `test_initial_field_option.py`, `test_plot_inputs.py`,
    `test_extraction.py` among them;
  - the new `ObjectFactories/tests/test_tuned_history_round_trip.py` and the golden fingerprint
    file;
  - `tools/history_and_bbn.py`, `tools/surfing_scan.py`, `tools/plot_surfing_histories.py`
    (ruling 5 only) and `tools/tests/`;
  - `config/argument_parser.py`;
  - `prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md` and
    `prompts/run-integrity/IMPLEMENTATION_STATE.md` (§4 only), and `.documents/OPEN_ISSUES.md`.

  Not: `golden_history_01/03/05.json`, `golden_history_06_tuned.json`, `golden_tuned_history.py`,
  `test_golden_history.py`, `test_tuning.py`, `ComputeTargets/BBNData.py`'s pre-check and its three
  tests, `PRyM/`, `thirdparty/`, the packages.
- **§0's rulings** bind it, each classified as §0 says.
- **It starts no Ray, opens no store but the tests' temporary ones, and runs no driver** beyond
  the `--help`s and `--inventory`.
- **Stop and ask** on any of §0's stop conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the golden tests' and the round-trip test's times;
  - every test added, modified and removed, with what it asserts or asserted;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - §2 item 3's grep, each remaining hit with its reason;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git show --stat <agent>` lists only §2's files, and the deletions are ruling 1's.
   `git status` is clean.
2. **The schema.** Read the three factories' `register`, `inventory_spec`, `build` and `store`
   against rulings 1–3, and `config/datastore.py` and `config/sharding.py` against ruling 1.
3. **The object and the compute path.** `ScalarHistory`, `compute_scalar_history`, `compute`,
   `store` against rulings 4–5; `AdiabaticHistory` and `BBNData` against ruling 6. Every former
   caller of `compute_scalar_history._function` calls `integrate_history` with its old arguments.
4. **The golden histories.** Both golden tests alone at `<agent>`; the four golden JSONs
   byte-identical to `<agent>~1`'s.
5. **The round trip.** Read `test_tuned_history_round_trip.py` against ruling 14, and run it alone
   with its time. Then a probe of the orchestrator's own: tune the golden point through
   `compute_scalar_history._function`, store it through `_TempStoreCase`, read it back, and compare
   all fifteen tuning values and the realized model with the tuning's own.
6. **`main.py` and the issues.** Read the grid, the three stages and the first pass against
   rulings 7–8; `missing_models` and its tests.
7. **The readers.** `plot_inputs.py` against ruling 11; the paths and labels in the plot scripts;
   `extract_common`'s columns; the tools' SQL and maps. The three history tools' tests pass.
8. **The fingerprint.** Regenerate `spec_digests` and compare with `SPEC_DIGESTS[5]`; the golden
   file new and the old files byte-identical; `VERSION_LABEL`.
9. **The drivers.** `main.py --help`, both plot scripts' and the wrapper's `--help` exit 0; one of
   the removed options refused (exit 2); `main.py --inventory` on a temporary store.
10. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
    says, revert, and check that `git status` is clean.
11. **The records and the suites.** The board (row, items, gate, §3, §3.1, §4, §5.8), the two
    other boards, the index, and the log in §5.1's form, with every deviation classified; §2 item
    3's grep. Then the five suites at `<agent>`, serially, at the counts the log states, and
    `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 07* paragraph in §5.8. Fix small
  residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log, the two other boards, the index);
  - add this note to the board's list of orchestrator notes, marked "used for 07";
  - update the status line: 7 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the primitive's callers move to `integrate_history`, and the adapter's users build the model
      directly at the same normalization, so every untuned value is bitwise unchanged (ruling 5);
    - `ScalarHistory` keeps a derived `T_Jordan_stop` (T_CMB) so that the BBN pre-check and its
      tests stand (ruling 4);
    - the super-Planckian warning iterates models, at φ_ref = 0 as before (ruling 9);
    - Λ's place in paths, labels and `histories.csv` is taken by n (ruling 11);
    - the `"model"` parent of the adiabatic and BBN specs is renamed `"history"` (ruling 13);
    - the round-trip test integrates once at the tuned golden's values instead of tuning (ruling
      14), and the float32 breakage rounds the value, since SQLite ignores `Float(32)` (ruling 15);
  - the test times, every removed test, and the five suites' counts.
- **Then 08.** Its note is written after this review.
