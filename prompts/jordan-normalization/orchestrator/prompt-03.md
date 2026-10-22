# Orchestrator — prompt 03, the sampling axis

Read [`../README.md`](../README.md) first: §0.3 item 4, §2.2 (03's charter), §5, §6.0 R4 and R12,
§6.2 P3 and P8. Read 01's and 02's logs too, in particular 02's "State handed to the next prompt".
The conventions of [`prompt-02.md`](prompt-02.md) §0 bind this prompt as well, and are restated
below where 03 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-the-sampling-axis.md`](../03-the-sampling-axis.md)
**Board items:** Z1–Z7 · **Gate:** "Axis" · **Closes:** three (the prompt's) · **Opens:** one, by
ruling 9 (and anything else the work finds) · **Model:** Opus, as the prompt recommends ·
**Network:** none.

**Gate:**
- 02 landed (`abccc23`) and reviewed (`7055532`); the gate "Rename" holds (board §2). `fd4e4fd`
  (housekeeping on the `run-integrity` board and the index) changes no code:
  `git diff --stat abccc23 fd4e4fd -- . ':!prompts' ':!.documents'` is empty. `HEAD` is the commit
  that adds this note, on `jordan-normalization`, with `git status` clean.
- The prompt's own gate: no tracked change, the five suites at 02's counts, no ChamPBH Ray process.
- One prompt at a time in this checkout. 04 is not dispatched until 03 is reviewed.

## 0. What makes this prompt unusual

**It changes a key that every value table joins on, and it moves the samples.** The prompt says
that the change in values is at the 10⁻¹⁵ level and that more than 10⁻¹² in any field is a stop.
On this tree that cannot hold as written (ruling 1). The orchestrator measured three things at
`fd4e4fd` before writing this note, with probes in the session scratchpad (not committed):

- **The production grid is not a set of exact multiples of ln 10/250.** `main.py` (and the golden
  history, which copies it) builds `np.logspace(0, 85, 21250)`: 21250 points spaced
  85/21249 = 0.0040001882 decades, not 1/250. Against k·Δ (Δ = ln 10/250 = 9.2103 × 10⁻³) the point
  of index k sits k × 1.88 × 10⁻⁷ decades high: 1.7 × 10⁻³ in ln(1 + z) at k = 4000, and a full Δ at
  the top. So 03's grid moves every sample but the first.
- **The sensitivity of a sample to the last bit of its position.** Two runs on the old code, one
  with z = `expm1(kΔ)` and one with z = `exp(kΔ) − 1` (k = 0 … 21250), differ in N_backward by at
  most 1.1 × 10⁻¹⁶. At β = 1.5 every field agrees bitwise. At β = 5.5 one sample (index 4803)
  differs, and there the relative difference is 1.6 × 10⁻¹⁶ in `raw_N`, 2.1 × 10⁻¹³ in φ,
  2.6 × 10⁻¹³ in π and `friction_term`, and **6.6 × 10⁻¹²** in `reflecting_term` (a steep wall,
  1.35 × 10⁻¹⁹ there). A field-wise 10⁻¹² threshold therefore fails on a single ulp of position.
- **ln(1 + z_J) along the golden histories** (computed on the old code as Z5 will compute it, with
  φ₀ the φ of the z = 0 sample): equal to 0.0 exactly at z = 0; against
  ln(1 + `cosmology.z`(T_J)) − ln(1 + `cosmology.z`(T_J,end)) the largest difference is
  **3.27 × 10⁻⁷** at β = 1.5 (sample 2167, ln(1 + z_E) = 5.10, near the QCD transition) and
  **3.69 × 10⁻⁸** at β = 5.5 (sample 4349, ln(1 + z_E) = 5.01). Inside the samples with H_J < 0
  (448 and 1496 of them) it is smaller: 1.55 × 10⁻⁷ and 2.37 × 10⁻⁸. ln(1 + z_J) is not monotonic
  in either history. Entropy conservation in the Jordan frame holds through the bounces as well,
  as it should.
- **Format 4's pin**, predicted by recomputing `spec_digests(factories)` with only `redshift`'s leaf
  renamed: 17 pairs, 14 distinct digests, and only `redshift`'s digest moves.

**Baselines** at `abccc23` (02's review), serially at load 19–64: CosmologyModels **18**,
ComputeTargets **61 + 118**, ObjectFactories **79**, tools **84**, RunDrivers **61**, all passing.
The golden history took about 11 s. The machine's load was 80–120 while this note was written;
other workloads share it.

**Fourteen statements in the prompt are loose, silent or wrong for this tree. They are ruled here.**

1. **The change in values (§2 item 2) is measured in two parts.** The prompt's comparison of
   matched samples mixes two changes: the code's (N_backward read from the axis), and the grid's
   (the points move, ruling 3). The two are separated.
   - **Part A, the code at fixed positions.** In a `git worktree` at the parent of the agent's
     commit (in the agent's scratch directory, removed afterwards), run the golden histories with
     the grid z = `math.expm1(k * DELTA)`, k = 0 … 21250, DELTA = `math.log(10.0) / 250`, by a scratch
     script that replaces `golden_history.make_z_grid`. On the agent's tree, run them on the axis
     grid of the same k. Save both sets of samples (all 17 old fields) and compare them by index.
     **The rule:**
     - the sample counts are equal;
     - at every sample whose `raw_N` is bitwise equal on the two trees, every field is bitwise
       equal;
     - where `raw_N` differs, it differs by at most 4 ulp of `final_N`.

     Anything else is a stop. Record the number of samples whose `raw_N` differs, and the largest
     relative difference of each field over them, with its index. These are expected to be few,
     with relative differences of up to about 10⁻¹¹ in `reflecting_term` (§0). Relative
     difference means |a − b|/max(|a|, |b|), and 0 where both are 0.
   - **Part B, the grid.** Record, without a threshold: the old grid's point count and spacing
     against the new one's; the largest displacement of a point in ln(1 + z), with where it
     occurs; and each golden history's `n_samples` in `golden_history_01.json` against the new
     file. The orchestrator expects 2722 → **2723** at β = 1.5 (the new grid is a little denser)
     and 4894 → **4894** at β = 5.5.
   - **The prompt's §4 stop "or the sample count changes"** applies to Part A only.

   Classify as `STRUCTURALLY REQUIRED`, citing this note.

2. **The golden history** (P3, the prompt's "golden history" bullet).
   - `ComputeTargets/tests/golden_history.py` builds its grid with ruling 3's helper, at
     `SAMPLES_PER_LOG10_Z = 250` and `LOG10_ONE_PLUS_Z_HIGH = 85`. 85 is `exponential.yaml`'s
     value, which production runs with until 08. `LOG10_ONE_PLUS_Z_LOW` goes. The module
     docstring's grid sentence says so.
   - `record_of` pins the label of each sample as `log_one_plus_z`: a `log_one_plus_z_sha256` in
     place of `z_sha256`, and `"log_one_plus_z"` (float.hex) in place of `"z"` in each excerpt.
     `first_difference` compares that key. `sample_fields` gains the new field (ruling 8).
   - `GOLDEN_PATH` names `data/golden_history_03.json`. `golden_history_01.json` is never written
     and is not deleted. The provenance's `code_tree` says that the file was generated from
     jordan-normalization prompt 03's working tree, before its commit, on top of `HEAD`;
     `generated_at_commit` stays `git rev-parse HEAD`, as the generator writes it. `setup`
     follows (`log10_one_plus_z_high` 85; no "low").
   - `test_golden_history.py`'s docstring names the new file. It says that 03 regenerated the
     record and 01's is kept as the record of 01 and 02. The cost line stays true.
   - The JSON is written **once**, by `PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history`
     after every code change is final, and then `test_golden_history` is run alone and passes.

   Classify as `STRUCTURALLY REQUIRED`.

3. **The grid (Z3).**
   - **One helper**, `log_one_plus_z_grid(samples_per_log10_z: int, log10_one_plus_z_high: float) -> list[float]`
     in `CosmologyConcepts/redshift.py`, returning `[k * DELTA for k in range(K + 1)]` with
     `DELTA = math.log(10.0) / samples_per_log10_z` and `K = int(round(log10_one_plus_z_high * samples_per_log10_z))`,
     each point an `int` times a `float`, never accumulated. Every builder of a grid uses it:
     `main.py`, `ComputeTargets/tests/golden_history.py`, `ComputeTargets/tests/test_fixed_T_values.py`
     (35 decades, as now) and `tools/history_and_bbn.py`'s `_z_grid` (35 decades, as now; its
     `LOG10_ONE_PLUS_Z_LOW` goes). `tools/surfing_scan.py` and `test_scalarhistory_failure_reason.py`
     import `_z_grid` and follow it without an edit. `tools/history_and_bbn.py` and
     `test_fixed_T_values.py` are not in the prompt's §5; this ruling adds them.
   - **The option names.** The prompt and P8 say `--samples-per-log10-z`. The option is
     `--samples-log10-z` (`config/argument_parser.py:105`, read as `args.samples_log10_z`). Keep
     it as it is: P8's point is that no option is renamed.
   - **The default.** `DEFAULT_LOG10_ONE_PLUS_Z_HIGH` is **35** in the parser on this tree, not 85.
     85 is `exponential.yaml`'s. It becomes 300. The log says that the old default was 35, and
     what an 85 in the YAML now means: K = 21250, the top point at ln(1 + z) = 195.7, the same
     density as before.
   - **The refusal.** `--log10-one-plus-z-low` gets a `type=` function beside `non_negative_float`.
     It accepts only 0, and otherwise raises `argparse.ArgumentTypeError`, with a message that the
     axis is anchored at the end of the history. The help says so. `main.py` no longer reads the
     option. A new test beside note 02's `--drop` refusal, in
     `ObjectFactories/tests/test_datastore_config.py`, checks two things: that
     `["--database", "x", "--log10-one-plus-z-low", "1"]` exits `SystemExit(2)` with the option's
     name in stderr; and that `"0"` parses to 0.0 and the default high is 300.
   - `main.py`'s `num_z_samples` arithmetic, its `logspace`, and the comment about 6 × 10³⁵ go. The
     payload of `convert_to_redshift` is `{"log_one_plus_z": x}`.

   Classify the helper and its users as `IMPLEMENTATION CHOICE`, citing this note; the rest as
   `STRUCTURALLY REQUIRED`.

4. **The concept (Z1).**
   - `redshift(store_id, log_one_plus_z)`. There is no `z=` keyword, so a stale caller fails
     loudly. The constructor raises `ValueError` on a negative or NaN `log_one_plus_z`.
   - `z` is a read-only property: `math.expm1(log_one_plus_z)`, with `OverflowError` caught and
     `float("inf")` returned. It prints no warning.
   - **Equality and hashing stay on `store_id`**, as they are today. The prompt's "ordering,
     equality, hashing … work on `log_one_plus_z`" means the places that read z, and equality and
     hashing read none. `__lt__`, `redshift_array`'s sort and every truncation mode compare
     `log_one_plus_z`.
   - **Truncation.** A float argument to `truncate` is an ln(1 + z). The tolerance in
     `redshift.py` becomes the axis constant of ruling 5 (10⁻¹⁰) in place of
     `DEFAULT_FLOAT_PRECISION` (10⁻⁷, which other factories keep). The docstrings say both. The
     log says that a grid point at most 10⁻¹⁰ beyond `final_N` is kept, and so sampled at
     N_forward ≥ −10⁻¹⁰. It also says whether that happens in either golden history (the
     orchestrator expects not).
   - **`float(z)`.** The callers on this tree are `plot_ScalarHistory.py:1294` (ruling 10) and
     `redshift_array.as_float_list`, which has no caller (`git grep -n 'as_float_list'`). The log
     lists the grep for `float(`, and for `math`/`numpy` calls on a redshift object, with what it
     found.

   Classify as `STRUCTURALLY REQUIRED`.

5. **The factory (Z2) and issue 09.**
   - **The tolerance.** `config/defaults.py` gains `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10`.
     `DEFAULT_REDSHIFT_PRECISION` and `DEFAULT_REDSHIFT_RELATIVE_PRECISION`, read only by
     `ObjectFactories/redshift.py`, are deleted.
   - **The filter** is one branch,
     `(table.c.log_one_plus_z > x - tol) & (table.c.log_one_plus_z < x + tol)`. That is
     |Δ| < tol, written as a range so that SQLite can use the column's index. `abs(col − x)` scans
     the table, and an H = 300 grid has 75 001 rows on every shard. The payload key is
     `"log_one_plus_z"`. `read_table` selects and orders by it.
   - **The cost, measured** (in the log, not a test). On a temporary store through the stand-in
     actor, time `build` for the 75 001 points of a 300-decade grid, first into an empty table and
     then again over the filled one. Give `EXPLAIN QUERY PLAN` for the lookup, which should name
     the index.
   - **Tests**, in a new `ObjectFactories/tests/test_redshift_axis.py` on the `_TempStoreCase`
     harness (`ObjectFactories/tests/test_version_keyed_lookups.py`):
     - issue 09: with a row at −10⁻⁶ written by the table's inserter (the class refuses it), a
       lookup at 0 does not match it and inserts a new row. With a row at 10⁻¹², a lookup at 0
       returns that row's serial. The old filter matched −10⁻⁶ (`abs` of a true comparison);
     - the collision: the first three grid points, built through the factory, get three distinct
       serials, and building them again returns the same three;
     - `read_table`: rows come back in ascending `log_one_plus_z`; with a `model_proxy` (a stub whose
       `get()` returns an object with `store_id`), only the redshifts of that history's value rows
       come back. 02's review found that no test exercised this join, and the factory has no
       caller of `read_table` on this tree (`git grep -n '\.read_table('`). This test is its only
       witness.
   - **The spec** is `InventorySpec(leaves=("log_one_plus_z",))`, with its comment.
   - **Format 4.** `FINGERPRINT_FORMAT = 4`, and a format-4 paragraph in the form of the first
     three. `SPEC_DIGESTS[4]` is generated by `spec_digests(factories)` and never typed. **Expected:**
     17 pairs, 14 distinct digests, and only `redshift`'s digest moved. `[1]`–`[3]` are byte-identical
     to `HEAD`'s. The new golden file is `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-03.json`,
     written once as 02's was, and `GOLDEN` names it. The format literal in
     `TestCommandLine.test_store_fingerprint` (`test_store_fingerprint_golden.py:336`) goes from 3 to
     4. The older golden files are not touched, and `RunDrivers/tests/test_registered_run_fingerprint.py`
     is not edited.

   Classify as `STRUCTURALLY REQUIRED`, except the range form and the timing, which are
   `IMPLEMENTATION CHOICE` citing this note.

6. **The two other factories that read the redshift.** `ObjectFactories/AdiabaticHistory.py`
   (`:205-226`) and `ObjectFactories/BBNData.py` (`:234-263`) select `redshift_table.c.z`, order by
   it and build `redshift(…, z=row.z)`. They move to `log_one_plus_z` exactly as
   `ObjectFactories/ScalarHistory.py` does. They are not in the prompt's §5; this ruling adds them.
   `AdiabaticHistoryValue.build`'s `inserters["redshift"](conn, z, …)` (`:450`) is not changed;
   ruling 11 records it. Classify as `STRUCTURALLY REQUIRED`.

7. **The sampling (Z4).**
   - The truncation goes into a module-level helper in `ComputeTargets/ScalarHistory.py`, which
     takes the grid and `final_N` and returns the cut grid, so that the prompt's §2 item 3 can
     exercise it with a synthetic `final_N`.
   - **The "grid too short" `raise` stays inline in `compute_scalar_history`.**
     `test_integrator_exceptions.test_g_one_runtime_error_in_the_integration_path` requires
     exactly one `RuntimeError` raise there, whose message contains `"largest supplied redshift"`.
     The message keeps that phrase and gives the grid's largest ln(1 + z) and the N required. That
     test is not edited.
   - A test drives that error through `compute_scalar_history._function`. It uses the golden
     β = 1.5 setup (about 0.6 s, and its docstring says so) with a 5-decade grid, which ends at
     N = 11.5 short of `final_N` ≈ 25.1, and asserts the `RuntimeError` and its phrase.
   - The log lists every removed expression (`largest_z = exp(final_N) - 1.0`,
     `log(1.0 + max_z.z)`, `log(1.0 + z.z)`, and any other).
   - `z_init_estimate = cosmology.z(T_init)` (`:1170`) is the cosmology's redshift of a
     temperature, not a label. It is not changed, and the log says so.

   Classify the helper as `IMPLEMENTATION CHOICE` citing this note, the rest `STRUCTURALLY REQUIRED`.

8. **ln(1 + z_J) (Z5).**
   - **The expression, exactly:** before the sample loop, `log_A_0 = coupling.log_Omega(phi_0)`
     with `phi_0 = StateVector._make(solution(final_N)).phi_Einstein`; in the loop,
     `log_one_plus_z_Jordan = N_backward + (log_A_0 - log_Omega)`, with `log_Omega` the loop's
     existing local. (`coupling.log_Omega` is ln A: ρ_R,J = ρ_R,E/A⁴ at `:1281`.) At the k = 0 sample
     N_forward = `final_N - 0.0` = `final_N`, so the value there is 0.0 exactly.
   - **Its place.** `log_one_plus_z_Jordan` is the **last** field of `SampleValues`, so the other 17
     keep their order. `ScalarHistoryValue` gains it; `ScalarHistory`'s stored-values loop, the
     factory's `store` and its value read carry it; the value table gains
     `sqla.Column("log_one_plus_z_Jordan", sqla.Float(64), nullable=False)` after `kicking_term_Mp`.
     `tools/history_and_bbn.py` builds `ScalarHistoryValue(None, z, **SampleValues…)` and follows
     without an edit. `ModelFunctions` does not change.
   - **The docstring** says it is a column and never a key (H_J < 0 in a bounce, so a_J is not
     monotonic), and that "from the end" will mean T_J = T_CMB once 07 lands.
   - **Tests**, in a new `ComputeTargets/tests/test_sampling_axis.py` (it sorts into `[g-z]`), on the
     golden β = 1.5 history, run once in `setUpClass` (about 0.6 s, said in the docstring):
     - **the identity:** at every sample, `log_one_plus_z_Jordan` equals
       `z.log_one_plus_z + (coupling.log_Omega(phi_0) - coupling.log_Omega(s.phi_Einstein))`
       by `assertEqual`, with φ₀ the k = 0 sample's φ; and the k = 0 value is 0.0;
     - **the Jordan redshift:** at every sample, |ln(1 + z_J) − (ln(1 + `cosmology.z`(T_J)) −
       ln(1 + `cosmology.z`(T_J,end)))| < **10⁻⁶**, with T_J,end the z = 0 sample's.
   - **The log** gives that difference's maximum for both golden histories, with its index and
     ln(1 + z_E). It gives the maximum separately over the samples with H_J ≥ 0 and with H_J < 0. It
     is expected to equal §0's numbers to a few digits.
   - **The prompt's third §4 stop is replaced:** more than 10⁻⁶ at **any** sample of either golden
     history is a stop, bounces included. §0 shows that the agreement holds through them.

   Classify as `STRUCTURALLY REQUIRED`.

9. **`ScalarHistory.functions`: not changed; an issue is opened.** `_create_functions`
   (`ComputeTargets/ScalarHistory.py:1655-1667`) builds each spline with z as the abscissa
   (`v.z.z`). It wraps them in `ZSplineWrapper(log_z=True)`, which evaluates the spline at
   ln(1 + z). It has no caller (`git grep -n -E '\bfunctions\b'` outside the module finds none).
   After this prompt, `v.z.z` is `inf` beyond 709.8 e-folds as well. Leave it as it is. In the same
   commit, open **`[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`** on this
   board's §3, with three parts:
   - *What*, with the lines and the grep;
   - *Impact*: none today, since there is no caller; a caller would get wrong values, and `inf`
     abscissae past 709.8 e-folds;
   - *Next step*: delete `functions`, `ModelFunctions` and `ZSplineWrapper` if they are still
     unread, or build on `log_one_plus_z`; for 07, which rewrites `ScalarHistory`'s stored shape.
     Not assigned.

   Add the row to `.documents/OPEN_ISSUES.md` §1.16. Classify the issue as `IMPLEMENTATION CHOICE`,
   citing this note.

10. **The readers (Z6).**
    - **`extract_common.py`.** The driver calls `plot_ScalarHistory` only with
      `x_coord="efolds"` (`plot_ScalarHistory.py:1581`), so no new axis mode is added: it would be
      behaviour with no caller, and 08 owns `extract_common`'s output. `get_x_coord`'s "redshift"
      branch keeps `1.0 + value.z.z`, which is now computed from the axis and is `inf` past float
      range. `_find_T_event_times`'s records gain `"log_one_plus_z"` beside `"z"`. That is what
      "offers ln(1 + z)" means here.
    - **`plot_ScalarHistory.py`'s frame** (`write_csv_file`): `"z": value.z.z` (no longer
      `float(value.z)`, which would now be ln(1 + z)); new columns `"log_one_plus_z"` and
      `"log_one_plus_z_Jordan"`, with `nan` in the empty row; the frame sorted by `"log_one_plus_z"`.
      `CodeComparisonTools/CompareMathematica.py` reads the frame's `"z"` and is not changed.
      `get_x_coord`'s `value.z.z` stays.
    - **`tools/bbn_from_store.py`:** `order by r.log_one_plus_z desc`, and the comment above it.
      Its test, `ComputeTargets/tests/test_bbn_from_store.py` (`:303`, `:337-338`), builds the table
      by hand. That table becomes `(serial integer primary key, log_one_plus_z real)`, with the
      `math.log1p` of the same three z, and its order assertion is unchanged.
    - **`tools/inventory_report.py`:** `_value_lines` no longer maps the leaf to `values`, so the
      redshift line reads `log_one_plus_z = [ … ]`. A reader must not take ln(1 + z) for z.
      `test_inventory_report.py`'s expectation (`:364`) follows, and its rows are written as
      `{"log_one_plus_z": value}` with the same numbers.
    - **The other tests that write redshift rows by column:** `test_store_fingerprint_golden.py:166`
      (`{"log_one_plus_z": value}`, the same five numbers) and `tools/tests/test_compare_stores.py:904-905`
      (`log_one_plus_z=math.log1p(1e12)` and `math.log1p(1e9)`).
    - **`ComputeTargets/AdiabaticHistory.py` (`:278`) and `ComputeTargets/BBNData.py` (`:454`)** use
      the redshift only as an object in a grid. The log says so, with these lines and their
      `plot_ScalarHistory.py` counterparts (`:1095`, `:1279-1283`, by `store_id`).

    Classify as `STRUCTURALLY REQUIRED`.

11. **Z7: the path is deleted.** `sqla_ScalarHistoryValue_factory.build` is reached only through
    `object_get("ScalarHistoryValue", …)` (DatastoreKit's `Datastore.py:565`), and nothing calls that
    (`git grep -n '"ScalarHistoryValue"'`: the registry, sharding, drop group, inventory map and the
    parent factory's tables). The value rows are written by `sqla_ScalarHistoryFactory.store` and
    read by its `build`. `build` is abstract in `SQLAFactoryBase`, so it cannot be removed:
    - its body becomes one `raise NotImplementedError(...)`, saying that `ScalarHistoryValue` rows
      are written and read only through `sqla_ScalarHistoryFactory`;
    - the φ/π comparison, and `DEFAULT_FLOAT_PRECISION`'s import if it falls unused, go with it;
    - a test in `ObjectFactories/tests/test_redshift_axis.py` checks that the call raises
      `NotImplementedError`, naming `sqla_ScalarHistoryFactory`;
    - this closes both `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` and
      `[05-the-value-factory-compares-stored-phi-against-pi]`.

    `AdiabaticHistoryValue`'s and `BBNDataValue`'s `build`s are in the same state. They, and
    ruling 6's inserter call, go to "Observations not acted on", with no issue opened. Classify
    as `STRUCTURALLY REQUIRED` (the prompt's own alternative).

12. **The breakages**, each recorded as a diff that `git apply` replays from the agent's commit:
    1. `log_one_plus_z_grid` accumulated (`x += DELTA`). It fails the grid test in
       `test_sampling_axis.py`, which compares the helper's points bitwise with `k * DELTA`, and the
       golden history;
    2. `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-2`. It fails the collision test (adjacent points are
       9.2 × 10⁻³ apart);
    3. `log_A_0` dropped from Z5's expression. It fails the identity test, and the `cosmology.z`
       test.

    For each, the log names the tests that fail and how.

13. **The records.**
    - **Boards.** On `dk-and-rpk-migration`'s board, `[01-…]` and `[09-…]` move from §3 to §4, each
      with a dated **Resolved** line naming this prompt and "this commit". On `science-readiness`'s
      board, `[05-…]` gets the same. That board keeps its §3 entries as bullets; a resolved one
      moves to §4 in the form §4 already uses. This board: row 03, Z1–Z7, the gate "Axis", §3.1's
      three rows marked closed (as 01's is), §3's new issue (ruling 9), §4, and
      `### 5.4 After prompt 03 (<date>)`.
    - **`.documents/OPEN_ISSUES.md`.** Delete the three rows from §1.15 and add ruling 9's row to
      §1.16. The count goes **27 → 25**. In the header, `dk-and-rpk-migration`'s "3 on" becomes
      "1 on" and `science-readiness`'s "2 on" becomes "1 on"; each clause says that
      jordan-normalization prompt 03 closed them, with the date. The `jordan-normalization`
      clause adds "prompt 03 closed three of them, leaving four, and opened one, <date>". Set
      **Last updated**.
    - **`CLAUDE.md`**: the "Redshift" bullet only (the prompt's text). Its suite counts are 10's (C2).
    - **Not edited:** `.documents/architecture-summary.md` and `numerical-strategies.md` (10's C1
      writes the axis into them); `exponential.yaml` (08's); `prompts/INDEX.md`.

14. **The checks that run a driver**: `main.py --help` (exit 0, the high default 300 in the help),
    and the refusal `PYTHONPATH=. ./venv/bin/python main.py --database <scratch>/absent.db --log10-one-plus-z-low 1`
    (exit 2, naming the option, before Ray and before any store, creating no file). These are the
    only driver invocations allowed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only. Show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Order.** The five suites and the golden test at the start, before any edit. Part A's old-tree
  run (ruling 1) next, from a worktree, so that it is done before the code changes. Then the
  edits. Then the regeneration (ruling 2), Part A's new-tree run and the comparison, the suites,
  the breakages, the drivers, and the records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its
  two halves. Send output to a file in the agent's scratch directory, grep the verdict from it,
  and record `uptime` beside each. A failure is re-run alone before it is called real.
- **An existing test that fails for a reason this note does not name is a stop**, never a reason
  to edit it. That includes a test whose pinned number came from a sample that the grid moved;
  send the orchestrator the test, the old value and the new.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Worktrees are removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened except the tests' temporary ones and ruling 5's timing
  store, which is also temporary and in the scratch directory. No Ray is started. No driver runs,
  except ruling 14's two. Nothing is written to `var/`.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.
- **Not touched**, though seen: `extract_common.py:421`'s `if "x_coord" == "efolds":` (a string
  literal, always false) and the redshift branch's `1 + z` against `get_xpos_attr`'s `z`. These are
  cosmetic, in a mode the driver never uses, and the orchestrator offers them to the user as
  housekeeping. The agent lists them under "Observations not acted on", with no issue.

## 1. Before you dispatch

Done at `fd4e4fd`, before this note was committed:
1. **The tree.** `jordan-normalization`, `git status --short` empty; no code change since `abccc23`.
2. **Ray.** `ray_cluster.sh status`: no processes, ports free.
   `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: no runs.
4. **The probes of §0**, in the session scratchpad.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README, this board, and 01's and 02's logs;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/` or prompts 04–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/03-the-sampling-axis.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change** are the prompt's §5, with these additions:
  - ruling 3's `tools/history_and_bbn.py` and `ComputeTargets/tests/test_fixed_T_values.py`;
  - ruling 6's two factories;
  - ruling 10's tests;
  - the two new test modules and `golden_history_03.json`;
  - the new fingerprint golden file;
  - the two other boards and `.documents/OPEN_ISSUES.md`.

  Nothing is deleted.
- **§0's fourteen rulings** bind it, each classified as §0 says.
- **`golden_history_01.json` is never written.**
- **It starts no Ray, opens no store but a temporary one, and runs no driver but ruling 14's.**
- **Stop and ask** on any of the prompt's §4 conditions as ruling 1 and ruling 8 restate them, or
  on an existing test failing for a reason §0 does not name.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - Part A's comparison (the counts, the samples whose `raw_N` differs, each field's largest
    relative difference there) and Part B's table;
  - the golden histories' new `n_samples` and the golden test's time;
  - the Jordan-redshift measurement for both histories, partitioned as ruling 8 says;
  - the 75 001-point timing and the query plan;
  - every test added or changed, with what it asserts;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the format-4 pin's pair and distinct-digest counts, and which digest moved;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists only §2's files, with no deletion. `git status` is
   clean, and `git worktree list` shows only the main checkout.
2. **Part A, again.** The orchestrator reruns ruling 1's comparison from its own scratch script,
   with the old tree in a temporary worktree at `<agent>~1`. It applies the three-part rule
   itself. Its counts and largest differences agree with the log's.
3. **The golden history.** Run the generator at `<agent>` with its output redirected into the
   scratchpad, and compare every pinned item with `golden_history_03.json`. `golden_history_01.json`
   is byte-identical to `<agent>~1`'s. `n_samples` is 2723 and 4894, or the log explains it.
   Run `test_golden_history` alone, with its time.
4. **ln(1 + z_J).** Rerun §0's measurement on `<agent>`'s samples, read from the field and no
   longer computed by the probe. The maxima agree with §0 to a few digits, and the k = 0 value
   is 0.0. The identity test passes, and the test of ruling 8 asserts `assertEqual`.
5. **The axis.** The 400-decade test exists, truncates at N = 800, and reads `z` without raising
   (`inf` above 709.78). `python -W error` on that test module raises no warning. The truncation
   helper is what `compute_scalar_history` calls, and the "too short" raise is inline, with its
   test.
6. **The factory.** The filter is the range of ruling 5 and is the only branch. The orchestrator
   runs `EXPLAIN QUERY PLAN` itself on a temporary table and sees the index. Issue 09's test,
   the collision test and `read_table`'s test exist and pass. As a probe, put back the old
   `abs((col - x) < tol)` form at x = 0 alone; issue 09's test must fail. Then revert the probe.
7. **Format 4.** `FINGERPRINT_FORMAT` 4. `SPEC_DIGESTS[1]`–`[3]` are byte-identical to
   `<agent>~1`'s. `SPEC_DIGESTS[4]` equals `spec_digests(factories)`, recomputed: 17 pairs, 14
   distinct digests, only `redshift` moved. The new golden file is format 4, with `taken` null
   and no problems, and the older ones are unchanged.
8. **The readers.** `git grep -n -P '\.z\b(?!_)|c\.z\b|"z"|float\('` over the code, read against
   rulings 4, 7, 9 and 10. No reader passes a redshift to `float`. Every SQL `order by` or
   `.order_by` on the redshift uses `log_one_plus_z`. `plot_ScalarHistory`'s frame has the three
   columns. `main.py --help` and ruling 14's refusal behave as stated.
9. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
   says, revert, and check that `git status` is clean.
10. **The records and the suites.** The three boards, the index (25 open, the clauses, the date),
    `CLAUDE.md`'s bullet, and the log in §5.1's form with every deviation classified. Then the five
    suites at `<agent>`, serially, at the counts the log states, and `black --check` on every
    changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03* paragraph in §5.4. Fix
  small residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log, the two other boards, the index);
  - add this note to the board's list of orchestrator notes, marked "used for 03";
  - update the status line: 3 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the comparison in two parts, because the old grid was not exact multiples (ruling 1);
    - the option keeps its real name, `--samples-log10-z`, and the old default was 35, not 85
      (ruling 3);
    - equality stays on `store_id` (ruling 4);
    - the range filter (ruling 5);
    - the two other factories (ruling 6);
    - the Jordan-redshift stop at 10⁻⁶ everywhere (ruling 8);
    - the `functions` issue (ruling 9);
    - Z7 by deletion (ruling 11);
  - Part A's result, and the samples that moved;
  - the five suites' counts;
  - **separately, as housekeeping and not 03's:** `extract_common.py:421`'s string-literal
    comparison, which makes text labels in an e-fold plot use the redshift branch's placement.
    Offer to fix it in a standalone commit.
- **Then 04.** Its note is written after this review.
