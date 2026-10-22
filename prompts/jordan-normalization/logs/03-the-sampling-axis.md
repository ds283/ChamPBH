# Log 03 — the sampling axis

**Subject:** Store the sampling axis as ln(1 + z) and add ln(1 + z_J) to each sample
**Commit:** `bd9ae42`, on branch `jordan-normalization`, on top of `9997eef`
**Date:** 2026-10-22, by Claude Opus 5.5
**Result:** **COMPLETE**, after one stop (Deviation 15), which the orchestrator ruled.
- Z1–Z7 shipped, under orchestrator note 03, whose fourteen rulings govern where they differ from
  the prompt.
- The golden history was regenerated as `golden_history_03.json`, and 01's file is untouched.
- Part A of the change in values holds ruling 1's rule.
- ln(1 + z_J) equals the identity exactly. It agrees with `cosmology.z`(T_J) to within 3.1e-7 at
  every sample, bounces included.
- Counts: CosmologyModels 18, tools 84 and RunDrivers 61 are unchanged. ComputeTargets went from 179
  to 188 (+9) and ObjectFactories from 79 to 86 (+7).
- All three deliberate breakages were caught.
- One issue opened and three closed.

## What shipped

- **Z1 — the concept** (`CosmologyConcepts/redshift.py`).
  - **The class.** `redshift(store_id, log_one_plus_z)`. There is no `z=` keyword. A negative or NaN
    `log_one_plus_z` raises `ValueError`.
  - **Reading z.** `log_one_plus_z` is a read-only property. So is `z`, which is
    `math.expm1(log_one_plus_z)`, returning `float("inf")` on `OverflowError`, with no warning.
  - **`float()`.** `__float__` returns ln(1 + z), and its docstring says that this changed.
  - **Ordering.** `__lt__`, `redshift_array`'s sort and all six truncation modes compare
    `log_one_plus_z`, with tolerance `DEFAULT_LOG_ONE_PLUS_Z_PRECISION` (1e-10) in place of
    `DEFAULT_FLOAT_PRECISION` (1e-7). `truncate`'s docstring says both, and says that a float limit
    is an ln(1 + z).
  - **Equality and hashing** stay on `store_id`.
  - **The grid helper** `log_one_plus_z_grid(samples_per_log10_z, log10_one_plus_z_high)` returns
    `[k * DELTA for k in range(K + 1)]`.
- **Z2 — the factory and format 4.**
  - **`ObjectFactories/redshift.py`.**
    - The column is `log_one_plus_z` (`Float(64)`, indexed), and the table has no `z`.
    - `build` reads the payload key `"log_one_plus_z"` and filters in one branch:
      `(log_one_plus_z > x - tol) & (log_one_plus_z < x + tol)`.
    - `read_table` selects and orders by `log_one_plus_z`.
    - The spec is `InventorySpec(leaves=("log_one_plus_z",))`, with its comment.
  - **`config/defaults.py`.** It gains `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10`.
    `DEFAULT_REDSHIFT_PRECISION` and `DEFAULT_REDSHIFT_RELATIVE_PRECISION` are deleted, since
    nothing else in the code read them (`git grep`).
  - **`config/fingerprint.py`.**
    - `FINGERPRINT_FORMAT = 4`, with a format-4 paragraph and an extended count paragraph.
    - `SPEC_DIGESTS[4]` was pasted by a scratch script from `spec_digests(factories)`, never typed:
      **17 pairs, 14 distinct digests; only `redshift` moved**, from `3e69e5a9…2ffb` to
      `b6e92df7…5805`. The other sixteen pairs equal format 3's.
    - `[1]`–`[3]` are unchanged: the diff removes only `FINGERPRINT_FORMAT = 3` and extends one
      comment line.
  - **The golden fingerprint.** It was written once by a scratch probe that asserts the file is
    absent, as `json.dumps(fingerprinter.take(primary, None)[0], indent=2, sort_keys=True) + "\n"`:
    - format 4, `taken` null, `problems` `{}`, 17 classes;
    - digest `05d4b92416ff49f2ed5281b4deef44304421738d03d5b1fe4d4bac1822cb07cf`;
    - SHA-256 `552b227679dfbae4c598cf6d1ae3977ec2501b9943e308ab18e4710e2ed9af6e`;
    - `grep -c golden-` on it gives 0.

    The older golden files are untouched. `RunDrivers/tests/test_registered_run_fingerprint.py`
    is not edited and passes.
- **Z3 — the grid.**
  - **Builders.** `main.py`, `ComputeTargets/tests/golden_history.py` (85 decades),
    `ComputeTargets/tests/test_fixed_T_values.py` (35) and `tools/history_and_bbn.py`'s `_z_grid`
    (35) all build with `log_one_plus_z_grid`. `LOG10_ONE_PLUS_Z_LOW` is gone from both modules.
    `tools/surfing_scan.py` and `test_scalarhistory_failure_reason.py` import `_z_grid` and follow
    it without an edit.
  - **`main.py`.** The `num_z_samples` arithmetic, the `logspace` and the comment about 6 × 10³⁵ are
    gone, and `main.py` no longer reads `args.log10_one_plus_z_low`. `convert_to_redshift`'s payload
    is `{"log_one_plus_z": x}`.
  - **`config/argument_parser.py`.**
    - `DEFAULT_LOG10_ONE_PLUS_Z_HIGH` is 300 (**it was 35**), and the help shows "(default 300)".
    - `--log10-one-plus-z-low` has `type=anchored_log10_one_plus_z_low`. It accepts only 0 and
      otherwise raises `argparse.ArgumentTypeError`, saying that the axis is anchored at the end of
      the history; the help says so too.
    - The option names (`--samples-log10-z`, `--log10-one-plus-z-high`, `--log10-one-plus-z-low`)
      are unchanged.
  - **What `exponential.yaml`'s 85 now means.** K = 21250 and the top point is ln(1 + z) = 195.7197,
    at the same density as before (250 per decade). The YAML is not edited; it is 08's.
- **Z4 — the sampling** (`ComputeTargets/ScalarHistory.py`).
  - **The truncation** is the module-level helper `truncate_to_history(z_grid, final_N)`
    (`:1120`), that is, `z_grid.truncate(final_N, keep="lower")`.
  - **"Grid too short".** The check stays inline in `compute_scalar_history`. It compares
    `max_z.log_one_plus_z < final_N`, and its message (`:1274`) keeps "largest supplied redshift"
    and gives the grid's largest ln(1 + z) and the N required.
  - **N_backward** is `z.log_one_plus_z` (`:1293`).
  - **Expressions removed:** `largest_z = exp(final_N) - 1.0`;
    `z_grid_cut = z_grid.truncate(largest_z, keep="lower")`, replaced by the helper on `final_N`;
    `max_N: float = log(1.0 + max_z.z)`; `if max_N < final_N:`, replaced by the comparison of axis
    values; `N_backward = log(1.0 + z.z)`; and the message's `z={max_z.z:.3g}` and
    `N={max_N:.3g}`. No `exp` or `log` of a label remains.
  - **Not changed:** `z_init_estimate = cosmology.z(T_init)` (`:1189`). It is the cosmology's
    redshift of a temperature, not a label.
  - **The edge.** A grid point at most 1e-10 beyond `final_N` is kept, and so is sampled at
    N_forward ≥ −1e-10. **This does not happen in either golden history.** The nearest point above
    `final_N` is 8.6e-3 away (β = 1.5) and 9.0e-4 away (β = 5.5).
- **Z5 — ln(1 + z_J).**
  - **The computation.** Before the sample loop (`:1287-1288`),
    `phi_0 = StateVector._make(solution(final_N)).phi_Einstein` and
    `log_A_0 = coupling.log_Omega(phi_0)`. In the loop,
    `log_one_plus_z_Jordan = N_backward + (log_A_0 - log_Omega)`, with `log_Omega` the loop's
    existing local.
  - **Where it lives.**
    - It is the **last** field of `SampleValues`, so the other 17 keep their order.
    - `ScalarHistoryValue` gains it as a required argument and property.
    - `ScalarHistory.store`'s values loop, the factory's `store` (`ObjectFactories/ScalarHistory.py:715`)
      and its value read (`:433`, `:479`) carry it.
    - The value table gains `sqla.Column("log_one_plus_z_Jordan", sqla.Float(64), nullable=False)`
      after `kicking_term_Mp` (`:901`).
  - **Docstrings.** The comment above `SampleValues` and the docstring of `ScalarHistoryValue` say
    that it is a column, never a key (H_J < 0 in a bounce, so a_J is not monotonic), and that
    "from the end" means T_J = T_CMB once 07 lands.
  - **Unchanged:** `tools/history_and_bbn.py` builds `ScalarHistoryValue(None, z, **SampleValues…)`
    and follows without an edit. `ModelFunctions` does not change.
- **Z6 — the readers.**
  - **`extract_common.py`.** `_find_T_event_times`' records gain `"log_one_plus_z"` beside `"z"`.
    `get_x_coord`'s redshift branch keeps `1.0 + value.z.z`, now derived from the axis (`inf` past
    float range), with a comment. No axis mode was added (ruling 10).
  - **`plot_ScalarHistory.py` `write_csv_file`.**
    - `"z": value.z.z`, in place of `float(value.z)`, which would now be ln(1 + z).
    - The new columns `"log_one_plus_z"` and `"log_one_plus_z_Jordan"`, with `nan` in the empty row.
    - The frame is sorted by `"log_one_plus_z"`.
    - `CodeComparisonTools/CompareMathematica.py` reads the frame's `"z"` and is not changed.
  - **`tools/bbn_from_store.py`.** `order by r.log_one_plus_z desc`, and the comment above it.
  - **`tools/inventory_report.py`.** `_value_lines` no longer maps the leaf to `values`, so the
    redshift line reads `log_one_plus_z = [ … ]`.
  - **Readers that use the redshift only as a key (checked, not changed).**
    - `ComputeTargets/AdiabaticHistory.py:278` (`z_grid.append(value.z)`) and
      `ComputeTargets/BBNData.py:454` (the same): the object goes into a grid that is stored by
      `z_serial`.
    - `plot_ScalarHistory.py:1095` and `:1279-1283` match by `.z.store_id`.
  - **The two factories (ruling 6).** `ObjectFactories/AdiabaticHistory.py` (`:206`, `:216`,
    `:226`) and `ObjectFactories/BBNData.py` (the same three in their value read) select and order
    by `redshift_table.c.log_one_plus_z` and build `redshift(…, log_one_plus_z=…)`.
- **Z7 — the value factory.** Nothing calls `sqla_ScalarHistoryValue_factory.build`.
  `git grep -n '"ScalarHistoryValue"'` finds only the registry, sharding, the drop group, the
  inventory map, the parent's spec and tables, and tests that insert rows. So its body is one
  `raise NotImplementedError(...)` naming `sqla_ScalarHistoryFactory`. The φ/π comparison went with
  it, and so did the imports `DEFAULT_FLOAT_PRECISION` and `fabs`, which fell unused. This closes
  `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` and
  `[05-the-value-factory-compares-stored-phi-against-pi]`.
- **The golden history (ruling 2).**
  - **Generator.**
    - `golden_history.py` builds the grid with the helper at `SAMPLES_PER_LOG10_Z = 250` and
      `LOG10_ONE_PLUS_Z_HIGH = 85`; the module docstring's grid sentence says so.
    - `record_of` pins `log_one_plus_z_sha256` in place of `z_sha256`, and `"log_one_plus_z"` in
      place of `"z"` in each excerpt, and `first_difference` compares that key.
    - `sample_fields` has 18 names.
    - `GOLDEN_PATH` is `data/golden_history_03.json`.
    - The provenance's `code_tree` reads "jordan-normalization prompt 03's working tree, before its
      commit, on top of generated_at_commit". `generated_at_commit` is `9997eef…`, and `setup` has
      `log10_one_plus_z_high` 85 and no "low".
  - **`test_golden_history.py`'s docstring** names the new file and says that 01's is kept as the
    record of 01 and 02.
  - **The run.** The JSON was written once, after the code was final, in 9.5 s (SHA-256
    `f1fbc53543c5019786da0afadb0216674bdbefa52ac6f7fbe9677b178ed1dab5`). `test_golden_history`
    alone then passed: 3 OK in 9.95 s.
  - **01's file.** `golden_history_01.json` was never written (`6898a432…62d6` before and after).
- **`CLAUDE.md`.** The "Redshift" bullet only.
- **The records.**
  - **This board:** row 03, Z1–Z7, the gate "Axis", §3 (the new issue), §3.1, §4, §5.4.
  - **Other boards:** on `dk-and-rpk-migration`, `[01-…]` and `[09-…]` move from §3 to §4 with
    **Resolved** lines; on `science-readiness`, `[05-…]` moves from §3 to §4 with a **Resolved** line.
  - **`.documents/OPEN_ISSUES.md`:** three rows deleted from §1.15 and one added to §1.16; **27 → 25
    open**; the header clauses are corrected and **Last updated** is 2026-10-22.

No Ray was started. No store was opened except the tests' temporary ones and ruling 5's timing
store, which was a temporary directory in the scratch space and was removed. Nothing was written to
`var/`.

## Tests added or changed

**Added: `ComputeTargets/tests/test_sampling_axis.py`** (9 tests, in the `[g-z]` half). Its
docstring says that `TestTheGridTooShort` and `TestTheJordanRedshift` each integrate the golden
β = 1.5 history once, in about 0.6 s.
- `TestTheGrid`:
  - `test_the_points_are_integer_multiples_of_delta`: for (S, H) of (250, 85), (250, 300),
    (250, 35) and (100, 7.5), the helper gives K + 1 points, the first is 0.0, and each equals
    `k * DELTA` by `float.hex`.
  - `test_the_production_grid`: 21251 points, the top one `21250 * DELTA` (195.7 = 85 ln 10 to
    1e-10), and 75 001 points at 300 decades.
  - `test_a_grid_past_float_range_cuts_and_reads_without_overflow` (prompt §2 item 3): a 400-decade
    grid has a top point above 921, where `exp` overflows. `truncate_to_history(grid, 800.0)` keeps
    `floor(800/DELTA) + 1` points, the largest ≤ 800 and the smallest 0.0, in descending order. Read
    on every point of the full grid, `z` is a float without raising: `inf` exactly where
    ln(1 + z) > ln(DBL_MAX), and otherwise `expm1(ln(1 + z))`. The cut contains `inf` points.
  - `test_a_point_just_past_the_end_is_kept`: a point 0.5e-10 beyond the limit is kept, and one
    2e-10 beyond it is not.
  - `test_the_label_is_not_negative_and_float_is_the_label`: −1e-12 and NaN raise `ValueError`;
    `float()` is ln(1 + z); `z` at 800 is `inf`.
- `TestTheGridTooShort.test_a_grid_that_ends_before_the_history_is_a_configuration_error`
  (ruling 7): the golden β = 1.5 setup, with `make_z_grid` patched to a 5-decade axis. It raises
  `RuntimeError`, and the message contains "largest supplied redshift",
  `ln(1 + z)=11.5129` and `solution required N=25.07`.
- `TestTheJordanRedshift` (ruling 8), on the golden β = 1.5 history run once in `setUpClass`:
  - `test_the_new_field_is_the_last`: 18 fields, the last `log_one_plus_z_Jordan`;
  - `test_the_identity_holds_exactly`: at every sample, `assertEqual(log_one_plus_z_Jordan,
    z.log_one_plus_z + (coupling.log_Omega(phi_0) - coupling.log_Omega(phi)))`, with φ₀ the k = 0
    sample's φ, and the k = 0 value is 0.0;
  - `test_the_jordan_redshift_is_that_of_the_jordan_temperature`: at every sample,
    |ln(1 + z_J) − (ln(1 + `cosmology.z`(T_J)) − ln(1 + `cosmology.z`(T_J,end)))| < 1e-6, with
    T_J,end that of the z = 0 sample. It also checks that some samples have H_J < 0.

**Added: `ObjectFactories/tests/test_redshift_axis.py`** (6 tests, on `_TempStoreCase`).
- `TestTheLookup`:
  - `test_a_lookup_at_zero_does_not_match_a_small_negative_row` (issue 09): a row at −1e-6,
    written by the table's inserter. `object_get("redshift", log_one_plus_z=0.0)` returns another
    serial with `_new_insert`, and the table has 2 rows. A scratch probe put the old filter
    (`abs((col - 0) < 1e-7)`) back into `build` by `mock`, and this test then fails (`1 == 1`);
    the next test passes under it.
  - `test_a_lookup_at_zero_matches_a_row_within_the_tolerance`: a row at 1e-12; the lookup at 0
    returns its serial, `_deserialized`, and the table has 1 row.
  - `test_adjacent_grid_points_do_not_collide`: the first three production grid points get three
    distinct serials; building them again returns the same three; 3 rows.
- `TestReadTable` (the only witness of `read_table`; `git grep -n '\.read_table('` finds only
  `plot_inputs.py`'s reads of other classes):
  - `test_rows_come_back_in_ascending_order`;
  - `test_a_model_proxy_keeps_only_that_historys_points`: with a stub proxy (`get()` returns an
    object with `store_id`), only the two points that carry that history's value rows come back,
    ascending, and the other history's point does not.
- `TestTheValueFactory.test_a_value_row_is_not_built_on_its_own` (ruling 11):
  `object_get("ScalarHistoryValue", …)` raises `NotImplementedError` naming
  `sqla_ScalarHistoryFactory`, and no row is written.

**Added: `ObjectFactories/tests/test_datastore_config.py::TestDropPolicy::test_a_nonzero_low_end_of_the_redshift_axis_is_refused`**
(ruling 3). `["--database", "x", "--log10-one-plus-z-low", "1"]` exits `SystemExit(2)` with the
option's name in stderr. `"0"` parses to the float 0.0, and the default high end is 300.

**Changed:**

| Test | Change | Why |
|---|---|---|
| `ComputeTargets/tests/test_golden_history.py` | docstring: the new file, the regeneration, 01's file kept; the cost line unchanged | ruling 2 |
| `ComputeTargets/tests/golden_history.py` | the grid, the pinned label, `GOLDEN_PATH`, provenance (above) | ruling 2 |
| `ComputeTargets/tests/test_fixed_T_values.py` | `full_history` builds its 35-decade grid with the helper; assertions unchanged | ruling 3 |
| `ComputeTargets/tests/test_bbn_from_store.py` | the hand-built `redshift` table is `(serial integer primary key, log_one_plus_z real)`, with `log1p` of the same three z; the order assertion unchanged | ruling 10 |
| `tools/tests/test_inventory_report.py` | rows written as `{"log_one_plus_z": value}` (the same 25 numbers); the redshift line expected as `log_one_plus_z = [ … ]` at **`:284`** (non-verbose) and **`:364`** (verbose) | ruling 10; `:284` confirmed by the orchestrator (Deviation 15) |
| `ObjectFactories/tests/test_store_fingerprint_golden.py` | `{"log_one_plus_z": value}` for the same five numbers; `GOLDEN` names the format-4 file; the format literal in `TestCommandLine.test_store_fingerprint` 3 → 4; a format-4 docstring paragraph | ruling 5 |
| `tools/tests/test_compare_stores.py` | `log_one_plus_z=math.log1p(1e12)` and `math.log1p(1e9)` (`:904-905`); `"log_one_plus_z_Jordan"` added as the 18th value column in `_fill_champbh_shard` | ruling 10; the orchestrator's ruling after the stop (Deviation 15) |

## Deviations from the prompt

Every ruling cited is in orchestrator note 03, §0.

1. **`STRUCTURALLY REQUIRED` (ruling 1): the change in values is measured in two parts**, Part A at
   fixed positions and Part B for the grid. The prompt's stop on a sample count applies to Part A
   only. See Verification.
2. **`STRUCTURALLY REQUIRED` (ruling 2): the golden history's grid, its pinned label and its new
   file.**
3. **`IMPLEMENTATION CHOICE` (ruling 3): one grid helper**, `log_one_plus_z_grid`, used by every
   builder, including `tools/history_and_bbn.py` and `test_fixed_T_values.py`, which are outside the
   prompt's §5.
4. **`STRUCTURALLY REQUIRED` (ruling 3): the rest of Z3.**
   - The option keeps its name `--samples-log10-z`, not the prompt's `--samples-per-log10-z`.
   - The old default was 35, not 85.
   - The refusal is in the parser, and `main.py` no longer reads the option.
5. **`STRUCTURALLY REQUIRED` (ruling 4): the concept.** There is no `z=` keyword. Equality and
   hashing stay on `store_id`. The truncation tolerance is 1e-10. The `float(` callers are listed
   under Verification.
6. **`STRUCTURALLY REQUIRED` (ruling 5): the factory, the spec and format 4.** The two
   `DEFAULT_REDSHIFT_*` constants are deleted.
7. **`IMPLEMENTATION CHOICE` (ruling 5): the lookup is written as a range**, so that SQLite searches
   the index, and the timing of a 75 001-point build was measured. See Verification.
8. **`STRUCTURALLY REQUIRED` (ruling 6): `ObjectFactories/AdiabaticHistory.py` and
   `ObjectFactories/BBNData.py`** read `log_one_plus_z`. Both are outside the prompt's §5.
9. **`IMPLEMENTATION CHOICE` (ruling 7): the truncation is the helper `truncate_to_history`.**
10. **`STRUCTURALLY REQUIRED` (ruling 7):** the "grid too short" raise stays inline (so
    `test_integrator_exceptions` is not edited), and a test drives it.
11. **`STRUCTURALLY REQUIRED` (ruling 8): ln(1 + z_J)'s exact expression, its place, and its test.**
    The prompt's third §4 stop is replaced by ruling 8's: more than 1e-6 at any sample.
12. **`IMPLEMENTATION CHOICE` (ruling 9): `ScalarHistory.functions` is not changed**, and the issue
    `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]` is opened.
13. **`STRUCTURALLY REQUIRED` (ruling 10): the readers.** No new axis mode in `extract_common`.
    `inventory_report` shows the leaf's name. The hand-built tables and rows of four tests are
    changed.
14. **`STRUCTURALLY REQUIRED` (ruling 11, the prompt's own alternative): Z7's path is deleted**,
    not fixed.
    - **`IMPLEMENTATION CHOICE`:** `from math import fabs, log` became `from math import log`.
      `fabs` was used only by the deleted φ/π check, as `DEFAULT_FLOAT_PRECISION` was, whose import
      ruling 11 removes.
15. **`STRUCTURALLY REQUIRED` (ruling 8; the orchestrator's ruling after a stop): `test_compare_stores`'
    value-column fixture.**
    - **The stop.** I stopped before committing:
      `tools/tests/test_compare_stores.py::TestChamPBHSchema::test_14_a_store_with_champbhs_schema`
      errored with `sqlite3.IntegrityError: NOT NULL constraint failed:
      ScalarHistoryValue.log_one_plus_z_Jordan`, in the full run and alone. Its fixture
      `_fill_champbh_shard` writes ChamPBH's real schema and names the 17 value columns, so the new
      non-null column of ruling 8 followed into it.
    - **The ruling.** Add `"log_one_plus_z_Jordan"` as the 18th name after `"kicking_term_Mp"`, so
      the row gets 0.5 + 0.25 i + 0.17, with no assertion changed and the column kept
      `nullable=False`.
    - **What note 03 missed.** Its ruling 10 should have named this, as it named the same file's
      redshift rows (`:904-905`).
    - **The second confirmation.** The orchestrator also confirmed that `test_inventory_report.py:284`
      (the non-verbose redshift line) follows `:364` under ruling 10. Both lines are changed.
16. **`STRUCTURALLY REQUIRED` (ruling 13): the records**, as ruled. `exponential.yaml`,
    `prompts/INDEX.md`, `.documents/architecture-summary.md` and `numerical-strategies.md` are not
    edited.
17. **`IMPLEMENTATION CHOICE`:** two comment edits in lines this prompt rewrote anyway.
    - `tools/bbn_from_store.py`'s comment names the factory class correctly
      (`sqla_ScalarHistoryFactory`, not `sqla_ScalarHistory_factory`).
    - `tools/history_and_bbn.py`'s constants comment no longer says that 35 is the parser's
      default.

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the start of each run. Outputs are in the scratch
directory `agent03/` (`before/`, `after/`, `final/`).

| Suite | Before (`9997eef`) | After (final, `bd9ae42`'s tree) | Change |
|---|---|---|---|
| CosmologyModels | 18 OK, 82 s, load 78 | 18 OK, 74 s, load 56 | none |
| ComputeTargets `test_[a-f]*` | 61 OK, 105 s, load 169 | 61 OK, 100 s, load 55 | none |
| ComputeTargets `test_[g-z]*` | 118 OK, 137 s, load 86 | **127 OK**, 118 s, load 23 | **+9**: `test_sampling_axis` |
| ObjectFactories | 79 OK, 12 s, load 63 | **86 OK**, 8 s, load 10 | **+7**: `test_redshift_axis` (6), the refusal test (1) |
| tools | 84 OK, 16 s, load 56 | 84 OK, 15 s, load 11 | none |
| RunDrivers | 61 OK, 21 s, load 48 | 61 OK, 24 s, load 11 | none |

- **Counts:** 18 / 179 / 79 / 84 / 61 before, and 18 / 188 / 86 / 84 / 61 after.
- **Between them:** one run of the edited tree had tools at 84 with 1 error (Deviation 15). After
  the ruled fixture change, tools alone gave 84 OK, and then the five suites above ran serially.
  No count fell.
- **The golden history before any edit:** 3 OK in 10.3 s.
- **`black --check`:** passes on the 26 Python files changed or added, all of which were black-clean
  at `HEAD`.

### The change in values (ruling 1)

**Part A — the code at fixed positions.**
- **The runs.** In a `git worktree` at `9997eef` (in scratch, since removed), a scratch script
  replaced `golden_history.make_z_grid` with z = `math.expm1(k * DELTA)`, k = 0 … 21250,
  DELTA = `math.log(10.0) / 250`, and ran both golden histories. On this tree it ran them on the axis
  grid of the same k (`log_one_plus_z_grid(250, 85)`, checked to equal `k * DELTA`). It saved all
  samples and compared them by index.
- **The script fault.** The new-tree run was first made with the module looked up as
  `import CosmologyConcepts.redshift as rmod`. Because the package re-exports the class `redshift`,
  that binds the class, so the script took the old-grid branch and fed `expm1(kΔ)` as ln(1 + z).
  That run (355 and 416 samples) was discarded, the lookup was changed to
  `importlib.import_module`, and the run was repeated. The old-tree run takes the old branch either
  way, since the helper does not exist there.
- **The counts:** equal, 2723 and 2723 (β = 1.5), 4894 and 4894 (β = 5.5).
- **Bitwise:** every sample whose `raw_N` is bitwise equal on the two trees is bitwise equal in all
  17 old fields; there are 0 exceptions in either history.
- **Where `raw_N` differs**, it differs by exactly 1 ulp of `final_N`. `final_N` is
  25.071121064622364 (ulp 3.55e-15) at β = 1.5 and 45.074508319349476 (ulp 7.11e-15) at β = 5.5.
- **The differing samples:** **1** at β = 1.5 (index 2715, k = 7) and **2** at β = 5.5 (indices 4798
  and 4803, k = 95 and 90). Relative difference is |a − b|/max(|a|, |b|), and 0 where both are 0.

| Field | β = 1.5, largest (index) | β = 5.5, largest (index) |
|---|---|---|
| `raw_N` | 1.42e-16 (2715) | 1.61e-16 (4798) |
| `phi_Einstein` | 1.79e-14 (2715) | 2.10e-13 (4803) |
| `pi_Einstein` | 5.04e-15 (2715) | 3.90e-13 (4798) |
| `log_rhorad_Einstein` | 0 | 1.48e-16 (4798) |
| `log_rhorad_Jordan` | 0 | 1.48e-16 (4798) |
| `log_fm` | 2.49e-16 (2715) | 4.72e-16 (4798) |
| `H_Einstein` | 0 | 1.43e-14 (4803) |
| `H_Jordan` | 0 | 1.57e-14 (4803) |
| `log_T_Jordan` | 0 | 1.47e-16 (4798) |
| `gstar_rho` | 0 | 1.66e-16 (4803) |
| `gstar_s` | 0 | 1.66e-16 (4803) |
| `dgstar_s_dlogT` | 0 | 1.35e-14 (4803) |
| `dgstar_rho_dlogT` | 0 | 1.33e-14 (4803) |
| `Sigma` | 0 | 2.13e-13 (4798) |
| `friction_term` | 5.21e-15 (2715) | 3.90e-13 (4798) |
| `reflecting_term` | 3.60e-14 (2715) | **6.59e-12** (4803) |
| `kicking_term` | 0 | 2.13e-13 (4798) |

So ruling 1's rule holds, and the note's §0 sample (4803, 6.6e-12 in `reflecting_term`) reappears
here.

**Part B — the grid.**
- **The old grid:** `np.logspace(0, 85, 21250) - 1`, 21250 points spaced 85/21249 = 0.0040001882
  decades (9.210774e-3 in ln(1 + z)).
- **The new grid:** 21251 points spaced 1/250 = 0.004 decades (Δ = 9.210340e-3).
- **The displacement, by index:** largest at index 21249, by 9.2103e-3 (one full Δ), from 195.71973
  to 195.71052. At k = 4000 it is 1.73e-3. Both grids top out at 195.7197329044939.
- **To the nearest new point,** the largest displacement is 4.6e-3, at old index 10625.
- **`n_samples`,** `golden_history_01.json` against `golden_history_03.json`: β = 1.5 goes
  **2722 → 2723** and β = 5.5 stays **4894 → 4894**, as the note expected.
- **Unchanged between the two files:** reflections, accepted steps, steps rejected by exception,
  compute steps, RHS evaluations, the first bounce, the fixed-T values, the step scalars and the
  solver label.

### ln(1 + z_J) (ruling 8)

On the samples of the Part A new-tree runs, which are those of the golden histories:
- **The identity** `log_one_plus_z_Jordan == kΔ + (ln A(φ₀) − ln A(φ))` holds by `==` at every
  sample of both histories, and the value at k = 0 is 0.0.
- **Against** ln(1 + `cosmology.z`(T_J)) − ln(1 + `cosmology.z`(T_J,end)), the largest |difference|
  is:

| History | Over | Samples | Max | Index | ln(1 + z_E) | ln(1 + z_J) | T_J |
|---|---|---|---|---|---|---|---|
| β = 1.5 | all | 2723 | **3.07e-7** | 2168 | 5.1025 | 4.9559 | 0.117 GeV |
| β = 1.5 | H_J ≥ 0 | 2275 | 3.07e-7 | 2168 | 5.1025 | 4.9559 | 0.117 GeV |
| β = 1.5 | H_J < 0 | 448 | 1.55e-7 | 1579 | 10.5274 | 6.2967 | 0.321 GeV |
| β = 5.5 | all | 4894 | **3.72e-8** | 4349 | 5.0104 | 5.0097 | 0.122 GeV |
| β = 5.5 | H_J ≥ 0 | 3397 | 3.72e-8 | 4349 | 5.0104 | 5.0097 | 0.122 GeV |
| β = 5.5 | H_J < 0 | 1497 | 2.37e-8 | 748 | 38.1769 | 12.8566 | 179 GeV |

- **Against §0:** the note gives 3.27e-7 at sample 2167 and 3.69e-8 at sample 4349; inside the
  bounces it gives 1.55e-7 and 2.37e-8, over 448 and 1496 samples. The β = 1.5 index moves by one
  because the new grid is one sample denser. The β = 5.5 count of samples with H_J < 0 is 1497
  against §0's 1496, because the grid moved.
- **The stop is not met:** every value is below 1e-6.
- **Not monotonic:** ln(1 + z_J), read in order of increasing k, falls 447 times (β = 1.5) and 1496
  times (β = 5.5).

### The cost of the lookup (ruling 5)

- **The store.** A temporary store (`TMPDIR` in the scratch directory, removed by the harness),
  built through the stand-in actor (`_TempStoreCase`), with
  `object_get("redshift", payload_data=…)` over the 75 001 points of `log_one_plus_z_grid(250, 300)`,
  at load 43–46.
- **Into the empty table:** **6.98 s** (93 µs per point; 75 001 inserts).
- **Over the filled table:** **5.12 s** (68 µs per point; 0 inserts).
- **`EXPLAIN QUERY PLAN`** for the lookup
  `SELECT redshift.serial FROM redshift WHERE redshift.log_one_plus_z > 11.365560018918611 AND redshift.log_one_plus_z < 11.365560019118611`
  gives `SEARCH redshift USING COVERING INDEX ix_redshift_log_one_plus_z (log_one_plus_z>? AND log_one_plus_z<?)`.
  The `abs(log_one_plus_z - x) < 1e-10` form gives `SCAN redshift USING COVERING INDEX
  ix_redshift_log_one_plus_z`, a scan of all 75 001 rows.

### The greps (ruling 4)

- `git grep -n -P 'float\('`, restricted to lines that name z, a redshift or a grid, found two
  callers of `float()` on a redshift before the edit:
  - `plot_ScalarHistory.py:1294`, `"z": float(value.z)`, now `value.z.z`;
  - `redshift_array.as_float_list`, which has no caller (`git grep -n as_float_list` finds only the
    three definitions) and now returns ln(1 + z), as its new docstring says.
- `git grep -n -P '(np|numpy|math)\.\w+\([^)]*\b(z|z_grid|z_sample|z_array|value\.z|v\.z)\b'`
  outside the records found nothing.
- `git grep -n -P '\.z\b(?!\()'` lists every attribute read of `.z`. Each one is now on a redshift
  (its derived `z`), is moved to `log_one_plus_z` above, or is a `.z.store_id`.

### The driver checks (ruling 14)

- `PYTHONPATH=. ./venv/bin/python main.py --help`: exit 0. The help shows
  `--log10-one-plus-z-high … (default 300)` and `--log10-one-plus-z-low …; only 0 is accepted, since
  the redshift axis is anchored at the end of the history`.
- `PYTHONPATH=. ./venv/bin/python main.py --database <scratch>/absent.db --log10-one-plus-z-low 1`:
  **exit 2**, with `main.py: error: argument --log10-one-plus-z-low: must be 0, not 1: the redshift
  axis is anchored at the end of the history`. No file was created at the path.

## The deliberate-breakage record

- **How each diff was made.** By a scratch script (`difflib.unified_diff`) from the working tree that
  `bd9ae42` holds.
- **How each was replayed,** in `bash`: `git apply --check`, `git apply`, `git apply -R --check`,
  the named tests, and `git apply -R`. Afterwards `git diff` was byte-identical to its state
  before.
- **Checked again** with `git apply --check` after the final fixture edit; all three apply.

**1. The grid accumulated (`x += DELTA`).**
- `test_sampling_axis`, 2 failures:
  - `test_the_points_are_integer_multiples_of_delta`: `'0x1.1af11061eb816p-2' != '0x1.1af11061eb815p-2'`;
  - `test_the_production_grid`: `195.71973290443736 != 195.7197329044939`.
- `test_golden_history`, 2 failures: `test_history_beta_1_5` and `test_history_beta_5_5`, each
  "sample 0: log_one_plus_z differs", then `H_Einstein`, `log_one_plus_z_sha256` and
  `samples_sha256`.
- `test_redshift_axis` passes, since the first three points are exact either way.

```diff
--- a/CosmologyConcepts/redshift.py
+++ b/CosmologyConcepts/redshift.py
@@ -38,7 +38,11 @@
     """
     DELTA = math.log(10.0) / samples_per_log10_z
     K = int(round(log10_one_plus_z_high * samples_per_log10_z))
-    return [k * DELTA for k in range(K + 1)]
+    points, x = [], 0.0
+    for k in range(K + 1):
+        points.append(x)
+        x += DELTA
+    return points
 
 
 @total_ordering
```

**2. `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-2`.**
- `test_redshift_axis`:
  - `test_adjacent_grid_points_do_not_collide` **errors**: with the points 9.2e-3 apart, the
    second point reuses the first's row, and on the rebuild a lookup matches two rows
    (`one_or_none` raises `MultipleResultsFound`);
  - `test_a_lookup_at_zero_does_not_match_a_small_negative_row` **fails** (`1 == 1`): the
    −1e-6 row is matched.
- `test_sampling_axis`, 2 failures from the truncation's tolerance:
  - `test_a_point_just_past_the_end_is_kept`: `102 != 101`;
  - `test_a_grid_past_float_range_cuts_and_reads_without_overflow`: `86860 != 86859`.
- In all: 3 failures and 1 error.

```diff
--- a/config/defaults.py
+++ b/config/defaults.py
@@ -20,7 +20,7 @@
 # the sampling axis, ln(1 + z) (jordan-normalization prompt 03, P8): the redshift factory's lookup
 # and redshift_array's truncation match by absolute difference below this. The grid spacing is
 # ln 10 / 250 = 9.2e-3, so two grid points can never collide
-DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10
+DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-2
 
 DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION = 1e-7
 DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION = 1e-5
```

**3. `log_A_0` dropped from Z5's expression.**
- `test_sampling_axis`, 2 failures:
  - `test_the_identity_holds_exactly`: `-0.003491930563512202 != 0.0` at k = 0;
  - `test_the_jordan_redshift_is_that_of_the_jordan_temperature`: 3.49e-3 at index 2168, not
    below 1e-6.
- `test_golden_history`, 2 failures: "sample 0: field log_one_plus_z_Jordan differs" and
  `samples_sha256`, at both β.
- In all: 4 failures.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1321,7 +1321,7 @@
                         friction_term=data.friction_term,
                         reflecting_term=data.reflecting_term,
                         kicking_term=data.kicking_term,
-                        log_one_plus_z_Jordan=N_backward + (log_A_0 - log_Omega),
+                        log_one_plus_z_Jordan=N_backward - log_Omega,
                     )
                 )
         except OverflowError as e:
```

## Observations not acted on

- **`AdiabaticHistoryValue`'s and `BBNDataValue`'s `build`s** (`ObjectFactories/AdiabaticHistory.py`,
  `ObjectFactories/BBNData.py`) are in the state `ScalarHistoryValue`'s was: reached only through
  `object_get` of their class, which nothing calls. `AdiabaticHistoryValue.build` calls
  `inserters["redshift"](conn, z, …)` with a redshift object (`:450`). Not changed (ruling 11); no
  issue opened.
- **`ScalarHistory._create_functions`** reads `self.z_sample`, which `ScalarHistory` does not define,
  so `functions` would raise `AttributeError` before it could give wrong values. This is recorded in
  the issue `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`; the function is
  not changed (ruling 9).
- **`extract_common.py:421`**'s `if "x_coord" == "efolds":` compares a string literal and is always
  false. In the same function, the redshift branch puts lines at `get_xpos_attr`'s `z` while
  `get_x_coord` plots `1 + z`. Both are cosmetic, in a mode the driver never uses. Not changed, and
  no issue (the note offers them to the user as housekeeping).
- **`.documents/architecture-summary.md:636`** still lists `DEFAULT_REDSHIFT_PRECISION = 1e-7`, which
  `bd9ae42` deletes. 10's C1 writes the axis into the documents; not edited here (ruling 13).
- **`--samples-log10-z`'s help** says "per log10(z)"; the axis is per decade of 1 + z. The option is
  08's; not changed.
- **`ComputeTargets/tests/golden_history.py`'s module docstring** still says the excerpts are "all
  float.hex", which is true. Its "Regenerate" paragraph is unchanged and stays true.

## State handed to the next prompt

- **Tree:** `bd9ae42`, on `jordan-normalization`, on top of `9997eef`.
- **The axis.**
  - `CosmologyConcepts.redshift.redshift(store_id, log_one_plus_z)`: `.log_one_plus_z` is the label,
    `.z` is `expm1` of it (`inf` past 709.78), and `float()` is ln(1 + z).
  - `log_one_plus_z_grid(S, H)` gives `[k * DELTA …]`; import it from `CosmologyConcepts.redshift`.
    `CosmologyConcepts/__init__.py` re-exports the *class* `redshift`, so
    `import CosmologyConcepts.redshift as m` binds the class: use `from … import` or
    `importlib.import_module`.
  - Truncation and the lookup use `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10`.
  - The table `redshift` has the column `log_one_plus_z`, indexed.
  - The parser's default high end is 300 decades, and `--log10-one-plus-z-low` accepts only 0.
    `exponential.yaml` still sets 85 (K = 21250, top point 195.72).
- **The samples.** `SampleValues` has 18 fields; the last is `log_one_plus_z_Jordan`. It is a
  required argument of `ScalarHistoryValue` and a non-null column of `ScalarHistoryValue`'s table.
  `compute_scalar_history` reads N_backward from the axis and cuts the grid with
  `truncate_to_history`. Its signature is unchanged (`cosmology, T_init, T_stop, phi, pi, z_grid,
  potential, coupling`).
- **Z7.** `sqla_ScalarHistoryValue_factory.build` raises `NotImplementedError`. Value rows go only
  through `sqla_ScalarHistoryFactory`.
- **Fingerprint.**
  - `FINGERPRINT_FORMAT = 4`; `SPEC_DIGESTS[1]`–`[4]` (format 4: 17 pairs, 14 distinct digests).
  - The current golden file is `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-03.json`,
    digest `05d4b924…07cf`.
  - The format literal in `TestCommandLine.test_store_fingerprint` is 4. **07 makes format 5.**
- **The golden history.**
  - `ComputeTargets/tests/data/golden_history_03.json` (SHA-256 `f1fbc535…dab5`), read by
    `test_golden_history` through `golden_history.GOLDEN_PATH`, about 10 s.
    `golden_history_01.json` is kept as the record of 01 and 02.
  - **04 must pass it unchanged** and change `golden_history.run_history` to follow the new
    interface.
  - **05 regenerates it deliberately** as a new file beside it, and logs the change.
  - The pinned label is `log_one_plus_z` (`log_one_plus_z_sha256`, and the excerpts' key).
- **Counts:** CosmologyModels 18, ComputeTargets 188 (61 + 127), ObjectFactories 86, tools 84,
  RunDrivers 61.
- **Open issues.**
  - Closed: `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`,
    `[05-the-value-factory-compares-stored-phi-against-pi]`,
    `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`.
  - Opened: `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]` (for 07, not
    assigned).
  - Four remain assigned to this campaign (board §3.1). Two are open on this board.
    `.documents/OPEN_ISSUES.md`: 25 open.
- **Test fixtures that name the value columns** (`tools/tests/test_compare_stores.py`'s
  `_fill_champbh_shard`, now 18 names) must follow any further change to `ScalarHistoryValue`'s
  columns (07).
- **Breakage replay:** the three diffs above apply to `bd9ae42`'s tree. `git apply --check` passes
  for each, and after applying each, `git apply -R --check` passes and the reverse restores the tree.
  Checked in `bash`.
