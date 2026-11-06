# Log 04 — drivers and exports

**Subject:** Give the drivers the one-loop law and write its three exports
**Commit:** this commit, on branch `one-loop-drain`, on top of `8df05c1`
**Date:** 2026-11-06, by Claude Sonnet 5.5
**Result:** **COMPLETE.** The Export gate holds (`.documents/one-loop-drain-verification.md` §4).
- **The option.** `--A-values` (one or more A, each finite and > 0, no upper bound, no default) beside `--alpha-values`
  (unchanged, default `[0]`), in `main.py` and both plot scripts. The rows are the fixed rows in the order of
  `--alpha-values`, then the one-loop rows in the order of `--A-values`, looked up under `DRAIN_MODEL_ONE_LOOP`.
- **The paths and labels.** `alpha_path(base, value, drain_model)` and `alpha_label(value, drain_model, spelling)`, with no
  default for the model. A fixed-law path, title, legend and printed line is unchanged character for character
  (checked against the base code's own strings, verification §4.4).
- **The exports.** histories.csv gains `drain_model` (after `alpha`) and the 23 columns of `DRAIN_CSV_COLUMNS` (before
  `failure_reasons`); fields.csv gains `log_m_eff_Mp` and `log_m_tracker_Mp` (after `Delta_gstar`); bounces.csv is new (30
  columns). **Every new cell obeys ruling 1's rule:** 168 + 52 886 + 2 130 cells, no failure; in the units the store was
  built with, 0 cells differ from the raw column.
- **The fixed law is byte-identical:** the base's and the tip's histories.csv (fixed-law records) and fields.csv (every
  history, both models), projected onto the base's columns, are equal byte for byte; the store's SHA-256 does not move
  under either export.
- **The tools.** `tools/bbn_from_store.py --drain-law`, `tools/outward_step_scan.py` (fixed rows only, with a count of the
  rest), `tools/inventory_report.py` (a row of another model reads `A=<A> (<model>)`; a store of fixed rows alone reads as
  before 03), and `describe_scope` (`A <values>` only when `--A-values` is given).
- **Suites:** before (the dispatch tree): 26, 622 (190 + 432), 149, 156, 68. After: 26, 651 (219 + 432), 163, 173, 72, all passing.
- **Issues:** none opened or closed. `.documents/OPEN_ISSUES.md` is unchanged.

Orchestrator note 04 (§0, rulings 1–9 and the conventions) governed where it differs from the prompt. Each ruling is
classified in §2. `FINGERPRINT_FORMAT` stays 7 and `VERSION_LABEL` 2026.10.0; nothing under `ComputeTargets/` or
`ObjectFactories/` changed but tests.

## 1. What shipped

### 1.1 The option, the rows and the drivers

- **`config/argument_parser.py`**: `drain_strength_A(text)`, the type of `--A-values`: `float(text) + 0.0`, then
  `check_drain_model(DRAIN_MODEL_ONE_LOOP, value, "--A-values")`, whose `ValueError` is re-raised as `ArgumentTypeError` with its
  message (ruling 5). `--A-values` has `nargs="+"` and `default=None`; the help says what A is, that it names the code's
  current one-loop model, that it is a key of every history, and that α = 0 is still built.
  - **Refusals** (exit 2, `argument --A-values: …`, before any Ray): 0, −0 (read as 0), −1, nan, inf, and an unparsable text;
    one bad value refuses the list. 10, 1000 and 1e6 are accepted. The YAML key is `A-values`.
- **`main.py`**: `A_values = distinct_values(args.A_values or [])`; the tolerances' `ray.get` has the alpha lookups (no model
  named) and then one `object_get("alpha_value", value=A, drain_model=DRAIN_MODEL_ONE_LOOP)` per A, all into `*alpha_rows`;
  `run_pipeline` is called once per row as before; a line `-- populated A grid with N value(s) = [ … ] (<model>)` is printed
  only when `--A-values` is given. The pipeline header names the row by `alpha_label(…, 'alpha')`.
- **`plot_inputs.parameters(…, alpha_values, A_values=())`**: one more lookup per A under `DRAIN_MODEL_ONE_LOOP`, after the
  alpha rows. **`PlotParameters.alphas` stays one tuple**, fixed rows first and then the one-loop rows, each row carrying its
  `drain_model` (so the drivers' loops over `alpha_rows` are unchanged). An A with no row under that model is
  `PlotOptionNotStored("alpha_value", "--A-values", "<A> (one-loop-measured-v1)")`; an α with no `fixed` row is refused as before.
- **`plot_by_beta.py`, `plot_ScalarHistory.py`**: both lists to `plot_inputs.parameters`; both loop over every row; a line `A values … (<model>)`
  beside the `alpha values …` line when `--A-values` is given (the alpha line lists the fixed rows only, so a run without
  `--A-values` prints the line of before). `build_beta_plot` takes `drain_model` after `alpha`; every path and label passes the
  row's model.

### 1.2 The helpers (ruling 2, ruling 3)

| helper | signature | fixed model | any other model |
|---|---|---|---|
| `extract_common.alpha_path` | `(base, alpha, drain_model)` | `base/alpha=<α:.5g>` | `base/<drain_model>/A=<A:.5g>` |
| `CosmologyConcepts.drain_model.alpha_label` | `(value, drain_model, alpha_spelling="α")` | `<spelling> = <α:g>` | `A = <A:g> (<drain_model>)` |
| `CosmologyConcepts.drain_model.model_of_law` | `(drain_law, context)` | `fixed` → `fixed` | `one_loop` → `one-loop-measured-v1`; the inverse of `law_of` |

Strings (checked by literal in `test_a_values_option`): `alpha_label(0.1, "fixed")` = `α = 0.1`; with `"alpha"`, `alpha = 0.1`;
with `r"$\alpha$"`, `$\alpha$ = 0.1`; `alpha_label(1.0, "one-loop-measured-v1")` = `A = 1 (one-loop-measured-v1)` in every spelling;
`alpha_path("out/label", 0.1, "fixed")` = `out/label/alpha=0.1`; `alpha_path("out/label", 1.0, "one-loop-measured-v1")` =
`out/label/one-loop-measured-v1/A=1`. Titles and `_cosmology_line` take the default spelling, the printed header lines
`alpha`, the legend of `plot_Delta_N_eff` `$\alpha$`. The model has no default in `alpha_path`, `alpha_label` and
`build_history_record`; `add_beta_summary_labels` and `add_ScalarHistory_labels` keep `alpha=None` (no alpha part) and take
`drain_model=None`, but `_cosmology_line` raises `ValueError` for an `alpha` without its model, and the `ast` tests check that every
call in the drivers and in `extract_common.py` passes it.

### 1.3 The three exports, as built

All floats are written at full precision (`repr`), blank is NULL, Booleans are 0 and 1.

| file | where | new columns (unit) |
|---|---|---|
| histories.csv | `<output>/<label>/histories.csv` (every row of the run, both models) | `drain_model` (after `alpha`; `fixed` or `one-loop-measured-v1`); the 23 columns of `DRAIN_CSV_COLUMNS` (before `failure_reasons`), all dimensionless: `drained_share_{one_loop,gap,classical,unassessed}`, `parking_share`, `candidates_{one_loop,gap,classical,unassessed}` (integers), `ell_min`, `log_R_min_drain`, `unresolvable_assessments`, `unresolved_declined` (integers), `all_worst_{birth,reflection,tracker}_margin`, `…_margin_N`, `all_{birth,reflection,tracker}_failing_share`, `bounce_rows` (integer) |
| fields.csv | `<output>/<label>/<row path>/csv/beta=<β:.5g>/M=<M/eV:.5g>eV_n=<n>/fields.csv`, every history | `log_m_eff_Mp`, `log_m_tracker_Mp` (after `Delta_gstar`): ln(m/M_P), blank where NULL; under every law |
| bounces.csv | beside that fields.csv, one-loop histories only | the 30 stored columns of `BOUNCE_COLUMNS`, in the table's order, without `history_serial` (units in log 03 §1.1) |

### 1.4 The tools and the registered run

- **`tools/bbn_from_store.py`**: `--drain-law {fixed,one_loop}` (default `fixed`). `--alpha` is read as text and checked after the
  parse (ruling 4): under `fixed`, absent is 0, otherwise `alpha_fraction`'s message (`argument --alpha: must be a fraction …`);
  under `one_loop` it is required, and `check_drain_model` refuses a bad A (`argument --alpha: …`, exit 2). The find query
  binds `a.drain_model = ?` with `model_of_law(drain_law)`; a shard whose `alpha_value` has no `drain_model` column holds
  `fixed` rows only (the query is that of before for `fixed`, and `None` for any other model); a shard with no `alpha_value` table is
  as before. Messages: `_alpha_text` is unchanged under `fixed`, ` A=<A> (<model>)` otherwise.
- **`tools/outward_step_scan.py`**: `select_from_store` reads `where a.drain_model = ?` (bound from `DRAIN_MODEL_FIXED`) and counts
  the `ScalarHistory` rows of any other model, per shard and before every filter; one line on **stderr**, after the last shard,
  only if the count is above 0. A shard with no `drain_model` column is read as before and nothing is counted.
- **`tools/inventory_report.py`**: the `alpha_value` class prints `values = [ 0, 0.1, A=1 (one-loop-measured-v1) ]` (fixed rows by value,
  then the other models); a history's alpha reads `alpha=0.1` or `A=1 (one-loop-measured-v1)`; a store of fixed rows alone
  reports as it did before 03.
- **`RunDrivers/registered_run.py`**: `describe_scope` adds `A <values>` after the α part only when `--A-values` is given.

## 2. Deviations from the prompt

The note's rulings, each as followed:
1. **The export's equality, in any units** (ruling 1). *STRUCTURALLY REQUIRED* (the user's remark of 2026-11-06). Every new cell is
   compared by `to_column(from_column(raw, kind, units), kind, units)` (the raw value for kind None): `float.hex` for a float,
   values for integers and strings, 0/1 for a Boolean, blank for NULL; the raw columns are read by SQL. One test runs the
   writers under a second `UnitsLike` on constructed rows. **That second `UnitsLike` is `Mpc_units`, not `GeV_units`** (the
   note's example): `GeV_units` has GeV = 1, and the ruling asks for GeV and M_P both ≠ 1 (`Mpc_units`: GeV = 1.56e38, M_P =
   3.8e56). *STRUCTURALLY REQUIRED* by the ruling's own condition. The measurement (cells differing from the raw column) is
   `check_new_columns.py`'s and is not a stop.
2. **The label helper** (ruling 2): one helper, with the spelling a parameter. *STRUCTURALLY REQUIRED.* It lives in
   `CosmologyConcepts/drain_model.py` and not in `extract_common.py`, since `main.py` needs it and does not import
   `extract_common` (matplotlib): *IMPLEMENTATION CHOICE.* `extract_common` imports it, so `from extract_common import
   alpha_label` also works.
3. **The model is never defaulted** (ruling 3). *IMPLEMENTATION CHOICE* (ruled).
   - `alpha_path(base, alpha, drain_model)` (the prompt said "or its value and model"; the row is not passed so that the
     helper is testable on floats), `alpha_label`, `build_history_record(…, drain_model)`.
   - **The two figure-label functions keep `alpha=None`** for "no alpha part" and gain `drain_model=None`, with a
     `ValueError` from `_cosmology_line` for an `alpha` without its model. A default of None is not a default model: a call
     that forgets it fails at the first figure. *IMPLEMENTATION CHOICE.*
   - **Existing calls changed:** `alpha_path` (2 in `plot_by_beta.py`, 1 in `plot_ScalarHistory.py`); `build_history_record`
     (1 in `plot_by_beta.py`; 7 in tests, below); the label functions (3 in `plot_by_beta.py`, 7 in `plot_ScalarHistory.py`, 1 in
     `extract_common.plot_dark_radiation`).
4. **`bbn_from_store.py`'s `--alpha`** (ruling 4). *STRUCTURALLY REQUIRED* (the type `alpha_fraction` refuses A ≥ 1 before the
   law is known). `model_of_law` is in `drain_model.py`, with a test that it inverts `law_of`. **`find_model(…, drain_model=
   DRAIN_MODEL_FIXED)` and `_find_query(…, drain_model=DRAIN_MODEL_FIXED)` keep the fixed model as a keyword default**, as
   `find_model` keeps `alpha=0.0`: the tool's callers that name no law are fixed-law callers, and the CLI always passes the
   law. Ruling 3 named three helpers, not these. *IMPLEMENTATION CHOICE.*
5. **`--A-values`' type** (ruling 5). *As the note says; made exact.* The parse tests check stderr names `--A-values` and carries
   `check_drain_model`'s message.
6. **§2.2's store and the two exports** (ruling 6). *IMPLEMENTATION CHOICE* (ruled), with these particulars:
   - **The store** is built by `build_store.py` through the harness of `test_drain_store` (`_DrainStoreCase`: the package's
     actor on a one-shard store in the stand-in cluster), and copied to `var/one-loop-drain/04/store/`. It holds seven rows:
     `F-alpha0` (M = 0.5, β = 2, α = 0), `F-alpha0.1` (M = 0.5, β = 2, α = 0.1: reflects and drains), `F-fail` (a fixed failure
     row), `O-2197` (P-2197, A = 1), `O-classical` (L-M4e-15, β = 2, A = 1, 28 classical candidates), `O-gap` (L-M8e-18, β = 2,
     A = 2, 3 one-loop candidates and one gap) and `O-fail` (a one-loop failure row); 71 bounce rows. It took 14 s.
   - **The store is read through the package's actor opened `read_only=True`** with the label's lookup serial
     (`set_lookup_version`), not through `ShardedPool(read_only=True)`: the harness writes the shard without registering shard keys
     in the primary, so the pool's read-only route refuses `beta_value`'s shard key (`ReadOnlyWrite`). The actor writes nothing; the
     SHA-256 of both files is taken before and after each export (equal in all four).
   - **BBN and Q are unavailable stand-ins** (`SimpleNamespace(available=False)`) in both trees, and `bbn=None`, `baseline=None` in
     the records; no BBN or adiabatic row exists.
   - **The base code** is a scratch worktree of `8df05c1` (the dispatch commit; its code is 03's reviewed code, `50e93c7`), run from its own root with this checkout's `venv/bin/python` and `PYTHONPATH=.`. The base's
     `write_csv_file` was extracted from its own `plot_ScalarHistory.py` by `ast` and executed unedited (`extract_function`); the
     tip's `write_csv_file` is extracted the same way (it calls `build_fields_frame`). `build_history_record` is called with
     `drain_model` only if the tree's signature has it.
   - **The comparison**: each tree writes histories.csv once for the fixed-law records and once for the one-loop records; the tip's
     file is projected onto the base's columns in the base's order and written with the same writer settings (`csv.writer`, `\r\n`; for
     fields.csv `\n`), and compared byte for byte.
7. **`outward_step_scan.py`** (ruling 7). *As the note says; made exact.* **The line goes to stderr, at the end of
   `select_from_store`** (not in `main`): the function is the one place that has every shard, and stdout stays the tool's.
   *IMPLEMENTATION CHOICE.*
8. **The printed lines** (ruling 8). *As the note says; made exact.* Under the fixed law every line is its old text (the `no valid BBN data`
   line keeps `alpha=<α>`, from `alpha_label(…).replace(" = ", "=", 1)`).
9. **Files** (ruling 9). *STRUCTURALLY REQUIRED; followed.* `CosmologyConcepts/drain_model.py` holds `model_of_law` and `alpha_label`.

And, beyond the note:
10. **`PlotParameters` carries one tuple** (the prompt asked which): fixed rows, then one-loop rows, each with its model.
    *IMPLEMENTATION CHOICE.* `plot_inputs.parameters(…, A_values=())` has a default of no A, so every caller of before is unchanged.
11. **The fields.csv function is `extract_common.build_fields_frame(BBN, Q, model, units) -> pandas.DataFrame`** (the sorted
    frame); `write_csv_file` calls it and writes the file. The existing arithmetic was moved by program (the text of
    `write_csv_file`'s body), not retyped. `extract_common.py` therefore imports pandas. *IMPLEMENTATION CHOICE.*
12. **The bounces writer is `extract_common.write_bounces_csv(bounces, units, path)`** (and `bounces_csv_rows`);
    `plot_ScalarHistory.plot_ScalarHistory` calls it under `if model.alpha.drain_law == DRAIN_LAW_ONE_LOOP`. *IMPLEMENTATION CHOICE.*
13. **`alpha` in the plot scripts' line** lists only the fixed rows (see §1.1). *IMPLEMENTATION CHOICE.*
14. **The inventory report's `values` line** is built for `alpha_value` when its key has a `drain_model` leaf; with only the one leaf
    (a format-6 spec) the old code path is used. *IMPLEMENTATION CHOICE.*
15. **The "before" suites** were measured on a scratch worktree of `8df05c1`, not on the main checkout (a first chain there was stopped when
    my edits began). `tools/tests` and `RunDrivers/tests` each have one test that reads the repository root and fails in a worktree
    (`test_registry_config.test_roots_and_distributions`, `test_registered_run.test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`),
    as in 03's log; the counts are 156 and 68 as the dispatch Gate measured them in the main checkout. *IMPLEMENTATION CHOICE.*

### 2.1 Existing tests changed (forced)

- `ComputeTargets/tests/test_alpha_option.py`: `alpha_path` takes the model (4 calls, and the `ast` check that every call passes
  three arguments, the row's value and the row's model); `main.py` has two `alpha_value` lookups, the second under the constant
  (the old test asserted one); the label helpers' calls must pass `drain_model` too.
- `ComputeTargets/tests/test_extraction.py` (3 calls), `ObjectFactories/tests/test_alpha_figures.py` (3),
  `ObjectFactories/tests/test_tuned_history_round_trip.py` (1): `build_history_record(…, drain_model="fixed")`. Their other lines are
  unchanged and pass, including the precedent's tests 5 and 6.
- `ObjectFactories/tests/test_alpha_figures.py` `test_8_the_dissipation_columns_of_the_record`: it pinned `DISSIPATION_CSV_COLUMNS` as the columns just
  before `failure_reasons`; the drain block now sits between them, so it checks the dissipation columns by position from the first of them,
  then `DRAIN_CSV_COLUMNS`, then `failure_reasons` last. Found by the after-chain, not before.
- `tools/tests/test_inventory_alpha.py`, `tools/tests/test_inventory_report.py`: **03's changes to the expected lines are restored**,
  each named: `test_9_alpha_value_is_a_parameter_class_after_beta_value` (`values = [ 0 ]` again, for 03's
  `- value=0, drain_model=fixed`), `test_9_one_alpha_is_printed_once_on_the_common_line` (`log10_tuning_tol=-5, alpha=0`),
  `test_9_two_alpha_are_printed_on_each_history_line` (the `alpha=0`/`alpha=0.1` split and `values = [ 0, 0.1 ]`),
  `test_inventory_report.TestReportOnAStore` (`log10_atol=-10, log10_rtol=-8, log10_tuning_tol=-5, alpha=0`). The module
  docstring of `test_inventory_alpha` is rewritten to say so, and `_AlphaStoreCase.build` takes `(value, drain_model)` pairs as
  well as bare values (so the new class `TestTheInventoryNamesTheModel` can build a one-loop row). The inserts still carry
  `drain_model="fixed"`, which the spec requires.

No count fell. New modules: `ComputeTargets/tests/test_a_values_option.py` (23), `ComputeTargets/tests/test_a_values_plot_inputs.py` (6),
`ObjectFactories/tests/test_export_04.py` (14), `tools/tests/test_one_loop_tools.py` (14),
`RunDrivers/tests/test_scope_a_values.py` (4), and 3 more in `tools/tests/test_inventory_alpha.py`.

## 3. The Export gate

Item by item in verification §4.6: (1) the new columns equal the stored record, §4.3 (`out/new_columns.txt`); (2) the existing columns
byte-identical, §4.4 (`out/compare.txt`); (3) the drivers refuse a law that does not match the store, §4.5.

## 4. Verification performed

**The suites**, serially, each chain in the background.
Before any change, on a scratch worktree of `8df05c1` (the main checkout's chain was stopped when my edits began), serially,
2026-11-06 09:48–10:35, the machine shared (one-minute loads 7–38):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 70.5 s | 20.02 18.61 19.98 (09:48) |
| ComputeTargets `test_[a-f]` | 190 OK | 146.9 s | 37.66 23.72 21.76 (09:49) |
| ComputeTargets `test_[g-z]` | 432 OK | 487.0 s | 10.44 19.24 20.39 (09:52) |
| ObjectFactories | 149 OK | 126.4 s | 8.56 10.34 15.41 (10:00) |
| tools | 156; 155 OK, 1 failing for the worktree's root (`test_registry_config`) | 165.5 s | 19.10 12.68 10.34 (10:31) |
| RunDrivers | 68; 67 OK, 1 failing for the worktree's root (`test_registered_run`, the script's path) | 23.2 s | 7.53 10.53 9.88 (10:34) |

The tools and RunDrivers rows are a second run with the checkout's `venv` linked into the worktree (the first, without the link, failed
ten `tools/tests` and one RunDrivers test for that); the two that fail in the worktree read the repository root, as in log 03 §4, and pass in the
main checkout.

After, on this prompt's tree (the main checkout), serially, 10:09–10:35:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 65.8 s | 8.23 8.80 12.05 (10:09) |
| ComputeTargets `test_[a-f]` | 219 OK | 144.9 s | 7.32 8.40 11.66 (10:10) |
| ComputeTargets `test_[g-z]` | 432 OK | 482.0 s | 6.43 7.27 10.65 (10:12) |
| ObjectFactories | 163 OK | 133.1 s | 9.79 7.19 8.24 (10:29) |
| tools | 173 OK | 260.9 s | 9.09 8.39 9.37 (10:23) |
| RunDrivers | 72 OK | 47.6 s | 4.25 6.04 8.05 (10:27) |

ComputeTargets goes from 622 to 651 (`test_a_values_option` 23 and `test_a_values_plot_inputs` 6, both in the `test_[a-f]` half),
ObjectFactories from 149 to 163 (`test_export_04`, 14), tools from 156 to 173 (`test_one_loop_tools` 14 and 3 in `test_inventory_alpha`) and
RunDrivers from 68 to 72 (`test_scope_a_values`, 4). No count fell. ObjectFactories' first after-run (128.5 s) failed
`test_alpha_figures.TestDarkRadiationFigure.test_8_the_dissipation_columns_of_the_record` (§2.1, last item); the row above is the run after
the fix. The other suites ran once, on the same tree but for that test file.

**The store and the exports** (verification §4): `build_store.py` (14 s; `out/build_store.txt`), `export_tree.py` at the base worktree
and at this tree, `compare_exports.py` (`out/compare.txt`), `check_new_columns.py` (`out/new_columns.txt`). The scratch worktree
was removed (`git worktree remove`, `git worktree prune`) before the commit.

No Ray was started, no `ray` command run, and none of `main.py`, `plot_by_beta.py` or `plot_ScalarHistory.py` run as a program;
the drivers are checked by `ast` and their functions on temporary stores. `black` was run on every file I changed.

## 5. The deliberate-breakage record

- The mass cells written without `to_column`: the fixed-law and P-2197 tests, in `Planck_units` where M_P = 1, still pass, and the
  second-`UnitsLike` test (`Mpc_units`) fails; that is the case for ruling 1's test.
- `bounces_csv_rows` taking the object's field instead of `bounce_row_columns(row, units)`: the second-`UnitsLike` test fails.
- `alpha_path` or `alpha_label` called with no model: `TypeError` (tested); a figure label with an alpha and no model: `ValueError`.
- `--A-values 0`, `-0`, `-1`, `nan`, `inf`: refused at parse, exit 2, the option named.
- `plot_inputs.parameters`: an A stored only as an α is refused naming `--A-values`; an α stored only as an A is refused naming
  `--alpha-values`; neither touches a byte of the store.
- `bbn_from_store.py`: `--alpha 10` under `fixed` is refused with today's message; `--drain-law one_loop` without `--alpha` is refused.
- The first run of `test_a_values_option` failed two of its own tests (an `ast` order assumption about `ast.walk`, and `-inf`, which
  argparse reads as an option); fixed in the tests.

## 6. Observations not acted on

- **`plot_ScalarHistory.py` keeps two imports it no longer uses** (`pandas as pd`, `numpy.nan`): `write_csv_file`'s body moved to
  `extract_common.build_fields_frame`. Not removed (an unrelated edit).
- **The `ShardedPool(read_only=True)` route cannot read a store the harness wrote** (no shard keys in the primary), so §2.2 reads
  through the actor with `read_only=True`. A store built by `main.py` through the pool has its shard keys, and the plot scripts
  read it as before. The two routes share the factories' `build`, which is what the exports exercise.
- **The round trip of a GeV column is exact in `Planck_units`** (0 of 55 000 cells differ from the raw column), since the raw value is
  `ln T - ln GeV` with both of the same magnitude (Sterbenz) and is formed again exactly. In other units a converted column's
  cell can differ from the raw one by an ulp; ruling 1's rule is the contract and `check_new_columns.py` measures it.
- **`--A-values` and `--alpha-values` share no value check**: A = 0.5 and α = 0.5 are two rows of the same plot run (the
  `Delta_N_eff` figure draws both, labelled by model). The histories.csv sort puts every `fixed` row before every one-loop row
  (string order of `drain_model`); a later model name sorts by its name.
- **A one-loop row's output sits in a directory the science-runs scripts' `alpha=*` glob does not match**, as P35 intends; any script
  that wants the one-loop products globs `one-loop-measured-v1/A=*` or the model it names.
- **`tools/history_and_bbn.py`'s `_law_alpha` and `tools/bbn_from_store.py`'s are two copies** (ruling 4: the first tool is not
  edited). A third reader would be a reason to move the function to `config/argument_parser.py`.
- **`outward_step_scan.py`'s `missing_in_store`** on a 2026.9.0 store (03's log §6) is still not acted on (prompt §3).
- **The test harness `_AlphaStoreCase.build`** takes pairs now; the `alpha_value` inserts in the other inventory tests are bare.
- **Cost:** `test_export_04` takes about 14 s (one integration of P-2197, 2-4 s, and 5 000-sample frames), `test_one_loop_tools` about 35 s
  (two integrations and the store copies), the rest are instant.


## 7. State handed to the next prompt

**For 05 (the derivation tool): the export format, exactly.** 05 reads only files; it opens no store. Every number is
written at full precision (`repr`), so a value read back by `float()` is the stored column exactly in the units the store was built
with (ruling 1: each cell is `to_column(from_column(raw, kind, units), kind, units)`, the raw value itself in `Planck_units`).

*Where the files are* (`<output>` is the plot scripts' `--output`; `<label>` is the cosmology's label, `QCD_Cosmology`):
- **histories.csv**: `<output>/<label>/histories.csv`, written by `plot_by_beta.py` once per cosmology, over every row of the run (fixed
  and one-loop), sorted by (`drain_model`, `alpha`, `M_Mp`, `n`, `beta`).
- **fields.csv**: `<output>/<label>/<row>/csv/beta=<β:.5g>/M=<M_eV:.5g>eV_n=<n>/fields.csv`, written by `plot_ScalarHistory.py` for every
  successful history it plots, under every law.
- **bounces.csv**: in the same directory as that history's fields.csv, for every successful **one-loop** history, never for a fixed one.
  A one-loop history with no bounce row has the header alone.
- `<row>` is `alpha=<α:.5g>` for a `fixed` row and `<drain_model>/A=<A:.5g>` (`one-loop-measured-v1/A=<A:.5g>`) for a one-loop row;
  `M_eV` is M in eV, `M_Mp × M_P/eV` with M_P/eV the reduced Planck mass in eV (`Units.base.REDUCED_PLANCK_MASS_EV`).

*How a history's three files are joined.* The directory carries β, M (eV) and n and the row carries A; a history's keys are
(`drain_model`, `alpha`, `M_Mp`, `n`, `beta`) in histories.csv. Match the directory's `<A:.5g>`, `<β:.5g>`, `<M_eV:.5g>` and `<n>` to the
same formatting of the histories.csv row (the directory values are rounded to five significant figures, the row's are not). fields.csv
and bounces.csv carry no key column; their rows belong to the history of their directory. The two files of one directory join row
to row only through `raw_N` (fields.csv) and `N` (bounces.csv), both the e-fold count.

*NULL and Booleans.* A blank cell is NULL (and, in histories.csv, any missing value). Booleans (`resolved`, `weighted`) are `0` and `1`,
blank where the column is NULL. Integers (counts, `ordinal`, `candidate_index`) are written as integers; `source`, `kind` and
`regime` are strings. A fixed record has every drain cell of histories.csv blank.

*The columns 05 reads, with units.* (units: the file's names; `_Mp` ln(x/M_P), `_Mp4` ln(x/M_P⁴) or x/M_P⁴ where the name says so,
`_GeV` ln(x/GeV))
- **histories.csv**: `drain_model`, `alpha` (α under `fixed`, A under the one-loop model), `M_Mp` (M/M_P), `n`, `beta`; `bounce_rows`
  (the count, to check bounces.csv against); and, dimensionless, `drained_share_{one_loop,gap,classical,unassessed}`, `parking_share`,
  `candidates_{one_loop,gap,classical,unassessed}`, `ell_min`, `log_R_min_drain`, `unresolvable_assessments`, `unresolved_declined`,
  `all_worst_{birth,reflection,tracker}_margin`, `all_worst_{…}_margin_N`, `all_{…}_failing_share`.
- **fields.csv** (one row per sample, sorted by `log_one_plus_z`, descending): `raw_N` (e-folds N), `log_one_plus_z` (ln(1 + z_E)),
  `log_one_plus_z_Jordan`, `log_T_Jordan_GeV` (ln(T_J/GeV)), `H_Einstein_Mp`, `H_Jordan_Mp` (H/M_P), `phi_Einstein_Mp`, `pi_Einstein_Mp`
  (M_P), `rho_chi_over_rho_rad`, and the two new columns `log_m_eff_Mp` and `log_m_tracker_Mp` (ln(m/M_P), the field's effective mass
  and the zero-lag tracker's; blank where not defined).
- **bounces.csv** (one row per bounce row, `ordinal` 0, 1, … in order; the 30 columns of `BOUNCE_COLUMNS`): `ordinal`; `source`
  (`candidate`, `plateau`, `wall`); `candidate_index` (blank for an event); `N`, `N_turn` (e-folds); `kind`; `resolved` (0/1);
  `log_T_Jordan_GeV` (ln(T_J/GeV)); `delta_K_Mp4` (ΔK/M_P⁴, linear); `log_rho_kin_Mp4` (ln(ρ_kin/M_P⁴)); `nonadiabaticity`, `alpha_eff`,
  `w_gate`; `K_before_Mp4` (K/M_P⁴, linear); `regime`, `ell`, `L`, `log_R`; `log_E_Mp4` (ln(E/M_P⁴)); `log_H_Mp` (ln(H_E/M_P));
  `log_k_ex_Mp`, `log_m_Mp`, `log_q_Mp` (ln(·/M_P)); `share`, `weighted` (0/1), `birth_margin`, `reflection_margin`, `reflection_margin_N`,
  `tracker_margin`, `tracker_margin_N`. A blank cell is NULL: an event row (`plateau`, `wall`) has only `ordinal`, `source`, `N`, `log_T_Jordan_GeV` and `log_m_Mp` (log 03
  §1.1 says which columns each row kind fills).
- **05 can work from these columns.** The list in log 03 §7 (n_g from `delta_K_Mp4`, `log_q_Mp` and `share`; E(N) from the sample masses
  and `raw_N`; the phase slip from `log_k_ex_Mp`, `log_H_Mp` and `N_turn`; the backreaction ratio from `log_m_Mp`, `log_E_Mp4`,
  `delta_K_Mp4` and `log_q_Mp`; K before each candidate from `K_before_Mp4`) is covered, and no column was added.

**For 06 (the rehearsal): the options of the command lines.**
- `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py` and `RunDrivers/registered_run.py` all take `--alpha-values` (fixed rows, default `0`)
  and `--A-values` (one-loop rows, no default). The rebuild of record is one invocation, for example
  `--alpha-values 0 --A-values 0.05 0.1 0.5 1 1.5 2 5 10` (U14), run on the isolated cluster with `--ray-address 127.0.0.1:6380`. A plot run
  must be given the same `--alpha-values` and `--A-values` as the build, or it stops (exit 2, naming the option): an A stored only as an α, or an
  α stored only as an A, is refused. A run given no `--A-values` is the fixed-law run of before, with the lines and paths of before.
- The registered run's scope string gains `A <values>` after the `α` part only when `--A-values` is given.
- The pipeline order is the fixed rows in the order of `--alpha-values`, then the one-loop rows in the order of `--A-values`; the
  plot scripts print `alpha values …` and, with `--A-values`, `A values … (one-loop-measured-v1)`.
- The tool route: `tools/history_and_bbn.py --drain-law one_loop --alpha A` (01), `tools/bbn_from_store.py STORE --beta B --M-Mp M
  --drain-law one_loop --alpha A` (this prompt; `--alpha 10` is valid under `one_loop`), `tools/outward_step_scan.py` reads the fixed rows
  only and prints one stderr line counting the other rows it skipped, and `main.py --inventory` labels a one-loop row `A=<A> (one-loop-measured-v1)`.
- The base of 06's bitwise comparison is `fixed` rows only. The fixed law's histories.csv and fields.csv of this prompt's tree are byte-identical
  to the dispatch tree's after the projection onto the old columns (verification §4.4); only the new columns differ.

