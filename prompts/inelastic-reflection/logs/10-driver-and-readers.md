# Log 10 — the driver and the readers

**Subject:** Give α an option and carry it through the drivers, the plots, the scope, the inventory and the tools
**Commit:** `8c12afa`, on branch `inelastic-reflection`, on top of `d097745`
**Date:** 2026-10-31, by Claude Sonnet 5.5
**Result:** **COMPLETE.** The gate "Driver" holds. No stop was needed. None opened, none closed.
- **What shipped.**
  - `--alpha-values` in the shared parser (`nargs="+"`, default `[0.0]`, replaced by a given list; α outside
    [0, 1) is refused at parse, exit 2). `distinct_values` collapses duplicates in the order of first appearance.
  - `main.py` and both plot scripts fetch one `alpha_value` row per distinct α and run `run_pipeline` once
    per row, inside the loop over the cosmologies.
  - `plot_inputs.parameters` takes `alpha_values` and returns `PlotParameters.alphas`; an unstored α is
    `PlotOptionNotStored("alpha_value", "--alpha-values", <α>)`.
  - Every per-α product of both plot scripts goes under `<output>/<label>/alpha=<α>/` (R26); `histories.csv`
    and `Delta_N_eff.pdf` span α.
  - `plot_Delta_N_eff` (R28) and `plot_dark_radiation` (R27), both in `extract_common.py`;
    `histories.csv` gains `alpha`, `Delta_N_eff_today`, `Delta_N_eff_1MeV`; `fields.csv` gains
    `rho_chi_over_rho_rad` and `Delta_gstar`.
  - The wrapper's scope reads `α <values>`; the inventory puts `alpha_value` among the parameters and `alpha`
    among the extra fields; `tools/history_and_bbn.py` and `tools/bbn_from_store.py` take `--alpha`.
- **The gate "Driver"** (README §8.4, prompt §4.1):
  - **α reaches every stage of `main.py`, once per value.** By `ast` (test 2): one lookup per value of
    `distinct_values(args.alpha_values)`, `run_pipeline` called once inside the loop over those rows with the
    loop's own row (B2), no literal α = 0 lookup left in any driver or in `plot_inputs.py`.
  - **A plot asked for an unstored α exits 2.** `plot_inputs` on a temporary store with α = 0 and 0.1: α = 0.3
    is `PlotOptionNotStored` naming `--alpha-values` (test 3, B3), and the drivers' handler prints it and
    `sys.exit(2)`s, by `ast`. The drivers themselves were not run: they open the pool after `ray.init`
    (ruling 6; §4.4).
  - **The scope and the inventory name α** (tests 8 and 9; §4.3).
  - **R26's paths and R27's and R28's figures:** tests 4, 5, 6 and 7, and §5.2.
  - **Both golden histories bitwise** (`test_golden_history`, `test_tuning` pass unedited), and **every α = 0
    output of the tools byte-identical** (§5.1).
- **Counts:**
  - before: 26 / 473 (98 + 375) / 121 / 115 / 64;
  - after: 26 / **497** (**122** + 375) / **129** / **123** / **68**;
  - all passing; the 44 new tests are listed in §2.3.
- No Ray was started. The only stores are the tests' temporary ones and the scratch stores of §4.3 and §5.1, deleted.
  Nothing was written to `var/`. No golden was written or regenerated. Both temporary worktrees were removed.

## 1. What shipped

### 1.1 The option and its refusals

`--alpha-values`, after `--tuning-tolerance`: `type=alpha_fraction`, `nargs="+"`, `default=[0.0]`, the default
action, so a given list replaces the default (B1 shows the `extend` action does not).

| Command line | Result |
|---|---|
| (none) | `[0.0]` |
| `--alpha-values 0.1` | `[0.1]`, not `[0.0, 0.1]` |
| `--alpha-values 0.03 0 0.1` | `[0.03, 0.0, 0.1]` |
| `--alpha-values -0` | `[0.0]`, with a positive sign (`float("-0") + 0.0`), so the row and `alpha=0/` have one spelling of zero |
| YAML `alpha-values: [0.03, 0.1]` (or the block list) | `[0.03, 0.1]`; the command line wins over the file |
| `--alpha-values -0.1` | exit 2: `main.py: error: argument --alpha-values: must be a fraction with 0 <= alpha < 1, not -0.1` |
| `1.0`, `1`, `2`, `nan`, `inf` | the same message, with the value |
| `0.1 1.0` | exit 2: one bad value refuses the list |

`distinct_values([0.1, 0.0, 0.1, 0.03, 0.0])` is `[0.1, 0.0, 0.03]`. The three drivers apply it once and use only
its result; the scope shows the list as given (§1.5).

`main.py` prints `   -- populated alpha grid with N value(s) = [ … ]` beside the β and M grids; the plot scripts print
one `@@ plot_by_beta: alpha values …` (or `plot_ScalarHistory`) line. Every banner reads
`>> RUNNING PIPELINE FOR MODEL <label>, alpha = <α:g>`.

### 1.2 The paths as built (R26)

Both scripts build every per-α path through `extract_common.alpha_path(base, alpha) = Path(base) /
f"alpha={alpha:.5g}"`, with the row's value (`alpha.value`; `model.alpha.value` in `plot_ScalarHistory`).
The output is `<out>`, the label `QCD_Cosmology`, M = 0.5 M_P (`1.2177e+27` eV), n = 1.

| Product | α = 0 | α = 0.1 |
|---|---|---|
| `plot_by_beta.py`, per (M, n) | `<out>/QCD_Cosmology/alpha=0/plots/M=1.2177e+27eV_n=1/BBN.pdf` (also `timings`, `max_Q`, `abundance_shifts`, `T_deliver`, `fixed_T`) | `<out>/QCD_Cosmology/alpha=0.1/plots/M=1.2177e+27eV_n=1/BBN.pdf` |
| `plot_by_beta.py`, CSV | `<out>/QCD_Cosmology/alpha=0/csv/M=1.2177e+27eV_n=1/data.csv` | `<out>/QCD_Cosmology/alpha=0.1/csv/M=1.2177e+27eV_n=1/data.csv` |
| `plot_by_beta.py`, figure 2 | `<out>/QCD_Cosmology/alpha=0/plots/M_convergence.pdf` | `<out>/QCD_Cosmology/alpha=0.1/plots/M_convergence.pdf` |
| `plot_by_beta.py`, spans α | `<out>/QCD_Cosmology/histories.csv` and `<out>/QCD_Cosmology/plots/M=1.2177e+27eV_n=1/Delta_N_eff.pdf` (written once, after the last α; none if no α > 0) | (the same files) |
| `plot_ScalarHistory.py` | `<out>/QCD_Cosmology/alpha=0/plots/beta=1.2/M=1.2177e+27eV_n=1/full_history.pdf` (also `thermo`, `Hubble`, `energy`, `ODE_terms`, `BBN_era`, `BBN_era_NP`) and `<out>/QCD_Cosmology/alpha=0/csv/beta=1.2/M=1.2177e+27eV_n=1/fields.csv` | the same under `alpha=0.1/`, and `…/plots/beta=1.2/M=1.2177e+27eV_n=1/dark_radiation.pdf` |

These are by construction (§2.2, test 4 by `ast`). The scripts were not run: they need Ray (§4.4).

### 1.3 The figures

- **`Delta_N_eff.pdf`** (R28): two panels sharing β, ΔN_eff at T_CMB (y-label with `(11/4)^{4/3}`, P7) above and
  at 1 MeV (`T_ν = T_γ`, R22) below, log axes, one curve with markers per α > 0 in increasing α. A NULL, or a value
  that is not positive, is left out of its panel and counted in one printed line. No α > 0 prints one line and
  writes nothing.
- **`dark_radiation.pdf`** (R27, P26): Δg* = `gstar_rho * rho_chi_over_rho_rad` above, ρ_χ/ρ_R below, shared x
  axis (e-folds, as `plot_ScalarHistory` calls it, with the T_J labels of the other figures), log y axes. A
  sample with ratio 0.0 is left out of both. Vertical lines at `parking_N` and `release_N`, each where it is not
  None. Written at α > 0 only.

### 1.4 The record, the CSVs

- `build_history_record(..., alpha, ...)`: `alpha` a required keyword after `phi_init_Mp`; `Delta_N_eff_today`
  and `Delta_N_eff_1MeV` from `scalar.dissipation`, NaN where missing.
- `CSV_COLUMNS`: `alpha` after `phi_init_Mp`; the two ΔN_eff after `rho_ratio_70keV`. `histories.csv` sorts by
  `(alpha, M_Mp, n, beta)`.
- `fields.csv` (`write_csv_file`): `rho_chi_over_rho_rad` and `Delta_gstar`, at every α (0.0 at α = 0).

### 1.5 The scope, the inventory, the tools

- **Scope:** `…; tuning tolerance 1e-05; α 0; 20 shards; …` by default; `α 0, 0.03, 0.1` for three; `α 7
  values, 0.01–0.07` for seven; duplicates as given.
- **Inventory:** `alpha_value` after `beta_value`; `("alpha", "alpha")` after `tuning_tol` in
  `HISTORY_EXTRA_FIELDS`. One α in a store prints `…, log10_tuning_tol=-5, alpha=0` on the common line and nothing
  per history; two print `alpha=0` and `alpha=0.1` per history and none on the common line (§5.3).
- **`tools/history_and_bbn.py --alpha`:** at α > 0 the `history` line has `alpha=0.1 ` before `wall=`; a `dissipation`
  line follows the `bounce` line. At α = 0 no line changes. The line, for M = 0.5, β = 2, α = 0.1:
  `dissipation beta=2 M=0.5: injections=… resolved_candidates=… … parking_log_T_Jordan_GeV=None …`
  (the field order is `DissipationValues._fields`, with the two ln T_J fields renamed and in GeV).
- **`tools/bbn_from_store.py --alpha`:** the header line gains ` alpha=0.1` after `phi=` at α > 0 only.

## 2. Tests

### 2.1 Existing tests edited

Each edit is of the kind prompt §1.10's list names, or ruling 1's.

| Module | Edit | Reason |
|---|---|---|
| `ComputeTargets/tests/test_plot_inputs.py` | `params.alpha.value` → `[a.value for a in params.alphas]`, and the "every parameter is a stored row" loop runs over `(*params[:-1], *params.alphas)`; `_parameters` passes `alpha_values=args.alpha_values`; `test_the_parameters` and the refusing-pool test pass `alpha_values=[0.0]` | `alpha` → `alphas`, and the α lookup by option (prompt §1.10). `parameters` takes `alpha_values` with no default, so a driver that forgot to pass it fails |
| `ComputeTargets/tests/test_extraction.py` | the three `build_history_record(...)` calls gain `alpha=0.0` after `phi_init_Mp`; `_synthetic_records`' history stand-in gains `dissipation=None` | ruling 1 |
| `ObjectFactories/tests/test_tuned_history_round_trip.py` | its one call gains `alpha=0.0`, the α of the history it stores | ruling 1 |
| `RunDrivers/tests/test_registered_run.py` | the three expected scopes gain `; α 0` after the tuning tolerance | the scope names α (prompt §1.10) |
| `tools/tests/test_inventory_report.py` | the common line gains `, alpha=0`; `alpha=0` leaves the two ScalarHistory lines and the `history: {…}` line | the inventory's α moves to the common line (prompt §1.10) |

`test_reflection_reporting`, `test_dissipation_store`, `test_adiabatic_dark_radiation`,
`test_bbn_sees_dark_radiation`, `dissipation_histories.py`, `test_driver_options`, `test_registered_run_streams`,
every golden module and JSON, and `test_bbn_from_store` pass unedited (the last by D1).

### 2.2 New tests

| Module | Test | Asserts |
|---|---|---|
| `ComputeTargets/tests/test_alpha_option.py` (13) | `test_1_the_default_is_zero_and_a_given_list_replaces_it` | default `[0.0]`; `0.1` gives `[0.1]`; `0.03 0 0.1` gives itself |
| | `test_1_the_yaml_key_gives_the_same_list` | `alpha-values: [0.03, 0.1]` and the block list give `[0.03, 0.1]`; the command line wins over the file |
| | `test_1_a_value_out_of_range_is_refused_at_parse_naming_the_option` | −0.1, 1.0, 1, 2, nan, inf: exit 2, `argument --alpha-values: must be a fraction`; one bad value refuses the list |
| | `test_1_minus_zero_is_zero` | `-0` is `0.0` with a positive sign, and `alpha_path` gives `alpha=0` for it (ruling 9) |
| | `test_1_duplicates_collapse_in_the_order_of_first_appearance` | `distinct_values` |
| | `test_1_the_help_says_what_alpha_is` | the help text |
| | `test_2_main_looks_up_one_row_per_value_of_the_list` | by `ast`: `alpha_values = distinct_values(args.alpha_values)`, one `alpha_value` lookup, `value=a`, in a comprehension over `alpha_values`, unpacked as `*alpha_rows` |
| | `test_2_every_pipeline_is_called_once_per_row_with_the_loops_own_row` | each driver: one loop over `alpha_rows`, inside the loop over `model_list`, calling `run_pipeline` once with the loop's variable; no `alpha_rows[` |
| | `test_2_the_plot_scripts_pass_the_deduplicated_list_to_plot_inputs` | both plot scripts pass `alpha_values=alpha_values` and look up no α row themselves |
| | `test_2_no_driver_looks_up_a_literal_alpha_of_zero` | `main.py`, both plot scripts and `plot_inputs.py` |
| | `test_4_alpha_path_names_the_value` | `alpha=0`, `alpha=0.1`, `alpha=0.03`, `alpha=0.12346`; zero is a level of its own |
| | `test_4_both_scripts_build_their_per_alpha_paths_through_it` | by `ast`: `alpha_path` called with the row's value (`alpha`, `alpha.value`, `model.alpha.value`); twice in `plot_by_beta.py` |
| | `test_4_every_figure_titled_with_the_cosmology_says_its_alpha` | every call of the two label helpers passes `alpha=` (ruling 2) |
| `ComputeTargets/tests/test_alpha_plot_inputs.py` (5) | `test_3_two_alpha_stored_both_rows_in_the_order_given` | rows 0 and 0.1 in the order given, each a stored row, distinct |
| | `test_3_an_alpha_not_stored_names_the_option_and_the_value` | `PlotOptionNotStored("alpha_value", "--alpha-values", "0.3")` and its whole message |
| | `test_3_the_first_missing_alpha_in_the_order_given_is_named` | α = 0.2 before 0.3; a missing `--rel-tol` before a missing α |
| | `test_3_the_read_only_pool_creates_no_alpha_row` | the `alpha_value` row count and the sha256 of every file are unchanged |
| | `test_3_both_scripts_stop_with_exit_2_on_an_alpha_not_stored` | by `ast`: the handler prints to stderr and `sys.exit(2)`s, around `plot_inputs.parameters` |
| `ComputeTargets/tests/test_bbn_from_store_alpha.py` (6) | `test_11_each_alpha_finds_its_own_history_and_the_default_the_zero_one` | α = 0, 0.1, 0.2 find serials 10, 20, 30; the default finds 10 |
| | `test_11_alpha_is_matched_as_the_dimensionless_factory_matches_it` | absolute at 0 (1e-9 is 0, 1e-6 is not), relative otherwise (1e-7 is, 1e-3 is not) |
| | `test_11_an_alpha_with_no_history_names_it_and_alpha_0_is_as_before` | the α = 0 message is the old one; at α > 0 it gains ` alpha=0.3` |
| | `test_11_a_shard_without_the_alpha_table_holds_alpha_0_only` | D1 |
| | `test_11_the_option_parses_through_alpha_fraction` | 1.0, −0.1, nan: exit 2 |
| | `test_11_the_cli_finds_each_alpha_without_a_solve` | `main` with the solve replaced: the header names α only above 0 and finds serials 10, 10, 20 |
| `ObjectFactories/tests/test_alpha_figures.py` (8) | `test_5_two_curves_per_panel_the_null_left_out_the_file_written` | α = 0.03, 0.1 at three β, a NULL at 1 MeV, an α = 0 record: two curves per panel in increasing α, the NULL out of the lower panel only, log axes, the P7 and R22 labels, the data by value, the file and its PNG written, the count line |
| | `test_5_a_value_that_is_not_positive_is_left_out_and_counted` | |
| | `test_5_alpha_zero_alone_writes_nothing` | `False`, one line, no figure, no file |
| | `test_6_the_three_columns_and_the_sort` | the columns' places; rows sorted by (α, M, n, β); a NULL is an empty cell |
| | `test_6_the_record_takes_alpha_from_the_caller_and_delta_N_eff_from_the_history` | `scalar` None, `dissipation` None, or set |
| | `test_7_the_file_the_delta_gstar_the_lines` | M = 10⁻³, β = 1.2, α = 0.1, read back from a temporary store: the file written; Δg* equals `gstar_rho * rho_chi_over_rho_rad` per sample by `==` (3 080 of 4 998 samples), and the ratio panel the ratio; ratio-0.0 samples left out; two vertical lines at `parking_N` and `release_N` exactly, on both panels; the title says `α = 0.1` |
| | `test_7_in_redshift_a_line_is_at_the_first_sample_at_or_after_its_n` | ruling 5 |
| | `test_7_at_alpha_zero_nothing_is_written` | `False`, no file, in both coordinates |
| `RunDrivers/tests/test_scope_alpha.py` (4) | `test_8_…` | the default reads `α 0` after the tuning tolerance; three values; seven values read as a count and a range; an α out of range is refused by the parser |
| `tools/tests/test_inventory_alpha.py` (4) | `test_9_…` | `alpha_value` follows `beta_value` and is in no other category; `alpha` follows `tuning_tol`; one α on the common line only; two α on each history line only |
| `tools/tests/test_history_and_bbn_alpha.py` (4) | `test_10_alpha_out_of_range_is_refused_at_parse` | 1.0, −0.1, nan, with nothing integrated |
| | `test_10_the_tuned_path_hands_alpha_to_the_tuning` | `tune_history`'s `alpha` is 0.0 and 0.1 |
| | `test_10_every_line_is_the_dispatch_commits` | the whole stdout at α = 0 equals `PINNED_ALPHA_ZERO`, taken at `d097745` (§5.1), with `wall=` and `PRyM_time=` masked; no `alpha=`, no `dissipation` |
| | `test_10_the_history_line_names_alpha_and_the_dissipation_line_parses_back` | α = 0.1: `alpha=0.1` before `wall=`; the `dissipation` line follows `bounce`; every field in `_fields` order; each printed value equals `float(f"{x:.6g}")` (ln T_J minus ln GeV), `None` as `None` |

Costs: `test_alpha_figures` 14 s (two 4-s integrations, load 5–9); `test_history_and_bbn_alpha` 7 s (two
integrations, no PRyMordial); every other new module under 1 s. The new modules add about 1 s to
`ComputeTargets/tests`' `test_[a-f]` half (the other two are in §4.1's times).

### 2.3 Tests by suite

| Suite | New tests |
|---|---|
| ComputeTargets, `test_[a-f]` | 13 + 5 + 6 = 24 |
| ObjectFactories | 8 |
| tools | 4 + 4 = 8 |
| RunDrivers | 4 |
| Total | 44 |

## 3. Deviations from the prompt

**The rulings of orchestrator note 10 (1–13):**

| # | Ruling, as applied | Class |
|---|---|---|
| 1 | `build_history_record(…, alpha, …)` required keyword; the record's two ΔN_eff from `scalar.dissipation`; `CSV_COLUMNS`, the sort and the docstring as ruled. `test_extraction` (three calls, the stand-in) and `test_tuned_history_round_trip` (one call) edited as §2.1 says and nothing else | STRUCTURALLY REQUIRED |
| 2 | `alpha=None` on both label helpers; the cosmology's line is `Cosmology: <label>, α = <α:g>`; every call in both scripts and in `plot_dark_radiation` passes it (test 4, by `ast`); `test_reflection_reporting` passes unedited | STRUCTURALLY REQUIRED |
| 3 | `alpha_path` in `extract_common.py`; `plot_by_beta.py` passes `alpha.value`, `plot_ScalarHistory.py` `model.alpha.value` | IMPLEMENTATION CHOICE (ruled) |
| 4 | `plot_by_beta.run_pipeline` returns `all_records`, writes `M_convergence.pdf` under `alpha_path(…)/plots/`, no longer writes `histories.csv`; `build_beta_plot` gains `alpha` (a float, after `group_model`); the driver block loops over the rows, then `write_alpha_summaries` writes `histories.csv` and one `Delta_N_eff.pdf` per (M, n) group. Work items, `RayWorkPool` parameters and stage order unchanged | STRUCTURALLY REQUIRED |
| 5 | `plot_dark_radiation(model, model_label, path, x_coord)` reads x through `get_x_coord`; exact lines in e-folds, the first sample at or after N in redshift; ratio-0.0 and non-positive Δg* left out; labels as the other figures; called after `BBN_era_NP_plot`; `plot_Delta_N_eff` leaves out and counts non-positive values; test 7 asserts both coordinates | IMPLEMENTATION CHOICE (ruled) |
| 6 | An α out of [0, 1) is refused by `alpha_fraction` at parse. An α in range that is not stored is `PlotOptionNotStored` after `ray.init`. `plot_inputs` is tested on a temporary store and the drivers' handler by `ast`; the drivers were not run (§4.4) | STRUCTURALLY REQUIRED |
| 7 | `alpha=<α:g>` on the `history` line only at α > 0, before `wall=`; the `dissipation` line only at α > 0, fields in `_fields` order, `:.6g`, ln T_J fields converted and renamed `…_GeV`; "parses back" is exact equality with `float(f"{x:.6g}")`; "byte for byte" masks `wall=` and `PRyM_time=` only | STRUCTURALLY REQUIRED |
| 8 | `find_model(…, *, alpha=0.0)`; the query matches α absolutely at 0 and relatively otherwise with `config.defaults`' constants; at α = 0 every line and message is unchanged, at α > 0 they gain `alpha=<α:g>` after `phi=`; `--alpha` uses `alpha_fraction` | STRUCTURALLY REQUIRED; IMPLEMENTATION CHOICE for the message |
| 9 | `alpha_fraction` returns `float(text) + 0.0`; `distinct_values` in `config/argument_parser.py`, applied once in each driver; the alpha grid line in `main.py`, one `@@` line in the plot scripts; the scope shows α as given | IMPLEMENTATION CHOICE (ruled) |
| 10 | α rows in the tolerances' `ray.get`, one per distinct α, in order; `warn_super_planckian` before the loop; `run_pipeline`'s signature unchanged, its header gains `, alpha = <α:g>`; test 2's "no literal lookup" covers `main.py`, both scripts and `plot_inputs.py` | IMPLEMENTATION CHOICE (ruled) |
| 11 | Test 7 uses 09's fixture and `_DissipationStoreCase`, not copies; the bitwise check reads `ax.lines[0].get_ydata()` and compares by `==` against `[v.gstar_rho * v.rho_chi_over_rho_rad for v in values if v.rho_chi_over_rho_rad > 0.0]` | IMPLEMENTATION CHOICE (ruled) |
| 12 | The files are the prompt's §6 list, ruling 1's two test modules, and the tests D1 and D2 name. Golden modules and JSONs, `ComputeTargets/` and `ObjectFactories/` outside `tests/`, the seven frozen tests, the surfing tools and `CLAUDE.md` are byte-identical (`git status` in §4.5) | as ruled |
| 13 | Suites serial, `ComputeTargets` in halves, to scratch files with `uptime` (§4.1); `test_[g-z]` in the background; no failure needed a re-run; §5.3's parse times are the minimum of five | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | **`find_model` takes a shard with no `alpha_value` table as holding α = 0 histories only.** Prompt §1.8 and ruling 8 join `alpha_value` unconditionally. `test_bbn_from_store`'s tests (c) and (d), which neither prompt nor rulings allow edited, build an imitation store with no `alpha_value` table and no `alpha_serial` column, so the unconditional join fails both with `no such table`. I did not edit them (any other edit of an existing test is a stop). `_find_query` looks for the table in `sqlite_master`: absent, it uses the old query (α = 0 only; at α > 0 the shard cannot match); present, the join and the α match. At α = 0 on a store with the table the query is ruling 8's. The alternative is to edit the imitation store; it is the orchestrator's to prefer. Both tests pass unedited; `test_11_a_shard_without_the_alpha_table_holds_alpha_0_only` shows it | IMPLEMENTATION CHOICE |
| D2 | `parameters`' `alpha_values` has no default, so four calls in `test_plot_inputs.py` gain an argument (§2.1) and `PlotParameters.alphas` is a tuple, so one loop of that module's first test iterates `(*params[:-1], *params.alphas)`. Listed in §2.1; all of it is "the α lookup by option" | STRUCTURALLY REQUIRED |
| D3 | `plot_by_beta.py` gains `write_alpha_summaries(model_label, records)` for the two products that span α, which the driver block calls once per cosmology. `extract_common.py` gains `_cosmology_line`, `_event_x` and `DELTA_N_EFF_PANELS` | IMPLEMENTATION CHOICE |
| D4 | `plot_Delta_N_eff` prints its count line every time it draws, not only when something was left out. `plot_dark_radiation` prints one line at α = 0 per history (the scripts call it for every history) | IMPLEMENTATION CHOICE |
| D5 | Stale prose corrected where `8c12afa` makes it false: `plot_inputs.py`'s module docstring ("the constant α = 0 until prompt 10") and `parameters`' docstring; `describe_scope`'s docstring; `tools/history_and_bbn.py`'s and `tools/bbn_from_store.py`'s module docstrings (the option and the lines) | IMPLEMENTATION CHOICE |
| D6 | `tools/history_and_bbn.py` gains `dissipation_values_printed` and `dissipation_line`, module-level so that the test can import them; the test compares against its own conversion from `DissipationValues` | IMPLEMENTATION CHOICE |
| D7 | `test_alpha_plot_inputs.py` builds its store with `test_plot_inputs`' own `_build_run`, then one `alpha_value` row for 0.1, and registers it in that module's `_MODULE` so `_ReadOnlyCase.copy` finds it | IMPLEMENTATION CHOICE |
| D8 | `ComputeTargets/tests/test_driver_options.py` was not edited: its four `ast` assertions on `main.py` (`itertools.product`, `m.compute`, `pool.object_get('tolerance', tol=args.tuning_tolerance)`, `warn_super_planckian`) still hold with the α loop around `run_pipeline`, and `tuning_tol=args.tuning_tolerance` is still in the `plot_inputs.parameters` call | IMPLEMENTATION CHOICE |
| D9 | The architecture note sits at the end of §7.2, after 09's. `numerical-strategies.md` is not edited: Δg* is P26's product and needs no derivation | IMPLEMENTATION CHOICE |

## 4. Verification performed

### 4.1 The suites

Serial, `ComputeTargets` in its two halves, to files in the scratch directory. `uptime` is the load averages (1,
5, 15 min) at each suite's start.

| Suite | Before (tree `d097745`) | After |
|---|---|---|
| CosmologyModels | 26 OK, 65.7 s, load 6.41 9.63 8.08 | 26 OK, 64.7 s, load 4.99 6.17 7.09 |
| ComputeTargets `test_[a-f]` | 98 OK, 128.6 s, load 6.50 8.98 7.95 | **122** OK, 128.9 s, load 4.21 5.68 6.83 |
| ObjectFactories | 121 OK, 64.5 s, load 5.43 7.68 7.57 | **129** OK, 80.1 s, load 2.90 4.62 6.23 |
| tools | 115 OK, 131.5 s, load 4.80 7.12 7.37 | **123** OK, 135.3 s, load 8.32 5.46 6.37 |
| RunDrivers | 64 OK, 27.0 s, load 7.47 7.45 7.47 | **68** OK, 23.5 s, load 17.87 11.71 8.85 |
| ComputeTargets `test_[g-z]` | 375 OK, 451.7 s, load 7.35 7.41 7.45 | 375 OK, 446.3 s, load 13.20 11.10 8.72 |

The before run began with `uptime` 21:29 and the after run with 22:03. The counts rose by the 44 new tests only (§2.3).
The ObjectFactories time rose by the 14 s of `test_alpha_figures`. A failure before any edit would have been a
stop; there was none. `black --check` is clean on the 22 changed `.py` files (the 15 modified and the 7 new).

### 4.2 The `--help`s

`main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `RunDrivers/registered_run.py`,
`tools/history_and_bbn.py` and `tools/bbn_from_store.py` exit 0. The three drivers' helps show
`--alpha-values` (twice each: the usage and the option), the tools' `--alpha`. The wrapper's help describes the
wrapper's own options and `main.py`'s follow `--`.

### 4.3 `main.py --inventory` on a temporary store with two α

A one-shard store was built by 09's harness with M = 0.5 M_P, β = 2 at α = 0 and 0.1 (the harness stores no
`QCD_Cosmology` row, so one was added by `sqlite3` into the copy), read by `main.py --inventory` (exit 0, no
Ray):
- `problems: none`;
- `alpha_value` is in "Redshifts, tolerances and parameter values", after `beta_value`, with `values = [ 0, 0.1 ]`;
- the common line is `cosmology=QCD_Cosmology(…), log10_atol=-8, log10_rtol=-8, log10_tuning_tol=-5`, with no α;
- the two history lines end `failure=False, alpha=0  | values: 5,287` and `failure=False, alpha=0.1  | values: 5,287`.

The one-α case is `test_the_scalar_histories` (edited) and `test_9_one_alpha_is_printed_once_on_the_common_line`.
The store was deleted.

### 4.4 A plot driver given an unstored α

The plot scripts open the pool after `ray.init`, and no Ray was to be started. So the fallback of prompt §3.2 and
ruling 6 was taken: `plot_inputs.parameters` is tested on a temporary store (test 3), and the drivers' handler by
`ast`. **The three drivers were not run end to end**, and the code of `plot_by_beta.py` (`build_beta_plot`,
`run_pipeline`, `write_alpha_summaries`, the driver loop) and of `plot_ScalarHistory.py` (the paths, the CSV, the
call of `plot_dark_radiation`) is shown by `ast`, `py_compile` and the tests of the functions it calls in
`extract_common.py`, not by a run. Their first run is 11's rehearsal (U11) on the isolated cluster.

### 4.5 The tree

`git status` shows 15 modified files and 7 new test modules, and `.documents/architecture-summary.md`, the log and
the board. Nothing under `ComputeTargets/` or `ObjectFactories/` outside `tests/` changed, and no golden.

## 5. The measurement (prompt §2)

The scratch directory is `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/11b6df77-ac98-4313-9afa-44a3964fdaea/scratchpad/agent10/`.

### 5.1 α = 0 is unchanged (§2.1)

M = 0.5, β = 2, `test_injection`'s setup, untuned, `--fixed-normalization -277.3027772506518
0.7439937496556057` (the model's seed realization: `ExponentialModel.seed`, its normalization and phi_ref in M_P),
small network. The dispatch tree was a temporary `git worktree` of `d097745` (`wt`, then `wt2` for §5.3), both
removed with `git worktree remove`; `git worktree list` shows only the main checkout.

| Output | Dispatch (`d097745`) | Final tree | Result |
|---|---|---|---|
| `tools/history_and_bbn.py 2 0.5 --fixed-normalization … --small-network`, the real PRyMordial solve (1 227 bytes after masking) | `hb-real-dispatch.txt`: `Yp=0.04743587711 DoH=0.06103858872 He3oH=0.5153551783 Li7oH=4.805815583`, PRyM 8.7 s | `hb-real-final.txt` | **byte-identical** with `wall=` and `PRyM_time=` masked (`cmp`) |
| `tools/bbn_from_store.py` on a temporary store holding that history at α = 0 (504 bytes) | `bfs-dispatch.txt` | `bfs-final.txt` | **byte-identical** with `wall=` masked |
| the same, on a store holding α = 0 and 0.1, with no `--alpha` | — | `bfs-final-two-default.txt` | **byte-identical** to the dispatch's: the default finds the α = 0 history |
| `bbn_from_store.py … --alpha 0.1` on that store | — | `bfs-final-two-a01.txt` | finds serial 2 (RHS 36 666, 1 476 window samples), header `phi=5 alpha=0.1:` |

The masks are those of ruling 7. The test's pinned lines (`PINNED_ALPHA_ZERO`, `pins.py`, stub BBN) were taken at
the dispatch commit in `wt`, and pass at the final tree.

### 5.2 The figures (§2.2)

Drawn once each, committed nowhere. The ΔN_eff records come from 09's fixture histories (the eight of log 09 §5.1,
untuned, α = 0.1) and four more at α = 0.03 (M = 0.5, β = 1.2, 2, 5, 25); the figures were drawn per (M, n) group, as
`plot_by_beta.py` does. The dark-radiation histories were stored and read back on a temporary store. `figs.py`.

| File (under `…/agent10/figures/`) | Content | y-ranges (data) |
|---|---|---|
| `Delta_N_eff_M0.5_n1.pdf` | M = 0.5: β = 1.2, 2, 5, 25 at α = 0.03 and 0.1 | T_CMB panel 9.33×10⁻⁴ – 6.54×10⁻² (axis 7.5×10⁻⁴ – 8.1×10⁻²); 1 MeV panel 9.04×10⁻⁴ – 6.07×10⁻² (axis 7.3×10⁻⁴ – 7.5×10⁻²). Two curves of four points each |
| `Delta_N_eff_M0.001_n1.pdf`, `…_M1e-12_…`, `…_M1e-16_…`, `…_M1e-30_…` | one history each, α = 0.1 | one point per panel: 0.0657 and 0.0608 (M = 10⁻³); 0.0524 and 0.0496 (M = 10⁻¹², 10⁻¹⁶, 10⁻³⁰) |
| `dark_radiation_b1.2_M1e-3_efolds.pdf` (and `_redshift`) | M = 10⁻³, β = 1.2 | 3 080 of 4 998 samples; Δg* 0.0298 – 0.617 (axis 0.0256 – 0.718); ρ_χ/ρ_R 8.82×10⁻³ – 1.38×10⁻² (axis 8.6×10⁻³ – 1.41×10⁻²); x (N) 17.7 – 46.0 |
| `dark_radiation_b2_M1e-30_efolds.pdf` (and `_redshift`) | M = 10⁻³⁰, β = 2 | 3 228 of 5 437 samples; Δg* 0.0238 – 0.349 (axis 0.0208 – 0.399); ρ_χ/ρ_R 3.76×10⁻³ – 9.14×10⁻³ (axis 3.6×10⁻³ – 9.55×10⁻³); x (N) 20.4 – 50.1 |

The ranges agree with log 09 §5.2: ΔN_eff(T_CMB) 0.0654 (M = 0.5, β = 1.2), 0.00123 (β = 25) at α = 0.1; 0.0657 at
M = 10⁻³; 0.0524 at M = 10⁻³⁰. The last r_χ plotted is `rho_chi_ratio_today` (8.82×10⁻³ and 7.04×10⁻³ ⇒ the
panel's lower end). **The lines fall where log 09 §5.2's columns put them**, to the bit:
- M = 10⁻³, β = 1.2: parking at N = 28.371911595351886 and release at N = 45.91020342925626 (the stored
  `parking_N` 28.3719 and `release_N` 45.9102), both lines drawn;
- M = 10⁻³⁰, β = 2: parking at N = 30.349329421482498 (stored 30.3493), no release, so one line.

In redshift the lines are at 1 + z = 4.66×10⁷ and 1.127 (M = 10⁻³), and 3.66×10⁸ (M = 10⁻³⁰). The parked
interval's ratio is flat, the plateau the figure shows after the parking line at M = 10⁻³. The PDFs:
- `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/11b6df77-ac98-4313-9afa-44a3964fdaea/scratchpad/agent10/figures/Delta_N_eff_M0.5_n1.pdf`
- `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/11b6df77-ac98-4313-9afa-44a3964fdaea/scratchpad/agent10/figures/dark_radiation_b1.2_M1e-3_efolds.pdf`
- `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/11b6df77-ac98-4313-9afa-44a3964fdaea/scratchpad/agent10/figures/dark_radiation_b2_M1e-30_efolds.pdf`
- and the others in that directory, with a PNG beside each.

### 5.3 The cost (§2.3)

Minimum of five runs of each `--help`, tree `d097745` against the final tree, interleaved (`timing.py`); `uptime` before
22:01, load 5.08 6.37 7.24, after 22:02, 4.97 6.21 7.11.

| Script | Before (s) | After (s) |
|---|---|---|
| `main.py --help` | 1.338 | 1.356 |
| `plot_by_beta.py --help` | 1.862 | 1.815 |
| `plot_ScalarHistory.py --help` | 1.826 | 1.905 |
| `RunDrivers/registered_run.py --help` | 0.050 | 0.047 |
| `tools/history_and_bbn.py --help` | 1.170 | 1.160 |
| `tools/bbn_from_store.py --help` | 0.975 | 0.954 |
| parser build and parse, in process | 0.48 ms | 0.48 ms |

Nothing moves beyond the load's noise (about 5 %).

## 6. The deliberate-breakage record (P11)

Each diff was made against the final tree, checked with `git apply --check` and applied in `bash`; the tests below ran;
the diff was reverted with `git apply -R --check` then `git apply -R`, and the file compared equal to its saved copy
(`cmp`). None is committed. The diffs are in `…/agent10/breakages/B1.diff`…`B8.diff`.

**B1: `--alpha-values` with `action="extend"`.** Fails 3 tests, in 5 subtests: `test_1_the_default_is_zero_and_a_given_list_replaces_it`,
`test_1_duplicates_collapse_in_the_order_of_first_appearance`, `test_1_the_yaml_key_gives_the_same_list` (3 subtests).
```diff
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -222,6 +222,7 @@
         "--alpha-values",
         type=alpha_fraction,
         nargs="+",
+        action="extend",
         default=[0.0],
         help="one or more values of the drained fraction alpha, 0 <= alpha < 1: the fraction of the "
         "condensate's kinetic energy that dissipation mode drains at each candidate. It is a key of "
```

**B2: `main.py` passing the first α row to every `run_pipeline` call.** Fails 1:
`test_2_every_pipeline_is_called_once_per_row_with_the_loops_own_row`.
```diff
--- a/main.py
+++ b/main.py
@@ -1159,7 +1159,7 @@
                 atol,
                 rtol,
                 tuning_tol,
-                alpha,
+                alpha_rows[0],
                 solvers,
                 {
                     "samples_per_log10_z": samples_per_log10_z,
```

**B3: `plot_inputs.parameters` looking up α = 0 whatever is given.** Fails 5:
`test_3_two_alpha_stored_both_rows_in_the_order_given`, `test_3_an_alpha_not_stored_names_the_option_and_the_value`,
`test_3_the_first_missing_alpha_in_the_order_given_is_named`, `test_3_the_read_only_pool_creates_no_alpha_row`, and
`test_2_no_driver_looks_up_a_literal_alpha_of_zero`.
```diff
--- a/plot_inputs.py
+++ b/plot_inputs.py
@@ -170,7 +170,7 @@
         ),
     ] + [
         (
-            pool.object_get("alpha_value", value=a),
+            pool.object_get("alpha_value", value=0.0),
             "alpha_value",
             "--alpha-values",
             _value(a),
```

**B4: the `alpha=` level dropped at α = 0.** Fails 2: `test_4_alpha_path_names_the_value`, `test_1_minus_zero_is_zero`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -109,7 +109,7 @@
     included (`alpha=0`). The alpha is the stored row's value, never the float the user typed
     (inelastic-reflection prompt 10, R26, ruling 3).
     """
-    return Path(base) / f"alpha={alpha:.5g}"
+    return Path(base) if alpha == 0.0 else Path(base) / f"alpha={alpha:.5g}"
 
 
 def add_beta_summary_labels(
```

**B5: `plot_Delta_N_eff` drawing α = 0 as a curve.** Fails 2: `test_5_two_curves_per_panel_the_null_left_out_the_file_written`,
`test_5_alpha_zero_alone_writes_nothing`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -1231,7 +1231,7 @@
     positive is left out of its panel; one line is printed with the number left out of each. Returns
     False, writing nothing, if no record has alpha > 0 (a store at alpha = 0 alone).
     """
-    alphas = sorted({r["alpha"] for r in records if r["alpha"] > 0.0})
+    alphas = sorted({r["alpha"] for r in records if r["alpha"] >= 0.0})
     if len(alphas) == 0:
         print(
             "!! plot_Delta_N_eff: no record has alpha > 0; Delta N_eff figure skipped"
```

**B6: Δg* drawn as `rho_chi_over_rho_rad` alone (g_ρ left out).** Fails 1: `test_7_the_file_the_delta_gstar_the_lines`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -1308,7 +1308,7 @@
     points = [
         (
             get_x_coord(v, x_coord),
-            v.gstar_rho * v.rho_chi_over_rho_rad,
+            v.rho_chi_over_rho_rad,
             v.rho_chi_over_rho_rad,
         )
         for v in values
```

**B7: the scope without α.** Fails 6: the new `test_8_the_default_reads_alpha_0_after_the_tuning_tolerance`,
`test_8_three_values`, `test_8_more_than_six_values_read_as_a_count_and_a_range`, and the three existing scope tests,
`test_the_pilots_scope`, `test_more_than_six_values_read_as_a_count_and_a_range` and
`test_the_manifest_records_the_program_the_store_the_scope_and_the_packages`.
```diff
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -697,7 +697,6 @@
     parts.append(f"n {_values(parsed.n_values)}")
     parts.append(f"φ* {_number(parsed.phi_init_Mp)} Mp")
     parts.append(f"tuning tolerance {_number(parsed.tuning_tolerance)}")
-    parts.append(f"α {_values(parsed.alpha_values)}")
     parts.append(f"{parsed.shards} shards")
     parts.append(f"job {parsed.job_name}")
     return "; ".join(parts)
```

**B8: `bbn_from_store.py`'s query without its α match.** Fails 4, 1 failure and 3 errors (5 entries):
`test_11_each_alpha_finds_its_own_history_and_the_default_the_zero_one` (error: three histories match),
`test_11_alpha_is_matched_as_the_dimensionless_factory_matches_it` (error), `test_11_the_cli_finds_each_alpha_without_a_solve`
(error in 3 subtests) and `test_11_an_alpha_with_no_history_names_it_and_alpha_0_is_as_before` (failure).
```diff
--- a/tools/bbn_from_store.py
+++ b/tools/bbn_from_store.py
@@ -257,9 +257,7 @@
         if alpha == 0.0:
             return _FIND_QUERY_WITHOUT_ALPHA, (beta, phi_init_Mp)
         return None
-    if alpha == 0.0:
-        return _FIND_QUERY + _ALPHA_ZERO, (beta, phi_init_Mp, alpha)
-    return _FIND_QUERY + _ALPHA_RELATIVE, (beta, phi_init_Mp, alpha, alpha)
+    return _FIND_QUERY, (beta, phi_init_Mp)
```

Not caught by a test of this prompt, by design: tests 10 and the inventory's tests do not have a B of their own in §1.10.

## 7. Observations not acted on

- **`alpha_path` names α to five significant figures** (`:.5g`, as ruled). Two α that differ beyond the fifth figure,
  such as 0.1 and 0.100001, are two rows and one directory, and the second run would overwrite the first. The
  parser accepts both. Not acted on: a measurement grid does not have such values.
- **`tools/bbn_from_store.py --csv` does not carry α.** Its rows have `beta`, `M_Mp` and `phi_init_Mp` and no `alpha`
  column, so two α of one model are told apart only by `input` text, which also has no α. The prompt did not ask
  for it, and `CSV_FIELDS` is that tool's format.
- **`plot_dark_radiation` prints one line per α = 0 history** (`alpha = 0, no dark radiation to plot`), because
  `plot_ScalarHistory.py` calls it for every history. On a store of thousands of α = 0 histories this is noise;
  the call could test α first. D4.
- **`plot_dark_radiation` in redshift** places a line at a sample, which the prompt's figures do not use
  (`plot_ScalarHistory.py` always calls with `"efolds"`). It is tested (ruling 5) but not reached by any script.
- **The drivers' first end-to-end run with α is 11's** (§4.4). The `plot_by_beta.py` restructuring (ruling 4) is the
  largest untested-by-run change of this prompt.
- **A figure's title carries α only through the helpers that take it.** The titles built by `plot_abundance_shifts`,
  `plot_T_deliver`, `plot_fixed_T` and `plot_convergence_in_M` are the strings the scripts pass, and the scripts now
  put `, α = <α:g>` in them; those four functions are unchanged.
- **`registered_run.py --help`** does not list `--alpha-values`: it prints the wrapper's own options, and `main.py`'s
  follow `--`. The shared parser does accept it, and the scope names it.
- `[00-xav-sigma-is-non-monotone-at-25-35-keV]` and `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]`, open, are not
  touched.

## 8. State handed to the next prompt

For 11, the measurement (U10) and the rehearsal (U11):
- **A grid over α with `main.py`.** `--alpha-values A1 A2 …` replaces the default `[0.0]`; the list is
  deduplicated in order. Each value is one `alpha_value` row, and `run_pipeline` runs once per row inside each
  cosmology. A registered run takes it after `--`:
  `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign C --prompt N --register SLUG --purpose "…" -- --alpha-values 0 0.03 0.1 …`.
  The scope then reads `α 0, 0.03, 0.1`. The banner names α, and `main.py` prints `-- populated alpha grid …`. An α of
  1 or more, or negative, or nan, is refused at parse, exit 2. A store may hold α = 0 and α > 0 histories side by
  side: the inventory prints `alpha=` on each history line (and `alpha_value` among the parameters).
- **A grid over α with `tools/history_and_bbn.py`.** One α per invocation:
  `./venv/bin/python tools/history_and_bbn.py BETA M --alpha A [--fixed-normalization NORM PHI_REF_MP]`.
  The tuned path hands α to `tune_history`; `--fixed-normalization` to `integrate_history` (untuned, to T_CMB, about
  4–6 s). At α > 0 the `history` line gains `alpha=A` before `wall=` and a `dissipation` line follows the `bounce`
  line; at α = 0 nothing changes (byte-identical to `d097745`, §5.1).
- **The `dissipation` line's fields**, in order, each `name=value` with `:.6g` and None as `None`:
  `injections`, `resolved_candidates`, `unresolved_candidates`, `injected_fraction`, `rho_chi_ratio_1MeV`,
  `rho_chi_ratio_70keV`, `rho_chi_ratio_today`, `Delta_N_eff_1MeV`, `Delta_N_eff_today`, `log_R_first_impact`,
  `parking_N`, `parking_log_T_Jordan_GeV`, `release_N`, `release_log_T_Jordan_GeV`, `worst_birth_margin`,
  `worst_birth_margin_N`, `birth_failing_share`, `worst_reflection_margin`, `worst_reflection_margin_N`,
  `reflection_failing_share`, `worst_tracker_margin`, `worst_tracker_margin_N`, `tracker_failing_share`,
  `unassessed_share`. These are `DissipationValues._fields`, with the two ln T_J fields converted to GeV as the
  store converts them (log 09 §2.1). The line is the payload's (`dissipation_values(data)`); after a run on a store
  the same values are `ScalarHistory.dissipation` (ln T_J in the cosmology's units) and the 24 columns.
- **`tools/bbn_from_store.py --alpha A`** finds the history of that α (default 0), matched absolutely at 0 and
  relatively otherwise; its header gains ` alpha=A` at A > 0. A shard with no `alpha_value` table holds α = 0 only
  (D1). Its `--csv` rows have no α (§7).
- **Where each figure lands.** Under `<output>/<label>/alpha=<α:.5g>/`: `plot_by_beta.py`'s `timings`, `BBN`, `max_Q`,
  `abundance_shifts`, `T_deliver`, `fixed_T` (in `plots/M=…eV_n=…/`), `csv/…/data.csv` and `plots/M_convergence.pdf`;
  `plot_ScalarHistory.py`'s figures and `fields.csv` (in `plots/beta=…/M=…eV_n=…/`, `csv/beta=…/…`), with
  `dark_radiation.pdf` beside them at α > 0 only. Spanning α, at `<output>/<label>/`: `histories.csv` (sorted by α, M,
  n, β; with `alpha`, `Delta_N_eff_today`, `Delta_N_eff_1MeV`) and `plots/M=…eV_n=…/Delta_N_eff.pdf`, written once
  per cosmology after the last α, and only if some record has α > 0. A plot asked for an α the store lacks exits 2
  after `ray.init`, naming `--alpha-values`; a plot's own options must match the build's (the tolerances, T*, φ*,
  `--tuning-tolerance`, `--n-values`, and now `--alpha-values`).
- **The inventory's α.** `alpha_value` is under "Redshifts, tolerances and parameter values"; the ScalarHistory common
  line has `alpha=` where one α is stored, and each history line where several are.
- **The scope.** `describe_scope` reads `α <values>` after the tuning tolerance, as given.
- **The α = 0 output** of `main.py`, the plots' α = 0 figures (moved to `alpha=0/`) and the tools is the old one. The
  drivers were not run (§4.4): the rehearsal is the first run of the three with a Ray cluster and α.
- **Counts after this prompt:** 26 / **497** (**122** + 375) / **129** / **123** / **68**. `CLAUDE.md`'s counts are still
  those of `tuning-followups` 02; this campaign's close (12) updates them and gives its plot-scripts bullet
  `--alpha-values`.
- **Issues:** none opened, none closed. The index is unchanged (count 25).
