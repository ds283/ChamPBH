# Log 08 — the driver and the tools

**Subject:** Give the driver its tuning options and tune the tools' histories
**Commit:** `e624f1a`, on branch `jordan-normalization`, on top of `3229c53`
**Date:** 2026-10-26, by Claude Opus 5.5
**Result:** **COMPLETE**, after one stop ruled by the orchestrator.
- D1–D7 shipped under orchestrator note 08, whose sixteen rulings govern where they differ from
  the prompt.
- **The stop.** `ComputeTargets/tests/test_scalarhistory_failure_reason.py` borrowed
  `tools.history_and_bbn.LAMBDA_EV`, which ruling 8 removes, and the note's file list did not hold
  the test. The orchestrator ruled option A: the test holds its own `LAMBDA_EV = 1.0e-3`
  (Deviation 14).
- **Both golden histories pass unchanged and bitwise.** `tools/history_and_bbn.py 3 0.01` prints
  the tuned golden's `tuning` record bitwise, all fifteen keys.
- **The short surfing scan** (β = 1.50, 3.00, 4.00, tuned to T_CMB) gives tuned values equal to the
  orchestrator's probe bitwise. It reproduces the committed rows: the roots to 5.1 × 10⁻¹⁵, the
  capture flags and kicks exactly, and every other temperature and N to at most 1.1 × 10⁻⁷
  relative. Predicted full scans: S1 about 55 minutes, S5 about 35.
- Counts: CosmologyModels 26, ComputeTargets 243 (76 + 167, +18), ObjectFactories 93 (+1), tools
  115 (+13), RunDrivers 64 (+3). One test removed, `test_an_unknown_potential_type`, whose subject
  `e624f1a` deletes (ruling 15).
- Closed: `[05-surfing-scan-runs-at-a-fixed-lambda]` (`surfing-temperature`). Opened:
  `[08-a-history-that-did-not-converge-is-never-retried]` (ruling 14). The index stays at 22 open.
- **An incident** during the breakage replay registered a run in `var/runs` (Observations,
  item 1). It is left for the user to end.

## What shipped

### D1 — the options (rulings 1–5)
- **`config/argument_parser.py`.**
  - `REMOVED_OPTIONS` maps each removed option to the prompt that removed it and its replacement.
    `RemovedOption`, an `argparse.Action` (`nargs="*"`, `help=SUPPRESS`, `default=SUPPRESS`), calls
    `parser.error(f"{option_string} was removed by jordan-normalization prompt NN: …")`. The removed
    options are registered last: the five Λ options and `--T-stop-GeV` (07), and `--potential-type`
    (08). `potential_types` is gone.
  - New: `--tuning-tolerance` (`positive_float`, default `DEFAULT_TUNING_TOLERANCE`) beside the
    tolerances; `--tuning-max-integrations` (`positive_int`, default
    `DEFAULT_TUNING_MAX_INTEGRATIONS`); `--n-values` (`positive_int`, `nargs="+"`, `default=[1]`,
    the default action) beside the M options. Both defaults are imported from
    `ComputeTargets.ScalarHistory`. New types: `positive_float`, `positive_int`.
- **`main.py`.**
  - The grid is `itertools.product(M_grid, n_values, beta_grid)`, each payload's `n` from it.
  - The grid line is now `   -- using model ExponentialModel, V = V0 exp((M/phi)^n), with n = [ 1 ]`.
  - `tuning_tol` is `object_get("tolerance", tol=args.tuning_tolerance)`, and
    `compute_solver_batch` passes `max_integrations=args.tuning_max_integrations`.
  - `DEFAULT_TUNING_TOLERANCE` is no longer imported.
- **`ComputeTargets/ScalarHistory.py`**, in `ScalarHistory.compute` only: the new
  `max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS` argument is passed to
  `compute_scalar_history.remote`. Its docstring says the cap is not a key.
- **`exponential.yaml`** loses `potential-type` and `log10-one-plus-z-high: 85`, so 03's default of
  300 applies. Note 08 §0 gives the reason: 85 decades are 195.7 e-folds, against histories that
  need up to about 200.
- **`plot_inputs.py`.**
  - `MODEL_CLASS = "ExponentialModel"`; `MODEL_CLASSES` and `MODEL_N` are gone.
  - `exponential_models(pool, M_table, beta_table, n_values, units)` makes one lookup per
    `product(M_table, n_values, beta_table)`, each read on its own. A missing combination is
    skipped with its warning (giving its n) and counted, with the count line's text unchanged.
  - After the count, an n with no stored model at any stored (M, β) raises
    `PlotOptionNotStored("ExponentialModel", "--n-values", n)`.
  - The tuning tolerance's miss names `--tuning-tolerance`.
- **`plot_by_beta.py`, `plot_ScalarHistory.py`.** They pass `args.n_values` and
  `args.tuning_tolerance`; `potential_types` and the import of `DEFAULT_TUNING_TOLERANCE` are gone.

### D2 — the warning (ruling 6)
- **The helper.** `pipeline_selection._seed_and_log_Omega(model, phi, cosmology)` returns the seed
  and `model.realize(None, seed.phi_ref).log_Omega(phi)`. It replaces `_log_Omega_at_zero_phi_ref`.
- **The signatures.** `super_planckian_couplings(models, phi_init, T_init, units, cosmology)` and
  `warn_super_planckian(…, cosmology, emit=print)`.
- **Where it runs.** `main.py` calls the warning once per cosmology, inside the loop over
  `build_model_list`, before `run_pipeline`, and keeps the returned list.
- **The docstring** gives:
  - ln(M_P/T*) = 32.4331, from `REDUCED_PLANCK_MASS_EV` = 2.4353e27 eV and T* = 2e4 GeV;
  - the thresholds at φ_ref = 0: 6.4866 (φ* = 5) and 32.4331 (φ* = 1);
  - the thresholds at the seed's φ_ref at M = 10⁻³: 6.5060 (tracking seed, φ_ref = 0.0149 M_P) and
    32.613 (0.0055 M_P);
  - at M = 0.5 (static seed, φ_ref ≈ 0.40 M_P): 7.045 and 39.00.

  All were computed with this tree's seeds, by bisection on β.

### D3 — the wrapper (ruling 7)
`describe_scope` drops the potential and adds `n <values>` after M and
`tuning tolerance <_number(tol)>` after φ*.

### D4 — `tools/history_and_bbn.py` (ruling 8)
- **Tuned by default.** `tune_history(…, tolerance=args.tuning_tolerance,
  max_integrations=args.tuning_max_integrations)` runs with `integrate_history`'s default atol and
  rtol. The capture keeps the last `IntegrationResult` only (`captured[:] = [result]`).
- **The history line** adds `integrations=k` (1 with `--fixed-normalization`).
- **The `tuning` line** (`tuning_line`) gives every key of `TUNING_OUTPUT_KEYS`, in order: a float
  as its repr, an int or a string as it is.
- **`--fixed-normalization NORM PHI_REF_MP`** realizes the model at those values and runs one
  `integrate_history`, with no tuning line.
- **`--T-stop-GeV`** without `--fixed-normalization` prints the refusal to stderr and returns 2,
  before anything runs. `--tuning-tolerance` and `--tuning-max-integrations` have main.py's names
  and defaults, and their help says they have no effect with `--fixed-normalization`.
- **The axis** is `log_one_plus_z_grid(250, DEFAULT_LOG10_ONE_PLUS_Z_HIGH)`, imported from
  `config.argument_parser`. `LAMBDA_EV` and `LOG10_ONE_PLUS_Z_HIGH` are gone.
- **The BBN stand-in** ends at the history's end: T_CMB (`cosmology.T_CMB_Kelvin * units.Kelvin`)
  unless `--T-stop-GeV` is given.
- **The docstring** gives both modes, the usage, the output lines and the measured costs.

### D5 — the surfing tools (ruling 9)
- **`tools/surfing_scan.py`: the tuning.**
  - `run_history(setup, beta, phi_init_Mp, M_Mp, T_window_low_GeV, tolerance, max_integrations)`
    tunes with `tune_history` on `history_and_bbn`'s setup and axis, keeping the last result.
  - `N_window = T_Jordan_crossing(result, ln T_window_low)`.
  - The pure `in_window(crossings, bounce, N_edge)` keeps the crossings with N ≤ N_edge, and the
    bounce if its N ≤ N_edge.
  - `HistoryOutcome` gains `tuning` (the `TUNING_OUTPUT_KEYS` values) and `N_window`;
    `tuned_columns` converts them.
- **`tools/surfing_scan.py`: the CSV and resume.**
  - `COLUMNS` is in ruling 9's order. `_INPUT_MATCH` is (`phi_init_Mp`, `M_Mp`, `tuning_tol`,
    `T_window_low_GeV`).
  - A header holding `Lambda_eV` or `T_stop_GeV` is refused with its own message before the generic
    header check.
  - The sidecar records `T_window_low_GeV`, `tuning_tolerance` and `tuning_max_integrations`, and no
    `T_stop_GeV` or `Lambda_eV`.
- **`tools/surfing_scan.py`: options and console.**
  - `--T-window-low-GeV` (default `T_WINDOW_LOW_GEV_DEFAULT` = 1e-3), `--tuning-tolerance` and
    `--tuning-max-integrations` are new.
  - `--T-stop-GeV` is a `RemovedOption` (prompt 08, message `T_STOP_REMOVED`).
  - The console line adds `iterations`, `phi_ref` and `V_ref`; the flag reads "no bounce above
    T_window".
- **`tools/plot_surfing_histories.py`.**
  - `trace_history(…, T_window_low_GeV, n_samples, tolerance, max_integrations)` traces to the
    windowed bounce, or to `N_window` (the history's end if it is None) when there is none.
  - The title reads "tuned to H0 at T_CMB, tolerance …".
  - The same three options, and `--T-stop-GeV` refused. The docstring gives the cost of six β.
- **`tools/plot_surfing_temperature.py`.** `FLOAT_FIELDS` follows the new columns. The title gives
  φ*, M, the tolerance and the window's edge. A header holding `Lambda_eV` is refused (a
  `ValueError` from `parse_rows`, so `main` exits 2) with the message naming its writer and the
  sidecar's commit.

### D6 — `histories.csv` (ruling 10)
- **Columns.** `CSV_COLUMNS` gains `normalization`, `V_ref_eV4`, `phi_ref_Mp`, `phi_0_Mp`,
  `tracking`, `residual_H`, `residual_phi` and `tuning_iterations` after `phi_init_Mp`. The comment
  above it states the units.
- **The record.** `build_history_record` reads `scalar.tuning`: the normalization unconverted, V_ref
  over `units.eV**4`, φ_ref and φ_0 over `units.PlanckMass`. They are NaN, a blank cell, without a
  successful history. The sort is unchanged, (M, n, β).

### D7 — the tests

**Added**
- `ComputeTargets/tests/test_driver_options.py`, 14 tests:
  - `TestRemovedOptions` (5):
    - the refused set is the campaign's, and `potential_types` is gone;
    - each removed option is refused on the command line, exit 2, with the prompt and the
      replacement named and no "unrecognized arguments";
    - refused with no value;
    - refused as a key of a `--config` YAML file;
    - hidden from the help.
  - `TestNewOptions` (4):
    - the defaults are the imported constants and [1];
    - a given value replaces the default and never extends it (`--n-values 2` gives [2]);
    - values that are not positive are refused;
    - the help states the defaults and the key.
  - `TestTheConfigFile` (1): `exponential.yaml` has neither line, and parses to the default 300.
  - `TestTheDriver` (4), by `ast`:
    - no driver reads `potential_type` or imports `DEFAULT_TUNING_TOLERANCE`;
    - `main.py`'s product, compute call, tolerance lookup and warning call;
    - the plot scripts pass `args.n_values` and `tuning_tol=args.tuning_tolerance`, and never
      `tuning_max_integrations`;
    - `ScalarHistory.compute`'s default cap is passed to the remote.
- `tools/tests/test_history_and_bbn.py`, 3 tests:
  - (a) tuned at `3 0.01`: the tuning line equals `golden_history_06_tuned.json`'s `tuning` bitwise,
    key for key; five `-- tune` lines; the samples and steps equal the record's; the BBN stand-in
    ends at T_CMB. 22 s alone at load 4.
  - (b) `--fixed-normalization` at the record's values (repr): one `integrate_history` call, whose
    realized model holds exactly those values, to T_CMB; samples, steps and RHS equal the record's.
    4.6 s alone.
  - (c) `--T-stop-GeV` alone: exit 2, the message, no integration.
- `test_initial_field_option`:
  - (e) β = 7 at M = 0.5, φ* = 5 is flagged at φ_ref = 0 (35 > 32.43), and not at its seed's
    static φ_ref = 0.3977 M_P (32.22); β = 7.2 (φ_ref 0.3921, 33.18) is flagged at both.
  - (f) the two lines' exact text at β = 7, M = 10⁻³.
- `test_plot_inputs.TestTheExponent`:
  - a store built at n = 2 is found at `--n-values 2`;
  - `--n-values 1 2` on an n = 1 store skips the four (M, 2, β) with their warnings, counts "4 of
    8", then raises `PlotOptionNotStored` for `--n-values 2`.
- `test_extraction.test_e_records_carry_the_tuning`: the tuning columns, converted; NaN on the
  failure record.
- `test_tuned_history_round_trip.TestTheHistoriesCsv` (e): the read-back history
  (`_do_not_populate`) through `build_history_record` and `write_histories_csv`. The header is
  `CSV_COLUMNS`, and each tuning cell, read back, equals the stored value converted, by `float.hex`.
  4.8 s alone.
- `test_surfing_scan.TestTheWindow` (4):
  - a crossing beyond the edge is dropped;
  - a bounce beyond the edge is dropped;
  - a crossing and a bounce at the edge are kept;
  - None keeps everything.
- `test_surfing_scan.TestResume`, two tests:
  - `test_d_a_csv_of_before_prompt_08_is_refused_with_its_own_message`: the committed CSV, then a
    header holding only `T_stop_GeV`.
  - `test_d_T_stop_is_refused_by_name`.
- `test_plot_surfing_histories.TestMain.test_d_the_title_gives_the_tuning_and_no_Lambda`.
- `test_plot_surfing_temperature`:
  - `TestParse.test_c_the_tuned_columns_are_read`;
  - `TestParse.test_d_a_csv_of_before_prompt_08_is_refused_with_its_own_message`;
  - `TestMain.test_b_an_old_csv_is_refused_exit_2_with_its_message`.
- `RunDrivers/tests/test_registered_run_streams.TestTheBeat.test_the_tuning_lines_beat_and_are_not_stages`:
  three lines verbatim from `logs/06-probes/b3_M0.01_phi5_secant.log`, after a banner. Each beats
  past the 30 s floor and the stage stays the banner's; so does one with a Ray prefix; and
  `StageTracker.feed` returns False for each.
- `RunDrivers/tests/test_registered_run.TestTheTuningLines`: the new `"tune"` stand-in behaviour
  (`stubbed_run.TUNE_LINES`, printed between the banner and the first progress line), run in
  process with `Run.heartbeat` recorded. No recorded stage holds "tune", and every stage from the
  banner on starts with the banner's title.
- `RunDrivers/tests/test_registered_run.TestTheRemovedOptions`: `--potential-type` and
  `--Lambda-values-eV` are refused by the wrapper with the parser's message, writing no manifest
  and calling no `ray.init`. It runs in process on the stand-in program (Observations, item 1).

**Modified** (each where 08 changes what it tests)
- `test_initial_field_option`: (a) and (d) take the cosmology (`_setup` returns it; QCD_Cosmology
  on Planck2018, 0.06 s); (a)'s docstring gives the seed's thresholds; the module docstring.
- `test_plot_inputs`:
  - `_build_run` takes `n_values` and no `--potential-type`, and builds the tuning tolerance from
    the option;
  - `_parameters` passes `args.tuning_tolerance`;
  - `_read_full` and `_read_grid_and_lookups` use the new signature, as do `TestThePayloads.test_the_models`
    (now over n ∈ {[1], [1, 2]}) and both `TestOtherRefusalsPropagate` model tests;
  - `test_a_tuning_tolerance_the_build_did_not_use_names_the_constant` is renamed
    `…_names_the_option`, and expects `--tuning-tolerance 1e-06`;
  - `TestNothingWritten` adds the two new reads.
- `test_extraction`: the stand-in `scalar` carries `tuning` (`_tuning`); the CSV test checks the
  tuning cells and the order.
- `test_surfing_scan`:
  - `_StubRuns` takes the new signature and returns `tuning` and `N_window`;
  - `test_a` checks the new columns, the options passed and the sidecar;
  - `test_c_T_stop_and_M_mismatch_refused` became `test_c_window_tolerance_and_M_mismatch_refused`:
    the window, the tolerance and M refused, and the cap accepted;
  - `test_d` checks the generic message;
  - `test_e` checks that the tuned columns are blank on a failure row;
  - `TestRealHistory.test_beta_2` became `test_beta_1_5_tuned` (ruling 9). It tunes β = 1.5 (21 s
    alone at load 4) and checks:
    - both residuals inside the tolerance;
    - 4 integrations;
    - `N_window` at the 1 MeV crossing;
    - the old crossing checks (each a crossing of 1 + 1.5 π/M_P = 0);
    - QCD capture;
    - T_capture, T_deliver, T_hot and T_cold within 10⁻⁶ of the committed row.
- `test_plot_surfing_histories`:
  - the `main` stubs take the new signature (test a checks the window and tuning options passed);
  - test c adds `--T-stop-GeV` refused;
  - `TestRealHistory` keeps β = 3.0 (now tuned, 33 s alone at load 5) at 10⁻⁶ instead of 10⁻⁷.
- `test_plot_surfing_temperature`:
  - `make_row` fills the new columns (blank tuned values on a failure row);
  - `test_b_the_header_must_be_the_scans` checks the generic message;
  - `test_g` checks the title's tolerance and window, and no Λ or T_stop.
- `RunDrivers/tests/test_registered_run`: `PILOT_SCOPE` and the manifest's scope as ruling 7; the
  second scope test with `--n-values 1 2 --tuning-tolerance 1e-6`.
- `RunDrivers/tests/test_registered_run_streams.test_main_pys_own_banners_are_stages`: the sample of
  main.py's grid line is the new one.
- `RunDrivers/tests/stubbed_run.py`: the `"tune"` behaviour and `TUNE_LINES`.
- `ComputeTargets/tests/test_scalarhistory_failure_reason.py`: its own `LAMBDA_EV` (Deviation 14).

**Removed**
- `test_plot_inputs.TestOtherRefusalsPropagate.test_an_unknown_potential_type`: it asserted
  `exponential_models(None, None, …)` raises "Unknown potential type: None". Its subject, the
  potential-type argument, is deleted by `e624f1a` (ruling 15). The removed-types subtests of
  `test_the_models`, inside a kept test, went with it.

### The prose
- `CLAUDE.md`: the counts (26; 243 = 76 + 167; 93; 115; 64), "about 320 s" for one
  ComputeTargets run, a sentence that the tools suite holds three tuned histories, and the
  plot-scripts bullet naming the matched options (`--tuning-tolerance`, `--n-values`) and the one
  not matched (`--tuning-max-integrations`).
- The four tools' docstrings and usage lines.
- The boards and the index (below). README, `architecture-summary.md` and
  `numerical-strategies.md` are 10's, and are not edited.

## Deviations from the prompt

Every ruling cited is in orchestrator note 08, §0.

1. **`IMPLEMENTATION CHOICE` (ruling 1).** The removed options are refused by name; the wording,
   listed under Verification, is mine. `RemovedOption` sets `default=SUPPRESS`, so the namespace
   holds no attribute for a removed option. The surfing tools reuse it for their `--T-stop-GeV`.
2. **`STRUCTURALLY REQUIRED` (ruling 2).** The three options. `positive_float` and `positive_int` are
   new in `config/argument_parser.py`, and the tools import them.
3. **`STRUCTURALLY REQUIRED` (ruling 3).** `main.py` and `ScalarHistory.compute`. `main.py`
   rebinds `Model_array` to the warning's (unchanged) return inside the cosmology loop.
4. **`IMPLEMENTATION CHOICE` (ruling 4).** `exponential.yaml` loses `log10-one-plus-z-high: 85`
   (note 08 §0: 85 decades are 195.7 e-folds against histories needing up to about 200, and the
   grid's extent enters no tuned value) and `potential-type`.
5. **`IMPLEMENTATION CHOICE` (ruling 5, the n rule).**
   - The n check runs after every lookup and the count line, and raises for the first missing n.
   - It is skipped when the store holds no (M, β) at all, where there is nothing to judge.
   - The exception has no `__cause__`, since no single miss stands for it; `PlotOptionNotStored`'s
     docstring says so.

   **`STRUCTURALLY REQUIRED`:** the rest of `plot_inputs` and the scripts.
6. **`STRUCTURALLY REQUIRED` (ruling 6).** The warning. The helper is
   `_seed_and_log_Omega(model, phi, cosmology)`, returning (seed, ln A(φ)), and the count line says
   "at the seed's φ_ref" (IMPLEMENTATION CHOICE: names and text).
7. **`STRUCTURALLY REQUIRED` (ruling 7).** The scope and its tests.

   **`IMPLEMENTATION CHOICE`:** the wrapper's refusal test is in process
   (`TestTheRemovedOptions`). It was first written as a subprocess test in
   `test_registered_run_outcomes.TestTheRefusalsBeforeBegin`, beside the existing refusals, and moved
   after the incident of Observation 1, so that a broken refusal runs the stand-in and never
   `main.py`. That file is unchanged by `e624f1a`.
8. **`STRUCTURALLY REQUIRED` (ruling 8).** `tools/history_and_bbn.py`. Implementation choices:
   - the refusal of `--T-stop-GeV` prints to stderr and returns 2 (not `parser.error`), as the
     tool's other refusals do;
   - the history line carries `integrations=1` with `--fixed-normalization`;
   - test (b) also compares the accepted steps and RHS evaluations with the record's.
9. **`STRUCTURALLY REQUIRED` (ruling 9).** The surfing tools.

   **`IMPLEMENTATION CHOICE`** (ruling 9 names it so):
   - `in_window`, and the two extra columns `tuning_tol` and `T_window_low_GeV`;
   - also mine: `tuned_columns`, the console line's fields, and `trace_history`'s end when no bounce
     is in the window (`N_window`, or the history's end).
10. **`STRUCTURALLY REQUIRED` (ruling 10).** `histories.csv`.
11. **`IMPLEMENTATION CHOICE` (ruling 11).** `CLAUDE.md` also gets "about 320 s" (97 + 218 s
    measured) for one ComputeTargets run, and a sentence that the tools suite holds three tuned
    tests.
12. **`STRUCTURALLY REQUIRED` (ruling 12).** Five breakages, below. Breakage 1's first replay
    included a subprocess test that runs the real wrapper (Observation 1); the record below is its
    in-process replay.
13. **Measurement, not a deviation.** The note expected about 27 s for the tuned run at
    (β 3, M 0.01) and 25 and 40 s for the two real surfing tests. They took 22, 21 and 33 s alone at
    load 4–5, and the docstrings give these.
14. **`STRUCTURALLY REQUIRED` (the orchestrator's ruling after the stop).**
    `ComputeTargets/tests/test_scalarhistory_failure_reason.py` builds its untuned model at
    4 ln Λ from `tools.history_and_bbn.LAMBDA_EV`, which ruling 8 removes. Note 08's file list
    missed this test. The ruling: the test holds its own module constant `LAMBDA_EV = 1.0e-3`, with
    a one-line comment, in place of `driver.LAMBDA_EV`, and nothing else in the file changes.
    - This is the pattern of note 07's ruling 5, and the model's values are bitwise as before.
    - The test's other borrowings from `driver` stay: `T_INIT_GEV`, `PHI_INIT_MP`, `PI_INIT` and
      `_z_grid`. `_z_grid` now gives 300 decades of 1 + z, not 35.
    - The test still passes, all 4 of its module's. The history fails at its 51st accepted step, at
      N = 4.33 and T_J = 269 GeV, which the grid's extent does not reach.
15. **`UNINTENDED DRIFT`:** none found.

## Verification performed

**Before any edit** (at `3229c53`), serially:

| Suite | Count | Result | Load |
|---|---|---|---|
| CosmologyModels | 26 | OK | 5.7 |
| ObjectFactories | 92 | OK | 8.5 |
| RunDrivers | 61 | OK | 11.6 |
| ComputeTargets first half | 61 | OK, 116 s | 11.2 |
| ComputeTargets second half | 164 | OK, 254 s | 9.6 |
| tools | 102 | OK | 4.5 |

Golden tests alone, at load 4.4–4.8: `test_golden_history` 11.1 s OK; `test_tuning…test_e` 26.6 s
OK.

**After** (`e624f1a`'s tree), serially, each with `uptime`:

| Suite | Count | Result | Load |
|---|---|---|---|
| CosmologyModels | 26 | OK, 72 s | 5.3 |
| ObjectFactories | 93 (+1) | OK | 16.6 |
| RunDrivers | 64 (+3) | OK | 12.9 |
| ComputeTargets first half | 76 (+15) | OK, 100 s | 9.9 |
| ComputeTargets second half | 167 (+3) | OK, 218 s, `test_prym_passenger` included | 7.8 |
| tools | 115 (+13) | OK, 115 s | 4.8 |

**What each count's change is:**
- **ComputeTargets +18:** 14 in `test_driver_options`; 1 in `test_extraction`; 2 in
  `test_initial_field_option`; 2 added and 1 removed in `test_plot_inputs`.
- **ObjectFactories +1:** the round trip's test (e).
- **tools +13:** 3 in `history_and_bbn`; 4 window and 2 resume tests in `surfing_scan`; 1 title
  test in `plot_surfing_histories`; 3 in `plot_surfing_temperature`.
- **RunDrivers +3:** the streams test, `TestTheTuningLines` and `TestTheRemovedOptions`.

The first run of the second half had the one error of the stop (Deviation 14). It was re-run
alone, with the same `AttributeError`, before the stop was reported. After the ruling the module
passes alone (4 tests), and in the final run above.

**Golden histories** alone after the edits, at load 10–11: `test_golden_history` 9.3 s OK;
`test_tuning…test_e` 23.1 s OK. No golden JSON or fingerprint file is touched.

**`tools/history_and_bbn.py 3 0.01`**, run for real, PRyMordial included, at load 10 → 8, 33.5 s
in all:
- **Tuning:** five `-- tune` lines; the history line `RHS=140823 accepted_steps=15331 reflections=0
  samples=5960 integrations=5 wall=22.1 s`.
- **The tuning line:** all fifteen values equal the golden record's bitwise, by `float.hex` for
  the floats.
- **BBN:** `Yp=0.2143927966 DoH=1.707593705`, small network, PRyM 9.6 s.

**§2 item 2, the short scan.** The command, run from the root (written to the agent's scratch
directory, `…/agent08/work/short_scan.csv`), took 119.2 s at a load falling from 8 to 5:

```
./venv/bin/python tools/surfing_scan.py --beta-values 1.50 3.00 4.00 --phi-init-Mp 5 --M 1e-3 \
    --out <scratch>/short_scan.csv
```

| β | integrations (per integration) | wall | normalization | V_ref (eV⁴) | φ_ref (M_P) | residual_H | residual_phi |
|---|---|---|---|---|---|---|---|
| 1.5 | 4 (5.0 s) | 20.1 s | −276.85483680276144 | 2.0995228253310708e-11 | 0.03496226638511382 | 1.2718458641813868e-07 | 1.7897725401727627e-06 |
| 3.0 | 4 (7.9 s) | 31.7 s | −276.95262086751717 | 1.930123526100922e-11 | 0.02366290357111863 | −1.4399086215588497e-06 | 9.425299244414376e-06 |
| 4.0 | 5 (13.0 s) | 65.3 s | −276.9997238986587 | 1.855248101991673e-11 | 0.02008124768076264 | 2.0545405376992676e-09 | 4.871368375347629e-08 |

Every tuned value, and every measured T and N, equals the probe's `out.json` exactly (as repr).
Against the committed `surfing_scan_phi5_M1e-3.csv`, relative:

| β | captured, kick | T_capture | N_capture | T_max | N_max | T_second_dip | N_second_dip | T_deliver | N_deliver | T_hot | T_cold |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.5 | true, QCD (same) | −3.2e-12 | +2.1e-13 | −9.8e-10 | −2.6e-8 | — | — | −1.1e-8 | −2.3e-8 | −5.1e-15 | −5.1e-15 |
| 3.0 | false (same) | — | — | — | — | — | — | +1.10e-7 | −3.7e-8 | — | — |
| 4.0 | true, EW (same) | +4.1e-9 | +1.9e-9 | −9.5e-10 | +5.7e-9 | +2.6e-10 | +9.4e-9 | −1.7e-8 | −3.1e-8 | −5.1e-15 | −5.0e-15 |

`deliver_reflected` (false), `n_crossings` (2, 0, 6) and `reflections` (0) are unchanged. The roots
agree to 5.1 × 10⁻¹⁵ (< 10⁻¹²), and no capture flag or kick changes: the stop condition as note 08
§0 reads it does not trigger. `accepted_steps` is now the final integration's to T_CMB (19958,
30867, 43655, against 1231, 4583, 14353 to 1 MeV).

**Predicted full scans.** The per-integration cost to T_CMB is interpolated through the three
points (5.0, 7.9, 13.0 s at β = 1.5, 3.0, 4.0, extrapolated linearly to 4.3 s at β = 1.1 and
18.2 s at 5.0), at 4.3 integrations a β:
- **S1** (79 β, 1.10–5.00): about 55 minutes (3300 s), against 280 s before.
- **S5** (61 β, 2.98–3.04): about 35 minutes (2100 s), against 150 s before.

These were measured at a load of 5–8. The note's probe, at load 6–9, implied about 65 and 40
minutes.

**The new tuned tests, each alone:**

| Test | Time | Load |
|---|---|---|
| `history_and_bbn` (a) | 22.2 s | 3.9 |
| `history_and_bbn` (b) | 4.6 s | 4.2 |
| `surfing_scan` `TestRealHistory` | 21.0 s | 4.1 |
| `plot_surfing_histories` `TestRealHistory` | 32.6 s | 4.8 |
| round trip (e) | 4.8 s | 4.4 |

Each is under a minute.

**`black --check`** passes on the 26 changed or new Python files. All were black-clean at
`3229c53`.

**`--help`** exits 0 for all of these: `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
`RunDrivers/registered_run.py`, `tools/history_and_bbn.py`, `tools/surfing_scan.py`,
`tools/plot_surfing_histories.py` and `tools/plot_surfing_temperature.py`.

**The refusals, run** (each exit 2, creating no file):
- `main.py --database <scratch> --potential-type Exponential`:
  `main.py: error: --potential-type was removed by jordan-normalization prompt 08: there is one
  model, ExponentialModel, V = V0 exp((M/phi)^n) with A = exp(beta (phi - phi_ref)/M_P);
  --n-values gives its exponent n`.
- `… --T-stop-GeV 1`: `--T-stop-GeV was removed by jordan-normalization prompt 07: every stored
  history ends at T_CMB; tools/history_and_bbn.py --fixed-normalization NORM PHI_REF_MP
  --T-stop-GeV T runs an untuned history to any T`.
- `… --Lambda-values-eV 1e-3` (likewise `--log10-Lambda-high-eV`, and the other three):
  `--Lambda-values-eV was removed by jordan-normalization prompt 07: each history's normalization
  is tuned so that H_J(T_CMB) = H0 and is stored with the history, so no Lambda is given;
  --tuning-tolerance sets the tuning`.
- `… --tuning-tolerance 0`: `argument --tuning-tolerance: must be a positive number, not 0`.
- `tools/surfing_scan.py … --T-stop-GeV 1e-3` and `tools/plot_surfing_histories.py … --T-stop-GeV
  1e-3`: `--T-stop-GeV was removed by jordan-normalization prompt 08: each history is tuned to
  T_CMB; --T-window-low-GeV sets the lower edge of the analysis window, the history's first
  crossing of that T_J`.
- `tools/history_and_bbn.py 3 0.01 --T-stop-GeV 1e-3`: `!! history_and_bbn: --T-stop-GeV is
  accepted only with --fixed-normalization; a tuned history ends at T_CMB`.

**The new option help** (main.py's, shared by the plot scripts):
- `--tuning-tolerance`: "the tolerance of each history's tuning on both of its residuals,
  |H_J(T_CMB)/H0 - 1| and |phi_ref/phi_min - 1| (default 1e-05). It is a key of every stored
  history, so a run at another tolerance computes histories of its own (jordan-normalization
  prompt 08)".
- `--tuning-max-integrations`: "the cap on the integrations of one history's tuning, the seed's
  included (default 30). It is not a key of the history: a history that does not converge within
  it is stored as a failure, and a later run with a larger cap does not recompute it (only --drop
  scalar-history, which drops every history, does) (jordan-normalization prompt 08)".
- `--n-values`: "one or more values of the model's exponent n, V = V0 exp((M/phi)^n); n is part of
  the model's key (default [1]; jordan-normalization prompt 08)".

**The tools' new options:**
- `--T-window-low-GeV` (`surfing_scan`): "the lower edge of the analysis window on each history
  tuned to T_CMB, in GeV: turning points and the first bounce are kept up to the history's first
  crossing of this Jordan-frame temperature (default 0.001, where the integration stopped before
  jordan-normalization prompt 08)". `plot_surfing_histories`' says the trace ends at the first
  bounce before that crossing, or at the crossing.
- `--tuning-tolerance` and `--tuning-max-integrations` in the three tools: "the tuning's tolerance
  on both residuals, … (default 1e-05, as main.py)", and "the cap on the tuning's integrations, the
  seed's included (default 30, as main.py)". In `history_and_bbn.py` both add "; no effect with
  --fixed-normalization".
- `--fixed-normalization NORM PHI_REF_MP`: "run one untuned history of the model realized at the
  normalization NORM (the model's own: ln V0, V0 in units of M_P^4, as a stored history holds it)
  and phi_ref = PHI_REF_MP M_P, by integrate_history, for diagnostics (default: tune the history, as
  main.py does)".
- `--T-stop-GeV` (`history_and_bbn`): "with --fixed-normalization only: the stopping temperature in
  the Jordan frame, in GeV (default: T_CMB). A tuned history ends at T_CMB".

**The warning's lines**, as (f) pins them:
- `!! warning: beta=7, M=0.001 M_P, n=1, phi*=5 M_P: Omega(phi*) T* = <x> M_P (super-Planckian
  start), with Omega at the seed's phi_ref = <y> M_P (tracking seed)`;
- `!! warning: 1 of 1 models start super-Planckian at the seed's phi_ref; they are computed like
  the others. The tuned phi_ref differs from the seed's, moving ln Omega(phi*) by -beta (phi_ref -
  phi_ref_seed)/M_P, in size up to 1.4e-3 at M = 1e-3 M_P with beta <= 5.5, 6.5e-3 at beta = 25,
  and up to 1.0 at M = 0.5 M_P from a static seed (jordan-normalization prompt 06's points)`.

**The refusals of an old CSV:**
- the scan: `!! surfing_scan: <path> was written before jordan-normalization prompt 08, by a scan of
  untuned histories at a fixed Lambda (its header holds Lambda_eV or T_stop_GeV); a tuned scan does
  not resume it. Give a new --out`;
- the plot: `<path>: written by tools/surfing_scan.py before jordan-normalization prompt 08, at a
  fixed Lambda (its header holds Lambda_eV); this tool reads the tuned scan's columns. Draw it with
  tools/plot_surfing_temperature.py at the commit its sidecar records`.

**The other new lines:**
- the n refusal: `!! plot: the store holds no ExponentialModel for --n-values 2; main.py was not run
  with it, so no history in this store can match. Give the plot the options the build used`;
- the tolerance's miss: `… for --tuning-tolerance 1e-06; …`.

## The deliberate-breakage record

Each diff is relative to `e624f1a`'s tree. Each was checked with `git apply --check` and
`git apply -R --check`, replayed in `bash`, and reversed, leaving the tree as before.

**Breakage 1: an old option accepted silently.**
```diff
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -79,10 +79,7 @@
         super().__init__(option_strings, dest, **kw)
 
     def __call__(self, parser, namespace, values, option_string=None):
-        parser.error(
-            f"{option_string} was removed by jordan-normalization prompt "
-            f"{self.removed_by}: {self.replacement}"
-        )
+        pass
 
 
 def non_negative_float(text: str) -> float:
```
Replayed on `test_driver_options`, `test_registered_run.TestTheRemovedOptions` and
`test_surfing_scan.TestResume.test_d_T_stop_is_refused_by_name`: 14 failures of 16.
- `test_driver_options`: the 7 subtests of `test_a_refused_on_the_command_line…` and
  `test_a_refused_with_no_value_too` fail with `AssertionError: Namespace(…) is not None` (the parse
  succeeded); the 3 subtests of `test_a_refused_as_a_key_of_a_config_file` the same.
- `TestTheRemovedOptions` (both subtests): `None is not an instance of … Refusal` (the stand-in
  ran), then a `FileExistsError` for the second registration.
- `test_d_T_stop_is_refused_by_name`: `SystemExit not raised`.

**This breakage's replay must not include a test that runs the real wrapper.**
`test_registered_run_outcomes.TestTheRefusalsBeforeBegin` and `run_the_script` launch
`RunDrivers/registered_run.py` with no stub. With the refusal disabled, the real wrapper registers
in `var/runs` and runs `main.py` (Observation 1). Nor should it include
`test_plot_surfing_histories.TestMain.test_c…`, which would then tune the six default β (about
3.5 minutes). The in-process `TestTheRemovedOptions` replaced the subprocess test I first wrote.

**Breakage 2: the warning evaluated at φ_ref = 0.**
```diff
--- a/pipeline_selection.py
+++ b/pipeline_selection.py
@@ -199,7 +199,7 @@
     (jordan-normalization prompt 08, P11; before it, Omega was taken at phi_ref = 0.)
     """
     seed = model.seed(cosmology)
-    return seed, model.realize(None, seed.phi_ref).log_Omega(phi)
+    return seed, model.realize(None, 0.0).log_Omega(phi)
 
 
 def super_planckian_couplings(
```
`test_initial_field_option`: 2 failures of 6.
- (e): `Lists differ: [7.0, 7.2] != [7.2]`.
- (f): the line's Ω(φ*) T* is `13.025` instead of `11.785`.

(a) and (d) pass, as at φ_ref = 0 before: at M = 10⁻³ the seed's φ_ref moves no β of {1, 6, 7, 25,
40} across the threshold.

**Breakage 3: the scan resuming into an old-format CSV** (the old-format check removed).
```diff
--- a/tools/surfing_scan.py
+++ b/tools/surfing_scan.py
@@ -533,7 +533,7 @@
     with open(path, newline="") as f:
         reader = csv.reader(f)
         header = next(reader, None)
-        if header is not None and any(c in header for c in _OLD_FORMAT_COLUMNS):
+        if False:
             print(
                 f"!! surfing_scan: {path} was written before jordan-normalization prompt 08, "
                 f"by a scan of untuned histories at a fixed Lambda (its header holds Lambda_eV "
```
`test_surfing_scan.TestResume`: 1 failure of 9,
`test_d_a_csv_of_before_prompt_08_is_refused_with_its_own_message`: `'was written before
jordan-normalization prompt 08' not found in '!! surfing_scan: …/scan.csv has a different header;
not resumed'`. The generic check still refuses the file (exit 2), and the test asserts the specific
message.

**Breakage 4: the window removed** (`in_window` returns its input).
```diff
--- a/tools/surfing_scan.py
+++ b/tools/surfing_scan.py
@@ -260,8 +260,7 @@
     order, and the bounce if its N <= N_edge (else None). N_edge None is no edge: everything is
     kept. Pure.
     """
-    if N_edge is None:
-        return list(crossings), bounce
+    return list(crossings), bounce
     kept = [c for c in crossings if c.N <= N_edge]
     if bounce is not None and not bounce.N <= N_edge:
         bounce = None
```
Replayed on `TestTheWindow`, the scan's `TestRealHistory` and `test_plot_surfing_histories.TestRealHistory`:
2 failures of 6, 54 s.
- `test_a_a_crossing_beyond_the_edge_is_dropped`: the crossing at N = 26 is kept.
- `test_b_a_bounce_beyond_the_edge_is_dropped`: `FirstBounce(N=30.0, …) is not None`.

The two real histories pass: at β = 1.5 and 3.0 the walk stops at the first bounce, above 1 MeV,
so the window drops nothing there (note 08 §0).

**Breakage 5: `--n-values` given `action="extend"`.**
```diff
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -298,6 +298,7 @@
         "--n-values",
         type=positive_int,
         nargs="+",
+        action="extend",
         default=[1],
         help="one or more values of the model's exponent n, V = V0 exp((M/phi)^n); n is part of "
         "the model's key (default %(default)s; jordan-normalization prompt 08)",
```
Replayed on `test_driver_options`, `test_plot_inputs`, `test_registered_run.TestScope` and
`TestRegistration`: 4 failures of 39.
- `test_b_given_values_replace_the_defaults`: `[1, 2] != [2]`.
- `TestTheExponent.test_a_store_built_at_n_2_is_found_at_n_values_2`: n = 1 models found as well.
- `TestTheExponent.test_an_n_the_build_did_not_use_stops_naming_n_values`: "4 of 12" instead of
  "4 of 8".
- `TestScope.test_more_than_six_values_read_as_a_count_and_a_range`: `n 1, 1, 2`.

## Observations not acted on

1. **A run registered in `var/runs` by the first replay of breakage 1.**
   - **What ran.** That replay included `test_registered_run_outcomes.TestTheRefusalsBeforeBegin`
     and a subprocess refusal test I had added there. Both run the real `RunDrivers/registered_run.py`
     with no stub.
   - **What it registered.** With the refusal disabled, the wrapper registered
     `var/runs/rrk-adoption-03-unit-20261026T151946` (argv `-- --database <tmp>/store.db
     --ray-address 127.0.0.1:6380 --potential-type Exponential`) and ran `main.py`.
   - **How far it got.** `main.py` reached `ray.init(address=127.0.0.1:6380)` with no cluster up;
     its `stdout.log` ends at the packages line. Its process (pid 4641) was gone within about five
     minutes, I believe by the test's 300 s `subprocess.run` timeout, so no end was recorded.
   - **Checked afterwards.** No Ray process ran (no raylet, `gcs_server` or worker), no new
     `/tmp/ray` session appeared, and the rehearsal lock was not touched. `runregistrykit list` shows
     the run stale.
   - **Left for the user.** I did not end it or touch `var/runs`: registry rule 5 makes ending it a
     person's step. The orchestrator has reported it to the user.
   - **The fix in this prompt.** I moved my test in process (`TestTheRemovedOptions`; Deviation 7).
   - **Still exposed.** `TestTheRefusalsBeforeBegin`'s existing subprocess refusals would launch
     `main.py` the same way if one of their refusals broke.
2. **The old run script is not refused.**
   `science-runs/2026-10-03-science-2026.6.0/full_run_2026.6.0.sh` passes `--config
   exponential.yaml` and no removed option; with this tree's YAML it would build tuned histories at
   n = 1 (note 08 §0). That is right for a rebuild; 10's handover says so.
3. **`plot_surfing_histories` when no bounce is in the window.** Its trace then ends at `N_window`.
   `bounce_N` and `bounce_T_GeV` hold the trace's end, so the figure marks that end as a bounce and
   the console line says "first bounce". It arises at no default β.
4. **`plot_by_beta.py` and `plot_ScalarHistory.py` accept `--tuning-max-integrations`.** They share
   the parser and do not read it (ruling 5).
5. **The surfing CSV's `accepted_steps`** is now the final integration's to T_CMB, so it is not
   comparable with the committed scans' column (to 1 MeV).

## State handed to the next prompt

- **Tree:** `e624f1a`, on `jordan-normalization`, on top of `3229c53`.
- **The driver.**
  - `main.py` builds `ExponentialModel` over `product(M, --n-values, β)`, its tuning tolerance
    from `--tuning-tolerance` (default 1e-5, a key), and passes `--tuning-max-integrations`
    (default 30, not a key) to `ScalarHistory.compute(label, max_integrations)`.
  - The Λ options, `--T-stop-GeV` and `--potential-type` are refused by name (`RemovedOption`,
    exit 2, and as the wrapper's refusal).
  - `exponential.yaml` gives neither the potential type nor the grid's extent (300 decades).
  - The super-Planckian warning runs once per cosmology at the seed's φ_ref.
  - **09 runs this driver unchanged.**
- **The wrapper's scope** reads `config …; β …; M …; n …; φ* … Mp; tuning tolerance …; N shards;
  job …`.
- **The tune lines** beat and are never a stage (`TestTheBeat`, `TestTheTuningLines`). **09 measures
  the silences.**
- **The plot scripts** match `--tuning-tolerance` and `--n-values` (an n with no stored model is
  `PlotOptionNotStored`, exit 2).
- **`tools/history_and_bbn.py BETA M`** tunes and prints the `tuning` line, every
  `TUNING_OUTPUT_KEYS` value as repr: the floats a store holds in `Planck_units`. **09 compares its
  stored values with this line bitwise.** `--fixed-normalization NORM PHI_REF_MP` runs the
  primitive.
- **The surfing tools** tune each β to T_CMB. The window ends at the first crossing of
  T_J = `--T-window-low-GeV` (1 MeV); the CSV carries the tuning per row. The committed CSVs and
  figures are records. **The full S1 and S5 re-run is the user's** (about 55 and 35 minutes; 10's
  handover).
- **`histories.csv`** has the tuning's columns.
- **The golden histories.** `golden_history_05.json` and `golden_history_06_tuned.json` pass
  bitwise, unchanged.
- **Counts:** CosmologyModels 26, ComputeTargets 243 (76 + 167), ObjectFactories 93, tools 115,
  RunDrivers 64.
- **Open issues.** Closed `[05-surfing-scan-runs-at-a-fixed-lambda]`; opened
  `[08-a-history-that-did-not-converge-is-never-retried]` (for the user to rule). Index: 22 open;
  §1.15 is empty.
- **For the user:** the stale run `rrk-adoption-03-unit-20261026T151946` in `var/runs`
  (Observation 1), to be ended by registry rule 5.
- **Breakage replay.** The five diffs above apply to `e624f1a`'s tree, checked in `bash`.
  Breakage 1 is replayed only on in-process tests.
