# Prompt 10 — the driver and the readers

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-10-31, after 09 landed (`3984ec5`) and was reviewed (`8987c67`), at the user's word
("After that, please write prompt 10."). It is written from:
- README §2.2's charter for 06 and §8.4's for 10, and the rulings they rest on: U8 as replaced by R12,
  P10, P26, P30 (§6, §8);
- the user's three rulings of the same day, R26–R28 (README §8.1), and the planner's P32–P39 (§8.2);
- log 09 §8, "State handed to the next prompt": the α row, every lookup and where it passes α, the
  columns, the accessors, and what 10 must add;
- log 09 §5.2: the stored values on eight histories, which bound the figures' ranges.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- no ChamPBH Ray process is running. You start none, and no test of yours may start one.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Sonnet**, as the plan has it (README §8.4). The work is plumbing and two figures.
Nothing in the integration, the store's schema or the physics changes.

**Read first:**

1. [`README.md`](README.md):
   - §5 (the rules);
   - §2.2's charter for 06, and §8.4's for 10;
   - §8.1 R6, R12, R22, R24, R26–R28; §8.2 P10, P26, P30, P32–P39.
2. `CLAUDE.md`:
   - "The plot scripts" (read-only, the matched options, exit 2);
   - "Ray on this machine" (the drivers refuse Ray without `--ray-address`; `main.py --inventory` needs no
     Ray);
   - the test rules (`_TempStoreCase`; `@ray.remote` functions through `._function`).
3. [`logs/09-store.md`](logs/09-store.md) §8, whole; §5.2 for the stored values.
4. `config/argument_parser.py`: `create_argument_parser`, and `--n-values` (:298) as the precedent for a
   list option whose given values replace the default.
5. `main.py`: `execute` (:923), the α fetch at :999, `run_pipeline` (:154) and its six history payloads.
6. `plot_inputs.py`: `parameters` (:108), `PlotParameters`, `PlotOptionNotStored`, `_get_parameter`.
7. `plot_by_beta.py`: `build_beta_plot` (:145), its paths (:164, :314, :496, :548, :658, :668), `run_pipeline`
   (:697) and the driver block (:1040–:1108).
8. `plot_ScalarHistory.py`: `plot_ScalarHistory` (:1379) and its paths (:1427–:1430), `write_csv_file`
   (:1272), `run_pipeline` (:1519) and the driver block.
9. `extract_common.py`: `build_history_record` (:867), `write_histories_csv` (:1150), `_save` (:946),
   `plot_fixed_T` (:1137) as the pattern of a figure that is importable and testable without Ray.
10. `RunDrivers/registered_run.py`: `describe_scope` (:672).
11. `tools/inventory_report.py`: `INVENTORY_CATEGORIES` (:67), `HISTORY_EXTRA_FIELDS` (:117), and how a
    common line and a varying field are printed.
12. `tools/history_and_bbn.py` (its options, :389 and :405, and the per-history lines) and
    `tools/bbn_from_store.py` (`_FIND_QUERY`, :207).
13. `ComputeTargets/ScalarHistory.py`: `DissipationValues`, `dissipation_values`, `ScalarHistory.dissipation`
    and `ScalarHistoryValue.rho_chi_over_rho_rad` (prompt 09).
14. The tests that exercise what changes:
    - `ComputeTargets/tests/test_driver_options.py`, `test_plot_inputs.py`, `test_bbn_from_store.py`;
    - `RunDrivers/tests/test_registered_run.py` (the scope);
    - `tools/tests/test_inventory_report.py`, `test_history_and_bbn.py`.

---

## 0. Where this prompt sits

### 0.1 What it is for

09 made α a key of every history and stored what dissipation mode computes. Every reader passes the
α = 0 row, and nothing can ask for another. This prompt gives α an option and carries it through:
- `--alpha-values` in the shared parser;
- `main.py` builds every history once per α;
- the plot scripts match α against the store, and draw R12's figures;
- the wrapper's scope, the inventory and the two tools name α.

What 11 does is not done here: the measurement of U10, the rehearsal of U11 and the verification
document.

### 0.2 What the user ruled for this prompt (2026-10-31)

- **R26 — the output paths.** "alpha=<α>/ level, every α". Every per-α product of both plot scripts goes
  under `<output>/<label>/alpha=<α>/`, α = 0 included. Today's α = 0 paths move down one level.
- **R27 — Δg*.** "New dark_radiation.pdf". A figure of its own on `plot_ScalarHistory.py`: Δg*(T_J) on top
  and ρ_χ/ρ_R below, both log-scale, with the parking and the release marked. It is written only at α > 0.
- **R28 — ΔN_eff(β).** "Both, two panels". One `Delta_N_eff.pdf` per (M, n) group on `plot_by_beta.py`:
  ΔN_eff today and at 1 MeV against β, one curve per α > 0 of the run. `histories.csv` gains α and both
  ΔN_eff columns.

### 0.3 What the planner found in the code

- **`action="extend"` with a non-empty default appends to it.** `--beta-values` and the M options use
  `nargs="*", action="extend", default=[]`. With `default=[0.0]`, `--alpha-values 0.1` would give
  `[0.0, 0.1]`. `--n-values` (`nargs="+"`, the default action) is the precedent where a given value
  replaces the default, and `test_driver_options` (b) asserts that for it. P32.
- **`integrate_history` refuses α outside [0, 1)** with a `ValueError`, inside a Ray task. So an α out of
  range must be refused by the parser, before Ray (exit 2), never by the task. P32.
- **`run_pipeline` in all three drivers takes one α row.** Each of its history payloads already carries
  `"alpha": alpha` (09). The least change is an outer loop over α around each driver's `run_pipeline`,
  inside its loop over cosmologies. P33.
- **`plot_inputs.parameters` returns one α**, `PlotParameters.alpha`, the constant α = 0 (09). P34.
- **Every plot path is built from `Path(args.output) / model_label`** in both scripts. `plot_by_beta.py`
  has six such paths, and `plot_ScalarHistory.py` two. `plot_by_beta.py`'s figure 2
  (`M_convergence.pdf`) and `histories.csv` span every (M, n) group of one α. P35.
- **The plot scripts parse their options at import**, so a figure function defined in them cannot be
  imported by a test. The figures that are tested (`plot_fixed_T`, `plot_abundance_shifts`) live in
  `extract_common.py`. P36.
- **`describe_scope` names every key but α** (`RunDrivers/registered_run.py:672`). P37.
- **The inventory prints α per history line** and `alpha_value` under "Other classes" (log 09 §7). With one
  α in a store, every history line repeats `alpha=0`. P38.
- **`tools/bbn_from_store.py`'s `_FIND_QUERY` has no α.** It is right while a store holds α = 0 alone, and
  finds two rows per model once a store holds two α. `tools/history_and_bbn.py` takes no α. P39.

---

## 1. What is wanted

### 1.1 The option (P32)

- **`--alpha-values`** in `config/argument_parser.py`, after `--tuning-tolerance`:
  - `nargs="+"`, `default=[0.0]`, the default action, so that a given list replaces the default, as
    `--n-values` does;
  - `type=` a new validator `alpha_fraction(text)`, which accepts 0 ≤ α < 1 and raises
    `argparse.ArgumentTypeError` otherwise. An α out of range is exit 2 at parse, before Ray;
  - a help text saying what α is (the fraction of the condensate's kinetic energy drained at each
    candidate), that it is a key of every history, that 0 is natural mode, and the default.
- **The YAML key** `alpha-values` works through `configargparse` as every other option does. Say so with a
  test.
- **Duplicates.** A value given twice is one α: the drivers deduplicate, keep the order of first
  appearance, and print the list they use.

### 1.2 `main.py` (P33)

- **The rows.** `execute` fetches one `alpha_value` row per α, in the `ray.get` beside the tolerances
  (:999), in the order of `--alpha-values`, and prints the α grid beside the β and M grids.
- **The loop.** Inside the loop over `model_list`, after `warn_super_planckian`, `run_pipeline` is called
  once per α row. `run_pipeline`'s signature is unchanged; it gets one α per call. Its header line names α:
  `>> RUNNING PIPELINE FOR MODEL <label>, alpha = <α:g>`.
- **Nothing else changes.** The work items, the batches, the sharding, the RayWorkPool parameters and the
  stage order are as now.

### 1.3 `plot_inputs.py` (P34)

- **`PlotParameters.alpha` becomes `alphas`**: a tuple of `alpha_value` rows, one per α of
  `--alpha-values` in the drivers' order (deduplicated).
- **`parameters`** takes `alpha_values: Sequence[float]` in place of the constant. Each α is looked up with
  `pool.object_get("alpha_value", value=α)`. A miss raises `PlotOptionNotStored("alpha_value",
  "--alpha-values", <α as given>)`, the first in the order given, after the parameters before it, as now.
- **Not a new stop.** An α with a stored row but no stored history for a model is that model's existing
  "not stored, skipped with a warning" (read-only-plots U3), per α.
- The read-only pool still refuses to create a row.

### 1.4 The output paths (R26, P35)

Both plot scripts write every per-α product under `<output>/<label>/alpha=<α:.5g>/`. α = 0 is
`alpha=0/`. A product that spans α sits at `<output>/<label>/`.

| Product | Today | After |
|---|---|---|
| `plot_by_beta.py`, per (M, n): `timings.pdf`, `BBN.pdf`, `max_Q.pdf`, `abundance_shifts.pdf`, `T_deliver.pdf`, `fixed_T.pdf`, `csv/…/data.csv` | `<label>/plots/M=…_n=…/…` | `<label>/alpha=<α>/plots/M=…_n=…/…` |
| `plot_by_beta.py`, figure 2, `M_convergence.pdf` | `<label>/plots/M_convergence.pdf` | `<label>/alpha=<α>/plots/M_convergence.pdf` |
| `plot_by_beta.py`, `Delta_N_eff.pdf` (R28, new) | — | `<label>/plots/M=…_n=…/Delta_N_eff.pdf` (spans α) |
| `plot_by_beta.py`, `histories.csv` | `<label>/histories.csv` | `<label>/histories.csv` (spans α, with an `alpha` column) |
| `plot_ScalarHistory.py`, every figure and CSV | `<label>/plots/beta=…/M=…eV_n=…/…`, `<label>/csv/…` | `<label>/alpha=<α>/plots/beta=…/…`, `<label>/alpha=<α>/csv/…` |
| `plot_ScalarHistory.py`, `dark_radiation.pdf` (R27, new) | — | beside the history's other figures, at α > 0 only |

- **Titles.** Every per-α figure says its α where it already says the model's label: on
  `add_beta_summary_labels`' line for `plot_by_beta.py`, and on `add_ScalarHistory_labels`' line for
  `plot_ScalarHistory.py`, as `α = <α:g>`.
- **The loop.** Each plot script calls its `run_pipeline` once per α row, as `main.py` does (P33).
  `plot_by_beta.py` gathers the records of every α for `histories.csv` and `Delta_N_eff.pdf`, which it writes
  once, after the last α.

### 1.5 The figures (R12, R27, R28, P26, P36)

Both are functions in `extract_common.py`, called by the plot scripts, so that a test can draw them from
constructed records without Ray, as `plot_fixed_T` is tested.

- **`plot_Delta_N_eff(records, path, title) -> bool`** (R28).
  - Two panels sharing the β axis: ΔN_eff(T_CMB) above, ΔN_eff(1 MeV) below, each on a log y-axis.
  - One curve per α > 0 present in `records`, each with markers, in increasing α, with a legend `α = …`.
  - A record with a NULL value (a failure, or a history that did not reach 1 MeV) is left out of its
    panel.
  - Nothing is drawn, and it returns `False` with one printed line, if no record has α > 0.
  - The y-label of the lower panel says T_ν = T_γ (R22); the upper panel's says (11/4)^{4/3} (P7).
  - The records are `build_history_record`'s, which gains `alpha`, `Delta_N_eff_today` and
    `Delta_N_eff_1MeV` from the history's `dissipation` record (None on a failure).
- **`histories.csv`** (R28) gains `alpha`, `Delta_N_eff_today` and `Delta_N_eff_1MeV`. Its rows sort by
  (α, M, n, β).
- **`plot_dark_radiation(model, model_label, path, x_coord) -> bool`** (R27, P26).
  - Two panels sharing the script's x axis (redshift or e-folds, with the T_J labels the other figures
    carry):
    - above, Δg*(T_J) = `gstar_rho * rho_chi_over_rho_rad` per sample, the reader's derivation of P26;
    - below, ρ_χ/ρ_R = `rho_chi_over_rho_rad` per sample.
  - Both log y-axes. Samples where the ratio is 0.0 (before the first drain) are left out.
  - Vertical lines at `dissipation.parking_N` and `dissipation.release_N`, each where it is not None,
    converted to the x coordinate as the other figures place their events, with a legend entry.
  - Written only when the history's α > 0, and returns `False` with no figure otherwise.
- **`plot_ScalarHistory.py`'s CSV** (`write_csv_file`) gains two columns at every α:
  `rho_chi_over_rho_rad` and `Delta_gstar`. Both are 0.0 at α = 0.

### 1.6 The wrapper's scope (P37)

`describe_scope` adds `α <values>` after the tuning tolerance, with `_values` as β's, always present: the
default reads `α 0`.

### 1.7 The inventory (P38)

- **`alpha_value`** joins "Redshifts, tolerances and parameter values", after `beta_value`.
- **`HISTORY_EXTRA_FIELDS`** gains `("alpha", "alpha")` after `tuning_tol`. Then a store whose histories
  share one α prints it once on the common line, and a store with several prints it per history, as the
  other extra fields are printed.

### 1.8 The tools (P10, P39)

- **`tools/history_and_bbn.py`** gains `--alpha` (one float, `alpha_fraction`, default 0.0).
  - It is passed to `tune_history` and to `integrate_history` (the `--fixed-normalization` path).
  - The `history` line gains `alpha=<α:g>`.
  - At α > 0 a new line, `dissipation <label>: …`, prints every field of `dissipation_values(data)` with
    `:.6g` (None as `None`). 11's measurement reads it (U10).
  - At α = 0 every existing line is byte-identical to today's, and no `dissipation` line is printed.
- **`tools/bbn_from_store.py`** gains `--alpha` (default 0.0). `_FIND_QUERY` joins
  `alpha_value a on a.serial = sm.alpha_serial` and matches α as the dimensionless factory does: absolute
  (`< DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION`) at 0, relative otherwise. Its output at α = 0 on a store
  holding α = 0 alone is unchanged.
- **The surfing tools** (`tools/surfing_scan.py`, `tools/plot_surfing_*.py`) stay at α = 0 and are not
  touched.

### 1.9 The documents (P12)

- **`.documents/architecture-summary.md`:** a dated note beside 09's: the option and its parse, the
  per-α loop in the three drivers, the paths of R26, the two figures, the scope, the inventory and the
  tools.
- **`.documents/numerical-strategies.md`:** nothing, unless a figure's derivation (Δg*) needs a line; if
  so, a dated line after 09's subsection.
- **`CLAUDE.md`** is not edited. Its plot-scripts bullet gains `--alpha-values` at 12.

### 1.10 The tests

No store but `_TempStoreCase`'s, no Ray. New module or modules, named for what they test.

1. **The option.**
   - The default is `[0.0]`, a given list replaces it, and the YAML key gives the same list.
   - −0.1, 1.0 and `nan` are refused, exit 2, with the option named.
   - Duplicates collapse in the drivers' list, in the order of first appearance.
2. **`main.py` carries α** (by `ast`, as `test_driver_options` (d) reads `main.py`):
   - one `alpha_value` lookup per value of `args.alpha_values`;
   - `run_pipeline` called inside a loop over those rows;
   - no literal `value=0.0` lookup of `alpha_value` left in any driver.
3. **`plot_inputs`.**
   - Two α stored: `parameters` returns both rows, in order.
   - An α not stored: `PlotOptionNotStored` naming `--alpha-values` and that α, and the driver's exit 2
     (the existing test pattern of `test_plot_inputs`).
   - The read-only pool creates no `alpha_value` row.
4. **The paths.** A function of `extract_common.py`, `alpha_path(base, alpha) -> Path` (or the same rule
   inlined and tested where it lives), gives `alpha=0`, `alpha=0.1`, `alpha=0.03` for those α. Both scripts
   use it (by `ast`).
5. **`plot_Delta_N_eff`** on constructed records:
   - α = 0.03 and 0.1 at three β each, one with a NULL at 1 MeV: two curves per panel, the NULL left out,
     the file written;
   - α = 0 only: no file and `False`.
6. **`histories.csv`** has the three new columns and sorts by (α, M, n, β).
7. **`plot_dark_radiation`** on a dissipation history stored and read back on a temporary store (09's
   harness, M = 10⁻³, β = 1.2, α = 0.1, parked and released):
   - the file is written;
   - the plotted Δg* equals `gstar_rho * rho_chi_over_rho_rad` per sample, bitwise (read the line's data
     back from the axes);
   - two vertical lines, at the parking and the release;
   - at α = 0 no file and `False`.

   The test's cost and load go in its docstring.
8. **The scope** names α for the default and for three values.
9. **The inventory.** One α: `alpha=0` on the common line and not per history. Two α: `alpha=` per history
   line. `alpha_value` under "Redshifts, tolerances and parameter values".
10. **`history_and_bbn.py --alpha`** parses and refuses 1.0. Its α = 0 lines are byte-identical to the
    dispatch commit's on one history (pinned in the test from the dispatch commit, with how they were
    taken). At α = 0.1 the `dissipation` line is printed and parses back to `dissipation_values(data)`.
    One untuned integration at M = 0.5, β = 2 (2–3 s) is allowed; no PRyMordial solve unless the tool's
    test already runs one.
11. **`bbn_from_store.py --alpha`** on a temporary store holding one model at α = 0 and 0.1: each α finds
    its own history, and the default finds the α = 0 one.

**Existing tests that change.** Existing tests may change only where α's option changes what they test
(README §5):
- `test_plot_inputs`: `alpha` → `alphas`, and the α lookup by option;
- the scope's expected strings gain `α 0`;
- the inventory's expected lines move `alpha=0` from each history line to the common line;
- a test that reads a plot path gains the `alpha=` level.

List each module, its edit and its reason in the log. Any other edit to an existing test is a stop. Both
golden histories and every test of 01–09 that does not read an option, a path, the scope or the inventory
pass unedited.

**Each new test is shown to catch a breakage** (P11). Record each as a diff, with the tests it fails; never
commit one.
- B1: `--alpha-values` with `action="extend"` (the default no longer replaced).
- B2: `main.py` passing the first α row to every `run_pipeline` call.
- B3: `plot_inputs.parameters` looking up α = 0 whatever is given.
- B4: the `alpha=` level dropped at α = 0.
- B5: `plot_Delta_N_eff` drawing α = 0 as a curve.
- B6: Δg* drawn as `rho_chi_over_rho_rad` alone (g_ρ left out).
- B7: the scope without α.
- B8: `bbn_from_store.py`'s query without its α match.

---

## 2. Measurement

Small, since nothing computes differently. No Ray. Stores are temporary only.

1. **α = 0 is unchanged.** On one history (M = 0.5, β = 2, `test_injection`'s setup, untuned), at the
   dispatch commit and the final tree, in a temporary worktree of the dispatch commit removed afterwards:
   - `tools/history_and_bbn.py --fixed-normalization …` output at α = 0, byte for byte, BBN included (one
     PRyMordial solve per tree);
   - `tools/bbn_from_store.py` on one temporary store holding that history at α = 0: byte for byte.
2. **The figures,** drawn once each and committed nowhere: `Delta_N_eff.pdf` from the eight histories of log
   09 §5.1 at α = 0.1, with α = 0.03 added for M = 0.5 at β ∈ {1.2, 2, 5, 25}; and `dark_radiation.pdf` for
   M = 10⁻³, β = 1.2 and β = 2, M = 10⁻³⁰. State the y-ranges, and that the parking and release lines fall
   where log 09 §5.2's columns put them. Keep the PDFs in your scratch directory and name them in the log;
   the orchestrator looks at them.
3. **The cost.** The `--help` and parse time of each driver, before and after (a few ms; say if it moves).

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 473 (98 + 375) / 121 / 115 / 64 (log 09 §8). A different count, or a failure, is a stop.
2. **After:**
   - the five suites. Each count's change is explained by the new tests;
   - `black --check` on the files you changed;
   - `main.py --help`, `plot_by_beta.py --help`, `plot_ScalarHistory.py --help`,
     `RunDrivers/registered_run.py --help`, `tools/history_and_bbn.py --help` and
     `tools/bbn_from_store.py --help` exit 0, and each driver's help shows `--alpha-values` (the tools',
     `--alpha`);
   - `main.py --inventory` on a temporary store with two α (09's harness) shows `alpha_value` in its
     category and `alpha=` per history (no Ray);
   - a plot driver given `--alpha-values 0.3` against that store exits 2 before Ray, naming the option.
     If it cannot get there without Ray (it opens the pool after `ray.init`), test `plot_inputs` instead
     and say so.
3. **The breakages** of §1.10, each with the tests it fails.

## 4. Acceptance

1. **§1 as written, and the gate "Driver"** (README §2.3 and §8.4, restated):
   - α reaches every stage of `main.py`, once per value;
   - a plot asked for an unstored α exits 2;
   - the scope and the inventory name α;
   - R26's paths, R27's and R28's figures;
   - both golden histories bitwise, and every α = 0 output of the tools byte-identical.
2. **The measurement** of §2, in the log.
3. **The log**, `logs/10-driver-and-readers.md`, in the template of `dk-and-rpk-migration` README §5.1. It
   holds:
   - the suite counts before and after;
   - the existing tests edited, each with its reason;
   - the breakages as diffs;
   - every deviation, classified;
   - "State handed to the next prompt", for 11: how to run a grid over α with `main.py` and with
     `tools/history_and_bbn.py`, the `dissipation` line's fields, where each figure lands, and the
     inventory's α.
4. **The documents** of §1.9.
5. **The board:**
   - row 10 and the gate Driver, in §1 and §2;
   - `### 5.12 After prompt 10 (<date>)` in §5;
   - anything opened, on §3 and in `.documents/OPEN_ISSUES.md`, in the same commit (`CLAUDE.md`).
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history changes, or any α = 0 output of §2.1 is not byte-identical.
- An existing test needs an edit other than §1.10's list.
- The read-only plot path would create a row, or would need Ray to refuse an α.
- The work seems to need a change to:
  - the store's schema, a factory's `register`, `inventory_spec` or `build`, the fingerprint or
    `VERSION_LABEL`;
  - anything `integrate_history`, `tune_history`, `AdiabaticHistory` or BBN computes;
  - `run_pipeline`'s work items, batches or stage order in any driver.
- A figure cannot be drawn from what 09 stores without computing something new from the samples beyond
  P26's product.

## 6. What this prompt does not do

- **No change to what is computed or stored.** No schema, no fingerprint, no version.
- **No measurement grid.** U10 is 11's, by the tools this prompt extends.
- **No rehearsal.** U11 is 11's, on the isolated cluster.
- **No F.** BBN's frame factor (log 09 §1.2) is not drawn or stored (R28: "Both, two panels").
- **No surfing tool** changes.
- **Files it changes:**
  - `config/argument_parser.py`;
  - `main.py`, `plot_inputs.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `extract_common.py`;
  - `RunDrivers/registered_run.py`;
  - `tools/inventory_report.py`, `tools/history_and_bbn.py`, `tools/bbn_from_store.py`;
  - the new tests, and the existing tests of §1.10;
  - `.documents/architecture-summary.md` (and `numerical-strategies.md` only as §1.9 allows);
  - the log, this board and `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
