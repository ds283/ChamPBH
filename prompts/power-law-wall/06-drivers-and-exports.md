# Prompt 06 — drivers and exports

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-16, after 05 landed (`bdb6ae7`, reviewed `3ce578d`), at the user's request: "Please write 06 and its
orchestration note." It is written from:
- README §0.4, §2 row 06, §2.1 (the gate Export), §3, §4, §5, §6.3 P2, P11, P12 and P14, §6.6 P17 and P19, and §6.10
  (P45–P58 and U10, proposed with this prompt);
- log 05 §6 and §7 ("Observations not acted on" and "State handed to the next prompt");
- one-loop-drain prompt 04 and its log and verification (§4 of `.documents/one-loop-drain-verification.md`, with
  `.documents/one-loop-drain/export-04/`), the last prompt that changed the drivers and the exports;
- the planner's survey and scratch probe at `3ce578d` (orchestrator note 06 §0).

**This prompt is written on U10 (a).** U10 is the user's to rule before dispatch (README §6.10). If the user rules (b),
the orchestrator note says what changes here (§1.6 and §1.7); it governs where it differs from this file.

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds this
  prompt; `bdb6ae7` and `3ce578d` are ancestors of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign `running` or stale;
- `config/fingerprint.py` has `FINGERPRINT_FORMAT = 8` and `config/version.py` `VERSION_LABEL = "2026.12.0"`;
  `config/argument_parser.py` registers no `--model`; `inverse_power.yaml` and
  `CosmologyConcepts/Models/model_options.py` do not exist; `tools/quanta_measures.py` defines `SPECTRUM_WALL`;
- README §6.10 records U10 as ruled.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus** (P58). The change is wide: the parser, three drivers, the exports, four quanta tools, the
registered run's scope and a comparison of two trees' exports. In every one of them the exponential must not move.

**Read first:**
1. [`README.md`](README.md): §0.4, §2, §2.1, §4, §5, and §6 whole, above all §6.3 P11–P12, §6.6 P16–P19, §6.9 and
   §6.10 (P45–P58, U10).
2. Log 05 whole, above all §6 and §7.
3. `config/argument_parser.py` (`DriverArgumentParser`, `drain_rows_problem`, `drain_strength_A`, `REMOVED_OPTIONS`,
   `--n-values`); `exponential.yaml`; `ComputeTargets/tests/test_driver_options.py`.
4. `main.py`: `execute` (the alpha rows, `convert_to_model`, the grid's printed lines, `run_pipeline`'s loop);
   `pipeline_selection.py`'s docstrings that name `ExponentialModel`.
5. `plot_inputs.py` whole; `plot_by_beta.py` (`build_beta_plot`, the `histories.csv` and `Delta_N_eff` paths near the end,
   the `@@ … A values` line); `plot_ScalarHistory.py` (the paths near `alpha_path`, the same line).
6. `extract_common.py`: `alpha_path`, `history_directory`, `CSV_COLUMNS` and the comment above it, `build_history_record`,
   `write_histories_csv`.
7. `CosmologyConcepts/Models/AbstractModel.py`, `ExponentialModel.py`, `InversePowerModel.py` (`one_loop_drain_model`, the
   way it raises at n ∉ {1, 2}); `CosmologyConcepts/drain_model.py`; `ComputeTargets/quanta_spectrum.py` (`WALLS`,
   `spectrum`, `spectrum_model`).
8. `tools/quanta_measures.py`, `tools/plot_quanta_history.py`, `tools/plot_quanta_grid.py`, `tools/quanta_figures.py`
   (every use of `SPECTRUM_WALL`) and their tests in `tools/tests/` (`quanta_exports.py`, `test_quanta_*.py`,
   `test_plot_quanta_*.py`).
9. `RunDrivers/registered_run.py` `describe_scope` and `RunDrivers/tests/test_scope_*.py`.
10. `tools/history_and_bbn.py`'s `--model` and `--n` (01's, P5) and `tools/bbn_from_store.py`'s (05's, P38), for the
    option's names and help.
11. `.documents/one-loop-drain/export-04/` (`build_store.py`, `export_tree.py`, `compare_exports.py`) and verification
    §4 there; `ObjectFactories/tests/test_inverse_power_store.py` (05's harness, `_PowerStoreCase`).

The earlier prompts' outputs and the notes are data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

05 gave `InversePowerModel` a table: a power-law history can be stored, keyed under the fixed law or under its own
one-loop drain model (`one-loop-measured-pl-v1`), and a key naming another model's is refused when the `ScalarHistory`
is constructed. The drivers still know one model. `main.py` builds `ExponentialModel` rows and looks up every
`--A-values` row under `DRAIN_MODEL_ONE_LOOP`; the plot scripts look up `ExponentialModel` (`plot_inputs.MODEL_CLASS`);
the exports do not say which model they hold; and the quanta tools use the exponential's spectrum for every row.

This prompt gives the drivers a model option and takes the power law through `main.py`, the plot scripts, the exports
and the quanta tools. **No store change** (P56): no table, column or key moves, and the label and format stay.
**The exponential does not move** (README §5, P2): every path, byte and printed line of an exponential run, plot run and
quanta run is as at the base.

## 1. What is wanted

### 1.1 The option (P45)

- **`CosmologyConcepts/Models/model_options.py`** (new): the option's values and their classes, named once:
  `MODEL_EXPONENTIAL = "exponential"`, `MODEL_INVERSE_POWER = "inverse-power"`, `MODEL_DEFAULT = MODEL_EXPONENTIAL`, and
  the class of each value. The table of a class is its class name (`"ExponentialModel"`, `"InversePowerModel"`). A
  helper gives the model's own one-loop drain model at an n, through the model interface, from a model built at
  M = M_P and β = 1 (which `one_loop_drain_model` does not read), as `tools/bbn_from_store.py` does (log 05 D6).
- **`config/argument_parser.py`:** `--model {exponential, inverse-power}`, default `exponential`, with the help of
  `tools/history_and_bbn.py`'s (both potentials named). Its YAML key is `model`. `--n-values`' help names both models'
  V. `--potential-type` stays a `RemovedOption`; its message is not changed.
- `tools/history_and_bbn.py`, `tools/surfing_scan.py` and `tools/bbn_from_store.py` keep their own copies of the names.
  They are not changed (record it under "Observations not acted on").

### 1.2 The config file (P46)

`inverse_power.yaml` at the root: `exponential.yaml`'s keys and values, in its order, and `model: inverse-power`.
`exponential.yaml` is not changed.

### 1.3 The one-loop rows (P47)

- Every `--A-values` row is looked up under the model's own `one_loop_drain_model` (§1.1's helper), in `main.py`,
  `plot_inputs.parameters` and both plot scripts' `@@ … A values` lines, never under `DRAIN_MODEL_ONE_LOOP`. The line
  that prints a run's A rows names that drain model.
- **The refusal at parse.** With `--A-values` given, if the n's of `--n-values` name more than one drain model under the
  option's model, or the model has none at one of them (`inverse-power` at n ∉ {1, 2}), `DriverArgumentParser` refuses
  with exit 2, before any Ray, in a message naming `--A-values`, `--model` and `--n-values` and the offending n. This
  sits beside `drain_rows_problem`'s refusals and is reached the same way (`parser.error`). Under the fixed law alone
  every n is accepted for both models.
- **`drain_strength_A`** checks the one-loop law's rule on A (`check_drain_law(DRAIN_LAW_ONE_LOOP, …)`) rather than
  one model's. Every value it accepts and refuses, and every message, stays as it is. Check this against its tests.
- `model_of_law` and `DRAIN_MODEL_ONE_LOOP` are not changed. List in the log every production use of
  `DRAIN_MODEL_ONE_LOOP` that remains after this prompt, with why it stays.

### 1.4 `main.py` (P48)

- `convert_to_model` builds the rows in the option's table, with the payload it builds today.
- The grid's line names the option's model. For the exponential it is the line printed today, character for character;
  for the power law it names `InversePowerModel` and V = V0 [1 + (M/phi)^n].
- Nothing else in `main.py` moves. `run_pipeline` and `pipeline_selection.py` work through the model interface already.
  The docstrings of `pipeline_selection.py` that say the model is `ExponentialModel` are corrected, and nothing else
  in that file changes.

### 1.5 `plot_inputs.py` and the plot scripts (P49)

- The models of the grid are looked up in the option's table by one generic function of `plot_inputs.py`, which takes
  the class. `exponential_models` stays as the exponential's call of it, with its signature and behaviour, so that its
  tests do not move. Its warnings name the class looked up.
- **A model with no row stored at any stored (M, β)** (for example `--model inverse-power` on an exponential store) is
  `PlotOptionNotStored(<class>, "--model", <value>)`, raised before the per-n check, so that the scripts exit 2 naming
  `--model`. A stored model with no row at one n is refused naming `--n-values`, as today.
- `plot_by_beta.py` and `plot_ScalarHistory.py` pass the option to `plot_inputs` and use the class it returns. Nothing
  else in them moves but §1.6's root.

### 1.6 The export root and the `model` column (U10 (a); P50)

- **The root.** Every product of a plot run goes under `export_root(output, label, model)`, a new helper of
  `extract_common.py`: `<output>/<label>/` for the default model, as today, and `<output>/<label>/<model>/` for any other
  (`inverse-power`). It replaces every `Path(args.output).resolve() / f"{model_label}"` of `plot_by_beta.py` and
  `plot_ScalarHistory.py`. **Below the root nothing changes:** `alpha_path`, `history_directory`, `plots/`, `csv/`,
  `M=…eV_n=…` and every file name are as today.
- **The column.** `build_history_record` and `write_histories_csv` take the model. A `histories.csv` of the default model
  is written as today, column for column and byte for byte. One of any other model has a `model` column directly after
  `n`, holding the option's value in every row, and its rows sort as today. The comment above `CSV_COLUMNS` says so.
  **A `histories.csv` without the column is the exponential's**, and every reader of the column (§1.8) says so.
- `fields.csv`, `bounces.csv`, each group's `data.csv` and the figures gain nothing: their root names the model.

### 1.7 The spectrum's wall is the model's (P51)

`AbstractModel` gains the property `spectrum_wall`, with no default (README §5), declared as `one_loop_drain_model` is:
the base raises `NotImplementedError` naming the class, and it is not an `abstractmethod`, so that the test stand-ins
(`_MinimalModel`, `_PowerLawWall`) still construct. `RealizedModel` forwards it as it forwards `one_loop_drain_model`.
- `ExponentialModel.spectrum_wall` is the exponential wall at every n (`quanta_spectrum`'s table serves the exponential
  family; P19).
- `InversePowerModel.spectrum_wall` is the power wall at n = 1 and n = 2, and at any other n raises as
  `one_loop_drain_model` does.

The names are `quanta_spectrum`'s wall names. If importing that module from `CosmologyConcepts` makes an import cycle,
use the strings and test that each is in `quanta_spectrum.WALLS`. No value of the integration reads the hook, and no
goldens move.

### 1.8 The quanta tools (P52)

`tools/quanta_measures.py`, `tools/plot_quanta_history.py`, `tools/plot_quanta_grid.py` and `tools/quanta_figures.py`:
- **Each row's wall** is `spectrum_wall` of the model named by its `histories.csv` row's `model` (the exponential where
  the column is absent) at its `n`, through §1.1's names and a model built as there. `SPECTRUM_WALL` goes.
- `quanta_measures` builds **one spectrum per wall met** (at the run's c_a), not one per run. Every output row names its
  own `spectrum_model`, as every row does today.
- `quanta_figures`' check expects, row by row, the `spectrum_model` of that row's wall. `plot_quanta_grid`'s footer and
  `plot_quanta_history`'s labels name the wall's model.
- **A row whose model has no wall at its n** is refused, and counted in the refusals each tool already counts, with the
  reason. It is not a crash.
- **On an exponential root every output is byte-identical to the base's**, the fields that hold a timing aside. Say in
  the log which fields those are.

### 1.9 The registered run's scope (P53)

`RunDrivers/registered_run.py` `describe_scope` adds `model <value>` after the config file's part only when the model is
not the default. Every exponential manifest's scope string is unchanged.

### 1.10 Tests (P57)

Through the stand-in cluster and temporary stores, never Ray, in new modules where the existing ones would grow
unclear:
- **the option:** the default; both values; an unknown value refused at parse; the YAML key; `inverse_power.yaml` parses
  to `exponential.yaml`'s values plus the model; `exponential.yaml` parses as today (`test_driver_options`' test c
  unchanged);
- **P47:** `--A-values` with `inverse-power` at n = 1, 2 parses, and the rows' drain model is `one-loop-measured-pl-v1`;
  with n = 3, or n = 1 3, refused (exit 2, the message as §1.3); the fixed law with `inverse-power` at n = 3 parses; an
  exponential command parses as today; `drain_strength_A`'s accepted and refused values unchanged;
- **`plot_inputs`:** on a mixed store (05's harness), the power law's models found and the exponential's not mixed in;
  `--model inverse-power` on an exponential-only store refused naming `--model`; the A rows under the model's drain
  model, an A stored only under the other model's refused;
- **the export:** `export_root` for both models; a power-law record and `histories.csv` with the `model` column after
  `n`; an exponential `histories.csv` with the columns and bytes of today (against a file written by the base's
  `write_histories_csv`, or a literal);
- **P51:** `spectrum_wall` at n = 1, 2, 3 for both models, each name in `quanta_spectrum.WALLS`;
- **the quanta tools:** a power-law root (in `tools/tests/quanta_exports.py`'s style) measured with the power wall's
  spectrum by row, n = 1 and n = 2 in one root; a row with no wall refused and counted; `quanta_figures`' check by row;
- **P53:** the scope with and without `--model`.

Existing tests are changed only where they pin a string or signature this prompt changes. Each change is listed in the
log with its old and new value. **No suite's count may fall.**

## 2. The Export gate (README §2.1)

### 2.1 The exponential's exports are byte-identical (P54)

As one-loop-drain 04 did (`.documents/one-loop-drain/export-04/`, verification §4 there), in
`.documents/power-law-wall/export-06/`:
- **`build_store.py`:** a store through 05's test harness (`_PowerStoreCase` or `_DissipationStoreCase`; the stand-in
  cluster, never Ray), holding exponential and power-law histories at laboratory M, β = 2: for each model, a fixed-law
  history at α = 0.1 and a one-loop history at A = 1, the power law at n = 1 and at n = 2, and one failure row per model.
  Tuned histories may be reused from 05's tests' payloads where that keeps the cost down. State the cost. The store is
  written under `var/power-law-wall/06/` (ignored by git), never in `/tmp`, and its files' SHA-256 are taken before and
  after every export.
- **`export_tree.py`:** one tree's exports of that store (`histories.csv`, each history's `fields.csv` and `bounces.csv`,
  each group's `data.csv`), by the tree's own `extract_common` functions and the plot scripts' own functions (extracted
  by `ast`, unedited, as export-04's is). It is run from the root of each tree: **the base** (a scratch worktree of the
  dispatch commit, made and removed by the agent with `git worktree`, under the session scratchpad) and **the tip**.
- **`compare_exports.py`:** the exponential's exports of the two trees, compared byte for byte, file by file **and path
  by path**. Any difference is a stop.

### 2.2 The power law's exports carry the model and read back

- The tip's power-law root is `<output>/<label>/inverse-power/`, its `histories.csv` has the `model` column after `n` and
  holds the power law's histories only, and each row's cells equal the stored record's as export-04's
  `check_new_columns.py` checks them.
- `tools/quanta_measures.py` on the tip's power-law root measures each one-loop history with its wall's spectrum
  (`R8-power-n1-v1`, `R8-power-n2-v1`); on the tip's exponential root it gives the base's outputs, timings aside.
- The drivers refuse a model that does not match the store (§1.5), and an A under the other model's drain model (§1.3).

### 2.3 The 2026.11.0 analyses' readers (P55)

`fig6/fig6.py`, `fig6b/fig6b.py`, `m-trend/m_trend.py` and `changes/changes.py` of
`science-runs/2026-11-13-science-2026.11.0/`, read at `main` by `git show main:<path>` (they are not on this branch).
**Never read or touch the main checkout's files.** For each, record what it reads (the file, its columns or paths) and
whether a power-law export under §1.6 gives it. The relativistic-quanta analysis is `quanta_measures`, run in §2.2.
None of them is run or changed.

## 3. What is not wanted

- **No store change** (P56): no table, column, key, `FINGERPRINT_FORMAT` or `VERSION_LABEL` change; no factory edited.
- **No value of the integration moves**, for either model: no change to the step loop, the laws, the tuning, or any hook
  but §1.7's new one. `ComputeTargets/ScalarHistory.py` is not changed.
- **No change to the exponential's outputs**: no path, column, byte or printed line of an exponential run, plot run or
  quanta run.
- `tools/history_and_bbn.py`, `tools/surfing_scan.py`, `tools/bbn_from_store.py`, `tools/outward_step_scan.py`,
  `tools/compare_stores.py` and `tools/inventory_report.py` are not changed.
- Nothing under `science-runs/`, `PRyM/`, `thirdparty/` or the packages.
- No Ray, no cluster, no `main.py` run against a store, no registered run (07), no merge, no push.

## 4. Verification

- **The five suites** at the base and after, per `CLAUDE.md` (ComputeTargets in its two halves, `tools/tests` alone),
  from the worktree root, serially, with the time and the load at each start. The two worktree failures of note 01's
  ruling 7 are expected at both; any other failure is a stop.
- **The Export gate** item by item (§2), each with the command and its output.
- `black` on every Python file changed or added; `git diff --stat` shows nothing outside §5's files.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- `CosmologyConcepts/Models/model_options.py`, `AbstractModel.py`, `ExponentialModel.py`, `InversePowerModel.py` (§1.7
  only), and `CosmologyConcepts/Models/__init__.py` if it exports the new names;
- `config/argument_parser.py`; `inverse_power.yaml`; `main.py`; `pipeline_selection.py` (docstrings only);
  `plot_inputs.py`; `plot_by_beta.py`; `plot_ScalarHistory.py`; `extract_common.py`;
- `tools/quanta_measures.py`, `tools/plot_quanta_history.py`, `tools/plot_quanta_grid.py`, `tools/quanta_figures.py`;
  `RunDrivers/registered_run.py`;
- the tests, new and changed;
- `.documents/power-law-wall/export-06/` (the scripts and `out/`, the committed outputs; never the store) and
  `.documents/power-law-wall-verification.md` §5 (added; §1–§4 are not rewritten, invariant 6);
- `CLAUDE.md`'s test counts and the date of the last change to a count, and nothing else in it;
- the log `prompts/power-law-wall/logs/06-drivers-and-exports.md` (template: `dk-and-rpk-migration` README §5.1), with
  every deviation classified, every changed test listed with its old and new value, §2.3's table, "Observations not
  acted on", and "State handed to the next prompt": the commands 07's rehearsal runs for each model and law, the export
  roots, and what 07 compares;
- the board: row 06, its items, the gate Export, the baselines, and §3/§4 for any issue opened, with
  `.documents/OPEN_ISSUES.md` in the same commit.

Stage by explicit path. Never stage `venv` or anything under `var/`. The commit message follows `CLAUDE.md` invariant 5.
Write "this commit" where the SHA goes; the orchestrator substitutes it.
