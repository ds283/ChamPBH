# Log 06 — drivers and exports

**Subject:** the potentials treated symmetrically (R12): `--models` for `main.py` and both plot scripts, each
potential's one-loop rows under its own drain model, every export under `<output>/<label>/<model>/` with a `model`
column, `spectrum_wall` on the model and the quanta tools' wall by row, the registered run's scope (P45–P58).
**Commit:** `fda2df2`, on branch `power-law-wall`, on top of `b1433f4`
**Date:** 2026-11-16, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Export holds (§4).
- **The option.** `--models {exponential, inverse-power} …`, default `[exponential]`, YAML key `models`; the values and
  their classes are named once, in `CosmologyConcepts/Models/model_options.py`. `exponential.yaml` gains
  `models: [exponential]`; `inverse_power.yaml` is its keys with `models: [inverse-power]`.
- **The one-loop rows.** Each potential's `--A-values` rows are looked up under its own `one_loop_drain_model`, through
  the model interface; a potential with no one-loop law at an n of `--n-values` is refused at parse (exit 2) under
  `--A-values`. No driver, the parser or `plot_inputs.py` names `DRAIN_MODEL_ONE_LOOP` any more.
- **main.py** builds each potential's rows in its own table and runs its pipelines, potential by potential. For one
  potential its lookups, pipelines and printed lines are those of the base, in five commands (§3.3).
- **The exports.** Every product of a plot run goes under `export_root(output, label, model)`; every `histories.csv` has
  `model` after `n`. The exponential's exports equal the base's once that directory and that column are accounted for
  (§4.1).
- **The quanta tools** take each row's wall from its `model` and `n` (`AbstractModel.spectrum_wall`); `SPECTRUM_WALL` is
  gone; a `histories.csv` without `model` is refused, exit 2. On the exponential root every output equals the base's,
  timing fields aside (§4.2).
- **No store change**: no table, column or key; `FINGERPRINT_FORMAT` 8 and `VERSION_LABEL` 2026.12.0 stand.
- **One unintended Ray start** (D4): the first base export started a private local Ray instance through `ray.put` and
  failed; the script was corrected, and Ray is patched out of every later export. Reported, not hidden.
- **Issues:** one opened (§6, board §3): `plot_quanta_grid` refuses a power-law root with n = 1 and 2 at one (β, M).
- **Suites:** base 26, 739 (226 + 513), 175, 307, 76; after 26, 783 (226 + 557), 175, 312, 79. In both runs only the two worktree failures
  of note 01's ruling 7 failed.

## 1. What shipped

- **`CosmologyConcepts/Models/model_options.py`** (new): `MODEL_EXPONENTIAL`, `MODEL_INVERSE_POWER`, `MODEL_CLASSES`,
  `MODEL_OPTIONS`, `MODEL_POTENTIALS` (each potential's V, for the printed lines and the help); `model_class`,
  `model_table`, `model_option` (the value of a model object or class); `reference_model` (the potential at n, M = M_P,
  β = 1, Planck units); `one_loop_drain_model`, `one_loop_drain_model_over` (exactly one drain model over a list of n,
  else a `ValueError` naming the potential and the n) and `spectrum_wall`, each read through the model's hook.
- **`AbstractModel.py`**: the property `spectrum_wall`, with no default (the base raises `NotImplementedError` naming the
  class; not an `abstractmethod`); `RealizedModel.spectrum_wall` forwards it. **`ExponentialModel.py`**: the exponential
  wall at every n. **`InversePowerModel.py`**: `power-n1` at n = 1, `power-n2` at n = 2, a `ValueError` naming the class,
  the hook and n otherwise. The names are `quanta_spectrum`'s, imported (no cycle; §2.2 ruling note).
- **`config/argument_parser.py`**: `--models` (`choices`, `nargs="+"`, default `[exponential]`), with the help of
  prompt §1.1; `--n-values`' help names both V; `models_problem`, the refusal at parse, run by `DriverArgumentParser`
  after `drain_rows_problem`, through `self.error`; `drain_strength_A` checks `check_drain_law(DRAIN_LAW_ONE_LOOP, …)`;
  the `--A-values` help names each potential's own drain model. `--potential-type` is unchanged.
- **`exponential.yaml`**, **`inverse_power.yaml`** (new): as §1.2.
- **`main.py`**: `models = distinct_values(args.models)`; each potential's drain model (`A_drain_models`) only under
  `--A-values`; the A rows looked up A by A, each under every potential's drain model in the order of `--models`, after
  the fixed rows in the tolerances' `ray.get`; the grid's line once per potential; `convert_to_model(table, …)` once per
  potential, in order, over one list of (M, n, β); the loop over the cosmologies runs each potential in turn, and the loop
  over `alpha_rows` skips another potential's A rows. The A line and the total line name each potential when there are
  several (D11).
- **`pipeline_selection.py`**: the two docstrings that named `ExponentialModel` alone; nothing else.
- **`plot_inputs.py`**: `MODEL_CLASS` is gone; `stored_models(pool, class_name, …, potential=None)`, the generic lookup,
  which with `potential` raises `PlotOptionNotStored(class, "--models", potential)` before the check of n;
  `potential_models(pool, potential, …)`, the scripts' call; `exponential_models` kept, with its signature and behaviour,
  as the exponential's call of `stored_models`; `parameters(…, A_values=(), A_drain_models=())` (D5).
- **`plot_by_beta.py`**, **`plot_ScalarHistory.py`**: `--models`, each potential's models and A rows, one pass per
  potential in the order given; every root through `export_root`; a line per pass naming the potential and its root;
  the A line per drain model (D10). `build_beta_plot` and `plot_ScalarHistory` take the potential from their model
  (`model_option`); `run_pipeline` (plot_by_beta) and `write_alpha_summaries` take it as a parameter (D9).
- **`extract_common.py`**: `export_root(output, label, model)`; `model` in `CSV_COLUMNS` after `n`, with its paragraph;
  `build_history_record(…, model, …)`, required, checked against the options; `write_histories_csv` refuses records of
  more than one potential (D8).
- **`tools/quanta_measures.py`**: `MODEL_COLUMN`, `wall_of(row)`, `root_models_text(rows)`; a `histories.csv` without
  `model` is exit 2; one spectrum per wall met; every output row its own `spectrum_model`; a row with no wall refused and
  counted; the summary names the spectrum models of the root's rows (D12). **`tools/quanta_figures.py`**: `read_histories`
  refuses a file without `model` (a `csv.Error`, which both figure tools already map to exit 2); `spectrum_model_of`,
  `expected_models`, `models_text`; `check_model_rows` and `read_checked` take the expected model (a string or a function
  of the row). **`tools/plot_quanta_grid.py`**, **`tools/plot_quanta_history.py`**: the check by row, each figure's footer
  or label naming its rows' model(s), the summary line naming the root's.
- **`RunDrivers/registered_run.py`**: `describe_scope` adds `models <values>` after the config file's part.
- **Tests, new** (§3.5) and **changed** (§3.6).
- **`.documents/power-law-wall/export-06/`**: `build_store.py`, `export_tree.py`, `compare_exports.py`,
  `check_power_law.py`, `quanta_roots.sh`, `compare_quanta.py`, `main_harness.py`, `main_order.py`, `main_cases.txt`, and
  `out/` (`build_store.txt`, `manifest.json`, `export-base.txt`, `export-tip.txt`, `compare.txt`, `power_law.txt`,
  `quanta.txt`, `main_order.txt`). **`.documents/power-law-wall-verification.md` §5** (added).
- **`CLAUDE.md`**: the counts and the note of their last change. **The board** and **`.documents/OPEN_ISSUES.md`**: row
  06, the gate Export, the baselines, the status line, and the one issue of §6.
- **Not committed:** the suites' outputs, the drafts and probes in the agent's scratch directory `…/scratchpad/agent06/`;
  the store, the exports and the quanta runs under `var/power-law-wall/06/`.

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note 06 §0)

1. **The worktree and the main checkout.** All work in the worktree with `./venv/bin/python` and `PYTHONPATH=.`; the main
   checkout neither read nor touched (the 2026.11.0 analyses read by `git show main:<path>` only); `venv` and `var/` not
   staged; `/tmp/ray-cpbh` never named. The base worktree was made under the scratchpad (`git worktree add --detach
   …/agent06/base b1433f4`) and removed with `git worktree remove` before the commit. One exception to "nothing in
   `/tmp`": D4. *STRUCTURALLY REQUIRED.*
2. **The exponential's histories do not move, and its exports only as R12 says.** The exponential exports equal the
   base's once `exponential/` and `model` are accounted for (§4.1); every `main.py` printed line, lookup and pipeline of an
   exponential-only run equals the base's in five commands (§3.3); every quanta output on the exponential root equals the
   base's, timing fields aside (§4.2); no golden's expectation is edited. No stop. *STRUCTURALLY REQUIRED.*
3. **The order of a one-potential run.** The lookups before the grid (the tolerances' `ray.get`: the three tolerances,
   the fixed rows, then the A rows) are one list, as before: for one potential `A_lookup_models` has one element, so
   `for A in A_values for drain_model in A_lookup_models` gives one row per A in the order of `--A-values`, the base's
   order; the solvers, the axis, β and M follow as before; the models are one `object_get` per potential, after the M
   grid, as before. `test_models_main` pins the base's sequence as a literal, and `main_order.py` compares five commands
   item for item (§3.3). No stop. *STRUCTURALLY REQUIRED.*
4. **Existing tests** changed only where they pin a string, path, column or signature this prompt changes (§3.6). No
   stop. *IMPLEMENTATION CHOICE, ruled.*
5. **The A rows.** No driver, the parser or `plot_inputs.py` looks up an A row under a drain model other than the
   potential's own; the remaining production uses of `DRAIN_MODEL_ONE_LOOP` are §3.4's, none of which selects or looks up
   a row. No stop. *IMPLEMENTATION CHOICE, ruled.*
6. **The two worktree failures** (`test_registered_run…this_repositorys_head`, `test_registry_config…
   test_roots_and_distributions`) failed at the base and after; nothing else failed. *IMPLEMENTATION CHOICE, ruled.*
7. **The machine.** Serial suites with `uptime` at every start and end (§3.1); another process loaded the machine
   (one-minute loads 5–45). *IMPLEMENTATION CHOICE, ruled.*
8. **The verification store** built through the stand-in cluster under `var/power-law-wall/06/`; its files' SHA-256
   equal before and after every export (§4.1); nothing under `var/` committed. *IMPLEMENTATION CHOICE, ruled.*
9. **A finding about production** recorded (§6) and opened as one §3 issue, with `.documents/OPEN_ISSUES.md`.
   *IMPLEMENTATION CHOICE, ruled.*
10. **No file but prompt §5's list.** The commit holds exactly §5's files (§1, `git diff --cached --name-status` before
    committing), with `tools/tests/quanta_exports.py`, a test fixture, among "the tests, new and changed".
    *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1 — the tip's exports are two plot runs into one `--output`.** `--n-values` is one list for every potential of a
  plot run, and the store holds the exponential at n = 1 only, so `--models exponential inverse-power --n-values 1 2`
  would refuse the exponential naming `--n-values`. `export_tree.py` exports each potential as
  `--models <potential> --n-values <its n> --no-plots` would (exponential [1], power law [1, 2]), each over its own model
  rows, into one output. *IMPLEMENTATION CHOICE.*
- **D2 — stand-in BBN rows in the verification store.** `build_beta_plot` returns before writing `data.csv` when no
  history of the group has BBN data, so a store without BBN rows would export no `data.csv` in either tree. Each successful
  history has a constructed BBNData row (no samples, `PRyM_version` "stand-in"), so the comparison covers each group's
  `data.csv`. No PRyMordial solve. *IMPLEMENTATION CHOICE.*
- **D3 — the plot scripts' functions are run without `@ray.remote`.** `export_tree.py` executes each tree's top-level
  imports and the functions it needs, extracted by ast and unedited except that their decorators are not applied (the
  undecorated function, as a test calls `fn._function`). *IMPLEMENTATION CHOICE.*
- **D4 — one unintended Ray start, by the first base export.** The first version of `export_tree.py` passed a
  `ScalarHistoryProxy` to the BBNData lookup. Its constructor calls `ray.put`, which the stand-in cluster does not patch,
  so Ray auto-initialised a private local instance inside that process (session directory
  `/tmp/ray/session_2026-11-16_11-52-35_742188_42121`, with `/tmp/ray/session_latest` and
  `/tmp/ray/prom_metrics_service_discovery.json` pointing at it). The export then failed (`AttributeError: 'ray.ObjectRef'
  object has no attribute 'cosmology'`) and the process exited; `ps` showed no raylet, GCS or worker left. It touched no
  cluster, no lock and no store (`/tmp/ray-cpbh` was not involved). The script now uses a stand-in proxy holding the
  history, and patches `ray.init` and `ray.put` to raise for the whole export, and asserts Ray is not initialised at its
  end; every later export ran under that guard. The session directory under `/tmp/ray` is left for the user to remove.
  *UNINTENDED DRIFT* (note 06's "No Ray").
- **D5 — `plot_inputs.parameters` gains `A_drain_models`** (the prompt's choice between "gains the potentials" and "a
  per-potential call"): the distinct drain models of the potentials of `--models`, in order; each A is looked up under each
  of them, A by A, so one potential gives one row per A as before. `A_values` without `A_drain_models` is a `ValueError`
  (no drain model is assumed). The scripts select a potential's rows from `alpha_rows` by drain model in the loop.
  *IMPLEMENTATION CHOICE.*
- **D6 — the A rows' order for several potentials is A-major** in `main.py` and `plot_inputs` (A₁ under every potential's
  model, then A₂, …). For one potential it is the base's order; for several, each potential's own rows are still in the
  order of `--A-values`, since the loop over `alpha_rows` keeps the fixed rows and the potential's own. *IMPLEMENTATION
  CHOICE.*
- **D7 — "no row at any stored (M, β)"** is judged over the grid the plot run asks for, stored M × `--n-values` × stored β:
  a potential stored only at n = 2 and asked at n = 1 alone is refused naming `--models`. The factories offer no read of a
  model table, so the store's other n are not looked for. *IMPLEMENTATION CHOICE.*
- **D8 — `write_histories_csv` takes the model through the records**, not a new parameter: `model` is in `CSV_COLUMNS` and
  in every record, and records of more than one potential are a `ValueError`, so a file is one potential's.
  *IMPLEMENTATION CHOICE.*
- **D9 — where the plot scripts find the potential.** `build_beta_plot` (a Ray task) and `plot_ScalarHistory` (a Ray task)
  take it from their model object (`model_option(group_model)`, `model_option(lagrangian)`), so their signatures do not
  change; `plot_by_beta.run_pipeline` and `write_alpha_summaries` take it as a parameter. *IMPLEMENTATION CHOICE.*
- **D10 — the plot scripts print one new line per pass**, `@@ plot_by_beta: <potential> (<table>): N model(s), exported
  under <root>` (and `plot_ScalarHistory`'s), so an exponential-only plot run prints one line more than before; the
  `@@ … A values` line is one per drain model, as before for one potential, with ` for <potentials>` when `--models`
  names more than one. Ruling 2's printed-line rule is `main.py`'s, whose lines are unchanged. *IMPLEMENTATION CHOICE.*
- **D11 — main.py with several potentials** prints the A line as `(<drain model> for <potential>; …)` and the total line
  as `= <total> (<potential> <count>, …)`; for one potential both are today's lines. *IMPLEMENTATION CHOICE.*
- **D12 — the quanta tools' summary lines** name the spectrum models of the walls of every row of the root's
  `histories.csv`, whatever its law, in the order met ("no spectrum" if none). On an exponential root that is
  `R8-exponential-v1`, as at the base, whether or not it has a one-loop row (checked on a fixed-law-only copy); on the
  power-law root `R8-power-n1-v1, R8-power-n2-v1`. *IMPLEMENTATION CHOICE.*
- **D13 — `tools/tests/quanta_exports.py`'s `history_record` takes `model` with the default `"exponential"`**, the
  potential of every existing caller, so that the fixture's existing callers do not change; `export_history` passes the
  history's own (`model_option(full_object.model)`). *IMPLEMENTATION CHOICE.*
- **D14 — the import of `quanta_spectrum` from the models.** `ExponentialModel` and `InversePowerModel` import the wall
  names from `ComputeTargets.quanta_spectrum` (numpy and scipy only), after `ComputeTargets.exceptions`, which already
  initialises `ComputeTargets`; importing `CosmologyConcepts.Models`, `ComputeTargets`, `model_options`, the parser,
  `extract_common`, `plot_inputs` and `tools.quanta_measures` first, each in a fresh process, succeeds. §1.7's fallback
  (strings) was not needed. *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 The suites (P58)

Serial, from the worktree root (`run_suites.sh` in the scratch directory). Cells: count / result / wall / one-minute load
at start → end. The base's store and exports (§4) ran from the base worktree during the base suites' ComputeTargets
halves (11:50–11:54), which loads those timings.

| Suite | Base `b1433f4` (2026-11-16 11:43–12:08) | After (2026-11-16 12:25–12:47) |
|---|---|---|
| CosmologyModels | 26 / OK / 82 s / 37.74 → 44.93 | 26 / OK / 65 s / 19.48 → 15.39 |
| ObjectFactories | 175 / OK / 175 s / 44.93 → 17.93 | 175 / OK / 164 s / 15.39 → 14.91 |
| RunDrivers | 76 / 1 failure (ruling 6) / 31 s / 17.93 → 19.36 | 79 / 1 failure (ruling 6) / 27 s / 14.91 → 16.13 |
| ComputeTargets `[a-f]` | 226 / OK / 175 s / 19.36 → 20.70 | 226 / OK / 160 s / 16.13 → 16.50 |
| ComputeTargets `[g-z]` | 513 / OK / 602 s / 20.70 → 9.38 | 557 / OK / 555 s / 16.50 → 5.11 |
| tools | 307 / 1 failure (ruling 6) / 390 s / 9.38 → 10.94 | 312 / 1 failure (ruling 6) / 334 s / 5.11 → 9.48 |

The failures are `test_registered_run…test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` (RunDrivers)
and `test_registry_config…test_roots_and_distributions` (tools), at both.

No count fell. The rises: ComputeTargets `[g-z]` +44 (`test_models_option` 25, `test_models_export` 8,
`test_models_plot_inputs` 7, `test_models_main` 4), tools +5 (`test_quanta_models`), RunDrivers +3
(`test_scope_models`).

An earlier after-run was stopped at its second minute (in `[a-f]`) when D12 was changed, and every suite was run again
after it; the run above is the one after the last change.

### 3.2 `black`

`black` on every Python file changed or added (45, the export-06 scripts included); `black --check` leaves all of them
unchanged. The changed test files differ from the base only in the lines of §3.6.

### 3.3 main.py, one potential (ruling 3)

`.documents/power-law-wall/export-06/main_order.py <base worktree> .` (about 12:16; its load was not recorded, and it times nothing): five commands, the tip as given
and with `--models exponential`, ten comparisons, each "lookups, pipelines and printed lines IDENTICAL"
(`out/main_order.txt`). `test_models_main` checks the same for one command against a literal of the base's sequence,
and for the power law alone and both potentials in either order: the A rows A by A under each potential's model, each
potential's table in the order of `--models`, its pipelines (the fixed rows, then its own A rows), the grid line once per
potential.

### 3.4 `DRAIN_MODEL_ONE_LOOP` in production after the change (ruling 5)

`git grep -n "DRAIN_MODEL_ONE_LOOP\b" -- '*.py' ':!.documents' ':!science-runs' ':!prompts' ':!*/tests/*'`:
- `CosmologyConcepts/drain_model.py` — its definition, `DRAIN_MODELS`, `_LAW_OF_MODEL` and `_MODEL_OF_LAW` (`model_of_law`),
  and comments: the law's own module, not changed (prompt §1.3);
- `CosmologyConcepts/Models/ExponentialModel.py:34, :100` — `one_loop_drain_model` returns it: the exponential naming its
  own drain model, which is the hook every driver now reads;
- `ComputeTargets/ScalarHistory.py:52` (the import, kept so that `SM.DRAIN_MODEL_ONE_LOOP` resolves) and comments at
  :205, :298, :300, :855, :2304 — not changed (prompt §3);
- `CosmologyConcepts/alpha_value.py:30`, `ObjectFactories/alpha_value.py:22` — docstrings of the key.

None is in a driver, the parser or `plot_inputs.py`, and none selects or looks up a row. `model_of_law` has no production
caller (its two mentions are `tools/bbn_from_store.py`'s docstrings).

### 3.5 Tests, new

- `ComputeTargets/tests/test_models_option.py` (25): the option, the YAML key and files, P47's acceptances and refusals
  (through `parser.error` too), `drain_strength_A` as before, the names of §1.1, `spectrum_wall` at n = 1, 2, 3 on both
  potentials, each in `quanta_spectrum.WALLS`, the realized model's forwarding, the base without a default.
- `ComputeTargets/tests/test_models_main.py` (4): P48, by the extracted `execute` on a stood-in pool.
- `ComputeTargets/tests/test_models_plot_inputs.py` (7): P49 and the A rows on a mixed store and an exponential-only
  store built through the stand-in cluster, read-only.
- `ComputeTargets/tests/test_models_export.py` (8): `export_root`, the column, the refusal of two potentials, no default,
  and the exponential's file against the base's writer (a literal written by `extract_common` at `b1433f4`).
- `tools/tests/test_quanta_models.py` (5): a power-law root at n = 1 and 2 measured row by row, a row with no wall (n = 3,
  and an unknown potential) refused and counted, a `histories.csv` without `model` refused by all three tools, the check
  by row, both figure tools on the power root.
- `RunDrivers/tests/test_scope_models.py` (3): P57.

### 3.6 The existing tests changed, old → new

Each pins a string, path, column or signature this prompt changes (ruling 4).
- **`ComputeTargets/tests/test_a_values_option.py`**: `test_2_main_looks_up_one_row_per_A_under_the_constant_after_the_alpha_rows`
  and `test_2_plot_inputs_looks_the_A_rows_up_under_the_constant`: the A lookup's keywords
  `[("value", "A"), ("drain_model", "DRAIN_MODEL_ONE_LOOP")]` → `[("value", "A"), ("drain_model", "drain_model")]`
  (each potential's own drain model, P47; ruling 5); the comment above the first. The test names are kept.
- **`ComputeTargets/tests/test_a_values_plot_inputs.py`** `_parameters`: with `As` given, also
  `A_drain_models=["one-loop-measured-v1"]` (the new keyword, D5).
- **`ComputeTargets/tests/test_driver_options.py`** `test_d_the_plot_scripts_match_n_and_the_tuning_tolerance`: the call
  `plot_inputs.exponential_models` → `plot_inputs.potential_models` (twice), with a comment.
- **`ComputeTargets/tests/test_plot_inputs.py`** `test_the_setup_is_plot_inputs`: the called function
  `"exponential_models"` → `"potential_models"`.
- **`ComputeTargets/tests/test_extraction.py`** (three calls), **`ObjectFactories/tests/test_alpha_figures.py`** (three),
  **`ObjectFactories/tests/test_export_04.py`** (one), **`ObjectFactories/tests/test_tuned_history_round_trip.py`**
  (one): `build_history_record(…)` gains `model="exponential"` (the new required keyword, P51).
- **RunDrivers**: the pinned scopes `"no config file; β …"` → `"no config file; models exponential; β …"`
  (`test_no_alpha_values` 1, `test_registered_run` 2, `test_scope_a_values` 2, `test_scope_alpha` 1) and
  `"config exponential.yaml; β …"` → `"config exponential.yaml; models exponential; β …"` (`test_registered_run` 1)
  (P57).
- **`tools/tests/quanta_exports.py`** (the fixture): `history_record` gains `model: str = "exponential"` and the record
  field (D13); `export_history` passes `model=model_option(full_object.model)` to `build_history_record`.
- **`tools/tests/test_no_alpha_values.py`**: the `plot_inputs.parameters` call gains
  `A_drain_models=["one-loop-measured-v1"]` (D5).
- **`tools/tests/test_plot_quanta_grid.py`**: the two `pg.build_figure(…)` calls gain
  `model=qs.spectrum_model(qs.WALL_EXPONENTIAL)` (the parameter lost its default, which named the exponential), and the
  import of `quanta_spectrum`.
- **`tools/tests/test_quanta_figures.py`**: `qs.spectrum_model(qf.SPECTRUM_WALL)` (twice) → the module constant
  `EXPONENTIAL_SPECTRUM = qs.spectrum_model(qs.WALL_EXPONENTIAL)` (`SPECTRUM_WALL` is gone); the six
  `qf.read_checked(path, scale)` calls → `qf.read_checked(path, scale, EXPONENTIAL_SPECTRUM)` (the expected model,
  P53).

No golden, fingerprint or exponential history test is edited.

## 4. The gate Export, item by item (prompt §2)

**2.1 The exponential's exports equal the base's.** Commands from the worktree root (`V = var/power-law-wall/06`):
- `cd <base> && PYTHONPATH=. <venv>/python .documents/…/build_store.py --dest $V` (11:50, load 18.1 → 23.5): 8 histories,
  49.2 s (`out/build_store.txt`).
- `cd <base> && PYTHONPATH=. <venv>/python .documents/…/export_tree.py --store $V/store --manifest $V/manifest.json --out
  $V/export-base` (11:53, load 34.3): "potentials ['exponential']; 6 files; store unchanged: True".
- `PYTHONPATH=. ./venv/bin/python .documents/…/export_tree.py … --out $V/export-tip` (12:18, load 10.8): "potentials
  ['exponential', 'inverse-power']; 17 files; store unchanged: True".
- `PYTHONPATH=. ./venv/bin/python .documents/…/compare_exports.py $V/export-base $V/export-tip`: exit 0. Paths the same
  file for file; `data.csv` ×2, `fields.csv` ×2 and `bounces.csv` IDENTICAL; `histories.csv`: "model at column 3 (after
  n: True), cells ['exponential'], rows 4; with model removed: IDENTICAL (2449 and 2449 bytes)"; the tip's `<label>/` holds
  `['exponential', 'inverse-power']` only. The store's SHA-256 (`store-shard0000.db` `2ec681d9…6d1abfe`, `store.db`
  `7aa84846…7f4601dd`) equal before and after both exports.

**2.2 The power law's exports carry the potential and read back.**
- `check_power_law.py $V/store $V/export-tip`: exit 0; 6 rows, `model` after `n`, `inverse-power` throughout; the five
  power-law histories one row each and one marker row; 218 cells and facts against the store, no failure.
- `compare_quanta.py $V` (after `quanta_roots.sh` in each tree): the tip's power-law root measured 2 histories, each with
  its wall's spectrum (`R8-power-n1-v1`, `R8-power-n2-v1`); the exponential roots' quanta files and PNGs byte-identical,
  PDFs identical but for `CreationDate`, the printed lines identical with `wall <x> s` masked; the base's root without
  `model` refused, exit 2, nothing written. STOP CONDITION HIT: False.
- The drivers' refusals (by the tests, §3.5): `--models inverse-power` on an exponential-only store,
  `PlotOptionNotStored("InversePowerModel", "--models", "inverse-power")`; an A stored only under `one-loop-measured-v1`
  read for the power law, `PlotOptionNotStored("alpha_value", "--A-values", "0.5 (one-loop-measured-pl-v1)")`; at parse,
  `--models inverse-power --n-values 3 --A-values 1`, exit 2, "--A-values needs one one-loop drain model for each
  potential of --models over the n of --n-values: the potential inverse-power has no one-loop drain law at n = 3:
  InversePowerModel (n=3) has no one-loop drain law: one_loop_drain_model is defined at n = 1 and 2 only (power-law-wall
  README §6.5 R7) (InversePowerModel(M=…,n=3,beta=1))". Each potential's A rows' drain model: `exponential` →
  `one-loop-measured-v1`, `inverse-power` → `one-loop-measured-pl-v1` (n = 1, 2).

**2.3 The 2026.11.0 analyses' readers:** the table of verification §5.8. Each of `fig6`, `fig6b`, `m_trend` and
`changes` reads committed copies of `histories.csv` (and `m_trend` of `bounces.csv`) by column name; an export of 06
gives each under `<label>/<model>/`, the column names unchanged and `model` unread.

The table, copied here from verification §5.8 by the orchestrator's review (prompt §5 asks the log to hold it):

| analysis | what it reads | an export of 06 gives it |
|---|---|---|
| `fig6/fig6.py` | `inputs/histories.csv` (a copy beside it), by `pd.read_csv` and column name: `failure_reasons`, `Yp_BBN`, `delta_Yp`, `delta_D_over_H`, `Delta_N_eff_today`, `parking_T_GeV`, `drain_model`, `M_Mp`, `T_deliver_GeV`, `beta` | yes: `<label>/<model>/histories.csv`, every column under its name; `model` is not read |
| `fig6b/fig6b.py` | `../fig6/inputs/histories.csv`; as fig6's, and `alpha`, `log_R_first_impact` | yes, as fig6 |
| `m-trend/m_trend.py` | `../fig6/inputs/histories.csv` (`alpha`, `beta`, `M_Mp`, `drain_model`, the shifts, `failure_reasons`, `Yp_BBN`); copies of `bounces.csv` at its own `inputs/bounces/beta=<β>/M=<M>eV_n=1/` (`delta_K_Mp4`) | yes: `histories.csv` as fig6; each `bounces.csv` at `<label>/<model>/<drain_model>/A=<A>/csv/beta=<β>/M=<M>eV_n=<n>/`, below the root as before |
| `changes/changes.py` | `../fig6/inputs/histories.csv` and 2026.10.0's copy, joined on `M_Mp`, `beta`, `drain_model`, `alpha`; `failure_reasons`, `Yp_BBN`, `T_deliver_GeV` and eleven observables by name | yes; its key has no `n` and no `model` (§6) |

**The gate Export holds.**

## 5. The deliberate-breakage record

Each made on the tree with a backup copy, the named module run, and the file restored from the backup
(`…/agent06/breakage.py`; after the suites, 12:47). Every restored line was found again afterwards.
1. **`main.py` looks up every A row under `one-loop-measured-v1`**: `test_models_main` 3 FAILs (the power law alone, both
   potentials in each order).
2. **`export_root` without the potential's directory**: `test_models_export` 3 FAILs (the root of each potential; below
   the root).
3. **`quanta_measures` takes the exponential's wall for every row**: `test_quanta_models` 4 FAILs (n = 1 and 2 in one
   root; the row with no wall; the check by row; the figure tools' models).
4. **The parser without `models_problem`**: `test_models_option` 5 FAILs (the four refusals at parse; through
   `parser.error`).
5. **`stored_models` without the `--models` refusal**: `test_models_plot_inputs` 1 FAIL (a potential not stored).

## 6. Observations not acted on

- **`plot_quanta_grid` groups by (drain model, A) only**, and refuses a figure with two rows in one (β, M) cell. A
  power-law root with n = 1 and n = 2 at the same (β, M), as the science run's grid will have, would have every figure
  refused. `test_quanta_models` draws its two n at two β for this reason. Opened as board §3
  `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`; not 06's to change (prompt §1.8 asks only for the wall and the
  footer).
- **`changes/changes.py` of 2026.11.0 keys on (M_Mp, beta, drain_model, alpha)**, without `n` or `model`. A comparison of
  a power-law export with n = 1 and 2 would join two rows per key. A standard analysis's own script; 08's handover may
  name it.
- **`REMOVED_OPTIONS["--potential-type"]`'s message** still says "there is one model, ExponentialModel" (prompt §1.1 keeps
  it unchanged); it now misdescribes the tree.
- **`--n-values` is one list for every potential** of a run: a two-potential run builds, and a two-potential plot run
  reads, every potential at every n; a power-law build at n = 1, 2 beside an exponential at n = 1 needs two runs (D1).
  07's commands should say which.
- **`tools/history_and_bbn.py`, `tools/surfing_scan.py` and `tools/bbn_from_store.py` keep their own single `--model`**
  and their own `MODEL_CLASSES` (prompt §1.1); `test_models_option` checks that they name the same values and classes
  as `model_options`.
- **`plot_inputs.exponential_models`** has no production caller now; it is kept with its tests (P49).
- **The unintended Ray session directory** of D4 is left under `/tmp/ray` for the user.

## 7. State handed to the next prompt

- **The option and the files.** `--models` (default `[exponential]`; YAML `models`); `exponential.yaml` names
  `models: [exponential]` and `inverse_power.yaml` `models: [inverse-power]`, with the same keys otherwise (16 shards,
  tolerances 1e-8, β 0.1–25 at 5 per unit, M = 0.5 M_P). `--n-values` is one list for every potential of a run.
- **The commands 07's rehearsal runs** (from the root of the tree that builds, with the isolated cluster,
  `--ray-address 127.0.0.1:6380`, through the wrapper; `<grid>` is 07's own `--M-values-Mp`/`--beta-values`):
  - the exponential alone, at the base and at the tip, each with its own tree's `exponential.yaml`:
    `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign power-law-wall --prompt 07 --register <slug>
    --purpose "…" -- --database var/datastores/power-law-wall-07-<slug>/store.db --config exponential.yaml <grid>
    --alpha-values 0.1 --A-values 1 --ray-address 127.0.0.1:6380` (at the base the file has no `models` key and the
    parser no `--models`; at the tip `--models exponential` is the default, so the command is the same);
  - the power law under both laws: the same with `--config inverse_power.yaml --n-values 1 2` (its A rows go under
    `one-loop-measured-pl-v1`; n = 3 with `--A-values` is refused at parse);
  - both together: `--models exponential inverse-power --n-values 1 …`, one store, each potential's rows in its own
    table, its pipelines after the other's in the order given (both at every n of the list).
  - The plot runs, read-only: `plot_by_beta.py` and `plot_ScalarHistory.py` with the build's options, `--models` the
    build's potentials, `--no-plots`, `--output <dir>`; a store holding a potential at other n than another's needs one
    plot run per potential (D1).
- **The export roots.** `<output>/QCD_Cosmology/exponential/` and `<output>/QCD_Cosmology/inverse-power/`, each with its
  `histories.csv` (`model` after `n`), and below it, as before, `alpha=<α>/` and `<drain_model>/A=<A>/`, each with
  `csv/M=…eV_n=…/data.csv` and `csv/beta=…/M=…eV_n=…/{fields,bounces}.csv` (and `plots/` without `--no-plots`). The base's
  exponential root is `<output>/QCD_Cosmology/`.
- **What 07 compares.** The exponential store, base against tip, bitwise (the fingerprint; `main.py`'s order for one
  potential is the base's, §3.3); the exponential exports, base against tip, with `exponential/` and `model` accounted
  for (`compare_exports.py`'s rule); the power-law stores, interrupted and resumed, by value, and against 04's sample;
  `tools/quanta_measures.py` on each potential's root (each row's wall from its `model` and `n`).
- **Open for 08 / the science run.** `plot_quanta_grid` on a power-law root with n = 1 and 2 at one (β, M) refuses every
  figure (§6, board §3); the 2026.11.0 `changes.py` key has no `n`.
