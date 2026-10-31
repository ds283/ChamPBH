# Orchestrator — prompt 10, the driver and the readers

Read [`../README.md`](../README.md) first: §5 (the rules), §8.1 (R12, R22, R24, R26–R28), §8.2 (P10,
P26, P30, P32–P39), and 10's charter in §8.4. Then the prompt and log 09 §8. This note binds 10 only. The
conventions of notes 02–09 bind here as well, and are restated below: a tolerance is a number with its
reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`10-driver-and-readers.md`](../10-driver-and-readers.md) · **Gate:** "Driver" ·
**Closes:** nothing · **Opens:** only what the work finds · **Model:** Sonnet, as the prompt recommends ·
**Network:** none. Nothing is pushed.

**Gate**, checked at `fc0b4cb` on 2026-10-31 at 21:22 GMT, before this note was committed:
- `inelastic-reflection` is checked out in the main checkout, at `fc0b4cb`, the commit that adds prompt 10.
  That commit came after 09 landed (`3984ec5`) and was reviewed (`8987c67`), and after the bath-drift
  correction (`87d48e0`).
  - `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main
    checkout alone.
  - No code has changed since 09 landed: `git diff --stat 3984ec5 fc0b4cb` lists `.documents/OPEN_ISSUES.md`
    and files under `prompts/` only.
  - So the prompt's line numbers hold for this tree. Checked: `main.py` :154, :923, :999, :1135–:1157;
    `plot_by_beta.py` :126 (`ray.init`), :145, :164, :314, :496, :548, :658, :668, :697, :1020–:1031,
    :1040–:1108; `plot_ScalarHistory.py` :107 (`ray.init`), :1272, :1379, :1427–:1430, :1519, :1600–:1658;
    `extract_common.py` :99, :193, :520, :867, :946, :1137, :1150; `RunDrivers/registered_run.py` :672;
    `tools/inventory_report.py` :67, :117; `tools/history_and_bbn.py` :389, :405; `config/argument_parser.py`
    :298. Two are off by two: `plot_inputs.parameters` is at :110 (the class at :100), and
    `tools/bbn_from_store.py`'s `_FIND_QUERY` at :209.
  - `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- SciPy is 1.17.0.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 473 (98 + 375) / 121 / 115 / 64.
- The load averages were 9 / 9 / 7. Other workloads share the machine.

## 0. What makes this prompt unusual

**Nothing is computed differently, so the risk is in the tests that already read what changes. The prompt
lists the tests that read an option, a path, the scope or the inventory. It misses the tests that read the
history record and the figure labels, and two of its own sentences contradict each other.** For this note
the orchestrator read `config/argument_parser.py` (the validators, `--tuning-tolerance`, `--n-values`),
`main.py` (`run_pipeline`'s head, `execute` from the tolerances to the end), `plot_inputs.py`
(`PlotParameters`, `parameters`), `plot_by_beta.py` (the import-time block, `build_beta_plot`'s paths, the end
of `run_pipeline`, the driver block), `plot_ScalarHistory.py` (`write_csv_file`'s head,
`plot_ScalarHistory`, `run_pipeline`, the driver block), `extract_common.py` (both label helpers, the event
helpers, `CSV_COLUMNS`, `build_history_record`, `_save`, `plot_fixed_T`, `write_histories_csv`),
`describe_scope`, `tools/inventory_report.py`'s categories, extra fields and `history_extras`,
`tools/history_and_bbn.py`'s `main`, `tools/bbn_from_store.py`'s queries and `find_model`, the dimensionless
factory's match, `DissipationValues` and `dissipation_values`, and every test module that names
`build_history_record`, `CSV_COLUMNS`, either label helper, `plot_inputs.parameters`, `run_pipeline` or
`alpha_value`. Here is what it found where the prompt is silent, loose or wrong.

- **`build_history_record` is called by two existing test modules that §1.10 does not list.**
  `ComputeTargets/tests/test_extraction.py` (five calls; `_synthetic_records` builds its histories from
  `_NoSamples`, a `SimpleNamespace` with no `dissipation`) and `ObjectFactories/tests/test_tuned_history_round_trip.py`
  (one call, on a read-back history). Prompt §1.5 makes the record read `alpha` and the two ΔN_eff "from the
  history's `dissipation` record".
  - Reading `scalar.dissipation` on `_NoSamples` raises `AttributeError`, so four tests of
    `test_extraction` fail.
  - A required `alpha` keyword fails all six calls.
  - A default `alpha=0.0` would hide a driver that forgot to pass it. A failed history has no `scalar`, so α
    cannot be read from it either: the caller must give it.
  - So the prompt's §1.5 cannot pass without an edit §1.10 does not allow, and §5 makes that a stop. Ruling 1.
- **`add_ScalarHistory_labels` is called by `test_reflection_reporting`** (:93) with a stand-in model that has
  no `alpha`. Note 09 froze that module, and §1.4's "Titles" would read α there. Ruling 2.
- **`histories.csv` and figure 2 are written inside `plot_by_beta.py`'s `run_pipeline`** (:1015–:1031), not
  by the driver block. R26 makes `histories.csv` span α, so with one `run_pipeline` call per α it would be
  overwritten by each α. §1.4's "gathers … after the last α" needs `run_pipeline` to return its records. That
  touches `run_pipeline`, and §5 lists `run_pipeline` among the stops, though only for its work items, batches
  and stage order. Ruling 4.
- **The plot scripts start Ray at import, before the pool** (`plot_by_beta.py` :126, `plot_ScalarHistory.py`
  :107). An α out of [0, 1) is refused at parse, before Ray. An α in range that the store does not hold is
  `PlotOptionNotStored`, which needs the pool, and so Ray, exactly as an unstored tolerance does today. §5's
  "would need Ray to refuse an α" read literally is then always true. §3.2's last bullet expects this. Ruling 6.
- **§1.8 contradicts itself, and the gate.** "The `history` line gains `alpha=<α:g>`" and "At α = 0 every
  existing line is byte-identical to today's" cannot both hold. The gate (§4.1) says "every α = 0 output of
  the tools byte-identical". Also, the `history` and `bbn` lines carry `wall=` and `PRyM_time=`, which are
  never byte-identical between two runs, so "byte for byte" needs a stated mask. Ruling 7.
- **`find_model` is called positionally** (`tools/bbn_from_store.py` :781: `find_model(stem, args.beta,
  args.M_Mp, args.phi_init_Mp, units)`). An `alpha` parameter placed before `units` would shift `units`.
  Ruling 8.
- **`f"{-0.0:.5g}"` is `'-0'`.** `0.0 <= -0.0` holds, so a validator written as the range check accepts
  `-0`. The path would be `alpha=-0/`, beside `alpha=0/`, for one row. `nan` fails `0.0 <= x`, so the range
  check refuses it without a special case. Ruling 9.
- **The two ln T_J fields of `dissipation_values` are in the cosmology's units** (Planck), not GeV. The
  store converts them; §1.8's line would print them raw, beside a `bounce` line that prints T_J in MeV.
  `:.6g` does not round-trip a float, so "parses back to `dissipation_values(data)`" needs a precise meaning.
  Ruling 7.
- **`parking_N` and `release_N` are on the samples' own N** (`SampleValues.raw_N` is the integrator's N, the
  same N as `Reflection.N`; `ScalarHistory.py` :2739). In e-folds the lines sit at those N exactly.
  `plot_ScalarHistory.py` always calls its figures with `x_coord="efolds"`. Ruling 5.
- **What does not need a ruling, checked:**
  - `--n-values`' test (`test_driver_options` (b)) is the precedent for the option, and still passes.
  - `test_driver_options` (d) reads `main.py`'s `itertools.product`, `m.compute`, the tuning-tolerance lookup
    and `warn_super_planckian` by `ast`. A loop over α inside the loop over cosmologies leaves all four.
  - Nothing parses `>> RUNNING PIPELINE FOR MODEL`. The wrapper's stage tracker takes any `>> ` banner as the
    stage, so the stage gains α, and `test_registered_run_streams` feeds its own line.
  - The inventory's class count is unchanged (16): `alpha_value` moves from "Other classes" to a category.
  - The dimensionless factory matches 0 absolutely to `DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION` (10⁻⁷) and
    anything else relatively to `DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION` (10⁻⁵), both in
    `config/defaults.py`.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The history record takes α from the caller, and the two tests that build records follow.**
   - `build_history_record` gains `alpha: float` as a **required** keyword, after `phi_init_Mp`. It is the
     α row's value, given by the caller for every record, a failure's included.
   - The record's `Delta_N_eff_today` and `Delta_N_eff_1MeV` are `scalar.dissipation`'s fields of those
     names. Each is NaN where `scalar` is None, `scalar.dissipation` is None, or the field is None, as the
     record's other missing values are.
   - `CSV_COLUMNS` gains `alpha` after `phi_init_Mp`, and `Delta_N_eff_today`, `Delta_N_eff_1MeV` after
     `rho_ratio_70keV`. `write_histories_csv` sorts by `(alpha, M_Mp, n, beta)`, and its docstring says so.
   - `test_extraction.py`: each `build_history_record(...)` call gains `alpha=0.0`, and `_synthetic_records`'
     history stand-in gains `dissipation=None`. Nothing else.
   - `test_tuned_history_round_trip.py`: its one call gains `alpha=0.0`, the α of the history it stores.
     Nothing else.
   - These join §1.10's list of allowed edits. The log lists each, with its reason.

   *STRUCTURALLY REQUIRED* (§0, first point: prompt §1.5 against §1.10 and §5).
2. **α on the titles is an optional keyword of each label helper.**
   - `add_ScalarHistory_labels(fig, model, model_label, alpha=None)` and
     `add_beta_summary_labels(fig, model_label, group_model, alpha=None)`. When `alpha` is given, the line
     that names the cosmology reads `Cosmology: <label>, α = <α:g>`. When it is None, the line is
     unchanged.
   - Both plot scripts pass `alpha=` at every call. A new test shows it by `ast`.
   - `test_reflection_reporting` passes unedited.

   *STRUCTURALLY REQUIRED* (§0, second point).
3. **The path's α is the row's value**, never the float the user typed.
   - `extract_common.alpha_path(base, alpha) -> Path` returns `Path(base) / f"alpha={alpha:.5g}"`. Both plot
     scripts build every per-α path through it.
   - `plot_by_beta.py` passes `alpha.value` of the row it is running. `plot_ScalarHistory.py` passes
     `model.alpha.value` of the history it is drawing. That task is a `@ray.remote` function and needs no new
     argument.
   - The title's α (ruling 2) is the same value.

   *IMPLEMENTATION CHOICE*, ruled here.
4. **`plot_by_beta.py`'s `run_pipeline` returns its records.**
   - It takes the α row, as now, and passes it to `build_beta_plot`, which gains an `alpha` parameter for the
     paths and titles of ruling 3.
   - It writes figure 2 (`M_convergence.pdf`) under `alpha_path(...)/plots/`, as now but one level down.
   - It no longer writes `histories.csv`. It returns its gathered records, `all_records`, a list, empty if
     nothing was read.
   - The driver block loops over the α rows inside its loop over cosmologies. It concatenates the records of
     every α. After the last α, it writes `histories.csv` at `<output>/<label>/`, and one `Delta_N_eff.pdf` per
     (M, n) group at `<output>/<label>/plots/M=…eV_n=…/`, both per cosmology label.
   - Its work items, its `RayWorkPool` parameters and the order of its stages are unchanged. This is not
     §5's stop.

   *STRUCTURALLY REQUIRED* (§0, third point).
5. **`plot_dark_radiation`'s lines and axes.**
   - It takes `x_coord` as the other figures do, and reads x through `extract_common.get_x_coord`.
   - In `"efolds"` each vertical line is at `parking_N` or `release_N` exactly.
   - In `"redshift"` each line is at the x of the first sample whose `raw_N` is at or after that N, as
     `_find_T_event_times` places its events at a sample.
   - A sample with ratio 0.0 is left out of both panels (log axes). So is a sample with a non-positive Δg*,
     which can only be one with ratio 0.0.
   - The figure carries `add_ScalarHistory_labels(..., alpha=...)` and the temperature labels the other
     history figures carry (`add_redshift_xaxis_labels`).
   - `plot_ScalarHistory` calls it after `BBN_era_NP_plot`, with `plot_path / "dark_radiation.pdf"`.
   - In `plot_Delta_N_eff` a value that is not positive is left out of its log panel, as a NULL is, and one
     printed line says how many were left out of each panel.
   - Test 7 asserts the line positions in `"efolds"`, and one position in `"redshift"`.

   *IMPLEMENTATION CHOICE*, ruled here.
6. **"Need Ray to refuse an α" (§5) means an α out of [0, 1).**
   - That α is refused by `alpha_fraction` at parse, exit 2, before `ray.init`, in every driver and in the
     wrapper. The test shows it for `main.py`'s parser, which the plot scripts and the wrapper share.
   - An α in range that the store does not hold is `PlotOptionNotStored` after `ray.init`, as every other
     unstored option is today. It exits 2 before any figure. That is not a stop.
   - §3.2's last bullet takes its fallback: `plot_inputs` is tested on a temporary store, and the drivers'
     handler by `ast` (`test_plot_inputs`' `test_the_setup_is_plot_inputs` already asserts it). The log says
     so.

   *STRUCTURALLY REQUIRED* (§0, fourth point).
7. **`tools/history_and_bbn.py`'s lines.**
   - **The gate wins over §1.8's "gains".** `alpha=<α:g>` is printed on the `history` line only at α > 0,
     just before `wall=`. At α = 0 no line changes.
   - **The `dissipation` line**, at α > 0 only, is `dissipation <label>: ` followed by `name=value` for every
     field of `dissipation_values(data)`, in `DissipationValues._fields` order, space-separated.
     - Each value is formatted `:.6g`, and None prints as `None`.
     - The two ln T_J fields are printed converted as the store converts them (`− log(units.GeV)`), under the
       names `parking_log_T_Jordan_GeV` and `release_log_T_Jordan_GeV`.
   - **Test 10's "parses back":** each printed value equals `float(f"{x:.6g}")` of the field, converted for
     the two ln T_J. The comparison is exact on that float, and `None` is `None`.
   - **"Byte for byte" (test 10, §2.1)** means byte for byte after the numbers of `wall=… s` and
     `PRyM_time=… s` are replaced by a placeholder (`re.sub(r"(wall|PRyM_time)=[0-9.]+ s", r"\1=# s", …)`).
     Nothing else is masked. The pinned lines in test 10 are taken with the tool's existing BBN stub, so
     they carry no `PRyM_time`. §2.1's comparison runs the real solve.

   *STRUCTURALLY REQUIRED* (§0, fifth and eighth points).
8. **`tools/bbn_from_store.py`'s α.**
   - `find_model` gains `alpha: float = 0.0` as a **keyword-only** parameter at the end (`*, alpha=0.0`).
     Every existing call is unchanged, and `main` passes `alpha=args.alpha`.
   - The query matches α as the factory does: `abs(a.value - ?) < DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION`
     at α = 0, and `abs((a.value - ?) / ?) < DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION` otherwise.
     Both constants are imported from `config.defaults`, not written as literals.
   - At α = 0 every printed line and every `LookupError` message is unchanged. At α > 0 they gain `alpha=<α:g>`
     after `phi=`.
   - `--alpha` uses `alpha_fraction`.

   *STRUCTURALLY REQUIRED* (§0, sixth point), and *IMPLEMENTATION CHOICE* for the message.
9. **The option's values.**
   - `alpha_fraction(text)` returns `float(text) + 0.0`, so that `-0` becomes `0.0`, and refuses anything
     that fails `0.0 <= value < 1.0`. That refuses `nan`, `inf` and `1`. The message names the range.
   - `config/argument_parser.py` gains `distinct_values(values) -> list`. It removes exact duplicates and keeps
     the order of first appearance. `main.py` and both plot scripts call it once on `args.alpha_values` and
     use only its result.
   - `main.py` prints `   -- populated alpha grid with N value(s) = [ … ]` beside the β and M grids, in their
     format. The plot scripts print one `@@` line with the list.
   - The scope (P37) shows `--alpha-values` as given, duplicates included, since it describes the arguments,
     as it does β's.
   - Test 1 gains `-0`, and asserts `alpha=0` for it in `alpha_path`.

   *IMPLEMENTATION CHOICE*, ruled here, from §0's seventh point.
10. **`main.py`.**
    - The α rows are fetched in the tolerances' `ray.get`, one `pool.object_get("alpha_value", value=a)` per
      distinct α, in order.
    - `warn_super_planckian` runs once per cosmology, before the α loop, as now.
    - `run_pipeline`'s signature is unchanged. Only its header line gains `, alpha = <α:g>`.
    - Test 2's "no literal `value=0.0` lookup of `alpha_value`" covers `main.py`, both plot scripts and
      `plot_inputs.py`.
11. **The tests' fixtures.**
    - Test 7 uses 09's fixture (`ComputeTargets/tests/dissipation_histories.py`) and
      `test_dissipation_store`'s store harness. It does not copy them.
    - The bitwise check of Δg* reads `ax.lines[i].get_ydata()` and compares with
      `[v.gstar_rho * v.rho_chi_over_rho_rad for v in values if v.rho_chi_over_rho_rad > 0.0]` by `==` on
      each element.
12. **No file but the prompt's §6 list and ruling 1's two test modules changes.** In particular these are
    byte-identical:
    - every golden module and golden JSON;
    - every module under `ComputeTargets/` except `tests/`, and `ObjectFactories/` except `tests/`;
    - `test_reflection_reporting`, `test_dissipation_store`, `test_adiabatic_dark_radiation`,
      `test_bbn_sees_dark_radiation`, `dissipation_histories.py`, `test_driver_options`,
      `test_registered_run_streams`;
    - the surfing tools and their tests;
    - `CLAUDE.md`.
13. **Timing and the suites.**
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output to a file
      in your own scratch directory, grep the verdict from it, and record `uptime` beside each.
    - The `test_[g-z]` half took 432–452 s in 09. Run it in the background and wait for its notification
      without polling.
    - A failure is re-run alone before it is called real.
    - §2.3's parse times are the minimum of five runs of each `--help`, with `uptime` beside them.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. Test 10's pinned lines, and §2.1's α = 0 outputs, taken at the dispatch commit, in a temporary worktree
     of this note's commit. Remove the worktree once both are taken.
  3. The option, `main.py`, `plot_inputs.py`, the scope.
  4. `extract_common.py`: the record, the CSV, `alpha_path`, the label helpers, the two figures.
  5. The plot scripts.
  6. The inventory and the two tools.
  7. The tests of prompt §1.10 and ruling 1's edits.
  8. The breakages B1–B8. Then the measurement of §2.
  9. The suites, `black --check`, the `--help`s and `main.py --inventory`.
  10. The records: the log, `.documents/`, the board, and `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`, not
  zsh. None is committed.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file goes
  under the repository. The temporary store for `main.py --inventory` is built there by a test harness and
  deleted afterwards. The §2.2 PDFs stay there; name them in the log.
- **Stores and Ray.** No store but the tests' temporary ones and the inventory's scratch one. No Ray.
  Nothing is written to `var/`. No golden is written or regenerated.
- **Stop conditions are real.** They are:
  - the prompt's §5, with ruling 1's edits allowed, ruling 4's change to `run_pipeline` allowed, and ruling 6
    reading its Ray condition;
  - an existing test failing for a reason this note does not name.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `fc0b4cb`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The callers.** `grep -rn` over the repository's `.py` files, outside `venv/` and `thirdparty/`, for
   `build_history_record`, `CSV_COLUMNS`, `write_histories_csv`, `histories.csv`, `add_beta_summary_labels`,
   `add_ScalarHistory_labels`, `plot_inputs.parameters`, `RUNNING PIPELINE` and `alpha_value`. Each test module
   found was read where it calls or pins them. That is how the first two points of §0 were found.
3. **The edge cases** `f"{-0.0:.5g}"`, `-0.0 + 0.0` and `nan >= 0.0`, in the venv's Python.
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout. Give it:
- the prompt, the README, the board, log 09;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 12. Its log is
  `logs/10-driver-and-readers.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store** but the temporary ones.
- **Stop and ask** on any of the prompt's §5 conditions (as amended by rulings 1, 4 and 6) or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the option's parse and refusals, with the messages;
  - the paths as built: one example of each product at α = 0 and α = 0.1;
  - every existing test edited, with its edit and its reason;
  - the new tests by name, each with what it asserts;
  - the tables of §2: the byte comparisons, the figures' y-ranges and lines, the parse times;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.**
   - `git show --stat <agent>` lists exactly ruling 12's files.
   - Its parent is this note's commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
2. **α = 0 and the frozen tests.**
   - Ruling 12's modules are byte-identical (`git diff --stat` on them is empty).
   - `git diff` on every existing test module is §1.10's class or ruling 1's, and nothing else.
3. **The code** against prompt §1 and rulings 1–10, line by line:
   - the option, its validator, `distinct_values`, the help text and the YAML key;
   - `main.py`'s fetch, loop and header;
   - `plot_inputs`' `alphas`, its miss and its order;
   - every per-α path through `alpha_path`, from the row's value, and the products that span α;
   - `run_pipeline`'s return in `plot_by_beta.py`, and that its work items are unchanged;
   - both figures, the CSV columns and their sort, the label keywords;
   - the scope, the inventory's category and extra field;
   - both tools' `--alpha`, the `dissipation` line, the α = 0 lines, the query's match.
4. **An independent probe** of my own (session scratchpad, not committed):
   - parse `--alpha-values` from the command line and from a YAML file, with the refusals;
   - store M = 0.5, β = 2 at α = 0 and 0.1 on a temporary store, and run `bbn_from_store.find_model` at both α
     and at the default. Run `plot_inputs.parameters` with both α and with an unstored one;
   - draw both figures from that store, and read the axes back against my own Δg* and ΔN_eff;
   - `tools/history_and_bbn.py --fixed-normalization …` at α = 0 at the dispatch commit and the agent's
     commit, masked as ruling 7 says, byte for byte. Use a temporary worktree, and remove it.
5. **The breakages.** Replay each of B1–B8 with `git apply` in `bash`. Each must fail the tests the log
   names, and is then reverted.
6. **The figures.** Look at the §2.2 PDFs the log names: the curves, the log axes, the lines at parking and
   release, the y-ranges against log 09 §5.2.
7. **The measurement and the records.**
   - **§2:** the byte comparisons, the figures and the parse times are all there.
   - **The log:** it is in §5.1's form, with the edited tests, the breakages, and every deviation classed,
     §0's rulings among them, and "State handed to the next prompt" as prompt §4.3 lists it.
   - **`.documents/`:** the notes are dated and additive only.
   - **The board:** row 10, the gate Driver, §5.12, and anything opened on §3 and in
     `.documents/OPEN_ISSUES.md`.
8. **The suites** at `<agent>`, run serially:
   - the counts the log gives, each at least the baselines;
   - each change explained by the new tests;
   - `black --check` on the changed `.py` files;
   - the six `--help`s exit 0, and show `--alpha-values` or `--alpha`.
9. **`main.py --inventory`** on a temporary store holding two α: `alpha_value` in its category, `alpha=` per
   history line.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it appears;
- the board's row 10 landed, the gate Driver held, and this note added to the list of orchestrator notes,
  marked "used for 10";
- under `### 5.12`, an *Orchestrator review of prompt 10* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not residue
is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Driver;
- the counts;
- rulings 1, 4 and 7, and why;
- where each product now lands, and the two figures;
- what does not change;
- anything opened or closed;
- that 11 is written next, from 10's "State handed to the next prompt" (README §2.1).
