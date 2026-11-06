# Prompt 04 — drivers and exports

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-06, on branch `one-loop-drain` in the main checkout, after 03 landed and was reviewed (`fe4c917`,
`50e93c7`). It is written from:
- README §8.4 (row 04), §8.5 (the Export gate) and §8.12 (the review before this prompt, and P33–P40);
- 03's log, §6 ("Observations not acted on") and §7 ("State handed to the next prompt"), and the board's review of 03;
- `inelastic-reflection` prompt 10, which gave the drivers `--alpha-values`, and its tests, which are this prompt's
  precedent;
- the tree's code at the commit that adds this prompt.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Outputs go under `var/one-loop-drain/04/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 622 = 190 + 432, 149, 156, 68), all passing.

If any of these fails, stop and ask.

**Closes:** nothing. **Narrows:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Sonnet**, as README §8.4 plans. Nothing of the integration or the schema moves: the work is the
drivers' options, three exports and four tools, on records 03 stored. The care it needs is in §2.2's comparison
against the base code.

**Read first:**
1. [`README.md`](README.md) §8.12 whole; §8.5's Export row; §8.10, decision 1 (the key).
2. [`logs/03-the-store.md`](logs/03-the-store.md) §1.1, §1.2 (the columns as built), §6 and §7.
3. `CosmologyConcepts/drain_model.py` whole: `DRAIN_MODEL_FIXED`, `DRAIN_MODEL_ONE_LOOP`, `law_of`, `check_drain_model`.
4. `config/argument_parser.py`: `alpha_fraction`, `distinct_values` (:112–:140), `--alpha-values` (:221–:229).
5. `main.py`: the alpha rows (:995–:1007), the grid lines (:1135–:1137), the loop over rows (:1156–:1175), and
   `run_pipeline`'s header line (:173).
6. `plot_inputs.py` whole, `parameters` most of all.
7. `plot_by_beta.py`: `build_beta_plot` (:151–:200), `run_pipeline` (:710–:1050), `write_alpha_summaries` (:1051), and
   the driver block (:1090–:1159).
8. `plot_ScalarHistory.py`: `write_csv_file` (:1278–:1388), `plot_ScalarHistory` (:1391–:1535), and the driver block
   (:1630–:1684).
9. `extract_common.py`: `_cosmology_line`, `alpha_path` (:99–:113), the label functions (:115, :212),
   `DISSIPATION_CSV_COLUMNS` and `CSV_COLUMNS` with their comment (:527–:590), `build_history_record` (:911–:1000),
   `write_histories_csv` (:1214–:1235), and `plot_Delta_N_eff` (:1252–:1297).
10. `ComputeTargets/ScalarHistory.py`: `DrainHistoryValues` (:4913–:4930), `BounceRow`, `ScalarHistory.drain_history`
    and `.bounces` (:6296–:6330), and `ScalarHistoryValue.log_m_eff` and `.log_m_tracker` (:6882–:6892).
11. `ObjectFactories/ScalarHistory.py`: `to_column`, `from_column`, `BOUNCE_COLUMNS`, `MASS_COLUMNS` and
    `bounce_row_columns` (:145–:220).
12. The tools:
    - `tools/bbn_from_store.py`: `_FIND_QUERY` and `_find_query` (:210–:270), `find_model` (:297), the `--alpha`
      option (:754);
    - `tools/outward_step_scan.py`: the store reader (:830–:880) and the parser (:1290–:1325);
    - `tools/inventory_report.py`;
    - `RunDrivers/registered_run.py`, `describe_scope` (:671–:705).
13. The tests that are this prompt's precedent:
    - `ComputeTargets/tests/`: `test_alpha_option.py`, `test_alpha_plot_inputs.py`, `test_plot_inputs.py`,
      `test_extraction.py`, `test_driver_options.py`;
    - `ObjectFactories/tests/`: `test_alpha_figures.py`, and `test_drain_store.py` (`_DrainStoreCase`,
      `one_loop_payload`);
    - `tools/tests/`: `test_inventory_alpha.py`, `test_inventory_report.py`, `test_outward_step_scan.py`,
      `test_history_and_bbn_alpha.py`;
    - `RunDrivers/tests/test_scope_alpha.py`.

---

## 0. Where this prompt sits

03 stored the one-loop law, but nothing can yet ask for it: every driver looks up `alpha_value` with no model, which is
`fixed`, and no export carries what 03 stored. This prompt gives the drivers the option, writes the exports that 05's
derivation tool will read, and makes the tools that read `alpha_value` by raw SQL tell the two models apart.

It changes no value of the integration, no schema and no label: `FINGERPRINT_FORMAT` stays 7 and `VERSION_LABEL`
2026.10.0. 05 reads only this prompt's export format (README §8.4), so §5's handover states it exactly.

## 1. What is wanted

### 1.1 The option (P33, P34)

- **`--A-values`** in `config/argument_parser.py`, beside `--alpha-values`:
  - one or more values, `nargs="+"`, with no default, so that a run without it is today's run;
  - each value is A of the one-loop law: finite and > 0, with no upper bound;
  - each is checked at parse by an argparse type that calls `check_drain_model(DRAIN_MODEL_ONE_LOOP, value, …)`, so
    that the rules are stated once. A refused value is exit 2, naming the option, before any Ray, as
    `alpha_fraction` refuses;
  - the help says what A is, that it names the code's current one-loop model, and that it is a key of every history.
- **`--alpha-values` is unchanged:** the same type, default [0] and help. A run given only `--A-values` therefore also
  builds, or reads, α = 0. Say so in the help of `--A-values`.
- **The rows.** Each driver applies `distinct_values` once to each list. It then has one `alpha_value` row per value:
  - the fixed rows first, in the order of `--alpha-values`, looked up as today, with no model named;
  - then the one-loop rows, in the order of `--A-values`, looked up with `drain_model=DRAIN_MODEL_ONE_LOOP`.
  The constant is imported, never typed as a string. Without `--A-values` the lookups are exactly today's.

### 1.2 `main.py`

- It runs one pipeline per row, fixed then one-loop, as today. `ScalarHistory.compute` already takes the law from the
  row (03), so nothing else in the pipeline changes.
- **The printed lines.**
  - `run_pipeline`'s header line names the row by P35's label.
  - The "populated alpha grid" line is unchanged.
  - A second line, "populated A grid … (<model>)", is printed only when `--A-values` is given.
  - A run without it prints exactly today's lines.
- `--inventory` is unchanged but for §1.8's labels.

### 1.3 The plot scripts and `plot_inputs.py`

- **`plot_inputs.parameters`** gains the A list. It looks up each A under `DRAIN_MODEL_ONE_LOOP`, after the α rows,
  and reads them in the order given.
  - An A with no row under that model is `PlotOptionNotStored`, naming `--A-values` and the value. That holds even
    when the same value is stored under `fixed`.
  - An α with no `fixed` row is refused as today, even when the value is stored under the one-loop model.
  - Choose how `PlotParameters` carries the rows (one tuple, fixed then one-loop, or two), and say which in the log.
- **Both scripts** pass both lists and loop over every row in that order. They also print the A rows' line, beside
  the alpha line, when there are any.
- **`plot_by_beta.py`.**
  - The records carry the row's model as well as its value.
  - `write_alpha_summaries` writes one histories.csv and the `Delta_N_eff.pdf` figures across every row of the run,
    both models, at `<label>/` as today.
- **`plot_ScalarHistory.py`** writes fields.csv (§1.6) for every history, and bounces.csv (§1.7) for each one-loop
  history.

### 1.4 The paths and the labels (P35)

- **The path helper.** `alpha_path(base, alpha)` becomes a helper that takes the `alpha_value` row (or its value and
  model).
  - Under `fixed` it gives `base / "alpha=<α:.5g>"`, as today.
  - Under any other model it gives `base / "<drain_model>" / "A=<A:.5g>"`.
- **The label helper.** One helper gives the label: `α = <α:g>` under `fixed`, as today, and
  `A = <A:g> (<drain_model>)` otherwise.
  - Every figure title, `_cosmology_line`, `run_pipeline`'s lines, and the legend of `plot_Delta_N_eff` use it.
  - Every per-row path in both scripts goes through the path helper, as the precedent's test (4) requires today.
- A fixed-law product's path and title are unchanged, character for character.

### 1.5 histories.csv (P36)

- **`drain_model`** is a new column directly after `alpha`. `alpha` keeps the key's value: α under `fixed`, A under the
  one-loop model. `build_history_record` takes the model from the caller, as it takes `alpha`, for every record, a
  failure's included.
- **`DRAIN_CSV_COLUMNS`** is the fields of `DrainHistoryValues`, in their order, under their stored names. It is a block
  after `DISSIPATION_CSV_COLUMNS` and before `failure_reasons`.
  - Read from `scalar.drain_history`, which is readable under `_do_not_populate`.
  - Blank where the record is None (`fixed`, α = 0, a failure, no `scalar`), and blank where a field is None.
  - An integer count is written as an integer.
- **The comment above `CSV_COLUMNS`** gains a sentence for both additions, dated and naming this prompt.
- **The sort** is by (`drain_model`, alpha, M, n, β). For a store holding only `fixed` rows, the rows come out in
  today's order.
- **`plot_Delta_N_eff`** draws one curve per (model, value) with value > 0, labelled by §1.4's helper, and orders the
  curves fixed first, then by value. A record at α = 0 is still never drawn.

### 1.6 fields.csv (P37)

- **The row-building** moves from `plot_ScalarHistory.write_csv_file` into a function of `extract_common.py`, so that a
  test can call it. Choose its signature and say which in the log.
  - The existing columns keep their names, their order, their arithmetic (character for character), the merge over
    store ids, the NaN row, and the sort.
  - `write_csv_file` calls the new function and writes the file as today.
- **Two columns** after `Delta_gstar`:
  - `log_m_eff_Mp` and `log_m_tracker_Mp`, each the stored column: `to_column` of the value with `MASS_COLUMNS`' kind;
  - blank where the value is None;
  - present under every law, as the masses are.

### 1.7 bounces.csv (P38)

- **A new export**, `bounces.csv`, in the same directory as the history's fields.csv.
  - It is written for every one-loop history, with a header only when `bounces` is empty, and never under `fixed`.
  - Its rows are those of `ScalarHistory.bounces`, in `ordinal` order.
  - Its columns are the stored columns of `BOUNCE_COLUMNS`, in the table's order, without `history_serial`. Each
    cell is `bounce_row_columns(row, units)`'s value for that column, so that no conversion is written twice.
  - Booleans (`resolved`, `weighted`) are written as 0 and 1, and None as a blank cell.
- **The writer** is a function of `extract_common.py`, beside §1.6's, and `plot_ScalarHistory` calls it.
- **No other column.** The history's keys come from the path and from histories.csv, as for fields.csv. If 05 cannot
  work from these columns (03's log §7 lists what it reads), stop and ask; do not add one.

### 1.8 The tools and the registered run (P39, P40)

- **`tools/bbn_from_store.py`** gains `--drain-law {fixed,one_loop}`, default `fixed`, with the meaning
  `tools/history_and_bbn.py`'s option has: `--alpha` carries α under `fixed` and A under `one_loop`.
  - The find query matches `drain_model` by `law_of`'s inverse: `fixed` → `DRAIN_MODEL_FIXED`, `one_loop` →
    `DRAIN_MODEL_ONE_LOOP`. Choose how to invert it, and keep the mapping in `drain_model.py` if you add one.
  - The value is checked by `check_drain_law` at parse.
  - The messages under `fixed` are unchanged (its `_alpha_text`). Under `one_loop` they name A and the model.
- **`tools/outward_step_scan.py`** reads only rows whose `drain_model` is `fixed`, since it rebuilds a history under
  the fixed law.
  - A store holding rows of another model gets one line counting the rows skipped. Choose where the line goes, and
    say which in the log.
  - Its `--alpha` options are unchanged.
- **A store whose `alpha_value` has no `drain_model` column** (format ≤ 6) holds only `fixed` rows. Both tools read it
  as today, and under `one_loop` `bbn_from_store.py` finds nothing there. This is the precedent of `_find_query`'s
  `has_alpha` branch.
- **`tools/inventory_report.py`** labels each `alpha_value` row by its model.
  - A `fixed` row reads as before 03: `0` in the values list, and `alpha=0` on a history's line.
  - A one-loop row reads `A=<A:.5g> (<drain_model>)` in both places.
  - A store holding only `fixed` rows then gives the report it gave before 03. Restore the test lines that 03 changed,
    and name each in the log.
- **`RunDrivers/registered_run.py`**: `describe_scope` adds `A <values>` after the α part only when `--A-values` is
  given. Every scope string the existing tests pin is unchanged.

### 1.9 Tests

No Ray and no persistent store. A temporary store goes through `_TempStoreCase` or `_DrainStoreCase`. Each test that
integrates states its cost in its docstring (P-2197 at A = 1 by `one_loop_payload` takes seconds).

- **The option** (`ComputeTargets/tests/`, instant):
  - with no `--A-values`, the parse is today's;
  - a given list is kept, and the YAML key `A-values` gives the same list;
  - 10 and 1000 are accepted;
  - 0, −0, −1, nan and inf are each refused, with exit 2 and the option named;
  - duplicates collapse;
  - the help says what A is and that α = 0 is still built.
- **The drivers, by `ast`**, as `test_alpha_option` (2) does:
  - `main.py` looks up one row per A with `drain_model=DRAIN_MODEL_ONE_LOOP`, by the name and not a string literal,
    after the α rows;
  - `run_pipeline` is called once per row;
  - both plot scripts pass both lists to `plot_inputs.parameters` and loop over every row;
  - no driver writes the string `"one-loop-measured-v1"`.
- **`plot_inputs.parameters`** on a temporary store with α = 0 and 0.5 under `fixed`, and A = 0.5 and 1 under the
  one-loop model:
  - both kinds of row are returned in the order of §1.1, each with its model;
  - an A stored only as α is refused, naming `--A-values`;
  - an α stored only as A is refused, naming `--alpha-values`;
  - the read-only pool writes no byte.
- **The helpers:** the path and the label under both models, and `alpha=0` for −0 as parsed. The fixed-law strings
  are today's, checked by literal.
- **histories.csv:**
  - the header holds `drain_model` after `alpha` and `DRAIN_CSV_COLUMNS` before `failure_reasons`;
  - a fixed record leaves every drain cell blank;
  - a one-loop record from a constructed `DrainHistoryValues` writes each field;
  - the sort with both models present;
  - the precedent's tests 5 and 6 still pass, changed only where a new column forces it.
- **`plot_Delta_N_eff`:** the same value under both models makes two curves with two labels, fixed first.
- **The exports of a stored history, read back from a temporary store:**
  - **A one-loop history (P-2197 at A = 1):**
    - histories.csv's drain cells equal the raw `ScalarHistory` columns;
    - fields.csv's two mass cells equal the raw `ScalarHistoryValue` columns, per sample, matched by redshift;
    - bounces.csv has `bounce_rows` rows in `ordinal` order, and every cell equals the raw `ScalarHistoryBounce`
      column.
    All of these are bitwise, read raw by SQL from the temporary store's file, as `test_dissipation_store` reads its
    rows.
  - **A fixed-law history at α > 0 and at α = 0:** the drain cells are blank, the mass cells equal the raw columns,
    and no bounces.csv is written.
  - **A one-loop history with no bounce row,** constructed if none is cheap: bounces.csv holds its header alone.
- **The tools:**
  - `bbn_from_store.py`'s find query on a store holding the same value under both models: each law finds its own
    history, and a format-6 imitation (no `drain_model` column) finds the fixed one;
  - `outward_step_scan.py` skips a one-loop row, with its count;
  - the inventory report's lines under both models, and for a store holding only `fixed` rows;
  - `describe_scope` with and without `--A-values`.
- **Existing tests.** Change only what a new column, helper or label requires, and name each change in the log. **No
  suite's count may fall.**

## 2. The Export gate (README §8.5, §8.12)

### 2.1 The new columns equal the stored record

§1.9's tests on stored histories pass. In addition, run §2.2's script on the one-loop histories of its store, and
compare every new cell of histories.csv, fields.csv and bounces.csv with the raw column:
- floats by their `float.hex`;
- integers, strings and Booleans as values;
- a blank cell against NULL.

Report the number of cells compared per file. A difference is a stop.

If a writer's formatting of a new float column does not round-trip, fix the formatting of the new columns only, and
report in "Observations not acted on" whether the existing columns round-trip. Do not change how an existing column is
written.

### 2.2 The existing columns are unchanged

**The store.** Build one store under `var/one-loop-drain/04/` through the stand-in cluster (never Ray), with:
- a fixed-law history at α = 0;
- one at α > 0 that reflects and drains (the reflecting setup of `test_injection`, or `dissipation_histories`'
  fixture);
- a failure row;
- P-2197 at A = 1;
- one more one-loop history with classical candidates, if one takes under a minute.

Reuse 03's payload builders (`ObjectFactories/tests/test_drain_store.py`, `.documents/one-loop-drain/store-03/`), and
name each history in the log.

**The two exports.** Export every history twice: with this prompt's code, and with the code at the commit that adds
this prompt, whose code is 03's reviewed code (`50e93c7`).
- The base code runs in a scratch worktree of that commit, under your subdirectory of the session scratchpad, removed
  when you are done.
- The schema has not moved, so both trees read the same store file.
- The base's fields.csv writer is a function in a script that parses its arguments on import. Run the base's own
  function unedited (for example extracted by `ast` from the base's file and executed), never a copy edited by hand.
  Say how in the log.

**The comparison.**
- **For each fixed-law history**, compare the existing columns of histories.csv and fields.csv: the projection onto
  the base's columns, in the base's order, written with the base's writer settings. They must be byte-identical. A
  difference is a stop.
- **For the one-loop histories**, report the same comparison. Every existing column should agree there too; where one
  does not, say why.
- **Paths and labels.** List each product's path and title under both trees. The fixed-law ones are equal, and the
  one-loop ones follow §1.4.

### 2.3 The drivers refuse a law that does not match the store

§1.9's `plot_inputs` tests pass. With them:
- the parse refusals of `--A-values`;
- `test_alpha_plot_inputs`' handler test, which still shows both scripts exiting 2 on `PlotOptionNotStored`.

## 3. What is not wanted

- **No value of the integration moves,** and no schema, factory, key, fingerprint or label changes. If the exports
  need one of these changed, stop and ask.
- **No Ray, no cluster and no run of `main.py`, `plot_by_beta.py` or `plot_ScalarHistory.py` as a program.** 06 runs
  the builds and 07 the figures. The drivers are checked by `ast` and their functions on temporary stores, as
  `inelastic-reflection` 10 checked them.
- **No derived measure** (05) and no new figure (07). `plot_Delta_N_eff` and the titles change only by §1.4 and §1.5.
- **No change to the tool route,** `tools/history_and_bbn.py`, beyond what a test of §1.9 forces.
- **The 2026.9.0 stores.** `outward_step_scan.py`'s `missing_in_store` (03's log §6) is not acted on. No migration
  (P32).
- No merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched.

## 4. Verification

- **The suites**, before the change and after, per `CLAUDE.md`: ComputeTargets in two halves, `tools/tests` alone.
  Record the counts, the times and `uptime`.
- **Where things go.** §2's scripts go in `.documents/one-loop-drain/export-04/`, with their text outputs in
  `export-04/out/`. The store and the exported files go under `var/one-loop-drain/04/`. Scratch, the base worktree
  included, goes in your subdirectory of the session scratchpad, never `/tmp`.
- **Reproducibility.** Every number of the verification section comes from a script in `export-04/` or a test, or
  from a file under `var/one-loop-drain/04/` that the section names.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- **the code:** `config/argument_parser.py`, `main.py`, `plot_inputs.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
  `extract_common.py`, `tools/bbn_from_store.py`, `tools/outward_step_scan.py`, `tools/inventory_report.py`, and
  `RunDrivers/registered_run.py`; `CosmologyConcepts/drain_model.py` only if §1.8's inverse mapping goes there; the
  new and changed tests;
- **the scripts:** `.documents/one-loop-drain/export-04/` and its `out/`;
- **the verification document:** `.documents/one-loop-drain-verification.md`, a new §4 holding §2's gate item by
  item. It is additive (invariant 6), and §1–§3 stay as they are;
- **a dated note** in `.documents/architecture-summary.md`: the option, the rows' order, the paths, and the three
  exports' new columns;
- **`CLAUDE.md`:**
  - the test counts;
  - in "The plot scripts", `--A-values` beside `--alpha-values`, and the one-loop path `<label>/<drain_model>/A=<A>/`;
- **the log** `logs/04-drivers-and-exports.md` (template: `dk-and-rpk-migration` README §5.1):
  - every deviation from this prompt, classified;
  - "Observations not acted on";
  - a "State handed to the next prompt" that gives 05 the export format exactly. That means, for each of
    histories.csv, fields.csv and bounces.csv: where it is written, which histories have it, every column 05 reads
    with its unit, how NULL and Booleans are written, and how a history's three files are joined. It also tells 06
    which options the rehearsal's command lines take;
- **the board:** row 04, its items, the Export gate, and **Last updated**. `.documents/OPEN_ISSUES.md` changes only if
  an issue is opened.

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the
orchestrator substitutes it.
