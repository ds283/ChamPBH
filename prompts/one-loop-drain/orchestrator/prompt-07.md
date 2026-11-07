# Orchestrator — prompt 07, the figure tools

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.4 (row 07), §8.5 (the gate Figures) and
§8.15 (P58–P65). Then the prompt. This note binds 07 only, and governs where it differs from the prompt (prompt,
"Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken under load, and
the suites run serially.

**The prompt:** [`07-figure-tools.md`](../07-figure-tools.md) · **Gate:** "Figures" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Sonnet, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-07 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work").

**Gate**, checked at `5bb6c40` on 2026-11-07 at 15:26 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `5bb6c40`, the commit that adds prompt 07.
  `git status --porcelain` is empty.
- `./venv/bin/python -m runregistrykit list`: 33 runs, all finished; none running, stale or unknown.
- `var/one-loop-drain/06/plot-reference/QCD_Cosmology/` (156 MB) and `var/one-loop-drain/05/exports/QCD_Cosmology/`
  (1.2 GB) exist, each with `histories.csv` and `quanta_histories.csv`, `quanta_histories-assoc0.csv` and
  `quanta_histories-assoc10.csv`. Neither `var/one-loop-drain/07/` nor `.documents/one-loop-drain/figures-07/` exists.
- The suites were not re-run for this note. 06's review left them at 26, 665 (219 + 446), 163, 191 and 72, all passing,
  and only `5bb6c40` (prompt 07, a document) has landed since. The agent measures them again first.
- **The load.** 11.91 / 9.38 / 6.00. The machine has 10 cores and 16 GB of memory, and it is shared. 39 GB free by `df`
  (the rest is purgeable; README §8.15).
- `PYTHONPATH` is unset in this session's shell. matplotlib is 3.10.8.

## 0. What the orchestrator found

For this note the orchestrator read: prompt 07 and README §8.15; 05's log §7; `tools/quanta_measures.py` (`suffix`,
`read_csv`, `history_dir`, `row_label`, `KEY_COLUMNS`, the column lists, `matter_excess`, `run`, `main`);
`extract_common.py` (`_cosmology_line`, `alpha_path`, `history_directory`, `reflection_count`,
`add_ScalarHistory_labels`); `plot_ScalarHistory.py`'s `plot_path` and `csv_path`; `tools/tests/quanta_exports.py`
whole; and the two export trees' histories.csv, quanta_histories csv files at three scales, and one history's four files.

Here is what it found where the prompt is silent, loose or wrong.

- **The trees are as the prompt says.**
  - 06's histories.csv has 54 rows: 36 `fixed`, of which 19 are marker rows; 9 one-loop marker rows; 9 drawn one-loop
    rows, 4 at A = 0.1 (one M, four β) and 5 at A = 1 (two M, five β). Its three quanta_histories files hold 9 rows
    each.
  - 05's has 230 rows, every one a drawn one-loop row: 27, 28, 29, 30, 29, 29, 29, 29 at A = 0.05 … 10. There are 18–19
    distinct β and 8–9 distinct M at each A. Its three quanta_histories files hold 230 rows each, each file naming one
    model, `U4-U17-v1`, and its own scale (`1.0`, `0.0`, `10.0`).
  - Neither tree has a failed one-loop row, and no drawn row lacks a quanta_histories row. The figures' crosses are
    therefore exercised by the tests alone.
  - No two drawn rows of one A share a (β, M) cell. The β and M values stay distinct under `%.5g` and `%.3g`.
- **The marker rows are mostly fixed rows.** 19 of 06's 28 marker rows have `drain_model` `fixed`. The prompt's four
  counts would count them twice unless an order is given (ruling 1).
- **The headers are those of 04's and 05's logs §7.** fields.csv has `raw_N`, `rho_chi_over_rho_rad`, `log_m_eff_Mp` and
  `log_m_tracker_Mp`. bounces.csv has `ordinal`, `source`, `candidate_index`, `N`, `N_turn`, `delta_K_Mp4`, `alpha_eff`,
  `K_before_Mp4`, `regime` and `log_q_Mp`. The two quanta files have the columns of 05's log §7.
- **Panel 1's frame is 05's.** `matter_excess` forms ln y = (N + ln m) − ln Q_g with N = `raw_N`. So ln(m a/M_P) =
  `log_m_*_Mp` + `raw_N` sits on the same axis as `log_q_Mp`, as the prompt says.
- **The ranges** (05's tree, c_a = 1). δ_stim runs from 2.2 × 10⁻⁷⁵ to 1.3 × 10⁻³, E_field_max from 2.7 × 10⁻⁴⁵ to
  5.4 × 10⁻²⁰, and backreaction_max from 1.4 × 10⁻⁵⁷ to 1.8 × 10³. None is blank or zero. `ln_n_g_max` runs from −163.5
  to 98.2, so exp(`ln_n_g`) neither overflows nor underflows to zero. `phase_slips_below_1` is 0 in 215 of 230.
  Panel (b)'s sum is positive in only 6–16 histories per A, so most of its cells are the zero grey. In 06's tree, panel (b)
  has one positive cell over both A. So a log panel can have no positive value or exactly one, and **the prompt gives
  no colour range for either** (ruling 4).
- **The selectors select a product.** `--A-values`, `--beta-values` and `--M-Mp-values` select every history that
  matches one value of each list. So §3's six histories, passed together, could select more than six (ruling 6).
- **The prompt names no expected spectrum model.** "A file naming another model" needs a reference (ruling 3).
- **The test helper needs no change.** `history_record` returns a plain dict. A test sets `failure_reasons`,
  `normalization`, the shares, `ell_min` and the margins on it before `write_histories_csv`. `candidate` takes `regime`,
  and `event` takes `source`. Marker rows, fixed rows, failed rows, blanks, zeros and all four regimes can all be built
  without editing `quanta_exports.py` (ruling 8).
- **`reflection_count`'s docstring** still says "elastic reflections". §1.4 changes the label only (ruling 8).

**Rulings.** Each is recorded in the log, with its class.
1. **The four counts, in order.** A histories.csv row is counted in the first of these that it meets, and only there:
   1. `fixed`: `drain_model` is `fixed`, marker or not;
   2. `marker`: `failure_reasons` is exactly `no ScalarHistory row in the store` and `normalization` is blank;
   3. `failed`: any other row with a blank `normalization`;
   4. `drawn`.

   On 06's tree that gives 36, 9, 0, 9. On 05's it gives 0, 0, 0, 230. `fixed` and `marker` are counts of the whole
   file, on the run's summary line. `drawn` and `failed` are counted per A, on each figure's line.

   *IMPLEMENTATION CHOICE*, ruled here (§0, second point).
2. **Values and cells.**
   - The grid tool's `--A-values` matches like the history tool's selectors: `math.isclose(rel_tol=1e-9)` against
     `float()` of the one-loop rows' `alpha` cells. A given value that matches no row exits 2.
   - The axes' categories are sorted by `float()` of the cell. β runs left to right, and M runs bottom to top, the
     least M at the bottom.
   - The join of histories.csv to quanta_histories.csv is on the five key cells as strings, as the prompt says.
   - If two drawn or failed rows of one A fall in one (β, M) cell, that A's figure is refused (exit 1). The printed
     line names both `row_label`s, and nothing is written for that A. Neither tree has such a cell.

   *IMPLEMENTATION CHOICE*, ruled here.
3. **The model check.**
   - **Expected values.** The expected `spectrum_model` is `ComputeTargets.quanta_spectrum.SPECTRUM_MODEL` of the tree's
     code. The expected `assoc_scale` is the scale asked for, compared as `float(cell) == scale`.
   - **Parsing.** `--assoc-scale` is parsed by `quanta_measures._scale`, so its refusals are 05's.
   - **The grid tool.** If `quanta_histories<suffix>.csv` is missing, unreadable, or fails the check, no figure is
     written and the exit is 1. The printed line gives the file and the reason.
   - **The history tool**, for a selected drawn history:
     - **Neither file.** If neither quanta file exists at the scale, the history was refused or failed by 05. It is
       skipped and counted as `no quanta files`, with its `row_label` printed. This is not a refusal.
     - **One file only,** or files without a quanta_histories row, or a file failing the check: the history is refused
       (exit 1) and nothing is written for it.
     - **quanta_histories fails.** If `quanta_histories<suffix>.csv` itself fails the check, every history is refused.

   *As the prompt says; made exact* (§0, sixth point).
4. **The colour scales, exactly.**
   - **The norms.**
     - A log₁₀ panel uses `matplotlib.colors.LogNorm` over that panel's finite positive values.
     - Panel (a) uses `Normalize(0, 1)`, and (d) uses `Normalize` over its finite values.
     - (e)–(g) use `Normalize(-m, m)` with a diverging colour map, with m the largest |value| of that panel. They do not
       use `TwoSlopeNorm`, which is not symmetric.
     - The tests assert the norm's type and its limits.
   - **Degenerate ranges.**
     - **No value in range.** The colour bar is drawn without ticks, and the panel's title says "(no positive value)"
       on a log₁₀ panel, or "(no value)". The special cells are still drawn.
     - **One value only.** The range is widened: to [v/√10, v·√10] on log₁₀, to [v − 0.5, v + 0.5] on linear, and m = 0
       becomes m = 1.
     - The log says which panels of which figures met either case.
   - **A negative value on a log₁₀ panel** is a defect of the input: every quantity drawn there is non-negative by
     construction. It is the prompt's stop "a file is not as 05's or 04's log §7 says". The same holds on the history
     figure.
   - The colour maps are the agent's choice, recorded in the log. The regimes' colours are one fixed list in
     `tools/quanta_figures.py`.

   *As the prompt says; made exact* (§0, fifth point).
5. **The history figure, exactly.**
   - Every log₁₀ axis is `set_yscale("log")` on the values. n_g is `exp(ln_n_g)`. An `ln_n_g` above 709 or below −745
     would overflow or vanish. That is a stop, and none is expected (§0, fifth point).
   - **Panel 2's undrawn candidates** are counted by cause:
     - ΔK = 0 (the fraction);
     - `alpha_eff` zero or blank;
     - `K_before_Mp4` blank.
   - **K_final.** It is the last row of quanta_samples.csv, which must be in ascending `raw_N`, or the history is
     refused. If K_final is 0, the K curve is not drawn, and the footer says so.
   - **Sharing N.** "The six panels share N" means one shared x-axis over the six axes, panel 3's twin included. The
     test checks the shared group.
   - Each y-axis label gives the quantity and its unit ("dimensionless", "rad", "e-folds" or "M_P").

   *IMPLEMENTATION CHOICE*, ruled here.
6. **§3's six histories.**
   - **Picking.** A script picks them, the check script or one beside it in `.documents/one-loop-drain/figures-07/`.
     The pick is the largest of each named column of quanta_histories.csv (c_a = 1), compared as floats. Ties go to the
     row that comes first in the file. Duplicates are merged.
   - **Drawing.** The history tool is run **once per picked history**, each run given that history's `alpha`, `beta`
     and `M_Mp` cells. Each run's line must report exactly one figure written.
   - The log lists the picks with the column or columns each was picked for.

   *STRUCTURALLY REQUIRED* (§0, seventh point). One call with all six histories' values could select their product.
7. **Committed names.**
   - A grid page is `A=<A:.5g>_quanta_grid.png`.
   - A history is `A=<A:.5g>_` + `history_directory(β, M_eV, n)` with `/` replaced by `_`, then `_quanta_history.png`.
     The prompt's example is formed this way.
   - The c_a = 0 and 10 grids of §3 stay under `var/`. Prompt §3's commit list does not include them.

   *As the prompt says; made exact.*
8. **Files and helpers.**
   - **Use as they are:**
     - `tools/quanta_measures.py`, whose `read_csv`, `history_dir`, `row_label`, `suffix`, `KEY_COLUMNS` and `_scale`
       are imported;
     - `tools/tests/quanta_exports.py`;
     - `tools/tests/test_quanta_measures.py`.
     Builders the new tests need go in the new test modules. Editing any of the three files is prompt §7's stop.
   - **`reflection_count`'s docstring** ("elastic reflections") is left. It goes in "Observations not acted on", and is
     not fixed.
   - **The README** is not edited.

   *STRUCTURALLY REQUIRED*; as the prompt says.
9. **Exit codes and arguments.** argparse's own exit is mapped as 05's `main` maps it: `2 if e.code else 0`. A root that
   is not a directory, or whose histories.csv cannot be read, exits 2. The two tools never call `sys.exit` but from
   `__main__`, so the tests call `main(argv)`.

   *As the prompt says (P58).*
10. **Timing and load.** Each tool's wall per figure, and its total, are recorded with the one-minute load at its start.
    The suites' times are recorded likewise. No run is repeated for load.

    *As the prompt says.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites, before anything else. They run serially: ComputeTargets in its two halves, with
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. `tools/quanta_figures.py`, the two tools and their tests; §1.4's label.
  3. The demonstration (prompt §3), the check script (§2), looking at every committed PNG.
  4. The suites again; the documents; the records; the commit.
- **Scratch** goes in `<scratch>` =
  `/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-cUz59V/6530d7c6-7563-4814-8fdd-8fe81bc215b8/scratchpad/agent-07`.
  Figures go under `var/one-loop-drain/07/`. Nothing goes in `/tmp`, and nothing is written under
  `var/one-loop-drain/0[1-6]*/`.
- **No store, no Ray, no cluster, no registered run** (prompt §4).
- **Stop conditions are real:**
  - prompt §7's, whole;
  - ruling 4's negative value on a log panel;
  - ruling 5's `ln_n_g` out of range;
  - a suite count that falls.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `5bb6c40`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout, in the background. Give it:
- the prompt, the README, the board and this note;
- `HEAD`, and the Gate as measured.

The agent reads this note and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with prompt §6's files. Its log is `logs/07-figure-tools.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - every command of §3 as run, with its exit, wall and printed summary line;
  - the gate Figures item by item, with the check script's output;
  - the committed PNGs with their sizes, and one line per figure on what it shows;
  - the degenerate colour ranges met (ruling 4) and the six picks (ruling 6);
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only prompt §6's files. Its parent is this note's commit, or a later commit that is
     not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - Outside the three new modules and their tests, the only code changes are §1.4's label and its two assertions.
     `quanta_measures.py`, `quanta_exports.py`, `test_quanta_measures.py`, the plot scripts, `PRyM/` and `.gitignore`
     are untouched. `CLAUDE.md` changes only in its counts.
   - Nothing under `var/` is tracked. No PDF is committed, and the committed PNGs are 05's eight grid pages and at most
     six histories, 8 MB or less together.
2. **The suites, re-run by me**, serially as the conventions say: the counts the log states, all passing, none fallen.
   `tools/tests` up by exactly the tests added, and the others unchanged.
3. **The code against the prompt and the rulings.**
   - The ten grid panels: their order, columns, files and norms.
   - The special cells: drawn and counted as §1.2 says.
   - The six history panels: their columns.
   - Ruling 1's order, ruling 3's checks, ruling 4's degenerate ranges, ruling 5's undrawn counts, and the `--output`
     mapping through the existing helpers, with no path formatted inline.
   - Every test reads artists, not just a file's existence, and the tests cover prompt §1.5's list.
4. **The demonstration, re-run by me.**
   - Both tools on 06's tree and the grid tool on 05's tree at c_a = 1, each with `--output` under my scratchpad.
   - The exits and printed counts are the agent's.
   - The PNGs are the same size as the agent's to within a few percent. They are not compared byte for byte: matplotlib
     stamps a date in the PDF, and none of this is pinned.
   - Neither input tree is written into, checked by a listing with mtimes taken before and after.
5. **The gate Figures, independently.** A script of my own: each figure's drawn count against quanta_histories.csv's rows
   at that A, and each history figure's generations against quanta_histories.csv's `generations`, both from the printed
   lines. The six picks, recomputed by ruling 6.
6. **The figures, by eye.** I open every committed PNG and compare it with the log's notes. Anything that reads wrongly
   and is the tool's is residue or a finding.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them. Its §7 gives 08 the command
     lines, exits, figures and costs.
   - The verification document's §1–§6 are unchanged, and §7 follows prompt §6.
   - The architecture summary has a dated note on the two tools.
   - The board: row 07, its items, the gate Figures in §2, an entry in §5, and **Last updated**.
     `.documents/OPEN_ISSUES.md` matches anything opened.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 07 landed, the gate Figures held, and this note added to the list of orchestrator notes, marked
  "used for 07";
- an *Orchestrator review of prompt 07* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Figures;
- what the figures show, and anything that reads wrongly;
- anything opened;
- that 08 (the close) is written next.
