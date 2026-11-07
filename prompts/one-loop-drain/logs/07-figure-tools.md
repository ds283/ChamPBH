# Log 07 — the figure tools

**Subject:** Add the grid and per-history figure tools for the one-loop exports, and render them on 06's and 05's exports
**Commit:** this commit, on branch `one-loop-drain`, on top of `d4031dc` (orchestrator note 07)
**Date:** 2026-11-07, by Claude Sonnet 5.5
**Result:** **COMPLETE.** The gate Figures holds (`.documents/one-loop-drain-verification.md` §7).
- **The tools** (§1): `tools/quanta_figures.py` (shared), `tools/plot_quanta_grid.py` (ten heat maps per one-loop A) and
  `tools/plot_quanta_history.py` (six panels sharing N per history). They read 04's exports and 05's outputs only: no store, no
  Ray, no registered run. Figures are built by `build_figure` functions that return a `matplotlib.figure.Figure`.
- **Tests:** 68 added to `tools/tests` (12 `test_quanta_figures`, 26 `test_plot_quanta_grid`, 30 `test_plot_quanta_history`),
  on constructed exports run through 05's tool; they read the artists. Three deliberate breakages together fail ten of them (§4).
- **The demonstration** (§3): 06's tree: 2 grid pages, 9 history figures; 05's tree: 8 grid pages at c_a = 1, A = 1 at c_a = 0 and
  10, and 4 history figures (the six picks of ruling 6 merge into four histories). Every exit 0. The check script (§2) finds 0
  failures in 58 checks: each figure's drawn count equals quanta_histories.csv's rows at its A, and each history figure's
  generations equal quanta_histories.csv's `generations`.
- **Committed:** 12 PNGs (2.9 MB) under `.documents/one-loop-drain/figures-07/`, the scripts and `out/`. PDFs and 06's figures stay
  under `var/one-loop-drain/07/`.
- **§1.4:** "Reflections (elastic model): N" is "Reflections: N"; two assertions changed, no test added or removed.
- **Suites:** before 26, 665 (219 + 446), 163, 191, 72, all passing; after 26, 665, 163, 259, 72, all passing (§4).
- **Issues:** none opened or closed. Observations in §6.

Orchestrator note 07 (§0, rulings 1-10) governed where it differs from the prompt. Each ruling is classified in §2. No value,
schema, export or measure changed; neither export tree was written into (listing of 3 194 files with mtimes and sizes taken
before and after the demonstration, byte-identical); `quanta_measures.py`, `quanta_exports.py`, `test_quanta_measures.py`, the
plot scripts, `PRyM/` and `.gitignore` are untouched; `ray stop` was never run and no cluster was started.

## 1. What shipped

- **`tools/quanta_figures.py`** (new): `REGIMES` (one-loop, gap, classical, unassessed; an Okabe-Ito subset) and
  `REGIME_COLOURS`/`REGIME_LABELS`; `classify`/`walk` (ruling 1's order); `read_histories`, `key_of` (the five key cells as
  strings), `is_close` (`math.isclose(rel_tol=1e-9)`); `check_model_rows`/`read_checked` (the model check, ruling 3);
  `to_output` (the root replaced by `--output`), `plots_dir` (the `csv` component of `quanta_measures.history_dir`'s path
  replaced by `plots`; no path formatted inline), `save_figure` (PDF and PNG at 150 dpi, creating the directories);
  `FigureRefused` and its subclass `InputDefect` (a negative value on a log10 panel, a non-finite cell, `ln_n_g` out of range).
- **`tools/plot_quanta_grid.py`** (new): `PYTHONPATH=. ./venv/bin/python tools/plot_quanta_grid.py ROOT [--A-values A ...]
  [--assoc-scale C] [--output DIR]`. One figure per (drain model, A) of the one-loop rows, or per A given. Ten panels, (a)-(j) in
  the prompt's order, in a 2 x 5 layout (24 x 10.5 in), each with its own colour bar and a title naming its columns: linear
  `Normalize(0, 1)` for (a); `LogNorm` over the finite positive values for (b), (c), (h), (i), (j); `Normalize` over the finite
  values for (d); `Normalize(-m, m)` for (e)-(g), m the largest |value| (never `TwoSlopeNorm`). Special cells, the same in every
  panel: a blank is a hatched grey `Rectangle`, a zero on a log10 panel a solid grey one, a failed history a cross in every
  panel, a drawn history with no quanta_histories row a cross in (h)-(j); cells with no history take a cream background, which
  was added after the first render (§2, D2). The footer and the printed line give the root, the drain model, A, the spectrum
  model, c_a and the counts (drawn, failed, no quanta row, blank and zero cells per panel, the degenerate colour ranges).
  Written to `alpha_path(root, A, drain_model)/plots/quanta_grid<suffix>.{pdf,png}`.
- **`tools/plot_quanta_history.py`** (new): `... ROOT [--A-values ...] [--beta-values ...] [--M-Mp-values ...] [--assoc-scale C]
  [--output DIR]`. Six panels on one shared x-axis (panel 3's twin included), 11 x 17 in: (1) ln(m a/M_P) for the field and
  tracker (lines) and each generation's `log_q_Mp` at `N_turn` (points by regime), `plateau` and `wall` events as ticks along the
  bottom; (2) `alpha_eff` (filled) and dK/(K_before + dK) (open) by regime; (3) K(N)/K_final, the parking fraction, and r_chi on a
  twin axis; (4) E_field, E_tracker, r_chi E_field, r_chi E_tracker; (5) n_g, n_bar, backreaction ratio at the bounce's N;
  (6) phase slip and k_ex/H with the 1 rad line. Log10 y on 2-6; panels 1, 2, 5 rasterised; one figure-level legend of the
  regimes; a wrapped footer with the key, model, c_a, counts, delta_stim, the shares and the zeros not drawn. Written to
  `<row>/plots/<history_directory>/quanta_history<suffix>.{pdf,png}`.
- **Tests** (`tools/tests/`): `test_quanta_figures.py` also holds `ExportMixin` (the constructed exports: five-regime candidate
  rows with a plateau and a wall event, a ledger-made K and r_chi, a parking leftover where wanted; A = 1: three measured
  histories (one with blank `ell_min` and a zero non-one-loop share, one with every drain scaled by 3), one drawn with no
  bounces.csv (so no quanta row), one failed, one marker; A = 2: two measured with a leftover; a fixed row and a fixed marker
  row; 05's tool run at c_a = 1, 0, 10) and the tests of ruling 1's classes, `is_close`, the model check, `to_output`,
  `plots_dir`, `save_figure` and the regime colours.
- **§1.4:** `extract_common.add_ScalarHistory_labels` and the two assertions in `ComputeTargets/tests/test_reflection_reporting.py`.
- **`.documents/one-loop-drain/figures-07/`:** `pick_histories.py` (ruling 6), `run_demo.sh` (every command of §3 with its output,
  exit and wall in `out/`), `check_figures.py` and `out/check_figures.txt`, `copy_pngs.py`, and the 12 PNGs.
- **`.documents/one-loop-drain-verification.md` §7**, **`.documents/architecture-summary.md`** (a dated note), **`CLAUDE.md`**
  (counts only), **the board** and this log.

## 2. Deviations from the prompt

Orchestrator note 07's rulings, each as the note classes it:

1. **The four counts, in order** (fixed, marker, failed, drawn). *IMPLEMENTATION CHOICE*, ruled. On 06's tree 36, 9, 0, 9; on
   05's 0, 0, 0, 230 (printed line, `out/grid-06.txt`, `out/grid-05.txt`).
2. **Values and cells** (`isclose` selectors; categories sorted by `float()`, M bottom to top; join on the five key cells as
   strings; two rows in one cell refuse that A, exit 1). *IMPLEMENTATION CHOICE*, ruled. A quanta_histories.csv with two rows of
   one key is refused likewise (my addition within the ruling's intent).
3. **The model check** (expected model `SPECTRUM_MODEL`, scale compared as `float(cell) == scale`, `--assoc-scale` parsed by
   `quanta_measures._scale`; the grid tool writes nothing and exits 1 if the file fails or is missing; the history tool skips a
   history with neither quanta file and counts it, refuses one with one file only, with a failing file or with no row, and
   refuses every history if quanta_histories fails). *As the prompt says; made exact.*
4. **The colour scales, exactly** (norm types and limits; degenerate ranges; a negative value on a log10 panel is a stop). *As
   the prompt says; made exact.* Met in the demonstration: panel (b) at 06's A = 0.1 (the four sums are all zero: `none`) and
   A = 1 (every positive sum is the same number: `one`), and none at any A of 05's tree (§3.3). No negative
   value was met; no stop.
5. **The history figure, exactly** (log10 axes by `set_yscale`; n_g = `exp(ln_n_g)` refused outside [-745, 709]; panel 2's
   undrawn candidates counted by cause; K_final the last sample, refused if `raw_N` is not ascending, K curve omitted if it is 0;
   one shared x-axis; units on every y label). *IMPLEMENTATION CHOICE*, ruled. `ln_n_g` ran to 98.2 at most: no stop.
6. **§3's six histories**, picked by script and drawn one run per pick, each run reporting exactly one figure. *STRUCTURALLY
   REQUIRED*, ruled. The picks are in §3.4.
7. **Committed names** (`A=<A:.5g>_quanta_grid.png`; `A=<A:.5g>_<history_directory with / as _>_quanta_history.png`; the c_a = 0
   and 10 grids stay under `var/`). *As the prompt says; made exact.* `copy_pngs.py` forms them.
8. **Files and helpers** (`quanta_measures.py`, `quanta_exports.py`, `test_quanta_measures.py` used as they are; builders in the new
   test modules; `reflection_count`'s docstring and the README left). *STRUCTURALLY REQUIRED*; none of the three files was edited.
9. **Exit codes and arguments** (argparse's exit mapped `2 if e.code else 0`; an unreadable root exits 2; `sys.exit` only from
   `__main__`). *As the prompt says (P58).*
10. **Timing and load.** Each run's wall and one-minute load are in `out/*.txt` and §3; no run was repeated for load. *As the
    prompt says.*

My own choices where the prompt and note are silent:

- **D1 (IMPLEMENTATION CHOICE): a history whose bounces.csv has a number of generations that is not quanta_histories.csv's
  `generations` is refused** (a defect of 05's output, not met). Likewise quanta_bounces.csv must match bounces.csv's `ordinal`s
  and `source`s in order, and quanta_samples.csv fields.csv's `raw_N` row by row.
- **D2 (IMPLEMENTATION CHOICE): the background of a grid cell with no history is cream (`#f4ecd8`), with a legend entry.** The first
  render used white, which is also where the diverging map passes through zero and the pale end of the sequential maps; the
  first A = 1 page could not tell an empty cell from a near-zero margin. Changed before the committed figures were made.
- **D3 (IMPLEMENTATION CHOICE): the history figure's footer is wrapped at 170 characters** (the first render clipped the shares and
  the parking share at the right edge), and panel 6's y range is set to include 1 (`axhline` does not enter autoscaling; without
  it the 1 rad line was off the axes whenever every phase slip is far above 1).
- **D4 (IMPLEMENTATION CHOICE): panel (b) is blank if any of the three shares is blank** (none is, in either tree: 0 mixed rows),
  and a non-finite cell is an `InputDefect` like a negative one on a log10 panel.
- **D5 (IMPLEMENTATION CHOICE): an A whose one-loop rows are all marker rows** is skipped and counted (`A with no drawn or failed row`),
  not an error; `--A-values` matches against every one-loop row, marker rows included. Neither tree has such an A.
- **D6 (IMPLEMENTATION CHOICE): the figures are built with `matplotlib.figure.Figure`, never pyplot,** so no backend is selected
  and the tests need none (the prompt said "Agg"; `savefig` on a `Figure` uses Agg's canvas).
- **D7 (IMPLEMENTATION CHOICE): colour maps.** viridis (a)-(c), cividis (d), RdBu (e)-(g), magma (h)-(j); the regime colours are
  `#0072B2`, `#E69F00`, `#CC79A7`, `#009E73`. Panels 5 and 6 use neutral dark/purple markers so that no colour is read as a regime.
- **D8 (IMPLEMENTATION CHOICE): the demonstration's picks give four histories, not six** (§3.4); `run_demo.sh` runs the history
  tool once per distinct pick.
- **D9 (IMPLEMENTATION CHOICE): the three-way breakage record** (§4) is a manual mutation of the code and back, not a committed test.
- **No UNINTENDED DRIFT** is known. Every changed file is in prompt §6's list.

## 3. Verification performed

### 3.1 Gate

At `d4031dc`: tree clean (`git status --short` empty at the start); `runregistrykit list`: 33 runs, 33 finished; both export trees
and the three quanta_histories files in each present; `var/one-loop-drain/07/` and `.documents/one-loop-drain/figures-07/`
absent; PYTHONPATH unset; machine load 4.98 / 7.31 / 5.66 at 15:28.

### 3.2 The commands, as run (`.documents/one-loop-drain/figures-07/run_demo.sh`, from the repository root)

`ROOT6 = var/one-loop-drain/06/plot-reference/QCD_Cosmology`, `ROOT5 = var/one-loop-drain/05/exports/QCD_Cosmology`; each run's
output is in `.documents/one-loop-drain/figures-07/out/`. Loads are the one-minute load at the start (low: the machine was quiet).

| # | command (`PYTHONPATH=. ./venv/bin/python`) | exit | wall | summary line |
|---|---|---|---|---|
| 1 | `tools/plot_quanta_grid.py ROOT6 --output var/one-loop-drain/07/plot-reference/QCD_Cosmology` | 0 | 4 s (tool 2.4 s), load 3.34 | figures written 2, refused 0; skipped as fixed-law 36, marker rows 9 |
| 2 | `tools/plot_quanta_history.py ROOT6 --output var/one-loop-drain/07/plot-reference/QCD_Cosmology` | 0 | 10 s (7.8 s; 0.8-0.9 s a figure), load 3.32 | figures written 9, refused 0, no quanta files 0; skipped as fixed-law 36, marker rows 9, failed histories 0 |
| 3 | `tools/plot_quanta_grid.py ROOT5 --output var/one-loop-drain/07/exports-05/QCD_Cosmology` | 0 | 14 s (12.5 s; 1.4-1.6 s a figure), load 3.50 | figures written 8, refused 0 |
| 4 | `tools/plot_quanta_grid.py ROOT5 --A-values 1 --assoc-scale 0 --output ...exports-05/QCD_Cosmology` | 0 | 3 s | figures written 1, refused 0 |
| 5 | the same with `--assoc-scale 10` | 0 | 4 s | figures written 1, refused 0 |
| 6-9 | `tools/plot_quanta_history.py ROOT5 --A-values A --beta-values B --M-Mp-values M --output ...exports-05/QCD_Cosmology` for the four picks | 0 each | 4, 3, 2, 3 s | figures written 1 each (generations 13 342, 64, 301, 4) |

The `--output` trees are the only places written. The listing of `var/one-loop-drain/05/exports` and `var/one-loop-drain/06`
(`find -printf '%T@ %s %p'`: 3 194 files) was taken before and after: identical.

### 3.3 The degenerate colour ranges (ruling 4)

From the printed lines (`out/grid-06.txt`, `out/grid-05*.txt`):
- **06's tree, A = 0.1:** panel (b) `none` ("no positive value" in its title, ticks removed): the four sums of the non-one-loop shares
  are all 0, so its four cells are the zero grey.
- **06's tree, A = 1:** panel (b) `one`: the one positive sum (five histories, four zero) is widened to [v/sqrt(10), v sqrt(10)].
- **05's tree, every A and every scale:** none. Panel (b) has 11-23 zero cells and positive values in the rest; panel (c) has one or two
  zero cells (the 1-2 histories with no parking); (d) has 4-5 blank cells.
- No negative value on any log10 panel; no `ln_n_g` out of range; no stop.

### 3.4 The six picks (ruling 6)

`pick_histories.py ROOT5` (largest of each column, quanta_histories.csv joined to histories.csv on the five key cells as strings,
compared as floats, first in file on a tie):

```
0.1 2.0 5e-20 1 | generations=13342.0, n_bar_max=8.977978071636221e+31, phase_slips_below_1=6302.0
5.0 2.0 5e-16 1 | drained_share_gap=0.999979014182823
10.0 1.0 1e-12 1 | drained_share_classical=1.0000000000000007
2.0 2.0 8e-18 1 | backreaction_max=1784.897704016393
```

(alpha beta M_Mp n.) Six columns, four histories. The classical share's maximum is 1.0000000000000007, above 1 by rounding (§6).

### 3.5 The gate Figures

1. **Both tools' tests pass in a `tools/tests` run whose count rose by exactly the tests added:** 191 -> 259, +68 = 12 + 26 + 30
   (§4). *Holds.*
2. **Both tools render on 06's exports with exit 0, every one-loop history drawn:** grid 2 figures; history 9 figures = the 9 drawn
   one-loop rows of 06's histories.csv. *Holds.*
3. **Both tools render on 05's exports with exit 0:** grid at the eight A and at A = 1 for c_a = 0 and 10; history on four
   histories. *Holds.*
4. **Counts against the tree's files, by script** (`check_figures.py`, output in `out/check_figures.txt`): 58 checks, 0 failures.
   Grid: each A's `drawn` equals the drawn rows of histories.csv at that A and, less the "no quanta row" count (0 everywhere),
   the rows of quanta_histories<suffix>.csv at that A (06: 4 and 5; 05: 27, 28, 29, 30, 29, 29, 29, 29; c_a = 0 and 10 at A = 1:
   30). History: generations 13 342, 64, 301, 4 equal quanta_histories.csv's `generations`; for 06's nine histories
   likewise (check lines in `out/check_figures.txt`). *Holds.*

### 3.6 The figures, by eye

Every committed PNG was opened in its final form (after the changes of D2 and D3; the three 05 history figures other than
`A=0.1` and the A = 1 grid were opened a second time after D3). What each shows, from the image and the printed line beside it;
none reads wrongly in a way that is the tool's. The pages are in `.documents/one-loop-drain/figures-07/`.

**Grid pages, 05's export (c_a = 1).** All eight have the same cells: a band at M = 4e-31 across beta = 1 to 1.1875, M = 1e-33
and 1e-30 at beta = 1.2 to 10, a column at beta = 2 from M = 1e-33 to 1e-12, and beta = 4 and 10 at a few M. The cream
background separates the empty cells from the pale ends of the colour maps. Counts are from the printed lines.
- **Panel (a):** 1.0 across the lab band and up the beta = 2 column to M = 1e-16 or 8e-18, small at the largest M.
- **Panel (b):** grey (zero) across the lab band, 11 zero cells at A = 0.05, 21-23 at the others; positive only on the
  beta = 2 column at M >= 5e-20 and at beta = 1, M = 1e-12.
- **Panel (c):** a positive parking share over most of the grid, 1 zero cell (2 at A = 1), largest at beta = 4, M = 1e-12.
- **Panel (d):** hatched cells (4 blank at every A but A = 1, which has 5) at M = 1e-12 (beta = 1 and 4) and at M = 4e-15 to
  5e-16 on beta = 2 (and, at A = 1, beta = 1.11, M = 1e-12).
- **Panels (e)-(g):** the margins are symmetric about 0 (white) and mostly positive: light blue in (e) and (f) over the lab band, dark
  blue in (g); negative cells at a few high-M cells of the beta = 2 column and at beta = 1, M = 1e-12.
- **Panels (h)-(j):** each spans tens of decades on its own log scale (delta_stim from about 1e-60 to 1e-4 at A = 1, E_field_max
  to about 1e-20); the largest values are at M = 1e-12 and on the beta = 2 column.
- **A = 1** has an empty column at beta = 1.11 in the lab band (30 drawn, against 27-29 at the others) and, at M = 1e-12, an
  extra cell at beta = 1.11 (the fifth hatched cell of panel (d)).
- **Colour bars:** (a) 0-1; (e)-(g) symmetric; (b), (c), (h)-(j) log.

**Grid pages at c_a = 0 and 10 (A = 1):** not committed; drawn 30 = 30 as at c_a = 1 (`out/grid-05-assoc0.txt`, `...assoc10.txt`);
not opened (they stay under `var/`; the counts above are the check).

**History figures, 05's export:**
- **`A=0.1_beta=2_M=1.2177e+08eV_n=1`** (13 342 generations; M = 5e-20 M_P): panel 1 shows eight one-loop generations (blue) at
  N = 20-25, one gap generation (orange), and the rest classical (pink) running down to ln(q/M_P) about -75 at N = 47, where
  the field's m a line is spiky; the plateau ticks pile up along the bottom. Panel 2: alpha_eff and the fraction fall from the
  one-loop generations to 10^-100; one candidate (dK = 0) is undrawn. Panel 3: K/K_final steps up to 1 by N = 25; r_chi
  1e-3 to 3e-3; the parking fraction appears at N = 47. Panel 4: E_tracker far above E_field. Panel 5: n_g and n_bar rise by tens
  of decades toward the last generation; the backreaction ratio falls. Panel 6: the phase slips cross the 1 rad line near
  N = 46 (the history has 6 302 below 1, `phase_slips_below_1`); k_ex/H stays above.
- **`A=5_beta=2_M=1.2177e+12eV_n=1`** (64 generations, M = 5e-16): one gap generation, then classical ones; alpha_eff of the gap
  generation about 1, the classical ones from 1e-4 to 1e-19; K/K_final is 1 from the first drain. Panel 5: the backreaction ratio is of
  order 1 to 10. Panel 6: every phase slip is 1e14-1e17, so the 1 rad line sits twelve decades below the data (the range includes 1, D3).
- **`A=10_beta=1_M=1e-12_n=1`** (301 generations, all classical): K/K_final runs 0.984 to 1 only, so panel 3's left axis spans
  a few percent; r_chi is about 1.7e-56. Panel 4: r_chi E is some 50 decades below E. Panel 6: the phase slips cross the 1 rad line near N = 43.
- **`A=2_beta=2_M=8e-18_n=1`** (4 generations: 3 one-loop, 1 gap, all at N = 20.4-20.6): the bounces sit at one N, so panels 2, 5 and
  6 show points in a column; E_field jumps by many decades there.
- In all four, **the legend names the regimes once**; `unassessed` has no point. The footer gives the key, the shares and the
  zeros not drawn (`K/K_final` 2209-3890, `r_chi` the same: the samples before the first drain, where K = 0;
  `parking_fraction` 22-2959; `n_bar` and `backreaction_ratio` 1 each; the printed lines in `out/history-05-*.txt`).

## 4. Suites

Run serially from the repository root with `PYTHONPATH=.`; `ComputeTargets/tests` in its two halves; times are the unittest
runner's; one-minute loads at each suite's start.

| suite | before (at `d4031dc`) | after |
|---|---|---|
| `CosmologyModels/tests` | 26 tests, 67.1 s, load 4.74 | 26 tests, 64.8 s, load 9.19 |
| `ComputeTargets/tests`, `test_[a-f]` | 219 tests, 144.3 s, load 3.67 | 219 tests, 143.0 s, load 8.21 |
| `ComputeTargets/tests`, `test_[g-z]` | 446 tests, 475.4 s, load 11.45 | 446 tests, 467.1 s, load 4.03 |
| `ObjectFactories/tests` | 163 tests, 124.5 s, load 5.41 | 163 tests, 124.7 s, load 8.81 |
| `RunDrivers/tests` | 72 tests, 22.6 s, load 17.07 | 72 tests, 22.6 s, load 5.14 |
| `tools/tests` | 191 tests, 201.9 s, load 12.71 | **259 tests**, 256.6 s, load 4.40 |

`uptime` at the start of the first run: `15:28 up 7 days, 17:49, load averages: 4.98 7.31 5.66` (before); `15:55 up 7 days, 18:16, load averages: 9.19 5.24 5.82` (after); all six OK.

The new modules alone (before the full run): `tools.tests.test_quanta_figures`, `test_plot_quanta_grid`, `test_plot_quanta_history`: 68
tests, about 53 s, OK.

**The deliberate-breakage record** (a manual mutation, restored afterwards; the three files were restored from copies and the
three modules re-run green):
- panel (h)'s scale changed from log10 to linear: fails `test_the_scales` and the counts, and `test_a_log_range...`;
- panel 6's 1 rad line moved to y = 2: fails `test_panel_6_phase_slips_and_the_1_rad_line`;
- the marker test in `classify` disabled: fails ruling 1's class tests and the run tests that count marker rows.
Together ten of the 68 tests failed.

## 5. Observations not acted on

1. **`reflection_count`'s docstring** (`extract_common.py`) still says "elastic reflections" (note 07 ruling 8: left).
2. **`drained_share_classical` is 1.0000000000000007** at A = 10, beta = 1, M = 1e-12. The tool draws it as it is; panel (a) is 0-1 and unaffected, (b) is a log sum. A rounding of 05's or 03's sum,
   not a defect of the tool; not opened.
3. **A history with every generation at one N** (A = 2, M = 8e-18, beta = 2: 4 generations at N = 20.4-20.6) gives a column of
   stacked points on panels 2, 5 and 6. That is the data.
4. **The 1 rad line is far from most data**: in three of the four committed histories every phase slip is 10^14-10^31 rad, so the
   line (kept in range, D3) sits far from the points; only the A = 0.1, M = 5e-20 history crosses it.
5. **The `unassessed` regime has no point** in any of the 12 committed figures (`drained_share_unassessed` is 0 at all 230
   histories of 05's tree; 06's nine are not committed).
6. **Panel 3's y range** is a few percent wide where K/K_final is near 1 throughout (the A = 10 history); the log axis shows
   the 0.984-1 range as its own ticks. The data's.
7. **The history tool reads each history's four CSVs in Python lists of dicts**; the 13 342-generation history takes 2.4 s. A tree of
   thousands of histories of that size would take hours; the figure tools are for a handful.
8. **The shard warning** and the ΔN_eff legend's alpha with no point are not 07's (prompt §4).
9. **The grid's "no quanta row" and "failed" crosses are exercised by the tests alone**: neither export tree has either (note 07 §0).

## 6. State handed to the next prompt (08, the close)

- **Commands.**
  - Grid: `PYTHONPATH=. ./venv/bin/python tools/plot_quanta_grid.py ROOT [--A-values A ...] [--assoc-scale C] [--output DIR]` writes
    `<ROOT or DIR>/one-loop-measured-v1/A=<A:.5g>/plots/quanta_grid<-assoc<c:g>>.{pdf,png}`; one line per figure, then a summary.
  - History: `PYTHONPATH=. ./venv/bin/python tools/plot_quanta_history.py ROOT [--A-values ...] [--beta-values ...] [--M-Mp-values ...]
    [--assoc-scale C] [--output DIR]` writes `<...>/A=<A>/plots/beta=<b>/M=<M_eV>eV_n=<n>/quanta_history<suffix>.{pdf,png}`.
  - Exits: 0 all asked-for figures written (the history tool also counts histories with no quanta files as skipped); 1 a figure or history was refused
    (the rest written) or the grid tool's quanta_histories file failed its check; 2 usage, a selector matching nothing, an unreadable root.
  - Both need `histories.csv` and `quanta_histories<suffix>.csv` (05's output) in ROOT, and the history tool each history's four CSVs.
    Neither opens a store or starts Ray; run from the repository root.
- **Figures.** 12 PNGs, 2.9 MB, committed: 05's eight grid pages (`A=<A>_quanta_grid.png`) and four history figures. Everything else is
  under `var/one-loop-drain/07/`: the PDFs (25, 1.75 MB), 06's two grid pages and nine history figures (`plot-reference/`), and
  A = 1's c_a = 0 and 10 grids (`exports-05/`). `run_demo.sh` regenerates all of them in about 50 s on a quiet machine, and
  `copy_pngs.py` renames the committed ones.
- **Costs.** A grid page 1.0-1.6 s; a history figure 0.8-0.9 s at 64-600 generations, 2.4 s at 13 342 (PDF 400 kB, PNG 290 kB; the
  others 36-60 kB PDFs and 250-290 kB PNGs); a grid PDF 51-72 kB and PNG 185-232 kB.
- **What the demonstration shows that the rebuild's handover should say.**
  - The grid has the same "plus" shape at every A: the figures differ in values and in which cells are zero or blank, not in
    where the data are. That is the grid 02d ran, not a rectangle.
  - Panel (b) is zero (grey) across the lab band, where the one-loop share is 1.0 (a), so the gap and classical shares are 0
    there; panel (c)'s parking share is zero in only one or two histories at each A.
  - Panel (d)'s blank `ell_min` cells are 4 (5 at A = 1) histories at every A (no one-loop drain).
  - The all-generation margins are mostly positive over the grid and negative at a few cells; this is the measurement the weighted
    margins hid. Each page's colour bars carry the numbers.
  - delta_stim reaches about 1e-4 and E_field_max about 1e-20 at most; the backreaction maximum is 1.8e3 at its largest (A = 2,
    M = 8e-18 M_P, beta = 2: `backreaction_max` in `out/picks.txt`).
  - Where the history figures show phase slips below 1 rad it is in the long classical tails; the one history with a count
    (6 302 below 1 of 13 342 generations) is the A = 0.1, M = 5e-20, beta = 2 one.
  - The tools refuse rather than guess: wrong model or scale, a history out of step with its quanta files, a generation count that
    is not quanta_histories', a negative value on a log axis, an `ln_n_g` out of float range. None was met.
- **Not touched:** `quanta_measures.py`, the plot scripts, the README, the prompts' index and the board's issue sections.
