# Prompt 07 — the figure tools

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-07, on branch `one-loop-drain` in the main checkout, after 06 landed and was reviewed (`1416b80`,
`0a0b540`). It is written from:
- README §8.4 (row 07), §8.5 (the gate Figures) and §8.15 (the review before this prompt, P58–P65);
- README §2 (the tools' first names) and §6.1's U7 (two tools in `tools/`, reading the exports; the figures of record
  are the user's);
- 05's log §7 (the tool's output format, exactly) and 04's log §7 (the export format);
- 06's log §6 and §7 (the marker rows, the footer label, and the exports to render on);
- the tree's code at the commit that adds this prompt.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. The demonstration's figures go under
`var/one-loop-drain/07/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 665 = 219 + 446, 163, 191, 72), all passing;
- `var/one-loop-drain/06/plot-reference/QCD_Cosmology/` and `var/one-loop-drain/05/exports/QCD_Cosmology/` exist, each
  with `histories.csv` and `quanta_histories.csv`, `quanta_histories-assoc0.csv` and `quanta_histories-assoc10.csv`;
- neither `var/one-loop-drain/07/` nor `.documents/one-loop-drain/figures-07/` exists.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Sonnet**, as README §8.4 plans. The work is plotting from files whose format is fixed and
stated; the care is in reading those files exactly, and in tests that check what is drawn rather than that a file
exists.

**Read first:**
1. [`README.md`](README.md) §8.15 whole; §8.3 (what the measures are); §8.10's table of regimes.
2. [`logs/05-derivation-tool.md`](logs/05-derivation-tool.md) §7, the output format, whole.
3. [`logs/04-drivers-and-exports.md`](logs/04-drivers-and-exports.md) §7: where histories.csv, fields.csv and
   bounces.csv are, and their columns with units.
4. [`logs/06-rehearse.md`](logs/06-rehearse.md) §6 and §7.
5. `tools/quanta_measures.py`: `read_csv`, `history_dir`, `row_label`, `suffix`, `KEY_COLUMNS`, the three output column
   lists, and `run` (how it walks histories.csv, and what it skips and counts).
6. `extract_common.py`: `alpha_path`, `history_directory`, `alpha_label`, `_cosmology_line`, `add_ScalarHistory_labels`,
   `reflection_count`.
7. `tools/tests/quanta_exports.py` (`write_constructed`) and `tools/tests/test_quanta_measures.py`: how 05's tests
   build exports and run the tool on them.
8. `tools/plot_surfing_histories.py` and `tools/tests/test_plot_surfing_histories.py`: a figure tool of this repository
   and tests that read a figure's artists.

---

## 0. Where this prompt sits

05 derived the spectrum-dependent measures from the exports and wrote them to three files per history and one per root.
06 rehearsed the whole chain under Ray and kept two export trees with the tool's files beside them. Nothing yet draws
those numbers. This prompt writes the two figure tools that U7 asks for, tests them on constructed exports, and renders
them on both trees. The figures of record are made by the user from the rebuild (U2, U7); these are the tools and
their demonstration.

It changes no value, schema, export or measure. The only code it changes outside the two new tools and their shared
module is one footer label (P64).

## 1. What is wanted

### 1.1 The shared module (P61)

`tools/quanta_figures.py`, imported by both tools. It holds:
- **the regimes' colours**, a fixed categorical order: one-loop, gap, classical, unassessed (the `regime` strings of
  bounces.csv: `one_loop`, `gap`, `classical`, `unassessed`; blank on an event row). A candidate row whose regime is
  none of these four is refused, not given a new colour;
- **finding the files.** The root is a cosmology directory, `<output>/<label>/`, the one 05's tool takes. A history's
  directory is `quanta_measures.history_dir`; its `plots/` directory is the same path with `csv` replaced by `plots`
  (`<row>/plots/<history_directory>`), as the plot scripts write it. Use the existing helpers; do not format a path
  inline;
- **the model check.** Given the scale asked for, the file names (`quanta_measures.suffix`) and the check that every
  row of every file read names one `spectrum_model` and that scale's `assoc_scale`. A file that fails is refused, with
  the history's `row_label` and the reason on the printed line;
- **the rows to draw.** Walking histories.csv as `quanta_measures.run` does, with four counts:
  - `fixed` (`drain_model` is `fixed`);
  - `marker` (`failure_reasons` is `no ScalarHistory row in the store`, the plot script's marker of a grid point with
    no history; see `plot_by_beta.py`);
  - `failed` (any other one-loop row with a blank `normalization`);
  - `drawn`.
- **the `--output` mapping.** The output path is the input path with the root replaced by `--output`, when it is
  given. The tool creates the directories it writes into and writes nothing else.

### 1.2 The grid figure (P59): `tools/plot_quanta_grid.py`

```
PYTHONPATH=. ./venv/bin/python tools/plot_quanta_grid.py ROOT [--A-values A [A ...]] [--assoc-scale C] [--output DIR]
```

One figure per one-loop A in histories.csv, or per A given (an A not in histories.csv is a usage error, exit 2). The
rows are histories.csv joined to quanta_histories.csv (at the scale's suffix) on `KEY_COLUMNS`, the cells compared as
strings: 05 copies them verbatim.

- **Axes.** β across and M up, each the sorted distinct values of that A's drawn and failed rows as categories. Tick
  labels give β as `%.5g` and M as M/M_P in `%.3g`. A cell with no row is empty (the background).
- **Panels**, in this order, each a heat map with its own colour bar and title naming the column it draws:

  | panel | column | file | scale |
  |---|---|---|---|
  | (a) | `drained_share_one_loop` | histories.csv | linear, 0–1 |
  | (b) | `drained_share_gap` + `drained_share_classical` + `drained_share_unassessed` | histories.csv | log₁₀ |
  | (c) | `parking_share` | histories.csv | log₁₀ |
  | (d) | `ell_min` | histories.csv | linear |
  | (e) | `all_worst_birth_margin` | histories.csv | diverging, centred at 0, symmetric about it |
  | (f) | `all_worst_reflection_margin` | histories.csv | as (e) |
  | (g) | `all_worst_tracker_margin` | histories.csv | as (e) |
  | (h) | `delta_stim` | quanta_histories.csv | log₁₀ |
  | (i) | `E_field_max` | quanta_histories.csv | log₁₀ |
  | (j) | `backreaction_max` | quanta_histories.csv | log₁₀ |

  A log₁₀ panel's colour range is the finite values' own range, not a fixed one.
- **Special cells**, each drawn the same way in every panel and counted in the footer:
  - a **blank** value (NULL), such as `ell_min` with no one-loop drain: hatched grey;
  - a **zero** on a log₁₀ panel: solid grey, distinct from the hatching;
  - a **failed** history (P58's `failed` count): a cross in every panel;
  - a drawn history with **no quanta_histories row** at this scale (05 refused it or its K check failed): a cross in
    panels (h)–(j) only.
- **The footer** gives the root, the drain model and A, the spectrum model and c_a, and the counts: drawn, failed, no
  quanta row, blank and zero cells per panel. The printed line per figure gives the same counts and the files written.
- **Written** to `alpha_path(root, A, drain_model) / "plots" / "quanta_grid.pdf"` and `.png`, with
  `quanta_measures.suffix` before the extension at another scale (`quanta_grid-assoc0.png`), under `--output` if given.
  The PNG at 150 dpi.

### 1.3 The per-history figure (P60): `tools/plot_quanta_history.py`

```
PYTHONPATH=. ./venv/bin/python tools/plot_quanta_history.py ROOT [--A-values …] [--beta-values …] [--M-Mp-values …] \
    [--assoc-scale C] [--output DIR]
```

One figure per drawn one-loop history that has its quanta files at the scale. With no selector, every one; with
selectors, those whose `alpha`, `beta` and `M_Mp` match one of each given list. Match a value given on the command line
to histories.csv's cell by `math.isclose(rel_tol=1e-9)`, since a typed value need not be the cell's `repr`, and report
a selector value that matches nothing (exit 2).

It reads, from the history's directory: fields.csv, bounces.csv, quanta_bounces.csv, quanta_samples.csv. It checks that
quanta_bounces.csv has bounces.csv's `ordinal`s in order and that quanta_samples.csv has fields.csv's rows, joined on
`raw_N` (05's log §7); a mismatch refuses the history.

Six panels sharing the e-fold axis, N (`raw_N`; `N` and `N_turn` in bounces.csv). Each y-axis is labelled with its
quantity and unit.

1. **m·a against the generations' q.** ln(m_eff a/M_P) = `log_m_eff_Mp` + `raw_N` and ln(m_tracker a/M_P) =
   `log_m_tracker_Mp` + `raw_N` from fields.csv as lines; each generation's `log_q_Mp` at its `N_turn` as a point,
   coloured by regime. A generation is a candidate row with `delta_K_Mp4` > 0 and a `log_q_Mp` (05's definition). The
   `plateau` and `wall` events are short ticks along the bottom of this panel.
2. **The drain.** Each candidate's `alpha_eff` at `N`, log₁₀, coloured by regime, filled markers; and the drained
   fraction ΔK/(K_before + ΔK) from `delta_K_Mp4` and `K_before_Mp4`, open markers in the same colours. A candidate with
   ΔK = 0 is not drawn on a log axis, and is counted in the footer.
3. **The dark radiation.** K(N)/K_final (`K_Mp4` over the last sample's) and `parking_fraction` from quanta_samples.csv,
   and r_χ (`rho_chi_over_rho_rad` from fields.csv), log₁₀. r_χ on a twin axis.
4. **The excess.** `E_field`, `E_tracker`, `r_chi_E_field`, `r_chi_E_tracker` from quanta_samples.csv, log₁₀.
5. **The occupations.** n_g = exp(`ln_n_g`) and `n_bar` per generation, and `backreaction_ratio` per candidate, from
   quanta_bounces.csv at the bounce's `N`, log₁₀. A zero is not drawn and is counted.
6. **The phase slip.** `phase_slip` and `k_ex_over_H` per assessed candidate, log₁₀, with a horizontal line at 1 rad
   labelled as the reference 05 counts `phase_slips_below_1` against.

The legend names the regimes once. Panels 1, 2 and 5 are rasterised inside the PDF (`rasterized=True` on their
collections), so that a history of tens of thousands of rows stays small. The footer gives the key (`row_label`), the
spectrum model and c_a, the generation and bounce counts, δ_stim, `drained_share_*` and `parking_share`, and the
counts of undrawn zeros.

**Written** to `<row>/plots/<history_directory>/quanta_history.pdf` and `.png`, beside the plot scripts' figures of that
history, with the scale's suffix, under `--output` if given. The PNG at 150 dpi.

### 1.4 The footer label (P64)

In `extract_common.add_ScalarHistory_labels`, "Reflections (elastic model): N" becomes "Reflections: N". The count and
`reflection_count` are unchanged. The two assertions in `ComputeTargets/tests/test_reflection_reporting.py` that name the
old label change with it; no test is added or removed there.

### 1.5 Tests (P62)

Two modules, `tools/tests/test_plot_quanta_grid.py` and `tools/tests/test_plot_quanta_history.py`, and tests of
`tools/quanta_figures.py` in one of them or a third. They build exports with `quanta_exports.write_constructed` in a
`tempfile` directory, run 05's tool on them through `quanta_measures.run`, and draw with the Agg backend. Each tool
builds its figure in a function that returns the `Figure`, and `main` saves it; the tests read the artists. At least:
- **grid:** the ten panels in order, with their scales (log, linear, diverging centred at 0); the cell count of each
  panel equals the constructed histories at that A; a blank, a zero, a failed history and a history with no quanta
  row are each drawn as §1.2 says and counted; marker and fixed rows are skipped and counted; two A give two figures;
  an A not in histories.csv exits 2;
- **history:** the six panels share N; panel 1 holds one point per constructed generation, coloured by its regime;
  panel 2's counts of drawn and undrawn candidates; the 1 rad line; a quanta_bounces.csv out of step with bounces.csv
  refuses that history (exit 1) and writes nothing for it, while the others are written; the selectors, and a selector
  that matches nothing (exit 2);
- **both:** `--assoc-scale` reads and writes the suffixed names; a file naming another model or two models is refused;
  `--output` writes under the other root and nothing under the input; both the PDF and the PNG are written; an
  unreadable root exits 2.

Constructed histories should include all four regimes, a history with no one-loop drain (blank `ell_min`), a zero
share, and enough generations that the panel counts are not trivially one.

## 2. The gate Figures (README §8.5)

Holds when:
1. both tools' tests pass, in a `tools/tests` run whose count rose by exactly the tests added;
2. both tools render on 06's exports (§3, first item) with exit 0, every one-loop history of that tree drawn;
3. both tools render on 05's exports (§3, second item) with exit 0;
4. each rendered figure's counts on its printed line agree with the tree's files: the drawn histories with
   quanta_histories.csv's rows at that A, and a history figure's generations with quanta_histories.csv's
   `generations`. Check it with a script, not by eye.

## 3. The demonstration (P63)

- **On 06's exports**, `ROOT=var/one-loop-drain/06/plot-reference/QCD_Cosmology`, with `--output
  var/one-loop-drain/07/plot-reference/QCD_Cosmology`:
  - `plot_quanta_grid.py` with no `--A-values` (A = 0.1 and 1);
  - `plot_quanta_history.py` with no selector (the nine one-loop histories).
- **On 05's exports**, `ROOT=var/one-loop-drain/05/exports/QCD_Cosmology`, with `--output
  var/one-loop-drain/07/exports-05/QCD_Cosmology`:
  - `plot_quanta_grid.py` with no `--A-values` (the eight A);
  - `plot_quanta_grid.py --A-values 1 --assoc-scale 0` and `--assoc-scale 10`;
  - `plot_quanta_history.py` on at most six histories: those holding the largest `generations`, `drained_share_gap`,
    `drained_share_classical`, `n_bar_max`, `backreaction_max` and `phase_slips_below_1` in quanta_histories.csv
    joined to histories.csv, duplicates merged. Pick them with a script and select them with the selectors.
- **Look at every figure** you commit (open the PNG), and record in the log, for each, one or two sentences on what it
  shows and anything that reads wrongly: an empty panel, a scale that hides the data, overlapping labels, a colour bar
  with no range. Fix what is the tool's; report what is the data's.
- **Committed:** the PNGs of 05's eight grid pages and of its history figures, under
  `.documents/one-loop-drain/figures-07/`, named by their row and history (for example `A=1_quanta_grid.png`,
  `A=0.1_beta=2_M=1.2177e+08eV_n=1_quanta_history.png`). If the PNGs come to more than 8 MB, leave the largest under
  `var/` and name them in the log. The PDFs and everything of 06's tree stay under `var/`.
- **Time and size.** Record each tool's wall time per figure and the PDF and PNG sizes, the largest history's
  included.

## 4. What is not wanted

- **No value, schema, key, export or measure changes.** No file of 04's or 05's format is written, and neither export
  tree is written into (`--output` throughout §3). If a figure needs a column that the files do not hold, stop and ask.
- **No threshold** colours, refuses or warns on a measure beyond the margins' zero and the 1 rad reference (P65), and no
  value seen in a figure opens an issue. A defect of a tool's input is recorded and opened (README P15); a figure
  merely showing a large number is a measurement.
- **No store, no Ray, no cluster, no registered run**, and no run of `main.py`, `plot_by_beta.py`,
  `plot_ScalarHistory.py` or `quanta_measures.py` as a program. The tests call `quanta_measures.run` directly.
- **No change to the plot scripts** but through `extract_common`'s label (§1.4). The ΔN_eff legend's α with no point
  (06's log §6) and the shard warning (a DatastoreKit issue, README §8.15) are not 07's.
- **No change** to `CLAUDE.md` but the test counts, and none to the architecture summary or `numerical-strategies.md`
  but a dated note naming the two tools (§6). The rest is 08's.
- **Nothing is written** under `var/one-loop-drain/0[1-6]*/` or the briefing notes' repository. No merge, no push.
  `PRyM/`, `thirdparty/` and the packages are not touched.

## 5. Verification

- **The suites,** before the change and after, per `CLAUDE.md`: ComputeTargets in two halves, and `tools/tests` alone.
  Record the counts, the times and `uptime`. The expected changes are `tools/tests` up by the tests added, and nothing
  else (§1.4 changes two assertions, not the count).
- **Where things go.**
  - §2's check script goes in `.documents/one-loop-drain/figures-07/`, with its text output beside it in `out/`.
  - The rendered figures go under `var/one-loop-drain/07/`.
  - Scratch goes in your subdirectory of the session scratchpad, never `/tmp`.
- **Reproducibility.** The log gives every command of §3 verbatim, and every count it quotes comes from a printed line,
  the check script or a test.

## 6. The commit, the log and the board

One commit (invariant 1), with:
- **the code:** `tools/quanta_figures.py`, `tools/plot_quanta_grid.py`, `tools/plot_quanta_history.py`, their tests, and
  §1.4's change to `extract_common.py` and `ComputeTargets/tests/test_reflection_reporting.py`;
- **the figures and the check:** `.documents/one-loop-drain/figures-07/` (the PNGs of §3, the check script and its
  `out/`);
- **the verification document:** `.documents/one-loop-drain-verification.md`, a new §7 holding the gate item by item,
  the commands verbatim, the times and sizes, and the per-figure notes of §3. It is additive (invariant 6), and §1–§6
  stay as they are;
- **dated notes:** in `.documents/architecture-summary.md`, the two tools and their shared module, what they read and
  where they write;
- **`CLAUDE.md`:** the test counts;
- **the log** `logs/07-figure-tools.md` (template: `dk-and-rpk-migration` README §5.1):
  - the suite counts before and after;
  - every deviation from this prompt, classified;
  - "Observations not acted on";
  - "State handed to the next prompt", for 08: each tool's command line and outputs, its exit codes, the figures
    committed and where the rest are, the costs, and what the demonstration shows that the rebuild's handover should
    say;
- **the board:** row 07 and its items, the gate Figures in §2, an entry in §5, and **Last updated**; anything opened in §3
  and in `.documents/OPEN_ISSUES.md`, in the same commit (`CLAUDE.md`).

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. Nothing under `var/` is committed. The commit message follows `CLAUDE.md` invariant 5. Write "this
commit" where the SHA goes; the orchestrator substitutes it.

## 7. Stop conditions: stop and ask

- An item of the Gate at the top fails, or a baseline count differs or fails.
- **A file is not as 05's or 04's log §7 says**: a column missing, a model column other than the one stated, a join
  that does not hold on 06's or 05's tree. That is a defect of 04's or 05's work; record it, and a lettered prompt fixes
  it, not this one.
- **A figure needs a column the files do not hold**, or a value the tools would have to recompute from the store.
- **A test count falls**, in any suite.
- **The work seems to need a change** to any code but §1's, any test but §1.4's two assertions and the new modules,
  `.gitignore` or the git configuration.
