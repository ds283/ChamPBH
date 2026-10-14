# Log — surfing-temperature 03: the plot

**Subject:** `tools/plot_surfing_temperature.py`, which reads the CSV of prompt 02 and draws the
reachability roots and the four measured temperatures against β, and its tests.
**Commit:** this commit · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent
`9d6bbaa` · **Model:** Claude Sonnet 5.5 · **Result:** landed. F1–F3 done. No stop condition fired:
the figure is legible on both CSVs, including the 79-row probe scan.

## What shipped

- **`tools/plot_surfing_temperature.py`** (new).
  - **Options.** `--csv PATH` (02's output) and `--out PREFIX`. It writes `PREFIX.pdf` and
    `PREFIX.png`, the PDF path built as `Path(args.out + ".pdf")` by concatenation, so that a prefix
    such as `run-2026.6.0` is not cut; `extract_common._save` writes the PNG beside it.
  - **Exit codes.** 0 if both files were written; 1 if `_save` failed; 2 if the run was refused
    (not the repository root, a header that is not `tools.surfing_scan.COLUMNS`, a field that cannot
    be read, an `ok` row whose `captured` is neither `true` nor `false`, a status that is neither
    `ok` nor `failure`, a malformed sidecar, or no rows). A refusal writes nothing.
  - **The root check** is 02's: `CosmologyModels/GenericEOS/Xav_EOS_data.csv` must be a file under
    the working directory (`tools.surfing_scan.EOS_TABLE`).
  - **The functions.** `parse_rows(path)`, `read_sidecar(csv_path)`, `fine_grid(beta_min,
    beta_max)`, `compute_roots(betas) -> RootGrid`, `build_figure(rows, roots, sidecar) -> Figure`
    and `main(argv)`. `RootGrid` is a namedtuple `(betas, records, thresholds)`: `records` holds, per
    β, one dict per kick (`kick`, `T_hot`, `T_cold` in GeV or None, `merged`), hot to cold, and
    `thresholds` maps a kick label to √((2 + Σ_peak)/(6 Σ_peak)).
  - **The figure-building function** is `build_figure(rows: Sequence[dict], roots: RootGrid,
    sidecar: Optional[dict]) -> matplotlib.figure.Figure`. It needs no cosmology, and it is built
    without `pyplot`, as `plot_T_deliver` is. Its artists carry a `gid` (`root-hot-QCD`,
    `merged-cold-QCD+mu-pi`, `marker-capture`, `ratio-delivery`, `edge-failure-upper`,
    `band-label-ee`, `threshold-QCD`, …) so that a test finds them.
  - **The data path in `main`.** Read the rows and the sidecar; `fine_grid`; `compute_roots` (builds
    the cosmology through `tools.surfing_scan.make_setup`, calls `find_kicks(cosmology, _eos._T_min
    × GeV, _eos._T_max × GeV)` once, and `reachability_roots` at each β); `build_figure`; `_save`.
  - **The fine grid.** The CSV's β range is rounded outwards to 0.05 with
    `floor(β_min/0.05 + 1e-9)` and `ceil(β_max/0.05 − 1e-9)`, then built by index with
    `tools.surfing_scan.beta_grid(lo, hi, 0.005)`. T5's CSV gives 1.15–5.00 (771 β), the probe
    scan's 1.10–5.00 (781 β).
- **`tools/tests/test_plot_surfing_temperature.py`** (new): 15 tests, below.

### The figure (F2), as built

- **Two panels** sharing β (A4 width, 8.27 × 9.0 in, `layout="constrained"`), heights 2 : 1.
- **Upper panel**, T_J in GeV on a log axis, with headroom of ×25 above the largest value for the
  threshold labels.
  - **Bands.** Per kick, hot root solid and cold root dashed, with the band between filled at
    alpha 0.18. Kick colours (muted, and none of them a marker colour): EW `#117733`, QCD `#882255`,
    μ/π `#44AA99`, e⁺e⁻ `#999933`.
  - **Merged interval.** One band per merged group (here QCD + μ/π), filled once, from the hotter
    kick's record. Its hot edge is in QCD's colour, its cold edge in μ/π's. The separate lines are
    NaN where `merged` is set, and the merged lines are NaN elsewhere, so nothing joins the two
    sides, and there is no near-vertical segment. A gap of one fine cell (0.005) shows at the
    change.
  - **Labels.** Each band's label hangs below its cold edge at the middle of its widest run: EW,
    QCD, μ/π, e⁺e⁻, and "QCD + μ/π" on the merged run. "reachable, not surfed" is added under the
    label of every kick no row captures (here μ/π and e⁺e⁻), as the brief ruled; QCD and EW do not
    carry it.
  - **Thresholds.** A dotted vertical line at each kick's threshold β in both panels, with a label
    ("QCD 1.107") rotated along the line at the top. A threshold outside the β range is neither
    drawn nor labelled (D8).
- **Markers**, one set for both panels, filled by shape and colour, and drawn only for captured rows
  and only where the column has a value:

  | Series | Marker | Colour | Size | z |
  |---|---|---|---|---|
  | capture | downward triangle | `#E69F00` (orange) | 5.0 | 9 |
  | largest | upward triangle | `#D55E00` (vermilion), white edge | 4.0 | 11 |
  | second dip | open diamond | `#0072B2` (blue), 1.1 pt edge | 4.5 | 10 |
  | delivery | filled circle | black | 6.5 | 8 |

  - **Coincident maximum and delivery** (the wall-type maximum, board §5.3) stay visible by size and
    order: delivery is the larger disc, drawn first, and the largest a smaller orange triangle
    drawn over it. Test h pins the size, the z-order and the two colours.
  - The shapes carry the distinction in greyscale (down, up, open diamond, disc).
- **Rows not drawn.** A not-captured row is a grey vertical tick, and a failure row a red cross, on
  the lower axis edge of each panel (`clip_on=False`, size 14). Each has a legend entry only if the
  file has such a row. A not-captured row's delivery is not drawn (the brief).
- **Lower panel.** Each measured temperature over the row's `T_hot_GeV`, linear axis, line at 1,
  y-limits from the data with 6 % padding. A row with no `T_hot_GeV` has no ratio, and nothing is
  drawn for it.
- **One legend**, below the axes (`fig.legend(loc="outside lower center")`): the four series, the two
  edge marks, hot/cold/threshold line styles and the four kick colours.
- **The title** has φ*, M, Λ and T_stop from the rows (each distinct value, `:g`), then "git HEAD"
  with the 7-digit SHA of every distinct `git_head` in the sidecar's `runs`, in order, each marked
  " + tracked changes" where `git_tracked_changes` is true. T5's CSV reads "f6440d1 + tracked
  changes"; the probe scan's "61e0c42".
- **A missing sidecar** (the agent's choice, D7): the figure is drawn and its title says "HEAD
  unknown", and `main` warns on stderr. A malformed sidecar is a refusal (exit 2).

## Deviations from the prompt

The brief's rulings govern where they differ from the prompt; the first five are those.

| # | Deviation | Tag |
|---|---|---|
| D1 | **A row is classified by `status` first.** A failure row's blank `captured` is neither false nor a parse error; an `ok` row with a `captured` that is neither `true` nor `false` is refused. (The prompt says "rows with `captured` false, and failure rows".) | STRUCTURALLY REQUIRED (brief) |
| D2 | **Uncaptured rows' deliveries are not drawn** as markers; the row is only the edge mark. | STRUCTURALLY REQUIRED (brief) |
| D3 | **The title gives every distinct `git_head`, with the tracked-change mark**, not "the git `HEAD` from the sidecar". | STRUCTURALLY REQUIRED (brief) |
| D4 | **The merged band is filled once, and the lines are broken where `merged` changes.** "Reachable, not surfed" is on every kick no row captures, not only e⁺e⁻. | STRUCTURALLY REQUIRED (brief) |
| D5 | **The β range is rounded outwards with a tolerance** (`1e-9`), and built by `beta_grid`. F3.4 uses a narrow range of its own (β = 1.15 and 1.20, 11 calls). | STRUCTURALLY REQUIRED (brief) |
| D6 | **`build_figure` takes a `RootGrid`** (the fine grid, the per-β records in GeV, the thresholds), and the conversion to GeV and the cosmology live in `compute_roots`. The prompt says "the rows, the roots and the sidecar". | IMPLEMENTATION CHOICE |
| D7 | **Missing sidecar: draw, with "HEAD unknown" in the title and a stderr warning;** the header must equal `COLUMNS` (refused otherwise). | IMPLEMENTATION CHOICE |
| D8 | **A threshold off the β axis is not drawn or labelled.** A label placed off the axes made constrained layout collapse the axes (a warning, in the narrow-range test). On the scan, T5's QCD threshold (1.107) is left of its 1.15 axis, so T5's figure shows three. | IMPLEMENTATION CHOICE |
| D9 | **Edge marks are on both panels**, the legend entry only if such a row exists; the prompt says "the lower axis edge". The x-limits of the lower panel are padded by 1 % of the range, so a marker on the end β is not cut. | IMPLEMENTATION CHOICE |
| D10 | **Band labels hang below the cold edge** (middle of the widest run), the merged run is labelled "QCD + μ/π", and the threshold labels are rotated at the top, in ×25 headroom. Chosen after looking at the first rendering, where labels at the band's middle sat among the markers. | IMPLEMENTATION CHOICE |
| D11 | **More tests than the prompt lists** (15, below): the parser, the merged band, the title, the coincident markers, the fine grid, the refusals, the missing sidecar. | IMPLEMENTATION CHOICE |
| D12 | **`tools/plot_surfing_temperature.py` existed (untracked, no tests) while the baseline suites ran,** and a scratch script computing the roots ran concurrently with them, so the baseline timings carry that load. The new test module was held out of `tools/tests/` until the tools baseline had finished, so no count includes it. The baseline counts equal the orchestrator's. The prompt's "before any edit" was not kept to the letter. | UNINTENDED DRIFT |
| D13 | **`extract_common._save` (private) is imported**, as the prompt's idiom says. It writes the PNG beside the PDF. | IMPLEMENTATION CHOICE |

## Verification performed

### Counts (serially, in the foreground; load from `uptime` at the start of each run)

| Suite | Before (`9d6bbaa`, code of `61e0c42`) | After (this commit's tree) |
|---|---|---|
| CosmologyModels | 18 OK, 66.2 s, load 11.6 | 18 OK, 66.2 s, load 5.4 |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 97.3 s, load 7.0 | 61 OK, 103.1 s, load 7.2 |
| ComputeTargets `test_[g-z]*.py` | 115 OK, 178.0 s, load 5.0 | 115 OK, 133.5 s, load 4.7 |
| ObjectFactories | 69 OK, 8.8 s, load 20.8 | 69 OK, 5.9 s, load 7.2 |
| tools | 49 OK, 13.8 s, load 20.9 | **64 OK**, 11.9 s, load 6.4 |

- The counts equal the orchestrator's baseline. tools is 49 plus the 15 new tests. No count fell,
  and no failure needed a re-run.
- The commands are `CLAUDE.md`'s, ComputeTargets in its two halves; each run's output went to a file
  in the scratch directory, and the verdict was grepped from it.
- `black --check tools/plot_surfing_temperature.py tools/tests/test_plot_surfing_temperature.py`:
  `2 files would be left unchanged.`
- No tracked file is modified: `git status` shows only the two new files among tracked-or-new paths
  (besides the user's untracked set).

### The new tests, by class

`PYTHONPATH=. ./venv/bin/python -m unittest discover -s tools/tests -t . -p 'test_plot_surfing_temperature.py'`:
**15 tests, 2.3 s (load 14 at the start), OK.** No Ray, no store, no history, no PRyMordial.

| Class | Tests | What it pins |
|---|---|---|
| `TestParse` | 2 | the failure row's blank `captured` is read as a failure and not as false, and an ok row with a blank `captured` is refused; a foreign header is refused |
| `TestBuildFigure` | 8 (a–h) | **(a)** one marker per existing value per series, none at a blank, no zero, none for the uncaptured row's delivery, and the lower panel has the same β; **(b)** the lower panel's ratios equal the CSV's quotients to 1e-12; **(c)** the not-captured and failure rows have edge marks on both panels and legend entries, and no entry where the file has none; **(d)** roots drawn on the stub grid: x equals the grid, y equals the stub's roots and NaN where merged or absent, thresholds labelled with their values; **(e)** the merged band is one: hot edge in QCD's colour, cold in μ/π's, NaN off the merge, no second copy for the other kick, the separate lines NaN at merged β, five fills; **(f)** "reachable, not surfed" on e⁺e⁻ and μ/π, not on QCD or EW; **(g)** the title names both SHAs, marks only the run with tracked changes, and says "HEAD unknown" with no sidecar; **(h)** delivery is the larger disc drawn under the largest, with different colours, and the second dip is open |
| `TestFineGrid` | 1 | 1.15–5.0 gives 771 values from 1.15 (not 1.10); 1.10–5.0 gives 781; 1.17–1.23 gives 1.15–1.25; one β is one |
| `TestMain` | 3 | with `make_setup`, `find_kicks` and `reachability_roots` stubbed (the stub works in units where GeV = 2): exit 0, the directory gains exactly `run-2026.6.0.pdf` and `.png`, both non-empty, the stub is called 561 times from 1.2 to 4.0; refused outside the root and on a foreign header (exit 2, nothing written); no sidecar gives exit 0, a warning and "HEAD unknown" |
| `TestFineGridCalls` | 1 | **builds the production cosmology**, runs the real `find_kicks` (once) and `reachability_roots` (11 calls, counted with wrappers) on a two-row CSV, β = 1.15 and 1.20: the calls are at 1.15 … 1.20 in steps of 0.005, and both files are written. 1.3 s alone, at load 15 |

### Step 3: the figure from both CSVs (not committed)

The command, from the repository root, in the foreground:

```
./venv/bin/python tools/plot_surfing_temperature.py --csv <scratch>/inputs/surfing_t5.csv --out <scratch>/main_out/surfing_t5
./venv/bin/python tools/plot_surfing_temperature.py --csv <scratch>/inputs/scan79.csv --out <scratch>/main_out/scan79
```

with `<scratch>` the agent's scratch directory. Both exit 0.

| CSV | Rows | Roots on | `main` took | Load (start, end) |
|---|---|---|---|---|
| `surfing_t5.csv` | 5 | 771 β, 1.15–5.00 | **52 s** (51 s in the roots) | 12.7, 7.7 |
| `scan79.csv` | 79 | 781 β, 1.1–5 | **60 s** | 7.4, 5.7 |

That is 0.066–0.077 s per β, about the board's §5.2 figure, and well below the orchestrator's probe's
0.245 s at load 8.

**`scan79.png` (the legibility judgement).**
- **Upper panel.** Three bands are visible above 1 keV: EW (3.01–5, 18–176 GeV), QCD (the hot edge
  rising from 0.19 to 4.3 GeV, the cold edge falling from 0.18 to 0.016 GeV, a narrower band left of
  2.15) and the separate μ/π sliver (2.02–2.155, at 0.04–0.07 GeV) that joins QCD in one merged band;
  below, the e⁺e⁻ band (1.86–5, 6e-5 to 9e-4 GeV). The QCD band's fill and lines break cleanly at the
  merge, with a thin white gap at β = 2.15 and no vertical segment. The merged band is filled once
  (a single wine fill, not a double-density one). The four thresholds are labelled in the headroom
  without colliding: QCD 1.107 is at the left edge and readable, e⁺e⁻ 1.864 and μ/π 2.018 are
  0.15 apart and clear, EW 3.012.
- **Markers.** All 77 captured β appear: the capture triangles, the largest triangles and the
  deliveries lie along the hot edge, within about 20 % of each other on the log axis, so the
  three-series stack is a thick ribbon; that is what the lower panel is for. Open diamonds are seen
  for the second dips (2.6–5.0). The edge ticks at β = 1.10 and 3.00 are visible on both panels.
- **Coincident maximum and delivery** (β = 1.15–1.65 and 3.05): the orange up-triangle sits on the
  larger black disc, and both are visible, at 77 points on the 3.9-wide axis.
- **Lower panel.** Spans 0.78–1.07. Capture sits at 0.78–0.91, the largest spans 0.965–1.067 and rises with β, delivery and the second dip lie at 0.985–1.02 and overlap each other near 1 (the dip's
  open diamond is visible around the disc). The jump at the EW threshold is plain, as is the
  isolated delivery at 2.95 (1.019).
- **What is hard to read.**
  - In the upper panel the three series are not separable by value; they are separable only by
    shape, and the ribbon hides the hot root's line along its length.
  - Delivery and the second dip overlap near 1 in the lower panel where they differ by less than
    1.5 %; shape and colour still tell them apart.
  - The e⁺e⁻ label sits near the bottom edge (it hangs below the cold edge, 3e-5 GeV, above the axis
    limit).
  - None of this is a stop. The figure is legible at A4 width.

**`surfing_t5.png`.** The five rows, as isolated markers, with the roots across the 1.15–5.00 axis.
The QCD threshold (1.107) is off the axis and is not drawn. The five points sit on the hot edges:
QCD at 1.15, 1.5 and 2.5, EW at 3.5 and 5.0, with the second dip (open diamond) at the last two. The
title reads "git HEAD f6440d1 + tracked changes". Nothing is hard to read. The end markers are not
cut (the 1 % padding).

## The deliberate-breakage record

- **How the diffs were made.** Each is against the final tree, made with `diff -u --label a/<path>
  --label b/<path>`, and checked in `bash` with `git apply --check` (forward, on the clean tree) and
  `git apply -R --check` (after applying). They were applied, `test_plot_surfing_temperature.py`
  was run, and they were reversed; `shasum` of the two files was equal before and after the three
  replays. Each replays with `git apply` from a clean tree at this commit.
- **The command:**
  `PYTHONPATH=. ./venv/bin/python -m unittest discover -s tools/tests -t . -p 'test_plot_surfing_temperature.py'`.
- None was committed.

**1. Blanks drawn as zeros.** 2 of 15 fail:
`TestBuildFigure.test_a_one_marker_per_existing_value_and_none_at_a_blank` and
`TestBuildFigure.test_b_ratios_equal_the_quotients_of_the_csv`.
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -425,9 +425,8 @@
     handles: List = []
     for column, key, text, marker, c, filled, size, z in SERIES:
         pts = [
-            (r["beta"], r[column], r["T_hot_GeV"])
+            (r["beta"], r[column] or 0.0, r["T_hot_GeV"])
             for r in captured
-            if r[column] is not None
         ]
         style = dict(
             linestyle="none",
```

**2. The ratio taken against `T_cold_GeV`.** 1 of 15 fails:
`TestBuildFigure.test_b_ratios_equal_the_quotients_of_the_csv`.
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -425,7 +425,7 @@
     handles: List = []
     for column, key, text, marker, c, filled, size, z in SERIES:
         pts = [
-            (r["beta"], r[column], r["T_hot_GeV"])
+            (r["beta"], r[column], r["T_cold_GeV"])
             for r in captured
             if r[column] is not None
         ]
```

**3. The not-captured rows dropped silently** (no edge mark and no legend entry). 1 of 15 fails:
`TestBuildFigure.test_c_rows_not_drawn_have_edge_marks_and_legend_entries`.
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -454,7 +454,7 @@
         (UNCAPTURED, "uncaptured", "not captured", "|", "0.25"),
         (FAILURE, "failure", "failure", "x", "#CC3311"),
     ):
-        xs = [r["beta"] for r in rows if r["kind"] == kind]
+        xs = [r["beta"] for r in rows if r["kind"] == kind and kind != UNCAPTURED]
         if not xs:
             continue
         style = dict(
```

## Observations not acted on

- **The roots are not stored with the CSV.** `main` recomputes them on every run (about one minute on
  the full range here), so a re-plot of an unchanged CSV repeats that work. Nothing to do now.
- **The three series are not separable by value on the upper panel** at this scan's range of 6.5
  decades. The shapes and the lower panel carry the information, as the prompt intended.
- **`reachability_roots` took 0.066–0.077 s per β** here at load 6–13, against the orchestrator's
  probe's 0.245 s at load 8.
- **The merge leaves a one-cell gap (0.005) in the QCD lines and fill** (D4, as ruled). Across it
  the QCD cold root goes from 76.7 to 32.4 MeV.
- **The scratch roots computation and the baseline overlapped** (D12).
- No issue opened, and `.documents/OPEN_ISSUES.md` is untouched. Nothing was written outside the
  agent's scratch directory, `tools/`, and the board and log.

## State handed to the next prompt

- **The branch.** `surfing-temperature`, this commit on top of `9d6bbaa`. Nothing is staged or
  modified after it. The untracked entries are the user's, as before.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 64.
- **The command for 04**, from the repository root:
  `./venv/bin/python tools/plot_surfing_temperature.py --csv PATH.csv --out PREFIX`
  with the sidecar `PATH.csv.meta.json` beside the CSV. It writes `PREFIX.pdf` and `PREFIX.png`,
  takes about one minute for the whole grid (the roots at 781 β), prints one `roots:` progress line
  per 100 β, starts no Ray and opens no store. Exit 0 on success, 1 if saving failed, 2 if it was
  refused.
- **The title** carries every distinct `git_head` of the sidecar's runs (7 digits, with " + tracked
  changes" for a run made on a modified tree). A scan run on a committed tree reads "git HEAD
  <sha>". If the CSV has no sidecar the title says "HEAD unknown".
- **The figure on the probe scan** is legible (step 3 above). Nothing in
  `plot_surfing_temperature.py` needs to change for the real scan, and 04 may not change it.
- **The probe's rows are the shape of the real scan's:** QCD captures at β = 1.15–2.95, EW at
  3.05–5.00, uncaptured at 1.10 and 3.00, no failure, no reflection. If the real scan differs, the
  figure handles it: a failure row is a cross on the lower edge, and a kick that a row captures
  loses its "reachable, not surfed" label. `deliver_reflected` is not drawn differently.
- **No issue opened;** `.documents/OPEN_ISSUES.md` is untouched.
- **The breakage diffs** are in this log, and replay from a clean tree at this commit.
