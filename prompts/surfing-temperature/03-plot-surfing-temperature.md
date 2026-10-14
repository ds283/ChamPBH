# Prompt 03 — the plot

**Campaign:** [`README.md`](README.md) · **Board items:** **F1**–**F3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, with 01, 02, 04 and 05, after the user's rulings. Its orchestrator note,
written after 02's review, hands it the CSV header 02 committed and 02's T5 rows. It governs where it
differs from this file.
**Gate:**
- 01 and 02 landed and reviewed, with the gates "Roots" and "Turning points" holding (board §2);
- on branch `surfing-temperature`, no tracked change in `git status` (the untracked set is 01's gate
  list);
- you start no Ray, and open no store.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One figure from one CSV, and its tests.

**Read first:**

1. [`README.md`](README.md): §0.1, §2.2 (03's charter), §4 (the CSV and the figure), §6.2 (P4, P8).
2. Logs 01 and 02: "What shipped" and "State handed to the next prompt". Log 01's production table
   says where the roots lie and whether any interval merges. Log 02's T5 table gives five real rows.
3. `extract_common.py`: `find_kicks` and `reachability_roots` (01); `_save` (`:658-668`) and
   `plot_T_deliver` (`:752-804`), whose idiom this figure follows: a `matplotlib.figure.Figure`
   without `pyplot`, saved through `_save`.
4. `tools/surfing_scan.py` (02): the CSV columns, the sidecar, and how it reads its own CSV.

---

## 1. What is wanted

One figure that makes the roots and the four measured temperatures readable against β (README §4).

### 1.1 The items

- **F1 — `tools/plot_surfing_temperature.py`.**
  - **Its options and outputs.** It takes `--csv` (02's output) and `--out` (a path prefix). It
    writes `<out>.pdf` and `<out>.png` (P8), starts no Ray, and runs from the repository root (the
    EOS reads its table by a relative path; refuse otherwise, as 02's tool does).
  - **The data.** It reads the CSV and its sidecar. It builds the production cosmology, and from it
    `find_kicks` and `reachability_roots` on a fine β grid, Δβ = 0.005, over the CSV's β range
    rounded outwards to 0.05 (P4).
  - **Separation.** The figure is built by a function that takes the rows, the roots and the
    sidecar and returns the `Figure`, so that a test can call it without the cosmology. `main(argv)`
    does the reading and the saving.
- **F2 — the figure**, two panels sharing the β axis, the upper about twice the lower's height.
  - **The upper panel**: T_J in GeV, log axis.
    - **The roots.** For each kick, the hot root as a solid line and the cold root as a dashed line,
      in the kick's colour, with the band between them lightly filled and labelled ("EW", "QCD",
      "e⁺e⁻").
    - **A merged interval** (log 01) is drawn as one band, with its hot and cold edges in the two
      kicks' colours.
    - **The e⁺e⁻ band** is labelled "reachable, not surfed" when no row has `capture_kick == "ee"`.
    - **The thresholds.** A thin vertical dotted line at each kick's threshold β, from 01's
      Σ_peak, labelled with its value.
  - **The measured temperatures.** One marker style each, the same colour in both panels, and
    distinct by shape as well as colour:
    - capture: a downward triangle;
    - largest: an upward triangle;
    - second dip: an open diamond;
    - delivery: a filled circle.

    They are drawn only for captured rows. A blank is a gap, never a zero.
  - **Rows not drawn.** Rows with `captured` false, and failure rows, appear as a small mark on the
    lower axis edge, with a legend entry ("not captured", "failure"), so the reader can see that the
    point exists.
  - **The lower panel**: each measured temperature divided by `T_hot_GeV` of its row, on a linear
    axis, with a horizontal line at 1. Same markers.
  - **Labels.** The axes are β and T_J [GeV], and T_J / T_hot. One legend, outside the data or in
    the emptiest corner. The title gives φ*, M, Λ and T_stop from the rows, and the git `HEAD` from
    the sidecar.
  - **Legibility.** The figure must be legible at A4 width, with colours that survive greyscale and
    the common colour-vision deficiencies. The shapes carry the distinction in any case.
- **F3 — the tests**, `tools/tests/test_plot_surfing_temperature.py`, with no Ray and no store:
  1. **A synthetic CSV** in a `tempfile` directory, in 02's exact header. It has:
     - a not-captured row;
     - a QCD capture with no second dip;
     - a QCD capture with all four temperatures;
     - an EW capture;
     - a failure row.

     Its sidecar is beside it.
  2. **The figure-building function** on those rows, with stub roots:
     - the upper panel has one marker per existing value per series, and no marker at a blank;
     - the lower panel's ratios equal the CSV's quotients to 1e-12;
     - the not-captured and failure rows appear in their legend entries;
     - the roots are drawn on the stub grid.
  3. **`main(argv)`** on the synthetic CSV, with `reachability_roots` and the cosmology
     monkeypatched to stubs. It writes both files, non-empty, and changes no other file in the
     directory.
  4. **The fine grid**: when the cosmology is not stubbed, `reachability_roots` is called at
     Δβ = 0.005 over the rounded range. Count the calls with a wrapper. This test builds the
     production cosmology; say so, and give its time in the docstring.

---

## 2. Verification

1. **Before any edit:** the four roots at the board's counts, serially, with the load.
2. **After:**
   - the four roots: tools + the new tests, the others unchanged;
   - `black --check` on every touched Python file.
3. **A figure from 02's T5 CSV.** If 02's scratch CSV is still in the session scratchpad, plot it.
   Otherwise rebuild the five rows from log 02's table into a scratch CSV. Look at the PNG, and
   record in the log what it shows and anything that is hard to read. The figure is not committed.
   04 makes the real one.
4. **Deliberate breakages** (each a diff, never committed):
   - blanks drawn as zeros;
   - the ratio taken against `T_cold_GeV`;
   - the not-captured rows dropped silently.

   Each must fail at least one new test, named in the log.

## 3. Acceptance

1. F1–F3 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. Suites: CosmologyModels 18, ComputeTargets as left by 02, ObjectFactories 69, tools as left by
   02 + the new tests.
4. **One commit**, with the log `logs/03-plot-surfing-temperature.md` in the template of
   `dk-and-rpk-migration` README §5.1. It updates the board: row 03, F1–F3, and a new
   `### 5.4 After prompt 03 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- **02's CSV lacks something the figure needs.** Do not change 02's tool. Report what is missing.
- **The figure cannot be made legible** with the four series and the root bands together. Propose
  the alternative (for example a separate panel per kick) in the log, and stop before committing.
- An existing test fails, or anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - the new `tools/plot_surfing_temperature.py`;
  - the new test module;
  - this board, and the log.
- It does not change `extract_common.py`, `tools/surfing_scan.py` or any production module.
- It runs no history, starts no Ray, and opens no store.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
