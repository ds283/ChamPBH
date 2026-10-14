# Prompt 03b — draw the first bounce of uncaptured histories

**Campaign:** [`README.md`](README.md) · **Board item:** **F4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, after 03's review, for the user's ruling **U7** (README §6.1). Its
orchestrator note governs where it differs from this file.
**Gate:**
- 03 landed (`1f40b53`) and reviewed (`9ab3e65`); U6 recorded (`c4c38bc`);
- on branch `surfing-temperature`, no tracked change in `git status` (the untracked set is 01's gate
  list);
- you start no Ray, and open no store.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One more series in an existing figure, and its tests.

**Read first:**

1. [`README.md`](README.md): §4 (the figure), §6.1 (U6, U7).
2. The board's §5.4 (03's review, and the near-miss at β = 3.00) and §5.4a (U6).
3. Log 03 in full, and `tools/plot_surfing_temperature.py` and
   `tools/tests/test_plot_surfing_temperature.py` (03).

---

## 1. What is wanted

**Why.** Note 03 ruled that an uncaptured row is drawn only as an edge mark, so its first bounce is
not plotted. Near β = 3.0 that hides a measurement the user wants to see. The field nearly surfs on
the EW kick and is captured by neither kick, and across that window the first bounce rises from
about 1.6 GeV to about 44 GeV (board §5.4a). 04 may not change the plot script, so the change is
made here, before 04.

### 1.1 The item

- **F4 — the first bounce of an uncaptured history**, in the upper panel only.
  - **What is drawn.** For each row with `kind` "not captured" (`status=ok`, `captured=false`)
    whose `T_deliver_GeV` is not blank, one marker at (β, `T_deliver_GeV`).
  - **Its style.** An open circle in a mid grey, labelled "first bounce, not captured" in the
    legend. It must be told apart from the filled black delivery disc in colour and in greyscale,
    by its fill. Its legend entry appears only if such a point exists.
  - **The lower panel is unchanged.** An uncaptured row has no `T_hot_GeV`, so it has no ratio.
  - **The edge mark stays.** It still marks the row in both panels, and its legend entry stays.
  - **The upper axis limits** include the new points.
  - **Failure rows are unchanged.** They are an edge mark only, since a failure row has no
    delivery.
  - **Its own artist**, with its own `gid` (for example `marker-delivery-uncaptured`). The captured
    delivery series (`marker-delivery`) is unchanged and still holds captured rows only.
  - **The module docstring** says what the new series is.

### 1.2 What must not move

- **03's 15 tests pass unchanged.** README §5 rule 13 applies to them too. If one fails, that is
  a stop (§4), not a reason to edit it.
- Everything else the figure draws is unchanged: bands, roots, thresholds, the four series, the
  ratios, the title.
- The options, the outputs and the exit codes of `main` are unchanged.

---

## 2. Verification

1. **Before any edit:** the four roots at the board's counts, serially, with the load.
2. **New tests**, in the existing module as a new class (03's classes are not edited), on 03's
   synthetic CSV idiom:
   - an uncaptured row with a delivery gets exactly one new marker, at its (β, `T_deliver_GeV`),
     in the upper panel;
   - no new marker in the lower panel, and none for a captured or failure row;
   - an uncaptured row with a blank `T_deliver_GeV` gets no marker and does not crash the plot;
   - the legend entry appears if and only if such a point exists;
   - `marker-delivery` still excludes every uncaptured β;
   - the upper y-limits contain every new point.
3. **After:** the four suites (tools at 64 + the new tests, the others unchanged); `black --check`
   on both touched files.
4. **Figures**, in your scratch directory, not committed. Look at each PNG and record in the log
   what it shows:
   - `scan79.csv` (79 rows; uncaptured at 1.10 and 3.00);
   - `edges.csv` (14 rows near β = 3.0, ten of them captured and four uncaptured). The rise of the
     first bounce across the uncaptured window must be visible.
5. **Deliberate breakages** (each a diff, never committed):
   - the uncaptured deliveries drawn in the captured delivery series' style and artist;
   - the uncaptured deliveries drawn in the lower panel as well;
   - the uncaptured deliveries dropped.

   Each must fail at least one new test, named in the log.

## 3. Acceptance

1. F4 as §1.1 says, with §1.2 holding.
2. §2's checks, each with its command and output in the log.
3. Suites: CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 69, tools 64 + new.
4. **One commit**, with the log `logs/03b-plot-uncaptured-deliveries.md` in the template of
   `dk-and-rpk-migration` README §5.1. It updates the board: row 03b, F4, and a new
   `### 5.4b After prompt 03b (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- One of 03's tests fails after the change.
- The new series makes the figure illegible on either CSV of §2 step 4. Propose an alternative in
  the log, and stop before committing.
- An existing test fails, or anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `tools/plot_surfing_temperature.py`;
  - `tools/tests/test_plot_surfing_temperature.py` (a new class only);
  - this board, and the log.
- It does not change `extract_common.py`, `tools/surfing_scan.py` or any production module.
- It runs no history, starts no Ray, and opens no store.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
