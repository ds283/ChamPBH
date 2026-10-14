# Prompt 04b — move the e⁺e⁻ band label clear of the edge marks

**Campaign:** [`README.md`](README.md) · **Board item:** **F5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, after 04's review, for the user's ruling **U8** (README §6.1). Its
orchestrator note governs where it differs from this file.
**Gate:**
- 04 landed (`90d44f8`) and reviewed (`25cdd2e`);
- on branch `surfing-temperature`, no tracked change in `git status` (the untracked set is 01's gate
  list);
- you start no Ray, and open no store.

**Closes:** nothing. **Opens:** nothing expected.
**Recommended model:** **Sonnet**. A cosmetic change to one figure.

**This is a tidy-up, and it is costed as one.** It runs no history, and only the `tools` suite. It
adds no test and no deliberate breakage. Do not extend it.

**Read first:**

1. The board's §5.5: 04's record and its review.
2. Log 04, "Observations not acted on", first item: where the label sits and what it crosses.
3. `tools/plot_surfing_temperature.py`. Read `label_band` (about `:390-425`), the edge marks (about
   `:480-505`) and the upper y-limits (`:520`).
4. The two committed figures, `.documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3.png`
   and `…_near3.png`.

---

## 1. What is wanted

**Why.** A band label hangs just below its band's cold edge. The coldest band is e⁺e⁻, and its cold
edge sets the upper panel's lower y-limit (`min(ys) / 3.0`), so its label sits on the bottom spine.
The edge marks for uncaptured rows are drawn there too: on the axis at y = 0, size 14, `clip_on=False`,
z-order 20. In 04's fine figure, the label's second line, "reachable, not surfed", lies across the
marks at β = 2.999–3.006 and is hard to read. No value is hidden. The user ruled that it is tidied
(U8).

### 1.1 The item

- **F5 — no band label overlaps an edge mark.** In both committed figures, every band label is
  clear of the strip at the bottom of the upper panel where the edge marks are drawn, and is
  readable.
  - **The suggested way.** Leave more room below the coldest band: a larger divisor in the lower
    y-limit at `:520`, chosen so that the e⁺e⁻ label clears the marks' tops on both figures, with
    a short comment saying why. The agent may do it another way if that is simpler, and says why
    in the log.
  - **Not allowed:**
    - an opaque background behind the label, since it would hide marks;
    - moving, shrinking or restyling the edge marks;
    - changing the label's words, colour, or the band it labels.
- **The two committed figures are redrawn in place,** from the committed CSVs, with
  `tools/plot_surfing_temperature.py --csv <CSV> --out <prefix>` as in the verification document
  §1.1. That writes the PDF and PNG for each. The CSVs and sidecars are not touched, and the title
  still names `e7ed715`, the tree the data came from.
- **`.documents/surfing-temperature-verification.md` gets a new §2**, "The figures redrawn (prompt
  04b, <date>)". It is a few lines: what moved and why, that the data are unchanged, and that
  §1.6's account of the overlap describes the figures as committed in `90d44f8`. §1 is not edited
  (`CLAUDE.md` invariant 6).

### 1.2 What must not move

- Every test in `tools/tests/test_plot_surfing_temperature.py` (03's and 03b's) passes unedited.
- Everything else the figure draws is unchanged: the bands, roots, thresholds, series, rings, edge
  marks, the lower panel and the title. Only the upper panel's lower limit, or the label's position,
  moves.
- The script's options, outputs and exit codes.

---

## 2. Verification

Keep it to this.
1. **After the change:** `tools/tests` only, at 70. `black --check` on the script.
2. **The figures.** Redraw both, look at each PNG, and say in the log:
   - where the e⁺e⁻ label now sits, and that it is clear of every edge mark in the fine figure;
   - that nothing else visibly changed.
3. **The data.** `git diff --stat` shows no change to the CSVs or sidecars.

The other suites are not run. The change touches one plotting module, which only its own tests in `tools/tests` import.

## 3. Acceptance

1. F5 as §1.1 says, with §1.2 holding.
2. **One commit** containing:
   - the script;
   - the four redrawn figures (two PDF, two PNG);
   - the verification document's new §2;
   - the log `logs/04b-tidy-band-label.md`, short, in the template of `dk-and-rpk-migration` README
     §5.1;
   - the board: row 04b, F5, and a new `### 5.5a After prompt 04b (<date>)`.

   Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- A test in `tools/tests` fails.
- No placement clears the marks without hiding something, or without changing more than §1.2
  allows.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:** `tools/plot_surfing_temperature.py`; the four figures under
  `.documents/surfing-temperature/out/`; the verification document (a new §2 only); this board;
  the log.
- It does not touch the CSVs, sidecars, tests, `extract_common.py` or `tools/surfing_scan.py`.
- It runs no history, starts no Ray, opens no store, and runs no suite but `tools`.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
