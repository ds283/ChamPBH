# Log — surfing-temperature 04b: move the e⁺e⁻ band label clear of the edge marks

**Subject:** F5, the user's ruling U8. **Commit:** this commit · **Date:** 2026-10-14 · **Branch:**
`surfing-temperature`, parent `f30a3c0` · **Model:** Claude Sonnet 5.5 · **Result:** landed. No
stop condition fired.

## What shipped

- **`tools/plot_surfing_temperature.py`**: the upper panel's lower y-limit, `min(ys) / 3.0`, is now
  `min(ys) / 8.0`, with a two-line comment saying why. Nothing else in the file.
- **The four figures** under `.documents/surfing-temperature/out/` (two PDF, two PNG), redrawn from
  the committed CSVs by the verification document §1.1's two plot commands (full figure 61 s, fine 2 s,
  load about 3.7 at the start).
- **`.documents/surfing-temperature-verification.md`**: a new §2. §1 is untouched.
- **This log**, and the board: row 04b, F5, and §5.5a.

**The divisor is 8**, the smallest the note's table allows ("at least 8"): the e⁺e⁻ label then clears
by about 7.5 pt on its window extent (note's measure), and the axis reaches just below 10⁻⁵. No simpler
placement was found: moving the label itself would need a change to `label_band`, which also places the
other bands' labels.

**The gap, on the fine figure,** measured on the redrawn PNG (100 dpi, 1 px = 0.72 pt): the lowest ink
of the label ("reachable, not surfed", its comma) is at pixel row 496, and the top of the edge marks at
row 509. That is 12 blank rows, about 9 pt, clear. (The note's window-extent measure includes the text
box's descent, which is why it is the smaller number.) The label's span, 2.999–3.007, covers nine marks.

## Deviations from the prompt

None, except the observation below.

| # | Deviation | Tag |
|---|---|---|
| D1 | The fine PNG's lower panel differs from the old one by sub-pixel anti-aliasing amounts (below). The note expected only the upper panel to differ. A redraw of the committed CSV by the *old* script reproduces the committed PNG exactly, so this is the change's doing, not noise. The cause is `layout="constrained"`: the new 10⁻⁵ tick label changes the upper panel's tick-label extent, and the lower panel shifts by a fraction of a pixel. I did not chase it further. | STRUCTURALLY REQUIRED |

## Verification performed

- **`tools` suite**, after the change: 70 tests, OK, 12.6 s, load averages 3.52 / 6.55 / 7.78 at the
  start (`uptime`). The count is the baseline's. `black` on the script: left unchanged.
- **The figures, looked at.**
  - *Fine:* the e⁺e⁻ label ("e⁺e⁻ / reachable, not surfed") sits in the band's lower edge region, with a
    visible empty strip above the edge marks (β = 2.999–3.019 on the spine). The marks, rings, bands,
    thresholds and the lower panel look as before.
  - *Full:* the label sits above the spine. The two edge marks (1.10, 3.00) are at the left and in the
    middle, clear of it. Nothing else visibly changed.
- **Pixel difference against the committed PNGs** (copies kept in scratch), as bounding boxes
  (x0, y0, x1, y1) of 827 × 900:
  - *Full:* (31, 55, 819, 520), inside the upper panel (note's probe box: x 31–823, y 49–521).
  - *Fine:* (31, 46, 825, 728). Rows 46–528 are the upper panel. Below it, small differences at
    rows 545–727 (inside the lower panel's region), with no
    shift of the panel's frame: its left spine is at column 72 in both, and the best alignment by
    whole-pixel shift is none. See D1.
- **The data.** `git diff --stat` shows no change to the CSVs or sidecars.
- Other suites: not run, as the prompt says.

## The deliberate-breakage record

None, as the prompt says.

## Observations not acted on

- None needing action. The fine PNG's lower panel moving by a sub-pixel amount (D1) is a layout
  consequence and is recorded here only.

## State handed to the next prompt

- **The branch.** `surfing-temperature`, this commit on top of `f30a3c0`. Nothing is staged or modified
  after it. The untracked entries are the user's, as before: 17 `pilot-2026.6.0*.db`, `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 70.
  Unchanged; only `tools` was run.
- **The figures** under `.documents/surfing-temperature/out/` are redrawn with the upper panel's
  lower limit at `min(ys) / 8.0`. The CSVs and sidecars are as in `90d44f8`. The verification document
  has §1 and a new §2.
- **F5 holds.** The e⁺e⁻ label is clear of the edge marks in both figures. The "For the user" note
  in log 04 about the overlap is answered; §1.6 of the verification document still describes the
  `90d44f8` figures, and §2 says so.
- **No issue opened;** the index is untouched.
