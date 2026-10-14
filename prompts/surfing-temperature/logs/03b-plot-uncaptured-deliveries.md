# Log — surfing-temperature 03b: the first bounce of an uncaptured history

**Subject:** one more series in the figure of `tools/plot_surfing_temperature.py`, the first bounce
of a row that was not captured (F4, ruling U7), and its tests.
**Commit:** `ee4a2df` · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent
`f19332d` · **Model:** Claude Sonnet 5.5 · **Result:** landed. F4 done. No stop condition fired:
03's 15 tests pass unedited, and the figure is legible on both CSVs.

## What shipped

- **`tools/plot_surfing_temperature.py`.** In `build_figure`, after the four-series loop and before
  the edge marks:
  - `uncaptured_deliveries` is `(beta, T_deliver_GeV)` for every row of kind `UNCAPTURED` whose
    `T_deliver_GeV` is not `None`.
  - If it is non-empty, one `ax.plot` (a `Line2D`, not a `scatter`, so that 03's `test_e` count of the
    upper panel's collections is untouched) with gid `marker-delivery-uncaptured`, and one legend
    handle "first bounce, not captured", placed after the four series and before the edge marks.
  - The points are added to `ys`, so the upper y-limits contain them.
  - The lower panel gets nothing. The edge marks, their legend entries and the failure rows are as
    before. The module docstring says what the series is.
- **`tools/tests/test_plot_surfing_temperature.py`.** One new class, `TestUncapturedDeliveries`, at the
  end (6 tests), and one line added to the module docstring's list. 03's classes, helpers and
  constants are not edited (the docstring is not a test).

### The marker

Open circle (`"o"`), size 6.0, no face, edge colour `0.45` (mid grey), edge width 1.3, z-order 7
(drawn at 7 against the captured delivery disc's 8, and under the edge marks' 20).
- **Against the delivery disc** (filled black, size 6.5, z 8): the same shape on purpose (both are a
  "first bounce"), told apart by fill. The disc is solid black, the new one a hollow ring whose
  middle shows the white axes background and the grid, so the difference is the fill, not a hue, and
  survives greyscale (checked, below). The edge colour is lighter than the disc, which is also a
  value difference, but nothing relies on it.
- **Against the other series.** Orange and vermilion triangles and the blue open diamond differ from
  it in shape and colour. The second dip is also an open mark (a diamond, 1.1 pt, blue), so the open
  circle is told from it by shape and, in colour, by being grey.
- **Size and weight.** 6.0 and 1.3 pt make a ring that reads at A4 width without out-shouting the
  disc, and at 3.012 and 3.013 (0.001 apart, 0.1 % of the axis) two adjacent rings are separate.

### `build_figure`, the diff

```diff
diff --git a/tools/plot_surfing_temperature.py b/tools/plot_surfing_temperature.py
index e8d6c8a..77e04af 100644
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -35,8 +35,11 @@ kick's colour and its cold edge in the colder kick's, and every line is broken w
 changes; a dotted vertical line at each kick's threshold beta; and, for captured rows only, the
 four measured temperatures (capture, largest, second dip, delivery). A blank is a gap, never a
 zero. A band no row captures in is labelled "reachable, not surfed". Rows that are not captured,
-and failure rows, are marks on the lower axis edge. The lower panel shows each measured
-temperature divided by `T_hot_GeV` of its row, with a line at 1.
+and failure rows, are marks on the lower axis edge; in addition a row that is not captured but
+has a `T_deliver_GeV` shows its first bounce, in the upper panel only, as an open grey circle
+("first bounce, not captured", gid `marker-delivery-uncaptured`), distinct from the filled black
+delivery disc of a captured row (prompt 03b, F4). It has no `T_hot_GeV`, so no ratio. The lower
+panel shows each measured temperature divided by `T_hot_GeV` of its row, with a line at 1.
 
 A row is classified by `status` first: a failure row has every other field blank, `captured`
 included, and a blank `captured` is read as false only where it has no meaning to read.
@@ -449,6 +452,34 @@ def build_figure(
         lower.set_gid(f"ratio-{key}")
         handles.append(Line2D([], [], label=text, **{**style, "zorder": 1}))
 
+    # ---- the first bounce of a row that was not captured (F4, prompt 03b): an open grey circle,
+    # upper panel only. It has no T_hot, so no ratio. It is its own artist, and the captured
+    # delivery series above is unchanged.
+    uncaptured_deliveries = [
+        (r["beta"], r["T_deliver_GeV"])
+        for r in rows
+        if r["kind"] == UNCAPTURED and r["T_deliver_GeV"] is not None
+    ]
+    if uncaptured_deliveries:
+        style = dict(
+            linestyle="none",
+            marker="o",
+            markersize=6.0,
+            markerfacecolor="none",
+            markeredgecolor="0.45",
+            markeredgewidth=1.3,
+            zorder=7,
+        )
+        (bounce,) = ax.plot(
+            [p[0] for p in uncaptured_deliveries],
+            [p[1] for p in uncaptured_deliveries],
+            **style,
+        )
+        bounce.set_gid("marker-delivery-uncaptured")
+        handles.append(
+            Line2D([], [], label="first bounce, not captured", **{**style, "zorder": 1})
+        )
+
     # ---- rows not drawn as measurements: marks on the lower axis edge
     for kind, key, text, marker, c in (
         (UNCAPTURED, "uncaptured", "not captured", "|", "0.25"),
@@ -484,6 +515,7 @@ def build_figure(
         if np.isfinite(v)
     ]
     ys += [r[c[0]] for r in captured for c in SERIES if r[c[0]] is not None]
+    ys += [T for _, T in uncaptured_deliveries]
     if ys:
         ax.set_ylim(min(ys) / 3.0, max(ys) * 25.0)  # headroom for the threshold labels
     rx.axhline(1.0, color="k", linewidth=0.8)
```

## Deviations from the prompt

| # | Deviation | Tag |
|---|---|---|
| D1 | **Six new tests, not an exact count the prompt gives:** its six bullets are one test each (a: one marker at the `(beta, T_deliver_GeV)` and none in the lower panel; b: none for a captured or a failure row; c: a blank delivery; d: the legend iff; e: `marker-delivery` excludes uncaptured β; f: the limits). | IMPLEMENTATION CHOICE |
| D2 | **One line added to the test module's docstring** (the list of what the module covers), as the note allows. | IMPLEMENTATION CHOICE |
| D3 | **`black` reformatted both files** on first run (it re-wrapped my additions only; `git diff` shows no change to a line I did not write). | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT. The baselines were run before any edit, serially.

## Verification performed

### Counts (serially, in the foreground; output in the scratch directory; load from `uptime` at the start)

| Suite | Before (`f19332d`) | After |
|---|---|---|
| CosmologyModels | 18 OK, 81.9 s, load 6.9 | 18 OK, 65.3 s, load 5.9 |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 124.7 s, load 6.6 | 61 OK, 96.3 s, load 5.5 |
| ComputeTargets `test_[g-z]*.py` | 115 OK, 105.0 s, load 9.1 | 115 OK, 104.5 s, load 3.2 |
| ObjectFactories | 69 OK, 5.7 s, load 6.2 | 69 OK, 5.1 s, load 5.9 |
| tools | 64 OK, 11.6 s, load 5.8 | **70 OK**, 10.9 s, load 6.6 |

- The counts equal the orchestrator's baseline; tools is 64 plus the 6 new tests. No count fell, no
  failure needed a re-run.
- The new class alone: `PYTHONPATH=. ./venv/bin/python -m unittest tools.tests.test_plot_surfing_temperature`:
  **21 tests (15 + 6), 2.8 s, OK.** No Ray, no store, no history, no PRyMordial.
- `black --check` on both touched files: `2 files would be left unchanged.`

### The new tests

| Test | What it pins |
|---|---|
| `test_a_one_marker_per_uncaptured_delivery_in_the_upper_panel_only` | with two extra uncaptured rows, the series has exactly the three (β, T) pairs, in row order; the lower panel has no such artist and only its `edge-uncaptured-lower`; the marker is `"o"` with no face |
| `test_b_no_marker_for_a_captured_or_failure_row` | on the synthetic rows the series is just β = 1.2; with only captured and failure rows there is no series |
| `test_c_a_blank_delivery_gets_no_marker_and_no_crash` | a blank delivery is absent from the series, its edge mark stays, and with only blank uncaptured rows there is no series and no legend entry |
| `test_d_the_legend_entry_appears_if_and_only_if_the_series_does` | the entry appears once when the series exists and not when it does not; the "not captured" edge entry stays |
| `test_e_marker_delivery_still_holds_captured_rows_only` | `marker-delivery` is exactly the captured β |
| `test_f_the_upper_limits_contain_every_new_point` | with deliveries at 1e6 and 1e-9 GeV, both lie strictly inside the upper y-limits |

### Step 4: the figures (scratch, not committed)

Commands, from the repository root, both exit 0:

```
./venv/bin/python tools/plot_surfing_temperature.py --csv <scratch>/inputs/edges.csv  --out <scratch>/fig/edges    (3.7 s)
./venv/bin/python tools/plot_surfing_temperature.py --csv <scratch>/inputs/scan79.csv --out <scratch>/fig/scan79   (53.9 s)
```

with `<scratch>` = `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/efa1eece-841e-465e-b842-b383d743cb82/scratchpad/agent03b`.
Greyscale copies (`PIL` `convert("L")`): `fig/edges_grey.png`, `fig/scan79_grey.png`.

**`fig/edges.png` (14 rows near β = 3.0).**
- Four rings are drawn: one at β = 2.999 (1.64 GeV), three at 3.012, 3.013 and 3.015 (39.2, 41.0 and
  44.1 GeV). They sit directly on the line of the captured deliveries: the QCD deliveries fall from
  1.8 to 1.64 GeV at β ≤ 2.997, the ring at 2.999 continues that, the jump to 39 GeV at 3.012 is
  plain, and the EW deliveries begin at 50 GeV at 3.020. So the rise of the first bounce across the
  uncaptured window is visible, which is what U7 asked for.
- 3.012 and 3.013, 0.001 apart (0.1 % of this 0.1-wide axis), are two rings that overlap slightly
  (checked on a 4x crop, `fig/edges_zoom.png`); both stay countable, and neither hides the other's
  centre. 3.015 is clear of them. The EW threshold line (3.012) passes through the first.
- The ring at 2.999 is in the QCD band's top, at 1.64 GeV, clear of every filled marker (the nearest, at
  2.997, is 0.002 away, and a disc with a triangle is easily told from a hollow ring).
- The lower panel is unchanged: the four uncaptured edge ticks only, and no ratio for them.
- Legend: "first bounce, not captured" (open grey circle) sits beside "not captured" (the tick).

**`fig/scan79.png` (79 rows).**
- Two rings: β = 1.10 at 0.143 GeV, left of the first captured disc (at 1.15, 0.2 GeV) and below it,
  and β = 3.00 at 1.68 GeV, just left of the EW band's first disc and just above the last QCD one. Both
  are visible and unobstructed, and neither sits on a filled marker.
- Everything else is as in 03's reading of this CSV: the ribbon along the hot edge, the thresholds'
  labels, the bands, the lower panel.

**Greyscale (both).** The rings are a mid grey hollow circle and the deliveries a solid black disc,
so they stay different by fill. The hollow ring against the grey QCD band fill is lower contrast than
in colour, but the white centre and the 1.3 pt edge keep it readable. The orange and vermilion
triangles become grey fills of different value; as in 03, the shapes carry them.

**What is hard to read.** Nothing that this change makes hard. In `edges.csv`, the label of the e⁺e⁻
band ("e⁺e⁻ / reachable, not surfed") overlaps the uncaptured edge mark at β = 2.999 (see below).

### The deliberate-breakage record

- **How the diffs were made.** Each is against the tree of this commit, made with `diff -u --label
  a/<path> --label b/<path>`, and checked in `bash` with `git apply --check` (on the clean tree)
  and `git apply -R --check` (after applying). Each was applied, the test module run, and reversed;
  the tree was clean after each. None was committed.
- **The command:** `PYTHONPATH=. ./venv/bin/python -m unittest tools.tests.test_plot_surfing_temperature`.

**1. The uncaptured deliveries drawn in the captured delivery series' style and artist** (the rows
are added to the `delivery` series' points, and the new series is switched off). 7 of 21 fail:
`TestBuildFigure.test_a_one_marker_per_existing_value_and_none_at_a_blank` (03's, which pins that
`marker-delivery` excludes β = 1.2) and all six of `TestUncapturedDeliveries` (a, b, c, d, e, f).
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -429,7 +429,7 @@
     for column, key, text, marker, c, filled, size, z in SERIES:
         pts = [
             (r["beta"], r[column], r["T_hot_GeV"])
-            for r in captured
+            for r in (captured + [u for u in rows if u["kind"] == UNCAPTURED] if key == "delivery" else captured)
             if r[column] is not None
         ]
         style = dict(
@@ -460,7 +460,7 @@
         for r in rows
         if r["kind"] == UNCAPTURED and r["T_deliver_GeV"] is not None
     ]
-    if uncaptured_deliveries:
+    if False:
         style = dict(
             linestyle="none",
             marker="o",
```

**2. The uncaptured deliveries drawn in the lower panel as well.** 1 of 21 fails:
`TestUncapturedDeliveries.test_a_one_marker_per_uncaptured_delivery_in_the_upper_panel_only`.
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -476,6 +476,8 @@
             **style,
         )
         bounce.set_gid("marker-delivery-uncaptured")
+        (lowb,) = rx.plot([p[0] for p in uncaptured_deliveries], [1.0] * len(uncaptured_deliveries), **style)
+        lowb.set_gid("marker-delivery-uncaptured")
         handles.append(
             Line2D([], [], label="first bounce, not captured", **{**style, "zorder": 1})
         )
```

**3. The uncaptured deliveries dropped.** 5 of 21 fail: `TestUncapturedDeliveries` a, b, c, d and f.
```diff
--- a/tools/plot_surfing_temperature.py
+++ b/tools/plot_surfing_temperature.py
@@ -460,7 +460,7 @@
         for r in rows
         if r["kind"] == UNCAPTURED and r["T_deliver_GeV"] is not None
     ]
-    if uncaptured_deliveries:
+    if False:
         style = dict(
             linestyle="none",
             marker="o",
```

## Observations not acted on

- **The e⁺e⁻ band's label overlaps the uncaptured edge mark on `edges.csv`** (note §0: pre-existing,
  not 03b's, ruled to be recorded and taken to the user). The label ("e⁺e⁻ / reachable, not
  surfed") hangs at the bottom centre of the upper panel, and the edge mark at β = 2.999 sits at
  the axis edge under it. **This change does not make it worse:** it adds nothing near the bottom
  edge (the new rings are at 1.6 to 44 GeV, five to six decades above), and the label and the mark are
  as they were. On `scan79.csv` the label is clear of the marks.
- **Closely spaced uncaptured rings overlap slightly.** At 3.012 and 3.013 (0.001 apart on
  `edges.csv`'s 0.1-wide axis) the two rings intersect, and remain two. 04's fine scan, 2.980 to
  3.040 by 0.001 on a 0.06-wide axis, has the rings 1.7 times farther apart in the figure, so they
  overlap less. This is a point to look at when 04's figure is made, and not a defect of the marker.

## State handed to the next prompt

- `tools/plot_surfing_temperature.py` draws, in the upper panel only, an open grey circle
  (gid `marker-delivery-uncaptured`, legend "first bounce, not captured") at (β, `T_deliver_GeV`) for
  every not-captured `ok` row that has a delivery. The edge marks, the failure rows, the lower panel
  and the captured series are unchanged, and the y-limits include the new points.
- Suites at this commit: CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 69, tools 70.
  03's 15 tests are unedited.
- Prompt 04 may run `plot_surfing_temperature.py` on its CSVs and expect the rings. The e⁺e⁻ band
  label's overlap with an edge mark near β = 2.999 (above) is for the user's decision. No issue
  opened; `.documents/OPEN_ISSUES.md` is untouched.
