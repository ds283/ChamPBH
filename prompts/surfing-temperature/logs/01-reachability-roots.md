# Log — surfing-temperature 01: reachability roots

**Subject:** `find_kicks` and `reachability_roots` in `extract_common.py`, with their tests.
**Commit:** this commit · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent `2f38193` ·
**Model:** Claude Sonnet 5.5 · **Result:** landed. R1, R2 and R3 done; the gate "Roots" holds.
Four kicks on the production EOS (U5), the expected count, so §4's first stop did not fire.

## What shipped

- **`extract_common.py`** (beside `kick_threshold_curve`, which is unchanged):
  - `find_kicks(cosmology, T_lo, T_hi, n_grid=20000)` (R1);
  - `reachability_roots(cosmology, beta, kicks, n_grid=20000)` (R2);
  - private helpers `_Sigma`, `_Sigma_eff`, `_refine_extremum`, `_local_maxima`, `_dlnSigma_dlnT`;
  - constants `KICK_SIGMA_FLOOR = 1e-4`, `KICK_LABELS_PRODUCTION`, `REACHABILITY_N_GRID` and three
    numerical tolerances;
  - **one import added at module level**, `from scipy.optimize import brentq, minimize_scalar`, so
    `plot_by_beta.py` and `plot_ScalarModel.py` now import `scipy.optimize` too.
- **`ComputeTargets/tests/test_reachability_roots.py`** (R3): 15 tests, 6.0 s on a loaded machine.

### The interface (what 02 and 03 read)

- **A kick** is a `dict`: `label`, `T_peak`, `Sigma_peak`, `T_window_hot`, `T_window_cold`,
  `Sigma_at_hot_edge`, `Sigma_at_cold_edge`. Temperatures are dimensionful, in the cosmology's
  units (divide by `units.GeV` for GeV). `find_kicks` returns them hot to cold.
- **A root record** is a `dict`: `kick` (the kick's label), `beta`, `T_hot`, `T_cold`, `slope_hot`,
  `slope_cold`, `merged`, `multiple_crossings`. `reachability_roots` returns one per kick, in the
  order of `kicks`. `T_hot`, `T_cold` and the slopes are `None` where the kick has no interval.
  `merged` is `None`, or the label of the other kick (a tuple of labels if an interval holds more
  than two peaks; it does not on the production EOS).
- **Range.** `find_kicks` takes `T_lo` and `T_hi` as arguments. The test passes the table's ends,
  read from the EOS (`cosmology._eos._T_min * units.GeV` and `_T_max * units.GeV`), which are
  1e-5 GeV and 25118.86 GeV. `reachability_roots` takes its range from `kicks`: the hottest
  window's hot edge and the coldest window's cold edge, so they are the same ends.

### The method, as built

- **Grid.** 20 000 points uniform in ln T. Sigma is `1 - 3 w(T)`, with `w` called on a float
  dimensionful T, as `kick_threshold_curve` does.
- **Extrema.** The grid's local maxima are refined by `minimize_scalar(method="bounded",
  xatol=1e-12)` on the two neighbouring cells in ln T, then polished by `brentq` on
  `dw_dlogT = 0` within 1e-4 of the minimiser where the cosmology has `dw_dlogT` (see D1). The
  troughs, between consecutive kicks, are refined the same way.
- **Roots.** Sign changes of Sigma_eff - c on the grid with each kick's `T_peak` added to it, each
  refined by `brentq` in ln T with `xtol=1e-13`. They alternate hot, cold, hot, cold as T falls, and
  consecutive pairs are the intervals. An interval belongs to every kick whose `T_peak` it
  contains.
- **Slope.** `-3 dw_dlogT / Sigma` where the cosmology has `dw_dlogT`, otherwise a central
  difference of ln Sigma in ln T with step 1e-4. The docstring says so.
- **Guard.** If Sigma_eff exceeds c at either end of the range, `reachability_roots` raises
  `ValueError`: the end would be an unpaired root, and none is invented. The production EOS has
  Sigma = 0 at both ends.

## Deviations from the prompt

| # | Deviation | Tag |
|---|---|---|
| D1 | **`T_peak` and trough locations are polished by `brentq` on `dw_dlogT`.** The prompt says "a bounded scalar minimisation". The bounded minimiser alone locates a flat extremum only to about 1e-8 in ln T (the function changes by less than its rounding there), which cannot meet R3.1's `T_peak` to 1e-8 relative. The minimiser still brackets and falls back: where the cosmology has no `dw_dlogT`, or the derivative has no sign change within 1e-4 of the minimiser, its result stands. | IMPLEMENTATION CHOICE |
| D2 | **`kicks` is a list of `dict`s with a `label` key, and the root record's `kick` holds the label.** The prompt allows "a `namedtuple` or `dict`" and lists the root record's fields, not the kick's key for the label; `label` is the prompt's word ("the label"). | IMPLEMENTATION CHOICE |
| D3 | **`merged` is a tuple of labels if an interval holds more than two peaks.** The prompt says "names the other kick". The case does not arise on the production EOS, and is untested. | IMPLEMENTATION CHOICE |
| D4 | **The pairing and the `multiple_crossings` rule are as built, not spelled out by the prompt.** An interval belongs to the kicks whose `T_peak` it contains. `multiple_crossings` is set when another interval has a root inside the kick's window, and then `T_hot` and `T_cold` are the outermost of the kick's own interval and those. On Sigma_eff > c > 1e-4 every extra interval would hold a maximum above the floor and so would be a kick; the flag can only arise for c < 1e-4 (beta > 57), outside the campaign's range. Test c1 reaches it with a stub ripple of 5e-5 at beta = 100. | IMPLEMENTATION CHOICE |
| D5 | **`reachability_roots` has an optional `n_grid` and raises `ValueError` if Sigma_eff > c at an end of the range.** The prompt gives the signature `(cosmology, beta, kicks)`; the extra keyword has the same default as `find_kicks`'s. | IMPLEMENTATION CHOICE |
| D6 | **The brief narrowed §4's `T_peak` condition**: only Sigma_peak binds to the last digit; `T_peak` is compared to the orchestrator's refined values to 1e-3. The test does so. All four `T_peak` agree with the probe's printed digits (below). | STRUCTURALLY REQUIRED (by the brief) |

No UNINTENDED DRIFT.

## Verification performed

### Counts (serially, in the foreground; load from `uptime` at the start of each run)

| Suite | Before (`2f38193`) | After |
|---|---|---|
| CosmologyModels | 18 OK, 129.8 s, load 15 | 18 OK, 139.5 s, load 21 |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 449.4 s, load 32 | 61 OK, 457.0 s, load 40 |
| ComputeTargets `test_[g-z]*.py` | 91 OK, 573.9 s, load 85 | **106 OK**, 404.0 s, load 111 |
| ObjectFactories | 69 OK, 9.0 s, load 37 | 69 OK, 25.2 s, load 15 |
| tools | 34 OK, 13.3 s, load 37 | 34 OK, 29.5 s, load 31 |

ComputeTargets is 152 before and 167 after (106 + 61): 152 + the 15 new tests. No count fell. No
failure, so no re-run was needed.

`black --check extract_common.py ComputeTargets/tests/test_reachability_roots.py`: both unchanged.

### The kicks (R1), production EOS, 20 000 points, T in GeV

Every local maximum found, hot to cold (`_local_maxima`, no floor; the count is nine):

| T (GeV) | Sigma | Above the floor? | Kick |
|---|---|---|---|
| 53.262334 | 0.037435752 | yes | EW |
| 0.18189339 | 0.3145324 | yes | QCD |
| 0.043155861 | 0.085365138 | yes | mu-pi |
| 1.6045032e-4 | 0.10073248 | yes | ee |
| 2.7756828e-5 | 2.0918888e-5 | no | ripple |
| 2.1672253e-5 | 1.6839738e-6 | no | ripple |
| 1.7062786e-5 | 1.1352077e-7 | no | ripple |
| 1.3516807e-5 | 7.9780622e-9 | no | ripple |
| 1.049251e-5 | 2.4534783e-9 | no | ripple |

The five ripples agree with the brief's. The grid's minima below the floor are at 1.2e-5, 1.5e-5,
1.9e-5, 2.4e-5 and 3.0e-5 GeV, with Sigma = -2.6e-9, -2.9e-8, -3.6e-7, -2.6e-6 and +3.5e-6 (four
negative). Both agree with the brief's "twelve to thirty keV, four with Sigma < 0".

The four kicks and their windows:

| Label | T_peak | Sigma_peak | Window, hot edge to cold edge | Sigma at the edges |
|---|---|---|---|---|
| EW | 53.262334 GeV | 0.037435752 | 25118.864 GeV to 9.0264799 GeV | 0 ; 0.0047411287 |
| QCD | 0.18189339 GeV | 0.31453240 | 9.0264799 GeV to 72.988103 MeV | 0.0047411287 ; 0.074496195 |
| mu-pi | 43.155861 MeV | 0.085365138 | 72.988103 MeV to 7.5440285 MeV | 0.074496195 ; 0.00025951455 |
| ee | 0.16045032 MeV | 0.10073248 | 7.5440285 MeV to 1e-5 GeV | 0.00025951455 ; 0 |

- **Peaks.** Sigma_peak equals README §0.3's to its digits (0.037436, 0.31453, 0.085365, 0.10073).
  `T_peak` equals the orchestrator's probe (53.2623 GeV, 0.181893 GeV, 43.1559 MeV, 0.160450 MeV)
  to every digit it printed. So §4's second stop did not fire, and the `T_peak` narrowing was not
  needed; README §0.3's 53.25 GeV is the grid argmax (D6).
- **Thresholds**, sqrt((2 + Sigma_peak)/(6 Sigma_peak)): EW 3.011779, QCD 1.107448, mu-pi 2.017786,
  ee 1.864339.

### The troughs

| Between | T | Sigma | Sigma_eff | Sigma_eff > 1/(3*5^2) = 0.01333? |
|---|---|---|---|---|
| EW and QCD | 9.02648 GeV | 0.00474113 | 0.00472992 | no |
| QCD and mu-pi | 72.9881 MeV | 0.0744962 | 0.071821 | **yes** |
| mu-pi and ee | 7.54403 MeV | 0.000259515 | 0.000259481 | no |

So, as README §0.3 expects, only the QCD and mu-pi trough exceeds 1/(3 * 5^2): intervals merge
inside the campaign's range there and nowhere else. This is a measurement (README §5 rule 10); no
test asserts these values, and `test_d6` prints them.

### The beta table (R2; GeV; `T_hot`, `T_cold`, `slope_hot`, `slope_cold`; "-" is `None`)

`multiple_crossings` is False in every entry. `merged` is `-` except where a partner is named.

| beta | EW | QCD | mu-pi | ee |
|---|---|---|---|---|
| 1.10 | - | - | - | - |
| 1.2 | - | 0.229183, 0.153276; -1.341, 2.46 | - | - |
| 1.5 | - | 0.35698, 0.130272; -0.9256, 2.992 | - | - |
| 2 | - | 0.741603, 0.0965289; -0.7367, 1.155 | - | 2.29122e-4, 1.18252e-4; -0.7612, 1.106 |
| 2.1 | - | 0.852586, 0.0863437; -0.7248, 0.6661 | 0.0586962, 0.034502; -0.3835, 0.7654 | 2.58125e-4, 1.09074e-4; -0.9041, 1.411 |
| 2.2 | - | 0.972913, 0.0310956; -0.7434, 1.079; merged mu-pi | the same record; merged QCD | 2.85443e-4, 1.02592e-4; -1.036, 1.772 |
| 3 | - | 1.9859, 0.0216183; -1.073, 2.556; merged mu-pi | the same record; merged QCD | 4.70282e-4, 8.02191e-5; -1.464, 3.323 |
| 3.1 | 68.4976, 41.0946; -0.4299, 0.4854 | 2.11116, 0.0210733; -1.111, 2.676; merged mu-pi | the same record; merged QCD | 4.91981e-4, 7.86768e-5; -1.498, 3.561 |
| 4 | 125.766, 24.1204; -1.198, 1.401 | 3.18424, 0.017901; -1.419, 3.674; merged mu-pi | the same record; merged QCD | 6.8089e-4, 6.95869e-5; -1.684, 4.641 |
| 5 | 175.991, 18.4309; -1.466, 1.842 | 4.26094, 0.0159886; -1.669, 4.308; merged mu-pi | the same record; merged QCD | 8.84357e-4, 6.34127e-5; -1.775, 5.095 |

These reproduce the orchestrator's probe at beta = 1.2, 2, 2.1, 2.2, 3.1 and 5 to every digit it
gave. The first ee root exists at beta = 1.864 and the first mu-pi root at 2.018, so at beta = 2.0
mu-pi is absent and ee present. The merge is between beta = 2.1 (QCD and mu-pi separate) and 2.2
(one interval, from 0.973 GeV to 31.1 MeV), with threshold 2.154. At every root checked,
`kick_threshold_curve`'s beta_th(T) equals beta to 1e-9 (test d3, d4).

### The new tests (R3), by class

Command: `PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_reachability_roots.py' -v`.
15 tests, 6.0 s (load 30), OK.

| Class | Tests | What it pins |
|---|---|---|
| `TestTwoSeparatedGaussians` | 6 (a1-a6) | two kicks labelled `kick0` and `kick1`, `T_peak` and `Sigma_peak` analytic to 1e-8; roots analytic to 1e-9 at beta = 5; slopes (`slope_hot < 0 < slope_cold`, and equal to -(x - m)/s^2); the central-difference slope without `dw_dlogT`; None for the weaker kick at beta = 2; None everywhere at beta = 1 |
| `TestMergedInterval` | 2 (b1, b2) | trough above c: one interval, both records carry its roots, `merged` set on both, no root at the trough; and separate, `merged` None, when c is above the trough |
| `TestMultipleCrossings` | 1 (c1) | a sub-floor ripple crossing c: flag set, the outermost pair returned; no flag on a single Gaussian |
| `TestProductionEOS` | 6 (d1-d6) | four labelled kicks, Sigma_peak to README's digits, `T_peak` to 1e-3, the windows tile the table; the thresholds 3.01, 1.107, 2.018, 1.86; at threshold + 0.01 each kick's roots straddle `T_peak` and solve `kick_threshold_curve`; every root over six beta solves it to 1e-9; the merge at 2.1 and 2.2; the troughs and the table printed |

## The deliberate-breakage record

Each diff is against `extract_common.py` at this commit. Each was checked with `git apply --check`
and `git apply -R --check`, and replayed in `bash` from the final tree (applied, then reversed, and
the file compared with the committed one). None was committed. Test command for each:
`PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_reachability_roots.py'`.

**1. The hot and cold roots swapped** (`T_cold, T_hot = own[0]`). 6 of 15 fail:
`test_a2_roots_above_both_thresholds`, `test_a5_beta_between_the_thresholds`,
`test_b1_merged_above_the_trough`, `test_c1_flag_and_outermost_pair`,
`test_d3_roots_straddle_the_peak_and_solve_the_threshold_curve`, `test_d5_the_merge_of_qcd_with_mu_pi`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -787,5 +787,5 @@
         }
         if own:
-            T_hot, T_cold = own[0]
+            T_cold, T_hot = own[0]
             others = [
                 k["label"]
```

**2. Sigma in place of Sigma_eff.** 6 of 15 fail: `test_a2_roots_above_both_thresholds`,
`test_a5_beta_between_the_thresholds`, `test_b1_merged_above_the_trough`,
`test_c1_flag_and_outermost_pair`, `test_d3_roots_straddle_the_peak_and_solve_the_threshold_curve`,
`test_d4_every_root_solves_the_threshold_curve`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -595,5 +595,5 @@
 
 def _Sigma_eff(Sigma: float) -> float:
-    return Sigma / (1.0 + 0.5 * Sigma)
+    return Sigma
 
 
```

**3. A trough refined onto the neighbouring peak** (`argmax` for `argmin`). 6 of 15 fail:
`test_a1_kicks`, `test_a2_roots_above_both_thresholds`, `test_b2_separate_when_c_is_above_the_trough`,
`test_d1_four_kicks_and_their_peaks`, `test_d5_the_merge_of_qcd_with_mu_pi`,
`test_d6_beta_table_and_troughs`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -677,5 +677,5 @@
     for k in range(n - 1):
         i_hot, i_cold = peaks[k][2], peaks[k + 1][2]
-        m = i_cold + int(np.argmin(S[i_cold : i_hot + 1]))
+        m = i_cold + int(np.argmax(S[i_cold : i_hot + 1]))
         m = min(max(m, i_cold + 1), i_hot - 1)
         xr = _refine_extremum(cosmology, x[m - 1], x[m + 1], False)
```

**4. The merged case returning a root at the window edge.** 3 of 15 fail:
`test_b1_merged_above_the_trough`, `test_d4_every_root_solves_the_threshold_curve`,
`test_d5_the_merge_of_qcd_with_mu_pi`.
```diff
--- a/extract_common.py
+++ b/extract_common.py
@@ -795,4 +795,5 @@
             if others:
                 record["merged"] = others[0] if len(others) == 1 else tuple(others)
+                T_cold = max(T_cold, kick["T_window_cold"])
             in_window = [
                 iv
```

Note: the hot and cold labels were first thought of as a `"hot"`/`"cold"` tag on each sign change.
It was unused (the intervals are paired by order in T), so it was removed, and breakage 1 swaps the
interval's two ends instead.

## Observations not acted on

- **`Xav_EOS_spline.w` ripples below 30 keV** (Sigma between -3e-6 and +2e-5 over the bottom
  decade) are listed above. None is near c, which is at least 0.0133 for beta <= 5. Nothing to do.
- **`test_kicking_function.py`'s `test_three_peaks`** looks in three fixed windows and so does not
  see the mu-pi shoulder. The brief says it pins three windowed peaks correctly, and it is not
  touched (README §5 rule 13). No issue opened.
- **README §0.3's 53.25 GeV** for the EW peak is a grid argmax; the refined peak is 53.2623 GeV
  (the brief's ruling). The README is not edited here (records are additive, `CLAUDE.md` rule 6).
- **The EW kick's slopes are shallow** (|slope| 0.43 at beta = 3.1, 0.49 at the cold root), so the
  EW capture oscillator is weakly stable there. A measurement for 02, not a defect.
- One scratch copy of `extract_common.py` was briefly written to `/tmp/x` in error, against the
  brief's scratch rule, and removed at once. Nothing else was written outside the repository and
  the agent's scratch directory.

## State handed to the next prompt

- **Branch** `surfing-temperature`, HEAD is this commit, on top of `2f38193`. Nothing staged or
  modified after it. The untracked entries are the user's, as before.
- **Counts:** CosmologyModels 18, ComputeTargets 167 (61 + 106), ObjectFactories 69, tools 34.
- **Available in `extract_common.py`:** `find_kicks(cosmology, T_lo, T_hi, n_grid=20000)` and
  `reachability_roots(cosmology, beta, kicks, n_grid=20000)`, with the interfaces above. On the
  production EOS pass `T_lo = cosmology._eos._T_min * units.GeV` and
  `T_hi = cosmology._eos._T_max * units.GeV` (1e-5 GeV, 25118.86 GeV). Compute `kicks` once per
  cosmology.
- **The production kicks:** `EW` 53.262 GeV (Sigma 0.037436), `QCD` 0.18189 GeV (0.31453),
  `mu-pi` 43.156 MeV (0.085365), `ee` 0.16045 MeV (0.10073). Windows are bounded by the troughs
  at 9.0265 GeV, 72.988 MeV and 7.5440 MeV.
- **Merged intervals:** for every beta above 2.154 the QCD and `mu-pi` records are one interval,
  both records carrying its `T_hot`, `T_cold` and slopes, with `merged` naming the other. The
  QCD capture's `T_cold` there is the interval's cold edge (31.1 MeV at beta = 2.2, 16.0 MeV at 5).
  EW merges with QCD only above beta = 8.395 and `mu-pi` with `ee` above 35.84, so not in 1.1-5.
- **No root exists** for beta below 1.1074 (QCD), and none for `mu-pi` below 2.018, `ee` below
  1.864 and EW below 3.012. The slope's sign is `slope_hot < 0 < slope_cold` at every root.
- **Kick records give temperatures in the cosmology's units**, not GeV, and roots are floats.
  `kick_threshold_curve`, `plot_by_beta.py` and every EOS class are untouched.
- **One import is new** in `extract_common.py`, `scipy.optimize`, which both plot scripts now pay.
- **No issue opened;** `.documents/OPEN_ISSUES.md` is untouched.
- **The breakage diffs** are in this log and replay from a clean tree at this commit.
