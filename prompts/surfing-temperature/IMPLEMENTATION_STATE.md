# surfing-temperature campaign — implementation state

**Last updated:** 2026-10-14 · **Status: LIVE — 7 of 7 prompts written, 6 of 7 landed (01, 02, 03, 03b, 04, 04b). U1–U8 and P1–P10
ruled by the user on 2026-10-14 (README §6); P1–P10 as proposed. U9 ruled 2026-10-14, before 05:
one issue open (§3).**
The plan is [`README.md`](README.md), written on 2026-10-14 on `main` at `f41b104`. Its planning
commit `8adb806` is the first on branch `surfing-temperature`, cut from `main` at `f41b104` (P10).

The campaign computes the hot and cold roots of the reachability condition for every β the user
asked for, and measures on integrated histories (φ* = 5 M_P, M = 10⁻³ M_P) four temperatures: the
capture, the largest, the second dip and the delivery. It plots them all against β. Nothing is
stored, and no production code path changes.

**Campaign:** [`README.md`](README.md) ·
**Code:** `extract_common.py`, `ComputeTargets/ScalarModel.py` (a new function only), new
`tools/surfing_scan.py` and `tools/plot_surfing_temperature.py` ·
**Verification:** `.documents/surfing-temperature-verification.md` (created by 04) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.13 (one opened, U9;
none assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Reachability roots](01-reachability-roots.md) | `find_kicks`, `reachability_roots`: kick windows, hot and cold roots, stability sign, merged intervals | Sonnet | ✍️ 2026-10-14 | ✅ 2026-10-14 | `2c98370` | [log](logs/01-reachability-roots.md) |
| 02 | [Surfing turning points and the driver](02-surfing-turning-points-and-driver.md) | `surfing_turning_points` on the dense output; `tools/surfing_scan.py`; five β run, cost projected | Opus | ✍️ 2026-10-14 | ✅ 2026-10-14 | `f166b31` | [log](logs/02-surfing-turning-points-and-driver.md) |
| 03 | [The plot](03-plot-surfing-temperature.md) | `tools/plot_surfing_temperature.py`: roots and measured temperatures against β, two panels | Sonnet | ✍️ 2026-10-14 | ✅ 2026-10-14 | `1f40b53` | [log](logs/03-plot-surfing-temperature.md) |
| 03b | [Uncaptured deliveries](03b-plot-uncaptured-deliveries.md) | the first bounce of an uncaptured history drawn in the upper panel (U7) | Sonnet | ✍️ 2026-10-14 | ✅ 2026-10-14 | `ee4a2df` | [log](logs/03b-plot-uncaptured-deliveries.md) |
| 04 | [Run the scan](04-run-the-scan.md) | 79 β, 1.10–5.00, CSV and figure committed; verification document (U4); fine scan near β = 3.0 (U6) | Opus | ✍️ 2026-10-14, amended for U6 | ✅ 2026-10-14 | `90d44f8` | [log](logs/04-run-the-scan.md) |
| 04b | [Tidy the band label](04b-tidy-band-label.md) | the e⁺e⁻ band label moved clear of the edge marks; both committed figures redrawn (U8) | Sonnet | ✍️ 2026-10-14 | ✅ 2026-10-14 | `a65bd74` | [log](logs/04b-tidy-band-label.md) |
| 05 | [Close](05-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.2.
*Written 2026-10-14 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch.

| Note | Commit | Used for |
|---|---|---|
| [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) | `2f38193` | 01 (reviewed 2026-10-14) |
| [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) | `f6440d1` | 02 (reviewed 2026-10-14) |
| [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) | `9d6bbaa` | 03 (reviewed 2026-10-14) |
| [`orchestrator/prompt-03b.md`](orchestrator/prompt-03b.md) | `f19332d` | 03b (reviewed 2026-10-14) |
| [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) | `e7ed715` | 04 (reviewed 2026-10-14) |
| [`orchestrator/prompt-04b.md`](orchestrator/prompt-04b.md) | `f30a3c0` | 04b (reviewed 2026-10-14) |

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **R1** | 01 | `find_kicks`: Σ's peaks and the troughs between them, refined on the spline; labels EW, QCD, mu-pi, ee (P5, U5) | ✅ |
| **R2** | 01 | `reachability_roots`: hot and cold roots per kick, slopes, merged intervals, the `multiple_crossings` flag | ✅ |
| **R3** | 01 | `ComputeTargets/tests/test_reachability_roots.py`: stub Σ, a merged stub, the production EOS | ✅ |
| **T1** | 02 | `surfing_turning_points` beside `first_bounce`; not called by production | ✅ |
| **T2** | 02 | `tools/surfing_scan.py`: options, CSV per README §4, sidecar, resume | ✅ |
| **T3** | 02 | the summary per history: capture, largest, second dip, delivery, capture kick and its roots (U1–U3) | ✅ |
| **T4** | 02 | `ComputeTargets/tests/test_surfing_turning_points.py`, `tools/tests/test_surfing_scan.py`; one real history | ✅ |
| **T5** | 02 | five β run (P6), per-history cost, scan projected against P7 | ✅ |
| **F1** | 03 | `tools/plot_surfing_temperature.py`: reads the CSV and sidecar, roots on Δβ = 0.005 (P4) | ✅ |
| **F2** | 03 | the two-panel figure of README §4 | ✅ |
| **F3** | 03 | `tools/tests/test_plot_surfing_temperature.py` | ✅ |
| **F4** | 03b | the first bounce of an uncaptured history, an open grey circle in the upper panel; 03's tests unchanged (U7) | ✅ |
| **S1** | 04 | the 79-point scan, chunked and resumable (U4, P7) | ✅ |
| **S2** | 04 | the figure from the scan | ✅ |
| **S3** | 04 | two β re-run, equal as strings but for `wall_s` | ✅ |
| **S4** | 04 | `.documents/surfing-temperature-verification.md` §1; the outputs committed (P8) | ✅ |
| **S5** | 04 | the fine scan near β = 3.0, β = 2.980–3.040 by 0.001, its CSV and figure (U6) | ✅ |
| **F5** | 04b | no band label overlaps an edge mark; the two committed figures redrawn from the committed CSVs (U8) | ✅ |
| **Z1** | 05 | `architecture-summary.md`: the new functions and tools | ⬜ |
| **Z2** | 05 | `CLAUDE.md`: the suites' counts | ⬜ |
| **Z3** | 05 | the handover, `.documents/review-remediation-verification.md` §4.14 | ⬜ |
| **Z4** | 05 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` if an issue opened | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Roots | on a stub Σ the roots equal the analytic ones; on the production EOS the peaks and thresholds equal README §0.3's and the paper's, and β_th(root) = β | 01 | ✅ 2026-10-14 (`2c98370`) |
| Turning points | on stub interpolants every crossing of β π/M_P = −1 is found in order and with its direction, none after the first bounce; on a real history \|1 + β π/M_P\| < 1e-8 at each, and delivery equals `first_bounce` | 02 | ✅ 2026-10-14 (`f166b31`) |
| Scan | every β of both grids (S1, S5) has a CSV row (a failure is a row with its reason); every first bounce lies above T_stop; two β reproduce as strings, and β = 3.00 is equal in both CSVs; each figure is drawn from its CSV alone | 04 | ✅ 2026-10-14 (`90d44f8`) |

## 3. Active and unresolved issues

### `[05-surfing-scan-runs-at-a-fixed-lambda]`

**Opened:** 2026-10-14 by the orchestrator, while writing note 05, and ruled by the user the same
day (U9, README §6.1). **Not fixed:** the campaign's code is finished, and the change belongs to
whichever campaign makes Λ an output.

- **What.** `tools/surfing_scan.py` builds every history's potential with
  `tools.history_and_bbn.LAMBDA_EV`, 1e-3 eV (`tools/history_and_bbn.py:111`; imported at
  `tools/surfing_scan.py:106-112`, used at `:210-216`). It has no option to change Λ, and φ_ref does
  not exist in the code. A resumed run is refused if a row's `Lambda_eV` differs from the tool's
  (`_INPUT_MATCH`, `:155`), so one CSV holds one Λ. README §0.4 and §7, and the verification
  document §1.7, say that the re-run after `jordan-normalization` is the same commands on that
  campaign's tree. That holds only if that campaign changes the tool.
- **Impact.** None on the committed results: every row and both sidecars record `Lambda_eV` = 0.001,
  and the roots do not depend on Λ. What depends on Λ is set by the wall's timing: the largest
  temperature, the second dip, delivery, and which β near 3.0 are captured at all (verification
  §1.7, §1.8). The brief of `jordan-normalization` has Λ = 2.13 meV at β = 1.5 after tuning, and
  there Λ is per history (β, M, φ*, n), not one value per scan.
- **Next step.** For the planner of `jordan-normalization`, or a later campaign: give the scan the
  tuned Λ and φ_ref per β (from that campaign's tuning, or by an option), let a CSV carry a Λ per
  row, and then re-run §1.1's commands. Not assigned: `jordan-normalization` has no board yet.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-14)

Planned at the user's request, after `read-only-plots` closed and the `datastorekit` `v0.4.0` pin
(`f41b104`). Nothing was run. The planner read the paper's surfing section, the temperature law, the
EOS spline, `first_bounce`, `kick_threshold_curve` and `tools/history_and_bbn.py`.
- **The turning points of T_J are the crossings of β π/M_P = −1** (README §0.2), so capture is the
  first minimum. The user ruled the measured set accordingly (U1). Delivery is the existing
  T_deliver (U2), and the first episode is measured whichever kick captures it (U3).
- **The full scan is run by a prompt** (U4), as an exception to the run-length rule. It runs no
  PRyMordial and no Ray.
- **Independent of `jordan-normalization`** for the roots and the capture. The largest temperature,
  the second dip and delivery may move when that campaign retunes Λ (README §0.4). The science
  rebuild, held for that campaign, is not touched.
- **Suites at `f41b104`:** 18 / 152 / 69 / 34.

### 5.1 Prompts written (2026-10-14)

The user accepted P1–P10 as proposed. Prompts 01–05 are written from the plan's reading and from a
further read at `8adb806`:
- `test_first_bounce.py` and `test_kinematic_cap_loop.py`'s helpers, the idiom for driving the
  integrator in a test without Ray;
- `TestKickThresholdCurve`, the stub-cosmology idiom;
- `extract_common.py`'s `_save` and `plot_T_deliver`, the figure idiom;
- the existing `tools/tests` import style (`import tools.<module>`).

`extract_common.py`, `ComputeTargets/ScalarModel.py` and `tools/history_and_bbn.py` are black-clean
at `8adb806`.

One refinement of the README's charter for 01 was made in writing it, and is recorded in README
§2.2. The roots are found as sign changes over the whole range and paired into reachable
intervals, so that an interval spanning a trough at large β (c = 1/(3β²) = 0.0133 at β = 5) is
reported as merged rather than cut at the window edge. The windows (P5) still assign roots and
captures to kicks.

### 5.1a U5, before 01 was dispatched (2026-10-14)

The orchestrator probed the production spline while writing note 01, at `a8823a9`. It found four
local maxima of Σ above 01's floor of 1e-4, not three. The fourth is a shoulder at 43.16 MeV
(Σ = 0.085365), which is in the table's rows and lies where muons and pions annihilate. Its threshold
is β = 2.018, and it merges with QCD above β = 2.154 (README §0.3). As written, 01 would have
stopped on its §4 condition. **The user ruled it a fourth kick, `mu-pi` (U5).** 01's labels, stop
condition and production tests, and 03's band labels, were amended before dispatch. In a merged
interval both kicks' records carry the interval's roots, and that clarification was made with U5
(README §6.1).

### 5.2 After prompt 01 (2026-10-14)

Prompt 01 landed in `2c98370`; the log is [`logs/01-reachability-roots.md`](logs/01-reachability-roots.md).
`find_kicks` and `reachability_roots` are in `extract_common.py`, with
`ComputeTargets/tests/test_reachability_roots.py` (15 tests).
- **Four kicks** on the production EOS, as U5 ruled: `EW` 53.262 GeV (Σ 0.037436), `QCD` 0.18189 GeV
  (0.31453), `mu-pi` 43.156 MeV (0.085365), `ee` 0.16045 MeV (0.10073). Σ_peak equals README §0.3's
  to its digits and `T_peak` the probe's. Nine local maxima in all, five of them ripples below the
  floor (10–28 keV).
- **The troughs:** 9.0265 GeV (Σ 0.0047411), 72.988 MeV (0.074496) and 7.5440 MeV (0.00025951).
  Only the QCD–`mu-pi` one exceeds Σ_eff = 1/(3·5²), so intervals merge inside the β range there
  and nowhere else. The merge is between β = 2.1 and 2.2.
- **The gate "Roots" holds:** stub roots analytic to 1e-9; β_th(root) = β to 1e-9 on the production
  EOS; each of the four deliberate breakages fails a named test (6, 6, 6 and 3 of the 15).
- **Counts:** CosmologyModels 18, ComputeTargets 167 (61 + 106), ObjectFactories 69, tools 34. One
  import is new in `extract_common.py`, `scipy.optimize`. No issue opened; the index is untouched.
- **Deviations:** one tagged STRUCTURALLY REQUIRED (the brief's narrowing for `T_peak`) and five
  IMPLEMENTATION CHOICE, none UNINTENDED DRIFT. The main one: extrema are polished by `brentq` on
  `dw_dlogT`, since the bounded minimiser alone locates a flat peak only to about 1e-8 in ln T.

**Orchestrator review of prompt 01** (2026-10-14, Claude Opus 5.5, against `2c98370`; note
[`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) §3). All ten checks pass.
- **Scope.** The four files of the prompt's §5, and the untracked set unchanged. By `ast` against
  `2f38193`, `extract_common.py` differs only by the `scipy.optimize` import and the new constants and
  functions after `kick_threshold_curve`. Black is clean.
- **By reading**, the method is sound:
  - the roots alternate hot, cold, because Σ_eff − c < 0 at both ends;
  - a root on a grid point is found once;
  - each trough is the argmin between two kicks, so a ripple below the floor cannot split a window;
  - each `T_peak` is added to the root grid, so a narrow pair just above threshold is not missed;
  - the slope is −3 (dw/d ln T)/Σ.
- **An independent probe** reproduces the note's table:
  - the four `T_peak` to 2e-6 and Σ_peak to 5e-7;
  - every root at β = 1.2, 2.0, 2.1, 2.2, 3.1 and 5.0;
  - `merged` unset at 2.1, and QCD ↔ `mu-pi` from 2.2;
  - β_th(root) = β to 2.4e-14;
  - all None at β = 1.10, and only a narrow QCD pair at 1.1075.

  At each threshold, and 1e-9 either side of it, there is no exception and the result is consistent
  (None, or a degenerate pair). `n_grid` 500 and 5000 give the same kicks and roots to 1e-13.
- **The breakages.** All four replay with `git apply` in bash and fail exactly the tests the log
  names (6, 6, 6, 3), and the tree is clean after each. The tests pin the root order about
  `T_peak`, the slope signs, the merged record on the production EOS at 2.2, and the four labels.
- **Suites at `2c98370`**, serially: 18, 61 + 106, 69 and 34, all OK, at load 9–26.
- **Residue fixed here:** the SHA for "this commit", the notes table, the status line.
- **The agent's `/tmp/x`**, which the log owns up to, is gone, and nothing else was written outside
  the scratchpad. No issue is opened.
- **For the next notes:**
  - **02.** `find_kicks` is called with the table's ends, which today are reachable only through
    the private `cosmology._eos._T_min`/`_T_max`. The kick's key is `label`, and the record's key is
    `kick`. `T_cold` for a QCD capture above β = 2.154 is the merged edge.
  - **03.** `reachability_roots` costs 0.077 s per β, so the fine grid of 781 β takes about 60 s.
    `mu-pi`, like ee, is probably never the first kick to capture, so the "reachable, not surfed"
    label probably applies to it as well.

### 5.3 After prompt 02 (2026-10-14)

Prompt 02 landed in `f166b31`; the log is
[`logs/02-surfing-turning-points-and-driver.md`](logs/02-surfing-turning-points-and-driver.md).
- **What shipped.** `SurfingCrossing` and `surfing_turning_points(result, coupling)` after
  `first_bounce` in `ComputeTargets/ScalarModel.py` (129 lines added, none removed; not called by
  `compute_scalar_model`). The new `tools/surfing_scan.py`, with `summarise`, `run_history`,
  `beta_grid` and the resumable CSV of README §4 and its sidecar. Two new test modules:
  `ComputeTargets/tests/test_surfing_turning_points.py` (9 tests, stub interpolants) and
  `tools/tests/test_surfing_scan.py` (15 tests, one real history at β = 2).
- **The walk** reads g = 1 + (d ln Ω/dφ) π through the coupling at each step's ends and 8 interior
  points, up to `first_bounce(result).N` on the bounce's own step, and refines by `brentq` at
  xtol 1e-15.
- **The gate "Turning points" holds:** on the stubs every crossing in order with its direction to
  1e-12, none after the bounce, two zeros in one step both found; at β = 2 |1 + β π/M_P| ≤ 2.2e-16
  at both crossings, and delivery equals `first_bounce` exactly. The β = 2 crossings reproduce the
  orchestrator's probe to its printed digits (N = 14.166305971, 18.947923558; bounce 20.352082230).
- **T5** (φ* = 5, M = 10⁻³, T_stop = 1 MeV; load 3.0), T in GeV:

  | β | wall (s) | steps | n | kick | T_capture | T_max | T_2nd dip | T_deliver |
  |---|---|---|---|---|---|---|---|---|
  | 1.15 | 0.39 | 875 | 2 | QCD | 0.18858 | 0.20404 | — | 0.20404 |
  | 1.5 | 0.47 | 1231 | 2 | QCD | 0.32110 | 0.36139 | — | 0.36139 |
  | 2.5 | 1.10 | 3214 | 2 | QCD | 1.1351 | 1.3804 | — | 1.3469 |
  | 3.5 | 3.59 | 11059 | 4 | EW | 77.267 | 100.72 | 97.199 | 97.621 |
  | 5.0 | 7.90 | 24349 | 10 | EW | 136.87 | 187.85 | 173.35 | 175.99 |

  Every first bounce is a turning point above T_stop; no history failed; at most one crossing on
  any accepted step; every count is even. The rows agree with the orchestrator's probe.
- **The projection (P7).** The wall time interpolated piecewise-linearly in β over the 79-point
  grid sums to **243 s (4.0 min)**, against P7's 60 minutes (228 s if interpolated linearly in
  ln wall). The longest single history is β = 5.0, at **7.9 s**. A run adds about 2 s of setup
  (imports, the cosmology, `find_kicks`). So 04 can run the scan in one invocation under the
  ten-minute Bash cap.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 49.
- **The breakages:** each of the five fails a named new test (log).
- **No issue opened;** the index is untouched.

**Orchestrator review of prompt 02** (2026-10-14, Claude Opus 5.5, against `f166b31`; note
[`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) §3). All eleven checks pass.
- **Scope.** The six files of the prompt's §5, and the untracked set unchanged. By `ast` against
  `f6440d1`, `ScalarModel.py` differs only by `SurfingCrossing` and `surfing_turning_points`,
  inserted after `first_bounce`. `compute_scalar_model` dumps identically.
  `history_and_bbn.py`, `extract_common.py` and `config/` are untouched. Black is clean.
- **By reading**, the method is sound:
  - the walk ends at `first_bounce(result).N`, on the bounce's own step;
  - g is read through `coupling.d_logOmega_dphi(φ)`;
  - each step boundary is read once, so a zero there is counted once;
  - `brentq` runs at xtol 1e-15.

  `summarise` applies U1–U3 as written. It keeps delivery on an uncaptured row, labels the capture
  by `find_kicks`'s windows and takes that kick's roots, the merged edge included. Floats are
  written by `repr`, and the capture wrapper is removed in a `finally`.
- **An independent probe.** The tool was run on β = 1.10, 1.15, 1.5, 2.0, 2.5, 3.0, 3.5 and 5.0,
  on the committed code.
  - Every row equals the note's table to its digits, step counts included, the merged `T_cold` at
    2.5 too. At 1.10 and 3.0, `captured` is false and the delivery is present.
  - This also covers the log's caveat that T5 ran before the function was restructured.
  - On the same histories, `surfing_turning_points` equals the orchestrator's own finder bitwise.
    With 0 or 32 interior samples the counts and directions are unchanged, and N moves by at most
    2.3e-11 at β = 5. That is the conditioning of the late, shallow zeros, where |g| is at rounding
    over that range.
- **The tool's behaviour.**
  - A re-run adds no row, leaves the CSV byte-identical and appends a resumed entry to the sidecar.
  - `--phi-init-Mp 4` exits 2 and leaves the file unchanged.
  - A run outside the root exits 2 and writes nothing.
  - β = 1.5 and 3.5, re-run into a separate CSV, equal the first rows as strings, `wall_s` apart.
    That is 04's S3 in miniature.
  - `beta_grid(1.10, 5.00, 0.05)` gives 79 values, each equal to `round(1.10 + 0.05 k, 10)`.
- **The breakages.** All five replay with `git apply` in bash, and each fails exactly the tests the
  log names. The tree is clean after each. The stub tests pin the following:
  - two zeros in one step;
  - the stop at the bounce;
  - a reflection before and between crossings;
  - the wall-type `max` on the bounce's own step (h);
  - g read through the coupling.

  The real history pins |g| < 1e-8 and delivery's equality with `first_bounce`.
- **Suites at `f166b31`**, serially: 18, 61 + 115, 69 and 49, all OK, at load 5–6.
- **Residue fixed here:** the SHA for "this commit" (board and log), the notes table.
- **Observation, not acted on.** A CSV left empty, by a run killed between creating the file and
  writing its header, is refused on resume rather than resumed. A one-chunk scan makes this
  unlikely, and the remedy is to delete the empty file. No issue is opened.
- **For the next notes:**
  - **03.**
    - The sidecar is `{"runs": [...]}`, one entry per invocation, so a resumed CSV can carry more
      than one `git_head`. 03's title must say which it shows.
    - Uncaptured rows (β = 1.10 and 3.00 on the grid) carry a delivery, which F2 does not draw.
    - At 32 of the 77 captured β, the largest T_J is the wall's turning of the field, so its
      marker sits on the delivery marker. At β = 1.15–1.50 the two are equal to six digits.
    - T5's CSV is in the session scratchpad at `agent02/t5/surfing_t5.csv`.
  - **04.** The whole grid is one invocation of about 4 minutes, so the chunk plan is a single
    chunk. `wall_s` includes the walk.

### 5.4 After prompt 03 (2026-10-14)

Prompt 03 landed in `1f40b53`; the log is
[`logs/03-plot-surfing-temperature.md`](logs/03-plot-surfing-temperature.md).
- **What shipped.** The new `tools/plot_surfing_temperature.py` (`--csv`, `--out`; writes
  `PREFIX.pdf` and `.png`; exit 0, 1 saving failed, 2 refused) and
  `tools/tests/test_plot_surfing_temperature.py` (15 tests). The figure is built by
  `build_figure(rows, roots, sidecar)`, which needs no cosmology; `main` builds the production
  cosmology, `find_kicks` once, and `reachability_roots` on the fine grid (β range rounded outwards
  to 0.05 with a tolerance, step 0.005, built by index).
- **The figure.** Two panels (2 : 1). Upper: the four kicks' bands, hot solid and cold dashed; the
  QCD–μ/π merged interval one band, filled once, with the lines broken where `merged` changes;
  "reachable, not surfed" on every kick no row captures (μ/π and e⁺e⁻ on the probe scan); the four
  thresholds labelled; four markers for captured rows only (capture ▾, largest ▴, second dip open ◇,
  delivery ●; a coincident maximum and delivery both visible: disc under triangle); a tick for a
  not-captured row and a cross for a failure on the lower edge. Lower: each temperature over
  `T_hot_GeV`, line at 1. The title names every distinct sidecar `git_head`, marking tracked changes.
- **Plotted from both CSVs** (T5's and the orchestrator's 79-row probe), each in about one minute
  (52 s and 60 s; the roots at 771 and 781 β). Both legible at A4 width; the three series are
  separable on the upper panel by shape only, and the lower panel (0.78–1.07) carries the values.
  The legibility stop did not fire.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 64.
- **The breakages:** each of the three (blanks as zeros, the ratio against `T_cold_GeV`, the
  not-captured rows dropped) fails a named new test (2, 1 and 1 of the 15; log).
- **Deviations:** 5 STRUCTURALLY REQUIRED (the brief's rulings), 7 IMPLEMENTATION CHOICE and 1
  UNINTENDED DRIFT (the new module and a scratch roots run existed while the baseline suites ran;
  no count is affected). No issue opened; the index is untouched.

**Orchestrator review of prompt 03** (2026-10-14, Claude Opus 5.5, against `1f40b53`; note
[`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) §3). All ten checks pass.
- **Scope.** The four files of the prompt's §5, and the untracked set unchanged. `git diff` of
  `extract_common.py`, `tools/surfing_scan.py`, `ComputeTargets/` and `config/` is empty. Black is
  clean.
- **By reading**, the script follows the prompt and the note's rulings:
  - a row is classified by `status` first;
  - blanks are `None`, never 0;
  - the title lists every distinct `git_head` with its tracked-change mark;
  - the β range is rounded outwards with a 1e-9 tolerance and built by `beta_grid`;
  - `find_kicks` is called once;
  - `build_figure` needs no cosmology;
  - the PDF path is built by concatenation.

  In the figure, the merged band is drawn once, from the hotter kick's record, and the lines of a
  kick drawn alone are separate artists from the merged ones, so nothing joins the two sides.
  "Reachable, not surfed" is decided from the rows' `capture_kick`.
- **An independent probe** ran the committed `main` on `surfing_t5.csv` and `scan79.csv`:
  - exit 0, 771 and 781 calls of `reachability_roots`, in 147 s and 101 s at load 8–12 (the
    agent's 52 s and 60 s were at a lighter load);
  - the upper markers and the lower panel's ratios equal the CSV's values and quotients exactly,
    77 / 77 / 43 / 77 points;
  - edge marks at 1.10 and 3.00 only;
  - QCD's own lines end at β = 2.150 and the merged ones start at 2.155;
  - μ/π and e⁺e⁻ labelled "reachable, not surfed", the merged band "QCD + μ/π".

  A copy of T5's CSV with a failure row (blank `captured`) draws, with the red cross and its legend
  entry. A run outside the root exits 2 and writes nothing.
- **Legibility** (the PNGs looked at, and a greyscale copy):
  - the figure reads at A4 width;
  - the coincident maximum and delivery show as a triangle on a disc at β = 1.15–1.65 and 3.05;
  - the threshold labels do not collide;
  - in greyscale the series are told apart by shape.

  The hot-root line is hidden under the markers where they ride it, and the merge leaves a
  one-cell white gap in the QCD fill at β = 2.15. Neither hides a value. Not acted on.
- **The breakages.** All three replay with `git apply` in bash, and each fails exactly the tests
  the log names. The tree is clean after each. The tests also pin:
  - the failure row's blank `captured`;
  - the merged band's colours and the line break;
  - the title's SHAs;
  - the coincident markers' sizes and draw order;
  - the fine grid's 11 calls and its ends on the real `reachability_roots`.
- **D12** (UNINTENDED DRIFT) does not affect any count: `tools/tests` discovers only `test_*.py`, so
  an untested module present during the baseline cannot change a count. The baseline counts equal
  the orchestrator's.
- **Suites at `1f40b53`**, serially: 18, 61 + 115, 69 and 64, all OK, at load 9–14.
- **Residue fixed here:** the SHA for "this commit" (board and log), the notes table.
- **No issue opened.**
- **For the next note (04):**
  - **The plot's cost.** It takes one to two and a half minutes on the full range, depending on
    the load. The probe scan took 308 s at load 4–8, and β = 5.0 alone took 25 s against T5's
    7.9 s. One invocation of each still fits under the cap.
  - **The β = 3.00 near-miss** (the orchestrator's scratch probe, at the user's question):
    - At β = 3.00 the EW peak Σ_eff (0.036748) is 2.9e-4 short of c = 1/(3β²). So the field
      approaches the kinematic velocity without reaching it: g = 1 + βπ/M_P reaches its minimum,
      +0.0225, at 46.5 GeV.
    - T_J nearly stalls while g < 0.1. That lasts 9.3 e-folds, during which φ falls from 4.40 to
      1.43.
    - The QCD kick then starts. The field reaches the wall at 1.68 GeV with βπ/M_P = −0.875,
      before the kick brings it to the kinematic velocity. This happens even though 1.68 GeV lies
      inside QCD's reachable interval (hot root about 1.99 GeV).
    - The stall lengthens as the deficit closes:

      | β | g < 0.1 for (e-folds) | captured | first bounce (GeV) |
      |---|---|---|---|
      | 2.95 | 2.9 | yes (QCD) | 1.96 |
      | 2.99 | 7.1 | yes (QCD) | 1.83 |
      | 3.00 | 9.3 | no | 1.68 |
      | 3.005 | 11.0 | no | 14.3 |
      | 3.01 | 13.5 | no | 34.7 |
      | 3.0115 | 13.7 | no | 38.2 |

    - At 3.005, and from 3.01, the wall comes during or just after the EW stall, before QCD.
    - The 0.05 grid does not resolve this, since only β = 3.00 falls between 2.95 and 3.05.
      Whether 04 adds finer β near the thresholds is the user's call; as written it does not.

### 5.4a U6, before 04 is dispatched (2026-10-14)

After the review of 03, the user asked about the near-miss at β = 3.00 (§5.4, "For the next note")
and ruled that **04 adds a fine β grid near 3.0 (U6)**. The orchestrator probed 14 β of the fine
range at `9ab3e65` (scratch; load 2–3; 1.4–3.4 s each):
- **QCD** still captures at β = 2.991–2.997, and its first bounce falls from 1.802 to 1.644 GeV.
  At 2.999 nothing captures, and the first bounce is at 1.641 GeV.
- **Nothing captures at 3.012–3.015**, where the first bounce is at 39.2, 41.0 and 44.1 GeV.
- **EW captures from β = 3.020**, which is above 01's analytic threshold of 3.0118: the capture is
  at 49.36 GeV and the first bounce at 49.72 GeV. At 3.045 they are 52.28 and 61.18 GeV.

So nothing captures between about 2.998 and 3.017, and the first bounce rises across that window
from about 1.6 GeV to about 44 GeV. Prompt 04 is amended:
- **S5:** the fine scan, β = 2.980–3.040 in steps of 0.001 (61 values), with its own CSV and figure;
- **S3:** β = 3.00, which lies on both grids, must be equal in the two CSVs;
- **S4:** a new item 8, with a table of the fine rows, and the EW dynamical threshold taken from
  the fine grid;
- **§2–§5:** the checks, the acceptance, the budget and the file list.

The README records U6 in §6.1, §2 and §5 rule 12. The fine figure shows uncaptured rows only as edge
marks (note 03's ruling), so the table in the verification document carries their deliveries.

**U7, the same day.** The fine figure would show the uncaptured window only as edge marks, because
of note 03's ruling. The user therefore ruled that the first bounce of an uncaptured history is
drawn (U7). That needs a change to the plot script, which 04 may not make, so **prompt 03b** is
inserted before 04 to make it:
- an open grey circle in the upper panel, with its own legend entry;
- the edge mark kept;
- the lower panel unchanged;
- 03's 15 tests unchanged.

Its figures are checked on `scan79.csv`, and on the orchestrator's 14-row probe near β = 3.0
(`edges.csv`). 04's gate now includes 03b, and its S4 item 8 says how the fine figure shows those
rows.

### 5.4b After prompt 03b (2026-10-14)

**03b landed in `ee4a2df`** (parent `f19332d`; Sonnet). F4 is done: the first bounce of an
uncaptured history is drawn (U7).
- **What.** `build_figure` draws, in the upper panel only, an open grey circle (size 6, edge `0.45`)
  at (β, `T_deliver_GeV`) for every not-captured `ok` row that has a delivery: one `ax.plot`, gid
  `marker-delivery-uncaptured`, a legend entry "first bounce, not captured" only if such a point
  exists, and the points are in the upper y-limits. The edge mark and its entry, the failure rows,
  the lower panel and the captured series are unchanged.
- **Tests.** One new class, `TestUncapturedDeliveries`, 6 tests. 03's 15 are unedited and pass.
  Suites: CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 69, tools 64 → 70.
- **Figures** (scratch): `edges.csv` shows the four rings and the rise of the first bounce from
  1.64 GeV (β = 2.999) to 39–44 GeV (3.012–3.015), with 3.012 and 3.013 slightly overlapping but
  two; `scan79.csv` shows the rings at 1.10 and 3.00. Both are legible in colour and in greyscale.
- **Breakages.** Each of the three fails a named new test (log).
- **Not 03b's, for the user.** The e⁺e⁻ band label overlaps the uncaptured edge mark at β = 2.999 on
  `edges.csv`; it is not made worse. No issue opened.
- **For 04.** Rings 0.001 apart overlap slightly at the figure's scale; look at the fine figure.

**Orchestrator review of prompt 03b** (2026-10-14, Claude Opus 5.5, against `ee4a2df`; note
[`orchestrator/prompt-03b.md`](orchestrator/prompt-03b.md) §3). All nine checks pass.
- **Scope and rule 13.** The four files of the prompt's §5, and the untracked set unchanged. `git
  diff` of `extract_common.py`, `tools/surfing_scan.py`, `ComputeTargets/` and `config/` is empty.
  The test module's diff only adds lines: one in its docstring and the new class at the end. Black
  is clean on both files.
- **By reading.** The diff of `build_figure` is what the prompt asks:
  - one `ax.plot` on the upper panel only, gid `marker-delivery-uncaptured`;
  - over the `UNCAPTURED` rows whose `T_deliver_GeV` is not `None`;
  - an open circle with a `0.45` edge, at z 7, under the captured disc's 8;
  - a legend handle only when the series is non-empty;
  - its points added to `ys` before `set_ylim`.
  Nothing else in the figure is touched.
- **By probe.** The committed script was run on `scan79.csv` (53 s) and `edges.csv` (4 s) into the
  scratchpad, and both PNGs and greyscale copies were looked at:
  - on `scan79.csv`, rings at 1.10 (0.143 GeV) and 3.00 (1.68 GeV); the rest is as in 03's review;
  - on `edges.csv`, four rings, with the rise from 1.64 GeV (2.999) to 39–44 GeV (3.012–3.015)
    plain, and 3.012 and 3.013 overlapping slightly but still two;
  - the rings are told from the black discs by fill, in greyscale too.
  Independently of the tests, the new artist's data, read off the upper panel, equal the CSVs'
  uncaptured (β, `T_deliver_GeV`) pairs exactly on both files. It is absent from the lower panel and
  from `marker-delivery`, and the upper panel has no collection added.
- **03's 15 tests** pass unedited, run alone (2.2 s).
- **The breakages.** All three replay with `git apply` in bash, and each fails exactly the tests the
  log names: 7, 1 and 5. The tree is clean after each. Breakage 1 also fails 03's `test_a`, which
  pins `marker-delivery` to captured rows; that is the pin working, not an edit.
- **Suites at `ee4a2df`**, serially: 18, 61 + 115, 69 and 70, all OK, at load 4–36 (another
  process loaded the machine during ComputeTargets).
- **A correction to the log** (its "Observations not acted on", second item). The log says 04's fine
  figure has a 0.06-wide axis, so its rings would be 1.7 times farther apart than on `edges.csv`.
  But the plot rounds the CSV's β range outwards to 0.05 (P4), so 2.980–3.040 is drawn on
  2.95–3.05. That is the same 0.1-wide axis as `edges.csv`. So in 04's fine figure every pair of
  adjacent uncaptured rings overlaps as 3.012 and 3.013 do here, in a chain of about twenty across
  2.998–3.019. The log is left as written. Not acted on: the rings stay countable, and 04's
  verification table carries the values.
- **Residue fixed here:** the SHA for "this commit" (board and log), the notes table.
- **No issue opened.**
- **For the user, before 04:** the e⁺e⁻ band's label overlaps the uncaptured edge mark at β = 2.999
  on `edges.csv`. This was there before 03b, which leaves it unchanged. 04's fine figure will put
  some twenty edge marks under that label.
- **For the next note (04):**
  - the plot takes 53 s on the 79-point range at load about 4, and 4 s on a 0.1-wide range;
  - the fine figure's axis is 2.95–3.05, not 2.98–3.04;
  - the label overlap above is the user's to rule on.

### 5.5 After prompt 04 (2026-10-14)

**04 ran the scan in `90d44f8`** (parent `e7ed715`; Opus). The log is
[`logs/04-run-the-scan.md`](logs/04-run-the-scan.md), and the record is
`.documents/surfing-temperature-verification.md` §1. S1–S5 are done, and the gate "Scan" holds. No
code changed, and no Ray, store or PRyMordial was used.
- **The runs.** All ran on `e7ed715`, whose code is `ee4a2df`'s. Each sidecar has one entry with
  `git_tracked_changes: false`.
  - **S1**, one chunk (load 4.79 at its start): 79 β in 287 s, exit 0, `wall_s` summing to 280 s,
    the longest 10.3 s at β = 5.00.
  - **S5**, one chunk (load 9.86): 61 β in 155 s, exit 0.

  Together they took 442 s against P7's 60 minutes. Nothing was cut off or resumed. The figures
  took 52 s and 4 s.
- **The checks.** Both CSVs have their grid's β once each, as `repr` strings, in order, and every
  row is `ok`. Each sidecar's options are the command's. S3: at β = 1.5 and 4.0, the re-run equals
  the scan in all 23 columns but `wall_s`, and β = 3.0 is equal in the two CSVs. The eight outputs
  total 300 457 bytes.
- **What the scan shows** (measurements, README §5 rule 10):
  - **S1.** QCD captures at 1.15–2.95 and EW at 3.05–5.00. Nothing captures at 1.10 or 3.00, and
    μ/π and e⁺e⁻ never capture. Every first bounce is a turning point above T_stop; the lowest is
    0.143 GeV at 1.10. A second dip appears at QCD 2.60–2.80 and at EW 3.15–5.00.
    `multiple_crossings` is false everywhere.
  - **S5.** QCD captures up to 2.998 and EW from 3.020; nothing captures at 2.999–3.019 (21 β).
    Across that window the first bounce rises from 1.64 to 48.8 GeV, most steeply at 3.003–3.005.
    There is no jump by the document's criterion: a change more than three times the larger of its
    neighbours.
  - **The dynamical thresholds.** EW's is 3.020 on the fine grid (analytic 3.0118). QCD's lies in
    (1.10, 1.15] (analytic 1.1074).
  - **The overshoot.** T_max/T_capture is 1.082–1.372. At 9 β the maximum is at the bounce
    (N_deliver − N_max < 0.01). At β = 1.2 and 2.0 the ratio is 1.125 and 1.141; the audit's F1,
    at M = 0.5, has 1.125 and 1.141 from its own table.
  - **The oscillator.** On the 43 rows with a second dip, the second half-period is within 1.1 % of
    π/√(D − 1/4), with D from the production cosmology at T_hot. The decays are within 3.7 % of
    0.5 per e-fold. The first half-period differs by up to 18.5 %, at β = 3.15.
- **Suites,** before the scans, serially: 18, 61 + 115, 69 and 70, all OK, at load 4.7–17.5. They
  were not re-run, since no code changed.
- **Deviations:** 1 STRUCTURALLY REQUIRED (`multiple_crossings` read from `reachability_roots`, since
  the CSV has no such column) and 7 IMPLEMENTATION CHOICE, none UNINTENDED DRIFT.
- **Not acted on** (the log):
  - The e⁺e⁻ label's second line, "reachable, not surfed", lies across the edge marks at β =
    2.999–3.006 in the fine figure. The marks are drawn over it, and no ring or captured marker is
    hidden. This is for the user to revisit (note 04 §0).
  - Adjacent rings overlap where the first bounce rises slowly.
  - The fine figure's EW band starts at the root grid's 3.015.
  - §5.3's "32 of 77" wall-type maxima is not reproduced by the log's criteria.
  - The wall's timing sets which β near 3.0 are captured, so those edges may move with Λ after
    `jordan-normalization`.
- **No issue opened.** `.documents/OPEN_ISSUES.md` is untouched.

**Orchestrator review of prompt 04** (2026-10-14, Claude Opus 5.5, against `90d44f8`; note
[`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) §3). All eleven checks pass.
- **Scope.** The eleven files of note 04 §0: eight outputs, the verification document, the log and
  the board. `git diff` of `*.py` is empty, and the untracked set is unchanged.
- **The sidecars.** One entry each, `git_head` `e7ed715`, `git_tracked_changes: false`, `resumed`
  false, `skipped` 0. The options equal the commands, and each `beta_grid` is the prompt's grid.
- **The CSVs.** Each header equals `COLUMNS`. There are 79 and 61 rows, with the β strings equal to
  the grids, no duplicates, and the inputs as commanded.
- **By reproduction** (`git diff 61e0c42 90d44f8` of the scan code is empty):
  - all 79 S1 rows equal the orchestrator's probe `scan79.csv` (at `61e0c42`) in every column but
    `wall_s`, as strings;
  - the 13 β that S5 shares with `edges.csv` (at `9ab3e65`) are equal in the same way;
  - the orchestrator's own run of β = 2.998, 3.016, 3.019 and 5.00 at `90d44f8` equals the
    committed rows (19 s, load 6.5);
  - β = 3.0 is equal in the two CSVs.
- **The figures.** Both are redrawn from the committed CSVs into the scratchpad (62 s and 4 s), and
  equal the committed PNGs pixel for pixel. Looked at:
  - S1's is as in the reviews of 03 and 03b;
  - S5's shows QCD to 2.998, the chain of 21 rings rising from 1.64 to 48.8 GeV, and EW from 3.020;
  - the e⁺e⁻ label's second line crosses the edge marks at about 2.999–3.006, as the log says.
- **The document, recomputed** by the orchestrator's own script from the committed CSVs. These
  agree with the document to its digits:
  - the captures per kick on both grids;
  - the four ratio ranges per kick;
  - the second-dip ranges;
  - the overshoot groups. Nine rows have N_deliver − N_max ≤ 1.3e-6, and the next smallest is
    0.022 at 1.55, so the 0.01 cut separates cleanly.

  At β = 2.7, 3.15, 4.0 and 5.0 the oscillator values agree too: D, the predicted half-period, the
  measured half-periods and the decays (3.15: 6.08 predicted against 7.20 and 6.05 measured). No
  sentence tests a measurement against the paper or the audit.
- **"32 of 77" (D3), resolved.** The source is note 02, which counted 32 captured β whose *last*
  `max` crossing is the wall's (within 1e-5 e-folds of the bounce). §5.3 above restated that as the
  *largest* T_J. It is the largest only where it is also the only maximum before a dip-free
  bounce: the 9 rows of the log's criterion. So §5.3's sentence is the orchestrator's misstatement,
  and the CSV is consistent with note 02. §5.3 is left as written (invariant 6).
- **The cost.** 442 s for S1 and S5 against P7's 60 minutes. The outputs total 300 457 bytes.
- **Suites at `90d44f8`**, serially: 18, 61 + 115, 69 and 70, all OK, at load 3.8–5.3.
- **Residue fixed here:** the SHA for "this commit" (board and log), and the notes table.
- **No issue opened.**
- **For the next note (05):**
  - **README §0.4 and §7 need qualifying in the handover.** They say the capture will not move with
    Λ. The fine grid shows that near β = 3.0 the wall's timing decides *whether* the first episode
    is captured (2.998–3.019; log, "Observations not acted on"). So the edges of the uncaptured
    window may move after `jordan-normalization`, and the handover should say so.
  - The user's ruling on the e⁺e⁻ label in the fine figure.

**U8, after the review (2026-10-14).** The user ruled that the e⁺e⁻ band label is tidied (U8). In
the fine figure its second line lies across the edge marks at β = 2.999–3.006. 04 could not change
the plot script, so **prompt 04b** is inserted before 05. It:
- moves the label clear of the marks, by more room below the coldest band or another placement
  that hides nothing;
- keeps the label's words and the edge marks unchanged;
- redraws both committed figures from the committed CSVs, and adds a short §2 to the verification
  document.

It is costed as a tidy-up: no new test, no breakage, and only the `tools` suite. 05's gate now
includes 04b.

### 5.5a After prompt 04b (2026-10-14)

**04b landed in `a65bd74`** (parent `f30a3c0`; Sonnet). The log is
[`logs/04b-tidy-band-label.md`](logs/04b-tidy-band-label.md). F5 is done. No history, Ray or store was
used, and no issue opened.
- **The change.** In `tools/plot_surfing_temperature.py` the upper panel's lower y-limit is
  `min(ys) / 8.0`, from `/ 3.0`, with a comment saying why. Nothing else in the script moved.
- **The gap.** On the fine figure the label's lowest ink is 12 blank pixel rows (about 9 pt at 100 dpi)
  above the edge marks' top. The label covers the marks at β = 2.999–3.007.
- **The figures.** Both are redrawn in place from the committed CSVs. Against the old PNGs the full
  figure differs only in x 31–819, y 55–520 (the upper panel). The fine one differs in the upper panel
  and, by sub-pixel anti-aliasing, in the lower panel (log D1: the constrained layout moves it by a
  fraction of a pixel when the new 10⁻⁵ tick label appears).
- **The suite.** `tools`: 70 OK, as before; `black` clean. No other suite was run.
- **The document.** `.documents/surfing-temperature-verification.md` has a new §2; §1 is unchanged.

**Orchestrator review of prompt 04b** (2026-10-14, Claude Opus 5.5, against `a65bd74`; note
[`orchestrator/prompt-04b.md`](orchestrator/prompt-04b.md) §3). Five of six checks pass. Check 3
found a third of a pixel more than §1.2 allows, and the user accepted it.
- **Scope.** The eight files of the prompt's §5 list. No CSV, sidecar, test, `extract_common.py`,
  `tools/surfing_scan.py` or index change, and the untracked set is unchanged.
- **The script.** The diff is the y-limit line and a two-line comment. `black --check` is clean.
- **The figures.** Redrawn by the orchestrator from the committed CSVs (63 s and 2 s), they equal
  the committed PNGs pixel for pixel. The titles still read "git HEAD e7ed715". Against `90d44f8`:
  - the full figure differs only inside the upper panel;
  - **the fine figure's lower panel differs too** (rows 545–727). Measured on the axes' window
    extents at divisors 3, 6, 8 and 10: at 8 alone, the 10⁻⁵ tick label lands on the upper panel's
    bottom edge (lower limit 9.93e-6), and the constrained layout raises the upper panel's bottom by
    0.6 px and lowers the lower panel's top by 0.3 px. Divisors 6 and 10 leave both panels where
    they were. The note's "8 or 10, either acceptable" missed this, since its probe drew PNGs only
    at 6 and 10.
  - **Ruled by the user (2026-10-14):** divisor 8 is accepted with the shift, and nothing is
    redrawn. Log D1 is re-tagged IMPLEMENTATION CHOICE, since the shift follows from choosing 8 and
    is not forced. The verification document gets a new §2.1 recording the measurement, because its
    §2 says "Nothing else is drawn differently".
- **The clearance.** In both PNGs the e⁺e⁻ label reads and is clear of every edge mark. In the fine
  figure, nine marks (2.999–3.007) lie under the label's span. The agent measured 12 blank pixel rows
  between them; the note's window-extent probe gives +7.5 pt at divisor 8.
- **The suite** at `a65bd74`: `tools` 70 OK, 13 s, at load 7.9.
- **The records.** Row 04b, F5 and this §5.5a are as the note asks. The log is in §5.1's form. The
  verification document's §1 is untouched (the diff only adds lines).
- **Residue fixed here:**
  - the SHA for "this commit" (board and log);
  - the notes table;
  - D1's tag;
  - the verification document's §2.1.
- **No issue opened.**
- **For the next note (05):**
  - README §0.4/§7's capture claim needs qualifying in the handover (from §5.5);
  - the figures' lower limit is now `min(ys) / 8.0`. 05 changes no figure.

**U9, before 05 (2026-10-14).** Writing note 05, the orchestrator read how `tools/surfing_scan.py`
sets Λ. It imports `LAMBDA_EV` = 1e-3 eV from `tools/history_and_bbn.py` and has no option for it,
so README §0.4's and §7's "a re-run after `jordan-normalization` is one command" holds only if that
campaign changes the tool. **The user ruled (U9) that the handover says so and that an issue is
opened**, so that the planner of `jordan-normalization` finds it: `[05-surfing-scan-runs-at-a-fixed-lambda]`,
§3 above and the index's §1.13. 05's handover names it, and 05's records count it.
