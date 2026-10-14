# surfing-temperature campaign — implementation state

**Last updated:** 2026-10-14 · **Status: LIVE — 5 of 5 prompts written, 1 of 5 landed (01). U1–U5 and P1–P10
ruled by the user on 2026-10-14 (README §6); P1–P10 as proposed.**
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
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section yet; none
opened or assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Reachability roots](01-reachability-roots.md) | `find_kicks`, `reachability_roots`: kick windows, hot and cold roots, stability sign, merged intervals | Sonnet | ✍️ 2026-10-14 | ✅ 2026-10-14 | `2c98370` | [log](logs/01-reachability-roots.md) |
| 02 | [Surfing turning points and the driver](02-surfing-turning-points-and-driver.md) | `surfing_turning_points` on the dense output; `tools/surfing_scan.py`; five β run, cost projected | Opus | ✍️ 2026-10-14 | ⬜ | — | — |
| 03 | [The plot](03-plot-surfing-temperature.md) | `tools/plot_surfing_temperature.py`: roots and measured temperatures against β, two panels | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |
| 04 | [Run the scan](04-run-the-scan.md) | 79 β, 1.10–5.00, CSV and figure committed; verification document (U4) | Opus | ✍️ 2026-10-14 | ⬜ | — | — |
| 05 | [Close](05-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.2.
*Written 2026-10-14 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch.

| Note | Commit | Used for |
|---|---|---|
| [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) | `2f38193` | 01 (reviewed 2026-10-14) |

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **R1** | 01 | `find_kicks`: Σ's peaks and the troughs between them, refined on the spline; labels EW, QCD, mu-pi, ee (P5, U5) | ✅ |
| **R2** | 01 | `reachability_roots`: hot and cold roots per kick, slopes, merged intervals, the `multiple_crossings` flag | ✅ |
| **R3** | 01 | `ComputeTargets/tests/test_reachability_roots.py`: stub Σ, a merged stub, the production EOS | ✅ |
| **T1** | 02 | `surfing_turning_points` beside `first_bounce`; not called by production | ⬜ |
| **T2** | 02 | `tools/surfing_scan.py`: options, CSV per README §4, sidecar, resume | ⬜ |
| **T3** | 02 | the summary per history: capture, largest, second dip, delivery, capture kick and its roots (U1–U3) | ⬜ |
| **T4** | 02 | `ComputeTargets/tests/test_surfing_turning_points.py`, `tools/tests/test_surfing_scan.py`; one real history | ⬜ |
| **T5** | 02 | five β run (P6), per-history cost, scan projected against P7 | ⬜ |
| **F1** | 03 | `tools/plot_surfing_temperature.py`: reads the CSV and sidecar, roots on Δβ = 0.005 (P4) | ⬜ |
| **F2** | 03 | the two-panel figure of README §4 | ⬜ |
| **F3** | 03 | `tools/tests/test_plot_surfing_temperature.py` | ⬜ |
| **S1** | 04 | the 79-point scan, chunked and resumable (U4, P7) | ⬜ |
| **S2** | 04 | the figure from the scan | ⬜ |
| **S3** | 04 | two β re-run, equal as strings but for `wall_s` | ⬜ |
| **S4** | 04 | `.documents/surfing-temperature-verification.md` §1; the outputs committed (P8) | ⬜ |
| **Z1** | 05 | `architecture-summary.md`: the new functions and tools | ⬜ |
| **Z2** | 05 | `CLAUDE.md`: the suites' counts | ⬜ |
| **Z3** | 05 | the handover, `.documents/review-remediation-verification.md` §4.14 | ⬜ |
| **Z4** | 05 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` if an issue opened | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Roots | on a stub Σ the roots equal the analytic ones; on the production EOS the peaks and thresholds equal README §0.3's and the paper's, and β_th(root) = β | 01 | ✅ 2026-10-14 (`2c98370`) |
| Turning points | on stub interpolants every crossing of β π/M_P = −1 is found in order and with its direction, none after the first bounce; on a real history \|1 + β π/M_P\| < 1e-8 at each, and delivery equals `first_bounce` | 02 | ⬜ |
| Scan | every β of the grid has a CSV row (a failure is a row with its reason); every first bounce lies above T_stop; two β reproduce as strings; the figure is drawn from the CSV alone | 04 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

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

