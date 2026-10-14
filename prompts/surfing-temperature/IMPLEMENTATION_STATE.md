# surfing-temperature campaign — implementation state

**Last updated:** 2026-10-14 · **Status: LIVE — 5 of 5 prompts written, 0 landed. U1–U4 and P1–P10
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
| 01 | [Reachability roots](01-reachability-roots.md) | `find_kicks`, `reachability_roots`: kick windows, hot and cold roots, stability sign, merged intervals | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |
| 02 | [Surfing turning points and the driver](02-surfing-turning-points-and-driver.md) | `surfing_turning_points` on the dense output; `tools/surfing_scan.py`; five β run, cost projected | Opus | ✍️ 2026-10-14 | ⬜ | — | — |
| 03 | [The plot](03-plot-surfing-temperature.md) | `tools/plot_surfing_temperature.py`: roots and measured temperatures against β, two panels | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |
| 04 | [Run the scan](04-run-the-scan.md) | 79 β, 1.10–5.00, CSV and figure committed; verification document (U4) | Opus | ✍️ 2026-10-14 | ⬜ | — | — |
| 05 | [Close](05-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-14 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.2.
*Written 2026-10-14 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **R1** | 01 | `find_kicks`: Σ's peaks and the troughs between them, refined on the spline; labels EW, QCD, ee (P5) | ⬜ |
| **R2** | 01 | `reachability_roots`: hot and cold roots per kick, slopes, merged intervals, the `multiple_crossings` flag | ⬜ |
| **R3** | 01 | `ComputeTargets/tests/test_reachability_roots.py`: stub Σ, a merged stub, the production EOS | ⬜ |
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
| Roots | on a stub Σ the roots equal the analytic ones; on the production EOS the peaks and thresholds equal README §0.3's and the paper's, and β_th(root) = β | 01 | ⬜ |
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
