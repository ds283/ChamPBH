# surfing-temperature campaign — implementation state

**Last updated:** 2026-10-14 · **Status: PLANNED — 0 of 5 prompts written; U1–U4 ruled by the
user 2026-10-14; awaiting the user's ruling on README §6.2 (P1–P10).**
The plan is [`README.md`](README.md), written on 2026-10-14 on `main` at `f41b104`. The branch
`surfing-temperature` is cut with the planning commit (P10).

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
| 01 | Reachability roots | `reachability_roots`: kick windows, hot and cold roots, stability sign | Sonnet | ⬜ | ⬜ | — | — |
| 02 | Surfing turning points and the driver | `surfing_turning_points` on the dense output; `tools/surfing_scan.py`; five β run, cost projected | Opus | ⬜ | ⬜ | — | — |
| 03 | The plot | `tools/plot_surfing_temperature.py`: roots and measured temperatures against β, two panels | Sonnet | ⬜ | ⬜ | — | — |
| 04 | Run the scan | 79 β, 1.10–5.00, CSV and figure committed; verification document (U4) | Opus | ⬜ | ⬜ | — | — |
| 05 | Close | documents, handover, index, board | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.2.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Roots | on a stub Σ the roots equal the analytic ones; on the production EOS the peaks and thresholds equal README §0.3's and the paper's, and β_th(root) = β | 01 | ⬜ |
| Turning points | on stub interpolants every crossing of β π/M_P = −1 is found in order and with its direction, none after the first bounce; on a real history \|1 + β π/M_P\| < 1e-8 at each, and delivery equals `first_bounce` | 02 | ⬜ |
| Scan | every β of the grid has a CSV row (a failure is a row with its reason); every first bounce lies above T_stop; the figure is drawn from the CSV alone | 04 | ⬜ |

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
