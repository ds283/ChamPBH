# electroweak-eos campaign — implementation state

**Last updated:** 2026-11-12 · **Status: PLANNED — 0 of 3 landed.** Planned 2026-11-12 at the user's
request. U1–U7 are for the user, and P1–P12 stand unless ruled otherwise (README §6). The plan is
[`README.md`](README.md), on branch `electroweak-eos`, cut from `main` at `ec6bca8`.

The campaign replaces ChamPBH's w table, which above about 100 GeV is a free gas at vacuum masses, with
the table that follows Laine & Meyer through the electroweak crossover. That is the notebook's rows
together with the points of 2026-11-12, as recorded in the kicking-function audit's addendum (`ec6bca8`).
It then measures the electroweak-capture threshold and the change to a sample of the `2026.10.0` run's
histories, and hands a rebuild under `2026.11.0` to the user.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyModels/GenericEOS/` (`Xav_EOS_data.csv`, `Xav_EOS_spline`), `tools/` (the build script),
`config/version.py` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no issue assigned until the
user rules; P11)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | Install the table | the table by U2; the build script and test; `VERSION_LABEL` 2026.11.0; re-pinned tests and golden histories; the docstring; the provenance issue | Opus | ⬜ | ⬜ | | |
| 02 | Measure the effect | the EW threshold; the sample of `2026.10.0` histories old against new; the bath drift | Opus | ⬜ | ⬜ | | |
| 03 | Documents and handover | the documents, issues, indexes and the handover; the close | Sonnet | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed, and none before
U1–U7 are ruled.

Orchestrator notes, one prompt at a time, in `orchestrator/`.

### 1.1 Items

None yet.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Table | the build script regenerates the CSV byte for byte; the class evaluates the new table; every moved pin listed with a reason; the spline's coupling below the first changed row measured | 01 | ⬜ |
| Threshold | the EW-capture threshold bracketed to 0.001 in β, beside 3.020 (today) and 3.00285 (notebook) | 02 | ⬜ |
| Effect | every sampled history's change in Y_p, D/H, ΔN_eff, capture temperature and turning points, set beside the figures' resolution | 02 | ⬜ |
| Close | documents, issues, indexes and handover written; suites at their counts | 03 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

None yet. `[00-kicking-function-table-has-no-provenance-in-the-repository]` (`review-remediation` board)
is proposed for assignment (P11).

## 4. Resolved issues

None.

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-09): CosmologyModels 26, ComputeTargets 665 (219 + 446),
ObjectFactories 163, tools 277, RunDrivers 76. Prompt 01 measures the counts again before it changes
anything.
