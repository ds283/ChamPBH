# electroweak-eos campaign — implementation state

**Last updated:** 2026-11-12 · **Status: LIVE — 1 of 4 landed (01a); 01 held at its stop, to resume on 01a (§8 of the README).** Planned 2026-11-12 at the user's
request. U1–U6 ruled by the user as proposed the same day, and U7 (the provenance of the rows) ruled out of
scope. P1–P12 stand, with P11 amended by U7 (README §6). The plan is
[`README.md`](README.md), on branch `electroweak-eos`, cut from `main` at `ec6bca8`.

The campaign replaces ChamPBH's w table, which above about 100 GeV is a free gas at vacuum masses, with
the table that follows Laine & Meyer through the electroweak crossover. That is the notebook's rows
together with the points of 2026-11-12, as recorded in the kicking-function audit's addendum (`ec6bca8`).
It then measures the electroweak-capture threshold and the change to a sample of the `2026.10.0` run's
histories, and hands a rebuild under `2026.11.0` to the user.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyModels/GenericEOS/` (`Xav_EOS_data.csv`, `Xav_EOS_spline`), `tools/` (the build script),
`config/version.py` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no issue assigned; P11 as
amended by U7)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01a | Resolved turning points | an interior pair of sign changes of π kept only if the solver resolves it (README §8); lands before 01's commit | Opus | ✍️ | ✅ | this commit | [`logs/01a-resolved-turning-points.md`](logs/01a-resolved-turning-points.md) |
| 01 | Install the table | the table by U2 (b); the build script and test; `VERSION_LABEL` 2026.11.0; re-pinned tests and golden histories; the docstring | Opus | ✍️ ⏸ 01a | ⬜ | | |
| 02 | Measure the effect | the EW threshold; the sample of `2026.10.0` histories old against new; the bath drift | Opus | ⬜ | ⬜ | | |
| 03 | Documents and handover | the documents, issues, indexes and the handover; the close | Sonnet | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. 01a was inserted on
2026-11-12 while 01 was held at its stop; its proposals U8–U11 (README §8.3) were
ruled as proposed by the user the same day: "U8 to U11 are fine as proposed; write 01a".

Orchestrator notes, one prompt at a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written
2026-11-12 with the prompt and dispatched the same day at `468266d`; 01's handoff at its stop,
[`prompt-01-handoff.md`](orchestrator/prompt-01-handoff.md); [`prompt-01a.md`](orchestrator/prompt-01a.md), written
2026-11-12 with prompt 01a and not yet dispatched.

**Planner's probe for 01 (2026-11-12, at `b3d5906`, not committed).** The U2 (b) table (170 rows) was built in a
scratch worktree and the suites run on it.
- 22 tests fail because of the table, and pass with the old one restored. CosmologyModels passes in full.
- Above 386.7 GeV the table has Σ = 0, so a history that starts at rest is not kicked until 387 GeV. On the
  golden β = 1.5 history a wall-side turning point at N = 3.94 (T_J ≈ 390 GeV) becomes `first_bounce`
  (it was at N = 18.64).
- The note makes this a stop for the user's ruling before anything that reads `first_bounce` is re-pinned
  (note §0 and ruling 3).

**01 at its stop (2026-11-12, nothing committed).** 01 was dispatched at `468266d` and stopped at its note's
ruling 3. The early wall/plateau pair is the interpolant read below the solver's tolerance, not the field
turning, and `first_bounce` does not move (README §8.1). It also stopped on a failure outside §0's list,
`test_dissipation_store` test_10, which pins `VERSION_LABEL`. **Ruled by the user, 2026-11-12:** "Handle it
in a separate classifier prompt; re-pin test_10 to 2026.11.0". 01's work so far (the table installed, the
build script and its test, the label, the reachability pins and test_10 re-pinned) stays uncommitted in the
main checkout until 01a lands.

**01a landed (2026-11-12, this commit, on branch `electroweak-eos-01a` in a worktree, U10).** An interior pair of sign
changes of π is kept only if its excursion exceeds the step's tolerance on π (README §8.3 U8); a dropped pair is counted in
`IntegrationResult.unresolved_turning_point_pairs`, which is not stored (U9). 01a stopped once: its scans to T_CMB found
two late near-stall pairs dropped on the old table. **Ruled by the user, 2026-11-12:** "For the stop issue, choose (a).
For the `dropped_drains` issue, add to the allowed differences." Gate Resolved holds as so ruled (log 01a §3). It opens
one issue (§3) and closes `[01-turning-points-read-below-the-solver-tolerance]` (§4). On 01's table, 4 of the 22 tests
pass with it and 18 still fail; the list is in log 01a §6, for 01's resume.

### 1.1 Items

None yet.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Resolved | on the old table, suites, goldens and 01's scan bitwise unchanged; on the new table, the early pair dropped wherever 01 saw it (README §8.2). *As ruled by the user 2026-11-12 at 01a's stop:* on the old table no pair dropped down to 100 GeV and everything else bitwise, the late near-stall pairs (two in 36 histories to T_CMB) recorded; on the new table `dropped_drains` too may differ at α = 0 | 01a | ✅ 2026-11-12, as ruled (log 01a §3) |
| Table | the build script regenerates the CSV byte for byte; the class evaluates the new table; every moved pin listed with a reason; the spline's coupling below the first changed row measured | 01 | ⬜ |
| Threshold | the EW-capture threshold bracketed to 0.001 in β, beside 3.020 (today) and 3.00285 (notebook) | 02 | ⬜ |
| Effect | every sampled history's change in Y_p, D/H, ΔN_eff, capture temperature and turning points, set beside the figures' resolution | 02 | ⬜ |
| Close | documents, issues, indexes and handover written; suites at their counts | 03 | ⬜ |

## 3. Active and unresolved issues

- **`[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]`** (opened 2026-11-12, by prompt 01a,
  from its check of `surfing_turning_points`, prompt §2). `surfing_turning_points` samples g = 1 + βπ/M_P on each step
  as `observe_turning_points` samples π, and keeps every sign change of g, with no tolerance.
  - *Measured* (log 01a §5, `.documents/electroweak-eos/turning-points/out/surfing_survey.txt`): not in the early
    pair's circumstances, where g ≈ 1 (no crossing within 385–389 GeV in any scanned history). But in the captured
    histories of the scans (β = 10 and 20, M = 10⁻³ and 0.5 M_P, both tables, to T_CMB) it records 22 to 138 crossings,
    most of them with T_J equal to 10⁻⁹ or better, over tens of e-folds: on the old table at β = 20, 71 of 88 within
    10⁻⁹ of 846.17002981 GeV, some 7.5 × 10⁻⁶ apart in N. That is g held at zero on the surfing attractor with its sign
    set by the solver's error. The excursions of g between them were not measured against g's tolerance.
  - *Impact:* `tools/surfing_scan.py` counts them in `n_crossings`. Its capture, largest-maximum and second-dip
    columns read the first crossings, which are resolved. Nothing stored reads it.
  - *Next step:* none proposed; for the user. A rule like 01a's for g would need g's tolerance from π's.

### 3.1 Assigned to this campaign

None. `[00-kicking-function-table-has-no-provenance-in-the-repository]` was proposed for assignment and
left out of scope by the user's ruling on U7 (2026-11-12); it stays on the `review-remediation` board.

## 4. Resolved issues

- **`[01-turning-points-read-below-the-solver-tolerance]`** (opened 2026-11-12, from 01's stop).
  `observe_turning_points` samples π inside each accepted step and records every sign change of the
  interpolant as a turning point, however small. Where π is below the solver's absolute tolerance, the
  interpolant can change sign twice inside a step whose nodes agree in sign, and that is recorded as a
  wall and a plateau although the field never turns.
  - *Measured* (README §8.1): on the new table, 14 of 20 scanned histories started at rest have such a
    pair at T_J = 385–389 GeV, where Σ switches on (golden β = 1.5: π at the nodes −3.97 × 10⁻¹² and
    −1.82 × 10⁻⁹, interior +6.74 × 10⁻¹⁰, atol 10⁻⁸); none on the old table; gone at atol ≤ 10⁻¹².
  - *Impact:* `first_bounce` is unaffected. `turning_points`, the fixed-law drains, the one-loop bounce rows
    (two event rows at the head, every ordinal +2) and the first candidate's nonadiabaticity are.
  - *Next step:* 01a (README §8.2), under U8–U11 as ruled 2026-11-12.
  - **Assigned (2026-11-12):** to this campaign's prompt 01a, by the user's ruling "Handle it in a separate
    classifier prompt".
  - **Closed (2026-11-12) by prompt 01a, this commit.** `observe_turning_points` now keeps an interior pair only if
    its excursion exceeds τ = atol + rtol · max(|π_low|, |π_high|) (`resolved_sign_changes`), and counts a dropped
    pair in `IntegrationResult.unresolved_turning_point_pairs`. On the old table, over 01's 36 scanned histories to
    T_CMB, no pair is dropped down to 100 GeV and everything else is bitwise its baseline, but for two late
    near-stall pairs (β = 10 and 20 at M = 10⁻³ M_P, T_J ≈ 24 eV and 0.46 keV, excursions 0.175 τ and 0.95 τ), which
    the user ruled at 01a's stop to be the rule working as ruled; the suites and the three golden records pass
    unchanged. On 01's table the pair is dropped in every scanned history and every
    test integration where it was seen, and nowhere else (log 01a §3).

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-09): CosmologyModels 26, ComputeTargets 665 (219 + 446),
ObjectFactories 163, tools 277, RunDrivers 76. Prompt 01 measures the counts again before it changes
anything.

After 01a (2026-11-12, in its worktree, log 01a §4): CosmologyModels 26, ComputeTargets 684 (219 + 465; 01a's
`test_resolved_turning_points` adds 19), ObjectFactories 163, tools 277, RunDrivers 76. Two of them fail in the worktree
only, before and after, for its path (`test_registry_config`, `test_registered_run`). `CLAUDE.md`'s counts wait for 01.
