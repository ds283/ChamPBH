# one-loop-drain campaign — implementation state

**Last updated:** 2026-11-03 · **Status: LIVE — 1 of 8 landed (01).** Planned
2026-11-02 as `quanta-diagnostics` at the user's request, after a scoping session the same day
([`source/scoping_2026-11-02.md`](source/scoping_2026-11-02.md)); U1–U7 and P1–P16 ruled the same day,
all as proposed (README §6). **Re-planned the same day around the drain law and renamed `one-loop-drain`**
(branch and folder), at the user's rulings of README §6.3, after the premise was corrected
([`source/margins-correction_2026-11-02.md`](source/margins-correction_2026-11-02.md)) and a one-loop drain
probed ([`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md)). **README §8 is the plan;** U8–U12 ruled the same day, all as proposed (README §8.6).
The branch was cut from `reflection-restart` at `3b8a553`. Under U1 as extended by ruling 6, no prompt is
written until `reflection-restart` 04 has landed and been merged here; under U2, the rebuild of record
waits for this campaign's 08. *(2026-11-02: `reflection-restart` 04 landed and that campaign closed; its branch was
merged here (`1561b60`). The user ruled the wall-search fix into 01 (README §8.8), and the fixed-α issue came
back to §3 for it. Prompt 01 written the same day.)* *(2026-11-03: prompt 01 landed, with the user's ruling (b) on
the inside-wall stop (§3, §4); the Law gate holds, and two issues closed.)*

The campaign replaces the fixed-α drain by a one-loop law, α_eff = min(α_cap, A·2.7/ℓ)·w_gate at a
one-loop turnaround and zero at R ≤ 1, with the fixed law kept beside it, bitwise. It measures the law
across the grid before the user rules the gap prescription and the A values of record; stores the
law's model-free instrumentation (a row per bounce, the masses per sample, the regime shares and margins
per history); derives the spectrum-dependent measures in a tool outside the integrator; rehearses both
laws; and writes the figure tools.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/ScalarHistory.py`, `CosmologyConcepts/Models/AbstractModel.py` (the wall search, 01), `ComputeTargets/quanta_spectrum.py` (new),
`ObjectFactories/ScalarHistory.py`, `config/`, `main.py`, `plot_*.py`, `extract_common.py`, `tools/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.23

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

The plan as first ruled had seven prompts (README §2), none written; the re-plan replaced them (README §8.4).

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The drain law](01-the-drain-law.md) | the law and A as inputs; the regime in the loop; α_eff by regime; the gap prescription as a module attribute; the `"drain"` payload key; the tool route's options; the 3873 failure; the wall-search fix (README §8.8) | Opus | ✅ | ✅ | `b99f6a4` | [01](logs/01-the-drain-law.md) |
| 02 | [Measure the law](02-measure-the-law.md) | U11's thirty histories by the tool route, re-tuned, at three A and both gap prescriptions, with two exact shortcuts (S1, S2) spot-checked; regime shares, the gap's reach, the R·S(L) bound, Y_p, D/H, ΔN_eff against the fixed law; then U13–U14 | Opus | ✍️ | ⬜ | | |
| 03 | The store | the keys, `ScalarHistoryBounce`, the sample and history columns, the gap prescription fixed, `FINGERPRINT_FORMAT` 7, `VERSION_LABEL` | Opus | ⬜ | ⬜ | | |
| 04 | Drivers and exports | the law's options in `main.py` and the plot scripts; `histories.csv`, `fields.csv`, `bounces.csv` | Sonnet | ⬜ | ⬜ | | |
| 05 | Derivation tool | `quanta_spectrum.py` and `tools/quanta_measures.py`: n_g, n̄_j, δ_stim, E, phase slip, backreaction ratio, the R·S(L) bound | Opus | ⬜ | ⬜ | | |
| 06 | Rehearse | the fixed law base against tip, bitwise; the one-loop law interrupted and resumed, by value | Opus | ⬜ | ⬜ | | |
| 07 | Figure tools | the grid figure and the per-history figure | Sonnet | ⬜ | ⬜ | | |
| 08 | Close | the verification document, the law and measures for the paper, the handover for the rebuild, the indexes | Opus | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed.

Orchestrator notes, one prompt at a time, in `orchestrator/`:
- [`prompt-01.md`](orchestrator/prompt-01.md) (`b1415a3`) — used for 01.

**Orchestrator review of prompt 01 (2026-11-03)**, by orchestrator note 01 §3, on `b99f6a4`. Passed; seven
checks:
1. **Scope.**
   - The commit's parent is `b1415a3`, the note's. It holds ruling 9's 36 files and nothing else, and nothing is
     pushed. Only `var/` outputs are untracked.
   - The diff over `config`, `main.py`, the plot scripts, `ObjectFactories` and `tools/outward_step_scan.py` is
     empty.
   - In `AbstractModel.py` the diff adds the predicate, its cache and the one early return in
     `classical_turning_point`. `_search_log_field`, `WALL_ROUNDING_QUANTA` and `WALL_SEARCH_HALVINGS` are
     unchanged.
   - `VERSION_LABEL` is 2026.9.0 and `FINGERPRINT_FORMAT` is 6.
2. **Nothing left running.** No pid under `var/one-loop-drain/01/` answers `kill -0`. `runregistrykit list` is
   unchanged: 23 runs, all finished.
3. **The fixed law.**
   - Run by the orchestrator: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` and
     `test_store_fingerprint_golden`, 50 OK in 154 s.
   - Three of `reflection-restart` 04's 42, re-run through the scanner on `b99f6a4`: `shard0015#3298`,
     `shard0003#3819` and `shard0015#4389`. Each equals `the-42.jsonl` in outcome, both digests, `n_samples`,
     `accepted_steps`, `RHS_evaluations`, `reflections` and `restart_retakes`, and 4389 fails with the same
     reason.
4. **The fix.**
   - Read against P21: the early return sits after `r_0 >= 0` and before the search, and the docstring carries
     the argument.
   - In a scratch probe, the old behaviour was taken with `_turning_point_unresolvable` patched to False, and
     compared with the new on `probe 2197` and `stored shard0000#1001` (16.5 quanta): the new returns None, the
     old raises.
   - At 2197's ln V_floor, with ln E at the least finite value (predicate False), old and new return the same
     root, bitwise. One float below it (predicate True), the new returns None and the old raises.
5. **The law.**
   - `drain_alpha_effective` reads `min(DRAIN_ALPHA_CAP, A * DRAIN_C1 / ell) * w_gate`, as ruling 3 asks.
   - `unresolved_reflection_refused` shares `_wall_and_kinetic` and `_g2_refuses` with `_reflection_guards`, and
     is called only where `unresolved_turnaround` fired.
   - Re-run by the orchestrator: 2197 at A = 1, gap zero, `gate_weight` patched
     (`law/one_loop_run.py ... probe`). It was compared with the probe's `2197-A1.json` by the orchestrator's own
     comparison: all 10 candidates' (N, α_eff, ΔK) equal, and the dark-radiation values, `accepted_steps`,
     `reflections` and the parking N equal.
6. **The tool route.**
   - `--help` shows `--drain-law` and `--gap-prescription`.
   - The pinned fixed-law tests (`test_history_and_bbn`, `test_history_and_bbn_alpha`) pass, 7 OK in 38 s.
   - One run, `1.185 4e-31 --drain-law one_loop --alpha 1 --fixed-normalization …`, prints
     `drain_law=one_loop A=1 gap=zero` and a `drain` line (10 one-loop candidates, `unresolved_declined=0`).
     r_χ today is 8.21841×10⁻³, the probe's.
7. **The records.**
   - The log follows `dk-and-rpk-migration` README §5.1's six sections, with every deviation classed and the
     user's ruling (b) quoted.
   - The verification document is new, with §1 only, and `law/out/` holds text only.
   - The numerical-strategies and architecture-summary notes are dated 2026-11-03.
   - Row 01 and the Law gate are landed. Both issues are on §4 with their measurements.
   - `.documents/OPEN_ISSUES.md` reads 30 open, last updated 2026-11-03, with 4 rows under §1.23 and 30 in all.
   - `CLAUDE.md`'s counts are the measured 26, 566 (163 + 403), 130, 156 and 68.

   The review substituted `b99f6a4` for "this commit" on the board, in the log and in the verification
   document.

Noted, not acted on: the deliberate-breakage record's B5 found that one test of `TestTheLawsInputs`
(`test_1_integrate_history_refuses_before_any_integration`) does not catch A = 0 being accepted, since its mock
model raises a ValueError of its own. The others of its class do.

### 1.1 Items

| Item | Prompt | State |
|---|---|---|
| `drain_law` on `integrate_scalar_history`, `integrate_history`, `tune_history` (P22) | 01 | ✅ |
| the regime in the loop, α_eff by regime, the constants (P17, P18, P23, P24, U9, U10) | 01 | ✅ |
| `_GAP_PRESCRIPTION`, temporary (P20, P25) | 01 | ✅ (03 removes it) |
| the `"drain"` payload key, `DrainRecord`/`DrainCandidate` | 01 | ✅ |
| the wall-search fix, `_turning_point_unresolvable` (P21) | 01 | ✅ |
| the declined unresolved reflection (the user's ruling (b), 2026-11-03) | 01 | ✅ |
| `tools/history_and_bbn.py --drain-law`, `--gap-prescription` (P25) | 01 | ✅ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Law | fixed law: golden histories bitwise, payload unchanged but the new key; one-loop law: 2197 and 2188 reproduce the probe's table, 3016 and 3233 drain nothing; 3873 explained | 01 | ✅ 2026-11-03 (verification §1.10): the goldens bitwise, the 42 equal 04's, the probe's table bitwise at A = 1 and 0.1, 3016/3233 drain nothing, 3873 explained and completing after ruling (b) |
| Measure | the sample at three A and both gap prescriptions, re-tuned; regime shares and the gap's reach across M and β; the prescriptions' spread in Y_p, D/H, ΔN_eff; the cost | 02 | ⬜ |
| Store | the round trip exact, bounce rows in order; the fixed law and α = 0 write no bounce row; fingerprint format 7 beside 6 | 03 | ⬜ |
| Export | the new export columns equal the stored record; existing `histories.csv` columns byte-identical; the drivers refuse a mismatched law | 04 | ⬜ |
| Derive | the derived measures against direct quadrature (10⁻⁶); peak-collapsed against the scoping probe's method | 05 | ⬜ |
| Bitwise | the fixed law's tip build equal to the base's in every pre-existing column | 06 | ⬜ |
| R | the one-loop build, interrupted and resumed, equals the straight build by value (bounce rows included) and 02's tool route | 06 | ⬜ |
| Figures | both tools render from 06's exports; their tests pass | 07 | ⬜ |

## 3. Active and unresolved issues

Opened by the planner on 2026-11-02, from the scoping (`source/scoping_2026-11-02.md`); two more
opened the same day, after the plan was ruled, from the drain probe (`source/drain-probe_2026-11-02.md`).
At the re-plan the same day (README §8) one of those was split in two (§4), and the rest re-pointed at
the new prompts.

- **`[00-weighted-margins-hide-the-least-relativistic-generations]`** — the stored worst margins and
  failing shares (`inelastic-reflection` 09) run over the generations holding at least 10⁻³ of
  K(N_final) only, and the generations are not stored. In the low-α transition window at laboratory M
  the others hold 80–98% of ρ_χ. Measured on two histories (serials 2188, 2197; β 1.06, 1.185, α 0.001):
  tracker margin over all generations 7.3 and 1.6 against a stored 68.3 and 69.0; reflection margin
  −17.5 and −20.4 against 17.9 and 17.5, with 0.92 and 0.84 of ρ_χ below zero against a stored failing
  share of 0. **Impact:** the stored record understates exposure at later bounces wherever the
  below-weight share is large (up to 0.98 at 4×10⁻³¹, 0.65 at β = 20); the expansion history is not
  affected (matter-like excess ≤ 5×10⁻⁶ of ρ_χ). **Next:** closed by 02, when the all-generation values
  and the generations are stored.
  **Re-planned 2026-11-02** (README §8.3): the all-generation margins and the bounce rows are stored under
  the one-loop law only (ruling 2), by 03, which closes this; under the fixed law the record stays as it
  is, and 08 documents that.
- **`[00-later-bounces-act-on-quanta-already-present]`** — a bounce is non-adiabatic for quanta with
  p ≪ ω_t, which the wall's mass makes of most of ρ_χ in the transition-window histories (the wall's
  mass is e²²–e⁵² above the between-bounce mass). It rescatters them and stimulates production into
  occupied modes; the (k, −k) pairs are correlated, so coherent build-up is possible. The ledger treats
  the quanta as passive radiation. **Impact:** unmeasured; the drain fraction and the spectrum's peak
  at later bounces could change. The user ruled it documented and quantified, not modelled (2026-11-02).
  **Next:** quantified by 04 and 05 (δ_stim, δ_resc under the stated spectrum and random phases),
  documented by 07, and left open as a known limitation.
  **Narrowed 2026-11-02** (source `drain-probe_2026-11-02.md` §§1, 3): the premise above is wrong in
  one respect. A bounce acts on a mode by where it sits against the bounce's band, not by p/m: a mode
  with p ≫ k_ex follows adiabatically whatever its mass, so only generations with ln(Q_g/Q_k) ≲ O(1)
  are rescattered or stimulated. Parametric resonance is excluded wherever production is
  non-negligible: the phase slip per bounce is 10⁶–10³⁰ rad in every producing train probed (two
  laboratory-M histories under a one-loop drain, three at 10⁻¹² M_P at stored α and without a drain),
  against |β|² ≲ 1. The random-phase recursion n_{j+1} = n_j + |β_j|²(1 + 2n_j) is then the complete
  model; under a one-loop drain it is negligible (n ~ αe^{−4ℓ} ≪ 1). At the stored fixed α the late
  classical generations still have n ≫ 1, which is `[00-fixed-alpha-drain-overstates-late-reflections]`.
  **Re-planned 2026-11-02** (README §8.3): quantified by the derivation tool (05) under the one-loop law,
  on 02's and 06's histories: δ_stim, with rescattering bounded by it; documented by 08, and closed there
  if negligible under the one-loop law, otherwise left open as ruled. Under the fixed law it stays a
  documented limitation.
- **`[00-fixed-alpha-drain-overstates-late-reflections]`** — opened 2026-11-02 (source
  `drain-probe_2026-11-02.md`). A fixed α cannot follow the drain along the cascade or with M. At
  α = 10⁻³ at laboratory M it underestimates the early one-loop drains (one loop: 0.08–0.26) and asks
  the late classical bounces (R < 1) for occupations n ≫ 1 in their own band: in 2197, 61% of ρ_χ is born
  at R < 1 with n > 1, against |β|² ≲ O(1) from one bounce (a fraction ~R e^{4ℓ} of E; R·S(L) on the
  classical wall). Under α_eff = min(1, A·2.7/ℓ), zero at R ≤ 1, 2197 and 2188 park after 10–95 one-loop
  bounces and r_χ rises 1–10× (2197's ΔN_eff 0.0064 → 0.061 at A = 1); at 10⁻¹² M_P nothing is drained
  and ΔN_eff → 0 (3233: 0.335 → 0). **Impact:** the stored ρ_χ of the low-α laboratory-M histories is
  dominated by an unphysical late drain, and the stored large-M ΔN_eff rests on drains far above what
  a bounce can deliver; observable effects are below current bounds in the histories probed. **Next:**
  the user's decisions, README §6.3 (1–4).
  **Re-planned 2026-11-02:** this campaign's object (README §8). The one-loop law lands in 01; 03 closes
  this when the gap prescription of record (U13) is fixed and the law's keys are stored.
- **`[00-the-parking-leftover-has-no-spectrum]`** — the energy drained at the parking switch (up to
  0.10 of ρ_χ at 4×10⁻³¹, β 1.06–1.107; ≤ 0.03 elsewhere) has no peak estimate, so no margin and no
  wall-exposure measure covers it; it is reported only as `unassessed_share`. **Impact:** not measured.
  **Next:** out of this campaign's scope; recorded for a later one.
  **2026-11-02, at the re-plan:** under the one-loop law the parking leftover was 0.008–0.23% of K in the
  four laboratory-M probe runs, and at 10⁻¹² M_P it is all of ρ_χ (3016: r_χ 1.0×10⁻⁶ today). 02 measures
  its share across the sample (README §8.3); the spectrum stays out of scope.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

- **`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`** — opened 2026-11-02, when
  `[00-reflection-restart-tree-fails-probe-histories]` was split (§4; source `drain-probe_2026-11-02.md` §5).
  On `reflection-restart` at `2d50b95`, at the stored normalization, 2197 and 2188 at their stored
  α = 10⁻³ fail after about 115 s in `classical_turning_point` (ln E = −306.7: a residual not monotone,
  alternating −inf and repeated values, which looks like float cancellation in V − V_floor), though both
  completed on `0209aac`. Logs: `source/probe/drain/results/2197-base-stored-alpha.log`,
  `2188-base-stored-alpha.log`. **Impact:** not investigated; histories of the low-α transition window
  may fail on the fixed tree where they succeeded before, and `reflection-restart` 03's F3 changes the
  trajectory after every restart, so it may be that prompt's regression. Under the one-loop law both
  histories complete on the same tree. **Next:** `reflection-restart` 04.
  **Assigned (2026-11-02):** to `reflection-restart` 04, by the user's ruling 6 (README §6.3), to find
  whether its 03 caused this; that campaign's board §3.1 carries it.
  **Narrowed (2026-11-02) by `reflection-restart` 04** (`ac8ce15`; its board §3.1 holds the measurements, its
  verification document §3.2–§3.6 the evidence): verdict (ii), 03's F3 moves a correct trajectory onto a limit
  of the wall search that predates it. The search cannot find a root of V(φ_t) − V_floor = E that lies below
  17 quanta of ln V's rounding (`WALL_ROUNDING_QUANTA` = 16 treats the region as −∞), and raises "not monotone"
  or "the outer end is still −inf". The step loop meets it at an infall's first step start, near the apex,
  where E/V_floor falls below about 10⁻¹² and the field is far out in the tail (φ/M ≈ 7×10²³ at the two
  failures). The roots are at 1.51 and 1.58 quanta in 2197 and 2188, and at 0.11–16.54 in the nine
  "not monotone" failures of the 2026.8.0 store, made before `reflection-restart`. Eight of those nine still
  fail on the fixed tree.
  **Assigned (2026-11-02):** to this campaign's 01, by the user: "Add the wall-search fix to `one-loop-drain`
  01." The fixed law runs these histories, and 01 already owns the related inside-wall stop of 3873. The form of
  the fix is README §8.8, P21. **Next:** 01.
  **Closed 2026-11-03 by prompt 01** (`b99f6a4`; `.documents/one-loop-drain-verification.md` §1.3–§1.4): the
  rule of README §8.8 P21, `_turning_point_unresolvable`, makes `classical_turning_point` return None where ln E lies
  below the least finite ln(V − V_floor); only calls that raised change. All 19 measured failing inputs now return
  None with the predicate holding, and each raises with a copy of the method before the fix. 2197 and 2188 at their
  stored normalization, α = 10⁻³, complete (131 and 46 assessments answered); the nine of `reflection-restart` 04
  §0.2, tuned, all succeed; the 42 replay equal 04's; the predicate costs under 10⁻³ of the wall time.

- **`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`** — opened 2026-11-02, when
  `[00-reflection-restart-tree-fails-probe-histories]` was split (§4; source `drain-probe_2026-11-02.md` §5).
  3873 (10⁻¹² M_P, β 1.11), run on `2d50b95` at its stored normalization with nothing drained, stops at
  N = 43.76 at the check "reflection requested inside the wall" (W/(π²/2) = 1.485); at its stored
  α = 0.03 it passes. Log: `source/probe/drain/results/3873-A1.log`. The check dates from `fc97233`
  (2026-10-01, the integrator audit), not from `reflection-restart` 03, as the source record and the first
  form of this entry said. **Impact:** under the one-loop law every large-M history drains nothing, so all
  of them take the path on which 3873 stops; how many stop is not known. **Next:** 01 (README §8.4).
  **Narrowed 2026-11-03 by prompt 01** (verification §1.7): the branch is the unresolved turnaround. In the wall's
  linear tail (nothing drained, E/V₀ ≈ e^{−9.2}) W/ρ_kin = 0.5 at 3φ_t and exceeds 1 inside 2φ_t. φ_t crossed the
  10⁻⁸ M_P threshold only at φ = 1.67 φ_t, so the rule fired late and G2 refused. The same mechanism is behind 21 of
  the 2026.8.0 store's fixed-law failures (M = 10⁻¹⁵–10⁻¹² M_P, β 0.5–1.0). **Ruled by the user, 2026-11-03**, on
  option (b): "I see. I had assumed that was an energy balance that meant the field and moved beyond its reflection
  point, but I see that it's not. In which case, (b) is correct." **Closed 2026-11-03 by prompt 01** (`b99f6a4`):
  where the unresolved reflection would fail G2 (G2's own expression), that step start makes no unresolved
  decision and the floor rule is tested as usual; every such state raised before, so nothing that succeeded moves.
  3873 under the one-loop law at A = 1 now completes (19 declined step starts); 3016 and 3233 are unchanged; 3460
  and 4397, two of the 21, complete tuned under the fixed law (4 and 13 declined in the accepted integration).

- **`[00-reflection-restart-tree-fails-probe-histories]`** — opened 2026-11-02 from the drain probe;
  **closed the same day by splitting** at the user's ruling 6 (README §6.3) into
  `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, assigned to
  `reflection-restart` 04, and `[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`, for this
  campaign's 01 (§3). Its text said the inside-wall check was added by `reflection-restart` 03; it dates
  from `fc97233`.

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-01, after `inelastic-reflection` 12): CosmologyModels 26,
ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68. `c95650a` added one to
ObjectFactories, and `reflection-restart` will add more before 01 starts, so 01 measures the counts again
on its own tree before it changes anything.

**2026-11-02, at the re-plan:** on `reflection-restart` after its 03 (`b81eab8`), CosmologyModels 26,
ComputeTargets 514 (122 + 392), ObjectFactories 130, tools 147, RunDrivers 68. `reflection-restart` 04
may add more, so 01 still measures them on its own tree first.

**2026-11-03, prompt 01:** measured first at `b1415a3`: 26, 514 (122 + 392), 130, 147, 68, all passing. After 01:
26, 566 (163 + 403), 130, 156, 68, all passing (log 01 §4).
