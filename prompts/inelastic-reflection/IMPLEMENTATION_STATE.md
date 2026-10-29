# inelastic-reflection campaign — implementation state

**Last updated:** 2026-10-29 · **Status: LIVE, RE-PLANNED 2026-10-29 (README §8): 04–12 replace
the old 04–08; the campaign now reaches laboratory M through a parking switch. Before that: 01 landed in `85661b5` and reviewed (2026-10-28;
gate DR holds); 02 landed in `fe2a944` and reviewed (2026-10-28; gate Inject holds), after one
stop and the user's ruling of interior sampling; 3 of 8 prompts written: 03 written 2026-10-28
from log 02 §8, after the user ruled 𝒜₀ = 1 (README §6.1); 03 landed in `0afa199` and reviewed (2026-10-28;
gate Taper holds, as restated when the user withdrew the stopping rule mid-prompt, note 03 ruling 16).
R1–R5 ruled by the user 2026-10-27; U1–U8 ruled the same
day (U4 as the revised proposal (b′): wall-side turning points only) and P1–P16 stand. The prompts
are written one at a time, each after the one before has landed and been reviewed (README §2.1).**
The plan is [`README.md`](README.md), written on 2026-10-27 on branch `inelastic-reflection`, cut
from `main` at `6f635e6` with the planning commit (U1). Its physics basis is
[`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md).

At each reflection, the campaign drains a fraction α of the condensate's kinetic energy into
chameleon quanta. It carries them as dark radiation, ρ_χ,E = K e^{−4N} with K piecewise constant,
and certifies per history that they stay relativistic, at the bounces and on the late tracker to
today. It makes α a key of every history and stores ΔN_eff and ρ_χ/ρ_R. At α = 0 every value is
bitwise the old code's.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/ScalarHistory.py`, `ComputeTargets/Policies/PotentialDerivativePolicy.py`,
`ComputeTargets/AdiabaticHistory.py`, `ObjectFactories/ScalarHistory.py`, `config/`, `main.py`,
`plot_inputs.py`, the plot scripts, `RunDrivers/registered_run.py`, `tools/history_and_bbn.py` ·
**Verification:** `.documents/inelastic-reflection-verification.md` (created by 07) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.19

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Dark-radiation component](01-dark-radiation-component.md) | the ledger; ρ_χ in V/3H²M_P², Ḣ/H², E, `HubblePolicy`, V_target; K = 0 bitwise | Opus | ✍️ 2026-10-27 | ✅ 2026-10-28 | `85661b5` | [01](logs/01-dark-radiation-component.md) |
| 02 | [Injection at turning points](02-injection-at-turning-points.md) | turning points in the step loop; the drain at the KE maximum; the restart; the floor-rule drain; the record; α in `integrate_history`/`tune_history` | Opus | ✍️ 2026-10-28 | ✅ 2026-10-28 | `fe2a944` | [02](logs/02-injection-at-turning-points.md) |
| 03 | [Gate and stopping rule](03-gate-and-stopping-rule.md) | 𝒜_k measured, then the gate at wall-side turning points (U4 (b′), 𝒜₀ = 1); the plateau diagnostic; ~~the tapered stopping rule (U5, P17)~~ withdrawn by the user (ruling 16); the residual's continuity | Opus | ✍️ 2026-10-28 | ✅ 2026-10-28 | `0afa199` | [03](logs/03-gate-and-stopping-rule.md) |
| 04 | The wall through the model | φ_t, V″, 𝓡, the one-loop turnaround, the full-trace minimum and dφ_min/dN as model methods with generic defaults (R9) | Opus | ⬜ | ⬜ | — | — |
| 05 | Unresolved turnarounds | φ_t < 10⁻⁸ M_P or 𝓡 > 1 → reflect at 3φ_t (R8); w_gate = 1 there and at the floor rule (R10, P20); the records; the warnings (P23) | Opus | ⬜ | ⬜ | — | — |
| 06 | Parking | the cut ρ_kin/ρ_R < ε (R14, P19, P21); the parked phase to T_CMB through the dense-output interface (R15); the tuning at small M measured | Opus | ⬜ | ⬜ | — | — |
| 07 | Unparking | the release at m/H < 10 (R16, P24) | Opus | ⬜ | ⬜ | — | — |
| 08 | Spectrum peak and margins | the peak by 𝓡 (R7, P25); ℓ; the energy weight (U9); three worst margins and the first failure (R13) | Opus | ⬜ | ⬜ | — | — |
| 09 | Store | α as a key (U3); the per-sample ratio; ΔN_eff at T_CMB and 1 MeV, the margins, parking and release as columns; JSON details; `AdiabaticHistory`; format 6, 2026.8.0 | Opus | ⬜ | ⬜ | — | — |
| 10 | Driver and readers | `--alpha-values`; plot matching; Δg* and ΔN_eff figures (R12); scope; inventory; the tool | Sonnet | ⬜ | ⬜ | — | — |
| 11 | Measure | the grid of U10; the rehearsal (U11); the verification document | Opus | ⬜ | ⬜ | — | — |
| 12 | Close | documents, `CLAUDE.md`, handover §4.18, indexes, board | Sonnet | ⬜ | ⬜ | — | — |

*The rows for the old 04–08 (relativistic margin, store, driver and readers, measure, close), none of
them written, were replaced by the re-plan of 2026-10-29 (README §8).*

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–08 were held until the user ruled on README §6.1 and §6.2,
which they did on 2026-10-27. At the user's word of the same day ("If subsequent prompts build on
earlier ones it may be preferable not write them all at once"), each of 02–08 is written after
the one before has landed and been reviewed, from what its log hands on.

Orchestrator notes, one prompt at a time, in [`orchestrator/`](orchestrator/):
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`00c17f4`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`77af9c8`; §0a, rulings 15 and 8a, added at the review), used for 02.
- [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) (`1d3b0c6`; §0a, the user's ruling 16, added `c3fcfdf`), used for 03.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| DR | at K = 0 both golden histories bitwise; with an initial ρ_χ constraint and continuity agree, the radiation limit holds, a tuned history meets both conditions | 01 | ✅ 2026-10-28 (log 01 §4) |
| Inject | at α = 0 both golden histories bitwise; at α > 0 H_E continuous across every injection, ledger = Σ drains; a floor-rule drain exact | 02 | ✅ 2026-10-28 (log 02 §1.2, §4, §6) |
| Taper | weights equal hand values; the tuning residual continuous across a threshold; the omitted fraction within its bound. *Since ruling 16 (2026-10-28, the stopping rule withdrawn): w_gate equals its hand values, and the residual is continuous across a threshold* | 03 | ✅ 2026-10-28, as restated (log 03 §4, §5.3) |
| Wall | φ_t and 𝓡 equal the exponential closed form; the notes' table (classical wall y ≈ 105, one-loop stopping point y ≈ 40 at 1 meV) reproduced; an n = 2 model needs no closed form; φ_min and dφ_min/dN checked | 04 | ⬜ |
| Unresolved | α = 0 bitwise; an unresolved reflection is made at 3φ_t with w_gate = 1; the reflected history equals the resolved one in the probe's band; the warnings | 05 | ⬜ |
| Park | α = 0 bitwise; an M = 10⁻³⁰ drained history reaches T_CMB; parked samples on φ_min; H_E continuous at the switch; the leftover in the ledger; the guard | 06 | ⬜ |
| Release | a parked field released at m/H < 10 continues; continuity at the release; the lag after it as estimated | 07 | ⬜ |
| Margin | the three margins equal a hand computation; energy-weighted; the classical birth margin negative by construction; NULL at α = 0 | 08 | ⬜ |
| Store | key and columns round-trip; two α are distinct rows; the new golden fingerprint; `AdiabaticHistory` bitwise at α = 0 and computed on parked samples | 09 | ⬜ |
| Driver | α reaches every stage; an unstored α exits 2 in the plots; scope and inventory name α | 10 | ⬜ |
| Measure | every point has a row; the drain independent of M below 10⁻¹² to 10⁻³; ΔN_eff rises with α; margins reported; α = 0 reproduces 2026.7.0 | 11 | ⬜ |
| R (U11) | the rehearsal's stores equal after interruption and resume | 11 | ⬜ |

*The gates Margin, Store, Driver, Sensitivity and R of the old 04–07 were replaced by the re-plan of
2026-10-29 (README §8.4).*

## 3. Active and unresolved issues

One opened by the planner.

### `[00-xav-sigma-is-non-monotone-at-25-35-keV]`

**Opened:** 2026-10-27 by the planner, found by the probe `tracking_mass.py`
(`source/planning-probes-2026-10-27/`).

- **What.** Σ = 1 − 3w from `Xav_EOS_spline.w` is non-monotone between 25 and 35 keV. Between 25.4
  and 33.0 keV it takes the values 3.2×10⁻⁶ to 8.8×10⁻⁵, rising and falling (`tracking_mass.out`,
  last block). There are two causes:
  - **The table is irregular.** The nodes of `CosmologyModels/GenericEOS/Xav_EOS_data.csv` at 35.5,
    31.6, 28.2 and 25.1 keV give Σ = 2.0×10⁻⁴, 2.3×10⁻⁵, 2.0×10⁻⁵ and 6.3×10⁻⁷. A Boltzmann tail
    would fall by a similar factor at each step.
  - **The cubic spline rings between the nodes** (`make_interp_spline`, `Xav_EOS_spline.py:75`).
- **Impact.** Not measured. Σ enters the kick, −3M_P² E (d ln A/dφ) R, and d ln ρ_R,E/dN for every
  history. The matter trace there is ρ_m/ρ_R ≈ 3×10⁻⁵, so the spurious part of Σ is comparable to
  it, inside the BBN window. In the probe at the tracking minimum it gives a ×1.19 rise of m_eff·a
  over 30.2–28.0 keV. The probe ignores the frames and follows no trajectory.
- **Next step.** For the user. Measure the effect on a history's φ, H_J and BBN between 50 and
  20 keV against a smooth Σ (a monotone fit to the table, or the e± Boltzmann tail plus the
  matter-free plasma). Then decide whether the table is regenerated or the spline replaced. Fixing
  it changes every history and needs its own ruling, so it is not in this campaign (README P15). Not
  assigned. `Xav_EOS_data.csv` has no generating script (`[00-kicking-function-table-has-no-provenance-in-the-repository]`, `review-remediation` board).

### 3.1 Assigned to this campaign

- **`[00-settling-at-physical-M-needs-a-parked-tracking-model]`** (`integrator-remediation` board
  §3), assigned 2026-10-29 by the re-plan (README §8.6): prompts 06 and 07 build the parked tracker,
  with the switch criterion supplied by the drain. The entry stays on its own board until it is
  closed.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-27)

Planned at the user's request, after the scoping of the same day
([`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md)). Nothing in the code was changed or
run beyond the three planning probes, which read the EOS only and started no Ray.
- **Dark radiation, not f(q)** (R2). The quanta stay relativistic by at least 10⁴ in p/m to today,
  and the distribution function would need its own adiabaticity check (scoping §3–§4).
- **The margin is checked per history** (R3), at the bounces and on the late tracker with today's
  mass (R4).
- **The regime.** At M = 0.5 M_P α is physically about zero (4.5×10⁻³⁷), so α there is a
  sensitivity parameter. The physical regime needs the parked-tracking model (U2).
- **The stopping rule.** The user proposed one; the planner's refinement is U5. Neither is ruled.
- **No run registered**: `runregistrykit list` showed 11 finished runs and nothing running.
- **Suites at `6f635e6`** (from `tuning-followups` 02's review, not re-run): 26 / 269 (77 + 192) /
  103 / 115 / 64.

### 5.1 After prompt 01 (2026-10-28)

01 landed in `85661b5`, under orchestrator note 01 (eleven rulings), with no stop. Log:
[`logs/01-dark-radiation-component.md`](logs/01-dark-radiation-component.md).
- **What shipped.** `ComputeTargets/dark_radiation.py` (`DarkRadiationLedger`: K(N) = K₀ +
  Σ_{N_j ≤ N} ΔK_j, K₀ present at every N); r_χ = ρ_χ,E/ρ_R,E in V/3H²M_P², V′/3H²M_P², Ḣ/H² + 3
  (−Y·R/2), E (taken out of the kick) and `HubblePolicy`'s V = 0 branch, through `ODEPolicy`'s
  `dark_radiation` and `ODEPolicyData.rho_chi_over_rho_rad`; V_target subtracts ρ_χ,J; the test
  harness `rho_chi_ratio_init` on `integrate_history` and `tune_history`; the payload key
  `"dark_radiation"`. `compute_scalar_history` is unchanged; nothing injects and nothing is stored.
- **The derivations** of every §1.2 expression are in the log and agree with the planner's. E uses
  (G − V/3H²M_P²)(1 + f_m)/(1 + f_m + r_χ), which adds no cancellation.
- **Gate DR holds.** Both golden histories bitwise at K = 0; the constraint to rounding at 40
  constructed states and every sample of a real history; d ln H_E/dN on the dense output equal to
  the policy's Ḣ/H² to 2e-6 (Y·R/2 peaks at 9.5e-3 at T_J = 0.19 GeV for r₀ = 0.1); Ḣ/H² = −2
  exactly in the radiation limit; a tuning with r₀ = 1 converges in five integrations.
- **Breakages** (P11): five, each failing a named test; leaving ρ_χ out of V_target also fails the
  tuned test after 30 integrations (H_J/H₀ stalls at 1 + 2.6e-5).
- **Cost at K = 0:** golden minima 10.9 s → 10.9 s, tuned 23.3 s → 24.1 s, inside the spread of the
  tree before.
- **Counts:** 26 / 269 (77 + 192) / 103 / 115 / 64 before; 26 / **288** (96 + 192) / 103 / 115 / 64
  after, all passing. `CLAUDE.md`'s counts are 08's to update.
- **Issues:** none opened, none closed.
- **For 02** (log 01 §7): the ledger's API; the policy holds the ledger, so an `add` while Radau runs
  is seen with no new policy; the N_j ≤ N edge at an interpolant ending at N_j is 02's to settle.

*Orchestrator review of prompt 01 (2026-10-28).* The note's ten checks, at `85661b5`, run
serially at a load of 4–10:
1. **Scope.** The commit's 8 files are the prompt's §5 list exactly; its parent is `00c17f4`; the
   tree is clean, `git worktree list` shows the main checkout alone, and nothing is pushed. No change
   to `AdiabaticHistory.py`, `ObjectFactories/`, `config/`, `main.py`, `tools/`, `RunDrivers/`,
   the golden generators or `ComputeTargets/tests/data`. `compute_scalar_history`,
   `build_extra_data` and `ScalarHistory.store` are byte-identical to `00c17f4`'s.
2. **The golden histories.** `test_golden_history` passes alone (3 tests, 9.8 s), and so does the
   tuned golden test (22.8 s). Both JSONs are byte-identical to `00c17f4`'s.
3. **The expressions.** Every changed line of `PotentialDerivativePolicy.py`, `ODEPolicy`,
   `HubblePolicy` and `_tuning_state` was read against note 01 §1's derivation, and agrees. At
   `r_chi == 0.0` each of T's two branches, E, Ḣ/H² + 3, the V = 0 branch and V_target performs
   `00c17f4`'s operations in the same order. No path adds 0.0. r_χ is threaded into
   V′/3H²M_P² as well (D3), which the prompt omitted and the reflecting term needs.
4. **The ledger.**
   - K₀ is present at every N (ruling 1), and `add` is ordered against the injections.
   - `is_zero` uses no exp or log, and the payload carries the ledger on every success (ruling 3).
   - It pickles, which a test shows.
   - `ODEPolicy` forms r_χ from the trial state's ln ρ_R,E, so Radau's Jacobian sees the dependence.
5. **The breakages.** B1–B5, replayed from the agent's diffs (whose changed lines are the log
   §5's) in `bash`, each fail the tests the log names and no others:
   - B1: 3 tests;
   - B2: 2;
   - B3: 2;
   - B4: 2, the tuned test after 118 s;
   - B5: 3.

   Each was reverted, and the tree was checked restored after each.
6. **The numerics of the tests.** The V = 0 states sit at ln(ρ_R/V) = 746 and assert U = 0.0
   (ruling 2). Each bound is stated and argued (ruling 8). The E test also asserts that its bound is
   below 10⁻³ of the breakage's signal. The H check asserts that Y·R/2 exceeds 10³ times its
   tolerance. A probe of my own (session scratchpad, not committed) used a different model
   (M = 0.5 M_P, β = 2, φ = 0.7 M_P, π = −0.3 M_P, T_J = 150 MeV, ln(ρ_R/V) = −1 and 4) at r_χ = 1.
   Against the definitions, its errors were:
   - H² to 8.5e-15;
   - Ḣ/H² to 2.2e-16;
   - E·3H²M_P² = ρ_R(1 + f_m) to 2.2e-16.

   Y·R/2 was 2.9e-2 and 6.7e-2 there.
7. **The tuning with ρ_χ.** It converges at r₀ = 1 in five integrations (the test, in the suite and
   alone). The V_target breakage stalls at H_J/H₀ − 1 ≈ 2.6e-5. That is note 01 §0's estimate of
   4.5e-5 r_χ(today) ≈ 1.75e-5 times b′²/G = 1.49 at the tuned point, a factor the estimate left
   out; the log records it.
8. **Nothing reaches the store.** `store` and `build_extra_data` read named keys only. There is no
   new column, key or fingerprint, and ObjectFactories stays at 103.
9. **The records.** The log is in §5.1's form, with the derivations, note 01's rulings R1–R11 and
   the agent's D1–D10 each classed, and no UNINTENDED DRIFT. The two `.documents/` notes are dated
   and additive (`architecture-summary.md` §7.2, `numerical-strategies.md` §6.1). The board
   agrees with the log. No issue was opened, so `.documents/OPEN_ISSUES.md` is untouched, as it
   should be.
10. **The suites** at `85661b5`, serially:
    - CosmologyModels 26 (64 s);
    - ComputeTargets 96 + 192 (120 s + 241 s);
    - ObjectFactories 103 (18 s);
    - tools 115 (115 s);
    - RunDrivers 64 (25 s).

    All pass. ComputeTargets rose by the 19 new tests only. `black --check` is clean on the four
    changed `.py` files.

Residue fixed in this review's commit:
- "this commit" → `85661b5` in the log and on this board;
- one docstring of `test_dark_radiation.py` (`test_e_V_target_puts_H_J_at_H0`) said ρ_R,E is
  exp(285); it is exp(−285).

`CLAUDE.md`'s ComputeTargets counts (269; 77 in the [a-f] half) are stale. They are 08's to update
(README §2.2); 02's prompt, when written, takes its baselines as 26 / 288 (96 + 192) / 103 / 115 / 64.

### 5.2 After prompt 02 (2026-10-28)

02 landed in `fe2a944`, under orchestrator note 02 (rulings 1–14). It stopped once; ruling 15 (the
user's) and ruling 8a (the coordinator's) followed. Log:
[`logs/02-injection-at-turning-points.md`](logs/02-injection-at-turning-points.md).
- **The stop.** The §2 measurement found a step at β = 5, α = 0.1 (step 6051, T_J ≈ 0.7 MeV) on
  which π dips from +1.9e-6 to −1.0e-6 and back inside one step. That is a plateau and a wall-side
  turning point that the end-point test missed: prompt §5's third stop. The user chose interior
  sampling (ruling 15). The coordinator accepted my derived H bound in place of k·ε·(1 + c)
  (ruling 8a).
- **What shipped** (`ComputeTargets/ScalarHistory.py`):
  - `TurningPoint` and `Injection` records. `IntegrationResult.turning_points` and `.injections`
    are appended last, and the payload carries the same two keys.
  - The detection: on every appended step, π at the two ends (scalar calls, `first_bounce`'s) and
    at 8 interior points. One end-to-end change is bracketed as `first_bounce` brackets it; two or
    more are refined on their sub-intervals.
  - The drain at the first step end past |π|'s maximum after each wall-side point, with the ledger
    added to before the restart. The floor rule drains in its own restart, with
    π_out = −√(1 − α)π_in.
  - The dense output is right-continuous (`alt_segment=True`).
  - `alpha` (default 0.0) on `integrate_scalar_history`, `integrate_history` and `tune_history`,
    checked at entry. `compute_scalar_history`, `store`, `build_extra_data` and `first_bounce` are
    unchanged.
- **Gate Inject holds.**
  - Both golden histories are bitwise at α = 0.
  - H_E is equal bitwise across 194 of 195 drains, and the other is one ulp of the exponent apart
    (1.414e-14 at β = 25, against a derived bound of 97 ε).
  - The ledger equals the drains exactly, and the floor drain is exact.
- **Breakages:** B1–B9, each failing a named test. B8 (a restart at α = 0 with y unchanged) changes
  both golden histories and the tuned one, so a restart is not free.
- **Measured** at M = 0.5 M_P, untuned, to T_CMB:
  - at α = 0 the counts equal prompt §0's table exactly;
  - at α = 0.1 there are 7, 27, 86 and 75 drains for β = 1.2, 2, 5 and 25, none dropped, with
    Σ Δρ/ρ_R = 1.5e-2, 1.2e-2, 2.0e-3 and 4.4e-4;
  - the tuning at β = 2, α = 0.1 converges in 11 integrations.
- **Cost at α = 0:** golden 9.73 → 10.76 s (+11%, measurable), tuned 23.2 → 25.3 s (inside HEAD's
  spread at a load of 5–15). The detection adds 451 `brentq` calls to the golden histories.
- **Counts:** 26 / 288 (96 + 192) / 103 / 115 / 64 before; 26 / **305** (96 + 209) / 103 / 115 / 64
  after, all passing.
- **Issues:** none opened, none closed. The loop's first wall point can differ from `first_bounce`
  if an interior pair ever precedes the first end-to-end wall point. That is recorded in log 02 §7
  and §8, not opened.
- **For 03** (log 02 §8): α_eff is set in `alpha_effective()`, the one place for w_gate·w_rate.
  Dropped drains are derived, and need a count once a weight can be zero. The near-stall pair at
  β = 5 is the first measured small oscillation.

*Orchestrator review of prompt 02 (2026-10-28).* The note's ten checks, at `fe2a944`, run serially.
The load was 3–108 (other workloads share the machine):
1. **Scope.**
   - The commit's 6 files are the prompt's §6 list exactly, and its parent is `77af9c8`.
   - The tree is clean, `git worktree list` shows the main checkout alone, and nothing is pushed.
   - There is no change to `AdiabaticHistory.py`, `Policies/`, `dark_radiation.py`, `Quadrature/`,
     `ObjectFactories/`, `config/`, `main.py`, `tools/`, `RunDrivers/`, `plot_inputs.py`, the golden
     generators or `ComputeTargets/tests/data`.
   - `compute_scalar_history`, `build_extra_data` and `ScalarHistory.store` are byte-identical to
     `77af9c8`'s.
2. **The golden histories.** Two runs, all passing:
   - `test_golden_history`, `test_first_bounce`, `test_surfing_turning_points` and
     `test_kinematic_cap_loop`, run together: 34 tests in 14.7 s, all unedited;
   - the tuned golden test (`test_tuning.py` (e)) with `test_injection`: 18 tests in 31.3 s.

   The golden JSONs are byte-identical to `77af9c8`'s.
3. **The loop.** Every changed line of `integrate_scalar_history` was read against rulings 1–6 and 15.
   - **Classification.** The end values are scalar calls at `ts[k]` and `ts[k + 1]`. An end-to-end
     change uses `first_bounce`'s whole-step `brentq`.
   - **The drain.** The drain-point test reads `solver.y[1]` and `solver.f[1]`.
   - **The order.** Append, classify, the `finished` exit, drain, then restart. `ledger.add` comes
     before `new_solver`, and the last step never drains.
   - **At α = 0** there is no ledger call, no restart and no change to y. The floor branch performs
     `77af9c8`'s two lines.
4. **The drain.** ΔK and Δρ/ρ_R are formed in logs (D3), and ρ_kin uses H_E with the ledger before
   the drain. A probe of my own (session scratchpad, not committed) at M = 0.5 M_P, α = 0.1, for
   β = 2 and 5 recomputed each drain from the definitions, with H from
   H² = (V + ρ_R(1 + f_m) + K e^{−4N})/(3M_P²G):
   - H_after/H_before − 1 ≤ 2.2e-16;
   - the policy's H against the definitions to 7.6e-15;
   - ΔK against α·½H²π²e^{4N} to 3.7e-14;
   - `ledger.injections` equal to the drains as the same floats, and K(N_final) = Σ ΔK exactly;
   - `solution(N_j)[1]` equal to `pi_after` bitwise.
5. **The breakages.** B1–B9 were replayed in `bash` from the agent's diffs, with `git apply --check`
   and `-R --check`, the source's md5 checked restored after each. Each fails the tests the log
   names:
   - B1: 4 tests;
   - B2: 3;
   - B3: 3;
   - B4: 2;
   - B5: 1;
   - B6: 2;
   - B7: 3;
   - B8: the two golden histories and the tuned one;
   - B9: both item-11 tests.

   B6's diff isolates the order (D8): it re-adds the drain after `new_solver`, and only item 7
   fails for the reason intended.
6. **The numerics of the tests.**
   - **Ruling 8a.** `H_bound` is derived term by term, and its exponent term ε|x| covers the β = 25
     drain at 64 ε. One ulp of x ≈ −180 is 128 ε on H², which is 64 ε on H. The test reports its
     worst ratio (0 at c = 0.043, bound 99 ε, on β = 2).
   - **Ruling 6.** Item 6 asserts the drained state bitwise.
   - **Item 7.** It fails under B6.
7. **The measurement.** My probe re-ran β = 2 and β = 5 at α = 0.1, and both agree with the log:
   - β = 2: 3 836 steps, 27/26 turning points, 27 injections;
   - β = 5: 7 494 steps, 86/85, 86 injections, with the kinds alternating.

   The α = 0 counts in the log equal prompt §0's table, and no step is left with more sign changes
   than the loop recorded. The tuning at β = 2, α = 0.1 is reported as measured: it converges in
   11 integrations.
8. **Nothing reaches the store.** `store`, `build_extra_data`, `golden_history.record_of` and
   `TUNING_OUTPUT_KEYS` are unchanged; the new payload keys are read by nothing on the way to the
   store. The ObjectFactories, tools and RunDrivers counts are unchanged.
9. **The records.**
   - The log is in §5.1's form, with the H_E argument (§1.1) and the bounds (§1.2).
   - It classes note 02's rulings R1–R15 with 8a, and the agent's D1–D8, with no UNINTENDED DRIFT.
   - The two `.documents/` notes are dated and additive only (no removed line):
     a dated note in `architecture-summary.md` §7.2, and a new `numerical-strategies.md` §6.2.
   - Nothing was opened, so `.documents/OPEN_ISSUES.md` is untouched.
10. **The suites** at `fe2a944`, serially, all passing:
    - CosmologyModels 26 (66 s);
    - ComputeTargets 96 + 209 (127 s + 267 s);
    - ObjectFactories 103 (16 s);
    - tools 115 (161 s);
    - RunDrivers 64 (26 s).

    ComputeTargets rose by the 17 new tests only. `black --check` is clean on the two changed `.py`
    files.

Residue fixed in this review's commit:
- "this commit" → `fe2a944` in the log and on this board;
- note 02 gains §0a, which records rulings 15 (the user's) and 8a (mine) as they were sent to the
  agent after the stop.

One observation of the review's, not opened. It is the neighbour of note 02 ruling 7 and needs an
exact equality:
- **The case.** The termination branch's `g_low == 0.0` case (ln T_J exactly at log_T_stop at a step
  end) ends the history at the previous node without appending the last step. If a drain had been
  made at that node, ts[-1] would be the drain point.
- **What goes wrong.** `solution(N_final)`, and so the sample at z = 0, would read the pre-drain
  interpolant against the post-drain K, while `final_state` is the drained state.

The detection's cost at α = 0 (+11% on the golden history, about 20 µs per accepted step) applies to
every history `main.py` builds. It is recorded in log 02 §7, and is by the prompt's design.
`CLAUDE.md`'s counts (269; 77 + 192) are stale. They are 08's to update; 03's baselines are
26 / 305 (96 + 209) / 103 / 115 / 64.

### 5.3 After prompt 03 (2026-10-28)

03 landed in `0afa199`, under orchestrator note 03 (rulings 1–15) and ruling 16, the user's,
which arrived while the agent worked. Log:
[`logs/03-gate-and-stopping-rule.md`](logs/03-gate-and-stopping-rule.md).
- **Ruling 16: the stopping rule withdrawn.** The user, after asking how w_rate prevents a restart:
  "I'd drop `w_rate`". w_rate is never zero, so it never saved a restart, its one stated benefit,
  and only withheld up to (π/2)ε per e-fold of energy. So α_eff = α · w_gate; ε, the window, S, the
  omitted part and P17 are not in the code; prompt items 4–6 and breakages B2, B3, B4 and B7 were
  dropped. The agent had written the stopping rule and removed it. The gate "Taper" now holds when
  w_gate equals its hand values and the tuning residual is continuous across a threshold.
- **What shipped** (`ComputeTargets/ScalarHistory.py`):
  - `log_m_eff_squared` (ln m_eff², m_eff² = V″ + [(ln A)″ + (ln A)′²] ρ_R,E(Σ + f_m); 04 reuses it),
    `step_nonadiabaticity` and `gate_weight`, module-level; `NONADIABATICITY_SCALE = 1.0`.
  - At α > 0 only: ln m_eff² and H_E at the ten samples of every appended step (10 policy calls, no
    RHS), 𝒜 = H_E|d ln m_eff²/dN|/2m_eff, its maximum over each half-oscillation (N_{k−1}, N_k];
    every drain point and floor reflection a candidate at α_eff = α · w_gate(𝒜_k).
  - `TurningPoint` gains `nonadiabaticity` and `m_eff_over_H`; `Injection` (one per candidate)
    `nonadiabaticity` and `w_gate`; `IntegrationResult` `dropped_drains`, and the payload
    `"dropped_drains"`. Nothing is stored.
  - At α = 0 nothing of it runs; the golden histories are bitwise and their time did not move.
- **Gate Taper holds, as restated.**
  - w_gate is within 2 ε of exact rationals at 𝒜 = 0, 10⁻³, 1, 10, 10³, 10²⁰⁰.
  - The residual scans at α = 0.1 deviate from a cubic by 0.039 (β = 2) and 1.2e-3 (β = 5) of the
    tolerance, against 0.052 and 1.8e-3 at α = 0. The ±10⁻³ scans crossed no gated count change. The
    agent searched: none at β = 2 in 13 integrations (to ±3% in residual_H); at β = 5 one, 173 → 172
    wall-side points, and the scan across it deviates by 2.0e-3 with no resolved jump.
- **𝒜_k against an independent reference** (33 points per step, its own mass expression):
  |Δw_gate| ≤ 1.5e-3 at every turning point of β = 2, α = 0.1 (tolerance 0.05); 𝒜_k within 3%.
- **Measured** at M = 0.5 M_P, α = 0.1: 𝒜_k at wall-side points falls from 10²–10³ to 10⁻³–10⁻⁵;
  candidates 7, 28, 155 and 296 for β = 1.2, 2, 5, 25, all drained, none dropped; Σ Δρ/ρ_R and r_χ
  within 5% of 02's (the early bounces carry the energy); the floor harness's w_gate is 0.80.
- **The near-stall pair** at β = 5 is gone under the gate. Item 11 of `test_injection.py` was
  rebuilt on β = 10, α = 0.1 (step 9333, T_J ≈ 36 MeV), found in the 7th integration of a search;
  B9 still fails it.
- **Tuning** at α = 0.1 converges: β = 2 in 11 integrations, β = 5 in 20 (three guards).
- **Cost at α = 0.1**: about 180 µs per accepted step, 1.37–1.89 × the dispatch commit's time; it
  does not double, so H_E is computed at every sample.
- **Breakages:** B1, B5, B6, B8, B9 and B10, each failing a named test. B10 failed item 3 only after
  the agent added an m_eff/H_E check to it (D3).
- **Counts:** 26 / 305 (96 + 209) / 103 / 115 / 64 before; 26 / **317** (96 + 221) / 103 / 115 / 64
  after, all passing.
- **Issues:** none opened, none closed.

*Orchestrator review of prompt 03 (2026-10-28).* The note's ten checks, as ruling 16 trims them, at
`0afa199`, run serially. The load was 12–114 (other workloads share the machine):
1. **Scope.**
   - The commit's 7 files are the prompt's §6 list exactly, and its parent is `c3fcfdf`, the
     orchestrator's record of ruling 16.
   - The tree is clean, `git worktree list` shows the main checkout alone, and nothing is pushed.
   - There is no change to `AdiabaticHistory.py`, `Policies/`, `dark_radiation.py`, `Quadrature/`,
     `ObjectFactories/`, `config/`, `main.py`, `tools/`, `RunDrivers/`, `plot_inputs.py`, the golden
     generators or `ComputeTargets/tests/data`.
   - `compute_scalar_history`, `build_extra_data`, `store`, `first_bounce`,
     `surfing_turning_points`, `_tuning_state`, `fixed_T_value_at` and `TUNING_OUTPUT_KEYS` are
     byte-identical to `c3fcfdf`'s (compared by AST source segment).
2. **α = 0.** The golden tests, the tuned golden test, `test_first_bounce`,
   `test_surfing_turning_points` and `test_kinematic_cap_loop` are unedited and pass (check 10), and
   the golden JSONs are unchanged. I read the α = 0 path of every changed line:
   - the step-end reads are the same scalar calls, kept whole;
   - the mass, H_E and 𝒜 are computed only under `alpha > 0.0`;
   - the drain-point and floor branches call `candidate` only under `alpha > 0.0`, and the floor's
     elastic restart is `c3fcfdf`'s two lines;
   - `dropped_drains` is a counter and touches no state.
3. **The loop**, against rulings 3–7 and 16:
   - the samples are 02's ten reads, folded on the fast path too, and go through `policy` and
     `hubble` only, before any drain at the step's end;
   - attribution is half-open, (`boundary_N`, N_k], closed at each turning point and floor
     reflection;
   - a candidate's order is H_E and ln ρ_kin, then w_gate, then α_eff = α · w_gate, then 02's
     drain, or the α_eff == 0.0 record with no ledger entry and no restart;
   - the three functions are called by module name, so a patch intercepts them.
4. **The mass and 𝒜.**
   - `log_m_eff_squared` is the form of §1 item 2, summed in logs with signs, and the hand value on a
     stand-in with (ln A)″ = 3 is m² = 8 = 1 + 7.
   - A probe of my own (session scratchpad, not committed) at β = 5, α = 0.1, M = 0.5 M_P:
     - every record's w_gate, recomputed by its own expression, agrees to 2.5e-16 relative;
     - α_eff == 0.1 · w_gate exactly;
     - π_after == √(1 − α_eff) π_before;
     - the ledger equals the records with α_eff > 0;
     - `dropped_drains` equals the walls no record names.
   - The 33-point reference at β = 5, a second history beside item 3's β = 2, gives |Δw_gate| ≤
     1.6e-3 (wall) and 1.3e-3 (plateau), and 𝒜_k within 1.8%.
5. **The breakages.** B1, B5, B6, B8, B9 and B10 were replayed in `bash` from the log's diffs, with
   `git apply --check` and `-R --check`, the source's md5 checked restored at the end. Each fails
   the tests the log names:
   - B1: the hand values, item 9, item 11;
   - B5: item 3, item 9, item 11;
   - B6: item 3, item 9, item 11;
   - B8: item 1 only, with both golden histories and the tuned one passing;
   - B9: both item-11 tests;
   - B10: item 3, the hand value and the generic state.

   B10 fails item 3 through D3's m_eff/H_E check. That check is needed: w_gate saturates where the
   trace term dominates, so the gate alone does not see B10 (log 03 §3, D3).
6. **The numerics of the tests.**
   - Ruling 8's edges, 𝒜 = 10²⁰⁰ and 5e-324, are asserted exactly.
   - The 2 ε bound on w_gate is derived for both branches.
   - Item 7's Δρ/ρ_R bound is 02's `delta_K_bound` argument.
   - Item 3 measured 1.50e-3 (wall) and 1.31e-3 (plateau) against 0.05.
   - Ruling 10's conditional restrictions were not reached (smallest α_eff on β = 2 is 2.3e-3).
7. **The measurement.**
   - My probe re-ran β = 5 at α = 0.1 and agrees with log 03 §5.1 in every column it computes:
     155/154 turning points, 155 candidates, 0 dropped, 9 270 steps, 91 558 RHS evaluations,
     𝒜_k 1.14e-3/0.149/842, w_gate 49/91/15, m/H at plateau points 0.637/112/225 with 2 below 1,
     and Σ Δρ/ρ_R = 1.966e-3.
   - `17>loop` is 0 on every history.
   - The tunings at β = 2 and 5 converge (11 and 20 integrations).
   - Ruling 11: every scan, the two across the β = 5 count change included, deviates from its cubic
     by under 0.1 of the tolerance, and α = 0.1 by no more than the α = 0 baseline, so the clause
     holds without the review's judgement that ruling 11 reserved.
   - The scans do not discriminate the gate from 02's rule: under the comparison patch (α_eff = α)
     the count changes crossed show no jump either, because the candidates that appear there are
     near-stall ones whose ρ_kin is small. The continuity is measured, not attributed to the gate.
8. **Nothing reaches the store.** `store`, `build_extra_data`, `golden_history.record_of` (no change
   to the file) and `TUNING_OUTPUT_KEYS` are unchanged, and `"dropped_drains"` is read by nothing on
   the way to the store. The ObjectFactories, tools and RunDrivers counts are unchanged.
9. **The records.**
   - The log is in §5.1's form, with m_eff²'s derivation and how it differs from `M2eff_over_H2`
     (§1.1).
   - It classes note 03's rulings R1–R16, R16 quoting the user, and the agent's D1–D8, with no
     UNINTENDED DRIFT.
   - The two `.documents/` notes are dated and additive only (no removed line).
   - Nothing was opened, so `.documents/OPEN_ISSUES.md` is untouched.
10. **The suites** at `0afa199`, serially, all passing:
    - CosmologyModels 26 (84 s);
    - ComputeTargets 96 + 221 (151 s + 418 s);
    - ObjectFactories 103 (16 s);
    - tools 115 (131 s);
    - RunDrivers 64 (27 s).

    ComputeTargets rose by the 12 new tests only. `black --check` is clean on the three changed
    `.py` files.

Residue fixed:
- "this commit" → `0afa199` in the log and on this board, in this review's commit;
- two stale docstrings in `test_injection.py`, in `67cb5f8`:
  - item 11's class listed β = 3 among the gated searches, but β = 3 was searched only under the
    withdrawn w_rate code;
  - test 4's said every H_E ratio on β = 2 measured 0.0, true only under 02's drains.

What the gate changes, for 04 and 07, recorded and not opened:
- **The late oscillations are no longer damped.** At β = 5 and 25 the gated histories have more
  wall-side turning points than 02's (155 against 86, 296 against 75) and more steps (+24%, +34%),
  because the small late oscillations now drain almost nothing.
- **The energy is carried by the early bounces.** Σ Δρ/ρ_R and r_χ stay within 5% of 02's at every
  β.
- **The cost.** At α = 0.1 the cost is 1.37–1.89 × the dispatch commit's, about 180 µs per accepted
  step. It is within the prompt's "does not double", and 05's builds inherit it.

### 5.4 Re-plan (2026-10-29)

At the user's word ("Campaign scope: sure, we need to replan. That's life."), after the handoff
[`source/handoff_2026-10-29_large-M-stand-in.md`](source/handoff_2026-10-29_large-M-stand-in.md)
and the probes of [`source/planning-probes-2026-10-29b/`](source/planning-probes-2026-10-29b/README.md)
(`6cab14d`, `920b79e`). The plan is README §8. Nothing in the code changed.
- **Ruled by the user** (README §8.1): R6 the two modes, U2 superseded; R7 the peak estimate chosen by
  𝓡, one-loop with ln(k_ex/m_ta) and no renormalization scale; R8 unresolved below φ_t = 10⁻⁸ M_P or
  above 𝓡 = 1, reflected at 3φ_t; R9 no closed forms of the potential; R10 w_gate = 1 when unresolved;
  R11 the quanta as dark radiation; R12 ΔN_eff at T_CMB and 1 MeV, Δg* continuous; R13 the flags as
  worst margins in SQL columns, energy-weighted; R14 the parking cut; R15 the parked tracker; R16
  unparking at m/H < 10.
- **Proposed** (P18–P26, standing unless overruled) and **for the user** (U9 the energy weight, U10
  the measurement grid, U11 the rehearsal).
- **Assigned:** `[00-settling-at-physical-M-needs-a-parked-tracking-model]` from the
  `integrator-remediation` board (§3.1 above), with `.documents/OPEN_ISSUES.md` §1.20.
- **No run registered**: `runregistrykit list` showed 11 finished runs and nothing running.
