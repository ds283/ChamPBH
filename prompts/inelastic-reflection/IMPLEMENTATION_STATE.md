# inelastic-reflection campaign — implementation state

**Last updated:** 2026-10-28 · **Status: LIVE — 01 landed in `85661b5` and reviewed (2026-10-28;
gate DR holds); 2 of 8 prompts written: 02 written 2026-10-28 from log 01 §7, ready to dispatch.
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
| 02 | [Injection at turning points](02-injection-at-turning-points.md) | turning points in the step loop; the drain at the KE maximum; the restart; the floor-rule drain; the record; α in `integrate_history`/`tune_history` | Opus | ✍️ 2026-10-28 | ⬜ | — | — |
| 03 | Gate and stopping rule | 𝒜_k measured, then the gate at wall-side turning points (U4 (b′)); the plateau diagnostic; the tapered stopping rule (U5); the residual's continuity | Opus | ⬜ | ⬜ | — | — |
| 04 | Relativistic margin | generation momenta; m_eff on the trajectory; the margin to today; the warning | Opus | ⬜ | ⬜ | — | — |
| 05 | Store | α as a key (U3); per-sample ρ_χ/ρ_R; whole-history columns; `AdiabaticHistory`; format 6, 2026.8.0 | Opus | ⬜ | ⬜ | — | — |
| 06 | Driver and readers | `--alpha-values`; `main.py`; plot matching; the wrapper's scope; the inventory; the tool; the figures (U8) | Sonnet | ⬜ | ⬜ | — | — |
| 07 | Measure | the sensitivity scan by the tools; the rehearsal (U6); the verification document | Opus | ⬜ | ⬜ | — | — |
| 08 | Close | documents, `CLAUDE.md`, handover §4.18, indexes, board | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–08 were held until the user ruled on README §6.1 and §6.2,
which they did on 2026-10-27. At the user's word of the same day ("If subsequent prompts build on
earlier ones it may be preferable not write them all at once"), each of 02–08 is written after
the one before has landed and been reviewed, from what its log hands on.

Orchestrator notes, one prompt at a time, in [`orchestrator/`](orchestrator/):
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`00c17f4`), used for 01.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| DR | at K = 0 both golden histories bitwise; with an initial ρ_χ constraint and continuity agree, the radiation limit holds, a tuned history meets both conditions | 01 | ✅ 2026-10-28 (log 01 §4) |
| Inject | at α = 0 both golden histories bitwise; at α > 0 H_E continuous across every injection, ledger = Σ drains; a floor-rule drain exact | 02 | ⬜ |
| Taper | weights equal hand values; the tuning residual continuous across a threshold; the omitted fraction within its bound | 03 | ⬜ |
| Margin | the margin equals a hand computation; reported on real histories; below ln 10 warns | 04 | ⬜ |
| Store | key and columns round-trip; two α are distinct rows; the new golden fingerprint; `AdiabaticHistory` bitwise at α = 0 | 05 | ⬜ |
| Driver | α reaches every stage; an unstored α exits 2 in the plots; scope and inventory name α | 06 | ⬜ |
| Sensitivity | every scan point has a row; ΔN_eff rises with α; margins reported; α = 0 reproduces 2026.7.0 | 07 | ⬜ |
| R (if U6) | the rehearsal's stores equal after interruption and resume | 07 | ⬜ |

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
