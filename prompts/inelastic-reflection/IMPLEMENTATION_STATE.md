# inelastic-reflection campaign — implementation state

**Last updated:** 2026-10-27 · **Status: PLANNED — 1 of 8 prompts written (01); R1–R5 ruled by the
user 2026-10-27; U1–U8 ruled the same day (U4 as the revised proposal (b′): wall-side turning points
only) and P1–P16 stand. The prompts are written one at a time, each after the one before has landed
and been reviewed (README §2.1). 01 is ready to dispatch.**
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
| 01 | [Dark-radiation component](01-dark-radiation-component.md) | the ledger; ρ_χ in V/3H²M_P², Ḣ/H², E, `HubblePolicy`, V_target; K = 0 bitwise | Opus | ✍️ 2026-10-27 | ⬜ | — | — |
| 02 | Injection at turning points | turning points in the step loop; the drain at the KE maximum; the restart; the floor-rule drain; the record; α in `integrate_history`/`tune_history` | Opus | ⬜ | ⬜ | — | — |
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

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| DR | at K = 0 both golden histories bitwise; with an initial ρ_χ constraint and continuity agree, the radiation limit holds, a tuned history meets both conditions | 01 | ⬜ |
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
