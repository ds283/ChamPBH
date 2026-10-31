# Campaign — inelastic-reflection

**Written:** 2026-10-27 on branch `inelastic-reflection`, cut from `main` at `6f635e6`, by Claude
Opus 5.5. Planned at the user's request of the same day ("Can we open a campaign to implement this,
write up this analysis as a planning document, and scope out the prompting plan?"). It follows a
scoping that the user and the planner worked through together that day,
[`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md). The user's rulings from that
conversation are R1–R5 (§6.0). **Ruled by the user on 2026-10-27: U1–U8 (U4 as the revised
proposal (b′)), and P1–P16 stand** (§6.1, §6.2). Prompts 01–03 are written (01 landed `85661b5`, 02 `fe2a944`); 04–08 are written one at a
time (§2.1). **𝒜₀ = 1 ruled by the user on 2026-10-28** (§6.1), and P17 added the same day (§6.2). **The
stopping rule of U5 (w_rate), and P17 with it, were withdrawn by the user later that day**: α_eff =
α · w_gate (§6.1). **Re-planned on 2026-10-29, after 03 landed (§8):** the campaign now covers
laboratory M through a parking switch; §8 supersedes the charters of 04–08, U2 and §7.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- **The scoping** ([`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md)): the physics,
  the reasons for each reduction, and the estimates this plan relies on.
- **The planning probes** ([`source/planning-probes-2026-10-27/`](source/planning-probes-2026-10-27/README.md)),
  whose results are §0.3.
- **The briefing notes**, which the user ruled supersede the paper's particle-production sections:
  `~/Documents/Git paper repositories/Chameleon PBH (David's stuff)/BriefingNotes/`
  (`elastic_collision_estimate.tex`, `wall_backreaction.tex`, `production_energy_ratio.tex`,
  `adiabatic_transgression.tex`) and `KadanoffBaym/kb_partB_report.tex`.
- **The open issue** `[00-settling-at-physical-M-needs-a-parked-tracking-model]`
  (`integrator-remediation` board §3), which bounds this campaign's regime (U2).
- **The handover for the science rebuild**, `.documents/review-remediation-verification.md` §4.16
  and §4.17. The user holds the rebuild until this campaign lands (R5).

## 0. Why this campaign exists

### 0.1 The problem

At a reflection the scalar condensate loses part of its kinetic energy to particle production. The
code treats every reflection as perfectly elastic: a resolved turning point at the simulated M, or
π → −π under the floor rule at small M. The user asked for the loss to be modelled: at each
reflection, "drain an adjustable fraction `alpha` of the condensate's KE into chameleon quanta"
(R1), and follow how the quanta redshift, because that "will impact how the measured Planck scale
is related to the BBN Planck scale".

The scoping found that the quanta can be carried as **dark radiation** (R2): ρ_χ,E a_E⁴ is
piecewise constant between injections. The quanta are born relativistic, and stay relativistic to
today with a margin of 10⁴ or more in p/m (scoping §3). This is not a theorem, so every history
certifies it with a check computed outside the integration loop (R3). The check covers the bounces
and the late tracker, today's mass included (R4). The distribution function would be heavier, and
would need its own validity check as well (scoping §4).

**The regime tension (U2).** At the science grid's M = 0.5 M_P the bounce is classical, and the
produced energy is at most 4.5×10⁻³⁷ ρ_kin. There a nonzero α is a sensitivity study. The one-loop
value α ≈ 2.7/ℓ (0.08–0.4) applies for M ≲ TeV, where the bounces pile up in finite time and need
the parked-tracking model this campaign does not build.

### 0.2 The code at `6f635e6`

- **The state** is (φ_E, π_E = dφ/dN, ln ρ_R,E, ln f_m, ln T_J). H is rebuilt from the constraint
  3M_P²H²(1 − π²/6M_P²) = ρ_R(1 + f_m) + V. No state component or policy has room for a component
  that is neither matter nor the SM bath.
- **Where ρ_χ must enter:**
  - **V/3H²M_P²**, through its denominator T = 1/(1 + (ρ_R/V)(1 + f_m))
    (`ComputeTargets/Policies/PotentialDerivativePolicy.py:34-61`). Every policy quantity is built
    from it.
  - **Ḣ/H² + 3** = G·A₁ + C·A₂ (`PotentialDerivativePolicy.py:178-212`). It carries the pressure,
    and ρ_χ has w = 1/3.
  - **E = G − V/3H²M_P²** (`ComputeTargets/ScalarHistory.py:329`, `ComputeTargets/AdiabaticHistory.py:199`).
    It is ρ_R(1 + f_m)/3H²M_P² today and multiplies the kick. It must stay matter only, because
    the quanta are traceless.
  - **The V = 0 branch of `HubblePolicy`** (`ScalarHistory.py:386`).
  - **The tuning's V_target**, Eq. 7 (`_tuning_state`, `ScalarHistory.py:1578`). Ω_χ ≈ 7×10⁻⁶ is
    at the tolerance (scoping §3.3).
- **The step loop** (`integrate_scalar_history`, `ScalarHistory.py:562-777`).
  - **The floor rule's elastic reflection** (`:655-667`) already restarts Radau from a modified
    state at the same N. An injection is the same operation with π scaled.
  - **Resolved turning points** are found after the solve, on the dense output (`first_bounce`,
    `:795`; `surfing_turning_points`, `:885`), not in the loop. The loop must learn to see them.
- **The sampling and storage.**
  - **Per sample:** `ScalarHistoryValue` columns (`ObjectFactories/ScalarHistory.py:958-978`).
  - **Per history:** whole-history columns on the parent row (`:234-276`), including the
    fixed-temperature values at 1 MeV and 70 keV (`FIXED_T_JORDAN_*`, `ScalarHistory.py:77-78`).
  - **The keys:** the tuning tolerance is a key through a `tolerance` row
    (`ObjectFactories/ScalarHistory.py:310`, `main.py:993`, `plot_inputs.py:115-163`), and is
    named in the wrapper's scope (`RunDrivers/registered_run.py:698`). α would follow that route
    (U3).
  - **The version.** `VERSION_LABEL` is `2026.7.0` (`config/version.py:56`); `FINGERPRINT_FORMAT`
    is 5 (`config/fingerprint.py:81`).
- **BBN reads H_J** (the Hubble-only route). The density ratio r = (3M_P²H_J² − ρ_R,J(1 + f_m))/ρ_R,J
  picks up ρ_χ with no change to `BBNData` or the vendored `PRyM/`. `AdiabaticHistory` rebuilds
  V/3H²M_P² and E from the stored samples, so it needs ρ_χ on each sample.
- **Golden data.** Two golden histories pin the integrator's values:
  `ComputeTargets/tests/data/golden_history_05.json`, and `golden_history_06_tuned.json` (tuned).
  At α = 0 they must pass bitwise in every prompt (§5).

### 0.3 Measured, 2026-10-27, read-only

From the probes (`source/planning-probes-2026-10-27/`, on the production EOS, n = 1, M = 0.5 M_P):
- **The trace at the minimum falls at a^{−2} or faster**, but only just: d ln ρ_c/dN is between
  −1.98 and −2.6 from 20 TeV to 12.6 keV. m_eff·a at the tracking minimum falls by 10¹³–10¹⁵ from
  20 TeV to today for β = 0.1–25. It rises twice: by ×1.09 late at β = 25 (γ < 2/3), and by ×1.19
  between 30.2 and 28.0 keV for every β (below).
- **Σ from `Xav_EOS_spline` is non-monotone between 25 and 35 keV.** Between 25.4 and 33.0 keV it
  swings between 3.2×10⁻⁶ and 8.8×10⁻⁵, against a matter trace ρ_m/ρ_R ≈ 3×10⁻⁵ there. Two causes:
  - **The table is irregular.** The nodes of `Xav_EOS_data.csv` at 35.5, 31.6, 28.2 and 25.1 keV
    give Σ = 1 − 3w = 2.0×10⁻⁴, 2.3×10⁻⁵, 2.0×10⁻⁵ and 6.3×10⁻⁷. A Boltzmann tail would fall by a
    similar factor at each step.
  - **The cubic spline rings between the nodes** (`make_interp_spline`, `Xav_EOS_spline.py:75`). That is inside
  the BBN window, and it enters the kick and d ln ρ_R/dN of every history. Its effect on stored
  values is not measured. It is opened as `[00-xav-sigma-is-non-monotone-at-25-35-keV]` (board §3)
  and is not fixed here (P15).
- **Today's tracker mass** is m₀ = 0.27 H₀ at β = 0.1, 1.79 at β = 1, 3.26 at β = 2 and 33 at
  β = 25. The comoving Hubble rate at a bounce, relative to today's, is 5.2×10⁷ at 1 MeV and
  4.1×10⁵ at 10 keV. So p₀/m₀ ≥ 1.2×10⁴ for any quantum born with p ≥ H at or before 10 keV.

**Not measured at planning:** how many turning points a history at M = 0.5 M_P has, and their
non-adiabaticity. Prompt 02 measures the first and 03 the second, before 03 fixes the gate.

### 0.4 Baselines

At `6f635e6`, from `tuning-followups` 02's review and `CLAUDE.md`: CosmologyModels **26**,
ComputeTargets **269** (77 + 192), ObjectFactories **103**, tools **115**, RunDrivers **64**, all
passing. They were not re-run at planning; prompt 01 re-runs them first.

## 1. Scope

**In scope:**
- **The dark-radiation component**: ρ_χ,E = K(N) e^{−4N}, with K piecewise constant and held in a
  ledger. It enters the four policy expressions and V_target. At K = 0 nothing changes, bitwise (01).
- **The injection**: turning points of φ seen in the step loop; the drain of α·ρ_kin at the
  kinetic-energy maximum that follows each wall-side turning point (U4 (b′)); the Radau restart; the same drain on the
  floor rule's reflections; a record per injection (02).
- **Which turning points inject, and when to stop**: a gate on the bounce's non-adiabaticity at the
  wall-side turning points, with the plateau turnarounds recorded and never drained (U4 (b′)),
  and a tapered stopping rule on the injection rate per e-fold (U5) (03). The stopping rule was
  **withdrawn by the user on 2026-10-28** (§6.1).
- **The relativistic margin**, the check of R3/R4 (scoping §3.4): per generation, over the turning
  points and the samples, today included. It is stored, with a warning below threshold (04).
- **The store**: α as a key of the history (U3); ρ_χ/ρ_R per sample; the whole-history values;
  `AdiabaticHistory` reading ρ_χ; `FINGERPRINT_FORMAT` 6 and `VERSION_LABEL` 2026.8.0 (05).
- **The driver and the readers**: `--alpha-values`, the plot scripts' matching, the wrapper's
  scope, the inventory, `tools/history_and_bbn.py`, and the figures of U8 (06).
- **The measurement**: a sensitivity scan at M = 0.5 M_P by the tools, no Ray. If U6, a short
  registered rehearsal on the isolated cluster. The verification document (07).
- **The close**: the documents, `CLAUDE.md`, the handover for the rebuild, the indexes (08).

**Out of scope:**
- **The physical-M regime and the parked-tracking model** (U2). The floor-rule path does inject,
  so a small-M history runs as far as its step budget allows, but nothing is done about the budget.
  `[00-settling-at-physical-M-needs-a-parked-tracking-model]` stays open on its board.
- **A computed α** (2.7/ℓ_k). Each injection record keeps ℓ_k's inputs, so a later campaign can add
  it.
- **The distribution function f(q)** (R2). It is the escalation path if the margin ever fails.
- **Standard Model exchange through O₅ and O₆** (P16, scoping §7): Γ/H ≲ 10⁻¹¹ on the grid.
- **The Σ spline noise** at 25–35 keV: opened, not fixed (P15).
- **The packages, SecondaryGWKit, StochasticInstantons, the vendored `PRyM/`, `thirdparty/`**:
  read, never edited.
- **Records.** Other campaigns' records, `science-runs/` and dated `.documents/` text stay as written
  (`CLAUDE.md` invariant 6).

## 2. Prompts

> **Superseded for 04 onwards by §8 (2026-10-29).** The table, charters and gates below are the plan
> as first written; 01–03 landed under them.

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-dark-radiation-component.md` | the ledger; ρ_χ in V/3H²M_P², Ḣ/H², E, `HubblePolicy` and V_target; the derivation in the log; K = 0 bitwise; a fixed initial ρ_χ as a test harness | Opus |
| 02 | `02-injection-at-turning-points.md` | turning points of φ in the step loop; the drain at the kinetic-energy maximum after each; the restart; the floor-rule drain; the record; α as a parameter of `integrate_history` and `tune_history`; the turning-point count measured | Opus |
| 03 | `03-gate-and-stopping-rule.md` | the non-adiabaticity 𝒜_k and its gate (U4), measured on real histories first; the stopping rule and its taper (U5); the tuning residual's continuity measured | Opus |
| 04 | `04-relativistic-margin.md` | comoving peak momentum per generation; m_eff(φ) on the trajectory; the margin over turning points and samples, today included; the warning | Opus |
| 05 | `05-store.md` | α as a key (U3); the per-sample ratio; the whole-history columns; `AdiabaticHistory`; `FINGERPRINT_FORMAT` 6, `VERSION_LABEL` 2026.8.0, the golden fingerprint | Opus |
| 06 | `06-driver-and-readers.md` | `--alpha-values`; `main.py`; `plot_inputs.py`'s matching; the wrapper's scope; the inventory; `tools/history_and_bbn.py`; the figures (U8) | Sonnet |
| 07 | `07-measure.md` | the sensitivity scan by the tools; the rehearsal (U6); `.documents/inelastic-reflection-verification.md` | Opus |
| 08 | `08-close.md` | the documents, `CLAUDE.md`, the handover §4.18, the indexes, the board | Sonnet |

They run in order: each builds on the one before, and 01–05 touch the same two files.

### 2.1 When each prompt file is written

After the user rules U1–U8 and P1–P16, which they did on 2026-10-27. **One at a time** (the user,
2026-10-27: "If subsequent prompts build on earlier ones it may be preferable not write them all at
once"): 01 was written first, and each later prompt is written after the one before has landed and
been reviewed, from its log's "State handed to the next prompt". The charters of §2.2 stay the plan;
a prompt that departs from its charter says so and why. Each is dispatched with an orchestrator note
and reviewed in its own commit, as in `jordan-normalization`. Prompt 03's constants are fixed only after its first
part has measured the turning points (§0.3). If that measurement does not separate wall bounces from
small oscillations, 03 stops and asks.

### 2.2 The charters

**01 — the dark-radiation component.**
- **The ledger.** A ledger class, `DarkRadiationLedger`, holds the injections as (N_j, ΔK_j) and
  returns ρ_χ,E(N) = K(N) e^{−4N}, with K(N) = Σ_{N_j ≤ N} ΔK_j. Its one constructor argument for an
  initial ρ_χ is the test harness.
- **The policies.** `ODEPolicy` reads ρ_χ from the ledger at the N it is called with, and passes it
  to `PotentialDerivativePolicy`. ρ_χ enters V/3H²M_P², Ḣ/H² + 3 (as radiation), E (taken out, so
  that E stays matter) and the V = 0 branch of `HubblePolicy`. `_tuning_state`'s V_target
  subtracts ρ_χ,E today. The log holds the derivation of each expression from
  3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ and Ḣ = −(ρ + p)/2M_P².
- **Exactness at K = 0.** The new term is skipped exactly, so the floating-point order is
  unchanged.
- **Tests:**
  - both golden histories bitwise at K = 0;
  - with a fixed initial ρ_χ, the dense output's d ln H/dN equals the policy's Ḣ/H² to the solver's
    tolerance;
  - in a radiation-only limit, H² = (ρ_R + ρ_χ)/3M_P² to rounding;
  - a tuned history with an initial ρ_χ meets H_J(T_CMB) = H₀ and φ_ref = φ_min.
- **Gate "DR".**

**02 — the injection.**
- **Turning points.** After each accepted step the loop checks π for a sign change. A turning point
  is located on the step's interpolant. It is **wall-side** if π changes from − to + (the field
  turned back by the potential) and a **plateau turnaround** if from + to − (U4 (b′)). Only a
  wall-side turning point drains. The **drain point** is the end of the first accepted step
  after the following maximum of |π|, where π·π′ changes sign. At a kinetic-energy maximum
  dρ_kin/dN = 0, so injecting at the step's end errs only at second order in the step (P3).
- **The drain.** At the drain point, π → √(1 − α_eff) π and ΔK = α_eff ½H_E²π² e^{4N}, then a new
  Radau instance, as the floor rule does. H_E is continuous to rounding; H_J jumps, as π does.
- **Floor-rule reflections.** π_out = −√(1 − α_eff) π_in, with the drain α_eff ½H_E²π_in², in the
  one restart.
- **The record.** `IntegrationResult` gains a list of `Injection` records (P4). `reflections` keeps
  its meaning: elastic floor-rule events only.
- **The parameter.** α enters `integrate_history` and `tune_history` as a parameter, default 0.0. In
  02, α_eff = α at every wall-side turning point and floor-rule reflection; 03 adds the weights.
  Plateau turnarounds are counted and never drain.
- **Measured:** the number of turning points and drain points, and the cost in steps and restarts,
  for β ∈ {1.2, 2, 5, 25} at α = 0.1.
- **Tests:** H continuous across each injection; Σ ΔK equals the ledger; α = 0 bitwise; a
  floor-rule reflection at small M with α > 0; `first_bounce` and `surfing_turning_points` unchanged
  at α = 0.
- **Gate "Inject".**

**03 — the gate and the stopping rule.**
- **First, measure.** On the histories of 02, measure 𝒜_k = max |ṁ_eff|/m_eff² over the
  half-oscillation that ends at turning point k, for wall bounces and small oscillations. Then fix
  the gate of U4 (b′), w_gate = 𝒜_k²/(𝒜_k² + 𝒜₀²), and its constant 𝒜₀ (P6), or stop and ask.
- **The plateau diagnostic** (U4 (b′)): 𝒜_k and m_eff/H at each plateau turnaround, recorded on
  the history and reported in 07, never drained.
- **The stopping rule of U5.** S_k = Σ Δρ/ρ_R over the injections in the trailing e-fold of N_E,
  the candidate included. Its weight is w_rate = 1/(1 + (ε/S_k)²) with ε = 10⁻⁴, so it re-arms by
  construction. The effective fraction is α_eff = α · w_gate · w_rate.
- **The stopping rule is **withdrawn by the user on 2026-10-28** (§6.1).** w_rate never reaches zero, so it never saves
  a restart, its one stated benefit; it only withheld energy. The effective fraction is
  α_eff = α · w_gate. The tests and the measurement of the stopping rule, S and the omitted fraction
  go with it.
- **Tests:** each weight against hand values on synthetic sequences; a Zeno-like sequence keeps
  injecting while S ≥ ε; the omitted fraction stays under its bound.
- **Measured:** the tuning residual as a function of the normalization on a real history across a
  threshold, by finite differences. It must be continuous, which is the reason for the tapers.
- **Gate "Taper".**

**04 — the relativistic margin.**
- **Per generation:** ln q_j = ln k_ex,j + N_j, with k_ex,j = λ|v|/2√2, the classical bounce's
  version of the briefing note's Eq. `eq:ell`: λ = −d ln V″/dφ at the turning point, and v = H_E π
  at the drain point.
- **The mass:** m_eff²(φ) = V″(φ) + (β/M_P)² A ρ_c on the trajectory, at each turning point and at
  each stored sample, today included.
- **The margin:** min over j and over N > N_j of ln q_j − ln m_eff − N, stored with the N at which it
  occurs. It is printed as a warning below ln 10 (P6) and never refuses a history: warn, do not
  filter, as the user ruled for `science-readiness` P6.
- **Tests:** a constructed history against a hand computation; the warning; at α = 0 the margin is
  absent (NULL), with nothing computed.
- **Gate "Margin".**

**05 — the store.**
- **The key.** α becomes a key of `ScalarHistory` through a dimensionless row (U3).
- **Per sample:** `ScalarHistoryValue` gains `rho_chi_over_rho_rad`, the frame-invariant
  ρ_χ/ρ_R,E, which is 0.0 at α = 0.
- **Per history** (P7): the injection count, the injected and omitted fractions, ρ_χ/ρ_R at 1 MeV,
  70 keV and today, ΔN_eff today, the margin and its N.
- **Readers.** `AdiabaticHistory` reads ρ_χ from the sample. BBN is unchanged; a test shows r gains
  ρ_χ/ρ_R,J in a limit.
- **The version.** `FINGERPRINT_FORMAT` 6 with a new `SPEC_DIGESTS` pin and golden file, and
  `VERSION_LABEL` 2026.8.0 (P8).
- **Tests:** a round trip on a temporary store (`_TempStoreCase`); lookups at two α of one model; the
  golden fingerprint; `AdiabaticHistory` bitwise at α = 0.
- **Gate "Store".**

**06 — the driver and the readers.**
- **The option.** `--alpha-values` (one or more, default `[0.0]`) in the shared parser, a key, so
  the plot scripts match it and stop (exit 2) on an α with no stored history.
- **The plumbing.** `main.py` carries α through every stage. The wrapper's scope names it, the
  inventory reports it, and `tools/history_and_bbn.py` gains `--alpha`. The surfing tools stay at
  α = 0.
- **The figures** of U8.
- **Tests:** the parser, the matching, the scope, the inventory.
- **Gate "Driver".**

**07 — the measurement.**
- **The scan.** At M = 0.5 M_P, β ∈ {1.2, 2, 5, 25} and α ∈ {0, 0.03, 0.1, 0.3}, by
  `tools/history_and_bbn.py`: ΔN_eff, Yp and D/H against α; the injection counts and cost; the
  margins; the omitted fraction. A few PRyMordial solves per point, inside the run-length rule.
- **The rehearsal (U6).** If ruled, a short registered build on the isolated cluster (two histories
  at α = 0 and 0.1), interrupted and resumed, as `jordan-normalization` 09.
- **The record.** `.documents/inelastic-reflection-verification.md`.
- **Gate "Sensitivity"**, and "R" if U6.

**08 — the close.**
- **The documents.** Dated sections in `architecture-summary.md` and `numerical-strategies.md`, and
  `CLAUDE.md`'s counts and a units/redshift bullet for ρ_χ.
- **The handover.** `.documents/review-remediation-verification.md` §4.18: the α grid and ΔN_eff
  for the rebuild.
- **The indexes and the board.**

### 2.3 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **DR** | at K = 0 both golden histories pass bitwise; with an initial ρ_χ the constraint and continuity agree to tolerance, the radiation limit holds to rounding, and a tuned history meets both conditions | 01 |
| **Inject** | at α = 0 both golden histories pass bitwise; at α > 0 H_E is continuous across every injection to rounding and the ledger equals the sum of the drains; a floor-rule drain is exact | 02 |
| **Taper** | each weight equals its hand value; the tuning residual is continuous across a gate or stopping threshold on a real history; the omitted fraction is within its bound. *Since 2026-10-28 (U5 withdrawn): w_gate equals its hand values, and the residual is continuous across a threshold* | 03 |
| **Margin** | the margin equals a hand computation on a constructed history and is reported on real ones; a margin below ln 10 warns | 04 |
| **Store** | the new key and columns round-trip; histories at two α of one model are distinct rows; the golden fingerprint is the new one; `AdiabaticHistory` is bitwise at α = 0 | 05 |
| **Driver** | `--alpha-values` reaches every stage; a plot asked for an unstored α exits 2; the scope and the inventory name α | 06 |
| **Sensitivity** | every scan point has a row (a failure with its reason); ΔN_eff rises with α; every margin is reported; α = 0 reproduces the 2026.7.0 values | 07 |
| **R** (if U6) | the rehearsal's stores are equal after interruption and resume; the two α are distinct histories | 07 |

## 3. Stores

None is opened but temporary SQLite stores in `tempfile` directories (`_TempStoreCase`,
`ObjectFactories/tests/test_version_keyed_lookups.py`), except the isolated cluster's rehearsal store
under `var/datastores/` if U6. No production or shared store is touched. The science store has not
been rebuilt since `jordan-normalization`, so no 2026.7.0 store needs migrating (P8).

## 4. The interfaces between prompts

- **01 → 02:** the ledger's API, `add(N, ΔK)` and `rho_chi(N)`, and how the policies receive it.
- **02 → 03:** the `Injection` record's fields, and where the weights are applied.
- **03 → 04:** the record's final fields; which injections count as generations (those with
  α_eff > 0).
- **04 → 05:** the values to store and their names.
- **05 → 06:** the key's lookup and its name in `plot_inputs.py`.
- **06 → 07:** the tool's `--alpha` option.
- **To `main`:** the branch is merged into `main` at the user's word (U1).
- **To the rebuild:** §4.18, from 08.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", and from `dk-and-rpk-migration`: deliberate
breakages are recorded as diffs, never committed; no test needs Ray or opens a store outside a
`tempfile` directory; a package defect is reported upstream; no change in a sibling repository. And:
- **α = 0 is the old code.** Both golden histories pass bitwise in every prompt. A prompt that cannot
  keep them bitwise at α = 0 stops and asks.
- **The physics is the scoping's.** A prompt that finds an expression in the scoping or this plan
  wrong records it, derives the correct one in its log and stops for a ruling if it changes a gate.
- **Existing tests are modified only where a prompt changes what they test**, each listed with its
  reason.
- **Run length.** Nothing a prompt runs takes longer than a few PRyMordial solves, except the
  rehearsal of U6 on the isolated cluster.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.0 Ruled by the user (2026-10-27)

- **R1 — the extension.** At each reflection, "drain an adjustable fraction `alpha` of the
  condensate's KE into chameleon quanta", with the reflection "near instantaneous at the reflection
  point where `dot(phi)` changes sign". Rescattering among the quanta is neglected. This must land
  before the science rebuild.
- **R2 — dark radiation, not f(q).** After the comparison of scoping §4: "Good. Can we open a
  campaign to implement this". The distribution function is the escalation path, not this
  campaign's.
- **R3 — "stays relativistic" is checked per history**, not assumed. The user, of the scoping's
  §3.1–§3.2: "OK. That sounds correct. I couldn't see there was a guarantee."
- **R4 — the check covers the late tracker and today's mass.** The user asked: "When the bounces
  have stopped and the field is on its late-time tracker solution, do we not also need to check the
  current chameleon mass?"
- **R5 — the order.** This campaign lands before the science rebuild ("before proceeding to rebuild
  the science run, we want to add the following physics extension").

### 6.1 For the user to rule (U1–U8)

**Ruled by the user, 2026-10-27:** U2 "agree"; U3 "agree"; U5 "use your version"; U1, U6–U8
"accept as proposed. Leave out SM exchange." P1–P16 were not overruled and stand.

**U4 ruled (b′).** The user, on the proposal below: "the turnaround on the plateau is surely very
gentle, and I am not sure we have studied particle production there. What are you proposing
here?" The briefing notes estimate production at the wall only. The planner withdrew
"both directions" the same day and proposed (b′) instead, which the user ruled: "Yes, (b') for
U4."
- **drain at wall-side turning points only**, where π changes from − to + (the field turned back
  by the potential), and at every floor-rule reflection, which is wall-side by construction,
  weighted by the non-adiabaticity gate w_gate = 𝒜_k²/(𝒜_k² + 𝒜₀²), with 𝒜₀ fixed by 03's
  measurement;
- **never drain at a plateau turnaround**, where π changes from + to − (the field turned back by
  the matter coupling). There is no production estimate there, and m_eff there can be at or
  below H, where a quantum is not defined. 03 records 𝒜_k and m_eff/H at each plateau
  turnaround as a diagnostic, so that the question can be answered from the data later.

**𝒜₀ ruled, 2026-10-28.** Prompt 03's planning probe
(`source/planning-probes-2026-10-28b/`) found no gap between wall bounces and small oscillations:
𝒜_k falls steadily through each history, from 10²–10³ at the first bounce to 10⁻³–10⁻² at the last.
So 𝒜₀ could not be read off the measurement, as §2.1 and P6 assumed. The planner offered three
options: 𝒜₀ = 1, no gate, or a wider probe first. The user chose "𝒜₀ = 1", the physical threshold
|ṁ|/m² = 1. Prompt 03 measures 𝒜_k and does not re-fit it.

**U5 withdrawn, 2026-10-28.** While prompt 03 was being implemented, the user asked how w_rate
prevents a solver restart, since it is never zero. It does not: the loop skips a drain and its restart
only when α_eff is exactly 0.0, which only underflow gives. So the stopping rule never delivered the
benefit the scoping gave it (§6: "Stopping saves the restarts, not the bounces"). Its only effect was
to keep up to (π/2)ε per e-fold of energy in the condensate. Restarts are cheap at M = 0.5 M_P:
02's drained histories ran faster than at α = 0 (log 02 §5). The orchestrator offered three
options: drop w_rate; keep the taper with a hard cut-off; or keep it and document it. The user
ruled: "I'd drop `w_rate`". So **α_eff = α · w_gate**, every candidate drains its gated amount,
nothing is omitted, and ε, the trailing window, P17 and the `omitted_fraction` column of P7 go.
Orchestrator note 03, ruling 16, carries it to prompt 03.

The table below is the proposal as first written; its U4 row is superseded by (b′).

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name and branch | `inelastic-reflection`, cut from `main` at `6f635e6` with the planning commit; merged into `main` at the user's word | another name |
| **U2** — the regime | **the science grid, M = 0.5 M_P, with α as a sensitivity parameter.** The floor-rule path injects too, so small-M histories run as far as the step budget allows. The physical regime is planned with the parked-tracking model, whose physics is the authors' | plan both together now, which first needs the parking model's switch criterion and ρ_φ, p_φ ruled |
| **U3** — where α is keyed | **a key of `ScalarHistory`**, as the tuning tolerance is, through a dimensionless row. The model stays the Lagrangian (`jordan-normalization` R9). The gate and stopping constants are module constants under `VERSION_LABEL` | (b) a new "reflection model" table holding α, 𝒜₀ and ε as one key, so the constants can change without a version bump; (c) α in the model's key |
| **U4** — which turning points inject | **every turning point of φ, both directions** ("where `dot(phi)` changes sign"), **weighted by the non-adiabaticity gate** w_gate = 𝒜_k²/(𝒜_k² + 𝒜₀²), with 𝒜₀ fixed by 03's measurement. An adiabatic oscillation about the minimum produces nothing and must not be damped | (b) wall-side turning points only (π from − to +), gated; (c) every turning point, ungated, relying on the stopping rule |
| **U5** — the stopping rule | **the planner's version** (scoping §6): the trailing-e-fold sum S of Δρ/ρ_R in N_E, w_rate = 1/(1 + (ε/S)²), ε = 10⁻⁴, re-armed by construction. It bounds the omitted ΔN_eff by about 200ε = 0.02 | (b) the user's proposal as first stated: per bounce, against a fixed fraction of H_J at the bounce; (c) a different ε |
| **U6** — a rehearsal | **yes**: a short registered build on the isolated cluster, two histories at α = 0 and 0.1, interrupted and resumed, because the key and schema change | none: the temporary-store tests of 05 and the tools' scan of 07 only |
| **U7** — Standard Model exchange | **out of scope**, with scoping §7's estimate (Γ/H ≲ 10⁻¹¹) recorded in the verification document | a prompt that computes Γ/H per history as a stored diagnostic, with no back-reaction |
| **U8** — figures | **a ΔN_eff(β) panel per α in `plot_by_beta.py`, and ρ_χ/ρ_R on `plot_ScalarHistory.py`'s density panel** | the stored columns only, with no figure changes |

### 6.2 Proposed by the planner (P1–P16)

Accepted as proposed unless the user rules otherwise.
- **P1 — the ledger.** ρ_χ,E(N) = K(N) e^{−4N}, with K piecewise constant and changed only at a
  restart, so the state stays five-dimensional. A sixth component would change Radau's error norm,
  an RMS over the components, and break α = 0 bitwise.

  **The frame, and what the law assumes** (the user asked on 2026-10-27; this does not change the
  plan). ρ_χ,E is the Einstein-frame energy density, and N is the integrator's own variable, the
  Einstein-frame e-fold count N_E = ln a_E. ΔK_j = Δρ_χ,E,j e^{4N_j}.
  - **Why the Einstein frame.** φ is minimally coupled and canonical in the Einstein frame, and
    ρ_χ,E is the quantity that enters the Einstein-frame constraint, from which H is rebuilt.
  - **The Jordan frame.** For massless quanta the law is the same there:
    ρ_χ,J = A⁻⁴ρ_χ,E and a_J = A a_E, so ρ_χ,J a_J⁴ = ρ_χ,E a_E⁴. a_J is not monotonic through a
    bounce, so N_J is not a usable clock. BBN sees ρ_χ through H_J.
  - **The ratio** ρ_χ/ρ_R is the same in both frames, because both densities carry A⁻⁴.

  The law assumes:
  - **massless quanta**, which the margin of 04 certifies;
  - **collisionless quanta**: no rescattering, no exchange with the SM bath (U7), and no Hartree
    force from the condensate, which is suppressed by (m/p)²;
  - **homogeneous and isotropic quanta**, so their pressure is ρ_χ/3;
  - **instantaneous injection**, at the drain point.

  The SM bath does not obey this law: ρ_R,E departs from a_E⁻⁴ through Σ and g_s. ρ_χ,E obeys it
  exactly.
- **P2 — the expressions.**
  - **V/3H²M_P² = G/(1 + (ρ_R(1 + f_m) + ρ_χ)/V)**, in the existing log-safe form.
  - **E = G − V/3H²M_P² − ρ_χ/3H²M_P²**, so that the kick stays the trace of matter.
  - **Ḣ/H² + 3** gains the radiation term of ρ_χ.
  - **V_target** subtracts ρ_χ,E(today) inside Eq. 7's bracket in the Einstein frame.

  01 derives each in its log, and the gate checks them numerically.
- **P3 — the drain point** is the end of the first accepted step after the |π| maximum that follows
  a turning point, or the reflection itself under the floor rule. Draining at the kinetic-energy
  maximum makes the step-end error second order and needs no rollback.
- **P4 — the `Injection` record:** N; kind (turning point or floor reflection); φ and its sign of π
  at the turning point; ρ_kin = ½H_E²π² at the drain point; λ; k_ex; m_ta; 𝒜_k; w_gate; w_rate;
  α_eff; Δρ/ρ_R; ln q.
- **P5 — the restart** is the floor rule's `new_solver(N, y)`. An injection's restart is counted
  apart from a reflection's.
- **P6 — the constants**, module constants in `ComputeTargets/ScalarHistory.py`: ε = 10⁻⁴, a
  trailing window of 1 e-fold of N_E, 𝒜₀ from 03's measurement, and a margin warning at ln 10.
  (ε and the window were withdrawn with U5 on 2026-10-28.)
- **P7 — the whole-history columns** (nullable, NULL at α = 0 where undefined): `injections`,
  `injected_fraction` (Σ Δρ/ρ_R at injection), `omitted_fraction` (the bound of what the tapers
  left out; withdrawn with U5 on 2026-10-28), `rho_chi_ratio_1MeV`, `rho_chi_ratio_70keV`, `rho_chi_ratio_today`, `Delta_N_eff`
  (today, (ρ_χ/ρ_R) g_ρ(T_CMB)/[(7/4)(4/11)^{4/3}] from the EOS's own g_ρ, about 7.45 ρ_χ/ρ_R),
  `relativistic_margin`,
  `relativistic_margin_N`.
- **P8 — the version.** `FINGERPRINT_FORMAT` 6, for the key and the columns. `VERSION_LABEL` 2026.8.0,
  so that no 2026.7.0 store is reused under a schema it cannot hold. There is no such science store
  to migrate (§3).
- **P9 — BBN** is not changed. 05 adds a test that the density ratio r includes ρ_χ.
- **P10 — the tools.** `tools/history_and_bbn.py` gains `--alpha`. The surfing tools stay at α = 0,
  and their outputs are unchanged.
- **P11 — tests.** Each new test is shown to catch a breakage of what it tests. Every prompt records
  the per-package counts before and after; a count that falls is a stop.
- **P12 — the documents.** 01–06 each add dated notes in `architecture-summary.md` and
  `numerical-strategies.md` where they change what those documents describe. 08 writes the
  integrated sections and the handover. The scoping is kept as written.
- **P13 — models:** Opus for 01–05 and 07; Sonnet for 06 and 08.
- **P14 — the `H_J` jump.** π, and so H_J, is discontinuous at every injection. BBN's H spline
  already sees resolved bounces (`[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`,
  `jordan-normalization` board). 07 measures the BBN spline across injections, and an effect is
  recorded and opened, not fixed.
- **P15 — issues.** `[00-xav-sigma-is-non-monotone-at-25-35-keV]` is opened at planning, on this
  board, and not fixed here: fixing it changes every history, and needs its own ruling. Anything a
  prompt finds is recorded and opened, not fixed.
- **P16 — Standard Model exchange** is out of scope (U7). If U7 is ruled otherwise, its prompt sits
  between 05 and 06.
- **P17 — the stopping rule's sum counts candidates, not drains** (added 2026-10-28, when prompt 03
  was written). U5's S sums each candidate in the trailing e-fold at its gated, untapered size,
  c_j = α · w_gate,j · ρ_kin,j/ρ_R,E,j, the candidate itself included.
  - **Why.** If S summed the drains as made, after the taper, a Zeno sequence would never re-arm.
    1 000 candidates of 10⁻⁵ in one e-fold would leave out 10⁻², 100ε, and U5's stated bound would
    fail.
  - **The bound.** With c_j, the part the taper omits in any one-e-fold window is at most (π/2)ε.
  - **Its standing.** It clarifies "the planner's version" that the user ruled for U5, so that the
    version keeps its own bound. It stands unless the user rules otherwise.
  - **Withdrawn, 2026-10-28**, with the stopping rule itself (§6.1, "U5 withdrawn").

## 7. What this campaign hands to the user

- **α as a key of every history**, with ΔN_eff, ρ_χ/ρ_R at the BBN temperatures and today, and a
  per-history relativistic margin. The choice of α grid for the rebuild is the user's.
- **A sensitivity measurement** at M = 0.5 M_P, in the verification document.
- **What stays open:**
  - the physical regime and parking (U2);
  - a computed α;
  - the Σ spline noise (P15).
- **The merge into `main`** (U1).

## 8. Re-plan of 2026-10-29: the true M, the drain and parking

**Written:** 2026-10-29 on branch `inelastic-reflection` at `920b79e`, after prompt 03 had landed and
been reviewed, by Claude Opus 5.5, at the user's word ("Campaign scope: sure, we need to replan.
That's life.", then "I suggest we now replace [replan] on this basis, with `K=3` and
`phi_t = 1E-8 Mp` as you propose").
**It supersedes the charters of 04–08 in §2.2, their rows in §2 and §2.3, the interfaces of §4 from
03 onwards, U2 and §7.** Prompts 01–03 and their gates stand as landed. §2.1 stands: the prompts are
written one at a time, each after the one before has landed and been reviewed. Everything else in
§0–§6 stands unless this section says otherwise.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- The handoff [`source/handoff_2026-10-29_large-M-stand-in.md`](source/handoff_2026-10-29_large-M-stand-in.md)
  (`6cab14d`), whose §5.1 proposed the route taken here.
- The probes [`source/planning-probes-2026-10-29/`](source/planning-probes-2026-10-29/README.md)
  (the large-M stand-in at α = 0) and
  [`source/planning-probes-2026-10-29b/`](source/planning-probes-2026-10-29b/README.md) (`6cab14d`,
  `920b79e`: the true M, the parking cut, the parked tracker, the threshold scan).
- The briefing notes, which supersede `Paper1.tex` on particle production:
  `wall_backreaction.tex` §"Where the field is stopped", §"The loop-expansion parameter along the
  wall", §"The chain of estimates in terms of the test-field formula", §"The logarithm in terms of
  the parameters of the capture"; `adiabatic_transgression.tex` §"Laboratory-allowed M".

### 8.0 Why the plan changed

The plan of §0–§7 confined the campaign to the science grid, M = 0.5 M_P, where α is a sensitivity
parameter (U2), and left the physical regime to the parked-tracking model. Laboratory bounds put
M ≲ meV ≈ 4×10⁻³¹ M_P, where the one-loop α is about 0.08 and where the reflection is energy-limited
(`adiabatic_transgression.tex`). The probes of 2026-10-29 showed that this regime can be computed
with the code as it stands plus a parking switch:
- **At M = 10⁻³⁰ M_P every reflection is a floor-rule event**, and the drain makes each cascade a
  geometric sequence: the per-bounce ratio of |π| is √(1 − α_eff) to 3–4 digits
  (`floor_cascade.out`). Without a switch every history fails at the bottom of its first full cascade
  (φ ≤ 0 at |π| ≈ 6×10⁻¹² M_P).
- **A cut relative to the bath, ρ_kin/ρ_R < 10⁻⁶**, parks every one of them first, in 1.6–12 k steps,
  with at most 2×10⁻⁴ of the drain still to come (`cut_rhoR.out`).
- **The zero-lag tracker is adiabatic to today for M ≤ 10⁻⁷**: m/H ≥ 37 everywhere from 2 GeV to
  T_CMB at β = 0.5–5, and its lag reproduces the lags of the α = 0 histories to 5–40 %
  (`parked_tracker.out`). At M = 0.5 it stops being adiabatic at about 10–20 MeV.
- **Turnarounds too thin for Radau.** With the cut, 80 of 82 drained histories from M = 10⁻⁸ to 10⁻¹⁷
  parked. The two failures were at M = 10⁻¹⁰, where a classical wall with φ_t ≈ 10⁻¹² M_P was
  resolved and failed on Radau's float spacing (`r_criterion.out`).
- **The fix for them is numerical, not physical.** The test-field ratio 𝓡 never exceeded 1 where
  the field reached the wall: 𝓡 ∝ M⁻⁴ crosses 1 only near M ≈ 3×10⁻¹⁶, where today's floor rule
  already reflects the field far outside the wall. Reflecting a wall with φ_t < 10⁻⁸ M_P at 3 φ_t
  reproduces the resolved histories wherever both exist, and removes the failures; reflecting at
  10 φ_t shifts the branch of the bounce sequence at M = 3×10⁻¹⁰–10⁻⁹ (`phi_t_scan.out`,
  `scan_K_compare.out`).
- **The drain converges in M.** Σ Δρ/ρ_R is the same to about 10⁻³ for every M ≤ 3×10⁻¹¹ and every
  treatment of the wall: 0.115, 0.218 and 0.261 of the first-impact ceiling 1/(6β² − 1) at β = 1.2,
  2 and 3, α = 0.1. It is not α × the ceiling: the first impact gives 30–92 % of the total, and
  Hubble friction takes most of what is left.

### 8.1 Ruled by the user, 2026-10-29

Each in the user's words, from the conversation of 2026-10-29.

- **R6 — two modes.** "A 'natural' mode where there is no dissipation, so `alpha = 0`. In this mode
  we can only handle 'large' values of `M`", and "A 'dissipation' mode with specified `alpha > 0`.
  In this mode we can handle 'small' values of `M`. If dissipation mode is used with `M > M_crit`
  then the code should warn that the dynamics are expected to be nearly elastic in this regime, but
  should not actually refuse the run." Natural mode at small M: "Warn and let the run fail, if
  that's what it does." **U2 is superseded**: "Campaign scope: sure, we need to replan."
- **R7 — the estimate of the spectrum's peak.** "If the turnaround is resolved, we estimate the mass
  at turnaround and the position of the peak of the produced spectrum from the potential and the
  details of the wall at the turnaround point"; "If the turnaround is not resolved, we estimate the
  mass at turnaround and the position of the peak from the 1-loop estimate based on the field's
  incoming kinetic energy." Which of the two is used is decided physically, by 𝓡 ("Which estimate
  of the spectral peak to use is decided physically: agree"). The one-loop logarithm takes no
  renormalization scale: "The turnaround comes from the state-dependent 'dynamic' contribution,
  and for that the logarithm is `ln(k_ex / m_ta)`." The logarithm is recorded ("agree").
- **R8 — what counts as unresolved.** "Resolved vs unresolved: agree", then, after the scan, "with
  `K=3` and `phi_t = 1E-8 Mp` as you propose": a turnaround whose classical turning point lies
  below 10⁻⁸ M_P, or whose 𝓡 exceeds 1, is reflected at 3 φ_t without being resolved.
- **R9 — no closed forms of the potential.** "They depend on the potential, which is pluggable. They
  won't apply for a different potential." The turning point, 𝓡, the one-loop turnaround and the
  minimum are computed through the model's interface.
- **R10 — the gate at an unresolved turnaround** is w_gate = 1: "Use `w_gate=1` rather than an
  analytic `A_k`: OK".
- **R11 — the quanta.** "At each reflection we assume that a fraction `alpha` of the KE is deposited
  in dark radiation", which "always redshifts like radiation in the Jordan frame. It does not feel
  the Standard Model kicking function." This is prompt 01's ledger (P1).
- **R12 — ΔN_eff and Δg*.** Point values of ΔN_eff at T_CMB and at 1 MeV ("Keep the point samples of
  `Delta N_eff`"), and Δg*, the effective bosonic degrees of freedom of the quanta at T_J, as a
  continuous variable ("I like the idea of being able to plot `Delta g*` continuously"). Both are
  "just for presentation and interpretation purposes": ρ_χ reaches BBN only through H_J.
- **R13 — the relativistic flags.** "If the peak of the spectrum is not in the relativistic regime,
  we set a flag on the history"; "probably we want two flags, so we know which kind of failure it
  was: reflection or tracker", "and we can store details of the first encountered failure in the
  JSON blob". "Flags: SQL columns are OK", storing "the worst margin". On resolved turnarounds the
  flag at birth fires on every history "by design. For these histories `alpha=0` makes the most
  physical sense." The momenta checked are energy-weighted ("OK. It was bothering me that the list
  of peaks to check would become long").
- **R14 — the parking cut.** Relative to the bath, ρ_kin/ρ_R < ε ("My first guess is relative to
  `rho_R`, although the exact value of `epsilon` used may need to be tuned"), with the three
  conditions ("I agree with all your three conditions"): tested only where the energy is all
  kinetic, never before the first impact, and only where the zero-lag tracker is adiabatic. A
  guard on small |π| is a sign of a bug ("Agree with your proposal"). Parking is irreversible
  ("agree, this is the intended outcome").
- **R15 — the parked field** "tracks this evolving effective minimum with zero lag", with
  π = dφ_min/dN, and "the reduced integration can populate all the variables we need (`rho_R`,
  `f_m`, `T_J`, `H_J`) and we find `phi` and `pi` by solving algebraically for them", delivered
  "through the existing interface". The samples and the fixed-temperature values are filled from
  it, there are no turning points, and `AdiabaticHistory` computes Q on it ("happy to compute `Q`
  from the parking condition").
- **R16 — unparking.** When "the zero-lag tracker [is] no longer adiabatic", at m/H < 10 ("I suggest
  `m/H < 10`").
- **The tuning** is not changed. Whether it converges on its first integration at small M is
  "just something we learn by testing".
- **R17 — parking at large M** (2026-10-29, after prompt 06 was written; added here because it extends
  R14). The orchestrator's probes found that the switch to parking also fires at M = 0.5 M_P, at
  β ≥ 5, and at M = 10⁻³ M_P. It fires at resolved drain points, where the gate drains little, so it ends
  oscillations that would have gone on. The user ruled that R14's condition applies at every M: "At
  large M, the dissipative mode is 'unofficial'. It is not clear it corresponds to anything physical.
  I think we stick with the first option (the same parking condition applies at large M, even if it
  does terminate oscillations that could have kept going), and simply document the behaviour. It's up
  to end users to decide whether the answer represents something meaningful for whatever problem they
  are trying to solve."
- **R18 — the wall search is fixed before 08** (2026-10-30, after prompt 07 was reviewed). The
  orchestrator's probes for 08
  ([`source/planning-probes-2026-10-30/`](source/planning-probes-2026-10-30/README.md)) found that
  dissipation mode fails at its first infall, or soon after, at every M tried from 10⁻²⁵ to
  10⁻¹³ M_P. The cause is `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`. Asked
  whether to fix it before 08, fix it before 11, or leave it documented, the user chose "Insert 07b
  before 08". Prompt 07b is inserted (§8.4).
- **R19 — the peak in the one-loop gap** (2026-10-30). Asked which estimate 08 uses where
  1 < 𝓡 ≲ 10^2.9 and the one-loop chain has no solution
  (`[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`), the user chose "Classical, flagged".
  That is the one-loop estimate where 𝓡 > 1 and the chain has a solution, and the classical estimate
  elsewhere, gap included, with each generation recording which estimate it got.
- **R20 — the tracker margin is the late tracker's** (2026-10-30, when prompt 08 was written). The
  orchestrator offered three masses for R13's tracker margin, with its probe's measurements
  ([`source/planning-probes-2026-10-30b/`](source/planning-probes-2026-10-30b/README.md)):
  - A, the tracker's mass at every sample after birth;
  - B, the tracker's mass on the late tracker only, after the last turning point or generation;
  - C, the field's own mass at every sample.

  The user chose B: "B is cleanest, provided we are sure that checking at the reflections safely covers the
  time period before the last generation event." So 08 adds the plateau turning points to the reflection
  margin, derives the bound by which the turnarounds cover the samples before the late tracker, and
  measures it on every history, with a stop.
- **R21 — the parking leftover is unassessed** (2026-10-30). The leftover of P19 has no bounce. Asked
  how the margins treat it, the user chose "Unassessed, share reported": it gets no peak and no margins,
  and its share of ρ_χ is reported beside the failing shares.
- **R22 — ΔN_eff at 1 MeV** (2026-10-31, when prompt 09 was written). P7's expression,
  (4/7) g_ρ (11/4)^{4/3} ρ_χ/ρ_R, takes T_ν/T_γ = (4/11)^{1/3}, true only after e± annihilation.
  Asked which convention 09 stores at 1 MeV, the user chose "(4/7) g_ρ r at 1 MeV":
  ΔN_eff(1 MeV) = (4/7) g_ρ(1 MeV) ρ_χ/ρ_R, taking T_ν = T_γ (the scoping's own expression), and
  ΔN_eff(T_CMB) = (4/7) g_ρ(T_CMB) (11/4)^{4/3} ρ_χ/ρ_R, P7's. The orchestrator's probe
  ([`source/planning-probes-2026-10-31/`](source/planning-probes-2026-10-31/README.md)) gives 0.050
  and 0.052 at M = 10⁻³⁰ M_P, β = 2, α = 0.1, apart by what is injected after 1 MeV. P7's expression
  at 1 MeV would give 0.19.
- **R23 — the JSON blob** (2026-10-31). `extra_data` is declared `String(256)` and holds about 145
  characters; the first failures of R13 need about 550 more. The user chose "Widen extra_data": it
  becomes a `Text` column, and the first failures go under their own key in it.
- **R24 — α = 0 rows** (2026-10-31). The user chose "Zeros where defined": at α = 0, ρ_χ/ρ_R, ΔN_eff
  and the counts are 0.0 or 0, since there is exactly no dark radiation. The margins, 𝓡 at the first
  impact, the parking and the release are NULL, since none is computed (P22).
- **R25 — the margins' columns** (2026-10-31). The user chose "Charter + unassessed share": the three
  worst margins, their N, the three failing shares and the unassessed share are columns. The
  per-estimate counts, the weighted and below-weight shares and the worst generations' indices go to
  the JSON with the first failures.

### 8.2 Proposed by the planner (P18–P31)

Accepted as proposed unless the user rules otherwise.
- **P18 — the constants**, module constants in `ComputeTargets/ScalarHistory.py` under
  `VERSION_LABEL`, as P6: the parking ε = 10⁻⁶; K = 3; the unresolved threshold φ_t < 10⁻⁸ M_P; the
  unparking threshold m/H = 10, also the cut's adiabaticity condition; the |π| guard at 10⁻⁹ M_P.
  None is a key.
- **P19 — the leftover kinetic energy goes into the ledger** at the cut, so that energy is conserved
  exactly. The rest of the cascade would have drained it, Hubble friction being negligible in its
  fast tail; it is at most ε ρ_R.
- **P20 — every floor-rule reflection is unresolved**, with w_gate = 1, as the reflections of R8.
  The probes found that the gate at a floor reflection reads the infall, never the wall, and gave
  w = 0.29–1 at M = 10⁻³⁰: an artefact of where the floor puts the reflection.
- **P21 — the guard.** |π| < 10⁻⁹ M_P at a reflection, the cut not having fired, is a failure of the
  history with its reason.
- **P22 — natural mode is the old code.** At α = 0 nothing of this section runs: no φ_t, no 𝓡, no
  cut, no parking. Both golden histories stay bitwise. The two warnings of R6 are printed lines,
  never values.
- **P23 — the warnings are per history and generic.** Dissipation mode warns "nearly elastic" when
  𝓡 < 1 at the first impact; natural mode warns, at its first wall-side turnaround, when
  φ_t < 10⁻⁸ M_P, that the history is expected to fail on the step budget. Neither names an M, which
  depends on the potential.
- **P24 — one park, one release.** A released field is integrated in full to T_CMB, draining at its
  reflections, and never parks again.
- **P25 — the classical peak** is the briefing notes' k_ex = λ|φ̇_in|/2√2 (`wall_backreaction.tex`
  §"Where the field is stopped"), with λ the wall's local steepness at the classical turning point.
  The one-loop peak is the same expression at the one-loop turnaround φ_ta (§"The chain of
  estimates"). The margin at birth is ln(k_ex/m_ta).
- **P26 — Δg* is derived, not stored**: Δg* = g_ρ(T_J) ρ_χ/ρ_R from the per-sample ratio and the
  cosmology's g_ρ, in the readers. ΔN_eff at T_CMB and 1 MeV are whole-history columns.
- **P27 — the per-sample ratio travels beside the samples** (added 2026-10-31, when prompt 09 was
  written). `integrate_history`'s payload gains `"rho_chi_over_rho_rad"`, a tuple parallel to
  `"sample"` holding the policy's own r_χ at each sample. `SampleValues` does not change: both golden
  histories hash its fields, so a new field would break them at α = 0 (README §5).
- **P28 — α's row** (2026-10-31). A new dimensionless table, `alpha_value`, built by the existing
  dimensionless-quantity factory, replicated as `beta_value` and `tolerance` are. `ScalarHistory`
  gains `alpha_serial`, and its inventory spec the parent `alpha`. α = 0 is a row like any other.
- **P29 — `AdiabaticHistory` reads ρ_χ** (2026-10-31). Since 01, the potential policy takes r_χ, but
  `AdiabaticComputePolicy` calls it without, so at α > 0 its M²_eff/H² and Ḣ/H² treat ρ_χ as absent.
  09 passes each sample's stored ratio. The quanta do not couple to φ, so the conformal mass reads
  the matter and bath part E alone, never E + Y; 09 derives it in its log. At r_χ = 0.0 every
  expression takes its old operations, so α = 0 is bitwise.
- **P30 — the readers at α = 0** (2026-10-31). In 09 every lookup of a history (`main.py`, the plot
  scripts, `plot_inputs.py`, the tools) passes the α = 0 row, as far as the schema forces, as
  `jordan-normalization` 07 did for its key. The option, the matching and the figures are 10's.
- **P31 — the values at 1 MeV and 70 keV** (2026-10-31) are taken at the history's first crossing of
  T_J, as `fixed_T_values` takes φ and ρ_NP/ρ_R,J, with r_χ the policy's at the dense output's state
  there and g_ρ the cosmology's at that T_J.

### 8.3 For the user to rule (U9–U11)

**Ruled by the user, 2026-10-29:** U9 "agree"; U11 "agree"; U10, as proposed and at its full size,
after the planner explained that it is a verification run, not a science run, and estimated it at
about 52 tuned histories and 1–3 hours: "U10 is fine. 1-3 hours is fine." So §5's run-length rule
does not bind prompt 11's measurement. The user added that some of the rules in `CLAUDE.md` were
never specified or agreed by them; that is for them to settle, and this plan does not lean on the
run-length rule for anything else.

| | Proposed | Alternatives |
|---|---|---|
| **U9** — the energy weight | the margins run over the generations carrying at least 10⁻³ of the history's final ρ_χ; the fraction of ρ_χ in generations that fail is stored beside each worst margin | a different share; or every generation, with the failing fraction only |
| **U10** — the measurement | dissipation mode at M = 4×10⁻³¹ M_P (1 meV) and 10⁻¹² M_P, β ∈ {1.2, 2, 3, 5}, α ∈ {0.03, 0.1, 0.3, 0.9}; natural mode at M = 0.5 and 10⁻⁷ as the reference; ΔN_eff, Yp, D/H, the margins and the cost, by the tools; and ε ∈ {10⁻⁴, 10⁻⁶, 10⁻⁸} at one point per β | a smaller grid; or the science grid's β |
| **U11** — the rehearsal | U6 stands: a short registered build on the isolated cluster, now one natural and one dissipation history, interrupted and resumed | none |

### 8.4 The prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01–03 | as landed | the ledger; the injection; the gate | — |
| 04 | `04-the-wall-through-the-model.md` | the classical turning point, V″, 𝓡, the one-loop turnaround and the full-trace minimum with dφ_min/dN, as model methods with generic defaults | Opus |
| 05 | `05-unresolved-turnarounds.md` | the unresolved rule (R8, P20) in the step loop at α > 0; w_gate = 1 there; the records; the two warnings (P23) | Opus |
| 06 | `06-parking.md` | the cut (R14, P19, P21); the parked phase (R15) to T_CMB through the dense-output interface; the tuning at small M measured | Opus |
| 07 | `07-unparking.md` | the release at m/H < 10 (R16, P24) and the full integration after it | Opus |
| 07b | `07b-the-wall-search-at-the-rounding-of-ln-V.md` | the wall search where V − V_floor is lost in the rounding of ln V (R18); dissipation mode measured across 10⁻²⁵–10⁻¹³ M_P | Opus |
| 08 | `08-spectrum-peak-and-margins.md` | the peak per generation by 𝓡 (R7, P25); ℓ; the energy weight (U9); the three worst margins and the first failure (R13) | Opus |
| 09 | `09-store.md` | α as a key (U3); the per-sample ratio; ΔN_eff, the margins, the parking and release, 𝓡 at the first impact as columns; the JSON details; `AdiabaticHistory` on parked samples; format 6, 2026.8.0 | Opus |
| 10 | `10-driver-and-readers.md` | `--alpha-values`; the plot matching; Δg* on `plot_ScalarHistory.py`; ΔN_eff(β) per α on `plot_by_beta.py`; the scope; the inventory; the tool | Sonnet |
| 11 | `11-measure.md` | the measurement of U10; the rehearsal (U11); `.documents/inelastic-reflection-verification.md` | Opus |
| 12 | `12-close.md` | the documents, `CLAUDE.md`, the handover §4.18, the indexes, the board | Sonnet |

They run in order; 04–09 touch `ComputeTargets/ScalarHistory.py`.

**04 — the wall through the model.** No change to the integrator.
- **Methods on the model**, each a generic default through the existing hooks (`log_V`,
  `log_V_floor`, `d_logV_dphi`, `d2_logV_dphi2`, `log_Omega` and its derivatives,
  `reflects_at_origin`), which a model may override with a closed form, as `effective_minimum` is
  built:
  - the classical turning point φ_t, the root of V(φ_t) − V_floor = E inwards from φ, and V″ there;
  - 𝓡 = V″(φ_t)²/8π²E;
  - the one-loop turnaround φ_ta, where the dressing m⁴ ln(k_ex/m)/32π², m² = V″, equals E, with
    m_ta, k_ex and ℓ = ln(k_ex/m_ta), transcribed from the briefing notes with its derivation in the
    log;
  - the minimum on the full trace, V′ + (ln A)′ A ρ̃_c = 0 with ρ̃_c from ρ_R,E(Σ + f_m), and
    dφ_min/dN by the implicit function theorem along the background's derivatives.
- **Tests:**
  - φ_t and 𝓡 against the exponential closed form (the probe found 10⁻¹² in φ_t);
  - the notes' table at M = 1 meV, V₀ = 10⁻⁴⁸ GeV⁴, β = 1.2: the classical wall at y ≈ 105 and the
    one-loop stopping point at y ≈ 40;
  - a model with n = 2, so that no n = 1 closed form is used;
  - φ_min against `effective_minimum` where Σ = 0, and dφ_min/dN against a finite difference.
- **Gate "Wall".**

**05 — unresolved turnarounds.**
- **At α > 0**, on an infalling step, φ_t is computed through 04 from E = H_E²π²/2. If
  φ_t < 10⁻⁸ M_P or 𝓡 > 1, the field is reflected as soon as φ < 3φ_t, through the floor branch
  (guards, drain, restart). Today's floor rule stays.
- **Every unresolved reflection, the floor rule's included, drains with w_gate = 1** (R10, P20).
- **The record:** each candidate gains resolved or unresolved, φ_t, 𝓡 and E.
- **The warnings** of P23, printed once per history.
- **Tests:** both golden histories bitwise at α = 0; an unresolved reflection at M = 10⁻¹⁰ on the
  P1 harness, which changes `test_injection` item (8), with its reason; the band of the probe, where
  the reflected history equals the resolved one at M = 10⁻⁹, β = 2, run to a window's end; the
  warnings.
- **Gate "Unresolved".**

**06 — parking.**
- **The cut** (R14): ρ_kin/ρ_R,E < ε at a floor or unresolved reflection (on π_in) or a drain point,
  after the first impact, where m_eff/H on the tracker is at least 10. The leftover kinetic energy
  goes into the ledger (P19). The guard of P21.
- **The parked phase** (R15): a reduced integration of (ln ρ_R,E, ln f_m, ln T_J) with the existing
  right-hand side at φ = φ_min(N), π = dφ_min/dN from 04, to T_CMB. Its interpolant returns the full
  five-component state, so that the samples, the fixed-temperature values, `first_bounce` and the
  surfing readers need no change. In 06 a parked tracker that loses adiabaticity (m/H < 10) is a
  failure with its reason; 07 replaces that with the release.
- **Measured:** the tuning at M = 10⁻³⁰ and 10⁻⁷ (its integrations and residuals), and the cost.
- **Tests:** α = 0 bitwise; an M = 10⁻³⁰ history at α = 0.1 runs to T_CMB (it fails today); φ on
  the parked samples equals φ_min to rounding; H_E continuous across the switch to the order of
  the leftover; the ledger holds the leftover; the guard.
- **Gate "Park".**

**07 — unparking.**
- **The release** at m/H < 10 on the parked tracker, from (φ_min, dφ_min/dN), with a new Radau
  instance; drains continue at later reflections; the field never parks again (P24).
- **Tests:** a history that parks and is released (dissipation mode at M = 10⁻³, where the probe
  put the loss of adiabaticity near 30–40 keV); continuity at the release; the lag after it against
  the first-order estimate.
- **Gate "Release".**

**07b — the wall search at the rounding of ln V** (inserted 2026-10-30, R18).
- **The fix**, generic, in 04's `_log_V_minus_floor`: a difference ln V − ln V_floor within the
  rounding of ln V is no difference, and returns −∞, as a V that rounds to V_floor does. The width
  is a module constant derived from the search's geometry and the rounding of ln V. No closed form
  of the potential is used (R9).
- **Measured:** dissipation mode across the band 10⁻²⁵–10⁻¹³ M_P at β = 1.2, 2, 3, and the
  candidates' 𝓡 there, so that 08 knows which histories cross the one-loop gap.
- **Tests:** the helper at the edge of its width; `classical_turning_point` at the inputs on which
  the shipped search fails, against the exponential's closed form; one history in the band to
  T_CMB. Every history outside the band, and α = 0, bitwise 07's.
- **Closes** `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`.
- **Gate "Band".**

**08 — the spectrum's peak and the margins** (§2.2's 04, rewritten).
*Amended 2026-10-30, when 08 was written (R20, R21).* The reflection margin also runs over the
plateau turning points. The tracker margin runs only on the late tracker, after the last turning point or
generation, and today. The leftover is unassessed. The gate adds the coverage measured within its derived
bound, and 08 closes `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (R19).
- **Per generation:** the peak by 𝓡 (classical below 1, one-loop above, R7, P25), its comoving
  ln q_j, ℓ_j and the energy share ΔK_j/K.
- **Three margins**, each the worst over the generations of U9 and stored with its N:
  - **at birth**, ln(k_ex/m_ta);
  - **at the reflections**, ln(p/m_eff) at every later wall-side turnaround and reflection;
  - **on the tracker**, ln(p/m_eff) on the parked or late tracker, today included (R4).
  The details of the first failure of each kind go to the JSON metadata. A margin below ln 10 is
  printed as a warning and never refuses a history.
- **Tests:** a constructed history against a hand computation; the energy weight; the classical
  birth margin negative by construction on a resolved turnaround; NULL at α = 0.
- **Gate "Margin".**

**09 — the store** (§2.2's 05, extended).
*Amended 2026-10-31, when 09 was written (R22–R25, P27–P31).* ΔN_eff at 1 MeV takes T_ν = T_γ;
`extra_data` becomes `Text`; an α = 0 row holds zeros where they are defined and NULL elsewhere; the
margins' columns are the charter's and the unassessed share. The per-sample ratio is a payload key of
its own, α has its own row, `AdiabaticHistory` reads ρ_χ, and every reader passes α = 0 until 10.
09 closes `[00-settling-at-physical-M-needs-a-parked-tracking-model]`.
- **The key and the per-sample ratio** as §2.2's 05. **Whole-history columns:** P7's (without
  `omitted_fraction`), ΔN_eff at T_CMB and at 1 MeV, the three worst margins with their N, the
  failing fractions of U9, 𝓡 at the first impact, the N and T_J of parking and of release, the
  counts of resolved and unresolved candidates. **JSON metadata:** the first failure of each margin.
- **`AdiabaticHistory`** computes Q on the parked samples like any others.
- **The version.** `FINGERPRINT_FORMAT` 6 and `VERSION_LABEL` 2026.8.0 (P8).
- **Gate "Store"**, as §2.3, with the new columns.

**10 — the driver and the readers** (§2.2's 06). As there, with the figures of R12 in place of U8:
Δg*(T_J) on `plot_ScalarHistory.py`'s density panel, and ΔN_eff(β) per α on `plot_by_beta.py`.
**Gate "Driver".**

**11 — the measurement** (§2.2's 07). The grid of U10 by `tools/history_and_bbn.py`; the rehearsal
of U11; the verification document. **Gates "Measure"**: every point has a row, a failure with its
reason; at M ≤ 10⁻¹² the drain is independent of M to the probes' 10⁻³; ΔN_eff rises with α; every
margin is reported; α = 0 reproduces 2026.7.0. **"R"** as §2.3.

**12 — the close** (§2.2's 08).

### 8.5 The interfaces

- **03 → 04:** the gate's records; `log_m_eff_squared`.
- **04 → 05, 06, 08:** the model methods and their names.
- **05 → 06:** the reflection kinds and the candidate record.
- **06 → 07:** the parked phase's state and its interpolant.
- **07 → 08:** the trajectory with parked and released phases; the generations.
- **08 → 09:** the values to store and their names.
- **09 → 10, 10 → 11, to `main`, to the rebuild:** as §4.

### 8.6 What this campaign now hands to the user

- **α as a key of every history**, with two modes: natural (α = 0) at large M, and dissipation
  (α > 0) down to laboratory M through parking.
- **ΔN_eff at T_CMB and 1 MeV, and Δg* as a continuous variable**; three relativistic margins per
  history.
- **The measurement** of U10, in the verification document.
- **Closed here:** `[00-settling-at-physical-M-needs-a-parked-tracking-model]`
  (`integrator-remediation` board), assigned to this campaign on 2026-10-29: 06 and 07 build the
  parked-tracking model, with the switch criterion supplied by the drain.
- **What stays open:**
  - a computed α (2.7/ℓ is recorded, not used);
  - the reflection at laboratory M, which lies outside the low-energy theory: α is bracketed, not
    predicted (handoff §2.2);
  - the Σ spline noise (P15), which sets the release point at M = 10⁻³ (`parked_tracker.out`);
  - the handoff's observation that BBN yields at M ≥ 10⁻³ are not in the hard-wall limit (§2.2 of
    the handoff), which is the user's to place.
