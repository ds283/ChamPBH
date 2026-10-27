# Campaign — inelastic-reflection

**Written:** 2026-10-27 on branch `inelastic-reflection`, cut from `main` at `6f635e6`, by Claude
Opus 5.5. Planned at the user's request of the same day ("Can we open a campaign to implement this,
write up this analysis as a planning document, and scope out the prompting plan?"). It follows a
scoping that the user and the planner worked through together that day,
[`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md). The user's rulings from that
conversation are R1–R5 (§6.0). **Ruled by the user on 2026-10-27: U1–U8 (U4 as the revised
proposal (b′)), and P1–P16 stand** (§6.1, §6.2). Prompt 01 is written; 02–08 are written one at a
time (§2.1).

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
  and a tapered stopping rule on the injection rate per e-fold (U5) (03).
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
| **Taper** | each weight equals its hand value; the tuning residual is continuous across a gate or stopping threshold on a real history; the omitted fraction is within its bound | 03 |
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
- **P7 — the whole-history columns** (nullable, NULL at α = 0 where undefined): `injections`,
  `injected_fraction` (Σ Δρ/ρ_R at injection), `omitted_fraction` (the bound of what the tapers
  left out), `rho_chi_ratio_1MeV`, `rho_chi_ratio_70keV`, `rho_chi_ratio_today`, `Delta_N_eff`
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

## 7. What this campaign hands to the user

- **α as a key of every history**, with ΔN_eff, ρ_χ/ρ_R at the BBN temperatures and today, and a
  per-history relativistic margin. The choice of α grid for the rebuild is the user's.
- **A sensitivity measurement** at M = 0.5 M_P, in the verification document.
- **What stays open:**
  - the physical regime and parking (U2);
  - a computed α;
  - the Σ spline noise (P15).
- **The merge into `main`** (U1).
