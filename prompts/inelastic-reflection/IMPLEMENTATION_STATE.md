# inelastic-reflection campaign — implementation state

**Last updated:** 2026-11-01 · **Status: CLOSED 2026-11-01 — 13 of 13 prompts landed and reviewed (01–11 with 07b, and 12, the close); every
gate holds; three issues open, none assigned (a fourth closed as measured after the close); the merge into `main` is the user's (U1).**
The plan is [`README.md`](README.md), written on 2026-10-27 on branch `inelastic-reflection`, cut
from `main` at `6f635e6` with the planning commit (U1). Its physics basis is
[`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md).

At each reflection, the campaign drains a fraction α of the condensate's kinetic energy into
chameleon quanta. It carries them as dark radiation, ρ_χ,E = K e^{−4N} with K piecewise constant,
and certifies per history that they stay relativistic, at the bounces and on the late tracker to
today. It makes α a key of every history and stores ΔN_eff and ρ_χ/ρ_R. At α = 0 every value is
bitwise the old code's.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/ScalarHistory.py`, `ComputeTargets/dark_radiation.py`,
`ComputeTargets/Policies/PotentialDerivativePolicy.py`, `ComputeTargets/AdiabaticHistory.py`,
`CosmologyConcepts/Models/AbstractModel.py`, `CosmologyConcepts/alpha_value.py`,
`ObjectFactories/ScalarHistory.py`, `config/`, `main.py`, `plot_inputs.py`, `extract_common.py`, the plot
scripts, `RunDrivers/registered_run.py`, `tools/history_and_bbn.py`, `tools/bbn_from_store.py`,
`tools/inventory_report.py` ·
**Verification:** `.documents/inelastic-reflection-verification.md` (created by 11) ·
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
| 04 | [The wall through the model](04-the-wall-through-the-model.md) | φ_t, V″, 𝓡, the one-loop turnaround, the full-trace minimum and ∂φ_min/∂ ln T as model methods with generic defaults (R9); dφ_min/dN moves to 06 | Opus | ✍️ 2026-10-29 | ✅ 2026-10-29 | `81b933f` | [04](logs/04-the-wall-through-the-model.md) |
| 05 | [Unresolved turnarounds](05-unresolved-turnarounds.md) | φ_t < 10⁻⁸ M_P or 𝓡 > 1 → reflect at 3φ_t (R8); w_gate = 1 there and at the floor rule (R10, P20); the records; the warnings (P23) | Opus | ✍️ 2026-10-29 | ✅ 2026-10-29 | `9c2483b` | [05](logs/05-unresolved-turnarounds.md) |
| 06 | [Parking](06-parking.md) | the cut ρ_kin/ρ_R < ε (R14, P19, P21); the parked phase to T_CMB through the dense-output interface (R15); the tuning at small M measured | Opus | ✍️ 2026-10-29 | ✅ 2026-10-29 | `e967cb7` | [06](logs/06-parking.md) |
| 07 | [Unparking](07-unparking.md) | the release at the root of m/H = 10 on the parked step (R16, P24); the oscillating phase resumes; no second parking | Opus | ✍️ 2026-10-29 | ✅ 2026-10-30 | `5397f8f` | [07](logs/07-unparking.md) |
| 07b | [The wall search at the rounding of ln V](07b-the-wall-search-at-the-rounding-of-ln-V.md) | the wall search's helper returns −∞ within the rounding of ln V (R18), with its width derived; dissipation mode measured across 10⁻²⁵–10⁻¹³ M_P; the one-loop gap measured for 08 | Opus | ✍️ 2026-10-30 | ✅ 2026-10-30 | `92f41f2` | [07b](logs/07b-the-wall-search-at-the-rounding-of-ln-V.md) |
| 08 | [Spectrum peak and margins](08-spectrum-peak-and-margins.md) | the peak by 𝓡 (R7, P25), the gap flagged (R19); ℓ; the energy weight (U9); three worst margins with the first failure (R13), the reflection margin over plateau turning points too and the tracker margin on the late tracker (R20), the leftover unassessed (R21); the coverage bound measured | Opus | ✍️ 2026-10-30 | ✅ 2026-10-31 | `86fc526` | [08](logs/08-spectrum-peak-and-margins.md) |
| 09 | [Store](09-store.md) | α as a key in its own table (U3, P28); the per-sample ratio as a payload key of its own (P27); ΔN_eff at T_CMB and 1 MeV (R22), the margins (R25), parking and release as columns, zeros at α = 0 where defined (R24); the first failures in `extra_data`, widened (R23); `AdiabaticHistory` with ρ_χ (P29) and on parked samples; every reader at α = 0 (P30); format 6, 2026.8.0 | Opus | ✍️ 2026-10-31 | ✅ 2026-10-31 | `3984ec5` | [09](logs/09-store.md) |
| 10 | [Driver and readers](10-driver-and-readers.md) | `--alpha-values` (P32); one pipeline per α in each driver (P33); the plots' matching (P34) and `alpha=<α>/` paths (R26); `dark_radiation.pdf` with Δg* (R27) and `Delta_N_eff.pdf`, two panels (R28); the scope, the inventory, both tools' `--alpha` (P37–P39) | Sonnet | ✍️ 2026-10-31 | ✅ 2026-10-31 | `8c12afa` | [10](logs/10-driver-and-readers.md) |
| 11 | [Measure](11-measure.md) | the grid of U10, 48 tool runs as a detached batch (R29, P40, P41); the ε scan by patch (R30, P42); the rehearsal of `jordan-normalization` 09's run `a` at two α, interrupted and resumed, α = 0 against its 2026.7.0 store, and the readers' first run with α (R31); the verification document | Opus | ✍️ 2026-11-01 | ✅ 2026-11-01 | `c797831` | [11](logs/11-measure.md) |
| 12 | [Close](12-close.md) | documents (§6.10, §6.11, §7.2, §14), `CLAUDE.md` by description only, handover §4.18, indexes, board | Sonnet | ✍️ 2026-11-01 | ✅ 2026-11-01 | `7ad6744` | [12](logs/12-close.md) |

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
- [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) (`a8e0406`), used for 04.
- [`orchestrator/prompt-05.md`](orchestrator/prompt-05.md) (`cd19768`), used for 05.
- [`orchestrator/prompt-06.md`](orchestrator/prompt-06.md) (`7094056`), used for 06.
- [`orchestrator/prompt-07.md`](orchestrator/prompt-07.md) (`35a103c`; ruling 9 amended at the user's word in `0994a10`), used for 07.
- [`orchestrator/prompt-07b.md`](orchestrator/prompt-07b.md) (`c2aac94`), used for 07b.
- [`orchestrator/prompt-08.md`](orchestrator/prompt-08.md) (`f0c139d`), used for 08.
- [`orchestrator/prompt-09.md`](orchestrator/prompt-09.md) (`03b769e`), used for 09.
- [`orchestrator/prompt-10.md`](orchestrator/prompt-10.md) (`d097745`), used for 10.
- [`orchestrator/prompt-11.md`](orchestrator/prompt-11.md) (`6d1a77c`), used for 11.
- [`orchestrator/prompt-12.md`](orchestrator/prompt-12.md) (`7cfced4`), used for 12.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| DR | at K = 0 both golden histories bitwise; with an initial ρ_χ constraint and continuity agree, the radiation limit holds, a tuned history meets both conditions | 01 | ✅ 2026-10-28 (log 01 §4) |
| Inject | at α = 0 both golden histories bitwise; at α > 0 H_E continuous across every injection, ledger = Σ drains; a floor-rule drain exact | 02 | ✅ 2026-10-28 (log 02 §1.2, §4, §6) |
| Taper | weights equal hand values; the tuning residual continuous across a threshold; the omitted fraction within its bound. *Since ruling 16 (2026-10-28, the stopping rule withdrawn): w_gate equals its hand values, and the residual is continuous across a threshold* | 03 | ✅ 2026-10-28, as restated (log 03 §4, §5.3) |
| Wall | φ_t and 𝓡 equal the exponential closed form; the notes' table (classical wall y ≈ 105, one-loop stopping point y ≈ 40 at 1 meV) reproduced; an n = 2 model needs no closed form; φ_min and dφ_min/dN checked. *As prompt 04 §0.2 reads it: y = 39.1 is the chain's (`sec:chain`), and 04 checks ∂φ_min/∂ ln T, dφ_min/dN being 06's* | 04 | ✅ 2026-10-29 (log 04 §1.5, §4.2, §4.3, §5.1) |
| Unresolved | α = 0 bitwise; an unresolved reflection is made at 3φ_t with w_gate = 1; the reflected history equals the resolved one in the probe's band; the warnings | 05 | ✅ 2026-10-29 (log 05 §4, §5.4; at M = 3×10⁻¹¹ the resolved comparison run itself fails, as the planner's did; D14 accepted by the user) |
| Park | α = 0 bitwise; an M = 10⁻³⁰ drained history reaches T_CMB; parked samples on φ_min; H_E continuous at the switch; the leftover in the ledger; the guard | 06 | ✅ 2026-10-29 (log 06 §1.4, §2, §5.4; the guard tested with its threshold raised by a patch, as the prompt allows) |
| Release | a parked field released at m/H < 10 continues; continuity at the release; the lag after it as estimated. *As ruled by the user (ruling 9, option (c)): the lag after the release has the sign of δ₁ and lies within the bound derived from each oscillation* | 07 | ✅ 2026-10-30 (log 07 §1.2, §1.4, §4, §5.2, §5.5) |
| Band | α = 0 bitwise; every history outside the band bitwise 07's; every history of the band (β = 1.2, 2, 3 × M = 10⁻²⁷–10⁻¹³ M_P) reaches T_CMB; the width derived | 07b | ✅ 2026-10-30 (log 07b §1.2, §5.1, §5.4, §5.5) |
| Margin | the three margins equal a hand computation; energy-weighted; the classical birth margin negative by construction; NULL at α = 0. *As written 2026-10-30 (R19–R21): the estimate chosen by 𝓡 with the gap flagged; the coverage of the samples before the late tracker measured within its derived bound; every value of the integration bitwise* | 08 | ✅ 2026-10-31 (log 08 §1, §2, §5.1, §5.3, §5.5) |
| Store | key and columns round-trip; two α are distinct rows; the new golden fingerprint; `AdiabaticHistory` bitwise at α = 0 and computed on parked samples. *As written 2026-10-31 (R22–R25): an α = 0 row holds R24's zeros and NULLs; both golden histories bitwise* | 09 | ✅ 2026-10-31 (log 09 §4, §5.1, §5.3, §5.5) |
| Driver | α reaches every stage; an unstored α exits 2 in the plots; scope and inventory name α. *As written 2026-10-31 (R26–R28): the `alpha=<α>/` paths, `dark_radiation.pdf` and `Delta_N_eff.pdf`; both golden histories bitwise and the tools' α = 0 output byte-identical* | 10 | ✅ 2026-10-31 (log 10 §4, §5.1, §5.2) |
| Measure | every point has a row; the drain independent of M below 10⁻¹² to 10⁻³; ΔN_eff rises with α; margins reported; α = 0 reproduces 2026.7.0. *As written 2026-11-01 (R31, P44): the drain compared at M = 10⁻¹² and 4×10⁻³¹ per (β, α); ΔN_eff today strictly rising; α = 0 bitwise against `jordan-normalization` 09's reference store* | 11 | ✅ 2026-11-01 (log 11 §0; verification §1, §2.4) |
| R (U11) | the rehearsal's stores equal after interruption and resume. *As written 2026-11-01 (R31): the two α distinct histories; the readers' first run with α as log 10 says* | 11 | ✅ 2026-11-01 (log 11 §0; verification §2) |

*The gates Margin, Store, Driver, Sensitivity and R of the old 04–07 were replaced by the re-plan of
2026-10-29 (README §8.4).*

## 3. Active and unresolved issues

The campaign closed on 2026-11-01 with these four open and none assigned; prompt 12 opened and closed none.
*(2026-11-01, after the close: `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]` was
measured and closed as measured by the user (§4); three stay open.)*
One was opened by the planner, one by the orchestrator's review of prompt 09, and two by prompt 11. Prompt 04's
was closed by prompt 08, and prompt 05's by prompt 07b (§4). The one assigned (§3.1) was closed by prompt 09 (§4).

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

### `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]`

**Opened:** 2026-10-31 by the orchestrator's review of prompt 09, from log 09 §5.2 and §7.

- **What.** Along a history the stored ρ_R,J departs from (π²/30) g_ρ(T_J) T_J⁴, the bath the cosmology's
  own tables give at the history's T_J.
  - **Measured (log 09 §5.2, §7):** −0.210 % at T_J = 1 MeV and −0.078 % today, on M = 0.5 M_P, β = 2,
    α = 0.1. The ODE sets ρ_R from g_ρ T⁴ only at T* and evolves it through Σ = 1 − 3w, while T_J follows
    entropy conservation with g_s. The temperature law holds to 2×10⁻¹⁰.
  - **The cause, as far as the review measured.** The relation g_s = (3/4)(1 + w) g_ρ holds exactly while
    every species shares one temperature. `Xav_EOS_spline`'s three functions miss it:
    g_s/((3/4)(1 + w) g_ρ) − 1 is −6.7×10⁻⁵ at 10 GeV, +3.6×10⁻³ at 1 GeV, −9.1×10⁻⁴ at 100 MeV,
    +1.6×10⁻⁴ at 10 MeV and +3.0×10⁻³ at 1 MeV. (Below about 0.1 MeV the relation does not apply,
    since T_ν ≠ T_γ there; it reaches 0.16.) Part of the value at 1 MeV may be the start of
    e± annihilation, which the relation does not cover.
  - **Corrected, 2026-10-31 (after the review, at the user's question).** The algebraic relation above
    is the single-temperature special case. It is not the condition the ODE needs, and it fails after
    neutrino decoupling by construction (`SaikawaShirai_EOS_spline.w` is that relation solved for w, and
    is frozen at 2 MeV for that reason). The condition that holds at every temperature follows from
    energy and entropy conservation of the whole plasma. With x = (1/3) d ln g_s/d ln T:

      4 + d ln g_ρ/d ln T = 3(1 + w)(1 + x).

    Its residual, integrated over the tables alone from T* = 2×10⁴ GeV (no history), is the drift of
    ln ρ_R from ln(g_ρ T⁴):

    | T (GeV) | 100 | 1 | 0.1 | 10⁻² | 10⁻³ | 10⁻⁴ | today |
    |---|---|---|---|---|---|---|---|
    | cumulative drift | +0.19 % | **−1.17 %** | −0.08 % | −0.21 % | **−0.211 %** | −0.098 % | **−0.078 %** |

    It reproduces log 09's −0.210 % at 1 MeV and −0.078 % today, so the cause is confirmed: the
    tabulated w (Xav's table) and the Saikawa–Shirai fits for g_ρ and g_s disagree on the shape of the
    QCD transition. The error is not about 10⁻³ throughout. It peaks at −1.2 % in ρ_R near 1 GeV, where
    the QCD-era kicks act. Most of it is undone by 0.1 GeV. The issue's name keeps its first wording.
- **Impact.**
  - It is the whole of ruling 12's residual, −1.328×10⁻³ on every history (log 09 §5.2). Every ΔN_eff
    column (R12, R22) carries it at about 10⁻³ relative. Those columns are for presentation (R12).
  - ρ_NP reaches PRyMordial as r ρ_SM(T) (`build_rho_NP_callback`), with r relative to the stored
    ρ_R,J. So the offset enters BBN's expansion rate only through r: at r = 0 BBN is plain PRyMordial.
  - The history's own H, and so its dynamics, carries the bath's offset. *Corrected, 2026-10-31:* that
    is up to about 0.6 % in H (1.2 % in ρ_R) near 1 GeV, and about 10⁻³ at BBN. Its effect on φ, H_J and
    the yields is not measured.
  - It is every history's, at every α, and predates this campaign.
- **Next step.** For the user. Decide whether to make the table's w consistent with its g_ρ and g_s, or to
  hold ρ_R to g_ρ T⁴ along the history. Either changes every history and needs its own ruling, so it is
  not in this campaign (README P15). It is related to `[00-xav-sigma-is-non-monotone-at-25-35-keV]` (the
  same table, another defect) and `[00-kicking-function-table-has-no-provenance-in-the-repository]`
  (`review-remediation` board). Not assigned.
  - **The options**, as put to the user on 2026-10-31:
    1. derive g_s from w and g_ρ by integrating the condition above, from a high-T anchor where
       g_s = (3/4)(1 + w) g_ρ;
    2. derive w from g_ρ and g_s (Σ would become a difference of the fits' derivatives, noise-dominated
       in the BBN window);
    3. derive g_ρ from w and g_s (g_ρ also feeds BBN's ρ_SM, `gstar_rho` and ΔN_eff);
    4. set ρ_R = (π²/30) g_ρ T_J⁴ from T_J and drop it from the ODE's state (it breaks the golden
       histories and the bath's energy conservation);
    5. rebuild the table from one p(T), with neutrino decoupling explicit, which would also answer the
       Σ issue and the provenance issue;
    6. leave it documented.

    The orchestrator recommended measuring first: one history with option 1's g_s against the current
    tables, compared in φ, H_J, Yp and D/H.
  - **Ruled by the user, 2026-10-31:** "For now, though, I'm going just to leave it documented." Option 6.
    The issue stays open and not assigned.

### `[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]`

**Opened:** 2026-11-01 by prompt 11 (log 11 §6; verification §6).

- **What.** Ray forwards worker output through a deduplicator that keys a line on its words without digits
  and, for a key already seen from another process within 5 s, prints only the latest with
  `[repeated Nx across cluster]` (`jordan-normalization` verification §1.13). The history's warnings (R6's
  "nearly elastic", P23's, R13's margin lines) carry the history's label, but their words are the same across
  histories. In `reference-a` the α = 0.1 histories β = 1.5 and 3 printed three warnings each (the standalone
  tool prints them: `out/11-tool-1.5-a0.1.txt`, `out/11-tool-3-a0.1.txt`), and the build's log holds them
  only inside β = 5.5's lines, `[repeated 3x across cluster]`.
- **Impact.** A registered build's log does not show which histories warned. The stored columns (𝓡 at the
  first impact, the margins and their shares, R25) hold the same facts, and the inventory and the tools read
  them. `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` (`jordan-normalization`) was closed by
  `tuning-followups` 02 as superseded by quiet tune lines; the warnings are printed whatever the quiet rule
  says, so they are now what is lost.
- **Next step.** For the user: accept it (the columns are the record), or have the drivers set Ray's
  `RAY_DEDUP_LOGS=0`, or print a per-history summary of the warnings from the stored row after each batch.
  Not assigned.

### 3.1 Assigned to this campaign

None now. `[00-settling-at-physical-M-needs-a-parked-tracking-model]` was closed by prompt 09 (§4).

## 4. Resolved issues

### `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]`

**Opened:** 2026-11-01 by prompt 11, as README P14 and P46 ask ("an effect is recorded and opened, not fixed").

- **What.** The ratio ρ_NP/ρ_R,J that BBN's spline reads steps at each injection and at the switch to parking,
  since π, and so H_J, is discontinuous there (P14). The tool's `ratio` lines measure it per window
  (verification §3; `out/11-grid-tables.md` §2.9).
  - **Measured, 10–100 keV window,** rms_step of consecutive samples against natural mode at M = 10⁻⁷, the same
    β: β = 1.2, α = 0.03 (injecting through the window, parks at 0.57 eV) **6.4×** (0.0106 against 0.00165);
    β = 1.2, α = 0.1 (parks at 11.6 keV) **8.4×**; β = 1.2, α = 0.3 (parks at 44.2 keV) **2.9×**; β = 2,
    α = 0.03 (parks at 25.4 keV) **1.26×**. The same at M = 10⁻¹² and 4×10⁻³¹.
  - Everywhere else it is below natural mode's (0.26× to 3×10⁻⁵×): the history parked above 100 keV, and a
    parked history's ratio is flat (rms_step about 10⁻¹⁴).
- **Impact.** Not measured. The steps are of the size of natural mode's own resolved-bounce steps
  (`[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`, `jordan-normalization` board) and up to 8×
  larger. Whether PRyMordial's spline rings on them, and by how much Yp and D/H move, is not known. Yp at
  β = 1.2 is not monotonic in α (0.25386, 0.24949, 0.24952, 0.25411 at α = 0.03, 0.1, 0.3, 0.9), which may or
  may not be this.
- **Next step.** For the user. Compare Yp and D/H from the stored samples with those from a spline that
  respects the steps (or from samples refined around each injection), at β = 1.2, α = 0.03 and 0.1. Not
  assigned.

- **Closed (2026-11-01, measured, outside any campaign):** closed as measured by the user ("please record
  the measurements and close the issues as measured. i won't plan a fix at the moment"). The record is
  [`.documents/bbn-ratio-spline-2026-11-01/`](../../.documents/bbn-ratio-spline-2026-11-01/README.md): six
  tuned histories (dissipation at M = 4×10⁻³¹ for β = 1.2, α = 0.1 and 0.03 and β = 2, α = 0.03; natural
  mode at M = 10⁻⁷ for β = 1.2 and 2, and at β = 1.6, M = 10⁻⁵), each sampled again 100× finer inside BBN's
  window.
  - The steps that matter are the reflections' jumps, about 1 in r between two samples, which natural mode
    has too; a drain is a much smaller step.
  - The cubic spline rings next to each jump (15–20 % of it), but cubic, linear and PCHIP on the same
    samples give Yp within 3.7×10⁻⁵ relative and D/H within PRyMordial's scatter.
  - Where each jump falls inside its interval moves Yp by up to 4.8×10⁻⁴ and D/H by up to 6.2×10⁻⁴,
    relative, converging tenfold per tenfold in density. That is too small to explain the non-monotonic Yp
    at β = 1.2 above.
  - No fix is planned. The same record closes `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`,
    which this entry and verification §3 call the `jordan-normalization` board's; it is the
    `science-readiness` board's (its §4).

### `[00-settling-at-physical-M-needs-a-parked-tracking-model]`

Assigned from the `integrator-remediation` board (its §3, now its §4).

- **`[00-settling-at-physical-M-needs-a-parked-tracking-model]`** (`integrator-remediation` board
  §3), assigned 2026-10-29 by the re-plan (README §8.6): prompts 06 and 07 build the parked tracker,
  with the switch criterion supplied by the drain. The entry stays on its own board until it is
  closed.
  - **Narrowed (2026-10-30, prompt 07):** the switch, the tracker and the release are built and
    measured (logs 06 and 07): every history of log 05's grid parks and reaches T_CMB, and the four
    large-M histories whose tracker loses adiabaticity are released at m/H = 10 and reach T_CMB. What
    remains is Q on the parked samples (`AdiabaticHistory`), which is 09's (README §8.1 R15). So 07
    narrows it, and 09 closes it.
  - **Closed (2026-10-31, prompt 09, `3984ec5`):** `AdiabaticHistory` reads each stored sample's
    ρ_χ/ρ_R and computes Q on the parked samples as on any other (R15). Q is finite on every parked sample
    measured: 2 142 at β = 2, M = 10⁻³⁰ and 10⁻¹⁶; 1 904 at M = 10⁻³, β = 1.2 (released); 244 at M = 0.5,
    β = 5 (released) (log 09 §5.3; test 8 (c)). The entry is closed on the `integrator-remediation` board
    too (moved to its §4), and its row is deleted from `.documents/OPEN_ISSUES.md` §1.20, the count
    corrected 25 → 24.

### `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`

**Opened:** 2026-10-29 by prompt 04, found by its measurement of the 𝓡 = 1 crossover (log 04 §5.2).

- **What.** R7 and README §8.4's charter for 08 choose the spectrum's peak by 𝓡: the classical estimate
  below 𝓡 = 1, the one-loop estimate above. On the exponential wall (n = 1, V₀ = 10⁻⁴⁸ GeV⁴), the
  one-loop chain of `wall_backreaction.tex` §"The chain of estimates" has **no solution for
  1 < 𝓡 ≲ 10^2.9**:
  - At the worked example's E, 𝓡 = 1 at M = 10^−15.378 M_P (1.02 TeV).
  - The fixed point m_ta⁴ℓ = 32π²E, ℓ = ln(k_ex/m_ta) exists only from M = 10^−16.109 M_P (190 GeV)
    downwards. It disappears there as a saddle-node at ℓ ≈ 0.24, with the field still outside the
    classical wall.
  - At E × 10⁻⁴ the gap is the same 0.73 decades of M (𝓡 = 1 at 10^−16.457, the chain's edge at
    10^−17.190).
  - At 𝓡 = 1 itself the first iterate already gives ℓ = −0.20.
  - At the chain's edge the one-loop ln k_ex is 0.039–0.043 below the classical one, and
    ln m_ta − ln ω_t = −1.05.
  - Prompt 04 §1.3 expected the chain to reach below 𝓡 = 1. The notes (`sec:params`) put its end where
    y reaches L, at M_crit; measured, it ends first, at ℓ ≈ 0.24.
- **Impact.** For 1 < 𝓡 ≲ 10³ the estimate R7 names does not exist, and `one_loop_turnaround` raises
  there. Nothing calls it yet. 05 needs only 𝓡 and φ_t, which exist everywhere. Per history, how many
  generations fall in the gap is not measured; at laboratory M (10⁻³⁰ M_P) 𝓡 is 10^50 or more, far
  from it.
- **Next step.** For 08's prompt, and the user if it changes the gate "Margin". The options include:
  - the classical estimate up to the chain's edge;
  - the one-loop estimate where it exists and the classical one elsewhere, with the switch recorded;
  - a flag on generations in the gap.

  At the edge the two k_ex differ by 4 %, so the first two differ little in ln q. Not assigned.
- **Ruled (2026-10-30, README §8.1 R19).** The user chose "Classical, flagged": 08 uses the one-loop
  estimate where 𝓡 > 1 and the chain has a solution, and the classical estimate elsewhere, gap included,
  with each generation recording which estimate it got. The orchestrator's probes
  (`source/planning-probes-2026-10-30/`) found no generation in the gap on the histories that ran.
  Histories that cross it lie near M = 10⁻¹⁵–10⁻¹⁷ M_P (ln 𝓡 from −19.7 to +4.2 decades at 10⁻¹⁶), inside
  the band where `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` fails them. 07b
  measures the gap there, and 08 closes this issue.
- **Measured (2026-10-30, prompt 07b, log 07b §5.3).** On the 30 band histories the chain raises
  ("ell fell to …") on every candidate with 0.05 ≤ log₁₀ 𝓡 ≤ 2.92 and solves every one with
  log₁₀ 𝓡 ≥ 3.00. The histories that cross the gap are β = 1.2 at M = 10⁻¹⁶, 10⁻¹⁷, 10⁻²⁰; β = 2 at
  10⁻¹⁵–10⁻²⁰; β = 3 at 10⁻¹⁵–10⁻¹⁸. At M = 10⁻¹⁶ the generations in the gap carry 0.88, 0.59 and 0.52
  of K (β = 1.2, 2, 3).
- **Closed (2026-10-31, prompt 08, `86fc526`).** `peak_estimate` takes the one-loop estimate where
  ln 𝓡 > 0 and `one_loop_turnaround` returns one, and the classical estimate elsewhere, flagged
  `"classical_gap"` where the chain raises or returns `None` (R19). Each generation records its
  estimate. On the 51 histories of log 08 §5.1, every gap generation's reason is the saddle-node ("ell
  fell to …"); no raise lies at log₁₀ 𝓡 ≥ 3.00, and no solution has ℓ ≤ 0 (the smallest is 0.341, at
  log₁₀ 𝓡 = 2.996). At M = 10⁻¹⁶, β = 2, the three gap generations carry 0.5877 of K and the one-loop
  one 0.2812, as log 07b §5.3 found (log 08 §5.2).

### `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`

**Opened:** 2026-10-29 by prompt 05, found by its measurement down to laboratory M (log 05 §5.5).

- **What.** 04's generic `classical_turning_point` forms V − V_floor from ln V and ln V_floor. Where
  M/φ is below a few ulps of ln V, ln V − ln V_floor is quantized: on the exponential at
  ln V ≈ −276.6 it is exactly 5.68×10⁻¹⁴ (one ulp) at both φ = 3.43×10⁻¹⁷ M_P and φ/e (M = 10⁻³⁰ M_P).
  `_search_log_field` then sees the same finite residual at two probes and raises "the searched
  quantity is not monotone". Further out, where the difference is 0.0, the residual is −∞, which the
  search allows. The method is right; its input has no resolution there.
- **Impact.** At α > 0 the step loop calls it at every infalling step start, and a raise fails the
  history. All three M = 10⁻³⁰ histories of log 05 §5.5 (β = 1.2, 2, 3) failed this way. Each did so
  at the bottom of its cascade, once |π| ≈ 4×10⁻⁷ M_P put the floor reflection at φ ≈ 3×10⁻¹⁷ M_P,
  0.25–5.6 e-folds after ρ_kin/ρ_R had fallen below 10⁻⁶. So 06's cut comes first on every history
  measured. 08's per-generation calls would meet the same at any state with M/φ ≈ 10⁻¹⁴–10⁻¹³.
- **Next step.** For 06 to confirm that the cut always comes first, and for the methods' owner: treat
  a residual within the rounding of ln V as −∞, or give the search a tolerance on equal values, or a
  closed-form override in `ExponentialModel`. A change to 04's methods is not 05's (its §5). Not
  assigned.
- **Narrowed (2026-10-29, prompt 06).** With parking the cut comes first on every history measured:
  log 05 §5.5's 18 (β = 1.2, 2, 3; M = 10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰ M_P) park at candidates 6,
  48 and 58 and reach T_CMB, as do β = 2 and 3 at M = 10⁻³; β = 1.2 there parks too, and β = 0.5
  never empties its cascade (log 06 §5.4, §5.3). The model's search never raised on any of the 22, and the parked phase calls only `minimum_on_trace`, never the wall search. The
  defect stays: 08's per-generation calls, or a history that does not park, can still meet it.
- **Measured (2026-10-30, the orchestrator's probes for 08, `source/planning-probes-2026-10-30/` §1).**
  The impact is wider than recorded. Every dissipation history fails, at its first infall or soon after,
  at every M tried from 10⁻²⁵ to 10⁻¹³ M_P (β = 2 at eight M; β = 1.2 and 3 at two). In each, the
  infall's search meets M/φ ≈ 3×10⁻¹⁴ on its way in. β = 2 runs at 3×10⁻¹³ and 10⁻²⁸. Log 05's and
  log 06's grids lay outside the band. Treating a difference within 64 ulps of ln V as −∞ makes all twelve
  reach T_CMB, and leaves four histories outside the band bitwise.
- **Assigned (2026-10-30)** to this campaign's prompt 07b, inserted before 08 at the user's word (README
  §8.1 R18), which closes it.
- **Closed (2026-10-30, prompt 07b, `92f41f2`).** `_log_V_minus_floor` returns −∞ where
  ln V − ln V_floor ≤ 16 ulp(max(|ln V|, |ln V_floor|)) (`WALL_ROUNDING_QUANTA`), the width derived
  from the search's closest comparison of two finite values (log 07b §1). All 30 dissipation histories
  of β = 1.2, 2, 3 × M = 10⁻¹³–10⁻²⁷ M_P reach T_CMB, 27 of which failed before. Every history outside
  the band, and α = 0, is bitwise the dispatch commit's (log 07b §5). The width gives up roots with
  E ≲ 10⁻¹² V_floor, which no measured history comes near.

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
- **Ruled the same day:** U9 "agree" and U11 "agree"; then U10 at its full size, about 52 tuned
  histories and 1–3 hours ("U10 is fine. 1-3 hours is fine.").
- **Prompt 04 written** the same day, at the user's word ("can we now write prompt 04?"), from log 03
  §8 and README §8.4. It departs from the charter in three places, each stated in its §0.2:
  - dφ_min/dN along the trajectory moves to 06, and 04 gives ∂φ_min/∂ ln T;
  - λ is the notes' −d ln V/dφ;
  - the one-loop turnaround is `sec:chain`'s, whose worked example gives y = 39.1. The gate's
    "y ≈ 40" is §`sec:stop`'s κ condition.
- **Assigned:** `[00-settling-at-physical-M-needs-a-parked-tracking-model]` from the
  `integrator-remediation` board (§3.1 above), with `.documents/OPEN_ISSUES.md` §1.20.
- **No run registered**: `runregistrykit list` showed 11 finished runs and nothing running.

### 5.5 After prompt 04 (2026-10-29)

04 landed in `81b933f`, under orchestrator note 04 (rulings 1–12). Log:
[`logs/04-the-wall-through-the-model.md`](logs/04-the-wall-through-the-model.md).
- **What shipped** (`CosmologyConcepts/Models/AbstractModel.py`): four generic methods through the
  hooks, mirrored on `RealizedModel`. `ExponentialModel` overrides none.
  - `classical_turning_point` returns `ClassicalTurningPoint(phi_t, log_omega_t_squared, log_R,
    lambda_t, log_k_ex, ell_t)`, or `None` inside φ_t.
  - `one_loop_turnaround` returns `OneLoopTurnaround(phi_ta, log_m_ta, lambda_ta, log_k_ex, ell,
    iterations, outside_classical)`, or `None` inside φ_ta.
  - `minimum_on_trace` returns the root of V′ + (ln A)′T = 0.
  - `d_minimum_d_log_trace` is ∂φ_min/∂ ln T at fixed T, with no (ln A)′² term.

  The search doubles in ln φ, checks monotonicity on values (ruling 3), halves 4 times, then uses
  brentq with rtol 4ε and xtol 10⁻¹⁴ (ruling 2). The exception is imported inside the module
  (ruling 1). The integrator is untouched.
- **Gate Wall holds.**
  - Item 1: φ_t, ln 𝓡 and k_ex within 7.4e-15, 3.3e-12 and 1.4e-14 of the closed form at 12 points.
  - The worked example: classical y_t = 105.307; one-loop y = 39.080, Λ = 53.792,
    k_ex = 5.625×10¹³ GeV, ℓ = 32.3956 in 6 iterations, against the notes' 105, 39.1, 53.8 and
    5.6×10¹³.
  - m_ta² is 0.2300 GeV² at the chain's ℓ, and 0.2314 GeV² at ℓ = 32, where the notes print 0.25.
  - The n = 2 exponential and a power-law wall meet every defining equation within derived rounding
    bounds (largest ratio 0.49, on m_ta⁴ℓ).
  - The minimum equals `effective_minimum` today to ≤ 1.9e-14, and ∂φ_min/∂ ln T equals a centred
    difference to ≤ 4.2e-8.
- **Measured** (log 04 §5.2): **the one-loop chain stops above 𝓡 = 1**, at 𝓡 ≈ 10^2.9, 0.73 decades of
  M below the point where 𝓡 = 1, at both E. So prompt §2.2's jump at 𝓡 = 1 cannot be measured. At
  the chain's edge, Δ ln k_ex = −0.039.
- **Costs per call:**
  - classical: 21–24 µs (about 12 % of 03's per-step cost at α > 0);
  - one-loop: 167–482 µs;
  - minimum: 14–18 µs;
  - derivative: 4.5 µs.
- **Breakages:** B1–B9, each failing named tests.
  - B7 (prompt §1.5's tolerance) fails item 1 at (10⁻¹⁰, −180), not at (10⁻¹⁶, −120).
  - B9 (a top-level import) fails ruling 1's import test.
- **Counts:** 26 / 317 (96 + 221) / 103 / 115 / 64 before; 26 / **339** (96 + **243**) / 103 / 115 /
  64 after, all passing.
- **Issues:** opened `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (§3), with
  `.documents/OPEN_ISSUES.md` §1.19; closed none.

*Orchestrator review of prompt 04 (2026-10-29).* The note's nine checks at `81b933f`, run serially.
The load was 7–20 (other workloads share the machine):
1. **Scope.**
   - The commit's 6 files are the prompt's §6 list plus `.documents/OPEN_ISSUES.md`, for the issue
     opened. Its parent is `a8e0406`, the orchestration note.
   - The tree is clean, `git worktree list` shows the main checkout alone, and nothing is pushed.
   - Outside the prompt's files, the only change under `ComputeTargets`, `CosmologyModels`,
     `ExponentialModel.py`, `Models/__init__.py`, `ObjectFactories`, `config`, `main.py`, `tools`,
     `RunDrivers` and `plot_inputs.py` is the new `test_wall_model.py`.
2. **The import.** `import CosmologyConcepts.Models` succeeds in a fresh interpreter, both alone and
   after `import ComputeTargets`. `AbstractModel.py` has no module-top import of `ComputeTargets`;
   the exception is imported inside `_failure` (ruling 1).
3. **The methods**, read line by line against the prompt and rulings 2–7:
   - every quantity is in logs;
   - one search (`_search_log_field`) serves all three roots. It doubles from the start, checks
     monotonicity on values at every probe and halving midpoint, and ends with a chained
     `ComputationFailureError` when a probe leaves the normal float range or a hook raises;
   - brentq uses rtol 4ε and xtol 10⁻¹⁴;
   - the `None` cases are prompt §1.1's and ruling 4's;
   - the one-loop iteration raises on ℓ ≤ 0 or after 100 iterations, with ℓ's history in the
     message;
   - the minimum's sign logic searches outwards when V′ + (ln A)′T < 0, and inwards otherwise;
   - the derivative is formed as −1/(V″/((ln A)′T) + (ln A)″/(ln A)′), with the ratio in logs;
   - the docstrings set fixed T against `log_m_eff_squared`'s fixed ρ̃_c.

   D3 (`outside_classical` by one evaluation) equals φ_ta > φ_t wherever the classical search's
   monotonicity holds, and is right where the classical point is behind the field.
4. **An independent probe** (session scratchpad, not committed): bisection to the last halving in
   ln φ through the hooks, with no brentq and no code of the agent's.
   - Item 1's 12 points: φ_t, ln 𝓡 and ln k_ex agree with the methods to 7.2e-15, 3.1e-12 and 2.1e-14,
     and φ_t with the closed form to 7.3e-15.
   - The worked example: y_t = 105.307438, y = 39.079956, ℓ = 32.39563882539746 (the reference
     differs in the 15th digit), 6 iterations each, k_ex = 5.6247×10¹³ GeV, m_ta² = 0.22999 GeV²,
     `outside_classical` True.
   - n = 2 and the power-law wall at M = 10⁻³⁰ and 10⁻²⁰, ln E = −180 and −120: φ_t, ln 𝓡, φ_ta and ℓ
     agree to ≤ 1.4e-14, 5.9e-12, 1.4e-14 and 4.3e-14.
   - The minimum on the exponential and the power-law wall (with γ), at M = 10⁻³⁰ and 0.5, at
     ln T = ln ρ_m0 and −200: φ_min agrees to ≤ 2.5e-14; ∂φ_min/∂ ln T agrees with the reference's
     closed expression to ≤ 1.8e-13, and with a centred difference (h = 10⁻³) to ≤ 4.2e-8.
5. **The breakages.** B1–B9 were replayed from the log's diffs in `bash`, running
   `test_wall_model` under each. Each failed exactly the tests the log names, and the two files'
   md5s were restored afterwards:
   - B1: 9 errors;
   - B2: 4 failures;
   - B3: 1 failure and 3 errors (test_3 classical on n = 2);
   - B4: the derivative test, on the power-law wall;
   - B5: test_1 and test_3 classical;
   - B6: test_2's chain, test_3 one-loop and test_5's no-solution case;
   - B7: test_1;
   - B8: the bump test;
   - B9: the import-alone test.

   B7 fails at (10⁻¹⁰, −180), not at the note's (10⁻¹⁶, −120): the agent's search halves its bracket
   before brentq, so the bracket brentq starts from differs from the probe's. Ruling 2 allowed for
   this ("or name the point").
6. **The numerics of the tests.**
   - Item 1's tolerances are the prompt's, with the conditioning stated.
   - Item 3's bounds are derived in the test's docstring and log §1.5. The largest measured ratio is
     0.49, on m_ta⁴ℓ.
   - The worked example is evaluated by the test's own loop (`_the_notes_chain`), never by the
     methods (ruling 10).
   - Item 4's step h = 10⁻³ has a truncation of about 2e-8 and a rounding of about 5e-12, against
     its 10⁻⁶.
   - D7's start-independence bound was corrected from 2τ to 2(τ + R/S) after one failure, before
     any suite run. The missing term is the residual's rounding, which a root's position does carry,
     so this is a correction of the derivation, not a loosening. It is not one of item 3's bounds.
7. **The measurement.** I recomputed the crossover from the reference alone, at the worked example's
   E and at E × 10⁻⁴:
   - 𝓡 = 1 at M = 10^−15.378 and 10^−16.457 M_P;
   - the one-loop chain's edge at 10^−16.110 and 10^−17.191, where 𝓡 = 10^2.93;
   - at the edge ℓ ≈ 0.27, Δ ln k_ex = −0.037 and −0.041, and ln m_ta − ln ω_t ≈ −1.01.

   This agrees with log 04 §5.2 (10^−16.109 and 10^−17.190, −0.039 and −0.043, −1.05). The small
   differences are how close to the saddle-node each search gets. The gap 1 < 𝓡 ≲ 10^2.9 is real.
8. **The records.**
   - The log is in §5.1's form, with each method's derivation by equation label and the bounds.
     Rulings 1–12 are classed, along with D1–D12; there is no UNINTENDED DRIFT.
   - The `architecture-summary.md` note is dated and additive.
   - The board's §3 entry and `.documents/OPEN_ISSUES.md` §1.19 agree, and the count went 25 → 26.
9. **The suites** at `81b933f`, serially: CosmologyModels 26 (70 s), ComputeTargets 96 (124 s) +
   243 (292 s), ObjectFactories 103 (16 s), tools 115 (130 s), RunDrivers 64 (25 s), all OK.
   - ComputeTargets rose by the 22 new tests only.
   - The golden tests pass unchanged.
   - `black --check` is clean on the two changed `.py` files.

Residue fixed in this review's commit: "this commit" → `81b933f` in the log and on this board. Nothing
else was found. **The gate Wall holds.**

The issue `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` stays open and unassigned. It
does not touch 05, which needs only φ_t and 𝓡, and those exist everywhere. Before 08 is written,
the user rules how R7's choice of the peak estimate treats the band 1 < 𝓡 ≲ 10^2.9.

**Prompt 05 written** the same day, at the user's word ("Please write prompt 05"), from log 04 §8 and
README §8.4's charter for 05. It departs from the charter in these places, each stated in its §0.2:
- E is the energy that reaches the wall, ρ_kin + (V − V_floor), not H_E²π²/2 alone. They differ by
  about (V₀/E)^{2/3} at 3φ_t on the exponential, but not on a gentler wall;
- the reflection is made at a step start, so in [2.7 φ_t, 3 φ_t);
- the P1 harness at M = 10⁻¹⁰ keeps a floor-rule reflection (the floor fires outside 3 φ_t). P20
  changes its weight to 1, which is the change to `test_injection` (8) and `test_gate_and_taper` (8);
- natural mode evaluates φ_t at most once, for its warning, and never fails a history on it (P22
  against P23);
- the natural-mode warning says "may fail": M = 10⁻⁷, U10's reference, lies below 10⁻⁸ M_P and
  resolves;
- the warnings are returned by the loop and printed once per history by `tune_history`;
- an unresolved reflection is a `Reflection`, with a kind, so that `first_bounce` and the counts see
  it.

### 5.6 After prompt 05 (2026-10-29)

05 landed in `9c2483b`, under orchestrator note 05 (rulings 1–13). Log:
[`logs/05-unresolved-turnarounds.md`](logs/05-unresolved-turnarounds.md).
- **What shipped** (`ComputeTargets/ScalarHistory.py`):
  - the constants `UNRESOLVED_REFLECTION_FACTOR = 3.0` and `UNRESOLVED_PHI_T_THRESHOLD_MP = 1.0e-8`;
  - `assess_infall(model, φ, ln ρ_kin,E)` → (ln E, `ClassicalTurningPoint` or `None`), with
    E = ρ_kin,E + V − V_floor;
  - `unresolved_turnaround(φ, turning point, M_P)`;
  - the four warning phrases, with `warning_kind`.

  At α > 0, on a model with a wall, every step start with π < 0 is assessed before the floor rule,
  and φ < 3φ_t with φ_t < 10⁻⁸ M_P or ln 𝓡 > 0 is reflected there through the floor branch, as kind
  `"unresolved"`. Every reflection drains with w_gate = 1.0 (R10, P20). The records:
  - `Reflection.kind`;
  - `Injection.resolved`, `phi_assessed`, `log_E_assessed` and `classical_turning_point`;
  - `IntegrationResult.warnings`, and the payload's `"warnings"`, kept through a failure (ruling 2);
  - `tune_history` prints each kind once per history (ruling 1).

  α = 0 is the old code, but for one model call at natural mode's first floor reflection.
- **Gate Unresolved holds.**
  - α = 0 is bitwise: the golden tests pass unedited; the model is called 0 times on a golden
    history and once on the P1 harness, whose run equals `kcl.integrate`'s.
  - At M = 10⁻⁸ the first wall approach is reflected at 2.959 φ_t, with w_gate = 1 and α_eff = α.
  - In the band (β = 2) the rule agrees with the resolved run to 7.5e-7 at M = 10⁻⁹, 5.6e-9 at
    3×10⁻¹⁰ and 1.4e-6 at 10⁻¹⁰, with equal candidates. At 3×10⁻¹¹ the resolved run loses 10⁹ of
    the kinetic energy at a resolved wall with φ_t = 5×10⁻¹³ M_P, as the planner's "R only" did. The
    agent did not take this as prompt §5's stop (log 05 D14).
  - The warnings print once per history.
- **α > 0 at M = 0.5 is 03's**, bitwise, at β = 1.2, 2, 5, 25. Every candidate is resolved and
  assessed, and each history warns "nearly elastic" (log₁₀ 𝓡 = −62.5 to −53.4).
- **Measured** (log 05 §5):
  - under P20, K is invisible in Σ (K = 10 against 3: 1.6e-8);
  - the drain to the cut is 0.1153, 0.2902 and 0.4219 of the ceiling at β = 1.2, 2, 3, the same at
    every M from 10⁻⁷ to 10⁻³⁰, against README §8.0's gated 0.115, 0.218 and 0.261;
  - without parking every small-M history fails 0.25–5.6 e-folds after the cut: on G2 at an
    unresolved reflection where E ≈ V − V_floor, or (M = 10⁻³⁰) in the model's search;
  - the smallest φ/φ_t at an unresolved reflection before the cut is 2.70008.
- **Costs**: about 30 µs per assessment (H_E 11 µs, `assess_infall` 16–22 µs); +4.6 % on the
  M = 0.5, β = 2 history.
- **Breakages:** B1–B11, each failing named tests. B1 (K = 10) fails item 3, as ruling 3 asks.
- **Counts:** 26 / 339 (96 + 243) / 103 / 115 / 64 before; 26 / **374** (96 + **278**) / 103 / 115 /
  64 after, all passing.
- **Issues:** opened `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` (§3), with
  `.documents/OPEN_ISSUES.md` §1.19; closed none.

*Orchestrator review of prompt 05 (2026-10-29).* The note's nine checks at `9c2483b`, run serially.
The load was 4–120 (other workloads share the machine):
1. **Scope.**
   - The commit's 9 files are the prompt's §6 list plus `.documents/OPEN_ISSUES.md`, for the issue
     opened. Its parent is `cd19768`, the orchestration note.
   - The edits to `test_injection.py` and `test_gate_and_taper.py` are item (8) only: w_gate == 1.0,
     α_eff == α, resolved False, kind "floor".
   - The files ruling 12 protects are byte-identical. The tree is clean, `git worktree list` shows the
     main checkout alone, nothing is pushed, and no Ray process is running.
2. **α = 0.** The golden tests, `test_first_bounce`, `test_surfing_turning_points` and
   `test_kinematic_cap_loop` pass unedited.
3. **The loop**, read against prompt §1 and rulings 1–9:
   - the assessment comes before the floor rule, at α > 0 on a model with a wall only;
   - `unresolved_turnaround` is strict in all three comparisons and reads K and the threshold at call
     time;
   - every reflection drains with w_gate = 1.0, and `gate_weight` is not called for it;
   - a `None` assessment leaves the infall's latest standing (ruling 7);
   - natural mode's one model call is made at the first floor reflection and catches only
     `ComputationFailureError` (ruling 8);
   - the loop's failures carry `e.warnings`, and `integrate_history`'s failure payload carries them
     (ruling 2).
4. **An independent probe** (session scratchpad, not committed), against the code before 05 where
   needed:
   - at M = 10⁻⁸ the first reflection is unresolved, at N = 20.352100 and φ/φ_t = 2.95900, with
     w_gate = 1 and α_eff = 0.1;
   - at M = 10⁻⁹, to N = 30.3, the rule gives Σ = 0.012615740892 and the resolved run 0.012615731446
     (7.49e-7), with 46 candidates each; K = 10 differs from K = 3 by 1.3e-8;
   - at M = 0.5, β = 2, α = 0.1, the turning points, every pre-05 field of every injection, the final
     state, N_final, nfev, the steps and the samples are bitwise `cd19768`'s, in a temporary worktree
     that was removed afterwards;
   - **D14**: on `cd19768`'s code, at M = 3×10⁻¹¹ the resolved run (threshold 0) reaches the cut at
     N = 30.0096 after 34 floor reflections, with Σ = 0.012611199925 to 10 digits. The rule gives 48
     reflections and Σ = 0.0126159746, which matches the other masses to 3×10⁻¹⁰. The different
     count is the resolved run's own float-spacing failure, which R8 removes.
5. **The breakages.** B1–B11 were replayed from the log's diffs in a separate worktree, removed
   afterwards. Each failed the tests the log names, and each reverted cleanly. B1 (K = 10) fails
   item 3 and the constants' and decision's tests, as ruling 3 asks. B6 fails item 3's setUpClass and
   item 5's count. The clean module ran 35 tests OK in 34 s.
6. **The numerics of the tests.** Each tolerance is derived in its docstring and log §1. Item 3's lower
   bound on φ/φ_t has the slack of ruling 4, and item 5's tolerance is set from the measured 7.5e-7.
7. **The measurement.** The tables of log §5 are complete. §5.4 sets the planner's values beside 05's,
   and says which differences come from the planner's kinetic E, gated floor reflections and parking.
8. **The records.** The log is in §5.1's form, with the energy argument, D1–D14 classed and no
   UNINTENDED DRIFT. The documents' notes are dated and additive. The board's §3 entry and
   `.documents/OPEN_ISSUES.md` agree.
9. **The suites** at `9c2483b`, serially: CosmologyModels 26 (80 s), ComputeTargets 96 (136 s) + 278
   (350 s), ObjectFactories 103 (25 s), tools 115 (142 s), RunDrivers 64 (26 s), all OK.
   ComputeTargets rose by the 35 new tests only. `black --check` is clean on the 4 changed `.py` files.

**D14, ruled by the user.** Prompt §5's band stop ("Σ Δρ/ρ_R by more than 1 %, or a different number
of wall approaches") is met literally at M = 3×10⁻¹¹, where the resolved comparison run itself fails.
The agent did not take it as a stop. The user ruled: "Yes, accept D14."

Residue fixed in this review's commit: "this commit" → `9c2483b` in the log and on this board. Nothing
else was found. **The gate Unresolved holds.**

The issue `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` stays open and unassigned.
The cut comes first on every history measured, which 06 confirms (its §4.5).

**Prompt 06 written** the same day, at the user's word ("Please can you write prompt 06?"), from log
05 §8, README §8.4's charter for 06 and the orchestrator's probes at `9c2483b`
([`source/planning-probes-2026-10-29c/`](source/planning-probes-2026-10-29c/README.md)). The orchestrator's
review of 05 was committed after it, once the user had ruled on D14; 06's gate requires it.
What the probes found, and the prompt's departures from the charter (its §0.2, §0.3):
- the field never reaches φ_min: every reflection is made outside it (1.7–2.5 φ_min under the
  unresolved rule, up to 10¹⁸ φ_min under the floor rule). The switch moves ln A by at most 7×10⁻⁹,
  against the 2×10⁻⁵–7×10⁻³ that ln A spans over the oscillation it replaces. That comparison is the
  switch's measure, not φ;
- π_min = dφ_min/dN is solved algebraically: d ln T/dN is linear in π;
- at β = 1.2 the cut oscillation is wide (to 6×10⁻³ M_P at ρ_kin/ρ_R = 5×10⁻⁷), and the parked ln T_J
  differs from the oscillating one by 1.1×10⁻³; at β = 2 and 3 by at most 1.7×10⁻⁵. 06 measures it at
  ε = 10⁻⁴, 10⁻⁶ and 10⁻⁸;
- **the cut fires at M = 0.5 M_P** at β = 5, 10 and 25 (at 81 MeV, 358 MeV and 10 GeV, m/H = 78–267),
  at a resolved drain point, as R14 is ruled; at β = 1.2 and 2 it does not (m/H = 1.9, 3.1). At β = 5
  the tracker falls below m/H = 10 at 9.65 MeV, so in 06 that history fails there and 07 releases it.
  Two existing tests (`test_injection` (11), β = 10; `test_gate_and_taper` (9), β = 5) therefore run
  with the cut disabled (ε = 0). At such a drain point the gate, not w_gate = 1, drained the cascade, so
  P19's reason holds only in its bound, ε ρ_R. The user ruled the same day that the cut applies at
  every M, and that the behaviour is documented (README §8.1 R17); 06's §0.2 and §4 item 4 now say so.

### 5.7 After prompt 06 (2026-10-29)

06 landed in `e967cb7`, under orchestrator note 06 (rulings 1–17). Log:
[`logs/06-parking.md`](logs/06-parking.md).
- **What shipped** (`ComputeTargets/ScalarHistory.py`):
  - the constants `PARKING_EPSILON = 1e-6`, `PARKING_ADIABATICITY_THRESHOLD = 10.0` and
    `PARKING_PI_GUARD_MP = 1e-9`, none a key;
  - the decisions `parking_ratio_test(index, ratio)`, `tracker_adiabatic(m/H)` and
    `parking_guard(π_in, M_P)`;
  - the tracker: `log_trace`, `tracker_velocity_terms` / `tracker_velocity` (π_min = s a/(1 − s b),
    solved, never differenced), `parked_tracker`, `tracker_m_eff_over_H`, `cut_tracker`,
    `parking_leftover`; the classes `ParkedRHS` and `ParkedSegment`;
  - the record `Parking` on `IntegrationResult.parking`, and the payload's `"parking"`, not stored.

  At α > 0 on a model with a wall, after every candidate after the first: the ratio test, the tracker,
  and where both hold the switch (the leftover into the ledger at the same N) and a parked phase
  integrated by DOP853 to the history's end, its steps `ParkedSegment`s of the dense output, which
  return the full state and depend on N alone. A parked step end with m/H < 10 fails the history. At a
  reflection where the cut did not fire, |π_in| < 10⁻⁹ M_P fails it. Nothing runs at α = 0.
- **Gate Park holds.**
  - α = 0 is bitwise: the golden tests, `test_first_bounce`, `test_surfing_turning_points`,
    `test_kinematic_cap_loop` and `test_unresolved_turnarounds` pass unedited; `minimum_on_trace` is
    called 0 times at α = 0, also with two floor reflections and the cut opened wide.
  - M = 10⁻³⁰, β = 2 reaches T_CMB, parking at candidate 48 (N = 30.3493), bitwise the cut-disabled
    history before it.
  - The parked samples lie on φ_min to the root's tolerance; π on them is π_min bitwise.
  - H_E at the switch: bitwise at M = 10⁻³⁰, 2.2×10⁻¹² at 10⁻⁷, within the bound from V(φ_min) − V(φ)
    and the tracker's kinetic energy; the leftover is the ledger's second entry at the switch.
  - The guard fails a history with its reason (threshold raised by a patch: G2 or the wall search
    comes first on every real history).
- **α > 0 at M = 0.5 is 05's**: bitwise with the cut disabled at β = 1.2, 2, 5, 25; with it on at
  β = 1.2 and 2 (no parking); up to the switch at β = 5 and 25, which park at drain points (81.2 MeV,
  9.97 GeV) and fail where the tracker loses adiabaticity (9.40 MeV, 39.8 keV). Natural mode bitwise.
- **Measured** (log 06 §5):
  - all 18 histories of log 05 §5.5's grid park at the candidates and N log 05 named and reach T_CMB,
    in 1.6–8 s; none of log 05's bottom-of-cascade failures occurs;
  - Σ + leftover is 0.11531, 0.29019 and 0.42197 of the ceiling at β = 1.2, 2, 3, the same at every M
    to ≤ 1.3×10⁻⁷; Δ ln A ≤ 6.8×10⁻⁹ against ln A ranges of 2×10⁻⁵–7×10⁻³;
  - the parked ln T_J differs from the oscillating one by 1.1×10⁻³ at β = 1.2 and ≤ 1.7×10⁻⁵ at β = 2
    and 3; at β = 1.2, M = 10⁻⁷, ε = 10⁻⁴ and 10⁻⁶ park at the same candidate, ε = 10⁻⁸ at #44
    (51 eV) with 5×10⁻⁷;
  - at M = 10⁻³ β = 1.2, 2, 3 park at drain points; β = 1.2 fails at 0.25 eV (for 07); β = 0.5 never
    parks;
  - the tuning converges on its seed (one integration) at M = 10⁻³⁰ (β = 1.2, 2, 3) and 10⁻⁷ (β = 2).
- **Costs**: ratio test 0.13 µs; tracker 75–90 µs where the ratio passes; parked right-hand side about
  50 µs; parked step 0.5–0.9 ms; the M = 0.5, β = 2 history unchanged (2.65 s against 2.66 s).
- **Breakages:** B1–B11, each failing named tests.
- **Edits to existing tests:** `test_injection` (11) and `test_gate_and_taper` (9) run with ε = 0.0
  (ruling 14).
- **Counts:** 26 / 374 (96 + 278) / 103 / 115 / 64 before; 26 / **408** (96 + **312**) / 103 / 115 /
  64 after, all passing.
- **Issues:** opened none; narrowed `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`
  (§3), with `.documents/OPEN_ISSUES.md` §1.19.

*Orchestrator review of prompt 06 (2026-10-29).* The note's ten checks at `e967cb7`, run serially, at
a load of 4–11:
1. **Scope.**
   - The commit's 9 files are the prompt's §6 list plus `.documents/OPEN_ISSUES.md`, for the
     narrowing. Its parent is `7094056`; the tree is clean and nothing is pushed.
   - Ruling 16's files are byte-identical to `7094056`: `CosmologyConcepts/Models/`, `Policies/`,
     `dark_radiation.py`, `exceptions.py`, `AdiabaticHistory.py`, the golden files, the five named
     tests, `tools/`, `RunDrivers/`, `CosmologyModels/` and `ObjectFactories/`.
   - `test_injection` (11) and `test_gate_and_taper` (9) each change one `setUpClass`, wrapped in
     `PARKING_EPSILON = 0.0`, and one docstring sentence.
2. **α = 0.** The cut and the guard run only under `assess_wall` (α > 0, and a model with a wall and a
   floor). Nothing new runs at α = 0, and the tests 06 must not edit pass unedited (check 10).
3. **The loop**, read line by line against prompt §1 and rulings 1–11:
   - At a reflection, `cut` follows `candidate()`, also when α_eff = 0 (elastic restart state). At a
     drain point it reads the drained state, or `solver.y` if nothing drained.
   - The comparisons are strict, and the three constants are read at call time.
   - The leftover is formed in logs from the candidate's `H_Einstein_after` and `pi_after`, and added
     to the ledger at the same N.
   - The parking record holds only floats and ints, and `armed` is cleared at the switch.
   - The parked phase runs inside the loop's `try`, so its failures carry the warnings. Its DOP853
     step has no halving, and every failure names the switch. The step budget counts both phases, and
     `nfev` is counted through `fun`.
   - The adiabaticity check runs at every step end and at N_final.
   - The anchor is fixed per step, and the left node is re-evaluated with it (D4). Array calls are a
     loop of scalar calls.
   - One edge: if the previous node lies exactly on log_T_stop, the final state is read from the new
     segment, with the new anchor. That is consistent with D4 and harmless.
4. **An independent probe** (scratchpad, not committed):
   - **β = 2, M = 10⁻³⁰ to T_CMB.** It parks at #48 (floor), N = 30.349329, with m/H = 4.80×10¹⁴, and
     ends at ln T_J = ln T_CMB exactly.
     - H_E is equal before and after the switch, bitwise. The ledger's last entry is the leftover,
       and my own leftover from the record agrees bitwise.
     - From 10 keV down, ρ_χ/ρ_R is 7.04×10⁻³ and constant.
     - The history is bitwise the ε = 0 history at 3 000 points before the switch.
     - Over the window to 30.9, parked ln T_J differs from oscillating ln T_J by at most 1.71×10⁻⁵,
       and by 4.0×10⁻⁸ at the window's end. That is log §5.5's 1.71×10⁻⁵ | 4.9×10⁻⁸ on another sampling.
   - **The segment determinism.** At 107 N, shuffled scalar calls and one array call give the same
     values bitwise.
   - **φ and π on 300 parked samples.**
     - φ agrees with my own `minimum_on_trace`, started 7 % away, to 4×10⁻¹⁴.
     - π agrees with s·(d ln T/dN) to 8×10⁻⁸. I took d ln T/dN by a five-point difference of my own
       ln T along the dense output, not from the a, b formula.
   - **M = 0.5, β = 5.** It parks at #105, N = 38.004408 (81.2 MeV, m/H = 122.7). It fails at
     N = 40.29089 (9.396 MeV, m/H = 8.94), and the reason names the switch.
   - **M = 0.5, β = 2.** Full history, with the cut on and at ε = 0, bitwise the dispatch commit's
     over records, samples, nfev and steps.
   - **M = 0.5, β = 5 at ε = 0.** Window to N = 39.0, bitwise the dispatch commit's. Both comparisons
     ran in a temporary worktree of `7094056`, since removed.
   - All of this agrees with the log and with the planning probes (`probe_large_M.out`).
5. **The breakages.** I replayed B1, B4, B5, B6, B7, B10 and B11 with `git apply`, against
   `test_parking`, from the agent's diffs. I read those diffs: B5 adds (ln A)′ to the denominator
   divided by (ln A)′ T, which is the mass's (ln A)′² term. Each fails exactly the tests the log
   names, and the checkout was restored by md5 each time.
6. **The numerics of the tests.**
   - **H_E bound.** I re-derived it. The ledger gains the candidate's kinetic energy, except for
     (1 − α_eff) π_in² (H_after² − H_before²)/2. B3, which drops the leftover from the ledger, moves H
     by about half the leftover fraction, 10³ times the test's second bound.
   - **Item 3's finite difference.** Its tolerance is |D(2h) − D(h)| plus the roots' error. It rests
     on a measured h³–h⁴ convergence, and it is required to lie below B5's change/10 at M = 0.5.
   - **My note's estimate of B5 was wrong.** Ruling 5's estimate, βφ_min/(2 M_P), assumed φ ≫ M,
     where V″/|V′| ≈ 2/φ. At M = 0.5 the field sits at φ ≈ 6×10⁻³ M_P ≪ M, where V″/|V′| ≈ M/φ², so
     B5's change is |(ln A)′ s| ≈ 1–6×10⁻⁴. The agent's correction (log §1.2) is right.
   - The root tolerance is brentq's bound, doubled. The β = 5 failure window is derived from the two
     crossings plus one step.
7. **The measurement.**
   - §5.4 has all 18 histories at T_CMB, parked at log 05's candidates, with ruling 12's W/E column.
   - §5.5 has the parked against the oscillating histories and the three ε.
   - §5.6 gives each tuning's integrations and residuals.
   - §5.2 says what is bitwise and where each history parks or fails.
8. **The derivation.** Re-derived from `ODERHS`:
   - d ln T/dN = −4 + c g, with g = 1 + (ln A)′π;
   - c = Σ + (3w′/D + f_m(1 − Σ))/(Σ + f_m), and D = 1 + G_s′/(3G_s);
   - hence a = c − 4, b = c(ln A)′(φ_min), and π_min = s a/(1 − s b), as coded in
     `tracker_velocity_terms`.
9. **The records.**
   - The log is in §5.1's form, and every deviation is classed (D1–D9 and rulings 1–17; no
     UNINTENDED DRIFT).
   - The `.documents/` notes are dated and additive. The only lines removed are the issue row's last
     line, which is extended in place.
   - The board, the narrowing of `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` and
     `.documents/OPEN_ISSUES.md` agree.
10. **The suites.** 26 / 408 (96 + 312) / 103 / 115 / 64, all OK. These are the log's counts, and
    ComputeTargets rose by the 34 new tests only. `black --check` is clean on the 4 changed `.py`
    files.

Residue fixed in this review's commit: "this commit" → `e967cb7` in the log and on this board, and
this note added to the list of orchestrator notes. Nothing else was found, and nothing is opened.
**The gate Park holds.** Prompt 07 (release) is written next, from log 06 §8.

**Prompt 07 written** the same day, at the user's word ("Please write prompt 07"). It is written from:
- log 06 §8;
- README §8.4's charter for 07;
- the orchestrator's probes at `7b75ae7`
  ([`source/planning-probes-2026-10-29d/`](source/planning-probes-2026-10-29d/README.md)). The probes
  are a string-replaced copy of the loop with a release added.

What the probes found:
- **All four histories that 06 fails on the tracker's loss of adiabaticity reach T_CMB** once released at
  the root of m/H = 10:
  - M = 0.5 at β = 5, 10 and 25, released at 9.75 MeV, 8.70 MeV and 40.3 keV;
  - M = 10⁻³ at β = 1.2, released at 0.27 eV.

  Each takes fewer steps than its never-parked history, and no reflection follows a release.
- **The release is continuous to the bit.** The last parked segment and the first Radau segment return
  the same floats at N_rel.
- **P24 binds at β = 5 and 10.** After the release the field oscillates with ρ_kin/ρ_R ≈ 10⁻⁷ < ε, and
  the tracker's m/H climbs back above 10 within an e-fold. Under the ruling it is not parked again; 07
  measures how often it would have been.
- **The field oscillates after the release.** It is released at zero lag, so it swings about the lagged
  tracker with a half-swing of 23–75 times the first-order lag δ₁. Averaged per oscillation, φ − φ_min
  is 0.99–1.36 δ₁ at β = 5 and 10.
- **The tuning converges on released histories**: in 4 integrations at M = 10⁻³, β = 1.2, and in 21
  (131 s) at M = 0.5, β = 5.

The prompt departs from the charter in four places, each stated in its §0.3:
- the release is at the root on the parked step, not at the step end;
- the tests use M = 0.5, β = 5 and M = 10⁻³, β = 1.2. The charter's M = 10⁻³ case at 30–40 keV was
  β = 0.5, which never parks;
- the lag is compared per oscillation;
- `[00-settling-at-physical-M-needs-a-parked-tracking-model]` is narrowed by 07 and closed by 09. The
  issue's next step names the adiabatic diagnostic, which R15 gives to 09.

One existing test changes: `test_parking`'s β = 5 failure becomes the release.

### 5.8 After prompt 07 (2026-10-30)

07 landed in `5397f8f`, under orchestrator note 07 (rulings 1–14), with ruling 9 amended by the
user after the agent's stop (option (c): "OK, go with (c). I am not sure this matters a lot.").
Log: [`logs/07-unparking.md`](logs/07-unparking.md).
- **What shipped** (`ComputeTargets/ScalarHistory.py`):
  - the release. A parked step end (or N_final) with the tracker's m_eff/H_E below 10 releases the
    field at the root of m_eff/H_E = 10 on that step's `ParkedSegment` (brentq, `RELEASE_ROOT_XTOL`
    10⁻¹⁴, `RELEASE_ROOT_RTOL` 4ε). The root replaces the step's node in `ts`;
  - the hand-back. `_ParkedPhaseEnd` is returned through `cut` to both branches of the loop, which
    start a new Radau instance from the segment's state at N_rel. The drain is disarmed, the infall
    cleared, and the gate's half-oscillation begins at N_rel;
  - P24: no ratio test or tracker after a release. The guard's reason names the release;
  - the record `Release` on `IntegrationResult.release` and the payload's `"release"`, not stored.
    `Parking.min_m_eff_over_H` excludes the failing check.

  06's adiabaticity failure is gone. Nothing runs at α = 0.
- **Gate Release holds**, as ruled:
  - α = 0 bitwise;
  - the 24 non-releasing histories of §2.5 bitwise the dispatch commit's, and the four released ones
    bitwise through a window in the parked phase;
  - each released history reaches T_CMB, released at m/H = 10 to ≤ 2.1×10⁻¹² (derived bound
    2.6×10⁻¹¹ at β = 5);
  - the state, H_E and the ledger bitwise continuous at N_rel;
  - no second parking, although every candidate after the release would pass the cut at β = 5 and
    10;
  - the lag has the sign of δ₁ and lies within the derived bound T1 + T2 on each of eight oscillations
    (ratio 0.98–1.20 at β = 5; 1.05–1.36 at β = 10, measured). δ₁ × −1 is caught; × 2 and × ½ are
    not catchable (bound 1.9–3.7 |δ₁|).
- **The released histories** (log 07 §5.2):
  - M = 0.5, β = 5: N = 40.25454, 9.745 MeV, 15 drains after;
  - β = 10: N = 65.32727, 8.695 MeV, 19 after;
  - β = 25: N = 145.77929, 40.3 keV, 1 after;
  - M = 10⁻³, β = 1.2: N = 45.91020, 0.27 meV, none after.

  Each release lies inside the parked step on which 06 failed, whose check the record names to 06's
  printed digits. The tuning converges on released histories (4 integrations at M = 10⁻³, β = 1.2;
  20 at M = 0.5, β = 5).
- **Costs**: root 0.4–0.7 ms and new Radau 0.2 ms per release. A released history is cheaper than
  the never-parked one: 5.4 against 6.3 s at β = 5, 1.6 against 5.9 s at M = 10⁻³. β = 2 is unchanged
  (2.58 against 2.60 s).
- **Breakages:** B1–B7 and the lag check C1 (δ₁ × −1), each failing named tests.
- **Edits to existing tests:** `test_parking`'s item-7 test at β = 5 asserts the release (prompt §1.6
  item 9), with its class docstring and the module docstring's item (7).
- **Counts:** 26 / 408 (96 + 312) / 103 / 115 / 64 before; 26 / **427** (96 + **331**) / 103 / 115 /
  64 after, all passing.
- **Issues:** opened none; narrowed `[00-settling-at-physical-M-needs-a-parked-tracking-model]`
  (§3.1, its `integrator-remediation` entry, `.documents/OPEN_ISSUES.md` §1.20).
- **Found:**
  - the M = 10⁻³, β = 1.2 release (06's failure) is at 0.27 meV, not 0.27 eV as prompt 07 and log 06
    printed;
  - the anharmonic shift the lag's bound allows does not appear in the data (log 07 §7).

*Orchestrator review of prompt 07 (2026-10-30).* The note's ten checks at `5397f8f`, run serially, at
loads of 5–28:
1. **Scope.**
   - `git show --stat 5397f8f` lists exactly the prompt's §6 files (9), and its parent is `0994a10`, the
     orchestrator's record of the user's ruling.
   - The tree is clean, `git worktree list` shows the main checkout alone, and nothing is pushed.
   - `git diff 35a103c 5397f8f` is empty on ruling 13's files. Under `ComputeTargets/tests/` it touches
     only `test_release.py` (new) and `test_parking.py`. That edit is confined to item 9's test, its
     class docstring and the module docstring's item (7).
2. **α = 0, and what does not release.** The golden tests, `test_first_bounce`,
   `test_surfing_turning_points`, `test_kinematic_cap_loop`, `test_unresolved_turnarounds`,
   `test_injection` and `test_gate_and_taper` pass unedited (check 10). At α = 0 `cut` is never called
   (`assess_wall` is False), so nothing new runs. In a parked phase that stays adiabatic the only new
   code is `check`'s return value and the `released` test of its end; the minimum is folded as before.
3. **The loop**, read line by line against prompt §1 and rulings 1–5, 11 and 12:
   - the root is brentq on [`segment.t_min`, N_check] of m/H on the segment by scalar calls, with g at
     the left node read on the segment. If g ≤ 0 there, the release is at the left node and the segment
     is popped;
   - `ts[-1] = N_rel` and the released state is `segment(N_rel)`, the same float. On the step that ends
     the history the appended `N_end` is replaced, never appended twice;
   - `_ParkedPhaseEnd` is a return value through `cut` to both branches. `resume_after_release` resets
     `armed`, `armed_assessment`, `infall_assessment`, `running_max` and `boundary_N` and builds the new
     Radau. The drain-point branch sets `y_drained = None`; the reflection branch `continue`s before
     `new_solver(N, y_reflected)`;
   - P24 is `release is None and parking_ratio_test(...)`. The guard's reason after a release names
     it;
   - `check` folds only passing values, and the root's value is folded. `Release` holds floats and
     ints only and is the payload's `"release"`; `steps_after` is counted from `accepted_at_release`.
4. **An independent probe** (session scratchpad, my own setup through `integrate_history`):
   - M = 0.5, β = 5 to T_CMB: parks at #105 and is released at N = 40.254538273373846 (9.7454 MeV),
     m/H 10 − 5.2×10⁻¹³. The failing check is at 40.290890061858164 (8.9373). My own bisection of g on
     the segment lands 7×10⁻¹⁵ from N_rel. The parked segment, the Radau segment and the dense output
     at N_rel are bitwise equal, H_E either side is the same float, and K is unchanged across N_rel.
     There are 15 drain points after the release and no reflection. N_final = 62.7518551, 8 295 steps.
     All of this agrees with `probe_release.out` and with the log;
   - M = 10⁻³, β = 1.2: parks at #6, is released at N = 45.91020343 at T_J = 2.66×10⁻¹⁰ MeV
     (0.27 meV, confirming the log's D5), with no candidate after it, and reaches T_CMB;
   - M = 0.5, β = 2 and M = 10⁻³⁰, β = 2 at the dispatch commit `35a103c` (temporary worktree,
     removed) against `5397f8f`: bitwise in every record, the samples, the fixed-T values, the final
     state, `nfev`, the steps and `ts`; the M = 10⁻³⁰ parking record is equal.
5. **The breakages**, replayed with `git apply` in bash in a temporary worktree of `5397f8f`
   (removed), each checked with `--check` and `-R --check` and reverted to a clean tree:
   - B1 fails item 2's threshold and in-step tests;
   - B3 fails item 5's spy test;
   - B4 fails `test_5_the_first_turning_point_and_candidate_after_the_release`, the sentinel test of
     ruling 6;
   - B5 fails item 8's T_final-inside-the-step test;
   - B7 fails the reflection-branch test of ruling 7;
   - C1 (δ₁ × −1) fails item 6.
6. **The numerics of the tests.**
   - The root's tolerance on N is 2(xtol + rtol|N|) plus g's noise over the slope. On m/H it is
     |slope| times that, plus twice the noise, plus 10³ε × 10. The measured residuals (5×10⁻¹³ and
     10⁻¹³) are well inside the derived 2.6×10⁻¹¹ and 4.6×10⁻¹².
   - Item 2's window is 5×10⁻⁶ plus the root's tolerance, as ruling 8 has it.
   - Item 8's N_stop-inside-the-step bound adds the DOP853 interpolant's tolerance through ∂(m/H)/∂y,
     over the slope (D6): 5.9×10⁻⁷ against a measured 2.9×10⁻⁷.
   - The lag test forms T1 = |x(b) − x(a)|/2π and T2 = |F_φφ/F_φ|S²/4 per oscillation from the
     history itself, with no constant read off `lag.out`. It is the sign-and-order test of the user's
     option (c).
7. **The measurement.** §5's tables are complete:
   - all four released histories reach T_CMB, released inside the step on which 06 failed. Their N_check
     and m/H equal the dispatch commit's failure reasons as printed;
   - the would-park counts are 15/15, 19/19, 0/1 and none;
   - released against never parked and the lag agree with `compare.out` and `lag.out`;
   - §5.5 is bitwise for the 24 non-releasing histories and through windows for the four released;
   - the three tunings converge.
8. **The derivation.** The root's tolerance re-derived from `RELEASE_ROOT_XTOL`, `RELEASE_ROOT_RTOL` and
   the slope, as coded in `release_tolerances`. The lag bound re-read against `lag_by_oscillation`:
   T1 at the turning points themselves, T2 from the means of the central differences over the
   oscillation.
9. **The records.**
   - The log is in §5.1's form. Every deviation is classed (D1–D12 and rulings 1–14, ruling 9 as the
     user's), with one UNINTENDED DRIFT (D8, the main checkout edited while a baseline suite ran, with
     no effect on the baselines).
   - The `.documents/` notes and the `integrator-remediation` entry are dated and additive: no line is
     removed.
   - The narrowing of `[00-settling-at-physical-M-needs-a-parked-tracking-model]` agrees on this board,
     on the `integrator-remediation` board and in `.documents/OPEN_ISSUES.md` §1.20. All three give Q on
     the parked samples to 09. The index count stays 27.
10. **The suites** at `5397f8f`, serially: 26 / 427 (96 + 331) / 103 / 115 / 64, all OK. These are the
    log's counts. ComputeTargets rose by the 19 new tests only. The times were 70 / 131 + 442 / 21 /
    134 / 28 s. `black --check` is clean on the 3 changed `.py` files.

Residue fixed in this review's commit: "this commit" → `5397f8f` in the log and on this board, and this
note added to the list of orchestrator notes. Nothing else was found, and nothing is opened. **The gate
Release holds**, with the lag as the user's sign-and-order test. Prompt 08 (the spectrum's peak and the
margins) is written next, from log 07 §8. `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` is
still the user's to rule before 08.

**Prompt 07b inserted and written** (2026-10-30). The user asked for prompt 08 ("Please write prompt
08"). The orchestrator's probes for it ([`source/planning-probes-2026-10-30/`](source/planning-probes-2026-10-30/README.md))
measured the peak and the margins on fourteen histories. They also found every dissipation history from
M = 10⁻²⁵ to 10⁻¹³ M_P failing on `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`.
Asked two questions, the user chose:
- "Insert 07b before 08" (README §8.1 R18): prompt 07b fixes the search's helper, generically, and
  measures the band;
- "Classical, flagged" for the one-loop gap (R19), which 08 implements.

08 is written after 07b has landed and been reviewed (README §2.1), from log 07 §8, log 07b §8 and the
probes' `peaks.out`.

### 5.9 After prompt 07b (2026-10-30)

07b landed in `92f41f2`, under orchestrator note 07b (rulings 1–11). Log:
[`logs/07b-the-wall-search-at-the-rounding-of-ln-V.md`](logs/07b-the-wall-search-at-the-rounding-of-ln-V.md).
- **What shipped** (`CosmologyConcepts/Models/AbstractModel.py`):
  - `_log_V_minus_floor` returns −∞ where ln V − ln V_floor ≤ K ulp(max(|ln V|, |ln V_floor|)), with
    K = `WALL_ROUNDING_QUANTA` = 16. Elsewhere it returns the shipped expression bitwise, and +∞ stays
    +∞;
  - the helper's docstring and `_evaluate`'s phrase say what −∞ now means. Beyond that, only the `ulp`
    import changed.

  The search, its constants, the methods, `assess_infall`, G2 and every model are unchanged.
- **K derived** (log 07b §1). A halving midpoint 1/16 e-fold from a finite end binds:
  (e^{1/16} − 1)(K + ½) > 1, so K > 15.005, for ρ = ½ quantum of rounding and slope s = n = 1. The
  doubling's comparison, where every observed failure was, needs only K ≥ 1. The roots the width gives
  up are D_t ≲ 17 quanta (E ≲ 10⁻¹² V_floor), measured by a synthetic scan; the edge is sharp at
  K ≥ 15 and ragged at K = 0 and 1.
- **Gate Band holds**:
  - α = 0 bitwise, in the band too (M = 10⁻¹⁶, β = 2), warnings included;
  - the 28 histories of prompt §2.4 bitwise the dispatch commit's on 27 keys, warnings included, and 12
    more that ran at both trees;
  - all 30 band histories (β = 1.2, 2, 3 × M = 10⁻¹³–10⁻²⁷) reach T_CMB, 27 of which failed at the
    dispatch commit. They park at #6, #48 and #58 as log 06's do, with Σ + leftover within 3.4×10⁻⁸ of
    M = 10⁻³⁰'s;
  - K derived.
- **The edges** at β = 2, at the dispatch commit: failures from 10⁻¹³·⁰⁰ to 10⁻²⁶·⁰⁰, contiguous on the
  quarter-decade grid. 10⁻¹²·⁷⁵ and 10⁻²⁶·²⁵ run.
- **For 08** (log 07b §5.3): the chain raises on every candidate with 0.05 ≤ log₁₀ 𝓡 ≤ 2.92 and solves
  every one above. The histories that cross the gap are M = 10⁻¹⁵–10⁻²⁰. At M = 10⁻¹⁶ 52–88 % of K is
  in the gap.
- **Ruling 5:** the smallest margin of a root above the width is m_D = 25.18 (m = 25.22), against the
  0.137 condition (3) needs. M = 10⁻¹² and 10⁻³⁰ have 367–2 760 reclassified helper calls and are
  bitwise.
- **Costs:** the helper is 172–188 ns per call against 94–98 ns. A band history takes 5.9 s against
  6.2 s at M = 10⁻³⁰ (β = 2).
- **Breakages:** B1 (16 failures), B2 (2), B3 (1), B4 (16). B2 is caught by item 1 alone; no input can
  catch it at the binding comparison (log 07b §2).
- **Counts:** 26 / 427 (96 + 331) / 103 / 115 / 64 before; 26 / **439** (96 + **343**) / 103 / 115 /
  64 after, all passing.
- **Issues:** closed `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` (§4), with its
  row removed from `.documents/OPEN_ISSUES.md` §1.19; opened none. Recorded the gap's measurement on
  `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (§3).

**Orchestrator review of prompt 07b** (2026-10-30, at `92f41f2`, under note 07b §3). It passes. Each check:
1. **Scope.**
   - `git show --stat 92f41f2` lists exactly the prompt's §6 seven files, and its parent is `c2aac94`.
   - The tree is clean, the main checkout is the only worktree, and nothing is pushed.
   - `git diff c2aac94 92f41f2` on `AbstractModel.py` touches only the `ulp` import, the constant with
     its comment, the helper, and `_evaluate`'s docstring phrase (ruling 3). No code line of
     `_evaluate`, `_search_log_field`, `classical_turning_point` or `one_loop_turnaround` changes.
2. **α = 0 and the frozen tests.**
   - No existing test module is in the diff.
   - The golden tests, `test_wall_model` (the bump test included) and the integrator's tests pass at
     `92f41f2` (check 9).
3. **The helper** matches prompt §1.1:
   - the comparison is ≤;
   - the width is K · ulp of the larger magnitude;
   - it is −∞ where ln V ≤ ln V_floor;
   - it returns the shipped expression unchanged elsewhere.

   D1, the `width < inf` guard, only keeps ln V = +∞ at +∞, as shipped.
4. **The derivation**, re-derived independently. The binding comparison is a halving midpoint
   2⁻⁴ e-folds from a finite end: (K + ½)(e^{1/16} − 1) > 1, so K > 15.0052, and the constant is 16.
   - The doubling needs K ≥ 1 only, which agrees with the orchestrator's K = 1 probe for the note.
   - Condition (3) is a sufficient bound on the root (18.35 quanta), not a tight one. The scan's edge
     and my sweep lie below it.
   - In practice K = 16 does not extend the roots the search can find. It replaces a ragged region of
     failures between 4 and 15 quanta by a sharp edge at 17. Above that edge, a "not monotone" from two
     finite probes can then only mean a non-monotone model. The log says this (§1.3).
5. **An independent probe** (session scratchpad, not committed).
   - **The twelve inputs.** `classical_turning_point` at all twelve `band_shipped.out` inputs agrees
     with the closed form to ≤ 7.1×10⁻¹⁵ in ln φ_t.
   - **A sweep of the root** (D_t = 15–25 quanta, 60 fields per root) raises only for D_t ≤ 16.99q,
     each time with "a midpoint … is not between its ends". The log's scan gives 16.94q failing and
     17.01q succeeding.
   - **One band history.** β = 2, M = 10⁻¹⁶ reaches T_CMB at `92f41f2` and parks at #48 at
     N = 30.349329, with no release. At `c2aac94` it fails with "not monotone". β = 2, M = 10⁻¹⁷ also
     reaches T_CMB.
   - **Bitwise against `c2aac94`**, in a temporary worktree, since removed: β = 2 at M = 10⁻¹²,
     β = 5 at M = 0.5, β = 3 at M = 10⁻³⁰, and natural mode at M = 0.5, β = 2. They are equal by `repr`
     on all 39 keys (the `IntegrationResult` fields, `ts` and the payload, warnings included). The
     only exceptions are `metadata`'s compute times and `z_grid`'s object address.
6. **The breakages.** The log's four diffs are identical to the agent's scratch files. Each was
   replayed with `git apply` in `bash`, in a worktree of `92f41f2`, and reverted. They fail exactly the
   tests the log names:
   - B1: 16 (4 failures and the 12 item-2 subtests as errors);
   - B2: 2;
   - B3: 1;
   - B4: 16.

   `test_wall_model` passed under all four. B2 is caught by item 1 alone because K is derived inside
   the test (D4). The log's argument that no input can catch it at the binding comparison agrees with
   check 4.
7. **The numerics of the tests.**
   - Item 1 builds j quanta with `nextafter` and asserts the difference is j · 2⁻⁴⁴ exactly.
   - Item 2's tolerance is a sum of named terms (6.6×10⁻¹⁴ or more), against measured errors of
     7.1×10⁻¹⁵ or less.
   - Item 3 uses ruling 6's 8.2×10⁻¹⁰ and measures −9.8×10⁻¹².
8. **The measurement and the records.**
   - **§2:** the tables are complete. §2.1 has thirty histories at both trees. §2.2 is per quarter
     decade and contiguous, with the upper edge as predicted. §2.3 is the gap table. §2.4 and §2.5
     are bitwise, warnings included. §2.6 is given, and ruling 5's counts and margins are reported
     (smallest m_D = 25.18 against 0.137).
   - **The log:** it is in §5.1's form, with D1–D9 classed and the note's rulings recorded.
   - **`.documents/`:** the notes are dated and additive only (architecture note, numerical
     strategies §6.7).
   - **The issue:** it is moved to §4 with its Closed line, its index row is deleted, and the count is
     corrected 27 → 26.
9. **The suites** at `92f41f2`, run serially:
   - CosmologyModels 26 OK (79 s);
   - ComputeTargets 96 OK (143 s) + 343 OK (603 s);
   - ObjectFactories 103 OK;
   - tools 115 OK;
   - RunDrivers 64 OK.

   The load averages ranged from 4 to 81, with other work on the machine. ComputeTargets rose by the
   12 new tests only. `black --check` is clean on both changed `.py` files.

Residue fixed in this review's commit: "this commit" → `92f41f2` in the log and on this board, and note
07b added to the list of orchestrator notes. Nothing opened.

### 5.10 After prompt 08 (2026-10-31)

08 landed in `86fc526`, under orchestrator note 08 (rulings 1–16). Log:
[`logs/08-spectrum-peak-and-margins.md`](logs/08-spectrum-peak-and-margins.md).
- **What shipped** (`ComputeTargets/ScalarHistory.py`). After the integration, and under the step loop's
  own condition (α > 0 on a model with a wall), `integrate_history` calls `history_margins`, a gatherer,
  which calls the pure core `relativistic_margins`. The payload gains three keys, none stored:
  - `"generations"`, a tuple of `Generation`;
  - `"margins"`, a `Margins` record;
  - `"margin_warnings"`, the four new kinds' lines (ruling 1).

  `"warnings"` is the loop's, bitwise. `tune_history` prints from both keys through one set of kinds.
  The constants are `MARGIN_THRESHOLD = ln 10` (P6) and `MARGIN_WEIGHT_SHARE = 1e-3` (U9). The step loop,
  `IntegrationResult` and every record the loop builds are unchanged.
- **The definitions.** A generation is a candidate with ΔK > 0 and an assessment, its peak by 𝓡:
  classical (ln 𝓡 ≤ 0), one-loop, or classical flagged `"classical_gap"` where the chain raises (R19).
  ln q = ln k_ex + N_turn. The margins:
  - birth, ℓ_t or ℓ bitwise;
  - reflection, over every later candidate, plateau turning point and wall-side turning point with no
    candidate;
  - tracker, over the samples after N_late and today, with the zero-lag tracker's mass (R20).

  The worst, the failing share and the first failure of each run over the generations carrying at
  least 10⁻³ of K. The leftover and K_0 are unassessed (R21).
- **Gate Margin holds**:
  - the hand computation (`TestAConstructedHistory`) and the weight at its edge;
  - the estimate by 𝓡 at M = 10⁻¹⁶, β = 2: 3 gap generations carrying 0.5877, one one-loop carrying
    0.2812 with ℓ = 1.392;
  - the classical birth margin ℓ_t bitwise, −0.70 to −0.71 (−3.0 at M = 10⁻³, β = 0.5);
  - the coverage: no sample of 51 histories below §1's bound. The largest shortfall is 1.8×10⁻³ against
    a bound of 0.225; the parked segments' bound is 0.006–0.021, against 2.3–13.2 e-folds;
  - `None`, `None`, `()` at α = 0;
  - 27 histories bitwise on 37 keys against `f0c139d`, `"warnings"` included.
- **The derivation** (log 08 §1). On the exponential V'' falls strictly with φ (V is completely
  monotone) and (ln A)'' = 0, so m_eff² peaks at a swing's inner end at fixed trace. The shortfall is at
  most (ΔN if outward) + τ + max(0, δ), τ the trace's term and δ the estimate's mass against the field's
  own. δ ≤ +0.0037 at resolved turning points and −6.5 to −52 at reflections. The parked segment is
  bounded through d ln m_tr/dN ≤ −1, measured ≤ −1.02 (β = 25).
- **Measured**: at M ≥ 10⁻¹³–10⁻¹⁵ every generation is classical, and the birth and reflection margins
  fail; the tracker passes except at M = 10⁻³, β = 0.5. At M ≤ 10⁻²² (β = 1.2, 2) and ≤ 10⁻²⁰ (β = 3)
  every margin passes. The cost is 4–83 ms per history, 0.2–2.9 %.
- **Rulings applied**: all sixteen (log 08 §3). Deviations D1–D7; D2 (the K = 0 guard) is STRUCTURALLY
  REQUIRED, found by B7's first replay.
- **Breakages**: B1 (1), B2 (2), B3 (3), B4 (3), B5 (2, one an error), B6 (1), B7 (3),
  B8 (item 8's class, in setUpClass). B1 was caught by nothing until D3.
- **Counts:** 26 / 439 (96 + 343) / 103 / 115 / 64 before; 26 / **471** (96 + **375**) / 103 / 115 / 64 after,
  all passing.
- **Issues:** closed `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (§4), with its row
  removed from `.documents/OPEN_ISSUES.md` §1.19 and the count corrected 26 → 25; opened none.

**Orchestrator review of prompt 08** (2026-10-31, at `86fc526`, under note 08 §3). It passes. Each check:
1. **Scope.**
   - `git show --stat 86fc526` lists exactly the prompt's §6 seven files, and its parent is `f0c139d`.
   - The tree is clean, the main checkout is the only worktree, and nothing is pushed.
   - Ruling 15: the hunks of `ScalarHistory.py` are the new constants, `WARNING_KINDS` and its comment,
     the new block after `fixed_T_values`, `integrate_history`'s docstring, call and payload, and
     `tune_history`'s docstring and printing loop. `integrate_scalar_history`, `IntegrationResult` and
     the loop's records are untouched. `CosmologyConcepts/` and every existing test module are
     unchanged.
2. **α = 0 and the frozen tests.** The tests of prompt §1.8 item 9 pass unedited at `86fc526` (check 9),
   `test_unresolved_turnarounds:592` and `test_parking:933` among them. That is ruling 1's purpose.
3. **The definitions** match prompt §1.1–§1.3 and rulings 2–6 and 8–10, line by line:
   - `peak_estimate` takes ln 𝓡 ≤ 0 as classical. It flags `"classical_gap"` on a
     `ComputationFailureError` or `None`, and lets anything else propagate.
   - ln q uses `N_turn`. The events are the later candidates, plateau turning points, and wall-side
     turning points with no candidate, taken with a strict N > N_turn.
   - N_late is the largest turning-point N or candidate `N_turn`. The tracker is searched from each
     sample's own φ, and today from the final state's.
   - The weight uses ≥, K is the ledger's K(N_final), and the leftover and K_0 are unassessed.
   - Ties go to the lowest index. A first failure is the minimum of (N, margin, index). The threshold is
     ln 10.
   - The caller's condition is the loop's `assess_wall` expression verbatim, and only
     `ComputationFailureError` is caught there.
4. **The derivation**, re-derived independently for the exponential. It is sound.
   - V'' falls strictly with φ, since V is completely monotone, and (ln A)'' = 0. So m_eff² peaks at a
     swing's inner end at fixed trace.
   - The trace's term τ = ½ log1p(ε_in(T_max/T_in − 1)) follows from m² ≤ V''(φ_in) + Q T_max.
   - The bound is ΔN on an outward swing and none on an inward one, plus τ + max(0, δ_in). The inward
     swing is tighter than the prompt's "N_b − N_a".
   - The parked bound B_park = (Parking.N − N_turn,cut) + max(0, ln m_tr(Parking.N) − ln m_cut) follows
     from d ln m_tr/dN ≤ −1.
   - The tracker exponent (n + 2)/(2(n + 1)) = ¾ for the V'' term, and ½ for the trace's, check.
   - The log's general range for d ln m_tr/dN (−3 to −1.5) does not hold at β = 25. The log says so and
     relies on the measured −1.02 there, with 2 % to spare.
5. **An independent probe** (session scratchpad, not committed). It uses only the records, the model
   methods and `log_m_eff_squared`.
   - **The margins.** On β = 2 at M = 10⁻¹⁶ and 10⁻³⁰, and M = 0.5 at β = 2, 5 and 25, it recomputes
     every generation's estimate, ln q and birth, reflection and tracker margins. It also recomputes the
     worst margins with their indices, and the failing shares. All equal the payload's, with a
     difference of exactly 0. The shares partition K to 2.2×10⁻¹⁶.
   - **The coverage.** The largest shortfall outside a parked segment is +3.05×10⁻⁴ (β = 25, #27, at
     N = 130.48), as the log has. Elsewhere it is ≤ −2.7×10⁻⁴. The parked segments' field-own margins
     lie 3.30 (β = 5) and 3.29 (β = 25) above min(b, r).
   - **Bitwise against `f0c139d`**, in a temporary worktree, since removed. β = 2 at M = 10⁻¹⁶, β = 5 at
     M = 0.5, and natural mode at M = 0.5, β = 2 are equal by `repr` on all 35 shared keys: the
     `IntegrationResult` fields, `ts` and the payload, `"warnings"` included. `metadata` and `z_grid` are
     excluded. At α = 0 the new keys are `None`, `None`, `()`.
6. **The breakages.** The log's eight diffs are identical to the agent's scratch files. Each was replayed
   with `git apply` in `bash`, in a worktree of `86fc526`, and reverted. Each fails exactly the tests the
   log names:
   - B1: 1;
   - B2: 2;
   - B3: 3;
   - B4: 3;
   - B5: 1 failure and 1 error;
   - B6: 1;
   - B7: 3;
   - B8: `setUpClass` of item 8, with 30 tests run.

   The clean run passes 32/32.
7. **The numerics of the tests.**
   - Item 1 builds every expected value by the same operations and asserts equality. The partition is
     held to 6ε for six terms.
   - Item 2's boundary is `nextafter(1e-3, 0)` against 1e-3 exactly, at K = 1.
   - Item 3 holds 5×10⁻⁵ (ruling 9), measured at 4.4×10⁻⁵ and 4.8×10⁻⁵.
8. **The measurement and the records.**
   - **§2:** the tables are complete. §2.1 has 51 histories, the prompt's 45 and more. §2.2 tabulates
     the raise texts, all of them "ell … fell to". §2.3 has the parked segments. §2.4–§2.7 are given,
     with ruling 12's comparison.
   - **The log:** it is in §5.1's form, with D1–D7 classed and the sixteen rulings recorded.
   - **`.documents/`:** the notes are dated and additive only (+28 and +59 lines, nothing removed).
   - **The issue:** it is moved to §4 with its Closed line, its index row is deleted, and the count is
     corrected 26 → 25.
9. **The suites** at `86fc526`, run serially:
   - CosmologyModels 26 OK (67 s);
   - ComputeTargets 96 OK (124 s) + 375 OK (440 s);
   - ObjectFactories 103 OK;
   - tools 115 OK;
   - RunDrivers 64 OK.

   The load averages ranged from 7 to 24. ComputeTargets rose by the 32 new tests only. `black --check` is
   clean on both changed `.py` files.

Residue fixed in this review's commit: "this commit" → `86fc526` in the log and on this board, and note 08
added to the list of orchestrator notes. Nothing opened. The log's observation that the parked bound's
condition fails in the Σ wiggle at 23–35 keV is recorded there against
`[00-xav-sigma-is-non-monotone-at-25-35-keV]`. No measured parked segment crosses that band, so nothing
rests on it.

### 5.11 After prompt 09 (2026-10-31)

09 landed in `3984ec5`, under orchestrator note 09 (rulings 1–17). Log: [`logs/09-store.md`](logs/09-store.md).
- **The stop.** Ruling 9's required `r_chi`, and `compute_adiabatic_values` reading the stored ratio, fail
  `test_adiabatic_mass`, which the prompt and ruling 16 froze: 11 errors in 9 tests. The agent stopped. The
  user ruled, 2026-10-31: "(b) is fine". `_M2eff` passes r_χ = 0.0 when the signature has it, and the stand-in
  samples gain `rho_chi_over_rho_rad=0.0`. Ruling 9 stands (log 09 D1).
- **What shipped:**
  - α is a key of every history: a new replicated table `alpha_value`, and `ScalarHistory.alpha_serial`
    with the inventory parent `alpha` (U3, P28).
  - The payload gains `"rho_chi_over_rho_rad"`, stored as a column of `ScalarHistoryValue` (P27), and
    `"dark_radiation_values"` (R22, P31).
  - There are 24 whole-history columns: the counts, the injected fraction, r_χ and ΔN_eff at 1 MeV,
    70 keV and today, ln 𝓡 at the first impact, the parking and the release, and the margins of R25. On the
    object they are `ScalarHistory.dissipation`, read with `_do_not_populate`. At α = 0 they hold zeros
    where defined and NULL elsewhere (R24).
  - `extra_data` is `Text`, with `"margins"` (R23).
  - `AdiabaticHistory` reads ρ_χ through H only, with E the bath's share (P29; log 09 §1.1).
  - Every reader passes the α = 0 row (P30).
  - Format 6, with its golden file, and 2026.8.0.
- **Gate Store holds:**
  - The round trip is bitwise on every column and sample of eight histories (§5.1), and in tests 1 and 2.
  - Two α are two rows, and α = 0.3 finds neither (test 3).
  - R24 holds (test 4).
  - Format 6, its golden file and 2026.8.0 are in place.
  - `AdiabaticHistory` is bitwise at α = 0: it equals the pins taken at `03b769e` (test 8 (a)), and §5.5
    compares four setups by `repr`.
  - Q is finite on 6 332 parked samples (§5.3).
  - Both golden histories pass unedited.
- **Measured:**
  - ΔN_eff(1 MeV) is 0.049–0.061 at β ≤ 2 and 0.0058 / 0.0012 at β = 5 / 25, M = 0.5.
  - ΔN_eff(today)/ΔN_eff(1 MeV) is the closed form (K_f/K₁)[(11/4) g_s(T_CMB)/g_s(1 MeV)]^{4/3}, to
    −1.328×10⁻³ on every history. The residual is the bath's ρ_R,J drifting from (π²/30) g_ρ T_J⁴ by
    about 10⁻³ (log 09 §5.2, §7).
  - BBN's frame factor F at 1 MeV is 0.93–1.02 at small M, and 0.11 down to 4.4×10⁻⁵ at M = 0.5 for
    β = 1.2–25. Test 9 asserts the identity Δr_BBN = F r_χ.
  - ρ_χ moves M²_eff/H² by at most 12 % (relative), near a zero of it; by at most 1.25 % of max(|m|, 1);
    and max |Q| by at most 0.8 %.
  - The cost is +1–3 % on the store and on `compute_adiabatic_values`.
- **Rulings applied:** all seventeen (log 09 §3). Deviations D1–D9; D1 (the stop), D3 (the inventory test's
  expectations move by the α row) and D4 (an import) are STRUCTURALLY REQUIRED.
- **Breakages**, by the number of tests each fails:
  - B1: 2;
  - B2: 1;
  - B3: 1;
  - B4: 1;
  - B5: 1;
  - B6: 2;
  - B7: 1, in 5 subtests;
  - B8: 1 failure and 9 errors.
- **Counts:** 26 / 471 (96 + 375) / 103 / 115 / 64 before; 26 / **473** (**98** + 375) / **121** / 115 / 64
  after, all passing.
- **Issues:**
  - closed `[00-settling-at-physical-M-needs-a-parked-tracking-model]` (§3.1 → §4; `integrator-remediation`
    §3 → §4; index §1.20 emptied, count 25 → 24);
  - opened none. Log 09 §7 records the ρ_R,J drift and F's absence from ΔN_eff for the user.

*Orchestrator review of prompt 09* (2026-10-31, at `3984ec5`, against orchestrator note 09 §3):
1. **Scope.**
   - `git show --stat 3984ec5` lists 38 files. They are ruling 16's: the prompt's §6 list,
     `tools/history_and_bbn.py` (ruling 4), the registry pins of ruling 1, and `test_adiabatic_mass.py` by
     the user's ruling.
   - Its parent is `03b769e`, note 09's commit. The tree is clean, the main checkout is the only worktree,
     and nothing is pushed.
   - In `ScalarHistory.py` the diff touches only an import, the new record, functions and constants
     (:3077–:3268), `integrate_history`'s sampling append, call and payload, `tune_history`'s docstring,
     `compute_scalar_history`, `build_extra_data`, `ScalarHistory` and `ScalarHistoryValue`. The step loop,
     `IntegrationResult`, the records of 02–08 and `SampleValues` are untouched.
2. **α = 0 and the frozen tests.**
   - Both golden histories, the tuned one included, and the eight modules ruling 16 froze pass unedited.
   - Every edit to an existing test is an α row, a registry pin, format 6 or 2026.8.0, or the user's
     ruling (log 09 §3).
   - D3, `test_inventory_report`'s expectations moving by the α row, is accepted. The tool is unchanged,
     and it prints every key field generically.
3. **The schema**, line by line against prompt §1.1–§1.4 and rulings 2–8: it agrees.
   - `alpha_value` follows `beta_value` in the registry, the batch sizes and the replicated tables.
   - `alpha_serial` is a non-null foreign key, filtered in `build`, with the parent `alpha`.
   - The 24 columns are the table's names, all nullable, filled by `dissipation_values` from the
     payload with `.get`. An absent ratio key with samples raises `RuntimeError`.
   - `extra_data` is `Text`, and `"margins"` holds the 13 keys and nothing else of `Generation`.
4. **The physics.**
   - Log 09 §1.1 agrees with the orchestrator's own derivation: ρ_χ enters M²_eff/H² through H² in
     V″/H², through Ḣ/H², and through E's normalization. The code takes `ODEPolicy`'s branch on
     `r_chi == 0.0` verbatim.
   - F = A²(1 + (ln A)′π)²/G and ruling 12's closed form are as note 09 §0 derived them.
5. **An independent probe** (`review_probe.py`, session scratchpad).
   - M = 0.5, β = 5 and β = 2, M = 10⁻³⁰, at α = 0.1, were stored on a temporary store. Every one of the 24
     columns was recomputed without the agent's functions: the crossing by the orchestrator's own brentq,
     r_χ from the ledger as K e^{−4N}/ρ_R,E, the fraction by `fsum`. The largest relative difference is
     2.9×10⁻¹⁴.
   - Every per-sample ratio (6 814 and 5 437) equals the ledger's.
   - The `"margins"` key equals the payload's first failures.
   - Q on the parked samples of the read-back objects is finite at every label. Its maxima (0.1859 and
     2.0985×10⁻⁸ at k/H = 10) agree with the orchestrator's dispatch-tree probe to the 0.15 % that ρ_χ
     moves them.
   - At α = 0, every payload key of golden β = 5.5 and of natural mode at M = 0.5, β = 2 hashes by
     `repr` as at `03b769e`, compared in a temporary worktree that was then removed. Only
     `"rho_chi_over_rho_rad"` and `"dark_radiation_values"` are new.
6. **The breakages.** B1–B8, replayed with `git apply` in `bash` in a temporary worktree of `3984ec5`, and
   reverted with the tree left clean. Each fails what the log names:
   - B1: 2;
   - B2: 1;
   - B3: 1;
   - B4: 1;
   - B5: 1;
   - B6: 2;
   - B7: 6 (the test and 5 subtests);
   - B8: 1 failure and 9 errors.

   The clean run passes 20/20. The log's B8 diff had lost its trailing context line and did not apply.
   Restored in this review's commit; it now passes `git apply --check`.
7. **The numerics of the tests.**
   - Test 6 repeats the code's operations, so it is bitwise.
   - Test 9's bound is derived in log 09 §1.2. Its largest miss over the bound is 0.032 and 0.037, with
     F = 0.0094–0.0111 at β = 5.
   - Test 8 (a)'s pins were taken at `03b769e` by the script the log quotes.
   - Ruling 7: object == payload held bitwise on every converted column measured.
8. **The version.**
   - `FINGERPRINT_FORMAT` 6. `SPEC_DIGESTS[6]` was regenerated by the guard's `spec_digests` and is equal:
     16 pairs, only `ScalarHistory`'s digest moved, and `alpha_value` is new.
   - The one new golden file sits beside the old ones, which are unchanged.
   - `VERSION_LABEL` is 2026.8.0, with its history line.
9. **The measurement and the records.**
   - §2.1 has the 8 histories, §2.2 ruling 12's residuals and F, §2.3 ruling 13's locations, and §2.4
     both measures. §2.5 is bitwise on four setups, and §2.6 has three rounds in both trees.
   - The log is in §5.1's form, with D1–D9 classed and the seventeen rulings recorded.
   - The `.documents/` notes are dated and additive.
   - The issue is closed on both boards with its Closed lines. Its index row is deleted, and the count went
     25 → 24 before this review.
10. **The suites** at `3984ec5`, run serially:
    - CosmologyModels 26 OK (64 s);
    - ComputeTargets 98 OK (129 s) + 375 OK (457 s);
    - ObjectFactories 121 OK (66 s);
    - tools 115 OK (127 s);
    - RunDrivers 64 OK (23 s).

    The load averages ran 3–9. The counts rose by the 20 new tests only. `black --check` is clean on the 31
    changed `.py` files, and the four `--help`s exit 0.

Residue fixed in this review's commit:
- "this commit" → `3984ec5` in the log, on this board and on the `integrator-remediation` board;
- note 09 added to the list of orchestrator notes;
- the log's B8 diff restored.

**Opened** in this review's commit: `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]` (§3; index §1.19,
count 24 → 25). Log 09 §7 found the stored ρ_R,J below (π²/30) g_ρ T_J⁴ by 0.21 % at 1 MeV. The review
traced it to `Xav_EOS_spline`'s w, g_ρ and g_s, which miss g_s = (3/4)(1 + w) g_ρ by up to 3.6×10⁻³ above
1 MeV. It is every history's and predates the campaign, so it is opened, not residue. Log 09 §7's other
observation, that ΔN_eff(1 MeV) does not carry BBN's frame factor F (0.93–1.02 at small M, down to
4.4×10⁻⁵ at M = 0.5, β = 25), is for the user under R22 and is not opened.

### 5.12 After prompt 10 (2026-10-31)

10 landed in `8c12afa`, under orchestrator note 10 (rulings 1–13). Log: [`logs/10-driver-and-readers.md`](logs/10-driver-and-readers.md).
- **No stop.** Prompt §5 and the note's amendments were not reached. One existing-test constraint is a deviation to
  review (log 10 D1): `test_bbn_from_store`'s imitation store has no `alpha_value` table, so `find_model` takes a
  shard without the table as holding α = 0 only, rather than the unconditional join of ruling 8. The alternative is
  to edit that store.
- **What shipped:**
  - `--alpha-values` (`nargs="+"`, default `[0.0]`, replaced by a given list; `alpha_fraction` refuses α outside
    [0, 1) and `nan` at parse, exit 2; `-0` is 0.0); `distinct_values`;
  - `main.py`, `plot_by_beta.py` and `plot_ScalarHistory.py` fetch one `alpha_value` row per distinct α and call
    `run_pipeline` once per row inside the loop over the cosmologies; `plot_inputs.parameters` takes `alpha_values`
    and returns `PlotParameters.alphas`; an unstored α is `PlotOptionNotStored` naming `--alpha-values`;
  - every per-α product of both plot scripts under `<output>/<label>/alpha=<α>/` (R26); `plot_by_beta.run_pipeline`
    returns its records and `histories.csv` and `Delta_N_eff.pdf` are written once per cosmology after the last α;
  - `plot_Delta_N_eff` (R28, two panels) and `plot_dark_radiation` (R27, Δg* and ρ_χ/ρ_R, parking and release
    lines) in `extract_common.py`; `histories.csv` gains `alpha`, `Delta_N_eff_today`, `Delta_N_eff_1MeV` and sorts
    by (α, M, n, β); `fields.csv` gains `rho_chi_over_rho_rad` and `Delta_gstar`;
  - the scope reads `α <values>`; the inventory has `alpha_value` among the parameters and `alpha` among the extra
    fields; `tools/history_and_bbn.py --alpha` (a `dissipation` line at α > 0) and `tools/bbn_from_store.py --alpha`.
- **Gate Driver holds:**
  - α reaches every stage of `main.py`, once per value (test 2, by `ast`; B2);
  - an unstored α is `PlotOptionNotStored` and the drivers' handler exits 2 (test 3; the drivers were not run, which
    needs Ray, ruling 6);
  - the scope and the inventory name α (tests 8 and 9; `main.py --inventory` on a two-α store);
  - R26's paths (test 4), R27's and R28's figures (tests 5 and 7, §5.2: the lines fall at log 09 §5.2's N to the bit);
  - both golden histories pass unedited, and `history_and_bbn.py` and `bbn_from_store.py` at α = 0 are byte-identical
    to `d097745` with the real PRyMordial solve (log 10 §5.1).
- **Measured:** the figures' ranges agree with log 09 §5.2 (ΔN_eff(T_CMB) 0.0654 at M = 0.5, β = 1.2 and 0.00123 at
  β = 25, α = 0.1); the `--help`s and the parser cost the same before and after (log 10 §5.3).
- **Rulings applied:** all thirteen (log 10 §3). Deviations D1–D9; D2 is STRUCTURALLY REQUIRED, the rest
  IMPLEMENTATION CHOICE.
- **Breakages**, by the number of tests each fails: B1: 3; B2: 1; B3: 5; B4: 2; B5: 2; B6: 1; B7: 6 (three existing);
  B8: 4.
- **Counts:** 26 / 473 (98 + 375) / 121 / 115 / 64 before; 26 / **497** (**122** + 375) / **129** / **123** / **68** after,
  all passing. 44 new tests.
- **Issues:** opened none, closed none; the index is unchanged (count 25). Log 10 §7 records the five-figure α
  directory name, the missing α in `bbn_from_store.py`'s CSV rows, and that the drivers' first run with α is 11's.

*Orchestrator review of prompt 10* (2026-10-31, at `8c12afa`, against orchestrator note 10 §3):
1. **Scope.**
   - `git show --stat 8c12afa` lists 25 files: the prompt's §6 list, and ruling 1's two test modules.
   - Its parent is `d097745`, the note's commit. The tree is clean, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
2. **α = 0 and the frozen tests.**
   - `git diff --stat d097745 8c12afa` is empty on every file ruling 12 freezes: the code under
     `ComputeTargets/` and `ObjectFactories/` outside `tests/`, the goldens, `test_reflection_reporting`,
     `test_dissipation_store`, `test_adiabatic_dark_radiation`, `test_bbn_sees_dark_radiation`,
     `dissipation_histories.py`, `test_driver_options`, `test_registered_run_streams`, `test_bbn_from_store`,
     the surfing tools and `CLAUDE.md`.
   - The five existing test modules edited are each §1.10's class or ruling 1's, and nothing else:
     - `test_plot_inputs`: `alphas` and `alpha_values`;
     - `test_extraction` and `test_tuned_history_round_trip`: `alpha=0.0` and `dissipation=None`;
     - `test_registered_run`: `α 0`;
     - `test_inventory_report`: `alpha=0` moved to the common line.
3. **The code** against prompt §1 and rulings 1–10, read line by line in the diff: it matches. **D1 is
   accepted** as an *IMPLEMENTATION CHOICE*.
   - `find_model` takes a shard with no `alpha_value` table as one written before format 6, whose histories
     are α = 0 by construction. That is right for any such store, and narrower than editing
     `test_bbn_from_store`'s imitation store, which ruling 12 froze.
   - A shard that has the table takes ruling 8's join and match exactly. The note's §0 counted five
     `build_history_record` calls in `test_extraction`; there are three, and all three gained `alpha=0.0`.
4. **The independent probe** (session scratchpad, `review10/probe.py`, not committed).
   - **The option:**
     - the default is `[0.0]`, a given list replaces it, and the YAML key gives the same list;
     - `-0` parses to `0.0`, and duplicates collapse in order;
     - −0.1, 1, `nan` and `inf` exit 2 with the message naming the range.
   - **The store**, with M = 0.5, β = 2 at α = 0 and 0.1, and M = 10⁻³, β = 1.2 at α = 0.1, on 09's harness:
     - `find_model` finds serial 1 by default and at α = 0, and serial 2 at α = 0.1 and 0.1000005;
     - α = 0.3 is a `LookupError` naming it;
     - `plot_inputs.parameters([0.1, 0.0])` returns both rows in that order.
   - **`plot_dark_radiation`** on the read-back histories:
     - Δg* equals my own `gstar_rho * rho_chi_over_rho_rad` by `==` on all 3 078 and 3 080 drawn samples;
     - the parking and release lines sit at the stored `parking_N` = 28.371911595351886 and `release_N` =
       45.91020342925626 exactly, and there is no line where the history does not park;
     - at α = 0 it returns `False`.
   - **The records:** each ΔN_eff equals `ScalarHistory.dissipation`'s, and α = 0 gives 0.0.
     `histories.csv` sorts by (α, M, n, β).
   - **α = 0 unchanged:** `tools/history_and_bbn.py 2 0.5 --fixed-normalization …` at `d097745`, in a
     temporary worktree I then removed, and at `8c12afa`, both with the real PRyMordial solve. After ruling 7's
     mask they are byte-identical, 1 227 bytes.
   - **α = 0.1:** at M = 10⁻³, β = 1.2 the `history` line carries `alpha=0.1` before `wall=`, and the
     `dissipation` line prints all 24 fields in order, the ln T_J in GeV.
5. **The breakages.** Each of B1–B8 was replayed from the agent's diffs with `git apply` in `bash`, then
   reverted, leaving the tree clean. Each fails what the log names: B1 3 tests (5 entries), B2 1, B3 5, B4 2,
   B5 2, B6 1, B7 6 (three existing), B8 4 (1 failure, 5 errors).
6. **The figures.** I looked at the PNGs of `dark_radiation_b1.2_M1e-3` (both coordinates) and
   `Delta_N_eff_M0.5_n1`.
   - The ΔN_eff curves fall with β, and α = 0.1 lies above α = 0.03 in both panels, the two closing up at
     β = 25.
   - Δg* runs 0.030–0.62, with the parking line at the start of the plateau and the release at N = 45.9, as
     log 09 §5.2 puts them.
   - Two cosmetic points of the redshift mode, which no script calls, are left as observations, not opened:
     - its x axis is not inverted as the other history figures' are;
     - its temperature lines sit at z (`get_xpos_attr`) while its curve and its own lines are at 1 + z
       (`get_x_coord`). That offset is `extract_common`'s and predates the campaign.
7. **The records.**
   - The log is in §5.1's form, with every ruling recorded and D1–D9 classed. "State handed to the next
     prompt" covers prompt §4.3's list.
   - The architecture note is dated, after 09's, with no line removed.
   - §5.12 and the gate are filled. No issue was opened, so the index is untouched (25).
8. **The suites** at `8c12afa`, run serially:
   - CosmologyModels 26 OK (75 s);
   - ComputeTargets 122 OK (164 s) + 375 OK (474 s);
   - ObjectFactories 129 OK (81 s);
   - tools 123 OK (133 s);
   - RunDrivers 68 OK (31 s).

   The loads were 6–91 (another workload peaked during the first ComputeTargets half). The counts rose by the
   44 new tests only. `black --check` is clean on the 22 changed `.py` files. The six `--help`s exit 0, and
   each shows `--alpha-values` or `--alpha`.
9. **The inventory** of a temporary store holding the two α of check 4, given a stand-in `QCD_Cosmology` row:
   - it was read by `read_inventory` and `format_inventory_report`, the two calls `main.py --inventory` makes;
   - `problems: none`;
   - `alpha_value` is under "Redshifts, tolerances and parameter values";
   - the common line has no α, and the history lines end `alpha=0` and `alpha=0.1`.

Residue fixed in this review's commit:
- "this commit" → `8c12afa` in the log and on this board;
- note 10 added to the list of orchestrator notes.

**Opened:** none. The drivers' first run with α is 11's rehearsal (U11), as log 10 §4.4 says. 11 is written next,
from log 10 §8 (README §2.1).

### 5.13 After prompt 11 (2026-11-01)

11 landed in `c797831`, under orchestrator note 11 (rulings 1–9). Log: [`logs/11-measure.md`](logs/11-measure.md).
Record: [`.documents/inelastic-reflection-verification.md`](../../.documents/inelastic-reflection-verification.md).
- **No stop.** Prompt §5 was not reached: no lock refusal, no driver failure, no gate item failed, every history
  converged, no stall.
- **What shipped:** no code. The measurement scripts under `.documents/inelastic-reflection/measure/`
  (`points.py`, `eps_point.py`, `run_grid.py`, `summarise_grid.py`, `histories.py`, `compare_jn09.py`); the
  verification document §1–§6; `.documents/inelastic-reflection/out/` (the 48 points' outputs, the tables, the
  rehearsal's records, the helpers). The stores `inelastic-reflection-11-reference` and `…-resumed` are kept under
  `var/datastores/` for the user, each with its sidecar.
- **Gate Measure holds:** 48 of 48 points, all exit 0; the drain's M-independence 1.47×10⁻⁵ at worst against 10⁻³;
  ΔN_eff today strictly rising in α at all 8 (M, β); the ten margin fields at every dissipation point; α = 0
  bitwise 2026.7.0 over 548 922 values.
- **Gate R holds:** `compare_stores` `RESULT: equal`, both stores `afd2f329…1264`; the signal (28 s after the α = 0.1
  banner) found three of four α = 0.1 histories stored and the resume computed the fourth; the two α are eight
  distinct rows; the inventory, both plot scripts (23 s, 18 s) and the unstored α (exit 2, nothing written) as log
  10 says, the store byte-identical across each; the tool and both stores bitwise on 8 × 15 × 2 tuning values.
- **Measured:** dissipation at M ≤ 10⁻¹² converges on its seed in 11–31 s, every reflection unresolved, every
  history parked and none released; ΔN_eff today 0.0027–0.565, saturating in α at β ≥ 2; at M = 10⁻¹² every birth
  margin fails (−0.70 to −0.74), at 4×10⁻³¹ none; ε = 10⁻⁸ parks 5.416 e-folds later at β = 1.2 as log 06 §5.5 found,
  and ε moves the drain by ≤ 2.5×10⁻³; natural mode at 10⁻⁷ warns (P23) at every β and reaches T_CMB; the grid is
  23.4 min serial. At M = 10⁻³ the rehearsal's β = 1.5 parks and is released at today's temperature, and β = 0.5
  ends with ΔN_eff = 8.95 (log 11 §6).
- **Rulings applied:** all nine (log 11 §2). Deviations D1–D8: D2 STRUCTURALLY REQUIRED (`git_dirty` manifests);
  D3 and D8 UNINTENDED DRIFT, corrected or harmless (the first ε points patched a class, and ran again; a stray
  wait); the rest IMPLEMENTATION CHOICE.
- **Counts:** 26 / 497 (122 + 375) / 129 / 123 / 68 before and after, all passing.
- **Issues:** opened `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]` (P14's effect, up to
  8.4× natural mode's rms_step in 10–100 keV) and `[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]`;
  closed none. Index 25 → 27.

**Orchestrator review of prompt 11** (2026-11-01, at `c797831`, by the eight checks of note 11 §3). Dispatched at
02:40 GMT, not 05:00 as the note had it: the user said the other workload had finished early and to dispatch now.
The load then was 3.56 / 3.87 / 6.26 and falling, the lock free, no run going.
1. **Scope.** `c797831`'s parent is `6d1a77c`; 123 files, all on the prompt's §6 list. `git diff 6d1a77c c797831`
   over `*.py` outside `.documents/inelastic-reflection/measure/` is empty, and over `ComputeTargets/` empty. The tree
   is clean, one worktree, nothing pushed.
2. **Nothing left running.** `ray_cluster.sh status`: no process, ports 6380, 6381, 8267, 52367 free; lock free;
   `runregistrykit list` 14 runs, all finished (`reference-a` done, `resumed-a` killed, `resumed-a2` done);
   `run_grid.pid`'s pid 56929 does not answer `kill -0`.
3. **The scripts.** `points.py` gives P40's 48 points. `eps_point.py` takes the module by `importlib` and asserts it
   is `sys.modules`'s. Replayed at β = 2, M = 4×10⁻³¹, α = 0.1 (load 5.8): at ε = 10⁻⁶ the output equals the plain
   tool's after ruling 7's mask but for the `epsilon=` line and the tune line's `wall=`, which the mask does not
   cover; at ε = 10⁻⁴ it parks at N = 29.1825 against 30.3493, and equals the committed
   `eps0.0001-M4e-31-b2-a0.1.txt` but for its header, footer and timings. So D3's fix reaches the cut. D3 is
   accepted: my note's §0 said patching the module attribute is enough and did not warn that `import … as` binds
   the class `ComputeTargets/__init__.py` exports under the module's name. The agent's observation that a test might
   patch through that name was checked: every test module that patches `ComputeTargets.ScalarHistory` takes it by
   `importlib.import_module` (`test_parking`, `test_release`, `test_tuning`, `test_margins` and ten more). Nothing
   to open.
4. **The stores.** `compare_stores.py` myself: `RESULT: equal`, problems none. `store fingerprint` of the reference
   `afd2f329d79b3b3e65fd7af53dd1ef33b80f60f74e49eb1d77698da0ea6f1264`; the resumed store's matches `resumed-a2`'s
   record. My own read of §2.7, written apart from `compare_jn09.py`, on β = 1.5 and 3: the α = 0 row picked by
   `alpha_serial`, the 2026.7.0 row by its label; 27 tuning and history columns, and 100 samples spread over
   ln(1 + z) by 18 columns each, matched through `z_serial` to `redshift.log_one_plus_z`. The z sets are equal
   (5161, 5973 samples); 3 654 values compared by `float.hex`, 0 differences.
5. **The interruption.** The reference's α = 0.1 timestamps 03:02:38.172, :38.560, :47.917, 03:03:05.158 against the
   banner at 03:02:23.291: +14.881, +15.269, +24.626, +41.867 s; (14.881 + 41.867)/2 = 28.374 → 28 s. In the resumed
   store three α = 0.1 histories are stamped 03:08:07.9–03:08:17.5, before the 03:08:21 signal, and the fourth
   03:10:10.9, by `resumed-a2`.
6. **The grid.** 48 `.txt` in `out/grid/`, 48 rows in `11-grid.csv`. Re-derived from the `.txt`: `injected_fraction`
   at β = 1.2, α = 0.03 is 4.77064×10⁻³ (M = 10⁻¹²) against 4.77057×10⁻³ (4×10⁻³¹), 1.47×10⁻⁵ relative; ΔN_eff today
   at 4×10⁻³¹, β = 2 is 0.0169, 0.0524, 0.123, 0.147 at α = 0.03, 0.1, 0.3, 0.9, strictly rising; at β = 1.2 the
   parking N is 28.3641 at ε = 10⁻⁴ and 10⁻⁶ and 33.7804 at 10⁻⁸, 5.416 e-folds later.
7. **The figures.** `Delta_N_eff.pdf` (`var/inelastic-reflection-11/rehearsal/plot-by-beta/QCD_Cosmology/plots/M=2.4353e+24eV_n=1/`):
   the α = 0.1 curve only, α = 0's zeros left off the log axes as note 10 ruling 5 says; today's panel falls from
   8.95 at β = 0.5 to 6×10⁻³ at β = 5.5, and the 1 MeV panel has no β = 0.5 point (its drains are all after
   1 MeV). `dark_radiation.pdf` for β = 1.5, α = 0.1: the parking line at N = 36.34 and the release at 47.51; ρ_χ/ρ_R
   settles at 8.3×10⁻³, which with (4/7)(11/4)^{4/3} g_ρ(today) ≈ 7.4 gives ΔN_eff ≈ 0.061, the summary figure's
   value at β = 1.5.
8. **The records.** Log 11 is in §5.1's form, the nine rulings and D1–D8 classed, §6 and §7 as the prompt asks.
   The verification document is dated, new, and in `jordan-normalization`'s form; `out/` holds text only. My token
   grep over the commit's files: no output, exit 1. `black --check` on the six measure scripts: unchanged. The
   board's row 11, the gates Measure and R, §5.13, and the two issues on §3 and in `.documents/OPEN_ISSUES.md`
   (27 open) agree.

Residue fixed in this review's commit:
- "this commit" → `c797831` in the log and on this board;
- note 11 added to the list of orchestrator notes.

**Opened:** none by the review. **For the user:** the rehearsal's β = 0.5 history at M = 10⁻³, α = 0.1 ends with
ΔN_eff today = 8.95 (log 11 §6). It is in R17's unofficial large-M regime and is recorded, not opened. 12, which closes
the campaign, is written next, from log 11 §7 (README §2.1).

**The stores, 2026-11-01, after the review.** The user retired two stores with `runregistrykit store retire`; each
sidecar now holds the tombstone. `inelastic-reflection-11-resumed` was retired with its fingerprint matched
(`afd2f329…1264`). `jordan-normalization-09-reference` was retired `--without-fingerprint`: it predates this
campaign's schema, so the current code cannot read it (`StoreSchemaMismatch`, the error recorded in the
tombstone); 11's α = 0 histories are bitwise equal to it (§2.4 of the verification document).
`inelastic-reflection-11-reference` is kept, and is now the reference store at 2026.8.0. The paragraphs above,
which say both of 11's stores are kept, were true when written.

### 5.14 After prompt 12 (2026-11-01)

12 landed in `7ad6744`, under orchestrator note 12 (rulings 1–10). Log: [`logs/12-close.md`](logs/12-close.md).
- **No stop.** Prompt §4 was not reached: §1.20 lists none assigned, no suite failed and no count fell, every
  statement of the documents that the campaign made false is corrected by a dated note, `CLAUDE.md` gained no
  rule, and the inventory changed no byte of the store.
- **What shipped:** no code. `numerical-strategies.md` §6.10 (the measurement of 11) and §6.11 (dissipation mode
  as it stands, with the table of §6.1–§6.9's superseded statements); `architecture-summary.md`: the §7.2 map,
  the §14 note (D3 and `PARKING_EPSILON`), and dated notes at §2's tree, §5.2, `first_bounce_reflected`'s row and
  §8.2, with a one-line note at `numerical-strategies.md` §3.5.3; `review-remediation-verification.md` §4.18;
  `CLAUDE.md`'s counts, the time, the parenthesis, the plot-scripts bullet and the Redshift bullet (descriptions
  only); this board, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md`. No existing line of the three documents is
  deleted or reworded (`git diff` shows 0 deleted lines in them).
- **Counts:** 26 / 497 (122 + 375) / 129 / 123 / 68, log 11's exactly, all passing; the `test_[g-z]` half took
  454.6 s.
- **Issues:** opened none, closed none. Index 27.
- **Rulings applied:** all ten (log 12 §2). Deviations D1–D14: D1, D5, D10, D11 and D12 STRUCTURALLY REQUIRED
  (D11: the store has 18 files, the manifest included; D12: the prompt's "payload key and not a column" is
  corrected to the code, which also stores a `ScalarHistoryValue` column); D13 UNINTENDED DRIFT (two suites ran
  twice because the first run cut off their counts; harmless); D7, D8 and D14 NOTED; the rest IMPLEMENTATION CHOICE.

**Orchestrator review of prompt 12** (2026-11-01, at `7ad6744`, by the ten checks of note 12 §3). Dispatched at
09:05 GMT on Sonnet; the load spiked to 42 from the machine's daemons, and no ChamPBH process ran.
1. **Scope.** `7ad6744`'s parent is `7cfced4`; eight files, exactly the prompt's §5 list. `git diff 7cfced4 7ad6744` over
   `*.py`, `*.sh`, `*.toml`, `*.yaml`, `*.json`, `requirements.txt`, `science-runs/`, `orchestrator/` and the README is
   empty. The tree is clean, one worktree, nothing pushed.
2. **Additive.** The three documents' diffs have no deleted line. §4.18's hunk starts after §4.17's last line; §6.10 and
   §6.11 sit between §6.9 and `## 7.`.
3. **The code claims.** 56 `file:line` references of §6.11.1, the §7.2 map, §14 and §5.2 read in the code, across
   `ScalarHistory.py`, `AbstractModel.py`, `dark_radiation.py`, `ObjectFactories/ScalarHistory.py`, `config/`,
   `main.py`, `extract_common.py` and the tools: each names what the note says. D3 reproduced in a scratch probe:
   `import ComputeTargets.ScalarHistory as X` gives the class (`X is sys.modules[…]` is `False`), and
   `importlib.import_module` gives the module. One looseness, recorded and not edited: §14's "finds 17" is the grep it
   describes, but `tools/bbn_from_store.py` is in that set for `import_module("ComputeTargets.BBNData")`, not for the
   `ScalarHistory` module. D14 is right: `_parking_cut`, which note 12 (and note 11) named, is not a function of the code;
   the read is in `parking_ratio_test`. D12 is right: `rho_chi_over_rho_rad` is a `ScalarHistoryValue` column
   (`ObjectFactories/ScalarHistory.py:1092`) as well as a payload key.
4. **The numbers.** 27 numbers of §6.10 and §4.18 found in the verification document (the drain's 1.47×10⁻⁵ and
   0.00477064, the ΔN_eff table, 0.85, 53.5–191.1 s and 493 615 steps, 0.0997, 1.9–2.5×10⁻³, 2.4×10⁻⁵, 5.416, 8.9×10⁻⁶,
   6.54, 0.966–0.9999, 12.7, 1.6×10⁻³, 6.4×, 8.95, 548 922, 153 s, 4 min 46 s, 0.00019824, 11.1 s, 32 106) and the stage
   times 2m 33.1s and 43.2 s (verification §2.1). §4.16's 6.6–8.4 hours is §4.16's. R6's and R17's quotes are the
   README's (`:572`–`:573`, `:624`).
5. **The grep.** My own grep for "elastic" at `7cfced4`: every hit is in a dated section, and the ones describing the
   present are covered by the new §3.5.3 note and the `first_bounce_reflected` note. None is false and unnoted.
6. **The handover's commands.** The `--help` of each command exits 0 (run before dispatch). `main.py --inventory` on
   the reference store myself: exit 0, 75,050 records in 16 classes, `problems: none`; its 18 files' `shasum -a 256`
   equal before and after, and equal to the hashes taken before dispatch. `--alpha-values` probed on
   `create_argument_parser`: `[0.0]` by default, `[0.1]` for `0.1` (replaces), `[0.0, 0.1]` for `0 0.1`.
   `runregistrykit list`: 14 runs, all finished; the cluster's status: no process, lock free.
7. **The "made false" table** against note 12 §0. One difference, the agent's right: test_injection test_4's
   docstring was already corrected by `67cb5f8` (the review of 03), which my note missed. The two code statements
   left are `REFLECTIONS_KEY`'s comment (`ComputeTargets/ScalarHistory.py:75`) and `integrate_history`'s verbose
   line (`:4024`), offered to the user as housekeeping.
8. **`CLAUDE.md`, the board and the indexes.** `CLAUDE.md` is descriptive; my modal grep finds one line, the
   original's "They **must not** need a Ray cluster" carried inside a re-flowed line ("run it alone" is also the
   original's). INDEX: 15 rows, 15 **complete**, as its header says. OPEN_ISSUES: 27 rows, as its header says; §1.19
   and §1.20 say the campaign closed.
9. **The log** is in §5.1's form, the ten rulings and D1–D14 classed; the message is in `CLAUDE.md`'s form.
10. **The suites** were not re-run: check 1 found no code changed. The agent's counts, 26 / 497 (122 + 375) / 129 /
    123 / 68, are log 11's.

Residue fixed in this review's commit:
- "this commit" → `7ad6744` in the log, on this board, in §4.18 and in `prompts/INDEX.md`;
- note 12 added to the list of orchestrator notes; the status line says "landed and reviewed".

**Opened:** none by the review. Note 11 §0's statement that the shard key now includes α was wrong (it is β;
`alpha_value` is replicated, `architecture-summary.md` §5.2's note of this date); nothing depended on it.

**The campaign is closed.** 13 of 13 prompts landed on the branch `inelastic-reflection`, cut from `main` at `6f635e6`
(`git merge-base main inelastic-reflection` gives `main` itself, so the merge is a fast-forward); nothing is pushed
and the merge is the user's (U1). The handover for the science rebuild is
[`.documents/review-remediation-verification.md`](../../.documents/review-remediation-verification.md) §4.18. Four
issues stay open and none is assigned (§3). The reference store is
`var/datastores/inelastic-reflection-11-reference.db`.
