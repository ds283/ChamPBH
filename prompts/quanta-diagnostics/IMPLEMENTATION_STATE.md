# quanta-diagnostics campaign — implementation state

**Last updated:** 2026-11-02 · **Status: PLANNED — 0 of 7 landed.** Planned 2026-11-02 at the user's
request, after a scoping session the same day ([`source/scoping_2026-11-02.md`](source/scoping_2026-11-02.md)).
U1–U7 and P1–P16 ruled by the user the same day, all as proposed (README §6). The plan is
[`README.md`](README.md), on branch `quanta-diagnostics`, cut from `reflection-restart` at `3b8a553`.
Under U1, no prompt is written until `reflection-restart` 03 has landed and been merged here; under U2,
the rebuild of record waits for this campaign's 07.

**2026-11-02, after the rulings: held for the user's decisions of README §6.3.** The premise was corrected
(births, not later bounces; [`source/margins-correction_2026-11-02.md`](source/margins-correction_2026-11-02.md)),
and a probe of a one-loop drain ([`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md),
README §0.4) found the late classical cascade to be a product of the fixed-α drain. Whether the drain
changes, and where, decides what this campaign becomes.

The campaign stores what the run computes and discards about the produced quanta: every generation and
turning point as a row of a child table, the field's and the tracker's mass and the matter-like excess at
every sample, and per history the margins over all generations, the late-born share, and three measures
of what a later bounce does to quanta already present, with two summaries (δ_stim, δ_resc). It measures
them on a sample and a rehearsal, and writes the tools for the grid figure and the per-history m·a figure.
No stored value moves.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/quanta_diagnostics.py` (new), `ComputeTargets/ScalarHistory.py`,
`ObjectFactories/ScalarHistory.py`, `config/`, `extract_common.py`, `plot_ScalarHistory.py`, `tools/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.22

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | The measures | `quanta_diagnostics.py`: the spectrum model, kernels, the measures; the sample masses; the `"quanta"` payload key | Opus | ⏸ `reflection-restart` 03 | ⬜ | | |
| 02 | The store | `ScalarHistoryBounce`, the sample and history columns, `FINGERPRINT_FORMAT` 7 | Opus | ⬜ | ⬜ | | |
| 03 | Exports | `histories.csv`, `fields.csv`, `bounces.csv` | Sonnet | ⬜ | ⬜ | | |
| 04 | Measure | about forty tuned histories by the tool route; the model's sensitivity; the cost | Opus | ⬜ | ⬜ | | |
| 05 | Rehearse | base and tip builds on the isolated cluster, interrupted and resumed; bitwise; by value | Opus | ⬜ | ⬜ | | |
| 06 | Figure tools | the grid figure and the per-history figure, from the exports | Sonnet | ⬜ | ⬜ | | |
| 07 | Close | the verification document, the measures for the paper, the handover for the rebuild, the indexes | Opus | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed.

Orchestrator notes, one prompt at a time, in `orchestrator/`.

### 1.1 Items

None yet.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Measures | each measure against direct quadrature (10⁻⁶); peak-collapsed against the scoping probe's method on two histories; golden histories bitwise; existing payload unchanged | 01 | ⬜ |
| Store | the round trip exact, bounce rows in order; α = 0 writes no row; fingerprint format 7 beside 6 | 02 | ⬜ |
| Export | the new export columns equal the stored record; existing `histories.csv` columns byte-identical | 03 | ⬜ |
| Measure | the sample measured; the seven of the scoping set beside source §3; sensitivity to the associated piece; the cost | 04 | ⬜ |
| Bitwise | every column of the base build bitwise equal in the tip build | 05 | ⬜ |
| R | the tip, interrupted and resumed, equals the straight build by value (bounce rows included) and 04's tool route | 05 | ⬜ |
| Figures | both tools render from 05's exports; their tests pass | 06 | ⬜ |

## 3. Active and unresolved issues

Opened by the planner on 2026-11-02, from the scoping (`source/scoping_2026-11-02.md`); the last two
opened the same day, after the plan was ruled, from the drain probe (`source/drain-probe_2026-11-02.md`).

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
- **`[00-reflection-restart-tree-fails-probe-histories]`** — opened 2026-11-02 (source
  `drain-probe_2026-11-02.md` §5). On `reflection-restart` at `2d50b95`, at the stored normalization:
  2197 and 2188 at their stored α = 10⁻³ fail after about 115 s in `classical_turning_point`
  (ln E = −306.7: a residual not monotone, alternating −inf and repeated values, which looks like float
  cancellation in V − V_floor), though both completed on `0209aac`; and 3873 (10⁻¹² M_P, β 1.11) without
  a drain stops at N = 43.76 at the guard "reflection requested inside the wall" (W/(π²/2) = 1.485).
  **Impact:** not investigated; histories of the low-α transition window may fail on the fixed tree
  where they succeeded before. **Next:** for `reflection-restart` 04, at the user's word (README §6.3,
  6).
- **`[00-the-parking-leftover-has-no-spectrum]`** — the energy drained at the parking switch (up to
  0.10 of ρ_χ at 4×10⁻³¹, β 1.06–1.107; ≤ 0.03 elsewhere) has no peak estimate, so no margin and no
  wall-exposure measure covers it; it is reported only as `unassessed_share`. **Impact:** not measured.
  **Next:** out of this campaign's scope; recorded for a later one.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

None.

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-01, after `inelastic-reflection` 12): CosmologyModels 26,
ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68. `c95650a` added one to
ObjectFactories, and `reflection-restart` will add more before 01 starts, so 01 measures the counts again
on its own tree before it changes anything.
