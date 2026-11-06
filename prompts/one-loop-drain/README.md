# Campaign — one-loop-drain (planned as quanta-diagnostics)

**Written:** 2026-11-02 on branch `quanta-diagnostics`, cut from `reflection-restart` at `3b8a553` (that
campaign's plan and its prompt 01, committed; none of its code), by Claude Opus 5.5, at the user's request:
"Yes, please now plan this campaign", after a scoping session the same day. U1–U7 and P1–P16 ruled by
the user the same day, all as proposed (§6).

**Re-planned 2026-11-02 around the drain law, and renamed `one-loop-drain`** (branch and folder, `d00fc01`),
at the user's rulings of §6.3. **§8 is the plan.** §§0–7 are the plan as first written and ruled, kept as
the record; they stand only where §8 says so.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the scoping record
[`source/scoping_2026-11-02.md`](source/scoping_2026-11-02.md), with the probe scripts and their
per-history summaries in [`source/probe/`](source/probe/); `inelastic-reflection` README §8 and its
prompt 08 (the peak and the margins) and 09 (the store); the briefing notes
`BriefingNotes/production_energy_ratio.tex` and `meff_and_wall_production.tex` (the classical spectrum)
and `KadanoffBaym/kb_partB_report.tex` §§ "The soft occupation", "The hard spectrum", "What is open"
(the two-loop toy), all in the repository *Chameleon PBH (David's stuff)*; the `reflection-restart` plan.
Added 2026-11-02, after the plan was ruled: [`source/margins-correction_2026-11-02.md`](source/margins-correction_2026-11-02.md),
[`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md) with its probe in
[`source/probe/drain/`](source/probe/drain/), and the briefing note `BriefingNotes/nonrelativistic_quanta.tex`
(the whole argument, with what is measured and what is only possible) and `BriefingNotes/wall_backreaction.tex`
§"The chain of estimates" (the one-loop inelastic fraction).

## 0. Why this campaign exists

### 0.1 What the store cannot say today

`inelastic-reflection` 08 gave each history three relativistic margins, ln(p/m) at the peak of each
generation's spectrum (a generation is the quanta of one injection): at birth, at every later turning
point (the wall's mass), and on the late tracker. 09 stored the worst of each and the share of ρ_χ
failing ln 10, **over the weighted generations only**, those holding at least 10⁻³ of K(N_final). The
generations themselves are dropped at the store. The scoping (source §§2–3) found:

- **The weighting hides the least relativistic quanta.** In the low-α transition window at laboratory M
  (β ≈ 1.06–1.2, α = 0.001), 80–98% of ρ_χ is born below T_J = 1 eV in 5,700–6,650 injections, each under
  the weight. Re-integrating two such histories: the tracker margin over all generations is 7.3 and 1.6
  (stored: 68 and 69); the reflection margin is −17.5 and −20.4 (stored: 17.9 and 17.5), with 84–92% of
  ρ_χ below zero (stored failing share: 0).
- **The expansion history is safe from it.** Between bounces those quanta stay relativistic: the
  matter-like excess E of ρ_χ over its massless value is ≤ 5×10⁻⁶ of ρ_χ, ≤ 4×10⁻⁹ of ρ_R. The earliest
  generations are the most relativistic (margins 72–82 e-folds today) because m·a falls by 20–35 e-folds
  over a history.
- **At their own bounce they are not.** The wall's mass exceeds the between-bounce mass by e²²–e⁵². The
  scoping read the negative reflection margins as earlier generations made non-relativistic by later
  bounces. **Corrected 2026-11-02**, by re-integrating history 2197 (β = 1.185, α = 0.001) from `main` at
  `0209aac` and keeping each generation's margins and the event that sets its reflection margin:
  - Q = k_ex·a falls from each generation to the next (6,638 of 6,647 steps; ln Q from +6.8 to −72.3).
    At a later bounce k, an earlier generation g has p/m = (Q_g/Q_k)(k_ex,k/m_k), which is never below
    bounce k's own birth margin. A reflection-margin failure is therefore a failure at birth: the last
    bounce leaves the first generation's margin at about +59.
  - The birth margin over all generations has median −15.7 and minimum −20.4. 72% of ρ_χ is born with
    ln(k_ex/m) < 0; a further 12% is born above 0 but meets a later bounce whose births are below it. A
    generation's reflection margin equals the next generation's birth margin to a median 0.003. The
    weighting hid this too (stored birth failing share: 0).
  - The cause is the classical turning point once the impact energy E falls well below V_0. The field
    then turns in the 1/φ tail, at φ_t ≈ M V_0/E, where k_ex ∝ E^{5/2} and ω_t ∝ E^{3/2}, and
    ln(k_ex/ω_t) ≈ ln(E/V_0) − ln 2√2 instead of the steep wall's ln ½. The 5,981 late classical
    generations (birth margin < −5) follow d ln k_ex/d ln ω_t = 1.667. Their birth margins put
    ln(E/V_0) at −15 to −19. The steep-wall spectrum (k_ex = λ_t E^{1/2}/2) is derived outside this
    regime, and every such turnaround at laboratory M is a floor reflection drained with w_gate = 1.
- **What a bounce does to quanta already present** is set by where they sit against that bounce's
  production band (around k_ex), not by p/m. A mode far above the band follows the bounce adiabatically
  (|β|² falls steeply above the peak), so a bounce neither rescatters it nor stimulates production into
  it. In the classical regime the band and the mass nearly coincide (k_ex ≈ ω_t/2). In the one-loop
  regime they do not (m_ta ≪ k_ex, ln(k_ex/m_ta) up to 32), so a relativistic quantum can still sit in
  a later bounce's band. Since Q falls by about 0.5% per bounce in 2197's late tail, about 100 earlier
  generations lie within half an e-fold of each bounce's peak. A crude occupation for them (each
  generation's ΔK in a shell one e-fold wide at its Q, n ≈ 2π² ΔK/Q⁴) is e³⁰–e¹⁷⁰. This does not
  contradict inelastic-reflection's small occupations, n ~ α e^{−4ℓ}, which hold for one-loop
  turnarounds (R ≫ 1, k_ex ≫ E^{1/4}): in 2197 the 209 one-loop generations (39% of ρ_χ) have
  ln n from −135 to −12. Once R < 1, k_ex < E^{1/4}, and the same αE in softer modes gives n ≫ 1: the
  first generation with n > 1 has ln R = −1.0, and the 61% of ρ_χ at R < 1 has n > 1. If that is right,
  the late modes are classical, the first-order n̄_j and ε_j of §1.1 are outside their validity there,
  and the late drain is outside the model's. The ledger treats all of it as passive radiation. The user
  has ruled the effect documented and quantified, not modelled (source §7). What §1.1's measure 4
  should become in the light of this is open for the user; the argument and its numbers are in
  [`source/margins-correction_2026-11-02.md`](source/margins-correction_2026-11-02.md).
  **Narrowed 2026-11-02** ([`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md) §1):
  being NR at the wall and being disturbed by the bounce are different thresholds,
  ln(Q_g/Q_k) < ln(m_k/k_ex,k) (15–20 e-folds wide in the tail) against ln(Q_g/Q_k) ≲ O(1). A quantum
  NR at the wall but outside the band keeps its occupation; what remains for it is the equation of
  state (measure 2) and an unmodelled backreaction n_χ m(φ). The n ≫ 1 at birth is a property of the
  fixed-α drain: one bounce from vacuum gives |β|² ≲ O(1) in its band, a fraction of about R e^{4ℓ} of
  E, and the classical-wall computation gives R·S(L).
- **The drain, not the redshifting (2026-11-02, §0.4).** A drain set by the one-loop inelastic
  fraction, scaled by A, with nothing drained at R ≤ 1, removes the late classical cascade in the two
  laboratory-M histories probed: the field parks after 10–95 one-loop bounces, and r_χ rises 1–10×.
  Parametric resonance is excluded wherever production is non-negligible (phase slip per bounce ≳
  (k_ex/H)·ΔN²), so the random-phase recursion, stimulated emission, is the whole multi-bounce model.

Seven histories cannot carry a statement about a grid of 3,846, and a figure cannot be made from what
the store keeps. This campaign stores what the run already computes and discards, adds the measures
the scoping defined, and makes the figures' tools.

### 0.2 What the code has today (on `main` at `0209aac`, unchanged at `3b8a553`)

- `history_margins` (`ComputeTargets/ScalarHistory.py`) returns every generation (`Generation`: N_turn,
  ln q, ln k_ex, ln m, ln R, share, weighted, and its three margins) and the `Margins` record; it runs
  in `integrate_history` at α > 0 on a model with a wall, after the integration.
  `tracker_log_m` and `field_log_m` give the tracker's and the field's mass at a state.
- `relativistic_margins` is the pure core; `DissipationValues` and `_DISSIPATION_MARGIN_FIELDS` carry
  the stored columns; `build_extra_data` puts the first failures and the shares in JSON.
- `ObjectFactories/ScalarHistory.py`: the `ScalarHistory` row (dissipation columns since format 6), the
  `ScalarHistoryValue` sample table (`rho_chi_over_rho_rad` its last column). `config/datastore.py`
  registers both in the drop group `scalar-history`; `config/sharding.py` shards both by `shard_key`.
  `FINGERPRINT_FORMAT` is 6, `VERSION_LABEL` 2026.8.0.
- `extract_common.py` writes `histories.csv` (`CSV_COLUMNS`, `DISSIPATION_CSV_COLUMNS`);
  `plot_ScalarHistory.py` writes each history's `fields.csv`.
- In the 2026.8.0 store: 1.43 M candidates over 3,718 successful histories, at most 6,674 in one; each
  history keeps 4,900–15,200 samples. Re-integration costs a median 12 s and a maximum of 12 min.

### 0.3 The order with `reflection-restart`

`reflection-restart` (live; plan `a566748`) changes `integrate_scalar_history`'s step control,
moves `VERSION_LABEL` to 2026.9.0, and ends with a verdict on what of the 2026.8.0 run is rebuilt, a
rebuild that is the user's. This campaign touches the same file (after the integration, not inside the
step loop) and changes the store's schema. Hence U1 and U2: this campaign's prompts are written and run
**after `reflection-restart` 03 lands**, on its fixed tree, and the rebuild of record waits for this
campaign, so that one rebuild carries both.

### 0.4 The drain question (2026-11-02, open for the user)

Added after the plan was ruled, from a session that continued the margins correction
([`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md); the briefing note
`BriefingNotes/nonrelativistic_quanta.tex` holds the whole argument). The user has said that whether
any of this belongs in this campaign is to be decided by what is right, not by the plan's wording;
§5's "no stored value moves" was written for a diagnostics-only campaign.

- **What is wrong with a fixed α.** The drained fraction should vary along the cascade and with M, and
  a fixed α cannot. At α = 10⁻³ at laboratory M it underestimates the early one-loop drains (one loop
  gives 0.08–0.26) and asks the late classical bounces for n ≫ 1, which one bounce cannot deliver. At
  large M, where R < 1 from the first impact, every stored α (0.01–0.15) is far above what a bounce can
  deliver.
- **The proposal examined.** α_eff = min(1, A·2.7/ℓ) where the one-loop chain has a solution
  (`wall_backreaction.tex`, ρ_prod/ρ_kin ≈ 4c/ℓ, a formal answer near the elastic end), zero (or R·S(L))
  at R ≤ 1, with A scanned: A = 1 is one loop, A → 0 elastic, the clamp fully inelastic.
- **What it does** (two laboratory-M histories, three at 10⁻¹² M_P, stored normalization, not re-tuned).
  - At laboratory M the field parks after 10 (A = 1) or 95 (A = 0.1) bounces, all one-loop. r_χ rises:
    2197's ΔN_eff goes from 0.0064 to 0.061 at A = 1. Each integration takes seconds instead of ten
    minutes.
  - At large M nothing is drained: about 300 elastic bounces, and ΔN_eff → 0 (3233: 0.335 → 0).
- **What is not settled.**
  - The gap 1 < R < 10^2.9, where the one-loop fraction is O(1) and the classical one is about 10⁻⁵ R.
    The probes never reached it.
  - The tail, where production at R < 1 has not been computed.
  - Whether the cascade parks before R ≤ 1 across the grid and at A < 0.1.
  - The parking model at physical M.
- **Found on the way.** Two integrator failures on the `reflection-restart` 03 tree, at the stored
  normalization (board §3, `[00-reflection-restart-tree-fails-probe-histories]`).

The decisions this leaves are listed in §6.3.

## 1. Scope

> **Superseded 2026-11-02 by §8.1–§8.3.** §§1–5 below are the plan as first ruled.

**In scope:**
- **The measures** (§1.1), computed after the integration from the payload's generations, the turning
  points, the samples and the ledger: margins over all generations; E(N) at every sample; the share of
  ρ_χ born late; and three wall-exposure measures at every bounce, with two per-history summaries.
- **The store**: a child table with one row per bounce, three columns per sample, about twenty columns on
  the history row. The existing columns and the JSON keep their meaning and their values.
- **The exports**: `histories.csv` gains the new columns; each history's `fields.csv` gains the sample
  columns; a new `bounces.csv` per history.
- **A measurement** on a sample of tuned histories through the tool route, including the worst
  transition-window histories, and a **registered rehearsal** under Ray showing that the new tree
  stores every pre-existing value bitwise and the new ones correctly through an interruption.
- **Two figure tools**, reading the exports: the grid figure of the margins and the wall-exposure
  measures, and the per-history figure of m·a, the generations' q, E and the measures per bounce.
- **The documents**: the measures defined for the paper, with their assumptions and limitations; the
  handover for the rebuild.

**Out of scope:**
- **Modelling** stimulated production, rescattering or the coherent (k, −k) build-up: they are measured
  under a stated model and documented (source §7). The drain, the ledger, the integration and every
  stored value of today stay as they are.
- **The rebuild** and the figures of record made from it: the user's (§7).
- The spectrum of the parking leftover (opened as an issue, §6.2 P15); the classical-regime birth margin
  of ≈ −0.7 by construction (recorded, not changed).
- `reflection-restart`'s files and decisions; the packages; `PRyM/`; `thirdparty/`.

### 1.1 The measures, defined

All momenta are comoving, q = p·a with a = e^N (Einstein frame); p/m is frame-independent. A generation
g has birth N_g, scale Q_g = k_ex,g a(N_g), energy ΔK_g, and the occupation (§6.2 P4)

    n_g(q) = A_g s(q/Q_g),  s(x) = s_dir(x) + s_assoc(x),  A_g = 2π² ΔK_g / (Q_g⁴ I),  I = ∫ x³ s(x) dx,

so that (1/2π²)∫ q³ n_g dq = ΔK_g. The measures:

1. **Margins over all generations**: `relativistic_margins`' worst margin, its N and its failing share,
   for each kind, with the weight ignored.
2. **The matter-like excess** at each sample, with m the field's own m_eff there:
   E(N) = Σ_{N_g ≤ N} ΔK_g G(m a/Q_g) / K(N),  G(y) = ∫ x³ s(x) [√(1 + y²/x²) − 1] dx / I.
   Per history: its maximum, the N there, its value today, and the maximum of r_χ E.
3. **Late production**: the share of K(N_final) in generations born at T_J below 1 eV.
4. **At each bounce j** (each candidate with an estimate, and each turning point of the reflection
   margin; m_j its mass there, K_j⁻ the K just before it), over the generations g born before it:
   - the **NR-at-wall share** S_j = Σ_g ΔK_g F(m_j a_j/Q_g) / K_j⁻, F(y) = ∫₀^y x³ s dx / I: the fraction of
     the ρ_χ present that is non-relativistic at the wall;
   - the **occupation overlap** n̄_j = ∫ q³ s(q/Q_j) Σ_g n_g(q) dq / ∫ q³ s(q/Q_j) dq: the occupation of the
     modes bounce j produces into, defined where bounce j is itself a generation (ΔK_j > 0); stimulated
     emission multiplies that drain by about 1 + 2n̄_j;
   - the **rescattered fraction** ε_j = Σ_g ∫ q³ n_g(q) 2 n_j(q) dq / ((2π²) K_j⁻): the fraction of the ρ_χ
     present changed by the bounce at first order with random phases (n_j the bounce's own occupation,
     |β_j|², from its ΔK_j; zero where ΔK_j = 0).
   **Under review (2026-11-02; §0.4, §6.3):** S_j largely repeats the birth margin and the Q table;
   n̄_j and ε_j are first-order, random-phase measures that apply only where n ≲ 1, and parametric
   resonance (the coherent build-up below) is now measured to be absent wherever production is
   non-negligible.
   Per history: max S_j, the number of bounces with S_j > ½, max n̄_j, max ε_j, and the two summaries
   **δ_stim** = Σ_j 2 n̄_j ΔK_j / K(N_final) and **δ_resc** = Σ_j ε_j K_j⁻ / K(N_final): the fractional
   change of the final ρ_χ that stimulated emission and rescattering would make, at first order, with
   random phases. These are the numbers the paper can quote, with their two caveats (the extrapolated
   spectrum, U4; coherent build-up would make them lower estimates).

The ratios of overlaps depend only on ln(Q_g/Q_j), so the kernels are tabulated once (P5) and the
bounce measures cost O(G²) table lookups per history (≈ 2×10⁷ at G = 6,674).

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-the-measures.md` | `ComputeTargets/quanta_diagnostics.py`: the spectrum model and its kernels, the measures of §1.1 on the payload's generations, turning points, samples and ledger; the per-sample masses; a `"quanta"` payload key; tests against direct quadrature and the scoping's probe | Opus |
| 02 | `02-the-store.md` | the child table `ScalarHistoryBounce`, the sample and history columns, `FINGERPRINT_FORMAT` 7, the registry, drop group, sharding and batch size, the read-back; tests in a temporary store | Opus |
| 03 | `03-exports.md` | `histories.csv`, `fields.csv`, a new `bounces.csv`; readers and their tests | Sonnet |
| 04 | `04-measure.md` | the measures on a sample of tuned histories by the tool route (no Ray): the seven of the scoping and about thirty across M, α and the β regions; the model's sensitivity (P4); the cost | Opus |
| 05 | `05-rehearse.md` | a registered rehearsal on the isolated cluster: a reference build at the campaign's base and the same grid at its tip, interrupted and resumed; every pre-existing value bitwise; the new values against 04's | Opus |
| 06 | `06-figure-tools.md` | `tools/plot_quanta_margins.py` (the grid) and `tools/plot_quanta_history.py` (one history), from the exports; tests; rendered on 05's store as a demonstration | Sonnet |
| 07 | `07-close.md` | the verification document, the measures for the paper, `architecture-summary.md` and `numerical-strategies.md`, `CLAUDE.md`'s counts, the handover for the rebuild, the issues, the indexes | Opus |

Written one at a time, each after the previous one lands and is reviewed. 06's tests need only 03's
export format, so 06 may be written while 05 runs; it renders on 05's store once that lands.

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Measures** | each measure agrees with a direct quadrature over the full spectrum to 10⁻⁶ relative on constructed generations; with the spectrum collapsed to its peak, E and the all-generation margins agree with the scoping probe's brute-force method (`source/probe/all_generations.py`) on the same integration of two histories; the golden histories pass bitwise; the weighted margins and every existing payload key are unchanged | 01 |
| **Store** | a history written and read back in a temporary store returns every new value exactly, its bounce rows in order; a history at α = 0 writes no bounce row and its new columns are NULL; the fingerprint guard carries format 7 beside 6 | 02 |
| **Export** | every new column of the exports equals the stored record on read-back; `histories.csv`'s existing columns are byte-identical for a store without the new data | 03 |
| **Measure** | the measures on the sample, the seven of the scoping set beside source §3 with the differences the integrator fix makes stated; the sensitivity to the associated piece (off, as measured, ×10); added wall time per history against its integration | 04 |
| **Bitwise** | on the rehearsal grid, every column of the base tree's store (history and sample rows) is bitwise equal in the tip's store | 05 |
| **R** | the tip's build, interrupted with `SIGTERM` and resumed, equals the straight build by value, bounce rows included, and its new values equal 04's tool route for the same histories | 05 |
| **Figures** | both tools render from 05's exports, and their tests pass on constructed exports | 06 |

## 3. Stores

- 01–04 and 06 open no store but temporary SQLite stores in `tempfile` directories, through
  `standin_pool` (`CLAUDE.md`).
- 04 runs histories through `tools/history_and_bbn.py`'s path (no Ray, no store), writing under
  `var/quanta-diagnostics/04/`; its sample's inputs come from the 2026.8.0 store read-only.
- 05 builds two registered stores, `var/datastores/quanta-diagnostics-05-base` and
  `…-05-tip` (and a resumed copy of the tip), on the isolated cluster, with `--campaign
  quanta-diagnostics --prompt 05`. The base build runs from a worktree at the campaign's base commit.
  No production or science store is opened for writing.

## 4. The interfaces

- **01 → 02.** The payload key `"quanta"`: a record of the per-history values, a tuple of bounce
  records in N order, and three per-sample arrays aligned with `"sample"`. Floats, ints, strings and
  None only, so that it crosses Ray.
- **02 → 03, 05.** The table, the columns and the read-back on `ScalarHistory`.
- **03 → 06.** The column names and units of the three exports.
- **04 → 05.** The tool route's values for the rehearsal's histories.
- **To the user.** 07's handover: the rebuild that carries these columns, and the two figures of record
  made from it in `science-runs/`.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", and:
- **No stored value moves.** The integration, the ledger, the drain, the weighted margins and every
  existing column, sample and JSON key are bitwise unchanged; the golden histories pass bitwise at every
  prompt. A measure that would need a value to move is a stop.
- **Measures, not bounds.** S_j, n̄_j, ε_j, δ_stim and δ_resc are recorded as measured. No threshold
  refuses or warns on them unless the user rules one; the one count (S_j > ½) is a description.
- **The spectrum model is one function** (`quanta_diagnostics.spectrum`), with its constants and their
  sources in one place, so that a later measurement of the one-loop shape changes one module.
- **Long runs are detached and recorded** (`CLAUDE.md`, "Long-running jobs"): 05's builds through the
  wrapper; 04's sample, if over a few minutes, detached with its output in `var/`.
- **Cluster discipline**: `tools/ray_cluster.sh start`/`stop` and the rehearsal lock, as `CLAUDE.md`
  says; exit 3 is a stop.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 Ruled by the user (U1–U7)

**Ruled by the user, 2026-11-02, all as proposed:** "U1 - fine; U2 - yes, delay the rebuild; U3 - fine;
U4 - fine; U5 - yes; U6 - yes; U7 - yes". So:
- **U1:** no prompt is written until `reflection-restart` 03 has landed; that branch is then merged into
  this one, and 01 is written on the merged tree.
- **U2:** the rebuild of record waits for this campaign's 07. `reflection-restart` 04's verdict says what
  is rebuilt; the build itself carries both campaigns' changes.
- **U3:** `FINGERPRINT_FORMAT` 7; `VERSION_LABEL` stays at whatever `reflection-restart` 03 sets.
- **U4–U7** as in the table.

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name, branch, timing | `quanta-diagnostics`, on a branch of that name cut from `reflection-restart` at `3b8a553` (plan only). Its prompts are written and run **after `reflection-restart` 03 lands**: `reflection-restart` is merged into this branch first, so 01 starts on the fixed integrator | a branch from `main`, run in parallel with `reflection-restart` (conflicts in `ScalarHistory.py` and in the labels; the rehearsal's base would then not be the fixed tree) |
| **U2** — the rebuild | the rebuild of record (`reflection-restart`'s verdict says what) **waits for this campaign's 07**, so that one rebuild carries the fix and the new columns. The cost is the delay of seven prompts | rebuild after `reflection-restart`, and re-run only the figures' histories through the tool route later (no grid figure from the store); or rebuild twice |
| **U3** — the labels | `FINGERPRINT_FORMAT` 6 → 7 (a new table, new columns); **`VERSION_LABEL` unchanged** (no value moves; it is 2026.9.0 after `reflection-restart` 03), provided no store of record is built at 2026.9.0 before this campaign lands (U2) | move `VERSION_LABEL` as well (2026.9.1) |
| **U4** — the spectrum model at every M | the shape in units of each generation's k_ex, whatever its estimate (classical, one-loop or flagged): s_dir ∝ x² below the peak at x_p = 0.62 (0.31 ω_t with k_ex = ω_t/2), exponential above, its slope fitted to the note's spectrum; s_assoc = 49 x_*³/x with x_* = 0.0058 (the toy's 50× at q = 0.0029 ω_t), not saturated at the mass. 04 reports the measures with s_assoc off and ×10 beside it | the associated piece fixed in absolute terms (c_N R ω_t/q with R the drained fraction), which ties it to the 1+1 normalisation; or direct only |
| **U5** — the sample masses | ln m_eff and ln m_tr at every sample **at every α** (natural-mode histories get the m·a curve too), NULL where the tracker's minimum is not found; E at α > 0 only (0.0 at α = 0, as r_χ) | the masses at α > 0 only (cheaper; no m·a for natural mode) |
| **U6** — the summaries | δ_stim and δ_resc (§1.1) stored and quoted as the paper's quantitative statement of the effect, with the random-phase caveat | the per-bounce values only, summaries made at figure time |
| **U7** — the figures | two tools in `tools/`, reading the exports, demonstrated on 05's store; the figures of record made from the rebuild by the user, in `science-runs/` | figure scripts written directly into the next run's folder |

### 6.2 Proposed by the planner (P1–P16)

Accepted by the user as their defaults, 2026-11-02 ("§6.2 P1 to P16 seem OK as their defaults").
- **P1 — the module.** `ComputeTargets/quanta_diagnostics.py`, pure functions on plain values (as
  `relativistic_margins`), plus one gatherer called from `integrate_history` beside `history_margins`,
  on the same condition and under the same `ComputationFailureError` rule (one warning line, never the
  history's failure; the line on `"margin_warnings"`).
- **P2 — once, not per tuning integration.** The measures run on the integration the tuning accepts,
  not on each of its up to 30 integrations: computed in `tune_history` (and `integrate_history` when
  called directly) after the last integration. 01 confirms where the payload is final.
- **P3 — all-generation margins** reuse `relativistic_margins` with the weight set to 0; the weighted
  record is computed exactly as today.
- **P4 — the shape's constants** come from `production_energy_ratio.tex` (x_p, the slope above the peak,
  fitted by 01 from the note's table and its stated 10⁻²⁰ at 5 ω_t, or from `BriefingNotes/scripts/` if
  it reproduces the spectrum) and `kb_partB_report.tex` (the ratio 50 at 0.0029 ω_t); each constant is
  quoted with its source in the module.
- **P5 — kernels.** G, F and the two overlap kernels as functions of ln(Q_g/Q_j) (or of ln y),
  tabulated once at import on a grid fine enough that interpolation error is below 10⁻⁶ relative (the
  Measures gate), with the asymptotes beyond the grid in closed form (G → (y²/2)∫x s dx/I as y → 0 and
  G → y∫x² s dx/I as y → ∞; F ∝ y³ as y → 0, set by the associated piece, and F → 1 above the peak).
- **P6 — bounces.** Every candidate (ΔK = 0 included, with no ε) and every turning point of the
  reflection margin that is not a candidate (plateau, and wall-side without a candidate), as
  `history_margins` gathers them; their mass as there. Rows at α > 0 on a model with a wall only.
- **P7 — the child table** `ScalarHistoryBounce`: `history_serial` (indexed), `shard_key`, `ordinal`,
  `source` (candidate, plateau, wall), `kind`, `resolved`, `N`, `log_T_Jordan` (GeV), `delta_K`,
  `share`, `K_before`, `estimate`, `log_k_ex`, `log_m`, `log_q`, `log_R`, `weighted`, `birth_margin`,
  `reflection_margin`, `reflection_margin_N`, `tracker_margin`, `tracker_margin_N`, `wall_nr_share`,
  `occupation_overlap`, `rescattered_fraction`; nullable where a source has no value. In the drop group
  `scalar-history`, sharded by `shard_key`, batch size 250, loaded only on request (as samples).
- **P8 — the sample columns** `log_m_eff_Mp`, `log_m_tracker_Mp` (nullable) and `rho_chi_excess` on
  `ScalarHistoryValue`.
- **P9 — the history columns**: `all_worst_<kind>_margin`, `all_worst_<kind>_margin_N`,
  `all_<kind>_failing_share` for birth, reflection and tracker; `below_weight_share` (now a column as
  well as JSON); `rho_chi_excess_max`, `rho_chi_excess_max_N`, `rho_chi_excess_today`,
  `rho_chi_excess_over_rho_rad_max`; `late_born_share`; `wall_nr_share_max`, `wall_nr_bounces`,
  `occupation_overlap_max`, `rescattered_fraction_max`, `stimulated_fraction` (δ_stim),
  `rescattered_total` (δ_resc). Under the `whole-history values on the parent row` rule.
- **P10 — the late threshold** is T_J = 1 eV, after recombination's onset, as the scoping used; a
  constant beside the margin constants.
- **P11 — 04's sample**: the seven histories of the scoping; the ten largest below-weight shares at
  laboratory M; three each at M = 10⁻³³, 10⁻³⁰ and in block C (β > 1.1); the four β regions at α = 0.01
  and 0.1. About forty, re-tuned on the campaign's tree. Wall time bounded at about two hours, detached.
- **P12 — the cost gate's figure**: 04 states the added time per history as a fraction of its
  integration (median and maximum); 05 states it per build. Over a quarter, median, is a stop and ask.
- **P13 — 05's grid** is `inelastic-reflection` 11's reference grid extended by two laboratory-M
  transition-window points at α = 0.001 and one block-C point, about 55 histories with BBN; the base
  build at the campaign's base commit, run from a worktree; the comparison by `tools/compare_stores.py`,
  extended in 05 to the bounce table if it does not read it.
- **P14 — tests.** In `ComputeTargets/tests/` (the measures, against quadrature and constructed
  generations), `ObjectFactories/tests/` (the round trip, α = 0, the fingerprint guard),
  `tools/tests/` (the exports' readers and the two figure tools). Each prompt records the suites' counts
  before and after; a count that falls is a stop.
- **P15 — issues.** Opened on this board at planning: `[00-weighted-margins-hide-the-least-relativistic-generations]`
  (closed by 02, when the all-generation values are stored); `[00-later-bounces-act-on-quanta-already-present]`
  (quantified by 04 and 05, documented by 07, and left open as a known limitation, as the user ruled);
  `[00-the-parking-leftover-has-no-spectrum]` (out of scope; open). Anything else found is recorded and
  opened, not fixed.
- **P16 — documents.** `.documents/quanta-diagnostics-verification.md` (new) holds 01's checks, 04's and
  05's measurements and the measures' definitions as the paper will use them; `architecture-summary.md`
  gets the table and the columns; `numerical-strategies.md` the spectrum model and its kernels;
  `CLAUDE.md` the test counts and one line on the new table.

### 6.3 Open for the user (2026-11-02, not ruled)

From §0.4 and [`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md) §6. Nothing is
written until these are decided; U1–U7 and P1–P16 stand until then, but several depend on them (§5's
"no stored value moves", U3's labels, U6's summaries, P15's issues).

1. **The drain.** Keep the fixed α, or replace it by α_eff = min(1, A·2.7/ℓ) with A scanned (α's stored
   key then carries A).
2. **R ≤ 1.** Drain zero, or R·S(L) as a stated upper extrapolation.
3. **The gap 1 < R < 10^2.9.** The interpolation between the one-loop and classical estimates, and how
   its effect is measured.
4. **Where it lands.** This campaign re-planned around the drain (the drain first, the diagnostics
   measuring it), or a campaign of its own. Either way, before the rebuild of record (U2).
5. **Measure 4** (margins-correction, "Open for the user"). Under a one-loop drain, n̄_j and ε_j apply
   wherever the probes reached (n ≪ 1). The random-phase recursion is the complete multi-bounce model,
   and a phase-slip check per bounce would document the absence of resonance. S_j: drop or re-describe.
6. **The two integrator failures** (board §3): whether they go to `reflection-restart` 04.

**Reviewed 2026-11-02** (a Claude Code session at the user's request: "review the current status, decide
whether the correct information is still being captured … then … whether the current order, structure,
and purpose of the prompt sequence is still correct"). The review put six decisions, numbered differently
from the list above; each is given here with the item above it answers. One fact found on the way: the
check "reflection requested inside the wall" that 3873 meets without a drain dates from `fc97233`
(2026-10-01, the integrator audit), not from `reflection-restart` 03, as the drain-probe record §5, the
briefing note and board §3 said.

| Review | Put as | Answers |
|---|---|---|
| 1 | re-plan around the drain law, as one campaign (the drain first, the diagnostics as its instrumentation); keep or change the name | 4 |
| 2 | keep the fixed α as a selectable law beside the one-loop one (it keeps a bitwise gate and the comparison) | 1 |
| 3 | the measures: keep the all-generation margins and the sample masses; cut S_j, ε_j, δ_resc and the per-sample E; replace the late-born share by the share of K drained in each regime; derive n̄_j and δ_stim in a tool; add the phase slip, the backreaction ratio n_χ m_k/E_k and the drain-law columns | 5 |
| 4 | store each bounce's inputs and derive whatever depends on the spectrum model outside the integrator | 5 |
| 5 | rule the gap law and R ≤ 1 after a measurement prompt; zero at R ≤ 1 by default, R·S(L) as a sensitivity | 2, 3 |
| 6 | 2197 and 2188 to `reflection-restart` 04 (is it 03's regression?); 3873 to this campaign's 01 | 6 |

**Ruled by the user, 2026-11-02**, in their words:
- **1:** "yes, replan around the drain law, as one campaign. `quanta-diagnostics` is now an opaque name.
  `one-loop-drain` is perhaps a better choice."
- **2:** "yes, OK, provided this does not cost significant effort to support a mode that has (mostly) been
  superseded. The instrumentation columns would have limited meaning in this mode."
- **3:** "agree".
- **4:** "yes, let's factor the spectrum model out of the integrator".
- **5:** "agree".
- **6:** "agree".

And on the science: "I was never taking a large `M` `Delta_Neff` seriously, so there is no problem with
this disappearing. … The science result is Fig. 6 at the lab-allowed value of `M`." (The review had
meant Fig 6a's large-M ΔN_eff constraint, drawn there for comparison.)

## 7. What this campaign hands to the user

> **Superseded 2026-11-02 by §8.7.**

- The measures, stored per bounce, per sample and per history, on the fixed integrator, with
  `FINGERPRINT_FORMAT` 7.
- Measured on about forty tuned histories and a rehearsal grid: the margins over all generations, E,
  and the wall-exposure summaries δ_stim and δ_resc, with their sensitivity to the associated piece.
- Two figure tools and their demonstration figures.
- The handover for the rebuild that carries all of it (U2), and for the figures of record made from it.

## 8. Re-planned 2026-11-02: the drain law

Written by Claude Opus 5.5 the same day, from the user's rulings of §6.3. This section is the plan; §§0–7
stand only where it says so.

### 8.0 Why the plan changed

The campaign was planned to store and measure what the run discards about the produced quanta, with no
stored value moving (§5). Two findings of the same day changed its object (§0.1's corrections, §0.4,
`BriefingNotes/nonrelativistic_quanta.tex`):
- the non-relativistic, highly occupied late quanta of the laboratory-M histories are a product of the
  **fixed-α drain**, which asks a late classical bounce (R < 1) for occupations one bounce cannot deliver;
- under a **one-loop drain**, α_eff = min(1, A·2.7/ℓ) with nothing drained at R ≤ 1, the two laboratory-M
  histories probed park after 10–95 one-loop bounces, every birth is relativistic by ℓ > 0, the
  occupations are about α e^{−4ℓ} ≪ 1, and parametric resonance is excluded. Most of §1.1's measure 4
  would then compute numbers near zero.

The open questions are now the drain law's: whether the cascade reaches the gap (1 < R < 10^2.9) or
R ≤ 1 anywhere on the grid, what happens for A < 0.1, how much ρ_χ the parking leaves. This campaign
implements the law, measures it across the grid, and stores the instrumentation that answers them.

**What it does to the science.** The science result is Fig 6 at laboratory M (the user, 2026-11-02),
and the one-loop law moves it: 2197's ΔN_eff today goes from 0.0064 to 0.061 at A = 1 and 0.011 at
A = 0.1 (§0.4, not re-tuned). At large M the law drains nothing and the dissipation-mode ΔN_eff vanishes,
which the user does not take seriously in either case (Fig 6a drew it for comparison only).

### 8.1 Scope

**In scope:**
- **The drain law** (§8.2): a one-loop law beside today's fixed α, selected per history, with A its
  strength; the fixed law bitwise as today.
- **The instrumentation** (§8.3): per candidate and turning point, a row of a child table; per sample the
  field's and the tracker's mass; per history the share of K drained in each regime, the drain counts and
  extremes, and the margins over all generations. Model-free, and written under the one-loop law only.
- **The derivation tool** (§8.3): the spectrum model (U4) and every measure that depends on it, computed
  from the exports, outside the integrator, so that a better spectrum needs a re-run of the tool, not a
  rebuild.
- **A measurement** of the law across the grid through the tool route, re-tuned, from which the user
  rules the gap law and the A values of record (§8.5, U13–U14).
- **The store, the keys and the drivers; the exports; a registered rehearsal; two figure tools; the
  documents and the handover for the rebuild.**
- **The 3873 failure** (`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`): every large-M
  history drains nothing under the new law, so all of them take the path on which 3873 stops.

**Out of scope:**
- Modelling stimulated emission, rescattering or the backreaction n_χ m(φ): measured by the tool and
  documented, not modelled (the user's ruling of source §7 stands).
- Production at R < 1 in the 1/φ tail, and the physics inside the gap: the gap is bracketed (§8.2) and
  the bracket's spread measured, not resolved.
- The parking model at physical M, and the spectrum of the parking leftover (its share is measured, §8.3).
- Instrumentation under the fixed law (the user's ruling 2: limited meaning there, no significant effort).
- The 2197/2188 failure at the fixed α, which goes to `reflection-restart` 04 (ruling 6).
- The rebuild of record and the figures of record (the user's, §8.7); `reflection-restart`'s files; the
  packages; `PRyM/`; `thirdparty/`.

### 8.2 The drain law

*(Superseded in part, 2026-11-05: the one-loop and gap rows are replaced by the measured law of §8.10.)*

At every candidate the code already classifies the turning point (`peak_estimate`, after the
integration): **one-loop** where ln R > 0 and `one_loop_turnaround` solves; **gap** where ln R > 0 and it
does not (`ESTIMATE_CLASSICAL_GAP`); **classical** where ln R ≤ 0; **unassessed** where there is no
assessment. Under the one-loop law that classification moves into the loop, before the drain, and sets

| regime | α_eff |
|---|---|
| one-loop | min(α_cap, A · c₁ / ℓ) · w_gate, with c₁ = 4c ≈ 2.7 (`wall_backreaction.tex`, "The inelastic fraction") and ℓ the turnaround's |
| gap | by the gap prescription: **zero**, or **edge**, A · c₁ / ℓ_edge with ℓ_edge = 4 (the probe's `PROBE_GAP=high`); 02 measures both; both kept, `zero` the default (U13, ruled 2026-11-04; P26) |
| classical (R ≤ 1) | zero (ruling 5); R·S(L) is computed by the tool from the bounce rows as an upper bound, not drained |
| unassessed | zero, counted |

The fixed law is today's α_eff = α · w_gate, unchanged. α = 0 (natural mode) is untouched by either.
`c₁`, `α_cap` and `ℓ_edge` are module constants with their sources quoted beside them. Until 03 the
gap prescription is a module attribute that 02 sets (the precedent is `reflection-restart` 01's
`_PROBE_OUTWARD_FIX`). U13 as ruled keeps both, with `zero` the default; 03 replaces the attribute by a key
(P26).

### 8.3 The measures (ruling 3) and where each is computed (ruling 4)

**In the integrator, under the one-loop law only** (model-free; the fixed law writes no bounce row and
leaves the new history columns NULL):
- **Bounce rows** (P6, P7 as revised): every candidate and every turning point of the reflection margin,
  with P7's columns less `wall_nr_share`, `occupation_overlap` and `rescattered_fraction`, plus `regime`,
  `ell` (one-loop only), `alpha_eff`, `w_gate`, `log_E` (the impact energy), `log_H` and whatever the tool
  needs for S(L).
- **History columns**: the share of K drained in each regime (one-loop, gap, classical, unassessed) and
  the parking leftover's share; the number of drains and of gap and unassessed candidates; ℓ_min; the
  least ln R at a drain; the margins over all generations (measure 1: the worst of each kind, its N and
  its failing share; P3).

**At every sample, under every law** (U5 as ruled): `log_m_eff_Mp` and `log_m_tracker_Mp` (nullable).
One code path; written under the fixed law too, since leaving them out there would be the extra effort.

**In the derivation tool, from the exports**: the spectrum model (U4, P4) and its kernels (P5, now only
G and the occupation overlap); per generation the occupation at birth n_g; per bounce n̄_j, per history
δ_stim, with rescattering bounded by it (≈ 2n) and the bound stated, not computed separately; E(N), its
maximum and r_χ E's; per bounce the phase slip (k_ex/H)·ΔN² and its minimum over producing bounces; per
bounce the backreaction ratio n_χ m_k/E_k (n_χ a³ ≈ Σ_g ΔK_g/Q_g) and its maximum; the R·S(L) bound.

**Cut:** S_j and its counts; ε_j and δ_resc; the per-sample E column; the late-born share (P10, which the
regime shares replace).

### 8.4 Prompts

*(Amended 2026-11-05: 02c and 02d inserted, and 03 revised, by §8.10.)*

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-the-drain-law.md` | the law and A as inputs of the integration and the tuning (keys not yet stored); the classification in the loop; α_eff by regime; the gap prescription as a module attribute; a `"drain"` payload key with each candidate's regime, ℓ, α_eff, w_gate, ln E, ln H; the tool route's options; the 3873 failure investigated and, if it is in this code, fixed; tests | Opus |
| 02 | `02-measure-the-law.md` | about forty histories by the tool route, re-tuned (§8.5): A ∈ {0.01, 0.1, 1}, both gap prescriptions; regime shares, gap entry, ℓ_min, the parking leftover, the R·S(L) bound from the payload; Y_p, D/H and ΔN_eff against the fixed law; tuning integrations and wall time. Then the user rules U13–U14 | Opus |
| 02b | `02b-measure-wide-A.md` | inserted 2026-11-04 at the user's request (§8.6): 02's thirty histories at A ∈ {0.05, 0.5, 1.5, 2, 5, 10}, both gap prescriptions, with S2 generalised to every A; 02's lines at A = 0.01, 0.1 and 1 reused after a reproduction check; the first impact's drained fraction and the cap; the handover for U14 at nine A | Opus |
| 03 | `03-the-store.md` | the keys (U8), `ScalarHistoryBounce`, the sample and history columns, the gap prescription as a key (U13, P26), `FINGERPRINT_FORMAT` 7, `VERSION_LABEL` (U12), the registry, drop group, sharding, batch size, read-back; tests in a temporary store | Opus |
| 04 | `04-drivers-and-exports.md` | `main.py`'s and the plot scripts' options for the law (the match against stored keys, as `--alpha-values`); `histories.csv`, `fields.csv`, a new `bounces.csv`; readers and tests | Sonnet |
| 05 | `05-derivation-tool.md` | the spectrum model as one module outside the integrator (`ComputeTargets/quanta_spectrum.py`, pure), and `tools/quanta_measures.py` reading the exports and writing the derived measures per bounce and per history; tests against direct quadrature; run on 02's histories | Opus |
| 06 | `06-rehearse.md` | registered builds on the isolated cluster: the fixed law at the campaign's base and at its tip, bitwise; the one-loop law at the tip, interrupted and resumed, by value and against 02's tool route | Opus |
| 07 | `07-figure-tools.md` | the grid figure (regime shares, ℓ_min, the margins, δ_stim) and the per-history figure (m·a, the generations' q, the bounces), from the exports and 05's output; tests; rendered on 06's store | Sonnet |
| 08 | `08-close.md` | `.documents/one-loop-drain-verification.md`, the law and the measures for the paper, `architecture-summary.md`, `numerical-strategies.md`, `CLAUDE.md`, the handover for the rebuild, the issues, the indexes | Opus |

Written one at a time, each after the previous one lands and is reviewed. 01 is written after
`reflection-restart` 04 lands and that branch is merged here (U1, extended by ruling 6). 05 needs only
04's export format, and 07 only 05's output format, so either may be written while the prompt before it
runs.

### 8.5 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Law** | under the fixed law every golden history (plain, tuned and reflecting) passes bitwise and the payload is unchanged but for the new key; under the one-loop law 2197 and 2188 at the stored normalization reproduce §0.4's table (A = 1, 0.1; drains, parking N, r_χ), with any difference the merged tree makes stated; 3016 and 3233 drain nothing; 3873's failure explained | 01 |
| **Measure** | the sample measured at both gap prescriptions and three A, re-tuned; the regime shares and the gap's reach stated across M and β; the spread of Y_p, D/H and ΔN_eff between the prescriptions; the cost | 02 |
| **Wide** | 02's sample measured at 02b's six A, with the reproduction check bitwise; the regime shares, the first impact and the cap across nine A; the cost; the handover for U14 | 02b |
| **Store** | the round trip exact, bounce rows in order; the fixed law and α = 0 write no bounce row and NULL new columns; the fingerprint guard carries format 7 beside 6 | 03 |
| **Export** | every new export column equals the stored record; `histories.csv`'s existing columns byte-identical for a store without the new data; the drivers refuse a law that does not match the store | 04 |
| **Derive** | each derived measure agrees with direct quadrature to 10⁻⁶ relative on constructed generations; with the spectrum collapsed to its peak, E and the margins agree with `source/probe/all_generations.py` on the same integration | 05 |
| **Bitwise** | the fixed law's build at the tip equals the base's in every pre-existing column, history and sample rows | 06 |
| **R** | the one-loop build, interrupted with `SIGTERM` and resumed, equals the straight build by value, bounce rows included, and 02's tool route for the same histories | 06 |
| **Figures** | both tools render from 06's exports; their tests pass on constructed exports | 07 |

### 8.6 Decisions

**Standing from §6:** U1 (extended: 01 waits for `reflection-restart` 04, not 03); U2 (the rebuild of record
waits for 08); U4 (the spectrum shape, now in the tool); U5; U7 (the figures, now 07's); P2–P4, P6, P12,
P14. **Revised:** U3 (values move under the one-loop law, so `VERSION_LABEL` moves: U12); U6 (δ_stim is
derived by the tool, δ_resc cut); P1 (the integrator gathers the model-free values only; the spectrum is
in its own module, 05); P5 (G and the overlap only); P7–P9 (§8.3); P11 (02's sample, §8.5 below); P13
(06's grid, both laws); P15 (board §3); P16 (the verification document is
`one-loop-drain-verification.md`). **Withdrawn:** P10.

**For the user to rule now (U8–U12).** **Ruled by the user, 2026-11-02, all as proposed:** "U8 - agree; U9 - yes,
keep it; U10 - 0.99 is OK; U11 - OK; U12: OK". The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U8** — the keys | the `alpha_value` table gains `drain_law` (`fixed` or `one_loop`); its value is α under the fixed law and A under the one-loop law; α = 0 rows are law `fixed` and unchanged; A = 0 refused under the one-loop law (that is natural mode) | a table of its own (`drain_value`: law, A) beside `alpha_value` |
| **U9** — the gate under the one-loop law | kept: α_eff = min(α_cap, A·c₁/ℓ)·w_gate, the law setting the amplitude and the gate the half-oscillation's adiabaticity, as today; 02 reports how often w_gate < 1 at a one-loop candidate (the probe left the gate out) | no gate under the one-loop law |
| **U10** — the cap | α_cap = 0.99, as the probe, since the code refuses α ≥ 1 | 1 − 10⁻⁶ |
| **U11** — 02's sample | 2197, 2188 and the ten largest below-weight shares at laboratory M; three each at M = 10⁻³³ and 10⁻³⁰; a ladder of five M between 10⁻³⁰ and 10⁻¹² chosen where ln R at the first impact crosses zero; 3016, 3233, 3873; the four β regions at one laboratory M. About forty, each at A ∈ {0.01, 0.1, 1} and both gap prescriptions, re-tuned; detached, bounded at about three hours | a smaller sample at A = 0.1 and 1 only |
| **U12** — the labels | `FINGERPRINT_FORMAT` 7 and `VERSION_LABEL` 2026.10.0 in 03 | `VERSION_LABEL` 2026.9.1 |

**For the user after 02 (U13–U14):** the gap prescription of record (U13), from 02's spread; the A values
of the rebuild of record (U14). **U13 ruled by the user, 2026-11-04:** "Please keep both, but make `zero` the default in production". Both
prescriptions are kept, and `zero` is the default (P26 below). **Prompt 02b inserted at the user's
request, 2026-11-04,** before U14 is ruled: "Can we try `A \in { 0.05, 0.1, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0 }`?", and then "Running a few histories where the condensate loses most or almost all of its KE on the first impact might be worth doing, to get a measurement of what happens when the collision is mostly inelastic. A=5 and A=10 sound like they probe slightly different configurations in this regime, so leave those. Let's test first using a prompt 02b". **U14 ruled by the user, 2026-11-04,** after 02b:
"In that case, it seems to me that our original list `A \in { 0.05, 0.1, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0 }` covers the required region adequately. I see no reason to change it, unless you have an objection." The orchestrator raised no objection. The rebuild of record samples
A ∈ {0.05, 0.1, 0.5, 1, 1.5, 2, 5, 10} under the one-loop law; A = 0.01 is not among them.

**Proposed by the planner (P17–P20), the defaults unless the user says otherwise:**
- **P17 — the law's constants** (c₁, α_cap, ℓ_edge) beside the margin constants in `ScalarHistory.py`,
  each with its source; c₁ quoted from `wall_backreaction.tex`.
- **P18 — the classification once.** The one-loop law classifies each candidate in the loop; the
  post-integration `peak_estimate` reuses that result where it exists, so the one-loop solve is not run
  twice. Under the fixed law `peak_estimate` runs as today.
- **P19 — unassessed candidates** drain zero under the one-loop law and are counted; 02 reports how many.
- **P20 — the gap prescription's name** in the payload and the logs, so that 02's two runs of one history
  are told apart without the module attribute.

**Proposed by the orchestrator after U13 (P26), the default unless the user says otherwise:**
- **P26 — the prescription as a key.** Since both prescriptions are kept, a one-loop history's prescription
  changes its values and so is a key: a `gap_prescription` column of `alpha_value` beside U8's `drain_law`,
  `zero` or `edge`, with no prescription under the fixed law (03 states how the key stays unique there). The
  module attribute becomes an argument beside `drain_law` (P22), default `zero`. `main.py` gains
  `--gap-prescription`, default `zero`, and the plot scripts match it as they match `--alpha-values` (04).
  The tool route keeps its `--gap-prescription`, default `zero`.

### 8.7 What this campaign hands to the user

- The one-loop drain law beside the fixed one, with both gap prescriptions (`zero` the default, U13) and the
  A values ruled from a measurement across the grid.
- Per bounce, per sample and per history, the model-free instrumentation of the law, stored under
  `FINGERPRINT_FORMAT` 7; and a tool that derives the spectrum-dependent measures from the exports.
- Two figure tools and their demonstration figures.
- The handover for the rebuild of record (U2), at the A values of U14, and for the figures of record made
  from it, Fig 6 at laboratory M first.

### 8.8 After `reflection-restart` closed (2026-11-02)

`reflection-restart` 04 landed on 2026-11-02 (`ac8ce15`, reviewed `25b1242`), and that campaign closed. Its
branch was merged here the same day (`1561b60`), as U1 requires: the merged tree's computing code is
`reflection-restart`'s, with F3 and the guard, `VERSION_LABEL` 2026.9.0 and the scanner
`tools/outward_step_scan.py`.

Its 04 gave verdict (ii) on `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`.
`reflection-restart` 03's F3 moves 2197 and 2188 onto a limit of the wall search that predates it: a root
below 17 quanta of ln V's rounding cannot be found, and the search raises. The issue's measurements are
on `reflection-restart`'s board §3.1 and in its verification document §3. That campaign proposed this
campaign's 01 as the fix's destination.

**Ruled by the user, 2026-11-02:** "Add the wall-search fix to `one-loop-drain` 01." So 01 takes the
fix, under the Law gate, beside the drain law and 3873 (§8.4's row 01, extended). The issue went back
to this board's §3.

**Proposed by the planner for 01 (P21–P25), the defaults unless the user says otherwise.** Prompt 01 is
written on them.
- **P21 — the form of the wall-search fix.**
  - **Where:** in `AbstractModel.classical_turning_point`, after its test `r_0 >= 0` (which returns
    None) and before the search.
  - **The rule:** return None when ln E is below the least finite value that `_log_V_minus_floor` can
    return at this ln V_floor. The turning point then lies inside the region the search reads as −∞,
    and cannot be resolved.
  - **Why nothing else moves:** every such call raises today. Every finite residual is positive, so the
    outer end stays −∞. The rule changes only calls that raise, so no history that succeeds today moves,
    and the fixed law stays bitwise.
  - **What the step loop does with None:** it already reads None as "no decision", and the infall's
    latest assessment stands. At the eleven calls measured, the field is far out in the tail, where no
    decision would be made.
  - **Not chosen:**
    - a closed-form hook for ln(V − V_floor), which would move the digits of every wall search and so
      lose the fixed law's bitwise gates (Law, here, and 06's Bitwise);
    - reporting a root inside the −∞ region, which changes the search that the one-loop turnaround
      also uses, for the same set of calls.
  - **Accounting:** the calls it answers are counted in the `"drain"` payload key.
- **P22 — the law's interface.**
  - `integrate_scalar_history`, `integrate_history` and `tune_history` take `drain_law`: `"fixed"`, the
    default, or `"one_loop"`. It sits beside `alpha`, which carries A under the one-loop law, as U8's
    keys will.
  - Under the one-loop law, A must be finite and > 0. A = 0 is refused, since it is natural mode. There
    is no upper bound, because α_cap clamps.
  - `compute_scalar_history` and `main.py` refuse the one-loop law until 03 and 04 store its keys, so
    that no one-loop history can be written under a fixed-law key.
- **P23 — the constants.**
  - c₁ = 2.7 exactly. It is the note's own figure: `wall_backreaction.tex`, "The inelastic fraction",
    gives 4c with c ≈ 0.68 and writes 2.7. It is also the probe's, so that the Law gate compares with the
    probe directly.
  - α_cap = 0.99 (U10), and ℓ_edge = 4 (the probe's `PROBE_GAP=high`).
- **P24 — ℓ ≤ 0.** A one-loop solution with ℓ ≤ 0 is classified gap, with its reason, since A·c₁/ℓ has
  no meaning there. 01 reports whether any arises.
- **P25 — the tool route.** `tools/history_and_bbn.py` gains `--drain-law`. Until 03 it also gains
  `--gap-prescription`, which sets the module attribute of §8.2 in its own process, as
  `reflection-restart` 01's `--variant` set its switch. 03 removes the attribute; the option stays, default `zero` (U13, P26).

### 8.9 The side-campaign `crossover` (2026-11-04)

After 02b, the small-A failures were diagnosed (board §3,
`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`): the field never parks before today. While discussing
that, the user and the orchestrator agreed that the one-loop fraction 2.7/ℓ is not reliable when ℓ becomes small
(`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]`). The user's words: "It sounds much more reasonable that the
production fraction decreases as `ell -> 0`. What would be involved in testing the crossover law? Now we need the mode
integral with the mass `m_ta` in the propagator, so solving the 1-loop system self consistently becomes more
complicated."

**Ruled by the user, 2026-11-04:** "Please record the diagnosis and also our belief that the one-loop formula is not
reliable when `ell` becomes too small. Then yes, I'd like to run this as a side-campaign. To keep things simple I let's
try to land it before doing prompt 03. Create a directory in `source/` to hold the scripts and other outputs. Then
proceed with stage 1."

**What it is.** A research side-campaign, run by the orchestrator in this campaign's `source/crossover/`. It measures
the production fraction through the crossover from the one-loop regime (ℓ ≳ 4) to the classical wall (R ≤ 1), with the
mass in the mode functions. It changes no production code. It has three stages:
1. **Stage 1, the bracketing test-field runs.**
   - The classical bounce with no backreaction, which must reproduce the classical-wall limit (S(L),
     `production_energy_ratio.tex`).
   - Massive-mode production on the Born scheme's self-consistent trajectories (`wall_backreaction.tex`,
     `scripts/return_fraction.py`), set against the scheme's own Born production, where those trajectories exist
     (ℓ ≳ 1.6).
2. **Stage 2, the self-consistent solve.** The background with the one-loop tadpole from mode functions that carry the
   mass, renormalized against the instantaneous adiabatic vacuum. It must overlap the Born table at ℓ ≈ 4–8 and the
   classical limit as the backreaction vanishes. It conserves energy, and reports the loop parameter.
3. **Stage 3, the scan and a fit.** The fraction against ℓ (and R, L), and whether one function of one variable
   replaces 2.7/ℓ and the gap prescriptions.

**What it decides.** Nothing by itself. Whether the drain law changes, and so whether 02's and 02b's measurements at
A ≤ 0.1 are superseded, is the user's ruling once stage 3 is in. 03 waits for it, at the user's word.


### 8.10 The measured drain law (2026-11-05)

The side-campaign `crossover` (§8.9) closed with a drain law measured by a self-consistent one-loop solve with
massive modes (`source/crossover/README.md`, stage 2 onwards; its fits in `fit_law.py`, its figure
`fig/drain_law.png`). The user adopted it in place of 2.7/ℓ and of both gap prescriptions.

**Ruled by the user, 2026-11-05** (the user's words; the proposals they answer are in the conversation of
2026-11-04/05 recorded in `source/crossover/README.md`):
- **The law (U13 re-ruled).** "I think this model is close enough to the analytic 1-loop understanding that I am
  willing to use it." The model is "the full `ln L`-dependent exponential for `ell > 1`, the 0.9 plateau for
  `ell < 1` down to the gap, and your fitting function in the gap". This supersedes U13 as ruled on 2026-11-04
  (both prescriptions kept, `zero` the default).
- **The A values (U14 stands).** "Yes, let's keep the original list of `A` to check": A ∈ {0.05, 0.1, 0.5, 1, 1.5,
  2, 5, 10}. Under the adopted law R-632 (β = 1.0) and the 5 × 10⁻²⁰ M_P ladder do not park at A ≤ 0.1 (board §3),
  as under the old law; the user keeps A = 0.05 regardless.
- **The key (decision 1).** "agree", to one stored key naming the drain model and its version (for example
  `one-loop-measured-v1`), with the fixed law another value of it. The law can then change later without a change of
  schema. It replaces P26's gap-prescription key, and the separate law key of U8 is folded into it (03 designs it).
- **`zero` and `edge` (decision 2).** "agree, remove these. They are dead now." `_GAP_PRESCRIPTION`, its constants,
  `DRAIN_ELL_EDGE` and `tools/history_and_bbn.py --gap-prescription` are removed. 02's and 02b's records stay
  reproducible from their commits.
- **The classical regime (decision 3).** "if it's cheap, keep the table. I agree that zero here is also
  defensible, however." At R ≤ 1 the drain is R·S(L), with S(L) tabulated.
- **Dispatch.** "Don't dispatch it, however; this session has become long, and I'll do that in a fresh context."

**The law.** Under the one-loop law each candidate drains α_eff = min(α_cap, A·f)·w_gate, with α_cap = 0.99 (U10) and
w_gate as today (U9). L = M/φ_t and ln R are the assessment's (`ClassicalTurningPoint`); ℓ is the turnaround's.

| regime | f |
|---|---|
| one-loop, ℓ ≥ 1 | min(0.95, 1 − exp[−2.69/(ℓ + 0.9691 − 0.41 ln L)]), and 0.95 where ℓ + 0.9691 − 0.41 ln L ≤ 0 |
| one-loop, ℓ < 1 | 0.9 |
| gap (ln R > 0, no chain) | 1 − exp[−0.0198 (ln R − 1.5601)^1.5472 (1 + 0.841 ln L)] for ln R > 1.5601; 0 below |
| classical (ln R ≤ 0) | R·S(L), S from `source/crossover/out/stage1a_S_of_L.txt` |
| unassessed | 0 |

- **Sources.** The one-loop and gap constants are `source/crossover/out/fit_law.txt`'s fits to every stage-2 point
  (one-loop within 0.074 of 27 points, gap within 0.062 of 34). 2.69 is the Born coefficient (`wall_backreaction.tex`,
  "The inelastic fraction"), held fixed. The cap 0.95 and the plateau 0.9 are judgement, as the user asked for
  qualitative behaviour (stage 2 measured 0.88–0.98 and 0.80–0.97 there).
- **S(L)** is stage 1a's table at L = 0.3–134 (21 values), interpolated linearly in (ln L, ln S). It is held at
  S(0.3) below and continued as S(134)·(134/L)² above (L²S tends to a constant, `production_energy_ratio.tex`).
  The table is 0.1% high from its quadrature (stage 2, V2q); that is immaterial here.
- **The reference implementation** is `fraction_adopted` in `source/crossover/measured_law_map.py`. Crossover's
  outputs under it (`out/adopted-A0.05/`, `out/adopted-floor/`) are what 02c reproduces.

**U15, models with n ≠ 1** (ruled 2026-11-05, below). Stage 2 measured the n = 1 wall, V₀e^{M/φ}, only, and `main.py`
accepts `--n-values`. The options:
- (a) under the one-loop law a model with n ≠ 1 is a `ValueError`, before any integration, until it is measured;
- (b) apply the law with L = ln(1 + E/V₀), stage 2's definition, which equals M/φ_t at n = 1, unmeasured at n ≠ 1;
- (c) stage-2 runs on the n = 2 wall first.

The planner recommends (a) if no rebuild of record uses n ≠ 1. 02c's gate required U15 ruled.

*(2026-11-05, measured at the user's request: stage 2 on the n = 2 wall (`source/crossover/README.md`, "After
closing: the n = 2 wall"). With L = ln(1 + E/V₀) the adopted law is within 0.125 of the n = 2 one-loop points and
within 0.10 of the gap points that histories can reach (rms 0.05). With L = M/φ_t it is low by about 0.1. Option (b),
with L = ln(1 + E/V₀) at every n, is therefore supported. It equals M/φ_t at n = 1, so the law there is unchanged.
The planner's recommendation moves to (b).)*

**U15 ruled by the user, 2026-11-05:** "OK, let's take option (b) for now." Under the one-loop law the drain applies
at every n, with L = ln(1 + E/V₀); at n = 1 that is M/φ_t, and the law there is unchanged. "For now": a later
measurement at other n may revise it, through the drain-model key's version (decision 1).

**The re-plan.** Prompts 02c and 02d are inserted before 03, and 03 is revised.

| # | Prompt | Covers | Model |
|---|---|---|---|
| 02c | `02c-the-measured-law.md` | the adopted law in `ScalarHistory.py`, replacing 2.7/ℓ and both gap prescriptions; S(L); U15 as ruled; the `"drain"` record names the model version and records L and ln R per candidate; `--gap-prescription` removed; tests; the reproduction of crossover's adopted-law maps | Opus |
| 02d | `02d-measure-the-measured-law.md` | 02b's sample re-tuned under the adopted law at U14's eight A: tuning success, regime shares, parking, ΔN_eff, Y_p, D/H, against 02/02b; the cost. It replaces 02's and 02b's one-loop measurements | Opus |
| 03 | `03-the-store.md` (revised) | as §8.4, with one drain-model key (decision 1) in place of the gap prescription key (P26) and the separate law key | Opus |

The gates: **Measured** (02c) holds when the fixed law is unchanged bitwise (the goldens), the law equals the
reference implementation at synthetic inputs, and crossover's adopted-law maps are reproduced candidate by
candidate. **Remeasure** (02d) holds when the sample is measured at eight A, re-tuned, with the failures stated.

**Ruled by the user during 02c, 2026-11-05** (the user's words; the questions are orchestrator note 02c's, and its
ruling 4's stop):
- **The gap below L = e^{−1/q} ≈ 0.304.** The gap function above is negative there, where 1 + 0.841 ln L < 0, and so
  is the reference implementation. "I think probably we set `f = 0` there." In the gap f = 0 where 1 + q ln L ≤ 0.
  No gap candidate of 02c's runs reaches it (least L 2.578). The M = 10⁻³³ M_P histories are the likeliest place;
  02d counts it from `DrainCandidate.L`.
- **L is the model's.** L = ln(1 + E/V₀) is the exponential family's wall exponent (M/φ_t)^n. Computed in
  `ScalarHistory.py`, it coupled the integration to the form of the potential. "I suggest we move `L` into the model
  class, with no default. Then when a new potential is implemented, breakage is loud, and the implementer has to think
  about what they are changing." `AbstractModel._drain_L` has no default. `ExponentialModel` defines it, and under the
  one-loop law the three entry points refuse a model that does not.
- **The maps' headers.** The production L and the reference's M/φ_t differ by at most 3.3 × 10⁻¹⁴. Carried through the
  integration, that moves the full-precision `parking_N` and `N_final` of three of the four maps in their last digits
  (about 5 × 10⁻¹⁵ relative). Every candidate row is equal. "Decision 2: yes": this is the stated exception, not a
  stop.

The fitted constants and S(L) are themselves measured on this family (log 02c, "Observations not acted on"); moving L
into the model does not make them model-independent.

### 8.11 Before 03 (2026-11-06)

Reviewed by Claude Opus 5.5 at the user's request: "Please review the `one-loop-drain` campaign. We should now be ready
to write prompt 03. If you agree, please do so."

**Ready.** 02d landed and was reviewed (`98b1601`, `f830085`), with the Remeasure gate held. The tree is clean, and
no run is live (`runregistrykit list`: 23 finished). Every ruling 03 needs is in: U8 (the key on `alpha_value`), U12
(`FINGERPRINT_FORMAT` 7, `VERSION_LABEL` 2026.10.0), U5 (the sample masses), ruling 2 (no instrumentation under the
fixed law), and decision 1 of §8.10 (one versioned drain-model key, which "03 designs"). The questions 02d's handover
left for the user (which (β, M, A) the rebuild builds, and C-3873's tuning cost) are the rebuild's, not 03's.

**Stale beside §8.10, for the prompts after 03** (not acted on here):
- §8.3 and §8.4 row 05: "the R·S(L) bound" is no longer a bound. Under decision 3 the classical regime drains R·S(L),
  and the bounce rows carry it as α_eff. 05 needs no separate computation of it.
- §8.5 gate R: "02's tool route" should read 02d's, which replaced 02's and 02b's one-loop measurements.
- §8.4 row 04: the option it adds selects the drain model (decision 1), not a gap prescription (P26, superseded).

**Proposed by the planner for 03 (P27–P32), the defaults unless the user says otherwise.** Prompt 03 is written on them.
- **P27 — where the key lives.** On `alpha_value`, as U8 ruled. Its column `drain_model` takes the place of U8's
  `drain_law`, since decision 1 folds the law into the model key.
  - A row is (value, drain_model), and the pair is unique. The value is α under `fixed` and A under
    `one-loop-measured-v1`. α = 0 exists only under `fixed`, and the one-loop model refuses A ≤ 0.
  - The law follows from the model. The stored string is the code's current `DRAIN_MODEL_*`, so a later version of the
    law is a new value. A lookup made under it finds no row of the old version, as a lookup under a new
    `VERSION_LABEL` finds none of the old label.
  - A lookup that names no model asks for `fixed`. So `main.py`, the plot scripts and the tools are unchanged until 04,
    which adds their option, and none of them can store a one-loop history before then (P22's intent).
  - Not chosen: a table of its own (U8's rejected alternative); a column on `ScalarHistory` (it would key the history
    twice over the same drain).
- **P28 — the guard.** `ScalarHistory.store()` refuses a success payload whose `DrainRecord.model` is not its
  `alpha_value`'s `drain_model`.
- **P29 — the bounce table.** `ScalarHistoryBounce`, one row per candidate and per turning point of the reflection margin
  that is not a candidate, under the one-loop law only (prompt 03 §1.3 lists the columns).
  - It is written, read, validated and pruned wherever `ScalarHistoryValue` is, and loaded with the samples.
  - The history row gains `bounce_rows`, which `validate` checks as it checks `z_samples`.
  - The table has no `inventory_spec`, as `ScalarHistoryValue` has none. DatastoreKit's `InventorySpec` names a single
    value table (`values`), so the inventory does not count bounce rows. That is recorded, not worked around (`CLAUDE.md`,
    the packages).
- **P30 — the all-generation margins** are taken from the generations `relativistic_margins` already returns, which
  carry every generation's margins. They are not computed by a second run of the core, which is O(G·E) and would add a
  pass over 13 000 candidates on the ladder's classical tails.
- **P31 — the sample masses** are computed once per stored history, on the payload `tune_history` returns, with the
  accepted integration's realization (P2). They are not computed on every tuning integration (C-3873 takes 28).
- **P32 — no migration.** A format-6 store is read with the code of its own commit, as for every earlier format and label
  (`config/version.py`).

### 8.12 Before 04 (2026-11-06)

Written by Claude Opus 5.5 after 03 landed and was reviewed (`fe4c917`, `50e93c7`), at the user's request: "Please write
prompt 04".

**Ready.** The Store gate holds, and the tree is clean. 03's log §7 hands 04:
- the key, `alpha_value` = (value, `drain_model`), with `fixed` where no model is named;
- the history's drain record (`ScalarHistory.drain_history`, readable under `_do_not_populate`);
- the bounce rows (`ScalarHistory.bounces`, with `BOUNCE_COLUMNS` and `bounce_row_columns` in
  `ObjectFactories/ScalarHistory.py`);
- the sample masses (`ScalarHistoryValue.log_m_eff`, `.log_m_tracker`; `MASS_COLUMNS`);
- two items for 04: the tools that read `alpha_value` by raw SQL see only its value, and the inventory report's α
  label needs a decision.

**What changes from §8.4's row 04.** The option selects a drain model (decision 1 of §8.10), not a gap prescription
(P26, superseded). The "readers" of that row are the tools that read `alpha_value` by raw SQL, and the inventory report.

**What the drivers do today.**
- **`main.py`.** It looks up one `alpha_value` per `--alpha-values` value with no model, so every row is `fixed`. It
  runs one pipeline per row. `alpha_fraction` refuses a value ≥ 1 at parse, so no A above 1 can be given.
- **The plot scripts.** They match the same list through `plot_inputs.parameters`, and write under
  `<label>/alpha=<α>/`. Their histories.csv and `Delta_N_eff.pdf` span every α of the run, at `<label>/`.
- **The tool route.** `tools/history_and_bbn.py` takes `--drain-law {fixed,one_loop}`, and its `--alpha` carries α or
  A by the law (01, P25).

**Proposed by the planner for 04 (P33–P40), the defaults unless the user says otherwise.** Prompt 04 is written on them.
- **P33 — the option.** `main.py` and the plot scripts gain `--A-values`: one or more A, each finite and > 0, with no
  upper bound. Each value names the row (A, `DRAIN_MODEL_ONE_LOOP`), the code's current one-loop model. No driver
  takes a model string.
  - **The checks.** Values are checked at parse by `check_drain_model`, the one statement of the rules. Duplicates
    are collapsed by `distinct_values`.
  - **The default is none.** `--alpha-values` keeps its default [0], so a run given only `--A-values` also builds,
    or reads, natural mode, the baseline every figure compares with.
  - **Why this form.** The rebuild of record (U14, with α = 0 beside it) is then one invocation, and one plot run
    writes one histories.csv across both models. A plot run of a store built under an older version of the law
    stops (exit 2), as a run under another `VERSION_LABEL` finds nothing.
  - **Not chosen:**
    - (a) `--drain-model` selecting the model of `--alpha-values`, one model per run. Natural mode and the A values
      would then need two runs, and the second plot run would overwrite the first one's `<label>/histories.csv`.
    - (c) pairs such as `fixed:0 one-loop-measured-v1:1`, which put the version string on every command line.
- **P34 — the order.** The fixed rows come first, in the order of `--alpha-values`, then the one-loop rows, in the
  order of `--A-values`, with one pipeline per row as today. A run given no `--A-values` looks up exactly what it does
  today.
- **P35 — the paths and the labels.**
  - **Under `fixed`, unchanged:** the products go to `<label>/alpha=<α:.5g>/`, and the label reads `α = <α:g>`.
  - **Under the one-loop model:** the products go to `<label>/<drain_model>/A=<A:.5g>/`, and the label reads
    `A = <A:g> (<drain_model>)`.
  - **Why the version is in the path:** a later version is then a new directory. The science-runs scripts glob
    `alpha=*` and so never read a one-loop directory.
  - One helper gives the path and one gives the label, each from the `alpha_value` row.
- **P36 — histories.csv.**
  - **A `drain_model` column** follows `alpha`. `alpha` keeps the key's value: α under `fixed`, A under the one-loop
    model.
  - **The drain record** is a block of columns, `DRAIN_CSV_COLUMNS`. It holds every field of `DrainHistoryValues`
    under its stored name, after `DISSIPATION_CSV_COLUMNS` and before `failure_reasons`, and is blank under `fixed`
    and on a failure.
  - **The sort** is by (`drain_model`, alpha, M, n, β). For a store holding only `fixed` rows, the order is today's.
  - **`Delta_N_eff.pdf`** draws one curve per (model, value) with value > 0, labelled as in P35.
- **P37 — fields.csv.**
  - **Two columns:** `log_m_eff_Mp` and `log_m_tracker_Mp`, after `Delta_gstar`, under every law. Each is the stored
    column, by `MASS_COLUMNS`' conversion, and blank where NULL.
  - **The row-building moves to `extract_common.py`,** so that it can be tested without running the script, which
    parses its arguments on import. The existing columns keep their arithmetic exactly.
- **P38 — bounces.csv.**
  - **Where and when.** It sits beside each one-loop history's fields.csv. It is written for every one-loop history,
    header only if the history has no bounce row, and never under `fixed`, so that a missing file means the fixed
    law.
  - **Rows:** one per bounce row, in `ordinal` order.
  - **Columns:** those of `BOUNCE_COLUMNS` in the table's order, but `history_serial`, by `bounce_row_columns`, so
    that each cell is the stored column's value. Booleans are written as 0/1 and NULL as blank.
  - The history's keys come from the path and from histories.csv, as for fields.csv.
- **P39 — the tools that read `alpha_value` by raw SQL.**
  - **`tools/bbn_from_store.py`** gains `--drain-law {fixed,one_loop}`, default `fixed`, as `tools/history_and_bbn.py`
    has (P25), and its query matches the model the law gives.
  - **`tools/outward_step_scan.py`** rebuilds a history under the fixed law. It therefore reads only `fixed` rows
    from a store, and counts the rows of other models it skips.
  - **A store whose `alpha_value` has no `drain_model` column** (format ≤ 6) holds only `fixed` rows. Both tools read
    it as today, as `bbn_from_store.py` reads a store with no `alpha_value` table as α = 0.
  - **`tools/inventory_report.py`** labels a `fixed` row `alpha=<α>`, as before 03, and a one-loop row
    `A=<A> (<drain_model>)`, in the values list and on each history's line. A store holding only `fixed` rows then
    reports as it did before 03.
- **P40 — the registered run's scope.** `describe_scope` adds `A <values>` only when `--A-values` is given, so every
  scope a fixed-law run records is unchanged.

**The Export gate made exact** (§8.5): every new column of histories.csv, fields.csv and bounces.csv equals the stored
column, bitwise. For fixed-law histories, the existing columns of histories.csv and fields.csv are byte-identical to
those the code at the commit adding prompt 04 writes from the same store. The plot scripts refuse an `--alpha-values`
or `--A-values` value that has no row under its model (exit 2). Nothing runs under Ray: the drivers are checked by
`ast` and their functions on temporary stores, as `inelastic-reflection` 10 checked `--alpha-values`. 06 runs them
under Ray.

### 8.13 Before 05 (2026-11-06)

Written by Claude Opus 5.5 after 04 landed and was reviewed (`64a6274`, `48d98fd`), at the user's request: "Please
write prompt 05".

**Ready.** The Export gate holds, and the tree is clean. 04's log §7 (as corrected in the review) gives the export
format exactly:
- histories.csv at `<output>/<label>/`, with `drain_model` and the drain record;
- fields.csv per history, with the sample masses;
- bounces.csv beside it for each one-loop history, the 30 stored columns of `ScalarHistoryBounce`.

Each converted cell is the store's own conversion of its column (note 04, ruling 1). 05 needs nothing else, and opens
no store.

**Stale beside §8.10 and §8.11, now settled for 05:**
- **"The R·S(L) bound"** (§8.3, §8.4 row 05) is dropped. Under decision 3 the classical regime drains R·S(L), and each
  row carries it as `alpha_eff`.
- **"Run on 02's histories"** (§8.4 row 05) reads 02d's, which replaced 02's and 02b's one-loop measurements.
- **The Derive gate's "E and the margins agree with the probe"** (§8.5): the all-generation margins are 03's stored
  columns, checked in 03's review against the core with the weight set to 0. 05 compares E only.

**What the tool computes** (§8.3, with ruling 3 of §6.3): per generation the occupation at birth n_g; per producing
bounce the occupation overlap n̄_j; per history δ_stim; E(N) at every sample, its maximum and r_χ E's; per bounce the
phase slip and its minimum over producing bounces; per bounce the backreaction ratio n_χ m_k/E_k and its maximum.
Rescattering is bounded by δ_stim (about 2n), and the bound is stated, not computed separately (§8.3).

**For the user (U16) — the spectrum's shape for one-loop generations.**
- **U4 as ruled (2026-11-02).** One shape for every generation, whatever its estimate, in units of its k_ex:
  - s_dir ∝ x² below a peak at x_p = 0.62, exponential above;
  - s_assoc = 49 x_*³/x, with x_* = 0.0058.
  The direct piece is the classical wall's test-field spectrum (`production_energy_ratio.tex`, §"Results": peak at
  0.31 ω_t = 0.62 k_ex, slope 1.99 below, about 10⁻²⁰ at 5 ω_t). The associated piece is the 2PI toy's soft
  occupation, fifty times the test field's at 0.0029 ω_t (`kb_partB_report.tex`, "The soft occupation").
- **What has changed since.** Under the measured law most drained energy is one-loop. In 02d, at every A, the median
  history drains between 0.997 and 1 of its K in one-loop generations; four or five histories per A drain mostly in
  the other regimes (`var/one-loop-drain/02d/sample.jsonl`, `drain_values`). U4 was ruled when the fixed α's late
  classical drains dominated.
- **What the Born scheme gives for a one-loop bounce** (`wall_backreaction.tex`, "Weak pulse (one-loop scheme, Born
  regime)"):
  - The final spectrum's energy peaks at 1.06 k_ex, with 89% of it between 0.3 and 3 k_ex, against 1.14 k_ex for
    U4's x³s_dir.
  - Below 0.1 k_ex, n_k → S²/4k², against U4's x² plus the 1/x associated piece.
  - With a 1/x² occupation the overlap ∫x³ s(x) s(x e^{−u}) dx diverges logarithmically at x → 0. The shape would
    then need an infrared end at each generation's own mass, x ≥ m_ta/k_ex = e^{−ℓ}, which makes every kernel a
    function of two variables.
  - The same note finds the scheme self-consistent only with its infrared cutoff near the band's edge.
- **Proposed: keep U4 as ruled for 05.** The shape is one versioned function, named in every output, so a later shape
  is a re-run of the tool, not a rebuild. 05 reports the associated piece off and ×10 beside it, as U4 asked (P46).
  - **Alternative (b):** the Born scheme's final spectrum for one-loop and gap generations, with U4's for classical
    ones, cut off at e^{−ℓ}. That means two-variable kernels and a tabulation from `scripts/spec_born.py`.
  - **Alternative (c):** U4 as the model of record, and (b) as a further sensitivity.
- **Prompt 05 is written on the proposal.** It is not dispatched until the user answers U16.

**U16 ruled by the user, 2026-11-06:** U4 stays for every generation, whatever its estimate. Alternatives (b) and (c)
are withdrawn, and the associated piece stays in the model of record at scale 1, the default. The user's words: "Surely
`wall_backreaction.tex` is wrong. I can't think why we would have that divergent `1/k^2` piece. The point of U4 was to
use the spectrum that we had measured. At the time, I think I wondered even whether to use the `1/k` associated
production piece." And then: "Yes, please record that way. Keep the associated piece as the default."

*Correction (the planner, 2026-11-06).* The U16 entry above misdescribed the Born scheme's final spectrum as a
candidate shape for one-loop bounces, and in conversation called it "the one-loop spectrum". It is not a measured
spectrum.
- In the scheme, n_k = |∫ V̇'' e^{−2ikt} dt / 4k²|² tends to S²/4k² as k → 0, with S = ∫ V'' dt. That is the Born
  result for massless modes struck by a pulse of nonzero area. The scheme's modes are massless, with the mass entering
  only as an infrared cutoff.
- On the exponential wall itself, the same note's test-field calculation gives n ≃ 34 (k/ω_t)²/L², the k² law of U4's
  direct piece. The note sets the scheme's S²/4k² against it.
- `kb_partB_report.tex` finds the soft modes populated by associated production with a 1/q spectrum, "not by the
  retarded kick".

U4 is the spectrum that was measured, and nothing in the notes supports the 1/k² piece. P46 reports the associated
piece at 0 and 10 beside the default 1, as U4 asked.

**Proposed by the planner for 05 (P41–P49), the defaults unless the user says otherwise.** Prompt 05 is written on them.
- **P41 — two modules,** as §8.4 names them.
  - `ComputeTargets/quanta_spectrum.py` is pure: the shape, its moments, its kernels, its constants with their
    sources, and a version string. It does no I/O and imports nothing of the integrator.
  - `tools/quanta_measures.py` reads the exports and writes the measures. It opens no store and starts no Ray.
- **P42 — the inputs are 04's exports alone.**
  - A history is found from its histories.csv row, by the directory 04's helpers give it.
  - Two rows that map to one directory are refused.
  - A fixed-law history has no bounces.csv. It is skipped with a count, since nothing in it carries a generation.
- **P43 — K(N) is rebuilt from the bounce rows.**
  - It is right-continuous, with entries at each candidate's drain N:
    - K_before + ΔK at a candidate, bitwise as the ledger adds it (03, ruling 6);
    - the parking leftover where the next candidate's K_before shows it, or after the last candidate, from
      `parking_share`.
  - It is checked against each sample's r_χ ρ_R,E e^{4N} from fields.csv. The leftover has no spectrum (the open issue
    `[00-the-parking-leftover-has-no-spectrum]`). It enters K but not E's numerator, and its share is reported.
- **P44 — the measures, exactly** (prompt 05 §1.4):
  - **The generations** are the candidate rows with ΔK > 0 and a `log_q`, born at `N_turn`, with Q_g = e^{log_q}.
  - **n_g** is the occupation at the direct peak, A_g s(x_p), with A_g = 2π² ΔK_g/(Q_g⁴ I).
  - **n̄_j** = Σ_{g: N_g < N_j} A_g Ω(ln(Q_g/Q_j)), with Ω(u) = ∫x³ s(x) s(x e^{−u}) dx / I.
  - **δ_stim** = Σ_j 2 n̄_j ΔK_j / K(N_final).
  - **E(N)** with the field's mass, and with the tracker's beside it, as the probe computes it (§1.1 measure 2).
  - **The phase slip** as `source/probe/drain/phase_slip.py` defines it, over consecutive assessed candidates, with
    `log_H` as the probe's `H_before`.
  - **The backreaction ratio** at every candidate row with `log_m` and `log_E`: n_χ m_k/E_k, with
    n_χ a³ = Σ_{g before k} ΔK_g J/Q_g and J = ∫x² s/∫x³ s.
- **P45 — the outputs.**
  - `quanta_bounces.csv` and `quanta_samples.csv` beside each one-loop history's bounces.csv.
  - `quanta_histories.csv` at `<label>/`, one row per one-loop history, keyed as histories.csv.
  - Every file names the spectrum's version and the associated piece's scale. 07 reads these files, so 05's log
    states them exactly.
- **P46 — the sensitivity.** The tool takes the associated piece's scale, default 1. 05 reports 0 and 10 beside 1.
- **P47 — the run on 02d's histories.**
  - Every one of 02d's 230 successful tasks is re-integrated once at its accepted tuning, as `store-03/cost_03.py`
    realizes them, with its candidates checked against 02d's partial line by `repr`.
  - Each is stored on a temporary store, exported by 04's writers in the plot scripts' layout, and measured at the
    three scales.
  - The run is detached, at most six workers, under `var/one-loop-drain/05/`. 02d's per-integration walls sum to
    about 10 500 s.
- **P48 — the Derive gate, made exact.**
  - Each kernel, moment and measure agrees with direct quadrature to 10⁻⁶ relative on constructed generations.
  - With the spectrum collapsed to its peak, E agrees with the probe's `excess` (run unedited, extracted by `ast`) on
    P-2197 at A = 1, fed the same generations, masses and ledger.
  - K(N) agrees with the payload's ledger.
- **P49 — measurements, not bounds.** No threshold refuses or warns on a measure, and no value opens an issue.
  `[00-later-bounces-act-on-quanta-already-present]` is narrowed by 05's δ_stim on the sample. It stays open as a
  known limitation, as the user ruled (P15).
