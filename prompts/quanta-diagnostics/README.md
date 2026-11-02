# Campaign — quanta-diagnostics

**Written:** 2026-11-02 on branch `quanta-diagnostics`, cut from `reflection-restart` at `3b8a553` (that
campaign's plan and its prompt 01, committed; none of its code), by Claude Opus 5.5, at the user's request:
"Yes, please now plan this campaign", after a scoping session the same day. U1–U7 are for the user to
rule (§6.1); P1–P16 stand unless the user rules otherwise (§6.2).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the scoping record
[`source/scoping_2026-11-02.md`](source/scoping_2026-11-02.md), with the probe scripts and their
per-history summaries in [`source/probe/`](source/probe/); `inelastic-reflection` README §8 and its
prompt 08 (the peak and the margins) and 09 (the store); the briefing notes
`BriefingNotes/production_energy_ratio.tex` and `meff_and_wall_production.tex` (the classical spectrum)
and `KadanoffBaym/kb_partB_report.tex` §§ "The soft occupation", "The hard spectrum", "What is open"
(the two-loop toy), all in the repository *Chameleon PBH (David's stuff)*; the `reflection-restart` plan.

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
- **At a later bounce they are not.** The wall's mass exceeds the between-bounce mass by e²²–e⁵²; for
  quanta with p ≪ ω_t a bounce is non-adiabatic, so it rescatters them and stimulates production into
  occupied modes. The ledger treats them as passive radiation. The user has ruled this documented and
  quantified, not modelled (source §7).

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

## 1. Scope

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

### 6.1 For the user to rule (U1–U7)

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

Accepted as proposed unless the user rules otherwise.
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

## 7. What this campaign hands to the user

- The measures, stored per bounce, per sample and per history, on the fixed integrator, with
  `FINGERPRINT_FORMAT` 7.
- Measured on about forty tuned histories and a rehearsal grid: the margins over all generations, E,
  and the wall-exposure summaries δ_stim and δ_resc, with their sensitivity to the associated piece.
- Two figure tools and their demonstration figures.
- The handover for the rebuild that carries all of it (U2), and for the figures of record made from it.
