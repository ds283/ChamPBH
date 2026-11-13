# Campaign — electroweak-eos

**Written:** 2026-11-12 on branch `electroweak-eos`, cut from `main` at `ec6bca8`, by Claude Opus 5.5, at
the user's request: "start planning the migration campaign around this new merged table". U1–U6 ruled
by the user as proposed on 2026-11-12, and U7 ruled out of scope (§6.1). P1–P12 stand, with P11 amended
by U7 (§6.2). U8–U11 ruled as proposed (§8.3); P13–P20 stand (§9); U12 ruled (a) on 2026-11-13, and P21–P29
stand unless ruled otherwise (§10).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the kicking-function audit and its addendum
of 2026-11-12, [`.documents/peakewsurf-kicking-function/report.tex`](../../.documents/peakewsurf-kicking-function/report.tex)
§§ provenance and addendum (`ec6bca8`), with its `data/` (the eos15 file, the points of 2026-11-12, the
merged table, `laine_meyer_stats.json`) and `scripts/laine_meyer_compare.py`; the field-equations comparison,
`.documents/peakewsurf-field-equations/`; the surfing-temperature scan,
`.documents/surfing-temperature-verification.md` (the EW threshold in (3.019, 3.020] on today's table);
the `2026.10.0` run record, `science-runs/2026-11-08-science-2026.10.0/README.md`.

## 0. Why this campaign exists

### 0.1 What is wrong with today's table

ChamPBH's kicking function Σ(T_J) = 1 − 3w(T_J) is a cubic spline in ln T of
`CosmologyModels/GenericEOS/Xav_EOS_data.csv` (`Xav_EOS_spline`, type_id 3). Above about 100 GeV that
table is a free gas of Standard Model particles at their **vacuum** masses: it agrees with one to 3.3 % of
Σ from 100 GeV to 25 TeV (addendum §1). In the symmetric phase the W, Z, H and top have no vacuum mass, and
Laine & Meyer's equation of state (arXiv:1503.04935) has Σ falling to about zero by 360 GeV. Today's table
has Σ = 3.8–5.7 × 10⁻³ at 300–380 GeV, where Laine & Meyer have less than 7 × 10⁻⁴.

The notebook `PeakEWsurf.nb`'s table, from the same author, follows Laine & Meyer through the crossover.
On 2026-11-12 the user supplied 29 points sampling its curve every 10 GeV from 100 to 380 GeV. With the
notebook's other rows they make the **merged table** (addendum §3; `data/merged_w_table.csv`, 157 rows).
Measured there:
- It lies within the spread of Laine & Meyer's own stated uncertainty (about 0.01 in Σ) and inside the
  steps at their joins. It is within ±1.1 × 10⁻³ of their Σ except at the cusp near 159 GeV (−3.9 × 10⁻³).
- Over 100–300 GeV its ∫Σ d ln T is within 0.2 % of theirs.
- It splines cleanly as `Xav_EOS_spline` splines: Σ has one maximum above 40 GeV (0.037757 at 54.02 GeV,
  against today's 0.037436 at 53.26 GeV) and falls to zero at 386.7 GeV with no step in dΣ/d ln T.
- `Xav_EOS_spline` takes it without a change of code.

The audit of 2026-10-16 measured, in the notebook's integrator, that the notebook's table moves the
electroweak-capture threshold from β = 3.01949 (today's table) to 3.00285. Between those values, histories
change from capture at the QCD transition to capture at the electroweak peak. The merged table is the
notebook's curve, more finely sampled, so a threshold near 3.003 is expected. It has not been measured in
either code.

### 0.2 A difference at the low-temperature end (planner, 2026-11-12; not in the addendum)

The notebook's table starts at 36.5 keV with w = 1/3 exactly. Today's table continues the e⁺e⁻ tail to
10 keV. Above 50.1 keV the two agree row for row. Below it, splined:

| T [keV] | 30.0 | 34.1 | 38.8 | 44.2 | 50.2 |
|---|---|---|---|---|---|
| Σ, merged table | 0 | 0 | 1.7 × 10⁻⁵ | 7.2 × 10⁻⁴ | 2.96 × 10⁻³ |
| Σ, today's table | 4.0 × 10⁻⁶ | 1.5 × 10⁻⁴ | 2.7 × 10⁻⁴ | 1.26 × 10⁻³ | 2.95 × 10⁻³ |

The e⁺e⁻ tail is physical (m_e/T ≈ 14 at 36 keV), and this is inside the BBN window. The difference is
small, but it is a change nobody has asked for. **U2** decides it.

## 1. Scope

**In scope:**
- **The table** (prompt 01). Install the table U2 rules as the production w table, with the script that
  builds it from the committed inputs, so that how it is assembled is in the repository. Bump the version
  label. Re-pin every test whose pinned values move, each with a stated reason. Update the class's
  docstring and the documents that describe the table.
- **The effect** (prompt 02). Measure the electroweak-capture threshold in ChamPBH on the new table, with
  the surfing-temperature scan's grids. Measure what the change does to a sample of the `2026.10.0` run's
  histories (Y_p, D/H, ΔN_eff, capture temperature, turning points), old tree against new. Re-measure the
  bath drift of `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]`, which depends on w.
- **The close** (prompt 03). The documents, the issues, the indexes, and the handover to a science
  rebuild under the new label.

**Out of scope:**
- **The science rebuild.** A rebuild of record is the user's, on the user's word, recorded in
  `science-runs/` (§7).
- **g_ρ and g_s.** They stay the Saikawa–Shirai fits unless U4 rules otherwise. That leaves
  `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]` and `[09-…bath-drifts…]` open.
- **Rows below 50 keV and 50 keV – 100 GeV** beyond what U2 decides. In particular
  `[00-xav-sigma-is-non-monotone-at-25-35-keV]` is left alone unless U2 (a) removes the rows it is about,
  in which case 01 records that.
- **The drain law, the spectrum model and the wall model.** They are local to the wall and do not read w;
  P8 says how that is checked rather than assumed.
- **The provenance of the rows** (U7, ruled out of scope 2026-11-12): asking the author how the rows and
  the 10 GeV curve were made, and `[00-kicking-function-table-has-no-provenance-in-the-repository]`.
- Laine & Meyer's negative Σ above 357 GeV. The merged table is zero there, as the notebook is.
- The packages; `thirdparty/`; `PRyM/`; the notebook itself.
- Records: the audit, the run records and other campaigns' boards stay as written (invariant 6). They
  gain dated notes where this campaign adds to them.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-install-the-table.md` | the table by U2; the build script and its test; `VERSION_LABEL`; the re-pinned characterisation tests and golden histories with reasons; the class docstring | Opus |
| 01a | `01a-resolved-turning-points.md` | an interior pair of sign changes of π kept only if the solver resolves it (§8); lands before 01's commit | Opus |
| 02 | `02-measure-the-effect.md` | the EW threshold on the surfing grids (S1, and a fine grid placed by the coarse one); the sample of `2026.10.0` histories old against new; the bath drift re-measured; optionally the notebook-side threshold (U5) | Opus |
| 03 | `03-documents-and-handover.md` | `architecture-summary.md` and `numerical-strategies.md` where they describe the EOS; the verification document; the issues; the indexes; the handover for the rebuild; closes the campaign | Sonnet |

Written one at a time, each after the previous one lands and is reviewed. 01a was inserted on 2026-11-12, while 01 was held at its stop (§8). 02's "old" side runs on
`main` at `ec6bca8`, or any tree before 01, in a worktree or a separate checkout. It never runs on 01's
tree with the old CSV swapped back in.

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Table** | the build script regenerates the installed CSV byte for byte from the committed inputs; `Xav_EOS_spline` evaluates the new table (its peak and its zero at the top row as §0.1 states); every moved pin is listed with its old and new value and a reason; values below the first changed row change only at the spline's coupling, measured and stated | 01 |
| **Resolved** | on the old table, the suites, the golden records and 01's scan bitwise unchanged; on the new table, the early pair dropped wherever 01 saw it, α = 0 histories bitwise apart from `turning_points` (§8.2) | 01a |
| **Threshold** | the EW-capture threshold on the new table is bracketed to 0.001 in β on a fine grid, and stated beside 3.020 (today, ChamPBH) and 3.00285 (notebook) | 02 |
| **Effect** | for every sampled history: the change in Y_p, D/H, ΔN_eff (1 MeV, T_CMB), the capture temperature and the turning points, old against new, set beside what the run's figures resolve | 02 |
| **Close** | documents, issues, indexes and the handover written; the suites at their counts | 03 |

## 3. Stores

- 01 opens none but temporary SQLite stores in `tempfile` directories.
- 02 writes its sample, old and new, to stores of its own under `var/datastores/` (`electroweak-eos-02-old`,
  `-new`). It reads `var/datastores/science-2026.10.0` read-only (sqlite `mode=ro`) only to choose the
  sample and its stored normalizations. It never writes to the run's store.
- No Ray is needed. If 02's sample is run through `main.py`, it uses the isolated cluster by
  `tools/ray_cluster.sh` and the registry wrapper, as `CLAUDE.md` says.

## 4. The interfaces

- **01 → 02.** The new tree and its label. 02's "new" side is 01's commit.
- **02 → the user.** The threshold and the per-history changes, which tell the user what a rebuild will
  move and whether any figure of `2026.10.0` stands.
- **03 → the run.** A dated note in the `2026.10.0` run record that its histories were made with the
  vacuum-mass table, and the handover in `.documents/review-remediation-verification.md` (§4.21).

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–8 and "Repository mechanics", and:
- **No production store is written.** The run's stores are read-only throughout.
- **The table is built, not typed.** The installed CSV is the output of a committed script from committed
  inputs, never edited by hand. The inputs are the audit's `data/` files, which are not modified.
- **Measurements, not bounds.** The threshold and the per-history changes are recorded as measured. A
  threshold enters only where the user rules one.
- **Long runs are detached and recorded** (`CLAUDE.md`, "Long-running jobs"), if any of 02 is long.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 For the user to rule (U1–U7)

**Ruled by the user, 2026-11-12:** "U1 to U6 are fine as proposed; U7 doesn't need to be in scope here."
So:
- **U1–U6 as proposed.** The campaign is `electroweak-eos` on its branch. The table is U2 (b): today's
  rows below 50.1 keV, the notebook's from 50.1 keV to 89.1 GeV, the 29 points, and the 386.7 GeV row.
  It is installed in place with `VERSION_LABEL` `2026.11.0`. g_ρ and g_s are unchanged, the notebook-side
  threshold is not run, and 02's sample is about 40 histories, run detached.
- **U7 out of scope.** The campaign does not ask the author for the code behind the rows, and does not
  assign, narrow or close `[00-kicking-function-table-has-no-provenance-in-the-repository]`. That issue
  stays on the `review-remediation` board as it is. 01 still builds the installed CSV from committed
  inputs (P1), which records how the new table is assembled, but not how its rows were made.

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name and branch | `electroweak-eos`, on a branch of that name cut from `main` at `ec6bca8` (this plan is its first commit) | another name |
| **U2** — the table's rows | **(b)**: today's rows below 50.1 keV (the e⁺e⁻ tail, §0.2), the notebook's rows from 50.1 keV to 89.1 GeV, the 29 points of 2026-11-12 (100–380 GeV), and the notebook's w = 1/3 row at 386.7 GeV. Above 50.1 keV this is the merged table of the addendum. | (a) the merged table exactly as the addendum built it (the notebook's 36.5 keV truncation included) |
| **U3** — how the table is installed | **in place**: the new rows replace `Xav_EOS_data.csv`, `Xav_EOS_spline` and type_id 3 are kept, and `VERSION_LABEL` moves to `2026.11.0`, so a version-keyed lookup never mixes the tables. The old table stays in git and in the audit's record. | a new class and type_id 4 beside the old one, selectable per run, so both can live in one store. More code, a driver option and an inventory change, for a comparison 02 makes anyway with two trees |
| **U4** — g_ρ and g_s | **unchanged** (the Saikawa–Shirai fits). The author says they are not meant to be consistent with any w, and no consistent pair is in hand. | ask the author for g_ρ and g_s consistent with the new w, and hold 01 until they come |
| **U5** — the notebook-side threshold | **not run.** The notebook is not in the repository, and ChamPBH's own threshold (02) is the number the paper uses. | 02 also re-runs the audit's bisection in the notebook's integrator with the merged Σ (needs the notebook's path and Mathematica 15) |
| **U6** — 02's sample | **about 40 histories** from the `2026.10.0` grid: every M of blocks A–C and R, β spread across each block, α = 0 and the one-loop A values the figures use, plus the β nearest 3.00–3.02. Each is tuned on both trees and run through BBN; about 1–2 h, detached. | a smaller sample (about 15, under 30 min); or a whole block (C) on both trees |
| **U7** — provenance of the rows below 100 GeV | **ask the author** for the code that made the notebook's rows and the 10 GeV curve, and record whatever arrives. Meanwhile 01 narrows `[00-kicking-function-table-has-no-provenance-in-the-repository]` to the rows below 100 GeV, whose construction is still not recorded, rather than closing it. | close the issue on the strength of the addendum's record |

### 6.2 Proposed by the planner (P1–P12)

Accepted as proposed unless the user rules otherwise.
- **P1 — the build script.** `tools/build_w_table.py` reads the audit's `data/notebook_w_table.csv`,
  `data/notebook_w_points_2026-11-12.csv` and, under U2 (b), today's CSV at the planning commit. Today's
  rows are kept as `data/pipeline_w_table_ec6bca8.csv` in the campaign's own folder, since 01 overwrites the
  original. The script writes `CosmologyModels/GenericEOS/Xav_EOS_data.csv` with the same two columns and
  17 significant digits. A test in `tools/tests/` regenerates it and compares byte for byte, and checks the
  composition rule (which rows come from which source).
- **P2 — the version label.** `VERSION_LABEL` moves to `2026.11.0`. The fingerprint format moves only if
  what the fingerprint covers changes (01 decides from `config/fingerprint.py` and says why). A
  `2026.10.0` store stays readable.
- **P3 — the characterisation tests.** `CosmologyModels/tests/test_kicking_function.py`,
  `test_eos_w_derivative.py` and `test_temperature_law.py`, and `eos_reference.py`, are re-pinned to the
  new table. Each changed pin is listed in the log with its old and new value. A pin that does not move
  is left alone. The docstring names this campaign.
- **P4 — the golden histories.** Every history crosses the electroweak scale from T_init = 20 000 GeV,
  so every golden history is expected to move: `golden_history_05.json`, `golden_history_06_tuned.json`
  and the three `rr03` records. Each is regenerated under a new name (`golden_history_ee01*.json`), and
  the old file is kept as the record it is (the repository's practice since `jordan-normalization` 03).
  The log states how far each history moved (turning points, end values) and why that is the table
  and not something else: with the old CSV the tree must reproduce the old goldens, and 01 shows that it
  does before swapping.
- **P5 — the class docstring** of `Xav_EOS_spline` is rewritten for the new table: its rows, range,
  spacing, peaks, the sources by range, and the build script. `CodeComparisonTools/XavEOS_data.csv` and
  `XavEOS_test.py` are left alone; they compare the old table, and their docstring gains a dated note.
- **P6 — the threshold grid** (02). Run `tools/surfing_scan.py` as surfing-temperature 04 ran S1 (79 β,
  1.10–5.00), then a fine grid of 21 β at 0.001 spacing placed by the coarse grid's capture edge. Also
  state the reachability prediction from the new peak Σ beside today's 3.011779. Both runs take minutes.
- **P7 — the effect sample** (02), under U6: the same histories, tuned, on both trees, compared on the
  stored observables. Any history whose type of capture changes (QCD to EW) is listed with β, M and α.
- **P8 — what is not expected to move.** The drain law and the spectrum constants are local to the wall.
  02 checks it on the sample: the per-bounce drain records, compared old against new, should differ only
  where the bounce sequence differs. It records what it finds and does not re-measure the law.
- **P9 — the bath drift** (02). Re-measure `[09-…bath-drifts…]` at 1 GeV, 1 MeV and today, as that issue
  measured it, and record the new values on its board entry. It stays open (U4).
- **P10 — documents** (03). `.documents/electroweak-eos-verification.md` (new) holds 01's and 02's
  measurements. `architecture-summary.md` and `numerical-strategies.md` gain a dated note wherever they
  describe the w table. The audit gets a dated pointer to the campaign and nothing else.
- **P11 — issues** (amended by U7, 2026-11-12). No issue is assigned to this campaign. The provenance
  issue is left as it stands (§6.1). Anything else found is recorded and opened, not fixed. The proposal
  as first put was to assign the provenance issue and to narrow or close it in 01.
- **P12 — standard analyses** (invariant 8). The campaign changes no input that a standard analysis
  reads directly, but it changes every history. So the handover says that a rebuild under `2026.11.0`
  repeats every standard analysis in `science-runs/README.md`.

## 7. What this campaign hands to the user

- The new table in production, built by a committed script from committed inputs, with `VERSION_LABEL`
  `2026.11.0`.
- The electroweak-capture threshold measured in ChamPBH on that table.
- What the change does to the run's histories, from 02's sample: which figures of `2026.10.0` it moves,
  and by how much.
- The rebuild itself, which stays the user's. Every block of `2026.10.0` crosses the electroweak scale,
  so the expectation, to be tested by 02 and not assumed, is that a rebuild of record is needed for every
  figure. Whether the near-threshold β (about 3.00–3.02) change their type of capture is 02's to say.

## 8. The early turning-point pair, and prompt 01a (2026-11-12)

### 8.1 What 01 found

Prompt 01 stopped at its note's ruling 3 on 2026-11-12, with the table installed and nothing committed.
Its measurements, from probes that changed no code:
- **The pair.** On the new table, a history that starts at rest at T* = 20 000 GeV is kicked only by
  −β f_m above 386.7 GeV (f_m ≈ 1.8 × 10⁻¹²), so π stays at 10⁻¹⁴ to 10⁻⁹ M_P. On the accepted step that
  straddles 386.7 GeV, π at both nodes is negative (on the golden β = 1.5 history, −3.97 × 10⁻¹² and
  −1.82 × 10⁻⁹). The step's dense-output interpolant swings to +6.74 × 10⁻¹⁰ between them. The interior
  sampling of `observe_turning_points` (inelastic-reflection 02, its note's ruling 15) reads that swing as a
  "wall" at N = 3.9415 and a "plateau" at N = 3.9473, at T_J ≈ 388 and 386 GeV.
- **It is not the field turning.** The kick −β(Σ + f_m) is negative there, and friction cannot reverse π,
  so the exact π does not cross zero. The swing is below the solver's absolute tolerance (atol = 10⁻⁸ on
  that history). At atol 10⁻⁹ and 10⁻¹⁰ the pair remains; at 10⁻¹² and 10⁻¹⁴ there is none.
- **Where it appears.** In 14 of 20 scanned histories (β from 0.5 to 20, M = 4 × 10⁻³¹ and 10⁻³ M_P),
  always as a wall then a plateau within ΔN ≤ 0.0063 at T_J = 385–389 GeV. It depends on where the steps
  fall, not on β or M. It was seen in 31 of the 33 integrations of the 22 tests the table breaks. No history
  has such a pair on the old table, where Σ above 387 GeV keeps |π| at 10⁻⁶ to 10⁻³.
- **What it changes.** `first_bounce` does not move: it takes a step only where π goes from negative at
  the start to positive at the end, and both ends of the pair's step are negative (note 01's §0 had expected
  it to move). The pair changes `turning_points`; under the fixed law at α > 0 it arms a drain that its
  plateau drops; under the one-loop law it adds two event rows at the head of the bounce rows, so every
  ordinal shifts by 2; and it resets the half-oscillation window, so the first real candidate's
  nonadiabaticity changes (on P2197, A_k from 388 to 1.6 × 10⁵).

**Ruled by the user, 2026-11-12:** "Handle it in a separate classifier prompt; re-pin test_10 to
2026.11.0". The second half is `test_dissipation_store` test_10, which pins `VERSION_LABEL` and was outside
note 01's list; 01 re-pins it.

**A tail above 386.7 GeV was considered and not proposed** (the user asked, 2026-11-12). Restoring one
would hide the defect rather than fix it: the pair comes back at any start where Σ and π are both near
zero. The old table's tail is the vacuum-mass gas U2 replaced (∫Σ d ln T = +1.8 × 10⁻³ from 386.7 GeV to
20 TeV). Laine & Meyer's is negative (−6.4 × 10⁻⁴ over the same range, least −3.9 × 10⁻⁴ at 574 GeV), well
inside their stated uncertainty of about 0.01 in Σ, and would turn a field started at rest the other way,
which would put a real early turning point at the head of every history. It stays out of scope (§1).

### 8.2 Prompt 01a

**01a — turning points resolved above the solver's tolerance** (Opus). It changes
`observe_turning_points` so that a pair of sign changes the interpolant shows inside one step, with π of
one sign at both nodes, is a pair of turning points only if the excursion between them is resolved by the
solver. It lands **before** 01's commit, so 01's re-pins and golden records are made once, without the
pair. 01 then resumes from its uncommitted work, under an addendum to its note.

Gate **Resolved** (01a):
- **Old table, bitwise.** The five suites at their counts with no test re-pinned; every golden record
  reproduced as committed; on the old table, the turning points of 01's 32-history scan identical to the
  last bit (N, kind, φ, ln T_J, A_k, m_eff/H), and no pair dropped anywhere.
- **New table, by probe** (01's built CSV read through a redirect, not installed): the pair dropped in
  every integration where 01 saw it; at α = 0 every history bitwise identical with and without 01a apart
  from `turning_points`; `first_bounce` unchanged; and a list of which of the 22 tests then pass on the new
  table, for 01.
- **Unit tests** of the rule on constructed interpolants: an unresolved pair dropped, a resolved pair kept,
  a single end-to-end change kept whatever its size, and three changes on one step.

### 8.3 For the user to rule (U8–U11)

**Ruled by the user, 2026-11-12:** "U8 to U11 are fine as proposed; write 01a". So 01a keeps an interior
pair only if its excursion exceeds atol + rtol · max(|π_low|, |π_high|) (U8). A dropped pair is as if absent,
counted in an unstored `IntegrationResult` field (U9). 01a runs in a worktree on `electroweak-eos-01a` and is
fast-forwarded into `electroweak-eos` (U10), with no label bump (U11).

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U8** — the rule | An interior pair (π of one sign at both nodes, two sign changes between them) is kept only if the largest \|π\| of the opposite sign, over the samples between the two crossings, exceeds the tolerance Radau holds π to on that step, atol + rtol · max(\|π_low\|, \|π_high\|). Otherwise both are dropped. A change across the step's nodes (π of opposite signs at the two ends) is always kept, as `first_bounce` keeps it. With more than two changes on one step, consecutive pairs are tested in order from the step's start. The rule is stated in the solver's own tolerance, so it holds in any units. | (b) count only sign changes between the step's nodes, which undoes the interior sampling ruled in inelastic-reflection 02; (c) a fixed floor on \|π\|, which needs a unit and a number with no reason behind it |
| **U9** — a dropped pair | **is as if it were not there**: no TurningPoint, no armed drain dropped, no boundary of the half-oscillation window, no bounce row. It is counted in a new `IntegrationResult.unresolved_turning_point_pairs`, which is not stored: no column, no format change. | store the count as a column of the history (fingerprint format 8) |
| **U10** — where 01a runs | **In a worktree** on a branch `electroweak-eos-01a`, cut from the commit that adds 01a's prompt, so that 01's uncommitted work in the main checkout is left as it is. The orchestrator checks that 01a's commit touches no file 01 has changed, and fast-forwards `electroweak-eos` to it in the main checkout. The worktree's ten known failures (note 01 §0) are recorded, not fixed. | 01's agent sets its work aside as a patch and 01a runs in the main checkout; or 01 commits first with the pair re-pinned, and 01a re-pins those tests again |
| **U11** — the label | **no bump in 01a.** 01's `2026.11.0` covers both: no store is built from the branch between them, and the branch is not merged before 01. | 01a bumps to `2026.10.1`, and 01 to `2026.11.0` after it |

01a also checks whether `surfing_turning_points`, which samples g the same way, can do the same, and
records what it finds without changing it.

## 9. Prompt 02, as planned after 01 (2026-11-13)

01 landed as `5947b4f` (review `850cc72`). This section fixes what P6–P9 left open, from what 01 and the planner
measured. Each P below is accepted as proposed unless the user rules otherwise. None changes U1–U11.

### 9.1 What changed since P6–P9 were written

- **The window has moved.** The planner's probe on the new table, tuned at M = 10⁻³ M_P (orchestrator note 02 §0):
  QCD captures at 2.90–2.97; nothing captures at 2.98–3.002, the first bounce rising from 1.95 GeV (2.98) through 20.6 GeV (2.985)
  to 50.2 GeV (3.002); EW captures from 3.004. So the uncaptured window lies in (2.97, 2.98] to (3.002, 3.004], against S5's
  2.999–3.019 on the old table, untuned. S5's grid, 2.980–3.040, would miss the new window's lower edge.
- **The old threshold, 3.020, was measured untuned** at `2026.6.0`, before `jordan-normalization` (surfing-temperature
  verification §1.8). `tools/surfing_scan.py` has tuned every history since that campaign's 08. "Today's" threshold under
  today's tool has not been measured.
- **The run's store is whole and its tree is known.** Every block of `2026.10.0` ran at `5837a47`. Between it and
  `ec6bca8` only drivers' options changed (`--no-alpha-values`, `--no-plots`). Between `ec6bca8` and `5947b4f` only
  01a's classifier and the table changed computation: `main.py`, the plot scripts, `surfing_scan.py` and
  `history_and_bbn.py` are the same files.
- **A worktree build through the shared cluster has a precedent.** `one-loop-drain` 06 built its base store from a
  worktree at an older tree, through its own `registered_run.py`, with `venv` and `var` linked in, and a code probe
  (its P51) showed which tree's code each Ray worker ran.

### 9.2 Proposals for 02 (P13–P20)

- **P13 — the threshold grids** (amends P6). All with `tools/surfing_scan.py` at φ* = 5 M_P, M = 10⁻³ M_P, tuned, as
  surfing-temperature 04 ran them:
  - **S1-new**: S1's grid, β = 1.10–5.00 in 0.05, on the new tree;
  - **F-new**: a fine grid at 0.001 from S1-new's largest QCD-captured β to its smallest EW-captured β, both included.
    It replaces P6's "21 β placed by the coarse grid";
  - **F-old**: S5's grid, 2.980–3.040 at 0.001, on the old tree (a worktree at `ec6bca8`), so that today's threshold is
    measured with today's tuned tool, beside the untuned 3.020.
  Each runs as one process, in the background, resumable from its CSV. They need no Ray and no store.
- **P14 — the sample** (makes U6 concrete). 48 histories, each built on both trees, all at φ* = 5 M_P and
  `--bbn-wall-clock-limit 600`, as the run's were:

  | run | M / M_P | β | rows | histories | from block |
  |---|---|---|---|---|---|
  | `a-edge` | 4 × 10⁻³¹ | the 13 of the dense grid's 2.980–3.040 | A = 1 | 13 | A |
  | `a-spread` | 4 × 10⁻³¹ | 1.2, 2.0, 5.0 | α = 0.1; A = 0.05, 1, 10 | 12 | A |
  | `b` | 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰, 10⁻⁴² | 2.0, 3.0 | A = 1 | 12 | B |
  | `c` | 10⁻¹⁵, 10⁻¹² | 2.0 | α = 0.1; A = 1 | 4 | C |
  | `d` | 10⁻³, 0.1, 0.5 | 2.0, 3.0 | α = 0 | 6 | D |
  | `r` | 10⁻¹⁶ | 2.0 | A = 1 | 1 | R-ladder |

  Every M of the run, the A and α of every figure's central line, and the run's whole near-threshold grid at
  the laboratory mass, where the figures are. Every β is a value of its block's grid, so each history has a stored
  twin in `2026.10.0`. Block-average costs from the run record put a side at about 120 core-minutes, most of it `c`
  and `r`.
- **P15 — both sides built by `main.py`** (as README §3 and U6 already say), through `RunDrivers/registered_run.py` on
  ChamPBH's isolated cluster. The old side from a worktree at `ec6bca8` with `venv` and `var` linked, as
  `one-loop-drain` 06's base. Stores `var/datastores/electroweak-eos-02-old.db` and `-new.db`. Each side is exported
  with `plot_by_beta.py --no-plots` and `plot_ScalarHistory.py --no-plots` from its own tree.
- **P16 — the code probe.** `one-loop-drain` 06's `code_probe.py`, extended: besides module files and labels, each
  process builds `Xav_EOS_spline` and prints the SHA-256 of the CSV it read and Σ at 300 GeV (5.685 × 10⁻³ on the old
  table, 1.889 × 10⁻³ on the new: planner, 2026-11-13). It runs from both trees before any build. A worker that reads the other
  tree's code or table is a stop.
- **P17 — the old side checked against the run.** Every history of the old store's export equals its twin in the run's
  export (`var/science-2026.10.0/out/`), column for column as written. This shows that the old side is the run's code
  and inputs, so the comparison old against new is the change a rebuild would make. A difference is a stop.
- **P18 — the comparison.** Per history, old against new: Y_p, D/H, ΔN_eff at T_CMB and at 1 MeV, T_deliver, the
  reflections, injections, bounce rows and candidates by regime, the parking and release temperatures, and any
  failure. A history whose T_deliver crosses 10 GeV, between the QCD deliveries (about 1–2 GeV) and the EW ones
  (about 30–60 GeV), is listed with β, M and its row. The 10 GeV is a label for the list, not a test. For each figure
  of the run, its notes state what it plots and over what range; the changes are set beside that.
- **P19 — the bounce rows** (P8 made concrete). For each history, the two exports' `bounces.csv` aligned by ordinal: the
  counts, the first ordinal whose `kind` or `source` differs, and before it the largest relative difference in N,
  ln T_J and `alpha_eff`. Recorded, not bounded.
- **P20 — the bath drift** (P9 made concrete). The issue's tables-only integral of
  4 + d ln g_ρ/d ln T − 3(1 + w)(1 + x), on both tables, at the issue's temperatures (100, 1, 0.1, 10⁻², 10⁻³ and
  10⁻⁴ GeV, and today); and per history, ln ρ_R,J − ln((π²/30) g_ρ T_J⁴) from `fields.csv` at 1 GeV, 1 MeV and today,
  on `d`'s β = 2, M = 0.5 history from both stores, and on the run's own export of the same history (R-natural's,
  in the reflection store's export, since the main store's per-history export holds one-loop rows only). The
  numbers go on the issue's board entry as a dated addendum, and the issue stays open (U4).

## 10. Prompt 03, as planned after 02 (2026-11-13)

02 landed as `be81f8b` (review `4fa0aa7`). This section fixes what P10–P12 left open, from what 01 and 02 handed on.
Each P below is accepted as proposed unless the user rules otherwise. U12 is the user's.

### 10.1 What bears on 03

- **The user's uncommitted work is in three files 03 is meant to change.** Since before 01, the main checkout has held
  the user's edits to `CLAUDE.md` (invariant 8, and a sentence on science runs), to `science-runs/README.md` (the list
  of standard analyses), and to `science-runs/2026-11-08-science-2026.10.0/README.md` (its `relativistic/` analysis and
  a section "Standard analyses"), with `relativistic/` untracked. 03 is meant to:
  - update `CLAUDE.md`'s suite counts (log 01 §6; board §5);
  - add the run record's dated note (§4);
  - name the standard analyses a rebuild repeats (P12).

  No agent has staged these files, and none may. **U12** decides.
- **The workers' working directory was already documented.** `architecture-summary.md` §14's note of 2026-11-07
  (`one-loop-drain` 08) says that a Ray worker runs in the cluster's working directory, and that `Xav_EOS_data.csv` and
  `PRyMrates/` are then the cluster's start directory's. 02's issue
  `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]` says that 06's probe did not see
  it. That is true of the table's effect, which needs two different tables to show, but not of the mechanism, which 06
  recorded and 08 wrote down. Neither 02's plan nor its agent connected the two. The issue needs a dated correction,
  not a new finding.
- **The surfing tests' reference rows** (log 01 §6, log 02 §6) are
  `.documents/electroweak-eos/table/out/surfing_reference_rows.csv`: this tree's
  *untuned* rows at β = 1.5 and 3, which `test_surfing_scan` and `test_plot_surfing_histories` compare against. 02's
  S1-new is *tuned* (`threshold/out/s1_new.csv`), and it is a measurement, not a regression record of the untuned path.
- **The stores of 02** (`var/datastores/electroweak-eos-02-old` and `-new`, 16 shards each, with sidecars) and its
  exports and logs under `var/electroweak-eos/02/` (278 MB in all) are not committed. 02 left it to 03 to say whether
  they are kept.
- **`main` is at `ec6bca8`, an ancestor of this branch**, so the merge is a fast-forward. It is the user's.
- **The audit** (`.documents/peakewsurf-kicking-function/report.tex`, with its committed `report.pdf`) is a LaTeX
  record. `pdflatex` and `latexmk` are on this machine.

### 10.2 For the user to rule (U12)

**Ruled by the user, 2026-11-13:** "(a) for U12, please". The user's files are committed before 03 is dispatched, and
03 works on top of that commit. The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U12** — the user's uncommitted files | **(a) The user commits them before 03 is dispatched.** 03's Gate then asks for a clean tree, and 03 makes its changes to `CLAUDE.md` and the run record on top of the user's commit, as P24 and P23 say. | (b) 03 leaves the three files alone. The counts and the run record's note are written into the handover (§4.21) as left for the user, and the Close gate holds without them. |

### 10.3 Proposals for 03 (P21–P29)

- **P21 — the documents** (P10 made concrete).
  - `.documents/electroweak-eos-verification.md` gains §3, 03's checks.
  - `architecture-summary.md` gains a dated note at §8.5, under "a lattice QCD equation of state for g*(T)". It says:
    w is a separate table from g_ρ and g_s; it follows Laine & Meyer through the crossover and is built by
    `tools/build_w_table.py`; and the labels are `VERSION_LABEL` 2026.11.0 and `FINGERPRINT_FORMAT` 7, unchanged.
  - §14's note of 2026-11-07 on the workers' working directory gains a dated line: 02 measured that effect with two
    different tables.
  - `numerical-strategies.md` gains a dated note wherever it describes the table or Σ as it was (for example, "Σ_T's
    source": "0 beyond the table" now holds above 386.7 GeV).
  - Both documents are searched, not trusted to this list.
  - `surfing-temperature-verification.md` gains a dated pointer subsection after its §3. It gives the threshold on the
    new table, (3.002, 3.003], and points to `electroweak-eos-verification.md` §2.2. §1.8 is not edited.
  - The audit gains one dated paragraph at the end of its addendum section: the campaign, the label, the measured
    threshold and the verification's path. Its PDF is rebuilt with `latexmk`. If the PDF cannot be rebuilt, 03 says so
    and leaves the PDF as it is.
- **P22 — the handover**, `.documents/review-remediation-verification.md` §4.21, in §4.20's form. It covers:
  - what the campaign did, prompt by prompt with commits;
  - what changes for a build: the table, 01a's classifier, the label, and what the tip does with a `2026.10.0` store,
    measured by an inventory on a copy of `electroweak-eos-02-old`;
  - what moves, from 02: the threshold, the window on the run's dense grid, the 13 histories that change their kind of
    delivery, and the changes by run and in each figure's units;
  - the standard analyses a rebuild repeats (P12);
  - the cost: 02's sample times beside the run's block times;
  - the working-directory hazard for any cross-tree build;
  - the stores kept;
  - what stays open;
  - that the merge is the user's.
- **P23 — the run record** (README §4). A dated note in `science-runs/2026-11-08-science-2026.10.0/README.md` saying:
  - its histories were made at `2026.10.0`, with the w table that is a vacuum-mass ideal gas above about 100 GeV;
  - `electroweak-eos` replaced that table;
  - what 02 measured of the change, pointing to §4.21 and the verification's §2.4.

  Nothing above the note is edited. This is done under U12 (a) only.
- **P24 — `CLAUDE.md`, by description only** (under U12 (a) only). The suite counts become 26, 684 (219 + 465), 163,
  292 and 76, with the times from 03's own run. The long parenthesis that dates the counts is replaced by one sentence
  giving the date and the campaign that last changed a count. The "Run everything from the repository root" bullet
  gains a clause saying that `Xav_EOS_data.csv` is the output of `tools/build_w_table.py`, which `--check` compares.
  No rule is added.
- **P25 — the issues.**
  - None is closed.
  - `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]` gains a dated correction (§10.1)
    and stays open for the user.
  - `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]` is left as 02 left it.
  - `[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]` is unchanged.
  - `OPEN_ISSUES.md` §1.25 and its header say that the campaign is closed.
- **P26 — the user's open point in the run record** (under U12 (a) only). The user's draft of the run record's
  "Standard analyses" section says that `tools/plot_quanta_grid.py`'s categorical β axis cannot be read when the dense
  and sparse grids share it, and is "to be opened on the next campaign's board". 03 opens it on this board's §3, with
  the index, quoting `relativistic/PLAN.md` §4.3 Q4 for what is measured. It proposes no fix.
- **P27 — the surfing tests' reference rows stay.** They are the untuned path's regression record on this tree.
  S1-new does not replace them. 03 changes no test, and the handover says where each record is.
- **P28 — the stores stay.** `electroweak-eos-02-old`, `-new` and `var/electroweak-eos/02/` are kept, and listed in
  the handover with their sizes and `store_id`s. Retiring them is the user's.
- **P29 — the close.** The board is closed: row 03, the Close gate, a short status line, with the old one kept below
  a heading. `prompts/INDEX.md`'s row says the campaign is complete. The merge into `main` (a fast-forward) and the
  push are the user's, and 03 does neither.
