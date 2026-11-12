# Campaign — electroweak-eos

**Written:** 2026-11-12 on branch `electroweak-eos`, cut from `main` at `ec6bca8`, by Claude Opus 5.5, at
the user's request: "start planning the migration campaign around this new merged table". U1–U6 ruled
by the user as proposed on 2026-11-12, and U7 ruled out of scope (§6.1). P1–P12 stand, with P11 amended
by U7 (§6.2).

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
| 02 | `02-measure-the-effect.md` | the EW threshold on the surfing grids (S1, and a fine grid placed by the coarse one); the sample of `2026.10.0` histories old against new; the bath drift re-measured; optionally the notebook-side threshold (U5) | Opus |
| 03 | `03-documents-and-handover.md` | `architecture-summary.md` and `numerical-strategies.md` where they describe the EOS; the verification document; the issues; the indexes; the handover for the rebuild; closes the campaign | Sonnet |

Written one at a time, each after the previous one lands and is reviewed. 02's "old" side runs on
`main` at `ec6bca8`, or any tree before 01, in a worktree or a separate checkout. It never runs on 01's
tree with the old CSV swapped back in.

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Table** | the build script regenerates the installed CSV byte for byte from the committed inputs; `Xav_EOS_spline` evaluates the new table (its peak and its zero at the top row as §0.1 states); every moved pin is listed with its old and new value and a reason; values below the first changed row change only at the spline's coupling, measured and stated | 01 |
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
