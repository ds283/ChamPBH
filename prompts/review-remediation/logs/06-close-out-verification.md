# Log 06 — Close-out verification and the handover to the numerical campaign

**Prompt:** prompts/review-remediation/06-close-out-verification.md
**Commit:** the commit that adds this file — "Close out the review-remediation campaign and hand over"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/06-close-out-verification.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE WITH DEVIATIONS

**Every README §6 row meets its target on the final tree (`01e5975`).** No production file and
no test changed. Both suites pass, at 12 and 13 tests, and every file in `git diff f5896bb..01e5975`
is inside the campaign's scope. All five earlier logs exist, and each has Result `COMPLETE WITH
DEVIATIONS`. No §4 stop condition fired.

Two rows meet a target that the **user** restated during the campaign:

- **§6.2 row 3** (option C, 2026-09-29). Against the literal five-figure 0.25409 the Yp is
  1.49e-5 off; against the decided pin it is 0.
- **§6.4's peak temperatures** ("Option 1", 2026-09-29). Against the original 56.23 GeV the EW
  peak is −5.35 %, outside ±5 %; against the decided 53.25 GeV it is −0.06 %.

Both are reported in full in the verification document. Neither is a miss under the targets in
force.

**One defect was found, in code outside the campaign's scope, and is reported rather than fixed.**
The hard-reflection caption on `plot_ScalarModel.py`'s figures reads the wrong key and always
prints 0. It is opened as `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`.
It was found by reading the code for the handover, not by a re-run, and it touches no §6 row
(Deviations 2). The orchestrator may want to put it to the user.

## What shipped

No production code and no test. `VERSION_LABEL` is `"2026.2.0"` before and after (not touched).

- **`.documents/review-remediation-verification.md`** (new). It has five sections:
  - §1, the before/after table. Every row of README §6.1–§6.4, with the `f5896bb` value, the
    target, the value on `01e5975` and the witness. §1.5 gives the suite counts and the scope
    check of the diff, file by file.
  - §2, what changed, one paragraph per prompt, citing each log and its commit.
  - §3, what was deliberately not changed: README §0.4 and the 15 open board issues.
  - §4, the handover (README §7):
    - the fresh-database rule;
    - the run list;
    - the expected magnitudes;
    - the paper figures and the redshift-label question for each;
    - the issues to fix first.
  - §5, how to reproduce.
- **`prompts/review-remediation/logs/06-probes/`** (new): `probe06_deriv.py`, `probe06_knots.py`
  and a `README.md`. These print the two figures that no test prints (Deviations 1).
- **`prompts/review-remediation/IMPLEMENTATION_STATE.md`.**
  - The header reads **COMPLETE — 6 of 6**, with the date and the verified tree.
  - Row 06 is landed.
  - R4 is done.
  - §3 gains one issue and qualifies `[00-hard-reflection-count-is-stored-but-never-reported]`.
- **`.documents/OPEN_ISSUES.md`.**
  - The count goes from 14 to **15**; the date stays 2026-09-30.
  - One row is added.
  - The hook of the N1 row is corrected: it said the count "appears in no plot or caption", and
    a caption does show it, wrongly.
  - **No issue was closed.** No earlier prompt closed one and forgot the index: the board's §3
    and the index both list the same 15 names (checked by `diff` of the sorted names).
- **`prompts/INDEX.md`.**
  - The status goes to **complete**, and the date to 2026-09-30.
  - The open-issue count goes to 15.
  - A pointer to the handover is added.
- **`.documents/audit-2026-09-29/README.md`.** One dated, italic line under the title points at
  the verification document. Nothing else in the file changed.

## Deviations from the prompt

### 1. The two scratch probes are kept beside the log — IMPLEMENTATION CHOICE

- **The problem.** Two figures in the before/after table come from no test: the worst
  derivative disagreements (1.259e-8 and 2.438e-7), which the tests assert only as ≤ 1e-6, and
  the re-measured knot count (636).
- **Alternatives.** (a) Leave the probes in the session scratchpad, as prompts 01–04 did. (b)
  Commit them beside the log (chosen).
- **Reason.** README §5 rule 8 wants provenance for every figure. The user asked for prompt 05's
  probes to be kept (`0791935`), which sets the precedent. The probes are neither production
  code nor tests, and `black` formats them.

### 2. A defect outside the scope is reported as a new issue, not a stop — IMPLEMENTATION CHOICE

- **The prompt.** It says "if a re-run reveals a defect, that is a §4 stop". The dispatch says "a
  defect found now is reported, not fixed".
- **What was found.** The handover's run list asks for `hard_reflections` per history, so I
  traced where that count is read. `ComputeTargets/ScalarModel.py:1271` stores it in
  `extra_data` as `number_hard_reflections`. `extract_common.py:216` looks for
  `hard_reflections`, and so always prints "Hard reflections: 0".
- **Why this is not a stop.**
  - No re-run revealed it; it came from reading the code.
  - It is in code the campaign never touched (N1, out of scope by README §0.4).
  - No §6 row depends on it.
- **What was done.** It is opened as its own §3 issue and in the index. The N1 issue it qualifies
  gets a dated note. The handover warns the reader not to trust the caption.
- **Alternative.** Stop and ask. It was rejected because every stop condition in the prompt's §4
  is unmet. The orchestrator can still take it to the user.

### 3. "Final SHA" in the board header — STRUCTURALLY REQUIRED

- **What the prompt assumed.** The board header can carry "the final SHA".
- **What is there.** This commit is the campaign's final commit, and a commit cannot name its own
  SHA.
- **Done instead.** The header names the tree that was verified, `01e5975`. It adds that the
  close-out commit adds only documents, and says where to find its SHA: `git log -1` on this log.
  Every measurement was taken on `01e5975`. After the documents were written, both suites were
  re-run, with the same counts, OK.

### 4. The paper-facing note is checked, not edited — IMPLEMENTATION CHOICE

- **Background.** Log 05 left the note's §3–§4 figures "for prompt 06 to re-measure on the final
  tree".
- **Re-measured:**
  - the knot count, 636 in [0.02, 5] MeV at 250–309 per decade;
  - the callback accuracies;
  - the passenger timing;
  - the baseline abundances.
- **Result.** All agree with the note to the figures it prints. The verification document §2
  says so.
- **Why not an addendum to the note.** The prompt's §2–§3 list the files to write, and the note
  is not among them. With nothing to correct, an addendum would add no information.

### 5. Issues that an earlier prompt pointed at "prompt 06, if its scope allows" are left open — IMPLEMENTATION CHOICE

Two issues say prompt 06 might take them. Its §2–§3 do not include either, and it changes no code.
Both stay open and are listed in the verification document §3.

- `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`, a dated addendum to
  `numerical-strategies.md` §7;
- `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]`, comment-only edits to production
  files.

### 6. The figure table uses the paper repository — IMPLEMENTATION CHOICE

The handover asks which paper figures carry the redshift-label offset. The answer depends on each
figure's x-axis, which the captions do not always state.

- **What I read** (read-only), in `/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/`:
  - the axis text of each figure PDF, with `pdftotext`;
  - each figure file's `git log` date in that repository.
- **What it shows.** Two figures (`DEDenomPlot`, `DEpolePlot`) are plotted against z, and there is
  no dark-energy code in ChamPBH.
- **How the table is phrased.** Every row where provenance is unknown ends in a question for the
  authors, as the prompt asks.

## Verification performed

**Tree.** Everything ran on `01e5975` (`git rev-parse HEAD` at the start and before the commit),
from the repository root, with `venv/bin/python`, on 2026-09-30. I ran all of it myself.

**Per-package suite counts** (a count that falls is a stop; none fell):

| Package | before (`01e5975`) | after (this commit's tree) |
|---|---|---|
| `CosmologyModels/tests` | **12**, OK: `Ran 12 tests in 4.965s`, 6.43 s wall. jax is importable and the case ran, not skipped | **12**, OK: `Ran 12 tests in 5.058s` |
| `ComputeTargets/tests` | **13**, OK: `Ran 13 tests in 44.468s`, 45.92 s wall | **13**, OK: `Ran 13 tests in 43.311s` |

**The prompt's §1 items:**

1. **The three audit scripts.**
   - `tlaw_check.py` (2.3 s): `kappa=1` equals `exact` at all eight rows; T_CMB is 40.0754 in
     both. **The `kappa=1/ln10` column double-divides**, as prompt 02 said: 39.4574 at T_CMB, and
     ρ ratios 1.35–10.96. It is reported as such, and the script is not edited.
   - `eos_consistency.py` (0.9 s): 1.00135, 1.00005 and 1.00002.
   - `spline_test.py` (1.2 s): reproduces the audit's §2 table to every printed figure.
2. **Both suites**, with the counts and wall-clock above. `CHAMPBH_TEST_REPORT=1` printed:
   - the temperature-law table (every N_code − N_exact within ±3.7e-8; ρ ratios 0.99790 / 0.99915
     / 0.99922);
   - the kicking-function values (0.100732 at 160.3 keV, 0.314532 at 182 MeV, 0.037436 at
     53.22 GeV; ∫Σ = 0.161813; witnesses 1.001346 / 1.000053 / 0.999223);
   - the callback maxima (5.187e-17; 9.748e-9 / 7.396e-7);
   - the end-to-end run (Yp −1.89e-6, D/H −8.85e-5 against prompt 03);
   - the baseline.
3. **`tools/bbn_baseline.py`** (9.1 s wall, 7.2 s solve): `PRyM_version=bf24c3d+cham03`,
   Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017. These are identical to
   log 04.
4. **The scope check.** `git diff --stat f5896bb..HEAD` covers 57 files, +7682 / −187; the
   verification document §1.5 maps each production file to its prompt.
   - `git diff` of `main.py`, `PRyM/PRyM_main.py` and the jax class shows only the version label,
     the marked `dTNPdt` patch and a docstring.
   - `Xav_EOS_spline.py` changed only in its module docstring.
   - `git diff --stat` over `thirdparty/`, `ComputeTargets/ScalarModel.py`, `Xav_EOS_data.csv`,
     `config/` and `Datastore/SQL/ObjectFactories/ScalarModel.py` is empty.

**The measurements added for the §6.2 rows**, which the tests assert but do not print:

- `prym_fixtures` `zero` / `reference` / `constant` / `oscillating`: 7.81 / 7.49 / 7.82 / 8.78 s.
- 0 `RuntimeWarning`s in every case.
- `zero` and `reference` are identical to all printed figures.
- `constant`: Yp 0.2540937879, D/H 2.671499971. `oscillating`: Yp 0.2469265751, D/H 2.787693732.

**The added probes** (`logs/06-probes/`):

- `probe06_deriv.py`: max |d ln g_s/d ln T| on the grid is 1.273. Against the central difference,
  worst 1.259e-8 at 180 MeV; against jax, worst 2.438e-7 at 180 MeV. 0 of 60 points exceed 1e-6
  in either. This matches log 02.
- `probe06_knots.py`: 636 samples in [0.02, 5] MeV, 265.2 per decade on average and 250.0–308.6
  locally; 2304 over [0.1 eV, 100 MeV]; 1167 over [0.3 keV, 10 MeV]. This matches log 04.

**Arithmetic in the handover** (from the fixture values above):

- r = 0.08 against the baseline: D/H +8.498 %, Yp +2.919 %, N_eff +0.669.
- `exponential.yaml` gives 125 values of β, Δβ = 0.2008. `beta 1.1–3.0` at 20 per unit gives 38,
  Δβ = 0.0514. Both use `main.py:955`'s formula, evaluated in Python.

**Reasoned, not run:**

- the hard-reflection key mismatch (code reading; a plot needs a store);
- everything the earlier logs already mark as not run (`plot_by_beta.py`'s printing and drawing,
  and the refusal path inside `compute_BBN_data`).

## Observations not acted on

1. **The hard-reflection caption always prints 0.** →
   `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]` (Deviations 2). The
   count is stored under `number_hard_reflections` (`ScalarModel.py:1271`) and read as
   `hard_reflections` (`extract_common.py:216`). It also qualifies audit §8, which says no plot
   reports the count.
2. **`prompts/INDEX.md` had not moved since planning.** It still said "planned — 6 prompts
   written, 0 landed" and "7" open issues, while five prompts had landed and the index held 14.
   This prompt's §3 updates it, which corrects that. No issue.
3. **The board header's "Code" line omits three files**: `SaikawaShirai_common.py` (prompt 02,
   R5), and `SaikawaShirai_EOS_jax_autodiff.py` and `Xav_EOS_spline.py` (prompt 05, docstrings).
   All three are within README §0.4 and §3. Left as written; the verification document §1.5 gives
   the complete map. No issue.
4. **Board §1 rows 02–05 carry commit subjects, not SHAs.** They are `47c50ae`, `ec206a3`,
   `eba4473` and `bb840f6` (from `git log --diff-filter=A` on each log). Those rows are the earlier
   prompts', and are left as written; the verification document §2 gives the SHAs. No issue.
5. **Two seeded issues cite line numbers that have moved.**
   - `[00-initial-field-value-is-hard-coded-and-unchecked]` cites `main.py:814`; φ\* is now set
     at `main.py:815–817`, because prompt 02 added three comment lines.
   - README §6.1 and log 02 cite `plot_by_beta.py:67` for the version label; it is now `:74`.
   Cosmetic. No issue.
6. **The paper's figure provenance** is a set of questions for the authors (verification document
   §4.4). The key question is whether `DEDenomPlot` and `DEpolePlot` came from ChamPBH output,
   since there is no dark-energy code in this repository. Not an issue on this board; it is the
   authors'.

## State handed to the next prompt

There is no next prompt in this campaign. This section is the handover's summary for whoever plans
the numerical campaign. The full handover is **`.documents/review-remediation-verification.md`
§4**.

- **Start from an empty database.** Every store built under `VERSION_LABEL = "2026.1.1"` is
  invalid. The label is now `"2026.2.0"` (`main.py:83`, `plot_by_beta.py:74`). Lookups do not
  filter on the version, so an old store's rows would be reused silently. The one exception is
  `BBNData`, which fails loudly on its missing `failure_reason` column.
- **The run list.**
  - `exponential.yaml` restricted to 1.1 ≤ β ≤ 3 at finer spacing (now Δβ ≈ 0.2). Keep M = 0.5 M_P,
    Λ = 1e-3 eV, T\* = 2×10⁴ GeV, φ\* = 5 M_P, π\* = 0, `log10-one-plus-z-high` 85 and the 1e-8
    tolerances.
  - The SM baseline (`tools/bbn_baseline.py`, drawn by `plot_by_beta.py`).
  - The review's three cheap tests:
    - the ratio spline is now production; compare stored against thermodynamic ρ_SM instead;
    - there is no ρ_NP-below-1-MeV switch; it needs a scratch driver or a code change;
    - the two-β `density_NP_ratio`-against-T_J overlay is not in the tree.
  - `density_NP_ratio` at 1 MeV for β = 2.
  - `hard_reflections` per history, read from `extra_data["number_hard_reflections"]` and not
    from the caption.
- **Expected magnitudes.**
  - For a parked field with r = 0.08: D/H **+8.50 %**, Yp **+2.92 %**, N_eff **+0.669** against
    the baseline (Yp 0.2468872958, D/H 2.462251065 × 10⁻⁵).
  - `density_NP_ratio` ≈ 0.08 at 1 MeV for β = 2 is H1's prediction.
  - D/H differences below ≈ 7e-4 relative are PRyMordial noise.
  - New z labels differ from old ones by 1 + z = e^{1.422} ≈ 4.15.
- **Decide the network before the survey.** `small_network` has never reached PRyMordial, so every
  result so far used the full network.
- **Paper figures.**
  - `DEDenomPlot` and `DEpolePlot` are against z. They carry the offset if they came from ChamPBH,
    and their provenance is unknown.
  - `PhiPlot` carries it only if its N was derived from the stored z.
  - Every history figure, and `BBNdhPlot`, is affected by R1 whatever its axis.
  - `SigmaPlot` is not affected.
- **Fix first, if wanted:**
  - H5, `[00-adiabaticity-diagnostic-omits-the-source-response-term]`, before regenerating
    `AdiabaticHistory` rows;
  - H8, `[00-initial-field-value-is-hard-coded-and-unchecked]`;
  - `[03-small-network-flag-is-never-read-by-prymordial]`;
  - `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`.
- **Open issues at close: 15**, all on the board's §3 and indexed in `.documents/OPEN_ISSUES.md`
  §1.1.
- **Reproduce the whole verification** from the repository root in about 75 s, with the commands
  in the verification document's provenance table.
