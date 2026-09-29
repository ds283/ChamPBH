# Log 05 — Pin the kicking function; EOS hygiene; the paper-facing note

**Prompt:** prompts/review-remediation/05-kicking-function-and-eos-hygiene.md
**Commit:** the commit that adds this file — "Pin the kicking function and write the paper-facing note"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/05-kicking-function-and-eos-hygiene.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-29
**Result:** COMPLETE WITH DEVIATIONS

The pinning half of R4 is done.

- **The tests.** Every README §6.4 row is measured and within tolerance. The peak temperatures
  are those of the user's 2026-09-29 decision. `CosmologyModels/tests` goes from 6 to 12; prompt
  case 7 is a reference to prompt 02's test, not a copy.
- **The docstrings.** Both `w()` docstrings and the `Xav_EOS_spline.py` module docstring are in
  place.
- **The note.** `.documents/numerical-methods-for-paper.md` exists.

**One finding qualifies README §2 (c), and the orchestrator should take it to the user.** The
table–g consistency of "0.3 %" holds for the integrated ρ_R witness *at and below 100 MeV*, and
case 5 passes. But the table's Σ is not the Σ implied by the Saikawa–Shirai g's through the QCD and
electroweak crossovers.

- **The ρ_R witness mid-transition.** From 2×10⁴ GeV it reads 0.9847 at 31.6 GeV and 1.0179 at
  178 MeV.
- **The peaks.** The g's imply a QCD peak of 0.299 at 155 MeV (the table gives 0.3145 at 182 MeV)
  and an EW peak of 0.058 at 47.6 GeV (the table gives 0.0374 at 53 GeV).
- **Status.** Opened as `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`.
  This is not one of the prompt's literal stop conditions, since case 5 does not miss. It is
  that stop condition's subject: "the paper's Σ needs a decision from the authors". See
  Deviations 6.

## What shipped

Tree at dispatch: `89bd52e`. No numerical code changed. `VERSION_LABEL` is `"2026.2.0"` before
and after (not touched).

- **`CosmologyModels/tests/test_kicking_function.py`** (new). It has one class,
  `TestKickingFunction`, and six tests. Everything goes through `Xav_EOS_spline.w` (via
  `eos_reference.production_eos()`) and never reads the CSV.
  - **The grid.** Σ = 1 − 3w on a grid log-spaced at 1000 points per decade over [10 keV, 30 TeV],
    evaluated once in `setUpClass`.
  1. `test_three_peaks`. The argmax in [10 keV, 5 MeV], [50 MeV, 1 GeV] and [20 GeV, 1 TeV],
     against the `PEAKS` targets:
     - Σ 0.1007 / 0.3138 / 0.03733, ± 1e-3;
     - T 0.1605 MeV / 0.1819 GeV / 53.25 GeV, ± 5 % (the decision).
  2. `test_ee_profile`. Σ at 2, 0.5, 0.2, 0.1 and 0.05 MeV (0.0030 / 0.0345 / 0.0946 / 0.0680 /
     0.0029, ± 1e-3), and |Σ(20 keV)| < 1e-6.
  3. `test_ee_integral`. ∫Σ d ln T over [10 keV, 3 MeV], by Simpson at 1000 points per decade:
     0.1617 ± 2e-3.
  4. `test_w_is_one_third_outside_the_table`. `w == 1.0/3.0` exactly at T_min and T_max (read from
     the class), and at 9.99e-6, 5e-6, 1e-6, 1e-9, 2.35e-13, 2.6e4, 3e4, 1e6 and 1e16 GeV.
  5. `test_table_is_consistent_with_the_gs`. The ρ_R witness (`integrate_temperature_law(...,
     with_rho=True)`) to 10 keV from 5 MeV, 100 MeV and 2×10⁴ GeV: 1.00135 / 1.00005 / 0.99922,
     ± 5e-3 (Deviations 2).
  6. `test_the_2_MeV_freeze_is_not_the_production_path`. Three checks:
     - `QCD_Cosmology(0, GeV_units(), Planck2018())._eos` is an `Xav_EOS_spline`, and its
       `type_id` is `XAV_IMPROVED_EOS_IDENTIFIER`;
     - `SaikawaShirai_EOS_spline.w(1 MeV) == w(2 MeV)`;
     - `QCD_Cosmology.w(1 MeV) != w(2 MeV)`.
  - **Prompt case 7 is not duplicated.** It is
    `test_temperature_law.TestTemperatureLaw.test_spline_and_jax_derivatives_agree` (prompt 02's
    case 4), and the module docstring names it.
  - **Constants.** Every one carries a comment giving its source and the value this module
    measured. `CHAMPBH_TEST_REPORT=1` prints the measured values.
- **`CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py`, `w()` docstring** (`:197–201` →
  `:197–206`). The prompt's sentence is added verbatim, with "e⁺e⁻" written "e+e-", plus a
  line naming the campaign and prompt. The code is unchanged.
- **`CosmologyModels/GenericEOS/SaikawaShirai_EOS_jax_autodiff.py`, `w()` docstring**
  (`:227–230` → `:227–235`). The same text. The code is unchanged.
- **`CosmologyModels/GenericEOS/Xav_EOS_spline.py`, module docstring** (new, 32 lines, before
  the imports). It records what the CSV establishes about itself:
  - its columns and its 189 rows;
  - the range 10 keV to 25.1 TeV, at 20 rows per decade uniform in log10 T;
  - w = 1/3 at the bottom row to 7e-16, and |Σ| < 1e-9 in every row up to 15.8 keV;
  - **w = 1/3 − 3.3e-7 at the top row** (Deviations 5);
  - no row has w > 1/3 by more than 3.3e-11;
  - the table-row peaks and the spline peaks;
  - that its construction is not in the repository (commit `1759515`, unchanged since), with the
    board issue's name.

  It does not say how the table was built.
- **`.documents/numerical-methods-for-paper.md`** (new). The five sections the prompt asks for,
  each number tagged with its source (test or script, and commit). §2 carries the table–g
  finding, and §5 points at the board.

## Deviations from the prompt

### 1. Peak temperatures are the spline's, per the user's decision — STRUCTURALLY REQUIRED

- **What the prompt assumed.** §1 case 1 and README §6.4 took T_J = 0.1585 MeV, 0.1778 GeV and
  56.23 GeV. Those are the CSV rows with the largest Σ.
- **What is there.** The spline `w` evaluates peaks between rows. The EW peak is 5.3 % from
  56.23 GeV.
- **Done instead.** The board's decision of 2026-09-29 ("Option 1", `89bd52e`) took precedence:
  0.1605 MeV, 0.1819 GeV and 53.25 GeV, ± 5 %. The Σ targets are unchanged.
- **Measured.** At 1000 points per decade: 0.16033 MeV (−0.10 %), 0.18199 GeV (+0.05 %) and
  53.22 GeV (−0.05 %). This agrees with the orchestrator's 5000-per-decade probe to the grid
  spacing.
- This is the user's decision, not this agent's. It is listed so the log is complete.

### 2. Case 5's centres are prompt 02's post-R5 values — STRUCTURALLY REQUIRED

- **What the prompt says.** "1.005 ± 5e-3 from 5 MeV; from 100 MeV, 1.003 ± 5e-3; from
  2×10⁴ GeV, 1.003 ± 5e-3". These centres predate R5.
- **What was done.** README §6.4 says "prompt 05 takes the value prompt 02 records". Log 02
  recorded 1.00135, 1.00005 and 0.99922 (State handed to the next prompt, `47c50ae`), and those
  are the centres. The tolerance is the prompt's ± 5e-3. No target was loosened.
- **Both sets of centres pass.** Measured: 1.001346, 1.000053 and 0.999223. Against the prompt's
  literal centres these are 3.7e-3, 2.9e-3 and 3.8e-3 off, all inside ± 5e-3.

### 3. Case 5 is a new test; `test_temperature_law.py` is untouched — IMPLEMENTATION CHOICE

- **What the prompt says.** "Tighten prompt 01's case 5" appears under §1, the new module.
- **Alternatives.**
  - (a) Edit `test_temperature_law.test_rho_R_witness`.
  - (b) Add a stricter-centred witness to the new module and leave the old one in force.
- **Picked: (b).**
  - §1 names only the new file.
  - §5 acceptance 2 counts cases added to the suite.
  - (a) would have changed prompt 02's guard in a prompt that is not about the temperature law.
- **What "tighten" amounts to.** Centres on measured post-R5 values, and a new start point,
  100 MeV.
- **What it does not do.** At 2×10⁴ GeV → 10 keV the new ± 5e-3 is looser than the existing
  ± 3e-3 in `test_temperature_law`. That one is still in force, so nothing is weakened.
- **The cost.** Neither test's ± 5e-3 about 1.00135 can see R5 (log 02, observation 4). Cases 1
  and 2 of `test_temperature_law` see it.

### 4. Evaluation grid of 1000 points per decade, not 200 — IMPLEMENTATION CHOICE

- The prompt asks for "≥ 200". The decision noted that at exactly 200 per decade the EW argmax
  passes only by grid luck against the old target.
- At 1000 per decade the argmax is within 0.23 % in T of the spline's peak. The whole module runs
  in ≈ 0.5 s.
- The integral uses a separate grid, at 1000 per decade exactly on [10 keV, 3 MeV], so that both
  endpoints are knots. Simpson gives 0.161813, and `scipy.integrate.quad` gives 0.161813.

### 5. The CSV is not exactly 1/3 at its top row — STRUCTURALLY REQUIRED

- **What the prompt assumed.** §2 asks the module docstring to record "that w = 1/3 exactly at
  both ends".
- **What is there.** The bottom row is 0.3333333333333326, which is 1/3 to 7e-16. The top row,
  at 25 118.86 GeV, is **0.3333329996**, which is 1/3 − 3.3e-7.
- **What the docstring says.** What is there. The class returns exactly 1/3 at and beyond both
  ends, which case 4 pins. The step of 3.3e-7 in w at 25.1 TeV is recorded in the docstring.
  It is above the default `--T-init-GeV` of 2×10⁴ GeV.

### 6. The note does not say "table–g consistency at 0.3 %" unqualified — STRUCTURALLY REQUIRED (qualifies README §2 (c))

- **What the prompt assumed.** §3.2 asks for "the table–g consistency at 0.3 %". README §2 (c)
  calls the 0.3–0.5 % residual "the table-versus-g_ρ mismatch", and audit §4 concludes that the
  table "is compatible with the Saikawa–Shirai g's".
- **What is there** (scratch probes on `89bd52e` + this diff, below).
  - **As an integrated witness ending at or below 100 MeV,** the 0.3 % holds. From 2×10⁴ GeV the
    ratio at every T₁ ≤ 100 MeV is in [0.99788, 0.99922], and case 5 passes.
  - **At intermediate temperatures it does not hold.** It reads **0.9847 at 31.6 GeV** and
    **1.0179 at 178 MeV**. These are converged in `max_step`: 0.05 and 0.01 agree to 4e-8.
  - **The cause.** At fixed field, entropy conservation and continuity fix the Σ the g's imply:
    Σ_g = 4 − (4 + d ln g_ρ/d ln T)/(1 + ⅓ d ln g_s/d ln T).
    - The spline and jax classes give the same Σ_g to five figures, so this is not fit ringing.
    - **e⁺e⁻: they agree.** Σ_g peaks at 0.1000 at 0.159 MeV; the table at 0.1007 at 0.160 MeV.
    - **QCD: they do not.** Σ_g is 0.2990 at 155 MeV; the table is 0.3145 at 182 MeV.
    - **Electroweak: they do not.** Σ_g is 0.0580 at 47.6 GeV; the table is 0.0374 at 53 GeV.
    - The largest |Σ_table − Σ_g| is 0.078 at 138 MeV and 0.021 at 45 GeV. At and below 110 MeV
      it is ≤ 2.5e-3.
    - Integrated over [10 keV, 2×10⁴ GeV] the difference in ∫Σ d ln T is only 1.1e-3, which is
      why the witness recovers.
- **Done instead.** The note states both facts, and the finding is opened on the board. No test
  pins the intermediate values; that would be a new target, and it is not this prompt's to set.
- **Why this is flagged.** The table's EW and QCD peaks are the values the paper quotes, and the
  temperature law's g's do not reproduce them. By the paper's formula, β_min is 3.02 at the
  table's EW Σ and 2.43 at Σ_g's. So the prompt's §6 stop condition "the paper's Σ needs a
  decision from the authors" is met in substance, though its literal trigger (case 5 missing) is
  not. The work is committed because every acceptance test passes and nothing was changed to
  make them pass. The orchestrator should put this to the user.

### 7. Case 6 reads `QCD_Cosmology._eos` and also checks `type_id` — IMPLEMENTATION CHOICE

- `LambdaCDM_GenericEOS` has no public accessor for its EOS, so the test reads the private
  attribute.
- It also asserts the public `type_id`, which is what the datastore keys on, and it evaluates
  `w` through the cosmology object, the production path.
- `QCD_Cosmology` builds with `store_id=0` and `Planck2018()`, with no Ray and no datastore.
  Checked: neither `ray` nor `sqlalchemy` is imported.

### 8. The note gives β_min arithmetic — IMPLEMENTATION CHOICE

- In §2 the note converts Σ to β_min with the paper's own formula: 2.92 at 0.04, and 3.02 at the
  table's 0.0373.
- It is labelled as arithmetic, not a code result. It is included because the paper's table
  rounds the EW peak to 0.04.

## Verification performed

Everything was run from the repository root with `venv/bin/python` on `89bd52e` plus this diff.
Scratch probes, not committed, are in the session scratchpad: *(Added 2026-09-30 by the orchestrator, at the user's request: the probes are now kept
unchanged in [`05-probes/`](05-probes/), with how to run them.)*

- `probe05.py`: peaks at 200, 1000 and 5000 per decade; profile; integral by Simpson, trapezoid
  and `quad`; the ends; the freeze; table statistics; the witness at two `max_step`.
- `break05.py`: the breakage check.
- `witness_scan.py`: the ρ_R witness from 2×10⁴ GeV at 37 T₁ log-spaced over [10 keV, 10 TeV].
- `sigma_implied.py` and `sigma_implied2.py`: Σ_g from the spline and jax classes against the
  table.

### README §6.4, row by row (I ran this: `CHAMPBH_TEST_REPORT=1 … test_kicking_function -v`)

| Feature | Target | Measured | |
|---|---|---|---|
| e⁺e⁻ peak | Σ 0.1007 ± 1e-3; T 0.1605 MeV ± 5 % | **0.100732 at 0.16033 MeV** | ✅ |
| QCD peak | Σ 0.3138 ± 1e-3; T 0.1819 GeV ± 5 % | **0.314532 at 0.181993 GeV** (Σ 7.3e-4 off, the narrowest margin) | ✅ |
| EW peak | Σ 0.03733 ± 1e-3; T 53.25 GeV ± 5 % | **0.037436 at 53.22 GeV** | ✅ |
| Σ(2 MeV) / Σ(20 keV) | 0.0030 ± 1e-3 / < 1e-6 | **0.002946 / 5.17e-8** | ✅ |
| Σ at 0.5 / 0.2 / 0.1 / 0.05 MeV (prompt case 2) | 0.0345 / 0.0946 / 0.0680 / 0.0029 ± 1e-3 | **0.034450 / 0.094564 / 0.067984 / 0.002858** | ✅ |
| ∫Σ d ln T, [10 keV, 3 MeV] | 0.1617 ± 2e-3 | **0.161813** | ✅ |
| w outside the table | exactly 1/3 | exactly 1/3 at all 11 points; just inside, 1/3 − 3.7e-12 (bottom) and 1/3 − 3.3e-7 (top) | ✅ |
| ρ_R witness 5 MeV → 10 keV | 1.00135 ± 5e-3 (Deviations 2) | **1.001346** | ✅ |
| same, from 100 MeV | 1.00005 ± 5e-3 | **1.000053** | ✅ |
| same, from 2×10⁴ GeV | 0.99922 ± 5e-3 | **0.999223** | ✅ |
| freeze not in production | as prompt case 6 | `SaikawaShirai_EOS_spline.w` is 0.33357281449212284 at both 1 and 2 MeV; `QCD_Cosmology.w` is 0.32973 and 0.33235 | ✅ |
| spline vs jax derivative (prompt case 7) | ≤ 1e-6 in d ln g_s/d ln T | still passes in `test_temperature_law` (suite run below); log 02 measured 2.44e-7 worst | ✅ (referenced) |

**Grid dependence of the peaks** (`probe05.py`):

| Grid | e⁺e⁻ | QCD | EW |
|---|---|---|---|
| 200 per decade | 0.160424 MeV | 0.18237 GeV | 53.2728 GeV |
| 1000 per decade | 0.16033 MeV | 0.181993 GeV | 53.2214 GeV |
| 5000 per decade | 0.160469 MeV | 0.181871 GeV | 53.253 GeV |

Peak Σ is the same to six figures on all three grids. The table rows
(pandas, same tree) reproduce the audit: 0.100718 at 0.1585 MeV, 0.313767 at 0.1778 GeV, 0.037326
at 56.23 GeV.

**The witness convergence.** `max_step` 0.05 and 0.01 give 1.001346 / 1.000053 / 0.999223 for
both. The whole scan and its mid-transition values are in Deviations 6.

**The tests fail when they should** (I ran this: `break05.py`, which patches the class in memory
and runs the module).

- **With the 2 MeV freeze in production** (`Xav_EOS_spline.w = SaikawaShirai_EOS_spline.w`):
  27 failures in all six tests. Examples:
  - e⁺e⁻ peak Σ −0.0006 at 4.99 MeV;
  - integral −0.0041;
  - witness 0.83 / 0.83 / 0.77;
  - "production w is not frozen" fails.
- **With the table shifted by 6 % in T:** 11 failures.
  - All three peak temperatures fail, at 5.6–5.8 %.
  - The profile fails at four of its points and at 20 keV.
  - The witness from 100 MeV fails.
  - `w` at T_min is no longer exactly 1/3.

**The suites (I ran these).**

- `CosmologyModels/tests`: **6 → 12**, `Ran 12 tests in 5.101s OK`. The six new tests are cases
  1–6; case 7 is a reference.
- `ComputeTargets/tests`: **13 → 13**, `Ran 13 tests in 43.624s OK`.

**Formatting.** `black --check` is clean on the four changed Python files. All three production
files were black-clean at `HEAD` before the edit (`git show HEAD:… | black --check -`), and
`black` changed nothing after it.

**What I reasoned but did not run.** The note's §3 and §4 figures are quoted from logs 02–04 and
the board, with their commits, and not re-measured:

- the knot counts;
- the callback accuracies;
- the passenger timings;
- the baseline abundances.

Prompt 06 re-runs them on the final tree.

## Observations not acted on

1. **The table's Σ and the Saikawa–Shirai g's disagree through the QCD and EW crossovers**
   (Deviations 6). → board §3 `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`.
   - Audit §4's "compatible with the Saikawa–Shirai g's" and README §2 (c)'s attribution of the
     residual are qualified by it.
   - The audit is additive-only and is not in this prompt's files, so no addendum was written.
     The issue records the measurement.
2. **`[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` is still open.**
   - This prompt edited `SaikawaShirai_EOS_jax_autodiff.py`, but its §2 hygiene lists only the
     two `w()` docstrings and the module docstring.
   - The jax class's "units of the output will be 1/GeV" (`:204`, `:221`), the base `dG_s_dlogT`
     docstring and the "600 keV" comment were left alone.
   - The board entry is unchanged.
3. **Two sub-1e-5 features of the spline `w`.** Neither is actionable, and no issue is opened.
   - The step of 3.3e-7 in w at 25.1 TeV (Deviations 5). It is above the default start
     temperature.
   - An undershoot to Σ = −2.6e-6 near 24 keV. No table row goes below Σ = −1e-10, so this is
     the cubic's ringing.
4. **README §6.4's case-5 centres (1.005, 1.003) and §2 (c)'s pre-R5 figures are stale.** Log 02
   already reported this, and README §6.4 already defers to prompt 02's values. No issue.
5. **The paper's e⁺e⁻ value of 0.03 matches nothing in the table.** The CSV has exactly one
   version in git (`1759515`), so the §6 stop condition ("a version of the table that is not the
   one in the repository") has no evidence for it.
   - The paper's 0.04 (EW) and 0.31 (QCD) are this table's peaks, rounded.
   - The paper's own margin notes already call 0.03 wrong.
   - Reported in the note; not reconciled.

## State handed to the next prompt

- **New test module:** `CosmologyModels/tests/test_kicking_function.py`, class
  `TestKickingFunction`, six tests:
  - `test_three_peaks`
  - `test_ee_profile`
  - `test_ee_integral`
  - `test_w_is_one_third_outside_the_table`
  - `test_table_is_consistent_with_the_gs`
  - `test_the_2_MeV_freeze_is_not_the_production_path`

  Reproduce with
  `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest CosmologyModels.tests.test_kicking_function -v`.
- **Measured on `89bd52e` + this diff, at 1000 points per decade:**
  - **Peaks:** e⁺e⁻ 0.100732 at 0.16033 MeV; QCD 0.314532 at 0.181993 GeV; EW 0.037436 at
    53.22 GeV.
  - **The e⁺e⁻ profile:** Σ = 0.002946 (2 MeV), 0.034450 (0.5 MeV), 0.094564 (0.2 MeV), 0.067984
    (0.1 MeV), 0.002858 (50 keV), 5.17e-8 (20 keV). ∫Σ d ln T over [10 keV, 3 MeV] = 0.161813.
  - **The ρ_R witness to 10 keV:** 1.001346 (from 5 MeV), 1.000053 (from 100 MeV), 0.999223 (from
    2×10⁴ GeV). From 2×10⁴ GeV it is in [0.99788, 0.99922] at every T₁ ≤ 100 MeV. It reads 0.9847
    at 31.6 GeV and 1.0179 at 178 MeV.
  - **Σ_g peaks** (from the g's, fixed field): 0.1000 at 0.159 MeV, 0.2990 at 155 MeV, 0.0580 at
    47.6 GeV.
- **Suite counts after this prompt:** `CosmologyModels/tests` 12 (≈ 5 s); `ComputeTargets/tests`
  13 (≈ 44 s).
- **The paper-facing note** is `.documents/numerical-methods-for-paper.md`. Its §§3–4 figures are
  quoted from logs 02–04 and are for prompt 06 to re-measure on the final tree.
- **Open for the user:** `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`,
  that is, whether the table's Σ or the g's describe the intended plasma through the QCD and EW
  crossovers. It bears on the paper's Σ peaks and β_min.
