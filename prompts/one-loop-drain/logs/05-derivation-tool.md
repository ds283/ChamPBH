# Log 05 — the derivation tool

**Subject:** Add the spectrum model and the derivation tool, run on 02d's sample
**Commit:** this commit, on branch `one-loop-drain`, on top of `88c6ae5`
**Date:** 2026-11-06, by Claude Opus 5.5
**Result:** **COMPLETE.** The Derive gate holds (`.documents/one-loop-drain-verification.md` §5.8).
- **The spectrum module.** `ComputeTargets/quanta_spectrum.py`, pure: U4's shape for every generation (U16) with U17's
  end of the associated piece, `SPECTRUM_MODEL = "U4-U17-v1"`; x_p = 0.62, κ = 3.96578 (fitted at L = 90, the median of
  the Table's five rows: 4.1334, 4.0178, 3.9658, 3.9235, 3.8821), x_* = 0.0058, 49; I and J in closed form; G and Ω
  tabulated once per scale, with closed-form asymptotes beyond the tables.
- **The kernels against quadrature** (§2.1): G within 8.1 × 10⁻¹² inside its table and 5.6 × 10⁻¹¹ to four decades
  beyond, Ω within 2.3 × 10⁻⁹ inside and 8.4 × 10⁻¹⁵ beyond, the moments within 4.4 × 10⁻¹⁶, at c_a = 1, 0 and 10; every
  asymptote within 10⁻⁷ (7.1 × 10⁻¹² at worst) at its table's end.
- **The tool.** `tools/quanta_measures.py EXPORT_ROOT [--assoc-scale C]`, from 04's exports alone: K(N) rebuilt from the
  bounce rows and checked against fields.csv; n_g, n̄_j, δ_stim, E(N) for both masses, the phase slip and the
  backreaction ratio; `quanta_bounces.csv`, `quanta_samples.csv`, `quanta_histories.csv`, every row naming the model.
- **Against the probe** (§2.2), the spectrum collapsed to its peak on P-2197 at A = 1: E within 2.5ε relative of the probe's `excess` at all 3 081 compared samples for both masses (2.4ε field, 2.5ε tracker), the 1 914 samples with K(N) = 0 matched as blanks, none beyond the tolerance; K(N) equal to the payload ledger's bit for bit at all 4 995 samples.
- **The run on 02d's sample** (§1.6): 230 of 230 tasks re-integrated, every `DrainCandidate` equal to 02d's by `repr` (82 139), stored, exported and the stores deleted, in 1 601 s with five workers; the merged histories.csv equal to its parts; the tool at c_a = 1, 0 and 10 measures all 230 (about 52 s each), the K check within 1.46 × 10⁻¹¹. δ_stim is 2.2 × 10⁻⁷⁵ to 1.3 × 10⁻³ (median 3.5 × 10⁻¹⁷); E at most 7.2 × 10⁻¹⁶.
- **Suites:** before (the dispatch tree, see §4): 26, 651 (219 + 432), 163, 173, 72. After: 26, 665 (219 + 446), 163, 191, 72, all passing.
- **Issues:** none opened or closed; `[00-later-bounces-act-on-quanta-already-present]` narrowed with δ_stim on the
  sample (board §3, `.documents/OPEN_ISSUES.md`). The index count is unchanged (29).

Orchestrator note 05 (§0, rulings 1–12 and the conventions) governed where it differs from the prompt. Each ruling is
classified in §2. `FINGERPRINT_FORMAT` stays 7 and `VERSION_LABEL` 2026.10.0; nothing under `ComputeTargets/` changed but
the new module and its test, and nothing under `ObjectFactories/`.

## 1. What shipped

- **`ComputeTargets/quanta_spectrum.py`** (new, pure). The constants at the top, each with its source quoted
  (`X_PEAK`, `BETA2_MAX_BY_L`, `BETA2_AT_5_OMEGA_T`, `X_AT_5_OMEGA_T`, `KAPPA_FIT_L`, `kappa_fit`, `KAPPA_BY_L`, `KAPPA`,
  `X_STAR`, `ASSOC_RATIO_AT_X_STAR`, `ASSOC_COEFFICIENT`, `ASSOC_END` for U17, `DEFAULT_ASSOC_SCALE`); `SPECTRUM_MODEL`;
  the grids (`G_Y_MIN`, `G_Y_MAX`, `G_LOG_Y_STEP`, `OMEGA_U_MAX`, `OMEGA_U_STEP`) and the tabulation's Gauss–Legendre
  rule; the shape (`s_dir`, `s_assoc`, `shape`); the closed-form `moment`; `QuantaSpectrum` (one per scale: `model`,
  `assoc_scale`, `C`, `I`, `J`, `log_I`, `log_J`, `log_s_peak`, `s`, `G`, `G_of_log_y`, `G_small`, `G_large`, `log_Omega`,
  `Omega`, `log_Omega_large`, and the references `moment_quad`, `I_quad`, `J_quad`, `G_quad`, `Omega_quad`); `spectrum(c_a)`,
  cached; `PeakSpectrum`/`PEAK_SPECTRUM` for §2.2; `LOG_TWO_PI_SQUARED`. The module docstring derives every asymptote and
  bounds what each leaves out.
- **`tools/quanta_measures.py`** (new): `history_dir` (by `alpha_path` and `history_directory`), the readers `Bounce` and
  `Sample`, `KHistory` (K(N) rebuilt, the leftover placed), `K_check`, `Generations`, `matter_excess`, `occupation_overlaps`,
  `phase_slips`, `backreaction_ratios`, `measure_history`, `run`, `main`; the tolerances `K_ULP_TOLERANCE` (8ε) and
  `K_CHECK_TOLERANCE` (10⁻⁹), each with its reason; the three files' columns (`BOUNCE_OUTPUT_COLUMNS`,
  `SAMPLE_OUTPUT_COLUMNS`, `HISTORY_OUTPUT_COLUMNS`).
- **`extract_common.history_directory(beta, M_eV, n)`** (ruling 9), and `plot_ScalarHistory.py`'s `plot_path` and `csv_path`
  calling it (their strings unchanged), with its import.
- **Tests (new):** `ComputeTargets/tests/test_quanta_spectrum.py` (14), `tools/tests/test_quanta_measures.py` (16),
  `tools/tests/test_history_directory.py` (2); the helper `tools/tests/quanta_exports.py` (not a test module), which writes
  constructed and stored histories' exports by 04's writers.
- **Scripts** (`.documents/one-loop-drain/derive-05/`): `kernels_05.py` (§2.1), `probe_05.py` (§2.2), `run_05.py` and
  `launch_05.sh` (§1.6), `report_05.py` (§1.6's report); outputs in `out/`: `kernels.txt`, `probe.txt`, `run.txt`,
  `tool-assoc1.txt`, `tool-assoc0.txt`, `tool-assoc10.txt`, `report.txt`.
- **Documents:** verification §5 (new); dated notes in `.documents/architecture-summary.md` (at its end) and
  `.documents/numerical-strategies.md` (§3, after 03's); `CLAUDE.md` (the counts); the board; `.documents/OPEN_ISSUES.md`.

## 2. Deviations from the prompt

The note's rulings, each as followed:
1. **U17, the associated piece's upper end** (ruling 1). *STRUCTURALLY REQUIRED*; ruled by the user. s_assoc = 49 x_*³/x
   for x ≤ x_p and 49 x_*³/x · e^{−κ(x − x_p)} above, with the direct piece's κ; quoted beside the constants (`ASSOC_END`)
   and named by `SPECTRUM_MODEL = "U4-U17-v1"`. s is continuous at x_p and s(x_*) = 50 s_dir(x_*) at c_a = 1 (tested; also
   0.0 contribution at c_a = 0). The piece carries 2.95 × 10⁻⁵ of I at c_a = 1 (the note's 2.9 × 10⁻⁵).
2. **κ** (ruling 2). *IMPLEMENTATION CHOICE*, ruled. κ_L = ln(|β|²_max,L/10⁻²⁰)/(10 − x_p), recomputed by the module at each
   of the Table's rows: 4.133414986453124 (L = 40), 4.017828332140974 (70), 3.9657820823215935 (90), 3.923520326182121
   (110), 3.88205232098606 (134). The module takes L = 90, κ = 3.9657820823215935; the test checks it against the formula
   from 1.43 × 10⁻⁴. `BriefingNotes/scripts/` does not hold the spectrum's script, so no fit to its output was possible.
3. **The kernels' accuracy, beyond the grid as within it** (ruling 3). *STRUCTURALLY REQUIRED.*
   - **Grids:** ln y ∈ [ln 10⁻³, ln 50], step 0.01 (1 083 nodes); u ∈ [0, 4], step 0.005 (801 nodes), with Ω(u) = e^{4u} Ω(−u)
     for u < 0 (exact; tested against quadrature at negative u).
   - **G as y → 0:** G I = M_1 y²/2 − C y³/3 + B_4 y⁴, B_4 = −(1/8)∫s_dir/x dx + C/(8x_p) − (C/8)e^{κx_p}E_2(κx_p)/x_p. The
     1/x piece on [0, x_p] is integrated exactly (C[((X² + y²)^{3/2} − y³ − X³)/3]), which gives the non-analytic −C y³/3 and
     C/8x_p; the direct piece on [0, x_p] gives no y³ or y⁵ term and a (y⁶/16) ln(X/y) term; the tails are analytic in y².
     Left out: O(y⁶ ln y), 6.6 × 10⁻¹² relative at y = 10⁻³. At y = 10⁻³ the y³ term is 5.2 × 10⁻⁸ relative at c_a = 1 and
     5.2 × 10⁻⁷ at 10 (needed there), 0 at c_a = 0; the y⁴ term is 6.4 × 10⁻⁷ (needed at all three).
   - **G as y → ∞:** G I = M_2 y − M_3 + M_4/(2y) − M_6/(8y³) + M_8/(16y⁵), from the expansion of √(x² + y²) − x for x < y;
     M_3 = I, so the −1 is exact. At y = 50: −1 is 1.9 × 10⁻² relative, M_4/2y 2.1 × 10⁻⁴, M_6/8y³ 4.2 × 10⁻⁸, M_8/16y⁵
     3.0 × 10⁻¹¹; left out ≤ 5M_10/(128 y⁷ I) = 4.4 × 10⁻¹⁴, and the region x > y, of order e^{−κy}.
   - **Ω as u → +∞:** for x e^{−u} ≤ x_p, s(x e^{−u}) = x² e^{−2u} + C e^u/x exactly, so Ω I = e^{−2u} M_5 + C e^u M_2 − R with
     0 ≤ R ≤ ∫_{x_p e^u}^∞ x³ s(x)(x² e^{−2u} + C e^u/x) dx, of order e^{−κ x_p e^u} = e^{−134} at u = 4. At c_a = 0 the second
     term vanishes; both forms are in the module.
   - **The tests** compare each kernel with quadrature at nodes, between nodes, at each end, a decade beyond each end and far
     beyond, at the three c_a (`TestTheKernels`, `TestTheAsymptotes`), to 10⁻⁶; the asymptotes term by term to 10⁻⁷ at the
     ends. The quadratures split at x_p and at y, or x_p e^u, and at the tails' scales; their stated error is below
     5 × 10⁻¹⁴ everywhere §2.1 evaluates them (`out/kernels.txt`).
4. **The shape as an object, and the peak** (ruling 4). *IMPLEMENTATION CHOICE*, ruled. `QuantaSpectrum` per c_a (cached by
   `spectrum(c_a)`) carries the shape, I, J, G, Ω, the version and the scale; the tool takes it as an argument internally
   (`measure_history(spectrum, …)`, `matter_excess(spectrum, …)`). `PEAK_SPECTRUM` has I = J = 1, G(y) = y²/(√(1 + y²) + 1)
   (and y − 1 + 1/2y above y = e^20, to avoid overflow), no Ω; nothing on the command line selects it. E reads G only through
   `spectrum.G_of_log_y`, so §2.2 exercises the code 07 reads.
5. **§2.2, exactly** (ruling 5). *As the prompt says; made exact.* `excess` is extracted by `ast` from the committed file and
   run unedited, with `Ns`, `gN`, `gq`, `gdK` (share × the payload's `bounces.K_final`, which is `ledger.K(N_final)`),
   `ledger`, `np` and `isfinite` bound; `lm` is the payload's `log_m_eff` (then `log_m_tracker`) in the sorted samples' order,
   None as NaN. **The tolerance:** |E_tool − E_probe| ≤ a ε Σ_born ΔK_g/K(N) + b |E_probe| with a = 2 and b = 8 ε n_born,
   with their reasons in `probe_05.py`'s docstring (the probe's √(1 + x²) − 1 carries up to 1.06 ε absolute per term for
   x ≤ 1; each side's per-term rounding is about 6u, gdK against ΔK 2u, and a sum of n positive terms (n − 1)u). The
   largest of each part and the matched-blank counts are in §3 and verification §5.5.
6. **K(N), in any units** (ruling 6). *STRUCTURALLY REQUIRED.*
   - **Order:** a history whose bounces.csv is not in ordinal order (ordinals 0, 1, …), or whose candidate rows do not have
     strictly increasing `candidate_index` and non-decreasing `N` **and `N_turn`**, is refused. The `N_turn` condition is
     mine (the generations are searched by N_turn, and the ordinal order is by N_turn): *IMPLEMENTATION CHOICE.*
   - **Bitwise is a tolerance**, `K_ULP_TOLERANCE` = 8ε relative to the larger K (each converted cell is one division by
     M_P⁴, half an ulp, and the ledger's sum one addition, half an ulp, so at most about 2.5 ulps in any units). The count of
     bitwise-equal pairs is printed per history and reported; it is never asserted.
   - **The leftover** is a jump above that tolerance, placed at the previous candidate's `N`, and its size must equal
     parking_share × K_final to 8ε K_final, or the history is refused (tested). With no bracketing row it is placed at the
     last candidate's `N`, K_final = K_last/(1 − parking_share).
   - **The check against fields.csv.** *Deviation:* the tolerance is **10⁻⁹ relative**, one number, but derived from two
     sources, not from the magnitudes alone: the magnitudes |ln K| + 4|N| + |ln ρ_R,E| + 4|ln(GeV/M_P)| (882.8 at most over
     the run), times ε, for each of the two passes (≈ 2.0 × 10⁻¹³ per pass); **and** the leftover's division, since K_final =
     K_last/(1 − parking_share) carries a relative error of about ε · parking_share/(1 − parking_share), 1.44 × 10⁵ ε =
     3.2 × 10⁻¹¹ at 02d's largest share (0.999993, C-3016-A0.05), larger than the first by two orders. The margin is about
     30. The ruling's derivation would have refused C-3016's histories. *STRUCTURALLY REQUIRED* (the information is
     parking_share, rounded). Where K(N) = 0, `rho_chi_over_rho_rad` must be exactly 0 (tested). ln(GeV/M_P) is the
     `UnitsLike`'s (`Planck_units` in the tool); `test_the_check_in_any_units` shows the largest difference agrees under
     `Planck_units`, `GeV_units` and `Mpc_units` within the tolerance. The same conditioning enters the comparison of K
     with the payload ledger after an unbracketed leftover, whose tolerance is 8ε(1 + ps/(1 − ps)) in the tests.
7. **Where K(N) = 0** (ruling 7). *IMPLEMENTATION CHOICE*, ruled, followed: blanks there for `E_field`, `E_tracker`, both
   `r_chi_E_*` and `parking_fraction`; 0.0 where K > 0 and nothing is born; `parking_fraction` 0.0 before the leftover. The
   two counts are printed on each history's line and reported in §3.
8. **Drains with no spectrum** (ruling 8). *IMPLEMENTATION CHOICE*, ruled, followed: in K, not a generation, counted on the
   history's line, no column. §3 names every history of §1.6 that has one.
9. **The history's directory** (ruling 9). *STRUCTURALLY REQUIRED*, followed: `extract_common.history_directory(beta,
   M_eV, n)` returns `beta=<β:.5g>/M=<M_eV:.5g>eV_n=<n>`; `plot_ScalarHistory.py` writes `base_path / f"plots/{…}"` and
   `base_path / f"csv/{…}"`, the same paths. `test_history_directory` checks the helper against the old f-strings on six
   literals and, by `ast`, that both assignments call it and no inline copy remains. The tool gets M_eV = M_Mp ×
   (M_P/eV) from `Planck_units` and refuses a row whose `n` does not parse as an integer (tested).
10. **The outputs** (ruling 10). *IMPLEMENTATION CHOICE*, ruled, followed: `spectrum_model` and `assoc_scale` lead every
    row; the suffix `-assoc<c:g>` at a scale other than 1; the tool overwrites its own files, and removes a history's own
    files of that scale when the history is refused or fails the check (mine: so that no stale output survives a refusal;
    *IMPLEMENTATION CHOICE*); exit 2 / 1 / 0 as ruled; no truncation of n̄ (the cost did not require it, §3).
11. **§1.6's run** (ruling 11). *IMPLEMENTATION CHOICE*, ruled, followed:
    - **Step 1 is a copy** of `cost_03.py`'s (the first part of its `run`), with one change, the masses computed
      (`compute_sample_masses` at its default), and without the timer and the capture of the result, which 03 needed for its
      cost; **step 3 is imported** (`cost_03.write_and_read`, with its `Store`), as is. Recorded in `run_05.py`'s docstring.
    - The export reads the object back from the temporary store (with its samples and bounce rows) and builds every file
      from it: histories.csv's record by `build_history_record` (bbn and baseline None) on the object read back (the
      populated object, not a second `_do_not_populate` read: the parent row's columns are the same), fields.csv by
      `plot_ScalarHistory.py`'s `write_csv_file` (extracted by `ast`) with unavailable stand-ins, bounces.csv by
      `write_bounces_csv`. `<label>` is `QCD_Cosmology`, `config/model_list.py`'s.
    - The merge, the five workers (largest first by 02d's per-integration wall), the per-child limit 4 × wall + 900 s, the
      scheduler launched once by `nohup`, and the wait in one background command for `scheduler.done`, as ruled.
12. **Files** (ruling 12). *STRUCTURALLY REQUIRED*, followed: the commit holds prompt §5's files, `extract_common.py` and
    `plot_ScalarHistory.py`; the helper's tests are `tools/tests/test_history_directory.py`. No existing test module changed.
    Nothing under `var/` is committed; under `.documents/one-loop-drain/` only `derive-05/`. The README is not edited.

Further deviations of mine:
- **The before-suites ran partly on a changed tree.** ObjectFactories', tools' and RunDrivers' before-runs (12:13–12:19) ran
  with the additive edits of `extract_common.py` (the new helper) and `plot_ScalarHistory.py` (its two lines and the import)
  already in the tree: I made them at 12:13:59, as ObjectFactories' run started. CosmologyModels' and both ComputeTargets
  halves ran on the clean dispatch tree. The counts (163, 173, 72) equal the dispatch's and all passed. *UNINTENDED DRIFT*
  from the note's order ("the five suites, before anything else"); the edits add a function and move no output.
- **The tool refuses a history whose `bounces.csv` row count differs from histories.csv's `bounce_rows`** (04's log names
  the column "to check bounces.csv against"). *IMPLEMENTATION CHOICE*; it held at every history of §1.6.
- **`k_ex_over_H` is written at every assessed candidate**, and `k_ex_over_H_min` is over all of them; the probe computes
  k_ex/H at the interior rows only, where the slip is defined. *IMPLEMENTATION CHOICE* (the prompt's "k_ex/H at each" read as
  each assessed candidate).
- **`backreaction_max_N` is the row's `N_turn`**, the N_k at which the ratio is evaluated (prompt §1.4). *IMPLEMENTATION
  CHOICE.*
- **The quadrature references** take two passes (a first at 10⁻⁸ for the sum's scale, then 10⁻¹³ relative or 10⁻¹⁶ of the
  scale absolute per piece), after the single-pass version raised scipy's `IntegrationWarning` at |u| ≈ 10.5–11.6 (pieces
  that carry nothing); the values agreed to 10⁻¹⁴ before and after. *IMPLEMENTATION CHOICE.*

### 2.1 Existing tests changed

None.

## 3. The Derive gate

Item by item in verification §5.8:
1. **Against direct quadrature** (prompt §2.1; `out/kernels.txt`): every kernel and moment below 10⁻⁶ at the three c_a on
   2 165 + 400 points for G and 2 001 + 200 for Ω per scale (largest 5.6 × 10⁻¹¹ for G, 2.3 × 10⁻⁹ for Ω, 4.4 × 10⁻¹⁶ for
   the moments); every asymptote within 10⁻⁷ at its end; the tests pass, among them every measure of the tool against a
   direct computation in q on constructed generations at the three c_a, to 10⁻⁶.
2. **Against the scoping probe, peak-collapsed** (prompt §2.2; `out/probe.txt`): P-2197 at A = 1, 4 995 samples, 5 generations; tolerance a = 2, b = 8ε n_born. Field mass: 3 081 compared (1 806 equal bit for bit), largest |diff|/|E_probe| 5.3 × 10⁻¹⁶ (2.4ε), largest |diff| over the b part 0.30 and over the a part 1.2 × 10⁻⁴¹; tracker: 3 081 (1 782 bitwise), 5.6 × 10⁻¹⁶ (2.5ε), 0.15 and 3.9 × 10⁻³⁴. Matched blanks: mass None 0, K(N) = 0 1 914 (none with a generation born), for both masses. No term in the probe's cancellation band (P-2197's y are all below e^{−20}), so the a part is never used here. K(N) bitwise at 4 995 of 4 995.
3. **On the sample** (prompt §2.3; `out/run.txt`, `out/report.txt`): 230 of 230 tasks, none stopped, every end marker; step 2 equal at 230 of 230; the merge equal byte for byte; the tool at three scales with 230 measured and exit 0 at each; the leftover after the last candidate in 221 histories and absent in 9, bracketed in none; the K check's largest difference 1.46 × 10⁻¹¹ (C-3016-A0.05) against 10⁻⁹; K_before + ΔK equal to the next K_before bit for bit at all 81 909 pairs; ruling 7: no sample with K(N) = 0 and a generation born, 45 samples in 9 histories between a birth and its drain (29 in C-3873-A1); ruling 8: no history with a drain without a spectrum. The tool's run at c_a = 1, repeated, gives its 461 files byte-identical.

## 4. Verification performed

**The suites**, serially, each chain in the background. Before, 2026-11-06 11:56–12:19, the machine shared:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 67.3 s | 5.02 5.34 7.98 (12:00) |
| ComputeTargets `test_[a-f]` | 219 OK | 153.1 s | 6.23 5.59 7.84 (12:01) |
| ComputeTargets `test_[g-z]` | 432 OK | 475.4 s | 15.45 10.26 9.37 (12:04) |
| ObjectFactories | 163 OK | 126.8 s | 7.37 8.68 8.86 (12:13) |
| tools | 173 OK | 200.1 s | 5.27 7.39 8.31 (12:15) |
| RunDrivers | 72 OK | 23.0 s | 8.05 9.28 9.06 (12:18) |

(The first CosmologyModels and ComputeTargets `test_[a-f]` runs, at 11:56–11:59, were not captured whole; the rows above are their
re-runs. ObjectFactories, tools and RunDrivers ran with the helper's edit in the tree: §2, last group.)

After, on this commit's tree, serially, 15:07–15:25, the machine shared:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 65.1 s | 13.56 12.30 10.18 (15:07) |
| ComputeTargets `test_[a-f]` | 219 OK | 146.2 s | 10.60 11.88 10.19 (15:08) |
| ComputeTargets `test_[g-z]` | 446 OK | 480.6 s | 4.57 8.92 9.26 (15:10) |
| ObjectFactories | 163 OK | 129.1 s | 5.03 6.76 8.10 (15:18) |
| tools | 191 OK | 207.5 s | 5.08 6.28 7.72 (15:21) |
| RunDrivers | 72 OK | 24.3 s | 4.58 5.73 7.19 (15:24) |

ComputeTargets goes from 651 to 665 (`test_quanta_spectrum`, 14, in the `test_[g-z]` half) and tools from 173 to 191
(`test_quanta_measures` 16, `test_history_directory` 2). No count fell. (The session paused between the run, which ended at 12:58,
and these suites, from a usage limit; nothing ran in between.)

**The scripts:** `kernels_05.py` (85 s), `probe_05.py` (4 s), the run (§3), the tool at the three scales, `report_05.py`.
No Ray was started, no `ray` command run, nothing registered, and none of `main.py`, `plot_by_beta.py` or `plot_ScalarHistory.py`
run as a program. No store was opened but the tests' and the run's temporary ones, each deleted. `black` was run on every
Python file I changed or added.

## 5. The deliberate-breakage record

Each by `unittest.mock` on the named function, the named tests run (a scratch script; nothing committed):
- G's small-y series without the y³ term: `test_G_small_y` fails (at c_a = 10, 5.2 × 10⁻⁷ against 10⁻⁷).
- G's large-y series without the −1: `test_G_large_y` and `test_G_against_quadrature` fail (9 subtests).
- Ω(u < 0) taken as Ω(−u), the e^{4u} left out: `test_Omega_against_quadrature` and the tool's constructed measures fail (16).
- U17's end removed from the quadrature's integrand (1/x above x_p): the moments' closed form disagrees with quadrature (2).
- The parking leftover left out of K(N): the constructed leftover test and the stored P-2197 test fail (the K check).
- E with the generations born strictly before N (N_g < N, not ≤): the constructed measures fail at a sample at a birth.
- n̄_j with the generation j itself included (N_g ≤ N_j): the constructed n̄ against quadrature fails.
- The K check with GeV taken as M_P (ln(GeV/M_P) left out): every constructed history fails the check.
The unbroken run: 30 tests, all passing.

## 6. Observations not acted on

- **The random-phase premise fails in a few histories** (verification §5.7): n̄_max > 1 in 33 histories (the L ladder, 5 × 10⁻²⁰
  to 4 × 10⁻¹⁵ M_P at β = 2, and group C at 10⁻¹² M_P), up to 9.0 × 10³¹ (L-M5e-20-A0.1), and ln n_g up to 98; the phase slip at a
  producing bounce is below 1 rad in 15 (least 5.1 × 10⁻¹⁰ rad, C-3873-A1). The large overlaps sit at bounces that drain almost
  nothing (that history's δ_stim is 4.2 × 10⁻⁹), and δ_stim stays at most 1.3 × 10⁻³ everywhere. They are measurements (P49),
  recorded in the narrowing of `[00-later-bounces-act-on-quanta-already-present]`; no issue is opened. 08's documentation may
  want to say that the first-order, random-phase reading of δ_stim does not hold there.
- **The backreaction ratio exceeds 1** in histories of every group but B and C (P ≤ 244, W ≤ 258, R ≤ 918, L ≤ 1.8 × 10³): the
  produced quanta's rest energy, at the mass of a later bounce, exceeds that bounce's impact energy. A measurement (P49); not
  acted on.
- **E is negligible on the whole sample** (at most 5.4 × 10⁻²⁰ with the field's mass and 7.2 × 10⁻¹⁶ with the tracker's): the
  one-loop generations stay relativistic, as 03's margins said.
- **The probe's cancellation was not exercised**: on P-2197 every y is below e^{−20}, so the probe takes its 0.5x² branch and
  §2.2's a part is never used. A history with y near e^{−10} would exercise it.
- **No history of 02d has a bracketed leftover** (no drain after parking, no release): that placement is tested on constructed
  exports only.
- **`run_05.py` and `report_05.py` were reformatted by `black` after the run** (layout only); the run used `run_05.py` as first
  written.
- **The tool's per-history wall** is at most 0.12 of its integration's (median 0.032); n̄'s O(G²) at 13 342 generations takes about
  2 s, so no truncation was needed.
- **`tools/tests/quanta_exports.py`** writes exports by 04's writers for the tests and the scripts; 07's tests may reuse it.

## 7. State handed to the next prompt

**For 07 (the figure tools): the output format, exactly.** The tool reads 04's exports only and writes three CSV files. Every
number is written as `repr` of a float64 (so `float()` of a cell gives back the value written), integers as integers,
strings as they are, and NULL as an empty cell. **Every row of every file begins with two columns, `spectrum_model` and
`assoc_scale`**: the string `SPECTRUM_MODEL` of `ComputeTargets/quanta_spectrum.py` (today `U4-U17-v1`) and the associated
piece's scale c_a as a float (`1.0`, `0.0`, `10.0`). No file has a comment line. **07 reads the default's files only** (c_a = 1,
no suffix); at another scale every name carries `-assoc<c:g>` (`quanta_bounces-assoc0.csv`, `quanta_histories-assoc10.csv`).

*Which histories have them.* Each histories.csv row whose `drain_model` is not `fixed` and whose `normalization` is not blank,
whose directory holds both fields.csv and bounces.csv, that is not refused (two rows giving one directory, `n` not an integer,
bounces.csv not in ordinal order or not of `bounce_rows` rows, candidate rows out of order, K falling or jumping twice, a
leftover that is not `parking_share` × K_final) and that passes the K check. A history that is refused or fails has none of
its files at that scale (the tool deletes its own stale ones) and no row in quanta_histories.csv; the tool's printed line
says why.

*Where, and how they join 04's exports.*
- **`quanta_bounces.csv`** and **`quanta_samples.csv`**: in the history's directory, beside its fields.csv and bounces.csv,
  `<label>/<drain_model>/A=<A:.5g>/csv/beta=<β:.5g>/M=<M_eV:.5g>eV_n=<n>/` (`extract_common.alpha_path` and
  `history_directory`). quanta_bounces.csv has one row per bounces.csv row, in the same order, and joins it on `ordinal`;
  quanta_samples.csv has one row per fields.csv row, in the same order (log_one_plus_z descending, raw_N ascending), and
  joins it row to row (and on `raw_N`, which it repeats).
- **`quanta_histories.csv`**: at `<label>/`, beside histories.csv, one row per measured history, sorted as histories.csv is
  (by `drain_model`, `alpha`, `M_Mp`, `n`, `beta`); it joins histories.csv on the five key columns, which are histories.csv's
  cells copied verbatim.

*quanta_bounces.csv* — after the two model columns:

| column | unit | where | meaning |
|---|---|---|---|
| `ordinal` | integer | every row | bounces.csv's `ordinal` |
| `source` | string | every row | `candidate`, `plateau` or `wall`, bounces.csv's |
| `N` | e-folds | every row | bounces.csv's `N` (a candidate's drain N, an event's N) |
| `N_turn` | e-folds | candidates | bounces.csv's `N_turn`, the birth N; blank for an event |
| `ln_n_g` | ln (dimensionless) | generations | ln of the occupation at birth at the direct peak, ln(A_g s(x_p)), A_g = 2π²ΔK_g/(Q_g⁴ I) |
| `n_bar` | dimensionless | generations | the occupation overlap n̄_j = Σ_{N_g < N_j} A_g Ω(ln Q_g/Q_j), over the generations born strictly before; 0.0 for the first |
| `phase_slip` | rad | interior assessed candidates | `phase_slip.py`'s slip \|θ(x, b) − θ(a, x)\| over consecutive assessed candidates (rows with `log_k_ex_Mp`), in ordinal order; blank at the first and last |
| `k_ex_over_H` | dimensionless | assessed candidates | k_ex/H_E, both before the candidate (`log_k_ex_Mp` − `log_H_Mp`) |
| `backreaction_ratio` | dimensionless | candidate rows with `log_m_Mp` and `log_E_Mp4` | n_χ a³ e^{−3N_turn} m/E, n_χ a³ = Σ_{N_g < N_turn} ΔK_g J/Q_g; 0.0 where nothing is born before |

A generation is a candidate row with `delta_K_Mp4` > 0 and a `log_q_Mp`; a candidate with `delta_K_Mp4` > 0 and no `log_q_Mp`
drains into K but is not a generation (blank `ln_n_g` and `n_bar`).

*quanta_samples.csv* — after the two model columns:

| column | unit | meaning |
|---|---|---|
| `raw_N` | e-folds | fields.csv's `raw_N` |
| `log_one_plus_z` | ln(1 + z_E) | fields.csv's |
| `K_Mp4` | M_P⁴ (linear) | K(N) = ρ_χ,E e^{4N}, rebuilt from the bounce rows, the parking leftover included; 0.0 where there is no dark radiation |
| `parking_fraction` | dimensionless | K_parking/K(N), the parking leftover's share of K(N), which has no spectrum; 0.0 before it enters; blank where K(N) = 0 |
| `E_field`, `E_tracker` | dimensionless | the matter-like excess E(N) = Σ_{N_turn ≤ N} ΔK_g G(m e^N/Q_g)/K(N), with the field's mass (`log_m_eff_Mp`) or the tracker's (`log_m_tracker_Mp`): the fraction by which ρ_χ exceeds its massless value; blank where K(N) = 0 or that mass is blank; 0.0 where K(N) > 0 and nothing is born |
| `r_chi_E_field`, `r_chi_E_tracker` | dimensionless | `rho_chi_over_rho_rad` × E: the excess relative to the bath; blank where E is |

*quanta_histories.csv* — after the two model columns:

| column | unit | meaning |
|---|---|---|
| `drain_model`, `alpha`, `M_Mp`, `n`, `beta` | as histories.csv | the key, histories.csv's cells verbatim (`alpha` is A under the one-loop model) |
| `generations` | integer | the number of generations |
| `delta_stim` | dimensionless | δ_stim = Σ_j 2 n̄_j ΔK_j / K_final, the fractional change of the final ρ_χ that stimulated emission would make at first order with random phases; rescattering is bounded by it (about 2n, README §8.3), not computed |
| `n_bar_max`, `n_bar_max_ordinal` | dimensionless; integer | the largest n̄_j and its row's `ordinal` |
| `ln_n_g_max`, `ln_n_g_median` | ln | over the generations |
| `E_field_max`, `E_field_max_N`, `E_field_today`, `r_chi_E_field_max` | dimensionless; e-folds | the largest E with the field's mass and its `raw_N`, E at the last sample, and the largest r_χ E |
| `E_tracker_max`, `E_tracker_max_N`, `E_tracker_today`, `r_chi_E_tracker_max` | as above | the same with the tracker's mass |
| `phase_slip_min_producing`, `phase_slip_min`, `phase_slips_below_1` | rad; rad; integer | the least slip at a producing candidate (ΔK > 0) and over all interior assessed candidates; the number of slips below 1 rad |
| `k_ex_over_H_min` | dimensionless | over the assessed candidates |
| `backreaction_max`, `backreaction_max_N` | dimensionless; e-folds | the largest backreaction ratio and its row's `N_turn` |
| `parking_share` | dimensionless | histories.csv's `parking_share`, the leftover's share of K_final |
| `K_check_max_rel` | dimensionless | the largest relative difference of K(N) against r_χ ρ_R,E e^{4N} over the samples (tolerance 10⁻⁹) |

A summary is blank where it has nothing to summarise (no generation, no interior assessed candidate, no row with a mass and
an energy, no sample with a mass).

**For 06 (the rehearsal): running the tool on a rehearsal's exports.** After `plot_by_beta.py` (histories.csv) and
`plot_ScalarHistory.py` (fields.csv and bounces.csv) have run with the build's `--A-values`, from the repository root:

    PYTHONPATH=. ./venv/bin/python tools/quanta_measures.py <output>/QCD_Cosmology
    PYTHONPATH=. ./venv/bin/python tools/quanta_measures.py <output>/QCD_Cosmology --assoc-scale 0
    PYTHONPATH=. ./venv/bin/python tools/quanta_measures.py <output>/QCD_Cosmology --assoc-scale 10

`<output>` is the plot scripts' `--output`. It needs no store and no cluster, and takes about 52 s for 02d's 230
histories at one scale. It prints one line per history (generations, drains without a spectrum, the leftover's placement,
the bitwise count of K, the K check, ruling 7's counts, its wall) and a summary; exit 0, or 1 if a row was refused or a
history failed the K check (everything else still written), or 2 on a usage error or an unreadable root. Fixed-law rows and
failed histories are skipped and counted. A comparison of two runs' outputs (the straight build against the resumed one)
can compare the three files byte for byte: the tool is deterministic on the same exports (verification §5.6).
