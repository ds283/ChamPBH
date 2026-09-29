# Review remediation — close-out verification and handover to the numerical campaign

**Written:** 2026-09-30, by `review-remediation` prompt 06. **Tree measured:** `01e5975`. That is
the campaign's last commit before this close-out, whose own commit adds only documents.
**Campaign:** [`prompts/review-remediation/`](../prompts/review-remediation/README.md). **Board:**
[`IMPLEMENTATION_STATE.md`](../prompts/review-remediation/IMPLEMENTATION_STATE.md).
**Source audit:** [`audit-2026-09-29/README.md`](audit-2026-09-29/README.md).

This document has three uses:

- It scores the final tree against every row of the campaign's acceptance table (README §6).
- It says what the campaign changed and what it deliberately left alone.
- It hands over to whoever plans the numerical campaign, which regenerates the scalar histories
  and the BBN survey on the corrected code (§4).

It is additive. A later re-run that supersedes a figure gets a new dated subsection; the figures
below stay as they are, correct for `01e5975`.

**Provenance tags.** Every "final" figure below was printed by one of these, run by prompt 06 from
the repository root with `venv/bin/python` on `01e5975`, on 2026-09-30.

| Tag | Command | Wall-clock |
|---|---|---|
| **[S1]** | `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . -v` | 6.4 s (unittest: 12 tests in 4.97 s), OK, jax case run |
| **[S2]** | `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -v` | 45.9 s (unittest: 13 tests in 44.47 s), OK |
| **[A]** | `./venv/bin/python .documents/audit-2026-09-29/{tlaw_check,eos_consistency,spline_test}.py` | 2.3 s, 0.9 s, 1.2 s |
| **[F]** | `PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.prym_fixtures {zero,reference,constant,oscillating}` | 7.8, 7.5, 7.8, 8.8 s |
| **[B]** | `./venv/bin/python tools/bbn_baseline.py` | 9.1 s (solve 7.2 s) |
| **[P]** | `PYTHONPATH=. ./venv/bin/python prompts/review-remediation/logs/06-probes/probe06_{deriv,knots}.py` | ≈ 5 s, ≈ 1 s |
| **[G]** | `grep` on the tree | — |

The "`f5896bb`" column is README §6's "now" column, from the audit scripts and the §2 (f) probes
on `f5896bb`. It was not re-measured here, since `f5896bb`'s code is not the final tree.

---

## 1. Before and after

**Every row meets its target on the final tree.** Nothing was loosened by this prompt.

- **Amended in README §6 itself:** the derivative norm, by the user's decision (b) of 2026-09-29.
- **Restated only in a board decision, not in README §6:** two rows. They are §6.2 row 3 (option
  C) and §6.4's peak temperatures (Option 1). Each row says so, and gives the value against the
  original target as well.

### 1.1 The temperature law (README §6.1; prompts 01, 02)

| Quantity | `f5896bb` | Target | Final (`01e5975`) | Witness |
|---|---|---|---|---|
| N from 2×10⁴ GeV to T_CMB, code law vs T a g_s^{1/3} = const | 41.497 vs 40.075 | agree to 1e-5 | **40.0754 vs 40.0754, −3.620e-8** | [S1] `test_guard_temperature_law_matches_entropy_conservation`; [A] `tlaw_check.py` `kappa=1` column = `exact` column at all 8 rows |
| corrected law to 10 keV and T_CMB (R5's step) | +1.465e-4 | ≤ 1e-5 | **−3.620e-8 / −3.620e-8** | [S1] `test_guard_corrected_convention`. Join: G_s(T_LO⁺) = G_s(T_LO) = 3.931000, ⅓ ln ratio = −3.7e-17 |
| same, to 1 GeV / 100 MeV / 1 MeV / 70 keV / 10 keV | +0.180 / +0.779 / +0.994 / +1.403 / +1.422 | each ≤ 1e-5 | **−6.864e-12 / −3.626e-8 / −3.617e-8 / −3.605e-8 / −3.620e-8** (also 5 MeV −3.618e-8, 100 keV −3.659e-8) | [S1] case 1 |
| \|dG_s_dlogT − central\| / G_s on the 60-point grid | ratio 2.303 | ≤ 1e-6 absolute (restated 2026-09-29, user decision (b)) | **worst 1.259e-8 at 180 MeV; 0 of 60 over 1e-6** | [S1] `test_derivative_convention_is_natural_log`; [P] `probe06_deriv.py` |
| \|spline − jax\| / G_s, same grid | ratio 2.303 | ≤ 1e-6 absolute (same decision) | **worst 2.438e-7 at 180 MeV; 0 of 60 over 1e-6** | [S1] `test_spline_and_jax_derivatives_agree` (jax importable, not skipped); [P] |
| ρ_R witness from 2×10⁴ GeV to 1 MeV / 70 keV / 10 keV | 0.022 / 0.0044 / 0.0041 | 0.998 / 0.999 / 0.999 ± 3e-3 | **0.99790 / 0.99915 / 0.99922** | [S1] `test_rho_R_witness`; [A] `tlaw_check.py` 0.9979 / 0.9991 / 0.9992 |
| ρ_R witness from 5 MeV to 10 keV | 0.182 | 1.001 ± 5e-3 | **1.00135** | [S1] `test_rho_R_witness`; [A] `eos_consistency.py` 1.00135 |
| `VERSION_LABEL` | `"2026.1.1"` | `"2026.2.0"` in `main.py` and `plot_by_beta.py` | **`"2026.2.0"`** at `main.py:83` and `plot_by_beta.py:74` | [G] |

**The audit scripts on the final tree.**

- **`tlaw_check.py`'s `kappa=1/ln10` column now double-divides.** The script applies its own ÷ ln 10
  to a derivative that is already natural-log since prompt 02, so the column no longer means
  "corrected". It prints 39.4574 against the exact 40.0754 at T_CMB, and ρ_R ratios of 1.35–10.96.
  The script is not edited; it is the record of the audit. **Read its `kappa=1` column as the
  corrected law.**
- **`low_t_join_probe.py` also applies ÷ ln 10 itself**, so it was not run. Guard case 2 replaces
  it.
- **`eos_consistency.py`** prints 1.00135 (5 MeV → 10 keV), 1.00005 (100 MeV → 10 keV) and 1.00002
  (5 MeV → 1 MeV), as in log 02.
- **`spline_test.py`** reproduces the audit's §2 table to every printed figure. For example, at
  250 per decade: asinh 7.9e-10 and ratio 5.2e-17 (constant); asinh 3.3e-8 and ratio 9.7e-9
  (oscillating).

### 1.2 The PRyMordial passenger (README §6.2; prompt 03)

| Quantity | `f5896bb` | Target | Final (`01e5975`) | Witness |
|---|---|---|---|---|
| oscillating synthetic ρ_NP with `NP_thermo_flag`, wall | > 600 s | ≤ 60 s | **8.78 s**, 0 `RuntimeWarning`s; N_eff 3.70221, Yp 0.2469265751, D/H ×10⁵ 2.787693732 | [F] `oscillating`; [S2] `test_a_oscillating_case_completes` |
| ρ_NP ≡ 0, `NP_thermo_flag` True vs False, Yp and D/H | not measured (warnings) | ≤ 1e-6 relative | **identical to all ten printed figures** (Yp 0.2468872958, D/H 2.462251065), 0 warnings | [F] `zero` and `reference`; [S2] `test_b_patch_is_inert` |
| ρ_NP = 0.08 ρ_SM, Yp / D/H ×10⁵ | 0.25409 / 2.6715 | unchanged to 1e-5 relative. **Restated 2026-09-29 (user, option C):** against the fixture's own unpatched values 0.2540937879 / 2.671500711 | **0.2540937879 / 2.671499971**, i.e. 0 / −2.77e-7. Against the literal five-figure 0.25409 / 2.6715 it is 1.49e-5 / −1.1e-8; the Yp difference is the rounding that caused the decision | [F] `constant`; [S2] `test_c_reference_abundances_unchanged` |
| a failed `compute_BBN_data` | `{"failure": True}` | carries a `failure_reason`, stored on the row, printed by `plot_by_beta.py` with the dropped (β, M, Λ) | **Returned and stored.** The column is `failure_reason` in `Datastore/SQL/ObjectFactories/BBNData.py`. `report_dropped_bbn_models` is at `plot_by_beta.py:549`, called at `:734`. The printing needs a cluster and a store and has not been run (log 03) | [S2] `test_d_failure_carries_a_reason`; [G] |
| `PRyM_version` on new rows | `"bf24c3d"` | names the patch | **`"bf24c3d+cham03"`** (`ComputeTargets/BBNData.py:40`; printed by [B]) | [G]; [B] |

### 1.3 The interface (README §6.3; prompt 04)

At 250 knots per decade in ln T over [0.1 eV, 100 MeV], evaluated on [0.02, 5] MeV.

| Quantity | `f5896bb` (asinh) | Target (ratio) | Final (`01e5975`) | Witness |
|---|---|---|---|---|
| constant ratio 0.08: max spurious ρ_NP/ρ_SM | 7.9e-10 | ≤ 1e-12 | **5.187e-17** (P_NP 1.550e-17, dρ 1.428e-15) | [S2] `test_a_constant_ratio_is_exact` |
| oscillating ratio: max spurious ρ_NP/ρ_SM | 3.3e-8 | ≤ 2e-8 | **9.748e-9** | [S2] `test_b_oscillating_ratio_bounds` |
| oscillating ratio: max derivative error / (4 ρ_SM) | 3.0e-6 | ≤ 1.5e-6 | **7.396e-7** | same |
| no worse than asinh, both families | — | ratio ≤ asinh | constant 5.19e-17 vs 7.85e-10; oscillating 9.75e-9 vs 3.30e-8 (ρ), 7.40e-7 vs 2.96e-6 (dρ) | [S2] `test_c_no_worse_than_asinh` |
| non-monotonic `log_T_Jordan` | sorted silently | refused (`ComputationFailureError`, reason recorded) | **Refused by `build_NP_callbacks`.** The conversion to `_failure_payload("BBN callbacks: …")` inside `compute_BBN_data` is reasoned, not run (log 04) | [S2] `test_d_non_monotonic_input_is_refused` |
| `Ω″` term | `Ω″ π` | `Ω″ π²`; a non-exponential stand-in shows the difference | **`Omega_primeprime * pi**2`** (`ComputeTargets/BBNData.py:235`, in `jordan_Hdot_over_H2`) | [S2] `test_g_Hdot_over_H2_Omega_primeprime_term`; [G] |
| end to end: ratio 0.08 through the new callbacks | — | Yp, D/H match §6.2 row 3 to 1e-4 relative | **Yp 0.2540933067 (−1.89e-6), D/H 2.671263588 (−8.85e-5).** Met, but inside PRyMordial's own noise band of ~7e-4 in D/H (board issue `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`) | [S2] `test_h_end_to_end_constant_ratio` |
| ρ_NP ≡ 0 baseline through `compute_BBN_data`'s path | not available | a function and a script, drawn by `plot_by_beta.py` | **Available.** `compute_SM_baseline`; `tools/bbn_baseline.py` gives Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017, identical to [F] `zero`. `plot_by_beta.py:869` computes it, and `--no-baseline` (`:60`) turns it off. The drawing has not been run (needs a cluster) | [S2] `test_i_SM_baseline`; [B]; [G] |

### 1.4 The kicking function (README §6.4; prompt 05)

Evaluated through `Xav_EOS_spline.w` at 1000 points per decade. `Xav_EOS_spline.py` changed only
in its module docstring, and `Xav_EOS_data.csv` not at all, so the `f5896bb` values are the
same function. The "`f5896bb`" column is the audit's table-row reading.

| Feature | `f5896bb` (table rows, audit §4) | Target | Final (`01e5975`) | Witness |
|---|---|---|---|---|
| e⁺e⁻ peak | 0.1007 at 0.158 MeV | Σ 0.1007 ± 1e-3; T **0.1605 MeV** ± 5 % | **0.100732 at 0.1603 MeV** (T −0.11 %) | [S1] `test_three_peaks` |
| QCD peak | 0.314 at 178 MeV | Σ 0.3138 ± 1e-3; T **0.1819 GeV** ± 5 % | **0.314532 at 0.1820 GeV** (Σ +7.3e-4, the narrowest margin in the campaign; T +0.05 %) | same |
| EW peak | 0.0373 at 56 GeV | Σ 0.03733 ± 1e-3; T **53.25 GeV** ± 5 % | **0.037436 at 53.22 GeV** (T −0.06 %) | same |
| Σ(2 MeV) / Σ(20 keV) | 0.0030 / < 1e-6 | ± 1e-3 | **0.00294586 / 5.17e-8** | [S1] `test_ee_profile` |
| ∫Σ d ln T over [10 keV, 3 MeV] | 0.1617 | ± 2e-3 | **0.161813** | [S1] `test_ee_integral` |
| ρ_R witness 5 MeV → 10 keV, corrected law | 0.182 (shipped law) | 1.00135 ± 5e-3 (log 02's value; README §6.4 said 1.005 before R5) | **1.001346** | [S1] `test_table_is_consistent_with_the_gs` |
| same, from 100 MeV | 0.080 (shipped law) | 1.00005 ± 5e-3 (README: 1.003 ± 5e-3 before R5) | **1.000053** | same |
| same, from 2×10⁴ GeV | 0.0041 (shipped law) | 0.99922 ± 5e-3 | **0.999223** | same |

**The T targets in bold are the user's restatement of 2026-09-29 ("Option 1").** They are the peaks
of the spline `w` actually evaluates. README §6.4's original column, 0.1585 MeV, 0.1778 GeV and
56.23 GeV, is the argmax over the CSV's own rows, about 20 per decade.

- **Against the original column** the measured peaks are +1.2 %, +2.4 % and **−5.35 %** off.
- **So the EW row would miss ±5 %** without the decision, and the decision supersedes that
  column (board, Decisions).
- **The Σ targets were not restated,** and all three pass.
- The 5 MeV and 100 MeV witness rows pass against README §6.4's literal pre-R5 centres too: they
  are 3.7e-3 and 2.9e-3 from 1.005 and 1.003.

### 1.5 The suites and the diff

| Package | at `f5896bb` | after prompt 05 (`bb840f6`) | on `01e5975`, before and after this prompt |
|---|---|---|---|
| `CosmologyModels/tests` | 0 (absent) | 12 | **12**, OK, 6.4 s wall; the jax case runs |
| `ComputeTargets/tests` | 0 (absent) | 13 | **13**, OK, 45.9 s wall, of which about 40 s is PRyMordial solves |

**`git diff --stat f5896bb..01e5975`: 57 files, +7682 / −187. Every file is in the campaign's scope.**
Production code:

| File | Prompt | What |
|---|---|---|
| `CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py` | 02, 05 | ÷ ln 10 in both derivatives (R1); `w()` docstring |
| `CosmologyModels/GenericEOS/SaikawaShirai_common.py` | 02 | `LOW_T_GSTAR` 3.383, `LOW_T_G_S_STAR` 3.931 (R5; README §0.4 and §2 (j) allow it) |
| `CosmologyModels/GenericEOS/SaikawaShirai_EOS_jax_autodiff.py` | 05 | `w()` docstring only (+5 lines) |
| `CosmologyModels/GenericEOS/Xav_EOS_spline.py` | 05 | module docstring only (+32 lines) |
| `ComputeTargets/BBNData.py` | 03, 04 | `PRYM_VERSION`, failure reasons; ratio callbacks, refusal, Ω″π², baseline |
| `Datastore/SQL/ObjectFactories/BBNData.py` | 03 | nullable `failure_reason` column; `build()` returns the newest failed row |
| `PRyM/PRyM_main.py` | 03 | `dTNPdt` returns 0, with a marker comment; the original body is kept commented out |
| `main.py` | 02 | `VERSION_LABEL` and its comment only (+4 / −1) |
| `plot_by_beta.py` | 02, 03, 04 | `VERSION_LABEL`; the dropped-model list; the baseline |
| `tools/bbn_baseline.py` | 04 | new |

The rest of the diff is the two new test packages, `.documents/`, `CLAUDE.md` and `prompts/`.

- **Not in the diff:** `ComputeTargets/ScalarModel.py` (the fix is in the EOS class, README §2 (b)),
  `thirdparty/`, `Xav_EOS_data.csv`, `config/` and `Datastore/SQL/ObjectFactories/ScalarModel.py`.
- The board header's "Code" line omits the three `GenericEOS` files touched by prompts 02 and 05.
  All three are inside README §0.4 and §3; the line is incomplete, not the diff out of scope.

---

## 2. What changed, prompt by prompt

**Prompt 01 — the guard** (`ec3a994`; [log 01](../prompts/review-remediation/logs/01-temperature-law-harness.md)).
Created `CosmologyModels/tests/`, with `eos_reference.py` and `test_temperature_law.py`.
- **The guard** scores the temperature law against exact entropy conservation,
  T a g_s^{1/3} = const. That needs `G_s` and nothing else.
- **What it found on the unfixed tree.** It characterised R1 (offset +1.4223 e-folds to T_CMB;
  derivative ratio ln 10; ρ_R witness 0.0041) and R5 (+1.465e-4 at the 10 keV join).
- **It showed a simulated fix would make five of its six tests fail**, so the later fix would
  announce itself.
- **No production code changed.** It also found that the audit's §11 correction about `1/3.9` was
  wrong, and that the derivative target of 1e-6 relative could not be met. The user restated that
  target.

**Prompt 02 — the fix** (`47c50ae`; [log 02](../prompts/review-remediation/logs/02-fix-the-entropy-derivative.md)).
- **R1.** `SaikawaShirai_EOS_spline.dG_s_dlogT` and `dG_rho_dlogT` now divide the log10-grid
  spline derivative by ln 10, so both return d g/d ln T. The grid and the consumer are unchanged.
- **R5.** The low-T limits became the fit's own, 3.931 and 3.383.
- **The guard flipped** from characterising the defects to asserting the law.
- **`VERSION_LABEL` went to `"2026.2.0"`.**
- **Verified.** N to T_CMB agrees with exact entropy conservation to −3.6e-8. With the old
  production files restored, the new tests failed 140 subtests; with R5 alone reverted, five.

**Prompt 03 — PRyMordial's passenger and failure reasons** (`ec206a3`; [log 03](../prompts/review-remediation/logs/03-prymordial-passenger-and-failure-reasons.md)).
- **R2.** The inert `dTNPdt` in the vendored PRyMordial returns 0, with a marker comment. The
  oscillating case finishes in 8–9 s instead of stalling.
- **Shown inert.** ρ_NP ≡ 0 with the NP machinery on reproduces the no-NP run exactly, and 916
  divide-by-zero warnings go to 0. The 0.08 family moves by 2.8e-7.
- **Failure reasons.** `compute_BBN_data` returns and stores a `failure_reason` on every failure
  path, and `plot_by_beta.py` lists the models it drops.
- **Version string.** New rows say `PRyM_version = "bf24c3d+cham03"`.
- **Created `ComputeTargets/tests/`.**
- **Found** that `small_network=True` has never reached PRyMordial, and that PRyMordial's output
  moves by 1e-5–1e-4 under 1e-9 changes to ρ_NP.

**Prompt 04 — ratio splines and a baseline** (`eba4473`; [log 04](../prompts/review-remediation/logs/04-ratio-splines-and-a-baseline.md)).
- **R3.** `build_NP_callbacks` splines ρ_NP/ρ_R,J and p_NP/ρ_R,J against ln T and multiplies back
  by the thermodynamic ρ_SM = (π²/30) g_ρ T⁴. dρ_NP/dT is the analytic derivative of the
  interpolant. The asinh path and its sort are gone.
- **Fail closed.** A non-monotonic T_J raises `ComputationFailureError`, with a reason.
- **Ω″ π became Ω″ π²** (`jordan_Hdot_over_H2`).
- **A ρ_NP ≡ 0 baseline** runs through the same PRyMordial settings: `compute_SM_baseline`,
  `tools/bbn_baseline.py`, and a line on the `plot_by_beta.py` panels.
- **Every §6.3 target was met.** The end-to-end D/H target, 1e-4, passed at 8.85e-5, inside
  PRyMordial's noise.

**Prompt 05 — the kicking function pinned** (`bb840f6`; [log 05](../prompts/review-remediation/logs/05-kicking-function-and-eos-hygiene.md)).
- **R4, pins.** `test_kicking_function.py` pins:
  - the three spline peaks of Σ = 1 − 3w;
  - the e⁺e⁻ profile and its integral;
  - w = 1/3 outside the table;
  - that production does not use the 2 MeV freeze;
  - the ρ_R witness from three starting points.
- **Hygiene.** It labelled the non-production `w()` implementations and documented the CSV in
  `Xav_EOS_spline.py`.
- **The paper-facing note** is [`numerical-methods-for-paper.md`](numerical-methods-for-paper.md).
- **It found** that the table's Σ and the Saikawa–Shirai g's do not describe the same plasma
  through the QCD and EW crossovers. The user accepted that as an open question for the authors
  (board, 2026-09-30).

**Prompt 06 — this document** (log 06). No production code or test changed. The figures in the
note's §3–§4, which prompt 05 quoted from logs 02–04, were re-measured here:

- 636 samples in [0.02, 5] MeV, 250–309 per decade ([P] `probe06_knots.py`);
- the callback accuracies 5.2e-17 / 9.75e-9 / 7.4e-7 ([S2]);
- the passenger timing, ≈ 8–9 s ([F]);
- the baseline abundances ([B]).

All agree to the figures the note prints.

---

## 3. What was deliberately not changed

**From README §0.4, by design:**

- **No pipeline run.** No `main.py` run, datastore or Ray cluster.
- **The field equation's physics is untouched:** `ODEPolicy`, the kicking term, reflection, the
  bounce regions and `PotentialDerivativePolicy`. `ScalarModel.py` is not in the diff.
- **Not corrected:**
  - the adiabaticity diagnostic (H5);
  - the hard-coded φ\* (H8);
  - reflection-count reporting (N1).
- **Unchanged:** the sampling density, the BBN spline domain and `T_BBN_*_spline_*`.
- **Not edited:** `Xav_EOS_data.csv`, the Saikawa–Shirai coefficients and every branch boundary.
  Only the two R5 limit constants moved.
- **The datastore** gained one nullable column, `failure_reason`, and nothing else. Lookups still
  ignore `version`.
- **The paper** was not edited. The note is for the authors.

**Open on the board (§3) at close, 15 issues:**

| Issue | Why it was left |
|---|---|
| `[00-adiabaticity-diagnostic-omits-the-source-response-term]` | H5; changes stored `AdiabaticHistory` rows |
| `[00-initial-field-value-is-hard-coded-and-unchecked]` | H8; a CLI/tag change |
| `[00-hard-reflection-count-is-stored-but-never-reported]` | N1; see also the new `06-…` issue below |
| `[00-kicking-function-table-has-no-provenance-in-the-repository]` | needs the authors |
| `[00-datastore-lookups-ignore-the-version-column]` | the datastore layer's; the fresh-database rule covers it |
| `[00-two-files-are-not-black-clean]` | housekeeping, in a commit of its own |
| `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` | harmless; the domain was frozen by §0.4 |
| `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` | comment-only; outside every prompt's allowed lines |
| `[03-small-network-flag-is-never-read-by-prymordial]` | changes physical output, so needs a version decision |
| `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` | PRyMordial's tolerances; the numerical campaign's question |
| `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]` | a further vendored patch |
| `[03-main-recomputes-failed-bbn-rows-on-every-run]` | belongs with the version-column issue |
| `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` | needs a dated addendum to `numerical-strategies.md` §7 |
| `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]` | a decision for the authors |
| `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]` | **new, found by this prompt** (§4.5); outside the campaign's scope |

---

## 4. Handover to the numerical campaign

Written for whoever plans the campaign that regenerates the scalar histories and the BBN survey.
Everything here is about the tree at `01e5975` or later.

### 4.1 Start from an empty database

- **Every store built under `VERSION_LABEL = "2026.1.1"` is invalid and must not be reused.** The
  label is now **`"2026.2.0"`** (`main.py:83`, `plot_by_beta.py:74`).
- **Why.** On 2026-09-29 the temperature law lost a spurious factor ln 10 in its entropy term (R1),
  and the low-T g's changed (R5). So every `ScalarModel`, `AdiabaticHistory` and `BBNData` row
  computed since 2026-01-19 (`5962833`) is wrong.
- **Nothing will stop you reusing an old store.** No lookup filters on the `version` column
  (`[00-datastore-lookups-ignore-the-version-column]`), so a corrected `main.py` pointed at an old
  store is handed its stale rows without complaint.
  - The one exception is `BBNData`. The first lookup on an old store fails loudly with `no such
    column: BBNData.failure_reason` (log 03).
  - `ScalarModel` and `AdiabaticHistory` have no such tripwire.
- **Start with a new database file.**

### 4.2 The run list

**The grid.** Use `exponential.yaml` restricted to **1.1 ≤ β ≤ 3** at finer spacing.

- **As it stands** the file samples β from 0.1 to 25 at `samples-per-beta: 5`. `main.py` turns
  that into 125 values, Δβ ≈ 0.20.
- **Keep everything else the same**, so the comparison with the paper's runs is like for like:
  - `M-values-Mp: 0.5`;
  - `Lambda-values-eV: 1E-3`;
  - `log10-one-plus-z-high: 85`;
  - the tolerances (`abs-tol`, `rel-tol` 1e-8);
  - T\* = 2×10⁴ GeV (the `--T-init-GeV` default, `config/argument_parser.py:13`);
  - φ\* = 5 M_P, π\* = 0 (hard-coded, now at `main.py:815–817`).
- **For example,** `beta-low: 1.1`, `beta-high: 3.0`, `samples-per-beta: 20` gives 38 values at
  Δβ ≈ 0.051, by `main.py`'s `int(round(samples_per_beta * (beta_high - beta_low) + 0.5))`. The
  spacing is the campaign's choice. It needs to be fine enough to resolve the
  1.1 ≤ β ≲ 2 band that is missing from the paper's `BBNdhPlot` (audit §3).

**Also run:**

1. **The SM baseline through the same PRyMordial path.** `tools/bbn_baseline.py` gives it in about
   10 s, and `plot_by_beta.py` draws it unless `--no-baseline` is given. On this tree it is:
   - Yp 0.2468872958;
   - D/H 2.462251065 × 10⁻⁵;
   - ³He/H 1.042050273 × 10⁻⁵;
   - ⁷Li/H 5.423441017 × 10⁻¹⁰.
2. **The review's three cheap tests** (review H6), as they now stand:
   - *"Rerun with the ratio spline."* The ratio spline is now the production interface (prompt 04),
     so this test is passed by construction. The comparison still worth making is log 04's
     observation 3: re-run one real history with ρ_SM taken from a spline of the stored
     `log_rhorad_Jordan` instead of the thermodynamic formula. That is a one-line change in
     `compute_BBN_data`. On a parked synthetic family the two differ by 5.9e-5 in Yp and 7.7e-4
     in D/H (log 04).
   - *"Rerun with ρ_NP set to zero below 1 MeV."* **No switch exists for this.** It needs a small
     scratch driver that wraps the `NPCallbacks` from `build_NP_callbacks` and passes them to
     PRyMordial, or a code change. That is the campaign's decision.
   - *"Plot `density_NP_ratio` against T_J for two adjacent β."* `plot_ScalarModel.py` (≈ `:1101`)
     draws it against T_J for one model; the two-β overlay is not in the tree.
3. **`density_NP_ratio` at T_J = 1 MeV for β = 2** (review H1).
4. **`hard_reflections` for every plotted history.** Read it from the `ScalarModel` row's
   `extra_data` JSON, where the key is **`number_hard_reflections`**. **Do not trust the "Hard
   reflections:" caption on `plot_ScalarModel.py`'s figures.** It looks up a different key and
   prints 0 whatever the count (§4.5).
5. **A `density_NP_ratio` against T_J overlay for two adjacent β.**

### 4.3 Expected magnitudes: how to tell a result from a bug

**For a parked field with A² − 1 = r.** PRyMordial through `compute_BBN_data`'s settings, with a
constant ratio r = 0.08, p = ρ/3 and the Saikawa–Shirai g_ρ ([F] `constant` against [F] `zero`,
`01e5975`):

| | SM baseline | r = 0.08 | change |
|---|---|---|---|
| N_eff | 3.04439 | 3.71342 | +0.669 |
| Yp | 0.2468872958 | 0.2540937879 | **+2.92 %** |
| D/H × 10⁵ | 2.462251065 | 2.671499971 | **+8.50 %** |

This is README §2 (f)'s "+8.5 %", reproduced. A `density_NP_ratio` near **0.08 at 1 MeV for β = 2**
is what the review's H1 predicts (A² − 1 ≈ 0.08 for the parked β = 2 field).

- **Signs of a remaining bug:**
  - `density_NP_ratio` near 0.08 at 1 MeV but D/H within a fraction of a per cent of the
    baseline. That was the old code's signature, when ρ_R,J was suppressed 50× at weak freeze-out.
  - A D/H curve flat in β across the band where the field has not yet parked.
- **Signs of PRyMordial's noise, not physics:**
  - D/H differences below **≈ 7e-4 relative** are not resolved. Rescaling ρ_NP by 1 − 1e-8 moves
    D/H by 7.1e-4 (log 04; `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`).
  - So β-to-β wiggles in D/H at the 1e-4 level mean nothing.
- **The network.** Every BBN solve so far, including every figure above, ran the **full** network,
  whatever the stored `small_network` flag says (`[03-small-network-flag-is-never-read-by-prymordial]`).
  - The small network moves ⁷Li/H by about 1 %, and Yp and D/H by 1.5e-6 and 1.5e-4.
  - **Decide which network the survey uses before it starts.** Fixing the flag changes physical
    output, so it needs a version decision of its own.
- **Redshift labels change by a factor ≈ 4.15.** The old code reached T_CMB 1.422 e-folds late, so
  1 + z_label = e^{1.422} (1 + z_true) (log 01: offset +1.422278). On the new code the offset is
  −3.6e-8 e-folds. Any comparison of new against old output **at fixed z** must allow for that;
  comparisons at fixed T_J need no such correction.

### 4.4 Which paper figures carry the redshift-label offset — questions for the authors

The offset of audit §1, consequence 3, applies to anything read from ChamPBH output **against its
stored `z`**. R1 affects every history independently of the axis: the cooling rate through the
transitions, T_J(N), the stored ρ_R,J and the BBN interface.

- **Sources.** `Paper1.tex` in the paper repository, 2026-09-30. Axis text from `pdftotext` on
  the figure files. "Committed" is that repository's `git log` date for the figure file, not
  necessarily the date the plot was made.
- **Every figure was committed after `5962833` (2026-01-19).** So any that came from ChamPBH was
  made on the defective law.

| Figure (label, file) | x-axis | Offset? | Otherwise affected by R1? | Question for the authors |
|---|---|---|---|---|
| `SigmaPlot`, `KickingFunction.pdf` (committed 2026-09-14) | T_J | no | no: an EOS function, unchanged by the campaign | Was it drawn from `Xav_EOS_data.csv` / `Xav_EOS_spline.w`? Its peaks should then be the table's; see `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]` |
| `VeffPlot`, `SmallBigM.pdf` (2026-06-26) | T_J | no | **yes**: the field history through the transitions | Made from ChamPBH `ScalarModel` output? If so, regenerate |
| `PhiPlot`, `PhiEvolDerivTJ1.pdf` (2026-09-18) | N, 0–50, with a T_J(N) panel | **only if N was derived from the stored z** | **yes**: T_J(N) cooled 1.35–1.6× too slowly through the kicks, and N to T_CMB was 41.50 against 40.08 | Is N counted from T\* (`raw_N`) or computed from the stored z? |
| `AdiPlot`, `Adiabaticity1.pdf` (2026-06-26) | T_J | no | **yes**: the history. The diagnostic also omits the H5 term | Made from ChamPBH `AdiabaticHistory` rows? |
| `EnergyPlot`, `EnergyDensity.pdf` (2026-06-26) | T_J | no | **yes, directly.** If ρ_R is the stored `log_rhorad_Jordan`, it was 0.022× the thermodynamic value at 1 MeV and 0.0044× at 70 keV | Is ρ_R the stored `log_rhorad_Jordan`? |
| `BBNdhPlot`, `BBN.pdf` (2026-06-26) | β | no | **yes**: the flat D/H curve (audit §1, consequence 1) and the models dropped silently (audit §3) | — (made by `plot_by_beta.py`) |
| `EoScompPlot`, `OmegaFrame.pdf` (2026-06-26) | T_J | no | **yes**: the history | Made from ChamPBH output? |
| `DEDenomPlot`, `DenominatorZero.pdf` (2026-06-26) | **z**, 1–4 | **yes, if made from ChamPBH output**; its ρ⁰_DM a³ normalisation would also be off (the stored "today" has ρ_m ≈ ρ_m0/70) | yes | **Provenance unknown: there is no dark-energy code in this repository.** Which script made it, and from which data? |
| `DEpolePlot`, `DEeosPole.pdf` (2026-06-26) | **z**, 1–5 | **yes, if made from ChamPBH output**; the pole redshifts would be labels, not true redshifts | yes | Same question |

### 4.5 Seeded issues the numerical campaign may want fixed first

- **H5, `[00-adiabaticity-diagnostic-omits-the-source-response-term]`.** Fix it before regenerating
  `AdiabaticHistory` rows, or the rows will need regenerating twice. It needs dΣ/d ln T from the
  w-spline, which is not yet exposed.
- **H8, `[00-initial-field-value-is-hard-coded-and-unchecked]`.** Fix it if the survey goes beyond
  β ≈ 6.5, where A\*T\* > M_P. The restricted 1.1 ≤ β ≤ 3 grid stays below that, but φ\* is still
  not in the store tags.
- **The network decision,** `[03-small-network-flag-is-never-read-by-prymordial]` (§4.3). Make it
  before the first BBN row is stored.
- **New, found by this prompt: `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`.**
  Reasoned from the code, not run.
  - The count is stored under `extra_data["number_hard_reflections"]`
    (`ComputeTargets/ScalarModel.py:1271`, `store_attr("hard_reflections", "number_hard_reflections", 0)`).
  - `extract_common.add_ScalarModel_labels` tests `"hard_reflections" in extra_data`
    (`extract_common.py:216`). That key is never present, so the `else` branch prints
    "Hard reflections: 0" on every `plot_ScalarModel.py` figure, whatever the count.
  - This also qualifies audit §8 and `[00-hard-reflection-count-is-stored-but-never-reported]`. A
    caption does read the count, and gets it wrong.
  - It matters for run-list item 4.
- **Also worth having before a large survey:**
  - `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`: a failed PRyMordial
    integration is not detected;
  - `[03-main-recomputes-failed-bbn-rows-on-every-run]`.

---

## 5. Reproduce

From the repository root: the commands in the provenance table, about 75 s in all, most of it the
`ComputeTargets` suite and the four PRyMordial fixture solves. For the scope check, run
`git diff --stat f5896bb..01e5975`.
