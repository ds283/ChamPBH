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

### 4.6 Addendum 2026-09-30 — the `production-readiness` campaign

Added by `production-readiness` prompt 04, measured on `6cab788` (the tree the campaign's three
prompts left, before this commit). Nothing above this heading has been changed; where it is
superseded, this section says so by statement. The campaign fixed four of the issues §4.5 names:
the reflection count, the caption, the network flag and the adiabatic mass. It bumped
`VERSION_LABEL` once. Its board is
[`prompts/production-readiness/IMPLEMENTATION_STATE.md`](../prompts/production-readiness/IMPLEMENTATION_STATE.md).

**This supersedes**, by statement and not by edit:

- **§4.2 item 4's caption caution.** The caption is now right; see point 3.
- **§4.3's "decide which network".** The user decided; see point 2.
- **§4.5's H5 bullet and its network bullet.** Both issues are closed; see points 2 and 4. §4.5's
  bullet on `[06-hard-reflection-caption-…]` is closed too (point 3).

**The five points.**

1. **`VERSION_LABEL = "2026.3.0"`; every store made before it is invalid.** That includes any made
   under `2026.2.0`, which §4.1 called the valid label. The production run starts from an empty
   database.
   - *Evidence.* `main.py:89` and `plot_by_beta.py:79`, `grep -n VERSION_LABEL main.py
     plot_by_beta.py`. One bump, made by prompt 02 (`8503fe7`); prompt 03 (`db0d8fc`) lands under
     the same label.
   - *Why.* The network flag changes physical output (point 2) and the adiabatic mass changes
     (point 4). Still nothing stops an old store being reused
     (`[00-datastore-lookups-ignore-the-version-column]`, open).
2. **The network.** BBN solves use the **full** network, and the stored `small_network` column now
   describes the run.
   - *Evidence.* `_configure_PRyMordial` sets `PRyM_init.smallnet_flag`, the name PRyMordial reads
     (`ComputeTargets/BBNData.py:264`); `test_network_flag` (a), which fails on `cf773b2`'s
     `BBNData.py` (log 02). `main.py:755`, the `compute_BBN_data` default (`BBNData.py:301`),
     `plot_by_beta.py:903` and `tools/bbn_baseline.py`'s default all pass `False`. Every pinned
     abundance passes unchanged: they were full-network values all along. The SM baseline is as
     §4.2 item 1 (Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017).
   - *The small network, for awareness.* `small_network=True` now runs the 12-reaction network. On
     the constant 0.08 family it moves ⁷Li/H by 1.006e-2, D/H by 2.274e-4 and Yp by 6.200e-5
     (`test_network_flag` (b)). The user decided on 2026-09-30 that the Yp and D/H offsets are
     PRyMordial's property and are not bounded; ⁷Li/H alone witnesses the flag.
   - The ⁷Li warning in `add_BBN_info_labels` (`extract_common.py:149`, now `==`) no longer fires
     for a production store.
3. **The reflection count.** It is in `plot_by_beta.py`'s `data.csv` (column `hard_reflections`:
   the count, 0 when the key is absent, NaN when no model) and on stdout (one summary line per
   (M, Λ), then one line per model that reflected), and in every `plot_ScalarModel.py` caption.
   - *Where to read it.* One reader, `extract_common.hard_reflection_count`, over the stored key
     `HARD_REFLECTIONS_KEY = "number_hard_reflections"` (`ScalarModel.py`). The count 0 is never
     stored, so an absent key means zero.
   - *Evidence.* `test_hard_reflection_reporting`, 5 tests, all OK; the caption test fails on
     `cf773b2`'s reader (log 01). The grep for `"hard_reflections"` outside the payload and the
     test finds only the CSV column name in `plot_by_beta.py:500`.
4. **The adiabatic mass** now includes the source response, so **no `AdiabaticHistory` row made
   before 2026.3.0 is comparable.**
   - *What to expect.* M²_eff/H² changes by the term 3 M_P² E (ln Ω)′² S, with bracket
     S(1 + f_m) = Σ² − Σ_T/(1 + x) + f_m. In the bracket norm the radiation part ranges over
     **−0.4067 at 144.3 MeV to +0.3498 at 230.4 MeV** (`test_adiabatic_mass` (b), f_m = 0,
     1000 points over [12 keV, 20 TeV]). That is O(β²) through the QCD and e⁺e⁻ features, up to
     about ±5 in M²/H² at β = 2. In matter domination it tends to 3β² f_m/(1 + f_m) (the textbook
     β²ρ_m/(M_P²H²)).
   - *Matter-limit check.* At f_m = 1e6 the code differs from β²ρ_m,E/(M_P²H²) by **4.067e-7**
     relative, at 144.3 MeV; that is |B_min|/f_m, as log 03 §1.3 predicts (`test_adiabatic_mass`
     (d), target 1e-6).
   - *Against the independent reference* (built from entropy conservation and `w`, `G_s` only):
     9.570e-8 / 4.702e-8 / 1.950e-9 for f_m = 0 / 1 / 100 (target 1e-6, test (b)). The audit's
     form, which lacks the factor 1/(1 + x), is off by up to 0.7327 at 153.8 MeV and has the wrong
     sign at the QCD peak (test (f)); it is not implemented.
   - *Expect max |Q| to change*, and Q no longer depends on the sampling where M²_eff crosses zero.
     Q's numerator is m(1 + Ḣ/H²) + ½ dm/dN from a spline of asinh m. On the crossing history it
     is within 3.070e-6 of max |A·C| against the analytic value on N ∈ [0.5, 11.5]; the old
     route was off by 1.529. The two end samples of a history are 2.26e-4 (accepted by the user;
     recorded, not bounded). At an exact zero of M²_eff, Q is finite and matches ½(dm/dN)/(k_p/H)³
     to 4.65e-6.
   - *What Q assumes*, unchanged: a test field on an unperturbed background; the source's response
     at fixed a_E and entropy; Q at fixed k_p/H ∈ {10 … 10⁴}. The last two are open for the
     authors (`[00-adiabaticity-is-evaluated-at-fixed-k-over-H-not-for-fixed-comoving-modes]`,
     `[00-paper-gives-two-inconsistent-adiabaticity-conditions]`).
   - *Not measured.* How far max |Q| moves on a real history, and Ḣ/H² from the policy formula
     against the stored H samples: both need a solve, which is the production run's.
5. **The issues still open on the `review-remediation` board** that the production run may want
   fixed first, by name: H8 `[00-initial-field-value-is-hard-coded-and-unchecked]` (β beyond
   about 6.5); `[00-datastore-lookups-ignore-the-version-column]`;
   `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`;
   `[03-main-recomputes-failed-bbn-rows-on-every-run]`. The full list is
   [`.documents/OPEN_ISSUES.md`](OPEN_ISSUES.md) (13 open on this date, 2 of them on the
   `production-readiness` board, both for the authors).

**Verification table.** Each README §6 row of the `production-readiness` campaign, on `6cab788`.
Tests print their figures with `CHAMPBH_TEST_REPORT=1`. "Log" is the figure the prompt's log
quotes; every one reproduced to the digits printed.

| Row | Target | Final value | Witness |
|---|---|---|---|
| 6.1 caption, payload count 3 | "Hard reflections: 3" | "Hard reflections: 3" | `test_hard_reflection_reporting` (c) |
| 6.1 caption, count 0 | unchanged, key absent | "Hard reflections: 0", key absent | same |
| 6.1 `extra_data` from a payload | identical to the unfactored block | identical on four payloads | same (a) |
| 6.1 readers of the stored key | 1 function, grep finds nothing else | `hard_reflection_count`; grep finds only the CSV column name `plot_by_beta.py:500` | `grep -rn "'hard_reflections'\|\"hard_reflections\"" --include='*.py'` |
| 6.1 `data.csv` column | `hard_reflections` | present (`plot_by_beta.py:500`, NaN when no model) | read the diff |
| 6.1 stdout summary | one line per (M, Λ), one per reflecting model | present (`plot_by_beta.py:520–550`) | read the diff |
| 6.1 new test on `HEAD~1` | fails | failed (log 01); **not re-run here** (needs a production file swapped) | log 01 |
| 6.2 `smallnet_flag` after `(True)` / `(False)` | True / False | True / False | `test_network_flag` (a); fails on `cf773b2` (log 02) |
| 6.2 `small_network_flag` in `ComputeTargets/ tools/ main.py plot_by_beta.py` | empty (comments allowed) | comments only: `BBNData.py:263`, `prym_fixtures.py:193`, `test_network_flag.py:21, 176` | grep |
| 6.2 `small_network` default | False everywhere | False: `main.py:755`, `BBNData.py:301`, `plot_by_beta.py:903`, `tools/bbn_baseline.py`, `run_prym` | grep; `test_network_flag` (c) |
| 6.2 pinned abundances | pass unchanged | pass: Yp 7.521e-11, D/H 2.768e-07 against the pins; SM baseline as §4.2 | suite |
| 6.2 ⁷Li/H, small vs full | ≥ 5e-3 | **1.006e-2** (D/H 2.274e-4, Yp 6.200e-5 recorded, unbounded) | `test_network_flag` (b) |
| 6.2 `add_BBN_info_labels` | `==` | `==` (`extract_common.py:149`) | read |
| 6.2 `VERSION_LABEL` | `"2026.3.0"` in both | `"2026.3.0"` (`main.py:89`, `plot_by_beta.py:79`) | grep |
| 6.3 `dw_dlogT`, `Xav_EOS_spline`, ≥ 1000 points | ≤ 1e-6 | **3.375e-8** (1010 points, 143.0 MeV) | `test_eos_w_derivative` |
| 6.3 same, spline class and base formula, above 2 MeV | ≤ 1e-6 (h = 1e-5 within a factor 1.03 of 120 MeV, by the user's decision) | **4.737e-9** (h = 1e-4, 1007 points); **1.102e-8** (h = 1e-5, 3 points in the window; h = 1e-4 there 1.104e-6, reported) | same |
| 6.3 same, jax class | ≤ 1e-6 | **7.051e-9** (1010 points, 118.4 MeV) | same, jax importable |
| 6.3 conformal part vs reference, exponential, f_m = 0 / 1 / 100 | ≤ 1e-6 | **9.570e-8 / 4.702e-8 / 1.950e-9** | `test_adiabatic_mass` (b) |
| 6.3 same, stand-in coupling | ≤ 1e-6 | **7.589e-8 / 3.617e-8 / 3.762e-9** | (c) |
| 6.3 f_m → ∞ limit | 1e-6 relative | **4.067e-7** | (d) |
| 6.3 A·C, both histories, ΔN = ln 10/250 | ≤ 1e-4 of max \|A·C\|, N ∈ [0.5, 11.5] | **3.070e-6** (crossing), **6.304e-6** (spikes); all samples 2.258e-4 / 6.304e-6 | (e) |
| 6.3 exact zero of M²_eff | finite Q, ½(dm/dN)/(k_p/H)³ to 1e-4 | relative **4.650e-6**, nothing raised | (e) |
| 6.3 audit's form | measured, reported | **0.7327** at 153.8 MeV | (f) |
| 6.3 `ScalarModel.py` in the diff | absent | present only as prompt 01's `HARD_REFLECTIONS_KEY` and `build_extra_data` factoring (75 lines, read); no change in prompt 03 | `git diff 204795e..HEAD -- ComputeTargets/ScalarModel.py` |
| 6.3 planning probe, Table 1 | matches log 03's | matches to every printed digit (Σ, dΣ/d ln T, x, B_audit, B_closed form = B_reference, ten temperatures); Table 2 9.887e-6 / 9.885e-8 / 9.944e-10; both `q_sign_change_probe` rows as README §2 (e) | `PYTHONPATH=. venv/bin/python prompts/production-readiness/planning-probes/h5_bracket_probe.py` |
| 6.4 suites | pass; counts not below 12 / 13 | `CosmologyModels/tests` **18** OK (109.6 s); `ComputeTargets/tests` **30** OK (74.7 s); at `204795e`: 12 / 13 | the two `unittest discover` commands |

The `git diff --stat 204795e..HEAD` on `6cab788`, 43 files, 4990 insertions and 114 deletions,
is all in files the campaign's plan, boards and logs allow: the production files of prompts 01–03
(`extract_common.py`, `plot_by_beta.py`, `main.py`, `ComputeTargets/{AdiabaticHistory,BBNData,
ScalarModel}.py`, `CosmologyModels/GenericEOS/*` (five files), `tools/bbn_baseline.py`); their
tests (`prym_fixtures.py`, `test_bbn_callbacks.py` and four new modules); dated additions to
three documents under `.documents/` (additive only: 6, 53 and 88 lines, no deletions) and the
index; and the campaign's own planning material. `PRyM/`, `thirdparty/`, any schema and
`Xav_EOS_data.csv` are absent from it. `prompts/review-remediation/IMPLEMENTATION_STATE.md` has
28 additions and no deletions: the four **Resolved** lines.

**To reproduce all of it** from the repository root (about 4 minutes):

```bash
PYTHONPATH=. CHAMPBH_TEST_REPORT=1 ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. CHAMPBH_TEST_REPORT=1 ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/h5_bracket_probe.py && PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/q_sign_change_probe.py && git diff --stat 204795e..HEAD && grep -n VERSION_LABEL main.py plot_by_beta.py
```


### 4.7 Addendum 2026-09-30 — the `run-integrity` campaign

Added by `run-integrity` prompt 04, measured on `6fd9017` (the tree the campaign's three prompts
left, before this commit). Nothing above this heading has been changed; where it is superseded,
this section says so by statement. The campaign fixed three of the issues §4.5 and §4.6 leave open,
and two the planner found beside them: lookups that ignore the version label, PRyMordial solves
that fail silently, failed BBN rows recomputed on every run, a NaN that hangs PRyMordial, and
lookup results paired with the wrong models. It bumped `VERSION_LABEL` once. Its board is
[`prompts/run-integrity/IMPLEMENTATION_STATE.md`](../prompts/run-integrity/IMPLEMENTATION_STATE.md).

**This supersedes**, by statement and not by edit:

- **§4.6 point 1's "Still nothing stops an old store being reused".** Something does now: point 1
  below.
- **Every earlier statement that `VERSION_LABEL` is `"2026.3.0"`**, including §4.6 point 1's
  evidence (`main.py:89` and `plot_by_beta.py:79`). The label is `"2026.4.0"`, defined in one
  place, `config/version.py:33`. `main.py` and `plot_by_beta.py` no longer define it.
- **§4.5's and §4.6's mentions of `[00-datastore-lookups-ignore-the-version-column]` as open.** It
  is resolved (point 1).

**The five points.**

1. **`VERSION_LABEL = "2026.4.0"`, defined once in `config/version.py`. Every store made before
   2026.4.0 is invalid, and a lookup no longer returns such rows.** An old store opened under the
   new label recomputes every compute target beside its old rows, rather than reusing them. The
   fresh-database rule (§4.1) is still the clean choice, but it is no longer the only protection.
   - *Evidence.* `grep -rn "VERSION_LABEL =" --include='*.py' .` outside `venv/`, `thirdparty/`
     and `claude-context/` finds one line, `config/version.py:33`. `main.py:65`,
     `plot_by_beta.py:49` and `plot_ScalarModel.py:57` import it; `plot_ScalarModel.py` moves from
     `"2026.1.1"`. The three compute-target factories register `"key_on_version": True` and filter
     `table.c.version == serial`, raising if the serial is missing; `Datastore.object_get` supplies
     it. Tests (a)–(d) and (f) of `Datastore/tests/test_version_keyed_lookups.py` (prompt 01, log
     01) fail on `90b2c86`. `datastore_version_probe.py` on this tree prints `available=False` for
     its lines [3] and [5].
   - *What a bump costs.* Every history is recomputed, including for a change that touches only
     BBN. Per-target labels are out of scope. `--inventory` still lists every version.
   - *What stays unkeyed.* Parameter tables (couplings, potentials, value tables). Test (e) checks
     that one β gives one serial under two labels.
2. **BBN failures are detected.** A `solve_ivp` that gives up raises `PRyMSolverFailureError`
   inside PRyMordial. Any `Exception` inside the PRyMordial call becomes a failure row with its
   reason (`"PRyMordial: <Type>: <message>"`). A non-finite new-physics sample, or a callback
   called with a non-finite T, fails before PRyMordial starts. `PRyM_version` is
   `"bf24c3d+cham03+ri02"`. **An exception from ChamPBH's own code outside the PRyMordial call
   still stops the run, by design.**
   - *Evidence.* All eight `solve_ivp` calls in `PRyM/PRyM_main.py` are followed by a marked
     `_check_solve_ivp` (`grep -c`: 8 and 8; ten marked insertions in the diff, no deletions). The one new
     `except Exception` is `ComputeTargets/BBNData.py:336`, inside `_run_PRyMordial`, around the
     `PRyMclass(...).PRyMresults()` call only. Tests (a), (b) and (d) of
     `ComputeTargets/tests/test_bbn_solver_failures.py` fail on `f0de762`; every pinned abundance
     passes unchanged.
   - *The probe.* As committed, `prymordial_solver_probe.py truncated` now truncates nothing,
     because it picks the solve by line number (`truncate_line = 1252`) and the patch moved that
     call to `:1291`. It returns the SM baseline, Yp 0.2468872958, D/H x1e5 2.462251065, ⁷Li/H
     x1e10 5.423441017. A scratch copy with only that number changed raises
     `PRyMSolverFailureError` in stage `'low-T nuclear network (full)'`. The probe's `nan` case
     calls PRyMordial with its own callback, bypassing `build_NP_callbacks`, so it still does not
     return within 60 s; it measures PRyMordial, not the guard.
   - *Where a NaN sample actually fails.* README §2 (c) said PRyMordial hung on a NaN in the
     density ratio. On `f0de762` `make_interp_spline` raised `ValueError` first (log 02,
     Deviation 5, accepted by the user); the hang comes from a callback called with T = NaN. Both
     are closed.
3. **Failed BBN rows are final within a label.** A new label retries them, and so does
   `--retry-failed-bbn` (default off). Reasons are in `BBNData.failure_reason`, in the per-stage skip
   summary on stdout (`main.py:531`, `:793`), and in `plot_by_beta.py`'s failure lookup.
   - *Evidence.* `main.py:632` passes `"failure": None`. `BBNData.build(failure=None)` returns the
     success if one exists, else the newest failure. Test (a) of
     `Datastore/tests/test_bbn_failure_lookup.py` raises `MultipleResultsFound` on `765e80d`.
     `config/argument_parser.py:246` defines the flag.
4. **A failed `ScalarModel` no longer misdirects the adiabatic or BBN stage.** Both stages pair
   lookups through `pipeline_selection.build_query_entries` and `select_missing`, which raise on a
   length mismatch (`main.py:357`, `:407`, `:610`, `:662`). On `pairing_probe.py`'s bin the helper
   selects {V2, V4}; the probe's copy of the old logic selects {V1, V3}. V1's `ScalarModel` failed,
   and the old logic would have stopped the run on it.
5. **What is still open,** by name. H8 (`[00-initial-field-value-is-hard-coded-and-unchecked]`).
   - The NaN route not closed: a non-finite EOS value
     (`[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]`).
   - A short sample grid that escapes `compute_BBN_data`
     (`[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]`).
   - Unvalidated compute-target rows are still served by `AdiabaticHistory` and `BBNData` lookups
     (`[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]`); see the board for its
     scope.
   - A Ray task timeout, and a worker that dies: neither is handled, and neither has an issue.
   - Step 1's redundant first-pass lookup (`[03-step-1-first-pass-lookup-filters-nothing]`).
   - `AdiabaticHistory.build` ignores `_do_not_populate`, so every adiabatic lookup reads every
     value row (`[04-adiabatichistory-lookup-ignores-do-not-populate]`).

**Verification table** (README §6; measured on `6fd9017`; "suite" is the `unittest discover`
command of `CLAUDE.md`). Suites: `CosmologyModels/tests` 18 OK (80.0 s), `ComputeTargets/tests` 41
OK (89.4 s), `Datastore/tests` 17 OK (1.2 s); at `27a32bc`, 18 / 30 / 0. A row marked "log" was
shown to fail on `HEAD~1` by that prompt's log, and is not re-shown here.

| Row | Target | Final value | Witness |
|---|---|---|---|
| 6.1 `ScalarModel`, `AdiabaticHistory`, `BBNData` (`failure=True` and default) stored under A, looked up under B | not returned | not returned | `Datastore/tests/test_version_keyed_lookups.py` (a), (b), (c1), (c2); suite; fail on `90b2c86` (log 01) |
| 6.1 the same rows under A | returned, unchanged | returned | same |
| 6.1 a keyed `build()` with no serial | raises | raises `RuntimeError` | (d); suite |
| 6.1 `ExponentialCoupling` under A then B | one row, same serial | one row, same serial | (e); suite (a regression guard, passes on both) |
| 6.1 `VERSION_LABEL` definitions | 1, in `config/version.py` | 1: `config/version.py:33`, value `"2026.4.0"` after prompt 02 (`"2026.3.0"` after prompt 01, as targeted) | grep above; (f) |
| 6.1 dated label comment | in `config/version.py`, verbatim | present, `config/version.py:19–27`, plus dated sentences for prompts 01 and 02 | read; log 01 |
| 6.1 schema | unchanged | unchanged | the `register()` diff is the flag and its comment only; `sqlite_master` byte-identical on prompt 01 (log 01) |
| 6.1 `Datastore/tests` count | ≥ 5, in `CLAUDE.md` | 17; the command is in `CLAUDE.md` | suite |
| 6.2 five production calls forced to fail | raises, names the stage | raises, five stage names | `test_bbn_solver_failures` (a); fails on `f0de762` (log 02) |
| 6.2 two small-network calls forced to fail | raises, names the stage | raises | (b); same |
| 6.2 line 239 | patched | patched (8 of 8) | `grep -c "= solve_ivp(" PRyM/PRyM_main.py` = 8; `grep -c "^ *_check_solve_ivp(sol_"` = 8 |
| 6.2 a forced failure through the helper | failure payload `"PRyMordial: "` | as targeted | (c); suite |
| 6.2 a `RuntimeError` callback inside PRyMordial | failure payload naming the type | as targeted | (c); suite |
| 6.2 an exception outside the helper | propagates | propagates | one new `except`, `BBNData.py:336`, inside `_run_PRyMordial` |
| 6.2 NaN in the density ratio | `ComputationFailureError` before any solve | as targeted | (d); suite; on `f0de762` it raised `ValueError` (log 02 Deviation 5) |
| 6.2 a callback at T = NaN | `ComputationFailureError` | as targeted | (d); suite |
| 6.2 every pinned abundance | unchanged | unchanged | suite |
| 6.2 `PRYM_VERSION` | `"bf24c3d+cham03+ri02"` | `ComputeTargets/BBNData.py:42` | grep; (e) |
| 6.2 `VERSION_LABEL` | `"2026.4.0"`, `config/version.py` only | `config/version.py:33` | grep |
| 6.2 patched lines in `PRyM/` | marked | ten insertions, each with `ChamPBH run-integrity prompt 02` | `git diff 27a32bc..HEAD -- PRyM` |
| 6.3 `build(failure=None)`, two failures | the newest | the newest | `Datastore/tests/test_bbn_failure_lookup.py` (a); fails on `765e80d` (log 03) |
| 6.3 failure then success; success then failure | the success | the success | (b1), (b2); same |
| 6.3 `failure=True`, `failure=False` | unchanged | unchanged | (c1)–(c3); suite |
| 6.3 `main.py`'s BBN lookup | `failure=None` | `main.py:632` | grep |
| 6.3 `--retry-failed-bbn` | present, default False | `config/argument_parser.py:246` | (g); suite |
| 6.3 helper on the probe's bin | {V2, V4} | {V2, V4}; {V2, V3, V4} with V3 failed and the flag | `test_pipeline_selection` (d), (e); `pairing_probe.py` still prints the old {V1, V3} |
| 6.3 wrong-length results | raises | raises `ValueError` | (f) |
| 6.3 adiabatic stage | same helper | `main.py:357`, `:407` | read |
| 6.3 skip summary | one line per stage | `main.py:531`, `:793` | read |
| 6.3 `VERSION_LABEL`, `PRYM_VERSION` | unchanged by prompt 03 | `"2026.4.0"`, `"bf24c3d+cham03+ri02"` | grep |

**To reproduce all of it** from the repository root (about 4 minutes):

```bash
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . && PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/datastore_version_probe.py && PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/pairing_probe.py && grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/" && grep -n PRYM_VERSION ComputeTargets/BBNData.py && git diff --stat 27a32bc..HEAD
```

### 4.8 Addendum 2026-10-01 — the `integrator-remediation` campaign

Added by `integrator-remediation` prompt 04, measured on `abcc99f` (the tree the campaign's three
prompts left, before this commit). Nothing above this heading has been changed; where it is
superseded, this section says so by statement. The campaign replaced the scalar-field integrator's
two-region fragment scheme with one Radau step loop under a kinematic step cap, kept the elastic
reflection as a deliberate model triggered at the representable-step floor, clamped SciPy's
Jacobian perturbation factor, deleted the solver fallback that was never wired, settled the
exception taxonomy, added a step budget, and documented all of it. It bumped `VERSION_LABEL` once.
Its board is
[`prompts/integrator-remediation/IMPLEMENTATION_STATE.md`](../prompts/integrator-remediation/IMPLEMENTATION_STATE.md);
its source is [`integrator-audit-2026-09-30/README.md`](integrator-audit-2026-09-30/README.md).

**This supersedes**, by statement and not by edit:

- **Every earlier statement that `VERSION_LABEL` is `"2026.4.0"`**, including §4.7 point 1 and its
  evidence (`config/version.py:33`) and the §4.7 verification rows that quote it. The label is
  `"2026.5.0"`, still defined once, at `config/version.py:36` (point 1).
- **§4.2's run-list expectations of cost per history.** §4.2 states none: the sections §4.2 and §4.3
  contain no run-time or step-count figure for a history. The costs to expect are the table under
  point 3.
- **Any reading of a hard reflection as a failure indicator.** §4.3 states none either. The
  statements to correct are §4.2 item 4 and §4.5, which name the stored key
  `number_hard_reflections` and a hard-reflection count to read for every plotted history. That
  key is no longer written, and the thing it counted is no longer done (the hard reflection at
  `φ = 0` is gone). The count to read is `number_reflections`, the **elastic** reflections of
  point 2, which is a feature of the model and not a symptom: it is 0 for every history with
  `M ≳ 1e-8`, and 140 for β = 0.9 at physical `M`, which completes.

**The five points.**

1. **`VERSION_LABEL = "2026.5.0"`, defined once in `config/version.py`. Every `ScalarModel`
   history made before it is invalid, and so is every `AdiabaticHistory` and `BBNData` row built on
   one.** The keyed lookups of §4.7 point 1 do not return such rows: an old store opened under the
   new label recomputes every compute target beside its old rows.
   - *Why.* Every step size of every history changed (prompt 01, `fc97233`). The histories are
     chaotic after delivery (audit §11), so an old row is not a perturbation of a new one.
   - *Evidence.* `grep -rn "VERSION_LABEL =" --include='*.py' .` outside `venv/`, `thirdparty/`
     and `claude-context/` finds one line: `config/version.py:36:VERSION_LABEL = "2026.5.0"`. One
     bump, in prompt 01; prompts 02 and 03 landed under it.
2. **The integrator.** `integrate_scalar_history` (`ComputeTargets/ScalarModel.py`) is one
   `scipy.integrate.Radau` instance stepped by hand, with:
   - *the kinematic cap,* `f = 0.1`: before every step the maximum step is
     `min(0.1, f φ/|π| if π < 0, sqrt(2 f φ/|π̇|) if π̇ < 0)`, never below the floor, so that no
     step can cross the repulsive wall; *the floor* `1e-11` e-folds, with *the elastic reflection*:
     if `π < 0` and `f φ/|π|` is below the floor, `π ← −π` and the solver restarts. The reflection
     is guarded: **G1**, the potential must declare `reflects_at_origin` (only
     `ExponentialPotential` does); **G2**, `W ≤ ½π²` with `W` the wall part of the potential
     fraction. Either failing is a failure row;
   - *the Jacobian clamp,* `jac_factor ≤ 1e-4` after every accepted step;
   - *one `OdeSolution`* of the accepted steps' dense outputs, sampled on the unchanged z grid;
     termination at the root of `ln T_J − ln T_stop` on the last step's interpolant;
   - *no regions, no fragments, no events, no hard reflection at `φ = 0`, no fallback.* An accepted
     `φ ≤ 0` is a failure. A `ComputationFailureError` from the RHS on a trial state is a rejected
     step (the step is halved; below `1e-13` e-folds it is the history's failure).
   - *Stored per history* (`extra_data`): `cap_fraction`, `cap_floor`, `cap_global_max_step`,
     `jacobian_factor_max`, `accepted_steps` always; `number_reflections` and
     `steps_rejected_by_exception` only when positive; the RHS statistics blocks as before. The
     ten region/fragment/hard-reflection keys are gone. The stepper label is
     `"Radau+kinematic-cap-stepping0"`, registered in `main.py`, `plot_by_beta.py` and
     `plot_ScalarModel.py`; the five old labels stay registered so that an old history still loads.
   - *Evidence.* README §6 of the campaign, row by row, in
     [`logs/04-close-out-verification.md`](../prompts/integrator-remediation/logs/04-close-out-verification.md).
     The loop is `numerical-strategies.md` §3.5; the paper's sentences that now disagree with the
     code are in [`paper-corrections-numerical-section.md`](paper-corrections-numerical-section.md).
     The grep for `HARD_REFLECTIONS_KEY`, `SolutionFragment`, `notify_level_1` and
     `notify_hard_reflection` over `*.py` outside the excluded directories and `prompts/` finds
     nothing; `solver_list`, `LSODA`, `DOP853`, `"BDF"` and `solve_ivp` are not in
     `ScalarModel.py`.
3. **The cost.** The nine full histories of the campaign's README §6.1 (d), from `main.py`'s
   initial data (`φ* = 5`, `π* = 0`, `T* = 2×10⁴ GeV`), `atol = rtol = 1e-8`, through the
   production loop (`integrate_scalar_history` with `StepControl()`), on `abcc99f`, one machine,
   run one after another. "Shipped" is the brief's figure on the two-region scheme.

   | β | M | RHS | accepted steps | wall | wall bounces | first bounce `N` / `T_J` | shipped (brief) |
   |---|---|---|---|---|---|---|---|
   | 0.9 | 0.5 | 26 372 | 3 040 | 0.8 s | 16 | 36.15918 / 1.044 eV | 0.06×10⁶ RHS |
   | 1.2 | 0.5 | 24 193 | 2 728 | 0.8 s | 17 | 17.66246 / 231.07 MeV | 1.52×10⁶ |
   | 2.0 | 0.5 | 40 580 | 4 469 | 1.5 s | 26 | 20.34303 / 746.63 MeV | 2.78×10⁶ |
   | 3.0 | 0.5 | 57 526 | 6 120 | 2.9 s | 48 | 24.48381 / 1 682.85 MeV | 3.13×10⁶ |
   | 1.2 | 0.01 | 108 789 | 11 484 | 5.1 s | 195 | 17.66783 / 231.11 MeV | 5.90×10⁶, **failed** at `N` = 37.165 |
   | 2.0 | 0.01 | 98 133 | 10 632 | 5.0 s | 196 | 20.35192 / 746.69 MeV | unfinished after 2 h |
   | 3.0 | 0.01 | 151 480 | 16 513 | 7.5 s | 284 | 24.49870 / 1 680.06 MeV | unfinished |
   | 2.0 | 0.001 | 271 783 | 27 979 | 13.4 s | 803 | 20.35208 / 746.69 MeV | unfinished |
   | 3.0 | 0.001 | 327 046 | 34 342 | 14.6 s | 1 017 | 24.49897 / 1 680.01 MeV | — |

   - All nine complete, to `T_J` = 2.7255 K, with 0 reflections, 0 rejected steps and no
     `T_Jordan = 0` substitution.
   - The audit's probe loop (`p_full.py … kin reflect`) agrees on RHS to under 1 % in every row
     and on the turning point to `10⁻⁹` in `N` (log 04).
   - **A science run of the grid of §4.2 costs seconds per history at `M = 0.5`.** The cost per
     history grows with the number of matter-era rebounds, so it is set mainly by `M`.
   - **Physical `M` (`M = 4.1×10⁻²⁸`, a field mass of 1 eV),** through `compute_scalar_model`:
     β = 0.9 completes with 140 elastic reflections in 187 485 RHS (26 312 accepted steps,
     6.7 s including the sampling). β = 2 does **not** complete: it ends as a failure row (`{"failure": True}`) on the step budget, after 1 414 s (23.6 min) of wall time: "step budget exhausted: integrate_scalar_history took 2000001 accepted steps (budget 2000000) at N=45.68459, T_J=1.896e-11 GeV, with 14769 reflection(s)", at about 89 % of the way to `T_CMB` measured in `ln T_J` (from 2×10⁴ GeV to 2.35×10⁻¹³ GeV, in the status line's measure). That is the cost of a clean failure at physical `M`: about 24 minutes, not days. A survey over β ≥ 1.2 at physical `M` therefore spends about that per history and stores a failure row.
4. **What a failure row means now.** `{"failure": True}` is what `ScalarModel.store()` records
   when `compute_scalar_model` catches a `ComputationFailureError`; the reason is **printed
   and not stored** (the schema has no column for it). The reasons are:
   - a step that could not be taken: too small after trial-state rejections, or Radau's own
     message (Newton failure, step too small);
   - **the step budget:** more than `2×10⁶` accepted steps ("step budget exhausted: … took n
     accepted steps (budget b) at N=…, T_J=… GeV, with r reflection(s)"). This is what a physical-`M`
     history with β ≥ 1.2 ends in, until the parked-tracking model of point 5 exists;
   - the failsafe (`N = 1000`); an accepted `φ ≤ 0` (the cap was violated); G1 or G2 at a
     reflection; an unbracketed termination root; a non-finite RHS output; `T_J ≤ 0` or another
     unphysical state that persists; or an error in the sampling.
   - **A `RuntimeError` is not a failure row.** The one left in the integration path is the z grid
     being too short for the final `N`; it is a configuration error and ends the run. An
     `AssertionError` is a bug.
   - *Where the reason appears.* On the task's stdout, as `-- compute_scalar_model (<label>):
     integration failure` followed by the message. The supervisors no longer print tracebacks.
   - A failure row's cause cannot be counted from the datastore; see
     `[00-scalarmodel-failure-rows-carry-no-reason]`.
5. **What is still open,** by name (all on the campaign's board §3, none assigned):
   - `[00-settling-at-physical-M-needs-a-parked-tracking-model]`: **no physical-`M` history with
     β ≥ 1.2 can be produced until it exists, and such runs fail on the step budget until then.**
     The authors' physics (audit §3.7, §9.4).
   - `[00-stored-samples-alias-the-rebounds]` (turning-point sampling);
   - `[00-atol-does-not-scale-with-phi]` (the `atol` vector);
   - `[00-analytic-jacobian-would-remove-num-jac]`;
   - `[00-scalarmodel-failure-rows-carry-no-reason]` (the failure-reason column);
   - `[00-region-properties-on-the-potentials-become-unread]` (the unread region properties on the
     potentials);
   - `[00-declare-reflects-at-origin-for-the-other-potentials]`;
   - `[01-trial-state-exception-in-radau-start-up-is-not-a-rejection]`;
   - `[02-negative-E-is-clamped-not-raised-on-trial-states]`.
   - A caution that is not an issue. A bounce's `N` and `φ_min` taken **at the accepted step after
     `π` turns positive** depend on the width of that step; compare turning points on the dense
     output (the root of `π` on the step's interpolant). At β = 0.9, `M = 0.5` the accepted-step `N`
     of the first bounce differs between the audit's probe and the production loop by 7.9×10⁻⁵ (the
     probe's step there is 8.5×10⁻⁵ wide); the dense-output turning points agree to 4×10⁻⁹. The
     user's ruling of 2026-10-01 on `φ_min` (the dense-output minimum) is the one to follow.

**Verification table** (the campaign's README §6; measured on `abcc99f`; the full rows, with
witnesses, are in log 04). Suites: `CosmologyModels/tests` 18 OK (103.6 s), `ComputeTargets/tests`
67 OK (135.3 s), `Datastore/tests` 17 OK (1.8 s), against 18, 41 and 17 at `2b89022`. The §6.1
(a)–(f) rows are at or better than target; the nine histories complete.

**To reproduce** from the repository root (the three suites take about four minutes):

```bash
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . && grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/" && git diff --stat 2b89022..HEAD
```

The audit's probe scripts do not run against this tree (`harness.build` passes the supervisor an
argument it no longer takes); run them from an export of `918590e` (`git archive 918590e`).

### 4.9 Addendum 2026-10-01 — how small `M` can go before the parked-tracking model is needed

Added after the `integrator-remediation` close-out, at the user's request. Measured on `1265c75`,
whose code tree is the same as `abcc99f`'s (§4.8). Nothing above this heading has been changed.
The question was whether histories at `M ≥ 1e-5` are safe without the parked-tracking model of
`[00-settling-at-physical-M-needs-a-parked-tracking-model]`, and how far below that they stay
safe.

**Provenance.** Every figure below was measured on `1265c75` by three scratch scripts, which are
not in the repository. They use only the helpers of
`ComputeTargets/tests/test_kinematic_cap_loop.py` (`build`, `integrate`, `wall_bounces`,
`interpolated_minima`).

- *Histories.* `integrate_scalar_history` with the default `StepControl()` (budget `2×10⁶`), from
  `main.py`'s initial data (`φ = 5`, `π = 0`, `T_init = 2×10⁴ GeV`), as log 04's `nine.py` does,
  to `T_CMB`. Stdout and stderr were captured. Six to twelve histories ran at once on ten cores,
  so the **wall times are inflated**: the `M = 0.01` baselines took 64–80 s here against 5–8 s in
  log 04. RHS and step counts do not depend on load.
- *Tolerances.* `integrate(P1, M, 21.0, params=StepControl(atol=…, rtol=…))` from the P1 state
  (β = 2).
- *Sampling.* The turning points are the sign changes of `π` between accepted states, located by
  linear interpolation. Half-periods are assigned to a window by `T_J` at their midpoint. A
  stored sample is one step of the production z grid, 250 per decade of `1 + z`
  (`DEFAULT_SAMPLES_PER_LOG10_Z`), so `ΔN = ln 10/250 = 0.00921`.

**1. Full histories at `M = 1e-5` and `1e-6`: all complete, inside the budget.** All six end at
`T_J = 2.72550 K`. In each there were 0 reflections, 0 rejected steps, 0 `T_Jordan = 0`
substitutions, 0 "negative value of E" prints and no traceback.

| β | M | RHS | accepted steps | fraction of budget | wall bounces | first bounce `N` / `T_J` (MeV) | `N` at `T_CMB` | wall (loaded) |
|---|---|---|---|---|---|---|---|---|
| 1.2 | 1e-5 | 874 284 | 91 349 | 0.046 | 1 338 | 17.667942 / 231.1076 | 46.0743 | 129 s |
| 1.2 | 1e-6 | 1 330 925 | 140 736 | 0.070 | 1 603 | 17.667942 / 231.1076 | 46.0750 | 256 s |
| 2.0 | 1e-5 | 1 679 987 | 162 676 | 0.081 | 4 457 | 20.352100 / 746.6864 | 50.0740 | 293 s |
| 2.0 | 1e-6 | 3 338 450 | 323 552 | 0.162 | 7 504 | 20.352100 / 746.6864 | 50.0749 | 391 s |
| 3.0 | 1e-5 | 2 165 080 | 207 448 | 0.104 | 6 561 | 24.499004 / 1 680.0056 | 55.0737 | 343 s |
| 3.0 | 1e-6 | 4 711 033 | 445 083 | 0.223 | 12 382 | 24.499005 / 1 680.0055 | 55.0749 | 531 s |

- The `M = 0.01` and `1e-3` baselines, re-run alongside, reproduce log 04's RHS, steps, bounces
  and first bounces exactly. At β = 1.2, `M = 1e-3` (not in log 04) the figures are 254 491 RHS,
  26 458 steps and 473 wall bounces.
- β = 2 at `1e-6` gives 3 338 450 RHS, against the 3.33×10⁶ of the audit's probe (audit §3.7);
  the audit counted 7 429 bounces against 7 504 here.
- From `1e-5` to `1e-6`, accepted steps grow 1.54× (β = 1.2), 1.99× (β = 2) and 2.15× (β = 3)
  per decade of `M`. **Extrapolation, not measurement:** at that rate β = 3 reaches the budget
  near `M ≈ 1e-8`, and β = 2 at a few ×1e-9. That second estimate falls where the floor
  reflection starts to fire (`M ≲ 3e-9`, audit §3.7), and the behaviour changes there. Neither
  limit is measured.

**2. The absolute tolerance does not limit accuracy at the first bounce, to `M = 1e-6`.** From
P1 to `N = 21`, the dense-output first-bounce `N` and `φ_min` are identical to the printed digits
under four settings. The settings are the scalar `atol = rtol = 1e-8` (the default), the vector
`[1e-8 M, 1e-8 M, 1e-8, 1e-8, 1e-8]`, scalar `1e-12`, and the vector at `1e-12 M`.

| M | first bounce `N` | `φ_min` | `φ(21)`, default | `φ(21)`, 1e-12 references | RHS: default / vector / 1e-12 scalar / 1e-12 vector |
|---|---|---|---|---|---|
| 1e-3 | 20.352082227 | 9.1505066e-6 | 1.184500643e-1 | 1.184500674e-1 | 2 275 / 2 894 / 13 904 / 20 426 |
| 1e-5 | 20.352100199 | 9.1505127e-8 | 1.184428761e-1 | 1.184428765e-1 | 2 831 / 3 466 / 17 627 / 26 062 |
| 1e-6 | 20.352100362 | 9.1505128e-9 | 1.184428107e-1 | 1.184428112e-1 | 3 003 / 3 842 / 17 949 / 25 796 |

`φ_min` falls below `atol` at `M = 1e-6`, yet nothing moves. The step cap, not the tolerance,
controls the error at the wall. `[00-atol-does-not-scale-with-phi]` stays a refinement.

**3. Sampling against the stored z grid, by Jordan-frame temperature window.**

- **10 MeV–1 keV (PRyMordial's working range): essentially independent of `M`.** For each β the
  window holds nearly the same number of half-periods, with nearly the same sampling, at
  `M = 0.01`, `1e-3`, `1e-5` and `1e-6`.
  - β = 1.2: 6 half-periods; a median of 152 samples per half-period (minimum 42).
  - β = 2: 43–45 half-periods; median 6.7–7.2 (7.2 only at `M = 0.01`), minimum 5.1.
  - β = 3: 97–99 half-periods; median 3.0, minimum 2.3.

  The β = 3 figure is marginal at every `M`; it belongs to `[00-stored-samples-alias-the-rebounds]`.
- **Below 1 keV: under-sampled at every `M`, much more so at small `M`.**

| M | median stored samples per half-period, 1 keV–0.1 eV (β = 1.2 / 2 / 3) | half-periods with fewer than 1 sample, same window | median `φ` swing / `φ` | max `π²/6` |
|---|---|---|---|---|
| 0.01 | 1.50 / 1.67 / 1.31 | 0 % / 19 % / 25 % | 4e-3 – 7e-2 | ≤ 4.1e-5 |
| 1e-3 | 0.59 / 0.42 / 0.35 | 78 % / 91 % / 93 % | 8e-3 – 0.24 | ≤ 4.2e-5 |
| 1e-5 | 0.32 / 0.06 / 0.05 | 87 % / 98 % / 99 % | 0.14 – 0.94 | ≤ 4.2e-5 |
| 1e-6 | 0.32 / 0.03 / 0.02 | 87 % / 99 % / 99 % | 0.39 – 0.99 | ≤ 4.2e-5 |

  Below 0.1 eV it is the same or worse: at `1e-5` and `1e-6`, 99–100 % of half-periods hold fewer
  than one sample.

**What this means.** The integration is measured safe at `M = 1e-5` and `1e-6` for β = 1.2, 2 and
3, using at most 22 % of the step budget. What BBN sees in PRyMordial's window barely depends on
`M` over this range. The adiabatic stage is the open question. `compute_adiabatic_values` takes
`max |Q|` over every stored sample, and `Q`'s numerator uses the spline derivative of
`asinh(M²_eff/H²)` on the z grid. Below 1 keV at small `M` that grid samples the bounces at random
phase. Whether this segment sets the stored maximum was not measured. It is opened as
`[post-adiabatic-Q-reads-aliased-late-samples]` on the `integrator-remediation` board.

### 4.10 Addendum 2026-10-02 — the `science-readiness` campaign

Added by `science-readiness` prompt 09, measured on `8efc50f` (the tree prompts 01–08 left, before
this commit). Nothing above this heading has been changed; where it is superseded, this section
says so by statement. The campaign replaced the `NP_thermo_flag` route to PRyMordial with a patched,
Hubble-only one, gave PRyMordial a wall-clock limit and its output checks, made `ScalarModel` rows
carry a failure reason, the first bounce and four fixed-temperature values, made φ\* a run option,
narrowed the BBN spline, and added the extraction and four figures of the science run. It bumped
`VERSION_LABEL` once. Its board is
[`prompts/science-readiness/IMPLEMENTATION_STATE.md`](../prompts/science-readiness/IMPLEMENTATION_STATE.md);
its plan is its [`README.md`](../prompts/science-readiness/README.md) (§0.2 records the user's
rulings, including the withdrawal of the bounce averages and the move of the fixed-`T` values to the
`ScalarModel` row); its source is
[`source/campaign_reevaluation_2026-10-01.md`](../prompts/science-readiness/source/campaign_reevaluation_2026-10-01.md).

**This supersedes**, by statement and not by edit:

- **Every earlier statement that `VERSION_LABEL` is `"2026.5.0"`**, including §4.8 point 1 and its
  evidence (`config/version.py:36`). The label is `"2026.6.0"`, still defined once, now at
  `config/version.py:43` (point 1).
- **§4.2's run list, where it implies that a datastore made before 2026.6.0 can be reused.** §4.1's
  "start from an empty database" now holds for every store made under any earlier label, and a
  store made under `"2026.5.0"` is no exception (point 1). In the same list:
  - §4.2's SM baseline stands. `tools/bbn_baseline.py` on `8efc50f` prints Yp 0.2468872958, D/H
    2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017, as §4.2 does, and it takes 7.0 s.
  - §4.2's check "Rerun with ρ_NP set to zero below 1 MeV" and the `NPCallbacks` it names do not
    exist any more (point 2).
  - §4.2's "φ\* = 5 M_P … (hard-coded, now at `main.py:815–817`)" is an option, `--phi-init-Mp`,
    with default 5.0 (point 4).
- **Any statement that BBN uses `NP_thermo_flag` or a pressure callback.** That covers §1.2's
  `NP_thermo_flag` rows, §2's prompt 04 entry (R3: `build_NP_callbacks` splining a pressure ratio
  and `jordan_Hdot_over_H2`), §4.2 item 2, §4.7's mention of `build_NP_callbacks`, and §4.3's
  magnitudes for "p = ρ/3" (the +2.92 % in Yp and +8.50 % in D/H with `N_eff` 3.71342). Those were
  true of the route they were measured on. The route is point 2, and the same constant ratio on it
  is quoted there.

**The six points.**

1. **`VERSION_LABEL = "2026.6.0"`, and the science run needs a fresh datastore file. Every store
   made before 2026.6.0 is invalid, and an old file cannot be opened by the new code.**
   - *Why.* Prompt 01 changed every `BBNData` row (the route, the output checks, the dropped
     `pressure_NP_MeV4` column). Prompts 02, 03 and 06b then added columns to the `ScalarModel`
     table, under the same label and with no migration: `failure_reason` (`77a7e0c`), four
     `first_bounce_*` (`568c23a`), and `phi_Einstein_1MeV`, `density_NP_ratio_1MeV`,
     `phi_Einstein_70keV`, `density_NP_ratio_70keV` (`489ab26`). The `ScalarModel` lookup selects these columns
     in its `build`, whether or not samples are populated (`Datastore/SQL/ObjectFactories/ScalarModel.py`),
     so an old table is missing columns the new code reads.
   - *Evidence.*
     - `grep -rn "VERSION_LABEL =" --include='*.py' .` outside `venv/`, `thirdparty/` and
       `claude-context/` finds one line: `config/version.py:43:VERSION_LABEL = "2026.6.0"`.
     - `git log 6aaa706..HEAD -- config/version.py` finds one commit, `1bc8977` (prompt 01). One
       bump; prompts 02–08 landed under it.
     - `grep -rniE "alter table|migrat" Datastore/ --include="*.py"` finds nothing: the datastore
       has no migration.
     - The round trips run against a temporary SQLite store of the new schema:
       `Datastore.tests.test_scalarmodel_failure_reason`, `…test_first_bounce_round_trip`,
       `…test_fixed_T_values_round_trip` (all OK, below).
   - *Reasoned, not run.* Opening an old file with the new code was not tried; that it fails on a
     missing column is read from the column list above. Do not rely on any other outcome.
2. **The BBN route.** The scalar field reaches PRyMordial through the expansion rate alone.
   - *Route.* `PRyM_init.NP_hubble_flag` (`PRyM/PRyM_init.py:80`) adds ρ_NP to `Hubble` and
     nowhere else (`PRyM/PRyM_main.py:142` is the only read). `_configure_PRyMordial` sets
     `NP_thermo_flag = False` (`ComputeTargets/BBNData.py:245`) and checks it
     (`BBNData.py:260`). The thermodynamic solve integrates (T_γ, T_ν) only: `test_bbn_solver_failures (f)`
     printed `solve_ivp y0 lengths [2, 1, 2, 8, 8]; rho_NP callers {'Hubble': 1930}`. `p_NP`,
     `T_NP`, `dρ_NP/dT`, `jordan_Hdot_over_H2` and `Tstart_NP` are gone:
     `grep -rn "pressure_NP\|P_NP\|drho_NP_dT\|jordan_Hdot_over_H2\|Tstart_NP" ComputeTargets/
     Datastore/ plot_ScalarModel.py main.py tools/` prints nothing. The `cham03` patch is reverted.
   - *`PRYM_VERSION`* is `"bf24c3d+ri02+sr01"` (`ComputeTargets/BBNData.py:44`; `test_bbn_solver_failures (e)`).
   - *The wall-clock limit.* `PRyMclass(…, wall_clock_limit=None)`. The default for production
     solves is `DEFAULT_BBN_WALL_CLOCK_LIMIT = 600.0` s (`BBNData.py:49`), set from
     `--bbn-wall-clock-limit SECS` on `main.py` (`config/argument_parser.py:279`; 0 disables it).
     `compute_SM_baseline` has no limit. A solve that exceeds it is a failure row, and
     `--retry-failed-bbn` retries it. `test_bbn_solver_failures (g)` printed
     `PRyMordial: PRyMWallClockLimitError: wall-clock limit of 0.001 s exceeded in stage
     'thermodynamics (no NP)'`. The driver with `--wall-clock-limit 0.001` at β = 2, M = 0.5
     printed the same failure at 0.4 s of BBN wall.
   - *The output checks.* A successful return is stored only if all four abundances are finite,
     0 < Yp < 0.5, and D/H, ³He/H, ⁷Li/H > 0. `test_bbn_solver_failures (h)` printed `PRyMordial
     output: Yp_BBN=0.7 is outside (0, 0.5)`, `… DOverH=nan is not finite` and `… Li7OverH=0 is
     not positive`. These classify our failures; they are not accuracy bounds.
   - *The spline window* is [0.2 keV, 100 MeV], so a history must reach 20 eV
     (`T_Jordan_stop ≤ 0.1 × 0.2 keV`). `test_bbn_spline_floor (a)` checks that `--T-stop-GeV 1e-8`
     passes the pre-check and `1e-7` fails it with the reason naming `20 eV`.
     `test_bbn_spline_floor (b)` printed `calls=1944 lowest positive T = 0.3628 keV (floor 0.2
     keV)`: PRyMordial never queries below the floor, and the domain guard did not fire on any
     roster history (point 5).
   - *The same constant ratio, on this route.* ρ_NP = 0.08 ρ_SM through the patched route
     (`test_network_flag (b)`): small network Yp 0.2536690816, D/H ×10⁵ 2.6481673; **full
     network Yp 0.2536754614 (+2.749 % against §4.2's baseline), D/H 2.648809882 (+7.577 %)**. §4.3's
     +8.50 % was the old route, in which the new-physics fluid shared the e± entropy; the
     difference is that sharing, which a scalar field does not do (README §6.1).
3. **What a `ScalarModel` row now carries.** Each is read without loading samples
   (`_do_not_populate` stays on in `plot_by_beta.py`: four literals, as before).
   - *The failure reason* (`failure_reason String(256)`, nullable; `ScalarModel.failure_reason`).
     A failure row stores why it failed, truncated to 256; `main.py` prints this run's failures
     grouped by first clause, and `plot_by_beta.py` reports models dropped for it. The step budget
     at 50 gives `step budget exhausted: integrate_scalar_history (reason-test) took 51 accepted
     steps (budget 50) at N=4.325263426, T_J=269.15 GeV, with 0 reflection(s)`
     (`test_scalarmodel_failure_reason`). It supersedes §4.8 point 4's "the reason is printed and
     not stored".
   - *The first bounce* (`first_bounce_N`, `first_bounce_log_T_Jordan`, `first_bounce_phi_Einstein`,
     `first_bounce_reflected`; `ScalarModel.first_bounce`, `None` when there was none). It is the
     root of π on the first accepted step whose interpolant has π(t_k) < 0 < π(t_{k+1}), or the
     first elastic reflection if that comes earlier. It exists because the sample-based detector
     lands at 742.79 MeV where the dense output gives 746.69 MeV (β = 2, M = 10⁻³), and at
     0.39 MeV where it gives 420.76 MeV (β = 1.6, M = 10⁻⁵) (log 03). The roster's values are in point 5.
   - *The fixed-temperature values* (`phi_Einstein_1MeV`, `density_NP_ratio_1MeV`,
     `phi_Einstein_70keV`, `density_NP_ratio_70keV`; `ScalarModel.fixed_T_values`). φ and
     ρ_NP/ρ_R,J at the first crossing of T_J = 1 MeV and 70 keV on the dense output, with the ratio
     built as `compute_BBN_data` builds it. φ is in M_P in the column. A pair is `None` where the
     temperature is not reached, and all four are NULL on a failure row. Each roster history
     crosses each temperature once (the driver's `crossings=1`, twenty times).
   - *There are no cell means.* Prompt 05 built bounce averages of `H_J²` and φ, measured them
     and withdrew them by the user's ruling. **BBN reads the point `H_J`.** Log 05's finding is
     that in PRyMordial's window the z grid *resolves* the bounces: the median is 0 half-periods
     per sample cell from 100 MeV down to about 100 eV (β = 1.6 and 2 at M = 10⁻⁵, β = 2 at
     M = 0.5), and aliasing begins only below that. At β = 1.6, M = 10⁻⁵ only 6 of 130 cells in
     [0.3, 1) keV and 2 of 120 in [1, 3) keV hold a sign change of π. The sub-3-keV "noise" in the
     ratio is a resolved sawtooth whose ten largest steps carry 95 % and 99 % of the rms². The cell
     means cut the rms step only to 0.71–0.84×, biased `H_J²` by +5.65e-5, moved D/H at β = 2,
     M = 0.5 by 1.57e-3, and made β = 2, M = 10⁻⁵ fail in PRyMordial. This does not
     contradict §4.9, whose table is the stored samples between 1 keV and 0.1 eV (a median over a
     window whose cold end dominates) and stands; it must not be read as a statement about
     PRyMordial's window above about 100 eV. The roster's point windows are in point 5.
4. **The options.**
   - `--phi-init-Mp` (default 5.0). `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` read it,
     with no `5.0 * units.PlanckMass` literal left (`test_initial_field_option (b)`). The driver at
     φ\* = 2, β = 2, M = 0.5 gives 41 070 RHS, 4 603 steps, first bounce N = 14.476112028 at
     659.393816 MeV, Yp 0.2487467993, D/H ×10⁵ 2.584485137.
   - **The super-Planckian warning is a warning only.** A coupling with `ln Ω(φ*) + ln T* > ln M_P`
     is printed by `main.py` before step 1 and computed and stored like any other: φ\* = 5 selects
     {7, 25, 40} of {1, 6, 7, 25, 40}, φ\* = 1 selects {40}, with `ln(M_P/T*) = 32.433`
     (`test_initial_field_option (a)`, (d): the list comes back unchanged). With `exponential.yaml`
     (β 0.1–25) and φ\* = 5 it warns about 93 of 125 couplings (log 04).
   - `--bbn-wall-clock-limit SECS` (point 2) and `--band-half-width` (default 0.025 in β;
     `config/argument_parser.py:137`; `test_extraction (f)`). The figures and `histories.csv` come
     from `plot_by_beta.py --database <store.db> --output <dir>`, which was checked by `ast` and has
     **not been run against a store** (log 07; point 6).
5. **The roster's figures.** The driver `tools/history_and_bbn.py β M`, full network, on `8efc50f`,
   one history at a time with nothing else of the campaign running (the machine's load average was 6–9, from
   a source not identified; the history walls equal log 06b's within 7 % for the three histories
   they share),
   ten histories, each completing with BBN inside the output checks. Baseline: `tools/bbn_baseline.py`, Yp 0.2468872958, D/H ×10⁵ 2.462251065 (7.0 s).

   | β | M | RHS | accepted steps | history wall | first bounce `N` | `T_J` (MeV) | BBN wall |
   |---|---|---|---|---|---|---|---|
   | 1.2 | 0.5 | 24 193 | 2 728 | 0.9 s | 17.662454825 | 231.069537 | 7.2 s |
   | 1.6 | 0.5 | 31 492 | 3 364 | 1.2 s | 18.967065903 | 420.726607 | 7.2 s |
   | 2.0 | 0.5 | 40 580 | 4 469 | 1.5 s | 20.343026853 | 746.634744 | 7.9 s |
   | 3.0 | 0.5 | 57 526 | 6 120 | 2.1 s | 24.483798229 | 1682.865313 | 7.9 s |
   | 1.2 | 10⁻³ | 254 491 | 26 458 | 6.5 s | 17.667931109 | 231.107524 | 7.9 s |
   | 1.6 | 10⁻³ | 155 961 | 16 275 | 4.6 s | 18.974419125 | 420.758090 | 8.3 s |
   | 2.0 | 10⁻³ | 271 783 | 27 979 | 7.4 s | 20.352082230 | 746.686275 | 9.0 s |
   | 3.0 | 10⁻³ | 327 046 | 34 342 | 9.9 s | 24.498974358 | 1680.011085 | 9.0 s |
   | 1.6 | 10⁻⁵ | 1 445 132 | 137 137 | 35.2 s | 18.974433718 | 420.758153 | 8.3 s |
   | 2.0 | 10⁻⁵ | 1 679 987 | 162 676 | 40.8 s | 20.352100202 | 746.686377 | 8.1 s |

   All ten: 0 reflections, no wall-clock limit reached, the first bounce not reflected. RHS and
   steps at β = 2 (M = 0.5, 10⁻³, 10⁻⁵), β = 1.2 and 3.0 (M = 0.5), β = 3.0 (M = 10⁻³) and
   β = 1.2 (M = 10⁻³) equal §4.8 and §4.9's; β = 1.6 at M = 10⁻⁵ equals README §6.1's.

   | β | M | Yp | ΔYp | D/H ×10⁵ | ΔD/H | source's ΔD/H (§2) |
   |---|---|---|---|---|---|---|
   | 1.2 | 0.5 | 0.2582949607 | +4.621 % | 2.649704815 | +7.613 % | 7.5 % |
   | 1.6 | 0.5 | 0.2490967227 | +0.895 % | 2.53373365 | +2.903 % | 2.8 % |
   | 2.0 | 0.5 | 0.249229266 | +0.949 % | 2.560889654 | +4.006 % | 4.0 % |
   | 3.0 | 0.5 | 0.2509606949 | +1.650 % | 2.603108857 | +5.721 % | 5.8 % |
   | 1.2 | 10⁻³ | 0.2567571266 | +3.998 % | 2.599025939 | +5.555 % | 5.79 % |
   | 1.6 | 10⁻³ | 0.2468868563 | −0.000 % | 2.459878815 | −0.096 % | −0.03 % |
   | 2.0 | 10⁻³ | 0.2467606164 | −0.051 % | 2.463862263 | +0.065 % | 0.07 % |
   | 3.0 | 10⁻³ | 0.2467560634 | −0.053 % | 2.457894203 | −0.177 % | −0.02 % |
   | 1.6 | 10⁻⁵ | 0.2468788501 | −0.003 % | 2.4647705 | +0.102 % | *BBN failed* |
   | 2.0 | 10⁻⁵ | 0.2467016048 | −0.075 % | 2.46477019 | +0.102 % | −0.02 % |

   - *Against the source.* The source (§2) ran the production `compute_BBN_data._function` on the
     Hubble-only route through the old callbacks, and does not state a network. The production
     default is the full network (`test_network_flag (c)`), and our β = 2 rows at M = 0.5 and
     10⁻³ equal README §6.1's full-network "honly" rows to every printed digit. Its ΔYp at M ≤ 10⁻²
     are +3.97 to +4.02 % (β = 1.2; ours +3.998 %), ≈ 0 (β = 1.6; −0.000 %), −0.10 to −0.07 %
     (β = 2.0; ours −0.051 % and −0.075 %) and −0.01 to +0.04 % (β = 3.0; ours −0.053 %). The
     ΔD/H agree to within 0.24 percentage points, as the source's own §2 says of the pointwise
     differences (0.1–0.3 %); the largest are β = 3.0 at M = 10⁻³ (−0.177 against −0.02) and
     β = 1.2 at M = 10⁻³ (+5.555 against +5.79). The source's β = 2, M = 0.5 D/H of 2.5597 against
     ours 2.560889654 differs by 4.6e-4 relative, which is inside PRyMordial's response to
     ulp-level changes in the input (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`).
     These are recorded as measurements, not bounds. **The source's one PRyMordial failure,
     β = 1.6, M = 10⁻⁵, did not reproduce here, nor at β = 2.0, M = 10⁻⁵**: both complete.
   - *The first bounce.* At β = 2 it is 746.63 MeV at M = 0.5 and 746.69 MeV at 10⁻³ and 10⁻⁵, as
     §4.8–§4.9. At β = 3, M = 0.5 the dense-output root is `N` = 24.483798, against §4.8's
     24.48381 (1.2e-5; §4.8 itself cautions that an accepted-step `N` depends on the step's width);
     at β = 3, M = 10⁻³ it is 24.498974, as §4.8.
   - *The ratio windows,* ρ_NP/ρ_R,J from the stored samples as `compute_BBN_data` computes it,
     **point values, as BBN reads them** (median / rms step between samples; no averaged window
     exists):

     | β | M | [0.3, 1) keV | [1, 3) keV | [3, 10) keV | [10, 100) keV |
     |---|---|---|---|---|---|
     | 1.2 | 0.5 | 0.04615 / 0.0001356 | 0.05523 / 4.243e-05 | 0.0581 / 1.364e-05 | 0.05921 / 0.001702 |
     | 1.6 | 0.5 | 0.04431 / 5.013e-05 | 0.04238 / 5.955e-05 | 0.03695 / 1.734e-05 | 0.03747 / 0.001671 |
     | 2.0 | 0.5 | 0.05748 / 0.0002526 | 0.04666 / 0.0001986 | 0.05301 / 3.784e-05 | 0.05571 / 0.006658 |
     | 3.0 | 0.5 | 0.08466 / 6.302e-05 | 0.07739 / 5.636e-05 | 0.07138 / 4.329e-05 | 0.06462 / 0.00886 |
     | 1.2 | 10⁻³ | 0.02595 / 0.0001334 | 0.03488 / 4.174e-05 | 0.03771 / 1.341e-05 | 0.03879 / 0.001663 |
     | 1.6 | 10⁻³ | 0.0001158 / 0.0004092 | 0.0001394 / 0.0002707 | −0.0002198 / 0.0002084 | 0.00038 / 0.007167 |
     | 2.0 | 10⁻³ | 0.001347 / 0.002129 | −0.001016 / 0.001586 | 0.005897 / 3.634e-05 | 0.008126 / 0.01604 |
     | 3.0 | 10⁻³ | −0.0002623 / 0.003212 | 0.001065 / 0.001777 | 0.001387 / 8.152e-05 | 0.006542 / 0.02187 |
     | 1.6 | 10⁻⁵ | −0.0004114 / 0.001253 | −0.0005007 / 0.0007573 | 0.002184 / 2.316e-05 | 0.00377 / 0.007134 |
     | 2.0 | 10⁻⁵ | 0.001831 / 0.002238 | −0.001186 / 0.001652 | 0.006159 / 3.637e-05 | 0.008392 / 0.01604 |

     The β = 2 rows at M = 0.5, 10⁻³ and 10⁻⁵ and the β = 1.6, M = 10⁻⁵ row equal log 05's
     printed figures to every digit, as log 01's do for the first two.
   - *Fixed-temperature values* (the driver's `fixed_T` line) for the same ten histories are in
     log 09; for the three histories of log 06b they equal its figures to every printed digit (for example β = 2, M = 0.5: φ = 1.138197048e-02 and
     ρ_NP/ρ_R,J = −4.810565953e-02 at 1 MeV).
6. **What is still open,** by name:
   - **Do not report `AdiabaticHistory` max |Q| for M ≲ 10⁻³** until
     `[post-adiabatic-Q-reads-aliased-late-samples]` (`integrator-remediation`) is settled.
     `[00-stored-samples-alias-the-rebounds]` (`integrator-remediation`) stays open for its
     adiabatic half only: it has no BBN half in PRyMordial's window (point 3).
   - `[00-settling-at-physical-M-needs-a-parked-tracking-model]` (`integrator-remediation`): the
     parked-tracking model does not exist, and a physical-`M` history with β ≥ 1.2 still ends in a
     step-budget failure row, now with its reason stored.
   - **The physical-`M` cross-check is now possible** with `--T-stop-GeV 1e-8` (a history stopped at
     10 eV passes the 20 eV pre-check). It costs about 24 minutes per history (§4.8) and is the
     user's to run.
   - `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]` (`science-readiness`): the cubic
     spline through the ratio's resolved bounce jumps may overshoot between samples, unmeasured.
     Until it is measured, a PRyMordial failure on point input cannot be attributed to the true H
     rather than to our interpolation.
   - `[05-the-value-factory-compares-stored-phi-against-pi]` (`science-readiness`): a one-word
     fix, with nothing calling the path today.
   - **`plot_by_beta.py` has not been run against a store.** Its figures and `histories.csv` are
     built from synthetic records in tests and its wiring is checked by `ast` and by reading. The
     warning for a super-Planckian start is printed by `main.py` only, which has not been run
     either (README §0.5).
   - PRyMordial's response to ulp-level input changes
     (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`, `review-remediation`): β-to-β
     differences in D/H below about 7e-4 relative are not resolved (§4.3).
   - The rest of §4.8 point 5 and of `OPEN_ISSUES.md` §1.1–§1.6 is as it was.

**Verification table** (the campaign's README §6.2–§6.8, re-run on `8efc50f` by prompt 09; the rows
and their witnesses are in
[`logs/09-close-out-verification.md`](../prompts/science-readiness/logs/09-close-out-verification.md)).
Suites: `CosmologyModels/tests` 18 OK (68.8 s), `ComputeTargets/tests` 103 OK (85.0 s),
`Datastore/tests` 31 OK (1.9 s), against 18, 67 and 17 at `6aaa706`. Every row is at or better than
its target, and none regressed since its prompt's log.

**To reproduce** from the repository root (the three suites take about three minutes; each history
of the roster takes 10–50 s):

```bash
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . && grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/" && git diff --stat 6aaa706..8efc50f
./venv/bin/python tools/bbn_baseline.py
./venv/bin/python tools/history_and_bbn.py 2 0.5      # and β M for each roster row, one at a time
```

---

## 5. Reproduce

From the repository root: the commands in the provenance table, about 75 s in all, most of it the
`ComputeTargets` suite and the four PRyMordial fixture solves. For the scope check, run
`git diff --stat f5896bb..01e5975`.
