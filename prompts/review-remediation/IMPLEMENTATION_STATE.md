# Review remediation campaign — implementation state

**Last updated:** 2026-09-29 · **Status: IN PROGRESS — 6 prompts written, 4 landed.** The campaign was
opened on 2026-09-29 from the code audit
[`.documents/audit-2026-09-29/README.md`](../../.documents/audit-2026-09-29/README.md) of the paper
review `Paper1_review.tex`. It fixes the audit's items 1–4: the ln 10 error in the Jordan
temperature law (since commit `5962833`, 2026-01-19), the singular passenger equation in the
vendored PRyMordial, the arcsinh representation in the BBN interface, and the unpinned kicking
function. Items 5–9 of the audit are recorded in §3 as seeded issues and are out of scope.
**Amended 2026-09-29.** Prompt 01's first dispatch stopped on its case 2 and committed nothing.
It found R5, the 10 keV join (audit §11). The campaign now characterises R5 in 01 and fixes it
in 02. **Prompt 01 landed 2026-09-29** (re-dispatch): the guard is in `CosmologyModels/tests/`
and characterises R1 and R5; it opened one issue, on prompt 02's derivative target, since resolved by restating the target
(§4).
**Prompt 02 landed 2026-09-29:** R1 and R5 are fixed in the EOS class. The guard now asserts that
the law agrees with exact entropy conservation to 1e-5 down to T_CMB (measured −3.6e-8).
`VERSION_LABEL` is now `"2026.2.0"`. **Every store built under 2026.1.1 is invalid**, and the
numerical campaign starts from an empty database. Prompt 02 opened one issue (§3).
**Prompt 03 landed 2026-09-29** (re-dispatch, after the user's two decisions below). R2 is fixed:
PRyMordial's inert `dTNPdt` returns 0, and the oscillating case finishes in about 9 s instead of
more than 120 s. The patch is inert: ρ_NP ≡ 0 reproduces the no-NP run with difference 0, and the
constant 0.08 family moves by 2.8e-7. A failed BBN solve now stores a `failure_reason`, and
`plot_by_beta.py` lists the models it drops. New rows say `PRyM_version = "bf24c3d+cham03"`.
Prompt 03 opened four issues (§3). Among them, `compute_BBN_data`'s `small_network` switch has
never reached PRyMordial, so every BBN result so far used the full network.
**Prompt 04 landed 2026-09-29.** R3 is fixed. The BBN callbacks now spline ρ_NP/ρ_R,J and
p_NP/ρ_R,J and multiply back by the thermodynamic ρ_SM(T_J); the asinh path is gone. A
non-monotonic T_J is refused rather than sorted, and the Ω″ term is Ω″π². A ρ_NP ≡ 0 baseline goes
through the same PRyMordial settings (`compute_SM_baseline`, `tools/bbn_baseline.py`) and is drawn
by `plot_by_beta.py`. The end-to-end D/H target (1e-4) is met at 8.85e-5, inside a measured
PRyMordial noise band of 7e-4. Prompt 04 opened one issue and narrowed one (§3).
**Next: 05.** Target branch `review-remediation` from `f5896bb`.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py`, `ComputeTargets/BBNData.py`,
`PRyM/PRyM_main.py`, `Datastore/SQL/ObjectFactories/BBNData.py`, `plot_by_beta.py`, `main.py`
(version label only), and the new `CosmologyModels/tests/`, `ComputeTargets/tests/`, `tools/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.1

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing here. Where the two disagree,
> this board is right. See `CLAUDE.md`.

### Decisions

- **2026-09-29, the user.** R5 is to be fixed in this campaign, not deferred. Prompt 01's case 2
  is amended to characterise it. Prompt 02 sets `LOW_T_G_S_STAR` and `LOW_T_GSTAR` to the fit's
  own limits, 3.931 and 3.383 (README §2 (j)), under the version bump it already makes.

- **2026-09-29, the user.** Prompt 02's derivative targets become an absolute norm on
  d ln g_s/d ln T, ≤ 1e-6 (option (b)), replacing "≤ 1e-6 relative". See §4,
  `[01-derivative-agreement-target-1e-6-is-missed-at-the-low-T-end]`.

- **2026-09-29, the user.** Prompt 03's test (c) compares against the fixture's own unpatched
  values, not README §2 (f)'s five-figure 0.25409 / 2.6715 ("Test (c): use option C"). The
  fixture's ρ_SM uses the `SaikawaShirai_EOS_spline` class. The pins are Yp 0.2540937879 and
  D/H ×10⁵ 2.671500711, measured on `47c50ae`, and the tolerance stays 1e-5. The first dispatch
  stopped because the literal target missed by 1.49e-5 in Yp on the unpatched tree too. See log 03,
  Deviations.

- **2026-09-29, the user.** `sqla_BBNDataFactory.build()` asked for `failure=True` returns the
  newest failed row rather than raising `MultipleResultsFound` ("build(): yes, pick the newest
  row"). See log 03.

- **2026-09-29, the user.** The `small_network` flag bug is opened as a board issue and not fixed
  in prompt 03 (§3, `[03-small-network-flag-is-never-read-by-prymordial]`).

- **2026-09-29, the user ("Option 1").** Prompt 05's peak temperatures are restated as the peaks
  of the spline `Xav_EOS_spline.w` actually evaluates, not the table rows with the largest Σ.
  README §6.4's T_J column (0.1585 MeV, 0.1778 GeV, 56.23 GeV) and prompt 05 §1 case 1 took the
  argmax over the CSV's own rows, about 20 per decade. The spline peaks between rows. The EW peak
  is at 53.25 GeV, −5.3 % from 56.23 GeV, outside the ±5 % tolerance. The test would pass only
  by grid luck (−4.87 % at exactly 200 points per decade).
  - **New T_J targets, still ±5 %:** e⁺e⁻ **0.1605 MeV**, QCD **0.1819 GeV**, EW **53.25 GeV**.
  - **Σ targets and tolerance (±1e-3) unchanged:** 0.1007, 0.3138, 0.03733. The spline gives
    0.10073, 0.31453 and 0.037436.
  - **Provenance.** The orchestrator's scratch probe on `eba4473`, argmax of 1 − 3
    `Xav_EOS_spline.w` on a 5000-points-per-decade log grid over the prompt's three windows. The
    table-row values reproduce the audit exactly (pandas read, same commit).
  - **In the note.** The paper-facing note gives both: the spline peak, and the table-row maximum
    as what the CSV itself contains.
  - **Precedence.** This supersedes §6.4's T_J column and prompt 05 §1 case 1 where they differ.

None pending otherwise. Decisions the prompts may surface (each is a stop-and-ask in its
prompt's §7): whether to divide by ln 10 or rebuild the EOS grid in ln T (02, either is allowed);
whether ρ_SM(T) for the ratio interface is the thermodynamic formula or a spline of the stored
`log_rhorad_Jordan` (04, thermodynamic preferred); what `failure_reason` may hold (03, ≤ 256
characters).

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The temperature-law harness](01-temperature-law-harness.md) | **R1** (guard), **R5** (characterised) | Opus | ✍️ 2026-09-29, amended 2026-09-29 | ✅ 2026-09-29 | `ec3a994` | [01](logs/01-temperature-law-harness.md) |
| 02 | [Fix the entropy derivative](02-fix-the-entropy-derivative.md) | **R1**, **R5** (fix) | Opus | ✍️ 2026-09-29, amended 2026-09-29 | ✅ 2026-09-29 | "Fix the ln 10 in the entropy derivative and the 10 keV join" | [02](logs/02-fix-the-entropy-derivative.md) |
| 03 | [PRyMordial's passenger equation and failure reasons](03-prymordial-passenger-and-failure-reasons.md) | **R2** | Opus | ✍️ 2026-09-29 | ✅ 2026-09-29 | "Patch PRyMordial's inert T_NP equation and record BBN failures" | [03](logs/03-prymordial-passenger-and-failure-reasons.md) |
| 04 | [Ratio splines and a baseline](04-ratio-splines-and-a-baseline.md) | **R3** | Opus | ✍️ 2026-09-29 | ✅ 2026-09-29 | "Spline the BBN new-physics ratios and add an SM baseline" | [04](logs/04-ratio-splines-and-a-baseline.md) |
| 05 | [Pin the kicking function; EOS hygiene](05-kicking-function-and-eos-hygiene.md) | **R4** (pins) | Opus | ✍️ 2026-09-29 | ⬜ | — | — |
| 06 | [Close-out verification and handover](06-close-out-verification.md) | **R4** (verification), handover | Opus | ✍️ 2026-09-29 | ⬜ | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| R1 | **DEFECT, critical** | `dG_s_dlogT` / `dG_rho_dlogT` in the spline EOS class return d/d log10 T; the temperature law consumes them as d/d ln T. N to T_CMB 41.497 vs exact 40.075; stored ρ_R,J at 1 MeV is 0.022× thermodynamic. | 01, 02 | ✅ done (02): both derivatives return d g/d ln T; N to T_CMB 40.0754 vs exact 40.0754 (−3.6e-8). **Every store built under 2026.1.1 is invalid.** |
| R2 | **DEFECT, high** | PRyMordial's inert `dTNPdt` is singular where ρ_NP′ = 0; oscillating ρ_NP stalls LSODA (> 600 s vs 9 s); failures swallowed and dropped silently. | 03 | ✅ done (03): `dTNPdt` returns 0 (marker comment in `PRyM/PRyM_main.py`); oscillating case 8–9 s (was > 120 s); ρ_NP ≡ 0 True vs False difference 0, `RuntimeWarning`s 916 → 0; constant 0.08 unchanged to 2.8e-7; `failure_reason` stored and printed; `PRyM_version` `"bf24c3d+cham03"` |
| R3 | **DEFECT, low** | asinh representation of ρ_NP, p_NP; sort hides non-monotonic T_J; `Ω″ π` should be `Ω″ π²`; no SM baseline through the same path. | 04 | ✅ done (04): `build_NP_callbacks` splines the ratios (constant 5.2e-17, oscillating 9.75e-9 / derivative 7.40e-7, against asinh 7.85e-10 / 3.30e-8 / 2.96e-6); non-monotonic T_J raises `ComputationFailureError`; `Ω″ π²` (`jordan_Hdot_over_H2`); `compute_SM_baseline` + `tools/bbn_baseline.py` + `plot_by_beta.py --no-baseline`. **ρ_SM choice: thermodynamic** (π²/30) g_ρ T⁴ with exact derivative; the stored-spline alternative differs by −2.12e-3 to −7.8e-4 on [10 keV, 10 MeV] and by 5.9e-5 (Yp) / 7.7e-4 (D/H) end to end. End-to-end D/H 8.85e-5 vs target 1e-4 |
| R4 | **DOCUMENTATION** | Kicking-function peaks and table–g consistency unpinned; paper describes dead code; two derivative implementations disagree. | 05, 06 | ⬜ planned |
| R5 | **DEFECT, minor** | Below 10 keV `G_s` and `G_rho` return 3.94 and 3.38, not the fit's own limits 3.931 and 3.383. The corrected law is off by +1.465e-4 e-folds at 10 keV and T_CMB, and the ρ_R witness at 10 keV reads 1.00258 instead of 0.99922. Found by prompt 01's first dispatch, 2026-09-29; audit §11. | 01, 02 | ✅ done (02): limits 3.931 / 3.383; residual at 10 keV and T_CMB −3.6e-8; ρ_R witness at 10 keV 0.99922 |

---

## 3. Active and unresolved issues

Seven were **seeded at planning on 2026-09-29** from the audit. The two issues opened on
2026-09-29 after that (one at the re-plan, one by prompt 01) are both resolved; see §4.
Prompt 02 opened one more, prompt 03 opened four, and prompt 04 opened one (the last six entries
below); prompt 04 also narrowed `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`. Measurements for the seeded issues:
`.documents/audit-2026-09-29/README.md`, section in brackets.

- **[00-adiabaticity-diagnostic-omits-the-source-response-term]** *(audit §5; review H5)* —
  `ComputeTargets/AdiabaticHistory.py:103` sets the conformal contribution to m_eff² to
  3M_P² E Ω″ R, which is zero for the exponential coupling. The response of the source to δφ,
  (Ω′)² ρ_R,E [Σ(4 − d ln(ρ_J − 3p_J)/d ln T_J) + f_m], is missing. Harmless where V″ dominates
  (rebounds, stabilised phase); wrong as a general statement, and the paper's §NumericalSection
  repeats it. **Next step:** a prompt in the numerical campaign, since it changes stored
  `AdiabaticHistory` rows; needs dΣ/d ln T, which the w-spline can provide. **Out of scope here.**
- **[00-initial-field-value-is-hard-coded-and-unchecked]** *(audit §6; review H8)* —
  `main.py:814` fixes φ* = 5 M_P and π* = 0; nothing checks A*T* ≲ M_P, so `exponential.yaml`
  runs β up to 25 with A* = e¹²⁵. Numerically harmless (everything is in logs); physically
  super-Planckian for β ≳ 6.5. **Next step:** make φ* a CLI/yaml parameter carried in the store
  tags, and warn or refuse when βφ*/M_P > ln(M_P/T*). **Out of scope here.**
- **[00-hard-reflection-count-is-stored-but-never-reported]** *(audit §8; review N1)* —
  `ScalarModel` rows carry `hard_reflections`; no plotting script reads it. **Next step:** print
  it per model in `plot_ScalarModel.py` and in the survey summary. **Out of scope here.**
- **[00-kicking-function-table-has-no-provenance-in-the-repository]** *(audit §4)* —
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` (189 rows, 10 keV–25 TeV) was added in commit
  `1759515` without the script that built it; the treatment of g_ρ after neutrino decoupling that
  gives the e⁺e⁻ peak 0.1007 rather than 0.075 is therefore undocumented. Prompt 05 records what
  the file itself establishes; it does not invent provenance. **Next step:** the authors supply the
  construction and it goes under `.documents/` or beside the CSV.
- **[00-datastore-lookups-ignore-the-version-column]** *(README §2 (e))* — `ScalarModel`,
  `AdiabaticHistory`, `BBNData` rows carry `version` but `build()` never filters on it
  (`Datastore/SQL/ObjectFactories/ScalarModel.py:246–259`, `BBNData.py:138–144`). A corrected
  `main.py` opening a `2026.1.1` store is handed stale rows. Prompt 02 bumps `VERSION_LABEL` and
  states the fresh-database rule; keying on the version is the datastore layer's. **Out of scope
  here.**
- **[00-two-files-are-not-black-clean]** — `Datastore/SQL/ObjectFactories/base.py` and
  `CosmologyModels/LambdaCDM/Planck.py` at `f5896bb` (`black --check`, 2026-09-29). Housekeeping;
  reformat in a commit of their own, never inside a prompt's diff.
- **[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]** *(audit §2)* —
  `BBNData.py:47` tabulates from 100 MeV down to 0.1 eV; PRyMordial evaluates the callbacks from
  10 MeV to about 0.3 keV (`t_end = 10⁷ s`). The low end includes the matter-era re-delivery
  oscillations of the review's H1 (vii). Harmless at 250 knots per decade; a narrower domain
  would need the `T_Jordan_stop` pre-check (`BBNData.py:77`) adjusted with it. **Not changed by
  prompt 04**, which keeps the domain and records the knot count it actually used.
- **[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]** *(log 02, observation 1)*.
  - **What.** Three comments in `CosmologyModels/GenericEOS/` are wrong. None of them was in prompt
    02's allowed lines:
    - `GenericEOS.py:68–75`: the abstract `dG_s_dlogT` docstring says "d(g_S)/dT"; the method
      returns d g_S/d ln T, as its sibling `dG_rho_dlogT` states.
    - `SaikawaShirai_EOS_jax_autodiff.py:204, 221`: "units of the output will be 1/GeV"; the
      output is dimensionless.
    - `SaikawaShirai_common.py`: the comment above `SAIKAWA_SHIRAI_T_LO` says the cut is at
      600 keV; it is at 10 keV.
  - **Impact.** None on any number; a reader can be misled about the convention R1 was about.
  - **Next step.** A comment-only commit, in whichever prompt next owns these files (prompt 05's
    EOS hygiene, if its scope allows), or one of its own.

- **[03-small-network-flag-is-never-read-by-prymordial]** *(log 03, observation 1; opened at the
  user's instruction, 2026-09-29)*.
  - **What.** `ComputeTargets/BBNData.py:324` (`:306` before prompt 03) sets
    `PRyMini.small_network_flag = small_network`. PRyMordial never reads that name. It reads
    `smallnet_flag` (`PRyM/PRyM_init.py:111`, default `False`) at `PRyM_main.py:599, 884, 982, 989,
    1161, 1167`. `small_network=True`, which `main.py` passes, therefore has no effect: every
    production BBN solve ran the **full** network. The `small_network` value stored on `BBNData`
    rows, and printed by `add_BBN_info_labels`, is a label that does not describe the run.
  - **Also affected.** README §2 (f)'s figures ("small network") and prompt 03's fixture
    (`run_prym(..., small_network=True)`, which copies the flag faithfully) also ran the full
    network.
  - **Measured** on `47c50ae` with a scratch probe. Constant family 0.08 ρ_SM, raw-fit g_ρ, 3.38
    below 10 keV:
    - `smallnet_flag = True` set directly: Yp 0.25408633, D/H ×10⁵ 2.6709992, ³He/H ×10⁵ 1.07231,
      ⁷Li/H ×10¹⁰ 5.14143, 5.9 s;
    - as `compute_BBN_data` sets it, that is the full network: 0.25408672, 2.6713932, 1.07201,
      5.0910, 9.1 s.
  - **Impact.** Yp and D/H move by 1.5e-6 and 1.5e-4 relative, and ⁷Li/H by 1 %. The small network
    is documented as unreliable for ⁷Li, so the full network is arguably the better one. The defect
    is that the flag and the stored label say otherwise.
  - **Next step.** Decide which network the pipeline should use, then either set `smallnet_flag` or
    remove the switch and the column's claim. It changes physical output, so it needs a version
    decision. **Not fixed by prompt 03** (out of scope).
- **[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]** *(log 03, observation 2)*.
  - **What.** PRyMordial's abundances respond to changes in the NP callbacks far below any
    physical scale.
  - **Measured** on `47c50ae`, raw-fit constant family, ρ_NP × (1 + ε):
    - ε = +1e-9 moves Yp by +1.8e-5 and D/H by +5e-6;
    - ε = −1e-8 moves D/H by +3.6e-4;
    - the response is not monotonic in ε.
  - **More measurements.** Two constructions of the same ρ_NP agree to 2.2e-10 in ρ and 1.6e-8 in
    dρ/dT, yet differ by 2.8e-5 in Yp. Re-associating one product (r · ρ_SM(T) against
    r · (π²/30) · g · T⁴) moves D/H by 1.0e-4. The patched tree behaves the same.
  - **Impact.** Any test that compares PRyMordial output across two constructions of the "same"
    ρ_NP is limited to ~1e-4 in D/H. **This bears on prompt 04's end-to-end target** (Yp, D/H to
    1e-4 through the ratio callbacks).
  - **Next step.** Prompt 04 measures the spread before relying on its 1e-4 target; the planner
    decides whether that target stands. Whether PRyMordial's internal tolerances should be
    tightened is a question for the numerical campaign.
  - **Narrowed 2026-09-29 by prompt 04** (log 04, Verification; scratch `probe_e2e.py` on
    `ec206a3` plus prompt 04's diff). The constant ratio 0.08 went through the new
    `build_NP_callbacks` into `run_prym`, with ρ_NP × (1 + ε).
    - ε = 0 gives D/H 8.85e-5 from prompt 03's value and Yp 1.9e-6, so the 1e-4 target passes.
    - ε = ±1e-9 and +1e-8 give D/H within 4.6e-6–9.4e-5.
    - ε = −1e-8 gives **7.1e-4**.
    - The oscillating family through the new callbacks differs from prompt 03's fixture by 6.1e-4
      in D/H.
    - So the passing end-to-end test (`test_bbn_callbacks` (h)) sits inside PRyMordial's noise and
      could fail on another platform with no change to the code. The target was not loosened; the
      planner's decision above is still open.
- **[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]** *(log 03, observation 3)*.
  - **What.**
    - None of the eight `solve_ivp` calls in `PRyM/PRyM_main.py` checks `.status` or `.success`,
      so a failed integration returns whatever the truncated arrays give. This is a plausible
      source of the non-positive abundances `plot_by_beta.py` filters as "must represent a
      PRyMordial integration failure".
    - `compute_BBN_data` catches only `(OverflowError, ValueError, ComputationFailureError)`. Any
      other exception leaves the Ray task with no failure row.
  - **Next step.** Check `sol.success` after each background solve (a vendored patch, with a
    marker) and raise `ComputationFailureError`, so the reason is recorded. **Not in prompt 03's
    scope.**
- **[03-main-recomputes-failed-bbn-rows-on-every-run]** *(log 03, observation 4)*.
  - **What.** `main.py`'s BBN lookup (`:617–630`) uses `build()`'s default `failure=False`. A model
    whose BBN computation fails deterministically is recomputed, and a new failed row stored, on
    every run.
  - **Impact.** Wasted solves, and duplicate failed rows. Since prompt 03, `build(failure=True)`
    returns the newest, the user's decision.
  - **Next step.** Decide whether a failed row should stop recomputation, perhaps unless the
    `PRyM_version` or the `VERSION_LABEL` differs. That belongs with
    `[00-datastore-lookups-ignore-the-version-column]`.
- **[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]** *(log 04, observation 1)*.
  - **What.** `.documents/numerical-strategies.md` §7.2–7.4 describe the BBN interface as it was
    before prompt 04:
    - the asinh/sinh transform and its chain-rule derivative (§7.2);
    - `_make_spline`'s sort and the warn-only monotonicity check (§7.3);
    - the `sinh` overflow path (§7.4).

    Since prompt 04, `build_NP_callbacks` splines ρ_NP/ρ_R,J and p_NP/ρ_R,J and multiplies back by
    the thermodynamic ρ_SM; it refuses non-monotonic T_J, and there is no `sinh`. §7.1's Ḣ_J/H_J²
    description does not mention the Ω″π² correction either.
  - **Impact.** None on any number. A reader is told the code does what it no longer does.
  - **Next step.** A dated addendum to §7, additive per CLAUDE.md rule 6, in whichever prompt next
    owns `.documents/` (prompt 06's close-out, if its scope allows), or a commit of its own. **Not
    in prompt 04's files.**

---

## 4. Resolved issues

- **[00-xav-eos-w-above-the-table-returns-one-over-3.9]** *(audit §11, correcting §4)*.
  - **What.** `Xav_EOS_spline.w` returns `1.0 / 3.9` for T ≥ `_T_max` = 25 119 GeV, where 1/3
    is plainly meant; the audit's §4 had said it returns 1/3.
  - **Impact: latent.** The default `--T-init-GeV` is 20 000 (`config/argument_parser.py:13`),
    below the table's top. Any run started above 25 TeV would get Σ = 0.23 there.
  - **Next step:** a one-line fix with a test, in the prompt or campaign that next owns
    `Xav_EOS_spline`. **Not assigned; not in 02's allowed files.**
  - **Measured by prompt 01 (2026-09-29): the claim does not match the tree.** At `b9bc694` (and
    at `41b410d`) the branch returns `1.0 / 3.0`; `w(3e4 GeV)` = 0.3333333. Commit `449de62`
    (2026-01-16, "Fix typo in the implementation of Xav's equation of state") changed `1.0 / 3.9`
    to `1.0 / 3.0`. Audit §4's original statement was right and §11's correction is not. Left open
    for the board owner to close; see log 01, "Observations not acted on" 1.
  - **Withdrawn 2026-09-29 (planning correction after `ec3a994`): not a defect.** The planner
    read `Xav_EOS_spline.py` as it stood at `1759515` instead of the working tree. The audit §11
    addendum carries a dated correction. There is nothing to fix.

- **[01-derivative-agreement-target-1e-6-is-missed-at-the-low-T-end]** *(log 01, observation 2;
  README §6.1 rows 4–5)*.
  - **What.** Prompt 02's targets are `dG_s_dlogT` against a central difference of `G_s` in ln T,
    and spline against jax, each ≤ 1e-6 relative. On prompt 01's 60-point grid
    (`derivative_test_grid_GeV()`, [20 keV, 5 TeV] with 120 MeV avoided by a factor 1.5) they
    **cannot pass even with an exact ÷ln 10**. The residual ratio/ln 10 − 1 does not change
    when the derivative is rescaled.
  - **Measured** on `b9bc694` (scratch probe calling `eos_reference`):
    - against the central difference, worst −4.68e-6 at 20 keV (1 point > 1e-6);
    - against jax, worst −1.61e-5 at 27.5 keV (4 points > 1e-6: 20, 27.5 and 37.9 keV, 11.5 GeV).
    - Everywhere else the two agree to ≤ 3.7e-7 and ≤ 6.7e-7.
  - **Cause.** Relative error where the derivative is tiny. dg_s/d log10 T is 1.4e-6 at 20 keV,
    the e± Boltzmann tail. It dips to 0.25 near 10 GeV.
  - **Impact.** None on the physics. Prompt 02's stated acceptance test fails as written.
  - **Next step:** before prompt 02 runs, the planner decides whether to measure the target on a
    sub-grid (for example T ≥ 40 keV), in a norm scaled by max |dg_s/d ln T|, or at a different
    tolerance. **Not a target change made by prompt 01.**
  - **Resolved 2026-09-29 by the user's decision (option (b)).** Both targets are restated as
    the absolute difference in the quantity the temperature law consumes: |Δ dG_s_dlogT| / G_s,
    that is |Δ(d ln g_s/d ln T)|, ≤ 1e-6 at every grid point (README §6.1; prompt 02 F3).
  - **Measured** on `ec3a994` with a scratch probe that calls `eos_reference`: the production EOS
    and the spline class with ÷ln 10 applied in memory, on `derivative_test_grid_GeV()`, central
    difference half-step 1e-4. Worst 1.26e-8 against the central difference and 2.44e-7 against
    jax, both at 180 MeV. The largest |d ln g_s/d ln T| on the grid is 1.27.
