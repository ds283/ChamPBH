# Review remediation campaign — implementation state

**Last updated:** 2026-09-29 · **Status: PLANNED — 6 prompts written, 0 landed.** The campaign was
opened on 2026-09-29 from the code audit
[`.documents/audit-2026-09-29/README.md`](../../.documents/audit-2026-09-29/README.md) of the paper
review `Paper1_review.tex`. It fixes the audit's items 1–4: the ln 10 error in the Jordan
temperature law (since commit `5962833`, 2026-01-19), the singular passenger equation in the
vendored PRyMordial, the arcsinh representation in the BBN interface, and the unpinned kicking
function. Items 5–9 of the audit are recorded in §3 as seeded issues and are out of scope.
**Amended 2026-09-29.** Prompt 01's first dispatch stopped on its case 2 and committed nothing.
It found R5, the 10 keV join (audit §11). The campaign now characterises R5 in 01 and fixes it
in 02. **Next: 01 (re-dispatch).** Target branch `review-remediation` from `f5896bb`.

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

None pending otherwise. Decisions the prompts may surface (each is a stop-and-ask in its
prompt's §7): whether to divide by ln 10 or rebuild the EOS grid in ln T (02, either is allowed);
whether ρ_SM(T) for the ratio interface is the thermodynamic formula or a spline of the stored
`log_rhorad_Jordan` (04, thermodynamic preferred); what `failure_reason` may hold (03, ≤ 256
characters).

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The temperature-law harness](01-temperature-law-harness.md) | **R1** (guard), **R5** (characterised) | Opus | ✍️ 2026-09-29, amended 2026-09-29 | ⬜ | — | — |
| 02 | [Fix the entropy derivative](02-fix-the-entropy-derivative.md) | **R1**, **R5** (fix) | Opus | ✍️ 2026-09-29, amended 2026-09-29 | ⬜ | — | — |
| 03 | [PRyMordial's passenger equation and failure reasons](03-prymordial-passenger-and-failure-reasons.md) | **R2** | Opus | ✍️ 2026-09-29 | ⬜ | — | — |
| 04 | [Ratio splines and a baseline](04-ratio-splines-and-a-baseline.md) | **R3** | Opus | ✍️ 2026-09-29 | ⬜ | — | — |
| 05 | [Pin the kicking function; EOS hygiene](05-kicking-function-and-eos-hygiene.md) | **R4** (pins) | Opus | ✍️ 2026-09-29 | ⬜ | — | — |
| 06 | [Close-out verification and handover](06-close-out-verification.md) | **R4** (verification), handover | Opus | ✍️ 2026-09-29 | ⬜ | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| R1 | **DEFECT, critical** | `dG_s_dlogT` / `dG_rho_dlogT` in the spline EOS class return d/d log10 T; the temperature law consumes them as d/d ln T. N to T_CMB 41.497 vs exact 40.075; stored ρ_R,J at 1 MeV is 0.022× thermodynamic. | 01, 02 | ⬜ planned |
| R2 | **DEFECT, high** | PRyMordial's inert `dTNPdt` is singular where ρ_NP′ = 0; oscillating ρ_NP stalls LSODA (> 600 s vs 9 s); failures swallowed and dropped silently. | 03 | ⬜ planned |
| R3 | **DEFECT, low** | asinh representation of ρ_NP, p_NP; sort hides non-monotonic T_J; `Ω″ π` should be `Ω″ π²`; no SM baseline through the same path. | 04 | ⬜ planned |
| R4 | **DOCUMENTATION** | Kicking-function peaks and table–g consistency unpinned; paper describes dead code; two derivative implementations disagree. | 05, 06 | ⬜ planned |
| R5 | **DEFECT, minor** | Below 10 keV `G_s` and `G_rho` return 3.94 and 3.38, not the fit's own limits 3.931 and 3.383. The corrected law is off by +1.465e-4 e-folds at 10 keV and T_CMB, and the ρ_R witness at 10 keV reads 1.00258 instead of 0.99922. Found by prompt 01's first dispatch, 2026-09-29; audit §11. | 01, 02 | ⬜ planned |

---

## 3. Active and unresolved issues

Seven were **seeded at planning on 2026-09-29** from the audit. One more,
`[00-xav-eos-w-above-the-table-returns-one-over-3.9]`, was **opened at the re-plan after prompt
01's first dispatch** (2026-09-29, audit §11). Measurements: `.documents/audit-2026-09-29/README.md`, section in brackets.

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

- **[00-xav-eos-w-above-the-table-returns-one-over-3.9]** *(audit §11, correcting §4)*.
  - **What.** `Xav_EOS_spline.w` returns `1.0 / 3.9` for T ≥ `_T_max` = 25 119 GeV, where 1/3
    is plainly meant; the audit's §4 had said it returns 1/3.
  - **Impact: latent.** The default `--T-init-GeV` is 20 000 (`config/argument_parser.py:13`),
    below the table's top. Any run started above 25 TeV would get Σ = 0.23 there.
  - **Next step:** a one-line fix with a test, in the prompt or campaign that next owns
    `Xav_EOS_spline`. **Not assigned; not in 02's allowed files.**

---

## 4. Resolved issues

None yet.
