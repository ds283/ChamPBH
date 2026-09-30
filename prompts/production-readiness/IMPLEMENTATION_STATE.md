# Production readiness campaign — implementation state

**Last updated:** 2026-09-30 · **Status: IN PROGRESS — 3 of 4 prompts landed (01, 02, 03).**
**`VERSION_LABEL` is `"2026.3.0"` since prompt 02: every store made before 2026.3.0 is invalid.**
**Since prompt 03 the adiabatic mass includes the source response: no `AdiabaticHistory` row made
before 2026.3.0 is comparable.**
Amended 2026-09-30
at `b0e46bc` (README header): prompt 03's A3 now removes the singular log|M²| route to Q's
numerator instead of guarding it, and two issues are opened for the authors (§3).

The campaign was opened on 2026-09-30, against `main` at `204795e`. It fixes four issues on the
closed [`review-remediation`](../review-remediation/IMPLEMENTATION_STATE.md) board, which the user
chose as the ones to clear before the production run:

- the hard-reflection count that no summary reports (P1);
- the caption that reads the count under the wrong key (P1);
- the `small_network` switch that never reaches PRyMordial (P2);
- the adiabatic mass that omits the source response (P3, review H5).

Those four issues stay on the `review-remediation` board, with an **Assigned** line naming this
campaign. When a prompt closes one, it adds a **Resolved** line there (README §5 rule 4). This
board's §3 is for issues the campaign's own prompts open.

Target branch `production-readiness` from `204795e`. `VERSION_LABEL` was `"2026.2.0"`; prompt 02
made it `"2026.3.0"` (2026-09-30), and prompt 03 lands under the same label.

**Campaign:** [`README.md`](README.md) ·
**Code:** `extract_common.py`, `plot_by_beta.py`, `ComputeTargets/ScalarModel.py` (the
`extra_data` builder only), `ComputeTargets/BBNData.py`, `main.py`, `tools/bbn_baseline.py`,
`ComputeTargets/AdiabaticHistory.py`, `CosmologyModels/GenericEOS/`, and both test packages ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.2 (assigned), §1.3 (opened here)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The same holds when a prompt closes one of the four assigned issues on the
> `review-remediation` board. The index is an index: one line per issue, pointing at the board
> that holds it. Where the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-09-30, the user: the full reaction network.** The flag is wired to PRyMordial's
  `smallnet_flag`. Production, the SM baseline and the fixtures pass `small_network=False`. The
  alternatives offered were to keep the small network (⁷Li ~1 % off, fixtures re-pinned) and to
  remove the switch.
- **2026-09-30, the user: one bump, to `VERSION_LABEL = "2026.3.0"`.** It is made in prompt 02,
  the first prompt that changes physical output. Prompt 03 lands under the same label.
  **Every store made before 2026.3.0 is invalid.**
- **2026-09-30, the planner: the H5 formula is decided by an independent reference, not by the
  audit** (README §2 (c)). The planning probe on `204795e` finds that audit §5's bracket omits a
  factor 1/(1 + ⅓ d ln g_s/d ln T_J). The reference agrees with the corrected form to 9.9e-8. The
  two forms differ in sign at the QCD peak. Prompt 03 re-derives this and stops if its own
  derivation disagrees.
- **2026-09-30, the user: amend the plan after an audit of the rest of the adiabaticity code.**
  The planner audited the code on `b0e46bc`, beyond the missing term; the user asked for the
  audit, and approved the amendment.
  - **Right as it stands:** Q, and the self, (ln Ω)″ and gravitational pieces of M²_eff.
  - **Wrong:** the route to Q's numerator. The log|M²| spline is singular at a sign change of
    M²_eff, where Q is smooth. At production sampling its error on a crossing history is 1.8 of
    max |A·C| (`planning-probes/q_sign_change_probe.py`).
  - **The amendment.** README §2 (e) is rewritten, since it had called the spike physical. Prompt
    03's A3 now requires the smooth form A·C = m(1 + Ḣ/H²) + ½ dm/dN, accurate to 1e-4 of max
    |A·C| through zero and across a bounce, and forbids a fail-closed guard. README §2 (h) records
    what the diagnostic assumes.
- **2026-09-30, the user: the small network's Yp and D/H shifts are not bounded.** Prompt 02
  measured the small network 6.2e-5 from the full one in Yp, against README §6.2's 1e-5. That
  offset is PRyMordial's, and PRyMordial never promised any level for it, so there is no contract
  to impose and no issue to open. `[02-network-shift-bounds-sit-inside-prymordial-noise]` is
  withdrawn (§4). The expected-failure test and the D/H bound are removed, and ⁷Li/H alone
  witnesses the network. The shifts stay recorded in `.documents/numerical-strategies.md` §7.5,
  to be aware of. README header and §6.2 amended.

None pending. Decisions the prompts may surface, each a stop-and-ask in its prompt:

- how prompt 03 represents dm/dN in Q's numerator: an asinh spline, an analytic derivative, or
  another choice meeting README §6.3. It is recorded as an IMPLEMENTATION CHOICE;
  - *Taken by prompt 03 (2026-09-30):* a cubic spline of asinh m (log 03, Deviations).
- anything that would need `ScalarModel.py` beyond prompt 01's factoring.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Report the hard-reflection count](01-report-hard-reflections.md) | **P1** | Sonnet | ✍️ 2026-09-30 | ✅ 2026-09-30 | see `git log` ("Report the hard-reflection count in captions and the survey") | [`logs/01-report-hard-reflections.md`](logs/01-report-hard-reflections.md) |
| 02 | [Wire the network flag; run the full network](02-wire-the-network-flag.md) | **P2**, version bump | Opus | ✍️ 2026-09-30 | ⚠️ 2026-09-30, with deviations | see `git log` ("Wire the BBN network flag to PRyMordial and run the full network") | [`logs/02-wire-the-network-flag.md`](logs/02-wire-the-network-flag.md) |
| 03 | [The adiabatic mass: the source-response term](03-adiabatic-source-response.md) | **P3** | Opus | ✍️ 2026-09-30 | ⚠️ 2026-09-30, with deviations | see `git log` ("Add the source-response term to the adiabatic mass") | [`logs/03-adiabatic-source-response.md`](logs/03-adiabatic-source-response.md) |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-09-30 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| P1 | **DEFECT, low** | The count is stored as `extra_data["number_hard_reflections"]`, but the caption reads `"hard_reflections"` and always prints 0. `plot_by_beta.py` reports it nowhere. Closes `[00-hard-reflection-count-is-stored-but-never-reported]` and `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`. | 01 | **done 2026-09-30** (log 01; suites 12 / 18) |
| P2 | **DEFECT, medium** | `_configure_PRyMordial` sets `small_network_flag`, but PRyMordial reads `smallnet_flag`. Every stored `small_network = True` row ran the full network. Closes `[03-small-network-flag-is-never-read-by-prymordial]`. | 02 | **done 2026-09-30, COMPLETE WITH DEVIATIONS** (log 02). `VERSION_LABEL` `"2026.3.0"`: **every store made before 2026.3.0 is invalid.** The Yp bound of README §6.2 was missed (6.2e-5 against 1e-5); the user withdrew the Yp and D/H bounds on 2026-09-30 (Decisions; log 02, addendum). Suites now 12 / 21, all OK |
| P3 | **DEFECT, high** | `M2eff_over_H2` omits (ln Ω)′² ρ_R,E [Σ² − Σ_T/(1 + x) + f_m]. That is the whole density-dependent mass for the exponential coupling. The bracket runs from −0.407 (146 MeV) to +0.350 (230 MeV), and → 1 in matter domination. Closes `[00-adiabaticity-diagnostic-omits-the-source-response-term]`. | 03 | **done 2026-09-30, COMPLETE WITH DEVIATIONS** (log 03). The term is in `conformal_mass_over_H2`; Q's numerator is computed as m(1 + Ḣ/H²) + ½ dm/dN from an asinh spline. The test reference agrees to 9.6e-8 in the bracket norm. **No `AdiabaticHistory` row made before 2026.3.0 is comparable.** One §6.3 witness is missed: the h = 1e-4 central difference misses 1e-6 in a 2 MeV window above 120 MeV for the non-production spline class (1.1e-6). This is the witness's own h² error, not the derivative's; §3. Suites now 18 / 30, all OK |

---

## 3. Active and unresolved issues

Two opened at the plan's amendment on 2026-09-30, for the authors. One opened by prompt 02 was
withdrawn by the user the same day (§4). Two opened by prompt 03 (the last two below). Issues
opened by this campaign's prompts go here too, with an index row under §1.3 of
`.documents/OPEN_ISSUES.md`.

- **[00-adiabaticity-is-evaluated-at-fixed-k-over-H-not-for-fixed-comoving-modes]** *(the
  planner's audit, 2026-09-30, on `b0e46bc`; reasoned from the code and the paper, not run)*.
  - **What.** `compute_adiabatic_values` evaluates |Q| at fixed k_p/H ∈ {10, 10², 10³, 10⁴}
    (`AdiabaticHistory.Q_labels`). At each N that is whichever comoving mode currently sits at that
    depth inside the horizon, so the stored max |Q| over the history mixes different modes.
    - The derivative in Q is right for a fixed comoving k, because k drops out of dω/dτ. Each value
      is therefore correct for the mode it describes; it is the maximum over N that mixes them.
  - **What the paper says.**
    - The main text (`Paper1.tex`, "Adiabaticity" paragraph, `eq:adiabaticity`) describes the code
      as it is, "for a physical wavenumber k", and reads the maximum as "whether adiabaticity is
      violated at any point".
    - The appendix ("Adiabaticity revisited") instead fixes the comoving k = (aH) at the first
      rebound, which is k_p/H ≈ 1 there.
    - The stored scales include no horizon-scale mode. That is where the gravitational term and
      the source-response term matter most relative to k_p²/H².
  - **Impact.** No wrong number. The summary may not answer the question the text asks of it: a
    violation for one mode can be missed, or a violation reported for a mode that never
    experiences it.
  - **Next step.** A decision for the authors: which modes the diagnostic should follow, whether
    fixed comoving k (for example, the scale at the first rebound) or fixed k_p/H, and whether
    k_p/H ~ 1 belongs in the set. **Not changed by prompt 03**, which keeps the scales.

- **[00-paper-gives-two-inconsistent-adiabaticity-conditions]** *(the planner's audit,
  2026-09-30; paper text, not code)*.
  - **What.** `Paper1.tex` states two adiabaticity conditions that differ.
    - **The main-text `eq:adiabaticity`.** Q = (m_eff²/H²)(1 + ½ d ln|m_eff²|/dN) /
      |m_eff²/H² + k²/H²|^{3/2}, with m_eff² including the metric term −H²(2 + Ḣ/H²). The code
      implements this.
    - **The appendix's `adiabaticity`.** It drops the 2H·V_eff″ term from dω/dτ, keeps only the
      bare V″ and V‴, and sets the metric term to zero for radiation domination.
  - **Two further points on the appendix.**
    - Its argument that "the conformal contributions to the second and third derivative cancel
      exactly" is the H5 omission (`[00-adiabaticity-diagnostic-omits-the-source-response-term]`).
    - Its justification, suppression by M/M_P, holds near the V_eff minimum. It does not hold
      while the field is displaced from it: parked, or kicked.
  - **Also.** The main text says taking the modulus "allows the sign changes to be passed through
    without loss". Of the code before prompt 03 that is not true; see README §2 (e).
  - **Impact.** A reader cannot tell which condition produced `Adiabaticity1.pdf`.
  - **Next step.** The authors reconcile the two against the code as prompt 03 leaves it. Prompt
    03's addendum to `.documents/numerical-methods-for-paper.md` gives them the material. **Not a
    code change.**

- **[03-spline-eos-derivative-witness-misses-1e-6-above-the-120-mev-join]** *(log 03, Deviations
  1; `CosmologyModels/tests/test_eos_w_derivative.py` and a scratch probe, on `5aba202` plus
  prompt 03's diff)*.
  - **What.** README §6.3 row 2 scores `SaikawaShirai_EOS_spline.dw_dlogT` and the base-class
    formula against a central difference of `w` with half-step 1e-4, to ≤ 1e-6. Between 120.0
    and 122.2 MeV that witness is itself in error by up to **1.12e-6**: 1.104e-6 at the test's
    grid point 120.9 MeV, and 1.12e-6 on a 200001-point scan. The spline class's g splines are
    fitted across the raw fits' 120 MeV jump and ring there, which makes w‴ large.
  - **Why it is the witness, not the derivative.** The difference scales as h². It is 1.12e-8
    at h = 1e-5, and Richardson extrapolation of h = 1e-4 and 5e-5 gives 7.4e-8. `dw_dlogT` is
    the exact derivative of the class's w.
  - **What was done.** The threshold was not rewritten. Within a factor 1.03 of 120 MeV the test
    uses h = 1e-5, and asserts ≤ 1e-6 there (it measures 1.10e-8). It reports the h = 1e-4 value
    and does not assert it. Everywhere else it asserts h = 1e-4.
  - **Impact.** None on production, which uses `Xav_EOS_spline` (3.4e-8 against the h = 1e-4
    witness everywhere). The spline class is not on the pipeline's path.
  - **Next step.** A decision for the user: accept the h = 1e-5 witness in that window, or
    restate the row.

- **[03-q-numerator-end-samples-carry-the-spline-end-condition-error]** *(log 03, Deviations 3;
  `ComputeTargets/tests/test_adiabatic_mass.py` (e), on `5aba202` plus prompt 03's diff)*.
  - **What.** `Q_numerator` takes dm/dN from a not-a-knot cubic spline of asinh m. At the first
    and last samples of a history the end condition sets the derivative. On README §2 (e)'s
    sign-changing history at ΔN = ln 10/250 the error in A·C is **2.26e-4** of max |A·C| at the
    last sample and 1.81e-4 at the first. At the samples with N ∈ [0.5, 11.5] it is 3.1e-6.
  - **What was done.** Test (e) asserts ≤ 1e-4 on [0.5, 11.5], the window of the planning probe
    the target came from, and reports the error at all samples. A quintic spline gets the ends
    to 2.1e-5, but it is 4.6 times worse on a spike 0.03 wide (4.2e-4 against 9.3e-5), so it
    was not used.
  - **Impact.** Small, and only on the first and last few samples of each history. In Q the
    error is further divided by |B|^{3/2}, which is ≥ 10³ for the stored scales. The maximum of
    |Q| is set at bounces, not at the endpoints.
  - **Next step.** Only if the end samples come to matter: clamp the spline's end derivatives
    with one-sided estimates, or drop the end samples from the maximum. Not needed for
    production.

---

## 4. Resolved issues

Two assigned issues, closed by prompt 01 on 2026-09-30. Their entries stay on the
`review-remediation` board, each with a **Resolved** line; they are listed here as the record.

- **[00-hard-reflection-count-is-stored-but-never-reported]** and
  **[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]** — resolved by
  prompt 01 (log 01). One stored-key constant (`HARD_REFLECTIONS_KEY`), one reader
  (`extract_common.hard_reflection_count`) used by the caption and by `plot_by_beta.py`, a
  `hard_reflections` column in `data.csv` and a per-(M, Lambda) stdout summary. The caption test
  fails on the old reader.

One assigned issue, closed by prompt 02 on 2026-09-30:

- **[03-small-network-flag-is-never-read-by-prymordial]** — resolved by prompt 02 (log 02).
  `_configure_PRyMordial` sets `PRyM_init.smallnet_flag`, the name PRyMordial reads, and no longer
  sets `small_network_flag`. Production, the SM baseline, `tools/bbn_baseline.py` and the fixtures
  pass `small_network=False`, the full network; every pinned abundance passes unchanged.
  `VERSION_LABEL` is `"2026.3.0"`. Test (a) fails on the old `BBNData.py`.

One assigned issue, closed by prompt 03 on 2026-09-30:

- **[00-adiabaticity-diagnostic-omits-the-source-response-term]** — resolved by prompt 03 (log 03).
  - **The mass.** `AdiabaticComputePolicy.M2eff_over_H2` now includes the source response,
    3 M_P² E (ln Ω)′² (Σ² − Σ_T/(1 + x) + f_m)/(1 + f_m), in `conformal_mass_over_H2`.
    Σ_T = −3 `dw_dlogT`, which is now on every EOS class.
  - **The reference.** It is built from entropy conservation alone and agrees with the term to
    9.6e-8 in the bracket norm, converging as h². Audit §5's form differs from it by up to 0.73.
  - **Q's numerator** is m(1 + Ḣ/H²) + ½ dm/dN, from a spline of asinh m, so a sign change of
    M²_eff no longer breaks it.
  - **The new test** fails on the old `AdiabaticHistory.py`: 0.41 in the bracket norm, 1.53 on
    the crossing history, and a raise at an exact zero.

One issue opened by prompt 02 and withdrawn by the user on 2026-09-30:

- **[02-network-shift-bounds-sit-inside-prymordial-noise]** *(log 02, Deviations and
  Verification; `ComputeTargets/tests/test_network_flag.py` (b) and two scratch probes, on
  `cf773b2` plus prompt 02's diff)*.
  - **What.** README §6.2 bounds the small-against-full shift, constant 0.08 ρ_SM family, by
    ≥ 5e-3 in ⁷Li/H, ≤ 1e-3 in D/H and ≤ 1e-5 in Yp, from the `review-remediation` board's
    measurement (1 %, 1.5e-4, 1.5e-6; raw-fit g_ρ, 3.38 below 10 keV, `47c50ae`). Measured through
    the fixed flag:

    | Construction | ⁷Li/H | D/H | Yp |
    |---|---|---|---|
    | fixture `CONSTANT` (spline-class g_ρ), test (b) | 1.006e-2 | 2.274e-4 | **6.200e-5** |
    | raw-fit `_raw_G_rho` (3.383 below 10 keV), scratch | 1.194e-2 | **1.203e-3** | 2.333e-6 |
    | ρ_NP ≡ 0 (SM), scratch | 1.168e-2 | **1.736e-3** | 2.193e-5 |

  - **Reading.** The two constant-family constructions agree to 2.2e-10 in ρ_NP (review-remediation
    log 03), yet the network's Yp shift changes by a factor 27 and its D/H shift by a factor 5
    between them. The Yp and D/H shifts sit inside PRyMordial's own noise
    (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`). The ⁷Li/H shift, 1.0–1.2 %,
    is robust, and it is what shows that the flag selects the network.
  - **What was done.** The Yp bound was not rewritten. It is `test_b_prime_Yp_within_1e_5`, marked
    `unittest.expectedFailure`; test (b) asserts the ⁷Li/H, D/H and pin rows, which pass.
  - **Impact.** None on production, which runs the full network. The table's bound cannot be met
    on the fixture as written.
  - **Next step.** A decision for the user: accept the miss, or restate the Yp and D/H rows as
    bounds PRyMordial can resolve (for example ⁷Li/H only, with Yp and D/H bounded by the noise
    band), and then remove the `expectedFailure`.
  - **Withdrawn (2026-09-30, the user):** not an issue. The small network's offset from the full
    one in Yp and D/H is a property of PRyMordial, which never promised to hold it at any level;
    the 1e-5 and 1e-3 bounds were ours, not PRyMordial's. Both bounds and the expected-failure
    test are removed; ⁷Li/H alone witnesses that the flag selects the network. README §6.2
    amended; log 02, addendum.
