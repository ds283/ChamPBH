# Production readiness campaign — implementation state

**Last updated:** 2026-09-30 · **Status: IN PROGRESS — 1 of 4 prompts landed (01).** Amended 2026-09-30
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

Target branch `production-readiness` from `204795e`. `VERSION_LABEL` is `"2026.2.0"`, and prompt
02 makes it `"2026.3.0"`.

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

None pending. Decisions the prompts may surface, each a stop-and-ask in its prompt:

- how prompt 03 represents dm/dN in Q's numerator: an asinh spline, an analytic derivative, or
  another choice meeting README §6.3. It is recorded as an IMPLEMENTATION CHOICE;
- anything that would need `ScalarModel.py` beyond prompt 01's factoring.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Report the hard-reflection count](01-report-hard-reflections.md) | **P1** | Sonnet | ✍️ 2026-09-30 | ✅ 2026-09-30 | see `git log` ("Report the hard-reflection count in captions and the survey") | [`logs/01-report-hard-reflections.md`](logs/01-report-hard-reflections.md) |
| 02 | [Wire the network flag; run the full network](02-wire-the-network-flag.md) | **P2**, version bump | Opus | ✍️ 2026-09-30 | — | — | — |
| 03 | [The adiabatic mass: the source-response term](03-adiabatic-source-response.md) | **P3** | Opus | ✍️ 2026-09-30 | — | — | — |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-09-30 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| P1 | **DEFECT, low** | The count is stored as `extra_data["number_hard_reflections"]`, but the caption reads `"hard_reflections"` and always prints 0. `plot_by_beta.py` reports it nowhere. Closes `[00-hard-reflection-count-is-stored-but-never-reported]` and `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`. | 01 | **done 2026-09-30** (log 01; suites 12 / 18) |
| P2 | **DEFECT, medium** | `_configure_PRyMordial` sets `small_network_flag`, but PRyMordial reads `smallnet_flag`. Every stored `small_network = True` row ran the full network. Closes `[03-small-network-flag-is-never-read-by-prymordial]`. | 02 | open |
| P3 | **DEFECT, high** | `M2eff_over_H2` omits (ln Ω)′² ρ_R,E [Σ² − Σ_T/(1 + x) + f_m]. That is the whole density-dependent mass for the exponential coupling. The bracket runs from −0.407 (146 MeV) to +0.350 (230 MeV), and → 1 in matter domination. Closes `[00-adiabaticity-diagnostic-omits-the-source-response-term]`. | 03 | open |

---

## 3. Active and unresolved issues

Two opened at the plan's amendment on 2026-09-30, for the authors. Issues opened by this
campaign's prompts go here too, with an index row under §1.3 of `.documents/OPEN_ISSUES.md`.

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
