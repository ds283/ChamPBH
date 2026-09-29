# Production readiness campaign — implementation state

**Last updated:** 2026-09-30 · **Status: PLANNED — 0 of 4 prompts landed.**

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
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.2

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

None pending. Decisions the prompts may surface, each a stop-and-ask in its prompt:

- whether the zero-crossing guard fails closed or floors |M²_eff| (03). Either is allowed, and it
  is recorded as an IMPLEMENTATION CHOICE;
- anything that would need `ScalarModel.py` beyond prompt 01's factoring.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Report the hard-reflection count](01-report-hard-reflections.md) | **P1** | Sonnet | ✍️ 2026-09-30 | — | — | — |
| 02 | [Wire the network flag; run the full network](02-wire-the-network-flag.md) | **P2**, version bump | Opus | ✍️ 2026-09-30 | — | — | — |
| 03 | [The adiabatic mass: the source-response term](03-adiabatic-source-response.md) | **P3** | Opus | ✍️ 2026-09-30 | — | — | — |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-09-30 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| P1 | **DEFECT, low** | The count is stored as `extra_data["number_hard_reflections"]`, but the caption reads `"hard_reflections"` and always prints 0. `plot_by_beta.py` reports it nowhere. Closes `[00-hard-reflection-count-is-stored-but-never-reported]` and `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`. | 01 | open |
| P2 | **DEFECT, medium** | `_configure_PRyMordial` sets `small_network_flag`, but PRyMordial reads `smallnet_flag`. Every stored `small_network = True` row ran the full network. Closes `[03-small-network-flag-is-never-read-by-prymordial]`. | 02 | open |
| P3 | **DEFECT, high** | `M2eff_over_H2` omits (ln Ω)′² ρ_R,E [Σ² − Σ_T/(1 + x) + f_m]. That is the whole density-dependent mass for the exponential coupling. The bracket runs from −0.407 (146 MeV) to +0.350 (230 MeV), and → 1 in matter domination. Closes `[00-adiabaticity-diagnostic-omits-the-source-response-term]`. | 03 | open |

---

## 3. Active and unresolved issues

None yet. Issues opened by this campaign's prompts go here, with an index row under §1.2 of
`.documents/OPEN_ISSUES.md`.

---

## 4. Resolved issues

None yet.
