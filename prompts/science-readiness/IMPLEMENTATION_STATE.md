# Science-readiness campaign — implementation state

**Last updated:** 2026-10-01 · **Status: PLANNED — 0 of 9 landed.** Planned on 2026-10-01 against
`main` at `6aaa706`, from a Claude Science re-evaluation kept at
[`source/campaign_reevaluation_2026-10-01.md`](source/campaign_reevaluation_2026-10-01.md) and
checked against the tree by the planner (README §0.3). Suites at `6aaa706`: CosmologyModels 18,
ComputeTargets 67, Datastore 17.
**Target branch** `science-readiness`, to be cut from `6aaa706`; planning and orchestration commits
land on it.
**`VERSION_LABEL` is `"2026.5.0"`**; prompt 01 bumps it to `"2026.6.0"`, once. From then on every
store made before 2026.6.0 is invalid, and the science run needs a fresh datastore file (columns
are added with no migration).

The campaign hands the scalar field to PRyMordial through the expansion rate alone, with a patched
PRyMordial that has no fictitious new-physics temperature and an optional wall-clock limit. It
checks PRyMordial's output before storing it. It stores a `ScalarModel`'s failure reason and first
bounce. It makes φ\* a run option, with a warning for a super-Planckian start. It replaces the aliased point
samples BBN reads with bounce averages on the dense output. It narrows the BBN spline to the range
PRyMordial reads, and adds the extraction and the four figures of the science run.

**Campaign:** [`README.md`](README.md) ·
**Code (planned):**
- `PRyM/PRyM_init.py`, `PRyM/PRyM_main.py` (the Hubble flag, the wall-clock limit, the `cham03`
  revert; prompt 01);
- `ComputeTargets/BBNData.py` (01, 05, 06); `ComputeTargets/ScalarModel.py` (02, 03, 05);
- `Datastore/SQL/ObjectFactories/BBNData.py` (01), `…/ScalarModel.py` (02, 03, 05);
- `main.py` (01, 02, 04), `plot_by_beta.py` (02, 04, 07), `plot_ScalarModel.py` (01, 04),
  `extract_common.py` (07), `pipeline_selection.py` (04), `config/argument_parser.py` (01, 04,
  07), `config/version.py` (01);
- `tools/history_and_bbn.py` (new in 01; 03, 04, 05);
- `ComputeTargets/tests/`, `Datastore/tests/`;
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `architecture-summary.md`, `paper-corrections-numerical-section.md` (08, additive);
  `.documents/review-remediation-verification.md` (09, additive).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.7–§1.8.

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing at the board that holds it. Where
> the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-10-01, the user (planning conversation): README §0.2 U1–U5.**
  - U1: replace the `NP_thermo_flag` route outright by patching PRyMordial, and remove `T_NP`.
  - U2: the wall-clock limit is an optional parameter of the PRyMordial patch, returned as a
    failure.
  - U3: bounce averages by Gauss–Legendre on the dense output at sampling time.
  - U4: the first bounce in dedicated columns.
  - U5: the Phase D extraction and figures in this campaign.
- **2026-10-01, the planner: README §0.2 P1–P9 proposed.** P1 removes `p_NP` everywhere; P2
  reverts `cham03`; P3 is the 600 s default and the option; P4 folds in two `run-integrity`
  issues; P5 defines the first bounce; P6 (as proposed) skipped super-Planckian couplings; P7
  defines the cell; P8 sets the floor at 0.2 keV; P9 is one bump and a fresh datastore.
- **2026-10-01, the user: P1–P5 and P7–P9 accepted as proposed; P6 overruled.** A coupling with
  Ω(φ*) T* > M_P is warned about and computed, never skipped or refused. The user's reason:
  there can be numerical reasons to start slightly super-Planckian (to kill transients), the
  premise is that the final state does not depend on the initial one, and dropping such points
  brings the proposed science run no benefit. README §0.2 P6, §2 (h) and prompt 04 carry the
  ruling. With this line the precondition of orchestrator 01 §1.1 is met. The two `run-integrity`
  issues of P4 are now firmly assigned to prompt 01. The user had earlier asked how close the
  probes came to P3's 600 s; the answer (the Hubble-only solves took 9.7–10.0 s, about 1.7 % of
  the limit, unloaded) left P3 as proposed.

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- the patched route does not reproduce the reference (prompt 01, §5);
- a callback exception does not propagate out of LSODA (prompt 01, §5);
- the first negative-to-positive turning point is not a wall bounce on some history (prompt 03);
- the noise below 3 keV does not fall as required with averaged input (prompt 05);
- the `N`-average is not within `1e-3` of the time-average (prompt 05).

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The Hubble-only BBN route, the wall-clock limit and the output checks](01-hubble-only-bbn-route.md) | **R**, **W**, **O**, version bump, driver | Opus | ✍️ 2026-10-01 | — | — | — |
| 02 | [A failure reason on `ScalarModel` rows](02-scalarmodel-failure-reasons.md) | **F** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 03 | [Store the first bounce](03-first-bounce.md) | **T** | Opus | ✍️ 2026-10-01 | — | — | — |
| 04 | [The initial field as a run option, with a super-Planckian warning](04-initial-field-option.md) | **P** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 05 | [Bounce averages on the dense output](05-bounce-averages.md) | **A** | Opus | ✍️ 2026-10-01 | — | — | — |
| 06 | [Narrow the BBN spline to PRyMordial's range](06-bbn-spline-floor.md) | **L** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 07 | [Extraction and the science figures](07-extraction-and-figures.md) | **G** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 08 | [Documents](08-documents.md) | **D** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 09 | [Close-out verification and handover](09-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-10-01 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| R | **DEFECT, medium** | The `NP_thermo_flag` route integrates a fictitious `T_NP`, and adds −3H(ρ_NP + p_NP) and dρ_NP/dT to the plasma equation, cancelling only to `≈ r·1.2×10⁻³`. Replace it with a Hubble-only patch. Closes `[00-bbn-route-integrates-a-fictitious-np-temperature]`. | 01 | open |
| W | **GAP** | No bound on a PRyMordial solve's wall time. Closes `[00-prymordial-has-no-wall-clock-limit]`. | 01 | open |
| O | **GAP** | A successful PRyMordial return is stored unchecked. Closes `[00-prymordial-output-is-stored-unchecked]`. | 01 | open |
| F | **GAP** | A `ScalarModel` failure row carries no reason. Closes `[00-scalarmodel-failure-rows-carry-no-reason]` (assigned). | 02 | open |
| T | **GAP** | The first bounce is not stored and cannot be read from the samples at small `M`. Closes `[00-first-bounce-is-not-stored]`. | 03 | open |
| P | **GAP** | φ\* is a literal in three drivers, with no super-Planckian warning. Closes `[00-initial-field-value-is-hard-coded-and-unchecked]` (assigned). | 04 | open |
| A | **DEFECT, medium at M ≲ 10⁻⁴** | BBN integrates a spline through random-phase samples of the bounces below a few keV. Closes `[00-bbn-input-is-aliased-at-small-M]`; narrows `[00-stored-samples-alias-the-rebounds]` (assigned). | 05 | open |
| L | **DEFECT, low** | The BBN spline reaches 0.1 eV, while PRyMordial reads to 0.363 keV. Closes `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` (assigned). | 06 | open |
| G | **GAP** | No extraction or figures for the science run. Closes `[00-no-extraction-for-the-science-figures]`. | 07 | open |
| D | documents | Three documents describe the `NP_thermo_flag` route. Closes `[00-documents-describe-the-thermo-route]` and `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` (assigned). | 08 | open |

---

## 3. Active and unresolved issues

Seven opened by the planner on 2026-10-01, one per campaign item that has no issue on another
board. Seven more are assigned from other boards (§3.1).

- **[00-bbn-route-integrates-a-fictitious-np-temperature]** *(README §0.3, §1 R; `PRyM_main.py`
  `:95–201` on `6aaa706`)*.
  - **What.** `_configure_PRyMordial` sets `NP_thermo_flag`, which puts ρ_NP into `Hubble`, which
    is wanted. It also adds −3H(ρ_NP + p_NP) and dρ_NP/dT to dT_γ/dt and integrates a third
    variable `T_NP` that no output reads (`cham03` makes its equation inert). The plasma obeys the
    Standard-Model equation only through a cancellation between two spline-derived terms.
  - **Impact.** README §6.1 shows the cost of the route on real histories.
  - **Next step.** Prompt 01.
- **[00-prymordial-has-no-wall-clock-limit]** *(README §1 W)*.
  - **What.** A solve that slows without failing occupies a Ray worker indefinitely; `run-integrity`
    declined a Ray timeout (its README §0.4).
  - **Impact.** One slow history stalls a survey's BBN stage.
  - **Next step.** Prompt 01, as a PRyMordial parameter (U2).
- **[00-prymordial-output-is-stored-unchecked]** *(README §1 O)*.
  - **What.** `_run_PRyMordial` returns `res[4:8]` as they come. `plot_by_beta.py` drops
    non-positive abundances at plot time, after storing them.
  - **Impact.** An unphysical result is a stored success.
  - **Next step.** Prompt 01.
- **[00-first-bounce-is-not-stored]** *(README §1 T; the source §2)*.
  - **What.** The dense-output first bounce (`N`, `T_J`, `φ_min`) exists only during the
    integration. The source's sample-based detector returns 0.138 GeV for 0.747 GeV at small `M`.
  - **Impact.** `T_deliver(β)`, a figure of the science run, cannot be made from a store.
  - **Next step.** Prompt 03.
- **[00-bbn-input-is-aliased-at-small-M]** *(README §1 A; the source §2; README §6.1)*.
  - **What.** Below a few keV, `ρ_NP/ρ_R,J` jumps from sample to sample at small `M`, because the
    samples catch the bounces at random phase: ±0.4 % at M = 10⁻⁵ and ±0.85 % at 10⁻³, around a
    median ten times smaller (README §6.1). The source reports one PRyMordial failure from it
    (β = 1.6, M = 10⁻⁵, low-T network); the planner did not reproduce it on `6aaa706`.
  - **Impact.** Noise of the order of the signal in the input PRyMordial integrates below 3 keV.
  - **Next step.** Prompt 05.
- **[00-no-extraction-for-the-science-figures]** *(README §1 G)*.
  - **What.** None of the four Phase D figures, nor a per-history table, can be produced from a
    store.
  - **Next step.** Prompt 07.
- **[00-documents-describe-the-thermo-route]** *(README §1 D)*.
  - **What.** `numerical-strategies.md` §7, `numerical-methods-for-paper.md` §4 and
    `architecture-summary.md` §7.4 describe the `NP_thermo_flag` route and the pressure callback.
  - **Next step.** Prompt 08, once the code is final.

### 3.1 Assigned to this campaign from other boards

Each entry stays on its own board, which carries an **Assigned (2026-10-01)** line. The prompt
that closes one adds a dated **Resolved** line there, deletes the index row, and records it in §4
here (README §5 rule 4).

| Issue | Board | Prompt |
|---|---|---|
| `[00-initial-field-value-is-hard-coded-and-unchecked]` | `review-remediation` | 04 |
| `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` | `review-remediation` | 06 |
| `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` | `review-remediation` | 08 |
| `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]` | `run-integrity` | 01 (P4) |
| `[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]` | `run-integrity` | 01 (P4) |
| `[00-scalarmodel-failure-rows-carry-no-reason]` | `integrator-remediation` | 02 |
| `[00-stored-samples-alias-the-rebounds]` | `integrator-remediation` | 05 (narrowed: the BBN half) |

---

## 4. Resolved issues

None yet.
