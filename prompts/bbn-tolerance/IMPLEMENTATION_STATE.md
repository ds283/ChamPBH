# BBN-tolerance campaign — implementation state

**Last updated:** 2026-10-03 · **Status: PLANNED — 0 of 3 landed.** Planned on 2026-10-03 against
`main` at `4ae25b4`, from a Claude Science brief kept at
[`source/brief_prym_lowT_failures.md`](source/brief_prym_lowT_failures.md) and checked against the
tree by the planner (README §0.3). Suites at `8efc50f` (no code changed through `4ae25b4`):
CosmologyModels 18, ComputeTargets 103, Datastore 31.
**Target branch** `bbn-tolerance`, cut from `4ae25b4`; planning and orchestration commits land on
it.
**`VERSION_LABEL` is `"2026.6.0"`** and does not change in this campaign (README §0.2 P5).
**`PRYM_VERSION` is `"bf24c3d+ri02+sr01"`**; prompt 02 makes it `"bf24c3d+ri02+sr01+bt02"`.

The campaign finds out why PRyMordial's low-temperature network fails on 11 of 684 φ\* = 5
histories of the 2026.6.0 science run, and why D/H scatters by up to 2.2×10⁻³ under ulp-level input
changes. It chooses a tolerance by measurement, patches it in, warns when a store serves BBN rows
from another PRyMordial version, and hands the user the route to refresh BBN without recomputing
histories.

**Campaign:** [`README.md`](README.md) ·
**Code (planned):**
- `tools/bbn_from_store.py` (new in 01);
- `PRyM/PRyM_main.py` (the two low-T calls; 02); `ComputeTargets/BBNData.py` (`PRYM_VERSION`;
  02);
- `pipeline_selection.py`, `main.py`, `plot_by_beta.py` (the warning; 02);
- `ComputeTargets/tests/` (01, 02);
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `paper-corrections-numerical-section.md`, `review-remediation-verification.md` (03, additive).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.9–§1.10.

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing at the board that holds it. Where
> the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-10-03, the user (planning conversation): README §0.2 U1.** Prompt 01 may run the full
  tolerance scan (about 350 solves) in parallel, 8–10 at a time, as a one-off exception to
  `CLAUDE.md`'s limit of a few PRyMordial solves per prompt. Wall times are measured separately,
  serially, on an idle machine. The store stays read-only.
- **2026-10-03, the planner: README §0.2 P1–P9 proposed.**
  - P1: prompt 01 changes no production code and nothing in `PRyM/`; tolerances are changed by
    interception.
  - P2: the scan grid.
  - P3: the rule for choosing the setting.
  - P4: the patch to both low-T calls, and `PRYM_VERSION` `+bt02`.
  - P5: no `VERSION_LABEL` bump; refresh by copy and `--drop bbn-data`.
  - P6: warn on a foreign `PRyM_version`, never filter.
  - P7: re-pin pinned constants, never loosen a bound.
  - P8: never accept a partial solve.
  - P9: residuals are measurements.

- **2026-10-03, the user: P1–P9 accepted as proposed.** With this line the precondition of
  orchestrator 01 §1.1 is met. P3's rule stands; the setting it selects is ruled separately after
  log 01, as orchestrator 02 §1.1 requires.

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- the tool does not reproduce the stored outcome (prompt 01, §8);
- the mechanism points outside the low-T stage (prompt 01, §8);
- no setting meets P3 (prompt 01, §6);
- **the setting itself** (P3): the user rules on log 01's recommendation before prompt 02;
- the patched tree does not reproduce log 01 digit for digit (prompt 02, §5);
- a re-pinned test fails its unchanged bound (prompt 02, §5).

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The mechanism and the low-T tolerance scan](01-mechanism-and-tolerance-scan.md) | **M**; measures **S**, **N** | Opus | ✍️ 2026-10-03 | — | — | — |
| 02 | [Set the low-T tolerance and warn on a stale PRyMordial version](02-low-T-tolerance-patch.md) | **S**, **N**, **K** | Opus | ✍️ 2026-10-03 | — | — | — |
| 03 | [Documents and close-out](03-documents-and-close-out.md) | **D** | Sonnet | ✍️ 2026-10-03 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| M | measurement | The mechanism, the tolerance scan, Yp's floor, upstream's defaults, and a recommended setting. | 01 | open |
| S | **DEFECT, medium** | The full low-T network fails near T_J = 1 keV on 11 of 684 φ\* = 5 histories, depending on ulp-level details of the input. Closes `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`. | 01, 02 | open |
| N | **DEFECT, low–medium** | D/H moves by up to 2.2×10⁻³ under a 10⁻¹² change to ρ_NP at the default tolerance. Closes `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` (assigned). | 01, 02, 03 | open |
| K | **GAP** | A store that was not refreshed serves BBN rows from an older PRyMordial with no warning. Closes `[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`. | 02 | open |
| D | documents | The tolerance, its measurements, the refresh route, the handover. | 03 | open |

---

## 3. Active and unresolved issues

Two opened by the planner on 2026-10-03, one per defect or gap with no issue on another board. One
assigned from another board (§3.1).

- **[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]** *(README §1 S; the brief §1–§2)*.
  - **What.** In the 2026.6.0 science store, 11 of 684 φ\* = 5 histories have a `BBNData` failure
    row with `PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)':
    status=-1, message='Required step size is less than spacing between numbers.'`. Each failed at
    96–99 % of the stage's end time, at T_J just above 1 keV. 9 are surfing histories and 2
    non-surfing (README §6.0). Their neighbours in β and M complete. The brief reports that any
    change to the input of 10⁻¹² relative or more cures every one.
  - **Measured (the brief, 2026-10-03; not yet re-measured in this repository).** The low-T calls
    pass no `rtol`, so SciPy's `1e-3` applies. With `rtol = 1e-6` on the full call, four failures
    and the control complete.
  - **Impact.** 11 rows are "not assessed", including β = 1.345 inside the threshold zoom and five
    points of the convergence-in-M comparison. Whether a history fails is not reproducible across
    harnesses (β = 1.6, M = 10⁻⁵ failed on 2026-10-01 and completed in the `science-readiness`
    close-out).
  - **Next step.** Prompt 01 measures; prompt 02 fixes.
- **[00-a-store-serves-bbn-rows-from-another-prym-version-silently]** *(README §0.3, §1 K)*.
  - **What.** `BBNData` lookups are keyed on `VERSION_LABEL` and ignore `PRyM_version`
    (`Datastore/SQL/ObjectFactories/BBNData.py` `build`). Only `inventory` lists the versions
    present.
  - **Impact.** After prompt 02, `plot_by_beta.py` on a store that was not refreshed would plot
    old-tolerance abundances with no sign that they are old.
  - **Next step.** Prompt 02 (README §0.2 P6): warn, do not filter.

### 3.1 Assigned to this campaign from other boards

Each entry stays on its own board, which carries an **Assigned (2026-10-03)** line. The prompt
that closes one adds a dated **Resolved** line there, deletes the index row, and records it in §4
here (README §5 rule 4).

| Issue | Board | Prompt |
|---|---|---|
| `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` | `review-remediation` | 01 (measures), 02 (closes) |

---

## 4. Resolved issues

None yet.
