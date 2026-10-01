# Log 03 — Documents and the paper's corrections

**Prompt:** prompts/integrator-remediation/03-documents-and-paper-corrections.md
**Commit:** the commit that adds this file ("Document the step loop and list the paper's corrections"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-01
**Result:** COMPLETE

The work was done on top of `614c41a` (`HEAD` at dispatch, prompt 02's commit). "HEAD~1" below means the parent of this prompt's commit. No production code and no test was touched. `Paper1.tex` was read, not edited.

## What shipped

**D1 — `.documents/numerical-strategies.md`.** A new §3.5, "Added 2026-10-01 (`integrator-remediation`): the step loop that replaced §2.1 and §3.1–§3.4", inserted between §3.4's last line and the `---` before §4 (`:175–359` after the insertion). Seven subsections:
- 3.5.1 the pure loop, its signature, `StepControl`, `IntegrationResult`, termination;
- 3.5.2 the kinematic cap with both terms and the bound they enforce, with the audit's cost figures;
- 3.5.3 the floor, the elastic reflection, guards G1 and G2, the agreement with the resolved bounce, the validity range;
- 3.5.4 the Jacobian clamp and the mechanism it defeats (audit §8 F2);
- 3.5.5 the exception table as implemented in logs 01 and 02, and the one stepper and one label;
- 3.5.6 the step budget and why it exists (audit §3.7);
- 3.5.7 what is stored per history, the gone and the new `extra_data` keys.

The text names §2.1, §2.2's tolerance note and §3.1–§3.4 as describing the code before `VERSION_LABEL 2026.5.0`. Nothing above the new heading changed.

**D2 — `.documents/architecture-summary.md`.** Six dated blockquote notes, each inserted after the passage it annotates (line numbers on `614c41a`, which are the `b1f64d8` numbers):
- `:477`, after the `IntegrationSolver` code block (`"solve_ivp+Radau"` label): the single label `"Radau+kinematic-cap"`, stepping 0; old registrations kept;
- `:518`, `ScalarFieldIntegrationSupervisor`: the level-entry, hard-reflection and fragment notifications are gone; `notify_reflection`, `notify_step_cap` are new;
- `:577`, `SolutionFragment`: the type is gone; one `OdeSolution`; `ODERHS` is called by `Radau`, not `solve_ivp`;
- `:609`, the integration procedure (after step 5 so that the numbered list is not broken): the loop, the cap, the floor reflection, the budget;
- `:682`, `AbstractPotential`: the region properties are defined and unread since 2026.5.0, and the two new non-abstract properties;
- `:844`, "Solver fallback": it never ran and is deleted; one `try`; the exception table.

Each points to the §3.5 subsection.

**D3 — `.documents/integrator-audit-2026-09-30/README.md`.** A "## Outcome (added 2026-10-01)" subsection after a `---`, at the end. It names the campaign, lists prompts 01 (`fc97233`), 02 (`614c41a`) and this prompt by subject, names which of §9.1, §9.2, §9.4, §10 items 1–4 and the §8 F2, §5 and §4 findings landed, notes the one audit claim met in a narrower sense (`φ_min` on the dense output), and lists what stays open by issue name. Nothing above it changed.

**D4 — `.documents/paper-corrections-numerical-section.md` (new).** A table of nine rows, each with the sentence quoted and its `Paper1.tex` line, what the code did on `b1f64d8`, what it does now, the measurement and the script, and a suggested replacement:
1. the "$10^{-2}$ outside both regions" cap;
2. the "$10^{-5}$ and $10^{-6}$ inside" caps;
3. the two nested regions, halting, restarting, and the "crossings are located by root-finding";
4. the replacement text for 1–3 (the kinematic cap);
5. the BDF → LSODA → Dormand–Prince fallback;
6. the relaxed tolerances for $M/M_P \lesssim 10^{-3}$;
7. the sentence that $10^{-8}$ "drives the step size below the point at which the integration can make progress";
8. the hard reflection described as a fallback;
9. the solution "assembled from fragments".

Then a section of statements that remain correct (with the reason), and a section of statements the paper may carry: $T_J$ rises 12–13 % during the surfing overshoot and the first-bounce temperature is reached after that rise (audit §8 F1); the physical-$M$ settling that needs a parked-tracking model (audit §3.7, §9.4); the aliasing of the stored samples (audit §7).

**Board and index.** `IMPLEMENTATION_STATE.md`: status "3 of 4 landed", row 03, item D, and the issue `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]` moved from §3 to §4 with a dated "Resolved" line; the §3 intro counts. `.documents/OPEN_ISSUES.md`: its row deleted, count 26 → 25, board count 10 → 9, "none assigned", §1.6 intro updated.

## Deviations from the prompt

### 1. Row 4 in the paper-corrections table is a replacement, not a mismatch — IMPLEMENTATION CHOICE

The prompt lists the sentences that must appear. I added row 4, which quotes no sentence and gives the kinematic cap's text, because rows 1–3 each delete a sentence and an author would need one place that says what to write instead. It also records that the paper's premise ("the field can traverse the entire steep region within a single trial step") is correct and stays. The alternative, a replacement sentence in each of rows 1–3, would have repeated the same paragraph three times. Row 7 is the "$10^{-8}$ drives the step size below…" clause, which the prompt lists separately from the relaxed-tolerance sentence; I kept it separate.

### 2. The §3.5 heading level and numbering — IMPLEMENTATION CHOICE

The prompt says "a new subsection after §3.4". I numbered it §3.5 with seven `####` subsections (3.5.1–3.5.7), rather than a single flat block, so that the architecture-summary notes can point at a subsection. The alternative was a single un-numbered "Addendum" heading. A numbered §3.5 does not collide with any existing reference: nothing in the repository cites a "§3.5" of this file (`grep -rn "numerical-strategies.md.*3\.5" .` finds only the new notes).

### 3. A dated note is inserted after a passage rather than in front of it — IMPLEMENTATION CHOICE

For `:477`, `:609` and `:682` the passage is inside or at the end of a code block or a numbered list. I put the note after the block or after step 5 so that neither is split. The prompt's "at each of the lines" is read as "next to".

## Verification performed

**Additions only** (`git diff --numstat` of the working tree against `614c41a` before the commit; the three annotated documents):

| file | added | removed |
|---|---|---|
| `.documents/numerical-strategies.md` | 184 | 0 |
| `.documents/architecture-summary.md` | 12 | 0 |
| `.documents/integrator-audit-2026-09-30/README.md` | 48 | 0 |
| `.documents/paper-corrections-numerical-section.md` | new file | — |

`git diff HEAD~1 HEAD -- <file> | grep '^-'` prints only the `---` header line for each of the three. The board and the index have deletions by design (the issue's row and the counts).

**README §6.3, every row:** D1's addendum after §3.4 with §2.1–§3.4 unchanged above it; D2's notes at `:518`, `:577`, `:605–608` (placed after `:609`), `:671–676` (placed after `:682`), `:844` and, besides, at `:477`; D3's Outcome; D4's file.

**Every number in the new text** was read from audit README §3.2, §3.3, §3.5–§3.7, §4, §6, §7, §8, §9.1–§9.3, or from logs 01 and 02, and carries the section or the script. I did not run an audit script or a new measurement: the prompt adds no code and its numbers are the audit's and the logs'. One figure is my own arithmetic and is labelled as such in the text: the `M = 1e-6`, β = 2 history's step count (about 3.5×10⁵) is 3.33×10⁶ RHS divided by 9–10 RHS per accepted step, the ratio that the nine histories of log 01 show (§3.5.6 says so).

**Paper quotations** were checked against `Paper1.tex` lines 2942–3030, read on 2026-10-01; the line numbers in the table are those of that read.

**Suites** (from the root, `PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`; run on this tree, no code changed):

| package | before (`614c41a`) | after |
|---|---|---|
| CosmologyModels | 18 OK | 18 OK |
| ComputeTargets | 67 OK | 67 OK |
| Datastore | 17 OK | 17 OK |

## Observations not acted on

1. **`numerical-strategies.md` §2.2's last paragraph is wrong for the production potential even before this campaign.** It says `ReclinerPotential.default_abs_tol`/`default_rel_tol` loosen the tolerance for $M/M_P < 10^{-3}$ and implies this is the tolerance mechanism. Audit §1 found that nothing reads these overrides and the production potential is `ExponentialPotential`. §3.5.7 now says so. This is §2 (the integrator section) and covered by D1, so no stop condition applies. Sections 1 and 4–9 of the file were not found to be wrong for the final tree in this prompt's reading, which was confined to the integrator sections and the lines D2 names; I did not audit them.
2. **`architecture-summary.md` line 14** ("ODE supervisor framework for driving `scipy.integrate.solve_ivp`") and the `ODERHS` description at `:575` still name `solve_ivp`. Line 575 is annotated by the `:577` note. Line 14 is a one-line overview outside the lines the prompt names; I left it. It is accurate for `QuadSupervisor`'s use, which still runs through the same base class. Not an issue.
3. **The audit's scripts do not run on this tree** (log 01 Observations 6). The Outcome subsection says so and points at that log's method. No action.
4. **`.documents/paper-corrections-numerical-section.md` rests on the paper as read today.** The paper is in another repository and its text may change; the line numbers will move.

No new §3 issue was opened.

## State handed to the next prompt

- **The documents are done.** Prompt 04 (close-out) adds to `.documents/review-remediation-verification.md` §4, additively, per campaign README §7. For §7 item 2 and item 5 it can quote `numerical-strategies.md` §3.5 (the loop, cap, floor, guards, clamp, table, budget, stored keys) rather than re-deriving them. The paper corrections file is the place to point item "what is still open" at for the authors.
- **Names and sections prompt 04 may cite:** `numerical-strategies.md` §3.5.1–§3.5.7; `architecture-summary.md` notes dated 2026-10-01 after `:477`, `:518`, `:577`, `:609`, `:682`, `:844`; `integrator-audit-2026-09-30/README.md` "Outcome (added 2026-10-01)"; `.documents/paper-corrections-numerical-section.md` rows 1–9 and §§2–4.
- **Suite counts after this prompt are unchanged:** CosmologyModels 18, ComputeTargets 67, Datastore 17 (no code changed). Prompt 04 re-records them.
- **Issues:** `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]` is closed. Nine remain open, none assigned (index §1.6). The board's header says 3 of 4 landed.
- **The one estimate in §3.5.6** (about 3.5×10⁵ accepted steps for the `M = 1e-6`, β = 2 history, from the RHS-per-step ratio) is not a measurement. If prompt 04 runs that history it can replace it with the step count in an additive note; the log's figure is 3.33×10⁶ RHS from the audit (§3.7).
