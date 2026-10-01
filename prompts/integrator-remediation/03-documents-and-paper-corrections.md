# Prompt 03 — Documents and the paper's corrections

**Campaign:** [`README.md`](README.md) · **Board item:** **D** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and D.
**Closes:** `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]` on this board.
**Recommended model:** **Sonnet**. No production code and no tests. The work is describing the
final shape of the integrator accurately, additively, and listing for the authors every sentence
of the paper's numerical section that the code now contradicts, with the measured fact beside it.

**Read first:**

1. [`README.md`](README.md) §0, §2, §6.3.
2. `logs/01-…` and `logs/02-…`: the names, the parameters, the exception table as implemented.
3. `.documents/numerical-strategies.md` §2–§3 (`:69–176` on `2b89022`) and
   `.documents/architecture-summary.md` at `:475`, `:518`, `:568–577`, `:605–608`, `:671–676`,
   `:844` (the lines the audit found describing fragments, regions, the fallback and the hard
   reflection; re-find them on `HEAD`).
4. `.documents/integrator-audit-2026-09-30/README.md` §1 (the paper–code mismatches), §3.5,
   §3.7, §9, §11.
5. `Paper1.tex` is **not in this repository** (it is at
   `/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/Paper1.tex`, `NumericalSection`
   at `\label{NumericalSection}`, the paragraphs "Stiffness" and "Resolving the reflections",
   about `:2930–3010`). Read it; do not edit it.
6. `CLAUDE.md` rule 6: verification documents are additive.

---

## 1. The changes

**D1 — `.documents/numerical-strategies.md`.** A new subsection after §3.4, headed with the date
and this campaign's name, saying that §2.1 (the fallback cascade), §2.2's note on the potentials'
tolerance overrides, and §3.1–§3.4 (events, fragments, clamping, hard reflection, fragment
failsafes) describe the code before `VERSION_LABEL 2026.5.0`, and then describing what replaced
them: the pure loop and its signature; the kinematic cap with the two terms and the bound they
enforce; the floor and the elastic reflection, when each applies, and the agreement the audit
measured; the Jacobian clamp and the mechanism it defeats; the exception table of README §2 (h)
as implemented; the step budget and why it exists; what is stored per history. Cite the audit
README's sections for every number. **Change nothing above the new heading.**

**D2 — `.documents/architecture-summary.md`.** At each of the lines in "Read first" 3, a dated
one- or two-sentence note saying what the described mechanism became (or that it is gone), with
a pointer to D1. Where a code excerpt is quoted (the `SolutionFragment` type, the
`bounce_region_*` properties), say the type is gone, or that the properties are defined but
unread since 2026.5.0.

**D3 — the audit's outcome.** At the end of `.documents/integrator-audit-2026-09-30/README.md`,
a dated "Outcome" subsection: this campaign, the commits of prompts 01 and 02 by subject and SHA,
which §9 and §10 items landed and which remain open (by issue name). Nothing above it changes.

**D4 — the paper corrections.** A new file `.documents/paper-corrections-numerical-section.md`.
For each sentence or clause of `NumericalSection` that disagrees with the code after this
campaign, a row: the sentence (quoted, with its approximate line), what the code did on
`b1f64d8`, what it does now, the measurement that shows it (audit section and script), and a
suggested replacement sentence. At least these:

- "Outside both regions the maximum step is of order `10⁻²` e-folds": the code had `inf`; now a
  global cap of `0.1` e-folds and the kinematic cap.
- "Inside the outer region it is reduced to `10⁻⁵` e-folds, and inside the inner region to
  `10⁻⁶`": the code had `3e-3 M` and `1e-4 M`; now there are no regions.
- "Two nested regions … the integration is halted when `φ` crosses either boundary … restarted":
  gone.
- "If the integration fails, it is retried … backward differentiation … automatically switching
  … Dormand–Prince as a last resort": never wired; now one stepper.
- "For very steep potentials, `M/M_P ≲ 10⁻³`, these tolerances are relaxed to `10⁻⁵` and
  `10⁻⁶`": never in the production path; `1e-8` throughout, and the audit's §3.6 measurement that
  the relaxation buys nothing inside a cap.
- "a tolerance of `10⁻⁸` drives the step size below the point at which the integration can make
  progress": not reproduced (audit §3.6, §9.3); what does limit progress is the representable
  step at the wall for `M ≲ 1e-8`, handled by the elastic reflection.
- Any sentence describing the hard reflection as a failure fallback: it is now a deliberate
  model, with its trigger and its validity range (audit §3.5, §3.7).
- The settling at physical `M` (audit §3.7): not a correction but a statement the paper may need,
  that histories at `M ≲ 1e-10` with β ≥ 1.2 require a parked-tracking model the code does not
  yet have.

The file also records the two statements the audit confirmed and strengthened that the paper
may wish to carry: `T_J` rises by 12–13 % during the surfing overshoot (audit §8 F1), and the
first-bounce temperature is reached after that rise.

---

## 2. Tests

None. This prompt adds no code.

## 3. What this prompt does not do

- No production code, no tests, no edit to `Paper1.tex`, no edit to any line above a new dated
  heading in any document.
- No edit to the campaign READMEs of earlier campaigns.

## 4. Acceptance

1. README §6.3, every row.
2. `git diff --stat HEAD~1 HEAD` touches only the four documents of §1, the log, the board and
   the index.
3. For each document, `git diff HEAD~1 HEAD -- <file>` shows additions only (no `-` lines other
   than at the insertion points' blank lines, if any). Quote the `--numstat`.
4. The board and the index: D done; the issue closed.

## 5. Stop conditions — stop and ask the user

- A statement in `numerical-strategies.md` §1 or §4–§9 (outside the integrator sections) turns
  out to be wrong for the final tree: record it as an observation and an issue, do not fix it.
- Log 01 or 02 leaves a name or a number you need undefined.

## 6. The log and the board

`logs/03-documents-and-paper-corrections.md` in the README §5.1 template.
