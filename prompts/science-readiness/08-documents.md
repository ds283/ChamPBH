# Prompt 08 — Documents

**Campaign:** [`README.md`](README.md) · **Board item:** **D** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and D.
**Closes:** `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`, assigned from
the `review-remediation` board (README §5 rule 4), and `[00-documents-describe-the-thermo-route]`
on this board.
**Recommended model:** **Sonnet.** No production code and no test. Additions only.

**Read first:**

1. [`README.md`](README.md) §0, §2, §7.
2. Logs 01–07, all of them.
3. `.documents/numerical-strategies.md` §7 (all of it), `.documents/numerical-methods-for-paper.md`
   §4, `.documents/architecture-summary.md` §7.4 and its schema and CLI sections, and
   `.documents/paper-corrections-numerical-section.md`.
4. `CLAUDE.md` rule 6: verification documents and everything under `.documents/` are additive.

---

## 1. The changes — dated addenda, nothing above them edited

- **`numerical-strategies.md`**: a dated §7.6 (or the next free number) after §7's last
  subsection. It states:
  - that §7.2–§7.4 (asinh, sort, `sinh`) were superseded in review-remediation prompt 04;
  - the route as it now is: the ρ_NP ratio spline, the cell-mean `H_J²`, the window
    [0.2 keV, 100 MeV] and the 20 eV pre-check, the Hubble-only patch, the wall-clock limit and
    the output checks;
  - why `p_NP` and `T_NP` are gone;
  - in §3 (the sampling), a dated note that the cell means exist, what they are, and that the
    adiabatic stage does not read them.
- **`numerical-methods-for-paper.md`**: a dated addendum after §4 that replaces, by statement,
  the "Why the plasma is unaffected" argument with the Hubble-only route, and gives the planner's
  and the prompts' measured figures.
- **`architecture-summary.md`**: dated notes at §7.4 (`compute_BBN_data`'s description), at the
  `ScalarModel` and `BBNData` schema descriptions (columns added and removed), and at the CLI
  options list (`--phi-init-Mp`, `--bbn-wall-clock-limit`, `--band-half-width`).
- **`paper-corrections-numerical-section.md`**: new rows for every `NumericalSection` (or BBN
  section) sentence that now disagrees with the code: the new-physics pressure, the
  temperature-dependent new-physics fluid, the asinh splines, the BBN spline range. Each row has
  the measured fact and the log it comes from. Read `Paper1.tex` at the path in
  `.documents/paper-corrections-numerical-section.md`'s header if it is given there. Otherwise
  list the statements to check, and say so.
- **The vendored patches**: a dated paragraph in `numerical-strategies.md`'s addendum listing
  every `PRyM/` hunk this campaign made, from log 01. A PRyMordial upgrade re-applies them from
  that paragraph.

## 2. What this prompt does not do

No code, no test, no edit above any dated heading, no edit to `Paper1.tex`. Not the handover
(prompt 09).

## 3. Acceptance

`git diff --numstat` shows only additions in `.documents/` (0 deletions in every file). The board
and the index: D done, both issues closed.

## 4. Stop conditions — stop and ask the user

- A statement cannot be made true without editing text above a dated heading.

## 5. The log and the board

`logs/08-documents.md`, in the README §5.1 template, with the `--numstat`.
