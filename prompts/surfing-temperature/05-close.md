# Prompt 05 — close

**Campaign:** [`README.md`](README.md) · **Board items:** **Z1**–**Z4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, with 01–04, after the user's rulings. Its orchestrator note, written after
04's review, hands it the counts and findings of 01–04. It governs where it differs from this file.
**Gate:**
- 01–04 landed and reviewed, with the gates "Roots", "Turning points" and "Scan" holding (board §2);
- on branch `surfing-temperature`, no tracked change in `git status`;
- you start no Ray, and open no store.

**Closes:** the campaign. **Opens:** nothing expected.
**Recommended model:** **Sonnet**. Prose and records, and the final suites.

**Read first:**

1. [`README.md`](README.md): §1, §2.2 (05's charter), §5, §7.
2. This board in full; logs 01–04: "What shipped", "Observations not acted on" and "State handed
   to the next prompt".
3. `.documents/surfing-temperature-verification.md` §1, which 04 created.
4. `.documents/architecture-summary.md`:
   - the tree (`:21-…`, where `extract_common.py` is at `:28`);
   - §2's dated notes on `extract_common.py` and `tools/` (`:132-142`);
   - §7.2, `ScalarModel` (`:648-750`), where `first_bounce` is described.
5. `.documents/review-remediation-verification.md` §4.13 (`:1466-`), the most recent handover: the
   form to follow.
6. `CLAUDE.md`, "Repository mechanics" (the test counts and their date); [`prompts/INDEX.md`](../INDEX.md);
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md).

---

## 1. What is wanted

The repository says what the campaign added, the user can re-run the scan from one command, and
the records are closed.

### 1.1 The items

- **Z1 — `.documents/architecture-summary.md`.** This file describes the code. It is not a dated
  record, but its existing `> **Note, …**` blocks are dated records, and are kept byte for byte.
  - **The tree** gains `tools/surfing_scan.py` and `tools/plot_surfing_temperature.py`, each with a
    comment.
  - **A dated note** in the form of the one at `:132-142` says:
    - what `find_kicks` and `reachability_roots` compute, and that they read the integrator's own w;
    - what `surfing_turning_points` finds, and that production does not call it;
    - what the two tools do, and that neither starts Ray or opens a store.
  - **§7.2** gains one sentence beside `first_bounce`: that `surfing_turning_points` walks the same
    interpolants, and why T_J's turning points are the crossings of β π/M_P = −1.
- **Z2 — `CLAUDE.md`.** Update the four suites' counts and their date in "Repository mechanics" to
  the final suites. Nothing else changes, unless a log 01–04 records a statement in it that the
  campaign has made false. Name each such statement in the log.
- **Z3 — the handover**, as `.documents/review-remediation-verification.md` **§4.14**, after §4.13,
  in its form. It says:
  - what the campaign did;
  - where the figure, the CSV and the verification document are;
  - **the command that re-runs the scan** at another φ* or M, or after `jordan-normalization`
    lands, with the plot command after it;
  - what that campaign would move: the roots and the capture not at all, the largest temperature,
    the second dip and delivery possibly (README §0.4);
  - that per-history surfing values are not stored, and that storing them would be a later
    campaign's `ScalarModel` columns on the science rebuild (README §7);
  - every issue the campaign opened, if any.
- **Z4 — the records.**
  - **The board closed.** The status line becomes "COMPLETE", with the commits of 01–04 and this
    one; every item and gate is ✅; a new `### 5.6 Close (<date>)`.
  - **`prompts/INDEX.md`.** The row becomes **complete**, with the landed commits, the
    verification document and the final suites, and the header count is corrected.
  - **`.documents/OPEN_ISSUES.md`.** Only if the campaign opened an issue: its rows and the header
    count and date. Otherwise it is untouched.

---

## 2. Verification

1. **The final suites**, serially, in the foreground, with the load: CosmologyModels, ComputeTargets
   in its two halves, ObjectFactories, tools. Each against the counts on the board. A count that
   falls is a stop.
2. **The handover's command, read and not run.** Its options are the ones 04's sidecar records.
3. `git diff --stat` of the commit: only §5's files.

## 3. Acceptance

1. Z1–Z4 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. **One commit**, with the log `logs/05-close.md` in the template of `dk-and-rpk-migration` README
   §5.1. Write "this commit" where the SHA goes.
4. **No merge.** The branch is merged into `main` after this prompt's review (P10), by the
   orchestrator with the user's go-ahead.

## 4. Stop conditions: stop and ask

- A suite count falls, or a test fails.
- A statement in `architecture-summary.md` or `CLAUDE.md` needs more than the changes listed in §1
  to stay true.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `.documents/architecture-summary.md`;
  - `CLAUDE.md` (the counts, and any statement Z2 names);
  - `.documents/review-remediation-verification.md` (a new §4.14 only);
  - `prompts/INDEX.md`;
  - `.documents/OPEN_ISSUES.md` (only if an issue opened);
  - this board, and the log.
- It changes no code, runs no history, starts no Ray, and opens no store.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
