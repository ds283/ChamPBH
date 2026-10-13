# Prompt 04 — close

**Campaign:** [`README.md`](README.md) · **Board items:** **Z1**–**Z4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-13, with 01–03, after the user's rulings. Its orchestrator note, written after
03's review, hands it the counts and findings of 01–03, and governs where it differs from this file.
**Gate:**
- 01, 02 and 03 landed and reviewed, with the gates "Ray address", "Read-only plots" and "R" holding
  (board §2);
- on branch `read-only-plots`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** the campaign; no §3 issue. **Opens:** nothing expected.
**Recommended model:** **Sonnet**. Prose and records, and one final measurement.

**Read first:**

1. [`README.md`](README.md): §1, §2.2 (04's charter), §5, §7.
2. This board in full; logs 01, 02 and 03: "What shipped", "Observations not acted on" and "State
   handed to the next prompt".
3. `.documents/read-only-plots-verification.md` §1, which 03 created.
4. `.documents/architecture-summary.md`, the sections that name the drivers or the plot scripts:
   - the tree (`:29-30`);
   - §2's dated note on `pipeline_selection.py` and `extract_common.py` (`:132-142`);
   - §4.2's paragraph on the drivers' pools (`:270-275`);
   - §5.1, the argument parser.
5. `.documents/review-remediation-verification.md` §4.12 (the pattern, and its point 3: the run
   scripts' lines).
6. `CLAUDE.md`, "Repository mechanics"; [`prompts/INDEX.md`](../INDEX.md);
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md).
7. The user's run scripts, `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh` (untracked):
   read them, never edit, copy or run them.

---

## 1. What is wanted

The repository says what the campaign changed, and the user's next run knows what it must pass.

### 1.1 The items

- **Z1 — `.documents/architecture-summary.md`.** This file describes the code; it is not a dated
  record, but its existing `> **Note, …**` blocks are, and are kept byte for byte.
  - The tree gains `plot_inputs.py`, with a comment.
  - A dated note, in the form of the note at `:132-142`, says:
    - what `plot_inputs.py` holds, and why it exists;
    - that both plot scripts open their pools read-only, and what each kind of miss does (U3, U4);
    - that they refuse `--drop`.
  - §4.2's paragraph on the drivers' pools says the plot scripts' pools are read-only.
  - §5.1 is as 01 left it; check it.
- **Z2 — `CLAUDE.md`.** Check that 01 left "Ray on this machine" right. Add one sentence to
  "Repository mechanics": the plot scripts read a store read-only and stop if their options do not
  match the build's. Nothing else changes.
- **Z3 — the handover**, as `.documents/review-remediation-verification.md` **§4.13**, after §4.12,
  in its form. It says:
  - what the campaign did;
  - what it supersedes by statement: §4.12 point 3's rows for `--ray-address`, now enforced, and
    §4.12's description of the plot runs;
  - every driver call in the two run scripts that would now refuse to start, by line, and what it
    needs (`--ray-address 127.0.0.1:6380`; none for `--inventory`);
  - that the plot loop (`:54-57` / `:56-59`) must be given the options the build used, or it stops,
    and which options those are in the scripts;
  - that the science rebuild is held until `jordan-normalization` lands (the user's ruling of
    2026-10-13).
- **Z4 — the records.**
  - **This board closes.** Status line **COMPLETE**, with the final suites; every §1 row ✅; Z1–Z4;
    §2's gates; §3 and §4 as they stand. A final `### 5.5 After prompt 04 (<date>)` lists:
    - what is left to the user: 03's store under `~/ScienceData/ChamPBH-rehearsals/read-only-plots/`,
      the merge into `main` (P5), and the run scripts' lines (Z3);
    - the two behaviours README §1 recorded and did not open.
  - **`prompts/INDEX.md`**: the row **complete**, in `dk-and-rpk-migration`'s form, with the date,
    the final suites and the open-issue column from counting. The header's count and date.
  - **`.documents/OPEN_ISSUES.md`**: the header's description of this campaign (closed; one issue
    assigned and closed by 02; none open), and the count by counting. If §1.12 is empty after 02,
    it stays, saying so.

---

## 2. Verification

1. **The suites**, all four roots, serially, before and after: the counts 03 handed on.
2. **The documents are additive** where they are records: `git diff --numstat` on `.documents/`
   shows no deletions but in `architecture-summary.md`'s descriptive lines and in
   `OPEN_ISSUES.md`'s header. Extract every `> **Note, …**` block of `architecture-summary.md` at
   `HEAD` and after your edits, and `diff` them: identical but for the note Z1 adds.
3. **Every citation exists.** Every path, function, option and line Z1–Z3 name exists in the tree or
   in the run scripts; give the list in the log.
4. **The index**: `.documents/OPEN_ISSUES.md`'s count equals its rows by counting; `prompts/INDEX.md`
   agrees with this board.

## 3. Acceptance

1. Z1–Z4 as §1.1 says, and §2 in the log.
2. **No deliberate breakage**: 04 adds no code and no test.
3. **One commit**, with the log `logs/04-close.md` in the template of `dk-and-rpk-migration` README
   §5.1. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- A gate does not hold when you start.
- A sentence of a dated record would need rewriting rather than adding to.
- A count moves.
- Anything would need an edit to the run scripts, or outside §5.

## 5. What this prompt does not do

- **Files it changes:**
  - `.documents/architecture-summary.md`;
  - `CLAUDE.md`;
  - `.documents/review-remediation-verification.md` (§4.13 appended);
  - `.documents/OPEN_ISSUES.md`;
  - `prompts/INDEX.md`;
  - this board, and the log.
- No code, no test, no script, no store. It does not merge the branch.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
