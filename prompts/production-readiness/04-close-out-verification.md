# Prompt 04 — Close-out verification and handover

**Campaign:** [`README.md`](README.md) · **Board:** `IMPLEMENTATION_STATE.md`. Close it.
**Recommended model:** **Sonnet**. No production code and no test changes. The work is
re-measuring, and writing an addendum a production run can act on.

**Read first:**

1. [`README.md`](README.md) §0, §6, §7.
2. `IMPLEMENTATION_STATE.md`, and the logs `logs/01-…`, `logs/02-…`, `logs/03-…`.
3. `.documents/review-remediation-verification.md` §4, the handover this addendum amends.
4. `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`.

---

## 1. Re-measure

On the final tree, record the commit:

- **Both suites.** Counts, wall-clocks and results, against 12 and 13 at `204795e`.
- **Every README §6.1–§6.3 row.** The final-tree value and its witness: the test that prints it, or
  the grep, run by you, with `CHAMPBH_TEST_REPORT=1` where the tests support it. Where a value
  differs from the one its prompt's log quotes, say by how much. **A row at or better than target
  in the log but not now is a stop.**
- **The scope check.** `git diff --stat 204795e..HEAD`. Every file must be one a prompt was allowed
  to touch. Check against each prompt's §1 and the README's §0.4. Name any that is not.
- `grep -n VERSION_LABEL main.py plot_by_beta.py`.
- **The planning probe,** re-run. Its Table 1 must match prompt 03's re-measured Table 1 to the
  digits printed.

## 2. Write

**A new section at the end of `.documents/review-remediation-verification.md` §4**, headed with
today's date and this campaign's name. It carries README §7's five points, each with:

- its evidence: commit, test, number;
- for point 4, the bracket's range and the matter-limit check from log 03.

It supersedes, *by statement and not by edit*, three things in the earlier handover:

- §4.2 item 4's caption caution;
- §4.3's "decide which network";
- §4.5's H5 and network bullets.

**Nothing above it is changed** (CLAUDE.md rule 6).

**A short verification table in the same section:** each README §6 row, target, final value,
witness.

## 3. Close the board

- **The header.** COMPLETE, 3 of 3 plus the close-out, with the final SHA, and the fresh-database
  rule: **every store made before 2026.3.0 is invalid**.
- **Your row** in §1.
- **§3 and §4** as they stand. Any issue a prompt opened stays open. Its index row stays in
  `.documents/OPEN_ISSUES.md`, under this board.
- **`prompts/INDEX.md`.** Status complete, the dates, and the open-issue column.
- **`.documents/OPEN_ISSUES.md`.** Count and date. The four issues this campaign closed must be
  gone from it. If one is not, that is a stop.

## 4. What this prompt does not do

- No production file and no test changes. **A defect found now is reported and opened on the
  board, not fixed.**
- No rewriting of anything under `.documents/`, including the earlier handover.
- No pipeline run.

## 5. Acceptance

1. `git diff --stat HEAD~1 HEAD` shows only these files:
   - `.documents/review-remediation-verification.md` (additions only);
   - the board;
   - `prompts/INDEX.md`;
   - `.documents/OPEN_ISSUES.md`;
   - the log.
2. Every README §6 row is present with a final value at or better than target.
3. The addendum states the label, the network, where the reflection count is read, and what to
   expect of `AdiabaticHistory`.

## 6. Stop conditions — stop and ask the user

- Any README §6 row misses on the final tree.
- A file outside every prompt's allowed list is in `git diff 204795e..HEAD`.
- One of the four closed issues is still in the index, or its `review-remediation` board entry has
  no **Resolved** line.

## 7. The log

`logs/04-close-out-verification.md`, in the README §5.1 template. "State handed" names the
addendum's heading, and the one command that reproduces the whole verification.
