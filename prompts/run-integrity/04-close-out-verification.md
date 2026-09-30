# Prompt 04 — Close-out verification and handover

**Campaign:** [`README.md`](README.md) · **Board:** `IMPLEMENTATION_STATE.md`. Close it.
**Recommended model:** **Sonnet**. No production code and no test changes. The work is
re-measuring, and writing an addendum a science run can act on.

**Read first:**

1. [`README.md`](README.md) §0, §6, §7.
2. `IMPLEMENTATION_STATE.md`, and the logs `logs/01-…`, `logs/02-…`, `logs/03-…`.
3. `.documents/review-remediation-verification.md` §4, including §4.6 (`production-readiness`),
   the handover this addendum amends.
4. `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`.

---

## 1. Re-measure

On the final tree, record the commit:

- **All three suites.** Counts, wall-clocks and results, against 18, 30 and 0 at `27a32bc`.
- **Every README §6.1–§6.3 row.** The final-tree value and its witness: the test that prints it,
  or the grep, run by you. Where a value differs from the one its prompt's log quotes, say by how
  much. **A row at or better than target in the log but not now is a stop.**
- **The scope check.** `git diff --stat 27a32bc..HEAD`. Every file must be one a prompt was
  allowed to touch: check against each prompt's §1 and §3, and the README's §0.4. Name any that
  is not.
- `grep -rn "VERSION_LABEL =" --include='*.py' .`, outside `venv/`, `thirdparty/` and
  `claude-context/`, and `grep -n PRYM_VERSION ComputeTargets/BBNData.py`.
- **The three planning probes, re-run.**
  - `datastore_version_probe.py` must now print "not returned" behaviour for [3] and [5], and no
    raise for [6]. It was written for `27a32bc`, so read its output rather than its captions.
  - `prymordial_solver_probe.py truncated` must now raise the new class. `nan` is unchanged,
    since the probe calls PRyMordial directly, bypassing the guard; say so.
  - `pairing_probe.py` reproduces the old logic and is unchanged by design.

## 2. Write

**A new section at the end of `.documents/review-remediation-verification.md` §4**, after §4.6,
headed with today's date and this campaign's name. It carries README §7's five points, each with
its evidence: commit, test, number. It supersedes, *by statement and not by edit*:

- §4.6 point 1's "still nothing stops an old store being reused";
- every earlier statement that `VERSION_LABEL` is `"2026.3.0"`.

**Nothing above it is changed** (CLAUDE.md rule 6).

**A short verification table in the same section:** each README §6 row, target, final value,
witness.

## 3. Close the board

- **The header.** COMPLETE, 3 of 3 plus the close-out, with the final SHA, and the rule: **every
  store made before 2026.4.0 is invalid, and a lookup no longer returns its rows.**
- **Your row** in §1.
- **§3 and §4** as they stand. Any issue a prompt opened stays open, and so does its index row.
- **`prompts/INDEX.md`.** Status complete, the dates, and the open-issue column.
- **`.documents/OPEN_ISSUES.md`.** Count and date. The five issues this campaign closed must be
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
3. The addendum states the five points of README §7.

## 6. Stop conditions — stop and ask the user

- Any README §6 row misses on the final tree.
- A file outside every prompt's allowed list is in `git diff 27a32bc..HEAD`.
- One of the five closed issues is still in the index; one of the three assigned ones has no
  **Resolved** line on the `review-remediation` board; or one of the two opened here is not in
  this board's §4.

## 7. The log

`logs/04-close-out-verification.md`, in the README §5.1 template. "State handed" names the
addendum's heading, and the one command that reproduces the whole verification.
