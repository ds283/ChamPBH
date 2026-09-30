# Orchestrator — prompt 04, close-out verification and handover

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6, §7 first. **You do not write
code, and neither does the agent.**

**The prompt:** [`../04-close-out-verification.md`](../04-close-out-verification.md)
**Board:** the campaign close

## 1. Before you dispatch

1. Prompts 01–03 landed and reviewed; branch; `HEAD`; `git status` clean.
2. All three suite counts and wall-clocks, recorded.
3. `git diff --stat 27a32bc..HEAD`. Keep it; you will check the agent's copy against it.
4. `grep -c "^- \`\[" .documents/OPEN_ISSUES.md` and the header count. Expect 15 at planning,
   less the five this campaign closes, plus anything prompts 01–03 opened.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, template in `README.md`. Add: *"No production file and no
test may change. A defect found now is reported and opened on the board, not fixed. The addendum
to `.documents/review-remediation-verification.md` is added at the end of its §4, after §4.6;
nothing above it changes."*

## 3. The review — six checks

1. **Diff contents.** `git diff --stat HEAD~1 HEAD` shows only these files:
   - `.documents/review-remediation-verification.md`;
   - this campaign's board;
   - `prompts/INDEX.md`;
   - `.documents/OPEN_ISSUES.md`;
   - the log.
2. **Additive.** `git diff HEAD~1 HEAD -- .documents/review-remediation-verification.md` has no
   removed lines.
3. **Every README §6 row is present** in the table with a final-tree value and a witness, and each
   value is at or better than target. Spot-check three by running the witness yourself: the
   prompt-01 cross-label test, the prompt-02 forced-failure test, and the prompt-03 helper test.
4. **The addendum** carries README §7's five points:
   - the label, defined once, and that old rows are no longer returned;
   - failure detection and the boundary;
   - the retry rule and where to read the reasons;
   - the pairing fix;
   - what is still open, by name.

   It supersedes §4.6 point 1's "nothing stops an old store being reused" by statement, not by
   edit.
5. **The board closes.**
   - The header is COMPLETE with the SHA.
   - `prompts/INDEX.md` shows the campaign complete.
   - The index count and date are correct, and none of the five closed issues is listed.
   - Each of the three assigned ones has a Resolved line on the `review-remediation` board:
     ```bash
     grep -n "Resolved" prompts/review-remediation/IMPLEMENTATION_STATE.md
     ```
   - Both planning issues are in this board's §4.
6. **The scope check.** The agent's `git diff --stat 27a32bc..HEAD` matches yours, and the log
   names no out-of-scope file, or names and explains each one.

## 4. Stop and ask the user

- Any README §6 row misses on the final tree.
- A production file or a test is in the diff.
- A removed line in the verification document.

## 5. After it lands

Report:

- the commit;
- the verification table verbatim;
- the addendum's five points in one line each;
- the final open-issue count.

Then stop. The science run is the user's.
