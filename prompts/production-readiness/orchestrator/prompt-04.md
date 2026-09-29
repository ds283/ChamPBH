# Orchestrator — prompt 04, close-out verification and handover

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6, §7 first. **You do not write
code, and neither does the agent.**

**The prompt:** [`../04-close-out-verification.md`](../04-close-out-verification.md)
**Board:** the campaign close

## 1. Before you dispatch

1. Prompts 01–03 landed and reviewed; branch; `HEAD`; `git status` clean.
2. Both suite counts and wall-clocks, recorded.
3. `git diff --stat 204795e..HEAD`. Keep it; you will check the agent's copy against it.
4. `grep -c "^- \`\[" .documents/OPEN_ISSUES.md` and the header count. Expect 11, not counting
   anything prompts 01–03 opened.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, template in `README.md`. Add: *"No production file and no
test may change. A defect found now is reported and opened on the board, not fixed. The addendum
to `.documents/review-remediation-verification.md` is added at the end of its §4; nothing above it
changes."*

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
   value is at or better than target. Spot-check three by running the witness yourself. One of
   them must be the prompt-03 bracket test, and one the prompt-02 network test.
4. **The addendum** carries README §7's five points:
   - the label and the fresh-database rule;
   - the full network;
   - where to read the reflection count;
   - what to expect of `AdiabaticHistory`, with numbers;
   - the open issues by name.

   It supersedes the three earlier passages by statement, not by edit.
5. **The board closes.**
   - The header is COMPLETE with the SHA.
   - `prompts/INDEX.md` shows the campaign complete.
   - The index count and date are correct, and none of the four closed issues is listed.
   - Each of the four has a Resolved line on the `review-remediation` board:
     ```bash
     grep -n "Resolved" prompts/review-remediation/IMPLEMENTATION_STATE.md
     ```
6. **The scope check.** The agent's `git diff --stat 204795e..HEAD` matches yours, and the log
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

Then stop. The production run is the user's.
