# Orchestrator — prompt 06, close-out verification and handover

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6, §7 first. **You do not write
code, and neither does the agent.**

**The prompt:** [`../06-close-out-verification.md`](../06-close-out-verification.md)
**Board item:** R4 (verification), the campaign close

## 1. Before you dispatch

1. Prompts 01–05 landed and reviewed; branch; `HEAD`; `git status` clean.
2. Both suite counts and wall-clocks, recorded.
3. `git diff --stat f5896bb..HEAD` — keep it; you will check the agent's copy against it.

## 2. Dispatch

One fresh-context subagent, Opus, template in `README.md`. Add: *"No production file and no test
may change. A defect found now is reported, not fixed."*

## 3. The review — six checks

1. **Diff contents.** `git diff --stat HEAD~1 HEAD` shows only
   `.documents/review-remediation-verification.md` (new), one added line at the top of
   `.documents/audit-2026-09-29/README.md`, `prompts/INDEX.md`, the board, the index, and the log.
2. **Every §6 row is present** in the before/after table with the final-tree value and a witness,
   and every value is at or better than target. Spot-check three by running the witness yourself.
3. **The handover** contains the fresh-database rule with the label, the run list, the expected
   magnitudes (+8.5 % D/H at r = 0.08; ratio ≈ 0.08 at 1 MeV for β = 2), the figure-provenance
   question, and the seeded issues by name.
4. **Additive.** The audit README's diff is one added line.
5. **The board closes**: header COMPLETE 6 / 6 with the SHA; R4 done; `prompts/INDEX.md` status
   complete; index count and date correct.
6. **The log** names the verification document in "State handed".

## 4. Stop and ask the user

- Any §6 row misses on the final tree.
- A production file or test is in the diff.

## 5. After it lands

Report: the commit; the before/after table verbatim; the handover's run list. Then stop. The
numerical campaign is planned by the user from the handover.
