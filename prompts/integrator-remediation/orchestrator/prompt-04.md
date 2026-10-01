# Orchestrator — prompt 04, close-out verification and handover

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6, §7 first. **You do not
write code.**

**The prompt:** [`../04-close-out-verification.md`](../04-close-out-verification.md)

## 1. Before you dispatch

1. Prompts 01–03 landed; on branch `integrator-remediation`; `git status` clean; record `HEAD`.
2. Record the three suite counts and `md5` of `.documents/review-remediation-verification.md`,
   `prompts/INDEX.md`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, template in `README.md`. Add: *"Your diff may touch
`.documents/review-remediation-verification.md` (a new section after §4.7 only),
`prompts/INDEX.md` (this campaign's row), this campaign's board and log, and
`.documents/OPEN_ISSUES.md`. No production code, no tests, no other document. The physical-`M`
histories of your §1 take minutes; the β = 2 one must end on the budget, and you report its wall
time."*

## 3. The review — five checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **Additive.** `git diff --numstat HEAD~1 HEAD -- .documents/review-remediation-verification.md`:
   zero deletions.
3. **Every row.** The log has a final-tree value and witness for every §6.1–§6.3 row, and the nine
   histories' two sets of figures. Re-run two §6.1 (a) tests and one history yourself; they must
   match the log.
4. **The budget.** The log records the β = 2, `M = 4.1e-28` history ending with the budget message
   and `{"failure": True}`, and its wall time.
5. **The index.** `prompts/INDEX.md`'s row says complete, with the date and the final commit's
   subject; `.documents/OPEN_ISSUES.md`'s count equals the number of rows.

## 4. Report, then stop

The campaign is closed when this lands. Report as `README.md` says, and add the list of open
issues by name, so the user can decide what the next campaign owns.
