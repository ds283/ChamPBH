# Orchestrator — prompt 09, close-out

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6, §7 first. **You do not write
code.**

**The prompt:** [`../09-close-out-verification.md`](../09-close-out-verification.md)

## 1. Before you dispatch

1. Prompts 01–08 landed with no unresolved miss; on branch `science-readiness`; `git status`
   clean; record `HEAD`.
2. Baselines: the three suite counts and wall-clocks.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`.documents/review-remediation-verification.md`, additively;*
> - *`.documents/OPEN_ISSUES.md`;*
> - *`prompts/INDEX.md` (this campaign's row);*
> - *the log; and this campaign's board.*
>
> *No production code and no test. Run the roster one history at a time, with nothing else
> running.*

## 3. The review — five checks

1. **No code.** `git diff --stat HEAD~1 HEAD`: only the allowed files.
2. **Additive.** `git diff --numstat HEAD~1 HEAD -- .documents/review-remediation-verification.md`:
   zero deletions.
3. **The roster.** Re-run two roster histories yourself, unloaded: β = 1.6, `M = 10⁻⁵`, and one
   other. Their RHS, first bounce and abundances match the log to the printed digits.
4. **The scope check.** The log names every file in `git diff --stat 6aaa706..HEAD` and the prompt
   that touched it.
5. **The board.** COMPLETE, nine of nine. Every issue this campaign opened is resolved or carries
   a dated line. The index counts are right.

## 4. Report, then stop

As `README.md` says. Remind the user of README §7 point 1: the science run needs a fresh datastore
file.
