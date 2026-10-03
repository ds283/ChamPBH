# Orchestrator — prompt 03, documents and close-out

> **Do not run (U3, 2026-10-03).** Revised with prompt 02's rewrite.

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (f), §6.3, §7 first. **You do
not write code.**

**The prompt:** [`../03-documents-and-close-out.md`](../03-documents-and-close-out.md)
**Board item:** D · **Closes:** the campaign

## 1. Before you dispatch

1. Prompt 02 landed with no unresolved miss. On branch `bbn-tolerance`; `git status` clean apart
   from the user's untracked run files; record `HEAD`.
2. The three suite counts.
3. The store's mtimes.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch only `.documents/numerical-strategies.md`,
> `.documents/numerical-methods-for-paper.md`,
> `.documents/paper-corrections-numerical-section.md`,
> `.documents/review-remediation-verification.md` (all additively), the log, this campaign's
> board, `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`. No code.*

## 3. The review — five checks

1. **Allowed files**, and no Python file changed.
2. **Additive.** `git diff --numstat HEAD~1 -- .documents/` shows no deletions, except in
   `OPEN_ISSUES.md`'s header lines.
3. **The roster.** The log's 17 figures equal log 02's. Re-run two yourself: the control, and
   β = 1.6 at M = 10⁻⁵.
4. **The handover.** §4.11 carries README §7's six points. Its refresh commands name
   `--drop bbn-data` on a **copy** of the store, not `--retry-failed-bbn`, and no `VERSION_LABEL`
   bump.
5. **Suites** unchanged from prompt 02; the store's mtimes unchanged.

Then the board: D done, the status **COMPLETE**; `prompts/INDEX.md` row complete; the index header
corrected.

## 4. Report, then stop

As `README.md` says. Remind the user that the refresh of the science store (§4.11) is theirs to
run, and that the branch is ready to merge.
