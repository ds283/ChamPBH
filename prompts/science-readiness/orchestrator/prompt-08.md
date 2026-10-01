# Orchestrator — prompt 08, documents

Read [`README.md`](README.md) and [`../README.md`](../README.md) §1 D first. **You do not write
code.**

**The prompt:** [`../08-documents.md`](../08-documents.md)
**Board item:** D · **Closes:** `[00-documents-describe-the-thermo-route]` and
`[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` (assigned)

## 1. Before you dispatch

1. Prompt 07 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`.documents/numerical-strategies.md`, `.documents/numerical-methods-for-paper.md`,
>   `.documents/architecture-summary.md` and `.documents/paper-corrections-numerical-section.md`,
>   additively;*
> - *the log; this campaign's board; the `review-remediation` board (a Resolved line only); and
>   `.documents/OPEN_ISSUES.md`.*
>
> *No code and no test.*

## 3. The review — three checks

1. **Additive.** `git diff --numstat HEAD~1 HEAD -- .documents/`: zero deletions in every file
   except `OPEN_ISSUES.md`, which is an index.
2. **True.** Spot-check four statements against the code:
   - the route;
   - the cell means;
   - the floor;
   - the vendored-patch list against `git diff 6aaa706 -- PRyM/`.
3. **No code.** `git diff --stat` touches nothing outside `.documents/` and `prompts/`.

Then the board: D done; both issues closed; the index corrected.

## 4. Report, then stop

As `README.md` says.
