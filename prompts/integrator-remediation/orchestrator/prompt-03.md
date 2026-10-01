# Orchestrator — prompt 03, documents and the paper's corrections

Read [`README.md`](README.md) and [`../README.md`](../README.md) §6.3 first. **You do not write
code.**

**The prompt:** [`../03-documents-and-paper-corrections.md`](../03-documents-and-paper-corrections.md)
**Board item:** D · **Closes:** `[00-paper-and-documents-describe-a-scheme-the-code-does-not-run]`

## 1. Before you dispatch

1. Prompts 01 and 02 landed; on branch `integrator-remediation`; `git status` clean; record
   `HEAD`.
2. Record `wc -l` of `.documents/numerical-strategies.md`, `.documents/architecture-summary.md`
   and `.documents/integrator-audit-2026-09-30/README.md`, and `md5` of each.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, template in `README.md`. Add: *"Your diff may touch
`.documents/numerical-strategies.md`, `.documents/architecture-summary.md`,
`.documents/integrator-audit-2026-09-30/README.md` (additions only, below new dated headings),
`.documents/paper-corrections-numerical-section.md` (new), the log, this campaign's board and
`.documents/OPEN_ISSUES.md`. Nothing else. `Paper1.tex` is outside this repository: read it, do
not edit it."*

## 3. The review — four checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **Additive.** `git diff --numstat HEAD~1 HEAD` shows zero deletions for the three existing
   documents, or deletions that are only blank lines at insertion points, which you confirm by
   reading the diff.
3. **Accuracy.** Pick three statements in the new `numerical-strategies.md` subsection (the cap
   formula, the floor rule, the exception table) and check each against the code on `HEAD` by
   reading the relevant lines. A statement that does not match the code is a fail.
4. **The corrections list.** Every bullet of the prompt's D4 list has a row with a quoted
   sentence, a `b1f64d8` fact, a `HEAD` fact, an audit citation and a suggested sentence.

Then the board: D done; the issue moved to §4; the index corrected.

## 4. Report, then stop

As `README.md` says.
