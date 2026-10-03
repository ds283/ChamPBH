# Orchestrator — prompt 03, documents and close-out

> **Revised 2026-10-03 with prompt 02's rewrite, after the user's ruling U4 on log 01c.** The
> first version is in `git log` (this file at `f3fa41a`).

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U3, U4), §2 (f), §6.3, §7
first. **You do not write code.**

**The prompt:** [`../03-documents-and-close-out.md`](../03-documents-and-close-out.md)
**Board item:** D · **Closes:** the campaign

## 1. Before you dispatch

1. Prompts 02, 02b (added 2026-10-03, U5) and 01b landed with no unresolved miss. On branch
   `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record `HEAD`.
2. The three suite counts.
3. The store's mtimes (with `/usr/bin/stat`).

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md` with `NN-<name>` =
`03-documents-and-close-out`. Add:

> *Your diff may touch only `.documents/numerical-strategies.md`,
> `.documents/numerical-methods-for-paper.md`,
> `.documents/paper-corrections-numerical-section.md`,
> `.documents/review-remediation-verification.md` (all additively), the log and `logs/03-probes/`,
> this campaign's board, `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`. No code. Do not run
> `main.py` or touch any store.*

## 3. The review — five checks

1. **Allowed files**, and no Python file changed outside `logs/03-probes/`.
2. **Additive.** `git diff --numstat HEAD~1 -- .documents/` shows no deletions, except in
   `OPEN_ISSUES.md`'s header and rows.
3. **The roster.** The log's 17 small-network `prod` figures equal log 02's. Re-run two yourself
   with `--small-network`: the control, and β = 1.6 at M = 10⁻⁵.
4. **The handover.** §4.11 carries README §7's six points.
   - Its refresh commands name `--drop bbn-data` on a **copy** of the store. They do not use
     `--retry-failed-bbn`, and do not bump `VERSION_LABEL`.
   - It says that every BBN row moves by the network offset.
   - It says that ⁷Li/H from the small network is not used.
   - It quotes the warning a store that was not refreshed prints.
5. **Suites** unchanged from prompt 02; the store's mtimes unchanged.

Then the board: D done, and the status **COMPLETE**. §3 names what stays open. The
`prompts/INDEX.md` row reads complete, and the index header is corrected.

## 4. Report, then stop

As `README.md` says. Remind the user that the refresh of the science store (§4.11) is theirs to
run, and that the branch is ready to merge.
