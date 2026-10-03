# Orchestrator — prompt 01b, the kick threshold with Σ_eff

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U2), §2 (h), §6.1b first.
**You do not write code.**

**The prompt:** [`../01b-kick-threshold-sigma-eff.md`](../01b-kick-threshold-sigma-eff.md)
**Board item:** B · **Closes:** `[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]`

## 1. Before you dispatch

1. Prompt 01 landed with no unresolved miss (01b does not depend on it, but the campaign runs in
   order). On branch `bbn-tolerance`; `git status` clean apart from the user's untracked run
   files; record `HEAD`.
2. The three suite counts.
3. `venv/bin/black --check extract_common.py plot_by_beta.py ComputeTargets/tests/test_extraction.py`.
   (`plot_by_beta.py` may not be clean already; record it.)

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch only `extract_common.py` (`kick_threshold_curve` and the label in
> `plot_T_deliver`), `plot_by_beta.py` (the comment above the call), `ComputeTargets/tests/test_extraction.py`,
> the log, this campaign's board and `.documents/OPEN_ISSUES.md`.*

## 3. The review — four checks

1. **Allowed files.** The `plot_by_beta.py` hunk is a comment only. The `extract_common.py` hunks
   are the function and the label only.
2. **The breakage, run by you.** Check out `extract_common.py` from `HEAD~1`. Tests (d) and (d2)
   must fail on the values: (d) on w = 7/23 giving 1.958, and (d2) on a minimum of 1.0295. Restore
   it, and confirm `git status` is clean.
3. **The formula.** Read `kick_threshold_curve`: it computes √((2 + Σ)/(6Σ)) with Σ = 1 − 3w from
   the cosmology passed in, and omits Σ ≤ 0.
4. **Suites.** All three pass; ComputeTargets has risen by one; `black --check` is clean on the
   files changed.

Then the board: B done; the issue closed; the index corrected.

## 4. Report, then stop

As `README.md` says.
