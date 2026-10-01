# Orchestrator — prompt 06, the spline floor

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (P8), §2 (j), §6.7 first.
**You do not write code.**

**The prompt:** [`../06-bbn-spline-floor.md`](../06-bbn-spline-floor.md)
**Board item:** L · **Closes:** `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]`
(assigned)

## 1. Before you dispatch

1. Prompt 05 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts and wall-clocks. The driver on β = 2 at `M = 0.5` and `10⁻³`,
   unloaded; keep the abundances.
3. `venv/bin/black --check ComputeTargets/BBNData.py`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`ComputeTargets/BBNData.py` (the default and its comment);*
> - *`ComputeTargets/tests/`;*
> - *the log; this campaign's board; the `review-remediation` board (a Resolved line only); and
>   `.documents/OPEN_ISSUES.md`.*

## 3. The review — four checks

1. **Allowed files**, and the `BBNData.py` hunk is the default and its comment only.
2. **The breakage, run by you.** Test (a)'s `1e-8` GeV case fails on `HEAD~1`, with the pre-check
   reason.
3. **The abundances.** The driver's two histories are within `1e-5` relative of your baseline, and
   the domain guard did not fire.
4. **Suites.** All three pass and rise.

Then the board: L done; the assigned issue closed by README §5 rule 4; the index corrected.

## 4. Report, then stop

As `README.md` says.
