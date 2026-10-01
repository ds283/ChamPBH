# Orchestrator — prompt 02, `ScalarModel` failure reasons

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (f), §6.3 first. **You do not
write code.**

**The prompt:** [`../02-scalarmodel-failure-reasons.md`](../02-scalarmodel-failure-reasons.md)
**Board item:** F · **Closes:** `[00-scalarmodel-failure-rows-carry-no-reason]` (assigned)

## 1. Before you dispatch

1. Prompt 01 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts (from log 01) and wall-clocks.
3. `venv/bin/black --check ComputeTargets/ScalarModel.py Datastore/SQL/ObjectFactories/ScalarModel.py main.py plot_by_beta.py`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`ComputeTargets/ScalarModel.py` (the two failure returns, `ScalarModel`'s reason);*
> - *`Datastore/SQL/ObjectFactories/ScalarModel.py` (one column);*
> - *`main.py` (the summary);*
> - *`plot_by_beta.py` (the drop report);*
> - *`ComputeTargets/tests/` and `Datastore/tests/`;*
> - *the log; this campaign's board; the `integrator-remediation` board (a Resolved line only);
>   and `.documents/OPEN_ISSUES.md`.*
>
> *Not the step loop, the sampling, BBN, or `.documents/`.*

## 3. The review — six checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The column.** One nullable `String(DEFAULT_STRING_LENGTH)` column, written in `store` and
   read in `build`. No other schema hunk.
3. **The stand-in on `HEAD~1`, run by you.** With the old factory and `ScalarModel.py`, test
   (a)'s row reads back with no reason. The log shows the same.
4. **Both exits.** `git diff` shows a reason on both `{"failure": True}` returns; `grep -n
   '"failure": True}' ComputeTargets/ScalarModel.py` finds none without a reason.
5. **No trajectory change.** No hunk in `integrate_scalar_history`, `ODEPolicy` or `ODERHS`.
6. **Suites.** All three pass and rise.

Then the board: F done; the assigned issue closed by README §5 rule 4; the index corrected.

## 4. Report, then stop

As `README.md` says.
