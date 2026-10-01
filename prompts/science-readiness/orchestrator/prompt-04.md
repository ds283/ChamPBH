# Orchestrator — prompt 04, the initial field option

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (P6), §2 (h), §6.5 first.
**You do not write code.**

**The prompt:** [`../04-initial-field-option.md`](../04-initial-field-option.md)
**Board item:** P · **Closes:** `[00-initial-field-value-is-hard-coded-and-unchecked]` (assigned)

## 1. Before you dispatch

1. Prompt 03 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts and wall-clocks.
   `grep -n "5.0 \* units.PlanckMass" main.py plot_by_beta.py plot_ScalarModel.py` (three lines).
3. `venv/bin/black --check config/argument_parser.py main.py plot_by_beta.py plot_ScalarModel.py pipeline_selection.py tools/history_and_bbn.py`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`config/argument_parser.py`;*
> - *`main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` (the φ\* line; in `main.py` also the
>   warning, which must not change the coupling array);*
> - *`pipeline_selection.py`;*
> - *`tools/history_and_bbn.py`;*
> - *`ComputeTargets/tests/`;*
> - *the log; this campaign's board; the `review-remediation` board (a Resolved line only); and
>   `.documents/OPEN_ISSUES.md`.*
>
> *Not the YAML files, the factories, or `.documents/`.*

## 3. The review — five checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **No literal left.** Your grep from §1.2 finds nothing. Test (b) fails on `HEAD~1` when you run
   it with the three drivers checked out from there.
3. **The arithmetic, and nothing dropped.** Test (a)'s sets match README §6.5; test (d) passes.
   `git diff` shows no hunk that removes from, filters or reorders `main.py`'s coupling array. The
   log lists which β the check warns about for each YAML file.
4. **Reproduction.** The driver with `--phi-init-Mp 5` on β = 2, `M = 0.5` is identical to log 03.
5. **Suites.** All three pass and rise.

Then the board: P done; the assigned issue closed by README §5 rule 4; the index corrected.

## 4. Report, then stop

As `README.md` says.
