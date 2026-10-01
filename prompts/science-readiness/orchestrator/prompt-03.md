# Orchestrator — prompt 03, the first bounce

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U4, P5), §2 (g), §6.4
first. **You do not write code.**

**The prompt:** [`../03-first-bounce.md`](../03-first-bounce.md)
**Board item:** T · **Closes:** `[00-first-bounce-is-not-stored]`

## 1. Before you dispatch

1. Prompt 02 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts and wall-clocks. **The driver on β = 2 at `M = 0.5` and
   `10⁻³`, and β = 1.6 at `10⁻⁵`, unloaded, one at a time.** Keep RHS, accepted steps and the
   wall-bounce counts. This is the "no trajectory moved" reference.
3. `venv/bin/black --check ComputeTargets/ScalarModel.py Datastore/SQL/ObjectFactories/ScalarModel.py tools/history_and_bbn.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`ComputeTargets/ScalarModel.py` (the new function and type; `compute_scalar_model` after the
>   loop; `ScalarModel`'s storage and property);*
> - *`Datastore/SQL/ObjectFactories/ScalarModel.py` (four columns);*
> - *`tools/history_and_bbn.py`;*
> - *`ComputeTargets/tests/` and `Datastore/tests/`;*
> - *the log; this campaign's board; and `.documents/OPEN_ISSUES.md`.*
>
> *Not `integrate_scalar_history`, `IntegrationResult`, the sampling loop, `extra_data`, BBN, or
> `.documents/`.*

## 3. The review — seven checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The loop did not change.** No hunk inside `integrate_scalar_history`, the sampling loop,
   `ODEPolicy`, `HubblePolicy` or `ODERHS`.
3. **The measure.** Read `first_bounce`. It uses the step's own interpolant and `brentq`, and
   handles a reflection that comes first. It applies no `φ < 1.5 M` filter, unless P5 was
   overruled.
4. **The tests, run by you.** (a)–(e) pass. (a)'s figures match README §6.4.
5. **The histories, run by you, unloaded.** The driver on the three histories:
   - the first bounces agree with §4.8 to `1e-5` in `N`;
   - RHS, steps and bounces are identical to your baseline.
6. **The stand-in.** The log records the sample-based detector's value beside the dense-output
   value at β = 2, `M = 10⁻³`.
7. **Suites.** All three pass and rise. `extra_data` keys are unchanged (read
   `build_extra_data`).

Then the board: T done; the issue closed; the index corrected.

## 4. Report, then stop

As `README.md` says.
