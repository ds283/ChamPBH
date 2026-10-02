# Orchestrator — prompt 06b, the fixed-temperature values

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (the U6 amendment), §2 (g),
§2 (n), §6.7b first. **You do not write code.**

**The prompt:** [`../06b-fixed-T-values.md`](../06b-fixed-T-values.md)
**Board item:** V · **Closes:** `[00-fixed-T-values-are-not-stored]`

## 1. Before you dispatch

1. Prompt 06 has landed and `8fcb295` (the revert of prompt 07's first implementation) is in the
   history, with no unresolved miss. You are on branch `science-readiness` and `git status` is
   clean. Record `HEAD`.
2. Baselines:
   - the three suite counts and their wall-clocks;
   - **the driver on β = 2 at `M = 0.5` and `10⁻³`, and β = 1.6 at `10⁻⁵`, unloaded, one at a
     time.** Keep RHS, accepted steps, the wall-bounce count and the first bounce's `N`. This is
     the "no trajectory moved" reference.
3. `venv/bin/black --check ComputeTargets/ScalarModel.py Datastore/SQL/ObjectFactories/ScalarModel.py tools/history_and_bbn.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`ComputeTargets/ScalarModel.py`: the two constants, the namedtuple and the two functions;
>   `compute_scalar_model` after the loop; `ScalarModel`'s storage and property;*
> - *`Datastore/SQL/ObjectFactories/ScalarModel.py` (four columns);*
> - *`tools/history_and_bbn.py`;*
> - *`ComputeTargets/tests/` and `Datastore/tests/`;*
> - *the log; this campaign's board; and `.documents/OPEN_ISSUES.md`.*
>
> *Not `integrate_scalar_history`, `IntegrationResult`, the sampling loop, the policies,
> `extra_data`, `BBNData.py`, `plot_by_beta.py`, `extract_common.py`, or anything else under
> `.documents/`.*
>
> *If the prompt's stop conditions apply, stop and ask. Recording a deviation is not a
> substitute for stopping.*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The loop did not change.** There is no hunk inside `integrate_scalar_history`, the sampling
   loop, `ODEPolicy`, `HubblePolicy` or `ODERHS`.
3. **The measure.** Read `T_Jordan_crossing` and `fixed_T_values`. They use the steps' own
   interpolants and `brentq`, and they take the first crossing. The ratio is built as
   `compute_BBN_data` builds it.
4. **The tests, run by you.** (a)–(d) pass, including (d)'s read under `_do_not_populate`.
5. **The histories, run by you, unloaded.** On the three driver histories:
   - each temperature is crossed exactly once;
   - RHS, accepted steps, wall bounces and the first bounce's `N` are identical to your
     baseline.
6. **The stand-in.** The log records the sample-interpolated values beside the dense-output
   values on all three histories.
7. **Nothing is populated that was not before.** `grep -n _do_not_populate` in `main.py` and
   `plot_by_beta.py` shows the same lines as on `HEAD~1`.
8. **Suites.** All three pass and rise. `extra_data` keys are unchanged (read
   `build_extra_data`).

Then the board: V done; the issue closed; the index corrected.

## 4. Report, then stop

As `README.md` says. Name every prompt §5 stop condition you checked, and say whether it applies.
