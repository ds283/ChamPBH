# Orchestrator — prompt 05, bounce averages

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U3, P7), §2 (i), §6.1,
§6.6 first. **You do not write code.**

**The prompt:** [`../05-bounce-averages.md`](../05-bounce-averages.md)
**Board item:** A · **Closes:** `[00-bbn-input-is-aliased-at-small-M]`; narrows
`[00-stored-samples-alias-the-rebounds]` (assigned)

## 0. What makes this prompt unusual

It changes what BBN reads at every `M`, and it adds cost to every history. The review is four
things:

- the noise below 3 keV falls (the breakage witness);
- no point value and no trajectory moved;
- the cost stays inside 1.5×;
- the `N`-average is a good proxy for the time-average.

## 1. Before you dispatch

1. Prompt 04 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts and wall-clocks. **The driver on β = 2 at `M = 0.5`, `10⁻³`
   and `10⁻⁵`, and β = 1.6 at `10⁻⁵`, unloaded, one at a time.** Keep:
   - RHS, steps and the first bounce;
   - the history's wall time;
   - the `ratio` windows;
   - the BBN outcome and abundances.

   The `ratio` windows below 3 keV are the breakage record. Expect them to match README §6.1.
3. `venv/bin/black --check ComputeTargets/ScalarModel.py ComputeTargets/BBNData.py Datastore/SQL/ObjectFactories/ScalarModel.py tools/history_and_bbn.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`ComputeTargets/ScalarModel.py` (the sampling loop and the new pure averaging function;
>   `SampleValues`, `ScalarModelValue`, `ScalarModel.store()`);*
> - *`ComputeTargets/BBNData.py` (the `density_NP` line);*
> - *`Datastore/SQL/ObjectFactories/ScalarModel.py` (two value columns);*
> - *`tools/history_and_bbn.py`;*
> - *`ComputeTargets/tests/` and `Datastore/tests/`;*
> - *the log; this campaign's board; the `integrator-remediation` board (a Narrowed line only);
>   and `.documents/OPEN_ISSUES.md`.*
>
> *Not `integrate_scalar_history`, the point sample fields' values, `AdiabaticHistory`, the spline
> floor, or `.documents/`.*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **No point value moved.** Test (d) passes when you run it. No hunk changes how an existing
   `SampleValues` field is computed.
3. **The breakage, run by you, unloaded.** The averaged `ratio` windows' rms step at β = 1.6,
   `M = 10⁻⁵` is ≤ 1/10, and at β = 2, `M = 10⁻³` ≤ 1/2, of your §1.2 record. β = 1.6 at
   `M = 10⁻⁵` still completes BBN, inside the output checks.
4. **No trajectory moved.** RHS, steps and first bounce on all four histories are identical to
   your baseline.
5. **The cost.** The β = 2, `M = 10⁻⁵` wall time against your baseline is ≤ 1.5×. The log quotes
   the evaluation count.
6. **The resolved case.** At β = 2, `M = 0.5`, D/H and Yp moved by ≤ 3e-4 relative.
7. **`N` against time.** The log quotes acceptance 5's figure. Above `1e-3` it is a stop.
8. **Suites.** All three pass and rise. `AdiabaticHistory.py` is untouched.

Then the board: A done; the issue closed; the assigned issue narrowed (a dated line on its board;
the index row's hook corrected, not deleted); the index counts corrected.

## 4. Report, then stop

As `README.md` says.
