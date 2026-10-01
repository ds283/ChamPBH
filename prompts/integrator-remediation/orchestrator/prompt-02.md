# Orchestrator — prompt 02, the fallback and the exception taxonomy

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (g)–(i), §6.2 first.
**You do not write code.**

**The prompt:** [`../02-fallback-and-exceptions.md`](../02-fallback-and-exceptions.md)
**Board items:** B, C, S, X (RHS) · **Closes:** four `[00-…]` issues named in the prompt's header

## 0. What makes this prompt unusual

It is deletions plus one behavioural change to the RHS on unphysical states. The review is that
no physical value moved, which log 01's figures make checkable to `1e-10`.

## 1. Before you dispatch

1. Prompt 01 landed with no unresolved miss; on branch `integrator-remediation`; `git status`
   clean; record `HEAD`.
2. Baselines: the three suite counts (from log 01) and wall-clocks.
3. Record `grep -n "RuntimeError" ComputeTargets/ScalarModel.py`, `grep -n "print_tb"
   Quadrature/supervisors/base.py`, and `grep -c "solver_list" ComputeTargets/ScalarModel.py`.
4. `venv/bin/black --check ComputeTargets/ScalarModel.py Quadrature/supervisors/base.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`ComputeTargets/ScalarModel.py` (the fallback wrapper, the `RuntimeError` sites,
`ODEPolicy._get_T_Jordan`, `ODERHS`'s diagnostic branch, the loop's budget field and check);
`Quadrature/supervisors/base.py` (`RHS_timer.__exit__` only); `ComputeTargets/tests/` (one new
module); the log; this campaign's board; and `.documents/OPEN_ISSUES.md`. Not the loop's cap,
floor, clamp or reflection; not any other `ODEPolicy` or `PotentialDerivativePolicy` method; not
`main.py`, the plotting scripts, the potentials, the datastore, or `.documents/`."*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The breakage, run by you.** Generic procedure with `T` = the new module and
   `F` = `ComputeTargets/ScalarModel.py`: on `HEAD~1`, (a) fails with `AttributeError` and (b)
   fails because `_get_T_Jordan` returns. Restore; everything passes.
3. **No physical value moved.** Test (c)'s two sets of figures in the log agree to `1e-10`
   relative, and the second set is what the test printed when *you* ran it.
4. **The table.** `grep -n "RuntimeError" ComputeTargets/ScalarModel.py`: one site, the z-grid
   check. `grep -n "print_tb\|print(f\"type=" Quadrature/supervisors/base.py`: nothing.
   `grep -c "solver_list\|\"BDF\"\|\"LSODA\"\|\"DOP853\"" ComputeTargets/ScalarModel.py`: 0.
5. **The RHS.** The only hunk inside `ODEPolicy` is `_get_T_Jordan`'s substitution becoming a
   raise; the only hunk inside `ODERHS` is the `d_logV_dphi` line. `ComputeTargets/Policies/`
   untouched.
6. **The budget.** Test (e) names 51 steps; the default in the parameters object is `2_000_000`
   and the log records it as the planner's.
7. **The label and the loop.** `VERSION_LABEL` is still `"2026.5.0"`; the cap, floor, clamp and
   reflection code is unchanged (`git diff` shows no hunk in those lines).
8. **Suites.** All three pass; `ComputeTargets/tests` rose by the new methods.

Then the board: B, C, S, X done; four issues moved to §4; the parking-model issue carries a dated
line; the index corrected.

## 4. Report, then stop

As `README.md` says.
