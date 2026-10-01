# Orchestrator — prompt 01, the kinematic-cap step loop

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2, §2 (a)–(g), §6.1 first.
**You do not write code.**

**The prompt:** [`../01-kinematic-cap-step-loop.md`](../01-kinematic-cap-step-loop.md)
**Board items:** A, J, X (loop) · **Closes:** three `[00-…]` issues named in the prompt's header

## 0. What makes this prompt unusual

It rewrites the function every stored history comes from, and bumps the label. The review is
five things:

- the audit's pointwise figures at P1, P2, P3 are reproduced within the stated tolerances;
- the nine full histories complete at no more than twice the audit's RHS, with zero reflections
  and zero `T_Jordan = 0` substitutions;
- nothing about the RHS, the EOS or the sampling changed (the diff shows it);
- the reflection fires at the floor and nowhere else, and `φ ≤ 0` raises;
- one bump, to `"2026.5.0"`, in `config/version.py`.

## 1. Before you dispatch

1. The board's Decisions section records the user's acceptance of README §0.2 and the two
   guards (dated 2026-10-01). If it does not, **stop and ask**.
2. On branch `integrator-remediation`; `git status` clean; record `HEAD`.
3. Baselines: all three suite counts (18, 41, 17) and wall-clocks.
4. **Run the audit's probes on this tree and keep their output**; they are the "now" column:
   ```bash
   PYTHONPATH=. ./venv/bin/python .documents/integrator-audit-2026-09-30/p1_smallM.py 0.01 2>&1 | grep -E "^(regions|vel)"
   PYTHONPATH=. ./venv/bin/python .documents/integrator-audit-2026-09-30/p_smallM_scan.py regions 1e-10 2>&1 | grep -E "^regions"
   ```
   The first must show `regions@1e-08 … nfev= 26634`; the second must show `FAILED: Required
   step size is less than spacing between numbers`. These are the breakage records: the audit's
   scripts drive their own copy of the old loop, so they keep printing them after the prompt
   lands; the point is that they were measured on the tree the prompt changes.
5. Record `grep -n "VERSION_LABEL =" config/version.py` (`"2026.4.0"`) and
   `grep -rn "HARD_REFLECTIONS_KEY\|SolutionFragment" --include='*.py' . | grep -v "venv/\|thirdparty/\|integrator-audit\|prompts/" | wc -l`.
6. `venv/bin/black --check ComputeTargets/ScalarModel.py Quadrature/supervisors/ScalarField.py extract_common.py plot_by_beta.py plot_ScalarModel.py main.py config/version.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`ComputeTargets/ScalarModel.py`; `Quadrature/supervisors/ScalarField.py`; `extract_common.py`;
`plot_by_beta.py`; `plot_ScalarModel.py`; `main.py` (the label registration only);
`config/version.py`; `CosmologyConcepts/Potentials/AbstractPotential.py` and
`ExponentialPotential.py` (the two new properties only); `ComputeTargets/tests/` (new tests, and
the reporting test rewritten under a new name); the log; this campaign's board; and
`.documents/OPEN_ISSUES.md`. Not `Quadrature/supervisors/base.py`, not any other potential, not
any `ODEPolicy` or `PotentialDerivativePolicy` method, not the datastore, not `.documents/`.
Leave the fallback wrapper in place."*

## 3. The review — ten checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`: nothing outside the dispatch's list.
2. **The RHS did not change.** `git diff HEAD~1 HEAD -- ComputeTargets/ScalarModel.py` shows no
   hunk inside `ODEPolicy`, `HubblePolicy` or `ODERHS`. `ComputeTargets/Policies/` is untouched.
3. **The sampling did not change.** The hunk in the sampling block replaces the fragment walk
   with one `sol(N_forward)` call and nothing else; `SampleValues` is unchanged; the z-grid
   truncation and the `max_N < final_N` check are unchanged.
4. **The tests, run by you.** The new module passes; `ComputeTargets/tests` rose by its methods
   and the rewritten reporting test has at least five; the other two suites are 18 and 17.
5. **The breakage.** The log quotes the §1.4 figures with their commit. Re-run §1.4 yourself on
   the landed tree: the probes still print them (they drive the audit's copy), and the new tests
   (a) at `M = 0.01` and (b) at `M = 1e-10` print `≤ 3 500` RHS and one reflection respectively.
6. **The nine histories.** Run the audit's driver for each row of README §6.1 (d) and compare
   with the log's figures from the production loop:
   ```bash
   PYTHONPATH=. ./venv/bin/python .documents/integrator-audit-2026-09-30/p_full.py 2.0 0.5 1e-8 1e-4 kin reflect 2>&1 | grep -E "^(FULL|  wall|  final)"
   ```
   (and the other eight β, M pairs). First bounce `N` must agree to `1e-5`, RHS to 10 %, and both
   must be at or under the table's target. Any `T_Jordan = 0` line in the output is a fail.
7. **The floor rule and the guards.** Read the loop: the reflection branch tests exactly `π < 0`
   and `f φ/|π| < h_floor`; before reflecting it checks `potential.reflects_at_origin` and
   `W ≤ ½π²` with `W` built from `V_over_3H2Mp2`, `log_V` and `potential.log_V_floor` (README
   §2 (b′)); it negates `π` and changes nothing else in the state; `φ ≤ 0` after an accepted step
   raises. No branch mirrors `φ` or reflects at `φ = 0`. The log quotes the maximum `W/(½π²)`
   over every reflection in tests (a), (b) and the nine histories, and it is `≤ 1e-3`; the G2
   test's quoted ratio is about 23. `git diff HEAD~1 HEAD -- CosmologyConcepts/Potentials/`
   shows only the two properties on `AbstractPotential` and `ExponentialPotential`.
8. **The clamp.** `np.minimum(solver.jac_factor, …, out=…)` (or equivalent) runs after every
   accepted step, guarded for `None`.
9. **Metadata and label.** `build_extra_data` writes the README §2 (e) keys and no old one; the
   grep of §1.5 now finds zero lines; the new label is in all three scripts' lists and
   `main.py`'s registration.
10. **The bump.** `config/version.py` says `"2026.5.0"` with a dated sentence; no other
    `VERSION_LABEL =` exists; the log states before and after.

Then the board: A, J done; X "loop half"; three issues moved to §4 with Resolved lines; the index
rows deleted, count and date corrected.

## 4. Report, then stop

As `README.md` says. Name the nine histories' figures explicitly.
