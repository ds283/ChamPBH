# Orchestrator — prompt 01, the Hubble-only BBN route

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2, §2 (a)–(e), (m), §6.0–§6.2
first. **You do not write code.**

**The prompt:** [`../01-hubble-only-bbn-route.md`](../01-hubble-only-bbn-route.md)
**Board items:** R, W, O · **Closes:** the issues named in the prompt's header

## 0. What makes this prompt unusual

It patches vendored code, removes a route through four modules and five test files, rewrites tests
whose subject disappears, and bumps the label. The review is five things:

- the patched route reproduces the reference;
- the limit and the checks fire;
- nothing else in PRyMordial moved;
- no test's purpose was lost;
- there is exactly one bump.

## 1. Before you dispatch

1. **The board's Decisions record the user's ruling on README §0.2 P1–P9**, dated. If they do
   not, **stop and ask**. Note which were overruled: the review below follows the ruling.
2. On branch `science-readiness`; `git status` clean; record `HEAD`.
3. Baselines: the three suite counts (18, 67, 17) and wall-clocks.
4. Record:
   - `grep -n "VERSION_LABEL =" config/version.py` (`"2026.5.0"`);
   - `grep -n "PRYM_VERSION =" ComputeTargets/BBNData.py`;
   - `grep -c "ChamPBH" PRyM/PRyM_main.py PRyM/PRyM_init.py`;
   - `git diff --stat 6aaa706 -- PRyM/` (empty).
5. `venv/bin/black --check ComputeTargets/BBNData.py Datastore/SQL/ObjectFactories/BBNData.py plot_ScalarModel.py main.py config/argument_parser.py config/version.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`PRyM/PRyM_init.py` and `PRyM/PRyM_main.py`, only as the prompt's §1 R1 and W1 say;*
> - *`ComputeTargets/BBNData.py`;*
> - *`Datastore/SQL/ObjectFactories/BBNData.py` (the `pressure_NP_MeV4` column only);*
> - *`plot_ScalarModel.py` (the `p_NP` panels only);*
> - *`main.py` (the BBN payload and the option);*
> - *`config/argument_parser.py`; `config/version.py`;*
> - *`tools/history_and_bbn.py` (new);*
> - *`ComputeTargets/tests/`;*
> - *the log; this campaign's board; the `run-integrity` board (Resolved lines only, if P4 was
>   accepted); and `.documents/OPEN_ISSUES.md`.*
>
> *It may not touch `ComputeTargets/ScalarModel.py`, any other factory, `PRyM/PRyM_thermo.py`,
> `.documents/`, or anything under `thirdparty/`.*

## 3. The review — ten checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The vendored diff.** Read `git diff HEAD~1 HEAD -- PRyM/`. Every hunk is marked with the
   campaign and prompt, and is one of the following; anything else is a stop:
   - the flag;
   - the `Hubble` line;
   - the exception class;
   - the `wall_clock_limit` parameter;
   - the `fun`/`jac` wrappers at the eight `solve_ivp` sites, and the between-stage check;
   - the `dTNPdt` revert (if P2 was accepted).

   No rate, tolerance, temperature, time or sampling constant changed.
3. **The breakage, run by you.** The generic procedure, with `F` = `PRyM/PRyM_main.py`,
   `PRyM/PRyM_init.py` and `ComputeTargets/BBNData.py` together:
   - test (a) fails on `HEAD~1` with three components;
   - test (e) fails, because the bad results are returned as successes;
   - test (f) fails with `ValueError` and with a NaN return.

   Restore; all pass.
4. **The reference, run by you.** Test (c) passes. Its pinned constants are README §6.2's
   `const-honly` row, with provenance.
5. **The real histories, run by you, unloaded.** The driver on β = 2 at `M = 0.5` and `10⁻³`. Yp
   and D/H against README §6.1's honly rows are within `1e-5` relative; the wall time is within
   1.5×.
6. **The limit and the checks.** Tests (d) and (e) pass. Read the failure reasons they assert.
7. **P1.** The grep of acceptance 3 finds nothing, unless P1 was overruled.
8. **Tests kept their purpose.** For every deleted test method, the log names its replacement, and
   the replacement asserts the same property of the new route. `ComputeTargets/tests` did not
   fall.
9. **The label.** One line, `VERSION_LABEL = "2026.6.0"`, with a dated sentence. `PRYM_VERSION`
   as the prompt says. No other file mentions `2026.6.0` except logs and the board.
10. **Suites.** All three pass.

Then the board: R, W, O done; the issues closed as the header says (assigned ones by README §5
rule 4); the index corrected.

## 4. Report, then stop

As `README.md` says.
