# Orchestrator — prompt 03, PRyMordial's passenger equation and failure reasons

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (d), (f), §6.2 first. **You do
not write code.**

**The prompt:** [`../03-prymordial-passenger-and-failure-reasons.md`](../03-prymordial-passenger-and-failure-reasons.md)
**Board item:** R2 · **Closes:** R2

## 0. What makes this prompt unusual

A vendored third-party file is patched. The whole review is: does the patch change any physical
output (it must not), does it remove the stall (it must), and is `NP_e_flag` anywhere in the diff
(it must not be).

## 1. Before you dispatch

1. Prompt 02 landed and reviewed; branch; `HEAD`; `git status` clean.
2. Baselines: `CosmologyModels/tests` count; `ComputeTargets/tests` does not exist (0).
3. Reproduce README §2 (f) rows 1–2 yourself (≈ 20 s) with a script equivalent to the audit's
   probe, and keep the abundances: Yp 0.24689 / 0.25409, D/H 2.4623 / 2.6715. **Do not** run
   the oscillating case without a `timeout 120`.
4. `grep -n "NP_e_flag" ComputeTargets/BBNData.py` is empty.

## 2. Dispatch

One fresh-context subagent, Opus, template in `README.md`. Add: *"`PRyM/` is vendored and may be
patched with a marker comment; `thirdparty/` may not be touched. Never set `NP_e_flag`. The
oscillating probe must always run under an external `timeout`."*

## 3. The review — nine checks

1. **Allowed files.** `PRyM/PRyM_main.py`, `ComputeTargets/BBNData.py`,
   `Datastore/SQL/ObjectFactories/BBNData.py`, `plot_by_beta.py`, `ComputeTargets/tests/`, log,
   board, index. Nothing else.
2. **The patch is what was asked.** `git diff HEAD~1 HEAD -- PRyM/` shows `dTNPdt` returning
   `0.0` with the marker comment and the original line kept as a comment; the function's
   signature and its place in `dTtotdt` unchanged; `Hubble`, `dTgdt`, `dTnudt` untouched.
3. **No `NP_e_flag`.** `grep -rn "NP_e_flag" ComputeTargets/ plot_by_beta.py` is empty.
4. **The stall is gone, and was there.** Run the new suite yourself (≈ 40 s). Then the breakage
   check: `git checkout HEAD~1 -- PRyM/PRyM_main.py`, run only the oscillating test under
   `timeout 120`, confirm it does not finish or fails its 60 s bound, restore. Quote both.
5. **Inert.** Test (b) passes: zero-ρ_NP with the flag on vs off agree to 1e-6; no
   `RuntimeWarning`. Test (c) passes: Yp 0.25409 / D/H 2.6715 to 1e-5.
6. **Failure reasons.** `compute_BBN_data`'s failure returns carry `failure_reason`; the factory
   has the nullable column and selects it; `BBNData.failure_reason` is readable when
   `failure` is true (read the property: it must not raise). `plot_by_beta.py` prints the dropped
   list; `git diff` shows nothing about *what is plotted* changed.
7. **`PRyM_version` names the patch.** `grep -n PRyM_version ComputeTargets/BBNData.py`.
8. **Suites**: `ComputeTargets/tests` 0 → 4; `CosmologyModels/tests` unchanged.
9. **Housekeeping**: `black --check` on changed files (not on `PRyM/`, which was never
   black-formatted — the agent must not reformat it); log with the P0 table; board R2; index.

## 4. Stop and ask the user

- Any of the prompt's §6.
- Check 5 misses by any amount.
- The diff reformats `PRyM/PRyM_main.py` beyond the patched lines.

## 5. After it lands

Report: the commit; the P0 table; the breakage record; the inertness numbers; the column name and
what an old store would do; the suite counts. Then stop.
