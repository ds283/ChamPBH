# Orchestrator — prompt 02, detect BBN solver failures and bump the version

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2, §2 (b), §2 (c), §6.2
first. **You do not write code.**

**The prompt:** [`../02-detect-bbn-solver-failures.md`](../02-detect-bbn-solver-failures.md)
**Board item:** F · **Closes:** `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`,
`[00-a-nan-new-physics-sample-hangs-prymordial]`

## 0. What makes this prompt unusual

It patches the vendored PRyMordial, and it carries the campaign's one version bump. The review
is four things:

- a forced failure raises, at every stage;
- no successful solve moved: every pin holds;
- the boundary is exactly the PRyMordial call;
- the label is `"2026.4.0"`.

## 1. Before you dispatch

1. Prompt 01 landed and reviewed; branch `run-integrity`; record `HEAD`; `git status` clean.
2. Baselines: all three suite counts, and the `ComputeTargets/tests` wall-clock.
3. Run the truncated case of the probe and keep its output. The last abundances must be
   Yp 0.246887219, D/H 2.474578712, ⁷Li/H 5.425221518, with no exception:
   ```bash
   PYTHONPATH=. ./venv/bin/python -u -W ignore prompts/run-integrity/planning-probes/prymordial_solver_probe.py truncated 2>&1 | grep -v entering
   ```
4. Record:
   ```bash
   grep -n "solve_ivp(" PRyM/PRyM_main.py          # 8 call sites
   grep -n "PRYM_VERSION =" ComputeTargets/BBNData.py
   grep -n VERSION_LABEL config/version.py          # "2026.3.0"
   ```
5. `venv/bin/black --check ComputeTargets/BBNData.py config/version.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`PRyM/PRyM_main.py` (the solve_ivp checks, their exception class and marker comments only, never
black-formatted), `ComputeTargets/BBNData.py`, `config/version.py` (the value and one dated
sentence), `ComputeTargets/tests/` (a new module; no pin re-taken),
`.documents/numerical-strategies.md` (additive notes), the log, this campaign's board, the
`review-remediation` board (one Resolved line), and `.documents/OPEN_ISSUES.md`. Not `main.py`,
not any datastore factory, not `thirdparty/`."*

## 3. The review — nine checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The patch is only checks.** `git diff HEAD~1 HEAD -- PRyM/` shows:
   - one exception class;
   - a check after each of the 8 `solve_ivp` calls;
   - a marker comment at each.

   It changes no argument, tolerance or method, and touches no Julia branch. It is not
   reformatted.
3. **The breakage, run by you.** Use the generic procedure in `README.md`, with `T` the new test
   module and `F` = `PRyM/PRyM_main.py` plus `ComputeTargets/BBNData.py`. On `HEAD~1`:
   - (a) and (b) fail because nothing names a stage, **not** on an import;
   - (d) fails because nothing raises.

   Restore, and confirm it passes.
4. **The probe agrees.** Re-run §1 item 3: it now raises the new class and names the low-T
   full-network stage.
5. **No pin moved.** `git diff HEAD~1 HEAD -- ComputeTargets/tests/` changes no numeric pin, and
   the whole `ComputeTargets` suite passes.
6. **The boundary.** In `ComputeTargets/BBNData.py`:
   - the only new `except` is `except Exception` in the helper, around the PRyMordial call;
   - `compute_BBN_data` uses the helper;
   - `compute_SM_baseline` does not.
7. **The guard.** `build_NP_callbacks` rejects non-finite samples, and the callbacks reject a
   non-finite T. Check that test (d) runs no solve.
8. **The label and the version string.** `config/version.py` says `"2026.4.0"`, with a dated
   sentence added beneath the existing ones. `PRYM_VERSION` is `"bf24c3d+cham03+ri02"`.
9. **Housekeeping.**
   - Suite counts up in `ComputeTargets/tests` only; `black --check` clean outside `PRyM/`.
   - The board: F done, and the header says every store before 2026.4.0 is invalid.
   - Both index rows gone; count and date corrected.
   - One Resolved line on the `review-remediation` board.
   - The planning issue moved to this board's §4.
   - `.documents/` additions only.

## 4. Stop and ask the user

- Any of the prompt's §5.
- A pin moved, or `PRyM/` changed beyond the checks.
- An `except` outside the helper, or `except BaseException` anywhere.
- The label is anything but `"2026.4.0"`.

## 5. After it lands

Report:

- the commit;
- the breakage record;
- the probe's truncated case before and after;
- the stage names;
- `VERSION_LABEL` and `PRYM_VERSION` before and after, with the new comment sentences;
- the consequence sentence;
- the suite counts.

Then stop.
