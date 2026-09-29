# Orchestrator — prompt 02, fix the entropy derivative

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (b), (e), (j), §6.1 first. **You do
not write code.**

**The prompt:** [`../02-fix-the-entropy-derivative.md`](../02-fix-the-entropy-derivative.md)
**Board items:** R1 (fix), R5 (fix) · **Closes:** R1, R5

## 0. What makes this prompt unusual

The diff should be a few lines in one EOS class, two constants in `SaikawaShirai_common.py`
(R5), a test-constant flip, a version-label bump and a
dated note. Everything else in the review is about **where** the fix went (the EOS class, not the
consumer) and about the deliberate-breakage check, which you run yourself.

## 1. Before you dispatch

1. Prompt 01 landed and reviewed; branch `review-remediation`; record `HEAD`; `git status` clean.
2. Baseline: `CosmologyModels/tests` count from prompt 01's landing (expected 6).
3. `grep -n VERSION_LABEL main.py plot_by_beta.py` → `"2026.1.1"` in both.
4. `venv/bin/black --check CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py CosmologyModels/GenericEOS/SaikawaShirai_common.py main.py plot_by_beta.py`
   — record whether they are clean before the agent touches them.

## 2. Dispatch

One fresh-context subagent, Opus, template in `README.md`. Add: *"Prompt 01's log and tests are
in the tree; read the log first. Your diff may touch `SaikawaShirai_EOS_spline.py`,
`SaikawaShirai_common.py` (the two low-T constants and their comment block only),
`CosmologyModels/tests/`, `main.py` and `plot_by_beta.py` (the version label only),
`.documents/numerical-strategies.md` (additive), the log, the board and the index. Nothing else."*

## 3. The review — nine checks

1. **Allowed files only.** `git diff --stat HEAD~1 HEAD`. `ScalarModel.py` is **not** in it. If it
   is, stop.
2. **Both derivatives, same convention.** Read the two methods. Both are divided by ln 10 (or both
   are on a natural-log grid with converted clamps). One fixed and one not is a stop. In
   `SaikawaShirai_common.py`, `git diff HEAD~1 HEAD` touches only `LOW_T_GSTAR` = 3.383,
   `LOW_T_G_S_STAR` = 3.931 and their comment block. `SAIKAWA_SHIRAI_T_LO` and the coefficients
   are unchanged.
3. **The deliberate breakage, run by you.**
   ```bash
   git stash -u 2>/dev/null; git checkout HEAD~1 -- CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py CosmologyModels/GenericEOS/SaikawaShirai_common.py
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | tail -20
   git checkout HEAD -- CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | tail -20
   git checkout HEAD -- CosmologyModels/GenericEOS/SaikawaShirai_common.py; git stash pop 2>/dev/null
   ```
   - **Both files unfixed.** Cases 1, 3, 4 and the ρ_R witness must **fail**, printing +1.422,
     2.303, 0.182 or 0.0041 (or their tolerances' violations).
   - **Only the constants unfixed.** Case 2's join half (or case 1 at 10 keV and T_CMB) must fail
     with ≈ +1.465e-4.
   - Then confirm the suite passes at `HEAD`.
4a. **The derivative cases use the amended norm** (README §6.1): cases 3 and 4 assert
    |Δ dG_s_dlogT| / G_s ≤ 1e-6 absolute, not a ratio at 1e-6 relative. A relative test that
    passes would mean the grid changed; stop.
4. **The guard is at 1e-5.** `grep -n "1e-5\|1.0e-5" CosmologyModels/tests/test_temperature_law.py`
   finds the e-fold tolerance; the offsets are zero.
5. **Re-scored.** Run `tlaw_check.py` yourself: the `kappa=1` column equals the exact column to
   1e-5 at every row, including 10 keV and T_CMB. Run `eos_consistency.py`: 0.99–1.01.
6. **The version label.** `"2026.2.0"` in both files; the one-sentence note beside it in `main.py`.
7. **The note in `numerical-strategies.md` is additive**: `git diff HEAD~1 HEAD --
   .documents/numerical-strategies.md` contains additions only.
8. **The consequence is stated** in the log and on the board's R1 row: every store built under
   2026.1.1 is invalid.
9. **Housekeeping.** Suite count ≥ before; `black --check` clean on changed files; log in
   template; board and index in the same commit.

## 4. Stop and ask the user

- Any of the prompt's §6.
- Check 3 does not fail on the unfixed class — then the guard does not bite and prompt 01 was
  wrong.
- Any other check fails.

## 5. After it lands

Report: the commit; which of the two fix styles was chosen; the R5 constants as landed; the breakage record as you ran it;
the re-scored table; the version label; the consequence sentence. Then stop.
