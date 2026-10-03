# Prompt 01b — Draw the kick threshold with Σ_eff

**Campaign:** [`README.md`](README.md) · **Board item:** **B** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and B.
**Closes:** `[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]`.
**Recommended model:** **Sonnet.** One formula, its text, and a test.

> Added 2026-10-03 by the user's ruling U2 (README §0.2). This prompt is independent of the
> PRyMordial work: it shares no file with prompts 01, 02 or 03 except `ComputeTargets/tests/`.

**Read first:**

1. [`README.md`](README.md) §0.2 (U2), §2 (h), §5, §6.1b.
2. `extract_common.py`: `kick_threshold_curve` and `plot_T_deliver`.
3. `plot_by_beta.py`, where it calls `kick_threshold_curve` (the comment above the call).
4. `ComputeTargets/tests/test_extraction.py`: `TestKickThresholdCurve`.
5. The paper's reachability condition (`Paper1.tex`, `eq:surfing-equation`, and the definition
   β_s² ≡ 1/(3Σ_eff) = (2 + Σ)/(6Σ)), as quoted in README §2 (h). The paper is in another
   repository; do not edit it.

---

## 1. The changes

- **`kick_threshold_curve`** returns β_th(T) = √((2 + Σ)/(6Σ)), that is 1/√(3Σ_eff) with
  Σ_eff = Σ/(1 + Σ/2), where Σ = 1 − 3w(T) from the cosmology's own `w`. A T with Σ ≤ 0 is still
  omitted.
  - The docstring states the formula, names the paper's reachability condition, and says that
    `w` is the model's own: for `QCD_Cosmology` that is the `Xav_EOS_data.csv` spline, the same
    `w` the integration reads in `ScalarModel.py`'s RHS.
- **The legend label** in `plot_T_deliver` reads `β_th(T) = 1/√(3Σ_eff(T))`, in the label's
  existing mathtext style.
- **The comment** above the call in `plot_by_beta.py` states the same formula.
- Nothing else. In particular, not the Σ: it is already the integration's (README §2 (h)).

## 2. Tests

In `ComputeTargets/tests/test_extraction.py`, `TestKickThresholdCurve`:

- **(d), with the expected values changed.** Use stub `w` values for which the new formula gives
  exact numbers:
  - w = 7/23 gives Σ = 2/23 and β_th = 2;
  - w = 1/5 gives Σ = 2/5 and β_th = 1;
  - w = 0 gives Σ = 1 and β_th = 1/√2;
  - Σ < 0 and Σ = 0 are omitted, as now.

  **This fails on `HEAD~1`**, where w = 7/23 gives 1/√(6/23) = 1.958. Keep the omitted-point
  cases.
- **(d2), new: the production curve's minimum.** Build `QCD_Cosmology(0, Planck_units(),
  Planck2018())` and evaluate the curve on 4000 points log-spaced over [0.05, 50] GeV.
  - Its minimum is 1.1074 ± 5e-4 and lies within [0.17, 0.20] GeV. That is the paper's 1.11, and
    the planner's measurement on `d78c9f8` (README §2 (h)).
  - On `HEAD~1` the minimum is 1.0295.
  - No solve. The docstring says it builds the cosmology, which takes a few seconds.

## 3. What this prompt does not do

It does not change Σ, the EOS, `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`,
or any other figure. It does not run `plot_by_beta.py` against a store. It does not edit
`prompts/science-readiness/`, whose README §2 (k) specified the old form: plans are records.

## 4. Acceptance

README §6.1b, every row. All three suites pass and ComputeTargets rises by one. `black --check`
is clean on the files you changed.

## 5. Stop conditions — stop and ask the user

- The production minimum is not within 5e-4 of 1.1074, or not at the QCD peak.
- `plot_by_beta.py` passes `kick_threshold_curve` a cosmology other than the history's own.

## 6. The log, the board and the index

- `logs/01b-kick-threshold-sigma-eff.md`, in the README §5.1 template.
- The board: B done; the issue moved to §4 with a **Resolved** line quoting the old and new
  minima.
- The index: delete the row; correct the count and date.

**Allowed files:**
- `extract_common.py` (`kick_threshold_curve` and the label in `plot_T_deliver` only);
- `plot_by_beta.py` (the comment only);
- `ComputeTargets/tests/test_extraction.py`;
- the log; this campaign's board; `.documents/OPEN_ISSUES.md`.
