# Log 01b — Draw the kick threshold with Σ_eff

**Prompt:** prompts/bbn-tolerance/01b-kick-threshold-sigma-eff.md
**Commit:** the commit that adds this file ("Draw the kick threshold with Sigma_eff"); its SHA is in `git log`
**Model:** Claude Sonnet 5.5
**Date:** 2026-10-03
**Result:** COMPLETE. No deviations, no stop condition met. `kick_threshold_curve` now returns
√((2 + Σ)/(6Σ)) = 1/√(3Σ_eff). The production curve's minimum is 1.10745 at 0.18202 GeV (it was
1.02945 at the same temperature). The new test (d2) and the changed test (d) both fail on the old
source and pass on the new. Suites: 18, 113 → 114, 31.

Everything was run on `7320afe` plus this prompt's uncommitted diff, the branch head at dispatch.

## What shipped

`VERSION_LABEL` stays `"2026.6.0"`; `PRYM_VERSION` stays `"bf24c3d+ri02+sr01+bt02"`. No new public
symbol.

- **`extract_common.py`, `kick_threshold_curve` (`:554`)**: `1.0 / sqrt(3.0 * Sigma)` ->
  `sqrt((2.0 + Sigma) / (6.0 * Sigma))`. The docstring now states
  β_th = 1/√(3Σ_eff) = √((2 + Σ)/(6Σ)) with Σ_eff = Σ/(1 + Σ/2) and Σ = 1 − 3w, names the paper's
  reachability condition (`Paper1.tex`, `eq:surfing-equation`) and says that `w` is the model's own:
  for `QCD_Cosmology` the `Xav_EOS_data.csv` spline, the `w` the integration reads in
  `ScalarModel.py`'s RHS. The omission of Σ ≤ 0 is unchanged. Σ is untouched.
- **`extract_common.py`, `plot_T_deliver` (`:792`)**: legend label
  `$\beta_{\mathrm{th}}(T) = 1/\sqrt{3\Sigma(T)}$` ->
  `$\beta_{\mathrm{th}}(T) = 1/\sqrt{3\Sigma_{\mathrm{eff}}(T)}$`.
- **`plot_by_beta.py` (`:656`)**: the comment above the call now reads
  "beta_th(T) = 1/sqrt(3 Sigma_eff(T)) = sqrt((2 + Sigma)/(6 Sigma)) over the QCD range, from the
  model's own EOS" (wrapped over two lines). Nothing else in the file.
- **`ComputeTargets/tests/test_extraction.py`, `TestKickThresholdCurve`**:
  - (d) now uses stub `w` = 7/23, 1/5, 0, 0.4, 1/3 and expects T = [1, 2, 3] with β_th = 2, 1, 1/√2
    to 1e-14; the Σ < 0 (w = 0.4) and Σ = 0 (w = 1/3) points are still omitted.
  - (d2), new, `test_d2_production_curve_minimum`: builds `QCD_Cosmology(0, Planck_units(),
    Planck2018())`, evaluates the curve on 4000 log-spaced points over [0.05, 50] GeV, and asserts
    the minimum is 1.1074 ± 5e-4 and lies within [0.17, 0.20] GeV. No solve; a few seconds. Its
    docstring says so.

## Deviations from the prompt

None.

## Verification performed

- **Stop conditions.**
  - The production minimum is 1.10745 (script below), within 5e-4 of 1.1074, at 0.18202 GeV, the
    QCD peak of Σ (Σ = 0.31453 there). Not a stop.
  - `plot_by_beta.py` passes `scalar_data[0]._cosmology`, the history's own cosmology (read at
    `:658-660`). Not a stop.
- **Probe**, `venv/bin/python` from the repository root, scratch script (not kept; it is
  test (d2)'s body), 4000 points over [0.05, 50] GeV: new source prints
  `4000  1.1074489305733635  0.18201630316810608  0.31453173726923467` (points kept, minimum,
  its T in GeV, Σ there). Old source (`git stash` of the two source files) prints
  `4000  1.02945445123202  0.18201630316810608  0.31453173726923467`. These are README §2 (h)'s
  1.0295 and 1.1074.
- **Breakage check** (new tests against the old source, `git stash push extract_common.py
  plot_by_beta.py`, then `unittest ComputeTargets.tests.test_extraction`): 2 failures.
  - (d2): `1.02945445123202 != 1.1074 within 0.0005 delta`.
  - (d): `1.9578900207451224 != 2.0 within 1e-14 delta`, README §6.1b's "now" value 1.958.
  - With the new source: `Ran 13 tests ... OK`. The stash was popped and the tree restored.
- **Suites** (`unittest discover`, repository root):

  | package | before (7320afe) | after |
  |---|---|---|
  | CosmologyModels | 18 | 18 |
  | ComputeTargets | 113 | 114 |
  | Datastore | 31 | 31 |

  All pass. ComputeTargets rises by one (d2).
- **`black`** on `extract_common.py`, `plot_by_beta.py` and `test_extraction.py`: "3 files left
  unchanged" (clean before and after my edit).
- **Not done.** `plot_by_beta.py` was not run against a store (prompt §3). The figure was not
  redrawn; reasoning only, from the curve's values, that it moves up (β_th rises by up to 7.6 % at
  the QCD peak).

## Observations not acted on

None. `prompts/science-readiness/` README §2 (k) still specifies 1/√(3Σ); plans are records and the
prompt says to leave it.

## State handed to the next prompt

- `kick_threshold_curve(cosmology, T_grid)` returns β_th = √((2 + Σ)/(6Σ)); its signature and its
  return value (two lists, Σ ≤ 0 omitted) are unchanged, so nothing that calls it changes.
- Prompt 03's handover item 5 applies: any copy of figure 3 made before this commit is redrawn by
  re-running `plot_by_beta.py`; no store changes. The dashed curve's minimum moves from 1.0295 to
  1.1074 (the paper's 1.11), both at 0.182 GeV.
- Suites after this prompt: 18, 114, 31.
