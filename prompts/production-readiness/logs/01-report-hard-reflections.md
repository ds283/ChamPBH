# Log 01 — Report the hard-reflection count

**Prompt:** prompts/production-readiness/01-report-hard-reflections.md
**Commit:** the commit that adds this file ("Report the hard-reflection count in captions and the survey"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-09-30
**Result:** COMPLETE

## What shipped

- **F1.** `ComputeTargets/ScalarModel.py`: new module constant `HARD_REFLECTIONS_KEY =
  "number_hard_reflections"` (beside `EXPECTED_SOL_LENGTH`). The stored name and the payload key
  `"hard_reflections"` (`compute_scalar_model`) are unchanged.
- **F2.** `ComputeTargets/ScalarModel.py`: the `store_attr` block moved out of `ScalarModel.store()`
  into module-level `build_extra_data(data: dict) -> Optional[dict]`, placed before `class
  ScalarModel`. Body identical apart from using `HARD_REFLECTIONS_KEY`; it returns `None` where the
  old code left `self._extra_data` unset. `store()` now reads `extra_data = build_extra_data(data)`
  and assigns `self._extra_data` if it is not `None`. Nothing else in the file changed.
- **F3.** `extract_common.py`: new `hard_reflection_count(extra_data: Optional[dict]) -> int`
  (0 for `None` or an absent key; docstring says an absent key means zero).
  `add_ScalarModel_labels` prints `Hard reflections: <n>` through it; the two-branch `if/else` and
  its misleading comment are replaced by one `fig.text`. The early `return` on
  `extra_metadata is None` is untouched. New import `HARD_REFLECTIONS_KEY`.
- **F4.** `plot_by_beta.py`: `data.csv` rows gain `hard_reflections` (count, or `nan` when no model
  for that beta), placed before `Yp_BBN`. In `build_beta_plot`, once per (M, Lambda), a summary
  line and one indented line per reflecting model (style of the dropped-models list), or one line
  saying none reflected. Plots unchanged.
- **Test.** `ComputeTargets/tests/test_hard_reflection_reporting.py`, 5 methods.
- `VERSION_LABEL` is untouched (`"2026.2.0"`; prompt 02 bumps it).

## Deviations from the prompt

### Test (d): the row builder is not factored — IMPLEMENTATION CHOICE
`plot_by_beta.py` runs `parser.parse_args()` and `ray.init(...)` at import, and the row builder
sits inside a `@ray.remote` function, so it cannot be imported by a test. Factoring it into a
module-level function in `plot_by_beta.py` would still leave it untestable; the only importable
home is `extract_common.py`, which the diff scope allows only `add_ScalarModel_labels` and one
reader. Alternative rejected: a new helper in `extract_common.py`, out of scope. The reader that
both use is tested; the orchestrator should read the diff for the column and printout.

## Verification performed

- **Suites** (from the root, `venv/bin/python`): `CosmologyModels/tests` 12 before, 12 after, OK.
  `ComputeTargets/tests` 13 before, 18 after (+5), OK.
- **Test (a):** builder equals a verbatim copy of the old block on four payloads (all populated;
  RHS statistics all `None`; count 0 and one fragment; mixed None). Passes.
- **Test (b):** count 3 survives `json.loads(json.dumps(...))` and reads back 3; count 0 gives an
  absent key and reads 0; `None` and `{}` read 0. Passes.
- **Test (c):** `add_ScalarModel_labels` on an Agg figure with a `SimpleNamespace` stand-in model
  (`solver.label`, `_coupling.name`, `_potential.name`, `extra_metadata`) produces the text
  `Hard reflections: 3`, and `Hard reflections: 0` for a count of 0. **Fails on the old
  reader:** I replaced `extract_common.py` with `HEAD:extract_common.py`, and shimmed
  `hard_reflection_count` in the test's import (the old file has no such function). Result:
  `test_c_caption_reports_the_stored_count` failed with `'Hard reflections: 3' not found in
  [..., 'Hard reflections: 0', 'Solution fragments: 2']`; the other four passed. I then restored
  both files. That is the full old-reader run on `05ea605`'s `extract_common.py`, which equals
  `HEAD~1` for this file after the commit.
- **Grep** `grep -rn "'hard_reflections'\|\"hard_reflections\"" --include='*.py' .` (excluding
  `venv/`, `thirdparty/`, `claude-context/`):
  - before: `extract_common.py:216, 220, 225` (reader, wrong key), `ScalarModel.py:923` (payload),
    `ScalarModel.py:1271` (source in the block);
  - after: `ScalarModel.py:927` (payload), `ScalarModel.py:963` (source key in
    `build_extra_data`), the test (reference copy and payload), and `plot_by_beta.py:500`, which
    is the **CSV column name** `"hard_reflections"`, a new string the prompt required (F4a), not a
    reader of the stored key.
- **Stdout sample.** No store available, so this is the same f-strings run on synthetic values,
  not the function itself:
  ```
  !! build_beta_plot 'Planck2018-Xav', M=1e+09 eV, Lambda=2.5e+06 eV: 2 of 21 model(s) used hard reflections
       -- beta=1.5, M=1e+09 eV, Lambda=2.5e+06 eV: 2 hard reflection(s)
       -- beta=4, M=1e+09 eV, Lambda=2.5e+06 eV: 1 hard reflection(s)
  @@ build_beta_plot 'Planck2018-Xav', M=1e+09 eV, Lambda=2.5e+06 eV: no models used hard reflections (21 model(s) checked)
  ```
  The last line is the all-zero case. `plot_by_beta.py` itself was not run (it needs a store and Ray).
- `black --check` on the four changed Python files: clean.

## Observations not acted on

- `build_extra_data` can never return `None` in practice: the boundary and max-step keys are stored
  unconditionally, so the dictionary is never empty. The `None` branch is kept because the prompt
  requires identical behaviour. Not an issue.
- The early `return` in `add_ScalarModel_labels` when `extra_metadata is None` skips the fragments
  caption too; left as instructed. It is unreachable in practice for the same reason.
- `extract_common.py:148` still has `small_network is "True"` (prompt 02's).

## State handed to the next prompt

- `extract_common.py` now imports `HARD_REFLECTIONS_KEY` from `ComputeTargets.ScalarModel` and has
  `hard_reflection_count`; `add_BBN_info_labels` is untouched, so prompt 02 finds it as it was
  (`small_network is "True"` at `extract_common.py:148` region; line numbers in the file shifted
  by a few lines above `add_ScalarModel_labels`, not above it).
- `plot_by_beta.py` has a new multi-line `from extract_common import (...)` block; prompt 02 will
  add its `VERSION_LABEL` and baseline `small_network` edits elsewhere in the file.
- Suite counts after this prompt: `CosmologyModels/tests` 12, `ComputeTargets/tests` 18.
- `VERSION_LABEL` still `"2026.2.0"`.
