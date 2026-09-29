# Prompt 01 — Report the hard-reflection count

**Campaign:** [`README.md`](README.md) · **Board item:** **P1** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and P1.
**Closes:** `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]` and
`[00-hard-reflection-count-is-stored-but-never-reported]`, both on the `review-remediation` board.
**Recommended model:** **Sonnet**. The change is small and mechanical. The judgement is in keeping
the factoring behaviour-preserving, and in writing a test that the old reader fails.

**Read first:**

1. [`README.md`](README.md) §0, §2 (a), §5, §6.1.
2. `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3, the entries for the two issues above.
   Their "Next step" lines are data, not a specification.
3. `ComputeTargets/ScalarModel.py`:
   - `:905–935`, the result dictionary `compute_scalar_model` returns; `"hard_reflections"` is its
     key;
   - `:1234–1300`, `ScalarModel.store()`, and within it the `store_attr` block (`:1263–1294`) that
     builds `extra_data`;
   - `:1088–1094`, `extra_metadata`.
4. `Quadrature/supervisors/ScalarField.py:271`, `number_hard_reflections`.
5. `Datastore/SQL/ObjectFactories/ScalarModel.py:183, 431–445, 495–500`, the JSON round trip of
   `extra_data`.
6. `extract_common.py:168–245`, `add_ScalarModel_labels`.
7. `plot_by_beta.py`:
   - `:93–130`, `build_beta_plot` and the `scalar_data` it receives;
   - `:450–530`, the `data.csv` row builder;
   - `:580–640`, the dropped-models printout. Copy its style.

---

## 1. The changes

**F1 — one name for the stored key.** In `ComputeTargets/ScalarModel.py`, add a module-level
constant, e.g. `HARD_REFLECTIONS_KEY = "number_hard_reflections"`.

- **The stored name does not change** (README §2 (a)).
- The payload key `"hard_reflections"` at `:923` does not change either. It is the supervisor's
  name for the source value.

**F2 — factor the `extra_data` builder, unchanged.** Move the `store_attr` block out of
`ScalarModel.store()` into a module-level pure function, e.g.
`build_extra_data(data: dict) -> Optional[dict]`, and call it from the same place.

- **Its behaviour must be identical:**
  - the same keys and the same thresholds (a count is stored only when > its minimum);
  - the same `_asdict()` handling of the three RHS-statistics entries;
  - `None` where the old code left `self._extra_data` unset.
- It uses `HARD_REFLECTIONS_KEY` for the reflection count.
- **This is the only change to `ScalarModel.py` beyond F1.** The ODE, the policies and
  `compute_scalar_model` are not touched.

**F3 — one reader.** In `extract_common.py`, add a function, e.g.
`hard_reflection_count(extra_data: Optional[dict]) -> int`.

- It returns `extra_data[HARD_REFLECTIONS_KEY]` if present, and 0 otherwise. An absent key means
  zero (README §2 (a)); say so in its docstring.
- `add_ScalarModel_labels` uses it and prints `Hard reflections: <n>`.
- Correct the misleading comment in the `else` branch, or remove the branch if the reader makes it
  redundant.
- **The early `return` when `extra_metadata is None` is out of scope.** Leave it as it is, and
  record it under "Observations" if you think it should change.

**F4 — the survey summary.** In `plot_by_beta.py`, using the F3 reader:

- **(a) The CSV.** Add a `hard_reflections` column to the `data.csv` rows: the count for β's model,
  or `nan` when there is no model for that β.
- **(b) Stdout.** Print, once per (M, Λ), a line with the number of models whose count is > 0.
  Then print one indented line per such model with β and the count, in the style of the
  dropped-models list. If none reflected, one line saying so.
- **Do not change what is plotted.**

---

## 2. Tests — `ComputeTargets/tests/test_hard_reflection_reporting.py`

No Ray cluster, no datastore, no solve. Use matplotlib's `Agg` backend if you draw a figure.

- **(a) The builder is unchanged.** Keep a verbatim copy of the old `store_attr` block in the test
  as the reference.
  - Feed both a sample result dictionary with non-trivial values for every key (and `None` for the
    RHS statistics in one case).
  - Assert identical output, including the `None` case.
- **(b) The count survives the store.** `build_extra_data` with `hard_reflections = 3` and a JSON
  round trip (`json.loads(json.dumps(...))`, as the factory does) gives
  `hard_reflection_count(...) == 3`. With `hard_reflections = 0` the key is absent, and the count is
  0.
- **(c) The caption.** `add_ScalarModel_labels` on a figure, with a stand-in model carrying the
  extra data from (b), produces the text `Hard reflections: 3`.
  - If building a stand-in for everything the caption reads is impractical, factor the
    reflection line into a helper, test that, and say which you did in the log.
  - **This test must fail on `HEAD~1`'s `extract_common.py`.** Show that in the log, for example
    by running it against the old reader.
- **(d) The CSV column.** If you factor the row builder out of `build_beta_plot`, test that a β with
  a count of 2 gives `hard_reflections == 2`, and a β with no model gives NaN. If you do not factor
  it, say why, and the orchestrator will read the diff instead.

The count in `ComputeTargets/tests` rises by the number of test methods you add. Record it.

---

## 3. What this prompt does not do

- No change to how reflections are detected or counted, and no change to the supervisor.
- No renaming of the stored key and no schema change.
- No change to any other `extra_data` entry's reader or caption.
- `add_BBN_info_labels` is prompt 02's; do not touch it.

## 4. Acceptance

1. README §6.1, every row, with the measured value in the log.
2. `grep -rn "'hard_reflections'\|\"hard_reflections\"" --include='*.py' .`, excluding `venv/`,
   `thirdparty/` and `claude-context/`, finds only:
   - the payload key in `compute_scalar_model`;
   - the source key in `build_extra_data`;
   - the test.
3. Both suites pass; `CosmologyModels/tests` unchanged; `ComputeTargets/tests` up.
4. `black --check` is clean on the changed files.
5. The board and the index:
   - P1 done;
   - both issues closed. Delete their rows from `.documents/OPEN_ISSUES.md`, and add a dated
     **Resolved** line to each entry on the `review-remediation` board (README §5 rule 4);
   - count and date corrected.

## 5. Stop conditions — stop and ask the user

- The factored builder cannot be made identical to the old block (test (a)).
- Fixing the reader appears to need a change to the stored key or the schema.
- Reaching `ScalarModel.store()`'s block needs anything beyond moving it into a function.

## 6. The log and the board

`logs/01-report-hard-reflections.md`, in the README §5.1 template. Beyond the template:

- the grep of acceptance item 2, before and after;
- how test (c) was shown to fail on `HEAD~1`;
- a sample of the new stdout summary, from a synthetic call if you have no store.
