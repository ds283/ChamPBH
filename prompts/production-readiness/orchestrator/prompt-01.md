# Orchestrator — prompt 01, report the hard-reflection count

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (a), §6.1 first. **You do not
write code.**

**The prompt:** [`../01-report-hard-reflections.md`](../01-report-hard-reflections.md)
**Board item:** P1 · **Closes:** `[00-hard-reflection-count-is-stored-but-never-reported]`,
`[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]`

## 0. What makes this prompt unusual

`ScalarModel.py` is touched, in exactly one place: the `extra_data` block, moved into a function
unchanged, plus one constant. Everything else in that file is off limits (README §0.4). The rest
of the review is whether the old reader fails the new test.

## 1. Before you dispatch

1. The planning commit is in the tree (`prompts/production-readiness/` exists). Cut the branch
   `production-readiness` if needed (README "Before every dispatch" item 1). Record `HEAD`;
   `git status` clean.
2. Baselines: both suite counts (12 and 13 expected).
3. The grep of the prompt's acceptance item 2, kept for comparison:
   ```bash
   grep -rn "'hard_reflections'\|\"hard_reflections\"" --include='*.py' . | grep -v "^./venv\|^./thirdparty\|^./claude-context"
   ```
   Expected: `ComputeTargets/ScalarModel.py:923`, `:1271`, and `extract_common.py:216, 220`.
4. `venv/bin/black --check ComputeTargets/ScalarModel.py extract_common.py plot_by_beta.py`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, template in `README.md`. Add: *"Your diff may touch
`ComputeTargets/ScalarModel.py` (a module constant and the `extra_data` block moved into a
function, nothing else), `extract_common.py` (`add_ScalarModel_labels` and a new reader function
only), `plot_by_beta.py`, `ComputeTargets/tests/`, the log, this campaign's board, the
`review-remediation` board (a Resolved line per closed issue, nothing else) and
`.documents/OPEN_ISSUES.md`. Nothing else."*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **`ScalarModel.py` is a pure move.** `git diff HEAD~1 HEAD -- ComputeTargets/ScalarModel.py`
   shows three things and nothing else:
   - the constant;
   - the new function, whose body is the old block with `self._extra_data` replaced by a return
     value;
   - the call site.

   No other hunk. If anything in `compute_scalar_model`, `ODEPolicy` or `ODERHS` moved, stop.
3. **The builder is identical.** Test (a) exists, keeps a verbatim copy of the old block, and
   passes. Read the copy against `git show HEAD~1:ComputeTargets/ScalarModel.py` lines 1263–1294.
4. **The breakage, run by you.** Use the generic procedure in `README.md` with
   `F = extract_common.py` and the new test module.
   - With the old reader, test (c) must fail with the caption reading `Hard reflections: 0` where
     3 was expected. An `ImportError` of the new reader function is not enough.
   - If the test imports the reader from `extract_common`, check the log's own demonstration
     against the old code, and say which you relied on.
5. **The grep.** Rerun §1 item 3. It finds only the payload key, the builder's source key and
   the test.
6. **The survey.** Read the `plot_by_beta.py` diff:
   - `data.csv` gains `hard_reflections`;
   - the stdout summary is printed once per (M, Λ);
   - nothing about what is plotted changed.

   Run test (d) if it exists.
7. **Suites.** `ComputeTargets/tests` up by the number of new methods; `CosmologyModels/tests`
   12; all passing.
8. **Housekeeping.**
   - `black --check` clean on the changed files; the log in the template.
   - The board: P1 done.
   - The index: both rows gone, count 15 → 13, date.
   - The `review-remediation` board: exactly two added lines, the Resolved lines. Check with
     `git diff HEAD~1 HEAD -- prompts/review-remediation/`.

## 4. Stop and ask the user

- Any of the prompt's §5.
- Check 2 shows any change to `ScalarModel.py` beyond the move.
- Check 4 does not fail on the old reader.

## 5. After it lands

Report:

- the commit;
- the grep before and after;
- the breakage record as you ran it;
- a sample of the stdout summary from the log;
- the suite counts;
- the two issues closed.

Then stop.
