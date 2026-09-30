# Orchestrator — prompt 03, stop recomputing failed BBN rows and pair lookups correctly

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2, §2 (d), §6.3 first. **You
do not write code.**

**The prompt:** [`../03-failure-caching-and-pairing.md`](../03-failure-caching-and-pairing.md)
**Board item:** R · **Closes:** `[03-main-recomputes-failed-bbn-rows-on-every-run]`,
`[00-main-pairs-lookup-results-against-the-unfiltered-bin]`

## 0. What makes this prompt unusual

The logic lives in `main.py`, which no test can import. The review is four things:

- the helper decides "missing" correctly, including under the retry flag;
- both stages use it;
- no zip against the unfiltered bin survives;
- a stored failure now counts as done.

## 1. Before you dispatch

1. Prompt 02 landed and reviewed; branch `run-integrity`; record `HEAD`; `git status` clean.
2. Baselines: all three suite counts.
3. Run the pairing probe and keep its output ({V1, V3} against {V2, V4}):
   ```bash
   ./venv/bin/python prompts/run-integrity/planning-probes/pairing_probe.py
   ```
4. Record:
   ```bash
   grep -n "binned_batch\[key\]\|if not obj.failure\|failure=" main.py
   grep -n "retry" config/argument_parser.py        # empty
   ```
5. `venv/bin/black --check main.py config/argument_parser.py Datastore/SQL/ObjectFactories/BBNData.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`Datastore/SQL/ObjectFactories/BBNData.py` (`build` only), `main.py` (the adiabatic and BBN
stages' batch builders and their summary lines), `config/argument_parser.py` (one argument), a new
`pipeline_selection.py` or equivalent pure module, `Datastore/tests/` and `ComputeTargets/tests/`
(new modules), `.documents/architecture-summary.md` (an additive note), the log, this campaign's
board, the `review-remediation` board (one Resolved line), and `.documents/OPEN_ISSUES.md`. Not
`ComputeTargets/BBNData.py`, not `PRyM/`, not `RayTools/`, not step 1 of `run_pipeline`."*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The breakage, run by you.**
   - **(a).** `T` = the new `Datastore/tests` module, `F` = `Datastore/SQL/ObjectFactories/BBNData.py`.
     On `HEAD~1`, (a) fails with `MultipleResultsFound`. Restore; it passes.
   - **(g).** `F` = `config/argument_parser.py`. On `HEAD~1`, (g) fails because the argument
     does not exist.
   - **(d)–(f).** Their record is §1 item 3's probe output, quoted in the log. Check that it is.
3. **The helper is right.** Run the new `ComputeTargets/tests` module yourself. (d) gives
   {V2, V4}; (e) gives {V2, V4} and {V2, V3, V4}; (f) raises.
4. **Both stages use it.** Re-run §1 item 4's first grep.
   - `binned_batch[key]` survives only where the pairs enter the helper.
   - Neither function zips results against it.
   - The BBN query passes `failure=None`.
5. **The flag.** `--retry-failed-bbn` is `store_true`, default False, and it reaches the helper
   through `args`.
6. **Nothing else moved.**
   - Step 1 of `run_pipeline`, `ScalarModel.build` and `AdiabaticHistory.build` are not in the
     diff.
   - `VERSION_LABEL` is `"2026.4.0"` and `PRYM_VERSION` is `"bf24c3d+cham03+ri02"`, both
     unchanged.
7. **The summary lines.** One per stage, in the format the log quotes.
8. **Housekeeping.**
   - All three suites up and passing; `black --check` clean.
   - The board: R done, and the header states the consequence.
   - Both index rows gone; count and date corrected.
   - One Resolved line on the `review-remediation` board.
   - The planning issue moved to §4.
   - `.documents/` additions only.

## 4. Stop and ask the user

- Any of the prompt's §5.
- A zip against the unfiltered bin survives, or the helper truncates.
- `RayWorkPool`, step 1, or any stored value is changed.

## 5. After it lands

Report:

- the commit;
- the breakage record;
- the probe's {V1, V3} and the helper's {V2, V4};
- the helper's names;
- the flag;
- the summary-line format;
- the consequence sentence;
- the suite counts.

Then stop.
