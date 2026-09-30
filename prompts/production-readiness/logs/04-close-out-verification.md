# Log 04 — Close-out verification and handover

**Prompt:** prompts/production-readiness/04-close-out-verification.md
**Commit:** the commit that adds this file ("Add the production-readiness close-out verification"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-09-30
**Result:** COMPLETE

Every README §6 row is at or better than target on the final tree, and reproduces the figure its
prompt's log quotes to the digits printed. No stop condition was met.

## What shipped

- `.documents/review-remediation-verification.md`: a new §4.6, "Addendum 2026-09-30 — the
  `production-readiness` campaign", inserted at the end of §4 (before §5). 134 lines added, none
  changed or removed. It carries README §7's five points with their evidence, three statements of
  supersession (§4.2 item 4, §4.3 "decide which network", §4.5's H5 and network bullets), the
  verification table, the scope statement and the one reproduce command.
- `prompts/production-readiness/IMPLEMENTATION_STATE.md`: header COMPLETE with the fresh-database
  rule; row 04. §3 and §4 untouched.
- `prompts/INDEX.md`: status complete, dates, open-issue column, header count.
- `.documents/OPEN_ISSUES.md`: header states the `production-readiness` board closed. The count
  stays **13**: the four issues this campaign closed are absent from the index, and no §3 or §4
  entry changed.
- This log. No production file and no test changed. `VERSION_LABEL` is `"2026.3.0"`.

## Deviations from the prompt

### The scope check was made against the board and the logs, not prompts 01–03 — IMPLEMENTATION CHOICE
The instruction for this run was not to read the other prompts, while prompt 04 §1 says to check
each file against each prompt's §1. I checked against README §0.4, the board's "Code:" list and
the "What shipped" of logs 01–03 instead. Every file matched. If a reviewer wants the literal
check, it is one read of each prompt's §1.

### The new tests' "fails on `HEAD~1`" rows are not re-run — IMPLEMENTATION CHOICE
Reproducing them means swapping a production file back to its old version, and this prompt allows
no production change. The rows are cited to logs 01–03, whose orchestrators re-ran them.

## Verification performed

All from the repository root with `venv/bin/python` on `6cab788`, clean tree, 2026-09-30.

- **Suites.** `CosmologyModels/tests`: **18**, OK, 109.6 s. `ComputeTargets/tests`: **30**, OK,
  74.7 s. Against 12 and 13 at `204795e` (12 / 18 after 01; 12 / 21 after 02; 18 / 30 after 03).
  Nothing fell.
- **Every §6.3 row and the §6.1/§6.2 test rows**, with `CHAMPBH_TEST_REPORT=1`
  (`test_adiabatic_mass`, `test_eos_w_derivative`, `test_hard_reflection_reporting`,
  `test_network_flag`). Figures are in the addendum's table. None differs from the log's; the
  largest of each: 9.570e-8 (conformal part), 4.067e-7 (matter limit), 3.375e-8 (Xav `dw_dlogT`),
  1.006e-2 (⁷Li/H shift), 3.070e-6 / 6.304e-6 (A·C, windowed), 2.258e-4 (end sample, recorded).
- **Greps.** `grep -n VERSION_LABEL main.py plot_by_beta.py`: `"2026.3.0"` at `main.py:89` and
  `plot_by_beta.py:79`. `"hard_reflections"` in `*.py`: only the payload key, the source key in
  `build_extra_data`, the test, and the CSV column name at `plot_by_beta.py:500`.
  `small_network_flag`: comments and docstrings only. `small_network` defaults: `False`.
- **Planning probe** `h5_bracket_probe.py`, re-run: Table 1 matches log 03's to every printed
  digit; Table 2 9.887e-6 / 9.885e-8 / 9.944e-10; min −0.4065 at 145.9 MeV, max 0.3498 at
  229.6 MeV on its 1500-point grid. `q_sign_change_probe.py` reproduces README §2 (e) (1.8,
  3.89e-8, 1.06e-5; 2.83e-6, 6.96e-4, 9.25e-6).
- **Scope.** `git diff --stat 204795e..HEAD` (before this commit): 43 files, 4990 insertions, 114
  deletions. Every file is one the plan allows. `ComputeTargets/ScalarModel.py` differs only by
  `HARD_REFLECTIONS_KEY`, `build_extra_data` and its call site (read line by line). The three
  documents under `.documents/` and the `review-remediation` board have additions only. `PRyM/`,
  `thirdparty/`, schemas and `Xav_EOS_data.csv` are absent.
- **Board checks.** The four closed issues have no row in `.documents/OPEN_ISSUES.md`, and each
  has a **Resolved** line on the `review-remediation` board (four lines: two issues share prompt
  01's).
- `git diff --stat` for this commit is given in the report to the caller; it holds only the five
  allowed files.
- Not run: no pipeline, no `ScalarModel` solve.

`git diff --stat 204795e..HEAD` as recorded, before this commit:

```
 .documents/OPEN_ISSUES.md                          |  42 +-
 .documents/architecture-summary.md                 |   6 +
 .documents/numerical-methods-for-paper.md          |  53 ++
 .documents/numerical-strategies.md                 |  88 +++
 ComputeTargets/AdiabaticHistory.py                 | 174 ++++-
 ComputeTargets/BBNData.py                          |   9 +-
 ComputeTargets/ScalarModel.py                      |  75 ++-
 ComputeTargets/tests/prym_fixtures.py              |  32 +-
 ComputeTargets/tests/test_adiabatic_mass.py        | 711 +++++++++++++++++++++
 ComputeTargets/tests/test_bbn_callbacks.py         |   8 +-
 .../tests/test_hard_reflection_reporting.py        | 141 ++++
 ComputeTargets/tests/test_network_flag.py          | 252 ++++++++
 CosmologyModels/GenericEOS/GenericEOS.py           |  19 +
 CosmologyModels/GenericEOS/LambdaCDM_GenericEOS.py |   4 +
 .../GenericEOS/SaikawaShirai_EOS_jax_autodiff.py   |  25 +
 .../GenericEOS/SaikawaShirai_EOS_spline.py         |  20 +
 CosmologyModels/GenericEOS/Xav_EOS_spline.py       |  27 +
 CosmologyModels/tests/test_eos_w_derivative.py     | 241 +++++++
 extract_common.py                                  |  39 +-
 main.py                                            |  10 +-
 plot_by_beta.py                                    |  42 +-
 prompts/INDEX.md                                   |  10 +-
 .../01-report-hard-reflections.md                  | 137 ++++
 .../02-wire-the-network-flag.md                    | 151 +++++
 .../03-adiabatic-source-response.md                | 233 +++++++
 .../04-close-out-verification.md                   |  90 +++
 .../production-readiness/IMPLEMENTATION_STATE.md   | 273 ++++++++
 prompts/production-readiness/README.md             | 482 ++++++++++++++
 .../logs/01-report-hard-reflections.md             |  95 +++
 .../logs/02-wire-the-network-flag.md               | 277 ++++++++
 .../logs/03-adiabatic-source-response.md           | 528 +++++++++++++++
 .../logs/03-probes/join_window_probe.py            |  45 ++
 .../logs/03-probes/spline_order_probe.py           |  58 ++
 prompts/production-readiness/logs/README.md        |   6 +
 .../production-readiness/orchestrator/README.md    | 100 +++
 .../production-readiness/orchestrator/prompt-01.md |  91 +++
 .../production-readiness/orchestrator/prompt-02.md |  93 +++
 .../production-readiness/orchestrator/prompt-03.md | 128 ++++
 .../production-readiness/orchestrator/prompt-04.md |  72 +++
 .../planning-probes/h5_bracket_probe.py            | 116 ++++
 .../planning-probes/q_sign_change_probe.py         |  67 ++
 prompts/review-remediation/IMPLEMENTATION_STATE.md |  28 +
 tools/bbn_baseline.py                              |   6 +-
 43 files changed, 4990 insertions(+), 114 deletions(-)
```

## Observations not acted on

- `IMPLEMENTATION_STATE.md`'s prompts table, and the "Code:" header line, were left as the other
  prompts wrote them. Line numbers quoted by logs 01 and 02 (for example `main.py:752`) have
  shifted by a few lines since prompt 03's comment; the addendum uses the current ones.
- Log 03's observation 1 stands: Ḣ/H² from the policy against the stored H samples on a real
  history has not been measured. It needs a solve, which is the production run's. Not opened.
- No defect found in this pass; no issue opened.

## State handed to the next prompt

This is the last prompt of the campaign. What the production run should read is the addendum,
**§4.6 "Addendum 2026-09-30 — the `production-readiness` campaign" of
`.documents/review-remediation-verification.md`**.

The one command that reproduces the whole verification, from the repository root (about 4 minutes):

```bash
PYTHONPATH=. CHAMPBH_TEST_REPORT=1 ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. CHAMPBH_TEST_REPORT=1 ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/h5_bracket_probe.py && PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/q_sign_change_probe.py && git diff --stat 204795e..HEAD && grep -n VERSION_LABEL main.py plot_by_beta.py
```

Final suite counts: `CosmologyModels/tests` 18, `ComputeTargets/tests` 30, both OK.
`VERSION_LABEL = "2026.3.0"`: **every store made before 2026.3.0 is invalid.**
