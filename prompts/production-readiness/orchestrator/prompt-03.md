# Orchestrator — prompt 03, the adiabatic mass: the source-response term

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.3, **§2 (c)–(f)**, §6.3
first. **You do not write code, and you do not re-derive the physics.** Your job is to check that
the agent's reference is independent of its closed form, and that the reference, not the agent's
confidence, decided the formula.

**The prompt:** [`../03-adiabatic-source-response.md`](../03-adiabatic-source-response.md)
**Board item:** P3 · **Closes:** `[00-adiabaticity-diagnostic-omits-the-source-response-term]`

## 0. What makes this prompt unusual

It is the only prompt that changes physics. The planner's derivation (README §2 (c)) says the
audit's formula is wrong by a factor. That derivation is itself only evidence. The review is
three things:

- the reference uses no derivative the fix introduces;
- the agent re-derived the formula rather than copying it;
- the diagnostic changed while the background evolution did not.

## 1. Before you dispatch

1. Prompt 02 landed and reviewed; branch; `HEAD`; `git status` clean.
2. `grep -n VERSION_LABEL main.py plot_by_beta.py` gives `"2026.3.0"` in both. **If not, stop.**
   Prompt 03 must not bump it.
3. Baselines: both suite counts.
4. Run the planning probes yourself and keep their output. They take about 60 s and a few
   seconds.
   ```bash
   PYTHONPATH=. ./venv/bin/python prompts/production-readiness/planning-probes/h5_bracket_probe.py
   ./venv/bin/python prompts/production-readiness/planning-probes/q_sign_change_probe.py
   ```
   Table 2 should give 9.9e-8 at h = 1e-4. The analytic dΣ/d ln T should match a central
   difference to 1.0e-7. The second probe should show the log route at 1.8 on the crossing history,
   and the asinh spline at 1.1e-5 and 9.3e-6.
5. `venv/bin/black --check ComputeTargets/AdiabaticHistory.py CosmologyModels/GenericEOS/*.py main.py`.
   Record which files are not clean.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`ComputeTargets/AdiabaticHistory.py`, `CosmologyModels/GenericEOS/` (the new `dw_dlogT` method and
its forwarding only), `main.py` (one added comment sentence, no label change), both `tests/`
packages, `.documents/numerical-strategies.md` and `.documents/numerical-methods-for-paper.md`
(dated additions only), the log, this campaign's board, the `review-remediation` board (one
Resolved line), and `.documents/OPEN_ISSUES.md`. Not `ComputeTargets/ScalarModel.py`. Write the
derivation in the log before you write code. If your derivation or your reference disagrees with
README §2 (c), stop and report; do not implement either form."*

## 3. The review — ten checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`. **`ComputeTargets/ScalarModel.py` is absent.**
   If it is present, stop.
2. **The reference is independent.** Read the test's reference function. Stop if it breaks any
   of these:
   - T_J(φ ± h) is found from `G_s` alone, by root-finding entropy conservation;
   - neither `dG_s_dlogT` nor `dw_dlogT` appears anywhere in the reference's path;
   - the closed-form bracket is not computed inside it.
3. **The derivation is in the log** and gives the response relations. Check that they are read off
   `ScalarModel.py` with line numbers, not copied from the README. The log states the result and
   the limits.
4. **The reference decided.**
   - The log's re-measured Table 1 agrees with your §1 item 4 probe output to the digits
     printed, in the B_entropy and B_fd columns.
   - The audit-form measurement (test (f)) is reported and not asserted.
5. **The breakage, run by you.** Use the generic procedure in `README.md` with
   `F = ComputeTargets/AdiabaticHistory.py` and test (b).
   - It must fail because the conformal part is zero, not only by `TypeError` on the new argument.
   - If it can fail only by signature, check that the log quotes the old formula's error against
     the reference. It should be max |bracket| ≈ 0.41, plus the f_m term. Say which you relied on.
6. **The numbers.** Run both new test modules with `CHAMPBH_TEST_REPORT=1` if supported. Check
   against README §6.3:
   - `dw_dlogT` ≤ 1e-6 for each class;
   - the bracket ≤ 1e-6 for the exponential and the stand-in couplings;
   - the matter limit to 1e-6.
   *Amended 2026-09-30 after the run (the user; README header): for the spline class and the base
   formula, the witness within a factor 1.03 of 120 MeV is h = 1e-5.*
7. **No finite difference in production.**
   ```bash
   git diff HEAD~1 HEAD -- CosmologyModels/GenericEOS/ ComputeTargets/AdiabaticHistory.py
   ```
   Read it: `dw_dlogT` is analytic, spline or autodiff in every class, and there is no step size
   in production code.
8. **Q's numerator** (A3, amended 2026-09-30).
   - The diff computes A·C as m (1 + Ḣ/H²) + ½ dm/dN. `log` of |M²_eff| is no longer on the path
     to `abs_Q`.
   - **Nothing fails, floors or clips a history because M²_eff crosses zero.** Any of those is a
     stop.
   - Run test (e) yourself: ≤ 1e-4 of max |A·C| on both synthetic histories, and the exact-zero
     sample finite. Quote the numbers.
     *Amended 2026-09-30 after the run (the user; README header): the ≤ 1e-4 is on N ∈ [0.5, 11.5];
     the end samples are quoted, not checked.*
   - Run test (e) against `HEAD~1`'s `AdiabaticHistory.py`, the generic procedure. It must fail
     on the crossing history or the exact zero.
   - Q's definition (the `abs_Q` expression from A·C and |B|^{3/2}), `Q_labels` and the stored
     fields are unchanged.
   - The log records the dm/dN representation as an IMPLEMENTATION CHOICE.
9. **The label is not bumped.** It is still `"2026.3.0"`, with one added sentence in `main.py`'s
   comment.
10. **Housekeeping.**
    - Both suites up and passing; `black --check` clean on changed files.
    - The documents: additions only, with the same `grep '^-'` check as prompt 02.
    - The board: P3 done, and the header's consequence line is present.
    - The index row gone, count and date corrected; one Resolved line on the `review-remediation`
      board.

## 4. Stop and ask the user

- Any of the prompt's §6. In particular, a derivation or a reference that disagrees with README
  §2 (c). **Relay the agent's three-way comparison verbatim.**
- Check 1, 2 or 7 fails.
- The breakage check passes on `HEAD~1`, and the log offers no other demonstration.

## 5. After it lands

Report:

- the commit;
- the derivation's result as the log states it;
- the reference's convergence;
- Table 1, re-measured;
- the audit-form difference;
- the bracket's range;
- the A3 representation and test (e)'s numbers, before and after;
- the breakage record;
- the suite counts.

Then stop.
