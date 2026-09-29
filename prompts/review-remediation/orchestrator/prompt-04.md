# Orchestrator — prompt 04, ratio splines and a baseline

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (c), (g), §6.3 first. **You do
not write code.**

**The prompt:** [`../04-ratio-splines-and-a-baseline.md`](../04-ratio-splines-and-a-baseline.md)
**Board item:** R3 · **Closes:** R3

## 0. What makes this prompt unusual

This is the prompt the review asked for and the audit found unnecessary for results. The review
is therefore strict about **no regression anywhere** (the synthetic comparison against the old
representation), about the derivative callback being differentiated rather than differenced, and
about the refactor not reaching into anything it was not asked to touch.

## 1. Before you dispatch

1. Prompt 03 landed and reviewed; branch; `HEAD`; `git status` clean.
2. Baselines: both suite counts (expected 6+ and 4).
3. Run `venv/bin/python .documents/audit-2026-09-29/spline_test.py` yourself and keep the
   250-knot rows: asinh 7.9e-10 / 3.3e-8; ratio 5.2e-17 / 9.7e-9; derivative 6.8e-8 / 3.0e-6 vs
   2.0e-13 / 7.4e-7.
4. `grep -n "asinh\|sinh" ComputeTargets/BBNData.py` — record the lines; they must be gone after.

## 2. Dispatch

One fresh-context subagent, Opus, template in `README.md`. Add: *"The derivative callback is the
analytic derivative of the interpolant; a finite difference anywhere in the callbacks is a stop.
No datastore schema change. Keep the spline domain and the pre-check as they are."*

## 3. The review — ten checks

1. **Allowed files.** `ComputeTargets/BBNData.py`, `ComputeTargets/tests/`, `tools/bbn_baseline.py`,
   `plot_by_beta.py`, log, board, index. Not `ScalarModel.py`, not the factories.
2. **asinh gone.** `grep -n "asinh\|sinh" ComputeTargets/BBNData.py` is empty.
3. **No finite difference.** Read the three callbacks and `build_NP_callbacks`: `drho_NP_dT` uses
   the ratio spline's `.derivative()` and the ρ_SM derivative; no `(f(T+h) − f(T−h))` anywhere
   in production code (the *test* may difference the truth).
4. **Fail closed.** Read the monotonicity check: it raises before any spline is built, names the
   pair, and there is no `sort` in the builder. The `print` at the old `:112` is gone.
5. **The Ω″ term.** `pi_Einstein ** 2` (or `pi * pi`) multiplies `log_Omega_primeprime`; the
   factored function exists and the stand-in test covers Ω″ ≠ 0.
6. **The bounds.** Run the suite yourself; read the printed maxima in the log against §6.3:
   constant ≤ 1e-12; oscillating ≤ 2e-8; derivative ≤ 1.5e-6; and the against-asinh case passes.
7. **End to end.** The constant-ratio PRyMordial case reproduces prompt 03's abundances to 1e-4;
   the baseline test reproduces README §2 (f) row 1 to 1e-4.
8. **The ρ_SM choice** is stated with the measured alternative (≤ 1 % apart), on the board's R3 row.
9. **Baseline plumbing.** `venv/bin/python tools/bbn_baseline.py` prints four abundances from the
   root; `plot_by_beta.py --help` shows `--no-baseline`; nothing is stored.
10. **Housekeeping.** Suite up by the cases added; `black --check` clean on changed files; log
    gives the knot count in the window; board R3; index.

## 4. Stop and ask the user

- Any of the prompt's §5.
- Any bound in check 6 missed, or the against-asinh case fails.
- The end-to-end abundances differ from prompt 03's by more than 1e-4.

## 5. After it lands

Report: the commit; `build_NP_callbacks`' signature; the ρ_SM choice and the alternative's
difference; the four maxima; the end-to-end and baseline agreements; the knot count. Then stop.
