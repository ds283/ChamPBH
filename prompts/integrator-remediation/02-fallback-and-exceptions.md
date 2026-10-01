# Prompt 02 — Remove the solver fallback and settle the exception taxonomy

**Campaign:** [`README.md`](README.md) · **Board items:** **B**, **C**, **S**, and the RHS half of
**X** · **Board:** `IMPLEMENTATION_STATE.md`. Update your row and B, C, S, X.
**Closes:** `[00-solver-fallback-is-not-wired]`, `[00-runtime-errors-mix-bugs-and-failures]`,
`[00-trial-state-exceptions-abort-the-solve]` and
`[00-physical-M-histories-run-for-days-without-a-parking-model]` on this board (the last as
"made a clean failure"; the model itself stays open as
`[00-settling-at-physical-M-needs-a-parked-tracking-model]`).
**Recommended model:** **Opus**. Mostly deletions and one table. The judgement is in changing
how the RHS behaves on an unphysical trial state without changing any value it returns on a
physical one, and in proving that with prompt 01's numbers.

**Read first:**

1. [`README.md`](README.md) §0.2, §0.4, §2 (g)–(i), §5, §6.2.
2. `logs/01-kinematic-cap-step-loop.md`, in particular "State handed to the next prompt": the
   loop's names, the parameters namedtuple, and the P1 (a) figures at `M = 0.5` you must
   reproduce to `1e-10`.
3. `.documents/integrator-audit-2026-09-30/README.md` §4, §5, §3.7 (the settling divergence), §8.
4. `ComputeTargets/ScalarModel.py` on `HEAD`:
   - the fallback wrapper around prompt 01's loop call (`solver_list`, `solver_labels`,
     `while not success`, `except ComputationFailureError`, `if not success`);
   - the remaining `RuntimeError` sites (state length; z grid too short);
   - `ODEPolicy._get_fm`, `_get_T_Jordan`, `__call__` (`G < 0`, `E < 0`, the NaN/inf input check);
   - `ODERHS.__call__`'s NaN/inf output branch and its `data.d_logV_dphi` read;
   - the loop function's `ComputationFailureError` handling and its failsafe check.
5. `ComputeTargets/Policies/PotentialDerivativePolicy.py`: every `raise ComputationFailureError`.
6. `Quadrature/supervisors/base.py:98–120`, `RHS_timer`.
7. `RayTools/RayWorkPool.py` around `obj.store()` (`:430` on `2b89022`) and `ScalarModel.store()`
   (`ComputeTargets/ScalarModel.py`, the `ray.get` and the `failure` branch), to confirm for
   yourself the README §1 statement that a `RuntimeError` in a task ends the run.

---

## 1. The changes

**B1 — one stepper.** Delete `solver_list`, the `solver_labels` dict, `success`, the
`while not success` loop and the `except` that advances the name. One `try` around the call to
the loop and the sampling returns `{"failure": True}` on `ComputationFailureError`, after printing
the reason as the old code did. The returned `"solver_label"` is prompt 01's constant.

**C1 — the taxonomy.** README §2 (h), every row:

- the state-length check becomes an `assert`;
- the `d_logV_dphi` line in `ODERHS`'s diagnostic branch reads `self.policy.potential`'s
  `d_logV_dphi(phi_Einstein)` (or is dropped from the print); the branch raises
  `ComputationFailureError` as it intends;
- reaching `N_failsafe` raises `ComputationFailureError` (prompt 01 may already have done this
  inside the loop; if so, record it and make sure no `RuntimeError` for it remains);
- the z-grid check keeps its `RuntimeError`.

**X1 — trial states.** `_get_T_Jordan`: a non-positive `T_J` raises `ComputationFailureError`
with the existing message, instead of printing and substituting 1 K. No other policy changes.
This is the only edit to `ODEPolicy`, and it cannot change a value returned for a physical state
(`T_J > 0` on every accepted state of every history; the loop raises on the sampled states if it
ever were not). Prove it with the P1 (a) reproduction in §2 (c) below.

**S1 — the step budget.** `StepControl` gains `step_budget` (default `2_000_000`). The loop
counts accepted steps and raises `ComputationFailureError` when the count exceeds it, with a
message naming `N`, `T_J` in GeV, the step count and the reflection count, prefixed
`"step budget exhausted"`. Record the default as the planner's (README §0.2) in the log.

**C2 — a quiet timer.** `RHS_timer.__exit__` stops printing the exception type and traceback. It
still records the time. Nothing else in `base.py` changes.

---

## 2. Tests — `ComputeTargets/tests/test_integrator_exceptions.py`

No Ray, no datastore. Reuse prompt 01's test module's fixtures (import its constants and its
builder; do not duplicate the states).

- **(a) `d_logV_dphi`.** A stand-in `ODEPolicy` whose `__call__` returns an `ODEPolicyData` with a
  NaN `friction_term`, wrapped in `ODERHS`; calling it raises `ComputationFailureError`. **Must
  fail on `HEAD~1`** with `AttributeError`; the orchestrator runs it.
- **(b) `_get_T_Jordan`.** A `StateVector` with `log_T_Jordan = −1e4` given to `ODEPolicy`
  raises `ComputationFailureError`. **Must fail on `HEAD~1`**, where it returns (and prints).
- **(c) Physical values unchanged.** P1 (a) at `M = 0.5` through the loop reproduces log 01's
  `φ(21)`, `π(21)` and first-bounce `N`, `φ_min` to `1e-10` relative. Quote both sets in the log.
- **(d) The failsafe.** The loop from P1 at `M = 0.5` with `N_failsafe = N₀ + 0.1` raises
  `ComputationFailureError`, not `RuntimeError`.
- **(e) The budget.** The loop from P2 with `step_budget = 50` raises `ComputationFailureError`
  whose message begins `"step budget exhausted"` and names a step count of 51.
- **(f) The timer is quiet.** Prompt 01's exception-rejection test (its (f)), run with stdout and
  stderr captured, produces no text containing `"Traceback"`.
- **(g) The fallback is gone.** `ast`-parse `ComputeTargets/ScalarModel.py`: no name
  `solver_list`, no string `"BDF"`, `"LSODA"` or `"DOP853"` in `compute_scalar_model`.

---

## 3. What this prompt does not do

- No change to the loop's cap, floor, clamp or reflection (prompt 01's), beyond adding the
  budget field and its check.
- No change to any `ODEPolicy` or `PotentialDerivativePolicy` value or exception other than
  `_get_T_Jordan`'s substitution.
- No schema change: no failure-reason column. The reason is printed.
- No parked-tracking model.
- No edit to `.documents/`, to the potentials, or to `main.py`'s label registrations.

## 4. Acceptance

1. README §6.2, every row, with measured values in the log.
2. `grep -n "RuntimeError" ComputeTargets/ScalarModel.py` inside `compute_scalar_model` and the
   loop finds one site, the z-grid check. `grep -n "print_tb" Quadrature/supervisors/base.py`
   finds nothing.
3. All three suites pass; `ComputeTargets/tests` rises by your methods. `black --check` clean on
   changed files.
4. The board and the index: B, C, S done, X done; the four issues closed (§4, Resolved lines,
   index rows deleted, count and date corrected); the parking-model issue stays in §3, with a
   dated line saying the budget now makes its absence a clean failure.

## 5. Stop conditions — stop and ask the user

- Test (c) does not reproduce log 01's figures to `1e-10`: something changed a physical value.
- A `RuntimeError` site cannot be classified by README §2 (h).
- Removing the fallback needs a change to how `ScalarModel.store()` reads the dict beyond the
  label.

## 6. The log and the board

`logs/02-fallback-and-exceptions.md` in the README §5.1 template. State `VERSION_LABEL` before
and after (`"2026.5.0"` both). In "State handed to the next prompt": the final shape of
`StepControl`, the exception table as implemented, and the exact grep commands of §4.
