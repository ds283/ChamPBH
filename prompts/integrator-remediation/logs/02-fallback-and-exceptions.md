# Log 02 — Remove the solver fallback and settle the exception taxonomy

**Prompt:** prompts/integrator-remediation/02-fallback-and-exceptions.md
**Commit:** the commit that adds this file ("Remove the solver fallback and settle the exception taxonomy"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-01
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `fc97233` (`HEAD` at dispatch, prompt 01's commit). "HEAD~1" below
means the parent of this prompt's commit, which has the same tree as `fc97233` in every file
this prompt touches. HEAD~1 measurements were made on an export of `fc97233`
(`git archive fc97233 | tar -x -C <scratchpad>/head`), run from that directory with this
repository's `venv/bin/python`. Unless a different script is named, every number below was
printed on this prompt's tree by `ComputeTargets/tests/test_integrator_exceptions.py` or by
prompt 01's helpers in `ComputeTargets/tests/test_kinematic_cap_loop.py`.

The dispatch stopped once, before any code was written. Acceptance 2's `grep print_tb` could not
pass inside the allowed scope (Deviations 1). The user ruled on 2026-10-01: "A, and go ahead with
both smaller points". That ruling is the source of Deviations 1, 2 and 3.

## What shipped

**`VERSION_LABEL`: `"2026.5.0"` before and after** (`config/version.py:36`, not touched).

**B1 — one stepper** (`ComputeTargets/ScalarModel.py`, `compute_scalar_model`).
- Removed: `solver_list`, the `solver_labels` dict of the four `solve_ivp+…` names, `success`,
  the `while not success` loop, the `except` that advanced the name, and the `if not success`
  block. On HEAD~1 these were at `:809–880`.
- Now: `step_control`, `N_failsafe = 1000.0`, `N_start` and the initial `StateVector` are built
  once (`:839–852`). Then **one `try`** (`:857`) runs:
  - the supervisor's `with`;
  - the call to `integrate_scalar_history`;
  - the new dimension `assert` (`:878`);
  - the verbose reflection print;
  - the z-grid check;
  - the sampling, including its own inner `except OverflowError`, which is unchanged.
- `except ComputationFailureError` (`:950`) prints `-- compute_scalar_model (<label>):
  integration failure`, then the reason (`e.message`), then `!! … marked as total integration
  failure`, and returns `{"failure": True}`.
- The returned `"solver_label"` is `SCALAR_MODEL_STEPPER_LABEL` (prompt 01's constant),
  unchanged.
- `ScalarModel.store()` is not touched. It still reads `data.get("failure")` and, on success,
  `self._solver_labels[data["solver_label"]]`. `ScalarModel.__init__`'s `solver_labels` argument
  is that label → `IntegrationSolver` dictionary and stays.
- The sampling block is the same code, indented one level to sit inside the `try`. The z grid,
  `SampleValues`, the `policy`/`hubble` evaluation and `solution(N_forward)` are unchanged.

**C1 — the taxonomy** (README §2 (h)). Every row, as implemented, is in "State handed to the
next prompt".
- **State length.** No such site existed any longer (Deviations 2). The new
  `assert len(result.solution(result.N_final)) == EXPECTED_SOL_LENGTH, …` is at `:878`.
- **`data.d_logV_dphi`** (`ODERHS.__call__`'s NaN/inf branch, `:448–456`). The read is
  dropped, and so is the `V'/V=` field of the "potential" print line (Deviations 4). The branch
  now reaches its `raise ComputationFailureError` (`:482`).
- **Failsafe.** Prompt 01 already raised `ComputationFailureError` inside the loop (`:744`); it
  is unchanged. No `RuntimeError` for it remains anywhere.
- **The z grid.** The `RuntimeError` "largest supplied redshift …" (`:897`) is kept. It sits
  inside the `try`, but the `except` catches only `ComputationFailureError`, so it still ends
  the run.

**X1 — trial states.** In `ODEPolicy._get_T_Jordan` (`:247–265`), `T_Jordan <= 0` now prints the
existing message (`!! ODEPolicy (…): T_Jordan = …, log_T_Jordan = … at N=…`) and raises
`ComputationFailureError(msg)`. It no longer substitutes `1 * self.Kelvin`. The comment saying
the substitution was harmless is replaced by one saying why it raises. No other `ODEPolicy` or
`PotentialDerivativePolicy` line changes.

**S1 — the step budget.**
- `StepControl` gains `step_budget`, appended last with default `2_000_000` (`:84–109`). The
  default is the planner's (README §0.2), accepted by the user on 2026-10-01.
- `integrate_scalar_history` reads `params.step_budget` (`:598`). After each accepted step,
  once `accepted_steps` has been incremented and before the Jacobian clamp, it checks
  `accepted_steps > step_budget` (`:689`). It then raises `ComputationFailureError("step budget
  exhausted: integrate_scalar_history (<label>) took <n> accepted steps (budget <b>) at N=…,
  T_J=… GeV, with <r> reflection(s)")`. `T_J` is `exp(y[4]) / policy.GeV`.
- The docstring says so.

**C2 — quiet supervisors** (`Quadrature/supervisors/base.py`).
- `RHS_timer.__exit__` (`:104`) records the elapsed time and notifies the supervisor. It no
  longer prints `type=…, value=…` and the `print_tb` traceback.
- `IntegrationSupervisor.__exit__` (`:47`) records `integration_time` and also no longer prints
  them. This edit was authorised by the user (Deviations 1).
- `from traceback import print_tb` is removed.
- Both `__exit__` methods still return `None`, so exceptions propagate as before.

**Tests.** `ComputeTargets/tests/test_integrator_exceptions.py` is new, with 10 methods:
- `test_a_nan_output_raises_computation_failure`;
- `test_b_nonpositive_T_Jordan_raises`;
- `test_c_physical_values_unchanged`;
- `test_d_failsafe_is_computation_failure`;
- `test_e_step_budget` and `test_e_default_step_budget`;
- `test_f_prompt_01_rejection_test_is_quiet` and `test_f_rejection_inside_the_timer_is_quiet`;
- `test_g_no_fallback` and `test_g_one_runtime_error_in_the_integration_path`.

It imports prompt 01's `P1`, `P2`, `build`, `integrate`, `rel`, `wall_bounces`, `_log_T_stop`,
`REPO_ROOT` and `SM` by name. It imports that module itself as `kcl` for (f), so that discovery
does not collect prompt 01's `TestCase` classes a second time. It defines three stand-ins:
`NaNFrictionPolicy`, `RaisingPolicy` (raises on its first three calls once armed) and
`ArmOnFirstStep`.

## Deviations from the prompt

### 1. `IntegrationSupervisor.__exit__`'s traceback print removed too — STRUCTURALLY REQUIRED (authorised by the user, 2026-10-01)

What the prompt assumed. Acceptance 2 requires `grep -n "print_tb" Quadrature/supervisors/base.py`
to find nothing, while C2 and the dispatch allowed only `RHS_timer.__exit__` to change. What was
there: a second `print(f"type=…")` / `print_tb(exc_tb)` pair in `IntegrationSupervisor.__exit__`
(`:51–54` on HEAD~1), inherited by `ScalarFieldIntegrationSupervisor.__exit__` through
`super()`. It fired once per failed history, when a `ComputationFailureError` left the
supervisor's `with`. The import at `:17` served both. With only `RHS_timer` changed, the grep
would still find `:17` and `:54`.

What was done. I stopped and asked before writing any code. The user chose option (A) on
2026-10-01: widen the scope to `IntegrationSupervisor.__exit__` and drop its print and the
import. Done. The grep now finds nothing. `test_d_failsafe_is_computation_failure` witnesses
it: on HEAD~1 its captured output contains the supervisor's `type=<class
'ComputeTargets.exceptions.ComputationFailureError'>, value=integrate_scalar_history: the
failsafe N=20.102 …` and the `File "…"` lines. On this tree it contains neither. Side effect:
`QuadSupervisor` (`Quadrature/simple_quadrature.py:30`) also inherits this `__exit__` and also
uses `RHS_timer`, so its exceptions now pass through without the print as well. Its behaviour
is otherwise unchanged; the exception still propagates.

### 2. The state-length `assert` placed after the loop — STRUCTURALLY REQUIRED (the user's ruling, 2026-10-01)

What the prompt assumed. C1 says "the state-length check becomes an `assert`", and README §6.2
has the row "`assert len(...) == EXPECTED_SOL_LENGTH` | `RuntimeError` → assert". What was
there: prompt 01 deleted the site together with the fragment loop. It was at `:677–680` on
`918590e` and checked `len(StateVector._make(sol.y))` after each `solve_ivp` fragment. HEAD~1
therefore had no such `RuntimeError`, and `EXPECTED_SOL_LENGTH` (`:67`) was defined but unread.
What was done, as proposed and approved: one `assert` on the dimension of the integration's
output, at the equivalent point. That is immediately after `integrate_scalar_history` returns in
`compute_scalar_model`, `len(result.solution(result.N_final)) == EXPECTED_SOL_LENGTH`, with the
old message's wording. The alternative was to record the row as already met by prompt 01 and add
nothing. With that choice the §6.2 row would have had no witness in the diff.

### 3. Test (f) also checks for the text the prints actually produced — IMPLEMENTATION CHOICE (the user's ruling, 2026-10-01)

The prompt's (f) asks that prompt 01's exception-rejection test, run with stdout and stderr
captured, produce no text containing `"Traceback"`. Two facts make that check alone unable to
see the regression:
- `traceback.print_tb` never writes "Traceback". I checked on this machine: it writes only
  `  File "…", line N, in …` and the source line, to stderr. The removed `print` wrote
  `type=<class …>, value=…` to stdout.
- Prompt 01's `FailFirstStepCalls` raises *before* calling `ODERHS`, so its exceptions never
  pass through `RHS_timer`.

As written, (f) therefore passes on HEAD~1 too. I measured this: `test_f_prompt_01_rejection_test_is_quiet`
is `ok` on the HEAD~1 export. So, as approved:
- `TRACEBACK_MARKERS = ("Traceback", "type=<class", 'File "')` is checked in (d), (e) and both
  (f) methods;
- `test_f_prompt_01_rejection_test_is_quiet` runs prompt 01's test as specified (through a
  `TextTestRunner`, streams captured);
- `test_f_rejection_inside_the_timer_is_quiet` raises three `ComputationFailureError`s from
  inside `ODEPolicy` (`RaisingPolicy`, armed once the loop has a step cap), i.e. inside
  `RHS_timer`. It requires rejected steps, the same `φ(21)` to `1e-5`, and no marker in the
  captured stdout and stderr. **It fails on HEAD~1** with three `type=<class
  '…ComputationFailureError'>, value=test: injected trial-state failure in ODEPolicy` lines and
  the `File "…ScalarModel.py", line 384, in __call__` frames.

The alternative was to change prompt 01's `FailFirstStepCalls`. That would have edited prompt
01's test module, which is outside the allowed files.

### 4. The `V'/V` field dropped from the diagnostic print, not re-derived — IMPLEMENTATION CHOICE

C1 offered two options: read `self.policy.potential.d_logV_dphi(phi_Einstein)`, or drop the
line from the print. I dropped it, for two reasons:
- `d_logV_dphi` is not abstract on `AbstractPotential` (the comment at `:85–88` says a potential
  may implement the plain-`V` set instead), so the call can itself raise `AttributeError`;
- `ExponentialPotential.d_logV_dphi` evaluates `pow(M/φ, n)` outside its `try`, so on the very
  non-finite states this branch exists for it can raise `ZeroDivisionError` or `OverflowError`.

Either failure would again replace the branch's intended `ComputationFailureError` with another
exception type. The information is not lost: the "cosmology" line already prints `V/3H2Mp2` and
`V'/3H2Mp2`. A comment at `:448` records why.

### 5. The `try` also covers the sampling's `ComputationFailureError` — IMPLEMENTATION CHOICE (as the prompt worded it)

B1 says "One `try` around the call to the loop and the sampling". On HEAD~1 the fallback's `try`
covered only the integration. A `ComputationFailureError` from `policy(N_forward, state)` during
sampling (`G < 0`, or now `T_J ≤ 0`, on a sampled state) would have escaped
`compute_scalar_model` and ended the run. Under one `try` it becomes a failure row. No such
failure was seen in any run. The smoke run below samples 5 392 states without one. The z-grid
`RuntimeError` inside the same `try` is not caught (C1).

## Verification performed

**Suites** (from the root, `PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`):

| package | before (`fc97233`) | after |
|---|---|---|
| CosmologyModels | 18 OK | 18 OK |
| ComputeTargets | 57 OK | **67 OK** (+10 new) |
| Datastore | 17 OK | 17 OK |

`black --check` on the three changed Python files: "3 files would be left unchanged".

**README §6.2, every row** (I ran these):

| quantity | target | measured |
|---|---|---|
| `solver_list`, the `while not success` loop | gone; one `try` | gone. `test_g_no_fallback`: no `Name` `solver_list` in the module; no `solver_labels`/`success` name, no `While`, no string containing `BDF`/`LSODA`/`DOP853` in `compute_scalar_model`. Fails on HEAD~1 (`'solver_list' unexpectedly found`) |
| `RuntimeError` raised in `compute_scalar_model` | 1 (z grid) | **1**, `:897`. `test_g_one_runtime_error_in_the_integration_path` counts `Raise` nodes: 0 in `integrate_scalar_history`, 1 in `compute_scalar_model`, the "largest supplied redshift" one. This test also passes on HEAD~1, because prompt 01 had already removed the other five |
| `assert len(...) == EXPECTED_SOL_LENGTH` | assert | `:878` (Deviations 2) |
| `data.d_logV_dphi` | fixed; a forced NaN raises `ComputationFailureError` | `test_a_…` passes. **On HEAD~1 it errors with `AttributeError: 'ODEPolicyData' object has no attribute 'd_logV_dphi'`** |
| `_get_T_Jordan` on `T_J ≤ 0` | raises; the loop rejects the step | `test_b_…` passes (`log_T_Jordan = −1e4`: "T_Jordan = 0" in the message). **On HEAD~1: "ComputationFailureError not raised"**. The rejection itself is the loop's prompt 01 path, exercised by both (f) tests |
| every `ODEPolicy` value on a physical state | P1 (a) at M = 0.5 to 1e-10 rel. | **bit-identical**: see below |
| failsafe `N = 1000` | `ComputationFailureError` | `test_d_…` with `N_failsafe = N₀ + 0.1 = 20.1016…`: `ComputationFailureError` "the failsafe N=20.102 was reached …", not a `RuntimeError`, no traceback text |
| the step budget | present, default `2×10⁶`; message names N, T_J, steps, reflections | `StepControl().step_budget == 2_000_000`, last field. From P2 with `step_budget = 50`: `step budget exhausted: integrate_scalar_history took 51 accepted steps (budget 50) at N=…, T_J=… GeV, with 0 reflection(s)`. HEAD~1: `TypeError: … unexpected keyword argument 'step_budget'` |
| `RHS_timer.__exit__` | prints nothing | removed. Both (f) tests have no marker in the captured streams. The inside-the-timer one fails on HEAD~1 (Deviations 3). `grep -n "print_tb" Quadrature/supervisors/base.py` finds nothing (exit 1) |
| `VERSION_LABEL` | unchanged | `"2026.5.0"` (`grep -n "VERSION_LABEL =" config/version.py` → `:36`) |

**Physical values unchanged, P1 (a) at M = 0.5 to N = 21** (`test_c_physical_values_unchanged`).
The references are log 01's `repr` values for φ, π. The first-bounce pair was printed on the
HEAD~1 export by a scratch script that calls prompt 01's `integrate`/`wall_bounces`; the same
script was then run on this tree:

| quantity | HEAD~1 (`fc97233`) | this tree |
|---|---|---|
| `φ(21)` | 0.12203833994225839 | 0.12203833994225839 |
| `π(21)` | −0.12971527073457434 | −0.12971527073457434 |
| first-bounce `N` | 20.343034651924082 | 20.343034651924082 |
| first-bounce `φ_min` (accepted step) | 0.004573794758933947 | 0.004573794758933947 |
| dense-output minimum `N`, `φ` | 20.343026850496464, 0.004573704679806311 | identical |
| RHS, accepted steps | 1 945, 224 | 1 945, 224 |

The differences are exactly 0, well inside the `1e-10` target.

**Further checks on this tree** (scratch scripts in the session scratchpad, calling prompt 01's
helpers). I ran these.
- The windows. P1 at M = 0.5, 0.01, 1e-10 (to 21), P2 (to 40) and P3 (to 37.5) give RHS 1 945,
  2 120, 1 693, 18 880, 38 547 and end `φ` 0.12203833994225839, 0.11851542297125517,
  0.11844280391325095, 0.019096925077987468, 0.000580781984178963. Each has 0 `T_Jordan = 0`
  prints, 0 "negative value of E" prints and 0 rejected steps. All match log 01.
- **The nine full histories** of README §6.1 (d), driven as log 01 describes, with streams
  captured. RHS, accepted steps, wall bounces, first bounce and `N_final` are identical to log
  01's table in every row: 26 372 / 24 193 / 40 580 / 57 526 / 108 789 / 98 133 / 151 480 /
  271 783 / 327 046 RHS; first bounces 36.15918 / 17.66246 / 20.34303 / 24.48381 / 17.66783 /
  20.35192 / 24.49870 / 20.35208 / 24.49897. All have 0 reflections, 0 rejections, 0
  `T_Jordan = 0` prints, 0 negative-E prints and no traceback text. Wall time is 1.6 s to 20.4 s
  each. Log 01 states that none of the nine ever substituted `T_J`. The `_get_T_Jordan` change
  therefore could not have altered them, and they confirm it.
- **`compute_scalar_model` smoke run.** The undecorated `compute_scalar_model._function`, no
  Ray: β = 2, M = 0.5, a 9 000-point z grid (`10^linspace(36, 0, 9000) − 1`). It returns
  `solver_label = "Radau+kinematic-cap-stepping0"`, 5 392 samples, last `raw_N = 49.6603`,
  `compute_steps = 40 580`, `accepted_steps = 4 469`, and `build_extra_data` = `{cap_fraction
  0.1, cap_floor 1e-11, cap_global_max_step 0.1, jacobian_factor_max 1e-4, accepted_steps
  4469}`. These are log 01's figures. With `SM.StepControl` patched to `step_budget = 50` it
  returns `{"failure": True}` and prints `-- compute_scalar_model (smoke): integration failure`
  / `step budget exhausted: integrate_scalar_history (smoke) took 51 accepted steps (budget 50)
  at N=4.325263426, T_J=269.15 GeV, with 0 reflection(s)` / `!! compute_scalar_model (smoke):
  marked as total integration failure`.

**Breakage on HEAD~1.** I copied the new module into the HEAD~1 export and ran it: 10 tests, 4
failures and 3 errors. They are (a) `AttributeError`; (b) not raised; (d) traceback text from
the supervisor's `__exit__`; (e) ×2, no `step_budget`; (f) inside the timer, traceback text;
(g) `solver_list` found. Three pass on HEAD~1, as they should: (c), which pins unchanged
values; prompt 01's (f) as specified (Deviations 3); and the `RuntimeError` count, already 1
after prompt 01.

**`RayWorkPool` and `store()`** (read, not run). At `RayTools/RayWorkPool.py:430`, `obj.store()`
is called with no `try`. `ScalarModel.store()` calls `ray.get(self._compute_ref)`, which
re-raises a task's exception. A `RuntimeError` in `compute_scalar_model` therefore ends
`main.py`, as README §1 states. The only one left is the configuration error that should.

**Not done here.** No `main.py` run, no Ray, no datastore, as the campaign rules require.

## Observations not acted on

1. **`E < 0` is still clamped, not raised.** `ODEPolicy.__call__` (`:311–319`) prints
   "negative value of E" and sets `E = 0` with the `raise` commented out. This is the same kind
   of silent substitution on an unphysical state that X1 removed for `T_J`. The prompt allows
   no other policy change. It never fired in any run above: the five windows and the nine
   histories had 0 prints. Opened as `[02-negative-E-is-clamped-not-raised-on-trial-states]`
   (board §3).
2. **Each trial-state rejection still prints its policy's `!!` line.** `_get_fm`,
   `_get_T_Jordan`, `G < 0`, the NaN/inf input check and `PotentialDerivativePolicy` all
   `print(msg)` before raising. The traceback that followed each one is gone. The one-line
   reason stays, by design ("the reason is printed"). No issue.
3. **The stale commented-out debug line** in `ODERHS.__call__` (`:486–488`) still reads
   `supervisor._max_step_size`, which no longer exists (log 01 Observations 4). It is a comment
   and outside this prompt's edits to `ODERHS`, so I left it. No issue.
4. **`[01-trial-state-exception-in-radau-start-up-is-not-a-rejection]`** is not in this prompt's
   list and stays open. After this prompt `_get_T_Jordan` raises too, so a `T_J ≤ 0` probe
   inside `Radau.__init__` would now fail the history where it used to be substituted. None was
   seen. A `T_J ≤ 0` probe needs the runaway `jac_factor` that prompt 01 clamps, and the clamp
   applies only after the first step.

## State handed to the next prompt

- **`StepControl`, final shape** (`ComputeTargets/ScalarModel.py:84–109`):
  `namedtuple("StepControl", ["cap_fraction", "cap_floor", "global_max_step",
  "jacobian_factor_max", "atol", "rtol", "step_budget"], defaults=[0.1, 1e-11, 0.1, 1e-4,
  DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE, 2_000_000])`. `compute_scalar_model` builds
  `StepControl(atol=atol, rtol=rtol)`. `integrate_scalar_history`'s signature is unchanged from
  log 01.
- **The exception table as implemented:**

  | condition | where | outcome |
  |---|---|---|
  | RHS raises `ComputationFailureError` on a trial state inside `solver.step()` (including `T_J ≤ 0`, now) | loop `:666–678` | rejected step, `h ← h/2`; below `1e-13` e-folds `ComputationFailureError` |
  | `T_J ≤ 0` (`exp(ln T_J)` underflow) | `ODEPolicy._get_T_Jordan` `:255–263` | prints, `ComputationFailureError` |
  | `G < 0`, overflow in `exp(ln f_m)`/`exp(ln T_J)`, non-finite input; potential overflows | `ODEPolicy`, `PotentialDerivativePolicy` | `ComputationFailureError` (unchanged) |
  | `E < 0` | `ODEPolicy.__call__` | printed, clamped to 0 (unchanged; issue `[02-…]`) |
  | non-finite RHS output | `ODERHS.__call__` `:443–485` | diagnostic print, `ComputationFailureError` |
  | Radau `step()` returns a message | loop | `ComputationFailureError` |
  | step budget exceeded | loop `:689` | `ComputationFailureError` "step budget exhausted: … took n accepted steps (budget b) at N=…, T_J=… GeV, with r reflection(s)" |
  | `φ ≤ 0` accepted; G1/G2 at a reflection; termination root unbracketed | loop (prompt 01) | `ComputationFailureError` |
  | failsafe `N_failsafe` reached | loop `:744` | `ComputationFailureError` |
  | solution dimension ≠ 5 | `compute_scalar_model` `:878` | `AssertionError` (a bug) |
  | `ComputationFailureError` from the loop or the sampling | `compute_scalar_model` `:950` | reason printed; `{"failure": True}` → failure row |
  | z grid too short for `N_final` | `compute_scalar_model` `:897` | `RuntimeError` (configuration; ends the run) |
  | any exception through `RHS_timer` / a supervisor's `with` | `base.py` | timing recorded, nothing printed, exception propagates |

- **The grep commands of §4**, from the root:
  - `grep -n "RuntimeError" ComputeTargets/ScalarModel.py` finds `:856` (a comment) and `:897`
    (the z grid) up to the end of `compute_scalar_model`. Everything after `:1150` is in the
    `ScalarModel` class's accessors, outside the integration path.
    `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_integrator_exceptions.TestFallbackGone -v`
    counts it by `ast`.
  - `grep -n "print_tb" Quadrature/supervisors/base.py` finds nothing.
  - `grep -n "solver_list\|LSODA\|DOP853\|\"BDF\"" ComputeTargets/ScalarModel.py` finds nothing.
- **To reproduce:** `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_integrator_exceptions -v`
  (10 tests, about 0.3 s). The (c) references are the module constants `P1_M0p5_*`.
- **For prompt 03's documents.**
  - Failure reasons a `ScalarModel` failure row can now stand for, all printed and none
    stored: step too small after trial-state rejections; a Radau step message; the step budget;
    the failsafe; `φ ≤ 0`; G1; G2; an unbracketed termination root; and a
    `ComputationFailureError` during sampling.
  - The planned four ("step too small, budget, failsafe, `φ ≤ 0`") are a subset of this list.
  - The supervisors no longer print tracebacks.
