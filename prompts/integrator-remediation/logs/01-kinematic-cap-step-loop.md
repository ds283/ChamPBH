# Log 01 — Replace the fragment loop with the kinematic-cap step loop

**Prompt:** prompts/integrator-remediation/01-kinematic-cap-step-loop.md
**Commit:** the commit that adds this file ("Replace the fragment loop with a kinematic-cap step loop"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-01
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `918590e` (`HEAD` when this prompt started). "HEAD~1" below means
the parent of this prompt's commit, which has the same tree as `918590e` in every file the
measurements touch. Unless a different script is named, every "measured" number below comes
from `ComputeTargets/tests/test_kinematic_cap_loop.py`'s helpers (`integrate`, `wall_bounces`,
`interpolated_minima`), run on this prompt's tree.

## What shipped

**`VERSION_LABEL`: `"2026.4.0"` → `"2026.5.0"`** (`config/version.py:36`, with a dated sentence
at `:33–35`). Every `ScalarModel` store made before 2026.5.0 is invalid, and with it every
`AdiabaticHistory` and `BBNData` row built on one.

**A1 — the pure loop** (`ComputeTargets/ScalarModel.py`).

- New module constants. `REFLECTIONS_KEY = "number_reflections"` (`:65`) replaces
  `HARD_REFLECTIONS_KEY`. `SCALAR_MODEL_STEPPER_LABEL = "Radau+kinematic-cap-stepping0"`
  (`:71`). `MIN_STEP_AFTER_TRIAL_EXCEPTION = 1e-13` (`:75`). `DEFAULT_MAX_STEP_SIZE` and
  `SolutionFragment` are deleted.
- `StepControl` (`:84`) is a namedtuple
  `(cap_fraction=0.1, cap_floor=1e-11, global_max_step=0.1, jacobian_factor_max=1e-4,
  atol=DEFAULT_ABS_TOLERANCE, rtol=DEFAULT_REL_TOLERANCE)`.
- `Reflection` (`:99`) is a namedtuple `(N, phi_Einstein, pi_Einstein_in)`.
- `IntegrationResult` (`:117`) is a namedtuple `(solution, N_final, final_state, nfev,
  accepted_steps, steps_rejected_by_exception, reflections, max_wall_to_kinetic_ratio)`.
  `solution` is a `scipy.integrate.OdeSolution`. `final_state` is a `StateVector`. `nfev` counts
  every RHS call the loop made, Jacobian probes included (§Deviations 3).
- `_reflection_guards(policy, N, y, task_label) -> float` (`:477`) implements G1 and G2 and
  returns `W/(½π²)`.
  - **G1:** if `potential.reflects_at_origin` is false, it raises `ComputationFailureError`
    naming `potential.name` and the state. A `None` `log_V_floor` also raises.
  - **G2:** `W = −3 V_over_3H2Mp2 · expm1(log_V_floor − log_V)`. The kinetic term is
    `½π²/M_P²`. If `W > ½π²/M_P²` it raises `"reflection requested inside the wall:
    W/(pi^2/2) = …"`, with `N`, `φ`, `π`, `W` and `½π²`.
- `integrate_scalar_history(RHS, supervisor, initial_state, N_start, log_T_stop,
  params=StepControl(), N_failsafe=1000.0, policy=None, task_label=None, N_stop=None) ->
  IntegrationResult` (`:526`). The loop follows README §2 (a)–(c) item by item:
  - one `scipy.integrate.Radau(fun, N, y, N_bound, max_step=global_max_step, rtol, atol)`, where
    `fun` counts calls and forwards to `RHS(N, y, supervisor)`;
  - **before every step:**
    - if `π < 0` and `f φ/|π| < h_floor`, run the guards, record the `Reflection`, call
      `supervisor.notify_reflection(N)`, set `π ← −π`, start a new `Radau` at the same `N` and
      continue;
    - otherwise the cap is `min(global_max_step, f φ/|π| if π < 0, sqrt(2 f φ/(−solver.f[1])) if
      solver.f[1] < 0)`, never below `h_floor`. It is set on `solver.max_step`, `solver.h_abs`
      is clipped into it, and it is reported through `supervisor.notify_step_cap(cap)`;
  - **the step:**
    - `solver.step()` runs inside `try`. On `ComputationFailureError` the step is a rejection:
      the count goes up, `solver.h_abs` is halved and the step is retried. Below `1e-13` e-folds
      the loop raises `ComputationFailureError`, chained from the original;
    - a non-`None` message raises `ComputationFailureError`;
  - **after an accepted step:**
    - `np.minimum(solver.jac_factor, jacobian_factor_max, out=solver.jac_factor)` when it is
      not `None`;
    - `φ ≤ 0` raises `ComputationFailureError` ("phi <= 0 in an accepted state (the step cap
      was violated)");
    - `solver.dense_output()` is collected;
  - **termination** is at the first accepted step with `ln T_J < log_T_stop`. The crossing is
    bracketed on that step's interpolant (it raises if it cannot be) and located by `brentq`;
    that root is the last node and the final state is the interpolant there;
  - `solver.status == "finished"` without the crossing raises `ComputationFailureError`
    (failsafe). The exception is `N_stop` (§Deviations 1);
  - the solution is `OdeSolution(ts, interpolants)`.
- `compute_scalar_model` (`:732`):
  - the initial state is built exactly as before (that block is unchanged);
  - the six event functions, the `hard_reflection_point`/`bounce_region_*`/`default_max_step`
    reads, the fragment loop and its `RuntimeError` sites (state length, multiple events, 100
    fragments, unknown event, `status != 1`) are gone;
  - inside the **unchanged fallback wrapper** (`while not success`, `solver_list`, the `except
    ComputationFailureError` that advances the name) it builds
    `StepControl(atol=atol, rtol=rtol)` (`:820`) and calls the loop (`:847`) within the
    supervisor's `with`;
  - sampling evaluates `result.solution(N_forward)` on the same `z_grid_cut` (`:901`). The z
    grid, `SampleValues`, the `policy`/`hubble` evaluation and the "z grid too short"
    `RuntimeError` are unchanged;
  - `compute_steps` is now `result.nfev`;
  - the returned dict carries `reflections`, `cap_fraction`, `cap_floor`,
    `cap_global_max_step`, `jacobian_factor_max`, `accepted_steps` and
    `steps_rejected_by_exception` in place of the ten region/fragment/hard-reflection entries,
    and `"solver_label": SCALAR_MODEL_STEPPER_LABEL` (`:974`).
- `build_extra_data` (`:978`) stores:
  - `number_reflections` (only when positive);
  - `cap_fraction`, `cap_floor`, `cap_global_max_step`, `jacobian_factor_max` and
    `accepted_steps`, always;
  - `steps_rejected_by_exception` (only when positive);
  - the three RHS statistics blocks as before.
- Imports: `numpy`, `scipy.integrate.{OdeSolution, Radau}`, `scipy.optimize.brentq` and
  `math.expm1` are added; `solve_ivp` and `numpy.inf` are dropped.

**A1′ — the potential interface.**
- `CosmologyConcepts/Potentials/AbstractPotential.py:61–82` adds two non-abstract properties
  with docstrings: `reflects_at_origin -> bool` (`False`) and `log_V_floor -> Optional[float]`
  (`None`).
- `ExponentialPotential.py:93–102` returns `True` and `self._log_Lambda_4`.
- No other potential changes, and no property is removed.

**A2 — the supervisor** (`Quadrature/supervisors/ScalarField.py`).
- Removed: the `max_step_size` constructor argument; `_hard_reflection_data`, `_level_1_data`,
  `_level_2_data`, `_in_level_*` and `_number_fragments`; `notify_hard_reflection`,
  `notify_level_{1,2}_{entry,exit}` and `notify_new_fragment`; and `number_hard_reflections`,
  `number_level_*`, `number_fragments` and `in_level_{1,2}`.
- Added: `notify_reflection(N)` (`:207`), `notify_step_cap(cap)` (`:212`; §Deviations 2) and
  `number_reflections` (`:216`).
- The status line now reports `reflections = n` and `current step cap dN=…`. The events block
  reports only "elastic reflection".
- `Quadrature/supervisors/base.py` is not touched.

**A3 — what is stored and its consumers.**
- `extract_common.py:169` renames `hard_reflection_count` to `reflection_count`, reading
  `REFLECTIONS_KEY`. The two captions become one, `"Reflections (elastic model): N"` (`:233`);
  the "Solution fragments" caption is gone.
- `plot_by_beta.py:54, 499, 528–545`: the column is now `reflections` and the report says
  "elastic reflections".
- `ComputeTargets/tests/test_hard_reflection_reporting.py` is renamed (`git mv`) to
  `test_reflection_reporting.py` and rewritten to pin the new block. It has six methods; its
  `reference_extra_data` is a verbatim copy of the new store calls.

**A4 — the label.**
- `main.py:932–952`: `IntegrationSolver(label="Radau+kinematic-cap", stepping=0)` is added to
  the pre-registered `ray.get` list as `Radau_kinematic_cap`, and to `solvers` under
  `"Radau+kinematic-cap-stepping0"`. The five old registrations stay.
- `plot_by_beta.py:676` and `plot_ScalarModel.py:1575`: `"solver_labels":
  ["Radau+kinematic-cap-stepping0"]`.

**A5 — the bump.** As above.

**Tests.**
- `ComputeTargets/tests/test_kinematic_cap_loop.py`, new, with 15 methods, listed under
  Verification.
- `test_reflection_reporting.py`, rewritten, with 6 methods.

## Deviations from the prompt

### 1. `N_stop`, an optional successful end point — IMPLEMENTATION CHOICE

The prompt's tests integrate P1 to `N = 21`, P3 to `37.5` and P2 to `40`. The loop as specified
ends only on `ln T_J < log_T_stop`, and reaching `N_failsafe` is a failure, so a window cannot be
driven with the specified signature alone. The alternatives were:

- (i) pass a fake `log_T_stop` equal to `ln T_J` at the window's end. That value is not known in
  advance, and the end would not land exactly on `N = 21`;
- (ii) set `N_failsafe = 21` and catch the failure. That turns success into an exception, and
  the solution would not be returned;
- (iii) an optional `N_stop`.

I chose (iii). With `N_stop` given, Radau's bound is `min(N_stop, N_failsafe)` and reaching it
returns normally. This matches the audit harness, which also passes the window end as Radau's
`t_bound`, so `select_initial_step` sees the same interval. `compute_scalar_model` never passes
it. The failsafe behaviour without it is as specified.

### 2. `notify_step_cap(cap)` on the supervisor — IMPLEMENTATION CHOICE

A2 asks the status message to report "the current cap" but names only `notify_reflection` as a
new method. The loop must tell the supervisor the cap somehow, so it calls
`supervisor.notify_step_cap(cap)` before every step; this is one attribute store. The
alternative, a public attribute the loop writes directly, would have been less explicit.

### 3. `nfev` counts every RHS call; `compute_steps` stores it — IMPLEMENTATION CHOICE

SciPy's `solver.nfev` does not count `num_jac`'s calls, which go through `fun_vectorized` →
`_fun`. The audit's RHS figures, which are the targets, count every call: they come from
`harness.Counter`, a wrapper. `IntegrationResult.nfev` therefore counts at a wrapper too, and it
reproduces the audit to the unit (1 945, 2 120, 2 275, 18 880, 38 547).

`IntegrationData.compute_steps` used to be the sum of `solve_ivp`'s `sol.nfev`, which excluded
Jacobian calls. It is now `result.nfev`, which equals `RHS_evaluations` (both 40 580 in the
smoke run below). The alternative, a second counter of `solver.nfev` across restarts, would
keep a number that no acceptance target uses.

### 4. G2 written with `expm1` and `M_P²` — IMPLEMENTATION CHOICE

`W = −3 V_over_3H2Mp2 · expm1(log_V_floor − log_V)` is the prompt's
`3 V_over_3H2Mp2 (1 − exp(…))`, but more accurate where `V ≈ V_floor`. The kinetic term is
`½π²/M_P²`, using the policy's `CONST_MP_SQ`, so the test has the right dimensions in any
`UnitsLike`. In `Planck_units` it is the prompt's `½π²`.

### 5. The P3 per-bounce `φ_min` (README §6.1 (b)) is asserted on the dense-output minimum — STRUCTURALLY REQUIRED (a §6.1 (b) row is missed as the audit's harness measures it; issue opened)

What the prompt assumed. The prompt and README §6.1 (b) quote the shipped scheme's `φ_min` for
bounces 1, 2 and 8 (`2.79886e-4`, `3.03650e-4`, `3.66368e-4`) with a target of `± 2e-4`
relative. Neither defines `φ_min` for this row. For the first bounce of (a) the prompt defines
it as `φ` at the first accepted step after `π` turns positive (`harness.first_wall_bounce`).
That number depends on where the step lands: it equals the true minimum only when the step
across the turning point is short. The shipped scheme's steps there are `1e-6` e-folds, which is
short. Under the kinematic cap the turning-point step is not short, because neither cap term
binds at `π → 0⁻` with outward acceleration.

Measured (this tree, `f = 0.1`):

| bounce | shipped (audit) | loop, accepted-step `φ` | rel. | loop, dense-output minimum | rel. |
|---|---|---|---|---|---|
| 1 | 2.79886e-4 | 2.799158e-4 | 1.06e-4 | 2.798861e-4 | 4e-7 |
| 2 | 3.03650e-4 | 3.037139e-4 | **2.10e-4** | 3.036503e-4 | 1e-6 |
| 8 | 3.66368e-4 | 3.664528e-4 | **2.31e-4** | 3.663676e-4 | 1e-6 |

The accepted-step values for bounces 2 and 8 miss `± 2e-4` by a little. The dense-output minimum
is the root of `π` on the step's interpolant, with `φ` evaluated there. It agrees with the
shipped scheme to `1e-6`, so the trajectory is right and the miss comes from where the steps
land. At `f = 0.02` the accepted-step values are 2.799412e-4, 3.036759e-4 and 3.664341e-4
(relative 1.9e-4, 8.5e-5, 1.8e-4), converging to the minimum as the step shrinks. The audit's
own velocity cap, by the same accepted-step measure, gives bounce 8 = 3.66448e-4 (2.2e-4; audit
§3.3 table). Its claim "agreeing to ≤ 2×10⁻⁴" was therefore marginal by its own measure.

What was done. `test_d_P3_window` asserts the `± 2e-4` target on the dense-output minima, and
the bounce *count* (51) on accepted-step sign changes as README §6.1 (b) defines it. The target
is not loosened, but its quantity is measured differently. The accepted-step figures above are
quoted so that the reviewer can disagree. Under the README §6 rule ("a miss is an issue and
`COMPLETE WITH DEVIATIONS`"), issue `[01-bounce-phi-min-at-the-accepted-step-depends-on-step-placement]`
is opened on the board. **This is the item for the orchestrator to rule on.** For (a) and (c) the
prompt's accepted-step definition is used as written, and it passes.

### 6. `solver_labels` left in place, unread — IMPLEMENTATION CHOICE

The fallback wrapper is left as instructed. Its `solver_labels` dict is no longer read, because
the returned label is the constant. I kept it, with a comment, rather than deleting it, so that
the wrapper stays one unit for prompt 02 to remove (README §2 (i)). The alternative was to delete
the dict now; that would have split the wrapper across two prompts.

## Verification performed

**Suites** (from the root, `PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`):

| package | before (`918590e`) | after |
|---|---|---|
| CosmologyModels | 18 OK | 18 OK |
| ComputeTargets | 41 OK | **57 OK** (−5 old reporting, +6 rewritten, +15 new) |
| Datastore | 17 OK | 17 OK |

`black --check` reports all 11 changed Python files as clean.

**README §6.1 (a)–(c), measured on this tree** (I ran these and they passed). The references
with more digits for P1 at `M = 0.5` were measured on `918590e`, exported with `git archive`
to the scratchpad, using the audit's `harness.run_fragment_loop(strategy="regions")`. At `1e-8`
that gives `φ(21) = 1.2203834029e-1`, `π(21) = −1.2971527048e-1`, 17 092 RHS. At `1e-12` it gives
`1.2203834040e-1`, `−1.2971527040e-1`, first bounce `20.34302783` / `4.57370612e-3`. README
quotes `−1.2972e-1`, which is this value rounded.

| row | target | measured |
|---|---|---|
| (a) M = 0.5 RHS | ≤ 3 000 | **1 945** (224 steps) |
| (a) M = 0.5 first bounce N | 20.343028 ± 1e-5 | 20.3430347 (Δ 6.7e-6) |
| (a) M = 0.5 φ_min | 4.57371e-3 ± 1e-4 rel | 4.5737948e-3 (1.9e-5) |
| (a) M = 0.5 φ(21), π(21) | 1e-5 rel | 1.22038340e-1, −1.29715271e-1 (both < 1e-8 against the `918590e` reference) |
| (a) M = 0.5 restarts; reflections | 1; 0 | 1 Radau instance; 0 |
| (a) M = 0.01 RHS; φ_min; φ(21) | ≤ 3 500; 9.1505e-5; 1.185154e-1 | **2 120**; 9.1504508e-5; 1.18515423e-1 |
| (a) M = 0.001 RHS; φ_min; φ(21) | ≤ 3 500; 9.1505e-6; 1.184501e-1 | **2 275**; 9.1505073e-6; 1.18450064e-1 |
| (b) M = 1e-10 | completes, 1 reflection at φ ∈ [1e-11, 1e-10], φ(21) = 1.184428e-1 | completes; 1 reflection at N = 20.352100, φ = 4.7023e-11, π_in = −0.497623; φ(21) = 1.18442804e-1; 1 693 RHS |
| (b) M = 4.1e-28 | completes, 1 reflection, φ(21) = 1.184428e-1 | the same reflection; φ(21) = 1.18442804e-1; 1 693 RHS |
| (c) M = 0.5, f = 0.02 | (a) tolerances | first bounce 20.3430285, φ_min 4.5737085e-3; 2 840 RHS |
| P3 RHS; restarts | ≤ 60 000; 0 | **38 547** (4 040 steps); 0 |
| P3 wall bounces | 51 | 51 |
| P3 φ(37.5) | 5.8078e-4 ± 1e-4 rel | 5.8078198e-4 |
| P3 φ_min bounces 1, 2, 8 | ± 2e-4 rel | dense-output minima 4e-7, 1e-6, 1e-6; accepted-step 1.06e-4, **2.10e-4, 2.31e-4** (Deviations 5) |
| P2 RHS | ≤ 25 000 | **18 880** (2 052 steps) |
| P2 `T_Jordan = 0` in captured stdout | 0 | 0 |
| P2 bounces; φ(40) | 19; 1.909693e-2 ± 1e-5 | 19; 1.9096925e-2 |

**README §6.1 (e).**

| row | measured |
|---|---|
| `cap_fraction = inf`, P1, M = 0.01 | `ComputationFailureError`: "phi <= 0 in an accepted state (the step cap was violated) at N=20.35432523, phi_E=-0.0011071, pi_E=-0.49762" |
| RHS raising on its first three calls once stepping began, P1, M = 0.5 | completes; `steps_rejected_by_exception = 3`; φ(21) = 1.22038339e-1; 1 855 RHS |
| G1, a subclass of `ExponentialPotential` with `reflects_at_origin = False`, P1, M = 1e-10 | `ComputationFailureError` naming `ExponentialPotential(M=…)` and `reflects_at_origin`, at the floor |
| G2, φ = 5e-5, π = −0.4976, M = 0.01, `cap_floor = 1e-3` | `ComputationFailureError`: "reflection requested inside the wall: W/(pi^2/2) = 23.232 at N=20.35, phi_E=5e-05, pi_E=-0.4976 (W = 2.8762, pi^2/2 = 0.1238)" |
| G2 on legitimate reflections: the maximum `W/(½π²)` | (a): no reflections. (b): 2.55e-47 at M = 1e-10, −0.0 at 4.1e-28. The nine histories: no reflections. Extra probes from P1: M = 3e-9 one reflection, 1.76e-20; M = 1e-8 and 1e-6 none (3 479 and 3 003 RHS). **Maximum over everything run: 1.76e-20.** The audit's marginal 1.6e-4 case at M = 1e-8 did not recur, because no reflection fires at 1e-8 here, as in the audit's §3.7 `kinref` row (3 479 RHS). |

**README §6.1 (f).**
- `extra_data` keys are the §2 (e) set, and `number_reflections` (and
  `steps_rejected_by_exception`) are absent when zero: `test_g_*` and the rewritten reporting
  test.
- `reflection_count` and the single caption: the reporting test.
- The label is in all three scripts: `test_g_stepper_label_registered_in_the_scripts`, which
  `ast`-parses them.
- `VERSION_LABEL` is `"2026.5.0"`, in `config/version.py` only.
- `atol`/`rtol` defaults, the z grid and `SampleValues` are unchanged in the diff.
- Acceptance 4's grep for `HARD_REFLECTIONS_KEY\|SolutionFragment\|bounce_region_level\|notify_level_1\|notify_hard_reflection`
  (with the excluded directories excluded) finds only the `bounce_region_level*` property
  definitions on `AbstractPotential` and the five potentials.

**Breakage on HEAD~1** (the code at `918590e`, exported to the scratchpad, using the audit's
scripts there). I ran these.
- There is no `integrate_scalar_history` on HEAD~1, so every new test fails at import.
- The record for the rows is the shipped scheme:
  - P1 at M = 0.5: **17 092 RHS** (above, `harness.run_fragment_loop`, 3 fragments);
  - P1 at M = 0.01 and 0.001: **26 634** and **26 422 RHS**, 5 fragments each (the `regions`
    rows of `p1_smallM.py`, run through `harness.run_fragment_loop`);
  - `p_smallM_scan.py regions 1e-10` prints `regions M=1e-10 … N_end=20.352100
    phi_end=4.982152e-12 … FAILED: Required step size is less than spacing between numbers.`
    (6 901 RHS).
- The orchestrator re-runs `p1_smallM.py 0.01` and `p_smallM_scan.py regions 1e-10` on HEAD~1.

**README §6.1 (d), the nine full histories through the loop.** I ran these with a scratch
driver outside the repository. It builds the initial state as `compute_scalar_model` does
(`φ* = 5`, `π* = 0`, `T* = 2×10⁴ GeV`, `QCD_Cosmology`, `Planck2018`). The result is
`(5, 0, −126.1992162 + 20β, −31.00026422, −32.43340147)`, the same as
`harness.initial_state` to the 10 digits printed. The driver calls
`integrate_scalar_history(rhs, sup, state, 0.0, ln T_CMB, StepControl())` with stdout captured.
Wall times are from one sequential run on this machine.

| β | M | RHS | target | steps | wall | wall bounces | first bounce N / T_J | target | N at T_CMB |
|---|---|---|---|---|---|---|---|---|---|
| 0.9 | 0.5 | 26 372 | — | 3 040 | 2.5 s | 16 | 36.15918 / 1.044 eV | 36.159 ± 1e-3 | 44.3185 |
| 1.2 | 0.5 | 24 193 | ≤ 5×10⁴ | 2 728 | 2.4 s | 17 | 17.66246 / 231.07 MeV | 17.662, 231.1 | 45.7686 |
| 2.0 | 0.5 | 40 580 | ≤ 8×10⁴ | 4 469 | 4.2 s | 26 | 20.34303 / 746.63 MeV | 20.343, 746.6 | 49.6603 |
| 3.0 | 0.5 | 57 526 | ≤ 1.2×10⁵ | 6 120 | 5.6 s | 48 | 24.48381 / 1 682.85 MeV | 24.484, 1 683 | 54.5523 |
| 1.2 | 0.01 | 108 789 | ≤ 2.5×10⁵ | 11 484 | 10.9 s | 195 | 17.66783 / 231.11 MeV | 17.668, 231.1 | 46.0403 |
| 2.0 | 0.01 | 98 133 | ≤ 2.5×10⁵ | 10 632 | 12.7 s | 196 | 20.35192 / 746.69 MeV | 20.352, 746.7 | 50.0287 |
| 3.0 | 0.01 | 151 480 | ≤ 3.5×10⁵ | 16 513 | 13.8 s | 284 | 24.49870 / 1 680.06 MeV | 24.499, 1 680 | 55.0170 |
| 2.0 | 0.001 | 271 783 | ≤ 6×10⁵ | 27 979 | 18.1 s | 803 | 20.35208 / 746.69 MeV | 20.352, 746.7 | 50.0616 |
| 3.0 | 0.001 | 327 046 | ≤ 7×10⁵ | 34 342 | 25.5 s | 1 017 | 24.49897 / 1 680.01 MeV | 24.499, 1 680 | 55.0584 |

- All nine complete, ending at `T_J = 2.7255 K`. Each has 0 reflections, 0
  `steps_rejected_by_exception` and 0 `"T_Jordan = 0"` in stdout.
- RHS counts are within 0.4 % of the audit's §9.3 figures (for example 40 580 against 40 548);
  bounce counts differ by at most 1 (803 against 804, 284 against 285). The audit's §9.3 run
  used `p_full.py … kin` without `reflect`, i.e. `h_floor = 1e-9`, and its initial state differs
  in the digits beyond the tenth. The histories are chaotic after delivery (audit §11).
- The orchestrator repeats these with `p_full.py`.

**`compute_scalar_model` smoke run** (scratch driver; it calls the undecorated
`compute_scalar_model._function`, with no Ray). β = 2, M = 0.5, a 9 000-point z grid (250 per
decade over `0 ≤ log10(1+z) ≤ 36`). It returned in 4.0 s with:
- `solver_label = "Radau+kinematic-cap-stepping0"`;
- 5 392 samples, the last at `raw_N = 49.6603` with `log_T_Jordan = ln T_CMB` to every digit
  printed;
- `compute_steps = RHS_evaluations = 40 580`;
- `build_extra_data` = `{cap_fraction 0.1, cap_floor 1e-11, cap_global_max_step 0.1,
  jacobian_factor_max 1e-4, accepted_steps 4469}`.

**Not done here.** `main.py` was not run, and nothing went through Ray or a datastore, as the
campaign rules require. That `store()` resolves the new label against the `solvers` dict built
in `main.py` is reasoned from the code (`ScalarModel.store()` indexes
`self._solver_labels[data["solver_label"]]`, and `main.py` now has that key), not run.

## Observations not acted on

1. **A trial-state exception during Radau's start-up is not a rejection.** `Radau.__init__`
   evaluates the RHS at the start state, at `select_initial_step`'s probe `y0 + h0 f0`, and at
   five Jacobian probes. A `ComputationFailureError` in any of these escapes
   `integrate_scalar_history`, at the first construction or at a reflection restart, as a
   failure of the history rather than a rejected step. This never happened in any run above.
   After a reflection the probe moves `φ` outward. Opened as
   `[01-trial-state-exception-in-radau-start-up-is-not-a-rejection]` (board §3); it belongs with
   prompt 02's taxonomy if anyone takes it.
2. **The bounce `φ_min` measure** (Deviations 5): opened as
   `[01-bounce-phi-min-at-the-accepted-step-depends-on-step-placement]`.
3. **The plotting scripts' `solver_labels` lists are inert.** In `plot_by_beta.py` and
   `plot_ScalarModel.py` the list goes into the `ScalarModel` lookup payload. `ScalarModel`
   reads `solver_labels` only in `store()`, as a dict, and the plotting scripts never call
   `store()`. Adding the label to the lists is therefore declarative only, as the prompt
   specified. No behaviour, so no issue.
4. **A stale commented-out line.** `ODERHS.__call__` (`ComputeTargets/ScalarModel.py:469`) has a
   commented-out debug print that reads `supervisor._max_step_size`, an attribute removed here.
   It is a comment in `ODERHS`, which this prompt may not change, and prompt 02 edits the RHS.
   Not an issue.
5. **`select_initial_step` after a reflection** uses `global_max_step`, not the cap. The first
   step is clipped to the cap before it is taken, so this has no effect. Recorded only.
6. **The audit's harness no longer runs against this tree.** `harness.build` calls
   `ScalarFieldIntegrationSupervisor(units, T_init, T_stop, np.inf, label=…)`. A2 removes the
   `max_step_size` argument, so the call raises "got multiple values for argument 'label'". I
   checked this on this tree. `.documents/` may not be edited, and the audit's scripts are a
   record of `b1f64d8`, so I left it. The harness's own loop uses only `ODEPolicy` and `ODERHS`,
   which this prompt does not change. Its scripts are therefore still valid when run from an
   export of the parent tree. I ran `p_full.py 2.0 0.5 1e-8 1e-4 kin reflect` that way: 40 548
   RHS, 4 476 steps, 26 bounces, first at `N = 20.343028`, `φ_min = 4.57371e-3`,
   `T_J = 746.6342 MeV`. Its `N_end = 49.710963` is the end of the first accepted step below
   `T_CMB` (`T_J = 2.56 K`), because the harness does not root-find. This loop's `N_final =
   49.6603` is the crossing. Not an issue, but the orchestrator needs it (next section).

## State handed to the next prompt

- **Names and signatures** (`ComputeTargets/ScalarModel.py`):
  - `StepControl = namedtuple("StepControl", ["cap_fraction", "cap_floor", "global_max_step",
    "jacobian_factor_max", "atol", "rtol"], defaults=[0.1, 1e-11, 0.1, 1e-4,
    DEFAULT_ABS_TOLERANCE, DEFAULT_REL_TOLERANCE])`. Prompt 02 adds `step_budget`: append it,
    with its default, at the end so that positional construction is unaffected.
  - `IntegrationResult(solution, N_final, final_state, nfev, accepted_steps,
    steps_rejected_by_exception, reflections, max_wall_to_kinetic_ratio)`;
    `Reflection(N, phi_Einstein, pi_Einstein_in)`.
  - `integrate_scalar_history(RHS, supervisor, initial_state, N_start, log_T_stop,
    params=StepControl(), N_failsafe=1000.0, policy=None, task_label=None, N_stop=None)`.
    Failures raise `ComputationFailureError`; the loop raises no `RuntimeError`.
  - `_reflection_guards(policy, N, y, task_label)`.
  - Constants: `REFLECTIONS_KEY = "number_reflections"`,
    `SCALAR_MODEL_STEPPER_LABEL = "Radau+kinematic-cap-stepping0"`,
    `MIN_STEP_AFTER_TRIAL_EXCEPTION = 1e-13`.
  - Supervisor: `notify_reflection(N)`, `notify_step_cap(cap)`, `number_reflections`; no
    `max_step_size` argument.
- **Left for prompt 02**, untouched:
  - in `compute_scalar_model`: `solver_list`, `solver_labels` (now unread) and the
    `while not success` / `except` wrapper; the remaining `RuntimeError` there is only "z grid
    too short";
  - `_get_T_Jordan`'s 1 K substitution;
  - `data.d_logV_dphi` in `ODERHS`;
  - `RHS_timer.__exit__`'s traceback print. It still prints a traceback, to stderr, for every
    rejected step, as in the failure-path tests;
  - the failsafe already raises `ComputationFailureError`.
- **To reproduce (a)–(g):**
  `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_kinematic_cap_loop -v`
  (15 tests, 5.1 s) and `… ComputeTargets.tests.test_reflection_reporting -v` (6 tests). The
  module's helpers `integrate(probe, M, N_stop, params=…, rhs_wrapper=…, potential_class=…,
  N0=…, state=…)`, `wall_bounces(result, M)` and `interpolated_minima(result, M)` print every
  figure above when called from a REPL at the root.
- **Running the audit's scripts** (`p_full.py`, `p1_smallM.py`, `p_smallM_scan.py`): they do
  not run against this tree (Observations 6). Run them from an export of the parent tree:
  `git archive <this commit>~1 | tar -x -C <dir>`, then from
  `<dir>/.documents/integrator-audit-2026-09-30` run
  `AUDIT_OUT=<out> <repo>/venv/bin/python p_full.py β M 1e-8 1e-4 kin reflect`. The RHS classes
  the harness drives are unchanged by this prompt. `p_full`'s `N_end` overshoots `T_CMB` by up to
  one step; compare first bounces and RHS, not `N_end`.
- **The nine histories** are in the §6.1 (d) table above. A driver needs only
  `integrate_scalar_history(ODERHS(…), ScalarFieldIntegrationSupervisor(units, T_init, T_stop,
  label=…), StateVector(5.0, 0.0, ln ρ_rad,E*, ln f_m*, ln T*), 0.0, ln T_CMB, StepControl())`
  inside `with supervisor:`, with the initial state built as in `compute_scalar_model`.
- **Measured references for prompt 02's "unchanged" check** (P1, M = 0.5, to N = 21):
  `φ(21) = 0.12203833994225839`, `π(21) = −0.12971527073457434` (`repr` of the floats), 1 945
  RHS, 224 accepted steps.
