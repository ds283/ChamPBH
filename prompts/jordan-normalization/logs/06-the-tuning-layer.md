# Log 06 — the tuning layer

**Subject:** Tune each history's normalization and reference point to H0 at T_CMB
**Commit:** `1b83bbd`, on branch `jordan-normalization`, on top of `8db352b`
**Date:** 2026-10-26, by Claude Opus 5.5
**Result:** **COMPLETE**, with no stop.
- T1–T7 shipped under orchestrator note 06, whose seventeen rulings govern where they differ from
  the prompt.
- The golden history (`golden_history_05.json`) passes **unchanged and bitwise** through
  `integrate_history`. No existing golden JSON was written.
- `tune_history` reproduces the orchestrator's prototype: **both comparison tables equal note 06's
  §0 in every integration count, step count and digit shown** (the wall times aside).
- The secant never needs more integrations than the plain update: 73 against 116 over the thirteen
  points, fewer at twelve and as many at one (β = 3, M = 10⁻⁵). The methods agree to
  |Δ ln Λ| ≤ 4.2 × 10⁻⁶ and |Δφ_ref|/φ_ref ≤ 2.0 × 10⁻⁵, below 10⁻⁴ everywhere.
- A tuned golden history is pinned: `ComputeTargets/tests/data/golden_history_06_tuned.json`.
- Counts: ComputeTargets went from 205 (61 + 144) to 226 (61 + 165), +21 (`test_tuning.py`).
  CosmologyModels 26, ObjectFactories 86, tools 102 and RunDrivers 61 are unchanged.
- All three deliberate breakages are caught.
- No issue was opened or closed.

## What shipped

All in `ComputeTargets/ScalarHistory.py` unless named.
- **T1: `integrate_history`** (ruling 1). A plain function, not `@ray.remote`, with
  `compute_scalar_history`'s signature and order:
  `integrate_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model, task_label="integrate_history", atol=DEFAULT_ABS_TOLERANCE, rtol=DEFAULT_REL_TOLERANCE, verbose=False)`.
  Its body is `compute_scalar_history`'s, moved unchanged except that the function's own name in
  its printed lines, in the `RuntimeError` and in the assertion's message is now
  `integrate_history` (six strings; Deviation 1). The docstring says that it is the primitive
  (R8): one realized model, any T_stop, no normalization logic. A diff of the moved body against
  `8db352b`'s `compute_scalar_history` shows only those strings, the decorator, the name, the
  default label and the docstring's new paragraph.
- **`compute_scalar_history`** (ruling 2) is still the `@ray.remote`, with its signature, defaults
  and docstring unchanged. Its body is `return integrate_history(...)`, with every argument passed
  by name. `ScalarHistory.compute`, `main.py`, `golden_history.py`, the tools and every other
  caller are unchanged.
- **The constants** (rulings 4, 7, 10): `DEFAULT_TUNING_TOLERANCE = 1.0e-5`,
  `DEFAULT_TUNING_MAX_INTEGRATIONS = 30`, `TUNING_GUARD_LIMIT = 4`, and `TUNING_OUTPUT_KEYS`, the
  fifteen keys of ruling 10 in its order. `config/defaults.py` is not edited.
- **T2: the residuals** (ruling 5).
  - `tuning_converged(residual_H, residual_phi, tolerance)`, true iff
    `abs(residual_H) < tolerance and abs(residual_phi) < tolerance`.
  - `_tuning_state(cosmology, model, rm, data)` forms today's state from `data["sample"][-1]` by
    ruling 5's expressions, the prototype's, in their order. It returns a `TuningState`
    namedtuple: `phi_0`, `pi_0`, `log_V`, `V_target`, `phi_min`, `residual_H`, `residual_phi`.
    `effective_minimum`'s `ValueError` propagates to the loop.
  - The prompt's r_H = ln(H_J/H0) and r_φ = φ_ref − φ_min are not formed.
- **T3: the step, `TuningSecant(secant=True)`** (ruling 6). It holds B, the last point, its F and
  the kind of the last step, and nothing else.
  - `step(x, G)` returns `(x_new, kind)`. A plain step sets B = −I and returns G itself. A secant
    step (the secant on and B set) applies Broyden's update and solves B d = −F by `_solve2`
    (Cramer's rule, the prototype's `solve2`). The twice rule applies when the point came from a
    secant step and ‖F‖ > 2‖F_prev‖. Every expression is the prototype's.
  - `guard()` resets B, the last point, its F and the last kind.
  - `B` is a read-only property, a copy (None when unset), for the tests.
- **T3, T4, T5, T6: `tune_history`** (rulings 4, 7, 8, 9).
  - **The signature:**
    `tune_history(cosmology, model, T_init, phi_init, pi_init, z_grid, task_label="tune_history", atol=DEFAULT_ABS_TOLERANCE, rtol=DEFAULT_REL_TOLERANCE, tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS, secant=True, verbose=False)`.
  - **T_stop** is `cosmology.T_CMB_Kelvin * units.Kelvin`, a float, with no argument for it.
  - **The seed** is `model.seed(cosmology)`, called once per tuning (T4, R13): no cache, and nothing
    from another history.
  - **What it reads of the model:** only `realize`, `renormalize`, `normalization_coordinate`,
    `normalization_from_coordinate`, `effective_minimum`, `seed` and the `RealizedModel`'s methods
    and properties. It never reads V₀, M, n or β.
  - **After integration k**, in ruling 7's order:
    1. the state and the residuals, then the progress line, then the convergence test;
    2. `scale` = φ_min, set at integration 0 only;
    3. the guard: on V_target ≤ 0 the count rises, and the fourth in a row fails. Otherwise the
       next iterate is `renormalize(norm, phi0, 0.5 * exp(logV))` with φ_ref = φ_min, and the
       stepper's `guard()` is called. A positive V_target resets the count;
    4. the cap, `k + 1 >= max_integrations`;
    5. otherwise the stepper's step on x = (`normalization_coordinate(norm)`, φ_ref/scale), with
       G = (`normalization_coordinate(renormalize(norm, phi0, V_target))`, φ_min/scale).
  - **The kinds** are `seed`, `plain`, `secant` and `guard`, counted per step.
  - **The failures** (T5, ruling 8), each through `_failure_payload`, with one line
    `!! tune_history (<label>): <reason>` (Deviation 8):
    - `the normalization did not converge: after N integrations, H/H0-1=%+.3e, phi_ref/phi_min-1=%+.3e`;
    - `no normalization reaches H0: V_target <= 0 at 4 integrations running, the last at integration k`;
    - `the effective minimum was not found: <the ValueError's text> (tuning integration k)`;
    - an integration's own reason, unchanged, with ` (tuning integration k)` appended.

    `pipeline_selection.py` is not edited.
  - **The progress line** (T6, ruling 9), printed with `flush=True` after the residuals are formed,
    the converging integration's included:
    `-- tune <task_label> it=<k> kind=<kind> H/H0-1=<%+.3e> phi_ref/phi_min-1=<%+.3e> norm=<%.12g> phi_ref=<φ_ref/M_P, %.10e> wall=<%.1f>`.
    `wall` is the `perf_counter` interval of that integration's `integrate_history` call. An
    integration that fails, or whose effective minimum is not found, prints no such line.
  - **What `StageTracker.feed` does with it: nothing.** `_STAGE_BANNER`
    (`^\s*(?:\*\*|>>)\s+(\S.*?)\s*$`) does not match a line that begins `-- `. `_QUEUE_PROGRESS`
    (`… work items remaining = …% complete`) and `_QUEUE_COMPLETE` (`ALL WORK ITEMS COMPLETE`) do
    not occur in it. So the stage does not move, and `feed` returns False; test (c) checks it on
    every line. Forwarded by Ray to the driver's stdout, the line is output, so it beats the
    heartbeat. The failure line begins `!! `, which no pattern matches either.
- **T7: the return** (ruling 10). The final integration's payload, every key as
  `integrate_history` returns it, plus `TUNING_OUTPUT_KEYS`, each a Python `float`, `int` or `str`:
  - `normalization`;
  - `V_ref` = `exp(rm.log_V(phi_ref))` on the final realization;
  - `phi_ref`, `phi_0`, `pi_0`, and `tracking` = `pi0 / (1.5 * phi0)`;
  - `residual_H`, `residual_phi`;
  - `tuning_iterations`, `plain_steps`, `secant_steps`, `guard_steps`;
  - `seed_kind`, `seed_normalization`, `seed_phi_ref`.
- **Imports:** `from time import perf_counter`; `AbstractModel` and `Seed` beside `RealizedModel`.
- **`ComputeTargets/tests/test_integrator_exceptions.py`** (ruling 3): `TestFallbackGone`'s two
  tests look up `integrate_history`, and the comment says why. Nothing else in the file changes.
- **The tests and the tuned golden history** (rulings 11, 12): `ComputeTargets/tests/test_tuning.py`,
  `ComputeTargets/tests/golden_tuned_history.py` and
  `ComputeTargets/tests/data/golden_history_06_tuned.json` (below).
- **The comparison** (ruling 13): `prompts/jordan-normalization/logs/06-probes/`, with its
  `README.md`, the three scripts, the 26 logs, `driver.log` and `table.txt`.
- **The records:** this log and the board (row 06, T1–T7, the gate "Tuning", §5.7).
  `.documents/OPEN_ISSUES.md` is unchanged (no issue opened or closed).

## Tests added or changed

**Added: `ComputeTargets/tests/test_tuning.py`** (21 tests, in the `[g-z]` half; no Ray, no store).
Its docstring and each class's or test's docstring give the cost. The module is loaded with
`importlib.import_module("ComputeTargets.ScalarHistory")` as `SM`, as `test_kinematic_cap_loop.py`
does, because `ComputeTargets` exports a class of the same name.
- **(a) `TestConvergenceTest`** (no integration):
  - `test_a_both_inside`: (0, 0) and (−9.9e-6, 9.9e-6) at 1e-5 are converged;
  - `test_a_each_outside_alone`: (±2e-5, 0) and (0, ±2e-5) are not;
  - `test_a_the_tolerance_itself_is_outside`: (1e-5, 0), (0, −1e-5) and (1e-5, 1e-5) are not.
- **(b) `TestTheStepper`** (synthetic maps, no integration, no model):
  - `test_b_the_first_step_is_the_plain_map_bitwise`: B is None before; the first step is `plain`,
    returns G bitwise (`float.hex`), and leaves B = −I;
  - `test_b_the_second_step_is_a_secant`: the second step is `secant`. The updated B satisfies the
    secant condition B dx = dF, and the step solves B d = −F, each to 14 places;
  - `test_b_after_guard_the_next_step_is_plain_and_B_is_minus_identity`: after a plain and a secant
    step B ≠ −I. After `guard()`, B is None, and the next step is `plain`, returns G, and leaves
    B = −I;
  - `test_b_the_twice_rule`: from the point a secant step produced, a G with ‖F‖ three times the
    last gives a `plain` step that returns G, with B = −I;
  - `test_b_the_secant_needs_fewer_steps_on_an_affine_contraction`: G(x) = A x + b,
    A = ((0.6, 0.3), (−0.2, 0.7)), from (1, −1), until ‖G(x) − x‖ < 1e-12. The secant takes fewer
    steps (measured: 4, `plain secant secant secant`, against 74 plain), and both reach the same
    point to 10 places.
- **(c), (d), (e) `TestAConvergedHistory`.** `setUpClass` tunes the tests' point once through
  `golden_tuned_history.run_tuning`, with stdout captured: five integrations.
  - `test_c_both_stored_residuals_are_inside_the_tolerance`: each |residual| < 1e-5, and
    `tuning_converged` agrees;
  - `test_c_A_of_phi_ref_is_one_exactly_and_V_ref_is_the_realizations`:
    `model.realize(normalization, phi_ref).log_Omega(phi_ref) == 0.0`, and `V_ref` equals
    `exp(rm.log_V(phi_ref))`;
  - `test_c_the_residuals_recomputed_from_the_last_sample_are_the_stored_ones`: ruling 5's
    expressions, written out in the test, from the payload's last sample give `residual_H` and
    `residual_phi` bitwise (`float.hex`). `phi_0`, `pi_0` and `tracking` are the sample's;
  - `test_c_the_seed_is_the_models`: `seed_*` equal `model.seed(cosmology)`'s;
  - `test_c_the_outputs_and_the_step_counts`: every T7 key is a `float`, `int` or `str`, and
    `tuning_iterations = 1 + plain + secant + guard`;
  - `test_c_one_progress_line_per_integration_that_the_tracker_ignores`: there are
    `tuning_iterations` lines beginning `-- tune `. Each matches ruling 9's format (a regular
    expression), with `it` = 0, 1, …, the label, and `seed` only at 0. A `StageTracker` fed each
    line returns False and keeps `stage` None. The last line prints the stored residuals,
    normalization and φ_ref in ruling 9's formats;
  - `test_d_a_second_tuning_is_bitwise_the_same`: a second tuning (five more integrations) equals
    the first in `golden_history.record_of` and in every T7 key, with the same type and, for
    floats, the same `float.hex`;
  - `test_e_the_tuned_golden_history`: the record of (c)'s output equals
    `golden_history_06_tuned.json`, by `golden_tuned_history.differences`
    (`first_difference` for the history, each tuning key exactly), and the JSON holds exactly
    `TUNING_OUTPUT_KEYS`. On a mismatch it prints both environments and the regeneration command.
- **(f)–(j) `TestTheFailures`:**
  - `test_f_the_cap`: `max_integrations=2` fails with the first clause "the normalization did not
    converge" (read by `summarise_failure_reasons`), the reason says "after 2 integrations", and
    two progress lines were printed;
  - `test_g_no_normalization_reaches_H0`: `_NoMinimumModel` (its `effective_minimum` returns
    φ₀ − M_P/β) fails with "no normalization reaches H0", "the last at integration 4", after five
    lines of kinds `seed plain guard guard guard`;
  - `test_h_the_seed_is_each_models_own`: β = 3 at M = 0.01 and then at M = 0.1, each with
    `max_integrations=1`. Each prints one line, `it=0 kind=seed`, whose `norm` and `phi_ref` are
    that model's `seed(cosmology)` in `%.12g` and `%.10e` of φ_ref/M_P;
  - `test_i_the_effective_minimum_is_not_found`: `_RaisingMinimumModel` fails with "the effective
    minimum was not found", the `ValueError`'s text and "(tuning integration 0)", with no
    progress line;
  - `test_j_an_integrations_own_failure`: `SM.StepControl` patched to a step budget of 50, as
    `test_scalarhistory_failure_reason.py` does. The reason starts "step budget exhausted", has
    that first clause, ends "(tuning integration 0)" (or is `DEFAULT_STRING_LENGTH` long), is at
    most `DEFAULT_STRING_LENGTH`, and no progress line was printed.

The reasons, as measured (scratch `impl06/check/reasons.log`):
- (f): `the normalization did not converge: after 2 integrations, H/H0-1=+5.356e-03, phi_ref/phi_min-1=-5.453e-02`;
- (g): `no normalization reaches H0: V_target <= 0 at 4 integrations running, the last at integration 4`;
- (i): `the effective minimum was not found: a test model's effective minimum has no bracket (tuning integration 0)`;
- (j): `step budget exhausted: integrate_scalar_history (golden_tuned_history) took 51 accepted steps (budget 50) at N=4.218451972, T_J=303.41 GeV, with 0 reflection(s) (tuning integration 0)`.

(g)'s five lines have H/H0 − 1 = −4.058e-2, +0.4383, +0.5579, +0.5518, +0.5422, and φ_ref goes
negative after the first step (−0.276 M_P), as the subclass makes it.

**Added: `ComputeTargets/tests/golden_tuned_history.py`** (ruling 12; its name does not match
`test*.py`). It takes `record_of`, `first_difference`, `environment`, `_hex` and
`EXCLUDED_METADATA` from `golden_history.py`.
- The setup: β = 3, M = 0.01 M_P, n = 1, φ* = 5 M_P, π* = 0, T* = 2 × 10⁴ GeV, atol = rtol =
  1e-8, the default tolerance and cap, and `log_one_plus_z_grid(250, 300)`.
- `make_cosmology`, `make_model(units, beta, M_over_Mp, cls)`, `make_z_grid`,
  `run_tuning(cosmology, model, z_grid, **kwargs)`, which the tests share;
  `tuning_record`, `build_record`, `differences`, and `main`.
- The JSON holds `provenance` (the commit, the code tree "jordan-normalization prompt 06's working
  tree, before its commit, on top of generated_at_commit", the generator, the regeneration
  command, the environment, `generated_at`, `generation_seconds`, the setup and what is not
  pinned), `history` (`record_of` of the final payload), and `tuning` (the T7 keys, floats as
  `float.hex`).

**Added: `ComputeTargets/tests/data/golden_history_06_tuned.json`.** Generated at
`8db352b` plus this prompt's working tree (29.3 s, 5 integrations, load about 9). It records:
- `tuning_iterations` 5, from a `tracking` seed, with `plain_steps` 1, `secant_steps` 3 and
  `guard_steps` 0;
- `normalization` `-0x1.1573404f5900fp+8` (−277.4502000419897) and `phi_ref`
  `0x1.fdc67ec7575e5p-5` (0.06222843896351175 M_P);
- `residual_H` −7.034e-7 and `residual_phi` +4.692e-6;
- `n_samples` 5960 and `accepted_steps` 15331.

The normalization and φ_ref equal the prototype's `CONVERGED` line at this point, by `repr`.
`golden_history_01/03/05.json` are byte-identical (`git status` shows them unchanged).

**Changed: `ComputeTargets/tests/test_integrator_exceptions.py`** (ruling 3; README §5's "tests
are modified only where a prompt changes what they test"). `TestFallbackGone.test_g_no_fallback`
and `test_g_one_runtime_error_in_the_integration_path` read the body of `integrate_history` in
place of `compute_scalar_history`, because the body moved. The comment above the first lookup
says so. Without the change the second fails (`compute_scalar_history` now has no `RuntimeError`)
and the first passes vacuously.

**Not changed:** `golden_history.py`, `test_golden_history.py` and every golden JSON (ruling 2).

## Deviations from the prompt

Every ruling cited is in orchestrator note 06, §0, unless it says otherwise.

1. **`IMPLEMENTATION CHOICE` (ruling 1): `integrate_history`,** a plain function with
   `compute_scalar_history`'s signature, its body moved unchanged but for its own name. Besides the
   printed lines and the `RuntimeError` that ruling 1 names, I renamed the assertion's message
   (`"integrate_history ({task_label}): solution does not have … components"`), because it names
   the function it is in. The commented-out `print` of the initial data still says
   `compute_scalar_history`. It is a comment, and it is moved unchanged.
2. **`STRUCTURALLY REQUIRED` (ruling 2): `compute_scalar_history` stays the `@ray.remote`,** a
   thin wrapper. The prompt's "`compute_scalar_history` becomes a thin wrapper over
   `tune_history` in 07" stays 07's. The golden history passes unchanged through it, and nothing
   regenerates `golden_history_05.json`.
3. **`STRUCTURALLY REQUIRED` (ruling 3): `TestFallbackGone`** reads `integrate_history`.
4. **`IMPLEMENTATION CHOICE` (ruling 4): `tune_history`'s signature,** the constants in
   `ScalarHistory.py`, T_stop from the cosmology, and `secant=False` for the comparison and
   diagnostics only.
5. **`STRUCTURALLY REQUIRED` (ruling 5): the residuals are `residual_H = H_J/H0 − 1` and
   `residual_phi = φ_ref/φ_min − 1`,** not the prompt's r_H = ln(H_J/H0) and r_φ = φ_ref − φ_min.
   They are what the line prints, what the test reads and what T7 returns.
6. **`STRUCTURALLY REQUIRED` (ruling 6): the step is the object `TuningSecant`,** holding only the
   Broyden state, with the prototype's expressions. The prompt's "initial inverse Jacobian chosen
   so that the first step is the plain step" is B = −I with the plain step returning G(x) itself.
   Its read-only `B` property, a copy, is mine (Deviation 13).
7. **`IMPLEMENTATION CHOICE` (ruling 7): the loop's order, and a fourth kind, `guard`.** The
   prompt's line has kinds `seed|plain|secant`. The cap is tested as `k + 1 >= max_integrations`,
   where ruling 7 says "equals": the same for every cap of 1 or more, and it cannot loop for ever on
   a cap of 0. `TUNING_GUARD_LIMIT = 4` names ruling 8's "fourth in a row".
8. **Ruling 8, the failures.**
   - **`STRUCTURALLY REQUIRED`:** a fourth first clause, "the effective minimum was not found", for
     `effective_minimum`'s `ValueError`, which the prompt does not name.
   - **`IMPLEMENTATION CHOICE`:** the suffix ` (tuning integration k)` on an integration's own
     reason (truncation may cut it, which ruling 8 accepts).
   - **`IMPLEMENTATION CHOICE`, mine:** a failed tuning prints one line,
     `!! tune_history (<label>): <reason>`, before it returns, as `integrate_history` prints
     `!! … marked as total integration failure`. No ruling names it. No pattern of `StageTracker`
     matches `!! `.
9. **`STRUCTURALLY REQUIRED` (ruling 9): the progress line begins `-- tune`,** not the prompt's
   `>> tune`, which `_STAGE_BANNER` would take for a stage banner. The tracker's handling is in
   "What shipped".
10. **`IMPLEMENTATION CHOICE` (ruling 10): the returned keys,** with the step counts by kind as
    three keys (`plain_steps`, `secant_steps`, `guard_steps`). `TUNING_OUTPUT_KEYS` lists them for
    the tests and for 07.
11. **`STRUCTURALLY REQUIRED` (ruling 11): the tests,** in one module `test_tuning.py` (the prompt's
    `test_tuning*.py`), (a)–(j) as ruled. The prompt's test 3, "a model whose `renormalize` cannot
    reach H0", is (g)'s subclass of `effective_minimum`, as ruled.
12. **`STRUCTURALLY REQUIRED` (ruling 12): the tuned golden history,** its generator beside
    `golden_history.py`, and its JSON. The generator also imports `EXCLUDED_METADATA` for the
    provenance's "not pinned" list, and holds the setup's helpers that the tests share
    (`make_cosmology`, `make_model`, `make_z_grid`, `run_tuning`, `differences`). The provenance
    records `generated_at` as the time.
13. **`IMPLEMENTATION CHOICE`, mine: the structure inside `ScalarHistory.py`.** `_tuning_state`
    and its `TuningState` namedtuple hold ruling 5's expressions. `_tuning_failure` prints and
    builds the payload. `TuningSecant.B` is a copy for the tests. None changes a value: the
    comparison's tables equal note 06's §0 to the digit, and two points (β = 3 at M = 0.01 and
    M = 0.5, the guard included) were checked line by line against the prototype run beside
    it, with equal final `repr`s.
14. **`STRUCTURALLY REQUIRED` (ruling 13): the comparison,** run detached in eight lanes from my
    scratch, and recorded in `logs/06-probes/`.
15. **`STRUCTURALLY REQUIRED` (ruling 14): the breakages** are ruling 14's three. The prompt's "show
    which point then fails or slows" for the guard's reset is replaced by test (b): no point of the
    grid fires the guard after a secant step (below).
16. **`IMPLEMENTATION CHOICE` (ruling 16): the records** are the board and this log.
    `.documents/OPEN_ISSUES.md`, `prompts/INDEX.md`, the README, `CLAUDE.md` and `.documents/` are
    not edited.

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the start of each run. Outputs are in the scratch
directory `impl06/` (`suites-before/`, `suites-after/`, `compare-logs/`, `check/`, `br/`). Every
suite was run serially in the foreground of one script, with `CLAUDE.md`'s commands, and never
while the comparison ran.

| Suite | Before (`8db352b`) | After (`1b83bbd`'s tree) | Change |
|---|---|---|---|
| CosmologyModels | 26 OK, 69 s, load 7.5 | 26 OK, 63 s, load 4.5 | none |
| ComputeTargets `test_[a-f]*` | 61 OK, 100 s, load 8.6 | 61 OK, 95 s, load 4.7 | none |
| ComputeTargets `test_[g-z]*` | 144 OK, 126 s, load 7.9 | **165 OK**, 211 s, load 6.4 | **+21**: `test_tuning` |
| ObjectFactories | 86 OK, 8 s, load 5.6 | 86 OK, 7 s, load 6.3 | none |
| tools | 102 OK, 33 s, load 5.7 | 102 OK, 32 s, load 7.0 | none |
| RunDrivers | 61 OK, 23 s, load 5.9 | 61 OK, 23 s, load 6.3 | none |

- **Counts:** 26 / 205 (61 + 144) / 86 / 102 / 61 before, equal to the brief's baselines, and
  26 / 226 (61 + 165) / 86 / 102 / 61 after. No count fell. The one change is
  `test_tuning.py`'s 21 tests.
- **No test failed** in either run, `test_prym_passenger`'s wall-clock test included.

### The golden history

- **Before any edit:** `test_golden_history` alone, 3 tests OK in 9.354 s (10.6 s wall), load 6.4.
- **After T1 and rulings 2–3,** before anything else: `test_golden_history` and
  `TestFallbackGone` together, 5 tests OK in 9.2 s. It also passed in the same run a moment
  before. **The golden history is bitwise through `integrate_history`.**
- **After** (at the end of the after-suites): `test_golden_history` alone, 3 tests OK in
  9.226 s (10.6 s wall), load 8.2. `golden_history_01/03/05.json` are unchanged.

### `tune_history` against the prototype

Before the comparison, `tune.py` (the orchestrator's prototype) and `tune_history` were run side
by side at β = 3, M = 0.01 (5 integrations) and at β = 3, M = 0.5 (12, with a guard step at
integration 0), with the secant. In every progress line the iteration, kind, both residuals
(`%+.3e`), `norm` (`%.12g`) and `phi_ref` (`%.10e`) are equal. The final normalization and φ_ref
are equal by `repr`: −277.4502000419897 and 0.06222843896351175; −280.1287443104298 and
0.2708473342370741.

### The comparison (§2 item 2; ruling 13)

`run_all.sh` ran the 26 tunings in eight lanes from 09:16:15 to 09:28:29, at a load of 5–16 (the
machine was otherwise quiet). Every tuning converged (exit 0). The longest were β = 25 (732 s plain
and 648 s secant).

**The table** (§2 item 2), from `table.py`:

| β | M/M_P | φ* | seed | plain: integrations (steps) | secant: integrations (steps) | Λ plain (meV) | Λ secant (meV) | φ_ref/M_P plain | φ_ref/M_P secant |
|---|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.125321 | 2.125322 | 3.4962045e-02 | 3.4962266e-02 |
| 25 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 1.842954 | 1.842955 | 6.8089327e-03 | 6.8089580e-03 |
| 5.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 2.020615 | 2.020615 | 1.6719234e-02 | 1.6719361e-02 |
| 3 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.073998 | 2.073996 | 2.3662575e-02 | 2.3662904e-02 |
| 1.1 | 1e-3 | 2 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.144994 | 2.144996 | 4.1533670e-02 | 4.1533944e-02 |
| 0.5 | 1e-3 | 1 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.184947 | 2.184949 | 6.3821123e-02 | 6.3821650e-02 |
| 3 | 1e-5 | 5 | tracking | 3 (plain 2) | 3 (plain 1, secant 1) | 2.226473 | 2.226473 | 2.6704632e-03 | 2.6704769e-03 |
| 1 | 1e-5 | 5 | tracking | 5 (plain 4) | 4 (plain 1, secant 2) | 2.236588 | 2.236588 | 4.6642085e-03 | 4.6642162e-03 |
| 3 | 0.01 | 5 | tracking | 7 (plain 6) | 5 (plain 1, secant 3) | 1.831404 | 1.831403 | 6.2228103e-02 | 6.2228439e-02 |
| 1 | 0.03 | 5 | tracking | 8 (plain 7) | 5 (plain 1, secant 3) | 1.881162 | 1.881157 | 2.0284503e-01 | 2.0284635e-01 |
| 3 | 0.1 | 5 | static | 13 (plain 12) | 9 (plain 1, secant 7) | 1.388919 | 1.388919 | 1.4772415e-01 | 1.4772170e-01 |
| 1 | 0.5 | 5 | static | 21 (plain 20) | 9 (plain 1, secant 7) | 1.332252 | 1.332252 | 6.2763553e-01 | 6.2762591e-01 |
| 3 | 0.5 | 5 | static | 23 (guard 1, plain 21) | 12 (guard 1, plain 1, secant 9) | 0.937490 | 0.937486 | 2.7085278e-01 | 2.7084733e-01 |

Each run begins with the seed's integration, which the step counts leave out. Λ is
`exp(normalization / 4) / units.eV * 1e3`, and φ_ref is `phi_ref / units.PlanckMass`, both from the
converged iterate.

| β | M/M_P | φ* | \|Δ ln Λ\| | \|Δφ_ref\|/φ_ref | plain's last \|φ_ref/φ_min − 1\| | Λ_secant/Λ_planning − 1 | φ_ref,secant/φ_ref,planning − 1 | wall plain, secant (s) |
|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | 3.2e-07 | 6.3e-06 | 2.9e-06 | -2.781e-04 | -3.8e-06 | 66, 43 |
| 25 | 1e-3 | 5 | 3.4e-07 | 3.7e-06 | 7.0e-07 | -2.767e-04 | -6.2e-06 | 732, 648 |
| 5.5 | 1e-3 | 5 | 1.4e-07 | 7.6e-06 | 6.1e-06 | -2.779e-04 | -8.3e-06 | 217, 173 |
| 3 | 1e-3 | 5 | 1.1e-06 | 1.4e-05 | 4.6e-06 | -2.795e-04 | +1.5e-07 | 77, 57 |
| 1.1 | 1e-3 | 2 | 7.8e-07 | 6.6e-06 | 2.7e-06 | -2.774e-04 | +1.0e-06 | 60, 40 |
| 0.5 | 1e-3 | 1 | 1.0e-06 | 8.3e-06 | 1.5e-06 | -2.768e-04 | +7.1e-06 | 36, 24 |
| 3 | 1e-5 | 5 | 2.9e-08 | 5.1e-06 | 1.2e-06 | -2.773e-04 | -8.6e-06 | 213, 214 |
| 1 | 1e-5 | 5 | 3.3e-08 | 1.6e-06 | 1.6e-06 | -2.776e-04 | +3.5e-06 | 43, 35 |
| 3 | 0.01 | 5 | 5.7e-07 | 5.4e-06 | 1.3e-07 | -2.783e-04 | +3.8e-06 | 64, 46 |
| 1 | 0.03 | 5 | 2.2e-06 | 6.5e-06 | 1.1e-06 | -2.894e-04 | +2.7e-06 | 20, 14 |
| 3 | 0.1 | 5 | 1.2e-07 | 1.7e-05 | 9.7e-06 | -2.780e-04 | +1.6e-05 | 77, 56 |
| 1 | 0.5 | 5 | 2.9e-07 | 1.5e-05 | 7.1e-06 | -2.772e-04 | +1.6e-05 | 34, 14 |
| 3 | 0.5 | 5 | 4.2e-06 | 2.0e-05 | 8.2e-06 | -2.647e-04 | +1.2e-05 | 69, 32 |

The wall times are this machine's at load 5–16, not note 06's (load 60–160).

- **It equals note 06's §0.** Compared by script, cell by cell, after trimming: the first table
  equals the note's in every cell. The second equals it in every cell but the wall times. Every
  integration count, step count and digit shown agrees.
- **The secant is never worse.** It needs fewer integrations at twelve points, and as many at one
  (β = 3, M = 10⁻⁵, 3 each). In all, 73 integrations against 116.
- **Agreement.** |Δ ln Λ| ≤ 4.2e-6 everywhere. |Δφ_ref|/φ_ref is below 1e-5 at nine points. It is
  1.4e-5 to 2.0e-5 at four: β = 3 at M = 10⁻³, 0.1 and 0.5, and β = 1 at M = 0.5. At each of those,
  the plain update's last |φ_ref/φ_min − 1| is 4.6e-6 to 9.7e-6, just inside the tolerance, and
  the plain update contracts slowly there. That is ruling 13's "slack": the plain update stops on
  its step, and its iterate lies further from the fixed point than its step. **No difference
  exceeds 1e-4**, so there is no stop (two fixed points).
- **Against the planning probes** (README §0.3 item 1, on the units before 05, from the plain
  start). Λ_secant/Λ_planning − 1 is −2.767e-4 to −2.795e-4 at eleven points, against 05's
  −2.78e-4. It is −2.894e-4 at β = 1, M = 0.03 and −2.647e-4 at β = 3, M = 0.5. φ_ref, which is in
  Planck units and does not move with the electronvolt, agrees to 1.6e-5 or better. The planning
  values are README §0.3's table (Λ to 1e-6 meV, φ_ref to seven digits), transcribed in
  `table.py`.
- **The guard** fires only at β = 3, M = 0.5 (static seed), at integration 0, in both methods: one
  `guard` step, at H/H0 − 1 = +1.079 from the seed. **The twice rule never fires.** Every secant run
  is `seed`, (`guard`,) `plain`, then `secant` to the end (`grep kind=` of each log).

### The test times

- **`test_golden_history` alone:** 9.354 s before (load 6.4); 9.2 s after T1, with
  `TestFallbackGone` (load 5.5); 9.226 s after everything (load 8.2).
- **`test_tuning` alone:** 21 tests OK in **87.482 s** (89.5 s wall), at load 4.7, after the
  comparison had finished. Its three breakage runs took 87.5–89.8 s at load 8–21. Its tunings are
  `setUpClass` (5 integrations), (d) (5), (f) (2), (g) (5), (h) (2) and (i) (1), each integration
  about 4–6 s at this load.

### `black`

`black --check` passes on the six Python files `1b83bbd` adds or changes:
- `ComputeTargets/ScalarHistory.py`;
- `ComputeTargets/tests/test_integrator_exceptions.py`;
- `ComputeTargets/tests/test_tuning.py`;
- `ComputeTargets/tests/golden_tuned_history.py`;
- `logs/06-probes/`'s `compare.py` and `table.py`.

`ScalarHistory.py` was black-clean before.

### What did not run

No Ray was started, no store was opened, and `main.py` was neither changed nor run.

## The deliberate-breakage record

- **How each diff was made:** `diff -u` of a scratch copy of `ComputeTargets/ScalarHistory.py`
  against the file, with the labels `a/…` and `b/…`.
- **How each was replayed,** in `bash` (`impl06/br/replay.sh`): `git apply --check`, `git apply`,
  `git apply -R --check`, then the whole `test_tuning` module (21 tests), then `git apply -R`.
  Afterwards `git diff --stat` was as before.

**1. `tuning_converged` on `residual_H` alone** (ruling 14.1). 2 failures out of 21, both in (a):
- `test_a_each_outside_alone`: `tuning_converged(0.0, 2.0e-5, 1.0e-5)` is True
  (`AssertionError: True is not false`);
- `test_a_the_tolerance_itself_is_outside`: `tuning_converged(0.0, -1.0e-5, 1.0e-5)` is True.

`test_a_both_inside` passes. So do (c)–(j): at the tests' point both residuals fall inside the
tolerance at the same integration (note 06's §0), so the converged history is unchanged.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1465,7 +1465,7 @@
     residual_H = H_J/H0 - 1 and residual_phi = phi_ref/phi_min - 1 at T_J = T_CMB, are inside
     the tolerance. The tolerance itself is outside.
     """
-    return abs(residual_H) < tolerance and abs(residual_phi) < tolerance
+    return abs(residual_H) < tolerance
 
 
 def _solve2(B, F):
```

**2. The seed read from a module-level cache** (ruling 14.2). 1 failure out of 21:
- `test_h_the_seed_is_each_models_own`: the M = 0.1 model's `it=0` line prints the first model's
  seed, `'-277.581681683' != '-276.998824803'` (the line's `norm` against M = 0.1's own seed).

Every other test passes, since every other tuning in the module is at (β = 3, M = 0.01), whose
seed the cache holds (`setUpClass` tunes it first).

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1609,6 +1609,9 @@
     """A failed tuning: the line, and _failure_payload's payload."""
     print(f"!! tune_history ({task_label}): {reason}", flush=True)
     return _failure_payload(reason)
+
+
+_SEED_CACHE = None
 
 
 def tune_history(
@@ -1666,7 +1669,10 @@
     Mp: float = units.PlanckMass
     T_stop: float = cosmology.T_CMB_Kelvin * units.Kelvin
 
-    seed: Seed = model.seed(cosmology)
+    global _SEED_CACHE
+    if _SEED_CACHE is None:
+        _SEED_CACHE = model.seed(cosmology)
+    seed: Seed = _SEED_CACHE
     norm, phi_ref = seed.normalization, seed.phi_ref
 
     stepper = TuningSecant(secant=secant)
```

**3. `guard()` that does not reset B** (ruling 14.3). 1 failure out of 21:
- `test_b_after_guard_the_next_step_is_plain_and_B_is_minus_identity`: after `guard()`, B is still
  the Broyden matrix, `[[-0.9, 0.09999999999999998], [0.020000000000000018, -0.98]] is not None`.

(g) passes: its guard steps follow one another, so the stepper never steps after a guard. The
grid's points fire the guard only at integration 0, before B is set (note 06's §0). So (b) is the
only test that sees the reset.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1511,7 +1511,6 @@
 
     def guard(self):
         """Forget B and the last point: the next step is plain."""
-        self._B = None
         self._x_prev = None
         self._F_prev = None
         self._last_kind = None
```

Each diff applies to `1b83bbd`'s tree: `git apply --check` passes for each, and after applying
each, `git apply -R --check` passes and the reverse restores the tree. Checked in `bash`.

## Observations not acted on

- **Ray deduplicates worker lines** (note 06's §0, not re-measured here). Ray's log deduplicator
  drops every word that contains a digit before it compares lines. So tuning lines of one `kind`
  from different workers within its 5 s window are collapsed into one, with "[repeated N× across
  cluster]". The first is printed at once, so the heartbeat still beats, but the run log loses
  lines. 09 sees it under Ray.
- **The static seed starts far from H0.** At integration 0, H/H0 − 1 is +0.681 (β = 3, M = 0.1),
  +0.679 (β = 1, M = 0.5) and +1.079 (β = 3, M = 0.5). The tracking seed starts between −7.4e-2
  (β = 1, M = 0.03) and −1.7e-4 (β = 3, M = 10⁻⁵). So the static points take 9–12 integrations
  even with the secant (the comparison's logs).
- **The plain update's slack at its tolerance** (above, ruling 13). At M ≥ 0.1 it contracts slowly,
  and its converged φ_ref is up to 2.0e-5 from the secant's, with its last step inside 1e-5.

## State handed to the next prompt

- **Tree:** `1b83bbd`, on `jordan-normalization`, on top of `8db352b`.
- **The two layers** (`ComputeTargets/ScalarHistory.py`):
  - `integrate_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model: RealizedModel, task_label=…, atol=…, rtol=…, verbose=False)`
    is the primitive: today's integration of one realized model to any T_stop, returning today's
    payload or `_failure_payload`'s.
  - `compute_scalar_history` is still the `@ray.remote`, with an unchanged signature, and calls
    `integrate_history` by name. `ScalarHistory.compute` and `main.py` still compute untuned
    histories through it. **07 points it at `tune_history`.**
  - `tune_history(cosmology, model: AbstractModel, T_init, phi_init, pi_init, z_grid, task_label=…, atol=…, rtol=…, tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS, secant=True, verbose=False)`
    integrates to `cosmology.T_CMB_Kelvin * units.Kelvin` from `model.seed(cosmology)`, and
    returns the final payload plus `TUNING_OUTPUT_KEYS`: `normalization`, `V_ref`, `phi_ref`,
    `phi_0`, `pi_0`, `tracking`, `residual_H`, `residual_phi`, `tuning_iterations`,
    `plain_steps`, `secant_steps`, `guard_steps`, `seed_kind`, `seed_normalization`,
    `seed_phi_ref`. Each is a Python `float`, `int` or `str`. **07 stores exactly these.**
  - `DEFAULT_TUNING_TOLERANCE = 1.0e-5` and `DEFAULT_TUNING_MAX_INTEGRATIONS = 30` are module
    constants. **08 gives them options.** `secant=False` is for diagnostics, and no driver passes
    it.
  - Failures have four first clauses: "the normalization did not converge", "no normalization
    reaches H0", "the effective minimum was not found", and an integration's own reason with
    " (tuning integration k)" appended. `summarise_failure_reasons` groups them unchanged.
  - The progress line is `-- tune <label> it=<k> kind=<seed|plain|secant|guard> H/H0-1=… phi_ref/phi_min-1=… norm=… phi_ref=… wall=…`.
    `StageTracker` ignores it; forwarded by Ray it beats. Ray's deduplicator may collapse lines
    of one kind from several workers (Observations). **09 measures the silences.**
- **The golden histories.** `golden_history_05.json` passes unchanged through `integrate_history`
  (`golden_history.py` still calls `compute_scalar_history._function`).
  `golden_history_06_tuned.json` pins the tuned history at β = 3, M = 0.01 M_P, φ* = 5 M_P (five
  integrations; `golden_tuned_history.py`, test (e)). **07 and 08 pass it bitwise** (P3).
- **The comparison** is in `logs/06-probes/`. Over the thirteen points the secant takes 73
  integrations against the plain update's 116: 3–12 per history from the model's seeds, the
  seed's included.
- **Counts:** CosmologyModels 26, ComputeTargets 226 (61 + 165), ObjectFactories 86, tools 102,
  RunDrivers 61.
- **Open issues.** None closed or opened. `.documents/OPEN_ISSUES.md` is unchanged.
- **Breakage replay.** The three diffs above apply to `1b83bbd`'s tree, checked in `bash`.
- **The flaky wall-clock test** `test_prym_passenger.test_a_oscillating_case_completes` passed in
  both of this prompt's suite runs.
