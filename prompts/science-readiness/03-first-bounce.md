# Prompt 03 — Store the first bounce

**Campaign:** [`README.md`](README.md) · **Board item:** **T** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and T.
**Closes:** `[00-first-bounce-is-not-stored]` on this board.
**Recommended model:** **Opus.** The function is short. The judgement is in where a bounce is,
across a reflection restart and at the edges of the solution, and in proving it is the measure
the user ruled for.

**Read first:**

1. [`README.md`](README.md) §0.2 (U4, P5), §0.5, §2 (g), §5, §6.4.
2. `prompts/integrator-remediation/IMPLEMENTATION_STATE.md`, Decisions: the user's rulings on
   `φ_min` and the first bounce (the dense-output turning point).
3. `logs/02-scalarmodel-failure-reasons.md`, "State handed to the next prompt".
4. `ComputeTargets/ScalarModel.py`: `IntegrationResult`, `Reflection`, `integrate_scalar_history`
   (how `ts`, `interpolants` and `reflections` are built, and what happens at a reflection
   restart), and `compute_scalar_model` after the loop.
5. `ComputeTargets/tests/test_kinematic_cap_loop.py`: `P1`, `P2`, `build`, `integrate`,
   `turning_points`, `wall_bounces`, `interpolated_minima`.
6. `.documents/review-remediation-verification.md` §4.8 point 3 and §4.9 point 1: the first
   bounces to reproduce.
7. `Datastore/SQL/ObjectFactories/ScalarModel.py`, and `tools/history_and_bbn.py`.

---

## 1. The changes

- **`FirstBounce`** (a namedtuple: `N`, `phi_Einstein`, `log_T_Jordan`, `reflected`) and
  **`first_bounce(result: IntegrationResult) -> Optional[FirstBounce]`** in
  `ComputeTargets/ScalarModel.py`, as README §2 (g).
  - Use the steps' own interpolants, `result.solution.interpolants` and `.ts`.
  - A reflection that comes earlier than the first turning point is the answer, with
    `reflected=True` and the state at the reflection.
  - Do not use the `φ < 1.5 M` filter (P5), unless the board records that P5 was overruled.
- **`compute_scalar_model`** calls it after the loop and returns `"first_bounce"`: the tuple, or
  `None`.
- **`ScalarModel`** stores it through `store()` and `__init__`. A `first_bounce` property returns
  the tuple or `None`, and raises on a failure row as `metadata` does.
- **The factory:** four nullable columns, `first_bounce_N`, `first_bounce_log_T_Jordan`,
  `first_bounce_phi_Einstein` (`Float(64)`) and `first_bounce_reflected` (`Boolean`). They are
  written and read. All four are NULL when there is no bounce, and on a failure row.
- **The driver** prints the first bounce: `N`, `T_J` in MeV, `φ`, and whether it was reflected.

## 2. Tests

- **(a) The P1 window** (β = 2, `M = 0.5`, to `N = 21`). `first_bounce` gives `N = 20.343028 ±
  1e-5`, `φ = 4.57371e-3 ± 1e-4` relative, and `reflected = False`. It equals
  `interpolated_minima(result, 0.5)[0]` to `1e-12`. Under a second.
- **(b) A reflection.** P1 at `M = 1e-10` gives `reflected = True`, with `φ ∈ [1e-11, 1e-10]`
  and `N` equal to `result.reflections[0].N`.
- **(c) No bounce.** P2 from `N₀` to `N₀ + 0.01` gives `None`.
- **(d) Agreement with the filtered helper.** On the P1 windows at `M = 0.5, 0.01, 0.001` and the
  P3 window, the first element of `wall_bounces` and `first_bounce` locate the same step. P3 is
  about two seconds; its docstring says so.
- **(e) The round trip** (`Datastore/tests/`). A temporary SQLite store. A row with a bounce reads
  back all four values, floats to the last bit. A row without one reads back `None`. A failure
  row raises on `first_bounce`.
- **On `HEAD~1`** nothing stores the bounce. The stand-in measurement is the source's: the first
  `π` sign change among the *stored samples* with `φ < 1.5 M`, on the driver's β = 2,
  `M = 10⁻³` history. It is recorded beside the dense-output value. The source reports 0.138 GeV
  against 0.747 GeV at small `M`.

## 3. What this prompt does not do

No change to the step loop, the sampling, `IntegrationResult`, the reflection, or `extra_data`.
The bounce does not go in `extra_data` (U4). No count of later bounces is stored.

## 4. Acceptance

1. README §6.4, every row.
2. **The driver** on β = 2 at `M = 0.5` and `10⁻³`, and β = 1.6 at `10⁻⁵`. The first bounce
   agrees with §4.8 to `1e-5` in `N` and to the printed digits in `T_J`. The β = 1.6 value is
   recorded.
3. RHS, accepted steps and wall bounces on those three histories are identical to log 01's
   driver output: no trajectory moved.
4. All three suites pass and rise by your methods. `black --check` clean.
5. The board and the index: T done, its issue closed.

## 5. Stop conditions — stop and ask the user

- (d) finds a history where the first negative-to-positive turning point is not the first
  `φ < 1.5 M` wall bounce. P5's premise would then be false.
- A reflection restart makes the interpolant before or after it unusable for locating a turning
  point.
- Acceptance 3 fails.

## 6. The log and the board

`logs/03-first-bounce.md`, in the README §5.1 template. In "State handed to the next prompt", give
`first_bounce`'s signature, the four column names, and the driver's output on the three
histories.
