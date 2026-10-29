# Log 05 — unresolved turnarounds

**Subject:** Reflect unresolved turnarounds at three classical turning points
**Commit:** this commit, on branch `inelastic-reflection`, on top of `cd19768`
**Date:** 2026-10-29, by Claude Opus 5.5
**Result:** **COMPLETE.** The gate "Unresolved" holds. One issue opened.
- **The rule ships, at α > 0 only.** At the start of every step with π < 0, on a model with a wall,
  `assess_infall` gives ln E = ln(ρ_kin,E + V − V_floor) and the model's classical turning point.
  If φ < K φ_t with φ_t < 10⁻⁸ M_P or ln 𝓡 > 0 (`unresolved_turnaround`; K = 3), the field is
  reflected there, through the floor branch, as a reflection of kind `"unresolved"`. Every
  reflection, the floor rule's included, drains with w_gate = 1.0 exactly (R10, P20).
- **The gate "Unresolved"** (README §8.4):
  - **α = 0 is bitwise.** Both golden histories, `test_first_bounce`, `test_surfing_turning_points`
    and `test_kinematic_cap_loop` pass unedited. The model is called 0 times on a golden history and
    once on the P1 harness at M = 10⁻¹⁰, for natural mode's warning; that run is `kcl.integrate`'s
    bitwise.
  - **An unresolved reflection is made at 3 φ_t with w_gate = 1** (M = 10⁻⁸ M_P, β = 2,
    φ/φ_t = 2.959), and the floor rule's reflection drains with w_gate = 1.
  - **The reflected history equals the resolved one in the probe's band.** At M = 10⁻⁹ the two
    differ by 7.5e-7 in Σ Δρ/ρ_R, with the same 46 candidates to the window's end. The band's
    largest difference where both runs are sound is 1.4e-6, at M = 10⁻¹⁰ (§5.4). At M = 3×10⁻¹¹ the
    *resolved* run is numerically wrong (§5.4, Observations).
  - **The warnings, once per history**, kept through a failure.
- **α > 0 at M = 0.5 M_P is bitwise 03's** (β = 1.2, 2, 5, 25): the turning points, every pre-05
  field of every `Injection`, the sample and the final state. Every candidate is resolved and
  assessed.
- **Measured down to M = 10⁻³⁰** (§5.5). Without 06's cut every history fails at the bottom of its
  cascade, **in every case after ρ_kin/ρ_R has fallen below 10⁻⁶**:
  - at M = 10⁻⁷–10⁻¹², on guard G2: the unresolved rule fires where E ≈ V − V_floor;
  - at M = 10⁻³⁰, in the model's classical_turning_point: "not monotone" where V − V_floor is a few
    ulps of ln V. Opened as `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`;
  - once (β = 1.2, M = 10⁻¹²), in `log_m_eff_squared` at a dense-output sample with φ < 0.
- **Counts:** 26 / 339 (96 + 243) / 103 / 115 / 64 before; 26 / **374** (96 + **278**) / 103 / 115 /
  64 after, all passing. The 35 new tests are in `ComputeTargets/tests/test_unresolved_turnarounds.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was regenerated.

## 1. The physics and the numerics

### 1.1 The energy that reaches the wall (prompt §0.2)

Across the crossing of the wall, ρ_kin + V is conserved: expansion is neglected, as 04's methods
neglect it. Far from the wall V = V_floor. So the energy that reaches the wall is
E = ρ_kin + (V(φ) − V_floor), the same at every point of the infall, and the turning point
V(φ_t) − V_floor = E is the same from every point.
- With ρ_kin alone, φ_t drifts outwards as the field climbs the wall. On the exponential wall at the
  M of this prompt, (V − V_floor)/E at an unresolved reflection before the cut is at most 3.7e-16
  (§5.5), so the two agree. On 04's power-law wall (p = 2) V − V_floor is E/9 at 3 φ_t.
- ln E is formed in logs: ln W = ln V + ln(−expm1(ln V_floor − ln V)) (04's `_log_V_minus_floor`
  expression), then ln E = max + log1p(exp(min − max)). Where V rounds to V_floor, ln E = ln ρ_kin,E
  exactly.
- With this E, `classical_turning_point` returns `None` on an infalling step only where ρ_kin is
  below the rounding of V − V_floor (`test_2_a_field_with_no_kinetic_energy_*`). It never happened
  on any history of §5: 0 of about 400 000 assessments.

### 1.2 The decision and the order at a step start

- **The decision** (R8): φ < K φ_t, and either φ_t < 10⁻⁸ M_P or ln 𝓡 > 0. All three comparisons
  are strict. `unresolved_turnaround` reads K and the threshold from the module at call time.
- **The order**, at α > 0 with π < 0, on a model with a wall (`reflects_at_origin` and a
  `log_V_floor`):
  1. the assessment: H_E from `policy` and `hubble` (never `fun`), ln ρ_kin,E = 2 ln H_E + 2 ln|π| −
     ln 2 (the expression `candidate` uses), then `assess_infall(model, φ, ln ρ_kin,E)`;
  2. the unresolved rule, if the turning point is not `None` (ruling 7);
  3. the floor rule, f φ/|π| < h_floor, as before.

  If either fires, the floor branch runs as it stands, with the kind `"unresolved"` or `"floor"`:
  the guards, `max_wall_to_kinetic_ratio`, the `Reflection`, `notify_reflection`, the drop of an
  armed drain, the close of the half-oscillation, the candidate, and the restart from
  π_out = −√(1 − α_eff) π_in.
- **Where the reflection lies.** The kinematic cap moves the field inwards by at most fφ per step
  (f = 0.1) when the step is velocity-capped and the field decelerates. The reflection then lies in
  [(1 − f) 3 φ_t,prev, 3 φ_t), with φ_t,prev the previous step start's. On item 3's history
  φ/φ_prev = 0.90000017 and φ_t,prev/φ_t = 1 − 2.35e-12 (§4.3). Over the 562 unresolved reflections
  before the cut in §5.4–§5.5 the smallest φ/φ_t is **2.70008**. After the cut, at the bottom of
  the cascades, E ≈ V − V_floor and φ_t jumps between step starts; there φ/φ_t falls to 1.011 (§5.5).
- **The weight** (R10, P20): `alpha_effective(A_k, resolved)` returns w_gate = 1.0 for an
  unresolved candidate without calling `gate_weight`, so α_eff = α · 1.0 = α exactly. 𝒜_k is still
  closed and recorded.

### 1.3 The records' assessment (prompt §1.4, ruling 7)

The loop keeps the infall's latest assessment (φ, ln E, `ClassicalTurningPoint`):
- it is set at each step start whose assessment is not `None`;
- a wall-side turning point that arms a drain captures it for that drain;
- every turning point and every reflection starts a new infall.

So a reflection records its own step start's assessment, or the infall's latest if its own was
`None` (never seen). A drain point records the last assessment of the infall that ended at its
wall-side turning point. Where a plateau and a wall-side turning point share one step, the wall's
infall had no step start and the drain point records `None`; that did not happen on the histories
measured (every candidate at M = 0.5 is assessed, §5.2).

### 1.4 Natural mode (P22, P23; ruling 8)

At α = 0 no assessment is made: `assess_wall` is false and the floor rule is the old code. The
warning needs φ_t at most once:
- at a wall-side `TurningPoint` the field is at rest, so φ is φ_t, and the test (φ < 10⁻⁸ M_P) costs
  nothing. It runs until a warning is made;
- at the first floor reflection of the integration, `natural_floor_warning` computes H_E and calls
  `assess_infall` once, whatever the outcome; there is no second call.
  - It catches `ComputationFailureError` from either, and nothing else. A failure becomes the
    "could not be assessed" line and the history goes on.
  - A `None` turning point would mean the field is at its turning point to rounding, and φ is taken
    for φ_t (D2).

The call reads the state and the policy, changes neither, and is made after the guards, which
already read the same state. The run is bitwise `kcl.integrate`'s (item 1; and under a raising model,
item 7).

### 1.5 The warnings (R6, P23; rulings 1, 2, 9)

Four kinds, each a fixed opening phrase in a module constant:

| Constant | Opening phrase | When |
|---|---|---|
| `WARNING_NEARLY_ELASTIC` | "dissipation mode: the reflections are expected to be nearly elastic in this regime" | α > 0, first candidate, its assessment has ln 𝓡 < 0 |
| `WARNING_REGIME_UNASSESSED` | "dissipation mode: the regime could not be assessed at the first impact" | α > 0, first candidate without an assessment |
| `WARNING_TURNING_POINT_BELOW` | "natural mode: the classical turning point lies below" | α = 0, a wall-side turning point or the first floor reflection's φ_t below 10⁻⁸ M_P |
| `WARNING_WALL_UNASSESSED` | "natural mode: the wall could not be assessed" | α = 0, the one model call failed |

The lines as they print, from the histories of §5:
- `!! tune_history (<label>): dissipation mode: the reflections are expected to be nearly elastic in
  this regime: the test-field ratio R = 10^-61.301 < 1 at the first impact (phi_t = 0.00496 M_P)`
- `!! tune_history (<label>): natural mode: the classical turning point lies below 1e-08 M_P
  (phi_t = 9.151e-13 M_P at the floor reflection at N = 20.35208972), where dissipation mode would
  reflect without resolving it; this history may fail on Radau's float spacing or the step budget`
- the wall-side variant says "at the wall-side turning point at N = …";
- `natural mode: the wall could not be assessed at the first floor reflection (N = …, phi = … M_P):
  <the exception's message>; the history goes on`.

None names an M.
- **The path.** The loop collects them, at most one per kind, and returns them in
  `IntegrationResult.warnings`. `integrate_history` passes them in its payload under `"warnings"` and
  prints nothing. `tune_history` prints the first line of each kind it sees, from the first
  integration that returns it, whatever `progress` is, with `flush=True`, and no later line of that
  kind (`warning_kind`; ruling 1). Each integration renormalizes, and so changes 𝓡 in the line.
- **The failure path** (ruling 2). The step loop runs inside `try`. Its handler sets
  `e.warnings = tuple(warnings)` on the `ComputationFailureError` and re-raises the same exception;
  `ComputeTargets/exceptions.py` is unchanged.
  - `integrate_history`'s failure payload carries them: `_failure_payload(reason, warnings=None)`
    adds `"warnings"` when given, and the sampling-overflow path passes `result.warnings`.
  - `tune_history` prints them before its `!!` failure line.
  - The tuning's own failures (`_tuning_failure`) pass none, so their payloads are unchanged.

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`

`git diff -w` shows the change best: the step loop is re-indented into a `try` (D5).
- **Constants** beside `NONADIABATICITY_SCALE`, each citing R8 and P18, neither a key:
  `UNRESOLVED_REFLECTION_FACTOR = 3.0`, `UNRESOLVED_PHI_T_THRESHOLD_MP = 1.0e-8`; and the warning
  phrases `WARNING_NEARLY_ELASTIC`, `WARNING_REGIME_UNASSESSED`, `WARNING_TURNING_POINT_BELOW`,
  `WARNING_WALL_UNASSESSED`, with `WARNING_KINDS`.
- **Module-level functions** (ruling 6), called by module name:
  - `assess_infall(model, phi_Einstein, log_rho_kin) -> Tuple[float, Optional[ClassicalTurningPoint]]`;
  - `unresolved_turnaround(phi_Einstein, turning_point, PlanckMass) -> bool`;
  - `warning_kind(line) -> str` (ValueError for a line of no kind).
- **The records**, fields appended last, comment blocks updated:
  - `Reflection(N, phi_Einstein, pi_Einstein_in, kind)`, `kind` defaulting to `None` (ruling 5);
  - `Injection(..., nonadiabaticity, w_gate, resolved, phi_assessed, log_E_assessed,
    classical_turning_point)`, `kind` now `"turning_point"`, `"floor"` or `"unresolved"`;
  - `IntegrationResult(..., dropped_drains, warnings)`.
- **`integrate_scalar_history`**: `assess_wall`, the infall's and the armed drain's assessments, the
  warnings list; `alpha_effective(A_k, resolved)`; `candidate(..., resolved, assessment)`, which also
  makes dissipation mode's warning at the first candidate (`first_candidate_warning`);
  `handle_turning_point` (the two turning-point loops' arming, now one helper, D4);
  `natural_floor_warning`; the assessment and the rule at the step start; the handler of ruling 2.
  The docstring describes the rule, the weight, the records and the warnings.
- **`_failure_payload(reason, warnings=None)`**; **`integrate_history`**'s payload gains
  `"warnings"`, on success and on both failure paths, and the `"reflections"` comment now says it
  counts both kinds; **`tune_history`** prints the warnings (rulings 1, 9), and its docstring says so.
- **Unchanged, byte for byte:** `compute_scalar_history`, `build_extra_data`, `ScalarHistory.store`,
  `first_bounce`, `TUNING_OUTPUT_KEYS`, `ODEPolicy`, `HubblePolicy`, `ODERHS`, `log_m_eff_squared`,
  `step_nonadiabaticity`, `gate_weight`, `_reflection_guards`. `REFLECTIONS_KEY` and its comment are
  untouched (Observations).

### The tests (`ComputeTargets/tests/test_unresolved_turnarounds.py`, new; 35 tests, about 30 s at a load of 5–20)

| Test | Item | Asserts | Tolerance |
|---|---|---|---|
| `test_2_K_is_3_and_the_threshold_is_1e_minus_8_M_P` | 2, ruling 3 | the constants equal the literals 3.0 and 1.0e-8 | exact |
| `test_2_phi_equal_to_K_phi_t_does_not_fire` | 2 | φ = 3φ_t: no; `nextafter(3φ_t, 0)`: yes; M_P = 1 and 2.435e18 | exact |
| `test_2_phi_t_equal_to_the_threshold_does_not_fire` | 2 | φ_t = 10⁻⁸ M_P: no; the next float below: yes | exact |
| `test_2_ln_R_equal_to_zero_does_not_fire` | 2 | ln 𝓡 = 0.0 and −0.0: no; 5e-324: yes | exact |
| `test_2_outside_K_phi_t_nothing_fires` | 2 | φ ≥ 3φ_t never fires, whatever φ_t and 𝓡 | exact |
| `test_2_the_rule_reads_its_constants_at_call_time` | 2 | patching K to 10 and the threshold to 0 changes the answer | exact |
| `test_2_ln_E_is_rho_kin_plus_the_wall` | 2 | power-law wall (ln V₀ = −120, M = 10⁻⁶, φ = M, W = V₀, ρ_kin = 8W): ln E = ln(ρ_kin + W) formed by the test, and ln 9W | 6ε(\|ln V\| + \|norm\| + \|ln E\| + 1) (log 04's R) |
| `test_2_phi_t_is_the_root_for_that_E` | 2 | φ_t = φ/3; V(φ_t) − V_floor = E | τ + R/S in ln φ; R + Sτ |
| `test_2_a_field_with_no_kinetic_energy_beyond_rounding_has_no_turning_point` | 2 | ρ_kin = e⁻⁶⁰ W: `None` | — |
| `test_1_no_model_call_and_no_warning_on_the_golden_histories` | 1, 7 | β = 1.5, 5.5: spy count 0, `"warnings"` == (), no reflection | exact |
| `test_1_one_model_call_on_the_floor_harness_and_the_run_is_todays` | 1 | P1 at α = 0: spy count 1; reflections (kind "floor"), nfev, steps, final state = `kcl.integrate`'s; no injection; ledger empty | bitwise |
| `test_7_natural_mode_warns_once_on_the_floor_harness` | 7 | one line, kind "below", no "M = " | — |
| `test_7_a_failing_model_call_is_a_warning_and_the_run_is_todays` | 7 | model raising: one call; run = `kcl.integrate`'s; one "wall could not be assessed" line with the reason | bitwise |
| `test_5_the_reflection_records_carry_their_kind` | ruling 5 | no real `Reflection` holds the default, α = 0 and 0.1 | — |
| `test_3_the_first_wall_approach_is_an_unresolved_reflection` | 3 | M = 10⁻⁸: `Reflection` and `Injection` of kind "unresolved", resolved False, before every turning point | exact |
| `test_3_w_gate_is_one_and_alpha_eff_is_alpha` | 3 | w_gate == 1.0, α_eff == 0.1, 𝒜_k finite | exact |
| `test_3_phi_lies_below_three_phi_t` | 3 | φ < 3φ_t and φ_t < 10⁻⁸ M_P with literals; the decision True | exact |
| `test_3_phi_lies_above_the_bound_the_step_before_allowed` | 3, ruling 4 | the previous step start: the decision False, φ ≥ 3φ_t; h ≤ fφ/\|π\|; π′ > 0; φ ≥ (1 − f)3φ_t(1 − δ)(1 − 8ε), \|δ\| < 1e-10 | §4.3 |
| `test_3_pi_out_and_the_dense_output` | 3 | π_out == −√(1 − α)π_in; `solution(N)[1]` == π_out | exact |
| `test_3_the_ledger_equals_the_records` | 3 | the ledger = the records with α_eff > 0 | exact |
| `test_3_H_Einstein_is_continuous_across_it` | 3 | `ti.H_bound` (02's) | 2.8e-14 (ratio measured 0) |
| `test_3_first_bounce_reports_the_reflection` | 3 | reflected, at the reflection's N | exact |
| `test_8_the_reflection_records_its_own_assessment` | 8 | φ_assessed; ln E and the turning point from `assess_infall(φ, log_rho_kin)`; ln E ≥ ln ρ_kin | exact |
| `test_7_dissipation_mode_warns_nearly_elastic_with_the_first_impacts_R` | 7 | one line, kind "nearly elastic", with the record's log₁₀ 𝓡 | — |
| `test_5_the_same_wall_approaches_and_candidates` | 5 | M = 10⁻⁹ to N = 30.3: the same counts; unresolved only in the rule's run | exact |
| `test_5_the_drained_energy_agrees` | 5 | Σ Δρ/ρ_R | 1e-5 rel (§5.4) |
| `test_6_every_candidate_resolved_with_an_assessment_and_no_reflection` | 6 | M = 0.5: no reflection; every candidate "turning_point", resolved, assessed, ln 𝓡 < 0, gated | exact |
| `test_6_bitwise_the_run_whose_decision_never_fires` | 6 | turning points, injections, final state, N_final, nfev, sample, ledger | bitwise |
| `test_7_one_nearly_elastic_warning_with_R_below_one` | 7 | one line, the first candidate's 𝓡 < 1 in it; payload = result | — |
| `test_8_a_drain_points_assessment_is_its_infalls_last` | 8 | each drain point's assessment = the one recomputed at its wall's step start (π < 0 there), with the ledger as it stood | exact |
| `test_7_each_kind_is_printed_once_from_the_first_integration_that_returns_it` | 7, ruling 1 | three integrations with three 𝓡 and two kinds: two lines, the first of each, then the failure line | exact |
| `test_7_no_warning_no_line` | 7 | no warning, only the failure line | — |
| `test_7_the_loop_raises_with_its_warnings` | ruling 2 | P1 at α = 0 with step budget 213 (reflection at step 210): "step budget exhausted", `e.warnings` one "below" line | — |
| `test_7_the_failure_payload_carries_them` | ruling 2 | through `integrate_history`: failure, `"warnings"` one "below" line | — |
| `test_7_tune_history_prints_them_before_its_failure_line` | ruling 2 | through `tune_history`: the warning line, then the "step budget exhausted" line | — |

Helpers in the module: `window` (a history to N_stop, left by the private `_LeaveHistory` before
it samples; ruling 10), `assessment_at`, `model_spy` (a `mock.patch.object` on
`AbstractModel.classical_turning_point`, `autospec`, wrapping the original), `failing_floor_harness`
and `_PlainSteps`. It imports `test_injection`, `test_kinematic_cap_loop`, `test_wall_model`
(`_PowerLawWall`), `golden_history` and `golden_tuned_history`, none edited (ruling 12).

### Edits to existing tests (prompt §1.6 item 4)

1. **`test_injection.py`, `TestAFloorRuleDrain.test_8_the_floor_reflection_drains`.**
   - The line `assertEqual(i.alpha_eff, ALPHA * i.w_gate)` is replaced by: the reflection's kind is
     "floor", `resolved` False, `w_gate == 1.0`, `alpha_eff == ALPHA`, 𝒜_k recorded (> 0).
   - The class docstring says why.
   - Reason: P20. The old line still passed (with w_gate = 1 it says α_eff = α), so without the edit
     B5 would not fail it.
2. **`test_gate_and_taper.py`, `TestAFloorRuleCandidate`.**
   - The method is renamed `test_8_the_floor_reflection_is_a_gated_candidate` →
     `test_8_the_floor_reflection_is_an_unresolved_candidate`, and its docstring and the class's
     say why.
   - The line `assertEqual(i.w_gate, SM.gate_weight(i.nonadiabaticity))` is replaced by: kind
     "floor", `resolved` False, `w_gate == 1.0`; `alpha_eff == ALPHA * i.w_gate` becomes
     `== ALPHA`.
   - Reason: P20; the old line failed (1.0 against 0.8019). The floor's 𝒜_k is still 2.012, written
     to stderr as before.

No other existing test was edited.

### The documents (P12)

- `.documents/architecture-summary.md`: a dated note after 03's in §7.2: the constants and
  functions, the records' fields, the reflection kinds, the warnings and the payload key.
- `.documents/numerical-strategies.md`: new §6.4, dated, after §6.3: the assessment, E, the
  decision and its constants, the order, the weight, natural mode, and what was measured.

## 3. Deviations from the prompt

**The rulings of orchestrator note 05 (1–13):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | Warnings deduplicated by kind: four phrase constants, `warning_kind`; `tune_history` prints the first line of each kind. Item 7's test returns one kind with three 𝓡 (and a second kind twice) and asserts the first of each; B9 and B10 each fail it | STRUCTURALLY REQUIRED |
| R2 | The loop's handler sets `e.warnings` and re-raises; `integrate_history`'s failure payload carries `"warnings"` (`_failure_payload`'s optional argument); `tune_history` prints them before its `!!` line. Tested on the P1 harness at α = 0 with budget 213, directly, through `integrate_history` and through `tune_history`; B11 fails two of the three (the loop's own test does not go through `integrate_history`) | STRUCTURALLY REQUIRED |
| R3 | The constants asserted as literals; item 3 asserts φ < 3.0 φ_t and the lower bound with the literal 3; B1 fails item 3. §5.4 gives K = 10 against K = 3 at M = 10⁻⁹ and 3×10⁻¹⁰, and says the planner's K = 10 shift was the floor rule's gate | STRUCTURALLY REQUIRED |
| R4 | The upper bound is the decision; the lower bound is (1 − f)3φ_t(1 − δ)(1 − 8ε), δ = 1 − φ_t,prev/φ_t measured on the history (2.35e-12), the step shown velocity-capped and decelerating; §5.5 tabulates φ/φ_t at every unresolved reflection, smallest 2.70008 before the cut | IMPLEMENTATION CHOICE (ruled) |
| R5 | `Reflection.kind` defaults to `None`; the loop passes every field; `test_5_the_reflection_records_carry_their_kind` and item 3 assert no real record holds `None`. `test_surfing_turning_points.py` passes unedited. The other new fields have no default | STRUCTURALLY REQUIRED |
| R6 | `assess_infall(model, φ, ln ρ_kin,E)` → (ln E, turning point or `None`); `unresolved_turnaround(φ, turning point, M_P)`; the loop forms ln ρ_kin,E as `candidate` does | IMPLEMENTATION CHOICE (ruled) |
| R7 | A `None` assessment makes no decision; the infall's latest stands and a reflection there records it. `None` was returned 0 times in about 400 000 assessments (§5) | IMPLEMENTATION CHOICE (ruled) |
| R8 | One natural-mode call, at the first floor reflection, whatever its outcome; catches `ComputationFailureError` from H_E and the model only; B7 raises `ComputationFailureError`; the wall-side test runs until a warning is made | IMPLEMENTATION CHOICE (ruled) |
| R9 | `!! tune_history (<task label>): ` + the line, `flush=True` | IMPLEMENTATION CHOICE (ruled) |
| R10 | Windows by wrapping `SM.integrate_scalar_history` to pass `N_stop` and leaving `integrate_history` by `_LeaveHistory`; item 5's two runs in `setUpClass`; the window ends at N = 30.3, before both runs' ρ_kin/ρ_R < 10⁻⁶ (30.34933 and 30.34939) | IMPLEMENTATION CHOICE (ruled) |
| R11 | §5.4's table gives 05's values beside the planner's "R only", K = 3 and K = 10 and its w < 1 counts, and says which differences P20, the kinetic E and parking explain; §5.5 gives 05's converged values beside README §8.0's; Observations records the change for 11 | STRUCTURALLY REQUIRED |
| R12 | Only the prompt's §6 files change, plus `.documents/OPEN_ISSUES.md` for the issue opened. `CosmologyConcepts/Models/`, `exceptions.py`, `Policies.py`, `dark_radiation.py`, the golden modules and JSON, `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`, `test_wall_model.py`, tools and drivers are byte-identical | as ruled |
| R13 | Costs are minima of 2 000 calls with `uptime`; suites serial in halves, output to scratch, `uptime` beside each, in the background; §5.5's 18 histories (and 11 more) four at a time in the background, under `timeout 900` (none reached it) | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | The names: the constants, functions and fields of §2; the prompt asked me to choose | IMPLEMENTATION CHOICE |
| D2 | Natural mode's one call: a `None` turning point takes φ for φ_t (the field is at its turning point to rounding). Never seen | IMPLEMENTATION CHOICE |
| D3 | At most one natural-mode warning per integration: a "could not be assessed" line also ends the wall-side test. The prompt's "at the first of these two events" | IMPLEMENTATION CHOICE |
| D4 | The two turning-point loops (the termination step's and the ordinary one) now call one nested helper, `handle_turning_point`, which also captures and resets the assessment and makes the wall-side warning. The arming and the `dropped_drains` count are 03's operations, in the same order | IMPLEMENTATION CHOICE |
| D5 | The step loop is re-indented into `try` for ruling 2's handler: the diff is 738 lines, 484 with `git diff -w` | STRUCTURALLY REQUIRED |
| D6 | `integrate_history`'s sampling-overflow failure carries `result.warnings` too; the tuning's own failure payloads carry no `"warnings"` key, so they are unchanged | IMPLEMENTATION CHOICE |
| D7 | Every turning point resets the infall's assessment, wall-side ones after the capture. After a wall-side point π > 0 until the next plateau, so this equals the prompt's "a plateau turnaround or a reflection starts a new infall" | IMPLEMENTATION CHOICE |
| D8 | Tests beyond the prompt's list: the literal constants (ruling 3), the `None` assessment, the constants read at call time, the reflection kinds (ruling 5), no warning → no line, the loop's own `e.warnings` | IMPLEMENTATION CHOICE |
| D9 | The spy is on `AbstractModel.classical_turning_point`, which `ExponentialModel` inherits and `RealizedModel` calls; "a mock spy on the model's class" | IMPLEMENTATION CHOICE |
| D10 | Item 7's golden check runs the two `golden_history` β; the tuned golden history (M = 10⁻², β = 3) is not run here. Its own test passes unchanged | IMPLEMENTATION CHOICE |
| D11 | B3 and B5 are written by kind inside `candidate` (`resolved or kind == "unresolved"`, `... == "floor"`), so that each breaks only its own reflection kind | IMPLEMENTATION CHOICE |
| D12 | The measurements ran in a scratch worktree holding a copy of the final `ScalarHistory.py` (md5 `e850fecaf104ae2d4393e941b1a5fa4b`, the committed file's) while the breakages were replayed in the main checkout; removed afterwards | IMPLEMENTATION CHOICE |
| D13 | §5.4's comparison is to the cut (the planner's park, ρ_kin/ρ_R < 10⁻⁶ at a candidate, read from the `Injection` before it is recorded, by a scratch hook) and to N = 30.3; §5.5's failures are read through a scratch `Radau` subclass and the same hook. Neither is in the shipped code | IMPLEMENTATION CHOICE |
| D14 | **Prompt §5's band stop was not taken at M = 3×10⁻¹¹**, where the resolved run has a different number of wall approaches and Σ differs by 3.6e-4. The stop is "differ by much more than the probes found". There the probes found the same: their "R only" parked after 24 floor reflections and one resolved wall, against 41. The cause is the resolved run's float-spacing failure, which R8 removes (§5.4). At M = 10⁻⁹–10⁻¹⁰ the counts are equal and Σ agrees to ≤ 1.4e-6. Flagged for the orchestrator's review | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT.

## 4. Verification performed

### 4.1 The suites

Serially, ComputeTargets in two halves, output in my scratch directory. "Before" at `cd19768`
before any edit; "after" on the final tree.

| Suite | Before (count, time, 1-min load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 71 s, 5.4 → 29.7 | 26 OK, 69 s, 19.5 → 14.9 |
| ComputeTargets [a-f] | 96 OK, 124 s, 29.7 → 8.0 | 96 OK, 133 s, 14.9 → 23.9 |
| ComputeTargets [g-z] | 243 OK, 285 s, 8.0 → 13.6 | **278** OK, 318 s, 23.9 → 4.6 |
| ObjectFactories | 103 OK, 17 s, 13.6 → 10.7 | 103 OK, 18 s, 4.6 → 5.4 |
| tools | 115 OK, 125 s, 10.7 → 4.6 | 115 OK, 131 s, 5.4 → 7.6 |
| RunDrivers | 64 OK, 24 s, 4.6 → 4.6 | 64 OK, 28 s, 7.6 → 18.7 |

- The baselines are the dispatch's: 26 / 339 (96 + 243) / 103 / 115 / 64.
- ComputeTargets rose by 35, the new module, which sorts into [g-z]. Nothing fell.
- **The golden tests passed unchanged**: `test_golden_history`, the tuned golden test,
  `test_first_bounce`, `test_surfing_turning_points` and `test_kinematic_cap_loop`.
- `black --check` on the four changed Python files: all four unchanged.

### 4.2 α > 0 at M = 0.5 M_P is 03's (prompt §2.2)

The dispatch commit `cd19768` in a scratch worktree, and the final tree, each integrating β = 1.2,
2, 5, 25 at α = 0.1 (`integrate_history`, test_injection's setup), compared after pickling as plain
tuples:

| β | bitwise (turning points, `Injection` fields 1–14, reflections, sample, final state, N_final, nfev, steps, dropped, ledger) | candidates | resolved | assessed | first candidate N | log₁₀ 𝓡 there | φ_t there (M_P) | warning |
|---|---|---|---|---|---|---|---|---|
| 1.2 | yes | 7 | 7 | 7 | 17.66270 | −62.514 | 5.097970e-3 | nearly elastic |
| 2 | yes | 28 | 28 | 28 | 20.34324 | −61.301 | 4.960119e-3 | nearly elastic |
| 5 | yes | 155 | 155 | 155 | 29.71679 | −53.351 | 4.229069e-3 | nearly elastic |
| 25 | yes | 296 | 296 | 296 | 127.82996 | −56.696 | 4.454159e-3 | nearly elastic |

At β = 25 the first wall-side turning point is at N = 127.82; the history ends at N = 159.82.

### 4.3 Item 3's history (M = 10⁻⁸ M_P, β = 2, α = 0.1, to N = 20.6)

- **Why it fires first.** The field falls from 5 M_P with |π| ≈ 0.4976 M_P. The floor rule fires
  where fφ/|π| < h_floor, φ < 10⁻¹⁰|π| ≈ 5×10⁻¹¹ M_P. 3 φ_t = 2.83×10⁻¹⁰ M_P lies outside that, so
  the assessment reaches φ < 3φ_t first. At M = 10⁻⁹ (3 φ_t ≈ 2.8×10⁻¹¹) the floor fires first.
- **The reflection**: N = 20.352100382, φ = 2.7904×10⁻¹⁰ M_P, φ_t = 9.4301×10⁻¹¹ M_P,
  φ/φ_t = 2.95900, log₁₀ 𝓡 = −27.763, 𝒜_k = 229.7, w_gate = 1.0, α_eff = 0.1. It is the 450th step
  start; 450 assessments, none `None`, about 0.3 s.
- **The step before**: φ_prev/φ_t,prev = 3.2878 (not fired); h = 6.2303940e-11 against the velocity
  cap fφ/|π| = 6.2304047e-11; π′ = +2.67e-5 (decelerating); φ/φ_prev = 0.90000017;
  δ = 1 − φ_t,prev/φ_t = 2.348e-12. The bound (1 − f)3φ_t(1 − δ)(1 − 8ε) = 2.5461×10⁻¹⁰ M_P;
  φ is 1.0959 times it.
- **H_E across it**: |ΔH/H| = 0 against 02's bound 2.82e-14 (c = 0.043).
- `first_bounce`: reflected, at N = 20.352100382.

## 5. The measurement (prompt §2)

Every history: test_injection's setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, default
tolerances, the seed realization, untuned), step budget 2×10⁵ (patched `SM.StepControl`), α = 0.1
unless stated. The scripts are `band.py`, `common.py`, `bench.py`, `dump_mhalf.py` and
`compare_mhalf.py` in my scratch directory.

### 5.1 The cost (prompt §2.1)

**Per assessment**, minima (means) of 2 000 calls, at assessed step starts of the first infall, load
15–20:

| State | H_E (policy + hubble) | `assess_infall` | `classical_turning_point` alone | together (H_E, ln ρ_kin, assess) |
|---|---|---|---|---|
| M = 0.5, mid-infall (φ = 3.71 M_P, φ/φ_t = 963) | 10.8 µs (11.4) | 19.5 (21.3) | 19.0 (20.6) | 31.9 (34.3) |
| M = 0.5, at the wall (φ/φ_t = 1.0000) | 10.8 (11.1) | 16.3 (18.3) | 16.3 (17.9) | 28.6 (31.8) |
| M = 10⁻⁹, mid-infall (φ/φ_t = 1.4×10¹¹) | 10.8 (11.0) | 21.5 (23.4) | 21.1 (22.7) | 33.8 (35.9) |
| M = 10⁻⁹, near the wall (φ/φ_t = 8.4) | 10.8 (11.0) | 16.9 (18.6) | 16.5 (18.1) | 29.4 (31.9) |

**Per history**, M = 0.5, β = 2, α = 0.1 (`integrate_history`, sampling included), the dispatch commit
and the final tree back to back, three rounds, load 11–16:

| | round 1 | round 2 | round 3 | minimum |
|---|---|---|---|---|
| `cd19768` | 2.41 s | 2.38 s | 2.38 s | 2.38 s |
| this commit | 2.58 s | 2.55 s | 2.49 s | 2.49 s (+4.6 %) |

1723 assessments × about 30 µs is 0.05 s of the 0.11 s.

**Assessments per history**:
- M = 0.5: 864, 1723, 3799 and 6068 at β = 1.2, 2, 5 and 25;
- the band (to the cut): 7 100–9 900;
- §5.5: 3 000–31 000.

`None` was returned 0 times in all of them.

### 5.2 M = 0.5 M_P (prompt §2.2)

§4.2. All four are bitwise 03's in every pre-05 field; every candidate is resolved and assessed; each
warns "nearly elastic" once, with log₁₀ 𝓡 = −62.5 to −53.4 at the first impact.

### 5.3 Where ln 𝓡 > 0 is seen (orchestrator note §0)

At M = 10⁻⁹, β = 2, 100 of the first 467 assessments (N < 20.4) have ln 𝓡 > 0, up to +5.96 at
φ = 5 M_P, all at φ/φ_t ≥ 7.5×10¹¹. That is the physics: 𝓡 rises with E, and early on E is large. The
rule cannot fire there, since it needs φ < 3φ_t. At M = 10⁻⁷ and 0.5 the largest ln 𝓡 is −12.5 and
−80.4. At M = 10⁻³⁰ the first impact has log₁₀ 𝓡 = +58 to +61, so no "nearly elastic" line.

### 5.4 The probe's band (prompt §2.3; ruling 11)

β = 2, α = 0.1, to the cut: each run stops at the first candidate with ρ_kin/ρ_R < 10⁻⁶, read before it
is recorded, as the planner's park did. "t0" is the threshold patched to 0 (every wall resolved,
𝓡 < 1); "K10" is K patched to 10. The ceiling is 1/(6β² − 1) = 1/23.

| M (M_P) | run | candidates | Σ Δρ/ρ_R | / ceiling | cut at N | failure | planner: R only / K = 10 / K = 3 (w<1) |
|---|---|---|---|---|---|---|---|
| 10⁻⁹ | 05 | 3 floor, 45 unresolved | 0.01261597462 | 0.2902 | 30.349330 | — | 0.2664 (3) / 0.2902 (0) / 0.2664 (3) |
| | t0 | 6 floor, 42 turning point | 0.01261596515 | 0.2902 | 30.349387 | — | |
| | K10 | 48 unresolved | 0.01261597478 | 0.2902 | 30.349327 | — | |
| 3×10⁻¹⁰ | 05 | 6 floor, 42 unresolved | 0.01261597463 | 0.2902 | 30.349330 | — | 0.2206 (5) / 0.2363 (3) / 0.2206 (5) |
| | t0 | 9 floor, 39 turning point | 0.01261597456 | 0.2902 | 30.349332 | — | |
| | K10 | 3 floor, 45 unresolved | 0.01261597462 | 0.2902 | 30.349330 | — | |
| 10⁻¹⁰ | 05 | 9 floor, 39 unresolved | 0.01261597459 | 0.2902 | 30.349329 | — | FAIL (spacing) / 0.2186 (6) / 0.2177 (7) |
| | t0 | 22 floor, 26 turning point | 0.01261595682 | 0.2902 | 30.349806 | — | |
| 3×10⁻¹¹ | 05 | 23 floor, 25 unresolved | 0.01261597460 | 0.2902 | 30.349329 | — | 0.2176 (8) / 0.2177 (8) / 0.2177 (8) |
| | t0 | 34 floor | 0.01261119993 | 0.2901 | **30.009613**, a resolved wall whose drain point has ρ_kin/ρ_R = 1.9×10⁻¹⁵ | — | |

**05 against the resolved run**, relative difference in Σ:

| M | to the cut | to N = 30.3 | candidates to N = 30.3 |
|---|---|---|---|
| 10⁻⁹ | 7.50e-7 | 7.49e-7 | 46 / 46 |
| 3×10⁻¹⁰ | 5.6e-9 | 5.5e-9 | 46 / 46 |
| 10⁻¹⁰ | 1.41e-6 | 1.41e-6 | 46 / 46 |
| 3×10⁻¹¹ | 3.79e-4 | 3.60e-4 | 46 / 34 floor + 67 turning point |

**K = 10 against K = 3** (ruling 3): 1.6e-8 relative at M = 10⁻⁹, 0 at 3×10⁻¹⁰. **Under P20, K is
invisible in Σ.** The planner's K = 10 shift (0.2902 against 0.2664 at 10⁻⁹) came from the gate on
floor-rule reflections: in its K = 3 run the first three approaches were floor reflections at
φ ≈ 4–5 φ_t, gated (its "w<1: 3"), and K = 10 judged them unresolved, at w = 1. Under P20 they drain
at w = 1 at every K (`b2_M1e-9_t1e-8_gate_K3.json` and `b2_M1e-9_t0_gate.json` under
`var/probes/floor-cascade-2026-10-29b/`, read only).

**Which differences from the probe each cause explains:**
- **P20** explains all of them at the cut. Every 05 run gives 0.2902 of the ceiling, the planner's
  w = 1 value. Its "R only" and K = 3 values are lower by the floor reflections it gated, 3 to 8 of
  them, with w down to 0.62.
- **The kinetic E** explains none measurable. Before the cut, (V − V_floor)/E ≤ 3.7e-16 at every
  unresolved reflection, so φ_t is the same to rounding.
- **Parking** sets where each run stops. The cut here is the planner's park, without its release of
  the remaining drain, so the stopping N agree (30.3493 against its 30.697 at 10⁻⁹ under the gated
  floors; with w = 1 the cascade drains faster).

**M = 3×10⁻¹¹: the resolved run is wrong, not the rule.**
- With the threshold patched to 0, the field reaches a wall with φ_t = 5.0×10⁻¹³ M_P after its 34th
  floor reflection (at 1.35 φ_t, N = 30.009598).
- Within 1.5×10⁻⁵ e-folds Radau records a plateau and a resolved wall-side turning point. The next
  drain point has π = 1.06×10⁻⁷ M_P: ρ_kin/ρ_R falls from 8.7×10⁻⁶ to 1.9×10⁻¹⁵ across one bounce.
- The field then sits near φ ≈ 7.7×10⁻¹³ M_P, with 66 more turning points by N = 30.3.
- No physics removes 10⁹ of the kinetic energy in 10⁻⁵ e-folds. This is the float-spacing failure
  of a resolved wall that R8 exists to remove, in its milder form: at M = 10⁻¹⁰ the planner's
  "R only" failed outright.
- The planner's "R only" at 3×10⁻¹¹ went the same way (`b2_M3e-11_t0_gate.json`): 24 floor
  reflections and one resolved wall, then a park at N = 28.75, against its K = 3 run's 41 floor
  reflections and a park at 29.07.
- 05's rule reflects that wall at 2.708 φ_t and continues as at every other M: Σ = 0.01261597460,
  equal to the other three to 3×10⁻¹⁰.

So the band's one difference "by much more than the probes found" (prompt §5) is the resolved run's
failure, which the probes found too. Item 5 is at M = 10⁻⁹, where both runs are sound. **Its
tolerance**, 1e-5 relative, is 7 times the largest difference measured where both are sound
(1.41e-6, at 10⁻¹⁰).

### 5.5 Down to laboratory M (prompt §2.4)

α = 0.1, to T_CMB, budget 2×10⁵. "Σ" is to the failure. "Σ to cut" is over the candidates before
the first with ρ_kin/ρ_R < 10⁻⁶ ("cut" gives that candidate's index). "W/E" is the largest
(V − V_floor)/E at an unresolved reflection; "1st 𝓡" is log₁₀ 𝓡 at the first candidate.

| β | M | failure | N | T_J | \|π\| (M_P) | candidates | Σ (/ ceiling) | Σ to cut (/ ceiling), cut | W/E | 1st 𝓡 | warning | assessments | `None` | wall, steps |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻⁷ | G2: W/(π²/2) = 2.98 | 33.9858 | 41.8 eV | 2.24e-6 | 467 unresolved | 0.015092605 (0.1153) | 0.015092104 (0.1153), 6 | 0.451 | −33.59 | nearly elastic | 4 696 | 0 | 3 s, 5 228 |
| 1.2 | 10⁻⁸ | G2: 1.029 | 33.9460 | 43.5 eV | 1.60e-6 | 488 unresolved | 0.015092605 (0.1153) | 0.015092105 (0.1153), 6 | 0.497 | −29.59 | nearly elastic | 6 567 | 0 | 4 s, 7 251 |
| 1.2 | 10⁻⁹ | G2: 1.023 | 33.9324 | 44.1 eV | 5.63e-7 | 1 floor, 497 unresolved | 0.015092605 (0.1153) | 0.015092105 (0.1153), 6 | 0.498 | −25.59 | nearly elastic | 8 868 | 0 | 5 s, 9 765 |
| 1.2 | 10⁻¹⁰ | G2: 1.032 | 33.9290 | 44.3 eV | 2.03e-7 | 5 floor, 487 unresolved | 0.015092606 (0.1153) | 0.015092105 (0.1153), 6 | 0.499 | −21.59 | nearly elastic | 11 537 | 0 | 6 s, 12 712 |
| 1.2 | 10⁻¹² | `log_m_eff_squared`: m_eff² ≤ 0 at a sample with φ = −2.37e-12 | 32.6282 | 163 eV | 5.66e-8 | 13 floor | 0.015092366 (0.1153) | 0.015092105 (0.1153), 6 | — | −13.59 | nearly elastic | 3 030 | 0 | 2 s, 3 275 |
| 1.2 | 10⁻³⁰ | `classical_turning_point`: not monotone (φ = 3.43e-17) | 33.9253 | 44.4 eV | 3.73e-7 | 166 floor | 0.015092605 (0.1153) | 0.015092105 (0.1153), 6 | — | +58.41 | none | 24 167 | 0 | 13 s, 25 561 |
| 2 | 10⁻⁷ | G2: 88.6 | 31.3361 | 32.3 keV | 1.08e-8 | 508 unresolved | 0.012616641 (0.2902) | 0.012615974 (0.2902), 48 | 0.261 | −31.76 | nearly elastic | 8 771 | 0 | 6 s, 9 593 |
| 2 | 10⁻⁸ | G2: 90.1 | 31.1527 | 38.8 keV | 6.77e-9 | 546 unresolved | 0.012616642 (0.2902) | 0.012615975 (0.2902), 48 | 0.274 | −27.76 | nearly elastic | 11 762 | 0 | 8 s, 12 748 |
| 2 | 10⁻⁹ | G2: 86.9 | 31.1075 | 40.6 keV | 3.18e-9 | 3 floor, 575 unresolved | 0.012616642 (0.2902) | 0.012615975 (0.2902), 48 | 0.308 | −23.76 | nearly elastic | 15 229 | 0 | 10 s, 16 391 |
| 2 | 10⁻¹⁰ | G2: 81.9 | 31.0948 | 41.1 keV | 1.27e-9 | 9 floor, 593 unresolved | 0.012616642 (0.2902) | 0.012615975 (0.2902), 48 | 0.339 | −19.76 | nearly elastic | 19 021 | 0 | 12 s, 20 365 |
| 2 | 10⁻¹² | G2: 1.78 | 31.0896 | 41.3 keV | 2.53e-9 | 67 floor, 579 unresolved | 0.012616642 (0.2902) | 0.012615975 (0.2902), 48 | 0.472 | −11.76 | nearly elastic | 26 646 | 0 | 17 s, 28 340 |
| 2 | 10⁻³⁰ | `classical_turning_point`: not monotone (φ = 3.31e-17) | 31.0856 | 41.5 keV | 3.57e-7 | 184 floor | 0.012616642 (0.2902) | 0.012615975 (0.2902), 48 | — | +60.24 | none | 29 586 | 0 | 19 s, 31 160 |
| 3 | 10⁻⁷ | G2: 1.012 | 29.6656 | 18.05 MeV | 1.82e-6 | 599 unresolved | 0.007961977 (0.4220) | 0.007961174 (0.4219), 58 | 0.487 | −30.71 | nearly elastic | 9 703 | 0 | 6 s, 10 490 |
| 3 | 10⁻⁸ | G2: 26.0 | 29.6523 | 18.28 MeV | 3.07e-8 | 565 unresolved | 0.007961977 (0.4220) | 0.007961174 (0.4219), 58 | 0.059 | −26.71 | nearly elastic | 12 714 | 0 | 9 s, 13 667 |
| 3 | 10⁻⁹ | G2: 18.7 | 29.6485 | 18.35 MeV | 1.43e-8 | 592 unresolved | 0.007961977 (0.4220) | 0.007961174 (0.4219), 58 | 0.084 | −22.71 | nearly elastic | 16 255 | 0 | 11 s, 17 381 |
| 3 | 10⁻¹⁰ | G2: 18.0 | 29.6473 | 18.37 MeV | 5.35e-9 | 19 floor, 590 unresolved | 0.007961977 (0.4220) | 0.007961174 (0.4219), 58 | 0.104 | −18.71 | nearly elastic | 20 006 | 0 | 14 s, 21 311 |
| 3 | 10⁻¹² | G2: 17.4 | 29.6467 | 18.38 MeV | 7.19e-10 | 87 floor, 564 unresolved | 0.007961976 (0.4220) | 0.007961173 (0.4219), 58 | 0.153 | −10.71 | nearly elastic | 27 127 | 0 | 19 s, 28 779 |
| 3 | 10⁻³⁰ | `classical_turning_point`: not monotone (φ = 3.33e-17) | 29.6466 | 18.38 MeV | 3.60e-7 | 219 floor | 0.007961976 (0.4220) | 0.007961173 (0.4219), 58 | — | +61.29 | none | 31 030 | 0 | 21 s, 32 723 |

"G2" is guard G2's "reflection requested inside the wall: W/(π²/2) = …", at an unresolved reflection
in every case.

**How they fail, for 06.** Every history crosses ρ_kin/ρ_R < 10⁻⁶ first: at candidate 6, 48 and 58
at β = 1.2, 2 and 3, the same N at every M (28.364, 30.349, 29.394). Each then fails 0.25–5.6 e-folds
later, at the bottom of its cascade, in one of three ways:
1. **Guard G2 at an unresolved reflection** (M = 10⁻⁷–10⁻¹²).
   - Deep in the cascade the field oscillates about its effective minimum, and the energy is no
     longer all kinetic. E = ρ_kin + W ≈ W, so φ_t ≈ φ, and the rule fires at φ/φ_t down to 1.011.
   - (V − V_floor)/E reaches 0.50 at the recorded unresolved reflections after the cut, and finally
     W exceeds the kinetic energy, which G2 refuses.
   - 06's cut (tested only where the energy is all kinetic, R14) parks the field before this.
2. **The model's search refuses a quantized wall** (M = 10⁻³⁰).
   - Once |π| ≈ 4×10⁻⁷ M_P, the floor rule reflects at φ ≈ 3×10⁻¹⁷ M_P, where M/φ ≈ 3×10⁻¹⁴. That is
     below one ulp of ln V (5.7×10⁻¹⁴ at ln V ≈ −276.6).
   - `ln V − ln V_floor` is the same float (5.68e-14) at φ and φ/e, and `_search_log_field` refuses
     the repeated finite value as "not monotone". Before that point it is exactly 0.0, a −∞
     residual, which the search allows.
   - It is a defect of 04's generic method, not of the rule. Opened as
     `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`. The cut comes first on every
     history measured.
3. **`log_m_eff_squared` at a dense-output sample with φ < 0** (β = 1.2, M = 10⁻¹², N = 32.63): 03's
   ten samples per step read an interpolant that dips below zero inside a step whose ends are
   positive. Also after the cut.

**φ/φ_t at the unresolved reflections** (ruling 4), counts in the bins [1, 1.5), [1.5, 2), [2, 2.5),
[2.5, 2.7), [2.7, 2.8), [2.8, 2.9), [2.9, 3):

| History | before the cut: n, smallest, largest W/E, bins | after the cut: n, smallest, largest W/E, bins |
|---|---|---|
| band β = 2, M = 10⁻⁹ | 45, 2.700143, 1.8e-16, [0,0,0,0,13,18,14] | — |
| band β = 2, M = 3×10⁻¹⁰ | 42, 2.704148, 1.2e-16, [0,0,0,0,11,16,15] | — |
| band β = 2, M = 10⁻¹⁰ | 39, 2.701750, 2.2e-16, [0,0,0,0,9,20,10] | — |
| band β = 2, M = 3×10⁻¹¹ | 25, 2.707542, 1.1e-16, [0,0,0,0,7,9,9] | — |
| β = 1.2, M = 10⁻⁷ | 6, 2.708162, 9.4e-19, [0,0,0,0,1,2,3] | 461, 1.061293, 0.451, [133,103,79,28,50,33,35] |
| β = 1.2, M = 10⁻⁸ | 6, 2.706748, 7.7e-18, [0,0,0,0,3,1,2] | 482, 1.062787, 0.497, [136,103,80,27,53,45,38] |
| β = 1.2, M = 10⁻⁹ | 5, 2.702717, 1.9e-18, [0,0,0,0,4,1,0] | 492, 1.076549, 0.498, [134,101,78,28,52,56,43] |
| β = 1.2, M = 10⁻¹⁰ | 1, 2.864197, 2.4e-18, [0,0,0,0,0,1,0] | 486, 1.098070, 0.499, [129,100,79,26,60,48,44] |
| β = 2, M = 10⁻⁷ | 48, 2.700082, 3.7e-16, [0,0,0,0,19,12,17] | 460, 1.040738, 0.261, [125,102,78,30,35,46,44] |
| β = 2, M = 10⁻⁸ | 48, 2.700973, 1.2e-16, [0,0,0,0,13,14,21] | 498, 1.039504, 0.274, [134,106,81,32,38,49,58] |
| β = 2, M = 10⁻⁹ | 45, 2.700143, 1.8e-16, [0,0,0,0,13,18,14] | 530, 1.037554, 0.308, [137,107,83,31,58,64,50] |
| β = 2, M = 10⁻¹⁰ | 39, 2.701750, 2.2e-16, [0,0,0,0,9,20,10] | 554, 1.036685, 0.339, [138,108,82,31,63,71,61] |
| β = 2, M = 10⁻¹² | 0 | 579, 1.029197, 0.472, [140,108,84,29,90,79,49] |
| β = 3, M = 10⁻⁷ | 58, 2.701146, 1.3e-22, [0,0,0,0,23,18,17] | 541, 1.011204, 0.487, [190,109,86,35,34,46,41] |
| β = 3, M = 10⁻⁸ | 58, 2.701423, 5.3e-23, [0,0,0,0,23,13,22] | 507, 1.046371, 0.059, [137,110,84,32,55,59,30] |
| β = 3, M = 10⁻⁹ | 58, 2.700265, 9.0e-23, [0,0,0,0,19,22,17] | 534, 1.041823, 0.084, [138,110,85,33,72,47,49] |
| β = 3, M = 10⁻¹⁰ | 39, 2.728646, 5.5e-23, [0,0,0,0,9,15,15] | 551, 1.039647, 0.104, [138,110,86,34,63,72,48] |
| β = 3, M = 10⁻¹² | 0 | 564, 1.035434, 0.153, [139,110,85,34,61,75,60] |
| **all** | **562, 2.700082**, ≤ 3.7e-16 | 7 239, 1.011204, ≤ 0.499 |

The M = 10⁻³⁰ histories have no unresolved reflection: the floor rule fires far outside 3φ_t.

**Against README §8.0's converged values** (ruling 11):

| β | 05 to the cut (any M) | README §8.0 (gated floor reflections) |
|---|---|---|
| 1.2 | 0.1153 | 0.115 |
| 2 | 0.2902 | 0.218 |
| 3 | 0.4219 | 0.261 |

The drain is still independent of M. At each β, Σ to the cut agrees across M = 10⁻⁷ to 10⁻³⁰ to
3×10⁻⁹ relative, far inside 11's 10⁻³. P20 moves the converged fraction at β = 2 and 3 (Observations).

### 5.6 Natural mode (prompt §2.5)

α = 0, β = 2, budget 2×10⁵:

| M (M_P) | outcome | warning |
|---|---|---|
| 0.5 | reaches T_CMB (N = 48.70), no reflection | none |
| 10⁻⁷ | fails: step budget exhausted at N = 42.775 (T_J = 0.35 eV), no reflection | "below" (a wall-side turning point; no model call) |
| 10⁻¹⁰ | fails: step budget exhausted at N = 42.518 (T_J = 0.45 eV), 25 reflections | "below" (the first floor reflection; one model call) |

Under `main.py`'s budget (2×10⁶) M = 10⁻⁷ may finish; U10 runs it as the reference.

## 6. The deliberate-breakage record (P11)

- **The replay.**
  - `make_breakages.py` wrote each diff against the final `ScalarHistory.py` (difflib).
  - `replay.sh`, run in bash, checked each with `git apply --check`, applied it, checked it with
    `git apply -R --check`, ran `test_unresolved_turnarounds` and the two item-8 classes (38 tests),
    reverted it, and compared the md5 with the final file's. All were restored.
  - Both scripts are in my scratch directory. None is committed.
- B1–B9 are the prompt's, in its order; B10 and B11 are rulings 1's and 2's.

**B1 — K = 10.** Fails 5: `test_3_phi_lies_below_three_phi_t` (the reflection is at 9.0 φ_t; ruling
3), `test_2_K_is_3_and_the_threshold_is_1e_minus_8_M_P`, `test_2_phi_equal_to_K_phi_t_does_not_fire`,
`test_2_outside_K_phi_t_nothing_fires`, `test_2_the_rule_reads_its_constants_at_call_time`. Item 5
passes under it, as ruling 3 expects.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -105,7 +105,7 @@
 #   UNRESOLVED_PHI_T_THRESHOLD_MP:     the threshold on phi_t, in units of M_P; multiplied by the
 #                                      units' PlanckMass where it is used
 # unresolved_turnaround reads both at call time, so that a patch of either changes the rule
-UNRESOLVED_REFLECTION_FACTOR = 3.0
+UNRESOLVED_REFLECTION_FACTOR = 10.0
 UNRESOLVED_PHI_T_THRESHOLD_MP = 1.0e-8
```

**B2 — ≤ in each comparison.** Fails the three edge tests: `test_2_phi_equal_to_K_phi_t_does_not_fire`,
`test_2_phi_t_equal_to_the_threshold_does_not_fire`, `test_2_ln_R_equal_to_zero_does_not_fire`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -949,10 +949,10 @@
     comparisons are strict. K and the threshold are read here, at call time.
     """
     phi_t: float = turning_point.phi_t
-    if not phi_Einstein < UNRESOLVED_REFLECTION_FACTOR * phi_t:
+    if not phi_Einstein <= UNRESOLVED_REFLECTION_FACTOR * phi_t:
         return False
     return (
-        phi_t < UNRESOLVED_PHI_T_THRESHOLD_MP * PlanckMass or turning_point.log_R > 0.0
+        phi_t <= UNRESOLVED_PHI_T_THRESHOLD_MP * PlanckMass or turning_point.log_R >= 0.0
     )
```

**B3 — the gate kept at an unresolved reflection.** Fails 2: `test_3_w_gate_is_one_and_alpha_eff_is_alpha`
and `test_3_pi_out_and_the_dense_output`. The floor reflections stay at w = 1, so item 4 passes.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1312,7 +1312,9 @@
         # rho_kin,E = H_E^2 pi^2/2, in logs (prompt 02)
         log_rho_kin: float = 2.0 * log(H_before) + 2.0 * log(abs(pi_before)) - LOG_2
 
-        alpha_eff, w_gate = alpha_effective(nonadiabaticity, resolved)
+        alpha_eff, w_gate = alpha_effective(
+            nonadiabaticity, resolved or kind == "unresolved"
+        )
         phi_a, log_E_a, turning_point_a = (
             (None, None, None) if assessment is None else assessment
         )
```

**B4 — E = ρ_kin alone.** Fails 4: `test_2_ln_E_is_rho_kin_plus_the_wall`, `test_2_phi_t_is_the_root_for_that_E`,
and on the M = 0.5 history `test_6_every_candidate_resolved_with_an_assessment_and_no_reflection`
and `test_8_a_drain_points_assessment_is_its_infalls_last`. Near the turning point, with E = ρ_kin,
the field is inside its "classical wall" and the assessment returns `None`, so the drain point records an
earlier one.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -928,7 +928,7 @@
     log_V_floor: float = model.log_V_floor
     log_V: float = model.log_V(phi_Einstein)
 
-    if log_V > log_V_floor:
+    if False:
         log_W: float = log_V + log(-expm1(log_V_floor - log_V))
         if log_W > log_rho_kin:
             log_E: float = log_W + log1p(exp(log_rho_kin - log_W))
```

**B5 — the floor rule gated again.** Fails item 4: `test_injection`'s
`test_8_the_floor_reflection_drains` and `test_gate_and_taper`'s
`test_8_the_floor_reflection_is_an_unresolved_candidate` (w_gate 0.802).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1312,7 +1312,9 @@
         # rho_kin,E = H_E^2 pi^2/2, in logs (prompt 02)
         log_rho_kin: float = 2.0 * log(H_before) + 2.0 * log(abs(pi_before)) - LOG_2
 
-        alpha_eff, w_gate = alpha_effective(nonadiabaticity, resolved)
+        alpha_eff, w_gate = alpha_effective(
+            nonadiabaticity, resolved or kind == "floor"
+        )
         phi_a, log_E_a, turning_point_a = (
             (None, None, None) if assessment is None else assessment
         )
```

**B6 — an unresolved reflection not appended to `reflections`.** Errors item 3's `setUpClass`
(no reflection: its 10 tests do not run) and fails `test_5_the_same_wall_approaches_and_candidates`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1659,9 +1659,10 @@
                 ratio = _reflection_guards(policy, N, solver.y, task_label)
                 max_ratio = ratio if max_ratio is None else max(max_ratio, ratio)
 
-                reflections.append(
-                    Reflection(N=N, phi_Einstein=phi, pi_Einstein_in=pi_, kind=kind)
-                )
+                if kind == "floor":
+                    reflections.append(
+                        Reflection(N=N, phi_Einstein=phi, pi_Einstein_in=pi_, kind=kind)
+                    )
                 supervisor.notify_reflection(N)
```

**B7 — the natural-mode model call unguarded** (it catches nothing the model raises). Errors
`test_7_a_failing_model_call_is_a_warning_and_the_run_is_todays` with the patched
`ComputationFailureError`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1607,7 +1607,7 @@
             H_: float = H_Einstein_at(N_, y_)
             log_rho_kin_: float = 2.0 * log(H_) + 2.0 * log(abs(float(y_[1]))) - LOG_2
             _, turning_point_ = assess_infall(model, phi_, log_rho_kin_)
-        except ComputationFailureError as e:
+        except ZeroDivisionError as e:
             natural_warned = True
             warnings.append(
                 f"{WARNING_WALL_UNASSESSED} at the first floor reflection (N = {N_:.10g}, "
```

**B8 — the assessment at α = 0 on every infalling step.** Fails 2 and errors 1:
- `test_1_no_model_call_and_no_warning_on_the_golden_histories` (the spy is called);
- `test_1_one_model_call_on_the_floor_harness_and_the_run_is_todays`;
- `test_7_a_failing_model_call_*`, whose raising model now fails the history.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1188,7 +1188,7 @@
     #                       the drain in `armed`
     Mp: float = policy.units.PlanckMass
     assess_wall: bool = (
-        alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None
+        model.reflects_at_origin and model.log_V_floor is not None
     )
     infall_assessment: Optional[Tuple[float, float, ClassicalTurningPoint]] = None
     armed_assessment: Optional[Tuple[float, float, ClassicalTurningPoint]] = None
```

**B9 — `tune_history` printing on every integration.** Fails
`test_7_each_kind_is_printed_once_from_the_first_integration_that_returns_it` (five lines).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2920,7 +2920,7 @@
         # before a failure's line (orchestrator note 05, rulings 1, 2 and 9)
         for line in data.get("warnings", ()):
             line_kind = warning_kind(line)
-            if line_kind not in warned_kinds:
+            if True:
                 warned_kinds.add(line_kind)
                 print(f"!! tune_history ({task_label}): {line}", flush=True)
```

**B10 — deduplicated by the whole string** (ruling 1). Fails the same test: every 𝓡 differs, so
five lines.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2919,7 +2919,7 @@
         # line of each kind, from the first integration that returns it, whatever progress is, and
         # before a failure's line (orchestrator note 05, rulings 1, 2 and 9)
         for line in data.get("warnings", ()):
-            line_kind = warning_kind(line)
+            line_kind = line
             if line_kind not in warned_kinds:
                 warned_kinds.add(line_kind)
                 print(f"!! tune_history ({task_label}): {line}", flush=True)
```

**B11 — the warnings dropped on failure** (ruling 2). Errors `test_7_the_failure_payload_carries_them`
(no `"warnings"` key) and fails `test_7_tune_history_prints_them_before_its_failure_line` (one line,
not two).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2465,7 +2465,7 @@
         )
         # the warnings collected before the failure (inelastic-reflection prompt 05; orchestrator
         # note 05, ruling 2); none if the failure came from outside the step loop
-        return _failure_payload(e.message, warnings=getattr(e, "warnings", ()))
+        return _failure_payload(e.message)
 
     # the first bounce, on the dense output (science-readiness prompt 03); None if there is none
     bounce: Optional[FirstBounce] = first_bounce(result)
```

## 7. Observations not acted on

- **The resolved comparison at M = 3×10⁻¹¹ is numerically wrong** (§5.4). With the threshold patched
  to 0, a resolved wall at φ_t = 5×10⁻¹³ M_P loses 10⁹ of the kinetic energy in one bounce, and the
  history then sits near φ_t. 05's rule reflects that wall at 2.708 φ_t and agrees with M = 10⁻⁹ to
  3×10⁻¹⁰ in Σ. The planner's "R only" went the same way there. It is the failure R8 removes; it
  needs no action. It is the band's only difference larger than 1.4e-6.
- **The converged drain under P20** (§5.5): 0.1153, 0.2902 and 0.4219 of the ceiling at β = 1.2, 2
  and 3, against README §8.0's 0.115, 0.218 and 0.261, which had gated floor reflections. 11's gate
  "independent of M below 10⁻¹² to 10⁻³" holds (to 3×10⁻⁹). Any number taken from §8.0 or
  `scan_K_compare.out` as a reference for 11 is superseded at β = 2 and 3.
- **K is invisible in Σ under P20** (§5.4): K = 10 and K = 3 agree to 1.6e-8. K still sets where
  each unresolved reflection is made, and so 08's per-generation records.
- **`"reflections"` is also `REFLECTIONS_KEY`** in `extra_metadata` (`build_extra_data`), and now
  counts unresolved reflections. At α = 0 nothing changes, and no α > 0 history is stored before 09.
  `REFLECTIONS_KEY`'s comment still says "elastic reflections (the floor-triggered reflection
  model)". For 09 to decide.
- **`integrate_history`'s verbose line** still says "elastic reflection(s)"; at α > 0 they drain.
  Unchanged.
- **`classical_turning_point` refuses a quantized wall** (§5.5 item 2). It raises "not monotone"
  where V − V_floor is a few ulps of ln V (M/φ ≈ 3×10⁻¹⁴ at ln V ≈ −277). Opened as
  `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`. A fix is 04's methods', not this
  prompt's (prompt §5).
- **Two more bottom-of-cascade failure modes**, both after the cut (§5.5): guard G2 at an unresolved
  reflection where E ≈ V − V_floor; and `log_m_eff_squared` on a dense-output sample with φ < 0
  (β = 1.2, M = 10⁻¹²). 06's cut precedes both on every history measured. If 06 finds a history
  where it does not, they are the next failures.
- **The dissipation-mode warning fires on every history above M ≈ 4×10⁻¹⁶ M_P**, at log₁₀ 𝓡 from
  −62 (M = 0.5) to −11 (M = 10⁻¹²), U10's M = 10⁻¹² included, as the orchestrator noted. It is P23's
  wording, as accepted.
- **Natural mode at M = 10⁻⁷** warns "below" and fails under the reduced budget (2×10⁵) at
  N = 42.8. The warning says "may fail", per prompt §0.2.

## 8. State handed to the next prompt

- **The records** (`ComputeTargets/ScalarHistory.py`, module level):
  - `Reflection(N, phi_Einstein, pi_Einstein_in, kind)`: `kind` is `"floor"` or `"unresolved"`. It
    is always set by the loop; it defaults to `None` only for stubs. At α = 0 it is always
    `"floor"`.
  - `Injection(N, kind, N_turn, phi_turn, pi_before, pi_after, log_rho_kin, alpha_eff, delta_K,
    delta_rho_over_rho_rad, H_Einstein_before, H_Einstein_after, nonadiabaticity, w_gate,
    resolved, phi_assessed, log_E_assessed, classical_turning_point)`:
    - `kind` is `"turning_point"`, `"floor"` or `"unresolved"`;
    - `resolved` is True only for `"turning_point"`;
    - an unresolved candidate has `w_gate == 1.0` and `alpha_eff == alpha`, its `nonadiabaticity`
      recorded;
    - `phi_assessed`, `log_E_assessed` (ln of E = ρ_kin,E + V − V_floor) and
      `classical_turning_point` (04's record: φ_t, ln ω_t², ln 𝓡, λ_t, ln k_ex, ℓ_t) are the
      assessment the candidate was judged on, or all three `None`. A reflection has its own step
      start's; a drain point has the last of its infall's.
  - `IntegrationResult(..., dropped_drains, warnings)`, warnings a tuple of lines.
- **The payload**: `"warnings"`, on success and on `integrate_history`'s failures. `"reflections"`
  counts both kinds. Nothing new is stored.
- **The reflection kinds and the order at a step start**: at α > 0, on a model with a wall, with
  π < 0: the assessment, then the unresolved rule, then the floor rule. Both go through the floor
  branch (guards G1, G2; drop of an armed drain; close of the half-oscillation; candidate; restart).
  At α = 0 nothing is assessed.
- **Where the assessment is made and what it costs**: every step start with π < 0 at α > 0; about
  11 µs for H_E and 16–22 µs for `assess_infall`, about 30 µs together; 864–6 068 per M = 0.5
  history (+4.6 % at β = 2), 3 000–31 000 per small-M history. `None` never seen.
- **How the small-M histories fail without parking** (§5.5): all after ρ_kin/ρ_R < 10⁻⁶. The cut is
  at N = 28.364, 30.349 and 29.394 for β = 1.2, 2 and 3, at every M, at candidate 6, 48 and 58. The
  failures:
  - G2 at an unresolved reflection where E ≈ V − V_floor (M = 10⁻⁷–10⁻¹²), 0.25–5.6 e-folds later,
    at T_J = 42–163 eV (β = 1.2), 32–41 keV (β = 2) and 18.0–18.4 MeV (β = 3);
  - `classical_turning_point` "not monotone" at φ ≈ 3×10⁻¹⁷ M_P (M = 10⁻³⁰), the open issue;
  - `log_m_eff_squared` on a sample with φ < 0 once.

  Σ to the cut is 0.1153, 0.2902 and 0.4219 of the ceiling. Σ after the cut adds 3×10⁻⁵, 5×10⁻⁵
  and 1×10⁻⁴ of it.
- **The constants**: `UNRESOLVED_REFLECTION_FACTOR = 3.0`, `UNRESOLVED_PHI_T_THRESHOLD_MP = 1.0e-8`
  (× `units.PlanckMass`), neither a key; the warning phrases `WARNING_NEARLY_ELASTIC`,
  `WARNING_REGIME_UNASSESSED`, `WARNING_TURNING_POINT_BELOW`, `WARNING_WALL_UNASSESSED`, in
  `WARNING_KINDS`.
- **The functions**: `assess_infall(model, phi, log_rho_kin) -> (log_E, ClassicalTurningPoint |
  None)`, `unresolved_turnaround(phi, turning_point, PlanckMass) -> bool`, `warning_kind(line) -> str`.
- **Issues**: opened `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`;
  `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` is still open for 08.
- **Tests**: `ComputeTargets/tests/test_unresolved_turnarounds.py` (35); `test_injection` and
  `test_gate_and_taper` item (8) assert w_gate = 1 at the floor.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 374 (96 + 278), ObjectFactories
  103, tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02; this
  campaign's close (12) updates them.
