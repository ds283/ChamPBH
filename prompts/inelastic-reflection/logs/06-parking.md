# Log 06 — parking

**Subject:** Park the field on its zero-lag tracker once the drain has emptied the cascade
**Commit:** `e967cb7`, on branch `inelastic-reflection`, on top of `7094056`
**Date:** 2026-10-29, by Claude Opus 5.5
**Result:** **COMPLETE.** The gate "Park" holds. No issue opened; one narrowed.
- **The cut ships, at α > 0 on a model with a wall only.** After every candidate after the
  history's first, ρ_kin,E/ρ_R,E < ε = 10⁻⁶ (strict) and a zero-lag tracker with m_eff/H_E ≥ 10
  switch the history to a **parked phase**: the leftover kinetic energy goes into the ledger (P19),
  and (ln ρ_R,E, ln f_m, ln T_J) is integrated by DOP853 at φ = φ_min, π = dφ_min/dN to the
  history's end. Each parked step is a `ParkedSegment` of the history's `OdeSolution`, returning the
  full state, so every reader reads it unchanged. A parked step end with m_eff/H_E < 10 fails the
  history (07 releases it). P21's guard fails a reflection with |π_in| < 10⁻⁹ M_P where the cut did
  not fire.
- **The gate "Park"** (README §8.4):
  - **α = 0 is bitwise.** Both golden histories, `test_first_bounce`, `test_surfing_turning_points`,
    `test_kinematic_cap_loop` and `test_unresolved_turnarounds` pass unedited; `minimum_on_trace` and
    `d_minimum_d_log_trace` are called 0 times on the golden histories, on the P1 harness, and on the
    P1 harness with two floor reflections and the cut opened wide (ruling 10). Natural mode at
    M = 0.5 M_P, β = 2 is bitwise the dispatch commit's.
  - **An M = 10⁻³⁰ drained history reaches T_CMB** (β = 2, α = 0.1; it failed at `9c2483b`). It parks
    at candidate 48 (a floor reflection), N = 30.349329, 83.6 keV, as log 05 §5.5 found, and is
    bitwise the cut-disabled history up to there.
  - **The parked samples lie on φ_min**, to the root's tolerance, and π on them is π_min bitwise.
  - **H_E is continuous at the switch**: bitwise at M = 10⁻³⁰, 2.2×10⁻¹² at M = 10⁻⁷, against a
    leftover fraction of 8.4×10⁻⁷; within the bound from V(φ_min) − V(φ) and the tracker's kinetic
    energy.
  - **The leftover is in the ledger**, a second entry at the switch's N.
  - **The guard** fails a history with its reason (with its threshold raised by a patch: no history
    reaches it before another failure, §5.4).
- **α > 0 at M = 0.5 M_P is 05's**: bitwise at β = 1.2, 2, 5, 25 with the cut disabled, and with it
  on at β = 1.2 and 2, which do not park; at β = 5 and 25 bitwise up to the switch. β = 5 and 25 park
  at a drain point (R17) and fail where the tracker loses adiabaticity.
- **Measured down to M = 10⁻³⁰** (§5.4): all 18 histories of log 05's grid park where log 05 put the
  cut and reach T_CMB, in 1.6–8 s; none of log 05's bottom-of-cascade failures occurs.
- **The tuning converges on its seed** at M = 10⁻³⁰ (β = 1.2, 2, 3) and 10⁻⁷ (β = 2): one
  integration each (§5.6).
- **Counts:** 26 / 374 (96 + 278) / 103 / 115 / 64 before; 26 / **408** (96 + **312**) / 103 / 115 /
  64 after, all passing. The 34 new tests are in `ComputeTargets/tests/test_parking.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was regenerated.

## 1. The physics and the numerics

### 1.1 The order at a candidate, and the constants

**The constants** (P18), beside 05's in `ComputeTargets/ScalarHistory.py`, none a key, each read at
call time:
- `PARKING_EPSILON = 1.0e-6` (R14);
- `PARKING_ADIABATICITY_THRESHOLD = 10.0` (R14's condition, R16's release);
- `PARKING_PI_GUARD_MP = 1.0e-9`, multiplied by `units.PlanckMass` (P21).

**The decisions** (ruling 1), module level:
- `parking_ratio_test(candidate_index: int, ratio: float) -> bool`: False at index 0, else
  `ratio < PARKING_EPSILON`;
- `tracker_adiabatic(m_eff_over_H: float) -> bool`: `m_eff_over_H >= PARKING_ADIABATICITY_THRESHOLD`
  (NaN is False);
- `parking_guard(pi_in: float, PlanckMass: float) -> bool`: `abs(pi_in) < PARKING_PI_GUARD_MP * PlanckMass`.

**The order** (ruling 2), at α > 0 on a model with a wall (05's `assess_wall`), at each drain point
and each reflection of either kind:
1. `candidate()` as before: the record, and its drain at α_eff (none if α_eff = 0.0).
2. The ratio test on the candidate's index in `injections` and exp(`log_rho_kin` − ln ρ_R,E), with
   ln ρ_R,E the state's (the drain does not change it).
3. If it passes, `cut_tracker(policy, hubble, N, y_post)` at the post-candidate state: Σ and f_m from
   one policy call; ln T = ln ρ_R,E + ln(Σ + f_m) (Σ + f_m ≤ 0 or a non-finite ln T is a
   `ComputationFailureError`); φ_root = `minimum_on_trace(φ, ln T)` from the field; the parked state
   (φ_min, π_min) = `parked_tracker(model, cosmology, φ_root, reduced)`; m_eff/H_E there with the
   ledger after the drain, before the leftover. `tracker_adiabatic` decides on that m_eff/H_E.
4. If both hold, the switch: `parking_leftover(H_Einstein_after, pi_after, N, ln ρ_R,E)` → (ΔK, Δρ/ρ_R),
   formed in logs, added to the ledger at the same N; H_E at the parked state with it; `armed` and
   `armed_assessment` cleared; the `Parking` record; the parked phase.
5. At a reflection where the cut did not fire, the guard, after steps 1–3 (so its reason gives m/H
   when step 3 ran).

At α = 0 none of this runs (P22). The cut runs whether or not the candidate drained; with
α_eff = 0.0 the leftover is all of ρ_kin,E (`pi_after` = sign·π, `H_Einstein_after` = H_before).

**φ_min in the record is the parked phase's own value** (D3). `cut_tracker` finds φ_root from the
field, then evaluates `parked_tracker` from φ_root, exactly as the first `ParkedSegment` evaluates
its left node. So the dense output at the switch's N returns the record's (φ_min, π_min) bitwise;
the two roots differ by at most the root's tolerance.

### 1.2 The tracker's velocity (prompt §1.4)

ODERHS gives, with g = 1 + (ln A)′π, D = 1 + (d ln g_s/d ln T_J)/3 (its `1 + dG_s_dlogT/G_s/3`) and
w′ = dw/d ln T_J:

    d ln ρ_R,E/dN = Σ − 4 + Σ(ln A)′π = Σg − 4,
    d ln f_m/dN  = (1 − Σ)g,
    d ln T_J/dN  = −g/D,

and Σ = 1 − 3w(T_J) gives dΣ/dN = −3w′ d ln T_J/dN = 3w′g/D. With T = ρ_R,E(Σ + f_m),

    d ln T/dN = (Σg − 4) + [3w′g/D + f_m(1 − Σ)g]/(Σ + f_m) = −4 + c g,
    c = Σ + (3w′/D + f_m(1 − Σ))/(Σ + f_m),

so d ln T/dN = a + bπ with **a = c − 4** and **b = c (ln A)′(φ_min)**: every π-dependence enters through
g. The tracker sits on the root of V′ + (ln A)′T = 0, so dφ_min/dN = s d ln T/dN with
s = `d_minimum_d_log_trace(φ_min, ln T)` = −(ln A)′T/(V″ + (ln A)″T), with no (ln A)′² term (T is held
fixed in the root; log 04 §8). The tracker's π is its own dφ_min/dN, so π = s(a + bπ), and

    π_min = s a/(1 − s b).

1 − sb = 0, or a non-finite π_min, is a `ComputationFailureError`. `tracker_velocity_terms(model,
cosmology, φ_min, reduced) -> TrackerVelocity(pi_min, a, b, s, log_T)` computes it;
`tracker_velocity` returns π_min. Nothing is differenced.

**B5 and B6 at the synthetic states** (ruling 5; item 3's tolerances, the test's derivation in its
docstring). "FD error" is |D(h) − π_min|/π_min for the five-point difference at h = 10⁻³; "tolerance"
is the test's, relative; "B5" is the relative change of π_min under B5, measured; "B6" is
|bπ_min/(a + bπ_min)|, B6's relative change of d ln T/dN:

| β | M (M_P) | T_J | φ_min (M_P) | π_min (M_P) | FD error | tolerance | B5 | B6 |
|---|---|---|---|---|---|---|---|---|
| 2 | 10⁻⁷ | 1 GeV | 1.198e-9 | 4.509e-11 | 2.1e-11 | 2.6e-9 | 2.8e-11 | 2.2e-11 |
| 2 | 10⁻⁷ | 10 MeV | 1.682e-9 | 3.025e-10 | 5.2e-8 | 3.7e-7 | 5.5e-11 | 3.9e-10 |
| 2 | 10⁻⁷ | 100 keV | 2.219e-9 | 2.635e-10 | 2.4e-9 | 1.8e-8 | 9.4e-11 | 1.5e-10 |
| 2 | 10⁻³⁰ | 1 GeV | 3.085e-32 | 2.885e-33 | 1.4e-11 | 2.7e-9 | 0 | 1.4e-33 |
| 2 | 10⁻³⁰ | 10 MeV | 9.939e-32 | 9.114e-32 | 6.1e-8 | 4.3e-7 | 0 | 1.2e-31 |
| 2 | 10⁻³⁰ | 100 keV | 1.578e-30 | 3.349e-30 | 5.5e-9 | 3.8e-8 | 0 | 1.9e-30 |
| 2 | 0.5 | 1 GeV | 5.039e-3 | 1.601e-4 | 4.1e-11 | 1.7e-9 | **1.0e-4** | 7.8e-5 |
| 2 | 0.5 | 10 MeV | 6.658e-3 | 9.563e-4 | 5.2e-8 | 3.6e-7 | **1.7e-4** | 1.2e-3 |
| 2 | 0.5 | 100 keV | 8.255e-3 | 7.377e-4 | 2.4e-9 | 1.7e-8 | **2.6e-4** | 4.2e-4 |
| 5 | 0.5 | 1 GeV | 4.974e-3 | 1.561e-4 | 4.7e-11 | 1.7e-9 | **2.4e-4** | 1.9e-4 |
| 5 | 0.5 | 10 MeV | 6.547e-3 | 9.267e-4 | 5.2e-8 | 3.7e-7 | **4.2e-4** | 2.9e-3 |
| 5 | 0.5 | 100 keV | 8.086e-3 | 7.086e-4 | 2.4e-9 | 1.7e-8 | **6.3e-4** | 1.0e-3 |

At every M = 0.5 state the tolerance is below a tenth of B5's change (the test asserts it). At small M
B5 is invisible, as orchestrator note §0 said. The note's size for it, βφ_min/(2 M_P) ≈ 6×10⁻³,
holds where V″/(−V′) ≈ 2/φ; on the exponential at M = 0.5 V″/(−V′) ≈ M/φ² ≫ 2/φ, so the change is
|(ln A)′ s| = 1–6×10⁻⁴ (Observations). The self-consistency's tolerance is 16ε(|a| + |bπ_min|),
3.6×10⁻¹⁵ relative, so B6 is seen at M = 10⁻⁷ and 0.5, not at 10⁻³⁰.

### 1.3 The parked phase and its solver (prompt §1.3; rulings 3, 4, 7, 11)

- **The reduced right-hand side** is `ParkedRHS(fun, model, cosmology, phi_anchor)`, a module-level
  class: `__call__(N, reduced) -> np.ndarray` (3,) returns `fun(N, (φ_min, π_min, reduced))[2:5]`,
  with (φ_min, π_min) = `parked_tracker(model, cosmology, phi_anchor, reduced)`. `fun` is the loop's,
  so `nfev` and the supervisor count every evaluation. `full_state(N, reduced)` returns the full
  state. The loop sets `phi_anchor` per step.
- **The solver is SciPy's DOP853**, with `params.rtol`, `params.atol` and `params.global_max_step`
  (0.1) as `max_step`. Why: the reduced system has no fast mode (φ is slaved), so an implicit method
  buys nothing and Radau's Jacobian would cost 3 more right-hand sides per step through
  `minimum_on_trace`; DOP853 is the probe's choice, its 8th order holds rtol 10⁻⁸ at the step cap,
  and it was the one SciPy interpolant the orchestrator found to give scalar and array calls the same
  floats (not relied on: ruling 3 builds it). **Its cost**: 0.51–0.86 ms per step (minimum; 198
  steps of the M = 10⁻³⁰, β = 2 history: median 0.54 ms), a parked history to T_CMB 124–257 steps,
  0.12 s; a right-hand side about 50 µs (§5.1). The step is always the cap: the parked phase is as
  finely stepped as the oscillating phase at its widest.
- **Determinism** (ruling 3). The anchor of step k is the φ_min at its left node, computed at the end
  of step k − 1 from step k − 1's anchor (the switch's φ_root for the first step). Before step k (k > 0)
  the loop sets `ParkedRHS.phi_anchor` to it and re-evaluates the solver's derivative at the left node
  (`psolver.f = psolver.fun(t, y)`), so that DOP853's first stage (FSAL, otherwise made by the step
  before with the old anchor) uses it too: every evaluation of step k, its dense-output stages
  included, and its segment, search from the same float. One extra right-hand side per step (D4).
- **The segment.** `ParkedSegment(reduced_interpolant, phi_anchor, model, cosmology)`, a
  `scipy.integrate.DenseOutput`: `state_at(N)` reads the DOP853 interpolant by a scalar call and returns
  (φ_min, π_min, ln ρ_R,E, ln f_m, ln T_J); `_call_impl` returns (5,) for a scalar and, for an array,
  a loop of `state_at` stacked into (5, n). It holds nothing a call changes. At its left node DOP853's
  interpolant returns `y_old` exactly, so at the switch's N (alt_segment=True selects the first parked
  segment) the dense output is the parked state bitwise.
- **The end**: the first parked step with ln T_J < log_T_stop, at the brentq root on its reduced
  interpolant (the oscillating phase's rule, `g_low == 0` included); N_stop; N_failsafe, a failure. The
  step budget counts both phases; `accepted_steps` is the total.
- **The check** (ruling 11): `tracker_m_eff_over_H(policy, hubble, N, y)` at every parked step end,
  on the solver's state (the next segment's left node), and at N_final on the interpolated state on
  the last step; `tracker_adiabatic` False is a failure. H_E through `policy` and `HubblePolicy`, never
  `fun`. The minimum of the switch's and every check's m/H is the record's.
- **Failures** (rulings 7, 9). The phase runs inside the loop's `try`, so its failures carry
  `warnings`. Every `ComputationFailureError` from DOP853's construction, a step, the dense output, a
  segment, the tracker or the check is re-raised as the history's failure, no halving or retry,
  `steps_rejected_by_exception` untouched. Every reason names the switch:
  - `integrate_scalar_history (<label>): in the parked phase (parked at N = …, T_J = … MeV, at candidate #k): the parked tracker lost adiabaticity: m_eff/H_E = … < 10 at N = …, T_J = … MeV`;
  - `… in the parked phase (…): <the model's or the right-hand side's message>`;
  - `… in the parked phase (…): DOP853 step failed at N = …: "<message>"`;
  - `… in the parked phase (…): cannot bracket the termination root on the last step […]`;
  - `… in the parked phase (…): the failsafe N=… was reached …`;
  - `step budget exhausted: integrate_scalar_history (<label>) took … accepted steps (budget …) at N=…, T_J=… GeV, with … reflection(s), … of the steps in the parked phase (parked at …)`.

  On §2's histories only the first was ever raised (β = 5 and 25 at M = 0.5, β = 1.2 at M = 10⁻³).
  `minimum_on_trace` and `d_minimum_d_log_trace` never raised on a history of §2.2–§2.5.
- **The guard's reason**: `integrate_scalar_history (<label>): the |pi| guard (P21): a reflection of kind <kind> at N = … with |pi_in| = … M_P < 1e-09 M_P, where the cut did not fire (rho_kin/rho_R,E = …, m_eff/H_E = <value> | not computed (the ratio test did not pass))`.

### 1.4 The switch's energy (item 5)

With the leftover in the ledger, 3M_P²H² − H²π²/2 = ρ_R(1 + f_m) + ρ_χ + V. The ledger gains
α_eff H_b²π_in²/2 + (1 − α_eff)H_a²π_in²/2, which is the candidate's ρ_kin,E but for
(1 − α_eff)π_in²(H_a² − H_b²)/2, so

    3M_P²(H_p² − H_b²) = V(φ_min) − V(φ) + H_p²π_min²/2 + (1 − α_eff)π_in²(H_a² − H_b²)/2.

The test bounds |H_p/H_b − 1| by the magnitudes over 6M_P²H_b², plus 2ε(|ln V| + |ln 3H²M_P²| + 4) for
H_E's rounding (exp of a difference of logs of order 280). Measured: 0 at M ≤ 10⁻¹⁰, 2.2×10⁻¹² at
M = 10⁻⁷ (within 0.4 % of the leading term), against leftover fractions of 4.5–8.4×10⁻⁷. Without the
leftover (B3) H moves by about half the leftover fraction (−4.6×10⁻⁷). At M = 0.5 the V term is
1.4×10⁻⁷ (β = 5), 0.15 of the leftover fraction.

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`

- **Imports**: `DenseOutput`, `DOP853` from `scipy.integrate`.
- **Constants**: `PARKING_EPSILON`, `PARKING_ADIABATICITY_THRESHOLD`, `PARKING_PI_GUARD_MP` (§1.1).
- **Records**, appended, with comments:
  - `Parking(N, log_T_Jordan, candidate_index, phi_Einstein, pi_Einstein, phi_min, pi_min,
    rho_kin_over_rho_rad, delta_K, delta_rho_over_rho_rad, m_eff_over_H, H_Einstein_before,
    H_Einstein_parked, delta_log_A, parked_steps, min_m_eff_over_H)`, floats and ints;
  - `IntegrationResult(..., warnings, parking)`, no default (ruling 6: no test builds one);
  - `TrackerVelocity(pi_min, a, b, s, log_T)` and `CutTracker(phi_root, phi_min, pi_min, log_T,
    m_eff_over_H, H_Einstein)`.
- **Module-level functions and classes**, called through the module's names:
  - `parking_ratio_test`, `tracker_adiabatic`, `parking_guard` (§1.1);
  - `_log_trace(log_rhorad_Einstein, Sigma, fm, context) -> float`;
  - `log_trace(cosmology, reduced_state) -> (ln T, Σ, f_m)`;
  - `tracker_velocity_terms(model, cosmology, phi_min, reduced_state) -> TrackerVelocity`,
    `tracker_velocity(...) -> float`;
  - `parked_tracker(model, cosmology, phi_start, reduced_state) -> (φ_min, π_min)`;
  - `tracker_m_eff_over_H(policy, hubble, N, y) -> (m_eff/H_E, H_E)`;
  - `cut_tracker(policy, hubble, N, y) -> CutTracker` (step 3);
  - `parking_leftover(H_Einstein, pi_Einstein, N, log_rhorad_Einstein) -> (ΔK, Δρ/ρ_R,E)` (step 4);
  - `ParkedRHS`, `ParkedSegment` (§1.3).
- **`integrate_scalar_history`**: the nested `cut(N, y_post, kind, pi_in)` (steps 2–5; returns `None`
  or (N_final, final_state)) and `run_parked_phase(record, phi_anchor, y_parked)`; the cut called after
  the candidate in the reflection branch and at the drain point, under `assess_wall`; `parking` in the
  result. The docstring gains a paragraph on parking.
- **`integrate_history`**: the payload gains `"parking"` (the record or `None`), not stored; the
  docstring says so.
- **Unchanged**: `ODEPolicy`, `HubblePolicy`, `ODERHS`, `log_m_eff_squared`, `candidate`, `drain`,
  `first_bounce`, `surfing_turning_points`, the fixed-T readers, `ScalarHistory.store`,
  `build_extra_data`, `TUNING_OUTPUT_KEYS`, `tune_history`, `compute_scalar_history`, the existing
  records' fields and `"reflections"`. `git diff` on the file is additive but for one import line and
  the docstrings.

### The tests (`ComputeTargets/tests/test_parking.py`, new; 34 tests, 51 s at a load of about 10)

| Test | Item | Asserts | Tolerance |
|---|---|---|---|
| `test_2_the_constants_are_the_rulings` | 2 | 1e-6, 10.0, 1e-9 as literals | exact |
| `test_2_epsilon_itself_does_not_fire_and_the_next_float_below_does` | 2 | ε: no; `nextafter(ε, 0)`: yes; 0.0 yes; NaN no | exact |
| `test_2_the_first_candidate_never_fires` | 2 | index 0 never, any ratio, ε = inf included | exact |
| `test_2_m_over_H_of_ten_fires_and_the_next_float_below_does_not` | 2 | 10: yes; `nextafter(10, 0)`: no; NaN no | exact |
| `test_9_the_guard_at_its_edge` | 9 | 10⁻⁹M_P: no; next float below: yes; both signs; M_P = 1 and 2.435e18 | exact |
| `test_2_the_decisions_read_their_constants_at_call_time` | 2 | patches change each answer | exact |
| `test_2_a_wide_open_cut_parks_at_the_second_candidate` | 2, ruling 1 | P1 harness α = 0.1 to N = 21.5, ε = inf, threshold 0: parks at index 1, at the second reflection; N_final 21.5 | exact |
| `test_2_with_the_shipped_constants_neither_candidate_parks` | 2 | same run, shipped constants: no parking | — |
| `test_9_the_guard_fails_the_second_reflection` | 9 | guard patched to 0.45 M_P: fails at the second reflection; reason's \|π_in\| = 0.4232, ratio 0.03088, N = 21.38082, "not computed" | 4–5 decimals |
| `test_9_the_guards_reason_gives_m_over_H_when_the_tracker_ran` | 9 | ε = inf, threshold inf, guard 0.45: the reason carries a finite m/H | — |
| `test_1_no_minimum_on_a_golden_history` | 1 | β = 1.5, 5.5: both spies 0, `"parking"` None | exact |
| `test_1_no_minimum_on_the_floor_harness` | 1 | P1 at α = 0: spies 0, no parking, ledger empty | exact |
| `test_1_none_with_two_reflections_and_the_cut_wide_open` | 1, ruling 10 | P1 at α = 0 to 21.5, two reflections, ε = inf, threshold 0: spies 0; reflections, nfev, final state = the unpatched run's | bitwise |
| `test_3_pi_min_against_a_finite_difference_of_phi_min` | 3, ruling 5 | 12 synthetic states (§1.2): five-point FD along the constructed trace; at M = 0.5 the tolerance < B5's change / 10 | \|D(2h) − D(h)\| + 1.5 root_tolerance φ/h |
| `test_3_the_reduced_right_hand_side_is_a_plus_b_pi_min` | 3, ruling 4 | d ln T/dN from `ParkedRHS`'s three derivatives = a + bπ_min | 16ε(\|a\| + \|bπ\|) |
| `test_3_the_parked_state_is_the_trackers` | 3 | `parked_tracker`'s π = `tracker_velocity` at its φ_min | exact |
| `test_4_it_reaches_T_CMB` | 4 | M = 10⁻³⁰, β = 2: success, ln T_J(end) = ln T_CMB, N > 48 | 9 places |
| `test_4_it_parks_where_log_05_found_the_cut` | 4 | candidate 48, a floor reflection, N = 30.349; every earlier candidate's ratio ≥ ε | 3 places in N |
| `test_4_bitwise_the_cut_disabled_history_up_to_the_switch` | 4, ruling 8 | against ε = 0 to N = 30.9: injections, reflections, turning points, ts, every step's interpolant at its ends and midpoint before the switch, the ledger's 49 entries, the oscillating steps | bitwise |
| `test_4_the_parking_record_by_construction` | 4 | every field from the candidate's record, `parking_leftover`, ln A; m/H ≥ 10; min ≤ switch's; steps = segments; payload = result; floats and ints | exact |
| `test_5_the_ledger_holds_the_leftover` | 5 | last two entries (N, ΔK_candidate), (N, ΔK_leftover); K after = K before + both; K(N_final) = K(N) | exact |
| `test_5_H_is_continuous_at_the_switch` | 5 | `H_change_and_bound` (§1.4); < 10⁻³ of the leftover fraction | derived |
| `test_5_the_dense_output_at_the_switch_is_the_parked_state` | 5 | φ_min, π_min, ln T_J bitwise; ln ρ, ln f, ln T against the last Radau step's end | exact; 64ε |
| `test_6_phi_on_every_parked_sample_is_phi_min` | 6 | 2 142 samples: `minimum_on_trace` from the sample's φ on its own trace | root_tolerance = 2(xtol + 4ε\|ln φ\|) in ln φ |
| `test_6_pi_on_every_parked_sample_is_pi_min` | 6 | π = `tracker_velocity` at the sample | exact |
| `test_3_pi_on_the_samples_against_a_finite_difference_of_phi` | 3 | central FD over the samples (h = 0.00921), skipping the stencil across 10 keV | 2·h²/6·max\|π″\| (5 samples) + root_tolerance φ/h + 8ε\|π\| |
| `test_6_a_parked_segment_is_deterministic` | 6, ruling 3 | 3 segments × 7 points: forward, backward, shuffled scalar and array calls; shapes (5,), (5, n) | bitwise |
| `test_6_the_readers_read_the_parked_phase` | 6 | `first_bounce` before the switch; 1 MeV before and 70 keV after it; φ at 70 keV on φ_min | root_tolerance |
| `test_8_N_stop_ends_it_successfully_in_the_parked_phase` | 8 | M = 10⁻⁷ to N = 30.9: parked (#48, unresolved), N_final = ts[−1] = 30.9, last segment parked, final φ on φ_min | root_tolerance |
| `test_5_H_is_continuous_at_the_switch_here_too` | 5 | as above at M = 10⁻⁷ (2.2×10⁻¹²) | derived |
| `test_7_no_parking_at_beta_2_and_the_history_is_the_cut_disabled_one` | 7 | M = 0.5, β = 2: success, no parking; turning points, injections, final state, N_final, nfev, steps, samples, ledger = ε = 0's | bitwise |
| `test_7_at_beta_2_a_candidate_passes_the_ratio_test_but_not_the_tracker` | 7 | candidate 27 passes the ratio test (and the history does not park) | — |
| `test_7_at_beta_5_it_parks_at_a_drain_point` | 7 | window to N = 40.0: parked at a `"turning_point"` candidate with m/H ≥ 10 | — |
| `test_7_at_beta_5_it_fails_where_the_tracker_loses_adiabaticity` | 7, ruling 9 | the failure's kind, switch N and candidate, m/H < 10, N_fail in (40.2537, 40.2557 + 0.1]; one warning | window derived in §5.2 |

Helpers in the module: `no_cut`, `wide_open_cut`, `minimum_spy` (spies on `AbstractModel`'s two
methods, `autospec`), `history` (`integrate_history` with the result captured, failures returned),
`floor_harness` (P1 at M = 10⁻¹⁰ to any N), `root_tolerance`, `H_change_and_bound`,
`synthetic_setup`, `synthetic_reduced_state`. It imports `test_kinematic_cap_loop`,
`golden_history`, `golden_tuned_history`, `test_injection` and `test_unresolved_turnarounds`
(`window`), none edited.

### Edits to existing tests (prompt §1.7 item 10; ruling 14)

1. **`test_injection.py`, `TestTwoTurningPointsOnOneStep.setUpClass`**: `integrate_captured(10.0, 0.5,
   ALPHA)` wrapped in `mock.patch.object(SM, "PARKING_EPSILON", 0.0)`; the class docstring gains one
   sentence. Reason: the cut ends the β = 10 oscillation at 358 MeV, before the 36 MeV near-stall the
   test needs.
2. **`test_gate_and_taper.py`, `TestTheNearStallIsGated.setUpClass`**: the same wrap of
   `ti.integrate_captured(5.0, 0.5, ALPHA)`; one docstring sentence. Reason: the β = 5 history parks at
   81 MeV and then fails at 9.40 MeV.

No other existing test was edited.

### The documents (P12)

- `.documents/architecture-summary.md`: a dated note after 05's in §7.2: the constants, functions,
  classes, `Parking`, `IntegrationResult.parking`, the payload key, the guard, and the behaviour at
  large M (R17) with the M = 0.5 and 10⁻³ histories that park.
- `.documents/numerical-strategies.md`: new §6.5, dated: the cut, the switch, π_min, the reduced
  system and its dense output, the end and the checks, the 10 keV step of π_min, large M, and what
  was measured.

## 3. Deviations from the prompt

**The rulings of orchestrator note 06 (1–17):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | Three module-level decisions reading their constants at call time; synthetic edge tests; the loop-level test (P1 harness to N = 21.5, ε = inf, threshold 0) parks at index 1; B1 fails both | IMPLEMENTATION CHOICE (ruled) |
| R2 | The order of §1.1; the leftover from the candidate's `H_Einstein_after` and `pi_after`; the cut whether or not it drained | IMPLEMENTATION CHOICE (ruled) |
| R3 | Each segment's start fixed (the left node's φ_min, carried from the previous step's end); the left-node derivative re-evaluated per step; arrays a loop of scalar calls; asserted bitwise at nodes and interiors | STRUCTURALLY REQUIRED |
| R4 | `ParkedRHS` module level, used by the loop and by item 3's self-consistency; B6 applied inside it fails item 3 | STRUCTURALLY REQUIRED |
| R5 | Synthetic states at M = 0.5, β = 2 and 5, with B5's change and the tolerance tabulated (§1.2); the test asserts tolerance < B5's change/10 | IMPLEMENTATION CHOICE (ruled) |
| R6 | `Parking`, floats and ints, appended last to `IntegrationResult` with no default; `"parking"` the record or `None`; nothing callable | IMPLEMENTATION CHOICE (ruled) |
| R7 | The parked phase inside the loop's `try`; every failure the history's, no retry; none but the adiabaticity failure raised on §2's histories | IMPLEMENTATION CHOICE (ruled) |
| R8 | The cut-disabled references are windows at ε = 0.0: item 4's to N = 30.9; §2.5's to 29.5, 30.9, 29.6 (33.95 for ε = 10⁻⁸, D7). The guard never fired in one. Item 9 raises the threshold by a patch | IMPLEMENTATION CHOICE (ruled) |
| R9 | Every reason in the parked phase names the switch's N, T_J and candidate; item 7 asserts the kind, the switch at a drain point (from a window to N = 40.0) and N_fail in a derived window | IMPLEMENTATION CHOICE (ruled) |
| R10 | B10 written concretely (the α > 0 condition removed, the ratio from the reflection's own H_E at α = 0); item 1 adds the α = 0 run with two reflections and the cut wide open, because neither golden history nor the P1 harness to N = 21 has two reflections; B10 makes the spy count 2 | STRUCTURALLY REQUIRED |
| R11 | The check at every parked step end, at N_final on the interpolated state on the last; H_E via `policy` | IMPLEMENTATION CHOICE (ruled) |
| R12 | (V − V_floor)/ρ_kin,E at the cut candidate in §5.3–§5.5; largest 41 (M = 0.5, β = 5); at M ≤ 10⁻⁷ at most 7.0×10⁻⁷ | IMPLEMENTATION CHOICE (ruled) |
| R13 | "The oscillation it replaces" is the dense output from the previous candidate's N to the switch's N at 4 000 points | IMPLEMENTATION CHOICE (ruled) |
| R14 | Each setup wrapped in a patch of ε to 0.0, one docstring sentence each | IMPLEMENTATION CHOICE (ruled) |
| R15 | One worktree of `7094056` (and one of the final tree, D8), removed afterwards; whole histories at ε = 0 and windows up to the switch | IMPLEMENTATION CHOICE (ruled) |
| R16 | Only the prompt's §6 files change, plus `.documents/OPEN_ISSUES.md`. `CosmologyConcepts/Models/`, `Policies.py`, `dark_radiation.py`, `exceptions.py`, the golden modules and JSON, `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`, `test_unresolved_turnarounds.py`, `test_wall_model.py`, the tools and drivers are byte-identical | as ruled |
| R17 | Costs are minima of 1 000–2 000 calls (the step: all 198 of one history) with `uptime`; suites serial in halves, to scratch, in the background; §5.4's 22 histories four at a time under `timeout 900` (none reached it) | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | The names of §2; the prompt asked me to choose | IMPLEMENTATION CHOICE |
| D2 | Two helper records beside `Parking`, `TrackerVelocity` and `CutTracker`, and `log_trace`, `parked_tracker`, `tracker_m_eff_over_H` as separate functions, so that the tests, the loop and the segment share one expression each | IMPLEMENTATION CHOICE |
| D3 | The record's (φ_min, π_min) is `parked_tracker` from the cut's root, the parked phase's own value at the switch, so the dense output returns it bitwise; the decision's m/H is at that state. It differs from the root from the field by at most the root's tolerance | IMPLEMENTATION CHOICE |
| D4 | One extra right-hand side per parked step (the left-node derivative with the step's anchor), for ruling 3's "every reduced right-hand-side evaluation of the step" | STRUCTURALLY REQUIRED |
| D5 | The test of π on the samples skips the stencils across T_J = 10 keV, where π_min steps (Observations); 1 sample of 2 142 at M = 10⁻³⁰ | STRUCTURALLY REQUIRED |
| D6 | Tests beyond the prompt's list: the literals, the guard's edge and its m/H variant, the call-time reads, the shipped constants on the harness, `test_3_the_parked_state_is_the_trackers`, item 5 at a second M | IMPLEMENTATION CHOICE |
| D7 | §2.5's ε = 10⁻⁸ parks at N = 33.78, after the 29.5 window; its window runs to 33.95, before the oscillating history's failure at 33.986 | STRUCTURALLY REQUIRED |
| D8 | The measurements ran in a scratch worktree holding a copy of the final `ScalarHistory.py` (md5 `0f25d804bdfdaf194b9bdf324c599b0e`, the committed file's), while the breakages were replayed in the main checkout; both worktrees removed afterwards | IMPLEMENTATION CHOICE |
| D9 | Three tests read the run's outcome before its records (`assertFalse(data.get("failure"))`, the spies before a run's exception), so that B8, B10 and B11 fail them as failures, not errors | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT.

## 4. Verification performed

### 4.1 The suites

Serially, ComputeTargets in two halves, output in my scratch directory, "before" at `7094056` before
any edit, "after" on the final tree.

| Suite | Before (count, time, 1-min load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 75 s, 8.1 → 9.1 | 26 OK, 71 s, 6.2 → 7.0 |
| ComputeTargets [a-f] | 96 OK, 131 s, 9.1 → 9.7 | 96 OK, 127 s, 7.0 → 7.5 |
| ComputeTargets [g-z] | 278 OK, 333 s, 9.7 → 7.3 | **312** OK, 384 s, 7.5 → 22.0 |
| ObjectFactories | 103 OK, 18 s, 7.3 → 6.3 | 103 OK, 25 s, 22.0 → 29.2 |
| tools | 115 OK, 135 s, 6.3 → 10.5 | 115 OK, 140 s, 29.2 → 7.2 |
| RunDrivers | 64 OK, 30 s, 10.5 → 12.6 | 64 OK, 38 s, 7.2 → 8.6 |

- The baselines are the dispatch's: 26 / 374 (96 + 278) / 103 / 115 / 64.
- ComputeTargets rose by 34, the new module, which sorts into [g-z]. Nothing fell.
- **The golden tests passed unchanged**: `test_golden_history`, the tuned golden test,
  `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop` and
  `test_unresolved_turnarounds`.
- `black --check` on the four changed Python files: all unchanged.

## 5. The measurement (prompt §2)

Every history: test_injection's setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, default
tolerances, the seed realization, untuned), step budget 2×10⁵ (patched `SM.StepControl`), α = 0.1
unless stated. The scripts (`common.py`, `grid.py`, `grid_all.sh`, `m_half.py`, `compare_mhalf.py`,
`compare.py`, `tune.py`, `cost.py`, `cost_hist.py`) are in my scratch directory, `agent06/meas/`.

### 5.1 The cost (prompt §2.1)

Minima (means) of 2 000 calls (`cut_tracker` 1 000) at the switch's state, load 12–15:

| History (switch) | ratio test | `cut_tracker` | `ParkedRHS` | ODERHS alone | `minimum_on_trace` from the root | `ParkedSegment` scalar call | DOP853 step (min / median, over n) |
|---|---|---|---|---|---|---|---|
| β = 2, M = 10⁻³⁰ (#48) | 0.13 µs (0.19) | 78.0 µs (87.2) | 47.2 µs (49.9) | 20.0 (21.5) | 8.3 (8.6) | 48.0 (53.6) | 0.51 / 0.54 ms, 198 |
| β = 5, M = 0.5 (#105) | 0.13 (0.19) | 74.3 (79.1) | 50.4 (62.8) | 20.7 (21.7) | 8.2 (8.4) | 48.8 (53.2) | 0.86 / 0.89 ms, 11 |
| β = 2, M = 10⁻⁷ (#48) | 0.13 (0.19) | 87.9 (96.0) | 57.5 (61.9) | 20.2 (20.7) | 17.7 (19.6) | 49.5 (53.6) | 0.86 / 0.87 ms, 6 |

The ratio test runs at every candidate after the first; the tracker only where it passes (once per
history, or a handful). The parked phase of the M = 10⁻³⁰ history to T_CMB took 0.12 s of steps.

**Per history**, M = 0.5, β = 2, α = 0.1 (`integrate_history`, sampling included), the dispatch
commit and the final tree back to back, three rounds, load 8.6–10.9:

| | round 1 | round 2 | round 3 | minimum |
|---|---|---|---|---|
| `7094056` | 2.700 s | 2.663 s | 2.665 s | 2.663 s |
| final | 2.686 s | 2.653 s | 2.696 s | 2.653 s |

No measurable cost: one tracker call (candidate 27) and 27 ratio tests.

### 5.2 M = 0.5 M_P (prompt §2.2, §2.7)

The dispatch commit `7094056` and the final tree, each in a scratch worktree, compared after pickling:

| β | ε = 0 whole vs dispatch (records, samples, final state, N_final, nfev, steps, ledger, ts) | cut on | parks | where the tracker falls below m/H = 10 |
|---|---|---|---|---|
| 1.2 | bitwise | bitwise the dispatch's whole history | no (7 candidates) | — |
| 2 | bitwise | bitwise the dispatch's whole history | no (28; #27 passes the ratio test, m/H = 3.1) | — |
| 5 | bitwise | bitwise up to the switch (window to N = 39.0: 106 injections, turning points, ts and every step's interpolant at 6 583 steps, ledger) | **#105, drain point, N = 38.00441, 81.20 MeV, m/H = 122.7**; ρ_kin/ρ_R = 9.47e-7, leftover 9.47e-7, ΔH/H = 1.43e-7, Δ ln A = −3.6e-6 against ln A's range 1.3e-4; (V − V_floor)/ρ_kin = 40.6 | fails at N = 40.29089, T_J = 9.396 MeV (m/H = 8.94), 27 parked steps |
| 25 | bitwise | bitwise up to the switch (window to N = 140.0: 84 injections, 6 644 steps) | **#83, drain point, N = 132.55077, 9.972 GeV, m/H = 78.0**; ratio 8.96e-7, leftover 8.93e-7, ΔH/H = 1.35e-7, Δ ln A = −5.3e-5 against 1.05e-3; W/E = 13.9 | fails at N = 145.78944, T_J = 39.80 keV (m/H = 9.19) |
| 2, α = 0 | bitwise (natural mode) | — | — | — |

**Item 7's window at β = 5.** On the oscillating history (ε = 0), the tracker evaluated at its trace
crosses m/H = 10 at N = 40.25469 (9.745 MeV); on the parked history at N = 40.25454, inside the step
[40.19089, 40.29089], whose end is the failure. The test's window is (40.2547 − 10⁻³, 40.2547 + 10⁻³ +
0.1]: the two crossings differ by 1.6×10⁻⁴, and the check lags a crossing by at most one step,
global_max_step = 0.1. The probe's 40.26 was the oscillating crossing to two decimals.

### 5.3 M = 10⁻³ M_P (prompt §2.3)

| β | outcome | parks | m/H at switch / smallest | Σ (/ ceiling), leftover | Δ ln A / ln A range | ΔH/H | W/E | steps before + after |
|---|---|---|---|---|---|---|---|---|
| 0.5 | T_CMB (N = 42.548) | never: 13 drain points, smallest ratio 0.33 | — | 1.2056 (0.603) | — | — | — | 2 944 |
| 1.2 | **fails**: tracker loses adiabaticity at N = 45.95242, T_J = 0.255 eV (m/H = 9.36) | #6, drain point, N = 28.37191, 11.47 keV | 19.6 / 19.6 at the switch | — , 4.45e-7 | 1.4e-6 / — | −1.4e-10 | — | — |
| 2 | T_CMB (N = 50.017) | #48, drain point, N = 30.37400, 81.84 keV | 829 / 12.6 | 0.012615020 (0.2901), 7.82e-7 | −3.5e-8 / 2.4e-5 | 6.8e-10 | 1.1 | 6 322 + 197 |
| 3 | T_CMB (N = 55.006) | #58, drain point, N = 29.40828, 23.02 MeV | 1 530 / 17.6 | 0.007959550 (0.4219), 8.12e-7 | −1.9e-7 / 2.3e-5 | 3.5e-9 | 1.1 | 7 763 + 257 |

The planner's `parked_tracker.out` put the loss of adiabaticity at 31 keV at β = 0.5 and near today
at β = 1.2. At β = 0.5 the history never parks (the drain does not empty its cascade: Σ is 0.60 of
the ceiling 1/(6β² − 1) = 2); at β = 1.2 it fails at 0.25 eV. Both are for 07.

### 5.4 Down to laboratory M (prompt §2.4)

All 18 histories reach T_CMB. "Σ" is over the candidates; the ceiling is 1/(6β² − 1); "W/E" is
(V − V_floor)/ρ_kin,E at the cut candidate (ruling 12); "ln A range" is over the oscillation from the
previous candidate (ruling 13); "m/H" at the switch and its smallest over the parked phase.

| β | M | parks at (candidate, kind, N, T_J) | ρ_kin/ρ_R | W/E | Σ (/ ceil) | leftover | Σ + leftover (/ ceil) | Δ ln A / range | ΔH/H | m/H / min | steps | wall |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻⁷ | #6 unresolved, 28.36389, 11.56 keV | 5.006e-7 | 8.8e-14 | 0.015092154 (0.11530) | 4.506e-7 | 0.015092605 (0.11531) | −2.9e-9 / 7.45e-3 | 0 | 1.52e3 / 72.3 | 1 580 + 178 | 1.6 s |
| 1.2 | 10⁻⁸ | #6 unresolved, 28.36426, 11.56 keV | 5.002e-7 | 4.8e-14 | 0.015092155 (0.11530) | 4.502e-7 | 0.015092606 (0.11531) | −2.8e-10 / 7.45e-3 | 0 | 4.47e3 / 128 | 1 735 + 178 | 1.7 s |
| 1.2 | 10⁻⁹ | #6 unresolved, 28.36405, 11.56 keV | 5.004e-7 | 3.7e-14 | 0.015092155 (0.11530) | 4.504e-7 | 0.015092605 (0.11531) | −2.5e-11 / 7.45e-3 | 0 | 1.31e4 / 228 | 1 894 + 178 | 1.8 s |
| 1.2 | 10⁻¹⁰ | #6 unresolved, 28.36407, 11.56 keV | 5.004e-7 | 6.8e-14 | 0.015092155 (0.11530) | 4.504e-7 | 0.015092605 (0.11531) | −1.8e-12 / 7.45e-3 | 0 | 3.80e4 / 406 | 1 978 + 178 | 1.8 s |
| 1.2 | 10⁻¹² | #6 floor, 28.36418, 11.56 keV | 5.003e-7 | 1.2e-18 | 0.015092155 (0.11530) | 4.503e-7 | 0.015092605 (0.11531) | −1.3e-13 / 7.45e-3 | 0 | 3.14e5 / 1.28e3 | 2 012 + 178 | 1.8 s |
| 1.2 | 10⁻³⁰ | #6 floor, 28.36406, 11.56 keV | 5.004e-7 | 0 | 0.015092155 (0.11530) | 4.504e-7 | 0.015092605 (0.11531) | −2.0e-13 / 7.45e-3 | 0 | 3.54e11 / 4.06e7 | 2 018 + 178 | 1.8 s |
| 2 | 10⁻⁷ | #48 unresolved, 30.34933, 83.63 keV | 9.324e-7 | 6.0e-17 | 0.012616068 (0.29017) | 8.391e-7 | 0.012616907 (0.29019) | −6.1e-9 / 2.16e-5 | 2.2e-12 | 7.17e4 / 106 | 5 969 + 198 | 4.3 s |
| 2 | 10⁻⁸ | #48 unresolved, 30.34933, 83.63 keV | 9.324e-7 | 1.4e-16 | 0.012616068 (0.29017) | 8.392e-7 | 0.012616907 (0.29019) | −5.4e-10 / 2.16e-5 | 2.6e-13 | 2.16e5 / 188 | 7 059 + 198 | 5.0 s |
| 2 | 10⁻⁹ | #48 unresolved, 30.34933, 83.63 keV | 9.324e-7 | 1.3e-16 | 0.012616068 (0.29017) | 8.392e-7 | 0.012616907 (0.29019) | −5.2e-11 / 2.16e-5 | 2.8e-14 | 6.47e5 / 335 | 8 146 + 198 | 6.1 s |
| 2 | 10⁻¹⁰ | #48 unresolved, 30.34933, 83.63 keV | 9.324e-7 | 2.7e-16 | 0.012616068 (0.29017) | 8.392e-7 | 0.012616907 (0.29019) | −4.5e-12 / 2.16e-5 | 0 | 1.94e6 / 596 | 9 102 + 198 | 6.8 s |
| 2 | 10⁻¹² | #48 floor, 30.34933, 83.63 keV | 9.324e-7 | 4.0e-23 | 0.012616068 (0.29017) | 8.392e-7 | 0.012616907 (0.29019) | −3.8e-13 / 2.16e-5 | 0 | 1.71e7 / 1.88e3 | 9 731 + 198 | 7.1 s |
| 2 | 10⁻³⁰ | #48 floor, 30.34933, 83.63 keV | 9.324e-7 | 0 | 0.012616068 (0.29017) | 8.392e-7 | 0.012616907 (0.29019) | −4.4e-13 / 2.16e-5 | 0 | 4.80e14 / 5.96e7 | 9 731 + 198 | 7.1 s |
| 3 | 10⁻⁷ | #58 unresolved, 29.39425, 23.32 MeV | 9.366e-7 | 9.4e-24 | 0.007961268 (0.42193) | 8.429e-7 | 0.007962111 (0.42197) | −6.8e-9 / 2.07e-5 | 1.5e-12 | 1.37e5 / 144 | 7 058 + 257 | 5.2 s |
| 3 | 10⁻⁸ | #58 unresolved, 29.39426, 23.32 MeV | 9.366e-7 | 2.8e-23 | 0.007961268 (0.42193) | 8.429e-7 | 0.007962111 (0.42197) | −6.2e-10 / 2.07e-5 | 1.7e-13 | 4.19e5 / 256 | 8 371 + 257 | 6.1 s |
| 3 | 10⁻⁹ | #58 unresolved, 29.39426, 23.32 MeV | 9.366e-7 | 3.0e-23 | 0.007961267 (0.42193) | 8.429e-7 | 0.007962110 (0.42197) | −6.0e-11 / 2.07e-5 | 1.4e-14 | 1.28e6 / 454 | 9 690 + 257 | 6.9 s |
| 3 | 10⁻¹⁰ | #58 unresolved, 29.39426, 23.32 MeV | 9.366e-7 | 7.1e-23 | 0.007961268 (0.42193) | 8.429e-7 | 0.007962111 (0.42197) | −5.5e-12 / 2.07e-5 | 0 | 3.91e6 / 807 | 10 757 + 257 | 7.7 s |
| 3 | 10⁻¹² | #58 floor, 29.39426, 23.32 MeV | 9.366e-7 | 1.7e-33 | 0.007961267 (0.42193) | 8.429e-7 | 0.007962110 (0.42197) | −6.5e-13 / 2.07e-5 | 0 | 3.62e7 / 2.55e3 | 11 301 + 257 | 8.0 s |
| 3 | 10⁻³⁰ | #58 floor, 29.39426, 23.32 MeV | 9.366e-7 | 0 | 0.007961267 (0.42193) | 8.429e-7 | 0.007962110 (0.42197) | −7.0e-13 / 2.07e-5 | 0 | 1.14e16 / 8.07e7 | 11 301 + 257 | 8.0 s |

- **None of log 05 §5.5's bottom-of-cascade failures occurs**: no G2, no "not monotone", no φ < 0
  sample. The cut comes first on every history, so the issue
  `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` is narrowed (§7).
- **The candidate and N are log 05's** at every M (6, 48, 58; 28.364, 30.349, 29.394).
- **Σ + leftover** is independent of M to 5×10⁻⁸, 2×10⁻⁸ and 1.3×10⁻⁷ relative at β = 1.2, 2 and 3. The leftover adds 3.0×10⁻⁵,
  6.7×10⁻⁵ and 1.06×10⁻⁴ of Σ, as log 05 §8 found for the cascade after the cut.
- **Δ ln A** is at most 6.8×10⁻⁹, 4 to 9 orders below the range of ln A it replaces. **ΔH/H** is at
  most 2.2×10⁻¹², against leftovers of 4.5–8.4×10⁻⁷.
- **The tracker stays adiabatic**: smallest m/H 72 (β = 1.2, M = 10⁻⁷), at the end of the history.
- **The wall time** is 1.6–8.0 s per history (load about 10), against log 05's 2–21 s to failure.

### 5.5 The parked against the oscillating history (prompt §2.5)

Over [N_switch, the window's end] at 401 points; "max | end" of parked − oscillating; H_E and H_J
relative to the oscillating H_E (H_J passes through zero at a bounce):

| β | M | window | parks at | ln T_J | ln ρ_R,E | H_E | H_J |
|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻⁷ | 29.5 | #6 unresolved, 28.36389 | 1.12e-3 \| 1.12e-3 | 4.6e-12 \| −9.7e-13 | 1.3e-7 \| 1.0e-7 | 2.0e-3 \| 9.0e-4 |
| 1.2 | 10⁻³⁰ | 29.5 | #6 floor, 28.36406 | 1.12e-3 \| 1.12e-3 | 1.8e-11 \| −1.6e-11 | 1.3e-7 \| 1.0e-7 | 2.0e-3 \| 9.0e-4 |
| 2 | 10⁻⁷ | 30.9 | #48 unresolved, 30.34933 | 1.71e-5 \| 6.4e-8 | 8.2e-7 \| 9.0e-10 | 1.3e-7 \| 1.3e-7 | 4.5e-3 \| −9.2e-5 |
| 2 | 10⁻³⁰ | 30.9 | #48 floor, 30.34933 | 1.71e-5 \| 4.9e-8 | 8.2e-7 \| 9.5e-10 | 1.3e-7 \| 1.3e-7 | 4.5e-3 \| −9.7e-5 |
| 3 | 10⁻⁷ | 29.6 | #58 unresolved, 29.39425 | 1.73e-5 \| 1.7e-7 | 8.3e-7 \| −5.2e-9 | 6.4e-8 \| 6.4e-8 | 6.7e-3 \| −6.7e-4 |
| 3 | 10⁻³⁰ | 29.6 | #58 floor, 29.39426 | 1.73e-5 \| 1.6e-7 | 8.3e-7 \| −5.0e-9 | 6.4e-8 \| 6.4e-8 | 6.7e-3 \| −6.7e-4 |

**β = 1.2, M = 10⁻⁷ at three ε:**

| ε | parks at | ρ_kin/ρ_R | W/E | m/H | window | ln T_J | ln ρ_R,E | H_E | H_J |
|---|---|---|---|---|---|---|---|---|---|
| 10⁻⁴ | #6 unresolved, N = 28.36389, 11.56 keV | 5.0e-7 | 8.8e-14 | 1.52e3 | 29.5 | 1.12e-3 \| 1.12e-3 | 4.6e-12 \| −9.7e-13 | 1.3e-7 \| 1.0e-7 | 2.0e-3 \| 9.0e-4 |
| 10⁻⁶ | #6 unresolved, N = 28.36389, 11.56 keV | 5.0e-7 | 8.8e-14 | 1.52e3 | 29.5 | as above | | | |
| 10⁻⁸ | #44 unresolved, N = 33.78052, 51.3 eV | 9.1e-9 | 7.0e-7 | 1.16e4 | 33.95 | 5.2e-7 \| −1.5e-10 | 0 \| 0 | 1.0e-11 \| 4.9e-12 | 2.6e-4 \| −1.1e-6 |

At β = 1.2 candidate 6 is the first below 10⁻⁴ (its predecessor's ratio is above it), so ε = 10⁻⁴ and
10⁻⁶ park at the same place. The ln T_J offset of 1.1×10⁻³ is the width of the oscillation cut
(log 05 §5.5, orchestrator note §0.2): at ε = 10⁻⁸ the switch comes 5.4 e-folds later and the
offset is 5×10⁻⁷. The full history at ε = 10⁻⁸ reaches T_CMB with Σ + leftover = 0.015092604 (the
same to 2×10⁻⁸). This is an input to 11's ε (U10).

### 5.6 The tuning (prompt §2.6)

`tune_history` at α = 0.1, `main.py`'s defaults otherwise (tolerance 10⁻⁵, 30 integrations, the
default tolerances), `progress="every"`:

| β | M | converged | integrations | residual_H, residual_phi | parked (candidate, N, T_J, m/H, smallest) | warnings | wall |
|---|---|---|---|---|---|---|---|
| 2 | 10⁻³⁰ | yes, on the seed | 1 | 4.49e-7, 5.24e-7 | #48, 30.349329, 83.6 keV, 4.8e14, 6.0e7 | none | 6.6 s |
| 2 | 10⁻⁷ | yes, on the seed | 1 | 1.34e-8, 6.65e-7 | #48, 30.349328, 83.6 keV, 7.2e4, 106 | "nearly elastic" (R = 10^−31.763) | 4.3 s |
| 1.2 | 10⁻³⁰ | yes, on the seed | 1 | 3.51e-7, −5.10e-8 | #6, 28.364056, 11.6 keV, 3.5e11, 4.1e7 | none | 1.8 s |
| 3 | 10⁻³⁰ | yes, on the seed | 1 | 2.47e-7, 2.85e-7 | #58, 29.394255, 23.3 MeV, 1.1e16, 8.1e7 | none | 7.9 s |

At M ≤ 10⁻⁷ the parked field sits at φ_min with M/φ_min ≫ 1 and V ≈ V₀ today, so the seed's
normalization already meets both conditions.

### 5.7 Natural mode (prompt §2.7)

M = 0.5 M_P, β = 2, α = 0: bitwise the dispatch commit's (§5.2's last row): records, samples, final
state, N_final, nfev and steps.

## 6. The deliberate-breakage record (P11)

- **The replay.** `make_breakages.py` wrote each diff against the final files (difflib);
  `replay.sh`, run in bash, checked each with `git apply --check`, applied it, checked it with
  `git apply -R --check`, ran `test_parking` (34 tests), reverted it, and compared both files' md5 with
  the final ones. All were restored. Both scripts are in `agent06/breakages/`; none is committed.
  B8, B10 and B11 were replayed again after D9, and B5 after item 3's estimate of B5's size was
  corrected to |(ln A)′ s| (Observations); each failed as recorded.

**B1 — the cut tested at the history's first candidate.** Fails
`test_2_the_first_candidate_never_fires` and `test_2_a_wide_open_cut_parks_at_the_second_candidate`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1047,8 +1047,6 @@
     rho_kin,E/rho_R,E < PARKING_EPSILON, strictly. The ratio is the candidate's, rho_kin,E from
     before its drain. PARKING_EPSILON is read here, at call time (orchestrator note 06, ruling 1).
     """
-    if candidate_index == 0:
-        return False
     return ratio < PARKING_EPSILON
```

**B2 — ≤ for ε.** Fails `test_2_epsilon_itself_does_not_fire_and_the_next_float_below_does`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1049,7 +1049,7 @@
     """
     if candidate_index == 0:
         return False
-    return ratio < PARKING_EPSILON
+    return ratio <= PARKING_EPSILON
```

**B3 — the leftover not added to the ledger.** Fails `test_5_the_ledger_holds_the_leftover`,
`test_5_H_is_continuous_at_the_switch` and `test_5_H_is_continuous_at_the_switch_here_too`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2187,8 +2187,6 @@
                 delta_K, delta_rho_over_rho_rad = parking_leftover(
                     record_.H_Einstein_after, record_.pi_after, N_, float(y_post[2])
                 )
-                ledger.add(N_, delta_K)
-
                 y_parked = np.array(
```

**B4 — π_min = 0 in the parked state.** Fails 5: `test_3_pi_min_against_a_finite_difference_of_phi_min`,
`test_3_the_reduced_right_hand_side_is_a_plus_b_pi_min`, `test_3_the_parked_state_is_the_trackers`,
`test_3_pi_on_the_samples_against_a_finite_difference_of_phi`, `test_6_pi_on_every_parked_sample_is_pi_min`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1181,7 +1181,7 @@
     """
     log_T, _, _ = log_trace(cosmology, reduced_state)
     phi_min: float = model.minimum_on_trace(phi_start, log_T)
-    return phi_min, tracker_velocity(model, cosmology, phi_min, reduced_state)
+    return phi_min, 0.0
```

**B5 — `d_minimum_d_log_trace`'s denominator given the (ln A)′² term** (as (ln A)′ in the reduced
form). Fails `test_3_pi_min_against_a_finite_difference_of_phi_min` (at the M = 0.5 states).
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -778,7 +778,7 @@
                 )
         except OverflowError as e:
             raise _failure(f"{context}: V''/((ln A)' T) overflows") from e
-        denominator = ratio + d2_A / d1_A
+        denominator = ratio + d2_A / d1_A + d1_A
         if not (denominator > 0.0 and isfinite(denominator)):
```

**B6 — π_min left out of the right-hand side's evaluation, kept on the samples.** Fails
`test_3_the_reduced_right_hand_side_is_a_plus_b_pi_min` and
`test_3_pi_min_against_a_finite_difference_of_phi_min`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1291,6 +1291,7 @@
 
     def __call__(self, N, reduced_state) -> np.ndarray:
         y = self.full_state(float(N), reduced_state)
+        y[1] = 0.0
         derivatives = self.fun(N, y)
```

**B7 — `minimum_on_trace` started from the last root found.** Fails
`test_6_a_parked_segment_is_deterministic`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1321,6 +1321,7 @@
         phi_min, pi_min = parked_tracker(
             self.model, self.cosmology, self.phi_anchor, reduced
         )
+        self.phi_anchor = phi_min
         return np.array(
```

**B8 — the adiabaticity condition dropped from the cut.** Fails
`test_7_no_parking_at_beta_2_and_the_history_is_the_cut_disabled_one` and
`test_7_at_beta_2_a_candidate_passes_the_ratio_test_but_not_the_tracker` (β = 2 parks at #27 with
m/H = 3.1 and fails), and `test_9_the_guards_reason_gives_m_over_H_when_the_tracker_ran` (it parks).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2183,7 +2183,7 @@
         tracker: Optional[CutTracker] = None
         if parking_ratio_test(index, ratio):
             tracker = cut_tracker(policy, hubble, N_, y_post)
-            if tracker_adiabatic(tracker.m_eff_over_H):
+            if True:
```

**B9 — the guard removed.** Fails `test_9_the_guard_fails_the_second_reflection` and
`test_9_the_guards_reason_gives_m_over_H_when_the_tracker_ran`.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2223,7 +2223,7 @@
                 )
                 return N_end, final_state_
 
-        if pi_in is not None and parking_guard(pi_in, Mp):
+        if False:
```

**B10 — the cut tested at α = 0** (ruling 10). Fails `test_1_none_with_two_reflections_and_the_cut_wide_open`
(the spy counts 2 calls).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2176,9 +2176,16 @@
           - at a reflection (pi_in given) where the cut did not fire, the guard (P21).
         """
         nonlocal parking, armed, armed_assessment
-        index: int = len(injections) - 1
-        record_: Injection = injections[index]
-        ratio: float = exp(record_.log_rho_kin - float(y_post[2]))
+        if alpha == 0.0:
+            index: int = len(reflections) - 1
+            record_ = None
+            H_ = H_Einstein_at(N_, y_post)
+            log_rho_kin_ = 2.0 * log(H_) + 2.0 * log(abs(float(y_post[1]))) - LOG_2
+            ratio: float = exp(log_rho_kin_ - float(y_post[2]))
+        else:
+            index = len(injections) - 1
+            record_ = injections[index]
+            ratio = exp(record_.log_rho_kin - float(y_post[2]))
 
         tracker: Optional[CutTracker] = None
         if parking_ratio_test(index, ratio):
@@ -2310,7 +2317,7 @@
 
                 # the cut (inelastic-reflection prompt 06), after the candidate; where it does not
                 # fire, the |pi| guard (P21). At alpha = 0, or without a wall, neither runs
-                if assess_wall:
+                if model.reflects_at_origin and model.log_V_floor is not None:
                     parked = cut(N, y_reflected, kind, pi_)
```

**B11 — the parked phase's adiabaticity check removed.** Fails
`test_7_at_beta_5_it_fails_where_the_tracker_loses_adiabaticity` (the history does not fail).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2065,7 +2065,7 @@
             m_eff_over_H_, _ = guarded(tracker_m_eff_over_H, policy, hubble, N_, y_)
             if not (m_eff_over_H_ >= min_m_eff_over_H):
                 min_m_eff_over_H = m_eff_over_H_
-            if not tracker_adiabatic(m_eff_over_H_):
+            if False:
```

## 7. Observations not acted on

- **π_min steps at T_J = 10 keV.** The equation-of-state table ends at 10 keV: below it w = 1/3 and
  dw/d ln T = 0 exactly, above it dw/d ln T = −3.6×10⁻⁸. π_min reads w′, so it steps there (by 4.7×10⁻⁴
  relative on a synthetic state at M = 10⁻³⁰, β = 2); φ_min is continuous. Item 3's sample test skips that stencil
  (D5). 09's `AdiabaticHistory` on parked samples will see the step if it differentiates π. Related to
  `[00-xav-sigma-is-non-monotone-at-25-35-keV]`, where the central difference of φ also departs from π
  by up to 24 % (π″ is large there, and the test's tolerance follows it). No issue opened: it is the
  table's, and the user's to place.
- **At large M the cut fires where the energy is not all kinetic** (ruling 12). At M = 0.5 the cut's
  drain points have (V − V_floor)/ρ_kin = 13.9 (β = 25) and 40.6 (β = 5), at M = 10⁻³ 1.1. There V −
  V_floor is the potential that balances the trace, not a wall, so R14's "energy all kinetic" does not
  describe these switches. They are R17's, as ruled; at M ≤ 10⁻⁷ W/E ≤ 7×10⁻⁷.
- **At M = 0.5 the switch moves H_E by 0.15 of the leftover fraction** (1.4×10⁻⁷ against 9.5×10⁻⁷ at
  β = 5): V(φ_min) − V(φ) is not small there, though Δφ/φ is 10⁻⁴. Within prompt §5's bound.
- **Orchestrator note §0's size for B5** (βφ_min/2 ≈ 6×10⁻³) assumed V″/(−V′) ≈ 2/φ. On the
  exponential at M = 0.5 V″/(−V′) ≈ M/φ², so B5 moves π_min by |(ln A)′ s| = 1–6×10⁻⁴. The test is
  tuned to the measured size (§1.2).
- **β = 0.5 at M = 10⁻³ never parks**: its cascade does not empty (Σ = 0.60 of a ceiling of 2, the
  smallest ratio 0.33). The planner's 31 keV loss of adiabaticity there does not arise in 06. β = 1.2
  at M = 10⁻³ parks at 11.5 keV with m/H = 19.6 and fails at 0.25 eV: 07's first release case.
- **The tuning converges on the seed** at M ≤ 10⁻⁷ (§5.6): one integration each. Whether that holds
  after 07's release, which changes the late history, is for 07 or 11.

## 8. State handed to the next prompt

- **The parking record** (`ComputeTargets/ScalarHistory.py`), `Parking`, on `IntegrationResult.parking`
  and the payload's `"parking"` (not stored); `None` if the history did not park:
  - `N`, `log_T_Jordan`: the switch;
  - `candidate_index`: the cut candidate in `injections` (never 0);
  - `phi_Einstein`, `pi_Einstein`: the drained state's field just before the switch;
  - `phi_min`, `pi_min`: the parked state, which the dense output returns at `N` bitwise;
  - `rho_kin_over_rho_rad`: the ratio tested;
  - `delta_K`, `delta_rho_over_rho_rad`: the leftover, the ledger's last entry, at `N`;
  - `m_eff_over_H`: the tracker's at the switch, the decision's;
  - `H_Einstein_before` (the candidate's), `H_Einstein_parked` (with the leftover);
  - `delta_log_A`: ln A(φ_min) − ln A(φ);
  - `parked_steps`, `min_m_eff_over_H`: the parked phase's steps and smallest m/H (switch included).
- **The parked segment's interface.** `ParkedSegment(reduced_interpolant, phi_anchor, model,
  cosmology)`, a `DenseOutput` over [t_old, t]: `segment(N)` → (5,), `segment(array)` → (5, n),
  `segment.state_at(N)`; `reduced_interpolant` is DOP853's dense output of (ln ρ_R,E, ln f_m, ln T_J);
  `phi_anchor` the step's left-node φ_min. **A release would read** its state from the last parked step
  end: (φ_min, π_min) = `parked_tracker(model, cosmology, anchor, psolver.y)` with the reduced state
  `psolver.y`, exactly what `run_parked_phase` computes as `y_end` before its check; a new Radau
  instance from that five-component state continues the oscillating phase (P24).
- **Where the adiabaticity check is made**: in `run_parked_phase`'s nested `check`, at every parked
  step end on (`psolver.t`, `y_end`), and on the last step at N_final on the segment's value; m_eff/H_E
  from `tracker_m_eff_over_H` (`log_m_eff_squared` at the tracker, H_E from `policy` and
  `HubblePolicy` with the ledger as it stands). `tracker_adiabatic` False raises today; 07 replaces the
  raise with the release. The check lags the crossing of m/H = 10 by at most one step (0.1 e-folds).
- **The histories that lose adiabaticity**, for 07:
  - M = 0.5, β = 5: parks at #105, N = 38.00441 (81.2 MeV); fails at N = 40.29089 (9.40 MeV); the
    crossing is at N = 40.25454 (9.75 MeV);
  - M = 0.5, β = 25: parks at #83, N = 132.55077 (9.97 GeV); fails at N = 145.78944 (39.8 keV);
  - M = 0.5, β = 10: parks at 358 MeV (orchestrator's probe; `test_injection` (11) runs it with the cut
    disabled);
  - M = 10⁻³, β = 1.2: parks at #6, N = 28.37191 (11.5 keV, m/H = 19.6); fails at N = 45.95242
    (0.25 eV).

  None at M ≤ 10⁻⁷ (smallest m/H 72) or at M = 10⁻³, β = 2, 3 (12.6, 17.6).
- **The constants**: `PARKING_EPSILON = 1.0e-6`, `PARKING_ADIABATICITY_THRESHOLD = 10.0` (also 07's
  release threshold), `PARKING_PI_GUARD_MP = 1.0e-9` (× `units.PlanckMass`); none a key.
- **The functions**: `parking_ratio_test`, `tracker_adiabatic`, `parking_guard`, `log_trace`,
  `tracker_velocity_terms`, `tracker_velocity`, `parked_tracker`, `tracker_m_eff_over_H`,
  `cut_tracker`, `parking_leftover`; classes `ParkedRHS`, `ParkedSegment`; records `Parking`,
  `TrackerVelocity`, `CutTracker`.
- **The cost**: ratio test 0.13 µs per candidate; `cut_tracker` 75–90 µs where the ratio passes; a
  parked right-hand side 47–58 µs; a parked step 0.5–0.9 ms; a parked history to T_CMB 124–257 steps,
  about 0.1 s; the M = 0.5, β = 2 history unchanged (2.65 s against 2.66 s).
- **Issues**: opened none; narrowed `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`
  (the wall search never raised on the 22 histories measured; the 21 whose cascade empties park
  first). `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`
  is still open for 08.
- **Tests**: `ComputeTargets/tests/test_parking.py` (34); `test_injection` (11) and
  `test_gate_and_taper` (9) run with ε = 0.0.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 408 (96 + 312), ObjectFactories
  103, tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02; this
  campaign's close (12) updates them.
