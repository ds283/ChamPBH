# Log 03 — the gate and the stopping rule

**Subject:** Gate each drain on the bounce's non-adiabaticity
**Commit:** `0afa199`, on branch `inelastic-reflection`, on top of `c3fcfdf`
**Date:** 2026-10-28, by Claude Opus 5.5
**Result:** **COMPLETE, with the stopping rule withdrawn by the user mid-prompt (ruling 16).**
- **The gate ships; the stopping rule does not.** At α > 0 every drain point and every floor
  reflection is a candidate, drained at α_eff = α · w_gate, w_gate = 𝒜_k²/(𝒜_k² + 1). While I was
  working, the user withdrew w_rate (orchestrator note 03, ruling 16: "I'd drop `w_rate`"). I had
  written it; I removed it, and nothing of the stopping rule (ε, the window, S, the omitted part,
  P17) is in the code, the records or the tests. Prompt items 4–6 and breakages B2, B3, B4 and B7
  went with it.
- **The gate "Taper"**, as ruling 16 restates it:
  - **w_gate equals its hand values**, within 2 ε of exact rationals at 𝒜 = 0, 10⁻³, 1, 10, 10³
    and 10²⁰⁰ (exactly 0, ½ and 1 at 0, 1 and 10²⁰⁰);
  - **the tuning residual is continuous** across the turning-point-count changes the scans found
    (§5.3).
- **α = 0 is the old code.** Both golden histories, `test_first_bounce.py`,
  `test_surfing_turning_points.py` and `test_kinematic_cap_loop.py` pass unedited, and at α = 0
  none of 03's functions is called (item 1). The golden tests' wall time did not move (§4.4).
- **𝒜_k agrees with an independent reference** (item 3): |Δw_gate| ≤ 1.5e-3 at every turning
  point of the β = 2 history, against a tolerance of 0.05.
- **Nothing a build stores moved.** No column, key, fingerprint format or version changed. No store
  was opened, no Ray started, nothing written to `var/`, and no golden JSON regenerated.
- **Counts:** 26 / 305 (96 + 209) / 103 / 115 / 64 before; 26 / **317** (96 + **221**) / 103 / 115 /
  64 after, all passing. The 12 new tests are in `test_gate_and_taper.py`.
- Opened no issue and closed none. `.documents/OPEN_ISSUES.md` is unchanged.

## 1. The physics and the numerics

### 1.1 The effective mass (prompt §1.1; note 03 ruling 2)

`ODERHS` integrates π′ = −π(Ḣ/H² + 3) − V′/H² − (ln A)′ ρ_R,E (Σ + f_m)/H², in units with
M_P = 1 here and per e-fold. The φ-dependent force is −∂_φ V_eff with

  V_eff(φ) = V(φ) + A(φ) ρ̃_c,  ρ̃_c ≡ ρ_R,E (Σ + f_m)/A,

since ∂_φ(A ρ̃_c) = A′ρ̃_c = (ln A)′ A ρ̃_c = (ln A)′ ρ_R,E(Σ + f_m), which is the coupling term. The
local mass of a quantum is the curvature of V_eff with the source held fixed: ρ̃_c is the trace
with its conformal factor taken out, which the prompt takes as conserved (prompt §1.1), and which
does not respond to a fluctuation of φ on a quantum's time scale. So

  m_eff² = ∂²_φ V_eff = V″ + A″ ρ̃_c = V″ + (A″/A) ρ_R,E (Σ + f_m),

and A″/A = (ln A)″ + (ln A)′². With V″ = V[(ln V)″ + (ln V)′²] this is prompt §1.1's form. For the
exponential coupling (ln A)″ = 0, so on every real history m_eff² = V″ + β² ρ_R,E (Σ + f_m)/M_P², the
planner's probe's expression.

**How it differs from `AdiabaticComputePolicy.M2eff_over_H2`.** That is the mass of the
*perturbation* δφ of the homogeneous background, which the ODE's own response determines: it
differentiates V′_eff at fixed Einstein-frame scale factor and comoving entropy, so the source
responds through T_J(φ): its (ln A)′² term carries Σ² − Σ_T/(1 + x) + f_m instead of Σ + f_m (Σ_T =
dΣ/d ln T_J, x = d ln g_s/3 d ln T_J), and it subtracts H²(2 + Ḣ/H²), the gravitational mass. A
quantum oscillates far faster than the bath responds, so its source is frozen, and the
gravitational term is of order H², small where m ≫ H. The two agree when Σ² − Σ_T/(1 + x) = Σ and
H ≪ m. `M2eff_over_H2` is not called.

**Why the function returns ln m_eff².** The loop needs ln m_eff² for d ln m_eff²/dN, and m_eff
= exp(½ ln m_eff²) for 𝒜 and m_eff/H. Formed as ln V + ln|(ln V)″ + (ln V)′²| and
ln ρ_R,E + ln|[(ln A)″ + (ln A)′²](Σ + f_m)|, each with its sign, and summed relative to the larger,
neither V nor ρ_R,E is exponentiated, so nothing overflows however large M/φ is. A non-positive or
non-finite sum is a `ComputationFailureError` naming φ, ln ρ_R,E, ln f_m, Σ and the model. None was
seen on any history this prompt ran (m_eff² > 0: V is convex for φ > 0 and Σ ≥ 0).

### 1.2 The non-adiabaticity and its attribution

𝒜 = |dm/dt|/m² = H_E |d ln m²/dN|/(2m), since dt = dN/H_E and d ln m = ½ d ln m². On each appended
step the ten samples (the two end states, read by the scalar calls 02 already makes, and the 8
interior states of its one vectorized call) give ln m² and H_E (one `ODEPolicy` call and one
`HubblePolicy` call each, never `fun`), and `np.gradient(ln m², N)` within the step: second-order
central differences inside, one-sided at the ends, nothing across a step boundary.

A sample belongs to the half-oscillation (N_{k−1}, N_k] that contains its N (note 03 ruling 3): the
loop keeps `boundary_N` and a running maximum, folds the samples with boundary_N < N ≤ N_k into it,
closes it at each turning point (either kind) and each floor reflection, and starts the next there.
A sample at a node shared by two steps counts in each, with its own step's gradient, unless it is at
or before the current boundary. That rule leaves out two samples by construction: the history's
first (at N_start, the first boundary), and, after a floor reflection, the next step's first (at
N_refl). The half-open interval says so, and the reference does the same.

### 1.3 The rounding bounds of the tests

- **w_gate** (item 2), against exact rationals (`fractions.Fraction`) of A²/(A² + 1) at the float
  input. Below 𝒜₀: A·A, A·A + 1 and the quotient each round once (ε/2), and the quotient's condition
  in A·A is 1/(1 + A²) ≤ 1: at most 1.5 ε relative. Above 𝒜₀: r = 1/A (ε/2), r·r (1.5 ε), 1 + r·r
  (r²/(1 + r²) ≤ ½ of r·r's error, plus one rounding: 1.25 ε) and 1/(…) (ε/2): at most 1.75 ε. The
  test asserts 2 ε relative.
- **Δρ/ρ_R** (item 7), against α_eff·½H²π²/ρ_R by products: the loop forms
  exp(ln α_eff + ln ρ_kin − ln ρ_R), with ln ρ_kin = 2 ln H + 2 ln|π| − ln 2. Each log has an
  absolute rounding of ε/2 times its size (doubled by the factors 2), each sum ε/2 of its partial
  result, and exp turns its argument's absolute error into a relative one. So the bound is
  ε(2|ln H| + 2|ln|π|| + |ln ρ_kin| + |ln α_eff| + |ln ρ_R| + |ln(Δρ/ρ_R)| + 8), 02's
  `delta_K_bound` with ln ρ_R in place of 4N.
- **The mass** (ruling 2's hand value): at φ = 0, ρ_R = 1, Σ = ¼, f_m = exp(ln ¾), m² = 1 + 7 = 8.
  f_m is within 1.5 ε of ¾, the trace term's log within about 2 ε of ln 7, and the relative sum and
  its log add about 2 ε: asserted to 8 ε on ln m² ≈ 2.08. A generic state (φ = 0.3, quadratic log V)
  is checked against the expression formed directly in floats to 32 ε relative.

### 1.4 Ruling 16: the stopping rule withdrawn

The user's words: "I'd drop `w_rate`". The reason, as the orchestrator gave it: the loop skips a
drain and its restart only when α_eff == 0.0, which only underflow gives. w_rate is never zero, so
it never saved a restart, which was its only stated benefit (scoping §6: "Stopping saves the
restarts, not the bounces"). It only withheld up to (π/2)ε per e-fold of energy, for nothing. Log 02
§5 shows restarts are cheap at M = 0.5: the drained histories ran faster than at α = 0.

So α_eff = α · w_gate, as floats. There is no S, window, ε or omitted part; `RATE_EPSILON`,
`RATE_WINDOW_EFOLDS`, the window sum, the w_rate function and the omitted-part function are not in
the code. `Injection` gains only `nonadiabaticity` and `w_gate`. The bound of prompt §1.4 and its
proof (note 03 ruling 9) are withdrawn with it, so this log does not carry them.

One thing I found before the ruling reached me, recorded because the prompt states it. Prompt §0.3's
counterexample says that a stopping sum over the drains as made (B2's form) would, on 1 000
candidates of 10⁻⁵ in one e-fold, omit 10⁻² (100ε, nearly all). I simulated it: it omits 1.02e-3
(10ε, about a tenth), because the drains as made accumulate in the window, and S passes ε after
about 100 candidates instead of P17's 10. P17's form omitted 1.51e-4, inside (π/2)ε = 1.571e-4. The
conclusion of §0.3 (B2's form breaks the bound) held, by a factor 6.5, not 64. It is moot now.

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`
- **The constant** `NONADIABATICITY_SCALE = 1.0` (𝒜₀, the user's ruling), with a comment saying the
  stopping rule was withdrawn.
- **Module-level functions**, called by the loop through their module names, so that
  `unittest.mock.patch` on `ComputeTargets.ScalarHistory.<name>` intercepts them (ruling 1):
  - `log_m_eff_squared(model, phi_Einstein, log_rhorad_Einstein, log_fm, Sigma)` (§1.1);
  - `step_nonadiabaticity(N_samples, log_m2_samples, H_samples)`: 𝒜 at a step's samples, with
    `np.gradient` in N; a non-finite value is a `ComputationFailureError`;
  - `gate_weight(A)`: A²/(A² + 𝒜₀²) up to 𝒜₀, 1/(1 + (𝒜₀/A)²) above; negative or non-finite A is
    a `ComputationFailureError` (ruling 8).
- **The records** (ruling 13), module-level namedtuples, the new fields appended last, the comment
  blocks naming them:
  - `TurningPoint(..., nonadiabaticity, m_eff_over_H)`, both `None` at α = 0;
  - `Injection(..., nonadiabaticity, w_gate)`, one per candidate at α > 0, α_eff = 0.0 included;
  - `IntegrationResult(..., dropped_drains)`.
- **`integrate_scalar_history`**, at α > 0 only (ruling 1):
  - `observe_turning_points` reads the step's two end states and 8 interior states as full states
    (the same calls 02 makes, `[1]` taken after), and at α > 0 computes ln m_eff² and H_E at each
    (`mass_and_H_at`: one `policy` call, Σ from its data, `hubble` on the same data) and 𝒜 by
    `step_nonadiabaticity`, before the fast-path return and before any drain at the step's end
    (ruling 5). It folds the samples into the running maximum of the half-oscillation in progress
    (`fold`), closes it at each turning point (`close_half_oscillation`), and records 𝒜_k and
    m_eff/H_E (at the turning point's own state) on each `TurningPoint` (ruling 3).
  - `alpha_effective(nonadiabaticity)` is the one place α_eff is set: it returns (0.0, None) at
    α == 0 before any weight, and otherwise (α · w_gate, w_gate).
  - `candidate(...)` takes ruling 4's order as amended by ruling 16: H_E and ln ρ_kin,E (02's
    expressions, moved out of `drain` unchanged), then `alpha_effective`, then 02's `drain` at
    α_eff > 0, or a record with π_after = sign·π_before, ΔK = Δρ/ρ_R = 0.0, H_after = H_before (the
    same float) and no ledger entry or restart at α_eff == 0.0.
  - `drain(...)` now takes H_before and ln ρ_kin from `candidate`; its arithmetic is 02's.
  - The floor branch at α > 0 closes the half-oscillation at the reflection's N and makes the
    reflection a candidate with that 𝒜_k. With α_eff == 0.0, or at α = 0, its restart is 02's
    elastic one, the same two lines.
  - The drain-point branch at α > 0 makes a candidate on the armed wall point's 𝒜_k; at α = 0 it
    does what 02 did (nothing but disarm).
  - `dropped_drains` (ruling 7) is counted at every α: a turning point of either kind arriving while
    a drain is armed, a floor reflection while armed, and a drain still armed when the history ends.
    The termination step's turning points now go through the same arming loop, so a wall-side
    turning point on the last step counts as dropped; that makes walls − "turning_point" candidates
    equal `dropped_drains` at α > 0 exactly.
  - The docstring describes the gate and says none of it runs at α = 0.
- **`integrate_history`**: the payload gains `"dropped_drains"`; the docstring says so.
- **Unchanged, byte for byte:** `compute_scalar_history`, `build_extra_data`, `ScalarHistory.store`,
  `first_bounce`, `surfing_turning_points`, `fixed_T_value_at`, `_tuning_state`,
  `TUNING_OUTPUT_KEYS`, `tune_history`, `ODEPolicy`, `HubblePolicy`, `ODERHS` (no hunk of the diff
  is in them), and `ComputeTargets/tests/golden_history.py` (`record_of`). Nothing on the way to the
  store iterates over the payload's keys, so `"dropped_drains"` is not stored.

### The tests (`ComputeTargets/tests/test_gate_and_taper.py`, new; 12 tests, about 13 s at a load of 6–15)
Each docstring gives its cost and the load, and each bound is stated with its derivation (§1.3).
- **`TestTheGate`** (milliseconds):
  - `test_2_gate_weight_against_hand_values`: w_gate at 𝒜 = 0, 10⁻³, 1, 10, 10³ and 10²⁰⁰ against
    exact rationals of A²/(A² + 1), to 2 ε relative; in [0, 1]; exactly 0, ½, 1 and 0 at 0, 1, 10²⁰⁰
    and 5e-324.
  - `test_2_the_gate_refuses_a_non_finite_or_negative_value`: −10⁻³, NaN and ∞ raise
    `ComputationFailureError`.
- **`TestTheMass`** (milliseconds; ruling 2), on a stand-in with log V = v₁φ + v₂φ² and
  ln A = βφ + γφ², (ln A)″ = 2γ ≠ 0:
  - `test_2_the_mass_against_a_hand_value`: m_eff² = 1 + (3 + 4)(1)(¼ + ¾) = 8 at φ = 0, to 8 ε on
    ln m_eff² (5 without the (ln A)″ term, 1 without the trace);
  - `test_2_the_mass_at_a_generic_state`: φ = 0.3 against the expression in floats, 32 ε relative;
  - `test_2_a_non_positive_mass_is_a_failure`: m_eff² < 0 and m_eff² = 0 raise.
- **`TestAlphaZeroIsTheOldCode`** (item 1; the golden β = 1.5 history, about 1.5 s):
  `test_1_nothing_of_prompt_03_runs_at_alpha_zero`. `log_m_eff_squared`, `step_nonadiabaticity` and
  `gate_weight` are patched with wrappers and called 0 times; every TurningPoint's two fields are
  `None`; no injection; the ledger empty; `dropped_drains` an `int`, equal to the payload's.
- **`TestARealHistoryAtAlpha`** (items 3 and 7; β = 2, α = 0.1, one integration, about 5 s with the
  reference):
  - `test_3_the_nonadiabaticity_against_a_reference`: the loop's 𝒜_k (10 samples per step) against
    `reference_nonadiabaticity` (33 per step, its own m_eff² expression, H_E with the ledger as the loop
    saw it, ruling 3's attribution), at every wall-side and plateau turning point: |Δw_gate| ≤ 0.05,
    both gates by the file's own expression. Measured: wall 1.50e-3 (largest relative difference in
    𝒜_k 2.9%), plateau 1.31e-3 (2.0%). And m_eff/H_E at every turning point against the reference's
    expression at its state, to ε(|ln V| + |ln ρ_R,E| + |ln H_E| + 16) (about 1e-13; measured 1.4e-14)
    (D3).
  - `test_7_alpha_eff_is_alpha_times_the_gate`: α_eff == α · w_gate exactly; w_gate ==
    `gate_weight`(𝒜_k); 𝒜_k equal to the wall point's; both TurningPoint fields set, m_eff/H > 0.
  - `test_7_delta_rho_over_rho_rad_from_the_record`: Δρ/ρ_R against α_eff·½H²π²/ρ_R by products, to
    the log-sum bound of §1.3.
  - `test_7_the_ledger_holds_exactly_the_drains_made`: the ledger's injections are the records with
    α_eff > 0, as the same floats; an α_eff == 0.0 record would have zero ΔK and Δρ, π and H
    unchanged; `dropped_drains` == walls − "turning_point" candidates == walls no record names ==
    the payload's.
- **`TestAFloorRuleCandidate`** (item 8; the P1 harness at M = 10⁻¹⁰, about 0.3 s):
  `test_8_the_floor_reflection_is_a_gated_candidate`. The record carries 𝒜_k and w_gate;
  α_eff == α · w_gate; the ledger holds it iff α_eff > 0; π_out == −√(1 − α_eff) π_in exactly, which is
  also `solution(N)[1]`. **The floor's 𝒜_k is 2.01, w_gate 0.802, α_eff 0.0802.**
- **`TestTheNearStallIsGated`** (item 9; β = 5, α = 0.1, one integration, about 6 s):
  `test_9_a_wall_side_turning_point_with_small_A_k_is_gated_off`: at least one candidate after a
  wall-side point with 𝒜_k < 10⁻² (there are 8), and each has w_gate < 10⁻⁴. **This history holds no
  interior pair under the gate**; the docstring says so (D7).

### Edits to `ComputeTargets/tests/test_injection.py` (ruling 10 and prompt §1.6)
1. **Item 2** (`test_2_one_injection_per_wall_side_turning_point_and_none_after_a_plateau`):
   `assertEqual(i.alpha_eff, ALPHA)` → `assertEqual(i.alpha_eff, ALPHA * i.w_gate)`, and the
   docstring's "at alpha_eff = alpha" → "alpha * w_gate". Reason: the drain's fraction is α·w_gate
   since 03 (ruling 16). The literal α → `i.alpha_eff` would assert α_eff == α_eff; the product is
   the check the line made.
2. **Item 5** (`test_5_the_ledger_is_the_sum_of_the_drains`): `sqrt(1.0 - ALPHA)` →
   `sqrt(1.0 - i.alpha_eff)` and `ALPHA * 0.5 * ...` → `i.alpha_eff * 0.5 * ...` (black split the
   product over lines), and the docstring's α → α_eff. Reason: the drain's fraction.
3. **The floor test** (`test_8_the_floor_reflection_drains`): π_after with `i.alpha_eff`;
   `assertEqual(i.alpha_eff, ALPHA)` → `ALPHA * i.w_gate`; `direct` with `i.alpha_eff`. Reason: the
   same. The floor's α_eff is 0.080 (w_gate 0.80), so `delta_K_bound` stays valid there.
4. **Item 11** (`TestTwoTurningPointsOnOneStep`): `setUpClass` integrates β = 10 instead of 5, and
   the class docstring says why and records the search. Reason: under the gate the β = 5, α = 0.1
   trajectory holds no interior pair (prompt §1.6: "rebuild the test on a history that does"). The
   two test methods are unedited, and B9 still fails both (§6).
5. **The module docstring**: two sentences saying (11) moved to β = 10 and (2), (5), (8) check
   α_eff.

Ruling 10's conditional restrictions (`delta_K_bound` and test 6's inequality to α_eff > 0.0, test
6's π_after ≠ π to √(1 − α_eff) ≠ 1) are **not reached**: every record on the β = 2 history has
α_eff ≥ 2.3e-3 (its smallest wall-side 𝒜_k is 0.153), and the floor's is 0.080, so I made none of
them.

**The search for item 11's pair** (prompt §1.6), each a full integration at M = 0.5 M_P, n = 1, the
seed realization, by `pair_search.py` in my scratch directory. A "pair" is a step on which the loop
recorded two or more turning points.

| run | α | β | pair? | 17 > loop |
|---|---|---|---|---|
| §5's measurement | 0.1 | 1.2, 2, 5, 25 | none | 0 |
| 1 | 0.2 | 5 | none | 0 |
| 2 | 0.05 | 5 | none | 0 |
| 3 | 0.3 | 5 | none | 0 |
| 4 | 0.1 | 4 | none | 0 |
| 5 | 0.1 | 6 | none | 0 |
| 6 | 0.1 | 8 | none | 0 |
| 7 | 0.1 | 10 | **step 9333**: plateau at N = 63.85269 (𝒜_k = 2.8e-2), wall at 63.85355 (𝒜_k = 2.1e-5), T_J ≈ 36 MeV | 0 |

Seven integrations after the four of §5. (I ran the same search once before ruling 16 reached me,
under the w_rate code; it found no pair in eight, β = 3 included. Those trajectories are not the
shipped code's and are not counted.) The β = 10 pair's wall point has w_gate ≈ 4e-10, so its drain
is ≈ 4e-11 of the kinetic energy: the near-stall is gated off there, as at β = 5 in 02.

### The documents (P12)
- `.documents/numerical-strategies.md`: new §6.3, dated 2026-10-28, after §6.2: the mass, the
  non-adiabaticity and its attribution, the gate, α_eff == 0.0, α = 0, and that the stopping rule
  was withdrawn.
- `.documents/architecture-summary.md`: a dated note at the end of §7.2, after 02's: the functions,
  the records' new fields, `dropped_drains` and the payload key.

## 3. Deviations from the prompt

**The rulings of orchestrator note 03 (1–16):**

| # | Deviation or ruling | Class |
|---|---|---|
| R1 | At α = 0 nothing of 03 runs: `alpha_effective` returns 0.0 first; the loop calls it, the mass, the non-adiabaticity and the gate only under `alpha > 0.0`; no sample state kept, no `Injection`, the TurningPoint fields `None`. The functions are called by module name, so a patch intercepts them (item 1, B8) | STRUCTURALLY REQUIRED |
| R2 | One module-level mass function, `log_m_eff_squared(model, φ, ln ρ_R,E, ln f_m, Σ)`, returning **ln m_eff²** (§1.1 says why). Σ comes from the `ODEPolicyData` of the one policy call that feeds `HubblePolicy`; f_m is `exp(state.log_fm)`, the same quantity `ODEPolicy._get_fm` forms, so I pass ln f_m from the state (ruling 2's signature) rather than `data.fm`. The hand value on a stand-in with (ln A)″ ≠ 0; item 3's reference uses its own expression; B10 fails the hand value and item 3 (after D3) | IMPLEMENTATION CHOICE (ruled) |
| R3 | One attribution rule, half-open (N_{k−1}, N_k], for the loop and the reference; the gradient over the step's own samples before attribution. At a floor reflection, the next step's first sample is at the boundary and belongs to neither half-oscillation, as the rule says | STRUCTURALLY REQUIRED |
| R4 | A candidate's order: H_E, ln ρ_kin,E; w_gate; α_eff; then 02's drain or the α_eff == 0.0 record. Its window clauses withdrawn by R16 | STRUCTURALLY REQUIRED |
| R5 | The ten samples are 02's reads, kept as full states (fast path included), through `policy` and `hubble` only, before any drain at the step's end. `nfev` is unchanged: the policy-call count rises by 10 per step, the RHS count does not (§4.5) | STRUCTURALLY REQUIRED |
| R6 | An empty half-oscillation has 𝒜_k = 0.0. It happened **0 times** on every history this prompt ran (§5) | IMPLEMENTATION CHOICE (ruled) |
| R7 | `dropped_drains` counted at every α by the arming logic; at α > 0 equal to walls − "turning_point" candidates (item 7); at α = 0 only checked to be an `int` (item 1) | IMPLEMENTATION CHOICE (ruled) |
| R8 | w_gate finite in [0, 1] for every finite 𝒜 ≥ 0, via 1/(1 + (𝒜₀/𝒜)²) above 𝒜₀; non-finite or negative 𝒜 a `ComputationFailureError`; item 2 adds 𝒜 = 10²⁰⁰ (and 5e-324); hand values to 2 ε (§1.3). Its S clauses withdrawn by R16 | IMPLEMENTATION CHOICE (ruled) |
| R9 | Withdrawn by R16 (the omitted part and the bound) | — |
| R10 | The edits to `test_injection.py` are the prompt's α → α_eff (items 2, 5, the floor) and item 11's rebuild; the conditional restrictions were not reached (§2) | STRUCTURALLY REQUIRED |
| R11 | Scans in `normalization_coordinate`, φ_ref fixed; the comparison by `mock.patch` of `gate_weight` → 1.0 (R16 item 8) in the scratch script. The ±10⁻³ scans at α = 0.1 crossed no count change, so the search was run: none at β = 2 in 13 integrations, one at β = 5 in 12, scanned across (§5.3) | as ruled |
| R12 | Timing: the dispatch commit in a scratch worktree (since removed), back to back, three rounds; policy calls counted. The linear ln H_E option was not taken: the cost is below double (§4.5) | as ruled |
| R13 | Records module-level namedtuples, fields appended last in the prompt's order (as amended by R16), comments naming them; `"dropped_drains"` the payload key; `compute_scalar_history`, `build_extra_data`, `store`, `record_of`, `TUNING_OUTPUT_KEYS` byte-identical | as ruled |
| R14 | Items 3 and 7 share the β = 2 integration; item 9 and test_injection's item 11 are in different modules and now on different β, so they do not share one. Each docstring gives its cost and load | as ruled |
| R15 | Suites serial, ComputeTargets in halves, output to scratch, `uptime` beside each | as ruled |
| **R16** | **The stopping rule withdrawn by the user**: "I'd drop `w_rate`". α_eff = α · w_gate; no S, window, ε, omitted part or P17; `Injection` gains only `nonadiabaticity` and `w_gate`; items 4–6 and B2, B3, B4, B7 dropped; the measurement's w_rate, Σc and Σo columns dropped; the comparison patch is w_gate → 1. I had written the stopping rule and its tests before the ruling arrived and removed them all (§1.4) | STRUCTURALLY REQUIRED (ruled by the user) |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | `step_nonadiabaticity` is a module-level function too, besides the ones the prompt names, so that B5 has one place to break and item 1 can patch it | IMPLEMENTATION CHOICE |
| D2 | The termination step's turning points go through the arming loop (02 discarded them). This changes nothing 02 recorded, and it makes `dropped_drains` count a wall-side turning point on the last step, which the identity of ruling 7 needs | IMPLEMENTATION CHOICE |
| D3 | **Item 3 also checks m_eff/H_E at every turning point** against the reference's own expression at its state, to ε(\|ln V\| + \|ln ρ_R,E\| + \|ln H_E\| + 16) (measured 1.4e-14). Without it, B10 failed only the hand values: where the trace term matters (plateau turnarounds, up to 10³⁹ × V″), 𝒜_k is far above 𝒜₀ with or without it, so both gates are 1 and Δw_gate stays below 0.05 (2.7e-3 under B10), while the relative difference in 𝒜_k was 6.7e31. Ruling 2 requires B10 to fail item 3 | STRUCTURALLY REQUIRED |
| D4 | Item 2 also asserts w_gate(5e-324) == 0.0 and that negative, NaN and infinite 𝒜 raise | IMPLEMENTATION CHOICE |
| D5 | The mass has two tests besides the hand value: a generic state (φ = 0.3, quadratic log V) against the expression in floats, and m_eff² < 0 and = 0 raising | IMPLEMENTATION CHOICE |
| D6 | Item 7 also asserts w_gate == gate_weight(𝒜_k), that a record's 𝒜_k is its wall point's exactly, and that every TurningPoint carries both fields | IMPLEMENTATION CHOICE |
| D7 | Item 9's history holds no interior pair under the gate; its docstring and this log say so, and the pair's gating is reported from the β = 10 history (§2) instead of asserted | as the prompt directs |
| D8 | The log is titled by the prompt's file name, "the gate and the stopping rule", though only the gate shipped | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT.

## 4. Verification performed

### 4.1 The suites
Serially, ComputeTargets in two halves, output in my scratch directory. "Before" at `1d3b0c6`
before any edit; "after" on the final tree (on top of `c3fcfdf`, which touches only `prompts/`).

| Suite | Before (count, time, 1-min load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 72 s, 13.7 → 9.4 | 26 OK, 75 s, 6.9 → 9.8 |
| ComputeTargets [a-f] | 96 OK, 131 s, 9.4 → 7.8 | 96 OK, 129 s, 9.8 → 7.0 |
| ComputeTargets [g-z] | 209 OK, 283 s, 7.8 → 9.0 | **221** OK, 299 s, 7.0 → 57.8 |
| ObjectFactories | 103 OK, 18 s, 9.0 → 7.7 | 103 OK, 17 s, 57.8 → 50.7 |
| tools | 115 OK, 134 s, 7.7 → 20.2 | 115 OK, 133 s, 50.7 → 14.4 |
| RunDrivers | 64 OK, 27 s, 20.2 → 15.1 | 64 OK, 27 s, 14.4 → 12.0 |

- The baselines are the dispatch's (26 / 305 (96 + 209) / 103 / 115 / 64). ComputeTargets rose by
  12, the new `test_gate_and_taper.py` (it sorts into [g-z]), and nothing fell.
- `black --check` on the three changed Python files: all unchanged.
- After the suites I corrected one cost in a docstring of `test_gate_and_taper.py` (no code), and
  re-ran that module alone: 12 tests OK.

### 4.2 The golden tests alone, before any edit (load 6–7)
`test_golden_history`: 3 tests in 10.96 s (12.7 s real). The tuned golden test: 25.6 s (27.2 s real).

### 4.3 α = 0 is bitwise
`test_golden_history`, `test_first_bounce`, `test_surfing_turning_points`,
`test_kinematic_cap_loop` and the tuned golden test passed unedited on the final tree, 35 tests,
and in the "after" suites. Item 1 holds. The golden JSONs are untouched.

### 4.4 The cost at α = 0 (ruling 12)
The unittest "Ran … in" times, alternating the dispatch commit (`1d3b0c6`, a scratch worktree) and
the change, three rounds, at a 1-minute load of 5–21:

| | dispatch (s) | min | change (s) | min |
|---|---|---|---|---|
| golden (two histories) | 10.78, 10.53, 10.73 | **10.53** | 10.98, 10.25, 10.30 | **10.25** |
| tuned golden | 26.55, 27.45, 25.17 | **25.17** | 27.82, 25.00, 25.26 | **25.00** |

**Nothing measurable**: the change's minima are inside the dispatch commit's spread, and lower.
Nothing of 03 runs at α = 0. B8 (the 𝒜 computation at α = 0 too) costs golden 10.25 → 15.15 s
(+48%) and tuned 25.0 → 37.5 s (+50%) at a load of 6–8 (a first run at a load of 38 gave 17.0 and
42.2 s).

### 4.5 The cost at α = 0.1 (ruling 12)
The four §5 histories, `time_alpha.py`, alternating with the dispatch commit, minima of three:

| β | dispatch: wall (s) / steps / RHS | change: wall (s) / steps / RHS | ratio | per step: dispatch → change (µs) |
|---|---|---|---|---|
| 1.2 | 0.914 / 1 935 / 17 387 | 1.255 / 1 934 / 17 425 | 1.37 | 472 → 649 |
| 2 | 1.684 / 3 836 / 35 642 | 2.410 / 3 969 / 36 666 | 1.43 | 439 → 607 |
| 5 | 3.200 / 7 494 / 71 156 | 5.742 / 9 270 / 91 558 | 1.79 | 427 → 619 |
| 25 | 5.666 / 12 843 / 117 973 | 10.711 / 17 232 / 159 966 | 1.89 | 441 → 622 |

- **No history more than doubles**, so H_E is taken from `HubblePolicy` at every sample, as the
  prompt's default, and the linear ln H_E option was not used. β = 25 comes closest (1.89): it has
  34% more steps under the gate (fewer drains early, more oscillations kept), and each costs more.
- **The 𝒜 computation costs about 170–190 µs per accepted step** (the per-step difference above),
  on top of a step's 430–470 µs at the dispatch commit (about 40%). The trajectories differ (the gate changes the drains), so this
  difference also carries the RHS evaluations per step (9.50 → 9.88 at β = 5); at β = 1.2, whose
  steps and RHS counts are nearly equal, it is 177 µs.
- **Policy calls added, counted** by wrapping `ODEPolicy.__call__` (total − `nfev` − one per stored
  sample): dispatch 2 per drain (H_before, H_after), plus 2; change 10 per accepted step, 1 per
  turning point (m_eff/H_E), 2 per drain, plus 2 (the fixed-temperature reads). Each count is
  exact:

  | β | dispatch: added | change: added | = 10 × steps + turning points + 2 × drains + 2 |
  |---|---|---|---|
  | 1.2 | 16 | 19 369 | 19 340 + 13 + 14 + 2 |
  | 2 | 56 | 39 803 | 39 690 + 55 + 56 + 2 |
  | 5 | 174 | 93 321 | 92 700 + 309 + 310 + 2 |
  | 25 | 152 | 173 505 | 172 320 + 591 + 592 + 2 |
- **Per history**, at α = 0.1: 1.3, 2.4, 5.7 and 10.7 s for β = 1.2, 2, 5 and 25, against 02's 0.9,
  1.7, 3.3 and 6.1 s (log 02 §5, at a load of 23–39).

## 5. The measurement (prompt §2)

The setup is 02's: M = 0.5 M_P, n = 1, the seed realization, untuned, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, atol = rtol = 1e-8, main.py's axis, α = 0.1. `measure.py` in my scratch
directory, on the final tree, load 14–17.

### 5.1 The histories at α = 0.1

**Turning points, candidates and the gate.** "𝒜_k" is over wall-side turning points; the w_gate
counts are of wall-side turning points; m_eff/H_E is at plateau turnarounds.

| β | wall-side | plateau | candidates: turning point / floor | α_eff = 0 | dropped | empty half-osc. | 𝒜_k at wall-side: min / median / max | w_gate < 0.01 | 0.01–0.99 | > 0.99 | m_eff/H_E at plateau: min / median / max | < 1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 7 | 6 | 7 / 0 | 0 | 0 | 0 | 79.3 / 223 / 2.24e3 | 0 | 0 | 7 | 3.60e-2 / 0.585 / 0.707 | 6 |
| 2 | 28 | 27 | 28 / 0 | 0 | 0 | 0 | 0.153 / 8.58 / 1.44e3 | 0 | 14 | 14 | 0.137 / 1.89 / 24.0 | 8 |
| 5 | 155 | 154 | 155 / 0 | 0 | 0 | 0 | 1.14e-3 / 0.149 / 842 | 49 | 91 | 15 | 0.637 / 112 / 225 | 2 |
| 25 | 296 | 295 | 296 / 0 | 0 | 0 | 0 | 5.47e-5 / 2.31e-2 / 163 | 199 | 85 | 12 | 1.76 / 252 / 537 | 0 |

𝒜_k at plateau turnarounds (min / median / max): 80.7 / 166 / 2.14e3; 0.834 / 11.8 / 1.35e3;
3.73e-2 / 0.171 / 794; 1.01e-2 / 3.96e-2 / 145.

**The energy, the cost and the check.** "Injected" is Σ α_eff ρ_kin/ρ_R = Σ Δρ/ρ_R. 02's columns are
log 02 §5's, at α_eff = α (load 23–39 there; 14–17 here).

| β | injected (02's) | largest single | r_χ at 1 MeV (02's) | at 70 keV (02's) | at the end (02's) | restarts (02's) | accepted steps (02's) | RHS evaluations (02's) | wall time (02's) | 17 > loop |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 1.504e-2 (1.50e-2) | 1.38e-2 | 1.008e-2 (1.008e-2) | 8.82e-3 (8.82e-3) | 8.78e-3 (8.78e-3) | 7 (7) | 1 934 (1 935) | 17 425 (17 387) | 1.4 s (0.9 s) | 0 |
| 2 | 1.244e-2 (1.25e-2) | 4.30e-3 | 8.16e-3 (8.16e-3) | 6.94e-3 (6.95e-3) | 6.92e-3 (6.93e-3) | 28 (27) | 3 969 (3 836) | 36 666 (35 642) | 2.6 s (1.7 s) | 0 |
| 5 | 1.966e-3 (1.98e-3) | 6.69e-4 | 9.57e-4 (9.65e-4) | 8.02e-4 (8.09e-4) | 7.99e-4 (8.06e-4) | 155 (86) | 9 270 (7 494) | 91 558 (71 156) | 6.3 s (3.3 s) | 0 |
| 25 | 4.19e-4 (4.39e-4) | 3.39e-5 | 1.98e-4 (2.07e-4) | 1.66e-4 (1.74e-4) | 1.65e-4 (1.73e-4) | 296 (75) | 17 232 (12 843) | 159 966 (117 973) | 11.7 s (6.1 s) | 0 |

- **The gate passes the early bounces whole and switches the late ones off.** 𝒜_k falls through each
  history as the probe found, from 10²–10³ at the first bounce to 10⁻³–10⁻⁵ at the last. The early
  bounces carry almost all the energy (the largest single drain is 92%, 35%, 34% and 8% of the total),
  so the injected total and r_χ move by at most 5% from 02's (β = 25), and by under 1% at β ≤ 2.
- **More turning points and more drains at β = 5 and 25.** Gating the late drains leaves the
  condensate more energy, so it keeps oscillating: 155 against 86 wall-side points at β = 5 and 296
  against 75 at β = 25, with 24% and 34% more accepted steps. Every candidate drains (none has
  α_eff == 0.0; the smallest α_eff is about 3e-10, at β = 25).
- **None dropped, no floor event, no empty half-oscillation** (ruling 6), on any of the four.
- **The near-stall** at β = 5: under the gate this trajectory holds no interior pair (the pair of 02 is
  gone). 8 wall-side turning points have 𝒜_k < 10⁻², each with w_gate < 10⁻⁴ (item 9). At β = 10 the
  pair that item 11 now uses has 𝒜_k = 2.1e-5 at its wall point.
- **02's 17-point check** is 0 on every history (and on the seven of the pair search).

### 5.2 The tuning (prompt §2 item 1)

`tune_history` from the model's seed, α = 0.1, `progress="every"`, at a load of 14–27:

| β | integrations | plain / secant / guard | H/H₀ − 1 | φ_ref/φ_min − 1 | wall | final walls / candidates / dropped |
|---|---|---|---|---|---|---|
| 2 | **11, converged** | 1 / 9 / 0 | −9.997e-8 | −3.022e-7 | 28.2 s | 29 / 29 / 0 |
| 5 | **20, converged** | 2 / 14 / 3 | −7.924e-8 | −1.583e-7 | 135.8 s | 173 / 173 / 0 |

β = 2 takes the same 11 integrations as under 02's rule (log 02 §5), with residuals equal to 02's to
3 digits through integration 7 (they part at 4.3e-6 against 4.5e-6 at integration 8). β = 5 takes
three guard steps (V_target ≤ 0 at integrations 0, 1 and 8; integration 8's secant step overshot
to φ_ref < 0) and then converges. Its residual H/H₀ − 1 per
integration: +1.263, +0.876 (guard), +0.535 (guard), −0.151, −4.56e-2, +4.67e-2, +4.51e-3, +1.49e-2,
+0.804, −0.141 (guard), −4.78e-2, −1.22e-2, −6.58e-3, +9.04e-3, −3.93e-3, −9.44e-4, −3.02e-5,
−1.67e-5, +1.61e-5, −7.92e-8.

### 5.3 The residual's continuity (prompt §2 item 2; note 03 ruling 11 as amended by ruling 16)

`scan.py` in my scratch directory. Each scan is 21 integrations evenly spaced in
`normalization_coordinate` (ln V₀ for this model) about the tuned normalization n* of §5.2, with the
tuned φ_ref fixed; residual_H from `_tuning_state`. The half-width was set by a two-point pilot at
α = 0.1 so that residual_H spans ±10⁻³ (4.50e-3 at β = 2, 5.00e-3 at β = 5), and the α = 0 baseline
and the comparison (α = 0.1 with `gate_weight` patched to 1.0 by `mock.patch`, 02's rule
α_eff = α) use the same 21 points. "dev" is the largest |residual_H − cubic| over the scan, in units
of the tuning tolerance 10⁻⁵. Load 4–24.

| β | scan | residual_H span | max dev / tol | wall-side count over the scan |
|---|---|---|---|---|
| 2 | α = 0.1, the gate | −9.99e-4 … +1.00e-3 | **0.039** | 29 throughout |
| 2 | α = 0 (baseline) | −1.00e-3 … +1.00e-3 | **0.052** | 26 throughout |
| 2 | α = 0.1, α_eff = α | −1.00e-3 … +1.00e-3 | **0.035** | 28 throughout |
| 5 | α = 0.1, the gate | −9.99e-4 … +1.00e-3 | **1.2e-3** | 173 throughout |
| 5 | α = 0 (baseline) | −9.99e-4 … +1.00e-3 | **1.8e-3** | 124 → 125 once, at residual_H = −1.0e-4 (dev there +3.3e-4) |
| 5 | α = 0.1, α_eff = α | −9.99e-4 … +1.00e-3 | **1.6e-3** | 90 ↔ 91, six changes (dev at them ≤ 7.3e-4) |

**The threshold clause was not exercised by the ±10⁻³ scans at α = 0.1**: the gated count is
constant on both. So I searched for one (ruling 11): widen the bracket outward from n*, doubling,
at most 6 integrations per side, until the wall-side count differs between its ends; bisect to the
main scan's width; scan 21 points across it, with the gate and under the comparison patch.
- **β = 2: no change found** in 13 integrations, out to ±0.144 in ln V₀ (residual_H from −3.1% to
  +3.3%): 29 wall-side turning points throughout. The search stopped there, as ruled.
- **β = 5: found** after 12 integrations, 173 → 172 between ln V₀ = −280.85846 and −280.85346
  (residual_H ≈ +1.6%). The scan across it, at the same half-width 5.00e-3:

| β = 5, across the threshold | residual_H span | max dev / tol | count change | dev / tol either side of it |
|---|---|---|---|---|
| α = 0.1, the gate | +1.567e-2 … +1.772e-2 | **2.0e-3** | 173 → 172 at ln V₀ = −280.856961 | −4.0e-6 → −3.2e-4 |
| α = 0.1, α_eff = α | +1.567e-2 … +1.772e-2 | **2.6e-3** | 90 ↔ 91, four changes | ≤ 8.5e-4 |

- **The residual's jump at the gated count change** is not resolved: the step in residual_H across
  it is 1.0237e-4, against 1.0236e-4 before it and 1.0240e-4 after it. Any jump is below about
  10⁻³ of the tolerance (10⁻⁸ in residual_H), the cubic's scatter.
- **All six main scans and both threshold scans deviate from their cubic by less than a tenth of the
  tuning tolerance**, and the α = 0.1 scans by no more than the α = 0 baseline's order (0.039
  against 0.052 at β = 2; 1.2e-3 against 1.8e-3 at β = 5). The acceptance clause holds, and no stop
  of prompt §5 is met.
- The count changes the scans did cross (α = 0 and the comparison at β = 5) show no jump either. The
  scatter at β = 2 (0.04 tol, 4e-7 in residual_H) is the integrator's: it is the same at α = 0.

## 6. The deliberate-breakage record (P11)

- Each diff was written against the final tree by `make_breakages.py` (difflib), checked with
  `git apply --check`, applied, checked with `git apply -R --check`, run in bash
  (`replay_breakages.sh`) through `test_gate_and_taper` and `test_injection` (B8: the golden test,
  the tuned golden test and item 1), reverted, and the source's md5 compared with the final tree's.
  All restored. None is committed.
- B2, B3, B4 and B7 were the stopping rule's and are dropped (R16). B9 is 02's, replayed to show
  item 11 still catches it on β = 10.
- Item 9 and test_injection's item 11 also fail under B1, B5 and B6: those change the β = 5 and
  β = 10 trajectories (B1 drains every wall at α; B5 and B6 inflate 𝒜_k so no wall is below 10⁻²),
  so the small-𝒜 walls and the pair are gone. Each breakage's own failure is named first.

**B1 — w_gate = 1.** It fails 3 tests:
- `test_2_gate_weight_against_hand_values`: `1.0 != 0.0` at 𝒜 = 0;
- `test_9_a_wall_side_turning_point_with_small_A_k_is_gated_off`: `1.0 not less than 0.0001`, the
  candidate at N = 36.846 (α_eff = 0.1);
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded` (incidental: the β = 10
  trajectory changes).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -817,6 +817,7 @@
         raise ComputationFailureError(
             f"gate_weight: the non-adiabaticity must be finite and >= 0, not {A!r}"
         )
+    return 1.0
     A0: float = NONADIABATICITY_SCALE
     if A > A0:
         r: float = A0 / A
```

**B5 — 𝒜 without H_E** (|d ln m²/dN|/2m). It fails 3 tests:
- `test_3_the_nonadiabaticity_against_a_reference`: |Δw_gate| = 0.0622 > 0.05 at the plateau
  turnaround N = 29.163 (loop 𝒜_k = 9.3e45, reference 3.88);
- `test_9_...`: no wall-side point with 𝒜_k < 10⁻² is left ("0 not greater than or equal to 1");
- `test_11_a_plateau_and_wall_pair...` (incidental).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -796,7 +796,7 @@
     """
     d_log_m2 = np.gradient(log_m2_samples, N_samples)
     with np.errstate(over="ignore", invalid="ignore"):
-        A = 0.5 * H_samples * np.abs(d_log_m2) * np.exp(-0.5 * log_m2_samples)
+        A = 0.5 * np.abs(d_log_m2) * np.exp(-0.5 * log_m2_samples)
     if not np.isfinite(A).all():
         raise ComputationFailureError(
             f"step_nonadiabaticity: a non-finite non-adiabaticity on the step "
```

**B6 — the running maximum not reset at a turning point.** It fails 3 tests:
- `test_3_...`: |Δw_gate| = 0.0621 at the plateau turnaround N = 29.163 (loop 𝒜_k = 1.44e3, the
  first bounce's, carried; reference 3.88);
- `test_9_...`: no wall-side point with 𝒜_k < 10⁻² is left;
- `test_11_a_plateau_and_wall_pair...` (incidental).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1040,7 +1040,6 @@
         """
         nonlocal running_max, boundary_N
         A_k: float = 0.0 if running_max is None else running_max
-        running_max = None
         boundary_N = float(N_end)
         return A_k
 
```

**B8 — the 𝒜 computation run at α = 0 too.** The golden histories still pass (`test_golden_history`
3 tests OK, the tuned golden test OK): the samples change no value. Item 1 fails:
`test_1_nothing_of_prompt_03_runs_at_alpha_zero`: `12330 != 0 : log_m_eff_squared` (10 per
accepted step on the β = 1.5 history's 1 233). **Its cost**: golden 10.25 → 15.15 s (+48%), tuned
25.0 → 37.5 s (+50%), at a load of 6–8 (§4.4).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1213,7 +1213,7 @@
         # the non-adiabaticity at the step's ten samples (prompt 03), at alpha > 0 only
         N_samples = None
         A_samples = None
-        if alpha > 0.0:
+        if True:
             n_samples: int = TURNING_POINT_INTERIOR_SAMPLES + 2
             N_samples = np.empty(n_samples)
             N_samples[0] = t_low
```

**B9 — detection at the step ends only** (02's, replayed on item 11's new history, β = 10). It
fails 2 tests:
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded`: no step holds two turning points;
- `test_11_no_step_holds_more_sign_changes_than_the_loop_recorded`: step 9333 shows 2 changes at 17
  points, and the loop recorded 0.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1239,8 +1239,8 @@
                 fold(N_samples, A_samples, float("inf"))
             return []
 
-        Ns = [t_low] + [float(x) for x in N_interior] + [t_high]
-        pis = [pi_low] + [float(x) for x in pi_interior] + [pi_high]
+        Ns = [t_low, t_high]
+        pis = [pi_low, pi_high]
 
         # the sign changes between consecutive samples: (j_before, j_after, j_zero or None, kind),
         # where j_zero is the first sample exactly 0.0 since the last non-zero one
```

**B10 — the mass without its trace term.** It fails 3 tests:
- `test_3_the_nonadiabaticity_against_a_reference` (by D3's check): m_eff/H_E off by 9.5e-9
  relative at the first wall-side turning point, against 1.0e-13. Before D3 it passed (|Δw_gate| ≤
  2.7e-3, though 𝒜_k differed by a factor up to 6.7e31 on the plateau side);
- `test_2_the_mass_against_a_hand_value`: ln m_eff² = 0.0 (m² = 1), not ln 8;
- `test_2_the_mass_at_a_generic_state`: off by 18%.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -757,7 +757,7 @@
     q_A: float = model.d2_logOmega_dphi2(phi_Einstein) + d1_A * d1_A
 
     try:
-        q_trace: float = q_A * (Sigma + exp(log_fm))
+        q_trace: float = 0.0
     except OverflowError as e:
         raise failure("overflow in f_m") from e
 
```

## 7. Observations not acted on

- **Prompt §0.3's B2 arithmetic** (§1.4): a sum over the drains as made omits about 10ε, not 100ε,
  on its own example. Moot since ruling 16.
- **𝒜_k is set on the plateau side.** As the planner's probe found, the largest 𝒜 of a
  half-oscillation is where m_eff is smallest, on the plateau side, up to 1.4e3 at the first bounce;
  w_gate is 1 there whatever the sampling. Where 𝒜_k is of order 1 (the gate's sensitive
  range) the 10-point sampling is within 3% of the 33-point one (item 3). Nothing suggests another
  𝒜₀; it is the user's.
- **m_eff/H_E < 1 at plateau turnarounds**: 6 of 6 at β = 1.2, 8 of 27 at β = 2, 2 of 154 at β = 5,
  0 of 295 at β = 25 (§5). That is where U4 (b′) says a quantum is not defined; no drain is made
  there. This is the diagnostic U4 (b′) asked for, recorded on every `TurningPoint`; 07 reports it.
- **test_injection's test_4 docstring** says every ratio on the β = 2 history measured 0.0. Under the
  gate one is 1.43e-14 (115 ε bound), one ulp of the exponent as at β = 25 in 02. The test passes;
  the docstring is stale, and editing it is outside ruling 10.
- **The interior pair moved to β = 10.** It now depends on that trajectory. Any later change to the
  drains (04's records do not change them) may move it again; the B9 test then needs another search.
- **Cost.** At α = 0.1 a history costs 1.4–1.9 × what it did, about 180 µs per accepted step for ten
  policy calls. It does not double, so the linear ln H_E option is left for later if 05's builds
  need it.

## 8. State handed to the next prompt

- **The records** (`ComputeTargets/ScalarHistory.py`, module level):
  - `TurningPoint(N, phi_Einstein, log_T_Jordan, kind, nonadiabaticity, m_eff_over_H)`; the last
    two `None` at α = 0, else 𝒜_k of the half-oscillation that ends at it and m_eff/H_E at it.
  - `Injection(N, kind, N_turn, phi_turn, pi_before, pi_after, log_rho_kin, alpha_eff, delta_K,
    delta_rho_over_rho_rad, H_Einstein_before, H_Einstein_after, nonadiabaticity, w_gate)`, one per
    candidate at α > 0 (drain points and floor reflections), α_eff = 0.0 included. α_eff = α · w_gate.
    04 appends λ, k_ex, m_ta and ln q; there are no placeholders.
  - `IntegrationResult(..., turning_points, injections, dropped_drains)`; the payload carries
    `"turning_points"`, `"injections"`, `"dropped_drains"` and `"dark_radiation"`. None is stored.
- **Generations for 04**: the records with α_eff > 0.0, which are exactly the ledger's injections
  (item 7). On every history measured all candidates had α_eff > 0 (the smallest, at β = 25, is
  about 0.1 × (5.5e-5)² ≈ 3e-10).
- **The mass function 04 reuses**: `log_m_eff_squared(model, phi_Einstein, log_rhorad_Einstein,
  log_fm, Sigma)` → ln m_eff², with m_eff² = V″ + [(ln A)″ + (ln A)′²] ρ_R,E (Σ + f_m). It takes a
  `RealizedModel` and a stored sample's columns (φ, ln ρ_R,E, ln f_m, Σ), no cosmology, no N. It is
  not `AdiabaticComputePolicy.M2eff_over_H2`. m_eff² ≤ 0 raises `ComputationFailureError`.
- **Where α_eff is set**: `alpha_effective(nonadiabaticity)` inside `integrate_scalar_history`;
  `candidate()` computes H_E and ln ρ_kin,E first and records every candidate.
- **The cost**: nothing at α = 0; at α > 0 about 180 µs per accepted step (10 policy calls), 1.4–1.9
  × the history's time.
- **Constants**: `NONADIABATICITY_SCALE = 1.0`. There is no stopping rule (ruling 16); 05's P7
  `omitted_fraction` column is gone with it (README §6.1).
- **The counts** at M = 0.5 M_P, α = 0.1, untuned: candidates (all drained) 7, 28, 155 and 296 for
  β = 1.2, 2, 5 and 25; Σ Δρ/ρ_R = 1.50e-2, 1.24e-2, 1.97e-3 and 4.19e-4; r_χ today 8.8e-3, 6.9e-3,
  8.0e-4 and 1.65e-4. None dropped, no floor event, no empty half-oscillation.
- **Tests**: `test_gate_and_taper.py` (12); test_injection's item 11 is on β = 10.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 317 (96 + 221), ObjectFactories
  103, tools 115, RunDrivers 64. `CLAUDE.md`'s counts are 08's.
