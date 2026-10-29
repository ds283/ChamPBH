# Log 04 — the wall through the model

**Subject:** Add the wall and the minimum to the model interface
**Commit:** this commit, on branch `inelastic-reflection`, on top of `a8e0406`
**Date:** 2026-10-29, by Claude Opus 5.5
**Result:** **COMPLETE.** The gate "Wall" holds, as prompt §0.2 reads it. One issue opened.
- **Four methods on `AbstractModel`**, generic defaults through the hooks alone (README §8.1 R9), each
  mirrored on `RealizedModel`:
  - `classical_turning_point`;
  - `one_loop_turnaround`;
  - `minimum_on_trace`;
  - `d_minimum_d_log_trace`.

  `ExponentialModel` overrides none of them. Two namedtuples, `ClassicalTurningPoint` and
  `OneLoopTurnaround`, sit beside `Seed`.
- **The gate "Wall"**:
  - φ_t, ln 𝓡 and k_ex equal the exponential closed form at 12 points (the largest errors are
    7.4e-15, 3.3e-12 and 1.4e-14, against 1e-12, 1e-10 and 1e-12);
  - the notes' chain is reproduced: classical y_t = 105.307 (the notes print 105), one-loop y = 39.080,
    Λ = 53.792, k_ex = 5.625×10¹³ GeV (the notes print 39.1, 53.8 and 5.6×10¹³);
  - the n = 2 exponential and a test-only power-law wall satisfy every defining equation within
    rounding bounds derived from the operations;
  - the minimum agrees with `effective_minimum` and with its own equation, and its derivative
    agrees with a finite difference to ≤ 4.2e-8.
- **A finding for 08** (§5.2): **the one-loop chain has no solution just above 𝓡 = 1.** At the worked
  example's E it stops at M = 10^−16.109 M_P, where 𝓡 = 10^2.92, about 0.73 decades of M below the
  point where 𝓡 = 1. There ℓ falls to 0.24 and the fixed point disappears (a saddle-node). Prompt
  §1.3 expected the chain to reach below 𝓡 = 1. So 08's rule ("classical below 𝓡 = 1, one-loop
  above") has no one-loop estimate for 1 < 𝓡 ≲ 10³, and the jump at 𝓡 = 1 that prompt §2.2 asks
  for cannot be measured there. It is measured at the chain's edge instead: Δ ln k_ex = −0.039.
  Opened as `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`.
- **The integrator is untouched.** No change to `ComputeTargets/ScalarHistory.py` or to any file of
  the integrator, the policies, the records or the payload. The golden tests passed unchanged in the
  "after" suites.
- **Counts:** 26 / 317 (96 + 221) / 103 / 115 / 64 before; 26 / **339** (96 + **243**) / 103 / 115 / 64
  after, all passing. The 22 new tests are in `ComputeTargets/tests/test_wall_model.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was
  regenerated.

## 1. The physics and the numerics

### 1.1 The classical turning point (prompt §1.2; `sec:stop`, `sec:chain`, `sec:loop`)

A field at φ falls in with kinetic energy E = ρ_kin (Einstein frame, at the infall), and
|φ̇_in| = (2E)^{1/2}. The crossing takes far less than a Hubble time, so expansion is neglected, as
the notes neglect it. Energy conservation without back-reaction stops the field where
V(φ_t) − V_floor = E: the classical wall.
- **The root** is searched inwards from the field in x = ln φ, on ln(V − V_floor) − ln E. The
  method forms ln(V − V_floor) as ln V + ln(−expm1(ln V_floor − ln V)), which is −∞ where V rounds
  to V_floor (§1.5).
- **V″** = V[(ln V)″ + (ln V)′²], from the three V hooks, as ln ω_t² = ln V + ln q with
  q = (ln V)″ + (ln V)′². q ≤ 0 at φ_t raises.
- **The test-field ratio**: 𝓡 = ω_t⁴/8π²E. This is the first paragraph of `sec:chain`, ρ_naive/ρ_kin
  with ρ_naive = ω_t⁴/8π², and so ln 𝓡 = 2 ln ω_t² − ln 8π² − ln E. `sec:loop` shows 𝓡 = 2ε at the
  classical wall, where ω_t² = λ²E for the exponential.
- **The classical peak.** `eq:kex-over-wt` gives k_ex = λ(φ_ta)|φ̇_in|/2√2. Prompt §1.2 applies it
  at the classical wall (y = L): k_ex = λ_t (2E)^{1/2}/2√2 = λ_t E^{1/2}/2, with λ = −d ln V/dφ, the
  notes' λ (prompt §0.2). Then ℓ_t = ln k_ex − ½ ln ω_t². For the exponential,
  ω_t² = λ_t²E(1 + 2/y), so ℓ_t = ln ½ − ½ ln(1 + 2/y), which is −0.70 here. The classical regime's
  ℓ is negative by construction (R13).

### 1.2 The one-loop turnaround (prompt §1.3; `eq:chain-dress`, `eq:peak-is-ta`, `eq:chain-turn`, `sec:params`)

**The condition.** Along the back-reacted approach the dressing is
ρ_dress = m⁴ ln(k_ex/m)/32π², with m² = V″ (`eq:chain-dress`). For M ≪ M_crit the potential and the
production are negligible at the turning point, so the field stops where ρ_dress(m_ta) = E
(`eq:chain-turn`):

  m_ta⁴ = 32π²E/ℓ,  ℓ = ln(k_ex/m_ta).

- **No renormalization scale** enters (R7): the logarithm's ends are k_ex and m_ta.
- **The position.** V″(φ_ta) = m_ta². On the approach V″ rises monotonically (`sec:chain`, before
  `eq:peak-is-ta`), so the first crossing inwards from the field is the turning point. The root is
  found on ln V″ − 2 ln m_ta, the same search as §1.1.
- **The peak.** `eq:kex-over-wt` with λ at φ_ta: k_ex = λ_ta E^{1/2}/2.
- **The fixed point.** ℓ fixes m_ta, which fixes φ_ta, which fixes λ_ta, k_ex and so ℓ. The method
  iterates from ℓ₀ = 32 until |ℓ_new − ℓ| ≤ 10⁻¹² ℓ_new, within 100 iterations, and returns
  ℓ_new = ln k_ex − ln m_ta of the returned values.
- **The exponential reduction.** For the exponential this is `sec:params` exactly:
  V″ = y⁴e^y(1 + 2/y)V₀/M² turns V″ = m_ta² into y = Λ − 4 ln y − ln(1 + 2/y) with
  Λ = ln(m_ta²M²/V₀) (`eq:y-Lambda`). λ = y²/M gives k_ex = y²E^{1/2}/2M (`eq:kex-Ts`). The method
  does not use this reduction; the test does (§1.6 item 2).
- **Where the chain has no solution.** An iterate with ℓ ≤ 0 raises, and so does non-convergence,
  each with ℓ's values in the message.
- **`outside_classical`** is φ_ta > φ_t. Since V − V_floor rises inwards (the search checks this
  between the field and each root), it equals V(φ_ta) − V_floor < E, one evaluation. Where the
  classical point is behind the field, V(φ) − V_floor ≥ E, so V(φ_ta) − V_floor ≥ E and the answer is
  False, as ruling 4 says (D3).
- **None** if V″ at the field is already at or above m_ta² at an iterate (ruling 4).

### 1.3 The minimum and its response (prompt §1.4)

ODERHS's force is −V′ − (ln A)′ T, with T = ρ_R,E(Σ + f_m), the trace (log 03 §1.1). A parked field
sits where

  V′(x) + (ln A)′(x) T = 0.

- **Why this equation.** With ρ̃_c = T/A(φ_min), it is V′ + (ln A)′ A ρ̃_c = 0 at x = φ_min.
  `effective_minimum` solves V′ + (ln A)′ ρ_m,E(φ₀) A(x)/A(φ₀) = 0, and the two agree at x = φ₀ =
  φ_min when T = ρ_m,E(φ₀).
- **Solved in logs:** g(x) = ln V + ln(−(ln V)′) − ln (ln A)′ − ln T, which falls as x grows for a
  reflecting wall with (ln A)′ > 0. From the caller's start the method searches outwards if g > 0
  (on −g) and inwards if g < 0. g = −∞ where V′ ≥ 0.
- **∂φ_min/∂ ln T** by implicit differentiation at fixed T:
  −(ln A)′T/(V″ + (ln A)″T) = −1/(V″/((ln A)′T) + (ln A)″/(ln A)′). The first ratio is formed in logs.
  **The denominator has no (ln A)′² term.** `log_m_eff_squared` has one: it is the curvature at fixed
  ρ̃_c, where A(x)ρ̃_c varies with x, while here T is held. The docstring says so for 06.
- **phi_ref does not enter.** The equation reads (ln A)′ and (ln A)″, never ln A, and `RealizedModel`
  passes them through unshifted (ruling 5).

### 1.4 The search (prompt §1.5; rulings 2 and 3)

`_search_log_field(residual, x_0, r_0, direction, context)` serves all three roots. The residual
is negative at the start and rises towards 0 along the search.
1. **Doubling.** Probes at x_0 ± 1, ± 3, ± 7, …: the step doubles until the first probe with
   residual ≥ 0, at most 64 times.
2. **Monotonicity on values** (ruling 3). Each probe's residual must exceed the previous one's;
   repeated −∞ is allowed. A violation raises, naming every probe and its value.
3. **Halving.** The last bracket is halved **4 times**, each midpoint required to lie strictly
   between its ends. Doubling stops at the first probe with the inner sign, so a final bracket of
   2^k e-folds was seen by its ends only. Four midpoints sample it at 1/16 of its width before
   brentq, at four evaluations' cost. It is halved further, at most 64 more times, while the outer
   end is −∞, because brentq's interpolation cannot use an infinite value.
4. **brentq** with rtol = 4ε_mach and xtol = 10⁻¹⁴ (`WALL_ROOT_RTOL`, `WALL_ROOT_XTOL`, ruling 2).
   SciPy stops once the sign change is bracketed within xtol + rtol|x|, so the root is within that
   of the true one, up to the residual's own rounding.
5. **Float range** (ruling 3). A probe whose φ = e^x is not a positive normal float, a hook that
   raises (any `Exception`, chained), or a residual of NaN or +∞ raises `ComputationFailureError`
   naming the probe.

Every error names the method, the model, the normalization, the inputs and the reason.

### 1.5 The rounding bounds of item 3 (ruling 8)

Each bound has three parts:
- the residual's sensitivity S to x = ln φ, times the root's tolerance τ(x) = 10⁻¹⁴ + 4ε|x|;
- the rounding R of the residual's own evaluation;
- for m_ta⁴ℓ, the iteration's stopping tolerance.

ε is the machine epsilon.
- **V(φ_t) − V_floor = E**, residual ln W − ln E.
  - S = |φ(ln V)′| V/W, from the hooks.
  - R: the hooks' ln V is rounded within 2.5ε(|ln V| + |norm|) on both models (the power or log1p,
    the division, the sum). ln(V − V_floor) adds an expm1 and a log, whose condition in
    ln V − ln V_floor is V_floor/W < 1, since E > V₀. The subtraction adds ε|ln E|/2. Bound:
    R = 6ε(|ln V| + |norm| + |ln E| + 1).
- **V″(φ_ta) = m_ta²**, residual ln V″ − 2 ln m_ta.
  - S = |d ln V″/dx|. It needs (ln V)‴, which no hook gives, so the test takes a centred difference
    with h = 10⁻³ (O(h²) relative, enough for a bound).
  - R = 6ε(|ln V| + |norm| + |ln E| + |ln q| + 4); ln q and 2 ln m_ta add a few ε each.
- **m_ta⁴ℓ = 32π²E**, residual 4 ln m_ta + ln ℓ − ln 32π² − ln E, with ℓ recomputed as ln k_ex − ln m_ta
  (ruling 9). The method stops when |ℓ_new − ℓ| ≤ rℓ_new, r = 10⁻¹², and returns ℓ_new with the m_ta
  of ℓ. So |ln(ℓ_new/ℓ)| ≤ −ln(1 − r) ≤ r(1 + r). Bound:
  r(1 + r) + 4ε(4|ln m_ta| + |ln ℓ| + |ln E| + 8).
- **k_ex = λE^{1/2}/2** and ℓ = ln k_ex − ln m_ta are recomputed identities. Their rounding is
  4ε (and ε) times the sizes of the logs that enter. λ itself is asserted equal to −(ln V)′ from the
  hook, exactly.
- **V′(φ_min) + (ln A)′T = 0**, residual g.
  - S = |φ(q/(ln V)′ − (ln A)″/(ln A)′)|.
  - R = 6ε(|ln V| + |norm| + |ln(−(ln V)′)| + |ln (ln A)′| + |ln T| + 2).

The measured residuals, each against its bound (field 0.1 M_P, norm −279.87):

| Model, M/M_P, ln E | V − V_f = E | V″ = m_ta² | m_ta⁴ℓ = 32π²E | k_ex | ℓ, iterations |
|---|---|---|---|---|---|
| exp n = 2, 1e-30, −180 | 3.1e-13 / 1.6e-11 | 1.7e-13 / 7.5e-12 | 2.8e-14 / 1.3e-12 | 0 / 2.4e-13 | 29.03, 7 |
| exp n = 2, 1e-30, −120 | 2.0e-13 / 2.4e-11 | 2.6e-13 / 1.2e-11 | 0 / 1.2e-12 | 0 / 1.9e-13 | 44.93, 7 |
| exp n = 2, 1e-20, −180 | 3.4e-13 / 1.1e-11 | 3.7e-13 / 1.0e-11 | 1.4e-13 / 1.3e-12 | 0 / 2.4e-13 | 6.75, 10 |
| exp n = 2, 1e-20, −120 | 1.2e-12 / 1.8e-11 | 7.9e-13 / 1.3e-11 | 5.5e-13 / 1.2e-12 | 0 / 1.6e-13 | 22.49, 7 |
| power p = 2, 1e-30, −180 | 0 / 1.1e-12 | 1.4e-14 / 1.5e-12 | 5.7e-14 / 1.3e-12 | 0 / 2.4e-13 | 36.29, 6 |
| power p = 2, 1e-30, −120 | 0 / 9.8e-13 | 2.1e-14 / 1.5e-12 | 2.8e-14 / 1.2e-12 | 0 / 2.1e-13 | 58.85, 6 |
| power p = 2, 1e-10, −180 | 0 / 1.0e-12 | 2.8e-14 / 1.3e-12 | 6.5e-13 / 1.3e-12 | 0 / 2.4e-13 | 13.14, 7 |
| power p = 2, 1e-10, −120 | 0 / 9.0e-13 | 2.8e-14 / 1.3e-12 | 5.7e-14 / 1.2e-12 | 0 / 1.7e-13 | 35.76, 6 |

- ℓ recomputed equals the returned ℓ exactly at every point, and every point has `outside_classical`
  True.
- The minimum's residual is at most 1.1e-13 against bounds of 1.2e-12 to 4.2e-12, at the same models
  and ln T ∈ {ln ρ_m0 = −277.43, −200}.
- **The bounds were not changed after measurement.** One bound of my own was wrong when first
  written: the start-independence test of the minimum (D7). It is not one of item 3's bounds.

## 2. What shipped

### `CosmologyConcepts/Models/AbstractModel.py`

- **Module level:**
  - `ClassicalTurningPoint(phi_t, log_omega_t_squared, log_R, lambda_t, log_k_ex, ell_t)`;
  - `OneLoopTurnaround(phi_ta, log_m_ta, lambda_ta, log_k_ex, ell, iterations,
    outside_classical)`;
  - the constants `WALL_ROOT_RTOL` (4ε), `WALL_ROOT_XTOL` (10⁻¹⁴), `WALL_SEARCH_MAX_DOUBLINGS` (64),
    `WALL_SEARCH_HALVINGS` (4), `ONE_LOOP_ELL_START` (32), `ONE_LOOP_ELL_RTOL` (10⁻¹²) and
    `ONE_LOOP_MAX_ITERATIONS` (100);
  - the private helpers `_failure`, `_log_V_minus_floor`, `_evaluate` and `_search_log_field`.
- **`AbstractModel`:**
  - `classical_turning_point(normalization, phi, log_E)`;
  - `one_loop_turnaround(normalization, phi, log_E)`;
  - `minimum_on_trace(normalization, phi_start, log_T)`;
  - `d_minimum_d_log_trace(normalization, phi_min, log_T)`;
  - the private `_wall_context`, `_check_wall_inputs` and `_wall_floor`.
- **`RealizedModel`:** the four mirrors, without the normalization, each one line.
- **`ComputationFailureError`** is imported inside `_failure`, with a docstring naming the import
  cycle (ruling 1). `CosmologyConcepts/Models/__init__.py` is unchanged.
- The module docstring gains a paragraph naming the four methods.

### The tests (`ComputeTargets/tests/test_wall_model.py`, new; 22 tests, about 2.2 s plus two subprocesses, at a load of about 6)

| Test | Asserts | Tolerance |
|---|---|---|
| `test_1_phi_t_ln_R_and_k_ex_equal_the_closed_form` | at M ∈ {1e-30, 1e-16, 1e-10, 1e-7} M_P × ln E ∈ {−180, −120, −60}: E/V₀ > 1; φ_t = M/ln(1 + E/V₀); ln 𝓡 with ω_t² = (E + V₀)(y⁴ + 2y³)/M²; k_ex = (y²/M)E^{1/2}/2; ℓ_t = ln k_ex − ½ ln ω_t² | 1e-12 rel; 1e-10 abs; 1e-12 rel; exact |
| `test_2_the_classical_wall` | worked example: y_t = ln(1 + E/V₀), and rounds to 105 | 1e-12 rel |
| `test_2_the_one_loop_turnaround_is_the_notes_chain` | y, ℓ, ln k_ex and ln m_ta against the test's own loop over `sec:params`' equations; `outside_classical`; more than one iteration | 1e-8 rel |
| `test_2_the_notes_printed_values` | the test's chain: y = 39.1, Λ = 53.8, k_ex = 5.6×10¹³ GeV | ± 0.05, ± 0.05, ± 3 % |
| `test_3_the_classical_turning_point` | n = 2 and power-law: V(φ_t) − V_floor = E; ln ω_t² = ln V″; ln 𝓡; λ_t = −(ln V)′; ln k_ex | §1.5 bounds; λ exact |
| `test_3_the_one_loop_turnaround` | V″(φ_ta) = m_ta²; ℓ recomputed; m_ta⁴ℓ = 32π²E; λ_ta = −(ln V)′; ln k_ex; `outside_classical` = φ_ta > φ_t | §1.5 bounds |
| `test_3_the_minimum` | V′ + (ln A)′T = 0 on both models at ln T ∈ {ln ρ_m0, −200} | §1.5 bound |
| `test_4_the_residual` | the same, at M = 1e-30 and 0.5 M_P on the exponential (n = 1) and the power-law wall with γ ≠ 0 | §1.5 bound |
| `test_4_the_root_does_not_depend_on_the_start` | starts at 1e-6, 0.5, 3 and 1e4 × φ_min find the same root | 2(τ + R/S) in ln φ |
| `test_4_effective_minimum_agrees_today` | at T = ρ_m,E today, Σ = 0, `effective_minimum(norm, phi_ref, φ_min, T)` returns φ_min: the four models above and test_model_interface (f)'s three states | 1e-13 + 1e-14 + τ(ln φ_min) rel |
| `test_4_the_derivative_against_a_finite_difference` | ∂φ_min/∂ ln T < 0 and equals a centred difference, h = 10⁻³ | 1e-6 rel |
| `test_5_a_model_that_does_not_reflect_raises` | all four methods raise, naming the model | — |
| `test_5_a_model_without_a_floor_raises_at_the_turning_points` | the two turning points raise; the minimum does not need the floor | — |
| `test_5_E_not_positive_raises` | ln E or ln T = −∞, NaN, +∞ raise, in all four | — |
| `test_5_a_field_not_finite_and_positive_raises` | φ = 0, −0.1, NaN, ∞ raise, in all four | — |
| `test_5_a_field_inside_its_turning_points_returns_None` | between φ_t and φ_ta: one-loop `None`, classical unchanged; inside φ_t: both `None` | 1e-13 rel |
| `test_5_a_one_loop_call_without_a_solution_raises` | worked E at M = 1e-16 M_P (𝓡 = 10^2.49): raises "no one-loop turnaround" with ℓ's history | — |
| `test_5_a_search_that_leaves_float_range_raises` | power-law wall with p = 0.1 (φ_t ≈ e^−1600 M): "float range" | — |
| `test_5_a_bump_between_the_field_and_the_wall_is_refused` | `_BumpWall` raises "not monotone"; the same wall without the bump succeeds | — |
| `test_6_each_method_delegates` | each `RealizedModel` mirror calls the model's method once with (model, normalization, φ, ln X) and returns its result (`mock.patch.object(..., autospec=True)`) | identity |
| `test_7_the_models_import_alone` | `import CosmologyConcepts.Models` in a fresh interpreter from the repository root exits 0 | — |
| `test_7_the_models_import_after_ComputeTargets` | `import ComputeTargets; import CosmologyConcepts.Models` exits 0 | — |

The test-only models are in the module:
- `_PowerLawWall`: V = V₀(1 + (M/φ)^p), ln A = βφ/M_P + γφ²/M_P² (ruling 6), hooks written there;
- `_BumpWall`: a power-law wall with a bump at ln φ = −7 (ruling 3).

The power-law's γ is β/φ_c, with φ_c its γ = 0 root at T = ρ_m0. That makes (ln A)″T 17 % of the
denominator at the minimum, so dropping it moves the derivative by 0.166 (B4).

### The documents (P12)

A dated note in `.documents/architecture-summary.md` §8.4a: the four methods, the two records,
generic and overridable, and what each needs and raises.

## 3. Deviations from the prompt

**The rulings of orchestrator note 04 (1–12):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | `ComputationFailureError` imported inside `_failure`, with a docstring naming the cycle; `__init__.py` unchanged; the namedtuples imported from `AbstractModel`. `test_7_*` runs both imports in subprocesses; B9 (a top-level import) fails the first with "cannot import name 'AbstractModel' from partially initialized module" | STRUCTURALLY REQUIRED |
| R2 | rtol = 4ε, xtol = 10⁻¹⁴, module constants with the reason. Item 1 uses the probe's 12 points and asserts E/V₀ > 1 at each. B7 (rtol = 10⁻¹³, SciPy's default xtol) fails item 1 at **(10⁻¹⁰, −180)**, not at (10⁻¹⁶, −120), where my search's bracket lands within tolerance (§6) | STRUCTURALLY REQUIRED |
| R3 | Doubling from the field; 4 halvings, more while the outer end is −∞ (§1.4); monotonicity on values at every probe and midpoint; float range, a raising hook and NaN/+∞ end the search with the hook's exception chained; the 64-doubling backstop; `_BumpWall` and B8 | IMPLEMENTATION CHOICE (ruled) |
| R4 | `None` when V″ at the field is at or above m_ta² at an iterate; `outside_classical` False where the classical point is behind; tested in `test_5_a_field_inside_*` | IMPLEMENTATION CHOICE (ruled) |
| R5 | The signatures take the normalization and no φ_ref; the mirrors pass `self._normalization`; the minimum's docstring says why φ_ref does not enter; `test_6_*` is a `mock` spy | IMPLEMENTATION CHOICE (ruled) |
| R6 | `_PowerLawWall` has ln A = βφ/M_P + γφ²/M_P²; item 4's residual and derivative run on it and on the exponential; B4 fails the derivative test on it (0.166 against 1e-6) | STRUCTURALLY REQUIRED |
| R7 | φ ≤ 0 or non-finite, and ln E or ln T NaN or ±∞, raise naming the model, inputs and reason; E ≤ 0 tested as −∞ and NaN (and +∞); the minimum checks `reflects_at_origin` and not the floor | IMPLEMENTATION CHOICE (ruled) |
| R8 | The bounds of §1.5, derived before measuring, each stated as a number in the test; none loosened; none failed | STRUCTURALLY REQUIRED |
| R9 | Item 3 recomputes ℓ and asserts it equals the returned ℓ and that m_ta⁴ℓ = 32π²E; B6 fails item 3 as well as item 2 | IMPLEMENTATION CHOICE (ruled) |
| R10 | The worked example by the test's own loop (`_the_notes_chain`), in Planck units through `units.GeV`; the printed values asserted within prompt §1.6's tolerances; m_ta² at ℓ = 32 and at the converged ℓ in §5.1 | STRUCTURALLY REQUIRED |
| R11 | Only the prompt's §6 files change, plus `.documents/OPEN_ISSUES.md` for the issue opened. `ExponentialModel.py`, `CosmologyConcepts/Models/__init__.py`, `ComputeTargets/ScalarHistory.py` and `test_model_interface.py` are byte-identical | as ruled |
| R12 | Costs are minima of 2 000 calls with `uptime` beside them; the suites serial, ComputeTargets in halves, output to my scratch directory, `uptime` beside each. All six runs went in one background script, and I waited for its notification without polling | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | The names: the methods `classical_turning_point`, `one_loop_turnaround`, `minimum_on_trace`, `d_minimum_d_log_trace`; the records as §2. The prompt proposed names and asked me to choose | IMPLEMENTATION CHOICE |
| D2 | `d_minimum_d_log_trace` takes the root φ_min and does not find it, so 06 can pass the φ_min it has (4.4 µs against 18 µs). Its docstring says it must be the root | IMPLEMENTATION CHOICE |
| D3 | `outside_classical` is decided as V(φ_ta) − V_floor < E, one evaluation, instead of a second root-find. It equals φ_ta > φ_t wherever V − V_floor is monotone between them, which the search checks from the field inwards; item 3 asserts it equals φ_ta > φ_t | IMPLEMENTATION CHOICE |
| D4 | The minimum raises if (ln A)′ ≤ 0 at any point the search evaluates, the root included, where the prompt says "at the root". Off the root the residual is undefined there | IMPLEMENTATION CHOICE |
| D5 | −∞ is a valid residual (V rounds to V_floor; V′ ≥ 0 in the minimum); NaN and +∞ raise. Any `Exception` from a hook is wrapped and chained, not only the arithmetic ones | IMPLEMENTATION CHOICE |
| D6 | `ONE_LOOP_MAX_ITERATIONS = 100`. Near the chain's edge the fixed point loses stability (f′ → 1 at a saddle-node), and the limit gives up within 0.004 decades of M of the true edge (§5.2) | IMPLEMENTATION CHOICE |
| D7 | Tests beyond the prompt's list: the root's independence of the start; the float range; the floor; the field checks; +∞. **My first bound for the start-independence test, 2τ, omitted the residual's rounding and failed** (2.30e-14 against 2.28e-14 in φ, power-law at M = 0.5). The correct bound is 2(τ + R/S), since each root lies within τ of a sign change of the rounded residual, which can sit R/S from the true root. Corrected before any suite run. It is not one of item 3's bounds | IMPLEMENTATION CHOICE |
| D8 | Item 3's n = 2 masses are 10⁻³⁰ and 10⁻²⁰ M_P. At 10⁻¹⁶, ln E = −180 the chain has no solution (𝓡 = 10^−2.9) | IMPLEMENTATION CHOICE |
| D9 | The test imports `cosmology` (for ρ_m0) from `test_model_interface.py` besides `exponential_model` and `rel` | IMPLEMENTATION CHOICE |
| D10 | Prompt §2.2's "jump in ln k_ex at 𝓡 = 1" cannot be measured: the one-loop chain has no solution at 𝓡 = 1. It is measured at the chain's edge and reported with the gap (§5.2) | STRUCTURALLY REQUIRED |
| D11 | B9 (a top-level import of the exception) is added to the breakages, to show ruling 1's test catches it | IMPLEMENTATION CHOICE |
| D12 | The development was done in a scratch worktree while the "before" suites ran, then copied into the main checkout; the worktree is removed | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT.

## 4. Verification performed

### 4.1 The suites

Serially, ComputeTargets in two halves, output in my scratch directory. "Before" at `a8e0406`
before any edit; "after" on the final tree.

| Suite | Before (count, time, 1-min load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 69 s, 5.9 → 6.4 | 26 OK, 68 s, 5.7 → 5.2 |
| ComputeTargets [a-f] | 96 OK, 127 s, 6.4 → 7.6 | 96 OK, 125 s, 5.2 → 5.6 |
| ComputeTargets [g-z] | 221 OK, 295 s, 7.6 → 8.0 | **243** OK, 296 s, 5.6 → 8.8 |
| ObjectFactories | 103 OK, 16 s, 8.0 → 7.4 | 103 OK, 17 s, 8.8 → 9.2 |
| tools | 115 OK, 132 s, 7.4 → 8.3 | 115 OK, 137 s, 9.2 → 25.4 |
| RunDrivers | 64 OK, 24 s, 8.3 → 8.0 | 64 OK, 25 s, 25.4 → 23.8 |

- The baselines are the dispatch's: 26 / 317 (96 + 221) / 103 / 115 / 64.
- ComputeTargets rose by 22, the new `test_wall_model.py`, which sorts into [g-z]. Nothing fell.
- **The golden tests passed unchanged**: `test_golden_history`, the tuned golden test,
  `test_first_bounce`, `test_surfing_turning_points` and `test_kinematic_cap_loop`, all in [g-z]
  or [a-f] above. Nothing in the integrator changed, and the golden JSONs are untouched.
- `black --check` on the two changed Python files: both unchanged.

### 4.2 Item 1 at every point

On the final tree; the tolerances are 1e-12 rel, 1e-10 abs and 1e-12 rel.

| M/M_P | ln E | φ_t rel | ln 𝓡 abs | k_ex rel |
|---|---|---|---|---|
| 1e-30 | −180 / −120 / −60 | 5.5e-16 / 3.5e-15 / 7.4e-15 | 1.1e-13 / 1.2e-12 / 3.3e-12 | 0 / 1.4e-14 / 1.4e-14 |
| 1e-16 | −180 / −120 / −60 | 1.9e-15 / 2.2e-15 / 5.3e-15 | 4.6e-13 / 7.1e-13 / 2.4e-12 | 7.2e-15 / 7.2e-15 / 7.1e-15 |
| 1e-10 | −180 / −120 / −60 | 1.4e-15 / 9.7e-16 / 1.3e-15 | 3.4e-13 / 3.4e-13 / 6.0e-13 | 0 / 0 / 0 |
| 1e-7 | −180 / −120 / −60 | 1.2e-15 / 8.3e-16 / 1.0e-15 | 2.8e-13 / 2.8e-13 / 4.7e-13 | 0 / 0 / 3.5e-15 |

- **The conditioning bound** of ruling 2 is ε|norm|/(n ln(1 + E/V₀)), with |norm| = 279.87 and
  ln(1 + E/V₀) = 100, 160 and 220. It gives 6.2e-16, 3.9e-16 and 2.8e-16 in φ_t.
- **The root's tolerance**, 10⁻¹⁴ + 4ε|ln φ_t|, is up to 8e-14 in φ_t and is not reached.
- The largest φ_t error, 7.4e-15, is about 26 × the conditioning bound but a tenth of the root's
  tolerance. brentq stops once the bracket is within tolerance, not at the conditioning limit. The
  largest ln 𝓡 error, 3.3e-12, is S·δx with S = 2(y + 4) ≈ 450, so δx ≈ 7e-15.

### 4.3 Item 4

| Model, M/M_P, ln T | φ_min/M_P | residual / bound | ∂φ/∂ ln T rel to the difference | without (ln A)″, rel | `effective_minimum` rel / tolerance |
|---|---|---|---|---|---|
| exp n = 1, 1e-30, ln ρ_m0 | 2.083e-16 | 0 / 1.2e-12 | 4.2e-8 | 0 | 8.4e-15 / 1.5e-13 |
| exp n = 1, 1e-30, −200 | 1.340e-31 | 2.8e-14 / 1.8e-12 | 8.3e-9 | 0 | — |
| power γ ≠ 0, 1e-30, ln ρ_m0 | 3.272e-21 | 5.7e-14 / 1.3e-12 | 1.1e-8 | **0.166** | 1.4e-15 / 1.6e-13 |
| power γ ≠ 0, 1e-30, −200 | 2.739e-32 | 2.8e-14 / 1.3e-12 | 1.9e-8 | 4.1e-12 | — |
| exp n = 1, 0.5, ln ρ_m0 | 0.3210 | 0 / 1.2e-12 | 3.2e-8 | 0 | 1.6e-14 / 1.2e-13 |
| exp n = 1, 0.5, −200 | 7.009e-3 | 0 / 2.0e-12 | 1.8e-10 | 0 | — |
| power γ ≠ 0, 0.5, ln ρ_m0 | 0.2061 | 0 / 1.2e-12 | 1.1e-8 | **0.166** | 1.8e-14 / 1.2e-13 |
| power γ ≠ 0, 0.5, −200 | 1.726e-12 | 0 / 1.1e-12 | 1.9e-8 | 4.1e-12 | — |

- ln ρ_m0 = −277.43 (Planck2018, Planck units).
- `effective_minimum` is checked only at ln ρ_m0, where Σ = 0 (prompt §1.6 item 4).
- test_model_interface (f)'s three states (M = 10⁻³ M_P) pass the same check. At β = 1.5, 5.5 and
  25 the differences are 1.9e-14, 1.2e-15 and 2.3e-15, against tolerances of about 1.2e-13.
- `effective_minimum` and the new minimum agree where they should (no stop).

### 4.4 The cost per call (prompt §2.3; ruling 12)

Minimum and mean of 2 000 single calls each, field 0.1 M_P, normalization −279.87, ln E = −120 and
T = ρ_m0. `uptime` 5.41 / 6.80 / 7.34 before and after.

| Method | M = 10⁻³⁰ M_P, min (mean) | M = 10⁻¹⁰ M_P, min (mean) |
|---|---|---|
| `classical_turning_point` | 24.3 µs (27.1) | 21.2 µs (23.0) |
| `one_loop_turnaround` | 167 µs (194), 7 iterations | 482 µs (513), 21 iterations |
| `minimum_on_trace`, start 0.1 M_P | 17.1 µs (19.0) | 18.2 µs (19.8) |
| `minimum_on_trace`, start 1.01 φ_min | 13.7 µs (15.5) | 16.4 µs (18.4) |
| `d_minimum_d_log_trace` | 4.4 µs (4.7) | 4.5 µs (4.8) |

- The classical call costs about 12 % of 03's ≈ 180 µs per accepted step at α > 0. 05 calls it on
  every infalling step.
- Building the error context (`_wall_context`, which formats the model's name) costs 2.9 µs of each
  call, even when nothing fails.
- At M = 10⁻¹⁰ the one-loop point (𝓡 = 10^3.6) is near the chain's edge, so it takes 21 iterations.

## 5. The measurement (prompt §2)

### 5.1 The worked example's chain (prompt §2.1)

M = 1 meV = 4.106×10⁻³¹ M_P, V₀ = 10⁻⁴⁸ GeV⁴, E^{1/4} = 1.18 × 0.23 = 0.2714 GeV; the field at
10⁻³ M_P.

| Quantity | This prompt | The notes | Where |
|---|---|---|---|
| classical y_t = M/φ_t | 105.3074 | 105 | `sec:stop` table |
| ω_t = V″(φ_t)^{1/2} | 8.246×10¹⁴ GeV | 8.5×10¹⁴ GeV | `sec:stop` table |
| λ_t | 1.109×10¹⁶ GeV⁻¹ | 1.1×10¹⁶ GeV⁻¹ | `sec:stop` table |
| classical k_ex, ℓ_t | 4.084×10¹⁴ GeV, −0.7026 | — | |
| 𝓡 | 10^60.03 | — | |
| one-loop y = M/φ_ta | 39.0800 | 39.1 (κ condition: 40) | `sec:params`, `eq:y-Lambda` |
| Λ | 53.7923 | 53.8 | `sec:params` |
| m_ta | 0.4796 GeV | — | |
| m_ta² at the converged ℓ | 0.2300 GeV² | — | |
| m_ta² at ℓ = 32 | 0.2314 GeV² | 0.25 GeV² | `eq:chain-turn` |
| λ_ta | 1.527×10¹⁵ GeV⁻¹ | 1.6×10¹⁵ GeV⁻¹ (at y = 40) | `sec:stop` table |
| k_ex | 5.6247×10¹³ GeV | 5.6×10¹³ GeV | `eq:kex-Ts` |
| k_ex/ω_t | 1/14.66 | 1/14 | `eq:kex-over-wt` |
| ℓ | 32.39563882540 | 32.4 | `eq:ell-exact` table |
| V/E at φ_ta | 1.7×10⁻²⁹ | 4×10⁻²⁹ (at y = 40) | `sec:stop` table |
| iterations | 6 (method, \|Δℓ\| ≤ 10⁻¹²ℓ); 7 (test's loop, 10⁻¹⁴) | "three steps" for y | |
| `outside_classical` | True | — | |

The fixed point from ℓ₀ = 32, by the test's loop:

| Iterate | ℓ in | y | Λ | ℓ out |
|---|---|---|---|---|
| 1 | 32 | 39.08554 | 53.79845 | 32.392852 |
| 2 | 32.392852 | 39.079995 | 53.792348 | 32.3956193 |
| 3 | 32.3956193 | 39.0799560 | 53.7923054 | 32.39563869 |
| 4 | 32.39563869 | 39.07995571 | 53.79230510 | 32.395638824 |
| 5–7 | … | 39.0799557124 | 53.7923051012 | 32.3956388254 |

- The contraction is about 0.007 per step, as the orchestrator measured.
- The notes' 0.25 GeV² at ℓ = 32 is π(c_β T_s)² = 0.2314 GeV² rounded up. It is not asserted.

### 5.2 The crossover at 𝓡 = 1 (prompt §2.2)

The exponential n = 1, V₀ = 10⁻⁴⁸ GeV⁴, the field at 0.1 M_P. "1L" is the one-loop estimate, "cl" the
classical one, and ln k_ex, ln m_ta and ln ω_t are in Planck units. `none` means the chain raised:
"ℓ ≤ 0 at iterate k" names where ℓ first fell to zero or below.

**At the worked example's E** (ln E = −174.563):

| log₁₀ M/M_P | log₁₀ 𝓡 | cl ln k_ex | cl ln ω_t | 1L ln k_ex | 1L ln m_ta | ℓ | iterations |
|---|---|---|---|---|---|---|---|
| −15.00 | −1.51 | −44.122 | −43.420 | none (ℓ ≤ 0 at iterate 1) | | | |
| −15.25 | −0.51 | −43.547 | −42.844 | none (iterate 1) | | | |
| −15.50 | 0.49 | −42.971 | −42.268 | none (iterate 2) | | | |
| −15.75 | 1.49 | −42.395 | −41.693 | none (iterate 2) | | | |
| −16.00 | 2.49 | −41.820 | −41.117 | none (iterate 4) | | | |
| −16.25 | 3.49 | −41.244 | −40.541 | −41.304 | −42.164 | 0.860 | 24 |
| −16.50 | 4.49 | −40.668 | −39.966 | −40.756 | −42.313 | 1.557 | 17 |
| −17.00 | 6.49 | −39.517 | −38.814 | −39.655 | −42.460 | 2.805 | 13 |
| −18.00 | 10.49 | −37.214 | −36.512 | −37.451 | −42.612 | 5.162 | 11 |
| −19.00 | 14.49 | −34.912 | −34.209 | −35.249 | −42.704 | 7.456 | 10 |

At M = 10⁻¹³ M_P (𝓡 = 10^−9.5) the chain fails at iterate 1 with ℓ = −5.49. The full scan, every
0.25 decade from 10⁻¹³ to 10⁻¹⁹, is in my scratch directory (`scan.out`).

**At E × 10⁻⁴** (ln E = −183.773):

| log₁₀ M/M_P | log₁₀ 𝓡 | cl ln k_ex | cl ln ω_t | 1L ln k_ex | 1L ln m_ta | ℓ | iterations |
|---|---|---|---|---|---|---|---|
| −16.25 | −0.83 | −46.032 | −45.329 | none (iterate 1) | | | |
| −16.50 | 0.17 | −45.457 | −44.753 | none (iterate 1) | | | |
| −16.75 | 1.17 | −44.881 | −44.177 | none (iterate 2) | | | |
| −17.00 | 2.17 | −44.305 | −43.602 | none (iterate 3) | | | |
| −17.25 | 3.17 | −43.730 | −43.026 | −43.784 | −44.372 | 0.588 | 32 |
| −17.50 | 4.17 | −43.154 | −42.451 | −43.241 | −44.577 | 1.336 | 18 |
| −18.00 | 6.17 | −42.003 | −41.299 | −42.145 | −44.743 | 2.598 | 13 |
| −19.00 | 10.17 | −39.700 | −38.997 | −39.951 | −44.905 | 4.954 | 11 |

**The landmarks:**

| | worked E | E × 10⁻⁴ |
|---|---|---|
| 𝓡 = 1 at | M = 10^−15.3783 M_P = 4.185×10⁻¹⁶ M_P (1.02 TeV) | 10^−16.4574 M_P = 3.488×10⁻¹⁷ M_P (85 GeV) |
| classical at 𝓡 = 1: ln k_ex, ln ω_t, ℓ_t | −43.2511, −42.5485, −0.7026 | −45.5546, −44.8511, −0.7034 |
| one-loop at 𝓡 = 1 | none: ℓ = −0.202 at iterate 1 | none: ℓ = −0.204 at iterate 1 |
| the fixed point disappears at | 10^−16.1088 M_P, ℓ* = 0.2407 | 10^−17.1898 M_P, ℓ* = 0.2398 |
| last solution within 100 iterations | 10^−16.1128 M_P, ℓ = 0.312 | 10^−17.1938 M_P, ℓ = 0.311 |
| 𝓡 there | 10^2.94 | 10^2.95 |
| at that edge: 1L − cl ln k_ex | **−0.0389** | **−0.0427** |
| at that edge: ln m_ta − ln ω_t | −1.053 | −1.057 |
| `outside_classical` at the edge | True | True |

- **The chain stops above 𝓡 = 1, not below it.**
  - Above the edge the fixed point ℓ = f(ℓ) does not exist: f(ℓ) − ℓ < 0 for every ℓ > 0.
  - At the edge it appears as a saddle-node at ℓ* ≈ 0.24, with f′(ℓ*) → 1.
  - Below the edge the plain iteration from 32 converges more slowly the nearer the edge. The
    contraction is 0.67 at 0.01 decades below, 0.20 at 0.25 and 0.04 at 2.
  - I located the edge on the exponential's closed-form chain (`sec:params`' equations, as in the
    test), since the generic method agrees with it to 4e-15. The script is `edge.py` in my
    scratch directory, with its output in `edge.out`.
  - The 100-iteration limit gives up 0.004 decades of M before the true edge (D6).
- **The gap is 0.73 decades of M at both E.** 𝓡 ∝ M⁻⁴ at fixed E, so the gap is 1 < 𝓡 < 10^2.9.
  There 08's rule would ask for a one-loop estimate that does not exist.
- **The jump.** At 𝓡 = 1 none can be measured (D10). At the chain's edge the one-loop ln k_ex is below
  the classical one by only 0.039–0.043, while ln m_ta is about 1.05 below ln ω_t. So k_ex is
  nearly continuous between the estimates there, and the masses are not.
- **The expectation.** Prompt §2.2 expected m_ta/ω_t = (4/ℓ)^{1/4} at 𝓡 = 1. That holds only where
  both estimates exist, which they never do at 𝓡 = 1 on this potential.

## 6. The deliberate-breakage record (P11)

- **The replay.**
  - `make_breakages.py` wrote each diff against the final tree (difflib).
  - `replay_breakages.sh`, run in bash, checked each with `git apply --check`, applied it, checked
    it with `git apply -R --check`, ran `ComputeTargets.tests.test_wall_model`, reverted it, and
    compared the source's md5 with the final tree's. All were restored.
  - Both scripts are in my scratch directory. None is committed.
- **B1–B6** are the prompt's, in its order. **B7** and **B8** are rulings 2's and 3's. **B9** is mine
  (D11).

**B1 — a wrong sign in V″** ((ln V)″ − (ln V)′², in the classical point, the one-loop search and the
derivative). It errors 9 tests, each with `ComputationFailureError`: "V″ ≤ 0 at the classical turning
point", "V″ ≤ 0" from the one-loop search, and "phi_min is not a minimum".
- `test_1_phi_t_ln_R_and_k_ex_equal_the_closed_form`;
- `test_2_the_classical_wall`;
- `test_2_the_one_loop_turnaround_is_the_notes_chain`;
- `test_3_the_classical_turning_point`;
- `test_3_the_one_loop_turnaround`;
- `test_4_the_derivative_against_a_finite_difference`;
- `test_5_a_field_inside_its_turning_points_returns_None`;
- `test_5_a_one_loop_call_without_a_solution_raises`;
- `test_5_a_bump_between_the_field_and_the_wall_is_refused` (the plain wall's call raises).
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -563,5 +563,5 @@
 
         d1 = self._d_logV_dphi(normalization, phi_t)
-        q = self._d2_logV_dphi2(normalization, phi_t) + d1 * d1
+        q = self._d2_logV_dphi2(normalization, phi_t) - d1 * d1
         if not q > 0.0:
             raise _failure(
@@ -626,5 +626,5 @@
         def log_V2(p: float) -> float:
             d1 = self._d_logV_dphi(normalization, p)
-            q = self._d2_logV_dphi2(normalization, p) + d1 * d1
+            q = self._d2_logV_dphi2(normalization, p) - d1 * d1
             if not q > 0.0:
                 raise ValueError(f"V'' <= 0 ((ln V)'' + (ln V)'^2 = {q!r})")
@@ -766,5 +766,5 @@
         d2_A = self._d2_logOmega_dphi2(phi_min)
         d1_V = self._d_logV_dphi(normalization, phi_min)
-        q_V = self._d2_logV_dphi2(normalization, phi_min) + d1_V * d1_V
+        q_V = self._d2_logV_dphi2(normalization, phi_min) - d1_V * d1_V
 
         try:
```

**B2 — λ from V″** (λ = (V″/V)^{1/2}, the notes' ω_t = λE^{1/2} read backwards). It fails 4 tests:
- `test_1_*`: k_ex off by 9.9e-3 relative at (10⁻³⁰, −180);
- `test_2_the_one_loop_turnaround_is_the_notes_chain`: 9.0e-6 > 1e-8;
- `test_3_the_classical_turning_point` and `test_3_the_one_loop_turnaround`: λ ≠ −(ln V)′
  (2.011e33 against 1.996e33).
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -569,5 +569,5 @@
                 f"((ln V)'' + (ln V)'^2 = {q!r}); a reflecting wall is convex there"
             )
-        lambda_t = -d1
+        lambda_t = q**0.5
         if not lambda_t > 0.0:
             raise _failure(
@@ -647,5 +647,6 @@
 
             phi_ta = exp(_search_log_field(residual, x_0, r_0, -1.0, context))
-            lambda_ta = -self._d_logV_dphi(normalization, phi_ta)
+            d1_ta = self._d_logV_dphi(normalization, phi_ta)
+            lambda_ta = (self._d2_logV_dphi2(normalization, phi_ta) + d1_ta * d1_ta) ** 0.5
             if not lambda_ta > 0.0:
                 raise _failure(
```

**B3 — the exponential n = 1 closed form for the generic root.** It fails 4 tests:
- `test_3_the_classical_turning_point` on **n = 2**: |ln W − ln E| = 9874 against 1.5e-9;
- three errors where the model has no `_M_float` (`test_3_the_one_loop_turnaround`,
  `test_5_a_search_that_leaves_float_range_raises`, `test_5_a_bump_*`).

Item 1 and item 2 pass under B3, as they should: on n = 1 the closed form is right.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -560,5 +560,5 @@
             return None
 
-        phi_t = exp(_search_log_field(residual, x_0, r_0, -1.0, context))
+        phi_t = self._M_float / log(1.0 + exp(log_E - normalization))
 
         d1 = self._d_logV_dphi(normalization, phi_t)
```

**B4 — the missing (ln A)″ term.** It fails 1 test, `test_4_the_derivative_against_a_finite_difference`:
0.166 > 1e-6 on the power-law wall at M = 10⁻³⁰, ln ρ_m0. On the exponential, (ln A)″ = 0 and
nothing changes (ruling 6).
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -779,5 +779,5 @@
         except OverflowError as e:
             raise _failure(f"{context}: V''/((ln A)' T) overflows") from e
-        denominator = ratio + d2_A / d1_A
+        denominator = ratio
         if not (denominator > 0.0 and isfinite(denominator)):
             raise _failure(
```

**B5 — a wrong factor in 𝓡** (4π² for 8π²). It fails 2 tests: `test_1_*` and
`test_3_the_classical_turning_point`, by ln 2 = 0.693 in ln 𝓡.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -580,5 +580,5 @@
             phi_t=phi_t,
             log_omega_t_squared=log_omega_t_squared,
-            log_R=2.0 * log_omega_t_squared - _LOG_8_PI_SQUARED - log_E,
+            log_R=2.0 * log_omega_t_squared - log(4.0 * pi * pi) - log_E,
             lambda_t=lambda_t,
             log_k_ex=log_k_ex,
```

**B6 — the iteration stopped after one step.** It fails 3 tests:
- `test_2_the_one_loop_turnaround_is_the_notes_chain`: 1.4e-4 > 1e-8;
- `test_3_the_one_loop_turnaround`: m_ta⁴ℓ = 32π²E off by 0.097 against 1.3e-12 (ruling 9);
- `test_5_a_one_loop_call_without_a_solution_raises`: no raise, because the first iterate's ℓ is
  positive at M = 10⁻¹⁶.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -661,5 +661,5 @@
                     f"at iteration {iteration} (ell from ell_0: {ells!r})"
                 )
-            if abs(ell_new - ell) <= ONE_LOOP_ELL_RTOL * ell_new:
+            if True:
                 outside = (
                     _log_V_minus_floor(self._log_V(normalization, phi_ta), log_V_floor)
```

**B7 — prompt §1.5's tolerance** (rtol = 10⁻¹³, SciPy's default xtol 2e-12). It fails 1 test,
`test_1_*`, at **(M = 10⁻¹⁰ M_P, ln E = −180)**. Item 1's errors at every point under B7:

| M/M_P | ln E | φ_t rel | ln 𝓡 abs | k_ex rel |
|---|---|---|---|---|
| 1e-16 | −180 | 3.2e-13 | 6.6e-11 | 6.4e-13 |
| 1e-16 | −120 | 2.2e-15 | 7.1e-13 | 7.2e-15 |
| **1e-10** | **−180** | **1.0e-12 (fails)** | **2.1e-10 (fails)** | **2.0e-12 (fails)** |
| 1e-7 | −60 | 3.4e-14 | 1.5e-11 | 6.8e-14 |
| others | | as on the final tree | | |

- **Ruling 2** asks B7 to fail at (10⁻¹⁶, −120) "or name the point where it does". It does not fail
  there. The likely reason, not verified, is that my search's four halvings hand brentq a different
  bracket from the orchestrator's probe, which had none, so its last step lands elsewhere.
- It fails at (10⁻¹⁰, −180) on all three quantities, ln 𝓡 included.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -96,5 +96,5 @@
 # that times 2(y + 4), which the orchestrator measured at 1.3e-10 against the exponential closed
 # form, above the test's 1e-10 (ruling 2)
-WALL_ROOT_RTOL = 4.0 * sys.float_info.epsilon
+WALL_ROOT_RTOL = 1.0e-13
 WALL_ROOT_XTOL = 1.0e-14
 # the search doubles its step in ln phi from the field, at most this many times (a backstop: a probe
@@ -247,5 +247,4 @@
                 min(x_out, x_in),
                 max(x_out, x_in),
-                xtol=WALL_ROOT_XTOL,
                 rtol=WALL_ROOT_RTOL,
             )
```

**B8 — the monotonicity check removed.** It fails 1 test,
`test_5_a_bump_between_the_field_and_the_wall_is_refused`: "ComputationFailureError not raised". The
search brackets past the bump and returns the wall's root.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -202,5 +202,5 @@
         probes.append((x_in, r_in))
         if not (r_in > r_out or (r_in == _NEG_INF and r_out == _NEG_INF)):
-            raise not_monotone("a probe moved away from the target")
+            pass
         if r_in >= 0.0:
             break
@@ -224,7 +224,5 @@
         probes.append((x_mid, r_mid))
         if not ((r_out < r_mid < r_in) or (r_mid == _NEG_INF and r_out == _NEG_INF)):
-            raise not_monotone(
-                f"a midpoint of the bracket [{x_out!r}, {x_in!r}] is not between its ends"
-            )
+            pass
         if r_mid >= 0.0:
             x_in, r_in = x_mid, r_mid
```

**B9 — the exception imported at the top of the module** (ruling 1). It fails 1 test,
`test_7_the_models_import_alone`: exit 1, with "ImportError: cannot import name 'AbstractModel' from
partially initialized module 'CosmologyConcepts.Models.AbstractModel'".
`test_7_the_models_import_after_ComputeTargets` passes, as the orchestrator found, so the import
alone is the test that sees it.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -53,4 +53,6 @@
 
 from scipy.optimize import brentq
+
+from ComputeTargets.exceptions import ComputationFailureError
 
 from datastorekit import DatastoreObject
```

## 7. Observations not acted on

- **The one-loop chain has no solution for 1 < 𝓡 ≲ 10^2.9** (§5.2). It stops 0.73 decades of M
  below the point where 𝓡 = 1, at ℓ ≈ 0.24, with the field still outside the classical wall. 08 must
  decide what to use there. Opened as `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`.
- **The notes on where the chain ends.** `sec:params` says the chain ends where y reaches L, at
  M_crit ≈ 1 TeV, where the one-loop point meets the classical wall. Measured (§5.2), the fixed
  point disappears first, at ℓ ≈ 0.24, at about 190 GeV at the worked example's E. There
  `outside_classical` is still True and m_ta is 0.35 ω_t. M_crit itself (𝓡 = 1) is at 1.02 TeV,
  as the notes say. This is data on the notes, recorded, not a correction.
- **The cost of the error context.** `_wall_context` formats the model's name on every call
  (2.9 µs of the classical call's 21–24 µs). If 05 finds the classical call too dear, a lazy
  context is the first saving, before any closed-form override.
- **The one-loop iteration is plain.** Near the chain's edge it needs 20–100 iterations. A secant or
  Steffensen step would cut that. 08 calls it once per generation, so this was not done.
- **`effective_minimum`'s bracket** (0.05 φ₀, 20 φ₀) is fixed. At φ₀ = φ_min it always contains the
  root, so item 4's comparison is fair. A caller of it with φ₀ far from the minimum would get a
  ValueError, as its docstring says. Unchanged.

## 8. State handed to the next prompt

- **The methods** (`CosmologyConcepts/Models/AbstractModel.py`). Each is on `AbstractModel` with the
  normalization first, and on `RealizedModel` without it. `ExponentialModel` overrides none. E is the
  Einstein-frame ρ_kin at the infall (05 has it as `log_rho_kin`, ln(H_E²π²/2)); T is ρ_R,E(Σ + f_m).
  - `classical_turning_point(phi, log_E) -> Optional[ClassicalTurningPoint]`, with fields
    `(phi_t, log_omega_t_squared, log_R, lambda_t, log_k_ex, ell_t)`:
    - ln 𝓡 = 2 ln ω_t² − ln 8π² − ln E; λ_t = −d ln V/dφ;
    - ln k_ex = ln(λ_t E^{1/2}/2); ℓ_t = ln k_ex − ½ ln ω_t² (≈ −0.70 on the exponential);
    - returns `None` if V(φ) − V_floor ≥ E (the field is at or inside its classical wall).
  - `one_loop_turnaround(phi, log_E) -> Optional[OneLoopTurnaround]`, with fields
    `(phi_ta, log_m_ta, lambda_ta, log_k_ex, ell, iterations, outside_classical)`:
    - m_ta⁴ℓ = 32π²E, V″(φ_ta) = m_ta², k_ex = λ_ta E^{1/2}/2, ℓ = ln k_ex − ln m_ta;
    - returns `None` if V″(φ) ≥ m_ta² at an iterate (the field is inside its one-loop turnaround);
    - **raises where the chain has no solution**: ℓ ≤ 0, or no convergence in 100 iterations.
  - `minimum_on_trace(phi_start, log_T) -> float`: the root of V′ + (ln A)′T = 0, from a start the
    caller gives.
  - `d_minimum_d_log_trace(phi_min, log_T) -> float`: −(ln A)′T/(V″ + (ln A)″T) at the given root;
    **no (ln A)′² term**. 06 composes it with d ln T/dN of the right-hand side.
- **They raise `ComputationFailureError`** naming the model, the inputs and the reason, for:
  - a model that is not `reflects_at_origin`; for the turning points, a model with no or a
    non-finite `log_V_floor`;
  - φ not finite and > 0; ln E or ln T not finite;
  - no bracket in 64 doublings; a search that leaves float range; a hook that raises;
  - a quantity not monotone along the search;
  - V″ ≤ 0 or λ ≤ 0 at the root;
  - (ln A)′ ≤ 0 in the minimum's search; a non-positive denominator in the derivative.
- **The constants**: `WALL_ROOT_RTOL` = 4ε, `WALL_ROOT_XTOL` = 10⁻¹⁴, `WALL_SEARCH_MAX_DOUBLINGS` =
  64, `WALL_SEARCH_HALVINGS` = 4, `ONE_LOOP_ELL_START` = 32, `ONE_LOOP_ELL_RTOL` = 10⁻¹²,
  `ONE_LOOP_MAX_ITERATIONS` = 100. None is a key.
- **The costs**, minima at a load of about 5–7:
  - classical: 21–24 µs per call;
  - one-loop: 167 µs (7 iterations) to 482 µs (21 iterations, near the chain's edge);
  - minimum: 14–18 µs;
  - derivative: 4.5 µs.
- **The 𝓡 = 1 crossover, for 08** (§5.2), on the exponential with V₀ = 10⁻⁴⁸ GeV⁴:
  - 𝓡 = 1 at M = 4.19×10⁻¹⁶ M_P at the worked example's E, and 3.49×10⁻¹⁷ at E × 10⁻⁴;
  - the one-loop chain exists only for 𝓡 ≳ 10^2.9: from 10^−16.109 M_P downwards at the worked
    E, and from 10^−17.190 at E × 10⁻⁴;
  - **between 𝓡 = 1 and 𝓡 ≈ 10^2.9 neither estimate is the one R7 names**. The classical estimate
    exists there, the one-loop one does not;
  - at the chain's edge, one-loop ln k_ex − classical ln k_ex = −0.039 (−0.043), and
    ln m_ta − ln ω_t = −1.05.
  - Issue `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` is open for 08.
- **Tests**: `ComputeTargets/tests/test_wall_model.py` (22), with the test-only models
  `_PowerLawWall` and `_BumpWall`, and `_the_notes_chain`.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 339 (96 + 243), ObjectFactories
  103, tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02
  (2026-10-27); this campaign's close (12) updates them.
