# Log 01 — the dark-radiation component

**Subject:** Carry a dark-radiation component in the policy expressions
**Commit:** `85661b5`, on branch `inelastic-reflection`, on top of `00c17f4`
**Date:** 2026-10-28, by Claude Opus 5.5
**Result:** **COMPLETE.** No stop.
- §1.1–§1.5 shipped under orchestrator note 01, whose eleven rulings govern where they differ from
  the prompt. Every expression of prompt §1.2 was derived here (§1 below) and agrees with the
  planner's form.
- **The gate "DR" holds.**
  - Both golden histories pass unchanged and bitwise at K = 0 (`test_golden_history.py`, and
    `test_tuning.py` (e)).
  - With an initial ρ_χ the constraint holds to rounding at 40 constructed states and on every
    sample of a real history, and d ln H_E/dN on the dense output equals the policy's Ḣ/H² to 2e-6.
  - The radiation limit holds to rounding: Ḣ/H² = −2 exactly at R = 0, V = 0, π = 0 for every r_χ.
  - A tuned history with r₀ = 1 converges in five integrations, with both conditions inside 1e-5.
- **Nothing a build stores moved.** No column, key, fingerprint format or version changed. No store
  was opened, no Ray started, nothing written to `var/`, no golden JSON regenerated.
- **Counts:**
  - before: CosmologyModels 26, ComputeTargets 269 (77 + 192), ObjectFactories 103, tools 115,
    RunDrivers 64;
  - after: 26, **288** (96 + 192, +19, all in the new `test_dark_radiation.py`), 103, 115, 64.
  - All passing, before and after.
- Opened no issue; closed none. `.documents/OPEN_ISSUES.md` is unchanged.

## 1. The derivations (prompt §1.2)

Notation: X ≡ 3H²M_P² (Einstein frame), U ≡ V/X, B ≡ ρ_R(1 + f_m)/X, Y ≡ ρ_χ/X,
r_χ ≡ ρ_χ,E/ρ_R,E, G ≡ 1 − π²/6M_P², π = dφ/dN so φ̇ = Hπ. Σ = 1 − 3w, so p_R = ρ_R(1 − Σ)/3,
and R ≡ (Σ + f_m)/(1 + f_m).

The two equations given:
- (C) X G = V + ρ_R(1 + f_m) + ρ_χ, i.e. **G = U + B + Y**.
- (H) Ḣ = −(ρ + p)/2M_P², with ρ + p = H²π² + ρ_R(1 + f_m) + p_R + (4/3)ρ_χ.

**V/3H²M_P².** From (C), G = U [1 + (ρ_R/V)(1 + f_m) + ρ_χ/V] = U [1 + (ρ_R/V)(1 + f_m + r_χ)],
so U = G·T with **T = 1/(1 + (ρ_R/V)(1 + f_m + r_χ))**. Multiplying top and bottom by V/ρ_R gives
the other branch, **T = (V/ρ_R)/(V/ρ_R + 1 + f_m + r_χ)**. *Agrees with the planner.*

**E.** From (C), G − U = B + Y. With B : Y = ρ_R(1 + f_m) : r_χρ_R = (1 + f_m) : r_χ,

    B = (G − U)(1 + f_m)/(1 + f_m + r_χ),   Y = (G − U) r_χ/(1 + f_m + r_χ),

and E ≡ G − U − Y = B = ρ_R(1 + f_m)/3H²M_P². *Agrees with the planner (both forms are this one).*

**Ḣ/H² + 3.** Divide (H) by H²: Ḣ/H² = −(ρ + p)/(2M_P²H²) = −(3/2)(ρ + p)/X. Term by term:
- H²π²/X = π²/3M_P² = 2(1 − G);
- [ρ_R(1 + f_m) + p_R]/X = B[1 + (1 − Σ)/(3(1 + f_m))]. Since (1 − Σ)/(1 + f_m) = 1 − R, this is
  B(4 − R)/3;
- (4/3)ρ_χ/X = (4/3)Y.

So Ḣ/H² = −3(1 − G) − B(4 − R)/2 − 2Y, and Ḣ/H² + 3 = 3G − B(4 − R)/2 − 2Y. Put B = G − U − Y:

    Ḣ/H² + 3 = 3G − (G − U − Y)(4 − R)/2 − 2Y
             = G(1 + R/2) + (U/2)(4 − R) + Y[(4 − R)/2 − 2]
             = G·A₁ + C·A₂ − Y·R/2,

with A₁ = 1 + R/2, A₂ = 4 − R, C = U/2, which are the code's. *Agrees with the planner.*
- **Radiation limit** (Σ = 0, f_m = 0, so R = 0): the new term vanishes. With V = 0 and π = 0
  (U = 0, G = 1), Ḣ/H² + 3 = 1 and **Ḣ/H² = −2** whatever ρ_χ is.
- **Matter limit** (R = 1, V = 0, π = 0, so B + Y = 1): Ḣ/H² = −3/2 − Y/2. With
  Y = ρ_χ/(ρ_m + ρ_χ): −(3/2)(ρ_m + (4/3)ρ_χ)/(ρ_m + ρ_χ) = −(3/2)(1 + Y/3) = −3/2 − Y/2. *Agrees.*

**`HubblePolicy`'s V = 0 branch.** (C) with V = 0: X G = ρ_R(1 + f_m + r_χ), so
**3H²M_P² = ρ_R(1 + f_m + r_χ)/G**. *Agrees.*

**`_tuning_state`'s V_target.** H_J = H_E b′/A with b′ = 1 + (d ln A/dφ)π, so H_E = H_J A/b′.
ρ_R,E = A⁴ρ_R,J (the code's ln ρ_R,J = ln ρ_R,E − 4 ln A). Put H_J = H₀ in (C):

    V = 3M_P² H₀² A² G/b′² − A⁴ρ_R,J(1 + f_m) − ρ_χ,E
      = A⁴ [3M_P² H₀² G/(A² b′²) − ρ_R,J(1 + f_m) − ρ_χ,E/A⁴],

and ρ_χ,E/A⁴ = r_χ ρ_R,E/A⁴ = r_χ ρ_R,J. So **V_target = A⁴(3M_P²H₀²G/(A²b′²) − ρ_R,J(1 + f_m + r_χ))**,
the same as subtracting ρ_χ,E outside the bracket, because r_χ is frame-invariant. *Agrees.*

**Unchanged, checked by reading:**
- `effective_minimum` reads ρ_m,E only (`_tuning_state`'s `rho_mE0`). Massless quanta are traceless
  and do not shift the minimum.
- d ln ρ_R/dN, d ln f_m/dN and d ln T_J/dN in `ODERHS` read Σ, d ln A/dφ and π only, not H.
- `_reflection_guards`' W = −3 U expm1(ln V_floor − ln V) is built from U, so it sees ρ_χ through
  T's denominator.
- **`fixed_T_value_at`'s ρ_NP** = 3M_P²H_J² − ρ_R,J(1 + f_m) includes ρ_χ,J through H_J. This is
  intended: it is the ratio BBN sees (README §0.2).

**The form of E (ruling 9).** The two candidates, given G − U computed as a difference:
- **(i) E = (G − U) − Y**, with Y = r_χ(G − U)/(1 + f_m + r_χ).
  - When V dominates, G − U is cancelled: its relative error is about EPS·G/(G − U), up to 3×10³ at
    ln(ρ_R/V) = −8.
  - It then subtracts Y from G − U, and Y/(G − U) = r_χ/(1 + f_m + r_χ) → 1 as r_χ ≫ 1 + f_m. That is
    a second cancellation, which multiplies the relative error by (1 + f_m + r_χ)/(1 + f_m): ×11 at
    r_χ = 10, f_m ≈ 0.
- **(ii) E = (G − U)(1 + f_m)/(1 + f_m + r_χ).**
  - It inherits G − U's error and adds a few roundings: one product and one quotient of positive
    numbers. It adds no cancellation, whatever r_χ is.

**I used (ii)**, in `PotentialDerivativePolicy.matter_and_dark_radiation_fractions(G, U, f_m, r_χ)`,
which returns (E, Y) and is shared by `ODEPolicy` (E) and `Hdot_over_H2_plus_3` (Y).
- A form that avoids G − U altogether, E = G·T·(ρ_R/V)(1 + f_m), was considered and not used. At
  K = 0 the code must take G − U exactly, and using it only at K > 0 would make the two paths differ
  by G − U's rounding as r_χ → 0.
- Measured at the constructed states of test (b): E·3H²M_P² against ρ_R(1 + f_m) to at most 841 EPS
  at ln(ρ_R/V) = −8 (G/(G − U) = 2980). Elsewhere it was at most 68 EPS, the same as the constraint.

## 2. What shipped

### The ledger (`ComputeTargets/dark_radiation.py`, new)
`DarkRadiationLedger(K_initial: float = 0.0)` (ruling 1):
- **What it holds.** K(N) = K₀ + Σ_{N_j ≤ N} ΔK_j. K₀ is the history's initial condition, present at
  every N, below N_start as well. The injections (N_j, ΔK_j) are held in lists, with the cumulative
  K and ln K per segment cached at `add`.
- **Its interface:**
  - `add(N, delta_K)`: N below the last injection's N is a `ValueError` (equal is allowed); so are a
    negative or non-finite ΔK and a non-finite N.
  - `K(N)`: one `bisect_right` over the N_j, or none when there are no injections.
  - `is_zero(N)`: K(N) == 0, with no exp and no log.
  - `rho_chi(N)` = exp(ln K − 4N), and `ratio(N, log_rhorad_Einstein)` = exp(ln K − 4N − ln ρ_R,E);
    each is 0.0 where K(N) = 0.
  - read-only: `K_initial`, `injections` (a tuple of (N_j, ΔK_j)), `is_empty`.
  - `__repr__`.
- It holds floats and lists only, so it pickles.

### The policies (`ComputeTargets/Policies/PotentialDerivativePolicy.py`)
- `V_over_3H2Mp2`, `Vprime_over_3H2Mp2`, `Vprimeprime_over_3H2Mp2`, their three
  `_evaluate_…_using_log_V` forms and `Hdot_over_H2_plus_3` take `r_chi: float = 0.0`, last, and
  forward it.
- `_evaluate_V_over_3H2Mp2_using_log_V`: each branch of T branches on `r_chi == 0.0` to its old
  expression, and otherwise adds r_χ to 1 + f_m.
- `Hdot_over_H2_plus_3`: at `r_chi == 0.0` it returns `G * A1 + C * A2` as before; otherwise
  `G * A1 + C * A2 - Y * R / 2.0`.
- New static method `matter_and_dark_radiation_fractions(G, V_over_3H2Mp2, fm, r_chi)` → (E, Y),
  form (ii).

### `ComputeTargets/ScalarHistory.py`
- **`ODEPolicyData`** gains `rho_chi_over_rho_rad`, appended last (ruling 6). The one construction
  uses keywords. A `grep` of `ODEPolicyData`, `ODEPolicy(`, `Hdot_over_H2_plus_3` and
  `V_over_3H2Mp2(` over the tree finds no unpacking by position and no other construction.
- **`ODEPolicy`** takes `dark_radiation: Optional[DarkRadiationLedger] = None`.
  - `_get_r_chi(N, state)` returns 0.0 when the ledger is `None` or `is_zero(N)`; otherwise it returns
    `ledger.ratio(N, state.log_rhorad_Einstein)`. An `OverflowError` there becomes a
    `ComputationFailureError`, as `_get_fm` does.
  - `__call__` passes r_χ to `V_over_3H2Mp2`, `Vprime_over_3H2Mp2` and `Hdot_over_H2_plus_3`.
  - E is `G - V_over_3H2Mp2` at r_χ = 0.0, as before, and form (ii) otherwise.
  - It returns `rho_chi_over_rho_rad=r_chi`.
- **`HubblePolicy`**'s V = 0 branch reads `data.rho_chi_over_rho_rad`:
  `rho_rad * (1.0 + fm) / G` at 0.0, as before, and `rho_rad * (1.0 + fm + r_chi) / G` otherwise.
- **`integrate_history`** takes `rho_chi_ratio_init: float = 0.0`, documented as a test harness.
  - It always builds a ledger, with K₀ = r₀ exp(ln ρ_R,E,init + 4 N_start) when r₀ ≠ 0.0 and 0.0
    otherwise.
  - It passes the ledger to the one `ODEPolicy` (ruling 3); `N_failsafe` and `N_start` moved above
    the policy's construction, with the same values.
  - The success payload gains `"dark_radiation"`. A failure payload is `_failure_payload`'s, as before.
- **`_tuning_state`** reads `data["dark_radiation"]`.
  - If `is_zero` at the last sample's `raw_N`, V_target is the old expression: the same operations,
    laid out over three lines by black.
  - Otherwise it subtracts `rho_RJ * (1.0 + fm + r_chi)`, with r_χ = `ratio(raw_N, log_rhorad_Einstein)`.
- **`tune_history`** takes `rho_chi_ratio_init: float = 0.0`, last, documented, and passes it to
  `integrate_history` as a keyword (ruling 7).
- **`compute_scalar_history`** is unchanged, byte for byte: it is not in the diff.

**The payload on the way to the store (prompt §1.4), read:**
- `ScalarHistory.store` (`ComputeTargets/ScalarHistory.py`, the `store` method of the class) reads
  `failure`, `failure_reason`, `first_bounce`, `fixed_T_values`, `TUNING_OUTPUT_KEYS`, `metadata`,
  `sample`, `z_grid` and `solver_label` by name.
- `build_extra_data` reads eight named keys.
- `golden_history.record_of` reads named keys.
- `ObjectFactories/tests/test_tuned_history_round_trip.py` copies the payload with `dict(data)` and
  reads `TUNING_OUTPUT_KEYS` only.
- The `.items()` loop at `ObjectFactories/ScalarHistory.py:651` iterates over the factory's own
  `attributes`, not the payload.
- So nothing iterates over the payload's keys, and the key is not stored. Test (d) asserts that
  `build_extra_data` does not carry it. The payload pickles (test (d), r₀ = 0).

### The tests (`ComputeTargets/tests/test_dark_radiation.py`, new; 19 tests, ~20 s at a load of 4)
Each bound is stated as a number, and the docstring says why it is rounding (ruling 8).
`rounding_bound(*logs) = (Σ|ln x| + 64)·EPS`: exp of an argument x is wrong in relative terms by up to
|x|·EPS/2, and H² and V/3H²M_P² are rebuilt from exps of ln V and ln ρ_R. The 64 covers the two dozen
products, quotients and sums of positive terms in between.

**(a) `TestTheLedger`** (microseconds):
- `test_a_K_is_piecewise_constant`: K at 8 points, exact.
- `test_a_an_injection_counts_at_its_own_N`: K(N_j) is the new K and K(N_j − 1e-12) the old, exact;
  `rho_chi(N_j)` is exact.
- `test_a_rho_chi_scales_as_a_to_the_minus_4_between_injections`: ρ_χ(N + dN)/ρ_χ(N) = e^{−4dN} and
  the same for r_χ. The bound is (|x₁| + |x₂| + 4dN + 8)·EPS over the three exps' arguments.
- `test_a_an_empty_ledger_is_zero_everywhere`: exact zeros.
- `test_a_add_out_of_order_raises`.
- `test_a_K_initial_is_present_below_N_start`: K(−1e-9) == K(0) (ruling 1).
- `test_a_it_pickles`.

**(b) `TestTheConstraint`**, at 40 constructed states (milliseconds):
- **The states:**
  - ln(ρ_R/V) ∈ {−8, −2, 2.5, 10, 746}, r_χ ∈ {1e-6, 1e-2, 1, 10};
  - (ln f_m, π/M_P) ∈ {(−20, −0.4), (0.5, 0.05)};
  - ln ρ_R,E = −166, φ = 0.2 M_P, N = 0;
  - the model is `ExponentialModel(M = 0.01 M_P, n = 1, β = 3)`, realized at the normalization that
    gives the wanted ln(ρ_R/V).
- `test_b_the_states_take_the_branches_they_are_meant_to`: r_χ is the requested one to 64 EPS.
  `data.V_over_3H2Mp2 == 0.0` at ln(ρ_R/V) = 746 (ruling 2), and > 0 elsewhere.
- `test_b_the_constraint_to_rounding`: |3M_P²H_E²G − V − ρ_R(1 + f_m) − ρ_χ|/total ≤
  rounding_bound(ln V, ln ρ_R), about 400 EPS. The worst measured was 68 EPS.
- `test_b_E_is_the_matter_and_bath_only`: E is read back from the kick,
  E = −kicking/(3M_P²(d ln A/dφ)R). |E·3H²M_P² − ρ_R(1 + f_m)|/ρ_R(1 + f_m) ≤
  rounding_bound·G/(G − U), the cancellation of G − U. The test also asserts that this bound is
  below 1e-3 of the breakage's error r/(1 + f_m + r).

**(c) `TestHdot`** (milliseconds):
- `test_c_Hdot_from_the_definitions`: Σ = 0.3 at all 40 states, so R > 0 at f_m ≈ 2e-9 too. Ḣ/H² is
  written out from ρ + p and the constraint's H². |policy − definitions| ≤ 3·rounding_bound,
  **absolute** (see deviation D9). Worst measured: 19 EPS. The smallest Y·R/2 at these states is
  4.9e-11, against a bound of 2.6e-13.
- `test_c_with_R_zero_the_term_vanishes`: Σ = 0, f_m = 0, π = 0. Ḣ/H² == −2.0 exactly at V = 0 for
  every r_χ; at the other states it agrees with the definitions to the same bound.
- `test_c_with_matter_it_is_the_dust_and_radiation_law`: R = 1, V = 0, π = 0, against
  −(3/2)(ρ_m + (4/3)ρ_χ)/(ρ_m + ρ_χ), to 16 EPS.

**(d) `TestARealHistory`**: the golden β = 1.5 history with r₀ = 0.1. The setup is
golden_history.py's: M = 1e-3 M_P, Λ = 1 meV, φ* = 5 M_P, T* = 2e4 GeV, T_stop = 1 MeV,
atol = rtol = 1e-8, the production axis. One integration, 0.5 s at a load of 4.
- `test_d_the_payload_carries_the_ledger_it_was_built_with`: no injections. r_χ(N = 0) = r₀ to
  64 EPS. `build_extra_data` does not carry the key.
- `test_d_the_constraint_on_the_samples`: at all samples, |3M_P²H_E²G − ρ_R(1 + f_m) − V −
  ρ_χ,E(N)|/total ≤ rounding_bound(ln V, ln ρ_R,E), about 540 EPS. Worst measured: 129 EPS
  (2.9e-14). The same check with r₀ = 0 gives 129 EPS too.
- `test_d_d_ln_H_dN_on_the_dense_output`. **The H check** (ruling 4).
  - At the midpoint of every accepted step longer than 1e-3 e-folds (560 midpoints), it compares
    (ln H_E(N + h) − ln H_E(N − h))/2h, with h = min(1e-4, step/8), against
    `Hdot_over_H2_plus_3(..., r_chi=data.rho_chi_over_rho_rad) − 3` at the interpolated state.
  - **The tolerance is 2e-6.** The derivative of Radau's collocation polynomial between its nodes
    carries the local error, rtol = 1e-8, over the step, about 1e-2 at the QCD transition: about
    1e-6. The centred difference adds h²/6·|(ln H)‴| < 1e-8 and a rounding of 1e-14/h = 1e-10.
  - **Measured:**
    - worst residual 4.0e-7 with r₀ = 0.1;
    - worst 4.8e-7 on the same history with r₀ = 0 (probe, not a test). So the bound is the
      integrator's, not the dark radiation's.
    - percentiles at r₀ = 0.1: 50th 1.3e-8, 90th 5.7e-8, 99th 1.9e-7.
  - It also asserts that Y·R/2 somewhere exceeds 1e3 times the tolerance.
- `TestTheEmptyLedger.test_d_r0_zero_gives_an_empty_ledger_and_a_payload_that_pickles`: 0.5 s.

**(e) the tuning:**
- `TestTheTuningState.test_e_V_target_puts_H_J_at_H0` (milliseconds).
  - The constructed last sample is the tuned golden history's last excerpt, from its JSON, with its
    realized model. A ledger gives r_χ = 0.39 there.
  - V_target, put back into the Einstein-frame constraint, gives |H_J/H₀ − 1| ≤
    4·rounding_bound(ln ρ_R,E) ≈ 3.1e-13.
  - Why rounding: V_target subtracts the matter, about 0.3 of the total, a cancellation of at most
    1/0.7; and ρ_R,E is exp(−285.6).
- `TestATunedHistory.test_e_it_converges_with_H_J_at_H0_and_phi_ref_at_phi_min`: `tune_history` at
  the tuned golden point (β = 3, M = 0.01 M_P) with r₀ = 1.0. Five integrations, 19.6 s at a load
  of 4.
  - It converges, and both residuals, recomputed from the last sample (H_J/H₀ − 1 and
    φ_ref/φ_min − 1), are inside 1e-5.
  - The ledger has K₀ > 0, and 0.3 r₀ < r_χ(T_CMB) < 0.5 r₀.

### The documents (P12)
- `.documents/numerical-strategies.md`: new §6.1, dated 2026-10-28, after §6's hygiene list. It
  places the policy expressions beside `V/3H²M_P²`'s log-safe form and refers to §5.2's E.
- `.documents/architecture-summary.md`: a dated note at the end of §7.2, on the ledger, the new
  module, who holds it and the payload key. The directory tree of §2 is not edited
  (`CLAUDE.md` invariant 6).

## 3. Deviations from the prompt

**The orchestrator's rulings:**

| # | Deviation | Class |
|---|---|---|
| R1 | The initial ρ_χ is a constant K₀, present at every N, and not an injection at N_start. Constructor `DarkRadiationLedger(K_initial)`; `add` is ordered against the injections only; K₀ and the injections are exposed read-only; the added test checks K at N_start − 1e-9. Under the prompt's form, a sample at N_forward ∈ [−1e-10, 0) would see K = 0 | IMPLEMENTATION CHOICE (ruled) |
| R2 | The V = 0 state has ln(ρ_R/V) = 746, not "> 745", and the test asserts `V_over_3H2Mp2 == 0.0` there | STRUCTURALLY REQUIRED |
| R3 | `integrate_history` always builds a ledger and gives it to its one `ODEPolicy`, empty at r₀ = 0. Every success payload carries it. "Empty at N" is `is_zero`, with no exp or log | STRUCTURALLY REQUIRED (02) |
| R4 | §1.5 item 4 is split. The constraint check reads the samples and the payload's ledger. The H check reads the dense output of the `IntegrationResult`, captured by wrapping `integrate_scalar_history` inside `integrate_history`, as `test_fixed_T_values.full_history` does, so the ledger is the one `integrate_history` built. It does not drive `integrate_scalar_history` from a hand-built state. Ḣ/H² is `V_policy.Hdot_over_H2_plus_3(..., r_chi=data.rho_chi_over_rho_rad)` | IMPLEMENTATION CHOICE |
| R5 | The `.documents/` scripts are not touched | as ruled (Observations) |
| R6 | `rho_chi_over_rho_rad` is appended last to `ODEPolicyData` | IMPLEMENTATION CHOICE (ruled) |
| R7 | `tune_history` passes the ratio by keyword; `compute_scalar_history` is unchanged. No existing test failed | as ruled |
| R8 | Every "to rounding" is a stated relative or absolute bound, with its reason in the docstring; no `assertAlmostEqual` | as ruled |
| R9 | E is form (ii); §1 above says how each candidate behaves | IMPLEMENTATION CHOICE (ruled) |
| R10 | Timing: three runs at each tree, alternating, through a temporary worktree of `00c17f4` in the scratch directory, since removed | as ruled |
| R11 | Suites run serially; output to scratch; `uptime` beside each | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | `add` also refuses a negative or non-finite ΔK and a non-finite N, and the constructor a negative or non-finite K₀. ρ_χ ≥ 0, and ln K is taken | IMPLEMENTATION CHOICE |
| D2 | The ledger has more than the prompt's interface: `K(N)`, `is_zero(N)`, `is_empty`, `K_initial`, `injections` and `__repr__`. R1 and R3 require the first four in substance | IMPLEMENTATION CHOICE |
| D3 | r_χ is threaded into `Vprime_over_3H2Mp2`, which the prompt does not list. The reflecting term D = 3M_P²·V′/3H²M_P² = V′/H² must see ρ_χ through H, and the H check and the constraint would fail without it | STRUCTURALLY REQUIRED |
| D4 | r_χ is threaded into `Vprimeprime_over_3H2Mp2` too, for uniformity. Its one user is `AdiabaticHistory`, which passes nothing and gets 0.0 (05's) | IMPLEMENTATION CHOICE |
| D5 | `PotentialDerivativePolicy.matter_and_dark_radiation_fractions`, a static helper that `ODEPolicy` (E) and `Hdot_over_H2_plus_3` (Y) share, so that both use form (ii) | IMPLEMENTATION CHOICE |
| D6 | An `OverflowError` from `ledger.ratio` in `ODEPolicy` becomes a `ComputationFailureError`, as for `exp(log_fm)`, so that a wild trial state is a rejected step | IMPLEMENTATION CHOICE |
| D7 | The E of §1.5 item 2 is read back from the kicking term rather than exposed as a new field, so the test checks what reaches the kick | IMPLEMENTATION CHOICE |
| D8 | Extra tests beyond §1.5: the ledger pickles; r₀ = 0 gives an empty ledger and a payload that pickles; the payload's ledger is the one it was built with and is not in `extra_data`; the matter limit (R = 1) | IMPLEMENTATION CHOICE |
| D9 | §1.5 item 3's comparison is absolute, on the scale 3. The policy returns Ḣ/H² + 3 ∈ [1, 3], so taking 3 off leaves an absolute rounding. Ḣ/H² itself is about −7e-4 at ln(ρ_R/V) = −8, a cancelled difference. A relative bound failed at that state (1.8e-13 against 8.6e-14) on my first run; the error was mine, in the test | STRUCTURALLY REQUIRED |
| D10 | §1.5 item 5's unit test takes its "constructed last sample" from the tuned golden JSON's last excerpt, so that it is a realistic state today at no cost | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT. Before the commit, my first run of the new tests had three failures. All three
were bounds of mine that were too tight: D9's two, and the a⁻⁴ test's 8 EPS, which ignored the
arguments of the exps, |x| ≈ 130. I widened them with the reason given, and the code was not changed.

## 4. Verification performed

**Suites**, serially, ComputeTargets in two halves. The "before" runs were at `00c17f4` before any
edit; the "after" runs were on the final tree.

| Suite | Before (count, wall, load 1-min before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 68 s, 11.2 → 14.8 | 26 OK, 65 s, 4.1 → 3.2 |
| ComputeTargets [a-f] | 77 OK, 100 s, 14.8 → 11.8 | **96** OK, 119 s, 3.2 → 3.3 |
| ComputeTargets [g-z] | 192 OK, 244 s, 11.8 → 10.6 | 192 OK, 235 s, 3.3 → 3.6 |
| ObjectFactories | 103 OK, 17 s, 10.6 → 8.7 | 103 OK, 17 s, 3.6 → 3.5 |
| tools | 115 OK, 117 s, 8.7 → 6.3 | 115 OK, 113 s, 3.5 → 3.6 |
| RunDrivers | 64 OK, 34 s, 6.3 → 4.5 | 64 OK, 25 s, 3.6 → 3.7 |

- The baselines are README §0.4's.
- ComputeTargets rises by 19 (`test_dark_radiation.py` sorts into [a-f]), and nothing fell.
- `black --check` on the four changed Python files: all unchanged.

**Golden tests after each file** (order 3 of the note), all passing:
- before any edit;
- after `PotentialDerivativePolicy.py`;
- after the `ScalarHistory.py` edits.

**The cost at K = 0 (ruling 10).** I timed `ComputeTargets.tests.test_golden_history` and
`test_tuning.TestAConvergedHistory.test_e_the_tuned_golden_history`. Each was run three times in a
worktree at `00c17f4` and three times in the changed tree, alternating, at a 1-minute load of
3.7–5.8:

| | HEAD runs (s) | min | change runs (s) | min |
|---|---|---|---|---|
| golden (two histories) | 11.0, 11.8, 10.9 | **10.9** | 10.9, 11.6, 11.1 | **10.9** |
| tuned golden (setUpClass tuning + test e) | 23.3, 24.9, 23.7 | **23.3** | 24.1, 24.3, 24.1 | **24.1** |

- The tuned test's minimum moved by +0.8 s. That is inside HEAD's own spread of 1.6 s, so it is
  "nothing measurable".
- A microbenchmark of one `ODEPolicy` call at the P1 state (best of 7 × 20000) agrees:
  - HEAD: 7.33 and 7.80 µs (two runs);
  - change, ledger `None`: 7.73 µs;
  - change, empty ledger: 7.81 µs.
- Before any edit, at a load of 3.9: golden 12.9 s, tuned 25.4 s (one run each).

**§1.5 item 4: where Y·R/2 peaks.** The golden β = 1.5 history with r₀ = 0.1, on the 560 step
midpoints:
- **The peak** is 9.46e-3, at N = 19.18, T_J = 0.19 GeV, on the QCD transition, where Σ is largest.
- Y·R/2 > 1e-4 at 477 of the 560.
- **By range of T_J:**

  | T_J | max Y·R/2 |
  |---|---|
  | 1–20 TeV | 2.4e-5 |
  | 10 GeV–1 TeV | 1.6e-3 |
  | 1–10 GeV | 2.8e-3 |
  | 0.1–1 GeV | 9.5e-3 |
  | 10–100 MeV | 2.3e-3 |
  | 1–10 MeV | 2.4e-4 |

- Every value is far above rtol = 1e-8 and the H check's 2e-6.

**§1.5 item 5: the tuning with r₀ = 1.0.**
- **Convergence:** five integrations (seed, plain, three secants), each about 3.9 s. Residuals
  H/H₀ − 1 = −1.18e-6 and φ_ref/φ_min − 1 = +4.24e-6.
- **r_χ(T_CMB)** = 0.390 r₀, as the note estimated: ρ_χa⁴ is constant while the bath is heated
  through g_s^{4/3}.
- **Physicality:** no "negative value of E" or "G" line was printed in any run (stdout read in the
  probes).
- **Why r₀ = 1 fails under the V_target breakage, and by how much.** Leaving ρ_χ,J out of Eq. 7's
  bracket makes V_target too large, so H_J² is too large by ρ_χ,J A²b′²/(3M_P²H₀²G).
  - So H_J/H₀ − 1 ≈ (1/2)·r_χ(T_CMB)·Ω_R,J·A²b′²/G = (1/2)·0.390·9.13e-5·(0.9954)²·1.492/0.9991 =
    **2.63e-5**.
  - b′ = 1 + βπ₀/M_P = 1.222 because the field still moves today.
  - The unit test's breakage residual is 2.6347e-5. The tuning stalls at H/H₀ − 1 = +2.57e-5 to
    +2.68e-5 from integration 4 on, with φ_ref/φ_min − 1 below 1e-6.
  - It falls short by a factor 2.6 against 1e-5. At the planning estimate's 4.5e-5 r_χ (b′ = 1),
    r₀ = 1 would have been marginal.

## 5. The deliberate-breakage record (P11)

- Each diff was written against the final tree.
- Each was checked with `git apply --check`, applied, checked with `git apply -R --check`, run
  through `ComputeTargets.tests.test_dark_radiation` in bash
  (`scratchpad/agent01/replay_breakages.sh`), reverted, and the three source files' md5 compared
  with before. All five were restored.
- None is committed.

**B1 — the −Y·R/2 term dropped.** It fails 3 tests:
- `test_c_Hdot_from_the_definitions`: 4.9e-11 against 2.6e-13, at (−8, 1e-6, f_m ≈ 2e-9);
- `test_c_with_matter_it_is_the_dust_and_radiation_law`: 1.26e-7 against 3.6e-15, at r = 1e-6;
- `test_d_d_ln_H_dN_on_the_dense_output`: first exceeds at N = 1.74 (2.28e-6). Measured over the
  whole history, the worst is 9.46e-3 at N = 19.18, and 550 of 568 midpoints are over 2e-6.
```diff
--- a/ComputeTargets/Policies/PotentialDerivativePolicy.py
+++ b/ComputeTargets/Policies/PotentialDerivativePolicy.py
@@ -256,4 +256,4 @@
         # Hdot/H^2 + 3 = G A1 + C A2 - Y R/2. The term vanishes when R = 0 (a radiation bath), where
         # Hdot/H^2 = -2 whatever rho_chi is
         _, Y = self.matter_and_dark_radiation_fractions(G, V_over_3H2Mp2, fm, r_chi)
-        return G * A1 + C * A2 - Y * R / 2.0
+        return G * A1 + C * A2
```

**B2 — ρ_χ left in E.** It fails 2 tests:
- `test_b_E_is_the_matter_and_bath_only`: 1.0e-6 against 2.6e-10, at (−8, 1e-6, f_m ≈ 2e-9);
- `test_d_d_ln_H_dN_on_the_dense_output`: first at N = 4.06 (2.1e-6). Over the whole history the
  worst is 6.7e-3, and 488 of 573 midpoints are over. The kick then drives π off the trajectory that
  energy conservation implies.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -369,9 +369,7 @@
         if r_chi == 0.0:
             E = G - V_over_3H2Mp2
         else:
-            E, _ = self.V_policy.matter_and_dark_radiation_fractions(
-                G, V_over_3H2Mp2, fm, r_chi
-            )
+            E = G - V_over_3H2Mp2
         # E must be positive, because it is proportional to rho_R/H^2
         if E < 0.0:
             # unclear whether we should treat this as a genuine computational error, or whether it just means that rho_r is very small
```

**B3 — r_χ left out of the V = 0 branch.** It fails 2 tests:
- `test_b_the_constraint_to_rounding`: 1.0e-6 against 2.5e-13, at (746, 1e-6, f_m ≈ 2e-9);
- `test_b_E_is_the_matter_and_bath_only`: the same state, since E·3H²M_P² reads the wrong H.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -436,7 +436,7 @@
             if r_chi == 0.0:
                 ThreeH2Mp2 = rho_rad * (1.0 + fm) / G
             else:
-                ThreeH2Mp2 = rho_rad * (1.0 + fm + r_chi) / G
+                ThreeH2Mp2 = rho_rad * (1.0 + fm) / G
             H2_Einstein: float = ThreeH2Mp2 / self.CONST_3_MP_SQ
 
         H_Einstein: float = sqrt(H2_Einstein)
```

**B4 — r_χ left out of V_target.** It fails 2 tests:
- `test_e_V_target_puts_H_J_at_H0`: 2.6347e-5 against 3.1e-13.
- **The tuned test fails too.** `test_e_it_converges_with_H_J_at_H0_and_phi_ref_at_phi_min` runs to
  the cap: "the normalization did not converge: after 30 integrations, H/H0-1=+2.658e-05,
  phi_ref/phi_min-1=+3.861e-07". The test module took 117 s under it.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1662,7 +1662,7 @@
     else:
         r_chi = ledger.ratio(s.raw_N, s.log_rhorad_Einstein)
         V_target = A0**4 * (
-            3.0 * Mp**2 * H0**2 * G / (A0**2 * bp**2) - rho_RJ * (1.0 + fm + r_chi)
+            3.0 * Mp**2 * H0**2 * G / (A0**2 * bp**2) - rho_RJ * (1.0 + fm)
         )
     rho_mE0 = A0**4 * fm * rho_RJ
     phi_min = model.effective_minimum(norm, phi_ref, phi0, rho_mE0)
```

**B5 — the ledger's rule changed to N_j < N.** It fails 3 tests:
- `test_a_an_injection_counts_at_its_own_N`: K(2.0) = 1.0, not 4.0;
- `test_a_K_is_piecewise_constant`: K(1.0) = 0.0, not 2.0;
- `test_a_rho_chi_scales_as_a_to_the_minus_4_between_injections`: from N_j = 5 it reads the old K.
```diff
--- a/ComputeTargets/dark_radiation.py
+++ b/ComputeTargets/dark_radiation.py
@@ -43,7 +43,7 @@
 and is computed in logs, exp(ln K - 4N - ln rho_R,E), so that no intermediate overflows.
 """
 
-from bisect import bisect_right
+from bisect import bisect_left, bisect_right
 from math import exp, isfinite, log
 from typing import Optional, Tuple
 
@@ -129,7 +129,7 @@
 
     def _segment(self, N: float) -> int:
         """The number of injections with N_j <= N."""
-        return bisect_right(self._N, N) if self._N else 0
+        return bisect_left(self._N, N) if self._N else 0
 
     def K(self, N: float) -> float:
         """K(N) = K_0 + sum_{N_j <= N} Delta K_j."""
```

## 6. Observations not acted on

- **The scripts under `.documents/` do not construct `ODEPolicy` today** (ruling 5). They call it
  with four positional arguments (potential, coupling), the signature before `jordan-normalization`
  04, and the constructor takes three:
  - `.documents/integrator-audit-2026-09-30/harness.py:60`;
  - `.documents/peakewsurf-field-equations/scripts/pipe_runs.py:146`.

  They are dated records (`CLAUDE.md` invariant 6), so they are not edited and no issue is opened.
- **`CLAUDE.md`'s ComputeTargets counts are now stale**: 269 → 288, and the [a-f] half 77 → 96. The
  file is not in this prompt's list. Under the campaign's plan, 08 updates `CLAUDE.md`'s counts.
- **`AdiabaticHistory` does not see ρ_χ.** Its own E (`AdiabaticHistory.py:199`) and its calls of
  `Hdot_over_H2_plus_3` and `V_over_3H2Mp2` pass no `r_chi`, so they get 0.0. It rebuilds from stored
  samples, which carry no ρ_χ until 05 (README §2.2, 05).
- **E's cancellation when V dominates is old, and unchanged at K = 0.** E = G − V/3H²M_P² loses up to
  log₁₀(G/(G − U)) digits (3.5 at ln(ρ_R/V) = −8). A cancellation-free form exists,
  G·T·(ρ_R/V)(1 + f_m). Adopting it would change every history, so it is not in scope (README §5).
- **The H check's residual at K = 0** is 4.8e-7 on the golden β = 1.5 history: the dense output's
  derivative against the policy's Ḣ/H². It is the same with ρ_χ (4.0e-7). Recorded as a measurement.
- **The planning estimate of the V_target breakage** left out b′². At the tuned golden point
  b′² = 1.49, so leaving ρ_χ out misplaces H_J/H₀ by 2.6e-5 r_χ(today)/0.39, not 1.75e-5. Anyone
  sizing r₀ for a test of the tuning at another point should include (1 + βπ₀/M_P)².

## 7. State handed to the next prompt

- **The ledger API** (`ComputeTargets/dark_radiation.py`):
  - `DarkRadiationLedger(K_initial=0.0)` gives K(N) = K₀ + Σ_{N_j ≤ N} ΔK_j.
  - `add(N, delta_K)`: N must be ≥ the last injection's N, and ΔK finite and ≥ 0, else `ValueError`.
    ΔK_j = Δρ_χ,E,j e^{4N_j}.
  - `K(N)`, `is_zero(N)` (no exp or log), `rho_chi(N)` = K e^{−4N}, and
    `ratio(N, log_rhorad_Einstein)` = r_χ, both computed in logs.
  - read-only: `K_initial`, `injections` (a tuple of (N_j, ΔK_j)), `is_empty`.
  - It pickles. 02 checks Σ ΔK_j against its drains through `injections`.
- **How the policies receive it.**
  - `ODEPolicy(task_label, cosmology, model, dark_radiation=None)` holds the ledger object and reads
    it at each call's N. So an `add` made while Radau runs is seen by the next RHS evaluation at
    N ≥ N_j, with no new policy.
  - `integrate_history` builds one ledger and one `ODEPolicy`, used by the step loop, the reflection
    guards, the sampling loop and `fixed_T_values`. It is empty unless `rho_chi_ratio_init` ≠ 0.
  - r_χ reaches `PotentialDerivativePolicy` as the keyword `r_chi` (default 0.0), and
    `ODEPolicyData.rho_chi_over_rho_rad` (last field) carries it to `HubblePolicy`.
  - At r_χ == 0.0 every expression takes its old operations, and both golden histories are bitwise.
- **The N_j ≤ N edge belongs to 02.**
  - After an injection at N_j, the new Radau instance evaluates the RHS at N_j with the new K, which
    is right.
  - But the dense-output interpolant of the step that ends at N_j holds the state *before* the
    injection. Evaluated at N_j, through `OdeSolution`, the samples, `fixed_T_value_at` or
    `first_bounce`, it would be paired with the new K.
  - 02 must decide which interpolant, and which K, a query at exactly N_j gets. One option is to
    record the step's end as a node and have queries at N_j read the post-injection state.
  - K₀ has no such edge: it is present at every N, below N_start included (R1).
- **The payload key.**
  - A successful `integrate_history` payload, and so `tune_history`'s, carries `"dark_radiation"`,
    the ledger.
  - `_tuning_state` reads it, at the last sample's `raw_N` and `log_rhorad_Einstein`.
  - Nothing on the way to the store reads it, and it is not stored; 05 decides what of it is stored.
- **What is not done:**
  - nothing injects;
  - `compute_scalar_history` takes no ρ_χ;
  - `AdiabaticHistory`'s E and Ḣ ignore ρ_χ (05);
  - `CLAUDE.md`'s counts are not updated (08).
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 288 (96 + 192), ObjectFactories
  103, tools 115, RunDrivers 64.
