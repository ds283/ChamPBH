# Log 04 — the model interface

**Subject:** Read V and A through one realized model, with the rows behind an adapter
**Commit:** `597a215`, on branch `jordan-normalization`, on top of `a0ca6a1`
**Date:** 2026-10-25, by Claude Opus 5.5
**Result:** **COMPLETE**, after one stop, which the orchestrator ruled (Deviations 14 and 15).
- M1–M7 shipped, under orchestrator note 04, whose sixteen rulings govern where they differ from
  the prompt, and under the orchestrator's two rulings after the stop.
- The golden history passes **unchanged and bitwise** through the adapter. No golden JSON was
  written.
- The shift law holds: the largest difference is 2.685e-14 on the rounding scale of the summed
  terms.
- Eq. 7 holds at every stored sample of both golden histories: at most 4.251e-14 (β = 1.5) and
  6.875e-14 (β = 5.5).
- Counts:
  - CosmologyModels 18, ObjectFactories 86, tools 93 and RunDrivers 61 are unchanged;
  - ComputeTargets went from 188 (61 + 127) to 205 (61 + 144), +17.
- All three deliberate breakages are caught. Breakage 1 is a one-ulp shift, because the ruled
  `log(exp(…))` is bitwise a no-op (Deviation 16).
- One issue closed and none opened.

## What shipped

- **M1, M2: `CosmologyConcepts/Models/AbstractModel.py`.**
  - **`Seed`.** `namedtuple("Seed", ["kind", "normalization", "phi_ref"])`, and
    `SEED_KINDS = ("tracking", "static", "plain")`.
  - **`RealizedModel(model, normalization, phi_ref)`.**
    - **The methods.** `log_V`, `d_logV_dphi`, `d2_logV_dphi2`, `log_Omega`, `Omega`,
      `d_logOmega_dphi`, `d2_logOmega_dphi2`, plus the properties `log_V_floor`,
      `reflects_at_origin`, `name`, `normalization`, `phi_ref` and `model`. There is no
      `log_A`/`A`; the docstring says that Ω is the conformal factor A.
    - **A(φ_ref) = 1.** `log_Omega(phi)` is `model._log_Omega(phi) - self._log_Omega_ref`, with
      `_log_Omega_ref = model._log_Omega(phi_ref)` computed once, in `__init__`. `Omega` is
      `exp(self.log_Omega(phi))`. Every other method delegates with nothing added.
    - **Immutable.** The attributes are set through `object.__setattr__` in `__init__`, and
      `__setattr__`/`__delattr__` raise `AttributeError`. The properties have no setter.
    - **`name`.** `f"{model.name}[normalization={…:.6g},phi_ref={phi_ref/Mp:.6g}Mp]"`.
  - **`AbstractModel(store_id, units)`.**
    - **Abstract:** `name`, `type_id` and `shard_key`, and the six hooks `_log_V(normalization, phi)`,
      `_d_logV_dphi`, `_d2_logV_dphi2`, `_log_Omega(phi)`, `_d_logOmega_dphi` and
      `_d2_logOmega_dphi2`.
    - **Defaults:** `_log_V_floor(normalization)` gives `None`, and `reflects_at_origin` gives
      `False`, as on `AbstractPotential`. `units` is a property.
    - **`realize(normalization, phi_ref)`.**
    - **`renormalize`, `normalization_coordinate` and `normalization_from_coordinate`** raise
      `NotImplementedError` in the base class (Deviation 7).
    - **`effective_minimum(normalization, phi_ref, phi_0, rho_m_E_0, bracket=None)`.** It is the
      root of `exp(rm.log_V(x)) * rm.d_logV_dphi(x) + rm.d_logOmega_dphi(x) * rho_m_E_0 *
      exp(rm.log_Omega(x) - rm.log_Omega(phi_0))`. The solver is `brentq` on (0.05 φ₀, 20 φ₀) by
      default, with `xtol = 1e-14 * phi_0` and `rtol = 1e-13`. A bracket without a sign change, or
      with a NaN end, raises `ValueError` naming the bracket.
    - **`seed(cosmology)`.** It gives `plain`: `log(3.0 * Mp**2 * H0**2 * (1.0 - cosmology.omega_m))`
      with φ_ref `0.0`. This equals `log(cosmology.rho_cc)` bitwise on Planck2018 (checked in the
      scratch copy).
- **M3: `CosmologyConcepts/Models/ExponentialModel.py`.**
  - **Identity.** The constructor is `ExponentialModel(store_id, M, n, beta, units)`, and it
    asserts `n >= 0`.
    - `type_id` is `EXPONENTIAL_MODEL = 1`, from the new `CosmologyConcepts/Models/model_ids.py`.
    - `shard_key` is the `beta_value`.
    - `name` is `ExponentialModel(M=0.001Mp,n=1,beta=1.5)`: M is formatted by `energy_formatter`,
      β by `:.5g`.
  - **The hooks.** They are `ExponentialPotential.log_V`/`d_logV_dphi`/`d2_logV_dphi2`, copied, with
    `self._log_Lambda_4` replaced by `normalization` and "ExponentialModel" in the messages. That
    includes `GetFieldValue`, `pow(self._M_float / phi_float, self._n)`, the division by `phi`, and
    the exception handling.
    - `_log_Omega` is `self._beta_float * phi_float / self._Mp`, `_d_logOmega_dphi` is
      `self._beta_float / self._Mp`, and `_d2_logOmega_dphi2` is `0.0`.
    - `_log_V_floor(normalization)` is `normalization`, and `reflects_at_origin` is `True`.
  - **`renormalize`.** It is `normalization + log(V_target) - self._log_V(normalization, phi)`,
    and it raises `ValueError` unless `V_target > 0`. The coordinate pair is the identity.
  - **`seed`.** `_analytic_seed(cosmology, kind)` transcribes `analytic()`:
    - the starting φ, at most 200 iterations with the `1e-14` test, and V's expression with
      `cosmology.rho_m0` and `cosmology.rho_r0`;
    - normalization `log(V) - Mf / phi`;
    - `None` on `ValueError` or `ZeroDivisionError`, on non-convergence, or on V, G or
      1 + βφ′/M_P not positive.

    `seed` tries tracking, then static, then the base class's plain. It gives plain for n ≠ 1.
  - **The adapter `from_rows(potential, coupling)`.** It is a classmethod, and builds
    `cls(0, potential._M, potential._n, coupling._beta, potential._units)` and realizes it at
    `4.0 * log(float(potential._Lambda))`, φ_ref `0.0`. It raises `TypeError` unless the rows are an
    `ExponentialPotential` and an `ExponentialCoupling`. Its docstring says that it is 07's to
    remove.
  - **`CosmologyConcepts/Models/__init__.py`** exports `AbstractModel`, `RealizedModel`, `Seed`,
    `SEED_KINDS`, `ExponentialModel` and `EXPONENTIAL_MODEL`. `AbstractModel` comes first, with a
    comment on the import order.
- **M4: the consumers** (ruling 8). One argument replaces the two.
  - **`ComputeTargets/ScalarHistory.py`.**
    - `ODEPolicy(task_label, cosmology, model)` stores `self.model`.
    - `HubblePolicy(model, units)`.
    - `_reflection_guards` reads `policy.model`. Its messages say "model {name}" and keep
      `reflects_at_origin`.
    - `surfing_turning_points(result, model, interior_samples=8)`.
    - `fixed_T_value_at(result, k, N, policy, model, units)` and
      `fixed_T_values(result, policy, model, units)`.
    - `compute_scalar_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model, …)`.

    Every arithmetic expression is unchanged. `first_bounce` and the commented-out `ODERHS` line
    are not edited.
  - **`ComputeTargets/Policies/PotentialDerivativePolicy.py`.** Its signature is
    `PotentialDerivativePolicy(task_label, cosmology, model)`.
    - **Removed:** the three `_evaluate_*_using_plain_V` methods, the plain-V fallbacks and the
      "no log functions" branches of the three dispatchers, the feature detection
      (`_has_log_V_functions`, `_has_plain_V_functions`) with its `RuntimeError`, and the now-unused
      `log` and `numpy.inf` imports.
    - **Kept verbatim:** the log path's statements and messages. Only `self.potential.` became
      `self.model.`.
    - A class docstring says why.
  - **`ComputeTargets/AdiabaticHistory.py`.** `AdiabaticComputePolicy(task_label, cosmology, model)`,
    and `compute_adiabatic_values` reads `model.realized_model` from its history.
  - **`ComputeTargets/BBNData.py` is not changed.** `compute_BBN_data` reads only the stored
    samples, `T_Jordan_stop` and the cosmology, and nothing in it computes with A or V (its only
    `potential`/`coupling` uses are labels, the shard key and properties).
- **M5: the adapter in `ScalarHistory`.**
  - **`realized_model`.** It is a read-only property, built on first access by
    `ExponentialModel.from_rows(self._potential, self._coupling)` and cached in `_realized_model`,
    which `__init__` sets to `None`. The import is local to the property, since
    `ExponentialModel` imports `ComputeTargets.exceptions` (Deviation 10).
  - **`compute()`** passes it, and the default task label is `self.realized_model.name`.
  - **Not changed:** `shard_key`, `potential`, `coupling` and the `check_required_parameter` calls.
- **The tools** (ruling 11). `tools/history_and_bbn.py` passes
  `ExponentialModel.from_rows(potential, coupling)` to `compute_scalar_history`.
  `tools/surfing_scan.py` builds `model` once and passes it there and to `surfing_turning_points`.
  `_StandInModel` and `tools/plot_surfing_histories.py` are not touched.
- **M6** (ruling 12). Deleted:
  - from `AbstractPotential`: the five abstract properties `bounce_region_level{1,2}_boundary`,
    `bounce_region_level{1,2}_max_step` and `hard_reflection_point`;
  - from `ExponentialPotential`: those five, `default_max_step`, and the `from numpy import inf`
    that fell unused.

  The witness finds nothing (Verification).
- **Not changed** (rulings 9, 10): `pipeline_selection.py`, `main.py`, `plot_by_beta.py`,
  `plot_ScalarHistory.py` and `extract_common.py`, all in the prompt's §5, and `BBNData.py`.
- **The records:**
  - this board;
  - `integrator-remediation`'s board (the issue moves from §3 to §4);
  - `.documents/OPEN_ISSUES.md`;
  - `.documents/architecture-summary.md`: `Models/` in the `CosmologyConcepts/` tree, the five
    properties gone from §8.3's listing, a dated note in §8.3 and §8.4, and a new §8.4a.

## Tests added or changed

**Added: `ComputeTargets/tests/test_model_interface.py`** (17 tests, in the `[g-z]` half). Its
docstring gives the cost of the golden histories (about 10 s).
- **(a) `TestTheAdapterIsBitwise`** (its docstring says that 07 removes it):
  - `test_a_every_value_equals_the_rows`: for β ∈ {1.5, 5.5, −1.5}, through `from_rows` on
    `golden_history.rows`:
    - `phi_ref == 0.0`, and the normalization equals `potential._log_Lambda_4`;
    - at 12 φ (`logspace(-6, 1)` M_P, that is 10⁻³ M to 10 M_P), `assertEqual` of each of the
      seven methods against the row's;
    - `log_V_floor` and `reflects_at_origin` equal the potential's.
  - `test_a_the_adapter_takes_only_the_exponential_rows`: swapped rows raise `TypeError`.
  - `test_a_the_names`: the model's name is `ExponentialModel(M=0.001Mp,n=1,beta=1.5)`, and the
    realized model's name begins with it. `type_id` is 1, and `shard_key` is 1.5.
- **(b) `TestTheReferencePoint`:**
  - for φ_ref ∈ {0, 0.01, 0.0349624, 0.3, 5} M_P, `log_Omega(phi_ref) == 0.0` and
    `Omega(phi_ref) == 1.0` by `assertEqual`, on `ExponentialModel` and on `_MinimalModel`, a
    test-only `AbstractModel` with constant V, ln Ω = γφ and no handling of φ_ref;
  - a `RealizedModel` refuses attribute assignment.
- **(c), (d) `TestTheGoldenHistories`.** Both golden histories run once in `setUpClass`.
  - **`test_c_the_shift_law`.** At every one of the 2723 β = 1.5 samples, `fixed_T_value_at` itself
    runs on a one-step stub result whose interpolant returns the sample's state. v = 1 + r + f_m at
    φ_ref = δ is compared with v_ref e^{−2βδ/M_P}, for δ ∈ {−0.01, 0.03, 0.1} M_P.
    - **The criterion** (Deviation 15): |v − v_ref e^{−2βδ/M_P}| ≤ 1e-12 × (1 + f_m + |r_ref|)
      e^{−2βδ/M_P}.
    - **Measured:** at most **2.685e-14**, at δ = −0.01, sample 2216. The plain relative difference
      is at most **2.789e-11**, at δ = +0.03, sample 2024, a T_J turning point.
  - **`test_d_eq_7_at_the_stored_samples`.** At every stored sample of both histories, bounces
    included, 3 M_P² H_J²/ρ_R,J (stored `H_Jordan` and `log_rhorad_Jordan`) is compared with
    A²(1 + βφ′/M_P)²/(1 − φ′²/6M_P²)(1 + f_m + V/ρ_R,E), taking A, V and β/M_P from the realized
    model, to 1e-12 relative.
    - **Measured:** at most **4.251e-14** (β = 1.5, sample 598) and **6.875e-14** (β = 5.5,
      sample 74), as note 04 §0 measured.
- **(e) `TestRenormalize`:**
  - after `renormalize`, V(φ) = V_target to 1e-12 at φ ∈ {1e-4, 0.0349624, 2} M_P and
    V_target ∈ {1e-121, 3e-120};
  - V_target of 0 or less raises `ValueError`;
  - the coordinate round-trips exactly for four normalizations.
- **(f) `TestEffectiveMinimum`.** It uses three states from README §0.3's table, written in the test
  as (β, Λ meV, φ_ref/M_P, φ_min/φ₀): (1.5, 2.125913, 0.0349624, 1.046), (5.5, 2.021177, 0.0167195,
  1.006) and (25, 1.843465, 0.0068090, 1.000). From these it takes normalization 4 ln Λ,
  φ₀ = φ_ref/(φ_min/φ₀) and ρ_m,E,0 = A(φ₀)⁴ ρ_m0.
  - The generic root agrees to 1e-12 with a bisection run to the last representable halving.
  - V_eff′ is negative at root × (1 − 1e-6) and positive at root × (1 + 1e-6).
  - A bracket (10 φ₀, 20 φ₀) with no sign change raises `ValueError` mentioning "bracket".
- **(g) `TestSeed`.** The expected values are `float.hex` literals from a copy of `analytic()` in the
  scratch directory (`agent04/analytic_copy.py`), at this tree's units and cosmology. The comment
  names it.
  - **β = 1.5, M = 1e-3:** `tracking`, with Λ⁴ `0x1.74311d9d83c1dp-400` (Λ = 2.110131124 meV) and
    φ `0x1.19467e2f45537p-5` (3.433537145e-2 M_P).
  - **β = 1.5, M = 0.5:** `static`, because the tracking loop raises a math domain error. Λ⁴ is
    `0x1.0c1627f2edec7p-400` (1.943956227 meV) and φ `0x1.b7daa46d0060cp-1` (8.590899833e-1 M_P).
  - **β = −1.5, M = 1e-3:** `plain`, because both loops raise. The normalization is exactly
    `-0x1.14a16ebc655f3p+8` (−276.63059594607256), and φ_ref is 0.0. So are β = 0
    (`ZeroDivisionError`), n = 2, and `_MinimalModel`.

  Each analytic kind matches its literals to 1e-12 relative, in e^{normalization} and in φ_ref.
- **(h) `TestPickle`.** The β = 5.5 adapter model, and the same model realized at 0.03 M_P,
  round-trip through `pickle`. The name, normalization, φ_ref, `log_V_floor`, `reflects_at_origin`
  and the seven methods at three φ are all equal.

**Changed** (ruling 13 and the orchestrator's rulings after the stop; no assertion changed except
G1's name literal):

| File | Change | Why |
|---|---|---|
| `ComputeTargets/tests/golden_history.py` | `rows(beta, units)` builds the two rows; `run_history` passes `ExponentialModel.from_rows(potential, coupling)`; module and function docstrings say so | ruling 13; the helper is Deviation 13 |
| `ComputeTargets/tests/test_fixed_T_values.py` | `full_history` builds the adapter's model, passes it, and returns `(data, result, policy, model)`; the calls pass `model` or `policy.model`; docstrings | ruling 13 |
| `ComputeTargets/tests/test_kinematic_cap_loop.py` | `build(beta, M, model_class=ExponentialModel)` realizes `model_class.from_rows(potential, coupling)`; `NonReflectingExponentialModel(ExponentialModel)`; `potential_class` → `model_class`; G1's literal `"ExponentialPotential("` → `"ExponentialModel("`; docstring | ruling 13 |
| `ComputeTargets/tests/test_integrator_exceptions.py` | the two policy constructions take `rhs.policy.model` (black joined each onto one line) | ruling 13 |
| `ComputeTargets/tests/test_adiabatic_mass.py` | `_StandInRealizedModel`, which delegates to the stand-in potential and the coupling, used by `_policy`; in test (e), the stand-in history carries `realized_model=None` and `_StandInPolicy.__init__` takes `(task_label, cosmology, model)` | ruling 13; the orchestrator's ruling after the stop (Deviation 14) |
| `ComputeTargets/tests/test_sampling_axis.py` | the identity test's ln A from `ExponentialModel.from_rows(*gh.rows(1.5, units))`; the unused `beta_value` and `ExponentialCoupling` imports removed | ruling 13 |
| `ComputeTargets/tests/test_scalarhistory_failure_reason.py` | the call passes `ExponentialModel.from_rows(potential, coupling)` | ruling 13 |

**Not changed:** `test_surfing_turning_points.py` (its stubs are duck-typed on `d_logOmega_dphi`,
ruling 1), `test_initial_field_option.py` (ruling 9) and the ObjectFactories tests (ruling 10).

## Deviations from the prompt

Every ruling cited is in orchestrator note 04, §0, unless it says "the orchestrator's ruling after
the stop".

1. **`IMPLEMENTATION CHOICE` (ruling 1): the integrator's names only.** There is no `log_A` or `A`.
   The hooks take no φ_ref. `RealizedModel`'s immutability is also enforced by
   `__setattr__`/`__delattr__`, not only by having no setters.
2. **`STRUCTURALLY REQUIRED` (ruling 2): the expressions copied bitwise**, and the adapter's
   normalization `4.0 * log(float(potential._Lambda))` at φ_ref `0.0`. The bitwise witness is test
   (a); the golden history is the end-to-end one.
3. **`IMPLEMENTATION CHOICE` (ruling 3): the identity.** This covers the constructor, `model_ids.py`,
   `shard_key`, the names and `from_rows`.
   - `from_rows` builds `cls(...)`, so a subclass's adapter builds the subclass. The kinematic-cap
     test's `NonReflectingExponentialModel` needs this.
   - It imports the row classes locally, so that 07 deletes the adapter and its imports together.
   - **My choice:** `AbstractModel.__init__` takes `units`, and `units` is a property, because
     `RealizedModel.name` needs M_P for φ_ref/M_P.
4. **`STRUCTURALLY REQUIRED` (ruling 4): `effective_minimum` has no closed form.** The generic method
   is the base class's, and `ExponentialModel` does not override it. The prompt's "closed and
   generic agree" test is replaced by the bisection reference and the minimum check.
5. **`STRUCTURALLY REQUIRED` (ruling 5): the Σ ρ_R,E term is left out.**
   `1.0 - 3.0 * cosmology.w(T_CMB)` on `QCD_Cosmology(Planck2018)` in `Planck_units` is **0.0**
   exactly, with w(T_CMB) = 0.3333333333333333 (`agent04/analytic_copy.py`). No history was run to
   T_CMB.
6. **`STRUCTURALLY REQUIRED` (ruling 6): the seed.** At M = 0.5 the seed is `static`, not `plain`
   as the prompt's M7 said. `plain` arises for β ≤ 0 and for n ≠ 1.
7. **`IMPLEMENTATION CHOICE` (ruling 7): the name `normalization_from_coordinate`.**
   - **My choice:** the base class's `renormalize` and coordinate pair raise
     `NotImplementedError` rather than being abstract.
   - So `_MinimalModel` implements only the hooks, plus the `name`, `type_id` and `shard_key`
     that any `AbstractModel` must declare, as test (b) asks.
8. **`STRUCTURALLY REQUIRED` (ruling 8): the consumers' signatures.**
   - **`IMPLEMENTATION CHOICE` (the prompt's own option): the plain-V paths are removed.** With
     them go the feature-detection block and its `RuntimeError`, and the unused imports.
   - The policy's attribute is `self.model`. The golden history stays bitwise.
   - `ODERHS`'s diagnostic line still prints "- potential: log(V/GeV^4)=…"; it is not a consumer
     signature, and it is not edited.
9. **`IMPLEMENTATION CHOICE` (ruling 9): `pipeline_selection.py` is not changed**, and neither is
   `test_initial_field_option.py`.
10. **`STRUCTURALLY REQUIRED` (ruling 10): `realized_model` is lazy.**
    - The other files are unchanged, which is `IMPLEMENTATION CHOICE` (ruling 10):
      `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `extract_common.py`, and
      `AdiabaticHistory`'s and `BBNData`'s own rows and labels.
    - **`IMPLEMENTATION CHOICE`:** `ExponentialModel` is imported inside the property, which avoids
      an import cycle. `ExponentialModel` imports `ComputeTargets.exceptions`, which runs
      `ComputeTargets/__init__` and so `ScalarHistory`. A top-level import would fail whenever
      `CosmologyConcepts.Models` is imported first. Checked in fresh interpreters: each of
      `import CosmologyConcepts.Models`, `import ComputeTargets`, the direct import of
      `ExponentialModel`, and `import tools.history_and_bbn` succeeds.
11. **`STRUCTURALLY REQUIRED` (ruling 11): the tools.** Their tests pass unedited.
12. **`STRUCTURALLY REQUIRED` (ruling 12): M6, and the issue closed.** `from numpy import inf` fell
    unused in `ExponentialPotential` and is removed. `AbstractPotential`'s imports are all still
    used.
13. **`STRUCTURALLY REQUIRED` (ruling 13): the tests.** (a) and (h) are `IMPLEMENTATION CHOICE`
    (ruling 13).
    - **`IMPLEMENTATION CHOICE`:** `golden_history.rows(beta, units)`, used by `run_history`,
      `test_sampling_axis` and the new module, so the rows are built in one place.
    - **`IMPLEMENTATION CHOICE`:** checks beyond the note's list: the adapter's `TypeError`, the
      names and ids, immutability, `V_target ≤ 0`, the bracket refusal, and plain at β = 0, at
      n = 2 and on the base class.
14. **`STRUCTURALLY REQUIRED` (ruling 8; the orchestrator's ruling after the stop):
    `test_adiabatic_mass.py`'s test (e).**
    - **The stop.** With ruling 8's signatures, the three test-(e) tests
      (`test_e_crossing_history_through_compute`, `test_e_exact_zero`,
      `test_e_spike_history_through_compute`) errored with `AttributeError: 'types.SimpleNamespace'
      object has no attribute 'realized_model'`. Their stand-in history had `potential=None,
      coupling=None`, and `_StandInPolicy.__init__` took `(task_label, cosmology, potential,
      coupling)`. Ruling 13 named only `_policy`'s stand-in. I stopped before committing.
    - **The ruling.** The stand-in history carries `realized_model=None`, and
      `_StandInPolicy.__init__` takes `(task_label, cosmology, model)`. No assertion is changed.
15. **`STRUCTURALLY REQUIRED`, a correction of ruling 13 (c) (the orchestrator's ruling after the
    stop): the shift law's criterion.**
    - **The stop.** Built as ruled (`fixed_T_value_at`, then 1 + r + f_m, 1e-12 relative), test (c)
      failed. The plain relative difference exceeded 1e-12 at 23, 31 and 25 of the 2723 samples
      for δ = −0.01, +0.03 and +0.1 M_P, at most 2.789e-11 (sample 2024).
    - **Why.** Those samples are T_J turning points: 1 + βφ′/M_P ≈ −2.2e-3 and H_J ≈ −1.3e-40.
      There `fixed_T_value_at`'s r ≈ −(1 + f_m), and 1 + r + f_m ≈ 5.4e-6 is a cancelled sum.
    - **The same states, measured otherwise** (`agent04/shift_probe.py`):
      - 3M_P²H_J²/ρ_R,J formed directly (the note's probe): at most 2.7e-14;
      - the difference on the scale 1 + f_m + |r|: at most 2.7e-14.
    - **The ruling (option A).** Keep `fixed_T_value_at` and 1e-12, and measure
      |v − v_ref e^{−2βδ/M_P}| against (1 + f_m + |r|) e^{−2βδ/M_P}. The docstring says why.
      - **`IMPLEMENTATION CHOICE`:** r in the scale is that of the φ_ref = 0 reference.
    - **Recorded:** the scaled maximum is 2.685e-14 (δ = −0.01, sample 2216), and the plain
      relative maximum is 2.789e-11 (δ = +0.03, sample 2024).
16. **`STRUCTURALLY REQUIRED`: breakage 1 is a one-ulp shift, not ruling 14's `log(exp(…))`.**
    - **Why.** At the golden Λ = 1 meV, `log(exp(4.0 * log(Λ)))` equals `4.0 * log(Λ)` bitwise
      (`-0x1.17df23d5ed618p+8` both ways). So do `log(Λ**4)` and `2.0 * log(Λ*Λ)`, and it holds for
      all 1001 Λ in 0.5–3 meV that I tried.
    - **The ruled diff.** It was replayed: every test passes under it (29 tests:
      `test_model_interface`, `test_golden_history` and `test_sampling_axis`).
    - **The replacement.** Breakage 1 is
      `nextafter(4.0 * log(float(potential._Lambda)), 0.0)`, one ulp towards zero, which is the
      kind of error the ruled breakage was meant to model. Test (a) and both golden histories fail
      under it, as ruling 14 expects.
17. **`STRUCTURALLY REQUIRED` (ruling 15): the records.**
    - **`IMPLEMENTATION CHOICE`:** the new section is numbered §8.4a, so that §8.5 onwards keep
      their numbers.
    - **`IMPLEMENTATION CHOICE`:** the five properties are removed from §8.3's code listing, which
      describes the tree; the dated note says so.
    - `numerical-strategies.md`, `CLAUDE.md` and `prompts/INDEX.md` are not edited.

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the start of each run. Outputs are in the scratch
directory `agent04/` (`before/`, `mid/`, `after/`). Every suite was run serially in the foreground,
with CLAUDE.md's commands.

| Suite | Before (`a0ca6a1`) | After (`597a215`'s tree) | Change |
|---|---|---|---|
| CosmologyModels | 18 OK, 144 s, load 10.0 | 18 OK, 208 s, load 33.0 | none |
| ComputeTargets `test_[a-f]*` | 61 OK, 201 s, load 7.5 | 61 OK, 192 s, load 13.3 | none |
| ComputeTargets `test_[g-z]*` | 127, **1 failure** (wall clock, below), 398 s, load 7.3 | **144 OK**, 232 s, load 13.7 | **+17**: `test_model_interface` |
| ObjectFactories | 86 OK, 29 s, load 45.8 | 86 OK, 11 s, load 10.1 | none |
| tools | 93 OK, 84 s, load 38.3 | 93 OK, 54 s, load 9.5 | none |
| RunDrivers | 61 OK, 65 s, load 35.8 | 61 OK, 44 s, load 10.0 | none |

- **Counts:** 18 / 188 / 86 / 93 / 61 before, and 18 / 205 / 86 / 93 / 61 after. No count fell.
- **The wall-clock test before.** `test_prym_passenger.test_a_oscillating_case_completes` (a 60 s
  bound) failed in the before-run with 93.9 s, at load 7.3 rising to over 40. Re-run alone at load
  41.2, it failed again with 76.5 s. That was before any edit. The orchestrator ruled that this is
  not a stop (Observations).
- **The mid-edit run** (before the stop): ComputeTargets 61 with 3 errors (Deviation 14) and 144
  with 1 failure (Deviation 15); ObjectFactories 86, tools 93 and RunDrivers 61 OK.
- **The wall-clock test after.** It passed in the after-suite. Re-run alone at load 10.9, it took
  28.4 s and passed.
- **`black --check`** passes on the 19 Python files changed or added. `ExponentialModel.py` was
  reformatted by black (line wrapping only) after the first after-run of the golden history
  (58.4 s, below) and before the suites.

### The golden history (ruling 13)

- **Before any edit:** 3 OK in 28.4 s, at load 12.1.
- **Through the adapter, mid-edit:** 3 OK in 13.7 s, at load 8.9.
- **After:**
  - 3 OK in 58.4 s, at load 19.2;
  - again alone, 3 OK in 9.5 s, at load 9.7.
- **Back to back**, with `a0ca6a1` in a temporary worktree (since removed) and this tree,
  alternately:
  - `a0ca6a1`: 10.9 s (load 8.5), then 24.7 s (load 9.1);
  - this tree: 13.5 s (load 8.7), then 20.3 s (load 20.1).
- **Reading the times.** The wall-clock time follows the machine's load, which other workloads set.
  The paired runs show no consistent slowdown.
- **The delegation, measured directly.** One method call through `RealizedModel` costs 70–260 ns
  more than the row's call: `log_V` 417 → 488 ns, `d_logV_dphi` 536 → 792 ns, `Omega` 603 → 583 ns,
  `d_logOmega_dphi` 203 → 274 ns. That is small beside an RHS evaluation.
- **No golden JSON was written.** `git status` shows nothing under `ComputeTargets/tests/data/`.

### The M6 witness (ruling 12)

`git grep -n -P 'bounce_region|hard_reflection_point|default_max_step' -- . ':!prompts' ':!science-runs' ':!.documents'`
prints nothing, with exit status 1. Before the edit it found the 13 lines of the two potential
files.

### A's normalization (prompt §4, second stop condition)

`git grep -n -P 'log_Omega\(|\bOmega\(|_beta_float \*|beta \* phi|beta\*phi'` outside the records
finds the README §0.2 sites:
- `HubblePolicy`;
- `fixed_T_value_at`;
- the initial ρ_R,E;
- the sampled ρ_R,J;
- `pipeline_selection` (the fifth).

It also finds 03's ln(1 + z_J) identity, `N_backward + (log_A_0 - log_Omega)`, whose φ_ref cancels:
a relabelling only. Nothing else, so no stop.

### `main.py --help` (ruling 16)

`PYTHONPATH=. ./venv/bin/python main.py --help` exits 0 on the final tree. No other driver was run,
no Ray was started, and no store was opened except the tests' temporary ones.

## The deliberate-breakage record

- **How each diff was made.** By a scratch script (`difflib.unified_diff`) from the working tree that
  `597a215` holds.
- **How each was replayed,** in `bash` (`agent04/replay.sh`): `git apply --check`, `git apply`,
  `git apply -R --check`, then `test_model_interface`, `test_golden_history` and
  `test_sampling_axis` together, then `git apply -R`. Afterwards `git diff` was byte-identical to
  its state before.
- **How far (c) misses** was measured by `agent04/c_scaled.py` with the diff applied, since the test
  stops at its first failing sample.

**1. The adapter's normalization one ulp off** (Deviation 16; ruling 14's `log(exp(…))` is a
no-op, and every test passes under it).
- **`test_model_interface`**, 1 failure:
  - `test_a_every_value_equals_the_rows`: `-279.871640558676 != -279.87164055867606`, the
    normalization against `_log_Lambda_4`.
- **`test_golden_history`**, 2 failures, `test_history_beta_1_5` and `test_history_beta_5_5`. The
  first differing field is sample 0's `H_Einstein`:
  - β = 1.5: recorded `0x1.c1b1439e4f4d7p-71`, now `0x1.c1b170520a0e0p-71`;
  - β = 5.5: recorded `0x1.699ab9fa301cfp-13`, now `0x1.699ac1338041fp-13`.

  These also differ: `RHS_evaluations`, `accepted_steps`, `compute_steps`, `first_bounce`,
  `fixed_T_values` (β = 1.5) and `samples_sha256`.
- In all: 3 failures out of 29.

```diff
--- a/CosmologyConcepts/Models/ExponentialModel.py
+++ b/CosmologyConcepts/Models/ExponentialModel.py
@@ -24,7 +24,7 @@
 the potential and coupling rows give.
 """
 
-from math import log, sqrt
+from math import log, nextafter, sqrt
 
 from ComputeTargets.exceptions import ComputationFailureError
 from CosmologyConcepts import M_value, beta_value, FieldLike, GetFieldValue
@@ -81,7 +81,7 @@
             )
 
         model = cls(0, potential._M, potential._n, coupling._beta, potential._units)
-        return model.realize(4.0 * log(float(potential._Lambda)), 0.0)
+        return model.realize(nextafter(4.0 * log(float(potential._Lambda)), 0.0), 0.0)
 
     @property
     def name(self) -> str:
```

**2. The base class's subtraction removed** (`log_Omega` returns `model._log_Omega(phi)`).
- **(b)**, 2 failures:
  - `test_b_exponential_model`: `0.015 != 0.0` at φ_ref = 0.01 M_P;
  - `test_b_a_model_that_does_nothing_about_phi_ref`: `0.02 != 0.0` at φ_ref = 0.01 M_P.
- **(c)**, 1 failure, at its first sample: 0.0296 against 1e-12 (δ = −0.01, sample 0). It fails at
  **every** sample. The scaled difference runs from 8.1e-8 to 2.955e-2 at δ = −0.01, from 2.6e-7 to
  9.42e-2 at +0.03, and from 9.5e-7 to 0.350 at +0.1. Its size is set by e^{2βδ/M_P} − 1.
- **The golden history and `test_sampling_axis` pass**, since φ_ref = 0.
- In all: 3 failures out of 29.

```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -112,7 +112,7 @@
         return self._model._d2_logV_dphi2(self._normalization, phi)
 
     def log_Omega(self, phi) -> float:
-        return self._model._log_Omega(phi) - self._log_Omega_ref
+        return self._model._log_Omega(phi)
 
     def Omega(self, phi) -> float:
         return exp(self.log_Omega(phi))
```

**3. The sign reversed** (`model._log_Omega(phi) + self._log_Omega_ref`).
- **(b)**, 2 failures:
  - `test_b_exponential_model`: `0.03 != 0.0` at φ_ref = 0.01 M_P;
  - `test_b_a_model_that_does_nothing_about_phi_ref`: `0.04 != 0.0` at φ_ref = 0.01 M_P.
- **(c)**, 1 failure, at its first sample: 0.0582 against 1e-12. It fails at **every** sample. The
  scaled difference runs from 1.6e-7 to 5.824e-2 at δ = −0.01, from 5.4e-7 to 0.197 at +0.03, and
  from 2.2e-6 to 0.822 at +0.1, that is up to |e^{4βδ/M_P} − 1|.
- **The golden history and `test_sampling_axis` pass**, since φ_ref = 0: the sign of a subtracted
  zero changes nothing.
- In all: 3 failures out of 29.

```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -112,7 +112,7 @@
         return self._model._d2_logV_dphi2(self._normalization, phi)
 
     def log_Omega(self, phi) -> float:
-        return self._model._log_Omega(phi) - self._log_Omega_ref
+        return self._model._log_Omega(phi) + self._log_Omega_ref
 
     def Omega(self, phi) -> float:
         return exp(self.log_Omega(phi))
```

## Observations not acted on

- **`test_prym_passenger.test_a_oscillating_case_completes`** is a 60 s wall-clock bound that fails
  under load:
  - before any edit: 93.9 s in the suite (load 7–40) and 76.5 s alone (load 41.2);
  - after: it passed in the suite, and took 28.4 s alone at load 10.9.

  The orchestrator ruled it not a stop. It is not edited, and no issue is opened.
- **`.documents/integrator-audit-2026-09-30/harness.py`** still reads the deleted region
  properties. It is a dated record and is not edited (ruling 12). It cannot run against this tree
  as it stands.
- **`ExponentialPotential.default_abs_tol` and `default_rel_tol`** are unread, as the deleted
  properties were. The prompt does not name them, so they are not deleted, and no issue is opened
  (ruling 12).
- **Test (a) and the golden history** are both at Λ = 1 meV. There, `log(exp(x))`, `log(Λ⁴)` and
  `2 log(Λ²)` all equal `4 log Λ` bitwise (Deviation 16). So the bitwise witnesses cannot tell such
  a rewriting from the right expression; they catch a change of value, not of form. No issue is
  opened.
- **`ODERHS`'s diagnostic branch** still prints "- potential: log(V/GeV^4)=…". Its commented-out
  line reads `self.policy.potential._M_float`. Neither is edited (ruling 8).
- **`.documents/architecture-summary.md` §7.2** still says that `ODERHS` is "passed to
  `scipy.integrate.solve_ivp`". Its §6/§7 notes of 2026-10-01 say otherwise. This prompt did not
  make it false, so it is not edited; it is 10's (C1).
- **The base class's `plain` seed** is a normalization in the exponential model's sense (ln V₀ =
  ln ρ_Λ). A model that reads the normalization otherwise must override `seed`. The docstring says
  so.

## State handed to the next prompt

- **Tree:** `597a215`, on `jordan-normalization`, on top of `a0ca6a1`.
- **The interface** (`CosmologyConcepts/Models/`; import `RealizedModel` from
  `CosmologyConcepts.Models.AbstractModel` in `ComputeTargets`, because of the import cycle,
  Deviation 10).
  - `ExponentialModel(store_id, M: M_value, n: int, beta: beta_value, units)`.
    - The normalization is ln V₀, and its coordinate is itself.
    - `renormalize(normalization, phi, V_target)` raises `ValueError` for V_target ≤ 0.
    - `effective_minimum(normalization, phi_ref, phi_0, rho_m_E_0, bracket=None)` raises
      `ValueError` naming the bracket when there is no sign change.
    - `seed(cosmology)` gives `Seed(kind, normalization, phi_ref)`, with kind tracking / static /
      plain.
    - `type_id` is `EXPONENTIAL_MODEL = 1`, and `shard_key` is the `beta_value`. 07 stores it.
  - `model.realize(normalization, phi_ref)` gives an immutable `RealizedModel` with A(φ_ref) = 1
    exactly. It pickles.
  - **The adapter** `ExponentialModel.from_rows(potential, coupling)` gives the model at
    `4.0 * log(float(Λ))`, φ_ref `0.0`, with store id 0. It is 07's to remove, together with
    `ScalarHistory.realized_model` (lazy, cached) and test (a).
- **The signatures** (one `RealizedModel` in place of a potential and a coupling):
  - `compute_scalar_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model, task_label=…, atol=…, rtol=…, verbose=…)`;
  - `ODEPolicy(task_label, cosmology, model)`, `HubblePolicy(model, units)`,
    `PotentialDerivativePolicy(task_label, cosmology, model)` (log path only),
    `AdiabaticComputePolicy(task_label, cosmology, model)`;
  - `fixed_T_value_at(result, k, N, policy, model, units)`, `fixed_T_values(result, policy, model, units)`;
  - `surfing_turning_points(result, model, interior_samples=8)`.

  `first_bounce`, `BBNData`, `pipeline_selection` and the drivers are unchanged.
- **The rows.** They are still stored and still key the history (`ScalarHistory.potential`,
  `.coupling` and `shard_key`). The region properties and `default_max_step` are gone.
- **The golden history.** `ComputeTargets/tests/data/golden_history_03.json` is unchanged and
  passes bitwise through the adapter. `golden_history.rows(beta, units)` builds the rows, and
  `run_history` passes `from_rows`. **05 regenerates it deliberately.**
- **Test (c)'s criterion** is the difference on the scale (1 + f_m + |r|) e^{−2βδ/M_P}, at 1e-12. A
  plain relative criterion fails at the T_J turning points, from cancellation (Deviation 15).
- **Counts:** CosmologyModels 18, ComputeTargets 205 (61 + 144), ObjectFactories 86, tools 93,
  RunDrivers 61.
- **Open issues.**
  - Closed: `[00-region-properties-on-the-potentials-become-unread]`.
  - Opened: none.
  - Three remain assigned to this campaign (board §3.1): two for 07 and one for 08. Two are open on
    this board.
  - `.documents/OPEN_ISSUES.md`: 24 open.
- **Breakage replay.** The three diffs above apply to `597a215`'s tree. `git apply --check`
  passes for each, and after applying each, `git apply -R --check` passes and the reverse restores
  the tree. Checked in `bash`.
- **The flaky wall-clock test** `test_prym_passenger.test_a_oscillating_case_completes` can fail
  under load. Re-run it alone before calling it real.
