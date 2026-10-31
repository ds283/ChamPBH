# Log 09 — the store

**Subject:** Store dissipation mode and key every history by alpha
**Commit:** `3984ec5`, on branch `inelastic-reflection`, on top of `03b769e`
**Date:** 2026-10-31, by Claude Opus 5.5
**Result:** **COMPLETE**, after one stop that the user ruled (D1). The gate "Store" holds. One issue
closed, none opened.
- **What shipped.**
  - α is a key of every history: there is a new replicated table `alpha_value`, and `ScalarHistory`
    gains `alpha_serial`.
  - The payload of `integrate_history` gains two keys:
    - `"rho_chi_over_rho_rad"`, r_χ at each sample, which is stored as a column of `ScalarHistoryValue`;
    - `"dark_radiation_values"`, r_χ and ΔN_eff at 1 MeV, 70 keV and today.
  - `ScalarHistory` gains 24 whole-history columns. On the object they are one record,
    `ScalarHistory.dissipation`, which is readable with `_do_not_populate`.
  - `extra_data` becomes `Text` and carries the margins' first failures under `"margins"`.
  - `AdiabaticHistory` reads ρ_χ, and computes Q on parked samples.
  - Every reader passes the α = 0 row.
  - Format 6, with a new golden file, and `VERSION_LABEL` 2026.8.0.
- **The gate "Store"** (README §8.4, prompt §4.1):
  - **The key and the columns round-trip, bitwise.**
    - Tests 1 and 2 cover them (§4.2).
    - On the eight histories of §5.1 no column and no sample field differs, raw or on the object.
    - For every converted column, object == payload held bitwise as well.
  - **Two α are two rows.** Test 3 shows it: α = 0.3 finds neither.
  - **An α = 0 row holds R24's zeros and NULLs**, shown by test 4.
  - **The new golden fingerprint, format 6 and 2026.8.0**, shown by the edited guard and golden tests
    and by test 10.
  - **`AdiabaticHistory` is bitwise at α = 0**, by two checks:
    - test 8 (a) checks the pins taken at `03b769e`;
    - §5.5 compares four setups by `repr` against `03b769e`.
  - **Q is computed on parked samples**:
    - test 8 (c);
    - §5.3: 6 332 parked samples on four histories, all finite.
  - **Both golden histories are bitwise.** `test_golden_history` and `test_tuning` pass unedited (§4.1).
- **The issue.** `[00-settling-at-physical-M-needs-a-parked-tracking-model]` is closed. It moved to
  `integrator-remediation` §4, and its index row was deleted (§1.20; the count went from 25 to 24).
- **Counts:**
  - before: 26 / 471 (96 + 375) / 103 / 115 / 64;
  - after: 26 / **473** (**98** + 375) / **121** / 115 / 64;
  - all passing.
- No store but the tests' temporary ones and one scratch store for `--inventory`, since deleted. No Ray
  was started, and nothing was written to `var/`. No existing golden was regenerated.

## 1. The derivations (prompt §1.5, ruling 9; ruling 11)

Written before any code changed (orchestrator note 09, conventions, order 2).

### 1.1 Where ρ_χ enters M²_eff/H² (prompt §1.5, P29, ruling 9)

`AdiabaticComputePolicy.M2eff_over_H2` computes

  M²_eff/H² = ∂_φ V′_eff/H² − (2 + Ḣ/H²) = 3M_P² V″/(3H²M_P²) + [conformal]/H² + 1 − (Ḣ/H² + 3),

with V′_eff = V′ + (ln A)′ (Σ ρ_R,E + ρ_m,E), and ∂_φ taken at fixed a_E and fixed comoving entropy,
as the ODE responds to δφ (`conformal_mass_over_H2`'s docstring; production-readiness log 03 §1).

**The quanta.** ρ_χ,E = K(N) e^{−4N} (P1). K changes only at a drain, never with φ at fixed N: the quanta
are collisionless, exchange nothing with the bath (U7), feel no Hartree force from the condensate (P1),
and are massless, so their trace −ρ_χ + 3p_χ vanishes. Hence

- ρ_χ does not appear in V′_eff: the source of φ is the trace of matter, (ln A)′(Σρ_R,E + ρ_m,E), and
  the quanta are traceless;
- δρ_χ,E/δφ = 0 at fixed a_E, so ∂_φ V′_eff holds no ρ_χ term either.

So ρ_χ enters M²_eff/H² only through H, and in exactly three places:

1. **The self mass**, 3M_P² V″/(3H²M_P²). V″ holds no ρ_χ; the normalization by H² does, through the
   constraint 3M_P²H²G = V + ρ_R,E(1 + f_m) + ρ_χ,E. `Vprimeprime_over_3H2Mp2` takes it as `r_chi`, through
   `V_over_3H2Mp2` (P2, 01).
2. **The gravitational term**, 1 − (Ḣ/H² + 3). Ḣ holds ρ_χ + p_χ = (4/3)ρ_χ:
   `Hdot_over_H2_plus_3` with `r_chi` gives G A₁ + C A₂ − Y R/2, Y = ρ_χ/3H²M_P² (01's derivation).
3. **The conformal term**, 3M_P² E [(ln A)″ R + (ln A)′² S]. Its numerator is the response of the source
   (ln A)′(Σρ_R,E + ρ_m,E) to δφ, which is the bath's and matter's alone, with E = ρ_R,E(1 + f_m)/3H²M_P²
   their share of 3H²M_P². ρ_χ enters only E's normalization by H². So E is
   `matter_and_dark_radiation_fractions`'s E = (G − V/3H²M_P²)(1 + f_m)/(1 + f_m + r_χ), **not**
   G − V/3H²M_P², which is E + Y and holds ρ_χ in its numerator.

**No other term holds ρ_χ.** Σ, Σ_T = dΣ/d ln T_J and x = (1/3) d ln g_s/d ln T_J are functions of the
bath's T_J; (ln A)′ and (ln A)″ are functions of φ; f_m = ρ_m,E/ρ_R,E is the bath's and matter's; the
response d ln T_J/d ln A = −1/(1 + x) is the bath's entropy conservation, which the quanta do not enter.
R and S are ratios of the bath's and matter's terms. G = 1 − π²/6M_P² is the field's. So ρ_χ enters
M²_eff otherwise than through H and E nowhere, and prompt §5's stop does not fire.

**At r_χ = 0.0** every call takes its old operations: `V_over_3H2Mp2`, `Vprimeprime_over_3H2Mp2` and
`Hdot_over_H2_plus_3` branch on `r_chi == 0.0` (01), and the conformal mass's E must branch too, since
`matter_and_dark_radiation_fractions` at r_χ = 0 multiplies and divides by 1 + f_m and is not bitwise
G − V/3H²M_P². So `AdiabaticComputePolicy` forms E as `ODEPolicy.__call__` does (`ScalarHistory.py`
:700–:706): G − V/3H²M_P² when `r_chi == 0.0`, otherwise the function's E (ruling 9). The clamp at E < 0
is unchanged.

**Ḣ/H²** (the second output of the policy) is (Ḣ/H² + 3) − 3 with the same `r_chi`.

### 1.2 BBN's density ratio and the frame factor F (ruling 11, P9)

BBN's ratio (`ComputeTargets/BBNData.py` :462–:469, and `fixed_T_value_at`) is

  r_BBN = (3M_P²H_J² − ρ_R,J(1 + f_m))/ρ_R,J.

With H_J = H_E b/A, b = 1 + (ln A)′π (`HubblePolicy`), 3M_P²H_E²G = V + ρ_R,E(1 + f_m) + ρ_χ,E (the
constraint) and ρ_R,J = A⁻⁴ρ_R,E:

  3M_P²H_J² = (V + ρ_R,E(1 + f_m) + ρ_χ,E) b²/(G A²),

  r_BBN = (A²b²/G)[V/ρ_R,E + (1 + f_m) + r_χ] − (1 + f_m).

At one state (φ, π, ρ_R,E, f_m) the ledger changes only r_χ, so the difference between the ratio with
the history's ledger and with an empty one is exactly

  Δr_BBN = F r_χ,  **F = A²(1 + (ln A)′π)²/G**.

F multiplies the bath's term (1 + f_m) in the same way, so in H_J the quanta stand to the bath as r_χ: BBN
sees ρ_χ through H_J (P9) with the weight the bath has. F is not near 1 in general: it is A² b²/G, the
square of the ratio of H_J to H_E at fixed energy, and at M = 0.5, β = 5 the orchestrator's probe found
it 0.0098–0.0107 in 1 keV–10 MeV. So prompt test 9's limit ("holds ρ_χ/ρ_R,J to a tolerance derived from
what it neglects") holds only where F ≈ 1; test 9 asserts the identity instead (ruling 11).

**The tolerance.** Δr_BBN is the difference of two ratios, each R − (1 + f_m) with R = 3M_P²H_J²/ρ_R,J
formed in floating point. `HubblePolicy` forms H_E² as exp(ln V − ln(V/3H²M_P²))/3M_P², so R carries a
relative rounding of about ε(|ln V| + |ln(V/3H²M_P²)|) from the exponent (|ln V| ≈ 277–280 in Planck
units here), and a few ε more from the products; 1 + f_m carries ε. So

  |Δr_BBN − F r_χ| ≤ c ε [(4 + |ln V| + |ln(V/3H²M_P²)|)(R₁ + R₀) + 2(1 + f_m) + F r_χ],  c = 8.

Measured (test 9, §4.2): the largest |Δr_BBN − F r_χ| over this bound is 0.032 (β = 2, M = 0.5, 1 476
samples) and 0.037 (β = 5, M = 0.5, 1 472 samples).

ΔN_eff(1 MeV) (R22) is (4/7) g_ρ r_χ, without F: it counts ρ_χ against the bath in the frame-independent
ratio r_χ, while BBN counts it through H_J with the weight F. Not acted on (§7).

## 2. What shipped

### 2.1 The schema as built

**`alpha_value`** (new table): a `sqla_dimensionless_quantity_factory(alpha_value)`.
- Columns: `serial`, `timestamp`, `value` (Float(64), indexed).
- Relative match, with the absolute match at zero.
- Registered immediately after `beta_value`, with serial batch size 200.
- Replicated, immediately after `beta_value`. Not in `read_table_config`.
- In no `--drop` group, since it is a parent of `ScalarHistory`, not a child. Two checks:
  - `tables_to_drop(["scalar-history", "adiabatic-history", "bbn-data"])` returns the nine tables of the
    three groups;
  - `tables_to_drop(["scalar-history"])` raises the same message as before, which does not name
    `alpha_value`.

**`ScalarHistory`** (`ObjectFactories/ScalarHistory.py`):

| Column | Type | Source |
|---|---|---|
| `alpha_serial` | Integer, FK `alpha_value.serial`, not null, indexed | `obj.alpha.store_id`; `build` filters on it; `inventory_spec` parent `"alpha"` |
| `injections` | Integer, nullable | candidates with `delta_K > 0` (`dissipation_values`, from `"injections"`) |
| `resolved_candidates` | Integer, nullable | candidates with `Injection.resolved` |
| `unresolved_candidates` | Integer, nullable | candidates without it |
| `injected_fraction` | Float(64), nullable | Σ `delta_rho_over_rho_rad` over the candidates, left to right from 0.0, + `Parking.delta_rho_over_rho_rad` if parked |
| `rho_chi_ratio_1MeV`, `rho_chi_ratio_70keV` | Float(64), nullable | `DarkRadiationValues`: the policy's r_χ at the first crossing of T_J = 1 MeV, 70 keV |
| `rho_chi_ratio_today` | Float(64), nullable | the policy's r_χ at N_final |
| `Delta_N_eff_1MeV` | Float(64), nullable | (4/7) g_ρ(T_J) r_χ at the 1 MeV crossing (R22) |
| `Delta_N_eff_today` | Float(64), nullable | (4/7) g_ρ (11/4)^{4/3} r_χ at N_final (P7) |
| `log_R_first_impact` | Float(64), nullable | `injections[0].classical_turning_point.log_R`; NULL with no candidate or no assessment |
| `parking_N`, `parking_log_T_Jordan` | Float(64), nullable | `Parking.N`; `Parking.log_T_Jordan − ln GeV` |
| `release_N`, `release_log_T_Jordan` | Float(64), nullable | `Release.N`; `Release.log_T_Jordan − ln GeV` |
| `worst_birth_margin`, `worst_birth_margin_N`, `birth_failing_share` | Float(64), nullable | `Margins`' fields of the same names |
| `worst_reflection_margin`, `worst_reflection_margin_N`, `reflection_failing_share` | Float(64), nullable | the same |
| `worst_tracker_margin`, `worst_tracker_margin_N`, `tracker_failing_share` | Float(64), nullable | the same |
| `unassessed_share` | Float(64), nullable | `Margins.unassessed_share` |
| `extra_data` | **Text** (was String(256)), nullable | `build_extra_data`, now with `"margins"` |

All 24 new value columns are NULL on a failure row. Each is NULL where its payload key is absent (ruling 5).
At α = 0 the first nine are 0 or 0.0 and the rest NULL (R24).

**`ScalarHistoryValue`:** `rho_chi_over_rho_rad`, Float(64), not null, from the payload's
`"rho_chi_over_rho_rad"` (P27).

**`extra_data["margins"]`**, only where the payload's `margins` is not `None`, holds:
- `first_birth_failure`, `first_reflection_failure`, `first_tracker_failure` (`MarginFailure._asdict()`
  or `null`);
- `worst_birth_margin_index`, `worst_reflection_margin_index`, `worst_tracker_margin_index`;
- `classical_generations`, `one_loop_generations`, `classical_gap_generations`, `weighted_generations`,
  `unassessed_candidates`;
- `weighted_share`, `below_weight_share`.

The blob is 586–879 characters on §5.1's histories, against 124–150 before.

### 2.2 The code

- **`ComputeTargets/ScalarHistory.py`**:
  - **New code**, a block after `fixed_T_values`:
    - the constants `NEFF_PER_G_RHO = 4/7` and `NEFF_NEUTRINO_HEATING = (11/4)^{4/3}`;
    - `DarkRadiationValues` and `dark_radiation_values(result, policy, cosmology)` (ruling 3);
    - `DissipationValues`, with its fields in the table's order, and `dissipation_values(data)`
      (rulings 5, 6, 8).
  - **The sampling loop** appends `data.rho_chi_over_rho_rad`. `integrate_history` calls
    `dark_radiation_values` after `fixed_T_values`, and the payload gains the two keys.
  - **`compute_scalar_history`** takes `alpha: float = 0.0`, last, and passes it to `tune_history`.
  - **`build_extra_data`** adds `"margins"`, read with `.get`.
  - **`ScalarHistory`**:
    - it takes `alpha` (required) and exposes `alpha` and `dissipation`;
    - `compute` checks `_alpha` and passes `alpha=self._alpha.value`;
    - `store()` sets `_dissipation = dissipation_values(data)`. It reads `"rho_chi_over_rho_rad"` with
      `.get`, and raises `RuntimeError` if it is absent while there are samples.
  - **`ScalarHistoryValue`** takes `rho_chi_over_rho_rad` as a required keyword-only argument, with a
    property.
  - **Unchanged**: the step loop, `IntegrationResult`, every record of 02–08 and `SampleValues`.
- **`ComputeTargets/AdiabaticHistory.py`** (ruling 9):
  - `M2eff_over_H2(..., T_Jordan, r_chi)` and `Hdot_over_H2(..., fm, r_chi)` take a required `r_chi` and
    pass it to `Vprimeprime_over_3H2Mp2`, `V_over_3H2Mp2` and `Hdot_over_H2_plus_3`.
  - E is `G − V/3H²M_P²` at r_χ = 0.0, else `matter_and_dark_radiation_fractions`'s E. The clamp is
    unchanged.
  - `compute_adiabatic_values` passes `value.rho_chi_over_rho_rad`.
  - `conformal_mass_over_H2`'s docstring for E is corrected.
- **`CosmologyConcepts/alpha_value.py`** (new), exported from `CosmologyConcepts/__init__.py`.
- **`ObjectFactories/ScalarHistory.py`**:
  - the columns above, the parent, the filter and the select;
  - `_dissipation_columns` and `_dissipation_from_row`, which convert ln T_J, mirroring `_tuning_*`;
  - `build` passes `alpha` and `"dissipation"`, and reads the value column;
  - `store` writes them.
- **`config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`** (format 6 with its paragraph
  and its pin, generated by `spec_digests`), and **`config/version.py`** (2026.8.0 with its history
  paragraph).
- **The readers, at α = 0 only (P30; ruling 15):**
  - **`main.py`** fetches `object_get("alpha_value", value=0.0)` in the `ray.get` beside the tolerances.
    `run_pipeline` takes `alpha` and passes it in all six lookups.
  - **`plot_inputs.PlotParameters`** gains `alpha` after `tuning_tol`. It is looked up as "the constant α",
    "α = 0", so a store without the row gives `PlotOptionNotStored`.
  - **`plot_by_beta.py` and `plot_ScalarHistory.py`** unpack it and pass it to their history payloads.
  - **`tools/history_and_bbn.py`** passes the payload's ratio to `ScalarHistoryValue` (ruling 4); its
    output is unchanged at α = 0.
  - **Not changed:**
    - `tools/bbn_from_store.py` (ruling 15; see §8);
    - `tools/inventory_report.py`. Its test's expectations moved instead (D3).
- **The documents:**
  - `.documents/architecture-summary.md`: a dated note at the end of §7.2, after 07's note;
  - `.documents/numerical-strategies.md`: §6.9.

### 2.3 The tests

| Module | Tests | Cost (load 5–7) |
|---|---|---|
| `ObjectFactories/tests/test_dissipation_store.py` | 15: tests 1–7 and 10 | ~35 s for its 4 integrations (cached), then ~1 s per test |
| `ObjectFactories/tests/test_adiabatic_dark_radiation.py` | 3: test 8 (a)–(c) | the golden integration (~5 s) plus the cached histories |
| `ComputeTargets/tests/test_bbn_sees_dark_radiation.py` | 2: test 9 | 2 integrations (2.6 s, 5.6 s) |
| `ComputeTargets/tests/dissipation_histories.py` | the shared fixture, no tests | integrations cached per process |

Each test, with what it asserts and its tolerance:
- **`test_1_every_column_raw_and_on_the_object`** (M = 0.5, β = 2, α = 0.1):
  - every one of the 24 columns, raw, against the definitions written out apart from the code
    (`expected_columns`), bitwise (`float.hex`), with ints of type `int`;
  - the object store() populates, against the payload, bitwise;
  - the object read back, against the row, bitwise; the converted columns as row + ln GeV.
- **`test_1_every_sample_ratio_bitwise`**:
  - every payload ratio equals the ledger's `ratio(raw_N, log_rhorad_Einstein)`, bitwise, with at least
    one positive;
  - `rho_chi_ratio_today` is the last sample's, bitwise;
  - every row and every read-back value equals the payload, bitwise.
- **`test_1_extra_data_margins_equal_the_payload`**: the 13 keys equal the payload's `Margins`. Each float,
  the `MarginFailure` fields included, is compared by `hex`, so the JSON round trip is exact. At least one
  first failure is non-null.
- **`test_1_the_same_with_do_not_populate`**:
  - the record equals the row;
  - `alpha` and `"margins"` are present;
  - `values` raises.
- **`test_2_parked_not_released`** (M = 10⁻³⁰, β = 2) and **`test_2_parked_and_released`** (M = 0.5,
  β = 5):
  - the parking and release columns equal the payload's records, bitwise, or are NULL;
  - `parking_N < release_N`;
  - the whole row and the object, as in test 1.
- **`test_3_each_lookup_finds_its_own_row`** and **`test_3_another_alpha_finds_neither`**:
  - α = 0 and 0.1 are two rows with distinct `alpha_value` rows, each found by its own lookup;
  - α = 0.3 finds no row.
- **`test_4_zeros_and_nulls`** and **`test_4_every_sample_ratio_is_zero`** (R24):
  - the nine zeros, as exact `0` (int) or `0.0` (float);
  - NULL elsewhere;
  - no `"margins"` key;
  - every sample's ratio exactly 0.0.
- **`test_5_every_new_column_null`**:
  - a failure row holds every new column NULL, NULL `extra_data` and no samples;
  - it reads back as a failure with `dissipation` `None`.
- **`test_6_delta_N_eff_at_1_MeV_and_today`**:
  - r_χ comes from the ledger at the crossing, and g_ρ from `G_rho` at the interpolant's T_J;
  - ΔN_eff = `(4.0 / 7.0) * g * r` and `(4.0 / 7.0) * g * (11.0 / 4.0) ** (4.0 / 3.0) * r`, written
    literally;
  - equal bitwise (the same operations, in the same order), and so is the stored row.
- **`test_7_long_extra_data_round_trips_whole`** and **`test_7_the_reflected_column_is_text`**:
  - the stored text, over 256 characters, equals `json.dumps` of the object's;
  - it reads back equal;
  - `sqla.inspect` reflects the column as `Text`, not `VARCHAR`.
- **`test_10_the_version_label_and_the_format`**: `VERSION_LABEL == "2026.8.0"`, `FINGERPRINT_FORMAT == 6`,
  and the pin has `alpha_value`.
- **`test_8a_alpha_zero_is_the_dispatch_commits`**:
  - the tuned golden setup at α = 0, stored and read back;
  - `repr(max_abs_Q_values)` and the SHA-256 of `repr((raw_N_grid, [abs_Q_samples[l] for l in Q_labels]))`
    equal the pins, exactly (§4.3);
  - there are 5 960 samples.
- **`test_8b_alpha_reads_rho_chi`** (M = 0.5, β = 2, α = 0.1, stored and read back):
  - at every sample, M²_eff/H² and Ḣ/H² equal a recomputation from the policy's parts, with `r_chi` and
    §1.1's E, bitwise;
  - every |Q| from `compute_adiabatic_values` equals Q formed from the recomputed grids by `Q_numerator`,
    bitwise;
  - more than 1 000 samples have r_χ > 0.
- **`test_8c_Q_is_finite_on_the_parked_samples`** (M = 10⁻³⁰, β = 2): more than 1 000 parked samples, and
  no non-finite |Q| at any label.
- **`test_9_the_identity_on_test_1s_history`** and **`test_9_the_identity_where_F_is_far_from_1`**:
  - at every sample in BBN's window with r_χ > 0, |(r_BBN with the ledger) − (r_BBN without) − F r_χ| is
    within §1.2's bound;
  - the H_J it reads is the sample's own, bitwise;
  - at β = 5, min F < 0.05.

**Existing tests edited** (each the "α row" of §1.9, the format/version move, ruling 1's pins, or the
user's ruling of D1):

| Module | Edit | Reason |
|---|---|---|
| `ObjectFactories/tests/test_version_keyed_lookups.py` | `_alpha = _stand_in(17)`; `alpha=_alpha` in `_scalar_history_query`; `alpha_serial=17` in `_insert_failed_scalar_history` | the shared harness builds and looks up a history; `test_foreign_bbn_provenance` (3 tests) and every module importing the helpers pass through it, unedited |
| `ObjectFactories/tests/test_scalar_history_failure_lookup.py` | `alpha_serial=17` in `_key_columns` | inserts history rows |
| `ObjectFactories/tests/test_scalarhistory_failure_reason.py` | imports `_alpha`; `alpha=_alpha` in its two `ScalarHistory(...)` | constructs histories |
| `ObjectFactories/tests/test_first_bounce_round_trip.py` | imports `_alpha`; `alpha=_alpha` in `_model` | constructs a history (`test_fixed_T_values_round_trip` imports `_model`, unedited) |
| `ObjectFactories/tests/test_tuned_history_round_trip.py` | `self.alpha = get("alpha_value", value=0.0)`; `alpha=` in `query()` and `write()` | builds and looks up a history |
| `ObjectFactories/tests/test_datastore_config.py` | `alpha_value` in `EXPECTED_FACTORIES` and `EXPECTED_SERIAL_BATCH_SIZES` after `beta_value` | ruling 1 |
| `ObjectFactories/tests/test_inventory_specs.py` | `alpha_value` in `EXPECTED_ORDER`; `test_twelve_definitions_serve_fifteen_classes` renamed `…_sixteen_classes`, 15 → 16 owners, `alpha_value` among the quantities with leaf `value` | ruling 1 |
| `ObjectFactories/tests/test_fingerprint_format_guard.py` | docstring counts; 15 → 16 pairs | format 6 |
| `ObjectFactories/tests/test_store_fingerprint_golden.py` | the Format 6 paragraph; `GOLDEN` → `store_fingerprint_inelastic-reflection-09.json`; an `alpha_value` row and `alpha_serial`; `DISSIPATION_COLUMNS` literals; 15 → 16 classes; format 5 → 6 | format 6 (prompt §1.6) |
| `ComputeTargets/tests/test_plot_inputs.py` | `_build_run` fetches the α = 0 row with the tolerances; `params.alpha.value == 0.0`; the α lookup in `test_the_parameters` | main.py's lookups and `plot_inputs.parameters` gain the α row (P30) |
| `ComputeTargets/tests/test_adiabatic_mass.py` | `_M2eff` passes `r_chi = 0.0` when the signature has it; the stand-in samples gain `rho_chi_over_rho_rad=0.0` | the user's ruling of D1, 2026-10-31: "(b) is fine" |
| `tools/tests/test_compare_stores.py` | an `alpha_value` row at `o + 9` on every shard; `alpha_serial`; `rho_chi_over_rho_rad` among the value columns | inserts history and value rows by hand |
| `tools/tests/test_inventory_report.py` | an `alpha_value` row and `alpha_serial`; 15 → 16 classes and 44 → 45 records; `, alpha=0` in the three history lines | inserts history rows; D3 |

The tests §1.9 names pass unedited, except `test_adiabatic_mass` (D1):
- the golden histories, the tuned one included: `test_golden_history`, `test_tuning`;
- `test_kinematic_cap_loop`, `test_reflection_reporting`, `test_injection`, `test_parking`,
  `test_release`, `test_margins`, `test_wall_model`, `test_wall_rounding`.

## 3. Deviations from the prompt

**The rulings of orchestrator note 09 (1–17):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | `test_datastore_config`, `test_inventory_specs` gain `alpha_value`; listed in §2.3 | STRUCTURALLY REQUIRED |
| R2 | `alpha_value.py` mirrors `beta_value.py`; registered after `beta_value` (batch 200), replicated after it; not in `read_table_config`; no drop group (§2.1 shows `tables_to_drop`) | IMPLEMENTATION CHOICE (ruled) |
| R3 | `DarkRadiationValues` under `"dark_radiation_values"`; `dark_radiation_values` after `fixed_T_values` with its crossing, state and g_ρ; the two constants in the stated order; computed at every α; `None` where not reached. `rho_chi_ratio_today` equals the last sample's ratio bitwise on all eight histories of §5.1 and in test 1 | IMPLEMENTATION CHOICE (ruled) |
| R4 | The loop appends `data.rho_chi_over_rho_rad`; `ScalarHistoryValue` takes it keyword-only with no default; `tools/history_and_bbn.py` passes the payload's; no spline | STRUCTURALLY REQUIRED (the tool), IMPLEMENTATION CHOICE (ruled) |
| R5 | `store()` reads the six keys with `.get`; absent → NULL; absent ratio with samples → `RuntimeError`. The stand-in payloads of `test_first_bounce_round_trip` and `test_fixed_T_values_round_trip` pass with the α row only | STRUCTURALLY REQUIRED |
| R6 | As ruled; `injected_fraction` from 0.0 left to right, the leftover last | IMPLEMENTATION CHOICE (ruled) |
| R7 | Raw == forward conversion and object == row + ln GeV, bitwise. Object == payload for the converted columns: test 1's history neither parks nor is released (both NULL), so nothing to compare there; on the six parked histories of §5.1 it held bitwise for every converted column (`measure.py stored`, no mismatch) | IMPLEMENTATION CHOICE (ruled) |
| R8 | `alpha` required beside `tuning_tol`, checked in `compute`; `DissipationValues` in the table's order as `dissipation`, ln T_J in the cosmology's units, `None` on a failure, read with `_do_not_populate` | IMPLEMENTATION CHOICE (ruled) |
| R9 | Required `r_chi`; E by the ODE's branch, verbatim; the derivation (§1.1) written before the code | IMPLEMENTATION CHOICE / STRUCTURALLY REQUIRED; see D1 |
| R10 | Pins taken in a worktree of `03b769e`, stored and read back through `_TempStoreCase` (§4.3) | IMPLEMENTATION CHOICE (ruled) |
| R11 | Test 9 asserts F r_χ with §1.2's bound; F derived (§1.2) and reported at 1 MeV on every history (§5.2); ΔN_eff's lack of F under §7 | STRUCTURALLY REQUIRED |
| R12 | §5.2 gives the ratio, K_f/K₁, the g_s factor and the residual. The residual, −1.328×10⁻³ on every history, is far above 100 rtol and is explained there | IMPLEMENTATION CHOICE (ruled) |
| R13 | §5.3 locates each parked maximum against `Parking.N`; §5.4 gives the absolute change beside the relative one | IMPLEMENTATION CHOICE (ruled) |
| R14 | One worktree of `03b769e`, removed before the commit; the stand-in proxy at the dispatch tree; §5.5 by `repr` without `metadata`'s times and `z_grid`'s address | IMPLEMENTATION CHOICE (ruled) |
| R15 | `bbn_from_store.py` unchanged (§8); `inventory_report.py` unchanged (D3); `PlotParameters.alpha` as ruled; `main.py` in the tolerances' `ray.get` | IMPLEMENTATION CHOICE (ruled) |
| R16 | The files are §6's list, `tools/history_and_bbn.py`, ruling 1's tests and the tests D1 and D3 add. Golden modules and existing golden JSONs are byte-identical, as are the frozen tests except `test_adiabatic_mass` (D1). `CLAUDE.md` is untouched. `ScalarHistory.py`'s hunks are those listed, plus D4 and D5 | as ruled, with D1, D3–D5 |
| R17 | Costs: the minimum of three rounds, with `uptime`. Suites serial, halves, to scratch with `uptime`. §5's histories one at a time, none near 15 min. No failure needed a rerun | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | **A stop, ruled by the user.** Ruling 9's required `r_chi` and `compute_adiabatic_values` reading `value.rho_chi_over_rho_rad` fail `test_adiabatic_mass`, which §1.9 and ruling 16 freeze: 8 `TypeError`s (`_M2eff` calls the policy positionally) and 3 `AttributeError`s (its stand-in samples), 11 errors in 9 tests. I stopped. The user ruled, 2026-10-31: **"(b) is fine"**: `_M2eff` passes `r_chi = 0.0` when the signature has it, and the stand-in samples gain `rho_chi_over_rho_rad=0.0`. Nothing else in that file changes, and ruling 9 stands. At r_χ = 0.0 every call takes its old operations, so what the test compares is unchanged | STRUCTURALLY REQUIRED (ruling 9 against ruling 16) |
| D2 | A module-level helper `dissipation_values(data)` beside the records, called by `ScalarHistory.store()`, and `_dissipation_columns`/`_dissipation_from_row` in the factory, mirroring the tuning's | IMPLEMENTATION CHOICE |
| D3 | `tools/tests/test_inventory_report.py`'s expectations move by the α row: its store gains an `alpha_value` row (the α row the history now needs), so the report counts 16 classes and 45 records, lists `alpha_value` under "Other classes", and prints `alpha=0` on each history line. The report prints any key field generically. The tool is unchanged. Placing α among the parameters and in the common line is 10's (the inventory's α) | STRUCTURALLY REQUIRED |
| D4 | `ScalarHistory.py` gains `alpha_value` in its `CosmologyConcepts` import, which the annotation needs | STRUCTURALLY REQUIRED |
| D5 | Stale prose corrected where this commit makes it false: `integrate_history`'s `:param alpha:` and `:return:`, and one sentence of `tune_history`'s docstring ("compute_scalar_history does not take it"); `config/datastore.py`'s docstring (six → seven quantity entries); `plot_inputs.py`'s docstring; `conformal_mass_over_H2`'s `:param E:` | IMPLEMENTATION CHOICE |
| D6 | Test 9 runs on two histories: test 1's, and M = 0.5, β = 5, where F ≈ 0.01, to show the identity where the prompt's limit fails. At test 1's history F is 0.030–0.088, not near 1 either | IMPLEMENTATION CHOICE |
| D7 | Test 1 also checks each payload ratio against the ledger's, which catches B2. A round trip alone carries a shifted ratio through unchanged | IMPLEMENTATION CHOICE |
| D8 | The architecture note sits at the end of §7.2, after 07's, where the store and the payload notes are. 08's note is in §8.4a, about the model methods | IMPLEMENTATION CHOICE |
| D9 | §2.6's dispatch costs were first taken at a load of 21–47 (another workload). They were retaken at 5–6 to compare with the final tree (5–7). Both are kept in scratch, and the table uses the second | IMPLEMENTATION CHOICE |

## 4. Verification performed

### 4.1 The suites

These were run serially with `CLAUDE.md`'s commands, ComputeTargets in two halves, with the output in my
scratch directory:
- "before" was at `03b769e`, before any edit;
- "after" was on the final tree.

| Suite | Before (`03b769e`) | After | Change |
|---|---|---|---|
| CosmologyModels | 26 OK, 64.6 s (load 8.0) | 26 OK, 70.9 s (load 6.5) | 0 |
| ComputeTargets `test_[a-f]` | 96 OK, 120.0 s (load 10.7) | **98** OK, 128.2 s (load 4.3) | +2: `test_bbn_sees_dark_radiation` |
| ComputeTargets `test_[g-z]` | 375 OK, 431.8 s (load 4.2) | 375 OK, 451.5 s (load 15.6) | 0 |
| ObjectFactories | 103 OK, 14.5 s (load 3.7) | **121** OK, 66.8 s (load 7.6) | +18: `test_dissipation_store` (15), `test_adiabatic_dark_radiation` (3) |
| tools | 115 OK, 119.7 s (load 3.1) | 115 OK, 128.6 s (load 19.3) | 0 |
| RunDrivers | 64 OK, 22.1 s (load 3.2) | 64 OK, 23.5 s (load 14.7) | 0 |

The loads are the one-minute averages from `uptime` at each suite's end. Before ran 15:09–15:23, after
20:07–20:22. Each count's change is the new tests'. The tests §1.9 names pass unedited, except
`test_adiabatic_mass` (D1).

- `black --check` is clean on the 31 Python files changed or added.
- `main.py --help`, `plot_by_beta.py --help`, `plot_ScalarHistory.py --help` and
  `RunDrivers/registered_run.py --help` each exit 0.
- **`main.py --inventory`** was run on a temporary store, built in my scratch directory by
  `_DissipationStoreCase`. It held the α = 0 and α = 0.1 histories of test 3. It was given a
  `QCD_Cosmology` row, since the harness's cosmology is a stand-in, and was deleted afterwards. The
  result: exit 0, `problems: none`, 16 classes, and `@@ alpha_value (replicated): 2 records`,
  `values = [ 0, 0.1 ]`. The history lines read
  `… model=ExponentialModel(M=1.2177e+27 eV, beta=2, n_value=1), failure=False, alpha=0  | values: 5,287`
  and `… alpha=0.1  | values: 5,287`: the parent `alpha` is resolved.

### 4.2 The new tests

The clean run of the three modules: 20 tests, OK, 49.8 s at a load of 5. Test 9 measured:
- β = 2: 1 476 samples, F in [0.0302, 0.0878], largest miss/bound 0.032;
- β = 5: 1 472 samples, F in [0.00943, 0.0111], largest miss/bound 0.037.

### 4.3 Test 8's pins (ruling 10)

They were taken in a temporary worktree of `03b769e`, since removed, by
`PYTHONPATH=. venv/bin/python measure.py pins out` run from that worktree. Mode `pins` of the script is
quoted whole below; the script's harness classes `Store` and `Harness` wrap `_TempStoreCase`, as the
test's `_DissipationStoreCase` does.

```python
def mode_pins():
    import json
    golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
    tuning = {k: (float.fromhex(v) if isinstance(v, str) and k != "seed_kind" else v)
              for k, v in golden["tuning"].items()}
    realized = gth.make_model(U).realize(tuning["normalization"], tuning["phi_ref"])
    with quiet():
        data = SM.integrate_history(COS, temperature(0, gth.T_INIT_GEV * U.GeV), COS.T_CMB_Kelvin * U.Kelvin,
            phi_value(0, gth.PHI_INIT_OVER_MP * MP), pi_value(0, gth.PI_INIT), ZGRID, realized,
            task_label=gth.TASK_LABEL, atol=gth.ATOL, rtol=gth.RTOL)
    payload = dict(data)
    payload.update({k: tuning[k] for k in SM.TUNING_OUTPUT_KEYS})
    s = Store()          # _TempStoreCase, with test_injection's parameter rows (= the golden setup's)
    try:
        get = s.store.object_get
        s.T_init = get("temperature", value=gth.T_INIT_GEV * U.GeV, units=U)
        s.phi = get("phi_value", value=gth.PHI_INIT_OVER_MP * MP, units=U)
        s.pi = get("pi_value", value=gth.PI_INIT, units=U)
        s.tuning_tol = get("tolerance", tol=gth.TOLERANCE)
        _, model, _ = s.write(payload, gth.BETA, gth.M_OVER_MP, 0.0)   # ScalarHistory.store() + factory store
        got, _ = s.read(model, 0.0)                                     # object_get
        with quiet():
            out = AH.compute_adiabatic_values._function(NS(get=lambda: got), AH.AdiabaticHistory.Q_labels, "pins")
    finally:
        s.close()
    rep = repr((out["raw_N_grid"], [out["abs_Q_samples"][l] for l in AH.AdiabaticHistory.Q_labels]))
    return dict(max_abs_Q_values=repr(out["max_abs_Q_values"]),
                sha256=hashlib.sha256(rep.encode()).hexdigest(), n=len(out["raw_N_grid"]))
```

`ZGRID` is the golden axis with store ids k + 1. The result:
- `max_abs_Q_values` = `{'kp_over_H_1E1': 18.9909249914034, 'kp_over_H_1E2': 1.2320960122983178,
  'kp_over_H_1E3': 0.016026391467678723, 'kp_over_H_1E4': 5.683352478895833e-05}`;
- SHA-256 `a59c5cc762e2cdd2db0d51b005099d25050a1621634d293ff54bb39114f96c0f`;
- 5 960 samples.

## 5. The measurement (prompt §2)

Every history is `test_injection`'s setup at α = 0.1, untuned. The script is `measure.py`, in my scratch
directory, run in each tree. The final tree's runs were at 20:03–20:06, at a load of 4–7.

### 5.1 The round trip at scale (§2.1)

The eight histories were stored on one temporary store and read back. On every history:
- every one of the 24 columns: raw == forward conversion, and object == inverse == payload, bitwise;
- every sample's 18 `SampleValues` fields and its ratio, bitwise;
- `extra_data` equals `json.loads(json.dumps(build_extra_data(payload)))`.

No difference was found.

| History | Samples | Integration (s) | Store (s) | Read (s) | Read, `_do_not_populate` (s) | `extra_data` (chars) |
|---|---|---|---|---|---|---|
| M = 0.5, β = 1.2 | 4 896 | 1.5 | 0.133 | 0.061 | 0.0012 | 847 |
| M = 0.5, β = 2 | 5 287 | 2.6 | 0.138 | 0.060 | 0.0012 | 854 |
| M = 0.5, β = 5 | 6 814 | 5.6 | 0.185 | 0.080 | 0.0013 | 853 |
| M = 0.5, β = 25 | 17 352 | 6.2 | 0.455 | 0.293 | 0.0014 | 852 |
| M = 10⁻³, β = 1.2 | 4 998 | 1.6 | 0.235 | 0.061 | 0.0013 | 848 |
| β = 2, M = 10⁻¹² | 5 437 | 6.3 | 0.163 | 0.067 | 0.0014 | 879 |
| β = 2, M = 10⁻¹⁶ | 5 437 | 6.3 | 0.155 | 0.064 | 0.0013 | 879 |
| β = 2, M = 10⁻³⁰ | 5 437 | 6.4 | 0.149 | 0.066 | 0.0015 | 586 |

### 5.2 The stored values (§2.2, rulings 11 and 12)

Each column of §1.3 is shown with ln T_J as T_J.

| Column | M = 0.5, β = 1.2 | M = 0.5, β = 2 | M = 0.5, β = 5 | M = 0.5, β = 25 | M = 10⁻³, β = 1.2 | β = 2, M = 10⁻¹² | β = 2, M = 10⁻¹⁶ | β = 2, M = 10⁻³⁰ |
|---|---|---|---|---|---|---|---|---|
| `injections` | 7 | 28 | 121 | 85 | 7 | 49 | 49 | 49 |
| `resolved_candidates` | 7 | 28 | 121 | 85 | 7 | 0 | 0 | 0 |
| `unresolved_candidates` | 0 | 0 | 0 | 0 | 0 | 49 | 49 | 49 |
| `injected_fraction` | 0.0150439 | 0.0124401 | 0.00196674 | 0.000419997 | 0.0150924 | 0.0126169 | 0.0126169 | 0.0126169 |
| `rho_chi_ratio_1MeV` | 0.0100759 | 0.00815523 | 0.000957634 | 0.000198111 | 0.0100882 | 0.00823907 | 0.00823907 | 0.00823907 |
| `rho_chi_ratio_70keV` | 0.00881749 | 0.00694237 | 0.000802683 | 0.000166056 | 0.00885189 | 0.00706894 | 0.00706894 | 0.00706894 |
| `rho_chi_ratio_today` | 0.00878255 | 0.00691503 | 0.000799494 | 0.000165406 | 0.00881722 | 0.00704086 | 0.00704086 | 0.00704086 |
| `Delta_N_eff_1MeV` | 0.0607171 | 0.0491432 | 0.00577067 | 0.00119381 | 0.0607913 | 0.0496484 | 0.0496484 | 0.0496484 |
| `Delta_N_eff_today` | 0.0654126 | 0.0515033 | 0.00595465 | 0.00123195 | 0.0656709 | 0.0524405 | 0.0524405 | 0.0524405 |
| `log_R_first_impact` | −143.943 | −141.15 | −122.846 | −130.547 | −114.358 | −27.0837 | 9.75769 | 138.702 |
| `parking_N` | NULL | NULL | 38.0044 | 132.551 | 28.3719 | 30.3493 | 30.3493 | 30.3493 |
| `parking_log_T_Jordan` (as T_J) | NULL | NULL | 81.2 MeV | 9.97 GeV | 11.47 keV | 83.63 keV | 83.63 keV | 83.63 keV |
| `release_N` | NULL | NULL | 40.2545 | 145.779 | 45.9102 | NULL | NULL | NULL |
| `release_log_T_Jordan` (as T_J) | NULL | NULL | 9.745 MeV | 40.27 keV | 0.2665 meV | NULL | NULL | NULL |
| `worst_birth_margin` | −0.710045 | −0.709454 | −0.703936 | −0.703089 | −0.708921 | −0.708333 | −0.708333 | 20.9043 |
| `worst_birth_margin_N` | 26.2986 | 29.2286 | 35.3208 | 130.765 | 26.2843 | 29.2667 | 29.2667 | 29.2667 |
| `birth_failing_share` | 0.999991 | 0.997089 | 0.991366 | 0.986658 | 0.999944 | 0.992405 | 0.992405 | 0 |
| `worst_reflection_margin` | 0.0143617 | −0.527273 | −0.640427 | −0.667675 | 0.00473542 | −0.554696 | −0.554696 | 20.9351 |
| `worst_reflection_margin_N` | 25.8458 | 29.3509 | 35.3675 | 130.765 | 25.8465 | 29.3425 | 29.3425 | 29.3425 |
| `reflection_failing_share` | 0.0384266 | 0.99425 | 0.991366 | 0.986658 | 0.0404475 | 0.989063 | 0.989063 | 0 |
| `worst_tracker_margin` | 7.37588 | 8.14441 | 8.58586 | 20.0219 | 10.1987 | 15.5492 | 20.448 | 37.6562 |
| `worst_tracker_margin_N` | 29.263 | 33.8614 | 43.6128 | 147.465 | 28.3742 | 30.3563 | 30.3563 | 30.3563 |
| `tracker_failing_share` | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `unassessed_share` | 0 | 0 | 0.00087571 | 0.00227443 | 5.04645×10⁻⁵ | 0.00011796 | 0.00011796 | 0.00011796 |

The ΔN_eff agree with the planning probe's (0.0491/0.0515 at M = 0.5, β = 2; 0.00577/0.00595 at β = 5), which
took the nearest samples rather than the crossing.

**ΔN_eff(T_CMB)/ΔN_eff(1 MeV) against the closed form (ruling 12)**, (K_f/K₁)·[(11/4) g_s(T_CMB)/g_s(1 MeV)]^{4/3}.
The g_s factor uses the cosmology's G_s at the two T_J, and F is taken at the 1 MeV crossing.

| History | Share of K after 1 MeV | Ratio | K_f/K₁ | g_s factor | Closed form | Residual | F at 1 MeV |
|---|---|---|---|---|---|---|---|
| M = 0.5, β = 1.2 | 0.04219 | 1.077334 | 1.044047 | 1.033254 | 1.078766 | −1.328×10⁻³ | 0.1134 |
| M = 0.5, β = 2 | 0.01540 | 1.048026 | 1.015645 | 1.033254 | 1.049420 | −1.328×10⁻³ | 0.04943 |
| M = 0.5, β = 5 | 0.00000 | 1.031882 | 1.000000 | 1.033254 | 1.033254 | −1.328×10⁻³ | 0.009872 |
| M = 0.5, β = 25 | 0.00006 | 1.031947 | 1.000063 | 1.033254 | 1.033319 | −1.328×10⁻³ | 4.39×10⁻⁵ |
| M = 10⁻³, β = 1.2 | 0.04479 | 1.080268 | 1.046891 | 1.033254 | 1.081704 | −1.328×10⁻³ | 1.018 |
| β = 2, M = 10⁻¹² | 0.02306 | 1.056237 | 1.023603 | 1.033254 | 1.057642 | −1.328×10⁻³ | 0.9331 |
| β = 2, M = 10⁻¹⁶ | 0.02306 | 1.056237 | 1.023603 | 1.033254 | 1.057642 | −1.328×10⁻³ | 0.9331 |
| β = 2, M = 10⁻³⁰ | 0.02306 | 1.056237 | 1.023603 | 1.033254 | 1.057642 | −1.328×10⁻³ | 0.9331 |

- **The ratio is explained** by the share injected after 1 MeV (K_f/K₁ = 1/(1 − share)) and by the bath's g_s
  between the two temperatures, as the closed form says. The g_s factor is 1.0333 on every history.
- **The residual is −1.328×10⁻³ on every history.** This is far above 100 rtol (10⁻⁶), so it was examined
  (ruling 12), on M = 0.5, β = 2:
  - **The temperature law holds.** ln(T_J a_J g_s^{1/3}) is −22.372421481 both at the first sample below
    1 MeV and at the last; (ratio)⁻⁴ − 1 = 2.0×10⁻¹⁰.
  - **What does not hold is ρ_R,J = (π²/30) g_ρ(T_J) T_J⁴.** The stored ρ_R,J lies below it by 0.2104 %
    (ln) near 1 MeV and by 0.0776 % today. The ratio of the two offsets is 1.0013295, the residual exactly.
  - The ODE evolves ρ_R,E through Σ = 1 − 3w(T_J). It sets ρ_R = (π²/30) g_ρ T⁴ only at T*, so the EOS's w
    and its g_ρ, g_s drift apart by about 10⁻³ over the history.
  - ΔN_eff uses the cosmology's g_ρ with the stored r_χ, as R22 asks, so it inherits that 10⁻³. Not acted on
    (§7).
- **F at 1 MeV** (ruling 11) is 0.93–1.02 at β = 2 below M = 10⁻¹² and at M = 10⁻³, β = 1.2. At
  M = 0.5 M_P it is 0.11, 0.049, 0.0099 and 4.4×10⁻⁵ for β = 1.2, 2, 5 and 25.

### 5.3 Q on the parked samples (§2.3, ruling 13; the final tree)

The parked samples are those with Parking.N < N ≤ Release.N (or N_final). No |Q| is non-finite at any label
on any history, so the stop of prompt §5 does not fire.

| History | Parked samples | k/H | max \|Q\| parked (#, N, N − Parking.N) | Where | max \|Q\| elsewhere (#, N) |
|---|---|---|---|---|---|
| β = 2, M = 10⁻³⁰ | 2 142 of 5 437 | 10, 10², 10³, 10⁴ | 2.099×10⁻⁸ each (#5436, 50.0756, 19.73) | the last sample | 4.056, 3.977×10⁻³, 3.976×10⁻⁶, 3.976×10⁻⁹ (#3294, 30.3471) |
| β = 2, M = 10⁻¹⁶ | 2 142 of 5 437 | 10, 10² | 6.636×10⁻⁵ (#5436, 50.0756, 19.73) | the last sample | 2.557, 2.506×10⁻³ (#3294, 30.3471) |
| | | 10³, 10⁴ | 6.608×10⁻⁵, 4.573×10⁻⁵ (#5436) | the last sample | 2.506×10⁻⁶, 2.506×10⁻⁹ (#3294) |
| M = 10⁻³, β = 1.2 (released) | 1 904 of 4 998 | 10, 10² | 2.281, 0.02308 (#3080, 28.374155, 0.0022) | the first 0.01 e-fold after the switch | 3.696, 4.952×10⁻³ (#3079, 28.364945) |
| | | 10³ | 1.463×10⁻⁴ (#4184, 38.542371, 10.17) | neither | 4.968×10⁻⁶ (#3079) |
| | | 10⁴ | 2.164×10⁻⁷ (#4173, 38.441057, 10.07) | neither | 4.968×10⁻⁹ (#3079) |
| M = 0.5, β = 5 (released) | 244 of 6 814 | 10 | 0.1859 (#4363, 40.186521, 2.182) | neither | 19.68 (#3525, 32.468256) |
| | | 10² | 6.921×10⁻³ (#4277, 39.394432, 1.390) | neither | 1.143 (#3622, 33.361659) |
| | | 10³, 10⁴ | 2.275×10⁻⁵, 2.325×10⁻⁸ (#4128, 38.022091, 0.0177) | neither (just past 0.01) | 0.0169, 2.733×10⁻⁵ (#3526, 32.477466) |

At M = 10⁻³, β = 1.2 the parked maximum at k/H ≤ 10² lies 0.0022 e-folds after the switch, next to the
largest maximum elsewhere (#3079, the last sample before it). The spline of asinh m carries the switch's
step into the first parked sample, as the orchestrator's probe found.

### 5.4 What ρ_χ changes in `AdiabaticHistory` (§2.4, ruling 13)

This is the final tree against `03b769e`, on item 3's histories at α = 0.1. The samples are equal on both
trees, by the SHA of `repr(sample)`.

| History | Largest rel. change of M²_eff/H² (N, m) | Largest \|Δm\|/max(\|m\|, 1) (N, m) | Largest rel. change of Ḣ/H² (N) | max \|Q\| rel. change, k/H = 10, 10², 10³, 10⁴ | Samples bitwise unchanged |
|---|---|---|---|---|---|
| β = 2, M = 10⁻³⁰ | 1.083×10⁻² (23.7249, −0.0021) | 9.08×10⁻³ (22.2696, −4.68) | 6.98×10⁻⁴ (21.9288) | 4.86×10⁻³, 4.72×10⁻³, 4.72×10⁻³, 3.2×10⁻⁷ | 2 209 of 5 437 |
| β = 2, M = 10⁻¹⁶ | 1.083×10⁻² (23.7249, −0.0021) | 9.08×10⁻³ (22.2696, −4.68) | 6.98×10⁻⁴ (21.9288) | 4.97×10⁻³, 4.83×10⁻³, 3.2×10⁻⁷, 1.1×10⁻⁷ | 2 209 of 5 437 |
| M = 10⁻³, β = 1.2 | 1.235×10⁻¹ (19.6520, −6.1×10⁻⁴) | 1.249×10⁻² (17.6717, 1.34) | 9.85×10⁻⁴ (17.8375) | 7.69×10⁻³, 6.99×10⁻³, 2.04×10⁻³, 3.31×10⁻³ | 1 918 of 4 998 |
| M = 0.5, β = 5 | 1.741×10⁻³ (34.9550, 3 795) | 1.741×10⁻³ (34.9550, 3 795) | 1.12×10⁻⁴ (36.8800) | 3.7×10⁻⁴, 5.8×10⁻⁴, 7.9×10⁻⁴, 1.45×10⁻³ | 3 227 of 6 814 |

The 12 % at M = 10⁻³ is near a zero of M²_eff (m = −6×10⁻⁴), as the orchestrator expected. The absolute
measure there is 1.25 %. The unchanged samples are those before the first drain, where r_χ = 0.0 exactly.

### 5.5 What does not change (§2.5, ruling 14)

These were compared by `repr` against `03b769e`, at α = 0, on 26 payload keys each. `metadata` was compared
by its steps and RHS evaluations only, and `z_grid` by (store id, ln(1 + z)).

| Setup | Keys differing | `compute_adiabatic_values` (max \|Q\|, every \|Q\|) | New keys |
|---|---|---|---|
| golden, β = 1.5 (to 1 MeV) | none | equal | 2 723 ratios all 0.0; record: 1 MeV and 70 keV `None` (the history stops at 1 MeV), today 0.0, ΔN_eff today 0.0 |
| golden, β = 5.5 | none | equal | 4 894 ratios 0.0; 1 MeV 0.0, 70 keV `None`, today 0.0 |
| tuned golden (its final integration) | none | equal | 5 960 ratios 0.0; every field 0.0 |
| natural mode, M = 0.5, β = 2 | none | equal | 5 287 ratios 0.0; every field 0.0 |

The tuned golden history passes `test_tuning` and `test_golden_history` unedited (§4.1). At α = 0.1 the
samples of item 3's four histories are bitwise too (§5.4), so the integration is unchanged.

### 5.6 The cost (§2.6, ruling 17)

Each figure is the minimum of three rounds, in seconds, for each step:
- the store: `ScalarHistory.store()` and the factory's;
- the read;
- the read with `_do_not_populate`;
- `compute_adiabatic_values` on the read-back history.

| History | `03b769e` (load 5–6) | Final (load 4–7) |
|---|---|---|
| M = 0.5, β = 2 | 0.1327 / 0.0572 / 0.00086 / 0.0652 | 0.1349 / 0.0599 / 0.00120 / 0.0668 |
| β = 2, M = 10⁻³⁰ | 0.1361 / 0.0584 / 0.00085 / 0.0655 | 0.1375 / 0.0638 / 0.00115 / 0.0672 |
| β = 2, M = 10⁻¹⁶ | 0.1354 / 0.0581 / 0.00084 / 0.0656 | 0.1369 / 0.0615 / 0.00113 / 0.0673 |
| M = 10⁻³, β = 1.2 | 0.1244 / 0.0538 / 0.00080 / 0.0590 | 0.1282 / 0.0573 / 0.00117 / 0.0611 |
| M = 0.5, β = 5 | 0.1724 / 0.0724 / 0.00085 / 0.0896 | 0.1736 / 0.0799 / 0.00118 / 0.0923 |

The `uptime` lines were 20:06–20:07 for the dispatch runs (load 5.1–6.3) and 20:04 for the final ones
(load 4.2–5.1).
- The store costs +1–3 %, and the read +5–10 % for one more value column.
- The read with `_do_not_populate` rises from 0.85 to 1.2 ms, for 25 more columns.
- `compute_adiabatic_values` costs +1–3 %.
- The dispatch tree's first rounds, at a load of 21–47, are kept in scratch (D9).

## 6. The deliberate-breakage record (P11)

Each diff below is on the final tree. Each was checked with `git apply --check` and, once applied, with
`git apply -R --check`. Each was replayed in `bash` (`breaks/replay.sh`) against the three new modules, then
reverted, and `git diff` was compared before and after to confirm the revert. The clean run passes 20/20.
None is committed.

**B1: α left out of `build`'s filter.** It fails 2:
- `test_3_another_alpha_finds_neither`;
- `test_3_each_lookup_finds_its_own_row` (α = 0 finds the α = 0.1 row).

```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -485,7 +485,6 @@
                 table.c.atol_serial == atol.store_id,
                 table.c.rtol_serial == rtol.store_id,
                 table.c.tuning_tol_serial == tuning_tol.store_id,
-                table.c.alpha_serial == alpha.store_id,
             )
         )
 
```

**B2: the ratio taken one sample late.** It fails 1: `test_1_every_sample_ratio_bitwise` (the payload
against the ledger; D7).

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -4196,7 +4196,7 @@
         # r_chi at each sample, a tuple parallel to "sample", and the dark radiation at 1 MeV, 70 keV
         # and today, a DarkRadiationValues (inelastic-reflection prompt 09; P27, R22, P31). Both are
         # stored by ScalarHistory.store, as are the records above
-        "rho_chi_over_rho_rad": tuple(rho_chi_ratios),
+        "rho_chi_over_rho_rad": tuple(rho_chi_ratios[1:] + rho_chi_ratios[-1:]),
         "dark_radiation_values": dark_values,
     }
 
```

**B3: `AdiabaticHistory` without `r_chi`.** It fails 1: `test_8b_alpha_reads_rho_chi`.

```diff
--- a/ComputeTargets/AdiabaticHistory.py
+++ b/ComputeTargets/AdiabaticHistory.py
@@ -308,7 +308,7 @@
 
             # the sample's stored rho_chi/rho_R (inelastic-reflection prompt 09, P29); 0.0 at
             # alpha = 0, and on a parked sample as on any other (R15)
-            r_chi: float = value.rho_chi_over_rho_rad
+            r_chi: float = 0.0
 
             M2eff_over_H2: float = policy.M2eff_over_H2(
                 phi_Einstein,
```

**B4: the conformal mass with G − V/3H²M_P² for E.** It fails 1: `test_8b_alpha_reads_rho_chi`.

```diff
--- a/ComputeTargets/AdiabaticHistory.py
+++ b/ComputeTargets/AdiabaticHistory.py
@@ -210,13 +210,7 @@
         # E = rho_R,E (1 + f_m)/3 H^2 M_P^2, formed as ODEPolicy.__call__ forms the kick's: with dark
         # radiation G - V/3H^2 M_P^2 holds rho_chi/3H^2 M_P^2 as well, which is taken out
         # (inelastic-reflection prompt 09; orchestrator note 09, ruling 9)
-        E: float
-        if r_chi == 0.0:
-            E = G - V_over_3H2Mp2
-        else:
-            E, _ = self.V_policy.matter_and_dark_radiation_fractions(
-                G, V_over_3H2Mp2, fm, r_chi
-            )
+        E: float = G - V_over_3H2Mp2
         if E < 0.0:
             # unclear whether we should treat this as a genuine computational error, or whether it just means that rho_r is very small
             # (e.g. at the end of the integration) and should harmlessly be treated as zero
```

**B5: ΔN_eff at 1 MeV with P7's (11/4)^{4/3}.** It fails 1: `test_6_delta_N_eff_at_1_MeV_and_today`.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3151,7 +3151,7 @@
         rho_chi_ratio_70keV=ratios[1],
         rho_chi_ratio_today=r_today,
         Delta_N_eff_1MeV=(
-            NEFF_PER_G_RHO * g_rho_1MeV * r_1MeV if r_1MeV is not None else None
+            NEFF_PER_G_RHO * g_rho_1MeV * NEFF_NEUTRINO_HEATING * r_1MeV if r_1MeV is not None else None
         ),
         Delta_N_eff_today=NEFF_PER_G_RHO * g_today * NEFF_NEUTRINO_HEATING * r_today,
     )
```

**B6: `parking_N` and `release_N` swapped.** It fails 2: `test_2_parked_not_released` and
`test_2_parked_and_released`.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3246,12 +3246,12 @@
                 injected = injected + parking.delta_rho_over_rho_rad
             values["injected_fraction"] = injected
         if parking is not None:
-            values["parking_N"] = parking.N
+            values["release_N"] = parking.N
             values["parking_log_T_Jordan"] = parking.log_T_Jordan
 
     release: Optional[Release] = data.get("release")
     if release is not None:
-        values["release_N"] = release.N
+        values["parking_N"] = release.N
         values["release_log_T_Jordan"] = release.log_T_Jordan
 
     dark: Optional[DarkRadiationValues] = data.get("dark_radiation_values")
```

**B7: an α = 0 row storing NULL for the ratios (R24 reversed).** It fails `test_4_zeros_and_nulls`, in 5
subtests (the three ratios and the two ΔN_eff) plus the test itself.

```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3255,7 +3255,7 @@
         values["release_log_T_Jordan"] = release.log_T_Jordan
 
     dark: Optional[DarkRadiationValues] = data.get("dark_radiation_values")
-    if dark is not None:
+    if dark is not None and dark.rho_chi_ratio_today != 0.0:
         values.update(dark._asdict())
 
     margins = data.get("margins")
```

**B8: `extra_data` truncated to 256 characters on store.** It gives 1 failure and 9 errors:
- the failure is `test_7_long_extra_data_round_trips_whole`;
- the errors are the four test-1 tests, both test-2 tests, `test_3_each_lookup_finds_its_own_row`
  (α = 0.1), `test_8b` and `test_8c`. Each raises `json.loads`' `JSONDecodeError` reading the truncated
  blob back.

```diff
--- a/ObjectFactories/ScalarHistory.py
+++ b/ObjectFactories/ScalarHistory.py
@@ -789,7 +789,7 @@
             "min_RHS_time": obj.metadata.min_RHS_time if not obj._failure else None,
             "validated": False,
             "extra_data": (
-                json.dumps(obj._extra_data) if obj._extra_data is not None else None
+                json.dumps(obj._extra_data)[:256] if obj._extra_data is not None else None
             ),
         }
 
```

## 7. Observations not acted on

- **ΔN_eff(1 MeV) does not carry BBN's frame factor F** (ruling 11). It counts ρ_χ against the bath in the
  frame-independent r_χ, while BBN, through H_J, weighs it by F = A²(1 + (ln A)′π)²/G.
  - F at 1 MeV is 0.93–1.02 at small M, but 0.11 down to 4.4×10⁻⁵ at M = 0.5 M_P for β = 1.2–25 (§5.2).
  - So at large M the stored ΔN_eff(1 MeV) overstates, by up to 10⁴, what BBN sees of the quanta.
  - R22's convention is the user's, and BBN is not changed (P9). It is recorded here for the user.
- **The bath's ρ_R,J drifts from (π²/30) g_ρ(T_J) T_J⁴** by about 10⁻³ over a history: −0.210 % at 1 MeV
  and −0.078 % today, on M = 0.5, β = 2.
  - The ODE evolves it through Σ = 1 − 3w and sets it from g_ρ T⁴ only at T*. The temperature law holds to
    2×10⁻¹⁰.
  - Every ΔN_eff (R22, which uses the cosmology's g_ρ with the stored r_χ) inherits the drift at about
    10⁻³. It is also the whole of ruling 12's residual (§5.2).
  - It concerns the EOS tables (w against g_ρ, g_s) and every history alike. It is related to, but not the
    same as, `[00-xav-sigma-is-non-monotone-at-25-35-keV]`. No issue is opened: the impact is 10⁻³ on a
    presentation quantity (R12). It is for the orchestrator to decide whether it merits one.
- **`DarkRadiationValues`' ΔN_eff fields are `numpy.float64`**, from `G_rho`, where the ratios are
  Python floats. They store and compare bitwise; nothing depends on the type.
- At α = 0 the golden β = 1.5 history (T_stop = 1 MeV) gives `rho_chi_ratio_1MeV = None`: it ends before
  the first crossing is found on its last step. It is stored as NULL, as `fixed_T_values`' pair is.
  Every stored history ends at T_CMB.
- **The inventory report** (D3) shows `alpha_value` under "Other classes" and `alpha=` on each history's
  line, not in the common line. That presentation is 10's ("the inventory's α").

## 8. State handed to the next prompt

For 10, the driver and the readers:
- **The α row.** The table is `alpha_value`, a `CosmologyConcepts.alpha_value(store_id, value)`, built by
  the dimensionless factory: relative match, and absolute (`DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION`) at
  0. It is replicated, after `beta_value`. A reader fetches it with
  `pool.object_get("alpha_value", value=α)`; the read-only pool raises `ReadOnlyMiss("alpha_value")` if it
  is absent. It is not in `read_table_config`, so `read_table("alpha_value")` is not available yet.
- **Every lookup and where it passes α** (α = 0 today):
  - **`main.py`**:
    - `alpha` is fetched at the tolerances' `ray.get` (`atol, rtol, tuning_tol, alpha = ray.get([...])`)
      and passed to `run_pipeline(…, tuning_tol, alpha, solvers, …)`;
    - within it, the six history payloads, `"alpha": alpha` in five vectorized query payloads and
      `alpha=alpha` in the work `object_get`.
  - **`plot_inputs.parameters`** returns `PlotParameters(..., tuning_tol, alpha)`. It looks up the constant
    α = 0, and its miss is `PlotOptionNotStored("alpha_value", "the constant α", "α = 0")`.
  - **`plot_by_beta.py` and `plot_ScalarHistory.py`** unpack `alpha` and pass it to `run_pipeline`, into
    their history payloads (`"alpha": alpha`).
  - **`tools/history_and_bbn.py`** does no lookup. It takes no `--alpha` yet, and passes the payload's ratio
    to `ScalarHistoryValue`.
  - **`tools/bbn_from_store.py`'s `_FIND_QUERY`** joins `ScalarHistory` to the model by raw SQL with no α.
    That is right while a store holds α = 0 alone, and wrong once two α are stored: it must gain
    `join alpha_value a on a.serial = sm.alpha_serial` and a match on α.
  - **`tools/inventory_report.py`** prints `alpha=` per history line, and `alpha_value` under "Other
    classes" (§7).
  - **`RunDrivers/registered_run.py`**'s scope does not name α.
- **The columns and their names**: §2.1's table. `ScalarHistory.alpha_serial`; the 24 `DissipationValues`
  fields as columns, with `parking_log_T_Jordan` and `release_log_T_Jordan` in GeV;
  `ScalarHistoryValue.rho_chi_over_rho_rad`; `extra_data` (`Text`) with `"margins"`.
- **The object's accessors**:
  - `ScalarHistory.alpha` (an `alpha_value`);
  - `ScalarHistory.dissipation` (`DissipationValues`, ln T_J in the cosmology's units, `None` on a failure,
    raises when unpopulated, readable with `_do_not_populate`);
  - `ScalarHistory.extra_metadata["margins"]`;
  - `ScalarHistoryValue.rho_chi_over_rho_rad`.

  The payload carries `"rho_chi_over_rho_rad"`, a tuple, and `"dark_radiation_values"`, a
  `DarkRadiationValues`. `compute_scalar_history(..., alpha=0.0)` and `ScalarHistory(..., alpha=...)` take
  it. Δg* (P26) is `gstar_rho * rho_chi_over_rho_rad` per value; `ModelFunctions` has no spline of it.
- **What 10 must add**:
  - `--alpha-values` in `main.py` and the wrapper's scope;
  - the plot scripts' matching, through `plot_inputs` with an option in place of the constant;
  - Δg*(T_J) and ΔN_eff(β) per α (R12);
  - the inventory's α (grouping and the common line);
  - `history_and_bbn.py`'s `--alpha`;
  - the α in `bbn_from_store.py`'s raw SQL.
- **Constants**: `NEFF_PER_G_RHO = 4/7`, `NEFF_NEUTRINO_HEATING = (11/4)^{4/3}`, neither a key.
- **Version**: `FINGERPRINT_FORMAT = 6` (16 pairs, 13 digests), golden
  `store_fingerprint_inelastic-reflection-09.json` (digest `63aef444…733c`), `VERSION_LABEL = "2026.8.0"`.
- **Tests**:
  - `ObjectFactories/tests/test_dissipation_store.py` (15);
  - `ObjectFactories/tests/test_adiabatic_dark_radiation.py` (3);
  - `ComputeTargets/tests/test_bbn_sees_dark_radiation.py` (2);
  - the fixture `ComputeTargets/tests/dissipation_histories.py`.
- **Counts after this prompt:** 26 / **473** (**98** + 375) / **121** / 115 / 64. `CLAUDE.md`'s counts are still those of
  `tuning-followups` 02; this campaign's close (12) updates them.
- **Issues**:
  - closed `[00-settling-at-physical-M-needs-a-parked-tracking-model]` (`integrator-remediation` §4; this
    board §3.1 and §4; index §1.20 emptied, count 25 → 24);
  - opened none.
