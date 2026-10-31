# Prompt 09 — the store

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-10-31, after 08 landed (`86fc526`) and was reviewed (`0f0cfe4`), at the user's word
("OK, can we write prompt 09?"). It is written from:
- README §8.4's charter for 09, and the rulings it rests on: U3, P7, P8, P9 (§6), R12, R13, R15, R21,
  P22, P26 (§8);
- the user's four rulings of the same day, R22–R25 (README §8.1), and the planner's P27–P31 (§8.2);
- log 08 §8 (the record and its fields), logs 06 and 07 §8 (the parking and release records);
- the probe [`source/planning-probes-2026-10-31/`](source/planning-probes-2026-10-31/README.md)
  (`neff.py`, `neff.out`). It is a reference, not code to copy.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- no ChamPBH Ray process is running. You start none, and no test of yours may start one.

**Closes:** `[00-settling-at-physical-M-needs-a-parked-tracking-model]` (`integrator-remediation` board §3;
this board §3.1; `.documents/OPEN_ISSUES.md` §1.20). Logs 06 and 07 built the switch, the tracker and
the release. What remained is Q on the parked samples, which this prompt computes (R15).

**Opens:** only what the work finds.

**Recommended model:** **Opus**. This is the campaign's schema change: a new key, a new table, about
twenty columns, every writer and reader moved at once, and the fingerprint. It also corrects the
adiabatic mass, which is physics.

**Read first:**

1. [`README.md`](README.md):
   - §6.1 U3; §6.2 P7, P8, P9, P11, P12;
   - §8.1 R12, R13, R15, R21, R22–R25; §8.2 P22, P26, P27–P31;
   - §8.4's charters for 09 and 10 (10 does the options, the matching and the figures);
   - §5 (the rules).
2. `CLAUDE.md`:
   - "The packages", and DatastoreKit's client contract (`docs/client-contract.md` at the pinned tag);
   - the test rules (`_TempStoreCase` in `ObjectFactories/tests/test_version_keyed_lookups.py`;
     `@ray.remote` functions called through `._function`);
   - "Whole-history values on the parent row" is a standing rule: per-history scalars are columns of
     `ScalarHistory`, read even with `_do_not_populate`.
3. Logs:
   - [`logs/08-spectrum-peak-and-margins.md`](logs/08-spectrum-peak-and-margins.md) §8: the payload
     keys, `Margins`, `MarginFailure`, `Generation`, and what 09 stores;
   - [`logs/06-parking.md`](logs/06-parking.md) §8 and [`logs/07-unparking.md`](logs/07-unparking.md)
     §8: `Parking`, `Release`, and how a reader tells the phases apart.
4. `prompts/jordan-normalization/07-the-store.md` and its log. They are the precedent for a key, a
   table, the readers moved as far as the schema forces, and a fingerprint format.
5. `ComputeTargets/ScalarHistory.py`:
   - `SampleValues` (:526), and `ODEPolicyData.rho_chi_over_rho_rad` (:489);
   - `fixed_T_values` (:3051) and its crossing helpers;
   - `integrate_history` (:3664): the sampling loop (:3857) and the payload (to :3990);
   - `Margins` (:3195), `MarginFailure` (:3165), `history_margins` (:3492);
   - `tune_history` (:4254), `compute_scalar_history` (:4493), `build_extra_data` (:4546);
   - `ScalarHistory` (:4581), its `compute` and `store` (:4973), and `ScalarHistoryValue` (:5049).
6. `ComputeTargets/AdiabaticHistory.py`: `AdiabaticComputePolicy.M2eff_over_H2` (:145),
   `Hdot_over_H2` (:225), `compute_adiabatic_values` (:244).
   `ComputeTargets/Policies/PotentialDerivativePolicy.py`: the `r_chi` argument of each method, and
   `matter_and_dark_radiation_fractions`.
7. `ObjectFactories/ScalarHistory.py`: `register` (:147; `extra_data` at :277), `inventory_spec` (:282),
   `build` (:317), `store` (:655). `ObjectFactories/DimensionlessQuantity.py`.
8. `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, `config/version.py`.
9. Every lookup of a history:
   - `main.py`'s six (`grep -n '"ScalarHistory"' main.py`), and the tolerance rows at :989;
   - `plot_inputs.py` (`parameters`), `plot_by_beta.py`, `plot_ScalarHistory.py`;
   - `tools/` and `RunDrivers/`, wherever they build a history's key.
10. The tests that build or look up a history:
    - `ObjectFactories/tests/`: `test_tuned_history_round_trip`, `test_first_bounce_round_trip`,
      `test_fixed_T_values_round_trip`, `test_scalar_history_failure_lookup`,
      `test_scalarhistory_failure_reason`, `test_version_keyed_lookups`,
      `test_store_fingerprint_golden`, `test_fingerprint_format_guard`;
    - `ComputeTargets/tests/test_plot_inputs.py` and `test_driver_options.py`;
    - `tools/tests/test_compare_stores.py` and `test_inventory_report.py`.

---

## 0. Where this prompt sits

### 0.1 What it is for

01–08 compute the dissipation mode and leave everything they compute in the payload, unstored. This
prompt stores it:
- α as a key of every history (U3);
- the ratio ρ_χ/ρ_R at every sample;
- the whole-history values of the charter, as columns;
- the margins' first failures, as JSON.

It also corrects `AdiabaticHistory`, which ignores ρ_χ (P29). It then computes Q on the parked samples,
which closes the issue this campaign was assigned.

What 10 does is not done here: the drivers' `--alpha-values`, the plot scripts' matching and figures,
the wrapper's scope, the inventory's α, and the tool's `--alpha`. Here every reader passes α = 0 (P30).

### 0.2 What the probe found

`neff.out`, α = 0.1, untuned:
- ΔN_eff at 1 MeV (T_ν = T_γ) and today (P7's) agree to 4–5 % on all three histories. The difference
  is what is injected after 1 MeV: every history drains after T_J = 1 MeV.
  - β = 2, M = 10⁻³⁰ M_P: 0.0497 against 0.0524.
  - β = 2, M = 0.5: 0.0491 against 0.0515.
  - β = 5, M = 0.5: 0.00577 against 0.00595.
- P7's expression at 1 MeV would be 3.85 times larger. R22 rules it out.

### 0.3 What the orchestrator found in the code

- **`AdiabaticHistory` treats ρ_χ as absent.** `AdiabaticComputePolicy` calls `Vprimeprime_over_3H2Mp2`,
  `V_over_3H2Mp2` and `Hdot_over_H2_plus_3` without `r_chi`.
  - At α > 0 its M²_eff/H² and Ḣ/H² are those of a universe without the quanta, and its E holds ρ_χ.
  - At α = 0 it is right, which is why no test has seen it.
- **The golden histories hash `SampleValues`.** `golden_history.py` and `golden_tuned_history.py` record a
  SHA-256 of the samples and the list of `SampleValues._fields`. A new field there breaks both at α = 0.
  So the ratio travels as a key of its own (P27).
- **`extra_data` is `String(256)`.** It holds about 145 characters today. The three first failures need
  about 550 more. SQLite does not enforce the length, so nothing has failed; R23 makes the declaration
  true.
- **`compute_scalar_history` takes no α.** `tune_history` does (02), but the stored path calls it without
  one. So no stored history can be in dissipation mode until this prompt.
- **`build_extra_data`'s tests use stand-in payloads without the new keys.** These are
  `test_kinematic_cap_loop`'s `test_g_*` and `test_reflection_reporting`. They pass unedited if the new
  key is read with `.get` and added only where `margins` is not `None`.

---

## 1. What is wanted

### 1.1 The key (U3, P28)

- **The table.** `alpha_value`, a dimensionless quantity, as `beta_value` is.
  - It has a class in `CosmologyConcepts` beside `beta_value`.
  - It is built by `sqla_dimensionless_quantity_factory` (its relative match, and its absolute match at
    zero).
  - In `config/datastore.py` it is registered before `ScalarHistory`, and it gets a serial batch size.
  - In `config/sharding.py` it is replicated, as `beta_value` and `tolerance` are.
  - It joins the `--drop` groups only if `tables_to_drop` requires it. Say which.
- **`ScalarHistory`'s key** gains `alpha_serial` (a foreign key to `alpha_value`, not null, indexed).
  - `build` filters on it beside `tuning_tol_serial`.
  - `inventory_spec` gains the parent `"alpha": Parent("alpha_serial", "alpha_value")`.
  - α = 0 is a row like any other.
- **The object.** `ScalarHistory` takes `alpha` (an `alpha_value`), as it takes `tuning_tol`.
  - It exposes `alpha`.
  - `compute` passes `alpha=self._alpha.value` to `compute_scalar_history`, which gains `alpha` and
    passes it to `tune_history`.
  - `compute_scalar_history`'s default is 0.0, so that every existing caller is the old code.

### 1.2 The per-sample ratio (P27)

- **`integrate_history`'s payload** gains `"rho_chi_over_rho_rad"`: a tuple, parallel to `"sample"`, of
  `data.rho_chi_over_rho_rad` from the policy call the sampling loop already makes at each sample.
  - It is the policy's own float, and exactly 0.0 at α = 0.
  - `SampleValues` does not change.
  - A failure payload does not carry the key.
- **`ScalarHistoryValue`** gains `rho_chi_over_rho_rad`, and `ScalarHistory.store` fills it from the key.
- **The value table** gains the column `rho_chi_over_rho_rad` (Float(64), not null). The factory writes it
  and reads it back.

### 1.3 The whole-history columns (P7, R12, R13, R21–R25, P31)

All are nullable `Float(64)` or `Integer` columns of `ScalarHistory`, and all are NULL on a failure row.
Name them as below unless a name collides; if one does, say so.

| Column | Value at α > 0 | At α = 0 (R24) |
|---|---|---|
| `injections` | candidates with `delta_K > 0` | 0 |
| `resolved_candidates`, `unresolved_candidates` | candidates by `resolved` | 0, 0 |
| `injected_fraction` | Σ `delta_rho_over_rho_rad` over the candidates, plus the leftover's (`Parking.delta_rho_over_rho_rad`) | 0.0 |
| `rho_chi_ratio_1MeV`, `rho_chi_ratio_70keV` | r_χ at the first crossing of T_J = 1 MeV and 70 keV (P31) | 0.0 |
| `rho_chi_ratio_today` | r_χ at N_final | 0.0 |
| `Delta_N_eff_1MeV` | (4/7) g_ρ(1 MeV) `rho_chi_ratio_1MeV` (R22) | 0.0 |
| `Delta_N_eff_today` | (4/7) g_ρ(T_CMB) (11/4)^{4/3} `rho_chi_ratio_today` (P7, R22) | 0.0 |
| `log_R_first_impact` | ln 𝓡 of `injections[0]`'s assessment, NULL if it has none | NULL |
| `parking_N`, `parking_log_T_Jordan` | `Parking.N` and its ln T_J, NULL if not parked | NULL |
| `release_N`, `release_log_T_Jordan` | `Release.N` and its ln T_J, NULL if not released | NULL |
| `worst_birth_margin`, `worst_birth_margin_N`, `birth_failing_share` | from `Margins` (R25) | NULL |
| the same for `reflection` and `tracker` | from `Margins` | NULL |
| `unassessed_share` | `Margins.unassessed_share` (R21, R25) | NULL |

- **Where the values come from.** They are computed in `ComputeTargets/ScalarHistory.py` from the
  payload, and stored as the table says.
  - The ratios and ΔN_eff are formed in `integrate_history`, in one record of their own (a payload key,
    as `fixed_T_values` is). r_χ comes from the policy at the dense output's state, and g_ρ is the
    cosmology's `G_rho` at that T_J (P31). Today's g_ρ is at the history's own T_J at N_final.
  - The rest are read by `ScalarHistory.store` from `injections`, `parking`, `release` and `margins`.
- **Units.** The ln T_J columns are stored with T_J in GeV, as `first_bounce_log_T_Jordan` is. N is
  dimensionless. Every ratio, share and margin is dimensionless.
- **Where the margins failed** (08's `None`, `None`, one warning line), the margin columns and
  `unassessed_share` are NULL, and the others are stored.
- **`_do_not_populate`.** Every column of this table is read back with it, as `tuning` is. The object
  exposes the values as one record, and the record is `None` on a failure.

### 1.4 The JSON (R13, R23, R25)

- **`extra_data`** becomes `sqla.Text` (nullable, as now).
- **`build_extra_data`** adds the key `"margins"` only where the payload's `margins` is not `None`, and
  reads it with `.get`. The key holds:
  - `first_birth_failure`, `first_reflection_failure`, `first_tracker_failure`: each `MarginFailure._asdict()`,
    or `null`;
  - `worst_birth_margin_index`, `worst_reflection_margin_index`, `worst_tracker_margin_index`;
  - `classical_generations`, `one_loop_generations`, `classical_gap_generations`, `weighted_generations`,
    `unassessed_candidates`;
  - `weighted_share`, `below_weight_share`.

  Nothing else of `Generation` is stored (log 08 §8).
- Every float in it round-trips through `json` exactly. Say so with a test.

### 1.5 `AdiabaticHistory` (P29, R15)

- **It reads each sample's `rho_chi_over_rho_rad`** and passes it as `r_chi` to every policy call:
  `V_over_3H2Mp2`, `Vprimeprime_over_3H2Mp2` and `Hdot_over_H2_plus_3`.
- **The conformal mass.** `conformal_mass_over_H2` takes E = ρ_R,E(1 + f_m)/3H²M_P², the matter and bath
  part. That is `matter_and_dark_radiation_fractions`'s E, not G − V/3H²M_P², which also holds ρ_χ.
  - **Derive this in the log before the code changes.** The quanta are collisionless and do not couple
    to φ (P1), so δρ_χ/δφ = 0, and ρ_χ enters M²_eff only through H (the gravitational term) and the
    normalization by H².
  - Say whether any other term of M²_eff/H² holds ρ_χ.
- **At r_χ = 0.0** every call takes its old operations (the policy's own branch, 01). So Q at α = 0 is
  bitwise the dispatch commit's.
- **The parked samples** need nothing of their own (R15). Their φ is φ_min and their π is dφ_min/dN, and Q
  is computed on them as on any sample. 09 measures it there (§2.3), and that closes the issue.

### 1.6 The version (P8)

- **`FINGERPRINT_FORMAT` 6.** The format moves because the registry gains `alpha_value` and
  `ScalarHistory`'s spec gains a parent. Add its history paragraph and its pin in `SPEC_DIGESTS`,
  generated by the guard's function, never typed.
- **A new golden file**, `ObjectFactories/tests/data/store_fingerprint_inelastic-reflection-09.json`,
  beside the old ones. `GOLDEN` names it. The golden store gains an `alpha_value` row, and every new
  column is set to a literal, as format 5 set the tuning's.
- **`VERSION_LABEL = "2026.8.0"`**, with its history line in `config/version.py`. No store made under
  2026.7.0 is reused (P8); there is no such science store to migrate (README §3).

### 1.7 The readers, as far as the schema forces (P30)

- **Every lookup of a history passes the α = 0 row**: `main.py`'s six, `plot_inputs.parameters` with
  the plot scripts, and any tool that builds a history's key. The drivers fetch it once, beside the
  tolerances.
  - Nothing gains an option.
  - Nothing matches α against a build.
  - No figure, CSV column or inventory line changes, unless a reader cannot run without it. If one
    cannot, add the least and log it.
- **`plot_inputs.parameters`** returns the α row beside the tuning tolerance. Its callers pass it on.
- **The read-only plots** must still refuse to create a row. An α = 0 row missing from an old store is
  that path's existing "not stored" stop, not a new one.

### 1.8 The documents (P12)

- **`.documents/architecture-summary.md`:** a dated note beside 08's covering:
  - the key and the table;
  - the per-sample ratio;
  - the columns and the JSON;
  - `AdiabaticHistory`'s change;
  - format 6 and 2026.8.0.
- **`.documents/numerical-strategies.md`:** a dated subsection after 08's covering:
  - ΔN_eff's two conventions (R22) and the crossing (P31);
  - the conformal mass with ρ_χ (§1.5), and its derivation in short.

### 1.9 The tests

No store but `_TempStoreCase`'s, no Ray. Each test that integrates gives its cost and the load in its
docstring.

New module or modules, named for what they test:
1. **A dissipation history round-trips**, at M = 0.5 M_P, β = 2, α = 0.1, `integrate_history`, untuned:
   - every column of §1.3 bitwise, raw (the row) and on the object;
   - every sample's `rho_chi_over_rho_rad` bitwise against the payload's key;
   - `extra_data`'s `"margins"` equal to the payload's `Margins` and first failures;
   - the same read with `_do_not_populate`.
2. **A parked and released pair.**
   - M = 10⁻³⁰, β = 2 (parked): the parking columns set, the release columns NULL.
   - M = 0.5, β = 5 (released): both set, equal to the payload's records.
3. **Two α are two rows.** One model, one set of initial data and tolerances, at α = 0 and 0.1:
   - each lookup finds its own row and not the other's;
   - a lookup at α = 0.3 finds neither.
4. **An α = 0 row (R24)** holds 0 and 0.0 where §1.3 says, and NULL elsewhere. Its samples' ratios are
   0.0.
5. **A failure row** holds every new column NULL and reads back as a failure.
6. **ΔN_eff against a hand computation.** The two expressions of R22, on the values of test 1, from the
   cosmology's `G_rho`. Equality where the operations are the same.
7. **`extra_data` longer than 256 characters** round-trips whole on the temporary store, and the column is
   `Text` in the reflected schema.
8. **`AdiabaticHistory`:**
   - at α = 0, `compute_adiabatic_values._function` on a stored golden-setup history is bitwise the
     dispatch commit's (pin the values in the test from the dispatch commit, with how they were
     taken);
   - at α = 0.1, each sample's M²_eff/H² and Ḣ/H² equal a recomputation with `r_chi` and §1.5's E;
   - on the parked history of test 2, every Q is finite, parked samples included.
9. **BBN sees ρ_χ (P9).** In a limit where ρ_χ dominates the new physics (a constructed sample, or a
   history at large α with V negligible), BBN's density ratio ρ_NP/ρ_R,J holds ρ_χ/ρ_R,J to a tolerance
   derived from what it neglects.
10. **The version.** The golden fingerprint, the format guard with format 6's pin, and `VERSION_LABEL`.

**Existing tests that change.** Existing tests may change only where the key changes what they test (README §5):
- one that builds or looks up a history gains the α = 0 row;
- the fingerprint and version tests move to format 6 and 2026.8.0.

List each module, its edit and its reason in the log. Any other edit to an existing test is a stop.
These pass unedited:
- both golden histories, the tuned one included;
- `test_kinematic_cap_loop`, `test_reflection_reporting`, `test_injection`, `test_parking`,
  `test_release`, `test_margins`, `test_wall_model`, `test_wall_rounding`, `test_adiabatic_mass`.

**Each new test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: α left out of `build`'s filter.
- B2: the ratio taken one sample late (`[1:] + [last]`).
- B3: `AdiabaticHistory` without `r_chi`.
- B4: the conformal mass with G − V/3H²M_P² for E.
- B5: ΔN_eff at 1 MeV with P7's (11/4)^{4/3}.
- B6: `parking_N` and `release_N` swapped.
- B7: an α = 0 row storing NULL for the ratios (R24 reversed).
- B8: `extra_data` truncated to 256 characters on store.

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned), α = 0.1, unless
stated. No Ray. Stores are temporary only. The dispatch commit's values are taken in a temporary
worktree of it, which is removed afterwards.

1. **The round trip at scale.** Store and read back, on one temporary store:
   - M = 0.5 at β ∈ {1.2, 2, 5, 25};
   - M = 10⁻³ at β = 1.2;
   - β = 2 at M ∈ {10⁻¹², 10⁻¹⁶, 10⁻³⁰}.

   Each row is bitwise on every column and every sample. Give the store and read times, and the row's
   `extra_data` length.
2. **The stored values.** For the histories of item 1, give:
   - every §1.3 column;
   - the share of K injected after T_J = 1 MeV;
   - ΔN_eff(today)/ΔN_eff(1 MeV).

   Say whether the ratio is explained by the share injected after 1 MeV and the bath's g_ρ, g_s
   between the two.
3. **Q on the parked samples.** On β = 2 at M = 10⁻³⁰ and 10⁻¹⁶, M = 10⁻³ at β = 1.2 (released) and
   M = 0.5 at β = 5 (released), give for each `Q_labels` k/H:
   - max |Q| over the parked samples and over the others;
   - the sample and N of each maximum;
   - any non-finite value (a stop, §5).
4. **What ρ_χ changes in `AdiabaticHistory`.** On item 3's histories at α = 0.1, compare the final tree
   with the dispatch commit. Give the largest relative change of M²_eff/H², of Ḣ/H² and of max |Q| per
   label, and where it lies.
5. **What does not change**, against the dispatch commit, bitwise:
   - at α = 0, `integrate_history`'s payload on every existing key, both golden setups and natural mode
     at M = 0.5, β = 2. The new keys are reported apart;
   - `compute_adiabatic_values` at α = 0 on the golden setups;
   - the tuned golden history.
6. **The cost.** Per history: the store, the read with and without `_do_not_populate`, and
   `compute_adiabatic_values`. Each against the dispatch commit, the minimum of three rounds, with the
   load.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 471 (96 + 375) / 103 / 115 / 64 (log 08 §8). A different count, or a failure, is a stop.
2. **After:**
   - the five suites. Each count's change is explained by the new tests. The tests of §1.9 that pass
     unedited do so; say so;
   - `black --check` on the files you changed;
   - `main.py --help`, `plot_by_beta.py --help`, `plot_ScalarHistory.py --help` and
     `RunDrivers/registered_run.py --help` exit 0;
   - `main.py --inventory` on a temporary store built by a test shows `alpha_value` and the parent
     `alpha` (no Ray).
3. **The breakages** of §1.9, each with the tests it fails.

## 4. Acceptance

1. **§1 as written, and the gate "Store"** (README §8.4, restated):
   - the key and the columns round-trip, bitwise;
   - two α are distinct rows;
   - an α = 0 row holds R24's zeros and NULLs;
   - the new golden fingerprint, format 6, and 2026.8.0;
   - `AdiabaticHistory` bitwise at α = 0 and computed on parked samples;
   - both golden histories bitwise.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/09-store.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - §1.5's derivation;
   - the schema as built: each new column, its type and its source;
   - the suite counts before and after;
   - the existing tests edited, each with its reason;
   - the breakages as diffs;
   - every deviation, classified;
   - "State handed to the next prompt", for 10: the α row and how a reader fetches it; every lookup and
     where it passes α; the columns and their names; the object's accessors; what 10 must add.
4. **The documents** of §1.8.
5. **The board:**
   - row 09 and the gate Store, in §1 and §2;
   - `### 5.11 After prompt 09 (<date>)` in §5;
   - §3.1's `[00-settling-at-physical-M-needs-a-parked-tracking-model]` closed, with a "Closed" line
     naming this commit. The entry on the `integrator-remediation` board §3 is closed there too (moved
     to its §4, with its Closed line);
   - in the same commit, its row deleted from `.documents/OPEN_ISSUES.md` §1.20, with the count and the
     date corrected (`CLAUDE.md`);
   - anything opened, on §3 and in the index.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history changes, the tuned one included.
- An existing test needs an edit other than §1.9's (the α row, format 6, 2026.8.0).
- Any value of §2.5 is not bitwise the dispatch commit's.
- A round trip is not bitwise.
- `AdiabaticHistory` gives a non-finite Q on a parked sample, or fails on one.
- §1.5's derivation finds that ρ_χ enters M²_eff otherwise than through H and E.
- DatastoreKit cannot express the key, the replicated table or the `Text` column as §1 says. That is a
  package question (`CLAUDE.md`, "The packages"), never a workaround here.
- A reader cannot be kept working at α = 0 without more than the α row (P30).
- The work seems to need a change to:
  - the step loop, `IntegrationResult`, the margins (08) or any record of 02–08;
  - `SampleValues`;
  - BBN's computation (P9).

## 6. What this prompt does not do

- **No option, no matching, no figure.** `--alpha-values`, the plot scripts' matching, Δg* and ΔN_eff(β),
  the wrapper's scope, the inventory's α and the tool's `--alpha` are 10's.
- **No change to the integration** or to anything 02–08 compute. The payload gains two keys, the ratio
  and the dark-radiation record; nothing else in it changes.
- **No store but the tests' temporary ones**, and no Ray. The rehearsal on the isolated cluster is 11's
  (U11).
- **No change to BBN** (P9). It reads H_J, which already holds ρ_χ.
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`: the payload's two keys, `compute_scalar_history`'s α,
    `build_extra_data`, `ScalarHistory` and `ScalarHistoryValue`;
  - `ComputeTargets/AdiabaticHistory.py`;
  - `CosmologyConcepts/` (the α class);
  - `ObjectFactories/ScalarHistory.py`;
  - `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, `config/version.py`;
  - `main.py`, `plot_inputs.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, and the tools that build a
    history's key;
  - the new tests, the existing tests of §1.9, and the new golden fingerprint file;
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, this board, the `integrator-remediation` board and `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
