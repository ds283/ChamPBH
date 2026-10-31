# Orchestrator — prompt 09, the store

Read [`../README.md`](../README.md) first: §5 (the rules), §6.1 U3, §6.2 P7–P9, P11, P12, §8.1 (R12, R13,
R15, R21–R25), §8.2 (P22, P26–P31), and 09's charter in §8.4. Then the prompt, log 08 §8, logs 06 and 07
§8, and the probe [`source/planning-probes-2026-10-31/`](../source/planning-probes-2026-10-31/README.md).
This note binds 09 only. The conventions of notes 02–08 bind here as well, and are restated below: a
tolerance is a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`09-store.md`](../09-store.md) · **Gate:** "Store" · **Closes:**
`[00-settling-at-physical-M-needs-a-parked-tracking-model]` · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `e7f8f80` on 2026-10-31 at 15:03 GMT, before this note was committed:
- `inelastic-reflection` is checked out in the main checkout, at `e7f8f80`, the commit that adds prompt 09.
  That commit came after 08 landed (`86fc526`) and was reviewed (`0f0cfe4`).
  - `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main
    checkout alone.
  - No code has changed since 08 landed: `git diff --stat 86fc526 e7f8f80` lists files under
    `prompts/inelastic-reflection/` only.
  - So the prompt's line numbers hold for this tree. Checked: `ScalarHistory.py` :489, :526, :3051,
    :3165, :3195, :3492, :3664, :3857, :3990, :4254, :4493, :4546, :4581, :4973, :5049;
    `AdiabaticHistory.py` :145, :225, :244; `ObjectFactories/ScalarHistory.py` :147, :277, :282, :317,
    :655; `main.py`'s six `"ScalarHistory"` lookups (:237, :292, :427, :546, :678, :802) and the tolerance
    rows at :989.
  - `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- SciPy is 1.17.0.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 471 (96 + 375) / 103 / 115 / 64.
- The load averages were 16 / 12 / 9. Other workloads share the machine.

## 0. What makes this prompt unusual

**The schema change is mechanical. What is not is that more existing tests pin the schema than the
prompt lists, that the old round-trip tests hand `ScalarHistory.store()` a stand-in payload, and that
BBN's density ratio carries ρ_χ with a frame factor that is not near 1.** For this note the
orchestrator read `ObjectFactories/ScalarHistory.py` (register, inventory_spec, build, store), the
dimensionless factory, `config/datastore.py`, `config/sharding.py`, `config/version.py`,
`AdiabaticHistory.py` whole, `PotentialDerivativePolicy.py` whole, `ODEPolicy.__call__`, `HubblePolicy`,
`DarkRadiationLedger`, `fixed_T_values` and its helpers, the sampling loop and payload of
`integrate_history`, `compute_scalar_history`, `build_extra_data`, `ScalarHistory.compute` and `.store`,
the records `Injection`, `Parking` and `Release`, `compute_BBN_data`'s ratio, `plot_inputs.parameters`,
and every module that names `"ScalarHistory"` or builds a `ScalarHistoryValue`. It also ran a probe
(`probe.py`, session scratchpad) at the dispatch tree on four histories at α = 0.1, untuned, on
`test_injection`'s setup: β = 2 at M = 10⁻³⁰ and 10⁻¹⁶ (parked), M = 0.5 at β = 5 and M = 10⁻³ at β = 1.2
(parked and released). Here is what it found where the prompt is silent, loose or wrong.

- **Two more test modules pin the registry, and the prompt does not list them.**
  `ObjectFactories/tests/test_datastore_config.py` pins `EXPECTED_FACTORIES` and
  `EXPECTED_SERIAL_BATCH_SIZES`. `ObjectFactories/tests/test_inventory_specs.py` pins its list of
  classes, and its `test_no_parent_crosses_a_shard` requires a parent to be replicated.
  - A new `alpha_value` table fails the first two pins. So the prompt's own §1.1 cannot pass without an
    edit that §1.9 does not allow, and §5 makes that a stop. Ruling 1.
  - `tools/tests/test_compare_stores.py` (:950) and `test_inventory_report.py` (:162) insert
    `ScalarHistory` rows by hand. With `alpha_serial` NOT NULL they need an α row. That is §1.9's "builds
    a history" and is allowed.
- **The old round-trip tests hand `store()` a stand-in payload.** `test_first_bounce_round_trip`'s
  `_success_payload` (:117), which `test_fixed_T_values_round_trip` imports, holds only the keys
  `ScalarHistory.store()` reads today: no `"injections"`, `"parking"`, `"release"`, `"margins"`,
  `"rho_chi_over_rho_rad"`, or the new record. A `store()` that reads them with `data[...]` fails both
  modules, and adding the keys to the stand-in is an edit §1.9 does not allow. The prompt saw this for
  `build_extra_data` (§0.3, fifth point) but not for `store()`. Ruling 5.
- **`ScalarHistoryValue` is built outside the store.** `tools/history_and_bbn.py:446` builds one per
  sample from `SampleValues._asdict()`. A required `rho_chi_over_rho_rad` breaks the tool; a default of
  0.0 would let it pass a dissipation history to BBN's stand-in with the ratio silently zero. Ruling 4.
- **`Parking` and `Release` carry `log_T_Jordan`** (`ScalarHistory.py` :368, :413), so `store()` can fill
  the four parking and release columns from the payload alone. The leftover of P19 is *not* an
  `Injection`: it is `Parking.delta_K`, its own ledger entry. So `injected_fraction` counts it once.
- **BBN's ratio holds ρ_χ with a frame factor, not 1 (P9, prompt test 9).** BBN's ratio is
  (3M_P²H_J² − ρ_R,J(1 + f_m))/ρ_R,J (`ComputeTargets/BBNData.py` :462–:469). With
  H_J = H_E(1 + (ln A)′π)/A (`HubblePolicy`), 3M_P²G H_E² = V + ρ_R,E(1 + f_m) + ρ_χ,E, and
  ρ_R,J = A⁻⁴ρ_R,E, the part of the ratio that is linear in ρ_χ is exactly **F·r_χ, with
  F = A²(1 + (ln A)′π)²/G**.
  - F multiplies the bath's term in H_J in the same way, so in H_J ρ_χ stands to the bath as r_χ.
  - The probe measured F on the samples in 1 keV – 10 MeV: 0.88–1.13 at β = 2 (both M), 0.71–1.14 at
    M = 10⁻³, β = 1.2, and **0.0098–0.0107 at M = 0.5, β = 5** (untuned).
  - So prompt test 9's limit ("holds ρ_χ/ρ_R,J to a tolerance derived from what it neglects") holds only
    where F ≈ 1, and fails by two orders of magnitude at β = 5. The exact identity replaces it. Ruling 11.
- **ΔN_eff(T_CMB)/ΔN_eff(1 MeV) has a closed form.** ρ_χ,J a_J⁴ is constant (P1), T_J a_J g_s^{1/3} is
  constant (the ODE's temperature law), and ρ_R,J ∝ g_ρ T_J⁴. So r_χ ∝ K g_s^{4/3}/g_ρ, the g_ρ of
  (4/7)g_ρ cancels, and

  ΔN_eff(T_CMB)/ΔN_eff(1 MeV) = [K(N_final)/K(N_1MeV)] · [(11/4) g_s(T_CMB)/g_s(T_1MeV)]^{4/3},

  to the integration's error, with no frame factor. Prompt §2.2 asks "whether the ratio is explained"
  without a number to explain it by. Ruling 12.
- **Q is finite on every parked sample at the dispatch tree,** at all four Q_labels, on all four
  histories (244–2 142 parked samples each). So the closure (R15) needs only the ratio passed in.
  - The largest |Q| on parked samples at k/H = 10 is 2.27 at M = 10⁻³, β = 1.2. It lies at N = 28.3742,
    0.0023 e-folds after `Parking.N` = 28.3719. The largest elsewhere, 3.73, is at the bounce just before
    it. So the spline of asinh(m) carries the switch's step into the first parked samples.
  - At β = 2 the parked maxima (2×10⁻⁸ to 7×10⁻⁵) lie at the history's last sample.
  - Ruling 13 asks where each maximum lies relative to the switch.
- **ρ_χ moves M²_eff/H² by up to 12 %** (M = 10⁻³, β = 1.2, at N = 19.65, before the switch), 1.1 % at
  β = 2 and 0.17 % at M = 0.5, β = 5; Ḣ/H² by at most 10⁻³.
  - The 12 % is a relative change, likely near a zero of M²_eff. §2.4 then needs an absolute measure
    beside it. Ruling 13.
- **The conformal mass's E has an exact precedent.** `ODEPolicy.__call__` (:701–:707) forms the kick's E
  as `G - V_over_3H2Mp2` when `r_chi == 0.0`, and otherwise as `matter_and_dark_radiation_fractions`'s E.
  At r_χ = 0 that function is not bitwise `G - V/3H²M_P²`, since it multiplies and divides by 1 + f_m.
  So `AdiabaticComputePolicy` must take the same branch, not call the function unconditionally. Ruling 9.
- **`extra_data` at the dispatch tree is 124–150 characters.** The three first failures alone are 94–387
  characters (94 at M = 10⁻³⁰, where one kind fails). So the margins key passes 256 on test 1's history
  (M = 0.5, β = 2) without a constructed case.
- **Each untuned history of §2 integrates in 2–6 s** here. §2's 9 histories, three rounds of §2.6, and the
  dispatch-commit comparisons are minutes, not hours.
- **Two tools read history rows without the factory.** `tools/bbn_from_store.py`'s `_FIND_QUERY` joins
  `ScalarHistory` to the model by raw SQL, with no α. While every store holds α = 0 alone that is right.
  At 10 it is not. Ruling 15.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The registry's pins move with the registry.**
   - `test_datastore_config.py` gains `alpha_value` in `EXPECTED_FACTORIES` and
     `EXPECTED_SERIAL_BATCH_SIZES`, at the position ruling 2 gives.
   - `test_inventory_specs.py` gains it in its class list.
   - Any other test that pins the registry, the replicated tables or `ScalarHistory`'s parents gains it
     too.
   - Each such edit is §1.9's "α row" class. The log lists each module, its edit and its reason. Any
     other edit to an existing test is still a stop.

   *STRUCTURALLY REQUIRED* (§0, first point: prompt §1.1 against §1.9 and §5).
2. **Where `alpha_value` sits.**
   - `CosmologyConcepts/alpha_value.py` mirrors `beta_value.py`, and `CosmologyConcepts/__init__.py`
     exports it beside `beta_value`.
   - In `config/datastore.py` it is registered immediately after `beta_value`, with serial batch size
     200, as `beta_value`. In `config/sharding.py` it is in `replicated_tables` immediately after
     `beta_value`. It is not in `read_table_config` (10's, if the plots need it).
   - It is in no `--drop` group. The agent shows that `tables_to_drop(["scalar-history"])` accepts
     without it (it is a parent of `ScalarHistory`, not a child), and says so in the log.

   *IMPLEMENTATION CHOICE*, ruled here.
3. **The payload's dark-radiation record** (prompt §1.3, "in one record of their own"):
   - A namedtuple `DarkRadiationValues`, under the payload key `"dark_radiation_values"`, with fields
     `rho_chi_ratio_1MeV`, `rho_chi_ratio_70keV`, `rho_chi_ratio_today`, `Delta_N_eff_1MeV`,
     `Delta_N_eff_today`. The existing key `"dark_radiation"` (the ledger) is not renamed.
   - A module-level function `dark_radiation_values(result, policy, cosmology)` beside `fixed_T_values`,
     called right after it.
     - The crossing is `_T_Jordan_crossing_step(result, log(T_MeV * units.MeV))` with
       `FIXED_T_JORDAN_HIGH_MEV` and `FIXED_T_JORDAN_LOW_MEV`, so its N is `fixed_T_values`'s, bitwise.
     - The state is `result.solution.interpolants[k](N)`, as `fixed_T_value_at` reads it. r_χ is
       `policy(N, state).rho_chi_over_rho_rad`, and g_ρ is `cosmology.G_rho(data.T_Jordan)` from that
       call, as the sampling loop takes `gstar_rho`.
     - Today is `result.solution(result.N_final)`, with its own policy call and g_ρ.
   - Two module constants: `NEFF_PER_G_RHO = 4.0 / 7.0` and `NEFF_NEUTRINO_HEATING = (11.0 / 4.0) **
     (4.0 / 3.0)`. ΔN_eff(1 MeV) = `NEFF_PER_G_RHO * g * r`, and ΔN_eff(today) = `NEFF_PER_G_RHO * g *
     NEFF_NEUTRINO_HEATING * r`, in that order, so that test 6 repeats the operations.
   - It is computed at every α. Nothing branches on α. At α = 0 the ratios are 0.0 because the ledger is
     empty, and the ΔN_eff are 0.0 because the ratios are.
   - A temperature the history does not reach gives `None` in its ratio and its ΔN_eff. A failure payload
     does not carry the key.
   - The log says whether `rho_chi_ratio_today` equals the last sample's ratio bitwise (it should, where
     the grid ends at ln(1 + z) = 0).

   *IMPLEMENTATION CHOICE*, ruled here.
4. **The per-sample ratio.**
   - The sampling loop appends `data.rho_chi_over_rho_rad` from the `data` it already computes. The
     payload key `"rho_chi_over_rho_rad"` is a tuple.
   - `ScalarHistoryValue` takes `rho_chi_over_rho_rad` as a **required keyword, with no default**.
   - `tools/history_and_bbn.py:446` passes the payload's ratio, zipped with the samples. Its output is
     unchanged at α = 0. The file joins the prompt's §6 list. *STRUCTURALLY REQUIRED* (§0, third point).
   - `ModelFunctions` gains no spline of the ratio; Δg* is 10's.

   Otherwise *IMPLEMENTATION CHOICE*, ruled here.
5. **`ScalarHistory.store()` reads every new payload key with `.get`.**
   - These are `"rho_chi_over_rho_rad"`, `"dark_radiation_values"`, `"injections"`, `"parking"`,
     `"release"` and `"margins"`. An absent key means "not computed": its columns are NULL.
   - Zeros are never inferred from an absent key. R24's zeros come from a real payload at α = 0, which
     carries every key, and test 4 asserts them on one. B7 is caught there.
   - An absent `"rho_chi_over_rho_rad"` with a non-empty `"sample"` raises `RuntimeError`, never a
     default. With an empty sample (the stand-ins) it gives no values.
   - `test_first_bounce_round_trip` and `test_fixed_T_values_round_trip` then change only by the α row
     their `ScalarHistory(...)` constructor needs.

   *STRUCTURALLY REQUIRED* (§0, second point).
6. **The columns from the records.**
   - `injections` counts the candidates with `delta_K > 0`. `resolved_candidates` and
     `unresolved_candidates` count the candidates by `Injection.resolved`. All are Python ints.
   - `injected_fraction` is summed left to right, starting from `0.0`, over every candidate's
     `delta_rho_over_rho_rad` in `injections` order, then adding `Parking.delta_rho_over_rho_rad` if the
     history parked. So it is a float at α = 0 too.
   - `log_R_first_impact` is `injections[0].classical_turning_point.log_R` (the field's name as 04 built
     it). It is NULL if there is no candidate or no assessment.
   - The parking and release columns are `Parking.N`, `Parking.log_T_Jordan − log(units.GeV)`,
     `Release.N` and `Release.log_T_Jordan − log(units.GeV)`. Each is NULL where its record is None.
   - The margin columns are `Margins`'s fields of the same names. All of them, and `unassessed_share`,
     are NULL where `margins` is None.

   *IMPLEMENTATION CHOICE*, ruled here.
7. **"Bitwise" for a converted column** means what `test_first_bounce_round_trip` means.
   - The raw row equals the forward conversion of the payload's value bitwise (`log_T − log(GeV)`).
   - The object's value equals the inverse conversion of the row bitwise (`row + log(GeV)`).
   - The test also asserts object == payload where that holds. The log says, per converted column on
     test 1's history, whether it held. Where it does not, that is the conversion's rounding, not a
     failed round trip, and it is not a stop.
   - Every unconverted column (N, ratios, shares, margins, counts, the per-sample ratio) is bitwise
     object == row == payload.

   *IMPLEMENTATION CHOICE*, ruled here.
8. **The object.**
   - `ScalarHistory` takes `alpha` (an `alpha_value`) as a required keyword, beside `tuning_tol`. It
     checks it in `compute` (`check_required_parameter("_alpha")`) and exposes it as `alpha`.
   - The whole-history values are one namedtuple, `DissipationValues`, whose fields are prompt §1.3's
     column names in the table's order. It is exposed as `ScalarHistory.dissipation`.
     - The ln T_J fields are in the cosmology's units on the object, as `FirstBounce` holds them.
     - It is `None` on a failure. It is read with `_do_not_populate`, as `tuning` is.
   - `compute_scalar_history` takes `alpha: float = 0.0` as its last keyword and passes it to
     `tune_history`. `ScalarHistory.compute` passes `alpha=self._alpha.value`.

   *IMPLEMENTATION CHOICE*, ruled here.
9. **`AdiabaticComputePolicy` takes the ODE's branch, verbatim.**
   - `M2eff_over_H2` and `Hdot_over_H2` gain a **required** `r_chi` argument, and pass `r_chi=r_chi` to
     `Vprimeprime_over_3H2Mp2`, `V_over_3H2Mp2` and `Hdot_over_H2_plus_3`.
   - E is `G - V_over_3H2Mp2` when `r_chi == 0.0`, and otherwise `matter_and_dark_radiation_fractions`'s E,
     as `ODEPolicy.__call__` forms it (:701–:707). The clamp at E < 0 is unchanged.
   - `compute_adiabatic_values` passes each sample's `value.rho_chi_over_rho_rad`.
   - The derivation of prompt §1.5 is written in the log before this code changes. It must say where ρ_χ
     enters:
     - the self mass, through H² in V″/H²;
     - the gravitational term, through Ḣ/H²;
     - the conformal term, through E's normalization only, since δρ_χ,E/δφ = 0 at fixed a_E.

   *IMPLEMENTATION CHOICE* (the branch) and *STRUCTURALLY REQUIRED* (that it is not the unconditional
   call, §0, eighth point).
10. **Test 8's pins at α = 0.**
    - Taken in a temporary worktree of this note's commit, by a script the log quotes whole.
    - The history is the golden setup, stored and read back through `_TempStoreCase`, so that the ratio
      the policy reads is the store's.
    - The pins are `max_abs_Q_values` by `repr`, and a SHA-256 of `repr((raw_N_grid, [abs_Q_samples[l]
      for l in Q_labels]))`.
11. **Test 9 is the identity, not the limit.**
    - At one sample of test 1's history inside BBN's window, form BBN's ratio by `compute_BBN_data`'s
      expression (cited by line). Do it from `ODEPolicy` and `HubblePolicy` at that state twice: with the
      history's ledger, and with an empty one.
    - Assert that the difference is F·r_χ, F = A²(1 + (ln A)′π)²/G (§0, fifth point). The tolerance is
      derived from the subtraction's rounding: a few ε times the larger ratio, over F·r_χ. Its value is
      given.
    - The log derives F, and reports F at the 1 MeV crossing on every history of §2.1.
    - BBN is not changed (P9). That ΔN_eff(1 MeV) (R22) does not carry F is recorded under "Observations
      not acted on", for the user.

    *STRUCTURALLY REQUIRED* (§0, fifth point: the prompt's limit fails by 10² at β = 5).
12. **§2.2's ratio is checked against the closed form** of §0, sixth point. For each history of §2.1:
    - give ΔN_eff(T_CMB)/ΔN_eff(1 MeV);
    - give K(N_final)/K(N_1MeV) from the ledger, and the g_s factor from the cosmology at the two T_J;
    - give the relative residual.

    A residual above 100 × rtol is explained in the log. It is not a stop. *IMPLEMENTATION CHOICE*, ruled
    here.
13. **§2.3 and §2.4, located.**
    - For each maximum |Q| on the parked samples, give its N − `Parking.N`, and whether it is within the
      first 0.01 e-fold after the switch or at the last sample.
    - §2.4 gives, beside the largest relative change of M²_eff/H², the largest absolute change over
      max(|m|, 1), and where each lies.

    *IMPLEMENTATION CHOICE*, ruled here.
14. **§2.4 and §2.5 at the dispatch commit.**
    - One temporary worktree of this note's commit, removed afterwards.
    - At the dispatch tree a dissipation history cannot be stored (`compute_scalar_history` takes no α),
      so `compute_adiabatic_values._function` runs there on a stand-in proxy. The stand-in holds the
      cosmology, the realized model and `ScalarHistoryValue`s built from that tree's own
      `integrate_history(..., alpha=0.1)` payload.
    - §2.5 compares by `repr` every payload key that existed, except `metadata`'s compute times and
      `z_grid`'s object address. The two new keys are reported apart: every ratio 0.0, and the record's
      zeros.

    *IMPLEMENTATION CHOICE*, ruled here.
15. **The readers and tools.**
    - `tools/bbn_from_store.py` is not changed. Its raw SQL has no α. The log records it under "State
      handed to the next prompt" for 10.
    - `tools/inventory_report.py` is not changed unless its tests fail without it. Then add the least, and
      log it (prompt §1.7).
    - `plot_inputs.PlotParameters` gains `alpha` after `tuning_tol`. Its lookup is
      `pool.object_get("alpha_value", value=0.0)`, labelled as the constant π lookup is ("the constant
      α", "α = 0"), so a store without the row gives the existing `PlotOptionNotStored`.
    - `main.py` fetches the α = 0 row once, in the `ray.get` beside the tolerances (:989).
16. **No file but the prompt's §6 list, `tools/history_and_bbn.py` (ruling 4), and the registry-pinned
    tests of ruling 1 changes.** In particular these are byte-identical:
    - every golden module and golden JSON that exists, the old fingerprint goldens included;
    - `test_kinematic_cap_loop`, `test_reflection_reporting`, `test_injection`, `test_parking`,
      `test_release`, `test_margins`, `test_wall_model`, `test_wall_rounding`, `test_adiabatic_mass`;
    - `CLAUDE.md` (its counts move at 12).

    In `ScalarHistory.py` the step loop (`integrate_scalar_history`), `IntegrationResult`, every record of
    02–08 and `SampleValues` are unchanged. `git diff` there touches only:
    - the sampling loop's append;
    - the new record, function and constants;
    - `integrate_history`'s call and payload;
    - `compute_scalar_history`, `build_extra_data`, `ScalarHistory` and `ScalarHistoryValue`.
17. **Timing and the suites.**
    - §2.6's costs are the minimum of three rounds per history, with `uptime` beside them.
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output to a
      file in your own scratch directory, grep the verdict from it, and record `uptime` beside each.
    - A run that may pass 600 s goes in the background. Wait for its notification without polling.
    - §2's histories run at most four at a time. One that has not ended after 15 minutes is stopped and
      recorded as such.
    - A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. §1.5's derivation, with ruling 9's three places, and ruling 11's F, written down before any code.
  3. The class, the registry, the factory, the object, the payload's keys and `AdiabaticHistory`.
  4. The readers.
  5. The version and the golden fingerprint.
  6. The tests of prompt §1.9.
  7. The breakages B1–B8. Then the measurement of §2.
  8. The suites, `black --check`, the `--help`s and `main.py --inventory`.
  9. The records: the log, `.documents/`, both boards, and `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`, not
  zsh. None is committed.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file goes
  under the repository. The temporary store for `main.py --inventory` is built there by the new tests'
  harness, and deleted afterwards. The dispatch commit's worktree is removed, so that `git worktree list`
  shows only the main checkout.
- **Stores and Ray.** No store but the tests' temporary ones and the inventory's scratch one. No Ray.
  Nothing is written to `var/`. The new golden fingerprint file is the only golden written. No existing
  golden is regenerated.
- **Stop conditions are real.** They are:
  - the prompt's §5, with ruling 1's edits allowed and ruling 11 replacing test 9's limit;
  - an existing test failing for a reason this note does not name.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `e7f8f80`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The pins.** `grep -rln '"ScalarHistory"'` over the repository, outside `venv/` and `thirdparty/`,
   and every module it found read where it names the registry, the specs or a history's key. That is
   how the first three points of §0 were found.
3. **`probe.py`** on four histories (§0, fifth to eleventh points).
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board, logs 06, 07 and 08, and the probe's README;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 16. Its log is `logs/09-store.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store** but the temporary ones.
- **Stop and ask** on any of the prompt's §5 conditions (as amended by rulings 1 and 11) or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the schema as built: each new column, its type and its source;
  - §1.5's derivation, and F;
  - every existing test edited, with its edit and its reason;
  - the new tests by name, each with what it asserts and its tolerance;
  - the tables of §2;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.**
   - `git show --stat <agent>` lists exactly ruling 16's files.
   - Its parent is this note's commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
   - Ruling 16: `git diff` on `ScalarHistory.py` leaves the step loop, `IntegrationResult`, the records
     of 02–08 and `SampleValues` alone.
2. **α = 0 and the frozen tests.**
   - The modules of ruling 16 pass at `<agent>` unedited.
   - `git diff` on every existing test module is the α row, a registry pin, format 6 or 2026.8.0, and
     nothing else.
3. **The schema** against prompt §1.1–§1.4 and rulings 2–8, line by line:
   - the table, its factory, its place, replication;
   - the key's column and filter;
   - the parent;
   - each column's name, type, nullability and source;
   - the `.get` reads and the `RuntimeError`;
   - the NULLs on a failure row;
   - the `Text` column;
   - the JSON's keys and nothing else of `Generation`.
4. **The physics.**
   - Re-derive §1.5 independently, and check ruling 9's three places against the log and the code.
   - Re-derive F (ruling 11) and the closed form of ruling 12.
5. **An independent probe** of my own (session scratchpad, not committed):
   - store and read back M = 0.5, β = 5 and β = 2 at M = 10⁻³⁰ at α = 0.1 on a temporary store, and
     compare every new column with my own computation from the payload, without the agent's functions;
   - Q on the parked samples of those two, against `probe.py`'s numbers at the dispatch tree;
   - bitwise against the dispatch commit on the golden setup at α = 0, in a temporary worktree that I
     remove.
6. **The breakages.** Replay each of B1–B8 with `git apply` in `bash`. Each must fail the tests the log
   names, and is then reverted.
7. **The numerics of the tests.** Check test 6's hand computation, test 9's tolerance and F, test 8's
   pins, and the converted columns of ruling 7.
8. **The version.**
   - `FINGERPRINT_FORMAT` 6 with its history paragraph.
   - `SPEC_DIGESTS[6]` regenerated by the guard's function, by me, and equal.
   - The new golden file beside the old ones, which are unchanged.
   - `VERSION_LABEL` 2026.8.0 with its history line.
9. **The measurement and the records.**
   - **§2:** the tables are complete. §2.1 has the 8 histories. §2.2 has ruling 12's residuals and F.
     §2.3 has ruling 13's locations. §2.4 has both measures. §2.5 is bitwise. §2.6 has its three rounds.
   - **The log:** it is in §5.1's form, with the derivation, the schema, the edited tests, the breakages,
     and every deviation classed, §0's rulings among them.
   - **`.documents/`:** the notes are dated and additive only.
   - **The issue:**
     - the board's row 09, the gate Store, and §5.11;
     - `[00-settling-at-physical-M-needs-a-parked-tracking-model]` closed on this board's §3.1 and moved
       on the `integrator-remediation` board from §3 to §4, each with its "Closed" line;
     - its row deleted from `.documents/OPEN_ISSUES.md` §1.20, with the count and the date corrected.
10. **The suites** at `<agent>`, run serially:
    - the counts the log gives, each at least the baselines;
    - each change explained by the new tests;
    - `black --check` on the changed `.py` files;
    - the four `--help`s exit 0.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in both boards, the log and `.documents/OPEN_ISSUES.md` wherever it appears;
- the board's row 09 landed, the gate Store held, and this note added to the list of orchestrator notes,
  marked "used for 09";
- under `### 5.11`, an *Orchestrator review of prompt 09* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not residue
is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Store;
- the counts;
- rulings 1, 5 and 11, and why;
- the stored values across M, the ΔN_eff ratio against the closed form, and F at 1 MeV;
- Q on the parked samples, and the closed issue;
- what does not change;
- the costs;
- anything opened or closed;
- that 10 is written next, from 09's "State handed to the next prompt" (README §2.1).
