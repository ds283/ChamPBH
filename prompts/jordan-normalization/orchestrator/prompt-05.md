# Orchestrator — prompt 05, the Planck mass

Read [`../README.md`](../README.md) first: §0.2 ("The units"), §2.2 (05's charter), §4, §5, §6.0 R3,
§6.1 U5, §6.2 P3, P9 and P13. Read 04's log too, in particular "State handed to the next prompt".
The conventions of [`prompt-04.md`](prompt-04.md) bind this prompt as well. They are restated below
where 05 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`05-the-planck-mass.md`](../05-the-planck-mass.md)
**Board items:** K1–K3 · **Gate:** "Units" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **Network:** none.

**Gate:**
- 04 landed (`597a215`) and reviewed (`58c1c93`); the gate "Model" holds (board §2).
- Since then the branch has taken, by merge from `main` (`5fc02a6`), `ray-sqla-align` (`668d30f`,
  `e04fbb8`): ray 2.59.0 and SQLAlchemy 2.0.55, `config/ray_auth.py` and Ray's token mode.
  `git diff --stat 58c1c93 HEAD -- . ':!prompts' ':!.documents' ':!science-runs'` lists only
  `CLAUDE.md`, `ObjectFactories/tests/test_package_provenance.py`, `config/ray_auth.py`, `main.py`,
  `plot_ScalarHistory.py`, `plot_by_beta.py`, `requirements.txt`, `runregistrykit.toml`,
  `tools/tests/test_ray_auth.py`, `tools/tests/test_registry_config.py` and `utilities.py`. None of
  them is in 05's path, but **tools' count is now 102, not 93**.
- **The merge left one test failing**, `tools/tests/test_ray_auth.py`'s check of
  `plot_ScalarModel.py`, a file this branch renamed in 02. At the user's word ("Housekeeping commit
  now"), the orchestrator fixed it in `cfd2e11`: the test, its method name, the comment in
  `config/ray_auth.py` and `CLAUDE.md`'s token-mode bullet now name `plot_ScalarHistory.py`.
- `HEAD` is the commit that adds this note, on `jordan-normalization`, on top of `cfd2e11`, with
  `git status` clean.
- The prompt's own gate: no tracked change, the five suites at the baselines below, and no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 06 is not dispatched until 05 is reviewed.

## 0. What makes this prompt unusual

**One constant, but its effect is not the uniform 2.8 × 10⁻⁴ the prompt expects, it reaches twelve
existing tests the prompt does not name, and three of the prompt's premises are wrong for this
tree.** The orchestrator measured these at `5fc02a6` and `cfd2e11`, before writing this note, with
probes in the session scratchpad (not committed). The probe tree was a worktree with exactly the
expressions of rulings 1 and 2 below.

- **The constant.** `6.70883e-45` MeV⁻² gives, by ruling 1's expression,
  `REDUCED_PLANCK_MASS_EV = 2.4353235955263046e+27` (`0x1.f79cf52984fafp+90`).
  2.436e27 / that − 1 = **2.7775 × 10⁻⁴**.
- **PRyMordial's expression is not the literal.** In `PRyM/PRyM_init.py:136`,
  `GN = 6.70883*1.e-39*1.e-6` evaluates to `0x1.3267ba6db6b1cp-147`. The literal `6.70883e-45` is
  `0x1.3267ba6db6b1dp-147`, one ulp higher. The two are the same decimal number, so the test
  compares decimally (ruling 3).
- **The consistency checks**, with rulings 1 and 2, are the same in each of the three systems:
  1 K against k_B **1.70 × 10⁻⁷**, 1/eV against ħc **4.08 × 10⁻⁸**, against ħ **1.58 × 10⁻⁷**, and
  1 kg **2.13 × 10⁻⁷**. With 2.436e27, every one of them is **2.78 × 10⁻⁴**. The prompt's numbers are
  confirmed.
- **The three "pinned strings" do not carry the constant.** `test_inventory_report.py` and
  `test_store_fingerprint_golden.py` insert `M_value` rows with the stored literal
  `value_eV = 2.436e24`, and the report prints it back. `test_compare_stores.py` inserts
  `value_eV = 2.435e24` and pins no string with it. None of them computes through a unit system, so
  none of them moves with the constant (ruling 5).
- **The golden histories, old against new** (§2 item 2's measurement). Both trees were run by
  `ComputeTargets/tests/golden_history.run_history`, and samples were compared at equal index. The
  sampled ln(1 + z) labels are equal at every common index, and `n_samples` is unchanged (2723 and
  4894). The relative difference is |a − b|/max(|a|, |b|), and 0 where both are 0.

  | field | β = 1.5: max (sample) | > 10⁻² | β = 5.5: max (sample) | > 10⁻² |
  |---|---|---|---|---|
  | `raw_N` | 1.388e-04 (0) | 0 | 6.977e-06 (0) | 0 |
  | `phi_Einstein` | 4.743e-05 (2024) | 0 | 1.502e-03 (4642) | 0 |
  | `pi_Einstein` | 1.388e-04 (0) | 0 | **4.105e-01** (4696) | 7 |
  | `log_rhorad_Einstein` | 1.155e-05 (0) | 0 | 6.842e-05 (0) | 0 |
  | `log_rhorad_Jordan` | 8.805e-06 (0) | 0 | 8.798e-06 (0) | 0 |
  | `log_fm` | 1.958e-05 (2722) | 0 | 1.958e-05 (4893) | 0 |
  | `H_Einstein` | 5.554e-04 (1782) | 0 | 5.552e-04 (4287) | 0 |
  | `H_Jordan` | 5.719e-04 (1576) | 0 | 6.512e-04 (2440) | 0 |
  | `log_T_Jordan` | 8.565e-06 (0) | 0 | 8.558e-06 (0) | 0 |
  | `gstar_rho` | 1.533e-07 (2160) | 0 | 9.737e-08 (4327) | 0 |
  | `gstar_s` | 1.451e-07 (2135) | 0 | 9.228e-08 (4327) | 0 |
  | `dgstar_s_dlogT` | 3.883e-05 (364) | 0 | 8.184e-05 (187) | 0 |
  | `dgstar_rho_dlogT` | 2.334e-05 (180) | 0 | 7.227e-05 (408) | 0 |
  | `Sigma` | 4.441e-07 (2168) | 0 | 1.539e-06 (4642) | 0 |
  | `friction_term` | 1.388e-04 (0) | 0 | **4.105e-01** (4696) | 7 |
  | `reflecting_term` | 1.374e-04 (2024) | 0 | **1.023e-01** (4623) | 120 |
  | `kicking_term` | 4.481e-07 (2168) | 0 | 1.560e-06 (4642) | 0 |
  | `log_one_plus_z_Jordan` | 2.820e-08 (2314) | 0 | 1.541e-05 (4892) | 0 |

  The medians of `H_Einstein` and `H_Jordan` are 5.55 × 10⁻⁴, which is 2 × 2.78 × 10⁻⁴: H ∝ T², and
  the history ends at T_J = 1 MeV, which is an eV-labelled input.

  The scalars:

  | | β = 1.5 old → new | β = 5.5 old → new |
  |---|---|---|
  | `n_samples` | 2723 → 2723 | 4894 → 4894 |
  | `reflections` | 0 → 0 | 0 → 0 |
  | `accepted_steps` | 1231 → 1233 | 26343 → 26331 |
  | `compute_steps` = `RHS_evaluations` | 9887 → 9870 | 232810 → 232530 |
  | first bounce N | 18.64397081911504 → 18.64397081911831 | 32.10765422995129 → 32.10765422995111 |
  | first bounce ln T_J | −43.35467531703388 → −43.35439760833413 | −37.03892534804545 → −37.03864763934282 |
  | `fixed_T_values` | (0.0023279537090081345, −0.05135980555622314, None, None) → (None, None, None, None) | (None × 4) → (1.904834518987192e-05, 0.017290191260207102, None, None) |

- **Why three fields exceed 10⁻² at β = 5.5.** The prompt's stop ("a difference above 10⁻² in any
  field … since nothing in the change can produce one") is wrong. Every sample above 10⁻² lies in
  the tail of the β = 5.5 history (samples 4308–4893), where φ sits against the potential's wall at
  M/φ ≈ 37–60.
  - **`reflecting_term`** goes as V ∝ e^{M/φ}, so a relative shift δφ/φ in φ moves ln V by about
    −(M/φ) δφ/φ. At eight samples from 4308 to 4873, −(M/φ) δφ/φ predicts the measured change in
    ln `reflecting_term` to within 5 %. For example, at sample 4697, M/φ = 43.0 and δφ/φ = −1.12e-3
    predict +4.83e-2, and +5.06e-2 is measured.
  - **`pi_Einstein`** (and `friction_term`, which follows it) differs by more than 10⁻² at seven
    samples, each next to a turning point of π inside a bounce. On the scale of the largest |π| over
    samples 4550–4893, the largest difference is **1.18 × 10⁻²**, at sample 4696. That is the
    bounce's phase moving by a fraction of a step.

  These are amplifications of the 2.8 × 10⁻⁴ input, not errors. Since the computation is bitwise
  deterministic and rulings 1 and 2 fix the expressions, the agent's table must reproduce this one
  digit for digit (ruling 7).
- **Why `fixed_T_values` flips.** The golden histories stop at T_stop = 1 MeV, and the first fixed
  temperature, `FIXED_T_JORDAN_HIGH_MEV`, is also 1 MeV. `log(1.0e-3 * units.GeV)` and
  `log(1.0 * units.MeV)` are bitwise equal with both constants. So the "first crossing" of
  T_J = 1 MeV is the history's own last node, and whether `_T_Jordan_crossing_step` finds it (an
  exact zero or a sign change on the last step) is decided by rounding at the stop. Where it is
  found, the value is the last sample's φ bitwise (0.0023279537090081345 at β = 1.5 old;
  1.904834518987192e-05 at β = 5.5 new). This is a property of the golden setup, not of 05's change,
  and is not a defect in production, where T_stop is far below 1 MeV, nor after 06, where every
  history runs to T_CMB.
- **The golden histories** took 0.66–0.88 s (β = 1.5) and 10.9–11.2 s (β = 5.5) at a load of about 7.
- **Twelve existing tests pin values measured on the old units.** On the probe tree (rulings 1
  and 2, nothing else), these fail besides the two golden histories:
  - `ComputeTargets/tests/test_kinematic_cap_loop.py`, nine tests: `test_a_M_0p5`, `test_a_M_0p01`,
    `test_a_M_0p001`, `test_b_M_1em10`, `test_b_M_4p1em28`, `test_c_convergence_in_f`,
    `test_d_P3_window`, `test_e_P2_window_and_jacobian_clamp` and
    `test_f_trial_state_exceptions_are_rejected_steps`;
  - `ComputeTargets/tests/test_integrator_exceptions.py`, three: `test_c_physical_values_unchanged`,
    `test_f_prompt_01_rejection_test_is_quiet` (which runs the cap-loop's `test_f`) and
    `test_f_rejection_inside_the_timer_is_quiet`;
  - `ComputeTargets/tests/test_first_bounce.py`, one: `test_a_P1_window_at_M_0p5`;
  - `tools/tests/test_plot_surfing_histories.py`, one:
    `test_beta_3_is_not_captured_and_bounces_as_the_committed_scan`.

  That is fourteen tests, of which `test_f_prompt_01_rejection_test_is_quiet` only re-runs another,
  so twelve pinned values. They are the integrator audit's references (integrator-remediation
  README §6.1), measured at the old units at tolerances of 10⁻⁵ (10⁻¹⁰ in `test_c`). On the new
  units the final φ moves by 1.17–1.22 × 10⁻⁴ (P1), 3.68 × 10⁻⁴ (P3) and 1.46 × 10⁻⁵ (P2), and the
  first bounce's N by about 1.0 × 10⁻⁵. P2's count of wall bounces to N = 40 goes from **19 to 20**:
  π has a shallow minimum near N ≈ 36.53 that stays positive on the old units (π's largest value
  on [35.9, 37.5] is 2.29 × 10⁻³, with no sign change) and dips below zero on the new, between
  N = 36.5175 and 36.5444, which `wall_bounces` counts as a turning point at φ = 0.01391 < 1.5 M.
  The surfing test compares the β = 3 history's first bounce bitwise with the committed fine scan
  `.documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv`, a dated record. The
  bounce T moves by 2.13 × 10⁻⁸ relative, and N by 9.1 × 10⁻¹⁰.

  The step counts these tests pin exactly do not move (1945 RHS and 224 accepted steps for P1 at
  M = 0.5), and every RHS bound still holds. The probe states P1, P2 and P3 are state vectors in
  Planck units and stay as they are: they are the tests' inputs. **The user ruled (multiple choice,
  2026-10-26) "Re-pin in 05"**: 05 replaces each reference with its value on the new units (ruling
  13). The orchestrator applied ruling 13's table to the probe tree, and all four modules pass.
- **The other failures on the probe tree belong to the worktree**, not to the units:
  `test_ray_cluster_lock` (eight; the worktree has no `venv/`), `test_registry_config` (the
  registry's root is the main checkout) and `test_registered_run`'s script-path test. They pass
  in the main checkout.

**Baselines** at `5fc02a6`, serially, at load 23 falling to 6: CosmologyModels **18**,
ComputeTargets **61 + 144**, ObjectFactories **86**, tools **102**, RunDrivers **61**. All passed
except tools' one error, which `cfd2e11` fixed. At `cfd2e11`, `tools.tests.test_ray_auth` runs 9
tests, all passing. These are 04's counts, with tools +9 from `ray-sqla-align`'s
`test_ray_auth.py`. Other workloads share the machine.

**These fourteen points, where the prompt is loose, silent or wrong for this tree, are ruled here.**

1. **The constant (K1), by these expressions.** In `Units/base.py`, at module level after the
   imports, with `from math import pi, sqrt` added:
   - `NEWTON_G_MEV_MINUS_2 = 6.70883e-45`. Its comment says three things: it is PRyMordial's
     `GN` (`PRyM/PRyM_init.py`, the PDG value, 6.70883 × 10⁻³⁹ GeV⁻²); it is in MeV⁻²; and it is
     CODATA 2018's G = 6.67430 × 10⁻¹¹ m³ kg⁻¹ s⁻², the G from which the Planck length, mass, time
     and temperature literals of the three unit systems are derived.
   - `REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2)`. Its comment says that
     this is 1/√(8πG_N), converted from MeV to eV, and that PRyMordial's `Mpl = 1/√G_N` is the full
     Planck mass from the same G_N, √(8π) times this.

   Do not write the expression another way: `1.0 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2 * 1.0e-12)`
   is one ulp lower, and §0's table depends on the bits. Nothing in `Units/__init__.py` changes.
   Classify as `IMPLEMENTATION CHOICE`, citing this note.

2. **The three systems (K2), by these expressions.** Each imports
   `from Units.base import REDUCED_PLANCK_MASS_EV, UnitsLike`.
   - `Planck_units`: `eV = PlanckMass / REDUCED_PLANCK_MASS_EV`.
   - `Mpc_units`: `eV = PlanckMass / REDUCED_PLANCK_MASS_EV`.
   - `GeV_units`: `PlanckMass = REDUCED_PLANCK_MASS_EV * eV`.

   No other statement changes, and no literal changes. The comments say where each number comes
   from: the Planck length 1.616255e-35 m, mass 2.176434e-8 kg, time 5.391247e-44 s and temperature
   1.416784e32 K (CODATA 2018), the megaparsec 3.08567758e22 m, and the electronvolt from
   `REDUCED_PLANCK_MASS_EV`. The sentence "here we are choosing it to be Mpc instead", which
   `Planck_units` and `GeV_units` copied from `Mpc_units`, is corrected in those two files.
   Classify as `IMPLEMENTATION CHOICE`, citing this note.

3. **The comparison with PRyMordial (K3, first bullet) is decimal.** The prompt's "equals … to the
   last digit" cannot be a float comparison of PRyMordial's expression, which is one ulp off the
   literal (§0). The test:
   - reads `PRyM/PRyM_init.py` as text, at a path built from the test's `__file__`. It never
     imports `PRyM`;
   - finds the one line matching `^GN\s*=\s*(.+?)\s*#`, and requires exactly one such line;
   - splits the expression on `*`, takes each factor as a `decimal.Decimal`, and requires their
     product to equal `Decimal(repr(NEWTON_G_MEV_MINUS_2))`;
   - requires the line `Mpl = 1./np.sqrt(GN) # Planck mass in [MeV], PDG` to be present verbatim,
     so that a change in PRyMordial's definition of `Mpl` fails;
   - requires `REDUCED_PLANCK_MASS_EV * sqrt(8.0 * pi)` to equal `1.0e6 / sqrt(G)` to 10⁻¹⁵
     relative, where G is the float of that decimal product. This is the statement that ChamPBH's
     M_P and PRyMordial's `Mpl` come from one G_N.

   The log records the one-ulp difference under "Observations not acted on", with no issue: it is
   PRyMordial's own expression, measured, not a defect (`PRyM/` is not patched here).
   Classify as `STRUCTURALLY REQUIRED`.

4. **The consistency tests (K3, second bullet)** are as the prompt says, in each of the three
   systems, at 10⁻⁶, against k_B = 8.617333262e-5 eV/K, ħc = 1.973269804e-7 eV m,
   ħ = 6.582119569e-16 eV s and 1 kg = 5.609588604e35 eV:
   - `units.Kelvin / units.eV` against k_B;
   - `(1.0 / units.eV) / units.Metre` against ħc;
   - `(1.0 / units.eV) / units.Second` against ħ;
   - `units.Kilogram / units.eV` against the kilogram.

   In each system, `units.PlanckMass / units.eV` also equals `REDUCED_PLANCK_MASS_EV` to 10⁻¹⁵
   relative. The measured values are §0's. Use `subTest` per system and quantity. The test's
   docstring names each reference value as CODATA 2018's.
   Classify as `STRUCTURALLY REQUIRED`.

5. **The three "pinned strings" are not edited (K3, third bullet).**
   - `test_inventory_report.py` and `test_store_fingerprint_golden.py` print back a stored literal,
     and `test_compare_stores.py` already stores 2.435e24 and pins no string. None of them moves
     with the constant (§0).
   - Editing `test_store_fingerprint_golden.py`'s row would move the golden fingerprint's digests
     with no spec change. That would need a new golden file with no format bump, against P2.
   - The campaign's rule is that tests are modified only where a prompt changes what they test
     (README §5), and 05 does not change what these test.

   All three pass unedited, and the log says so. The prompt's §5 loses these three files.
   Classify as `STRUCTURALLY REQUIRED`.

6. **The grep (§2 item 3)** is
   `git grep -n -P '2\.436e\+?2[47]' -- . ':!prompts' ':!.documents' ':!science-runs'`.
   - Expected after the change: the stored literals of ruling 5, which are
     `tools/tests/test_inventory_report.py` (lines 130, 273, 312, 328 and 351) and
     `ObjectFactories/tests/test_store_fingerprint_golden.py:179`, and nothing in `Units/`.
   - The log lists every hit by file and line, and what remains in `.documents/` and
     `science-runs/` by file only.
   - `ComputeTargets/tests/test_initial_field_option.py:63`'s docstring already says
     2.435e18 GeV, and is not edited.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

7. **The change in values (§2 item 2) is §0's table, reproduced.** The method:
   - a `git worktree` of `HEAD` (this note's commit) in the agent's scratch directory, as 03's was;
   - a probe that dumps both golden histories' full samples, labels and scalars through
     `golden_history.run_history`, run once on each tree;
   - per history and field, the largest relative difference, its sample index, and the number of
     samples above 10⁻², as §0 defines them, over the common indices after checking that the
     labels are equal there;
   - the scalars of §0's second table.

   **The agent's tables must equal §0's in every digit shown.** Both trees are deterministic, and
   rulings 1 and 2 fix the bits. The log carries both tables in full, and quotes §0's account of
   the three wall-amplified fields as the orchestrator's measurement. The prompt's "above 10⁻² … is
   a stop" is replaced by ruling 12.
   Classify as `STRUCTURALLY REQUIRED`.

8. **The golden history (P3).**
   - The new record is `ComputeTargets/tests/data/golden_history_05.json`, written by the generator
     after the units change. `GOLDEN_PATH` in `golden_history.py` points to it.
   - The provenance's `code_tree` string says "jordan-normalization prompt 05's working tree, before
     its commit, on top of generated_at_commit".
   - The docstrings of `golden_history.py` and `test_golden_history.py` say that 05 regenerated it,
     and the file they name is the new one.
   - `golden_history_01.json` and `golden_history_03.json` are not touched (byte-identical).
   - The new record must show §0's scalars: `n_samples` 2723 and 4894, the step counts, and the
     `fixed_T_values` as listed.
   - `test_golden_history` is run alone before and after, and the log gives both times.

   `golden_history.py` and `test_golden_history.py` are added to the prompt's §5.
   Classify as `STRUCTURALLY REQUIRED`.

9. **The prose.**
   - `CLAUDE.md`'s "Units" bullet gains the prompt's one sentence, naming `Units/base.py`'s
     `REDUCED_PLANCK_MASS_EV`.
   - `.documents/architecture-summary.md` §8.1 gains a dated note
     (`> **Note, <date> (jordan-normalization prompt 05):** …`) on the constant, G_N's source, and
     the three systems that use it. The `Units/` tree at `:109-113` is unchanged.
   - Edit only what describes the tree. `numerical-strategies.md` and `prompts/INDEX.md` are not
     edited.

10. **The breakages.** Each is recorded as a diff that `git apply` replays from the agent's commit.
    1. `GeV_units` left at `PlanckMass = 2.436e27 * eV`: ruling 4's tests fail for `GeV_units`
       only (2.78 × 10⁻⁴), including its `PlanckMass / eV` check.
    2. `NEWTON_G_MEV_MINUS_2 = 6.70884e-45`: ruling 3's decimal comparison fails.
       `test_golden_history` fails too.

    For each, the log names every test that fails and how, and says whether ruling 4's tests also
    failed under breakage 2 (a 1.5 × 10⁻⁶ change in G moves them by about 7.5 × 10⁻⁷, so they may
    or may not).

11. **The records.**
    - **This board:**
      - row 05, with K1–K3 and the gate "Units";
      - `### 5.6 After prompt 05 (<date>)`, in §5.5's form, with §0's tables;
      - §1.1's items for 05 marked as landed.
    - **`.documents/OPEN_ISSUES.md`** changes only if the work opens an issue. None is expected:
      the one-ulp difference and the fixed-T coincidence are observations (rulings 3 and 14).
    - **Not edited:** `prompts/INDEX.md`, and the README.

12. **The stop conditions.**
    - **A unit system cannot meet ruling 4's 10⁻⁶.** This is the prompt's own.
    - **The agent's §2 item 2 tables differ from §0's in any digit shown.** This replaces the
      prompt's "above 10⁻²".
    - **A test of ruling 13's table still fails once the table is applied**, or one of its new
      values differs from the agent's own measurement on the new tree.
    - **An existing test fails for a reason this note does not name.** Never edit such a test to
      make it pass.

13. **The twelve old-unit pins are re-pinned (the user's ruling).** Each reference becomes its
    value on the new units, to the digits it had, with tolerances unchanged:

    | file | literal (lines at `cfd2e11`) | old | new |
    |---|---|---|---|
    | `test_kinematic_cap_loop.py` | first bounce N, P1 at M = 0.5 (`:232`, `:283`) | `20.343028` | `20.343018` |
    | | first bounce φ (`:233`, `:284`) | `4.57371e-3` | `4.57375e-3` |
    | | φ(21), P1 at M = 0.5 (`:234`, `:285`, `:330`) | `1.2203834e-1` | `1.2202350e-1` |
    | | π(21), P1 at M = 0.5 (`:235`) | `-1.2971527e-1` | `-1.2979552e-1` |
    | | first bounce φ, M = 0.01 (`:244`) | `9.1505e-5` | `9.1505e-5` (unchanged) |
    | | φ(21), M = 0.01 (`:245`) | `1.185154e-1` | `1.185015e-1` |
    | | first bounce φ, M = 0.001 (`:254`) | `9.1505e-6` | `9.1506e-6` |
    | | φ(21), M = 0.001 (`:255`) | `1.184501e-1` | `1.184362e-1` |
    | | φ(21), reflected (`:265`) | `1.184428e-1` | `1.184289e-1` |
    | | φ(37.5), P3 (`:297`) | `5.8078e-4` | `5.8100e-4` |
    | | P3 minima 1, 2, 8 (`:303`) | `2.79886e-4`, `3.03650e-4`, `3.66368e-4` | `2.79895e-4`, `3.03661e-4`, `3.66383e-4` |
    | | P2 wall bounces (`:312`) | `19` | `20` |
    | | φ(40), P2 (`:313`) | `1.909693e-2` | `1.909720e-2` |
    | `test_integrator_exceptions.py` | `P1_M0p5_PHI_21` | `0.12203833994225839` | `0.12202349644648677` |
    | | `P1_M0p5_PI_21` | `-0.12971527073457434` | `-0.1297955195199084` |
    | | `P1_M0p5_FIRST_BOUNCE_N` | `20.343034651924082` | `20.343024418098206` |
    | | `P1_M0p5_FIRST_BOUNCE_PHI` | `0.004573794758933947` | `0.004573834433278705` |
    | `test_first_bounce.py` | first bounce N, φ (`:53`, `:54`) | `20.343028`, `4.57371e-3` | `20.343018`, `4.57375e-3` |

    - **How the new values were taken.** The short literals are the value they were rounded from,
      re-measured on the probe tree and rounded to the same number of significant figures. The
      first-bounce pair `20.343018`, `4.57375e-3` is the converged run's (`cap_fraction = 0.02`),
      as `20.343028`, `4.57371e-3` were (20.343028460751025, 4.573708520186028e-3 on the old
      units). The four `P1_M0p5_*` constants are `repr()` of the floats, as before. The minima
      at `:303` and `9.1505e-5` passed already. They are re-pinned so that every reference in
      the module is of one tree. `9.1505e-5` rounds to itself.
    - **The surfing test** keeps its comparison with the committed CSV row, which is a dated
      record and is not rewritten. Its two `assertEqual`s on `bounce_T_GeV` and `bounce_N` become
      `assertLessEqual(abs(x / float(row[…]) - 1.0), 1e-7)`. Every other assertion of that test
      is unchanged. Its docstring and the module docstring's bullet say "within 10⁻⁷ relative of",
      and say why: the scan was taken on the units before 05, and the shift is 2.1 × 10⁻⁸.
    - **The prose.**
      - `test_kinematic_cap_loop.py`'s module docstring keeps its account of the audit's values
        and gains a paragraph: the references were re-measured by jordan-normalization prompt 05,
        when the electronvolt was set from PRyMordial's G_N, on the same probe states, and the
        audit's values are in 05's log.
      - The four RHS counts in docstrings become the new tree's: `2120` → `2117` (`:239`),
        `2275` → `2262` (`:249`), `38 547` → `38 761` (`:292`) and `18 880` → `19 030` (`:307`).
        The `1945` of `:226` is unchanged.
      - `test_integrator_exceptions.py`'s module docstring and the comment above the constants say
        that the constants are 05's tree's, re-measured from prompt 01's (`fc97233`) after the
        units changed. `test_c`'s docstring says "reproduces prompt 05's tree".
    - **Nothing else in these files changes.** The probe states, the tolerances, the RHS bounds,
      the exact counts 1945 and 224, and the number 51 of P3's bounces all stay. A test that still
      fails after this table is applied is a stop.
    - The log gives this table with both columns, and the full-precision value behind each short
      literal. It records P2's 20th bounce as §0 accounts for it.
    - These four files are added to the prompt's §5.

    Classify as `STRUCTURALLY REQUIRED`, citing the user's ruling.

14. **Observations, not acted on** (log only, no issue):
    - PRyMordial's `GN` expression is one ulp below the literal (ruling 3);
    - the golden setup's T_stop equals `FIXED_T_JORDAN_HIGH_MEV`, so whether its 1 MeV fixed-T
      values exist is decided by rounding at the stop (§0);
    - the stored `M_value` literal 2.436e24 eV in two test fixtures is now 1.000278 × 10⁻³ M_P
      (ruling 5).

**No driver runs.** `main.py` is not changed and is not run. No Ray is started, and no store is
opened except the tests' temporary ones.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Order.**
  1. The five suites and `test_golden_history` alone (with its time), before any edit.
  2. The worktree of `HEAD` and the "old" dump.
  3. K1 and K2, then the "new" dump and ruling 7's tables. Stop here if they differ from §0.
  4. K3's tests, ruling 13's re-pins, then the golden JSON regenerated.
  5. The golden history alone, then the suites, the breakages and the grep.
  6. The records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its two
  halves. Send output to a file in the agent's scratch directory, grep the verdict from it, and
  record `uptime` beside each. A failure is re-run alone before it is called real.
  `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall-clock bound and fails
  under load; re-run it alone before calling it real.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `5fc02a6` and `cfd2e11`, before this note was committed:
1. **The tree.** `jordan-normalization`, with `git status --short` empty. The code changes since
   `58c1c93` are those listed under "Gate", and `cfd2e11`.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: three runs, all finished
   (`ray-sqla-align` 02's: two `done`, one `killed`, the SIGTERM it measured). None is `running` or
   stale.
4. **The probes of §0**, in the session scratchpad, with the worktree removed afterwards. The five
   suites gave the baselines above.
5. **The user's rulings**, by multiple choice on 2026-10-26: "Re-pin in 05" (ruling 13), and
   "Housekeeping commit now" for the merge's broken test (`cfd2e11`).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README, this board, and 04's log;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/` or prompts 06–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/05-the-planck-mass.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **The files it may add or change** are the prompt's §5, with these changes:
  - added: `ComputeTargets/tests/test_kinematic_cap_loop.py`,
    `ComputeTargets/tests/test_integrator_exceptions.py`, `ComputeTargets/tests/test_first_bounce.py`
    and `tools/tests/test_plot_surfing_histories.py` (ruling 13);
  - added: `ComputeTargets/tests/golden_history.py`, `ComputeTargets/tests/test_golden_history.py`
    and `ComputeTargets/tests/data/golden_history_05.json` (ruling 8);
  - removed: the three tests of the prompt's "Read first" item 5 (ruling 5).

  Nothing is deleted.
- **§0's fourteen rulings** bind it, each classified as §0 says.
- **It starts no Ray, opens no store but a temporary one, and runs no driver.**
- **Stop and ask** on any of ruling 12's conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the golden test's time before and after;
  - ruling 7's two tables, and whether they equal §0's;
  - ruling 13's table as applied, with the full-precision values it measured;
  - the constant's value and hex, and ruling 4's measured values per system;
  - every test added, with what it asserts;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the grep's output;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists only §2's files. `git status` is clean, and
   `git worktree list` shows only the main checkout.
2. **The expressions.** Read the diff of `Units/` against rulings 1 and 2: the four expressions, no
   literal changed, and the comments. `REDUCED_PLANCK_MASS_EV.hex()` is `0x1.f79cf52984fafp+90`.
3. **The PRyMordial test.** Read it against ruling 3. As a probe, change the scratch copy's
   `PRyM_init.py` text (never the repository's) and check that the parser fails as it should; or,
   failing that, check by reading that the path, the regex and the `Decimal` product are right.
4. **The consistency tests.** Rerun §0's four measurements per system from the orchestrator's own
   probe, at `<agent>`'s `Units/`, and compare them with the log.
5. **The golden history.** Run the generator's `build_record()` at `<agent>` into the scratchpad,
   and compare every pinned item with `golden_history_05.json`. `golden_history_01.json` and
   `_03.json` are byte-identical to `<agent>~1`'s. `test_golden_history` passes alone, with its time.
6. **The change in values.** Rerun §0's comparison at `<agent>` against `<agent>~1`, from the
   orchestrator's own probe, and check that it equals §0's tables and the log's.
7. **The tests left alone, and the re-pins.** `test_inventory_report.py`, `test_compare_stores.py`
   and `test_store_fingerprint_golden.py` are unchanged and pass, and the grep gives ruling 6's
   hits only. The diff of the four files of ruling 13 changes only the table's literals, the
   surfing test's two assertions and the prose that ruling 13 names. The orchestrator's own
   probe, rerun at `<agent>`, gives the table's new values.
8. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
   says, revert, and check that `git status` is clean.
9. **The prose.** The sentence in `CLAUDE.md`, and the note in `architecture-summary.md` §8.1.
   Nothing else in either file changes.
10. **The records and the suites.** The board (row, items, gate, §5.6) and the log in §5.1's form,
    with every deviation classified. Then the five suites at `<agent>`, serially, at the counts the
    log states, and `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 05* paragraph in §5.6. Fix small
  residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log);
  - add this note to the board's list of orchestrator notes, marked "used for 05";
  - update the status line: 5 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the three "pinned strings" carry stored literals, not the constant, and are not edited
      (ruling 5);
    - the comparison with PRyMordial is decimal, since its expression is one ulp off the literal
      (ruling 3);
    - the 10⁻² stop is replaced by a digit-for-digit match with §0, since three fields at β = 5.5
      exceed 10⁻² by amplification at the wall (rulings 7 and 12);
    - the golden fixed-T values flip with rounding at a stop that coincides with 1 MeV (§0);
    - the twelve old-unit pins of the integrator and surfing tests re-pinned, at the user's word,
      with P2's wall bounces 19 → 20 (ruling 13);
  - the golden test's time before and after;
  - the five suites' counts.
- **Then 06.** Its note is written after this review.
