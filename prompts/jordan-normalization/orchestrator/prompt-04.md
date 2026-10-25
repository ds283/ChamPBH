# Orchestrator — prompt 04, the model interface

Read [`../README.md`](../README.md) first: §0.2, §0.3 items 1 and 3, §2.2 (04's charter), §4, §5,
§6.0 R2, R6, R7 and R14, §6.2 P5, P10 and P13. Read 03's log too, in particular "State handed to the
next prompt". The conventions of [`prompt-03.md`](prompt-03.md) bind this prompt as well. They are
restated below where 04 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-the-model-interface.md`](../04-the-model-interface.md)
**Board items:** M1–M7 · **Gate:** "Model" · **Closes:** one, `[00-region-properties-on-the-potentials-become-unread]`
(`integrator-remediation`) · **Opens:** only what the work finds · **Model:** Opus, as the prompt
recommends · **Network:** none.

**Gate:**
- 03 landed (`bd9ae42`) and reviewed (`d619579`); the gate "Axis" holds (board §2).
- Since then the branch has taken, by merge from `main`, the housekeeping fix `807f175`
  (`extract_common.py`, one line: e-fold plot labels) and `rpk-v030-adoption` (`95ad91d`: the
  `raypoolkit` pin moves to `v0.3.0`, the cluster script uses its rehearsal lock, and
  `tools/tests/test_ray_cluster_lock.py` is new). `git diff --stat bd9ae42 HEAD -- . ':!prompts' ':!.documents' ':!science-runs'`
  lists only `CLAUDE.md`, `extract_common.py`, `requirements.txt` and that test. None of them is in
  04's path, but **tools' count is now 93, not 84**.
- `HEAD` is the commit that adds this note, on `jordan-normalization`, with `git status` clean.
- The prompt's own gate: no tracked change, the five suites at the baselines below, and no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 05 is not dispatched until 04 is reviewed.

## 0. What makes this prompt unusual

**It threads a new object through the integrator and every consumer, under a bitwise regression,
with the store left as it is.** The orchestrator measured four things at `b58f6a5` before writing
this note, with probes in the session scratchpad (not committed):

- **Eq. 7 at the golden histories' stored samples.** 3 M_P² H_J²/ρ_R,J against
  A²(1 + βφ′/M_P)²/(1 − φ′²/6M_P²) (1 + f_m + V/ρ_R,E). A and V come from the potential and coupling
  rows, f_m from `log_fm` and ρ_R,E from `log_rhorad_Einstein`. The largest relative difference is
  **4.2 × 10⁻¹⁴** at β = 1.5 (sample 598, 1 + βφ′ = 0.903) and **6.9 × 10⁻¹⁴** at β = 5.5 (sample 74).
  Relative difference means |a − b|/max(|a|, |b|), and 0 where both are 0.
- **The shift law.** At the stored states (every seventh sample), H_J is formed by `HubblePolicy`
  and ln ρ_R,J by `log_Omega`, as `fixed_T_value_at` forms them. This is done with a coupling that
  subtracts βδ/M_P and with the plain one. 3 M_P² H_J²/ρ_R,J (that is, 1 + r + f_m) for the shifted
  coupling, against the plain one times e^{−2βδ/M_P}, differs by at most **3.1 × 10⁻¹⁴** relative, for
  δ = −0.01, +0.03 and +0.1 M_P, in both histories.
- **Σ(T_CMB) is exactly 0.0.** `1.0 - 3.0 * cosmology.w(T_CMB)` on `QCD_Cosmology(Planck2018)` in
  `Planck_units` returns 0.0, so the brief's Σ ρ_R,E term vanishes identically at T_CMB (ruling 5).
- **The analytic seeds.** `analytic_seed.py`'s `analytic()`, copied into the scratchpad at
  `b58f6a5`'s units and cosmology, gives these. The probe itself cannot be imported here, because it
  imports `ComputeTargets.ScalarModel`.

  | β | M/M_P | static: Λ (meV), φ/M_P | tracking: Λ (meV), φ/M_P |
  |---|---|---|---|
  | 1.5 | 1e-3 | 2.233837387, 3.841967203e-2 (1 iteration) | 2.110131124, 3.433537145e-2 (14) |
  | 5.5 | 1e-3 | 2.220578784, 2.006401935e-2 | 2.006102933, 1.646485104e-2 (20) |
  | 25 | 1e-3 | 2.189477510, 9.410859255e-3 | 1.805148667, 6.547353474e-3 (33) |
  | 3 | 0.01 | 2.183933111, 8.590899833e-2 | 1.772675025, 5.819128236e-2 (37) |
  | 3 | 0.1 | 2.050746635, 2.716681062e-1 | `ValueError: math domain error` |
  | 1.5 | 0.5 | 1.943956227, 8.590899833e-1 | `ValueError: math domain error` |
  | −1.5 | 1e-3 | `ValueError` | `ValueError` |
  | 0 | 1e-3 | `ZeroDivisionError` | `ZeroDivisionError` |

  **Where the tracking seed has no solution, the static one has one.** So at M = 0.5 the seed is
  `static`, not `plain` as the prompt's M7 says (ruling 6). `plain` arises only for β ≤ 0.
- **The golden histories** took 0.6 s (β = 1.5) and 9.4 s (β = 5.5) at a load of about 7.

**Baselines** at `b58f6a5`, serially, at load 7–43: CosmologyModels **18**, ComputeTargets
**61 + 127**, ObjectFactories **86**, tools **93**, RunDrivers **61**, all passing. These are 03's
counts, with tools +9 from `rpk-v030-adoption`'s new test module. Other workloads share the
machine.

**Sixteen statements in the prompt are loose, silent or wrong for this tree. They are ruled here.**

1. **The method names (M2).** M2 allows either the integrator's names or new ones with the old as
   aliases. Take the integrator's names, and only them:
   - `RealizedModel` exposes `log_V`, `d_logV_dphi`, `d2_logV_dphi2`, `log_Omega`, `Omega`,
     `d_logOmega_dphi`, `d2_logOmega_dphi2`, `log_V_floor`, `reflects_at_origin`, `name`,
     `normalization`, `phi_ref` and `model`. There is no `log_A`, `A` or `d_logA_dphi`. The class
     docstring says that Ω is the conformal factor A.
   - A model implements hooks that take no φ_ref:
     - `_log_V(normalization, phi)`, `_d_logV_dphi(normalization, phi)` and
       `_d2_logV_dphi2(normalization, phi)`;
     - `_log_Omega(phi)`, `_d_logOmega_dphi(phi)` and `_d2_logOmega_dphi2(phi)`;
     - `_log_V_floor(normalization)`, and a `reflects_at_origin` property.
   - `RealizedModel.log_Omega(phi)` is `model._log_Omega(phi) - self._log_Omega_ref`, where
     `_log_Omega_ref = model._log_Omega(phi_ref)` is computed once, at realization. `Omega(phi)` is
     `exp(self.log_Omega(phi))`, as `ExponentialCoupling.Omega` is. Every other method delegates with
     nothing added.
   - **Why.** The stubs in `test_surfing_turning_points.py` (`StubCoupling`, `HalfCoupling`) and
     `test_adiabatic_mass.py`'s coupling are duck-typed on these names. Keeping the names also keeps
     the integrator's expressions unchanged.
   - `RealizedModel` is immutable. Its attributes are set in `__init__` and read through properties
     with no setter.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

2. **Bitwise, expression by expression (M3).**
   - `ExponentialModel`'s three V hooks are the bodies of `ExponentialPotential.log_V`,
     `d_logV_dphi` and `d2_logV_dphi2`, copied, with `self._log_Lambda_4` replaced by
     `normalization`. That includes `GetFieldValue`, `pow(self._M_float / phi_float, self._n)`, the
     derivatives' division by `phi` (not `phi_float`), and the `OverflowError`/`ValueError`
     handling with its messages. The messages may name the model in place of the potential.
   - The A hook is `self._beta_float * phi_float / self._Mp`. `_d_logOmega_dphi` is
     `self._beta_float / self._Mp`, and `_d2_logOmega_dphi2` is `0.0`.
   - `_M_float = float(M)`, `_beta_float = float(beta)` and `_Mp = units.PlanckMass`.
   - `_log_V_floor(normalization)` is `normalization`.
   - At φ_ref = 0.0, `_log_Omega_ref` is `beta * 0.0 / Mp`, which is ±0.0, and x − (±0.0) = x
     bitwise.
   - **The adapter's normalization** is `4.0 * log(float(potential._Lambda))`, the expression of
     `ExponentialPotential._log_Lambda_4`, and its φ_ref is `0.0`.

   Do not simplify any of these. Classify as `STRUCTURALLY REQUIRED`.

3. **The model's identity (M1, M3).**
   - **The constructor** is `ExponentialModel(store_id, M: M_value, n: int, beta: beta_value, units)`.
     It asserts n ≥ 0, as the potential does.
   - **`type_id`** is `EXPONENTIAL_MODEL = 1`, in a new `CosmologyConcepts/Models/model_ids.py` in
     the form of the potentials' and couplings'. 07 stores it.
   - **`shard_key`** is the `beta_value`, as `ExponentialCoupling`'s is. 07's S1 relies on this.
   - **`name`** is `ExponentialModel(M=…,n=…,beta=…)`. M uses `energy_formatter` as the potential's
     name does, and β uses `:.5g`.
   - **`RealizedModel.name`** begins with the model's `name`, followed by the normalization and
     φ_ref/M_P (each `:.6g`).
   - **The adapter** is the classmethod `ExponentialModel.from_rows(potential, coupling) -> RealizedModel`,
     with store id 0. It takes M and n from the potential row (`_M`, `_n`), β from the coupling
     (`_beta`), and the units from the potential. It raises `TypeError` unless the rows are an
     `ExponentialPotential` and an `ExponentialCoupling`. Its docstring says that it is 07's to
     remove.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

4. **`effective_minimum` has no closed form (M1, M3).**
   - With the factor A(φ)/A(φ₀) the equation is (M/φ²) V₀ e^{M/φ} = (β/M_P) ρ_m,E,0 e^{β(φ−φ₀)/M_P}.
     That is transcendental in both φ and 1/φ. Lambert W solves it only if A(φ)/A(φ₀) is dropped,
     and that would be a different equation.
   - So `ExponentialModel` does not override the base class.
   - The base class's method is
     `effective_minimum(normalization, phi_ref, phi_0, rho_m_E_0, bracket=None) -> float`. It is
     the root of `exp(rm.log_V(x)) * rm.d_logV_dphi(x) + rm.d_logOmega_dphi(x) * rho_m_E_0 * exp(rm.log_Omega(x) - rm.log_Omega(phi_0))`
     on `rm = self.realize(normalization, phi_ref)`.
   - The default bracket is (0.05 φ₀, 20 φ₀). The solver is `brentq` with `xtol = 1e-14 * phi_0` and
     `rtol = 1e-13`, as `phimin_iter_v2.one` uses.
   - A bracket without a sign change raises `ValueError` naming the bracket. It does not return
     `nan`. 06 decides what to do with it.

   **The prompt's test** "closed and generic agree to 10⁻¹²" is replaced, as follows:
   - the generic root agrees to 10⁻¹² relative with a reference that the test computes itself, by
     bisection to the last representable halving, at three states (β = 1.5, 5.5 and 25 at
     M = 10⁻³, with φ₀ and ρ_m,E,0 of the README §0.3 kind, chosen in the test and written down);
   - the root is a minimum: V_eff′ is negative at root × (1 − 10⁻⁶) and positive at
     root × (1 + 10⁻⁶).

   Classify as `STRUCTURALLY REQUIRED`.

5. **The Σ ρ_R,E term (M1).**
   - The prompt asks for it to be measured "at T_CMB on the golden histories' final states". The
     golden histories end at 1 MeV, so that cannot be done as written.
   - Σ(T_CMB) = `1.0 - 3.0 * cosmology.w(T_CMB)` is 0.0 exactly (§0), so the term vanishes
     identically. It is left out.
   - The log records that evaluation, with its expression. No history is run to T_CMB for it.

   Classify as `STRUCTURALLY REQUIRED`.

6. **The seed (M1, M3, P5).**
   - **`Seed`** is a `namedtuple("Seed", ["kind", "normalization", "phi_ref"])`. `kind` is one of
     `"tracking"`, `"static"` and `"plain"`.
   - **The base class** gives `plain`: normalization `log(3.0 * Mp**2 * H0**2 * (1.0 - cosmology.omega_m))`
     with φ_ref `0.0`. That is the probes' start. On Planck2018 it equals `log(cosmology.rho_cc)`
     bitwise; use the former. `Mp` is `cosmology.units.PlanckMass` and `H0` is `cosmology.H0`.
   - **`ExponentialModel.seed`** is `analytic()`'s loop, transcribed:
     - the starting φ;
     - 200 iterations at most, with the 10⁻¹⁴ test;
     - V's expression, with `cosmology.rho_m0` and `cosmology.rho_r0`;
     - normalization = ln(V e^{−M/φ}), which is ln Λ⁴ in the probe's terms. It may be computed as
       `log(V) - M_float / phi`.

     First tracking, then static, then the base class's `plain`. A kind has **no solution** in any
     of these cases:
     - the loop raises `ValueError` or `ZeroDivisionError`;
     - it does not converge within 200 iterations;
     - its final V, G or 1 + βφ′/M_P is not positive.
   - **n ≠ 1.** The analytic seeds assume n = 1 (V′ = −V M/φ²), so for n ≠ 1 the model returns
     `plain`.
   - **The tests**, with the expected values written as `float.hex` literals. Generate them from a
     copy of `analytic()` in the agent's scratch directory, at the tree's units and cosmology, and
     name that in a comment. `analytic_seed.py` cannot be imported here, since it imports
     `ComputeTargets.ScalarModel`.
     - β = 1.5, M = 10⁻³: `tracking`;
     - β = 1.5, M = 0.5: **`static`** (the tracking loop raises);
     - β = −1.5, M = 10⁻³: `plain`, with both analytic kinds raising.

     Compare e^{normalization} with the probe's Λ⁴, and φ_ref with its φ, each to 10⁻¹² relative.
     For `plain`, compare exactly. §0's table holds the values to cross-check.

   Classify as `STRUCTURALLY REQUIRED` (the prompt's M = 0.5 case was wrong).

7. **`renormalize` and the coordinate (M1).**
   - `renormalize(normalization, phi, V_target)` is
     `normalization + log(V_target) - self._log_V(normalization, phi)` for the exponential model. It
     raises `ValueError` for V_target ≤ 0, since 06's guard deals with that before calling it.
   - The coordinate pair is `normalization_coordinate(normalization) -> float` and
     `normalization_from_coordinate(x) -> normalization`. For the exponential model both are the
     identity.
   - **Tests:**
     - V(φ) after `renormalize` equals V_target to 10⁻¹² relative, at three φ and two V_target;
     - the coordinate round-trips exactly.

   Classify the name `normalization_from_coordinate` as `IMPLEMENTATION CHOICE`, citing this note.

8. **The consumers' signatures (M4).** One positional argument replaces the two.
   - `PotentialDerivativePolicy(task_label, cosmology, model)`. **The plain-V paths are removed**:
     `RealizedModel` has no `V`, so they have no user. The log path's statements and messages stay
     verbatim, and so does the class name.
   - `ODEPolicy(task_label, cosmology, model)`, with `self.model` in place of `self.potential` and
     `self.coupling`. `_reflection_guards` reads `policy.model`; its messages may say "model" for
     "potential" and keep the phrase `reflects_at_origin`.
   - `HubblePolicy(model, units)`.
   - `fixed_T_value_at(result, k, N, policy, model, units)` and
     `fixed_T_values(result, policy, model, units)`.
   - `surfing_turning_points(result, model, interior_samples=8)`.
   - `compute_scalar_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model, task_label=…, atol=…, rtol=…, verbose=…)`.
   - `AdiabaticComputePolicy(task_label, cosmology, model)`. `compute_adiabatic_values` takes the
     model from its history's `realized_model`.
   - **`BBNData` is not changed.** `compute_BBN_data` reads only stored samples, `T_Jordan_stop` and
     the cosmology. Nothing in it computes with A or V. The log says so.
   - `first_bounce` takes no model.
   - Commented-out lines that name `policy.potential` (`ODERHS`) are not edited.

   Classify as `STRUCTURALLY REQUIRED`, except removing the plain-V paths, which is
   `IMPLEMENTATION CHOICE` and is the prompt's own option.

9. **`pipeline_selection` is not changed.**
   - `super_planckian_couplings` and `warn_super_planckian` take the coupling rows, which still
     exist. `main.py` calls them once per coupling, before any history and without a potential, so
     there is no realized model to hand them. A model per (potential, coupling) pair would change
     the number of warnings.
   - Their only use of A is `log_Omega(φ*)`, which at φ_ref = 0 is what the adapter's model gives.
   - 08's D2 moves the warning onto the model at the seed's φ_ref.
   - `test_initial_field_option.py` is not edited.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

10. **The adapter's place (M5).**
    - **`ScalarHistory.realized_model`** is a read-only property. On first access it is built by
      `ExponentialModel.from_rows(self._potential, self._coupling)` and cached. It is **not** built
      in `__init__`: `ObjectFactories/tests/test_first_bounce_round_trip.py` and
      `ObjectFactories/tests/test_scalarhistory_failure_reason.py` construct histories on stand-in
      rows (`test_version_keyed_lookups.py:71-72`) that have no `_M` or `_Lambda`. Those tests are
      not edited.
    - `compute()` passes `self.realized_model`, and its default task label is
      `self.realized_model.name`.
    - `shard_key`, `potential`, `coupling` and the `check_required_parameter` calls stay.
    - **Not changed:**
      - `main.py` builds its histories through the factory and passes no model. Its job-name
        lambdas keep `potential.name` and `coupling.name`.
      - `AdiabaticHistory` and `BBNData` keep `_potential`, `_coupling` and their default labels.
      - `plot_by_beta.py`, `plot_ScalarHistory.py` (its `PE()` reads the potential row's `log_V`,
        equal at φ_ref = 0) and `extract_common.py`.

      07 moves all of these. A file in the prompt's §5 that needs no change is not touched, and the
      log says which ones.

    Classify as `STRUCTURALLY REQUIRED` (the laziness) and `IMPLEMENTATION CHOICE` (what is left).

11. **The tools.** `tools/history_and_bbn.py` and `tools/surfing_scan.py` build their rows as today.
    They pass `ExponentialModel.from_rows(potential, coupling)` to `compute_scalar_history`, and
    surfing_scan passes it to `surfing_turning_points` too.
    - `tools/plot_surfing_histories.py` builds no model of its own (it imports from `surfing_scan`)
      and is not touched.
    - `history_and_bbn.py`'s `_StandInModel` keeps `potential` and `coupling`, since
      `compute_BBN_data` reads neither.
    - Their outputs do not change, and their tests pass unedited.

    Classify as `STRUCTURALLY REQUIRED`.

12. **M6, and the issue.**
    - **Delete** `bounce_region_level{1,2}_boundary`, `bounce_region_level{1,2}_max_step` and
      `hard_reflection_point` from `AbstractPotential` and `ExponentialPotential`, and
      `default_max_step` from `ExponentialPotential`. Remove an import only if it falls unused.
    - **The witness** is
      `git grep -n -P 'bounce_region|hard_reflection_point|default_max_step' -- . ':!prompts' ':!science-runs' ':!.documents'`,
      which must find nothing.
    - `.documents/integrator-audit-2026-09-30/harness.py` reads the properties. It is a dated
      record and is not edited; the log's "Observations not acted on" lists it.
    - `default_abs_tol` and `default_rel_tol` on `ExponentialPotential` are also unread. The prompt
      does not name them, so they go to "Observations not acted on", with no issue.
    - **Close** `[00-region-properties-on-the-potentials-become-unread]` on
      `prompts/integrator-remediation/IMPLEMENTATION_STATE.md`. Move it from §3 to §4 with a dated
      **Resolved** line naming this prompt and "this commit".

    Classify as `STRUCTURALLY REQUIRED`.

13. **The tests (M7).**
    - **New module:** `ComputeTargets/tests/test_model_interface.py`, which sorts into `[g-z]`.
      - **(a) The unit-level bitwise witness.** At φ_ref = 0, through the adapter, `assertEqual` for
        each of the eight methods against the potential and coupling rows, at a dozen φ from
        10⁻³ M to 10 M_P. It is cheaper than the golden history and fails first. 07, which deletes
        the rows, removes it, and its docstring says so.
      - **(b) A(φ_ref) = 1 exactly.** `log_Omega(phi_ref) == 0.0` and `Omega(phi_ref) == 1.0` by
        `assertEqual`, for φ_ref ∈ {0, 0.01, 0.0349624, 0.3, 5} M_P. The test runs on
        `ExponentialModel` and on a test-only `_MinimalModel(AbstractModel)` that implements only the
        hooks: constant V and ln Ω = γφ, with no handling of φ_ref. A model that forgets φ_ref thus
        fails.
      - **(c) The shift law.** On the golden β = 1.5 history, run once in `setUpClass` (about
        0.6 s, said in the docstring), at every sample, for δ = −0.01, +0.03 and +0.1 M_P:
        - call `fixed_T_value_at` itself, with a one-step stub result whose interpolant returns the
          sample's state;
        - form 1 + r + f_m from its r and the state's f_m, with the model realized at φ_ref = δ;
        - require it to equal the φ_ref = 0 value times e^{−2βδ/M_P} to **10⁻¹²** relative.

        The measured maximum is 3 × 10⁻¹⁴ (§0).
      - **(d) Eq. 7** at every stored sample of both golden histories, run once in `setUpClass`
        (about 10 s, said in the docstring), with A, V and β/M_P from the realized model, to
        **10⁻¹²** relative. The measured maxima are 4.2 × 10⁻¹⁴ and 6.9 × 10⁻¹⁴ (§0). Bounces are
        included.
      - **(e)** `renormalize` and the coordinate, as in ruling 7.
      - **(f)** `effective_minimum`, as in ruling 4.
      - **(g)** `seed`, as in ruling 6.
      - **(h)** A `pickle` round trip of a `RealizedModel`. The loaded model gives equal values,
        since Ray sends it to a worker.
    - **The golden history** passes unchanged through the adapter.
      `ComputeTargets/tests/golden_history.py`'s `run_history` passes
      `ExponentialModel.from_rows(potential, coupling)`; its docstring says so. **No golden JSON is
      written.** `test_golden_history` is run alone, before and after, and the log gives both
      times. A slowdown of more than 25 % is reported, not a stop.
    - **Existing tests changed**, each listed in the log with its reason. No assertion changes
      except G1's name literal.
      - `golden_history.py`: as above.
      - `test_fixed_T_values.py`: the calls and `policy.model`.
      - `test_kinematic_cap_loop.py`:
        - `build` realizes the model through the adapter;
        - `NonReflectingExponentialPotential` becomes `NonReflectingExponentialModel(ExponentialModel)`,
          overriding `reflects_at_origin`;
        - `potential_class` becomes `model_class`;
        - G1's `"ExponentialPotential("` becomes `"ExponentialModel("`.
      - `test_integrator_exceptions.py`: the two policy constructions use `rhs.policy.model`.
      - `test_adiabatic_mass.py`: one stand-in realized model, a small class in the test that
        delegates to its two stand-ins.
      - `test_sampling_axis.py`: the identity test's ln A comes from the realized model. Its
        histories come through `golden_history.run_history`, so it has no call of its own to
        change.
      - `ComputeTargets/tests/test_scalarhistory_failure_reason.py`: the call.
    - **Not changed:** `test_surfing_turning_points.py` (ruling 1), `test_initial_field_option.py`
      (ruling 9), and the ObjectFactories tests (ruling 10).

    Classify as `STRUCTURALLY REQUIRED`, except (a) and (h), which are `IMPLEMENTATION CHOICE`
    citing this note.

14. **The breakages.** Each is recorded as a diff that `git apply` replays from the agent's commit.
    1. The adapter's normalization becomes `log(exp(4.0 * log(float(potential._Lambda))))`. The
       golden history fails, and the log shows its first differing field. Test (a) fails too.
    2. The base class's subtraction is removed: `log_Omega` returns `model._log_Omega(phi)`. Test
       (b) fails for both models, and (c) fails.
    3. The sign is reversed: `model._log_Omega(phi) + self._log_Omega_ref`. Tests (b) and (c) fail.
       The golden history still passes, since φ_ref = 0; the log says so.

    For each, the log names the tests that fail and how.

15. **The records.**
    - **This board.**
      - Row 04, M1–M7 and the gate "Model".
      - §3.1's row marked closed, as 03's are.
      - An entry in §4.
      - `### 5.5 After prompt 04 (<date>)`.
    - **The `integrator-remediation` board**, as in ruling 12.
    - **`.documents/OPEN_ISSUES.md`.**
      - Delete the row from §1.15, and in §1.15's preamble add "and prompt 04 one (<date>)".
      - The count goes **25 → 24**.
      - In the header, `integrator-remediation`'s "8 on" becomes "7 on", with a clause saying that
        `jordan-normalization` prompt 04 closed one, with the date.
      - The `jordan-normalization` clause adds "prompt 04 closed one of them, leaving three".
      - Set **Last updated**.
    - **`.documents/architecture-summary.md`.**
      - Add `Models/` to the `CosmologyConcepts/` directory tree (`:84-90`). The factories' tree
        (`:48-49`) is 07's.
      - Add a section after §8.4 on `AbstractModel`, `RealizedModel` and `ExponentialModel`.
      - Add a dated note in §8.3 and §8.4 saying that the integrator now reads the realized
        model, the rows reach it through the adapter until 07, and the region properties are
        gone.

      Edit only what describes the tree.
    - **Not edited:** `numerical-strategies.md` (10's), `CLAUDE.md` and `prompts/INDEX.md`.

16. **No driver runs.** `main.py` is not changed (ruling 10) and is not run.
    `PYTHONPATH=. ./venv/bin/python main.py --help` (exit 0) is the only invocation allowed, to show
    that it still imports.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Order.**
  1. The five suites and `test_golden_history` alone (with its time), before any edit.
  2. The interface (M1–M3), with test module (a)–(h).
  3. The consumers (M4, M5), the tools, the existing tests and M6.
  4. The golden history alone, then the suites, the breakages and `main.py --help`.
  5. The records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its two
  halves. Send output to a file in the agent's scratch directory, grep the verdict from it, and
  record `uptime` beside each. A failure is re-run alone before it is called real.
- **An existing test that fails for a reason this note does not name is a stop**, never a reason to
  edit it.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened except the tests' temporary ones. No Ray is started.
  Nothing is written to `var/`.
- **Stop conditions are real.** These are the prompt's §4, with ruling 5 replacing the Σ
  measurement. A stop goes to the user through the orchestrator, and the agent resumes with
  `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `b58f6a5`, before this note was committed:
1. **The tree.** `jordan-normalization`, with `git status --short` empty. The code changes since
   `bd9ae42` are those listed under "Gate".
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: no runs.
4. **The probes of §0**, in the session scratchpad. The five suites gave the baselines above.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README, this board, and 03's log;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/` or prompts 05–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/04-the-model-interface.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change** are the prompt's §5, with these additions:
  - `CosmologyConcepts/Models/model_ids.py` (ruling 3);
  - the tests of ruling 13;
  - `prompts/integrator-remediation/IMPLEMENTATION_STATE.md`;
  - `.documents/OPEN_ISSUES.md`.

  These are removed from it: `pipeline_selection.py` (ruling 9), and `main.py`, `plot_by_beta.py`,
  `plot_ScalarHistory.py` and `extract_common.py` (ruling 10). Nothing is deleted except the
  properties of ruling 12.
- **§0's sixteen rulings** bind it, each classified as §0 says.
- **No golden JSON is written.**
- **It starts no Ray, opens no store but a temporary one, and runs no driver but ruling 16's.**
- **Stop and ask** on any of the prompt's §4 conditions, or on an existing test failing for a
  reason §0 does not name.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the golden test's time before and after;
  - the measured maxima of tests (c) and (d), and the seeds' kinds and values;
  - the Σ(T_CMB) evaluation;
  - every test added or changed, with what it asserts;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the M6 witness's output;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists only §2's files. `git status` is clean, and
   `git worktree list` shows only the main checkout.
2. **The golden history.** Run `test_golden_history` alone at `<agent>`, with its time.
   `golden_history_03.json` is byte-identical to `<agent>~1`'s. As a probe, run the generator at
   `<agent>` into the scratchpad and compare every pinned item with the JSON.
3. **The expressions.** Read `ExponentialModel`'s hooks against `<agent>~1`'s
   `ExponentialPotential` and `ExponentialCoupling` line by line (ruling 2), and the adapter's
   normalization. Test (a) exists and asserts with `assertEqual`.
4. **The base class owns φ_ref.** `RealizedModel.log_Omega` is the only place φ_ref enters. No
   model hook takes φ_ref. Test (b) runs on `_MinimalModel`.
5. **The identities.** Rerun §0's two measurements on `<agent>`'s code, through the realized model.
   The maxima agree with §0 to a few digits, and with the log's.
6. **The seeds and the minimum.** Recompute §0's seed table at `<agent>`'s
   `ExponentialModel.seed` for the three test points and two more (β = 3, M = 0.1 → `static`;
   n = 2 → `plain`). Check that `effective_minimum` agrees with the orchestrator's own bisection at
   one state.
7. **The consumers.** `git grep -n -P '\b(potential|coupling)\b' -- ComputeTargets/ScalarHistory.py ComputeTargets/AdiabaticHistory.py ComputeTargets/Policies tools/history_and_bbn.py tools/surfing_scan.py`,
   read against rulings 8, 10 and 11. No computation reads a row; the remaining hits are the
   adapter, labels, shard keys and the rows kept on the objects. `pipeline_selection.py`,
   `main.py` and the plot scripts are unchanged. `main.py --help` exits 0.
8. **M6.** The witness finds nothing. The issue is in `integrator-remediation`'s §4 with its
   **Resolved** line, and the index row is gone.
9. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
   says, revert, and check that `git status` is clean.
10. **The records and the suites.** The two boards, the index (24 open, the clauses, the date),
    `architecture-summary.md`, and the log in §5.1's form with every deviation classified. Then the
    five suites at `<agent>`, serially, at the counts the log states, and `black --check` on every
    changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 04* paragraph in §5.5. Fix small
  residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log, `integrator-remediation`'s board, the
    index);
  - add this note to the board's list of orchestrator notes, marked "used for 04";
  - update the status line: 4 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the integrator's method names kept, with no `log_A` (ruling 1);
    - no closed-form minimum, and a bisection reference in its place (ruling 4);
    - the Σ term identically zero at T_CMB (ruling 5);
    - `static`, not `plain`, at M = 0.5 (ruling 6);
    - `pipeline_selection` left for 08 (ruling 9);
    - the lazy adapter, with `main.py` and the plot scripts unchanged (ruling 10);
  - the golden test's time before and after;
  - the five suites' counts.
- **Then 05.** Its note is written after this review.
