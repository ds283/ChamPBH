# Prompt 04 — the model interface

**Campaign:** [`README.md`](README.md) · **Board items:** **M1**–**M7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R6, one model object holding V and A,
one table per model type; R7, a normalization the model interprets; R14, n in the key; U1, the
names). Its orchestrator note, written after 03's review, governs where it differs from this file.
**Gate:**
- 03 landed and reviewed, the gate "Axis" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** `[00-region-properties-on-the-potentials-become-unread]` (`integrator-remediation`
board). **Opens:** only what the work finds.
**Recommended model:** **Opus**. A new interface threaded through the integrator and every consumer,
under a bitwise regression, with the store left as it is.

**Read first:**

1. [`README.md`](README.md): §0.1, §0.2 (the sites of A), §0.3 items 1 and 3 (the fixed point and
   the analytic minimum), §2.2 (04's charter), §4, §5, §6.0 R2, R6, R7, R14, §6.2 P5, P10.
2. The brief, `source/brief_jordan_normalization.md`, §2.1–§2.3 and §3 R1, R7; and
   `source/planning-probes-2026-10-20/analytic_seed.py` (`analytic`, the static and tracking seeds)
   and `phimin_iter_v2.py` (`one`: φ_min of a history's own state).
3. `CosmologyConcepts/Potentials/AbstractPotential.py`, `ExponentialPotential.py`;
   `CosmologyConcepts/ConformalCouplings/AbstractCoupling.py`, `ExponentialCoupling.py`;
   `ComputeTargets/Policies/PotentialDerivativePolicy.py`.
4. Every consumer: in `ComputeTargets/ScalarHistory.py`, `ODEPolicy`, `HubblePolicy`, the floor and
   reflection (`log_V_floor`, `reflects_at_origin`), `fixed_T_value_at`, `first_bounce`,
   `surfing_turning_points`, `compute_scalar_history`, the `ScalarHistory` class (`potential`,
   `coupling`, `compute`); `ComputeTargets/AdiabaticHistory.py`; `ComputeTargets/BBNData.py`;
   `pipeline_selection.py` (`super_planckian_couplings`, `warn_super_planckian`); the tools that
   build a history (`tools/history_and_bbn.py`, `tools/surfing_scan.py`,
   `tools/plot_surfing_histories.py`); the tests that build potentials or couplings
   (`test_adiabatic_mass.py` defines its own coupling).

---

## 1. What is wanted

The integrator and everything downstream of it see one object, a realized model, that gives V and A
and their derivatives with A(φ_ref) = 1. The model that realizes it holds the Lagrangian's
parameters and interprets the potential's normalization itself. The store does not change yet: an
adapter builds the model from today's potential and coupling rows, with φ_ref = 0 and V₀ = Λ⁴, and
the golden history passes **bitwise**.

### 1.1 The items

- **M1 — `AbstractModel`** in `CosmologyConcepts/Models/AbstractModel.py` (U1). A `DatastoreObject`
  with `name`, `type_id`, `shard_key`, and:
  - `realize(normalization, phi_ref) -> RealizedModel`;
  - `renormalize(normalization, phi, V_target) -> normalization`: the frozen-state step, the
    normalization at which V(φ) = V_target, however the model interprets it (R7);
  - `normalization_coordinate(normalization) -> float` and its inverse: the scalar 06's secant acts
    on;
  - `effective_minimum(normalization, phi_ref, phi_0, rho_m_E_0, bracket) -> float`: the root of
    V′(φ) + (d ln A/dφ) ρ_m,E(φ₀) A(φ)/A(φ₀), ρ_m,E ∝ A at fixed Einstein-frame number density (the
    brief's §3 R2; R2). A generic bracketed `brentq` in the base class; a model may override it.
    Measure the Σ ρ_R,E term the brief asks about at T_CMB on the golden histories' final states,
    and leave it out if its effect on φ_min is below 10⁻⁸ relative; say what it was;
  - `seed(cosmology) -> Seed` (P5): a named tuple of the kind (`tracking`, `static`, `plain`), the
    normalization and φ_ref. The base class provides `plain` only;
  - **A(φ_ref) = 1 is the base class's**, not the model's: `RealizedModel.log_A(φ)` is
    `model._log_A(φ) − model._log_A(φ_ref)`, so no model interprets φ_ref.
- **M2 — `RealizedModel`**, immutable, in the same package: `log_V`, `d_logV_dphi`,
  `d2_logV_dphi2`, `log_A`, `A`, `d_logA_dphi`, `d2_logA_dphi2`, `log_V_floor`, `reflects_at_origin`,
  `name`, and the `normalization`, `phi_ref` and `model` it came from. Keep the method names the
  integrator already calls (`log_Omega`, `Omega`, `d_logOmega_dphi`, `d2_logOmega_dphi2`) as the
  names, or as the only aliases, so that the integrator's arithmetic is the same expressions. Its
  `name` replaces the `"{potential.name}-{coupling.name}"` labels.
- **M3 — `ExponentialModel(M, n, beta)`** (R6, R14): V = V₀ exp((M/φ)ⁿ), A = exp(β(φ − φ_ref)/M_P).
  - **The normalization is ln V₀** (multiplicative in V, R7); its coordinate is itself. `renormalize`
    is ln V₀ + ln V_target − ln V(φ).
  - **Bitwise with today**: log V must be computed as `ExponentialPotential.log_V` computes it,
    `log_V0 + (M/phi)**n`, with the adapter passing `log_V0 = 4.0 * log(Lambda)` exactly as
    `_log_Lambda_4` is built; and log A as `beta * phi / Mp − beta * phi_ref / Mp`, which at
    φ_ref = 0 subtracts `0.0`. Do not simplify either expression.
  - `effective_minimum` in closed form for n = 1 where it exists, checked against the generic one;
    `seed` gives `tracking` (φ′ = 1.5 φ in Eq. 7 at A = 1, Planck2018's densities), else `static`,
    else `plain` (V₀ = ρ_Λ, φ_ref = 0), as `analytic_seed.py` computes them.
  - `log_V_floor` and `reflects_at_origin` as `ExponentialPotential` has them.
  - It is **not stored** in this prompt (07 adds its table). The adapter gives it a dummy
    `store_id` (0, as the tests' objects use) and keeps the rows' ids for labels only.
- **M4 — the consumers.** `compute_scalar_history`, `ODEPolicy`, `HubblePolicy`,
  `PotentialDerivativePolicy` (or its replacement), `fixed_T_value_at`, `fixed_T_values`,
  `first_bounce`, `surfing_turning_points`, `AdiabaticHistory`, `BBNData` and `pipeline_selection`
  take a `RealizedModel` where they took a potential and a coupling. With only the exponential
  model left, the plain-V fallback of `PotentialDerivativePolicy` has no user; remove it only if the
  golden history stays bitwise, else keep it and say why.
- **M5 — the adapter.** `ScalarHistory` (the object) keeps its `potential` and `coupling` rows and
  gains `realized_model`, built by `ExponentialModel.from_rows(potential, coupling)` with
  `normalization = 4 log Λ` and φ_ref = 0. `main.py`, the plot scripts and the tools pass that. It is
  marked in its docstring as 07's to remove.
- **M6 — what is not carried over.** `bounce_region_level{1,2}_boundary`, `…_max_step`,
  `default_max_step` and `hard_reflection_point` are not on the new interface; delete them from the
  potential classes once nothing reads them (`grep` as the witness), which closes
  `[00-region-properties-on-the-potentials-become-unread]`.
- **M7 — the tests** (P10, P13), under `ComputeTargets/tests/`:
  - the golden history **bitwise**, through the adapter (the brief's first stop condition);
  - **the shift law**: at a fixed Einstein-frame state, realizing with φ_ref = δ multiplies
    1 + r + f_m (Eq. 7, as `fixed_T_value_at` forms it) by e^{−2βδ/M_P}, to rounding, for three δ;
  - **Eq. 7 holds** at the golden histories' stored samples: 3 M_P² H_J²/ρ_R,J equals
    A²(1 + βφ′/M_P)²/(1 − φ′²/6M_P²) (1 + f_m + V/ρ_R,E) to rounding;
  - A(φ_ref) = 1 exactly for several φ_ref;
  - `renormalize` makes V(φ) = V_target to rounding; `effective_minimum` (closed and generic) agree
    to 10⁻¹² relative, and the root is a minimum;
  - `seed` reproduces `analytic_seed.py`'s two seeds at β = 1.5, M = 10⁻³ to 10⁻¹² relative, and
    returns `plain` where the tracking seed has no solution (M = 0.5).
- **The prose this commit makes false:** `.documents/architecture-summary.md` (the potential and
  coupling interfaces); not yet `numerical-strategies.md` (10).

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 03 handed on; the golden history passes.
2. **After:** the golden history **unchanged and passing**; the five suites, each change of count
   explained; `black --check`; `grep` for `bounce_region`, `hard_reflection_point`,
   `default_max_step` outside the records: nothing.
3. **Deliberate breakages**, each failing a named test:
   - `log_V0 = log(exp(4 log Λ))` in the adapter: the golden history fails (show the first field);
   - φ_ref subtracted in the model's `_log_A` instead of the base class's: the A(φ_ref) = 1 test or
     the shift law fails for a subclass that forgets it (write the test so);
   - the shift law's sign reversed.

## 3. Acceptance

1. M1–M7 as §1.1 says.
2. §2's checks in the log, with the Σ ρ_R,E measurement of M1.
3. **One commit**, with the log `logs/04-the-model-interface.md`. The board: row 04, M1–M7, the gate
   "Model", the issue closed on `integrator-remediation`'s board with its index row deleted and the
   count corrected, `### 5.5 After prompt 04 (<date>)`.

## 4. Stop conditions: stop and ask

- The golden history is not bitwise through the adapter, and the cause is not an expression you can
  restore (the brief's §5, first condition).
- A's normalization enters somewhere not listed in README §0.2 with consequences beyond relabelling.
- A consumer needs the potential or the coupling separately in a way the realized model cannot
  serve.

## 5. What this prompt does not do

- It changes no table, no option, no unit and no stored value. It does not tune anything (06).
- **Files it changes:** `CosmologyConcepts/Models/` (new); the potential and coupling classes (M6
  only); `ComputeTargets/ScalarHistory.py`, `AdiabaticHistory.py`, `BBNData.py`,
  `Policies/PotentialDerivativePolicy.py`; `pipeline_selection.py`; `main.py`, `plot_by_beta.py`,
  `plot_ScalarHistory.py`, `extract_common.py` and the tools of "Read first" item 4, where they pass
  the model; the tests; `.documents/architecture-summary.md`; the boards and the index; the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
