# Prompt 06 — the tuning layer

**Campaign:** [`README.md`](README.md) · **Board items:** **T1**–**T7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R1, H_J(T_CMB) = H0 at fixed ω_m;
R2, φ_ref = φ_min(T_CMB); R8, two layers; R10, a secant; R13, no seeding from other histories;
P4–P6, P12). Its orchestrator note, written after 05's review, governs where it differs from this
file.
**Gate:**
- 05 landed and reviewed, the gate "Units" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. A root-find wrapped round a stiff integrator, whose failure modes
were found in planning and must be handled, not rediscovered.

**Read first:**

1. [`README.md`](README.md): §0.3 items 1–3 in full (the fixed point, the V_target artefact, the
   rate of the plain update, the dependence on φ*, the seeds), §2.2 (06's charter), §4, §5 (the
   named exception for this prompt's comparison), §6.0 R1, R2, R8, R10, R13, §6.2 P4–P6, P12, P13.
2. `source/planning-probes-2026-10-20/`: `README.md`, `phimin_iter_v2.py` (the plain update, with
   the V_target ≤ 0 guard), `analytic_seed.py` (the seeds), and the logs.
3. The brief's §2.2 (Eq. 7) and §3 R2.
4. 04's `AbstractModel`, `ExponentialModel` and `RealizedModel`, and 04's log.
5. `ComputeTargets/ScalarHistory.py`: `compute_scalar_history` (its arguments, its failure payload
   `_failure_payload`, what it returns), `summarise_failure_reasons` in `pipeline_selection.py`, and
   the `ScalarHistory` class's `compute`.
6. `rrk-adoption` README P4 and its log 03 on worker output and the heartbeat.

---

## 1. What is wanted

Two functions in the compute layer, not yet called by `main.py` (07 calls them):
- `integrate_history`, the primitive: one realized model, integrated to any T_stop, with no
  normalization logic (R8). Today's `compute_scalar_history` body.
- `tune_history`: integrates to T_CMB, reads today's state, and adjusts (normalization, φ_ref) until
  H_J(T_CMB) = H0 and φ_ref = φ_min(T_CMB), with a secant step (R10).

### 1.1 The items

- **T1 — `integrate_history`.** `compute_scalar_history`'s body moved into a function taking a
  `RealizedModel`, T_init, T_stop, φ*, π*, the z grid and the tolerances, returning today's payload.
  `compute_scalar_history` (the `@ray.remote`) becomes a thin wrapper over `tune_history` in 07;
  in this prompt it still calls the primitive, so that `main.py` is unchanged. The golden history
  passes **bitwise** through `integrate_history`.
- **T2 — today's state and the residuals.** From the final sample (T_J = T_CMB): φ₀, φ₀′, A(φ₀),
  ρ_R,J, f_m, V(φ₀), H_J. The residuals are r_H = ln(H_J/H0) and r_φ = φ_ref − φ_min, with φ_min
  from `model.effective_minimum` at the history's own ρ_m,E(φ₀) = A(φ₀)⁴ f_m ρ_R,J (R2). Converged
  when |H_J/H0 − 1| < tol and |φ_ref/φ_min − 1| < tol, tol = 10⁻⁵ by default (P4).
- **T3 — the step** (R10, P4).
  - The **plain map** G(x), x = (`normalization_coordinate`, φ_ref): `renormalize` to Eq. 7's
    V_target at today's state, and φ_ref ← φ_min. It is the probes' update.
  - The **secant**: Broyden's ("good") method on the fixed-point residual R(x) = G(x) − x, with the
    initial inverse Jacobian chosen so that the first step is the plain step, and the components
    scaled (the coordinate in its own units; φ_ref relative to φ_min). Each step costs one
    integration.
  - **Guards.** A step whose V_target ≤ 0 (the frozen-state equation has no positive V) moves
    φ_ref only, halves V₀ through `renormalize`, and resets the Broyden matrix (§0.3 item 1: at
    M ≥ 0.1 the first state has A(φ₀) of 1.6–2.3). A secant step whose residual norm is more than
    twice the last is replaced by the plain step and the matrix reset. Record in the payload how
    many steps of each kind were taken.
  - **Cap**: 30 integrations by default.
- **T4 — the seed** (P5, R13). `model.seed(cosmology)`: `tracking`, else `static`, else `plain`.
  Deterministic in the history's own inputs; no value from any other history, and no cache.
- **T5 — failures**, each a distinct first clause, so that `summarise_failure_reasons` counts them
  apart:
  - `"the normalization did not converge: …"` after the cap, with the last residuals;
  - `"no normalization reaches H0: …"` when V_target ≤ 0 four steps running;
  - an integration's own failure, unchanged, with the iteration it happened at.
  Failed tunings return `_failure_payload`, as today.
- **T6 — progress** (P12). One line per integration, printed by the task:
  `>> tune <label> it=<k> kind=<seed|plain|secant> H/H0-1=<…> phi_ref/phi_min-1=<…> norm=<…> phi_ref=<…> wall=<s>`.
  Check it against `RunDrivers/registered_run.py`'s `StageTracker` patterns: it must not be taken for
  a RayWorkPool banner or progress line. Say what the tracker does with it.
- **T7 — what `tune_history` returns** (P6): the final integration's payload and samples, and
  `normalization`, `V_ref` = V(φ_ref), `phi_ref`, `phi_0`, `pi_0`, `tracking` = φ₀′/(1.5 φ₀),
  `residual_H`, `residual_phi`, `tuning_iterations` (the number of integrations), the step counts by kind, and `seed_kind`,
  `seed_normalization`, `seed_phi_ref`. 07 stores exactly this.
- **The tests** (P13), `ComputeTargets/tests/test_tuning*.py`, each tuned test under a minute with
  its cost in its docstring:
  1. a converged history at a cheap point (β = 3, M = 0.01, φ* = 5, about 30 s in planning): both
     residuals inside tol, A(φ_ref) = 1, and `V_ref` = V(φ_ref) as realized;
  2. determinism: the same inputs twice give bitwise the same T7 output;
  3. the cap at 2: the non-convergence reason; a model whose `renormalize` cannot reach H0 (a test
     subclass): the H0 reason;
  4. **a tuned golden history** (P3), `golden_history_06_tuned.json`, at that point, pinned as 01's
     is; 07 and 08 pass it bitwise.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 05 handed on; the golden history passes.
2. **Secant against plain** (README §5's named exception; about an hour of integrations in all,
   run detached in parallel lanes from a script in your scratch space, its logs copied into
   `prompts/jordan-normalization/logs/06-probes/`). On README §0.3 item 1's thirteen points, with
   the plain update (the secant disabled) and with the secant:
   - integrations to converge, the converged normalization and φ_ref, and the residuals;
   - the two methods' converged values agree to the tolerance;
   - the comparison with the planning probes' values, which differ by 05's units (about 2.8 × 10⁻⁴
     in Λ) and should agree otherwise; say how well.
   **The secant is accepted if it never needs more integrations than the plain update.** Otherwise
   stop and ask, with the table.
3. **After:** the five suites; `black --check`.
4. **Deliberate breakages**, each failing a named test: the convergence test on r_H alone; the seed
   read from a module-level cache; the guard's matrix reset removed (show which point then fails or
   slows, from item 2's script on one point).

## 3. Acceptance

1. T1–T7 and the tests as §1.1 says; `main.py` unchanged in behaviour.
2. §2's table in the log, in full.
3. **One commit**, with the log `logs/06-the-tuning-layer.md` and `logs/06-probes/`. The board:
   row 06, T1–T7, the gate "Tuning", `### 5.7 After prompt 06 (<date>)`.

## 4. Stop conditions: stop and ask

- A point of §0.3's grid does not converge with either method, or V_target ≤ 0 persists there (the
  brief's §5).
- The secant needs more integrations than the plain update anywhere (§2 item 2).
- The two methods converge to values that differ by more than the tolerance (two fixed points?).
- The golden history is not bitwise through `integrate_history`.

## 5. What this prompt does not do

- It changes no table, no option and no store; `main.py` still computes untuned histories.
- **Files it changes:** `ComputeTargets/ScalarHistory.py`; `CosmologyConcepts/Models/` only if
  04's interface needs a correction (say so; a change of meaning is a stop);
  `pipeline_selection.py` only if a failure clause needs it; the tests and the tuned golden data;
  the board; the log and `logs/06-probes/`.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
