# Numerical methods and solution strategies in the ChamPBH pipeline

This document is an inventory of the numerical methods, discretization choices, and
numerical-hygiene strategies used in the ChamPBH pipeline, written to support the drafting
of a "numerical methods" section of a science publication. It focuses on the physics
integration layer (`ComputeTargets`, `CosmologyModels`, and the supporting `Quadrature`
supervisors), and on the interface to the third-party PRyMordial BBN solver.

The scientific problem being solved is the cosmological evolution of a chameleon scalar
field (a screened dark-energy/modified-gravity model) through the radiation-dominated
epoch, from a high initial temperature (~20 TeV in the examples) down to the present CMB
temperature, followed by the derivation of its consequences for Big Bang Nucleosynthesis
(BBN). The system is evaluated over grids of potential parameters (M, Λ) and coupling
strength (β), so every choice below is made with an eye to robustness across a wide
parameter survey rather than being hand-tuned to a single trajectory.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7).** The system is evaluated over
> grids of (M, n, β), not of (M, Λ) and β: the model is one `ExponentialModel` per (M, n, β)
> (`main.py:1082`), and the normalization of the potential, which Λ was, is found for each history by
> tuning (§3.7.4). Every figure in the sections above this one was made before that campaign, with the
> fixed normalization and φ_ref = 0, and is left as the record of its tree.

---

## 1. The governing ODE system and the choice of state vector

### 1.1 Independent variable

The scalar-field background is integrated as an initial-value problem in the number of
e-folds `N = log(1+z)`, integrated *forward* in `N` from `N = 0` (present day, `z = 0`) —
except that the physical initial condition is actually specified at *high* temperature, so
the code integrates forward in `N` starting from `N_start = 0` and works with `N`
increasing, then afterwards re-maps `N` back to redshift via `z = exp(N) - 1`. A failsafe
upper bound of `N_failsafe = 1000` e-folds is imposed to guarantee termination even if the
physical stopping condition is somehow never met.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.9).** The re-map to a redshift is gone.
> A sample's label is ln(1 + z_E) itself, a point of the sampling axis, which is anchored at the end of
> the history: the sampling reads N_backward from the axis (`ComputeTargets/ScalarHistory.py:1296-1297`),
> so no `exp(N) − 1` is formed, and `z` is only the derived property `redshift.z` (`inf` past 709.78
> e-folds). The integration still runs forward in N, and every stored history ends at T_J = T_CMB.

### 1.2 State vector (`Quadrature/supervisors/ScalarField.py`, `StateVector`)

The integrated state is a 5-component vector, deliberately chosen so that quantities
spanning many orders of magnitude are evolved in logarithmic form:

- `phi_Einstein` — Einstein-frame scalar field value φ
- `pi_Einstein` — its e-fold derivative π = dφ/dN
- `log_rhorad_Einstein` — **log** of the Einstein-frame radiation energy density
- `log_fm` — **log** of the matter fraction f_m = ρ_m / ρ_rad
- `log_T_Jordan` — **log** of the Jordan-frame radiation temperature

A `namedtuple` is used for the state vector (and for all the intermediate data bundles:
`ODEPolicyData`, `HubblePolicyData`, `SampleValues`, `ModelFunctions`). This is an explicit
numerical-hygiene decision: it makes it structurally impossible to transpose components of
the state vector or the RHS when packing/unpacking arrays passed to/from SciPy.

### 1.3 Logarithmic evolution as a dynamic-range strategy

The use of `log ρ_rad`, `log f_m`, and `log T` (rather than the raw quantities) is the
central dynamic-range-management strategy for the ODE itself. The radiation density falls
by tens of orders of magnitude between the initial temperature (~20 TeV) and the CMB, so
integrating `ρ_rad` directly would be numerically hopeless; the log form keeps the evolved
variables O(1)–O(100) throughout. The RHS returns `d(log ρ)/dN`, `d(log f_m)/dN`,
`d(log T)/dN` as smooth O(1) quantities. Values are exponentiated back to physical form
only transiently inside the RHS, and each such exponentiation is guarded (see §6).

### 1.4 Two-frame (Einstein/Jordan) bookkeeping

The model is formulated in the Einstein frame but observables (temperature, BBN) live in
the Jordan frame, so both frames are tracked simultaneously. The Jordan-frame temperature is
one of the evolved state variables (it is what the physical stopping condition is written
against), while the Einstein-frame Hubble rate is reconstructed from the constraint and the
Jordan-frame Hubble rate is obtained from it by the conformal transformation
`H_J = H_E (1 + (dlogΩ/dφ) π) / Ω` (`HubblePolicy`). H_Jordan is allowed to be negative and
so is *not* stored logarithmically, unlike H_Einstein.

---

## 2. ODE integration: stepper choice, stiffness, tolerances

### 2.1 Solver and the fallback cascade

Integration is performed with `scipy.integrate.solve_ivp`. The primary stepper is **Radau**
(implicit Runge–Kutta, 5th order, L-stable) — chosen because the system is **stiff**: the
chameleon has a very heavy effective mass near the "brick wall" of the potential, and there
are widely separated timescales (fast field oscillations/bounces vs. slow cosmological
drift). An implicit, stiffly-stable method is essential.

A robustness cascade is built in (`compute_scalar_history`): the solver list is
`["Radau", "BDF", "LSODA", "DOP853"]`. If integration fails with a `ComputationFailureError`
under the current solver, the code falls back to the next solver in the list and retries the
*entire* integration. The first three are stiff solvers (Radau, BDF = backward
differentiation formulas, LSODA = automatic stiff/non-stiff switching); DOP853 (explicit
Dormand–Prince 8(5,3)) is the last-ditch non-stiff attempt. Only if all four fail is the
model marked as a total integration failure (`{"failure": True}`), which is recorded rather
than aborting the survey. The successful solver's identity is recorded in the datastore
alongside the result (`solver_label`).

Note: although `solve_ivp` is called with `method="Radau"` hard-coded in the inner loop, the
cascade machinery and solver labels are in place; the effective production stepper is Radau.

### 2.2 Tolerances

Absolute and relative tolerances are passed straight to `solve_ivp` as `atol`/`rtol`.
Defaults (`config/defaults.py`) are `DEFAULT_ABS_TOLERANCE = 1e-8` and
`DEFAULT_REL_TOLERANCE = 1e-8`. Tolerances are treated as first-class, *persisted* metadata
(the `tolerance` concept is stored in the database), so every result carries an explicit
record of the numerical accuracy at which it was produced.

Potentials can advertise parameter-dependent tolerance overrides. For example
`ReclinerPotential.default_abs_tol`/`default_rel_tol` loosen tolerances to `1e-5`/`1e-6`
when the mass scale `M/M_P < 1e-3`, recognizing that very small M makes the potential
extremely steep and demands a trade-off between achievable accuracy and step viability.

> **Note, 2026-10-20 (`jordan-normalization`, prompt 01).** `ReclinerPotential` was removed with the
> other three non-exponential potentials (user ruling R11), so the paragraph above describes a
> class that no longer exists. The exponential potential's overrides are the only ones left.

> **Note, 2026-10-21 (`jordan-normalization`, prompt 02).** `ScalarModel` was renamed `ScalarHistory`
> (and `compute_scalar_model` `compute_scalar_history`, `ScalarModelProxy` `ScalarHistoryProxy`,
> `ComputeTargets/ScalarModel.py` `ComputeTargets/ScalarHistory.py`, the tables likewise). Nothing
> computed changed. The sections headed "Added" and the notes above and below keep the names of the
> day they were written, so a path or a name in them is the old one.

### 2.3 Dense output and resampling

`solve_ivp` is called with `dense_output=True`. The continuous interpolant (`sol.sol`) is
retained for every integration fragment and is what is subsequently evaluated on the
user-requested redshift grid — i.e. the ODE is solved once at solver-chosen internal steps
and then *resampled* onto the output grid via the solver's own high-order dense interpolant,
rather than forcing the solver to hit the output points. This decouples solver step control
from output sampling.

---

## 3. Adaptive step-size control and event-driven region switching

Beyond SciPy's internal adaptive error-controlled stepping, ChamPBH layers a **problem-aware
adaptive maximum-step-size** scheme on top, driven by event detection. This is the most
distinctive numerical feature of the field integration.

### 3.1 Event functions

`solve_ivp` is driven with a set of terminal event functions (`events=(...)`), each carrying
`.terminal = True` and a `.direction` sign so that only zero-crossings in the correct
direction fire:

- `terminate_at_T_stop` — fires when `log T_Jordan` crosses `log T_stop` (usually T_CMB)
  from above; this is the physical end of the integration (`direction = -1`).
- `reflection_failure_detector` — fires if φ crosses the potential's
  `hard_reflection_point` (a "brick wall" near the origin), signalling a reflection.
- `enter/exit_bounce_region_level1` and `enter/exit_bounce_region_level2` — fire when φ
  crosses the potential's nested `bounce_region_level{1,2}_boundary` values.

Events that are not currently relevant are swapped for a `dummy_event_handler` that never
fires (e.g. once inside level 1, the "enter level 1" detector is replaced by the dummy and
the "exit level 1" detector is armed). This state-machine toggling avoids spurious
re-triggering.

### 3.2 Fragmented integration and step-size clamping

The integration proceeds as a sequence of **solution fragments**. Each `solve_ivp` call runs
until one event fires; the code then inspects which event fired, adjusts the maximum step
size, updates the state, and restarts a fresh `solve_ivp` from the event point. Fragments
are accumulated in a list (`SolutionFragment(N_low, N_high, sol)`), each holding its own
dense interpolant over its N-subinterval.

The adaptive strategy for the maximum step (`max_step` passed to `solve_ivp`):

- **Outside bounce regions:** `max_step` is set to the potential's `default_max_step` (for
  the Recliner potential, `1e-2` e-folds; generally unbounded/`inf` unless the potential
  restricts it). This lets the stepper stride freely through the slow cosmological drift.
- **Inside a "level 1" bounce region:** `max_step` is clamped to
  `bounce_region_level1_max_step` (e.g. `1e-5`, or `1e-6` for very small M) so that a
  chameleon bounce off the steep wall is temporally resolved.
- **Inside a deeper "level 2" region:** clamped tighter still (`1e-6`).

The exactly-one-event invariant is enforced defensively: the code checks that the sum of all
event-trigger counts across the fragment is exactly 1, raising if multiple or zero events
fired.

### 3.3 Hard reflection handling

When the `reflection_failure_detector` fires (the field has run into the brick wall), the
integration is restarted with the sign of the field velocity `pi_Einstein` reversed (an
elastic reflection), rather than trying to resolve the near-singular turnaround
dynamically. The number of hard reflections is recorded.

### 3.4 Fragment-count failsafes

Because each bounce generates fragments, two guards prevent runaway: a warning is printed
every 20 fragments, and a hard `RuntimeError` failsafe trips at 100 fragments. This caps
pathological trajectories that would otherwise spin forever near a wall.

### 3.5 Added 2026-10-01 (`integrator-remediation`): the step loop that replaced §2.1 and §3.1–§3.4

**What this supersedes.** §2.1 (the fallback cascade), the note in §2.2 on the potentials'
tolerance overrides, and §3.1–§3.4 (events, fragments and step-size clamping, the hard
reflection, the fragment failsafes) above describe `compute_scalar_model` as it was at tree
`b1f64d8` and before `VERSION_LABEL = "2026.5.0"`. They were correct for that tree and are left
as they stand. The code no longer does any of it. The audit that found the mismatches is
[`integrator-audit-2026-09-30/README.md`](integrator-audit-2026-09-30/README.md) ("audit" below;
section numbers are its); the code landed in `fc97233` (prompt 01) and `614c41a` (prompt 02) of
the `integrator-remediation` campaign. Every figure below is the audit's on `b1f64d8` or the
campaign's logs (`prompts/integrator-remediation/logs/01-…`, `02-…`) on the tree named there.

#### 3.5.1 One Radau instance in a pure loop

`integrate_scalar_history(RHS, supervisor, initial_state, N_start, log_T_stop,
params=StepControl(), N_failsafe=1000.0, policy=None, task_label=None, N_stop=None) ->
IntegrationResult` (`ComputeTargets/ScalarModel.py`) is a function of its arguments only: no
Ray, no datastore. It drives one `scipy.integrate.Radau` instance step by step (`solve_ivp` is
itself a thin loop over `solver.step()`, so nothing is lost, audit §9.2). There are no event
functions, no `SolutionFragment` and no restarts at region crossings. The solver is rebuilt only
at an elastic reflection (§3.5.3). Each accepted step's `solver.dense_output()` is kept, and the
history is one `scipy.integrate.OdeSolution(ts, interpolants)`, which the z-grid sampling
evaluates directly (§2.3 and the sampling itself are otherwise unchanged).
`compute_scalar_model` builds the initial state as before and calls the function inside the
supervisor's `with`.

The parameters are one namedtuple, `StepControl(cap_fraction=0.1, cap_floor=1e-11,
global_max_step=0.1, jacobian_factor_max=1e-4, atol=1e-8, rtol=1e-8, step_budget=2_000_000)`.
`IntegrationResult` carries `solution`, `N_final`, `final_state`, `nfev` (every RHS call,
Jacobian probes included), `accepted_steps`, `steps_rejected_by_exception`, `reflections` (a list
of `Reflection(N, phi_Einstein, pi_Einstein_in)`) and `max_wall_to_kinetic_ratio`.

Termination is at the first accepted step with `ln T_J < log_T_stop`; the crossing is located
by `brentq` on that step's interpolant, as `solve_ivp`'s event code did, and the solution is
truncated there. Reaching `N_failsafe = 1000` without the crossing is a failure of the history.

#### 3.5.2 The kinematic step cap (replaces §3.1 and the max-step regions of §3.2)

The nested regions and their per-region `max_step` are gone. Before every step the maximum step
is set from the field's own motion (audit §9.1). The wall is purely repulsive, so the only
inward force on `φ` is the conformal kick plus friction, and the inward displacement in a step
`h` from `(φ, π, π̇)` is bounded by `max(−π, 0) h + ½ max(−π̇, 0) h²`. Requiring this to be at
most `f φ` gives

    h ≤ f φ / |π|           if π < 0,
    h ≤ sqrt(2 f φ / |π̇|)   if π̇ < 0,

with `f = 0.1`, on top of a global cap of `0.1` e-folds, and never below the floor of §3.5.3.
`π̇` is `solver.f[1]`, which Radau already holds. The cap is written to `solver.max_step`, and
`solver.h_abs` is clipped into it. Both terms are needed: the velocity term alone has a hole at
an outer turning point (`π ≥ 0`, field falling from rest), found at β = 3, M = 0.01,
`N = 34.60`, where a step of `2.9e-2` carried `φ` from `3.7e-4` through the wall to `−8.7e-6`
(audit §9.1).

The bound enforced is that the field cannot move inward by more than a fraction `f` of its
distance to the origin in one step, whatever `M` is. The cost of an approach to the wall is
`≈ 10 ln(φ_start/φ_wall)` steps, independent of `M`: 1 945, 2 120 and 2 275 RHS for the first
reflection from the P1 state at `M = 0.5, 0.01, 0.001` (audit §3.6; the shipped scheme spent
17 092, 26 634, 26 422, `p1_sweep.py a` and `p1_smallM.py`). On the P2 parked window (β = 2,
M = 0.5, `N` 25 → 40) it is 18 880 RHS against 2 099 582, with `φ(40) = 1.909693e-2` in both;
on the P3 grazing window (β = 1.2, M = 0.01, `N` 32.9 → 37.5) 38 547 against 5 188 284, with the
same 51 bounces (audit §3.2, §3.3; campaign log 01). The nine full histories of audit §9.3 all
complete, in 24 193 – 327 046 RHS and 2 – 26 s each (log 01 §6.1 (d)); the shipped scheme could not
finish at least four of them.

#### 3.5.3 The floor and the elastic reflection (replaces §3.3)

Steps below about `10 · EPS · N ≈ 1e-13` e-folds are not representable at `N ~ 20–55`, and the
resolved cap needs `h ≈ 2e-3 M` at the wall, so the loop can resolve a reflection only for
`M ≳ 1e-8` (audit §3.7). Below that the bounce is modelled. The rule, before each step:

    if π < 0 and f φ / |π| < h_floor (= 1e-11):  π ← −π, restart the solver at the same N.

When the field decelerates inside a resolvable wall `|π| → 0` and the cap grows, so the floor is
never reached. When the wall is thinner than a representable step the field reaches
`φ_stop = |π| h_floor / f` (about `5e-11` at delivery speed) at full speed and is reflected.
The neglected flight lasts under `2 h_floor / f = 2e-10` e-folds, below the `1e-8` tolerance, and
the bounce is elastic because `½π² + V/(3H²M_P²)` is conserved by the wall force alone. Measured
from the P1 state (`p_smallM_scan.py kinref`; log 01): `φ(N = 21) = 1.184428e-1` for every `M`
from `1e-6` to `4.1e-28`, resolved at `1e-6` and `1e-8`, reflected at `φ = 4.70e-11` from
`3e-9` down. The reflection and the resolved bounce agree to seven digits where both exist
(audit §3.7). The shipped reflection at `φ = 0` agreed only for `M ≲ 1e-13` and failed or ran on
at `φ < 0` above that (audit §3.5); it is gone.

> **Note, 2026-11-01 (`inelastic-reflection`, prompt 12; §6.11).** At α > 0 the floor rule drains: the restart is π ← −√(1 − α_eff) π (§6.2), so the reflection there is not elastic; §6.11.1 gives the order in which a history meets it.

The reflection is a *model*, not a fallback, and it has two guards (log 01):

- **G1.** The potential must declare `reflects_at_origin` (new on `AbstractPotential`, default
  `False`; `True` on `ExponentialPotential` only). Otherwise reaching the floor is a
  `ComputationFailureError` naming the potential. `log_V_floor` (the potential's value far from
  the wall, `Λ⁴` for the exponential) is declared beside it.
- **G2.** At the moment of reflection the wall part of the potential fraction must not exceed the
  kinetic fraction, `W = 3 (V − V_floor)/(3H²M_P²) ≤ ½π²`, else the step has already passed the
  wall and the history fails ("reflection requested inside the wall"). On a state inside the
  wall at delivery speed `W/(½π²) ≈ 23`; on every legitimate reflection run it was at most
  `1.76e-20`.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.1).** G1 is now the model's:
> `reflects_at_origin` is a property of `AbstractModel` (default `False`,
> `CosmologyConcepts/Models/AbstractModel.py:191-198`), `True` on `ExponentialModel`
> (`ExponentialModel.py:71-75`) and read through `RealizedModel.reflects_at_origin` (`:130-132`), and
> `log_V_floor` is the normalization ln V₀ (`ExponentialModel.py:77-79`), not Λ⁴ (the two were equal at
> the fixed normalization 4 ln Λ). `AbstractPotential` no longer exists, and `ExponentialModel` is the one
> model that declares it.

An accepted state with `φ ≤ 0` is never reflected: under the cap it can only mean the cap was
violated, and it is a `ComputationFailureError`. Reflections are counted, passed to the
supervisor (`notify_reflection`) and stored as `number_reflections` (§3.5.7).

Validity range. The model is right where the wall is thinner than a representable step and
nothing but the wall turns an inward-moving field; that is the exponential potential at
`M ≲ 1e-8`. It is exact to the neglected flight time and nothing else. It does *not* make
physical-`M` histories with β ≥ 1.2 computable (§3.5.6).

#### 3.5.4 The Jacobian clamp (new; no counterpart above)

SciPy's `_dense_num_jac` (`scipy/integrate/_ivp/common.py`) multiplies a state component's
perturbation factor by 10 whenever that component's Jacobian column is negligible, with only a
lower clamp. The `ln T_J` column is identically zero once `g_s` is constant, so the factor grows
for the rest of the history: `−910, −8 627, −85 936, …` as substituted values, reaching
`−9.4×10³⁰⁷`, at which point `exp` is 0 and the next probe is non-finite (audit §8 F2). These
were the "wild trial states". After every accepted step the loop does
`np.minimum(solver.jac_factor, 1e-4, out=solver.jac_factor)` (`StepControl.jacobian_factor_max`;
`jac_factor` is `None` until the first Jacobian evaluation), which bounds the probe at
`1e-4 |y|`. Measured on P2: 35 `T_Jordan = 0` substitutions in 15 e-folds without the clamp, 0
with it, the same RHS count and the same trajectory (audit §8 F2; log 01). None occurred in the
nine full histories. An analytic Jacobian would remove `num_jac` altogether and is not done.

#### 3.5.5 The exception policy (replaces §2.1's cascade and §3.4's `RuntimeError`)

There is one stepper (Radau) and one label, `"Radau+kinematic-cap-stepping0"`. `solver_list`,
the `while not success` loop and the BDF / LSODA / DOP853 names are deleted: `method="Radau"`
had been a literal since `f67bc3a`, so a failing history was integrated four times identically
(audit §4). The table as implemented (logs 01, 02):

| condition | outcome |
|---|---|
| the RHS raises `ComputationFailureError` on a trial state inside `solver.step()` | a rejected step: `h ← h/2` and retry; below `1e-13` e-folds a `ComputationFailureError` |
| `T_J ≤ 0` on a trial state (`ODEPolicy._get_T_Jordan`) | raises (it used to substitute 1 K, which hid the Jacobian-factor growth) |
| `G < 0`, overflow, non-finite input, non-finite RHS output | `ComputationFailureError` (unchanged); the NaN branch no longer reads the missing `data.d_logV_dphi` |
| `E < 0` (`ODEPolicy.__call__`) | printed and clamped to 0 (unchanged; open issue `[02-negative-E-is-clamped-not-raised-on-trial-states]`) |
| Radau `step()` returns a message | `ComputationFailureError` |
| the step budget is exhausted (§3.5.6) | `ComputationFailureError` |
| `φ ≤ 0` in an accepted state; G1 or G2 at a reflection; the termination root not bracketed | `ComputationFailureError` |
| the failsafe `N = 1000` is reached | `ComputationFailureError` (it was a `RuntimeError`) |
| the solution's dimension is not 5 | `assert` (a bug) |
| the z grid is too short for `N_final` | `RuntimeError`: configuration; stops the run |

`compute_scalar_model` wraps the loop and the sampling in one `try` and turns a
`ComputationFailureError` into `{"failure": True}`, which `ScalarModel.store()` records as a
failure row. The reason is printed and not stored (open issue
`[00-scalarmodel-failure-rows-carry-no-reason]`). `RHS_timer.__exit__` and
`IntegrationSupervisor.__exit__` print nothing now, where they printed a traceback for every
exception passing through an RHS call. The 100-fragment failsafe of §3.4 does not exist.

#### 3.5.6 The step budget

`StepControl.step_budget = 2_000_000` accepted steps (about an hour). Exceeding it raises
`ComputationFailureError` "step budget exhausted: … took n accepted steps (budget b) at N=…,
T_J=… GeV, with r reflection(s)". Why: at `M ≲ 1e-10` with β ≥ 1.2 the settling bounces
double per e-fold from `N ≈ 37` (β ≤ 2) or `41` (β = 3), because a bounce period scales as the
square root of the amplitude and the amplitude decays exponentially; the loop followed 4 587 →
62 135 steps per e-fold for `N = 36 → 41` at β = 1.2, extrapolating to `10⁷–10⁸` steps to
`T_CMB` (audit §3.7; `p_full.py … kin reflect`). No scheme that follows the bounces one at a
time reaches `T_CMB` there. The missing piece is physics: once the amplitude is far below any
scale of interest the field is a passenger at `φ_wall(ρ)`, the minimum of the effective
potential, and a parked-tracking model (switch criterion, tracking solution, the parked field's
contribution to `ρ_φ`, `p_φ` and the adiabatic diagnostic) is for the authors (open issue
`[00-settling-at-physical-M-needs-a-parked-tracking-model]`). Until it exists such a history
fails on the budget and is stored as a failure row instead of running for days. The budget is on accepted steps, not RHS calls. The `M = 1e-6`, β = 2 history completes with
7 429 resolved bounces in 3.33×10⁶ RHS (audit §3.7); at 9–10 RHS per accepted step, the ratio
the nine histories of log 01 show, that is about 3.5×10⁵ steps, under the budget. That is an
estimate from the ratio, not a run under this tree.

#### 3.5.7 What is stored per history

Gone: `number_level_1_entries`, `…_exits`, `number_level_2_entries`, `…_exits`,
`level_{1,2}_boundary`, `level_{1,2}_max_step`, `number_fragments` and
`number_hard_reflections` (`HARD_REFLECTIONS_KEY`). Now, in `extra_data`:
`number_reflections` (only when positive), `cap_fraction`, `cap_floor`, `cap_global_max_step`,
`jacobian_factor_max`, `accepted_steps`, `steps_rejected_by_exception` (only when positive),
and the three RHS statistics blocks as before. `compute_steps` is now the count of every RHS
call, Jacobian probes included (it was `solve_ivp`'s `nfev`, which excludes them). The
tolerances `atol = rtol = 1e-8` are stored as before and are the same everywhere: the relaxed
`1e-5`/`1e-6` of the Recliner overrides in §2.2 were never in the production path, and inside a
cap the tolerance does not set the cost (audit §3.6, §6). `ExponentialPotential`'s
`bounce_region_level{1,2}_boundary`, `…_max_step`, `default_max_step` and
`hard_reflection_point` are still defined and nothing reads them (open issue
`[00-region-properties-on-the-potentials-become-unread]`).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.1).** Those properties no longer exist.
> Prompt 04 (`597a215`) deleted the five `bounce_region_level{1,2}_*` and `hard_reflection_point`
> properties from `AbstractPotential` and `ExponentialPotential`, and `default_max_step` from
> `ExponentialPotential`, and closed `[00-region-properties-on-the-potentials-become-unread]`; prompt 07
> (`223afc6`) deleted both classes. The model (§3.7.1) carries only `reflects_at_origin` and the log floor
> that the elastic reflection of §3.5.3 reads.

Every `ScalarModel` store made before `VERSION_LABEL = "2026.5.0"` is invalid, and so is every
`AdiabaticHistory` and `BBNData` row built on one.

### 3.6 Added 2026-10-02 (`science-readiness`, prompts 02, 03, 05, 06b): what the row now carries, and whether the samples resolve the bounces

**What this adds to.** §3.5.7 lists what the supervisor stores per history; it is correct for what
it lists and is left as it stands. Three whole-history quantities are added to the `ScalarModel`
row itself, each in its own nullable columns and not in `extra_data`, and one measurement is
recorded about the sample grid that the stage after the integration reads. `VERSION_LABEL` is
`"2026.6.0"` since `science-readiness` prompt 01 (`1bc8977`); every store made before it is
invalid, and because the datastore has no migration the columns below need a fresh datastore file.
Commits: prompt 02 `77a7e0c`, prompt 03 `568c23a`, prompt 05 `c242f64`, prompt 06b `489ab26`.
Figures are those printed by the logs named, on the trees named there
(`prompts/science-readiness/logs/`).

#### 3.6.1 Three additions on the parent row

| what | column(s) | definition | read back by |
|---|---|---|---|
| the failure reason (prompt 02) | `failure_reason String(256)` | both failure exits of `compute_scalar_model` return `{"failure": True, "failure_reason": …}`: the `ComputationFailureError` message, or `"sampling: overflow when assembling sample values: …"`, truncated to 256; NULL on a success | `ScalarModel.failure_reason` (readable on a failure row, `None` on a success) |
| the first bounce (prompt 03) | `first_bounce_N`, `first_bounce_log_T_Jordan` (ln of T_J in GeV), `first_bounce_phi_Einstein` (φ in M_P), `first_bounce_reflected` | `first_bounce(result)`: the root of π, by `brentq` (`xtol=1e-15`) on the interpolant of the first accepted step with π(t_k) < 0 < π(t_{k+1}); if an elastic reflection comes first, that reflection (`reflected=True`); `None` otherwise. No `φ < 1.5 M` filter. All four NULL with no bounce and on a failure row | `ScalarModel.first_bounce` |
| φ and ρ_NP/ρ_R,J at fixed T_J (prompt 06b) | `phi_Einstein_1MeV`, `density_NP_ratio_1MeV`, `phi_Einstein_70keV`, `density_NP_ratio_70keV` (φ in M_P) | `fixed_T_values(result, policy, coupling, units)`: the first crossing of `ln T_J = ln(1 MeV)` and `ln(0.07 MeV)` on the dense output (`T_Jordan_crossing`, `brentq` on the accepted step's interpolant), φ read there, and the ratio built as `compute_BBN_data` builds it, `(3 M_P² H_J² − ρ_R,J (1 + f_m))/ρ_R,J`. NULL where the history does not reach the temperature, and on a failure row | `ScalarModel.fixed_T_values`, also under `_do_not_populate` |

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.1).** `fixed_T_values` is
> `fixed_T_values(result, policy, model, units)` (`ComputeTargets/ScalarHistory.py:1093`), with a
> `RealizedModel` where the table says `coupling` (`fixed_T_value_at`, `:1060`, likewise), and the
> row and the properties are `ScalarHistory`'s. The values on the roster below are of the fixed
> normalization (Λ = 1 meV, φ_ref = 0) and the old electronvolt, and are not re-measured.

The two temperatures are the module constants `FIXED_T_JORDAN_HIGH_MEV = 1.0` and
`FIXED_T_JORDAN_LOW_MEV = 0.07`; they are not a run option and not part of the lookup key.

**Why the dense output.** The first bounce at small `M` lasts far less than one z-grid sample, so
it cannot be recovered from the stored samples. On the same histories (log 03, Verification) the
sample-based detector (first π sign change from − to + among the samples with `φ < 1.5 M`) returns
742.79 MeV at β = 2, `M = 10⁻³` against the dense output's 746.686 MeV, and 0.39 MeV at β = 1.6,
`M = 10⁻⁵` against 420.758 MeV. The same reasoning is why the fixed-`T` values are not read from
the samples: they are properties of the whole history, and reading them from the samples means
loading every sample of every history (the first implementation of the figures did, and was
reverted, `a2deb00` → `8fcb295`).

Values on the three roster histories (`tools/history_and_bbn.py`, log 03 and log 06b, full
network):

| β, M | first bounce N | T_J (MeV) | φ at 1 MeV (M_P) | ρ_NP/ρ_R,J at 1 MeV | φ at 70 keV (M_P) | ρ_NP/ρ_R,J at 70 keV |
|---|---|---|---|---|---|---|
| 2, 0.5 | 20.343026853 | 746.634744 | 1.138197048e-02 | −4.810565953e-02 | 8.695012936e-03 | 6.742167855e-02 |
| 2, 10⁻³ | 20.352082230 | 746.686275 | 3.524341402e-03 | −7.814304755e-02 | 6.541189438e-04 | 1.579537539e-03 |
| 1.6, 10⁻⁵ | 18.974433718 | 420.758153 | 1.418937374e-03 | −1.043543235e-02 | 1.313754026e-04 | −6.242391165e-03 |

On every window and history it was run on, the first negative-to-positive turning point of π
with no filter is the first `φ < 1.5 M` wall bounce, on the same accepted step (log 03; wall
bounces 26, 803 and 4 337 on the three histories). The ρ_NP/ρ_R,J ratio at 1 MeV is negative on
all three. Against the stand-in that interpolates the stored samples linearly in `ln T_J`, the
dense-output values differ by 3.55e-6 to 1.44e-2 (the largest is the 70 keV ratio at `M = 10⁻³`,
a small value between two samples; log 06b); that is a measurement, not a bound.

#### 3.6.2 The samples do not alias the bounces in PRyMordial's window (log 05)

The z grid has 250 samples per decade of Einstein-frame `1 + z`, so `ΔN = ln 10/250 = 0.0092`
e-folds per sample. The sample *cell* of a sample is the interval between the midpoints to its
neighbours in `N`. The half-periods of π (sign changes of π between accepted steps) were counted
per cell. The orchestrator's count, quoted in log 05 (Verification; scratch script
`orch_bounce_density.py` in the session scratchpad, not in the repository, run on `a522005` plus
the uncommitted built tree, whose trajectories are bit-identical to `a522005`; it reproduces
verification §4.9's 43–45 half-periods in 10 MeV–1 keV at β = 2), gives median half-periods per
cell, and the fraction of cells with at least one:

| T_J window | β = 1.6, M = 10⁻⁵ | β = 2, M = 10⁻⁵ | β = 2, M = 0.5 |
|---|---|---|---|
| 10–100 MeV | 0 / 0.01 | 0 / 0.03 | 0 / 0.03 |
| 1–10 MeV | 0 / 0.01 | 0 / 0.00 | 0 / 0.00 |
| 100 keV–1 MeV | 0 / 0.21 | 0 / 0.11 | 0 / 0.10 |
| 10–100 keV | 0 / 0.06 | 0 / 0.04 | 0 / 0.02 |
| 3–10 keV | 0 / 0.01 | 0 / 0.01 | 0 / 0.01 |
| 1–3 keV | 0 / 0.02 | 0 / 0.01 | 0 / 0.01 |
| 0.3–1 keV | 0 / 0.05 | 0 / 0.03 | 0 / 0.02 |
| 100–300 eV | 0 / 0.17 | 0 / 0.12 | 0 / 0.00 |
| 10–100 eV | 1 / 0.75 | 1 / 0.64 | 0 / 0.00 |
| 1–10 eV | 7 / 1.00 | 5 / 1.00 | 0 / 0.00 |
| 0.1–1 eV | 16 / 1.00 | 17 / 1.00 | 0 / 0.00 |

The implementation agent's own count (log 05's `diag.py`) agrees: at β = 1.6, `M = 10⁻⁵`, 6 of 130 cells in
[0.3, 1) keV and 2 of 120 in [1, 3) keV contain a sign change of π. So:

- **In PRyMordial's window (0.3628 keV to 10 MeV, §7.6.1) the grid resolves the bounces.** Most
  cells hold none. The jumps of ρ_NP/ρ_R,J below 3 keV at small `M` are resolved bounces, not
  phase noise: the ratio is a sawtooth that jumps by about +0.005 to +0.007 at a bounce
  (β = 1.6, `M = 10⁻⁵`) and falls smoothly by about 1.2e-4 per sample in between, and its 10
  largest steps carry 95 % ([0.3, 1) keV) and 99 % ([1, 3) keV) of the rms step². The point
  samples are therefore the behaviour of H_J on the solution there.
- **Aliasing begins below about 100 eV**, and at `M = 10⁻⁵` is complete below 10 eV (5–17
  half-periods per cell). The stage that reads the late samples, `AdiabaticHistory`, is therefore
  exposed to it: open issue `[post-adiabatic-Q-reads-aliased-late-samples]`
  (`integrator-remediation` board), and `[00-stored-samples-alias-the-rebounds]`, which
  `science-readiness` narrowed on 2026-10-02 to that adiabatic half.
- **Bounce averages were built, measured, and withdrawn. None of it is in the tree.** The plan
  was a cell mean of H_J² and φ_E by three-point Gauss–Legendre on the dense output, handed to
  BBN in place of the point values. Measured on the built tree (log 05, Deviation 1, back-to-back
  against `a522005`): the rms step of ρ_NP/ρ_R,J fell only to 0.71–0.84× (target ≤ 0.1 at
  β = 1.6, `M = 10⁻⁵`; ≤ 0.5 at β = 2, `M = 10⁻³`); the cell mean of H_J² against the point
  ρ_R,J carries a curvature bias of `sinh(4h)/(4h) − 1 = 5.65e-5` for a cell half-width
  `h = ΔN/2 = 0.0046`, because H_J² ∝ e^{−4N}; it moved D/H at β = 2, `M = 0.5` by 1.57e-3
  (2.560889654 → 2.564920856 ×10⁻⁵); and β = 2, `M = 10⁻⁵` failed in PRyMordial's low-T
  network. The user ruled on 2026-10-02 that, in that window, the point samples are the behaviour
  of H on the solution, and that a PRyMordial failure on that input is a finding about
  PRyMordial, not a reason to change the input. `SampleValues` and the `ScalarModelValue` table
  have no cell-mean field or column, and `compute_BBN_data` reads the point `H_J`.
- **Unmeasured.** Whether the cubic spline of §7.6.1, through the resolved jumps, overshoots
  between two samples 0.0092 e-folds apart: `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`
  (`science-readiness` board §3).

  > **Note, 2026-11-01 (a measurement outside any campaign; `bbn-ratio-spline-2026-11-01/`).** Measured, and
  > closed as measured by the user. The cubic spline overshoots by 15–20 % of the jump in the next interval, and
  > the overshoot falls about 3.7× per interval; cubic, linear and PCHIP interpolants of the same samples give
  > Yp within 3.7×10⁻⁵ relative and D/H within PRyMordial's scatter. On the tuned histories of today's tree,
  > the jumps at resolved reflections are about 1 in r, mostly at 0.1–1 MeV and 10–100 MeV. Where each falls
  > inside its interval moves Yp by up to 4.8×10⁻⁴ and D/H by up to 6.2×10⁻⁴, relative.

### 3.7 Added 2026-10-26 (`jordan-normalization`): the model, the tuning to H0 and φ_min, the sampling axis and the Planck mass

**What this adds to.** The campaign changed what a stored history is. The potential and the
conformal factor became one model; each history's potential normalization and reference point became
quantities that the history finds; the sampling axis became ln(1 + z); and the electronvolt was tied
to PRyMordial's G_N. The commits, on the branch `jordan-normalization` (cut from `main` at
`ac187c8`): prompt 01 `a1c88b6` (the strip to the exponential model), 02 `abccc23` (the rename to
`ScalarHistory`), 03 `bd9ae42` (the axis), 04 `597a215` (the model interface), 05 `1675e73` (the Planck
mass), 06 `1b83bbd` (the tuning layer), 07 `223afc6` (the store: `VERSION_LABEL` `"2026.7.0"`,
`FINGERPRINT_FORMAT` 5), 08 `e624f1a` (the driver and the tools), 09 `2d3520b` (the rehearsal) and
10 (this section and the handover, `review-remediation-verification.md` §4.16). Code lines are those
of `dbfa31f`. The measurements are the campaign's: its logs
(`prompts/jordan-normalization/logs/`), its board, README §0.3 (the planner's probes at `ac187c8`)
and [`jordan-normalization-verification.md`](jordan-normalization-verification.md) (prompt 09's
rehearsal); each figure below names which. The branch is not merged into `main` when this is written.

**This supersedes**, by statement and not by edit:

- §1's "grids of potential parameters (M, Λ)" and §1.1's "re-maps `N` back to redshift via
  `z = exp(N) - 1`" (and §9 item 4): the grids are of (M, n, β), the normalization is tuned
  (§3.7.4), and the label is read off the axis (§3.7.9);
- §3.5.3's G1, "new on `AbstractPotential`", and its `Λ⁴` (§3.7.1);
- §3.6.1's `fixed_T_values(result, policy, coupling, units)`, which takes a model;
- §4.1's `ScalarHistory._create_functions`, which no caller reaches (§3.7.9);
- §6's "dual potential representations" and its cached `4 log Λ`;
- §7.6.1's and §7.6.5's advice on `--T-stop-GeV`, an option that is gone (§3.7.2).

Each has a one-line dated note at the statement. **Every figure above this section was made
before the campaign**, with the fixed normalization (Λ = 1 meV: `exponential.yaml`'s
`Lambda-values-eV` and `tools/history_and_bbn.py`'s `LAMBDA_EV` at `ac187c8`), φ_ref = 0 and the
electronvolt of 2.436 × 10²⁷, and is a record of that tree; none is re-measured here.

#### 3.7.1 The model and the normalization

*The problem* (README §0.1). The model is Einstein-frame gravity with reduced Planck mass M_P, a
canonical scalar φ, and matter coupled to the Jordan metric g_J = A²(φ) g_E. The Standard Model
masses are constants of the Jordan-frame Lagrangian, so the Planck mass that matter sees is
M_P/A(φ), and a laboratory that measures G_N at a field value φ_ref measures M_P/A(φ_ref). Requiring
that M_P be the laboratory's Planck mass, the one the units use, is the condition A(φ_ref) = 1.
Before the campaign A(0) = 1 and the two Planck masses were identified, which chose φ_ref = 0
without saying so, and every Jordan-frame input (the Standard Model masses through g_*(T), T_init,
T_CMB, H0, ρ_m0, ρ_r0) was then labelled by A(φ_ref) to the power of its mass dimension.

*The classes.*

- `AbstractModel` (`CosmologyConcepts/Models/AbstractModel.py:135`) holds the Lagrangian's hooks,
  none of which takes φ_ref: `_log_V(normalization, φ)`, `_d_logV_dphi`, `_d2_logV_dphi2`,
  `_log_Omega(φ)`, `_d_logOmega_dphi`, `_d2_logOmega_dphi2`, `_log_V_floor(normalization)` (default
  `None`) and the property `reflects_at_origin` (default `False`).
- `realize(normalization, φ_ref)` (`:202`) returns a `RealizedModel` (`:61`), the one immutable
  object that the integrator and everything after it read.
- `ExponentialModel(store_id, M, n, beta, units)` (`CosmologyConcepts/Models/ExponentialModel.py:40`)
  is the one model: V = V₀ exp((M/φ)ⁿ) and A = exp(β(φ − φ_ref)/M_P), `type_id`
  `EXPONENTIAL_MODEL = 1`, shard key β, keyed in its table by (M, n, β)
  (`ObjectFactories/ExponentialModel.py`). The four other potentials were removed in prompt 01
  (README R11).

*A(φ_ref) = 1 is the base class's.* `RealizedModel.log_Omega(φ)` is `model._log_Omega(φ) −
model._log_Omega(φ_ref)`, the second term computed once in `__init__` (`AbstractModel.py:77`,
`:114-115`), so A(φ_ref) = 1 holds for any model and no model interprets φ_ref. For the exponential
model `_log_Omega` is βφ/M_P (`ExponentialModel.py:128-130`), so d ln A/dφ = β/M_P, which φ_ref does
not change.

*The normalization is the model's to interpret* (README R7). The exponential model reads it as
ln V₀: `_log_V` is `normalization + (M/φ)ⁿ` (`ExponentialModel.py:81-94`), `_log_V_floor` is the
normalization, since V → V₀ far from the wall (`:77-79`), `normalization_coordinate` is the identity
(`:149-151`), and `renormalize(normalization, φ, V_target)` is `normalization + ln V_target − ln V(φ)`
(`:138-147`), which makes V(φ) = V_target and raises `ValueError` for V_target ≤ 0.

*Where A enters, and what does not change* (README §0.2, at `ac187c8`). Outside the ODE, A enters in
four places: `HubblePolicy` (H_J = H_E (1 + βφ′/M_P)/A), `fixed_T_value_at`, the initial
Einstein-frame radiation density and the sampled ρ_R,J. Everything else reads only d ln A/dφ: the
ODE, `AdiabaticHistory`'s M²_eff, `surfing_turning_points`, the reachability roots and BBN. The BBN
interface (`compute_BBN_data` and the vendored `PRyM/`) is unchanged and `PRYM_VERSION` did not move:
with A normalized at φ_ref, Eq. 7's ratio r = ρ_NP/ρ_R,J = (3 M_P² H_J² − ρ_R,J (1 + f_m))/ρ_R,J is the
one PRyMordial needs (README §1).

*The shift law, measured* (log 04, tests (c) and (d)). At a fixed Einstein-frame state, shifting φ_ref
by δ multiplies 1 + r + f_m by e^{−2βδ/M_P}: the largest difference, on the scale
(1 + f_m + |r|) e^{−2βδ/M_P}, is 2.685e-14 over the two golden histories, and Eq. 7's identity holds to
4.251e-14 (β = 1.5) and 6.875e-14 (β = 5.5). The criterion is on that scale and not a plain relative
one, because at the T_J turning points 1 + βφ′/M_P ≈ −2.2e-3 and 1 + r + f_m is a cancelled sum (board
§5.5).

*The effective minimum* (`AbstractModel.effective_minimum`, `:227-265`) is the root of
V′(φ) + (d ln A/dφ) ρ_m,E(φ₀) A(φ)/A(φ₀), with ρ_m,E proportional to A at fixed Einstein-frame number
density, on the model realized at (normalization, φ_ref). It is found by `brentq` on
(0.05 φ₀, 20 φ₀) with `xtol` 1e-14 φ₀ and `rtol` 1e-13; a bracket without a sign change raises
`ValueError` naming it. The radiation term Σ ρ_R,E is left out because Σ = 1 − 3w is 0.0 exactly at
T_CMB on `QCD_Cosmology(Planck2018)` (docstring, `:243`).

#### 3.7.2 The two layers

`integrate_history` (`ComputeTargets/ScalarHistory.py:1137`) is the primitive (README R8): one
integration of one `RealizedModel` from T_init to any T_stop, sampled on the axis, with no
normalization logic; it is what `compute_scalar_history` was. `tune_history` (`:1575`) always
integrates to `cosmology.T_CMB_Kelvin * units.Kelvin` (`:1628`), by calling it once per iterate.
`compute_scalar_history` (`:1748`), the `@ray.remote` that `ScalarHistory.compute` submits, calls
`tune_history`, so every stored history is tuned. A stored history therefore always ends at the
cosmology's T_CMB: T_stop is not a key, `ScalarHistory.T_Jordan_stop` (`:1969`) is a property at
T_CMB, and `--T-stop-GeV` is gone (refused by `RemovedOption`, exit 2). The tests, the golden
histories' setup and `tools/history_and_bbn.py --fixed-normalization NORM PHI_REF_MP` call the
primitive directly, and the last takes `--T-stop-GeV` for the untuned run. No driver passes
`secant=False` to `tune_history` (`:1613-1614`).

#### 3.7.3 The conditions and the residuals

The history is tuned until two conditions hold at T_J = T_CMB (README R1, R2): the Jordan-frame
Hubble rate equals today's, H_J(T_CMB) = H0 (Planck 2018, at fixed ω_m; the deceleration parameter
is not a condition, R1), and φ_ref equals φ_min, the minimum of the Einstein-frame effective
potential at the history's own densities today.

- **Today's state** is the last sample, ln(1 + z) = 0 (`_tuning_state`, `ScalarHistory.py:1533`,
  `:1546`).
- **Eq. 7's V_target** is the potential at which H_J = H0 at that state:
  V_target = A₀⁴ [3 M_P² H0² G/(A₀² b²) − ρ_R,J (1 + f_m)], with G = 1 − π₀²/(6 M_P²),
  b = 1 + (d ln A/dφ) π₀ and A₀ = A(φ₀) (`:1554`); ρ_m,E today is A₀⁴ f_m ρ_R,J (`:1555`); and φ_min
  is `model.effective_minimum` at that ρ_m,E (`:1556`).
- **The residuals**, as stored: residual_H = H_J/H0 − 1 and residual_φ = φ_ref/φ_min − 1
  (`:1564-1565`).
- **Convergence** is `tuning_converged` (`:1423-1429`): `abs(residual_H) < tolerance and
  abs(residual_phi) < tolerance`, strictly. The tolerance is `DEFAULT_TUNING_TOLERANCE = 1e-5`
  (`:1391`) or `--tuning-tolerance`, and it is a key of every stored history (the `tolerance` row
  `tuning_tol`).

Measured (prompt 09, `.documents/jordan-normalization-verification.md` §1.3): every one of the
seven stored histories has both residuals strictly inside 1e-5.

#### 3.7.4 The update: the seed, the plain step, the secant, the guards and the cap

*The seed* is `model.seed(cosmology)` (`:1630`), taken afresh for each tuning, with no cache and
no use of any other history (README R13: "I'd only be happy with this if we were sure there was no
dependence on the initial field value"; §3.7.8 gives the dependence). `ExponentialModel.seed`
(`ExponentialModel.py:195`), for n = 1, tries the **tracking** analytic minimum (φ′ = 1.5 φ in Eq. 7 at
A = 1), then the **static** one (the field at rest), each from a fixed-point loop (`_analytic_seed`,
`:156-193`) that has no solution where it raises, does not converge in 200 iterations, or ends with
V, G or 1 + βφ′/M_P not positive; and then the base class's **plain** seed, V₀ = ρ_Λ =
3 M_P² H0² (1 − Ω_m) and φ_ref = 0 (`AbstractModel.py:267-280`). For n ≠ 1 the seed is plain.

*One step* (`tune_history`, `:1639-1745`), after an integration that has not converged:

1. **The guard.** If Eq. 7's V_target ≤ 0, the next iterate halves V(φ₀) through `renormalize` and sets
   φ_ref = φ_min, and the secant's matrix is reset (a "guard" step, `:1705-1718`). V_target ≤ 0 at
   `TUNING_GUARD_LIMIT = 4` (`:1395`) integrations running is a failure (§3.7.5). Why it exists
   (README §0.3 item 1): with φ_ref = 0 the first history at M ≥ 0.1 has A(φ₀) = 1.6–2.3, so V_target
   is negative before φ_ref has moved; updating φ_ref first and halving V cured it at every point
   tried.
2. **The cap.** After `max_integrations` integrations, the seed's included, the tuning fails
   (`:1723-1729`). The default is `DEFAULT_TUNING_MAX_INTEGRATIONS = 30` (`:1392`), and
   `--tuning-max-integrations` gives it; it is not a key (§3.7.5).
3. **The step** is `TuningSecant.step` (`:1439-1515`) on x = (the normalization's coordinate,
   φ_ref/scale), `scale` being φ_min of integration 0 (`:1702-1703`, `:1732-1738`), with G(x) the
   plain map: the coordinate of `renormalize(normalization, φ₀, V_target)` and φ_min/scale. The
   **first step is the plain update**, which returns G(x) and sets the matrix B = −I. Every later step is
   **Broyden's ("good") secant** on F = G(x) − x: B += (dF − B dx) dxᵀ/|dx|², then B d = −F by Cramer's
   rule (`_solve2`, `:1432`), and the next point is x + d. The **twice rule**: if the last step was a
   secant step and |F| > 2 |F_previous|, B is reset and the step is plain from x (`:1488-1489`).

*Measured* (prompt 06, `prompts/jordan-normalization/logs/06-probes/table.txt`; board §5.7). On the
thirteen points of README §0.3, the secant needs 73 integrations in all against the plain update's
116, and never more at any point: fewer at twelve, as many at one (β = 3, M = 10⁻⁵: 3 and 3).
Tracking seeds converge in 3–5 integrations (M ≤ 0.03), static seeds in 9–12 (M = 0.1: 9; M = 0.5: 9
and 12). The two methods agree to 4.2e-6 in ln Λ and 2.0e-5 in φ_ref (relative); the four
differences above 1e-5 are the plain update's slack at M ≥ 0.1 and at β = 3, M = 10⁻³. The guard
fires once, at β = 3, M = 0.5, at integration 0; the twice rule never fires. The static seed starts
far from H0: H/H0 − 1 at integration 0 is +0.681 (β = 3, M = 0.1), +0.679 (β = 1, M = 0.5) and +1.079
(β = 3, M = 0.5), against −7.4e-2 to −1.7e-4 for the tracking seeds (log 06, Observations).

*The analytic seed, measured* (README §0.3 item 3, at `ac187c8`, on the units of the time). The
analytic minimum V′ + (β/M_P) ρ_m0 = 0 is accurate in location and not in amplitude. At the
converged Λ, 2βΔφ/M_P is at most 1e-6 at M = 10⁻⁵, 4.5e-4 at 10⁻³, 2.4e-3 at 10⁻², 1e-2 at 0.1 and
1.5e-2 to 0.11 at 0.5; but V(φ_ref)/ρ_Λ at the converged history is 0.98 (M = 10⁻⁵, β = 1), 0.82
(β = 1.5, M = 10⁻³), 0.52 (β = 25) and 0.19–0.29 (M ≥ 0.1), because H_J carries the field's kinetic
energy and the factor (1 + βφ′/M_P)². The static seed puts φ_ref 4–38 % high at M = 10⁻³; the
tracking seed puts it within 1.4–3.8 % there and has no solution at M ≥ 0.1. As a seed they save
0–3 integrations. So the analytic route does not pin M_P without the dynamics, and the tuning stands.

#### 3.7.5 The failures

A failed tuning is stored as a failure row, whose reason's first clause is one of four (the
docstring of `tune_history`, `:1616-1624`):

- **"the normalization did not converge"**, after the cap (`:1723-1729`);
- **"no normalization reaches H0"**, for V_target ≤ 0 at four integrations running (`:1707-1714`);
- **"the effective minimum was not found"**, when `effective_minimum` raises `ValueError`
  (`:1664-1670`);
- **an integration's own reason**, with " (tuning integration k)" appended (`:1659-1662`).

Each failure prints `!! tune_history (label): reason` (`:1569-1572`), and `summarise_failure_reasons`
groups them by first clause, unchanged. **What `_tuning_state` does not catch**: only the `ValueError`
of the effective minimum is caught (`:1666`), so another exception from its arithmetic (a
`ComputationFailureError` from `log_V`, a `ZeroDivisionError` at 1 + βφ′/M_P = 0) would reach the
driver through the remote and not become a failure row (log 07, Observation 2). None arose in any test, in
prompt 06's thirteen points or in prompt 09's seven histories. **The cap is not a key**:
`--tuning-max-integrations` reaches the remote as an argument of `ScalarHistory.compute` and not
through the store, so a history that fails to converge within it is stored as a failure, and a later
run with a larger cap does not recompute it; only `--drop scalar-history`, which drops every history,
does (board issue `[08-a-history-that-did-not-converge-is-never-retried]`, open). No history failed
to converge at any point measured.

> **Note, 2026-10-27 (`tuning-followups`, prompt 01): the retry route.** The cap is still not a key,
> but a failed history can now be recomputed without dropping the others: a run with
> `--retry-failed-scalar` (and, typically, a larger `--tuning-max-integrations`) counts a stored
> scalar failure as missing and recomputes it, and the new row is stored beside the failure under
> the same key. Every lookup then returns the success if one exists, else the newest failure, as the
> `BBNData` lookup does for `--retry-failed-bbn` (`architecture-summary.md` §4.3's and §9's notes of
> this date). Without the flag the paragraph above stands: a stored failure counts as done. The
> board issue `[08-a-history-that-did-not-converge-is-never-retried]` is closed by this prompt.

#### 3.7.6 The progress line

After each integration, once the residuals are formed, `tune_history` prints and flushes one line
(`:1672-1677`):

    -- tune <label> it=<k> kind=<seed|plain|secant|guard> H/H0-1=… phi_ref/phi_min-1=… norm=… phi_ref=<M_P> wall=<s>

`kind` is how the iterate was reached, and `wall` is that integration's. Ray forwards worker output
to the driver, and the registered-run wrapper beats on each line without ever reading it as a stage.
Ray's log deduplicator keys a line on the words that hold no digit and holds back a line whose key
another process printed within 5 s, so tune lines of histories tuned in parallel can be collapsed
into one "[repeated Nx across cluster]"; prompt 09 saw β = 1.5's seed line swallowed this way, in
two runs (board issue `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`, open). The stored
`tuning_iterations` is the count to read. Prompt 09 measured the longest heartbeat silence at 58 s
(`reference-c`, the scalar stage), against the 900 s staleness window (verification §1.10).

> **Note, 2026-10-27 (`tuning-followups`, prompt 02): the line is printed only when the tuning is
> slow or unusual, and the paragraph above is of the tree before.** `tune_history` takes
> `progress="quiet"` (the default) or `"every"` and a `clock` (default `perf_counter`), and prints
> integration k's line (`tune_line_due`) when **any** of these holds: the clock has advanced
> `TUNE_LINE_INTERVAL_S` = 120 s since the history's last printed line (or since the tuning began, if
> it has printed none); k + 1 ≥ `TUNE_LINE_SLOW_INTEGRATIONS` = 10; the integration was reached by a
> guard step; or it was reached by the twice rule's reset of the secant (`TuningSecant.reset`, read after
> `step`). The line's form, the `!!` line of a failure, `compute_scalar_history` and every stored value
> are unchanged, so `main.py`'s histories are quiet; the three standalone tools
> (`tools/history_and_bbn.py`, `tools/surfing_scan.py` and, through it, `tools/plot_surfing_histories.py`)
> pass `progress="every"` and print one line per integration as before. The constants are the user's
> ruling of 2026-10-27 ("120s and 10 integrations … we can adjust later"). The lines exist for the
> heartbeat, which beats only on a stdout line, and the rule keeps the heartbeat inside its window while
> a tuning is slow: **while a tuning runs, its longest silence is at most `TUNE_LINE_INTERVAL_S` plus one
> integration, about 130 s at the slowest measured.** Between tunings, histories that converge in under
> 10 integrations and 120 s print nothing, and the only other line is `RayWorkPool`'s progress line,
> never sooner than `notify_min_time_interval` after the last (300 s, `RunDrivers/registered_run.py`'s
> `NOTIFY_FLOOR`), so **the run's longest silence is at most that floor plus the per-tuning bound, about
> 430 s.** Both bounds are **read, not measured** (no Ray was started), and both are inside the unchanged
> 900 s window, so no `--stale-after` is needed. With `--tuning-max-integrations` under 10, a fast tuning
> that fails at its cap prints only its `!!` line, which carries the last residuals. The board issue
> `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` is closed as superseded: the quiet lines
> are few and spread out, Ray's deduplication stays on, and the stored `tuning_iterations` is still the
> count to read.

#### 3.7.7 The stored outputs

`TUNING_OUTPUT_KEYS` (`:1399-1415`) are the fifteen values that `tune_history` returns and
`ScalarHistory` stores as nullable columns of its own row, NULL on a failure row
(`ObjectFactories/ScalarHistory.py:246-274`): `normalization` (the model's own value; ln V₀ for the
exponential model, in the run's units, Planck units for every driver), `V_ref` = V(φ_ref) (stored in
M_P⁴), `phi_ref`, `phi_0` and `pi_0` (the field and dφ/dN today) and `seed_phi_ref` (stored in M_P),
`tracking` = π₀/(1.5 φ₀) (the matter-era tracking diagnostic), `residual_H`, `residual_phi`,
`tuning_iterations` (the integrations, the seed's included), `plain_steps`, `secant_steps`,
`guard_steps`, `seed_kind` and `seed_normalization`. `ScalarHistory.tuning` (`:1996`) is a
`TuningValues` namedtuple, readable under `_do_not_populate`, and `ScalarHistory.realized_model`
(`:2014`) is `model.realize(normalization, phi_ref)`. `histories.csv` carries the normalization,
`V_ref_eV4`, `phi_ref_Mp`, `phi_0_Mp`, `tracking`, both residuals and the iterations
(`extract_common.py:520`).

#### 3.7.8 What was measured

- **The thirteen points** (prompt 06): §3.7.4.
- **Seven histories on the isolated cluster** (prompt 09, verification §1.3, §1.8): β = 0.5, 1.5, 3
  and 5.5 at M = 10⁻³ M_P, φ* = 5, β = 0.5 at φ* = 1, and β = 3 at M = 0.1 and 10⁻⁵. Every one
  converged, in 3–9 integrations (M = 10⁻⁵: 3; M = 0.1: 9; β = 5.5: 5; the others 4). **Every stored
  tuned value equals `tools/history_and_bbn.py`'s bitwise**, 7 histories × 15 keys × 2 stores, by
  `float.hex`. Against README §0.3, Λ is lower by 2.768–2.795 × 10⁻⁴ (the electronvolt, §3.7.10) and
  φ_ref agrees within 1.6 × 10⁻⁵. The tuning's integrations took 3–130 s per history by the tune
  lines seen, at a one-minute load of 2–7; M = 10⁻⁵ and β = 5.5 were the slowest (130 s and 113 s).
- **The dependence on the initial field** (README §0.3 item 2, π* = 0, `ac187c8`). Where the history
  surfs, every φ* falls onto one trajectory and the tuned Λ agrees to the tolerance (< 10⁻⁶ at
  β = 1.5, M = 10⁻³ and at β = 3, M = 0.1, φ* = 5 and 2 against 1, and at β = 3, M = 10⁻³, φ* = 5
  against 2). Where it does not surf, the memory of φ* is erased only by the late relaxation onto the
  matter-era tracker, and survives where that has not finished by T_CMB: Λ at φ* = 5 against φ* = 1,
  as README prints it, is +1.1e-3 at β = 0.8, +3.4e-3 at β = 0.5 and +1.6e-2 at β = 0.3
  (M = 10⁻³). These are measurements, not defects; the
  non-surfing block of the 2026.6.0 run (β 0.5–1.1 at φ* = 1, 2, 5) lies where they are largest.

#### 3.7.9 The sampling axis

- **The label** (`CosmologyConcepts/redshift.py`) is ln(1 + z_E), the Einstein-frame redshift counted
  back in e-folds from the end of the history, T_J = T_CMB. `redshift(store_id, log_one_plus_z)`;
  `.z` is derived by `expm1` and is `inf` past ln(1 + z) = 709.78, not an error (`:74-82`); `float()`
  of one is ln(1 + z) (`:84`). Before prompt 03 the label was z, which `exp(final_N) − 1` overflowed
  at N > 709.8 (README §0.3 item 4).
- **The grid** (`log_one_plus_z_grid`, `:24-41`) is the points k Δ, k = 0 … K, with Δ = ln 10/S,
  S = `--samples-log10-z` (default 250) and K = round(`--log10-one-plus-z-high` × S); the default
  extent is 300 decades of 1 + z, about 690.8 e-folds, and `--log10-one-plus-z-low` accepts only 0. Each
  point is an integer times a float and never accumulated, so every builder of the grid gets the
  same floats bit for bit. The reference store of prompt 09 holds 75,001 points, the last 690.78
  (`main.py --inventory`).
- **The lookup** is by absolute difference below `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10`
  (`config/defaults.py:23`), one range filter on the indexed column
  (`ObjectFactories/redshift.py:51-53`); the grid spacing is 9.2e-3, so two points cannot collide.
  Measured (board §5.4): the query plan searches the index, and 75,001 points took 7.0 s into an empty
  table.
- **The sampling** reads N_backward directly from the axis (`ScalarHistory.py:1296-1297`) and cuts the
  grid at the history's end with `truncate_to_history` (`:1118`, `:1272`); a history of 800 e-folds
  samples without overflow (log 03). A history that outruns the grid raises, as before.
- **ln(1 + z_J)** is on every sample, `SampleValues.log_one_plus_z_Jordan` (`:208`):
  ln(1 + z_E) + ln A(φ₀) − ln A(φ) (`:1327`), a column and never a key, since H_J < 0 during a bounce and
  a_J is not monotonic. Measured (log 03): the identity holds exactly at every golden sample; against
  `cosmology.z(T_J)` the largest difference is 3.07e-7 at β = 1.5 and 3.72e-8 at β = 5.5, bounces
  included.
- **What the axis moved** (log 03, against prompt 02): the sample counts are equal; every sample whose
  `raw_N` is bitwise equal on the two trees is bitwise equal in every field; `raw_N` differs by one
  ulp of `final_N` at 1 sample (β = 1.5) and at 2 (β = 5.5), and the largest relative difference is 6.6e-12
  (`reflecting_term`, β = 5.5); `n_samples` went from 2722 to 2723 at β = 1.5 and stayed 4894 at β = 5.5.
- **Left open:** `ScalarHistory._create_functions` (`:2130`) builds splines on `v.z.z` and reads
  `self.z_sample`, which `ScalarHistory` does not define, and nothing calls `functions` (`:2111`):
  board issue `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`.

#### 3.7.10 The Planck mass

`units.PlanckMass` is the reduced Planck mass. Before prompt 05 the three unit systems each set the
electronvolt from 2.436 × 10²⁷, 2.78e-4 above PRyMordial's value, so that 1 K, 1/eV and 1 kg
disagreed with k_B, ħc, ħ and the kilogram in eV by that much (README §0.2). Now `Units/base.py`
holds PRyMordial's G_N, `NEWTON_G_MEV_MINUS_2 = 6.70883e-45` (`:25`), and
`REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8π NEWTON_G_MEV_MINUS_2)` = 2.4353236 × 10²⁷ (`:31`), from
which `Planck_units`, `Mpc_units` and `GeV_units` set the electronvolt
(`Units/Planck_units.py:46`, `Units/Mpc_units.py:47`, `Units/GeV_units.py:38`); the mechanism is in
`architecture-summary.md` §8.1's note. Measured (log 05): the four relations hold to 1.70e-7 (1 K),
4.08e-8 (ħc), 1.58e-7 (ħ) and 2.13e-7 (kg) in every system; every eV-labelled input moves by
2.78e-4, the two golden histories' fields by at most 5.7e-4 except in the wall-amplified tail of
β = 5.5, and prompt 09's tuned Λ lie 2.768–2.795e-4 below README §0.3's.

---

## 4. Splines: where they are used and how boundary/dynamic-range issues are handled

Splines appear at three distinct places, each with its own boundary strategy.
All are `scipy.interpolate.make_interp_spline` (interpolating B-splines, cubic `k=3` by
default).

### 4.1 Output-history splines for the scalar model (`ScalarHistory._create_functions`)

Each stored history quantity (φ, π, log ρ_rad in both frames, log f_m, H in both frames,
log T_Jordan, g*_ρ, g*_s, Σ) is turned into a callable spline over redshift. Two hygiene
strategies are applied:

- **Sorting before splining.** The (z, value) pairs are explicitly sorted by z before the
  spline is built, because the integration produces samples from high to low z and
  `make_interp_spline` requires a strictly increasing abscissa.
- **Log abscissa.** Splines are built against `log(1+z)` rather than z (`log_z=True` in the
  `ZSplineWrapper`), matching the logarithmic spacing of the sample grid and the e-fold
  time variable, so the knot spacing is uniform in the natural variable.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.9).** `ScalarHistory._create_functions`
> (`ComputeTargets/ScalarHistory.py:2130`) still builds its splines on `v.z.z`, which since prompt 03 is
> derived from the axis and is `inf` past 709.78 e-folds, and wraps them in `ZSplineWrapper(…,
> log_z=True)`, which evaluates at ln(1 + z); it reads `self.z_sample`, which `ScalarHistory` does not
> define. Nothing calls `ScalarHistory.functions` (`:2111`): board issue
> `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`, open.

### 4.2 `ZSplineWrapper` boundary cushioning (`ComputeTargets/spline_wrappers.py`)

This wrapper is the main defense against extrapolation/boundary artefacts. On evaluation:

- **Hard rejection far out of bounds:** if `log(1+z)` exceeds the maximum knot by more than
  1% (or falls below the minimum by more than 1%), it raises a `RuntimeError` rather than
  silently extrapolating.
- **Soft cushioning near the boundary:** if the request is only marginally outside the knot
  range (within the 1% band), the evaluation point is *clamped* to the boundary knot value
  ("softly cushion the spline at the top end"). This prevents the notoriously wild behaviour
  of polynomial spline extrapolation just beyond the data while still tolerating the
  small overshoots that arise from floating-point mismatches between the sample grid
  endpoints and the requested endpoints.
- **Derivative chain rule:** when the wrapper is flagged as holding a derivative spline, and
  the spline is over `log(1+z)`, it divides by `(1+z)` to convert `d/d log(1+z)` back to a
  raw z-derivative — a deliberate correction to keep the differentiation consistent with the
  log abscissa.

### 4.3 Equation-of-state (g*) splines (`SaikawaShirai_EOS_spline`)

The relativistic degrees of freedom `g*_ρ(T)` and `g*_s(T)` come from the Saikawa & Shirai
(arXiv:1803.01038) fitting functions. Rather than call the (expensive, piecewise) raw
fitting functions at every RHS evaluation, they are **pre-splined once** at construction:

- The fitting functions are sampled on a grid uniform in `log10 T`, at a fixed density of
  `250 samples per decade`, over `[0.8 T_LO, 1.2 T_HI]` — i.e. the grid is deliberately
  widened 20% beyond the physical support so that the spline endpoints sit outside the
  region ever queried, avoiding edge artefacts at the temperatures actually used.
- Splines are built in `log10 T` (the natural variable). Their **analytic derivatives** are
  obtained with `spline.derivative()` and used directly for the `dG_/dlogT` quantities that
  the ODE needs — so `d g*/d log T` is consistent with `g*` by construction, rather than
  being finite-differenced.
- **Asymptotic clamping outside the fitted range.** Above `T_HI = 1e16 GeV` the value is
  pinned to the high-T relativistic count (106.75); below `T_LO = 1e-5 GeV` it is pinned to
  the post-e+e−-annihilation asymptotic values (g*_ρ = 3.38, g*_s = 3.94), and the
  corresponding derivatives are set exactly to zero. This gives clean, physically-correct
  plateaus rather than letting the spline ring near the ends of its support.

**Note added 2026-09-29 (review-remediation prompt 02, items R1 and R5).** The Jordan-frame
temperature law, d ln T_J/dN = −(1 + A′φ′)/(1 + ⅓ d ln g_s/d ln T_J), in
`ComputeTargets/ScalarModel.py`, needs `dG_s_dlogT` to be **d g_s/d ln T** (natural log,
dimensionless). The jax class (`SaikawaShirai_EOS_jax_autodiff`) returns T dg_s/dT, which is
that. The spline class differentiates a spline in log10 T, so its derivative is d g/d log10 T,
larger by ln 10 ≈ 2.303. From commit `5962833` (2026-01-19) until prompt 02 it returned that
derivative undivided. The entropy correction in the temperature law was therefore too large by
ln 10: from 2×10⁴ GeV, the law took 41.497 e-folds to reach T_CMB against 40.075 for exact
entropy conservation. Since prompt 02, `dG_s_dlogT` and `dG_rho_dlogT` divide the spline
derivative by ln 10, and both docstrings state the convention. The log10 grid and the clamps are
unchanged. The low-temperature clamp constants are now the fit's own limits, g*_ρ = 3.383 and
g*_s = 3.931, rather than 3.38 and 3.94 as described above (item R5). Those are the constant
terms of the fit's low-T branch, and the fit has converged to them at T_LO = 10 keV. The clamp is
therefore continuous. With 3.94, the step in g_s at the clamp put N at and below 10 keV
1.465e-4 e-folds away from exact entropy conservation. The guard
is `CosmologyModels/tests/test_temperature_law.py`. The details are in
`prompts/review-remediation/logs/02-fix-the-entropy-derivative.md` and
`.documents/audit-2026-09-29/README.md` §1 and §11.

### 4.4 Equation of state w(T)

`w(T)` is computed from `w = (4 g*_s)/(3 g*_ρ) − 1`, following directly from `s T = ρ + P`.
The base class notes that `g*_ρ` and `g*_s` are not independent (they must satisfy a
differential consistency constraint for this to be compatible with the continuity
equation). The spline subclass overrides `w(T)` to freeze the argument at a floor
temperature `_EOS_T_LO = 2e-3 GeV` below which the single-formula `w` would otherwise be
invalid (neutrinos already decoupled), yielding a smooth asymptotic 1/3.

---

## 5. The `AdiabaticHistory` computation (`ComputeTargets/AdiabaticHistory.py`)

### 5.1 What it computes

`AdiabaticHistory` quantifies the validity of the **adiabatic (WKB) approximation** for
perturbations of the chameleon field, as a function of comoving scale. For each sampled
history point it computes a dimensionless "adiabaticity parameter" `|Q|` for a set of fixed
physical wavenumbers `k_phys/H ∈ {10, 10², 10³, 10⁴}` (the `Q_labels` dictionary). The
maximum of `|Q|` over the whole history is retained for each scale, as a summary diagnostic
of where/whether the adiabatic condition is ever violated.

### 5.2 The effective mass

The core physical input is the chameleon effective mass normalized to H²,
`M²_eff/H²` (`AdiabaticComputePolicy.M2eff_over_H2`), assembled from three additive pieces:

- **self mass**: `3 M_P² · (V''/3H²M_P²)` — curvature of the bare potential;
- **conformal mass**: `3 M_P² · E · (d²logΩ/dφ²) · R`, where `E ∝ ρ_rad/H²`,
  `R = (Σ + f_m)/(1 + f_m)`, capturing the density-dependent chameleon mass;
- **gravitational mass**: `1 − (Ḣ/H² + 3)`, the metric contribution.

`R` is computed with an overflow-safe reformulation for large f_m (dividing through by f_m
when `f_m > 10`), the same guard used throughout the codebase. The kinetic constraint factor
`G = 1 − π²/6M_P²` and the radiation factor `E = G − V/3H²M_P²` are both bounds-checked; a
slightly-negative `E` (which can occur harmlessly when ρ_rad → 0 at late times) is clamped to
zero rather than being treated as a fatal error.

#### 5.2.1 Added 2026-09-30 (production-readiness prompt 03): the effective mass and Q's numerator as they now are

This subsection supersedes the three-piece list above and the account of the log-spline in
§5.3; both were right for the tree they describe (before `production-readiness` prompt 03). The
derivation is in `prompts/production-readiness/logs/03-adiabatic-source-response.md` §1. Numbers
marked **[adm]** are from `ComputeTargets/tests/test_adiabatic_mass.py`, and **[eosd]** from
`CosmologyModels/tests/test_eos_w_derivative.py`, both run with `CHAMPBH_TEST_REPORT=1` on
`5aba202` plus prompt 03's diff.

**The four pieces of `M²_eff/H²`** (`AdiabaticComputePolicy.M2eff_over_H2`):

- **self mass**, unchanged: `3 M_P² · (V''/3H²M_P²)` = V''/H².
- **conformal curvature term**, unchanged: `3 M_P² E (ln Ω)″ R`.
- **source response**, new: `3 M_P² E (ln Ω)′² (Σ² − Σ_T/(1 + x) + f_m)/(1 + f_m)`. It is the
  response of the source in V_eff′ = V′ + (ln Ω)′ (Σ ρ_R,E + ρ_m,E) to δφ at fixed Einstein-frame
  scale factor and fixed comoving entropy, which is how the ODE itself responds:
  d ln ρ_R,E/d ln Ω = Σ, d ln ρ_m,E/d ln Ω = 1, and d ln T_J/d ln Ω = −1/(1 + x) (entropy
  conservation, T_J Ω a_E g_s^{1/3} = const). Here Σ_T = dΣ/d ln T_J and
  x = ⅓ d ln g_s/d ln T_J. For the exponential coupling it is the whole conformal mass. The
  bracket B = Σ² − Σ_T/(1 + x) runs from −0.4067 (144 MeV) to +0.3498 (230 MeV) on 1000 points
  over [12 keV, 20 TeV] [adm, test (b)]. As f_m → ∞ the term tends to β² ρ_m,E/(M_P² H²), the
  standard matter-coupled chameleon mass; at f_m = 10⁶ it is within 4.1e-7 of it [adm, test (d)].
  (S = (B + f_m)/(1 + f_m) is guarded at large f_m in the same way as R.)
- **gravitational mass**, unchanged: `1 − (Ḣ/H² + 3)`.

The two conformal terms are computed by the module-level pure function
`conformal_mass_over_H2(three_MP_sq, E, Sigma, fm, d_logOmega_dphi, d2_logOmega_dphi2, Sigma_T, x)`.
`M2eff_over_H2` gains the argument `T_Jordan`, which `compute_adiabatic_values` sets to
`exp(value.log_T_Jordan)`, never anything derived from z.

**Σ_T's source.** Σ_T = −3 `cosmology.dw_dlogT(T_J)`, a method added to `GenericEOSBase` and
forwarded by `LambdaCDM_GenericEOS`. Each class's derivative is consistent with its own `w`: for
`Xav_EOS_spline` (production) it is the analytic derivative of the ln T spline, 0 beyond the
table; for `SaikawaShirai_EOS_spline` it is 0 at and below the 2 MeV freeze and the derivative of
4g_s/(3g_ρ) − 1 from `dG_s_dlogT`, `dG_rho_dlogT` above it; the jax class uses autodiff of its own
w. Against a central difference of `w` in ln T (half-step 10⁻⁴) the production class agrees to
3.4e-8 [eosd]. No production code finite-differences `w`. Σ is the ODE's Σ = 1 − 3w (Xav's
table), not one derived from the g's.

**Q's numerator, in its smooth form.** Q is unchanged: |A·C|/|B|^{3/2} with A = M²_eff/H²,
B = A + k_p²/H², C = 1 + ½ d ln|M²_eff|/dN. Since d ln H²/dN = 2Ḣ/H²,

```
A·C = m (1 + Ḣ/H²) + ½ dm/dN,        m = M²_eff/H²,
```

which is finite through m = 0, where Q = ½ |dm/dN| / (k_p/H)³. The code (`Q_numerator`) now
computes it in that form: Ḣ/H² is `Hdot_over_H2_plus_3` − 3 at each sample (the quantity the ODE
and the gravitational mass use), and dm/dN = √(1 + m²) d asinh m/dN, from a cubic spline of
asinh m against N. asinh is linear through zero and logarithmic at large |m|, so the spline is
accurate both through a sign change and across a bounce's dynamic range. On the synthetic
histories of the campaign README §2 (e), at the production sampling ΔN = ln 10/250, A·C is
within 3.1e-6 (sign-changing) and 6.3e-6 (four 10⁴ spikes) of its exact value, relative to
max |A·C|, at the samples with N ∈ [0.5, 11.5]; at the two end samples of the sign-changing
history the spline's end condition gives 2.3e-4 [adm, test (e)]. A history whose M²_eff is
exactly zero at a sample gives a finite Q there, equal to ½ (dm/dN)/(k_p/H)³ to 4.7e-6.
**Correction to §5.3:** taking log|M²_eff| before splining is *not* a sign-safe strategy. The
log route was singular where M²_eff changes sign (A·C error 1.53 of max |A·C| on the
sign-changing history) and raised at an exact zero. It is gone; nothing on the path to Q takes a
logarithm of |M²_eff|, and no history is failed because M²_eff crosses zero.

**What the diagnostic assumes** (campaign README §2 (h); unchanged by prompt 03):

1. **A test field.** δφ is a test field on an unperturbed background. Mixing with the metric
   perturbation enters at order π²/M_P² = 6(1 − G), small only while the field's kinetic
   energy is a small fraction of the total.
2. **The plasma's response** to δφ is taken at fixed a_E and entropy. For modes deep inside the
   horizon the plasma's own perturbations are dynamical, and the coupled δφ–plasma system is not
   modelled.
3. **Which modes.** Q is evaluated at fixed k_p/H ∈ {10, 10², 10³, 10⁴}: a different comoving
   mode at each N, and no horizon-scale mode. Whether that is the intended diagnostic is open for
   the authors (`production-readiness` board §3,
   `[00-adiabaticity-is-evaluated-at-fixed-k-over-H-not-for-fixed-comoving-modes]`).

### 5.3 The adiabaticity parameter and its use of a differentiated spline

The quantity Q is built from `M²_eff/H²`, the physical scale `k_p²/H²`, and a logarithmic
derivative of the effective mass along the history:

```
A  = M²_eff/H²
B  = M²_eff/H² + k_p²/H²
C  = 1 + (1/2) d[log|M²_eff|]/dN
|Q| = |A · C / |B|^(3/2)|
```

The derivative `d log|M²_eff|/dN` is obtained by:

1. building `log|M²_eff|` on the raw e-fold grid `raw_N` (note it uses `log(|H² · M²_eff/H²|)`
   = `log|M²_eff|`, absolute value taken to survive sign changes of the effective mass);
2. splining it against `raw_N` with `make_interp_spline`;
3. differentiating the spline analytically (`.derivative()`) and evaluating it at each grid
   node.

This is a deliberate choice to obtain a *smooth* logarithmic derivative from discretely
sampled data without finite-difference noise, and taking the log-and-absolute-value first is
the dynamic-range/sign strategy that lets a quantity which changes sign and spans many
decades be differentiated stably. `B^(3/2)` is likewise taken over `|B|` to be safe against
sign.

The whole computation is timed with a `WallclockTimer` and the timing persisted.

*Note added 2026-09-30 (production-readiness prompt 03):* the log|M²_eff| spline described above
was singular where M²_eff changes sign and has been replaced; see §5.2.1.

---

## 6. Numerical hygiene strategies (pervasive)

The codebase is unusually defensive; the following patterns recur and are worth calling out
as a group:

- **Guarded exponentials.** Every `exp()` of an evolved log-variable (`exp(log_fm)`,
  `exp(log_T_Jordan)`, `exp(log_rhorad)`) is wrapped in `try/except OverflowError`, raising a
  typed `ComputationFailureError` with a rich diagnostic string (current N, φ, π, f_m, T)
  rather than letting a raw exception propagate. This is what feeds the solver-fallback
  cascade.
- **NaN/Inf sentinels on both ends of the RHS.** The ODE RHS checks its *input* state for
  NaN/Inf at entry and its *output* derivative vector for NaN/Inf at exit, raising with a
  full physical dump if either is contaminated — catching corruption at the earliest possible
  point.
- **Physical-positivity constraints as guards.** `G = 1 − π²/6M_P²` must be positive
  (equivalent to `π < √6 M_P`, i.e. the scalar KE not dominating); `E ∝ ρ_rad/H²` must be
  non-negative. Violations of `G` are fatal; small negative `E` is treated as a benign
  round-to-zero. These encode analytic constraints as runtime invariants.
- **Overflow-safe algebraic reformulations.** `R = (Σ + f_m)/(1 + f_m)` is rewritten as
  `(1 + Σ/f_m)/(1 + 1/f_m)` when `f_m > 10`; the Hubble rate is computed either via
  `log(V/3H²M_P²)` (when V > 0) or directly from ρ_rad (when V = 0) to avoid `log(0)`; the
  `V/3H²M_P²` factor itself is computed via a `log ρ_rad − log V` comparison that switches
  branches at `log(ρ/V) = 2` to avoid overflow of either exponential
  (`PotentialDerivativePolicy._evaluate_V_over_3H2Mp2_using_log_V`).
- **Dual potential representations with fallback.** `PotentialDerivativePolicy` supports both
  a log-potential interface (`log_V`, `d_logV_dphi`, `d2_logV_dphi2`) and a plain interface
  (`V`, `dV_dphi`, `d2V_dphi2`). It prefers the log form (better dynamic range for
  exponentially steep chameleon potentials) but automatically falls back to plain V if
  `log_V` returns NaN/Inf, and potentials can opt out of either via `_disable_log_V` /
  `_disable_V` flags. Second log-derivatives are converted to plain second derivatives via
  `V''/V = (log V)'' + ((log V)')²`.
- **Constant pre-computation.** Frequently used constants (`π²/30` and its log, `3M_P²`,
  `6M_P²`, `4 log Λ`) are computed once at construction and cached, keeping the hot RHS loop
  free of redundant transcendental calls.
- **Per-RHS instrumentation.** An `IntegrationSupervisor`/`RHS_timer` context wraps every RHS
  evaluation, accumulating count, mean/min/max evaluation time, and (optionally) the
  running min/max/mean of each RHS component. These statistics are persisted as integration
  metadata, giving an audit trail of the numerical behaviour of each solve. Periodic status
  updates estimate completion from the rate of progress in `log T`.
- **Reproducibility metadata.** Tolerances, solver identity, e-fold sample counts, event
  counts (hard reflections, level-1/2 entries/exits), fragment counts, and region
  boundaries/step sizes are all stored in the datastore with each result, so a stored
  history is fully reproducible and self-documenting.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.1).** Two bullets above
> describe code that is gone. `PotentialDerivativePolicy` has only the log-potential route
> (`ComputeTargets/Policies/PotentialDerivativePolicy.py`, docstring), reading `RealizedModel.log_V`,
> `d_logV_dphi` and `d2_logV_dphi2`; the plain-V route is removed, and the `_disable_log_V` and
> `_disable_V` flags are defined nowhere. And "`4 log Λ`" is not cached at construction: it is the
> model's normalization, ln V₀, an argument of `_log_V` that each history carries. The event counts,
> fragment counts and region boundaries of the last bullet are superseded by §3.5.7.

### 6.1 Added 2026-10-28 (`inelastic-reflection` prompt 01): a dark-radiation component in the policy expressions

`inelastic-reflection` carries the quanta a reflection produces as dark radiation,
ρ_χ,E = K(N) e^{−4N}, with K piecewise constant and held in a `DarkRadiationLedger`
(`ComputeTargets/dark_radiation.py`; `architecture-summary.md` §7.2's note of this date). Prompt 01
puts ρ_χ into every policy expression that is built from the constraint, and injects nothing:
the only way to give a history a nonzero ρ_χ is the test harness `rho_chi_ratio_init` of
`integrate_history` and `tune_history`. The derivations are in
`prompts/inelastic-reflection/logs/01-dark-radiation-component.md`.

With r_χ = ρ_χ,E/ρ_R,E (the same in both frames), Y = ρ_χ/3H²M_P² and G = 1 − π²/6M_P², the
constraint is 3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ, and:

- **V/3H²M_P²** = G·T, with T = 1/(1 + (ρ_R/V)(1 + f_m + r_χ)), or
  (V/ρ_R)/(V/ρ_R + 1 + f_m + r_χ) in the branch ln(ρ_R/V) > 2 described above. Every policy
  quantity built from it (V′/3H²M_P², V″/3H²M_P², `HubblePolicy`'s log branch, the reflection
  guard's W) sees ρ_χ through it.
- **E**, the matter-and-bath factor of the kick −3M_P² E (d ln A/dφ) R, stays
  ρ_R(1 + f_m)/3H²M_P²: E = (G − V/3H²M_P²)(1 + f_m)/(1 + f_m + r_χ). The quanta are traceless and
  do not reach the kick. The form is a product and a quotient of G − V/3H²M_P², so it adds no
  cancellation to the one that difference already carries when V dominates; the alternative
  (G − V/3H²M_P²) − Y would subtract two terms whose ratio tends to 1 as r_χ grows.
  `PotentialDerivativePolicy.matter_and_dark_radiation_fractions` returns (E, Y).
- **Ḣ/H² + 3** = G·A₁ + C·A₂ − Y·R/2, with A₁, A₂, C and R as before. The new term vanishes for
  a radiation bath (R = 0), where Ḣ/H² = −2 whatever ρ_χ is.
- **`HubblePolicy`'s V = 0 branch**: 3H²M_P² = ρ_R(1 + f_m + r_χ)/G, reading the r_χ the
  `ODEPolicy` used (`ODEPolicyData.rho_chi_over_rho_rad`). The branch is taken when V/3H²M_P²
  underflows to 0.0, which needs ln(ρ_R/V) ≥ 746: `exp(−745.13)` is still a subnormal.
- **`_tuning_state`'s V_target** (Eq. 7, §3.7.3) subtracts ρ_χ,J = r_χ ρ_R,J inside the bracket,
  with r_χ read from the payload's ledger at the last sample.
- **Unchanged**: `effective_minimum` (ρ_m,E only), d ln ρ_R/dN, d ln f_m/dN, d ln T_J/dN (none
  involves H), and `AdiabaticHistory`'s own E (prompt 05 of the campaign). `fixed_T_value_at`'s
  ρ_NP = 3M_P²H_J² − ρ_R,J(1 + f_m) includes ρ_χ through H_J, by design: it is the ratio BBN sees.

**Exactness at K = 0.** When the ledger is absent or K(N) = 0, `ODEPolicy` passes r_χ = 0.0, and each
expression branches on `r_chi == 0.0` to exactly its old operations; the ledger answers K(N) = 0
without an exp or a log. Both golden histories pass unchanged, bitwise. Their wall time, three runs
each alternating with the tree before (load 3.7–5.8), had minima 10.9 s and 10.9 s (the two
untuned histories) and 23.3 s and 24.1 s (the tuned one), inside the spread of the tree before
(10.9–11.8 s and 23.3–24.9 s); one `ODEPolicy` call took 7.7–7.8 µs against 7.3–7.8 µs before.

### 6.2 Added 2026-10-28 (`inelastic-reflection` prompt 02): the drain in the step loop

Prompt 02 makes `integrate_scalar_history` see the turning points of φ and drain a fraction α of the
condensate's kinetic energy into the §6.1 ledger. The parameter `alpha` (default 0.0; 0 ≤ α < 1, else a
`ValueError` at entry) is taken by `integrate_scalar_history`, `integrate_history` and `tune_history`,
not by `compute_scalar_history`, so no stored history has α > 0. The record is
`prompts/inelastic-reflection/logs/02-injection-at-turning-points.md`.

- **Turning points, at every α.** After each accepted step that is appended to the dense output
  (the last one up to N_final), π is read on the step's interpolant at its two ends by scalar calls,
  as `first_bounce` reads it, and at 8 equally spaced interior points in one vectorized call
  (`TURNING_POINT_INTERIOR_SAMPLES`, as `surfing_turning_points` samples g). A single sign change
  running end to end is located by `brentq` over the whole step (xtol = 1e-15), exactly as
  `first_bounce` locates it. Two or more changes on one step are each refined on their own
  sub-interval; a sample exactly 0.0 is a turning point only if the signs either side differ. Each is a
  `TurningPoint`, "wall" (π − → +) or "plateau" (+ → −). The interior samples were added at the user's
  ruling, after a near-stall at β = 5, α = 0.1, M = 0.5 M_P put a plateau and a wall-side point inside
  one step (π > 0 at both ends, a dip to −1e-6 between). Detection reads the interpolant only and does
  not touch the solver.
- **The drain point** (P3). A wall-side turning point arms a drain. A plateau turnaround, a floor
  reflection or the history's end drops it. The drain point is the end of the first accepted step,
  counting from the turning point's own, at which the accepted state has π > 0 and
  π′ = `solver.f[1]` ≤ 0: the first step end past the maximum of |π|. On the measured histories no
  drain was dropped, and none was made on the turning point's own step.
- **The drain.** At (N, y), with α_eff = α (the one place prompt 03 multiplies in its weights):
  1. ρ_kin,E = ½H_E²π², with H_E from `HubblePolicy` with the ledger before the drain;
  2. ΔK = α_eff ρ_kin,E e^{4N}, computed in logs;
  3. `ledger.add(N, ΔK)`;
  4. π → √(1 − α_eff) π;
  5. a new Radau instance at (N, y_drained).

  The ledger is added to before the restart, because Radau's constructor evaluates the RHS at
  (N, y_drained) and must see the new K. The step that ends the history never drains.
- **The floor rule** drains in its own restart: π_out = −√(1 − α_eff) π_in, with ΔK from the state and
  ledger before it. At α_eff = 0 the restart is the old one exactly.
- **H_E is continuous.** 3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ holds at the same H for (π, ρ_χ) and
  (√(1 − α)π, ρ_χ + αρ_kin), since 3M_P²H²·απ²/6M_P² = αρ_kin. In floats, H_after equalled H_before
  bitwise at 194 of the 195 drains measured. The other differed by 1.4e-14, one ulp of the exponent
  ln(3H²M_P²) ≈ −180 that `HubblePolicy` exponentiates. So the rounding bound on H's jump is set by
  that ulp (about 64 ε), not by G's condition. H_J jumps, because π does (P14).
- **The dense output is right-continuous** (`OdeSolution(ts, interpolants, alt_segment=True)`). At a
  drain or reflection node it returns interpolant k + 1 at its start, the restarted state exactly. The
  ledger's rule N_j ≤ N pairs that state with the new K. At α = 0 only a query exactly at an interior
  node could see a difference, and neither golden history makes one.
- **At α = 0** nothing is drained, and no restart is made. Both golden histories pass bitwise.
  The detection adds 451 `brentq` calls to them (6 at β = 1.5, 445 at β = 5.5), and two scalar and
  one 8-point interpolant evaluation per accepted step, about 20 µs. It costs about 10% of their
  wall time (9.7 → 10.8 s on the minima of three alternating runs). A restart at each would-be drain point with y unchanged is not free:
  it changes both golden histories (β = 1.5: 1233 → 1237 steps).
- **The counts at M = 0.5 M_P** (untuned, to T_CMB, α = 0.1): 7, 27, 86 and 75 drains for
  β = 1.2, 2, 5 and 25. At α = 0 there are 9, 23, 141 and 353 wall-side turning points.

### 6.3 Added 2026-10-28 (`inelastic-reflection` prompt 03): the non-adiabaticity gate

Prompt 03 weights each drain of §6.2 by the non-adiabaticity of the bounce that precedes it. At
α > 0 every drain point and every floor reflection is a **candidate**, drained at
α_eff = α · w_gate. The prompt also specified a tapered stopping rule (w_rate, on the injection rate
per e-fold); the user withdrew it the same day, before it shipped, because w_rate is never zero and
so never saved the restart it was meant to save. Nothing of it is in the code. The record is
`prompts/inelastic-reflection/logs/03-gate-and-stopping-rule.md`.

- **The mass.** `log_m_eff_squared(model, φ_E, ln ρ_R,E, ln f_m, Σ)` returns ln m_eff², with
  m_eff² = V″(φ) + [(ln A)″ + (ln A)′²] ρ_R,E (Σ + f_m), the curvature of V_eff = V + A ρ̃_c at
  fixed ρ̃_c = ρ_R,E(Σ + f_m)/A: the Einstein-frame trace that `ODERHS` couples to. V″ is
  V[(ln V)″ + (ln V)′²]. Each term is formed as a sign and a log magnitude and summed relative to the
  larger, so nothing overflows. m_eff² ≤ 0 is a `ComputationFailureError`. It takes no cosmology
  and no N, so a stored sample is enough (prompt 04 reuses it). It is not
  `AdiabaticComputePolicy.M2eff_over_H2`, the perturbation's mass, which adds the source's
  response, (ln A)′²ρ_R,E(Σ² − Σ − Σ_T/(1 + x)), and −(2 + Ḣ/H²)H².
- **The non-adiabaticity.** On every appended step, at α > 0 only, the ten samples at which §6.2
  reads π are read as full states. At each, one `ODEPolicy` call gives Σ, and `HubblePolicy` on the
  same data gives H_E, with the ledger as it stands before any drain at the step's end. Then
  𝒜 = H_E |d ln m_eff²/dN|/(2 m_eff), the derivative `np.gradient` over the step's own samples
  (`step_nonadiabaticity`). These calls never go through the RHS, so `nfev` is unchanged.
- **The half-oscillation.** A turning point of either kind, a floor reflection, or N_start begins
  one. A sample belongs to the half-oscillation (N_{k−1}, N_k] that contains its N. 𝒜_k is the
  largest 𝒜 over it (0.0 if it holds no sample; never seen at M = 0.5 M_P). Every `TurningPoint`
  records 𝒜_k and m_eff/H_E at the turning point itself: the plateau diagnostic of U4 (b′).
- **The gate.** w_gate = 𝒜_k²/(𝒜_k² + 𝒜₀²), with 𝒜₀ = `NONADIABATICITY_SCALE` = 1, the user's
  ruling (the threshold |ṁ|/m² = 1). It is computed as 1/(1 + (𝒜₀/𝒜)²) above 𝒜₀, so it is finite
  for every finite 𝒜 ≥ 0. A drain point takes the 𝒜_k of its wall-side turning point. A floor
  reflection takes the infall's: there the wall is thinner than a step, so 𝒜_k is a lower bound.
  Plateau turnarounds never drain.
- **α_eff == 0.0** (only by underflow): the candidate is recorded with no ledger entry, no change
  to π and no restart (the floor's elastic restart stays).
- **At α = 0** nothing of this runs: no mass, H_E or 𝒜 is computed, and the golden histories are
  bitwise. Their wall time did not move measurably (log 03 §4).
- **Measured** at M = 0.5 M_P, α = 0.1 (untuned, to T_CMB): 𝒜_k falls from 10²–10³ at the first
  bounce to 10⁻⁴ at the last, so the gate passes the early bounces whole and switches the late ones
  off. See log 03 §5 for the tables.

### 6.4 Added 2026-10-29 (`inelastic-reflection` prompt 05): unresolved turnarounds

At laboratory M the wall is thinner than Radau can resolve: φ_t ≈ 10⁻¹²–10⁻¹⁴ M_P at
M = 10⁻¹⁰–10⁻¹² M_P, where histories failed on Radau's float spacing. The user ruled (README §8.1
R8) that in dissipation mode a turnaround whose classical turning point lies below 10⁻⁸ M_P, or
whose test-field ratio 𝓡 exceeds 1, is reflected at 3 φ_t without being resolved. The record is
`prompts/inelastic-reflection/logs/05-unresolved-turnarounds.md`.

- **The assessment.** At α > 0, on a model with a wall (`reflects_at_origin` and a `log_V_floor`),
  at the start of every step with π < 0, `assess_infall(model, φ, ln ρ_kin,E)` forms
  ln E = ln(ρ_kin,E + V(φ) − V_floor) in logs and calls the model's `classical_turning_point(φ, ln E)`
  (§8.4a of `architecture-summary.md`). ρ_kin,E = H_E²π²/2, with H_E from `ODEPolicy` and
  `HubblePolicy` on the state and the ledger as they stand, never through the RHS, so `nfev` is
  unchanged.
- **Why E includes the wall.** Across the crossing ρ_kin + V is conserved (expansion neglected, as
  the model's methods neglect it), and far out V = V_floor, so E = ρ_kin + V − V_floor is the same
  from every point of the infall and V(φ_t) − V_floor = E has the same root. With ρ_kin alone, φ_t
  drifts outwards as the field climbs the wall: on a power-law wall V₀(M/φ)², V − V_floor is E/9 at
  3 φ_t. On the exponential wall at these M it is below 10⁻¹⁵ of E there.
- **The decision.** `unresolved_turnaround(φ, turning point, M_P)`: φ < K φ_t, and either
  φ_t < 10⁻⁸ M_P or ln 𝓡 > 0, every comparison strict, with K = `UNRESOLVED_REFLECTION_FACTOR` = 3
  and the threshold `UNRESOLVED_PHI_T_THRESHOLD_MP` = 10⁻⁸, read at call time. A `None` turning
  point (ρ_kin below the rounding of V − V_floor) makes no decision.
- **The order at a step start**: the assessment, the unresolved rule, the floor rule. Either rule's
  reflection goes through the floor branch: the guards G1 and G2, a `Reflection`, the drop of an
  armed drain, the close of the half-oscillation (§6.3), the candidate, and the restart from
  π_out = −√(1 − α_eff) π_in. The kinematic cap moves the field inwards by at most fφ per step
  (f = 0.1), so an unresolved reflection lies in [(1 − f) 3 φ_t, 3 φ_t), up to the change of φ_t
  between the two step starts.
- **The weight.** Every reflection, the floor rule's included (P20), drains with w_gate = 1.0
  exactly, so α_eff = α: at a floor reflection 𝒜_k reads the infall, never the wall. Its 𝒜_k is
  still recorded. Drain points after a resolved wall-side turning point keep §6.3's gate.
- **At α = 0** nothing of this runs, and the floor rule is the old code. Natural mode's warning
  needs φ_t at most once: at a resolved wall-side turning point φ is φ_t, and at the first floor
  reflection the model is called once, its failure caught and turned into a warning.
- **Measured.** In the probe's band (β = 2, α = 0.1) the reflected history equals the resolved one
  to 1.4×10⁻⁶ or better in Σ Δρ/ρ_R, with the same wall approaches, at M = 10⁻⁹–10⁻¹⁰ M_P. At
  3×10⁻¹¹ the resolved run itself fails on a wall with φ_t = 5×10⁻¹³ M_P. Under P20, K no longer
  moves Σ. An assessment costs about 30 µs, 4.6 % of an M = 0.5 M_P history at β = 2. Without
  parking, every history measured at M ≤ 10⁻⁷ fails at the bottom of its cascade, after
  ρ_kin/ρ_R < 10⁻⁶. See log 05 §5.

### 6.5 Added 2026-10-29 (`inelastic-reflection` prompt 06): parking

In dissipation mode each cascade of bounces is a geometric sequence, and without a switch every
small-M history failed at the bottom of its first one. The user ruled (README §8.1 R14, R15) that
once ρ_kin/ρ_R < ε the field is parked: the leftover kinetic energy goes to the ledger (P19), and the
field tracks the minimum of its effective potential with zero lag. The record is
`prompts/inelastic-reflection/logs/06-parking.md`.

- **The cut.** At α > 0, on a model with a wall, after every candidate (drain point or reflection of
  either kind) is recorded and drained: `parking_ratio_test(index, ρ_kin,E/ρ_R,E)`, false at the
  history's first candidate and otherwise ratio < `PARKING_EPSILON` = 10⁻⁶, strictly, ρ_kin,E the
  candidate's `log_rho_kin` from before its drain. If it passes, `cut_tracker` finds the tracker at
  the drained state: Σ and f_m from one policy call, T = ρ_R,E(Σ + f_m), φ_min from the model's
  `minimum_on_trace` searched from the field, π_min below, and m_eff/H_E there (H_E with the ledger
  as it stands). `tracker_adiabatic` parks only where m_eff/H_E ≥ 10.
- **The switch.** The drained state's ρ_kin,E = H_after²π_after²/2 goes into the ledger at the same N,
  a second entry beside the candidate's own drain (`parking_leftover`), and the parked phase starts
  from (φ_min, π_min) and the drained ln ρ_R,E, ln f_m and ln T_J. With the leftover in the ledger H_E
  moves only by V(φ_min) − V(φ) and the tracker's kinetic energy: by 0 to 4×10⁻¹² at M ≤ 10⁻⁷ M_P,
  against the leftover's 4.5–8.4×10⁻⁷ of ρ_R. Δ ln A at the switch is 10⁻¹³–8×10⁻⁹ there, against the
  2×10⁻⁵–7×10⁻³ that ln A spans over the oscillation it replaces.
- **The tracker's velocity** is linear algebra, not a difference. With g = 1 + (ln A)′π the
  right-hand side gives d ln ρ_R,E/dN = Σg − 4, d ln f_m/dN = (1 − Σ)g, d ln T_J/dN = −g/D
  (D = 1 + (d ln g_s/d ln T)/3), and dΣ/dN = 3w′g/D. So d ln T/dN = a + bπ with c = Σ + (3w′/D +
  f_m(1 − Σ))/(Σ + f_m), a = c − 4, b = c (ln A)′(φ_min); and π = s(a + bπ) with
  s = `d_minimum_d_log_trace` (no (ln A)′² term) gives π_min = s a/(1 − s b).
- **The reduced system** (ln ρ_R,E, ln f_m, ln T_J) is integrated by SciPy's DOP853 with the
  history's rtol, atol and global_max_step (0.1) as its largest step. `ParkedRHS` evaluates ODERHS's
  components 2–4 through the loop's `fun` at (φ_min, π_min). The system has no fast mode: a parked
  history to T_CMB takes 124–257 steps.
- **The dense output.** Each accepted step appends a `ParkedSegment`, which reads the step's reduced
  interpolant by a scalar call at each N and returns (φ_min, π_min, the reduced state). Its
  `minimum_on_trace` start is fixed per segment, the φ_min at the step's left node carried from the
  previous step's end, and the solver's derivative at the left node is re-evaluated with it, so a
  segment's value at N depends on N alone; an array call is a loop of scalar calls. SciPy's own
  interpolants do not give a scalar and an array call the same floats.
- **The end and the checks.** The history ends as before: at the root of ln T_J = ln T_stop on the
  step's interpolant, at N_stop, or at N_failsafe (a failure); the step budget counts both phases. At
  the end of every parked step (at N_final on the last) m_eff/H_E < 10 fails the history, with the
  switch named in the reason; prompt 07 replaces this with the release. Any `ComputationFailureError`
  in the parked phase is the history's failure, with no retry. At a reflection where the cut did not
  fire, |π_in| < 10⁻⁹ M_P fails the history (P21).
- **π_min jumps at T_J = 10 keV**, the lower end of the equation-of-state table, where dw/d ln T
  steps from −3.6×10⁻⁸ to 0. φ_min is continuous there; only its derivative steps.
- **At large M** (README §8.1 R17) the same cut applies. It fires at resolved drain points, where the
  gate drained little, and ends oscillations that would have gone on, putting at most ε ρ_R,E into
  the ledger. At M = 0.5 M_P: β = 5 parks at 81.2 MeV and fails at 9.40 MeV, β = 25 parks at 9.97 GeV
  and fails at 39.8 keV, where the tracker loses adiabaticity; β = 1.2 and 2 never park. At
  M = 10⁻³ M_P: β = 1.2 parks at 11.5 keV (and fails at 0.25 eV), β = 2 at 81.8 keV and β = 3 at
  23.0 MeV; β = 0.5 never parks. There (V − V_floor)/ρ_kin at the cut is 1.1–41, not ≪ 1. Dissipation
  mode at large M is not claimed to be physical; whether its answer means anything is for the user
  of the result to judge.
- **Measured.** Every history of log 05 §5.5's grid (β = 1.2, 2, 3; M = 10⁻⁷–10⁻³⁰ M_P) parks at the
  candidate log 05 named and reaches T_CMB, in 1.6–8 s. The parked ln T_J differs from the oscillating
  history's by 1.1×10⁻³ at β = 1.2 and at most 1.7×10⁻⁵ at β = 2 and 3, over the windows 05's
  histories survive. A parked right-hand-side evaluation costs about 50 µs (ODERHS's 20 µs, a
  `minimum_on_trace` from the step's start and the velocity), a parked step about 0.5–0.9 ms, and the
  cut's tracker about 80 µs where the ratio test passes; the M = 0.5 M_P, β = 2 history costs the
  same as before (2.65 s against 2.66 s). See log 06 §5.

### 6.6 Added 2026-10-30 (`inelastic-reflection` prompt 07): the release

A parked tracker that stops being adiabatic failed the history in prompt 06. The user ruled (README
§8.1 R16) that the field is released there instead, and P24 that a released field is integrated in
full to T_CMB and never parks again. The record is `prompts/inelastic-reflection/logs/07-unparking.md`.

- **The release point.** At each parked step end (and at N_final on the step that ends the history)
  the check of §6.5 is made; where m_eff/H_E < 10 the field is released at the root N_rel of
  g(N) = m_eff/H_E − 10 on that step's `ParkedSegment`, between its left node and the point checked,
  rather than at the step end. g is evaluated on the segment by scalar calls; brentq with xtol 10⁻¹⁴
  and rtol 4ε puts N_rel within 2(xtol + 4ε|N|) ≈ 10⁻¹³ e-folds of a sign change of g. g's own noise is
  the jitter of φ_min's root (`minimum_on_trace`'s xtol and rtol) through |∂(m/H)/∂ ln φ|: 10⁻¹¹ at
  M = 0.5 M_P, β = 5. So m/H at the root is 10 to about 3×10⁻¹¹ there (measured 5×10⁻¹³), and N_rel
  does not depend on the parked step grid. If g ≤ 0 already at the left node the release is there and
  the step's segment is dropped; it did not arise on any history measured. The root costs about 1 ms
  (10 evaluations of g at 50–70 µs).
- **The hand-back.** The released state is the segment's value at N_rel, one scalar call, and N_rel
  replaces the step's node in `ts`; nothing is drained or added to the ledger. The parked phase
  returns it to the loop as a value (`_ParkedPhaseEnd`), never as an exception, in both branches that
  can park (a reflection, a drain point). The loop starts a new Radau instance there (0.2 ms), as after
  a drain. SciPy's Radau dense output at its left node returns the state it was built from, so the
  last parked segment, the first Radau segment and the history's dense output (right-continuous at
  the node) return the same floats at N_rel, and H_E either side is the same float.
- **The reset.** At the release no drain is armed, no infall assessment is kept, and the gate's
  half-oscillation begins at N_rel with no sample, so that the non-adiabaticity sampled before the
  switch (folded since the wall-side turning point that armed the cut candidate) never reaches a
  candidate after the release. On the measured histories the reset changes nothing by itself (at
  β = 5 the carried maximum is 0.13, below the first turning point's own 0.69); a sentinel on those
  samples shows it.
- **P24.** After a release no candidate makes the ratio test or calls the tracker. The guard (P21)
  still runs at every reflection; its reason then says the field was released, and where. At M = 0.5
  M_P, β = 5 and 10 every candidate after the release would pass both of the cut's conditions; the
  released histories are still cheaper than the never-parked ones (8 295 against 9 270 steps at β = 5).
- **The lag after the release.** The field is released at zero lag, so it oscillates about the lagged
  tracker φ_min + δ₁, δ₁ the first-order lag from the right-hand side's π′ linearised about the tracker.
  The half-swing is 23–75 |δ₁|, so the lag is compared per oscillation, wall-side turning point to
  wall-side turning point: over the first eight after the release ⟨φ − φ_min⟩/⟨δ₁⟩ is 0.98–1.20 at
  β = 5 and 1.05–1.36 at β = 10, approaching 1 as m/H grows. The bound one can derive from the
  oscillation itself, its non-closure |Δ(φ − φ_min)|/2π plus the anharmonic shift
  |F_φφ/F_φ| S²/4 of a half-swing S, is 1.9–3.7 |δ₁|: it holds the sign of the lag, not its size
  (the test is a sign-and-order test, by the user's ruling). The anharmonic shift that bound allows,
  0.9–2.2 |δ₁| outwards, does not appear in the data.
- **At large M** (R17) the histories that failed in prompt 06 are released and reach T_CMB: at M =
  0.5 M_P β = 5 (released at 9.745 MeV), 10 (8.695 MeV) and 25 (40.3 keV), and at M = 10⁻³ M_P β = 1.2
  (0.27 meV; §6.5 printed its failure as 0.25 eV, which was 0.25 meV). Against the same histories never
  parked (ε = 0) N_final moves by 10⁻⁶–10⁻³ and Σ Δρ/ρ_R by 6×10⁻⁶–2×10⁻³; the largest difference in
  ln T_J (2×10⁻⁴ to 10⁻²) is set before the release, by the parking. No history at M ≤ 10⁻⁷ M_P
  releases, and every history that does not release is bitwise prompt 06's. See log 07 §5.

### 6.7 Added 2026-10-30 (`inelastic-reflection` prompt 07b): the wall search at the rounding of ln V

The turning points' search (§6.4; `AbstractModel._search_log_field`) compares the residual
ln(V − V_floor) − ln E at successive probes and refuses a probe that does not move towards the target.
ln(V − V_floor) is formed from ln V and ln V_floor. The user ruled (README §8.1 R18) that the defect this
caused is fixed before the peak and the margins. The record is
`prompts/inelastic-reflection/logs/07b-the-wall-search-at-the-rounding-of-ln-V.md`.

- **The quantization.** On the exponential, ln V = normalization + (M/φ)ⁿ is one rounded sum and
  ln V_floor is the normalization, exactly. While both lie in one binade the difference D̂ is exact
  (Sterbenz) and a whole number of quanta q = ulp(|ln V_floor|), 2⁻⁴⁴ = 5.68×10⁻¹⁴ at ln V₀ ≈ −276.6,
  the nearest to the true D = (M/φ)ⁿ. So D̂ is within ρ = ½ quantum of D, and ln(V − V_floor) carries a
  relative rounding of about 1/(2j) at j quanta. Two differences are ordered if their true values
  differ by more than 2ρ quanta. The shipped helper returned −∞ only at D̂ = 0. A field at
  u₀ = M/φ ∈ [0.50, 0.552)q has D̂ = 1, and the search's first probe, one e-fold in at e·u₀ < 1.5q, has
  D̂ = 1 too. The equal residuals raised "not monotone", and the history failed.
- **The width.** The helper returns −∞ where D̂ ≤ K ulp(max(|ln V|, |ln V_floor|)). A finite value
  then has D ≥ (K + 1 − ρ)q. The search compares two finite values at three kinds of place. With s the
  slope of ln D in ln φ (s = n on the exponential, at every u), these are:
  1. consecutive doubling probes, at least 1 e-fold apart;
  2. a halving midpoint against a finite end, at least 2^−H e-folds apart (H = 4 halvings);
  3. while the outer end is −∞, a midpoint past the root against the inner end.

  Places (1) and (2) are strict for every input if (e^{s·sep} − 1)(K + 1 − ρ) > 2ρ. Place (2) binds:
  (e^{1/16} − 1)(K + ½) > 1, so K > 15.005 and **K = 16**. The 6 % headroom of the integer step covers
  the ε-relative roundings of M/φ, e^x and the midpoints. Place (1) alone gives K ≥ 1, and every
  failure seen was at place (1), so K = 1 also fixes those failures; it leaves place (2) unguarded
  for roots within about 15 quanta.
- **The unresolvable region.** Place (3) depends on the root, not on K. Let D_t = ln(1 + E/V_floor)
  and t = D_t/((K + ρ)q). It is strict if (K + ρ) t (√t − 1) > 2ρ, which at K = 16 is D_t > 18.35q, or
  ln(D_t/(Kq)) > 0.137. A synthetic scan of fields 0.3–60q and roots 0.4–80q gives a sharp edge:
  - at K = 16, every root above 17.0q is found and every one below 16.9q raises ("a midpoint … is not
    between its ends");
  - at K = 0 and 1 the failures are ragged up to 15q;
  - at K = 64 the edge is at 65q.

  So the width costs roots with E ≲ 10⁻¹² V_floor. On every history measured the smallest root lies
  31–32 e-folds above the edge in ln D_t (25 at M = 10⁻³ M_P, β = 0.5).
- **The band, before and after** (test_injection's setup, α = 0.1):
  - Shipped: every history fails, at its first infall or soon after, at β = 1.2, 2 and 3 for every M
    from 10⁻¹³ to 10⁻²⁵ M_P. At β = 2 the failures run from 10⁻¹³·⁰⁰ to 10⁻²⁶·⁰⁰ by quarter
    decades, contiguously. 10⁻¹²·⁷⁵ and 10⁻²⁶·²⁵ run. The upper edge is where a field starting at
    φ* = 5 M_P first meets u₀ = 0.552q: M ≈ 1.6×10⁻¹³ M_P.
  - With the width: all of these reach T_CMB. They park at candidates 6, 48 and 58 (β = 1.2, 2, 3) at
    N = 28.364, 30.349 and 29.394, with Σ Δρ/ρ_R and the leftover equal to the M = 10⁻³⁰ value
    within 1.6×10⁻⁸, 3.1×10⁻¹³ and 3.4×10⁻⁸ relative.
  - The candidates' 𝓡 crosses the one-loop gap near M = 10⁻¹⁵–10⁻²⁰ M_P, and the one-loop chain
    raises on every candidate with 1 < 𝓡 < 10^2.92 (log 07b §5.3, for prompt 08).
- **What does not change.** The 28 histories outside the band (log 05's grid, M = 0.5 and 10⁻³ M_P,
  natural mode) are bitwise the shipped code's, warnings included. At M = 10⁻¹² and 10⁻³⁰ M_P the
  width reclassifies 367–2 760 helper calls per history, and the search reaches the same roots.
  `assess_infall` keeps its own expression for ln E, so ln E is unchanged everywhere. The helper costs
  about 90 ns more per call (180 against 95 ns), about 17 ms on a β = 2 history of 190 000 calls.

### 6.8 Added 2026-10-31 (`inelastic-reflection` prompt 08): the spectrum's peak and the relativistic margins

After the integration, and outside its loop, `integrate_history` asks whether each generation of quanta
stays relativistic (README §8.1 R3, R4, R13). It runs at α > 0 on a model with a wall only, the step
loop's own condition, and reads the history without changing it: every value of the integration is
bitwise what it was. The record is `prompts/inelastic-reflection/logs/08-spectrum-peak-and-margins.md`.

- **The definitions.** Every momentum and mass is Einstein-frame (a_E = e^N); p/m is frame-independent.
  - A **generation** is a candidate with ΔK > 0 and an assessment. Its peak is chosen by 𝓡 (R7, R19):
    the classical estimate, ln k_ex and ln m = ln ω_t, where ln 𝓡 ≤ 0; the one-loop estimate, ln k_ex
    and ln m_ta from `one_loop_turnaround`, where ln 𝓡 > 0 and the chain has a solution; the classical
    estimate, flagged `"classical_gap"`, where it raises or returns None. Its comoving momentum is
    ln q = ln k_ex + N_turn, and its share is ΔK_j/K(N_final).
  - **Birth:** b_j = ln k_ex − ln m, which is ℓ_t or ℓ bitwise.
  - **Reflection:** r_j, the minimum of ln q − N_k − ln m_k over every event with N_k > N_turn: every
    later candidate (ΔK = 0 included, with its estimate's mass), every plateau turning point and every
    wall-side turning point without a candidate (the field's own m_eff on the dense output there).
  - **Tracker:** t_j, the minimum of ln q − N − ln m_tr over the stored samples after N_late and
    today. N_late is the last turning point or candidate (R20); m_tr is the zero-lag tracker's mass,
    `minimum_on_trace` searched from the sample's own field on its stored trace.
  - The worst of each kind runs over the generations with share ≥ 10⁻³ (U9). The failing share sums
    the shares below ln 10 (P6). The parking leftover (R21) and K_0 are unassessed: no peak, their
    share reported. The shares partition K.
- **The coverage bound (R20's proviso).** Between consecutive events φ is monotone: a turning point is a
  sign change of π, a reflection flips it, a drain only scales it. On the exponential, (ln A)'' = 0 and
  V is completely monotone ((M/φ)ⁿ is, and exp of a completely monotone function is), so V'' falls
  strictly with φ. At fixed trace, m_eff² is then largest at the swing's inner end. With the trace's
  largest value T_max on the swing and its fraction ε of m² at the inner end, ln m ≤ ln m_in + τ,
  τ = ½ log1p(ε(T_max/T_in − 1)). So the field's own margin falls below min(b_j, r_j) by at most
  (N_b − N_a if the swing is outward) + τ + max(0, δ), with δ = ln m_field − ln m_estimate at the inner
  end. A generic model needs m_eff² to peak at an end of each swing at fixed trace, and
  (ln A)'' + (ln A)'² ≥ 0.
  - At resolved turning points δ = 0 to +0.0037: the field sits at φ_t to 2×10⁻⁴ and the estimate
    omits the trace term. At floor and unresolved reflections δ = −6.5 to −52, because the code turns
    the field back outside φ_t (φ/φ_t = 1.19 to 5×10²¹). The estimate's mass is then the bounce's
    physical one, and the code's samples lie above the bound.
  - The parked segment of a released history is bounded separately: on the tracker
    dμ/dN = −1 − d ln m_tr/dN, so where d ln m_tr/dN ≤ −1 the margin does not fall after the switch.
    The bound there is (Parking.N − N_turn,cut) + max(0, ln m_tr(Parking.N) − ln m_cut): 0.006–0.021
    against the segments' 2.3–13.2 e-folds. On the exponential's tracker V'' ∝ T^{(n+2)/(n+1)}, so
    d ln m_tr/d ln T is ½ (trace-dominated) to ¾ (n = 1). Measured, d ln m_tr/dN ≤ −1.02 on every
    parked segment that lies before N_late.
  - Measured on 51 dissipation histories: no sample falls below its bound. The largest shortfall is
    1.8×10⁻³ against a bound of 0.225 (M = 10⁻³ M_P, β = 0.5), and 3.0×10⁻⁴ against 0.015 at
    M = 0.5 M_P, β = 25. The plateau turning points never bind.
- **Measured across M** (test_injection's setup, α = 0.1, untuned):
  - M ≥ 10⁻¹⁵ M_P (β = 1.2) or 10⁻¹³ (β = 2, 3): every generation classical. The worst birth margin is
    −0.70 to −0.71, and −3.0 at M = 10⁻³, β = 0.5, R13's expected "by design" flag. The worst
    reflection margin is −0.67 to +0.014, and −2.4 at M = 10⁻³, β = 0.5. The tracker passes (+4.1 to
    +25) except at M = 10⁻³, β = 0.5 (−2.2).
  - The gap: histories at M = 10⁻¹⁵–10⁻²⁰ M_P carry gap generations, up to 0.88 of K at β = 1.2,
    M = 10⁻¹⁶. Every one is "ell fell", the saddle-node of log 04 §5.2; none returns None.
  - M ≤ 10⁻²² M_P (β = 1.2, 2) and ≤ 10⁻²⁰ (β = 3): every weighted generation one-loop, and every
    margin passes; the worst birth margin is +3.2 to +27.
  - The cost is 4–83 ms per history, 0.2–2.9 % of its 1.7–9.2 s, nearly all of it the tracker's
    samples. The one-loop calls are at most 43 ms.
- **Nothing is stored.** The payload's `"generations"`, `"margins"` and `"margin_warnings"` are not read
  by any store; prompt 09 stores the record.

### 6.9 Added 2026-10-31 (`inelastic-reflection` prompt 09): ΔN_eff, and the conformal mass with ρ_χ

Prompt 09 stores what 01–08 compute and makes α a key. Two things it adds are numerical: the
dark radiation's ΔN_eff at two temperatures, and ρ_χ in `AdiabaticHistory`'s effective mass. The record
is `prompts/inelastic-reflection/logs/09-store.md`.

- **ΔN_eff's two conventions (README §8.1 R22).** One neutrino species carries (7/8)·2·(π²/30)T_ν⁴, so
  ρ_χ = ΔN_eff (7/4)(π²/30)T_ν⁴ against ρ_R = g_ρ(π²/30)T_γ⁴, and ΔN_eff = (4/7) g_ρ (T_γ/T_ν)⁴ r_χ.
  - At T_J = 1 MeV the neutrinos have not decoupled from the e± heating: T_ν = T_γ, and
    ΔN_eff(1 MeV) = (4/7) g_ρ(1 MeV) r_χ.
  - Today T_ν/T_γ = (4/11)^{1/3}: ΔN_eff(T_CMB) = (4/7) g_ρ(T_CMB) (11/4)^{4/3} r_χ, P7's.
  - P7's expression at 1 MeV would be (11/4)^{4/3} ≈ 3.85 times too large.
- **The crossing (P31).** The values at 1 MeV and 70 keV are taken at the history's *first* crossing of
  T_J, on the dense output, exactly where `fixed_T_values` takes φ and ρ_NP/ρ_R,J (its N bitwise), with
  r_χ the policy's at the interpolant's state there and g_ρ the cosmology's `G_rho` at that T_J. Today's
  are at N_final, where they equal the last sample's bitwise.
- **The two values agree up to the injection after 1 MeV.** ρ_χ,J a_J⁴ = K is constant between drains
  (P1), and T_J a_J g_s^{1/3} is constant (the temperature law), so r_χ ∝ K g_s^{4/3}/g_ρ, and

  ΔN_eff(T_CMB)/ΔN_eff(1 MeV) = [K(N_final)/K(N_1MeV)] · [(11/4) g_s(T_CMB)/g_s(1 MeV)]^{4/3},

  whose second factor is near 1 (e± annihilation moves g_s by 11/4). Measured on the eight histories of
  log 09 §5.1, the stored ratio differs from this closed form by −1.328×10⁻³ (relative), the same on
  each. The residual is not the temperature law, which holds to 2×10⁻¹⁰ in ln(T_J a_J g_s^{1/3}); it is that the stored ρ_R,J is not (π²/30) g_ρ(T_J) T_J⁴ of the EOS's g_ρ: it lies 0.210 % below it at 1 MeV and 0.078 % below today, and that ratio, 1.00133, is the residual. The bath's ρ_R is evolved through Σ = 1 − 3w, so it drifts from the EOS's (π²/30) g_ρ T_J⁴ by about 10⁻³ over the history; not acted on (log 09 §5.2, §7).
- **BBN sees ρ_χ through H_J with a weight F** (P9; orchestrator note 09, ruling 11). BBN's ratio
  (3M_P²H_J² − ρ_R,J(1 + f_m))/ρ_R,J holds ρ_χ as F r_χ, F = A²(1 + (ln A)′π)²/G, exactly, at one state.
  F is not near 1: at the 1 MeV crossing it is 0.93 (β = 2, M ≤ 10⁻¹²), 1.02 (β = 1.2, M = 10⁻³), 0.11, 0.049, 0.0099 and 4.4×10⁻⁵ at M = 0.5 M_P for β = 1.2, 2, 5 and 25. So at large M BBN counts the quanta far below their ΔN_eff. ΔN_eff (R22) does not carry F; BBN does, through H_J, and is not changed.
- **The conformal mass with ρ_χ (P29).** The quanta are collisionless and do not couple to φ, so
  δρ_χ,E/δφ = 0 at fixed a_E and ρ_χ is absent from V′_eff and from its φ-derivative. It enters
  M²_eff/H² only through H: in V″/H², in the gravitational term 1 − (Ḣ/H² + 3) (ρ_χ + p_χ = (4/3)ρ_χ in
  Ḣ), and in the normalization of the conformal term's E = ρ_R,E(1 + f_m)/3H²M_P². So E is
  `matter_and_dark_radiation_fractions`'s E, never G − V/3H²M_P², which holds ρ_χ/3H²M_P² as well. At
  r_χ = 0.0 the old operations are taken, so α = 0 is bitwise. Measured: against the tree before, ρ_χ moves M²_eff/H² by at most 12 % in relative terms, near a zero of it (m = −6×10⁻⁴ at M = 10⁻³ M_P, β = 1.2), and by at most 1.25 % of max(|m|, 1); Ḣ/H² by at most 10⁻³; max |Q| by at most 0.8 %.
- **Q on the parked samples (R15).** A parked sample has φ = φ_min and π = dφ_min/dN, and Q is computed
  on it as on any other: finite on every parked sample of the histories measured, 1 904–2 142 at β = 2, M = 10⁻¹⁶ and 10⁻³⁰ and at M = 10⁻³, β = 1.2, and 244 at M = 0.5, β = 5. The largest parked |Q| at k/H = 10 is 2.28, 0.0022 e-folds after the switch at M = 10⁻³, β = 1.2, where the spline of asinh m carries the switch's step; at β = 2 it is at the last sample, 2×10⁻⁸ to 7×10⁻⁵.

### 6.10 Added 2026-11-01 (`inelastic-reflection` prompt 12): the measurement of prompt 11

Prompt 11 ran 48 tuned histories with BBN through `tools/history_and_bbn.py`, at M = 4×10⁻³¹ and
10⁻¹² M_P (dissipation mode), M = 0.5 and 10⁻⁷ M_P (natural mode) and three values of the parking ε, and
rehearsed a two-α build under Ray (verification §1, §2). The code is prompt 10's (`8c12afa`;
`PARKING_EPSILON = 1.0e-6`, `ComputeTargets/ScalarHistory.py:126`). Every figure below is a line of
[`inelastic-reflection-verification.md`](inelastic-reflection-verification.md), at the section named,
and is measured; none is re-derived here. All 48 points ran with exit status 0, every history tuned and
every BBN solve a success (§1). The machine was shared, with the one-minute load between 2.2 and 33.9
(that document's header), so every time is a measurement under load.

- **The drain does not depend on M below 10⁻¹² M_P (§1.4).**
  - |Δ`injected_fraction`|/`injected_fraction` between M = 10⁻¹² and 4×10⁻³¹ at each of the 16 (β, α)
    is 1.47×10⁻⁵ at worst (β = 1.2, α = 0.03: 0.00477064 against 0.00477057, parking 0.0008 e-folds
    apart) and equal to the printed six digits at the other 15. The gate's limit was 10⁻³.
  - Every reflection at both M is unresolved (`resolved_candidates = 0`), every history parks, none is
    released: at these M the parked tracker stays adiabatic to today (§1.3).
  - Every history converged on its seed, in one integration, 11.1–31.3 s with BBN (about 8–9 s of it
    PRyMordial), 1 649–32 106 steps (§1.3).
- **ΔN_eff against α (§1.5).** ΔN_eff today rises strictly with α at all 8 (M, β), and so does ΔN_eff
  at 1 MeV; the values are the same at both M to the printed digits.

  | β | ΔN_eff today at α = 0.03, 0.1, 0.3, 0.9 | ΔN_eff/α, first to last |
  |---|---|---|
  | 1.2 | 0.021488, 0.065672, 0.19323, 0.56467 | 0.716 to 0.627 |
  | 2 | 0.016948, 0.052441, 0.1227, 0.14669 | 0.565 to 0.163 |
  | 3 | 0.012287, 0.031576, 0.041723, 0.046522 | 0.410 to 0.052 |
  | 5 | 0.0026859, 0.0062655, 0.014022, 0.019809 | 0.0895 to 0.0220 |

  The drain is close to linear in α only at β = 1.2 (ΔN_eff/α within 13 %); at β ≥ 2 it saturates.
  ΔN_eff at 1 MeV is 0.85–0.97 of the value today: the rest is injected after 1 MeV, or is the factor
  between R22's two conventions (§1.5).
- **Natural mode (§1.6).** At M = 10⁻⁷ P23's warning was printed at every β, and every history reached
  T_CMB and converged on its seed in one integration: 53.5–191.1 s, 150 847–493 615 steps (the slowest
  at β = 5, 191.1 s, 483 767 steps). At M = 0.5 nothing was printed, the histories took 9–20
  integrations, 19.2–118.1 s and 2 451–12 834 steps, and Yp falls from 0.148 to 0.0997 as β goes from
  1.2 to 5.
- **The parking ε (§1.7).** At M = 4×10⁻³¹, α = 0.1:
  - ε = 10⁻⁴ against 10⁻⁶ at β = 2, 3, 5: the switch comes 0.62–1.17 e-folds earlier, and
    `injected_fraction` is larger by 1.9–2.5×10⁻³ relative;
  - ε = 10⁻⁸ against 10⁻⁶: the switch comes 0.05–0.40 e-folds later at β = 2, 3, 5 and the drain differs
    by at most 2.4×10⁻⁵ relative; at β = 1.2 it parks 5.416 e-folds later (log 06 §5.5 found 5.4);
  - |ΔYp| ≤ 8.9×10⁻⁶ and |Δ(D/H)| ≤ 1.5×10⁻⁴ (×10⁻⁵) between any two ε at one β;
  - a smaller ε takes more steps before the switch: 3.3–4.2× more at 10⁻⁸ than at 10⁻⁴.
- **The margins (§1.3, "The margins").**
  - At M = 4×10⁻³¹ every worst margin is at least 6.54 (the birth margin at β = 1.2, α = 0.03), every
    failing share is 0 and no margin warning is printed.
  - At M = 10⁻¹² the worst birth margin is −0.70 to −0.74 at every point, with failing shares
    0.966–0.9999; the worst reflection margin is −0.68 to +0.62, failing shares 0.040–0.9994; the tracker
    margin is 12.7–21.2, share 0. Each of those 16 points printed the birth and reflection margin
    warnings, and R6's "nearly elastic" warning once.
  - The unassessed share (R21) is 5×10⁻⁷ to 1.6×10⁻³, the same at both M.
- **The BBN spline across injections (§3; P14, P46).** Below 10 keV every dissipation history is parked
  (β = 1.2 at α = 0.03 parks at 0.57 eV) and its ratio ρ_NP/ρ_R,J is flat. In the 10–100 keV window
  rms_step is 6.4×, 8.4×, 2.9× and 1.26× natural mode's at M = 10⁻⁷ for β = 1.2 at α = 0.03, 0.1 and 0.3
  and β = 2 at α = 0.03, the histories that inject or park there. Whether PRyMordial's spline rings on
  these steps, and what that does to Yp and D/H, is not measured: board issue
  `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]`.

  > **Note, 2026-11-01 (a measurement outside any campaign; `bbn-ratio-spline-2026-11-01/`).** Measured since,
  > and closed as measured by the user. The cubic spline rings next to each reflection's jump in r (15–20 % of
  > the jump), but that moves Yp by at most 3.7×10⁻⁵ relative. The jumps' positions within the sampling
  > interval move Yp by up to 4.8×10⁻⁴ and D/H by up to 6.2×10⁻⁴, relative, in both modes.
- **At M = 10⁻³ M_P, from the rehearsal (§2.3).** β = 1.5 at α = 0.1 parks at 17.7 eV (N = 36.34) and is
  released at N = 47.51, T_J = 2.37×10⁻¹⁰ MeV; β = 3 and 5.5 park and are not released; β = 0.5 never
  parks, its 13 drains are resolved and all after 1 MeV, and it ends with ΔN_eff today 8.95. That is R17's
  unofficial large-M regime. Its BBN, like its natural-mode twin's, fails on the spline's floor
  (`T_in_MeV = 0.00019824 MeV` against `T_BBN_spline_min = 0.0002 MeV`), as in
  `jordan-normalization` 09.
- **α = 0 is 2026.7.0 (§2.4).** 548 922 values of the four α = 0 histories of the rehearsal, compared
  by `float.hex` against `jordan-normalization` 09's reference store, are all equal; the store has since
  been retired, so the comparison cannot be repeated (board §5.13, "The stores").

### 6.11 Added 2026-11-01 (`inelastic-reflection` prompt 12): dissipation mode as it stands

§6.1–§6.9 were each written on the day of their prompt, and some statements in them were overtaken by a
later prompt. This section reads them in the order a history at α > 0 meets them, gives each step its
code at the dispatch commit (`7cfced4`, whose code is prompt 10's `8c12afa`; lines are in
`ComputeTargets/ScalarHistory.py` unless a file is named), and ends with the statements of §6.1–§6.9 that
are superseded and where. Natural mode (α = 0) runs none of it (README §8.2 P22); both golden histories
are bitwise unchanged, and the warnings of R6 are printed lines, never values. The sections are records,
and nothing in them is edited.

#### 6.11.1 What a history at α > 0 does, in order

0. **Entry.** `alpha` is a parameter of `integrate_scalar_history` (`:1432`), `integrate_history` (`:3857`)
   and `tune_history` (`:4464`), and of `compute_scalar_history` (`:4703`), which `ScalarHistory.compute`
   calls with the history's `alpha_value`. 0 ≤ α < 1 or `ValueError` (`:1590`). The wall is assessed
   only where `alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None` (`:1683`);
   the margins' condition at `:4130` is the same.
1. **The drain** (§6.1, §6.2; R11, P1–P3). The quanta are dark radiation, ρ_χ,E = K(N) e^{−4N}, in a
   `DarkRadiationLedger` (`ComputeTargets/dark_radiation.py:51`; `add` at `:103`). Turning points are
   seen at every α (`observe_turning_points`, `:1906`; `handle_turning_point`, `:2048`). A wall-side
   point arms a drain, made at the first step end past the maximum of |π|. A candidate (`candidate`,
   `:1777`) forms ΔK = α_eff ρ_kin,E e^{4N}, calls `ledger.add` (`:1881`), scales π by √(1 − α_eff) and
   restarts Radau (`new_solver`, `:1634`).
2. **The gate** (§6.3; U4 (b′), the user's 𝒜₀ = 1, R10). At a drain point after a resolved wall-side
   turning point α_eff = α w_gate, w_gate = `gate_weight(𝒜_k)` (`:1044`), 𝒜₀ = `NONADIABATICITY_SCALE`
   (`:99`). The weight is 1.0 at every unresolved reflection and every floor reflection (`:1750`, P20).
   The stopping rule of §6.3 was withdrawn and is not in the code.
3. **The unresolved rule** (§6.4; R8, R9, P18, P20, P23). At the start of every infalling step
   (`:2480`–`:2489`), `assess_infall` (`:1070`) asks the model for the classical turning point, and
   `unresolved_turnaround` (`:1104`) reflects the field at K φ_t, K = `UNRESOLVED_REFLECTION_FACTOR` = 3
   (`:109`), when φ_t < `UNRESOLVED_PHI_T_THRESHOLD_MP` = 10⁻⁸ M_P (`:110`) or ln 𝓡 > 0. Either rule's
   reflection goes through the floor branch and drains with w_gate = 1. The floor rule is the old one.
4. **Parking** (§6.5; R14, R15, R17, P18, P19, P21). After every candidate after the first,
   `parking_ratio_test` (`:1138`; used at `:2391`) asks ρ_kin,E/ρ_R,E < `PARKING_EPSILON` (`:126`,
   read at call time at `:1147`); `cut_tracker` (`:1317`) finds the zero-lag tracker, and
   `tracker_adiabatic` (`:1150`) requires m_eff/H_E ≥ `PARKING_ADIABATICITY_THRESHOLD` = 10 (`:127`). If
   both hold the leftover goes into the ledger and `run_parked_phase` (`:2132`) integrates (ln ρ_R,E,
   ln f_m, ln T_J) by DOP853 (`ParkedRHS`, `:1361`; `ParkedSegment`, `:1393`). The guard `parking_guard`
   (`:1160`, used at `:2433`) fails a history whose |π| at a reflection is below
   `PARKING_PI_GUARD_MP` = 10⁻⁹ M_P (`:128`) with the cut not having fired.
5. **The release** (§6.6; R16, P24). At each parked step end the same adiabaticity check is made
   (`:2192`); where it fails the field is released at the root of m_eff/H_E = 10 on the step's segment
   (`brentq` at `:2234`, tolerances `RELEASE_ROOT_XTOL`, `RELEASE_ROOT_RTOL`, `:135`–`:136`) and returned as
   a `_ParkedPhaseEnd` (`:2265`). It is integrated in full to T_CMB and never parks again (`release is
   None` at `:2391`).
6. **The wall search**, under steps 3–5 (§6.4, §6.7; R18). Every call of the model's
   `classical_turning_point` and `one_loop_turnaround` (`CosmologyConcepts/Models/AbstractModel.py:351`,
   `:356`) goes through `_search_log_field` (`:183`), which since 07b forms ln(V − V_floor) with
   `_log_V_minus_floor` (`:137`), returning −∞ within `WALL_ROUNDING_QUANTA` = 16 quanta (`:112`) of the
   rounding of ln V. A root with E ≲ 10⁻¹² V_floor raises, and a field inside such a root raises too
   (log 07b §7).
7. **The peak and the margins** (§6.8; R7, R13, R19–R21, U9, P25), after the integration and outside
   its loop: `peak_estimate` (`:3425`), `relativistic_margins` (`:3511`, a pure core),
   `history_margins` (`:3685`, the gatherer) and `margin_warning_lines` (`:3806`), called at `:4132`.
   The threshold is `MARGIN_THRESHOLD` = ln 10 (`:144`) and the weight `MARGIN_WEIGHT_SHARE` = 10⁻³
   (`:145`). Nothing of it changes a value of the integration.
8. **The stored values** (§6.9, §6.10; R22–R25, P26–P31). `dark_radiation_values` (`:3112`) forms r_χ and
   ΔN_eff at 1 MeV, 70 keV and today, `dissipation_values` (`:3224`) the 24 columns, and
   `ScalarHistory.store` (`:5242`) writes them with the per-sample ratio
   (`ObjectFactories/ScalarHistory.py`; α is `alpha_serial`, `:227`). The format is 6 and the label
   `2026.8.0` (`config/fingerprint.py:87`, `config/version.py:62`).

#### 6.11.2 Statements of §6.1–§6.9 that are superseded

The lines are this file's at `7cfced4`, before this section and the note of this date at §3.5.3 were added.
A reading of the rest of §6.1–§6.9 for this section, statement by statement, found no further one.

| Statement | Where | Superseded by | Where |
|---|---|---|---|
| "injects nothing: the only way to give a history a nonzero ρ_χ is the test harness" | §6.1 (`:1124`–`:1126`) | the drain | §6.2 (`:1162`), step 1 above |
| "`AdiabaticHistory`'s own E (prompt 05 of the campaign)" is unchanged | §6.1 (`:1149`–`:1150`) | `AdiabaticHistory` reads each sample's r_χ (P29) | §6.9 (`:1542`) |
| "not by `compute_scalar_history`, so no stored history has α > 0" | §6.2 (`:1163`–`:1164`) | α a key, stored | §6.9 (`:1517`) |
| the tapered stopping rule (w_rate) | §6.3 (`:1216`–`:1219`) | withdrawn on 2026-10-28; §6.3 says so | §6.3 itself |
| "every drain point and every floor reflection is a candidate, drained at α_eff = α · w_gate" | §6.3 (`:1215`) | w_gate = 1 at an unresolved reflection and at the floor rule (R10, P20) | §6.4 (`:1280`–`:1282`), step 2 |
| "A floor reflection takes the infall's [𝒜_k]" as the weight's input | §6.3 (`:1240`–`:1241`) | the weight is 1; 𝒜_k is still recorded | §6.4 (`:1280`–`:1282`) |
| "A `None` turning point … makes no decision" | §6.4 (`:1272`–`:1273`) | a root within the rounding width raises, and so does a field inside it | §6.7 (`:1438`); log 07b §7 |
| the wall search's refusal of a wall below the rounding of ln V (board issue `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`) | §6.4, log 05 §5.5 | the width K = 16; the issue is closed | §6.7 (`:1418`–`:1439`); board §4 |
| a parked step end with m_eff/H_E < 10 fails the history | §6.5 (`:1331`–`:1333`) | the release | §6.5 says so itself; §6.6 (`:1359`) |
| at M = 0.5 M_P β = 5 and 25, and at M = 10⁻³ M_P β = 1.2, the history fails after parking | §6.5 (`:1339`–`:1342`) | each is released and reaches T_CMB; "0.25 eV" was 0.25 meV | §6.6 (`:1395`–`:1397`) |
| "Every history of log 05 §5.5's grid parks … and reaches T_CMB" | §6.5 (`:1345`) | true of that grid; in the band 10⁻²⁵–10⁻¹³ M_P the code of that day failed, and 07b fixed it | §6.7 (`:1441`–`:1447`) |
| "Nothing is stored. The payload's `generations`, `margins` and `margin_warnings` are not read by any store" | §6.8 (`:1512`–`:1513`) | prompt 09 stores the record | §6.9 (`:1517`) |
| the M = 10⁻³, β = 1.2 release as 0.27 eV | `architecture-summary.md` §7.2's 06 note, §6.5 | 0.27 meV | §6.6 (`:1397`), `architecture-summary.md` §7.2's 07 note |

The drain at laboratory M is also not what README §8.0 gives: under P20 the converged drain is 0.1153,
0.2902 and 0.4219 of the first-impact ceiling at β = 1.2, 2 and 3 (log 05 §7), not 0.115, 0.218 and
0.261, which had gated floor reflections.

#### 6.11.3 What a reader of the code will find stale

Two statements in code are left as the campaign wrote them, and are listed in the log of this prompt:
the comment of `REFLECTIONS_KEY` (`ComputeTargets/ScalarHistory.py:75`) calls the count "elastic reflections
(the floor-triggered reflection model …)", though the count now includes unresolved reflections and at
α > 0 every reflection drains; and `integrate_history`'s verbose line prints "elastic reflection(s)"
(`:4024`) for the same count. A change of code corrects either, which this prompt does not make.

---

## 7. From scalar history to BBN via PRyMordial (`ComputeTargets/BBNData.py`)

This is the pipeline that converts a scalar-field history into light-element abundances
using the external PRyMordial code (imported as `PRyM.PRyM_init` / `PRyM.PRyM_main`).

### 7.1 What PRyMordial needs and what ChamPBH supplies

PRyMordial is run in its "new physics" (NP) mode (`PRyMini.NP_thermo_flag = True`), in which
the user supplies three callables describing an extra energy component beyond the Standard
Model plasma, as functions of temperature in MeV:

- `rho_NP(T)` — the NP energy density,
- `P_NP(T)` — the NP pressure,
- `drho_NP_dT(T)` — the temperature derivative of the NP density.

ChamPBH constructs these from the difference between the *actual* Jordan-frame Friedmann
budget of the chameleon model and the Standard-Model radiation+matter budget:

- `density_NP = 3 M_P² H_Jordan² − ρ_rad,Jordan (1 + f_m)` — i.e. whatever energy the
  modified expansion history implies beyond ordinary radiation and matter.
- `pressure_NP = −3 M_P² H_J² (1 + (2/3) Ḣ_J/H_J²) − w ρ_rad,Jordan` — obtained from the
  second Friedmann/acceleration equation, where `Ḣ_J/H_J²` is itself reconstructed from the
  Einstein-frame `Ḣ_E/H_E²` via the conformal-transformation chain (involving Ω', Ω'', π,
  and π′ = the field acceleration recomputed from the ODE RHS terms). This is a non-trivial
  frame-conversion of the acceleration.

### 7.2 The arcsinh/sinh transform — the key dynamic-range strategy for BBN

The NP density and pressure are **signed** quantities (the chameleon contribution can be
positive or negative) that span an enormous dynamic range across the BBN temperature window.
A plain log transform cannot represent a sign change, and a plain linear spline would lose all
precision at small values. The code therefore represents them through an **inverse
hyperbolic sine** transform:

- It splines `asinh(density_NP / MeV⁴)` and `asinh(pressure_NP / MeV⁴)` against `log(T/MeV)`.
- On evaluation, it inverts with `sinh(...)` to recover the physical value.

`asinh` behaves like `sign(x)·log|x|` for large |x| and like `x` for small |x|, so it
compresses the huge dynamic range like a log while remaining smooth and single-valued
through zero crossings — exactly the property needed for a signed quantity that spans many
decades. This is applied consistently to both ρ_NP and P_NP.

The derivative callback `drho_NP_dT` is built from the analytic derivative of the *asinh*
spline (`arcsinh_density_NP_MeV4_spline.derivative()`), then converted back to a derivative
of the physical density by the chain rule: `d ρ/dT = sqrt(1 + ρ̃²)/T · d(asinh ρ̃)/d log T`,
where `ρ̃ = sinh(asinh-spline)`. This keeps the supplied derivative exactly consistent with
the supplied density, avoiding the mismatch that finite-differencing would introduce and
that could destabilize PRyMordial's own thermodynamic ODEs.

### 7.3 Splining details and temperature window

- The NP splines are cubic (`_make_spline` forces `k=3`) and, as elsewhere, the (x, y) pairs
  are sorted by abscissa (`log(T/MeV)`) before splining. The samples are drawn only from the
  history points that fall inside the BBN temperature window `[T_min, T_max]`, with defaults
  `T_max = 100 MeV` and `T_min = 1e-4 keV` (bracketing PRyMordial's own working range, which
  begins around 10 MeV and ends near 1 keV).
- Monotonicity of the sampled `log(T/MeV)` is checked and warned upon (the temperature should
  be monotonically decreasing along the history).
- A precondition guard rejects the whole calculation up front (`{"failure": True}`) if the
  scalar integration did not run to low enough temperature: it requires
  `T_Jordan_stop ≤ 0.1 · T_BBN_spline_min`, i.e. the history must extend safely below the BBN
  window so the splines are never extrapolated during the BBN solve.

### 7.4 Boundary and defensive behaviour of the supplied callables

The `rho_NP`, `P_NP`, `drho_NP_dT` callbacks contain their own guards because PRyMordial
probes them at temperatures the caller does not control:

- **Negative temperatures** requested by PRyMordial (which it does transiently) return `0.0`
  harmlessly.
- Requests **above `T_max`** or **below `T_min`** raise `ComputationFailureError` (out of the
  splined support) rather than extrapolating.
- `OverflowError`/`ValueError` from the `sinh` inversion are caught and converted to
  `ComputationFailureError`.

**Note added 2026-09-30 (run-integrity prompt 02, item F).** Two finiteness guards were added;
nothing above is changed by them, and no callback's value changes for finite input.

- **The samples.** `build_NP_callbacks` refuses a non-finite sample of `log_T_MeV`,
  `density_ratio` or `pressure_ratio` with `ComputationFailureError`, before any spline is built.
  The message names the array, the first index and its T. `compute_BBN_data` already turns that
  exception into a failure row (`"BBN callbacks: ..."`). Before this prompt a non-finite ratio
  reached `make_interp_spline`, whose `ValueError` ("Array must not contain infs or nans") is
  not caught there and escaped the task; a NaN in `log_T_MeV` was reported as a
  monotonicity failure.
- **The temperature.** Each of the three callbacks raises `ComputationFailureError` for a
  non-finite T, before the negative-T guard. A NaN T passes both that guard (`T < 0` is False)
  and the domain check (both comparisons are False), and the callbacks returned NaN; `-inf`
  returned 0. A NaN new-physics value handed to PRyMordial made its high-T solve hang
  (`prompts/run-integrity/planning-probes/prymordial_solver_probe.py nan`: no return in 60 s).

Witness: `ComputeTargets/tests/test_bbn_solver_failures.py` (d). See
`prompts/run-integrity/logs/02-detect-bbn-solver-failures.md`.

### 7.5 Running PRyMordial and outputs

PRyMordial is imported *locally* inside the worker (with a comment noting the intent to avoid
leaking its module-level globals between Ray worker threads), verbose output is disabled, and
the NP-sector start temperature is set explicitly (`Tstart_NP = T_start / MeV_to_Kelvin`,
handling a unit inconsistency where PRyMordial's `T_start` is in Kelvin while everything else
is in MeV). By default the **small reaction network** is used (`small_network_flag = True`),
which is faster at the cost of Li-7 accuracy; this is configurable per run.

**Note added 2026-09-30 (production-readiness prompt 02, item P2).** The sentence above described
the intent, not what ran. `small_network_flag` is not a name PRyMordial reads; it reads
`smallnet_flag` (`PRyM/PRyM_init.py:111`) when the solve runs, so until this prompt every solve
used the **full** network whatever the switch said. `_configure_PRyMordial` now sets
`smallnet_flag = small_network`, and production (`main.py`, the `compute_BBN_data` default, the
`plot_by_beta.py` baseline and `tools/bbn_baseline.py`) passes `small_network=False`, the full
network. From `VERSION_LABEL = "2026.3.0"` the stored `small_network` describes the network that
ran. On the constant 0.08 ρ_SM fixture the small network moves ⁷Li/H by 1.0 %, D/H by 2.3e-4 and
Yp by 6.2e-5, in 5.2 s against 7.9 s (`ComputeTargets/tests/test_network_flag.py` (b)). See
`prompts/production-readiness/logs/02-wire-the-network-flag.md`.

`PRyMclass(rho_NP, P_NP, drho_NP_dT).PRyMresults()` is invoked inside a try/except that
converts any `OverflowError`, `ValueError`, or `ComputationFailureError` into a graceful
`{"failure": True}`. On success the code extracts and stores the primordial abundances
`Yp` (helium mass fraction), `D/H`, `He3/H`, and `Li7/H`, along with the small-network flag,
a pinned PRyMordial commit hash (`"bf24c3d"`, since PRyMordial lacks formal versioning), and
both the NP-construction and BBN-solve wall-clock times. The reconstructed NP density and
pressure (and the ratio `density_NP/ρ_rad,Jordan`) are stored per-redshift for later
inspection.

**Note added 2026-09-30 (run-integrity prompt 02, item F).**

- **What is now checked.** Until this prompt none of the eight `solve_ivp` calls in
  `PRyM/PRyM_main.py` checked its result, and the nuclear stages read the last point reached.
  A last solve that gave up after 1 % of its span returned D/H 5.0e-3 off, with no exception.
  Each of the eight calls is now followed by `_check_solve_ivp`, which raises
  `PRyMSolverFailureError` (defined in `PRyM/PRyM_main.py`) unless `sol.success`. The message
  names the stage, `sol.status`, `sol.message`, and the t reached against the target. The patch
  is marked at each site with a comment naming run-integrity prompt 02, and `PRyM_version` is
  `"bf24c3d+cham03+ri02"`. Under production's flags five calls run: thermodynamics (with NP),
  a(T), high-T n ↔ p, and the full network's mid-T and low-T solves. The Julia branches
  (`de.solve`) are not used, and are not patched.
- **Where the boundary is.** The PRyMordial call is factored into `_run_PRyMordial(callbacks,
  small_network)` (`ComputeTargets/BBNData.py`). It returns the abundances, or, for any
  `Exception` raised inside the `PRyMclass(...).PRyMresults()` call,
  `{"failure": True, "failure_reason": "PRyMordial: <Type>: <message>"}`. The `try/except` over
  three exception types described in the paragraph above is gone. `compute_BBN_data` returns that
  payload unchanged. Nothing outside the call gained an `except`: an exception from ChamPBH's own
  code outside PRyMordial still propagates, as a bug. `compute_SM_baseline` does not use the
  helper, and a failed baseline still raises. A `BaseException` such as `KeyboardInterrupt` is
  not caught.
- **What becomes a failure row.** A `solve_ivp` that did not succeed, in any stage
  (`PRyMSolverFailureError`); a `ComputationFailureError` from the callbacks, which PRyMordial
  calls (for example T outside the splined domain, or a non-finite T, §7.4); and any other
  `Exception` raised by PRyMordial or by the callbacks while it runs. Each is stored with
  its reason in `BBNData.failure_reason`. From `VERSION_LABEL = "2026.4.0"` a failed solve is
  stored as a failure, not as a success.

Witness: `ComputeTargets/tests/test_bbn_solver_failures.py` (a)–(c), (e). See
`prompts/run-integrity/logs/02-detect-bbn-solver-failures.md`.

### 7.6 Added 2026-10-02 (`science-readiness`, prompts 01 and 06): the BBN route as it now is

**What this supersedes.** The text above describes the interface at three earlier trees, and none
of it is edited:

- §7.2–§7.4 (the asinh/sinh transform, `_make_spline`'s sort, the `sinh` overflow path) were
  superseded in `review-remediation` prompt 04 (`eba4473`), which splined the ratios
  ρ_NP/ρ_R,J and p_NP/ρ_R,J and multiplied back by the thermodynamic ρ_SM, and refused a
  non-monotonic T_J rather than sorting it. §7.1's description of Ḣ_J/H_J² did not mention the
  Ω″π² correction that prompt added either.
- §7.1 (three callables, `NP_thermo_flag = True`, `pressure_NP`), the window and pre-check of §7.3,
  the three callbacks of §7.4, and §7.5's `PRyMclass(rho_NP, P_NP, drho_NP_dT)`,
  `Tstart_NP` and `PRyM_version` `"bf24c3d+cham03+ri02"` were superseded in `science-readiness`
  prompt 01 (`1bc8977`) and prompt 06 (`835aa79`). That is what this subsection describes.
- §9 item 2 (the delicate frame-conversion of Ḣ_J/H_J² "used for P_NP") has no subject: nothing
  computes p_NP now.

Figures are from `prompts/science-readiness/logs/` and README §6.1 of that campaign, on the trees
those name.

#### 7.6.1 What PRyMordial is given

PRyMordial receives **one** callable, `rho_NP(T)` (T in MeV, result in MeV⁴), built by
`build_rho_NP_callback` in `ComputeTargets/BBNData.py`:

- For each stored sample with `T_J ∈ [0.2 keV, 100 MeV]` (`T_BBN_keV_spline_min = 0.2`,
  `T_BBN_MeV_spline_max = 100`): `ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m)` and `r = ρ_NP/ρ_R,J`,
  with the **point** `H_J` of the sample. (The cell-mean `H_J²` of the withdrawn bounce averages
  is not used, §3.6.2.)
- `r` is splined, cubic and with no transform, against `ln(T_J/MeV)` on the stored samples.
  `T_J` must be strictly decreasing along the history; otherwise `ComputationFailureError`
  naming the first offending pair, and nothing is sorted.
- The callback returns `r(T) ρ_SM(T)`, with `ρ_SM = (π²/30) g_ρ(T) T⁴` from the cosmology's
  `G_rho` (`thermodynamic_rho_SM`; nothing splined). For finite in-domain input this is the same
  expression the earlier callback used.
- **Guards** (each a `ComputationFailureError`, which `compute_BBN_data` turns into a
  `"BBN callbacks: …"` failure row; prompt 01): fewer than `MIN_BBN_SAMPLES = 4` samples in the
  window (before, a `ValueError` from `make_interp_spline` escaped); a non-finite sample; a
  non-finite T; a T outside `[T_min, T_max]`; and a non-finite *value* `r(T) ρ_SM(T)` at a finite
  in-domain T (before, `nan` was returned to PRyMordial). A negative T returns 0.
- **The window and the pre-check.** The floor moved from 0.1 eV (`T_BBN_keV_spline_min = 1e-4`)
  to **0.2 keV** in prompt 06 (`835aa79`). PRyMordial's lowest callback query is **0.3628 keV** and
  its highest 10 MeV, with no negative T (`planning-probes/prym_callback_domain.py`, re-run on
  prompt 01's tree: 1 944 calls, 140 below 1 keV, 307 below 3 keV; and
  `ComputeTargets/tests/test_bbn_spline_floor.py` (b): lowest positive T 0.3628 keV, floor 0.2 keV).
  The pre-check rule is unchanged, `T_Jordan_stop ≤ 0.1 × T_BBN_spline_min`, so **a history must
  now reach 20 eV** (it was 0.01 eV): `--T-stop-GeV 1e-8` passes, `1e-7` fails with
  `"pre-check: T_Jordan_stop=0.1 keV is more than 0.1*T_BBN_spline_min=20 eV"`
  (`test_bbn_spline_floor (a)`). BBN abundances on β = 2 at `M = 0.5` and `10⁻³` are unchanged by
  the narrowing, to every printed digit (log 06).

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.2).** `--T-stop-GeV` no longer
> exists, so the advice above has no option to follow. Every stored history ends at the cosmology's
> T_CMB, and `ScalarHistory.T_Jordan_stop` (`ComputeTargets/ScalarHistory.py:1969`) is a property at
> that temperature, which is what `compute_BBN_data`'s pre-check reads. The rule
> `T_Jordan_stop ≤ 0.1 × T_BBN_spline_min` is unchanged, and T_CMB = 2.7255 K, about 2.3 × 10⁻⁴ eV, is
> far below the 20 eV it asks for. The option is refused by `RemovedOption` (exit 2);
> `tools/history_and_bbn.py --fixed-normalization NORM PHI_REF_MP --T-stop-GeV T` runs an untuned history
> to any T.

#### 7.6.2 The Hubble-only patch, and why `p_NP` and `T_NP` are gone

In the Jordan frame the plasma is minimally coupled: its energy is conserved and T_J follows the
Standard-Model temperature law. The scalar field reaches the nuclear network only through the
Jordan-frame expansion rate. The vendored PRyMordial is patched so that this is all it does:
`PRyM_init.NP_hubble_flag`, when set, adds `PRyMthermo.rho_NP(Tg)` to the total density in
`Hubble`, and nothing else reads it. `_configure_PRyMordial` sets `NP_thermo_flag = False` and
`NP_hubble_flag = True` and raises `AssertionError` unless `NP_nu_flag`, `NP_e_flag` and
`julia_flag` are false and `compute_bckg_flag` is true.

The route it replaces, `NP_thermo_flag`, put ρ_NP into `Hubble` as well, but also added
`−3H(ρ_NP + p_NP)` and `dρ_NP/dT` to the plasma's `dT_γ/dt` and integrated a third variable
`T_NP` that no output reads. The plasma then obeyed the Standard-Model equation only through a
cancellation between two spline-derived terms, ρ̇_NP = −3H(ρ_NP + p_NP) with p_NP built from Ḣ_J,
accurate to about `r · 1.2×10⁻³` (`numerical-methods-for-paper.md` §4). So:

- **`p_NP` is gone** because its only consumers were those two terms. `jordan_Hdot_over_H2`, the
  `ODEPolicy` call that gave π′, the `pressure_NP` field of `BBNDataValue` and its
  `pressure_NP_MeV4` column, the derivative callback `drho_NP_dT`, `build_NP_callbacks` and
  `NPCallbacks` are removed, and so are the |p_NP| and w_NP panels of `plot_ScalarModel.py`'s BBN
  figure (now three panels).
- **`T_NP` is gone** because with `NP_thermo_flag` off nothing integrates it: the thermodynamic
  `solve_ivp` has two components (T_γ, T_ν), not three (`test_bbn_solver_failures (f)`: first
  `y0` of length 2, against 3 on the earlier tree), and `rho_NP` is called only from `Hubble`
  (callers `{'Hubble': N}`; `{'Hubble': 2029, 'dTgdt': 903, 'N_eff': 1}` before). The `cham03`
  patch, which made `dTNPdt` return 0 so that an oscillating ρ_NP could finish, is reverted: the
  upstream body is back, its singularity in a branch nothing enters.
- **What was measured.** On β = 2 at `M = 0.5` and `10⁻³` the earlier route did not finish in
  900 s in the planner's unloaded probe (campaign README §0.3), where the Hubble-only route takes
  about 10 s. The Hubble-only route's abundances on the roster (full network; logs 01, 03, 05):

| β, M | Yp | D/H ×10⁵ | ³He/H ×10⁵ | ⁷Li/H ×10¹⁰ | BBN wall |
|---|---|---|---|---|---|
| 2, 0.5 | 0.249229266 | 2.560889654 | 1.054673338 | 5.241925487 | 9.6 s |
| 2, 10⁻³ | 0.2467606164 | 2.463862263 | 1.042634494 | 5.409240365 | 10.0 s |
| 1.6, 10⁻⁵ | 0.2468788501 | 2.4647705 | 1.042121506 | 5.419865323 | 9.7 s (loaded) |
| 2, 10⁻⁵ | 0.2467016048 | 2.46477019 | 1.042619141 | 5.407992384 | — |

Walls are log 01's (unloaded) for the two β = 2 rows at `M = 0.5` and `10⁻³`, and log 03's (loaded)
for β = 1.6. The β = 2, `M = 10⁻⁵` row is log 05's point-input run on `a522005`, before the spline floor moved
(log 06 shows that the move changes nothing, to every printed digit, on the two β = 2 rows it
re-ran).

- With ρ_NP ≡ 0 the route is plain PRyMordial exactly (`==` on all four abundances,
  `test_prym_passenger (b)`). The constant 0.08 ρ_SM family, small network, gives Yp 0.2536690816,
  D/H 2.6481673, against the planner's Hubble-only reference to 1.8e-10 and 4.5e-11 relative
  (`test_prym_passenger (c)`).
- **A sensitivity that is PRyMordial's.** Run through the builder, the constant-ratio callback
  differs from the exact constant family by a few ulp, and PRyMordial moves D/H by 1.06e-4 (Yp
  2.8e-6) under that: `test_bbn_callbacks (h)` therefore compares the builder's callback on the new
  route against the same callback on the earlier tree's Hubble-only route, at 1e-6, and prints the
  1.06e-4 without bounding it (log 01, Deviation 3; the user accepted it on 2026-10-01). It is
  another instance of `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`.

#### 7.6.3 The wall-clock limit and the output checks

- **The limit.** `PRyMclass(…, wall_clock_limit=None)`; with a limit, every `fun` and `jac` of the
  eight `solve_ivp` calls is wrapped to check `time.monotonic()` against the deadline, and the
  deadline is also checked before each of the eight calls. Past it, `PRyMWallClockLimitError(stage,
  elapsed, limit)`, which `_run_PRyMordial` returns as a failure row
  (`"PRyMordial: PRyMWallClockLimitError: …"`, naming the stage). The default is
  `DEFAULT_BBN_WALL_CLOCK_LIMIT = 600.0` s (an unloaded full-network solve takes about 10 s; the
  source quoted 34–120 s on a loaded ten-core machine); `main.py --bbn-wall-clock-limit SECS`
  overrides it and `0` disables it; `compute_SM_baseline` has no limit and still raises. At
  `1e-3` s the constant family fails in 0.001 s, in stage `'thermodynamics (no NP)'`
  (`test_bbn_solver_failures (g)`). A timeout is a stored failure like any other: final within a
  `VERSION_LABEL`, retried by `--retry-failed-bbn`.
- **The output checks.** `_check_abundances` rejects a result unless all four abundances are
  finite, `0 < Yp_BBN < 0.5` and `DOverH`, `He3OverH`, `Li7OverH` are `> 0`; the failure row's
  reason is `"PRyMordial output: <every failing value>"`, truncated to 256
  (`test_bbn_solver_failures (h)`). Before, such a result was stored as a success.
- **The stage name** `'thermodynamics (no NP)'` is now the stage whose `Hubble` carries ρ_NP; it
  keeps its name because `_check_solve_ivp` and the test of distinct stage names read it.

#### 7.6.4 The vendored patches, for a PRyMordial upgrade

`PRyM/` is a copy of PRyMordial pinned at `bf24c3d`. `PRYM_VERSION` is `"bf24c3d+ri02+sr01"`
(`ri02` is `run-integrity` prompt 02's `_check_solve_ivp` hunks, §7.5; `sr01` is this campaign's
prompt 01). The `cham03` hunk is **not** to be re-applied: it is reverted. Every `sr01` hunk
carries the comment "ChamPBH science-readiness prompt 01". Line numbers are those after prompt 01
(`1bc8977`); a PRyMordial upgrade re-applies them from this table:

| file:lines | hunk |
|---|---|
| `PRyM_init.py:77–80` | `NP_hubble_flag = False`, after `NP_e_flag`, with a three-line comment |
| `PRyM_main.py:35–68` | `class PRyMWallClockLimitError(Exception)` with `__init__(self, stage, elapsed, limit)` (attributes `stage`, `elapsed`, `limit`; message `"wall-clock limit of %.6g s exceeded in stage '%s': %.6g s elapsed"`); `_check_wall_clock(stage, t_start, limit)` (raises once `time.monotonic() - t_start > limit`; nothing if `limit is None`); `_limited(fn, stage, t_start, limit)` (returns `fn` itself if `limit is None`, else a wrapper that calls `_check_wall_clock` before `fn`) |
| `PRyM_main.py:72–81` | `PRyMclass.__init__(self, my_rho_NP=None, my_p_NP=None, my_drho_NP_dT=None, my_delta_rho_NP=None, wall_clock_limit=None)`; `wall_clock_start = time.monotonic()` is the first statement |
| `PRyM_main.py:141–143` | in `Hubble`: `if PRyMini.NP_hubble_flag: rho_tot += PRyMthermo.rho_NP(Tg)`, after the `NP_thermo_flag` line. Nothing else reads the flag |
| `PRyM_main.py:211–219` | `dTNPdt` back to the upstream body (identical, by `diff`, to the function at `6d3ecfa`); the `cham03` comment and the commented-out lines are gone |
| eight `solve_ivp` sites | before each, `_check_wall_clock("<stage>", wall_clock_start, wall_clock_limit)`; in each, `fun` → `_limited(fun, "<stage>", …)`, and `jac=J` → `jac=_limited(J, "<stage>", …)` where a `jac` is passed. The stages are `_check_solve_ivp`'s: `thermodynamics (with NP)` (`:268–279`), `thermodynamics (no NP)` (`:320–331`), `a(T)` (`:518–522`), `high-T n <-> p` (`:683–689`), `mid-T nuclear network (small)` (`:1127–1147`, with `jac`), `mid-T nuclear network (full)` (`:1219–1239`, with `jac`), `low-T nuclear network (small)` (`:1328–1348`, with `jac`), `low-T nuclear network (full)` (`:1408–1428`, with `jac`) |

The Julia branches are not patched (`julia_flag` is checked false). `N_eff` is not changed, and
with `NP_thermo_flag` off it no longer counts ρ_NP; ChamPBH neither stores nor reads it. No
reaction rate, network, tolerance, `T_start`, `T_end`, `t_end` or sampling changed. `black` was
applied to the new hunks of `PRyM_main.py` only (it was black-clean before); `PRyM_init.py` is not
black-clean and was not reformatted. Source of the table: log 01, "What shipped".

#### 7.6.5 Other things recorded

- The Standard-Model baseline through the new route is unchanged to every printed digit
  (`test_bbn_callbacks (i)`, full network: Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273,
  ⁷Li/H 5.423441017).
- `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`: until measured, a PRyMordial failure
  on this input cannot be attributed to the true H rather than to the spline (§3.6.2).

  > **Note, 2026-11-01.** Measured and closed (§3.6.2's note of this date;
  > `bbn-ratio-spline-2026-11-01/`). A monotone interpolant that cannot overshoot gives the same yields, and
  > all 40 solves of the measurement succeeded.
- The pre-check now accepts a history that stops as high as 20 eV (`--T-stop-GeV 1e-8` passes;
  it required 0.01 eV, `1e-11` GeV, before). That is what makes the source's physical-`M`
  cross-check possible with `--T-stop-GeV 1e-8`; a history that runs to `T_CMB` passes under
  both. A history that stops above 20 eV stores a pre-check failure row.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.2).** `--T-stop-GeV` is gone, and
> no stored history stops above T_CMB (the note at §7.6.1), so the pre-check cannot fail a stored
> history. An untuned history stopped higher is run only by
> `tools/history_and_bbn.py --fixed-normalization … --T-stop-GeV`, whose BBN stage the pre-check
> refuses, as before.

### 7.7 Added 2026-10-03 (`bbn-tolerance`, prompts 01–03): the network, the low-T tolerance, and what they leave

Added by `bbn-tolerance` prompt 03. Nothing above this heading has been changed; where it is
superseded, this section says so by statement. The campaign
([`prompts/bbn-tolerance/README.md`](../prompts/bbn-tolerance/README.md); board
[`IMPLEMENTATION_STATE.md`](../prompts/bbn-tolerance/IMPLEMENTATION_STATE.md)) started from a brief
that traced 11 low-T failures of the 2026.6.0 science run, and a D/H scatter of up to 2.2×10⁻³, to
PRyMordial's low-temperature nuclear network. It measured the mechanism and the tolerance (log 01,
tree `95da274`), measured the small network (log 01c, `893a5b1`), and then moved production to the
small network, set both low-T tolerances and added a warning (log 02, `2bc124b` plus its diff; commit
`5a72871`). Prompts 01b and 02b are not about PRyMordial's numerics: 01b corrects the kick-threshold
overlay (§7.7.8), 02b makes `tools/history_and_bbn.py` follow `main.py`.

**This supersedes**, by statement and not by edit:

- **§7.5's note of 2026-09-30**, that production passes `small_network=False`, the full network.
  Production runs the **small** network (§7.7.1). The note's measurement of the network offset on the
  constant 0.08 ρ_SM fixture is unaffected.
- **§7.6.4's `PRYM_VERSION` `"bf24c3d+ri02+sr01"`.** It is `"bf24c3d+ri02+sr01+bt02"`. §7.6.4's
  closing sentence, that no tolerance changed, describes the `sr01` hunks and stays true of them;
  this campaign changes two tolerances (§7.7.2, §7.7.7). `VERSION_LABEL` is `"2026.6.0"`, unchanged.
- **§7.6.5's baseline** (Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017) and
  §7.6.2's table of abundances. They were measured on the full network at the default low-T
  tolerance, and were right for that tree. On production's network the baseline is Yp 0.2468802117,
  D/H 2.458287893, ³He/H 1.041932695, ⁷Li/H 5.486373007 (small network, `rtol` 1e-6; §7.7.5).
- **§7.6.3's "an unloaded full-network solve takes about 10 s".** A small-network solve at the
  production tolerance takes 8.8 s (SM baseline) to 10.1 s (a history) on this machine (§7.7.6).

Figures are from `prompts/bbn-tolerance/logs/`, on the trees named. D/H and Yp spreads are
(max − min)/median over the three input variants `prod`, `pert12` and `pert9` of one history: the
production callback on the stored ratio grid, and the same on `r × (1 + 10⁻¹²)` and
`r × (1 + 10⁻⁹)`. That is a measure of PRyMordial's response to ulp-level input changes, not of its
accuracy.

#### 7.7.1 The network

Production runs PRyMordial's **small** (12-reaction) network. It is selected by one name,
`BBN_SMALL_NETWORK = True`, assigned in `main.py`'s `run_pipeline` (`main.py:801`) and read once, in
the BBN payload (`:807`) and the warning call (`:841`). `plot_by_beta.py` has its own module-level
`BBN_SMALL_NETWORK = True` (`:76`), which must match `main.py`'s so that the Standard-Model baseline
it draws is on the network the data were made on. The defaults of `compute_BBN_data`,
`BBNData.compute`'s two payload fallbacks, `tools/bbn_baseline.py` and `tools/history_and_bbn.py` also
say `True`, and `ComputeTargets/tests/test_network_flag.py` (c) holds them in step. The exception is
`tools/bbn_from_store.py`, which keeps the full network as its default because the reproduction
commands of logs 01 and 01c depend on it.

*Why.* The full network fails on about 1 % of histories. On the 2026.6.0 store 11 of 684 φ\* = 5
histories ended in `PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)'`
(§7.5), at T_J just above 1 keV. The cause (log 01, item 6) is one rate, `Li7dLi8p_bkwrd`
(`PRyM/PRyM_nuclear_net63.py:1142–1147`; identical in upstream `bf24c3d`): the reverse rate of
Li8(p,d)Li7 is α·exp(γ/T9) times a global quadratic spline of the forward-rate table. Near
T9 = 0.0116 the table is 1e-264 to 3e-241, the spline rings in sign at about 1e-54, and the product
reaches |1.45e39|. BDF's Newton iteration then fails at every step size, because the Jacobian it holds
was refreshed 10⁴ s earlier and has ∂f_Li8/∂Y_Li8 of opposite sign. It is not the error test. **No
tolerance removes it**: in log 01's scan it appeared at `rtol` 1e-6 and 1e-8, at both per-species
`atol` settings, and once in 109 solves at 1e-5. The small network has no Li8. It never failed in 340
solves (T1 and T2 of log 01c), and it completes all 165 solves of the 11 histories that fail on the
full network, the default tolerance included.

The ruling behind this (README §0.2 U3): the lithium abundance is not used for constraints, and what
matters is Yp and D/H computed reliably. **⁷Li/H from the small network is less reliable and is not
used.**

*How the full network is still selected.* By editing that one name to `False` in `main.py` (and in
`plot_by_beta.py`). There is deliberately no command-line flag (README §0.2 U4). `BBNData` treats
PRyMordial as a black box, so that another BBN code could be swapped in, and a first-class small/full
switch would tie client code to a PRyMordial concept; a change of code is handled by the versioning
mechanism (`PRyM_version`). The comment at the assignment in `main.py` says why the small network is
the default.

#### 7.7.2 The low-T tolerances

Upstream `bf24c3d` passes no `rtol` to either low-T `solve_ivp` call (`method='BDF', jac=…,
atol=1.e-11` small; `atol=1.e-15` full; read from GitHub, log 01 item 10; upstream `main` has not
changed it). SciPy's default `rtol = 1e-3` therefore applied to the stage, where the other six calls
pass `rtol=1e-6, atol=1e-9`. As patched:

| call | `rtol` | `atol` | was |
|---|---|---|---|
| low-T, small network | **1e-6** | 1e-11 (unchanged) | none passed: 1e-3 |
| low-T, full network | **1e-5** | 1e-15 (unchanged) | none passed: 1e-3 |

The small network's setting is the one P11's rule selects (§7.7.3). The full network's 1e-5 is log
01's provisional setting, applied for anyone who selects that network. It does not remove the Li8
failures. No other stage's tolerance, no rate, no Julia branch and not `_check_solve_ivp` was
changed, and **a partial solve is never accepted**: every failure of the 11 was at 96–99 % of the
stage's end time, after Yp and D/H had frozen, and is still stored as a failure row (README §0.2 P8).

#### 7.7.3 The scans

These are the measurements the settings were chosen from. The rules are criteria for choosing our own
parameters, not bounds on PRyMordial, and no test asserts them (README §0.2 P3, P11).

**The full network** (log 01, S1: 16 histories × 3 variants + the SM baseline = 49 solves per
setting, run 9 at a time; cost serially, under moderate load):

| low-T `rtol` | failed solves of 49 | D/H spread over variants: max (history) / median | SM D/H ×10⁵ | serial cost, SM / control, against the default |
|---|---|---|---|---|
| default (1e-3) | **11** (the 11, `prod`) | 2.83e-3 (β 1.2, M 10⁻³) / 1.45e-3, over the five controls | 2.462251065 | 1.00 / 1.00 (6.39 s / 7.42 s) |
| 1e-4 | 0 | 1.40e-4 (β 2.1, M 0.1) / 8.43e-5 | 2.45820648 | 1.28 / 1.37 |
| **1e-5** | 0 (1 of 109 solves at 1e-5 failed in the Yp-floor runs) | 1.22e-4 (β 2, M 10⁻⁵) / 4.09e-5 | 2.458895152 | 2.48 / 2.23 |
| 1e-6 | **1** (β 1.1, M 0.03, `prod`) | 1.06e-4 (β 2, M 10⁻⁵) / 1.76e-5 | 2.458947441 | 4.11 / 3.74 |
| 1e-8 | **1** (β 1.2, M 10⁻³, a control) | 1.04e-4 (β 2, M 10⁻⁵) / 2.03e-5 | 2.458917971 | 10.58 / 8.96 |

The SM baseline converges: D/H is 2.458895, 2.458947 and 2.458918 at 1e-5, 1e-6 and 1e-8, within
2.1e-5; **the default's 2.462251 is 1.35e-3 above them**, which is the default tolerance's own error
on the baseline. Log 01's rule P3 therefore selected nothing (its criterion 3 measured distance from
that default), and the campaign stopped, to be re-planned around the small network.

**The small network** (log 01c, T1: the same 49 solves per setting; T2: 95 solves of a breadth
sample at 1e-6, every 10th φ\* = 5 history in (M, β) order and every φ\* ≠ 5 history; **no solver
failure in any of the 340 solves**). P11's four criteria:

| low-T `rtol` | 2. D/H spread: max (history); histories missing the rule | 3. against `rtol` 1e-8: max D/H; max Yp; inputs missing 1e-4 / 1e-5 | 4. cost, SM / control, against the full network at its default | P11 |
|---|---|---|---|---|
| default (1e-3) | 1.91e-3 (β 1.2, M 10⁻³); 16 of 16 | 1.77e-3; 3.2e-5; 17 of 17 | 0.67 / 0.71 | fails 2, 3 |
| 1e-4 | 6.21e-4 (β 1.7, M 0.03); 9 | 5.4e-4; 3.8e-6; 15 of 17 | 0.75 / 0.78 | fails 2, 3 |
| 1e-5 | 1.14e-4 (β 2.12, M 10⁻⁵); 1 | 1.57e-4; 6.0e-7; 13 of 17 | 0.94 / 0.93 | fails 2, 3 |
| **1e-6** | 1.51e-4 (β 2.4, M 10⁻⁵), 1.46× its own 1.03e-4 at 1e-8; 0 | 5.5e-5; 2.1e-7; 0 | **1.15 / 1.13** | **meets all four** |
| 1e-8 | 1.03e-4 (β 2.4, M 10⁻⁵); 0 | the reference | 1.56 / 1.46 | meets all four |

Criterion 1 (no failed solve) held at every setting. Criterion 2 is "below 1e-4, or at most 1.5× the
history's own spread at 1e-8", the second clause allowing for a floor another stage sets. P11 takes
the largest `rtol` that meets all four: **1e-6**.

#### 7.7.4 The residuals at the production setting

Measurements, with provenance (README §0.2 P9). They are properties of PRyMordial, not bounded in a
test and not issues. Small network, `rtol` 1e-6, `atol` 1e-11: log 01c, reproduced in every printed
digit by log 02 on the patched tree (49 of 49) and again by prompt 03 (17 of 17; log 03).

- **The D/H spread over the three variants.** Median 4.4e-5 over the 16 histories; 15 of 16 are below
  1e-4. The maximum is **1.51e-4**, on β = 2.4, M = 10⁻⁵, and that history is above 1e-4 even at
  `rtol` 1e-8 (1.03e-4): the low-T stage does not set that floor. Log 01 found a floor of the same
  kind on the full network, on β = 2, M = 10⁻⁵, and traced it to the thermodynamic stage (it fell
  from 1.22e-4 to 2.4e-5 with that stage tightened alone). The small-network floor was not traced to
  a stage.
- **The Yp spread.** Median 1.5e-5, maximum 4.3e-5 (β = 2.4, M = 10⁻⁵). **No low-T `rtol` moves
  it**: the same figures at 1e-5, 1e-6 and 1e-8. On the full network (log 01 item 9) no single other
  stage removes it either; tightening the thermodynamic, a(T), high-T and mid-T stages together,
  from 1e-6 to 1e-9, cuts it by about 100×, to 1.4e-7 to 4.6e-7.
- **The convergence error against 1e-8**, relative, on all 17 inputs (the SM and each history's
  `prod`): D/H at most 5.5e-5 (median 2.6e-5; β = 1.6, M = 10⁻⁵ is the largest), Yp at most 2.1e-7.
  Only the low-T stage differs between the two solves, so this isolates its error.
- **A bias of another stage, measured on the full network only.** Tightening the a(T) stage from its
  `rtol` 1e-6 moves D/H by about +4.5e-4 on the control β = 1.6, M = 10⁻³ (2.461632 to 2.462733;
  log 01 item 9). That is larger than the spreads above. It was not measured on the small network
  and is not removed.
- **The default tolerance's error, which the stored results carry.** At the default low-T `rtol`
  D/H is off by up to 1.77e-3 (median 7.6e-4) against 1e-8 on the small network (log 01c), and by
  1.35e-3 on the full network's SM baseline (log 01).

#### 7.7.5 The offset between the networks, and the baseline

Measured as a difference (log 01c row 6, P12: (small − full)/full per input, with log 01's full-network
rows), not bounded:

| low-T `rtol` (both networks) | inputs | Yp: range of the difference | D/H: range of the difference |
|---|---|---|---|
| 1e-5 | 17 | −3.17e-5 to +2.11e-5 | −4.35e-4 to −3.23e-4 |
| 1e-6 | 16 (the full network failed β 1.1, M 0.03) | −3.21e-5 to +2.09e-5 | −3.58e-4 to −2.52e-4 |
| 1e-8 | 16 (the full network failed β 1.2, M 10⁻³) | −3.19e-5 to +2.10e-5 | −3.06e-4 to −2.35e-4 |

The small network gives D/H **2.4×10⁻⁴ to 3.6×10⁻⁴ lower** than the full network on every input at a
converged setting, never higher, and Yp within ±3.2×10⁻⁵, which is Yp's own spread. On the SM baseline
the D/H difference is −2.82e-4 at 1e-8. The shift is a systematic, nearly the same on every history
(a range of about 1.1e-4 across the 16 at 1e-6). The two networks also differ in ⁷Li/H by about 1 %
(on the constant 0.08 ρ_SM fixture, `test_network_flag (b)`: small at 1e-6 5.186233632, full at 1e-5
5.133203422, a shift of 1.033e-2; on the SM baseline, small 5.486373007 against full 5.428643941 at
1e-5, log 01).

Every BBN row of a refreshed store, and the SM baseline, moves by this offset and by the removal of the
default tolerance's error. The SM baseline moves from D/H 2.462251065 (full network, default; §7.6.5)
to 2.458287893 (small, 1e-6), a fall of 1.6×10⁻³ in D/H: the 1.35e-3 error of the default (§7.7.3)
plus about 2.8e-4 of network offset. Yp moves from 0.2468872958 to 0.2468802117, a relative 2.9e-5, which is
within the range of the Yp offset above.

**The re-pinned constants** (README §0.2 P7, P16; log 02 "What shipped" (e)). Each was re-derived from
its stated provenance (the "honly" route on `7b518c9`), at the new settings, and re-pinned with the old
value kept in a comment; **no bound was loosened**. For example `CONST_HONLY_SMALL_YP` is now
0.253669508 (was 0.2536690816) and `CONST_HONLY_SMALL_D_OVER_H_E5` 2.649288446 (was 2.6481673),
both at bound 1e-6, and `CONST_HONLY_FULL_YP` 0.2536731562 (was 0.2536754614) and
`CONST_HONLY_FULL_D_OVER_H_E5` 2.649990509 (was 2.648809882), at bound 1e-5; `README_BASELINE` in `test_bbn_callbacks (i)` is now the small-network SM
baseline above, at bound 1e-4, and the test calls `compute_SM_baseline(True)`.

#### 7.7.6 The cost

Serial medians of three repeats after a discarded warm-up, one process, in seconds (log 01c, row 5,
at 1-minute load 4.2–9.4; log 02, row 7, at 5.9–6.4). The ratio is the quotable figure.

| setting | SM baseline | control β 1.6, M 10⁻³, `prod` | against the old production (full, default) |
|---|---|---|---|
| **old production**: full, default | 7.61 | 8.86 | 1.00 |
| **production now**: small, 1e-6 | 8.79 (log 02: 8.76) | 10.05 (log 02: 10.11) | **1.15 / 1.13** |
| small, default | 5.09 | 6.30 | 0.67 / 0.71 |
| small, 1e-8 | 11.85 | 12.95 | 1.56 / 1.46 |
| full, 1e-5, now the full network's setting (log 01, another session) | 15.82 | 16.54 | 2.48 / 2.23 of log 01's own default (6.39 s / 7.42 s) |

So the small network at its production setting costs about the same as the full network did at the
default tolerance (1.13–1.15×). The campaign's cost target (README §0.2 P11 (4)) ranked last, behind
getting results at all and getting correct results.

#### 7.7.7 The `PRyM/` hunks, for an upgrade

Two hunks, each a marker comment and one keyword argument, in `PRyM/PRyM_main.py`. Line numbers are
those at the tip of this campaign (`5a72871` onward). Each marker is the line in the file's style
"ChamPBH bbn-tolerance prompt 02". §7.6.4's table is extended by this note, not edited.

| file:lines | hunk |
|---|---|
| `PRyM_main.py:1349–1350` | in the **small** network's low-T `solve_ivp`, after `jac=_limited(Jacobian, …)` and before `atol=1.0e-11`: the marker comment, and `rtol=1.0e-6,` |
| `PRyM_main.py:1431–1432` | in the **full** network's low-T `solve_ivp`, after `jac=_limited(Jacobian_LT, …)` and before `atol=1.0e-15`: the marker comment, and `rtol=1.0e-5,` |

The Julia branches are not patched (`julia_flag` is asserted false). The Li8 rate and `dYB8dtLT` are
not patched (§7.7.8). `PRYM_VERSION` carries `+bt02` for these two hunks.

#### 7.7.8 Other things recorded

- **A store that was not refreshed warns, and is still used.** `BBNData` lookups are keyed on
  `VERSION_LABEL` and ignore `PRyM_version` and `small_network`. `main.py` and `plot_by_beta.py` call
  `pipeline_selection.warn_foreign_bbn_provenance`, which prints one warning naming each foreign
  (`PRyM_version`, network) with its count, counting failure rows as "not stored", and ending with the
  refresh route. No row is skipped, filtered or recomputed because of it. There is no `VERSION_LABEL`
  bump: `ScalarModel` and `AdiabaticHistory` rows do not depend on PRyMordial, and a bump would orphan
  all 710 histories. How to refresh BBN on the science store, without recomputing a history, is in
  `review-remediation-verification.md` §4.11.
- **Open, on PRyMordial's side, not patched.**
  `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]` (the cause of the full network's failures; it
  stays for anyone who selects that network) and
  `[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]` (`dYB8dtLT`, `PRyM_nuclear_net63.py:1480`,
  unpacks Y in the superseded species order; the effect was not measured and is probably negligible,
  since B8 stays below 1e-16 and enters no reported abundance).
- **The `T_deliver` figure's threshold curve** (prompt 01b, `9e51437`). `kick_threshold_curve` returned
  1/√(3Σ), the first-order form; it now returns β_th = √((2 + Σ)/(6Σ)) = 1/√(3Σ_eff), the paper's
  reachability condition. Σ is unchanged (`cosmology.w` is the `Xav_EOS_data.csv` spline in both the
  integration and the overlay). The curve's minimum over [0.05, 50] GeV moves from 1.02945 to 1.10745,
  both at 0.18202 GeV, where Σ = 0.31453. Test (d) was changed and test (d2) added; both fail on the
  old source.

---

## 8. Distributed execution and its numerical implications

Every expensive computation (`compute_scalar_history`, `compute_adiabatic_values`,
`compute_BBN_data`) is a Ray remote task. This is primarily an infrastructure concern, but it
has one numerical-hygiene consequence worth noting: results are computed once and persisted
in a sharded SQLite datastore keyed (among other things) by tolerances and solver, so a
parameter-survey point is never silently recomputed at a different accuracy. The BBN and
adiabatic stages consume a lightweight `ScalarHistoryProxy` (a Ray object reference) so that
the large scalar history is not repeatedly serialized across workers.

---

## 9. Possible additional numerical categories worth flagging to the author

These were not in the original list but appear in the code and may deserve mention in the
publication:

1. **Event localization / root-finding precision.** The physically important times (T_stop
   crossing, bounce-region boundaries, reflections) are found by SciPy's built-in event
   root-finder (a bracketed Brent-type solve on the dense interpolant). The accuracy of the
   region-switching and of the final z = 0 endpoint depends on this, and it interacts with
   `rtol`/`atol`.
2. **Frame-conversion of second-order quantities.** The Jordan-frame acceleration `Ḣ_J/H_J²`
   used for `P_NP` is a genuinely delicate conformal-transformation computation (products of
   Ω', Ω'', π, and the field acceleration); its numerical conditioning may merit comment.
3. **The `E → 0` and `G > 0` clamping policy.** The decision to treat small negative ρ_rad/H²
   as zero (rather than error) is a physical-regularization choice that affects late-time
   behaviour and could be described explicitly.
4. **Endpoint re-mapping z ↔ N.** The integration terminates at the T_stop event and the
   final e-fold is *identified* with z = 0 (`largest_z = exp(final_N) − 1`); any small error
   in where the T_stop event fires maps into a small global redshift calibration, which is a
   category (calibration/normalization error) not otherwise listed.

> **Note, 2026-10-26 (`jordan-normalization`, prompt 10; §3.7.9).** Items 1 and 4
> describe an integration that stops at a `T_stop` event and a label `z = exp(final_N) − 1`. Every
> stored history stops at T_CMB (`tune_history`, `ComputeTargets/ScalarHistory.py:1628`), the crossing
> found by the root-find of §3.5.1, and that end is the zero of the axis, ln(1 + z_E) = 0: a sample at
> N_backward is at N_forward = final_N − N_backward (`:1296-1297`), and no `exp` of `final_N` is formed. An
> error in where the crossing is located therefore moves every sample's N_forward and is not a
> calibration of a z.
