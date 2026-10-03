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

A robustness cascade is built in (`compute_scalar_model`): the solver list is
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

---

## 4. Splines: where they are used and how boundary/dynamic-range issues are handled

Splines appear at three distinct places, each with its own boundary strategy.
All are `scipy.interpolate.make_interp_spline` (interpolating B-splines, cubic `k=3` by
default).

### 4.1 Output-history splines for the scalar model (`ScalarModel._create_functions`)

Each stored history quantity (φ, π, log ρ_rad in both frames, log f_m, H in both frames,
log T_Jordan, g*_ρ, g*_s, Σ) is turned into a callable spline over redshift. Two hygiene
strategies are applied:

- **Sorting before splining.** The (z, value) pairs are explicitly sorted by z before the
  spline is built, because the integration produces samples from high to low z and
  `make_interp_spline` requires a strictly increasing abscissa.
- **Log abscissa.** Splines are built against `log(1+z)` rather than z (`log_z=True` in the
  `ZSplineWrapper`), matching the logarithmic spacing of the sample grid and the e-fold
  time variable, so the knot spacing is uniform in the natural variable.

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
- The pre-check now accepts a history that stops as high as 20 eV (`--T-stop-GeV 1e-8` passes;
  it required 0.01 eV, `1e-11` GeV, before). That is what makes the source's physical-`M`
  cross-check possible with `--T-stop-GeV 1e-8`; a history that runs to `T_CMB` passes under
  both. A history that stops above 20 eV stores a pre-check failure row.

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

Every expensive computation (`compute_scalar_model`, `compute_adiabatic_values`,
`compute_BBN_data`) is a Ray remote task. This is primarily an infrastructure concern, but it
has one numerical-hygiene consequence worth noting: results are computed once and persisted
in a sharded SQLite datastore keyed (among other things) by tolerances and solver, so a
parameter-survey point is never silently recomputed at a different accuracy. The BBN and
adiabatic stages consume a lightweight `ScalarModelProxy` (a Ray object reference) so that
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
