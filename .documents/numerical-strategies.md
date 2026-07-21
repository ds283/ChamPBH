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

### 7.5 Running PRyMordial and outputs

PRyMordial is imported *locally* inside the worker (with a comment noting the intent to avoid
leaking its module-level globals between Ray worker threads), verbose output is disabled, and
the NP-sector start temperature is set explicitly (`Tstart_NP = T_start / MeV_to_Kelvin`,
handling a unit inconsistency where PRyMordial's `T_start` is in Kelvin while everything else
is in MeV). By default the **small reaction network** is used (`small_network_flag = True`),
which is faster at the cost of Li-7 accuracy; this is configurable per run.

`PRyMclass(rho_NP, P_NP, drho_NP_dT).PRyMresults()` is invoked inside a try/except that
converts any `OverflowError`, `ValueError`, or `ComputationFailureError` into a graceful
`{"failure": True}`. On success the code extracts and stores the primordial abundances
`Yp` (helium mass fraction), `D/H`, `He3/H`, and `Li7/H`, along with the small-network flag,
a pinned PRyMordial commit hash (`"bf24c3d"`, since PRyMordial lacks formal versioning), and
both the NP-construction and BBN-solve wall-clock times. The reconstructed NP density and
pressure (and the ratio `density_NP/ρ_rad,Jordan`) are stored per-redshift for later
inspection.

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
