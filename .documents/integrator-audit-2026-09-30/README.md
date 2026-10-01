# Audit of the scalar-field integrator (30 September 2026)

Scope: the step-control strategy of `compute_scalar_model` (`ComputeTargets/ScalarModel.py`) at
tree `b1f64d8` (`VERSION_LABEL` 2026.4.0): the two-region maximum-step scheme, the hard-reflection
fallback, the solver-fallback loop, the exception taxonomy, the tolerances and the stored-sample
density. The starting point was a briefing written by a Claude Science agent from a 16-history
spot check (`integrator_audit_brief.md`, outside the repository). Under `CLAUDE.md` invariant 7
that brief is evidence, not a specification; every claim in it that this audit relies on was
re-measured, and §11 lists where the two disagree.

No code was changed. No Ray cluster and no datastore were used. The scripts in this directory
reproduce every number below; §2 says how to run them.

**The one-line summary.** The region scheme does the one thing it must do, which is stop Radau
stepping across the repulsive wall in a single step, but it does it with a cap fixed in field
space and scaled by `M`, so it is a hundred times too tight wherever the field sits inside a
region without approaching the wall, and it turns the matter-era rebounds at small `M` into a
fragment storm that trips the 100-fragment failsafe. A cap set from the local kinematics
(`|Δφ| ≤ 0.1 φ` per step, from the velocity *and* the inward acceleration) resolves every
reflection at every `M` tried, needs no region boundaries, no fragments and no hard reflection,
and completes all nine full histories in 4–34 s each, including the three the shipped scheme
could not finish. Two further defects were found on the way: SciPy's finite-difference
Jacobian grows its perturbation factor without bound on the `ln T_J` component, which is the true
origin of the "wild trial states", and any exception raised by the right-hand side on a solver
trial state aborts the whole integration.

---

## 0. Findings at a glance

| # | Claim in the brief | Status | Where |
|---|---|---|---|
| A | The fixed regions and fixed per-region caps mismatch the dynamics: cost with no protection when parked inside a region (A1); a fragment storm when rebounds graze a boundary (A2) | **Confirmed**, with the mechanism of the step-over measured directly | §3 |
| — | The hard-reflection fallback recovers from a missed reflection | **Refuted**: it either stalls the integration or lets the field continue at `φ < 0` to `T_CMB` with no wall | §3.5 |
| B | The solver fallback is not wired (`method="Radau"` is a literal) | **Confirmed**; `solver_label` is nonetheless correct for every stored row | §4 |
| C | `RuntimeError`s mix bugs, numerical failures and configuration errors; an uncaught one stops `main.py` | **Confirmed**, plus two new items: a latent `AttributeError` in the RHS's diagnostic branch, and exceptions on *trial* states aborting the solve | §5 |
| D | `atol` does not scale with `φ`; the paper's relaxed tolerances are not in the code path | **Confirmed**; the `atol` effect is at tolerance level, and inside a cap the tolerance does not set the cost | §6 |
| E | Stored samples alias the rebounds; ~5 samples per bounce in the grazing phase | **Confirmed** for the windows re-measured here | §7 |
| F1 | `T_J` rises during the surfing overshoot | **Confirmed and larger**: `βπ` reaches −1.07 and `T_J` rises 12–13 % over 3–5 e-folds | §8 |
| F2 | "Wild Radau trial states" are presumably rejected Newton iterates | **Corrected**: they are SciPy's Jacobian probes with an unbounded perturbation factor; they killed two full histories in this audit until clamped | §8 |
| new | A velocity-only cap has a hole at outer turning points | Found at β = 3, M = 0.01; closed by the acceleration term | §9.1 |
| new | Very small and physical `M` (down to `M = 1 eV = 4.1e-28 M_P`) | The resolved cap works to `M = 1e-8`; below that a floor-triggered instantaneous reflection takes over and matches the hard-wall limit. The shipped hard reflection works for `M ≲ 1e-13` and fails between `1e-13` and `1e-8`. At physical `M` neither scheme can reach `T_CMB` for β ≥ 1.2: the settling bounces double per e-fold, and a parked-tracking model is needed | §3.7, §9.1 |

---

## 1. What the code does (checked against `b1f64d8`)

The brief's code map (§1 of the brief) is accurate. The facts this audit rests on:

- `solve_ivp(RHS, method="Radau", …, max_step=max_step_size, events=(six))` inside a
  `while not solution_complete` loop (`ScalarModel.py:626–660`); every terminal event ends a
  `solve_ivp` call, appends a `SolutionFragment` and restarts from the event state.
- Region parameters (`CosmologyConcepts/Potentials/ExponentialPotential.py:73–91`): L1 boundary
  `1.5 M`, L2 boundary `0.05 M`, caps boundary/500 in e-folds, so `3e-3 M` and `1e-4 M` e-folds.
  Outside both regions `default_max_step` is `inf` (`:69–71`), and `DEFAULT_MAX_STEP_SIZE = inf`.
  `Paper1.tex` (`NumericalSection`) says "of order 1e-2 e-folds outside, 1e-5 inside the outer
  region, 1e-6 inside the inner region": the outer figure is not in the code at all, and the inner
  two hold only for `M ≈ 3e-3` and `M = 1e-2` respectively.
- `method="Radau"` has been a literal since `f67bc3a` (13 January 2026, "Revert to Radau
  stepper"); before it the literal was `LSODA`, and before that `DOP853` (`e6340dc`) and `Radau`
  (`ca62a80`). No commit has ever passed the loop variable `solver` to `solve_ivp`.
- `ExponentialPotential.default_abs_tol` / `default_rel_tol` return `DEFAULT_ABS_TOLERANCE` /
  `DEFAULT_REL_TOLERANCE` (both `1e-8`) and nothing reads them. `main.py` passes `--abs-tol` /
  `--rel-tol` (defaults `1e-8`, `config/argument_parser.py:101–112`). The paper's
  "`1e-5`/`1e-6` for `M ≲ 1e-3`" exists only for `ReclinerPotential`, which is not the production
  potential.
- `RuntimeError` is raised at `:670` (failsafe `N = 1000` reached), `:678` (state length ≠ 5),
  `:711` (event count ≠ 1), `:731` (≥ 100 fragments), `:819` (unknown event), `:851` (z grid too
  short). Only `ComputationFailureError` is caught (`:823`). `RayTools/RayWorkPool.py` calls
  `obj.store()` on a completed compute task with no `try`, `ScalarModel.store()` calls
  `ray.get(self._compute_ref)`, and `main.py` has no exception handling, so a `RuntimeError` in a
  task propagates as a `RayTaskError` and ends the run (reasoned from the code; not run, since no
  Ray cluster is used here).
- The NaN/inf diagnostic branch of `ODERHS.__call__` reads `data.d_logV_dphi` (`:382`), a field
  `ODEPolicyData` does not have (`:70–83`). If that branch is ever entered it raises
  `AttributeError` instead of the intended `ComputationFailureError`, and `AttributeError` is not
  caught anywhere.
- `Quadrature/supervisors/base.py:116–117` (`RHS_timer.__exit__`) prints the type and traceback
  of *every* exception that passes through an RHS evaluation, including the ones the caller is
  about to handle. In this audit that produced multi-megabyte logs (§8, F2).
- SciPy is 1.17.0, NumPy 2.3.5 (`venv`).

---

## 2. Method

### 2.1 The harness

[`harness.py`](harness.py) builds `ODEPolicy`, `ODERHS` and a `ScalarFieldIntegrationSupervisor`
exactly as `compute_scalar_model` does, with `QCD_Cosmology`, `Planck2018`, `Planck_units`,
`ExponentialPotential(n = 1, Λ = 1e-3 eV)` and `ExponentialCoupling`, and drives the integration
from a stored mid-history state with one of two loops:

- `run_fragment_loop(strategy=…)` is a copy of the shipped fragment loop around `solve_ivp`:
  `"regions"` reproduces the shipped scheme (L1/L2 events, per-region `max_step`, restart at
  every crossing, hard reflection at `φ = 0`); `"none"` drops the regions (`max_step = inf`);
  `"fixedcap"` uses one global `max_step`. Dense output is off; the accepted steps, the
  per-fragment RHS counts and the first three step sizes of every fragment are recorded.
- `run_velocity_cap(cap_kind=…)` drives `scipy.integrate.Radau` directly, one step at a time,
  and sets `solver.max_step` before each step from the current state. `Radau._step_impl` reads
  `self.max_step` at every step, so this is a supported way to make the cap state-dependent.
  The cap kinds are in §9.1. Two hygiene options are used from §8 onwards: an exception raised by
  the RHS on a trial state is treated as a rejected step (`h ← h/2`, with a floor of `1e-13`
  e-folds below which it becomes a failure), and SciPy's Jacobian perturbation factor
  `solver.jac_factor` is clamped after every step.

Every run records the RHS count (`nfev`), accepted steps, fragments, hard reflections, the sign
changes of `π` at accepted steps (turning points), the first wall bounce (`N`, `φ_min`, `T_J`)
and the final state. **RHS counts are the machine-independent measure**; wall times are from this
session on a lightly loaded machine, where one RHS costs about 0.1–0.25 ms including Radau's
overhead. The state layout is `(φ_E, π_E, ln ρ_rad,E, ln f_m, ln T_J)` in Planck units.

### 2.2 Probe states

The three mid-history states are the brief's (its §9), exact samples of the dense output of the
shipped scheme:

| probe | β, M | N₀ | φ_E | π_E | ln ρ_rad,E | ln f_m | ln T_J | T_J |
|---|---|---|---|---|---|---|---|---|
| P1 delivery / first bounce | 2, 0.5 | 20.0016270506 | 0.174417541178 | −0.497741179951 | −166.040957866 | −20.8145686243 | −42.6274655299 | 748 MeV |
| P2 parked inside L2 | 2, 0.5 | 25.003077235 | 0.0242924190654 | 0.00184365317107 | −185.456771851 | −16.7033554358 | −46.7288490575 | 12.4 MeV |
| P3 grazing phase | 1.2, 0.01 | 32.8965084954 | 0.00437706545159 | −0.0116641233111 | −232.843822983 | −5.0399304446 | −58.2427706427 | 124 eV |

The delivery state P1 has `φ = 0.174 ≫ M`, so it is also a valid delivery state for `M = 0.01`
and `M = 0.001`; the small-`M` first-bounce probes reuse it. Full histories start from
`main.py`'s initial data (`φ* = 5`, `π* = 0`, `T* = 2×10⁴ GeV`, `ln ρ_rad,J* = −126.1992161962`,
`ln f_m* = −31.0002642242`) and stop at `T_CMB`.

### 2.3 Scripts and how to run them

Run from the repository root with `venv/bin/python`; results (pickles and one-line summaries) go
to `$AUDIT_OUT`, default `<tmp>/integrator-audit-2026-09-30/`, never into the repository. The
logs are noisy because `RHS_timer.__exit__` prints every caught exception; filter with
`grep -E '^(P1|P2|P3|FULL|regions|none|vel|fixed)'`.

| script | what it measures | §§ | time |
|---|---|---|---|
| `p1_sweep.py a|b|c` | P1: shipped scheme at 1e-8/1e-10/1e-12 and 1e-5/1e-6, `atol` vector, no regions, BDF/LSODA, fixed caps, velocity caps | 3.1, 3.4, 6 | 2 min |
| `p1_smallM.py M` | P1 with `M = 0.01`, `0.001`: shipped scheme against the caps | 3.6 | 15 s |
| `p2_parked.py`, `p2_caps.py` | P2, 25 → 40 e-folds: shipped scheme (4 min), no regions, global caps, velocity cap | 3.2 | 5 min |
| `p3_grazing.py regions|none`, `p3_variants.py` | P3, 32.9 → 37.5: shipped scheme (10 min), no regions, fixed caps, velocity caps | 3.3 | 12 min |
| `p_total2.py` | the step-over table: no cap / fixed caps / velocity cap at `M = 0.5, 0.01, 0.001`, with trial-state exceptions treated as rejections | 3.1, 3.6 | 1 min |
| `p_full.py β M [tol] [jac_factor_max] [cap_kind] [reflect]` | one full history from `main.py`'s initial data under the recommended loop; `reflect` enables the floor-triggered reflection of §9.1 | 9.3, 3.7 | 4–35 s (minutes to open-ended at physical `M`, §3.7) |
| `p_smallM_scan.py regions|kin|kinref M…` | the first reflection from the P1 state at each `M` given, under the shipped scheme, the resolved cap, or the cap with the floor-triggered reflection | 3.7 | seconds per `M` |
| `p_full_regions.py β M seconds` | one full history under the **shipped** scheme, with a time limit | 3.7 | up to the limit |

The two runs that take minutes are the shipped scheme itself; everything else is seconds, except the
physical-`M` full histories of §3.7, which do not terminate.

---

## 3. Finding A: the region scheme

### 3.1 Why a cap is needed at all: the step-over, measured

The paper's argument for the regions ("the field can traverse the entire steep region within a
single trial step and the error estimator may not register that anything has been missed") is
correct, and the probes show exactly how it happens. With no cap, Radau at `atol = rtol = 1e-8`
from the P1 state at `M = 0.01` accepts four steps:

| N | φ_E | π_E | h |
|---|---|---|---|
| 20.001627 | 1.744e-1 | −0.49774 | |
| 20.006418 | 1.720e-1 | −0.49774 | 4.8e-3 |
| 20.054325 | 1.482e-1 | −0.49771 | 4.8e-2 |
| 20.254861 | 4.839e-2 | −0.49763 | 2.0e-1 |
| 20.352100 | 1.7e-16 | −0.49762 | 9.7e-2 |

The wall is at `φ ≈ 9e-5`. The last step goes from `φ = 0.048` to `φ = 0` with `π` unchanged: none
of Radau's collocation nodes fell inside the wall, and for `φ < 0` the potential
`ln V = ln Λ⁴ + M/φ` is *smaller* than `Λ⁴`, so the far side of the wall exerts no force. The
error estimator sees a smooth trajectory. The `φ = 0` event then fires and the hard reflection
takes over (§3.5).

The same happens at `M = 0.5` (`φ_wall ≈ 4.6e-3`): 4 steps, then `φ = 1.7e-16` at
`N = 20.352100`, against the true first bounce at `N = 20.343028`, `φ_min = 4.5737e-3`. BDF and
LSODA behave the same way. Tightening the tolerance does not fix it reliably: with trial-state
exceptions treated as step rejections (§5), no-cap Radau resolves the first bounce at `1e-12` for
`M = 0.5` but steps over it at `1e-8` and `1e-10`, and at `M = 0.01` it steps over at all three
tolerances (`p_total2.py`).

A single fixed `max_step` works only if it scales with `M`. From the P1 state, with no regions
(`p_total2.py`, trial-state exceptions treated as rejections):

| fixed `max_step` (e-folds) | M = 0.5 | M = 0.01 | M = 0.001 |
|---|---|---|---|
| 1e-1 | steps over | steps over | steps over |
| 1e-2 | resolved, 2 213 RHS | steps over (φ_min 4.1e-3, true 9.2e-5) | steps over |
| 1e-3 | resolved, 8 385 RHS | steps over (φ_min 7.6e-4) | steps over |
| 3e-4 | resolved, 24 604 RHS | resolved, 24 769 RHS | steps over (φ_min 2.6e-4, true 9.2e-6) |
| kinematic cap (§9.1) | resolved, 1 945 RHS | resolved, 2 120 RHS | resolved, 2 275 RHS |

"Steps over" means a hard reflection fired and the subsequent trajectory is wrong. So the paper's
"of order 1e-2 e-folds" outside cap would protect the first bounce at `M = 0.5` and nothing
smaller; a cap of `3e-3 M` (the L1 value) protects it wherever it is in force. The shipped scheme
is therefore *correct where it applies*; its defects are where and how much it applies.

### 3.2 A1, parked inside a region: confirmed, factor 111 in cost for nothing

P2 is the β = 2, M = 0.5 history at 12.4 MeV, parked at `φ ≈ 0.007–0.024` with `|π| ≲ 0.03`. The L2
boundary is `0.025`, so the whole BBN window lies inside L2 and the cap is `5e-5` e-folds.

| strategy, tol 1e-8 | RHS 25 → 40 | steps | wall | bounces | first bounce N | φ at N = 40 |
|---|---|---|---|---|---|---|
| shipped regions | **2 099 582** | 299 939 | 253 s | 19 | 27.729127 | 1.909693e-2 |
| no cap | 18 916 | 2 049 | 5 s | 19 | 27.729360 | 1.909693e-2 |
| no cap, tol 1e-10 | 52 940 | 5 951 | 9 s | 19 | 27.729138 | 1.909693e-2 |
| global cap 0.1 | 18 986 | 2 058 | 4 s | 19 | 27.729512 | 1.909692e-2 |
| global cap 1e-2 | 23 890 | 2 831 | 6 s | 19 | 27.729128 | 1.909693e-2 |
| kinematic cap (§9.1) | 18 880 | 2 052 | 1.4 s | 19 | 27.729389 | 1.909693e-2 |

Every accepted step of the shipped run has `h = 5e-5` (the median is the cap; the maximum is the
cap). The RHS cost is `1.40×10⁵` per e-fold, as the brief measured; without the cap it is
`1.26×10³`. All six variants give the same 19 gentle bounces and the same `φ` at `N = 40` to seven
digits, so over these 15 e-folds the trajectory is not chaotic at the tolerance level and the cap
protects nothing. Radau's own error control resolves these bounces because the field is slow
(`|π| ~ 10⁻³–10⁻²`) and the turning points are far outside the wall.

### 3.3 A2, grazing the boundary: confirmed, the fragment storm and its cost

P3 is the β = 1.2, M = 0.01 history at 124 eV, entering the matter-era rebounds. The shipped scheme
from this state (`p3_grazing.py regions`, 618 s):

| quantity | shipped regions | brief's prediction |
|---|---|---|
| fragments 32.9 → 37.5 | **85** | ~84 |
| L2 entries / exits | 42 / 42 | 42 |
| L1 entries / exits | 0 / 0 | — |
| hard reflections | 0 | — |
| first L2 entry, last L2 entry | N = 33.184, 37.196 | 33.184, 37.196 |
| spacing of the last ten entries | 0.034–0.038 e-folds | shrinking to 0.034 |
| RHS | **5 188 284** (1.13×10⁶ per e-fold) | 1.26×10⁶ per e-fold |
| accepted steps | 741 086, median `h = 1e-6` | — |
| fraction of steps inside L2 | 0.82 | — |
| wall bounces | 51, `φ_min = 2.7989e-4` | — |

Added to the four L2 entries before `N = 32.9` in the full history this is 89 fragments by
`N = 37.2`, consistent with the brief's 100-fragment `RuntimeError` at `N = 37.165` in the full run
(the full run also carries its delivery fragments). The mechanism is as the brief describes: the
inner turning point of each rebound sits just inside the L2 boundary (`φ_min` runs from
`0.56` to `1.05` of the boundary) and the outer one outside it, so every rebound crosses the
boundary twice.

**Cost is set by the cap, not by the restarts.** The first three step sizes of every fragment
equal the new cap exactly (`3e-5, 3e-5, 3e-5` or `1e-6, 1e-6, 1e-6`): Radau's initial-step
heuristic proposes something larger and is clipped, so no time is lost re-growing the step. The
smallest fragment costs 4 635 RHS; Radau's start-up (one RHS for `f₀`, five for the
finite-difference Jacobian, one Newton solve) is under 20 RHS. The brief's question 2 is answered:
restarts are cheap, the `1e-6` cap over 82 % of the steps is the cost.

**The same window under the caps of §9.1** (`p3_variants.py`, `p_total2.py`):

| strategy, tol 1e-8 | RHS | steps | wall | bounces | φ at N = 37.5 | φ_min of bounce 1, 2, 8 |
|---|---|---|---|---|---|---|
| shipped regions | 5 188 284 | 741 086 | 618 s | 51 | 5.807869e-4 | 2.79886e-4, 3.03650e-4, 3.66368e-4 |
| velocity cap, `f = 0.1` | 38 570 | 4 038 | 4 s | 51 | 5.807805e-4 | 2.79916e-4, 3.03651e-4, 3.66448e-4 |
| velocity cap, tol 1e-10 | 105 574 | 11 624 | 15 s | 51 | 5.807869e-4 | — |
| kinematic cap, `f = 0.1` | 38 547 | 4 040 | 2 s | 51 | 5.807820e-4 | — |
| fixed cap 1e-3, no regions | 56 370 | 6 725 | 8 s | 51 | 5.807852e-4 | — |
| fixed cap 1e-4, no regions | 323 270 | 46 040 | 54 s | 51 | 5.807868e-4 | — |

Same 51 bounces, per-bounce `φ_min` agreeing to `≤ 2×10⁻⁴` relative, final `φ` to `10⁻⁵`, at
**134 times** lower cost. (No-cap Radau with trial-state exceptions treated as rejections also
survives this slow window, 38 487 RHS; with the shipped RHS it dies at once, §5.)

### 3.4 Restart side effects on the solution: none measurable

The brief asked whether discarding Radau's step-size and error history at every crossing changes
the per-bounce dissipation. Over the P1 window the shipped scheme (3 fragments, two restarts at
the L2 crossings) and the single-solve caps agree on `φ` and `π` at `N = 21` to seven digits
(`1.220383e-1`, `−1.2972e-1`), and over P3 (85 fragments) the per-bounce `φ_min` sequence agrees
with the single-solve cap to `2×10⁻⁴`. Any restart effect is below the tolerance.

### 3.5 The hard-reflection fallback: not a recovery

`reflection_failure_detector` fires when `φ` crosses `hard_reflection_point = 0` downwards; the
loop then flips the sign of `π` and restarts from the event state. For the exponential potential
this cannot work, for two reasons visible in the probes:

1. **The far side of the wall is free.** `ln V = ln Λ⁴ + M/φ` for `φ < 0` is below `ln Λ⁴`, so a
   step that lands at `φ < 0` felt no wall (§3.1). The event root then lies within rounding of
   `φ = 0`, on whichever side the root-finder lands: `+1.7e-16` in the Radau run, `−3.3e-16` in the
   LSODA run, `−1.3e-15` in a fixed-cap run.
2. **`φ = 0⁺` is the top of an infinitely steep wall.** With `φ = +1.7e-16`, `V'/V = −M/φ² ≈ −10³¹`
   and `V/3H²M_P² = 1`. Radau's restart fails at once with "Required step size is less than
   spacing between numbers"; BDF and LSODA fail with `G < 0` on a trial state. In the shipped
   loop `sol.success` is false, `ComputationFailureError` is raised, and the fallback loop
   (§4) repeats the identical Radau integration three more times before storing a failure row.
   That is the *good* outcome.
3. **`φ = 0⁻` is a free particle.** With `φ = −1.3e-15` after the flip (fixed cap `0.1`,
   `p_total2.py`), the kick term `−3M_P²E(β/M_P)R` is inward, `π` turns negative within one step and
   the field runs off to `φ = −0.10` by `N = 21` with no wall and no event: the downward-crossing
   detector cannot fire again from below zero. In the shipped loop this history would be stored
   as a success, sampled to `T_CMB`, and passed to BBN with `φ < 0` throughout.

Both outcomes are worse than a plain failure row *for these values of `M`*.

**Where the hard reflection is right (added later on 2026-09-30, after the user pointed out its
purpose).** The reflection was written for very small, physical `M`, where the bounce cannot be
resolved in double precision, and there it is the correct model and it works. `solve_ivp` locates
an event root to `4 EPS` in `N`, i.e. `φ` to about `|π| · 4e-16 · N ≈ 4e-15`. When the wall
`φ_wall ≈ M/109` is thinner than that, the restart state at `φ ≈ ±1e-15` is *outside* the wall on
either side, the flipped `π` carries the field away, and the reflection is an exact elastic bounce
in a background frozen for the `~1e-29` e-folds the true bounce would take. Measured from the P1
state (`p_smallM_scan.py regions …`, §3.7): at `M = 1e-14, 1e-20, 1e-27` the shipped loop fires
one hard reflection and reaches `N = 21` with `φ = 1.184428e-1`, identical to seven digits with the
resolved reflection at `M = 1e-6` and `1e-8`. At `M = 1e-20` and below the L1/L2 events are
inert as well (their boundaries `1.5 M`, `0.05 M` fall inside one root-finding tolerance), so the
history is a plain Radau integration with elastic bounces, which is exactly right.

The failure regime is the intermediate one, `1e-13 ≲ M ≲ 1e-8`, where the wall is thicker than
the event precision but thinner than the smallest representable step: the shipped scheme dies at
`M = 1e-10` with "Required step size is less than spacing between numbers" inside L2 (§3.7). For
`M ≳ 1e-8` the reflection should never fire; if it does, the two outcomes above follow. The
recommendation (§9.1) keeps the elastic reflection as a *deliberate* model with a controlled
trigger, and raises `ComputationFailureError` on `φ ≤ 0`, which under the cap can only mean the
cap was violated.

What the shipped scheme cannot do at physical `M` is finish: the settling phase produces hundreds
of bounces, each a fragment, and the 100-fragment failsafe stops the β = 2, `M = 1 eV` history at
`N = 39.04` (`p_full_regions.py 2.0 4.1e-28 900`). See §3.7 for why no bounce-resolving scheme
finishes there either.

### 3.6 The first bounce at small M, and the relaxed tolerances

From the P1 state with `M = 0.01` and `0.001` (`p1_smallM.py`):

| M | strategy | RHS | fragments | first bounce N | φ_min | φ at N = 21 |
|---|---|---|---|---|---|---|
| 0.01 | shipped regions, 1e-8 | 26 634 | 5 | 20.351919 | 9.15056e-5 | 1.185154e-1 |
| 0.01 | velocity cap, 1e-8 | 2 120 | 1 | 20.351919 | 9.15045e-5 | 1.185154e-1 |
| 0.01 | velocity cap, 1e-10 | 5 097 | 1 | 20.351919 | 9.15047e-5 | 1.185154e-1 |
| 0.001 | shipped regions, 1e-8 | 26 422 | 5 | 20.352082 | 9.15061e-6 | 1.184501e-1 |
| 0.001 | shipped regions, 1e-5/1e-6 | 25 391 | 5 | 20.352083 | 9.15432e-6 | 1.184501e-1 |
| 0.001 | velocity cap, 1e-8 | 2 275 | 1 | 20.352082 | 9.15051e-6 | 1.184501e-1 |
| 0.001 | velocity cap, 1e-10 | 5 880 | 1 | 20.352082 | 9.15055e-6 | 1.184501e-1 |
| 0.001 | velocity cap, 1e-5/1e-6 | 1 942 | 1 | 20.352082 | 9.15051e-6 | 2.771e-1 (2 bounces) |

Three points. (i) The shipped scheme resolves the first bounce at every `M`; the cost is 12 times
that of the cap and is set by the L1 cap `3e-3 M` over the whole approach from `1.5 M`. (ii) The
cap's cost is nearly independent of `M` (1 945, 2 120, 2 275 RHS at `M = 0.5, 0.01, 0.001`),
because a step bounded by `0.1 φ` walks in geometrically and the number of steps to reach the wall
is `∝ ln(φ_start/φ_wall)`. (iii) The paper's relaxed tolerances (`1e-5`/`1e-6` for `M ≲ 1e-3`)
are not needed for the integration to "make progress": at `1e-8` the shipped scheme completes
the `M = 0.001` reflection in 26 422 RHS, and at `1e-5`/`1e-6` it costs the same (25 391), because
inside a region the cap sets the step, not the tolerance. Under the cap alone, loose tolerances
*do* change the trajectory after the bounce (two bounces instead of one by `N = 21`); the
tolerance is doing real work there, and `1e-8` should be kept everywhere.

The first-bounce `N` converges to the hard-wall limit `20.3521` (where the no-cap runs hit
`φ = 0`) and `φ_min ≈ M/ln(ρ/V₀) ≈ M/109`, as expected.

### 3.7 Very small and physical M (added later on 2026-09-30)

`Paper1.tex` quotes `M = 1e-3`–`1e-2 M_P` and treats `M ≲ 1e-3` as "very steep"; `main.py` also
accepts `--M-values-eV`, and a chameleon with `M ~ 1 eV = 4.1e-28 M_P` is the physical case the
hard reflection was written for. The first reflection from the P1 state, `M` from `1e-3` down to
`1 eV` (`p_smallM_scan.py`; `φ_wall ≈ M/109`):

| M | shipped scheme | resolved kinematic cap (§9.1, no reflection) | cap + floor-triggered reflection (§9.1) |
|---|---|---|---|
| 1e-3 | resolved, 26 422 RHS | resolved, 2 275 | resolved, 2 275 |
| 1e-4 | resolved, 26 391 | resolved, 2 489 | resolved, 2 489 |
| 1e-6 | resolved, 26 677, `h_min = 7.9e-12` | resolved, 3 003, `h_min = 9.0e-12` | resolved, 3 003 |
| 1e-8 | resolved, 26 758, `h_min = 8.5e-14` | resolved, 3 465, `h_min = 1.1e-13` | resolved, 3 479 |
| 3e-9 | — | steps over (floor `1e-9` reached) | reflected at `φ = 4.7e-11`, 1 708 |
| 1e-10 | **fails**: "Required step size is less than spacing between numbers" at `φ = 5e-12` inside L2 | steps over | reflected at `φ = 4.7e-11`, 1 693 |
| 1e-14 | hard reflection at `φ = +1.9e-15`, recovers | steps over | reflected, 1 693 |
| 1e-20, 1e-27, 4.1e-28 | hard reflection at `φ = +1.7e-16`, recovers; L1/L2 events inert | steps over | reflected, 1 693 |

Every run that did not fail or step over ends at `N = 21` with `φ = 1.184428e-1` for `M ≤ 1e-6`
(`1.184435e-1` at `1e-4`, `1.184501e-1` at `1e-3`): the hard-wall limit, approached as `O(φ_wall)`.
The three columns agree with each other wherever two of them work. "Steps over" in the middle
column is the probe's own `h_floor = 1e-9` being reached; the probe then mirrors the state, which
happens to land outside the wall for these `M`, but that is luck, not a rule, and is why the
right-hand column exists.

**Full histories at physical `M`** under the cap with the floor-triggered reflection
(`p_full.py β M 1e-8 1e-4 kin reflect`):

| β | M | outcome |
|---|---|---|
| 0.9 | 4.1e-28 | **completes**: `N = 44.58`, 140 reflections (all at `φ` between `1.7e-12` and `8e-11`), 187 452 RHS, 34 s |
| 1.2 | 4.1e-28 | not finished: steps per e-fold 4 587 → 10 161 → 20 462 → 36 688 → 62 135 for `N = 36 → 41` (×2.0 per e-fold); stopped at `N = 41` after 174 s |
| 2.0 | 4.1e-28 and 1e-15 (identical to the step) | not finished: 1 185 → 3 006 → 7 655 → 19 333 → 46 166 steps per e-fold for `N = 36 → 41` (×2.4); stopped at `N = 41` after 105 s |
| 2.0 | 1e-10 | not finished: 1 366 → 3 450 → 8 450 → 32 729 → 80 537 per e-fold; stopped at `N = 41` |
| 3.0 | 4.1e-28 | not finished: 1 003 → 2 645 → 6 639 → 16 795 → 42 097 per e-fold for `N = 40 → 45` (×2.5); stopped at `N = 45` |
| 2.0 | 1e-6 (resolved, no reflection needed) | **completes**: `N = 50.08`, 7 429 wall bounces, 3.33×10⁶ RHS, 273 s; 1 167 bounces per e-fold at `N = 40–45` |

For comparison the `M = 1e-3` history (§9.3) has 804 bounces and costs 2.7×10⁵ RHS.

**What this is.** From `N ≈ 37` (β ≤ 2) or `41` (β = 3), in the matter era, the field is pressed
onto the wall by the kick and its bounce amplitude decays under Hubble friction. A ball dropped on
a floor under gravity bounces with a period `∝ sqrt(amplitude)`, so as the amplitude decays
exponentially in `N` the bounce rate grows exponentially: measured, a factor 2–2.5 per e-fold.
Each bounce costs the same ~50–200 steps (the geometric approach from the outer turning point to
the reflection surface at `~5e-11`, or to the wall when resolved). Extrapolating from
`N = 41` to `T_CMB` at `N ≈ 50` is `2^9`–`2.5^9` times the last e-fold's 5–8×10⁴ steps, i.e.
`10⁷`–`10⁸` steps and hours to days per history. At `M = 1e-6` the same growth is cut off
because the amplitude reaches the wall thickness and the bounces become resolved oscillations in
the well (7 429 of them, 273 s); at `M = 1e-3` sooner still. At physical `M` the amplitude has
`10²⁵` more decades to fall through and the growth never stops within the history.

**So the answer to "does it work at very small `M`"** is: the reflection model is right and the
delivery, the first bounce and the early rebounds are integrated correctly at any `M`, in the
hard-wall limit; but for β ≥ 1.2 no scheme that follows the bounces one by one, shipped or
proposed, reaches `T_CMB` at physical `M`, because the number of bounces diverges. The shipped
scheme stops at its 100th fragment (`N = 39.04` for β = 2, §3.5); the proposed loop keeps going at
a cost that doubles every e-fold. What is missing is physics, not step control: once the bounce
amplitude is far below any scale of interest (and the bounce period far below the sample spacing),
the field is a passenger sitting at `φ_wall(ρ)`, the minimum of the effective potential, and
should be switched to that tracking solution (`φ = φ_wall(ρ(N))`, `π = dφ_wall/dN`, energy of
the bounces discarded). That is a decision for the authors (§9.4, §10): when to park, and what
the parked field's `ρ_φ` and `w_φ` are for BBN and the adiabatic stage.

---

## 4. Finding B: the solver fallback

Confirmed as the brief describes: `solver_list = ["Radau", "BDF", "LSODA", "DOP853"]` is walked
on `ComputationFailureError`, `solve_ivp` is always called with `method="Radau"`, so a failing
history is integrated four times identically before `{"failure": True}` is returned. Two
corrections to the brief:

- `solver_label` is right for every *stored* row. It is `solver_labels[solver]` with `solver` the
  name current when the loop exited; since the integration is deterministic, a success only ever
  happens on the first pass, when `solver == "Radau"`. It is wrong only in the sense that the
  three extra names can never be reached.
- Whether DOP853 "is ever viable" was not measured, but the stiffness was: in the matter-era well
  at small `M` the Jacobian entry `∂π̇/∂φ` is `−1.3×10⁵` (`M = 0.01`) and `−1.1×10⁶`
  (`M = 0.001`), i.e. oscillation periods of `0.018` and `0.006` e-folds sustained for tens of
  e-folds. An explicit method would be stability-limited to steps of a few `10⁻³` e-folds
  throughout, and Radau's failures in this audit were never Radau's own (§5, §8).

Recommendation: delete the loop; keep a single stepper and a single label; make the paper's
text match (§10).

---

## 5. Finding C: the exception taxonomy, and exceptions on trial states

The brief's table of `RuntimeError` sites is correct (§1). The audit adds:

**Exceptions on trial states abort the whole integration.** Radau evaluates the RHS on Newton
iterates and on finite-difference Jacobian probes, states that were never accepted and may be
wildly unphysical. `ODEPolicy` raises `ComputationFailureError` for `G < 0` (`|π| > √6 M_P`), for
overflow in `exp(ln f_m)` or `exp(ln T_J)`, and for non-finite input, and `PotentialDerivativePolicy`
for overflow in the potential. `solve_ivp` does not catch exceptions from the RHS, so the first
such trial state ends the solve, whatever the accepted trajectory was doing. Measured:

- From P1 with no regions at `atol = rtol = 1e-10` and `1e-12`, `solve_ivp` dies with `G < 0`
  raised on a Newton iterate with **zero accepted steps** (`p1_sweep.py a`); at `1e-8` it gets
  four steps in. A fixed cap of `1e-2` dies the same way at `1e-8`. When the same exception is
  treated as a rejected step (§2.1) the `1e-2` cap resolves the bounce in 2 213 RHS.
- From P3 with no regions, `solve_ivp` dies on the first solve with `exp(ln f_m)` overflow at a
  trial state with `φ = 650`, `π = 13 934`; with rejection it completes the window.
- The two full histories in §8 died of the same mechanism after 38 e-folds.

The shipped loop treats these as `ComputationFailureError`, retries three times identically, and
stores a failure row for what may be a perfectly integrable history. None of the region caps in
the shipped scheme fix this; they only make the Newton iterates tamer by keeping `h` tiny.

**Two conflicting policies for the same kind of event.** `_get_T_Jordan` *substitutes* `T_J = 1 K`
when a trial state has `T_J ≤ 0` and prints a warning (`:181–195`), while `G < 0` and the overflows
*raise*. The substitution hides the F2 mechanism (§8); the raises kill histories.

**Proposed taxonomy** (for the campaign, §10):

| condition | today | proposed |
|---|---|---|
| RHS exception on a trial state | aborts the solve | reject the step (`h ← h/2`); below a floor (`1e-13` e-folds), `ComputationFailureError` |
| `φ ≤ 0` in an accepted state (missed reflection) | reflect and continue | `ComputationFailureError`, failure row |
| step size below floor / Radau `success = False` | `ComputationFailureError` | unchanged |
| failsafe `N = 1000` reached | `RuntimeError` | `ComputationFailureError` (a history that never cools to `T_CMB` is a numerical failure of that history) |
| ≥ 100 fragments, multiple events, unknown event | `RuntimeError` | removed with the fragment loop |
| state length ≠ 5, `AttributeError` at `:382` | `RuntimeError` / latent bug | assertion (bug), and fix the field name |
| z grid too short for `N_final` | `RuntimeError` | keep `RuntimeError`: configuration, must stop the run |
| `RHS_timer.__exit__` traceback print | on every exception | remove or make it debug-only |

---

## 6. Finding D: tolerances

- **`atol` versus `φ`.** From P1 at `M = 0.5`, with the shipped scheme, `atol = [1e-8 M, 1e-8 M,
  1e-8, 1e-8, 1e-8]` (`rtol = 1e-8`) gives `φ_min = 4.57374e-3` and first-bounce
  `N = 20.343031`; plain `1e-8` gives `4.57383e-3` and `20.343036`; the `1e-12` reference is
  `4.57371e-3` and `20.343028`. The scaled `atol` halves the error at 1 % extra cost. It is a
  refinement, not a fix: at tolerance level, as the brief expected.
- **Tolerance does not set the cost inside a cap** (§3.6). `1e-5`/`1e-6` inside the shipped
  regions costs the same as `1e-8`. The paper's statement that `1e-8` "drives the step size below
  the point at which the integration can make progress" for `M ≲ 1e-3` was not reproduced: the
  `M = 0.001` reflection completes at `1e-8` under both schemes (§3.6), and full `M = 0.001`
  histories complete at `1e-8` under the recommended loop (§9.3).
- **Which runs used relaxed tolerances:** none can have, unless `--abs-tol`/`--rel-tol` were
  passed on the command line; the potential's overrides are unread. This cannot be settled from
  the repository (the stored `tolerance` rows would say), but the paper's sentence describes code
  that is not in the production path.

---

## 7. Finding E: stored-sample density

The z grid is 250 per decade in `1 + z`, i.e. `ΔN ≈ 0.0092` between samples. Turning-point spacing
from the accepted steps of the probes:

| window | median spacing between turning points | samples per half-period (median / min) |
|---|---|---|
| P1 delivery, 20.34 → 21.0 (β = 2, M = 0.5) | 0.52 e-folds (one rebound) | 56 |
| P2 parked, 25 → 40 (β = 2, M = 0.5) | 0.080 (min 0.060) | 8.7 / 6.5 |
| P3 grazing, 32.9 → 37.5 (β = 1.2, M = 0.01) | 0.022 (min 0.014) | 2.4 / 1.5 |

The grazing phase has about five samples per bounce, as the brief says. The brief's claim of
extrema 1–2 samples apart in the 1–100 MeV delivery rebounds, and its 0.18 % shift of D/H under
denser sampling, were not re-measured here (they need a full history through BBN and the
PRyMordial interface). The remedy the brief suggests, recording every turning point in addition
to the z grid, is straightforward in the recommended loop (§9.1, every accepted step is seen), and
belongs to a separate decision (§10, prompt 05).

---

## 8. Finding F: the smaller observations

**F1, `T_J` rises during the surfing overshoot: confirmed, and larger than stated.** From full
histories at `M = 0.5` under the recommended loop (`ln T_J` at every accepted step):

| β | min βπ | at N, T_J | window where `ln T_J` rises | rise in `T_J` |
|---|---|---|---|---|
| 1.2 | −1.069 | 15.98, 216 MeV | N = 14.58 → 17.66 (3.1 e-folds, 335 samples) | 205.4 → 231.1 MeV (+12 %) |
| 2.0 | −1.054 | 15.57, 690 MeV | N = 14.17 → 18.97 (4.8 e-folds, 522 samples) | 657.1 → 749.9 MeV (+13 %) |

The brief's "at least −1.008" was read from stored samples; at the RHS level the excursion below
`βπ = −1` is 5–7 %. The "first bounce at 747 MeV" for β = 2 therefore happens *after* `T_J` has
risen from a 657 MeV minimum, and the delivery temperature is not the plateau temperature.
Anything that assumes `T_J(N)` is invertible above 100 MeV must allow for this; the BBN window is
unaffected.

**F2, "wild Radau trial states": mechanism found, and it is a failure source.** The
`T_Jordan = 0` substitutions the brief saw (`ln T_J` down to `−10²¹`) are not rejected Newton
iterates. In the P2 no-cap run the substituted values come in pairs at the same `N` and grow by
exactly a factor 10 each time: `−910, −914, −8 627, −85 936, −8.6×10⁵, −8.6×10⁶, …`. That is
SciPy's finite-difference Jacobian (`scipy/integrate/_ivp/common.py`, `_dense_num_jac`): the
perturbation factor of a state component is multiplied by `NUM_JAC_FACTOR_INCREASE = 10` whenever
that component's Jacobian column is smaller than `EPS^0.75` relative to the RHS scale, and it
has a lower clamp (`NUM_JAC_MIN_FACTOR`) but **no upper clamp**. The `ln T_J` column is
identically zero once `g_s` is constant (below a few keV), so every Jacobian evaluation multiplies
its factor by 10 for the rest of the history. The probe `ln T_J + factor·|ln T_J|` then reaches
`−9.4×10³⁰⁷` (seen verbatim in the logs), `exp` of it is 0, the substitution kicks in, and on the
next probe the RHS input is non-finite and `ODEPolicy` raises. Measured:

- P2, no cap: 35 substitutions in 15 e-folds; with `solver.jac_factor` clamped to `1e-4` after
  every step: **0**, same RHS count (18 880), same trajectory.
- Full histories β = 1.2, M = 0.01 and β = 2, M = 0.001 under the velocity cap without the clamp
  died at `N = 37.88` and `38.46` (`T_J ≈ 0.85` and `26 eV`) after 68 and 39 rejected trial
  states, with "input to ODE RHS has infinity or NaN values" and a probe at
  `ln T_J = −9.4×10³⁰⁷` in the log. A fresh solver restarted from the failing state integrates on
  without trouble, which is the signature of solver-internal state, not of the RHS. With the
  clamp both complete (§9.3).

The shipped code is exposed in the same way: its `T_J = 1 K` substitution hides the growth until
the probe becomes non-finite, at which point the `ComputationFailureError` ends the history. How
often this has happened in production cannot be told from the repository; the stored failure rows
carry no reason. The fix is either the clamp (a one-line attribute update in a custom step loop)
or an analytic Jacobian passed as `jac=`, which removes `num_jac` altogether (§9.4).

**F3 (`φ*` hard-coded) and F4 (`main.py:950` stale comment)** were not re-examined; F3 is already
issue `[00-initial-field-value-is-hard-coded-and-unchecked]` on the `review-remediation` board.

---

## 9. Recommended integration strategy

### 9.1 The kinematic cap, and why it is safe

The wall is purely repulsive: the only inward force on `φ` is the conformal kick (plus friction),
which is smooth and slowly varying, and the potential can only push outward. So the inward
displacement of the field during a step of length `h` starting from `(φ, π, π̇)` is bounded by

    Δφ_in ≤ max(−π, 0) · h + ½ · max(−π̇, 0) · h²

with `π̇` the RHS's second component at the step start (Radau already holds it as `solver.f`).
Requiring `Δφ_in ≤ f φ` gives

    h ≤ f φ / |π|          if π < 0,
    h ≤ sqrt(2 f φ / |π̇|)  if π̇ < 0,

and the field can never cross more than a fraction `f` of its distance to the origin in one step,
whatever `M` is. With `f = 0.1` the approach to the wall from `φ_start` costs about
`10 ln(φ_start/φ_wall)` steps (≈ 50 for `φ_start/φ_wall ≈ 100`), independent of `M`, which is the
cost scaling measured in §3.6. Away from the wall (`φ ~ M_P`) the cap is of order an e-fold and
never binds. The cap is applied on top of a global `max_step = 0.1` e-folds.

The second term matters. A velocity-only cap (the brief's "limit `|π|ΔN`") resolves P1, P2, P3 and
six of the nine full histories, but at β = 3, M = 0.01 it fails at `N = 34.60`: the field sits at
an outer turning point (`φ = 3.7e-4`, `π = +1.7e-5`), the cap is inactive because `π ≥ 0`, Radau
takes `h = 2.9e-2`, and within that step the field falls from rest, through the wall at
`φ ≈ 1.6e-4`, to `φ = −8.7e-6`. The acceleration term caps that step at `6.6e-3` e-folds. With
it, all nine histories complete (§9.3) and the two caps cost the same on the probe windows
(P1 1 945 vs 1 945 RHS, P2 18 880 vs 18 880, P3 38 547 vs 38 570).

Alternatives considered: a cap relative to the estimated wall position `φ − φ_wall(ρ)` (the
brief's suggestion) works equally well where tried but needs a model of the wall and can be
fooled when the estimate is off; a cap of `0.1 M` per step is wrong (it steps over at `M = 0.5`,
where `φ_wall ≈ 0.01 M`); a fixed `max_step` must scale as `∼ 3e-3 M` and is then paid
everywhere (§3.1).

**The floor, and the instantaneous reflection (added later on 2026-09-30).** Steps below about
`10 · EPS · N ≈ 1e-13` e-folds are not representable at `N ~ 20–55`, and the resolved cap needs
`h ≈ f φ_wall/|π| ≈ 2e-3 M` at the wall, so it can resolve the reflection only for `M ≳ 1e-8`
(§3.7). Below that the shipped code's idea is the right one: the bounce is instantaneous and
elastic. The rule that selects between the two automatically is a floor `h_floor = 1e-11`
e-folds on the cap:

    before each step, if π < 0 and f φ / |π| < h_floor:  π ← −π, restart the solver; else step.

When the field decelerates inside a resolvable wall, `|π| → 0` and `f φ/|π|` grows, so the floor
is never reached and the bounce is resolved. When the wall is thinner than the representable
step, the field arrives at `φ_stop = |π| h_floor / f` (about `5e-11` at delivery speed) still at
full speed and is reflected there. The reflection is exact to the extent the background is frozen
during the neglected flight, which lasts less than `2 h_floor / f = 2e-10` e-folds, below the
`1e-8` tolerance; and it is elastic because `½π² + V/(3H²M_P²)` is conserved by the wall force
alone, so the true trajectory would return to `φ_stop` with `−π` exactly. Because the wall is
exponential, the force at `φ_stop` is negligible whenever `φ_stop` is even 10 % outside the
balance point (`e^{−0.1·109}`), so the restart state is benign. Measured (§3.7): the reflected
and the resolved answers agree to seven digits where both exist, and the reflected answer is
`M`-independent below `M ≈ 1e-9`, as the hard-wall limit must be. Every reflection is counted and
recorded, replacing `HARD_REFLECTIONS_KEY`, so a stored history says how many of its bounces were
modelled rather than resolved.

### 9.2 The loop

`solve_ivp` accepts only a scalar `max_step`, so a state-dependent cap needs a step loop around
the public `scipy.integrate.Radau` class. `solve_ivp` is itself a thin Python loop over
`solver.step()`, so there is no performance cost. Sketch:

```python
solver = Radau(rhs, N0, y0, N_failsafe, max_step=0.1, rtol=rtol, atol=atol)
interpolants, ts = [], [N0]
while solver.status == "running":
    phi, pi = solver.y[0], solver.y[1]
    a_in = -solver.f[1]
    cap = 0.1
    if pi < 0 and f * phi / -pi < H_FLOOR:      # wall thinner than a representable step:
        y = solver.y.copy(); y[1] = -y[1]        # instantaneous elastic reflection (§9.1)
        reflections.append(solver.t)
        solver = Radau(rhs, solver.t, y, N_failsafe, max_step=0.1, rtol=rtol, atol=atol)
        continue
    if pi < 0:   cap = min(cap, f * phi / -pi)
    if a_in > 0: cap = min(cap, sqrt(2 * f * phi / a_in))
    cap = max(cap, H_FLOOR)
    solver.max_step = cap
    if solver.h_abs > cap: solver.h_abs = cap
    try:
        msg = solver.step()
    except ComputationFailureError:          # trial state was unphysical
        if solver.h_abs < 1e-13: raise         # genuine failure of this history
        solver.h_abs *= 0.5; continue          # otherwise a rejected step
    if msg is not None: raise ComputationFailureError(msg)
    np.minimum(solver.jac_factor, 1e-4, out=solver.jac_factor)   # F2
    if solver.y[0] <= 0.0: raise ComputationFailureError("missed reflection")
    interpolants.append(solver.dense_output()); ts.append(solver.t)
    if solver.y[4] < log_T_stop: break        # locate the crossing on the last interpolant
sol = OdeSolution(ts, interpolants)         # one object replaces the fragment list
```

The termination point is found by a bracketed root of `ln T_J − ln T_stop` on the last step's
interpolant (what `solve_ivp`'s event machinery does), and the z-grid sampling
(`ScalarModel.py:865–908`) evaluates one `OdeSolution` instead of walking fragments. Memory: a
full history is 3 000–35 000 steps, each interpolant a `5 × 3` array. Turning points and any
other per-step diagnostics fall out of the loop for free (Finding E).

### 9.3 Measured cost of the recommended loop

Full histories from `main.py`'s initial data, `f = 0.1`, global cap `0.1`, Jacobian factor clamp
`1e-4`, `atol = rtol = 1e-8`, no fragments, no events, no reflections (`p_full.py β M 1e-8 1e-4 kin`):

| β | M | RHS | steps | wall | wall bounces | first bounce N / T_J | N at T_CMB | shipped scheme (brief) | speed-up |
|---|---|---|---|---|---|---|---|---|---|
| 0.9 | 0.5 | 26 274 | 3 035 | 4 s | 16 | 36.159 / 1.0 eV | 44.32 | 0.06×10⁶ | 2.3 |
| 1.2 | 0.5 | 24 103 | 2 727 | 5 s | 17 | 17.662 / 231.1 MeV | 45.78 | 1.52×10⁶ | 63 |
| 2.0 | 0.5 | 40 548 | 4 476 | 8 s | 26 | 20.343 / 746.6 MeV | 49.71 | 2.78×10⁶ | 69 |
| 3.0 | 0.5 | 57 008 | 6 120 | 11 s | 48 | 24.484 / 1 683 MeV | 54.59 | 3.13×10⁶ | 55 |
| 1.2 | 0.01 | 108 578 | 11 461 | 14 s | 195 | 17.668 / 231.1 MeV | 46.04 | 5.90×10⁶, **failed** at N = 37.165 | > 54, and completes |
| 2.0 | 0.01 | 98 106 | 10 623 | 15 s | 196 | 20.352 / 746.7 MeV | 50.04 | ≥ 27×10⁶, unfinished after 2 h | > 275 |
| 3.0 | 0.01 | 151 454 | 16 448 | 21 s | 285 | 24.499 / 1 680 MeV | 55.03 | ≥ 22×10⁶, unfinished | > 145 |
| 2.0 | 0.001 | 272 311 | 27 963 | 30 s | 804 | 20.352 / 746.7 MeV | 50.08 | ≥ 28×10⁶, unfinished | > 103 |
| 3.0 | 0.001 | 328 147 | 34 413 | 34 s | 1 016 | 24.499 / 1 680 MeV | 55.07 | — | — |

No trial-state exception and no Jacobian-probe substitution occurred in any of the nine. The
first-bounce `N` and `T_J` agree with the brief's roster (its §2 table) in every case, and
between `M` values to `< 1e-4` in `ln T_J` as the brief found. With the floor-triggered
reflection enabled these nine are unchanged (no reflection fires above `M = 1e-8`); the
`M = 1e-6` history completes in 3.3×10⁶ RHS with 7 429 resolved bounces, β = 0.9 at `M = 1 eV`
completes with 140 reflections in 1.9×10⁵ RHS, and β ≥ 1.2 at `M ≤ 1e-10` does not complete
for the reason given in §3.7. Cost per e-fold: about `600` RHS
outside the bounce phases, `1.3×10³` parked, `8×10³` in the grazing rebounds; the small-`M`
histories are dominated by the hundreds of matter-era rebounds, each resolved rather than
stepped over. At `M = 0.01` the whole history costs what the shipped scheme spends on 0.1 e-fold
of L2.

### 9.4 What stays open

- **Settling at physical `M`: a parked-tracking model (§3.7).** This is the one item without
  which physical-`M` histories for β ≥ 1.2 cannot be produced by any integrator. It needs a
  criterion for when the bouncing field is declared parked (a bounce amplitude or a bounce
  period relative to the sample spacing), the tracking solution `φ = φ_wall(ρ(N))`, and a
  statement of what the parked field contributes to `ρ_φ`, `p_φ` and the adiabatic diagnostic.
  The reflection count and the bounce statistics the loop records are the inputs to that
  criterion.
- **The value of `f`.** `0.1` and `0.02` give the same first bounce to `2×10⁻⁵` relative in
  `φ_min` (§3.1 table, P1); `0.02` costs 1.5×. `0.1` is recommended; the campaign's acceptance
  test should show convergence in `f`.
- **Jacobian.** The clamp is a one-liner and is enough. An analytic Jacobian for the stiff
  `(φ, π)` block (`V''` and the kick's `φ`-dependence, both available from the potential and
  coupling classes) with finite differences for the slow components would remove `num_jac` and
  its factor logic entirely; worth doing if Newton failures show up in the science run, not
  before.
- **Tolerance.** Keep `1e-8` everywhere and drop the relaxed-tolerance text from the paper. The
  `atol` vector is a refinement (§6).
- **Sampling.** Record turning points alongside the z grid (Finding E); a decision for the
  authors, since it changes what BBN and the adiabatic stage see.

### 9.5 Acceptance tests (from the brief's §9, sharpened by the probes)

Pointwise, reproducible under any change to step control, tolerance or cap:

- P1 at `M = 0.5, 0.01, 0.001` from the state in §2.2: first bounce `N` to `1e-5`, `φ_min` to
  `1e-4` relative, `φ` and `π` at `N = 21` to `1e-5`, against the `1e-12` reference
  (`20.343028`, `4.57371e-3`, `1.220383e-1`, `−1.2972e-1` at `M = 0.5`). Under a second each.
- P3 from its state to `N = 37.5`: 51 wall bounces, `φ` at `N = 37.5` to `1e-4`. Two seconds.
- No hard reflection, no `φ ≤ 0`, no trial-state substitution in any of the above.
- The nine full histories of §9.3 complete, with first bounces matching the table to `1e-3` in
  `N`.

Statistical only (the brief's P4): `φ_park`, rebound counts, D/H and Yp over a β window. The
brief's spread of ±1.2 % in D/H from chaos at β = 1.90–2.10 stands as the yardstick. Specify for
the science run; do not run it in the campaign.

---

## 10. A remedial campaign, scoped as prompts

Suggested name `integrator-remediation`, under the `prompts/<campaign>/` conventions of
`CLAUDE.md`. Every prompt that changes a stored history needs the `VERSION_LABEL` bump; prompt 01
does, so the bump (to `2026.5.0`) lands there and 02–04 ship under it. The campaign folder is not
created by this audit: it needs the user's decisions on `f`, the fallback, and the sampling
(§9.4).

1. **Replace the fragment loop with the kinematic-cap step loop** (§9.2): custom `Radau` loop,
   global cap `0.1`, `f = 0.1`, `h_floor = 1e-11` with the floor-triggered elastic reflection
   (§9.1, counted and stored in place of `HARD_REFLECTIONS_KEY`), Jacobian-factor clamp,
   trial-state exception → rejection with a floor, `φ ≤ 0` → `ComputationFailureError`,
   termination by root on the last interpolant, one `OdeSolution` for sampling. Remove the six events, `SolutionFragment`, the level-1/2 machinery
   and its supervisor counters; store the cap parameters in `extra_data` in place of the region
   metadata. `VERSION_LABEL → 2026.5.0`. Acceptance: §9.5 pointwise tests; the P1 tests must
   *fail* on `HEAD~1` only in cost (the shipped scheme passes them at 12–100× the RHS), so the
   breaking test is the P3 fragment count and the β = 1.2, M = 0.01 completion.
2. **Delete the solver fallback and settle the exception taxonomy** (§4, §5): one stepper, one
   label; the table in §5; fix `data.d_logV_dphi`; make `RHS_timer.__exit__` quiet. Acceptance: a
   history that misses a reflection (force it with `f = 10`) becomes a failure row, not a stored
   history, and `main.py`'s work pool is not stopped by it.
3. **Tests** in `ComputeTargets/tests/`: the §9.5 pointwise tests as `unittest` cases from the
   §2.2 states (no Ray, no datastore, seconds), a step-over test with the cap disabled that must
   raise, and a Jacobian-probe test (P2 window, zero `T_Jordan = 0` substitutions).
4. **Documents**: dated addenda to `.documents/numerical-strategies.md` §2–3 and
   `architecture-summary.md`; a list of the `NumericalSection` corrections for the authors
   (outside cap, region caps, relaxed tolerances, fallback sequence, hard reflection).
5. **Optional, on the user's decision**: turning-point sampling (Finding E), the `atol` vector,
   the analytic Jacobian.
6. **Physics, for the authors before any physical-`M` run**: the parked-tracking model of §9.4,
   with its switch criterion and its contribution to the BBN and adiabatic stages. Until it
   exists, physical-`M` histories with β ≥ 1.2 should be expected to fail (cleanly, with a
   failure row) rather than to run for days.

Issues this audit would open if a campaign is planned (none opened here; `OPEN_ISSUES.md` is
untouched): the hard-reflection outcomes (§3.5), the trial-state exception policy (§5), the
Jacobian-factor growth (§8 F2), the `d_logV_dphi` field (§1), and the paper–code mismatches (§1).

---

## 11. Where this audit corrects the brief

- `solver_label` is correct for every stored row (§4).
- The "wild trial states" are SciPy Jacobian probes with an unbounded perturbation factor, not
  rejected Newton iterates, and they are a failure source (§8 F2).
- The hard-reflection fallback is not merely rarely useful; it cannot recover for this potential,
  and one of its two outcomes is a silently wrong stored history (§3.5).
- `βπ` reaches −1.05 to −1.07, not −1.008, and `T_J` rises 12–13 % (§8 F1).
- Restart overhead is under 20 RHS per fragment, and every fragment starts at the cap (§3.3).
- A velocity-only cap, the brief's candidate, is not sufficient; the inward acceleration must be
  capped too (§9.1).
- The parked phase is not chaotic at the tolerance level over 15 e-folds: six variants agree on
  `φ(N = 40)` to seven digits (§3.2). Chaos sets in with the fast delivery rebounds, as the
  divergent `φ` at `T_CMB` between `1e-8` and `1e-10` full histories shows
  (`0.2140` vs `0.2090` at β = 2, M = 0.5).

And one correction to this audit's own first pass, made the same day: §3.5 originally concluded
that the hard reflection "cannot recover". That holds for `M ≳ 1e-13`; for the physical `M` it
was written for it is the correct model and works (§3.5, §3.7), and the recommended loop keeps
it, with a trigger tied to the representable step rather than to `φ = 0`.

## Appendix: the shipped scheme's own numbers on the probe windows

For the record, from `p1_sweep.py a`, `p2_parked.py regions`, `p3_grazing.py regions`
(tol `1e-8`, `b1f64d8`):

| window | RHS | accepted steps | fragments | L2 entries | hard reflections | wall |
|---|---|---|---|---|---|---|
| P1 20.0016 → 21.0 | 17 092 | 2 405 | 3 | 1 | 0 | 3–6 s |
| P1 at M = 0.01 | 26 634 | 3 758 | 5 | 1 | 0 | 7 s |
| P1 at M = 0.001 | 26 422 | 3 728 | 5 | 1 | 0 | 7 s |
| P2 25.003 → 40 | 2 099 582 | 299 939 | 1 | 0 (already inside) | 0 | 253 s |
| P3 32.8965 → 37.5 | 5 188 284 | 741 086 | 85 | 42 | 0 | 618 s |

The event-count invariant (`!= 1`, `:702`) never tripped in 85 + 5 + 3 fragments; the tangential
grazing case the brief describes remains a reasoned risk, moot once the events are removed.
