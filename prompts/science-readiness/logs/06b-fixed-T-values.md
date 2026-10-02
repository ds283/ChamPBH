# Log 06b — Store the fixed-temperature values on the `ScalarModel` row

**Prompt:** prompts/science-readiness/06b-fixed-T-values.md
**Commit:** the commit that adds this file ("Store phi and the NP ratio at 1 MeV and 70 keV on ScalarModel"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Worked on top of `336630a`. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION`
(`"bf24c3d+ri02+sr01"`) are unchanged (README §0.2 P9; no bump). Four columns are added to the
`ScalarModel` table. Every deviation below is an `IMPLEMENTATION CHOICE`; none touches a §2
design fact.

## What shipped

**`ComputeTargets/ScalarModel.py`**
- `:77–78`, new module constants `FIXED_T_JORDAN_HIGH_MEV = 1.0` and
  `FIXED_T_JORDAN_LOW_MEV = 0.07`. They are not a run option and not part of the lookup key.
- `:862`, new `FixedTValues = namedtuple("FixedTValues", ["phi_Einstein_1MeV",
  "density_NP_ratio_1MeV", "phi_Einstein_70keV", "density_NP_ratio_70keV"])`. φ is in the
  cosmology's units and the ratio is dimensionless. Where the history does not reach a
  temperature, both of its fields are `None`.
- `:873`, new private `_T_Jordan_crossing_step(result, log_T) -> Optional[(k, N)]`. It walks
  `result.solution.ts` and `.interpolants` in order. On each step it evaluates
  `g = interpolant(t)[4] − log_T` at both ends. It returns the first step on which `g` is zero
  at an end, or changes sign. The root is found by `brentq(..., xtol=1e-15)` on that step's
  interpolant.
- `:901`, new `T_Jordan_crossing(result: IntegrationResult, log_T: float) -> Optional[float]`.
  It returns the `N` from `_T_Jordan_crossing_step`, or `None`.
- `:924`, new `fixed_T_value_at(result, k, N, policy, coupling, units) -> (phi, ratio)`. It reads
  the state at `N` from step `k`'s interpolant. It builds `H_J` with `policy` and
  `HubblePolicy(coupling, units)`, `ln ρ_R,J = ln ρ_R,E − 4 ln Ω` and `f_m = exp(log_fm)`, as the
  sampling loop does. Then, as `compute_BBN_data` does at `BBNData.py:455–465`, it returns
  `(3 M_P² H_J² − ρ_R,J (1 + f_m)) / ρ_R,J`.
- `:957`, new `fixed_T_values(result: IntegrationResult, policy: ODEPolicy, coupling:
  AbstractCoupling, units: UnitsLike) -> FixedTValues`. It applies the two functions above at
  `log(1 MeV)` and `log(70 keV)`.
- `compute_scalar_model`, `:1195`: `fixed_T = fixed_T_values(result, policy, coupling, units)`
  is called after `first_bounce`. The success payload carries `"fixed_T_values": fixed_T`
  (`:1211`).
- `ScalarModel`:
  - `_fixed_T_values` is set in `__init__` from `payload["fixed_T_values"]` (`None` with no
    payload), and in `store()` from `data["fixed_T_values"]` (`None` on a failure).
  - New property `fixed_T_values -> FixedTValues` (`:1451`). It raises on a failure row and on
    an unpopulated object, as `first_bounce` does. It does not check `_do_not_populate`.

**`Datastore/SQL/ObjectFactories/ScalarModel.py`**
- Four new nullable `Float(64)` columns, after `first_bounce_reflected`: `phi_Einstein_1MeV`,
  `density_NP_ratio_1MeV`, `phi_Einstein_70keV` and `density_NP_ratio_70keV`. The φ columns hold
  φ/M_P; the ratio columns hold the ratio as it is.
- `store` writes them. They are NULL where a temperature is not reached, and all four are NULL
  on a failure row.
- `build` selects them on every lookup, with or without `_do_not_populate`. On a success row it
  hands `ScalarModel` a `FixedTValues`, with φ multiplied back by `PlanckMass` and `None` for
  NULL. On a failure row it hands `None`.
- The table now has 33 columns of its own (29 + 4), plus `serial`, `version` and `timestamp`.

**`tools/history_and_bbn.py`**
- A new `fixed_T` line after the `bounce` line. For each of 1 MeV and 70 keV it prints:
  - `crossings`: the sign changes of `ln T_J − ln T*` across the accepted steps' end points;
  - `phi` (M_P) and `ratio`: the stored `fixed_T_values`;
  - `stand_in_phi` and `stand_in_ratio`: linear interpolation in `ln T_J` between the two stored
    samples either side, with the ratio computed per sample by `compute_BBN_data`'s expression.
- To count crossings, the driver keeps the `IntegrationResult`. It wraps
  `ComputeTargets.ScalarModel.integrate_scalar_history` for the duration of the
  `compute_scalar_model._function` call and restores it in a `finally`.
- New helpers: `_sample_ratio`, `_sign_changes`, `_stand_in`, `_fmt` and `_fixed_T_line`. The
  docstring documents the line.

**Tests**
- `ComputeTargets/tests/test_fixed_T_values.py` (new): tests (a), (b) and (c).
- `Datastore/tests/test_fixed_T_values_round_trip.py` (new): test (d), in five tests.
- `Datastore/tests/test_first_bounce_round_trip.py`: `_success_payload(bounce,
  fixed_T=NO_FIXED_T)` now carries `"fixed_T_values"`, with `NO_FIXED_T =
  SM.FixedTValues(None, None, None, None)`. `ScalarModel.store()` reads that key, so the
  existing success payload needed it. Its five tests are otherwise unchanged.

## Deviations from the prompt

### 1. Two helpers behind the two functions — IMPLEMENTATION CHOICE

The prompt names `T_Jordan_crossing` and `fixed_T_values`. Test (a) evaluates "the state and
ratio there with the function behind `fixed_T_values`". So the shipped code has two helpers:
- `_T_Jordan_crossing_step` returns the step index `k` with `N`;
- `fixed_T_value_at(result, k, N, …)` gives `(φ, ratio)` from step `k`'s interpolant.

**Why the step index is kept.** At a step boundary the two adjacent interpolants can differ:
- by rounding, on any boundary;
- in π's sign, at a reflection.

With the index, the state is read from the step the crossing was found on, as `first_bounce`
reads it. The two alternatives were rejected:
- calling `result.solution(N)` lets `OdeSolution` pick the segment, and at a node it may pick the
  other one;
- a `bisect` on `ts` has the same ambiguity.

`T_Jordan_crossing`'s signature is the prompt's.

### 2. The `IntegrationResult` is captured by wrapping `integrate_scalar_history` — IMPLEMENTATION CHOICE

Two consumers need the `IntegrationResult`:
- test (a), which needs the result of a full history and its stored samples;
- the driver, which counts sign changes.

`compute_scalar_model` does not return the result. For the duration of the call, both replace
`integrate_scalar_history` in the module with a wrapper that records its return value. The test
uses `mock.patch.object`; the driver restores the original in a `finally`. No production code
changes for this.

The alternatives were rejected:
- adding the result to the payload would send the whole `OdeSolution` through Ray and change
  `compute_scalar_model`'s return beyond §2 (n);
- re-running `integrate_scalar_history` separately would integrate every history twice.

### 3. The value on a success row is always a `FixedTValues` — IMPLEMENTATION CHOICE

On a success, `compute_scalar_model` always returns a `FixedTValues`, even when it is
`(None, None, None, None)`, and `build` reconstructs one on every success row. So
`fixed_T_values` returns a tuple on a success and raises on a failure. A caller can therefore
always index the fields. The alternative was `None` when nothing is reached. That would give
callers two kinds of "not reached" to tell apart.

### 4. `store()` reads `data["fixed_T_values"]` strictly, and one existing test helper was updated — IMPLEMENTATION CHOICE

`ScalarModel.store()` indexes the key, as it does `"first_bounce"`. It does not use
`data.get(...)`, because a success payload without it is a bug. The one test payload that
lacked it gained the key: `test_first_bounce_round_trip._success_payload`, in an allowed file.

### 5. The stand-in's bracketing pair — IMPLEMENTATION CHOICE

The prompt says "the two stored samples either side". The driver takes the **first** adjacent
pair of samples, in sample order (increasing `N`), whose `ln T_J − ln T*` values have opposite
signs or include a zero. This matches "first crossing". It interpolates φ and the per-sample
ratio linearly in `ln T_J`.

### 6. Wall bounces counted by a scratch wrapper, not by the driver — IMPLEMENTATION CHOICE

Acceptance 3 includes wall bounces, but the driver prints none, and the prompt does not ask it
to. A scratch wrapper captured the `IntegrationResult` the same way as deviation 2. It ran the
driver's `main` and printed `len(test_kinematic_cap_loop.wall_bounces(result, M))`. This
follows log 03, which got its wall-bounce counts from a scratch probe.

## Verification performed

All runs from the repository root with `venv/bin/python`. The machine was unloaded and the
histories ran one at a time.

**Suites** (the three commands of README §5 rule 6):

| | CosmologyModels | ComputeTargets | Datastore |
|---|---|---|---|
| before (`336630a`, run by me) | 18 OK (70.0 s) | 88 OK (85.3 s) | 26 OK (1.9 s) |
| after (this tree) | 18 OK (70.3 s) | **91** OK (86.1 s) | **31** OK (2.3 s) |

The counts went up by +3 in ComputeTargets (tests (a)–(c)) and +5 in Datastore (test (d)). No
test was deleted. `black --check` is clean on all six changed files.

**The new tests fail on `HEAD~1`.** I stashed the three production files, which put
`ComputeTargets/ScalarModel.py`, the factory and the driver back to `336630a`, and ran the new
modules with the new tests in place:
- `ComputeTargets.tests.test_fixed_T_values`: `FAILED (errors=3)`. The errors were
  `AttributeError: module 'ComputeTargets.ScalarModel' has no attribute 'T_Jordan_crossing'`
  (twice) and `'_T_Jordan_crossing_step'` (once).
- `Datastore.tests.test_fixed_T_values_round_trip`: import error, `AttributeError: … has no
  attribute 'FixedTValues'`.

I then restored the stash.

**README §6.7b, row by row.** Margins are from a scratch probe on this tree that ran the test's
own functions.

| row | target | measured |
|---|---|---|
| `T_Jordan_crossing` at three samples' own `ln T_J` (β = 2, M = 0.5, full history; 324 samples in (0.07, 1) MeV; chosen: 0.998939, 0.244485 and 0.0701009 MeV) | `raw_N` to 1e-10; φ to 1e-9 rel.; ratio = `compute_BBN_data`'s expression to 1e-8 rel. | \|ΔN\| = 0, 3.55e-15, 0; φ rel. 0, 4.53e-15, 0; ratio rel. 0, 7.04e-13, 0 (ratios −0.0483915, −0.00961481, 0.0682227) |
| a temperature the history does not reach (P1 window, M = 0.5, to N = 21; last T_J = 0.331 GeV) | `None` in all four fields | `FixedTValues(None, None, None, None)`. Targets 0.1 above the first `ln T_J` and 0.1 below the last give `None`. A target inside the window gives a crossing (positive control) |
| a crossing on the step that ends at a reflection (P1, M = 1e-10, reflection at N = 20.352100380349, step k = 209) | found on that step; `ln T_J` continuous to 1e-12 | found on step 209, strictly inside it; \|Δ ln T_J\| across the reflection = 0 (π −0.4976 → +0.4976). The reflection's own `ln T_J` is found on step 209 at N_r to 1e-12. A target on the next step is found on step 210, and its values are read from the post-reflection state |
| round trip, temporary SQLite store | four values to the last bit; `None` → `None`; a failure row raises; `_do_not_populate` read returns the values | as stated: `assertEqual` on every float; the 70 keV pair NULL → `None`; failure row: four NULLs, raises with and without `_do_not_populate`; the `_do_not_populate` read has `_values is None`, `.values` raises, and `fixed_T_values` returns all four, also through a fresh connection |
| the driver, three histories | one crossing of each temperature; the four values beside the stand-in; trajectory unchanged | below |

**Acceptance 2 and 3: the driver, unloaded, one at a time.** Each history ran through
`scratchpad/driver_with_walls.py`, which ran `tools/history_and_bbn.py`'s `main` and then counted
`wall_bounces` (deviation 6). The `ratio` lines are omitted below; they are unchanged.

```
history beta=2 M=0.5: RHS=40580 accepted_steps=4469 reflections=0 samples=5392 wall=1.6 s
bounce beta=2 M=0.5: N=20.343026853 T_J=746.634744 MeV phi=4.573705e-03 reflected=False
fixed_T beta=2 M=0.5: 1MeV crossings=1 phi=1.138197048e-02 ratio=-4.810565953e-02 stand_in_phi=1.138177098e-02 stand_in_ratio=-4.810748705e-02 | 70keV crossings=1 phi=8.695012936e-03 ratio=6.742167855e-02 stand_in_phi=8.694242406e-03 stand_in_ratio=6.742975209e-02
bbn beta=2 M=0.5: Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487 network=full PRyM_time=8.0 s wall=8.0 s PRyM_version=bf24c3d+ri02+sr01
walls beta=2 M=0.5: wall_bounces=26

history beta=2 M=0.001: RHS=271783 accepted_steps=27979 reflections=0 samples=5435 wall=7.3 s
bounce beta=2 M=0.001: N=20.352082230 T_J=746.686275 MeV phi=9.150507e-06 reflected=False
fixed_T beta=2 M=0.001: 1MeV crossings=1 phi=3.524341402e-03 ratio=-7.814304755e-02 stand_in_phi=3.524309993e-03 stand_in_ratio=-7.814332533e-02 | 70keV crossings=1 phi=6.541189438e-04 ratio=1.579537539e-03 stand_in_phi=6.528806368e-04 stand_in_ratio=1.602218054e-03
bbn beta=2 M=0.001: Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365 network=full PRyM_time=8.2 s wall=8.2 s PRyM_version=bf24c3d+ri02+sr01
walls beta=2 M=1e-3: wall_bounces=803

history beta=1.6 M=1e-05: RHS=1445132 accepted_steps=137137 reflections=0 samples=5219 wall=36.7 s
bounce beta=1.6 M=1e-05: N=18.974433718 T_J=420.758153 MeV phi=9.316270e-08 reflected=False
fixed_T beta=1.6 M=1e-05: 1MeV crossings=1 phi=1.418937374e-03 ratio=-1.043543235e-02 stand_in_phi=1.418654985e-03 stand_in_ratio=-1.043677789e-02 | 70keV crossings=1 phi=1.313754026e-04 ratio=-6.242391165e-03 stand_in_phi=1.309854365e-04 stand_in_ratio=-6.236574824e-03
bbn beta=1.6 M=1e-05: Yp=0.2468788501 DoH=2.4647705 He3oH=1.042121506 Li7oH=5.419865323 network=full PRyM_time=8.3 s wall=8.4 s PRyM_version=bf24c3d+ri02+sr01
walls beta=1.6 M=1e-5: wall_bounces=4337
```

- **One crossing of each temperature on every history** (`crossings=1` six times). Prompt 06b's
  first stop condition does not apply.
- **Trajectory unchanged (acceptance 3).** RHS, accepted steps, reflections, samples and the
  first bounce's `N`, `T_J` and φ are identical, to every printed digit, to the orchestrator
  baseline at `336630a`. The wall-bounce counts, 26, 803 and 4 337, equal log 03's scratch-probe
  counts. The orchestrator baseline does not list wall bounces. The BBN abundances also equal log
  03's to every printed digit.
- **Dense output against the sample-interpolated stand-in.** This is a measurement, not a bound,
  and it is the `HEAD~1` stand-in:

  | history | φ 1 MeV rel. | ratio 1 MeV rel. | φ 70 keV rel. | ratio 70 keV rel. |
  |---|---|---|---|---|
  | β = 2, M = 0.5 | 1.75e-5 | 3.80e-5 | 8.86e-5 | 1.20e-4 |
  | β = 2, M = 10⁻³ | 8.91e-6 | 3.55e-6 | 1.89e-3 | 1.44e-2 |
  | β = 1.6, M = 10⁻⁵ | 1.99e-4 | 1.29e-4 | 2.97e-3 | 9.32e-4 |

  The differences come from the linear interpolation between samples 1/250 decade apart in 1 + z.
  The largest is the 70 keV ratio at M = 10⁻³: 1.58e-3 against 1.60e-3, a small value between
  two samples.

What I reasoned and did not run:
- At a sample's own `N`, the stored ratio is the one BBN sees. Test (a) shows this to 7e-13. The
  1 MeV and 70 keV values are not at a sample, so BBN never evaluates the same number.

## Observations not acted on

None that is actionable. `[05-the-value-factory-compares-stored-phi-against-pi]` cites
`Datastore/SQL/ObjectFactories/ScalarModel.py:1008` on `a522005`. The four columns move that
line to `:1064` on this tree. The board entry names its tree, so it is still correct and
was left alone.

## State handed to the next prompt

- **Signatures** (in `ComputeTargets/ScalarModel.py`; the package does not re-export them, so
  import from the module):
  - `T_Jordan_crossing(result: IntegrationResult, log_T: float) -> Optional[float]`: the `N` of
    the first crossing of `ln T_J = log_T` on the dense output, or `None`.
  - `fixed_T_values(result: IntegrationResult, policy: ODEPolicy, coupling: AbstractCoupling,
    units: UnitsLike) -> FixedTValues`.
  - Helpers: `fixed_T_value_at(result, k, N, policy, coupling, units) -> (phi, ratio)` and the
    private `_T_Jordan_crossing_step(result, log_T) -> Optional[(k, N)]`.
  - Constants: `FIXED_T_JORDAN_HIGH_MEV = 1.0` and `FIXED_T_JORDAN_LOW_MEV = 0.07` (MeV).
- **The namedtuple:** `FixedTValues(phi_Einstein_1MeV, density_NP_ratio_1MeV,
  phi_Einstein_70keV, density_NP_ratio_70keV)`. φ is in the cosmology's units (divide by
  `units.PlanckMass` for M_P). The ratio is ρ_NP/ρ_R,J, dimensionless. A pair is `None` where the
  history did not reach that temperature.
- **The columns** (on the `ScalarModel` table, after `first_bounce_reflected`, all nullable
  `Float(64)`):
  - `phi_Einstein_1MeV` and `phi_Einstein_70keV`, in M_P;
  - `density_NP_ratio_1MeV` and `density_NP_ratio_70keV`, dimensionless.

  They are NULL where the temperature is not reached, and all four are NULL on a failure row.
- **The property:** `ScalarModel.fixed_T_values -> FixedTValues`. On a success row it is always
  a tuple, possibly with `None` fields. It raises `RuntimeError` on a failure row and on an
  unpopulated object. It works on an object read with `_do_not_populate=True`, because `build`
  selects the four columns on every lookup. Prompt 07 can read it with `_do_not_populate` kept.
- **Payload:** `compute_scalar_model`'s success dict has `data["fixed_T_values"]`, and
  `ScalarModel.store()` requires it. A hand-built success payload in a test must carry it (see
  `Datastore/tests/test_first_bounce_round_trip._success_payload`).
- **The driver's `fixed_T` output on the three histories** (this tree, unloaded):

  ```
  fixed_T beta=2 M=0.5: 1MeV crossings=1 phi=1.138197048e-02 ratio=-4.810565953e-02 stand_in_phi=1.138177098e-02 stand_in_ratio=-4.810748705e-02 | 70keV crossings=1 phi=8.695012936e-03 ratio=6.742167855e-02 stand_in_phi=8.694242406e-03 stand_in_ratio=6.742975209e-02
  fixed_T beta=2 M=0.001: 1MeV crossings=1 phi=3.524341402e-03 ratio=-7.814304755e-02 stand_in_phi=3.524309993e-03 stand_in_ratio=-7.814332533e-02 | 70keV crossings=1 phi=6.541189438e-04 ratio=1.579537539e-03 stand_in_phi=6.528806368e-04 stand_in_ratio=1.602218054e-03
  fixed_T beta=1.6 M=1e-05: 1MeV crossings=1 phi=1.418937374e-03 ratio=-1.043543235e-02 stand_in_phi=1.418654985e-03 stand_in_ratio=-1.043677789e-02 | 70keV crossings=1 phi=1.313754026e-04 ratio=-6.242391165e-03 stand_in_phi=1.309854365e-04 stand_in_ratio=-6.236574824e-03
  ```

  Reproduce with `./venv/bin/python tools/history_and_bbn.py BETA M` for (2, 0.5), (2, 1e-3) and
  (1.6, 1e-5).
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 91, Datastore 31.
