# Campaign — science readiness: the BBN route, the stored observables, the figures

**Source:** a numerical-campaign re-evaluation written by a Claude Science agent on 2026-10-01
against `main` at `6aaa706`, kept beside this README as
[`source/campaign_reevaluation_2026-10-01.md`](source/campaign_reevaluation_2026-10-01.md). It
ran 38 full histories through the production code and proposed five code changes ("Phase A",
A1–A5) and four figures ("Phase D") before a production science run. **It is evidence to be
checked, not a specification** (`CLAUDE.md` rule 7). §0.3 below records where the planner checked
it against the tree and where it was wrong.

**Reproduction.** The planner's probes are in [`planning-probes/`](planning-probes/), run from the
repository root with `venv/bin/python`. Every "now" figure in §6 names the probe and the tree it
was measured on.

**Planned:** 2026-10-01 against `main` at `6aaa706`.
**Target branch:** `science-readiness`, cut from `6aaa706`. Planning and orchestration commits land
on the same branch.
**Status board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md) ·
**Logs:** [`logs/`](logs/) · **Orchestrator prompts:** [`orchestrator/`](orchestrator/)

---

## 0. What this campaign is, and its boundaries

### 0.1 The one-sentence version

Hand the scalar field to PRyMordial through the expansion rate alone, with a patched PRyMordial
that has no fictitious new-physics temperature and an optional wall-clock limit; replace the
aliased point samples PRyMordial integrates with bounce averages taken on the dense output; store
the failure reason and the first bounce of every `ScalarModel`; make the initial field value a run
option with a super-Planckian warning; narrow the BBN spline to the range PRyMordial reads; and add
the extraction and the figures the science run reports.

### 0.2 Decisions

**Ruled by the user, 2026-10-01** (in the planning conversation; recorded on the board):

- **U1. Replace the BBN route outright, by patching PRyMordial.** The `NP_thermo_flag` route was
  the least invasive way to make PRyMordial see the scalar field. It has no other use. The
  fictitious new-physics temperature `T_NP` goes with it.
- **U2. The wall-clock limit is part of the PRyMordial patch.** It is an optional parameter.
  Exceeding it ends the solve with an exception that the existing boundary returns as a failure.
- **U3. Bounce averages by Gauss–Legendre quadrature on the dense output at sampling time**
  (option (i-a) of the planning discussion). The trajectory and the step loop are untouched.
- **U4. The first bounce is stored in dedicated columns**, not in `extra_data`.
- **U5. The Phase D extraction and figures are in this campaign.**

**Proposed by the planner, 2026-10-01; ruled by the user the same day:** P1–P5 and P7–P9
accepted as proposed; **P6 overruled** and replaced by the text below. The board's Decisions
record the ruling.

- **P1. `p_NP` goes everywhere.** After U1 nothing reads it. The pressure callback, its ratio
  spline, `jordan_Hdot_over_H2`, the `ODEPolicy` call that gave π′, the `pressure_NP` field of
  `BBNDataValue`, its `pressure_NP_MeV4` column, and the two `plot_ScalarModel.py` panels that
  draw it (|p_NP| and w_NP = p_NP/ρ_NP) are removed. The density panels stay. The alternative is
  to keep `p_NP` as a stored diagnostic, still computed from Ḣ_J. It costs one `ODEPolicy`
  evaluation per BBN sample and keeps a test of a formula that no longer feeds anything.
- **P2. Revert the `cham03` patch.** It makes `dTNPdt` return 0. With `NP_thermo_flag` off,
  `dTNPdt` is never called, and the vendored diff should carry only what runs. The upstream body
  comes back. Its singularity is in a branch nothing enters, and `_configure_PRyMordial` asserts
  the flag is off.
- **P3. The wall-clock limit defaults to 600 s for production solves**, set in one constant, with a
  `--bbn-wall-clock-limit SECS` option on `main.py` (0 disables it). An unloaded full-network solve
  takes about 10 s (§6.1). The source quotes 34–120 s on a ten-core machine running nine at once,
  so 600 s is five times the worst loaded figure. A timeout is a failure row like any other. It is
  cached, and `--retry-failed-bbn` retries it. `compute_SM_baseline` runs with no limit and raises,
  as now.
- **P4. Fold two `run-integrity` issues into prompt 01.** Prompt 01 rewrites the function both
  live in. `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]`: fewer than four samples raises
  `ComputationFailureError`. `[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]`:
  a non-finite callback value raises `ComputationFailureError`.
- **P5. "The first bounce" is the first sign change of π from negative to positive along the dense
  output, located as the root of π on that accepted step's interpolant (the user's ruling on the
  integrator campaign).** If an elastic reflection comes first, the first bounce is that
  reflection and is flagged as one. If neither happens, the columns are NULL. No `φ < 1.5 M`
  filter is applied. For the exponential potential the wall is the only outward force, so every
  negative-to-positive turning point is a wall bounce. Prompt 03's test shows it agrees with the
  test helper's `φ < 1.5 M` rule on every history it is run on.
- **P6 (as ruled by the user). A super-Planckian start is warned about, never skipped or
  refused.** A coupling with `ln Ω(φ*) + ln T* > ln M_P` (the review's H8 condition, A*T* > M_P)
  is printed as a warning before step 1, and computed like every other coupling. The user's
  reason: there can be numerical reasons to begin slightly super-Planckian (to kill transients),
  the scenario's premise is that the final state does not depend on the initial one, and
  dropping or refusing such points brings the proposed science run no benefit. The planner had
  proposed skipping them.
- **P7. The averaging cell** for sample `i` runs between the midpoints to its neighbours in `N`,
  clipped to the history's first and last `N`. The cells partition the history. Quadrature is
  three-point Gauss–Legendre on each piece of an accepted step inside a cell. Two quantities are
  averaged: `H_J²` and `φ_E`. The average is in `N`, not in cosmic time. The two differ at second
  order in the fractional oscillation of `H` over a cell; prompt 05 measures it.
- **P8. The BBN spline floor moves from 0.1 eV to 0.2 keV.** PRyMordial queries the callbacks
  down to 0.363 keV (§6.1). The pre-check rule `T_stop ≤ 0.1 × floor` is kept, so a history must
  reach 20 eV. This closes `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]`. It also
  makes the source's physical-`M` cross-check possible with `--T-stop-GeV 1e-8`. The source's
  "about 1 keV" would trip the domain guard on every solve.
- **P9. One version bump, to `"2026.6.0"`, in prompt 01.** That is the prompt that changes every
  `BBNData` row. Prompts 02–07 change the schema and the stored values under the same label, as
  earlier campaigns did, because no production store is made in between. **Every store made before
  2026.6.0 is invalid**, and because columns are added with no migration (the datastore has none),
  **the science run needs a fresh datastore file.** An old file cannot be opened by the new code.

### 0.3 What the planner checked in the source, and what it found

- **Its overlap with the repository holds.** β = 2, M = 10⁻⁵ gives 1 679 987 RHS, as in
  verification §4.9. The first bounce at β = 2 is 746.69 MeV for M ≤ 10⁻², as in §4.8–§4.9.
- **"The `NP_thermo_flag` route is unusable on 38/38" was shown under load, but it holds.** The
  source gave that route a 90 s limit while its other route took 34–120 s under the same load.
  Unloaded, the planner's probe finds the shipped route does not finish in 900 s on β = 2 at
  M = 0.5 or 10⁻³, where the Hubble-only route takes 10 s (§6.1). The route is replaced in any
  case (U1), on physical grounds. The repository's own conservation argument
  (`numerical-methods-for-paper.md` §4) says the plasma must obey the Standard-Model equation, and
  the Hubble-only route makes that exact rather than true to `≈ r·1.2×10⁻³`.
- **The source's one PRyMordial failure (β = 1.6, M = 10⁻⁵) did not reproduce.** On `6aaa706`, with
  the full network and the production spline floor, the Hubble-only route completes (§6.1). The
  noise it blamed is there as described.
- **"Record turning points" does not de-alias BBN.** PRyMordial needs `⟨H²⟩` over a bounce, and
  turning points give `φ_min` and `φ_max`. Hence U3.
- **"Raise the spline floor to about 1 keV" is wrong.** See P8.
- **`extra_data` is JSON in a `String(256)` column.** The worst case today is about 187
  characters; three more floats take it past 256. Hence U4.
- **φ\* = 5 is hard-coded in three drivers, not one:** `main.py:837`, `plot_by_beta.py:834` and
  `plot_ScalarModel.py:1671`. `phi_Einstein_init` is already part of the `ScalarModel` lookup key
  (`main.py:168`, `plot_by_beta.py:681`), so the store tag the source asks for is not needed.
- **Phase D needs no new compute target.** `BBNDataValue.density_NP_ratio` is stored per sample.
  After prompt 05 it is the bounce-averaged ratio. `T_deliver` is prompt 03's column, and
  φ at fixed `T` is prompt 05's averaged φ. What is missing is the extraction and the figures.

### 0.4 Correctness is the only objective

As in the last four campaigns, **a test that passes both before and after a prompt proves
nothing.** Each prompt names the test that must fail on `HEAD~1`, or the measurement on `HEAD~1`
that stands in for one where the feature is new. The orchestrator runs that check itself. The
histories are chaotic after delivery, so pointwise acceptance is confined to what is reproducible:
the trajectory before delivery, the first bounce, and the BBN outcome of a named history on a
fixed tree.

### 0.5 What this campaign does *not* do

- **It does not run the pipeline.** No `main.py` run, no datastore beyond a temporary SQLite file
  in a test, no Ray cluster. Full histories are run through the undecorated `._function`s by the
  driver prompt 01 adds (§2 (m)), as acceptance, not as tests. Nothing a prompt runs takes longer
  than a full history at `M = 10⁻⁵` plus a few PRyMordial solves.
- **It does not change the field equation, the RHS on any state, the step loop, the tolerances or
  the z grid.** Prompt 05 adds quadrature to the *sampling*. Prompt 03 reads the `OdeSolution`
  after the loop. Every trajectory is bit-identical to `6aaa706`'s.
- **It does not change the adiabatic stage.** `[post-adiabatic-Q-reads-aliased-late-samples]`
  stays open. The handover says not to report max |Q| for M ≲ 10⁻³ until it is settled.
- **It does not supply the parked-tracking model** for physical `M`.
- **It does not run the source's physical-`M` cross-check.** A physical-`M` history costs about
  24 minutes. Prompt 06 makes it possible; the run is the user's.
- **It does not edit `Paper1.tex`**, which is in another repository. Prompt 08 adds rows to
  `.documents/paper-corrections-numerical-section.md`.
- **It does not touch `thirdparty/`.** `PRyM/` is patched only as §2 (a)–(b) say, and every patch
  is marked with a comment naming this campaign and its prompt.

---

## 1. What this campaign lands

| ID | Severity | Description | Prompt |
|---|---|---|---|
| **R** | **DEFECT, medium** (wrong physics representation; a cancellation that fails noisily) | With `NP_thermo_flag`, PRyMordial adds −3H(ρ_NP + p_NP) and dρ_NP/dT to the plasma's dT_γ/dt and integrates a third variable `T_NP` that nothing reads. The plasma is meant to obey the Standard-Model equation. That holds only through a cancellation between two spline-derived terms, accurate to `≈ r·1.2×10⁻³`. Wherever the spline derivative is noisy, the denominator dT_γ/dt is built from is perturbed. | 01 |
| **W** | **GAP** (a hung BBN task blocks a survey) | Nothing bounds a PRyMordial solve's wall time. `run-integrity` closed the NaN hang route, but a solve that slows without failing still occupies a worker indefinitely. | 01 |
| **O** | **GAP** (unphysical output stored as a result) | A successful PRyMordial return is stored whatever it holds. `plot_by_beta.py` drops non-positive abundances at plot time, after they were stored as results. | 01 |
| **F** | **GAP** (survey bookkeeping) | A `ScalarModel` failure row stores `{"failure": True}` only; the reason is printed and lost. Closes `[00-scalarmodel-failure-rows-carry-no-reason]`. | 02 |
| **T** | **GAP** (an observable that cannot be read back) | The first bounce (`N`, `T_J`, `φ_min`) is not stored. At small `M` the bounce lasts far less than one z-grid sample, so it cannot be recovered from the samples: the source's sample-based detector gives 0.138 GeV for 0.747 GeV. | 03 |
| **P** | **GAP** (review H8) | φ\* = 5 M_P is a literal in three drivers. Nothing warns when A*T* > M_P. Closes `[00-initial-field-value-is-hard-coded-and-unchecked]`. | 04 |
| **A** | **DEFECT, medium at M ≲ 10⁻⁴** (aliasing reaches PRyMordial) | Below a few keV the z grid samples the matter-era bounces at random phase. The stored ρ_NP/ρ_R,J then jumps from sample to sample by ±0.4 % (M = 10⁻⁵) to ±0.85 % (M = 10⁻³) around a median ten times smaller, and PRyMordial integrates a spline through that noise (§6.1). The source records one PRyMordial failure from it (β = 1.6, M = 10⁻⁵); the planner did not reproduce the failure, only the noise. Narrows `[00-stored-samples-alias-the-rebounds]` to its adiabatic half. | 05 |
| **L** | **DEFECT, low** | The BBN spline runs from 100 MeV to 0.1 eV, while PRyMordial reads 10 MeV to 0.363 keV. Closes `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]`. | 06 |
| **G** | **GAP** | No extraction or figure for the science run's four figures: Δ abundances against β with a running median and band, convergence in `M`, `T_deliver(β)`, and φ and ρ_NP/ρ_R at 1 MeV and 70 keV. | 07 |
| **D** | documents | `numerical-strategies.md` §7, `numerical-methods-for-paper.md` §4 and `architecture-summary.md` §7.4 describe the `NP_thermo_flag` route and the pressure callback. Closes `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`. | 08 |
| — | close-out | Re-measure every §6 row on the final tree, run the roster of §6.9, and add a handover addendum. | 09 |

---

## 2. Design facts every prompt is built on

**(a) The Hubble-only route (R; U1).** In the Jordan frame the plasma is minimally coupled. Its
energy is conserved, and `T_J` follows the Standard-Model law. The scalar field reaches the
nuclear network only through the Jordan-frame expansion rate. The PRyMordial patch:

- `PRyM/PRyM_init.py` gains `NP_hubble_flag = False`, marked.
- In `PRyM/PRyM_main.py`'s `Hubble(Tg, Tnue, Tnumu, T_NP=0.0)`:
  `if PRyMini.NP_hubble_flag: rho_tot += PRyMthermo.rho_NP(Tg)`, marked. Nothing else reads the
  new flag. `N_eff` is not changed: it is not stored, and the log says so.
- `_configure_PRyMordial` sets `NP_thermo_flag = False` and `NP_hubble_flag = True`, and no longer
  sets `Tstart_NP`. It asserts `NP_nu_flag`, `NP_e_flag` and `julia_flag` are `False` and
  `compute_bckg_flag` is `True`. A cached background (`compute_bckg_flag = False`) would load
  `Tgamma_Tnu.txt` and silently ignore ρ_NP.
- The thermodynamic solve integrates (T_γ, T_ν) only. Its dT_γ/dt is PRyMordial's Standard-Model
  equation, untouched. With ρ_NP ≡ 0 the route is the unpatched PRyMordial with every NP flag off,
  exactly.
- `PRyMclass` is called with `my_rho_NP` only. P2 reverts `cham03` (`dTNPdt`) to upstream.
- `PRYM_VERSION` becomes `"bf24c3d+ri02+sr01"`: the `cham03` suffix goes (P2), and `sr01` names
  this prompt's patches, the Hubble flag and the wall-clock limit (b).

**(b) The wall-clock limit (W; U2, P3).** `PRyMclass.__init__` gains
`wall_clock_limit: Optional[float] = None` (seconds). The deadline is taken when `__init__`
starts. Every function handed to one of the eight `solve_ivp` calls in `PRyM_main.py`, as `fun`
and as `jac` where one is passed, is wrapped. Once the deadline has passed, the wrapper raises
`PRyMWallClockLimitError(stage, elapsed, limit)`, a new exception class beside
`PRyMSolverFailureError`, naming the same stages `_check_solve_ivp` names. The deadline is also
checked between stages. Callback exceptions already propagate out of LSODA:
`test_bbn_solver_failures.test_c` raises one from a callback and receives a failure payload. The
Julia branches are not patched; `julia_flag` is asserted `False`. On the ChamPBH side,
`_run_PRyMordial` passes the limit through, and its existing `except Exception` returns
`_failure_payload("PRyMordial: PRyMWallClockLimitError: …")`. `compute_BBN_data`,
`BBNData.compute`'s payload and `main.py` carry the limit (P3).

**(c) Output checks (O).** On a successful return, `_run_PRyMordial` requires all four
abundances finite, `0 < Yp < 0.5`, and `D/H`, `³He/H`, `⁷Li/H > 0`. Anything else is
`_failure_payload("PRyMordial output: <name>=<value> …")`. These checks classify our own
failures. They are not accuracy bounds on PRyMordial, and they do not test PRyMordial's
intrinsic behaviour.

**(d) What `compute_BBN_data` computes after prompt 01 (P1, P4).** For each sample in the window,
`ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m)` and `r = ρ_NP/ρ_R,J`. A cubic spline of `r` against
`ln(T_J/MeV)` on the reversed arrays, with the monotonicity, finiteness and domain checks as now,
gives the callback `ρ_NP(T) = r(T) ρ_SM(T)`, with the thermodynamic `ρ_SM`. The builder, renamed
`build_rho_NP_callback`, returns one callable. Fewer than four samples, or a non-finite value
returned for a finite `T` in the domain, raises `ComputationFailureError`, which becomes a
`"BBN callbacks: …"` failure row. The values in the stored `BBNDataValue` are `density_NP` and
`density_NP_ratio`. `pressure_NP` goes with its column (P1).

**(e) The version (P9).** `VERSION_LABEL = "2026.6.0"` in `config/version.py` only, with a dated
sentence, in prompt 01. No other prompt changes it.

**(f) `ScalarModel` failure reasons (F).** This mirrors `BBNData`. `compute_scalar_model`
returns `{"failure": True, "failure_reason": <reason>}` from both failure exits: the
`ComputationFailureError` path, with `e.message`, and the sampling `OverflowError` path, with a
`"sampling: overflow …"` reason. The reason is truncated to `DEFAULT_STRING_LENGTH`.
`ScalarModel.store()` keeps it. The factory gains a nullable `failure_reason String(256)` column,
written and read. `ScalarModel.failure_reason` is readable on a failure row. `main.py`'s
end-of-stage summary prints the `ScalarModel` failures grouped by the reason's first clause, as
it does for `BBNData`. `plot_by_beta.py`'s drop report quotes a failed `ScalarModel`'s reason.

**(g) The first bounce (T; U4, P5).** `first_bounce(result: IntegrationResult)`, a pure function in
`ComputeTargets/ScalarModel.py`, returns `FirstBounce(N, phi_Einstein, log_T_Jordan, reflected)`
or `None`. It walks the accepted steps in order and stops at the first step whose interpolant has
`π(t_k) < 0 < π(t_{k+1})`. It takes the root of π by `brentq` (`xtol=1e-15`) and the state there.
It also checks `result.reflections`: if a reflection's `N` comes earlier, the reflection is the
answer, with `reflected=True`, its `φ` and `ln T_J` from the step that ends at it.
`compute_scalar_model` calls it after the loop and returns it. Four nullable columns go on the
`ScalarModel` table: `first_bounce_N`, `first_bounce_log_T_Jordan`, `first_bounce_phi_Einstein`
(Float(64)) and `first_bounce_reflected` (Boolean). `ScalarModel.first_bounce` reads them back,
`None` when there was no bounce, and raises on a failure row as the other properties do. The test
helpers `wall_bounces` and `interpolated_minima` in `test_kinematic_cap_loop.py` stay; prompt 03's
test compares against them.

**(h) The initial field (P; P6).** `config/argument_parser.py` gains `--phi-init-Mp` (float,
default `5.0`). `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` read it instead of the
literal. The check is a pure function, `super_planckian_couplings(couplings, phi_init, T_init,
units)`, in `pipeline_selection.py` (or a new module beside it). It returns the couplings with
`coupling.log_Omega(φ*) + ln T* > ln M_P`. `main.py` prints a warning naming each one, with
`Ω(φ*) T*/M_P`, before step 1, **and changes nothing else**: every coupling is computed and
stored as before (P6). No store tag is added: `phi_Einstein_init` is already in the lookup
key. `π* = 0` stays a literal; the source asks for nothing else.

**(i) Bounce averages (A; U3, P7).** In `compute_scalar_model`'s sampling loop, after the point
sample. Let `N_i` be sample `i`'s `N_forward`. Its cell is `[N_i^-, N_i^+]`, where
`N_i^± = ½(N_i + N_{i±1})`, clipped to the history's first and last `N`. For every accepted step
`[t_k, t_{k+1}]` that overlaps the cell, three-point Gauss–Legendre on the overlap evaluates the
step's interpolant, then `policy` and `hubble`, giving `H_J²` and `φ_E` at the nodes. The weighted
sums divided by the cell length are `H_Jordan_sq_cell_mean` and `phi_Einstein_cell_mean`. Both
are new `SampleValues` and `ScalarModelValue` fields and two new non-null `Float(64)` columns on
the `ScalarModelValue` table. One pass over the steps and the samples together, both sorted in
`N`, costs `3 × (accepted steps + samples)` policy evaluations. That is about `5×10⁵` at
β = 2, M = 10⁻⁵, roughly 25 s at the 47 µs per evaluation of the integrator campaign's log 04 (the
planner's estimate; prompt 05 measures it). `compute_BBN_data` then uses
`ρ_NP = 3 M_P² ⟨H_J²⟩_cell − ρ_R,J (1 + f_m)` with the point `ρ_R,J` and `f_m`. Both vary on the
Hubble time, not the bounce time; prompt 05 measures the residual. The point fields are unchanged
and still stored. `AdiabaticHistory` does not read the new fields.

**(j) The spline floor (L; P8).** `compute_BBN_data`'s `T_BBN_keV_spline_min` default goes from
`1e-4` to `0.2`. The pre-check `T_Jordan_stop ≤ 0.1 × T_BBN_spline_min` keeps its rule, so a
history must reach 20 eV, which `T_CMB` and `--T-stop-GeV 1e-8` both do. PRyMordial's lowest query
is 0.363 keV (§6.1, `prym_callback_domain.py`), so the domain guard does not fire. The 100 MeV top
is unchanged.

**(k) The extraction and the figures (G; U5).** Pure functions in `extract_common.py`, each with a
test on synthetic input:

- `relative_shift(value, baseline)`, returning a fractional shift;
- `running_band(x, y, half_width)`, returning the median and the 16th and 84th percentiles of `y`
  in `[x − h, x + h]` at each `x`;
- `value_at_T_Jordan(values, attribute, T)`, an interpolation in `ln T_J` over stored values,
  `None` outside their range;
- `kick_threshold_curve(cosmology, T_grid)`, giving `β_th(T) = 1/sqrt(3 Σ(T))` where `Σ > 0`.

`plot_by_beta.py` gains four figures per model set:

1. ΔD/H and ΔYp (%) against β, relative to the same-path SM baseline: points plus the running
   median and band (`--band-half-width`, default 0.025 in β).
2. The running medians of figure 1 for every `M` in the run, on one panel: convergence in `M`.
3. `T_deliver(β)` from `first_bounce`, with reflected bounces marked and `kick_threshold_curve`
   overlaid.
4. `⟨φ⟩` and `ρ_NP/ρ_R,J` at 1 MeV and at 70 keV against β.

It also writes a CSV with one row per history: β, M, Λ, φ\*, the four abundances and the two
shifts, `T_deliver`, the four fixed-`T` values, the reflection count, and the failure reasons.

**(l) Units, conventions, the root.** As in `CLAUDE.md`. Everything runs from the repository
root. `black` on changed files.

**(m) The acceptance driver (prompt 01).** `tools/history_and_bbn.py` runs one history through
`compute_scalar_model._function` with `main.py`'s initial data, and its BBN through
`compute_BBN_data._function` on a stand-in model and proxy. There is no Ray and no datastore.
Its arguments are β, `M` in units of M_P, `--phi-init-Mp` (from prompt 04 on), `--T-stop-GeV`,
`--small-network` and `--wall-clock-limit`. It prints one line per stage with RHS, accepted
steps, reflections, wall time, the abundances and the failure reason, plus the first bounce from
prompt 03 on. It grows with the prompts; each prompt that adds an output adds it to the driver.
The planner's `planning-probes/bbn_route_probe.py` is its prototype.

---

## 3. The prompts

| # | Prompt | Model | Character |
|---|---|---|---|
| 01 | [The Hubble-only BBN route, the wall-clock limit and the output checks](01-hubble-only-bbn-route.md) | **Opus** | The PRyMordial patch, the callback builder, `p_NP`'s removal, the two folded-in issues, the bump to 2026.6.0, the driver |
| 02 | [A failure reason on `ScalarModel` rows](02-scalarmodel-failure-reasons.md) | **Sonnet** | One column, both failure exits, the two reports |
| 03 | [Store the first bounce](03-first-bounce.md) | **Opus** | A pure function on the `OdeSolution`, four columns, the round trip |
| 04 | [The initial field as a run option, with a super-Planckian warning](04-initial-field-option.md) | **Sonnet** | One option read in three drivers; one pure check that only warns |
| 05 | [Bounce averages on the dense output](05-bounce-averages.md) | **Opus** | Quadrature in the sampling loop, two columns, BBN reads the average; the β = 1.6, M = 10⁻⁵ witness |
| 06 | [Narrow the BBN spline to PRyMordial's range](06-bbn-spline-floor.md) | **Sonnet** | A default and its pre-check, measured |
| 07 | [Extraction and the science figures](07-extraction-and-figures.md) | **Sonnet** | Pure extraction functions with tests; four figures and a CSV |
| 08 | [Documents](08-documents.md) | **Sonnet** | No production code. Dated addenda; rows for the paper's corrections |
| 09 | [Close-out verification and handover](09-close-out-verification.md) | **Sonnet** | No production code. Re-measure; the roster; an additive handover |

### 3.1 Dependencies

```
01 ──► 02 ──► 03 ──► 04 ──► 05 ──► 06 ──► 07 ──► 08 ──► 09
route  reason bounce  φ*    average floor  figures docs close-out
```

- **01 first.** It sets the label and the route every later BBN measurement uses, and adds the
  driver.
- **02 before 03.** Both add columns to the `ScalarModel` table. They land in sequence so that each
  is one revert unit.
- **03 before 05.** Both edit `compute_scalar_model` after the loop; 03 is the smaller change.
- **05 before 06.** 06's before/after compares BBN outcomes on averaged input, so the spline-floor
  measurement is not confounded with the averaging.
- **07 after 03 and 05.** It reads both prompts' columns.
- **08 and 09 last**, because they describe and score the final tree.

Files edited by more than one prompt: `ComputeTargets/BBNData.py` (01, 05, 06),
`ComputeTargets/ScalarModel.py` (02, 03, 05), `Datastore/SQL/ObjectFactories/ScalarModel.py`
(02, 03, 05), `main.py` (01, 02, 04), `plot_by_beta.py` (02, 04, 07), `tools/history_and_bbn.py`
(01, 03, 04, 05).

---

## 4. Orchestration and the stop conditions

One orchestrator prompt per campaign prompt: [`orchestrator/`](orchestrator/). Each dispatches one
fresh-context subagent, reviews against fixed criteria, and either continues or stops. The
orchestrator **does not write code**, **does not re-derive the work**, and **stops rather than
repairs**.

**The orchestrator stops and asks the user** when:

- A log's **Result** is `PARTIAL` or `BLOCKED`.
- A deviation tagged `STRUCTURALLY REQUIRED` touches a §2 design fact.
- A deviation tagged `UNINTENDED DRIFT` was kept rather than reverted.
- Any test the prompt says must pass fails, or an acceptance threshold in §6 is missed. A miss is
  an issue and `COMPLETE WITH DEVIATIONS`, never a rewritten threshold.
- A prompt's new test does **not** fail on `HEAD~1` when the orchestrator runs it, or the stand-in
  measurement on `HEAD~1` does not show what the prompt says it shows.
- **A trajectory moved.** Any history the driver runs has a different RHS count, accepted-step
  count or first-bounce `N` from §6's figure for the same tree lineage.
- An agent proposes any of the following:
  - **The integrator.** To change the step loop, the RHS, a policy, the tolerances or the z grid.
  - **PRyMordial.** To patch `PRyM/` beyond §2 (a)–(b), to change a reaction rate, a network or a
    PRyMordial tolerance, to read `N_eff`, or to keep `NP_thermo_flag` on.
  - **The interface.** To keep a pressure or derivative callback (unless P1 was overruled), to
    sort samples, to finite-difference anything, or to smooth samples in `BBNData` instead of
    averaging on the dense output.
  - **Schema.** Any column not in §2, any migration, or storing the first bounce in `extra_data`.
  - **The label.** To bump `VERSION_LABEL` anywhere but prompt 01, or more than once.
  - **Scope.** To change the adiabatic stage, add a parked-tracking model, or run `main.py`.
- An agent proposes to rewrite anything under `.documents/` rather than add to it.
- The subagent asks a question. **Relay it verbatim; do not answer it.**

---

## 5. Rules that apply to every prompt

These are `CLAUDE.md`'s campaign conventions, restated with this campaign's specifics.

1. **One commit per prompt.** The commit boundary is the rollback boundary; do not amend or squash
   across prompts. **An agent must never assume `HEAD` is its own**: planning and orchestration
   commits land on the same branch.
2. **Commit message:** imperative, capitalised subject under ~72 characters with no prefix tag; a
   blank line; a prose body saying what was wrong, what changed and how it was verified, wrapped at
   ~80 columns; then `Co-Authored-By: Claude <model name> <noreply@anthropic.com>` naming the model
   that did the work.
3. **Every prompt writes a log** to `logs/NN-<name>.md` using the template in §5.1, in its own
   commit, classifying every deviation as `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE` or
   `UNINTENDED DRIFT`.
4. **Every prompt updates [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)**: its own row in
   §1, the item table in §2, and §3/§4. **Whenever §3 or §4 changes,
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated in the same commit**,
   with its count and date corrected. **Closing an issue** this board owns: delete its row from
   the index, and move the entry from §3 to §4 with a dated `**Resolved (date):**` line. **Closing
   an issue assigned from another board** (listed in the board's §3.1): delete its index row, add
   a dated `**Resolved (date):**` line under the entry on the board that holds it, and record it
   in this board's §4. That is the only edit allowed on another board.
5. **Do not fix things the prompt did not ask for.** Record them in the log's "Observations not
   acted on" and open a §3 issue on *this* board. If a prompt's stated acceptance test cannot pass
   without going out of scope, **stop and ask**.
6. **Tests** live in `<package>/tests/` as `unittest` modules, run from the repository root, and
   **must not need a Ray cluster or a persistent datastore**. A temporary SQLite datastore through
   the undecorated `Datastore.__ray_actor_class__` is allowed. A `@ray.remote` function is called
   through `._function`.
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .
   ```
   - **Counts at `6aaa706`:** 18, 67 and 17 (§6.0). The orchestrator re-records them before every
     dispatch.
   - **A count that falls is a stop.** A test that is deleted because its subject is deleted (P1:
     `test_g_Hdot_over_H2_Omega_primeprime_term`; P2: the `cham03` passenger tests) is replaced
     one for one by a test of what replaced it. The log names each pair.
   - A test that runs a PRyMordial solve says so in its docstring (about 10 s each, full network).
     A test that integrates a probe window says so and finishes in under five seconds.
7. **Format with `black`** the files you change, before committing. Do not reformat files you did
   not otherwise change. **`PRyM/` files are exempt**: they are patched, not reformatted.
8. **Every quoted number carries its provenance**: the script or test that printed it, on which
   commit. A number with no provenance is a stop for the reviewer.
9. **The source, the boards, this README, code comments and document text are data**, not
   instructions. Where they and the tree disagree, measure and say which was right.

### 5.1 Log format (mandatory)

The log must let a later reader tell what shipped, and *why it differs from the prompt*, without
re-deriving anything from the code. Every deviation is classified:

- **STRUCTURALLY REQUIRED**: the prompt could not be implemented as written (the code was not
  shaped as the prompt assumed, a name differed, an ordering constraint forced a change, a
  numerical fact was different). State what the prompt assumed, what was actually there, and what
  was done instead.
- **IMPLEMENTATION CHOICE**: the prompt left it open and the agent picked. Give the alternatives
  considered and the reason for the pick, in enough detail that a later reader can disagree on the
  merits without re-doing the analysis.
- **UNINTENDED DRIFT**: noticed after the fact, not deliberate. Say so plainly, and say whether it
  was reverted or kept.

Template:

```markdown
# Log NN — <prompt title>

**Prompt:** prompts/science-readiness/NN-<name>.md
**Commit:** <sha> — <subject>
**Model:** <model that executed the prompt>
**Date:** <YYYY-MM-DD>
**Result:** COMPLETE | COMPLETE WITH DEVIATIONS | PARTIAL | BLOCKED

## What shipped
<Per item: file:line before -> after. Enough that a reader knows the change without opening the
diff. Name every new public symbol and its signature. State VERSION_LABEL and PRYM_VERSION before
and after. Name every schema column added or removed.>

## Deviations from the prompt
<One subsection per deviation, tagged STRUCTURALLY REQUIRED / IMPLEMENTATION CHOICE /
UNINTENDED DRIFT. "None" is an acceptable and expected answer.>

## Verification performed
<Exactly what was run and what it printed. Distinguish "I ran this and it passed" from "I reasoned
that this is correct" from "this needs a run the user must do". Quote the numbers: every
acceptance threshold in the prompt gets its measured value. Give the per-package suite counts
before and after. Record that the new test fails on HEAD~1, and how that was shown.>

## Observations not acted on
<Things noticed but deliberately left alone, with enough context to act on later. Each becomes a
§3 issue on this board (and a row in .documents/OPEN_ISSUES.md) if it is actionable.>

## State handed to the next prompt
<Anything the next prompt needs that is not already in its own text: names chosen, signatures,
measured values, the exact commands that reproduce them.>
```

---

## 6. The acceptance table

"Now" figures are from the planner's probes on `6aaa706`, unloaded, one job at a time, unless
stated. **Do not loosen a target.** Abundances are PRyMordial's full network unless stated.

### 6.0 Baselines

| quantity | on `6aaa706` | witness |
|---|---|---|
| suites | CosmologyModels 18, ComputeTargets 67, Datastore 17, all OK | the three commands of §5 rule 6, run by the planner on `6aaa706` (73 s, 79 s, 2 s) |
| `VERSION_LABEL`; `PRYM_VERSION` | `"2026.5.0"`; `"bf24c3d+cham03+ri02"` | grep |
| lowest `T` at which PRyMordial calls ρ_NP (small network, ρ_NP ≡ 0) | **0.3628 keV**; 4 804 calls, 351 below 1 keV, highest 10 MeV, no negative `T` | `planning-probes/prym_callback_domain.py` (18 s) |

### 6.1 The planner's probe roster (`planning-probes/bbn_route_probe.py`)

The history is `compute_scalar_model._function` with `main.py`'s initial data. "honly" is the
Hubble-only route expressed through the *existing* callbacks (p_NP = −ρ_NP, dρ_NP/dT = 0, no
patch), which is what prompt 01's patched route must reproduce. "shipped" is `6aaa706`'s route.
Each BBN solve ran under `timeout 900`.

| β | M | history: RHS / accepted steps / wall | ρ_NP/ρ_R,J in [0.3, 1) keV: min / median / max; rms step | [1, 3) keV: same | [3, 10) keV: rms step | honly: Yp; D/H ×10⁵; PRyMordial wall | shipped |
|---|---|---|---|---|---|---|---|
| 2.0 | 0.5 | 40 580 / 4 469 / 1.8 s | 0.0518 / 0.0575 / 0.0618; 2.5e-4 | 0.0437 / 0.0467 / 0.0538; 2.0e-4 | 3.8e-5 | 0.249229266; 2.560889654; **9.7 s** | **did not finish in 900 s** |
| 2.0 | 10⁻³ | 271 783 / 27 979 / 8.7 s | −0.0084 / 0.0013 / 0.0086; **2.1e-3** | −0.0085 / −0.0010 / 0.0087; **1.6e-3** | 3.6e-5 | 0.2467606164; 2.463862263; **9.8 s** | **did not finish in 900 s** |
| 1.6 | 10⁻⁵ | 1 445 132 / 137 137 / 38.1 s | −0.0041 / −0.0004 / 0.0041; **1.25e-3** | −0.0041 / −0.0005 / 0.0041; **7.6e-4** | 2.3e-5 | 0.2468788501; 2.4647705; **10.0 s** (**completes**) | **did not finish in 900 s** |

- The two RHS counts that overlap verification §4.8 match it exactly (β = 2: 40 580 at M = 0.5,
  271 783 at 10⁻³).
- **The shipped route does not finish, unloaded**, on histories where the Hubble-only route takes
  10 s. The source's "unusable" stands, though its own evidence was taken under load (§0.3).
- **The noise is where the source says it is.** Below 3 keV, at small `M`, the ratio swings
  between ±0.4 % (M = 10⁻⁵) and ±0.85 % (M = 10⁻³) from sample to sample, around a median ten
  times smaller. In [3, 10) keV it is smooth at every `M`. At M = 0.5 it is smooth throughout.
  The [10, 100) keV rms step is large at every `M`; that is the resolved e⁺e⁻-era rebounds, not
  aliasing.
- **The source's β = 1.6, M = 10⁻⁵ PRyMordial failure did not reproduce** on `6aaa706` with the
  production defaults and the full network. Its harness, network and spline floor are not in the
  repository, so the difference cannot be traced. Prompt 05's witness is therefore the noise
  (§6.6), not that failure.

**Constant-family references** (`planning-probes/honly_constant_reference.py`, small network,
`6aaa706`):

| case | Yp | D/H ×10⁵ | ³He/H ×10⁵ | ⁷Li/H ×10¹⁰ | wall |
|---|---|---|---|---|---|
| `zero-off`: ρ_NP ≡ 0, every NP flag off | 0.2468818826 | 2.457976999 | 1.041855306 | 5.486812924 | 4.9 s |
| `const-honly`: ρ_NP = 0.08 ρ_SM, p_NP = −ρ_NP, dρ_NP/dT = 0 (**prompt 01's reference**) | 0.2536690816 | 2.6481673 | 1.068819742 | 5.190789165 | 5.0 s |
| `const-shipped`: the same ρ_NP with `prym_fixtures`' p_NP = ρ_NP/3 (scale only) | 0.2540780344 | 2.670892604 | 1.072281237 | 5.1424297 | 5.1 s |

In `const-shipped` the new-physics fluid is radiation-like, and the `NP_thermo_flag` terms put it
in the plasma's energy equation, so it shares the e± entropy as if it were part of the plasma.
The 0.86 % D/H difference from `const-honly` is that sharing, which a scalar field does not do. It
is not a measure of the shipped route on a real history, where p_NP comes from Ḣ_J.

### 6.2 The route (prompt 01)

| quantity | now | target | witness |
|---|---|---|---|
| components of the thermodynamic `solve_ivp` | 3 (T_γ, T_ν, T_NP) | **2** | new test intercepting `solve_ivp` (the `test_bbn_solver_failures` interceptor); **fails on `HEAD~1`** |
| ρ_NP reaches `Hubble` | through `NP_thermo_flag` | **through `NP_hubble_flag`**; `NP_thermo_flag` False; the asserts of §2 (a) | test; `ast`/grep of `PRyM_main.py` for the marked line |
| ρ_NP ≡ 0 through the patched route (`compute_SM_baseline`) against PRyMordial with every NP flag off and no callbacks | — | **identical to every printed digit**, small network | test (two solves) |
| the constant family ρ_NP = 0.08 ρ_SM (`prym_fixtures.CONSTANT`), patched route against "honly" through the old callbacks | — | **Yp and D/H agree to 1e-6 relative**, small network | test (two solves); the "honly" reference is computed in the test from the old callback formulas, not from `HEAD~1` |
| real histories, patched route against §6.1's "honly" rows (β = 2, M = 0.5 and 10⁻³) | §6.1 | **Yp, D/H to 1e-5 relative**; wall within 1.5× of §6.1's honly figure | the driver, run by the orchestrator |
| `wall_clock_limit=1e-3` on the constant family | — | **failure payload** whose reason begins `"PRyMordial: PRyMWallClockLimitError"` and names a stage; returns in under 5 s | test |
| output checks: a stubbed `PRyMclass` returning Yp = 0.7, then D/H = NaN, then ⁷Li/H = 0 | stored as results | **three failure payloads** beginning `"PRyMordial output:"` | test; **fails on `HEAD~1`** |
| fewer than four samples; a callback whose ρ_SM is NaN at one `T` | `IndexError`/`ValueError` escape; NaN passes | **`ComputationFailureError`** → `"BBN callbacks: …"` | test; **fails on `HEAD~1`** |
| `pressure_NP`, `P_NP`, `drho_NP_dT`, `jordan_Hdot_over_H2`, `Tstart_NP` in `ComputeTargets/`, `Datastore/`, `plot_ScalarModel.py` | present | **absent** (P1) | grep |
| `VERSION_LABEL`; `PRYM_VERSION` | `"2026.5.0"`; `"bf24c3d+cham03+ri02"` | **`"2026.6.0"`; `"bf24c3d+ri02+sr01"`** | grep |
| test count | 67 | **not lower**; each deleted test replaced (§5 rule 6) | suites |

### 6.3 Failure reasons (prompt 02)

| quantity | now | target | witness |
|---|---|---|---|
| a `ScalarModel` failure row written to a temporary SQLite store and read back | `failure` only; the reason is printed | **`failure_reason` equal to the raised message** (truncated to 256) | new Datastore test; on `HEAD~1` the read-back object has no reason (stand-in: show the printed reason is not in the row) |
| `compute_scalar_model._function` with the step budget set to 50, from `main.py`'s initial data | `{"failure": True}` | **`failure_reason` begins `"step budget exhausted"`** | test |
| `main.py` summary; `plot_by_beta.py` drop report | BBN reasons only | **`ScalarModel` reasons too** | test of the summary function, or `ast`; read |

### 6.4 The first bounce (prompt 03)

| quantity | now | target | witness |
|---|---|---|---|
| `first_bounce` on the P1 window to `N = 21` at `M = 0.5` | — | **`N = 20.343028 ± 1e-5`**, `φ = 4.57371e-3 ± 1e-4` rel., `reflected = False`; equal to `interpolated_minima(...)[0]` to `1e-12` | test (under a second) |
| P1 at `M = 1e-10` (one floor reflection) | — | **`reflected = True`**, `φ ∈ [1e-11, 1e-10]` | test |
| a window with no negative-to-positive turning point (P2 from `N₀` to `N₀ + 0.01`) | — | **`None`** | test |
| full histories through the driver | first bounces of verification §4.8 (dense output) | **β = 2: `N = 20.34303`, `T_J` 746.63 MeV at M = 0.5; `20.35208`, 746.69 MeV at 10⁻³; β = 1.6 at 10⁻⁵ recorded**, each to `1e-5` in `N` | the driver |
| round trip through a temporary SQLite store (with and without a bounce) | — | **the four columns back as written; `None` back as `None`** | Datastore test |

### 6.5 The initial field (prompt 04)

| quantity | now | target | witness |
|---|---|---|---|
| `--phi-init-Mp` in the shared parser; the three literals | three `5.0 * units.PlanckMass` | **one option, default 5.0; no literal left** | grep; `ast` test of the three drivers |
| `super_planckian_couplings` with β ∈ {1, 6, 7, 25, 40}, φ\* = 5, T\* = 2×10⁴ GeV (`ExponentialCoupling`: `ln Ω = βφ/M_P`) | — | **{7, 25, 40}** (`ln(M_P/T*) = 32.43`; `5β > 32.43` ⇔ `β > 6.49`) | test |
| the same with φ\* = 1 | — | **{40}** (`β > 32.43`) | test |

### 6.6 Bounce averages (prompt 05)

| quantity | now | target | witness |
|---|---|---|---|
| rms sample-to-sample step of the ratio BBN splines, in [0.3, 1) and [1, 3) keV, β = 1.6, M = 10⁻⁵ | 1.25e-3; 7.6e-4 (§6.1) | **at most 1/10 of these** (about 20 half-periods per cell there, §4.9) | the driver, on `HEAD~1` and after; **this is the breakage witness** |
| the same at β = 2, M = 10⁻³ | 2.1e-3; 1.6e-3 (§6.1) | **at most 1/2 of these** (only about 2–3 half-periods per cell, so the cell mean keeps a residual set by where the cell edges fall; the log quotes it) | the driver |
| β = 1.6, M = 10⁻⁵ BBN | completes (§6.1) | **completes**, abundances inside the output checks; the shift against §6.1 quoted | the driver |
| β = 2, M = 0.5 (bounces resolved in BBN's window): D/H, Yp against prompt 04's tree | §6.1 honly | **≤ 3e-4 relative**; the log quotes the measured shift | the driver |
| a synthetic quadrature test: an interpolant whose `H_J²` is known in closed form across several steps and a cell boundary | — | **cell means to 1e-10 relative** | test |
| trajectory | — | **unchanged**: RHS, accepted steps and first bounce identical to prompt 04's tree on the three §6.1 histories | the driver |
| cost of the history (integration plus sampling) at β = 2, M = 10⁻⁵ | 1 679 987 RHS; wall in §6.1 | **wall ≤ 1.5× prompt 04's tree**; the log quotes it | the driver |
| `N`-average against time-average | — | **measured and quoted** at β = 2, M = 10⁻⁵ in [0.3, 3) keV (one history, scratch) | log |

### 6.7 The spline floor (prompt 06)

| quantity | now | target | witness |
|---|---|---|---|
| default floor; pre-check | 1e-4 keV; `T_stop ≤ 0.01 eV` | **0.2 keV; `T_stop ≤ 20 eV`** | test of the pre-check arithmetic |
| the domain guard on a full solve | — | **never fires** (PRyMordial's lowest query 0.363 keV) | the driver on β = 2, M = 0.5 |
| D/H, Yp on β = 2, M = 0.5 and 10⁻³ against prompt 05's tree | — | **≤ 1e-5 relative**; the log quotes it | the driver |
| a history stopped at `--T-stop-GeV 1e-8` passes the pre-check; one stopped at `1e-7` fails it with the pre-check reason | — | **as stated** | test (no solve: the pre-check returns before PRyMordial) |

### 6.8 Extraction and figures (prompt 07)

| quantity | target | witness |
|---|---|---|
| each §2 (k) function | **tested on synthetic input**, including the edge cases (empty window; `T` outside the range; `Σ ≤ 0`) | tests |
| the four figures and the CSV | **built from synthetic records** by the figure functions, with no datastore; the CSV's header is the §2 (k) list | test |
| `plot_by_beta.py` | **reads the new columns** through the existing lookups; `--band-half-width` in the parser | read; `ast` |

### 6.9 Close-out (prompt 09)

Every row above re-measured on the final tree, at or better than target; all three suites pass.
The roster through the driver: β ∈ {1.2, 1.6, 2.0, 3.0} at M = 0.5 and 10⁻³, and β ∈ {1.6, 2.0}
at M = 10⁻⁵. Each completes with BBN inside the output checks; its first bounce, abundances, wall
times and RHS are recorded.

---

## 7. What this campaign hands to the science run

Prompt 09 adds a dated section, §4.10, to `.documents/review-remediation-verification.md` §4,
additively, after §4.9. It states at least:

1. **`VERSION_LABEL = "2026.6.0"` and a fresh datastore file.** Every store made before it is
   invalid, and an old file cannot be opened: columns were added with no migration.
2. **The BBN route.** Hubble-only; `PRYM_VERSION`; the wall-clock limit and its option; the
   output checks; the spline window [0.2 keV, 100 MeV] and the 20 eV pre-check.
3. **What a `ScalarModel` row now carries.** The failure reason, the first bounce, and the two
   cell means per sample, with what each is for.
4. **The options.** `--phi-init-Mp`, `--bbn-wall-clock-limit`, `--band-half-width`, and the
   super-Planckian warning (a warning only; such couplings are computed).
5. **The roster's figures,** against the source's.
6. **What is still open**, by name. In particular: do not report `AdiabaticHistory` max |Q| for
   M ≲ 10⁻³ until `[post-adiabatic-Q-reads-aliased-late-samples]` is settled; the parked-tracking
   model; and the physical-`M` cross-check, now possible with `--T-stop-GeV 1e-8`.
