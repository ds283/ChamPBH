# Prompt 01 — Replace the fragment loop with the kinematic-cap step loop

**Campaign:** [`README.md`](README.md) · **Board items:** **A**, **J**, and the loop half of **X** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and A, J, X.
**Closes:** `[00-region-scheme-costs-100x-and-storms-fragments]`,
`[00-hard-reflection-at-phi-zero-stalls-or-runs-free]` and
`[00-scipy-num-jac-factor-grows-without-bound]` on this board.
**Recommended model:** **Opus**. The change is one function's body, but it is the function every
stored history comes from, and the judgement is in getting the cap, the floor and the exception
handling exactly as specified while changing nothing about what the RHS computes or how the
history is sampled.

**Read first:**

1. [`README.md`](README.md) §0, §2 (a)–(g), §5, §6.1.
2. `.documents/integrator-audit-2026-09-30/README.md` §2, §3, §8 (F2), §9. Then its
   `harness.py`: `run_velocity_cap` (the `cap_kind="kin"` branch, the `reflect_at_floor` branch,
   the `jac_factor_max` clamp, the `except SM.ComputationFailureError` around `solver.step()`) is
   a working probe of the loop you are writing. It is a probe, not production code: it mirrors
   `φ` nowhere in the `kin` path, but it does lack the termination root-find, the
   `OdeSolution`, and the `φ ≤ 0` failure. Do not copy it; reproduce its measured behaviour.
3. `ComputeTargets/ScalarModel.py` on `HEAD`:
   - `:52–60` the module constants (`HARD_REFLECTIONS_KEY`, `EXPECTED_SOL_LENGTH`,
     `DEFAULT_MAX_STEP_SIZE`);
   - `:84–90` `SolutionFragment`;
   - `:425–840` `compute_scalar_model`: the initial state (`:456–505`), the six events
     (`:512–568`), the fallback wrapper (`:570–600`, `:823–839`), the fragment loop
     (`:600–822`);
   - `:842–957` the sampling and the returned dict;
   - `:960–1005` `build_extra_data`;
   - `:1293–1340` `ScalarModel.store()`, which reads the dict.
4. `Quadrature/supervisors/ScalarField.py`: the level-1/2 and hard-reflection tracking
   (`:80–95`, `:225–290`, `:319–323`) and the status message (`:148–200`).
5. `CosmologyConcepts/Potentials/ExponentialPotential.py:69–91` (the region properties, which
   become unread) and `AbstractPotential.py`.
6. `extract_common.py:169–243`, `plot_by_beta.py:490–545`,
   `ComputeTargets/tests/test_hard_reflection_reporting.py`: the consumers of the stored keys.
7. `main.py:924–949`: the pre-registered stepper labels; `plot_by_beta.py:676` and
   `plot_ScalarModel.py:1575`: the `solver_labels` lists.
8. `config/version.py`.
9. `scipy/integrate/_ivp/radau.py` in `venv/`: `Radau.__init__`, `_step_impl` (where
   `self.max_step`, `self.h_abs`, `self.jac_factor` are read and written), `dense_output`; and
   `scipy.integrate.OdeSolution`.
10. `CLAUDE.md`, "Repository mechanics".

---

## 1. The changes

**A1 — the pure loop.** A module-level function in `ComputeTargets/ScalarModel.py`, with a
signature of the form

```python
def integrate_scalar_history(RHS, supervisor, initial_state: StateVector, N_start: float,
                             log_T_stop: float, params: StepControl,
                             N_failsafe: float = 1000.0) -> IntegrationResult
```

where `StepControl` is a namedtuple of the loop's parameters with the README §0.2 defaults
(`cap_fraction=0.1`, `cap_floor=1e-11`, `global_max_step=0.1`, `jacobian_factor_max=1e-4`,
`atol`, `rtol`; prompt 02 adds `step_budget`), and `IntegrationResult` carries the `OdeSolution`,
`N_final`, the final `StateVector`, `nfev`, `accepted_steps`, `steps_rejected_by_exception`, and
the list of reflections as `(N, φ, π_in)`. The names are yours (IMPLEMENTATION CHOICE); the
behaviour is README §2 (a)–(c), item by item:

- one `scipy.integrate.Radau` from `(N_start, initial_state)` to `N_failsafe`, with
  `max_step=global_max_step`, `rtol`, `atol`;
- before every step, from `solver.y` and `solver.f`: the floor test and the reflection of
  §2 (b), else the cap of §2 (a) with the floor as its minimum, set on `solver.max_step` and
  clipped into `solver.h_abs`;
- `solver.step()` inside `try`: `ComputationFailureError` → `solver.h_abs /= 2` and retry; if
  `solver.h_abs < 1e-13` the error propagates; any non-`None` message from `step()` →
  `ComputationFailureError` with the message;
- after an accepted step: clamp `solver.jac_factor` (if not `None`); if `φ ≤ 0` raise
  `ComputationFailureError` naming `N`, `φ`, `π`; append `solver.dense_output()`;
- **the two guards of README §2 (b′), checked before any reflection:** G1, the potential's
  `reflects_at_origin` is `True`, else `ComputationFailureError` naming the potential and the
  state; G2, with `d = policy(N, state)` at the reflecting state,
  `W = 3 d.V_over_3H2Mp2 (1 − exp(potential.log_V_floor − d.log_V))` satisfies `W ≤ ½π²`, else
  `ComputationFailureError` ("reflection requested inside the wall") quoting `W/(½π²)`, `N`, `φ`,
  `π`. Record the maximum `W/(½π²)` over the history's reflections in the result, so the log can
  quote it;
- a reflection restarts a new `Radau` from the reflected state at the same `N`, with the same
  parameters; the reflection is appended to the list and `supervisor.notify_reflection(N)` is
  called;
- termination at the first accepted step with `ln T_J < log_T_stop`: the root of
  `ln T_J − log_T_stop` on that step's interpolant by `scipy.optimize.brentq` between the step's
  endpoints; the solution's last node is the root, the final state the interpolant there;
- `solver.status == "finished"` (the failsafe reached) → `ComputationFailureError` (prompt 02
  changes nothing here; it only removes the old `RuntimeError`);
- the result's solution is `OdeSolution(ts, interpolants)`.

`compute_scalar_model` builds `initial_state` as now (`:456–505` unchanged), calls the loop, and
samples the result's solution on the z grid exactly as `:865–908` do today, with the fragment walk
replaced by one call per sample. **Leave the fallback wrapper in place** (`while not success`,
`solver_list`, the `except`): prompt 02 removes it. Delete the six event functions,
`SolutionFragment`, `DEFAULT_MAX_STEP_SIZE` and the reads of the potential's region properties.

**A1′ — the potential interface.** `AbstractPotential` gains two properties with defaults:
`reflects_at_origin -> bool` (`False`) and `log_V_floor -> Optional[float]` (`None`), each with a
docstring stating what the loop uses it for (README §2 (b′)). `ExponentialPotential` returns
`True` and `self._log_Lambda_4`. No other potential changes; no other property is added or
removed.

**A2 — the supervisor.** `ScalarFieldIntegrationSupervisor` loses `notify_level_1_entry/exit`,
`notify_level_2_entry/exit`, `notify_new_fragment`, `notify_hard_reflection`, the `_level_*` and
`_hard_reflection_data` state, the `in_level_*` properties and the `max_step_size` constructor
argument, and gains `notify_reflection(N)` with a `number_reflections` property. The status
message reports the reflection count and the current cap instead of the level state and
fragments. Nothing in `Quadrature/supervisors/base.py` changes (prompt 02 owns `RHS_timer`).

**A3 — what is stored.** README §2 (e). `build_extra_data` writes the new key set; the module
constant becomes `REFLECTIONS_KEY = "number_reflections"`. `extract_common.hard_reflection_count`
becomes `reflection_count`, reading `REFLECTIONS_KEY`; the two captions become one,
`"Reflections (elastic model): N"`; `plot_by_beta.py`'s column and its report use it and say
"reflections" rather than "hard-reflection fallback". `test_hard_reflection_reporting.py` is
rewritten as `test_reflection_reporting.py` to pin the **new** block (its `reference_extra_data`
becomes a verbatim copy of the new `build_extra_data`'s store calls), with at least five methods.

**A4 — the label.** `compute_scalar_model` returns `"solver_label": "Radau+kinematic-cap-stepping0"`.
`main.py:924–949` registers `IntegrationSolver(label="Radau+kinematic-cap", stepping=0)` beside
the five existing ones and adds it to `solvers`; `plot_by_beta.py:676` and
`plot_ScalarModel.py:1575` add the same label to their lists. The five old registrations stay.

**A5 — the bump.** `config/version.py`: `VERSION_LABEL = "2026.5.0"`, with one dated sentence in
the comment block above it, in the style of the existing ones: from 2026.5.0 the scalar history
is integrated by one Radau step loop with a kinematic step cap and an elastic reflection at the
representable-step floor, replacing the two-region scheme, so every stored history changes.

**A6 — documents, additively.** Nothing in this prompt; prompt 03 owns the documents. The audit
README is not edited.

---

## 2. Tests — `ComputeTargets/tests/test_kinematic_cap_loop.py`

No Ray, no datastore, no solve longer than three seconds; the docstring of each method that
integrates says how long it takes. Build the objects as the audit's `harness.build` does
(`QCD_Cosmology`, `Planck2018`, `Planck_units`, `ExponentialPotential(n=1, Λ=1e-3 eV)`,
`ExponentialCoupling`, `ODEPolicy`, `ODERHS`, the supervisor) and call `integrate_scalar_history`
from the audit's states, which you copy into the module as constants with their provenance
(audit README §2.2).

- **(a) P1 at `M = 0.5, 0.01, 0.001`** to `N = 21`: README §6.1 (a), every row for that `M`,
  asserted with the stated tolerances. The first bounce is the first accepted step at which `π`
  changes sign from negative to positive with `φ < 1.5 M`; `φ_min` is `φ` there (as the audit
  measured it: `harness.first_wall_bounce`). **The `M = 0.5` RHS bound must fail on `HEAD~1`**:
  there is no `integrate_scalar_history` on `HEAD~1`, so the breakage record is the audit's
  17 092 and 26 634 and 26 422 RHS from `p1_sweep.py a` and `p1_smallM.py`, quoted in the log
  with their commit, and the orchestrator re-runs `p1_smallM.py 0.01` on `HEAD~1`.
- **(b) P1 at `M = 1e-10` and `M = 4.1e-28`**: completes, exactly one reflection, its `φ` in
  `[1e-11, 1e-10]`, `φ(21) = 1.184428e-1 ± 1e-5` relative. **Must fail on `HEAD~1`**: the
  orchestrator runs `p_smallM_scan.py regions 1e-10` on `HEAD~1`, which fails with "Required step
  size is less than spacing between numbers"; quote it in the log.
- **(c) Convergence in `f`**: P1 at `M = 0.5` with `cap_fraction = 0.02` meets the same first-bounce
  tolerances as (a).
- **(d) P3** to `N = 37.5`: README §6.1 (b). About two seconds.
- **(e) P2 to `N = 40`**, with stdout captured: README §6.1 (c); the captured text contains no
  `"T_Jordan = 0"`. About two seconds.
- **(f) Failure paths**: README §6.1 (e): the cap disabled (`cap_fraction = inf`) from P1 at
  `M = 0.01` raises `ComputationFailureError`; an RHS wrapper that raises
  `ComputationFailureError` on its first three calls completes with
  `steps_rejected_by_exception ≥ 1` and the (a) `φ(21)`.
- **(g) Metadata and labels**: `build_extra_data` on a stand-in result dict yields exactly the
  §2 (e) keys, `number_reflections` absent when zero; `ast`-parse `main.py`, `plot_by_beta.py`,
  `plot_ScalarModel.py` (do not import `main.py`) and find the new label string in each.
- **(h) The guards**: README §6.1 (e), the three G1/G2 rows. For G1, a subclass of
  `ExponentialPotential` overriding `reflects_at_origin` to `False`, from P1 at `M = 1e-10`. For
  G2, the state `φ = 5e-5`, `π = −0.4976`, `ln ρ_rad,E = −166.04`, `ln f_m = −20.81`,
  `ln T_J = −42.63` at `M = 0.01` (inside the wall, `φ_wall ≈ 9.2e-5`; the audit's step-over
  state), reached by calling the loop with `h_floor` large enough that the floor fires at once
  (`h_floor = 1e-3`): it must raise with the ratio quoted (`≈ 23`). For the negative control,
  assert on every (a) and (b) run that the recorded maximum ratio is `≤ 1e-3`.

`ComputeTargets/tests` rises from 41 by the methods you add, less nothing: the rewritten
reporting test keeps at least five. `CosmologyModels/tests` 18 and `Datastore/tests` 17 pass
unchanged.

---

## 3. What this prompt does not do

- No change to `ODEPolicy`, `ODERHS`, `HubblePolicy`, `PotentialDerivativePolicy`: not a value,
  not an exception. Prompt 02 owns the trial-state policy.
- No change to the fallback wrapper, the `RuntimeError` sites outside the deleted loop, or
  `RHS_timer`. Prompt 02.
- No change to the z grid, the sampling fields, `ScalarModel.store()` beyond what the new keys
  require (it reads `extra_data` through `build_extra_data`, so nothing), or any datastore code.
- No schema change. No removal of the region properties from the potentials, and no potential
  change beyond A1′.
- No parked-tracking model, no analytic Jacobian, no `atol` vector, no step budget (prompt 02).
- No edit to `.documents/`.

## 4. Acceptance

1. README §6.1 (a), (b), (c), (e) including the three guard rows, (f), every row, with
   measured values in the log, and the maximum `W/(½π²)` over every reflection in (a), (b) and
   the nine histories.
2. README §6.1 (d): run the nine histories yourself through the loop (a scratch driver outside
   the repository that builds the initial state as `compute_scalar_model` does, from
   `main.py`'s initial data as the audit's `harness.initial_state` writes it) and quote RHS,
   steps, wall time, reflections (0), `T_Jordan = 0` substitutions (0), first bounce `N` and
   `T_J`. The orchestrator repeats the run with the audit's `p_full.py` and compares.
3. All three suites pass. `black --check` is clean on the changed Python files.
4. `grep -rn "HARD_REFLECTIONS_KEY\|SolutionFragment\|bounce_region_level\|notify_level_1\|notify_hard_reflection" --include='*.py' .` outside `venv/`, `thirdparty/`, `.documents/integrator-audit-2026-09-30/` and `prompts/` finds only the potentials' property definitions.
5. The board and the index: A, J done, X marked "loop half done"; the three issues closed (moved
   to §4 with a dated Resolved line; rows deleted from `.documents/OPEN_ISSUES.md`; count and
   date corrected).

## 5. Stop conditions — stop and ask the user

- Any §6.1 (a) or (b) tolerance cannot be met at `f = 0.1`.
- Any of the nine histories does not complete, or needs more than twice the audit's RHS.
- `solver.f`, `solver.max_step`, `solver.h_abs` or `solver.jac_factor` is not available on the
  installed SciPy's `Radau` as the audit found it (SciPy 1.17.0).
- The termination root cannot be bracketed on the last step's interpolant.
- Either existing suite fails, or its count falls.

## 6. The log and the board

`logs/01-kinematic-cap-step-loop.md` in the README §5.1 template, with every number's provenance.
State `VERSION_LABEL` before (`"2026.4.0"`) and after (`"2026.5.0"`). In "State handed to the
next prompt": the names and signatures of the loop function, the parameters namedtuple and the
result type; the exact commands that reproduce (a)–(g); the nine histories' figures.
