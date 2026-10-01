# Campaign — integrator remediation: one step loop with a kinematic cap

**Source:** the audit of the scalar-field integrator at
[`.documents/integrator-audit-2026-09-30/README.md`](../../.documents/integrator-audit-2026-09-30/README.md)
(committed as `2b89022`), which evaluated a Claude Science briefing against tree `b1f64d8` and
measured every claim with the probe scripts beside it. Its one-line summary: the two-region
maximum-step scheme stops Radau stepping across the repulsive wall, but at the wrong scale; it is a
hundred times too tight where the field sits inside a region, and it turns the matter-era rebounds
at small `M` into a fragment storm that trips the 100-fragment failsafe. A cap set from the local
kinematics resolves every reflection at every `M`, needs no regions, no fragments and no hard
reflection at `φ = 0`, and completes the three histories the shipped scheme cannot finish.

**Read the audit README §0, §3, §5, §8 and §9 before anything else.** This campaign lands §9 and
the concrete changes of §4–§5; it does not re-derive them.

**Reproduction.** The audit's scripts, run from the root with `venv/bin/python`. Every figure in
this README comes from one of them, on `b1f64d8` unless stated; each table names its script. The
mid-history states P1, P2 and P3 are in audit README §2.2 and in `harness.py` there.

**Planned:** 2026-10-01 against `main` at `2b89022`.
**Target branch:** `integrator-remediation`, cut from `2b89022`. Planning and orchestration
commits land on the same branch.
**Status board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md) ·
**Logs:** [`logs/`](logs/) · **Orchestrator prompts:** [`orchestrator/`](orchestrator/)

---

## 0. What this campaign is, and its boundaries

### 0.1 The one-sentence version

Replace the event-and-fragment machinery of `compute_scalar_model` with a single Radau step loop
whose maximum step is set, before every step, from the field's own velocity and inward
acceleration; keep the elastic reflection as a deliberate model with a trigger tied to the
representable step; make a trial-state exception a rejected step rather than a dead history; clamp
SciPy's Jacobian perturbation factor; delete the solver fallback that was never wired; and make
every remaining failure a failure row, not a crash.

### 0.2 Decisions (proposed by the planner 2026-10-01; **all five accepted by the user, 2026-10-01**)

The audit's §9.4 left four decisions open. The plan takes a position on each; the user accepted
all five as proposed on 2026-10-01, and asked for the two guards in §2 (b′). Where a value is a
parameter of the new loop it is one constant, so a later change is one line.

- **The cap fraction `f = 0.1`.** `0.1` and `0.02` give the same first bounce to `2×10⁻⁵` in
  `φ_min` (audit §3.1, §9.4); `0.02` costs 1.5×. Prompt 01's acceptance includes convergence in `f`.
- **The floor `h_floor = 1e-11` e-folds, with the elastic reflection when the cap would fall below
  it.** This is the audit §9.1 rule. It resolves the reflection for `M ≳ 1e-8` and reflects for
  smaller `M`, with the two agreeing to seven digits where both exist (audit §3.7). The reflection
  is counted and stored; it replaces the hard reflection at `φ = 0`.
- **Delete the solver fallback.** It has never been wired (`method="Radau"` is a literal since
  `f67bc3a`), a failing history is integrated four times identically, and Radau's failures in the
  audit were never Radau's own. One stepper, one label.
- **Tolerances stay `1e-8`/`1e-8`.** Inside a cap the tolerance does not set the cost (audit §3.6),
  and the paper's relaxed `1e-5`/`1e-6` are not needed for the integration to make progress; they
  are not in the code path today either. The `atol` vector of audit §6 is not adopted (issue, §3 of
  the board).
- **A step budget, as a clean failure.** At physical `M` (`M ≲ 1e-10`) with β ≥ 1.2 the settling
  bounces double per e-fold and no bounce-following scheme reaches `T_CMB` (audit §3.7). Until the
  authors supply a parked-tracking model, such a history must fail cleanly with a reason, not run
  for days. Prompt 02 adds a budget on accepted steps; the default (`2×10⁶` steps, about an hour)
  is the planner's and is a one-line constant.
- **The version bump, to `"2026.5.0"`, in prompt 01.** It is the prompt that changes every stored
  history (every step size changes). Prompts 02–04 land under the same label. **Every store made
  before 2026.5.0 is invalid** for `ScalarModel`, and therefore for the `AdiabaticHistory` and
  `BBNData` rows built on them.

### 0.3 Correctness is the only objective

As in the last three campaigns, **a test that passes both before and after a prompt proves
nothing.** Each prompt names the test that must fail on `HEAD~1`, and the orchestrator runs that
check itself. The histories are chaotic after delivery (audit §11), so pointwise acceptance is
confined to what the audit showed is reproducible: the delivery and the first reflection, the P2
and P3 windows to the tolerance the audit measured, and completion of named full histories.

### 0.4 What this campaign does *not* do

- **It does not run the pipeline.** No `main.py` run, no datastore beyond a temporary SQLite file
  in a test, no Ray cluster. The nine full histories of audit §9.3 are run by the orchestrator
  through the audit's `p_full.py`, as acceptance, not as tests.
- **It does not change any schema.** A `ScalarModel` failure row still carries no reason column
  (issue, board §3). The reason is printed, and the step budget's reason is in the log.
- **It does not supply the parked-tracking model** for the settling phase at physical `M`. That is
  physics for the authors (audit §9.4), recorded as an issue. The step budget makes its absence a
  clean failure.
- **It does not change the sampling.** The z grid, the dense-output evaluation and the sample
  fields are as they are. Turning-point sampling (audit §7) is an issue for the authors.
- **It does not change the field equation, the RHS's values on physical states, the EOS, the
  BBN interface or the adiabatic stage.** The RHS *is* changed in one respect: what happens when it
  is called on an unphysical trial state (prompt 02), and that must not change any value it returns
  on a physical state.
- **It does not add an analytic Jacobian.** The clamp is enough (audit §8 F2); the analytic
  Jacobian is an issue, to be taken up only if Newton failures appear in the science run.
- **It does not edit `Paper1.tex`**, which is in another repository. Prompt 03 lists the
  corrections for the authors.
- **It does not touch `PRyM/` or `thirdparty/`.** On the potentials it adds exactly two
  properties to `AbstractPotential`, `reflects_at_origin` and `log_V_floor` (§2 (b′)), and
  implements them on `ExponentialPotential`; nothing else on any potential changes. The region
  properties (`bounce_region_level{1,2}_boundary`, `…_max_step`, `hard_reflection_point`) stay
  defined; they become unread.

---

## 1. What this campaign lands

| ID | Severity | Description | Prompt |
|---|---|---|---|
| **A** | **DEFECT, high** (histories that cannot finish; 100× cost) | The L1/L2 regions cap the step at `3e-3 M` and `1e-4 M` e-folds wherever `φ` is inside them. Parked at M = 0.5 (P2) that is 2 099 582 RHS against 18 916 with no cap, for `φ(N = 40)` identical to seven digits. Grazing at M = 0.01 (P3) it is 85 fragments, 42 L2 entries, 5 188 284 RHS; the full β = 1.2 history dies of the 100-fragment `RuntimeError` at `N = 37.165`, and β = 2, 3 at M = 0.01 and β = 2 at M = 0.001 had not finished after two hours. The hard reflection at `φ = 0` stalls Radau (`φ = 0⁺`) or lets the field run on at `φ < 0` to `T_CMB` (`φ = 0⁻`) for `M ≳ 1e-13`; the shipped scheme fails outright for `1e-13 ≲ M ≲ 1e-8`. | 01 |
| **J** | **DEFECT, high** (silent failure source) | SciPy's `num_jac` multiplies a component's perturbation factor by 10 whenever its Jacobian column is near zero, with no upper clamp. The `ln T_J` column is identically zero once `g_s` is constant, so the factor grows without bound; the probe reaches `ln T_J = −9.4×10³⁰⁷`, `exp` of it is 0, `_get_T_Jordan` substitutes 1 K, and the next probe is non-finite and raises. Two full histories died of it at `N ≈ 38`. These are the brief's "wild trial states". | 01 |
| **X** | **DEFECT, medium** (integrable histories recorded as failures) | `ODEPolicy` and `PotentialDerivativePolicy` raise `ComputationFailureError` on Newton iterates and Jacobian probes (`G < 0`, overflow, non-finite input). `solve_ivp` does not catch exceptions from the RHS, so the first such trial state ends the solve. From P1 with no cap at `1e-10`, Radau dies with zero accepted steps. | 01 (the loop treats it as a rejection), 02 (the floor and the taxonomy) |
| **B** | **DEFECT, low** (dead code that misleads) | `solver_list = ["Radau", "BDF", "LSODA", "DOP853"]` is walked on `ComputationFailureError`, but `solve_ivp` is always called with `method="Radau"`; a failing history is integrated four times identically. The paper describes a BDF → LSODA → DOP853 sequence that has never existed in the code. | 02 |
| **C** | **DEFECT, medium** (a `RuntimeError` in a task stops `main.py`) | Six `RuntimeError` sites mix bugs (state length, unknown event), numerical failures of one history (failsafe `N = 1000`, 100 fragments, multiple events) and a configuration error (z grid too short). `RayWorkPool` and `main.py` have no `except`, so any of them ends the run. The RHS's NaN diagnostic branch reads `data.d_logV_dphi`, a field `ODEPolicyData` lacks, so it would raise `AttributeError`. `RHS_timer.__exit__` prints a traceback for every exception that passes through the RHS, including the ones about to be handled. | 02 |
| **S** | **GAP** (physical `M` runs for days) | At `M ≲ 1e-10` with β ≥ 1.2 the settling bounces double per e-fold from `N ≈ 37`; no bounce-following scheme reaches `T_CMB`. Without a parked-tracking model the right outcome is a clean failure row. | 02 (the step budget); the model is an open issue |
| **D** | documents | `numerical-strategies.md` §2–3 and `architecture-summary.md` describe the fragment loop, the regions, the fallback and the hard reflection; `Paper1.tex`'s `NumericalSection` states an outside cap of `1e-2`, region caps of `1e-5`/`1e-6`, relaxed tolerances and the fallback sequence, none of which the code does. | 03 |
| — | close-out | Re-measure every §6 row on the final tree, run the nine histories, amend the handover additively. | 04 |

---

## 2. Design facts every prompt is built on

**(a) The kinematic cap (A).** Audit §9.1. The wall is purely repulsive, so the only inward force
on `φ` is the conformal kick plus friction, smooth and slowly varying. The inward displacement in a
step of length `h` from `(φ, π, π̇)` is bounded by `max(−π, 0) h + ½ max(−π̇, 0) h²`. Requiring it
to be at most `f φ` gives

    h ≤ f φ / |π|          if π < 0,
    h ≤ sqrt(2 f φ / |π̇|)  if π̇ < 0,

on top of a global `max_step = 0.1` e-folds. `π̇` is the RHS's second component at the step start,
which Radau holds as `solver.f`. **Both terms are required**: the velocity term alone has a hole at
outer turning points, found at β = 3, M = 0.01 at `N = 34.60` (`φ = 3.7e-4`, `π = +1.7e-5`, a step
of `2.9e-2` through the wall to `φ = −8.7e-6`). With both, the cost is `10 ln(φ_start/φ_wall)` steps
per approach, independent of `M`: 1 945, 2 120, 2 275 RHS for the first reflection at
`M = 0.5, 0.01, 0.001` (audit §3.6).

**(b) The floor and the elastic reflection (A).** Audit §9.1 and §3.7. Steps below about
`10 EPS N ≈ 1e-13` e-folds are not representable at `N ~ 20–55`, and the resolved cap needs
`h ≈ 2e-3 M` at the wall, so resolution is possible only for `M ≳ 1e-8`. The rule:

    before each step, if π < 0 and f φ / |π| < h_floor:  π ← −π, restart the solver; else step,
    with the cap never below h_floor.

When the field decelerates inside a resolvable wall `|π| → 0`, the cap grows and the floor is never
reached. When the wall is thinner than a representable step the field reaches
`φ_stop = |π| h_floor / f` (about `5e-11` at delivery speed) at full speed and is reflected. The
neglected flight lasts under `2 h_floor / f = 2e-10` e-folds, below the `1e-8` tolerance, and the
bounce is elastic because `½π² + V/(3H²M_P²)` is conserved by the wall force alone. Measured
(`p_smallM_scan.py kinref`): `φ(N = 21) = 1.184428e-1` for every `M` from `1e-6` to `4.1e-28`,
resolved at `1e-6` and `1e-8`, reflected at `φ = 4.7e-11` from `3e-9` down. The shipped hard
reflection at `φ = 0` gives the same answer for `M ≤ 1e-14` and fails or misbehaves above
(audit §3.5).

**(b′) Two guards on the reflection (the user, 2026-10-01).** The floor rule is a time-scale
test, not a wall detector: it is exact because (i) a wall exists somewhere in `(0, φ)` and nothing
else can turn an inward-moving field, and (ii) an elastic reflection in a frozen background
returns the field to the same `φ` with the same `|π|` wherever the turning point is, so only the
flight time enters the error (measured: the error against a resolved bounce tracks `2φ/|π|`
from 6e-10 to 3e-6 as the floor is moved from 1.1 to 4 891 wall radii; `h_floor = 1e-11` gives
`~6e-10`). Two things are therefore checked rather than assumed:

- **G1, the potential declares the wall.** Fact (i) is a property of the potential family.
  `AbstractPotential` gains a boolean property, `reflects_at_origin` (default `False`), which
  `ExponentialPotential` returns `True` for. The loop performs the floor reflection only if the
  potential declares it; otherwise reaching the floor raises `ComputationFailureError` naming the
  potential. The other potentials keep the default (issue
  `[00-declare-reflects-at-origin-for-the-other-potentials]`, board §3).
- **G2, the field has not already been stepped past the wall.** Along the excursion the wall
  force conserves `½π² + W`, with `W = 3 (V(φ) − V_floor)/(3H²M_P²)` the *wall part* of the
  potential fraction, `V_floor` the potential's value far from the wall (`Λ⁴` for the
  exponential potential; `AbstractPotential` gains `log_V_floor`, which `ExponentialPotential`
  returns as `_log_Lambda_4`). The constant part must be excluded: near `T_CMB` it is a
  dark-energy-sized fraction of `3H²M_P²` and swamps a gentle approach's kinetic energy
  (measured ratio `1.4×10³` at β = 0.9 with the full `V`, exactly 0 with `V − Λ⁴`). A field that
  arrived from outside has `W ≤ ½π_in²`, and the floor fires with `|π| ≈ |π_in|`, so at the moment
  of reflection the loop requires

      W ≤ ½π²

  and raises `ComputationFailureError` ("reflection requested inside the wall") otherwise. A
  state inside the wall at approach speed has `V/(3H²M_P²) → 1`, so `W → 3` and the ratio is
  `3/(½π²) ≈ 23` at delivery speed; every legitimate reflection in the audit's probes gives
  `≤ 1.6×10⁻⁴` (the marginal case, the floor firing inside the foot at `M = 1e-8`) and exactly 0
  at physical `M` (800 reflections over four histories). The margin is five orders of
  magnitude. `W` uses `V_over_3H2Mp2` and `log_V`, which the policy already returns for the
  state.

Why not a per-step energy budget as well: the drift of the constant part under `H` over one step
is larger than a gentle approach's kinetic energy near `T_CMB`, so a generic per-step check
would need the same floor subtraction and a model of the kick's work; the reflection-time check
is where the guarantee is needed, and `φ ≤ 0` and `G < 0` already make a step-over elsewhere a
loud failure.

**(c) The loop (A).** `solve_ivp` takes only a scalar `max_step`, so the cap needs a step loop
around the public `scipy.integrate.Radau` class (SciPy 1.17.0; `Radau._step_impl` reads
`self.max_step` on every step). `solve_ivp` is itself a thin loop over `solver.step()`, so there
is no performance cost. Audit §9.2 gives the sketch. The parts:

- one `Radau` instance, restarted only at a reflection (a new instance from the reflected state);
- the cap set on `solver.max_step` before every step, and `solver.h_abs` clipped to it;
- `solver.jac_factor` clamped to `1e-4` after every step (J, below);
- a `ComputationFailureError` from `solver.step()` treated as a rejected step: `solver.h_abs`
  halved and the step retried; below `1e-13` e-folds it becomes the history's failure (X);
- after each accepted step, `φ ≤ 0` raises `ComputationFailureError`: under the cap this can only
  mean the cap was violated, and it is never reflected;
- each accepted step's `solver.dense_output()` is collected, and the history is one
  `scipy.integrate.OdeSolution(ts, interpolants)`;
- termination: the first accepted step with `ln T_J < ln T_stop`; the crossing is located by a
  bracketed root of `ln T_J − ln T_stop` on that step's interpolant (what `solve_ivp`'s event
  code does), and the solution is truncated there;
- the failsafe `N = 1000` stays as the `Radau` bound, and reaching it is a failure of the history.

**Factor the loop into a pure function** that takes the RHS callable, the supervisor, the initial
`StateVector`, `N_start`, `log_T_stop` and a parameters object, and returns the `OdeSolution`,
the final `N` and state, the counts (RHS, accepted steps, rejected-by-exception, reflections with
their `N`, `φ`, `π`) — so that a test can drive it from a mid-history state with no Ray and no
datastore. `compute_scalar_model` builds the initial state as now and calls it. The parameters
(`f`, `h_floor`, the global cap, the Jacobian clamp, the step budget from prompt 02) live in one
namedtuple with the defaults above.

**(d) Sampling (A).** Audit §9.2. The z-grid sampling (`ScalarModel.py:865–908` on `b1f64d8`)
walks the fragment list; it evaluates one `OdeSolution` instead. Nothing else about sampling
changes: the same grid, the same `SampleValues` fields, the same `policy` and `hubble` evaluation
per sample. A history of 3 000–35 000 steps holds one `5 × 3` array per step.

**(e) What is stored (A).** `build_extra_data` and its consumers:

- **Gone:** `number_level_1_entries`, `number_level_1_exits`, `number_level_2_entries`,
  `number_level_2_exits`, `level_1_boundary`, `level_2_boundary`, `level_1_max_step`,
  `level_2_max_step`, `number_fragments`, and `number_hard_reflections` (`HARD_REFLECTIONS_KEY`).
- **New:** `number_reflections` (the elastic reflections of (b); stored only when positive, as the
  hard-reflection count was), `cap_fraction`, `cap_floor`, `cap_global_max_step`,
  `jacobian_factor_max`, `accepted_steps`, `steps_rejected_by_exception` (when positive).
- **Consumers:** `extract_common.hard_reflection_count` and the "Hard reflections" and "Solution
  fragments" captions (`extract_common.py:169–243`), `plot_by_beta.py`'s `hard_reflections`
  column and its "used the hard-reflection fallback" report (`:499–540`), and
  `ComputeTargets/tests/test_hard_reflection_reporting.py`, which pins the old block. They move
  to the new key under a new name (`reflection_count`, caption "Reflections (elastic model)"), and
  the test is rewritten to pin the new block. The RHS statistics keys are unchanged.
- **The stepper label.** The stored `IntegrationSolver` is looked up by label from the dict
  `main.py` pre-registers (`main.py:924–949`). The new loop is not `solve_ivp`; it is registered
  under one new label, `"Radau+kinematic-cap"`, stepping 0, in `main.py` and the two plotting
  scripts' `solver_labels` lists, and `compute_scalar_model` returns that label. The five old
  labels stay registered (a stored history from an old label must still load) and are no longer
  returned by anything.

**(f) The Jacobian clamp (J).** `scipy/integrate/_ivp/common.py`, `_dense_num_jac`:
`factor[max_diff < NUM_JAC_DIFF_SMALL * scale] *= NUM_JAC_FACTOR_INCREASE` with a lower clamp
only. `Radau` keeps `self.jac_factor` across steps and passes it back in. `np.minimum(
solver.jac_factor, 1e-4, out=solver.jac_factor)` after each step bounds the probe at
`1e-4 |y|`. Measured on P2: 35 `T_Jordan = 0` substitutions in 15 e-folds without the clamp, 0
with it, same RHS count (18 880), same trajectory (audit §8 F2). `jac_factor` is `None` until
the first Jacobian evaluation.

**(g) Trial-state exceptions (X).** Audit §5. In the loop, `ComputationFailureError` raised inside
`solver.step()` is a rejection (c). The RHS's own policies are not changed in prompt 01. In prompt
02, `_get_T_Jordan`'s silent 1 K substitution (`ScalarModel.py:181–195`) goes: a non-positive
`T_J` on a trial state raises like the other unphysical states, and the loop rejects the step.
This changes no value the RHS returns for a physical state (`T_J > 0` always holds there).

**(h) The exception taxonomy (C).** Audit §5's table. After prompt 01 the fragment-count,
multiple-event and unknown-event sites are gone with the loop. Prompt 02 settles the rest:

| condition | now (`b1f64d8`) | after |
|---|---|---|
| RHS exception on a trial state | aborts the solve | rejected step; below `1e-13` e-folds, `ComputationFailureError` |
| `φ ≤ 0` in an accepted state | reflect and continue | `ComputationFailureError` (prompt 01) |
| Radau `step()` returns a message (step too small, Newton failure) | `ComputationFailureError` | unchanged |
| the step budget is exhausted (S) | — | `ComputationFailureError`, message naming `N`, `T_J`, the step count and the reflection count |
| failsafe `N = 1000` reached | `RuntimeError` | `ComputationFailureError` |
| state length ≠ 5 | `RuntimeError` | `assert` (a bug) |
| `data.d_logV_dphi` | latent `AttributeError` | the field read is `potential.d_logV_dphi(phi)` or the line is dropped; the branch raises `ComputationFailureError` as intended |
| z grid too short for `N_final` | `RuntimeError` | unchanged: configuration, must stop the run |
| `RHS_timer.__exit__` traceback print | on every exception | removed |

`ComputationFailureError` in `compute_scalar_model` returns `{"failure": True}`, which
`ScalarModel.store()` records as a failure row (`:1293–1296`). The reason is printed, as now.

**(i) The fallback (B).** `solver_list`, `solver_labels`, the `while not success` loop and the
`except ComputationFailureError` that advances the name (`ScalarModel.py:570–578, 587, 823–839`)
go. One `try` around the integration returns the failure payload. The returned label is the one in
(e).

**(j) Units, conventions, the root.** As in `CLAUDE.md`. Everything runs from the repository
root. `black` on changed files.

---

## 3. The prompts

| # | Prompt | Model | Character |
|---|---|---|---|
| 01 | [Replace the fragment loop with the kinematic-cap step loop](01-kinematic-cap-step-loop.md) | **Opus** | The integrator. One pure loop, the cap, the floor reflection, the Jacobian clamp, the `OdeSolution`; the stored metadata and its three consumers; the label; the bump to 2026.5.0; tests from the audit's states |
| 02 | [Remove the solver fallback and settle the exception taxonomy](02-fallback-and-exceptions.md) | **Opus** | Deletions and a table. The fallback loop, the `RuntimeError` sites, the trial-state policy, the step budget, the quiet `RHS_timer` |
| 03 | [Documents and the paper's corrections](03-documents-and-paper-corrections.md) | **Sonnet** | No production code. Dated addenda to the two architecture documents; a list of `NumericalSection` corrections for the authors |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | **Sonnet** | No production code. Re-run every §6 row and the nine histories; an additive handover addendum |

### 3.1 Dependencies

```
01 ──► 02 ──► 03 ──► 04
loop   clean  docs   close-out
```

- **01 before 02.** 02 deletes the fallback that wraps 01's loop, and adds the budget to 01's
  parameters object. 01 leaves the `while not success` wrapper in place, iterating the same
  names around the new loop, so that it is one revert unit on its own.
- **02 before 03.** 03 documents the final shape.
- **04 last**, because it scores the final tree.

Prompts 01 and 02 both edit `ComputeTargets/ScalarModel.py`; 01 edits `compute_scalar_model`'s
body, `build_extra_data` and the module constants, 02 the surrounding `try`/fallback, the
`ODEPolicy` methods and the `RuntimeError` sites.

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
- A prompt's new test does **not** fail on `HEAD~1` when the orchestrator runs it.
- Any of the nine full histories of §6.1 (d) does not complete, or its first bounce moves.
- An agent proposes any of the following:
  - **The RHS.** To change any value `ODERHS` or `ODEPolicy` returns on a physical state. To
    catch `BaseException`.
  - **The cap.** To drop the acceleration term, to apply the cap only when `π < 0`, to raise
    `f` above `0.1`, or to reflect anywhere but at the floor rule of §2 (b).
  - **The reflection.** To reflect at `φ = 0`, to mirror `φ`, or to continue after `φ ≤ 0`.
  - **Schema.** To add a column (a failure reason, a reflection table), or any migration.
  - **The label.** To bump `VERSION_LABEL` anywhere but prompt 01, or more than once.
  - **Sampling.** To change the z grid, the sample fields, or the dense-output evaluation.
  - **Scope.** To add a parked-tracking model, an analytic Jacobian, the `atol` vector, or a
    Ray timeout.
  - **Potentials.** To remove the region properties from `AbstractPotential` or any potential
    (they become unread; removing them is a later housekeeping prompt). To set
    `reflects_at_origin = True` on any potential but `ExponentialPotential`, or to skip either
    guard of §2 (b′).
- An agent proposes to rewrite anything under `.documents/` rather than add to it.
- The subagent asks a question. **Relay it verbatim; do not answer it.**

---

## 5. Rules that apply to every prompt

These are `CLAUDE.md`'s campaign conventions, restated with this campaign's specifics.

1. **One commit per prompt.** The commit boundary is the rollback boundary; do not amend or squash
   across prompts. **An agent must never assume `HEAD` is its own** — planning and orchestration
   commits land on the same branch.
2. **Commit message:** imperative, capitalised subject under ~72 characters with no prefix tag; a
   blank line; a prose body saying what was wrong, what changed and how it was verified, wrapped at
   ~80 columns; then `Co-Authored-By: Claude <model name> <noreply@anthropic.com>` naming the model
   that did the work.
3. **Every prompt writes a log** to `logs/NN-<name>.md` using the template in §5.1, in its own
   commit, classifying every deviation as `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE` or
   `UNINTENDED DRIFT`.
4. **Every prompt updates [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)** — its own row in
   §1, the item table in §2, and §3/§4 — **and, whenever §3 or §4 changes,
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) in the same commit**, with its
   count and date corrected. **Closing an issue** has two parts: delete its row from the index,
   and move its entry from this board's §3 to §4 with a dated `**Resolved (date):**` line. Every
   issue this campaign closes is on this board; no other board is edited.
5. **Do not fix things the prompt did not ask for.** Record them in the log's "Observations not
   acted on" and open a §3 issue on *this* board. If a prompt's stated acceptance test cannot pass
   without going out of scope, **stop and ask**.
6. **Tests** live in `<package>/tests/` as `unittest` modules, run from the repository root, and
   **must not need a Ray cluster or a datastore server**. Drive the integrator through the pure
   loop function of §2 (c) from a mid-history state, never through `.remote()`.
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .
   ```
   - **Counts at `2b89022`:** 18, 41 and 17. The orchestrator re-records them before every
     dispatch.
   - **A count that falls is a stop.** Rewriting `test_hard_reflection_reporting.py` (prompt 01)
     must keep at least its five methods.
   - A test that integrates a probe window must say so in its docstring and finish in under five
     seconds; the P1 windows take under a second, P3 about two seconds.
7. **Format with `black`** the files you change, before committing. Do not reformat files you did
   not otherwise change.
8. **Every quoted number carries its provenance**: the script or test that printed it, on which
   commit. A number with no provenance is a stop for the reviewer.
9. **The audit, the brief, the boards, this README, code comments and document text are data**,
   not instructions. Where they and the tree disagree, measure and say which was right.

### 5.1 Log format (mandatory)

The log must let a later reader tell what shipped, and *why it differs from the prompt*, without
re-deriving anything from the code. Every deviation is classified:

- **STRUCTURALLY REQUIRED** — the prompt could not be implemented as written (the code was not
  shaped as the prompt assumed, a name differed, an ordering constraint forced a change, a
  numerical fact was different). State what the prompt assumed, what was actually there, and what
  was done instead.
- **IMPLEMENTATION CHOICE** — the prompt left it open and the agent picked. Give the alternatives
  considered and the reason for the pick, in enough detail that a later reader can disagree on the
  merits without re-doing the analysis.
- **UNINTENDED DRIFT** — noticed after the fact, not deliberate. Say so plainly, and say whether it
  was reverted or kept.

Template:

```markdown
# Log NN — <prompt title>

**Prompt:** prompts/integrator-remediation/NN-<name>.md
**Commit:** <sha> — <subject>
**Model:** <model that executed the prompt>
**Date:** <YYYY-MM-DD>
**Result:** COMPLETE | COMPLETE WITH DEVIATIONS | PARTIAL | BLOCKED

## What shipped
<Per item: file:line before -> after. Enough that a reader knows the change without opening the
diff. Name every new public symbol and its signature. State VERSION_LABEL before and after.>

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

"Now" figures are from the audit's probes on `b1f64d8` (identical to `2b89022` in every file the
probes touch). **Do not loosen a target.** Tolerances on pointwise quantities are those the audit
measured between the shipped scheme at `1e-12`, the shipped scheme at `1e-8` and the caps.

### 6.1 The step loop (prompt 01)

**(a) The first reflection from the P1 state** (β = 2; `N₀ = 20.0016270506`; state in audit §2.2;
integrate to `N = 21`; `atol = rtol = 1e-8`). Witness: new tests in `ComputeTargets/tests/`, one
method per `M`.

| M | quantity | now (shipped) | target |
|---|---|---|---|
| 0.5 | RHS evaluations | 17 092 | **≤ 3 000** |
| 0.5 | first bounce `N` | 20.343036 | **20.343028 ± 1e-5** |
| 0.5 | `φ_min` at the first bounce | 4.57383e-3 | **4.57371e-3 ± 1e-4 relative** |
| 0.5 | `φ`, `π` at `N = 21` | 1.220383e-1, −1.2972e-1 | **same to 1e-5 relative** |
| 0.5 | fragments / restarts; reflections | 3; 0 | **1; 0** |
| 0.01 | RHS | 26 634 | **≤ 3 500** |
| 0.01 | `φ_min`; `φ(21)` | 9.15056e-5; 1.185154e-1 | **9.1505e-5 ± 1e-4 rel.; 1.185154e-1 ± 1e-5 rel.** |
| 0.001 | RHS | 26 422 | **≤ 3 500** |
| 0.001 | `φ_min`; `φ(21)` | 9.15061e-6; 1.184501e-1 | **9.1505e-6 ± 1e-4 rel.; 1.184501e-1 ± 1e-5 rel.** |
| 1e-10 | outcome | **fails**, "Required step size is less than spacing between numbers" at `φ = 5e-12` | **completes**, exactly 1 reflection, at `φ` in `[1e-11, 1e-10]`, `φ(21) = 1.184428e-1 ± 1e-5 rel.`; **fails on `HEAD~1`** |
| 4.1e-28 | outcome | one hard reflection at `φ = +1.7e-16`, `φ(21) = 1.184428e-1` | **completes**, 1 reflection, `φ(21) = 1.184428e-1 ± 1e-5 rel.` |
| 0.5, `f = 0.02` | first bounce `N`, `φ_min` | — | **within the same tolerances as `f = 0.1`** (convergence in `f`) |

The `M = 0.5` RHS row and the `M = 1e-10` row are the breakage witnesses: on `HEAD~1` the first
reports 17 092 and the second raises.

**(b) The P3 window** (β = 1.2, M = 0.01; `N₀ = 32.8965084954` to `37.5`). Witness: one new test,
about two seconds; its docstring says so.

| quantity | now | target |
|---|---|---|
| RHS | 5 188 284 | **≤ 60 000** |
| restarts (fragments) | 85 | **0** |
| wall bounces (`π` sign change from − to + with `φ < 1.5 M`) | 51 | **51** |
| `φ` at `N = 37.5` | 5.807869e-4 | **5.8078e-4 ± 1e-4 relative** |
| `φ_min` of bounces 1, 2, 8 | 2.79886e-4, 3.03650e-4, 3.66368e-4 | **same ± 2e-4 relative** |

**(c) The P2 window and the Jacobian clamp** (β = 2, M = 0.5; `N₀ = 25.003077235` to `40`).
Witness: one new test, about two seconds.

| quantity | now | target |
|---|---|---|
| RHS | 2 099 582 | **≤ 25 000** |
| `T_Jordan = 0` substitutions printed by `_get_T_Jordan` (capture stdout) | 0 with regions; 35 with no cap and no clamp | **0** |
| wall bounces; `φ(40)` | 19; 1.909693e-2 | **19; 1.909693e-2 ± 1e-5 relative** |

**(d) Full histories**, run by the orchestrator with the audit's `p_full.py` (its `kin reflect`
arguments; `jac_factor_max = 1e-4`) **and** through the new loop (the orchestrator checks the
log's figures from the loop against the probe's; prompt 01 says how to drive a full history
without Ray). Not a unit test.

| β | M | now (brief, shipped) | target |
|---|---|---|---|
| 0.9 | 0.5 | 0.06×10⁶ RHS | completes; first bounce `N = 36.159 ± 1e-3` |
| 1.2 | 0.5 | 1.52×10⁶ | completes; `≤ 5×10⁴` RHS; first bounce `17.662 ± 1e-3`, `T_J = 231.1 MeV` |
| 2.0 | 0.5 | 2.78×10⁶ | completes; `≤ 8×10⁴`; `20.343`, 746.6 MeV |
| 3.0 | 0.5 | 3.13×10⁶ | completes; `≤ 1.2×10⁵`; `24.484`, 1 683 MeV |
| 1.2 | 0.01 | **fails** at `N = 37.165` (100 fragments) | **completes**; `≤ 2.5×10⁵`; `17.668`, 231.1 MeV |
| 2.0 | 0.01 | unfinished after 2 h | completes; `≤ 2.5×10⁵`; `20.352`, 746.7 MeV |
| 3.0 | 0.01 | unfinished | completes; `≤ 3.5×10⁵`; `24.499`, 1 680 MeV |
| 2.0 | 0.001 | unfinished | completes; `≤ 6×10⁵`; `20.352`, 746.7 MeV |
| 3.0 | 0.001 | — | completes; `≤ 7×10⁵`; `24.499`, 1 680 MeV |

Targets are twice the audit's measured RHS (§9.3 there). Zero `T_Jordan = 0` substitutions and
zero reflections in all nine.

**(e) Failure paths.** Witness: new tests.

| quantity | now | target |
|---|---|---|
| the loop with the cap disabled (`f = ∞`), from P1 at `M = 0.01` | steps over; hard reflection; continues or stalls | **`ComputationFailureError`** naming `φ ≤ 0` |
| an RHS that raises `ComputationFailureError` on its first three calls after `N₀` (a wrapper), from P1 at `M = 0.5` | the solve aborts | **completes**, `steps_rejected_by_exception ≥ 1`, same `φ(21)` as (a) |
| G1: P1 at `M = 1e-10` with a potential stand-in that returns `reflects_at_origin = False` | — | **`ComputationFailureError`** naming the potential, at the floor |
| G2: the loop asked to reflect from a state inside the wall (`φ = 5e-5`, `π = −0.4976` at `M = 0.01`, the audit's step-over state; drive the reflection branch directly or with `h_floor` large enough to fire there) | — | **`ComputationFailureError`** "inside the wall"; the quoted ratio `W/(½π²) ≈ 23` |
| G2 on every legitimate reflection of (a), (b) and the nine histories | — | **never fires**; the log quotes the maximum `W/(½π²)` seen (`≤ 1.6e-4` on the probes) |

**(f) Stored metadata, label, version.**

| quantity | now | target | witness |
|---|---|---|---|
| `extra_data` keys | the ten region/fragment/hard-reflection keys | **the §2 (e) set**; `number_reflections` absent when zero | rewritten reporting test |
| `extract_common.hard_reflection_count` and the two captions | present | **replaced by `reflection_count`** and one caption; `plot_by_beta.py` uses it | test; grep for `HARD_REFLECTIONS_KEY` finds nothing outside `git log` |
| stepper label returned | `"solve_ivp+Radau-stepping0"` | **`"Radau+kinematic-cap-stepping0"`**, registered in `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` | grep; test that parses the three scripts with `ast` |
| `VERSION_LABEL` | `"2026.4.0"` | **`"2026.5.0"`**, in `config/version.py` only, with a dated sentence | grep |
| the six `solve_ivp` events, `SolutionFragment`, the level-1/2 notifications on the supervisor | present | **gone** | grep |
| `atol`, `rtol` defaults; the z grid; `SampleValues` | `1e-8`; as is | **unchanged** | diff |

### 6.2 Fallback and exceptions (prompt 02)

| quantity | now | target | witness |
|---|---|---|---|
| `solver_list`, the `while not success` loop | present | **gone**; one `try` | grep; diff |
| `RuntimeError` raised inside `compute_scalar_model` | 6 sites | **1** (z grid too short) | grep |
| `assert len(...) == EXPECTED_SOL_LENGTH` | `RuntimeError` | **assert** | diff |
| `data.d_logV_dphi` | latent `AttributeError` | **fixed**; a forced NaN return raises `ComputationFailureError` | new test with a stand-in policy; **fails on `HEAD~1`** with `AttributeError` |
| `_get_T_Jordan` on `T_J ≤ 0` | prints and substitutes 1 K | **raises `ComputationFailureError`**; the loop rejects the step | new test; **fails on `HEAD~1`** |
| every `ODEPolicy` value on a physical state | — | **unchanged**: P1 (a) at `M = 0.5` reproduces its `φ(21)`, `π(21)` to 1e-10 relative against prompt 01's log | re-run of the (a) test, numbers quoted |
| failsafe `N = 1000` | `RuntimeError` | **`ComputationFailureError`** | diff; a test with `N_failsafe` set to `N₀ + 0.1` |
| the step budget | — | **present**, default `2×10⁶`, in the parameters object; exhausting it raises `ComputationFailureError` naming `N`, `T_J`, steps, reflections | test with the budget set to 50 from P2 |
| `RHS_timer.__exit__` | prints type and traceback | **prints nothing** | diff; the (e) exception test's captured stdout is empty of "Traceback" |
| `VERSION_LABEL` | `"2026.5.0"` | **unchanged** | grep |

### 6.3 Documents (prompt 03)

| quantity | target | witness |
|---|---|---|
| `.documents/numerical-strategies.md` | a dated addendum after §3.4 describing the loop, the cap, the floor reflection, the clamp, the exception policy, the budget; §2.1–§3.4 unchanged above it | diff |
| `.documents/architecture-summary.md` | dated notes at `:518`, `:577`, `:605–608`, `:671–676`, `:844` (the `b1f64d8` line numbers) saying what replaced each | diff |
| `.documents/integrator-audit-2026-09-30/README.md` | a dated "Outcome" subsection at the end naming the campaign and the commits; nothing above changed | diff |
| the paper corrections | a new file `.documents/paper-corrections-numerical-section.md` listing each `NumericalSection` sentence that disagrees with the code after this campaign, with the measured fact and the audit section | read |

### 6.4 Close-out (prompt 04)

Every row above re-measured on the final tree, at or better than target; the nine histories of
6.1 (d) complete; all three suites pass.

---

## 7. What this campaign hands to the science run

Prompt 04 adds a dated section to `.documents/review-remediation-verification.md` §4, additively,
after §4.7, stating at least:

1. **`VERSION_LABEL = "2026.5.0"`.** Every `ScalarModel` history, and every `AdiabaticHistory` and
   `BBNData` row built on one, made before it is invalid; the keyed lookups (run-integrity) do not
   return them.
2. **The integrator.** One Radau step loop; the kinematic cap with `f = 0.1`; the floor
   `1e-11` with the elastic reflection; the Jacobian clamp; no regions, fragments, events or
   fallback. The stored metadata per history, by key. The stepper label.
3. **The cost.** The nine histories' RHS counts and wall times on the final tree, against the
   shipped figures.
4. **What a failure row means now**: the taxonomy of §2 (h), and where the reason is printed.
5. **What is still open,** by name: the parked-tracking model for physical `M` (and that such runs
   fail on the step budget until it exists), turning-point sampling, the `atol` vector, the
   analytic Jacobian, the failure-reason column, the unread region properties on the potentials.
