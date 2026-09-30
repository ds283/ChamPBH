# Log 02 — Detect BBN solver failures; bump the version

**Prompt:** prompts/run-integrity/02-detect-bbn-solver-failures.md
**Commit:** the commit that adds this file ("Detect PRyMordial solver failures and bump to 2026.4.0"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE WITH DEVIATIONS

Every README §6.2 row is met (Verification). No stop condition of prompt §5 was met: every pinned
abundance passes unchanged, all five production stages (and both small-network ones) can be
forced to fail through `solve_ivp`, and the boundary sits at the PRyMordial call with no `except`
added anywhere else. The deviations are IMPLEMENTATION CHOICEs, plus one premise of README §2 (c)
and §6.2 that the tree contradicts (Deviation 5). It needed no change to what the prompt asked
for, but the orchestrator should see it.

**Consequence: every store made before `VERSION_LABEL = "2026.4.0"` is invalid.** Since prompt 01
a lookup no longer returns such rows, so an old store opened under 2026.4.0 recomputes every
compute target beside its old rows.

## What shipped

`VERSION_LABEL`: `"2026.3.0"` → **`"2026.4.0"`** (`config/version.py:33`).
`PRYM_VERSION`: `"bf24c3d+cham03"` → **`"bf24c3d+cham03+ri02"`** (`ComputeTargets/BBNData.py:42`).
No schema change, and no change to `main.py` or to any factory.

**F1 — PRyMordial checks its solves** (`PRyM/PRyM_main.py`, not black-formatted).

- **New, `:10–22`: `class PRyMSolverFailureError(Exception)`**, with
  `__init__(self, stage, status, solver_message, t_reached, t_target)`. It keeps all five as
  attributes (`.stage`, `.status`, `.solver_message`, `.t_reached`, `.t_target`). Its message is
  `solve_ivp failed in stage '<stage>': status=<status>, message=<repr>; t reached <t> of target <t>`.
- **New, `:26–32`: `_check_solve_ivp(sol, stage, t_target)`.** It raises
  `PRyMSolverFailureError` if `not sol.success`. The t reached is `sol.t[-1]`, or NaN if `sol.t`
  is empty.
- **The eight call sites.** Each has the marker comment `# ChamPBH run-integrity prompt 02: check
  the solve` and then one `_check_solve_ivp` call, inserted immediately after the `solve_ivp`
  call's closing parenthesis and before the first line that reads the result. Line numbers are
  on the patched file; the original line is in brackets.

  | `solve_ivp` at (was) | Check at | Stage name | Target t | Runs under production's flags |
  |---|---|---|---|---|
  | `:224` (`:199`) | `:233–234` | `thermodynamics (with NP)` | `tfin` | yes (`NP_thermo_flag`) |
  | `:266` (`:239`) | `:275–276` | `thermodynamics (no NP)` | `tfin` | no; exercised on a successful solve by `test_prym_passenger`'s `NP_thermo_flag=False` reference |
  | `:454` (`:425`) | `:463–464` | `a(T)` | `Tini_vec[1]` (ln T_start) | yes (`aTid_flag`) |
  | `:616` (`:585`) | `:624–625` | `high-T n <-> p` | `t_fin` | yes |
  | `:1055` (`:1022`) | `:1064–1065` | `mid-T nuclear network (small)` | `t_fin` | no (`small_network=True`) |
  | `:1132` (`:1097`) | `:1141–1142` | `mid-T nuclear network (full)` | `t_fin` | yes |
  | `:1226` (`:1189`) | `:1234–1235` | `low-T nuclear network (small)` | `t_fin` | no (`small_network=True`) |
  | `:1291` (`:1252`) | `:1299–1300` | `low-T nuclear network (full)` | `t_fin` | yes |

  The class and the helper carry the same marker (`:10`, `:26`). The call sites still use the
  module-level name `solve_ivp`. No tolerance, method, argument or `julia_flag` branch changed.
  `git diff -- PRyM/` is 41 insertions and no deletions.
- **Which calls run under production's flags.** Confirmed from `PRyM/PRyM_init.py`:
  `compute_bckg_flag = True` (`:66`), `aTid_flag = True` (`:64`), `julia_flag = False` (`:115`).
  `_configure_PRyMordial` sets `NP_thermo_flag = True` and `smallnet_flag = small_network`
  (`False` in production). The planning probe's call trace (below) gives the same five,
  in order: 199, 425, 585, 1097, 1252. This matches the README §2 (b) table.

**F2 — the version string** (`ComputeTargets/BBNData.py:37–42`). New comment sentence, verbatim:

> "ri02" is run-integrity prompt 02: every solve_ivp result is checked, and a solve that did not
> succeed raises PRyMSolverFailureError (PRyM/PRyM_main.py).

**F3 — the PRyMordial boundary** (`ComputeTargets/BBNData.py`).

- **New, `:314–345`: `_run_PRyMordial(callbacks: NPCallbacks, small_network: bool) -> dict`.**
  - It calls `_configure_PRyMordial(small_network)` outside the `try`.
  - Inside the `try` there is only `PRyMmain.PRyMclass(callbacks.rho_NP, callbacks.P_NP,
    callbacks.drho_NP_dT).PRyMresults()`.
  - `except Exception as e` (`:336`) returns `_failure_payload(f"PRyMordial: {type(e).__name__}: {e}")`.
  - On success it returns `{"Yp_BBN", "DOverH", "He3OverH", "Li7OverH"}` from `res[4:8]`.
- **`compute_BBN_data`, `:514–520`.** The old block, at `HEAD~1`'s `BBNData.py:437–446`, is
  replaced by `abundances: dict = _run_PRyMordial(callbacks, small_network)` inside the same
  `WallclockTimer`, then `if abundances.get("failure", False): return abundances`. The success
  return (`:533–537`) reads the four abundances from `abundances` rather than from `res`. The old
  block was:
  ```python
          PRyMmain = _configure_PRyMordial(small_network)

          try:
              # run PRyMordial
              res = PRyMmain.PRyMclass(
                  callbacks.rho_NP, callbacks.P_NP, callbacks.drho_NP_dT
              ).PRyMresults()
          except (OverflowError, ValueError, ComputationFailureError) as e:
              return _failure_payload(f"PRyMordial: {type(e).__name__}: {e}")
  ```
- **Nothing else gained an `except`.** The only `except` clauses in `compute_BBN_data` are the
  unchanged `except ComputationFailureError` around `build_NP_callbacks` (`:510`) and none other.
  The only new one in the file is `:336`, inside the helper.
- **`compute_SM_baseline` does not use the helper.** A three-line comment at `:356–358` says it
  keeps raising.

**F4 — the finiteness guard** (`build_NP_callbacks`).

- **`:140–154`, the samples.** Before the monotonicity loop, the three arrays are checked in the
  order `log_T_MeV`, `density_ratio`, `pressure_ratio`. The first non-finite sample raises
  `ComputationFailureError("<name> is not finite: index <i> has <name>=<value> at T=<exp(log_T[i])> MeV [<task_label>]")`.
- **`:168–174`, the temperature.** A closure `_check_finite(T_in_MeV, callback)` raises
  `ComputationFailureError("<callback> was called with a non-finite T_in_MeV=<value> [<task_label>]")`
  if `not math.isfinite(T_in_MeV)`. It is called first in `rho_NP` (`:193`), `P_NP` (`:212`) and
  `drho_NP_dT` (`:231`), before the negative-T guard.
- **The docstring** gains a dated paragraph. The import line becomes
  `from math import exp, isfinite, log`.
- **For finite input no value changes.** The added code only raises; it does not touch a
  computed value.

**F5 — the version bump** (`config/version.py:31–33`). New comment sentence, verbatim:

> On 2026-09-30 (run-integrity prompt 02), 2026.4.0: from 2026.4.0 a failed PRyMordial solve is
> stored as a failure with its reason, not as a success, and PRyM_version is "bf24c3d+cham03+ri02".

**F6 — documents, additively.** `.documents/numerical-strategies.md` gains two dated notes. Nothing
existing was edited.
- §7.4: the two finiteness guards, and what a non-finite sample did before.
- §7.5: what is now checked, where the boundary is, and what becomes a failure row. The note says
  that the three-type `try/except` described in the paragraph above it is gone.

**Tests — new `ComputeTargets/tests/test_bbn_solver_failures.py`**, 5 methods, about 24 s. It
reuses `run_prym`, `ZERO` and `test_network_flag._SavedPRyMGlobals`.
- `_ForcedFailure(fail_at)` wraps `PRyM_main.solve_ivp` through `mock.patch.object`. The k-th
  call integrates the first 1 % of its span, with `t_eval` cut to it, and is then marked
  `status=-1, success=False`.
- (a) Full network, k = 1…5.
- (b) Small network, k = 4, 5.
- (c) The boundary: a forced k = 1 failure and a `RuntimeError` callback through
  `_run_PRyMordial`, and `compute_SM_baseline` raising.
- (d) The finiteness guard, with no solve.
- (e) `PRYM_VERSION`.
- The new class is checked by `type(e).__name__` and `type(e).__module__`, and the helper is
  imported inside (c)'s body. So on `HEAD~1` each test fails on its own assertion.

## Deviations from the prompt

### 1. A helper, `_check_solve_ivp`, rather than eight inline `if` blocks — IMPLEMENTATION CHOICE

The prompt asks for a check after each call that raises one class. **The alternatives:**
- eight inline `if not sol.success: raise ...` blocks, each building its own message;
- one module-level helper beside the class, called at each site.

**The pick:** the helper. It keeps each call site to two lines (marker and call), so an upgrade
re-applies eight identical two-line insertions plus one block. The message format then cannot
drift between sites. The helper is part of the check itself, not an addition to PRyMordial.

### 2. The stage names — IMPLEMENTATION CHOICE

The names are those of the README §2 (b) table, qualified where two sites share a stage:
- `thermodynamics (with NP)` / `(no NP)`;
- `mid-T` / `low-T nuclear network (small)` / `(full)`.

The exception also carries `.stage`, so a test reads the name rather than parsing it.

### 3. The helper's return shape and argument — IMPLEMENTATION CHOICE

- **The argument.** `_run_PRyMordial` takes an `NPCallbacks` rather than three positional
  callables. It is what `build_NP_callbacks` returns, and what `compute_BBN_data` already holds.
- **The return.** On success it returns a dict of the four abundances rather than PRyMordial's
  raw `res` array. `compute_BBN_data` can then tell the two outcomes apart by
  `abundances.get("failure")`, as every `_failure_payload` consumer does.
- **The leading underscore** matches `_configure_PRyMordial` and `_failure_payload`, which the
  tests also import.

### 4. Test (c) also checks that `compute_SM_baseline` still raises — IMPLEMENTATION CHOICE

The prompt requires the baseline to keep raising, and to say so in a comment. It asks for no
test. A third subtest in (c) forces k = 1 through `compute_SM_baseline(False)` and requires
`PRyMSolverFailureError`. It costs one short partial solve, and it guards the boundary from both
sides. Test (d) likewise also checks `+inf` and `-inf` T, and a NaN in `log_T_MeV`, beyond the
prompt's three cases, because "non-finite" includes them.

### 5. README §2 (c) / §6.2: `build_NP_callbacks` with a NaN ratio did not return — it raised `ValueError` — premise measured differently; no change to the work

- **What the README says.** Its "Now" for "`build_NP_callbacks` with one NaN in the density ratio"
  is "returns; PRyMordial then hangs (> 60 s)".
- **What the tree does.** On `f0de762` (= `HEAD~1`), `build_NP_callbacks` with a NaN in
  `density_ratio`, or an `inf` in `pressure_ratio`, raises
  `ValueError: Array must not contain infs or nans.` That comes from `make_interp_spline`, whose
  `check_finite` defaults to True (scratch command below; test (d) on `HEAD~1`).
  - `compute_BBN_data` catches only `ComputationFailureError` around that call (`HEAD~1` `:433`),
    so **the `ValueError` escaped the task, with no failure row**.
  - A NaN in `log_T_MeV` was caught, but reported as "T_Jordan is not strictly decreasing".
- **Where the hang comes from.** The measured hang (the planning probe's `nan` case) comes from a
  callback that *returns* NaN. Through `build_NP_callbacks` that happens when a callback is called
  with a NaN T (both guards pass it, and the spline returns NaN). It does not happen from a NaN
  sample.
- **What was done.** Exactly what the prompt asks. The sample guard now raises
  `ComputationFailureError` before the spline, so the case becomes a `"BBN callbacks: ..."`
  failure row rather than an escaped `ValueError`. The T guard closes the route that does hang.
  The §6.2 target is met as written.
- **Classification.** Not STRUCTURALLY REQUIRED: nothing had to be done differently. It is
  recorded because README rule 9 asks which of the README and the tree was right. The tree was.
  The issue's hang, `[00-a-nan-new-physics-sample-hangs-prymordial]`, is real, but its route is a
  NaN-valued callback, not a NaN sample.

## Verification performed

All commands from the repository root with `venv/bin/python`. "Ran" means I ran it and quote its
output.

**Suite counts.**

| Suite | Before (`f0de762`) | After |
|---|---|---|
| `CosmologyModels/tests` | 18, OK | 18, OK |
| `ComputeTargets/tests` | 30, OK | **35**, OK (+5: the new module) |
| `Datastore/tests` | 11, OK | 11, OK |

Commands: `PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`, ran, before
any edit and after all of them.

**The planning probe, ran on `f0de762` before any edit** (`prymordial_solver_probe.py`, 74 s). It
reproduces the README exactly:
- SM baseline: calls at 199, 425, 585, 1097, 1252, all status 0; Yp 0.2468872958, D/H x1e5
  2.462251065, 7Li/H x1e10 5.423441017.
- Line 1252 truncated: Yp 0.246887219, D/H x1e5 2.474578712, 7Li/H x1e10 5.425221518, no
  exception.
- NaN: 199 and 425 succeed, 585 entered with `t_span=[nan, 0.745]`, no return in 60 s.

**The new test on `HEAD~1`.** Ran `test_bbn_solver_failures.py` against the unmodified tree
(`f0de762`, the new file present, nothing else changed): `Ran 5 tests … FAILED (failures=17,
errors=3)`.
- **(a) fails**, for each k = 1…5, on `call k failed and nothing raised`: PRyMordial returned
  abundances. What it returned (test output on `f0de762`):

  | k | stage (as named after the patch) | Yp | D/H x1e5 | 7Li/H x1e10 |
  |---|---|---|---|---|
  | 1 | thermodynamics (with NP) | 0.2468850737 | 2.46223455 | 5.423452045 |
  | 2 | a(T) | 0.2462805861 | 2.502813822 | 5.250696069 |
  | 3 | high-T n <-> p | 0.3438202414 | 3.094335277 | 6.451801139 |
  | 4 | mid-T nuclear network (full) | 0.3614693663 | 3.232021737 | 6.573359118 |
  | **5** | **low-T nuclear network (full)** | **0.246887219** | **2.474578712** | **5.425221518** |

  k = 5 is the probe's truncated case to every printed digit (D/H 5.0e-3 off the baseline).
- **(b) fails** for k = 4 and 5 on the same assertion.
  - k = 4 returned Yp 0.3614615742, D/H x1e5 3.232265895, 7Li/H x1e10 6.622502359.
  - k = 5 returned Yp 0.2468818065, D/H x1e5 2.470205056, 7Li/H x1e10 5.488345916.
- **(c) errors** on `ImportError: cannot import name '_run_PRyMordial'`. The helper is new, so
  that is its only possible failure on `HEAD~1`. The breakage record for (c) is (a), together with
  the `except (OverflowError, ValueError, ComputationFailureError)` clause at `HEAD~1`'s
  `BBNData.py:445`, quoted under F3. A `RuntimeError` or a `PRyMSolverFailureError` is neither,
  so it would have escaped `compute_BBN_data`.
- **(d) fails.**
  - Density-ratio NaN and pressure-ratio inf: `ValueError: Array must not contain infs or nans.`,
    not `ComputationFailureError` (Deviation 5).
  - `log_T_MeV` NaN: the message names neither the array nor the index.
  - `rho_NP`, `P_NP` and `drho_NP_dT` at T = NaN and at T = −inf: `ComputationFailureError not
    raised`. On `f0de762` they returned NaN and 0.0 (scratch command below).
- **(e) fails**: `'bf24c3d+cham03' != 'bf24c3d+cham03+ri02'`.

**The new test after the change.** Ran it: `Ran 5 tests in 23.657s OK`. The stage names, as the
tests printed them:
- **production (a), k = 1…5:**
  - `thermodynamics (with NP)`, t reached 99311 of target 1e+07;
  - `a(T)`, t reached −6.82326 of target 2.30259;
  - `high-T n <-> p`, t reached 0.0147626 of target 0.744992;
  - `mid-T nuclear network (full)`, t reached 1.92629 of target 118.875;
  - `low-T nuclear network (full)`, t reached 13277.6 of target 1.31599e+06.

  All five are distinct; each message has `status=-1` and the forced message.
- **small network (b), k = 4, 5:**
  - `mid-T nuclear network (small)`, t reached 1.92629 of target 118.875;
  - `low-T nuclear network (small)`, t reached 13277.6 of target 1.31599e+06.
- **(c):**
  - The forced failure gave `PRyMordial: PRyMSolverFailureError: solve_ivp failed in stage
    'thermodynamics (with NP)': status=-1, message='forced failure (test_bbn_solver_failures)';
    t reached 99311 of target 1e+07`.
  - The `RuntimeError` callback gave `PRyMordial: RuntimeError: synthetic callback failure`.
  - `compute_SM_baseline` raised `PRyMSolverFailureError`.
- **(d):**
  - `density_ratio is not finite: index 17 has density_ratio=nan at T=0.0119378 MeV [test-finite]`;
  - `pressure_ratio is not finite: index 17 has pressure_ratio=inf at T=0.0119378 MeV [test-finite]`;
  - `log_T_MeV is not finite: index 17 has log_T_MeV=nan at T=nan MeV [test-finite]`.
  - Every callback raises for NaN, +inf and −inf, and still returns 0.0 at T = −1.

**README §6.2, row by row.**

| Row | Target | Measured |
|---|---|---|
| each of the five production calls forced to fail | raises the new class, naming the stage | **met**: (a), five distinct stage names above; fails on `HEAD~1` (k = 5 there: D/H x1e5 2.474578712) |
| the two small-network calls | raises, naming the stage | **met**: (b); fails on `HEAD~1` |
| line 239 | patched, not run | **met**: check at `:275–276`. `grep -c "= solve_ivp(" PRyM/PRyM_main.py` = 8 and `grep -c "^ *_check_solve_ivp(sol_" PRyM/PRyM_main.py` = 8, each check directly after its call (table above) |
| a forced failure through the helper | failure payload, `"PRyMordial: "`, class and stage | **met**: (c) |
| a callback raising `RuntimeError` inside PRyMordial | failure payload naming the type | **met**: (c); the old `except` is quoted under F3 |
| an exception raised outside the helper | propagates | **met**, by reading the diff: the one new `except` is `BBNData.py:336`, inside `_run_PRyMordial`, around the `PRyMclass(...).PRyMresults()` call only |
| `build_NP_callbacks` with one NaN in the density ratio | `ComputationFailureError` before any solve, naming index and T | **met**: (d). On `HEAD~1` it raised `ValueError` (Deviation 5), not "returns" |
| a callback called with T = NaN | `ComputationFailureError` | **met**: (d); on `HEAD~1` it returned NaN |
| every pinned abundance in `ComputeTargets/tests/` | pass unchanged, not re-pinned | **met**: see below |
| `PRYM_VERSION` | `"bf24c3d+cham03+ri02"` | **met**: `BBNData.py:42`; (e) |
| `VERSION_LABEL` | `"2026.4.0"`, in `config/version.py` only, with a dated sentence | **met**: `grep -rn "VERSION_LABEL =" --include='*.py' .`, outside `venv/`, `thirdparty/` and `claude-context/`, finds one definition, `config/version.py:33: VERSION_LABEL = "2026.4.0"` |
| every patched line in `PRyM/` | marked | **met**: all ten insertions (class, helper, eight sites) carry `ChamPBH run-integrity prompt 02` |

**No successful solve moved.** The `ComputeTargets` suite passes with no pin touched: no file
under `ComputeTargets/tests/` other than the new module is in the diff. The printed values after
the change:
- `test_bbn_callbacks (i)`: baseline Yp_BBN 0.2468872958, DOverH 2.462251065, He3OverH
  1.042050273, Li7OverH 5.423441017. The first, second and fourth are the probe's `f0de762`
  baseline to every digit.
- `test_bbn_callbacks (h)`: Yp 0.2540933067, D/H x1e5 2.671263588. These are the figures recorded
  in `prompts/production-readiness/logs/02-wire-the-network-flag.md`.
- `test_network_flag (b)`:
  - small: Yp 0.2540780344, D/H x1e5 2.670892604, 7Li/H x1e10 5.1424297;
  - full: Yp 0.2540937879, D/H x1e5 2.671499971, 7Li/H x1e10 5.091224307.

  Both are identical to every digit to the same test's output on `f0de762`, run before any edit.

**`black --check`**: clean on `ComputeTargets/BBNData.py`,
`ComputeTargets/tests/test_bbn_solver_failures.py` and `config/version.py`. `PRyM/` was not
formatted.

**Scratch command, `f0de762`, before any edit** (inline `python -c`, not kept). It built callbacks
from 40 knots of r = 0.08, s = r/3, with ρ_SM = T⁴, and printed:
- a NaN at index 17 of `density_ratio`, or an inf in `pressure_ratio`:
  `ValueError Array must not contain infs or nans.`;
- a NaN in `log_T`: `ComputationFailureError T_Jordan is not strictly decreasing: …`;
- each callback: NaN at T = NaN, and 0.0 at T = −inf;
- **a grid of 0 samples:** `IndexError index 0 is out of bounds for axis 0 with size 0`;
- **a grid of 1 or 3 samples:** `ValueError The number of derivatives at boundaries does not
  match`;
- a grid of 4 samples: accepted.

**Not run.** No pipeline run, no Ray, no datastore. That a real stored history can produce a
failing solve or a non-finite ratio is not shown here. The tests force the failures.

## Observations not acted on

1. **A short BBN sample grid escapes `compute_BBN_data` with no failure row** (scratch command on
   `f0de762`; unchanged by this prompt).
   - `build_NP_callbacks` raises `IndexError` for an empty grid, and `ValueError` from
     `make_interp_spline` for 1–3 samples.
   - Both are outside the `except ComputationFailureError` at `BBNData.py:510`. Both are outside
     the PRyMordial boundary by the user's decision, so they propagate.
   - This happens when a model has fewer than four samples in [1e-4 keV, 100 MeV], a data
     condition rather than a code bug. The `T_Jordan_stop` pre-check does not exclude it.
   - **Next step:** a length check in `build_NP_callbacks` that raises `ComputationFailureError`.
     Out of scope: it changes which inputs become failure rows. Opened as
     `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]`.
2. **The callbacks do not check their own values for finiteness** (reasoned from the code; not
   run).
   - After this prompt a callback can still return NaN for a finite T in the domain only if
     `rho_SM_MeV4` or `drho_SM_dT_MeV3` does, that is if the EOS's `G_rho` or `dG_rho_dlogT` is
     non-finite there.
   - The spline of finite samples is finite. No measurement shows the EOS doing so.
   - If it did, the NaN would reach PRyMordial, and on the planning probe's evidence could hang
     it. This is the only NaN route found other than those this prompt closes.
   - **Next step:** raise `ComputationFailureError` on a non-finite return value. The prompt says
     not to change the callbacks' values, and this is a new guard it did not ask for. Opened as
     `[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]`.
3. **`t reached` for the two `t_eval` stages.** For thermodynamics and a(T), `solve_ivp` is
   called with `t_eval`, so `sol.t[-1]` is the last requested output point reached, not the exact
   point where the integration stopped. The difference is at most one sampling interval. Only the
   message's figure is affected, not the check. Not an issue.
4. **The §7.2–7.4 text still describes the removed asinh interface**, an existing issue:
   `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`, on the
   `review-remediation` board. This prompt's §7.4 note is additive and does not fix it.

## State handed to the next prompt

- **Labels.** `VERSION_LABEL = "2026.4.0"` (`config/version.py:33`). `PRYM_VERSION =
  "bf24c3d+cham03+ri02"` (`ComputeTargets/BBNData.py:42`). Prompt 03 lands under both unchanged.
  **Every store made before 2026.4.0 is invalid.**
- **The exception class.** `PRyM.PRyM_main.PRyMSolverFailureError(stage, status, solver_message,
  t_reached, t_target)`, raised by `PRyM.PRyM_main._check_solve_ivp(sol, stage, t_target)` after
  every one of the eight `solve_ivp` calls.
- **The stage names**, production then small network: `thermodynamics (with NP)`, `a(T)`,
  `high-T n <-> p`, `mid-T nuclear network (full)`, `low-T nuclear network (full)`;
  `mid-T nuclear network (small)`, `low-T nuclear network (small)`. The one not run is
  `thermodynamics (no NP)`.
- **The boundary.** `ComputeTargets.BBNData._run_PRyMordial(callbacks: NPCallbacks,
  small_network: bool) -> dict`. It returns `{"Yp_BBN", "DOverH", "He3OverH", "Li7OverH"}`, or
  `_failure_payload("PRyMordial: <Type>: <message>")` for any `Exception` inside the
  `PRyMclass(...).PRyMresults()` call.
  - `compute_BBN_data` returns that payload unchanged.
  - Its other failure reasons are unchanged: `"pre-check: ..."` and `"BBN callbacks: ..."`. A
    non-finite sample now arrives as `"BBN callbacks: <array> is not finite: index <i> ..."`.
  - `compute_SM_baseline` still raises.
- **What prompt 03 can rely on.** A stored `BBNData` failure row is now a real failure, with its
  reason, not a silent truncation. An exception inside PRyMordial no longer escapes the Ray task.
  Exceptions from ChamPBH's own code outside the call still do. Examples: a failed
  `ScalarModel`'s `values` (`RuntimeError`), and the short-grid case above.
- **Reproduce.**
  - `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_bbn_solver_failures`
    takes about 24 s.
  - **The planning probe selects its truncated call by line number (1252), which the patch
    moved to 1291.** Ran on the patched tree (`sm truncated`), both cases return the SM baseline,
    Yp 0.2468872958, D/H x1e5 2.462251065, 7Li/H x1e10 5.423441017, identical to `f0de762`:
    nothing is truncated. That is why the tests select by call order. Its `nan` case uses its
    own callback, not `build_NP_callbacks`, so it would still reach PRyMordial and hang; it
    measures PRyMordial, not the guard (not re-run).
- **Suites.** 18 / 35 / 11, all OK.
