# Log — surfing-temperature 02: surfing turning points and the driver

**Subject:** `surfing_turning_points` beside `first_bounce`, the driver `tools/surfing_scan.py`,
their tests, and five β run to measure the cost.
**Commit:** this commit · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent
`f6440d1` · **Model:** Claude Opus 5.5 · **Result:** landed. T1–T5 done; the gate "Turning
points" holds. No stop condition fired. The scan is projected at 4.0 minutes against P7's 60.

## What shipped

- **`ComputeTargets/ScalarModel.py`** (129 lines added after `first_bounce`, none removed):
  - the namedtuple `SurfingCrossing(N, phi_Einstein, pi_Einstein, log_T_Jordan, direction)`;
  - `surfing_turning_points(result, coupling, interior_samples=8)`. Its brentq fallback and the
    crossing constructor are nested functions inside it, so nothing else is added to the module.
  - It is not called by `compute_scalar_model`. The payload, the store and `VERSION_LABEL` are
    unchanged.
- **`tools/surfing_scan.py`** (new):
  - `COLUMNS`, README §4's columns in order;
  - `beta_grid(beta_min, beta_max, beta_step)`, built by index;
  - `make_setup()`, the units and cosmology, built once per run;
  - `run_history(setup, beta, phi_init_Mp, M_Mp, T_stop_GeV)`, which returns a `HistoryOutcome`;
  - `summarise(crossings, bounce, kicks, roots, units)`, which is pure (T3);
  - `format_field`, `read_existing` (resume) and `main(argv)`.
- **`ComputeTargets/tests/test_surfing_turning_points.py`**: 9 tests on stub interpolants.
- **`tools/tests/test_surfing_scan.py`**: 15 tests, with one real history.

### T1: the walk, as built

- **g** is `1 + coupling.d_logOmega_dphi(phi) * pi`, read on each step's interpolant. φ and π
  come from the interpolant, so g is read through the coupling and not built from β.
- **The end.** `N_end = first_bounce(result).N`, or `ts[-1]` if there is no bounce. A step is
  walked if `ts[k] < N_end`, over `[ts[k], min(ts[k+1], N_end)]`. So the bounce's own step is
  walked up to the bounce, and a reflection (a node) ends the walk at the end of the step that
  ends there.
- **The samples.** Each step is read at `np.linspace(t_low, t_high, interior_samples + 2)`, in one
  call of the interpolant on the array.
  - A node is read once, from the step that ends there. A later step skips its first point.
  - The samples where g ≠ 0 form one sequence, and a change of sign between two consecutive ones
    is a crossing. Its bracket always lies on one step.
  - The crossing is refined by `brentq` at xtol 1e-15, on scalar calls of that step's
    interpolant. If the scalar calls at the two ends do not differ in sign (a zero at a node, seen
    through rounding), the end where |g| is smaller is taken.
  - A sample where g is exactly 0 is the crossing, if the signs before and after it differ. It is
    counted once, at a node as anywhere else.
- **Direction.** `"min"` where g goes from + to −, and `"max"` where it goes from − to +.
- **Interior samples: 8.**
  - SciPy's `RadauDenseOutput` has a `Q` of three columns, so each component is a cubic in N on a
    step (read in `scipy/integrate/_ivp/radau.py`, SciPy 1.17.0). With a constant d ln Ω/dφ, g is
    then a cubic, with at most three zeros on a step.
  - Eight interior points split a step into nine parts, so two zeros on one step are both found
    unless they are closer than a ninth of the step: 0.011 e-fold at the global cap of 0.1.
  - The crossings in the five T5 histories are at least 0.76 e-fold apart (β = 5.0, the wall's
    pair), and otherwise 2.6 e-folds or more. The wall crossings lie on steps of 1e-8 to 2e-7
    e-fold.
  - Eight is also the orchestrator's probe's count, so its crossings can be compared directly.
  - The cost is one vectorised interpolant call per step and ten coupling calls. It was not timed
    apart from the history: β = 2 took 0.77 s in all, with the walk.

### T2 and T3: the driver and the summary

- **Setup.** The constants `PHI_INIT_MP`, `PI_INIT`, `T_INIT_GEV` and `LAMBDA_EV`, and the private
  `_z_grid()`, are **imported** from `tools/history_and_bbn.py` (unchanged). Importing that module
  imports `ComputeTargets.BBNData`, but runs no PRyMordial.
- **The root check.** The tool refuses (exit 2) unless
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` is a file under the working directory. That is
  the file the EOS reads by a relative path. It does not check for `PRyMrates/`, since no BBN runs.
- **The capture.** `run_history` wraps `integrate_scalar_history` for the duration of
  `compute_scalar_model._function`, as `history_and_bbn.py:300-326` do, and restores it in a
  `finally`. It then calls `surfing_turning_points(result, coupling)`.
  - It returns `HistoryOutcome(failure_reason, crossings, bounce, accepted_steps, reflections,
    wall_s, result)`.
  - `bounce` is the payload's `first_bounce`, which is `first_bounce(result)`.
  - `wall_s` covers the history and the walk.
- **The kicks** are `find_kicks(cosmology, cosmology._eos._T_min * units.GeV,
  cosmology._eos._T_max * units.GeV)`, once per run. `reachability_roots(cosmology, beta, kicks)`
  is called once per β, and only when there is a `"min"` crossing.
- **`summarise`** returns the measured columns:
  - **capture** is the first `"min"`;
  - **largest** is the `"max"` of largest ln T_J;
  - **second dip** is the first `"min"` with N above the first `"max"`'s;
  - **delivery** is the bounce's T_J, N and `reflected`, filled whether or not the history was
    captured (brief §0);
  - **`captured`** is `len(crossings) > 0`, and **`n_crossings`** is `len(crossings)`;
  - **`capture_kick`** is the first kick, hot to cold, with `T_window_cold <= T_capture <=
    T_window_hot`;
  - **`T_hot_GeV`, `T_cold_GeV`** come from that kick's record, as `reachability_roots` gives
    them, the merged edge included (β = 2.5: `T_cold` = 25.67 MeV).
- **`kicks=None` is not supported** (the fallback is not needed, as 01 has landed). A caller with
  no windows passes `[]`. The capture kick and the roots are then blank, as the real-history test
  does.
- **The CSV.** It is written with `csv.writer`, `lineterminator="\n"`.
  - A float is written as `repr`. `wall_s` is `f"{wall:.3f}"`. An int is written as `str`.
  - A boolean is `true` or `false`. A blank is an empty field.
  - Each row is written and flushed as its history ends. `failure_reason` is quoted by the csv
    module if it holds a comma.
- **A failure row** has the inputs, `status=failure`, `failure_reason` and `wall_s`. Every other
  field is blank, including `captured`, `n_crossings`, `reflections` and `accepted_steps`.
- **Resume.**
  - **The header.** It must equal `COLUMNS`. Every row must have 24 fields.
  - **The inputs.** Each row's `phi_init_Mp`, `M_Mp`, `Lambda_eV` and `T_stop_GeV` must equal the
    options to a relative 1e-12. `repr` reads back exactly.
  - **Exit 2** otherwise, with the file untouched.
  - **Skips.** Each β within 1e-9 of a row's is skipped, and that includes failure rows.
- **The sidecar** is `<out>.meta.json`, `{"runs": [...]}`, with one entry per invocation. Each
  entry holds:
  - `start_time` (UTC), `git_head`, and `git_tracked_changes`, from `git status --porcelain
    --untracked-files=no`;
  - `version_label`, the options, `Lambda_eV`, the β grid, `resumed`, `skipped`, `python` and
    `scipy`.

  A fresh run (no CSV) writes a new sidecar, and a resumed run appends an entry.
- **Exit status:** 0 if every history ran and none failed; 1 if a failure row was written; 2 if
  the run was refused.
- **The console.** One line per β:
  - the status, the four temperatures in GeV (`-` for a blank), the crossing count and the wall
    time;
  - a flag, `[not captured]` or `[captured, but no bounce above T_stop]`.

  `compute_scalar_model`'s own printing is **left on the console**. On a successful history it
  prints nothing, and the cosmology prints its one `@@` line once per run.

## Deviations from the prompt

| # | Deviation | Tag |
|---|---|---|
| D1 | **T4's real history ends at T_stop = 0.5 GeV**, below its first bounce at 0.7467 GeV, rather than the default 1 MeV. The prompt asks for "a T_stop just below its first bounce", and the brief leaves the choice to the agent. The test takes 0.35 s. | IMPLEMENTATION CHOICE |
| D2 | **The constants and `_z_grid` are imported from `tools/history_and_bbn.py`**, a private name included, rather than restated. That keeps one definition. The prompt allows either. | IMPLEMENTATION CHOICE |
| D3 | **The root check tests for the EOS table**, not for `PRyMrates/`. The brief leaves this to the agent. | IMPLEMENTATION CHOICE |
| D4 | **`run_history` returns a `HistoryOutcome`**, holding the crossings, the bounce, the counts, the wall time and the `IntegrationResult`, rather than the raw payload. `main`'s tests replace it with a stub, so no integration runs. `wall_s` therefore includes the walk. | IMPLEMENTATION CHOICE |
| D5 | **`summarise` takes `kicks` as a list, not `None`** (the brief: the fallback is not needed). It takes `roots=None` when there is no capture. | IMPLEMENTATION CHOICE |
| D6 | **A failure row leaves `captured`, `n_crossings`, `reflections` and `accepted_steps` blank.** The prompt specifies only `status` and `failure_reason`. | IMPLEMENTATION CHOICE |
| D7 | **Exit status 1** when a failure row was written. The prompt does not say. | IMPLEMENTATION CHOICE |
| D8 | **The interpolant is read once per step on an array of the step's samples.** That needs the interpolant to accept an array, as SciPy's dense output does, and the test stubs do. `brentq` uses scalar calls. | IMPLEMENTATION CHOICE |
| D9 | **`surfing_turning_points` has a keyword `interior_samples` (default 8).** The prompt gives the signature `(result, coupling)`, which the default keeps. | IMPLEMENTATION CHOICE |
| D10 | **More tests than the prompt lists:** a crossing exactly at a node, counted once (g); a crossing on the bounce's own step (h); g read through the coupling (i); a reflection between crossings (f); a capture in a window whose kick has no interval (e); `format_field`; the T_stop and M mismatches; the header mismatch; the failure row; the grid options; the refusal outside the root. | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT.

## Verification performed

### Counts (serially, in the foreground; load from `uptime` at the start of each run)

| Suite | Before (`f6440d1`, code at `b5d1c94`) | After (final tree) |
|---|---|---|
| CosmologyModels | 18 OK, 68.8 s, load 18.1 | 18 OK, 73.3 s, load 11.9 |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 97.0 s, load 33.6 | 61 OK, 100.8 s, load 11.3 |
| ComputeTargets `test_[g-z]*.py` | 106 OK, 100.5 s, load 9.4 | **115 OK**, 105.5 s, load 9.7 |
| ObjectFactories | 69 OK, 4.9 s, load 6.6 | 69 OK, 5.2 s, load 7.7 |
| tools | 34 OK, 7.0 s, load 6.4 | **49 OK**, 8.7 s, load 7.1 |

- ComputeTargets is 167 before and 176 after: 167 plus the 9 new tests. tools is 34 plus 15. No
  count fell, and no failure needed a re-run.
- The commands are `CLAUDE.md`'s, ComputeTargets in its two halves. Each run's output went to a
  file in the scratch directory, and the verdict was grepped from it.
- An earlier "after" run, made before `surfing_turning_points` was restructured (its helper and
  constant moved inside it; D9), gave the same counts, all OK.

`black --check` on the four Python files: `4 files would be left unchanged`.

### `compute_scalar_model` is unchanged (§2.3)

```
$ git diff --stat ComputeTargets/ScalarModel.py
 ComputeTargets/ScalarModel.py | 129 +++++++++++++++++++++
$ git diff -U0 ComputeTargets/ScalarModel.py | grep '^@@'
@@ -858,0 +859,129 @@ def first_bounce(result: IntegrationResult) -> Optional[FirstBounce]:
$ git diff ComputeTargets/ScalarModel.py | grep -c '^-[^-]'
0
```

The diff is one hunk of pure additions, after `first_bounce`. It adds `SurfingCrossing` and
`surfing_turning_points` and nothing else at module level. `tools/history_and_bbn.py`,
`extract_common.py` and `config/version.py` are not in the diff.

### The new tests, by class

Command for each module: `PYTHONPATH=. ./venv/bin/python -m unittest discover -s <root>/tests -t . -p '<module>.py'`.

**`ComputeTargets/tests/test_surfing_turning_points.py`**: 9 tests, 0.03 s (load 6.7), OK. No
integration. The stub result has `solution.ts`, `solution.interpolants` and `reflections`. The
stub coupling has d ln Ω/dφ = D = 2, and π = (g − 1)/D.

| Class | Tests | What it pins |
|---|---|---|
| `TestSurfingTurningPoints` | 9 (a–i) | **(a)** the damped stub: the four zeros before N_b = 11π/3, at (j + ½)π, to 1e-12, in order, min/max/min/max, with the state read at the crossing; **(b)** none of the zeros after N_b reported; **(c)** two zeros, 1.03 and 1.07, inside the step [1.0, 1.1], with g > 0 at both ends; **(d)** g = 0.5, an empty list; **(e)** a reflection at 1.0, before any zero, an empty list; **(f)** a reflection at 3.0, only the first crossing; **(g)** g exactly 0 at the node 2.5, counted once; **(h)** a `"max"` at 1.05 on the bounce's own step, the bounce at 1.07; **(i)** with half the coupling, no crossing |

**`tools/tests/test_surfing_scan.py`**: 15 tests, 1.6 s (load 6.7), OK.

| Class | Tests | What it pins |
|---|---|---|
| `TestSummarise` | 5 (a–e) | not captured (delivery kept); min only (delivery blank, no bounce); min-max with the wall before a second dip; min-max-min-max (second dip the second min, largest the later, larger max, EW kick and its roots); a capture in a window with no interval (roots blank) |
| `TestBetaGrid` | 2 | 1.10–5.00 by 0.05: 79 values, equal to `float("1.10")`, … with no drift; `format_field` (blank, `true`/`false`, `repr`) |
| `TestResume` | 7 (a–g) | through `main(argv)` in a `tempfile` directory with `run_history` stubbed: **(a)** two rows written and read back, the fields as written, then a third β run alone, with the sidecar's two entries; **(b)** φ* mismatch, exit 2, the file byte-identical; **(c)** T_stop and M mismatches, exit 2; **(d)** a foreign header, exit 2; **(e)** a failure row, exit 1, not retried; **(f)** the `--beta-min/max/step` grid; **(g)** run outside the root, exit 2, nothing written |
| `TestRealHistory` | 1 | β = 2, φ* = 5, M = 1e-3, T* to T_stop = 0.5 GeV, through `run_history`, 0.35 s at load 7. The wrapper is restored; there is at least one crossing; each crossing is before the bounce, with \|1 + 2π/M_P\| < 1e-8 on the interpolant; ln T_J at N ± 10⁻³ lies above a `"min"` and below a `"max"`; delivery is `first_bounce(result).log_T_Jordan` exactly |

### The β = 2 history (T4's one), scratch, T_stop = 1 MeV

This is `run_history` run once from a scratch script, to compare with the orchestrator's probe. It
ran on the code before D9's restructuring, which changes no arithmetic.

```
wall 0.772 steps 2083 reflections 0
min N=14.166305971050 T_J=0.657065099 GeV step=195 h=0.1 |g|=2.22e-16
max N=18.947923557953 T_J=0.749921226 GeV step=243 h=0.1 |g|=0.00e+00
bounce N=20.352082230017 T_J=0.746686275 reflected=False same_as_payload=True
```

These are the probe's values to every digit it printed: 14.166305971, 18.947923558, 20.352082230,
0.6570651, 0.7499212 and 0.7466863 GeV.

### T5: five β (P6)

**The command**, run once from the repository root, in the foreground, at load 2.97 (start) and
2.96 (end):

```
./venv/bin/python <scratch>/t5/run_t5.py <scratch>/t5/diag.txt \
    --beta-values 1.15 1.5 2.5 3.5 5.0 --out <scratch>/t5/surfing_t5.csv
```

- `run_t5.py` calls `tools.surfing_scan.main(argv)`. It wraps `run_history` so that, after each
  history and outside its timed region, it records the step, the step length and |g| of every
  crossing, and the largest number of crossings on one step.
- Exit 0, in 15.3 s in all. The five histories took 13.45 s, and the rest is imports, the
  cosmology and `find_kicks`.
- The CSV is `<scratch>/t5/surfing_t5.csv`, where `<scratch>` is
  `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/1fbd6ca7-3081-43f0-a7ee-21c884b88092/scratchpad/agent02`.
  It is not committed.

T in GeV:

| β | wall (s) | steps | n | kick | T_capture | T_max | T_2nd dip | T_deliver | bounce above T_stop | T_hot | T_cold |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.15 | 0.391 | 875 | 2 | QCD | 0.188582 | 0.204043 | — | 0.204042 | yes (turning point) | 0.211540 | 0.160756 |
| 1.5 | 0.472 | 1231 | 2 | QCD | 0.321103 | 0.361394 | — | 0.361394 | yes (turning point) | 0.356980 | 0.130272 |
| 2.5 | 1.102 | 3214 | 2 | QCD | 1.13511 | 1.38035 | — | 1.34692 | yes (turning point) | 1.35033 | 0.0256680 (merged with mu-pi) |
| 3.5 | 3.591 | 11059 | 4 | EW | 77.2670 | 100.716 | 97.1995 | 97.6214 | yes (turning point) | 97.6300 | 29.9907 |
| 5.0 | 7.895 | 24349 | 10 | EW | 136.868 | 187.847 | 173.352 | 175.991 | yes (turning point) | 175.991 | 18.4309 |

- **Failures and reflections.** No history failed, and no history reflected: every first bounce
  is a turning point.
- **The stops.** Every first bounce lies above T_stop, so §4's stop for a captured history with
  no bounce did not fire. Each step holds at most one crossing, so §4's stop for two crossings in
  one step did not fire.
- **The invariant.** Every `n_crossings` is even.
- **Residuals.** |g| at the crossings is ≤ 2.2e-16, except at the wall crossings: 6.1e-10
  (β = 1.15), 7.9e-12 (1.5) and 2.5e-14 (5.0).
- **The wall crossings.** At β = 1.15, 1.5 and 5.0 the last `"max"` lies 3.9e-7, 9.3e-7 and
  4.5e-6 e-folds before the bounce, on steps of 1.2e-8, 3.9e-8 and 2.2e-7.
- **Against the probe.** Every value agrees with the orchestrator's probe to the digits it gives
  (brief §0), the merged `T_cold` at β = 2.5 included. The wall times are within 0.6 s of the
  probe's, and the step counts are identical.

### The projection (P7)

- **The method.** The five wall times were interpolated piecewise-linearly in β, and extrapolated
  linearly to 1.10 from the first segment (0.379 s there). The interpolant was summed over the 79
  points β = 1.10 + 0.05k.
- **The total.** It is **242.5 s (4.0 min)**, or 228.5 s if interpolated linearly in ln wall. A
  run adds about 2 s of setup. P7's budget is 60 minutes, so §4's budget stop did not fire.
- **The longest single history** is β = 5.0, at **7.9 s**, the top of the grid. The wall time
  rises with β throughout the five. One invocation over all 79 β (about 4 minutes) fits under the
  ten-minute Bash cap.

The probe's 254 s for the whole grid agrees with this.

### The CSV, for 03

The header, verbatim:

```
beta,phi_init_Mp,M_Mp,Lambda_eV,T_stop_GeV,status,failure_reason,captured,capture_kick,T_capture_GeV,N_capture,T_max_GeV,N_max,T_second_dip_GeV,N_second_dip,T_deliver_GeV,N_deliver,deliver_reflected,n_crossings,reflections,accepted_steps,wall_s,T_hot_GeV,T_cold_GeV
```

The β = 2.5 row, as written:

```
2.5,5.0,0.001,0.001,0.001,ok,,true,QCD,1.1351110528562538,14.252534136009288,1.3803503113324511,18.403417110348094,,,1.3469230444732636,22.229095015806934,false,2,0,3214,1.102,1.3503286786260909,0.025668038233152272
```

## The deliberate-breakage record

- **How the diffs were made.** Each is against the final tree, and the file it touches is
  byte-identical in this commit. Each was made by `diff -u` with `a/` and `b/` labels, checked with
  `git apply --check` and `git apply -R --check`, and replayed in `bash`: applied, both modules
  run, reversed. The two files' `shasum` was compared before and after the replay.
- **The commands:**
  `PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_surfing_turning_points.py'`
  and
  `PYTHONPATH=. ./venv/bin/python -m unittest discover -s tools/tests -t . -p 'test_surfing_scan.py'`.
- None was committed.

**1. The direction of a crossing inverted.** 5 of 9 and 1 of 15 fail:
`test_a_every_crossing_in_order_with_direction`, `test_c_two_zeros_inside_one_step`,
`test_f_reflection_between_crossings`, `test_g_zero_at_a_step_boundary_counted_once`,
`test_h_crossing_on_the_bounce_step`; `TestRealHistory.test_beta_2`.
```diff
--- a/ComputeTargets/ScalarModel.py
+++ b/ComputeTargets/ScalarModel.py
@@ -970,7 +970,7 @@
 
             sign: int = 1 if g_j > 0.0 else -1
             if last_sign != 0 and sign != last_sign:
-                direction: str = "min" if last_sign > 0 else "max"
+                direction: str = "max" if last_sign > 0 else "min"
                 if zero_at is not None:
                     crossings.append(crossing_at(zero_at[0], zero_at[1], direction))
                 else:
```

**2. The walk not stopped at the first bounce.** 5 of 9 fail:
`test_a_every_crossing_in_order_with_direction`, `test_b_none_after_the_first_bounce`,
`test_e_reflection_before_any_crossing`, `test_f_reflection_between_crossings`,
`test_i_read_through_the_coupling`. The tools module passes: β = 2 to 0.5 GeV has no crossing
after its bounce.
```diff
--- a/ComputeTargets/ScalarModel.py
+++ b/ComputeTargets/ScalarModel.py
@@ -913,7 +913,7 @@
     interpolants = result.solution.interpolants
 
     bounce: Optional[FirstBounce] = first_bounce(result)
-    N_end: float = bounce.N if bounce is not None else float(ts[-1])
+    N_end: float = float(ts[-1])
 
     def refine(g, a: float, b: float) -> float:
         g_a: float = g(a)
```

**3. The interior samples removed.** 1 of 9 fails: `test_c_two_zeros_inside_one_step`.
```diff
--- a/ComputeTargets/ScalarModel.py
+++ b/ComputeTargets/ScalarModel.py
@@ -953,7 +953,7 @@
             y = interpolant(t)
             return 1.0 + coupling.d_logOmega_dphi(float(y[0])) * float(y[1])
 
-        Ns = np.linspace(t_low, t_high, interior_samples + 2)
+        Ns = np.linspace(t_low, t_high, 2)
         ys = interpolant(Ns)
         gs = [
             1.0 + coupling.d_logOmega_dphi(float(phi)) * float(pi_)
```

**4. `summarise` taking the first `"min"` as the second dip.** 3 of 15 fail:
`TestResume.test_a_write_then_skip`, `TestSummarise.test_c_min_max_then_the_wall`,
`TestSummarise.test_d_min_max_min_max`.
```diff
--- a/tools/surfing_scan.py
+++ b/tools/surfing_scan.py
@@ -346,7 +346,7 @@
         row["N_max"] = largest.N
 
         N_first_max = maxima[0].N
-        later = [c for c in minima if c.N > N_first_max]
+        later = minima
         if later:
             row["T_second_dip_GeV"] = T_GeV(later[0].log_T_Jordan)
             row["N_second_dip"] = later[0].N
```

**5. Resume ignoring a φ* mismatch.** 1 of 15 fails: `TestResume.test_b_phi_init_mismatch_refused`.
```diff
--- a/tools/surfing_scan.py
+++ b/tools/surfing_scan.py
@@ -152,7 +152,7 @@
 
 # the columns a resumed run must agree with the options on, and the relative tolerance (the file
 # holds repr of each value, which reads back exactly)
-_INPUT_MATCH = ("phi_init_Mp", "M_Mp", "Lambda_eV", "T_stop_GeV")
+_INPUT_MATCH = ("M_Mp", "Lambda_eV", "T_stop_GeV")
 _INPUT_RTOL = 1e-12
 
 # what one history returns to main
```

## Observations not acted on

- **T5 and the β = 2 scratch run were made on the code before D9's restructuring.** The
  brentq fallback was then a module-level helper, and the sample count a module constant
  `SURFING_INTERIOR_SAMPLES = 8`. Both were moved inside the function so that `ScalarModel.py`
  gains only the namedtuple and the function. No arithmetic changed.
  - The final code ran the stub tests (to 1e-12) and the real-history test.
  - The histories were not re-run, to stay within the prompt's five plus one.
- **T5's sidecar records `git_tracked_changes: true` at `f6440d1`.** It was run before this
  commit, with these changes in the working tree. 04's run, made on a committed tree, will record
  `false`.
- **At β = 1.15 and 1.5 `T_max` equals `T_deliver` to about six digits.** The only `"max"` is the
  wall's (brief §0). That is U1 applied as written, and 03's figure will show the two markers
  coinciding.
- **`wall_s` includes the walk** (D4), which was not timed separately. 04's verification should
  say so if it quotes per-history times.
- **`cosmology._eos._T_min` and `_T_max` are private** (board §5.2). The tool reads them as 01's
  test does. Nothing to do here.
- No scratch file was written outside the agent's scratch directory. No issue opened, and the
  index is untouched.

## State handed to the next prompt

- **The branch.** `surfing-temperature`, HEAD at this commit, on top of `f6440d1`. Nothing is
  staged or modified after it. The untracked entries are the user's, as before.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 49.
- **In `ComputeTargets/ScalarModel.py`:** `SurfingCrossing(N, phi_Einstein, pi_Einstein,
  log_T_Jordan, direction)` and `surfing_turning_points(result, coupling, interior_samples=8)`.
  `direction` is `"min"` or `"max"`. The function is not called by production.
- **In `tools/surfing_scan.py`:** `main(argv)`, `run_history`, `summarise`, `beta_grid`,
  `COLUMNS` and `HistoryOutcome`. For the full grid:
  `./venv/bin/python tools/surfing_scan.py --beta-min 1.10 --beta-max 5.00 --beta-step 0.05 --out PATH.csv`
  (φ* = 5, M = 1e-3 and T_stop = 1e-3 GeV are the defaults). Run it from the repository root.
  - Exit 0 if every history ran and none failed, 1 if a failure row was written, 2 if the run was
    refused.
  - Resume by re-running the same command. A β already in the file is skipped.
- **The CSV header**, verbatim:
  `beta,phi_init_Mp,M_Mp,Lambda_eV,T_stop_GeV,status,failure_reason,captured,capture_kick,T_capture_GeV,N_capture,T_max_GeV,N_max,T_second_dip_GeV,N_second_dip,T_deliver_GeV,N_deliver,deliver_reflected,n_crossings,reflections,accepted_steps,wall_s,T_hot_GeV,T_cold_GeV`
- **The fields.**
  - **A blank** is an empty field (`,,`).
  - **A boolean** (`captured`, `deliver_reflected`) is `true` or `false`, in lower case.
  - **A float** is Python's `repr`. `wall_s` is `%.3f`, and is the only field that depends on
    the run. `n_crossings`, `reflections` and `accepted_steps` are integers.
  - **`status`** is `ok` or `failure`. A failure row has the inputs, `status`, `failure_reason`
    and `wall_s`, and every other field blank.
- **What each field means.**
  - **An uncaptured row** has `captured=false` and `n_crossings=0`, but keeps `T_deliver_GeV`,
    `N_deliver` and `deliver_reflected`.
  - **A missing second dip** shows as `captured=true` with `T_second_dip_GeV` blank; at β ≤ 2.5 in
    T5, `n_crossings=2`.
  - **The kick.** `capture_kick` is `EW`, `QCD`, `mu-pi` or `ee`. `T_hot_GeV` and `T_cold_GeV` are
    that kick's roots, the merged interval's edge for QCD above β = 2.154.
- **T5's scratch CSV:**
  `/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/1fbd6ca7-3081-43f0-a7ee-21c884b88092/scratchpad/agent02/t5/surfing_t5.csv`
  (five rows, β = 1.15, 1.5, 2.5, 3.5, 5.0), with its sidecar beside it. It is not committed.
- **The cost.** The scan is projected at 243 s (4.0 min) for all 79 β, plus about 2 s of setup.
  The longest history is β = 5.0, at 7.9 s. One invocation fits under the ten-minute cap.
- **No issue opened;** `.documents/OPEN_ISSUES.md` is untouched.
- **The breakage diffs** are in this log, and replay from a clean tree at this commit.
