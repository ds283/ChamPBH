# Log 01 — The mechanism and the low-T tolerance scan

**Prompt:** prompts/bbn-tolerance/01-mechanism-and-tolerance-scan.md
**Commit:** the commit that adds this file ("Add bbn_from_store and measure PRyMordial's low-T failures"); its SHA is in `git log`
**Model:** Claude Opus 5.5
**Date:** 2026-10-03
**Result:** BLOCKED — the prompt's §6 and §8 stop: **no setting in the grid meets P3**. Everything
the prompt asks to measure is measured; the recommendation is withheld and the user rules. Two
further findings bear on the ruling: the failures are caused by a defective PRyMordial rate
(Li8(p,d)Li7 near 1 keV), which no tolerance in the grid removes, and P3's SM criterion is
measured against a default that is itself 1.35×10⁻³ off in D/H.

All runs are on `95da274` (the branch head when the prompt started; nothing landed on the
branch while it ran) with this prompt's uncommitted tool, test and probes. `PRyM/` is untouched.
The tool's CSV `commit` column reads `95da274`: untracked files do not make it "dirty".

## What shipped

**`tools/bbn_from_store.py` (new).** A port of `source/bbn_from_store.py`, as README §2 (b) asks.
Public names:

- `connect_ro(path) -> sqlite3.Connection`: `file:PATH?mode=ro`, URI mode. Every connection the
  tool opens comes from here.
- `find_model(stem, beta, M_Mp, phi_init_Mp=5.0, units=None) -> StoredHistory`. This is the
  source's join. It matches M by `value_eV / (units.PlanckMass / units.eV)` to 1e-3. It reads the
  samples in production's order (`order by redshift.z desc`) and returns the one BBNData row.
  It raises `LookupError` on no match or on more than one.
- `StoredHistory(shard, serial, RHS_evaluations, rows, bbn, first_bounce_log_T_Jordan)`.
- `ratio_grid(rows, units, T_BBN_keV_spline_min=0.2, T_BBN_MeV_spline_max=100.0) -> (log_T_MeV, r)`.
  The arithmetic is the factory's read path and then `compute_BBN_data`'s loop, operation for
  operation and in the same order.
- `callback_domain_MeV(units, ...) -> (T_min_MeV, T_max_MeV)`, formed as `compute_BBN_data` forms
  them.
- `make_callback(kind, logT, r, rho_SM, T_min_MeV, T_max_MeV, label)`, with `kind` one of
  `prod`, `pertE`, `linear`, `pchip`. This is the source's callback builder.
- `stage_tolerance_override(settings, methods=None, keep_sol=(), record=None)`. A context manager.
  `settings` is `{stage: (rtol, atol)}` and `methods` is `{stage: OdeSolver subclass}`. It yields
  a list of `StageCall` records (`stage`, `t_span`, `y0`, the `kwargs` SciPy received, `status`,
  `success`, `t_reached`, and `sol` for the stages named in `keep_sol`).
- `lowT_tolerance_override(rtol=None, atol=None, methods=None, keep_sol=(), record=None)`. This is
  the same override applied to `low-T nuclear network (full)` and `(small)` only.
- `STAGE_*`, `LOW_T_STAGES`, `ALL_STAGES`: PRyMordial's eight stage names. `SPECIES`: the order of
  the Y vector.
- `NAMED_ATOL = {"reported": (...)}`: the S2 vector, below. `resolve_atol(text)`.
- `solve_history_variant(kind, logT, r, rho_SM, T_min_MeV, T_max_MeV, label, small_network=False, wall_clock_limit=600.0, rtol=None, atol=None) -> (outcome, calls)`.
  It goes through `_run_PRyMordial`.
- `solve_SM_baseline(small_network=False, rtol=None, atol=None) -> (outcome, calls)`. It goes
  through `compute_SM_baseline`, and a raise becomes a FAILURE outcome.
- `CSV_FIELDS`. `main(argv)` is the command line of README §2 (b), plus `--tag`.

**`ComputeTargets/tests/test_bbn_from_store.py` (new).** Three tests:

- (a) `ratio_grid` is the production grid, bitwise. No solve.
- (b) the override touches the low-T call only. Two small-network solves.
- (c) `find_model` reads a temporary two-shard imitation store read-only. No solve.

**`prompts/bbn-tolerance/logs/01-probes/` (new).**

- Scripts: `run_jobs.py` (the parallel driver), `compare_reproduction.py`, `mechanism.py`,
  `final_abundances.py`, `summarize_scan.py`, `cost.py`, `yp_floor.py`, `summarize_yp_floor.py`,
  `pinned_values_now.py`, `pinned_reference_7b518c9.py`, `compare_brief_rtol1e-6.py`.
- Data: `reproduce.csv`, `scan.csv` (363 rows: S1 245, S2 98, S3 20), `cost.csv`, `yp_floor.csv`.
- Printed outputs: `reproduce_compare.txt`, `scan_summary.txt`, `cost_output.txt`,
  `yp_floor_summary.txt`, `pinned_values.txt`, `final_abundances_rtol1e-6.txt`, and
  `mechanism_*.txt` (four histories).

`run_jobs.py` also wrote each invocation's stdout to `out/`. That directory was deleted, because
the CSVs hold every value it printed.

**`VERSION_LABEL`** is `"2026.6.0"` and **`PRYM_VERSION`** is `"bf24c3d+ri02+sr01"`. Neither
changed: no production file and nothing in `PRyM/` changed.

## Deviations from the prompt

### 1. No recommendation: no setting meets P3 — STRUCTURALLY REQUIRED (the prompt's own stop)

The prompt (§6) asks for the largest setting that meets P3, or a stop. No setting in S1 or S2
meets P3; the table is under Verification, "The recommendation". Criterion 2 fails at every
setting. Criterion 3 fails at every setting that converges. Criterion 1 fails at 1e-6 and at both
S2 settings. Criterion 4 fails at 1e-6 and 1e-8. This is a stop for the user (README §4, the
prompt's §8).

Two rows of §6.1 depend on "the recommended setting": Yp's floor and the P7 pinned values. Both
were measured at a **provisional** setting, low-T `rtol = 1e-5` with the atol unchanged. That is
the largest rtol in the grid that:

- meets criteria 1 and 4;
- produced no failure in S1;
- brings the D/H spread below 1e-4 on 15 of the 16 histories, the 16th (β = 2, M = 10⁻⁵, at
  1.22e-4) being within 1.2× of the floor of 1.04e-4 that it keeps at every tighter setting.

It is labelled provisional wherever it appears. If the user rules another setting, the two
scripts take `--rtol` and re-run in a few minutes.

### 2. The tool imports the `Datastore` package indirectly — STRUCTURALLY REQUIRED

README §2 (b) says the tool "never imports the `Datastore` package". It needs
`build_rho_NP_callback`, `thermodynamic_rho_SM`, `_run_PRyMordial` and `compute_SM_baseline`.
All four live in `ComputeTargets/BBNData.py`, which runs `from Datastore import DatastoreObject`
at import, as the source script's import did. The import opens nothing. The store is opened only
through `connect_ro`, and test (c) shows every connection `find_model` makes is a `mode=ro` URI.
The store files' mtimes and sizes are unchanged from before the first run to after the last (Verification).

### 3. How the override recognises the low-T call — IMPLEMENTATION CHOICE

Every Python-branch `solve_ivp` in `PRyM_main` passes its fun (and jac) through
`_limited(fn, stage, t_start, limit)` inside its own argument list. The override replaces
`PRyM_main._limited` with an observer that records `stage` and returns `_limited`'s own result
unchanged, so PRyMordial passes the same function objects as before. The `solve_ivp` that
follows consumes the stage name. A call that announces no stage passes through untouched, with
`stage = None`.

The alternatives considered:

- **`_check_wall_clock`.** It is also called with the stage just before each solve. But when a
  limit is set, `_limited`'s wrapper calls it on every RHS evaluation as well.
- **Call position**, as `test_bbn_solver_failures` uses. It depends on the flags.
- **The call's `atol`.** It would break once prompt 02 or a later patch changes a tolerance.

Recognising the stage by name is independent of position and of tolerance. It keeps working
after prompt 02 adds an `rtol`, and it gives the general `stage_tolerance_override` the prompt
wanted for §5.

That the observer is inert is shown three ways:

- every production outcome reproduces bitwise under an override that changes nothing (§4 below);
- test (b) shows every other call's arguments are identical;
- the 15 rtol-1e-6 rows of the brief, which were made with a patched copy of `PRyM/`, reproduce
  to every printed digit (Verification).

### 4. Old-tree runs recognise the low-T call by order — STRUCTURALLY REQUIRED

P7's pins have their provenance on `7b518c9`, which predates `_limited` (science-readiness
prompt 01 added it). `pinned_reference_7b518c9.py` therefore treats the second `method="BDF"`
call as the low-T call: mid-T is the first. It checks that call by its `atol` (1e-11 small,
1e-15 full), by its `y0` length (8 or 12), and by the absence of an `rtol`; any mismatch raises.
That tree was checked out in a temporary `git worktree` and removed afterwards.

### 5. `find_model` details — IMPLEMENTATION CHOICE

The changes from the source:

- **Sample order.** The source orders samples by `raw_N`. The tool uses production's order,
  `redshift.z desc`, which is what `sqla_ScalarModel_factory.build` uses.
- **Matches.** The tool requires exactly one match. The source took the first.
- **The M match.** The literal `2.436e27` is replaced by `units.PlanckMass / units.eV`, which is
  `2.4360000000000003e27`. **No match changes**: all 16 roster histories are found, and with a
  1e-3 tolerance a 1e-16 difference cannot move one.
- **The callback domain.** `T_min_MeV` is formed as `compute_BBN_data` forms it:
  `0.2 * units.keV / units.MeV = 0.00020000000000000004`. The source used `0.2e-3`. Only the
  callback's domain guard sees this value. PRyMordial's lowest query is 0.363 keV.

### 6. Extra names and options — IMPLEMENTATION CHOICE

The tool has the following beyond README §2 (b):

- `--tag`, a CSV label for the scan block;
- `callback_domain_MeV`, `solve_history_variant`, `solve_SM_baseline` and `StageCall`;
- the `methods` and `keep_sol` arguments of the override, which the instrumented solve needs.

The CLI is otherwise as specified.

### 7. Test (b) pins the call as passed in a named constant — IMPLEMENTATION CHOICE

The prompt says the low-T `rtol` is "absent, then 1e-6". The test asserts exactly that, through
`LOWT_SMALL_RTOL_AS_PASSED = None` and `LOWT_SMALL_ATOL_AS_PASSED = 1e-11`. A patch to the call
then re-pins one constant (P7's pattern) and leaves the test's logic alone. Prompt 02 will need
to do this, since its patch makes the small call pass an `rtol` (State handed on).

### 8. The S2 settings and vector, the Yp-floor histories, and the cost set — IMPLEMENTATION CHOICE

- **The S2 rtol values: 1e-5 and 1e-6.** These are the two "best" S1 values by P3's criteria.
  1e-5 meets 1 and 4 and misses 2 on one history. 1e-6 has the lowest median D/H spread
  (1.8e-5) and misses 1 on one solve. 1e-8 was not picked: it is 9–11× the default's cost, and
  it had a failure on a control.
- **The vector, `reported`.** The seven species that enter a reported abundance are p, d, t, He3,
  He4, Li7 and Be7. Each gets `atol = 1e-7 × Y_final`. Y_final is the SM baseline's abundance at
  the end of the full low-T stage at rtol 1e-6 (`final_abundances_rtol1e-6.txt`), to two figures.
  The fraction is a tenth of the smallest S2 rtol, so at the final value the absolute term is at
  most 10 % of the relative one: the reported abundances are rtol-controlled. The five species
  that enter no reported abundance are n, He6, Li8, Li6 and B8. They keep PRyMordial's 1e-15,
  because their final abundances (4e-16, −5e-17, 3e-47, 9e-15, −3e-18) are at or below it. The
  effective change is that Li7 is tightened to 2.2e-18 and Be7 to 3.9e-17, from 1e-15.
  The alternative was a looser floor for the trace species, which might have kept Li8 out of the
  Newton norm (see the mechanism). It was not chosen: a Li8 error of 1e-12 would move Li7, at
  2e-11, by about 5 %.
- **The Yp-floor histories.** The README's control and β = 1.6, M = 10⁻⁵, plus **β = 2.0,
  M = 10⁻⁵**. That is the history on which criterion 2 fails at every setting, so it is where a
  floor from another stage would show.
- **"Tightened"** means rtol 1e-9 and atol 1e-12 for thermodynamics, a(T) and high-T. PRyMordial
  passes 1e-6 and 1e-9 there. Mid-T gets rtol 1e-9 and atol 1e-15, the low-T stage's own floor,
  since mid-T's abundances run far below 1e-9. One configuration tightens all four at once.
- **The cost set.** Cost was measured for every S1 and S2 setting, not only the leading
  candidates. One process ran everything, after a discarded warm-up solve, round-robin over the
  settings.

### 9. The machine was not fully idle during the cost run — UNINTENDED DRIFT (environmental; kept)

The cost run was the only job of this prompt running, started after the scans had finished. The
1-minute load average was:

- 6.5 at the start and 8.2 at the end;
- 5.6–9.7 for most solves;
- a spike of 31.8 during rep 1 of the 1e-5 solves, and of 19–21 during reps 1 and 2 of the 1e-6
  solves.

Spotlight indexing, PyCharm and other users' processes, none of them this prompt's, explain the
spikes. Each setting's three repeats agree to within 3 % in most cases. The worst cases are 1e-5,
whose SM repeats were 15.8, 17.96 and 15.8 s, and 1e-6, whose control repeats were 27.7, 26.2 and
28.9 s. The **median** is what README §2 (d) asks for, and it is robust to one slow repeat. The
measurement was kept and not re-run.

## Verification performed

**Suites.** Run from the repository root with `PYTHONPATH=. ./venv/bin/python -m unittest
discover -s <pkg>/tests -t .`.

| package | before (`95da274`) | after |
|---|---|---|
| CosmologyModels | 18, OK | 18, OK |
| ComputeTargets | 103, OK | **106**, OK |
| Datastore | 31, OK | 31, OK |

ComputeTargets has the three new tests. `black --check` is clean on the tool, the test and every
probe.

**The store stayed read-only.** `stat -c "%Y %s"` on all 17 files of
`~/ChamPBH-stores/science-2026.6.0*` gave identical values before the first run and after the
last.

### §6.1 row by row

**1–3. The reproduction: 16 of 16 (the stand-in for a breakage test).**

- Run as `run_jobs.py reproduce` (eight at a time), compared by `compare_reproduction.py`; the
  output is in `reproduce_compare.txt`.
- **All five controls reproduce the stored Yp and D/H bitwise**, so to every printed digit:

  | control | Yp | D/H |
  |---|---|---|
  | β = 1.6, M = 10⁻³ | 0.246894839 | 2.461511946 |
  | β = 2, M = 10⁻⁵ | 0.2466738687 | 2.461868633 |
  | β = 2, M = 0.5 | 0.2492446765 | 2.559741879 |
  | β = 1.2, M = 10⁻³ | 0.2567164299 | 2.604772555 |
  | β = 1.05, M = 10⁻⁵ | 0.2837420802 | 3.598619165 |

- **All 11 failures fail in `low-T nuclear network (full)` at the stored `t reached` and
  target.** The tool's failure reason is character-for-character the stored one in all 11.

**4. `lowT_tolerance_override`.**

- Test (b) passes. The five stages are recognised in order, the low-T call's `rtol` goes from
  absent to 1e-6, and its `atol` is 1e-11 both times. Every other call's `t_span`, `y0` and
  non-callable keyword arguments are identical.
- The interception reproduces the brief's patched-`PRyM/` runs: **15 of 15 of the brief's
  rtol-1e-6 rows** (`source/lt_failure_diagnostics.csv`) match S1 at rtol 1e-6 to every printed
  digit of Yp and D/H (`compare_brief_rtol1e-6.py`). For example, β = 2.4, M = 10⁻⁵, prod gives
  2.464413146 in both, and the control's pert12 gives 2.461746674 in both.

**5. `ratio_grid`.**

- Test (a) passes. 64 of 80 synthetic samples are in the window, and the arrays are equal by `==`.
- Mutation check, run from a scratch snippet: multiplying the ratio by (1 + 2.2e-16), or moving
  ln T by one ulp, makes the test fail.
- The brief says forming ln T as `log_T_GeV + log(GeV/MeV)` moved the control's D/H by 3.7e-4.
  That form is **bitwise identical** to production's on all 5,219 of the control's rows, measured
  by a scratch snippet on the store, read-only. So the brief's earlier reconstruction must have
  differed somewhere else. The docstring is worded so as not to repeat the claim.

**6. The mechanism.** The instrument is `mechanism.py`. It is a SciPy `BDF` subclass passed
through `methods=`. It holds a statement-for-statement copy of `solve_bdf_system` that records
every Newton iteration, and it records every Jacobian refresh. The instrumented solve reproduces
production's failure exactly: same `t reached` and same reason. Two failures at the default
tolerance:

| | β = 1.6, M = 10⁻⁵ | β = 1.05, M = 0.01 |
|---|---|---|
| low-T span (s) | 118.9 → 1 315 661 | 129.3 → 1 083 964 |
| accepted steps / attempts | 208 / 300 | 221 / 311 |
| last accepted step | t = 1 282 986.354, h = 3083 s, order 1 | t = 1 074 372.236, h = 9592 s, order 2 |
| steps over the last 5 % | 3.9e4, 308, 308, 3083, 3083 s | 3.8e4, 9592, 9592, 9592 s |
| then | 45 attempts, h from 3.08e4 down to 3.5e-9 s | 43 attempts, h from 9592 down to 4.4e-9 s |
| why each attempt fails | **Newton non-convergence, every attempt**; no error-test rejection | the same |
| Newton contraction rate at k = 1 | 1.02, 1.00, 1.01, 2.75, …, then **1.073** for every h ≲ 1e-7 s | 1.04, 0.99, 1.98, …, then **1.089** |
| dominant component of \|dy\|/scale | Li7 at large h, **Li8** at small h | the same |
| Jacobian used | refreshed at t = 1 313 811.6 (T_γ 1.00070 keV): ∂f_Li8/∂Y_Li8 = **+1.61e15 s⁻¹** | refreshed at t = 1 083 964.4 (T_γ 1.00000 keV): **+2.29e15 s⁻¹** |
| Jacobian at the retried t | T_γ 1.01264 keV: **−1.46e14 s⁻¹** | T_γ 1.00444 keV: **−2.04e14 s⁻¹** |
| stiff-limit rate, \|1 − J_true/J_used\| | 1.091 (1.073 measured) | **1.089 (1.089 measured)** |
| negative abundances at the last steps | He6 −1.6e-24, Li8 −4.6e-32 | He6 −3.5e-23, Li8 −5.9e-17 |
| abundances at or below atol (1e-15) | n 4.6e-16 | n 8.3e-16, B8 8.6e-16 |
| is f smooth? | yes: repeatable, and (f(y + s·dy₀) − f(y))/s = J·dy₀ to every printed digit for s = 1 … 1e-6 | yes |
| T_of_t breakpoints in the collapsing steps | none; the nearest, 1 284 726.7, is 1.7e3 s away | none; the nearest are 1 070 794.6 and 1 090 105.5 |
| at t_fail, prod against pert12 at the same t | Yp 3.8e-5, **D/H 5.4e-4** | Yp 4.1e-7, **D/H 2.0e-3** |
| pert12 from t_fail to the end of the stage | Yp 9.2e-14, D/H 6.2e-9 | Yp 4.4e-14, D/H 2.2e-9 |

The last two rows say the abundances are **frozen in time to about 1e-8** by the failure time.
They are **not equal between the two inputs to 1e-6**: prod and pert12 differ at that time by
the solver's scatter, accumulated earlier in the stage.

**What the mechanism is.** The Jacobian's Li8 column has the pattern
∂f_p/∂Y_Li8 = ∂f_Li8/∂Y_Li8 = −∂f_d/∂Y_Li8 = −∂f_Li7/∂Y_Li8. That identifies the reaction
**Li8 + p → Li7 + d**, PRyMordial's `Li7dLi8p_bkwrd`, and it is defective near 1 keV:

- **The code.** The rate is defined at `PRyM/PRyM_nuclear_net63.py:1142–1147` (the analytic
  definition at `:886–899` is shadowed by the later one). It is α·exp(γ/T9)·spline(T9) with
  γ = 2.2274. The spline is `interp1d(kind="quadratic", fill_value="extrapolate")` (`:197`)
  through a 500-node table of the forward rate, T9 from 0.001 to 10.
- **The table and the spline.** Near T9 = 0.0116 the tabulated forward rate is 1e-264 to 3e-241.
  The global quadratic spline gives **−6.9e-55 to +3.9e-54** over T9 ∈ [0.0105, 0.015], and is
  negative on 51 % of that range.
- **The reverse rate.** Times exp(γ/T9) ≈ e¹⁹⁰, the reverse rate reaches |1.45e39| and changes
  sign between neighbouring temperatures. So ∂f_Li8/∂Y_Li8 is ±1e12–1e15 s⁻¹.
- **Upstream.** The code is upstream's: identical at `bf24c3d` on GitHub, and never patched by a
  ChamPBH campaign (`git log` on the file shows only "Import local version of PRyMordial").
- **Uniqueness.** A regex scan of `PRyM_init.py`'s (α, β, γ) triples finds no other splined rate
  with γ > 0.

The failure itself runs like this:

1. A step's first Newton attempt fails.
2. BDF refreshes the Jacobian at that attempt's t_new, 1e4–3e4 s ahead.
3. Then it only halves h, keeping that Jacobian.
4. In the stiff limit, c·|J_Li8| ≫ 1 at every h it can reach (even h = 3e-9 s), so the Newton
   rate tends to |1 − J_true/J_used|.
5. When the reverse rate at the two temperatures differs in sign or by orders of magnitude, that
   rate is ≥ 1, or so close to 1 that SciPy's convergence test fails at every step size, and h
   collapses to the spacing of floating-point numbers.

**Verdict.**

- **The mechanism is in the low-T stage.** It does not point at `T_of_t` or the ratio
  interpolant. f is smooth, no `T_of_t` breakpoint falls in the collapsing steps, and the
  production cubic is not involved. So the README §4 stop "the mechanism points elsewhere" is not
  met.
- **But it is not the tolerance as such.** It is a defective rate, expressed through the stiff
  solver. The default rtol's long steps near 1 keV (3e3–4e4 s) make it likely. Tightening rtol
  does not remove it: S1 fails once at 1e-6 and once at 1e-8 (on a control), S2 once at each
  setting, and the Yp-floor runs once at 1e-5.
- **Two of these were instrumented** (`mechanism_b1.1_M0.03_rtol1e-6.txt`,
  `mechanism_b1.2_M1e-3_rtol1e-8.txt`), and they are the same mechanism:
  - β = 1.1, M = 0.03 at rtol 1e-6 failed at t = 1 156 559.5. The refreshed Jacobian
    (T9 0.011897) has ∂f_Li8/∂Y_Li8 = +2.08e12 against +5.01e8 at the retried T9 0.012082. The
    rate is 0.9998, which fails SciPy's `rate^(4−k)/(1−rate)·‖dy‖ > tol`.
  - β = 1.2, M = 10⁻³ at rtol 1e-8 failed at t = 1 276 576.0: +2.00e13 against −3.77e14, a rate
    of 19.8.

**7. The scan.** Run as `run_jobs.py S1|S2|S3`, nine solves at a time (U1), into `scan.csv`;
summarised by `summarize_scan.py --detail` into `scan_summary.txt`. The machine was heavily
loaded during the scan: load average up to ~185, from Spotlight indexing on top of the nine
solves. Loaded wall times are not used for cost. A spread is (max − min)/median over prod,
pert12 and pert9; for a history with a failed variant no spread is computed. D/H is ×10⁵.

S1 and S2, full network, 16 histories × 3 variants plus the SM baseline (49 solves per setting):

| setting | failed solves | D/H spread: max (history) / median / histories ≥ 1e-4 | Yp spread: max / median | SM Yp; D/H | SM shift from default: Yp; D/H |
|---|---|---|---|---|---|
| default (rtol 1e-3) | **11** (the 11, prod) | 2.83e-3 (β 1.2, M 1e-3) / 1.45e-3 / 5 of the 5 computable | 3.76e-5 / 1.04e-5 | 0.2468872958; 2.462251065 | — |
| 1e-4 | 0 | 1.40e-4 (β 2.1, M 0.1) / 8.43e-5 / 5 | 4.86e-5 / 1.52e-5 | 0.2468863437; 2.45820648 | 3.9e-6; **1.64e-3** |
| 1e-5 | 0 | 1.22e-4 (β 2, M 1e-5) / 4.09e-5 / 1 | 4.42e-5 / 1.64e-5 | 0.2468867911; 2.458895152 | 2.0e-6; **1.36e-3** |
| 1e-6 | **1**: β 1.1, M 0.03, prod (t 1.15656e6 / 1.25375e6) | 1.06e-4 (β 2, M 1e-5) / 1.76e-5 / 1 (of 15) | 4.46e-5 / 1.49e-5 | 0.2468868672; 2.458947441 | 1.7e-6; **1.34e-3** |
| 1e-8 | **1**: β 1.2, M 1e-3 (a control), prod (t 1.27658e6 / 1.29417e6) | 1.04e-4 (β 2, M 1e-5) / 2.03e-5 / 1 (of 15) | 4.47e-5 / 1.72e-5 | 0.2468868777; 2.458917971 | 1.7e-6; **1.35e-3** |
| S2: 1e-5, `reported` | **1**: β 1.6, M 1e-3 (a control), pert12 (t 1.22806e6 / 1.31591e6) | 1.08e-4 (β 2, M 1e-5) / 4.14e-5 / 1 (of 15) | 4.47e-5 / 1.51e-5 | 0.246886764; 2.459049708 | 2.2e-6; **1.30e-3** |
| S2: 1e-6, `reported` | **1**: β 2.09, M 1e-5, pert9 (t 1.29132e6 / 1.30035e6) | 1.04e-4 (β 2, M 1e-5) / 1.65e-5 / 1 (of 15) | 4.47e-5 / 1.72e-5 | 0.2468868494; 2.458929347 | 1.8e-6; **1.35e-3** |

Every failure in the table is in `low-T nuclear network (full)`, with "Required step size is less
than spacing between numbers". Per-history values are in `scan_summary.txt`.

- **The SM baseline converges.** At rtol 1e-5, 1e-6 and 1e-8, D/H is 2.458895, 2.458947 and
  2.458918, which agree to 2.1e-5. **The default's 2.462251 is 1.35e-3 above them.** That is the
  error of the default tolerance on the baseline itself.
- **The D/H spread falls about 35× from the default**: median 1.45e-3, over the five controls
  only, to 4.1e-5 at 1e-5. At 1e-6 and 1e-8 every history but one lies between 1e-6 and 9e-5.
  β = 2, M = 10⁻⁵ keeps 1.04e-4 even at rtol 1e-8, so that floor is not the low-T stage's (see
  Yp's floor).
- **The Yp spread does not move with the low-T rtol**: a maximum of about 4.5e-5 and a median of
  1.5e-5 at every setting.
- **The `reported` vector is not materially better** than the scalar 1e-15 (P4). The spreads are
  the same within noise, the SM D/H moves by ≤ 6.3e-5, the cost is the same, and it had a
  failure at each rtol.

S3, small network, prod (SM plus β 1.6 M 1e-3, β 1.6 M 1e-5, β 2.4 M 1e-5; 4 solves per setting):

| low-T rtol | failed | SM Yp; D/H | SM shift from default: Yp; D/H |
|---|---|---|---|
| default | 0 | 0.2468818826; 2.457976999 | — |
| 1e-4 | 0 | 0.2468799639; 2.457652507 | 7.8e-6; 1.32e-4 |
| 1e-5 | 0 | 0.2468802684; 2.457881437 | 6.5e-6; 3.9e-5 |
| 1e-6 | 0 | 0.2468802117; 2.458287893 | 6.8e-6; 1.27e-4 |
| 1e-8 | 0 | 0.2468802314; 2.458223906 | 6.7e-6; 1.01e-4 |

The small network has no Li8 and never failed. Its baseline moves by at most 1.3e-4 in D/H.

**8. Cost** (`cost.py`; `cost_output.txt`, `cost.csv`). Run alone, serially, after every scan
job had finished. One process did one discarded warm-up, then three repeats round-robin. The load
is in deviation 9. Medians, in seconds:

| setting | SM median | ratio | control (β 1.6, M 1e-3, prod) median | ratio |
|---|---|---|---|---|
| default | 6.39 | 1.00 | 7.42 | 1.00 |
| 1e-4 | 8.20 | 1.28 | 10.14 | 1.37 |
| 1e-5 | 15.82 | **2.48** | 16.54 | **2.23** |
| 1e-6 | 26.26 | **4.11** | 27.73 | **3.74** |
| 1e-8 | 67.57 | 10.58 | 66.48 | 8.96 |
| 1e-5, `reported` | 15.59 | 2.44 | 16.43 | 2.22 |
| 1e-6, `reported` | 25.81 | 4.04 | 26.58 | 3.58 |

Every repeat gave identical abundances. The default control reproduced the stored Yp
(0.24689483901116643) bitwise in this process too. The brief's "49–57 s against 16–23 s", about
3×, was under unequal load; measured properly, **rtol 1e-6 costs 3.7–4.1×**.

**9. Yp's floor** (`yp_floor.py`, `summarize_yp_floor.py`; `yp_floor_summary.txt`). The low-T
setting is the provisional 1e-5 throughout.

| history | base | thermo | a(T) | high-T | mid-T | all four |
|---|---|---|---|---|---|---|
| β 1.6, M 1e-3: Yp / D/H spread | 2.10e-5 / 5.72e-5 | 1.40e-5 / 7.81e-5 | 1.58e-5 / 2.59e-5 | 1.24e-5 / 4.00e-5 | 1.63e-5 / 6.52e-5 | **4.6e-7** / 3.13e-5 |
| β 1.6, M 1e-5 | 1.53e-5 / 7.96e-5 | (pert12 failed in low-T) | 2.01e-5 / 7.95e-5 | 2.57e-6 / 8.53e-5 | 5.81e-6 / 8.50e-5 | **1.4e-7** / 3.90e-5 |
| β 2, M 1e-5 | 4.42e-5 / 1.22e-4 | 1.41e-5 / **2.37e-5** | 3.67e-5 / 9.90e-5 | 4.83e-5 / 4.38e-5 | 9.42e-6 / 8.35e-5 | **4.6e-7** / 3.72e-5 |

**Yp's floor is not set by one stage.** No single stage removes it consistently. Tightening all
four other stages together cuts the Yp spread by about 100×, from 1.5–4.4e-5 to 1.4–4.6e-7. The
D/H floor on β = 2, M = 10⁻⁵ (1.22e-4) falls to 2.4e-5 with the thermodynamic stage tightened
alone. So criterion 2's one persistent miss comes from the thermodynamic solve at rtol 1e-6, not
from the low-T stage.

Tightening a(T) also moves D/H **systematically**: on β = 1.6, M = 10⁻³ prod it goes from
2.461632 to 2.462733 (+4.5e-4), and to 2.462673 with all four. That is a bias of PRyMordial's
a(T) solve at its rtol 1e-6, larger than any residual spread. It is recorded as a measurement
(P9); see Observations.

**10. Upstream.**

- **Upstream does not pass `rtol` to the low-T calls.** Read from GitHub
  (`raw.githubusercontent.com/vallima/PRyMordial/bf24c3d064fe35ec2d612f2ccd8f03306d570b6a/PRyM/PRyM_main.py`,
  read without saving). The small call (`:563`) passes
  `method='BDF', jac=Jacobian, atol=1.e-11` and the full call (`:577`)
  `method='BDF', jac=Jacobian_LT, atol=1.e-15`. Every other call passes `rtol=1.e-6, atol=1.e-9`.
  The Julia branches set `abstol` only (`1.e-13`, `1.e-16`).
- **Upstream `main` has not changed this.** Its `PRyM_main.py` was last changed on 2023-07-31
  (`dbfd849`), before `bf24c3d` (2023-08-03, "Update README.md"), and has the same two lines.
- **The missing rtol and the defective rate are both upstream's.** The Li7(d,p)Li8 code (`:197`,
  `:1142–1147` of `PRyM_nuclear_net63.py`) is identical at `bf24c3d`. So is the B8 unpacking noted
  below.

**11. The pinned values (P7)**, at the provisional low-T rtol 1e-5, both networks
(`pinned_values.txt`). The constants' provenance is the "honly" route on `7b518c9`:

- `CONST_HONLY_SMALL_*`: the planner's `honly_constant_reference.py const-honly`;
- `CONST_HONLY_FULL_*`: the same with the full network;
- `BUILDER_CONST_HONLY_FULL_*`: that tree's `build_NP_callbacks` on `test_bbn_callbacks`' knots.

Re-deriving them therefore **needs the older tree**, which was checked out in a worktree. It
first reproduced all three pins with no override, to every printed digit, including ³He/H and
⁷Li/H as science-readiness log 01 quotes them. It was then re-run at 1e-5.

| constant (test, bound) | pinned | the test's quantity now, at 1e-5 | against the old pin | re-derived on 7b518c9 at 1e-5 | against the re-derived pin |
|---|---|---|---|---|---|
| `CONST_HONLY_SMALL_YP` (passenger (c), 1e-6) | 0.2536690816 | 0.2536695386 | 1.80e-6, **fails** | 0.2536695386 | 0 to printed digits, passes |
| `CONST_HONLY_SMALL_D_OVER_H_E5` (1e-6) | 2.6481673 | 2.648913973 | 2.82e-4, **fails** | 2.648913973 | 0, passes |
| `CONST_HONLY_FULL_YP` (network_flag (b), 1e-5) | 0.2536754614 | 0.2536731562 | 9.09e-6, passes | 0.2536731562 | 0, passes |
| `CONST_HONLY_FULL_D_OVER_H_E5` (1e-5) | 2.648809882 | 2.649990509 | 4.46e-4, **fails** | 2.649990509 | 0, passes |
| `BUILDER_CONST_HONLY_FULL_YP` (callbacks (h), 1e-6) | 0.2536761805 | 0.2536745605 | 6.39e-6, **fails** | 0.2536745605 | 0, passes |
| `BUILDER_CONST_HONLY_FULL_D_OVER_H_E5` (1e-6) | 2.648529359 | 2.649973638 | 5.45e-4, **fails** | 2.649973638 | 0, passes |
| `test_network_flag (b)` ⁷Li/H shift, small vs full (≥ 5e-3) | — | 1.044e-2 | passes | — | — |

So at 1e-5 every P7 constant re-pins from its provenance, and every unchanged bound passes. As
at the default (science-readiness log 01), the new tree gives the old tree's values to every
printed digit.

**Not in P7's list, and it will fail.** `test_bbn_callbacks (i)` compares `compute_SM_baseline`
with `README_BASELINE`, review-remediation README §2 (f) row 1 (quoted figures), at a bound of
1e-4. At 1e-5:

| abundance | value at 1e-5 | against `README_BASELINE` | at bound 1e-4 |
|---|---|---|---|
| D/H | 2.458895152 | 1.38e-3 | **fails** |
| ³He/H | 1.041751728 | 2.38e-4 | **fails** |
| ⁷Li/H | 5.428643941 | 1.04e-3 | **fails** |
| Yp | — | 1.3e-5 | passes |

The same holds at every converged setting, since the README row is the default's own value.
Re-pinning it is the user's decision (State handed on).

### The recommendation: none — no setting meets P3

| setting | 1. the 11 complete, all variants | 2. D/H spread < 1e-4 on all 16 | 3. SM moves ≤ 1e-3 in D/H, ≤ 1e-4 in Yp | 4. serial cost ≤ 3× |
|---|---|---|---|---|
| 1e-4 | yes (0 failures in 49) | **no**: 5 histories, max 1.40e-4 | **no**: D/H 1.64e-3 (Yp 3.9e-6) | yes: 1.28 / 1.37 |
| **1e-5** | yes (0 failures in 49) | **no**: 1 history (β 2, M 1e-5), 1.22e-4 | **no**: D/H 1.36e-3 (Yp 2.0e-6) | yes: 2.48 / 2.23 |
| 1e-6 | **no**: β 1.1, M 0.03, prod fails | **no**: 1.06e-4 (β 2, M 1e-5), and one history incomplete | **no**: D/H 1.34e-3 | **no**: 4.11 / 3.74 |
| 1e-8 | yes for the 11, but a control (β 1.2, M 1e-3) fails | **no**: 1.04e-4, and one incomplete | **no**: D/H 1.35e-3 | **no**: 10.6 / 9.0 |
| 1e-5, `reported` | yes for the 11, but a control (β 1.6, M 1e-3, pert12) fails | **no**: 1.08e-4 | **no**: D/H 1.30e-3 | yes: 2.44 / 2.22 |
| 1e-6, `reported` | **no**: β 2.09, M 1e-5, pert9 fails | **no**: 1.04e-4 | **no**: D/H 1.35e-3 | **no**: 4.04 / 3.58 |

**Stop** (prompt §6 and §8; README §4). The user rules. What each criterion's failure means:

- **Criterion 3 cannot be met by any converged setting.** The default's own SM D/H is 1.35e-3
  above the converged value. A setting that moved the baseline by ≤ 1e-3 would have to stay
  about as inaccurate as the default. The criterion measures distance from the default, not
  error.
- **Criterion 2 misses on one history at every rtol ≤ 1e-5**: β = 2, M = 10⁻⁵, at 1.04–1.22e-4.
  That is a floor set by PRyMordial's thermodynamic stage (item 9), not by the low-T stage.
- **Criterion 1 is not guaranteed by any tolerance.** The failures come from the Li8(p,d)Li7
  rate (item 6). Settings with 0 failures in 49 solves (1e-4, 1e-5) are not immune: at 1e-5 one
  failure appeared in the Yp-floor runs. Over all solves at rtol 1e-5 (scan, cost, Yp floor),
  1 of 109 failed.

**If the user waives 3** (the move is the correction of the default's error) **and measures 2
against the floor the tighter settings reach**, P3 selects **rtol 1e-5, atol unchanged**:

- 2.2–2.5× the default's cost;
- no failure in S1;
- D/H spread below 1e-4 on 15 histories, and 1.22e-4 on β = 2, M = 10⁻⁵, whose floor is 1.04e-4;
- the SM moves to D/H 2.458895 and Yp 0.2468868 (−1.36e-3, −2.0e-6).

It still leaves the rate defect in place. A fix for that would be a patch to a PRyMordial rate,
which P4 and README §4 put outside prompt 02 without the user's ruling.

## Observations not acted on

1. **PRyMordial's Li8(p,d)Li7 rate is spline ringing near 1 keV** (item 6). This is the cause of
   the 11 failures. Opened as **`[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`** on this
   board (§3). Possible fixes, all outside P4:
   - interpolate the table in log space, or below its significant range return 0;
   - clamp the reverse rate;
   - use the shadowed analytic forward rate at `:886–889`.
2. **`dYB8dtLT` unpacks Y in the superseded species order** (`PRyM/PRyM_nuclear_net63.py:1480`,
   upstream too). Every other low-T equation unpacks
   `…, Yn4p3 (Li7), Yn3p4 (Be7), Yn4p2 (He6), Yn5p3 (Li8), Yn3p3 (Li6), Yn3p5 (B8)`, with the old
   order commented out. The B8 equation has only the old order active, so in it
   He6 ← Y[Li7], Li6 ← Y[Be7], Li7 ← Y[He6] and Be7 ← Y[Li6]. B8's equation is wrong and does not
   match the Jacobian's B8 row. B8 stays below 1e-16 and enters no reported abundance, so the
   effect is probably negligible; it was **not measured**. Opened as
   **`[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]`** (low).
3. **PRyMordial's a(T) solve at rtol 1e-6 biases D/H by about +4.5e-4** on the control (item 9).
   This is a property of the third-party code's settings, recorded as a measurement for prompt
   03's addendum (P9). It is not opened as an issue.
4. **`test_bbn_callbacks (i)`'s `README_BASELINE` fails at any converged setting** (item 11).
   P7's list omits it. Handed on, not opened: it is part of the ruling the stop asks for.
5. **The brief's §2.1 claim about forming ln T does not reproduce** (item 5): the two forms agree
   bitwise on the control's rows. Noted; nothing depends on it.
6. **During the scan the machine carried load from Spotlight (`mds`, `mds_stores`), PyCharm and a
   Ray cluster idle since the night before** (`ray::IDLE_RestoreWorker`, not this prompt's). Not
   acted on.

## State handed to the next prompt

The next step is **the user's ruling**, not prompt 02. What the ruling needs:

- **The stop.** No setting meets P3; see the table above.
- **The provisional setting.** The one P3 would pick with criterion 3 waived and criterion 2
  measured against the floor is low-T rtol 1e-5, atol unchanged (1e-15 full, 1e-11 small).
- **The decisions.** Whether to waive or restate criteria 2 and 3. Whether to accept a tolerance
  that does not guarantee criterion 1, or to allow a PRyMordial rate patch (outside P4) for
  `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`. Whether to re-pin `README_BASELINE`.

If prompt 02 is dispatched at **rtol 1e-5**, its "reproduce log 01 to every printed digit" target
is `scan.csv`'s S1 rows with `lowT_rtol == 1e-05`:

- SM: Yp 0.2468867911, D/H 2.458895152, ³He/H 1.041751728, ⁷Li/H 5.428643941.
- Control β = 1.6, M = 10⁻³, prod: Yp 0.2468928612042104, D/H 2.461632361.
- The cost target: control median 16.54 s, SM 15.82 s.
- The P7 re-pins: the re-derived column of item 11.
- `test_bbn_from_store.LOWT_SMALL_RTOL_AS_PASSED` must become 1e-5. That is deviation 7's one
  constant.

At any other setting, re-run `pinned_values_now.py`, `pinned_reference_7b518c9.py` and `cost.py`
with that `--rtol`.

**The tool.** Run from the root:

```bash
./venv/bin/python tools/bbn_from_store.py ~/ChamPBH-stores/science-2026.6.0 --beta 1.6 --M-Mp 1e-3 \
    --variant prod pert12 pert9 [--lowT-rtol 1e-5] [--lowT-atol reported] [--csv PATH]
./venv/bin/python tools/bbn_from_store.py --sm-baseline [--lowT-rtol 1e-5]
```

**Reproduction commands.** Run from the root.

- The 16-history reproduction: `run_jobs.py reproduce`, then `compare_reproduction.py`.
- The scan: `run_jobs.py S1`; `run_jobs.py S2 --s2-rtol 1e-5 1e-6 --s2-atol reported`;
  `run_jobs.py S3`; then `summarize_scan.py --detail`.
- The mechanism: `mechanism.py BETA M [--rtol X]`.
- The cost: `cost.py default 1e-4 1e-5 1e-6 1e-8 1e-5:reported 1e-6:reported`, alone, on an idle
  machine.
- Yp's floor: `yp_floor.py BETA M CONFIG --lowT-rtol 1e-5`.
- P7: `pinned_values_now.py CASE --rtol X`, and in a `7b518c9` worktree
  `pinned_reference_7b518c9.py CASE --rtol X`.

All the scripts above are in `prompts/bbn-tolerance/logs/01-probes/`.

**The residual floors (P9), for prompt 03.**

- **Yp:** about 1.5–4.5e-5. It is set jointly by the four other stages, and falls to about 5e-7
  with all four tightened.
- **D/H:** a few 1e-5 on most histories at rtol ≤ 1e-5. β = 2, M = 10⁻⁵ has 1.0–1.2e-4, set by the
  thermodynamic stage.
- **The a(T) bias:** about +4.5e-4 in D/H.
- **The default's SM error:** 1.35e-3 in D/H.
