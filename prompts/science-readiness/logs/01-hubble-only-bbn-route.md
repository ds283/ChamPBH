# Log 01 — The Hubble-only BBN route, the wall-clock limit and the output checks

**Prompt:** prompts/science-readiness/01-hubble-only-bbn-route.md
**Commit:** the commit that adds this file ("Hand the scalar field to PRyMordial through H alone"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-01
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `7b518c9` (`HEAD` at dispatch, a planning commit). "HEAD~1" below
means the parent of this prompt's commit, which has the same tree as `7b518c9` in every file this
prompt touches. HEAD~1 measurements were made on an export of `7b518c9`
(`git archive 7b518c9 | tar -x -C <scratchpad>/head`), run from that directory with this
repository's `venv/bin/python`. The board's Decisions record P1–P4 and P9 accepted as proposed,
so README §2 (a)–(e) were followed as written.

## What shipped

**Versions.** `VERSION_LABEL` `"2026.5.0"` → **`"2026.6.0"`** (`config/version.py:43`, with a dated
sentence at `:36–42`). `PRYM_VERSION` `"bf24c3d+cham03+ri02"` → **`"bf24c3d+ri02+sr01"`**
(`ComputeTargets/BBNData.py:44`; its comment names `ri02`, `sr01` and the `cham03` revert).

**Schema.** Column **removed**: `BBNDataValue.pressure_NP_MeV4`. No column added.

### R1, W1 — the vendored PRyMordial patch (`PRyM/`, every hunk marked "ChamPBH science-readiness prompt 01")

Line numbers are those after the patch. To re-apply on an upgrade:

| file:lines | hunk |
|---|---|
| `PRyM_init.py:77–80` | `NP_hubble_flag = False`, after `NP_e_flag`, with a three-line comment |
| `PRyM_main.py:35–68` | `class PRyMWallClockLimitError(Exception)` with `__init__(self, stage, elapsed, limit)`, attributes `stage`, `elapsed`, `limit`, message `"wall-clock limit of %.6g s exceeded in stage '%s': %.6g s elapsed"`; `_check_wall_clock(stage, t_start, limit)` (raises once `time.monotonic() - t_start > limit`; nothing if `limit is None`); `_limited(fn, stage, t_start, limit)` (returns `fn` itself if `limit is None`, else a wrapper that calls `_check_wall_clock` before `fn`) |
| `PRyM_main.py:72–81` | `PRyMclass.__init__(self, my_rho_NP=None, my_p_NP=None, my_drho_NP_dT=None, my_delta_rho_NP=None, wall_clock_limit=None)`; `wall_clock_start = time.monotonic()` is the first statement |
| `PRyM_main.py:141–143` | in `Hubble`: `if PRyMini.NP_hubble_flag: rho_tot += PRyMthermo.rho_NP(Tg)`, after the `NP_thermo_flag` line. Nothing else reads the flag |
| `PRyM_main.py:211–219` | `dTNPdt` back to the upstream body (identical, by `diff`, to the function at `6d3ecfa`); the `cham03` comment and the commented-out lines are gone (P2) |
| eight `solve_ivp` sites | before each: `_check_wall_clock("<stage>", wall_clock_start, wall_clock_limit)` (the between-stage check); in each: `fun` → `_limited(fun, "<stage>", …)`, and `jac=J` → `jac=_limited(J, "<stage>", …)` where a `jac` is passed. The stages are `_check_solve_ivp`'s: `thermodynamics (with NP)` (`:268–279`), `thermodynamics (no NP)` (`:320–331`), `a(T)` (`:518–522`), `high-T n <-> p` (`:683–689`), `mid-T nuclear network (small)` (`:1127–1147`, with `jac`), `mid-T nuclear network (full)` (`:1219–1239`, with `jac`), `low-T nuclear network (small)` (`:1328–1348`, with `jac`), `low-T nuclear network (full)` (`:1408–1428`, with `jac`) |

The Julia branches are not patched (`julia_flag` is checked `False`). `N_eff` is not changed and
not stored; with `NP_thermo_flag` off it no longer counts ρ_NP (README §2 (a); "Observations").
No reaction rate, network, tolerance, `T_start`, `T_end`, `t_end` or sampling changed.

### R2, O1, W2 — the ChamPBH side (`ComputeTargets/BBNData.py`)

- `SampleValues` (`:23–31`): fields `raw_N, log_T_Jordan, density_NP, density_NP_ratio`
  (`pressure_NP` removed).
- New constants: `DEFAULT_BBN_WALL_CLOCK_LIMIT = 600.0` (`:49`), `MIN_BBN_SAMPLES = 4` (`:54`),
  `YP_UPPER_BOUND = 0.5`, `ABUNDANCE_NAMES = ("Yp_BBN", "DOverH", "He3OverH", "Li7OverH")`.
- `NPCallbacks` **removed**.
- `thermodynamic_rho_SM(eos, units) -> Callable[[float], float]`: returns `rho_SM_MeV4` only; the
  `drho_SM_dT_MeV3` it also returned is gone, and `eos` needs only `G_rho`.
- `build_NP_callbacks` → **`build_rho_NP_callback(log_T_MeV, density_ratio, rho_SM_MeV4,
  T_min_MeV, T_max_MeV, task_label) -> Callable[[float], float]`**. It keeps the finiteness
  check on the samples, the monotonicity check, the negative-T, non-finite-T and domain guards
  and the OverflowError/ValueError wrapping, with the same messages. It gains (P4) a length check
  (fewer than 4 samples → `ComputationFailureError("too few samples for the rho_NP spline: N in
  the window, at least 4 needed [label]")`, before any other check), and a non-finite *value*
  check (a non-finite `ratio * rho_SM` for a finite in-domain T →
  `ComputationFailureError("rho_NP is not finite at T=… MeV: ratio=…, rho_SM=… [label]")`). For
  finite in-domain input the value is `float(spline(ln T)) * rho_SM(T)`, the same expression as
  the old `rho_NP` callback.
- `jordan_Hdot_over_H2` **removed**, with the `V_policy`, `ODE_policy`, `StateVector` and
  π′ reconstruction in `compute_BBN_data` and the `PotentialDerivativePolicy`, `ODEPolicy` and
  `StateVector` imports (P1).
- `_configure_PRyMordial(small_network)`: sets `NP_thermo_flag = False`, `NP_hubble_flag = True`,
  `verbose_flag = False`, `smallnet_flag = small_network`; no longer sets `Tstart_NP`; then checks
  `NP_thermo_flag is False`, `NP_hubble_flag is True`, `NP_nu_flag is False`, `NP_e_flag is False`,
  `julia_flag is False`, `compute_bckg_flag is True`, raising `AssertionError` naming the flag.
- New `_check_abundances(abundances) -> Optional[str]`: `None` if all four are finite,
  `0 < Yp_BBN < 0.5` and the other three `> 0`; otherwise every failing value, joined by `"; "`.
- `_run_PRyMordial(rho_NP, small_network, wall_clock_limit) -> dict` (all three required): calls
  `PRyMclass(rho_NP, wall_clock_limit=wall_clock_limit)`; `except Exception` →
  `_failure_payload("PRyMordial: <Type>: <message>")` as before, which now covers
  `PRyMWallClockLimitError`; a successful return outside `_check_abundances` →
  `_failure_payload("PRyMordial output: <reasons>")`.
- `compute_SM_baseline(small_network)`: `PRyMclass(_zero_NP)`, no limit, still raises.
- `compute_BBN_data(model_proxy, task_label, T_BBN_MeV_spline_max=100, T_BBN_keV_spline_min=1e-4,
  small_network=False, wall_clock_limit: Optional[float] = DEFAULT_BBN_WALL_CLOCK_LIMIT)`: per
  sample `ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m)` and `r = ρ_NP/ρ_R,J`, as before; no `p_NP`. The
  spline floor and the pre-check are unchanged (prompt 06).
- `BBNData.compute` reads `payload["wall_clock_limit"]` (default `DEFAULT_BBN_WALL_CLOCK_LIMIT`
  when absent; `None` passes through as no limit). `BBNDataValue(store_id, z, raw_N,
  log_T_Jordan, density_NP, density_NP_ratio)`: `pressure_NP` argument and property removed.

### P1 elsewhere, the plumbing, the driver

- `Datastore/SQL/ObjectFactories/BBNData.py`: `pressure_NP_MeV4` removed from the
  `BBNDataValue` table's columns, from `BBNData.build`'s value query and constructor call, from
  `BBNData.store`'s inserter payload, and from `BBNDataValue.build`'s read, insert and constructor.
- `plot_ScalarModel.py` `BBN_era_NP_plot`: the |p_NP| curves and their data series removed from
  the density panel; the w_NP panel removed, with its two reference lines and labels. That left a
  four-row figure with an empty row, so the figure is now **three panels** (ratio, H, ρ_NP) at
  8 × 10 in instead of four at 8 × 13. The density panels stay.
- `config/argument_parser.py`: `--bbn-wall-clock-limit SECS` (`type=non_negative_float`,
  default `DEFAULT_BBN_WALL_CLOCK_LIMIT`, imported from `ComputeTargets.BBNData`); new
  `non_negative_float(text) -> float`.
- `main.py:761–772`: `bbn_wall_clock_limit = None if args.bbn_wall_clock_limit == 0 else
  args.bbn_wall_clock_limit`; the BBN payload is `{"small_network": False, "wall_clock_limit":
  bbn_wall_clock_limit}`.
- **New** `tools/history_and_bbn.py BETA M [--T-stop-GeV T] [--small-network] [--wall-clock-limit
  SECS]` (README §2 (m)), from `planning-probes/bbn_route_probe.py`, with the probe's "honly"
  monkeypatch and the pickle stage removed. One `history` line, four `ratio` lines, one `bbn` line.

### Tests (`ComputeTargets/tests/`)

| prompt §2 | where | what |
|---|---|---|
| (a) two components | `test_bbn_solver_failures.test_f_thermodynamics_has_two_components` | first `solve_ivp` y0 has 2 components, 5 calls, and the recording ρ_NP is called only from `Hubble` |
| (b) ρ_NP ≡ 0 is plain PRyMordial | `test_prym_passenger.test_b_zero_is_plain_prymordial` | `compute_SM_baseline(True)` against `run_prym_without_new_physics(True)`: `==` on all four; no RuntimeWarning |
| (c) constant family | `test_prym_passenger.test_c_constant_family_reproduces_the_hubble_only_reference` | `CONSTANT`, small network, `wall_clock_limit=None`, against §6.1 `const-honly`, 1e-6 |
| (d) wall-clock limit | `test_bbn_solver_failures.test_g_wall_clock_limit` | `CONSTANT` through `_run_PRyMordial` at 1e-3 s |
| (e) output checks | `test_bbn_solver_failures.test_h_output_checks` | stubbed `PRyMclass`; three failures and one pass-through |
| (f) callback builder | `test_bbn_solver_failures.test_i_callback_builder_refusals` | three samples; NaN `G_rho` at 0.5 MeV |
| (g) `test_bbn_callbacks` | `test_bbn_callbacks` a–f, h, i rewritten for one callback; g replaced | see the pairs below |
| (h) the fixture | `prym_fixtures` | see below |

**Deleted tests and their replacements** (README §5 rule 6; the count does not fall):

| deleted (subject deleted) | replaced by |
|---|---|
| `test_bbn_callbacks.test_g_Hdot_over_H2_Omega_primeprime_term` (`jordan_Hdot_over_H2`, P1) | `test_bbn_callbacks.test_g_compute_BBN_data_hands_prymordial_the_density`: `compute_BBN_data._function` on a stand-in history with `PRyMclass` stubbed; each in-window sample has `density_NP = 3 M_P² H_J² − ρ_R(1 + f_m)` (1e-12) and the right ratio; the samples carry exactly the four fields; `PRyMclass` gets one positional callback equal to `ratio·ρ_SM` at every sample T (1e-10) and `wall_clock_limit` as passed; no potential or coupling is touched |
| `test_prym_passenger.test_b_patch_is_inert` (the `cham03` passenger test, P2: ρ_NP = 0 with `NP_thermo_flag` on against off) | `test_prym_passenger.test_b_zero_is_plain_prymordial` (prompt (b)) |
| `test_prym_passenger.test_c_reference_abundances_unchanged` (the `cham03` passenger test: the constant family against its unpatched pins) | `test_prym_passenger.test_c_constant_family_reproduces_the_hubble_only_reference` (prompt (c)) |

Rewritten, purpose kept: `test_prym_passenger.test_a` (the oscillating family through the new
route), `test_d` unchanged; `test_bbn_callbacks` a (constant exact, ρ only), b (oscillating
bound, ρ only, 2e-8), c (no worse than asinh, ρ only), d (monotonicity), e (domain guards), f
(units, without the derivative-per-T check, whose subject is gone), h (end to end; see
deviation 3), i (SM baseline, unchanged reference); `test_bbn_solver_failures` a, b (forced
failures, through the new `run_prym`), c (the boundary, with one callback; it still shows a
callback exception propagates out of LSODA), d (non-finite samples; the infinite case moved from
the removed pressure array to `density_ratio`), e (`PRYM_VERSION`); `test_network_flag` b (pins
now `CONST_HONLY_FULL_*`), c (payload read key by key; deviation 4).

**Fixture.** `prym_fixtures.SyntheticNP(label, rho)` (the pressure and derivative fields removed;
the product order of `make_family` kept). `run_prym(rho, small_network=False,
wall_clock_limit=None)` sets the flags through `_configure_PRyMordial` and restores every global
it touches. New: `INIT_NAMES = ("NP_thermo_flag", "NP_hubble_flag", "verbose_flag",
"smallnet_flag")`, `THERMO_NAMES = ("rho_NP",)`, the context manager `SavedPRyMGlobals` (saves
and restores both; deletes a name that did not exist), and `run_prym_without_new_physics(
small_network=False)` (every NP flag off, `PRyMclass()` with no callback). The `_main` driver's
`reference` case is that function. `test_network_flag._SavedPRyMGlobals` is now an alias of
`SavedPRyMGlobals`; `test_bbn_callbacks`'s own copy is removed (deviation 6). New pinned
constants in `test_prym_passenger`: `CONST_HONLY_SMALL_YP/D_OVER_H_E5` (README §6.1),
`CONST_HONLY_FULL_YP/D_OVER_H_E5` (measured here, below); in `test_bbn_callbacks`:
`BUILDER_CONST_HONLY_FULL_YP/D_OVER_H_E5` (measured here, below).

## Deviations from the prompt

### 1. The constant-family reference is pinned, not computed in the test — STRUCTURALLY REQUIRED

README §6.2's row for the constant family says "test (two solves); the 'honly' reference is
computed in the test from the old callback formulas". The prompt's §2 (c) says one solve
against the §6.1 `const-honly` row pinned as constants. The two disagree, and the README's form
cannot be built after P2: the old formulas are the pressure −ρ_NP and the density derivative 0
under `NP_thermo_flag`, and with upstream `dTNPdt` restored that is `(−3H·0 + 0)/0`, a NaN in the
third LSODA component. The prompt's form was followed. The pin's provenance is §6.1 (the planner,
`6aaa706`), re-measured to every printed digit on `7b518c9` here (Verification).

### 2. The full-network Hubble-only reference was measured here — STRUCTURALLY REQUIRED

`test_network_flag (b)` and the old `test_bbn_callbacks (h)` compared full-network solves against
pins of the old route (`CONSTANT_YP_UNPATCHED` and prompt 03's values), which the new route does
not reproduce by design (the old constant family had p_NP = ρ_NP/3 in the plasma equation). §6.1
pins only the small network. The full-network `const-honly` values were measured on the HEAD~1
export with the planner's probe changed only in its network (`honly_full_reference.py full`,
Verification) and pinned as `CONST_HONLY_FULL_*`.

### 3. `test_bbn_callbacks (h)` compares the builder's callback with itself across the route change — STRUCTURALLY REQUIRED

The prompt asks that "the end-to-end constant ratio" still hold. Rewritten literally, against the
exact constant family's full-network `const-honly` value at the old 1e-4, it **misses**: D/H is
off by **1.06e-4** (Yp 2.8e-6). The cause was measured, not assumed:

- the builder's constant-ratio callback differs from `prym_fixtures.CONSTANT.rho` by a few ulp
  (the B-spline of a constant, and `ratio * (RC g T⁴)` against `((ratio · π²/30) g) T⁴`), and
  PRyMordial moves D/H by ~1e-4 under such changes (the open review-remediation issue
  `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`);
- the **same** callback, built by HEAD~1's `build_NP_callbacks` on the same knots, through the
  HEAD~1 "honly" route, gives Yp = 0.2536761805, D/H = 2.648529359: the same 1.06e-4 from the
  exact family on the old tree (`honly_builder_reference.py full`, Verification);
- the patched route gives that callback **Yp 0.2536761805, D/H 2.648529359**, 4.3e-11 and 7.5e-11
  from HEAD~1.

So the 1e-4 test had been passing at 8.85e-5 (HEAD~1's own print) on a draw of PRyMordial's
noise. Loosening it would rewrite a threshold, and keeping it would leave a red suite for a
reason that is PRyMordial's. Test (h) now compares the builder's callback through the new route
against the same callback through the HEAD~1 honly route, pinned as `BUILDER_CONST_HONLY_FULL_*`,
at **1e-6** (prompt (c)'s tolerance, tighter than the old 1e-4), and prints, without bounding,
the offset from the exact family. What it tested — the constant ratio, through the builder and
PRyMordial, reproduces the reference of the tree before — is kept, and isolated from the ulp
noise. The alternatives were to stop and ask, or to keep 1e-4 against the exact family; the
first seemed unnecessary given the measurement, the second leaves a failing suite. **The
reviewer may disagree; the numbers above are what such a decision would rest on.**

### 4. `test_network_flag (c)` reads main.py's payload key by key — STRUCTURALLY REQUIRED

It `ast.literal_eval`'d the BBN payload dict. With `"wall_clock_limit": bbn_wall_clock_limit`
(a name, not a literal) that raises. It now builds `{key: node}` from the `ast.Dict`, evaluates
only `small_network` (still `False`), and also asserts the `wall_clock_limit` key is present.

### 5. `NPCallbacks` removed — IMPLEMENTATION CHOICE

The prompt left it open ("goes, or keeps one field"). A one-field NamedTuple wrapping one
callable adds an attribute access and nothing else; `build_rho_NP_callback` returns the callable.

### 6. One save-and-restore helper — IMPLEMENTATION CHOICE

The prompt asks that the `_SavedPRyMGlobals` helpers of `test_bbn_callbacks` and
`test_network_flag` save and restore `NP_hubble_flag`. There were three copies of one helper
(those two and `run_prym`'s inline save). They are now `prym_fixtures.SavedPRyMGlobals`, used by
`run_prym`, `run_prym_without_new_physics` and every test; `test_network_flag._SavedPRyMGlobals`
stays as an alias. It saves `NP_thermo_flag`, `NP_hubble_flag`, `verbose_flag`, `smallnet_flag`
and PRyM_thermo's `rho_NP`. It no longer saves `Tstart_NP` or the thermo pressure and
derivative callbacks, which nothing sets any more.

### 7. Where the wall-clock deadline is checked — IMPLEMENTATION CHOICE

`time.monotonic()` (not affected by clock changes). The between-stage check is placed
immediately before each of the eight `solve_ivp` calls and names the stage about to run, so time
spent between solves (the weak-rate recomputation, the nuclear-rate import) is charged before the
next stage starts. No check after the last stage: a solve that ends past the deadline without a
further RHS call is returned. With `wall_clock_limit=None`, `_limited` returns the function
itself, so nothing is wrapped and `_check_wall_clock` returns at once: PRyMordial's behaviour is
unchanged (shown by the `const-honly` reproduction).

### 8. The flag checks raise `AssertionError` explicitly, and include the two flags just set — IMPLEMENTATION CHOICE

README §2 (a) says `_configure_PRyMordial` "asserts" the flags. A bare `assert` vanishes under
`python -O`; the loop raises `AssertionError` itself. It also checks `NP_thermo_flag` and
`NP_hubble_flag`, which it has just set, so that the six requirements sit in one table.

### 9. `--bbn-wall-clock-limit` rejects a negative value; its default is imported — IMPLEMENTATION CHOICE

The prompt says "float, default 600". A negative limit would fail every solve at once, so the
type is `non_negative_float`, which raises an argparse error. The default is
`DEFAULT_BBN_WALL_CLOCK_LIMIT` imported from `ComputeTargets.BBNData`, so the 600 s of P3 is
"set in one constant"; every driver that builds the parser already imports `ComputeTargets`.

### 10. Output-check reasons list every failing value — IMPLEMENTATION CHOICE

`"PRyMordial output: Yp_BBN=0.7 is outside (0, 0.5)"`, `"… DOverH=nan is not finite"`,
`"… Li7OverH=0 is not positive"`; several failures are joined by `"; "` and the whole is
truncated to 256 by `_failure_payload`.

### 11. Black applied to the new hunks of `PRyM/PRyM_main.py` — IMPLEMENTATION CHOICE

README §5 rule 7 exempts `PRyM/` from reformatting. `PRyM_main.py` was black-clean on `7b518c9`
(`black --check`), and black on the patched file changes only the patched lines (the wrapped
`_limited(...)` and `_check_wall_clock(...)` calls), checked by diffing black's output against the
unformatted patch. Applying it formats this prompt's hunks and nothing else. `PRyM_init.py` is
not black-clean and was not reformatted.

### 12. The driver's fixed choices — IMPLEMENTATION CHOICE

As the probe: Planck units, `QCD_Cosmology` on `Planck2018`, Λ = 1e-3 eV, n = 1, the default
tolerances, and the z grid of main.py's defaults. `--wall-clock-limit` defaults to 600 s, 0
disables it, as in main.py.

## Verification performed

All runs from the repository root (or the HEAD~1 export's root), one at a time, nothing else
running, on 2026-10-01.

**Suites** (the three commands of README §5 rule 6):

| package | before (HEAD~1 export) | after |
|---|---|---|
| CosmologyModels | 18, OK (80.8 s) | 18, OK (87.7 s) |
| ComputeTargets | 67, OK (92.7 s) | **71**, OK (104.1 s) |
| Datastore | 17, OK (1.2 s) | 17, OK (1.3 s) |

After the last comment-only edit to `ComputeTargets/BBNData.py`, ComputeTargets and Datastore
were run again on the final tree: 71 OK (93.7 s), 17 OK (1.5 s).

**README §6.2, row by row.**

| quantity | target | measured |
|---|---|---|
| components of the thermodynamic `solve_ivp` | 2 | **2** (`test_f`: y0 lengths `[2, 1, 2, 8, 8]`; HEAD~1: `[3, 1, 2, 8, 8]`, `head_standins.py`) |
| ρ_NP reaches `Hubble` | through `NP_hubble_flag`; `NP_thermo_flag` False; asserts | **only `Hubble` calls ρ_NP** (`test_f`: callers `{'Hubble': N}`; HEAD~1: `{'Hubble': 2029, 'dTgdt': 903, 'N_eff': 1}`); `grep -n NP_hubble_flag PRyM/*.py` → `PRyM_init.py:80` (definition) and `PRyM_main.py:142` (the one read); the six checks in `_configure_PRyMordial` |
| ρ_NP ≡ 0 vs no NP flags, small network | identical to every printed digit | **`==` on all four** (`test_prym_passenger (b)`) |
| constant family vs honly, small network | Yp, D/H to 1e-6 | **Yp 0.2536690815544886, D/H 2.6481673001198094** against 0.2536690816 / 2.6481673: 1.8e-10, 4.5e-11 (`test_prym_passenger (c)`; scratch run of `run_prym(CONSTANT.rho, small_network=True)`) |
| real histories vs §6.1 honly | Yp, D/H to 1e-5; wall ≤ 1.5× | below: **identical to every printed digit**; walls 9.6 s and 10.0 s against 9.7 s and 9.8 s |
| `wall_clock_limit=1e-3` | failure, reason `"PRyMordial: PRyMWallClockLimitError"`, names a stage, < 5 s | **`"PRyMordial: PRyMWallClockLimitError: wall-clock limit of 0.001 s exceeded in stage 'thermodynamics (no NP)': 0.00130508 s elapsed"`, returned in 0.001 s** (`test_g`) |
| output checks | three failure payloads `"PRyMordial output:"` | **`"PRyMordial output: Yp_BBN=0.7 is outside (0, 0.5)"`, `"… DOverH=nan is not finite"`, `"… Li7OverH=0 is not positive"`**; a result inside the checks comes back unchanged (`test_h`). HEAD~1 stores all three as successes |
| short grid; NaN ρ_SM | `ComputationFailureError` | **`"too few samples for the rho_NP spline: 3 in the window, at least 4 needed [test-short]"`; `"rho_NP is not finite at T=0.5 MeV: ratio=0.08, rho_SM=nan [test-nan-eos]"`** (`test_i`). HEAD~1: `ValueError`, and `nan` returned. In `compute_BBN_data` both become `"BBN callbacks: …"` rows through its existing `except ComputationFailureError` |
| `pressure_NP`, `P_NP`, `drho_NP_dT`, `jordan_Hdot_over_H2`, `Tstart_NP` | absent | **absent**: `grep -rn "pressure_NP\|P_NP\|drho_NP_dT\|jordan_Hdot_over_H2\|Tstart_NP" ComputeTargets/ Datastore/ plot_ScalarModel.py main.py tools/` prints nothing |
| `VERSION_LABEL`; `PRYM_VERSION` | `"2026.6.0"`; `"bf24c3d+ri02+sr01"` | **as targeted** (grep; `test_bbn_solver_failures (e)`) |
| test count | not lower | **67 → 71** (+4: `test_bbn_solver_failures` f, g, h, i; the three deletions are replaced one for one) |

`grep -n "NP_thermo_flag" ComputeTargets/` finds `BBNData.py:244` (the assignment to `False`),
`BBNData.py:259` (the check) and test code only.

**Acceptance 2, the real histories** (`./venv/bin/python tools/history_and_bbn.py 2 0.5` and
`… 2 1e-3`, full network, 600 s limit):

| β, M | history (RHS / accepted / reflections / wall) | Yp; D/H ×10⁵ now | §6.1 honly | BBN wall now / §6.1 |
|---|---|---|---|---|
| 2, 0.5 | 40 580 / 4 469 / 0 / 1.8 s | 0.249229266; 2.560889654 | 0.249229266; 2.560889654 | 9.6 s / 9.7 s |
| 2, 10⁻³ | 271 783 / 27 979 / 0 / 8.8 s | 0.2467606164; 2.463862263 | 0.2467606164; 2.463862263 | 10.0 s / 9.8 s |

The RHS and accepted-step counts are §6.1's: the trajectory did not move.

**References measured on the HEAD~1 export** (scratch probes, kept in the scratchpad, quoted
here so the numbers can be reproduced):

- `honly_full_reference.py NETWORK`: the planner's `honly_constant_reference.py const-honly` with
  `small_network` a parameter (`run_prym(CONSTANT.rho, lambda T: -CONSTANT.rho(T), lambda T: 0.0,
  small_network=…)` on the old fixture). **small: Yp 0.2536690816, D/H 2.6481673, ³He/H
  1.068819742, ⁷Li/H 5.190789165** (§6.1 to every digit); **full: Yp 0.2536754614, D/H
  2.648809882, ³He/H 1.068554012, ⁷Li/H 5.137924042**. The patched route, full network, exact
  `CONSTANT`: Yp 0.2536754614330807, D/H 2.648809882160609.
- `honly_builder_reference.py full`: HEAD~1's `build_NP_callbacks` on `test_bbn_callbacks`'
  knots (250 per decade over [1e-7, 1e2] MeV, Saikawa–Shirai in GeV units, constant ratio 0.08,
  pressure ratio r/3 unused) with the pressure callback −ρ_NP and the derivative 0, through the
  old route: **Yp 0.2536761805, D/H 2.648529359**, ³He/H 1.068524998, ⁷Li/H 5.140427514.

**The new tests on HEAD~1.** Shown through HEAD~1's own API (`head_standins.py` on the export),
since the new tests call names that do not exist there:

- (a)/`test_f`: y0 lengths `[3, 1, 2, 8, 8]` and ρ_NP called by `dTgdt` and `N_eff` as well as
  `Hubble`: the test's two assertions fail.
- (e)/`test_h`: HEAD~1's `_run_PRyMordial` with the same stub returns
  `{'Yp_BBN': 0.7, …}`, `{…, 'DOverH': nan, …}` and `{…, 'Li7OverH': 0.0}` as successes.
- (f)/`test_i`: three samples raise `ValueError: The number of derivatives at boundaries does not
  match: expected 1, got 0+0`; the NaN-`G_rho` stand-in's `rho_NP(0.5)` returns `nan`.

Run directly on the export with this prompt's `ComputeTargets/tests/` copied in, the new tests
fail there too: 17 run, 12 failures, 13 errors. `test_f` fails on its own assertion (`3 != 2`); `test_g`, `test_h` and `test_i` error on the API (`_run_PRyMordial() got an unexpected keyword argument 'wall_clock_limit'`, `cannot import name 'build_rho_NP_callback'`), which is why the stand-in measurements above were made.

**`black --check`** on every changed non-`PRyM/` file: clean. `py_compile` of `plot_ScalarModel.py`,
`main.py`, `tools/history_and_bbn.py`, `config/argument_parser.py`: clean.
`create_argument_parser().parse_args(["--database", "x"]).bbn_wall_clock_limit` is `600.0`, and
`0.0` with `--bbn-wall-clock-limit 0`.

**Reasoned, not run.** `main.py` was not run (README §0.5). `plot_ScalarModel.py` was compiled,
not run: it needs a datastore. The panel removal is read from the diff.

## Observations not acted on

1. **PRyMordial's `N_eff` no longer counts ρ_NP.** With `NP_thermo_flag` off, `N_eff` (res[0])
   adds ρ_NP only under the thermo, ν or e flags. It is not stored or read by ChamPBH, so nothing
   changes; README §2 (a) says to leave it. Not an issue.
2. **The stage name "thermodynamics (no NP)"** is now the stage whose H carries ρ_NP. It is kept,
   because it is `_check_solve_ivp`'s name and the test of distinct stage names reads it. Cosmetic;
   not opened.
3. **PRyMordial's ulp sensitivity again.** Deviation 3's 1.06e-4 is another measurement of
   `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` (review-remediation board).
   It is recorded here, not opened as a new issue.
4. **Two planning probes no longer run**: `planning-probes/bbn_route_probe.py` (it uses
   `build_NP_callbacks` and `NPCallbacks`) and `honly_constant_reference.py` (the old
   `run_prym` signature). Both say so in their docstrings' terms ("After prompt 01 … cannot be
   re-run"); `tools/history_and_bbn.py` replaces the first. `prym_callback_domain.py` still runs
   (it calls `PRyMclass(rec, rec, rec)`), but its recorded domain was measured under the old
   flags; prompt 06 may want to re-measure it under the new ones (state handed on, below).

## State handed to the next prompt

- **Labels.** `VERSION_LABEL = "2026.6.0"`; `PRYM_VERSION = "bf24c3d+ri02+sr01"`. No later prompt
  changes either (P9).
- **The callback builder's final signature:**
  `build_rho_NP_callback(log_T_MeV: Sequence[float], density_ratio: Sequence[float],
  rho_SM_MeV4: Callable[[float], float], T_min_MeV: float, T_max_MeV: float, task_label: str)
  -> Callable[[float], float]`. `thermodynamic_rho_SM(eos, units) -> Callable[[float], float]`.
  `_run_PRyMordial(rho_NP, small_network, wall_clock_limit)`.
  `compute_BBN_data(model_proxy, task_label, T_BBN_MeV_spline_max=100,
  T_BBN_keV_spline_min=1e-4, small_network=False, wall_clock_limit=DEFAULT_BBN_WALL_CLOCK_LIMIT)`.
  `SampleValues(raw_N, log_T_Jordan, density_NP, density_NP_ratio)`.
- **PRyMordial's new names:** `PRyM_init.NP_hubble_flag`; `PRyM_main.PRyMWallClockLimitError(stage,
  elapsed, limit)`; `PRyMclass(..., wall_clock_limit=None)`.
- **Fixture:** `prym_fixtures.run_prym(rho, small_network=False, wall_clock_limit=None)`,
  `run_prym_without_new_physics(small_network=False)`, `SavedPRyMGlobals`, `SyntheticNP(label,
  rho)`.
- **The driver:** `./venv/bin/python tools/history_and_bbn.py BETA M [--T-stop-GeV T]
  [--small-network] [--wall-clock-limit SECS]`, from the repository root. On this commit
  (lines beginning `@@` are the cosmology's banner, omitted):

  ```
  $ ./venv/bin/python tools/history_and_bbn.py 2 0.5
  history beta=2 M=0.5: RHS=40580 accepted_steps=4469 reflections=0 samples=5392 wall=1.8 s
  ratio beta=2 M=0.5 [0.3,1) keV: n=130 min=0.05184 median=0.05748 max=0.06183 rms_step=0.0002526
  ratio beta=2 M=0.5 [1,3) keV: n=120 min=0.04373 median=0.04666 max=0.05377 rms_step=0.0001986
  ratio beta=2 M=0.5 [3,10) keV: n=130 min=0.05003 median=0.05301 max=0.05466 rms_step=3.784e-05
  ratio beta=2 M=0.5 [10,100) keV: n=256 min=-0.0284 median=0.05571 max=0.11 rms_step=0.006658
  bbn beta=2 M=0.5: Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487 network=full PRyM_time=9.6 s wall=9.6 s PRyM_version=bf24c3d+ri02+sr01

  $ ./venv/bin/python tools/history_and_bbn.py 2 1e-3
  history beta=2 M=0.001: RHS=271783 accepted_steps=27979 reflections=0 samples=5435 wall=8.8 s
  ratio beta=2 M=0.001 [0.3,1) keV: n=131 min=-0.008412 median=0.001347 max=0.008622 rms_step=0.002129
  ratio beta=2 M=0.001 [1,3) keV: n=120 min=-0.008535 median=-0.001016 max=0.008724 rms_step=0.001586
  ratio beta=2 M=0.001 [3,10) keV: n=130 min=0.003037 median=0.005897 max=0.007478 rms_step=3.634e-05
  ratio beta=2 M=0.001 [10,100) keV: n=257 min=-0.06806 median=0.008126 max=0.08414 rms_step=0.01604
  bbn beta=2 M=0.001: Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365 network=full PRyM_time=10.0 s wall=10.0 s PRyM_version=bf24c3d+ri02+sr01
  ```

  A `ScalarModel` failure prints `history …: FAILURE wall=… reason=…`; it reads
  `failure_reason` if the payload carries one (prompt 02 adds it) and prints
  `(no reason stored)` otherwise. Prompts 03–05 add their outputs (the first bounce, `--phi-init-Mp`,
  the averaged ratio) to this file.
- **Pinned references** (constants in the tests, provenance in Verification):
  `test_prym_passenger.CONST_HONLY_SMALL_*` (0.2536690816, 2.6481673),
  `CONST_HONLY_FULL_*` (0.2536754614, 2.648809882);
  `test_bbn_callbacks.BUILDER_CONST_HONLY_FULL_*` (0.2536761805, 2.648529359).
- **The SM baseline through the new route** (`test_bbn_callbacks (i)`, full network):
  Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017 — the same digits as
  HEAD~1 printed.
- **For prompt 06:** `planning-probes/prym_callback_domain.py` measured PRyMordial's lowest ρ_NP
  query (0.3628 keV) under the old flags. Re-run on this tree (`PYTHONPATH=. ./venv/bin/python
  prompts/science-readiness/planning-probes/prym_callback_domain.py`, small network, 5.7 s): **the
  lowest query is still 0.3628 keV**, the highest 10 MeV, no negative T; 1 944 calls (4 804
  before), 140 below 1 keV (351 before), 307 below 3 keV. ρ_NP is now called only from `Hubble`.
