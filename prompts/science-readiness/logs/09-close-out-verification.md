# Log 09 — Close-out verification and handover

**Prompt:** prompts/science-readiness/09-close-out-verification.md
**Commit:** the commit that adds this file ("Close the science-readiness campaign with a handover"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Measured on `8efc50f` (`HEAD` when the prompt was dispatched; `git status` clean). No production
code and no test. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION` (`"bf24c3d+ri02+sr01"`) are
unchanged. No schema column changed. Every deviation below is an `IMPLEMENTATION CHOICE`.

## What shipped

Documents and records only.

- **`.documents/review-remediation-verification.md`**: a new §4.10, "Addendum 2026-10-02 — the
  `science-readiness` campaign", between §4.9 and §5. `git diff --numstat` for the file: 255
  insertions, 0 deletions. It carries README §7's six points, each with its evidence, and says by
  statement that it supersedes:
  - every earlier statement that `VERSION_LABEL` is `"2026.5.0"` (§4.8 point 1 and its evidence);
  - §4.2's run list, where it implies a pre-2026.6.0 datastore can be reused (and its
    `NPCallbacks` check and the hard-coded φ\*);
  - any statement that BBN uses `NP_thermo_flag` or a pressure callback (§1.2's rows, §2's prompt 04
    entry, §4.2 item 2, §4.7's `build_NP_callbacks` mention, §4.3's "p = ρ/3" magnitudes).
- **`prompts/science-readiness/IMPLEMENTATION_STATE.md`**: the status line gives COMPLETE and 10 of
  10 landed with the final suites; row 09 in §1; a dated close-out line in §3 for the two issues
  that stay open.
- **`.documents/OPEN_ISSUES.md`**: the header and the §1.8 lead-in (the campaign is closed; no row
  added or removed, so the count stays 22).
- **`prompts/INDEX.md`**: this campaign's row marked complete, and the header's date and
  campaign counts (Deviation 1).
- **This log.**

## Deviations from the prompt

### 1. `prompts/INDEX.md`'s header was edited as well as the row — IMPLEMENTATION CHOICE

The prompt allows "this campaign's row". The row's status changes from planned to complete, and
the header line above the table ("Last updated: 2026-10-01 · 5 campaigns: 1 planned, 0 live, 4
closed") would then contradict it, so the date and counts were corrected in the same commit. The
alternative was to leave the header, which the file's own text says is a snapshot of the table.
The row had also not been touched since the planning commit (`7b518c9`): it still said "0 of 9
landed" and "cell-mean `H_J²` and `φ` on the dense output, read by BBN", which is withdrawn; its
"Owns" cell is corrected as well.

### 2. Row values the tests do not print came from a scratch probe — IMPLEMENTATION CHOICE

Several README §6 rows are asserted by tests that print nothing (`test_first_bounce`,
`test_initial_field_option`, `test_fixed_T_values`). No test could be edited. A scratch script in
the session's scratchpad imported the tests' own helpers (`kcl.integrate`, `kcl.interpolated_minima`,
`tf.full_history`, `tf.bbn_ratio`) and printed the values: §6.4's three window rows, §6.5's two
sets, §6.7b's first row. It is not in the tree. The alternative, quoting only "the test passed",
would have given no margin.

### 3. The scope check was judged against README §3.1, the board and the logs, not the prompts — IMPLEMENTATION CHOICE

I was told not to read the other prompts. "A file some prompt was allowed to touch" is therefore
judged from README §3.1's list of files edited by more than one prompt, the board's "Code
(planned)" list and its amendments, and each prompt's log ("What shipped", and its deviations where
a file is outside the plan). The table in Verification says which.

## Verification performed

All runs from the repository root with `venv/bin/python`, by me, on `8efc50f`. The suites ran
first, one after another, then the roster one history at a time, then the row witnesses; nothing
else of the campaign was running. The machine's load average was 6–9 throughout. A process list taken before the roster showed
no Python or Ray job but a Box helper at 0 % CPU; the source of the load was not identified.

### Suites (the three commands of README §5 rule 6)

| package | `6aaa706` (README §6.0) | `8efc50f`, orchestrator, before dispatch | `8efc50f`, run by me |
|---|---|---|---|
| CosmologyModels | 18 | 18 OK (70.0 s) | **18 OK** (68.8 s) |
| ComputeTargets | 67 | 103 OK (87.8 s) | **103 OK** (85.0 s) |
| Datastore | 17 | 31 OK (2.0 s) | **31 OK** (1.9 s) |

No count fell. This prompt adds no test, so the counts after are the counts before: 18, 103, 31.
`black --check` is clean on `extract_common.py`, `plot_by_beta.py`, `ComputeTargets/ScalarModel.py`
and `ComputeTargets/BBNData.py`.

### Version and flags

- `grep -rn "VERSION_LABEL =" --include='*.py' .`, excluding `venv/`, `thirdparty/` and
  `claude-context/`: one line, `config/version.py:43:VERSION_LABEL = "2026.6.0"`.
- `grep -n "PRYM_VERSION =" ComputeTargets/BBNData.py`: `44:PRYM_VERSION = "bf24c3d+ri02+sr01"`.
- `git log 6aaa706..HEAD -- config/version.py`: one commit, `1bc8977` (prompt 01).
- `grep -n "NP_hubble_flag" PRyM/*.py`: `PRyM_init.py:80` (the definition) and `PRyM_main.py:142`
  (the one read). `grep -n "NP_thermo_flag" ComputeTargets/*.py`: `BBNData.py:245` (set `False`) and
  `:260` (checked).
- `grep -rn "pressure_NP\|P_NP\|drho_NP_dT\|jordan_Hdot_over_H2\|Tstart_NP" ComputeTargets/
  Datastore/ plot_ScalarModel.py main.py tools/`: prints nothing.
- `grep -rniE "alter table|migrat" Datastore/ --include="*.py"`: prints nothing (no migration).

### README §6.2–§6.8, row by row

Witnesses: the named test modules, run together with `unittest -v` (71 tests, OK, 79.9 s), plus
the driver and the scratch probe of Deviation 2. "In the log" is the figure its prompt's log gave.
**No row is worse than its log; none is worse than its target.**

| README row | target | measured now | in its log |
|---|---|---|---|
| §6.2 thermodynamic `solve_ivp` components | 2 | **2**: `y0 lengths [2, 1, 2, 8, 8]` (`test_bbn_solver_failures (f)`) | same |
| §6.2 ρ_NP reaches `Hubble` | via `NP_hubble_flag` only | **`rho_NP callers {'Hubble': 1930}`** (same test); the one read at `PRyM_main.py:142`; the checks at `BBNData.py:245, 260` | same pattern (`{'Hubble': N}`) |
| §6.2 ρ_NP ≡ 0 against every NP flag off | identical, small network | **identical** (`test_prym_passenger (b)` OK; it prints Yp 0.2468818826, D/H 2.457976999, ³He/H 1.041855306, ⁷Li/H 5.486812924) | `==` on all four |
| §6.2 constant family against "honly" | Yp, D/H to 1e-6 | **1.79e-10 and 4.52e-11** (`test_prym_passenger (c)`) | 1.8e-10, 4.5e-11 |
| §6.2 real histories against §6.1 honly (β = 2, M = 0.5, 10⁻³) | Yp, D/H to 1e-5; wall ≤ 1.5× | **identical to every printed digit** (roster): Yp 0.249229266 / 0.2467606164, D/H 2.560889654 / 2.463862263; BBN wall 7.9 s / 9.0 s against 9.7 s / 9.8 s | identical; 9.6 s / 10.0 s |
| §6.2 `wall_clock_limit=1e-3` | failure beginning `PRyMordial: PRyMWallClockLimitError`, naming a stage, under 5 s | **`PRyMordial: PRyMWallClockLimitError: wall-clock limit of 0.001 s exceeded in stage 'thermodynamics (no NP)': 0.00101483 s elapsed`** (`test_g`); the driver with `--wall-clock-limit 0.001` at β = 2, M = 0.5: failure, BBN wall 0.4 s | same text, 0.00130508 s |
| §6.2 output checks | three `PRyMordial output:` payloads | **`Yp_BBN=0.7 is outside (0, 0.5)`; `DOverH=nan is not finite`; `Li7OverH=0 is not positive`** (`test_h`) | same |
| §6.2 short grid; NaN ρ_SM | `ComputationFailureError` → `BBN callbacks:` | **`too few samples for the rho_NP spline: 3 in the window, at least 4 needed [test-short]`; `rho_NP is not finite at T=0.5 MeV: ratio=0.08, rho_SM=nan [test-nan-eos]`** (`test_i`) | same |
| §6.2 `pressure_NP`, `P_NP`, `drho_NP_dT`, `jordan_Hdot_over_H2`, `Tstart_NP` | absent | **absent** (grep above) | absent |
| §6.2 `VERSION_LABEL`; `PRYM_VERSION` | `"2026.6.0"`; `"bf24c3d+ri02+sr01"` | **as targeted** (greps; `test_e`) | as targeted |
| §6.2 test count | not lower | **18 / 103 / 31** against 18 / 67 / 17 | 71 after prompt 01 |
| §6.3 failure row read back | `failure_reason` equal to the raised message, truncated to 256 | **OK**: `Datastore.tests.test_scalarmodel_failure_reason` (`test_a_failure_reason_round_trips_truncated_to_256`, `a2`, `a3`, `a4`) | first 256 of 300 characters |
| §6.3 step budget 50 | `failure_reason` begins `step budget exhausted` | **`step budget exhausted: integrate_scalar_history (reason-test) took 51 accepted steps (budget 50) at N=4.325263426, T_J=269.15 GeV, with 0 reflection(s)`** (printed by `ComputeTargets.tests.test_scalarmodel_failure_reason`) | identical message |
| §6.3 `main.py` summary; `plot_by_beta.py` drop report | `ScalarModel` reasons too | **`test_c_grouped_by_first_clause_most_frequent_first`, `test_c2_empty` OK**; `main.py:69, 294` calls `summarise_failure_reasons`; `plot_by_beta.py:769, 868` `report_dropped_scalar_models`. Neither driver was run | read; neither run |
| §6.4 P1, M = 0.5, to N = 21 | `N = 20.343028 ± 1e-5`, φ = 4.57371e-3 ± 1e-4 rel., not reflected, equal to `interpolated_minima[0]` to 1e-12 | **`N = 20.343026850` (−1.15e-6), `φ = 4.573704680e-03` (1.16e-6 rel.), `reflected = False`; ΔN = 0 and Δφ = 0 against `interpolated_minima[0]`** | −1.15e-6; 1.15e-6; exact |
| §6.4 P1, M = 1e-10 | `reflected = True`, φ ∈ [1e-11, 1e-10] | **`reflected = True`, `N = 20.352100380349 = reflections[0].N`, `φ = 4.702274e-11`, one reflection** | same |
| §6.4 P2 window | `None` | **`None`**, 0 reflections | `None` |
| §6.4 full histories | β = 2: `N = 20.34303`, 746.63 MeV at M = 0.5; `20.35208`, 746.69 MeV at 10⁻³; β = 1.6, 10⁻⁵ recorded; each to 1e-5 in `N` | **20.343026853 / 746.634744 MeV; 20.352082230 / 746.686275 MeV; 18.974433718 / 420.758153 MeV** (roster) | identical |
| §6.4 round trip | four columns back as written; `None` as `None` | **OK**: `test_first_bounce_round_trip` (turning point, reflection, no bounce, failure row, unpopulated) | same |
| §6.5 option and literals | one option, default 5.0; no literal | **OK**: `test_initial_field_option (b), (c)`; `grep` of `5.0 \* units.PlanckMass` in the three drivers prints nothing; `main.py:865`, `plot_ScalarModel.py:1613`, `plot_by_beta.py:1021` read `args.phi_init_Mp * units.PlanckMass` | same |
| §6.5 β ∈ {1, 6, 7, 25, 40}, φ\* = 5; φ\* = 1 | {7, 25, 40}; {40} | **[7.0, 25.0, 40.0]; [40.0]**; `ln(M_P/T*) = 32.43340147` (scratch probe; `test_a_the_check` OK); `test_d_the_warning_returns_the_list_unchanged` OK | {7, 25, 40}; {40} |
| §6.6 | withdrawn (2026-10-02) | **not re-measured**, as the amended prompt says. The roster's point `ratio` windows are recorded in §4.10 point 5 | log 05 holds the withdrawn figures |
| §6.7 default floor; pre-check | 0.2 keV; `T_stop ≤ 20 eV` | **OK** (`test_bbn_spline_floor (a)`); the failure text printed: `T_Jordan_stop=0.1 keV is more than 0.1*T_BBN_spline_min=20 eV` | same |
| §6.7 the domain guard on a full solve | never fires | **never fired** on any of the ten roster histories; `test_bbn_spline_floor (b)`: `calls=1944 lowest positive T = 0.3628 keV (floor 0.2 keV)` | same |
| §6.7 D/H, Yp at β = 2, M = 0.5 and 10⁻³ against log 05's point-input figures | ≤ 1e-5 relative | **0** (identical to every printed digit) | 0 |
| §6.7 `--T-stop-GeV 1e-8` passes the pre-check, `1e-7` fails it | as stated | **OK** (`test_bbn_spline_floor (a)`) | as stated |
| §6.7b crossings at three samples' own `ln T_J` (β = 2, M = 0.5; 324 samples in (0.07, 1) MeV) | `raw_N` to 1e-10; φ to 1e-9 rel.; ratio to 1e-8 rel. | **\|ΔN\| = 0, 3.55e-15, 0; φ rel. 0, 4.53e-15, 0; ratio rel. 0, 7.04e-13, 0** (ratios −0.0483915, −0.00961481, 0.0682227; scratch probe, `test_a` OK) | identical |
| §6.7b a temperature not reached | `None` ×4 | **`FixedTValues(None, None, None, None)`** (`test_b` OK) | same |
| §6.7b a crossing across a reflection | found on that step; `ln T_J` continuous to 1e-12 | **OK** (`test_c`: the reflection at N = 20.352100380349, step k found, \|Δ ln T_J\| ≤ 1e-12 asserted) | \|Δ ln T_J\| = 0 |
| §6.7b round trip, `_do_not_populate` | four values to the last bit; `None` as `None`; failure raises; `_do_not_populate` returns them | **OK**: `test_fixed_T_values_round_trip` (five tests) | same |
| §6.7b the driver on the three histories | one crossing of each; trajectory unchanged | **`crossings=1` for both temperatures on all ten roster histories**; the three of log 06b are identical to every printed digit (RHS, steps, first bounce, four values) | same |
| §6.8 each §2 (k) function | tested on synthetic input, with the edge cases | **12 tests OK** (`test_extraction`, under a second) | 12 |
| §6.8 the four figures and the CSV | built from synthetic records, header equals the §2 (k) list | **OK** (`test_e_the_four_figures_and_the_csv`, `test_e_records_carry_the_fixed_T_values`, `test_e_figure_4_keeps_negative_ratios`, `test_e_a_figure_with_nothing_to_plot_is_skipped`) | same |
| §6.8 `plot_by_beta.py` reads the columns through the existing lookups; `--band-half-width` in the parser | read; `ast` | **`_do_not_populate`: 4 literals in the file now and 4 at `541c048~1`** (`plot_by_beta.py:710, 848, 882, 909`); `--band-half-width` at `config/argument_parser.py:137`; `test_f_parser_option`, `test_f_plot_by_beta_uses_the_new_functions` OK | same |

Further printed figures, unchanged from the logs and recorded for the next reader:
`test_network_flag (b)`: small network Yp 0.2536690816, D/H 2.6481673, ⁷Li/H 5.190789165;
full network Yp 0.2536754614, D/H 2.648809882, ⁷Li/H 5.137924042; the small network against the
full one differs by 1.029e-2 in ⁷Li/H, 2.426e-4 in D/H and 2.515e-5 in Yp. This is a measurement
of PRyMordial's two networks and is not a bound or an issue. `test_bbn_callbacks (h)`: 4.33e-11 and
7.47e-11 against the builder's honly reference, and 2.83e-6 and 1.06e-4 against the exact
family's (printed, not bounded). `test_bbn_callbacks (i)`: the SM baseline,
Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017, equal to
`tools/bbn_baseline.py`'s (7.0 s).

### The roster (README §6.9)

`./venv/bin/python tools/history_and_bbn.py β M`, full network, one history at a time. All ten
completed; BBN completed on all ten with no failure and no domain-guard message; each history had 0
reflections; each first bounce is not reflected. The SM baseline is `tools/bbn_baseline.py` above.

| β | M | RHS | accepted steps | history wall | first bounce `N` | `T_J` (MeV) | BBN wall |
|---|---|---|---|---|---|---|---|
| 1.2 | 0.5 | 24 193 | 2 728 | 0.9 s | 17.662454825 | 231.069537 | 7.2 s |
| 1.6 | 0.5 | 31 492 | 3 364 | 1.2 s | 18.967065903 | 420.726607 | 7.2 s |
| 2.0 | 0.5 | 40 580 | 4 469 | 1.5 s | 20.343026853 | 746.634744 | 7.9 s |
| 3.0 | 0.5 | 57 526 | 6 120 | 2.1 s | 24.483798229 | 1682.865313 | 7.9 s |
| 1.2 | 10⁻³ | 254 491 | 26 458 | 6.5 s | 17.667931109 | 231.107524 | 7.9 s |
| 1.6 | 10⁻³ | 155 961 | 16 275 | 4.6 s | 18.974419125 | 420.758090 | 8.3 s |
| 2.0 | 10⁻³ | 271 783 | 27 979 | 7.4 s | 20.352082230 | 746.686275 | 9.0 s |
| 3.0 | 10⁻³ | 327 046 | 34 342 | 9.9 s | 24.498974358 | 1680.011085 | 9.0 s |
| 1.6 | 10⁻⁵ | 1 445 132 | 137 137 | 35.2 s | 18.974433718 | 420.758153 | 8.3 s |
| 2.0 | 10⁻⁵ | 1 679 987 | 162 676 | 40.8 s | 20.352100202 | 746.686377 | 8.1 s |

| β | M | Yp | ΔYp | D/H ×10⁵ | ΔD/H | source's ΔD/H (its §2) |
|---|---|---|---|---|---|---|
| 1.2 | 0.5 | 0.2582949607 | +4.621 % | 2.649704815 | +7.613 % | 7.5 % |
| 1.6 | 0.5 | 0.2490967227 | +0.895 % | 2.53373365 | +2.903 % | 2.8 % |
| 2.0 | 0.5 | 0.249229266 | +0.949 % | 2.560889654 | +4.006 % | 4.0 % |
| 3.0 | 0.5 | 0.2509606949 | +1.650 % | 2.603108857 | +5.721 % | 5.8 % |
| 1.2 | 10⁻³ | 0.2567571266 | +3.998 % | 2.599025939 | +5.555 % | 5.79 % |
| 1.6 | 10⁻³ | 0.2468868563 | −0.000 % | 2.459878815 | −0.096 % | −0.03 % |
| 2.0 | 10⁻³ | 0.2467606164 | −0.051 % | 2.463862263 | +0.065 % | 0.07 % |
| 3.0 | 10⁻³ | 0.2467560634 | −0.053 % | 2.457894203 | −0.177 % | −0.02 % |
| 1.6 | 10⁻⁵ | 0.2468788501 | −0.003 % | 2.4647705 | +0.102 % | BBN failed |
| 2.0 | 10⁻⁵ | 0.2467016048 | −0.075 % | 2.46477019 | +0.102 % | −0.02 % |

The shifts are against Yp 0.2468872958 and D/H 2.462251065 (`tools/bbn_baseline.py`, full network,
this tree).

The point `ρ_NP/ρ_R,J` windows (median / rms step between samples). No averaged window exists
(README §0.2 amendment):

| β | M | [0.3, 1) keV | [1, 3) keV | [3, 10) keV | [10, 100) keV |
|---|---|---|---|---|---|
| 1.2 | 0.5 | 0.04615 / 0.0001356 | 0.05523 / 4.243e-05 | 0.0581 / 1.364e-05 | 0.05921 / 0.001702 |
| 1.6 | 0.5 | 0.04431 / 5.013e-05 | 0.04238 / 5.955e-05 | 0.03695 / 1.734e-05 | 0.03747 / 0.001671 |
| 2.0 | 0.5 | 0.05748 / 0.0002526 | 0.04666 / 0.0001986 | 0.05301 / 3.784e-05 | 0.05571 / 0.006658 |
| 3.0 | 0.5 | 0.08466 / 6.302e-05 | 0.07739 / 5.636e-05 | 0.07138 / 4.329e-05 | 0.06462 / 0.00886 |
| 1.2 | 10⁻³ | 0.02595 / 0.0001334 | 0.03488 / 4.174e-05 | 0.03771 / 1.341e-05 | 0.03879 / 0.001663 |
| 1.6 | 10⁻³ | 0.0001158 / 0.0004092 | 0.0001394 / 0.0002707 | −0.0002198 / 0.0002084 | 0.00038 / 0.007167 |
| 2.0 | 10⁻³ | 0.001347 / 0.002129 | −0.001016 / 0.001586 | 0.005897 / 3.634e-05 | 0.008126 / 0.01604 |
| 3.0 | 10⁻³ | −0.0002623 / 0.003212 | 0.001065 / 0.001777 | 0.001387 / 8.152e-05 | 0.006542 / 0.02187 |
| 1.6 | 10⁻⁵ | −0.0004114 / 0.001253 | −0.0005007 / 0.0007573 | 0.002184 / 2.316e-05 | 0.00377 / 0.007134 |
| 2.0 | 10⁻⁵ | 0.001831 / 0.002238 | −0.001186 / 0.001652 | 0.006159 / 3.637e-05 | 0.008392 / 0.01604 |

The fixed-temperature values (the driver's `fixed_T` line; φ in M_P; `crossings=1` for both
temperatures on every history):

| β | M | φ at 1 MeV | ratio at 1 MeV | φ at 70 keV | ratio at 70 keV |
|---|---|---|---|---|---|
| 1.2 | 0.5 | 1.275013713e-01 | 3.254938182e-01 | 3.197018806e-02 | 9.636415344e-02 |
| 1.6 | 0.5 | 8.409885031e-03 | 1.712891880e-02 | 8.372870318e-03 | 2.181647545e-02 |
| 2.0 | 0.5 | 1.138197048e-02 | −4.810565953e-02 | 8.695012936e-03 | 6.742167855e-02 |
| 3.0 | 0.5 | 9.216109805e-03 | 5.774615380e-02 | 8.079681692e-03 | 6.601952900e-02 |
| 1.2 | 10⁻³ | 1.212272093e-01 | 3.054924141e-01 | 2.441964898e-02 | 7.526261697e-02 |
| 1.6 | 10⁻³ | 1.480268778e-03 | −9.688906604e-03 | 1.297772492e-04 | 1.051728183e-02 |
| 2.0 | 10⁻³ | 3.524341402e-03 | −7.814304755e-02 | 6.541189438e-04 | 1.579537539e-03 |
| 3.0 | 10⁻³ | 1.553742745e-03 | −4.733402692e-02 | 9.708724836e-05 | −3.760884100e-02 |
| 1.6 | 10⁻⁵ | 1.418937374e-03 | −1.043543235e-02 | 1.313754026e-04 | −6.242391165e-03 |
| 2.0 | 10⁻⁵ | 3.508772415e-03 | −7.820158832e-02 | 6.365845226e-04 | 7.249525705e-04 |

**Against the logs.**
- β = 2 at M = 0.5, 10⁻³ and 10⁻⁵, and β = 1.6 at 10⁻⁵: RHS, accepted steps, first bounce, all
  four abundances and the ratio windows equal log 05's `State handed on` block to every printed
  digit, and the `fixed_T` values equal log 06b's. History walls (1.5, 7.4 and 35.2 s) match
  log 06b's 1.6, 7.3 and 36.7 s within 7 % (log 05's were loaded).
- RHS equal §4.8's (β = 1.2, 3.0 at M = 0.5; β = 3.0 at 10⁻³) and §4.9's (β = 1.2 at 10⁻³: 254 491,
  26 458, 473 wall bounces not re-counted).
- First bounces equal §4.8–§4.9's to the printed digits except β = 3, M = 0.5: the dense-output root
  is `N = 24.483798229` at 1682.865 MeV, against §4.8's 24.48381 at 1682.85 MeV (1.2e-5 in `N`).
  §4.8 itself says an accepted-step `N` depends on the step's width, and this prompt did not check
  which of the two §4.8's column holds. README §6.4's 1e-5 target is for the two β = 2 rows, which
  hold.
- The β = 2 BBN figures at M = 0.5 and 10⁻³ equal README §6.1's full-network "honly" rows exactly.

**Against the source.** The source does not state its network. Its harness is not in the
repository. Its ΔD/H agree with ours to within 0.24 percentage points (largest: β = 1.2 at 10⁻³,
5.555 against 5.79; β = 3.0 at 10⁻³, −0.177 against −0.02). Its ΔYp (M ≤ 10⁻²): +3.97 to +4.02 %
at β = 1.2 (ours +3.998 %); ≈ 0 at β = 1.6 (−0.000 %); −0.10 to −0.07 % at β = 2.0 (ours −0.051 %
and −0.075 %); −0.01 to +0.04 % at β = 3.0 (ours −0.053 %). Its β = 2, M = 0.5 D/H of 2.5597
differs from ours (2.560889654) by 4.6e-4 relative; PRyMordial's response to ulp-level changes in
ρ_NP is of that order (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`). These
are measurements, not bounds. **The source's one PRyMordial failure, β = 1.6 at M = 10⁻⁵, did not
reproduce, and β = 2.0 at 10⁻⁵ also completes.**

### Options, run once more

- `tools/history_and_bbn.py 2 0.5 --phi-init-Mp 2`: RHS = 41 070, accepted steps = 4 603, first
  bounce `N = 14.476112028` at 659.393816 MeV, Yp 0.2487467993, D/H 2.584485137, BBN wall 8.9 s.
  Equals log 04's figures.
- `tools/history_and_bbn.py 2 0.5 --wall-clock-limit 0.001`: history as above, then `bbn …: FAILURE
  network=full wall=0.4 s reason=PRyMordial: PRyMWallClockLimitError: wall-clock limit of 0.001 s
  exceeded in stage 'thermodynamics (no NP)': 0.365168 s elapsed`.

### The scope check

`git diff --stat 6aaa706..HEAD`: 78 files, 10 547 insertions, 916 deletions. `thirdparty/` is not
in it. Every file, and the commit that touched it (from `git log --name-only`), judged as
Deviation 3 says:

| file | touched by |
|---|---|
| `ComputeTargets/BBNData.py` | 01, 06, and the housekeeping commit `20d86a5` (a message typo and a stale comment, standalone) |
| `ComputeTargets/ScalarModel.py` | 02, 03, 06b |
| `Datastore/SQL/ObjectFactories/BBNData.py` | 01 |
| `Datastore/SQL/ObjectFactories/ScalarModel.py` | 02, 03, 06b |
| `PRyM/PRyM_init.py`, `PRyM/PRyM_main.py` | 01 (marked patches, log 01) |
| `config/version.py` | 01 only |
| `config/argument_parser.py` | 01, 04, 07 (and 07's revert and redo) |
| `main.py` | 01, 02, 04 |
| `pipeline_selection.py` | 02, 04 (02's use accepted by the user; board Decisions, 2026-10-01) |
| `plot_ScalarModel.py` | 01, 04 |
| `plot_by_beta.py` | 02, 04, 07 |
| `extract_common.py` | 07 |
| `tools/history_and_bbn.py` | 01, 03, 04, 06b |
| `ComputeTargets/tests/`: `prym_fixtures.py`, `test_bbn_callbacks.py` (also `20d86a5`), `test_bbn_solver_failures.py`, `test_network_flag.py`, `test_prym_passenger.py` | 01 (and 06 for `test_bbn_callbacks.py`, `test_prym_passenger.py`; log 06 Deviation 1) |
| `ComputeTargets/tests/`: `test_scalarmodel_failure_reason.py`; `test_first_bounce.py`; `test_initial_field_option.py`; `test_bbn_spline_floor.py`; `test_fixed_T_values.py`; `test_extraction.py` | 02; 03; 04; 06; 06b; 07 |
| `Datastore/tests/`: `test_scalarmodel_failure_reason.py`; `test_first_bounce_round_trip.py`; `test_fixed_T_values_round_trip.py` | 02; 03 (and 06b, log 06b Deviation 4); 06b |
| `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`, `architecture-summary.md`, `paper-corrections-numerical-section.md` | 08 |
| `.documents/OPEN_ISSUES.md` | planning, and each prompt that moved an issue |
| `prompts/INDEX.md` | planning (`7b518c9`) |
| `prompts/{integrator-remediation,review-remediation,run-integrity}/IMPLEMENTATION_STATE.md` | planning, and the `Resolved`/`Narrowed`/`Assigned` lines of 01, 02, 04, 05, 06, 08 (README §5 rule 4) |
| `prompts/science-readiness/**` (README, board, prompts, logs, orchestrator, probes, source, figures) | the campaign's own folder: planning, orchestration and every prompt's log and board edit |

**No file is one that no prompt was allowed to touch.** The four files outside README §3.1's and
the board's lists are test modules edited by prompt 01 and prompt 06 (`prym_fixtures.py`,
`test_network_flag.py`, `test_prym_passenger.py`, `test_bbn_callbacks.py`), which the logs record
as replacing the tests of the removed route one for one; tests live in `<package>/tests/`
(README §5 rule 6).

What I reasoned and did not run:
- That an old datastore file cannot be opened by the new code: read from the column list and the
  absence of any migration, not tried.
- `main.py` and `plot_by_beta.py` against a store (README §0.5): neither was run, here or by any
  prompt of the campaign. The super-Planckian warning is printed by `main.py` only.
- The physical-`M` cross-check (about 24 minutes per history): the user's.

## Observations not acted on

- **`prompts/INDEX.md` was not updated by any prompt before this one.** Its header and row still
  said "planned, 0 of 9 landed" and described the withdrawn averaging. Corrected here
  (Deviation 1). Not an issue.
- **§4.8's first-bounce `N` at β = 3, M = 0.5** (24.48381) differs from the dense-output root
  (24.483798229) by 1.2e-5. Probably the accepted-step `N` that §4.8's own caution describes;
  not checked, because no README row depends on it. Not an issue.
- **Board counts.** The board's §1 table counts ten prompts (01–09 and 06b); the README's prose
  says nine ("nine of nine" in the orchestrator's check 5). The board says "10 of 10 landed".
- **`test_bbn_callbacks.py` still has `T_MIN_MEV = 1e-7`** and a comment on the old domain
  (log 06, Observations). Cosmetic; left.

No §3 issue is opened by this prompt. The two `[05-…]` issues stay open (board §3, dated line).

## State handed to the next prompt

There is no next prompt: this closes the campaign. What the science run needs is in
`.documents/review-remediation-verification.md` §4.10. In short:

- **A fresh datastore file.** `VERSION_LABEL = "2026.6.0"`; every earlier store is invalid and an
  old file cannot be opened by the new code (columns were added with no migration).
- **The command lines.** `main.py` with `--phi-init-Mp` (default 5.0), `--bbn-wall-clock-limit SECS`
  (default 600, 0 disables) and the pre-existing options; `plot_by_beta.py --database <store.db>
  --output <dir> [--band-half-width 0.025] [--phi-init-Mp 5.0] [--T-stop-GeV <GeV>]`. The
  histories must reach 20 eV (`--T-stop-GeV 1e-8`, or `T_CMB`).
- **Reproduce the roster:** `./venv/bin/python tools/history_and_bbn.py β M`, one invocation at a
  time, from the repository root; the numbers above are its output on `8efc50f`.
- **Not to be reported:** `AdiabaticHistory` max |Q| for M ≲ 10⁻³, until
  `[post-adiabatic-Q-reads-aliased-late-samples]` is settled.
- **Open, by name:** that issue; `[00-stored-samples-alias-the-rebounds]` (adiabatic half);
  `[00-settling-at-physical-M-needs-a-parked-tracking-model]`; the two `[05-…]` issues of this
  board; and the physical-`M` cross-check, now possible and the user's to run.
- Suite counts at the close: CosmologyModels 18, ComputeTargets 103, Datastore 31, all OK.
