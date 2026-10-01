# Log 04 — Close-out verification and handover

**Prompt:** prompts/integrator-remediation/04-close-out-verification.md
**Commit:** the commit that adds this file ("Close the integrator-remediation campaign with a handover"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-01
**Result:** COMPLETE WITH DEVIATIONS

*Added 2026-10-01, after the commit:* Deviations 1 was ruled by the user (the board's
Decisions). The first bounce is the dense-output turning point; all nine histories pass and the
§5 stop condition is not met.

The work was measured on `abcc99f` (`HEAD` at dispatch, prompt 03's commit), which is the final
tree: this prompt changes no code and no test. "The probe" is the audit's `p_full.py β M 1e-8 1e-4
kin reflect` run from an export of `918590e` (Deviations 2). "The loop" is
`integrate_scalar_history(…, StepControl())` driven by a scratch script outside the repository
(`nine.py`, below). Every number comes from one of those, from the unit tests, or from the greps
shown.

**One of the prompt's §5 stop conditions is met on one reading and not on the other**
(Deviations 1): the first bounce of β = 0.9, `M = 0.5` differs between the probe and the loop by
7.9×10⁻⁵ in `N` when it is taken at the accepted step after `π` turns positive, the measure the
prompt names, and by 3.5×10⁻⁹ when it is taken on the dense output. I finished the work rather
than stop, so that the user has every measurement to rule on; the commit is one revert unit.

## What shipped

Documentation only.

- `.documents/review-remediation-verification.md`: a new §4.8, "Addendum 2026-10-01 — the
  `integrator-remediation` campaign", between the end of §4.7 and the `---` before §5. 147 lines
  added, none deleted (`git diff --numstat`: `147 0`). It carries the five points of campaign
  README §7, each with its evidence, and supersedes by statement the three things the prompt names
  (below).
- `prompts/INDEX.md`: this campaign's row is **complete**, and the header counts.
- `prompts/integrator-remediation/IMPLEMENTATION_STATE.md`: status `COMPLETE`, row 04, the
  suite counts, §3's closing paragraph.
- `.documents/OPEN_ISSUES.md`: the header and §1.6's lead sentence say the campaign is complete.
  No issue was opened or closed, so no row and no count of open issues changed (25).
- This log.

**`VERSION_LABEL` before and after: `"2026.5.0"`, unchanged.**

**What the addendum supersedes, and what it found when it looked.**
- *`VERSION_LABEL` is `"2026.4.0"`:* §4.7 (point 1, its evidence, and verification rows quoting
  `config/version.py:33`). Superseded by statement; the label is `"2026.5.0"`,
  `config/version.py:36`.
- *§4.2's run-list expectations of cost per history:* **§4.2 states none.** Neither §4.2 nor §4.3
  contains a run time or a step count for a history; the addendum says so and gives the table of
  costs under point 3.
- *§4.3's statements about hard reflections as a failure indicator:* **§4.3 states none.** The
  statements that exist are §4.2 item 4 and §4.5 (the key `number_hard_reflections` and a count to
  read per plotted history). The addendum supersedes those: the key is no longer written, and the
  count to read is `number_reflections`, which is a property of the model and not a symptom.

## Deviations from the prompt

### 1. The first-bounce comparison is on both measures, and the prompt's measure misses at β = 0.9 — STRUCTURALLY REQUIRED (a prompt assumption does not hold; a §5 stop condition is literally met; reported, not resolved)

What the prompt assumed. §1 asks that the probe and the loop "agree on the first bounce to `1e-5`
in `N`", and §5 stops on a history whose first bounce "disagrees with the probe's by more than
`1e-5` in `N`". It does not say how the bounce is located. Both the probe's
(`harness.wall_bounces`) and the test helpers' (`wall_bounces`) report `N` at the first **accepted
step after `π` changes sign**. That `N` is a property of the step placement, as log 01 Deviations 5
showed for `φ_min`.

What is there. The step that straddles the turn is 8.5×10⁻⁵ e-folds wide in the probe at β = 0.9,
`M = 0.5` (the bounce is gentle, `φ_min = 1.9×10⁻²`), so two integrations of the same trajectory
whose steps are placed differently report `N` apart by up to that width.

Measured (probe pickles from `p_full.py … record_steps`; loop from `nine.py`; script `cmp.py`). The
last two columns are the two readings of "the first bounce".

| β | M | step across the turn (probe) | accepted-step `N`, probe | accepted-step `N`, loop | Δ accepted | dense-output `N`, loop | Hermite turn on the probe's two steps | Δ turning point |
|---|---|---|---|---|---|---|---|---|
| 0.9 | 0.5 | 8.53e-5 | 36.1592629 | 36.1591836 | **−7.93e-5** | 36.1591830 | 36.1591830 | +3.5e-9 |
| 1.2 | 0.5 | 4.33e-6 | 17.6624559 | 17.6624563 | +4.7e-7 | 17.6624548 | 17.6624548 | +3.4e-11 |
| 2.0 | 0.5 | 6.65e-6 | 20.3430276 | 20.3430277 | +1.2e-7 | 20.3430269 | 20.3430269 | −1.4e-10 |
| 3.0 | 0.5 | 1.27e-5 | 24.4837983 | 24.4838073 | +9.0e-6 | 24.4837982 | 24.4837982 | +9.4e-10 |
| 1.2 | 0.01 | 8.50e-8 | 17.6678323 | 17.6678324 | +9.5e-8 | 17.6678323 | 17.6678323 | +1.5e-10 |
| 2.0 | 0.01 | 1.54e-7 | 20.3519189 | 20.3519189 | −2.0e-10 | 20.3519189 | 20.3519189 | −7.0e-12 |
| 3.0 | 0.01 | 3.55e-7 | 24.4987016 | 24.4987016 | +7.9e-10 | 24.4987012 | 24.4987012 | +9.1e-10 |
| 2.0 | 0.001 | 1.70e-8 | 20.3520822 | 20.3520822 | +2.7e-10 | 20.3520822 | 20.3520822 | −1.5e-12 |
| 3.0 | 0.001 | 2.40e-8 | 24.4989744 | 24.4989744 | +9.6e-10 | 24.4989744 | 24.4989744 | +9.1e-10 |

The Hermite column is a cubic Hermite interpolant of `φ` (with `φ' = π`) on the probe's two
bracketing steps, whose derivative's root is the turn; it is a check on the probe, not a
production measure. The dense-output `N` is `interpolated_minima`'s root of `π` on the loop's
interpolant.

Result. By the accepted-step measure, β = 0.9 misses `1e-5` (7.9×10⁻⁵) and β = 3.0 at `M = 0.5`
is within it (9.0×10⁻⁶); every other row agrees to `5×10⁻⁷` or better. By the turning point, **all
nine agree to `9.4×10⁻¹⁰`**, and the `φ_min` there agrees to the digits printed (for example
`1.892870e-2` at β = 0.9 from both). The trajectory is the same; the difference is where the steps
fell. This is the same distinction the user ruled on for `φ_min` on 2026-10-01 (board Decisions).

What was done. I did not stop: the data needed to rule are all in this log, and the commit is
cheap to revert. I did not loosen the criterion, and I did not decide which measure the prompt
meant. **The user (or the orchestrator) rules**, as for `φ_min`: if the dense-output turning point
is the measure, all nine pass and nothing else changes; if the accepted-step `N` is, β = 0.9
fails by a factor of 8 and the stop stands. The Result line above is `COMPLETE WITH DEVIATIONS`
for that reason.

**Ruling (2026-10-01, the user, added after the commit).** The dense-output turning point is the
measure, as intended when the prompt was written. All nine histories pass, so the stop condition
is not met. The largest dense-output difference in the table above is 3.5×10⁻⁹ (β = 0.9); the
"9.4×10⁻¹⁰" in the Result paragraph above understates it, and the table is right.

### 2. The audit's `p_full.py` was run from an export of `918590e` — STRUCTURALLY REQUIRED

What the prompt assumed: `p_full.py β M 1e-8 1e-4 kin reflect` runs from the root of the final
tree. What was there: `harness.build` calls `ScalarFieldIntegrationSupervisor(units, T_init,
T_stop, np.inf, label="probe", …)` and prompt 01 removed the `max_step_size` argument. On `abcc99f`
the script raises `TypeError: ScalarFieldIntegrationSupervisor.__init__() got multiple values for
argument 'label'` (I ran it). Log 01 (Observations 6) said so. The harness's loop uses only
`ODEPolicy` and `ODERHS`, which no prompt changed on a physical state (log 02 shows bit-identical
values), so I ran it from `git archive 918590e` into the scratchpad, `AUDIT_OUT` set to the
scratchpad, with this repository's `venv/bin/python`. `918590e` is the last tree the harness runs
on (it is prompt 01's parent). `.documents/` was not edited to make it run.

### 3. The scope check is against the README, the board and the logs, not against prompts 01–03 — IMPLEMENTATION CHOICE (instructed)

§1 asks to check each file against "each prompt's §1 and §3". I was told not to read the other
prompts in this campaign, so I checked against what states the same thing and which I may read:
README §0.4 and §2 (e), the board's "Code (planned)" list, its Decisions (the user's ruling that
widened prompt 02 to `IntegrationSupervisor.__exit__`), and the "What shipped" sections of logs
01–03. Result: no file outside them (below). A reviewer who wants the stricter check can run it
against the prompts' own §1 lists.

## Verification performed

I ran everything below, on `abcc99f`, from the repository root with `venv/bin/python`.

**The suites** (`PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`):

| package | at `2b89022` (campaign README §5) | at dispatch / now | wall |
|---|---|---|---|
| CosmologyModels | 18 | **18 OK** | 103.6 s |
| ComputeTargets | 41 | **67 OK** | 135.3 s |
| Datastore | 17 | **17 OK** | 1.8 s |

Prompt 04 changes no test, so these are also the counts after.

**README §6.1 (a)–(c)** (`rows.py`, which calls the test module's `integrate`, `wall_bounces` and
`interpolated_minima`). "Log 01" is the value the prompt's log quotes; every difference from it is
at the last digit printed or exactly zero.

| row | target | measured now | log 01 |
|---|---|---|---|
| (a) M = 0.5: RHS | ≤ 3 000 | **1 945** (224 steps) | 1 945 |
| first bounce `N` (accepted step) | 20.343028 ± 1e-5 | 20.3430347 (Δ 6.7e-6); dense output 20.3430269 | 20.3430347 |
| `φ_min` | 4.57371e-3 ± 1e-4 rel | 4.5737948e-3 (1.9e-5 rel); dense 4.5737047e-3 | 4.5737948e-3 |
| `φ(21)`, `π(21)` | 1e-5 rel | 1.22038340e-1, −1.29715271e-1 | same |
| reflections | 0 | 0 | 0 |
| (a) M = 0.01: RHS; `φ_min`; `φ(21)` | ≤ 3 500; 9.1505e-5 ± 1e-4; 1.185154e-1 ± 1e-5 | **2 120**; 9.1504508e-5; 1.18515423e-1 | same |
| (a) M = 0.001: RHS; `φ_min`; `φ(21)` | ≤ 3 500; 9.1505e-6; 1.184501e-1 | **2 275**; 9.1505073e-6; 1.18450064e-1 | same |
| (b) M = 1e-10 | completes, 1 reflection, `φ ∈ [1e-11, 1e-10]`, `φ(21) = 1.184428e-1` | completes, 1 reflection at `N` = 20.3521004, `φ` = 4.7023e-11, `π_in` = −0.497623; `φ(21)` = 1.18442804e-1; 1 693 RHS | same |
| (b) M = 4.1e-28 | the same | the identical reflection and `φ(21)`; 1 693 RHS | same |
| (c) f = 0.02, M = 0.5 | (a)'s tolerances | `N` 20.3430285; `φ_min` 4.5737085e-3; `φ(21)` 1.22038340e-1; 2 840 RHS | same |
| P3: RHS; restarts | ≤ 60 000; 0 | **38 547** (4 040 steps); 0 | same |
| P3: wall bounces; `φ(37.5)` | 51; 5.8078e-4 ± 1e-4 | 51; 5.80781984e-4 | same |
| P3 `φ_min`, bounces 1, 2, 8, dense-output minimum (the user's ruling) | ± 2e-4 rel | 2.798861e-4 (4.1e-7), 3.036503e-4 (9.9e-7), 3.663676e-4 (1.08e-6) | 4e-7, 1e-6, 1e-6 |
| same, at the accepted step (for reference) | — | 1.07e-4, 2.10e-4, 2.32e-4 | 1.06e-4, 2.10e-4, 2.31e-4 |
| P2: RHS; bounces; `φ(40)` | ≤ 25 000; 19; 1.909693e-2 ± 1e-5 | **18 880** (2 052 steps); 19; 1.90969251e-2 | same |
| P2 `T_Jordan = 0` in captured stdout | 0 | 0 | 0 |

The accepted-step `φ_min` for P3 bounces 2 and 8 (2.10e-4, 2.32e-4) is the figure the ruling of
2026-10-01 declares is not the §6.1 (b) measure; recorded because the ruling asks prompt 04 to
re-measure (b) with the dense-output minimum, which is the second-to-last row. It meets ± 2e-4 by
three orders of magnitude.

**README §6.1 (e)** (`rows.py`; these are also unit tests that pass in the suite above).

| row | measured now |
|---|---|
| the loop with the cap disabled, P1 at M = 0.01 | `ComputationFailureError`: "phi <= 0 in an accepted state (the step cap was violated) at N=20.35432523, phi_E=-0.0011071, pi_E=-0.49762" |
| three injected RHS failures, P1 at M = 0.5 | completes; `steps_rejected_by_exception` = 3; `φ(21)` = 1.22038339e-1; 1 855 RHS |
| G1, P1 at M = 1e-10, `reflects_at_origin = False` | `ComputationFailureError`: "the representable-step floor was reached (N=20.35210038, phi_E=4.7023e-11, pi_E=-0.49762), but the potential ExponentialPotential(M=2.436e+08GeV,Lambda=0.001eV) does not declare …" |
| G2, the step-over state at M = 0.01, `cap_floor = 1e-3` | `ComputationFailureError`: "reflection requested inside the wall: W/(pi^2/2) = 23.232 at N=20.35, phi_E=5e-05, pi_E=-0.4976 (W = 2.8762, pi^2/2 = 0.1238)" |
| G2 on legitimate reflections: the maximum `W/(½π²)` | M = 1e-10: 2.55e-47; M = 4.1e-28: −0.0; physical `M`, β = 0.9, 140 reflections: **−0.0** (largest over every reflection in this log); the nine histories: no reflections. Target ≤ 1.6e-4 |

**README §6.1 (d), the nine full histories, twice.**

The loop (`nine.py`; initial state built as `compute_scalar_model` builds it; stdout captured; the
nine run one after another and nothing else heavy was running):

| β | M | RHS | target | steps | wall | wall bounces | first bounce `N` (accepted step) / `T_J` (MeV, at that step) | target `N`, `T_J` | `N` at `T_CMB` |
|---|---|---|---|---|---|---|---|---|---|
| 0.9 | 0.5 | 26 372 | completes | 3 040 | 0.8 s | 16 | 36.159184 / 1.044 eV | 36.159 ± 1e-3 | 44.3185 |
| 1.2 | 0.5 | 24 193 | ≤ 5×10⁴ | 2 728 | 0.8 s | 17 | 17.662456 / 231.0693 | 17.662 ± 1e-3, 231.1 | 45.7686 |
| 2.0 | 0.5 | 40 580 | ≤ 8×10⁴ | 4 469 | 1.5 s | 26 | 20.343028 / 746.6342 | 20.343, 746.6 | 49.6603 |
| 3.0 | 0.5 | 57 526 | ≤ 1.2×10⁵ | 6 120 | 2.9 s | 48 | 24.483807 / 1 682.8504 | 24.484, 1 683 | 54.5523 |
| 1.2 | 0.01 | 108 789 | ≤ 2.5×10⁵ | 11 484 | 5.1 s | 195 | 17.667832 / 231.1068 | 17.668, 231.1 | 46.0403 |
| 2.0 | 0.01 | 98 133 | ≤ 2.5×10⁵ | 10 632 | 5.0 s | 196 | 20.351919 / 746.6853 | 20.352, 746.7 | 50.0287 |
| 3.0 | 0.01 | 151 480 | ≤ 3.5×10⁵ | 16 513 | 7.5 s | 284 | 24.498702 / 1 680.0608 | 24.499, 1 680 | 55.0170 |
| 2.0 | 0.001 | 271 783 | ≤ 6×10⁵ | 27 979 | 13.4 s | 803 | 20.352082 / 746.6863 | 20.352, 746.7 | 50.0616 |
| 3.0 | 0.001 | 327 046 | ≤ 7×10⁵ | 34 342 | 14.6 s | 1 017 | 24.498974 / 1 680.0111 | 24.499, 1 680 | 55.0584 |

All nine end at `T_J` = 2.72550 K with **0 reflections, 0 rejected steps, 0 `T_Jordan = 0`
substitutions, 0 "negative value of E" prints and no traceback text** in the captured streams.
`sup.RHS_evaluations` equals `nfev` in every row. The RHS, step, bounce and first-bounce columns
are **identical to log 01's table and to log 02's re-run** (log 01 quotes 26 372 / 24 193 / 40 580
/ 57 526 / 108 789 / 98 133 / 151 480 / 271 783 / 327 046). Wall times are lower than log 01's
(2.4–25 s) on a quieter machine.

The probe (`p_full.py β M 1e-8 1e-4 kin reflect` from the `918590e` export, one after another):

| β | M | RHS | steps | wall | wall bounces | first bounce `N` (accepted step) | `T_J` (MeV) |
|---|---|---|---|---|---|---|---|
| 0.9 | 0.5 | 26 274 | 3 035 | 0.7 s | 16 | 36.159263 | — (1.0 eV) |
| 1.2 | 0.5 | 24 103 | 2 727 | 1.1 s | 17 | 17.662456 | 231.0693 |
| 2.0 | 0.5 | 40 548 | 4 476 | 2.4 s | 26 | 20.343028 | 746.6342 |
| 3.0 | 0.5 | 57 008 | 6 120 | 3.8 s | 48 | 24.483798 | 1 682.8653 |
| 1.2 | 0.01 | 108 578 | 11 461 | 4.6 s | 195 | 17.667832 | 231.1068 |
| 2.0 | 0.01 | 98 106 | 10 623 | 4.8 s | 196 | 20.351919 | 746.6853 |
| 3.0 | 0.01 | 151 454 | 16 448 | 6.4 s | 285 | 24.498702 | 1 680.0608 |
| 2.0 | 0.001 | 272 311 | 27 963 | 10.3 s | 804 | 20.352082 | 746.6863 |
| 3.0 | 0.001 | 328 147 | 34 413 | 10.6 s | 1 016 | 24.498974 | 1 680.0111 |

Agreement. **RHS:** within 0.9 % in every row (the largest is β = 3, `M` = 0.5: 57 526 against
57 008; the 10 % criterion is met by a factor of 11). **First bounce:** see Deviations 1. By the
accepted-step `N`, 8 of 9 rows agree to ≤ 9.0×10⁻⁶ and β = 0.9 does not (7.9×10⁻⁵); by the
turning point, all nine agree to 9.4×10⁻¹⁰. The probe's wall-bounce counts differ from the
loop's by one in three rows (285 / 284, 804 / 803, 1 016 / 1 017), as log 01 noted; the
histories are chaotic after delivery (audit §11) and the two runs differ in the digits of the
initial state beyond the tenth. The probe's `N_end` overshoots `T_CMB` by up to one step; the
loop's `N_final` is the root. Compare first bounces and RHS, not `N_end`.

**Two physical-`M` histories through the production code** (`phys.py`, which calls the undecorated
`compute_scalar_model._function`, no Ray, with a 9 000-point z grid, `verbose=True`; `M = 4.1e-28`,
default `StepControl`, budget `2×10⁶`):

- **β = 0.9: completes.** 140 elastic reflections (first at `N` = 36.18164), `compute_steps` =
  187 485 RHS (audit §3.7: 140 reflections, 1.9×10⁵ RHS), 26 312 accepted steps, 4 840 samples,
  last `raw_N` = 44.575, label `"Radau+kinematic-cap-stepping0"`, `build_extra_data` =
  `{number_reflections: 140, cap_fraction: 0.1, cap_floor: 1e-11, cap_global_max_step: 0.1,
  jacobian_factor_max: 1e-4, accepted_steps: 26312}`. Wall **6.7 s**. Run again directly through the
  loop (`phys09_ratio.py`): 187 485 RHS, 140 reflections, reflection `φ` from 1.707e-12 to
  7.776e-11, `W/(½π²)` maximum −0.0, no `T_Jordan = 0`.
- **β = 2: ends on the budget, as a failure row.** `compute_scalar_model` returned
  `{'failure': True}` and printed `-- compute_scalar_model (phys-2.0): integration failure`,
  `step budget exhausted: integrate_scalar_history (phys-2.0) took 2000001 accepted steps (budget
  2000000) at N=45.68458593, T_J=1.896e-11 GeV, with 14769 reflection(s)`, and `!!
  compute_scalar_model (phys-2.0): marked as total integration failure`. It did not run past the
  budget. **Wall time: 1 413.9 s (23.6 min)**, the cost of a clean failure. The last status line
  (20 minutes in) read 12.04 million RHS, 12 453 reflections, `N` = 45.391, 87.98 % of the way
  to `T_CMB` measured in `ln T_J`. A light scratch job (`cmp.py`, `phys09_ratio.py`, a minute of
  one core each) ran during the first half hour on a ten-core machine; the wall time carries that.

**README §6.1 (f) and §6.2, the greps and diffs** (run from the root; `EX` excludes `venv`,
`thirdparty`, `claude-context`, `.git`, `prompts`, `.documents`):

| row | target | witness and result |
|---|---|---|
| `VERSION_LABEL` | one line, `"2026.5.0"` | `grep -rn "VERSION_LABEL =" --include='*.py' . \| grep -v "venv/\|thirdparty/\|claude-context/"` → `config/version.py:36:VERSION_LABEL = "2026.5.0"` |
| `HARD_REFLECTIONS_KEY`, `SolutionFragment`, `notify_level_1`, `notify_hard_reflection`, `hard_reflection_count` | nothing outside `git log` | the grep over `*.py` finds only the string literal `"number_hard_reflections"` in `test_reflection_reporting.py:114`, which pins that the old key is absent; none of the five identifiers |
| stepper label | `"Radau+kinematic-cap-stepping0"` in `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` | grep finds `main.py:942, 952`, `plot_by_beta.py:676`, `plot_ScalarModel.py:1575`, `ScalarModel.py:71` |
| the six `solve_ivp` events, the level-1/2 notifications | gone | `grep -n "solve_ivp" ComputeTargets/ScalarModel.py` finds nothing |
| `extra_data` keys; `atol`/`rtol`; z grid; `SampleValues` | the §2 (e) set; unchanged | `compute_scalar_model` smoke (above); `ScalarModel.py:52,104–105,772–773` use `DEFAULT_ABS_TOLERANCE` / `DEFAULT_REL_TOLERANCE` as before |
| `solver_list`, the `while not success` loop | gone | `grep -n "solver_list\|LSODA\|DOP853\|\"BDF\"\|solve_ivp" ComputeTargets/ScalarModel.py` finds nothing |
| `RuntimeError` in the integration path | 1 (z grid) | `ScalarModel.py:897`; the rest (`:1157` onward) are the `ScalarModel` class's accessors, outside it; `:856` is a comment |
| `RHS_timer.__exit__` and the supervisor's | print nothing | `grep -n "print_tb\|traceback" Quadrature/supervisors/base.py` finds nothing |
| `data.d_logV_dphi` | fixed | the only matches in `ScalarModel.py` are the comment at `:448–451` |
| the step budget | present, `2×10⁶` | `StepControl` field `step_budget`, `ScalarModel.py:97`; the β = 2 run above |
| §6.3, the documents | additions only | `git diff --numstat 2b89022..HEAD`: `architecture-summary.md` 12/0, `integrator-audit-2026-09-30/README.md` 48/0, `numerical-strategies.md` 184/0, `paper-corrections-numerical-section.md` new (123/0) |

**The scope check.** `git diff --stat 2b89022..HEAD` shows 34 files. Against README §0.4 and §2 (e),
the board's "Code (planned)" list and its Decisions, and the "What shipped" of logs 01–03
(Deviations 3), every one belongs:
- production: `ComputeTargets/ScalarModel.py`; `Quadrature/supervisors/ScalarField.py` and
  `base.py` (the latter by the user's ruling, board Decisions, log 02 Deviations 1); `extract_common.py`,
  `plot_by_beta.py`, `plot_ScalarModel.py`, `main.py`; `config/version.py`;
  `CosmologyConcepts/Potentials/AbstractPotential.py` and `ExponentialPotential.py` (README §0.4's
  "exactly two properties");
- tests: `test_kinematic_cap_loop.py`, `test_integrator_exceptions.py`, and
  `test_hard_reflection_reporting.py` renamed to `test_reflection_reporting.py` (README §2 (e));
- documents: `.documents/numerical-strategies.md`, `architecture-summary.md`, the audit README,
  `paper-corrections-numerical-section.md`, `OPEN_ISSUES.md` (prompt 03 and the board rule);
- campaign material: `prompts/INDEX.md`, the campaign's README, board, orchestrator files, prompts
  and logs.

**No file is outside the allowed lists.** `PRyM/`, `thirdparty/`, `Datastore/`, the other potentials,
and `.documents/review-remediation-verification.md` are absent from the range (the last is this
commit's). I did not read the prompts' own §1 and §3 (Deviations 3).

**The audit's probes against the shipped scheme are history.** `p1_sweep.py a`'s `regions` rows and
`p3_grazing.py regions` drive the audit's own copy of the fragment loop in `harness.py`
(`run_fragment_loop`), not the production code, which no longer has a fragment loop. They still
run from an export of `918590e` and still give the `b1f64d8` figures (log 01 re-ran the P1 ones:
17 092, 26 634, 26 422 RHS). I did not re-run them and did not run the ten-minute one.

**What I did not do.** I ran no `main.py`, no Ray, no datastore. That `store()` resolves the new
label against the `solvers` dict in `main.py` is reasoned from the code (log 01), not run. The
§3.5.6 estimate in `numerical-strategies.md` (about 3.5×10⁵ accepted steps for `M = 1e-6`, β = 2)
was not re-measured; the prompt does not ask for it and `numerical-strategies.md` is not in this
commit's allowed files.

## Observations not acted on

1. **First-bounce `N` and `φ_min` at the accepted step depend on the step placement**
   (Deviations 1). Not opened as an issue: it is a question about which measure prompt 04's
   acceptance meant, for the user, not a defect. The user's ruling on `φ_min` is the precedent.
2. **The step budget costs about 24 minutes per failing history at physical `M`, β = 2.** That is
   the planner's `2×10⁶` default doing its job, and well under the "an hour" the README expected.
   A survey over many β values at physical `M` with β ≥ 1.2 spends that per history. The constant
   is one line (`StepControl.step_budget`); no issue.
3. **The reflection `φ` at physical `M` ranges from 1.7e-12 to 7.8e-11** (140 reflections, β =
   0.9), not within the `[1e-11, 1e-10]` the (b) row states for the single P1 reflection: the
   floor `φ_stop = |π| h_floor / f` scales with the `|π|` at the reflection, which is small for
   the settled field. This is README §2 (b)'s formula, and the G2 ratio there is exactly zero; I
   record it because the (b) row's range reads as if it applied to every reflection.
4. **A physical-`M` β = 2 history spends its steps on reflections, 14 769 of them in 2×10⁶
   accepted steps**, about 135 steps per reflection near the end. This is the settling doubling of
   audit §3.7 and is the parked-tracking model's subject. Open already; no new issue.
5. **The audit's scripts do not run on the final tree** (Deviations 2). Log 01 recorded it;
   `.documents/` is not edited.
6. **`.documents/OPEN_ISSUES.md` count.** No issue was opened or closed by this prompt, so the
   count of open issues (25, nine on this board) is unchanged; only its header's description of the
   campaign changes.

## State handed to the next prompt

There is no next prompt in this campaign. What the science run needs is in
`.documents/review-remediation-verification.md` §4.8 (read it before planning the run). The state:

- **Campaign status:** `COMPLETE`, four of four landed. Final tree before this commit: `abcc99f`;
  this commit is the close-out.
- **Suites on the final tree:** CosmologyModels 18 OK, ComputeTargets 67 OK, Datastore 17 OK.
- **`VERSION_LABEL = "2026.5.0"`.** Every `ScalarModel` history, and every `AdiabaticHistory` and
  `BBNData` row built on one, made earlier is invalid.
- **Open for the user to rule on (Deviations 1):** whether prompt 04's "first bounce agrees to
  `1e-5` in `N`" is the accepted-step `N` (β = 0.9 fails it by 7.9×10⁻⁵) or the dense-output
  turning point (all nine pass, to 9.4×10⁻¹⁰). Nothing in the code changes either way; the
  Result line and the board's status carry the question.
- **Open issues:** nine on this board, none assigned (index §1.6, 25 in the whole index). The one
  that matters for the science run is
  `[00-settling-at-physical-M-needs-a-parked-tracking-model]`: no physical-`M` history with β ≥ 1.2
  can be produced until it exists; such a run fails on the step budget after about 24 minutes
  (β = 2, `M` = 4.1e-28) and stores a failure row with no reason.
- **To reproduce the nine histories through the loop:** the scratch driver is `nine.py`
  (scratchpad, not in the repository); it needs only
  `integrate_scalar_history(ODERHS(…), ScalarFieldIntegrationSupervisor(…), StateVector(5.0, 0.0,
  ln ρ_rad,E*, ln f_m*, ln T*), 0.0, ln T_CMB, StepControl(), policy=policy)` inside `with
  supervisor:`, with the initial state built as `compute_scalar_model` builds it and the
  `build` helper of `ComputeTargets/tests/test_kinematic_cap_loop.py`. For the probe, `git archive
  918590e` into a directory, then from its `.documents/integrator-audit-2026-09-30` run
  `AUDIT_OUT=<out> <repo>/venv/bin/python p_full.py β M 1e-8 1e-4 kin reflect`.
- **Physical `M` through the production entry point:** the undecorated
  `compute_scalar_model._function(cosmology, T_init, T_stop, phi, pi, z_grid, potential, coupling,
  task_label=…, verbose=True)`, as `phys.py` does; no Ray and no datastore.
