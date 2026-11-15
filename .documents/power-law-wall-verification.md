# power-law-wall — verification

**Campaign:** [`prompts/power-law-wall/`](../prompts/power-law-wall/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/power-law-wall/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`power-law-wall/`](power-law-wall/)

This document records what the campaign measured, prompt by prompt. It is additive (`CLAUDE.md` invariant 6): a
re-run that supersedes a figure gets a new dated section, and the figures below stay as they are, correct for the
tree they were taken on.

**Measurements, not bounds** (README §5). The numbers below are reported, not judged against anything, except where
a test's tolerance is stated as such.

---

## 1. The model (prompt 01, 2026-11-14)

**Tree:** the worktree `.claude/worktrees/power-law-wall` on branch `power-law-wall`, at `75ac6fb` plus prompt 01's
changes (committed as prompt 01's commit). No Ray, no store but the temporary ones of the suites, no PRyMordial
outside `tools/history_and_bbn.py`'s BBN stage. Host: the planner's Mac (10 CPUs), shared with other sessions; the
load is given beside every timing.

### 1.1 The class

`CosmologyConcepts/Models/InversePowerModel.py`, type id `INVERSE_POWER_MODEL = 2`:

    V(phi) = V_0 [1 + (M/phi)^n],    A(phi) = exp(beta (phi - phi_ref)/M_P),    n an integer >= 1.

- Every hook is computed from ln u = n ln(M/phi): ln V = ln V_0 + ln(1 + u) as ln u + log1p(e^{-ln u}) (ln u > 0) or
  log1p(e^{ln u}); (ln V)' = -n s/phi and (ln V)'' = [n(n + 1) s - n^2 s^2]/phi^2 with s = u/(1 + u) from ln u
  (s = e^{ln u} below ln u = -700). (ln V)'' divides by phi twice, not by phi^2, which underflows below about
  1e-154.
- phi <= 0, NaN or inf raises `ComputationFailureError` naming the model, the hook and phi in M_P (orchestrator note 01,
  ruling 2). The integrator treats one raised at a trial state as a rejected step and halves the step.
- `_log_V_floor` is ln V_0; `reflects_at_origin` is True; the coupling is the exponential's, written in the class.
- No `_drain_L`: `check_drain_law_model` refuses the model under the one-loop law; the fixed law and natural mode run
  it.
- `renormalize` is ln V_0 + ln V_target - ln V(phi); the normalization is its own coordinate.
- The seed, at every n: the tracking analytic minimum, else the static one, else plain. It solves
  n V u/((1 + u) phi) = beta rho_m0/M_P, i.e. phi^{n+1}(1 + u) = C(phi) with C = n V M^n M_P/(beta rho_m0) and V from
  the Friedmann condition of its kind, by the fixed point ln phi <- [ln C - ln(1 + u)]/(n + 1), stopped at
  |phi_new/phi - 1| < 1e-14 within 200 iterations (the exponential's rule).

`tools/compare_stores.py` declares the id unstored (`UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]`), so that
its test of the declared maps holds; prompt 05 gives the class its table and moves the id into the map (the user's
ruling of 2026-11-14, "(a), add the unstored entry to compare_stores").

### 1.2 The tests and their tolerances

`ComputeTargets/tests/test_inverse_power_model.py` (21 tests) and two entries added to `test_wall_model.py`'s
`_NO_CLOSED_FORM` (InversePowerModel n = 1 and 2 at M = 1e-30 and 1e-10 M_P), on which test (3)'s defining-equation
checks run unchanged.

| Check | Points | Tolerance (reason in the test's docstring) | Worst measured |
|---|---|---|---|
| ln V against 60-digit decimal ln V_0 + ln(1 + u) | n = 1, 2; M = 4e-31, 1e-3 M_P; 31 phi from 1e-90 M to 10 M_P | 4 eps (\|ln V_0\| + \|ln V\|) | 0.90 eps (\|ln V_0\| + \|ln V\|) |
| V - V_0 = V_0 u (at ln V_0 = 0) | as above | 4 eps (\|ln V\| + 2) in ln | within |
| ln(-V') against ln(n V_0 M^n) - (n + 1) ln phi | as above | 8 eps × the logs' sizes | 0.056 of the bound |
| ln V'' against ln(n(n + 1) V_0 M^n) - (n + 2) ln phi | as above | the same | 0.052 of the bound |
| (ln V)' phi, (ln V)'' phi against centred differences (h = 1e-5 in ln phi) | as above | 1e-7 relative | 2.5e-9, 2.5e-9 |
| classical phi_t = M (V_0/E)^{1/n}; ln omega_t^2, ln R, lambda_t, ln k_ex | n = 1, 2; M = 4e-31, 1e-16, 1e-3 M_P; ln E = -180, -120, -60 | brentq's tolerance plus rounding over the slope n | phi_t exact (0 of the bound) |
| one_loop_turnaround against the chain iterated with this wall's closed forms | as above | 1e-8 relative | 1.3e-13 |
| minimum_on_trace against (n V_0 M^n M_P/(beta T))^{1/(n+1)}; d phi_min/d ln T = -phi_min/(n + 1) | n = 1, 2; M = 4e-31, 1e-3 M_P; ln T = ln rho_m0, -200 | root tolerance + R/S; S root tolerance + R | within |
| the seed against an independent brentq of the same equation | see 1.3 | 1e-12 relative in phi_ref, 1e-12 in the normalization | 2.1e-14, 0 |

Also: phi <= 0, NaN and inf raise from each hook, naming the model and the hook; `renormalize` reaches its target to
1e-12 and refuses V_target <= 0; the coordinate round-trips exactly; `check_drain_law_model` refuses the model under
`one_loop` naming `InversePowerModel` and `_drain_L`, and accepts it under `fixed`; a pickled realized model returns
every hook's value bit for bit.

Two short untuned integrations (`integrate_history`, the seed's realization, M = 4e-31 M_P, beta = 2, phi* = 5 M_P, to
T_J = 100 MeV, 20 samples per decade), at a load of about 10:

| Law | n | Reflections | Accepted steps | Samples | First bounce | Wall |
|---|---|---|---|---|---|---|
| natural | 2 | 4 | 1212 | 198 | N = 20.352098, 746.7 MeV | 0.5 s |
| fixed, alpha = 0.1 | 1 | 5 | 1407 | 199 | N = 20.352098, 746.7 MeV | 0.9 s |

### 1.3 The seed

| n | beta | M / M_P | Kind | phi_ref / M_P | rel. to the independent root | \|Δ normalization\| |
|---|---|---|---|---|---|---|
| 1 | 1.5 | 1e-3 | tracking | 3.3889418494e-02 | 5.3e-15 | 0 |
| 1 | 1.5 | 0.5 | static | 6.4472655005e-01 | 2.9e-15 | 0 |
| 1 | 2.0 | 4e-31 | tracking | 6.6544823963e-16 | 7.1e-15 | 0 |
| 2 | 1.5 | 1e-3 | tracking | 1.3893849643e-02 | 0 | 0 |
| 2 | 1.5 | 0.5 | static | 8.1184210758e-01 | 8.8e-15 | 0 |
| 2 | 2.0 | 4e-31 | tracking | 7.0757560169e-21 | 7.0e-15 | 0 |

At beta = -1.5 and 0 both kinds have no solution and the seed is plain, as on the exponential. At laboratory M every
tuned run of §1.5 converged in one integration from the tracking seed, at n = 2 as at n = 1 (the planner's probe, with
the exponential's n = 1 seed, took three at n = 2).

### 1.4 The tool route

`tools/history_and_bbn.py` and `tools/surfing_scan.py` take `--model {exponential, inverse-power}` (default
exponential) and `--n` (default 1).
- At the defaults, given or not, every line is as before: `test_history_and_bbn`, `test_history_and_bbn_alpha` (its
  pinned stdout), `test_history_and_bbn_drain_law`, `test_surfing_scan` and the two `test_plot_surfing_*` modules
  pass unedited, and `tools/tests/test_model_option.py` (10 tests) shows the explicit defaults print the pinned
  stdout and write the same console, CSV and meta.json options as no option.
- Away from the defaults every label of `history_and_bbn` gains ` model=<model> n=<n>` after M (for example
  `history beta=2 M=4e-31 model=inverse-power n=2: ...`), and `surfing_scan`'s first line and every beta's line name
  the model and n; its CSV gains no column, its meta.json options record them, and a CSV is resumed only by a run of
  its own model and n.
- `history_and_bbn --drain-law one_loop --model inverse-power` exits 2 before any integration, with
  check_drain_law_model's message on stderr.

### 1.5 The runs

`model-01/runs.sh` (text outputs in [`power-law-wall/model-01/out/`](power-law-wall/model-01/out/), the table in
`out/summary.md`): tuned, by the tool, from the worktree root, three processes at a time, 2026-11-14 18:52:14 to
18:55:37, one-minute load 4.7 to 8.3 (each file holds `uptime` at its start and end). The exponential counterpart of
each row is the same command without `--model` and `--n` (orchestrator note 01, ruling 4). "—": the tool prints no
dissipation line at alpha = 0.

| Run | Status | Wall (s) | Integr. | Seed | Refl. | Inj. | ln R first impact | ΔN_eff today | ΔN_eff 1 MeV | Y_p | D/H (×1e5) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IP n=1, 4e-31, β 1.2, α 0.1 | ok | 13 | 1 | tracking | 9 | 9 | 511.074 | 0.0656475 | 0.0607733 | 0.2495309517 | 2.490743146 |
| IP n=2, 4e-31, β 1.2, α 0.1 | ok | 13 | 1 | tracking | 9 | 9 | 308.986 | 0.0656475 | 0.0607733 | 0.2495292897 | 2.490791564 |
| exp, 4e-31, β 1.2, α 0.1 | ok | 12 | 1 | tracking | 9 | 9 | 138.168 | 0.0656475 | 0.0607733 | 0.2495309517 | 2.490743146 |
| IP n=1, 4e-31, β 2.0, α 0.1 | ok | 19 | 1 | tracking | 49 | 49 | 530.575 | 0.0524407 | 0.0496486 | 0.2470913297 | 2.472581604 |
| IP n=2, 4e-31, β 2.0, α 0.1 | ok | 18 | 1 | tracking | 49 | 49 | 320.687 | 0.0524407 | 0.0496486 | 0.2470878 | 2.472626501 |
| exp, 4e-31, β 2.0, α 0.1 | ok | 18 | 1 | tracking | 49 | 49 | 142.366 | 0.0524407 | 0.0496486 | 0.2470913297 | 2.472581604 |
| IP n=1, 4e-31, β 2.9, α 0.1 | ok | 17 | 1 | tracking | 42 | 42 | 545.639 | 0.038435 | 0.0372475 | 0.2473962618 | 2.470819503 |
| IP n=2, 4e-31, β 2.9, α 0.1 | ok | 16 | 1 | tracking | 42 | 42 | 329.726 | 0.038435 | 0.0372475 | 0.2473923377 | 2.470734445 |
| exp, 4e-31, β 2.9, α 0.1 | ok | 17 | 1 | tracking | 42 | 42 | 145.602 | 0.038435 | 0.0372475 | 0.2473962485 | 2.470617709 |
| IP n=1, 1e-3, β 1.2, natural | FAILURE (G2, below) | 3 | — | — | — | — | — | — | — | — | — |
| IP n=2, 1e-3, β 1.2, natural | ok | 49 | 4 | tracking | 4 | — | — | — | — | 0.2534231058 | 2.507814782 |
| exp, 1e-3, β 1.2, natural | ok | 36 | 4 | tracking | 0 | — | — | — | — | 0.2481063767 | 2.367545759 |
| IP n=1, 1e-3, β 2.0, natural | FAILURE (G2) | 6 | — | — | — | — | — | — | — | — | — |
| IP n=2, 1e-3, β 2.0, natural | ok | 81 | 4 | tracking | 28 | — | — | — | — | 0.2422304191 | 2.343407516 |
| exp, 1e-3, β 2.0, natural | ok | 40 | 4 | tracking | 0 | — | — | — | — | 0.2362848396 | 2.19356787 |
| IP n=1, 1e-3, β 2.9, natural | FAILURE (G2) | 11 | — | — | — | — | — | — | — | — | — |
| IP n=2, 1e-3, β 2.9, natural | ok | 95 | 4 | tracking | 59 | — | — | — | — | 0.2412793593 | 2.315996159 |
| exp, 1e-3, β 2.9, natural | ok | 46 | 4 | tracking | 0 | — | — | — | — | 0.2347322025 | 2.151896626 |

Wall is the whole process's (BBN included). The first bounce of every successful pair is at the same N and T_J as the
exponential's (17.6679 and 231.1 MeV at β 1.2, 20.3521 and 746.7 MeV at β 2.0, 23.8878 and 1870.3 MeV at β 2.9).
D/H is printed as the tool prints it (×1e5).

The three n = 1 failures at 1e-3 M_P are the integrator's guard G2, raised in the seed's integration (tuning
integration 0) and stored as the history's reason:

| β | N | phi_E / M_P | pi_E / M_P | W/(pi^2/2) | phi_t = phi_E (W/(pi^2/2))^{1/n} |
|---|---|---|---|---|---|
| 1.2 | 35.9874939 | 8.8224e-13 | -8.9421e-3 | 1.6097 | 1.42e-12 M_P |
| 2.0 | 37.75808722 | 4.7358e-14 | -4.8214e-4 | 1.5358 | 7.27e-14 M_P |
| 2.9 | 40.6223989 | 5.753e-15 | -6.023e-5 | 1.1679 | 6.72e-15 M_P |

W = 3(V - V_0)/(3H^2 M_P^2) is proportional to (M/phi)^n here, and pi^2/2 is the kinetic fraction, so the classical
turning point of that state is phi_E (W/(pi^2/2))^{1/n}: the field had been stepped past its turning point, by a
factor 1.17 to 1.61 in phi, when the floor rule asked for the reflection, and G2 refused it. Before the failure each of
these histories printed the tuning's warning that its classical turning point at the first floor reflection lies
below 1e-8 M_P (phi_t = 4.2e-48, 8.4e-50 and 4.1e-51 M_P at β 1.2, 2.0, 2.9, at N = 17.668, 20.352, 23.888); at n = 2
the same warning gives 6.6e-26, 9.3e-27 and 2.1e-27 M_P.

### 1.6 Natural mode at large M, and the even continuation (a measurement for the user)

- **With the committed class** (phi <= 0 a `ComputationFailureError`), no accepted state has phi <= 0 on any of the six
  natural-mode histories at 1e-3 M_P. Trial states at phi < 0 reach the RHS in two of them, and each is a rejected step:
  - n = 1, β = 2: one RHS call, at N = 34.8166, phi = -1.90e-5 M_P, pi = -5.81e-3 M_P; the history then goes on to
    fail at G2 at N = 37.758 (§1.5);
  - n = 2, β = 2: four RHS calls, at N = 31.324 to 31.346, phi from -9.2e-6 to -2.1e-4 M_P; the history succeeds.
  The other four reach no phi <= 0. The planner's probe, before this prompt, failed both n at 1e-3 M_P with an uncaught
  `ValueError` from such trial states; with the class's error they are rejected steps, n = 2 succeeds, and n = 1 fails
  at G2 instead.
- **The scratch probe** (not committed): the same six histories with the hooks continued to phi < 0 as the even
  extension V(|phi|), (ln V)' odd, (ln V)'' even. Four histories are unchanged (no phi <= 0 reached). In the two that
  reach phi < 0 the step control does not reject the trial points, and an accepted step lands at phi < 0, which the
  integrator refuses as "phi <= 0 in an accepted state":
  - n = 1, β = 2: five RHS calls at phi < 0 (N = 34.8166 and 34.8285), failure at N = 34.82854076,
    phi_E = -1.1955e-4 M_P, pi_E = -1.1301e-2 M_P (tuning integration 0);
  - n = 2, β = 2: five RHS calls (N = 31.3455 and 31.3695), failure at N = 31.36954244, phi_E = -6.9508e-4 M_P,
    pi_E = -2.7173e-2 M_P (tuning integration 1), where the committed class succeeds.
  The probe's identical rows (production mode) reproduce §1.5's outcomes and counts.

Nothing in the integrator was changed; which continuation, if any, is wanted is the user's to rule.

---

## 2. The wall (prompt 02, 2026-11-14 to 15)

**Tree:** the worktree on branch `power-law-wall` at `35f6359` plus prompt 02's files, all under
`prompts/power-law-wall/source/wall/` and `.documents/power-law-wall/wall-02/`; no production file changes. No Ray, no
store, no registry run. The scans used at most six processes (three for the map). Each output file records `uptime`
at its start and end; the one-minute load ranged from 3 to 34 over the work (the high values from other processes on
the machine). Scripts, what they compute and how to run each: [`source/wall/README.md`](../prompts/power-law-wall/source/wall/README.md).

**Measurements, not bounds.** Nothing below is a threshold, a recommendation or a reason to exclude a history. The
options for U3–U5 are stated with their evidence in §2.10; the user rules.

### 2.1 The copies, the wall and the units

- **The copies** (P6): `walls.py`, `born.py`, `stage1a_classical.py`, `stage2.py` and `stage2_validate.py`, from
  crossover at `d0fe64a`, `9da5162`, `9da5162`, `d0fe64a` and `00a4ffc` (each one's last change). Below their headers
  they are crossover's files except where marked "power-law-wall 02": `walls.PowerWall`; `stage2.solve` and
  `run_point` accept a wall object; `stage2.V4` reads any wall's `V4`; `solve`'s start cap is 1e15 times
  max(the entry point, 1) in place of 1e15, so that every run crossover's cap let through is unchanged.
- **`PowerWall(x, n)`**: U = V − V₀ = A φ^{−n}, ω_t = 1, x = E/V₀ at φ_t, L = ln(1 + x). Units of φ_t (the default,
  orchestrator note 02 ruling 2): φ_t = 1, A = E = 1/(n(n + 1)), V₀ = E/x; x = ∞ is the wall with no floor. Units of
  M (walls.py's): φ_t = x^{−1/n}, A = c = V₀ = φ_t^{n+2}/(n(n + 1)), E = cx.
- **The loop term in these units.** With field unit u and time unit 1/ω_t, ½V'''⟨δφ²⟩/(uω_t²) = ½Ũ'''Σ̃(ω_t/u)², and
  (ω_t/u)² = 8π²RẼ because R = ω_t⁴/(8π²E_phys) and Ẽ = E_phys/(u²ω_t²). So the background equation is
  φ'' = −U' − K V''' Σ with K = 4π²RE in any unit of field, born.py's G = RE/2 in the Born limit; stage2.py is
  unchanged in units of φ_t.
- **What the floor does.** In units of φ_t, U and its derivatives do not depend on x, so the classical bounce, the
  mode equation and the stage-2 system are the same at every x. x enters only through λ = −d ln V/dφ = n s/φ,
  s = U/(V₀ + U): the chain's k_ex and ℓ (and the production's) and the copied `run_point`'s grid references.
  `powerscan.run_point_power` sets the grid from the floor-free chain's m_ta and k_ex instead (§2.3, V6).
- **Derivatives** (`validate_wall.py` D): U, U', V'', V''', V'''' against central differences of the one below
  (h = 1e-4 φ), both units, n = 1, 2, x = 10 and 1e40 (φ_t units), 10 and 1e3 (M units), φ = 0.5–30 φ_t: worst
  7.0e-8 against a tolerance of 1e-6. V''(φ_t) = 1, U(φ_t) = E and U(φ_t) = xV₀ to 1e-15.
- **The units** (`validate_wall.py` U), units of φ_t against units of M:

  | quantity | points | difference | tolerance (reason) |
  |---|---|---|---|
  | S (241-point ln-k trapezoid) | n = 1, 2 at x = 10, 1e3 | ≤ 2.7e-11 relative | 1e-6 (DOP853 at rtol 1e-11) |
  | ℓ (`born.chain`) | n = 1: x = 1e3 ln R 40 (1.70274), x = 1e2 ln R 29.5 (0.56969); n = 2: x = 1e3 ln R 28.5 (1.86476), x = 1e2 ln R 20 (0.71339); at x = 10 neither unit has a chain (ln R 20 and 15) | ≤ 6.7e-16 | 1e-10 (the chain's 1e-12 on ℓ) |
  | stage-2 fraction (`run_point`) | n = 1: x = 10 ln R 10 (0.4540304688), x = 1e3 ln R 40 (0.6684185809); n = 2: x = 10 ln R 8 (0.4486221582), x = 1e3 ln R 25 (0.7685216224) | ≤ 1.2e-14, same step count | 1e-8 (one fixed-step RK4 in rescaled variables) |

- **The exponential stays reproducible** (`reproduce_exponential.py`, copies on crossover's `Wall` and `WallN`), to the
  digits crossover recorded: S(10) 1.3529e-03 (stage 1a); S of WallN(5, 2) by stage 2, loop off, 2.14335e-03; born.run
  on Wall(20.95) at ln R = 44.44, k_IR = 10 m_ta, 0.44982; stage2.run_point(7.885, 23.833): 0.71709, v_out/v_in 0.5319,
  M/φ_turn 1.678, 311 modes, 53265 steps. All equal.

### 2.2 The paths (§1.4)

`path_map.py` by `runs_map.sh`, 2026-11-14 23:41–23:42 (load 5.0 → 5.9), three at a time; outputs `wall-02/map/`,
summary `wall-02/map_summary.txt`. All twenty-five histories tuned in one integration from the tracking seed; none
failed. Every candidate of every history is in the one-loop regime, and every history parks while one-loop; no
mapped candidate is in the gap or classical.

| history | candidates | ln x range | ln R range | ℓ range | parks at N (candidate) |
|---|---|---|---|---|---|
| IP n=1, 4e-31, β 1.2, α 0.01 / 0.1 | 5 / 9 | 40.21–102.14 / 33.30–102.14 | 201.4–511.1 / 166.9–511.1 | 15.86–41.75 / 12.96–41.75 | 30.053 (4) / 32.046 (8) |
| IP n=1, 4e-31, β 2.0, α 0.01 / 0.1 | 85 / 49 | 55.00–106.04 / 56.08–106.04 | 275.4–530.6 / 280.8–530.6 | 22.05–43.37 / 22.50–43.37 | 30.621 (84) / 30.349 (48) |
| IP n=1, 4e-31, β 2.9, α 0.01 / 0.1 | 55 / 42 | 39.78–109.06 / 75.77–109.06 | 199.3–545.6 / 379.2–545.6 | 15.68–44.63 / 30.73–44.63 | 38.898 (54) / 29.695 (41) |
| IP n=1, 1e-33, β 2.0, α 0.01 / 0.1 | 85 / 49 | 55.00–106.04 / 56.08–106.04 | 299.3–554.5 / 304.7–554.5 | 24.05–45.37 / 24.50–45.37 | 30.621 / 30.349 |
| IP n=1, 1e-15, β 2.0, α 0.01 / 0.1 | 85 / 49 | 55.00–106.04 / 56.08–106.04 | 133.5–388.8 / 138.9–388.8 | 10.17–31.53 / 10.62–31.53 | 30.621 / 30.349 |
| IP n=2, 4e-31, β 1.2, α 0.01 / 0.1 | 5 / 9 | as n = 1 | 123.2–309.0 / 102.5–309.0 | 14.67–38.01 / 12.05–38.01 | as n = 1 |
| IP n=2, 4e-31, β 2.0, α 0.01 / 0.1 | 85 / 49 | as n = 1 | 167.6–320.7 / 170.8–320.7 | 20.25–39.48 / 20.66–39.48 | as n = 1 |
| IP n=2, 4e-31, β 2.9, α 0.01 / 0.1 | 55 / 42 | as n = 1 | 121.9–329.7 / 229.9–329.7 | 14.50–40.61 / 28.08–40.61 | as n = 1 |
| IP n=2, 1e-33, β 2.0, α 0.01 / 0.1 | 85 / 49 | as n = 1 | 191.5–344.7 / 194.8–344.7 | 23.26–42.48 / 23.67–42.48 | as n = 1 |
| IP n=2, 1e-15, β 2.0, α 0.01 / 0.1 | 85 / 49 | as n = 1 | 25.7–178.9 / 29.0–178.9 | 2.25–21.67 / 2.68–21.67 | as n = 1 |
| exp, 4e-31, β 2.0 / 2.9 / 3.05, α 0.1 | 49 / 42 / 61 | 56.08–106.04 / 75.77–109.06 / 91.24–123.61 | 87.3–142.4 / 109.4–145.6 / 126.4–161.2 | 19.06–33.36 / 24.86–34.19 / 29.25–38.17 | 30.349 / 29.695 / 26.668 |
| IP n=1 / n=2, 4e-31, β 3.05, α 0.1 | 61 / 61 | 91.24–123.61 | 456.6–618.4 / 276.3–373.4 | 37.19–50.71 / 33.91–46.08 | 26.668 (60) |

At each β and α, every power-law history (both n, every M) has the same candidates, at the same N and ln x, and parks
at the same candidate; where the exponential was mapped (laboratory M, α = 0.1, β = 2, 2.9, 3.05) it has them too
(01 found the same). The path formula ln R = ln(V₀/M⁴) + 2 ln(n(n + 1)) − ln 8π² + (1 + 4/n) ln x holds at
every power-wall candidate (1148) to **2.8e-13** at worst; crossover's exponential curve holds at the 152 exponential
candidates to 4.4e-12. The curves' intercepts and R = 1 points are note §0's: ln(V₀/M⁴) = 3.344645, 27.310504,
−138.475623; R = 1 at x = 0.930 / 0.426 (laboratory, n = 1 / 2), 7.70e-3 / 1.44e-4 (1e-33), 1.93e12 / 1.45e20 (1e-15).

**The Planck scale.** ln(k_ex/M_P) = `log_k_ex − ln units.PlanckMass`, Einstein frame. ln A(φ_t) = β(φ_t − φ_ref)/M_P
at the first impact is −1.3e-15 (laboratory M), −6.7e-17 (1e-33) and −6.7e-8 (1e-15) at n = 1, −1.4e-20, −2.6e-22 and
−2.6e-10 at n = 2: the Jordan-frame k differs by that factor. The estimate is note §0's formula at the first impact's
measured E; "note §0" is the planner's number where it gave one.

*(Added at the orchestrator's review, 2026-11-15.)* M_P here and throughout §2 is `units.PlanckMass`, the **reduced**
Planck mass (`CLAUDE.md`, "Units"); the full Planck mass is √(8π) ≈ 5.01 times larger, so ln(k_ex/M_P,full) is
ln(k_ex/M_P) − 1.612.

| history | first impact N | T_J (MeV) | ln(E/V₀) | classical | turnaround | max classical | max turnaround | # classical > 0 | estimate | measured − estimate | note §0 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IP n=1 4e-31 β 1.2 α 0.01 / 0.1 | 17.66785 | 231.124 | 102.1423 | 84.1988 | −1.3710 | 84.1988 | −1.3710 | 4/5, 7/9 | 84.1988 | −2.9e-7 | |
| IP n=1 4e-31 β 2.0 | 20.35210 | 746.688 | 106.0425 | 90.0491 | 1.2228 | 90.0491 | 1.2228 | 85/85, 49/49 | 90.0491 | +4.1e-7 | ≈ 89.8 (10³⁹) |
| IP n=1 4e-31 β 2.9 | 23.88782 | 1870.300 | 109.0554 | 94.5685 | 3.2266 | 94.5685 | 3.2266 | 54/55, 42/42 | 94.5685 | +2.1e-7 | |
| IP n=1 4e-31 β 3.05 α 0.1 | 20.96367 | 67669.439 | 123.6099 | 116.4001 | 12.9083 | 116.4001 | 12.9083 | 61/61 | 116.4001 | −4.4e-8 | |
| IP n=1 1e-33 β 2.0 | 20.35210 | 746.688 | 106.0425 | 96.0406 | 3.2124 | 96.0406 | 3.2124 | 85/85, 49/49 | 96.0406 | −1.4e-7 | |
| IP n=1 1e-15 β 2.0 | 20.35210 | 746.688 | 106.0425 | 54.5941 | −10.5424 | 54.5941 | −10.5424 | 9/85, 11/49 | 54.5941 | −3.1e-7 | |
| IP n=2 4e-31 β 1.2 | 17.66785 | 231.124 | 102.1423 | 33.8208 | −5.0839 | 33.8208 | −5.0839 | 3/5, 3/9 | 33.8208 | +2.8e-7 | |
| IP n=2 4e-31 β 2.0 | 20.35210 | 746.688 | 106.0425 | 37.7210 | −2.6510 | 37.7210 | −2.6510 | 11/85, 12/49 | 37.7210 | ≤ 5e-7 | ≈ 38.0 (3×10¹⁶) |
| IP n=2 4e-31 β 2.9 | 23.88782 | 1870.300 | 109.0554 | 40.7339 | −0.7715 | 40.7339 | −0.7715 | 22/55, 42/42 | 40.7339 | +2.8e-7 | |
| IP n=2 4e-31 β 3.05 α 0.1 | 20.96367 | 67669.439 | 123.6099 | 55.2883 | 8.3092 | 55.2883 | 8.3092 | 61/61 | 55.2883 | −2.3e-7 | |
| IP n=2 1e-33 β 2.0 | 20.35210 | 746.688 | 106.0425 | 43.7125 | 0.3355 | 43.7125 | 0.3355 | 38/85, 24/49 | 43.7125 | −4.7e-7 | |
| IP n=2 1e-15 β 2.0 | 20.35210 | 746.688 | 106.0425 | 2.2659 | −20.3036 | 2.2659 | −20.3036 | 1/85, 1/49 | 2.2659 | +2.0e-7 | ≈ 2.3 (10 M_P) |
| exp 4e-31 β 2.0 α 0.1 | 20.35210 | 746.688 | 106.0425 | −6.6657 | −8.7244 | −6.6657 | −8.7244 | 0/49 | −6.6657 | −4.8e-7 | ≈ −6.9 (10⁻³) |
| exp 4e-31 β 2.9 α 0.1 | 23.88782 | 1870.300 | 109.0554 | −5.1032 | −7.1476 | −5.1032 | −7.1476 | 0/42 | −5.1032 | −1.3e-7 | |
| exp 4e-31 β 3.05 α 0.1 | 20.96367 | 67669.439 | 123.6099 | 2.4245 | 0.4408 | 2.4245 | 0.4408 | 1/61 | 2.4245 | +1.6e-7 | ≈ 3.2 (25 M_P, EW) |

Where a row covers both α the first impact is the same history; the counts are given for α = 0.01, then 0.1. The
measured − estimate column is the printed digits (6 decimals) apart from ln s_t = −ln(1 + 1/x) ~ e^{−100}. The first
impact's E: ln E = ln x + ln V₀ = −170.59 (≈ 0.29 GeV⁴) at β = 2 and −153.02 (≈ 1.2×10⁷ GeV⁴) at β = 3.05, against note
§0's 0.3 GeV⁴ (taken from 01) and its EW estimate 7×10⁷ GeV⁴.

### 2.3 The validation (§1.3), before the scans

`validate_wall.py` → `wall-02/validation.txt` (2026-11-15 00:07–00:10, load 5.8 → 6.6, six processes);
`chain_vs_production.py` → `wall-02/chain_vs_production.txt`.

1. **The classical limit.** Stage 2 with the loop off (units of φ_t) against walls.py's independent test field (481-point
   ln-k trapezoid) at x = 1e40 (units of φ_t) and x = 1, 0.1 (units of M): ratio 1.000001 (n = 1, S = 1.026202e-2) and
   1.000002 (n = 2, 5.216159e-3) at all three; tolerance 1e-4 (crossover's V-A agreement and the two quadratures). With
   the loop on, frac/R at ln R = −8, −6, −4, −2, 0: 1.0258e-2, 1.0232e-2, 1.0044e-2, 8.742e-3, 2.862e-3 (n = 1) and
   5.2144e-3, 5.2032e-3, 5.1214e-3, 4.5476e-3, 1.7062e-3 (n = 2): it tends to S_n as R → 0, 4e-4 and 3e-4 short at
   ln R = −8.
2. **The Born limit.** Stage 2 cut at k_IR = 10 m_ta against born.run at the same k_IR (x = 1e40): ratio 1.00043
   (ℓ = 5) and 1.00028 (ℓ = 8) at n = 1, 1.00071 and 1.00040 at n = 2; tolerance 2e-3 (crossover's V1, 4e-4–1.0e-3).
   Ruling 3's overlap, born.py self-consistent (k_IR = √V''_max) against stage 2's physical infrared end: ratio 0.936,
   0.956, 0.975, 0.984, 0.990 at ℓ = 5, 8, 15, 25, 40 (n = 1) and 0.932, 0.954, 0.974, 0.984, 0.990 (n = 2), against
   crossover's 0.93–0.96 at ℓ = 4.9–9.5.
3. **Energy.** max |E_tot − E|/E along each run: ≤ 2e-8 in every validation run with stage 2's own grid, ≤ 1.9e-8 at
   every one of the 153 scan points on the curves, ≤ 7.4e-11 in the classical rows. Runs of the copied `run_point` at
   small x, whose grid follows the floor (item 6), reach 4.7e-7 (x = 100, ln R = 40) and, off the curves, 1.5e-2 to
   5e19 (§2.5): the energy check flags them.
4. **Convergence** (mode spacing halved, k_max doubled, k_min halved, step halved, EPS/10), at four points per n:

   | point | n = 1: largest change in f | n = 2 |
   |---|---|---|
   | one-loop, ℓ = 25 (n = 1), 24.2 (n = 2) | 4.5e-6 (k_min) | 5.0e-6 (k_min) |
   | one-loop, ℓ = 1.0 | 5.1e-4 (k_min) | 2.7e-4 (k_min) |
   | gap, ln R = 10 (n = 1), 8 (n = 2) | 2.0e-3 (k_max) | 5.8e-3 (k_min) |
   | gap, ln R = 3.25 (n = 1), 3.64 (n = 2) | 1.4e-4 | 6.3e-4 |

   The mode spacing, the step and EPS move f by ≤ 1.3e-6 everywhere. The scan's stated precision: ±1e-5 at ℓ ≳ 20,
   ±1e-3 near ℓ = 1, ±6e-3 in the gap.
5. **The chain against production.** (a) At all 1148 mapped power-wall candidates, `born.chain` on PowerWall(x, n) at
   the candidate's printed ln x and ln R against the history's `one_loop_turnaround` ℓ: worst 5.0e-6, tolerance 1e-5
   (the printed digits); no candidate where one has a chain and the other not. (b) Twelve candidates directly, through
   `InversePowerModel` in Planck units: ℓ and φ_ta/φ_t to 1.7e-14 relative at worst, tolerance 1e-9. (c)
   `powerscan.f_law` equals `drain_fraction` bitwise at 4000 inputs. No disagreement with the production wall search.
6. **Scale invariance.** S_n at x = 1e10, 1e20, 1e40, 1e100: bitwise equal (1.0262017159e-2, 5.2161509765e-3). ℓ at fixed
   R: equal at x ≥ 1e20 for ln R ≤ 300 (ln R ≤ 100 from 1e10). **The floor starts to matter for ℓ** (|ℓ(x) − ℓ_∞| >
   1e-3) below x ≈ 1.2–2×10³ (φ_ta/φ_t)ⁿ, at every ln R tested (n = 1: x = 10^4.5, 10^6, 10^10.5, 10^25 at ln R = 20,
   40, 100, 300; n = 2: 10^4.75, 10^5.75, 10^9.75, 10^25 at ln R = 15, 25, 60, 200), as ℓ(x) − ℓ_∞ ≈ ln s_ta ≈
   −(φ_ta/φ_t)ⁿ/x says. The stage-2 fraction by `run_point_power` does not depend on x (its inputs do not); by the
   copied `run_point` it equals that to 2.4e-5 at x = 1e4 and 3e-4 at 1e3 (n = 1, ln R = 40), and falls to 0.546
   against 0.669 at x = 100, where the floor-suppressed λ puts the grid's top below the production (n = 2, ln R = 25:
   0.7521 against 0.7686).

### 2.4 S on this wall (§1.5, U4)

`stage1a_power.py` → `wall-02/stage1a_S_n.txt` (2026-11-14 23:56–23:58, load 15.5 → 5.6).

**S_n does not depend on x.** The runs in units of M at x = 1e-3, 1e-2, 0.1, 0.3, 1, 3, 10, 100, 1e3 and the run in
units of φ_t at 1e10 give the same value to every printed digit:

| n | S_n | \|β\|²_max | at k/ω_t | the exponential's S(L_E) at x = 0.3 / 1 / 10 / 1e3 / 1e10 | S_n/S(L_E) there |
|---|---|---|---|---|---|
| 1 | **1.02620e-2** | 7.680e-2 | 0.165 | 1.004e-2 / 9.330e-3 / 5.875e-3 / 2.201e-3 / 3.663e-4 | 1.02 / 1.10 / 1.75 / 4.66 / 28.0 |
| 2 | **5.21615e-3** | 3.390e-2 | 0.207 | the same | 0.52 / 0.56 / 0.89 / 2.37 / 14.2 |

The quadrature (at x = 10, as crossover's V2q): the ln-k trapezoid on [1e-3, 10] with 121, 241, 481 points and on
[1e-4, 20] with 601 points agree to 3e-7 (n = 1) and 7e-7 (n = 2); the linear-k trapezoid is 0.10% and 0.10% high at
121 points, 0.006% at 481; EPS/100 moves S by 9e-7 relative; stage 2 with the loop off at 80 and 160 modes a decade
gives 1.0262021e-2 and 5.216159e-3. The exponential's S(L) tends to S_1 as L → 0 (S(0.3) = 1.0041e-2): its wall at
small L is the n = 1 power wall.

### 2.5 The one-loop fraction along the paths (§1.6, U3)

`scan_curves.py` → `wall-02/stage2_scan.txt`, tabled with the law in `wall-02/stage2_curves.md` (2026-11-14
23:59 – 2026-11-15 00:04, load 3.3 → 6.8, six processes; 153 curve runs in 1023 s of process time, 2–17 s each).
Six curves (laboratory M, 1e-33 and 1e-15 M_P; n = 1, 2), each from its largest mapped ℓ down to ℓ = 0.1 (or the
chain's end), eight gap points from the gap's entry to R = 1, and ln R = −0.5, −2, −5. "mapped" marks the one-loop
points with ℓ in the range the curve's histories reached; every other point is beyond where any mapped history goes.

- **The fraction is one function of ln R per n.** Points at the same ln R on different curves agree to the scan's
  precision (0.059815 at ln R = 490.17 on the laboratory and 1e-33 curves; 0.11572 at ln R = 250.87 on the laboratory
  and 1e-15 curves; the classical rows identical to six digits). What differs between curves at the same R is x, and
  with it the production's ℓ: on the 1e-33 curve ℓ = 1 sits at ln R = 58.8 (ℓ_∞ = 3.85, f = 0.469), on the laboratory
  curve at 29.2 (ℓ_∞ = 1.30, f = 0.869), on the 1e-15 curve at 25.9 (ℓ_∞ = 1.00, f = 0.941).
- **The gap entries** (the least ln R on the curve with a chain): ln R = 20.27, 13.70 (laboratory, n = 1, 2); 52.37,
  48.01 (1e-33); 17.36, 11.64 (1e-15), where ℓ_∞ is 0.34, 0.43 (laboratory), 3.03, 4.75 (1e-33) and 0.09, 0.14
  (1e-15). On the 1e-33 curve the floor removes the chain while the bounce is still deep in the one-loop regime.
- **Largest fractions.** 0.974 (ℓ = 0.4, laboratory n = 1; ℓ = 0.7, laboratory n = 2); 13 points did not come back out
  within the run (ℓ ≲ 1–1.5 on the laboratory and 1e-15 curves, and one gap point each on the 1e-33 curves at
  ln R = 22.9 and 15.0, where ℓ_∞ = 0.73 and 0.77).
- **The first impacts:** f = 0.0538 (ℓ = 44.63, n = 1) and 0.0611 (ℓ = 40.61, n = 2) at laboratory M; 0.0529 (45.37)
  and 0.0585 (42.48) at 1e-33; 0.0752 (31.53) and 0.1119 (21.67) at 1e-15.
- **The gap** falls with ln R: on the laboratory curve, n = 1, 0.874 at ln R = 19.0 to 0.140 at 3.8 and 0.0040 at 1.27;
  n = 2, 0.826 at 12.8 to 0.068 at 2.57 and 8e-4 at 0.86.
- **The loop parameter** ε_turn = V''''RE/2 at the turn is 10³⁹ (first impacts) to ~10² (ℓ ≈ 1), 3–10⁵ in the gap (the larger on the 1e-33 curves) and
  0.01–1.7 in the classical rows: outside the scheme's own expansion, as on the exponential.
- **The off-curve grid** (x = 1e2, 1e3, 1e4, 1e10, 1e40 at ln R = 10, 20, 40, 100 (n = 1) and 8, 15, 25, 60 (n = 2),
  by the copied `run_point`), against `run_point_power` at the same ln R: equal at x ≥ 1e10 (to the last digit at
  1e40); within 3e-4 at x = 1e3 and 1e4 wherever the copy's run is valid; at x = 100 within 8.4e-4 at the two lower
  ln R, and 0.546 against 0.669 (n = 1, ln R = 40) and 0.752 against 0.769 (n = 2, ln R = 25). At ln R = 100 (n = 1)
  and 60 (n = 2) and x ≤ 1e4 the copy's grid, set from the floor-suppressed λ, gives energies off by 5.4e-4 to 5e19:
  those six runs are invalid, not measurements of f.

The curve tables (`stage2_curves.md`) give, for each point, ln R, ln x, the regime, ℓ, ℓ_∞, f from the modes and from
the balance (equal to six digits at every point), v_out/v_in, back, ε_turn, |ΔE/E|, the run time, and the adopted
law at L_E and at L = n.

### 2.6 The adopted law against it (§1.7, U3)

`fit_law.py` → `wall-02/fit_law.txt`. Residual = law − stage 2, at the 140 curve points where the field came back out;
regime and ℓ the production's. "mapped": the one-loop points within the mapped ℓ range (ℓ ≥ 12–24, and n = 2 at
1e-15 from 2.25).

| option | n | one-loop ℓ ≥ 1: max / rms / mean (all) | mapped only | plateau ℓ < 1 | gap |
|---|---|---|---|---|---|
| (i) L_E = ln(1 + x) | 1 | 0.429 / 0.128 / +0.073 (34) | 0.021 / 0.012 / +0.011 (12) | 0.409 / 0.254 / +0.186 (5) | 0.806 / 0.294 / +0.099 (23) |
| | 2 | 0.524 / 0.156 / +0.070 (31) | 0.070 / 0.026 / +0.018 (15) | 0.500 / 0.212 / +0.130 (6) | 0.895 / 0.318 / +0.047 (23) |
| (ii) L = n | 1 | 0.276 / 0.088 / +0.008 | 0.005 / 0.004 / +0.004 | as (i) | 0.445 / 0.149 / +0.004 |
| | 2 | 0.411 / 0.127 / +0.021 | 0.100 / 0.028 / −0.010 | as (i) | 0.565 / 0.199 / +0.038 |
| (iii) L_M = x^{1/n} | 1 | 0.896 / 0.636 / +0.596 | 0.896 / 0.790 / +0.780 | as (i) | 0.839 / 0.399 / +0.198 |
| | 2 | 0.837 / 0.401 / +0.306 | 0.837 / 0.476 / +0.357 | as (i) | 0.739 / 0.378 / +0.158 |
| (iv) best constant L | 1 | L = 0.731: 0.253 / 0.087 / +0.001 | 0.004 / 0.004 / +0.004 | as (i) | L = 1.317: 0.445 / 0.140 / +0.046 |
| | 2 | L = 0.383: 0.293 / 0.118 / −0.010 | 0.158 / 0.046 / −0.020 | as (i) | L = 3.065: 0.565 / 0.193 / +0.074 |
| (v) refit | 1 | b = 1.0975: 0.254 / 0.087 / +0.001 | 0.004 / 0.004 / +0.004 | measured 0.49–0.89, mean 0.71 | G 0.0017, R0 −1.80, P 2.39: 0.445 / 0.139 / +0.053 |
| | 2 | b = 1.3627: 0.293 / 0.118 / −0.010 | 0.158 / 0.046 / −0.020 | measured 0.40–0.88, mean 0.77 | G 1e-4, R0 −3.95, P 3.52: 0.565 / 0.192 / +0.082 |

The exponential's own accuracy (crossover `fit_law.txt`): one-loop max 0.074 (rms 0.021), gap max 0.062 (rms 0.024).
The cap: the largest measured fraction is 0.974 (n = 1 and 2).

**By curve** (one-loop ℓ ≥ 1 / gap, max |res|): at L_E, 0.052 / 0.124 (laboratory), 0.429 / 0.806 (1e-33), 0.119 /
0.436 (1e-15) at n = 1, and 0.060 / 0.112, 0.524 / 0.895, 0.070 / 0.345 at n = 2; at L = n, 0.124 / 0.135,
0.276 / 0.445, 0.157 / 0.133 (n = 1) and 0.147 / 0.138, 0.411 / 0.565, 0.169 / 0.141 (n = 2). The 1e-33 curve's points
carry the largest residuals under every option: there the production's ℓ is lowered by the floor (by up to 3.0 at
n = 1 and 4.6 at n = 2 below ℓ_∞ = 20), and its "gap" covers ℓ_∞ up to 3.0 (n = 1) and 4.75 (n = 2). For reference only (not an option of §1.7), the law read at ℓ_∞ and its
regime: one-loop max 0.154 / 0.345 (L_E, n = 1 / 2) and 0.175 / 0.205 (L = n); gap 0.806 / 0.585 (L_E) and
0.135 / 0.141 (L = n).

**Classical points** (ln R = −0.5, −2, −5, every curve): f/(R S_n) = 0.473, 0.852, 0.998 (n = 1) and 0.523, 0.871,
0.996 (n = 2), the same on every curve. Against the exponential's table at L_E, f/(R S(L_E)) is 0.48–1.02 at
laboratory M and 1e-33 (n = 1), 0.27–0.52 there at n = 2, and 18.7–37.4 (n = 1), 25.7–45.9 (n = 2) at 1e-15, where
L_E ≈ 27–46.

### 2.7 The spectrum (§1.8, U5)

`spectrum.py` → `wall-02/spectrum.txt` (2026-11-15 00:06, load 3.6 → 6.5). k in units of the production's
k_ex = λ_t√E/2 at φ_t, k/k_ex = (k/ω_t)/(k_ex/ω_t), k_ex/ω_t = (s_t/2)√(n/(n + 1)), s_t = x/(1 + x). x_p by the grid
(300 points in k/k_ex on [1e-2, 12]) and by a parabola in ln k through the seven points round the peak; the slope by
a fit of ln|β|² on ln k over k/k_ex ∈ [0.02, 0.1]; κ by `kappa_fit`'s two-point rule with |β|² at x = 10 measured (and
again at EPS/1000, rtol 1e-13, atol 1e-17: "strict"), and by a straight-line fit of ln|β|² on k/k_ex over [3, 10].

| wall | x | k_ex/ω_t | \|β\|²_max | x_p grid / fit | slope below | \|β\|²(x = 10) (strict) | κ two-point (strict) | κ tail fit |
|---|---|---|---|---|---|---|---|---|
| power n = 1 | 1e40 | 0.35355 | 7.680e-2 | 0.466 / 0.466 | 1.34 | 3.81e-10 (3.80e-10) | 2.006 (2.006) | 2.145 |
| power n = 1 | 6.87 (laboratory, ln R = 10) | 0.30865 | 7.680e-2 | 0.537 / 0.533 | 1.37 | 6.06e-9 (6.08e-9) | 1.728 (1.728) | 1.858 |
| power n = 1 | 0.930 (laboratory, R = 1) | 0.17038 | 7.680e-2 | 0.972 / 0.966 | 1.47 | 2.82e-5 | 0.876 | 0.966 |
| power n = 2 | 1e40 | 0.40825 | 3.390e-2 | 0.512 / 0.507 | 1.76 | 1.52e-12 (1.12e-12) | 2.511 (2.544) | 2.713 |
| power n = 2 | 11.9 (laboratory, ln R = 10) | 0.37671 | 3.390e-2 | 0.550 / 0.549 | 1.78 | 9.77e-12 (9.63e-12) | 2.325 (2.326) | 2.504 |
| power n = 2 | 0.426 (laboratory, R = 1) | 0.12198 | 3.390e-2 | 1.677 / 1.695 | 1.92 | 2.23e-4 | 0.604 | 0.672 |
| exponential WallN(90, 1), exact k_ex | — | 0.49454 | 1.4286e-4 | 0.649 / 0.646 | 1.985 | 1.108e-20 (1.108e-20) | 3.967 (3.967) | 4.262 |
| exponential, k_ex = ω_t/2 | — | 0.5 | 1.4285e-4 | 0.634 / 0.639 | 1.984 | 6.85e-21 (6.88e-21) | 4.012 (4.012) | 4.311 |

The peak in units of ω_t is the same at every x (0.1646 at n = 1, 0.2068 at n = 2); x_p and κ move with x only
through k_ex/ω_t. The method reproduces the exponential's constants: slope 1.985 (the note's 1.99), |β|² at 5 ω_t
1.1e-20 (the note's "about 10⁻²⁰"), κ = 3.967 against `KAPPA` = 3.966, and k_peak/ω_t = 0.320 (Table tab:beta 0.316,
x_p = 0.62 from 0.31/0.5 against 0.64 here). On the power wall at n = 2 the default tolerances leave |β|²(x = 10)
≈ 1e-12 36% above the strict value; κ changes by 0.03.

### 2.8 The breakage record (§3)

Each mutation was made in a scratch copy of `source/wall/` and the checks of §2.3 run on it (diffs in log 02 §5).

| mutation | caught by | not caught by |
|---|---|---|
| G doubled (`K = 8π²RE` in `stage2.solve`) | the Born limit (ratio 0.515, 0.512 against 1 ± 2e-3); energy (\|ΔE/E\| 0.50 in the Born and units rows, 1.3e-4 at ln R = −6) | the classical limit (loop off), S, ℓ, the units comparison (both units broken alike) |
| V''' of the wrong sign in `PowerWall` | D (worst 2.0, against 1e-6); energy (\|ΔE/E\| 0.12–0.13 in the units rows, 5e-4 in the Born rows, 6.7e-5 at ln R = −6) | the classical limit (ratio 0.999986, within 1e-4: the test field reads V''' only through ω' in its adiabatic projection), S, ℓ, the Born ratio (1.00095, 1.00131) |
| the floor converted wrongly (`V₀ = A x^{−1−2/n}`, walls.py's M-unit value, in units of φ_t) | the units comparison of ℓ (difference 0.54, 0.75 against 1e-10) and of f (3e-4, 8e-5 against 1e-8); D's U(φ_t)/(xV₀) (100 to 1e80 against 1); the chain against production ((a) 8.1e-3 against 1e-5, (b) 3.9e-4 against 1e-9) | S, the classical and Born limits, energy (the dynamics do not read V₀) |

### 2.9 README §0.3's estimates, checked

- **φ_t at early times** ("E/V₀ of order 10⁶⁰; φ_t ≈ 10⁻⁶⁰M (n = 1), 10⁻³⁰M (n = 2), against M/138"): at the QCD
  kick E/V₀ = 10^44.4–10^47.4 (ln x = 102.1–109.1, β = 1.2–2.9), so φ_t ≈ 10⁻⁴⁶M (n = 1) and 10⁻²³M (n = 2); at the
  electroweak kick (β = 3.05) 10^53.7, φ_t ≈ 10⁻⁵⁴M and 10⁻²⁷M. The exponential's φ_t = M/L with L = 102–124. Every
  first impact is below 10⁻⁸ M_P (the largest mapped φ_t is 1.1e-27 M_P, n = 2 at 1e-15 M_P). (§2.2, map.)
- **The drain's variables** ("ℓ, L and R may sit where the law was never measured"): ℓ = 38–46 at the first impacts
  (51 at β = 3.05, n = 1) against the exponential's 33–38; ln R = 102–618 against 87–161; L_E = 33–124. Stage 2 there:
  §2.5; the law's residuals: §2.6.
- **The spectrum constants** (x_p = 0.62, κ from the exponential): x_p = 0.47 (n = 1), 0.51 (n = 2) at large x, 0.53–1.70
  near the floor; κ = 2.0 and 2.5 at large x, 0.6–1.7 near the floor, against 0.62 (0.65 remeasured) and 3.97 (§2.7).
- **The thresholds** (reported only, P10; 04 measures them in histories): every mapped first impact is far below the
  unresolved-reflection threshold 10⁻⁸ M_P (φ_t ≤ 1.1e-27 M_P at every candidate); every mapped history parked
  (N = 26.67–38.90) while one-loop; `WALL_ROUNDING_QUANTA` and the surfing thresholds are not measured here.

### 2.10 For the user: U3–U5, and the Planck scale

The options and the evidence. The user rules; nothing here is acted on.

- **U3, the law on this wall.** Stage 2 on V₀[1 + (M/φ)ⁿ] gives a fraction that depends on ln R and n alone (§2.5);
  the law reads (ℓ, L, ln R), and on this wall the production's ℓ depends on x through the floor where
  x ≲ 10³(φ_ta/φ_t)ⁿ (§2.3 item 6). The residuals of (i)–(v) are §2.6's table; against the exponential's 0.074
  (one-loop) and 0.062 (gap):
  - (i) L_E: within 0.07 at the mapped points (n = 1: 0.021; n = 2: 0.070) and on the laboratory curve's one-loop rows
    (0.052, 0.060); 0.42–0.90 at worst on the 1e-33 curve and in the gap.
  - (ii) L = n: 0.005 (n = 1) and 0.100 (n = 2) at the mapped points; gap 0.13–0.14 on the laboratory and 1e-15
    curves, 0.45–0.57 on the 1e-33 curve.
  - (iii) L_M: off by 0.3–0.9 throughout.
  - (iv) best constant L: one-loop L = 0.73 / 0.38, gap L = 1.32 / 3.07; no better than (ii) at the worst point.
  - (v) the refit (b = 1.10 / 1.36; gap G, R0, P per n): one-loop max 0.25 / 0.29, gap 0.45 / 0.57, both set by the
    1e-33 curve; the plateau measured 0.40–0.89 and the cap 0.974.
  - Every option's worst residual is on the 1e-33 curve, below the mapped range, where the floor shifts ℓ and the
    regime but not f.
- **U4, S on this wall.** S_n is a constant: S_1 = 1.02620e-2, S_2 = 5.21615e-3 (§2.4), against the exponential's
  S(L_E), which is within 2% (n = 1) at x ≤ 0.3, 1.1–4.7 times smaller than S_1 at x = 1–1e3 and 28 times smaller at
  1e10; at n = 2 S_2 is 0.52–0.56 of S(L_E) at x ≤ 1. The classical points follow R S_n (0.998 and 0.996 of it at
  ln R = −5; 0.47–0.52 at ln R = −0.5, where the dressing deforms the bounce, as crossover's V6 found).
- **U5, the spectrum.** x_p and κ against the exponential's (§2.7): large x, x_p = 0.47 / 0.51 and κ = 2.01 / 2.51
  (two-point; tail fit 2.14 / 2.71) against 0.62–0.65 and 3.97; near the floor both move with k_ex/ω_t = (s_t/2)
  √(n/(n + 1)). The rise below the peak is k^1.3 (n = 1) and k^1.8 (n = 2) on [0.02, 0.1] k_ex, against k^1.99.
- **Beside these, the Planck scale** (§2.2): at laboratory M the classical k_ex at the first impact is e^84–e^116 M_P
  (n = 1) and e^34–e^55 M_P (n = 2), and the turnaround's e^−1.4–e^12.9 and e^−5.1–e^8.3; at 1e-15 M_P, n = 2, e^2.27 and
  e^−20.3; on the exponential e^−6.7 (β = 2), e^−5.1 (β = 2.9) and e^2.42 (β = 3.05, the electroweak kick).

## 3. The law on this wall (prompt 03, 2026-11-15)

**Tree:** the worktree on branch `power-law-wall` at `7c60226` plus prompt 03's files. No Ray, no store, no registry run.
The spectrum tables and their measurements used six processes; the tool-route histories ran one at a time. Each output
in `.documents/power-law-wall/law-03/` records `uptime` at its start and end; the one-minute load ranged from 3 to 13
over the work. Scripts and how to run each: [`source/wall/README.md`](../prompts/power-law-wall/source/wall/README.md),
"Prompt 03".

**Measurements, not bounds.** The fit's residuals, the step at ln R = 0, the spectrum's variation over L and E/V₀, and old
against new are reported; none is a threshold.

### 3.1 The fit f_n(ln R) (R7; P18)

`fit_fn.py` on 02's `stage2_scan.txt` (`law-03/fit_fn.txt`, 11:04, load 3.0; first run at 09:54, load 8.0, with the
same numbers). The points: the `curve-*` rows with the field back out and production's regime not classical, 62
(n = 1) and 60 (n = 2). The form y = −ln(1 − f): rising branch y = G (ln R − R₀)^P fitted on 0.3 < ln R < 20 (22 | 23 points), falling branch y = A/(ln R − c) on ln R > 28 (40 | 32
points), unweighted least squares in f, switched where the two y are equal: the windows, weights and starting points of
the orchestrator's scratch fit (note 03 §0), kept so that the law is the one R7 was ruled on.

| n | G | R₀ | P | A | c | switch ln R_× |
|---|---|---|---|---|---|---|
| 1 | 2.86769e-3 ± 3.3e-3 | −1.27414 ± 1.64 | 2.22126 ± 0.34 | 27.9671 ± 0.14 | 14.9835 ± 0.14 | 23.08943252571177 |
| 2 | 2.85913e-3 ± 3.8e-3 | −1.10903 ± 1.45 | 2.45613 ± 0.42 | 19.6100 ± 0.057 | 10.9039 ± 0.096 | 16.710187997698853 |

The constants are the fit's rounded to six significant figures, and are production's literals; the ± are curve_fit's
standard errors (G, R₀ and P are strongly correlated). The switch is the root of y_rise = y_fall at the literals; f's
step across it is 1.1e-16 (n = 1) and 0 (n = 2). f peaks there at 0.9683 | 0.9659.

Residuals (law − stage 2) at the literals, beside note 03 §0's:

| | n = 1: max / rms / mean (points) | §0 | n = 2: max / rms / mean (points) | §0 |
|---|---|---|---|---|
| all non-classical back-out | 0.0541 / 0.0191 / −0.0004 (62) | 0.054 / 0.019 | 0.0557 / 0.0215 / −0.0016 (60) | 0.056 / 0.022 |
| the mapped histories' points | 0.0042 / 0.0033 / −0.0033 (12) | 0.004 | 0.0030 / 0.0017 / −0.0012 (15) | 0.003 |
| one-loop ℓ ≥ 1 | 0.0090 / 0.0035 (34) | | 0.0309 / 0.0075 (31) | |
| one-loop ℓ < 1 | 0.0270 / 0.0187 (5) | | 0.0336 / 0.0250 (6) | |
| gap | 0.0541 / 0.0299 (23) | | 0.0557 / 0.0311 (23) | |
| laboratory curve: one-loop ℓ ≥ 1, ℓ < 1, gap | 0.0090, —, 0.0541 | 0.009, 0.054 | 0.0309, 0.0336, 0.0483 | 0.031, 0.034, 0.048 |
| 1e-33 M_P curve | 0.0042, 0.0040, 0.0425 | 0.004, 0.004, 0.043 | 0.0017, 0.0018, 0.0380 | 0.002, 0.002, 0.038 |
| 1e-15 M_P curve | 0.0047, 0.0270, 0.0508 | 0.005, 0.027, 0.051 | 0.0221, 0.0318, 0.0557 | 0.022, 0.032, 0.056 |

The unrounded constants give the same residuals to the four digits printed. Beside it, the exponential law's own
accuracy: 0.074 one-loop, 0.062 gap (crossover's `fit_law.txt`).

- **Not back out** (law − measured): n = 1, seven points, −0.0177 to +0.0114; n = 2, six points, −0.0491 to +0.0262.
- **The grid rows** back out with |ΔE/E| ≤ 1e-8 (not fitted): n = 1, 20 rows, max 0.0165 (rms 0.0115, mean +0.0094);
  n = 2, 15 rows, max 0.0300 (rms 0.0225, mean +0.0081), the largest at ln R = 8 (gap) on every copy of x.
- **The step at ln R = 0:** f_n(0⁺) = 0.00489983 (n = 1) and 0.00367977 (n = 2), against R S_n = 0.010262 and
  0.00521615 at ln R = 0: ratios 0.4775 and 0.7055. The classical points: f/(R S_n) = 0.473 | 0.523 at ln R = −0.5,
  0.852 | 0.871 at −2 and 0.998 | 0.996 at −5 (02's measurement, the same on each curve).

### 3.2 The law's dispatch (R7; P15–P17)

- **The drain models** (`CosmologyConcepts/drain_model.py`): `DRAIN_MODEL_ONE_LOOP_PL = "one-loop-measured-pl-v1"`, whose
  law is the one-loop law; `DRAIN_MODELS = ("fixed", "one-loop-measured-v1", "one-loop-measured-pl-v1")`; `model_of_law`
  written out, still `one-loop-measured-v1` for the one-loop law. The regimes (`REGIME_*`, `DRAIN_REGIMES`) moved here
  from `ScalarHistory.py`, which imports them, so that a model can name them without a cycle; every `SM.REGIME_*` is the
  same object.
- **The hooks** (`AbstractModel`, no generic default): `_drain_fraction(normalization, regime, ell, log_E, log_R)` (base
  raises `NotImplementedError` naming the class; `drain_fraction_implemented`; `RealizedModel.drain_fraction(regime,
  ell, log_E, log_R)`), and the property `one_loop_drain_model` (base raises `NotImplementedError`; a model whose law is
  undefined at its parameters raises `ValueError`; `RealizedModel.one_loop_drain_model`). `ExponentialModel` keeps
  `_drain_L` and names `one-loop-measured-v1`; `InversePowerModel` defines `_drain_fraction` and names
  `one-loop-measured-pl-v1` at n = 1, 2, and raises `ValueError` from both at any other n.
- **The law** (`InversePowerModel`): `DRAIN_PL_S`, `_G`, `_R0`, `_P`, `_A`, `_C`, `_SWITCH`, keyed by n, each with its
  source; unassessed 0; classical, or ln R ≤ 0, exp(ln R) S_n; one-loop and gap 1 − e^{−y}, the rising y below the switch
  and the falling at and above it. It reads neither ℓ, ln E nor the normalization.
- **The check** (`check_drain_law_model`): under the one-loop law, refuses a model with neither or both of `_drain_L` and
  `_drain_fraction` ("the one-loop drain law needs exactly one of the model's wall variable L, _drain_L(normalization,
  log_E), and its own fraction, _drain_fraction(normalization, regime, ell, log_E, log_R); C implements neither|both"),
  one whose `one_loop_drain_model` raises ("the one-loop drain law: " + the hook's message, e.g. "InversePowerModel (n=3)
  has no one-loop drain law: one_loop_drain_model is defined at n = 1 and 2 only (power-law-wall README §6.5 R7) (…)"),
  or one naming a model whose law is not the one-loop law. The fixed law checks nothing.
- **The loop:** `drain_law_inputs` gives L = None on a model without `_drain_L`; `drain_alpha_effective(…, model=None,
  log_E=None)` takes the L law when the model is None or defines `_drain_L`, the model's own fraction otherwise, with the
  cap and gate unchanged; the record's `model` is the model's `one_loop_drain_model` under the one-loop law,
  `DRAIN_MODEL_FIXED` under the fixed law. `drain_fraction`, its constants and the S table are unchanged.

### 3.3 The spectrum tables (R8; P19–P21; R9, R10)

`spectrum_table.py` (`law-03/spectrum_tables.txt`, 10:28–10:53, load 3.2 to 13.2), walls.Bounce and walls.beta_squared
unchanged. x = k/k_ex, k_ex the production's: 0.494535 ω_t on WallN(90, 1) (U4-U17-v1 took ω_t/2), and
(s_t/2)√(n/(n + 1)) = 0.353553 | 0.408248 ω_t on PowerWall(1e40, n), s_t = 1 to double precision.

- **The precision.** At walls.py's setting (rtol 1e-11, atol 1e-14, EPS 1e-5) |β| carries an absolute error near 1e-7,
  1e-3 of |β|² where the tables end; the tables are computed at the strict setting of 02's `spectrum.py` (rtol 1e-13,
  atol 1e-17, EPS 1e-8).
- **The low-k power** d ln|β|²/d ln k (centred difference over k(1 ± 0.02)):

  | k/ω_t | 1e-2 | 1e-3 | 3e-4 | 1e-4 | 3e-5 | 1e-5 |
  |---|---|---|---|---|---|---|
  | exponential, walls.py's | 1.9976 | 2.0008 | 2.0069 | 2.0529 | 2.9522 | −5.1339 |
  | exponential, strict | 1.9976 | 2.0000 | 2.0000 | 2.0004 | 1.9982 | 2.0974 |
  | n = 1, walls.py's | 1.4353 | 1.6638 | 1.7272 | 1.7562 | 1.7279 | 1.7389 |
  | n = 1, strict | 1.4353 | 1.6649 | 1.7223 | 1.7591 | 1.8113 | 2.1739 |
  | n = 2, walls.py's | 1.8729 | 1.9828 | 1.9931 | 2.0421 | 2.1213 | 2.3856 |
  | n = 2, strict | 1.8717 | 1.9863 | 1.9959 | 1.9991 | 1.9981 | 0.9251 |

  The precision floor, where the power stops tending to 2: near 1e-4 ω_t at walls.py's setting (note 03 §0), near 1e-5
  ω_t at the strict one. R8's lower edge, 1e-3 ω_t, is above both.
- **x_hi:** the smallest of 3, 3.5, 4, … at which the tail, as the model continues it, carries at most 1e-3 of x³s.
  The measured data alone give 3.5, 6.5 and 5.0 (shares 7.5e-4, 6.8e-4, 9.9e-4); under the model those carry 1.2e-3,
  7.4e-4 and 1.3e-3, so the tables end at 4.0, 6.5 and 5.5.

| wall | x_lo | x_hi | nodes | spline at midpoints (max / rms) | monotone either side of the peak | x_p (parabola) | κ_hi (direct difference) | tail share | J | M₁/M₃ | M₄/M₃ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| exponential | 0.0020221 | 4.0 | 153 | 2.75e-6 / 2.5e-7 | yes (6081 points) | 0.646803 (0.644555) | 4.191348 (4.191444) | 2.22e-4 | 0.835827 | 0.828542 | 1.383507 |
| power n = 1 | 0.0028284 | 6.5 | 156 | 2.28e-6 / 2.0e-7 | yes (6201) | 0.465750 (0.464618) | 2.157451 (2.157501) | 7.43e-4 | 0.591750 | 0.464252 | 2.112967 |
| power n = 2 | 0.0024495 | 5.5 | 156 | 2.34e-6 / 2.1e-7 | yes (6201) | 0.506743 (0.505470) | 2.720202 (2.720259) | 4.47e-4 | 0.689202 | 0.608930 | 1.776235 |

The moments are at c_a = 0 (the script's trapezoid; the module's quadrature agrees to 1e-6). The share of x³s below
x_lo is below 1e-14 on every wall.

### 3.4 The module (P19, P20)

- `spectrum(wall, assoc_scale=1.0)`, the wall required: `WALL_EXPONENTIAL` ("exponential"), `WALL_POWER_N1`
  ("power-n1"), `WALL_POWER_N2` ("power-n2"); an unknown wall is a ValueError naming the three. `SPECTRUM_MODELS` gives
  each its string, "R8-exponential-v1", "R8-power-n1-v1", "R8-power-n2-v1", which is `QuantaSpectrum.model` and the
  `spectrum_model` column of every quanta output (no column added). `SPECTRUM_MODEL` is gone, so a reader that still names
  it fails loudly.
- The tables are literals (ln|β|² at the nodes, X_LO, X_HI, k_ex/ω_t), each with its source; s_dir = x² below x_lo (the
  normalization), the not-a-knot spline in (ln x, ln s) on the table, s(x_hi) e^{−κ_hi(x − x_hi)} above, κ_hi from the
  spline's end derivative; x_p the spline's maximum; s_assoc = C₁/x to x_p and (C₁/x) s_dir/s_dir(x_p) above, C₁ = 49 x_*
  s_dir(x_*): 9.5597e-6 (exponential; 49 x_*³ = 9.5605e-6), 7.3932e-6 (n = 1), 9.3848e-6 (n = 2).
- Moments and kernel nodes by composite Gauss–Legendre on the pieces between the table's nodes, x_p and x_hi (and their
  images times e^u for Ω), with the asymptotes of the module docstring. OMEGA_U_MAX = 9 (it was 4) and OMEGA_U_STEP =
  0.01 (it was 0.005): the asymptote's remainder bound at u = 9 is below 1.2e-15 of M₅ and 1e-18 of M₂ on every wall
  (3.6e-4 at u = 8). G's grid is unchanged.
- **Against direct quadrature**, every wall at c_a = 1, 0 and 10: moments to 1e-12; G (37 points across the grid, its
  ends and beyond) within 1e-10, the largest 9.7e-11 (n = 1); Ω (31 points over ±13) within 4.8e-10 (exponential); the
  quadratures' stated errors 1.1e-14. The unit tests assert 1e-7 (ruling 5).
- `PEAK_SPECTRUM` is unchanged. `X_PEAK`, `KAPPA`, `KAPPA_BY_L`, `BETA2_MAX_BY_L`, `kappa_fit`, the fit's constants and
  the closed forms are removed; no reader used them (two tool tests' direct quadratures did, and are re-pinned, §3.8).

### 3.5 The shapes over L and over E/V₀ (R9)

At c_a = 0, each table built by the same rule (step 0.05):

| exponential, WallN(L, 1) | k_ex/ω_t | x_p | J | M₁/M₃ | κ_hi | against L = 90: x_p, J, M₁/M₃, κ_hi |
|---|---|---|---|---|---|---|
| L = 40 | 0.487950 | 0.62636 | 0.83030 | 0.82383 | 4.0365 | ×0.9684, 0.9934, 0.9943, 0.9631 |
| L = 70 | 0.493007 | 0.64198 | 0.83464 | 0.82764 | 4.1541 | ×0.9925, 0.9986, 0.9989, 0.9911 |
| L = 90 | 0.494535 | 0.64680 | 0.83583 | 0.82854 | 4.1913 | ×1 |
| L = 110 | 0.495516 | 0.64992 | 0.83656 | 0.82906 | 4.2157 | ×1.0048, 1.0009, 1.0006, 1.0058 |
| L = 134 | 0.496310 | 0.65245 | 0.83713 | 0.82944 | 4.2356 | ×1.0087, 1.0016, 1.0011, 1.0106 |

On the power wall, measured at each E/V₀ in its own x (walls.py's units of M below 1e40):

| wall, x = E/V₀ | s_t | x_p | J | M₁/M₃ | κ_hi | against the table: x_p, J, M₁/M₃, κ_hi | ln\|β\|² against the table at x s_t / at x (max) |
|---|---|---|---|---|---|---|---|
| n = 1, 1e40 | 1 | 0.46575 | 0.59175 | 0.46425 | 2.1575 | ×1 | 4.4e-16 / 0 |
| n = 1, 1e3 | 0.9990 | 0.46622 | 0.59116 | 0.46332 | 2.1552 | ×1.0010, 0.9990, 0.9980, 0.9990 | 3.1e-7 / 0.01 |
| n = 1, 10 | 0.9091 | 0.51232 | 0.53795 | 0.38368 | 1.9595 | ×1.1000, 0.9091, 0.8264, 0.9082 | 2.4e-6 / 1.24 |
| n = 1, 1 | 0.5000 | 0.93150 | 0.29588 | 0.11606 | 1.0787 | ×2.0000, 0.5000, 0.2500, 0.5000 | 5.0e-8 / 6.84 |
| n = 1, 0.9302 (lab, R = 1) | 0.4819 | 0.96647 | 0.28517 | 0.10781 | 1.0397 | ×2.0751, 0.4819, 0.2322, 0.4819 | 7.4e-8 / 6.98 |
| n = 2, 1e40 | 1 | 0.50674 | 0.68920 | 0.60893 | 2.7202 | ×1 | 1.8e-15 / 0 |
| n = 2, 1e3 | 0.9990 | 0.50725 | 0.68851 | 0.60771 | 2.7173 | ×1.0010, 0.9990, 0.9980, 0.9989 | 3.0e-7 / 0.01 |
| n = 2, 10 | 0.9091 | 0.55742 | 0.62655 | 0.50325 | 2.4717 | ×1.1000, 0.9091, 0.8264, 0.9086 | 2.0e-6 / 1.34 |
| n = 2, 1 | 0.5000 | 1.01349 | 0.34460 | 0.15223 | 1.3561 | ×2.0000, 0.5000, 0.2500, 0.4985 | 3.4e-7 / 7.28 |
| n = 2, 0.4261 (lab, R = 1) | 0.2988 | 1.69601 | 0.20592 | 0.05436 | 0.8102 | ×3.3469, 0.2988, 0.0893, 0.2978 | 2.6e-7 / 9.53 |

The measured spectrum at each E/V₀ is the reference table read at x s_t, to 2.4e-6 in ln|β|²: the table's error in x
is the factor 1/s_t, exactly, and x_p, J, M₁/M₃ and κ_hi move as 1/s_t, s_t, s_t² and s_t. Measured, not acted on (R9).

### 3.6 Old against new on the exponential

Both normalized with s_dir = x² at small x (U4-U17-v1's x is k/(ω_t/2), the table's k/(0.494535 ω_t)):

| | c_a = 1: U4-U17-v1 | R8-exponential-v1 | new/old | c_a = 0: new/old |
|---|---|---|---|---|
| I | 9.300716e-2 | 1.240385e-1 | 1.3336 | 1.3336 |
| J = M₂/M₃ | 1.056044 | 0.8358392 | 0.7915 | 0.7915 |
| M₁/M₃ | 1.306207 | 0.8285867 | 0.6343 | 0.6343 |
| ln s(x_p) | −0.9560315 | −1.975485 | 2.0663 | 2.0664 |
| x_p | 0.62 | 0.6468034 | 1.0432 | 1.0432 |
| G(0.01) | 6.530617e-5 | 4.142724e-5 | 0.6344 | 0.6343 |
| G(1) | 0.4817193 | 0.3266532 | 0.6781 | 0.6781 |
| G(10) | 9.615863 | 7.427097 | 0.7724 | 0.7724 |
| Ω(0.5) | 0.1994639 | 0.1055976 | 0.5294 | 0.5294 |
| Ω(1) | 0.1574770 | 0.1093577 | 0.6944 | 0.6944 |
| Ω(2) | 2.683541e-2 | 3.351884e-2 | 1.2491 | 1.2503 |

The relativistic-quanta analysis of the exponential moves by these when it is repeated after this campaign (R8,
convention 8).

### 3.7 The gate Law, item by item

1. **The law at synthetic inputs** (`law-03/gate_law.txt`, 11:04, load 3.0): `InversePowerModel`'s fraction equals
   `fit_fn.fraction_power` bit for bit at every one of 122019 evaluations per (n, units), n = 1, 2, under Planck_units
   and GeV_units, over ln R ∈ [−20, 600] (62001 grid points, 0 and its float neighbours, the switches and theirs; the
   classical regime at ln R ≤ 0, one-loop and gap above). The unit test `test_power_wall_law` (d) asserts the same on its
   own grid.
2. **02's maps, candidate by candidate:** at all 1148 candidates of the twenty-two `map/ip-*.txt` files (the prompt's
   twenty and the two at β = 3.05 that 02's amended map added; every candidate one-loop), production's α_eff at A = 1,
   w_gate = 1, through `drain_alpha_effective` with the model, equals min(0.99, `fraction_power`) bit for bit.
3. **The exponential bitwise:** `test_golden_history`, `test_golden_reflecting_history`, `test_measured_drain_law`,
   `test_drain_law`, the tool tests under both laws (`test_history_and_bbn`, `_alpha`, `_drain_law`, `test_surfing_scan`)
   and the fingerprint goldens (in `ObjectFactories/tests`) pass unedited. The exponential's tool route below is identical
   to the base's, timings masked.
4. **The tool route** (`law-03/tool_route_*.txt`, 10:54–10:55, load 8.9 to 5.8; 16–17 s each), `tools/history_and_bbn.py
   2.0 4e-31 --drain-law one_loop --alpha 1`, tuned in one integration from the tracking seed:

   | | injections (regimes) | Σ α_eff | first impact ln R, α_eff | ΔN_eff 1 MeV / today | parking N (ln T_J) | drain.model |
   |---|---|---|---|---|---|---|
   | n = 1 | 37 (one-loop 37) | 3.102674 | 530.575, 0.052798 | 0.0285519 / 0.030891 | 30.9009 (−9.90666) | one-loop-measured-pl-v1 |
   | n = 2 | 48 (one-loop 48) | 4.673102 | 320.687, 0.061340 | 0.0329852 / 0.0345102 | 29.654 (−8.81398) | one-loop-measured-pl-v1 |
   | exponential | 45 (one-loop 45) | 5.315675 | 142.366, 0.079630 | 0.041888 / 0.044624 | 30.1253 (−9.19825) | one-loop-measured-v1 |

   The power-law records have L None at every candidate. Σ α_eff is from a scratch capture of the same tuned payloads
   (not committed). The exponential's output equals the base's (`git archive 7c60226`, run from scratch) line for line
   with the timings masked.
5. **The spectrum** as R8 rules on every wall (§3.3, §3.4): `test_quanta_spectrum` rewritten (23 tests), the tools' quanta
   tests pass, re-pinned only where the spectrum moved (§3.8).

### 3.8 The tests that moved

In the three quanta modules (P22): `test_quanta_spectrum.py` is rewritten for the walls (14 tests → 23); in
`test_quanta_measures.py` the direct-quadrature helpers take the exponential wall's shape and breaks (`qs.shape(x, c)`,
`qs.X_PEAK`, `qs.KAPPA` → `qs.shape(WALL, x, c)`, the wall's x_lo, x_p, x_hi and κ_hi), ln n_g's reference reads
`qs.x_peak(WALL)`, and `spectrum_model` is `qs.spectrum_model(WALL)` (was `qs.SPECTRUM_MODEL`); in
`test_quanta_figures.py` `qs.SPECTRUM_MODEL` → `qs.spectrum_model(qf.SPECTRUM_WALL)` (two places). No tolerance moved.
Outside them, where the prompt's own change contradicts an assertion: the footers' "U4-U17-v1" → "R8-exponential-v1"
(`test_plot_quanta_history`, `test_plot_quanta_grid`); `DRAIN_MODELS` gains its third value
(`test_drain_store_records`); `model_of_law` inverts `law_of` on the first two models only (`test_a_values_option`); the
one-loop refusal of `InversePowerModel` moves from n = 2 to n = 3 (`test_inverse_power_model` (g), `test_model_option`
(4)). No test that pins an exponential history, golden or fingerprint is edited.
