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
