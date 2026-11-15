# Log 03 — the law on this wall

**Subject:** the power wall's one-loop drain law, f_n(ln R) above R·S_n, in `InversePowerModel` under
`one-loop-measured-pl-v1`, through two new model hooks; the spectrum's direct piece as measured tables on three walls,
the exponential's included (R6–R10).
**Commit:** this commit, on branch `power-law-wall`, on top of `7c60226`
**Date:** 2026-11-15, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Law holds (§4). No stop was reached. Six tests outside the three quanta modules
were edited because the prompt's own change contradicts what they pinned (§2.2 D12); none pins an exponential history,
golden or fingerprint.
- **The fit** reproduces note 03 §0's: worst 0.0541 | 0.0557 over all points, 0.0042 | 0.0030 over the mapped ones
  (n = 1 | 2), against §0's 0.054 | 0.056 and 0.004 | 0.003.
- **The law** equals its reference bit for bit at 122019 synthetic inputs per (n, units) and at all 1148 candidates of
  02's maps; the exponential's goldens, tool tests and fingerprints pass unedited, and its tool route is identical to
  the base's.
- **The spectrum:** tables from k/ω_t = 10⁻³ to x_hi = 4.0 | 6.5 | 5.5 (exponential | n = 1 | n = 2), the spline within
  2.8e-6 of direct evaluations, the tail ≤ 7.4e-4 of the energy; every kernel within 5e-10 of its quadrature reference.
- **Issues:** none opened, none closed.
- **Suites:** 26, 705 (219 + 486), 163, 302, 76 at the base; 26, 732 (219 + 513), 163, 302, 76 after. Only the two
  worktree failures of note 01's ruling 7 failed, both times.

## 1. What shipped

- **Production:**
  - `CosmologyConcepts/drain_model.py`: `DRAIN_MODEL_ONE_LOOP_PL = "one-loop-measured-pl-v1"` (law: one-loop), third in
    `DRAIN_MODELS`; `_MODEL_OF_LAW` written out (still `one-loop-measured-v1` for the one-loop law); the regimes
    `REGIME_*` and `DRAIN_REGIMES`, moved here from `ScalarHistory.py` (D2).
  - `CosmologyConcepts/Models/AbstractModel.py`: `_drain_fraction(normalization, regime, ell, log_E, log_R)` and
    `drain_fraction_implemented`; the property `one_loop_drain_model`; no generic default for either (each base raises
    `NotImplementedError` naming the class); `RealizedModel.drain_fraction(regime, ell, log_E, log_R)`,
    `.drain_fraction_implemented`, `.one_loop_drain_model`.
  - `CosmologyConcepts/Models/ExponentialModel.py`: `one_loop_drain_model` → `DRAIN_MODEL_ONE_LOOP` (only).
  - `CosmologyConcepts/Models/InversePowerModel.py`: the constants `DRAIN_PL_S`, `_G`, `_R0`, `_P`, `_A`, `_C`,
    `_SWITCH` (keyed by n, each with its source), `_drain_fraction`, and `one_loop_drain_model` →
    `one-loop-measured-pl-v1` at n = 1, 2, a `ValueError` naming the class, the hook and n otherwise; docstring.
  - `ComputeTargets/ScalarHistory.py`: `check_drain_law_model` (docstring and messages rewritten);
    `drain_law_inputs` (L None on a model without `_drain_L`); `drain_alpha_effective(…, model=None, log_E=None)`
    dispatching to the L law or the model's own fraction, cap and gate unchanged; the loop passes the model and the
    assessment's ln E; the record's `model` is the model's `one_loop_drain_model` under the one-loop law; comments.
    `drain_fraction`, its constants and the S table are unchanged; no expression depends on a wall's form.
  - `ComputeTargets/quanta_spectrum.py`: rewritten for the walls (§3.5): `WALLS`, `SPECTRUM_MODELS`, `spectrum_model`,
    `spectrum(wall, assoc_scale)`, the three tables as literals, the measured shape, R10's associated piece, moments and
    kernels by composite Gauss–Legendre, the asymptotes re-derived, the references by adaptive quadrature;
    `PEAK_SPECTRUM` unchanged.
  - The four readers (`tools/quanta_measures.py`, `quanta_figures.py`, `plot_quanta_history.py`,
    `plot_quanta_grid.py`): `SPECTRUM_WALL = qs.WALL_EXPONENTIAL` with a comment naming prompt 06, passed where they
    called `qs.spectrum(c)` or named `qs.SPECTRUM_MODEL`; docstring lines in `quanta_measures` and `quanta_figures`.
- **Tests:** `ComputeTargets/tests/test_power_wall_law.py` (new, 18 tests); `test_quanta_spectrum.py` rewritten (23); the
  re-pins of §3.6.
- **`prompts/power-law-wall/source/wall/`:** `fit_fn.py` (the fit, `fraction_power`, and the `gate` mode),
  `spectrum_table.py` (modes `lowk`, `energy`, `table`, `shape`, `oldnew`), `README.md` appended ("Prompt 03").
- **`.documents/power-law-wall/law-03/`** (new, text): `fit_fn.txt`, `gate_law.txt`, `spectrum_tables.txt`,
  `tool_route_ip_n1.txt`, `tool_route_ip_n2.txt`, `tool_route_exponential.txt`.
- **`.documents/power-law-wall-verification.md` §3** (after §2, which is unchanged).
- **`CLAUDE.md`:** the ComputeTargets and `test_[g-z]` counts (705 → 732, 486 → 513) and the note under them.
- **The board:** row 03, the gate Law, the baselines, the status line and history. No §3/§4 entry moved, so
  `.documents/OPEN_ISSUES.md` is unchanged.
- **Not committed:** the suites' outputs, the table literals as written by `table`, the base tree (`git archive 7c60226`)
  and its exponential output, the Σα_eff capture, the breakage copies, in the agent's scratch directory
  (`…/scratchpad/agent03/`).

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note 03 §0)

1. **The worktree and the main checkout.** All work in the worktree with `./venv/bin/python` and `PYTHONPATH=.`; the main
   checkout untouched; `venv` never staged; `/tmp/ray-cpbh` never named; nothing in `/tmp` but the session scratchpad.
   *STRUCTURALLY REQUIRED.*
2. **The fit.** §0's fit reproduced with its windows (0.3 < ln R < 20 rising, ln R > 28 falling), weights (unweighted in
   f) and starting points; constants agree with §0's to its printed digits, and the worst residual over all points is
   0.0541 | 0.0557 against 0.054 | 0.056 (verification §3.1). No report to the user was needed. *IMPLEMENTATION CHOICE,
   ruled.*
3. **One set of literals.** `fit_fn.FN_*` and `InversePowerModel.DRAIN_PL_*` are the same decimal literals (and the test
   module's verbatim copy); the gate compares bit for bit and finds every value equal. *IMPLEMENTATION CHOICE, ruled.*
4. **The hook's shape.** `_drain_fraction(normalization, regime, ell, log_E, log_R)`, the prompt's suggested signature,
   so that a later wall's law may read ℓ or ln E; `one_loop_drain_model` a property on `AbstractModel`, raising
   `NotImplementedError` in the base and `ValueError` where the model's law is undefined at its parameters, so that the
   check can refuse both with the model's own words (class, hook, n) and nothing in `ScalarHistory.py` knows n.
   `drain_alpha_effective` gains keyword arguments `model` and `log_E` (default None: the L law, so every existing call
   is unchanged), so that the gate's "through `drain_alpha_effective` and the model's hook" holds. *IMPLEMENTATION
   CHOICE, ruled.*
5. **The spectrum's tolerances.** Interpolation 2.8e-6 at worst (≤ 1e-4); every kernel within 4.8e-10 of its quadrature
   reference over its whole range (≤ 1e-7); the tail's share 2.2e-4, 7.4e-4, 4.5e-4 (≤ 1e-3). All met. *IMPLEMENTATION
   CHOICE, ruled.*
6. **The tests that move.** The three quanta modules' edits are listed in §3.6 with old and new values; none pins an
   exponential history, golden or fingerprint. Edits outside them: D12. *STRUCTURALLY REQUIRED.*
7. **The two worktree failures** (`test_registered_run…this_repositorys_head`, `test_registry_config…
   test_roots_and_distributions`) failed at the base and after, and nothing else. *IMPLEMENTATION CHOICE, ruled.*
8. **A finding about production** is recorded in §6, not fixed. No issue opened. *IMPLEMENTATION CHOICE, ruled.*
9. **The machine.** Six workers for the tables; the three tool-route histories (and the base's exponential) one at a
   time; `uptime` beside every timing in the outputs and here. *IMPLEMENTATION CHOICE, ruled.*
10. **No file but §4's list.** The commit holds the production files, the tests, `fit_fn.py`, `spectrum_table.py`, the
    README, `law-03/`, the verification document, `CLAUDE.md`, the log and the board. The gate's script is a mode of
    `fit_fn.py` and not a file of its own for this reason (D13). *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1 — the base suites were disturbed, and re-measured.** The base run was started at 09:47; I began editing
  production files at about 09:55, while its `test_[g-z]` half was running, which would have let the later suites import
  the change. I stopped the run, saved the edits as a patch, restored the base files (`git checkout --` of the five
  production files), and re-ran `test_[g-z]`, ObjectFactories, tools and RunDrivers on the clean base (09:57–10:14).
  CosmologyModels and `test_[a-f]` had finished at 09:49 and 09:51, before any edit, and their results stand. The
  edits were then re-applied from the patch. The order of note 03 §0 (suites first, then the change) was not kept.
  *UNINTENDED DRIFT.*
- **D2 — the regimes moved to `drain_model.py`.** `InversePowerModel._drain_fraction` names the regimes;
  `ScalarHistory.py` imports `CosmologyConcepts`, so the model could not import them from there. They are defined in
  the leaf module, as the drain law's names were by one-loop-drain 03, and `ScalarHistory.py` imports them, so every
  `SM.REGIME_*` is the same object (asserted by `test_power_wall_law` (a)). *IMPLEMENTATION CHOICE.*
- **D3 — the switch literal.** The five fitted constants are rounded to six significant figures; the switch is the root
  of y_rise = y_fall at those literals, typed as its repr (17 digits), so that f is continuous to rounding there (step
  1.1e-16 | 0). *IMPLEMENTATION CHOICE.*
- **D4 — x_hi by the model's share.** The prompt asks for the energy above x_hi to be ≤ 1e-3; above x_hi the model's
  energy is the matched tail's, and the local slope at x_hi is smaller than the data's mean fall, so the share under the
  model exceeds the data's (1.2e-3 against 7.5e-4 at 3.5 on the exponential; 1.3e-3 against 9.9e-4 at 5.0 on n = 2).
  `table` starts from the data's choice and raises x_hi by 0.5 until the model's share is ≤ 1e-3: 4.0, 6.5, 5.5.
  *IMPLEMENTATION CHOICE.*
- **D5 — the strict setting.** At walls.py's tolerances |β| carries an absolute error near 1e-7 (02's spectrum.txt),
  1e-3 of |β|² near 1e-8; the tables use 02's strict setting (rtol 1e-13, atol 1e-17, EPS 1e-8); `lowk` reports both
  settings. *IMPLEMENTATION CHOICE.*
- **D6 — the interpolant.** A not-a-knot cubic spline in (ln x, ln s), not a monotonicity-preserving scheme (PCHIP would
  flatten the peak onto a node); it is checked monotone on either side of the peak on a 40× finer grid (no wrong-signed
  derivative on any wall). *IMPLEMENTATION CHOICE.*
- **D7 — x_p.** "x_p, the table's peak, by a fit": the maximum of the spline through the table (a root of its
  derivative), so that s_assoc is C¹ at x_p; the seven-node parabola of 02's spectrum.py is reported beside (0.6446,
  0.4646, 0.5055 against 0.6468, 0.4658, 0.5067). *IMPLEMENTATION CHOICE.*
- **D8 — the spectrum's names.** One string per wall in `SPECTRUM_MODELS` ("R8-exponential-v1", "R8-power-n1-v1",
  "R8-power-n2-v1"), the `spectrum_model` column of every output (no column added), and `SPECTRUM_MODEL` removed, so that
  a reader that still names it fails at once rather than mislabelling. Walls named "exponential", "power-n1",
  "power-n2". *IMPLEMENTATION CHOICE.*
- **D9 — the normalization.** s_dir = x² below x_lo exactly, so the matched c is 1 and U4-U17-v1's I and ln s(x_p) compare
  with the new ones (§3.4); every result is independent of the normalization. *IMPLEMENTATION CHOICE.*
- **D10 — Ω's grid.** OMEGA_U_MAX 4 → 9 (the asymptote needs x_lo e^u past the energy: the remainder bound is 3.6e-4 at
  u = 8 and below 1.2e-15 at 9 on every wall), OMEGA_U_STEP 0.005 → 0.01 (the spline is within 4.8e-10 at it); G's grid
  unchanged. *IMPLEMENTATION CHOICE.*
- **D11 — a scalar path in `shape`.** The tools' direct quadratures call `qs.shape(WALL, x, c)` per point; through the
  array path `tools/tests`' four quanta modules took 340 s, which would have pushed `tools/tests` near the ten-minute
  cap. `shape` of a float now uses the scalar expressions (69 s, against 62 s at the base). *IMPLEMENTATION CHOICE.*
- **D12 — tests edited outside the three quanta modules.** Each pinned exactly what the prompt changes; none pins an
  exponential history, golden or fingerprint:
  - `ComputeTargets/tests/test_drain_store_records.py` `test_the_names_are_the_leaf_modules`: `DRAIN_MODELS` was
    `("fixed", "one-loop-measured-v1")`, now with `"one-loop-measured-pl-v1"` third (P17);
  - `ComputeTargets/tests/test_a_values_option.py` `test_3_model_of_law_inverts_law_of`: `model_of_law(law_of(m)) == m`
    for every m in `DRAIN_MODELS`, now for the first two, and the third maps to `one-loop-measured-v1` (P17);
  - `ComputeTargets/tests/test_inverse_power_model.py` (g) `test_g_the_one_loop_law_refuses_it`: refused at n = 2 naming
    `_drain_L`, now at n = 3 naming `one_loop_drain_model` and n (P16);
  - `tools/tests/test_model_option.py` (4): `--n 2 --drain-law one_loop` refused with the `_drain_L` message, now
    `--n 3` with the new message (its prefix asserted; the model's name follows it);
  - `tools/tests/test_plot_quanta_history.py` `test_the_footer` and `tools/tests/test_plot_quanta_grid.py`
    `test_the_footer_and_the_printed_line_give_the_counts`: the needle "U4-U17-v1" → "R8-exponential-v1", the
    spectrum's name, which moved with the spectrum (P22's reason, outside its list of modules).
  *STRUCTURALLY REQUIRED.*
- **D13 — the gate's script.** `fit_fn.py gate` (from the source directory, `PYTHONPATH` the repository root) writes
  `law-03/gate_law.txt`; the tool-route outputs are the tool's own stdout with `uptime` lines. *IMPLEMENTATION CHOICE.*
- **D14 — twenty-two map files.** The prompt says "twenty power-law map files"; 02's amended map holds twenty-two
  (`ip-n1-…-b3.05-a0.1.txt` and `ip-n2-…-b3.05-a0.1.txt` added), 1148 candidates. The gate runs on all of them.
  *STRUCTURALLY REQUIRED.*
- **D15 — Σα_eff.** `history_and_bbn.py` prints no Σα_eff; it is summed from the same tuned payloads captured by a
  scratch wrapper around `tune_history` (BBN skipped), not committed. *IMPLEMENTATION CHOICE.*
- **D16 — the exponential at the base.** Run from `git archive 7c60226` extracted into scratch, with the worktree's venv,
  so the worktree did not have to be returned to the base. *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 The suites

From the worktree root, serially, by a background script (`…/scratchpad/agent03/run_suites.sh`). Time is the wall time;
load the one-minute average at the start.

| Suite | Base `7c60226` (ran / result / time / load) | After (ran / result / time / load) |
|---|---|---|
| CosmologyModels | 26 / OK / 67 s / 8.38 | 26 / OK / 69 s / 7.13 |
| ComputeTargets `[a-f]` | 219 / OK / 147 s / 11.56 | 219 / OK / 143 s / 5.64 (re-run after D12's `test_a_values_option` re-pin; the first run, 152 s at 8.42, had that one failure) |
| ComputeTargets `[g-z]` | 486 / OK / 520 s / 4.86 | 513 / OK / 565 s / 8.18 |
| ObjectFactories | 163 / OK / 133 s / 3.65 | 163 / OK / 132 s / 8.93 |
| tools | 302 / 1 failure (ruling 7) / 282 s / 3.59 | 302 / 1 failure (ruling 7) / 291 s / 6.25 |
| RunDrivers | 76 / 1 failure (ruling 7) / 25 s / 6.46 | 76 / 1 failure (ruling 7) / 25 s / 5.51 |

No count fell; `[g-z]` rose by 27 (18 in `test_power_wall_law`, 14 → 23 in `test_quanta_spectrum`). No test that passed
on the base fails after.

### 3.2 The exponential, at once after the law's change (note §0 order, step 3)

At 10:14 (load 4.9): `test_golden_history`, `test_golden_reflecting_history`, `test_measured_drain_law`, `test_drain_law`,
`test_drain_store_records`: 102 tests, one failure (`DRAIN_MODELS`, D12), 30 s. At 10:15 (load 3.5):
`test_history_and_bbn`, `_alpha`, `_drain_law`, `test_surfing_scan`, `test_model_option`, `test_inverse_power_model`:
68 tests, three failures (the n = 2 refusals, D12), 83 s; every exponential tool test passed unedited. The fingerprint
goldens (`ObjectFactories/tests`) pass in the full suite.

### 3.3 The fit and the law

Verification §3.1 (the constants, standard errors, switch and residuals beside §0's) and §3.2 (the dispatch).
`test_power_wall_law` (18 tests, 0.8 s at load 4.3): (a) the drain models, `alpha_value(0, 1.0,
"one-loop-measured-pl-v1")` accepted and A ≤ 0 refused, the regimes one object; (b) each model's one-loop drain model,
n = 3's `ValueError`, the bases' `NotImplementedError`; (c) the check refusing neither, both, no drain model and n = 3,
accepting n = 1, 2 and the exponential; (d) the fraction against `fraction_power` bit for bit under two units, the
literals, R·S_n, continuity at the switch (≤ 4e-16) and f's maximum there, 0 unassessed, through `drain_alpha_effective`
with the model; (e) a one-loop integration at n = 2 (4 candidates, all one-loop, 0.7 s): `drain.model` is
`one-loop-measured-pl-v1`, L None and α_eff = min(0.99, f) w_gate at every candidate.

### 3.4 The spectrum

Verification §3.3–§3.6: the low-k power and the precision floor; the tables (edges, sizes, x_p, κ_hi, shares); the module;
the shapes over L and E/V₀; old against new on the exponential. `test_quanta_spectrum` (23 tests, 14.4 s at load 4.9):
the walls and their names, an unknown wall refused naming the three, `spectrum()` without a wall a TypeError; s_dir = x²
below x_lo and continuous there; s and d ln s/dx continuous at x_hi (left slope within 1e-5, right within 1e-9 of κ_hi);
the spline through the nodes; s_dir ≤ x² above x_lo; x_p; the associated piece's form, coefficient and s(x_*) =
50 s_dir(x_*); the moments to 1e-12; G and Ω to 1e-7 at the nodes, midpoints, ends and beyond on every wall at c_a = 1,
0, 10; the asymptotes term by term and Ω's remainder bound at u = 9; Ω's symmetry; `PEAK_SPECTRUM`.

### 3.5 The module's measured accuracy

Measured in scratch before the module was installed, on every wall at c_a = 1, 0, 10: moments against adaptive
quadrature to ≤ 1.1e-15; G at 37 points over its grid, ends and beyond within 9.7e-11 (n = 1); Ω at 31 points over
±13 within 4.8e-10 (exponential, at OMEGA_U_STEP 0.01); the tables build in 1.3–1.5 s (Ω) per (wall, scale).

### 3.6 The tests that moved (P22)

| Module | Assertion | Old | New | Why |
|---|---|---|---|---|
| `test_quanta_spectrum.py` | the whole module | 14 tests on U4-U17-v1 (X_PEAK = 0.62, KAPPA's fit, 49 x_*³, I's assoc share 2.5e-5–3.5e-5, G/Ω at TOL 1e-6, Ω's asymptote at U = 4) | 23 tests on the three walls (the tables' ends, x_p, R10's piece, the assoc share 4e-5–7e-5 on the exponential and 1e-4–2e-3 on the power wall, J and M₁/M₃ to 1e-5 of `table`'s, G/Ω at TOL 1e-7, U = 9) | the shape is R8's |
| `test_quanta_measures.py` | `_quad_x`'s breaks | 0, X_PEAK, X_PEAK + {4, 16, 60}/KAPPA | 0, x_lo, x_p, 60 points to x_hi, x_hi + {4, 16, 60}/κ_hi | the shape's breaks moved |
| `test_quanta_measures.py` | the direct integrands | `qs.shape(x, c)` | `qs.shape(WALL, x, c)`, WALL the exponential's | the wall is required |
| `test_quanta_measures.py` | `direct_n_bar`'s extra breaks | X_PEAK/r, (X_PEAK + k/KAPPA)/r | x_lo/r, x_p/r, (x_hi + k/κ_hi)/r | the shape's breaks moved |
| `test_quanta_measures.py` | ln n_g's reference (two places) | `qs.shape(qs.X_PEAK, c)` | `qs.shape(WALL, qs.x_peak(WALL), c)` | x_p is the table's |
| `test_quanta_measures.py` | `spectrum_model` | `qs.SPECTRUM_MODEL` ("U4-U17-v1") | `qs.spectrum_model(WALL)` ("R8-exponential-v1") | the spectrum's name |
| `test_quanta_figures.py` | `spectrum_model` (two places) | `qs.SPECTRUM_MODEL` | `qs.spectrum_model(qf.SPECTRUM_WALL)` | the spectrum's name |

No tolerance of a tools test moved. The other edits are D12's.

## 4. The gate Law, item by item

1. **The law at synthetic inputs:** bit for bit at 122019 evaluations per (n, units), n = 1, 2, Planck_units and
   GeV_units, ln R ∈ [−20, 600], through the switch and ln R = 0 (`gate_law.txt`; `test_power_wall_law` (d)). **Holds.**
2. **02's maps, candidate by candidate:** α_eff at A = 1, w_gate = 1, through `drain_alpha_effective` and the hook,
   equals min(0.99, `fraction_power`) bit for bit at all 1148 candidates of the 22 files. **Holds.**
3. **The exponential bitwise:** goldens, tool tests under both laws and fingerprint goldens pass unedited; the
   exponential's tool route equals the base's, timings masked. **Holds.**
4. **The tool route** at laboratory M, β = 2, A = 1 (10:54–10:55, load 8.9 → 5.8; 16–17 s each):

   | | injections | regimes | Σ α_eff | ΔN_eff 1 MeV / today | parking N | drain.model |
   |---|---|---|---|---|---|---|
   | n = 1 | 37 | one-loop 37 | 3.102674 | 0.0285519 / 0.030891 | 30.9009 | one-loop-measured-pl-v1 |
   | n = 2 | 48 | one-loop 48 | 4.673102 | 0.0329852 / 0.0345102 | 29.654 | one-loop-measured-pl-v1 |
   | exponential | 45 | one-loop 45 | 5.315675 | 0.041888 / 0.044624 | 30.1253 | one-loop-measured-v1 |

   Both power-law histories ran, tuned in one integration. **Holds.**
5. **The spectrum** as R8 rules on every wall, the module's tests rewritten, the tools' tests passing with the re-pins of
   §3.6 and D12. **Holds.**

## 5. The deliberate-breakage record

Each mutation applied to the worktree file, the module named run, the file restored (`cmp` against the saved copy; no
`BREAKAGE` marker left):

| Mutation | Module run | Caught by |
|---|---|---|
| B1: the power wall's fraction read at production's ℓ in place of ln R (`log_R = ell if ell is not None else log_R`) | `test_power_wall_law` | 6 failures: (d) against the reference at both units and both n, (d) continuity at the switch, (e) the integration's α_eff |
| B2: S₁ and S₂ swapped in `DRAIN_PL_S` | `test_power_wall_law` | 7 failures: (d) against the reference ×4, the classical branch, the literals, through `drain_alpha_effective` |
| B3: the tail matched in value but not slope (κ_hi the chord from the largest node to x_hi) | `test_quanta_spectrum` | 9 failures: the matched tail on all three walls, the shape's J and M₁/M₃ on all three, Ω's remainder bound at u = 9 on n = 1 |
| B4: the associated piece's coefficient 49 x_*³ in place of 49 x_* s_dir(x_*) | `test_quanta_spectrum` | 6 failures: s(x_*) = 50 s_dir(x_*) and the coefficient on all three walls |

## 6. Observations not acted on

- **Note 03 §0's energy table is weighted in ln x, not x.** `kappa_probe.py` and `energy_split.py` take quartiles and the
  share above x = 3 from a cumulative sum of x³|β|² over a log-spaced grid with no interval weights, which measures
  ∫x³s d ln x. Weighted in x (`spectrum_table.py energy`), the share above x = 3 is 4.1e-3 (exponential), 0.145 (n = 1)
  and 0.058 (n = 2), against §0's 0.2%, 6.8% and 2.5%. The tables do not depend on it; R8's design does not either.
- **The measured tail is modulated.** Above the peak the energy share falls by irregular factors per 0.5 in x (on the
  exponential 3 to 17 above x = 5; on n = 1 1.7 to 4.7), so κ_hi, a local slope, depends on where x_hi falls: 4.191 at
  x_hi = 4.0 against 02's tail fit on [3, 10] (4.262) and two-point (3.967); 2.157 against 2.145 and 2.006 (n = 1); 2.720
  against 2.713 and 2.511 (n = 2). The tail carries ≤ 7.4e-4 of the energy on every wall.
- **The peak of f on the power wall exceeds the exponential law's caps.** f_n peaks at 0.968 | 0.966 at the switch; the
  exponential law caps its one-loop branch at `DRAIN_F_CAP` = 0.95 and its plateau at 0.9. Neither R7 nor the prompt asks
  for a cap on this wall; only `DRAIN_ALPHA_CAP` = 0.99 applies, through `drain_alpha_effective` (it binds where A f ≥
  0.99, i.e. A ≥ 1.02 at the peak).
- **The step at ln R = 0** is 0.48 (n = 1) and 0.71 (n = 2) of R·S_n (§0 noted it); the classical branch itself is 2.1 |
  1.9 times stage 2 at ln R = −0.5.
- **`model_of_law` still returns `one-loop-measured-v1` for the one-loop law**, so `bbn_from_store`, the drivers, the plot
  scripts and `config/argument_parser.py` would select or key a power-law model's one-loop histories under the
  exponential's model. P17 assigns them to 05 and 06.
- **The quanta tools read the exponential's table for every history** of an export, power-law ones included, until 06
  takes the wall from the model column (P19).
- **`InversePowerModel._drain_fraction` answers R·S_n for a "classical" regime at ln R > 0**, which `drain_regime` never
  gives (classical is ln R ≤ 0); `fraction_power` would give f_n there. Neither path is reachable.
- **`tools/tests`' quanta modules run 11% slower** (69 s against 62 s) with the measured shape's direct quadratures; the
  `test_[g-z]` half gains about 16 s (`test_quanta_spectrum` 14.4 s, `test_power_wall_law` 0.8 s).

## 7. State handed to the next prompt

- **The law on this wall:** `InversePowerModel._drain_fraction`, stored under `alpha_value.drain_model =
  "one-loop-measured-pl-v1"` (`DRAIN_MODEL_ONE_LOOP_PL`), at n = 1 and 2 only:
  - classical (ln R ≤ 0): f = R·S_n, S₁ = 1.02620e-2, S₂ = 5.21615e-3;
  - one-loop and gap: f = 1 − e^{−y}, y = G (ln R − R₀)^P below ln R_×, y = A/(ln R − c) at and above it:
    n = 1: G 2.86769e-3, R₀ −1.27414, P 2.22126, A 27.9671, c 14.9835, ln R_× 23.08943252571177;
    n = 2: G 2.85913e-3, R₀ −1.10903, P 2.45613, A 19.6100, c 10.9039, ln R_× 16.710187997698853;
  - unassessed: 0. α_eff = min(0.99, A f) w_gate. The record's L is None.
- **The hooks:** `AbstractModel._drain_fraction(normalization, regime, ell, log_E, log_R)` and
  `AbstractModel.one_loop_drain_model` (no defaults); a model defines exactly one of `_drain_L` and `_drain_fraction`;
  `check_drain_law_model` refuses otherwise, and refuses `InversePowerModel` at n ∉ {1, 2} under the one-loop law.
- **The drain models:** `DRAIN_MODELS = ("fixed", "one-loop-measured-v1", "one-loop-measured-pl-v1")`; `model_of_law`
  unchanged.
- **The spectrum:** `quanta_spectrum.spectrum(wall, assoc_scale=1.0)`, walls `"exponential"` (WallN(90, 1), serving the
  exponential family at every n), `"power-n1"`, `"power-n2"` (PowerWall(1e40, n)); `SPECTRUM_MODELS` "R8-exponential-v1",
  "R8-power-n1-v1", "R8-power-n2-v1"; x_p 0.6468, 0.4658, 0.5067; x_* = 0.0058 on every wall, unmeasured on the power
  wall; `SPECTRUM_MODEL` removed.
- **04 inherits** the law and the tool route under the one-loop law at n = 1, 2 (`--model inverse-power --n N
  --drain-law one_loop --alpha A`), and verification §3.5's table error in x near the floor (factor 1/s_t), for its share
  of K from generations with E/V₀ < 100 (README §6.6 U8).
- **05 inherits** `one_loop_drain_model` as the key a stored power-law history carries and the store tools select by
  (`bbn_from_store` uses `model_of_law` today).
- **06 inherits** the readers' `SPECTRUM_WALL` constants (four modules, each commented), to be taken from the export's
  model column, and the call sites naming `DRAIN_MODEL_ONE_LOOP` (`main.py`, `plot_inputs.py`, the plot scripts,
  `config/argument_parser.py`).
- **The standard analysis "Relativistic quanta"** of the exponential moves by verification §3.6 when repeated (R8,
  convention 8).
