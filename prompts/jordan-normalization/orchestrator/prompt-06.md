# Orchestrator — prompt 06, the tuning layer

Read [`../README.md`](../README.md) first: §0.3 items 1–3, §2.2 (06's charter), §4, §5, §6.0 R1, R2,
R8, R10 and R13, §6.2 P3–P7, P12 and P13. Read 04's and 05's logs too, in particular "State handed
to the next prompt". The conventions of [`prompt-05.md`](prompt-05.md) bind this prompt as well.
They are restated below where 06 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`06-the-tuning-layer.md`](../06-the-tuning-layer.md)
**Board items:** T1–T7 · **Gate:** "Tuning" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Opus, as the prompt recommends · **Network:** none.

**Gate:**
- 05 landed (`1675e73`) and reviewed (`65af27d`); the gate "Units" holds (board §2).
- `HEAD` is the commit that adds this note, on `jordan-normalization`, on top of `65af27d`, with
  `git status` clean. No code has changed since `65af27d`.
- The prompt's own gate: no tracked change, the five suites at the baselines below, and no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 07 is not dispatched until 06 is reviewed.

## 0. What makes this prompt unusual

**A root-find round the integrator, whose acceptance (the secant never worse than the plain update
at any of thirteen points) is a stop if it fails once. The orchestrator built the loop before
writing this note and ran it on the thirteen points with both methods.** The prototype is
`tune.py` in the session scratchpad (not committed), at `65af27d`'s code. It calls
`compute_scalar_history._function` on `ExponentialModel(0, M_value(0, M M_P), 1, beta_value(0, β),
Planck_units())`, realized at each iterate, with `QCD_Cosmology(Planck2018)`, T* = 2 × 10⁴ GeV,
φ* as given, π* = 0, T_stop = `cosmology.T_CMB_Kelvin * units.Kelvin`, atol = rtol = 10⁻⁸ (the
defaults), and main.py's default sampling axis (`log_one_plus_z_grid(250, 300)`). It starts from
`model.seed(cosmology)`, takes the steps of ruling 6, and stops at ruling 5's test with
tolerance 10⁻⁵. Its runs were made in eight parallel lanes at a machine load of 60–160 (other
work shared the machine), so the wall times are long.

- **The comparison** (§2 item 2's table, measured). Integrations to converge, the steps by kind,
  and the converged values: Λ = V₀^¼ in meV and φ_ref/M_P.

  | β | M/M_P | φ* | seed | plain: integrations (steps) | secant: integrations (steps) | Λ plain (meV) | Λ secant (meV) | φ_ref/M_P plain | φ_ref/M_P secant |
  |---|---|---|---|---|---|---|---|---|---|
  | 1.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.125321 | 2.125322 | 3.4962045e-02 | 3.4962266e-02 |
  | 25 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 1.842954 | 1.842955 | 6.8089327e-03 | 6.8089580e-03 |
  | 5.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 2.020615 | 2.020615 | 1.6719234e-02 | 1.6719361e-02 |
  | 3 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.073998 | 2.073996 | 2.3662575e-02 | 2.3662904e-02 |
  | 1.1 | 1e-3 | 2 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.144994 | 2.144996 | 4.1533670e-02 | 4.1533944e-02 |
  | 0.5 | 1e-3 | 1 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.184947 | 2.184949 | 6.3821123e-02 | 6.3821650e-02 |
  | 3 | 1e-5 | 5 | tracking | 3 (plain 2) | 3 (plain 1, secant 1) | 2.226473 | 2.226473 | 2.6704632e-03 | 2.6704769e-03 |
  | 1 | 1e-5 | 5 | tracking | 5 (plain 4) | 4 (plain 1, secant 2) | 2.236588 | 2.236588 | 4.6642085e-03 | 4.6642162e-03 |
  | 3 | 0.01 | 5 | tracking | 7 (plain 6) | 5 (plain 1, secant 3) | 1.831404 | 1.831403 | 6.2228103e-02 | 6.2228439e-02 |
  | 1 | 0.03 | 5 | tracking | 8 (plain 7) | 5 (plain 1, secant 3) | 1.881162 | 1.881157 | 2.0284503e-01 | 2.0284635e-01 |
  | 3 | 0.1 | 5 | static | 13 (plain 12) | 9 (plain 1, secant 7) | 1.388919 | 1.388919 | 1.4772415e-01 | 1.4772170e-01 |
  | 1 | 0.5 | 5 | static | 21 (plain 20) | 9 (plain 1, secant 7) | 1.332252 | 1.332252 | 6.2763553e-01 | 6.2762591e-01 |
  | 3 | 0.5 | 5 | static | 23 (guard 1, plain 21) | 12 (guard 1, plain 1, secant 9) | 0.937490 | 0.937486 | 2.7085278e-01 | 2.7084733e-01 |

  Each run begins with the seed's integration, which the step counts leave out. Λ is
  `exp(norm / 4) / units.eV * 1e3`, φ_ref is `phi_ref / units.PlanckMass`, both from the
  converged iterate.

  | β | M/M_P | φ* | \|Δ ln Λ\| | \|Δφ_ref\|/φ_ref | plain's last \|φ_ref/φ_min − 1\| | Λ_secant/Λ_planning − 1 | φ_ref,secant/φ_ref,planning − 1 | wall plain, secant (s, loaded) |
  |---|---|---|---|---|---|---|---|---|
  | 1.5 | 1e-3 | 5 | 3.2e-07 | 6.3e-06 | 2.9e-06 | −2.781e-04 | −3.8e-06 | 71, 49 |
  | 25 | 1e-3 | 5 | 3.4e-07 | 3.7e-06 | 7.0e-07 | −2.767e-04 | −6.2e-06 | 910, 798 |
  | 5.5 | 1e-3 | 5 | 1.4e-07 | 7.6e-06 | 6.1e-06 | −2.779e-04 | −8.3e-06 | 289, 239 |
  | 3 | 1e-3 | 5 | 1.1e-06 | 1.4e-05 | 4.6e-06 | −2.795e-04 | +1.5e-07 | 97, 60 |
  | 1.1 | 1e-3 | 2 | 7.8e-07 | 6.6e-06 | 2.7e-06 | −2.774e-04 | +1.0e-06 | 76, 50 |
  | 0.5 | 1e-3 | 1 | 1.0e-06 | 8.3e-06 | 1.5e-06 | −2.768e-04 | +7.1e-06 | 42, 26 |
  | 3 | 1e-5 | 5 | 2.9e-08 | 5.1e-06 | 1.2e-06 | −2.773e-04 | −8.6e-06 | 287, 282 |
  | 1 | 1e-5 | 5 | 3.3e-08 | 1.6e-06 | 1.6e-06 | −2.776e-04 | +3.5e-06 | 69, 56 |
  | 3 | 0.01 | 5 | 5.7e-07 | 5.4e-06 | 1.3e-07 | −2.783e-04 | +3.8e-06 | 93, 74 |
  | 1 | 0.03 | 5 | 2.2e-06 | 6.5e-06 | 1.1e-06 | −2.894e-04 | +2.7e-06 | 24, 14 |
  | 3 | 0.1 | 5 | 1.2e-07 | 1.7e-05 | 9.7e-06 | −2.780e-04 | +1.6e-05 | 96, 66 |
  | 1 | 0.5 | 5 | 2.9e-07 | 1.5e-05 | 7.1e-06 | −2.772e-04 | +1.6e-05 | 36, 17 |
  | 3 | 0.5 | 5 | 4.2e-06 | 2.0e-05 | 8.2e-06 | −2.647e-04 | +1.2e-05 | 125, 81 |

- **The secant is never worse.** It needs fewer integrations at twelve points and as many at one
  (β = 3, M = 10⁻⁵, three each): 4 against 6 at M = 10⁻³ for most β, 9 against 13 at M = 0.1,
  and 9 against 21 and 12 against 23 at M = 0.5. In all, 73 integrations against 116. From the
  tracking seed the plain update already needs only 3–8 (planning's plain start needed 6–19).
- **The two methods agree, but not always to the tolerance.** |Δ ln Λ| ≤ 4.2 × 10⁻⁶ everywhere.
  |Δφ_ref|/φ_ref is below 10⁻⁵ at nine points, and 1.4–2.0 × 10⁻⁵ at four (β = 3 at M = 10⁻³,
  0.1 and 0.5; β = 1 at M = 0.5). The plain update stops when
  its *step* is below the tolerance, |φ_ref/φ_min − 1| < 10⁻⁵, and it contracts by only 0.4–0.6 per
  integration at M ≥ 0.1, so its last iterate lies further from the fixed point than its last
  step. At β = 3, M = 0.1 its last step is 9.7 × 10⁻⁶ and its φ_ref is 1.66 × 10⁻⁵ above the
  secant's, whose last step is 2.9 × 10⁻⁷. That is one fixed point approached at two rates, not two
  fixed points (ruling 13 sets the criterion).
- **Against the planning probes** (README §0.3 item 1, on the units before 05, from the plain
  start). Λ_secant/Λ_planning − 1 is −2.77 × 10⁻⁴ to −2.80 × 10⁻⁴ at eleven points, 05's −2.78 × 10⁻⁴,
  and −2.89 × 10⁻⁴ (β = 1, M = 0.03) and −2.65 × 10⁻⁴ (β = 3, M = 0.5) at the other two, where the
  planning probe's own plain slack is of that size. φ_ref, which is in Planck units and does not
  move with the electronvolt, agrees to 1.6 × 10⁻⁵ or better.
- **The guard fires only at the seed.** With the model's seeds, V_target ≤ 0 occurs only at
  β = 3, M = 0.5 (static seed), and only at integration 0, before any secant step. The same holds
  for the probes' plain start (V₀ = ρ_Λ, φ_ref = 0) at β = 1, M = 0.5, run as a variant: one guard
  step at integration 0, then 19 integrations plain (planning's 19) against 9 secant.
  **So the Broyden matrix is never set when the guard fires, and removing the guard's reset (the
  prompt's breakage 3) changes nothing on any point measured**: the variant with the reset removed
  gave the same nine lines bit for bit. **Nor does the twice rule fire anywhere**: every secant run
  is `seed`, (`guard`,) `plain`, then `secant` to the end. Ruling 6 therefore puts the step in an
  object a unit test can drive (ruling 11 (b)), and breakage 3 fails that test.
- **A V_target that never recovers.** A test subclass of `ExponentialModel` whose
  `effective_minimum` returns φ₀ − M_P/β (so A(φ₀) = e at every iterate after the first) gives, at
  β = 3, M = 0.01, φ* = 5: integration 0 at the seed, then V_target ≤ 0 at integrations 1, 2, 3
  and 4 (V_target/V = −7.9, −20.9, −33.1, −49.4), and the failure after the fifth integration. Each
  of those integrations is an ordinary history (12–19 s at load 60–90; about 5 s unloaded).
- **The progress line the prompt specifies would be read as a stage banner.**
  `RunDrivers/registered_run.py`'s `_STAGE_BANNER` is `^\s*(?:\*\*|>>)\s+(\S.*?)\s*$`, so a line
  that begins `>> tune` is a banner. Ray prefixes a worker's line with `(task pid=…) ` when it
  forwards it, which saves it today, but not when the prefix is off
  (`RAY_DISABLE_WORKER_LOG_PREFIX`, or a job `LoggingConfig`; `ray/_private/worker.py:2188`), and
  never in a tool that tunes in-process. Ruling 9 begins the line with `-- `, which no pattern of
  the tracker matches and which the compute layer's other lines already use.
- **Ray deduplicates worker lines.** `ray/_private/ray_logging/__init__.py:208` drops every word
  that contains a digit before comparing lines, so tuning lines of the same `kind` from different
  workers within the aggregation window (5 s) are collapsed into one with "[repeated N× across
  cluster]". The first is printed at once, so the heartbeat still beats; the run log loses lines.
  An observation for the log (ruling 15); 09 sees it under Ray.
- **Two existing tests read `compute_scalar_history`'s body by name.**
  `ComputeTargets/tests/test_integrator_exceptions.py`'s `TestFallbackGone`
  (`test_g_no_fallback`, `:278`, and `test_g_one_runtime_error_in_the_integration_path`, `:306`)
  find the function in the module's AST. Once the body moves, the second fails (it expects the one
  `RuntimeError` there) and the first passes vacuously. Ruling 3 points both at `integrate_history`.
  No other test reads the module's source.
- **The prompt's residuals and its convergence test differ.** T2 defines r_H = ln(H_J/H0) and
  r_φ = φ_ref − φ_min, and converges on |H_J/H0 − 1| and |φ_ref/φ_min − 1|. 09's acceptance is
  "both stored residuals inside the tolerance", which holds only if the stored residuals are the
  ones the test reads. Ruling 5 stores those.
- **04's `effective_minimum`** takes `(normalization, phi_ref, phi_0, rho_m_E_0, bracket=None)`, not
  the prompt's `(…, rho_m_E, phi_bracket)`; ρ_m,E(φ₀) = A(φ₀)⁴ f_m ρ_R,J. It raises `ValueError`
  when its bracket (0.05 φ₀, 20 φ₀) has no sign change, a failure the prompt does not name
  (ruling 8). φ_min/φ₀ was 1.00–1.17 at every converged point.
- **Two of the prompt's breakages fail nothing as the tests are written.**
  - *Convergence on r_H alone*: at the tests' point (β = 3, M = 0.01) both residuals fall below the
    tolerance at the same integration, so the converged test passes. Ruling 5 makes the test a
    function with a unit test of its own.
  - *The seed from a module-level cache*: two runs of one model read the same seed either way, so
    determinism passes. Ruling 11 (h) tunes two models in one process.

**Baselines** at `65af27d`, serially, at load 8–16 (with the last prototype run beside them):
CosmologyModels **26**, ComputeTargets **61 + 144**, ObjectFactories **86**, tools **102**,
RunDrivers **61**, all passing. These are 05's counts. `test_prym_passenger`'s wall-clock test
passed.

**These seventeen points, where the prompt is loose, silent or wrong for this tree, are ruled here.**

1. **`integrate_history` (T1).** In `ComputeTargets/ScalarHistory.py`, a plain function (not
   `@ray.remote`) with `compute_scalar_history`'s signature and order:
   `integrate_history(cosmology, T_init, T_stop, phi_init, pi_init, z_grid, model, task_label="integrate_history", atol=DEFAULT_ABS_TOLERANCE, rtol=DEFAULT_REL_TOLERANCE, verbose=False)`,
   with `model` a `RealizedModel`. Its body is `compute_scalar_history`'s, moved unchanged, except
   that the function's own name in its printed lines and in the `RuntimeError`'s message becomes
   `integrate_history`. The docstring says it is the primitive (R8): one realized model, any
   T_stop, no normalization logic. Classify as `IMPLEMENTATION CHOICE`, citing this note.

2. **`compute_scalar_history` stays the `@ray.remote`**, with its signature, defaults and docstring
   unchanged, and its body `return integrate_history(...)` with every argument passed through by
   name. 07 points it at `tune_history`. `ScalarHistory.compute`, `main.py`, `golden_history.py`,
   the tools and every other caller are unchanged. The golden history passes unchanged through it
   (that is the prompt's "bitwise through `integrate_history`"); `golden_history_05.json` is not
   regenerated, and `golden_history.py` and `test_golden_history.py` are not edited.
   Classify as `STRUCTURALLY REQUIRED`.

3. **`TestFallbackGone`.** Both tests look up `"integrate_history"` in place of
   `"compute_scalar_history"`, and the comment at `:276-277` says so. Nothing else in the file
   changes. The modification is listed in the log with its reason (README §5). Classify as
   `STRUCTURALLY REQUIRED`.

4. **`tune_history`'s signature.**
   `tune_history(cosmology, model, T_init, phi_init, pi_init, z_grid, task_label="tune_history", atol=DEFAULT_ABS_TOLERANCE, rtol=DEFAULT_REL_TOLERANCE, tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS, secant=True, verbose=False)`,
   with `model` an `AbstractModel`.
   - `DEFAULT_TUNING_TOLERANCE = 1.0e-5` and `DEFAULT_TUNING_MAX_INTEGRATIONS = 30` are module
     constants of `ScalarHistory.py` (08 gives them options). `config/defaults.py` is not edited.
   - T_stop is `cosmology.T_CMB_Kelvin * cosmology.units.Kelvin`, a float: the same float as
     `main.py`'s T_CMB (`:1180`). There is no T_stop argument.
   - `secant=False` takes the plain step at every iterate. It exists for §2's comparison and for
     diagnostics; its docstring says so, and no driver passes it.
   - It reads the model only through `realize`, `renormalize`, `normalization_coordinate`,
     `normalization_from_coordinate`, `effective_minimum`, `seed` and the `RealizedModel`'s methods
     (README §4). It never reads V₀, M, n or β.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

5. **Today's state, the residuals and the test (T2).** From the last sample of each integration
   (`sample[-1]`, at ln(1 + z) = 0, which is T_J = T_CMB), with `rm` the iterate's realized model,
   `Mp = cosmology.units.PlanckMass` and `H0 = cosmology.H0`, by exactly these expressions (the
   prototype's; ruling 13 depends on the bits):
   - `phi0`, `pi0` = the sample's `phi_Einstein`, `pi_Einstein`; `A0 = rm.Omega(phi0)`;
     `rho_RJ = exp(log_rhorad_Jordan)`; `fm = exp(log_fm)`; `logV = rm.log_V(phi0)`;
   - `bp = 1.0 + rm.d_logOmega_dphi(phi0) * pi0`; `G = 1.0 - pi0 * pi0 / (6.0 * Mp * Mp)`;
   - `V_target = A0**4 * (3.0 * Mp**2 * H0**2 * G / (A0**2 * bp**2) - rho_RJ * (1.0 + fm))`
     (Eq. 7 solved for V at H_J = H0);
   - `rho_mE0 = A0**4 * fm * rho_RJ`; `phi_min = model.effective_minimum(norm, phi_ref, phi0, rho_mE0)`;
   - `residual_H = H_Jordan / H0 - 1.0`, `residual_phi = phi_ref / phi_min - 1.0`.

   These two are what the progress line prints, what the test reads and what T7 stores, under those
   names. The test is a module-level function, `tuning_converged(residual_H, residual_phi, tolerance)`,
   true iff `abs(residual_H) < tolerance and abs(residual_phi) < tolerance`. The prompt's r_H and
   r_φ are not formed. Classify as `STRUCTURALLY REQUIRED` (09's acceptance reads the stored
   residuals against the tolerance).

6. **The step (T3), in a small object.** A class in `ScalarHistory.py` (name it, say
   `TuningSecant`) holds the Broyden state and nothing else: no integration, no model. The
   prototype's expressions bind.
   - **The iterate** is x = (`model.normalization_coordinate(norm)`, `phi_ref / scale`), where
     `scale` is φ_min of integration 0, fixed for the tuning.
   - **The plain map**: G(x) = (`normalization_coordinate(model.renormalize(norm, phi0, V_target))`,
     `phi_min / scale`), and F = (G₀ − x₀, G₁ − x₁).
   - **A plain step** sets the matrix B to [[−1, 0], [0, −1]] and returns G(x) itself (not x + F).
   - **A secant step** is taken when the secant is on and B is set. It first updates B by Broyden's
     ("good") rule, with dx = x − x_prev and dF = F − F_prev:
     `B[i][j] += (dF[i] - (B dx)[i]) * dx[j] / (dx[0]**2 + dx[1]**2)`. It then solves B d = −F by
     Cramer's rule, as the prototype's `solve2`, and returns x + d.
   - **The twice rule.** If the point being stepped from came from a secant step and
     ‖F‖ > 2 ‖F_prev‖ (Euclidean, in x's coordinates), B is reset and the step is plain, from the
     current point.
   - After every step, x_prev and F_prev are the point stepped from and its F.
   - **`guard()`**, called by the guard of ruling 7, resets B, x_prev and F_prev. The next step is
     then plain.
   - The new iterate is `norm = model.normalization_from_coordinate(x_new[0])`,
     `phi_ref = x_new[1] * scale`.

   It counts nothing. `tune_history` counts the steps by kind. Classify as `STRUCTURALLY REQUIRED`
   (the breakage of the guard's reset is otherwise invisible, §0).

7. **The loop and the guard.** After integration k (k = 0 is the seed's), in this order:
   1. the residuals; if `tuning_converged`, return T7's output;
   2. if `V_target <= 0.0`: count it; if this is the fourth in a row, fail (ruling 8). Otherwise the
      next iterate is `norm = model.renormalize(norm, phi0, 0.5 * exp(logV))`, `phi_ref = phi_min`
      (directly, not through `scale`), the stepper's `guard()` is called, and the kind is `guard`.
      A positive V_target resets the count;
   3. if k + 1 equals `max_integrations`, fail (ruling 8);
   4. otherwise the stepper's step, of kind `plain` or `secant`.

   The kinds are `seed` (integration 0), `plain`, `secant` and `guard`. The prompt's three gain
   `guard`, so that a guard step is counted and printed as what it is. Classify as
   `IMPLEMENTATION CHOICE`, citing this note.

8. **The failures (T5).** Each reason's first clause, the text before the first `:`, is distinct,
   and `_failure_payload` is what a failed tuning returns:
   - `"the normalization did not converge: after N integrations, H/H0-1=…, phi_ref/phi_min-1=…"`;
   - `"no normalization reaches H0: V_target <= 0 at 4 integrations running, the last at integration k"`;
   - `"the effective minimum was not found: <the ValueError's text> (tuning integration k)"`, a
     fourth clause, for `effective_minimum`'s `ValueError`;
   - an integration's failure: its own reason, unchanged, with ` (tuning integration k)` appended,
     so that `summarise_failure_reasons` counts it under its own first clause as today. The
     truncation to `DEFAULT_STRING_LENGTH` may cut the suffix of a long reason; that is accepted.

   The residuals print with `%+.3e`. `pipeline_selection.py` is not edited. Classify the fourth
   clause as `STRUCTURALLY REQUIRED`, and the suffix as `IMPLEMENTATION CHOICE`, citing this note.

9. **Progress (T6).** One line per integration, printed by `tune_history` (with `flush=True`)
   after the residuals are formed, the converging integration included:
   `-- tune <task_label> it=<k> kind=<kind> H/H0-1=<%+.3e> phi_ref/phi_min-1=<%+.3e> norm=<%.12g> phi_ref=<phi_ref/Mp, %.10e> wall=<%.1f>`.
   `wall` is that integration's `perf_counter` interval. An integration that fails prints no line
   (its own lines are printed). The log says what `StageTracker.feed` does with the line: nothing,
   since neither `_STAGE_BANNER`, `_QUEUE_PROGRESS` nor `_QUEUE_COMPLETE` matches it, so the stage
   does not move and, forwarded by Ray, the line beats. Classify as `STRUCTURALLY REQUIRED` (§0).

10. **What `tune_history` returns (T7).** The final integration's success payload, every key as
    `integrate_history` returns it, and these keys, each a Python `float`, `int` or `str`:
    `normalization`, `V_ref` (= `exp(rm.log_V(phi_ref))` on the final realization), `phi_ref`,
    `phi_0`, `pi_0`, `tracking` (= `pi0 / (1.5 * phi0)`), `residual_H`, `residual_phi`,
    `tuning_iterations` (integrations, the seed's included), `plain_steps`, `secant_steps`,
    `guard_steps` (so `tuning_iterations = 1 + plain_steps + secant_steps + guard_steps`),
    `seed_kind`, `seed_normalization`, `seed_phi_ref`. 07 stores exactly these. Classify as
    `IMPLEMENTATION CHOICE`, citing this note.

11. **The tests** (P13): `ComputeTargets/tests/test_tuning.py`, discovered by the `[g-z]` half.
    Each docstring gives its cost. One tuning in `setUpClass` at the tests' point (β = 3,
    M = 0.01 M_P, φ* = 5 M_P, the rest as the prototype; five integrations, about 25 s unloaded)
    serves (c), (d) and (e).
    - (a) `tuning_converged`: both inside; each outside alone; the boundary (`tolerance` itself is
      outside).
    - (b) the stepper, on a synthetic map with no integration: the first step returns G(x)
      bitwise; the second is a secant; after `guard()` the next is plain and B is −I; the twice
      rule gives a plain step and resets B; and on an affine contraction in two variables the
      secant reaches 10⁻¹² in fewer steps than the plain map.
    - (c) converged: both stored residuals inside the tolerance; A(φ_ref) = 1 exactly
      (`model.realize(normalization, phi_ref).log_Omega(phi_ref) == 0.0`); `V_ref` equal to the
      realization's; the residuals recomputed by ruling 5 from the payload's last sample equal the
      stored ones bitwise; `seed_*` equal `model.seed(cosmology)`'s; the step counts add up; and the
      progress lines, captured, number `tuning_iterations`, each matching ruling 9's format, and
      `StageTracker().feed` returns False on each with its stage still None.
    - (d) determinism: a second `tune_history` in the test equals (c)'s bitwise, in
      `golden_history.record_of` of the payload and in every T7 key.
    - (e) the tuned golden history (P3): (c)'s output against `golden_history_06_tuned.json`
      (ruling 12).
    - (f) the cap: `max_integrations=2` fails with the first clause "the normalization did not
      converge" (read with `summarise_failure_reasons`), after two integrations.
    - (g) the H0 failure: §0's subclass (`effective_minimum` returns φ₀ − M_P/β) fails with "no
      normalization reaches H0", after five integrations.
    - (h) the seed is the model's own: β = 3 at M = 0.01 and at M = 0.1, each with
      `max_integrations=1`, in that order in one test; the `it=0` line of each, captured, prints
      that model's `seed(cosmology)` normalization and φ_ref in ruling 9's formats.
    - (i) the effective-minimum failure: a subclass whose `effective_minimum` raises `ValueError`
      fails with that clause after one integration.
    - (j) an integration's failure: `StepControl` patched to a step budget of 50, as
      `test_scalarhistory_failure_reason.py` does, fails with a reason that starts "step budget
      exhausted" and ends "(tuning integration 0)", or is truncated to `DEFAULT_STRING_LENGTH`.

    The module needs no Ray and no store. Classify as `STRUCTURALLY REQUIRED`.

12. **The tuned golden history (T7, P3).** A generator, `ComputeTargets/tests/golden_tuned_history.py`
    (its name does not match `test*.py`), beside `golden_history.py`, from which it imports
    `record_of`, `first_difference`, `environment` and `_hex`.
    - It runs (c)'s tuning and writes `ComputeTargets/tests/data/golden_history_06_tuned.json`:
      provenance as `golden_history.py`'s (commit, "jordan-normalization prompt 06's working tree,
      before its commit, on top of generated_at_commit", the setup, the environment, the time), the
      record of the final payload by `record_of`, and the T7 keys, floats as `float.hex`.
    - The test compares with `first_difference` for the record and exactly for the T7 keys.
    - `golden_history_01/03/05.json` are not touched (byte-identical).

    Classify as `STRUCTURALLY REQUIRED`.

13. **The comparison (§2 item 2).** The agent's own script, in its scratch directory, calls
    `tune_history` with `secant=True` and with `secant=False` on README §0.3's thirteen points, in
    parallel lanes, each run's progress lines to a log. The logs go to `logs/06-probes/` with a
    `README.md` saying which script made which log and how it was run; the script itself is
    committed there too, as a record, since nothing imports it.
    - **The agent's table must equal §0's** in every integration count, step count and digit shown,
      since both are deterministic and rulings 5–7 fix the bits. A difference is a stop, with the
      first progress line that differs.
    - **Agreement**: for each point, the log gives |Δ ln V₀|/4 (= |Δ ln Λ|) and |Δφ_ref|/φ_ref
      between the methods, and the plain update's last |φ_ref/φ_min − 1|. A difference above
      10⁻⁵ is accounted for by the plain update's slack (§0); **one above 10⁻⁴ is a stop** (two
      fixed points?).
    - **Against the planning probes**: Λ_06/Λ_planning − 1 and φ_ref,06/φ_ref,planning − 1 per point,
      expected near −2.78 × 10⁻⁴ in Λ (05's units) and small in φ_ref, as §0 says.

    The comparison may run detached, but the agent waits for it before it writes the log; it is the
    only long work in this prompt (README §5's named exception). Classify as `STRUCTURALLY
    REQUIRED`.

14. **The breakages.** Each is recorded as a diff that `git apply` replays from the agent's commit.
    1. `tuning_converged` on `residual_H` alone: (a) fails.
    2. The seed read from a module-level cache (the first model's seed reused): (h) fails.
    3. `guard()` that does not reset B: (b) fails.

    For each, the log names every test that fails and how. The prompt's "show which point then
    fails or slows" is replaced by (b): no point of §0.3's grid fires the guard after a secant step
    (§0). Classify as `STRUCTURALLY REQUIRED`.

15. **Observations, not acted on** (log only, no issue):
    - Ray's deduplication of worker lines that differ only in words with digits (§0);
    - the static seed (M ≥ 0.1) starts far from H0, H_J/H0 − 1 = +0.68 (β = 3, M = 0.1; β = 1,
      M = 0.5) and +1.08 (β = 3, M = 0.5), against −7 × 10⁻² to −2 × 10⁻⁴ for the tracking seed, so
      those points take 9–12 integrations even with the secant;
    - the plain update's slack at its tolerance (§0, ruling 13).

16. **The records.**
    - **This board:** row 06, T1–T7 and the gate "Tuning"; `### 5.7 After prompt 06 (<date>)`, in
      §5.6's form, with §2's table in full; §1.1's items for 06 marked as landed.
    - **`.documents/OPEN_ISSUES.md`** changes only if the work opens an issue. None is expected.
    - **Not edited:** `prompts/INDEX.md`, the README, `CLAUDE.md`, `.documents/`.

17. **The stop conditions.** The prompt's §4, with these changes:
    - "the two methods converge to values that differ by more than the tolerance" becomes "by more
      than 10⁻⁴" (ruling 13);
    - the agent's table differs from §0's (ruling 13);
    - the golden history (`golden_history_05.json`) fails;
    - an existing test fails for a reason this note does not name. Never edit such a test to make
      it pass.

**No driver runs.** `main.py` is not changed and is not run. No Ray is started, and no store is
opened except the tests' temporary ones (`test_tuning.py` opens none).

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Order.**
  1. The five suites and `test_golden_history` alone (with its time), before any edit.
  2. T1 and rulings 2–3; the golden history alone; stop here if it fails.
  3. Rulings 4–10.
  4. The comparison (ruling 13), started detached; while it runs, ruling 11's tests and ruling 12's
     generator, then the tuned JSON.
  5. `test_tuning` alone, with its time; the suites; the breakages.
  6. The comparison's table, then the records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its two
  halves, never while the comparison runs. Send output to a file in the agent's scratch directory,
  grep the verdict from it, and record `uptime` beside each. A failure is re-run alone before it
  is called real. `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall-clock
  bound and fails under load; re-run it alone before calling it real.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository, except the comparison's script and logs in
  `logs/06-probes/`. No worktree is needed.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `65af27d`, before this note was committed:
1. **The tree.** `jordan-normalization`, with `git status --short` empty.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: three runs, all finished
   (`ray-sqla-align` 02's). None is `running` or stale.
4. **The prototype and its variants** of §0, in the session scratchpad.
5. **The baselines** above.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README, this board, and 04's and 05's logs;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad,
  with the prototype `tune.py` beside it. The agent does not read `orchestrator/` or prompts 07–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/06-the-tuning-layer.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **The files it may add or change** are the prompt's §5, with these changes:
  - added: `ComputeTargets/tests/test_integrator_exceptions.py` (ruling 3);
  - added: `ComputeTargets/tests/test_tuning.py`, `ComputeTargets/tests/golden_tuned_history.py`
    and `ComputeTargets/tests/data/golden_history_06_tuned.json` (rulings 11, 12);
  - added: `prompts/jordan-normalization/logs/06-probes/` (ruling 13);
  - removed: `CosmologyConcepts/Models/` and `pipeline_selection.py`. Neither needs a change; a need
    is a stop.

  Nothing is deleted.
- **§0's rulings** bind it, each classified as §0 says.
- **It starts no Ray, opens no store, and runs no driver.**
- **Stop and ask** on any of ruling 17's conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - `test_golden_history`'s and `test_tuning`'s times;
  - the comparison table, and whether it equals §0's;
  - the agreement and the planning columns of ruling 13;
  - every test added, with what it asserts;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists only §2's files. `git status` is clean.
2. **The move.** `git diff <agent>~1 <agent> -- ComputeTargets/ScalarHistory.py`: `integrate_history`'s
   body is `compute_scalar_history`'s, with only ruling 1's name changes; `compute_scalar_history`
   passes every argument through. The golden history passes, and `golden_history*.py` and
   `golden_history_05.json` are unchanged.
3. **The loop against the prototype.** Read `tune_history`, the stepper and `tuning_converged`
   against rulings 5–10, expression by expression. It reads the model only through ruling 4's
   methods.
4. **The comparison, rerun in part.** Run the orchestrator's own probe through the agent's
   `tune_history` on three points (β = 3 at M = 0.01, 0.1 and 0.5, both methods), and compare its
   lines with the agent's logs and §0 bit for bit.
5. **The tests.** Read `test_tuning.py` against ruling 11, and run it alone with its time.
6. **The tuned golden.** Run the generator's builder at `<agent>` into the scratchpad and compare
   with `golden_history_06_tuned.json`; `golden_history_01/03/05.json` byte-identical to
   `<agent>~1`'s.
7. **The progress line.** Feed a captured line to `StageTracker`, and check the prefix against
   `_STAGE_BANNER`.
8. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
   says, revert, and check that `git status` is clean.
9. **The comparison's record.** `logs/06-probes/`: the script, the logs and the README; the table in
   the log and in §5.7 equal to §0's; the agreement and planning columns.
10. **The records and the suites.** The board (row, items, gate, §5.7) and the log in §5.1's form,
    with every deviation classified. Then the five suites at `<agent>`, serially, at the counts the
    log states, and `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 06* paragraph in §5.7. Fix small
  residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log);
  - add this note to the board's list of orchestrator notes, marked "used for 06";
  - update the status line: 6 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the progress line begins `-- `, since `>> ` is a stage banner to the wrapper (ruling 9);
    - the stored residuals are the ones the test reads, H_J/H0 − 1 and φ_ref/φ_min − 1 (ruling 5);
    - a fourth step kind, `guard`, and a fourth failure clause, for the effective minimum
      (rulings 7, 8);
    - the step lives in an object a unit test drives, because the guard never fires after a
      secant step on the grid, so the prompt's breakage 3 was invisible (rulings 6, 14);
    - agreement between the methods is judged at 10⁻⁴, with the plain update's slack accounting
      for differences above 10⁻⁵ (ruling 13);
  - the comparison table;
  - the test times, and the five suites' counts.
- **Then 07.** Its note is written after this review.
