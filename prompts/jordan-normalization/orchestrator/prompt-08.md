# Orchestrator — prompt 08, the driver and the tools

Read [`../README.md`](../README.md) first: §2.2 (08's and 09's charters), §4, §5, §6.1 U4, §6.2 P4,
P11, P12, P17 and P18. Read 06's and 07's logs too, in particular "State handed to the next prompt".
The conventions of [`prompt-07.md`](prompt-07.md) bind this prompt as well. They are restated
below where 08 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`08-the-driver-and-the-tools.md`](../08-the-driver-and-the-tools.md)
**Board items:** D1–D7 · **Gate:** "Driver" · **Closes:** `[05-surfing-scan-runs-at-a-fixed-lambda]`
(`surfing-temperature`) · **Opens:** `[08-a-history-that-did-not-converge-is-never-retried]` (this
board; ruling 14) and only what else the work finds · **Model:** Opus, as the prompt recommends ·
**Network:** none.

**Gate:**
- 07 landed (`223afc6`) and reviewed (`f8fced8`); the gate "Store" holds (board §2).
- `HEAD` is the commit that adds this note, on `jordan-normalization`, on top of `f8fced8`, with
  `git status` clean. No code has changed since `f8fced8`, which changed only a docstring of
  `test_golden_history.py`.
- The prompt's own gate: no tracked change, the five suites at the baselines below, and no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 09 is not dispatched until 08 is reviewed.

## 0. What makes this prompt unusual

**The user-facing surface, and the first tools that tune a history outside the store.** The
orchestrator read every file the prompt names at `f8fced8` and ran the prompt's short surfing scan
as a probe before writing this note. What it found, where the prompt is silent, loose or wrong for
this tree:

- **The prompt's stop on the roots rests on a wrong expectation, and the scan barely moves.** The
  probe `probe_tuned_surfing.py` (in the session scratchpad, beside its log and `out.json`) tuned
  β = 1.5, 3.0 and 4.0 at M = 10⁻³ M_P, φ* = 5 M_P to T_CMB with `tune_history`, on
  `tools/history_and_bbn.py`'s setup and a 300-decade axis, and analysed each final integration
  with `surfing_scan.summarise` on a window that ends at the history's first crossing of
  T_J = 1 MeV (ruling 9). Against the committed `surfing_scan_phi5_M1e-3.csv`, at load 6–9:

  | β | integrations (seed) | wall | Λ_eff = V_ref^¼ | φ_ref/M_P | captured (committed) | largest change of a T or N column | T_hot, T_cold |
  |---|---|---|---|---|---|---|---|
  | 1.5 | 4 (tracking) | 24.7 s | 2.140574 meV | 3.4962266385e-02 | QCD (QCD) | N_max −2.6e-8 | −5.1e-15 |
  | 3.0 | 4 (tracking) | 39.8 s | 2.096024 meV | 2.3662903571e-02 | no (no) | T_deliver +1.1e-7 | — |
  | 4.0 | 5 (tracking) | 81.8 s | 2.075394 meV | 2.0081247681e-02 | EW (EW) | N_deliver −3.1e-8 | −5.0e-15 |

  The roots (`reachability_roots`) are EOS temperatures converted back through the same
  `units.GeV`, so 05's units shift cancels in them: they agree to 5 × 10⁻¹⁵, not to the 2.8 × 10⁻⁴
  the prompt expects. Every other column moves by at most 1.1 × 10⁻⁷ relative, the crossing counts
  and capture kicks are unchanged, and the window holds every turning point of the whole history
  (the walk stops at the first bounce, above 1 MeV at all three). Before BBN V₀ is negligible
  against the matter and radiation, and a constant φ_ref rescales A, to which those dynamics are
  blind. **The prompt's stop condition therefore reads: a root differs by more than 10⁻¹², or a
  capture flag or kick differs from the committed row at any of the three β.** The tuned values
  must equal the probe's (§2 item 2, ruling 13).
- **The full scans now cost about 1¾ hours.** Per integration to T_CMB: 6.2 s (β 1.5), 10 s (3.0),
  16 s (4.0), against 0.5, 1.7 and 4.8 s for one integration to 1 MeV; four or five integrations per
  β. Summed over S1's 79 β (1.10–5.00) at about 4.3 integrations each, roughly 65 minutes; S5's 61 β
  near 3.0, roughly 40 minutes; against 287 s for S1 before. U4's estimate stands. The agent
  measures its own and predicts both.
- **`--tuning-max-integrations` cannot reach the remote through the store's objects.**
  `ScalarHistory.compute` calls `compute_scalar_history.remote(…, tolerance=self._tuning_tol.tol)`
  and passes no cap (`ComputeTargets/ScalarHistory.py:2200-2211`), and the factory builds a
  `ScalarHistory` from named payload keys (`ObjectFactories/ScalarHistory.py:318-340`). Passing
  the cap through the payload would change the factory, which README §4 makes a stop. The cap is
  not a key (the prompt makes only the tolerance one), so it is an argument of `compute` (ruling 3),
  which is not a factory.
- **A history that did not converge is never computed again.** The failure is stored under the
  key, which holds no cap; `pipeline_selection.missing_models` does not count a stored failure as
  missing (07, ruling 8); and `main.py` has no retry for the scalar stage (only `--retry-failed-bbn`
  for BBN). A later run with a larger cap therefore recomputes nothing, short of
  `--drop scalar-history`, which drops every history. 08 says so in the option's help and opens it
  as an issue (ruling 14).
- **`configargparse` refuses an unknown option with a generic message.** `--T-stop-GeV 1` gives
  `unrecognized arguments: --T-stop-GeV 1`, exit 2, naming nothing. The prompt wants the
  replacement named. A hidden option whose action calls `parser.error(…)` does that, on the
  command line and as a key of a `--config` YAML file alike (both checked in a scratch parser: exit
  2, the action's message). Calling `parser.error` matters: the wrapper replaces its parser's
  `error` (`RunDrivers/registered_run.py:624`), so the same action becomes the wrapper's refusal
  there (ruling 1).
- **`action="extend"` appends to a non-empty default.** With `default=[1]`, `--n-values 2` parses
  to `[1, 2]` (checked in the venv). The parser's other list options use `extend` with an empty
  default. `--n-values` must not copy them (ruling 2).
- **The old run script is not refused.** `science-runs/2026-10-03-science-2026.6.0/full_run_2026.6.0.sh`
  passes `--config exponential.yaml` and no removed option on its command line: Λ and the potential
  type lived in the YAML, which 07 and 08 rewrite. With this tree's YAML it would build tuned
  histories at n = 1. That is right for a rebuild, and 10's handover says so; the log records it as
  an observation.
- **`exponential.yaml`'s `log10-one-plus-z-high: 85` overrides 03's default of 300.** The grid's
  extent changes no tuned value: `integrate_history` only truncates the axis after integrating
  (`ComputeTargets/ScalarHistory.py:1271-1278`), and `_tuning_state` reads today's state from the
  last sample, ln(1 + z) = 0 (`:1546`), which every axis holds. A history longer than the axis
  fails ("largest supplied redshift"). 85 decades are 195.7 e-folds against histories that need up
  to about 200 (P8). Ruling 4 removes the line.
- **`tools/history_and_bbn.py`'s axis is too short for a tuned history.** It samples to 35 decades
  of 1 + z (80.6 e-folds); the brief's 35-decade grid fails at β = 25 (README §0.3). It takes
  `main.py`'s default (ruling 8). The grid does not enter the tuned values (above), so 09's bitwise
  comparison of stored values with this tool's is unaffected.
- **The warning needs a cosmology.** `ExponentialModel.seed(cosmology)` reads `H0`, `rho_m0`,
  `rho_r0` and the units (`CosmologyConcepts/Models/ExponentialModel.py:156-210`): it is analytic
  and cheap, but `warn_super_planckian` is called before `build_model_list` today
  (`main.py:1084`). It moves into the loop over the cosmologies (ruling 6).
- **How far the tuned φ_ref is from the seed's**, from 06's probe logs
  (`logs/06-probes/*_secant.log`, first and last `phi_ref=` of each): β(φ_ref − φ_ref,seed)/M_P,
  which is minus the change in ln A(φ*) between the two, is +8.5e-4 to +1.4e-3 at M = 10⁻³
  (β 0.5–5.5), +6.5e-3 at β = 25, +2.2e-6 and +6.3e-6 at M = 10⁻⁵, +1.2e-2 at M = 0.01, +3.0e-2 at
  M = 0.03, −0.37 at (β 3, M 0.1), −0.42 at (β 1, M 0.5) and −1.01 at (β 3, M 0.5); the last three
  start from a static seed. The warning's text cites this (ruling 6).
- **Two analyses must read only the window.** `surfing_turning_points` walks to the first bounce of
  the whole history, and the payload's `first_bounce` is the history's. Before 08 the integration
  stopped at T_stop, which is the first crossing of T_J = T_stop, so neither saw anything later.
  `T_Jordan_crossing(result, ln T)` (`ComputeTargets/ScalarHistory.py:1037`) gives that crossing's
  N on a history to T_CMB; a window N ≤ N_edge reproduces the old stop exactly, where a filter on
  T_J would not (T_J is not monotonic). Ruling 9.
- **A CSV without the tolerance and the window cannot be matched on resume.** The prompt drops
  `T_stop_GeV` and `Lambda_eV`, adds six tuned columns, and puts the tolerance in `_INPUT_MATCH`,
  which reads the CSV. The tolerance and the window's edge are therefore columns too (ruling 9).
- **`tools/plot_surfing_temperature.py` reads only `surfing_scan.COLUMNS`.** Once the columns
  change it cannot read the committed CSVs, which are records and stay as they are. It refuses one
  with a message naming its writer (ruling 9).
- **One tuned point reproduces a committed record.** `history_and_bbn.py`'s setup is
  `golden_tuned_history.py`'s (QCD_Cosmology on Planck2018 in `Planck_units`, T* = 2 × 10⁴ GeV,
  φ* = 5 M_P, π* = 0, atol = rtol = 10⁻⁸, tolerance 10⁻⁵, cap 30), so the tool tuned at β = 3,
  M = 0.01 must print `golden_history_06_tuned.json`'s `tuning` record bitwise: about 27 s, the
  test 09 needs (ruling 8).
- **Memory.** A tuning integrates four to twelve times. Both tools capture each
  `IntegrationResult` by wrapping `integrate_scalar_history` and append it to a list; only the last
  is read. They keep only the last (`captured[:] = [result]`, as the probe does).

**Baselines** at `223afc6`, from 07's review (serially, load 3–5; `f8fced8` changed only a
docstring): CosmologyModels **26**, ComputeTargets **61 + 164**, ObjectFactories **92**, tools
**102**, RunDrivers **61**, all passing, `test_prym_passenger`'s wall-clock test included.
`CLAUDE.md` still states 18, 176 (61 + 115), 78, 102 and 61.

**These sixteen points, where the prompt is loose, silent or wrong for this tree, are ruled here.**

1. **The removed options are refused by name (D1, D7).** In `config/argument_parser.py`, a small
   `argparse.Action` subclass (`RemovedOption`, or the agent's name), registered with
   `nargs="*"`, `help=argparse.SUPPRESS` and a message, whose `__call__` calls
   `parser.error(f"{option_string} was removed by jordan-normalization prompt NN: …")`. Never
   `sys.exit` or a print: the wrapper's `error` must see it. Registered for:
   - `--log10-Lambda-low-eV`, `--log10-Lambda-high-eV`, `--samples-per-log10-Lambda-eV`,
     `--Lambda-values-eV`, `--Lambda-values-Mp` (prompt 07): each history's normalization is tuned
     to H0 at T_CMB and stored, so no Λ is given; `--tuning-tolerance` sets the tuning;
   - `--T-stop-GeV` (prompt 07): every stored history ends at T_CMB;
     `tools/history_and_bbn.py --fixed-normalization NORM PHI_REF_MP --T-stop-GeV T` runs an untuned
     history to any T;
   - `--potential-type` (prompt 08): one model, `ExponentialModel`; `--n-values` gives its exponent.

   The wording is the agent's, listed in the log; each names its replacement. `potential_types`
   goes from the parser module and from `plot_by_beta.py:95`, where it is unread. Classify as
   `IMPLEMENTATION CHOICE`, citing this note.
2. **The three new options (D1).** Each beside the options it belongs with, its default imported
   from `ComputeTargets.ScalarHistory` (`DEFAULT_TUNING_TOLERANCE`,
   `DEFAULT_TUNING_MAX_INTEGRATIONS`), never retyped:
   - `--tuning-tolerance`, a positive float (an argparse type that refuses ≤ 0), default
     10⁻⁵: the tolerance on both residuals, |H_J(T_CMB)/H0 − 1| and |φ_ref/φ_min − 1|; a key of
     every stored history, so a run at another tolerance computes its own;
   - `--tuning-max-integrations`, a positive int, default 30: the cap on one history's
     integrations, the seed's included; not a key, and the help says that a history that does not
     converge within it is stored as a failure, which a later run with a larger cap does not
     recompute;
   - `--n-values`, positive ints, **`nargs="+"`, `default=[1]` and the default action** (not
     `extend`, §0): the model's exponent n, V = V₀ exp((M/φ)ⁿ); part of the model's key.

   `%(default)s` in each help. Classify as `STRUCTURALLY REQUIRED`.
3. **`main.py` (D1).**
   - The grid is `itertools.product(M_grid, n_values, beta_grid)`, M outer, n, β inner (β stays
     innermost for the shards' load), each model's payload `n` from it. The line
     `-- using potential type "…"` becomes one naming the model and the n values (text the
     agent's).
   - `tuning_tol = pool.object_get("tolerance", tol=args.tuning_tolerance)`.
   - `ScalarHistory.compute(self, label=None, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS)`
     passes `max_integrations=max_integrations` to `compute_scalar_history.remote`; its docstring
     says the cap is not a key. `main.py`'s `compute_solver_batch` passes
     `args.tuning_max_integrations`. Nothing else in `ComputeTargets/ScalarHistory.py` changes.
   - `run_pipeline`'s `metadata` and the stages are otherwise unchanged.

   Classify as `STRUCTURALLY REQUIRED`.
4. **`exponential.yaml` (D1)** loses `potential-type` and `log10-one-plus-z-high: 85`, so 03's
   default (300) applies; it gains nothing. The log gives §0's reason. Classify as
   `IMPLEMENTATION CHOICE`, citing this note.
5. **`plot_inputs.py` and the plot scripts (D1).**
   - `parameters(…, tuning_tol)` is given `args.tuning_tolerance`; its miss reads
     `PlotOptionNotStored("tolerance", "--tuning-tolerance", …)`.
   - `exponential_models(pool, M_table, beta_table, n_values, units)`: no `potential_type`,
     `MODEL_CLASSES` or `MODEL_N`; one lookup per `product(M_table, n_values, beta_table)`, each
     read on its own. A missing combination is skipped with its warning line (now giving its n)
     and counted, as today. **An n with no stored model at any stored (M, β)** raises
     `PlotOptionNotStored("ExponentialModel", "--n-values", n)`: the build was not run with it,
     and the scripts exit 2, as for a parameter.
   - `plot_by_beta.py` and `plot_ScalarHistory.py` pass `args.n_values` and `args.tuning_tolerance`
     and no `args.potential_type`. `--tuning-max-integrations` is not read by either.
   - Tests in `ComputeTargets/tests/test_plot_inputs.py`: an n = 2 store found at `--n-values 2`;
     `--n-values 1 2` on a store holding n = 1 only exits through `PlotOptionNotStored` for 2; the
     tolerance's miss names `--tuning-tolerance`.

   Classify the n rule as `IMPLEMENTATION CHOICE`, citing this note, and the rest as
   `STRUCTURALLY REQUIRED`.
6. **The super-Planckian warning (D2, P11).**
   - `super_planckian_couplings(models, phi_init, T_init, units, cosmology)` and
     `warn_super_planckian(models, phi_init, T_init, units, cosmology, emit=print)`: ln A(φ*) is
     `model.realize(None, s.phi_ref).log_Omega(phi)` with `s = model.seed(cosmology)`, one seed per
     model; the helper `_log_Omega_at_zero_phi_ref` becomes one at the seed's φ_ref.
   - `main.py` calls it once per cosmology, inside the loop over `build_model_list`'s list, before
     `run_pipeline`, and keeps the returned list.
   - Each line names β, M, n, φ*, Ω(φ*) T*/M_P, the seed's φ_ref/M_P and the seed's kind, and says
     the value is at the seed's φ_ref. The count line says once that the tuned φ_ref differs from
     the seed's, moving ln A(φ*) by −β(φ_ref − φ_ref,seed)/M_P, and gives §0's measured sizes (below
     1.4 × 10⁻³ at M = 10⁻³ with β ≤ 5.5, 6.5 × 10⁻³ at β = 25, up to 1.0 at M = 0.5 with a static
     seed; prompt 06's points). It stays a warning: the list is returned unchanged.
   - The docstrings: the test is β(φ* − φ_ref)/M_P > ln(M_P/T*); ln(M_P/T*) recomputed from
     `REDUCED_PLANCK_MASS_EV` and T* = 2 × 10⁴ GeV; the β thresholds at φ* = 5 and 1 M_P with the
     seed's φ_ref at M = 10⁻³ (computed, not estimated), and a sentence on how a large φ_ref (large
     M) raises them.
   - Tests in `ComputeTargets/tests/test_initial_field_option.py`: (a) and (d) take the cosmology;
     a new test with a model at which φ_ref = 0 and the seed's φ_ref disagree (for example β ≈ 7
     at M = 0.5, flagged at φ_ref = 0 and not at the seed's static φ_ref; the agent computes the
     pair and states it), and one on the lines' text.

   Classify as `STRUCTURALLY REQUIRED`.
7. **The wrapper (D3).** `describe_scope` drops the potential and adds `n <values>` after M and
   `tuning tolerance <_number(tol)>` after φ*. `PILOT_SCOPE` becomes `config exponential.yaml;
   β 1.1, 1.15, 1.2, 10, 25; M 0.001 Mp; n 1; φ* 5 Mp; tuning tolerance 1e-05; 16 shards; job
   rrk-adoption-04-reference`; the second scope test drops `--potential-type Exponential`, adds
   `--n-values 1 2 --tuning-tolerance 1e-6`, and expects `n 1, 2` and `tuning tolerance 1e-06`.
   Further tests:
   - `-- … --potential-type Exponential` and `-- … --Lambda-values-eV 1e-3` are refused by the
     wrapper with the action's message and write no manifest;
   - **the tuning lines**: three lines copied verbatim from
     `logs/06-probes/b3_M0.01_phi5_secant.log` (`-- tune …`), fed to `RegisteredStream` with
     `Clock` after a banner (as `TestTheBeat` does): they beat, and the stage stays the banner's;
     and a `standin_program` behaviour that prints them between RayWorkPool's banner and its first
     progress line, run through `stubbed_run`: the run's recorded stage is never a tune line.

   Classify as `STRUCTURALLY REQUIRED`.
8. **`tools/history_and_bbn.py` (D4, P18).**
   - **Tuned by default**: `tune_history(cosmology, model, T_init, φ*, π*, grid, task_label=…,
     tolerance=args.tuning_tolerance, max_integrations=args.tuning_max_integrations)` with
     `integrate_history`'s default atol and rtol, as `main.py`'s. Its `-- tune` lines are printed
     by `tune_history` itself. The capture keeps the last `IntegrationResult` only.
   - Then the `history …` line of today (the final integration's RHS evaluations, steps,
     reflections, samples; `wall` the whole tuning; `integrations=k` added), and **one `tuning …`
     line**: every key of `TUNING_OUTPUT_KEYS`, in order, as `key=<repr>` for a float and as is for
     an int or a string, unconverted (the run is in `Planck_units`, so each is the float a store
     would hold). `repr` round-trips, which 09's bitwise comparison needs. Then the bounce,
     fixed-T, ratio and BBN lines as today. A tuning failure prints `history …: FAILURE …` with
     the reason and exits 1.
   - **`--fixed-normalization NORM PHI_REF_MP`** (`nargs=2`, floats): `model.realize(NORM,
     PHI_REF_MP * units.PlanckMass)` and one `integrate_history`, no tuning line. NORM is the
     model's own normalization, ln V₀ with V₀ in M_P⁴, as a history stores it.
   - `--T-stop-GeV` only with `--fixed-normalization` (default T_CMB); without it, exit 2 with a
     message that a tuned history ends at T_CMB. `--tuning-tolerance` and
     `--tuning-max-integrations` (main.py's names and defaults) have no effect with
     `--fixed-normalization`, and their help says so.
   - The axis is `log_one_plus_z_grid(250, DEFAULT_LOG10_ONE_PLUS_Z_HIGH)` (300), imported from
     `config.argument_parser`. `LAMBDA_EV` and `LOG10_ONE_PLUS_Z_HIGH = 35.0` go.
   - The BBN stand-in's `T_Jordan_stop` is the history's end (T_CMB when tuned).
   - The docstring: both modes, the usage line, the output lines, and the costs (tuned at β = 3,
     M = 0.01: five integrations, about 27 s; one integration about 5 s; a PRyMordial solve about
     10 s).
   - **Tests**, a new `tools/tests/test_history_and_bbn.py`, through `main(argv)` with stdout
     captured and `compute_BBN_data` replaced by a stub (no PRyMordial; the docstring says so and
     gives the cost): (a) tuned at `3 0.01`: the `tuning` line's values equal
     `golden_history_06_tuned.json`'s `tuning` record bitwise (floats by `float.fromhex`), key for
     key; (b) `--fixed-normalization` at that record's normalization and φ_ref (their `repr` in
     argv): one `integrate_history` call, whose realized model has exactly those values, and the
     history line's sample count equal to the JSON history record's; (c) `--T-stop-GeV` without
     `--fixed-normalization`: exit 2 with the message, no integration.

   Classify as `STRUCTURALLY REQUIRED`.
9. **The surfing tools (D5, U4).**
   - **`surfing_scan.run_history(setup, beta, phi_init_Mp, M_Mp, T_window_low_GeV,
     tolerance=DEFAULT_TUNING_TOLERANCE, max_integrations=DEFAULT_TUNING_MAX_INTEGRATIONS)`**
     tunes with `tune_history` on `history_and_bbn`'s setup and axis, keeps the last
     `IntegrationResult`, and computes `N_edge = T_Jordan_crossing(result, ln T_window_low)` (None:
     the whole history). **A pure helper** (`in_window(crossings, bounce, N_edge)`, or the agent's
     name) keeps the crossings with `N <= N_edge` and the bounce if `bounce.N <= N_edge`, and
     `run_history` returns those. `HistoryOutcome` gains `tuning` (the payload's
     `TUNING_OUTPUT_KEYS` values, or None on a failure) and `N_window`.
   - **The option** `--T-window-low-GeV` (default 1e-3, `T_WINDOW_LOW_GEV_DEFAULT` in
     `T_STOP_GEV_DEFAULT`'s place), its help saying it is the lower edge of the analysis window on a
     history tuned to T_CMB, the first crossing of T_J. `--T-stop-GeV` is refused, exit 2, with a
     message naming it; likewise in `plot_surfing_histories.py`. Both tools gain
     `--tuning-tolerance` and `--tuning-max-integrations`, as in ruling 8.
   - **`COLUMNS`**, in this order: `beta`, `phi_init_Mp`, `M_Mp`, `tuning_tol`, `T_window_low_GeV`,
     `status`, `failure_reason`, `normalization`, `V_ref_eV4`, `phi_ref_Mp`, `residual_H`,
     `residual_phi`, `tuning_iterations`, then `captured` … `T_cold_GeV` as today.
     `normalization` is unconverted (ln V₀, V₀ in M_P⁴); `V_ref_eV4` is `V_ref / units.eV**4`;
     `phi_ref_Mp` is `phi_ref / units.PlanckMass`. The six tuned columns are blank on a failure
     row. The docstring states the columns' units.
   - **Resume.** `_INPUT_MATCH = ("phi_init_Mp", "M_Mp", "tuning_tol", "T_window_low_GeV")`. A
     header that holds `Lambda_eV` or `T_stop_GeV` is refused, exit 2, with its own message (a
     scan made before prompt 08 at a fixed Λ, which a tuned scan does not resume), before the
     generic header check. The sidecar's options lose `T_stop_GeV`, its top-level `Lambda_eV`
     goes, and it records the tolerance, the cap and the window's edge.
   - The console line adds the tuned values the agent chooses; the flag reads "no bounce above
     T_window". The docstring gives the measured cost per β and the predicted S1 and S5 times.
   - **`plot_surfing_histories.py`** calls the new `run_history` and traces to the windowed bounce,
     or to `N_window` if there is none in the window; its title replaces Λ by the tuning ("tuned to
     H0 at T_CMB, tolerance …"); its docstring gives the cost of six β.
   - **`plot_surfing_temperature.py`** reads the new columns (`FLOAT_FIELDS` follows); its title
     gives φ*, M, the tolerance and the window's edge, and no Λ or T_stop. A CSV whose header holds
     `Lambda_eV` is refused with a message: written before prompt 08, drawn by the tool at its
     sidecar's commit.
   - **Tests.** `test_surfing_scan.py`: `TestResume` on the new columns (the stub takes the new
     signature), with tolerance and window mismatches refused, and an old-format CSV refused with
     its own message; the window helper on hand-built crossings (one beyond N_edge dropped, a
     bounce beyond dropped, one at N_edge kept, None keeps all); `TestRealHistory` tunes β = 1.5
     (about 25 s): both residuals inside the tolerance, captured by QCD, and T_capture, T_deliver,
     T_hot and T_cold within 10⁻⁶ relative of the committed scan's row. `test_plot_surfing_histories.py`:
     its real history at β = 3.0 (about 40 s) is not captured and its bounce is within 10⁻⁶ of the
     committed fine scan's (the probe measured 1.1 × 10⁻⁷); its stubs take the new signature.
     `test_plot_surfing_temperature.py`: the synthetic CSV and the title on the new columns; an old
     header refused. Each real test's docstring gives its cost.

   This closes `[05-surfing-scan-runs-at-a-fixed-lambda]`. Classify as `STRUCTURALLY REQUIRED`,
   and the window helper and the two extra columns as `IMPLEMENTATION CHOICE`, citing this note.
10. **`histories.csv` (D6).** `CSV_COLUMNS`: `beta`, `M_Mp`, `n`, `phi_init_Mp`, `normalization`,
    `V_ref_eV4`, `phi_ref_Mp`, `phi_0_Mp`, `tracking`, `residual_H`, `residual_phi`,
    `tuning_iterations`, then `Yp_BBN` … `failure_reasons` as today. `build_history_record` reads
    `scalar.tuning` (readable with `_do_not_populate`) with the conversions of ruling 9, NaN where
    there is no successful history, so the cell is blank. The sort stays (M, n, β); the comment
    above the columns states the units. Tests: `test_extraction`'s records carry the columns; and a
    new test in `ObjectFactories/tests/test_tuned_history_round_trip.py` that passes the read-back
    history (`_do_not_populate`) through `build_history_record` and `write_histories_csv` into the
    temporary directory: the header is `CSV_COLUMNS`, and each tuning cell read back with `float`
    equals the stored value converted. It reuses the module's stored row. Classify as
    `STRUCTURALLY REQUIRED`.
11. **The prose (the prompt's last item).** `CLAUDE.md`: the plot-scripts bullet names the tuning
    tolerance and n among the options the scripts match; the counts and the two halves become
    08's. The four tools' docstrings and usage lines. `README.md`, `architecture-summary.md` and
    `numerical-strategies.md` are 10's and are not edited.
12. **The breakages** (§2 item 4, and two more), each a diff `git apply` replays from the agent's
    commit:
    1. an old option accepted silently: the action's `__call__` made a no-op;
    2. the warning evaluated at φ_ref = 0: the helper realizes at `0.0`;
    3. the scan resuming into an old-format CSV: the old-format check removed (the generic header
       check then refuses it with the generic message, so the test must assert the specific one);
    4. the window removed: `in_window` returns its input;
    5. `--n-values` given `action="extend"`.

    For each, the log names every test that fails and how. Classify as `STRUCTURALLY REQUIRED`.
13. **§2 item 2, the short scan**, through the new `tools/surfing_scan.py` into the agent's scratch
    directory, `--beta-values 1.50 3.00 4.00 --phi-init-Mp 5 --M 1e-3`. The table gives, per β:
    integrations and wall; each T and N column and the roots against the committed row (relative);
    the capture flag and kick; delivery; and the six tuned columns. **The tuned values must equal
    the probe's `out.json` bitwise** (normalization, V_ref, φ_ref, residuals, iterations): the same
    function on the same setup. From the walls, the predicted S1 and S5 times, with the load. No
    full scan runs.
14. **The issue this prompt opens.** `[08-a-history-that-did-not-converge-is-never-retried]` on
    this board's §3 (What: §0's bullet, with the lines; Impact: a run with a larger cap recomputes
    nothing, and `--drop scalar-history` drops every history; Next step: a retry flag for the scalar
    stage like `--retry-failed-bbn`, or a cap in the key, for the user to rule; not assigned), and
    its row in `.documents/OPEN_ISSUES.md` §1.16.
15. **The tests overall.** No Ray, stores only through `_TempStoreCase`, PRyMordial only where a
    docstring says so (none is needed: ruling 8 stubs it). Existing tests are modified only where
    08 changes what they test, each listed in the log with its reason. **No test is removed** except
    one whose subject this commit deletes, named in the log; a suite whose count falls otherwise is
    a stop. Every new tuned test is under a minute alone, with its cost in its docstring.
16. **The records.**
    - **This board:** row 08, D1–D7 and the gate "Driver"; `### 5.9 After prompt 08 (<date>)`, in
      §5.8's form; §1.1's items for 08 marked as landed; §3 for ruling 14; §3.1's row for
      `[05-surfing-scan-runs-at-a-fixed-lambda]` marked closed; §4 for it.
    - **`surfing-temperature` board:** the issue moved to §4 with its **Resolved** line, naming
      this commit, the tuned scan, and §0's finding that the three β reproduce the committed rows.
    - **`.documents/OPEN_ISSUES.md`:** the §1.15 row deleted, the §1.16 row added, the count (22)
      and the date.
    - **Not edited:** `prompts/INDEX.md`, the README, the committed CSVs, sidecars and figures of
      `.documents/surfing-temperature/out/`, the verification documents.

**The stop conditions.** The prompt's §4, its roots condition read as §0 says, with these added:
- `golden_history_05.json` or `golden_history_06_tuned.json` fails;
- the short scan's tuned values differ from the probe's;
- `tools/history_and_bbn.py` tuned at `3 0.01` does not print the tuned golden's `tuning` record
  bitwise;
- a suite's count falls (ruling 15);
- an existing test fails for a reason this note does not name. Never edit such a test to make it
  pass.

**No driver runs** beyond the `--help`s, the refusals, and `main.py --inventory` on a test-built
temporary store. No Ray is started, and no store is opened except the tests' temporary ones.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached
  --name-status` before committing. Count, then paste. A log states only what the diff and the
  tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Order.**
  1. The five suites and both golden tests alone (with their times), before any edit.
  2. Rulings 1–7 (the parser, `main.py`, the YAML, the plot inputs, the warning, the wrapper) with
     their tests.
  3. Rulings 8–10 (the history tool, the surfing tools, `histories.csv`) with their tests; the
     golden histories alone; stop here if either fails.
  4. §2 item 2's short scan (ruling 13); the suites; the breakages; the `--help`s.
  5. The records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its two
  halves. Send output to a file in the agent's scratch directory, grep the verdict from it, and
  record `uptime` beside each. A failure is re-run alone before it is called real.
  `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall-clock bound and fails
  under load; re-run it alone before calling it real. The tools suite now holds three tuned tests
  (about 25, 27 and 40 s); run it alone, in the foreground.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. No worktree is needed.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `f8fced8`, before this note was committed:
1. **The tree.** `jordan-normalization`, with `git status --short` empty.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: three runs, all finished
   (`ray-sqla-align` 02's). None is `running` or stale.
4. **The probes** of §0: the three tuned surfing histories (2 m 26 s, load 6–9; the β = 1.5 φ_ref
   equals 06's table to the printed digit); `configargparse` on a removed option, on the command
   line and as a YAML key; `extend` with a default.
5. **The baselines**: 07's review's, above.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README, this board, and 06's and 07's logs;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad,
  with the probe `probe_tuned_surfing.py`, its log and `out.json` beside it. The agent does not read
  `orchestrator/` or prompts 09–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/08-the-driver-and-the-tools.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change** are the prompt's §5, with these additions:
  `ComputeTargets/ScalarHistory.py` (`compute` only, ruling 3); `plot_by_beta.py` and
  `plot_ScalarHistory.py` (rulings 1, 5); the new `tools/tests/test_history_and_bbn.py`;
  `ComputeTargets/tests/test_plot_inputs.py`, `test_initial_field_option.py`, `test_extraction.py`
  and a new parser test module if it wants one; `ObjectFactories/tests/test_tuned_history_round_trip.py`
  (ruling 10, one test added); `RunDrivers/tests/` (including `stubbed_run.py`); `tools/tests/`;
  `prompts/surfing-temperature/IMPLEMENTATION_STATE.md` (§3 and §4 only) and
  `.documents/OPEN_ISSUES.md`.

  Not: any factory, `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, the golden
  JSONs and fingerprint files, `golden_history.py`, `golden_tuned_history.py`,
  `test_golden_history.py`, `test_tuning.py`, `ComputeTargets/BBNData.py`, `tools/bbn_from_store.py`,
  `PRyM/`, `thirdparty/`, the packages, `.documents/surfing-temperature/out/`.
- **§0's rulings** bind it, each classified as §0 says.
- **It starts no Ray, opens no store but the tests' temporary ones, and runs no driver** beyond
  the `--help`s, the refusals and `--inventory`.
- **Stop and ask** on any of §0's stop conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the golden tests' times and those of each new tuned test;
  - every test added, modified and removed, with what it asserts or asserted;
  - §2 item 2's table, its equality with the probe, and the predicted S1 and S5 times;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the text of every changed option help, refusal and warning;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git show --stat <agent>` lists only §2's files. `git status` is clean.
2. **The parser and `main.py`.** Read the removed-option action, the three new options and their
   help against rulings 1–2; the grid, the tolerance row and the cap's route through `compute`
   against ruling 3; `exponential.yaml` against ruling 4. In a scratch parse: `--n-values 2` gives
   `[2]`; a removed option on the command line and as a YAML key exits 2 naming its replacement.
3. **The plots.** `plot_inputs.py` and both scripts against ruling 5, and its tests.
4. **The warning.** Against ruling 6; compute the seed's φ_ref for one flagged model by hand, and
   check the line.
5. **The wrapper.** `describe_scope` and its tests against ruling 7; the tune-line tests; the
   refusal through the wrapper writes no manifest.
6. **`history_and_bbn.py`.** Against ruling 8; run its tuned test alone with its time; run the tool
   once by hand at `3 0.01` with a PRyMordial solve, and once with `--fixed-normalization` at the
   tuned golden's values and `--T-stop-GeV 1e-3`.
7. **The surfing tools.** Against ruling 9; the window helper read against §0; the CSV's columns and
   units; the old-format refusals of the scan and the plot. Re-run §2 item 2 from the log's
   command into the orchestrator's scratch directory and compare the CSV with the agent's
   (`wall_s` apart) and with the probe.
8. **`histories.csv`.** Against ruling 10, and its new test alone.
9. **The golden histories.** Both golden tests alone; the golden JSONs and fingerprint files
   byte-identical to `<agent>~1`'s.
10. **The breakages.** In `bash`, replay each diff, run the named tests, see them fail as the log
    says, revert, and check that `git status` is clean.
11. **The records and the suites.** This board (row, items, gate, §3, §3.1, §4, §5.9), the
    `surfing-temperature` board, the index (22 open), `CLAUDE.md`, and the log in §5.1's form, with
    every deviation classified. Then the five suites at `<agent>`, serially, at the counts the log
    states, the `--help` of `main.py`, both plot scripts, the wrapper and the four tools, and
    `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 08* paragraph in §5.9. Fix small
  residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (this board, the log, the `surfing-temperature` board, the
    index);
  - add this note to the board's list of orchestrator notes, marked "used for 08";
  - update the status line: 8 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written:
    - the roots cannot move by the units shift, which cancels in them; the tuned scan reproduces
      the committed rows to about 10⁻⁷ at three β, so the stop reads "a root moves by more than
      10⁻¹², or a capture changes" (§0);
    - the cap reaches the remote as an argument of `compute`, not through the store, and a history
      that did not converge is never retried; that is opened as an issue (rulings 3, 14);
    - the removed options are refused by a hidden action that names the replacement (ruling 1);
    - the YAML's 85-decade axis is removed and `history_and_bbn.py`'s 35-decade one replaced by
      main.py's 300, since the extent changes no tuned value (rulings 4, 8);
    - the warning is evaluated per cosmology, inside the loop (ruling 6);
    - the surfing CSV carries the tolerance and the window's edge, so that a resume can match them,
      and the window ends at the first crossing of T_J, not at a filter on T_J (ruling 9);
    - the old run script names no removed option and would build tuned histories (§0);
  - the predicted full-scan times, the test times, and the five suites' counts.
- **Then 09.** Its note is written after this review. `ray-sqla-align`, which 09's cluster needs, is
  already in this branch (`396526b` is an ancestor of `f8fced8`, through the merge `5fc02a6`).
