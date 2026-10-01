# Prompt 01 — The Hubble-only BBN route, the wall-clock limit and the output checks

**Campaign:** [`README.md`](README.md) · **Board items:** **R**, **W**, **O** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and R, W, O.
**Closes:** `[00-bbn-route-integrates-a-fictitious-np-temperature]`,
`[00-prymordial-has-no-wall-clock-limit]` and `[00-prymordial-output-is-stored-unchecked]` on
this board; and, if P4 was accepted, `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]` and
`[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]`, which are assigned from the
`run-integrity` board (README §5 rule 4 for how to close an assigned issue).
**Recommended model:** **Opus.** It is a patch to vendored code, the removal of one route through
four modules and five test files, and the version bump. The judgement is in keeping every test's
*purpose* while its subject changes, and in proving the patched route reproduces the reference.

**Read first:**

1. [`README.md`](README.md) §0.2 (U1, U2, P1–P4, P9), §0.5, §2 (a)–(e) and (m), §5, §6.0–§6.2.
2. The board's Decisions: which of P1–P4 the user accepted. **If one was overruled, follow the
   ruling and not §2.**
3. `ComputeTargets/BBNData.py`, all of it.
4. `PRyM/PRyM_main.py:1–80` (the exception class, `_check_solve_ivp`, `PRyMclass.__init__`),
   `:80–320` (`Hubble`, `dTnudt`, `dTgdt`, `dTNPdt`, the two thermodynamic branches), the other
   six `solve_ivp` sites (`grep -n "solve_ivp(" PRyM/PRyM_main.py`), and `:1345–1395`
   (`N_eff`, the results). `PRyM/PRyM_init.py:40–120`. `PRyM/PRyM_thermo.py:140–160`.
5. `git show` the commits that introduced `cham03` and `ri02`
   (`git log --oneline -- PRyM/PRyM_main.py`) to see how the existing patches are marked.
6. The tests that exercise the route: `ComputeTargets/tests/prym_fixtures.py`,
   `test_prym_passenger.py`, `test_bbn_callbacks.py`, `test_bbn_solver_failures.py`,
   `test_network_flag.py`. Note which test methods depend on `NP_thermo_flag`, `p_NP`,
   `drho_NP_dT`, `Tstart_NP` or `jordan_Hdot_over_H2`.
7. `Datastore/SQL/ObjectFactories/BBNData.py` (the `BBNDataValue` table and its `pressure_NP_MeV4`
   column), `plot_ScalarModel.py:1060–1230` (the `p_NP` panels), `main.py:640–800` (the BBN stage
   and `BBNData.compute`'s payload), `config/argument_parser.py`, `config/version.py`.
8. `prompts/science-readiness/planning-probes/` (the three probes) and README §6.1's table.

---

## 1. The changes

**R1 — the PRyMordial patch (README §2 (a)).** Mark every hunk with a comment naming
"ChamPBH science-readiness prompt 01", as `cham03` and `ri02` are marked.

- `PRyM_init.py`: `NP_hubble_flag = False`.
- `PRyM_main.py`'s `Hubble`: `rho_tot += PRyMthermo.rho_NP(Tg)` under `NP_hubble_flag`. No other
  function reads the flag.
- If P2 was accepted, `dTNPdt` goes back to the upstream body. Take it from the commented-out
  lines the `cham03` patch left. The `cham03` comments go.

**W1 — the wall-clock limit (README §2 (b)).** In the same patch:
- `PRyMWallClockLimitError(stage, elapsed, limit)` beside `PRyMSolverFailureError`;
- `PRyMclass.__init__(…, wall_clock_limit=None)`;
- a wrapper on every `fun`, and on every `jac` that is passed, of the eight `solve_ivp` calls;
- a deadline check between stages.

`None` means no limit and must leave PRyMordial's behaviour unchanged.

**R2 — the ChamPBH side (README §2 (a), (d)).**
- `_configure_PRyMordial` sets and asserts the flags of §2 (a), and no longer sets `Tstart_NP`.
- `build_NP_callbacks` becomes `build_rho_NP_callback`, returning one callable. It keeps the
  finiteness, monotonicity and domain checks, and gains the two checks of P4.
- `thermodynamic_rho_SM` keeps what is still read. If `drho_SM_dT` is no longer needed, it goes.
- `_run_PRyMordial(rho_NP, small_network, wall_clock_limit)` calls
  `PRyMclass(rho_NP, wall_clock_limit=…)`.
- `compute_BBN_data` stops computing `p_NP`: the Ḣ_J reconstruction, `V_policy` and `ODE_policy`
  go, and so does `jordan_Hdot_over_H2` (P1).
- `NPCallbacks` goes, or keeps one field. Your choice; record it.

**O1 — output checks (README §2 (c)).** In `_run_PRyMordial`, after a successful return.

**P1 consequences.**
- `SampleValues` and `BBNDataValue` lose `pressure_NP`.
- `sqla_BBNDataValue_factory` loses `pressure_NP_MeV4`, in its writes and reads.
- `plot_ScalarModel.py` loses the |p_NP| and w_NP panels and their data series. The density
  panels stay. If removing them leaves an empty figure or axis, remove that too, and say so.

**W2 — the limit, plumbed (P3).**
- `compute_BBN_data(…, wall_clock_limit: Optional[float] = DEFAULT_BBN_WALL_CLOCK_LIMIT)`, with the
  constant `600.0` in `ComputeTargets/BBNData.py`.
- `BBNData.compute` reads `wall_clock_limit` from its payload.
- `main.py` passes `args.bbn_wall_clock_limit`. A value of 0 means `None`.
- `config/argument_parser.py` gains `--bbn-wall-clock-limit` (float, default 600).
- `compute_SM_baseline` passes no limit.

**V1 — the version (P9).** `VERSION_LABEL = "2026.6.0"` with a dated sentence in
`config/version.py`'s history comment. `PRYM_VERSION = "bf24c3d+ri02+sr01"`, or
`"bf24c3d+cham03+ri02+sr01"` if P2 was overruled, and its comment names what `sr01` is.

**N1 — the driver (README §2 (m)).** `tools/history_and_bbn.py`, built from
`planning-probes/bbn_route_probe.py`. It also prints the `ratio` window lines the probe prints.
It runs the production route only; the probe's "honly" monkeypatch goes.

---

## 2. Tests

No Ray, no datastore. Every method that runs a PRyMordial solve says so in its docstring. The
count in `ComputeTargets/tests` must not fall: every test deleted because its subject is deleted
is replaced by a test of what replaced it, and the log pairs them (README §5 rule 6).

- **(a) The thermodynamic solve has two components.** Intercept `solve_ivp` in `PRyM.PRyM_main`
  (reuse `test_bbn_solver_failures`'s interceptor) on a ρ_NP ≡ 0 small-network solve. The
  thermodynamic stage is called with a two-component `y0` and is the "no NP" branch. A recording
  ρ_NP callback is called by `Hubble`. **Must fail on `HEAD~1`**, where `y0` has three
  components.
- **(b) ρ_NP ≡ 0 is plain PRyMordial.** `compute_SM_baseline(small_network=True)` against a solve
  with every NP flag off and no callbacks: every abundance identical. Two solves.
- **(c) The constant family reproduces the reference.** ρ_NP = 0.08 ρ_SM (`prym_fixtures.CONSTANT`)
  through the patched route, small network, against README §6.2's `const-honly` row (pinned in
  the test as constants with their provenance): Yp and D/H to `1e-6` relative. One solve.
- **(d) The wall-clock limit.** The constant family with `wall_clock_limit=1e-3` through
  `_run_PRyMordial` returns a failure payload whose reason begins
  `"PRyMordial: PRyMWallClockLimitError"` and names a stage, in under 5 s.
  `wall_clock_limit=None` changes nothing: (c) passes with it.
- **(e) Output checks.** With `PRyMclass` stubbed (no solve; see `test_network_flag`'s stub),
  results with Yp = 0.7, then D/H = NaN, then ⁷Li/H = 0 each give a failure payload beginning
  `"PRyMordial output:"`. A result inside the checks passes through unchanged. **Must fail on
  `HEAD~1`.**
- **(f) The callback builder.**
  - Three samples raise `ComputationFailureError` naming the count.
  - An `eos` stand-in whose `G_rho` is NaN at one `T` makes the callback raise
    `ComputationFailureError` at that `T`.
  - **Both must fail on `HEAD~1`:** the first with `ValueError`, the second by returning NaN.
- **(g) What `test_bbn_callbacks` tested still holds for ρ_NP:**
  - the exact constant ratio;
  - the oscillating bound;
  - the monotonicity refusal;
  - the domain guards;
  - units;
  - the end-to-end constant ratio;
  - the SM baseline.

  Rewrite each for the one callback.
- **(h) The fixture.** `prym_fixtures.run_prym` takes ρ_NP only, sets the flags through
  `_configure_PRyMordial`, and restores every global it touches, including `NP_hubble_flag`. The
  `_SavedPRyMGlobals` helpers in `test_bbn_callbacks.py` and `test_network_flag.py` save and
  restore the new flag.

---

## 3. What this prompt does not do

- No change to any reaction rate, network, tolerance, `T_start`, `T_end`, `t_end` or sampling
  inside PRyMordial. No change to `N_eff`.
- No change to the spline floor (prompt 06) or to which samples are used (prompt 05).
- No change to `ScalarModel`, the integrator or the adiabatic stage.
- No edit to `.documents/` (prompt 08).

## 4. Acceptance

1. README §6.2, every row, with measured values in the log.
2. **The real histories.** Run the driver on β = 2 at M = 0.5 and at 10⁻³. Yp and D/H agree with
   §6.1's "honly" rows to `1e-5` relative, and the BBN wall time is within 1.5× of §6.1's. Quote
   both sets.
3. `grep -rn "pressure_NP\|P_NP\|drho_NP_dT\|jordan_Hdot_over_H2\|Tstart_NP"` over
   `ComputeTargets/ Datastore/ plot_ScalarModel.py main.py tools/` finds nothing (P1).
   `grep -n "NP_thermo_flag" ComputeTargets/` finds only the assignment to `False`, the assert,
   and test code.
4. All three suites pass; `ComputeTargets/tests` does not fall. `black --check` is clean on the
   changed non-`PRyM/` files.
5. The board and the index: R, W, O done; the issues in the header closed.

## 5. Stop conditions — stop and ask the user

- (c) or acceptance 2 misses its tolerance. The patched route is supposed to be the "honly" route
  exactly up to the third LSODA component. A miss means something else changed.
- PRyMordial needs a change outside `Hubble`, `PRyM_init`, the wrappers and `dTNPdt` to run with
  `NP_thermo_flag` off and the Hubble flag on.
- A callback exception raised inside a `solve_ivp` RHS does not propagate out of `PRyMclass`
  (LSODA or the Fortran wrapper swallows it). Then the wall-clock limit cannot work as designed.
- A test cannot be rewritten without losing what it tested.

## 6. The log and the board

`logs/01-hubble-only-bbn-route.md`, in the README §5.1 template. State `VERSION_LABEL` and
`PRYM_VERSION` before and after. List every vendored hunk, with line numbers, so that it can be
re-applied on a PRyMordial upgrade. Name each deleted test and its replacement. In "State handed to
the next prompt", give the driver's command line, its output on β = 2 at M = 0.5 and 10⁻³, and the
callback builder's final signature.
