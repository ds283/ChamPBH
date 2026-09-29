# Log 04 — Ratio splines, fail-closed on non-monotonic T_J, the Ω″φ′² term, and a baseline

**Prompt:** prompts/review-remediation/04-ratio-splines-and-a-baseline.md
**Commit:** the commit that adds this file — "Spline the BBN new-physics ratios and add an SM baseline"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/04-ratio-splines-and-a-baseline.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-29
**Result:** COMPLETE WITH DEVIATIONS

R3 is closed. Every README §6.3 row is met:

- The callbacks spline ρ_NP/ρ_R,J and p_NP/ρ_R,J and multiply back by the thermodynamic ρ_SM(T_J).
  The asinh path is gone.
- Non-monotonic T_J is refused instead of sorted.
- The Ω″ term is Ω″π².
- A ρ_NP ≡ 0 baseline runs through the same PRyMordial settings, as a function, a script and a
  line on the `plot_by_beta.py` panels.

The deviations are all implementation choices; none touches a README §2 design fact.

**The end-to-end target is met, but it sits inside PRyMordial's own noise.** The nominal
construction reproduces prompt 03's D/H to 8.85e-5 against a target of 1e-4. Rescaling ρ_NP by
1 − 1e-8 moves D/H by 7.1e-4. See Verification and the narrowed board issue
`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`.

## What shipped

Tree at dispatch: `ec206a3`. The prompt's line numbers (`:34–39`, `:112`, `:131`, `:165`, …)
are those of `f5896bb`. Prompt 03 moved them by 14–18 lines; the text below uses `ec206a3`'s.

- **`ComputeTargets/BBNData.py` — new public symbols.**
  - `class NPCallbacks(NamedTuple)`: fields `rho_NP`, `P_NP`, `drho_NP_dT`, each
    `Callable[[float], float]`, T in MeV, returning MeV⁴, MeV⁴ and MeV³.
  - `thermodynamic_rho_SM(eos, units) -> tuple[rho_SM_MeV4, drho_SM_dT_MeV3]`. It returns two
    callables in MeV:
    - ρ_SM = `RadiationConstant · G_rho(T) · T⁴`;
    - dρ_SM/dT = `RadiationConstant · T³ · (4 G_rho(T) + dG_rho_dlogT(T))`.

    `eos` is anything with `G_rho` and `dG_rho_dlogT`: the cosmology in production, the EOS class
    in the tests. Nothing is splined or differenced here.
  - **The builder's final signature:**
    ```python
    build_NP_callbacks(
        log_T_MeV: Sequence[float],
        density_ratio: Sequence[float],
        pressure_ratio: Sequence[float],
        rho_SM_MeV4: Callable[[float], float],
        drho_SM_dT_MeV3: Callable[[float], float],
        T_min_MeV: float,
        T_max_MeV: float,
        task_label: str,
    ) -> NPCallbacks
    ```
    - **Input order.** The arrays are in solver order, decreasing T.
    - **Refusal.** If `log_T_MeV[i+1] < log_T_MeV[i]` fails for any i, it raises
      `ComputationFailureError` for the first such i. Equal neighbours are refused too. The
      message puts the pair first, with both samples' indices, ln(T/MeV) values and T:
      `T_Jordan is not strictly decreasing: sample i has log(T/MeV)=… (T=… MeV) and sample i+1
      has … [task_label]`. It is ≤ 256 characters for any reasonable label, and the label comes
      last so that truncation drops the label, not the pair.
    - **The splines.** Two cubic `make_interp_spline(k=3)`, of r and s against ln(T/MeV), built
      on the **reversed** arrays. Nothing is sorted.
    - **The callbacks.** `rho_NP = r·ρ_SM` and `P_NP = s·ρ_SM`. `drho_NP_dT = r′·ρ_SM/T + r·ρ_SM′`,
      where r′ is `density_ratio_spline.derivative()`, the analytic derivative of the interpolant.
      **No finite difference anywhere.**
    - **Guards, as before:**
      - T < 0 returns 0.0;
      - T > `T_max_MeV` or T < `T_min_MeV` raises `ComputationFailureError`, with the old wording;
      - an `OverflowError` or `ValueError` is printed and re-raised as `ComputationFailureError`.
  - `jordan_Hdot_over_H2(HEdot_over_HE2, Omega_prime, Omega_primeprime, pi, pi_prime) -> float`
    computes (Ḣ_E/H_E² − Ω′π)/A₁ + (Ω″π² + Ω′π′)/A₁², with A₁ = 1 + Ω′π.
  - `compute_SM_baseline(small_network: bool) -> dict` returns the keys `Yp_BBN`, `DOverH`,
    `He3OverH`, `Li7OverH`, `PRyM_version` and `small_network`. It is not a Ray task and stores
    nothing.
  - Private: `_configure_PRyMordial(small_network)` sets the four PRyMordial flags and returns
    `PRyM_main`, and `_zero_NP(T)` returns 0.0.
- **`compute_BBN_data`.**
  - **The sample loop.** It appends `density_NP / rhorad_Jordan` and `pressure_NP / rhorad_Jordan`
    to two new lists, `density_NP_ratio_grid` and `pressure_NP_ratio_grid`. These replace the
    `arcsinh_*_MeV4_grid` lists.
  - **Removed.** `MeV2`, `MeV4` and `last_log_T_Jordan_MeV`, and the monotonicity `print` (`:127–131`
    at `ec206a3`), which printed the same variable twice.
  - **The Ḣ_J/H_J² expression** (`:180–187` at `ec206a3`, the Ω″π at `:183`) is now a call to
    `jordan_Hdot_over_H2`, and the local `A1` goes with it. **`Ω″ π` → `Ω″ π²`.**
  - **After the loop**, inside the NP timer, it builds `thermodynamic_rho_SM(cosmology, units)` and
    calls `build_NP_callbacks` with `T_min_MeV = T_BBN_spline_min / units.MeV` and
    `T_max_MeV = T_BBN_spline_max / units.MeV`. A `ComputationFailureError` from the builder is
    printed and returned as `_failure_payload(f"BBN callbacks: {e}")`.
  - **Flags.** The four PRyMordial flag assignments move into `_configure_PRyMordial`, with their
    comments, and are applied in the same place (inside `BBN_timer`).
  - **Unchanged.** The caught exception tuple, the return dict, what is stored per redshift
    (`density_NP`, `pressure_NP` and `density_NP_ratio`; no pressure ratio is stored), the domain,
    the sampling and the pre-check.
- **`_make_spline` is deleted.** The imports `asinh`, `sinh` and `sqrt` are gone; `numpy` and
  `constants.RadiationConstant` are imported. `grep -n "sinh\|asinh" ComputeTargets/BBNData.py`
  prints nothing.
- **`plot_by_beta.py`.**
  - `--no-baseline` (`store_true`) is added to the parser locally, after `create_argument_parser()`.
  - Before the model loop, and unless `--no-baseline` is given, it calls
    `compute_SM_baseline(small_network=True)` once, prints the result, and passes it through
    `run_pipeline` → `build_plot_work` → `build_beta_plot`, whose new last parameter is
    `SM_baseline: Optional[dict] = None`.
  - `build_beta_plot` draws a black dotted `axhline` labelled "SM baseline (ρ_NP = 0)" on the Yp,
    D/H and ⁷Li/H panels.
  - Nothing is stored, and the CSV is unchanged.
- **`tools/bbn_baseline.py`** (new, and `tools/` is new). Run it as
  `./venv/bin/python tools/bbn_baseline.py [--no-small-network]`.
  - It puts the repository root on `sys.path` and refuses to run unless `PRyMrates/` is in the
    working directory.
  - It prints the version, the flag, the wall time and the four abundances to ten figures.
- **`ComputeTargets/tests/test_bbn_callbacks.py`** (new) holds nine tests, (a)–(i), one per prompt
  case §2.1–9.
- **`VERSION_LABEL`**: `"2026.2.0"` before and after (not touched). `PRYM_VERSION` is unchanged,
  `"bf24c3d+cham03"`, because nothing under `PRyM/` changed.

## Deviations from the prompt

### ρ_SM is the thermodynamic formula — IMPLEMENTATION CHOICE (the prompt's preferred option)

- **Chosen: thermodynamic.** ρ_SM = (π²/30) g_ρ(T_J) T_J⁴ and its exact derivative, through
  `cosmology.G_rho` and `dG_rho_dlogT`. There is no second interpolant.
- **Measured: the stored alternative.** A cubic spline of the integrated ln ρ_R,J against ln T,
  at 250 knots per decade. It was built from prompt 01's ρ_R witness at fixed field, integrated
  from 2×10⁴ GeV (as the pipeline starts) with the production `Xav_EOS_spline`. Scratch
  `probe_rhoSM.py` and `probe_stored_e2e.py` ran on `ec206a3` plus this diff.
  - stored/thermodynamic − 1 lies in **[−2.123e-3, −7.77e-4]** on [10 keV, 10 MeV]. The largest
    |difference| is **2.12e-3**, at 7.3 MeV. The stop condition is 1 % and is not reached.
  - On [0.02, 5] MeV the range is [−2.120e-3, −7.77e-4]. Pointwise: 0.99788 at 10 MeV and
    5 MeV, 0.99790 at 1 MeV, 0.99811 at 0.3 MeV, 0.99915 at 70 keV, 0.99922 at 20 and 10 keV.
    These are prompt 01's ρ_R witness values (0.998 / 0.999 / 0.99922).
  - End to end, constant ratio 0.08: thermodynamic gives Yp 0.2540933067, D/H 2.671263588;
    stored gives Yp 0.2540783882, D/H 2.673327025.
    - The difference is 5.9e-5 in Yp, which is the expected size. ρ_NP falls by 0.08 × ~1.5e-3
      of ρ_SM, so ΔN_eff ≈ −1e-3.
    - The difference is 7.7e-4 in D/H, which is inside PRyMordial's noise band (below).
- **Why thermodynamic.**
  - It is the prompt's preferred option, and it has an exact derivative with no second spline.
  - PRyMordial's H² is ρ_PRyM(1 + r), so r = ρ_NP/ρ_R,J is the quantity it needs. Multiplying r by
    a ρ_SM that is not exactly the pipeline's ρ_R,J changes H² at order r·ε, where ε ≈ 2e-3. This
    is the audit §2 caveat 1 argument.
- **The cost of the choice, stated so a reader can disagree.** README §2 (g)'s cancellation needs
  ρ̇_NP = −3H(ρ_NP + p_NP).
  - Writing ρ_SM = ρ_R,J(1 − δ(T)) scales ρ_NP and p_NP by the same (1 − δ). The identity then
    picks up a residual −δ̇ ρ_NP.
  - The largest |dδ/d ln T| on [0.02, 5] MeV is **1.2e-3**, at 0.178 MeV (`probe_stored_e2e.py`).
    In the derivative measure of §6.3 the residual is at most r·|dδ/d ln T|/4 ≈ **2.4e-5** for
    r = 0.08.
  - That is larger than the representation's own error (7.4e-7, oscillating).
  - It is the same order as, and smaller than, the violation already there. PRyMordial's
    identity holds only with its own H, which differs from H_J by O(ε) because ρ_PRyM ≠ ρ_R,J.
    So ρ_NP, p_NP and dρ_NP/dT stay mutually consistent "to the same order as before" in the
    sense README §2 (g) states, and the callbacks remain exact derivatives of one another.
  - The stored alternative would remove δ̇ exactly with respect to H_J, but not with respect to
    PRyMordial's H.

### The tests' ρ_SM goes through `SaikawaShirai_EOS_spline`, not `_raw_G_rho` — IMPLEMENTATION CHOICE

- **What the prompt says.** §2 asks for ρ_SM "from the Saikawa–Shirai g_ρ (import `_raw_G_rho`
  and the clamp, as the script does, so the test needs no cosmology object)".
- **What was done.** The builder needs ρ_SM′ as well as ρ_SM. The raw fit has no derivative in the
  tree except through the jax class, and differencing it would put a finite difference one step
  from the callbacks, against the prompt's rule. So the tests use `SaikawaShirai_EOS_spline(GeV_units())`
  through the production helper `thermodynamic_rho_SM`.
- **Why it is close to the prompt.**
  - The derivative is exact for the interpolant.
  - The clamp is the same (3.383 below 10 keV).
  - No cosmology object is needed, as the prompt wants.
  - It is the class whose `G_rho`/`dG_rho_dlogT` the production `Xav_EOS_spline` inherits, and
    the class prompt 03's fixture uses (option C).
- **Effect on the numbers.** The measured maxima reproduce `spline_test.py`, which uses the raw
  fit, to its printed figures: 7.85e-10 / 3.30e-8 / 2.96e-6 against 7.9e-10 / 3.3e-8 / 3.0e-6, and
  9.75e-9 / 7.40e-7 against 9.7e-9 / 7.4e-7.
- **The true derivative** used for scoring is r′ρ_SM/T + rρ_SM′, with r′ the analytic derivative
  of the synthetic ratio. The only finite difference in the new test module is in test (g), and
  it is applied to A₁(N) along a stand-in trajectory, not to any callback.

### The flag setting is factored into `_configure_PRyMordial` — IMPLEMENTATION CHOICE

- **Why.** §1.4 says the baseline "sets the same flags `compute_BBN_data` sets". Calling one
  helper from both paths makes that true by construction.
- **Rejected alternative.** Copying the four assignments, which could drift apart.
- **Effect.** No flag value or order changes. The helper is called inside `BBN_timer`, where the
  assignments were.

### `thermodynamic_rho_SM` is a module-level function — IMPLEMENTATION CHOICE

- **Why.** The prompt's builder takes two callables for ρ_SM and ρ_SM′; it does not say where they
  come from. Making their construction a pure function lets test (f) check the unit conversion,
  that is, the same MeV⁴ output from GeV-unit and Planck-unit EOS objects.
- **Rejected alternative.** Closures inside `compute_BBN_data`, which no test could reach.

### `--no-baseline` is added in `plot_by_beta.py`, not in `config/argument_parser.py` — IMPLEMENTATION CHOICE

- The shared parser is also `main.py`'s, where the switch would mean nothing. Adding it after
  `create_argument_parser()` keeps it to the one script.
- `plot_by_beta.py --help` lists it (checked).

### The plotted baseline uses `small_network=True` — IMPLEMENTATION CHOICE

- The baseline is computed once, before any rows are read, so it cannot follow each row's stored
  flag.
- `True` is what `main.py` passes (`main.py:749`).
- The flag has no effect anyway (board `[03-small-network-flag-is-never-read-by-prymordial]`), so
  the choice changes nothing today. If that issue is fixed, the baseline should follow whatever
  the pipeline then uses.

### Domain guards compare in MeV — IMPLEMENTATION CHOICE

- **What changed.** The old callbacks compared `T_in_MeV * units.MeV` against `T_BBN_spline_max`
  in the cosmology's units. The builder is unit-free, so it compares `T_in_MeV` against
  `T_BBN_spline_max / units.MeV`.
- **Effect.** The two can differ only within an ulp of a boundary. The messages are unchanged.

### Test (i) checks all four abundances — IMPLEMENTATION CHOICE

- "README §2 (f) row 1" also carries N_eff, which `compute_SM_baseline` does not return (the
  prompt lists the four abundances). The test checks those four at 1e-4.
- ⁷Li/H is the tightest, at 8.1e-5, because the README quotes it to four figures.

## Verification performed

Everything was run from the repository root with `venv/bin/python`, on `ec206a3` plus this diff.
The scratch probes are in the session scratchpad, not committed: `probe_synth.py`,
`probe_e2e.py`, `probe_rhoSM.py`, `probe_stored_e2e.py` and `probe_osc_e2e.py`.

### First, `spline_test.py` (I ran this)

`venv/bin/python .documents/audit-2026-09-29/spline_test.py` reproduced the audit's table. At
250 per decade:

- constant family: asinh 7.9e-10, ratio 5.2e-17, dρ 6.8e-8;
- oscillating family: asinh 3.3e-8 and dρ 3.0e-6; ratio 9.7e-9 and dρ 7.4e-7.

### README §6.3, row by row (I ran these; `test_bbn_callbacks`, `CHAMPBH_TEST_REPORT=1`)

| Quantity | Target | Measured | |
|---|---|---|---|
| constant ratio 0.08: max spurious ρ_NP/ρ_SM | ≤ 1e-12 | **5.19e-17** (P_NP 1.55e-17; derivative measure 1.43e-15, against ≤ 1e-9) | ✅ |
| oscillating ratio: max spurious ρ_NP/ρ_SM | ≤ 2e-8 | **9.75e-9** (P_NP 3.25e-9) | ✅ |
| oscillating ratio: max derivative error / (4 ρ_SM) | ≤ 1.5e-6 | **7.40e-7** | ✅ |
| no worse than asinh, both families (test (c)) | ratio ≤ asinh | constant: 5.19e-17 vs 7.85e-10 (ρ), 1.55e-17 vs 2.62e-10 (P), 1.43e-15 vs 6.85e-8 (dρ). Oscillating: 9.75e-9 vs 3.30e-8, 3.25e-9 vs 7.59e-9, 7.40e-7 vs 2.96e-6 | ✅ |
| non-monotonic `log_T_Jordan` | refused, reason recorded | the builder raises `ComputationFailureError` naming samples 1000 and 1001, for both a swap and an equal pair (test (d)). `compute_BBN_data` returns `_failure_payload("BBN callbacks: …")` (reasoned, see below) | ✅ |
| `Ω″` term | `Ω″ π²`; a stand-in shows the difference | new − old = Ω″π(π − 1)/A₁² to 1e-14; Ω″ = 0 gives equality. With the stand-in ln Ω = φ²/(2μ²) along a quadratic φ(N), the new A₁′/A₁² term matches a central difference of A₁(N) to 1e-8, and the old one is off by 0.2 (test (g)) | ✅ |
| end to end: ratio 0.08 through the new callbacks, Yp / D/H | 1e-4 relative | Yp **0.2540933067** (1.89e-6 vs prompt 03's 0.2540937879; 1.3e-5 vs README 0.25409). D/H **2.671263588** (**8.85e-5** vs prompt 03's 2.671499971; 8.8e-5 vs README 2.6715) | ✅ narrowly; see the next table |
| ρ_NP ≡ 0 baseline through `compute_BBN_data`'s path | a function and a script, drawn by `plot_by_beta.py` | `compute_SM_baseline(True)`: Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273, ⁷Li/H 5.423441017. Against README row 1: 1.1e-5, 2.0e-5, 4.8e-5, 8.1e-5 (test (i)). Identical to all ten figures to prompt 03's ZERO / no-NP runs. `tools/bbn_baseline.py` prints the same in 7.4 s. `plot_by_beta.py --help` shows `--no-baseline` | ✅. Drawing not run (needs a cluster) |

**The end-to-end spread** (`probe_e2e.py`). The constant ratio is 0.08 × (1 + ε), put through
`build_NP_callbacks` into `run_prym`. The columns are relative to prompt 03's patched values.

| ε | Yp | D/H ×10⁵ | ΔYp | ΔD/H |
|---|---|---|---|---|
| 0 | 0.2540933067 | 2.671263588 | 1.9e-6 | 8.9e-5 |
| +1e-9 | 0.2540889037 | 2.671607231 | 1.9e-5 | 4.0e-5 |
| −1e-9 | 0.2540901404 | 2.671487815 | 1.4e-5 | 4.6e-6 |
| +1e-8 | 0.2540890433 | 2.671248186 | 1.9e-5 | 9.4e-5 |
| −1e-8 | 0.2540913013 | 2.673399398 | 9.8e-6 | **7.1e-4** |

- The ε = 0 case is deterministic and passes, and the test pins it.
- But a change of 1e-8 in ρ_NP, far below any physical scale, moves D/H seven times further than
  the target allows. So the 1e-4 D/H target measures PRyMordial's step selection as much as the
  interface. It could fail on another platform or BLAS with no change to this code.
- This is recorded on the board. The target is not loosened.

**The oscillating ratio end to end** (`probe_osc_e2e.py`, a measurement and not a test). It
completes in 8.8 s, with N_eff 3.70224, Yp 0.2469288133 and D/H 2.789389513. Against prompt 03's
oscillating fixture (a finite-differenced analytic ρ_NP) that is 9.1e-6 in Yp and 6.1e-4 in D/H,
inside the same noise band.

**Knot count at the pipeline's density** (computed from the domain with the production EOS).

- **The grid.** The z grid is 250 per decade of Einstein-frame 1 + z. At fixed field (A′φ′ = 0),
  ln(1 + z) = ln T_J + ⅓ ln g_s + const, so there are 250 (1 + ⅓ d ln g_s/d ln T) knots per decade
  of T_J.
- **The counts:**
  - [0.02, 5] MeV (2.398 decades) holds **636 knots**, an average of 265 per decade. The local
    density runs from 250 to 309.
  - The whole domain [0.1 eV, 100 MeV] holds **2304**.
  - PRyMordial's working range [0.3 keV, 10 MeV] holds 1167.
- **A moving field.** For A′φ′ ≠ 0 the density divides by (1 + A′φ′). That is where the audit's
  "≳ 160" lower bound would come from; I did not verify that bound.
- The synthetic tests use exactly 250 per decade in T (2251 knots), the pessimistic end for a
  parked field.

**What I reasoned but did not run.**

- **The refusal inside `compute_BBN_data`.** The conversion of the builder's refusal into
  `{"failure": True, "failure_reason": "BBN callbacks: T_Jordan is not strictly decreasing: …"}`
  is not exercised. Reaching it needs a `ScalarModel` with `values`, a cosmology, a potential and
  a coupling for `PotentialDerivativePolicy` and `ODEPolicy`, which is the §5 stop condition's
  territory. The builder's own refusal is tested (d). The `except` clause is five lines and
  reuses `_failure_payload`.
- **`plot_by_beta.py`'s drawing** needs a Ray cluster and a datastore. Checked instead:
  - the file parses;
  - `black` is clean;
  - `--help` lists the switch;
  - the legend label renders through matplotlib (Agg).

**Breakage check (I ran this).** I edited `BBNData.py` in place with two changes: the monotonic
check disabled, and `pi**2` → `pi` in `jordan_Hdot_over_H2`. Tests (d) and (g) failed: (d) with
two errors, from `make_interp_spline` refusing the unsorted x, and (g) with one failure. The file
was then restored from a copy.

### The suites (I ran these)

- `ComputeTargets/tests`: **4 → 13**, `Ran 13 tests in 41.220s OK`. The nine new tests are
  (a)–(i); the four prompt 03 tests are unchanged.
- `CosmologyModels/tests`: **6 → 6**, OK.
- `black --check` is clean on `ComputeTargets/BBNData.py`, `ComputeTargets/tests/test_bbn_callbacks.py`,
  `plot_by_beta.py` and `tools/bbn_baseline.py`. The two pre-existing files were clean at `ec206a3`.

## Observations not acted on

1. **`.documents/numerical-strategies.md` §7.2–7.4 describe the removed interface.** They cover
   the asinh transform, `_make_spline`'s sort, the warn-only monotonicity check and the `sinh`
   overflow path. It is out of this prompt's files, and `.documents/` is additive (CLAUDE.md
   rule 6), so the fix is a dated addendum, not a rewrite. → board §3
   `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`.
2. **The 1e-4 end-to-end D/H target is inside PRyMordial's noise** (the spread table above). →
   board §3 `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` is narrowed with this
   measurement. Whether the target stands is the planner's decision, as that issue already says.
3. **The thermodynamic ρ_SM leaves a residual −δ̇ρ_NP in PRyMordial's cancellation**, of order
   r·1.2e-3 (Deviations, first entry). That is the same order as the existing ρ_PRyM ≠ ρ_R,J
   mismatch.
   - The numerical campaign's "ratio spline" cheap test (README §7 item 2) could run both ρ_SM
     candidates on one real history to bound it. The code change would be one line in
     `compute_BBN_data`.
   - No issue is opened: it is a recorded design consequence of the prompt's preferred choice,
     not a defect.
4. **An empty or tiny sample window would raise a bare `ValueError`.** `make_interp_spline`
   raises it for fewer than four samples, as `_make_spline` did before, and it escapes
   `compute_BBN_data` uncaught.
   - Unreachable in practice. The pre-check requires the history to run below 0.1 T_min, and the
     window [0.1 eV, 100 MeV] then holds about 2300 samples whenever T_init > 100 MeV.
   - It is covered in spirit by `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`.
     No new issue.
5. **`compute_SM_baseline` leaves PRyMordial's module flags and NP callbacks set**, as
   `compute_BBN_data` always has. Test (i) saves and restores them around the call, so the suite
   does not depend on order. No issue.

## State handed to the next prompt

- **Names and signatures** (`ComputeTargets/BBNData.py`):
  - `build_NP_callbacks(log_T_MeV, density_ratio, pressure_ratio, rho_SM_MeV4, drho_SM_dT_MeV3,
    T_min_MeV, T_max_MeV, task_label) -> NPCallbacks(rho_NP, P_NP, drho_NP_dT)`. The inputs are in
    decreasing T, and non-monotonic input raises `ComputationFailureError`.
  - `thermodynamic_rho_SM(eos, units) -> (rho_SM_MeV4, drho_SM_dT_MeV3)`.
  - `jordan_Hdot_over_H2(HEdot_over_HE2, Omega_prime, Omega_primeprime, pi, pi_prime)`.
  - `compute_SM_baseline(small_network) -> {"Yp_BBN", "DOverH", "He3OverH", "Li7OverH",
    "PRyM_version", "small_network"}`.
- **The ρ_SM choice:** thermodynamic.
  - Stored/thermodynamic − 1 is in [−2.12e-3, −7.8e-4] on [10 keV, 10 MeV], at fixed field from
    2×10⁴ GeV; these are prompt 01's ρ_R witness values.
  - End to end, the two candidates differ by 5.9e-5 in Yp and 7.7e-4 in D/H.
- **The SM baseline** (`./venv/bin/python tools/bbn_baseline.py`, 7–10 s): Yp 0.2468872958,
  D/H ×10⁵ 2.462251065, ³He/H ×10⁵ 1.042050273, ⁷Li/H ×10¹⁰ 5.423441017. `PRyM_version`
  `"bf24c3d+cham03"`. It is identical to prompt 03's ρ_NP ≡ 0 and no-NP runs.
- **The constant 0.08 family through the production callbacks:** Yp 0.2540933067, D/H ×10⁵
  2.671263588. Under 1e-8 perturbations of ρ_NP, D/H moves by up to 7.1e-4.
- **The synthetic maxima** at 250 knots per decade, on [0.02, 5] MeV:

  | family | representation | ρ | P | dρ |
  |---|---|---|---|---|
  | constant | ratio | 5.2e-17 | 1.6e-17 | 1.4e-15 |
  | constant | asinh | 7.85e-10 | 2.62e-10 | 6.85e-8 |
  | oscillating | ratio | 9.75e-9 | 3.25e-9 | 7.40e-7 |
  | oscillating | asinh | 3.30e-8 | 7.59e-9 | 2.96e-6 |

  Reproduce with
  `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_bbn_callbacks -v`.
- **Knots at the pipeline's density, fixed field:** 636 in [0.02, 5] MeV, 2304 over the domain.
- **Suite counts after this prompt:** `ComputeTargets/tests` 13 (about 41 s, six PRyMordial
  solves); `CosmologyModels/tests` 6.
- `tools/` now exists; `tools/bbn_baseline.py` is its only file.
