# Log 03 — The adiabatic mass: the source-response term

**Prompt:** prompts/production-readiness/03-adiabatic-source-response.md
**Commit:** the commit that adds this file ("Add the source-response term to the adiabatic mass"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE WITH DEVIATIONS

The deviation that sets the Result is a README §6.3 row-2 witness that is missed, narrowly. For
`SaikawaShirai_EOS_spline` and the base-class formula, the h = 1e-4 central difference of `w` is
itself up to 1.12e-6 off between 120.0 and 122.2 MeV, against the 1e-6 bound. That class is not
the production one. The miss is the witness's h² truncation error: it is 1.1e-8 at h = 1e-5, and
`dw_dlogT` is the exact derivative of the class's w. The threshold was not rewritten. The test
uses h = 1e-5 in that window only and reports the h = 1e-4 value. The miss is opened as board §3
`[03-spline-eos-derivative-witness-misses-1e-6-above-the-120-mev-join]`.

**The derivation and the reference agree with README §2 (c)** (§1, and Verification, "The derivation against the reference").
Neither stop condition of prompt §6 was met. Every other §6.3 row is met. Test (e) asserts
inside the planning probe's window; the end-sample figure is reported under Deviations 3.

## 1. The derivation (written before any code, on `5aba202`)

### 1.1 The three response relations, re-read from `ComputeTargets/ScalarModel.py`

Notation: N is the Einstein-frame e-fold number (the ODE's independent variable); a prime on a
field quantity is d/dφ; φ′ along the trajectory is written π (the state's `pi_Einstein`,
dφ/dN). ρ_R ≡ ρ_R,E, ρ_m ≡ ρ_m,E, f_m = ρ_m/ρ_R, T ≡ T_J.

`ODERHS.__call__` (`ScalarModel.py:352–363`) integrates

- d ln ρ_R/dN = Σ − 4 + Σ (ln Ω)′ π,
- d ln f_m/dN = (1 − Σ)(1 + (ln Ω)′ π),
- d ln T/dN = −(1 + (ln Ω)′ π)/(1 + x), with x = `dG_s_dlogT / G_s / 3` = ⅓ d ln g_s/d ln T.

(ln Ω)′ π dN = d ln Ω along the trajectory. Separate each right-hand side into the part at fixed
field (the dN part) and the part proportional to d ln Ω. The second is the response to δφ at fixed
Einstein-frame scale factor (fixed N):

1. **Radiation.** δ ln ρ_R = Σ δ ln Ω, so d ln ρ_R/d ln Ω = Σ.
2. **Matter.** ln ρ_m = ln f_m + ln ρ_R, so d ln ρ_m/dN = (1 − Σ) + (1 − Σ)(ln Ω)′π + Σ − 4 +
   Σ(ln Ω)′π = −3 + (ln Ω)′ π. Hence δ ln ρ_m = δ ln Ω: d ln ρ_m/d ln Ω = 1 (ρ_m ∝ Ω/a_E³).
3. **Temperature.** (1 + x) d ln T = −(dN + d ln Ω). Since (1 + x) d ln T = d[ln T + ⅓ ln g_s],
   this integrates to T Ω a_E g_s(T)^{1/3} = const, which is entropy conservation in the Jordan
   frame (a_J = Ω a_E). At fixed a_E: d ln T/d ln Ω = −1/(1 + x).

These are the three relations README §2 (c) states. I agree with all three.

### 1.2 The force and its φ-derivative

`ODEPolicy.__call__` (`ScalarModel.py:195–275`): `kicking_term = −3 M_P² E (ln Ω)′ R` with
R = (Σ + f_m)/(1 + f_m) and E = G − V/(3H²M_P²). `HubblePolicy` gives
3M_P²H² = (V + ρ_R(1 + f_m))/G, so 3M_P²H² E = 3M_P²H² G − V = ρ_R (1 + f_m). Therefore

  kicking_term · H² = −(ln Ω)′ (Σ ρ_R + ρ_m),

and the force the ODE integrates is V_eff′ = V′ + (ln Ω)′ (Σ ρ_R + ρ_m). Its φ-derivative at
fixed a_E and fixed comoving entropy, with Σ = Σ(T) and Σ_T ≡ dΣ/d ln T:

  ∂_φ[(ln Ω)′ (Σ ρ_R + ρ_m)]
   = (ln Ω)″ (Σ ρ_R + ρ_m) + (ln Ω)′ [ ρ_R ∂_φΣ + Σ ∂_φρ_R + ∂_φρ_m ]

with, from §1.1,

- ∂_φ Σ = Σ_T ∂_φ ln T = −Σ_T (ln Ω)′/(1 + x),
- ∂_φ ρ_R = Σ ρ_R (ln Ω)′,
- ∂_φ ρ_m = ρ_m (ln Ω)′ = f_m ρ_R (ln Ω)′.

**Result, physical variables:**

  M²_eff ⊃ (ln Ω)″ (Σ ρ_R,E + ρ_m,E) + (ln Ω)′² ρ_R,E [ Σ² − Σ_T/(1 + x) + f_m ].

**In the code's variables** (divide by H², use ρ_R/H² = 3M_P² E/(1 + f_m)):

  conformal_mass = 3 M_P² E [ (ln Ω)″ R + (ln Ω)′² (Σ² − Σ_T/(1 + x) + f_m)/(1 + f_m) ],

where Σ_T = −3 dw/d ln T. Write B ≡ Σ² − Σ_T/(1 + x) for "the bracket". The first term is what
`AdiabaticHistory.py:103` had; the second is new.

**This is README §2 (c)'s closed form, term for term.** No third form.

### 1.3 Limits

- **f_m → ∞.** (B + f_m)/(1 + f_m) → 1, and 3M_P² E (ln Ω)′² → (ln Ω)′² ρ_R(1 + f_m)/H², so the
  new term → (ln Ω)′² f_m ρ_R/H² = (ln Ω)′² ρ_m,E/H². For the exponential coupling,
  (ln Ω)′ = β/M_P: **β² ρ_m,E/(M_P² H²)**, the textbook matter-coupled chameleon mass. At finite
  f_m the relative difference from it is exactly B/f_m.
- **Σ = Σ_T = f_m = 0.** The new term is (ln Ω)′² ρ_R · 0 = 0. (So is the (ln Ω)″ term.)

### 1.4 Where audit §5's form differs, and why

Audit §5 writes the bracket as B_audit = Σ (4 − d ln(ρ_J − 3p_J)/d ln T_J) = Σ(4 − d ln(Σ ρ_J)/d ln
T_J), using Σ ρ_R,E = Ω⁴ (ρ_J − 3p_J). Differentiating Ω⁴ (ρ_J − 3p_J)(T_J) at fixed a_E gives
d ln(Σρ_R,E)/d ln Ω = 4 + [d ln(Σρ_J)/d ln T_J] · (d ln T_J/d ln Ω). The audit's form follows if
d ln T_J/d ln Ω = −1, i.e. T_J ∝ 1/(Ω a_E) = 1/a_J. The ODE's temperature law has instead
d ln T_J/d ln Ω = −1/(1 + x) (§1.1, relation 3). With the correct response the same route gives

  Σ [ 4 − (1/(1 + x)) d ln(Σρ_J)/d ln T_J ] = Σ [4 − 3(1 + w)(1 + x)/(1 + x)] − Σ_T/(1 + x)
                                           = Σ² − Σ_T/(1 + x) = B,

using d ln ρ_J/d ln T_J = 3(1 + w)(1 + x) (from dρ = T ds with s ∝ g_s T³) and 4 − 3(1 + w) = Σ.
So **the audit's form is missing the factor 1/(1 + x) on the log-derivative**. The two agree where
x = 0 (and where the g's are thermodynamically consistent with Σ). README §2 (c) says the same.
Evaluated as the audit writes it, with ρ_J ∝ g_ρ T⁴, B_audit = −Σ_T − Σ d ln g_ρ/d ln T; this is
the form measured in test (f).

### 1.5 The A3 identity (Q's numerator)

`compute_adiabatic_values` takes A = m ≡ M²_eff/H² and C = 1 + ½ d ln|M²_eff|/dN, where M²_eff =
H² m. For m ≠ 0,

  A·C = m + ½ m [d ln H²/dN + d ln|m|/dN] = m + m Ḣ/H² + ½ dm/dN,

since d ln H/dN = (Ḣ/H)/H = Ḣ/H² and m d ln|m|/dN = dm/dN. So

  **A·C = m (1 + Ḣ/H²) + ½ dm/dN**,

which extends continuously through m = 0, where Q = ½ |dm/dN| / |k_p²/H²|^{3/2}. In the code,
Ḣ/H² = `Hdot_over_H2_plus_3` − 3 (`PotentialDerivativePolicy.py:399`; the gravitational mass
1 − (Ḣ/H² + 3) = −(2 + Ḣ/H²) uses the same quantity). This agrees with README §2 (e).

## What shipped

`VERSION_LABEL`: **`"2026.3.0"` before and after** (not bumped; `main.py:89`, `plot_by_beta.py:79`).

- **A1 — `dw_dlogT(T: TemperatureLike) -> float`, d w/d ln T, dimensionless, on every EOS class.**
  - `CosmologyModels/GenericEOS/GenericEOS.py:102`, `GenericEOSBase.dw_dlogT` (non-abstract):
    (4/3)(g_s/g_ρ)[(dg_s/d ln T)/g_s − (dg_ρ/d ln T)/g_ρ], from `G_rho`, `G_s`, `dG_rho_dlogT`,
    `dG_s_dlogT`. No freeze.
  - `SaikawaShirai_EOS_spline.py:227`: raises for T ≤ 0 as `w` does. It returns exactly 0.0 at
    and below `_EOS_T_LO` = 2 MeV, where `w` freezes its argument. Above it, it returns
    `GenericEOSBase.dw_dlogT(self, T)`.
  - `Xav_EOS_spline.py`:
    - `__init__` builds `self._dspline = self._spline.derivative()` once, beside the w spline,
      which is in ln T;
    - `dw_dlogT` (`:110`) raises for T ≤ 0, returns exactly 0.0 at and beyond `_T_min` and
      `_T_max` (where `w` returns 1/3 without the spline), and `float(self._dspline(ln T_GeV))`
      between.
  - `SaikawaShirai_EOS_jax_autodiff.py`:
    - a module function `_jax_raw_w(T_in_GeV)` = 4 g_s/(3 g_ρ) − 1 from the class's raw jax
      fits, the same expression as its `w`;
    - `self._grad_raw_w = grad(_jax_raw_w)` in `__init__`;
    - `dw_dlogT` returns 0.0 at and below 2 MeV (the same freeze) and
      `float(T_GeV · grad(T_GeV))` above it.
  - `LambdaCDM_GenericEOS.py:118`: `dw_dlogT` forwards to `self._eos.dw_dlogT(T)`.
  - No finite difference of `w` anywhere in production code.
- **A2 — the term** (`ComputeTargets/AdiabaticHistory.py`).
  - New module-level pure function (`:25`):
    `conformal_mass_over_H2(three_MP_sq, E, Sigma, fm, d_logOmega_dphi, d2_logOmega_dphi2, Sigma_T, x) -> float`.
    It returns `3M_P² E (ln Ω)″ R` (the old expression, same operand order) plus
    `3M_P² E (ln Ω)′² S`, with S = (Σ² − Σ_T/(1 + x) + f_m)/(1 + f_m). S is guarded for
    f_m > 10 as (1 + B/f_m)/(1 + 1/f_m), exactly as R is.
  - `AdiabaticComputePolicy.M2eff_over_H2(self, phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm, T_Jordan)`:
    - it gains a required `T_Jordan`;
    - it reads `coupling.d_logOmega_dphi` and computes Σ_T = −3 `cosmology.dw_dlogT(T_Jordan)` and
      x = `dG_s_dlogT/G_s/3` at T_Jordan;
    - the conformal mass is `conformal_mass_over_H2(...)` (was `CONST_3_MP_SQ * E * d2_logOmega_dphi2 * R`
      at `:103`);
    - R's computation moved into the pure function, unchanged;
    - the self mass, the gravitational mass, G and the E clamp are unchanged;
    - the comment block now states the source-response line and that it was missing before this
      prompt.
  - New `AdiabaticComputePolicy.Hdot_over_H2(phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm) -> float`
    = `V_policy.Hdot_over_H2_plus_3(...) − 3`.
  - `compute_adiabatic_values` passes `T_Jordan = exp(value.log_T_Jordan)`, never anything from z.
- **A3 — Q's numerator in its smooth form.**
  - New module-level pure function (`:85`):
    `Q_numerator(raw_N_grid, M2eff_over_H2_grid, Hdot_over_H2_grid) -> List[float]`. It returns
    A·C = m(1 + Ḣ/H²) + ½ √(1 + m²) d asinh m/dN at each sample. d asinh m/dN is the derivative
    of a `make_interp_spline` cubic of asinh m against N, evaluated at the samples, and
    √(1 + m²) is `hypot(1, m)`.
  - `compute_adiabatic_values`:
    - `log_abs_M2eff_grid`, the `log(fabs(H2 * M2eff))` append, the log|M²| spline and its
      derivative, `A` and `C` are gone;
    - it collects `Hdot_over_H2_grid` from `policy.Hdot_over_H2`, calls `Q_numerator` once, and
      sets `abs_Q = fabs(AC_grid[i] / B2)`;
    - B and B2 = |B|^{3/2} are unchanged, as are `Q_labels`, the returned dict and what is
      stored;
    - `value.H_Einstein` is no longer read here;
    - `math.log` is no longer imported, and `asinh`, `hypot` and `Sequence` are.
    - Nothing on the path to Q takes a log of |M²_eff|. No history is failed, floored or
      clipped because M²_eff crosses zero.
- **A4.** `main.py:86–88`: one dated comment sentence under prompt 02's: from 2026.3.0,
  `AdiabaticHistory`'s M²_eff includes the source response and Q's numerator is smooth through
  a sign change. No label change.
- **A5 — documents, additively.**
  - `.documents/numerical-strategies.md`:
    - a new §5.2.1, "Added 2026-09-30 (production-readiness prompt 03)", with the four pieces,
      Σ_T's source, the smooth numerator, the correction to §5.3, and the three assumptions of
      README §2 (h);
    - a one-line dated note at the end of §5.3 pointing to it. The original text of §5.2 and
      §5.3 is untouched.
  - `.documents/numerical-methods-for-paper.md` §5: a dated addendum beneath the H5 bullet. It
    covers the term as implemented, the missing 1/(1 + x), the bracket's range, the matter
    limit, the "without loss" claim and what the code does now, the three assumptions, and the
    fixed-k_p/H question.
- **Tests.**
  - `CosmologyModels/tests/test_eos_w_derivative.py` (new, 6 tests): Xav, Xav beyond the table,
    the spline class, the base formula, jax (`skipUnless`), and the cosmology forwarding.
  - `ComputeTargets/tests/test_adiabatic_mass.py` (new, 9 tests): (a), (b), (c), (d), (f) in
    `TestAdiabaticMass`, and four tests of (e) in `TestQNumerator`.
- **Probes kept beside the log:** `logs/03-probes/join_window_probe.py` and
  `logs/03-probes/spline_order_probe.py`.
- **Boards and index.**
  - This board: P3, row 03, the header's consequence line, two §3 issues and the §4 record.
  - `.documents/OPEN_ISSUES.md`: the assigned row deleted, two rows added, count 14 → 15.
  - The `review-remediation` board: one **Resolved** line.
- **Not in the diff:** `ComputeTargets/ScalarModel.py`, `PotentialDerivativePolicy`, `PRyM/`,
  `thirdparty/`, any schema.

## Deviations from the prompt

### 1. STRUCTURALLY REQUIRED — the §6.3 row-2 witness cannot resolve 1e-6 just above 120 MeV

- **What the prompt assumed.** A central difference of `w` in ln T with half-step 1e-4 resolves
  `dw_dlogT` to 1e-6 for `SaikawaShirai_EOS_spline` and the base formula above 2 MeV.
- **What was there.**
  - The spline class's g_ρ and g_s splines are fitted, at 250 samples per decade, across the raw
    Saikawa–Shirai fits' jump at 120 MeV, and ring there. The spline is smooth, but w‴ is
    large on the intervals just above the join.
  - The h = 1e-4 witness's own truncation error (h²/6 · w‴) reaches **1.121e-6**. That is the
    maximum on 200001 points over (2 MeV, 20 TeV], at 119.99 MeV. It exceeds 1e-6 on
    [119.99, 122.2] MeV and 1e-7 on [118.9, 123.3] MeV (`join_window_probe.py`).
  - At h = 1e-5 the same comparison gives 1.12e-8. Richardson extrapolation from h = 1e-4 and
    5e-5 gives 7.4e-8. The difference is the witness's, not `dw_dlogT`'s, which is the spline's
    exact derivative. The test grid has one point, 120.9 MeV, in the window. The h = 1e-4
    witness gives 1.104e-6 there.
- **What was done.**
  - **The threshold was not rewritten.** At the grid points within a factor 1.03 of 120 MeV
    (only that one), the test's witness uses h = 1e-5. It asserts ≤ 1e-6 there and measures
    1.10e-8. It reports the h = 1e-4 value and does not assert it.
  - Everywhere else the witness is h = 1e-4, asserted.
  - The production class, `Xav_EOS_spline`, is not affected: 3.4e-8 at h = 1e-4 over the whole
    grid.
  - Opened as §3 `[03-spline-eos-derivative-witness-misses-1e-6-above-the-120-mev-join]`, for the
    user to accept the h = 1e-5 witness in the window, or to restate the row. It touches a §6.3
    witness, not a §2 design fact: §2 (d)'s definition of the spline class's derivative is
    implemented as written.

### 2. IMPLEMENTATION CHOICE — dm/dN from a cubic spline of asinh m

- **Alternatives considered.** All figures are from `spline_order_probe.py`, as error in A·C at
  the samples relative to max |A·C|, "inside" meaning N ∈ [0.5, 11.5].
  - **A plain spline of m.** 7.0e-4 on the spike history (README §2 (e) table) — fails the 1e-4
    target. Rejected.
  - **A quintic (k = 5) spline of asinh m.** Better on smooth data: 2.1e-5 at all samples of
    the crossing history, and 2.8e-8 inside. Worse on under-resolved features: 4.2e-4 against
    9.3e-5 on a spike 0.03 wide, and 6.3e-3 against 1.4e-3 on one 0.02 wide. A bounce is the
    feature the diagnostic exists for. Rejected.
  - **An analytic dm/dN along the trajectory.** It needs V‴, the derivative of the conformal
    term along the path (d²w/d ln T², dx/d ln T), and π′ from the ODE. That is a much larger
    change, with more EOS derivatives to validate. Rejected.
- **The pick.** The cubic asinh spline: 3.1e-6 (crossing, inside), 6.3e-6 (spikes), and
  ≤ 1e-4 on spikes down to width 0.03.
- **One cost, recorded rather than hidden.** On under-resolved spikes that do not cross zero the
  old log route is about twice as accurate: 3.5e-5 against 9.3e-5 at width 0.03, and 8.1e-4
  against 1.4e-3 at width 0.02. The log route cannot be kept (README §4), and on the specified
  spike history the two agree to 5.8e-6.
- `make_interp_spline`'s default k = 3 and not-a-knot end conditions match what the old route
  used.

### 3. IMPLEMENTATION CHOICE — where test (e)'s accuracy is asserted

- **What the prompt left open.** It says "A·C against its analytic value is ≤ 1e-4 of max
  |A·C|", citing the planning probe. The probe measures on N ∈ [0.5, 11.5], between samples.
- **The pick.**
  - The test asserts at the samples, which is where Q is evaluated, with N ∈ [0.5, 11.5]:
    3.070e-6 (crossing) and 6.304e-6 (spikes).
  - It reports the error at all samples. On the crossing history the cubic spline's not-a-knot
    end condition gives **2.258e-4 at the last sample** and 1.81e-4 at the first, above the 1e-4
    target. The spike history's ends are flat, and its error at all samples is 6.304e-6.
- **Alternatives considered.**
  - Asserting at every sample would fail the crossing history on its two end samples.
  - A quintic spline fixes the ends but is rejected (Deviation 2).
  - Clamping the end derivatives with one-sided estimates is possible, but it adds a finite
    difference and an end rule to production code for samples that do not set max |Q|.
- **Opened** as §3 `[03-q-numerator-end-samples-carry-the-spline-end-condition-error]`, so it is
  not lost.
- **Read this as a miss if the row is meant over every sample.** I read the row as the probe's
  window, since the target cites the probe's measurement.

### 4. IMPLEMENTATION CHOICE — two pure functions, and `Hdot_over_H2` on the policy

- **What.** The conformal mass is factored into `conformal_mass_over_H2`, as the prompt
  suggested, and A·C into `Q_numerator`. `AdiabaticComputePolicy` gains `Hdot_over_H2`.
- **Why.** The loop needs Ḣ/H² per sample, and a policy method keeps `compute_adiabatic_values`
  free of `V_policy` internals. It also gives test (e)'s stand-in policy a single seam to
  replace.
- **Alternative.** Compute Ḣ/H² inline from `policy.V_policy`. Equivalent; rejected for the
  seam.
- **How the tests use them.** Test (b) goes through `M2eff_over_H2` itself, not the pure
  function, so it scores the ingredients (Σ_T, x at T_Jordan) too. That needs no separate check
  that the method calls the function correctly.

### 5. IMPLEMENTATION CHOICE — the tests' call paths, so that they fail on `HEAD~1` for the right reason

- **Test (b)/(c)/(d).** They pass `T_Jordan` only if `inspect.signature` shows the method takes
  it. On `HEAD~1`'s `AdiabaticHistory.py` the old formula runs and fails on the missing term.
- **Test (e).** It reaches the loop through `compute_adiabatic_values._function`, patching the
  module's `AdiabaticComputePolicy` with a stand-in:
  - its `M2eff_over_H2` returns the sample's `phi_Einstein`, which the stand-in history sets to
    m;
  - its `Hdot_over_H2` returns −2.
  - That path exists on both trees. It compares |A·C| recovered as |Q|·|B|^{3/2}.
- **The signed comparison, and the comparison with a copy of the old log route**, go through
  `Q_numerator`. It is imported inside that test, so that the module still loads on `HEAD~1`.
- **One mechanical point.** `ComputeTargets/__init__.py` re-exports the class `AdiabaticHistory`,
  which shadows the submodule, so `import ComputeTargets.AdiabaticHistory as m` binds the
  class. The test gets the module with `importlib.import_module`.

### 6. IMPLEMENTATION CHOICE — the jax case runs on the full ≥ 1000-point grid, about 90 s

- **The conflict.** §3.1 says "a few seconds" and also "≥ 1000 points" for every class, the jax
  class included. The jax class's `w` and its gradient are dispatched op by op, at roughly
  0.1 s per point above 2 MeV. The spline classes take about 2 s.
- **The pick.** I kept the acceptance row's ≥ 1000 points and said so in the docstring: the
  module takes 92 s.
- **Alternative.** A sparser jax grid (e.g. 200 points, about 20 s). It is a one-line change
  (`_grid_GeV` for that test) if the user prefers speed.

### 7. IMPLEMENTATION CHOICE — the base formula is asserted over the whole grid

- **What.** §6.3 asks for the base formula above 2 MeV. It has no freeze, so the test asserts it
  over all 1010 points (12 keV–20 TeV), which includes that range. It measures 1.10e-8, with
  the same 120 MeV window as Deviation 1.

### 8. IMPLEMENTATION CHOICE — two scratch probes kept beside the log

- **What.** `logs/03-probes/join_window_probe.py` and `spline_order_probe.py` are the provenance
  for the numbers in Deviations 1–3 that the tests do not print, as the logs README allows.

No UNINTENDED DRIFT.

## Verification performed

All on `5aba202` plus this prompt's diff, from the repository root with `venv/bin/python`,
2026-09-30. "Ran" means I ran it and quote its output.

### Before any code

- `planning-probes/h5_bracket_probe.py` (ran, about 2 s, not 60): Table 1 as README §2 (c).
  - h = 1e-3 / 1e-4 / 1e-5 give 9.887e-6 / 9.885e-8 / 9.944e-10, at 121.4 MeV.
  - The spline's dΣ/d ln T against a central difference: 1.012e-7 at 150.1 MeV.
  - B min −0.4065 at 145.9 MeV, max 0.3498 at 229.6 MeV.
- `planning-probes/q_sign_change_probe.py` (ran). The error columns are log-spline, m-spline,
  asinh-spline:
  - crossing: 1.8, 3.89e-8, 1.06e-5;
  - spikes: 2.83e-6, 6.96e-4, 9.25e-6.
- Suites on `5aba202` (ran): CosmologyModels **12, OK**; ComputeTargets **21, OK**.

### The derivation against the reference

§1 gives README §2 (c)'s closed form. The reference of test (a) is built from `w` and `G_s`
only: a root-find of T Ω g_s^{1/3} = const, ρ_R,E by Simpson's rule on d ln ρ_R,E = Σ d ln Ω,
ρ_m,E ∝ Ω, and a central difference in φ. It agrees with the closed form, so **neither stop
condition of prompt §6 applies**.

`CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_adiabatic_mass`
(ran, 9 tests, OK, 2.6 s) printed:

**The reference's convergence** (test (a); exponential coupling, f_m = 0, 1000 points over
[12 keV, 20 TeV]):

| h in ln Ω | max \|B_reference − B_code\| | at T | successive difference of the reference |
|---|---|---|---|
| 1e-3 | 9.571e-6 | 121.8 MeV | \|B(1e-3) − B(1e-4)\| = 9.475e-6 |
| 1e-4 | 9.570e-8 | 121.8 MeV | \|B(1e-4) − B(1e-5)\| = 9.475e-8 |
| 1e-5 | 9.467e-10 | 121.8 MeV | ratio **100.00** (h²: 100); asserted in [50, 200] |

**Table 1, re-measured.** B_code is `M2eff_over_H2` − self − gravitational, divided by
3M_P²E β²; B_ref is the reference at h = 1e-4. Σ_T is from `dw_dlogT`, and B_audit is audit §5's
form.

| T/GeV | Σ | Σ_T | x | B_audit | B_code | B_ref |
|---|---|---|---|---|---|---|
| 3e-4 | 0.0676 | −0.0750 | 0.1062 | 0.0513 | 0.0724 | 0.0724 |
| 1.6e-4 | 0.1007 | 0.0005 | 0.2202 | −0.0770 | 0.0097 | 0.0097 |
| 1e-4 | 0.0680 | 0.1350 | 0.1987 | −0.1836 | −0.1080 | −0.1080 |
| 0.25 | 0.2321 | −0.3153 | 0.2106 | 0.1823 | 0.3143 | 0.3143 |
| 0.18 | 0.3144 | 0.0311 | 0.4243 | **−0.4405** | **0.0770** | 0.0770 |
| 0.14 | 0.2001 | 0.6371 | 0.4988 | −0.9534 | −0.3850 | −0.3850 |
| 0.1 | 0.0908 | 0.1183 | 0.1386 | −0.1592 | −0.0957 | −0.0957 |
| 80 | 0.0324 | −0.0218 | 0.0359 | 0.0185 | 0.0221 | 0.0221 |
| 53 | 0.0374 | 0.0003 | 0.0580 | −0.0068 | 0.0011 | 0.0011 |
| 40 | 0.0348 | 0.0188 | 0.0609 | −0.0252 | −0.0165 | −0.0165 |

This is the probe's Table 1 to every printed digit, now through the shipped code and an
independent reference.

**The audit's form (test (f), measured, not asserted):** max |B_audit − B_reference| = **0.7327**
at 153.8 MeV (B_audit −1.0765, B_reference −0.3438). The audit's form has the wrong sign at the
QCD peak (180 MeV). Where x is small it is close (53 GeV: −0.0068 against 0.0011).

**The bracket's extremes** (reference, f_m = 0, 1000-point grid): **min −0.4067 at 144.3 MeV,
max +0.3498 at 230.4 MeV**. The probe's 1500-point grid gave −0.4065 at 145.9 MeV and 0.3498 at
229.6 MeV.

### README §6.3, row by row

| Row | Target | Measured | Witness |
|---|---|---|---|
| `dw_dlogT`, `Xav_EOS_spline`, ≥ 1000 points over [12 keV, 20 TeV], h = 1e-4 | ≤ 1e-6 | **3.375e-8** at 143.0 MeV (1010 points) | `test_eos_w_derivative.test_xav_spline` |
| same, `SaikawaShirai_EOS_spline`, above 2 MeV | ≤ 1e-6 | **1.104e-6 at 120.9 MeV at h = 1e-4: missed** (Deviation 1). Asserted: 1.102e-8 with h = 1e-5 in the 120 MeV window, and ≤ 1e-6 at h = 1e-4 at the 1009 other points. 766 points above 2 MeV; exactly 0 at the 244 at or below and at 2 MeV itself | `test_saikawa_shirai_spline` |
| same, base formula | ≤ 1e-6 | as the spline class (1.102e-8 / 1.104e-6), over all 1010 points | `test_base_class_formula` |
| same, jax class | ≤ 1e-6 | **7.05e-9** at 118.4 MeV, 1010 points less those within 2h of 2 MeV and of 120 MeV; exactly 0 at and below 2 MeV | `test_jax_autodiff` (jax 0.9.0 imports) |
| conformal part vs reference, exponential, f_m ∈ {0, 1, 100}, bracket norm, h = 1e-4 | ≤ 1e-6 | **9.570e-8 / 4.702e-8 / 1.950e-9** (worst at 121.8, 121.8, 141.3 MeV); probe 9.9e-8 | test (b) |
| same, stand-in with (ln Ω)″ ≠ 0 | ≤ 1e-6 | **7.589e-8 / 3.617e-8 / 3.762e-9**, in the norm \|Δ(M²/H²)\| / (3M_P²E[(ln Ω)′² + \|(ln Ω)″\|]), ln Ω = 2φ/M_P + 1.5(φ/M_P)² at φ = 0.5 M_P | test (c) |
| f_m → ∞, exponential, f_m = 1e6 | 1e-6 relative | **4.067e-7** at 144.3 MeV (= \|B_min\|/f_m, as §1.3 predicts) | test (d) |
| A·C vs analytic, ΔN = ln 10/250, both histories | ≤ 1e-4 of max \|A·C\| | **3.070e-6** (crossing) and **6.304e-6** (spikes), at samples with N ∈ [0.5, 11.5], through both `Q_numerator` and `compute_adiabatic_values._function`. At all samples: 2.258e-4 (crossing, the end sample) and 6.304e-6 (Deviation 3) | test (e) |
| M²_eff exactly 0 at a sample | finite Q = ½(dm/dN)/(k_p/H)³ to 1e-4, not failed | **\|Q\| = 0.005235963408 against 0.005235987756, relative 4.65e-6**; every \|Q\| finite, nothing raised | `test_e_exact_zero` |
| the audit's form | measured and reported | 0.7327 at 153.8 MeV (above) | test (f) |
| `ScalarModel.py` in the diff | absent | absent (`git diff --stat HEAD~1 HEAD` below) | git |

**Prompt §6's spike-case stop condition** (the smooth form against the old log route, with no
sign change): they agree to **5.785e-6** of max |A·C|, against a bound of 1e-4, so the condition
is not met. On the crossing history the old route is off by 1.529.

### The new tests fail on `HEAD~1`

I replaced `ComputeTargets/AdiabaticHistory.py` with `git show 5aba202:ComputeTargets/AdiabaticHistory.py`,
kept this prompt's EOS classes, and ran `test_adiabatic_mass` with `CHAMPBH_TEST_REPORT=1`. Then
I restored the file. Ran: **FAILED (failures=8, errors=2)**.

- (b) fails **because the term is missing**, not because of the signature: 0.4067 at 144.3 MeV
  (f_m = 0), 0.6749 (f_m = 1), 0.9936 (f_m = 100). B_code is 0.0000 at every Table 1
  temperature.
- (c) fails at 0.3267, 0.5421 and 0.7981.
- (d) fails at 1.0.
- (e) crossing through `compute_adiabatic_values` fails at 1.529.
- The exact zero errors with `ValueError: math domain error`, from `log(0)`.
- The `Q_numerator` test errors with `ImportError`.
- The spike history through `compute_adiabatic_values` passes on the old code, as it should,
  since there is no sign change. The convergence test (a) passes too, since it does not use the
  code.
- `test_eos_w_derivative` needs `dw_dlogT`, which `HEAD~1` has on no class. I reasoned this
  rather than ran it.

### Suites and formatting

- `PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .` (ran):
  **before 12, after 18, OK (87 s)**.
- `PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .` (ran):
  **before 21, after 30, OK (66 s)**.
- `venv/bin/black --check` on every changed Python file (ran): clean. `main.py` was black-clean
  before and after.
- `git diff --stat HEAD~1 HEAD` (ran after the commit): see "State handed to the next prompt".
  `ComputeTargets/ScalarModel.py` is not in it.

### Not run

- **No pipeline run, no ScalarModel solve.** I did not measure how far max |Q| moves on a real
  history; that needs a `main.py` run, which is the user's.

## Observations not acted on

1. **Ḣ/H² now comes from the policy formula, not from the stored H samples.** The old route
   took d ln H²/dN implicitly from the spline of the stored `H_Einstein`. The new one uses
   `Hdot_over_H2_plus_3` − 3, the quantity the ODE itself integrates with, as the prompt
   specifies. The two should agree on a converged history. I did not measure them against each
   other on a real `ScalarModel`, since that needs a solve. Prompt 04 or the production run
   could, if wanted. Not opened as an issue: nothing indicates a disagreement.
2. **The two issues opened** (§3): the 120 MeV witness window (Deviation 1) and the end-sample
   accuracy (Deviation 3).
3. `SaikawaShirai_EOS_jax_autodiff.dG_rho_dlogT` and `dG_s_dlogT` carry the comment "units of
   the output will be 1/GeV". The value returned, T · dg/dT, is dimensionless. The comment is
   stale; the code is right. Not touched, and not opened: it is a comment.

## State handed to the next prompt

- **Names.**
  - `GenericEOSBase.dw_dlogT(T) -> float`, on every EOS class; `LambdaCDM_GenericEOS.dw_dlogT`
    forwards.
  - In `ComputeTargets/AdiabaticHistory.py`:
    - `conformal_mass_over_H2(three_MP_sq, E, Sigma, fm, d_logOmega_dphi, d2_logOmega_dphi2, Sigma_T, x)`;
    - `Q_numerator(raw_N_grid, M2eff_over_H2_grid, Hdot_over_H2_grid)`;
    - `AdiabaticComputePolicy.M2eff_over_H2(..., fm, T_Jordan)`;
    - `AdiabaticComputePolicy.Hdot_over_H2(...)`.
- **Tests.**
  - `CosmologyModels/tests/test_eos_w_derivative.py`: 6 tests, 92 s, of which about 90 s is the
    jax case.
  - `ComputeTargets/tests/test_adiabatic_mass.py`: 9 tests, 2.6 s.
  - Both print their measurements with `CHAMPBH_TEST_REPORT=1`.
- **To re-measure every §6.3 row:**
  ```bash
  CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest CosmologyModels.tests.test_eos_w_derivative
  CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_adiabatic_mass
  PYTHONPATH=. ./venv/bin/python prompts/production-readiness/logs/03-probes/join_window_probe.py
  ./venv/bin/python prompts/production-readiness/logs/03-probes/spline_order_probe.py
  ```
- **The `HEAD~1` check for test (b)** is described under Verification. Swap in
  `git show <parent>:ComputeTargets/AdiabaticHistory.py`, keep the new EOS classes, and run the
  module. Restore the file afterwards.
- **Measured values for the handover:**
  - bracket min −0.4067 (144.3 MeV), max +0.3498 (230.4 MeV);
  - reference vs code 9.57e-8 at h = 1e-4;
  - the audit's form off by up to 0.7327;
  - A·C 3.07e-6 / 6.30e-6 inside the window, 2.26e-4 at the crossing history's end sample;
  - the exact zero 4.65e-6 relative.
- **Suites:** CosmologyModels 18 and ComputeTargets 30, both OK.
- **Open on this board from prompt 03:**
  `[03-spline-eos-derivative-witness-misses-1e-6-above-the-120-mev-join]` (needs a user
  decision) and `[03-q-numerator-end-samples-carry-the-spline-end-condition-error]`.
- `VERSION_LABEL` is still `"2026.3.0"`. **No `AdiabaticHistory` row made before 2026.3.0 is
  comparable.**
