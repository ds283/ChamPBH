# Prompt 04 — ratio splines, fail-closed on non-monotonic T_J, the Ω″φ′² term, and a baseline

**Campaign:** [`README.md`](README.md) · **Board item:** **R3** ·
**Board:** `IMPLEMENTATION_STATE.md` — update your row and R3.
**Closes:** R3. **Recommended model:** **Opus**. A refactor that must preserve a conservation
identity, a representation change that must be *shown* to be no worse anywhere and better where
it matters, and a small amount of plumbing.

**Read first:**

1. [`README.md`](README.md) §0, §2 (c), (f), (g), (h), §5, §6.3.
2. `.documents/audit-2026-09-29/README.md` §2 and `spline_test.py` beside it — the synthetic
   experiment whose numbers are your acceptance table. Run it first.
3. `logs/03-…md` — `run_prym`, the reference abundances, `failure_reason`.
4. `ComputeTargets/BBNData.py` in full: `_make_spline` (`:34–39`), the sample loop (`:96–176`),
   the asinh grids (`:131`, `:174`), the Ḣ_J/H_J² formula (`:151–172`, the `Ω″ π` at `:165`),
   the three callbacks (`:190–290`), and `SampleValues` / `BBNDataValue`.
5. `CosmologyConcepts/ConformalCouplings/AbstractCoupling.py` and `ExponentialCoupling.py` —
   `d2_logOmega_dphi2` is 0 for the exponential coupling, which is why `:165` has never been
   exercised.
6. `constants.py` (`RadiationConstant`), `CosmologyModels/GenericEOS/LambdaCDM_GenericEOS.py`
   (`G_rho`, `dG_rho_dlogT`, corrected by prompt 02).
7. `plot_by_beta.py` — where the D/H, Yp and ⁷Li panels are drawn.

---

## 1. What to build

### 1.1 A pure callback builder

Factor the construction out of `compute_BBN_data` into a module-level function, for example

```python
def build_NP_callbacks(log_T_MeV, density_ratio, pressure_ratio, rho_SM_MeV4, drho_SM_dT_MeV3,
                       T_min_MeV, T_max_MeV, task_label) -> NPCallbacks
```

taking **arrays already in the order the solver produced them** (decreasing T), the two
dimensionless ratios, two callables for the Standard-Model density and its T-derivative in
PRyMordial units, and the domain. It returns the three callbacks `rho_NP`, `P_NP`, `drho_NP_dT`
with exactly the signatures and guards they have now (negative T → 0; outside the domain →
`ComputationFailureError`; `OverflowError`/`ValueError` wrapped). `compute_BBN_data` calls it and
does nothing else with the splines.

**Refuse non-monotonic input.** The builder raises `ComputationFailureError` naming the first
offending pair if `log_T_MeV` is not strictly decreasing. `_make_spline`'s sort goes; the spline
is built on the reversed arrays. `compute_BBN_data` turns that into
`{"failure": True, "failure_reason": …}` (prompt 03's field). The monotonicity `print` at `:112`
goes with it (it printed the same variable twice anyway).

### 1.2 The ratio representation

Spline `density_ratio = ρ_NP/ρ_R,J` and `pressure_ratio = p_NP/ρ_R,J` (both from the sample loop,
both already computed or one line from it) against ln(T/MeV), cubic, no transform. Then

- `rho_NP(T) = r(T)·ρ_SM(T)`, `P_NP(T) = s(T)·ρ_SM(T)`,
- `drho_NP_dT(T) = r′(T)·ρ_SM(T)/T + r(T)·ρ_SM′(T)`, with `r′` the analytic derivative of the
  ratio spline in ln T. **Never a finite difference** (README §2 (g)).

**Which ρ_SM.** Two acceptable choices; pick one, measure the other, and record both:

- **Thermodynamic (preferred):** ρ_SM(T) = `RadiationConstant · cosmology.G_rho(T) · T⁴` and
  ρ_SM′ from `dG_rho_dlogT` (correct after prompt 02), evaluated with T converted from MeV. No
  second spline; the derivative is exact.
- **Stored:** a spline of `log_rhorad_Jordan` against ln T. Self-consistent with the ratio's
  denominator by construction, but a second interpolant.

After prompt 02 the two agree to 0.2–0.3 % (README §2 (c)); the difference enters ρ_NP as
r × 0.003, second order. Quote the maximum relative difference over the window in the log.

The asinh path is **removed**, not kept behind a flag. The comparison with it is the acceptance
test below, done on synthetic data.

### 1.3 The Ω″ term

`BBNData.py:165`: `log_Omega_primeprime * value.pi_Einstein` → `… * value.pi_Einstein**2`
(A₁′ = Ω″φ′² + Ω′φ″). Factor the Ḣ_J/H_J² expression into a small pure function of
`(HEdot_over_HE2, Omega_prime, Omega_primeprime, pi, pi_prime)` so it can be tested with a
non-zero Ω″. For the exponential coupling nothing changes; the test uses a stand-in.

### 1.4 A Standard-Model baseline through the same path

`compute_SM_baseline(small_network: bool) -> dict` in `BBNData.py`: sets the same flags
`compute_BBN_data` sets, passes callbacks that return `0.0`, runs `PRyMclass`, returns the four
abundances plus `PRyM_version` and `small_network`. A script `tools/bbn_baseline.py` prints them.
`plot_by_beta.py` computes it once at plot time (≈ 10 s) and draws it as a labelled horizontal
line on each abundance panel, with a `--no-baseline` switch. **Not stored** in the datastore.

---

## 2. Tests — `ComputeTargets/tests/test_bbn_callbacks.py`

Synthetic inputs built as `spline_test.py` builds them: 250 knots per decade in ln T over
[10⁻⁷, 10²] MeV, ρ_SM from the Saikawa–Shirai g_ρ (import `_raw_G_rho` and the clamp, as the
script does, so the test needs no cosmology object), evaluated on 3,000 points in [0.02, 5] MeV.

1. **Constant ratio is exact.** r = 0.08, s = r/3: `|rho_NP − r ρ_SM|/ρ_SM ≤ 1e-12` and the same
   for `P_NP`; `drho_NP_dT` against the analytic derivative of r ρ_SM to 1e-9 relative to 4 ρ_SM.
2. **Oscillating ratio bounds** (README §2 (d) family): `≤ 2e-8` in ρ_NP/ρ_SM and `≤ 1.5e-6` in
   the derivative measure. Print the maxima.
3. **Against the old representation.** Rebuild the asinh callbacks locally in the test (copy the
   three-line construction, it is not worth importing dead code) and assert the ratio
   representation's maximum error is **no larger** on both families.
4. **Non-monotonic input is refused**, with the pair named.
5. **Domain guards** unchanged: below `T_min` and above `T_max` raise; negative T returns 0.
6. **Units.** For T in MeV the callbacks return MeV⁴ / MeV⁴ / MeV³: check against ρ_SM at 1 MeV
   ≈ 3.5 MeV⁴ (g_ρ ≈ 10.7).
7. **The Ḣ_J/H_J² function**: with Ω″ ≠ 0 the result differs from the old expression by
   Ω″ π (π − 1)/A₁², and equals it when Ω″ = 0.
8. **End to end** (docstring: runs PRyMordial, ≈ 10 s): constant ratio 0.08 through
   `build_NP_callbacks` into `run_prym` reproduces prompt 03's Yp and D/H to **1e-4 relative**.
9. **Baseline**: `compute_SM_baseline(True)` reproduces README §2 (f) row 1 to 1e-4 relative
   (docstring: ≈ 10 s).

---

## 3. What this prompt does not do

- No change to the spline domain, the sampling density, the `T_Jordan_stop` pre-check, or what
  is stored per redshift (`density_NP`, `pressure_NP`, `density_NP_ratio` stay; do not add a
  stored pressure ratio — it is one division at build time).
- No change to the field equation, `HubblePolicy`, or `ODEPolicy`.
- No datastore schema change.

---

## 4. Acceptance

1. README §6.3, every row, with measured values in the log.
2. `git diff` shows the asinh construction gone from `BBNData.py` and no `sinh`/`asinh` import
   left.
3. `ComputeTargets/tests` up by exactly the cases added; earlier tests unchanged. `black --check`
   clean on changed files.
4. `tools/bbn_baseline.py` runs from the root and prints the four abundances;
   `plot_by_beta.py --help` shows `--no-baseline`.
5. Board R3 done; the ρ_SM choice and the measured difference between the two candidates recorded
   in the R3 row.

---

## 5. Stop conditions — stop and ask the user

- The ratio representation is *worse* than asinh on either synthetic family (case 3).
- The two ρ_SM candidates differ by more than 1 % anywhere in [10 keV, 10 MeV] after prompt 02.
- Reaching `compute_BBN_data`'s sample loop for the end-to-end test requires a `ScalarModel`
  instance you cannot build without Ray or a datastore. (The builder takes arrays precisely so
  this does not happen; if the loop itself needs testing, say so and stop.)
- You want to store the baseline in the datastore.

---

## 6. The log and the board

`logs/04-ratio-splines-and-a-baseline.md`. Beyond the template: `build_NP_callbacks`' final
signature; the ρ_SM choice with the measured alternative; the two synthetic maxima and the
asinh comparison; the knot count the real pipeline would produce in the window at 250 per
decade (compute it from the domain — this is the number the "harmless at this density" claim
rests on). Board: R3 done.
