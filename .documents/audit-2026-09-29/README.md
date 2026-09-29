# Code audit against `Paper1_review.tex` (29 September 2026)

*Added 2026-09-30: items 1–4 were fixed by the `review-remediation` campaign, closed on
2026-09-30. The re-measurement on the final tree and the handover are in
[`../review-remediation-verification.md`](../review-remediation-verification.md). Nothing below is
changed.*

Scope: the code-relevant findings of the review of `Paper1.tex`
(H1, H5, H6, H7, H8, F8, N1, N3, T7, T9), plus anything found while tracing
those paths. Correctness only; no code was changed. The three scripts in this
directory reproduce every number quoted below (run from the repository root
with `venv/bin/python`).

The one-line summary: **the arcsinh spline is not the problem. The Jordan-frame
temperature law is.** Since commit `5962833` (19 January 2026) the entropy
correction in the temperature equation has been too large by a factor
ln 10 ≈ 2.303, and every scalar history and BBN result computed since then
carries the consequences (§1). The spline the review worried about is harmless
at the pipeline's sampling density (§2).

---

## 1. Temperature law: factor ln 10 in the entropy correction  — **major, new**

### What the code does

`ComputeTargets/ScalarModel.py:359-360` integrates

    d ln T_J / dN = -(1 + A'φ') / (1 + dG_s_dlogT / G_s / 3)

which is the paper's `TemperatureEvolEq`,
d ln T_J/dN_J = −1/(1 + ⅓ d ln g_s/d ln T_J), *provided* `dG_s_dlogT` is
dg_s/d ln T. It is not. The class in use is `Xav_EOS_spline`, which inherits
its derivatives from `SaikawaShirai_EOS_spline`; there the g_s spline is
built on a grid uniform in **log10 T** (`SaikawaShirai_EOS_spline.py:55`)
and `dG_s_dlogT` returns the spline derivative unchanged
(`SaikawaShirai_EOS_spline.py:155-171`), i.e. dg_s/d log10 T = ln 10 × dg_s/d ln T.
The alternative implementation `SaikawaShirai_EOS_jax_autodiff.py:223` returns
T dg_s/dT, the natural-log derivative, so the two classes disagree by ln 10.

`git show 5962833` shows where it entered: before that commit the RHS was
`1 + (T_Jordan/G_s) * dG_s_dT / 3` (correct); the commit moved the spline
onto a log10 grid, renamed `dG_s_dT → dG_s_dlogT`, and dropped the
conversion. The same factor affects `dG_rho_dlogT`, which is only stored and
plotted, not integrated.

### Verification (`tlaw_check.py`)

Integrating the code's law with the code's own EOS from T* = 2×10⁴ GeV and
comparing with exact entropy conservation T a g_s^{1/3} = const:

| T_J reached      | N exact | N code (κ=1) | N code with ÷ln10 | ρ_R(code)/ρ_R(thermo) at that T_J | with ÷ln10 |
|------------------|---------|--------------|-------------------|-----------------------------------|------------|
| 1 GeV            | 10.042  | 10.222       | 10.042            | 0.48                              | 0.988      |
| 100 MeV          | 12.804  | 13.583       | 12.804            | 0.051                             | 0.999      |
| 5 MeV            | 15.960  | 16.947       | 15.960            | 0.022                             | 0.998      |
| 1 MeV            | 17.575  | 18.569       | 17.575            | 0.022                             | 0.998      |
| 100 keV          | 20.140  | 21.476       | 20.140            | 0.0057                            | 0.999      |
| 70 keV (D bottleneck) | 20.547 | 21.950    | 20.547            | 0.0044                            | 0.999      |
| 10 keV           | 22.508  | 23.930       | 22.508            | 0.0041                            | 1.003      |
| T_CMB            | 40.075  | 41.497       | 40.075            | 0.0041                            | 1.003      |

Here ρ_R(code) is `exp(log_rhorad_Jordan)` integrated with
d ln ρ_R/dN = Σ − 4 (Σ from Xav's table) alongside the temperature law, and
ρ_R(thermo) = (π²/30) g_ρ(T_J) T_J⁴. With the ln 10 removed the two agree to
0.3 %, which also shows that Xav's tabulated w(T) is consistent with the
Saikawa–Shirai g_ρ, g_s. As written, the code's radiation density at its
own T_J is 2 % of the thermodynamic value at 1 MeV and 0.4 % at 70 keV.

### Consequences

1. **BBN interface (H1, H6, N3).** `BBNData.py` hands PRyMordial
   ρ_NP(T_J) = r(T_J) · ρ_R,J^code(T_J), where r = `density_NP_ratio` is
   correct but ρ_R,J^code is suppressed by 50× at weak freeze-out and 230× at
   the deuterium bottleneck. The chameleon effect on BBN was therefore
   essentially switched off in every run to date. This explains the flat
   D/H curve in Fig. `BBNdhPlot`. The review's proposed one-line check
   (read `density_NP_ratio` at 1 MeV for β = 2) would give ≈ 0.08 and
   correctly conclude "bug".
2. **Field histories through the transitions.** On an exact surfing
   solution T_J is frozen and the bug is inert, but during delivery, the
   rebounds and free cooling through the QCD and e⁺e⁻ peaks the cooling
   rate is too slow by up to ≈ 1.35–1.6×, so each kick lasts longer in N.
   φ_park, the rebound amplitudes and the timing of H1 steps (iv)–(vii)
   will all move when the runs are redone; the delivery temperatures
   themselves (set by Σ(T_J)) will not.
3. **Redshift labels.** The run stops when T_J = T_CMB, which now happens
   1.42 e-folds too late; the sample redshift is assigned from N relative to
   that endpoint (`ScalarModel.py:839-864`). Every "z" produced by ChamPBH
   is therefore 1 + z_label = e^{1.42} (1 + z_true) ≈ 4.15 (1 + z_true),
   and the stored "today" has ρ_m ≈ ρ_m0/70. Any late-time quantity read at
   z_label = 0 (φ₀, ω_DE(z), the pole redshift) from ChamPBH output is
   really evaluated 1.42 e-folds into the future. I could not find the
   script that produced the dark-energy figures (no DE code in the repo), so
   their provenance should be checked before assuming they are affected.
4. **Stored data.** All `ScalarModel`, `AdiabaticHistory` and `BBNData`
   rows computed since 19 January 2026 must be regenerated after the fix.
   The campaign should start from a fresh database or a new version tag.

### Fix and regression test

Divide the spline derivatives by ln 10 in `dG_s_dlogT` and `dG_rho_dlogT`
of `SaikawaShirai_EOS_spline` (or build the spline on a natural-log grid),
and fix the stale "units of the output will be 1/GeV" comments. Add
`tlaw_check.py` (or its core) as a test: integrate the temperature law with
each EOS class and require agreement with T a g_s^{1/3} = const to 1e-5 in N.
`CodeComparisonTools/XavEOS_test.py` compares w, g_ρ, g_s between
implementations but not the derivative, which is why this was not caught;
`CompareMathematica.py` does compare `dgstar_s_dlogT` with a Mathematica
column labelled d/d ln T, so the Mathematica implementation should be checked
for the same convention.

---

## 2. The arcsinh spline of ρ_NP, p_NP (H6 caveat 2) — mechanism right, magnitude not

`BBNData.py:131,178` spline asinh(ρ_NP/MeV⁴) and asinh(p_NP/MeV⁴) against
ln(T_J/MeV) and invert with sinh. For ρ_NP/ρ ≈ 0.08, |ρ_NP| < 1 MeV⁴ below
T_J ≈ 1.4 MeV, so in the whole weak-freeze-out and deuterium window the
transform is linear and the spline is a plain cubic fit to a quantity falling
as T⁴. The review's diagnosis of the mechanism is correct.

The review's magnitude (spurious ρ_NP/ρ ≈ 10⁻³ at 0.3 MeV) assumes a coarse
knot spacing. The pipeline samples at `DEFAULT_SAMPLES_PER_LOG10_Z = 250`
per decade of Einstein-frame 1+z (`config/argument_parser.py:17`;
`exponential.yaml` does not override it), which is ≳ 160 knots per decade of
T_J in the window. `spline_test.py` rebuilds the code's spline on synthetic
ρ_NP = r(T) ρ_SM(T) with the same knot spacing:

| knots/decade | r constant: max spurious ρ_NP/ρ | r oscillating on e-fold scale: max | ratio spline (oscillating) |
|--------------|---------------------------------|------------------------------------|----------------------------|
| 20           | 1.9e-5                          | 6.3e-4                             | 2.8e-4                     |
| 50           | 5.1e-7                          | 4.0e-5                             | 6.2e-6                     |
| 100          | 3.2e-8                          | 1.8e-6                             | 3.8e-7                     |
| 250          | 7.9e-10                         | 3.3e-8                             | 9.7e-9                     |

At the actual density the arcsinh spline contributes ΔN_eff ≲ 10⁻⁷, so it is
not the source of the β-dependent wiggles in D/H, and it is not why the curve
is flat (§1 is). The transform does do useful work above ≈ 2 MeV, where
|ρ_NP| ~ 10⁶–10⁹ MeV⁴ and a linear-space spline over the full domain would be
badly conditioned. Splining `density_NP_ratio` (and a pressure ratio) and
multiplying back by the code's own ρ_R,J(T_J) is still the better choice:
exact for a parked field, 3–100× better during rebounds, no dependence on the
choice of unit, and the derivative callback becomes r' ρ_SM + r ρ_SM'. It
should be adopted, but it will not change results.

Other points on the interface, all confirmed against the code:

- ρ_NP and p_NP are defined as the review states (`BBNData.py:120-172`);
  the Planck-mass conventions match (ours reduced, PRyMordial's full, the 8π
  cancels in `PRyM_main.py:76-79`); PRyMordial's plasma equation does add
  −3H(ρ_NP+p_NP) and dρ_NP/dT (`PRyM_main.py:126-130`) and the cancellation
  argument holds given how p_NP is built.
- `BBNData.py:165`: the term `log_Omega_primeprime * pi_Einstein` should be
  `log_Omega_primeprime * pi_Einstein**2` (A1' = Ω''φ'² + Ω'φ''). Zero for
  the exponential coupling, wrong for any other.
- Caveat 1 (SM baseline): the additive term ρ_PRyM − ρ_ours is real, but
  since H_J² ∝ ρ_ours(1+r) the physically relevant error relative to a
  chameleon universe with PRyMordial's plasma is r·ε, second order. A
  same-settings baseline with ρ_NP ≡ 0 is still the right control; there is
  currently no switch for it in `compute_BBN_data`.
- Caveat 3 (monotonic T_J): `BBNData.py:112` only prints, and the message
  prints the same variable twice. Worse, `_make_spline` (`BBNData.py:36`)
  sorts by T, so non-monotonic samples are silently interleaved. Make it a
  failure.
- The spline domain (100 MeV → 0.1 eV) is far wider than PRyMordial's use
  (10 MeV → ≈ 0.3 keV) and includes the matter-era re-delivery oscillations
  of H1 (vii). Harmless at this knot density, but pointless.

---

## 3. PRyMordial's inert NP-temperature equation is singular — **new, likely cause of dropped models**

With `NP_thermo_flag` (`BBNData.py:296`) PRyMordial integrates a third
variable T_NP with
dT_NP/dt = −3H(ρ_NP + p_NP)/ρ_NP'(T_γ) (`PRyM_main.py:139-147`).
T_NP is never used: `Hubble` ignores it, and `TNPofT` only feeds `Hubble`.
Whenever dρ_NP/dT crosses zero, which it does whenever ρ_NP changes sign or
oscillates (exactly the e⁺e⁻-era rebounds of H1 step (v)), this component
diverges, LSODA fails, `compute_BBN_data` returns `{"failure": True}`
(`BBNData.py:311`, exception swallowed), and `plot_by_beta.py:142` drops the
model silently. This is a concrete candidate for the missing
1.1 ≤ β ≲ 2 band in Fig. `BBNdhPlot`, and probably for the "PRyMordial
sometimes produces negative temperatures" guard in the callbacks.

Fix: patch the vendored `PRyM_main.py` so `dTNPdt` returns 0 (T_NP is
inert), and record the exception text in `BBNData` instead of a bare boolean.
Do **not** switch to `NP_e_flag`: it adds (ρ_NP+p_NP)/T to the plasma
entropy (`PRyM_thermo.py:170`), which feeds a(T) and η (`PRyM_main.py:367,445`)
and is wrong for a scalar that does not share the plasma's entropy.

---

## 4. The e⁺e⁻ kick and the "freeze at 2 MeV" (H7, T9)

The paper's numerical section says ω_R's argument is frozen at 2×10⁻³ GeV.
That describes `SaikawaShirai_EOS_spline.w` and the jax class, which are
**not** used: `QCD_Cosmology` wraps `Xav_EOS_spline`, whose `w()`
(`Xav_EOS_spline.py:55`) is a spline of `Xav_EOS_data.csv` (189 rows,
10 keV–25 TeV) and returns exactly 1/3 outside that range. From the table:

| feature       | peak Σ  | at T_J   |
|---------------|---------|----------|
| e⁺e⁻          | 0.1007  | 0.158 MeV |
| QCD           | 0.314   | 178 MeV  |
| electroweak   | 0.0373  | 56 GeV   |

with Σ(2 MeV) = 0.003, Σ(0.5 MeV) = 0.034, Σ(0.2 MeV) = 0.095,
Σ(0.1 MeV) = 0.068, Σ(50 keV) = 0.003, and ∫Σ d ln T = 0.16 over
[10 keV, 3 MeV]. So the numerics do contain the e⁺e⁻ kick at the 0.10 level
the review infers from Fig. `SigmaPlot`, and the paper's description should
be replaced by a description of the table (whose construction is not in the
repository; commit `1759515`). The EW peak is 0.037, as the review's typo
note says. The consistency check in §1 (0.3 % after the fix) shows the table
is compatible with the Saikawa–Shirai g's, so no separate electron formula is
needed in the code.

---

## 5. Adiabaticity diagnostic (H5, T3) — confirmed

`AdiabaticHistory.py:103` sets the conformal contribution to
3M_P² E Ω'' R, which vanishes for the exponential coupling; the response of
the source to δφ is omitted, as the review says. Differentiating
V_eff' = V' + Ω' ρ_R,E (Σ + f_m) at fixed a_E, using ρ_R,E Σ = A⁴(ρ_J − 3p_J)
and ρ_m,E ∝ A/a_E³, gives the missing piece

    (Ω')² ρ_R,E [ Σ (4 − d ln(ρ_J − 3p_J)/d ln T_J) + f_m ]

so in the code's variables `conformal_mass` should be
3M_P² E [ Ω'' R + (Ω')² (Σ(4 − d ln(Σ ρ_R,J)/d ln T_J) + f_m)/(1 + f_m) ].
The T_J-derivative needs dΣ/d ln T, available from the w-spline
(`Xav_EOS_spline._spline.derivative()`, not currently exposed). Numerically
harmless where V'' dominates, but the paper sentence and the diagnostic
should agree. The Q defined in the code matches eq. `eq:adiabaticity`.

---

## 6. Initial conditions (H8) — confirmed

`main.py:814` hard-codes φ* = 5 M_P, π* = 0; T* defaults to 2×10⁴ GeV.
Nothing checks A* T* ≲ M_P. For `exponential.yaml` (β up to 25) A* = e¹²⁵;
harmless numerically (everything is in logs) but unphysical for β ≳ 6.5.
Make φ* a parameter, record it in the store tags, and warn or refuse when
βφ*/M_P > ln(M_P/T*).

## 7. Dark-matter density (F8) — fine

`rho_m0 = 3H0² M_P² Ω_m` with Planck 2018 Ω_m = 0.311, i.e. 3.3×10⁻¹²¹ M_P⁴.
The value 3×10⁻¹³¹ does not occur in the code.

## 8. Elastic reflection (N1)

The hard boundary is φ_E = 0 (`ExponentialPotential.hard_reflection_point`),
so the fallback only fires if the field is driven negative; step-size
reduction applies below 1.5M and M/20. The reflection count is stored
(`hard_reflections`) but never reported by the plotting scripts; it should
appear in captions.

## 9. Failure reporting (N3)

`plot_by_beta.py:137-147` filters `available and not failure and X > 0`
without listing what was dropped, and `BBNData` stores no reason. Store the
exception text and print the dropped (β, M, Λ) list.

## 10. What could not be checked here

No ChamPBH database exists on this machine, so the review's direct reads
(`density_NP_ratio` at 1 MeV, reflection counts, the failure list) must wait
for the stored results. Given §1, those results need regenerating anyway.

---

## Suggested order of work for the campaign

1. Fix the ln 10 factor; add the temperature-law regression test; bump the
   version tag / start a fresh database.
2. Patch `dTNPdt`; record failure reasons; make non-monotonic T_J fatal.
3. Switch the interface to ratio splines; add a ρ_NP ≡ 0 baseline mode.
4. Rerun the scalar histories, then BBN for 1.1 ≤ β ≤ 3 with the baseline;
   read `density_NP_ratio` at 1 MeV for β = 2 (H1) and the reflection counts.
5. Correct the adiabaticity diagnostic; make φ* a parameter with the
   A*T* < M_P guard; fix the `Ω''φ'²` term; update the paper's numerical
   section (table-based Σ, not the 2 MeV freeze).

---

## 11. Addendum (29 September 2026): the 10 keV join — **new, minor**

*Added during the orchestration of `review-remediation` prompt 01, whose first dispatch stopped on
it. Additive: §1–§10 were correct for what they measured; this section corrects one rounding in §1
and one statement in §4. Tree `41b410d` (production code identical to `f5896bb`).*

### What was found

§1's table shows the corrected law (κ = 1/ln 10) agreeing with exact entropy conservation to four
decimals everywhere. It does not agree to 1e-5 at and below 10 keV:

```
venv/bin/python .documents/audit-2026-09-29/low_t_join_probe.py ship
```

| T₁ | corrected-law residual (N − N_exact), as shipped | with `LOW_T_*` patched in memory |
|---|---|---|
| 1 GeV | −6.9e-12 | −6.9e-12 |
| 100 MeV … 70 keV | −3.9e-08 to −4.3e-08 | the same |
| **10 keV** | **+1.465e-04** | −4.0e-08 |
| **T_CMB** | **+1.464e-04** | −4.0e-08 |

`tlaw_check.py` shows the same thing at four decimals: 22.5082 against 22.5080 at 10 keV, and
40.0747 against 40.0746 at T_CMB.

**The cause** is the constants that replace the fit below `SAIKAWA_SHIRAI_T_LO` = 10⁻⁵ GeV.
`LOW_T_G_S_STAR = 3.94` and `LOW_T_GSTAR = 3.38` (`SaikawaShirai_common.py:101,107`) differ from
the fit's own limits, **3.931** and **3.383**. The fit's limits are its constant terms,
2.008 + 1.923 and 2.030 + 1.353; the e± terms are ~e⁻⁵¹ at 10 keV.

- **Size.** The spline gives `G_s` = 3.938269 just above T_LO and the clamp returns 3.94 at T_LO,
  so ⅓ ln(3.938269/3.94) = −1.465e-4.
- **Why 3.938269 and not 3.931.** The grid starts at 0.8 T_LO (`SaikawaShirai_EOS_spline.py:50`),
  so the spline is fitted across the step and rings over the last ~1 % in T.
- **Why only `exact_efolds` sees it.** `dG_s_dlogT` is clamped to 0 below T_LO, so the ODE never
  sees the step. `exact_efolds` reads `G_s` at the endpoint.
- **`G_rho` has the same kind of step:** 3.380577 just above T_LO against 3.38 at it.

**The fix is two constants.** With them set in memory to 3.931 and 3.383, the residual at 10 keV
and T_CMB falls to −4.0e-08, the level of every other point, and nothing above 10 keV moves.

```
venv/bin/python .documents/audit-2026-09-29/low_t_join_probe.py patch
```

**The fix also moves the ρ_R witness** at the low end:

| ρ_R witness (corrected law) | as shipped | constants patched |
|---|---|---|
| 2×10⁴ GeV → 10 keV and → T_CMB | 1.00258 | **0.99922** |
| 5 MeV → 10 keV | 1.00471 | **1.00135** |
| 5 MeV → 10 keV, κ = 1 (shipped law) | 0.18184 | 0.18065 |

Part of §1's "residual 0.3–0.5 %" at 10 keV was therefore this step, not the table–g mismatch.
The shipped-law value 0.18184 differs from §2 (c)'s 0.182 only in the fourth digit. It also
differs from `eos_consistency.py`'s 0.18081, because that script reads the last solver sample,
not the event, and uses no `max_step`.

### History

The constants and the join do not come from ChamPBH. They come from SecondaryGWKit's history:

| Commit | Date | Change |
|---|---|---|
| `d114cc2` | 2025-04-03 | Low-T limits 3.36 and 3.91 (still commented out at `SaikawaShirai_common.py:99–100`). |
| `764f07d` | 2025-04-07 | Introduced `gs0 = 3.94` as a present-day normalisation in H(z), "from your document". |
| `b0100be` | 2025-05-12 | Promoted 3.38 and 3.94 to the clamp constants, with the `TODO: check` and the check 2 + 2·3.042·(7/8)·(4/11)^{4/3} = 3.38172. That check is correct for g_ρ. |
| `947a2d3`, `1cd3ed3` | 2025-05-15 | Introduced the `w` freeze at `EOS_T_LO` = 2 MeV, because 4g_s/(3g_ρ) − 1 goes wrong below neutrino decoupling. |

**The values 3.38 and 3.94 are in the literature**, as g_*(T₀) and g_*s(T₀) in arXiv:2109.01398.

**The g_s check was never done.** Applied to g_s, the argument above has to scale the neutrinos'
T³ weight as (N_eff/3)^{3/4}, since N_eff is defined through ρ. That gives 3.931 at N_eff = 3.046,
which is the fit's own limit. Scaling linearly in N_eff gives 3.936 at 3.042 and 3.938 at 3.046, both of which round to 3.94
(`w_two_temperature.py`, last block).

**SecondaryGWKit has the same issue.** It measured the same join independently as
`[00-eos-branch-joins-do-not-match]`, in its qcd-background-audit of 2026-09-14. It pinned the
join in a test and did not repair it. This addendum does not touch that repository.

### Why the naive w fails, and what the table does

```
venv/bin/python .documents/audit-2026-09-29/w_two_temperature.py
```

The fits for g_ρ and g_s are not at fault. The identity p = Ts − ρ holds only at a common
temperature, and the enthalpy is Σᵢ sᵢTᵢ. g_s weights the neutrino entropy by (T_ν/T)³, so
4g_s/(3g_ρ) − 1 overstates the neutrino enthalpy by T/T_ν = 1.401 at low T. That is why it gives
0.549 rather than 1/3.

The low-T branch of the fit already encodes T_ν/T. S(x) = 1 + (7/4)f_s(m_e/T) is the
entropy-transfer factor, with (T_ν/T)³ = (4/11)S. Weighting the neutrino entropy term 1.923·S by
T_ν/T reproduces Xav's table:

| T [MeV] | naive formula | two-temperature | Xav table |
|---|---|---|---|
| 2 | 0.3336 | 0.33250 | 0.33235 |
| 0.158 | 0.4047 | 0.29905 | 0.29976 |
| 0.07 | 0.5311 | 0.32519 | 0.32612 |
| ≤ 0.03 | 0.5493 | 0.33238 | 0.33333 |

- **The two agree to about 1e-3** through the whole e± annihilation.
- **Why neither the reconstruction nor the fit reaches 1/3 exactly.** The fit's "constant" terms
  (2.030 in g_ρ, 2.008 in g_s) are not pure photon terms (2); they absorb the fit's residuals.
  Only the totals are accurate. Using the exact neutrino enthalpy (4/3)ρ_ν instead gives 0.32466.
  So w = 1/3 below ~30 keV has to be imposed, which the table does.
- **The 2 MeV freeze** in `SaikawaShirai_EOS_spline.w` and the jax class freezes at 0.3336, not
  1/3, because T_ν/T = 0.9984 there in this parametrisation. It also removes the e± dip entirely.
  Neither class is in production.

### Correction to §4

§4 says `Xav_EOS_spline.w` "returns exactly 1/3 outside that range". Below the table
(T ≤ 10 keV) it does. **Above the table** (T ≥ 25 119 GeV) it returns `1.0 / 3.9`
(`Xav_EOS_spline.py`, the `T_in_GeV >= self._T_max` branch). That looks like a typo for 1/3.

It is latent: the default `--T-init-GeV` is 20 000 (`config/argument_parser.py:13`), below the
table's top, but any run started above 25 TeV would get Σ = 0.23 there.

### Disposition

- **The two constants** become `review-remediation` item **R5**, fixed in prompt 02 alongside R1.
- **The `1/3.9`** is seeded as an issue on that board.

### Correction to this addendum (29 September 2026, after prompt 01 landed at `ec3a994`)

*Additive. The text above is left as written.* Two statements in this section are wrong:

- **"Correction to §4" is itself wrong; §4 was right.** `Xav_EOS_spline.w` returns `1.0 / 3.0`
  above the table on the working tree, and has done since `449de62` (2026-01-16, "Fix typo in the
  implementation of Xav's equation of state"). The addendum's author read the file as it stood at
  `1759515`, where it was added with `1.0 / 3.9`, rather than as it is now. Prompt 01's agent
  measured `w(3e4 GeV)` = 0.3333333 at `b9bc694` (log 01, "Observations not acted on"). The
  issue this addendum seeded is withdrawn.
- **The explanation of `eos_consistency.py`'s 0.18081 is wrong.** The whole difference from the
  event value 0.18184 comes from that script setting no `max_step`. With a terminal event its
  last sample *is* the event, so it does not read "the last sample rather than the event"
  (log 01).
