# Numerical methods, as implemented — a note for rewriting `Paper1.tex`'s numerical section

**Written:** 2026-09-29, by `review-remediation` prompt 05 (item R4). **Tree:** `89bd52e` plus
prompt 05's diff, which touches no numerical code, only docstrings and tests.
**Scope:** what ChamPBH does, with the number and its source for each statement. It says where
the paper's current text (`Paper1.tex`, §"Numerical methods", the paragraphs *Thermodynamics*
and *Interface to nucleosynthesis*, lines ≈ 3030–3155 on 2026-09-29) differs from the code. It
does not say what the paper should claim. The authors rewrite the text themselves; this note
does not edit it.

**Provenance tags used below.**

| Tag | What it is |
|---|---|
| **[T-law]** | `CosmologyModels/tests/test_temperature_law.py` (prompts 01, 02); values in log 02, on `47c50ae` |
| **[kick]** | `CosmologyModels/tests/test_kicking_function.py` (prompt 05), run with `CHAMPBH_TEST_REPORT=1`, on `89bd52e` + prompt 05 |
| **[scan05]** | prompt 05's scratch probes, on the same tree. They call `CosmologyModels/tests/eos_reference.py` and are described in log 05 |
| **[bbn]** | `ComputeTargets/tests/test_bbn_callbacks.py` and the log 04 probes, on `ec206a3` + prompt 04 (`eba4473`) |
| **[prym]** | `ComputeTargets/tests/test_prym_passenger.py` and log 03, on `47c50ae` + prompt 03 (`ec206a3`) |
| **[audit]** | `.documents/audit-2026-09-29/README.md` and its scripts, on `f5896bb` |

Logs are in `prompts/review-remediation/logs/`; issues are on the board
`prompts/review-remediation/IMPLEMENTATION_STATE.md` §3.

---

## 1. The temperature law

**As implemented** (`ComputeTargets/ScalarModel.py`, the `log_T_Jordan` component of the RHS):

  d ln T_J / dN = −(1 + A′φ′) / (1 + ⅓ d ln g_s / d ln T_J),

which is the paper's `TemperatureEvolEq` with the A′φ′ term.

- **g_s and g_ρ** are the Saikawa–Shirai fits (arXiv:1803.01038, App. C), class
  `SaikawaShirai_EOS_spline`, which the production class `Xav_EOS_spline` inherits.
- **The spline.** The fits are sampled once, on a grid uniform in **log10 T** at **250 points
  per decade**. The grid runs from 0.8 × 10⁻⁵ GeV to 1.2 × 10¹⁶ GeV, that is 20 % beyond each
  clamp. A cubic interpolating spline (`make_interp_spline`, k = 3) is built on that grid.
  - The paper says "uniform in log T_J". The base is 10, and the "250 points per decade" is
    commented out in the paper's source.
- **The derivative** d g/d ln T is the spline's analytic derivative divided by ln 10. It is not
  a finite difference.
  - It agrees with a central difference of `G_s` in ln T to **1.26e-8** in d ln g_s/d ln T, the
    worst of 60 points on [20 keV, 5 TeV].
  - It agrees with the jax autodiff class to **2.44e-7**. Both worst cases are at 180 MeV, next
    to the fits' 120 MeV branch join. [T-law]
- **The clamps.**
  - At and above **10¹⁶ GeV**, g_ρ = g_s = 106.75.
  - At and below **10⁻⁵ GeV** (10 keV), g_ρ = **3.383** and g_s = **3.931**. These are the fits'
    own limits, their constant terms 2.030 + 1.353 and 2.008 + 1.923.
  - Beyond either clamp both derivatives are exactly zero. [T-law, case 6]
  - Before `47c50ae` the low-T constants were 3.38 and 3.94. The g_s step at 10 keV shifted N by
    +1.465e-4 (item R5; [audit] §11).
- **Accuracy.** At fixed field the law reproduces exact entropy conservation,
  T a g_s^{1/3} = const, to **3.6e-8 e-folds** at every temperature from 2 × 10⁴ GeV down to
  T_CMB. N to T_CMB is 40.0754, exact and integrated. [T-law, cases 1–2; log 02]

**The ln 10 error, 2026-01-19 to 2026-09-29.**

- **What was wrong.** From commit `5962833` (19 January 2026) until `47c50ae`, the spline's
  derivative was returned undivided. That is d g/d log10 T, which is ln 10 ≈ 2.303 times
  d g/d ln T, and the law consumed it as the latter. The entropy correction in the denominator
  was therefore too large by ln 10.
- **Size.** Integrating from 2 × 10⁴ GeV at fixed field gave N = 41.497 to reach T_CMB, against
  the exact 40.075.
- **ρ_R,J.** The radiation density carried alongside was **0.022×** the thermodynamic value at
  1 MeV and **0.0044×** at 70 keV. [audit] §1
- **Consequences** ([audit] §1):
  1. **BBN.** The chameleon contribution handed to PRyMordial, ρ_NP = r · ρ_R,J, was suppressed
     by ≈ 50× at weak freeze-out and ≈ 230× at the deuterium bottleneck. The chameleon's effect
     on BBN was essentially switched off, which is why the D/H figure is flat.
  2. **Field histories.** Through the QCD and e⁺e⁻ kicks the plasma cooled too slowly in N, by up
     to ≈ 1.35–1.6×. The kicks lasted longer in N, so φ_park and the rebound amplitudes will move
     when the runs are redone.
  3. **Redshift labels.** Every run stopped 1.42 e-folds late, so every ChamPBH redshift label
     carries 1 + z_label ≈ 4.15 (1 + z_true).
  4. **Stored data.** Every stored result made under `VERSION_LABEL = "2026.1.1"` is invalid. The
     label is now `"2026.2.0"` (log 02).

## 2. The kicking function Σ = 1 − 3 ω_R

**The paper's text describes code that production does not run.**

- **What the paper says.** "We freeze its argument at 2 × 10⁻³ GeV, so that ω_R approaches 1/3
  smoothly." That is what `SaikawaShirai_EOS_spline.w` and the jax class do.
- **What production runs.** `QCD_Cosmology` wraps `Xav_EOS_spline`, which overrides `w()`.
  - It is a cubic spline in ln T of the table `CosmologyModels/GenericEOS/Xav_EOS_data.csv`.
  - It returns exactly **1/3 at and below 10 keV and at and above 25.1 TeV** (25 118.86 GeV)
    without consulting the spline.
  - The 2 MeV freeze is never reached. [kick, cases 4 and 6]
- **What the freeze would have given.** Σ = −7.2e-4 everywhere below 2 MeV (w = 0.33357), with no
  e⁺e⁻ dip. [scan05]

**The table** (`Xav_EOS_spline.py`'s module docstring records this; [scan05]).

- **Shape.** 189 rows, from 10 keV to 25.1 TeV, uniformly spaced at 20 rows per decade of T.
- **Bottom row.** w is 1/3 to 7e-16.
- **Top row.** w = 0.3333329996, which is 1/3 to 3.3e-7 but not exactly. So w steps by 3.3e-7
  at 25.1 TeV, where the class switches to 1/3.
- **Construction.** How the table was built is **not in the repository**. It arrived in commit
  `1759515` (13 January 2026) without the script or the method, and it has not changed since.
  Board issue `[00-kicking-function-table-has-no-provenance-in-the-repository]`.

**The peaks.**

- The pipeline evaluates the spline, whose peaks fall between table rows.
- Both columns are given. The spline peak is the one the tests pin. [kick, case 1; scan05]

| Feature | Spline peak (what the code uses) | Largest table row |
|---|---|---|
| e⁺e⁻ | Σ = **0.10073** at T_J = **0.1605 MeV** | 0.10072 at 0.1585 MeV |
| QCD | Σ = **0.31453** at T_J = **0.1819 GeV** | 0.31377 at 0.1778 GeV |
| electroweak | Σ = **0.037436** at T_J = **53.25 GeV** | 0.037326 at 56.23 GeV |

The spline peak temperatures come from the orchestrator's 5000-points-per-decade probe on
`eba4473`. [kick] reproduces them at 1000 per decade: 0.16033 MeV, 0.181993 GeV and 53.22 GeV.

**The e⁺e⁻ profile.** [kick, cases 2–3]

- Σ = 0.00295 at 2 MeV, 0.03445 at 0.5 MeV, 0.09456 at 0.2 MeV, 0.06798 at 0.1 MeV and 0.00286 at
  50 keV. At 20 keV it is 5.2e-8.
- ∫Σ d ln T over [10 keV, 3 MeV] is **0.16181**.
- The spline undershoots slightly near 24 keV, to a minimum Σ = −2.6e-6. No table row goes below
  −1e-10. [scan05]

**What this means for the paper's numbers.**

- **The e⁺e⁻ peak.** The value 0.03 quoted in §"total EOS" matches nothing in the table. The
  table and the spline give 0.10. The paper's own margin notes already say so.
- **The electroweak peak.** The 0.04 is the table's 0.0373 rounded. The minimum β computed from
  it with the paper's β_min = √((1 + Σ/2)/(3Σ)) is 2.92 at 0.04 and 3.02 at 0.0373 (arithmetic
  from the paper's formula, not a code result).
- **Provenance of the paper's numbers.** No other version of the table exists in the repository,
  so they come from this one.

**Table–g consistency: 0.3 % where it is integrated to 10 keV, but not pointwise.** This
qualifies the audit's §4 statement.

- **The integrated check.** Carry ρ_R with d ln ρ_R/dN = Σ − 4 (Σ from the table) alongside the
  temperature law (g's from the fits) at fixed field, and compare with (π²/30) g_ρ(T) T⁴:
  - **1.00135** from 5 MeV to 10 keV;
  - **1.00005** from 100 MeV;
  - **0.99922** from 2 × 10⁴ GeV. [kick, case 5; log 02]
  - Everywhere below 100 MeV the ratio from 2 × 10⁴ GeV lies in **[0.99788, 0.99922]**. [scan05]
- **Through the transitions** the same ratio leaves that band. It is **0.9847 at 31.6 GeV** and
  **1.0179 at 178 MeV**, and it recovers by 100 MeV. These values are converged in the step size.
  [scan05]
- **The reason.** The table's Σ is not the Σ the g's imply. At fixed field, entropy conservation
  and continuity give Σ_g = 4 − (4 + d ln g_ρ/d ln T)/(1 + ⅓ d ln g_s/d ln T). The spline class
  and the jax class give the same Σ_g to five figures, so fit ringing is not the cause.

  | Feature | Table (spline) peak | Σ_g peak from the Saikawa–Shirai g's |
  |---|---|---|
  | e⁺e⁻ | 0.1007 at 0.160 MeV | 0.1000 at 0.159 MeV |
  | QCD | 0.3145 at 182 MeV | 0.2990 at 155 MeV |
  | electroweak | 0.0374 at 53 GeV | 0.0580 at 47.6 GeV |

  - The largest pointwise |Σ_table − Σ_g| is **0.078 at 138 MeV** and **0.021 at 45 GeV**.
  - It is **≤ 2.5e-3 at and below 110 MeV**.
  - Integrated over [10 keV, 2 × 10⁴ GeV] the two differ by only 1.1e-3 in ∫Σ d ln T, which is
    why the ρ_R check recovers. [scan05]
- **Consequence.** The kicking term and the temperature law use two descriptions of the plasma
  that agree through e⁺e⁻ annihilation but not through the QCD and electroweak crossovers. Which
  is intended is a question for the authors. It is opened as a board issue (see §5).

## 3. Sampling, and the two redshifts

- **The storage grid.** Histories are stored on a grid of **250 samples per decade of
  Einstein-frame 1 + z** (`config/argument_parser.py`, `DEFAULT_SAMPLES_PER_LOG10_Z`; `main.py`,
  where the z grid is built). The integrator's own steps are set separately by its tolerances and
  step limits.
- **Per decade of T_J.** At fixed field ln(1 + z) = ln T_J + ⅓ ln g_s + const, so there are
  250 (1 + ⅓ d ln g_s/d ln T) samples per decade of T_J.
  - In the BBN window [0.02, 5] MeV that is **636 samples**: 265 per decade on average, between
    250 and 309 locally. [bbn; log 04]
  - While the field moves, the density divides by (1 + A′φ′).
- **The two redshifts.** A stored sample's `z` is the **Einstein-frame** redshift. It is assigned
  from the e-fold count N relative to the end of the integration.
  - It is **not** the Jordan-frame redshift, and it is not derived from T_J.
  - The Jordan-frame temperature is stored separately, as `log_T_Jordan`.
  - Anything the paper plots against "z" from ChamPBH output is against the Einstein-frame label.
    Before `47c50ae`, that label also carried the 1.42-e-fold offset of §1.

## 4. The interface to PRyMordial (after prompts 03 and 04)

- **What is handed over** (`ComputeTargets/BBNData.py`, `compute_BBN_data`).
  - For each stored sample with T_J in **[0.1 eV, 100 MeV]**:
    - ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m);
    - p_NP = −3 M_P² H_J² (1 + ⅔ Ḣ_J/H_J²) − w ρ_R,J.
  - Here ρ_R,J is the stored `log_rhorad_Jordan` and w = (1 − Σ)/3.
  - Ḣ_J/H_J² is built from the Einstein-frame Ḣ and the coupling. Its Ω″ term is **Ω″ π²** since
    prompt 04; before that it was Ω″ π. [bbn, test (g)]
  - PRyMordial runs with `NP_thermo_flag = True` and `Tstart_NP` = 10 MeV. It works from 10 MeV
    down to about 0.3 keV.
  - `compute_BBN_data` refuses a history that does not reach 0.1 × 0.1 eV. That is the paper's
    "at least one decade below the nucleosynthesis window".
- **Why the plasma is unaffected** (README §2 (g)).
  - PRyMordial adds −3H(ρ_NP + p_NP) and dρ_NP/dT to the plasma temperature equation.
  - Because p_NP is built from Ḣ_J, ρ̇_NP = −3H_J(ρ_NP + p_NP) holds identically. The two terms
    cancel, and the plasma obeys the Standard Model equation.
  - The cancellation holds to the order at which PRyMordial's H agrees with H_J. See log 04 for
    the size of the residual this representation leaves, ≈ r · 1.2e-3.
- **The representation (prompt 04). The paper's asinh description is no longer true.**
  - The callbacks spline the ratios r = ρ_NP/ρ_R,J and s = p_NP/ρ_R,J. They are cubic splines
    against ln(T/MeV) at the stored samples.
  - They multiply back by the thermodynamic ρ_SM(T) = (π²/30) g_ρ(T) T⁴.
  - dρ_NP/dT = r′ ρ_SM/T + r ρ_SM′ is the analytic derivative of the interpolant.
  - A non-monotonic T_J sequence is refused (`ComputationFailureError`, with a stored reason)
    rather than sorted.
  - **Measured accuracy** at 250 knots per decade on [0.02, 5] MeV [bbn]:
    - constant ratio: spurious ρ_NP/ρ_SM **5.2e-17** (asinh: 7.9e-10);
    - an oscillating ratio: **9.75e-9** (asinh: 3.3e-8);
    - derivative error / 4ρ_SM **7.4e-7** (asinh: 3.0e-6).
  - Stored ρ_R,J and the thermodynamic ρ_SM differ by −2.1e-3 to −7.8e-4 on [10 keV, 10 MeV].
    That is the table–g offset of §2.
- **The passenger patch (prompt 03).** PRyMordial integrates a third variable T_NP, which no
  physical output reads.
  - Its equation dT_NP/dt ∝ 1/ρ_NP′(T) is singular wherever ρ_NP′ = 0. The vendored copy now
    returns 0 for it (`PRyM/PRyM_main.py`, marked).
  - With it, an oscillating ρ_NP finishes in **≈ 8–9 s**. It did not finish in 120 s before (600 s
    in the audit).
  - ρ_NP ≡ 0 with `NP_thermo_flag = True` reproduces `False` exactly.
  - A constant 0.08 ρ_SM family moves by ≤ 2.8e-7. [prym]
  - New rows carry `PRyM_version = "bf24c3d+cham03"`.
  - A failed solve now stores a `failure_reason`, and `plot_by_beta.py` lists every model it
    drops.
- **The baseline.** A ρ_NP ≡ 0 run goes through exactly the same settings (`compute_SM_baseline`,
  `tools/bbn_baseline.py`) and is drawn on the `plot_by_beta.py` panels. It gives:
  - Yp = 0.2468872958;
  - D/H = 2.462251065 × 10⁻⁵;
  - ³He/H = 1.042050273 × 10⁻⁵;
  - ⁷Li/H = 5.423441017 × 10⁻¹⁰. [bbn]
- **Two caveats for the text** (board §3):
  - **The network.** `compute_BBN_data`'s `small_network` switch has never reached PRyMordial.
    Every BBN result so far used the **full** network. `[03-small-network-flag-is-never-read-by-prymordial]`
    - *Added 2026-09-30 (production-readiness prompt 02):* fixed. The switch now sets the flag
      PRyMordial reads, and production uses the **full** network (`small_network=False`), so the
      stored label and the results agree from `VERSION_LABEL = "2026.3.0"`. The abundances above
      are full-network values and are unchanged.
  - **PRyMordial's noise.** Its abundances move by up to 7e-4 in D/H under 1e-8 relative changes
    to ρ_NP. D/H differences below that level are not resolved.
    `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`

## 5. What is not yet fixed that the paper's text touches

All of these are on the board, `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3.

- **Adiabaticity (review H5).** The paper's *Adiabaticity* paragraph lists three contributions to
  m_eff². The code is missing a fourth.
  - `AdiabaticHistory.py` gives the conformal part of m_eff² as 3 M_P² E Ω″ R. That is zero for
    the exponential coupling.
  - The source-response term (Ω′)² ρ_R,E [Σ(4 − d ln(ρ_J − 3p_J)/d ln T_J) + f_m] is missing.
  - `[00-adiabaticity-diagnostic-omits-the-source-response-term]`
- **The initial condition (review H8).** φ* = 5 M_P and π* = 0 are hard-coded (`main.py`).
  Nothing checks A* T* ≲ M_P, which fails for β ≳ 6.5.
  `[00-initial-field-value-is-hard-coded-and-unchecked]`
- **Reflections (review N1).** The paper's *Reflecting boundary condition* paragraph describes a
  fallback. Each `ScalarModel` row stores how often it fired (`hard_reflections`), but no plot or
  summary reports it, so the text cannot yet say. `[00-hard-reflection-count-is-stored-but-never-reported]`
- **The kicking table's provenance** (§2). `[00-kicking-function-table-has-no-provenance-in-the-repository]`
- **The table–g mismatch through the QCD and electroweak crossovers** (§2).
  `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`
