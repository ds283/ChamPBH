# Brief: normalize the conformal factor at the laboratory reference point

**For:** a Claude Code agent planning a campaign in the ChamPBH repository, under `CLAUDE.md` and
the campaign conventions in `prompts/`. **Written:** 2026-10-06, from a Claude Code session in the
paper repository, with the user. **Tree:** `main` at `9b3db51` (`VERSION_LABEL` 2026.6.0,
`PRYM_VERSION` `bf24c3d+ri02+sr01+bt02`). Treat this brief as evidence to check, not as a
specification (`CLAUDE.md`, invariant 7). The decisions recorded in §4 were made by the user and
are rulings; everything else is a recommendation. The campaign name `jordan-normalization` is a
suggestion. No line has been added to `prompts/INDEX.md` yet.

## 1. The problem

The model is Einstein-frame gravity with reduced Planck mass `Mp`, a canonical scalar φ, and matter
coupled to the Jordan metric g_J = A²(φ) g_E with A = exp(βφ/Mp). The Standard Model masses are
constants of the Jordan-frame Lagrangian. Under the frame map the Einstein–Hilbert term becomes
½ (Mp/A)² R_J, so the Planck mass that matter sees is

    M_J(φ) = Mp / A(φ).

A laboratory measures G_N at some field value φ_ref, so the physical requirement is
M_J(φ_ref) = M_lab ≡ (8π G_N)^(-1/2), i.e. **Mp = A(φ_ref) · M_lab**, not Mp = M_lab.

ChamPBH currently identifies Mp with M_lab through `Units/Planck_units.py`
(`eV = PlanckMass / 2.436e27`, and Kelvin, Second, Mpc from Planck values), while
`ExponentialCoupling` has A(0) = 1. Together these silently choose **φ_ref = 0**. For any other
reference, every Jordan-frame matter-sector input (SM masses through g_*(T), T_init, T_stop, T_CMB,
H0, ρ_m0 = 3 H0² Mp² Ω_m, ρ_r0) is mislabelled by A(φ_ref) to the power of its mass dimension.
Einstein-sector inputs (M, Λ in V = Λ⁴ e^{M/φ}, φ*, and β) are *not* mislabelled: M/Mp, Λ/Mp and
φ/Mp equal the corresponding Jordan-frame ratios to M_J, because the canonical Jordan-frame field is
φ/A and V_J = V/A⁴.

The user has ruled (§4) that the fix is to **bake φ_ref into A**:

    A(φ) = exp(β (φ − φ_ref) / Mp),

so that A(φ_ref) = 1, Mp = M_lab exactly, `Planck_units` is correctly labelled, and every output
(H_J, ρ_E, ρ_R,J, ρ_NP/ρ_R,J, temperatures in MeV) is correct as it comes out. A post-hoc rescaling
of stored histories was considered and rejected: it is easy to forget, and (§2.3) easy to do with
the wrong φ_ref.

## 2. What has already been established

### 2.1 Where A enters the code

`d ln A/dφ = β/Mp` does not depend on φ_ref, so no equation of motion changes. A itself enters only
through the coupling object, at four sites, all in `ComputeTargets/ScalarModel.py`:

- the initial Einstein-frame radiation density, `log_rhorad_Einstein_init` (≈ line 1035);
- the sampled Jordan-frame radiation density, ρ_R,J = ρ_R,E / A⁴ (≈ line 1151);
- `fixed_T_value_at` (≈ line 945), the same conversion;
- `HubblePolicy`, H_J = H_E (1 + β φ'/Mp) / A (≈ line 384–389).

A grep of `extract_common.py`, the plotting scripts and `tools/` found no code that computes a
conformal offset directly from φ. The agent should repeat that audit (§3, R6).

### 2.2 The pipeline's ratio

With φ' ≡ dφ/dN_E and the Einstein-frame Friedmann equation
3 Mp² H_E² (1 − φ'²/6Mp²) = ρ_R,E (1 + f_m) + V,

    1 + r + f_m ≡ 3 Mp² H_J² / ρ_R,J
                = A² (1 + βφ'/Mp)² / (1 − φ'²/6Mp²) · (1 + f_m + V/ρ_R,E).        (Eq. 7)

`compute_BBN_data` forms r exactly this way. With A normalized at φ_ref it is the ratio PRyMordial
needs, with no correction at the interface; T_J/units.MeV is unchanged, since T_J/m_e is the same in
either normalization.

### 2.3 A fixed-point test (β = 1.5, M = 10⁻³ Mp, φ* = 5 Mp)

Script: `source/phiref_iter.py` (a subclass of `ExponentialCoupling` with φ_ref subtracted in
`log_Omega`; runs `compute_scalar_model._function` as `tools/history_and_bbn.py` does; ChamPBH was
not modified). Each history took about 5 s. The test used **φ_ref = φ(T_CMB)**, not the φ_min the
user has since ruled for (§4, U2); the convergence behaviour should carry over.

- Baseline (Λ = 1 meV, φ_ref = 0) reproduces the production store: h = 0.406, Ω_V = 0.083,
  f_m(T_CMB) = 3407.
- Fixed point: update φ_ref ← φ₀ and Λ ← Λ (V_target / V(φ₀))^{1/4} with
  V_target = A₀⁴ [ 3 Mp² H0² (1 − φ₀'²/6Mp²) / (A₀² (1 + βφ₀'/Mp)²) − ρ_R,J (1 + f_m) ]
  (Eq. 7 solved for the V that gives H_J(T_CMB) = H0). Starting from Λ⁴ = ρ_Λ and φ_ref = 0,
  convergence is linear, a factor 5–10 per iteration:
  |φ₀/φ_ref − 1| = 8e-3, 1.6e-2, 1e-3, 2.6e-4, 3e-5, 3e-6, 9e-7 and |h/0.6766 − 1| reaching 3e-4
  after 3 iterations and 1e-8 after 7.
- Converged: Λ = 2.1272 meV, φ₀ = 0.03333 Mp (2βφ₀ = 0.100), h = 0.6766, Ω_m = 0.311,
  **Ω_V = 0.569, not 0.689**: the rest is the (1 + βφ')² factor in Eq. 7, since the field moves at
  0.88 of the tracking speed (βφ₀' = 0.066). Matching H0 is therefore not the same as V(φ₀) = ρ_Λ at
  this M.
- BBN (small network; SM baseline Yp = 0.24688, D/H = 2.4583): φ_ref = 0 gives Yp −0.05 %,
  D/H +0.17 %; the fixed point gives **Yp −3.57 %, D/H −9.13 %**. The normalization reverses the
  sign of the BBN signal at this M.
- Post-hoc check: Eq. 7 rescaling of a φ_ref = 0 run, (1 + r + f_m) e^{−2βφ_ref/Mp} − 1 − f_m,
  reproduces the fixed point to a median 4e-6 in r *if* φ_ref is the converged value (25 of 1498
  samples differ, all at reflections, whose timing moves slightly). But φ₀ read from the φ_ref = 0
  run is 8.7 % low, because V/ρ_E changes by A₀⁴ at late times and φ_min ∝ ρ_E^{−1/2}. That error
  costs Δr ≈ 0.007, Yp 0.3 %, D/H 0.8 %. This is why the fix integrates with the right A.
- For the converged history, φ_min(T_CMB) (§3, R2) is 1.0456 φ₀, i.e. 2β(φ_min − φ₀)/Mp = 0.0046:
  even a tracking field lags the minimum at this M.

### 2.4 The Planck-mass constant

PRyMordial uses G_N = 6.70883e-45 MeV⁻² (`PRyM/PRyM_init.py`), i.e. Mp = 2.435324e18 GeV.
`Planck_units` uses 2.436e27 eV. Its Kelvin (from the Planck temperature 1.416784e32 K) then implies
1 K = 8.61973e-5 eV, against k_B = 8.617333e-5 eV/K: an internal inconsistency of 2.8e-4 between the
eV-based and the kelvin-based inputs. With eV = Mp / 2.435324e27 the implied value is 8.617335e-5,
consistent to 2e-7.

## 3. What the campaign should do

- **R1. Coupling.** Add φ_ref to `ExponentialCoupling`, entering `log_Omega` and `Omega` but not
  `d_logOmega_dphi` or `d2_logOmega_dphi2`. It is part of the coupling's identity in the datastore
  (and in `name`). Decide whether `AbstractCoupling` should declare it.
- **R2. The fixed-point driver.** Wrap `compute_scalar_model` so that each history is integrated
  until both conditions hold at T_J = T_CMB:
  - **φ_ref = φ_min(T_CMB)** (ruling U2), the minimum of the Einstein-frame effective potential at
    the history's own densities today. Radiation is traceless today (Σ(T_CMB) ≈ 0), so it solves
    V'(φ_min) + (β/Mp) ρ_m,E(φ₀) A(φ_min)/A(φ₀) = 0, with ρ_m,E ∝ A at fixed Einstein-frame number
    density. A bracketed root-find (e.g. `brentq` on [0.2, 5] × φ₀) worked in the test. The
    agent should check whether the Σ ρ_R,E term is worth keeping for generality.
  - **H_J(T_CMB) = H0** at fixed ω_m (ruling U1), with Λ updated from Eq. 7 as in §2.3.
  Choose a tolerance (the test suggests 1e-5 relative in both is cheap) and an iteration cap, and
  give non-convergence its own failure reason. Two failure modes to anticipate: V_target ≤ 0 (the
  kinetic and Brans–Dicke factors alone exceed H0, so no Λ works), and oscillation of the update for
  histories still rebounding at T_CMB. Cost: about 3–7 integrations per history; production
  histories take 1.4–263 s each (median 22 s, `ChamPBH-stores-bt02`).
- **R3. Model identity and `main.py`.** Λ becomes an output, not a scanned input: a history is
  specified by (β, M, φ*, n) and the targets (H0, ω_m from `Planck2018`). `main.py` currently scans
  Λ (`--log10-Lambda-*`, `--Lambda-values-*`) and builds `ExponentialPotential` rows keyed on
  (M, Λ). The planner must decide how a tuned Λ and φ_ref are stored (on the potential and coupling
  rows, or as `ScalarModel` outputs), and what happens to the Λ scan options. Store with each
  history: the converged Λ and φ_ref, the actual φ₀ and φ₀', the tracking diagnostic
  φ₀'/(1.5 φ₀), the iteration count and the final residuals. Bump `VERSION_LABEL`. It is worth
  keeping a fixed-Λ, fixed-φ_ref path (with φ_ref = 0 it reproduces the present code) for
  regression tests and diagnostics, but the planner decides whether it is a user-facing option.
- **R4. Planck mass** (ruling U3). Set `Planck_units` from PRyMordial's G_N (§2.4), and check that
  Metre, Second, Kelvin and Kilogram are consistent with it to ≲1e-6. Decide whether ChamPBH should
  import the constant from one place shared with the PRyMordial interface.
- **R5. Jordan-frame redshift** (ruling U4). Add z_J to `ScalarModelValue`, computed at sampling.
  `cosmology.z(T)` already implements 1 + z = (T/T_CMB) [g_s(T)/g_s(T_CMB)]^{1/3}, which is exactly
  the Jordan-frame redshift (entropy conservation in the Jordan frame). The stored `z` remains the
  Einstein-frame redshift from N; name the new column unambiguously and update the `Redshift`
  bullet of `CLAUDE.md`.
- **R6. Audit.** Search for anything that assumes A(0) = 1 or forms 2βφ/Mp as a gravitational
  offset: `extract_common.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, `tools/`, `AdiabaticHistory`,
  `first_bounce`, the fixed-temperature columns, and the documents in `.documents/` (in particular
  any statement that Planck units "scale out" Mp). Do not add a post-hoc φ_ref rescaling to
  `tools/bbn_from_store.py`.
- **R7. Tests** (no Ray, no persistent store, per `CLAUDE.md`):
  - with φ_ref = 0, Λ fixed and no iteration, the integrator reproduces the present outputs on a short
    history (regression);
  - Eq. 7 holds at stored samples, and at a fixed Einstein-frame state, shifting φ_ref by δ
    multiplies 1 + r + f_m by e^{−2βδ/Mp};
  - a converged history has |φ_ref/φ_min − 1| and |H_J(T_CMB)/H0 − 1| within tolerance, and
    A(φ_ref) = 1;
  - z_J agrees with (A(φ₀)/A(φ)) (1 + z_E) along a history to integration accuracy (this also checks
    the T_J equation);
  - 1 K in eV equals k_B to 1e-6 under `Planck_units`.

## 4. Rulings (user, 2026-10-06)

- **U1. Target.** Tune Λ so that H_J(T_CMB) = H0 (Planck 2018 h = 0.6766) at fixed ω_m. Fitting the
  CMB acoustic angle θ* is out of scope.
- **U2. Reference point.** φ_ref = φ_min(T_CMB) for every history, not the instantaneous φ(T_CMB)
  and not a hybrid. Neither choice is exact at the simulated M (the model does not screen the Milky
  Way there, so no φ_ref is literally the laboratory value); φ_min is independent of the phase of a
  rebounding field and is smooth in the parameters. Store the actual φ₀ so the alternative can be
  recovered.
- **U3. Planck mass.** Use PRyMordial's value of G_N.
- **U4. Redshift.** Compute and store the Jordan-frame redshift.
- **U5. Store.** The user re-runs the store manually; no prompt starts a production run.

## 5. Stop and ask if

- the φ_ref = 0, fixed-Λ path does not reproduce the present outputs;
- a class of histories does not converge, or V_target ≤ 0 for a region of (β, M);
- the agent finds a place where A's normalization enters other than the four in §2.1, with
  consequences beyond relabelling;
- storing the tuned Λ requires a datastore design choice the planner cannot make from this brief.

## 6. Out of scope

Re-running the production store (U5); the paper text; fitting θ*; couplings other than
`ExponentialCoupling` (the only one in the tree); the BBN interface itself, which needs no change.

## 7. Why it matters

At laboratory-allowed M the reference makes no difference (2βφ₀/Mp ~ 10⁻¹⁵). At the simulated M it
decides the sign of the BBN signal (§2.3: Yp −3.6 % instead of ≈ 0 at β = 1.5, M = 10⁻³ Mp), the
late-time history used for the dark-energy figures, and the Hubble rate today. Building the
normalization into A means no downstream user has to remember to rescale anything.
