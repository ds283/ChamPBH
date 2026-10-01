# Numerical campaign: re-evaluation after `integrator-remediation`

**Date:** 2026-10-01. **Tree:** ChamPBH `main` at `6aaa706`; `VERSION_LABEL` 2026.5.0. This tree
contains the `integrator-remediation` campaign (`fc97233`–`1265c75`) and verification §4.8–§4.9.
**Supersedes:** the cost tables and §2.2–§2.4 of `bbn_campaign_design.md`, which describe the
two-region integrator that the campaign removed.

## 1. What was run

**The grid.** 38 histories through the production `compute_scalar_model._function`, with no Ray
cluster and no datastore, and with `main.py`'s initial data:
- φ\* = 5 M_P, π\* = 0, T\* = 2×10⁴ GeV;
- atol = rtol = 1e-8;
- the 250-per-decade z grid;
- exponential potential, n = 1, V₀ = (10⁻³ eV)⁴.

| M / M_P | β values |
|---|---|
| 0.5, 0.1, 10⁻², 10⁻³, 10⁻⁴, 10⁻⁵ | 1.2, 1.6, 2.0, 2.5, 3.0 |
| 10⁻² and 10⁻⁵ (chaos window) | additionally 1.90, 1.95, 2.05, 2.10 |

**Two BBN solves per history**, both through the production `compute_BBN_data._function`:
(a) the callbacks replaced by (ρ_NP, p_NP = −ρ_NP, dρ_NP/dT = 0), called *H-only* below;
(b) the shipped `NP_thermo_flag` route, under a 90 s wall limit.

**Load.** Nine histories ran at once on a 10-core Mac, so wall times are inflated. RHS counts do
not depend on load. Every count that overlaps verification §4.8/§4.9 matches it exactly, e.g.
β = 2, M = 10⁻⁵: 1 679 987 RHS.

Full table: `post_remediation_grid.csv`. Harness: `history2.py`, `driver.py`, `bbn_domain.py`.

## 2. Results

**Integration is solved at the target precision.**
- All 38 histories completed with 0 reflections.
- Integration cost scales roughly as M^(−0.3).

| M / M_P | RHS per history | ≈ s per history at 47 µs/RHS |
|---|---|---|
| 0.5 | 2.4–5.8×10⁴ | 1–3 |
| 10⁻² | 1.0–1.5×10⁵ | 5–7 |
| 10⁻³ | 1.6–3.3×10⁵ | 7–15 |
| 10⁻⁴ | 0.4–0.9×10⁶ | 18–40 |
| 10⁻⁵ | 0.9–2.2×10⁶ | 40–100 |

- The 47 µs/RHS figure is log 04's unloaded rate. Sampling onto the z grid adds a few seconds.
- At β = 2, M = 0.5 the new integrator reproduces the old one's observables:
  - φ_park(pre) 0.02343 against 0.02343, and φ_park(post) 0.012516 against 0.01252;
  - D/H ×10⁵ 2.5597 against 2.5604.

**The BBN interface is still the blocker.** The shipped `NP_thermo_flag` route gave a usable
answer on **0 of 38 histories**: 36 did not return within 90 s, and 2 hit a PRyMordial solver
failure. The H-only route succeeded on 37 of 38, in 25 s unloaded and 34–120 s loaded. BBN is now
the dominant cost per history for M ≥ 10⁻³.
- The fix is a change in `BBNData.py` only (no PRyMordial patch). It is not yet in the
  repository, and it isn't on any board or in `OPEN_ISSUES.md`.

**One new BBN failure mode at small M.** β = 1.6, M = 10⁻⁵ failed in PRyMordial's low-T nuclear
network ("step size less than spacing", t = 1.28×10⁶ s of 1.32×10⁶ s), even on the H-only route.
- Below about 3 keV, the stored ρ_NP/ρ_R,J jumps between ±0.4 % from sample to sample. The z grid
  samples the matter-era bounces at random phase, as verification §4.9 says, so the spline
  PRyMordial integrates is noise.
- Moving the spline's lower limit from 0.1 eV to 10 eV or 100 eV does not cure it; the noise sits
  at 0.3–3 keV, inside PRyMordial's own range.
- This is `[00-stored-samples-alias-the-rebounds]` reaching the BBN interface.

**The first bounce cannot be read from stored samples at small M.**
- My sample-based detector (the first π sign change with φ < 1.5M) returns nonsense for
  M ≲ 10⁻³: for example 0.138 GeV instead of 0.747 GeV, and 9×10⁻⁸ GeV.
- The cause is that the bounce at φ_min ~ 10⁻⁸ M_P lasts far less than one sample.
- The dense-output turning point is M-independent: 746.69 MeV at β = 2 for every M ≤ 10⁻², per
  §4.8/§4.9 and the user's ruling. It has to be stored at integration time.

**The physics converges for M ≲ 10⁻², even history by history.**

| β | ΔD/H at M = 0.5 / 0.1 / 10⁻² / 10⁻³ / 10⁻⁴ / 10⁻⁵ (%) | ΔYp at M ≤ 10⁻² (%) |
|---|---|---|
| 1.2 | 7.5 / 6.0 / 5.67 / 5.79 / 5.63 / 5.72 | +3.97 to +4.02 |
| 1.6 | 2.8 / 0.59 / 0.11 / −0.03 / −0.19 / (BBN failed) | ≈ 0 |
| 2.0 | 4.0 / 1.09 / 0.20 / 0.07 / 0.23 / −0.02 | −0.10 to −0.07 |
| 2.5 | 5.0 / 1.15 / 0.24 / 0.11 / 0.38 / 0.22 | −0.02 to +0.06 |
| 3.0 | 5.8 / 1.26 / 0.25 / −0.02 / 0.07 / 0.02 | −0.01 to +0.04 |

Chaos window, ΔD/H in %:

| β | M = 10⁻² | M = 10⁻⁵ |
|---|---|---|
| 1.90 | 0.09 | 0.08 |
| 1.95 | 0.50 | 0.40 |
| 2.00 | 0.20 | −0.02 |
| 2.05 | 1.91 | 1.91 |
| 2.10 | 1.35 | 1.06 |

The first-plateau φ_park agrees to the third digit, e.g. 0.02765 against 0.02754 at β = 2.05.

- Below M ≈ 10⁻² the wall behaves as a hard wall at φ ≈ 0, and the histories converge to that
  limit. Even the chaotic outcome of the QCD-era rebounds becomes M-independent.
- The pointwise differences are 0.1–0.3 % in D/H. That is comparable to the sampling (0.18 %) and
  tolerance (0.16 %) effects measured before, and below the ±1 % scatter across β.
- The M-dependence we saw at M = 0.5 and 0.1 is the wall-floor effect (φ_park ≥ φ_wall ≈ M/65).
  It is gone by M = 10⁻².

**ΔG/G = A² − 1 at z = 1090 scales almost linearly with M.**

| M / M_P | ΔG/G at z = 1090, β = 2 |
|---|---|
| 0.5 | 0.11 |
| 0.1 | 0.024 |
| 10⁻² | 2.8×10⁻³ |
| 10⁻³ | 3.3×10⁻⁴ |
| 10⁻⁴ | 3.9×10⁻⁵ |
| 10⁻⁵ | 5.2×10⁻⁶ |

It is set by the tracking value φ ~ M, so it is negligible at laboratory-allowed M.
- *Caveat:* at small M it is read from one sample at random bounce phase, so treat these values
  as order of magnitude only.

## 3. What this changes in the campaign

1. **The (β, M) "curve" collapses to a function of β.** For M ≲ 10⁻² the BBN outcome depends
   on β alone, including the chaotic scatter. The M-dependent part (M ≳ 0.03) lies in the
   laboratory-excluded region (review H2). So the paper's statement that the result is
   insensitive to M becomes correct, with the qualifier "for M ≲ 10⁻² M_P". One figure panel can
   show the approach to that limit.
2. **The science question is now narrow.**
   - Just above the surfing threshold (β = 1.2) the field is still parked on the first plateau
     at weak freeze-out, at φ ≈ 0.12 M_P. That gives ΔYp ≈ +4.0 % and ΔD/H ≈ +5.7 %,
     independent of M.
   - By β = 1.6 both shifts are ≲ 0.4 %.
   - What has to be located is the boundary in β between 1.2 and 1.6.
   - Also needed: the frequency and size of chaotic outliers at higher β (e.g. +1.9 % at
     β = 2.05), and whether they matter for the error budget.
3. **Cost is no longer the constraint.** At M = 10⁻³ a history costs ≈ 10 s to integrate plus
   ≈ 25 s for BBN. The fine β map that the chaos requires becomes cheap.

### Phase A — code changes before the science run

| # | change | why | blocking? |
|---|---|---|---|
| A1 | BBN H-only callbacks; sanity bounds 0 < Yp < 0.5, D/H > 0; a wall-clock limit on the PRyMordial call | thermo route unusable on 38/38 | **yes** |
| A2 | Remove aliasing from ρ_NP/ρ_R,J below a few keV before splining: average over bounces, or record turning points (`[00-stored-samples-alias-the-rebounds]`) | 1 PRyMordial failure in 37; random-phase noise of ±0.4–0.9 % in the input | yes for M ≲ 10⁻⁴ |
| A3 | Store the dense-output first bounce (N, T_J, φ_min) in `extra_data` | samples fail for M ≲ 10⁻³ | yes, for the T_deliver observable |
| A4 | Add `--phi-init-Mp` and a store tag (H8; still hard-coded at `main.py:837`) | the sub-threshold (β, φ\*) map | for block C5 only |
| A5 | Failure-reason column for `ScalarModel` (`[00-scalarmodel-failure-rows-carry-no-reason]`) | survey bookkeeping | no |
| — | Do not report `AdiabaticHistory` max\|Q\| for M ≲ 10⁻³ until `[post-adiabatic-Q-reads-aliased-late-samples]` is settled | aliasing | no (not needed for Fig. 6) |

### Phase C — production roster (fresh datastore, label 2026.5.0 or later)

| Block | β | M / M_P | φ\* | n | est. core-h |
|---|---|---|---|---|---|
| C1, main map | 1.00–3.00, Δβ = 0.01 | 10⁻³ | 5 | 201 | ≈ 2 |
| C2, confirmation of the limit | 1.00–3.00, Δβ = 0.01 | 10⁻⁵ | 5 | 201 | ≈ 6 |
| C3, approach to the limit | 1.00–3.00, Δβ = 0.05 | 0.5, 0.1, 0.03, 10⁻² | 5 | 164 | ≈ 1.5 |
| C4, threshold zoom | 1.10–1.70, Δβ = 0.005 | 10⁻³ | 5 | 121 | ≈ 1.2 |
| C5, non-surfing region | 0.5–1.1, Δβ = 0.05 | 10⁻³ | 1, 2, 5 | 39 | < 0.5 |
| — | SM baseline (same path) | — | — | 1 | — |

The total is ≈ 11 core-hours: one night on the 10-core machine, or about an hour on a small
cluster allocation. It was 200–300 core-hours on the old integrator.

**Optional cross-check at laboratory-allowed M.** The physical-M β = 2 history of §4.8 fails on
the step budget only at T_J ≈ 0.02 eV, long after BBN. If the BBN spline's lower limit is raised
to about 1 keV (`[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]`), a history stopped at
`--T-stop-GeV` ≈ 1e-7 would give BBN at physical M without the parked-tracking model. That would
test the M → 0 limit directly.

### Phase D — figures

1. **Fig. 6 replacement.** ΔD/H and ΔYp relative to the same-path SM baseline, against β, from
   C1 and C4: individual histories as points, with a running median and a 16–84 % band.
2. **Convergence in M.** The approach to the M-independent limit, from C3 together with C1 and
   C2; this is `M_convergence.png` at full resolution.
3. **T_deliver(β).** Dense-output values, with Σ(T) = 1/(3β²) overlaid.
4. **φ_park and ρ_NP/ρ_R at 1 MeV and 70 keV, against β.** This is the mechanism panel for H1.
