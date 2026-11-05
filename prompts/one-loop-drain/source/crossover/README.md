# The crossover side-campaign

**Campaign:** `one-loop-drain` ([README §8.9](../../README.md)) · **Issue:**
`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]` (board §3) · **Started:** 2026-11-04, at the user's ruling ·
**Lands before:** prompt 03.

Research, run by the orchestrator. Nothing here changes production code. The question: what fraction of a bounce's
kinetic energy is produced as quanta through the crossover from the one-loop regime (ℓ = ln(k_ex/m_ta) ≳ 4, where
`wall_backreaction.tex` verifies 2.7/ℓ) to the classical wall. The one-loop law used today rises as ℓ falls, up to
α_cap.

## Files

| file | what |
|---|---|
| `cascade_map.py` | runs one history at a tuned normalization (from a 02/02b `result.json`) under the one-loop law, and lists every candidate's regime, ℓ, α_eff, ln(E/V0), ln R, L = M/φ_t, ℓ_t and ln(E/M⁴) |
| `walls.py` | the wall U = c(e^{1/φ} − 1) measured from its floor, in units M = ω_t = 1; the classical bounce; \|β_k\|² of a massive test field by first-order adiabatic projection |
| `stage1a_classical.py` | S(L) from L = 0.3 to 134 |
| `rs_along_cascade.py` | R·S(L) at each bounce of a mapped cascade |
| `born.py` | the Born-regime scheme of `wall_backreaction.tex` (`scripts/return_fraction.py`) generalized to any (L, R); the chain in these units; the production on a trajectory as the scheme gives it, by the Born formula, and with massive mode functions |
| `stage1b_born.py` | the scheme with k_IR = √V''_max, self-consistent, at bounces of a mapped cascade |
| `stage1b_kmin.py` | the massive-mode production against the lower end of its k grid |
| `stage1b_occupation.py` | the occupation numbers on the stage-1b trajectories |
| `stage2-brief.md` | the handoff brief for stage 2, for a fresh session |
| `stage2.py` | stage 2's solver: the one-loop background with the tadpole from massive modes, carried as Bogoliubov coefficients on the instantaneous adiabatic basis, renormalized against the instantaneous vacuum; optionally the Hartree term in the modes |
| `stage2_validate.py` | stage 2's validation (brief §6): the Born limit, the classical limit, energy, convergence, the start threshold at large ℓ |
| `stage2_modes.py` | the two mode equations (brief §4): the scheme's soft tail, and the Hartree (fixed-variance linearized) equation |
| `stage2_scan.py` | the scan (brief §7): 2197 at A = 0.05 and 0.01, R-865, and an (L, ln R) grid |
| `stage2_mscan.py` | stage 2 on the curves of other M (the β = 2 ladder near M_crit, and 10⁻³ M_P), against the measured law |
| `fit_law.py` | the qualitative drain law fitted to every stage-2 point, in ℓ, L and R |
| `plot_law.py` | `fig/drain_law.png`: the gap law along each history's own (L, R) path and at the grid's fixed L, and the one-loop law against ℓ beside 2.7/ℓ and 1 − exp(−2.7/ℓ), each with stage 2's points at their own (L, R) |
| `measured_law_map.py` | `cascade_map.py` with the drain replaced by stage 2's measured fraction, patched in-process (no production change) |
| `out/` | every output, text only |
| `fig/` | figures |

Run from this directory with `../../../../venv/bin/python`, except `cascade_map.py`, which runs from the repository
root with `PYTHONPATH=.`.

## Stage 1a: the classical wall at small L (2026-11-04)

**Validation.** At L = 40, 70, 90, 110 and 134, S(L) equals `production_energy_ratio.tex` Table `tab:beta` to its quoted
digits (1.3936 × 10⁻⁴, 4.9353 × 10⁻⁵, 3.0600 × 10⁻⁵, 2.0810 × 10⁻⁵, 1.4204 × 10⁻⁵). At L = 70, k = 0.3 ω_t,
\|β\|² = 2.325252 × 10⁻⁴ against the note's 2.32525 × 10⁻⁴. A hundredfold stricter adiabatic start changes S in the
fifth digit at most. Output: [`out/stage1a_S_of_L.txt`](out/stage1a_S_of_L.txt).

S(L) was extended to L = 0.3, 0.5 and 0.7 for the gap at A = 0.01 (below): 1.0 × 10⁻², 9.7 × 10⁻³, 9.3 × 10⁻³.

**Result.** The wall stays nearly reflectionless down to L = 1: \|β\|²_max is 0.067 at L = 1, 0.022 at L = 5 and
0.008 at L = 10. S(L) is 8.7 × 10⁻³ at L = 1, 3.2 × 10⁻³ at L = 5 and 1.35 × 10⁻³ at L = 10. L²S falls from the note's
0.22–0.26 at L ≥ 40 to 0.08 at L = 5.

## Where the cascade's bounces sit

`out/cascade-P-2197-A0.05.txt` is 2197 (β = 1.185, laboratory M) at A = 0.05, gap `zero`, at 02b's tuning.
- As ℓ falls from 7.4 to 0.52 over the cascade's last hundred bounces, L falls from 21 to 4.8 and ln R from 44 to 17.
- E/V0 stays ≫ 1, so these bounces are on the exponential wall, not in its 1/φ tail.
- R·S(L) (`out/rs-P-2197-A0.05.txt`) is 7.8 × 10⁴ at the last bounce and larger before it.

`out/cascade-P-2197-A0.01.txt` is the same history at A = 0.01, at the A = 0.1 tuning's normalization: 02's
A = 0.01 tuning failed, and the seed is the same at every A. In it:
- the one-loop drains end at bounce 183, at ℓ = 0.083, where the gap begins, at ln R = 13.8 and L = 3.6;
- the gap runs to bounce 690, where R = 1, at L = 0.68 and ln(E/V0) ≈ 0;
- then 13 260 classical bounces follow, in the 1/φ tail, down to L = 4 × 10⁻⁹ and ln R = −97.

R·S(L) (`out/rs-P-2197-A0.01.txt`) is 4 × 10³ at gap entry and crosses 1 at bounce 512 (ln R = 4.9, L = 1.35). So
the first 329 of the 508 gap bounces have R·S > 1, and the last 179 and every classical bounce have R·S < 1, down to
9 × 10⁻³ at the first classical bounce and falling.

## Stage 1b: the Born scheme on the cascade's bounces (2026-11-04)

**Validation.** At the note's worked example (L = 105.4, the note's R), `born.py` gives E_fl/E = 0.5601,
v_out/v_in = 0.663 and M/φ_ta = 40.0 at k_IR = 0.01 k_ex, and 0.992, 0.088, 40.3 at 0.1 k_ex. That is the note's table
row for row. The Born formula agrees with the scheme's E_fl to four digits. In these units the chain's ℓ equals the
code's ℓ to four digits at every point below, so the mapping from (L, ln R) is right.

**Run** (`out/stage1b_born.txt`; 2197 at A = 0.05, the bounces nearest each target ℓ). k_IR = √V''_max,
self-consistent to 10⁻³. Production as a fraction of E:

| ℓ | L | ln R | scheme E_fl/E | massive modes | 2.7/ℓ | scheme / law | v_out/v_in |
|---|---|---|---|---|---|---|---|
| 32.3 | 102.1 | 138.1 | 0.084 | — | 0.084 | 1.00 | 0.96 |
| 9.54 | 26.7 | 52.2 | 0.259 | 0.260 | 0.283 | 0.92 | 0.86 |
| 7.40 | 21.0 | 44.4 | 0.322 | 0.322 | 0.365 | 0.88 | 0.82 |
| 4.91 | 14.7 | 35.4 | 0.447 | 0.447 | 0.549 | 0.81 | 0.74 |
| 3.68 | 11.7 | 30.7 | 0.552 | 0.553 | 0.733 | 0.75 | 0.67 |
| 2.80 | 9.7 | 27.2 | 0.663 | 0.664 | 0.964 | 0.69 | 0.58 |
| 2.38 | 8.8 | 25.5 | 0.731 | 0.732 | 1.13 | 0.65 | 0.52 |
| 1.99 | 7.9 | 23.8 | 0.808 | — (not back out) | 1.36 | 0.59 | — |
| 1.49 | 6.8 | 21.6 | 0.919 | — (not back out) | 1.82 | 0.51 | — |
| 0.98 | 5.7 | 19.2 | fails: k_IR = 1.15 k_ex, no drag | | | | |

**What it shows.**
1. **The mass in the mode functions changes nothing** on these trajectories. The massive production equals the
   scheme's to 0.1–0.2% from ℓ = 9.5 down to 2.4, once the modes are started adiabatically (`out/stage1b_kmin.txt`).
2. **With the physical infrared end, the scheme's fraction lies below 2.7/ℓ, and the gap widens as ℓ falls.**
   - It is 0.92 of the law at ℓ = 9.5 and 0.65 at ℓ = 2.4.
   - **But it still rises as ℓ falls.** At ℓ ≈ 2 the field no longer comes back out of the wall within the run, with
     81% of E in quanta, and at ℓ ≈ 1.5 92% is.
   - At ℓ ≈ 1 the infrared end passes k_ex, the drag changes sign, and the field runs on to the classical wall, where
     the scheme fails.
3. **The classical limit is not elastic where the one-loop chain fails.** R·S(L) is about 10⁵ at the last one-loop
   bounces at A = 0.05, and 4 × 10³ at gap entry at A = 0.01. A test field reaching the classical wall there would
   produce far more than E. The nearly elastic classical reflection holds only once R·S(L) < 1, which at A = 0.01 is
   the gap's last 179 bounces and the classical tail.

So stage 1 does not support a fraction that falls as ℓ → 0 within the one-loop range. The fraction rises toward
total absorption more slowly than 2.7/ℓ. A fall is expected only where R·S(L) < 1. Between the chain's failure and
R·S(L) ≈ 1, neither the Born scheme nor the classical test field applies. That is the region a self-consistent solve
(stage 2) has to cover. At A = 0.01 at laboratory M it is the gap's first 329 bounces, where `zero` drains nothing.

## Occupation numbers near full dissipation (2026-11-04)

Asked by the user: do the occupations stay O(1) or below as the dissipation approaches 1?
[`out/stage1b_occupation.txt`](out/stage1b_occupation.txt), on the stage-1b trajectories, from the Born formula with
massive modes alongside; the two agree to three digits wherever the field comes back out.
- **The produced energy sits at 0.6–5 k_ex** (10–90% of it).
- **The occupations there are ≪ 1 throughout.** The energy-weighted mean n is 10⁻¹³ at ℓ = 7.4, 7 × 10⁻⁸ at 3.7,
  6 × 10⁻⁶ at 2.4 and 1.2 × 10⁻⁴ at 1.5, where 92% of E is in quanta.
- **The largest occupation is at the soft end, k ≈ m_ta,** and it too stays small: 5 × 10⁻⁸, 6 × 10⁻⁵, 7 × 10⁻⁴ and
  4 × 10⁻³ at the same ℓ, growing roughly as e^{−2ℓ}.

Near-total dissipation comes from phase space, not from occupation. The quanta are spread over modes up to a few
k_ex, each weakly occupied, while E ~ m_ta⁴ℓ/32π² is set by the much lower mass. So production stays in the
linear-response (Born) regime, with no Bose enhancement, up to the edge of the scheme.

## Stage 2: the self-consistent solve with massive modes (2026-11-04)

Run from [`stage2-brief.md`](stage2-brief.md). Outputs: [`out/stage2_validation.txt`](out/stage2_validation.txt),
[`out/stage2_modes.txt`](out/stage2_modes.txt), [`out/stage2_scan.txt`](out/stage2_scan.txt),
[`out/cascade-R-865-A0.05.txt`](out/cascade-R-865-A0.05.txt).

**The solver** (`stage2.py`) is the scheme of `wall_backreaction.tex` §"The Erickcek et al. scheme" without the Born
reduction.
- **Background.** φ'' = −U' − K V''' Σ with K = 4π²RE, which is the physical ½V'''⟨δφ²⟩ in walls.py's units. In the
  Born limit Σ = −I/8π², which is `born.py`'s +G V''' I.
- **Modes.** Each obeys the scheme's equation χ'' + (k² + V''(φ(t)))χ = 0. They are carried as Bogoliubov
  coefficients on the instantaneous zeroth-order adiabatic basis.
  - Then |χ|² − 1/2ω = (\|β\|² + Re αβ*e^{−2iθ})/ω and the energy per mode is ω\|β\|², both exactly.
  - Σ and the energy are therefore measured against the static vacuum at the same φ, as the brief asks, with no
    cancellation. A direct integration of χ cannot resolve the occupations, which are 10⁻¹³–10⁻⁴.
- **Conserved energy.** E = ½φ'² + U + 2Kρ, conserved exactly by the equations.
- **Integration.** Fixed-order RK4, with the step 0.1 over max(2ω) on the started modes, about 600 steps per 1/k_ex.
- **Mode starts.** Each mode starts in the first-order adiabatic vacuum, when its adiabaticity reaches EPS or when V''
  reaches 10⁻⁶ m_ref².
- **Grid.** 80 modes a decade, from 0.03 m to 30 k_ex. In the gap the grid is set from the turn of a first pass.
- **Cost.** A run takes 3–25 s. The scan took about 10 minutes on six workers.

### Validation

1. **The Born limit (V1).**
   - With the mode grid cut at k_IR = 10 m_ta, where every mode has k ≫ m, stage 2 equals `born.py` run at the same
     k_IR to 0.04–0.07%, at ℓ = 9.5, 7.4, 5.8 and 4.9. This checks the loop normalization and the renormalization.
   - Cut at `born.py`'s own self-consistent k_IR = √V''_max, stage 2 is 2.1–3.7% higher. The modes near m are not in
     the Born regime there.
   - **With the physical infrared end** (massive modes from 0.03 m_ta, no cutoff), stage 2 gives 0.960, 0.950, 0.940
     and 0.932 of `born.py`'s self-consistent fraction. The mass ends the logarithm at a slightly lower point than the
     cutoff does, so a difference of order 0.4/ℓ was expected, and no closer agreement.
2. **The classical limit (V2, V2q, V6).**
   - With the loop off, 8π²ρ equals S(L) at L = 5, 10 and 40 to 10⁻⁵ of the converged value.
   - Stage 1a's S(L) is 0.100% above that converged value. V2q traces the 0.1% to stage 1a's quadrature, a linear-k
     trapezoid on 121 log-spaced points: on 481 points walls.py's own \|β\|² gives stage 2's value. The note's
     `tab:beta` agrees with stage 1a to its quoted digits, so it presumably carries the same 0.1%. Not acted on;
     stage 1a stands as recorded.
   - With the loop on, at the last gap bounce's L = 0.678, frac/R tends to S(L) as R → 0: 9.33 × 10⁻³ at ln R = −6,
     against 9.36 × 10⁻³ with the loop off.
3. **Energy (V3).** The largest \|E_tot − E\|/E along a run is 10⁻¹¹–10⁻⁷ in every validation and scan row
   (6.6 × 10⁻⁵ at one grid corner, L = 1.3, ln R = 25).
4. **Convergence (V4).** Five points, ℓ = 4.9 to the gap at R·S = 8.6:
   - halving the mode spacing changes the fraction by ≤ 2 × 10⁻⁷;
   - halving the step, by ≤ 1 × 10⁻⁷;
   - EPS/10 and F0/100, by ≤ 2 × 10⁻⁶;
   - doubling k_max, by ≤ 8 × 10⁻⁴;
   - halving k_min, by ≤ 1.4 × 10⁻³ (the largest is at the gap point; ≤ 5.5 × 10⁻⁴ on the one-loop side).

   The stated precision of the scan is therefore ±2 × 10⁻³ in the fraction.
5. **The start threshold at large ℓ (V5).**
   - At ℓ = 17.75 (R-865), EPS = 10⁻³ leaves the modes' energy 1.7 × 10⁻² off E's balance. The second-order start
     mismatch is weighted by the hierarchy e^{4ℓ}.
   - The background's own balance does not move, to five digits.
   - At EPS = 10⁻⁹ the error is 1.6 × 10⁻⁶. `run_point` lowers EPS as e^{−2(ℓ−8)} above ℓ = 8.

### The two mode equations (`out/stage2_modes.txt`)

- **The scheme's soft tail is there, but it carries almost no energy.**
  - On the one-loop side (ℓ = 4.9 and 2.0), the lowest modes have n_k k² = (0.99–1.01) S²/4 with S = ∫V''dt, the
    note's infrared artefact.
  - They carry 2 × 10⁻⁵ and 5 × 10⁻³ of the quanta's energy below m_turn. Halving k_min moves the fraction by
    ≤ 5.5 × 10⁻⁴ there (V4).
  - In the gap (#183, #430) the tail is absent (0.002 of S²/4).
- **The linearized equation is not usable here.** I took it as the Hartree term at fixed Σ, K V''''Σ added to the
  modes' ω².
  - On the scheme's own trajectories that shift is 44–85 k_turn² (10²–10⁶ m_turn²) at the turn, the note's
    "8 V_D/E".
  - With it in the modes, the run diverges at ℓ = 4.9, 2.0 and gap entry: energy errors of 10¹⁴–10¹⁹ that grow, not
    shrink, as the step is halved, or the field reaches φ ≤ 0.
  - At #430, where the shift is negative and small, it runs, but breaks the energy budget by a factor 20.
  - A consistent linearization needs the renormalized Hartree mass and the response of Σ, which the note says is
    non-local and not of oscillator form. Neither is constructed here.
  - So the comparison the brief asked for cannot be made. The soft-mode question is answered within the scheme
    instead, by the tail's energy share and the k_min test.

### The scan (`out/stage2_scan.txt`)

**2197 (β = 1.185, laboratory M).** The A = 0.05 and A = 0.01 maps lie on the same curve and give the same fraction
at the same ℓ (0.575 and 0.577 at ℓ = 3.0; 0.717 and 0.714 at 2.0). "Back out" means the field passed V'' = 10⁻⁶ m²
on the way out within four inbound times. Where it did not, frac includes dressing not yet returned, and 1 − frac
(0.03) is what the background holds.

| bounce | ℓ | R·S(L) | stage 2 | `born.py` (sc) | 2.7/ℓ | v_out/v_in | back out |
|---|---|---|---|---|---|---|---|
| A0.05 #5 | 9.54 | 10¹⁹ | 0.249 | 0.259 | 0.283 | 0.87 | yes |
| A0.05 #11 | 7.40 | 10¹⁶ | 0.306 | 0.322 | 0.365 | 0.83 | yes |
| A0.05 #35 | 4.91 | 10¹² | 0.417 | 0.447 | 0.549 | 0.76 | yes |
| A0.05 #60 | 3.68 | 10¹⁰ | 0.507 | 0.552 | 0.733 | 0.70 | yes |
| A0.05 #78 | 3.01 | 10⁹ | 0.575 | 0.634 | 0.898 | 0.65 | yes |
| A0.05 #93 | 2.49 | 3 × 10⁸ | 0.640 | 0.713 | 1.08 | 0.60 | yes |
| A0.05 #107 | 1.99 | 4 × 10⁷ | 0.717 | 0.808 | 1.36 | 0.53 | yes |
| A0.05 #119 | 1.49 | 5 × 10⁶ | 0.811 | 0.919 | 1.82 | 0.43 | yes |
| A0.05 #128 | 0.98 | 6 × 10⁵ | **0.921** | fails | 2.76 | 0.28 | yes |
| A0.05 #133 | 0.52 | 8 × 10⁴ | **0.973** | — | 5.17 | 0.16 | no |
| A0.01 #175 | 0.49 | 7 × 10⁴ | **0.966** | — | 5.56 | 0.18 | no |
| A0.01 #180 | 0.27 | 2 × 10⁴ | **0.918** | — | 10.1 | 0.29 | no |
| A0.01 #182 | 0.083 | 10⁴ | 0.878 | — | 32.8 | 0.35 | yes |
| A0.01 #183, gap entry | — | 4.2 × 10³ | 0.826 | — | — | 0.42 | yes |
| A0.01 #250, gap | — | 8.2 × 10² | 0.773 | — | — | 0.48 | yes |
| A0.01 #348, gap | — | 70 | 0.522 | — | — | 0.69 | yes |
| A0.01 #430, gap | — | 8.6 | 0.282 | — | — | 0.85 | yes |
| A0.01 #512, gap | — | 1.0 | 0.159 | — | — | 0.92 | yes |
| A0.01 #600, gap | — | 0.10 | 0.059 | — | — | 0.97 | yes |
| A0.01 #690, gap (R = 1) | — | 9.4 × 10⁻³ | 0.0027 | — | — | 0.999 | yes |

**R-865 (β = 2), at A = 0.05.**
- Its ℓ_min is 17.75 (`out/cascade-R-865-A0.05.txt`, 40 one-loop candidates). The brief's 8.5 must be its minimum
  over A.
- At ℓ = 33.4, 21.4 and 17.75 stage 2 gives 0.080, 0.122 and 0.145, against `born.py`'s 0.081, 0.124 and 0.148 and
  the law's 0.081, 0.126 and 0.152.
- So nothing changes at β = 2 at this A.

**The grid.** Fraction of E. Asterisks mark points where the field had not come back out. In brackets is ℓ where the
chain exists.

| L \ ln R | 5 | 10 | 15 | 20 | 25 |
|---|---|---|---|---|---|
| 1.3 | 0.159 | 0.528 | 0.786 | 0.816 | 0.329 |
| 2 | 0.176 | 0.566 | 0.808 | 0.962* | 0.608 |
| 3 | 0.203 | 0.644 | 0.839 | 0.975* | 0.820 |
| 4 | 0.230 | 0.709 | 0.880 (0.08) | 0.959* (0.27) | 0.833 (0.22) |
| 6 | 0.268 | 0.780 | 0.954* (0.76) | 0.888 (1.12) | 0.758 (1.27) |
| 8 | 0.304 | 0.809 (0.37) | 0.986* (1.17) | 0.821 (1.73) | 0.687 (2.09) |

**The fraction is not a function of ℓ alone, nor of ℓ and R·S(L).**
- At ℓ ≈ 1.2 the grid gives 0.76–0.99, and at ℓ ≈ 0.25 it gives 0.83–0.96.
- The second variable is V₀/M⁴. On a history, L and R are tied by
  ln R = ln(V₀/M⁴) + 2L + 2 ln(L⁴ + 2L³) − ln(e^L − 1) − ln 8π².
  - Every cascade point above, for both A and both β, has ln(V₀/M⁴) = 3.345 ± 0.005 (R-865's first bounce 3.39).
  - The grid points nearest that curve reproduce the cascade: L = 4, ln R = 15 (3.45) gives 0.880 against 0.878.
  - V₀/M⁴ is the note's loop scale: ε = (y⁸e^y/16π²)(V₀/M⁴).
- **So at fixed M and V₀ the fraction is one function of ℓ (or L), the same for every β and A.** A history at another M
  sits on another curve.

**The loop parameter.** ε = V''''/16π² at the turn is 1.2 × 10² to 5 × 10⁸ on 2197's one-loop rows, 20–160 in the gap,
2.5 at R = 1, and 10¹⁵–10²⁸ at β = 2. The Hartree shift at the turn is 23–90 k_turn² on every one-loop row. Stage 2
is therefore the scheme's answer, and like the Born table it lies outside the scheme's own domain, as the note says.

### What stage 2 hands back

1. **≥ 90% and ≥ 99%.**
   - On the laboratory-M curve a single bounce absorbs ≥ 90% from ℓ ≈ 1.1 down to ℓ ≈ 0.18. That is L ≈ 5.9 to 4.2,
     ln R ≈ 19.7 to 15.3, interpolating between rows.
   - The largest is 0.97 at ℓ ≈ 0.5. There, and from ℓ ≈ 0.5 to 0.27, the field had not come back out at the end of
     the run, and 0.03 of E was still in the background.
   - **No point reaches 99%.** The scan's largest is 0.986, at a grid point off the laboratory curve.
   - The one-loop law's α_cap = 0.99 is therefore never reached by the measured fraction.
   - Robustness: every convergence variant moves these by ≤ 2 × 10⁻³. They are the scheme's numbers at ε ~ 10², not
     a result outside it.
2. **From ℓ ≈ 1 to R·S ≈ 1.** The fraction does not fall as ℓ → 0 within the chain.
   - It peaks at 0.97 near ℓ = 0.5, and is 0.88 at the chain's last bounce (ℓ = 0.083) and 0.83 at gap entry.
   - Through the gap it falls with R·S(L): 0.77 at 8 × 10², 0.52 at 70, 0.28 at 8.6 and 0.16 at R·S = 1.
   - The field comes back out at every gap point, with v_out/v_in rising from 0.42 to 0.92.
   - The nearly elastic, classical behaviour (fraction < 0.1, v_out/v_in > 0.95) takes over between R·S ≈ 1 and 0.1,
     about #512–#600: roughly the gap's last 90–180 bounces at A = 0.01.
3. **Against the references.**
   - Against 2.7/ℓ: stage 2 is 0.88 of it at ℓ = 9.5, 0.69 at 3.7, 0.53 at 2.0 and 0.33 at 1.0, and the law exceeds 1
     below ℓ = 2.7.
   - Against `born.py`: 0.88–0.96 in the overlap, ℓ = 9.5–1.5, falling as ℓ falls.
   - Against R·S(L): the fraction is far below R·S wherever R·S ≫ 1, because backreaction caps it.
   - It is also below R·S at R·S < 1 while R ≳ 1: 0.29 of it at R = 1. There the loop's dressing, of order R·E, already
     deforms the bounce. It reaches R·S only for R ≪ 1 (V6).
4. **For the gap prescription.** The user rules. The evidence on each candidate offered on 2026-11-04:
   - **`zero`** drains nothing where the scheme absorbs 0.83 (gap entry) down to 0.16 (R·S = 1). It also drains
     nothing on the gap's last bounces and the classical tail, where the scheme gives 0.06–0.003 and, for R ≪ 1,
     R·S(L). The scheme contradicts it on every gap bounce with R·S ≳ 0.1.
   - **`edge`** drains A·2.7/4 = 0.675·A at every gap bounce. At A = 1 that is within 0.2 of the scheme's
     0.83–0.52 over the gap's first half, but too much once R·S ≲ 10, and it has no fall-off toward the classical
     regime. At A < 1 it scales a number that has no small-A meaning in the gap.
   - **A "terminal" drain at the first gap candidate** (α_cap, ending the cascade) overstates that bounce: 0.83 is
     absorbed and the field comes back out at 0.42 of its speed. But it is close in effect. At 0.8 a bounce, three
     bounces leave under 1% of E, and the scheme gives 0.83–0.97 at every bounce from ℓ ≈ 1.2 to the gap entry. Its
     cost is that it also ends cascades whose remaining energy the scheme would leave to the nearly elastic regime.
   - **A measured function** fits the evidence directly, but it needs two variables (ℓ or L, and V₀/M⁴), or one per M.
     It replaces 2.7/ℓ everywhere below ℓ ≈ 10, where the law overstates by 12% at ℓ = 9.5 and by a factor 3 at
     ℓ = 1. It also replaces both gap prescriptions.
   - **Its interaction with A.**
     - min(α_cap, A·f) with the measured f ≤ 0.97 never saturates for A ≤ 1.
     - The law's min(α_cap, A·2.7/ℓ) saturates at ℓ ≈ 2.7A: inside the chain for A ≳ 0.4, and at ℓ ≈ 0.14 at
       A = 0.05.
     - With f, A would be a plain multiplier on a bounded fraction. A > 1 would then mean more than the scheme gives,
       capped at 0.99.

What is not settled by stage 2:
- The scheme's own validity (ε ≫ 1 throughout).
- The fraction on curves at other M; the scan covers only the laboratory curve, and the grid's off-curve points.
- The cascade's fate after the first strongly inelastic bounce, which needs the map re-run under whichever law is
  ruled.

## The measured law on the cascades (2026-11-04)

Asked by the user: with the fraction ≈ 0.9 from ℓ = 1 down to gap entry, how much energy can be left when the
condensate exits the gap, and will it not already have parked?

**Energy.** On the laboratory-M curve gap exit (R = 1) is E ≈ V₀ (L = 0.68) whatever the prescription, since the
curve is fixed by V₀/M⁴. At ℓ = 1, E ≈ 300 V₀, so the field leaves the gap with about 0.3% of its ℓ = 1 energy. The
prescription decides how fast it gets there and where the energy goes, not how much is left.

**Parking is not set by V₀.** The code parks at a candidate with ρ_kin/ρ_R < 10⁻⁶ (`PARKING_EPSILON`), so it is a
race against radiation, and the drains are only part of it. Summed −ln(1 − α) at parking is 0.24 for R-865 at A = 0.05
and 7.1 for 2197 at A = 0.05.

**The run.** `measured_law_map.py` runs `cascade_map.py` with the drain α_eff = min(0.99, A f)·w_gate, where f is:
- one-loop, ℓ ≥ 1: 1 − exp(−2.69/(ℓ + 0.12)), the fit to stage 2's curve rows (within 0.007 from ℓ = 1 to 33);
- one-loop, ℓ < 1: 0.9;
- gap: stage 2's gap rows, interpolated in ln(R·S(L));
- classical: R·S(L)·g(ln R), with g from V6.

Each history keeps its old-law tuned normalization, and nothing is re-tuned (R-632 at A = 0.05 borrows A = 0.5's).
Outputs: `out/measured-*.txt`.

| history | A | measured law | old law (02, 02b) |
|---|---|---|---|
| 2197 (β = 1.185) | 1 | parks at N = 31.99 after 6 candidates, ℓ_min = 11.8 | parks at N = 33.12, 10 candidates, ℓ_min = 10.2 |
| R-632 (β = 1.0) | 1 | 17 drains down to ℓ = 1.55, then one at ℓ = 0.79 (f = 0.9); parks at N = 37.88 after it | parks at N = 37.61, 13 candidates, ℓ_min = 0.47 |
| 2197 | 0.05 | 191 one-loop and 14 gap candidates; parks at N = 39.47 at the 14th gap bounce (L = 3.39) | parks at N = 39.09 at ℓ = 0.52 |
| R-632 | 0.05 | 43 one-loop, 51 gap and 792 classical candidates; **does not park** by N_final = 45.08 (L = 6 × 10⁻⁷ at the end) | tuning fails (02b) |

**What this shows.**
- **At A = 1 neither history reaches the gap.**
  - 2197 parks while ℓ ≈ 12.
  - β = 1.0 does come down to the plateau, and parks right after its first plateau drain.
- **The gap matters only at small A.**
  - At A = 0.05, 2197 parks inside the gap's first bounces.
  - R-632 still crosses the whole gap and the classical tail unparked. Every drain there is scaled by A = 0.05, so a
    gap bounce drains at most 0.04.
  - So the small-A failure is a property of small A, not of the gap prescription.
- These are single integrations at old-law normalizations, a guide to the behaviour and not a re-tuned measurement.

## Other M: near M_crit and at 10⁻³ M_P (2026-11-04)

Asked by the user, after the laboratory-calibrated law was found to read only ℓ, L and R: stage-2 runs for M near
M_crit for surfing at the QCD temperature, and for M near 10⁻³ M_P.

**The curves.** On a history, ln R = ln(V₀/M⁴) + 2L + 2 ln(L⁴ + 2L³) − ln(e^L − 1) − ln 8π². The laboratory curve has
ln(V₀/M⁴) = 3.3.
- **M_crit** (where the one-loop stopping point meets the classical wall; about 1 TeV ≈ 4 × 10⁻¹⁶ M_P for QCD
  surfing, `wall_backreaction.tex` §"Where the field is stopped") lies inside the β = 2 ladder. The A = 1 maps
  (`out/cascade-L-M*-A1.txt`, the old law) show:

  | M (M_P) | ln(V₀/M⁴) | one-loop bounces | first gap bounce |
  |---|---|---|---|
  | 8 × 10⁻¹⁸ | −119.2 | 3, at ℓ = 4.1, 2.9 and 1.9 | ln R = 6.7, then 157 more gap bounces |
  | 10⁻¹⁶ | −129.3 | 1, at ℓ = 1.39 | then 9 gap bounces |
  | 5 × 10⁻¹⁶ | −135.7 | none | the first impact (ln R = 3.3); classical after it |

  Every bounce there is at L ≈ 75–106, far outside stage 2's earlier grid (L ≤ 8).
- **10⁻³ M_P** has ln(V₀/M⁴) = −249, so every bounce has R ~ e⁻⁸⁰ or less. No history exists at that M; four points on
  its curve were run.

**Results** (`out/stage2_mscan.txt`, energy conserved to ≤ 5 × 10⁻⁷).
- **One-loop bounces near M_crit absorb more than the laboratory law gives.** The excess is 0.03 at ℓ = 9 and grows
  to 0.25 at ℓ = 2:
  - 0.29, 0.37, 0.52, 0.66 and 0.88 at ℓ = 8.9, 6.8, 4.6, 3.5 and 2.4 (the 8 × 10⁻¹⁸ curve);
  - 0.97–0.98 at ℓ ≈ 1.9–2.0 (field not yet out);
  - 0.80 at ℓ = 0.69.
  - Both curves give the same f(ℓ).
- **The gap is very different from the laboratory gap at the same R·S.** At L ≈ 90–106, S(L) ~ 10⁻⁵, so R·S is
  10⁻⁵–10⁻². Yet the fraction is still large:

  | ln R | 6.7 | 5.6 | 4.7 | 4.0 | 3.2 | 2.4 | 1.3 | 0.03 |
  |---|---|---|---|---|---|---|---|---|
  | f (8 × 10⁻¹⁸ curve) | 0.73 | 0.57 | 0.45 | 0.34 | 0.19 | 0.06 | 0.005 | 10⁻⁴ |

  The 10⁻¹⁶ and 5 × 10⁻¹⁶ curves agree at the same ln R. The laboratory law, interpolated in ln(R·S), gives 0.003 at
  every one of these points. **R·S is the wrong variable in the gap.**
- **The classical regime.**
  - Near M_crit, frac/(R·S) is 2–4 at R ≈ 1, dips to 0.1 at ln R ≈ −1, and returns towards 1 by ln R = −3. The
    fraction there is ≤ 10⁻⁴.
  - At 10⁻³ M_P it is the test field exactly: frac/(R·S) = 1.000 ± 0.003, with fractions of 10⁻⁴⁰ to 10⁻⁹⁵.

**A law that covers every curve** (`fit_law.py`, `out/fit_law.txt`). It uses only ℓ, L = M/φ_t and R, which the code
has at each candidate. M enters only through them.
- **One-loop, ℓ ≥ 1:**

      f = 1 − exp[−2.69 / (ℓ + 0.97 − 0.41 ln L)]

  - The coefficient is held at its Born value 2.69; the shift is fitted.
  - It is within 0.074 (rms 0.021) of all 27 points where the field came back out: the laboratory curve, the grid,
    R-865 to ℓ = 33, and the curves near M_crit.
  - The laboratory-only form, 2.69/(ℓ + 0.12), is off by up to 0.125 on the same set.
  - At L ≈ 100 and ℓ ≲ 2 the denominator approaches 0, so a cap of about 0.95 is needed. The measured values there
    are 0.88–0.98.
- **One-loop, ℓ < 1:** f ≈ 0.9 (measured 0.80–0.97 on both kinds of curve).
- **Gap** (R > 1, no chain):

      y = 0.020 (ln R − 1.56)^1.55 (1 + 0.84 ln L),  f = 1 − e^{−y}

  It holds for ln R > 1.56, and is 0 below, where the measured fraction is ≤ 0.06. It is within 0.062 (rms 0.024)
  of all 34 gap points, on the laboratory curve, the grid (L ≤ 8, ln R ≤ 15) and near M_crit.
- **Classical (R ≤ 1):** f ≈ R·S(L). This is ≤ 0.003 at the laboratory curve's R = 1 and ≤ 10⁻⁴ near M_crit, and is
  exact for R ≪ 1.

The cap, the plateau and the classical piece are judgement, not fits. They are where the user asked for qualitative
behaviour only.

## The adopted law at small A (2026-11-05)

The user adopted, for the drain:
- the ℓ ≥ 1 law with its ln L shift;
- the 0.9 plateau below ℓ = 1, down to the gap;
- `fit_law.py`'s gap function;
- R·S(L) at R ≤ 1.

A = 10 stays the top of the range. At A = 10, min(0.99, 10 f) is capped for every bounce with f ≥ 0.1, that is ℓ ≲ 26,
under either law. The question was whether A = 0.05 still works at the bottom.

**The run.** `measured_law_map.py --law=adopted` was run on every history of 02b's A = 0.05 set, at each one's own
old-law normalization; R-632 and the 5 × 10⁻²⁰ M_P ladder, whose tunings failed, used their A = 0.5 normalization.
Nothing is re-tuned, so parking here is a proxy for the tuning succeeding. Outputs: `out/adopted-A0.05/`,
`out/adopted-floor/`.

**At A = 0.05.**
- **Every history that tuned under the old law still parks.** The P and W transition-window histories park
  0.3–0.4 e-folds later than before (N = 38.4–39.4 against 38.1–39.0), mostly inside the gap's first 5–22 bounces.
  The B, R-611, R-865, R-1193 and ladder histories park much as before.
- **The two that failed under the old law still fail.**
  - R-632 (β = 1.0) meets 43 one-loop, 52 gap and 800 classical candidates without parking.
  - The 5 × 10⁻²⁰ M_P ladder exhausts the step budget at N = 46.07 after 14 115 reflections.
- The spot history C-3233 (10⁻¹² M_P) does not park under either law, as before.

**Their floor.**

| history | A = 0.1 | A = 0.2 | A = 0.5 |
|---|---|---|---|
| R-632 (β = 1.0) | does not park (3269 classical candidates) | step budget at N = 44.1 | parks at N = 38.65, after 3 gap bounces |
| 5 × 10⁻²⁰ M_P ladder | step budget at N = 47.6 | parks at N = 30.71 | parks at N = 26.33 |

**Why small A is harder now.** Under the old law small A still parked fields through the divergence of 2.7/ℓ: at
A = 0.05 it drained 0.27 at ℓ = 0.5 and hit the cap near ℓ → 0. Under the adopted law no bounce at A = 0.05 drains
more than 0.045. The fields that parked did so on many weak drains, plus the gap's.
