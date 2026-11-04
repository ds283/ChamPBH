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
| `out/` | every output, text only |

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
