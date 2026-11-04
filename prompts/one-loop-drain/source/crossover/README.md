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
| `stage1a_classical.py` | S(L) from L = 1 to 134 |
| `rs_along_cascade.py` | R·S(L) at each bounce of a mapped cascade |
| `out/` | every output, text only |

Run from this directory with `../../../../venv/bin/python`, except `cascade_map.py`, which runs from the repository
root with `PYTHONPATH=.`.

## Stage 1a: the classical wall at small L (2026-11-04)

**Validation.** At L = 40, 70, 90, 110 and 134, S(L) equals `production_energy_ratio.tex` Table `tab:beta` to its quoted
digits (1.3936 × 10⁻⁴, 4.9353 × 10⁻⁵, 3.0600 × 10⁻⁵, 2.0810 × 10⁻⁵, 1.4204 × 10⁻⁵). At L = 70, k = 0.3 ω_t,
\|β\|² = 2.325252 × 10⁻⁴ against the note's 2.32525 × 10⁻⁴. A hundredfold stricter adiabatic start changes S in the
fifth digit at most. Output: [`out/stage1a_S_of_L.txt`](out/stage1a_S_of_L.txt).

**Result.** The wall stays nearly reflectionless down to L = 1: \|β\|²_max is 0.067 at L = 1, 0.022 at L = 5 and
0.008 at L = 10. S(L) is 8.7 × 10⁻³ at L = 1, 3.2 × 10⁻³ at L = 5 and 1.35 × 10⁻³ at L = 10. L²S falls from the note's
0.22–0.26 at L ≥ 40 to 0.08 at L = 5.

## Where the cascade's bounces sit

`out/cascade-P-2197-A0.05.txt` is 2197 (β = 1.185, laboratory M) at A = 0.05, gap `zero`, at 02b's tuning.
- As ℓ falls from 7.4 to 0.52 over the cascade's last hundred bounces, L falls from 21 to 4.8 and ln R from 44 to 17.
- E/V0 stays ≫ 1, so these bounces are on the exponential wall, not in its 1/φ tail.
- R·S(L) (`out/rs-P-2197-A0.05.txt`) is 7.8 × 10⁴ at the last bounce and larger before it.
