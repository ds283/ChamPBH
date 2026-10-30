# Probes for prompts 07b and 08, 2026-10-30

Read-only. The orchestrator ran these at `0b8f420` (prompt 07 landed and reviewed) at the user's word
("Please write prompt 08"). Nothing is stored, no Ray is started and nothing in the tree is changed. Every
history uses `test_injection`'s setup at α = 0.1: n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB,
default tolerances, the model's seed realization, untuned. The numbers are measurements; no threshold was set
in advance. Load 13–50 throughout.

All commands run from the repository root. Pickles go to `$PROBE_OUT`, or the system's temporary directory if
it is not set.

## 0. The peak and the margins (`peaks.py`), for 08

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-30/peaks.py BETA M [ALPHA]

One history through the shipped code, then a post-processing of its records. This is a probe of one reading
of README §8.4's charter for 08, not the design:
- **The generations** are the candidates with ΔK > 0. A generation's share is ΔK_j/K(N_final).
- **The estimate** comes from the candidate's own assessment (`classical_turning_point`, from
  `phi_assessed`, `log_E_assessed`):
  - ln 𝓡 ≤ 0: the classical one, k_ex and ω_t;
  - ln 𝓡 > 0: the one-loop one, `one_loop_turnaround`, k_ex and m_ta. A raise or a `None` falls back to
    the classical one and counts as "gap".
- **ln q** = ln k_ex + N_turn: comoving, born at the turnaround.
- **Three margins:**
  - **birth**: ln k_ex − ln m_ta (ℓ, or ℓ_t);
  - **reflection**: at every later generation's turnaround N_k, ln q − N_k − ln m_ta,k;
  - **tracker**: at every stored sample after the birth, and at N_final, ln q − N − ln m_eff(φ_min), with
    φ_min the tracker on that sample's trace.
- **Worst** is over the generations with share ≥ 10⁻³ (U9). "Failing share" is the share with margin
  < ln 10.

**`peaks.out`**: M = 0.5 at β = 2, 5, 10, 25; M = 10⁻³ at β = 1.2, 2; β = 2 at M = 10⁻⁷, 10⁻¹⁰, 10⁻¹²,
3×10⁻¹³, 10⁻²⁸, 10⁻³⁰; β = 3 at M = 10⁻³⁰; β = 1.2 at 4×10⁻³¹. What it shows:
- **M ≥ 3×10⁻¹³: every generation is classical** (ln 𝓡 ≤ −9.7 decades). The birth margin is ℓ_t =
  −0.70 ± 0.01 on every generation, as R13 expects ("fires on every history by design"). The worst
  reflection margin is −0.67 to +0.005, and the worst tracker margin −0.29 to +14.6.
- **M ≤ 10⁻²⁸: every generation is one-loop** (ln 𝓡 ≥ 28 decades). ℓ is 14–33, and one-loop ln k_ex
  lies 1.5–2.6 below the classical one. All three margins are ≥ +16.8 on every weighted generation.
- **No generation fell in the gap** on these histories.
- **The leftover's share** of K (P19) is 5×10⁻⁵ to 2.3×10⁻³.
- **The cost** of the post-processing is 0.1–17 ms for the peaks (one-loop calls), and 0.13–0.79 s for the
  tracker on 5 000–17 000 samples.

## 1. The wall search at the rounding of ln V (`fixprobe.py`), for 07b

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-30/fixprobe.py BETA M K

K = 0 runs the shipped code. K > 0 patches `AbstractModel._log_V_minus_floor` to return −∞ where
ln V − ln V_floor ≤ K ulp(max(|ln V|, |ln V_floor|)), and the shipped value elsewhere.

- **`band_shipped.out`** (K = 0): **every history fails, at its first infall or soon after**:
  - β = 2 at M = 10⁻¹³, 10⁻¹⁴, 10⁻¹⁵, 10⁻¹⁶, 10⁻¹⁸, 10⁻²⁰, 10⁻²², 10⁻²⁵;
  - β = 1.2 and 3 at M = 10⁻¹⁶ and 10⁻²⁰.

  Each failure is `classical_turning_point`'s "not monotone", issue
  `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`, at a φ where M/φ ≈ 3×10⁻¹⁴, a few ulps of
  ln V ≈ −276.6. β = 2 at 3×10⁻¹³ and at 10⁻²⁸ runs (`peaks.out`). Log 05's and log 06's grids
  (10⁻⁷–10⁻¹², 10⁻³⁰) lie outside the band.
- **`band_fixed.out`** (K = 64): **all twelve reach T_CMB**. They park at candidates 6, 48 and 58 (β = 1.2,
  2, 3) at N = 28.364, 30.349 and 29.394, as every drained history of log 06 does. At β = 2, ln 𝓡 over the
  candidates spans −31.7 to −7.8 decades at 10⁻¹³, −19.7 to +4.2 at 10⁻¹⁶, and +16.3 to +40.2 at 10⁻²⁵.
  So the gap of `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (1 < 𝓡 ≲ 10^2.9) is crossed
  by histories near M = 10⁻¹⁵–10⁻¹⁷.
- **`bitwise.out`**: β = 2 at M = 10⁻¹² and 10⁻³⁰, β = 5 at 0.5 and β = 1.2 at 10⁻⁷, K = 0 against
  K = 64. They are equal in every record, the assessments included, and in the samples, the final state,
  `nfev`, `ts`, and the parking and release records.
