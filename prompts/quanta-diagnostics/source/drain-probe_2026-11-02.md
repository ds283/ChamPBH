# The drain question: one-loop drain, multi-bounce effects, and what is established

**2026-11-02**, a Claude Code session (not a campaign prompt), continuing from
[`margins-correction_2026-11-02.md`](margins-correction_2026-11-02.md) at the user's questions. Nothing
in the code or a store changed. The probes ran on a scratch copy of the tree. The whole argument, with
its equations and tables, is written up as the briefing note
`BriefingNotes/nonrelativistic_quanta.tex` in the repository *Chameleon PBH (David's stuff)*. This
record keeps the session's steps, the probe's method and results, and the decisions it leaves open.

## 1. How the argument went

1. **Q falls, so later failures are birth failures** (margins-correction §1). The user asked whether
   a·m_eff and a·k_ex should be tracked. Both already are, per bounce: `log_q` and `log_m` are in P7's
   bounce table, so ln(a m_j) = `log_m` + N_j needs no new column, and U5 gives the sampled m_eff. In
   2197, a·m at the bounces falls by about 30 e-folds (ln Q minus the birth margin, from about −25.5 to
   about −55). This is one history's measurement, not a guarantee, and monotonicity was not checked.
2. **Two thresholds, not one.** Generation g is non-relativistic at bounce k if
   ln(Q_g/Q_k) < ln(m_k/k_ex,k), which is 15–20 e-folds wide in 2197's tail. It is disturbed by bounce k
   (rescattering, stimulated emission) only if ln(Q_g/Q_k) ≲ O(1). The user's condition for being
   undisturbed, k_ex1 ≫ k_ex2 with comoving k_ex, is the second threshold. A quantum that is NR at the
   wall but outside the band keeps its occupation: the mode is adiabatic, since |ω̇/ω²| ≤ k_ex/p. What
   remains is (a) the equation of state, which is measure 2's E, and (b) a backreaction term n_χ m(φ) on
   the field. The code doesn't include (b), and it hasn't been measured.
3. **What R is.** The code's R = ω_t⁴/8π²E (`AbstractModel.classical_turning_point`) is
   `production_energy_ratio.tex`'s ρ_sat/ρ_kin: the saturated single-bounce deposition against the
   impact energy. The code warns at R < 1 that the reflection is "nearly elastic". One bounce acting on
   vacuum gives |β|² ≲ O(1) in its band, which is energy of order k_ex⁴/2π², i.e. a fraction of about
   R e^{4ℓ} of E. The computed classical-wall value is R·S(L), with S ≈ 0.24/L². A fixed α far above
   that asks one bounce for n ≫ 1. The large classical occupations in the stored histories are
   therefore a property of the fixed-α drain.
4. **A one-loop drain.** `wall_backreaction.tex` ("The chain of estimates", "The inelastic fraction")
   gives the fraction lost at a one-loop reflection as ρ_prod/ρ_kin ≈ 4c/ℓ ≈ 2.7/ℓ, with
   ℓ ≈ ln(T*/M) + 7. The user proposed scaling the one-loop prediction by a factor A and scanning A, with
   R ≲ 1 drained at "almost nothing".
5. **Multi-bounce effects.** The user asked whether parametric resonance would mean computing many
   Floquet exponents. It doesn't. A train of bounces is a product of 2×2 transfer matrices, with
   (α_k, β_k) per bounce and a free phase θ_k between bounces. The random-phase limit is the
   stimulated-emission recursion n_{j+1} = n_j + |β_j|²(1 + 2n_j). The locked-phase limit is resonance.
   Resonance needs the phase slip per bounce to stay at or below about |β|² ≲ 1 for many bounces, and
   the slip is about (k_ex/H)·ΔN². It was measured (§3).

## 2. The probe

- **Tree.** `git archive` of `reflection-restart` at `2d50b95` (its prompt 03, F3 and the guard), patched
  by [`probe/drain/one_loop_drain.patch`](probe/drain/one_loop_drain.patch). With `PROBE_DRAIN_A` set,
  each candidate's α_eff is replaced by:
  - min(0.99, A·2.7/ℓ) where ln R > 0 and `one_loop_turnaround` returns a solution;
  - 0 where ln R ≤ 0, and 0 for an unassessed candidate;
  - in the gap (ln R > 0, no solution), 0, or A·2.7/4 with `PROBE_GAP=high`.

  Unset, the code is unchanged and runs at the stored α.
- **Driver.** [`probe/drain/drain_probe.py`](probe/drain/drain_probe.py) re-integrates one 2026.8.0
  history at its stored normalization (no tuning, no Ray, no store writes) and writes a JSON summary,
  including each candidate's N, H, ln k_ex, ln R, ln ω_t² and α_eff. Run from the scratch tree's root:
  `PROBE_DRAIN_A=1 PYTHONPATH=. python -u drain_probe.py SERIAL LABEL OUTDIR`.
  [`probe/drain/phase_slip.py`](probe/drain/phase_slip.py) computes §3's phase slips from those
  summaries.
- **Results** are in [`probe/drain/results/`](probe/drain/results/). Each run takes 2–40 s, except the
  stored-α laboratory-M runs. Those failed after about 115 s (§5), so the stored-α rows for 2197 and
  2188 below are the store's own values.

## 3. Results

**Laboratory M** (4×10⁻³¹ M_P; 2197: β 1.185, α 0.001; 2188: β 1.06, α 0.001):

| history | drain | drains | parks at N | r_χ today | ΔN_eff today | wall |
|---|---|---|---|---|---|---|
| 2197 | stored, α = 10⁻³ | 6,648 | 45.16 | 8.7×10⁻⁴ | 0.0064 | 624 s |
| 2197 | one-loop, A = 1 | 10 | 33.12 | 8.2×10⁻³ | 0.061 | 2 s |
| 2197 | one-loop, A = 0.1 | 95 | 38.45 | 1.5×10⁻³ | 0.011 | 9 s |
| 2188 | stored, α = 10⁻³ | 5,741 | 43.81 | 3.2×10⁻⁴ | 0.0024 | 653 s |
| 2188 | one-loop, A = 1 | 15 | 34.79 | 7.6×10⁻⁴ | 0.0057 | 2 s |
| 2188 | one-loop, A = 0.1 | 92 | 37.45 | 3.6×10⁻⁴ | 0.0026 | 8 s |

Every drain is one-loop: ℓ runs from 32.3 to 10.2 at A = 1 in 2197 (α_eff 0.084–0.26), and down to 1.8 at
A = 0.1. No candidate reaches R ≤ 1 or the gap, and the gap-high variant is identical to A = 1. The
thousands of late bounces come from the small α. r_χ rises because the early drains are 10–300 times
10⁻³.

**Large M** (10⁻¹² M_P, R < 1 at the first impact, so the one-loop drain is zero throughout):

| history | drain | bounces | parks at N | r_χ today | ΔN_eff today |
|---|---|---|---|---|---|
| 3016 (β 4.0) | stored, α = 0.1 | 65 | 31.26 | 1.1×10⁻³ | 0.0079 |
| 3016 | one-loop (zero) | 330 | 40.43 | 1.0×10⁻⁶ (parking only) | 7×10⁻⁶ |
| 3233 (β 1.0) | stored, α = 0.15 | 69 | 39.16 | 4.5×10⁻² | 0.335 |
| 3233 | one-loop (zero) | 301 | does not park | 0 | 0 |
| 3873 (β 1.11) | stored, α = 0.03 | 153 | 37.52 | 3.1×10⁻³ | 0.023 |
| 3873 | one-loop (zero) | — | fails at N = 43.76 (§5) | — | — |

The stored-α reruns reproduce the stored r_χ.

**Phase slip per bounce** (`phase_slip.py`; massless phase (Q/a)Δt at each bounce's own Q, which
understates the true slip):

| run | bounces | min k_ex/H | slip min / median (rad) |
|---|---|---|---|
| 2197 A = 1 / A = 0.1 | 10 / 95 | 2×10³⁰ / 8×10²⁸ | 10²⁸ / 5×10³⁰; 8×10²⁴ / 10²⁷ |
| 2188 A = 1 / A = 0.1 | 15 / 92 | 10³⁰ / 10²⁹ | 5×10²⁷ / 7×10²⁸; 10²⁵ / 8×10²⁶ |
| 3016 stored / zero | 65 / 330 | 8×10¹² / 3×10¹² | 10⁹ / 3×10¹⁰; 5×10⁶ / 4×10⁸ |
| 3233 stored / zero | 69 / 301 | 7×10⁸ / 0.08 | 3×10⁴ / 10⁸; 5×10⁻⁷ / 0.05 |
| 3873 stored | 153 | 6×10¹⁰ | 2×10⁶ / 4×10⁷ |

The only slips below a radian are 172 turnarounds of 3233 without a drain. All are at N > 43.2, with
ln R ≤ −192 and k_ex/ω_t ≤ e^{−5.7}, and 28 of them have k_ex < H. Those turnarounds are adiabatic for
every mode, so |β|² is exponentially small there. Parametric resonance is excluded wherever production
is non-negligible.

## 4. What is established and what is only possible

**Measured:**
- In 2197, Q falls almost monotonically and a·m at the bounces falls by about 30 e-folds. Later bounces
  cannot make earlier quanta NR, so a later failure is a birth failure.
- In the stored laboratory-M histories at α = 10⁻³, most of ρ_χ is born late, in the classical tail,
  NR at the wall and with n ≫ 1 at birth. This is a defect of the fixed-α drain.
- The equation-of-state effect is small, at most 4×10⁻⁹ of ρ_R in 2197. This uses the sampled mass, not
  the turning-point mass.
- Under the one-loop drain at laboratory M (two histories, not re-tuned), the field parks within 10–95
  one-loop bounces, r_χ rises 1–10×, and 2197's ΔN_eff reaches 0.06 at A = 1.
- At large M the one-loop drain is zero and ΔN_eff vanishes.
- No parametric resonance in any train that produces anything. The random-phase recursion is the
  complete multi-bounce model, and it is negligible under the one-loop drain (n ~ αe^{−4ℓ} ≪ 1).
- Two integrator failures on the `reflection-restart` 03 tree (§5).

**Possible, not established:**
- That the one-loop drain ends the cascade before R ≤ 1 across the grid at physical M, and for A < 0.1.
- That the drain at R < 1 is "almost nothing" in the tail. S(L) was computed for steep walls only.
- The crossover across the gap 1 < R < 10^2.9. There the one-loop fraction is O(1) (ℓ is a few at the
  gap's edge) and the classical one is about 10⁻⁵ R, and neither holds inside the gap. The probes never
  reached it.
- That parking after about ten bounces is right. That is the parking model's call, open at physical M.
- The backreaction n_χ m(φ) of quanta that are NR at the wall.
- Stimulated emission in a late tail. It is relevant only if a fixed α is kept.

## 5. The integrator failures

Both are on `2d50b95`, at the stored normalization. Neither depends on the drain prescription. Neither
was investigated.
- **2197 and 2188 at the stored α = 10⁻³** fail after about 115 s in `classical_turning_point`, at
  ln E = −306.7, phi ≈ 2.9×10⁻⁷ M_P. The search reports a residual that is not monotone, alternating
  between −inf and repeated values at neighbouring probes. That looks like float cancellation in
  V − V_floor. Both completed on `0209aac` (margins-correction). Logs:
  `probe/drain/results/2197-base-stored-alpha.log`, `2188-base-stored-alpha.log`.
- **3873 without a drain** stops at N = 43.76 with "reflection requested inside the wall"
  (W/(π²/2) = 1.485), the guard `reflection-restart` 03 added. It passes at its stored α. Log:
  `probe/drain/results/3873-A1.log`.

Under the one-loop drain, 2197 and 2188 complete on the same tree.

## 6. Open for the user

None of these is ruled. The user said the campaign's scope is to be decided by what is right, not by the
plan's wording (2026-11-02).
- Whether the drain becomes α_eff = min(1, A·2.7/ℓ) with A scanned, replacing α's meaning.
- What drains at R ≤ 1: zero, or R·S(L) as a stated upper extrapolation.
- How to interpolate across the gap 1 < R < 10^2.9.
- Where the change lands: this campaign re-planned around it, or a campaign of its own. Either way it
  should land before the rebuild of record (U2).
- What measure 4 becomes (margins-correction, "Open for the user"). Under a one-loop drain, n̄_j and ε_j
  apply where n ≲ 1, which is everywhere the probes reached. The random-phase recursion is the complete
  multi-bounce model. A phase-slip check per bounce would document resonance's absence.
- Whether the two integrator failures go to `reflection-restart` 04.
