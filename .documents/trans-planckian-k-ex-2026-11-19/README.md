# k_ex on the inverse-power wall, and the trans-Planckian question — notes of 2026-11-19

Notes from a discussion with the user on 2026-11-19, outside any campaign, on the `power-law-wall` branch at
`f36ef59`. They record why `k_ex` at a reflection is far larger on the inverse-power wall than on the exponential, how
much the one-loop turnaround lowers it, and an evaluation of the proposal that trans-Planckian production makes black
holes, a matter component, rather than chameleon dark radiation. Nothing here changes the code, and no issue is
opened. The numbers come from the scripts in this folder; the data are the `07d-r2` rehearsal exports
(`var/power-law-wall/07d-r2/`, git-ignored, rehearsal stores and not a run of record) and the committed exports of the
2026.11.0 run (`science-runs/2026-11-13-science-2026.11.0/`, on `main`).

**In one paragraph.** `k_ex = ½ E^{1/2} c/φ`, with `c = φ|V'''/V''|` a dimensionless steepness. The exponential is the
steeper wall (c ≈ nL against n + 2), but that steepness is what stops the field early: φ_t is a power of
L = ln(1 + E/V₀) below M, against a power of E/V₀ itself on the inverse power. The φ factor wins by tens of e-folds. The
one-loop turnaround, set by V'' rather than V, softens this (exponent 1/(n+2) instead of 1/n) but does not remove it.
Whether trans-Planckian production makes black holes is a quantum-gravity question; no semiclassical argument decides
it. It does not matter for BBN, though: black holes of mass ~k_ex ≈ 10–74 M_P evaporate within ~10⁻³⁶ s, and if they
left stable remnants, the stored histories show those would exceed the dark-matter density by ~10⁵–10⁷.

Units: M_P is the reduced Planck mass throughout (`units.PlanckMass`), G = 1/(8π M_P²). E is the Einstein-frame
energy density that reaches the wall (`assess_infall`), mass dimension 4.

## 1. The classical turning point

From `classical_turning_point` (`CosmologyConcepts/Models/AbstractModel.py`):

```
k_ex = ½ E^{1/2} |V'''/V''|  at φ_t,   V(φ_t) − V₀ = E
     = (E^{1/2}/2M) · c(u_t) · u_t^{1/n},   u = (M/φ)ⁿ,  c = φ|V'''/V''|
```

**Dimensions.** √E has mass dimension 2 and V'''/V'' = d ln V''/dφ has −1, so k_ex is a mass. Physically,
√E = |φ̇|/√2, so k_ex = |d ln m²/dt|/(2√2) with m² = V'': the inverse duration of the mass pulse. φ̇ is the inbound
speed (it vanishes at φ_t itself).

| | exponential, V = V₀ e^u | inverse power, V = V₀(1 + u) |
|---|---|---|
| turning point | u_t = L = ln(1 + E/V₀) | u_t = E/V₀ |
| φ_t | M L^{−1/n} | M (V₀/E)^{1/n} |
| c | nu + 2(n+1) − n(n+1)/(nu + n + 1) ≈ nL | n + 2, exactly |
| k_ex | ≈ (E^{1/2}/2M) n L^{1+1/n} [1 + 2(n+1)/(nL)] | (E^{1/2}/2M)(n+2)(E/V₀)^{1/n} |

so at matched (E, V₀, M, n)

```
k^IPL/k^exp ≈ [(E/V₀)/L]^{1/n} × (n+2)/(nL + 2(n+1))
              (φ_t^exp/φ_t^IPL)    (c^IPL/c^exp, < 1)
```

[`out/wall_closed_forms.txt`](out/wall_closed_forms.txt), part 1: at ln(E/V₀) = 90 the φ part is +85.5 e-folds (n = 1)
and +42.8 (n = 2), the steepness part −3.4 and −3.8.

**Steeper is not sharper.** At fixed φ the exponential is far steeper, which is why the field stops at u = ln(E/V₀); the
inverse power grows only linearly in u and the field must go to u = E/V₀. The pulse length that k_ex measures,
ℓ = |V''/V'''| = φ_t/c, is M/(nL^{1+1/n}) on the exponential and M(V₀/E)^{1/n}/(n+2) on the inverse power: at its own
turning point the inverse-power wall is the much sharper one.

**Against ω_t.** The same φ_t sets ω_t² = V''(φ_t), so k_ex/ω_t is O(1) on both: c/(2√(n(n+1)u + n²u²)) → ½ on the
exponential (0.5164 at L = 90, n = 1), (n+2)/(2√(n(n+1))) on the inverse power (1.0607 at n = 1, 0.8165 at n = 2) at
every E/V₀, the `k_ex_over_omega_t` constants of `ComputeTargets/quanta_spectrum.py`.

## 2. The one-loop turnaround

From `one_loop_turnaround`: V''(φ_ta) = m_ta², m_ta⁴ = 32π² E/ℓ, ℓ = ln(k_ex/m_ta), k_ex = ½E^{1/2} c/φ_ta. The prefactor
is common, so k^IPL/k^exp = (c^IPL/c^exp)(φ_ta^exp/φ_ta^IPL) exactly at matched E; equivalently, since k_ex = m_ta e^ℓ,
ln(k^IPL/k^exp) = ℓ^IPL − ℓ^exp − ¼ ln(ℓ^IPL/ℓ^exp). With

```
Λ ≡ ln(m_ta² M²/V₀) = ln(E/V₀) − 2 ln(E^{1/4}/M) + ½ ln(32π²/ℓ)
```

- exponential: u_ta ≈ Λ − (2 + 2/n) ln Λ, φ_ta = M u_ta^{−1/n}, c ≈ n u_ta — φ_ta moves from φ_t only by a ratio of
  logarithms, so the one-loop turnaround sits almost on the classical point;
- inverse power: V'' = n(n+1)V₀Mⁿ/φ^{n+2} exactly, so φ_ta = M [n(n+1) e^{−Λ}]^{1/(n+2)}, c = n + 2.

Against the classical φ_t = M e^{−ln(E/V₀)/n} the exponent falls from 1/n to 1/(n+2) (V'' carries two more powers of
1/φ than V), and the large logarithm is Λ, smaller than ln(E/V₀) where M < E^{1/4}. Then

```
ln(k^IPL/k^exp) ≈ Λ/(n+2) − (1 + 1/n) ln Λ + O(1)
```

positive once Λ ≳ 15–20; Λ ≈ 50–80 at the test points. [`out/wall_closed_forms.txt`](out/wall_closed_forms.txt),
part 2, the production models at the wall tests' points: Δ ln k_ta = +7.8 to +25.3; at n = 1, M = 10⁻³⁰ M_P,
ln E = −180, Λ = 52.7 gives ln(M/φ_ta^IPL) = 17.35 against the code's 17.3, and the split +13.71 − 2.64 = 11.07 against
11.0 measured.

## 3. How much the one-loop turnaround lowers k_ex

On the inverse power c is n + 2 at both points and E is shared, so the shortfall is the ratio of field values:

```
ln(k_t/k_ta) = ln(φ_ta/φ_t) = ln(E/V₀)/n − [Λ − ln n(n+1)]/(n+2)
             ≈ ln(E/V₀) · (n+4)/(2n(n+2))      (M ~ V₀^{1/4}, so Λ ≈ ½ ln(E/V₀))
             = (5/6) ln(E/V₀) at n = 1,   (3/8) ln(E/V₀) at n = 2
```

[`out/one_loop_shortfall.txt`](out/one_loop_shortfall.txt), every one-loop bounce of the 07d-r2 exports; k_t is rebuilt
from the stored `log_E_Mp4` and `log_R`, which reproduces the stored k_ex of the 27 classical-regime bounces to
3×10⁻¹³:

| wall | n | β (M = 9.7×10⁻⁴ eV) | ln(k_t/k_ta) range | median |
|---|---|---|---|---|
| exponential | 1 | 1.2, 2, 2.9 | 2.0–2.7 | 2.2–2.7 |
| inverse power | 2 | 1.2 | 4.9–39 | 6.6 |
| inverse power | 2 | 2, 2.9 | 20–42 | ≈ 25 |
| inverse power | 1 | 1.2 | 9.6–86 | 13.5 |
| inverse power | 1 | 2, 2.9 | 32–91 | ≈ 54 |

The ratio of the n = 1 and n = 2 medians is 2.18–2.21 against the predicted 2.22, and the maxima (91, 41.5) both point
to ln(E/V₀) ≈ 110, the first kick. A shortfall of "10–30 e-folds" (4–13 powers of ten) describes n = 2 at β ≥ 2 and
n = 1 at β = 1.2 only. The shortfall tracks the bounce temperature: low-β histories reflect late, at small E/V₀.

## 4. L on the exponential wall

[`out/L_at_reflection.txt`](out/L_at_reflection.txt), the 1229 bounces of the 2026.11.0 exports (exponential, n = 1):
L runs from 3 to 127. By β: 97–127 at β = 10, 54–106 at β = 2, 20–102 at β = 1.2, 3–91 in the A = 0.05 relativistic
sample at β = 1.0775. With E ~ T⁴ and V₀^{1/4} ≈ 2.4 meV, L ≈ 4 ln(T/V₀^{1/4}); M enters only weakly. The L = 90
reference of the spectrum tables is typical of β = 2–10.

## 5. The black-hole proposal

The proposal: where k_ex > M_P, quantum gravity resolves the production, perhaps into a population of Planck-mass (or
heavier) black holes, a component redshifting as matter.

**How common.** The stored first-impact k_ex (one-loop) runs from ln(k_ex/M_P) = −20 to +4.3 on the inverse power; it is
positive for n = 1 at β ≥ 2 and for n = 2 only at M = 2.4 μeV. Every exponential first impact is 3–42 e-folds below M_P.

**Whether a black hole forms.** The criterion is not "kinetic energy or not" but a packet's invariant mass, its
compactness and its motion.

1. *A single on-shell quantum* is never a black hole: its E_cm is its invariant mass m ≪ M_P, k is its energy in the
   cosmic frame, and a horizon is frame-independent. Its field is an Aichelburg–Sexl shock.
2. *A pair* (k, −k) has invariant mass 2k and r_s = k/(2πM_P²); it starts within its own r_s once k ≳ √(2π) M_P
   (ln(k/M_P) ≳ 0.92). But it flies apart at c: the time reverse of a black-hole-forming collision, with past-trapped,
   not future-trapped, surfaces (as a Hubble patch, which is always within its own r_s and does not collapse).
3. *A packet deposited at rest*, energy ~k_ex in a region ~1/k_ex, not as on-shell quanta (the user's reading): it lies
   within r_s = k_ex/(4πM_P²) once k_ex ≳ √(4π) M_P (ln(k_ex/M_P) ≳ 1.3, three of the first impacts). Time-symmetric data
   that compact contains an apparent horizon, so classical GR gives no reason against collapse. The on-shell assumption
   was doing real work.

What holds in every case: local conservation (∂_μT^{μν} = 0 for any matter) lets the condensate supply only about
E/k_ex³ ~ e^{−170} M_P within 1/k_ex in the pulse time, against ~k_ex needed, so a localised packet can only be a branch
of a quantum state whose ⟨T_μν⟩ stays homogeneous at ~e^{−173} M_P⁴, to which semiclassical gravity responds. And
energy k in a region 1/k is a Heisenberg-saturated, vacuum-fluctuation-sized object; whether it gravitates classically
at k > M_P is the spacetime-foam question. **The defensible statement: no semiclassical reason for collapse; quantum
gravity open.** Collisions between quanta of different pairs (converging, E_cm ~ 2k) are the classical channel; their
rate is Γ/H ~ (ρ_q/ρ)√(3ρ) k/(4πM_P³) ~ 10⁻³⁹ at the first impact.

**If they form.** m ~ k_ex (one quantum's worth; O(1) uncertain: the spectrum spreads in x = k/k_ex, not all of a
packet need end inside the horizon, a pair carries 2k). The three first impacts above threshold have
ln(k_ex/M_P) = 2.3, 4.3, 4.3, so m ≈ 10–74 M_P ≈ 4×10⁻⁵–3×10⁻⁴ g (M_P = 2.435×10¹⁸ GeV ≈ 4.3×10⁻⁶ g). With
τ = 5120π G² m³ = (80/π) m³/M_P⁴ and 1/M_P ≈ 2.7×10⁻⁴³ s, τ ≈ 7×10⁻³⁹ s at 10 M_P and 3×10⁻³⁶ s at 74 M_P (Hawking's
formula itself marginal there). Surviving to 1 s needs m ≳ 10⁹ g. So:

- *no remnant:* the energy returns as Hawking radiation at T_H ~ M_P/8π, shared democratically, mostly into the SM bath
  and gravitons — a different split between dark radiation and the bath, but no matter;
- *stable Planck remnants:* a matter component; the bound below applies.

**The matter component at BBN, if all the first impact's kinetic energy became matter.**
[`out/bh_matter.txt`](out/bh_matter.txt). Einstein frame throughout (black holes carry no scalar charge, so their
Einstein-frame mass is fixed): ρ_m,E(N) = ρ_kin,E(N₁) e^{−3(N−N₁)}, against ρ_R,E from `fields.csv`; the ratio is the
same in either frame. The first impact precedes any drain, so it is the same for every A and drain law.

| β | T₁ | r₁ = ρ_kin/ρ_R | ρ_m/ρ_R at 1 MeV, if all | at 0.1 MeV, if all | at 1 MeV, as stored (α_eff ρ_kin) | (ρ_m/s)/(ρ/s)_DM today |
|---|---|---|---|---|---|---|
| 1.2 | 0.23 GeV | 0.14 | 31 | 375 | 1.7–2.4 | 5.8×10⁷ |
| 2 | 0.75 GeV | 0.043 | 32 | 361 | 1.6–3.6 | 5.6×10⁷ |
| 2.9 | 1.9 GeV | 0.020 | 37 | 407 | 1.9–2.7 | 6.3×10⁷ |

- The same on both walls, at n = 1 and 2, and across M = 2.4 μeV–2.4 TeV: the first impact is set by the kick from the
  bath, not by the wall. The ratio is roughly r₁ T₁/T, and r₁T₁ ≈ 30 MeV at every β.
- At 1 MeV this acts on H like ΔN_eff ≈ 180–220 (ρ_m/ρ_R · g*_ρ/(7/4), T_ν = T_γ); the stored first-impact drain alone
  like ΔN_eff ~ 10. The stored dark radiation is ρ_χ/ρ_R = 0.004–0.01 at 1 MeV.
- With stable relics the binding limit is the dark-matter density, ρ_m/s ≤ 0.44 eV (Ω_c h² = 0.12): a fraction
  ε ≲ 2×10⁻⁸ of the first impact's energy. BBN alone (ρ_m/ρ_R ≲ 0.05 at 0.1 MeV) gives ε ≲ 10⁻⁴. A remnant of
  M_Pl ≈ 5 M_P per black hole of mass k_ex keeps a share 0.07–0.5; even with only the stored drain in packets the relics
  exceed the dark matter by ~10⁵–10⁶.

**Conclusion.** The assumption that decides the matter question is remnant stability, not whether the energy is
on-shell. Without stable remnants the drained energy ends as radiation either way; with them, the stored histories
exclude any such scenario unless almost no packets collapse. The generic UV outcomes — a wall softened so that k_ex is
capped near the cutoff, or heavy UV states that decay promptly — also leave radiation, which keeps the present
dark-radiation bookkeeping, with the spectrum above the cutoff marked unreliable.

## 6. The scripts

Run from the repository root of the `power-law-wall` worktree. None needs Ray or a store.

- [`wall_closed_forms.py`](wall_closed_forms.py) (`PYTHONPATH=.`; uses `InversePowerModel`):
  [`out/wall_closed_forms.txt`](out/wall_closed_forms.txt), §1–§2.
- [`one_loop_shortfall.py`](one_loop_shortfall.py), reads `var/power-law-wall/07d-r2`:
  [`out/one_loop_shortfall.txt`](out/one_loop_shortfall.txt), §3.
- [`L_at_reflection.py`](L_at_reflection.py) `[record dir]`, default the 2026.11.0 record on `main` (run here with the
  main checkout's path): [`out/L_at_reflection.txt`](out/L_at_reflection.txt), §4.
- [`bh_matter.py`](bh_matter.py), reads the A = 1 exports of `var/power-law-wall/07d-r2`:
  [`out/bh_matter.txt`](out/bh_matter.txt), §5.
