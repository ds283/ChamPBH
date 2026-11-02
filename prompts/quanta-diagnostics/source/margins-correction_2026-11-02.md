# The margins re-read: born non-relativistic, and two occupation regimes

**2026-11-02**, a Claude Code session (not a campaign prompt), at the user's questions. Read-only:
nothing in the code or a store changed. It corrects the scoping's reading of the reflection margin
(`scoping_2026-11-02.md`, README §0.1) and leaves one decision open for the user: what §1.1's measure 4
becomes (end of this note).

## How it was measured

History 2197 (M = 4×10⁻³¹ M_P, β = 1.185, α = 0.001; 6,648 generations, all floor reflections) was
re-integrated from a `git archive` of `main` at `0209aac`, in a scratch tree, because the checkout's
`ScalarHistory.py` carried uncommitted `reflection-restart` 03 work. The script is
[`probe/margin_events.py`](probe/margin_events.py): `probe/all_generations.py`'s set-up, with
`relativistic_margins` wrapped to capture its candidates and events, and every `Generation` pickled
(`events-2197.pkl`, not kept; 8 min to regenerate). Run from the scratch tree's root:
`PYTHONPATH=. python -u margin_events.py 2197 OUTDIR` (its store glob is absolute, the main
checkout's `var/datastores/`).

## 1. The failures are births, not later bounces

The margins compare ln of physical momentum with ln m: k_ex is physical at birth and Q = k_ex·a is
comoving (`log_q = log_k_ex + N_turn`). Generation g at a later bounce k has

    p_g(N_k)/m_k = (Q_g/Q_k) · (k_ex,k/m_k),

so its margin there is bounce k's own birth margin plus ln(Q_g/Q_k). The user's argument (if births
are relativistic, earlier quanta cannot be non-relativistic later) is right **given Q non-increasing**,
and it is: ln Q falls in 6,638 of 6,647 steps, from +6.8 to −72.3. The first generation's margin at
the last bounce is about +59.

The premise that fails is "births are relativistic":
- birth margin over all generations: median −15.7, minimum −20.4; 72% of ρ_χ born with ln(k_ex/m) < 0;
- a further 12% born ≥ 0 but meeting a later bounce whose births are below it;
- a generation's reflection margin equals the next generation's birth margin to a median 0.003.
Stored `birth_failing_share` is 0 only because each of these births is below the 10⁻³ weight.

## 2. Why late births are non-relativistic: the 1/φ tail

V = V₀ e^{M/φ} (n = 1); `classical_turning_point` solves V(φ_t) − V_floor = E, with E = H_E²π²/2 +
V − V_floor at assessment, ω_t² = V''(φ_t) = V λ²(1 + 2φ_t/M), k_ex = λ_t E^{1/2}/2.
- Steep wall (M/φ_t ≫ 1): ln(k_ex/ω_t) ≈ ln ½.
- E ≪ V₀: the field turns in the tail, φ_t ≈ M V₀/E; k_ex = E^{5/2}/(2 M V₀²) ∝ E^{5/2},
  ω_t ≈ √2 E^{3/2}/(M V₀) ∝ E^{3/2}, ln(k_ex/ω_t) ≈ ln(E/V₀) − ln 2√2.
- **Checked**: over the 5,981 classical generations with birth margin < −5, d ln k_ex/d ln ω_t = 1.667
  (prediction 5/3). Their birth margins put ln(E/V₀) at −15 to −19. Order-of-magnitude in eV (lab M ≈
  10⁻³ eV, V₀ ≈ 2.8×10⁻¹¹ eV⁴) agrees with the stored ln k_ex ≈ −117, ln m ≈ −97 (reduced-Planck units).
- The steep-wall spectrum is derived outside this regime; each such turnaround at lab M is a floor
  reflection drained with w_gate = 1.

## 3. What protects quanta already present: the band, not p/m

A bounce changes the χ mass on a timescale whose inverse is about its k_ex (λ_t × impact speed). Modes
with p well above it follow adiabatically (|β_p|² falls steeply above the peak). Stimulated emission
multiplies production into a mode by ≈ 1 + 2n and needs |β|² non-negligible there with n ≳ 1;
rescattering changes the occupation by ≈ 2|β_p|² n (random phases). Both need the old quanta inside
the new bounce's band. **p/m ≫ 1 is a proxy only in the classical regime** (k_ex ≈ ω_t/2); in the
one-loop regime m_ta ≪ k_ex (ℓ = ln(k_ex/m_ta) up to 32), so a quantum with p/m_ta = e¹⁰ can sit on a
later bounce's peak. Since Q falls monotonically, older generations sit at or above each bounce's peak;
in 2197's late tail Q falls ≈ 0.5% per bounce, so a median 107 earlier generations lie within half an
e-fold of each bounce's peak. The campaign's n̄_j and ε_j are overlap measures and so ask the right
question; S_j (NR share at the wall) now mostly repeats the all-generation birth margin.

## 4. Two occupation regimes (answers "weren't occupations ~10⁻⁵⁶?")

Occupation per mode, not total number. With a generation's ΔK = αE in a shell about one e-fold wide at
k_ex, n ≈ 2π² αE/k_ex⁴ — set by k_ex against E^{1/4}:
- **One-loop (R ≫ 1)**: m_ta⁴ ℓ = 32π²E so m_ta ~ E^{1/4}, k_ex = e^ℓ m_ta ⇒ n ~ α e^{−4ℓ}. This is
  inelastic-reflection's "occupations ~e^{−4ℓ}, no Bose enhancement" (sessions of 2026-10-28/30), and
  it holds: 2197's 209 one-loop generations have ln n −135 (generation 0, ℓ = 32.3) to −12;
  ln n + 4ℓ has median −8.9 (≈ ln α − 2).
- **Classical (R < 1)**: ω_t < (8π²E)^{1/4}, and in the tail k_ex ≪ ω_t, so n ~ (α/R) e^{−4ℓ} O(1) ≫ 1.
  Over the 6,439 classical generations, ln n − (ln α − ln R) has median 62 (≈ −4ℓ at ℓ ≈ −15).
- Crossover: R falls with the bounce energy (e¹³⁸ at the first impact, e⁻⁹⁶ in the tail); the first
  generation with n > 1 is #390 at N = 41.94, ln R = −1.0. By share of ρ_χ:

| ln R | share | median ln n |
|---|---|---|
| > 50 | 0.112 | −95 |
| 0 – 50 | 0.277 | −15 |
| ln α – 0 | 0.050 | +4 |
| −50 – ln α | 0.389 | +59 |
| < −50 | 0.171 | +140 |

(n here is the crude shell estimate; the campaign's spectrum model changes factors, not tens of e-folds.)

**Reading.** n ≫ 1 at birth indicts the drain, not rescattering: one bounce of order-one
non-adiabaticity gives |β|² ≲ O(1), so it can put only about R e^{4ℓ} of E into its own band. A fixed α
above that asks a single bounce for |β|² ≫ 1; the code allows it because α is an input. Coherent
build-up over many nearly identical bounces (Q steps of 0.5%) is the one physical route to n ≫ 1, and it
is not a single-bounce estimate. The same applies at large M, where R < 1 from the first impact
(R ∝ M⁻⁴, R = 1 near 10⁻¹⁵ M_P): the physical form of "the α each M should carry"
(`science-runs/2026-11-01-science-2026.8.0/diagnostic/NOTES.md`).

**Stakes, as far as measured.** These generations are born at T_J < 1 eV, after BBN; the matter-like
excess stays ≤ 4×10⁻⁹ of ρ_R. They bear on ΔN_eff at the CMB, not the yields. How common the E ≪ V₀ /
R < 1 regime is across the grid was not measured. History 2188 shows the same pattern in the scoping's
stored arrays (91.5% of ρ_χ with reflection margin < 0, generations born at N 38–44) but was not re-run
with birth margins.

## Open for the user

What §1.1's measure 4 becomes. Suggested, not ruled: record per generation the occupation at birth,
n_g ≈ 2π² ΔK_g/Q_g⁴ (from the campaign's spectrum model), and per bounce E/V₀ (or ℓ_t) and ln R; per
history the share of ρ_χ drained with n_g > 1 and with E < V₀. Keep n̄_j and ε_j only where n ≲ 1, or
replace them; drop or re-describe S_j.
