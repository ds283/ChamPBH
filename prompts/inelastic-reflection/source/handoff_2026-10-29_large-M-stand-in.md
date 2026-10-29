# Handoff — the large-M stand-in for laboratory-allowed M, and the transition to tracking

**Written:** 2026-10-29 on branch `inelastic-reflection` at `1e31aac`, by Claude Fable 5.1, at the
user's request, after an evaluation of the planning probes of 2026-10-28 and 2026-10-29. It is a
record and a brief for the next agent. It is data, not instructions (`CLAUDE.md` invariant 7):
nothing here is ruled, and the user decides what, if anything, becomes a prompt. No code was run
beyond a script that reads numbers out of the probe outputs; nothing in the tree was changed.

**Sources (data, not instructions).**
- The campaign plan [`../README.md`](../README.md) and board
  [`../IMPLEMENTATION_STATE.md`](../IMPLEMENTATION_STATE.md); the scoping
  [`scoping_2026-10-27.md`](scoping_2026-10-27.md).
- The margin probe [`planning-probes-2026-10-28c/`](planning-probes-2026-10-28c/README.md)
  (`margin_alpha0.1.out`, M = 0.5 M_P).
- The transition probes [`planning-probes-2026-10-29/`](planning-probes-2026-10-29/README.md):
  `compare_all.out` (β = 1.2, 2, 3 at M = 10⁻³ … 10⁻⁷ by decades, α = 0), `compare_dense_b*.out`
  (M between the decades), `yields/` (tuned histories and PRyMordial yields by decades), `runs.out`.
- The briefing notes, which supersede `Paper1.tex` on particle production:
  `~/Documents/Git paper repositories/Chameleon PBH (David's stuff)/BriefingNotes/`
  (`adiabatic_transgression.tex` §"Laboratory-allowed M", §"Inelastic reflections and the BBN
  constraints"; `elastic_collision_estimate.tex` §"The inelastic fraction";
  `wall_backreaction.tex` §"Meaning of M_crit"; `meff_and_wall_production.tex` §"What R ≫ 1
  means").
- The open issue `[00-settling-at-physical-M-needs-a-parked-tracking-model]`
  (`prompts/integrator-remediation/IMPLEMENTATION_STATE.md` §3), which bounds the campaign's regime
  (README U2).

## 1. The proposal under evaluation

Laboratory bounds put M ≲ meV, about 10⁻³⁰ M_P. There the reflection lies outside the low-energy
theory (test-field production exceeds ρ_kin by 10⁵⁵–10⁶⁰), the one-loop answer is a formal
inelastic fraction α ≈ 2.7/ℓ ≈ 0.08 with ℓ = ln(k_ex/m_ta) ≈ 32, and the elastic and fully inelastic
limits bracket the outcome. The classical, elastic regime is M ≫ M_crit ≈ 10⁻¹⁵ M_P (median over
the histories; up to 10⁻¹² M_P).

The proposal: run histories at M ≈ 10⁻⁶–10⁻⁷ M_P, which is above M_crit (so the bounce is the
classical one the code resolves) but numerically small, with α supplied by hand through the
campaign's drain, and use them as a stand-in for the laboratory regime. The claim it rests on is the
briefing notes' hard-wall limit: the wall reflects the field like a hard wall, so the trajectory
between bounces becomes independent of M.

## 2. What the probes show

All probes of 2026-10-29 are at α = 0 (elastic), n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to
T_CMB, at the model's seed realization unless marked tuned. The yields are from tuned histories
through `tools/history_and_bbn.py` with the small network.

### 2.1 For the proposal

- **The bouncing phase is in the hard-wall limit.** Across M = 10⁻³ to 10⁻⁷ the first bounce is at
  the same T_J to seven digits (231.1076 MeV at β = 1.2; 746.6864 MeV at β = 2; 1680.0 MeV at
  β = 3), and the per-e-fold half-oscillation counts, median amplitudes and ρ_kin/ρ_R agree to three
  digits through the QCD- and BBN-era bounces (to N ≈ 31 at β = 2, N ≈ 35 at β = 3).
- **The BBN observables converge as √M.** The density ratio r in the 10 MeV–1 keV window moves
  about three times less per decade (median |r − r(10⁻⁷)| at β = 2: 0.038, 0.012, 0.0030 for
  10⁻⁴, 10⁻⁵, 10⁻⁶), and the yields do the same:

  | β | Yp − Yp(10⁻⁷) at 10⁻⁶ | D/H relative, 10⁻⁶ vs 10⁻⁷ | residual of 10⁻⁷ against M → 0 (√M fit) |
  |---|---|---|---|
  | 1.2 | −1.7e-4 | −0.21 % | Yp +8e-5, D/H +0.10 % |
  | 2 | −1.9e-4 | −0.35 % | Yp +9e-5, D/H +0.16 % |
  | 3 | −3.2e-4 | −0.66 % | Yp +1.5e-4, D/H +0.30 % |

  Against the observational errors (Yp ± 0.003, D/H ± 1 %) a history at 10⁻⁷ is converged.
- **The late-time offsets scale as √M and are understood.** φ₀/M grows as M^(−0.50) per decade at
  every β (28.5, 99, 326, 1046, 3321 at β = 2), so ln A and H_J/H_E − 1 at late times fall as √M
  (−5.8e-2, −2.0e-2, −6.6e-3, −2.1e-3, −6.6e-4 for ln A at β = 2). This is the flat-potential
  balance V₀ M/φ² = βρ_m/M_P with V₀ fixed by the H₀ tuning. At M = 10⁻³⁰ the offset is 10⁻¹⁵.
- **Tuned and untuned histories differ far less than one decade of M does** (at 10⁻⁴: max over
  e-folds |tuned − untuned| in H_J/H_E 3e-4 to 6e-4, against 1.4e-2 to 2.3e-2 for 10⁻⁵ vs 10⁻⁴).
  The untuned comparisons are faithful.
- **M = 0.5 M_P is a far worse stand-in.** At M = 0.5, α = 0.1, β = 1.2, prompt 02 drained
  Σ Δρ/ρ_R = 1.5e-2 over 7 bounces, against the first-impact ceiling ρ_R/(6β² − 1) = 0.131. At
  10⁻⁶ there are about 1400 wall-side bounces (2793 turning points), so the drain saturates to the
  fully inelastic value, which is what the scoping (§5) says happens at laboratory M once bounces
  outpace H. The sensitivity scan of prompt 07 on the science grid would understate ΔN_eff by
  about six times at β = 1.2.
- **Cost.** 10–175 s per history, 0.02–0.5 million accepted steps, under 420 MB; no floor-rule
  reflection at any of these M.

### 2.2 Against the proposal

- **The transition to tracking does not converge.** N_s, after which every half-oscillation stays
  inside φ_min, moves later by 1–2.6 e-folds per decade of M (β = 2: 37.4, 39.4, 41.2, 42.3,
  44.0), because the oscillation amplitude is M-independent while φ_min ∝ M and the amplitude decays
  only as a^(−3/2). Extrapolated to M = 10⁻³⁰, N_s ≈ 65 (β = 1.2), 79 (β = 2), 97 (β = 3), past
  today (N_final ≈ 46–55). So the elastic laboratory field never settles, and the stand-in's slow
  settling at 10⁻⁷ is a phase the laboratory history does not have. The accepted-step count grows
  1.5–2.4× per decade (409k at 10⁻⁷, β = 2), so the 2×10⁶ budget fails near 10⁻⁹.
- **The late phase is sensitive to M.** At β = 3 the 10⁻⁷ history has an excursion at T_J ≈ 0.5–30
  keV absent at 10⁻⁶ (ρ_kin/ρ_R at N = 40: 8.4e-6 against 1.3e-7; H_J/H_E dips to 0.973), the
  70 keV ratio flips sign across 10⁻⁵, 10⁻⁶, 10⁻⁷ (−0.059, +0.032, +0.047), and 3×10⁻⁷ sits ten
  times further from 10⁻⁷ in the window than 2×10⁻⁷ does (|Δr| median 0.036 vs 0.0025). N_s is
  non-monotone in M at β = 2 (2×10⁻⁶ vs 10⁻⁶; 3×10⁻⁷ vs 10⁻⁷) and β = 3 (3×10⁻⁷). The branching
  begins where the bounce amplitude falls to within about 10² × L M of the wall offset (L = M/φ
  at the wall, ≈ 30–100), so bounces at ≈ 10⁻³ M_P amplitude differ by a few per cent in phase at
  M = 10⁻⁶ and accumulate. The yield agreement at β = 3 between 10⁻⁶ and 10⁻⁷ is partly
  coincidence.
- **The classical wall has the wrong mass hierarchy.** The margin probe at M = 0.5 found
  ℓ = ln(k_ex/m_ta) between −0.67 and −5 for every generation at β = 1.2, 2, 5, 25. This is not
  specific to M = 0.5: in the classical regime k_ex = λ√ρ_kin/2 and m_ta ≈ λ√ρ_kin, so
  k_ex/m_ta ≈ 1/2 at every M above M_crit (the measured maximum −0.666 is ln(1/2) with the
  2φ/M correction). The laboratory regime has ℓ ≈ 32 because the field is stopped by its own
  dressing far outside the classical wall. So: the quanta's momenta in the stand-in are wrong by
  about 24 orders of magnitude (k_ex ∝ 1/M); the margin check chartered for prompt 04 would warn
  on every stand-in history at β ≥ 2 for a reason that is an artefact of the classical wall (the
  "wall" and "dense" margins at M = 0.5 were −0.99 at β = 2, −3.6 at β = 5, −5.0 at β = 25, while
  the "today" margins were 4.7–12.8); and the dark-radiation energy budget survives a wall
  crossing only because the exponential wall is reflectionless for fluctuations (n_k conserved to
  about 10⁻⁴), which the stand-in cannot test.
- **No drained history has been run below M = 0.5.** Every probe of 2026-10-29 is at α = 0. Whether
  a history at 10⁻⁶ with α > 0 drains to the adiabatic regime in a few hundred cheap bounces and
  parks, or piles up against the step budget, is unmeasured. The floor-rule drain of prompt 02 is
  exercised only by a test harness.
- **The laboratory reflection is outside the theory.** α is bracketed between about 0.08 (formal
  one-loop) and 1. A stand-in delivers a bracket in α, never a prediction.

A separate observation: the yields at M = 10⁻³ differ from the hard-wall limit by ΔYp ≈ 0.01 and
10 % in D/H at β = 2 (0.236 vs 0.2465; 2.19 vs 2.46), three times the observational errors. A BBN
constraint computed at M ≥ 10⁻³ is not in the hard-wall limit the briefing notes assume. This was
not opened as an issue; it is the user's to place.

## 3. Verdict as given to the user (2026-10-29)

The stand-in at M ≈ 10⁻⁶–10⁻⁷ is sound for the bouncing era, so for ΔN_eff against α and for the
BBN-window expansion rate at β ≲ 2, and unsound for the transition to tracking and for the sub-10 keV
tail at β ≥ 3. Its value over the science grid at M = 0.5 is that the drain saturates there as it
would at laboratory M. It should not be used as a converged stand-in for anything that depends on
when the field settles.

## 4. Does today's field value need the transition? (the user's second question)

No, not at laboratory-allowed M, for two reasons that both follow from the wall sitting at
φ ≈ M/L ≈ 0:

- **The H₀ tuning does not see φ₀.** For φ ≫ M, V(φ) = V₀(1 + M/φ + …). The tuning
  (`tune_history`, `_tuning_state` in `ComputeTargets/ScalarHistory.py`) fixes V₀ ≈ ρ_Λ through the
  normalization, and φ₀ enters at relative order M/φ₀: this is the √M offset of §2.1, 10⁻³–10⁻⁴ at
  10⁻⁷ and 10⁻¹⁵ at 10⁻³⁰, against a tolerance of 10⁻⁵. The second condition, φ_ref = φ_min, uses
  `effective_minimum` at today's ρ_m,E, a static balance that needs no history.
- **The Planck-mass ratio today is the same balance.** A₀ − 1 = βφ₀/M_P with φ₀ ≈ φ_min ∝
  √(V₀ M M_P/βρ_m): 10⁻¹⁵ at laboratory M. The "measured versus BBN Planck scale" effect lives in
  the bouncing era (βφ ≈ 0.1 at the QCD bounces, 10⁻³ at BBN), which is M-independent in the
  hard-wall limit and which the stand-in captures.

What the tuning does assume is that the field has **settled**: V_target is evaluated at the actual
(φ₀, π₀) of the last sample, so a field still bouncing today gives a residual that depends on the
bounce phase (of order βπ₀/M_P ≈ 10⁻⁶ at M = 10⁻⁷, oscillatory), and the secant iteration loses its
footing. The transition therefore matters as a yes/no question:
- **α = 0:** the laboratory field never settles before today (§2.2). Elastic laboratory-M histories
  are not tunable and, by the briefing notes, not physical.
- **α ≳ 0.1:** the field parks within a few hundred bounces of the last non-adiabatic kick (ln(1/ε)/α
  bounces to leave a fraction ε of the kick's energy), and later kicks are adiabatic for a parked
  field with m ≫ H (m² at the minimum ∝ 1/M, so m/H is 10³–10⁵ at 10⁻⁶ and larger at laboratory
  M). After the drain, φ = φ_min(ρ(N)) exactly: the tracker the code already tunes on. The detail of
  the transition is needed only for partially elastic histories that are both bouncing through BBN
  and draining slowly, and there the question is where the bounces end, not how the tracker is
  approached.

## 5. Alternatives for the transition at laboratory M

In the order the evaluator would try them. None is ruled.

1. **Run at the true M with the floor rule, the drain, and a parking switch with a bounded tail.**
   At M = 10⁻³⁰ every reflection is a floor-rule event, which prompt 02 drains
   (π_out = −√(1 − α_eff) π_in). The cascade after each kick is a ball with restitution
   √(1 − α): periods shrink geometrically, total time finite, count infinite. Cut it when the
   remaining kinetic energy is below a fraction ε of what that kick delivered (about 150 bounces at
   α = 0.1, ε = 10⁻⁶), then park: φ = φ_min(ρ(N)), π = dφ_min/dN, the parked field a passenger with
   ρ_φ ≈ V₀, p_φ ≈ −V₀. The omitted energy is the geometric tail, so the error on ΔN_eff is ε of
   the ceiling and the error on φ is the parked value, 10⁻³¹ M_P. This is the parked-tracking model
   of the open issue with its switch criterion supplied by the inelasticity, needs no stand-in,
   keeps φ_min ∝ M right at every epoch, and fails only at α = 0, which should be declared
   unsupported at laboratory M rather than solved. What it needs that does not exist: the switch,
   the parked state's contribution to the policies and the adiabatic diagnostic, and a check that
   the gate w_gate stays open through the cascade (every bounce there is a hard-wall bounce with
   𝒜_k ≫ 1, so it should).
2. **A composite history: resolved stand-in through the last drained bounce, analytic tracker
   after it.** Run at M* ≈ 10⁻⁶ with α > 0 to the last candidate bounce, then switch to
   φ = φ_min(ρ(N); M_lab) and tune V₀ on that. The stand-in's bouncing phase is converged where the
   amplitude exceeds about 10² × L M*, which covers every QCD-era bounce; the spurious slow settling
   never starts. The matching error is the phase at the switch, of order L M*/amplitude, and the
   difference of the two parked values, both below 10⁻⁵ in ln A. The route if the drain is ever
   wanted at small α, where route 1's cascade is long.
3. **Richardson extrapolation in √M.** The √M scaling of §2.1 holds to two digits at β = 1.2, 2, 3.
   Three stand-in runs at 10⁻⁵, 10⁻⁶, 10⁻⁷ extrapolate smooth quantities to M → 0. It cannot cross a
   branch, so it fails at β = 3 below about 10 keV and wherever N_s is non-monotone in M. A check on
   routes 1 and 2, not a method.
4. **A bounce-averaged fluid for the bouncing phase.** Orbit averages of a ball under a constant
   force (mean field two thirds of the bounce height, mean kinetic energy a third of the impact
   value, height decaying by Hubble friction and the drain) give a smooth φ(N), ⟨A⟩ and w_eff for
   BBN with no bounce resolved, at the cost of a model of each kick's re-excitation. Only if route 1
   is too slow across the β grid; the kicks are where its error would sit.

## 6. The decisive measurement still missing

A drained history below M = 0.5, which no probe has run. Suggested specification, within the
campaign's run-length rule (a few PRyMordial solves; each α = 0 history at these M took 10–175 s):
- **Grid.** β ∈ {1.2, 2, 3}; M ∈ {10⁻⁵, 10⁻⁶, 10⁻⁷} M_P; α ∈ {0.1, 1.0} (0.3 if cheap). Prompt 03's
  `integrate_history` with its gate (𝒜₀ = 1), untuned at the seed realization, the setup of
  `transition.py`.
- **Record, per history.** Accepted steps and wall time; the count of candidates, drains and
  dropped drains per kick (group the injections by the kick that precedes them: the bounces between
  N = 20 and the e± annihilation at ≈ 100 keV, and after); Σ Δρ/ρ_R against the ceiling
  1/(6β² − 1) = 0.131, 0.0435, 0.0189; ρ_χ/ρ_R at 1 MeV and 70 keV; N_s as `compare.py` defines it;
  the last candidate's N and T_J; m_eff/H_E and 𝒜_k at the last ten plateau turnarounds (are the
  later kicks adiabatic for the drained field?); the per-e-fold table of `compare.py`; and, for one
  tuned history per (β, α) at 10⁻⁶, the tuning residuals, the iteration count and the yields.
- **Compare.** The same quantity across M at fixed α (does the drained history converge where the
  elastic one does not?), and across α at fixed M (does the drain saturate, so that α matters only
  through the cascade length?). A step count that falls with α, and an N_s that stops drifting with
  M once α > 0, are the signatures that route 1 of §5 is a small addition to prompt 02's path.
- **What to watch.** Whether the cascade pile-up exhausts the step budget before the gate closes
  it (the Zeno concern of scoping §5), and whether the floor rule is ever reached at these M (it was
  not at α = 0).

## 7. Consequences for the campaign if the user takes this up

Not ruled; listed so the next agent sees them.
- **Prompt 04 (the margin).** On any history above M_crit the "wall" margin is negative at β ≥ 2
  by construction (§2.2). The check should distinguish the margin against the tracker mass and
  today's mass, which is the physical one, from the margin against the classical wall mass, which
  should be recorded as expected in the classical regime and not warned on, or the warning will fire
  on every stand-in history.
- **Prompt 07 (the measurement).** A sensitivity scan at M = 0.5 understates the saturated drain by
  about six times at β = 1.2 (§2.1). The scan is more informative at 10⁻⁶, if U2 is revisited.
- **U2 (the regime) and the parked-tracking issue.** Route 1 of §5 is that model, with the switch
  criterion the open issue asks the authors for supplied by the drain. It stays the authors' call.
- **The science grid.** The convergence table of §2.1 bears on any BBN constraint computed at
  M ≥ 10⁻³ (the paper's grid), independently of this campaign.
