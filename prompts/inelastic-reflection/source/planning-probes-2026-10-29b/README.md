# Planning probes, 2026-10-29: the true M, the parking cut and the parked tracker

Read-only, at the user's request of 2026-10-29 ("Can we test the handoff's proposal §5.1?", then
"Happy for you to dispatch the probes"), after the handoff
[`../handoff_2026-10-29_large-M-stand-in.md`](../handoff_2026-10-29_large-M-stand-in.md). Code at
`1e31aac`. Nothing is stored, no Ray is started and nothing in the tree is changed: the probes wrap or
copy code in memory. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned). The JSON
records are in the git-ignored `var/probes/floor-cascade-2026-10-29b/` (`cascade/`, `rgrid/`). No
thresholds were set in advance; the numbers are measurements.

All commands run from the repository root.

## 1. The floor-rule cascade at the true M (`floor_cascade.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29b/floor_cascade.py BETA M ALPHA BUDGET OUT.json

One history through prompt 03's `integrate_history` at α > 0, with today's floor rule, drain and gate.
`SM.Injection`, `SM.Reflection` and `SM.TurningPoint` are wrapped so that their records survive a failed
history, `SM.Radau` is subclassed to count accepted steps, and `SM.StepControl` carries a reduced step
budget (6×10⁵). Records per candidate: N, kind, π_in, α_eff, w_gate, A_k, Δρ/ρ_R, the steps so far.

- **`floor_cascade_runs.out`**: the run lines. β ∈ {1.2, 2, 3} × α ∈ {0.1, 0.9} at M = 10⁻³⁰ M_P, and
  β = 2, α = 0.1 at M = 10⁻¹² and 10⁻⁶. Every M = 10⁻³⁰ history fails at the bottom of its cascade
  (φ ≤ 0, or guard G2 at β = 1.2, α = 0.1) with |π| ≈ 2–10×10⁻¹² M_P; M = 10⁻¹² fails with Radau's
  "step size less than spacing"; M = 10⁻⁶ runs to T_CMB.
- **`summarise.py EPS FILE...`** → **`floor_cascade.out`**: per history, the candidates, Σ Δρ/ρ_R
  against the first-impact ceiling 1/(6β² − 1), the spread of w_gate, the drain share of the first k
  candidates, and a cut where the remaining kinetic energy falls below EPS (10⁻⁶) of the largest |π_in|²
  since the last re-excitation (a rise of more than 5 %), with the per-bounce ratio of |π_in| against
  √(1 − α_eff).
- **`cut_rhoR.py FILE...`** → **`cut_rhoR.out`**: the cut relative to the bath instead, ρ_kin/ρ_R < ε at
  the first floor reflection after the first impact (ρ_kin/ρ_R ≈ π²/6 M_P² there), for ε = 10⁻⁶, 10⁻⁸,
  10⁻¹⁰: where it fires, the steps to it, and the share of the history's drain after it.

## 2. The parked tracker (`parked_tracker.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29b/parked_tracker.py

No integration. The background is the cosmology's equation of state with A = 1 (exact at laboratory
M; at large M it neglects A − 1): ρ_R = (π²/30) g_ρ T⁴, ρ_m from entropy conservation, Σ = 1 − 3w,
N from g_s^{1/3} T a = const, from 2 GeV to T_CMB on 6000 points. φ_min(N) solves
V′ + (β/M_P)(ρ_R Σ + ρ_m) = 0 on the seed realization. Along it: m_eff from the loop's own
`log_m_eff_squared`; A_ad = H|d ln m²/dN|/2m; the first adiabatic correction to the position,
δφ/φ_min = −(φ_min″ + (3 + Ḣ/H²)φ_min′)/(m²/H²)/φ_min (the lag); the kinetic fraction of the
zero-lag tracker; and the first temperature, from above, at which m/H < 10, A_ad > 0.1 or
|lag| > 10⁻².

- **`parked_tracker.out`**: β ∈ {0.5, 1.2, 2, 3, 5} × M ∈ {10⁻³⁰, 10⁻¹⁵, 10⁻⁷, 10⁻³, 0.5} M_P. The lag
  today reproduces the lags measured by `planning-probes-2026-10-29` (`compare_all.out` (d)) at
  M = 10⁻³ and 10⁻⁷ to within 5–40 %.

## 3. Resolved against unresolved turnarounds, to the parking cut (`r_criterion.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29b/r_criterion.py MODE BETA M ALPHA BUDGET OUT.json

A copy of `integrate_scalar_history`, edited by string replacement (each edit asserted to match once)
and exec'd in a copy of the module's namespace, patched in for `integrate_history`. For an infalling
field it computes the classical turning point ahead of it from the incoming kinetic energy
E = H_E²π²/2: y_t = ln(1 + E/V₀), φ_t = M/y_t, λ = y_t²/M, and the test-field ratio
𝓡 = λ⁴E/8π² there (the briefing notes' 𝓡 = 2ε at the classical wall).

- **MODE "old"**: today's rules.
- **MODE "R"**: also reflect (the floor branch, so prompt 02's drain) when 𝓡 > 1, as soon as
  φ < K φ_t, K = 10. Such a reflection drains with w_gate = 1; resolved turnarounds keep the gate.
- **MODE "RN"**: as R, and also when φ_t < PHI_NUM = 10⁻¹⁰ M_P, a classical wall thinner than the
  integrator resolves reliably. PHI_NUM is a probe value, set between the stand-in at M = 10⁻⁷
  (φ_t ≈ 10⁻⁹ M_P, resolved without trouble at α = 0) and the failures at M = 10⁻¹⁰ and 10⁻¹²
  (φ_t ≈ 10⁻¹² and 10⁻¹⁴ M_P); it was not scanned.
- **Every mode stops at the parking cut**, ρ_kin/ρ_R,E < ε = 10⁻⁶, tested at a floor reflection
  (on π_in) or at a drain point (the kinetic-energy maximum), so only after the first impact.

Outputs:
- **`r_criterion_runs.out`**: the run lines. "old" and "RN" for β ∈ {1.2, 2, 3} at α = 0.1 and for
  β = 2 at α = 0.9, M = 10⁻⁸ to 10⁻¹⁷ by decades; one "R" control at β = 2, M = 10⁻³⁰, α = 0.1.
- **`rsummary.py DIR`** → **`r_criterion.out`**: one row per history. The columns are the outcome
  (where it parked, or how it failed), steps and wall time; floor injections, drain-point
  injections and wall-side turning points; under "byR" and "byNum", the approaches that the 𝓡 rule
  and the φ_t rule judged unresolved; under "resolv", those judged resolved; the range of log₁₀ 𝓡
  over the judged approaches; and Σ Δρ/ρ_R, alone and against the ceiling. A row with no judged
  approach never came within K φ_t of the wall: today's floor rule reflected it further out.

What the probes show is in the planner's reports to the user of 2026-10-29.

## 4. Scan of the threshold PHI_NUM, and of the reflection point K (`phi_t_scan.py`)

At the user's word of 2026-10-29 ("let's do the job properly and try to scan the threshold for
`phi_t`"). §3 set PHI_NUM = 10⁻¹⁰ M_P without a scan; this section scans it. §3 stands as it was run.

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29b/phi_t_scan.py BETA M ALPHA PHI_NUM GATE BUDGET OUT.json [K]
    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29b/phi_t_scan.py check

`r_criterion.py`'s mode "RN", changed in two ways:
- **No closed form of the potential.** φ_t is the root of V(φ_t) − V_floor = E, found in ln φ inwards
  from the field through `RealizedModel.log_V` and `log_V_floor`. 𝓡 = ω_t⁴/8π²E, with
  ω_t² = V″(φ_t) = V[(ln V)″ + (ln V)′²] from `d_logV_dphi` and `d2_logV_dphi2`. It needs
  `reflects_at_origin`. **`check`** compares both with the exponential model's closed form: φ_t agrees
  to 10⁻¹², and log₁₀ 𝓡 differs by 0.01–0.02, the term 2M/φ³ of V″ that §3's closed form,
  λ = y_t²/M, leaves out.
- **PHI_NUM, GATE and K are arguments.**
  - GATE "gate" keeps prompt 03's gate on resolved drain points; "w1" gives every drain point
    w_gate = 1.
  - K (default 10) sets where an unresolved reflection is made: as soon as φ < K φ_t.

Outputs:
- **`phi_t_scan_runs.out`**: the 440 run lines. PHI_NUM ∈ {0 ("R only"), 10⁻¹², 3×10⁻¹², …, 10⁻⁸} M_P ×
  M ∈ {10⁻⁷, 3×10⁻⁸, …, 10⁻¹²} M_P. Every combination runs at β ∈ {1.2, 2, 3} with GATE "gate", and at
  β = 2 with "w1"; all at α = 0.1 and K = 10. 434 parked. The 6 failures are all at M = 10⁻¹⁰ with
  PHI_NUM ≤ 10⁻¹² (β = 2 and 3, and β = 2 with w1), with Radau's "step size less than spacing".
- **`scan_summary.py DIR`** → **`phi_t_scan.out`**: grids of M against PHI_NUM, one per (β, GATE): Σ Δρ/ρ_R
  against the ceiling, the parking temperature, the accepted steps, and the reflections judged
  unresolved. The "φ_t range" column gives the classical turning points of the approaches that came
  within K φ_t.
- **`scan_K_compare.py DIR`** → **`scan_K_compare.out`**: the band M = 10⁻⁹ to 3×10⁻¹¹, where the
  results of the grid depend on PHI_NUM. It compares "R only" with PHI_NUM = 10⁻⁸ (every wall
  reflected) at K = 10, 3 and 1.5. The K = 3 and 1.5 runs are in `var/probes/…/scanK/`. The column
  "w<1" counts the floor reflections that drained with w_gate < 1. Those are today's floor rule,
  whose gate the probe leaves reading the infall; only the reflections judged unresolved drain with
  w_gate = 1.
