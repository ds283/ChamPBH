# Planning probe, 2026-10-28 (prompt 04)

Read-only. The planner of prompt 04 ran it at `d1d5ccf`, after prompt 03 landed, from the repository
root, at a load of 13–34:

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-28c/margin.py 0.1 10 1.2 2 5 25

- **`margin.py`** integrates one history per β with prompt 03's `integrate_history`, at α = 0.1 and
  the model's seed realization. The setup is `test_injection.integrate_captured`'s: M = 0.5 M_P,
  n = 1; φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV; to T_CMB.
- **Per generation j** (a candidate with α_eff > 0), as README §2.2 charters 04:
  - λ_j = −d ln V″/dφ at the turning point, with ln V's third derivative analytic;
  - k_ex,j = λ_j|v_j|/2√2, with |v_j| = H_E|π| at the drain point, before the drain (from
    `log_rho_kin`);
  - m_ta,j = m_eff at the turning point (03's `log_m_eff_squared`), and ℓ_j = ln(k_ex,j/m_ta,j);
  - ln q_j = ln k_ex,j + N_j.
- **The margin of generation j against a set of points:** the minimum, over the points with N ≥ N_j,
  of ln q_j − ln m_eff(N) − N, which is ln(p/m) there. The sets are:
  - **wall:** the wall-side turning points, the mass at each later wall impact;
  - **drain:** the drain points' pre-drain states (the |π| maximum of each drained half-oscillation,
    with the field off the wall), plus today's state;
  - **dense:** 10 points per accepted step, the whole trajectory;
  - **today:** the last state alone.
- **Reported:** the history's margin (the minimum over j); how many generations fall below ln 10 and
  below 0; and the fraction of today's ρ_χ (ΔK_j/K) that those generations carry.
- **`margin_alpha0.1.out`**: β = 1.2, 2, 5 and 25 at α = 0.1.

What it shows is in prompt 04 §0, or, if 04 is held, in the planner's report to the user of
2026-10-28.
