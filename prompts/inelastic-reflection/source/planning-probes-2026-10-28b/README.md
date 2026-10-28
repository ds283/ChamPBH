# Planning probes, 2026-10-28 (prompt 03)

Read-only. The planner of prompt 03 ran the probe at `bc5314e`, from the repository root, at a load
of 20–50:

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-28b/nonadiabaticity.py 0.0 1.2 2 5 25
    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-28b/nonadiabaticity.py 0.1 5

- **`nonadiabaticity.py`** integrates one history per β with prompt 02's `integrate_history`, at
  α as given and the model's seed realization.
  - **Setup:** prompt 02's, as `ComputeTargets/tests/test_injection.py`'s `integrate_captured` sets it
    up: M = 0.5 M_P, n = 1; φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV; to T_CMB; main.py's axis.
  - **The non-adiabaticity.** Post hoc, on 33 equally spaced points of every accepted step's
    interpolant, it computes 𝒜 = H_E |d ln m²/dN| / 2m.
    - m² = V″(φ) + (d ln A/dφ)² ρ_R,E (Σ + f_m), since (ln A)″ = 0 for the exponential coupling.
    - d ln m²/dN is taken by `np.gradient` within each step.
  - For each of 02's turning points it prints 𝒜_k, the maximum of 𝒜 over the half-oscillation
    (N_{k−1}, N_k] that ends there. It prints it three ways: over every sample, over the samples
    with m ≥ H, and over those with m ≥ 10 H.
  - It also prints m/H and V″/(trace term) at the turning point itself.
- **`nonadiabaticity_alpha0.out`**: β = 1.2, 2, 5 and 25 at α = 0.
- **`nonadiabaticity_alpha0.1_beta5.out`**: β = 5 at α = 0.1. This is 02's drained trajectory; the
  near-stall pair is at N = 42.889 and 42.893.

Prompt 03 §0 tabulates them.
