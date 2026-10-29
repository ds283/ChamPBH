# Probes for prompt 06, 2026-10-29: the state at the parking cut

Read-only. Run by the orchestrator at `9c2483b` (prompt 05 landed) when writing prompt 06, at the user's
word ("Please can you write prompt 06?"). Nothing is stored, no Ray is started and nothing in the tree is
changed. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, default
tolerances, the model's seed realization, untuned) at α = 0.1. The numbers are measurements; no threshold
was set in advance.

All commands run from the repository root.

## 1. The cut and the switch (`probe_cut.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29c/probe_cut.py BETA M N_STOP

One history through `test_unresolved_turnarounds.window`, to N_STOP, past the cut. The cut candidate is
the first candidate after the history's first with ρ_kin/ρ_R,E < 10⁻⁶, ρ_kin before its drain. At it:
- φ_min = `minimum_on_trace(φ, ln T)`, T = ρ_R,E(Σ + f_m), and π_min = dφ_min/dN, solved algebraically.
  d ln T/dN is linear in π through the factors 1 + (ln A)′π of the right-hand side, so
  d ln T/dN = a + bπ and π_min = s a/(1 − s b), with s = `d_minimum_d_log_trace`; dΣ/dN uses the
  cosmology's `dw_dlogT`;
- m_eff/H_E of the tracker (`log_m_eff_squared` at φ_min, H_E at (φ_min, π_min));
- H_E at the parked state against H_E before the candidate, **without** the leftover in the ledger (so
  the difference is about half the leftover fraction; with it the probe expects continuity);
- Δ ln A = ln A(φ_min) − ln A(φ) at the switch, against the range of ln A over the oscillation since the
  previous candidate.

Then a reduced integration of (ln ρ_R,E, ln f_m, ln T_J) by SciPy's DOP853 (rtol 10⁻¹⁰, atol 10⁻¹²,
max step 0.1) with ODERHS's components 2–4 at (φ_min, π_min), from the cut to N_STOP, compared with the
oscillating history over the same window. Its φ_min start is the last call's (a cache: fine for a
probe, not for an interpolant). Last, π_min against a finite difference of φ_min along the reduced
solution.

**`probe_cut.out`**, the windows that 05's histories survive: β = 1.2 (to 29.5), 2 (to 30.9) and 3 (to
29.6), at M = 10⁻⁷ and 10⁻³⁰, and β = 2 at 10⁻¹².

## 2. Does the cut fire on a full history? (`probe_large_M.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29c/probe_large_M.py BETA M

One full history (`test_injection.integrate_captured`) at α = 0.1. For ε = 10⁻⁴, 10⁻⁶ and 10⁻⁸, the first
candidate after the first with ρ_kin/ρ_R,E < ε, and m_eff/H_E of the tracker there. It examines only that
first candidate; where it has m/H < 10 a later one could still fire. The three smallest ratios are
printed, to show whether any later one is below ε.

**`probe_large_M.out`**: M = 0.5 M_P at β = 1.2, 2, 5, 10, 25 and M = 10⁻³ M_P at β = 1.2, 2, 3.
