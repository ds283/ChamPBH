# Probes for prompt 09, 2026-10-31

Read-only. The orchestrator ran this at `0f0cfe4` (prompt 08 landed and reviewed) when prompt 09 was
written, at the user's word ("OK, can we write prompt 09?"). Nothing is stored, no Ray is started, and
nothing in the tree is changed. The numbers are measurements; no threshold was set in advance. The load
was about 5.

All commands run from the repository root.

## ΔN_eff at 1 MeV, 70 keV and today (`neff.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-31/neff.py BETA M

Each history is `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, the
model's seed realization, untuned) at α = 0.1, by `integrate_history`.
- r = ρ_χ/ρ_R is formed from the payload's ledger and the sample, K(N) e^{−4N}/ρ_R,E. It is taken at
  the first stored sample at or below T_J = 1 MeV and 70 keV, and at the last sample (today). These
  are samples near the temperature, not the crossing that prompt 09 uses (P31).
- Two conventions are printed beside r: (4/7) g_ρ r, which takes T_ν = T_γ, and P7's
  (4/7) g_ρ (11/4)^{4/3} r, which takes T_ν/T_γ = (4/11)^{1/3}. g_ρ is the sample's.

**`neff.out`**, β = 2 at M = 10⁻³⁰ and 0.5 M_P, and β = 5 at M = 0.5:

| History | ΔN_eff(1 MeV), T_ν = T_γ | ΔN_eff(1 MeV), P7's | ΔN_eff(today), P7's | last injection N | N at 1 MeV |
|---|---|---|---|---|---|
| β = 2, M = 10⁻³⁰ | 0.0497 | 0.191 | 0.0524 | 30.35 | 27.57 |
| β = 2, M = 0.5 | 0.0491 | 0.189 | 0.0515 | 34.62 | 27.55 |
| β = 5, M = 0.5 | 0.00577 | 0.0222 | 0.00595 | 43.64 | 42.54 |

- With T_ν = T_γ at 1 MeV and P7's expression today, the two values agree to 4–5 %. Each history
  injects after 1 MeV, so today's is the larger.
- P7's expression at 1 MeV is (11/4)^{4/3} ≈ 3.85 times the comparable value.
- The user ruled the first convention (README §8.1 R22).
