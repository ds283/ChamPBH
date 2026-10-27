# Planning probes, 2026-10-27

The three scripts the planner ran on `main` at `6f635e6` while the scoping in
[`../scoping_2026-10-27.md`](../scoping_2026-10-27.md) was worked through with the user. They read
the production EOS (`Xav_EOS_spline`) and nothing else of the code. They start no Ray and open no
store. Each `.out` file is the script's output on that tree. Run them from the repository root:

```bash
PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-27/trace_scaling.py
```

| Script | What it measures | Used in |
|---|---|---|
| `trace_scaling.py` | d ln ρ_c/dN for the trace ρ_c = Σρ_R + ρ_m along the thermal history (20 TeV to 12.6 keV, inside the EOS table), and the largest growth of m·a at the minimum for m² ∝ ρ_c^γ, γ = 1 and 3/2 | scoping §3.1 |
| `tracking_mass.py` | m_eff·a at the tracking minimum of V₀ exp(M/φ) + A ρ_c, n = 1, M = 0.5 M_P, for β = 0.1–25, from 20 TeV to today; βφ_min, γ today, the largest rise; Σ, g, g_s near 30 keV | scoping §3.1, README §0.3, board §3 |
| `late_time_margin.py` | m_eff today at the tracker in units of H₀ for the same β; the comoving Hubble rate (Ha) at a bounce relative to today's | scoping §3.3 |

They are probes, not tests. In particular:
- **Einstein and Jordan frames are not distinguished**, and A(φ) is set to 1 at late times.
- **The minimum is the tracking minimum.** The actual trajectory is not followed.
- **Below the table** (12.6 keV in `trace_scaling.py`, 10 keV in `tracking_mass.py`) Σ is set to 0
  and g, g_s to their relic values. The ×1.19 rise between 30.2 and 28.0 keV, inside the table,
  is not physics. Σ from the table is non-monotone there (`tracking_mass.out`, last block).
