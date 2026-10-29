# Probe, 2026-10-29: does the transition to tracking converge as M decreases?

Read-only, at the user's request of 2026-10-28/29, after prompt 03 landed (code at `20bfafa`). It asks
whether a history at M ~ 1e-6 M_P is a converged stand-in for smaller M through the passage from
the bouncing phase to late-time tracking. First pass only: β = 2 and 3 at M = 1e-5, 1e-6 and 1e-7
M_P, α = 0, untuned at each model's seed realization, with `test_injection`'s setup (n = 1,
φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, main.py's axis, default tolerances). No thresholds
were set: the user asked to see what the results do.

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29/transition.py BETA M OUT.json
    PYTHONPATH=. ./venv/bin/python -I prompts/inelastic-reflection/source/planning-probes-2026-10-29/compare.py DIR 2 3

- **`transition.py`** runs one history and records, after the run:
  - every turning point (N, kind, φ/M, T_J) with φ_min/M, the minimum of V + Aρ̃_c with the full
    trace ρ_R,E(Σ + f_m);
  - per e-fold of N, averages over the dense output: H_J/H_E and its rms, π²/6M_P², ln A,
    ρ_kin/ρ_R and BBN's r;
  - r averaged over each cell of the stored z grid, and r at the stored samples;
  - today's φ₀/M, the tracking diagnostic and the seed's H₀ residual.
- **`compare.py`** lays the three M side by side per β. Its output is **`compare.out`**; the run
  times, steps, turning points and memory are **`runs.out`**.
- The six histories ran as four detached chains on 2026-10-29 from 00:16 GMT, each in 1–3 minutes
  (0.13–0.49 million accepted steps, at most 363 MB). The JSON records are in the git-ignored
  `var/probes/transition-2026-10-29/`.

What it shows is in the orchestrator's report to the user of 2026-10-29.
