# Planning probes, 2026-10-28 (prompt 02)

Read-only. The probe was run at `b30f7bd` by the planner of prompt 02, from the repository root, at a
load of about 5:

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-28/turning_points.py 1.2 2 5 25

- **`turning_points.py`**: one untuned history per β, at the model's seed realization.
  - Setup: M = 0.5 M_P, n = 1; φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV; integrated to T_CMB; atol = rtol = 1e-8;
    main.py's axis; α = 0.
  - It counts the sign changes of π across accepted steps, with `first_bounce`'s strict convention, as
    wall-side (π − → +) and plateau (π + → −) turning points.
  - It also counts the steps whose interpolant, sampled at 17 points, has two or more sign changes.
  - It prints the T_J (MeV) of the first three and last three wall-side turning points.
- **`turning_points.out`**: its output. Prompt 02 §0 tabulates it.
