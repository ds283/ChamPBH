# Prompt 06's scratch probes

These two scripts print the figures in `.documents/review-remediation-verification.md` that no
test prints. They were written by prompt 06's implementation agent and run on `01e5975` on
2026-09-30. They are kept, following the precedent of `../05-probes/`. They are diagnostics, not
tests.

Run each one from the repository root:

```bash
PYTHONPATH=. ./venv/bin/python prompts/review-remediation/logs/06-probes/<script>.py
```

| Script | What it prints |
|---|---|
| `probe06_deriv.py` | The worst \|Δ(d ln g_s/d ln T)\| on `derivative_test_grid_GeV()`, against the central difference and against the jax class. The same quantities that `test_temperature_law` cases 3 and 4 assert ≤ 1e-6 |
| `probe06_knots.py` | Stored samples per decade of T_J at fixed field, at 250 per decade of Einstein-frame 1 + z. Given for [0.02, 5] MeV, the BBN spline domain and PRyMordial's working range |
