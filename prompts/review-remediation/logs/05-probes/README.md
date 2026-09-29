# Prompt 05's scratch probes

These are the scripts behind the `[scan05]` tag in `.documents/numerical-methods-for-paper.md` and
the scratch-probe figures in `../05-kicking-function-and-eos-hygiene.md`.

- **Written by** prompt 05's implementation agent and run on `89bd52e` plus prompt 05's diff,
  which landed as `bb840f6`.
- **Kept** at the user's request on 2026-09-30. They are copied here unchanged; they are
  diagnostics, not tests.

Run each one from the repository root:

```bash
PYTHONPATH=. ./venv/bin/python prompts/review-remediation/logs/05-probes/<script>.py
```

| Script | What it prints |
|---|---|
| `probe05.py` | Spline peaks at 200, 1000 and 5000 points per decade; the e⁺e⁻ profile; ∫Σ d ln T (Simpson, trapezoid, `quad`); w at and beyond the table's ends; the 2 MeV freeze; the CSV's own statistics; the ρ_R witness at two `max_step`s |
| `break05.py` | The breakage check. Pass `freeze` or `shift` as the argument; it reports the failures of `test_kicking_function` |
| `witness_scan.py` | The ρ_R witness from 2×10⁴ GeV at 37 end temperatures, 10 TeV down to 10 keV |
| `sigma_implied.py` | Σ from the table against Σ_g = 4 − (4 + d ln g_ρ/d ln T)/(1 + ⅓ d ln g_s/d ln T), and the witness at 31.6 GeV and 178 MeV |
| `sigma_implied2.py` | Σ_g peaks from the spline and jax classes, and a comparison at chosen temperatures |
