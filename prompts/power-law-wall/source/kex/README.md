# k_ex re-measured (power-law-wall prompt 07b)

**Campaign:** [`power-law-wall`](../../README.md) · **Prompt:** [`07b-remeasure-k-ex.md`](../../07b-remeasure-k-ex.md) ·
**Log:** [`logs/07b-remeasure-k-ex.md`](../../logs/07b-remeasure-k-ex.md) · **Outputs:**
[`.documents/power-law-wall/kex-07b/`](../../../../.documents/power-law-wall/kex-07b/) · **Record:**
[`.documents/power-law-wall-verification.md`](../../../../.documents/power-law-wall-verification.md) §7

Research. Nothing here changes production code. By the user's rulings R13–R17 (README §6.12) k_ex is
½E^{1/2}|V‴/V″| on every potential and at every use (Erickcek et al., arXiv:1310.5149, Eq. (64)), where the code takes
λE^{1/2}/2 with λ = −d ln V/dφ. This directory re-reads, and where needed re-measures, everything that k_ex labels or
classifies: the one-loop chain and its regimes, crossover's and 02's stage-2 points, the exponential's one-loop law, the
power wall's law, the spectrum tables and x_*, the mapped paths, and every use in the tree. Nothing here is a ruling: the
exponential's new law is U13, the user's.

## Files

| file | what |
|---|---|
| `crossover/` | copies of crossover's `walls.py`, `born.py`, `stage2.py`, `stage2_validate.py`, `fit_law.py` at `18add05`, each with a header naming its source and commit; byte for byte below it |
| `wall/` | copies of 02's `walls.py` (`PowerWall`), `born.py`, `stage2.py`, `powerscan.py`, `stage1a_power.py`, `spectrum_table.py`, `fit_fn.py` at `c798d02`, likewise (the last three's imports need the first four) |
| `kexchain.py` | `chain(w, R, form)`: born.chain with the rate a choice, `"lambda"` (−d ln V/dφ) or `"v3"` (\|V‴/V″\|), and three broken forms for the breakage record; the closed forms of new over old; `production_v3_classical` and `production_v3_turnaround`, a scratch evaluation of P75 through the production hooks; the loaders of the two copy directories |
| `definition.py` | §1.1: Eq. (64) numerically at the chain's φ_ta, and the factor its reduction leaves out; Eq. (71) on the power wall; the exponential's ratio and Eq. (66); φ_t, ω_t, ln R bitwise; the breakage record |
| `precision.py` | §1.2: the v3 chain against the production-hook evaluation at 40 mapped candidates |
| `rechain.py` | §1.3: every stage-2 row of crossover and 02 re-chained in both forms; bitwise against the copied born.chain; the record; the moves; ℓ_new − ℓ_old |
| `powerlaw.py` | §1.6: the power wall's fraction at every 02 row under both forms' labels, through `fit_fn.fraction_power` and production's `InversePowerModel._drain_fraction` |
| `stage2_kex.py` | §1.4: `run_point_kex`, crossover's run_point with the grid from the chosen form's k_ex; modes `reference`, `reference-kmax`, `new`, `validate`, `born-offcurve` |
| `fits.py` | §1.5: the reproduction of crossover's constants; candidates (i)–(iv) at n = 1 and 2 beside v1; writes `fits.json` |
| `spectrum_kex.py` | §1.7: k_ex/ω_t and x_*; the three tables in the new x and their literals; the exponential's shape over L; R9 on the power wall |
| `paths.py` | §1.8: crossover's cascades, 02's maps and 04's sample re-chained; the drained share per history |

The inventory (§1.9) is `.documents/power-law-wall/kex-07b/inventory.md`; it is a document, compiled with `git grep`, not
a script's output.

## How to run

Every script runs with `python -B` (no bytecode beside the copies). From this directory, with
`../../../../venv/bin/python -B`: `rechain.py`, `stage2_kex.py MODE`, `fits.py`. From the repository root, with
`PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/…`: `definition.py`, `precision.py`, `powerlaw.py`,
`spectrum_kex.py MODE [LITERALS]`, `paths.py` (they import production). Each script's docstring gives its command and
its output file under `.documents/power-law-wall/kex-07b/`. Order: `rechain.py`; `definition.py`, `precision.py`,
`powerlaw.py`; `stage2_kex.py reference`, then `new`, `validate`, `reference-kmax`, `born-offcurve`; `fits.py` (reads
`stage2_new.txt`); `paths.py` (reads `fits.json`); `spectrum_kex.py kex`, `table`, `shape`. 03's gate Law reference was
re-run unchanged from 02's own directory: `cd prompts/power-law-wall/source/wall && PYTHONPATH=../../../..
../../../../venv/bin/python -B fit_fn.py gate > ../../../../.documents/power-law-wall/kex-07b/gate_law_rerun.txt`.

No script uses Ray, a store or the registry. Stage-2 points run at most four at once, one at a time while the
one-minute load is above 24, each stopped at 30 minutes (none was); the spectrum pool is four processes.

## Results (2026-11-17)

The full record is `.documents/power-law-wall-verification.md` §7. In brief:

- **The definition.** Eq. (64) at the chain's φ_ta with V_D ∝ (V″)² equals ½E^{1/2}|V‴/V″| to 4e-9 at twelve points;
  the code's own dressing m⁴ℓ/32π², with ℓ varying along the approach, gives the factor (1 − 1/4ℓ)^{1/2} (0.90 at
  ℓ = 1.36, 0.99 at ℓ = 10), which the reduction leaves out. Eq. (71) holds to 4.4e-16 on the power wall; on the
  exponential new/old is the closed form to 2.2e-16, and Eq. (66) is λE^{1/2}/2 exactly, the leading term. φ_t,
  ln ω_t² and ln R are bitwise the same in both forms at nine mapped candidates. The three broken forms each fail E64,
  E71 and E66.
- **The production hooks.** The v3 chain equals the scratch evaluation through the hooks to 1.9e-11 at 40 candidates.
- **The re-chaining.** 371 rows; the lambda chain equals born.chain bitwise at all; the record reproduced at 199 rows
  to the printed digits. 33 exponential rows move (20 of crossover's 107 at n = 1, 13 of 63 at n = 2) and 52 of 02's
  201, all gap → one-loop. ℓ_new − ℓ_old is 0.035–0.10 at L ≥ 100, 0.24 at L ≈ 52 and 0.68–2.3 at L < 10 (n = 1); on
  the power wall 1.10 (n = 1) and 0.69 (n = 2) at large x, ln 3 and ln 2, and more near the floor.
- **Stage 2.** The grid's reference: the three points give 0.71722, 0.82626 and 0.15623 against 0.7171, 0.8264 and
  0.1594, within ±6e-3; the gap point's −3.2e-3 is the record's own convergence in k_max (both forms give 0.15623 at the
  same k_max). Fifteen new points (eleven at n = 1, four at n = 2), energy to ≤ 6.3e-10; classical limit to 2.7e-6,
  Born limit to 8.4e-4 (off-curve at ℓ_new = 6), convergence to 9.2e-4.
- **The laws.** Crossover's constants reproduced to their digits. In the new ℓ the plateau below ℓ = 1 is 0.16–0.82,
  not 0.9; (iii) and (iv) put the gap's form below ℓ_g ≈ 1.55; the residuals are in the verification document.
- **The power wall.** f_n(ln R) equal at all 201 rows under both forms' labels; 03's gate re-run identical.
- **The tables.** k_ex/ω_t = 0.5163952511757223, 1.0606601717798214, 0.816496580927726; X_HI 3.5, 3, 3; x_p 0.6194,
  0.1553, 0.2534; κ_hi 4.343, 6.546, 5.467; R9's error on the power wall ≤ 3.4e-7 in ln|β|²; x_* = 0.005616.
- **The paths.** 17 377 candidates, 327 change regime (all on the exponential, gap → one-loop); the power wall's drained
  share is unchanged (1.000000) on every history.
