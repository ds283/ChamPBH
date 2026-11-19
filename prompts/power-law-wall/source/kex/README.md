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

## The min(rise, fall) form (power-law-wall prompt 07b2, 2026-11-18)

**Prompt:** [`07b2-measure-the-min-form.md`](../../07b2-measure-the-min-form.md) · **Log:**
[`logs/07b2-measure-the-min-form.md`](../../logs/07b2-measure-the-min-form.md) · **Outputs:**
[`.documents/power-law-wall/kex-07b2/`](../../../../.documents/power-law-wall/kex-07b2/) · **Record:**
[`.documents/power-law-wall-verification.md`](../../../../.documents/power-law-wall-verification.md) §8

Research, as 07b; 07b's files above are imported and never changed. The form (README §6.13): for ln R > 0,
f = 1 − exp(−min(y_rise, y_fall)), y_rise = G(ln R − R₀)₊^P(1 + Q ln L)₊ and y_fall = A/(ℓ − ℓ₀) in the new ℓ where the
chain has a solution and ℓ > ℓ₀, +∞ otherwise; its variants (v-L) (the fall A/(ℓ − ℓ₀ − C ln L)), (v-A) (A = 2.69),
(v-J) (A and ℓ₀ shared by n = 1 and 2) and (vi) (a soft minimum, proposed here). Nothing here is a ruling: U13 is the
user's.

| file | what |
|---|---|
| `minform.py` | the library: the reference implementation `fraction(ell, L, ln_R, n, params)` (§1.8), the variants and their starts, the points (07b's `fits.points()` and this prompt's new ones), the selections S1–S3, the fit (`curve_fit` on `fraction` mapped over the points), and the other laws for the like-for-like tables |
| `minform_reproduce.py` | §1.1: 07b's (iii) and (iv) re-fitted by its own forms against `kex-07b/fits.json`; (v) on the orchestrator's scratch selection against README §6.13 |
| `minform_fits.py` | §1.2–§1.4 (`base`) and §1.5's refits (`new`): every variant on every selection with four starts and standard errors, the like-for-like tables, leave-one-set-out; writes `minform.json` |
| `minform_predict.py` | §1.5: chooses the new points and writes `prediction.json` with (v)'s S3 constants and the predicted f, before any point runs; refuses to overwrite it |
| `minform_stage2.py` | §1.5: runs the points of `prediction.json` by `stage2_kex.run_point_kex` and `stage2_kex.schedule` (`points`), and validates three of them (`validate`) |
| `minform_prediction.py` | §1.5: the new points against the prediction, per group; the sharpness check (A ± 10%) |
| `minform_edges.py` | §1.6: the crossing, the cap, ln R → 0⁺, the chain's 100-iteration limit |
| `minform_paths.py` | §1.7: every exponential history of 07b's path map under each variant; the drained share beside v1 and 07b's candidates |
| `minform_ref.py` | §1.8: `fraction` against every fitted value in `minform.json`, bit for bit |
| `minform_figs.py` | §1.9: the three figures in `fig/` |

**How to run**, in this order, each with `python -B` (no bytecode beside the copies). From this directory with
`../../../../venv/bin/python -B`: `minform_reproduce.py > …/kex-07b2/reproduce.txt`; `minform_fits.py base >
…/fits.txt`; `minform_predict.py > …/prediction.txt` (once: it refuses to overwrite `prediction.json`);
`minform_stage2.py points > …/stage2_points.txt`, then `validate > …/stage2_validate.txt`; `minform_prediction.py >
…/prediction_scored.txt`; `minform_fits.py new > …/refit.txt`; `minform_ref.py > …/reference.txt`; `minform_figs.py`.
From the repository root with `PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/…` (they import
production): `minform_edges.py > .documents/power-law-wall/kex-07b2/edges.txt` and `minform_paths.py >
.documents/power-law-wall/kex-07b2/paths.txt`. No script uses Ray, a store or the registry; the stage-2 points run under
07b's scheduler (at most four at once, one at a time above a one-minute load of 24, each stopped at 30 minutes).

**Results (2026-11-18).** The full record is `.documents/power-law-wall-verification.md` §8. In brief:

- **The reproduction.** 07b's (iii) and (iv) at n = 1 (both selections) and n = 2 re-fitted to `fits.json`'s printed
  digits; (v) on the orchestrator's scratch selection to README §6.13's (G 0.0180, R₀ 1.610, P 1.546, Q 1.008, A 2.42,
  ℓ₀ 1.243; 0.071 / 0.046).
- **The fits.** On S3 (ln(V₀/M⁴) ≤ 10) with the new points, (v): n = 1 A 2.497 ± 0.074, ℓ₀ 1.194 ± 0.063; n = 2 A 2.528 ±
  0.076, ℓ₀ 1.183 ± 0.072; max |res| 0.074 | 0.064 (the fall 0.015 | 0.015). (v-L)'s C is 0.021 ± 0.032 | 0.008 ± 0.039;
  (v-A) costs 8% in SSE; (v-J) A 2.514, ℓ₀ 1.186; (vi)'s s runs off (the sharp minimum). On S1 the off-curve corners
  dominate every law (0.41–0.62). The rise's constants are the same in every variant.
- **Like for like** (n = 1 | n = 2, S3): v1 in the old chain 0.098 | 0.125, in the new 0.743 | 0.638; 07b's (i) 0.350 |
  0.355; (iii)/(iv) 0.070–0.107 | 0.071; (v) 0.084 | 0.056 on 07b's points.
- **The prediction** (`prediction.json`, 01:07:44, before the first point at 01:07:49): 36 new points (laboratory curve
  n = 1 and 2, L-M1e-16, twelve held out) to max 0.071, rms 0.024; the fall points to 0.025 | 0.023. A × 1.1 doubles the
  fall's misses with the field back out (0.010 → 0.045 at n = 1). Every point validated by energy (≤ 3.4e-9); three by
  the classical limit (≤ 1.1e-5), the Born limit off the curve (≤ 8.4e-4) and convergence (≤ 7.0e-4). The refits move A
  and ℓ₀ by less than one standard error.
- **The edges.** The crossing sits at ℓ_new 1.84–1.98 everywhere, predicted peak 0.958–0.979; no (v) value exceeds 0.95
  at a fitted point; f = 0 for 0 < ln R ≤ R₀ where R·S(L) gives up to 1.0e-2 at ln R = 0; the chain's 100-iteration limit
  changes f by exactly 0 (the fall is infinite there).
- **The paths.** K_X/K_v1 0.93–1.01 for (v), 1.00–1.01 for (v-A); 07b's candidates' shares recomputed equal to its
  `paths.txt`.
- **The reference.** `fraction` equals all 3595 fitted values in `minform.json` bit for bit.
