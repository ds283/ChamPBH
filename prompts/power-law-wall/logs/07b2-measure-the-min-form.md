# Log 07b2 — measure the min(rise, fall) form

**Subject:** the exponential's one-loop law as f = 1 − exp(−min(y_rise, y_fall)), the rise G(ln R − R₀)₊^P(1 + Q ln L)₊
and the fall A/(ℓ − ℓ₀) in the new ℓ, and its variants (v-L), (v-A), (v-J), (vi), at n = 1 and 2, on three selections
like for like beside v1 and 07b's (i)–(iv); errors and leave-one-set-out; 36 new stage-2 points predicted before they
ran; the crossing, the cap and the edges; the paths; the reference implementation (P91–P98).
**Commit:** this commit, on branch `power-law-wall`, on top of `dec7793`
**Date:** 2026-11-18, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Form holds (§4). Nothing is ruled: U13 is the user's.
- **The reproduction:** 07b's (iii) and (iv) at n = 1 (both selections) and n = 2 to `fits.json`'s printed digits;
  README §6.13's (v) to its printed digits.
- **The fits:** on S3 with the new points (v) A 2.497 ± 0.074 | 2.528 ± 0.076, ℓ₀ 1.194 ± 0.063 | 1.183 ± 0.072
  (n = 1 | n = 2); the fall's L term C 0.021 ± 0.032 | 0.008 ± 0.039, within one standard error of 0; max |res|
  0.074 | 0.064, the fall 0.015.
- **The prediction:** 36 points, written 01:07:44, first run 01:07:49; residuals max 0.071, rms 0.024; the refits move A
  and ℓ₀ by less than one standard error.
- **Issues:** none opened. No code, test, config or golden changed.
- **Suites:** at the base 26, 783 (226 + 557), 175, 312, 79, in full; after, the same by discovery. Only the two worktree
  failures failed.

## 1. What shipped

- **`prompts/power-law-wall/source/kex/`** (new files beside 07b's, which are imported and unchanged): `minform.py` (the
  library and the reference implementation `fraction`), `minform_reproduce.py`, `minform_fits.py`, `minform_predict.py`,
  `minform_stage2.py`, `minform_prediction.py`, `minform_edges.py`, `minform_paths.py`, `minform_ref.py`,
  `minform_figs.py`, each with a docstring giving its method and command; `fig/f_vs_lnR.png`, `fig/f_vs_ell.png`,
  `fig/prediction.png`; the README's new section "The min(rise, fall) form".
- **`.documents/power-law-wall/kex-07b2/`** (new): `reproduce.txt`, `fits.txt`, `prediction.txt`, `prediction.json`,
  `stage2_points.txt`, `stage2_validate.txt`, `prediction_scored.txt`, `refit.txt`, `edges.txt`, `paths.txt`,
  `paths_candidates.txt`, `reference.txt`, `minform.json`.
- **`.documents/power-law-wall-verification.md` §8** (appended; nothing above it changed).
- **The board:** row 07b2, the gate Form, the baselines, the status line and its history. No issue opened, so §3/§4 and
  `.documents/OPEN_ISSUES.md` are unchanged.

## 2. Deviations from the prompt

**The prompt's §5 rulings**, each as ruled:
- **Ruling 1 — the worktree** (*STRUCTURALLY REQUIRED*): every command from the worktree's root or a directory in it,
  `./venv/bin/python`, `PYTHONPATH=.` where production is imported; nothing read or written in the main checkout; `venv`
  never staged; `/tmp/ray-cpbh` never named.
- **Ruling 2 — records stay records** (*STRUCTURALLY REQUIRED*): 07b's scripts (`fits.py`, `kexchain.py`, `rechain.py`,
  `stage2_kex.py`, `paths.py`), crossover's and `source/wall/` were imported, never written; every run used `python -B`
  (the spawned stage-2 workers inherit it); no `__pycache__` appeared (checked after).
- **Ruling 3 — the machine** (*IMPLEMENTATION CHOICE, ruled*): the points ran under `stage2_kex.schedule` (at most four at
  once, one at a time above a one-minute load of 24, 30 minutes per point). The 36 points started at loads 8.0–13.5 and
  took 2–32 s; the validation jobs started at ≤ 23.7 and the load reached 26.1 during them. No point was stopped.
- **Ruling 4 — the prediction fixed first** (*STRUCTURALLY REQUIRED*): `prediction.json` was written at 01:07:44 by a
  script that refuses to overwrite it; the first point started at 01:07:49; no point was added later.
- **Ruling 5 — the two worktree failures** (*IMPLEMENTATION CHOICE, ruled*): `test_registered_run …
  test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` and `test_registry_config …
  test_roots_and_distributions` failed at the base, and nothing else.
- **Ruling 6 — production findings recorded, not fixed** (*IMPLEMENTATION CHOICE, ruled*): none needed an issue (§6).
- **Ruling 7 — no option chosen** (*STRUCTURALLY REQUIRED*): §7 and verification §8.8 state each option's evidence.

**This prompt's deviations:**
- **D1 — the base suites in one background job** (*IMPLEMENTATION CHOICE*), serially, ComputeTargets in two halves and
  `tools/tests` alone, with `uptime` at each start. §1.1–§1.4's scripts (about one CPU-second each) were first run while
  the last three suites ran; their committed outputs were all re-made after the suites, and `prediction.json` was written
  after the suites ended (01:07:32).
- **D2 — the selections read** (*IMPLEMENTATION CHOICE*). The universe U_n is the points at n with the field back out and
  ln R > 0.3. S3 is ln(V₀/M⁴) ≤ 10 over every set and regime, in place of S1's set-based exclusions (it keeps the
  `curve` sets, whose ln(V₀/M⁴) is −119 to −249). S2 at n = 2 is S1 (prompt §1.3). "Dropped" is U_n less the selection.
- **D3 — the grouping of v1 and 07b's candidates** (*IMPLEMENTATION CHOICE*): as 07b's tables, ℓ ≥ 1, ℓ < 1, no chain;
  v1 in the old chain (its own), (i)–(iv) in the new. Column (d) is the identical point set for every law.
- **D4 — 07b's (i) and (ii) where ℓ + B₀ − C ln L ≤ 0** (*IMPLEMENTATION CHOICE*). The first refit with the new points
  scored (i) at 84.7: `fits.py`'s evaluation of (i)/(ii), which the tables copy, is unguarded there, which no point of
  07b's reached and 07b2's L-M1e-16 points (L ≈ 105, ℓ_new ≈ 1) do. The tables take f = 1 there, as 07b's `paths.law`
  does; 07b's own numbers on its points are unchanged (reproduced, §8.3).
- **D5 — (vi), the soft minimum** (*IMPLEMENTATION CHOICE*, the prompt's optional proposal): y = m(1 + (m/M)^s)^{−1/s}, (v)
  as s → ∞; its reason, whether the peak is rounded. Where it tends to (v), s is not determined (30–129 among the starts,
  infinite standard errors); the multi-start comparison sets s apart and prints its range.
- **D6 — the leave-one-set-out groups** (*IMPLEMENTATION CHOICE*): 07b's labels grouped by curve as the prompt lists
  them, with crossover's `curve-119` and `curve-129` (ln(V₀/M⁴) −119.16, −129.27) joined to L-M8e-18 and L-M1e-16, whose
  curves they are; `2197` and `lab` kept apart as listed. At n = 2 leaving out `n2-grid` leaves four points, not fittable,
  so a leave-one-L-out of each grid is given beside.
- **D7 — the new points' choices** (*IMPLEMENTATION CHOICE*): L-M1e-16 as the low-M curve (its region held three measured
  rows, none back out); targets denser across the peak; the held-out distance d = √((Δ ln R/2.5)² + (Δ ln L/0.2)²) ≥ 1. A
  dry run of `minform_predict.py` into the scratchpad (before `prediction.json` existed) failed the distance assertion at
  two held-out points, (n = 1, L 2.5, ln R 12.5) and (n = 2, L 3, ln R 10), which were moved to (1.6, 11) and (3, 6.5).
- **D8 — the Born limit off the curve** (*STRUCTURALLY REQUIRED*), as 07b's D4: on the curve at ℓ_new ≤ 4, k_IR = 10 m_ta
  lies above k_ex; the limit was taken at each validation point's L where ℓ_new = 6 (`stage2_kex.task_validate`'s
  `born-offcurve`).
- **D9 — four starts per fit** (*IMPLEMENTATION CHOICE*): the orchestrator's, v1's gap constants with A = 2.69, and two
  others. On S1 at n = 1 the optimum depends on the start for (v), (v-A) and (vi) (a second optimum, or overflow); it is
  printed.
- **D10 — the fit's model is the reference** (*IMPLEMENTATION CHOICE*): `curve_fit` minimises `minform.fraction` mapped
  over the points, and `minform.json` stores the model's own values at the optimum; §1.8's bitwise check is then of the
  constants' round trip through JSON and of the function, at every fitted point.
- **D11 — the figures' curves** (*IMPLEMENTATION CHOICE*): the law curves in `fig/` are `minform.fraction` and
  `fits.v1_fraction` evaluated at the constants in `minform.json` and production's; their points are in `fits.txt`,
  `refit.txt` and `stage2_points.txt`. No figure tabulates its curve elsewhere.
- **D12 — the cap's domain** (*IMPLEMENTATION CHOICE*): L in [0.5, 134] (the S table's range), 400 values; the maximum
  falls at its upper end.
- **D13 — `np.float64(…)` in the "PT" lines** (*UNINTENDED DRIFT*): `stage2_points.txt`'s machine lines write `balance`,
  `vout` and `drift` as `np.float64(…)` reprs. The values are exact and nothing reads those three fields; the run was not
  repeated, since the prediction is fixed and the points are the record.
- **D14 — bugs in my own scripts** (*UNINTENDED DRIFT*), each failing or visibly wrong on its first run and fixed before
  the committed output: `minform.fit` did not catch `OverflowError`; `minform_edges.py` first took `AbstractModel` as the
  class, not the module (07b's D14 again); `minform_paths.py` first parsed 07b's header line; `minform_fits.py` first
  compared (vi)'s unbounded s among the starts; `minform_prediction.py` first lacked the per-branch groups that show the
  sharpness (§3), and was re-run with them.
- **D15 — black after the first runs** (*IMPLEMENTATION CHOICE*). Every `minform*.py` is black-clean. The quick scripts
  were all re-run after the last formatting: `minform.json` byte-identical, the text outputs identical but for `uptime`.
  `minform_predict.py` and `minform_stage2.py` were last changed before they ran.

## 3. Verification performed

| suite | at the base `dec7793` (time, load at start) | after (discovery) |
|---|---|---|
| CosmologyModels | 26 OK (69 s; 13.6) | 26 |
| ComputeTargets `test_[a-f]` | 226 OK (152 s; 9.1) | 226 |
| ComputeTargets `test_[g-z]` | 557 OK (550 s; 6.2) | 557 |
| ObjectFactories | 175 OK (158 s; 5.3) | 175 |
| RunDrivers | 79, 1 failure (ruling 5) (27 s; 19.4) | 79 |
| tools | 312, 1 failure (ruling 5) (322 s; 23.1) | 312 |

No count fell. No file the suites import changed. `black --check` on the ten new scripts: clean.

## 4. The gate Form, item by item

- **The form and its variants fitted at n = 1 and 2 on each selection, with parameters, errors and residuals beside v1's
  and 07b's candidates on the identical point sets:** `fits.txt` and `refit.txt` (every fit, every start, the standard
  errors, the tables (a)–(d) on each selection and on its dropped points, leave-one-set-out); verification §8.2, §8.3.
- **07b's constants reproduced by the fitting code:** `reproduce.txt` (all 36 constants of (iii) and (iv) at three
  selections; README §6.13's (v)); verification §8.1.
- **The new points validated as crossover's stage 2 was and predicted before being fitted:** `prediction.json` at
  01:07:44 before 01:07:49; energy at all 36 (≤ 3.4e-9); classical (≤ 1.1e-5), Born (≤ 8.4e-4, off the curve) and
  convergence (≤ 7.0e-4) at three; `prediction_scored.txt`; verification §8.4.
- **The paths re-mapped:** `paths.txt`, `paths_candidates.txt`; verification §8.6.
- **The reference implementation equal to the fit:** `reference.txt`, 3595 of 3595 bitwise; verification §8.7.
- **Nothing ruled by the agent:** ruling 7.

## 5. The deliberate-breakage record

- **A law scored on the wrong chain** (prompt §3): v1's constants read in the new chain miss by 0.743 | 0.638 (n = 1 |
  n = 2, S3) where v1 in its own chain misses by 0.098 | 0.125; a row of every like-for-like table.
- **A 10% error in A** (prompt §3): the prediction's fall points with the field back out miss by 0.010 | 0.014 at the
  committed constants, 0.045 | 0.049 with A × 1.1, 0.040 | 0.037 with A × 0.9 (`prediction_scored.txt`).
- **07b's constants:** a re-fit that differed in any printed digit would print `MISMATCH` and `NOT REPRODUCED`
  (`minform_reproduce.compare`); none did.
- **The prediction's integrity:** `minform_prediction.py` asserts that each stored predicted value re-evaluates bitwise
  from the stored constants at the measured point's re-run chain; `minform_ref.py` that the stored constants equal
  `minform.json`'s (v) on S3 without the new points.

## 6. Observations not acted on

1. **The predicted maximum grows with L** and sits at the cap search's upper end (0.9725 | 0.9813 at L = 134); the
   crossing is at ℓ_new 1.84–1.98 everywhere.
2. **Near the peak the measured f exceeds (v)** where the field did not come back out (mean +0.014, up to +0.029 at
   L-M1e-16 ℓ_new 1.8, f 0.993 against 0.964). 15 of the 36 new points did not come back out, all predicted at f ≥ 0.83,
   so the fits, which keep only points back out, see little of the peak.
3. **The largest residuals are on the rise at small L** (grid L = 1.3, the laboratory curve at ℓ_new 0.55–0.65, held-out
   #1 and #8): up to 0.074 (n = 1) and 0.064 (n = 2) fitted, 0.071 predicted. The rise is v1's gap form; it is not a
   function of ℓ.
4. **v1 in its own chain still describes S3** to 0.098 | 0.125; what retires it is R15's k_ex, not its residuals on the
   histories' domain.
5. **07b's issue `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`:** under every
   variant here f is the same with or without the chain where the iterations exceed 100 (ℓ_new ≤ 0.31 < ℓ₀); the stored
   regime label would still differ. The issue is not narrowed here.
6. **At n = 2 the rise rests on one grid:** without `n2-grid` four points remain.
7. **07b's `fits.py` evaluates (i) and (ii) unguarded** where ℓ + B₀ − C ln L ≤ 0 (D4); its `paths.py` guards it. A record;
   not changed.
8. **The "PT" lines' `np.float64(…)` reprs** (D13).

## 7. State handed to the next prompt

**For U13** (the user's; nothing here is a recommendation). The evidence per option on the same points (n = 1 | n = 2,
S3 with the new points: max |res| over all points, and on the fall where the law has one; verification §8.3, §8.8):
- *v1 in the old chain:* 0.098 | 0.125 (ℓ ≥ 1: 0.074 | 0.125). It reads ℓ in the old k_ex, which R15 retires; read in the
  new chain it misses by 0.743 | 0.638.
- *07b (i)/(ii)* (v1's layout, plateau below ℓ = 1): 0.350 | 0.355, from the plateau.
- *07b (iii)/(iv)* (the gap's form below a split, or min with B₀, C): 0.070–0.107 | 0.071; their one-loop branch has
  B₀ ≈ −1.2, C ≈ 0, i.e. (v)'s fall.
- *(v):* 0.074 | 0.064 (fall 0.015 | 0.015, rise 0.074 | 0.064, gap 0.047 | 0.057); G 0.0202 ± 0.0029 | 0.0141 ± 0.0040,
  R₀ 1.729 ± 0.133 | 1.205 ± 0.265, P 1.495 ± 0.053 | 1.731 ± 0.104, Q 1.035 ± 0.072 | 0.908 ± 0.091, A 2.497 ± 0.074 |
  2.528 ± 0.076, ℓ₀ 1.194 ± 0.063 | 1.183 ± 0.072; leave-one-set-out A 2.458–2.586, ℓ₀ 1.130–1.219 (n = 1); predicted 36
  new points before they ran to 0.071 (rms 0.024; the fall points 0.025 | 0.023); the refits moved A and ℓ₀ by less than
  one standard error; drained share 0.93–1.01 of v1's on the nine mapped exponential histories.
- *(v-L):* C 0.021 ± 0.032 | 0.008 ± 0.039, within one standard error of 0, residuals as (v) (on S1, with the corners,
  C 0.36 ± 0.06).
- *(v-A):* A = 2.69 raises the SSE by 8%, the fall's miss to 0.028 | 0.029, ℓ₀ 1.042 | 1.044; drained share 1.00–1.01.
- *(v-J):* A 2.514 ± 0.052, ℓ₀ 1.186 ± 0.047 shared; residuals on S3 as (v).
- *(vi):* indistinguishable from (v) off the corners (s unbounded).
- *Which points the law must describe:* every option misses the off-curve corners (ln(V₀/M⁴) > 10) by 0.6–0.7; on S1,
  which keeps them, (v-L) and (vi) do best (0.41–0.42). No history has ln(V₀/M⁴) above 3.3.
- *The cap:* not needed by any residual; measured f reaches 0.993 near the peak with the field not back out.
- *ln R → 0⁺:* (v) gives f = 0 for 0 < ln R ≤ R₀ (1.73 | 1.20), where R·S(L) at ln R = 0 is up to 1.0e-2: the same step as
  v1's gap below 1.56.

**For 07c**, if the user rules this form: `minform.fraction(ell, L, ln_R, n, params)` in
`prompts/power-law-wall/source/kex/minform.py` is the reference for the gate Consistent; the constants of every variant on
every selection, without and with the new points, at full precision, with their fitted points and values, are
`.documents/power-law-wall/kex-07b2/minform.json` (keys `with_new` / `without_new`, then `n1/S3/v`, `n2/S3/v`,
`n12/S3/v-J`, …). Under (v) the chain's existence below ℓ₀ does not change f (§6 item 5). U11 and U12 stand as ruled.
