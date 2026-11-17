# Log 07b — re-measure everything that k_ex defines

**Subject:** k_ex = ½E^{1/2}|V‴/V″| (R13–R17) against Erickcek et al. and through the production hooks; every stage-2
point of crossover and 02 re-chained; new stage-2 points where the exponential's regime moved; the candidate laws for
`one-loop-exponential-v2` at n = 1 and 2; the power wall's law shown unchanged; the three spectrum tables and x_* in the
new x; the mapped paths; the inventory for 07c (P73–P90).
**Commit:** `0223d50`, on branch `power-law-wall`, on top of `e4ae58b`
**Date:** 2026-11-17, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`; the inventory
compiled by a subagent from its brief, D10)
**Result:** **COMPLETE.** The gate Remeasure holds (§4). Nothing is ruled: U13 is the user's.
- **The definition** reproduces Eq. (64) (4.0e-9), Eq. (71) (4.4e-16) and Eq. (66) (its leading term exactly); φ_t,
  ln ω_t² and ln R are bitwise unchanged; the reduction's left-out factor is (1 − 1/4ℓ)^{1/2}, measured.
- **The old chain reproduces the record:** bitwise the copied born.chain at 371 rows, every recorded ℓ (199) to its
  printed digits, crossover's fit constants to their printed digits, and note §0.3's refit.
- **33 exponential and 52 power-wall stage-2 rows move from gap to one-loop.** Fifteen new points, the grid's reference
  within ±6e-3 (no stop), three points validated.
- **The candidate laws** (i)–(iv) at n = 1 and 2 beside v1's residuals; v1's plateau misses the new region by up to 0.74.
- **The power wall's law does not move;** its table's error near the floor (R9) is gone in the new x.
- **Issues:** one opened (§6, board §3). No code, test, config or golden changed.
- **Suites:** at the base 26, 783 (226 + 557), 175, 312, 79, in full; after, the same by discovery. Only the two worktree
  failures of note 01's ruling 7 failed.

## 1. What shipped

- **`prompts/power-law-wall/source/kex/`** (new):
  - `crossover/`: copies of crossover's `walls.py`, `born.py`, `stage2.py`, `stage2_validate.py`, `fit_law.py` at
    `18add05`; `wall/`: copies of 02's `walls.py`, `born.py`, `stage2.py`, `powerscan.py`, `stage1a_power.py`,
    `spectrum_table.py`, `fit_fn.py` at `c798d02`. Each has a three-line header naming its source and commit, and is the
    source byte for byte below it (D2);
  - `kexchain.py` (the chain in either form, the closed forms, the production-hook evaluation, the loaders),
    `definition.py`, `precision.py`, `rechain.py`, `powerlaw.py`, `stage2_kex.py`, `fits.py`, `spectrum_kex.py`,
    `paths.py`, each with a docstring giving its method, tolerances and command; `README.md` in 02's form.
- **`.documents/power-law-wall/kex-07b/`** (new): `definition.txt`, `precision.txt`, `rechain.txt`, `powerlaw.txt`,
  `gate_law_rerun.txt`, `stage2_reference.txt`, `stage2_reference_kmax.txt`, `stage2_new.txt`, `stage2_validate.txt`,
  `stage2_born_offcurve.txt`, `fits.txt`, `fits.json`, `spectrum.txt`, `spectrum_tables.txt` (the literals 07c takes),
  `paths.txt`, `paths_candidates.txt`, `inventory.md`.
- **`.documents/power-law-wall-verification.md` §7** (appended; nothing above it changed).
- **The board** (row 07b, the gate Remeasure, §3's new issue, the baselines, the status line and its history) and
  **`.documents/OPEN_ISSUES.md`** (the issue under §1.26; 35 → 36; dated 2026-11-17).
- No figure was made (prompt §1.10 allows at most one per curve and n; the fits are tabled per point in `fits.txt`).

## 2. Deviations from the prompt

**The note's rulings**, each as ruled:
- **Ruling 1 — the worktree only** (*STRUCTURALLY REQUIRED*): every command from the worktree's root or a directory in
  it, `./venv/bin/python`; nothing read or written in the main checkout; `venv` never staged; `/tmp/ray-cpbh` never named.
- **Ruling 2 — records stay records** (*STRUCTURALLY REQUIRED*): crossover's directory and `source/wall/` were read and
  copied, never written. The one script run in place, 02's `fit_fn.py gate`, ran with `python -B`; `git status` shows no
  change there and no `__pycache__` appeared (checked after).
- **Ruling 3 — the machine** (*IMPLEMENTATION CHOICE, ruled*): `stage2_kex.py`'s scheduler starts at most four points at
  once and one at a time while the one-minute load is above 24, prints the load at each point's start and end, and stops a
  point at 30 minutes. Every point started at a load ≤ 21.7; none took longer than 6 minutes.
- **Ruling 4 — the grid's reference** (*IMPLEMENTATION CHOICE, ruled*): all three within the prompt's ±6e-3; no stop (D5).
- **Ruling 5 — the two worktree failures** (*IMPLEMENTATION CHOICE, ruled*): `test_registered_run …
  test_the_script_is_main_py…` and `test_registry_config … test_roots_and_distributions` failed at the base, and nothing
  else.
- **Ruling 6 — production findings recorded, not fixed** (*IMPLEMENTATION CHOICE, ruled*): one §3 issue opened (§6 item 1);
  the rest are §6's observations.
- **Ruling 7 — no option chosen** (*STRUCTURALLY REQUIRED*): §7 and verification §7.10 state each candidate's evidence; no
  candidate is recommended.
- **Ruling 8 — no file but the prompt's §4 paths** (*STRUCTURALLY REQUIRED*): the commit's 43 files are the board,
  `OPEN_ISSUES.md`, the verification document, this log, `source/kex/` and `kex-07b/`.

**This prompt's deviations:**
- **D1 — the base suites in one background job** (*IMPLEMENTATION CHOICE*). The five suites ran serially, ComputeTargets
  in its two halves and `tools/tests` alone, in one detached script waited for by its notification, with `uptime` beside
  each; my cheap probes (rechain, definition, precision) ran during them, and the loads are recorded.
- **D2 — two copy directories and three more copies** (*STRUCTURALLY REQUIRED*). Crossover's and 02's `walls.py`,
  `born.py` and `stage2.py` share names and differ, so each set is in its own directory and a process imports one
  (`kexchain.load`), or loads them under prefixed names (`load_aside`) where it needs both. `spectrum_table.py` and
  `fit_fn.py` import `powerscan`, `stage1a_power`, `stage2` and `born`, so `born.py`, `powerscan.py` and
  `stage1a_power.py` were copied too.
- **D3 — 03's gate run from 02's own directory** (*STRUCTURALLY REQUIRED*). The copy's `MAP_DIR` is a path relative to the
  file, one level too deep from `kex/wall/`; ruling 2 allows a record's script run in place with `python -B`, which is also
  "re-run unchanged" in the literal sense. Its output equals `law-03/gate_law.txt` but for the two `uptime` lines.
- **D4 — the Born limit off the curve** (*STRUCTURALLY REQUIRED*). At the three validation points (ℓ_new 1.23–1.48)
  k_IR = 10 m_ta is 2.3–2.9 k_ex: the Born reduction does not hold (born.run gives −5.5 to −10; stage 2's cut run breaks
  its energy, 1e5). Those runs are kept in `stage2_validate.txt` as measured. The Born limit was then taken at each point's
  L at the ln R where ℓ_new = 6 (`stage2_kex.py born-offcurve`, marked off-curve): 8.3e-4 and 8.4e-4 (tolerance 2e-3).
- **D5 — the gap reference examined** (*IMPLEMENTATION CHOICE*). 2197 A0.01 #512 gave 0.15623 against 0.1594: within
  ±6e-3, outside crossover's own ±2e-3. The added mode `reference-kmax` shows both forms give the same fraction at the same
  k_max (0.15623; 0.15944 against the record's 0.15937 at the old top) and that the fraction converges to 0.1561: the
  record is under-converged in k_max, and the label does not change the measurement.
- **D6 — the bins of §1.4** (*IMPLEMENTATION CHOICE*). "Every range of ℓ_new below 3" read as 0.5-wide bins; the
  region is each curve between the old form's gap entry and R = 1; the points counted are on-curve, crossover's rows
  included. The laboratory curve's three bins each hold four; L-M8e-18's chain begins at ℓ_new ≈ 0.297 (its fixed point
  folds), so its region holds four in [0.297, 0.41]; L-M1e-16's and L-M5e-16's histories reach no chain in their regions,
  so they need none. A first L-M8e-18 target set (0.08, 0.17, 0.26) lay below the fold and was replaced before any run.
- **D7 — the fits' selections and candidate (iv)** (*IMPLEMENTATION CHOICE*). The primary selection is the copied
  `fit_law.py`'s; note §0.3's (its gap exclusions on the one-loop points too) is reported beside, and on crossover's rows
  alone, where it reproduces the planner's numbers. At n = 2 crossover's U15 cut ln(V₀/M⁴) ≤ 10 is applied in every
  group, as its own n = 2 residuals were (v1 then reproduces its 0.125 / 0.102). (iii)'s ℓ_g is a scan from 0 to 2.5 in
  steps of 0.05; the plateau of (i)/(ii) is the mean measured f; (iv) is min(y_rise, y_fall), R7's structure with L kept.
- **D8 — the drained share** (*IMPLEMENTATION CHOICE*). Defined as K_X = Σ_c α_X,c ε_c, ε_c = (E_c/V₀)e^{4N_c} from each
  candidate's recorded ln(E/V₀) and N, the trajectory held fixed; reported as K_X/K_v1 on the exponential and K_new/K_old
  on the power wall, at each history's A (1 for 02's fixed-law maps), w_gate = 1 (all 1632 of 04's candidates record 1).
  The proxy reproduces 04's recorded ΔK/K to 9.3e-6 relative once scaled to their sum (below 1 by the parking's share).
- **D9 — the production search started at 100 M** (*STRUCTURALLY REQUIRED*). From φ = M (02's choice) production returns
  None for 2197's late candidates (V″(M) is already above m_ta² at the first iterate); the root does not depend on the
  start.
- **D10 — the inventory compiled by a subagent** (*IMPLEMENTATION CHOICE*), from a brief giving the categories, the
  search terms and the planner's list; I spot-checked its line references (AbstractModel.py:873, ScalarHistory.py:172 and
  :4273, quanta_spectrum.py's table fields) and amended its `ONE_LOOP_MAX_ITERATIONS` row to name §3's issue.
- **D11 — the literals' x_hi** (*IMPLEMENTATION CHOICE*). `spectrum_table.analyse` reports x_hi as exp(ln x_hi), 3.0000000000000004
  for 3; the literal is the rule's value, from whose logarithm the module rebuilds its nodes. The table mode was re-run
  with that change; nothing else moved.
- **D12 — black after the runs** (*IMPLEMENTATION CHOICE*). Nine scripts were reformatted by `black` after their outputs
  were made. The quick ones (rechain, definition, precision, powerlaw, fits, paths) were re-run after it: identical but for
  the `uptime` lines, and `fits.json` byte-identical. The stage-2 and spectrum outputs are from the scripts before
  formatting, which changed no expression.
- **D13 — the spectrum runs' concurrency** (*UNINTENDED DRIFT*). The first spectrum `table` and `shape` runs were started
  with a shell `&` (not a tracked background task) while the stage-2 validation ran, so up to eight of my processes ran at
  once (four stage-2, four spectrum); the load reached 32.9 during the validation, after every stage-2 start at ≤ 12.2.
  Ruling 3 (stage-2 points) was kept; the timings carry their loads.
- **D14 — bugs in my own scripts** (*UNINTENDED DRIFT*): `kexchain`'s production helpers first imported the
  `AbstractModel` class instead of its module; `precision.py` first compared φ_ta in units of M with φ_ta/φ_t; `fits.py`
  first passed NaN to `curve_fit`; `spectrum_kex.py` first wrote `np.float64(…)` reprs; `paths.py` first compared 04's
  6-figure f exactly and its proxy unscaled. Each failed or was visibly wrong on its first run and was fixed before the
  output that is committed.

## 3. Verification performed

| suite | at the base `e4ae58b` (time, load at start → end) | after (discovery) |
|---|---|---|
| CosmologyModels | 26 OK (66 s; 11.83 → 6.69) | 26 |
| ComputeTargets `test_[a-f]` | 226 OK (146 s; 6.69 → 4.21) | 226 |
| ComputeTargets `test_[g-z]` | 557 OK (550 s; 4.21 → 8.41) | 557 |
| ObjectFactories | 175 OK (155 s; 8.41 → 6.36) | 175 |
| RunDrivers | 79, 1 failure (ruling 5) (24 s; 6.36 → 6.99) | 79 |
| tools | 312, 1 failure (ruling 5) (342 s; 6.99 → 19.92) | 312 |

No count fell. No file the suites import changed, so the counts after are by discovery (P90).

## 4. The gate Remeasure, item by item

- **The definition reproduces Eqs. (66) and (71):** E71 4.4e-16 at six points; E66 the closed form to 2.2e-16 at six, with
  Eq. (66) its leading term exactly (§1.1; `definition.txt`). Eq. (64) itself, 4.0e-9 at twelve.
- **The old chain reproduces every recorded ℓ:** 199 of 199 within the printed digits (largest 5.0e-4, on the third
  decimal's rounding boundary), 371 of 371 bitwise against the copied born.chain (`rechain.txt`); crossover's fit constants
  to their printed digits (`fits.txt`).
- **Every stage-2 point re-chained** (371) **and every new point validated as crossover's stage 2 was:** energy at all
  fifteen (≤ 6.3e-10), classical limit (≤ 2.7e-6), Born limit (≤ 8.4e-4, D4), convergence (≤ 9.2e-4) at three.
- **The candidate laws' residuals at n = 1 and 2 beside v1's:** `fits.txt`, verification §7.5.
- **The three tables and x_* in the new x:** `spectrum.txt`, `spectrum_tables.txt`, verification §7.7.
- **The inventory complete:** `inventory.md`, 278 rows, the goldens classified.
- **Nothing ruled by the agent:** ruling 7.

## 5. The deliberate-breakage record

Each broken form of k_ex through §1.1's checks (`definition.txt` B) and §1.3's count:

| broken form | E64 | E71 | E66 | moves on crossover's 107 n = 1 rows |
|---|---|---|---|---|
| the rate as \|V‴/V′\| | 10.8 | no chain | 728 | 41 |
| the factor ½ dropped | 0.50 | 1.00 | 1.00 | 31 |
| the rate at φ_t inside the one-loop chain | 0.999 | 919 | 13.4 | 24 |
| (the ruled form) | 4.0e-9 | 4.4e-16 | 2.2e-16 | 20 |

Each broken form fails all three checks against tolerances of 1e-7, 1e-13 and 1e-13, and moves a different number of rows
from the ruled form's 20. The lambda form against born.chain (§1.3) would catch any change to the chain's other
expressions bit for bit.

## 6. Observations not acted on

1. **The fixed point needs more than production's 100 iterations near the chain's end** (opened as
   `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`, board §3): 195 at the laboratory
   point ℓ_new = 0.120, 107 on L-M8e-18 at 0.305, and 138 in the old form at 02's curve-1e-15-n1 row at ℓ = 0.100;
   production would call these gap. The chain's fixed point folds there (ℓ_new ≈ 0.297 on L-M8e-18, ≈ 0.12 on the
   laboratory curve), so ℓ does not go to 0 at the chain's end.
2. **Crossover's gap rows at small L are under-converged in k_max by about 3e-3**: #512 converges to 0.1561 against its
   recorded 0.1594 (D5). Its grid top, 30 × the old classical k_ex, is 5.8 ω_t there. The records stand; a refit that
   weights the small-L gap rows inherits it.
3. **The exponential's classical tail moves most:** in 2197 at A = 0.01, ℓ_t (and the spectrum scale of a classical
   generation) moves by up to 20.5 in ln at L ≈ 4e-9, where the wall is V₀(1 + M/φ) and new/old → 3/L. The quanta tools
   read the stored ln k_ex.
4. **The exponential's spectrum shape over L varies more in the new x:** x_p ×0.920 at L = 40 and ×1.023 at L = 134,
   against −3.2% to +0.9% in the old x (03), since the new k_ex/ω_t runs from 0.536 to 0.511.
5. **born.chain's "inside" test differs from production's None:** the chain gives up when V″(φ_t) ≤ m_ta² at an iterate
   (20 times among the re-chained rows, both forms counted); production searches inwards from the field and would find a
   turnaround inside φ_t there. Not measured further.
6. **The inventory's twenty items beyond the planner's list** (inventory.md, last section), among them the kernels' grid
   ends against the new x_lo (power-n1's x_lo 9.43e-4 < `G_Y_MIN` = 1e-3; x_lo e^{OMEGA_U_MAX} 7.6 against the
   docstring's "≥ 16.4") and `classical_turning_point_before_fix`, a copy of the old method in a test.
7. **With fit_law's own selection** the off-curve grid corners at L ≤ 3, ln R = 20–25, now one-loop, dominate the one-loop
   residuals of every candidate (0.35–0.62); crossover had excluded them from its gap fit as off every history's curve.
8. **The Born reduction does not reach ℓ ≲ 2.3** with k_IR = 10 m_ta (D4); crossover's own V1 stopped at ℓ = 4.9.

## 7. State handed to the next prompt

**For U13** (the user's; no recommendation): the candidates for `one-loop-exponential-v2`, each in its new regime
(verification §7.5; `fits.txt` lists every point with each law's value):
- *(i)* v1's form refitted, A = 2.69: n = 1 B0 0.3042, C 0.4106 (fit_law's selection; 0.5000, 0.5302 under note §0.3's),
  plateau 0.507 measured, gap 0.0230, 1.7972, 1.3795, 1.0496; n = 2 B0 0.2668, C 0.3632, plateau 0.618, gap 0.1285,
  2.8156, 0.5166, 0.5802. Residuals one-loop / below 1 / gap: n = 1 0.353 / 0.350 / 0.047 (note's 0.188 / 0.332 / 0.047),
  n = 2 0.177 / 0.355 / 0.045.
- *(ii)* A free: n = 1 A 3.2946 (note's 3.8016), n = 2 3.0730; one-loop 0.376 (0.159), 0.166; the rest as (i).
- *(iii)* ℓ_g fitted: n = 1 2.25 under fit_law's selection (one-loop 0.618, below 1 0.070, gap 0.046), 1.55 under the
  note's (0.070, 0.071, 0.046); n = 2 1.55 (0.039, 0.041, 0.056). Above ℓ_g B0 ≈ −1.2, C ≈ −0.04…−0.09.
- *(iv)* min(y_rise, y_fall), f continuous: n = 1 0.417 / 0.071 / 0.046 (fit_law's), equal to (iii) under the note's
  selection and at n = 2.
- *v1 beside:* its own rows in the old form 0.074 / 0.098 / 0.061 (n = 1), 0.125 / 0.112 / 0.102 (n = 2); the new points
  in the old form (all gap) 0.067 and 0.075; every point in the new form 0.262 / 0.743 / 0.043 (n = 1), 0.275 / 0.638 /
  0.064 (n = 2). The new points alone do not move v1; the new ℓ does, below ℓ = 1 (f = 0.16–0.82 against its 0.9).
- *The cap and plateau, measured:* largest f 0.986 / 0.990 (0.921 / 0.923 back out); below ℓ_new = 1, 0.157–0.802 (n = 1),
  0.262–0.821 (n = 2).
- *The drained share* on the mapped histories, K_X/K_v1 for (i)–(iv): 2197 A0.01 0.968–1.026 (fit_law's) and 0.978–1.075
  (note's); 2197 A0.05 0.993–1.116 and 1.001–1.217; the laboratory maps at A = 1 0.998–1.210 and 0.993–1.370; L-M8e-18
  1.008–1.144 and 0.967–1.237 (verification §7.8). The power wall's is 1.000000 everywhere.
- *Which points the law must describe* is part of the choice: the off-curve corners (D7, §6 item 7), and the fold below
  which no chain exists (§6 item 1).

**For 07c:**
- *The tables and x_*:* `.documents/power-law-wall/kex-07b/spectrum_tables.txt` holds the three `_LN_BETA2_*` tuples and
  `WallTable` entries as the module writes them: k_ex/ω_t 0.5163952511757223, 1.0606601717798214, 0.816496580927726;
  x_lo 0.0019365011543448797, 0.0009428090415820634, 0.001224744871391589; x_hi 3.5, 3.0, 3.0; 151, 163, 158 nodes;
  x_p 0.619424, 0.155250, 0.253372; κ_hi 4.342684, 6.545990, 5.467311; tail shares 7.4e-4, 8.0e-6, 1.4e-4. x_* =
  0.005615853347600149 (0.0029/0.5163952511757223); in k/ω_t on the power walls 0.002734 and 0.003552 (R10 carries x).
  R9's error is gone (≤ 3.4e-7 in ln|β|² at E/V₀ down to the laboratory R = 1).
- *The inventory:* `inventory.md`, 278 rows; no golden holds a k_ex column or an ℓ-dependent value; the kernels' grid
  ends against the new x_lo (§6 item 6).
- *The production-precision chain:* `kexchain.production_v3_turnaround` and `production_v3_classical`, through the hooks
  with (ln V)‴ by a centred difference (step 1e-5 φ/(1 + |(ln V)′|φ)), agree with the chain to 1.9e-11 at 40 candidates;
  07c's analytic `_d3_logV_dphi3` can be checked against them.
- *The fixed point's cap* near the chain's end (§3 issue).
- *For U11 and U12:* nothing here changes them. The power wall's law and constants are unchanged (§1.6), so a rename alone
  is U11; every history's stored ℓ, k_ex, ℓ_t, peak and regime label moves (§1.8: on the power wall ℓ_t by exactly ln 3
  and ln 2), which is what U12's new label guards. The fixed-law and natural-mode histories move in those columns too
  (through `margin_inputs`, inventory item 7), not in their integration.
