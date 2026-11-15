# Log 02 — measure the wall

**Subject:** S, the one-loop fraction and the spectrum on the power wall V₀[1 + (M/φ)ⁿ] at n = 1 and 2, by crossover's
method, along the histories' own paths; the adopted drain law against them. Research; no production change.
**Commit:** this commit, on branch `power-law-wall`, on top of `35f6359`
**Date:** 2026-11-14 to 15, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Wall holds (§4). No stop was reached.
- **The copies** reproduce crossover's recorded numbers on its walls; `PowerWall` in units of φ_t agrees with walls.py's
  units to 1e-11 (S), 1e-15 (ℓ) and 1e-14 (the stage-2 fraction).
- **The map:** all twenty-five histories tuned in one integration; every candidate one-loop, every history parked
  one-loop. The path formula holds to 2.8e-13 at 1148 candidates.
- **The validation:** classical limit, Born limit, energy, convergence, chain against production (1.7e-14) and scale
  invariance, each within its stated tolerance.
- **The finding** that shapes U3: on this wall S_n and stage 2's fraction depend on ln R and n alone (the floor enters
  no derivative of V), while the production's ℓ and k_ex depend on x through the floor where x ≲ 10³(φ_ta/φ_t)ⁿ.
- **Issues:** none opened, none closed.
- **Suites:** 26, 705 (219 + 486), 163, 302, 76 at the base and after; only the two worktree failures of note 01's ruling
  7 failed, both times.

## 1. What shipped

- **`prompts/power-law-wall/source/wall/`** (new):
  - the copies `walls.py`, `born.py`, `stage1a_classical.py`, `stage2.py`, `stage2_validate.py`, each with a header
    naming its source and crossover commit (`d0fe64a`, `9da5162`, `9da5162`, `d0fe64a`, `00a4ffc`). `born.py`,
    `stage1a_classical.py` and `stage2_validate.py` are byte for byte crossover's below the header. `walls.py` adds
    `PowerWall` (and an import); `stage2.py` takes a wall object in `solve` and `run_point`, reads any wall's `V4`, and
    caps its start relative to the entry point (D2);
  - `powerscan.py` (shared: the curves, the chain, `run_point_power`, the adopted law as `f_law`);
  - `path_map.py`, `runs_map.sh`, `map_summary.py` (§1.4);
  - `reproduce_exponential.py` (§1.2), `validate_wall.py` (§1.2, §1.3 items 1–4, 6), `chain_vs_production.py`
    (§1.3 item 5);
  - `stage1a_power.py` (§1.5), `scan_curves.py` and `curve_tables.py` (§1.6), `fit_law.py` (§1.7), `spectrum.py`
    (§1.8);
  - `README.md`: the files, how to run each, the units' derivation and the results.
- **`.documents/power-law-wall/wall-02/`** (new, text): `map/` (25 histories), `map_summary.txt`,
  `reproduce_exponential.txt`, `validation.txt`, `chain_vs_production.txt`, `stage1a_S_n.txt`, `stage2_scan.txt`,
  `stage2_curves.md`, `fit_law.txt`, `spectrum.txt`.
- **`.documents/power-law-wall-verification.md` §2** (added after §1, which is unchanged).
- **The board:** row 02, the Wall gate, the baselines and the status history. No §3/§4 entry moved, so
  `.documents/OPEN_ISSUES.md` is unchanged.
- **Not committed:** the suites' outputs, the breakage copies, the first map run and the scratch probes, in the agent's
  scratch directory (`…/scratchpad/agent/`). No figure was made (§1.10 allows "at most one"; none is needed to read the
  tables).

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note 02 §0)

1. **The worktree and the main checkout.** All work was done in the worktree with `./venv/bin/python` and
   `PYTHONPATH=.`. Nothing in the main checkout was touched, `venv` was never staged, and no command named
   `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **The units.** Units of φ_t (φ_t = 1, ω_t = 1), not relative tolerances. In them U and its derivatives do not depend
   on x, φ ≥ O(φ_t) throughout, and the copied absolute tolerances are harmless. The loop term is derived in
   `source/wall/README.md` (K = 4π²RE in any unit of field); validated against walls.py's units at x = 10, 10³
   (verification §2.1). *IMPLEMENTATION CHOICE, ruled.*
3. **Where stage 2 cannot reach, born.py.** Stage 2 ran at every ℓ needed, up to 45.4 (n = 1) and 42.5 (n = 2), in
   7–12 s, with energy to 2e-9. born.py was not needed for coverage; it was run in the overlap (ℓ = 5–40) as the ruling
   asks, born-sc/stage 2 = 0.93–0.99 (verification §2.3 item 2). *IMPLEMENTATION CHOICE, ruled.*
4. **Failures are results.** No mapped history failed. No stage-2 point raised. Six off-curve runs of the copied
   `run_point` at small x are invalid by their energy (5.4e-4 to 5e19); they are recorded as such, not retried
   (verification §2.5). No uncaught exception came from a production module. *IMPLEMENTATION CHOICE, ruled.*
5. **The machine.** The scans used six processes and the map three, except for one slip (D7). The load is recorded in
   every output. The scans took about 15 minutes of wall time in all, far inside the three hours. *IMPLEMENTATION
   CHOICE, ruled.*
6. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED.*
7. **The two worktree failures** (`test_registered_run …test_the_script_is_main_py…` in RunDrivers and
   `test_registry_config …test_roots_and_distributions` in tools) failed on the base and after, and nothing else did.
   *IMPLEMENTATION CHOICE, ruled.*
8. **A finding about production** is recorded, not fixed. The production wall search agrees with the chain at every
   mapped candidate (verification §2.3 item 5), so there is no such finding; the floor's effect on the production's ℓ
   is the wall's, not a defect, and goes to U3 as evidence. *IMPLEMENTATION CHOICE, ruled.*
9. **No recommendation is acted on.** The hand-back (§7, verification §2.10) states options and evidence; no law, L, S
   table or constant is chosen. *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1 — `PowerWall(x, n, units)`**, parametrized by x, with `units="phi_t"` (default) or `"M"`; `x = inf` is the wall
  with no floor; `c` is the floor V₀, as `born.chain` and `stage2.lam` read it. *IMPLEMENTATION CHOICE* (the prompt
  leaves x or L_E to the agent).
- **D2 — the copies' changes.** `stage2.solve` and `run_point` take `wall=`; `stage2.V4` uses `hasattr(w, "V4")`
  (Wall unchanged); `solve`'s cap on the start is `1e15 * max(p_entry, 1)` in place of `1e15`. In units of φ_t the
  turnaround lies at up to ~10⁴⁰ φ_t, beyond the absolute cap; every run the old cap let through is unchanged, and
  `reproduce_exponential.py` reproduces crossover's step counts exactly. The cap is *STRUCTURALLY REQUIRED* by ruling
  2's units; the other two are *IMPLEMENTATION CHOICE*.
- **D3 — `run_point_power`.** The scans set stage 2's grid and EPS from the floor-free chain (the bounce's own scales)
  rather than from the chain on PowerWall(x). The copied `run_point` takes its references from λ, which the floor
  suppresses: validate V6 measured 0.546 against 0.669 at x = 100 (n = 1, ln R = 40), and the off-curve grid six
  invalid runs. Both are kept in the record. *IMPLEMENTATION CHOICE.*
- **D4 — the map does not run BBN.** `path_map.py` uses the tool's model builder, setup, z grid and `tune_history` call,
  and stops after the history; nothing here reads BBN. *IMPLEMENTATION CHOICE.*
- **D5 — the map was run twice.** The first run (23:38–23:40) printed ln(k_ex/M_P) to 4 decimals, too few for the
  estimate's comparison; `path_map.py` was changed to print 6–7 decimals and the twenty-five rerun (23:41–23:42). Every
  outcome, count and parking point is the same in both; only the second is committed. *IMPLEMENTATION CHOICE.*
- **D6 — the base suites ran beside the work.** They started at 23:31, after the copies existed, and ran while the map
  and the validation ran (load 4–12). No production file was changed at any time, and the untracked files under
  `prompts/` and `.documents/` are read by no test. *IMPLEMENTATION CHOICE.*
- **D7 — six workers exceeded once.** The three breakage drivers were first launched together, each with a pool of six
  (eighteen processes), for under a minute (23:53–23:54) before I killed them and ran them one after another. Their
  partial outputs were discarded. *UNINTENDED DRIFT.*
- **D8 — S is one number per n.** §1.5 asks for a table in x dense enough to interpolate. On this wall S_n does not
  depend on x (the test field reads U'' and U''' only), which the runs in units of M at nine x from 1e-3 to 1e3 confirm
  to every printed digit. The table is that check, and S_n a constant. *STRUCTURALLY REQUIRED* (the measurement).
- **D9 — item 5 in two parts.** (a) every mapped candidate (1148), against the printed ℓ, tolerance 1e-5 from the
  printed digits; (b) twelve candidates through `InversePowerModel` directly, tolerance 1e-9. The prompt asks for ten
  or more. *IMPLEMENTATION CHOICE.*
- **D10 — §1.7's residuals are given also per curve and at the mapped points**, and, for reference, with the law read
  at the floor-free ℓ_∞ (not an option of §1.7, labelled so in `fit_law.txt`). *IMPLEMENTATION CHOICE.*
- **D11 — (v)'s gap form.** The exponential's gap function has a factor (1 + Q ln L); this wall has no L, and stage 2's
  fraction depends on ln R alone, so the refit is y = G(ln R − R0)^P per n. *IMPLEMENTATION CHOICE.*
- **D12 — the spectrum's two x near the floor** are the laboratory curve's ln R = 10 (in the gap; x = 6.87, 11.9) and
  R = 1 (x = 0.930, 0.426). *IMPLEMENTATION CHOICE.*
- **D13 — a fourth convergence point per n** (the gap at ln R ≈ 3.3–3.6) was added after the scan showed the gap's low
  end, and the validation rerun whole (00:07–00:10); only that run is committed. *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 The suites

From the worktree root, serially, by one background script (`…/scratchpad/agent/suites/run_suites.sh`). Load is the
one-minute average at the start and end.

| Suite | Base `35f6359` (ran / result / time / load) | After (ran / result / time / load) |
|---|---|---|
| CosmologyModels | 26 / OK / 70 s / 11.60 → 7.35 | 26 / OK / 66 s / 5.04 → 4.51 |
| ComputeTargets `[a-f]` | 219 / OK / 153 s / 7.35 → 5.40 | 219 / OK / 146 s / 4.51 → 3.63 |
| ComputeTargets `[g-z]` | 486 / OK / 549 s / 5.40 → 4.42 | 486 / OK / 516 s / 3.63 → 4.58 |
| ObjectFactories | 163 / OK / 133 s / 4.42 → 4.31 | 163 / OK / 144 s / 4.58 → 16.43 |
| tools | 302 / 1 failure (ruling 7) / 299 s / 4.31 → 10.63 | 302 / 1 failure (ruling 7) / 291 s / 16.43 → 4.14 |
| RunDrivers | 76 / 1 failure (ruling 7) / 26 s / 10.63 → 8.62 | 76 / 1 failure (ruling 7) / 28 s / 4.14 → 4.74 |

No count fell, and no test that passed on the base failed after.

### 3.2 The copies, the wall and the units (§1.2)

- `reproduce_exponential.txt` (23:45, load 3.7): S(10) 1.3529e-03, S of WallN(5, 2) 2.14335e-03, Born 0.44982,
  run_point(7.885, 23.833) 0.71709 / 0.5319 / 1.678 / 311 modes / 53265 steps: crossover's recorded values, all.
- `validation.txt` D: PowerWall's derivatives against central differences, worst 7.0e-8 (tolerance 1e-6).
- `validation.txt` U: units of φ_t against M at x = 10 and 10³: S ≤ 2.7e-11 (tolerance 1e-6), ℓ ≤ 6.7e-16 (1e-10; at
  x = 10 neither unit has a chain), the stage-2 fraction ≤ 1.2e-14 with identical step counts (1e-8).
- The planner's ℓ table (note §0) is reproduced to every printed digit with born.chain in units of φ_t.

### 3.3 The map (§1.4)

Verification §2.2. All 25 histories succeeded (one integration each, tracking seed); 1148 power-wall and 152
exponential candidates, all one-loop; parking N = 26.668–38.898. The path formula: worst 2.8e-13 (power wall), 4.4e-12
(the exponential's curve). The Planck-scale table is verification §2.2's.

### 3.4 The validation (§1.3)

| item | what | tolerance (reason) | worst |
|---|---|---|---|
| 1 | stage 2 loop off against walls.py's test field, x = 1e40, 1, 0.1, n = 1, 2 | 1e-4 (crossover V-A and the quadratures) | 2e-6 |
| 1 | frac/R → S_n as R → 0 | (a trend) | 4e-4 short of S_n at ln R = −8 |
| 2 | stage 2 cut at 10 m_ta against born.run, ℓ = 5, 8, n = 1, 2 | 2e-3 (crossover V1) | 7.1e-4 |
| 3 | max \|ΔE/E\| along each run | (reported) | 2e-8 (validation), 1.9e-8 (153 scan points) |
| 4 | convergence, four points per n | (sets the precision) | ±1e-5 (ℓ ≳ 20), ±1e-3 (ℓ ≈ 1), ±6e-3 (gap) |
| 5 | chain against `one_loop_turnaround` | 1e-5 (printed digits, 1148 candidates); 1e-9 (12 direct) | 5.0e-6; 1.7e-14 |
| 5 | `f_law` against `drain_fraction` | bitwise | 0 of 4000 differ |
| 6 | S, ℓ, f at fixed R, x = 1e10–1e100 | (reported) | S bitwise; ℓ equal at x ≥ 1e20; f equal at x ≥ 1e10 |
| 6 | where the floor matters for ℓ | (reported) | \|Δℓ\| > 1e-3 below x ≈ 1.2–2×10³(φ_ta/φ_t)ⁿ |

### 3.5 S, the scan, the law and the spectrum

Verification §2.4–§2.7, with the outputs named there. Run times and loads: `stage1a_S_n.txt` 109 s (load 15.5 → 5.6);
`stage2_scan.txt` 315 s for 201 runs (3.3 → 6.8); `spectrum.txt` 41 s (3.6 → 6.5); `validation.txt` 187 s
(5.8 → 6.6).

## 4. The gate Wall, item by item

- **S measured at n = 1, 2, validated:** holds. S_1 = 1.02620e-2, S_2 = 5.21615e-3, the same at every x; quadrature to
  7e-7, stage 2 loop off to 2e-6 (verification §2.4, §2.3 item 1).
- **The one-loop fraction measured at n = 1, 2 along the power-law paths, validated as crossover's stage 2 was (Born
  limit, classical limit, energy):** holds. 153 points on six curves; Born limit 7.1e-4, classical limit 2e-6 and
  frac/R → S_n, energy ≤ 1.9e-8, convergence stated (§3.4).
- **The spectrum measured at n = 1, 2:** holds. x_p, the slope and κ two ways, with the exponential recomputed by the
  same code (verification §2.7).
- **The adopted law's residuals at each candidate L stated:** holds. (i)–(v) by n and regime, against the exponential's
  0.074 and 0.062 (verification §2.6).
- **Nothing ruled by the agent:** holds. The hand-back states options and evidence only.

## 5. The deliberate-breakage record

Each mutation was applied to a scratch copy of `source/wall/` (never to the committed files), and `validate_wall.py`'s
D, U (S at n = 1; ℓ and f at x = 10³, n = 1, 2), V1 (x = 1e40), V1R (ln R = −6), V2 (ℓ = 5) run on it; for the third
also `chain_vs_production.py`. Outputs in `…/scratchpad/agent/breakage/`.

B1, G doubled:

```diff
--- a/prompts/power-law-wall/source/wall/stage2.py
+++ b/prompts/power-law-wall/source/wall/stage2.py
@@ -101,7 +101,7 @@
     w = wall if wall is not None else (Wall(L) if n == 1 else WallN(L, n))
     R = exp(lnR)
     E = w.E
-    K = 4.0 * pi**2 * R * E if loop else 0.0
+    K = 8.0 * pi**2 * R * E if loop else 0.0
     K2 = (
         8.0 * pi**2 * R * E
     )  # rho -> energy (kept for the measurement when the loop is off)
```

B2, V''' of the wrong sign:

```diff
--- a/prompts/power-law-wall/source/wall/walls.py
+++ b/prompts/power-law-wall/source/wall/walls.py
@@ -178,7 +178,7 @@
 
     def V3(self, p):
         n = self.n
-        return -n * (n + 1) * (n + 2) * self.A * p ** (-n - 3)
+        return n * (n + 1) * (n + 2) * self.A * p ** (-n - 3)
 
     def V4(self, p):
         n = self.n
```

B3, the floor converted wrongly (walls.py's M-unit V₀ used in units of φ_t):

```diff
--- a/prompts/power-law-wall/source/wall/walls.py
+++ b/prompts/power-law-wall/source/wall/walls.py
@@ -155,7 +155,7 @@
             self.phi_t = 1.0
             self.A = 1.0 / (n * (n + 1))
             self.E = self.A
-            self.c = self.E / self.x
+            self.c = self.A * self.x ** (-1.0 - 2.0 / n)
         elif units == "M":
             self.phi_t = self.x ** (-1.0 / n)
             self.c = self.phi_t ** (n + 2) / (n * (n + 1))
```

| Break | Caught by | Not caught by |
|---|---|---|
| B1 | the Born limit (ratio 0.515 and 0.512 against 1 ± 2e-3); energy (\|ΔE/E\| 0.50 in the Born and units rows, 1.3e-4 at ln R = −6) | the classical limit, S, ℓ, and the units comparison (both units broken alike) |
| B2 | D (worst 2.0 against 1e-6); energy (\|ΔE/E\| 0.12–0.13 in the units rows, 5e-4–6e-4 in the Born rows, 6.7e-5 at ln R = −6) | the classical limit (ratio 0.999986, within 1e-4), S, ℓ, the Born ratio (1.00095, 1.00131, within 2e-3) |
| B3 | the units comparison of ℓ (0.54, 0.75 against 1e-10) and of f (3.0e-4, 8.2e-5 against 1e-8); D's U(φ_t)/(xV₀) (100 to 1e80 against 1); chain against production ((a) 8.1e-3 against 1e-5, (b) 3.9e-4 against 1e-9) | S, the classical and Born limits, energy (the dynamics never read V₀) |

Each break is caught by at least two of §1.2–§1.3's checks. B2 shows that the classical limit alone does not see the
sign of V''' (the test field reads it only through ω' in its adiabatic projection, at O(EPS)); the derivative check and
the energy budget do.

## 6. Observations not acted on

- **The floor and the law's inputs.** On this wall stage 2's fraction depends on ln R and n alone, but the production's
  ℓ (and k_ex) carry the floor's factor s = U/(V₀ + U) through λ: ℓ − ℓ_∞ ≈ −(φ_ta/φ_t)ⁿ/x. On the 1e-33 M_P curves
  this lowers ℓ by up to 4.6 and classifies bounces with ℓ_∞ up to 3.0 (n = 1) and 4.75 (n = 2) as gap. Every option
  of §1.7 has its worst residual there. Evidence for U3; nothing changed.
- **The mapped histories never leave the one-loop regime** under the fixed law at α = 0.01 and 0.1: every one parks
  with ℓ ≥ 10 (n = 1) or ≥ 2.25 (n = 2, 1e-15 M_P). The gap and classical rows of §1.6 are therefore all beyond the map.
  Whether one-loop-law histories reach them is 04's to measure.
- **Crossover's `run_point` on a floored wall** takes its grid from the floor-suppressed λ and goes wrong at small x
  (0.546 against 0.669 at x = 100; six invalid off-curve runs, flagged by energy). Crossover is closed and was not
  changed; the copy is kept as it was and `run_point_power` used for the scans.
- **walls.py's default tolerances at n = 2** leave |β|² ≈ 1e-12 (at 10 k_ex) 36% above its value at EPS/1000,
  rtol 1e-13, atol 1e-17; κ moves by 0.03. At n = 1 and on the exponential the two agree to 0.4% and 0.02%.
- **The rise below the peak** is k^1.34 (n = 1) and k^1.76 (n = 2) on [0.02, 0.1] k_ex at large x, not k²: the U4
  shape's x² piece is the exponential's. For U5.
- **The Planck scale.** At laboratory M the classical k_ex at the first impact is e^84–e^116 M_P (n = 1) and e^34–e^55
  M_P (n = 2); the turnaround's e^−1.4–e^12.9 and e^−5.1–e^8.3. The exponential's is below M_P at the QCD kick and
  e^2.42 ≈ 11 M_P at β = 3.05. Reported, not judged (prompt §1.4).
- **The scheme is outside its own expansion** on this wall too: ε_turn is 10³⁹ at the first impacts and ~10² at
  ℓ ≈ 1, as crossover found on the exponential.
- **The exponential's spectrum, re-measured:** k_peak/ω_t = 0.320, so x_p = 0.646 (exact k_ex) or 0.639 (ω_t/2),
  against the module's 0.62 (from the Table's rounded 0.31); κ = 3.967 against `KAPPA` 3.966.

## 7. State handed to the next prompt

- **No production file changed.** `InversePowerModel` still has no `_drain_L`; the one-loop law still refuses it.
- **The tools** in `prompts/power-law-wall/source/wall/`: `PowerWall`, `powerscan.run_point_power`, the map, the
  validation, the scan and the fit, each runnable as its README says.
- **The hand-back for U3–U5** (verification §2.10 has the full text; options and evidence, the user rules):
  - **U3, the law.** Stage 2's fraction on the power wall depends on ln R and n alone; the production's ℓ depends on x
    through the floor where x ≲ 10³(φ_ta/φ_t)ⁿ. Residuals (law − stage 2), max over all points / at the mapped points,
    one-loop ℓ ≥ 1, n = 1 | n = 2:
    - (i) L_E: 0.429 / 0.021 | 0.524 / 0.070; gap 0.806 | 0.895; on the laboratory curve 0.052 / 0.124 (gap) | 0.060 /
      0.112;
    - (ii) L = n: 0.276 / 0.005 | 0.411 / 0.100; gap 0.445 | 0.565 (0.13–0.14 off the 1e-33 curve);
    - (iii) L_M: 0.896 / 0.896 | 0.837 / 0.837; gap 0.839 | 0.739;
    - (iv) best constant L: one-loop L = 0.731 (0.253 / 0.004) | 0.383 (0.293 / 0.158); gap L = 1.317 (0.445) | 3.065
      (0.565);
    - (v) refit: b = 1.0975 (0.254 / 0.004) | 1.3627 (0.293 / 0.158); gap G = 0.0017, R0 = −1.80, P = 2.39 (0.445) |
      G = 1e-4, R0 = −3.95, P = 3.52 (0.565); plateau measured 0.49–0.89 | 0.40–0.88; cap 0.974.
    - Against the exponential's 0.074 (one-loop) and 0.062 (gap). Every option's worst point is on the 1e-33 curve.
  - **U4, S.** S_1 = 1.02620e-2 and S_2 = 5.21615e-3, constants in x. The exponential's S(L_E) agrees with S_1 to 2% at
    x ≤ 0.3 and is 1.1–4.7 times smaller at x = 1–10³ and 28 times smaller at 10¹⁰; S_2 is 0.52–0.56 of S(L_E) at
    x ≤ 1. The classical rows follow R S_n (0.996–0.998 at ln R = −5).
  - **U5, the spectrum.** At large x: x_p = 0.466 / 0.507 (n = 1 / 2, fit), κ = 2.006 / 2.544 (two-point, strict) and
    2.145 / 2.713 (tail fit), the rise k^1.34 / k^1.76; against the exponential's 0.646, 3.967 (4.262 tail fit) and
    1.985 by the same code. Near the floor x_p scales as 1/(k_ex/ω_t) and κ as k_ex/ω_t, k_ex/ω_t = (s_t/2)√(n/(n + 1)).
  - **Beside these, the Planck scale** (verification §2.2's table, for the user).
- **For 04:** every mapped history at α = 0.01, 0.1 parks one-loop; the gap and classical regimes on this wall are met
  only by histories that drain less, or at other M.
- **No issue opened or closed**; `.documents/OPEN_ISSUES.md` is unchanged (32 open).
