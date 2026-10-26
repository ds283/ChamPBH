# Log 05 — the Planck mass

**Subject:** One reduced Planck mass from PRyMordial's G_N, in the three unit systems
**Commit:** `1675e73`, on branch `jordan-normalization`, on top of `91fcbf9`
**Date:** 2026-10-26, by Claude Sonnet 5.5
**Result:** **COMPLETE**, with no stop.
- K1–K3 shipped under orchestrator note 05, whose fourteen rulings govern where they differ from
  the prompt, and under the user's ruling "Re-pin in 05" (ruling 13).
- `REDUCED_PLANCK_MASS_EV = 2.4353235955263046e+27` (`0x1.f79cf52984fafp+90`), 2.7775e-4 below
  the 2.436e27 used until now.
- Ruling 7's two tables **equal note 05's §0 in every digit shown** (both golden histories).
- Counts: CosmologyModels 18 → 26 (+8, the new module); ComputeTargets 61 + 144, ObjectFactories 86,
  tools 102 and RunDrivers 61 are unchanged.
- Both deliberate breakages are caught.
- No issue was opened or closed.

## What shipped

- **K1: `Units/base.py`.** `from math import pi, sqrt`; `NEWTON_G_MEV_MINUS_2 = 6.70883e-45` and
  `REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2)`, each with a comment
  (ruling 1). Nothing imports `PRyM`. `Units/__init__.py` is unchanged.
- **K2: the three systems** (ruling 2). Each imports `REDUCED_PLANCK_MASS_EV`.
  - `Planck_units.eV = PlanckMass / REDUCED_PLANCK_MASS_EV`;
  - `Mpc_units.eV = PlanckMass / REDUCED_PLANCK_MASS_EV`;
  - `GeV_units.PlanckMass = REDUCED_PLANCK_MASS_EV * eV`.

  No other statement and no literal changed. The comments name the source of each literal (CODATA
  2018's Planck length, mass, time and temperature, the megaparsec, and the electronvolt from
  `REDUCED_PLANCK_MASS_EV`), and "here we are choosing it to be Mpc instead", which `Planck_units`
  and `GeV_units` had copied from `Mpc_units`, now says reduced Planck mass and GeV there.
- **K3: `CosmologyModels/tests/test_planck_mass.py`** (new, 8 tests, no solve), see "Tests added".
- **The golden history** is regenerated as `ComputeTargets/tests/data/golden_history_05.json`
  (ruling 8); `golden_history_01.json` and `golden_history_03.json` are byte-identical
  (`shasum -c` before and after).
- **Twelve old-unit pins re-pinned** in four files (ruling 13), see below.
- **The prose** (ruling 9): `CLAUDE.md`'s "Units" bullet, and a dated note in
  `.documents/architecture-summary.md` §8.1. The `Units/` tree there is unchanged.
- **The records:** this log and the board (row 05, K1–K3, gate "Units", §5.6).
  `.documents/OPEN_ISSUES.md` is unchanged (no issue opened or closed).

## Tests added or changed

**Added: `CosmologyModels/tests/test_planck_mass.py`** (8 tests; reads `PRyM/PRyM_init.py` as text at
a path from `__file__` and never imports `PRyM`).
- `TestNewtonConstantIsPRyMordials`:
  - `test_a_G_N_equals_PRyM_init_as_decimals`: finds exactly one line matching `^GN\s*=\s*(.+?)\s*#`,
    takes the `Decimal` product of its `*`-separated factors, and requires it to equal
    `Decimal(repr(NEWTON_G_MEV_MINUS_2))`;
  - `test_b_PRyMordials_Planck_mass_is_one_over_sqrt_G_N`: the line
    `Mpl = 1./np.sqrt(GN) # Planck mass in [MeV], PDG` is present verbatim;
  - `test_c_the_reduced_Planck_mass_is_from_the_same_G_N`: `REDUCED_PLANCK_MASS_EV * sqrt(8 pi)`
    equals `1.0e6 / sqrt(G)` to 1e-15 relative, G the float of PRyMordial's decimal product.
- `TestTheUnitSystemsAreConsistent`, each with a `subTest` per system (`Planck_units`, `Mpc_units`,
  `GeV_units`):
  - `test_a_Planck_mass_over_eV_is_the_constant`: `PlanckMass / eV` equals `REDUCED_PLANCK_MASS_EV`
    to 1e-15;
  - `test_b_kelvin_is_k_B`: `Kelvin / eV` against 8.617333262e-5 eV/K, 1e-6;
  - `test_c_inverse_eV_is_hbar_c_in_metres`: `(1/eV)/Metre` against 1.973269804e-7, 1e-6;
  - `test_d_inverse_eV_is_hbar_in_seconds`: `(1/eV)/Second` against 6.582119569e-16, 1e-6;
  - `test_e_kilogram_is_5p6096e35_eV`: `Kilogram / eV` against 5.609588604e35, 1e-6.

**Measured relative differences** (ruling 4), the same to the digits shown in the three systems:

| quantity | `Planck_units` | `Mpc_units` | `GeV_units` |
|---|---|---|---|
| 1 K against k_B | 1.700870e-7 | 1.700870e-7 | 1.700870e-7 |
| 1/eV against ħc | 4.080485e-8 | 4.080485e-8 | 4.080485e-8 |
| 1/eV against ħ | 1.580230e-7 | 1.580230e-7 | 1.580230e-7 |
| 1 kg against 5.609588604e35 eV | 2.130484e-7 | 2.130484e-7 | 2.130484e-7 |
| `PlanckMass / eV` against the constant | 1.1e-16 | 0.0 | 0.0 |

Note 05's §0 gave 1.70e-7, 4.08e-8, 1.58e-7 and 2.13e-7: they agree. With 2.436e27 each is 2.78e-4.

**Changed (ruling 13, re-pins; nothing else in these files).** The table has the old and the new
reference, each new one the full-precision value of the probe (`agent05/work/probe.py`, run on the
old tree in a worktree of `91fcbf9` and on the new tree) rounded to the digits the literal had.
Tolerances, probe states, RHS bounds, the exact counts 1945 and 224 and P3's 51 bounces are unchanged.

| file | quantity | old (full) | new (full) | literal old → new |
|---|---|---|---|---|
| `test_kinematic_cap_loop.py` | first bounce N, P1 M = 0.5, `cap_fraction` 0.02 (`:237`, `:288`) | 20.343028460751025 | 20.343018133861474 | `20.343028` → `20.343018` |
| | first bounce φ, same (`:238`, `:289`) | 4.573708520186028e-3 | 4.573747964295935e-3 | `4.57371e-3` → `4.57375e-3` |
| | φ(21), P1 M = 0.5 (`:239`, `:290`, `:335`) | 0.12203833994225839 | 0.12202349644648677 | `1.2203834e-1` → `1.2202350e-1` |
| | π(21), P1 M = 0.5 (`:240`) | -0.12971527073457434 | -0.1297955195199084 | `-1.2971527e-1` → `-1.2979552e-1` |
| | first bounce φ, M = 0.01 (`:243`) | 9.150450796454582e-5 | 9.150530347400683e-5 | `9.1505e-5` unchanged |
| | φ(21), M = 0.01 (`:250`) | 0.11851542297125517 | 0.11850150259035461 | `1.185154e-1` → `1.185015e-1` |
| | first bounce φ, M = 0.001 (`:259`) | 9.15050729771744e-6 | 9.150586849343115e-6 | `9.1505e-6` → `9.1506e-6` |
| | φ(21), M = 0.001 (`:260`) | 0.11845006431115615 | 0.11843616076150691 | `1.184501e-1` → `1.184362e-1` |
| | φ(21), reflected, M = 1e-10 and 4.1e-28 (`:270`) | 0.11844280391325095 | 0.11842890222778686 | `1.184428e-1` → `1.184289e-1` |
| | φ(37.5), P3 (`:302`) | 5.80781984178963e-4 | 5.809959027406927e-4 | `5.8078e-4` → `5.8100e-4` |
| | P3 minima 1, 2, 8 (`:308`) | 2.7988611567708756e-4, 3.036503005191302e-4, 3.663676042756645e-4 | 2.7989480800957737e-4, 3.0366053400598616e-4, 3.6638253375171086e-4 | `2.79886e-4`, `3.03650e-4`, `3.66368e-4` → `2.79895e-4`, `3.03661e-4`, `3.66383e-4` |
| | P2 wall bounces to N = 40 (`:317`) | 19 | 20 | `19` → `20` |
| | φ(40), P2 (`:318`) | 1.9096925077987468e-2 | 1.909720464553881e-2 | `1.909693e-2` → `1.909720e-2` |
| `test_integrator_exceptions.py` | `P1_M0p5_PHI_21` | 0.12203833994225839 | 0.12202349644648677 | repr of the float |
| | `P1_M0p5_PI_21` | -0.12971527073457434 | -0.1297955195199084 | repr |
| | `P1_M0p5_FIRST_BOUNCE_N` (default step control) | 20.343034651924082 | 20.343024418098206 | repr |
| | `P1_M0p5_FIRST_BOUNCE_PHI` | 0.004573794758933947 | 0.004573834433278705 | repr |
| `test_first_bounce.py` | first bounce N, φ (`:53`, `:54`) | 20.343028, 4.57371e-3 | 20.343018, 4.57375e-3 | `20.343028`, `4.57371e-3` → `20.343018`, `4.57375e-3` |

The old column is what I measured on `91fcbf9`, and agrees with the literals the modules had to
the digits they had.
- **P2's 20th bounce**, as note 05's §0 accounts for it: π has a shallow minimum near N ≈ 36.53
  that stays positive on the old units and dips below zero on the new, which `wall_bounces` counts
  as a turning point at φ = 0.01391 < 1.5 M. I did not re-measure that account; I measured that the
  count goes from 19 to 20 (probe lines above).
- **RHS counts in docstrings** (ruling 13): `2120` → `2117`, `2275` → `2262`, `38 547` → `38 761`,
  `18 880` → `19 030`, each equal to the probe's `nfev` (2117, 2262, 38761, 19030). `1945` is
  unchanged (probe: 1945 and 224 accepted).
- **`tools/tests/test_plot_surfing_histories.py`:** the two `assertEqual`s on `bounce_T_GeV` and
  `bounce_N` against the committed scan's row became `assertLessEqual(abs(x / float(row[...]) -
  1.0), 1e-7)`. The test docstring and the module docstring's bullet say why. Every other
  assertion is unchanged, and the CSV is not touched.
- **Prose:** `test_kinematic_cap_loop.py`'s module docstring gains the paragraph of ruling 13, and
  `test_integrator_exceptions.py`'s module docstring, constants comment and `test_c`'s docstring
  say that the constants are 05's tree's.
- **`test_golden_history.py`, `golden_history.py`:** `GOLDEN_PATH` and the docstrings name
  `golden_history_05.json`; the provenance's `code_tree` says "jordan-normalization prompt 05's
  working tree, before its commit, on top of generated_at_commit".

**Not changed** (ruling 5): `test_inventory_report.py`, `test_compare_stores.py`,
`test_store_fingerprint_golden.py`. They pass unedited.

## Deviations from the prompt

Every ruling cited is in orchestrator note 05, §0, unless it says otherwise.

1. **`IMPLEMENTATION CHOICE` (ruling 1): the constant's expressions,** exactly as ruled
   (`1.0e6 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2)`), for the bits of §0's table.
2. **`IMPLEMENTATION CHOICE` (ruling 2): the three systems' statements and comments,** exactly as
   ruled, with the copied "here we are choosing it to be Mpc instead" corrected in two files.
3. **`STRUCTURALLY REQUIRED` (ruling 3): the PRyMordial comparison is decimal.** The one-ulp
   difference is in "Observations not acted on".
4. **`STRUCTURALLY REQUIRED` (ruling 4): the consistency tests** are in each of three systems at
   1e-6, with `PlanckMass / eV` at 1e-15.
5. **`STRUCTURALLY REQUIRED` (ruling 5): the three "pinned strings" are not edited.** None moves
   with the constant: two insert the stored literal `value_eV = 2.436e24` and print it back, and
   one stores 2.435e24 and pins no string. Editing the fingerprint test's row would move the
   golden fingerprint's digests with no spec change.
6. **`IMPLEMENTATION CHOICE` (ruling 6): the grep's pattern** and its expected remainder (below).
   My own docstring paragraph in `test_kinematic_cap_loop.py` first named the old number as
   `2.436e27` and so was a hit; I reworded it ("the round number the audit's units used") so that
   the grep finds only what ruling 6 expects.
7. **`STRUCTURALLY REQUIRED` (ruling 7): the 1e-2 stop is replaced** by the table in "Verification".
8. **`STRUCTURALLY REQUIRED` (ruling 8): `golden_history_05.json`,** with `golden_history.py` and
   `test_golden_history.py` added to the prompt's §5.
9. **`IMPLEMENTATION CHOICE` (ruling 9): the prose.** `numerical-strategies.md` and
   `prompts/INDEX.md` are not edited.
10. **`IMPLEMENTATION CHOICE` (ruling 10): the two breakages** are the ruled ones.
11. **`IMPLEMENTATION CHOICE` (ruling 11): the records** are the board and this log; no issue.
12. **`STRUCTURALLY REQUIRED` (ruling 13, the user's ruling "Re-pin in 05"): the twelve pins** in
    four files, per the table above. The four modules pass on the new tree (41 tests), and every
    new value equals my own measurement on the new tree.
13. **`IMPLEMENTATION CHOICE`: the new test module's helpers.** `test_planck_mass.py` has two
    module functions (`_prym_lines`, `_prym_G_N`) that the decimal comparison and the 1e-15
    comparison share. Ruling 3 gives the algorithm; the factoring is mine.

No `UNINTENDED DRIFT`.

## Verification performed

**1. Baselines, before any edit** (`91fcbf9` plus nothing), serially, loads 3.5–10.7 (`uptime` at each
start in `agent05/work/before.summary`): CosmologyModels 18, ComputeTargets 61 + 144,
ObjectFactories 86, tools 102, RunDrivers 61, all passing. `test_golden_history` alone: 9.574 s test
time, 11.2 s wall, at load 5.7.

**2. The change in values** (ruling 7). Both golden histories were run through
`golden_history.run_history` on a worktree of `91fcbf9` (old) and on the working tree after K1 and
K2 (new), and every field of every sample compared at equal index. The labels ln(1 + z) are equal
at every common index, and `n_samples` is unchanged. The relative difference is
|a − b|/max(|a|, |b|), and 0 where both are 0. **Both tables equal note 05's §0 in every digit shown.**

| field | β = 1.5: max (sample) | > 10⁻² | β = 5.5: max (sample) | > 10⁻² |
|---|---|---|---|---|
| `raw_N` | 1.388e-04 (0) | 0 | 6.977e-06 (0) | 0 |
| `phi_Einstein` | 4.743e-05 (2024) | 0 | 1.502e-03 (4642) | 0 |
| `pi_Einstein` | 1.388e-04 (0) | 0 | 4.105e-01 (4696) | 7 |
| `log_rhorad_Einstein` | 1.155e-05 (0) | 0 | 6.842e-05 (0) | 0 |
| `log_rhorad_Jordan` | 8.805e-06 (0) | 0 | 8.798e-06 (0) | 0 |
| `log_fm` | 1.958e-05 (2722) | 0 | 1.958e-05 (4893) | 0 |
| `H_Einstein` | 5.554e-04 (1782) | 0 | 5.552e-04 (4287) | 0 |
| `H_Jordan` | 5.719e-04 (1576) | 0 | 6.512e-04 (2440) | 0 |
| `log_T_Jordan` | 8.565e-06 (0) | 0 | 8.558e-06 (0) | 0 |
| `gstar_rho` | 1.533e-07 (2160) | 0 | 9.737e-08 (4327) | 0 |
| `gstar_s` | 1.451e-07 (2135) | 0 | 9.228e-08 (4327) | 0 |
| `dgstar_s_dlogT` | 3.883e-05 (364) | 0 | 8.184e-05 (187) | 0 |
| `dgstar_rho_dlogT` | 2.334e-05 (180) | 0 | 7.227e-05 (408) | 0 |
| `Sigma` | 4.441e-07 (2168) | 0 | 1.539e-06 (4642) | 0 |
| `friction_term` | 1.388e-04 (0) | 0 | 4.105e-01 (4696) | 7 |
| `reflecting_term` | 1.374e-04 (2024) | 0 | 1.023e-01 (4623) | 120 |
| `kicking_term` | 4.481e-07 (2168) | 0 | 1.560e-06 (4642) | 0 |
| `log_one_plus_z_Jordan` | 2.820e-08 (2314) | 0 | 1.541e-05 (4892) | 0 |

Quoted from note 05's §0 as the orchestrator's measurement, not re-derived here: the medians of
`H_Einstein` and `H_Jordan` are 5.55e-4, twice 2.78e-4, since H ∝ T² and the history ends at
T_J = 1 MeV; the three fields above 1e-2 at β = 5.5 lie in the tail of the history (samples
4308–4893), where φ sits against the wall at M/φ ≈ 37–60, `reflecting_term` goes as e^(M/φ) so a
relative shift δφ/φ moves it by about (M/φ) δφ/φ, and `pi_Einstein` (and `friction_term`) differ
by more than 1e-2 at seven samples next to turning points of π inside a bounce. These are
amplifications of the 2.8e-4 input and not errors. I confirmed the table, and did not repeat the
analysis.

| | β = 1.5 old → new | β = 5.5 old → new |
|---|---|---|
| `n_samples` | 2723 → 2723 | 4894 → 4894 |
| `reflections` | 0 → 0 | 0 → 0 |
| `accepted_steps` | 1231 → 1233 | 26343 → 26331 |
| `compute_steps` = `RHS_evaluations` | 9887 → 9870 | 232810 → 232530 |
| first bounce N | 18.64397081911504 → 18.64397081911831 | 32.10765422995129 → 32.10765422995111 |
| first bounce ln T_J | −43.35467531703388 → −43.35439760833413 | −37.03892534804545 → −37.03864763934282 |
| `fixed_T_values` | (0.0023279537090081345, −0.05135980555622314, None, None) → (None, None, None, None) | (None × 4) → (1.904834518987192e-05, 0.017290191260207102, None, None) |

(The four values are in the order of `fixed_T_values._fields`: `phi_Einstein_1MeV`,
`density_NP_ratio_1MeV`, `phi_Einstein_70keV`, `density_NP_ratio_70keV`.) The new
`golden_history_05.json` shows the same scalars.

**3. The golden history.** `golden_history_05.json` was written by
`PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history` (9.3 s), at generated_at
commit `91fcbf9`. `test_golden_history` alone, after: **9.739 s** test time, 11.5 s wall, at load 10.2
(before: 9.574 s, load 5.7).

**4. The suites after** (serially; `agent05/work/after.summary` holds each `uptime`):

| suite | before | after |
|---|---|---|
| `CosmologyModels/tests` | 18 | **26** (+8: `test_planck_mass.py`) |
| `ComputeTargets/tests`, `test_[a-f]*` | 61 | 61 |
| `ComputeTargets/tests`, `test_[g-z]*` | 144 | 144 |
| `ObjectFactories/tests` | 86 | 86 |
| `tools/tests` | 102 | 102 |
| `RunDrivers/tests` | 61 | 61 |

All passing. No test failed in either run, `test_prym_passenger`'s wall-clock test included.
I did not run the suites between K1/K2 and the re-pins, so the failures note 05's §0 names on the
probe tree were not reproduced by me; the re-pinned modules (41 tests) and the new golden record
were each run after being applied, and passed.
`black --check` passes on every Python file `1675e73` changes or adds.

**5. The grep** (ruling 6), `git grep -n -P '2\.436e\+?2[47]' -- . ':!prompts' ':!.documents' ':!science-runs'`:
```
ObjectFactories/tests/test_store_fingerprint_golden.py:179:        M = ins("M_value", {"value_eV": 2.436e24})
tools/tests/test_inventory_report.py:130:        M = ins("M_value", {"value_eV": 2.436e24})
tools/tests/test_inventory_report.py:273:            "values = [ 2.436e+24 eV ]",
tools/tests/test_inventory_report.py:312:            "piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, "
tools/tests/test_inventory_report.py:328:            "piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, "
tools/tests/test_inventory_report.py:351:            "- M=2.436e+24 eV, Lambda=0.001 eV, n_value=1",
```
These are exactly ruling 5's stored literals, and nothing in `Units/`. What remains in the
records, by file only:
- `.documents/`: `dk-and-rpk-migration-verification.md`; `dk-and-rpk-migration/out/` (`02-readers.txt`,
  `06-readers.txt`, `10-h1-copy-inventory.txt`, `10-readers.txt`); `ray-sqla-align/out/02-plot-reference.log`;
  `read-only-plots/out/` (`03-inventory.txt`, `03-readers.txt`); `rrk-adoption/out/04-plot-reference.log`.
- `prompts/`: `bbn-tolerance/` (`01-mechanism-and-tolerance-scan.md`, `logs/01-mechanism-and-tolerance-scan.md`,
  `source/bbn_from_store.py`, `source/brief_prym_lowT_failures.md`); `dk-and-rpk-migration/logs/09-port-the-inventory.md`;
  `jordan-normalization/` (`05-the-planck-mass.md`, `orchestrator/prompt-05.md`, `README.md`,
  `source/brief_jordan_normalization.md`); `rrk-adoption/logs/02-the-fingerprinter.md`.
  (This log and the board are inside `prompts/`, which the grep excludes.)
- `science-runs/2026-10-03-science-2026.6.0/`: `fig6_draft/fig6_draft.py`;
  `logs/refresh-bt02/` (`inventory.log`, `plot-phi1.log`, `plot-phi2.log`, `plot-phi5.log`);
  `logs/science/` (the same four).

  `ComputeTargets/tests/test_initial_field_option.py:63`'s docstring says 2.435e18 GeV and is not
  edited.

## The deliberate-breakage record

- **How each diff was made:** `diff -u` of a scratch copy of the file against the file, with the
  labels `a/…` and `b/…`.
- **How each was replayed,** in `bash` (`agent05/work/br/replay.sh`): `git apply --check`,
  `git apply`, `git apply -R --check`, then six modules (`test_planck_mass`, `test_golden_history`,
  and the four modules of ruling 13: 52 tests), then `git apply -R`. Afterwards `git diff --stat --
  Units` was as before. Both diffs apply to `1675e73`'s tree.

**1. `GeV_units` left at 2.436e27** (ruling 10.1).
- **`test_planck_mass`**, 5 failures, every one with `system='GeV_units'` and none for another
  system:
  - `test_a_Planck_mass_over_eV_is_the_constant`: 2.7774726731899513e-4 not ≤ 1e-15;
  - `test_b_kelvin_is_k_B`: 2.779174e-4 not ≤ 1e-6;
  - `test_c_inverse_eV_is_hbar_c_in_metres`: 2.777881e-4;
  - `test_d_inverse_eV_is_hbar_in_seconds`: 2.779053e-4;
  - `test_e_kilogram_is_5p6096e35_eV`: 2.779604e-4.
- No golden or re-pinned test fails (they use `Planck_units`).

```diff
--- a/Units/GeV_units.py
+++ b/Units/GeV_units.py
@@ -35,7 +35,7 @@
     eV = _GeV / 1e9
     # keV, MeV, GeV are set by base UnitsLike
 
-    PlanckMass = REDUCED_PLANCK_MASS_EV * eV
+    PlanckMass = 2.436e27 * eV
 
     sqrt_NewtonG = sqrt(1.0 / (8.0 * pi)) / PlanckMass
 
```

**2. `NEWTON_G_MEV_MINUS_2 = 6.70884e-45`** (ruling 10.2).
- **`test_planck_mass`**, 2 failures:
  - `test_a_G_N_equals_PRyM_init_as_decimals`: `Decimal('6.70883E-45') != Decimal('6.70884E-45')`
    (the PRyMordial value is the first);
  - `test_c_the_reduced_Planck_mass_is_from_the_same_G_N`: 7.45e-7 not ≤ 1e-15.
- **Ruling 4's tests (`TestTheUnitSystemsAreConsistent`) do not fail.** The change of 1.5e-6 in G
  moves M_P by 7.5e-7, and the largest of the four distances to CODATA, 2.13e-7 (the kilogram), grows
  to at most 9.6e-7, still within 1e-6. (That is arithmetic on the numbers above, not a separate run.)
- **`test_golden_history`**, 2 failures, `test_history_beta_1_5` and `test_history_beta_5_5`. The
  first differing field is sample 0's `H_Einstein`:
  - β = 1.5: recorded `0x1.c1f13d990de81p-71`, now `0x1.c1f16bec62379p-71`; `RHS_evaluations` 9870
    → 9932 and `accepted_steps` 1233 → 1231;
  - β = 5.5: recorded `0x1.69ce2539312c0p-13`, now `0x1.69ce4efc94bdcp-13`; `RHS_evaluations`
    232530 → 232442 and `accepted_steps` 26331 → 26326.
- **`test_integrator_exceptions.TestTrialStatePolicy.test_c_physical_values_unchanged`**, 1 failure:
  3.27e-7 not ≤ 1e-10. The other re-pinned modules pass, since their tolerances (1e-4 to 1e-5) are
  above the shift.
- In all: 5 failures out of 52.

```diff
--- a/Units/base.py
+++ b/Units/base.py
@@ -22,7 +22,7 @@
 # temperature literals of the three unit systems (Planck_units, Mpc_units, GeV_units) are derived.
 # It is written out here and not imported: PRyM_init reads files from the working directory at
 # import.
-NEWTON_G_MEV_MINUS_2 = 6.70883e-45
+NEWTON_G_MEV_MINUS_2 = 6.70884e-45
 
 # The reduced Planck mass in eV: 1/sqrt(8 pi G_N), converted from MeV to eV. PRyMordial's
 # `Mpl = 1./np.sqrt(GN)` is the full Planck mass from the same G_N, sqrt(8 pi) times this, so that
```

## Observations not acted on

- **PRyMordial's `GN` expression is one ulp below the literal** (ruling 14). `GN = 6.70883*1.e-39*1.e-6`
  evaluates to `0x1.3267ba6db6b1cp-147`, and `6.70883e-45` is `0x1.3267ba6db6b1dp-147`. They are the
  same decimal number. It is PRyMordial's own expression, which is not patched here. Not a defect.
  This is why `test_planck_mass.py` compares decimally.
- **The golden setup's T_stop equals `FIXED_T_JORDAN_HIGH_MEV`** (ruling 14, note §0): both are
  1 MeV, and `log(1.0e-3 * units.GeV)` and `log(1.0 * units.MeV)` are bitwise equal on both
  constants. Whether the 1 MeV fixed-T values exist is decided by rounding at the stop, and they
  flip: present at β = 1.5 before, at β = 5.5 after. A property of the golden setup, not of 05's
  change, and not an issue in production, where T_stop is far below 1 MeV.
- **The stored `M_value` literal 2.436e24 eV** in two test fixtures (`test_inventory_report.py`,
  `test_store_fingerprint_golden.py`) is now 1.000278 × 10⁻³ M_P (ruling 14).

## State handed to the next prompt

- **Tree:** `1675e73`, on `jordan-normalization`, on top of `91fcbf9`.
- **The Planck mass.** `Units/base.py` holds `NEWTON_G_MEV_MINUS_2 = 6.70883e-45` (MeV⁻², PRyMordial's
  `GN`) and `REDUCED_PLANCK_MASS_EV = 2.4353235955263046e+27` (`1.0e6 / sqrt(8.0 * pi *
  NEWTON_G_MEV_MINUS_2)`). `Planck_units.eV`, `Mpc_units.eV` and `GeV_units.PlanckMass` are set
  from it, and 1 K, 1/eV (m and s) and 1 kg agree with CODATA 2018 to ≤ 2.2e-7 in all three
  systems. Gate "Units" holds.
- **The golden history** is `ComputeTargets/tests/data/golden_history_05.json` (`GOLDEN_PATH`).
  `golden_history_01.json` and `_03.json` are kept and untouched. 06 will regenerate again if it
  changes values; the next record is `_06`.
- **What moved.** Every eV-labelled input is 2.78e-4 different, so T_init, the g*(T) thresholds
  and the adapter's Λ. The golden histories' fields moved by at most 5.7e-4, except in β = 5.5's
  wall-amplified tail (note 05's §0, reproduced in "Verification"). A pin measured on the old
  units is wrong at 3e-4: the twelve that existed are re-pinned.
- **The re-pinned references** are those of this tree, in `test_kinematic_cap_loop.py`,
  `test_integrator_exceptions.py`, `test_first_bounce.py` and (as a 1e-7 comparison)
  `tools/tests/test_plot_surfing_histories.py`. P2's wall bounces to N = 40 are 20.
- **Counts:** CosmologyModels 26, ComputeTargets 205 (61 + 144), ObjectFactories 86, tools 102,
  RunDrivers 61.
- **Open issues.** None closed or opened. `.documents/OPEN_ISSUES.md` is unchanged.
- **Breakage replay.** The two diffs above apply to `1675e73`'s tree: `git apply --check` passes
  for each, and after applying each `git apply -R --check` passes and the reverse restores the
  tree. Checked in `bash`.
- **The flaky wall-clock test** `test_prym_passenger.test_a_oscillating_case_completes` passed in
  both of this prompt's suite runs.
