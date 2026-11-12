# Log 01 — install the table

**Subject:** Install the electroweak-crossover w table as 2026.11.0
**Commit:** this commit, on branch `electroweak-eos`, on top of `85b52ac` (01a `634f141`, its review `c88e672` and a
comment-only commit)
**Date:** 2026-11-12, by Claude Opus 5.5 (two agents: the first to the stop of note 01's ruling 3, the second from
the resume to the commit)
**Result:** **COMPLETE, after two stops.** Gate Table holds (§3.1).
- **The table.** `CosmologyModels/GenericEOS/Xav_EOS_data.csv` is the U2 (b) table, 170 rows (14 of the table before,
  126 of the notebook's, the 29 points, the notebook's 386.7 GeV row), built by the new `tools/build_w_table.py` from
  committed inputs. `--check` exits 0. SHA-256 `690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d`.
- **The labels.** `VERSION_LABEL` `2026.11.0`, covering 01a as well (U11). `FINGERPRINT_FORMAT` stays 7.
- **The re-pins.** 13 tests re-pinned with stated reasons and their tolerances kept (§2.2). Three golden records were
  regenerated beside the `rr03` ones. Two item-(11) classes now run on the old table, by the user's ruling.
- **Tests:** `tools/tests/test_build_w_table.py`, 15 tests; tools 277 → 292. No other count changes.
- **Issues:** none opened, none closed.

## 1. What shipped

- **The table and its build.**
  - `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`: a byte copy of the table before. SHA-256
    `28edd854…29a8`, equal to `git show ec6bca8:CosmologyModels/GenericEOS/Xav_EOS_data.csv`.
  - `tools/build_w_table.py` composes the table by U2 (b).
    - The four rows are from §1.2: today's below T₀ = 50.1 keV, the notebook's from T₀ to 89.1 GeV, the points
      from 100 to 380 GeV, and the notebook's 386.7085 GeV row.
    - It asserts the prompt's four checks: the shared row at T₀, the 100 GeV point equal to the notebook's
      bitwise, strictly increasing T, and the left-out notebook rows on the points' spline to 10⁻⁵ in Σ.
    - It writes `repr(float(x))`. `--check` builds in memory and compares byte for byte (exit 0/1), writing
      nothing.
  - `tools/tests/test_build_w_table.py` has 15 tests:
    - regeneration and `--check`;
    - the composition and the boundaries;
    - each assertion fired on broken input;
    - the class on the installed table: Σ = 0 at and above 386.7085 GeV, and the EW peak 0.0377570 at
      54.019 GeV, to ±2 × 10⁻⁶ in Σ and 0.2 % in T.
  - `CosmologyModels/GenericEOS/Xav_EOS_data.csv`: the build's output.
  - `Xav_EOS_spline.py`: the module docstring only, rewritten for the new table (P5). Its code is unchanged.
- **The labels.** `config/version.py`: `VERSION_LABEL = "2026.11.0"`, with a dated entry. The entry says what
  changed, that it covers 01a, that every history crossing the electroweak scale may change value, and that a
  `2026.10.0` store is read with its own tree.
- **The golden records** (`ComputeTargets/tests/data/`):
  - `golden_history_ee01.json`, `golden_history_ee01_tuned.json` and `golden_history_ee01_reflecting.json` are
    new.
  - `golden_history.py`, `golden_tuned_history.py` and `golden_reflecting_history.py` point at them. Each docstring
    gains its sentence, and each provenance string now names this prompt.
- **The re-pinned tests** (§2.2):
  - `test_reachability_roots`, `test_dissipation_store`, `test_adiabatic_dark_radiation`, `test_drain_law`;
  - `test_wall_rounding`, `test_release`, `test_history_and_bbn`, `test_history_and_bbn_alpha`;
  - `test_surfing_scan`, `test_plot_surfing_histories`;
  - `test_injection` and `test_resolved_turning_points` (item (11), on the old table, §2.1).
- **`CodeComparisonTools/XavEOS_test.py`.** It had no docstring. It gains a module docstring with the dated note
  (P5). Its code and data are unchanged.
- **`.documents/electroweak-eos/table/`** (scripts, and `out/` as text):
  - `sigma_change.py` (§1.8) → `out/sigma_change.{txt,json}`;
  - `compare_goldens.py` (§1.7) → `out/compare_goldens.txt`;
  - `drain_total_by_M.py` → `out/drain_total_by_M.txt`: test_wall_rounding's reference, measured again;
  - `surfing_reference.py` → `out/surfing_reference.txt` and `out/surfing_reference_rows.csv`: the two surfing tests'
    reference rows, made again.
- **`.documents/electroweak-eos-verification.md`** (new): §1, and the pointer to `turning-points/` (01a's, not edited).
- **The board** (row 01, items, the Table gate, §5) and this log. `CLAUDE.md` is not changed (ruling 1).
  `.documents/OPEN_ISSUES.md` is not changed: no issue is opened.

## 2. Deviations from the prompt

### 2.1 The rulings

**The orchestrator's note 01 (rulings 1–6) and its Addendum (after 01a).**
1. **The user's uncommitted work is never touched.**
   - `CLAUDE.md`, the two `science-runs/` READMEs and `science-runs/2026-11-08-science-2026.10.0/relativistic/`
     were never staged, edited, stashed or reverted.
   - `CLAUDE.md` was still modified at the resume and at the commit, so its counts are not updated. The counts are
     here (§3.3) and on the board, and §6 hands them on.
   - *STRUCTURALLY REQUIRED.*
2. **The expected failures** (note §0's 22, replaced by the Addendum's 18). A test of the 18 that passes is recorded as
   such (§3.3). A failure outside them is a stop. *IMPLEMENTATION CHOICE*, ruled.
3. **The early turning point was a stop.** The first agent established it (README §8.1), and the user ruled:
   "Handle it in a separate classifier prompt; re-pin test_10 to 2026.11.0".
   - Prompt 01a is that prompt (`634f141`).
   - On this tree the pair is gone:
     - 01a's `run_tests_new_table.py`: of 33 integrations, 0 hold an early TurningPoint and 31 drop one pair;
     - 01's 20-history scan to 100 GeV: no early TurningPoint, where it had one in 14 before 01a.
   - `test_dissipation_store` test_10 was re-pinned to "2026.11.0".
   - *STRUCTURALLY REQUIRED* (the finding; the user's ruling).
4. **The golden records** are `ee01`, beside `rr03`. *As the prompt says.* For `test_history_and_bbn`, see D10.
5. **Tolerances are kept,** d1 and d2 included. Every re-pin of §2.2 keeps its test's tolerance. *IMPLEMENTATION
   CHOICE*, ruled.
6. **No file but §4's list,** less `CLAUDE.md`, plus the modules whose pins move and the golden modules.
   `test_injection` and `test_resolved_turning_points` join it on the user's ruling below. *STRUCTURALLY REQUIRED.*

**Addendum: `test_injection` test_11 was a stop before any re-pin.** Its history (β = 10, M = 0.5 M_P, α = 0.1,
parking cut off) holds no interior pair on the new table.
- On the old table it has 457 turning points and one kept pair at N = 63.8527 (T_J 36.0 MeV, 117.6 τ). On the new
  table it has 455 and none: its oscillation starts at T_J = 274 GeV (405 GeV before). Near the old stall, the closest
  approach of π to zero on a step with π > 0 at both nodes is +373 τ, and anywhere after the first turning point it is
  +96 τ.
- 01a drops nothing in it (unresolved = 0). So the pair is gone because of the table.
- **A failure outside the 18, under the same stop:** 01a's
  `test_resolved_turning_points.TestTheInteriorPairOfItem11IsKept.test_the_pair_is_kept_with_its_excursion_above_tau`
  ("0 != 1"). It integrates the same history. Its sibling `test_no_pair_is_dropped_on_this_history` passed.
- **The options put:**
  - (A) β = 4, which holds one kept pair at 225.6 τ but also the early dropped pair, so two tests would need changing;
  - (B) β = 4 at M = 10⁻³, with several pairs, which needs more changes;
  - (D) keep β = 10, read on the old table.

**Ruled by the user, 2026-11-12:** "Go with (D), keep β = 10 on the old table".
- `test_injection` gains `OLD_W_TABLE` (the campaign's `pipeline_w_table_ec6bca8.csv`) and `on_the_old_w_table()`.
  That is a patch of `pandas.read_csv` inside `Xav_EOS_spline`, which redirects only a read of `Xav_EOS_data.csv`.
- Item (11)'s class and 01a's `TestTheInteriorPairOfItem11IsKept` build their cosmology under the patch. No
  assertion changes, and each class's docstring says why.
- All four tests pass on this tree.
- *STRUCTURALLY REQUIRED* (the user's ruling). The 01a test failure outside the 18 is recorded as found and resolved
  under the same ruling.

### 2.2 The re-pins

Each failure was shown to be the table's in two ways:
- this tree on the old table (read by redirect) reproduces the old pin, or the old golden bitwise (§3.2);
- the pinned quantity is read through w along a history that crosses the electroweak scale.

| test | constant | old | new | reason |
|---|---|---|---|---|
| `test_reachability_roots` d1 | EW Σ_peak (±5e-7) | 0.037436 | 0.037757 | the new rows raise the EW peak (find_kicks: 0.03775699) |
| same, d1 | EW T_peak (1e-3 rel.) | 53.2623 GeV | 54.0188 GeV | the peak moves hotter (54.018802); also failed, note named only Σ (D3) |
| same, d2 | EW threshold centre (±5e-3) | 3.01 | 2.9992 | √((2+Σ)/(6Σ)) at the new peak = 2.999176 (3.0118 before) |
| `test_dissipation_store` test_10 | `VERSION_LABEL` | "2026.10.0" | "2026.11.0" | the label (§1.5); the user's ruling |
| `test_adiabatic_dark_radiation` 8a | `PIN_MAX_ABS_Q` | {1E1: 18.9909249914034, 1E2: 1.2320960122983178, 1E3: 0.016026391467678723, 1E4: 5.683352478895833e-05} | {1E1: 13.966453760977432, 1E2: 0.8964169114084297, 1E3: 0.022162546864849465, 1E4: 9.212758841702059e-05} | the tuned golden history it integrates (the ee01 tuning) first-bounces at 48.49 GeV, not 1.68 GeV |
| same | `PIN_SHA256` | `a59c5cc7…6c0f` | `740c6c8d…1cd9` | as above; `PIN_SAMPLES` 5960 unchanged |
| `test_drain_law` 4 | `PINNED_ALPHA_EFF` | ('8.2276e-02', '9.1886e-02', '1.1431e-01', '1.2001e-01', '1.6459e-01') | ('8.2277e-02', '9.1883e-02', '1.1431e-01', '1.2001e-01', '1.6185e-01') | 2197's history moves with the table; still five candidates, all one_loop |
| same | `PINNED_PARKING_N` (±1e-9) | 29.035264723467392 | 28.876391476457353 | as above |
| `test_wall_rounding` 3 | `SIGMA_PLUS_LEFTOVER_AT_1E_30` | 0.012616907 | 0.012616956 | log 06's reference, a measurement on the old table, measured again (D13); M-dependence 6.5e-9, inside the kept 2.5e-8 |
| `test_release` 2 | `BETA_5_FAILED_CHECK_N` | 40.29068 | 40.31008 | the history parks at N = 37.924 (38.004 before), so the parked step grid moves |
| same | `BETA_5_PARKED_CROSSING_N` (5e-6 + τ_N) | 40.25454 | 40.25453 | measured 40.254529; fails once N_check is re-pinned (D11) |
| `test_release` 7 | parking `candidate_index` | 6 | 8 | β = 1.2, M = 10⁻³ parks at N = 32.045 (28.372 before); 8 before 01a too; release N 45.91 and 0.27 meV unchanged |
| `test_history_and_bbn` a, b | `GOLDEN` | `golden_history_06_tuned.json` | `golden_history_ee01_tuned.json` | the tuned golden record moved (D10) |
| `test_history_and_bbn_alpha` 10 | `PINNED_ALPHA_ZERO` | RHS=33838 accepted_steps=3725, bounce N=20.342271137 T_J=746.622784 MeV, … | RHS=33721 accepted_steps=3715, bounce N=20.342268911 T_J=746.624260 MeV, … (the whole masked stdout) | the β = 2, M = 0.5 history moves with the table |
| `test_surfing_scan` β = 1.5 | `COMMITTED_CSV` (row) | surfing-temperature 04's scan: T_capture 0.3211029090023808, T_deliver 0.3613940758101156 GeV | `.documents/electroweak-eos/table/out/surfing_reference_rows.csv`: 0.32110735128889967, 0.36139673150961826 GeV | the row was a measurement on the old table, made again on the new one (D12); 1e-6 kept, now met at 2.5e-10 and 1.1e-8 |
| `test_plot_surfing_histories` β = 3 | `NEAR3_CSV` (row) | T_deliver 1.6800110848779357 GeV, N_deliver 24.498974358027958 | 48.490199737754835 GeV, 21.064967186164502 | as above; β = 3 is still not captured (§3.4) |

**Following the path, not re-pinned.** The golden tests (`test_golden_history` ×2, `test_golden_reflecting_history`),
`test_tuning` (e) and `test_tuned_history_round_trip` test_a. Each reads its module's path and passes against `ee01`.

**Passing on this tree before any re-pin of mine** (01a's four): `test_injection` test_1, both test_2s, and
`test_drain_store`'s extremes test.

### 2.3 Others

- **D1.** Note 01 §0 said the early wall turning point "becomes first_bounce". Measured, it did not:
  `first_bounce` reads the step nodes' signs (README §8.1). A finding; no change.
- **D2.** `test_build_w_table.py` was in the tree during the first agent's old-table tools run (292 tests, not 277;
  its 9 tests and 3 subtests failing on the old table as expected; all 277 others passed). *UNINTENDED DRIFT*,
  harmless, and it serves as breakage evidence (§4).
- **D3.** d1's T_peak was re-pinned as well as Σ_peak. It fails on the table, and note 01 named only Σ.
  *STRUCTURALLY REQUIRED.*
- **D4.** d2's centre is 2.9992: four decimals, the measured value rounded, as 3.01 was the paper's. *IMPLEMENTATION
  CHOICE.*
- **D5.** `test_dissipation_store` test_10 was a failure outside note §0's table, re-pinned on the user's ruling.
  *STRUCTURALLY REQUIRED.*
- **D6.** `sigma_change.py` also reports the parse-level node differences. *IMPLEMENTATION CHOICE.*
- **D7.** The build parses with Python's `float()` (the csv module), not pandas, because pandas' default parser is
  not correctly rounded. *IMPLEMENTATION CHOICE.*
- **D8.** `test_build_w_table`'s EW-peak pins come from `minimize_scalar`, checked on a 3000-per-decade grid. The
  prompt says "near 54.0 GeV with Σ ≈ 0.03776". *IMPLEMENTATION CHOICE.*
- **D9.** Not edited: `test_kicking_function.py`'s and `eos_reference.py`'s comments that describe the old table's
  top row and peak. No pin there moves (ruling 6). The top-row pins still pass, and still test w = 1/3 at and above
  T_max, read from the class. §5.
- **D10. `test_history_and_bbn` hard-codes the tuned golden path.**
  - Ruling 4 said it reads the tuned record through `golden_tuned_history.GOLDEN_TUNED_PATH`. It does not:
    `GOLDEN = ROOT / "ComputeTargets/tests/data/golden_history_06_tuned.json"`. That file's tuning keys are
    `rr03`'s.
  - Test a failed on the tuning keys and test b on the counts (18050 vs 15331).
  - `GOLDEN` now names `golden_history_ee01_tuned.json`, as a path constant in the module's own form, with the
    docstring updated.
  - *STRUCTURALLY REQUIRED* (the test cannot pass otherwise). A deviation from ruling 4's letter, within its intent.
- **D11.** `test_release`: `BETA_5_PARKED_CROSSING_N` was re-pinned besides the first failing assertion, since it
  fails once N_check is re-pinned. *STRUCTURALLY REQUIRED.* test_7's name ("parks_at_6") is kept, so that test ids
  recorded elsewhere stay valid; a comment says 8. *IMPLEMENTATION CHOICE.*
- **D12. The surfing tests' reference rows.**
  - Both tests compared a tuned history with a row of surfing-temperature 04's committed scans. Those scans were
    untuned, at Λ = 1 meV, stopped at 1 MeV, on the old table. They are a record (invariant 6) and are not
    rewritten.
  - `table/surfing_reference.py` makes the rows again the same way. On the old table it reproduces the committed
    rows to 3 × 10⁻¹² (β = 1.5) and 2.1 × 10⁻⁸ (β = 3, T_deliver).
  - The tests now read `table/out/surfing_reference_rows.csv`. *STRUCTURALLY REQUIRED* (the reference is a measurement
    on the old table).
  - Remaking the untuned rows, rather than pointing at a tuned row of today's tool (which would compare the tool
    with itself), is an *IMPLEMENTATION CHOICE*.
- **D13.** `test_wall_rounding`'s reference is log 06 §5.4's measurement at M = 10⁻³⁰ M_P on the old table.
  `table/drain_total_by_M.py` measures it again: 0.012616907077 on the old table, 0.0126169558132557 on the new. The
  pin is the new value to eight figures, as log 06's was. *STRUCTURALLY REQUIRED.*
- **D14.** Each golden module's provenance string (`code_tree`) now names this prompt rather than reflection-restart
  03. It is not compared. *IMPLEMENTATION CHOICE.*
- **D15.** `config/version.py`'s entry also states that 2026.11.0 covers 01a's classifier change (U11). Without it,
  "no code that computes changes" would be false over the label's span. *IMPLEMENTATION CHOICE.*
- **D16.** `XavEOS_test.py` had no docstring, so its dated note is a new module docstring. *IMPLEMENTATION CHOICE.*
- **D17. How the suites were timed.**
  - The first new-table run (§3.3) ran beside my probes and the golden regeneration, so its times carry that load.
  - Re-pins to CT, OF and tools modules were made while that run was going, after each suite had imported its
    modules, so that run shows the pins as they were.
  - The second run had nothing beside it.
  - *IMPLEMENTATION CHOICE.*
- **D18.** The goldens' breakage and the "old table reproduces rr03" check use a redirect of the class's CSV read. The
  production CSV was never swapped back, because the suites were running. *IMPLEMENTATION CHOICE.*
- **D19.** Re-pinned values are this tree's outputs. `test_drain_law` 4's are not cross-checked against
  `measured_law_map.py`, as 02c's were. *IMPLEMENTATION CHOICE.*
- **D20.** Two scripts beyond the prompt's two in `table/` (D12, D13). *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 Gate Table

- **The build regenerates the CSV byte for byte:** `--check` exits 0 (170 rows: old 14, notebook_low 126, points 29,
  notebook_top 1). `test_build_w_table` regenerates and compares.
- **The class evaluates the new table:** Σ = 0 at and above 386.7085 GeV, and the EW peak is 0.037757 at 54.0188 GeV
  (`minimize_scalar`; 53.962 GeV on the 1000-per-decade grid). Above 40 GeV there is one maximum. The e± and QCD peaks
  are unchanged.
- **Every moved pin is listed** with old, new and reason (§2.2).
- **Below the first changed row, the spline's coupling**, measured (verification §1.3):
  - 10⁻⁵ GeV to T₀: |ΔΣ| ≤ 2.309 × 10⁻¹⁴;
  - T₀ to 22 GeV: ≤ 2.064 × 10⁻⁵, at 21.98 GeV, in the interval whose upper row changed.
- ∫Σ d ln T is 0.041830, 0.016524 and 0.000164 over 30–100, 100–300 and 300–25 000 GeV, against 0.040662, 0.016101
  and 0.003090 on the old table. The new values equal the addendum's merged table to the digits it prints.
- **Labels:** `VERSION_LABEL` 2026.11.0. `FINGERPRINT_FORMAT` 7, with `test_fingerprint_format_guard` and
  `test_store_fingerprint_golden` passing unchanged.

### 3.2 The goldens

- Regenerated once, on this tree, by each module's `main`, at 22:07–22:08 UTC (17.0 s, 35.8 s, 4.5 s, at loads
  12–19). The `rr03` files' SHA-256 are equal before and after.
- `compare_goldens.py` ends **FORM: SAME** (`table/out/compare_goldens.txt`). The per-history values are in
  verification §1.5. In brief:
  - β = 1.5: first bounce N 18.64397 → 18.64396, accepted steps 1233 → 1219;
  - β = 5.5: N 32.1077 → 32.1245, steps 26 331 → 28 511;
  - tuned β = 3: first bounce 1.68 → 48.49 GeV, steps 15 331 → 18 033; normalization −277.4502 → −277.4502002, φ_ref
    0.06222843896 → 0.06222843232 M_P;
  - reflecting: 32 → 35 reflections, steps 7183 → 7722; normalization and φ_ref unchanged.
- **The moves are the table's.** With the paths patched back to `rr03` and the old table read by redirect, this tree
  passes `test_golden_history`, `test_golden_reflecting_history` and `test_tuning` (e): 6 tests, OK.

### 3.3 The suites

Serial, from the repository root, ComputeTargets in two halves, through `run_suites.sh` in the session scratchpad.

| suite | first agent, old table, before 01a (14:12 UTC) | 01a, after (its worktree) | new tree, goldens at ee01, before the other re-pins (22:11 UTC) | new tree, final (22:39 UTC) |
|---|---|---|---|---|
| CosmologyModels | 26 OK, 73.4 s (32.22) | 26 OK | 26 OK, 74 s (7.29) | 26 OK, 66 s (4.62) |
| ObjectFactories | 163 OK, 130.8 s (26.40) | 163 OK | 163, 1 fail (adiabatic 8a), 158 s (32.28) | 163 OK, 134 s (3.21) |
| RunDrivers | 76 OK, 25.1 s (15.11) | 76 | 76 OK, 30 s (12.36) | 76 OK, 25 s (3.12) |
| ComputeTargets `[a-f]` | 219 OK, 151.6 s (12.54) | 219 OK | 219, 1 fail (drain_law 4), 184 s (11.69) | 219 OK, 155 s (3.46) |
| ComputeTargets `[g-z]` | 446 OK, 536.4 s (13.06) | 465 OK | 465, 5 fail (injection test_11; release 7 and 2; wall_rounding 3; resolved_turning_points item-11 kept), 680 s (8.45) | 465 OK, 544 s (7.41) |
| tools | 292, 12 fail (test_build_w_table only, D2), 282.6 s (14.53) | 277 | 292, 5 tests fail, 14 failures with subtests (history_and_bbn a and b; alpha 10; plot_surfing β = 3; surfing β = 1.5), 339 s (3.17) | 292 OK, 320 s (8.60) |

The figure in brackets is the one-minute load at the suite's start. `uptime` at the ends:
- the first agent's: 14:35, loads 10.52 / 14.19 / 17.71;
- the third column's: 22:36, loads 11.64 / 6.43 / 7.90;
- the final run's: 23:00, loads 16.74 / 13.93 / 10.55 (nothing else of mine ran beside it).

**No count fell.** From 01a's counts: CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories 163, tools
277 → 292 (+15, `test_build_w_table`), RunDrivers 76.

**The Addendum's 18 on this tree, before re-pinning.**
- Pass (7): golden β = 1.5 and 5.5, the reflecting golden, `test_tuning` (e) and `test_tuned_history_round_trip` a
  (all five by path); reachability d1 and d2 (re-pinned by the first agent).
- Fail, then re-pinned or ruled (11): `test_injection` test_11 (§2.1); `test_release` 7 and 2; `test_wall_rounding` 3;
  `test_drain_law` 4; `test_adiabatic_dark_radiation` 8a; `test_history_and_bbn` a and b (D10);
  `test_history_and_bbn_alpha` 10; `test_surfing_scan` β = 1.5; `test_plot_surfing_histories` β = 3.
- Outside the 18, the one failure was 01a's item-(11) kept-pair test (§2.1).

### 3.4 β = 3 at the electroweak scale

**Not captured, by the scan's definition.** At M = 10⁻³ M_P there is no crossing of βπ/M_P = −1 before the first
bounce, tuned or untuned. The stall deepens:
- the least g = 1 + βπ/M_P before the bounce is 0.00321 at T_J = 50.14 GeV (N = 13.160), against 0.02248 at 46.46 GeV
  before;
- the first bounce moves from 1.680 GeV (N = 24.499) to **48.49 GeV** (N = 21.065), beside the EW peak.

The tuned golden history (β = 3, M = 0.01) does the same: its first bounce is at 48.49 GeV. The reachability
prediction from the peak Σ is 2.99918, below 3. The threshold is 02's.

## 4. The deliberate-breakage record

- **The build test.**
  - One byte of the installed CSV changed (row 100, `0.30576` → `0.30577`): 4 tests of `test_build_w_table` fail
    and `--check` exits 1. Restored: SHA-256 `690090c8…0119d` and `--check` exit 0.
  - On the old table, 9 tests and 3 subtests of the module fail (D2).
- **The golden tests on the old table, their paths at `ee01`.** With the class's CSV read redirected to the old table,
  4 of the same 6 tests fail (`test_golden_history` β = 1.5 and 5.5, the reflecting record, `test_tuning` (e)), each
  "differs from golden_history_ee01*.json". The production CSV was never moved (D18).
- **The item-(11) classes without the redirect.** In the third run of §3.3 they ran on the installed table without
  `on_the_old_w_table()`, and test_11's first test and 01a's kept-pair test failed. With it, all four pass.
- **The surfing reference and the drain reference.** Run on the old table, the two scripts reproduce the committed
  rows and log 06's value (D12, D13). On the new table the tests' old references fail (§3.3, third run).

## 5. Observations not acted on

- **pandas' default `read_csv` is not correctly rounded.**
  - T differs from Python's `float()` by up to 8.6 × 10⁻¹³ relative on the new CSV and 5.0 × 10⁻¹² on the old.
  - Rewriting the 14 old rows below T₀ as `repr` text moves the class's nodes there by up to 5.02 × 10⁻¹² relative
    (1 of 14 bitwise equal).
  - The class reads with the default parser. Not changed (§2 of the prompt).
- **Stale comments about the old table:**
  - `CosmologyModels/tests/test_kicking_function.py`: the top row 25 118.86 GeV / 25.1 TeV, and "0.037436 at
    53.2214 GeV";
  - `CosmologyModels/tests/eos_reference.py`: "Xav's table ends at 25 TeV".
  - No pin there moves (D9).
- **Stale names of golden files in docstrings:**
  - `test_tuning` (e) and its module docstring: `rr03_tuned`;
  - `test_golden_reflecting_history`: `rr03_reflecting`;
  - `ObjectFactories/tests/test_tuned_history_round_trip.py`: `06_tuned`;
  - `tools/history_and_bbn.py`'s docstring: `06_tuned`.
  - The tests read the paths, not the names.
- **Stale comments in `test_release`:** `WINDOW_IN_THE_PARKED_PHASE` and `SENTINEL_WINDOW` cite the old switch
  (38.004), the old release step [40.19089, 40.29089] and the first candidate after the release (41.301). The
  constants still hold on the new table: the switch is now at 37.924 and the release step at [40.21008, 40.31008].
  test_7's name keeps "parks_at_6" (D11).
- **`test_injection`'s docstring.** It says the parking cut ends item (11)'s oscillation at 358 MeV. Item (11) now
  runs on the old table, where the park is at N = 61.073 (T_J 366 MeV by this probe's conversion). Not changed.
- **test_11's second test conflicts with 01a.**
  - The test asserts that no step's 17 samples show more sign changes than the loop recorded. That no longer holds
    for 01a's code on any history that holds the early pair: the dropped pair's step shows 2 changes against 0
    recorded.
  - Measured with β = 4 substituted: "2 not less than or equal to 0 : 43". The early pair at T_J 392.7 GeV has
    excursion 0.295 τ.
  - It passes on item (11)'s history only because that history has no early pair on either table.
  - No issue is opened. The test is right for the one history it runs, which is now fixed on the old table. A second
    use of the invariant would need it restated as "more than recorded, less the pairs dropped".
- **The item-(11) classes now read a campaign data file** (`prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`).
  So they no longer follow the production table. The interior sampling stays guarded on the case where it was ruled,
  but a future change of the production table will not move them. Moving or deleting that file breaks them.
- **The β = 4, M = 0.5 history holds a resolved interior pair on the new table** (225.6 τ, T_J 17.18 MeV). It is a
  knife edge: none at β = 3.9 or 4.1, nor at α = 0.05, 0.15 or 0.2. It was not adopted.
- **`CodeComparisonTools/XavEOS_test.py` reads `CosmologyModels/GenericEOS/XavEOS_data.csv`**, which is not there. The
  file is in `CodeComparisonTools/`. This predates the prompt.
- **The β = 1.5 and 5.5 goldens stop at 1 MeV**, and their fixed-temperature values at 1 MeV flip between present and
  None on the new table: β = 1.5 gains them, and β = 5.5 loses them. The last sample's reach of 1 MeV is a knife
  edge.
- **P2197's first candidate's A_k** went from 387.6 to 1.59 × 10⁵ on the new table before 01a, because of the early
  pair. It was not measured again on this tree. `test_drain_law` 4's alpha_eff are this tree's.
- **The reflecting golden's tuning prints two "not relativistic" margin warnings** (generations 6 and 9) on the new
  table. Whether the old one printed them was not compared.

## 6. State handed to the next prompt

- **For 02:**
  - "new" is this commit; "old" is `ec6bca8` (or any tree before this commit), in a worktree, never this tree with the
    old CSV swapped in.
  - The tuned golden history (β = 3, M = 0.01 M_P), old → new:
    - normalization −277.4502 (`rr03`, hex in the record) → −277.4502002;
    - φ_ref 0.06222843896 → 0.06222843232 M_P;
    - first bounce at T_J = 1.680 → 48.49 GeV.
  - The reflecting golden's tuning is unchanged.
- **The suite counts:** CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories 163, tools 292, RunDrivers
  76. **`CLAUDE.md`'s counts are to be updated once the user has committed their edits to it** (ruling 1). The tools
  count rises by 15 (`test_build_w_table`).
- **The surfing tests' reference rows** are `table/out/surfing_reference_rows.csv`, this tree's untuned rows at β = 1.5
  and 3. 02's S1 scan on the new table will be the campaign's record of the scan. Whether the tests should then read
  it is for 02 or 03.
- **The item-(11) classes run on the old table** (the user's ruling). The campaign data file they read must stay.
- **β = 3 is not captured but delivers at 48.49 GeV.** The reachability prediction is 2.99918. The EW threshold is 02's
  to bracket.
- No issue opened or closed; `.documents/OPEN_ISSUES.md` is unchanged.
