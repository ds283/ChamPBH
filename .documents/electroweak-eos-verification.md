# electroweak-eos — verification

**Campaign:** [`prompts/electroweak-eos/`](../prompts/electroweak-eos/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/electroweak-eos/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`electroweak-eos/table/`](electroweak-eos/table/) (prompt 01),
[`electroweak-eos/turning-points/`](electroweak-eos/turning-points/) (prompt 01a)

This document records what the campaign measured. It is additive (`CLAUDE.md` invariant 6): a re-run that
supersedes a figure gets a new dated section, and the figures below stay as they are, correct for the tree
they were taken on. **Measurements, not bounds** (README §5): the numbers are reported; none is tested
against a threshold the user has not ruled.

The turning-point classifier of prompt 01a, which landed before prompt 01's commit (README §8), is recorded
in [`electroweak-eos/turning-points/`](electroweak-eos/turning-points/): its `measurements.md`, its scans and
its breakage record, under `out/`. Log 01a is
[`prompts/electroweak-eos/logs/01a-resolved-turning-points.md`](../prompts/electroweak-eos/logs/01a-resolved-turning-points.md).
Prompt 01 does not edit that folder.

---

## 1. The table (prompt 01, 2026-11-12)

**Tree:** `85b52ac` (01a `634f141`, its review and a comment-only commit) with prompt 01's changes, before
its commit. No Ray, no store but the tests' temporary ones, no network.

### 1.1 The inputs

| file | rows | SHA-256 |
|---|---|---|
| `.documents/peakewsurf-kicking-function/data/notebook_w_table.csv` (the notebook's table; read, not modified) | 132 | `489e1e24b3223d522f1c524b956eacfd0d54a1615b3471de59e87b0a7e36e4e7` |
| `.documents/peakewsurf-kicking-function/data/notebook_w_points_2026-11-12.csv` (the 29 points; read, not modified) | 29 | `87444ca597233704881b7c5900d88be1aaff26d7df9fc8b216fa26dfd4142447` |
| `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv` (the table before, copied byte for byte) | 189 | `28edd85477f3b508ccf977f05faf79b3dfd76358b8be9ee6f358a45b9eda29a8`, equal to `git show ec6bca8:CosmologyModels/GenericEOS/Xav_EOS_data.csv` |

### 1.2 The build

`tools/build_w_table.py` composes the table by U2 (b) and writes `CosmologyModels/GenericEOS/Xav_EOS_data.csv`
(header `T_GeV,w`, each number as `repr(float(x))`). The installed CSV has SHA-256
`690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d`, and `--check` exits 0:

```
build_w_table --check: Xav_EOS_data.csv is the build (170 rows: old 14, notebook_low 126, points 29, notebook_top 1)
```

| from | rows | range |
|---|---|---|
| the table before | 14 | 10 keV to below T₀ = 50.1 keV (5.011872336272725 × 10⁻⁵ GeV) |
| the notebook's table | 126 | T₀ to 89.1 GeV, 0.05 dex |
| the points of 2026-11-12 | 29 | 100 to 380 GeV, 10 GeV |
| the notebook's table | 1 | 386.7085411060535 GeV, w = 1/3 exactly |

The build's assertions hold: the row at T₀ is shared by both tables (w equal to 10⁻¹²); the points' 100 GeV
row is the notebook's bitwise; T strictly increases; and the notebook's rows left out lie on a cubic spline
through the points to |ΔΣ| = 3.73 × 10⁻⁶ (158.5 GeV), 5.63 × 10⁻¹² (251.2 GeV) and 9.47 × 10⁻¹² (316.2 GeV),
against 10⁻⁵. `tools/tests/test_build_w_table.py` (15 tests) checks the regeneration, the composition, each
assertion fired on broken input, and the class on the installed table.

### 1.3 What the table does to Σ

`table/sigma_change.py` evaluates Σ = 1 − 3w through `Xav_EOS_spline` on both tables (the CSV read
redirected), on 1000 points per decade from 10⁻⁵ to 3 × 10⁴ GeV; output `table/out/sigma_change.txt`.

| decade (GeV) | largest \|ΔΣ\| (new − old) | at T (GeV) |
|---|---|---|
| 10⁻⁵–10⁻⁴ | 1.175 × 10⁻¹³ | 8.831 × 10⁻⁵ |
| 10⁻⁴–10⁻³ | 1.543 × 10⁻¹⁴ | 1.04 × 10⁻⁴ |
| 10⁻³–10⁻² | 2.22 × 10⁻¹⁶ | 1.007 × 10⁻³ |
| 10⁻²–10⁻¹ | 4.44 × 10⁻¹⁶ | 0.0980 |
| 10⁻¹–1 | 4.22 × 10⁻¹⁵ | 0.443 |
| 1–10 | 3.046 × 10⁻¹⁰ | 9.574 |
| 10–100 | 2.803 × 10⁻³ | 99.79 |
| 100–1000 | 4.058 × 10⁻³ | 336.6 |
| 10³–10⁴ | 6.031 × 10⁻⁴ | 1000.2 |
| 10⁴–3 × 10⁴ | 6.290 × 10⁻⁶ | 10003 |

Where the two tables' rows hold the same values, a difference comes from the spline's global coupling and
from how the rows' text parses:
- 10⁻⁵ GeV to T₀: largest |ΔΣ| 2.309 × 10⁻¹⁴, at 4.37 × 10⁻⁵ GeV;
- T₀ to 22 GeV: largest |ΔΣ| 2.064 × 10⁻⁵, at 21.98 GeV, in the spline interval [19.95, 22.39] GeV whose upper
  row is one the notebook changed.
- The class's nodes there (pandas' default parser): below T₀, w bitwise equal in 14 of 14 rows and T in 1,
  T within 5.02 × 10⁻¹² relative; T₀ to 22 GeV, w bitwise in 93 of 113 (|Δw| ≤ 1.28 × 10⁻¹⁵), T in 77
  (within 8.37 × 10⁻¹³).

**The electroweak peak.** On the grid: old Σ = 0.037436 at 53.221 GeV, new 0.037757 at 53.962 GeV. From
`minimize_scalar` on the class's spline (the class docstring and `test_build_w_table`): old 0.037436 at
53.26 GeV, new 0.037757 at 54.0188 GeV. The e± and QCD peaks are unchanged (0.100732 at 0.16045 MeV, 0.314532
at 0.181893 GeV). Σ is 0 at and above 386.7085 GeV.

**∫Σ d ln T** (trapezoid, 20 001 points in ln T), beside the addendum's figures:

| T (GeV) | old | new | addendum, merged | addendum, pipeline | Laine & Meyer |
|---|---|---|---|---|---|
| 30–100 | 0.040662 | 0.041830 | 0.041830 | 0.040600 | 0.040787 |
| 100–300 | 0.016101 | 0.016524 | 0.016524 | 0.016119 | 0.016496 |
| 300–25 000 | 0.003090 | 0.000164 | 0.000164 | 0.003101 | −0.000594 |

### 1.4 The labels

- `VERSION_LABEL` = `2026.11.0` (was `2026.10.0`), with a dated entry in `config/version.py`. It covers 01a
  too (README §8.3 U11).
- `FINGERPRINT_FORMAT` stays 7. No `inventory_spec()` changes; `test_fingerprint_format_guard` and
  `test_store_fingerprint_golden` pass unchanged.

### 1.5 The golden records

Each record was regenerated once, on this tree, by its module's own regeneration, beside its `rr03` record:
`golden_history_ee01.json`, `golden_history_ee01_tuned.json` and `golden_history_ee01_reflecting.json`
(`ComputeTargets/tests/data/`). The `rr03` files are unchanged (SHA-256 checked before and after).

**That the moves are the table's.** With the module paths patched back to the `rr03` files and the class's
CSV read redirected to the table before, this tree passes `test_golden_history`, `test_golden_reflecting_history` and `test_tuning` (e), 6
tests, bitwise. With the paths at `ee01` and the old table, 4 of those 6 fail (both histories of
`test_golden_history`, the reflecting record, and (e)).

`table/compare_goldens.py` compares each old record with its new one field by field
(`table/out/compare_goldens.txt`); it ends **FORM: SAME** (no change of keys, `solver_label`,
`sample_fields`, `n_samples` or the excerpts' indices and ln(1 + z)). Per history, old → new:

| | β = 1.5 | β = 5.5 | tuned (β = 3, M = 0.01) | reflecting (β = 1.5, M = 10⁻¹², α = 0.05) |
|---|---|---|---|---|
| reflections | 0 → 0 | 0 → 0 | 0 → 0 | 32 → 35 |
| accepted steps | 1233 → 1219 | 26 331 → 28 511 | 15 331 → 18 033 | 7183 → 7722 |
| RHS evaluations | 9870 → 9776 | 232 530 → 253 131 | 140 823 → 167 541 | 52 403 → 56 192 |
| first bounce N | 18.64397082 → 18.64396256 | 32.10765423 → 32.12445967 | 24.49869253 → 21.06474689 | 18.64398464 → 18.64397638 |
| first bounce φ_E (M_P) | 9.360344868e-6 → 9.360455674e-6 | 7.689203375e-6 → 7.69339784e-6 | 9.226888066e-5 → 8.176202509e-5 | 6.542680466e-11 → 6.427659008e-11 |
| first bounce ln T_J | −43.35439761 → −43.35439026 | −37.03864764 → −37.05535548 | −41.81778017 → −38.45525062 | −43.35439742 → −43.35439007 |
| φ_E at 1 MeV | None → 0.002237413273 | 1.904834519e-5 → None | 0.001715824618 → 0.0003921873569 | 0.04273364431 → 0.04279691336 |
| N/P ratio at 1 MeV | None → −0.05162692717 | 0.01729019126 → None | −0.3426786886 → −0.3091360702 | 0.09993469865 → 0.1001455468 |
| φ_E at 70 keV | None → None | None → None | 0.0003336060566 → 0.0002000490875 | 0.002597925968 → 0.002541979815 |
| N/P ratio at 70 keV | None → None | None → None | −0.2951970361 → −0.3169850434 | 0.07090111551 → 0.07184646442 |
| final φ_E (M_P) | 0.002327991794 → 0.002237413273 | 1.904834519e-5 → 1.687612514e-5 | 0.06069406648 → 0.0606940601 | 1.214931227e-6 → 1.214932051e-6 |
| final π_E (M_P) | −0.01962978216 → −0.01963302553 | 0.001545850448 → −0.001501390625 | 0.07384968753 → 0.07384973609 | 1.82239443e-6 → 1.822395667e-6 |
| final H_E | 1.824053867e-43 → 1.822492244e-43 | 1.811680259e-43 → 1.810578239e-43 | 4.829256566e-61 → 4.829256065e-61 | 5.926380907e-61 → 5.926379338e-61 |

The β = 1.5 and 5.5 histories stop at T_J = 1 MeV, and their fixed-temperature values at 1 MeV are present
or absent according to whether the history's last sample reaches 1 MeV; that flips on both. Every other
final-sample field is in `compare_goldens.txt`.

For the tuned records, old → new:
- tuned (β = 3, M = 0.01 M_P): normalization −277.4502 → −277.4502002 (+6.8 × 10⁻¹⁰ relative), φ_ref
  0.06222843896 → 0.06222843232 M_P (−1.07 × 10⁻⁷), residual_H −7.03 × 10⁻⁷ → −6.89 × 10⁻⁷, residual_φ
  4.69 × 10⁻⁶ → 4.71 × 10⁻⁶, five integrations on both; its first bounce moves from T_J = 1.68 GeV to
  48.49 GeV, beside the new electroweak peak;
- reflecting (β = 1.5, M = 10⁻¹² M_P, α = 0.05): normalization and φ_ref unchanged (−276.6307374,
  1.214931884 × 10⁻⁶ M_P; the seed is accepted in one integration on both), residual_H 3.24 × 10⁻⁷ →
  5.97 × 10⁻⁸, residual_φ 5.41 × 10⁻⁷ → −1.38 × 10⁻⁷; 35 floor reflections and injections, not 32.

### 1.6 The re-pinned tests

Every test the table moves was re-pinned to this tree's value with its tolerance kept, or follows the golden
record's path (the golden tests, `test_tuning` (e), `test_tuned_history_round_trip` a). Each failure was shown to
be the table's: on the old table, read by redirect, this tree gives the old value or the old golden bitwise (log 01
§2.2, §3.2).

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

Two of the re-pins rest on references measured again, not on the test's own output:
- `table/drain_total_by_M.py` (`out/drain_total_by_M.txt`): test_wall_rounding's reference, Σ Δρ/ρ_R plus
  the parking leftover at β = 2, α = 0.1, M = 10⁻³⁰ M_P. On the table before it gives 0.012616907077 (log 06
  §5.4's 0.012616907); on the new table 0.0126169558132557, with an M-dependence over 10⁻⁷ to 10⁻³⁰ M_P of
  6.5 × 10⁻⁹ relative.
- `table/surfing_reference.py` (`out/surfing_reference.txt`, `out/surfing_reference_rows.csv`): the untuned
  scan rows at β = 1.5 and 3.0 (Λ = 1 meV, stopped at 1 MeV, M = 10⁻³ M_P), made as surfing-temperature 04's
  scans made theirs. On the table before they reproduce the committed rows to 3 × 10⁻¹² (β = 1.5) and
  2.1 × 10⁻⁸ (β = 3, T_deliver). test_surfing_scan and test_plot_surfing_histories read the new rows.

**β = 3 at the electroweak scale.** On the new table the β = 3, M = 10⁻³ M_P history (untuned, as the
scans) is still **not captured** by the scan's definition: it has no crossing of βπ/M_P = −1 before its first
bounce (`n_crossings` 0, tuned and untuned). Its stall deepens: the least g = 1 + βπ/M_P before the bounce
is 0.00321 at T_J = 50.14 GeV (N = 13.160), against 0.02248 at 46.46 GeV on the table before. Its first
bounce moves from T_J = 1.680 GeV (N = 24.499) to **48.49 GeV** (N = 21.065), so it is delivered beside the
electroweak peak without a crossing. The reachability prediction from the peak Σ falls from 3.0118 to
2.99918. The threshold itself is prompt 02's.

### 1.7 test_injection's item (11), kept on the old table

On the new table, item (11)'s history holds no interior pair. That history is β = 10, M = 0.5 M_P, α = 0.1,
test_injection's setup, with the parking cut off.

| | old table | new table |
|---|---|---|
| turning points | 457 | 455 |
| injections | 229 | 228 |
| first turning point | N = 53.858, T_J = 404.7 GeV | N = 54.248, T_J = 274.0 GeV |
| steps with two turning points | one: [63.852649, 63.854452], T_J 36.0 MeV, plateau then wall, π > 0 at both nodes, excursion 117.6 τ | none |
| unresolved pairs (01a) | 0 | 0 |

- Near the old stall, π on a step with π > 0 at both nodes comes no closer to zero than +373 τ (N = 63.91, 34.2 MeV).
  After the first turning point, the closest approach anywhere is +96 τ.
- So the pair is gone because of the table, not 01a's rule (log 01a §6).
- 01a's `TestTheInteriorPairOfItem11IsKept` failed with it, outside the Addendum's 18.

**Ruled by the user, 2026-11-12:** "Go with (D), keep β = 10 on the old table".
- Both classes build their cosmology with `Xav_EOS_spline` reading
  `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv` (`test_injection.on_the_old_w_table()`).
- No assertion changed, and all four tests pass.
- At M = 0.5 on the new table, of 20 β from 3.0 to 20 at α = 0.1 and six other (β, α), only β = 4 at α = 0.1
  holds a resolved interior pair (225.6 τ at 17.18 MeV). It also holds the early dropped pair, which test_11's
  second test would count (log 01 §5).

### 1.8 The suites

Before and after (log 01 §3.3): no count fell. CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories
163, tools 292 (277 + 15 for `test_build_w_table`), RunDrivers 76.

The final run (2026-11-12, 22:39–23:00 UTC, serial; seconds, with the one-minute load at the start): CosmologyModels
26 OK (66 s, 4.62); ObjectFactories 163 OK (134 s, 3.21); RunDrivers 76 OK (25 s, 3.12); ComputeTargets `[a-f]` 219 OK
(155 s, 3.46); `[g-z]` 465 OK (544 s, 7.41); tools 292 OK (320 s, 8.60). `uptime` at the end: loads 16.74 / 13.93 /
10.55.

---

## 2. The effect (prompt 02, 2026-11-13)

**Tree.** "New" is this branch at `e1fd6bc` (2026.11.0), the commit that added prompt 02, whose code is `5947b4f`'s.
"Old" is `ec6bca8` (2026.10.0), in a worktree in the session scratchpad, with `venv` and `var` linked from the main
checkout. The user's uncommitted files (`CLAUDE.md`, two `science-runs/` READMEs, `relativistic/`) were in the main
checkout throughout, so every new-side manifest and sidecar records `git_dirty` / `git_tracked_changes` true. Host
`mps026088`, 10 CPUs, shared and loaded (one-minute loads 4–52 during the work). Python 3.13.16, SciPy 1.17.0, ray
2.59.0, datastorekit 0.4.0, raypoolkit 0.3.0.

Outputs: [`electroweak-eos/threshold/`](electroweak-eos/threshold/) (§2.2),
[`electroweak-eos/bath/`](electroweak-eos/bath/) (§2.3), [`electroweak-eos/effect/`](electroweak-eos/effect/) (§2.4).
Stores, exports and launch logs are under `var/` (§2.4.8); none is committed.

**Result.**
- **Threshold gate.** On the new table the EW-capture threshold at M = 10⁻³ M_P, φ* = 5 M_P, tuned, is
  **(3.002, 3.003]**. On the old table, tuned, it is (3.019, 3.020], the same bracket as the untuned 3.020. The
  notebook gave 3.00285, and reachability from the new peak gives 2.99918. The uncaptured window is now
  2.976–3.002 (27 β). It was 2.999–3.019 (21 β).
- **Effect gate.** All 48 sampled histories were built on both trees, and none failed on either. The old side equals
  the run's export of every one, every column as written. Thirteen histories change their delivery from about
  1.7–1.9 GeV to 20–48 GeV: β = 2.985–3.0 at the laboratory mass, and β = 3.0 at every M of B and D. The changes
  are set beside each figure in §2.4.7.

### 2.1 The old tree and its checks

- `git worktree add --detach <scratch>/old-ec6bca8 ec6bca8`, then two symbolic links, `venv` and `var`, to the main
  checkout's. In the worktree, `git status --porcelain` shows `?? var` and `?? venv`, and nothing else: the links
  are untracked. The scan sidecar written from the worktree records `git_tracked_changes: false`. The registry's
  manifests, and the store sidecar created from the worktree, record `git_dirty: true`.
- `git diff --stat ec6bca8 HEAD` over `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `plot_inputs.py`, `config/`,
  `tools/surfing_scan.py`, `tools/history_and_bbn.py` and `RunDrivers/` lists one file: `config/version.py` (13 + / 1 −).
  With its comments and docstrings set aside (an AST comparison), the only difference is `VERSION_LABEL`
  `2026.10.0` → `2026.11.0`.
- Over all Python files, the computation differs in `ComputeTargets/ScalarHistory.py` (01a) and nowhere else.
  `CosmologyModels/GenericEOS/Xav_EOS_spline.py` and `CodeComparisonTools/XavEOS_test.py` differ in their
  docstrings only (AST comparison). The rest are tests, the build script and the campaign's own scripts. The CSV
  differs (01), and so do the golden records `golden_history_ee01*.json`, which tests read.
  `git diff ec6bca8 HEAD -- PRyM PRyMrates` is empty.
- In the worktree, `VERSION_LABEL` is `2026.10.0`, `FINGERPRINT_FORMAT` is 7, and `Xav_EOS_data.csv` has SHA-256
  `28edd85477f3b508ccf977f05faf79b3dfd76358b8be9ee6f358a45b9eda29a8`.
- The worktree was removed at 13:50 GMT, after the old side's exports had been written (§2.4.4). The links went first
  (`rm` of each link), then `git worktree remove` and `git worktree prune`. `git worktree list` now shows the main
  checkout only. Nothing ran from the worktree after that.

### 2.2 The threshold (P13; the Threshold gate)

**The runs.** `tools/surfing_scan.py` at φ* = 5 M_P, M = 10⁻³ M_P, the tool's default tuning (tolerance 10⁻⁵, cap 30),
window edge 10⁻³ GeV. Each scan ran as one process in the background, with its console output in
`var/electroweak-eos/02/threshold/`. None was cut off or resumed, and every sidecar has one entry with `resumed: false`
and `skipped: 0`.

| scan | tree | β | from | start – end (GMT) | load at start (1/5/15) | wall | `wall_s` summed | exit |
|---|---|---|---|---|---|---|---|---|
| S1-new | new, `e1fd6bc`, 2026.11.0 | 1.10–5.00 by 0.05 (79) | main checkout | 11:49:33 – 13:09:54 | 11.37 / 13.87 / 11.78 | 4820.6 s | 4811.4 s | 0 |
| F-old | old, `ec6bca8`, 2026.10.0 | 2.980–3.040 by 0.001 (61) | worktree root | 11:49:33 – 12:31:27 | 11.37 / 13.87 / 11.78 | 2514.1 s | 2507.8 s | 0 |
| F-new | new, `e1fd6bc`, 2026.11.0 | 2.950–3.050 by 0.001 (101) | main checkout | 12:12:16 – 13:25:04 | 6.61 / 7.26 / 8.69 | 4367.4 s | 4358.6 s | 0 |

- **The sidecars.** F-old's names `git_head` `ec6bca8be47f…` with `version_label` `2026.10.0`. S1-new's and F-new's name
  `e1fd6bcc182e…` with `2026.11.0` and `git_tracked_changes: true` (the user's files).
- **The rows.** Every row is `status=ok`: 79, 101 and 61 of them.
- **The three scans ran at once,** so each one's wall time was taken beside the other two.
- **F-new's range** was read from S1-new by `threshold/edges.py range`. Its edges are the largest β with
  `capture_kick` QCD and the smallest with EW, and it counts the uncaptured runs that have a QCD capture below and an
  EW capture above. To let F-new run beside the others, it was first read when S1-new's console printed β = 3.1 (41
  rows, 12:12 GMT), and read again on all 79 rows at the end. Both readings gave QCD to 2.95, EW from 3.05, and one
  window (β = 3.0), so F-new is 2.950–3.050, **101 β**, as the orchestrator's §0 expected.
- **The figures** were drawn with `tools/plot_surfing_temperature.py` into `threshold/out/surfing_temperature_{s1_new,
  f_new,f_old}.{pdf,png}`: S1-new in 55 s, the fine grids in about 5 s each. That tool computes the reachability
  roots from the table of its own tree (`find_kicks`), so F-old's figure was drawn from the worktree.
- **The edges, the window and the full rows** are in `threshold/out/edges.txt` (`edges.py report`).

**Every row of the fine grids.** Temperatures are in GeV to six significant digits, and N is in e-folds from T*.

<details><summary>F-new, 101 rows</summary>

| β | captured | kick | n | T_capture | T_max | T_deliver | N_deliver |
|---|---|---|---|---|---|---|---|
| 2.95 | true | QCD | 2 | 1.58562 | 1.97757 | 1.97757 | 24.0801 |
| 2.951 | true | QCD | 2 | 1.58662 | 1.97539 | 1.97539 | 24.0863 |
| 2.952 | true | QCD | 2 | 1.58762 | 1.97247 | 1.97247 | 24.0928 |
| 2.953 | true | QCD | 2 | 1.58863 | 1.96871 | 1.96871 | 24.0998 |
| 2.954 | true | QCD | 2 | 1.58963 | 1.96405 | 1.96405 | 24.1072 |
| 2.955 | true | QCD | 2 | 1.59063 | 1.95839 | 1.95839 | 24.1152 |
| 2.956 | true | QCD | 2 | 1.59164 | 1.95165 | 1.95165 | 24.1238 |
| 2.957 | true | QCD | 2 | 1.59264 | 1.94374 | 1.94374 | 24.133 |
| 2.958 | true | QCD | 2 | 1.59364 | 1.93459 | 1.93459 | 24.1428 |
| 2.959 | true | QCD | 2 | 1.59465 | 1.92411 | 1.92411 | 24.1535 |
| 2.96 | true | QCD | 2 | 1.59565 | 1.91222 | 1.91222 | 24.1649 |
| 2.961 | true | QCD | 2 | 1.59665 | 1.89886 | 1.89886 | 24.1771 |
| 2.962 | true | QCD | 2 | 1.59766 | 1.88398 | 1.88398 | 24.1903 |
| 2.963 | true | QCD | 2 | 1.59866 | 1.86756 | 1.86756 | 24.2043 |
| 2.964 | true | QCD | 2 | 1.59966 | 1.84957 | 1.84957 | 24.2193 |
| 2.965 | true | QCD | 2 | 1.60067 | 1.83006 | 1.83006 | 24.2353 |
| 2.966 | true | QCD | 2 | 1.60167 | 1.80909 | 1.80909 | 24.2522 |
| 2.967 | true | QCD | 2 | 1.60267 | 1.78679 | 1.78679 | 24.2701 |
| 2.968 | true | QCD | 2 | 1.60368 | 1.76334 | 1.76334 | 24.2888 |
| 2.969 | true | QCD | 2 | 1.60468 | 1.73904 | 1.73904 | 24.3082 |
| 2.97 | true | QCD | 2 | 1.60568 | 1.71429 | 1.71429 | 24.328 |
| 2.971 | true | QCD | 2 | 1.60669 | 1.68967 | 1.68967 | 24.348 |
| 2.972 | true | QCD | 2 | 1.60769 | 1.66595 | 1.66595 | 24.3677 |
| 2.973 | true | QCD | 2 | 1.60869 | 1.64428 | 1.64428 | 24.3863 |
| 2.974 | true | QCD | 2 | 1.6097 | 1.62626 | 1.62626 | 24.4027 |
| 2.975 | true | QCD | 2 | 1.6107 | 1.6143 | 1.61429 | 24.4154 |
| 2.976 | false | — | 0 | — | — | 1.6121 | 24.4218 |
| 2.977 | false | — | 0 | — | — | 1.62584 | 24.418 |
| 2.978 | false | — | 0 | — | — | 1.66662 | 24.3973 |
| 2.979 | false | — | 0 | — | — | 1.75709 | 24.3474 |
| 2.98 | false | — | 0 | — | — | 1.95255 | 24.2433 |
| 2.981 | false | — | 0 | — | — | 2.43593 | 24.0204 |
| 2.982 | false | — | 0 | — | — | 4.22676 | 23.4632 |
| 2.983 | false | — | 0 | — | — | 10.0066 | 22.6014 |
| 2.984 | false | — | 0 | — | — | 15.8969 | 22.1428 |
| 2.985 | false | — | 0 | — | — | 20.6489 | 21.8832 |
| 2.986 | false | — | 0 | — | — | 24.537 | 21.7115 |
| 2.987 | false | — | 0 | — | — | 27.801 | 21.5872 |
| 2.988 | false | — | 0 | — | — | 30.6003 | 21.492 |
| 2.989 | false | — | 0 | — | — | 33.0397 | 21.4165 |
| 2.99 | false | — | 0 | — | — | 35.192 | 21.3549 |
| 2.991 | false | — | 0 | — | — | 37.1108 | 21.3038 |
| 2.992 | false | — | 0 | — | — | 38.8373 | 21.2606 |
| 2.993 | false | — | 0 | — | — | 40.404 | 21.2236 |
| 2.994 | false | — | 0 | — | — | 41.8368 | 21.1916 |
| 2.995 | false | — | 0 | — | — | 43.1557 | 21.1637 |
| 2.996 | false | — | 0 | — | — | 44.376 | 21.1391 |
| 2.997 | false | — | 0 | — | — | 45.5104 | 21.1173 |
| 2.998 | false | — | 0 | — | — | 46.5688 | 21.0979 |
| 2.999 | false | — | 0 | — | — | 47.5597 | 21.0805 |
| 3.0 | false | — | 0 | — | — | 48.4902 | 21.065 |
| 3.001 | false | — | 0 | — | — | 49.3662 | 21.051 |
| 3.002 | false | — | 0 | — | — | 50.193 | 21.0384 |
| 3.003 | true | EW | 2 | 50.91 | 50.9749 | 50.9749 | 21.027 |
| 3.004 | true | EW | 2 | 51.1322 | 51.7159 | 51.7159 | 21.0167 |
| 3.005 | true | EW | 2 | 51.3146 | 52.4193 | 52.4193 | 21.0074 |
| 3.006 | true | EW | 2 | 51.4801 | 53.0883 | 53.0883 | 20.999 |
| 3.007 | true | EW | 2 | 51.6347 | 53.7255 | 53.7255 | 20.9914 |
| 3.008 | true | EW | 2 | 51.7814 | 54.3333 | 54.3333 | 20.9845 |
| 3.009 | true | EW | 2 | 51.9218 | 54.9139 | 54.9139 | 20.9782 |
| 3.01 | true | EW | 2 | 52.0573 | 55.4693 | 55.4693 | 20.9726 |
| 3.011 | true | EW | 2 | 52.1884 | 56.0012 | 56.0011 | 20.9675 |
| 3.012 | true | EW | 2 | 52.3159 | 56.5112 | 56.5111 | 20.963 |
| 3.013 | true | EW | 2 | 52.4402 | 57.0008 | 57.0007 | 20.9589 |
| 3.014 | true | EW | 2 | 52.5617 | 57.4713 | 57.4713 | 20.9552 |
| 3.015 | true | EW | 2 | 52.6805 | 57.924 | 57.9239 | 20.9519 |
| 3.016 | true | EW | 2 | 52.7971 | 58.3599 | 58.3599 | 20.949 |
| 3.017 | true | EW | 2 | 52.9116 | 58.7802 | 58.7801 | 20.9465 |
| 3.018 | true | EW | 2 | 53.0242 | 59.1857 | 59.1856 | 20.9442 |
| 3.019 | true | EW | 2 | 53.1349 | 59.5773 | 59.5773 | 20.9423 |
| 3.02 | true | EW | 2 | 53.2441 | 59.9559 | 59.9559 | 20.9406 |
| 3.021 | true | EW | 2 | 53.3516 | 60.3221 | 60.3221 | 20.9392 |
| 3.022 | true | EW | 2 | 53.4578 | 60.6768 | 60.6768 | 20.938 |
| 3.023 | true | EW | 2 | 53.5626 | 61.0205 | 61.0205 | 20.9371 |
| 3.024 | true | EW | 2 | 53.6661 | 61.3539 | 61.3538 | 20.9364 |
| 3.025 | true | EW | 2 | 53.7684 | 61.6774 | 61.6774 | 20.9358 |
| 3.026 | true | EW | 2 | 53.8695 | 61.9917 | 61.9917 | 20.9355 |
| 3.027 | true | EW | 2 | 53.9696 | 62.2972 | 62.2971 | 20.9353 |
| 3.028 | true | EW | 2 | 54.0687 | 62.5943 | 62.5943 | 20.9353 |
| 3.029 | true | EW | 2 | 54.1668 | 62.8836 | 62.8836 | 20.9355 |
| 3.03 | true | EW | 2 | 54.2639 | 63.1653 | 63.1653 | 20.9358 |
| 3.031 | true | EW | 2 | 54.3602 | 63.4399 | 63.4399 | 20.9363 |
| 3.032 | true | EW | 2 | 54.4556 | 63.7078 | 63.7078 | 20.9368 |
| 3.033 | true | EW | 2 | 54.5502 | 63.9692 | 63.9692 | 20.9375 |
| 3.034 | true | EW | 2 | 54.644 | 64.2244 | 64.2244 | 20.9384 |
| 3.035 | true | EW | 2 | 54.737 | 64.4739 | 64.4738 | 20.9393 |
| 3.036 | true | EW | 2 | 54.8294 | 64.7177 | 64.7177 | 20.9403 |
| 3.037 | true | EW | 2 | 54.921 | 64.9563 | 64.9562 | 20.9415 |
| 3.038 | true | EW | 2 | 55.0119 | 65.1897 | 65.1897 | 20.9427 |
| 3.039 | true | EW | 2 | 55.1022 | 65.4184 | 65.4184 | 20.9441 |
| 3.04 | true | EW | 2 | 55.1919 | 65.6424 | 65.6424 | 20.9455 |
| 3.041 | true | EW | 2 | 55.2809 | 65.8621 | 65.8621 | 20.947 |
| 3.042 | true | EW | 2 | 55.3694 | 66.0775 | 66.0775 | 20.9486 |
| 3.043 | true | EW | 2 | 55.4572 | 66.2889 | 66.2889 | 20.9502 |
| 3.044 | true | EW | 2 | 55.5446 | 66.4964 | 66.4964 | 20.9519 |
| 3.045 | true | EW | 2 | 55.6314 | 66.7003 | 66.7003 | 20.9537 |
| 3.046 | true | EW | 2 | 55.7176 | 66.9006 | 66.9006 | 20.9556 |
| 3.047 | true | EW | 2 | 55.8034 | 67.0975 | 67.0975 | 20.9575 |
| 3.048 | true | EW | 2 | 55.8886 | 67.2912 | 67.2912 | 20.9595 |
| 3.049 | true | EW | 2 | 55.9734 | 67.4818 | 67.4818 | 20.9616 |
| 3.05 | true | EW | 2 | 56.0577 | 67.6694 | 67.6694 | 20.9636 |

</details>

<details><summary>F-old, 61 rows</summary>

| β | captured | kick | n | T_capture | T_max | T_deliver | N_deliver |
|---|---|---|---|---|---|---|---|
| 2.98 | true | QCD | 2 | 1.61565 | 1.9908 | 1.9908 | 24.2233 |
| 2.981 | true | QCD | 2 | 1.61665 | 1.98272 | 1.98272 | 24.2325 |
| 2.982 | true | QCD | 2 | 1.61765 | 1.9731 | 1.9731 | 24.2425 |
| 2.983 | true | QCD | 2 | 1.61866 | 1.96182 | 1.96182 | 24.2534 |
| 2.984 | true | QCD | 2 | 1.61966 | 1.94877 | 1.94877 | 24.2653 |
| 2.985 | true | QCD | 2 | 1.62066 | 1.93384 | 1.93384 | 24.2782 |
| 2.986 | true | QCD | 2 | 1.62167 | 1.91694 | 1.91694 | 24.2923 |
| 2.987 | true | QCD | 2 | 1.62267 | 1.89801 | 1.89801 | 24.3076 |
| 2.988 | true | QCD | 2 | 1.62367 | 1.87702 | 1.87702 | 24.3241 |
| 2.989 | true | QCD | 2 | 1.62468 | 1.85402 | 1.85401 | 24.3418 |
| 2.99 | true | QCD | 2 | 1.62568 | 1.82909 | 1.82909 | 24.3608 |
| 2.991 | true | QCD | 2 | 1.62668 | 1.80246 | 1.80246 | 24.381 |
| 2.992 | true | QCD | 2 | 1.62769 | 1.77445 | 1.77444 | 24.4023 |
| 2.993 | true | QCD | 2 | 1.62869 | 1.74557 | 1.74557 | 24.4243 |
| 2.994 | true | QCD | 2 | 1.62969 | 1.71662 | 1.71662 | 24.4466 |
| 2.995 | true | QCD | 2 | 1.6307 | 1.68873 | 1.68873 | 24.4686 |
| 2.996 | true | QCD | 2 | 1.6317 | 1.66365 | 1.66365 | 24.4891 |
| 2.997 | true | QCD | 2 | 1.6327 | 1.64402 | 1.64402 | 24.5065 |
| 2.998 | true | QCD | 2 | 1.63371 | 1.63415 | 1.63415 | 24.5177 |
| 2.999 | false | — | 0 | — | — | 1.64143 | 24.5181 |
| 3.0 | false | — | 0 | — | — | 1.68001 | 24.499 |
| 3.001 | false | — | 0 | — | — | 1.78121 | 24.4433 |
| 3.002 | false | — | 0 | — | — | 2.03312 | 24.3115 |
| 3.003 | false | — | 0 | — | — | 2.82151 | 23.9798 |
| 3.004 | false | — | 0 | — | — | 6.95467 | 23.071 |
| 3.005 | false | — | 0 | — | — | 14.2578 | 22.357 |
| 3.006 | false | — | 0 | — | — | 20.1118 | 22.0151 |
| 3.007 | false | — | 0 | — | — | 24.784 | 21.8062 |
| 3.008 | false | — | 0 | — | — | 28.6443 | 21.6611 |
| 3.009 | false | — | 0 | — | — | 31.8847 | 21.5539 |
| 3.01 | false | — | 0 | — | — | 34.6634 | 21.4709 |
| 3.011 | false | — | 0 | — | — | 37.0771 | 21.4047 |
| 3.012 | false | — | 0 | — | — | 39.1857 | 21.3511 |
| 3.013 | false | — | 0 | — | — | 41.0389 | 21.3071 |
| 3.014 | false | — | 0 | — | — | 42.6785 | 21.2705 |
| 3.015 | false | — | 0 | — | — | 44.1401 | 21.2397 |
| 3.016 | false | — | 0 | — | — | 45.4559 | 21.2135 |
| 3.017 | false | — | 0 | — | — | 46.653 | 21.191 |
| 3.018 | false | — | 0 | — | — | 47.7525 | 21.1712 |
| 3.019 | false | — | 0 | — | — | 48.7703 | 21.1539 |
| 3.02 | true | EW | 2 | 49.3572 | 49.7191 | 49.7191 | 21.1384 |
| 3.021 | true | EW | 2 | 49.5512 | 50.6088 | 50.6088 | 21.1246 |
| 3.022 | true | EW | 2 | 49.7189 | 51.4461 | 51.4461 | 21.1122 |
| 3.023 | true | EW | 2 | 49.8724 | 52.2338 | 52.2338 | 21.1012 |
| 3.024 | true | EW | 2 | 50.0164 | 52.9735 | 52.9735 | 21.0913 |
| 3.025 | true | EW | 2 | 50.1532 | 53.6671 | 53.6671 | 21.0825 |
| 3.026 | true | EW | 2 | 50.2842 | 54.3165 | 54.3165 | 21.0748 |
| 3.027 | true | EW | 2 | 50.4106 | 54.9237 | 54.9237 | 21.0681 |
| 3.028 | true | EW | 2 | 50.533 | 55.491 | 55.491 | 21.0622 |
| 3.029 | true | EW | 2 | 50.6519 | 56.0205 | 56.0205 | 21.0572 |
| 3.03 | true | EW | 2 | 50.7678 | 56.5146 | 56.5146 | 21.0529 |
| 3.031 | true | EW | 2 | 50.8809 | 56.9755 | 56.9755 | 21.0493 |
| 3.032 | true | EW | 2 | 50.9917 | 57.4057 | 57.4057 | 21.0464 |
| 3.033 | true | EW | 2 | 51.1002 | 57.8075 | 57.8075 | 21.044 |
| 3.034 | true | EW | 2 | 51.2067 | 58.1835 | 58.1835 | 21.0422 |
| 3.035 | true | EW | 2 | 51.3113 | 58.536 | 58.536 | 21.0408 |
| 3.036 | true | EW | 2 | 51.4143 | 58.8672 | 58.8672 | 21.0399 |
| 3.037 | true | EW | 2 | 51.5156 | 59.1793 | 59.1793 | 21.0393 |
| 3.038 | true | EW | 2 | 51.6154 | 59.4741 | 59.4741 | 21.0391 |
| 3.039 | true | EW | 2 | 51.7139 | 59.7533 | 59.7533 | 21.0392 |
| 3.04 | true | EW | 2 | 51.811 | 60.0184 | 60.0184 | 21.0395 |

</details>

**The edges.**

| grid | largest β QCD captures | uncaptured window | smallest β EW captures | windows | capture between uncaptured β |
|---|---|---|---|---|---|
| F-new | **2.975** | 2.976–3.002 (27 β, 0.027 wide) | **3.003** | 1 | none |
| F-old | 2.998 | 2.999–3.019 (21 β, 0.021 wide) | 3.020 | 1 | none |

On both grids every row below the window is a QCD capture and every row above it is an EW capture. Neither grid
shows a second window, or any capture inside the window.

**The EW threshold.**

| source | EW threshold |
|---|---|
| **F-new: ChamPBH, new table, tuned (this prompt)** | **(3.002, 3.003]** |
| F-old: ChamPBH, old table, tuned (this prompt) | (3.019, 3.020] |
| S5: ChamPBH, old table, untuned, 2026.6.0 (surfing-temperature §1.8) | 3.020, in (3.019, 3.020] |
| the notebook's integrator on the notebook's table (README §0.1) | 3.00285 |
| reachability from the new table's peak Σ (log 01 §3.4) | 2.99918 |
| reachability from the old table's peak Σ (surfing-temperature §1.3) | 3.011779 |

- **On the new table** the threshold lies 0.0028–0.0038 above the new reachability prediction. The notebook's
  3.00285 lies inside the bracket.
- **Today's threshold under today's tuned tool** (F-old) is the untuned one.
  - Every capture kick of F-old equals S5's.
  - |F-old/S5 − 1| is at most 1.6 × 10⁻⁸ in T_capture, 8.7 × 10⁻⁸ in T_max, 1.6 × 10⁻⁶ in T_deliver (largest at
    β = 3.004) and 1.0 × 10⁻⁷ in N_deliver.
- **The orchestrator's probe** (note 02 §0) put the threshold in (3.002, 3.004]. It did not run 3.003, which
  captures.

**The first bounce across the new window** rises from 1.6121 GeV at β = 2.976 (N 24.4218) to 50.193 GeV at 3.002
(N 21.0384).
- At the lower edge it is 0.99864 times the last QCD delivery (1.61429 GeV at 2.975), and at the upper edge 0.98466
  times the first EW delivery (50.9749 GeV at 3.003).
- The rise is steepest at 2.981–2.984. The ratios of adjacent deliveries there are 1.248, 1.735, 2.367 and 1.589;
  elsewhere in the window they are 1.0–1.30.
- On the old table, as surfing-temperature §1.8 recorded and F-old repeats, the bounce rose from 1.64143 GeV (2.999)
  to 48.7703 GeV (3.019), steepest at 3.003–3.006.
- At β = 3.000 the new first bounce is 48.4902 GeV (N 21.065), log 01 §3.4's value.

**S1-new against S1** (surfing-temperature §1.4: untuned, old table, 2026.6.0).
- **Who captures:** the same kick at every one of the 79 β. Nothing captures at 1.10 or 3.00, QCD captures at
  1.15–2.95 (37 β) and EW at 3.05–5.00 (40 β). μ/π and e⁺e⁻ capture nothing.
- **QCD** (37 β): S1-new/S1 − 1 is +4.4 × 10⁻⁶ to +8.1 × 10⁻⁵ in T_capture, −4.7 × 10⁻³ to +1.3 × 10⁻⁴ in T_max and
  −9.1 × 10⁻⁵ to +9.6 × 10⁻³ in T_deliver. The largest changes are at 2.95. T_capture/T_hot is 0.8246 (2.95) –
  0.9060 (1.30), against S1's 0.8246 – 0.9060.
- **EW** (40 β): T_capture changes by −4.0 % (5.00) to +6.3 % (3.05), T_max by +6.6 % (5.00) to +13.1 % (3.70), and
  T_deliver by +2.7 % (5.00) to +12.2 % (3.55).
  - T_deliver runs from 67.67 GeV at 3.05 to 180.75 GeV at 5.00, against S1's 62.14 and 175.99.
  - T_capture/T_hot runs from 0.7261 (4.30) to 0.8238 (3.05), against S1's 0.7777 (5.00) to 0.8435 (3.05).
  - T_max/T_hot runs from 0.9945 (3.05) to 1.1081 (5.00), against 0.9941 to 1.0674.
  - T_deliver/T_hot runs from 0.9945 (3.05) to 1.0008 (3.10), against 0.9941 to 1.0007.
- S1-new's tuning took 355 integrations over 79 β, F-new's 405 over 101 and F-old's 248 over 61.

### 2.3 The bath drift (P20)

`bath/bath_drift.py`. **Tables only:**
- **The integral.** Δ(T) = ∫ from ln T to ln T* of [4 + d ln g_ρ/d ln T − 3(1 + w)(1 + x)] d ln T, with
  x = (1/3) d ln g_s/d ln T.
- **The functions** are `QCD_Cosmology`'s own, in GeV units. The old table is read through a redirect of the one
  `pandas.read_csv` call that `Xav_EOS_spline` makes.
- **The grid** is `thermo_compare.py`'s, rebuilt from its definition in `nb_thermo.wls`: 10^i for
  i = −5, −5 + 1/2000, … up to log10(2 × 10⁴) − 10⁻⁶. That is 18 603 points, from 10⁻⁵ to 19 998.6 GeV, with T* at
  the top point.
- **"Today."** The integral is continued below the grid to T_CMB at the same density. It adds nothing: the integrand
  is zero there.
- **Units.** The issue's per cent are 100 Δ (its −1.17 % is Δ = −1.168 × 10⁻²). The relative offset of ρ_R itself is
  e^Δ − 1.

| T (GeV) | 100 | 1 | 0.1 | 10⁻² | 10⁻³ | 10⁻⁴ | 10⁻⁵ (end of table) | today |
|---|---|---|---|---|---|---|---|---|
| issue (old table, 2026-10-31) | +0.19 | −1.17 | −0.08 | −0.21 | −0.211 | −0.098 | — | −0.078 |
| old table, 100 Δ | +0.1918 | −1.1677 | −0.0831 | −0.2121 | −0.2106 | −0.0977 | −0.0778 | −0.0778 |
| **new table, 100 Δ** | **−0.0569** | **−1.2846** | **−0.1999** | **−0.3290** | **−0.3276** | **−0.2146** | **−0.1947** | **−0.1947** |
| new − old, 100 ΔΔ | −0.2487 | −0.1169 | −0.1169 | −0.1169 | −0.1169 | −0.1169 | −0.1169 | −0.1169 |

- **On the old table** the script reproduces every figure in the issue's table, to the digits the issue gives. That is
  the script's check (prompt §3).
- **Below about 22 GeV** the two tables are the same rows, and new − old is a constant −1.169 × 10⁻³. That offset is
  the one the new table's electroweak region adds.
- **The extremes on the grid**, which the issue's table does not show:
  - old table: least Δ = −1.690 × 10⁻² (100 Δ = −1.69; ρ offset −1.68 %) at 24.7 GeV, and largest +2.007 × 10⁻²
    (100 Δ = +2.01; ρ offset +2.03 %) at 0.163 GeV;
  - new table: −1.809 × 10⁻² at 24.9 GeV, and +1.890 × 10⁻² at 0.163 GeV.

**Per history.** d = ln ρ_R,J − ln((π²/30) g_ρ T_J⁴), from `fields.csv` (`log_rhorad_Jordan_GeV4`, `gstar_rho`,
`T_Jordan_GeV`).
- **The history** is β = 2, M = 0.5 M_P, α = 0, from both stores' exports and from the run's own export of it
  (R-natural, `var/science-2026.10.0/out/reflections/`).
- **Interpolation.** d is linear in ln T_J between the first pair of samples, in file order, that brackets the
  target. Both trees give the same pair indices.

| export | first sample (T_J ≈ 19 832 GeV) | T_J = 1 GeV | T_J = 1 MeV | last sample (T_J = 2.3487 × 10⁻¹³ GeV) |
|---|---|---|---|---|
| the run (R-natural) | −1.50 × 10⁻⁶ | −1.1677 × 10⁻² | −2.1066 × 10⁻³ | −7.778 × 10⁻⁴ |
| old side | −1.50 × 10⁻⁶ | −1.1677 × 10⁻² | −2.1066 × 10⁻³ | −7.778 × 10⁻⁴ |
| new side | −1.51 × 10⁻⁶ | −1.2846 × 10⁻² | −3.2759 × 10⁻³ | −1.9471 × 10⁻³ |

- The old side's `fields.csv` is byte for byte the run's (`cmp`).
- The history follows the tables-only integral to the digits shown, on both tables.

The numbers are on the issue's entry as a dated addendum (`inelastic-reflection` board §3). The issue stays open (U4).

### 2.4 The sample (P14–P19; the Effect gate)

#### 2.4.1 The clusters and the probe

**The probe** is `effect/code_probe.py`, one-loop-drain 06's extended. From the driver, one task and one actor it
prints:
- the module files, the labels and the first entries of `sys.path`;
- the cwd;
- the path `CosmologyModels/GenericEOS/Xav_EOS_data.csv` resolves to in that process, and that file's SHA-256;
- Σ(300 GeV) from a `Xav_EOS_spline` built in that process.

It exits 1 if a worker differs from the driver, or if a file lies outside the driver's tree. Its outputs are in
`effect/out/probe/`.

**One cluster for both sides was not possible.** The prompt's plan was one cluster for both sides. The cluster was
started from the main checkout (13:25:33 GMT, load 7.50; `start` exit 0, lock HELD), and the probe was run from the
worktree before any build. It exited 1 (`probe-1-worktree.txt`):
- **What the workers read.** The task and the actor imported the worktree's `ScalarHistory`, `ObjectFactories` and
  `Xav_EOS_spline`, at 2026.10.0. But their cwd was the main checkout, so `Xav_EOS_spline` read the main checkout's
  CSV: `690090c8…0119d`, Σ(300 GeV) = 1.8887 × 10⁻³. The driver read the worktree's: `28edd854…29a8`, Σ = 5.6853 × 10⁻³.
- **The cause.** Ray workers take the raylet's working directory, which is the directory `tools/ray_cluster.sh start`
  ran from (the script does no `cd`). `Xav_EOS_spline` reads its CSV by a path relative to the cwd. The worktree's
  code reached the workers through `sys.path` (the job's `py_driver_sys_path`), but its data files did not.
- **Why it was not seen before.** `one-loop-drain` 06's probe, which this one extends, showed module files and labels
  only. Its two trees had the same data files.
- **PRyMordial** reads `PRyMrates/` from the cwd as well. That read is the same file on both trees here.

The agent stopped (orchestrator note 02, Conventions). **Ruled by the user, 2026-11-13: "I think this is fine.
Please proceed."** The ruling was option (a), one cluster per side, each started from that side's tree root. The
cause is opened as `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]` (board §3).

| # | when (GMT) | cluster started from | probe run from | exit | every process |
|---|---|---|---|---|---|
| 1 | 13:26 | main checkout | worktree | **1** | driver: worktree, 2026.10.0, `28edd854…`, Σ 5.685 × 10⁻³; workers: worktree code but the main checkout's CSV `690090c8…`, Σ 1.889 × 10⁻³ (the stop) |
| 2 | 13:26 | main checkout | main checkout | 0 | main checkout, 2026.11.0, `690090c8…`, Σ 1.889 × 10⁻³ |
| 3 | 13:36 | worktree (old side) | worktree | 0 | worktree, 2026.10.0, `28edd854…`, Σ 5.685 × 10⁻³; workers' cwd the worktree |
| 4 | 13:48 | worktree (old side), after the old builds | main checkout | **1** | driver: main checkout, 2026.11.0, `690090c8…`; workers: main-checkout code but the worktree's CSV `28edd854…`, Σ 5.685 × 10⁻³ (the same failure, the other way round) |
| 5 | 13:49 | main checkout (new side) | main checkout | 0 | main checkout, 2026.11.0, `690090c8…`, Σ 1.889 × 10⁻³ |
| 6 | 14:01 | main checkout (new side), after the new builds and exports | main checkout | 0 | as 5 |
| 7 | 14:01 | main checkout (new side) | main checkout, `--worker-csv` the old copy | **1** | the breakage record (§2.4.9) |

- **The prompt's third probe.** The prompt runs a probe "from the main checkout again after the old side's builds
  end". Under one cluster per side, that is probe 5: the new side's own cluster, before its first build. Probe 4,
  on the old cluster, shows why that cluster could not be reused.
- **The clusters' statuses** after each `start` and `stop`, and the lock's, are in `effect/out/cluster/`.
  - Every `start` and `stop` exited 0. Each `stop` reported the grace period ending and a SIGKILL to the two head
    processes, then "no process names /tmp/ray-cpbh; ports … are free" and "released the rehearsal lock".
  - Each `status` after a stop showed no process, the four ports free, and the lock free. No old location appeared.

| cluster | started from | start | stop | load at start |
|---|---|---|---|---|
| 1 | main checkout | 13:25:33 | 13:36:06 | 7.50 / 7.39 / 10.34 |
| 2, the old side's | worktree, its own `tools/ray_cluster.sh` (no diff to HEAD's) | 13:36:34 | 13:49:11 | 9.24 / 8.47 / 9.37 |
| 3, the new side's | main checkout | 13:49:37 | 14:02:01 | 21.75 / 19.86 / 17.35 |

#### 2.4.2 The builds

Each side was built by `effect/run_side.sh <side>`. It runs README §9.2 P14's six runs in sequence through the tree's
own `RunDrivers/registered_run.py` (`--campaign electroweak-eos --prompt 02 --register <side>-<run>`) and stops at the
first failure. Every run had `--config exponential.yaml --database var/datastores/electroweak-eos-02-<side>.db
--shards 16 --job-name electroweak-eos-02-<side>-<run> --ray-address 127.0.0.1:6380 --phi-init-Mp 5
--bbn-wall-clock-limit 600` and P14's grid.
- **Launch.** Each side was launched detached with `nohup sh …/run_side.sh <side>`, from its tree root, with its log
  and pid file under `var/electroweak-eos/02/runs/`. It was checked once (`<side>.check.txt`) and waited for with
  `wait_for_line.py`. Both sides ended in about 10 minutes, far inside the three-hour bound.
- **The β** are the run's own. `run_side.sh` makes the 13 as `repr(i / 1000)` for i = 2980, 2985, …, 3040, as
  `run_science_2026.10.0.sh`'s `G` makes `2980:3040:5:1000`. The strings equal the β column of the run's export
  (`fig6/inputs/histories.csv`) at M = 4 × 10⁻³¹, and their `float.hex` values are equal. The values 1.2, 2.0,
  3.0 and 5.0 are each in the export as written.

| run id | state | exit | launch – end (GMT) | wall | load at launch (1/5/15) | `git_head` | `git_dirty` |
|---|---|---|---|---|---|---|---|
| `electroweak-eos-02-old-a-edge-20261113T133704` | done | 0 | 13:37:03 – 13:38:48 | 1 min 45 s | 7.82 / 8.19 / 9.23 | `ec6bca8` | true (links) |
| `electroweak-eos-02-old-a-spread-20261113T133850` | done | 0 | 13:38:48 – 13:41:16 | 2 min 28 s | 27.22 / 17.09 / 12.71 | `ec6bca8` | true |
| `electroweak-eos-02-old-b-20261113T134118` | done | 0 | 13:41:16 – 13:42:45 | 1 min 29 s | 15.43 / 14.66 / 12.33 | `ec6bca8` | true |
| `electroweak-eos-02-old-c-20261113T134247` | done | 0 | 13:42:45 – 13:44:08 | 1 min 23 s | 52.25 / 28.14 / 17.77 | `ec6bca8` | true |
| `electroweak-eos-02-old-d-20261113T134410` | done | 0 | 13:44:08 – 13:45:48 | 1 min 40 s | 24.06 / 25.09 / 17.60 | `ec6bca8` | true |
| `electroweak-eos-02-old-r-20261113T134549` | done | 0 | 13:45:48 – 13:47:01 | 1 min 13 s | 23.42 / 24.38 / 18.15 | `ec6bca8` | true |
| `electroweak-eos-02-new-a-edge-20261113T135000` | done | 0 | 13:49:59 – 13:52:24 | 2 min 25 s | 26.40 / 20.91 / 17.78 | `e1fd6bc` | true (the user's files) |
| `electroweak-eos-02-new-a-spread-20261113T135226` | done | 0 | 13:52:24 – 13:54:55 | 2 min 31 s | 47.07 / 34.79 / 24.07 | `e1fd6bc` | true |
| `electroweak-eos-02-new-b-20261113T135456` | done | 0 | 13:54:55 – 13:56:25 | 1 min 30 s | 11.13 / 24.69 / 21.71 | `e1fd6bc` | true |
| `electroweak-eos-02-new-c-20261113T135626` | done | 0 | 13:56:25 – 13:57:44 | 1 min 19 s | 39.57 / 30.82 / 24.36 | `e1fd6bc` | true |
| `electroweak-eos-02-new-d-20261113T135746` | done | 0 | 13:57:44 – 13:59:25 | 1 min 41 s | 17.35 / 26.53 / 23.35 | `e1fd6bc` | true |
| `electroweak-eos-02-new-r-20261113T135926` | done | 0 | 13:59:25 – 14:00:35 | 1 min 10 s | 7.75 / 20.68 / 21.43 | `e1fd6bc` | true |

- **The sides.** The old side took 9 min 58 s (13:37:03 – 13:47:01) and the new side 10 min 36 s (13:49:59 –
  14:00:35). Each holds 48 histories. No history failed on either.
- **Run times.** Each run's launch and end are those `run_side.sh` wrote into its log (`effect/out/runs/`). The
  registered pids were 98395, 98648, 98864, 99060, 99286 and 99500 (old), and 975, 1294, 1457, 1741, 1859 and 2000
  (new).
- **Not the planner's estimate.** P14 estimated about 120 core-minutes a side, most of it in `c` and `r`. The run's
  costs there came from β ≈ 1.1–1.2 at M_crit, which the sample does not hold.
- **The store sidecars.** Each was made by `runregistrykit store create`, from its side's root, after its builds.
  `electroweak-eos-02-old.manifest.json` records `git_head` `ec6bca8`, `git_dirty` true and store_id `821adc85…`.
  `electroweak-eos-02-new.manifest.json` records `e1fd6bc`, true.

#### 2.4.3 The exports

| side | from | script | started (GMT) | wall | exit |
|---|---|---|---|---|---|
| old | worktree root, old cluster | `plot_by_beta.py` | 13:47:42 | 28.0 s | 0 |
| old | worktree root, old cluster | `plot_ScalarHistory.py` | 13:48:10 | 28.2 s | 0 |
| new | main checkout, new cluster | `plot_by_beta.py` | 14:00:48 | 26.6 s | 0 |
| new | main checkout, new cluster | `plot_ScalarHistory.py` | 14:01:15 | 28.1 s | 0 |

- **The options**, the same on both sides: `--config exponential.yaml --database <store> --shards 16 --ray-address
  127.0.0.1:6380 --phi-init-Mp 5 --alpha-values 0 0.1 --A-values 0.05 1 10 --no-plots --output
  var/electroweak-eos/02/export/<side>`.
- **Why those options.** The stores hold fixed-law rows at α = 0.1 (`a-spread`, `c`) and α = 0 (`d`), and one-loop
  rows at A = 0.05, 1 and 10. `--alpha-values` and `--A-values` name exactly those. The plot scripts refuse any α or
  A with no stored row (exit 2), and the defaults of the tolerances, T*, `--tuning-tolerance` and `--n-values` are the
  builds'. Neither script refused.
- **What each wrote.** Each side's `histories.csv` holds 185 rows. 48 are histories, and 137 are the export's marker
  rows ("no ScalarHistory row in the store") for the (row, M, β) of the stored grid that the sample did not build.
  Each side has 48 `fields.csv` and 37 `bounces.csv`, one for each one-loop history.
- **Warnings.** Each script printed "number of shards read from database (=16) does not match specified number of
  shards (=10)", and one "no ExponentialModel stored … skipped" line for each (M, β) the sample did not build.

#### 2.4.4 The old side against the run (P17)

`effect/compare_with_run.py` matches every history of the old side's `histories.csv` to its twin in the run's export.
- **Where the twin is.** For every run but `r`, in the main store's table (`fig6/inputs/histories.csv`, 7956 rows).
  For `r`, in the reflection store's (`var/science-2026.10.0/out/reflections/QCD_Cosmology/histories.csv`, 112 rows;
  the orchestrator's corrected path).
- **The match.** β, n, φ* and α as floats, `drain_model` as written, and M to a relative 10⁻¹². Every other column is
  compared as written.

**Result: 48 of 48 matched, 0 with a difference, none failed on either.** The output is
`effect/out/compare_with_run.txt`.
- **As a supplement,** every per-history file of the old export was compared byte for byte with the run's export of
  the same history, wherever the run had one. 80 files are equal and none differs. The other 5 `fields.csv` have no
  twin there: `d`'s three β = 3 histories and its M = 0.1, β = 2 one, and `a-spread`'s α = 0.1, β = 5.0.
- **So the old side is the run's code and inputs**, and old against new is the change a rebuild would make.

#### 2.4.5 Old against new, by history (P18)

`effect/compare_sample.py` reads the two exports. It writes `effect/out/effect.csv`, one row per history with old,
new, the difference and the relative difference of each observable, and `effect/out/effect.txt`.

**Failures:** none on either side. Ruling 6's stop (a new-side failure of an unlisted kind) did not arise.

**T_deliver on opposite sides of 10 GeV** (a label between the QCD deliveries, about 1–2 GeV, and the EW ones, about
30–60 GeV; not a test): 13 histories.

| run | β | M / M_P | row | T_deliver old → new (GeV) |
|---|---|---|---|---|
| a-edge | 2.985 | 4 × 10⁻³¹ | A = 1 | 1.93385 → 20.6487 |
| a-edge | 2.99 | 4 × 10⁻³¹ | A = 1 | 1.8291 → 35.1919 |
| a-edge | 2.995 | 4 × 10⁻³¹ | A = 1 | 1.68874 → 43.1556 |
| a-edge | 3.0 | 4 × 10⁻³¹ | A = 1 | 1.68001 → 48.4902 |
| b | 3.0 | 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰, 10⁻⁴² | A = 1 | 1.68001 → 48.4902 (each) |
| d | 3.0 | 10⁻³ / 0.1 / 0.5 | α = 0 | 1.68001 / 1.68058 / 1.68294 → 48.4902 / 48.4897 / 48.4875 |

- **The rest of the edge.** At β = 2.98, T_deliver moves only from 1.99081 to 1.95252 GeV. On F-new that β is
  uncaptured, and was a QCD capture before. At 3.005 it moves from 14.2574 GeV (inside the old window) to 52.4193.
  At 3.01–3.04 the EW deliveries rise by 6–21 GeV (34.66 → 55.47 at 3.01, 60.02 → 65.64 at 3.04).
- **The deliveries do not depend on M.** At the laboratory mass the new T_deliver equals F-new's, at M = 10⁻³ M_P,
  to 2 × 10⁻⁵ (2.985: 20.6487 against 20.6489).

**By run.** For each observable, the largest and median |new − old| (relative, where noted) over the histories with
a value on both sides. All of them are in `effect.txt`.

| observable | a-edge (13) | a-spread (12) | b (12) | c (4) | d (6) | r (1) |
|---|---|---|---|---|---|---|
| Y_p (rel) | 1.10 × 10⁻³ / 4.0 × 10⁻⁴ | 2.3 × 10⁻⁴ / 2.3 × 10⁻⁵ | 6.6 × 10⁻⁴ / 2.2 × 10⁻⁴ | 3.2 × 10⁻⁵ / 1.4 × 10⁻⁵ | 3.7 × 10⁻⁴ / 5.6 × 10⁻⁵ | 7.0 × 10⁻⁵ |
| D/H (rel) | 2.69 × 10⁻³ / 9.5 × 10⁻⁴ | 1.58 × 10⁻³ / 4.1 × 10⁻⁵ | 1.63 × 10⁻³ / 5.9 × 10⁻⁴ | 1.1 × 10⁻⁴ / 6.9 × 10⁻⁵ | 2.80 × 10⁻³ / 1.6 × 10⁻⁴ | 7.6 × 10⁻⁵ |
| ΔY_p (pp of the baseline) | 0.111 / 0.040 | 0.024 / 0.0023 | 0.066 / 0.022 | 0.0032 / 0.0014 | 0.018 / 0.0045 | 0.0071 |
| Δ(D/H) (pp) | 0.271 / 0.095 | 0.161 / 0.0041 | 0.164 / 0.059 | 0.011 / 0.0070 | 0.244 / 0.0041 | 0.0078 |
| ΔN_eff today (abs) | 0.0202 / 0.0076 | 1.4 × 10⁻⁴ / 1.8 × 10⁻⁵ | 0.0125 / 0.0039 | 2.1 × 10⁻⁷ / 1.6 × 10⁻⁷ | 0 / 0 | 1.3 × 10⁻⁶ |
| ΔN_eff today (rel) | 0.542 / 0.413 | 0.039 / 3.2 × 10⁻⁴ | 0.436 / 0.187 | 0.015 / 0.0076 | — | 9.2 × 10⁻⁶ |
| ΔN_eff 1 MeV (rel) | 0.542 / 0.413 | 0.039 / 2.6 × 10⁻⁴ | 0.436 / 0.187 | 1.2 × 10⁻³ / 6.1 × 10⁻⁴ | — | 9.7 × 10⁻⁶ |
| T_deliver (rel) | 27.9 / 0.31 | 0.027 / 7.3 × 10⁻⁵ | 27.9 / 13.9 | 2.0 × 10⁻⁶ | 27.9 / 13.9 | 2.0 × 10⁻⁶ |
| reflections (abs) | 36 / 8 | 16 / 0 | 51 / 7.5 | 0 / 0 | 0 / 0 | 1 |
| injections (abs) | 36 / 8 | 16 / 0 | 51 / 7.5 | 0 / 0 | 0 / 0 | 1 |
| bounce rows (abs) | 72 / 16 | 32 / 0 | 102 / 15 | 0 / 0 | — | 2 |
| candidates one-loop / gap / classical (abs) | 36 / 0 / 0 | 16 / 0 / 0 | 51 / 0 / 0 | 0 / 0 / 0 | — | 0 / 0 / 1 |
| drained share one-loop (abs) | 1.9 × 10⁻⁴ / 1.4 × 10⁻⁴ | 8.4 × 10⁻⁴ / 8.8 × 10⁻⁷ | 2.6 × 10⁻⁴ / 6.2 × 10⁻⁵ | 0 | — | 1.3 × 10⁻⁵ |
| drained share classical (abs) | 0 | 0 | 0 | 1.2 × 10⁻⁴ / 6.2 × 10⁻⁵ | — | 1.3 × 10⁻⁹ |
| parking T (rel) | 60.2 / 1.53 | 0.975 / 0.025 | 56.7 / 1.48 | 3.5 × 10⁻³ / 1.8 × 10⁻³ | — | 2.4 × 10⁻³ |
| tuning iterations (abs) | 3 / 0 | 0 | 0 | 0 | 0 | 0 |
| ln R at first impact (abs) | 14.9 / 1.28 | 0.110 / 1.2 × 10⁻³ | 14.9 / 7.45 | 1.2 × 10⁻³ | — | 1.2 × 10⁻³ |

- **Columns that never moved.** `candidates_gap`, `candidates_unassessed` and `drained_share_unassessed` moved in
  no run. `drained_share_gap` moved only in `r`, and `all_reflection_failing_share` only in `c` and `r`. `release_T_GeV`
  is blank on both sides in every history.
- **Natural mode.** `d`'s ΔN_eff is 0 on both sides, as at α = 0.
- **Where the changes are.** The large relative changes in T_deliver, ΔN_eff and parking T are the 13 histories of
  the table above and the rest of the edge. Away from the edge (β = 1.2, 2.0, 5.0) the largest are these:
  - at β = 5.0: T_deliver +2.7 % in every row (the four rows share the history up to the delivery), and ΔN_eff
    today −3.9 % at A = 0.05;
  - at β = 1.2, α = 0.1: parking T 1.16 × 10⁻⁵ → 2.91 × 10⁻⁷ GeV;
  - Δ(D/H) by −0.16 pp at β = 1.2, α = 0.1.

#### 2.4.6 The bounce rows (P19)

Each one-loop history's two `bounces.csv` were aligned by ordinal (37 histories). The full list is in `effect.txt`.
- **No history has an ordinal whose `kind` or `source` differs within the shorter file's length.** The rows
  alternate candidate and plateau the same way on both trees, wherever the counts differ.
- **Where the counts are equal.** Over all the rows of each history, the largest differences are these:

  | histories | rel N | abs ln T_J | rel α_eff |
  |---|---|---|---|
  | β = 2.0, A = 1, at 4 × 10⁻³¹ and every M of `b` (7) | ≤ 3.4 × 10⁻⁶ | ≤ 1.0 × 10⁻⁴ (10⁻⁴², ordinal 76) | ≤ 2.5 × 10⁻⁵ |
  | β = 2.0, A = 10 | 1.0 × 10⁻⁶ | 1.9 × 10⁻⁵ | 1.4 × 10⁻⁵ |
  | β = 1.2, A = 10 | 2.2 × 10⁻⁵ | 3.8 × 10⁻⁴ | 1.0 × 10⁻⁵ |
  | β = 1.2, A = 1 | 3.8 × 10⁻⁴ | 1.2 × 10⁻² | 8.7 × 10⁻⁴ |
  | β = 2.0, A = 0.05 | 9.9 × 10⁻⁴ | 3.1 × 10⁻² | 7.5 × 10⁻⁴ |
  | β = 5.0, A = 10 | 2.2 × 10⁻³ | 6.4 × 10⁻² | 2.2 × 10⁻³ |
  | `c`, β = 2.0, A = 1, at 10⁻¹⁵ and 10⁻¹² | 1.1 × 10⁻⁴ | 3.5 × 10⁻³ | 3.4 × 10⁻² (the last ordinal) |
  | a-edge β = 3.03 | 1.8 × 10⁻² | 0.40 | 1.7 × 10⁻² |

  P8 expected the drain records to differ only where the bounce sequence differs. Here the sequences agree in kind and source, and the values
  differ by the figures above. That is recorded, not bounded.
- **Where the delivery changed** (a-edge, and b's β = 3):
  - **Counts.** The counts change from 61–107 to 131–163 at 2.98–3.0 and at β = 3.0 in `b`, and by −10 to +16 at
    3.005–3.04.
  - **The rows differ from ordinal 0.** At β = 3.0 the first row moves by 0.14 relative in N and by 3.36 in ln T_J,
    a factor of 29 in T_J: the 1.68 → 48.5 GeV delivery. Later rows move by up to 4.42 in ln T_J (2.995, ordinal 74).
    They are other events, though kind and source still alternate the same way.
- **The other changes.** At β = 1.2 (A = 0.05: 285 → 279 rows) and β = 5.0 (A = 0.05: 457 → 489; A = 1: 139 → 141)
  the counts change, and N moves by at most 1.1 × 10⁻² relative before the end of the shorter file.
- **R-ladder (`r`):** 307 → 305 rows, N within 1.1 × 10⁻³ and α_eff within 0.21 relative at the last ordinal.

#### 2.4.7 Beside the figures

For each figure of `2026.10.0`, from its `NOTES.md` and its script: what it plots and over what range, which sampled
histories lie on it, and how far each moved in the figure's own units. The changes are stated, not judged
(README §4).

**Figure 6** (`fig6/`).
- **What it plots.** M = 4 × 10⁻³¹ M_P, φ* = 5 M_P, against β in four columns (0.45–1.05, 1.05–1.52, 1.52–3.07
  linear, 3.07–22 log). There are four rows:
  - ΔY_p = 100 `delta_Yp` [%] (−1.5 to 12.5);
  - Δ(D/H) [%] (−3 to 23);
  - ΔN_eff today (log, 10⁻⁴ to 1);
  - T_J at parking [GeV] (log, 10⁻¹³ to 10³).
- **What it draws.** The A = 0.05–3 band, A = 1 black, A = 3, 5 and 10 green and α = 0.01, 0.1 blue. It also marks
  "the electroweak cut (3.000–3.005)", placed by the delivery temperatures.
- **On it:** `a-edge`'s 13 at A = 1, and `a-spread`'s 12 (β = 1.2, 2.0, 5.0 at A = 0.05, the band's lower edge; A = 1;
  A = 10; α = 0.1).

| β | row | ΔY_p [%] old → new (change, pp) | Δ(D/H) [%] old → new (pp) | ΔN_eff today old → new (×) | T_J at parking [GeV] old → new |
|---|---|---|---|---|---|
| 2.98 | A = 1 | 0.193 → 0.096 (−0.096) | 0.471 → 0.225 (−0.246) | 0.0357 → 0.0177 (×0.49) | 8.9 × 10⁻³ → 3.95 × 10⁻² |
| 2.985 | A = 1 | 0.203 → 0.092 (−0.111) | 0.486 → 0.215 (−0.271) | 0.0372 → 0.0171 (×0.46) | 8.6 × 10⁻³ → 4.92 × 10⁻² |
| 2.99 | A = 1 | 0.199 → 0.110 (−0.089) | 0.503 → 0.261 (−0.242) | 0.0387 → 0.0201 (×0.52) | 7.0 × 10⁻⁴ → 4.29 × 10⁻² |
| 2.995 | A = 1 | 0.205 → 0.099 (−0.106) | 0.500 → 0.236 (−0.265) | 0.0381 → 0.0183 (×0.48) | 1.05 × 10⁻³ → 5.54 × 10⁻² |
| 3.0 | A = 1 | 0.153 → 0.089 (−0.064) | 0.371 → 0.205 (−0.166) | 0.0282 → 0.0160 (×0.57) | 1.43 × 10⁻² → 7.78 × 10⁻² |
| 3.005 | A = 1 | 0.068 → 0.078 (+0.010) | 0.150 → 0.174 (+0.024) | 0.0122 → 0.0139 (×1.14) | 8.6 × 10⁻² → 0.106 |
| 3.01 | A = 1 | 0.110 → 0.067 (−0.044) | 0.255 → 0.160 (−0.095) | 0.0200 → 0.0120 (×0.60) | 4.4 × 10⁻² → 0.131 |
| 3.015 | A = 1 | 0.097 → 0.057 (−0.040) | 0.229 → 0.127 (−0.102) | 0.0180 → 0.0104 (×0.58) | 5.9 × 10⁻² → 0.150 |
| 3.02 | A = 1 | 0.084 → 0.050 (−0.034) | 0.194 → 0.113 (−0.081) | 0.0155 → 0.0091 (×0.59) | 8.6 × 10⁻² → 0.165 |
| 3.025 | A = 1 | 0.072 → 0.045 (−0.027) | 0.163 → 0.094 (−0.068) | 0.0132 → 0.0080 (×0.61) | 0.117 → 0.181 |
| 3.03 | A = 1 | 0.065 → 0.040 (−0.024) | 0.138 → 0.083 (−0.055) | 0.0113 → 0.0072 (×0.64) | 0.140 → 0.196 |
| 3.035 | A = 1 | 0.054 → 0.038 (−0.015) | 0.121 → 0.081 (−0.040) | 0.0098 → 0.0067 (×0.68) | 0.156 → 0.212 |
| 3.04 | A = 1 | 0.051 → 0.034 (−0.017) | 0.110 → 0.068 (−0.042) | 0.0088 → 0.0062 (×0.71) | 0.169 → 0.226 |
| 1.2 | α = 0.1 | 1.056 → 1.074 (+0.018) | 1.481 → 1.320 (−0.161) | 0.06567 → 0.06565 | 1.16 × 10⁻⁵ → 2.91 × 10⁻⁷ |
| 2.0 | α = 0.1 | 0.085 → 0.086 (+0.0003) | 0.587 → 0.581 (−0.005) | 0.05244 → 0.05244 | 8.36 × 10⁻⁵ → 8.36 × 10⁻⁵ |
| 5.0 | α = 0.1 | 0.038 → 0.035 (−0.003) | 0.071 → 0.072 (+0.001) | 0.00627 → 0.00618 | 0.616 → 0.670 |
| 1.2 | A = 0.05 | 3.869 → 3.846 (−0.024) | 4.471 → 4.368 (−0.102) | 0.002997 → 0.002985 | 1.71 × 10⁻⁹ → 1.82 × 10⁻⁹ |
| 2.0 | A = 0.05 | 0.009 → 0.013 (+0.004) | 0.263 → 0.223 (−0.040) | 0.002342 → 0.002342 | 2.93 × 10⁻⁶ → 2.88 × 10⁻⁶ |
| 5.0 | A = 0.05 | 0.004 → 0.004 (−0.0002) | −0.001 → −0.004 (−0.003) | 0.000693 → 0.000666 | 1.36 × 10⁻² → 1.45 × 10⁻² |
| 1.2 | A = 1 | 1.271 → 1.257 (−0.014) | 5.402 → 5.456 (+0.054) | 0.05656 → 0.05653 | 1.01 × 10⁻⁸ → 1.02 × 10⁻⁸ |
| 2.0 | A = 1 | 0.015 → 0.012 (−0.004) | 0.459 → 0.459 (+0.0004) | 0.04462 → 0.04462 | 1.01 × 10⁻⁴ → 1.01 × 10⁻⁴ |
| 5.0 | A = 1 | 0.029 → 0.030 (+0.0015) | 0.056 → 0.054 (−0.003) | 0.00481 → 0.00495 | 0.502 → 0.486 |
| 1.2 | A = 10 | 2.772 → 2.772 (+0.0004) | 7.132 → 7.130 (−0.002) | 0.5331 → 0.5330 | 9.46 × 10⁻² → 9.46 × 10⁻² |
| 2.0 | A = 10 | 0.779 → 0.779 (−0.0003) | 1.940 → 1.947 (+0.007) | 0.1456 → 0.1456 | 0.313 → 0.313 |
| 5.0 | A = 10 | 0.110 → 0.109 (−0.002) | 0.252 → 0.253 (+0.002) | 0.01977 → 0.01978 | 57.1 → 60.8 |

- **Where the cut falls.** On the dense grid at A = 1 the delivery now jumps between 2.98 (1.95 GeV) and 2.985
  (20.6 GeV). In the run it jumped between 3.000 (1.68) and 3.005 (14.26).
- **The tests.** No sampled history changes side of Figure 6's tests: ±2.45 % in Y_p, ±2.37 % in D/H and
  ΔN_eff < 0.30. β = 1.2 at A = 0.05, 1 and 10 is beyond a test on both trees, and every other history is inside
  them on both. The largest moves are 0.11 pp in ΔY_p and 0.27 pp in Δ(D/H).
- **The parking row.** It shades a BBN window, 20 keV to 2 MeV (`T_BBN_GEV` in `fig6.py`). The edge histories' parking
  moves up by factors 1.3–61, into 39–226 MeV.
  - Two edge histories leave the window: β = 2.99 (0.70 → 42.9 MeV) and 2.995 (1.05 → 55.4 MeV).
  - β = 1.2 at α = 0.1 parks lower, 11.6 → 0.29 keV, below the window on both trees.

**fig6b** (`fig6b/`).
- **What it plots.** M = 10⁻¹⁵ and 10⁻¹² M_P, against β on Figure 6's columns: ΔY_p, Δ(D/H), ΔN_eff today (log,
  10⁻⁹ to 1) and ln R at the first impact (−40 to 32), for one-loop A = 1 and the fixed law (drawn once).
- **On it:** `c`'s 4 histories, all at β = 2.0.
  - One-loop A = 1: ΔY_p moves by +0.0004 and +0.0019 pp, and Δ(D/H) by +0.010 and +0.011 pp. ΔN_eff today goes
    from 6.82 × 10⁻⁶ to 6.72 × 10⁻⁶ (10⁻¹⁵) and from 6.75 × 10⁻⁶ to 6.65 × 10⁻⁶ (10⁻¹²), −1.5 %. ln R moves from
    0.5473 to 0.5461 (10⁻¹⁵) and from −27.0837 to −27.0849 (10⁻¹²).
  - Fixed α = 0.1: ΔY_p moves by +0.003 and +0.001 pp, Δ(D/H) by −0.004 and −0.001 pp, and ΔN_eff by +2 × 10⁻⁷.
- **The electroweak cut.** Its notes place fig6b's cut at Figure 6's 3.000–3.005, because the deliveries at M_crit
  equal the laboratory mass's. In the sample the new deliveries at β = 3.0 are 48.49 GeV at every M from 10⁻⁴² to
  0.5 M_P (`b`, `d`). β = 3.0 at M_crit was not sampled.

**m-trend** (`m-trend/`).
- **What it plots.** M = 10⁻³⁰ to 10⁻⁴² M_P (log, 3 × 10⁻⁴³ to 3 × 10⁻³⁰) on the sparse grid's 26 surfing β:
  - (a) ΔN_eff today relative to 4 × 10⁻³¹, the median over β for each A (0.38 to 1.08);
  - (b) ℓ at the first impact, A = 1, β = 1.2, 2 and 10 (28 to 74);
  - (c) the largest excluded β.
- **On it:** `b`'s 12 and `a-spread`/`a-edge`'s A = 1 at β = 2.0 and 3.0 at 4 × 10⁻³¹, two of the 26 β of panel (a).
  Panel (b)'s β = 2 line is the first candidate row's `ell` in `bounces.csv`.

| M / M_P | (a) at β = 2.0, old → new | (a) at β = 3.0, old → new | (b) ℓ, β = 2.0, old → new | ℓ at β = 3.0, old → new |
|---|---|---|---|---|
| 10⁻³⁰ | 1.02823 → 1.02823 | 1.0177 → 1.0102 | 32.5249 → 32.5246 | 33.146 → 36.945 |
| 10⁻³² | 0.891145 → 0.891145 | 0.9336 → 0.9590 | 36.6868 → 36.6865 | 37.319 → 41.179 |
| 10⁻³³ | 0.839843 → 0.839843 | 0.8965 → 0.9342 | 38.7228 → 38.7225 | 39.363 → 43.264 |
| 10⁻³⁶ | 0.751967 → 0.751966 | 0.8052 → 0.8670 | 44.5395 → 44.5392 | 45.222 → 49.328 |
| 10⁻⁴⁰ | 0.689711 → 0.689712 | 0.7341 → 0.8078 | 50.5452 → 50.5448 | 51.438 → 56.461 |
| 10⁻⁴² | 0.689901 → 0.689903 | 0.7351 → 0.8044 | 51.2501 → 51.2495 | 52.425 → 58.862 |
| 4 × 10⁻³¹ (reference) | 1 | 1 | 33.3610 → 33.3607 | 33.984 → 37.793 |

- **At β = 2** panels (a) and (b) move by at most 2 × 10⁻⁶ and 6 × 10⁻⁴.
- **At β = 3,** an edge history (1.68 → 48.49 GeV), the ratio moves by up to +0.074 (10⁻⁴⁰) and ℓ by +3.8 to +6.4.
  Panel (b) does not draw β = 3. Panel (a)'s median over 26 β includes it.

**relativistic** (`relativistic/`, the user's uncommitted work, read as data).
- **What it plots.** rel-lab, at M = 4 × 10⁻³¹ on Figure 6's columns: max r_χ E_field, E_field today, the worst birth
  and reflection margins, the share of K failing at a wall, and the parking share. rel-M plots A = 1 at every M.
  rel-history draws three histories at 4 × 10⁻³¹: A = 0.05, β = 1.0775; A = 1, β = 2.0; A = 10, β = 2.0.
- **What is compared here.** E comes from `tools/quanta_measures.py`, which this prompt did not run, so only the
  stored margins and shares are compared.
  - **On it:** rel-history's A = 1 and A = 10 at β = 2.0 (`a-spread`).
    - Margins: birth 19.331 → 19.331 and 29.104 → 29.103; reflection 19.434 → 19.434 and 30.275 → 30.275.
    - Parking share: 1.3259 × 10⁻⁴ → 1.3258 × 10⁻⁴ and 9.47 × 10⁻⁷ → 9.47 × 10⁻⁷.
  - **rel-M (A = 1)** at β = 2.0 for every M of `b`: the margins move by ≤ 0.001.
  - **rel-M at β = 3.0:** the worst birth margin rises by 2.0 (10⁻³⁰) to 6.3 (10⁻³⁶), and the parking share by
    factors of 1.5 (10⁻³⁶) to 6.0 (10⁻³²). At the edge at 4 × 10⁻³¹ (a-edge) the margins move by up to 4.8.
  - **At M_crit (`c`)** the shares move by ≤ 1.3 × 10⁻⁴, and the reflection failing share from 8.771 × 10⁻³ to
    8.896 × 10⁻³ (10⁻¹⁵).
  - No `all_reflection_failing_share` moved from 0 at the laboratory or lower M.

#### 2.4.8 Where the stores and exports are

- Stores: `var/datastores/electroweak-eos-02-old.db` and `electroweak-eos-02-new.db`, 16 shards each, with their
  sidecars (`.manifest.json`).
- Exports: `var/electroweak-eos/02/export/{old,new}/QCD_Cosmology/`, each with `histories.csv`, and the per-history
  `fields.csv` and `bounces.csv` under `alpha=<α>/csv/` and `one-loop-measured-v1/A=<A>/csv/`.
- Launch logs: `var/electroweak-eos/02/runs/`, with copies in `effect/out/runs/`. Scan consoles:
  `var/electroweak-eos/02/threshold/`. The registry's records: `var/runs/electroweak-eos-02-*`.
- Kept as the run's are; whether to keep them is 03's to say.

#### 2.4.9 The breakage record

- **The probe**:
  - With `--worker-csv prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`, from the main checkout on the new
    side's cluster, it exits 1 (`probe-7-breakage-worker-csv.txt`). Every worker's CSV path, its SHA-256
    (`28edd854…`) and Σ(300 GeV) (5.685 × 10⁻³) differ from the driver's. Afterwards the installed CSV is still
    `690090c8…0119d`.
  - The two real failures, probes 1 and 4 (§2.4.1), exit 1 for the same three fields.
- **`compare_with_run.py`.** Against a scratch copy of the main store's table with one cell changed (β = 3.0,
  4 × 10⁻³¹, A = 1: `Yp_BBN` …0973 → …0971), it reports that one column and exits 1
  (`effect/out/breakage-compare_with_run.txt`).
- **`bath_drift.py`** on the old table reproduces the issue's figures (§2.3), which stands for its check.

### 2.5 The suites

The suites ran once, serially, after the measurements (2026-11-13, 14:03–14:23 GMT), from the repository root, with
ComputeTargets in two halves. The figures in brackets are the wall time and the one-minute load at the start.
- CosmologyModels: 26 OK (67 s, 27.35).
- ObjectFactories: 163 OK (141 s, 11.91).
- RunDrivers: 76 OK (27 s, 6.87).
- ComputeTargets `[a-f]`: 219 OK (148 s, 11.28).
- ComputeTargets `[g-z]`: 465 OK (530 s, 5.85).
- tools: 292 OK (300 s, 13.18).

`uptime` at the end showed load averages of 18.80 / 13.61 / 13.71. Every count is 01's (§1.8). No file a suite imports
changed after `5947b4f`: `git diff --name-only 5947b4f` lists only campaign records (prompts, boards,
this document, `OPEN_ISSUES.md`), `CLAUDE.md` and the `science-runs/` READMEs.
`tools/build_w_table.py --check` exits 0 at the end.
