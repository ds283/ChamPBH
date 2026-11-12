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
