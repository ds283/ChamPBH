# surfing-temperature — verification

**Campaign:** [`prompts/surfing-temperature/`](../prompts/surfing-temperature/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/surfing-temperature/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`surfing-temperature/out/`](surfing-temperature/out/) ·
**Tools:** `tools/surfing_scan.py`, `tools/plot_surfing_temperature.py`, run unchanged

This document records the campaign's scan, run by prompt 04 under the user's exception U4 (README
§5 rule 12). There are two scans. S1 covers β = 1.10–5.00 in steps of 0.05. S5 covers β = 2.980–3.040
in steps of 0.001, near the β = 3.0 near-miss (U6). Each has its figure. The document says what was
measured, and how.

**Measurements, not bounds** (README §5 rule 10). The numbers below are reported, and compared with
the paper and the integrator audit where the prompt asks. None is tested against them. A difference
is a measurement, and is not an issue unless it shows a defect of the code. None did.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure, for instance after
`jordan-normalization` (§1.7), gets a new dated section. The figures below stay as they are, correct
for the tree they were taken on.

---

## 1. The scan (prompt 04, 2026-10-14)

**Tree:** `e7ed715` (the orchestrator's note for 04), whose code is `ee4a2df`'s (03b's). Between them
only `prompts/surfing-temperature/` changed (`git diff --stat ee4a2df e7ed715`: the board, log 03b
and the note). Both sidecars record `git_head` `e7ed715be2b7c6bcfe1a62ab68179d6482c639de`,
`git_tracked_changes: false`, `version_label` `2026.6.0`, Python 3.13.16 and SciPy 1.17.0. NumPy
2.3.5 and Matplotlib 3.10.8. Host `mps026088`, 10 CPUs. No Ray was started, no store opened, and no
PRyMordial run.

**Result: the gate "Scan" holds.**
- Every β of both grids has one row: 79 and 61, all `status=ok`. No history failed, and none
  reflected.
- Every first bounce lies above T_stop = 1 MeV. The lowest is 0.143435 GeV, at β = 1.10.
- β = 1.50 and 4.00, re-run, equal the scan's rows as strings, `wall_s` apart. β = 3.00 is equal
  in the two CSVs.
- Each figure was drawn by `plot_surfing_temperature.py` from its CSV and sidecar alone.
- S1 and S5 together took **442 s (7.4 min)** against P7's 60 minutes.

### 1.1 The tree and the run (S4 item 1)

**The commands**, from the repository root, in the foreground, in this order:

```bash
# S1, one chunk (the 1-minute load was 4.79 <= 10 at its start: note 04 §0)
./venv/bin/python tools/surfing_scan.py --beta-min 1.10 --beta-max 5.00 --beta-step 0.05 \
    --phi-init-Mp 5 --M 1e-3 --out .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv
# S5, one chunk
./venv/bin/python tools/surfing_scan.py --beta-min 2.980 --beta-max 3.040 --beta-step 0.001 \
    --phi-init-Mp 5 --M 1e-3 --out .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv
# S3, into the agent's scratch directory (not committed)
./venv/bin/python tools/surfing_scan.py --beta-values 1.50 4.00 --phi-init-Mp 5 --M 1e-3 \
    --out <scratch>/s3/s3_repro.csv
# the two figures
./venv/bin/python tools/plot_surfing_temperature.py \
    --csv .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv \
    --out .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3
./venv/bin/python tools/plot_surfing_temperature.py \
    --csv .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv \
    --out .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3_near3
```

`--T-stop-GeV` is the default, 1e-3. Λ is the tool's fixed 1e-3 eV (`Lambda_eV` in every row).

**The chunk plan.** Each scan ran as one chunk. Neither was cut off, so neither was resumed: each
sidecar has one entry, with `resumed: false` and `skipped: 0`.

| Invocation | Start – end (BST) | Load at start (1 / 5 / 15 min) | Wall | Exit |
|---|---|---|---|---|
| S1, 79 β | 20:43:18 – 20:48:05 | 4.79 / 7.43 / 7.97 | 287 s | 0 |
| S5, 61 β | 20:48:12 – 20:50:47 | 9.86 / 9.28 / 8.73 | 155 s | 0 |
| S3, 2 β | 20:50:52 – 20:50:59 | 9.29 / 8.59 / 8.47 | 7 s | 0 |
| figure, S1 (781 β of roots) | 20:51:13 – 20:52:05 | 7.96 / 8.34 / 8.38 | 52 s | 0 |
| figure, S5 (21 β of roots) | 20:52:05 – 20:52:09 | 5.93 / 7.65 / 8.12 | 4 s | 0 |

**The per-history times** are the CSVs' `wall_s` column. It covers `compute_scalar_model._function`
and the walk of `surfing_turning_points`, which was not timed separately (log 02).

| S1 sub-range | β | `wall_s` summed | per history |
|---|---|---|---|
| 1.10–3.00 | 39 | 35.0 s | 0.359–1.673 s |
| 3.05–4.00 | 20 | 91.1 s | 3.181–6.031 s |
| 4.05–5.00 | 20 | 154.1 s | 4.932–10.312 s |
| **all** | 79 | **280.1 s** | longest β = 5.00, 10.3 s |

S5's 61 histories sum to 149.6 s, 1.63–3.98 s each, the longest at β = 3.004. The invocations' wall
times exceed these sums by about 7 s, for the imports, the cosmology and `find_kicks`.

**The checks** (prompt §2; the commands and their full output are in log 04):
- **S1's CSV.** One header equal to `COLUMNS` and 79 data rows. The β column equals
  `beta_grid(1.10, 5.00, 0.05)` as `repr` strings, in order, and no β is repeated. The sidecar's
  one entry has `options` equal to the command's, and its `beta_grid` is the prompt's.
- **S5's CSV.** The same, with 61 rows and `beta_grid(2.980, 3.040, 0.001)`.
- **S3.** Every column but `wall_s` is compared as a string: 23 columns, 0 differ, for each of
  β = 1.5 and 4.0 (scan against re-run) and β = 3.0 (S1's CSV against S5's). `wall_s` was 0.505 and
  0.512 s, 4.847 and 4.719 s, and 1.659 and 1.847 s.
- **The outputs' sizes.** The eight files total 300 457 bytes, under §2's 1 MB:

  | File | Bytes |
  |---|---|
  | `surfing_scan_phi5_M1e-3.csv` | 18 327 |
  | `surfing_scan_phi5_M1e-3.csv.meta.json` | 1 721 |
  | `surfing_temperature_phi5_M1e-3.pdf` | 68 285 |
  | `surfing_temperature_phi5_M1e-3.png` | 96 492 |
  | `surfing_scan_phi5_M1e-3_near3.csv` | 10 942 |
  | `surfing_scan_phi5_M1e-3_near3.csv.meta.json` | 1 571 |
  | `surfing_temperature_phi5_M1e-3_near3.pdf` | 33 117 |
  | `surfing_temperature_phi5_M1e-3_near3.png` | 70 002 |

### 1.2 Failures and flags (S4 item 2)

- **Failures: none.** 79 of 79 and 61 of 61 rows are `ok`, so `failure_reason` is blank everywhere.
- **No bounce above T_stop: none.** Every row has `T_deliver_GeV`, and no console line carries the
  flag "captured, but no bounce above T_stop". Every delivery is a turning point:
  `deliver_reflected` is `false` and `reflections` is 0 in every row.
- **`multiple_crossings`** (01's flag) is not a CSV column. It was read from `reachability_roots`
  in item 5's scratch script (§1.5) for every kick at every β of both grids. It is `False` for all
  4 × 140 records.
  - The same script found the capture kick's `T_hot` and `T_cold` equal to the CSV's
    `T_hot_GeV` and `T_cold_GeV` as `repr` strings, at every captured row of both grids.
- **Not captured** (`captured=false`, `n_crossings=0`, the delivery kept): β = 1.10 and 3.00 in S1,
  and 21 β (2.999–3.019) in S5 (§1.8).
- `n_crossings` is even in every row, as a walk that ends on a falling T_J gives.

### 1.3 The dynamical thresholds (S4 item 3)

The analytic threshold is √((2 + Σ_peak)/(6 Σ_peak)) at `find_kicks`'s peak (01). The dynamical
threshold is the smallest β on a grid whose first episode that kick captures.

| Kick | Σ_peak (at) | Analytic | Dynamical, S1 grid (0.05) | Dynamical, S5 grid (0.001) | Difference |
|---|---|---|---|---|---|
| EW | 0.0374358 (53.262 GeV) | 3.011779 | 3.05 | **3.020** (3.019 not captured) | +0.0082 on S5; +0.038 on S1 |
| QCD | 0.314532 (0.18189 GeV) | 1.107448 | **1.15** (1.10 not captured) | — (outside S5's range) | +0.043, resolved only to 0.05 |
| μ/π | 0.0853651 (43.156 MeV) | 2.017786 | none on the grid | none on the grid | — |
| e⁺e⁻ | 0.100732 (0.16045 MeV) | 1.864339 | none on the grid | none on the grid | — |

- **EW.** On the fine grid the dynamical threshold lies in (3.019, 3.020], 0.0072–0.0082 above the
  analytic 3.0118.
- **QCD.** There is no fine grid near QCD's threshold, so its dynamical threshold is known only to
  lie in (1.10, 1.15]. That is 0–0.043 above the analytic 1.1074.
- **μ/π and e⁺e⁻** never capture the first episode on either grid. Both are reachable above their
  thresholds, and the figure labels their bands "reachable, not surfed".
- **The largest β that QCD captures** is 2.95 on S1, and 2.998 on S5 (§1.8).

The paper says the dynamical thresholds lie "slightly above" the analytic ones. Here, EW's is
0.008 above on the fine grid, and QCD's is at most 0.043 above. These are reported, not tested.

### 1.4 The measured temperatures against β (S4 item 4)

**Who captures** (S1): nothing at β = 1.10; QCD at 1.15–2.95 (37 β); nothing at 3.00; EW at 3.05–5.00
(40 β). μ/π and e⁺e⁻ capture nothing. `n_crossings` is 2–4 for QCD and 2–10 for EW, and rises with
β: 2 → 4 at 3.15, → 6 at 3.55, → 8 at 4.15 and → 10 at 4.90.

**The ratios T/T_hot**, with T_hot the hot root of the capture kick (the merged QCD + μ/π interval's
for β ≥ 2.20):

| Kick | T_capture/T_hot | T_max/T_hot | T_second_dip/T_hot | T_deliver/T_hot |
|---|---|---|---|---|
| QCD (S1, 37 β) | 0.82455 (2.95) – 0.90597 (1.30) | 0.96456 (1.15) – 1.03323 (2.95) | 0.99560 (2.80) – 0.99675 (2.60), 5 β | 0.96456 (1.15) – 1.01866 (2.95) |
| EW (S1, 40 β) | 0.77770 (5.00) – 0.84346 (3.05) | 0.99408 (3.05) – 1.06737 (5.00) | 0.98500 (5.00) – 0.99969 (3.15), 38 β | 0.99408 (3.05) – 1.00069 (3.10) |
| QCD (S5, 19 β) | 0.82370 (2.998) – 0.82400 (2.980) | 0.82392 (2.998) – 1.01534 (2.980) | none | as T_max |
| EW (S5, 21 β) | 0.84854 (3.040) – 0.86067 (3.020) | 0.86698 (3.020) – 0.98295 (3.040) | none | as T_max |

Over 1.15–2.95 the QCD capture ratio falls from about 0.90 to 0.82, and the EW one falls from 0.84
at 3.05 to 0.78 at 5.00. EW's T_max/T_hot rises from 0.994 to 1.067. EW's delivery lies within 1e-4
of T_hot from β = 3.40, and within 2e-5 from 3.95.

**The second dip:**
- **QCD.** It first appears at β = 2.60 and last at 2.80; it is absent at 1.15–2.55 and 2.85–2.95.
- **EW.** It first appears at β = 3.15 and appears at every β up to 5.00; it is absent at 3.05 and
  3.10.
- **S5.** No row has one.

**Jumps.** I took as a jump a change between adjacent β more than three times the larger of the
changes on either side of it. This was tested on ln T and on T/T_hot, for each series and each kick.
A change of the capture kick is a jump by the note's ruling. The criterion finds the following, each
with the rows either side.

- **The capture kick changes** at 1.10 → 1.15 (none → QCD), 2.95 → 3.00 (QCD → none) and
  3.00 → 3.05 (none → EW). Across the last of these, ln T_deliver changes by +3.611 (1.68001 →
  62.1396 GeV), against +0.154 and +0.098 either side. It is the only jump in ln T_deliver over all
  79 rows. §1.8 resolves it on the fine grid.

  | β | kick | T_capture | T_max | T_deliver | N_max | N_deliver |
  |---|---|---|---|---|---|---|
  | 2.95 | QCD | 1.58555 | 1.98683 | 1.9588 | 22.9112 | 24.09 |
  | 3.00 | — | — | — | 1.68001 | — | 24.499 |
  | 3.05 | EW | 52.7243 | 62.1396 | 62.1396 | 21.053 | 21.053 |

- **QCD, T_max/T_hot and T_deliver/T_hot, 1.15 → 1.20:** 0.96456 → 1.00840 (+0.0438). The next
  change is +0.0093. At both β the maximum is at the bounce (§1.5), and T_hot moves from 0.21154 to
  0.229183 GeV, just above the threshold 1.1074.

  | β | T_capture | T_max = T_deliver (GeV) | T_hot | T_max/T_hot |
  |---|---|---|---|---|
  | 1.15 | 0.188582 | 0.204043 | 0.21154 | 0.96456 |
  | 1.20 | 0.205376 | 0.231108 | 0.229183 | 1.00840 |
  | 1.25 | 0.220964 | 0.249237 | 0.244894 | 1.01773 |

- **QCD, T_deliver/T_hot, 2.90 → 2.95:** 0.99778 → 1.01866 (+0.0209), against +0.0023 before it.
  T_max − the maximum before the bounce − is smooth here (1.03216 → 1.03323). What changes is how
  long after the maximum the bounce comes: N_deliver − N_max is 2.753 at 2.90 and 1.179 at 2.95
  (3.493 at 2.85). So at 2.95 the field is delivered sooner after the overshoot, while T_J is still
  near its maximum.

  | β | T_max | T_deliver | T_hot | T_deliver/T_hot | N_max | N_deliver |
  |---|---|---|---|---|---|---|
  | 2.85 | 1.85214 | 1.78821 | 1.79638 | 0.99545 | 20.1913 | 23.6843 |
  | 2.90 | 1.91954 | 1.85561 | 1.85974 | 0.99778 | 21.1425 | 23.896 |
  | 2.95 | 1.98683 | 1.9588 | 1.92292 | 1.01866 | 22.9112 | 24.09 |

- **EW, T_deliver/T_hot, 3.05 → 3.10:** 0.99408 → 1.00069 (+0.0066), against −0.0010 after it. At
  3.05 the maximum is at the bounce (N_max = N_deliver = 21.053). At 3.10 the bounce comes 2.894
  e-folds after the maximum, and at 3.15 a second dip has appeared.

  | β | n | T_max | T_second_dip | T_deliver | T_hot | T_deliver/T_hot | N_max | N_deliver |
  |---|---|---|---|---|---|---|---|---|
  | 3.05 | 2 | 62.1396 | — | 62.1396 | 62.5099 | 0.99408 | 21.053 | 21.053 |
  | 3.10 | 2 | 68.6257 | — | 68.5449 | 68.4976 | 1.00069 | 18.3061 | 21.2002 |
  | 3.15 | 4 | 73.6867 | 73.1932 | 73.1932 | 73.216 | 0.99969 | 15.2716 | 21.3818 |

- **Two hits at the level of the series' own turning, recorded as found.**
  - QCD T_max/T_hot (= T_deliver/T_hot), 1.45 → 1.50: +0.00091, against −0.000034 and +0.00023 on
    either side. The ratio has its minimum near β = 1.42 (1.01407, 1.01149, 1.01146, 1.01237,
    1.01260 at 1.35–1.55), so the neighbouring change is near zero.
  - EW T_deliver/T_hot, 4.95 → 5.00: +4.4e-7, against 1.45e-7. Both ratios are 1.00000 to five
    digits.

**On S5** the criterion finds no jump in any series, including the first bounce across the
uncaptured window (§1.8).

### 1.5 Comparisons, as measurements (S4 item 5)

**The overshoot T_max/T_capture.** T_max is the largest upward crossing before the first bounce.
Wherever a second dip exists it is the first maximum (checked below), so T_max/T_capture is the rise
from the T_J minimum to the maximum that follows it.

Some maxima are not a free overshoot: the largest T_J is where the wall turns the field. A row was put
in the **wall** group if its maximum is within 0.01 e-fold of the first bounce (N_deliver − N_max <
0.01). For the 9 rows that qualify, N_deliver − N_max is 4e-7 to 1.3e-6 and T_max/T_deliver − 1 is
1e-7 to 2e-7. Among the other rows the smallest separation is 0.0217 e-fold, at β = 1.55. Every other
row is in the **free** group.

| Kick | Group | β | Rows | T_max/T_capture |
|---|---|---|---|---|
| QCD | wall | 1.15–1.50 | 8 | 1.08199 (1.15) – 1.12795 (1.25) |
| QCD | free | 1.55–2.95 | 29 | 1.12582 (1.70) – 1.25308 (2.95) |
| EW | wall | 3.05 | 1 | 1.17858 |
| EW | free | 3.10–5.00 | 39 | 1.21516 (3.10) – 1.37246 (5.00) |
| QCD (S5) | wall | 2.980–2.998 | 19 | 1.00027 (2.998) – 1.23220 (2.980) |
| EW (S5) | wall | 3.020–3.040 | 21 | 1.00733 (3.020) – 1.15841 (3.040) |

- **What "free" covers.** In the free group, 24 of 29 QCD rows and 1 of 39 EW rows (3.10) have
  only the one maximum, with no second dip before the bounce. There the bounce comes 0.02–3.98
  e-folds after the maximum. The wall may still shape those maxima, and the CSV does not separate the two
  effects. **The board's §5.3 counted "32 of 77"** captured β whose largest T_J is the wall's; that
  count is not reproduced by either criterion here (9 by N_deliver − N_max < 0.01; 34 rows have
  `n_crossings = 2`). The orchestrator's criterion was not recorded.
- **Against the integrator audit's F1.** F1 (`integrator-audit-2026-09-30/README.md` §8) measured
  the rise from the T_J minimum to the following maximum at **M = 0.5 M_P**, β = 1.2 and 2.0, and
  called it 12–13 %. Its own table gives 205.4 → 231.1 MeV and 657.1 → 749.9 MeV, ratios 1.1251 and
  1.1412. This run is at **M = 10⁻³ M_P**:
  - at β = 1.20, 0.205376 → 0.231108 GeV, N = 14.6147 → 17.6679, ratio **1.12529** (wall group);
  - at β = 2.00, 0.657065 → 0.749921 GeV, N = 14.1663 → 18.9479, ratio **1.14132** (free group).

  F1's windows are N = 14.58 → 17.66 and 14.17 → 18.97. Over β = 1.15–5.00 the ratio here ranges
  from 1.082 to 1.372. It rises with β throughout on EW, and on QCD from β = 1.70.

**The capture oscillator** (`eq:capture-oscillator` in `Paper1.tex`, read as data):
τ″ + τ′ + D τ = 0, with τ = ln(T_J/T_s) and D = −6 c_s²/(2 + Σ) · d ln Σ/d ln T_J at T_s. So
successive extrema of τ are π/√(D − 1/4) e-folds apart, and |τ| decays as e^(−N/2), a rate of 0.5
per e-fold.

- **Measured**, on the 43 rows with a second dip (QCD 2.60–2.80 and EW 3.15–5.00), with
  T_s = `T_hot_GeV`:
  - τ at the capture, the maximum and the second dip;
  - the two half-periods, N_max − N_capture and N_second_dip − N_max;
  - the decay per e-fold, −ln(|τ_b|/|τ_a|)/(N_b − N_a), for each pair.

  N_capture < N_max < N_second_dip holds on all 43, so the maximum is the first upward crossing.
- **Predicted**, at each row's T_hot on the production cosmology:
  - Σ = 1 − 3w and `slope_hot` as `reachability_roots` computes them;
  - c_s² = 1/(3 + `dG_s_dlogT`/`G_s`), as the temperature law reads them
    (`ComputeTargets/ScalarModel.py:441`).

  The scratch script evaluates functions of T only. It runs no history.

| | QCD (2.60–2.80, 5 rows) | EW (3.15–5.00, 38 rows) |
|---|---|---|
| D at T_hot | 0.855 – 0.939 | 0.517 (3.15) – 1.446 (5.00) |
| predicted half-period π/√(D − 1/4) | 4.039 – 3.786 | 6.075 (3.15) – 2.873 (5.00) |
| measured N_max − N_capture, against predicted | −1.1 % to −1.3 % | +18.5 % (3.15) to −3.2 % (5.00) |
| measured N_second_dip − N_max, against predicted | +0.17 % to +0.21 % | −0.47 % (3.35) to +1.10 % (5.00) |
| decay per e-fold, capture → max | 0.4968 – 0.4975 | 0.4815 (3.15) – 0.4986 (3.45) |
| decay per e-fold, max → second dip | 0.5003 – 0.5005 | 0.5000 (3.35) – 0.5034 (5.00) |

- **The second half-period** lies within 1.1 % of the linear prediction on every row. The decay
  from the maximum to the second dip is within 0.7 % of 0.5, and the decay from capture to the
  maximum within 3.7 %.
- **The first half-period differs more**, by up to 18.5 % at β = 3.15, where D is closest to 1/4.
  The paper expects the first undershoot not to be linear, since the velocity lags by about an
  e-fold. The CSV gives at most three extrema per row, against the paper's fit to many. These are
  reported; nothing is tested.
- **Where D falls below 1/4.** For the rows without a second dip, D is 0.281 at EW 3.05 and
  0.404 at 3.10. On S5's EW rows (3.020–3.040) it is 0.144–0.250, so the linear oscillator is
  overdamped or critical there. Those rows show one minimum and the wall's maximum only.
- **The commands and output.** The script is `<scratch>/item5_oscillator.py`, run as
  `./venv/bin/python <scratch>/item5_oscillator.py` from the repository root. It took 11 s at load
  10.3 and exited 0. Its output for the 43 rows is reproduced in log 04.

### 1.6 The figures (S4 item 6)

[`surfing_temperature_phi5_M1e-3.png`](surfing-temperature/out/surfing_temperature_phi5_M1e-3.png)
(and the [PDF](surfing-temperature/out/surfing_temperature_phi5_M1e-3.pdf)) is drawn from S1's CSV
and its sidecar. The title reads "git HEAD e7ed715", with no tracked change.

- **Upper panel.** T_J in GeV on a log axis against β = 1.10–5.00:
  - the four kicks' reachable bands, hot root solid and cold root dashed, with QCD + μ/π merged from
    2.155, and μ/π and e⁺e⁻ marked "reachable, not surfed";
  - the measured capture (▾), largest (▴), second dip (◇) and delivery (●) of every captured β,
    riding the QCD and EW hot roots;
  - open grey circles for the first bounce at the two uncaptured β (1.10 at 0.143 GeV, 3.00 at
    1.68 GeV).
- **Lower panel.** Each measured temperature over its row's T_hot. It shows the undershoot at
  capture (0.78–0.91), the overshoot (largest, up to 1.067) and delivery near 1. The 2.95 delivery
  at 1.019 stands above its neighbours (§1.4).

[`surfing_temperature_phi5_M1e-3_near3.png`](surfing-temperature/out/surfing_temperature_phi5_M1e-3_near3.png)
(and the [PDF](surfing-temperature/out/surfing_temperature_phi5_M1e-3_near3.pdf)) is drawn from S5's
CSV. Its axis is 2.95–3.05 (P4's outward rounding), and the roots are on 21 β.

- **Upper panel.** The QCD captures at 2.980–2.998, near 1.6–2 GeV. The chain of 21 grey rings is
  the uncaptured first bounces rising from 1.64 to 48.8 GeV. The EW captures follow from 3.020, at
  49–60 GeV.
- **Lower panel.** QCD's largest and delivery fall from 1.015 to 0.824 of T_hot, and meet the
  capture series at 2.998. EW's rise from 0.867 to 0.983. The capture series sit at 0.824 and
  0.85–0.86.

What is hard to read in each is recorded in log 04 ("Observations not acted on"). In short:
- the e⁺e⁻ label's second line overlaps eight edge marks in the fine figure;
- adjacent rings overlap in the fine figure;
- the EW band in the fine figure begins at the root grid's 3.015, not at 3.012.

### 1.7 What would move after `jordan-normalization` (S4 item 7)

This restates README §0.4 for this run, whose Λ is the CSV's `Lambda_eV`, 10⁻³ eV.
- **The roots: nothing.** Σ is a function of T_J alone, and T_J/MeV is unchanged by the
  normalization. The Planck-mass reset relabels MeV by at most 2.8 × 10⁻⁴.
- **The capture: nothing.** The surfing system (φ′, T_J) depends only on d ln A/dφ = β/M_P, and
  φ_ref does not enter it.
- **The largest temperature, the second dip and delivery: possibly something.** These are set by
  when the field reaches the wall, and that depends on Λ. The normalization makes Λ an output: 2.13
  meV against this run's 1 meV in the brief's test at β = 1.5.

**What the CSVs add.** The rows below are ones where the wall's arrival sets the value or the
outcome:
- the wall group of §1.5, where T_max is the bounce;
- whether a second dip appears (QCD 2.60–2.80);
- near β = 3.0, which β are captured at all. At 2.998 the QCD capture comes 0.05 e-folds before the
  bounce, and at 2.999–3.019 the bounce comes first (§1.8).

So the edges of S5's uncaptured window are also among what may move with Λ. The re-run is the
commands of §1.1, unchanged, on the tree after that campaign.

### 1.8 Near β = 3.0: the fine scan (S5; S4 item 8)

**The run.** One chunk, 61 β, 20:48:12 – 20:50:47 BST, 155 s wall, load 9.86 at its start, exit 0
(§1.1). Its `wall_s` sums to 149.6 s.

**Every fine-grid row.** Temperatures are in GeV, to six significant digits; the CSV has the full
values. A row with `captured = false` keeps its delivery, and the figure draws it as an open grey
circle (03b, U7).

| β | `captured` | `capture_kick` | `n_crossings` | `T_capture_GeV` | `T_max_GeV` | `T_deliver_GeV` |
|---|---|---|---|---|---|---|
| 2.98 | true | QCD | 2 | 1.61565 | 1.9908 | 1.9908 |
| 2.981 | true | QCD | 2 | 1.61665 | 1.98272 | 1.98272 |
| 2.982 | true | QCD | 2 | 1.61765 | 1.9731 | 1.9731 |
| 2.983 | true | QCD | 2 | 1.61866 | 1.96182 | 1.96182 |
| 2.984 | true | QCD | 2 | 1.61966 | 1.94877 | 1.94877 |
| 2.985 | true | QCD | 2 | 1.62066 | 1.93384 | 1.93384 |
| 2.986 | true | QCD | 2 | 1.62167 | 1.91694 | 1.91694 |
| 2.987 | true | QCD | 2 | 1.62267 | 1.89801 | 1.89801 |
| 2.988 | true | QCD | 2 | 1.62367 | 1.87702 | 1.87702 |
| 2.989 | true | QCD | 2 | 1.62468 | 1.85402 | 1.85401 |
| 2.99 | true | QCD | 2 | 1.62568 | 1.82909 | 1.82909 |
| 2.991 | true | QCD | 2 | 1.62668 | 1.80246 | 1.80246 |
| 2.992 | true | QCD | 2 | 1.62769 | 1.77445 | 1.77444 |
| 2.993 | true | QCD | 2 | 1.62869 | 1.74557 | 1.74557 |
| 2.994 | true | QCD | 2 | 1.62969 | 1.71662 | 1.71662 |
| 2.995 | true | QCD | 2 | 1.6307 | 1.68873 | 1.68873 |
| 2.996 | true | QCD | 2 | 1.6317 | 1.66365 | 1.66365 |
| 2.997 | true | QCD | 2 | 1.6327 | 1.64402 | 1.64402 |
| 2.998 | true | QCD | 2 | 1.63371 | 1.63415 | 1.63415 |
| 2.999 | false | — | 0 | — | — | 1.64143 |
| 3.0 | false | — | 0 | — | — | 1.68001 |
| 3.001 | false | — | 0 | — | — | 1.78121 |
| 3.002 | false | — | 0 | — | — | 2.03312 |
| 3.003 | false | — | 0 | — | — | 2.8215 |
| 3.004 | false | — | 0 | — | — | 6.95466 |
| 3.005 | false | — | 0 | — | — | 14.2578 |
| 3.006 | false | — | 0 | — | — | 20.1117 |
| 3.007 | false | — | 0 | — | — | 24.784 |
| 3.008 | false | — | 0 | — | — | 28.6443 |
| 3.009 | false | — | 0 | — | — | 31.8847 |
| 3.01 | false | — | 0 | — | — | 34.6634 |
| 3.011 | false | — | 0 | — | — | 37.0771 |
| 3.012 | false | — | 0 | — | — | 39.1857 |
| 3.013 | false | — | 0 | — | — | 41.0389 |
| 3.014 | false | — | 0 | — | — | 42.6785 |
| 3.015 | false | — | 0 | — | — | 44.1401 |
| 3.016 | false | — | 0 | — | — | 45.4559 |
| 3.017 | false | — | 0 | — | — | 46.653 |
| 3.018 | false | — | 0 | — | — | 47.7525 |
| 3.019 | false | — | 0 | — | — | 48.7703 |
| 3.02 | true | EW | 2 | 49.3572 | 49.7191 | 49.7191 |
| 3.021 | true | EW | 2 | 49.5512 | 50.6088 | 50.6088 |
| 3.022 | true | EW | 2 | 49.7189 | 51.4461 | 51.4461 |
| 3.023 | true | EW | 2 | 49.8724 | 52.2338 | 52.2338 |
| 3.024 | true | EW | 2 | 50.0164 | 52.9735 | 52.9735 |
| 3.025 | true | EW | 2 | 50.1532 | 53.6671 | 53.6671 |
| 3.026 | true | EW | 2 | 50.2842 | 54.3165 | 54.3165 |
| 3.027 | true | EW | 2 | 50.4106 | 54.9237 | 54.9237 |
| 3.028 | true | EW | 2 | 50.533 | 55.491 | 55.491 |
| 3.029 | true | EW | 2 | 50.6519 | 56.0205 | 56.0205 |
| 3.03 | true | EW | 2 | 50.7678 | 56.5146 | 56.5146 |
| 3.031 | true | EW | 2 | 50.8809 | 56.9755 | 56.9755 |
| 3.032 | true | EW | 2 | 50.9917 | 57.4057 | 57.4057 |
| 3.033 | true | EW | 2 | 51.1002 | 57.8075 | 57.8075 |
| 3.034 | true | EW | 2 | 51.2067 | 58.1835 | 58.1835 |
| 3.035 | true | EW | 2 | 51.3113 | 58.536 | 58.536 |
| 3.036 | true | EW | 2 | 51.4143 | 58.8672 | 58.8672 |
| 3.037 | true | EW | 2 | 51.5156 | 59.1793 | 59.1793 |
| 3.038 | true | EW | 2 | 51.6154 | 59.4741 | 59.4741 |
| 3.039 | true | EW | 2 | 51.7139 | 59.7533 | 59.7533 |
| 3.04 | true | EW | 2 | 51.811 | 60.0184 | 60.0184 |

**The edges.**
- **QCD** captures the first episode up to β = **2.998**, the largest such β on the grid.
- **EW** captures it from β = **3.020**, the smallest.
- **Nothing captures at 2.999–3.019**, 21 β, a window 0.021 wide between the two.
- On every captured row of S5 the maximum is at the bounce (N_deliver − N_max ≤ 1.5e-6), and there is
  no second dip.

**The QCD captures approach the wall.** N_capture moves from 21.59 at 2.980 to 24.47 at 2.998. Over
the same rows N_deliver is 24.22–24.52. At 2.998 T_capture and T_max are 1.63371 and 1.63415 GeV,
0.05 e-folds apart. At 2.999 there is no capture before the bounce.

**The first bounce across the window** rises from 1.64143 GeV (2.999) to 48.7703 GeV (3.019). That
is 1.0045 times the last QCD delivery (1.63415 at 2.998), and 0.981 times the first EW one (49.7191
at 3.020). Its N falls from 24.52 to 21.15. The rise is steepest at 3.002–3.006: the ratios of
adjacent deliveries are 1.39, 2.46, 2.05 and 1.41 at 3.003, 3.004, 3.005 and 3.006. Elsewhere they
are 1.0045–1.14 and 1.02–1.23. By §1.4's criterion, a change more than three times the larger of its
neighbours, no β is a jump. The largest step in ln T_deliver, +0.902 from 3.003 to 3.004, has
neighbours +0.328 and +0.718. So on this grid S1's jump between 3.00 and 3.05 (1.68 → 62.1 GeV) is
resolved into a rise with no jump by that criterion. Across the window's edges the first bounce
changes by +0.45 % (2.998 → 2.999) and +1.9 % (3.019 → 3.020).

| β | `captured` | T_deliver (GeV) | N_deliver | `accepted_steps` |
|---|---|---|---|---|
| 3.002 | false | 2.03312 | 24.3115 | 5915 |
| 3.003 | false | 2.8215 | 23.9798 | 8238 |
| 3.004 | false | 6.95466 | 23.071 | 9882 |
| 3.005 | false | 14.2578 | 22.357 | 6907 |
| 3.006 | false | 20.1117 | 22.0151 | 6314 |

**The EW dynamical threshold** (item 3) is 3.020 on this grid, beside 3.05 on S1's and the analytic
3.011779.

**The mechanism.** The orchestrator's account is in the board's §5.4, cited here as the
orchestrator's. At β just below 3.0118, the EW peak's Σ_eff falls just short of 1/(3β²). The field
then approaches the kinematic velocity without reaching it, and T_J nearly stalls for many e-folds.
The wall arrives during or after that stall, before the QCD kick captures. This document adds no
probe of its own: the table is what the CSV shows.

**The figure.** See
[`surfing_temperature_phi5_M1e-3_near3.png`](surfing-temperature/out/surfing_temperature_phi5_M1e-3_near3.png)
(§1.6). The uncaptured rows are the open grey circles, which overlap their neighbours where the rise
is shallow. The table above carries the values.

**Against the orchestrator's probe** (board §5.4a, 14 β at `9ab3e65`), as measurements:
- QCD captures at 2.991–2.997 with first bounces 1.80246–1.64402 GeV, and nothing at 2.999
  (1.64143 GeV). These are the probe's figures to its digits.
- Nothing at 3.012–3.015 (39.1857, 41.0389, 44.1401 GeV).
- EW from 3.020: capture 49.3572 GeV, bounce 49.7191 GeV.
- β = 2.998, which the probe did not run, is captured by QCD.

### 1.9 The suites

Run once, before the scans (prompt §2 step 1). They were not re-run after, since 04 changes no code.
Each was run serially in the foreground, with its output in the agent's scratch directory:

| Suite | Result | Wall | Load at start (1 min) |
|---|---|---|---|
| CosmologyModels | 18 OK | 67 s | 17.48 |
| ComputeTargets `test_[a-f]*.py` | 61 OK | 97 s | 11.45 |
| ComputeTargets `test_[g-z]*.py` | 115 OK | 104 s | 5.12 |
| ObjectFactories | 69 OK | 6 s | 4.66 |
| tools | 70 OK | 12 s | 4.69 |

These are the counts at 03b (board §5.4b).
