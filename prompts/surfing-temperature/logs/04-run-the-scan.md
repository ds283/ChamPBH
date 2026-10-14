# Log — surfing-temperature 04: run the scan

**Subject:** the 79-point scan (S1), the fine scan near β = 3.0 (S5, U6), their figures (S2), the
reproducibility check (S3), and `.documents/surfing-temperature-verification.md` (S4).
**Commit:** `90d44f8` · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent `e7ed715`
· **Model:** Claude Opus 5.5 · **Result:** landed. S1–S5 done; the gate "Scan" holds. No stop
condition fired:
- S1 and S5 took 442 s together, against P7's 60 minutes;
- every history finished, and none failed;
- every first bounce lies above T_stop;
- S3 found no difference;
- `git_tracked_changes` is `false` in every sidecar entry.

## What shipped

- **`.documents/surfing-temperature/out/`**, eight new files:
  - `surfing_scan_phi5_M1e-3.csv` (79 rows) and its `.csv.meta.json`;
  - `surfing_temperature_phi5_M1e-3.pdf` and `.png`;
  - `surfing_scan_phi5_M1e-3_near3.csv` (61 rows) and its `.csv.meta.json`;
  - `surfing_temperature_phi5_M1e-3_near3.pdf` and `.png`.

  300 457 bytes in all.
- **`.documents/surfing-temperature-verification.md`**, new, §1 with S4's eight items (§1.1–§1.8)
  and the suites (§1.9).
- **This log**, and the board: row 04, S1–S5, the gate "Scan", and §5.5.
- No code. No `.py` file is touched, and `.documents/OPEN_ISSUES.md` is untouched (no issue opened).

### The invocations

All from the repository root, in the foreground, each with the Bash tool's timeout at 600 000 ms.
Each console output went to a file in the scratch directory `<scratch>` =
`/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/8bbc912d-d00a-4f7f-98bf-bec65b6db83d/scratchpad/agent04`.
Before each scan and S3, `git status --porcelain --untracked-files=no` printed nothing.

| # | Command | Start – end (BST) | Load (1 / 5 / 15) | Wall | Exit |
|---|---|---|---|---|---|
| 1 | `mkdir -p .documents/surfing-temperature/out` | 20:43 | 5.04 / 7.52 / 8.00 | — | 0 |
| 2 | S1: `./venv/bin/python tools/surfing_scan.py --beta-min 1.10 --beta-max 5.00 --beta-step 0.05 --phi-init-Mp 5 --M 1e-3 --out .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv` | 20:43:18 – 20:48:05 | 4.79 / 7.43 / 7.97 | 287 s | 0 |
| 3 | S5: `./venv/bin/python tools/surfing_scan.py --beta-min 2.980 --beta-max 3.040 --beta-step 0.001 --phi-init-Mp 5 --M 1e-3 --out .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv` | 20:48:12 – 20:50:47 | 9.86 / 9.28 / 8.73 | 155 s | 0 |
| 4 | S3: `./venv/bin/python tools/surfing_scan.py --beta-values 1.50 4.00 --phi-init-Mp 5 --M 1e-3 --out <scratch>/s3/s3_repro.csv` | 20:50:52 – 20:50:59 | 9.29 / 8.59 / 8.47 | 7 s | 0 |
| 5 | S2: `./venv/bin/python tools/plot_surfing_temperature.py --csv .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv --out .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3` | 20:51:13 – 20:52:05 | 7.96 / 8.34 / 8.38 | 52 s | 0 |
| 6 | S5's figure: `./venv/bin/python tools/plot_surfing_temperature.py --csv .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv --out .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3_near3` | 20:52:05 – 20:52:09 | 5.93 / 7.65 / 8.12 | 4 s | 0 |

- **The chunk plan** (note §0). S1 was one chunk, because the one-minute load was 4.79 ≤ 10 at its
  start. S5 was one chunk. Neither was cut off, and nothing was resumed.
- **The figures' consoles.** S1's: "roots on 781 beta, 1.1 to 5", eight progress lines, then "wrote
  … in 50 s". S5's: "roots on 21 beta, 2.95 to 3.05" and "wrote … in 2 s".
- **The scans' consoles.** Each has one `@@` cosmology line and one line per β. The only flag is
  "[not captured]", on β = 1.1 and 3.0 in S1, and on 2.999–3.019 in S5.

## Deviations from the prompt

| # | Deviation | Tag |
|---|---|---|
| D1 | **`multiple_crossings` (S4 item 2) is not a CSV column**, so it was read from `reachability_roots` at every β of both grids, in item 5's scratch script, which the note allows to call that function. It is `False` everywhere. | STRUCTURALLY REQUIRED |
| D2 | **Item 5's script also gives item 2's flag and item 3's analytic thresholds** (from `find_kicks`'s Σ_peak, to six decimals), and checks the capture kick's roots against the CSV as strings. It is one script that evaluates functions of T and runs no history, so these did not need a script of their own. | IMPLEMENTATION CHOICE |
| D3 | **The overshoot's groups (item 5).** A row is "wall" if N_deliver − N_max < 0.01 (9 rows: QCD 1.15–1.50, EW 3.05) and "free" otherwise. The document also gives the `n_crossings = 2` subset (34 rows). Neither reproduces the board §5.3's "32 of 77", whose criterion is not recorded. The document says so. | IMPLEMENTATION CHOICE |
| D4 | **The jump criterion (item 4):** a change between adjacent β more than three times the larger of its neighbouring changes, on ln T and on T/T_hot, per kick and series; a change of capture kick is one (note §0). Every hit is reported with its rows, including two where the neighbouring change is near zero because the series turns. | IMPLEMENTATION CHOICE |
| D5 | **What the e⁺e⁻ label covers was measured on the committed PNG's pixels** (a scratch script reading colours), not by re-drawing the figure, which would have been a third plot run. | IMPLEMENTATION CHOICE |
| D6 | **Item 8's table gives temperatures to six significant digits**, and says the CSV has the full values. | IMPLEMENTATION CHOICE |
| D7 | **Item 7 adds one observation from the CSVs** to README §0.4's restatement: the outcomes the wall's arrival sets (the wall group, the QCD second dips, the edges of S5's uncaptured window) are among what may move with Λ. It is a reading of the CSV, not a probe. | IMPLEMENTATION CHOICE |
| D8 | **Analysis by scratch scripts reading the committed CSVs:** `check_csv.py` (§2), `compare.py` (S3), `analyse.py`, `jumps.py` and `tables.py` (items 2, 4, 5, 8), and `label_pixels.py` (D5). All are in `<scratch>`, and none is committed. | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT. The order of note §0 was kept: baselines, `mkdir`, S1, S5, S3, the two plots,
§2's checks, the document, then the log and the board. No tracked file was touched before the
scans, S3 and the plots had run.

## Verification performed

### The suites (before the scans; prompt §2 step 1)

Serially, in the foreground, each to `<scratch>/suites/*_before.txt`, with the verdict grepped:

| Suite | Result | Wall | Load at start (1 / 5 / 15) |
|---|---|---|---|
| CosmologyModels | `Ran 18 tests` OK | 67 s | 17.48 / 10.70 / 8.85 |
| ComputeTargets `test_[a-f]*.py` | `Ran 61 tests` OK | 97 s | 11.45 / 10.63 / 9.00 |
| ComputeTargets `test_[g-z]*.py` | `Ran 115 tests` OK | 104 s | 5.12 / 8.72 / 8.44 |
| ObjectFactories | `Ran 69 tests` OK | 6 s | 4.66 / 7.63 / 8.05 |
| tools | `Ran 70 tests` OK | 12 s | 4.69 / 7.58 / 8.03 |

These are 03b's counts and the orchestrator's baseline at `3fa4d53`. They were not re-run after,
because 04 changes no code (prompt §2 step 1). Nothing failed, so nothing was re-run.

### §2 steps 2 and 3: the CSVs and sidecars

`<scratch>/check_csv.py` checks each CSV:
- the header against `tools.surfing_scan.COLUMNS`;
- the number of data rows;
- the β column against `[repr(b) for b in beta_grid(...)]`, in order;
- duplicates;
- each sidecar entry's `options` against the command's, as
  `{"beta_min", "beta_max", "beta_step", "beta_values": None, "phi_init_Mp": 5.0, "M": 1e-3, "T_stop_GeV": 1e-3, "out": <the path as given>}`;
- the union of the entries' `beta_grid` against the grid.

Command: `./venv/bin/python <scratch>/check_csv.py`, from the root. Output, less the `@@` line:

```text
== .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv
header == COLUMNS: True; data rows: 79 (expected 79); grid size 79
beta column == grid as strings (in order): True
duplicate beta: 0
status counts: {'ok': 79}
sidecar entries: 1
  entry 0: options == command's: True; git_head e7ed715be2b7c6bcfe1a62ab68179d6482c639de; git_tracked_changes False; resumed False; skipped 0; version_label 2026.6.0; Lambda_eV 0.001; python 3.13.16; scipy 1.17.0; start 2026-10-14T19:43:19+00:00
union of entries' beta_grid == grid: True
== .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv
header == COLUMNS: True; data rows: 61 (expected 61); grid size 61
beta column == grid as strings (in order): True
duplicate beta: 0
status counts: {'ok': 61}
sidecar entries: 1
  entry 0: options == command's: True; git_head e7ed715be2b7c6bcfe1a62ab68179d6482c639de; git_tracked_changes False; resumed False; skipped 0; version_label 2026.6.0; Lambda_eV 0.001; python 3.13.16; scipy 1.17.0; start 2026-10-14T19:48:14+00:00
union of entries' beta_grid == grid: True
```

One entry per sidecar, with `resumed: false` and `skipped: 0`, which is consistent with one
invocation each. S3's scratch sidecar also records `e7ed715` with `git_tracked_changes: false`.

### §2 step 4: S3

`<scratch>/s3/compare.py` reads the two committed CSVs and the re-run, with `csv.DictReader`, as
strings. It compares every column but `wall_s` (23 of 24). Command:
`./venv/bin/python <scratch>/s3/compare.py <scratch>/s3/s3_repro.csv`. Output:

```text
beta=1.5 scan vs re-run: 23 columns compared, 0 differ []; wall_s 0.505 vs 0.512
beta=4.0 scan vs re-run: 23 columns compared, 0 differ []; wall_s 4.847 vs 4.719
beta=3.0 S1 CSV vs S5 CSV: 23 columns compared, 0 differ []; wall_s 1.659 vs 1.847
RESULT: equal
```

S3's console: `beta=1.5 … capture=0.321103 max=0.361394 second_dip=- deliver=0.361394 GeV
crossings=2 wall=0.5 s` and `beta=4.0 … capture=98.418 max=132.063 second_dip=124.59
deliver=125.766 GeV crossings=6 wall=4.7 s`. β = 1.5 equals T5's row (log 02) to its digits.

### §2 step 5: sizes

`ls -l .documents/surfing-temperature/out/` gives:

| File | Bytes |
|---|---|
| CSV (S1) | 18 327 |
| sidecar (S1) | 1 721 |
| PDF (S1) | 68 285 |
| PNG (S1) | 96 492 |
| CSV (S5) | 10 942 |
| sidecar (S5) | 1 571 |
| PDF (S5) | 33 117 |
| PNG (S5) | 70 002 |

`wc -c` totals 300 457 bytes, under 1 MB.

### Item 5's script, output for the 43 rows with a second dip

Command: `./venv/bin/python <scratch>/item5_oscillator.py`, from the root, 11 s at load 10.32,
exit 0. Its first sections give:
- the analytic thresholds: EW 3.011779, QCD 1.107448, μ/π 2.017786, e⁺e⁻ 1.864339;
- `multiple_crossings True for 0` on both grids;
- the capture kick's roots equal to the CSV's as strings on both;
- D for every captured row.

The comparison:

```text
  beta kick order_ok  N_cap N_max N_2  dN1 dN2 | pred_half | tau_cap tau_max tau_2 | decay1 decay2 (pred 0.5)
  2.6   QCD True  14.5464 18.5427 22.5884  3.9963 4.0458 | 4.039 (D 0.8549) | -0.17969 +0.02461 -0.003251 | 0.4975 0.5003
  2.65  QCD True  14.7579 18.6837 22.6609  3.9259 3.9772 | 3.970 (D 0.8761) | -0.18229 +0.02587 -0.003537 | 0.4973 0.5004
  2.7   QCD True  15.0288 18.8884 22.8009  3.8595 3.9125 | 3.905 (D 0.8971) | -0.18462 +0.02710 -0.003826 | 0.4971 0.5004
  2.75  QCD True  15.3809 19.1778 23.0293  3.7969 3.8515 | 3.844 (D 0.9180) | -0.18671 +0.02829 -0.004118 | 0.4970 0.5004
  2.8   QCD True  15.8512 19.5887 23.3826  3.7376 3.7938 | 3.786 (D 0.9387) | -0.18857 +0.02945 -0.004411 | 0.4968 0.5005
  3.15  EW  True  8.0717 15.2716 21.3213  7.1999 6.0497 | 6.075 (D 0.5174) | -0.20525 +0.00641 -0.000311 | 0.4815 0.5003
  3.2   EW  True  7.8369 13.8645 19.1751  6.0276 5.3106 | 5.332 (D 0.5971) | -0.21231 +0.01121 -0.000787 | 0.4880 0.5002
  3.25  EW  True  7.6569 13.0372 17.9567  5.3803 4.9196 | 4.939 (D 0.6546) | -0.21770 +0.01542 -0.001317 | 0.4921 0.5001
  3.3   EW  True  7.5118 12.4697 17.0913  4.9580 4.6216 | 4.642 (D 0.7080) | -0.22236 +0.01907 -0.001891 | 0.4954 0.5001
  3.35  EW  True  7.3914 12.0390 16.4142  4.6476 4.3752 | 4.396 (D 0.7607) | -0.22626 +0.02240 -0.002513 | 0.4976 0.5000
  3.4   EW  True  7.2889 11.6933 15.8694  4.4043 4.1761 | 4.193 (D 0.8114) | -0.22939 +0.02552 -0.003164 | 0.4986 0.5000
  3.45  EW  True  7.1994 11.4079 15.4311  4.2085 4.0232 | 4.036 (D 0.8560) | -0.23187 +0.02844 -0.003804 | 0.4986 0.5000
  3.5   EW  True  7.1194 11.1673 15.0705  4.0479 3.9032 | 3.911 (D 0.8952) | -0.23392 +0.03112 -0.004420 | 0.4983 0.5001
  3.55  EW  True  7.0469 10.9609 14.7672  3.9140 3.8062 | 3.811 (D 0.9295) | -0.23567 +0.03358 -0.005004 | 0.4979 0.5002
  3.6   EW  True  6.9805 10.7806 14.5024  3.8001 3.7218 | 3.725 (D 0.9613) | -0.23723 +0.03580 -0.005564 | 0.4976 0.5002
  3.65  EW  True  6.9193 10.6211 14.2677  3.7018 3.6466 | 3.648 (D 0.9916) | -0.23864 +0.03784 -0.006106 | 0.4975 0.5003
  3.7   EW  True  6.8626 10.4784 14.0576  3.6158 3.5791 | 3.579 (D 1.0205) | -0.23991 +0.03973 -0.006629 | 0.4973 0.5003
  3.75  EW  True  6.8100 10.3498 13.8682  3.5398 3.5184 | 3.517 (D 1.0481) | -0.24105 +0.04149 -0.007134 | 0.4971 0.5004
  3.8   EW  True  6.7609 10.2330 13.6965  3.4721 3.4635 | 3.460 (D 1.0745) | -0.24208 +0.04314 -0.007622 | 0.4968 0.5005
  3.85  EW  True  6.7148 10.1260 13.5399  3.4112 3.4139 | 3.409 (D 1.0995) | -0.24300 +0.04469 -0.008092 | 0.4964 0.5006
  3.9   EW  True  6.6714 10.0273 13.3963  3.3559 3.3689 | 3.362 (D 1.1233) | -0.24382 +0.04616 -0.008544 | 0.4959 0.5007
  3.95  EW  True  6.6303 9.9359 13.2641  3.3056 3.3281 | 3.319 (D 1.1458) | -0.24455 +0.04754 -0.008979 | 0.4954 0.5008
  4.0   EW  True  6.5913 9.8508 13.1418  3.2595 3.2910 | 3.280 (D 1.1671) | -0.24520 +0.04886 -0.009396 | 0.4949 0.5009
  4.05  EW  True  6.5540 9.7712 13.0282  3.2172 3.2570 | 3.245 (D 1.1873) | -0.24579 +0.05010 -0.009797 | 0.4944 0.5011
  4.1   EW  True  6.5185 9.6965 12.9224  3.1781 3.2258 | 3.212 (D 1.2065) | -0.24632 +0.05127 -0.010181 | 0.4938 0.5012
  4.15  EW  True  6.4844 9.6264 12.8237  3.1420 3.1973 | 3.182 (D 1.2247) | -0.24680 +0.05239 -0.010548 | 0.4933 0.5013
  4.2   EW  True  6.4518 9.5604 12.7316  3.1086 3.1712 | 3.154 (D 1.2419) | -0.24725 +0.05345 -0.010899 | 0.4927 0.5014
  4.25  EW  True  6.4204 9.4981 12.6453  3.0777 3.1472 | 3.129 (D 1.2580) | -0.24766 +0.05446 -0.011233 | 0.4921 0.5016
  4.3   EW  True  6.3903 9.4392 12.5640  3.0489 3.1248 | 3.106 (D 1.2732) | -0.24806 +0.05542 -0.011554 | 0.4916 0.5017
  4.35  EW  True  6.3612 9.3833 12.4868  3.0220 3.1036 | 3.084 (D 1.2877) | -0.24844 +0.05632 -0.011865 | 0.4911 0.5018
  4.4   EW  True  6.3332 9.3300 12.4131  2.9968 3.0831 | 3.063 (D 1.3021) | -0.24880 +0.05717 -0.012166 | 0.4907 0.5019
  4.45  EW  True  6.3061 9.2791 12.3425  2.9730 3.0634 | 3.043 (D 1.3162) | -0.24914 +0.05799 -0.012459 | 0.4904 0.5020
  4.5   EW  True  6.2800 9.2304 12.2748  2.9505 3.0443 | 3.023 (D 1.3300) | -0.24945 +0.05876 -0.012743 | 0.4900 0.5021
  4.55  EW  True  6.2546 9.1838 12.2098  2.9291 3.0261 | 3.004 (D 1.3437) | -0.24975 +0.05950 -0.013020 | 0.4897 0.5021
  4.6   EW  True  6.2301 9.1390 12.1476  2.9089 3.0086 | 2.986 (D 1.3570) | -0.25001 +0.06022 -0.013290 | 0.4894 0.5022
  4.65  EW  True  6.2062 9.0959 12.0880  2.8897 2.9921 | 2.968 (D 1.3702) | -0.25025 +0.06091 -0.013550 | 0.4890 0.5023
  4.7   EW  True  6.1831 9.0545 12.0311  2.8715 2.9765 | 2.952 (D 1.3829) | -0.25047 +0.06158 -0.013801 | 0.4886 0.5025
  4.75  EW  True  6.1606 9.0147 11.9768  2.8542 2.9621 | 2.936 (D 1.3949) | -0.25066 +0.06224 -0.014043 | 0.4881 0.5026
  4.8   EW  True  6.1387 8.9764 11.9252  2.8378 2.9487 | 2.922 (D 1.4063) | -0.25083 +0.06287 -0.014276 | 0.4876 0.5028
  4.85  EW  True  6.1173 8.9395 11.8760  2.8222 2.9364 | 2.908 (D 1.4171) | -0.25099 +0.06348 -0.014498 | 0.4871 0.5029
  4.9   EW  True  6.0965 8.9040 11.8291  2.8075 2.9251 | 2.895 (D 1.4272) | -0.25114 +0.06407 -0.014711 | 0.4865 0.5030
  4.95  EW  True  6.0762 8.8698 11.7843  2.7935 2.9145 | 2.884 (D 1.4367) | -0.25128 +0.06465 -0.014914 | 0.4860 0.5032
  5.0   EW  True  6.0564 8.8367 11.7413  2.7802 2.9046 | 2.873 (D 1.4455) | -0.25141 +0.06519 -0.015109 | 0.4855 0.5034
```

### The figures, looked at

Both PNGs were read, with greyscale copies (`<scratch>/fig/*_grey.png`) and crops of the fine
figure's bottom edge and ring chain (`<scratch>/fig/near3_label_zoom.png`, `near3_rings_zoom.png`).

- **S1's figure.** The title reads "git HEAD e7ed715". The figure reads as in 03's and 03b's reviews
  of the orchestrator's probe:
  - the bands, the threshold labels (QCD 1.107, e⁺e⁻ 1.864, μ/π 2.018, EW 3.012), and "reachable,
    not surfed" on μ/π and e⁺e⁻;
  - the capture, largest and delivery markers riding the hot roots, and second-dip diamonds on
    QCD 2.60–2.80 and EW 3.15–5.00;
  - rings at 1.10 (0.143 GeV) and 3.00 (1.68 GeV);
  - edge ticks at 1.10 and 3.00 in both panels.

  In the lower panel, the 2.95 delivery (1.019) and largest (1.033) stand above their neighbours.
  The labels are clear of the edge ticks. In greyscale the series are told apart by shape. Nothing
  is hard to read that was not already recorded in 03's review.
- **S5's figure.** Described under "Observations not acted on".

## The deliberate-breakage record

None. The prompt names no breakage, since it changes no code (dk-and-rpk-migration rule 8 applies to
a prompt whose tests must catch one).

## Observations not acted on

- **The e⁺e⁻ band's label in the fine figure** (note §0; the user's to revisit). The label is in two
  lines, olive, at the bottom centre of the upper panel. It was measured on the committed PNG
  (827 × 900 px) by colour:
  - the upper panel's bottom spine is at y = 519;
  - the 21 uncaptured edge marks are dark grey bars at y = 510–528, 3 px wide, centred at x = 440,
    448, …, 587. A linear fit gives β ≈ 2.93913 + 0.000136 x.
  - **"e⁺e⁻"** (y 499–507, x 434–460, about β = 2.9981–3.0017) lies above the marks' tops, and
    overlaps none.
  - **"reachable, not surfed"** (y 512–519, x 396–497, about β = 2.9930–3.0067) lies across the
    upper halves of **eight edge marks, β = 2.999, 3.000, 3.001, 3.002, 3.003, 3.004, 3.005 and
    3.006**. The mark at 3.007 (x 498–500) is just clear of its last letter.
  - The marks (z-order 20) are drawn over the text. So the letters of ", not surfed" are broken by
    the bars, and "reachable" (x 396–439) is clear. No mark is hidden: all 21 are found as separate
    columns.
  - **No ring and no captured marker is hidden.** The rings and markers lie at 1.6–60 GeV, more than
    four decades above the label. The lowest captured-marker pixel is at y ≈ 228, against the
    label's 499–519. The e⁺e⁻ cold-root dashed line (y 488–490) is above "e⁺e⁻".

  The label reads "e⁺e⁻ / reachable, not surfed" with effort. The marks under it remain countable.
- **The rings overlap in the fine figure** (note §0; 03b's review). Where the first bounce rises
  slowly, adjacent rings 0.001 apart overlap in chains:
  - at 2.999–3.002 (1.64–2.03 GeV), where they also touch the last QCD disc at 2.998;
  - at 3.007–3.019 (24.8–48.8 GeV), where 3.019's ring touches the first EW disc at 3.020.

  At 3.003–3.006, the steep part, the rings are separated vertically. Every ring can be counted on
  the 2× crop. The table in the document's §1.8 carries the values.
- **The EW band in the fine figure starts at β = 3.015.** The root grid's step is 0.005, and 3.015 is
  its first point above the analytic 3.0118, while the threshold line is drawn at 3.012. So the
  rings at 3.015–3.019 sit inside the drawn band, and the ones at 3.012–3.014 sit left of it. This is
  P4's grid as designed; no value is misdrawn.
- **The board §5.3's "32 of 77"** wall-type maxima is not reproduced by either criterion here
  (D3). The orchestrator's criterion is not recorded, so the difference is not resolved here.
- **The wall sets outcomes that README §0.4 counts as capture** (document §1.7). README §0.4 says
  the capture will not move with Λ. The fine grid shows the wall arriving before or at capture at
  2.998–3.019. So which β near 3.0 are captured depends on the wall's timing, and may move with Λ.
  This is a measurement for the re-run after `jordan-normalization`, not a defect. No issue opened.
- **Against the orchestrator's probes** (board §5.4, §5.4a): S1 has the shape the note expected. S5's
  rows equal the §5.4a probe's quoted temperatures at 2.991, 2.997, 2.999, 3.012, 3.013, 3.015 and
  3.020. β = 2.998, which the probe did not run, is captured by QCD, so the uncaptured window is
  2.999–3.019.
- No issue opened, and `.documents/OPEN_ISSUES.md` is untouched. Nothing was written outside
  `<scratch>`, `.documents/surfing-temperature/`, the verification document, this log and the board.

## State handed to the next prompt

- **The branch.** `surfing-temperature`, `90d44f8` on top of `e7ed715`. Nothing is staged or
  modified after it. The untracked entries are the user's, as before: 17 `pilot-2026.6.0*.db`,
  `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`,
  `full_run_2026.6.0-refresh.sh`, `prompts/jordan-normalization/`.
- **Counts:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 70. These
  are unchanged, since no code changed.
- **The outputs**, under `.documents/surfing-temperature/out/`:
  - `surfing_scan_phi5_M1e-3.csv`: 79 rows, β = 1.10–5.00 by 0.05, all `ok`. QCD captures 1.15–2.95,
    EW 3.05–5.00, and 1.10 and 3.00 are not captured.
  - `surfing_scan_phi5_M1e-3_near3.csv`: 61 rows, 2.980–3.040 by 0.001, all `ok`. QCD captures to
    2.998, EW from 3.020, and 2.999–3.019 are not captured.
  - Each has a one-entry sidecar (`e7ed715`, no tracked change) and a PDF and PNG figure.
- **`.documents/surfing-temperature-verification.md`** exists, with §1 (items 1–8 and the suites).
  05 may cite it. The handover's re-run command is §1.1's, unchanged.
- **The headline measurements:**
  - EW's dynamical threshold is 3.020 on the fine grid, against 3.0118 analytic;
  - QCD's lies in (1.10, 1.15], against 1.1074;
  - μ/π and e⁺e⁻ never capture the first episode;
  - T_max/T_capture is 1.082–1.372;
  - the oscillator's second half-period agrees with π/√(D − 1/4) to within 1.1 %, and the decay
    with 0.5 per e-fold to within 3.7 %.
- **For the user** (note §0): the e⁺e⁻ label's second line overlaps the edge marks at β = 2.999–3.006
  in the fine figure. No ring or captured marker is hidden.
- **No issue opened;** the index is untouched.
