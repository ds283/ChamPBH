# 2026-11-21 — the `2026.14.0` science run

The first run of both potentials. It ran at `VERSION_LABEL` `2026.14.0`, the tree `power-law-wall` left. It builds:
- the exponential at n = 1, on [`2026.11.0`](../2026-11-13-science-2026.11.0/README.md)'s grid, under the refitted
  one-loop law `one-loop-exponential-v2`;
- the inverse power law V = V₀[1 + (M/φ)ⁿ] at n = 1 and 2, on the same grid without blocks C and D, under
  `one-loop-inverse-power-v1`.

It ran on the Mac Studio over SSH, the first run of record to do so, after its probe. Its plan is [`PLAN.md`](PLAN.md): §0.1 and §0.2 hold
the user's rulings in their words, §1 the grid, §2 where it ran, §3 the probe and §5 the analyses.

**Status: current.** It supersedes 2026.11.0, whose stores the tree cannot read (format 7 against 8). Every
registered block exited 0 at the first attempt, and all three stores are whole and validated. Of the 13 806
histories, 1037 failed (below, "Failed histories"):
- the exponential has 6 failures above the QCD surfing threshold, as in 2026.11.0;
- the power law fails at every M at n = 1 for every history at β ≥ 10 (β = 10, 15 and 20 at laboratory M; 10 and 20
  at B's M), and at n = 2 at β = 20 at 10⁻⁴⁰ and 10⁻⁴² M_P. These are overflows in the power wall's potential, not convergence failures.

**Figures made from it.** Each folder holds its script, its inputs and a note. They were made on 2026-11-22 by a Claude
Code session at the user's request, one figure per wall where the analysis has one, and the user reviewed the notes
on 2026-11-22:
- [`fig6/`](fig6/NOTES.md): Figure 6 at the laboratory mass, for `exp`, `pl1` and `pl2`. The walls look alike because
  everything before the first impact, and the whole fixed law, does not depend on the wall. Under the one-loop law a
  power-wall impact drains less (median ΔN_eff at laboratory M ×0.85 at n = 2, ×0.74 at n = 1). That shows mainly at
  A = 5, where 86, 63 and 41 histories are excluded (exp, pl2, pl1);
- [`relativistic/`](relativistic/NOTES.md): the quanta stay relativistic in energy on every wall. The matter-like
  share of ρ_R is ≤ 1.0×10⁻³² at or below 10⁻³⁰ M_P, and the exponential's margins hold far more widely than in
  2026.11.0;
- [`m-trend/`](m-trend/NOTES.md): the trend in M. The walls converge at 10⁻⁴² M_P (A = 1: ΔN_eff 0.026 on all three),
  so n = 1's gentler fall comes from where it starts at laboratory M. Under the new k_ex the exponential's ℓ has no
  maximum over M;
- [`fig6b/`](fig6b/NOTES.md): Figure 6 around M_crit, exponential only; essentially 2026.11.0's;
- [`changes/`](changes/NOTES.md): what moved since 2026.11.0 (PLAN.md P14). The fixed law is unchanged to the bit,
  and the one-loop ΔN_eff falls 4.9% at laboratory M and 10% at 10⁻⁴².

## Rulings after the build

- **The electroweak bracket** is (2.980, 2.985) on every wall, read from the exports (`fig6/NOTES.md`). The user
  asked for this on 2026-11-22: "fix m_trend and set the brackets".
- **The power walls' ln R extrapolation.** The user, 2026-11-22: "For now we'll accept the `ln R` extrapolation".
  The power-wall law's falling branch was measured to ln R = 554.5 (n = 1) and 344.7 (n = 2). This run's first
  impacts reach ln R = 635 and 383 at laboratory M, and 740 and 490 at 10⁻⁴² M_P. The notes first stated the measured
  range as 100 and 60; `fig6/NOTES.md` and `m-trend/NOTES.md` carry the correction. The test that followed
  ([`lnR-test/`](lnR-test/NOTES.md)) found the law's offset unchanged out to ln R = 700 (n = 1) and 500 (n = 2).

## Standard analyses

From `science-runs/README.md`'s list. The run script made every input they read (PLAN.md P9).
- **Figure 6:** made for each wall, [`fig6/`](fig6/NOTES.md).
- **Relativistic quanta:** made for each wall, [`relativistic/`](relativistic/NOTES.md), from the run's measures. As
  in 2026.11.0 it does not cover the fixed law, the parking leftover or the 19 K-check failures. On the power walls
  x_* and the associated piece are carried over from the exponential, unmeasured there (`power-law-wall` U5).

## The run

| | |
|---|---|
| When | 2026-11-21 01:17:43 UTC – 2026-11-22 03:03:12 UTC, 25 h 45 min. The build ran to 00:05:45, the inventories to 00:08:59, then the exports, measures and figure tools |
| Tree | `a916149` for every block, `git_dirty` false in every manifest, `main.py`'s SHA-256 `27056364…` |
| Labels | `VERSION_LABEL` `2026.14.0`, `FINGERPRINT_FORMAT` 8, `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (small network). One-loop rows are under `one-loop-exponential-v2` and `one-loop-inverse-power-v1`; spectra are `R8-exponential-v2`, `R8-power-n1-v2` and `R8-power-n2-v2` |
| Packages | datastorekit 0.4.0, raypoolkit 0.3.0, runregistrykit 0.2.0, ray 2.59.0, SQLAlchemy 2.0.55, numpy 2.3.5, scipy 1.17.0; Python 3.13.16 |
| Host | the Mac Studio, `mps026298.phys.susx.ac.uk` (arm64, macOS, 10 cores, 32 GB), over SSH. ChamPBH's isolated Ray cluster at 127.0.0.1:6380 with 8 CPUs, started and stopped with `tools/ray_cluster.sh` (PLAN.md P5) |
| Exponential store | `var/datastores/science-2026.14.0-exponential.db` on the Mac Studio, 16 shards, 8.8 GB: 4818 histories (A, B, C, D), 4624 BBN records, inventory "problems: none" |
| Power-law store | `var/datastores/science-2026.14.0-inverse-power.db`, 16 shards, 16 GB: 8904 histories (A and B at n = 1 and 2), 8479 BBN records, "problems: none" |
| Reflection store | `var/datastores/science-2026.14.0-reflections.db`, 16 shards, 254 MB: 84 histories (44 exponential, 40 power law), 84 BBN records, "problems: none" |
| Script | [`run_science_2026.14.0.sh`](run_science_2026.14.0.sh): the build, the inventories, the exports, the measures, the figure tools and P11's `gzip` of each wall's `fields.csv` |
| Output | `var/science-2026.14.0/out/` on the Mac Studio, 36 GB (below, "Where the output is") |
| Disk | 156 GB free at the start. The script's gate (55 GB for a block, 70 GB for a wall's exports) was never reached; the least was 108 GB, before the last wall's exports. 97 GB free after the run |

Every history starts at φ* = 5 M_P, with T* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, `--tuning-max-integrations` 30,
atol = rtol = 10⁻⁸, 16 shards and `--bbn-wall-clock-limit 600`. The slots, from PLAN.md §1, in the order they ran:

| Slot | Store | Potential, n | M / M_P | β grid | fixed α | one-loop A | Histories |
|---|---|---|---|---|---|---|---|
| A-exp | exponential | exp, 1 | 4×10⁻³¹ | dense, 191 values, 0.5–20 | 0.01, 0.1 | 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10 | 2292 |
| A-pl | inverse-power | pl, 1 and 2 | 4×10⁻³¹ | dense | 0.01, 0.1 | the ten | 4584 |
| B-exp | exponential | exp, 1 | 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰, 10⁻⁴² | sparse, 30 values | 0.01, 0.1 | the ten | 2160 |
| B-pl | inverse-power | pl, 1 and 2 | as B-exp | sparse | 0.01, 0.1 | the ten | 4320 |
| D-exp | exponential | exp, 1 | 10⁻³, 0.1, 0.5 | coarse, 62 values | 0 (natural mode) | — | 186 |
| R-lab | reflections | exp 1; pl 1, 2 | 4×10⁻³¹ | 1.2, 2.0, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 + 30 |
| R-ladder | reflections | exp 1; pl 1, 2 | 10⁻¹⁶ | 2.0 | 0.01, 0.1 | 0.1, 1, 10 | 5 + 10 |
| R-natural | reflections | exp, 1 | 10⁻³, 0.5 | 1.2, 2.0, 2.9 | 0 | — | 6 |
| C-exp | exponential | exp, 1 | 10⁻¹⁵, 10⁻¹² | sparse | 0.01, 0.1 | 1 | 180 |
| R-Mcrit | reflections | exp, 1 | 10⁻¹⁵, 10⁻¹² | 1.2, 2.0, 2.9 | 0.01, 0.1 | 1 | 18 |

The exponential's slots are 2026.11.0's, history for history (PLAN.md P1). The power law has no C, D, R-natural or
R-Mcrit (PLAN.md §0.1 and §0.2).

## The registered runs

Each slot ran through `RunDrivers/registered_run.py` as `--campaign science-2026.14.0 --prompt <slot>`. Their
`manifest.json` and `status.json` are copied unchanged into [`registry/runs/`](registry/runs/), and the three stores'
fingerprint sidecars, as they stood after the run, into [`registry/stores/`](registry/stores/). Times are UTC, from
each manifest's `created` to its status's last heartbeat. The digest is the one `status.json` records at the end.

| Run | Start → end | Wall | State | Store digest at the end |
|---|---|---|---|---|
| `science-2026.14.0-A-exp-A-exp-lab-20261121T011745` | 21 01:17:45 → 04:33:50 | 3 h 16 min | done | `5e275a77b1e3610d…` |
| `science-2026.14.0-A-pl-A-pl-lab-20261121T043352` | 21 04:33:52 → 09:36:58 | 5 h 03 min | done | `63c3ce011ebce29b…` |
| `science-2026.14.0-B-exp-B-exp-lab-M-trend-20261121T093701` | 21 09:37:01 → 13:40:47 | 4 h 04 min | done | `5e40e17a9fcf90b4…` |
| `science-2026.14.0-B-pl-B-pl-lab-M-trend-20261121T134049` | 21 13:40:49 → 19:46:45 | 6 h 06 min | done | `a53c999b4e604949…` (power-law store, final) |
| `science-2026.14.0-D-exp-D-exp-large-M-natural-20261121T194647` | 21 19:46:47 → 20:09:54 | 23 min | done | `30d1309f90f96971…` |
| `science-2026.14.0-R-R-exp-lab-20261121T200955` | 21 20:09:55 → 20:12:34 | 3 min | done | `2a04e031bbacfdc1…` |
| `science-2026.14.0-R-R-pl-lab-20261121T201236` | 21 20:12:36 → 20:15:17 | 3 min | done | `1575391cca0c0bfd…` |
| `science-2026.14.0-R-R-exp-ladder-20261121T201519` | 21 20:15:19 → 20:29:44 | 14 min | done | `77bf03ad6dd5c2c0…` |
| `science-2026.14.0-R-R-pl-ladder-20261121T202946` | 21 20:29:46 → 20:32:06 | 2 min | done | `806d5481e8091dad…` |
| `science-2026.14.0-R-R-exp-natural-20261121T203207` | 21 20:32:08 → 20:33:36 | 1 min | done | `b37e19993bf1c0f7…` |
| `science-2026.14.0-C-exp-C-exp-Mcrit-20261121T203338` | 21 20:33:38 → 23:39:50 | 3 h 06 min | done | `485bcdea3b5346d4…` (exponential store, final) |
| `science-2026.14.0-C-exp-R-exp-Mcrit-20261121T233952` | 21 23:39:52 → 22 00:05:45 | 26 min | done | `e72b797e24c07067…` (reflection store, final) |

The first slot into each store (A-exp, A-pl, R-lab exp) records "fingerprint_sidecar: not written", since the script
creates each store's sidecar after its first block, as before; every later slot wrote it. After the run each
sidecar held its store's last digest, and a fresh read-only fingerprint of each store matched it.

## Timings against the plan

PLAN.md §6 projected about 37 h of building at 8 CPUs, assuming one Mac Studio CPU is as fast as one of the laptop's.
The probe (§3.1) raised the bound to about 51 h. **The build took 22 h 48 min**, and the inventories, exports,
measures, figure tools and `gzip` 2 h 57 min more, against §6's 2.5–4 h.

| Slot | 2026.11.0 (laptop, 10 CPUs) | This run (Mac Studio, 8 CPUs) |
|---|---|---|
| A-exp | 4 h 00 min | 3 h 16 min |
| A-pl (n = 1 and 2) | — | 5 h 03 min |
| B-exp | 4 h 09 min | 4 h 04 min |
| B-pl (n = 1 and 2) | — | 6 h 06 min |
| D-exp | 23 min | 23 min |
| R (lab, ladder, natural; both potentials) | 18 min | 23 min |
| C-exp | 3 h 13 min | 3 h 06 min |
| R-Mcrit | 25 min | 26 min |
| Exports, measures, figure tools, `gzip` (three walls) | 58 min (one wall) | 2 h 54 min (exp 61 min, pl1 54 min, pl2 59 min) |

The exponential's slots took no longer on 8 of the Mac Studio's CPUs than on 10 of the laptop's, and A was 18%
shorter. That is consistent with the probe's ratio of about 1.3 per CPU. The power law at both n took less than twice
the exponential's time for twice its histories (A 1.55×, B 1.50×).

## Failed histories

From the exports' `histories.csv`, by wall, block and law, split at the analytic QCD surfing threshold β = 1.1074, as
in 2026.11.0's table. The categories are the first words of each failure reason. The export's marker rows ("no
ScalarHistory row in the store") are not counted.

| Wall | Block | Law | β ≤ 1.1074 | of which failed | β > 1.1074 | of which failed |
|---|---|---|---|---|---|---|
| exp | A | fixed | 46 | 6: 3 effective minimum, 3 PRyMordial | 336 | 0 |
| exp | A | one-loop | 230 | 33: 16 effective minimum, 11 PRyMordial, 6 step budget | 1680 | 0 |
| exp | B | fixed | 60 | 36: 18 effective minimum, 18 PRyMordial | 300 | 0 |
| exp | B | one-loop | 300 | 200: 108 effective minimum, 71 PRyMordial, 21 step budget | 1500 | 0 |
| exp | C | fixed | 20 | 13: 6 effective minimum, 4 PRyMordial, 3 normalization | 100 | 0 |
| exp | C | one-loop | 10 | 7: 7 effective minimum | 50 | 2 normalization (10⁻¹⁵ and 10⁻¹² at β = 1.4) |
| exp | D | natural | 21 | 8: 7 PRyMordial, 1 PRyMordial output | 165 | 4 (M = 0.5, β = 8, 10, 15, 20), as before |
| pl1 | A | fixed | 46 | 6: 3 effective minimum, 3 PRyMordial | 336 | 6 (β = 10, 15, 20) |
| pl1 | A | one-loop | 230 | 33: 16 effective minimum, 11 PRyMordial, 6 step budget | 1680 | 30 (β = 10, 15, 20) |
| pl1 | B | fixed | 60 | 36: 18 effective minimum, 18 PRyMordial | 300 | 24 (β = 10, 20, every M) |
| pl1 | B | one-loop | 300 | 188: 114 PRyMordial, 63 effective minimum, 11 step budget | 1500 | 120 (β = 10, 20, every M) |
| pl2 | A | fixed | 46 | 6: 3 effective minimum, 3 PRyMordial | 336 | 0 |
| pl2 | A | one-loop | 230 | 33: 15 effective minimum, 15 PRyMordial, 3 step budget | 1680 | 0 |
| pl2 | B | fixed | 60 | 36: 18 effective minimum, 18 PRyMordial | 300 | 4 (β = 20 at 10⁻⁴⁰ and 10⁻⁴²) |
| pl2 | B | one-loop | 300 | 186: 121 PRyMordial, 55 effective minimum, 10 step budget | 1500 | 20 (β = 20 at 10⁻⁴⁰ and 10⁻⁴²) |
| — | R | both | 0 | 0 | 84 | 0 |

- **The exponential** fails as in 2026.11.0, except B's one-loop rows below the threshold: 200 failures against 195.
  The kinds there moved from PRyMordial's floor towards the effective minimum and the step budget (`changes/NOTES.md`,
  "Status"). Above the threshold its 6 failures are 2026.11.0's 6.
- **The power wall's failures above the threshold** are all at β ≥ 10, and all of one of two kinds:
  - `(ln V)''` overflows in `InversePowerModel.d2_logV_dphi2`, at φ ≈ 10⁻¹⁵⁶ to 10⁻¹⁹⁸ M_P;
  - the classical turning-point search leaves float range (n = 1) or returns a non-finite value (n = 2).

  The probe found the same at β = 20 (PLAN.md §3.1), and the user ruled to keep β = 20 on the grid. In this run
  β = 10 and 15 fail the same way at n = 1. The β = 10 failures leave `pl1` without a β = 10 history at any M
  (`m-trend/NOTES.md`).
- **Below the threshold** the power wall fails in the same window and by the same kinds as the exponential.

## The exports and the measures

The script exported each wall into its own root (P10) with both exporters, ran the measures at c_a = 1, 0 and 10,
drew the figure tools, and compressed each finished wall's `fields.csv` files with `gzip` (P11).

| Root | Measured (each c_a) | Failed histories | Missing, refused | K check fails | Figures |
|---|---|---|---|---|---|
| exp main | 3593 | 2560 (with marker rows) | 0, 0 | 17 | `quanta_grid` × 10 |
| exp reflections | 16 | 46 | 0, 0 | 2 | `quanta_history` × 16 |
| pl1 main | 3464 | 246 | 0, 0 | 0 | `quanta_grid` × 10 |
| pl1 reflections | 12 | 4 | 0, 0 | 0 | `quanta_history` × 12 |
| pl2 main | 3607 | 103 | 0, 0 | 0 | `quanta_grid` × 10 |
| pl2 reflections | 12 | 4 | 0, 0 | 0 | `quanta_history` × 12 |

The exponential's 19 K-check failures are at C, 10⁻¹² M_P, A = 1, and in R-Mcrit at 10⁻¹² M_P, the class 2026.10.0
found and still undiagnosed. The power wall has none.

## Where the output is

Under `var/science-2026.14.0/out/` on the Mac Studio, which git ignores, one root per wall: `exp/` 12 GB,
`pl1/` 11 GB and `pl2/` 12 GB, each with a reflections root (296, 132 and 132 MB). Each holds:
- `histories.csv` under `QCD_Cosmology/<model>/`, with its `model` and `n` columns;
- the per-(M, row) figures and `data.csv`;
- each one-loop history's `fields.csv.gz` and `bounces.csv`;
- the measures' files and the figure tools' output.

Only what the analyses read was copied here, byte for byte, with SHA-256 checked against the source (`bc54c13`):
`fig6/inputs/`, `relativistic/inputs/` and `m-trend/inputs/`. M is in eV in the folder names, as before
(9.7413×10⁻⁴ eV is 4×10⁻³¹ M_P; 2026.11.0's README lists the rest).

## What stays open

- **Testing the power walls' ln R extrapolation.** The law f_n(ln R) was fitted on stage 2 of `power-law-wall` 02
  (`.documents/power-law-wall/wall-02/stage2_scan.txt`). That stage solves the bounce and its one-loop modes in units
  of the turning point, where the fraction depends on ln R and n alone. A test means stage-2 points at the ln R this
  run reaches, ln R ≈ 555–750 at n = 1 and 345–500 at n = 2, compared with `drain_fraction`:
  - the scan's script, `prompts/power-law-wall/source/wall/scan_curves.py`, does this with no Ray and no store;
  - its whole scan, about 200 points on six processes, took 315 s, and a point at ln R ≈ 545 took 12 s;
  - a dozen new points would take minutes on the laptop.

  At the top of the measured range the law is already 4–5% below the measured f at n = 1 (0.0513 against 0.0538 at
  ln R = 545.6) and 2% at n = 2. Only if a test moved the law would the power-law store need rebuilding: A-pl and B-pl,
  11 h on the Mac Studio at 8 CPUs, with about 2 h of exports.

  **Run later on 2026-11-22** ([`lnR-test/`](lnR-test/NOTES.md)), at the user's request ("run the ln R test"):
  - it reaches ln R = 700 at n = 1 and 500 at n = 2. That covers every first impact of the run at n = 2, and all but
    50 at n = 1 (β = 3.0–5.0 at 10⁻⁴⁰ and 10⁻⁴² M_P, up to 740, beyond the stage-2 code's float range);
  - the law stays below stage 2 by the offset it had at the top of the measured range: 4.6–4.8% at n = 1 and 2.4–2.7%
    at n = 2;
  - the extrapolation adds nothing new, and nothing was rebuilt.

  Whether to refit the law to remove the offset is the user's to decide.
- **As 2026.11.0 left them:**
  - `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`;
  - the one-loop tuning cap at M_crit;
  - the K check at C, 10⁻¹² M_P;
  - M below 10⁻⁴² (deferred by the user, 2026-11-09);
  - and PLAN.md §8's list.

## Where each file came from, and how that is known

- **`PLAN.md`** was written by a Claude Code session on 2026-11-19 at the user's request, in
  `science-runs/plan-science-2026.14.0/`. The user's rulings were added to §0.2 in `5482e13`, the probe's findings to
  §3.1 in `e1e6964`, `e65e635` and `2d151ba`, and the folder was renamed here in `a916149`, the tree every block ran
  on. The rulings in §0.1 and §0.2 are the user's, in their words. PLAN.md was not edited after `a916149`.
- **`run_science_2026.14.0.sh`** was written by the session on 2026-11-20 and committed in `c147a2d`. It was not edited
  after that commit.
- **`probe/`** holds the probe's scripts (`fb79f5a`, `075bbd1`), its logs and registry records from the Mac Studio, and
  the scripts behind §3.1's counts. PLAN.md §3.1 says what it found.
- **The run was launched by a Claude Code session** at the user's word, on 2026-11-21 at 01:17 UTC. It ran on the Mac
  Studio over SSH, with `nohup`, from that machine's checkout at `a916149`, after `tools/ray_cluster.sh start` took the
  rehearsal lock with 8 CPUs. The session stopped the cluster with `tools/ray_cluster.sh stop` after the script ended
  on 2026-11-22, and the lock was released.
- **`registry/`** was copied unchanged from the Mac Studio's `var/runs/<run>/` and the stores' sidecars on 2026-11-22,
  and committed in `bc54c13` with the analyses' inputs.
- **`logs/`** was copied unchanged from the Mac Studio's `var/science-2026.14.0/logs/` on 2026-11-22, after the
  analyses, by the session writing this README. The SHA-256 of all 52 files equals the source's. The logs are each
  slot's output as the script `tee`'d it, the four inventories, the exports, the measures and the figure tools. The
  script's own output (`run.out`) and the cluster's start log stay on the Mac Studio.
- **The analysis folders** were ported on 2026-11-20 (`e1e6964`) and run on 2026-11-22 (`6bc8eed`, `4ec8e68`,
  `5c08a3f`, `c79446a`, `dcbce18`), with notes reviewed by the user (`369dd44`). Each NOTES.md says what was changed
  and why.
- **This README** was written by the session on 2026-11-22 at the user's request ("Write the run README and the
  science-runs/README row"). It draws on the registry records, the Mac Studio's logs and `du`, and the committed
  `histories.csv` files.
