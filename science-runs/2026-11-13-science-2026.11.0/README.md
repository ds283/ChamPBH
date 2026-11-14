# 2026-11-13 — the `2026.11.0` science run

The rebuild of [`2026.10.0`](../2026-11-08-science-2026.10.0/README.md) on the w table that follows Laine & Meyer
through the electroweak crossover, which `electroweak-eos` installed as `VERSION_LABEL` `2026.11.0`. Its grid is
2026.10.0's, except that the dense β grid's 0.005 segment was moved to 2.950–3.040 to resolve the new QCD-capture edge
at 2.975. Its plan is [`PLAN.md`](PLAN.md): §0.1 and §0.2 hold the user's rulings in their words, §1 the grid, §2 the
launch and §3 the analyses.

**Status: current.** Every block completed at the first attempt, and both stores are whole and validated. 304 of the
main store's 4818 histories failed, 6 of them above the QCD surfing threshold, all of a kind diagnosed in 2026.10.0
(below, "Failed histories"). None of the reflection store's 44 failed.

**Figures made from it:** none yet. PLAN.md §3 lists them.

## Standard analyses

From `science-runs/README.md`'s list. The run script made every input they read (PLAN.md P3).
- **Figure 6:** not yet made.
- **Relativistic quanta:** not yet made.

## The run

| | |
|---|---|
| When | 2026-11-13 20:42:44 UTC – 2026-11-14 10:08:52 UTC (the build to 09:10:52, then the inventories, exports and measures) |
| Tree | `a19d71e` for every block, `git_dirty` false in every manifest, `main.py`'s SHA-256 `7b93fae4…` |
| Labels | `VERSION_LABEL` `2026.11.0`, `FINGERPRINT_FORMAT` 7, `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (small network); one-loop rows under `one-loop-measured-v1` |
| Packages | datastorekit 0.4.0, raypoolkit 0.3.0, runregistrykit 0.2.0, ray 2.59.0, SQLAlchemy 2.0.55, numpy 2.3.5, scipy 1.17.0; Python 3.13.16 |
| Host | `mps026088.phys.susx.ac.uk` (arm64, macOS, 10 cores), ChamPBH's isolated Ray cluster at 127.0.0.1:6380, started and stopped with `tools/ray_cluster.sh` |
| Main store | `var/datastores/science-2026.11.0.db`, 16 shards, 8.9 GB: 4818 histories (A + B + C + D), 90 001 records in 16 classes, inventory "problems: none" |
| Reflection store | `var/datastores/science-2026.11.0-reflections.db`, 16 shards, 183 MB: 44 histories (R), 75 180 records, "problems: none" |
| Script | [`run_science_2026.11.0.sh`](run_science_2026.11.0.sh): the build, the inventories, the exports and the derived measures |
| Output | `var/science-2026.11.0/out/`, 19 GB (below, "Where the output is") |
| Disk | 172 GB free at the start; the script's gate (55 GB for a block, 70 GB for the exports) was never reached, the least being 154 GB before D |

Every history starts at φ* = 5 M_P, with n = 1, T* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, `--tuning-max-integrations`
30, atol = rtol = 10⁻⁸, 16 shards and `--bbn-wall-clock-limit 600`. The blocks, from PLAN.md §1, in the order they
ran:

| Block | M / M_P | β grid | fixed α | one-loop A | Histories |
|---|---|---|---|---|---|
| A | 4×10⁻³¹ | dense, 191 values, 0.5–20 | 0.01, 0.1 | 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10 | 2292 |
| B | 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰, 10⁻⁴² | sparse, 30 values | 0.01, 0.1 | the ten | 2160 |
| D | 10⁻³, 0.1, 0.5 | coarse, 62 values | 0 (natural mode) | — | 186 |
| R-lab | 4×10⁻³¹ | 1.2, 2.0, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 |
| R-ladder | 10⁻¹⁶ | 2.0 | 0.01, 0.1 | 0.1, 1, 10 | 5 |
| R-natural | 10⁻³, 0.5 | 1.2, 2.0, 2.9 | 0 | — | 6 |
| C | 10⁻¹⁵, 10⁻¹² | sparse, 30 values | 0.01, 0.1 | 1 | 180 |
| R-Mcrit | 10⁻¹⁵, 10⁻¹² | 1.2, 2.0, 2.9 | 0.01, 0.1 | 1 | 18 |

The dense grid differs from 2026.10.0's only by the five β 2.950, 2.955, 2.965, 2.970 and 2.975 (PLAN.md §0.1), so
block A has 60 histories more; every other block is 2026.10.0's.

## The registered runs

Each block ran through `RunDrivers/registered_run.py` as `--campaign science-2026.11.0 --prompt <block>`. Their
`manifest.json` and `status.json` are copied unchanged into [`registry/runs/`](registry/runs/), and the two stores'
fingerprint sidecars, as they stood after the run, into [`registry/stores/`](registry/stores/). Times are UTC, from
each manifest's `created` to its status's last heartbeat; the digest is the one `status.json` records at the end.

| Run | Start → end | Wall | State | Store digest at the end |
|---|---|---|---|---|
| `science-2026.11.0-A-A-lab-20261113T204245` | 13 20:42:45 → 14 00:42:17 | 4 h 00 min | done | `62e4605d04c1c8fb…` |
| `science-2026.11.0-B-B-lab-M-trend-20261114T004219` | 14 00:42:19 → 04:51:00 | 4 h 09 min | done | `6aed85e6b9e014f4…` |
| `science-2026.11.0-D-D-large-M-natural-20261114T045102` | 14 04:51:02 → 05:14:12 | 23 min | done | `d05bf2bd498a2786…` |
| `science-2026.11.0-R-R-lab-20261114T051414` | 14 05:14:14 → 05:16:56 | 3 min | done | `d0bc61dbfcb8d687…` |
| `science-2026.11.0-R-R-ladder-20261114T051657` | 14 05:16:57 → 05:30:45 | 14 min | done | `b22b6b60bda4e9b3…` |
| `science-2026.11.0-R-R-natural-20261114T053046` | 14 05:30:46 → 05:32:15 | 1 min | done | `84412a9e21bc9aaf…` |
| `science-2026.11.0-C-C-Mcrit-20261114T053216` | 14 05:32:16 → 08:45:26 | 3 h 13 min | done | `54cb9ac49d648fe0…` (main store, final) |
| `science-2026.11.0-R-R-Mcrit-20261114T084528` | 14 08:45:28 → 09:10:52 | 25 min | done | `026cd7901e41f77c…` (reflection store, final) |

The first block into each store (A, R-lab) records "fingerprint_sidecar: not written", since the script creates each
store's sidecar after its first block (`runregistrykit store create`), as in 2026.10.0; every later block wrote it.

## Timings against the plan

PLAN.md §4 projected about 17–18 h for the build, of which about 8 h for slot C. The build took 12 h 28 min, and the
inventories, exports, measures and figure tools 58 min more.

| Slot | 2026.10.0 | Plan | Took |
|---|---|---|---|
| A | 3 h 34 min | 2026.10.0's + 2.6% | 4 h 00 min |
| B | 4 h 20 min in two parts | (as 2026.10.0) | 4 h 09 min |
| D | 24 min | (as 2026.10.0) | 23 min |
| R (lab, ladder, natural) | 18 min | (as 2026.10.0) | 18 min |
| C | 4 h 22 min | about 8 h with R-Mcrit | 3 h 13 min |
| R-Mcrit | 3 h 38 min | (with C) | 25 min |
| Inventories, exports, measures, figure tools | 8 min + 50 min | — | 58 min |

- **A** took 26 min more than in 2026.10.0, for 2.6% more histories, on a machine shared for the first part with a
  test suite in another worktree (it used no Ray and no lock).
- **R-Mcrit** took 25 min where 2026.10.0's took 3 h 38 min. That run's was set by one history, 10⁻¹⁵ M_P, β = 1.2,
  A = 1, which tuned alone to the 30-integration cap and failed; here it converged (below, "Failed histories"). C
  was also shorter, by 1 h 09 min, and its β ≈ 1.1–1.4 one-loop histories at M_crit still set its length: 10⁻¹² M_P,
  β = 1.3 reached its 27th integration before it converged.

## Failed histories

From the exports' `histories.csv`, by block and law, split at the analytic QCD surfing threshold β = 1.1074, as in
2026.10.0's table. The categories are the first words of each `failure_reasons`; the export's marker rows ("no
ScalarHistory row in the store", for a (row, M, β) the stored grid does not hold) are not counted.

| Block | Law | β ≤ 1.1074 | of which failed | β > 1.1074 | of which failed |
|---|---|---|---|---|---|
| A | fixed | 46 | 6: 3 PRyMordial, 3 effective minimum | 336 | 0 |
| A | one-loop | 230 | 33: 11 PRyMordial, 16 effective minimum, 6 step budget | 1680 | 0 |
| B | fixed | 60 | 36: 18 PRyMordial, 18 effective minimum | 300 | 0 |
| B | one-loop | 300 | 195: 102 PRyMordial, 78 effective minimum, 15 step budget | 1500 | 0 |
| C | fixed | 20 | 13: 4 PRyMordial, 6 effective minimum, 3 normalization | 100 | 0 |
| C | one-loop | 10 | 7: 7 effective minimum | 50 | 2 normalization (10⁻¹⁵ β = 1.4; 10⁻¹² β = 1.4) |
| D | natural | 21 | 8: 7 PRyMordial, 1 PRyMordial output | 165 | 4: 3 normalization (M = 0.5, β = 8, 10, 15), 1 effective minimum (M = 0.5, β = 20) |
| R | both | 0 | 0 | 44 | 0 |

Against 2026.10.0 (its README, "Failed histories"), blocks A, B and D fail in the same numbers and categories, and
A's 60 new histories, all above the threshold, do not fail. The changes are at M_crit, all in "the normalization did
not converge", the 30-integration tuning cap:
- C's fixed law has three such failures below the threshold where 2026.10.0 had two;
- C's one-loop law has none below the threshold where 2026.10.0 had one, and above it fails at 10⁻¹⁵ β = 1.4 where
  2026.10.0 failed at 10⁻¹⁵ β = 1.2 (10⁻¹² β = 1.4 fails in both);
- R-Mcrit's 10⁻¹⁵ β = 1.2, A = 1, which failed in 2026.10.0, converged.

The kinds are those of 2026.10.0, whose README says what each is: PRyMordial's spline floor at β = 0.5–0.9, the
non-surfing region's "effective minimum" and "step budget" failures
(`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`), the tuning cap at M_crit (undiagnosed), and D's
four at M = 0.5, the same four as in 2026.8.0 and 2026.10.0. Which histories sit at the tuning cap near M_crit moves
when the w table changes; the cap's cause is still open (PLAN.md §5).

## The exports and the measures

The script gave each store's export root both exporters (PLAN.md P3), so the measures ran on the first attempt.

- **The measures:** 3629 one-loop histories of the main store and 16 of the reflection store measured at each
  associated scale (1, 0 and 10), none refused, none missing (2026.10.0: 3577 and 15). The tool's "failed histories"
  count (2524 and 46) includes the export's marker rows.
- **The K check fails for 17 histories of the main store and 2 of the reflection store, and only there:** block C at
  10⁻¹² M_P, A = 1, β = 1.15, 1.25 and 1.5–2.9, and R-Mcrit's 10⁻¹² β = 2.0 and 2.9. The largest relative difference
  between the dark-radiation energy rebuilt from the bounce records and the per-sample value is 1.3×10⁻⁵ (R-Mcrit,
  β = 2.0); every other history passes at about 10⁻¹⁴. It is the class 2026.10.0 found (there 18 and 2, with β = 3.0,
  which passes here), still **undiagnosed**, and it is the measures' exit status 1, which the script reports and passes
  over.
- **The figure tools** wrote `quanta_grid.{pdf,png}` for each of the ten A of the main store and 16 per-history
  figures of the reflection store, none refused.

## Where the output is

Under `var/science-2026.11.0/out/`, which git ignores; nothing there is copied here.

- **`main/QCD_Cosmology/`**: `histories.csv` (every row of the main store, both laws, with the failure reasons and the
  one-loop drain columns), from `plot_by_beta.py` at α = 0, 0.01, 0.1 and every A; the figures and `data.csv` of each
  (M, row) under `alpha=<α>/` and `one-loop-measured-v1/A=<A>/`; each one-loop history's `fields.csv` and
  `bounces.csv` under `one-loop-measured-v1/A=<A>/csv/beta=<β>/M=<M>eV_n=1/`, from `plot_ScalarHistory.py --no-plots`;
  the measures' `quanta_histories.csv` (and `-assoc0`, `-assoc10`), with each history's `quanta_bounces*.csv` and
  `quanta_samples*.csv`; and `one-loop-measured-v1/A=<A>/plots/quanta_grid.{pdf,png}`.
- **`reflections/QCD_Cosmology/`**: every history's figures and CSV files from `plot_ScalarHistory.py`, `histories.csv`
  and its per-(M, row) figures from `plot_by_beta.py`, the measures, and the per-history figures of
  `tools/plot_quanta_history.py`.

M is in eV in the folder names, as in 2026.10.0: 9.7413×10⁻⁴ eV is 4×10⁻³¹ M_P; 2.4353×10⁻³, 10⁻⁵, 10⁻⁶, 10⁻⁹,
10⁻¹³ and 10⁻¹⁵ eV are 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰ and 10⁻⁴² M_P; 2.4353×10¹¹, 10¹² and 10¹⁵ eV are 10⁻¹⁶,
10⁻¹⁵ and 10⁻¹² M_P; 2.4353×10²⁴, 10²⁶ and 1.2177×10²⁷ eV are 10⁻³, 0.1 and 0.5 M_P.

## Where each file came from, and how that is known

- **`PLAN.md` and `run_science_2026.11.0.sh`** were written by a Claude Code session on 2026-11-13 at the user's
  request, and committed in `a7e0978` in `science-runs/plan-science-2026.11.0/`. The folder was moved here in
  `98cfb31`, and the user's ruling of P1–P6 added to PLAN.md §0.2 in `a19d71e`, the tree every block ran on. The
  rulings in PLAN.md §0.1 and §0.2 are the user's, in their words. **The script was not edited after it was
  committed**, and PLAN.md not after `a19d71e`.
- **The run was launched by a Claude Code session** (the run's orchestrator) at the user's word ("P1–P6 as proposed,
  launch it"), on 2026-11-13 at 20:42, with `nohup` from the repository root. PLAN.md §2 step 4 had the user start the
  cluster; at that word the session started it itself, with `tools/ray_cluster.sh start "science-2026.11.0"` from the
  repository root, which took the rehearsal lock. Before the launch the space held after the retire of `science-2026.8.0` had come back (`df`:
  170 GB free), and `runregistrykit list` showed nothing running or stale. The session stopped the cluster with
  `tools/ray_cluster.sh stop` on 2026-11-14 at 11:05, after the script ended, and the lock was released.
- **`logs/`** is copied unchanged from `var/science-2026.11.0/logs/`: each block's output as the script `tee`'d it,
  the two inventories, the exports, the measures and the figure tools. The script's own output,
  `var/science-2026.11.0/run.out`, repeats those logs line for line and is not copied. Each run's own copy of its
  output is also kept in `var/runs/<run>/stdout.log`.
- **`registry/`** is copied unchanged from `var/runs/<run>/` and from the stores' sidecars in `var/datastores/`, on
  2026-11-14 after the run.
- **This README** was written by the session on 2026-11-14 at the user's request, from those files and the exports'
  `histories.csv`.
