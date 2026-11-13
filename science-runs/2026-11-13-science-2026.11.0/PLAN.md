# Plan — the `2026.11.0` science run

**Drafted 2026-11-13**, on `main` at `84a2494`, after `electroweak-eos` closed and was merged. It rebuilds the
`2026-11-08` run of `2026.10.0` ([`2026-11-08-science-2026.10.0/`](../2026-11-08-science-2026.10.0/README.md)) on the
w table that follows Laine & Meyer through the electroweak crossover, which `electroweak-eos` installed as `2026.11.0`.
Its grid is that run's (its PLAN.md §8.2) with one change (§0.1). This folder holds the plan and the run script. When
the run starts, its files move into the run's own folder, `YYYY-MM-DD-science-2026.11.0/`. Until then it is a plan:
nothing has been launched.

The tree: `VERSION_LABEL` `2026.11.0` (`config/version.py:100`), `FINGERPRINT_FORMAT` 7 (`config/fingerprint.py:95`),
`PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (`ComputeTargets/BBNData.py:47`). A 2026.10.0 store "is not extended at
2026.11.0" (`config/version.py`), so the run starts new stores; this tree reads the 2026.10.0 stores read-only
(review-remediation verification §4.21 point 1), and they are kept as the record of that run.

## 0. Rulings and proposals

### 0.1 Ruled by the user, 2026-11-13, in their words

- **The run.** "First, a rebuild of the `2026.10.0` science store (and its data products) using the new
  thermodynamics implementation." The data products are the four analyses of 2026.10.0 (§3).
- **The grid.** "Rebuild: move the fine segment, so we resolve the edge". The dense β grid's 0.005 segment was
  2.980–3.040, made for the old uncaptured window 2.999–3.019. On the new table the largest β QCD captures is 2.975 and
  the window is 2.976–3.002 (at M = 10⁻³ M_P, tuned; review-remediation verification §4.21 point 2), so the edge at
  2.975 lay between the 0.02 grid's 2.96 and 2.98. The segment becomes 2.950–3.040, which adds 2.950, 2.955, 2.965,
  2.970 and 2.975 (2.96 and 2.98 were already in the grid): 191 dense β in place of 186. At laboratory M, A = 1, the
  delivery temperature jumps between 2.980 and 2.985 on the new table (the same point 2), which the segment covers too.
- **The disk.** "Disk: yes, retire `science-2026.8.0`. I'll thin the snapshots, but right before we launch."
  - The store was retired on 2026-11-13 with `runregistrykit store retire --without-fingerprint`. This tree cannot read
    it (schema of 2026.8.0, `StoreSchemaMismatch`), so no fresh fingerprint could be taken. The sidecar
    `var/datastores/science-2026.8.0.manifest.json` keeps the fingerprint recorded when the store was closed, and the
    reason, as a tombstone.
  - Its 6.5 GB had not yet come back after the retire (`df`: 78 GB free, 92% used), presumably held by local snapshots
    until they are thinned. Its exports (`var/science-2026.8.0/`, 389 MB) and its two small stores (`-reflections`,
    `-large-M-dissipation`, 265 MB together) are untouched.

### 0.2 Proposed, then ruled as proposed

Ruled by the user, 2026-11-13, in their words: "P1–P6 as proposed, launch it". Before the launch `df -h .` showed
170 GB free (82% used).

- **P1, the grid otherwise unchanged.** Every block, M, α row, A row, β grid other than the dense one, φ\*, tolerance
  and option is 2026.10.0's (§1), so every history of that run has a counterpart here and the comparison of P5 is one
  join.
- **P2, no probe.** `electroweak-eos` 02 built 48 of 2026.10.0's histories on this tree and on the old one (verification
  §2.4), with no failure on either side, and measured the threshold (§2.2). The only new β are five points in a range
  2026.10.0 already built at 0.02 spacing.
- **P3, one script.** The build, the inventories, every export and the derived measures run from one script,
  `run_science_2026.11.0.sh`. It folds in 2026.10.0's post-build script (`run_postbuild_2026.10.0.sh`), which was
  written after that run's measures step failed because each export root had only one exporter (its README, "The
  post-build steps").
- **P4, a disk gate.** Ray stops writing at 95% of the disk (about 46 GB free on this machine's 927 GB); B of 2026.10.0
  failed there (its README, "B's disk failure and the resume"). The script refuses to start a block with less than
  `MIN_FREE_GB_BUILD` (default 55) free, and the exports with less than `MIN_FREE_GB_EXPORT` (default 70). §2 sets the
  launch condition.
- **P5, a comparison with 2026.10.0.** Besides the four analyses, a short `changes/` analysis joins this run's
  `histories.csv` with 2026.10.0's on (M, β, row) and reports what moved: which histories change capture class, and the
  shifts in T_deliver, Y_p, D/H and ΔN_eff, beside `electroweak-eos` verification §2.4's 48. It sets no threshold.
- **P6, as 2026.10.0** (its PLAN.md §0.3 P4): no Datastore tags for purpose; one main store and one reflection store;
  every `main.py` block registered as `--campaign science-2026.11.0 --prompt <block>`; the run script committed in the
  run's folder before launch and never edited afterwards.

## 1. What is sampled

Every history has φ\* = 5 M_P, n = 1, T\* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, `--tuning-max-integrations` 30,
atol = rtol = 10⁻⁸, 16 shards and `--bbn-wall-clock-limit 600`. The one-loop rows are under `one-loop-measured-v1`.

**A** (one-loop): 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10. **α** (fixed law): 0.01 and 0.1 on every block near
laboratory M or M_crit; 0 (natural mode) on D and `R-natural` only.

**β**, generated as `i/d` from integers by the script's `G`, as 2026.10.0's:

- **Dense, 191 values**: NS (0.5, 0.75, 0.9, 1.0); W (1.0600–1.2000 by 0.0025, 1.20–1.50 by 0.01); 1.50–3.00 by 0.02;
  **2.950–3.040 by 0.005** (§0.1); the tail 3.25, 3.5, 4, 5, 6, 8, 10, 15, 20.
- **Sparse, 30 values**: NS; 1.10–3.00 by 0.10; 1.15, 1.25; 3.5, 5, 10, 20.
- **Coarse, 62 values**: NS; 1.10–3.00 by 0.05; 1.08–1.20 by 0.01; the tail.

| Order | Block | Store | M / M_P | β | α | A | Histories |
|---|---|---|---|---|---|---|---|
| 1 | `A-lab` | main | 4e-31 | dense (191) | 0.01, 0.1 | all 10 | 2292 |
| 2 | `B-lab-M-trend` | main | 1e-30, 1e-32, 1e-33, 1e-36, 1e-40, 1e-42 | sparse (30) | 0.01, 0.1 | all 10 | 2160 |
| 3 | `D-large-M-natural` | main | 1e-3, 0.1, 0.5 | coarse (62) | 0 | — | 186 |
| 4 | `R-lab` | reflections | 4e-31 | 1.2, 2, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 |
| 4 | `R-ladder` | reflections | 1e-16 | 2 | 0.01, 0.1 | 0.1, 1, 10 | 5 |
| 4 | `R-natural` | reflections | 1e-3, 0.5 | 1.2, 2, 2.9 | 0 | — | 6 |
| 5 | `C-Mcrit` | main | 1e-15, 1e-12 | sparse (30) | 0.01, 0.1 | 1 | 180 |
| 5 | `R-Mcrit` | reflections | 1e-15, 1e-12 | 1.2, 2, 2.9 | 0.01, 0.1 | 1 | 18 |

The main store, `var/datastores/science-2026.11.0.db`, holds 4818 histories (3770 one-loop); the reflection store,
`var/datastores/science-2026.11.0-reflections.db`, 44. Slot 5 runs last, as in 2026.10.0, since its β ≈ 1.1–1.4
histories at M_crit tune to the 30-integration cap.

## 2. How the run is launched and recorded

In this order, from the repository root:

1. **The plan and the script are committed** at the user's word, and this folder is moved to
   `science-runs/YYYY-MM-DD-science-2026.11.0/` (the launch date) in a commit of its own. The script refuses a dirty
   tree.
2. **The user thins the local snapshots** and checks `df -h .`. The launch needs about **90 GB free**: about 9 GB of
   stores and 18 GB of exports (2026.10.0's: 8.9 GB, 178 MB and 18 GB, almost all of it the one-loop rows' per-history
   CSV files), above Ray's 46 GB floor, with margin.
3. **Discovery**: `./venv/bin/python -m runregistrykit list` shows nothing `running` or stale.
4. **The cluster**, started by the user from the repository root (Ray workers take the cwd of the `start`, and
   `Xav_EOS_spline` and PRyMordial read their data by relative paths; `electroweak-eos` board §3,
   `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`):
   `tools/ray_cluster.sh start "science-2026.11.0"`. Exit 3 is a stop for the user.
5. **The launch**, detached:
   ```bash
   mkdir -p var/science-2026.11.0
   nohup bash science-runs/<folder>/run_science_2026.11.0.sh > var/science-2026.11.0/run.out 2>&1 &
   ```
   then one check that `A-lab` started (`runregistrykit list`), and nothing more until it ends. A slot-5 pipeline can
   be silent for longer than the registry's 900 s window: read it with `runregistrykit list --stale-after 4000`.
6. **While it runs**, nothing changes the main checkout's files or branch: the cluster's workers import from it.
   Campaign work goes in a worktree, and nothing else takes the rehearsal lock.
7. **Afterwards**: `tools/ray_cluster.sh stop` once the analyses that need Ray are done (none of §3's does; the
   exports are in the script), the registry's manifests copied into `registry/runs/`, the stores' sidecars into
   `registry/stores/`, the README written, and the run's row added to `science-runs/README.md`, with 2026.10.0 marked
   superseded.

## 3. After the build

The script makes every input these read: `histories.csv` of each store (`plot_by_beta.py`), each one-loop history's
`fields.csv` and `bounces.csv` in the same root (`plot_ScalarHistory.py --no-plots`), and the measures
(`tools/quanta_measures.py` at c_a = 1, 0 and 10). Each analysis is repeated from 2026.10.0's folder as its NOTES.md
says, with that folder's rulings and no new choices, and gets a new NOTES.md:

| Analysis | Kind | From |
|---|---|---|
| Figure 6 | standard | [`fig6/`](../2026-11-08-science-2026.10.0/fig6/NOTES.md) |
| Relativistic quanta | standard | [`relativistic/`](../2026-11-08-science-2026.10.0/relativistic/NOTES.md) |
| Figure 6b, around M_crit | this grid's | [`fig6b/`](../2026-11-08-science-2026.10.0/fig6b/NOTES.md) |
| The trend in M | this grid's | [`m-trend/`](../2026-11-08-science-2026.10.0/m-trend/NOTES.md) |
| What moved since 2026.10.0 | P5 | new |

The run's README has a section "Standard analyses" saying, for each, where it was made.

## 4. The cost — a projection, not a measurement

2026.10.0 took 16 h 45 min to build (A 3 h 34 min; B 4 h 20 min in two parts; D 24 min; R 18 min; C 4 h 22 min;
R-Mcrit 3 h 38 min), 8 min of inventories and exports, and 50 min of post-build steps, at loads up to 20 on this
machine's ten cores. This run adds 60 histories to A (2.6%) and moves no other block, so **about 17–18 h**, of which
slot 5 is about 8 h, set by the β ≈ 1.1–1.4 histories at M_crit that tune to the cap. The EW-captured histories at
β 2.985–3.0 (13 in 2026.10.0's grid, verification §2.4.5) are a different kind of history but too few to move the total.

## 5. What stays open

As 2026.10.0's README leaves them, none acted on here:

- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`**: β = 1.0 at small A, and its analogues at lower M.
- **The one-loop tuning cap at M_crit**: the Hubble residual stays near 10⁻⁴; undiagnosed.
- **The K check at C, 10⁻¹² M_P, A = 1** (18 histories): undiagnosed.
- **`tools/plot_quanta_grid.py`'s categorical β axis** (2026.10.0 `relativistic/PLAN.md` §4.3, Q4).
- **M below 10⁻⁴²**: deferred by the user (2026-11-09).
