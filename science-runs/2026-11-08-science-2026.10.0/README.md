# 2026-11-08 — the `2026.10.0` science run

The rebuild of record after `one-loop-drain`, which replaces [`2026.8.0`](../2026-11-01-science-2026.8.0/README.md)
under the one-loop drain law, with the fixed drain fraction α kept beside it. Its plan is [`PLAN.md`](PLAN.md): §0.2
and §8.1 hold the user's rulings in their words, §7 the probe that set the lowest M, and §8 the grid that ran.

**Status: current.** Every block completed and both stores are whole and validated. 304 of the main store's 4758
histories failed, 6 of them above the QCD surfing threshold, all of a diagnosed kind (below, "Failed histories").
One of the reflection store's 44 failed, of the same kind.

**Figures made from it** (each folder holds its script, its inputs and a note):
- [`fig6/`](fig6/NOTES.md): Figure 6 at the laboratory mass under the one-loop law, a band over A = 0.05–3,
  A = 1, 3, 5 and 10 and the fixed law at α = 0.01 and 0.1 as lines, and Planck's one-tailed ΔN_eff < 0.30
  (2026-11-09);
- [`m-trend/`](m-trend/NOTES.md): the trend in M from 10⁻³⁰ to 10⁻⁴² M_P, with the law's turnaround logarithm ℓ
  and its maximum (2026-11-09);
- [`fig6b/`](fig6b/NOTES.md): Figure 6 around M_crit, where the one-loop law's drain switches off (2026-11-09).

Each figure's input is `fig6/inputs/histories.csv`, an unchanged copy of the run's export of the main store, and
each cites the main store's sidecar in [`registry/stores/`](registry/stores/).

## The run

| | |
|---|---|
| When | 2026-11-08 09:40:12 UTC – 2026-11-09 03:31 UTC, with a gap of 1 h 06 min after B's disk failure (16:23–17:29); the post-build steps 2026-11-09 09:36:59 – 10:27:11 UTC |
| Tree | `5837a47` for every block, `git_dirty` false in every manifest; `4ade10b` for the post-build steps |
| Labels | `VERSION_LABEL` `2026.10.0`, `FINGERPRINT_FORMAT` 7, `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (small network); one-loop rows under `one-loop-measured-v1` |
| Packages | datastorekit 0.4.0, raypoolkit 0.3.0, runregistrykit 0.2.0, ray 2.59.0, SQLAlchemy 2.0.55, numpy 2.3.5, scipy 1.17.0; Python 3.13.16 |
| Host | `mps026088.phys.susx.ac.uk` (arm64, macOS, 10 cores), ChamPBH's isolated Ray cluster at 127.0.0.1:6380, started and stopped with `tools/ray_cluster.sh` |
| Main store | `var/datastores/science-2026.10.0.db`, 16 shards: 4758 histories (A + B + C + D), 89 812 records in 16 classes, inventory "problems: none" |
| Reflection store | `var/datastores/science-2026.10.0-reflections.db`, 16 shards: 44 histories (R), 75 178 records, "problems: none" |
| Scripts | [`run_science_2026.10.0.sh`](run_science_2026.10.0.sh) (the build), [`run_postbuild_2026.10.0.sh`](run_postbuild_2026.10.0.sh) (the exports and derived measures) |
| Output | `var/science-2026.10.0/out/` (below, "Where the output is") |

Every history starts at φ* = 5 M_P with `--bbn-wall-clock-limit 600`. The blocks, from PLAN.md §8.2, in the order
they ran (C last, at the user's ruling):

| Block | M / M_P | β grid | fixed α | one-loop A | Histories |
|---|---|---|---|---|---|
| A | 4×10⁻³¹ | dense, 186 values, 0.5–20 | 0.01, 0.1 | 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10 | 2232 |
| B | 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰, 10⁻⁴² | sparse, 30 values | 0.01, 0.1 | the ten | 2160 |
| D | 10⁻³, 0.1, 0.5 | coarse, 62 values | 0 (natural mode) | — | 186 |
| R-lab | 4×10⁻³¹ | 1.2, 2.0, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 |
| R-ladder | 10⁻¹⁶ | 2.0 | 0.01, 0.1 | 0.1, 1, 10 | 5 |
| R-natural | 10⁻³, 0.5 | 1.2, 2.0, 2.9 | 0 | — | 6 |
| C | 10⁻¹⁵, 10⁻¹² | sparse, 30 values | 0.01, 0.1 | 1 | 180 |
| R-Mcrit | 10⁻¹⁵, 10⁻¹² | 1.2, 2.0, 2.9 | 0.01, 0.1 | 1 | 18 |

Natural mode appears only at large M, as the user ruled; every block near the laboratory mass or M_crit names α =
0.01 and 0.1 so that it builds none (PLAN.md §0.2; `--no-alpha-values`, which would have allowed a block to name no α,
was merged later, `f8f185a`).

## The registered runs

Each block ran through `RunDrivers/registered_run.py` as `--campaign science-2026.10.0 --prompt <block>`. Their
`manifest.json` and `status.json` are copied unchanged into [`registry/runs/`](registry/runs/), and the two stores'
fingerprint sidecars, as they stood after the run, into [`registry/stores/`](registry/stores/). Times are UTC, from
each manifest's `created` to its status's last heartbeat; the digest is the one `status.json` records at the end.

| Run | Start → end | Wall | State | Store digest at the end |
|---|---|---|---|---|
| `science-2026.10.0-A-A-lab-20261108T094012` | 08 09:40:12 → 13:14:30 | 3 h 34 min | done | `df834cf8bc70323a…` |
| `science-2026.10.0-B-B-lab-M-trend-20261108T131433` | 08 13:14:33 → 16:23:37 | 3 h 09 min | **failed** (exit 1) | `f5601940d3c4fea7…` |
| `science-2026.10.0-B-B-lab-M-trend-20261108T172913` | 08 17:29:13 → 18:40:16 | 1 h 11 min | done | `a596844f31f7af7d…` |
| `science-2026.10.0-D-D-large-M-natural-20261108T184018` | 08 18:40:18 → 19:04:45 | 24 min | done | `0d107f407d0ff73f…` |
| `science-2026.10.0-R-R-lab-20261108T190447` | 08 19:04:47 → 19:07:32 | 3 min | done | `3cb1c09a9d8a0f32…` |
| `science-2026.10.0-R-R-ladder-20261108T190734` | 08 19:07:34 → 19:21:06 | 14 min | done | `9d5df3216f14f99b…` |
| `science-2026.10.0-R-R-natural-20261108T192108` | 08 19:21:08 → 19:22:37 | 1 min | done | `288ed9441814a7d6…` |
| `science-2026.10.0-C-C-Mcrit-20261108T192238` | 08 19:22:38 → 23:44:54 | 4 h 22 min | done | `81e0deb423b41896…` (main store, final) |
| `science-2026.10.0-R-R-Mcrit-20261108T234456` | 08 23:44:56 → 09 03:22:45 | 3 h 38 min | done | `1e7825328613f956…` (reflection store, final) |

Every manifest records `git_head` `5837a47`, `git_dirty` false and `main.py`'s SHA-256 `2b8956ff…`.

## B's disk failure and the resume

B's first attempt ended at 16:23 on 2026-11-08 with Ray's `OutOfDiskError` ("the local object store is full and the
local disk's utilization is over capacity (95% by default)"), raised when the driver fetched a finished history to
store it. `df` showed the volume 97% used, 30 GB free of 927 GB; Finder reported over 200 GB, the difference being
purgeable space, which Ray does not count. The run's own files were not the cause: its stores held 15 GB and Ray's
temporary directory 0.2 GB. The volume was already past Ray's threshold when the run began: the raylet warned
that it was over 95% full every 10 s from 09:40:21, nine seconds into block A, and the failure came only when the
object store filled and Ray had to spill to disk. `set -e` ended the script, before D, R, C and the post-build steps.

Every history B stored before the failure was of a known kind: its 108 failures were "the effective minimum was not
found" and "step budget exhausted" at the non-surfing β, none mentioning the disk. At the session's suggestion the
user thinned the four Time Machine local snapshots (`tmutil thinlocalsnapshots / 300000000000 4`), after which `df`
showed 206 GB free (78%). The cluster was still up and idle, with its threshold unchanged. The user resumed with
`START_AT=B` at 17:29 on the same tree; B recomputed only what it had not stored (lookups read validated rows only)
and finished in 1 h 11 min. Neither script was edited.

## The post-build steps

The run script's own steps after the build ran the inventories and the exports, then stopped at the derived measures:
it exported the main store through `plot_by_beta.py` alone and the reflection store through `plot_ScalarHistory.py`
alone, and `tools/quanta_measures.py` needs both in one root (`histories.csv` from the first, each history's
`fields.csv` and `bounces.csv` from the second). The main store's histories were all reported missing, and the
reflection root had no `histories.csv` (exit 2), which ended the script at 03:31. The builds and the exports were
unaffected.

The script is a record and was not edited. Instead, after `--no-plots` was added to the plot scripts (`5e3b0f8`; the
CSV files it writes are byte-identical to those written with figures, checked on the reflection store),
[`run_postbuild_2026.10.0.sh`](run_postbuild_2026.10.0.sh) (`4ade10b`) added each root's missing export, then ran the
measures at associated scales 1, 0 and 10 and the two figure tools. It took 50 min: the main store's one-loop export,
`--no-alpha-values --no-plots`, 14 min, and each of the main store's three measure runs about 11 min.

- **The measures:** 3577 one-loop histories of the main store and 15 of the reflection store measured at each scale,
  none refused, none missing. The tool's "failed histories" count (2525 and 47) includes the export's marker rows for
  the (row, M, β) the stored grid does not hold.
- **The K check fails for 18 histories, and only there:** block C at 10⁻¹² M_P, A = 1, β = 1.15–3.0 (and the two of
  R-Mcrit at β = 2.0 and 2.9). The dark-radiation energy rebuilt from the bounce records and the per-sample value
  differ by 3×10⁻⁸ to 6×10⁻⁴ relative where that energy is 10⁻⁵⁰ to 10⁻³⁵ in M_P⁴; every other history passes at about
  10⁻¹⁴. Their ΔN_eff is physically zero. **Undiagnosed.** It is the measures' exit status 1.

## Timings against the plan

PLAN.md §8.3 projected about 3.3–4.3 h before slot C and up to about 3.5 h more for C and R-Mcrit. The build took
16 h 45 min of running (09:40 to 03:31, less the 1 h 06 min gap).

| Slot | Plan | Took |
|---|---|---|
| A | 1.2–2.0 h | 3 h 34 min |
| B | about 1.4 h | 3 h 09 min to the failure + 1 h 11 min |
| D | about 25 min | 24 min |
| R (lab, ladder, natural) | 0.3–0.5 h | 18 min |
| C | 0.3 h, up to about 2 h a pipeline | 4 h 22 min |
| R-Mcrit | (with C) | 3 h 38 min |
| The run script's inventories and exports | — | 8 min |
| The post-build script | — | 50 min |

- **A and B** ran their twelve rows one after another, each a scalar stage of 5–18 min (A) followed by about 4 min of
  BBN, during which most cores wait for the slowest histories. The projection took per-history costs from the
  one-loop campaign's tool route, at six workers, as a total of work over ten workers.
- **C's one-loop row** took 4 h 02 min and **R-Mcrit's** 3 h 36 min, each set by the β ≈ 1.1–1.2 histories at M_crit
  whose tunings ran alone to the 30-integration cap and failed there ("the normalization did not converge"): R-Mcrit's
  last history, 10⁻¹⁵ and β = 1.2, at about 430 s an integration, twice the probe's 215 s for β = 1.1 at the same M.
  PLAN.md §7.3 had found the kind; the probe did not reach β = 1.2. C's fixed-law rows took 12 and 3 min.

## Failed histories

From the exports' `histories.csv`, by block and law, split at the analytic QCD surfing threshold β = 1.1074. The
categories are the first words of each `failure_reasons`.

| Block | Law | β ≤ 1.1074 | of which failed | β > 1.1074 | of which failed |
|---|---|---|---|---|---|
| A | fixed | 46 | 6: 3 PRyMordial, 3 effective minimum | 326 | 0 |
| A | one-loop | 230 | 33: 11 PRyMordial, 16 effective minimum, 6 step budget | 1630 | 0 |
| B | fixed | 60 | 36: 18 PRyMordial, 18 effective minimum | 300 | 0 |
| B | one-loop | 300 | 195: 102 PRyMordial, 78 effective minimum, 15 step budget | 1500 | 0 |
| C | fixed | 20 | 12: 4 PRyMordial, 6 effective minimum, 2 normalization | 100 | 0 |
| C | one-loop | 10 | 8: 7 effective minimum, 1 normalization | 50 | 2 normalization (10⁻¹⁵ β = 1.2; 10⁻¹² β = 1.4) |
| D | natural | 21 | 8: 7 PRyMordial, 1 PRyMordial output | 165 | 4: 3 normalization (M = 0.5, β = 8, 10, 15), 1 effective minimum (M = 0.5, β = 20) |
| R | both | 0 | 0 | 44 | 1 normalization (R-Mcrit, 10⁻¹⁵, β = 1.2, A = 1) |

- **PRyMordial** failures are all at β = 0.5–0.9, where the field is not at the wall before BBN: the history's
  temperature at the start of BBN is just below PRyMordial's spline floor (`T_in_MeV ≈ 0.000198 <
  T_BBN_spline_min = 0.0002`), as in 2026.8.0.
- **"The effective minimum was not found"** and **"step budget exhausted"** below the threshold are the non-surfing
  region's, as in 2026.8.0 and `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` (one-loop-drain
  board §3): β = 1.0 at the laboratory mass at small A, and their analogues at lower M.
- **"The normalization did not converge"** at M_crit is the 30-integration tuning cap, met by the one-loop law at
  β ≈ 1.1–1.4: the Hubble residual stays near 10⁻⁴ against the tolerance 10⁻⁵ (PLAN.md §7.3; one-loop-drain's C-3873).
  Why the one-loop tuning does not settle there, where the fixed law's does, is **undiagnosed**. `--retry-failed-scalar`
  with a larger `--tuning-max-integrations` would retry them.
- **D's four** are the same four as in 2026.8.0 (that run's README, "D's crash and the resume"), at M = 0.5.

## Where the output is

Under `var/science-2026.10.0/out/`, which git ignores; nothing there is copied here except the figures' inputs.

- **`main/QCD_Cosmology/`**: `histories.csv` (every row of the main store, both laws, with the failure reasons and the
  one-loop drain columns), from the run script's `plot_by_beta.py` at every α and A; the figures and `data.csv` of
  each (M, row) under `alpha=<α>/` and `one-loop-measured-v1/A=<A>/`; each one-loop history's `fields.csv` and
  `bounces.csv` under `one-loop-measured-v1/A=<A>/csv/beta=<β>/M=<M>eV_n=1/`, from the post-build script; the
  measures' `quanta_histories.csv` (and `-assoc0`, `-assoc10`), with each history's `quanta_bounces*.csv` and
  `quanta_samples*.csv`; and the grid figure `one-loop-measured-v1/A=<A>/plots/quanta_grid.{pdf,png}`.
- **`reflections/QCD_Cosmology/`**: every history's figures and CSV files from the run script's `plot_ScalarHistory.py`,
  `histories.csv` and its per-(M, row) figures from the post-build script's `plot_by_beta.py`, the measures, and the
  per-history figures of `tools/plot_quanta_history.py`.

M is in eV in the folder names: 9.7413×10⁻⁴ eV is 4×10⁻³¹ M_P; 2.4353×10⁻³, 10⁻⁵, 10⁻⁶, 10⁻⁹, 10⁻¹³ and 10⁻¹⁵ eV
are 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰ and 10⁻⁴² M_P; 2.4353×10¹¹, 10¹² and 10¹⁵ eV are 10⁻¹⁶, 10⁻¹⁵ and 10⁻¹² M_P;
2.4353×10²⁴, 10²⁶ and 1.2177×10²⁷ eV are 10⁻³, 0.1 and 0.5 M_P.

## Where each file came from, and how that is known

- **`PLAN.md`, `probe.py`, `summarise_probe.py` and `run_science_2026.10.0.sh`** were written by a Claude Code
  session on 2026-11-07 and 2026-11-08 at the user's request, and committed in `0dcae32` (the plan and the probe) and
  `5837a47` (the script, and the move of the folder from `science-runs/plan-science-2026.10.0/`, which the docstrings
  of `probe.py` and `summarise_probe.py` and PLAN.md §7 still name). The rulings in PLAN.md §0.2 and §8.1 are the
  user's, in their words in that session. **Neither script was edited after it was committed.**
- **`probe-out/`** holds the probe's outputs, run by the user on 2026-11-07 into `var/science-2026.10.0/probe/`, and
  `summary.txt` from `summarise_probe.py` (PLAN.md §2, §7).
- **The run was launched by the user**, both times with `nohup` from the repository root, on the cluster the user
  started with `tools/ray_cluster.sh start`; the session stopped it with `tools/ray_cluster.sh stop` on 2026-11-09,
  at the user's request, after the figures, and the rehearsal lock was released.
- **`run_postbuild_2026.10.0.sh`** was written by the session on 2026-11-09 and committed in `4ade10b`; the session
  ran it, at the user's request ("we can go ahead with the data analysis"), on the running cluster.
- **`logs/`** is copied unchanged from `var/science-2026.10.0/logs/` after the post-build steps: each block's output
  as the run script `tee`'d it (`B-lab-M-trend.log` holds both attempts, the first above the resume), the two
  inventories, the run script's exports and measures, and the post-build script's steps (`postbuild-*`). The scripts'
  own outputs, `var/science-2026.10.0/run.out` and `postbuild.out`, repeat those logs line for line and are not copied.
  Each run's own copy of its output is also kept in `var/runs/<run>/stdout.log`.
- **`registry/`** is copied unchanged from `var/runs/<run>/` and from the stores' sidecars in `var/datastores/`,
  after the run (the sidecars on 2026-11-09, with Figure 6).
- **`fig6/`, `m-trend/` and `fig6b/`** were written by the session on 2026-11-09 at the user's request, each with
  its rulings in its NOTES.md, and committed script first and renders second (`10a892e`, `1737378`; `6127880`,
  `a3d8d91`; `8ac1e48`, `1ecbd13`), so that each provenance file records a clean commit holding its script. Their
  Python files were formatted with `black` when they were written.
