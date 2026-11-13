# Log 02 — measure the effect

**Subject:** Measure the electroweak threshold and the sample, old against new
**Commit:** this commit, on branch `electroweak-eos`, on top of `e1fd6bc` (the commit that added prompt 02 and its
orchestrator note)
**Date:** 2026-11-13, by Claude Opus 5.5 (one agent, resumed once after a stop)
**Result:** **COMPLETE, after one stop.** Gates Threshold and Effect hold (§3.1, §3.2).
- **The stop.** Before any build, the code probe from the old worktree showed the workers on the worktree's code but
  reading the main checkout's w table. The cluster had been started from the main checkout, and Ray workers take the
  raylet's working directory. **Ruled by the user, 2026-11-13:** "I think this is fine. Please proceed." That is
  option (a), one cluster per side (§2.1, D1).
- **The threshold.** On the new table the EW-capture threshold at M = 10⁻³ M_P, φ* = 5 M_P, tuned, is
  **(3.002, 3.003]**. On the old table, tuned, it is (3.019, 3.020]. The uncaptured window is 2.976–3.002 (27 β),
  against 2.999–3.019.
- **The sample.** 48 histories on each tree, and none failed. The old side equals the run in all 48. 13 move their
  delivery across 10 GeV.
- **The bath drift.** Re-measured on both tables; an addendum is on the issue.
- **Issues:** one opened, `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`. None closed.
- **Tests:** none added or changed. The counts are 01's.

## 1. What shipped

- **`.documents/electroweak-eos/threshold/`**:
  - `edges.py`. Its `range` reads F-new's grid from S1-new. Its `report` writes `out/edges.txt`: the fine-grid rows,
    edges and windows, the threshold beside the references, F-old against S5, S1-new against S1, and the summed
    `wall_s`.
  - `out/s1_new.csv`, `out/f_new.csv` and `out/f_old.csv`, with their `.meta.json` sidecars.
  - `out/surfing_temperature_{s1_new,f_new,f_old}.{pdf,png}`.
- **`.documents/electroweak-eos/bath/`**:
  - `bath_drift.py`, with two parts: `tables` (the issue's integral on both tables) and `histories` (d from
    `fields.csv`).
  - `out/bath_tables.{txt,csv}` and `out/bath_histories.{txt,csv}`.
- **`.documents/electroweak-eos/effect/`**:
  - `code_probe.py`, one-loop-drain 06's extended with the CSV's resolved path and SHA-256, Σ(300 GeV) in each
    process, and `--worker-csv` for the breakage record (ruling 4);
  - `run_side.sh`, which builds one side's six runs;
  - `compare_with_run.py` (P17) and `compare_sample.py` (P18, P19);
  - `wait_for_line.py`, a copy of 06's;
  - `out/` holds:
    - `effect.{csv,txt}` and `compare_with_run.txt`;
    - `breakage-compare_with_run.txt`;
    - `probe/` (the seven probe outputs), `cluster/` (each cluster's start and stop, with `status` and the lock's
      `status`) and `runs/` (each side's launch log, check, store creation and the six runs' logs).
- **`.documents/electroweak-eos-verification.md` §2.** §1 is not edited.
- **The `inelastic-reflection` board**: a dated addendum on `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]`. The issue
  stays open and its index row is unchanged.
- **This board**:
  - row 02, the 02 paragraph, the items, and the Threshold and Effect gates;
  - §3's new issue, and §5's counts after 02.
- **`.documents/OPEN_ISSUES.md`**: the new row under §1.25, the count 30 → 31, and the date 2026-11-13.
- **Not committed:**
  - the stores `var/datastores/electroweak-eos-02-{old,new}.db` and their sidecars;
  - the exports `var/electroweak-eos/02/export/{old,new}/`;
  - the scan consoles and run logs under `var/electroweak-eos/02/`;
  - the registry's records `var/runs/electroweak-eos-02-*`.

## 2. Deviations from the prompt

### 2.1 The rulings

**The orchestrator's note 02, rulings 1–7.**
1. **The user's uncommitted work** was never staged, edited, stashed or reverted: `CLAUDE.md`, the two `science-runs/`
   READMEs and `relativistic/`.
   - It was in the tree throughout. So S1-new's and F-new's sidecars record `git_tracked_changes: true`, and the new
     side's manifests and store sidecar record `git_dirty: true`.
   - `relativistic/NOTES.md` was read, as the prompt asks, and was not changed.
   - *STRUCTURALLY REQUIRED*.
2. **The order of the work.** It was followed, but for D1 and D2:
   - the Gate (11:48);
   - the worktree;
   - the three scans in the background;
   - the bath drift's tables-only part (11:51, beside the scans);
   - after the scans: the cluster, the probes, the old side, the new side, the exports, each cluster stopped;
   - the comparisons, the per-history drift, the suites, the documents, the commit.

   The figures were drawn as each scan ended, and the comparison scripts were written while the scans ran.
   *IMPLEMENTATION CHOICE*, ruled.
3. **F-new's range** was read from S1-new: 2.950–3.050, 101 β, within ruling 3's bound of 151. It is what §0
   expected. For when it was read, see D2. *As the prompt says.*
4. **The code probe's breakage** used `--worker-csv`, a probe option, pointed at
   `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`. The probe exited 1, and the installed CSV was never
   changed (§4). *IMPLEMENTATION CHOICE*, ruled.
5. **The 10 GeV label** labels one list (§3.2). No conclusion rests on it. *As the prompt says.*
6. **Failures.** None on either side, so the stop never arose. No sampled history failed in the run. *IMPLEMENTATION
   CHOICE*, ruled.
7. **No file but §4's list.** The commit holds the prompt's §4 files only (§3.4). *STRUCTURALLY REQUIRED.*

**The orchestrator's correction at dispatch.** The prompt's paths to the run's exports omit one directory level.
- The main store's per-history export is `var/science-2026.10.0/out/main/QCD_Cosmology/`.
- The reflection store's `histories.csv` is `var/science-2026.10.0/out/reflections/QCD_Cosmology/histories.csv`.
- `compare_with_run.py` and `bath_drift.py` use those paths. The main store's twin table is still
  `fig6/inputs/histories.csv`.
- *STRUCTURALLY REQUIRED* (the orchestrator's correction).

**The user's ruling at the stop** (relayed by the orchestrator, 2026-11-13): "I think this is fine. Please proceed."
That is option (a), carried out as D1.

### 2.2 Others

- **D1 — one cluster per side, not one for both** (prompt §1.4, "One cluster for both sides").
  - **What happened.** The cluster started from the main checkout ran the worktree's code on the main checkout's
    table, so probe 1 exited 1 (verification §2.4.1). The cause: Ray workers take the raylet's working directory,
    which is the directory `tools/ray_cluster.sh start` ran from, and `Xav_EOS_spline` reads its CSV by a relative
    path. It was a stop under the note's Conventions.
  - **The new order, under the user's ruling:**
    - that cluster stopped;
    - the old side's cluster started from the worktree root with the worktree's own script; probe 3 (exit 0); the
      old builds, store sidecar and exports;
    - probe 4, from the main checkout on that cluster (exit 1, the same failure the other way round);
    - that cluster stopped;
    - the new side's cluster started from the main checkout; probe 5 (exit 0); the new builds, store sidecar and
      exports;
    - probe 6, the final one (exit 0), then probe 7, the breakage record (exit 1);
    - that cluster stopped.
  - **The prompt's third probe** ("from the main checkout again after the old side's builds end") is **probe 5**. It
    ran from the main checkout after the old builds had ended, on the cluster the new side then used, before its first
    build. Probe 4 records why the old side's cluster could not take that place.
  - **The issue** is opened on the board (§3) and indexed.
  - *STRUCTURALLY REQUIRED* (orchestrator's class).
- **D2 — F-new launched before S1-new ended.** Ruling 2 runs the three scans concurrently, and F-new's range comes from
  S1-new.
  - F-new's range was read when S1-new's console printed β = 3.1 (41 of 79 rows, 12:12 GMT). F-new was then launched,
    23 minutes after the other two.
  - The range was read again on S1-new's 79 rows: 2.95, 3.05, one window, the same.
  - Had the full CSV differed, F-new would have been run again.
  - *IMPLEMENTATION CHOICE.*
- **D3 — the driver files' diff is not empty.** `git diff --stat ec6bca8 HEAD` over the prompt's driver files lists
  `config/version.py` (13 + / 1 −). An AST comparison, comments and docstrings set aside, shows `VERSION_LABEL`
  `2026.10.0` → `2026.11.0` as its only change. The label is the tree's, not a driver's behaviour. The prompt
  expected the diff to be empty, because it listed `config/` among the driver files. *STRUCTURALLY REQUIRED.*
- **D4 — F-old's figure was drawn from the worktree.** `tools/plot_surfing_temperature.py` computes the reachability
  roots with `find_kicks` on its own tree's table. Drawn from the main checkout, F-old's figure would have shown the
  new table's roots beside the old table's scan. It was drawn first from the main checkout and then redrawn from the
  worktree (13:11). The committed figure is the worktree's. *STRUCTURALLY REQUIRED.*
- **D5 — the store sidecars were made by hand** after each side's builds, from that side's root, not by
  `run_side.sh`. *IMPLEMENTATION CHOICE.*
- **D6 — the export's marker rows are set aside.** `histories.csv` holds 137 marker rows ("no ScalarHistory row in the
  store") besides the 48 histories. Both comparison scripts count them and leave them out. *IMPLEMENTATION CHOICE.*
- **D7 — a supplement to P17.** `compare_with_run.py` also compares each per-history `fields.csv` and `bounces.csv` of
  the old export with the run's export of the same history, byte for byte: 80 equal, none different, 5 with no twin.
  A difference would have counted as a P17 difference. *IMPLEMENTATION CHOICE.*
- **D8 — ln T_J in the bounce alignment** (P19). The prompt asks for the largest relative difference in
  `log_T_Jordan_GeV`. Near 1 GeV, ln T_J is near 0, and its relative difference says nothing (55 at a-edge β = 3.0).
  `compare_sample.py` reports the absolute difference as well, which is the relative change of T_J, and the
  verification document quotes that. Both are in `effect.txt`. *IMPLEMENTATION CHOICE.*
- **D9 — "the first ordinal whose kind or source differs" is none in every history**, even where the counts differ by
  up to 102. The sequences alternate candidate and plateau the same way, so this criterion does not find where the
  events diverge. The largest differences and their ordinals are reported beside it. *IMPLEMENTATION CHOICE* (how it
  is reported; the measurement is the prompt's).
- **D10 — turning points of fixed-law and natural-mode histories.** The Effect gate names "the turning points". The
  exports write `bounces.csv` for one-loop histories only. So for the 11 fixed-law and natural histories the turning
  points are compared through T_deliver (the first bounce), the reflections and the injections. Their stores were
  not read directly. *STRUCTURALLY REQUIRED.*
- **D11 — the quanta measures were not run.** `relativistic/` plots E from `tools/quanta_measures.py`, which the prompt
  does not ask for. Its stored margins and shares are compared instead (verification §2.4.7). *IMPLEMENTATION CHOICE.*
- **D12 — the bath drift's "today"** is not on `thermo_compare.py`'s grid, which ends at 10⁻⁵ GeV. `bath_drift.py`
  continues the integral below the grid to T_CMB at the same density. The continuation adds nothing: the integrand is
  zero there, so today's value is the value at 10⁻⁵ GeV. It is reported beside the prompt's "end of the table's range".
  *IMPLEMENTATION CHOICE.*
- **D13 — the bath drift's units.** The issue's per cent are 100 Δ (its −1.17 % is Δ = −1.168 × 10⁻²). The script
  prints Δ, 100 Δ and 100(e^Δ − 1), and the documents quote 100 Δ. The orchestrator's message quoted the extremes as
  "−1.69 % at 24.7 GeV and +2.03 % at 0.163 GeV", which mixes the two conventions. In 100 Δ they are −1.69 and +2.01,
  and as offsets of ρ_R −1.68 % and +2.03 %. The addendum gives both. *IMPLEMENTATION CHOICE.*
- **D14 — the copied run logs.** The prompt names `out/` "as text and CSV only". The probe outputs, the clusters'
  status records and the runs' launch logs (308 KB with `effect.*`) were copied into `effect/out/`, so that the
  verification document's claims about them have a committed source. The scan consoles (204 KB of tuning lines) were
  not copied, and stay in `var/`. *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 Gate Threshold

- **Bracketed to 0.001 in β:** (3.002, 3.003] on F-new.
- **Beside 3.020 and 3.00285:**

  | source | threshold |
  |---|---|
  | F-new | (3.002, 3.003] |
  | F-old, tuned | (3.019, 3.020] |
  | S5, untuned | 3.020 |
  | the notebook | 3.00285 |
  | reachability, new peak | 2.99918 |
  | reachability, old peak | 3.011779 |

- **One window on each grid**, with no capture inside it:
  - F-new: QCD to 2.975, nothing at 2.976–3.002 (27 β), EW from 3.003;
  - F-old: QCD to 2.998, nothing at 2.999–3.019 (21 β), EW from 3.020.
- **The first bounce across the new window** rises from 1.6121 GeV (2.976) to 50.193 GeV (3.002), steepest at
  2.981–2.984.
- **S1-new against S1:** the same kick captures at every β. QCD's T_deliver moves by up to +0.96 % (2.95), and EW's by
  +2.7 to +12.2 %.
- **F-old against S5:** every kick is the same. T_deliver differs by at most 1.6 × 10⁻⁶ relative.
- **The scans:**

  | scan | wall | load at start (1 min) | sidecar |
  |---|---|---|---|
  | S1-new | 4820.6 s | 11.37 | `e1fd6bc`, 2026.11.0 |
  | F-old | 2514.1 s | 11.37 | `ec6bca8`, 2026.10.0 |
  | F-new | 4367.4 s | 6.61 | `e1fd6bc`, 2026.11.0 |

  All three ran at once, and every row is `ok`.

### 3.2 Gate Effect

- **Every sampled history** (48) was built on both trees. Its changes in Y_p, D/H, ΔN_eff (today, 1 MeV), the delivery
  temperature and the turning points (the bounce rows for the 37 one-loop histories, T_deliver, reflections and
  injections for all; D10) are in `effect.csv`. By run they are in verification §2.4.5, and beside every figure's axes
  and rows in §2.4.7.
- **The old side against the run (P17):** 48 of 48 matched, 0 differences, and 80 per-history files byte-equal.
- **The histories whose T_deliver crossed 10 GeV:** 13.
  - a-edge β = 2.985, 2.99, 2.995 and 3.0;
  - b β = 3.0 at all six M;
  - d β = 3.0 at all three M.
  - All moved from 1.68–1.93 GeV to 20.6–48.5 GeV.
- **Failures:** none on either side.
- **By run,** the largest and median relative |change|:

  | run | Y_p | D/H | ΔN_eff today |
  |---|---|---|---|
  | a-edge | 1.1 × 10⁻³ / 4.0 × 10⁻⁴ | 2.7 × 10⁻³ / 9.5 × 10⁻⁴ | 0.54 / 0.41 |
  | a-spread | 2.3 × 10⁻⁴ / 2.3 × 10⁻⁵ | 1.6 × 10⁻³ / 4.1 × 10⁻⁵ | 0.039 / 3.2 × 10⁻⁴ |
  | b | 6.6 × 10⁻⁴ / 2.2 × 10⁻⁴ | 1.6 × 10⁻³ / 5.9 × 10⁻⁴ | 0.44 / 0.19 |
  | c | 3.2 × 10⁻⁵ / 1.4 × 10⁻⁵ | 1.1 × 10⁻⁴ / 6.9 × 10⁻⁵ | 0.015 / 0.0076 |
  | d | 3.7 × 10⁻⁴ / 5.6 × 10⁻⁵ | 2.8 × 10⁻³ / 1.6 × 10⁻⁴ | 0 (α = 0) |
  | r | 7.0 × 10⁻⁵ | 7.6 × 10⁻⁵ | 9.2 × 10⁻⁶ |

### 3.3 The probe, the clusters and the runs

**The probe from both trees.** Each line gives the process, its code, its label, its CSV and Σ(300 GeV).

| # | cluster started from | run from | driver | task / actor | exit |
|---|---|---|---|---|---|
| 1 | main checkout | worktree | worktree, 2026.10.0, `28edd854…`, 5.685 × 10⁻³ | worktree code, 2026.10.0, but CSV `690090c8…` (the main checkout's), 1.889 × 10⁻³; cwd the main checkout | 1 |
| 2 | main checkout | main checkout | main checkout, 2026.11.0, `690090c8…`, 1.889 × 10⁻³ | the same | 0 |
| 3 | worktree | worktree | worktree, 2026.10.0, `28edd854…`, 5.685 × 10⁻³ | the same; cwd the worktree | 0 |
| 4 | worktree | main checkout | main checkout, 2026.11.0, `690090c8…` | main-checkout code, CSV `28edd854…`, 5.685 × 10⁻³ | 1 |
| 5 | main checkout | main checkout | main checkout, 2026.11.0, `690090c8…`, 1.889 × 10⁻³ | the same | 0 |
| 6 | main checkout | main checkout (final) | as 5 | as 5 | 0 |
| 7 | main checkout | main checkout, `--worker-csv` the old copy | as 5 | CSV `28edd854…`, 5.685 × 10⁻³ | 1 |

**The clusters.** Every `start` and `stop` exited 0, and the lock was HELD after each start. After each stop the
status showed no process, the ports free and the lock free; the lock had no old location throughout. No exit 3 and
no exit 4.

| cluster | from | start (load) | stop |
|---|---|---|---|
| 1 | main checkout | 13:25:33 (7.50) | 13:36:06 |
| 2 | worktree | 13:36:34 (9.24) | 13:49:11 |
| 3 | main checkout | 13:49:37 (21.75) | 14:02:01 |

**The registered runs.** All are `done` with exit 0, and every launch–end and load comes from the run's own log.

| id | wall | load at launch | `git_head` | `git_dirty` |
|---|---|---|---|---|
| `electroweak-eos-02-old-a-edge-20261113T133704` | 1:45 | 7.82 | `ec6bca8` | true |
| `electroweak-eos-02-old-a-spread-20261113T133850` | 2:28 | 27.22 | `ec6bca8` | true |
| `electroweak-eos-02-old-b-20261113T134118` | 1:29 | 15.43 | `ec6bca8` | true |
| `electroweak-eos-02-old-c-20261113T134247` | 1:23 | 52.25 | `ec6bca8` | true |
| `electroweak-eos-02-old-d-20261113T134410` | 1:40 | 24.06 | `ec6bca8` | true |
| `electroweak-eos-02-old-r-20261113T134549` | 1:13 | 23.42 | `ec6bca8` | true |
| `electroweak-eos-02-new-a-edge-20261113T135000` | 2:25 | 26.40 | `e1fd6bc` | true |
| `electroweak-eos-02-new-a-spread-20261113T135226` | 2:31 | 47.07 | `e1fd6bc` | true |
| `electroweak-eos-02-new-b-20261113T135456` | 1:30 | 11.13 | `e1fd6bc` | true |
| `electroweak-eos-02-new-c-20261113T135626` | 1:19 | 39.57 | `e1fd6bc` | true |
| `electroweak-eos-02-new-d-20261113T135746` | 1:41 | 17.35 | `e1fd6bc` | true |
| `electroweak-eos-02-new-r-20261113T135926` | 1:10 | 7.75 | `e1fd6bc` | true |

- **The sides.** The old side took 9 min 58 s (launched 13:37:03) and the new side 10 min 36 s (launched 13:49:59),
  both far inside the three-hour bound.
- **`git_dirty`.** The old side is dirty from the worktree's `venv` and `var` links (untracked). The new side is
  dirty from the user's files (ruling 1).
- **One check each.** Each side was checked once (`effect/out/runs/<side>.check.txt`) and waited for with
  `wait_for_line.py`.

**The exports.** Both sides used `--config exponential.yaml --database <store> --shards 16 --ray-address
127.0.0.1:6380 --phi-init-Mp 5 --alpha-values 0 0.1 --A-values 0.05 1 10 --no-plots --output
var/electroweak-eos/02/export/<side>`.
- **Why those options.** They name exactly the stored rows: fixed α = 0.1 (`a-spread`, `c`) and α = 0 (`d`), and
  one-loop A = 0.05, 1 and 10. The tolerances, T*, the tuning tolerance and n are the builds' defaults.
- **Neither plot script refused.** Each ran for 27–28 s, with exit 0.

### 3.4 Scope, the CSV, the suites

- **`git diff --name-only 5947b4f`**, run before the commit, lists 13 files: `.documents/OPEN_ISSUES.md`,
  `.documents/electroweak-eos-verification.md`, `CLAUDE.md`, `prompts/INDEX.md`,
  `prompts/electroweak-eos/{02-measure-the-effect.md, IMPLEMENTATION_STATE.md, README.md, logs/01-install-the-table.md,
  orchestrator/prompt-01.md, orchestrator/prompt-02.md}`, `prompts/inelastic-reflection/IMPLEMENTATION_STATE.md`, and
  the two `science-runs/` READMEs. Some are committed since 01 and some are uncommitted, this prompt's or the user's.
  The new files under `.documents/electroweak-eos/` are untracked scripts and outputs. None is imported by a suite,
  so no file the suites import changed (README §9.2).
- **`tools/build_w_table.py --check`** exits 0 at the end. The installed CSV is `690090c8…0119d` throughout.
- **`runregistrykit list`** at the end: 54 runs, the 12 of this prompt all `done`. `tools/ray_cluster.sh status`
  shows no process.

**The suites**, once, serially, after the measurements, through a runner in the scratchpad:

| suite | tests | result | wall (s) | one-minute load at start |
|---|---|---|---|---|
| CosmologyModels | 26 | OK | 67 | 27.35 |
| ObjectFactories | 163 | OK | 141 | 11.91 |
| RunDrivers | 76 | OK | 27 | 6.87 |
| ComputeTargets `[a-f]` | 219 | OK | 148 | 11.28 |
| ComputeTargets `[g-z]` | 465 | OK | 530 | 5.85 |
| tools | 292 | OK | 300 | 13.18 |

They ran from 14:03:18 to 14:23:33 GMT. `uptime` at the end showed load averages of 18.80 / 13.61 / 13.71, on a
shared machine; nothing else of this prompt ran beside them.

No count differs from 01's: CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories 163, tools 292,
RunDrivers 76.

## 4. The deliberate-breakage record

- **The probe** exits 1 when a worker's view differs from the driver's.
  - **Constructed** (probe 7): `--worker-csv prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`, run from the
    main checkout. For the task and the actor, `EOS_CSV`, `EOS_CSV_SHA256` (`28edd854…` against `690090c8…`) and
    `Sigma_300GeV` (5.685 × 10⁻³ against 1.889 × 10⁻³) differ from the driver's. Exit 1. The installed CSV is
    unchanged.
  - **Real** (probes 1 and 4): the same three fields differ, for the reason of D1. Exit 1 each.
- **`compare_with_run.py`.** Against a scratch copy of the run's main-store table with one cell changed (β = 3.0,
  M = 4 × 10⁻³¹, A = 1, `Yp_BBN` 0.24725819050492973 → 0.24725819050492971), it reports "1 columns differ … Yp_BBN" and
  exits 1 (`effect/out/breakage-compare_with_run.txt`).
- **`bath_drift.py`** on the old table reproduces every figure of the issue's table: +0.19, −1.17, −0.08, −0.21,
  −0.211, −0.098 and −0.078 (100 Δ). That stands for its check.
- **`edges.py`** exits 3 on a partial S1-new with no EW row yet ("0 windows", tried at 24 rows). On F-new and F-old it
  exits 0.

## 5. Observations not acted on

- **The cause of the stop is opened as an issue.** A worktree build through a cluster started from another tree reads
  that tree's data files (`Xav_EOS_spline`'s CSV; PRyMordial's `PRyMrates/`).
  - `one-loop-drain` 06's base build ran this way. Its probe could not see it, and its trees' data files were equal.
  - This is `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`, on the board in §3.
- **The bath drift is larger on the new table.** At 1 MeV it is −0.328 % (100 Δ), against −0.211 %, and today −0.195 %,
  against −0.078 %. Below 22 GeV the new drift is the old less a constant 1.169 × 10⁻³.
  - The grid's extremes, on the old table, are −1.69 at 24.7 GeV and +2.01 at 0.163 GeV (100 Δ).
  - The issue's "What" says "It peaks at −1.2 % in ρ_R near 1 GeV". That is the value at the quoted temperatures,
    not the extreme.
  - Recorded in the addendum. The issue's text is not changed.
- **`tools/plot_surfing_temperature.py` reads the w table of the tree it runs from** (through `find_kicks`). A
  scan's figure drawn on another tree shows that tree's reachability roots beside the scan's own temperatures. The
  figure has no check against its CSV's sidecar (`version_label`). Not opened. It is a property of a tool that reads
  its tree, and the sidecar records the tree.
- **Each plot script printed "number of shards read from database (=16) does not match specified number of shards
  (=10)"**, although `--shards 16` was given. The run's own export logs have the same line
  (`postbuild-plot_by_beta-reflections.log`). Not investigated.
- **`ray_cluster.sh stop` ended each cluster with SIGKILL to its two head processes** after the 15 s grace period. That
  was three times of three. The stop then reported every process gone and released the lock. Not investigated.
- **The cost of a sample side** was about 10 minutes at loads of 8–52, against P14's estimate of about 120
  core-minutes. The estimate took its costs from the run's blocks, whose cost at M_crit came from β ≈ 1.1–1.2. The
  sample has none of those.
- **Edge histories with the same delivery at every M.** At β = 3.0 the new first bounce is 48.4902 GeV at every M from
  10⁻⁴² to 10⁻³ M_P, and 48.4897 and 48.4875 GeV at 0.1 and 0.5 M_P. At M = 4 × 10⁻³¹ the a-edge deliveries equal
  F-new's (at M = 10⁻³ M_P) to 2 × 10⁻⁵.
  - So the new window's edge on Figure 6's dense grid at A = 1 is (2.980, 2.985]. The run had (3.000, 3.005].
  - fig6b's notes place its "electroweak cut" at Figure 6's 3.000–3.005.
- **Two edge histories now park above the BBN window.** At β = 2.99 and 2.995 (A = 1, 4 × 10⁻³¹) the parking
  temperature moves from 0.70 and 1.05 MeV, inside Figure 6's indicative 20 keV – 2 MeV window, to 42.9 and 55.4 MeV,
  above it. Recorded, not judged.
- **The P8 expectation is not met as worded.** The bounce sequences agree in kind and source wherever they are
  compared, but even where the delivery is unchanged the drain records differ. At β = 2.0 the differences are
  3 × 10⁻⁶ in N and 2.5 × 10⁻⁵ in α_eff. At M_crit, α_eff at the last ordinal differs by 3.4 × 10⁻², and in R-ladder by
  0.21. Recorded (P19: "recorded, not bounded").
- **`release_T_GeV` is blank** in every history on both sides. No sampled history releases.

## 6. State handed to the next prompt

- **For 03 — the threshold.** On the new table, at M = 10⁻³ M_P, φ* = 5 M_P, tuned:
  - EW captures from **3.003** (the threshold is in **(3.002, 3.003]**);
  - QCD captures to **2.975**;
  - nothing captures at **2.976–3.002** (27 β).
  - On the old table, tuned (today's tool), the threshold is (3.019, 3.020] and the window 2.999–3.019, the same as
    the untuned S5's.
  - The references: the notebook's 3.00285, reachability from the new peak 2.99918, and from the old 3.011779.
- **The window on the run's grid.** On the dense grid at the laboratory mass, A = 1, the delivery jumps between β =
  2.980 (1.95 GeV) and 2.985 (20.6 GeV). The run's jump was between 3.000 and 3.005.
- **The per-run changes** (largest / median relative |change|; verification §2.4.5):

  | run | Y_p | D/H | ΔN_eff today |
  |---|---|---|---|
  | a-edge | 1.1 × 10⁻³ / 4.0 × 10⁻⁴ | 2.7 × 10⁻³ / 9.5 × 10⁻⁴ | 0.54 / 0.41 |
  | a-spread | 2.3 × 10⁻⁴ / 2.3 × 10⁻⁵ | 1.6 × 10⁻³ / 4.1 × 10⁻⁵ | 0.039 / 3.2 × 10⁻⁴ |
  | b | 6.6 × 10⁻⁴ / 2.2 × 10⁻⁴ | 1.6 × 10⁻³ / 5.9 × 10⁻⁴ | 0.44 / 0.19 |
  | c | 3.2 × 10⁻⁵ / 1.4 × 10⁻⁵ | 1.1 × 10⁻⁴ / 6.9 × 10⁻⁵ | 0.015 / 0.0076 |
  | d | 3.7 × 10⁻⁴ / 5.6 × 10⁻⁵ | 2.8 × 10⁻³ / 1.6 × 10⁻⁴ | 0 / 0 |
  | r | 7.0 × 10⁻⁵ | 7.6 × 10⁻⁵ | 9.2 × 10⁻⁶ |

  In Figure 6's units the largest moves are 0.11 pp in ΔY_p and 0.27 pp in Δ(D/H), both at the edge. ΔN_eff at the
  edge falls to 0.46–0.71 of its old value, except at 3.005 (×1.14). No sampled history changes side of Figure 6's
  Y_p, D/H or ΔN_eff tests.
- **The histories that changed their kind of delivery** (T_deliver across 10 GeV; 13):
  - a-edge, β = 2.985, 2.99, 2.995 and 3.0 at 4 × 10⁻³¹, A = 1;
  - b, β = 3.0 at 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰ and 10⁻⁴², A = 1;
  - d, β = 3.0 at 10⁻³, 0.1 and 0.5, α = 0.
  - In each, 1.68–1.93 GeV → 20.6–48.5 GeV.
  - β = 3.005 moved inside the EW range, from 14.26 GeV (in the old window) to 52.42 GeV. β = 2.98 is now
    uncaptured, with T_deliver almost unchanged (1.991 → 1.953 GeV).
- **The bath drift on the new table** (100 Δ): −0.06 at 100 GeV, −1.28 at 1 GeV, −0.20 at 0.1 GeV, −0.33 at 10 MeV and
  1 MeV, −0.21 at 0.1 MeV, and −0.19 at 10 keV and today. The addendum is on `[09-…]`, and the issue stays open.
- **The stores and exports, by path** (whether they are kept is 03's to say):
  - stores: `var/datastores/electroweak-eos-02-old.db` (sidecar store_id `821adc85…`) and `-new.db`, 16 shards each,
    with their `.manifest.json`;
  - exports: `var/electroweak-eos/02/export/{old,new}/QCD_Cosmology/` (`histories.csv` and the per-history
    `fields.csv` and `bounces.csv`);
  - logs: `var/electroweak-eos/02/` (`threshold/`, `runs/`, `cluster/`, `probe/`, `export/*.log`), with copies of the
    small ones in `.documents/electroweak-eos/effect/out/`;
  - registry: `var/runs/electroweak-eos-02-{old,new}-*` (12 runs).
- **The surfing tests' reference rows** (log 01 §6) are still `table/out/surfing_reference_rows.csv`. Whether the tests
  should read S1-new (`threshold/out/s1_new.csv`, tuned) is 03's.
- **The suite counts** are unchanged from 01: CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories 163,
  tools 292, RunDrivers 76. `CLAUDE.md` still holds the user's edits, so its counts are not updated.
- **Issues.** One opened (§5, board §3); none closed. The index has 31 open.
