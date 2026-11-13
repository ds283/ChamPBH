# Prompt 02 — measure the effect

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-13, after 01 landed (`5947b4f`, review `850cc72`). It is written from:
- README §6.1 as ruled: "U1 to U6 are fine as proposed; U7 doesn't need to be in scope here". In particular, U5:
  the notebook-side threshold is not run. U6: a sample of about 40 histories, each tuned on both trees and run
  through BBN, about 1–2 h, detached;
- README §6.2 P6–P9 and P12, as §9 makes them concrete (P13–P20);
- log 01 §5 and §6 ("Observations not acted on", "State handed to the next prompt").

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `electroweak-eos`, at or after the commit that adds this prompt; `5947b4f` is an ancestor of `HEAD`;
- `git status`: no tracked change other than those the orchestrator note lists as the user's;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `tools/ray_cluster.sh status`: no ChamPBH Ray process, the four ports free; `./venv/bin/python -m
  raypoolkit.rehearsal_lock status`: free, and no `old location`;
- `tools/build_w_table.py --check` exits 0;
- `var/datastores/science-2026.10.0.db` and `science-2026.10.0-reflections.db` and their sidecars exist; no
  `var/datastores/electroweak-eos-02-*` exists;
- `.documents/electroweak-eos-verification.md` has §1 and no §2;
- `df -h .`: the volume no more than 93 % used. Ray stops spilling its object store to disk at 95 %, which ended
  the run's block B (run record, "B's disk failure").

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** Three measurements run detached on two trees, one of them through a Ray cluster
shared with a worktree. Which tree's code and table each worker ran must be shown, not assumed.

**Read first:**
1. [`README.md`](README.md): §0, §1, §2.1 (the Threshold and Effect gates), §3, §5, §6.1 as ruled, §6.2 P6–P9 and
   P12, and §9.
2. [`logs/01-install-the-table.md`](logs/01-install-the-table.md) §3.4 (β = 3 on the new table), §5 and §6.
3. `.documents/surfing-temperature-verification.md` §1.1 (how S1 and S5 were run), §1.3 (the dynamical thresholds)
   and §1.8 (the fine scan near 3.0), and `tools/surfing_scan.py`'s docstring.
4. The run record, `science-runs/2026-11-08-science-2026.10.0/README.md`: "The run", "The registered runs",
   "B's disk failure", "The post-build steps", "Failed histories" and "Where the output is"; its
   `run_science_2026.10.0.sh` and `run_postbuild_2026.10.0.sh`, for how each block and each export was invoked;
   and each figure's `NOTES.md` (`fig6/`, `m-trend/`, `fig6b/`, `relativistic/`), for what it plots and over what
   range.
5. `prompts/one-loop-drain/06-rehearse.md` §1.1–§1.4, and `.documents/one-loop-drain/rehearse-06/`
   (`code_probe.py`, `check.sh`, `wait_for_line.py`, `beat_times.py`): the precedent for a worktree build through
   the shared cluster.
6. `CLAUDE.md`: "Ray on this machine", "The rehearsal lock", "Ray's token mode" and "Long-running jobs — the run
   registry".
7. The issue `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]` on the `inelastic-reflection` board (§3), and
   `.documents/peakewsurf-field-equations/scripts/thermo_compare.py`, whose consistency integral it quotes.

---

## 0. Where this prompt sits

01 installed the new w table as `2026.11.0` and re-pinned what it moved. This prompt measures what the table does to
the physics: the electroweak-capture threshold, a sample of the `2026.10.0` run's histories old against new, and
the bath drift. 03 writes the documents and the handover to a rebuild. The rebuild itself is the user's (README §1).

**Nothing in this prompt changes code.** It adds scripts and their text outputs under `.documents/electroweak-eos/`,
builds two small stores of its own, and writes the verification document's §2, the log and the board.

"Old" is `ec6bca8`, in a worktree (README §2). "New" is this branch's `HEAD`. The old side never runs on this tree with
the old CSV swapped back in. A tables-only measurement (§1.3) may read the old CSV through a redirect inside its own
script, as 01's `sigma_change.py` does, since it integrates no history.

## 1. What is wanted

Throughout: `ADDRESS=127.0.0.1:6380`; `S=$PWD/var/datastores`; `L=$PWD/var/electroweak-eos/02`, for launch logs,
exports and other large output, which is ignored by git; `D=.documents/electroweak-eos`, for what is committed. `$PWD`
is the main checkout's root.

### 1.1 The old tree

- `git worktree add --detach <scratch>/old-ec6bca8 ec6bca8`, with `<scratch>` your subdirectory of the session
  scratchpad. In it, two symbolic links to this checkout's `venv` and `var`, as `one-loop-drain` 06 §1.1 made them.
  Record what the worktree's `git status` says about them. A dirty flag from the links in a manifest is expected.
- Confirm and record:
  - `git diff --stat ec6bca8 HEAD` restricted to `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
    `plot_inputs.py`, `config/`, `tools/surfing_scan.py`, `tools/history_and_bbn.py` and `RunDrivers/` is empty;
  - the computation differs by `ComputeTargets/ScalarHistory.py` (01a) and the CSV (01) and nothing else that a
    history reads;
  - the worktree's `VERSION_LABEL` is `2026.10.0`, and its CSV's SHA-256 is `28edd854…29a8`.
- It is removed (`git worktree remove`, then `git worktree prune`) once the old side's exports are written, and before
  the commit. Nothing after that runs from it.

### 1.2 The threshold (P13; the Threshold gate)

With `tools/surfing_scan.py`, φ* = 5 M_P, M = 10⁻³ M_P, the tool's default tuning, as surfing-temperature 04 ran S1:

| scan | tree | β | output |
|---|---|---|---|
| S1-new | new | 1.10–5.00 in 0.05 (79) | `$D/threshold/out/s1_new.csv` |
| F-new | new | 0.001 steps from S1-new's largest QCD-captured β to its smallest EW-captured β, both included | `$D/threshold/out/f_new.csv` |
| F-old | old (the worktree) | 2.980–3.040 in 0.001 (61), S5's grid | `$D/threshold/out/f_old.csv` |

- F-new's range is read from S1-new's CSV (`capture_kick`), and the log says how. If S1-new has more than one window
  between a QCD capture and an EW capture, or none, stop and ask.
- Each scan is one process, launched in the background, with its console output under `$L/threshold/`. Wait for each
  to end without polling. A scan cut off is resumed by running it again, which `surfing_scan.py` supports. F-old runs
  from the worktree's root, so that it reads the old tree's code and CSV.
- Each scan's sidecar (`.meta.json`) records the tree. Check that F-old's names `ec6bca8` and `2026.10.0`, and the
  others this branch's `HEAD` and `2026.11.0`.
- The CSVs and sidecars are committed. Draw each scan's figure with `tools/plot_surfing_temperature.py` into
  `$D/threshold/out/`, as surfing-temperature 04 did.
- **Record** (verification §2.1):
  - every row of F-new and F-old, as surfing-temperature's §1.8 lists S5's;
  - the edges on each fine grid: the largest β QCD captures, the smallest β EW captures, and the uncaptured window
    between them. If a fine grid shows more than one window, or a capture between uncaptured β, record it and stop;
  - the EW threshold on F-new, beside F-old's, the untuned 3.020 (surfing-temperature §1.8), the notebook's 3.00285
    (README §0.1), and the reachability prediction from the new peak, 2.99918 (log 01);
  - S1-new's captures against S1's (surfing-temperature §1.4): who captures, at which β, and T_capture, T_max and
    T_deliver by kick;
  - the first bounce across the new uncaptured window, as §1.8 records it for the old;
  - each scan's wall time and the load at its start.

### 1.3 The bath drift (P20)

`$D/bath/bath_drift.py`:
- **Tables only.** The issue's integral, Δ(T) = ∫ from ln T to ln T* of [4 + d ln g_ρ/d ln T − 3(1 + w)(1 + x)] d ln T,
  with x = (1/3) d ln g_s/d ln T, from `QCD_Cosmology`'s own functions, on the new table and on the old (read through
  the redirect). Use the grid `thermo_compare.py` used if it can be rebuilt, otherwise 2000 points per decade from
  10⁻⁵ to 2 × 10⁴ GeV, and say which. Report Δ at 100, 1, 0.1, 10⁻², 10⁻³ and 10⁻⁴ GeV and at the end of the table's
  range. On the old table it should give the issue's figures (+0.19 %, −1.17 %, …); say whether it does.
- **Per history**, after §1.4: ln ρ_R,J − ln((π²/30) g_ρ T_J⁴) from `fields.csv`
  (`log_rhorad_Jordan_GeV4`, `gstar_rho`, `T_Jordan_GeV`) at T_J = 1 GeV, 1 MeV and the last sample, on the sample's
  β = 2, M = 0.5 M_P, α = 0 history from both stores, and on the run's own export of that history (R-natural's, in
  `var/science-2026.10.0/out/reflections/`). Interpolate in ln T_J at each sample bracketing the temperature, on the
  first bracketing pair, and say so.
- The output goes to `$D/bath/out/`. The numbers go on the issue's entry as a dated addendum (§4). The issue stays
  open (U4).

### 1.4 The sample (P14–P17; the Effect gate)

**The cluster.** One cluster for both sides: `tools/ray_cluster.sh start "electroweak-eos 02 (sample)"` from the main
checkout, with its default of 10 CPUs. Record `status` and the lock's `status` after `start` and after `stop`. Exit 3
from the script or the lock is a stop: never wait, retry or remove the lock.

**The code probe (P16).** Copy `one-loop-drain` 06's `code_probe.py` to `$D/effect/code_probe.py` and extend it. From the
driver, from one task and from one actor, it prints what 06's printed, and also:
- the SHA-256 of `CosmologyModels/GenericEOS/Xav_EOS_data.csv` as that process resolves the path;
- Σ = 1 − 3w at 300 GeV from a `Xav_EOS_spline` built in that process. It is 5.685 × 10⁻³ on the old table and
  1.889 × 10⁻³ on the new (planner, README §9.2).

It exits 1 if a worker's module files, labels, CSV hash or Σ differ from the driver's. Run it from the worktree and from
the main checkout before any build, and from the main checkout again after the old side's builds end. From the worktree
every value must be the old tree's, and from the main checkout the new tree's. Anything else is a stop: a build whose
workers read the other table would compare a tree with itself.

**The builds.** README §9.2 P14's six runs, on each side, in this order: the whole old side, then the whole new side.
Every run gets `--config exponential.yaml --database $S/electroweak-eos-02-<side>.db --shards 16 --job-name
electroweak-eos-02-<side>-<run> --ray-address $ADDRESS --phi-init-Mp 5 --bbn-wall-clock-limit 600`, with:

| run | grid |
|---|---|
| `a-edge` | `--M-values-Mp 4e-31 --beta-values <the 13> --A-values 1 --no-alpha-values` |
| `a-spread` | `--M-values-Mp 4e-31 --beta-values 1.2 2.0 5.0 --alpha-values 0.1 --A-values 0.05 1 10` |
| `b` | `--M-values-Mp 1e-30 1e-32 1e-33 1e-36 1e-40 1e-42 --beta-values 2.0 3.0 --A-values 1 --no-alpha-values` |
| `c` | `--M-values-Mp 1e-15 1e-12 --beta-values 2.0 --alpha-values 0.1 --A-values 1` |
| `d` | `--M-values-Mp 1e-3 0.1 0.5 --beta-values 2.0 3.0 --alpha-values 0` |
| `r` | `--M-values-Mp 1e-16 --beta-values 2.0 --A-values 1 --no-alpha-values` |

- The 13 are the run's own values, generated as `run_science_2026.10.0.sh` generates them (`2980:3040:5:1000`). Every β
  must be its block's value to the bit (the run wrote them as `repr(i / den)`). Say how that was checked.
- One driver script per side, `$D/effect/run_side.sh <side>`, runs the six through that tree's
  `RunDrivers/registered_run.py` (`--campaign electroweak-eos --prompt 02 --register <side>-<run> --purpose …`) in
  sequence and stops at the first failure. It is launched detached (`nohup`, through `sh`, as `one-loop-drain` 06 §1.4
  did), from the side's tree root, with its log and pid file under `$L/runs/`. Make **one** check that it started, then
  wait in the background for its end line with `wait_for_line.py`. Never poll. Purposes name the side, the run, the
  grid and the tree.
- After each side's builds: `./venv/bin/python -m runregistrykit store create --purpose …` from that side's root, so
  that the sidecar's fingerprint is its tree's.
- **The bound.** If a side has not ended three hours after its launch, stop and ask. Do not kill it unasked.
- **Record per run:** id, slug, pid, state, exit status, launch and end, wall, the load at launch, and `git_head` and
  `git_dirty` from its manifest. The new side's manifests record `git_dirty` true while the user's uncommitted files of
  the orchestrator note are in the tree. Record it; it is not a stop.

**The exports.** For each side, from that side's tree root, with the cluster up:
- `plot_by_beta.py --no-plots` into `$L/export/<side>/`, with the options that match the side's builds, as the run's
  post-build script did;
- `plot_ScalarHistory.py --no-plots` into the same root, for every row, so that each history has `fields.csv` and
  `bounces.csv`.
Say what options each needed, and why. A plot script that refuses (exit 2) is not worked around by editing it: find the
options that match the build, and if none do, stop and ask.

Then run the probe from the main checkout a last time, and stop the cluster with `ray_cluster.sh stop`. Record its
`status` and the lock's.

**The old side against the run (P17).** `$D/effect/compare_with_run.py` matches every row of the old side's
`histories.csv` to its twin in the run's export: `science-runs/2026-11-08-science-2026.10.0/fig6/inputs/histories.csv`
(the main store's) or `var/science-2026.10.0/out/reflections/histories.csv` (for `r`). Match on β, M, n, φ*, α and
`drain_model`, M to a relative 10⁻¹² (the stored M is converted, so 10⁻⁴² reads back as 9.999999999999999e-43). Every
other column must be equal as written. Report the count matched and every difference. **Any difference is a stop.** It
would mean the old side is not the run's code and inputs, and the comparison would not be the change a rebuild makes.

**The comparison (P18, P19).** `$D/effect/compare_sample.py` reads the two exports and writes `$D/effect/out/effect.csv`
(one row per history) and `effect.txt`:
- per history, old and new and their difference, absolute and relative: `Yp_BBN`, `D_over_H`, `Delta_N_eff_today`,
  `Delta_N_eff_1MeV`, `T_deliver_GeV`, `reflections`, `injections`, `bounce_rows`, the `candidates_*` and
  `drained_share_*` columns, `parking_T_GeV`, `release_T_GeV`, `tuning_iterations`, and `failure_reasons`;
- the histories whose T_deliver lies on opposite sides of 10 GeV on the two trees, listed with β, M and row. 10 GeV is a
  label between the QCD deliveries (about 1–2 GeV) and the EW ones (about 30–60 GeV), not a test;
- per history, the two `bounces.csv` aligned by ordinal: the counts, the first ordinal whose `kind` or `source`
  differs, and over the rows before it the largest relative difference in `N`, `log_T_Jordan_GeV` and `alpha_eff`;
- by block, the largest and median |change| of each observable.

A history that fails on one side and not the other is recorded with both reasons. One whose new-side failure reason is
not among the kinds the run record lists ("Failed histories") is a stop.

**Beside the figures.** For each figure of the run, state from its `NOTES.md` what it plots (which observable, against
what, at which M and rows) and the range of its axes, and set the sample's changes in that observable beside them. Say,
for each, which of the sampled histories lie on it, and how far each moved in the figure's own units. Do not decide
whether a figure "stands": state what moved and by how much. That is the user's to judge (README §4).

### 1.5 The verification document

`.documents/electroweak-eos-verification.md` gains **§2** (02, dated): the old tree and its checks (§1.1); the
threshold (§1.2) with its table of fine-grid rows and the comparison of edges; the bath drift (§1.3); the sample
(§1.4): the cluster and the probe, the builds' table, the exports, the old side against the run, the comparison by
history and by block, the bounce rows, and the figures. §1 is not edited (invariant 6).

## 2. What is not wanted

- **No change to code.** Not the integrator, the table, the drivers, the plot scripts or `surfing_scan.py`. A script of
  this prompt imports production code and calls it. It never patches it, except the CSV redirect of §0.
- **No change to any test**, including the surfing tests' reference rows: whether they should read S1-new is 03's
  (log 01 §6).
- **No notebook-side threshold** (U5).
- **No write to the run's stores.** They are read-only, through the run's exports. Where a store must be read
  directly, use sqlite's `mode=ro` URI.
- **No `ray stop`, no cluster but ChamPBH's isolated one, no `--ray-address auto`.**
- **No fix** to anything the measurements find, including `[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]`
  (`surfing_scan.py`'s `n_crossings` counts the crossings it describes; its capture columns read the first crossings,
  which are resolved). Record and open, per invariant 4.
- **No judgement of the figures** (§1.4, "Beside the figures"), and no bound on any change (README §5).

## 3. Verification

- The suites once, after the measurements, serially, ComputeTargets in two halves, the long ones in the background.
  The counts must equal 01's (README §9.2 changes no file the suites import; show it with `git diff --name-only 5947b4f`
  before the commit). Record counts, times and `uptime`.
- `tools/build_w_table.py --check` exits 0 at the end.
- The Threshold gate and the Effect gate (README §2.1), item by item.
- **The breakage record:**
  - the code probe exits 1 when a worker's view differs: run it once from the main checkout with the CSV path it
    reports for workers pointed at the old copy (an environment variable or an option of the probe's own; never by
    changing the installed CSV), and show it fails;
  - `compare_with_run.py` reports a difference when one cell of the run's export is perturbed in a scratch copy;
  - `bath_drift.py` on the old table reproduces the issue's figures, which stands for its check.
- `black` on the Python files you add.
- At the end, `runregistrykit list` shows every run of this prompt `done`, and `ray_cluster.sh status` shows no process.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `$D/threshold/` (the CSVs, their sidecars, the figures), `$D/bath/` and `$D/effect/` (scripts, and `out/` as text and
  CSV only);
- `.documents/electroweak-eos-verification.md` (§2);
- the `inelastic-reflection` board's entry for `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]`: a dated addendum
  with §1.3's numbers on both tables, naming this campaign. The issue stays open and its index row is unchanged, unless
  the measurement narrows it, in which case the row is updated in the same commit;
- the log `logs/02-measure-the-effect.md` (template: `dk-and-rpk-migration` README §5.1), with every deviation
  classified, "Observations not acted on", and "State handed to the next prompt". The last includes what 03 needs: the
  threshold, the window, the per-block changes, the histories that changed their kind of delivery, and the stores and
  exports by path;
- the board: row 02, its items, the Threshold and Effect gates, §3/§4 for any issue opened, with
  `.documents/OPEN_ISSUES.md` in the same commit.

Nothing under `var/` is committed. The stores stay, as the run's do: 03 says whether they are kept.

Stage by explicit path. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the
orchestrator substitutes it.
