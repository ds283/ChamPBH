# Plan — the `2026.14.0` science run

**Drafted 2026-11-19**, on `main` at `29425ec`, after `power-law-wall` closed and was merged (`53130e9`). It replaces
the `2026-11-13` run of `2026.11.0` ([`2026-11-13-science-2026.11.0/`](../2026-11-13-science-2026.11.0/README.md))
with a run of both potentials: the exponential at n = 1, on 2026.11.0's grid under the refitted one-loop law, and the
inverse power law V = V₀[1 + (M/φ)ⁿ] at n = 1 and 2, on that grid without the blocks that do not fit its wall. This
folder holds the plan only. The probe's script and the run script are written once §0.2 is ruled, since where the run
goes (P5) changes both. Nothing has been launched.

The tree: `VERSION_LABEL` `2026.14.0` (`config/version.py:134`), `FINGERPRINT_FORMAT` 8 (`config/fingerprint.py:100`).
The tip does not read a format-7 store, so it cannot read 2026.11.0's stores (`review-remediation-verification.md`
§4.22 point 1). The exponential is therefore rebuilt, not re-exported, and the comparison with 2026.11.0 reads its
`histories.csv`. A copy of that file is committed at `2026-11-13-science-2026.11.0/fig6/inputs/histories.csv`, so the
comparison needs no store and works on any host.

What changed for a build since 2026.11.0 is §4.22 point 1. In short:
- the exponential's one-loop rows are `one-loop-exponential-v2`, and the power wall's are `one-loop-inverse-power-v1`;
- there is one k_ex, ½E^{1/2}|V‴/V″|;
- the spectrum is a measured table per wall (`R8-<wall>-v2`);
- every potential is exported under `<output>/<label>/<model>/`, and `histories.csv` has a `model` column.

## 0. Rulings and proposals

### 0.1 Ruled by the user, 2026-11-19, in their words

- **The exponential.** "we only want the exponential at `n=1`". `--n-values` is one list for every potential of a run
  (§4.22 point 3), so the exponential's blocks and the power law's are separate `main.py` invocations (P3).
- **Block C.** "drop block C on the inverse power law wall". On that wall the first impact is one-loop at every
  M ≤ M_P (§4.22 point 4), so it has no M_crit.
- **Block D.** "drop block D on the inverse power law wall". At n = 1, natural mode at large M fails at G2 (R5).
- **The exponential's block B.** "extrapolate the drain law for the exponential block B". Law v2 was fitted on
  ln(V₀/M⁴) ≤ 10 (`power-law-wall` README §6.13, U13). B's masses, 10⁻³⁰ to 10⁻⁴² M_P, reach past that domain: the one
  history measured at 10⁻³³ M_P had 47 one-loop candidates beyond it, up to 27.3 (verification §11.11). The law is used
  there as it stands. P4 says how the record marks it.
- **A probe.** "probably worth planning a prove run for the inverse power law wall". The probe is §3.
- **The analyses.** "the analysis scripts need fixing, as you point out". The ports are §5.
- **Where it runs.** Not ruled; raised by the user: "Something we should consider is running the analysis on the Mac
  Studio that is available as an SSH environment. The SecondaryGWKit project already does this. We would need to
  interleave the runs so that the machine is not being used by both at the same time, but the ray cluster lock exists
  for that reason." P5–P7 propose how.

### 0.2 Proposed, then ruled as proposed

Ruled by the user, 2026-11-19, in their words: "Let's go with P1–P14 as proposed. Drop R-natural and R-Mcrit on the
inverse power law wall. Panel (b) of the trend in M should show ln R, as you propose." So P2 is ruled without its
alternative (the reflection store holds 84 histories), and §5's new choice is ruled as proposed. The user added, of
§6's assumption: "The Mac Studio is slightly faster per core than this laptop."

- **P1, the exponential's grid is 2026.11.0's.** Every block, M, α row, A row, β grid, φ\*, tolerance and option is
  2026.11.0's (its PLAN.md §1 and run script), at n = 1: A, B, D, R-lab, R-ladder, R-natural, C and R-Mcrit. That is
  4818 histories in the main store and 44 reflections. Each history has a counterpart in 2026.11.0, so the comparison
  (P14) is one join.
- **P2, the power law's grid is the exponential's, less C and D.** Blocks A, B, R-lab and R-ladder at n = 1 and 2, with
  the exponential's M, β and rows.
  - **Proposed:** R-natural and R-Mcrit are dropped as well. They are the reflection-store counterparts of D and C, and
    R-natural at n = 1 fails as D does (R5).
  - **The alternative** is to keep R-natural at n = 2 alone: 6 histories, which succeed with 4–59 reflections (07d).
  - The power law then has 8904 histories in its main store and 40 reflections (§1).
- **P3, the stores.** Three stores under the Mac Studio's `var/datastores/` (P5), each with its sidecar made after its
  first block, as before:
  - `science-2026.14.0-exponential.db`, the exponential's main store;
  - `science-2026.14.0-inverse-power.db`, the power law's main store;
  - `science-2026.14.0-reflections.db`, the reflection store, holding both potentials.

  The rehearsals 07 and 07d measured a store holding both potentials and found it equal to separate stores, history for
  history and in its exports (verification §6.7, §11.6), so separate stores lose nothing. They also gain three things:
  - each store can be copied, fingerprinted and retired on its own;
  - no block of one potential writes into the other's store;
  - each potential's exports read only its own store.
- **P4, how the record marks B's extrapolation.** No change to the code.
  - The probe (§3) counts, at each of B's masses, the exponential's one-loop candidates that lie outside
    ln(V₀/M⁴) ≤ 10, and the run's README records the counts.
  - Every analysis that draws the exponential's one-loop rows below 4 × 10⁻³¹ M_P says in its NOTES.md that those rows
    use law v2 beyond its fitted domain, by the user's ruling.
  - The trend in M already marks a similar line for ℓ (its panel (b)), and its port marks this one the same way.
- **P5, the run goes on the Mac Studio; its records stay on the laptop.** This follows SecondaryGWKit's arrangement.
  - **What runs where.** The Mac Studio (`mps026298`: 10 cores, 32 GB, sleep disabled) runs the probe, the build, the
    inventories, the exports and the measures. The laptop runs the analysis scripts on copied inputs and commits the
    record, as SecondaryGWKit does.
  - **Why.** At about 50 h (§6), the run is longer than the laptop can be relied on to stay awake, and the laptop
    sleeps when moved ([[machine-sleep-and-timing-tests]]). It also has 16 GB of memory and 64 GB of free disk.
  - **CPUs.** The cluster starts with `tools/ray_cluster.sh start "science-2026.14.0" --num-cpus 6`. SecondaryGWKit runs
    with 6 because the Mac Studio also serves a website's object store.
  - **Amended 2026-11-20: 8 CPUs.** The user ruled: "It will be running over a weekend. I think it's safe to take 8
    cores." The cluster starts with `--num-cpus 8`, which leaves 2 for the object store (the user, 2026-11-19: "2 cores
    is adequate for `minio`+`nginx`"). `CategoriesService`'s loop has been stopped, and the Mac Studio was at load 3
    before the suites ran on 2026-11-20.
  - **Orchestration.** A laptop session starts a session over the desktop app's saved SSH connection `MacStudio`. That
    session drives the Mac Studio, waits on the run itself and commits nothing; the laptop copies the records back.
  - **The Mac Studio's checkout** is used only to build. It stays clean and is never moved while a build runs.
  - **The exports' CSV files stay on the Mac Studio.** Only what the analyses read comes back: `histories.csv`, the
    quanta summaries, and the few histories' bounce and sample files the analyses name (2026.11.0's inputs came to
    11.5 MB).
- **P6, preparing the Mac Studio.** As found on 2026-11-19 (read over `ssh macstudio-tunnel`, nothing changed):
  - the ChamPBH checkout is at `edf72b3` (2026-11-08) on remote `origin` (the same GitHub repository), with an untracked
    `PeakEWsurf.nb`;
  - its venv dates from December 2025 and has no `raypoolkit`;
  - there is no `var/`;
  - the disk shows 96 GB free of 926 GB (90% used);
  - the load is about 8, almost all of it macOS: `mediaanalysisd` at about 3.5 cores, running for 10 days, plus
    Spotlight.

  The preparation, in order:
  1. The laptop pushes `main` to `github`, at the user's word. One commit (`29425ec`) is not yet pushed, and this plan's
     commit will make two.
  2. The Mac Studio's checkout is fast-forwarded to the launch SHA.
  3. `PeakEWsurf.nb` is the user's file. Left as it is, it makes the tree dirty, and the run script refuses a dirty
     tree. **Proposed:** the user adds it to that checkout's `.git/info/exclude`, which is local and never committed, or
     moves it.
  4. The old venv moves to `~/Documents/Code/retired-venvs/`, as SecondaryGWKit's did. The new one is built with
     MacPorts' `/opt/local/bin/python3.13 -m venv venv` (3.13.16, as on the laptop) and
     `./venv/bin/pip install -r requirements.txt`.
  5. `~/.ray/auth_token` is checked to be at mode 600, without being printed. SecondaryGWKit's clusters already use it.
  6. The five suites run there, and their counts must equal the laptop's (26, 809, 175, 312, 79). They must also run
     clean of failures other than the timing failures already ruled as such.
  7. `tools/build_w_table.py --check` passes on the w table.
  8. The user confirms that automatic macOS update installs are off for the run's days.
  9. `mediaanalysisd` takes about a third of the machine. Whether to pause it (Photos' analysis) is the user's call.
- **P7, sharing the Mac Studio with SecondaryGWKit.**
  - **What the lock covers.** The rehearsal lock (`~/.raypoolkit/rehearsal-lock`, one per host) keeps the two projects'
    Ray clusters apart. ChamPBH's `ray_cluster.sh` takes it with the same contract SecondaryGWKit's uses (RayPoolKit
    `v0.3.0`).
  - **What it does not cover.** SecondaryGWKit's recent Mac Studio work was Ray-free and took no lock
    (`levin-phase-floor` 07c's runs say so in their scope). So the lock alone does not stop the two projects overlapping.
  - **Before each window** (the probe, then each part of the run) the session checks three things on the Mac Studio:
    the lock is free (`status`), SecondaryGWKit's registry shows nothing `running`, and ChamPBH's shows nothing
    `running` or stale. Exit 3 from the lock is a stop for the user, as on the laptop.
  - **Yield points.** The run script's slots are where the machine can be handed back. The script and the cluster can
    be stopped at a slot boundary, and the run resumes with `START_AT=<slot>` (as 2026.11.0's script did). P13 orders
    the slots so that the natural breaks fall between potentials.
- **P8, the probe before the launch** (§3). It checks two things on the Mac Studio, the first before the second:
  - **Part 1:** the Mac Studio builds what the laptop built.
  - **Part 2:** the power law's grid behaves where nothing has measured it.

  The run is launched only after the user has seen what it found and ruled the grid on it.
- **P9, the analyses are ported before the launch, on the probe's export** (§5). Then the run script's exports are
  tested against the scripts that read them, and the analyses can run as soon as the build ends. Each potential's wall
  gets its own Figure 6, relativistic-quanta figures and trend in M, with the same layout and rulings
  ([[treat-potentials-symmetrically]]). Figure 6b (around M_crit) and the comparison with 2026.11.0 are the exponential's
  alone, since only it has block C and a predecessor run.
- **P10, one export root per wall.** Each wall is exported into its own `--output`:
  - the exponential at n = 1;
  - the power law with `--n-values 1`;
  - the power law with `--n-values 2`.

  Each gets a `plot_by_beta.py` pass and a `plot_ScalarHistory.py --no-plots` pass of its one-loop rows. Then
  `tools/quanta_measures.py` (at c_a = 1, 0 and 10) and `tools/plot_quanta_grid.py` run on each root. A root that holds
  only one n never meets `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`. It also needs no copy made by
  `close-08/split_by_n.py`, which would double the power law's exports on disk. The reflection store is exported the
  same way, into one root per wall.
- **P11, the disk.** The launch condition is set from the probe's measured sizes. Until then it rests on 2026.11.0's,
  as a projection (§7).
  - **Proposed:** each wall's exports and measures run before the next wall's export starts. Once a wall's measures
    have run, the script compresses that wall's per-history `fields.csv` files with `gzip`. No analysis reads them
    afterwards: they make up about 10 of 2026.11.0's 18 GB of exports, and they compress about 2.4×.
  - The script keeps 2026.11.0's two disk gates: 55 GB free to start a build slot and 70 GB free to start the exports.
    The Mac Studio's disk is the same size as the laptop's, so Ray's 95% limit is again about 46 GB free.
- **P12, as 2026.11.0** (its P6). There are no Datastore tags for purpose. Every `main.py` block is registered as
  `--campaign science-2026.14.0 --prompt <slot>`, and the probe's as `--prompt Q`. The run script is committed in the
  run's folder before the launch and never edited afterwards, and it refuses a dirty tree.
- **P13, the order of the slots.** The script runs `A-exp A-pl B-exp B-pl D-exp R C-exp X`:
  - **A first.** Every wall's Figure 6 inputs are complete first, after about 20 h on the Mac Studio (§6).
  - **R** holds both potentials' reflection blocks.
  - **C-exp** holds C-Mcrit and R-Mcrit, last, as before: its tunings at M_crit are the slow ones.
  - **X** runs the inventories, the exports and the measures.
  - **The yield points of P7** fall after A-pl (about 20 h), after B-pl (about 41 h) and before X.
- **P14, the comparison with 2026.11.0 is the exponential's.** `changes/changes.py` joins on
  `M_Mp, beta, drain_model, alpha` (§4.22 point 3), and `drain_model` differs between v1 and v2.
  - **Proposed:** the port joins on (M, β, law class, strength), where the law class is `fixed` or `one-loop`. That maps
    `one-loop-measured-v1` onto `one-loop-exponential-v2`.
  - Its report then says what v2, one k_ex and the new spectrum tables moved, beside 07d's "ΔN_eff today −0.36% to
    −6.70%" on eleven histories (verification §11.5).
  - The power law has no predecessor run, so nothing is compared for it.

## 1. What is sampled

Every history has φ\* = 5 M_P, T\* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, `--tuning-max-integrations` 30,
atol = rtol = 10⁻⁸, 16 shards and `--bbn-wall-clock-limit 600`.

- **A rows:** 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10.
- **α rows:** 0.01 and 0.1, plus 0 (natural mode) on D-exp and R-natural only.
- **β grids:** 2026.11.0's, made by its script's `G`. Dense has 191 values (fine segment 2.950–3.040), sparse 30 and
  coarse 62; the R blocks use 1.2, 2 and 2.9.

| slot | block | store | potential, n | M (M_P) | β | rows | histories |
|---|---|---|---|---|---|---|---|
| A-exp | A | exponential | exp, 1 | 4e-31 | dense | α 0.01, 0.1; A all 10 | 2292 |
| A-pl | A | inverse-power | pl, 1 and 2 | 4e-31 | dense | as A-exp | 4584 |
| B-exp | B | exponential | exp, 1 | 1e-30, 1e-32, 1e-33, 1e-36, 1e-40, 1e-42 | sparse | as A-exp | 2160 |
| B-pl | B | inverse-power | pl, 1 and 2 | as B-exp | sparse | as A-exp | 4320 |
| D-exp | D | exponential | exp, 1 | 1e-3, 0.1, 0.5 | coarse | α 0 | 186 |
| R | R-lab | reflections | exp 1; pl 1, 2 | 4e-31 | 1.2, 2, 2.9 | α 0.01, 0.1; A 0.05, 1, 10 | 15 + 30 |
| R | R-ladder | reflections | exp 1; pl 1, 2 | 1e-16 | 2 | α 0.01, 0.1; A 0.1, 1, 10 | 5 + 10 |
| R | R-natural | reflections | exp, 1 | 1e-3, 0.5 | 1.2, 2, 2.9 | α 0 | 6 |
| C-exp | C-Mcrit | exponential | exp, 1 | 1e-15, 1e-12 | sparse | α 0.01, 0.1; A 1 | 180 |
| C-exp | R-Mcrit | reflections | exp, 1 | 1e-15, 1e-12 | 1.2, 2, 2.9 | α 0.01, 0.1; A 1 | 18 |

The exponential's store holds 4818 histories, the power law's 8904 and the reflection store 84 (44 exponential, 40
power law): 13 806 in all. Under P2's alternative, the reflection store holds 90.

**What is unmeasured on the power wall** (§4.22 point 4). At laboratory M, 188 of the 191 dense β have not been run,
among them the surfing region 2.95–3.04, and 7 of the 10 A rows. Of B's masses only 10⁻³³ M_P was run, at one β. No
history has reached 10⁻³⁶–10⁻⁴² M_P, nor R-ladder's 10⁻¹⁶ M_P. At n = 1 the surfing scan at 10⁻³ M_P found the QCD
onset only within (1.10, 1.30] and no EW edge (R11). The probe is for these gaps.

## 2. Where it runs and how it is recorded

On the Mac Studio, from its ChamPBH checkout's root, after P6:

1. **The plan, the probe's script and the run script are committed** on the laptop at the user's word, and pushed. The
   Mac Studio's checkout is fast-forwarded to that SHA. At the launch the folder is renamed
   `science-runs/YYYY-MM-DD-science-2026.14.0/`, with the date the run starts, in a commit of its own, which is pushed
   and fast-forwarded the same way.
2. **Discovery** on both hosts (CLAUDE.md, rule 1), and P7's checks on the Mac Studio.
3. **The cluster** is started on the Mac Studio from the checkout's root:
   `tools/ray_cluster.sh start "science-2026.14.0" --num-cpus 8` (P5, amended 2026-11-20). Workers read the w table and PRyMordial's rates by
   paths relative to the directory the cluster started in. Exit 3 is a stop for the user.
4. **The launch**, detached, then one check that `A-exp` started, and nothing more until it ends (rule 4):
   ```bash
   mkdir -p var/science-2026.14.0
   nohup bash science-runs/<folder>/run_science_2026.14.0.sh > var/science-2026.14.0/run.out 2>&1 &
   ```
   Slot C-exp's pipeline can be silent for longer than the registry's 900 s window. Read it with
   `runregistrykit list --stale-after 4000`.
5. **While it runs**, nothing moves the Mac Studio's checkout. The laptop's checkout is free: campaign work can go on
   there, since nothing on the laptop builds from it.
6. **Afterwards**, on the Mac Studio:
   - `tools/ray_cluster.sh stop`;
   - `store fingerprint --write` where a sidecar was made after its store's last run (CLAUDE.md, "Fingerprints and
     retirement");
   - the analyses' inputs copied back.

   On the laptop:
   - the registry's manifests and statuses copied into `registry/runs/`, and the sidecars into `registry/stores/`;
   - the analyses run (§5);
   - the README written;
   - the run's row added to `science-runs/README.md`, with 2026.11.0 marked superseded. The standard analyses' rows
     name their new folders and their per-wall roots (convention 8).

## 3. The probe

The probe runs on the Mac Studio, registered as `--campaign science-2026.14.0 --prompt Q`, into stores of its own. It
is recorded in the run's folder under `probe/`, as 2026.8.0's was.

**Part 1, the same tree on another host.** It rebuilds 07d's two stores of the tip, `power-law-wall-07d-r2-exp`
(runs a, b and c, 20 histories) and `-pl` (runs a, b and c, 40 histories). The commands are 07d's, from
`.documents/power-law-wall/rehearse-07d/out/07d-commands.txt`, with `--database` and `--job-name` changed. Then
`tools/compare_stores.py` compares each rebuild against a copy of the laptop's store (about 280 MB, copied across),
with the user's mask for `ScalarHistory.failure_reason` (`[07-failure-reason-carries-the-run-label]`).
- **Expected:** equal bit for bit, as 07d's rebuild of 07 was outside the k_ex columns.
- **Otherwise:** the differences are reported by value, column by column, and the probe stops for the user before
  Part 2.
- **Cost:** about 10 minutes.
- **Amended 2026-11-20: the exponential's reference is rebuilt on the laptop.** `power-law-wall-07d-r2-exp` was
  retired on 2026-11-19 at 16:10, so its sidecar and fingerprint remain but not its shards; `-r2-pl` is kept. The
  user ruled (b): "(b) please." The laptop first rebuilds the exponential store from the same three commands, into
  `science-2026.14.0-probe-ref-exp`, registered as `--prompt Q`. That rebuild's fingerprint is compared with the
  retired sidecar's (the tree has changed only in records since `00fb718`). The Mac Studio's rebuild is then compared
  by value against it, as against `-r2-pl`.
  - **Done 2026-11-20** (`probe/ref_exp_laptop.sh`, 17:59–18:07, runs `science-2026.14.0-Q-probe-ref-exp-{a,b,c}`,
    all `done`, tree `5482e13` with uncommitted records): the rebuild's fingerprint is `17b288e7…c14c`, equal to the
    retired sidecar's, and equal class by class (`probe/logs/probe-ref-exp-compare.log`).

**Part 2, coverage of the power wall.** At each mass of P2's grid it runs a sparse sample of β and rows, at both n:

- **M** (M_P): 4e-31, 1e-30, 1e-32, 1e-33, 1e-36, 1e-40, 1e-42 and 1e-16 (eight masses);
- **β:** 0.5, 1.0, 1.1, 1.2, 1.5, 2.0, 2.9, 2.975, 3.0, 3.04, 5 and 20 (twelve values, spanning the dense grid's
  segments and the surfing region at laboratory M);
- **rows:** α 0.01, A 0.05, 1 and 10;
- **n:** 1 and 2.

That is 768 histories. A further 72 exponential histories at B's six masses, the same β and A = 1 alone, check law v2
where it is extrapolated, and give P4's counts. The probe reports, per wall, M, β and row:
- whether the history succeeded, and the failure reason where it did not;
- the number of tuning integrations, and where the tuning cap was reached;
- the wall time from each block's log;
- the store's size per history, for §7.

From these it gives a measured ratio of Mac Studio to laptop per history, from Part 1's histories against 07d's costs.
That ratio replaces §6's assumed one.

- **Cost:** about 2.5 h on 6 CPUs, at 2026.11.0's 63–69 CPU-seconds per history for A and B. This is a projection.
- **No thresholds.** The probe sets no threshold and fixes nothing; what it finds is for the user. Grid changes it may
  prompt are the user's to rule before the launch: for example, a block dropped or thinned, or another β segment at
  n = 1.
- **Its exports** go into P10's per-wall roots, and the measures run on each. This is the run script's export
  section, run once in advance, and its output is what §5's ports are developed on.
- **Its stores** are kept until the ports are done. They are retired at the run's record (CLAUDE.md, "Fingerprints and
  retirement").

### 3.1 What the probe found (2026-11-20/21)

It ran on the Mac Studio at `075bbd1`, 8 CPUs, from 18:11 to 23:43 on 2026-11-20 (`run_probe.sh`). Its 20 registered
blocks are all `done`. The records are in `probe/`:
- `probe/macstudio/logs/` and `out/`: the block logs, the Part 1 comparisons, the per-history listings and
  `p2-summary.txt`;
- `probe/registry/`: the runs' manifests and statuses, and the stores' sidecars.

Measurements only; nothing is judged against a threshold.

- **Part 1: equal.** `compare_histories.py` finds the Mac Studio's rebuilds equal by value to the laptop's references,
  exponential and power law (exit 0 for each). The block walls at 8 CPUs total 11.6 min, against 12.5 min for 07d's on
  the laptop at 10 CPUs. That is about 1.3 times the laptop's speed per CPU, measured on blocks of 6–40 histories, so
  it may not hold for long blocks.
- **Part 2, the power wall: 768 histories, 107 failed.**
  - β = 0.5: 48 failed, every row at every M, "the effective minimum was not found". The exponential's 6 at β = 0.5 fail
    the same way, and 2026.11.0's Figure 6 marks β < 1 as having no BBN result.
  - β = 20: 40 failed. 32 are "the classical turning point search left float range" and 4 are an overflow in
    `InversePowerModel`'s (ln V)''. The exponential has no failure at β = 20.
  - β = 1.0: 15 failed; β = 1.1: 3; β = 1.2: 1. Of the 107, 7 ran out of step budget and 1 reached the tuning cap (at
    10⁻¹⁶ M_P).
  - No other β failed, including 2.9–3.04 and 5.
- **Part 2, the exponential at B's masses (law v2 extrapolated): 72 histories, 6 failed**, all at β = 0.5 and in the
  same way. P4's count of one-loop candidates outside ln(V₀/M⁴) ≤ 10 is not yet taken: `summarise.py` does not read
  the bounce rows.
- **Cost.**
  - Most power-wall histories took about 17 s from submission to storage (the median at each M).
  - Each block had a tail of single histories at 500–1000 s. At 10⁻¹⁶ M_P one history took 8094 s, and that block
    took 2.5 h of the probe's 5.3 h.
  - Block walls times 8 CPUs, divided by histories, bound the cost at about 127 CPU-s per history at laboratory M and
    34–195 CPU-s per history at B's masses. The bound is high, because in blocks of 96 histories the tail sets the wall.
  - At that bound, A-pl is about 20 h and B-pl about 17 h at 8 CPUs. The exponential's blocks, at 2026.11.0's cost
    and Part 1's ratio, come to about 14.5 h. The build's upper bound is therefore about 51 h, against §6's 37 h.
- **Disk.** The power law's store is 1.90 MB per history (1.462 GB for 768), against §7's assumed 1.95, so §7's
  launch condition of about 120 GB free stands. The Mac Studio had 152 GB free afterwards.
- **The export test (P9)** was the run script at `c147a2d`, `START_AT=X`, on the Part 1 stores, one wall at a time
  (`probe/macstudio/export-test/`). It ran clean on all three walls:
  - every step exited 0, and no measure exited non-zero;
  - each wall's main and reflection roots were written and its `fields.csv` gzipped;
  - each power-law root held only its own n.

  The same test on the laptop's references passed earlier the same evening. Its walls `pl1` and `pl2` were rerun with
  `ALLOW_DIRTY_TREE=1`, because the untracked analysis ports dirtied the laptop's tree.

**Added 2026-11-21**, at the user's request ("Take the P4 counts from the probe's bounce rows. Did the `beta = 20`
histories take a long time to fail?"). The user also said of the cost bound: "the cost is just the cost. If it's 51h,
that's what it is."

- **P4's counts** (`probe/p4_counts.py`, output `probe/macstudio/out/p4-counts.txt`).
  - **Method.** ln(V₀/M⁴) was taken for every one-loop candidate row of the probe's exponential stores, directly as
    ln E − ln(e^L − 1) − 4 ln M. It agrees with 07b's formula from (L, ln R) to 7 × 10⁻¹², over 3575 candidates.
  - **It depends on M alone.** It is one value per history, the same at every β and every row, and the same for every
    candidate of a history.
  - **Its values**, against the law's fitted domain ln(V₀/M⁴) ≤ 10:

    | M (M_P) | ln(V₀/M⁴) |
    |---|---|
    | 10⁻³⁰ | −0.32 |
    | 4 × 10⁻³¹ | 3.34 |
    | 10⁻³² | 18.10 |
    | 10⁻³³ | 27.31 |
    | 10⁻³⁶ | 54.94 |
    | 10⁻⁴⁰ | 91.78 |
    | 10⁻⁴² | 110.20 |

    10⁻¹⁵ M_P gives −138.5.
  - **So, of block B's six masses,** 10⁻³⁰ M_P lies inside the fitted domain. At the other five, every candidate lies
    outside it: 3080 of the probe's 3575 at B's masses. The furthest is at 10⁻⁴² M_P, eleven times the domain's edge.
    The figures' P4 marking (§5) shades all of M < 4 × 10⁻³¹ M_P, which includes 10⁻³⁰ M_P, inside.
- **The time to fail at β = 20** (`probe/fail_times.py`, output `probe/macstudio/out/p2-times-by-beta-pl.txt`).
  - **They fail quickly:** median 7.9 s and at most 54 s from submission to storage. The 40 together took 0.14 h.
    The 24 successes at β = 20 took a median of 65 s.
  - **Which fail.** Every n = 1 history at β = 20 fails, at all eight masses and all four rows (32): "the classical
    turning point search left float range at ln φ = −1021". At n = 2 only 10⁻⁴⁰ and 10⁻⁴² M_P fail (8): an overflow
    in (ln V)'' or a non-finite turning point. The six other masses succeed at n = 2.
  - **The costly failures are elsewhere.** At β = 0.5, 48 failures took 2.0 h, with a mean of 151 s and up to
    1016 s. At β = 1.1, 3 failures took 2.4 h, one of them 8094 s. At β = 1.0, 15 failures took 0.85 h.
- **Ruled 2026-11-21: the grid is unchanged.** The user: "Keep beta = 20 on the power wall's grid." No other grid
  change was asked for, so P2's grid stands as §1 gives it, and the run script needs no change.

## 4. The run script

The script is 2026.11.0's, with these changes:
- **The stores** are P3's.
- **The blocks** are P13's slots, each a `run()` with its own `--config`: `exponential.yaml` for the exponential's
  blocks, and `inverse_power.yaml --n-values 1 2` for the power law's. Each registered block's scope names its
  potentials (`models <values>`, `registered_run.py:684`).
- **`START_AT`** takes the new slot names.
- **The exports** are P10's per-wall roots, one wall at a time, with P11's `gzip` after each wall's measures.
- **The disk gates** are unchanged (P11).

The script is written after §0.2 is ruled. Its export section is tested on the probe's stores before the script is
committed (P9).

**Written 2026-11-20** (`run_science_2026.14.0.sh`, beside this file, uncommitted). A dry run with `run`, `inventory`
and `export_wall` stubbed gives §1's counts block by block: 4818, 8904 and 84, 13 806 in all. `START_AT` resumes at each
slot, `EXPORT_WALLS` at each wall, and a dirty tree, an unknown slot and an unknown wall are refused (exit 2).
- **Beyond §4's list:**
  - X can be resumed at a wall with `EXPORT_WALLS` (default `exp pl1 pl2`);
  - each wall's main and reflection roots are `out/<wall>/` and `out/<wall>-reflections/`;
  - `gzip` runs after that wall's `plot_quanta_grid` as well as its measures;
  - the power law's rows read no α = 0, since none is built for it;
  - the inventories read the reflection store once per potential.
- **The export test (P9):** `DB_EXP`, `DB_PL`, `DB_R`, `LOG`, `OUT`, `A_ALL` and `A_PLOT_R` can be set from the
  environment, and the run sets none of them. Once the probe ends, `START_AT=X` runs on the Mac Studio, one wall at a
  time:
  - `exp` with `DB_EXP` and `DB_R` both set to the probe's `-p1-exp` store, and `A_ALL` and `A_PLOT_R` set to
    `0.05 1 10`;
  - `pl1` and `pl2` the same way on `-p1-pl`;
  - `OUT` and `LOG` under `var/science-2026.14.0/probe/`.

## 5. After the build: the analyses

Each of 2026.11.0's five analysis folders is copied into the run's folder and ported. The changes, which §4.22 point 3
lists line by line:

| analysis | kind | per wall | the port |
|---|---|---|---|
| Figure 6 | standard | exp n1, pl n1, pl n2 | the one-loop rows chosen by the wall's own drain model in place of the retired `one-loop-measured-v1` (`fig6.py:52`, `:163`); rows filtered by `model` and `n`; `EW_BRACKET` set per wall from that wall's export and stated in NOTES.md (n = 2 matched the exponential at 10⁻³ M_P; at n = 1 there was no EW edge there) |
| Relativistic quanta | standard | exp n1, pl n1, pl n2 | the same drain-model change (`:61`); the join key (`:136`–`:145`) gains `model` and `n`; `HISTORY_PICKS` by the same rules within each wall; the power wall's figures state that x_* and the associated piece are carried over from the exponential, unmeasured on that wall (U5) |
| Trend in M | this grid's | exp n1, pl n1, pl n2 | the n in the bounce path (`:153`) taken from the wall; P4's marking for the exponential below 4 × 10⁻³¹ M_P; **panel (b) on the power wall** (below) |
| Figure 6b, around M_crit | this grid's | exp n1 | the drain-model change (`:42`, `:154`, `:160`); rows filtered by `model` |
| What moved since 2026.11.0 | P14 | exp n1 | P14's join key |

**One new choice, for the user.** The trend in M's panel (b) draws ℓ = ln(k_ex/m_ta) at the first impact, beside the
law's ℓ(M) solved on the exponential's potential. The power wall's law reads ln R, not ℓ.
- **Proposed:** on the power wall, panel (b) draws ln R at the first impact, as points only, for the same β.
- **The alternative** is to draw ℓ there too, as a diagnostic that the law does not read.

## 6. The cost — a projection, not a measurement

The basis is 2026.11.0's block walls on the laptop at 10 CPUs: A 4 h 00 min, B 4 h 09 min, D 23 min, R-lab 3 min,
R-ladder 14 min, R-natural 1 min, C 3 h 13 min, R-Mcrit 25 min. The power law's blocks use §4.22 point 4's
per-history ratios.

| | A | B | D | R | C + R-Mcrit | sum |
|---|---|---|---|---|---|---|
| exponential, n = 1 | 240 | 249 | 23 | 18 | 218 | 748 min, 12 h 28 min |
| power law, n = 1 | 236 | 241 | — | 17 | — | 494 min, 8 h 14 min |
| power law, n = 2 | 241 | 285 | — | 10 | — | 536 min, 8 h 56 min |

On the laptop that is about 29 h 40 min of building. 07d's exponential store took 2.5% longer under v2 than 07's
(301.7 s against 294.3 s), which is within the noise of this estimate.

On the Mac Studio at 6 CPUs it is **about 49 h** if one of its CPUs is as fast as one of the laptop's (×10/6). That
assumption is unmeasured, and it is what Part 1 of the probe measures. `mediaanalysisd`'s 3.5 cores (P6) would slow it
further.

The exports and measures took 58 min for 2026.11.0's one wall. Three walls have about three times its one-loop rows
(11 190 against 3770), which suggests **about 3–5 h** more at 6 CPUs. The probe takes about 3 h.

**At 8 CPUs (P5, amended 2026-11-20)** the same assumptions give about 37 h of building (×10/8 of the laptop's
29 h 40 min), about 2.5–4 h of exports and measures, and about 2 h for the probe's Part 2. These are still
projections; the probe replaces them.

**The weakest assumptions:** the power law's B, whose ratio rests on one history per n at 10⁻³³ M_P, with none at all
below it; and the CPU-speed ratio. The probe replaces both.

## 7. The disk — a projection, not a measurement

- **Stores.** 2026.11.0's two stores came to 9.46 GB for 4862 histories, about 1.95 MB per history. At that rate this
  run's three are about 27 GB: the exponential 9.4, the power law 17.4 and the reflections 0.2.
- **Exports.** 2026.11.0's main root was 18 GB, almost all of it the 3646 one-loop histories' per-history files (about
  5 MB each, `fields.csv` about 2.8 MB of that). The power law has 7420 one-loop histories in its main store against
  the exponential's 3770, so its two roots are about 37 GB, and the three walls about 55 GB raw.
- **With P11's `gzip`,** each finished wall shrinks to about two thirds of its size. The peak, during the last wall's
  export, is then about 27 + 12 + 12 + 18.5 ≈ 70 GB, and the end state about 64 GB.
- **What the Mac Studio needs.** Above Ray's floor of about 46 GB free, with margin, that is **about 120 GB free at the
  launch**, against 96 GB today. Without the `gzip` it is about 130 GB.
- **Where the room could come from:** SecondaryGWKit's scratch copies on the Mac Studio, its local snapshots, or a
  smaller export of B. On that last option: the analyses read B's per-history files at A = 1 only, but the plot scripts
  cannot export one block's masses at one A without exporting every block's, so that would mean deleting after the
  export.

The probe measures the power law's store size per history, which replaces the 1.95 MB assumed here, and the launch
condition is set from it.

## 8. What stays open

As 2026.11.0's README and §4.22 point 7 leave them, none acted on here:
- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`**, the one-loop tuning cap at M_crit, and the K
  check at C, 10⁻¹² M_P, A = 1: the exponential's, and carried into this run unchanged.
- **`[06-plot-quanta-grid-refuses-two-n-at-one-cell]`**: avoided by P10's per-wall roots, not fixed.
- **`[07-failure-reason-carries-the-run-label]`**: met by the probe's Part 1 mask.
- **`[03-plot-quanta-grid-beta-axis-is-categorical]`**, `[07-a-refused-plot-leaves-ray-errors-unretrieved]` and
  `[07d-a-reader-refuses-an-older-label-by-a-traceback]`: cosmetic.
- **The observations §4.22 point 7 lists** for a reader of the run. Among them: the power wall's surfing scans leave
  `T_max` blank and count `n_crossings` one lower where the first bounce is a reflection; and `ExponentialModel`'s
  comment says every history of record has ln(V₀/M⁴) ≤ 3.3, which B's do not.
- **M below 10⁻⁴²**: deferred by the user (2026-11-09).
- **The rehearsal stores** (`power-law-wall-07*`, `-07d*`, about 0.3 GB): theirs to retire, by §4.22 point 6.
