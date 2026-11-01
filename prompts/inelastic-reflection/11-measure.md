# Prompt 11 — the measurement and the rehearsal

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-10-31 to 2026-11-01, after 10 landed (`8c12afa`) and was reviewed (`5f7c3af`), at the user's word
("Please write prompt 11"). It is written from:
- README §8.3's U10 and U11 as ruled, and §8.4's charter for 11 with its gates;
- the user's three rulings of 2026-10-31, R29–R31 (README §8.1), and the planner's P40–P47 (§8.2);
- log 10 §8, "State handed to the next prompt": the option, the tools' lines, where each figure lands;
- `jordan-normalization` prompt 09, its log and its verification document §1: the rehearsal's protocol and
  commands, which this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: the lock free, no ChamPBH process, the
  ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/datastores/jordan-normalization-09-reference.db` and its 16 shards exist, and `runregistrykit store
  show` on it reports `problems: none`;
- neither `var/datastores/inelastic-reflection-11-reference.db` nor `…-resumed.db` exists, nor
  `var/inelastic-reflection-11/`, nor `.documents/inelastic-reflection/`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus**, as the plan has it (README §8.4). Real builds under real Ray, one
interrupted on purpose, the first run of the three drivers with α, and a measurement whose gate has numbers.

**Read first:**

1. [`README.md`](README.md):
   - §5 (the rules);
   - §8.0, §8.1 R6, R12, R13, R14, R17, R22, R24, R26–R31;
   - §8.2 P14 (in §6.2), P18, P22, P23, P40–P47;
   - §8.3 (U10, U11, and the user's word on run length), §8.4's charter for 11.
2. `CLAUDE.md`:
   - "Long-running jobs — the run registry" (the rules, the wrapper);
   - "Ray on this machine", "The rehearsal lock", "Ray's token mode";
   - "The plot scripts" (read-only, the matched options, exit 2).
3. [`logs/10-driver-and-readers.md`](logs/10-driver-and-readers.md) §8, whole: the option, the `history` and
   `dissipation` lines, `bbn_from_store.py --alpha`, the paths, the inventory.
4. `prompts/jordan-normalization/09-rehearse-a-tuned-build.md`, its log, and
   `.documents/jordan-normalization-verification.md` §1, whole. §1.14 holds the commands verbatim; follow them.
5. The rehearsal tools, run unchanged:
   - `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`, `interrupt_when.py`, `stage_counts.py`;
   - `tools/compare_stores.py`;
   - `jordan-normalization` 09's helpers, in `var/jordan-normalization-09/tools/` and committed as
     `.documents/jordan-normalization/out/09-*.py.txt`: `wait_for_line.py`, `ray_state.py`, `beat_times.py`,
     `silences.py`, `histories.py`, `compare_tool.py`, `check.sh`, `suite.sh`.
6. `tools/history_and_bbn.py` (its options, its lines, `dissipation_line`) and `tools/bbn_from_store.py`
   (`--sm-baseline`).
7. `ComputeTargets/ScalarHistory.py`: `PARKING_EPSILON` and the comment above it (:115–:128), and
   `_parking_cut`, which reads it at call time (:1142).
8. Logs 05 §5.5–§5.6, 06 §5.5–§5.6, 07b §5 and 09 §5.2: the drain, the cut at three ε, the tuning at small
   M, the stored values. These are the numbers this prompt's measurements are read against.
9. `source/scoping_2026-10-27.md` §7: the Standard Model exchange (U7).

---

## 0. Where this prompt sits

### 0.1 What it is for

01–10 built dissipation mode, stored it, and carried α through every driver and reader. Every test so far
ran without Ray, on temporary stores. This prompt does what the tests cannot:
- **the measurement of U10.** 48 tuned histories with BBN, by the tools, across the physical M, β and α,
  natural mode beside them, and the parking ε;
- **the rehearsal of U11.** A registered build under real Ray with two α, interrupted and resumed. It is the
  first run of `main.py`, `plot_by_beta.py` and `plot_ScalarHistory.py` with α (log 10 §4.4);
- **the record.** `.documents/inelastic-reflection-verification.md`.

12 closes the campaign. Nothing here changes code.

### 0.2 What the user ruled for this prompt (2026-10-31)

- **R29 — the grid runs as a detached batch.** A committed script runs each point as its own process, a few
  at a time. Each point's output goes to its own file under `var/inelastic-reflection-11/`, and points
  already done are skipped. The agent launches it detached, checks once that it started, and waits for it
  to end.
- **R30 — the ε scan patches the constant.** A committed measurement script sets `PARKING_EPSILON`, then calls
  the tool's `main` in the same process.
- **R31 — the rehearsal is `jordan-normalization` 09's run `a` at two α.**
  - The grid is M = 10⁻³ M_P, β ∈ {0.5, 1.5, 3, 5.5}, φ* = 5, with `--alpha-values 0 0.1`.
  - A second build is interrupted in the α = 0.1 scalar stage, then resumed.
  - The α = 0 histories are compared value by value with `jordan-normalization` 09's 2026.7.0 reference store.
  - Both plot scripts run on the store, an unstored α included.

### 0.3 What the planner found

- **The grid is about 25 minutes, not 1–3 h** (P40). Measured 2026-10-31:
  - The slowest point is natural mode at M = 10⁻⁷, β = 2. It took 129 s at load 7–21: one integration of
    113 s and 408 566 steps, converged on the seed, then 9 s of PRyMordial.
  - It printed P23's natural-mode warning (φ_t = 9.4×10⁻¹⁰ M_P) and reached T_CMB. Its values are
    Yp = 0.2465096843, D/H = 2.461463032, and residuals −2.5×10⁻⁶ and −4.2×10⁻⁸.
  - The dissipation points converge on the seed in 2–8 s (log 06 §5.6), and natural mode at M = 0.5 in
    about 20–40 s.
- **`PARKING_EPSILON` is read at call time** (`_parking_cut`, `ScalarHistory.py` :1142; orchestrator note 06,
  ruling 1). So setting the module attribute before `tune_history` runs changes the cut, and nothing else.
- **`jordan-normalization` 09's reference store is 2026.7.0.**
  - It was built on 2026-10-26 with the same `exponential.yaml`, tolerances, sampling and network that
    `main.py` uses today. Its run `a` stored four histories at M = 10⁻³, φ* = 5: β = 0.5, 1.5, 3 and 5.5.
  - Nothing between it and the campaign's base (`6f635e6`) changed a computed value: `tuning-followups`
    changed options and printed lines only.
  - So at α = 0 the rehearsal's histories should equal it bitwise in every column both schemas hold.
- **The banner names α** since prompt 10: `>> RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1`.
  `interrupt_when.py --banner` can then wait for the α = 0.1 pipeline, and `--after` for its scalar stage.
- **Dissipation at M = 10⁻³ parks, and at β = 1.2 is released** (log 07, log 09 §5.2). So the rehearsal's
  α = 0.1 histories exercise the parking and release, `AdiabaticHistory` on parked samples, BBN with ρ_χ, and
  `dark_radiation.pdf` with its lines. Whether β = 0.5 parks, or is released, at M = 10⁻³ has not been
  measured. The rehearsal reports what it does.

---

## 1. What is wanted

### 1.1 The measurement scripts (R29, R30, P41, P42)

Under `.documents/inelastic-reflection/measure/`, each with a docstring saying what it does, how it is run,
and that it writes only under `var/inelastic-reflection-11/`:
- **`points.py`**: the 48 points of P40 as a list, each with its slug, its command, and its kind (natural,
  dissipation or ε). Slugs read `nat-M0.5-b2`, `dis-M4e-31-b2-a0.1` and `eps1e-08-M4e-31-b2-a0.1`.
- **`eps_point.py EPS -- <tool arguments>`**:
  - sets `ComputeTargets.ScalarHistory.PARKING_EPSILON = EPS` before importing the tool;
  - prints `epsilon=<EPS:g>` as its first line, and asserts that the attribute holds EPS after the call;
  - calls `tools.history_and_bbn.main(argv)` and exits with its status.
- **`run_grid.py [--jobs N] [--only SLUG …]`**:
  - runs each point as its own process, from the repository root, `PYTHONPATH=.`, at most N at a time
    (default 3);
  - writes each point's stdout and stderr to `var/inelastic-reflection-11/grid/<slug>.part`, with a header
    (the command, `git rev-parse HEAD`, the start in UTC, `uptime`) and a footer (the end, `uptime`, the exit
    status, the wall time);
  - renames `<slug>.part` to `<slug>.txt` when the process ends. A point whose `.txt` exists is skipped, and
    a stale `.part` is deleted and rerun;
  - writes its own pid to `var/inelastic-reflection-11/grid/run_grid.pid`, one line per point started and
    ended to `run_grid.log`, and `** grid done` as its last line;
  - never retries a point that ended with a non-zero status. That status is the point's row.
- **`summarise_grid.py`**: parses every `.txt` into the tables of §2.1–§2.5, as one CSV
  (`grid.csv`, one row per point, every field of the `history`, `tuning`, `bounce`, `dissipation`,
  `fixed_T`, `ratio` and `bbn` lines) and a Markdown file of the tables. It reads; it computes nothing but
  ratios, differences and orders.

Each script is tested by running it, not by a unit test:
- `eps_point.py 1e-6 -- 2 4e-31 --alpha 0.1` against the plain tool at the same point. The outputs, timings
  masked as orchestrator note 10's ruling 7 masks them, must differ only by the `epsilon=` line.
- `run_grid.py --only` on two points, then again to show the skip.

No test module is added, and the suites' counts do not change.

### 1.2 The rehearsal (R31, U11)

`jordan-normalization` 09's protocol, its commands adapted (§1.14 there), on the isolated cluster at
`127.0.0.1:6380`:
- **`ADDRESS=127.0.0.1:6380`, `S=var/datastores`, `L=var/inelastic-reflection-11/rehearsal`.**
- **One run makes each store.** It goes through the wrapper, with `--campaign inelastic-reflection --prompt
  11 --register <slug> --purpose "…"`, and after `--`:

  `--config exponential.yaml --database $S/inelastic-reflection-11-<store>.db --shards 16 --job-name
  inelastic-reflection-11-<store>-<run> --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 1.5 3
  5.5 --phi-init-Mp 5 --alpha-values 0 0.1`

  That is eight histories: two pipelines, α = 0 then α = 0.1.
- **R1 — `reference`.** One run into `…-reference.db`, then `runregistrykit store create --purpose …`.
- **R2 — `resumed`.**
  - One run into `…-resumed.db`, sent `SIGTERM` by `interrupt_when.py` with `--banner "RUNNING PIPELINE FOR
    MODEL QCD_Cosmology, alpha = 0.1"`.
  - Its `--after` is taken from the reference's log, so that the signal falls in the α = 0.1 scalar stage
    after at least one of its histories is stored. Say how it was chosen.
  - The run is recorded `killed`. Then the same run is made again with `--prune-unvalidated`, then
    `store create`.
- **Every run is launched detached** through `sh`, as there, checked once that it started, and waited on by
  `wait_for_line.py` on its `** launcher saw exit status` line, never by polling. A recorder
  (`beat_times.py`) runs beside each.
- **R3 — the readers**, unregistered and read-only, on the reference store, with `shasum -a 256` of its files
  before and after each:
  - `main.py --inventory`;
  - `plot_by_beta.py --phi-init-Mp 5 --alpha-values 0 0.1 --output $L/plot-by-beta`;
  - `plot_ScalarHistory.py --phi-init-Mp 5 --alpha-values 0 0.1 --output $L/plot-history`;
  - `plot_by_beta.py --phi-init-Mp 5 --alpha-values 0.3 --output $L/plot-unstored`, which must exit 2, name
    `--alpha-values` and `0.3`, and write nothing under its output.

  The cluster is stopped after R3, with `ray_cluster.sh stop`, and its `status` and the lock's `status`
  recorded.

### 1.3 The grid (U10, P40, P45)

After the cluster is stopped, with nothing else of this prompt running:
- `run_grid.py` is launched detached (`nohup`, through `sh`, its stdout to
  `var/inelastic-reflection-11/grid/run_grid.out`), checked once that its pid is alive and its first point
  started, and waited on by one background `wait_for_line.py` on `** grid done`, with a timeout of 3 600 s.
  It is never polled.
- **If the wait times out**, or the conversation is interrupted, the same command resumes the grid. The log
  says which points ran twice, if any.
- **One more solve:** `tools/bbn_from_store.py --sm-baseline` (small network), for P43's reference, output
  to `var/inelastic-reflection-11/sm-baseline.txt`.

### 1.4 The record (P47)

- **`.documents/inelastic-reflection-verification.md`**, new, in the form of
  `.documents/jordan-normalization-verification.md`:
  - **§1 the measurement:** the grid, its tables (§2.1–§2.5 below), the gate Measure;
  - **§2 the rehearsal:** the builds, the comparisons, the readers, the gate R (§2.6–§2.8 below);
  - **§3 the BBN spline across injections** (P14, P46);
  - **§4 the Standard Model exchange** (U7): scoping §7's estimate, Γ/H ≲ 10⁻¹¹ for β ≤ 25 and T ≤ 20 TeV,
    and that it is out of scope, quoted as data;
  - **§5 the commands, verbatim**, as there;
  - **§6 observed, not acted on.**
- **`.documents/inelastic-reflection/out/`**, small text only:
  - every point's `.txt` (about 1.2–2 kB each), `grid.csv` and the tables;
  - the rehearsal's logs with ANSI codes removed, its recorders' outputs, `compare_stores`' output, the
    stored-values readers' outputs, `histories.csv` from R3, and the inventory;
  - the copied helpers as `.py.txt`, with diffs where adapted, as `jordan-normalization` 09 committed them;
  - no PDF and no PNG. The figures stay under `var/`, and the log names them.
- **The stores** `inelastic-reflection-11-reference` and `…-resumed` stay under `var/datastores/` for the
  user, each with its `store create` sidecar.

---

## 2. Measurement

Each table gives every point. A point that failed is a row with its reason.

### 2.1 Dissipation mode (U10)

- **For each (M, β, α):** converged or not, the integrations, the residuals, the wall, and the steps and RHS
  evaluations.
- **The `dissipation` line's fields:**
  - the counts and `injected_fraction`;
  - ρ_χ/ρ_R at 1 MeV, 70 keV and today, and ΔN_eff at 1 MeV and today;
  - ln 𝓡 at the first impact;
  - the parking and release, N and T_J (MeV);
  - the three worst margins with their N, the three failing shares, and the unassessed share.
- **BBN:** Yp, D/H, and their shifts from P43's two references: the Standard Model, and natural mode at
  M = 10⁻⁷ at the same β.
- **The warnings each point printed** (R6's "nearly elastic", any other `!!` line), counted.

### 2.2 The drain's independence of M (gate Measure, P44)

- For each (β, α): `injected_fraction` at M = 10⁻¹² and 4×10⁻³¹, their relative difference, and the same
  for ΔN_eff today.
- The limit is 10⁻³ (README §8.0, from the probes). Log 05 §5.5 measured 3×10⁻⁹ untuned. The grid's
  histories are tuned, so say whether the tuning's normalization moves it.

### 2.3 ΔN_eff against α (gate Measure, P44)

- For each (M, β): ΔN_eff today and at 1 MeV at the four α, and whether ΔN_eff today rises strictly.
- Give ΔN_eff today/α and its spread across α, which shows how far from linear the drain is. That is a
  measurement, not a gate.

### 2.4 Natural mode

- The 8 points: converged or not, integrations, residuals, wall, steps, the warnings, Yp and D/H.
- **At M = 10⁻⁷**, whether P23's warning was printed at every β, and whether every history reached T_CMB.

### 2.5 The parking ε (P40, P42)

At M = 4×10⁻³¹, α = 0.1, for each β and each ε ∈ {10⁻⁴, 10⁻⁶, 10⁻⁸}:
- the parking N and T_J, and the release if any;
- `injected_fraction`, ΔN_eff today and at 1 MeV, Yp and D/H;
- the wall and the steps.

Give the differences between ε values against log 06 §5.5's (at ε = 10⁻⁸ the switch came 5.4 e-folds later
at β = 1.2, and Σ + leftover agreed to 2×10⁻⁸). Also give the `eps_point.py` check of §1.1.

### 2.6 The rehearsal's builds (gate R)

As `jordan-normalization` verification §1.2–§1.6:
- **Each run:** its id, slug, store, pid, state, exit status, launch and end, wall, load at launch, and the
  fingerprint at finish.
- **Each history**, from the stored values (an adaptation of `histories.py` that reads α and the dissipation
  columns): the integrations, residuals, tuning wall, and parking and release, at both α.
- **The interruption:** the delay chosen, when the signal fell, what was stored by then, the Ray processes
  after it (`ray_state.py`, `ray_cluster.sh status`), and what the resume recomputed.
- **The comparison:** `compare_stores.py` on the two stores, which must be equal, and `store fingerprint` of
  each.
- **The silences:** the longest heartbeat silence of each run against the 900 s window.

### 2.7 α = 0 against 2026.7.0 (gate Measure, R31)

For each of the four α = 0 histories, against the history of the same (M, β, φ*) in
`var/datastores/jordan-normalization-09-reference.db`, by a read-only script over both stores' SQLite files:
- every column of `ScalarHistory` that both schemas hold: the tuning's outputs, `first_bounce`, the
  fixed-temperature values, the RHS count and steps. `compute_time` is not compared;
- every sample of `ScalarHistoryValue`, matched by ln(1 + z), every column both schemas hold;
- `AdiabaticHistory`'s and `BBNData`'s values, the same way.

Each column is bitwise equal, or the script names it with the history and the largest difference. Give the
number of values compared. A difference is a stop.

### 2.8 The readers (gates Driver and R)

- **`main.py --inventory`:** `alpha_value` under "Redshifts, tolerances and parameter values" with
  `values = [ 0, 0.1 ]`, and `alpha=` on each history line.
- **`plot_by_beta.py`:**
  - every product of R26 at its path (`alpha=0/`, `alpha=0.1/`, and `histories.csv` and `Delta_N_eff.pdf`
    at the label's level);
  - `histories.csv` with eight rows, sorted by (α, M, n, β), each ΔN_eff equal to the stored column;
  - the run's wall, and the store's `shasum` unchanged.
- **`plot_ScalarHistory.py`:**
  - every history's figures under its α;
  - `dark_radiation.pdf` for each α = 0.1 history and none for α = 0;
  - `fields.csv` with `rho_chi_over_rho_rad` and `Delta_gstar`;
  - the store unchanged.
- **The unstored α:** the exit status, the message, and that nothing was written under its output.
- **The figures:** look at `Delta_N_eff.pdf` and one `dark_radiation.pdf` (as PNG), and say what they show.

### 2.9 P14: the BBN spline across injections (P46)

- For each dissipation point and its natural-mode reference (M = 10⁻⁷, the same β): the `ratio` lines' `n`,
  median and `rms_step` in each window.
- Say whether α > 0 raises `rms_step` in a window where the history injects or parks. If it does, give the
  size, and open it (README P14); do not fix it.

---

## 3. Verification

1. **Before anything else:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 497 (122 + 375) / 129 / 123 / 68 (log 10 §8). A different count, or a failure, is a stop.
2. **After:** the same five suites, unchanged in count. No code changed, so nothing else is expected.
3. `git status` shows only the files of §6. Nothing under `var/` is committed.
4. No ChamPBH Ray process outlives `ray_cluster.sh stop`. The lock is released, and `runregistrykit list`
   shows every run of this prompt `done` or `killed`, none `running` or stale.
5. `grep -r -F -l -f ~/.ray/auth_token` over everything this prompt commits finds nothing.

## 4. Acceptance

1. **The gate "Measure"** (README §8.4, P44):
   - every point of P40 has a row, a failure with its reason;
   - at each (β, α), `injected_fraction` at M = 10⁻¹² and 4×10⁻³¹ agree to 10⁻³;
   - ΔN_eff today rises strictly with α at each (M, β);
   - every margin is reported;
   - α = 0 reproduces 2026.7.0 (§2.7).
2. **The gate "R"** (README §2.3):
   - the resumed store equals the reference;
   - the two α are distinct histories;
   - and, for the gate Driver's first real run, the readers of §2.8 behave as log 10 says.
3. **The measurement** of §2, in the verification document and the log.
4. **The log**, `logs/11-measure.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - the suite counts before and after;
   - the gates item by item;
   - every deviation, classified;
   - "Observations not acted on";
   - "State handed to the next prompt", for 12: the gates, the stores and where they are, the
     verification document's sections, what 12 must carry into `CLAUDE.md`, the architecture summary and
     the handover §4.18 (the α grid for the rebuild, ΔN_eff, the costs), and every open issue of this board.
5. **The board:**
   - row 11, and the gates Measure and R, in §1 and §2;
   - `### 5.13 After prompt 11 (<date>)` in §5;
   - anything opened, on §3 and in `.documents/OPEN_ISSUES.md`, in the same commit (`CLAUDE.md`).
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- An item of the Gate at the top fails, or a baseline count differs or fails.
- **The lock.** `ray_cluster.sh` or the lock gives exit 3: stop, never wait, retry or remove the lock. Exit
  4: run `stop`, then stop and ask.
- **A driver fails.** It errors, refuses, or writes to the store where it should read: a defect of 10's work.
  Record it. It is fixed by a lettered prompt, not here.
- **A gate item fails:**
  - a stored α = 0 value differs from 2026.7.0;
  - the resumed store differs from the reference beyond what a resume may change
    (`jordan-normalization` 09's protocol);
  - the drain differs across M by more than 10⁻³;
  - ΔN_eff falls with α;
  - an unstored α does not exit 2.
- **A history does not converge,** or the tool and the store disagree on a stored history's tuning outputs.
  Run the tool on each of the rehearsal's eight histories with `--alpha` as `jordan-normalization` 09 H3 did:
  normalization, φ_ref, residuals and iterations must match bitwise.
- **The grid stalls.** It does not end inside 3 600 s, or a single point runs for more than 15 minutes. Stop
  the point's process, record it, and ask before resuming.
- **The work seems to need a change** to any code, or to a test.

A dissipation point that fails with a reason is a row, not a stop, unless it is at M = 4×10⁻³¹ M_P, the
physical point. Then it is opened as an issue, and the log says why it failed.

## 6. What this prompt does not do

- **No code changes.** No test is added or edited, and no golden is written.
- **No `ray stop`, and no production run.** No run but the two registered builds and the readers of §1.2,
  and the grid's tool runs.
- **No fix** for anything found. It is recorded and opened (README P15).
- **No change** to `CLAUDE.md`, the architecture summary or `numerical-strategies.md`. Those are 12's.
- **Files it changes:**
  - `.documents/inelastic-reflection-verification.md` (new);
  - `.documents/inelastic-reflection/measure/` (new: `points.py`, `eps_point.py`, `run_grid.py`,
    `summarise_grid.py`, and the stored-values and comparison scripts of §2.6–§2.7);
  - `.documents/inelastic-reflection/out/` (new);
  - the log, this board and `.documents/OPEN_ISSUES.md`.

  Nothing under `var/` is committed. Any other file is a deviation to classify.
