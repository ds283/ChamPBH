# Orchestrator — prompt 07, the rehearsal

**Used for 07** (dispatched 2026-11-16 at `945bf5d`; stopped at the gate R §2.2, ruled (a) by the user;
landed `bbd99f4`; reviewed on the board, "Orchestrator review of prompt 07"). **Written** 2026-11-16 with the prompt, at the user's request: "Please write 07 and its orchestration note." Not
dispatched. P59–P72 (README §6.11) stand unless the user rules otherwise; none needs a ruling first. 07 is dispatched at
the user's word.

Read [`../README.md`](../README.md) first: §2.1 (the gate R, as P64–P68 make it exact), §3, §5, §6.10 (R12) and §6.11
(P59–P72). Then read the prompt. This note binds 07 only, and governs where it differs from the prompt. The conventions
of earlier notes bind here as well: a tolerance is a number with its reason, timing is taken under load, and the suites
run serially.

**You do not write code.** You may:
- run the suites, the scripts the log names and the tools the prompt names, read-only on the stores;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07-rehearse.md`](../07-rehearse.md) · **Gate:** "R" · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-16 between 16:22 and 16:26 local, before this note was committed:
- **The worktree.** `.claude/worktrees/power-law-wall` was on `power-law-wall` at `c7ba5ca` (review of 06), on top of
  `fda2df2` (06). `git status --porcelain` showed `?? venv`, plus the planning files before their commit.
  `git worktree list`: the main checkout and this worktree.
- **The main checkout** is on `main` at `9df9e13`, clean.
- **The registry.** `runregistrykit list`: 62 runs, all finished; none `running`, stale or unknown; none of this
  campaign. The 2026.11.0 rebuild has ended (U1: 07 waits for it).
- **The cluster.** `tools/ray_cluster.sh status` exits 0: the lock free (its directory does not exist), no Ray process
  of ChamPBH's, ports 6380, 6381, 8267 and 52367 free.
- **The paths.** No `$MAIN/var/datastores/power-law-wall-07-*`, no `var/power-law-wall/07/` in the worktree, no
  `.documents/power-law-wall/rehearse-07/`. `sample-04/runs/` holds the forty outputs and `runs.log`;
  `sample-04/tool-route/` the three `exp-*.txt`; `exponential_counterparts.csv` 22 lines.
- **The machine.** 10 cores, 16 GB, shared. Load 3.61 / 6.27 / 6.36 at 16:22; 115 GB free. `PYTHONPATH` is unset in this
  session's shell.
- **The suites.** Not re-run by the planner. 06's review left them at 26, 783 (226 + 557), 175, 312 and 79, with only the
  two worktree failures of note 01's ruling 7, and nothing has touched code since.

## 0. What the planner found before writing 07

A survey of the tree at `c7ba5ca` and of the records it names, and two scratch probes (in-process, nothing written).

- **The registry writes to the main checkout.** From this worktree's root, `runregistrykit.find_client_root()`, with
  and without `client_root` set to the worktree, resolves `runs_root` to `$MAIN/var/runs` and `stores_root` to
  `$MAIN/var/datastores`. That is why `tools.tests.test_registry_config` fails in the worktree (note 01, ruling 7). So
  07's run records land where `runregistrykit list` finds them, and its stores go beside the others in
  `$MAIN/var/datastores` (P61). `RunDrivers/registered_run.py` passes its own `REPO_ROOT` as `client_root`, and it
  resolves the same way. The base worktree is expected to resolve the same; the prompt has the agent confirm it.
- **The base, `a7e0978`.** The merge base of `power-law-wall` and `main`, the commit the campaign was cut from.
  - `VERSION_LABEL = "2026.11.0"`, `FINGERPRINT_FORMAT = 7`; `exponential.yaml` without `models`.
  - Its parser has every option the grid uses: `--A-values`, `--no-alpha-values`, `--n-values`, `--phi-init-Mp`,
    `--M-values-Mp`, `--beta-values`, `--job-name`. It has no `--models`, so the base's command is the tip's `exp`
    command exactly (the tip's default is `[exponential]`).
  - `--prune-unvalidated` is needed only at the tip.
  - Its pipeline banner is the tip's: `>> RUNNING PIPELINE FOR MODEL QCD_Cosmology, <alpha_label>`, where
    `alpha_label` gives `A = 0.05 (one-loop-measured-pl-v1)` for a power-law A row (`drain_model.py:133`). The banner
    does not name the potential, so in `mixed` the fixed rows' banners appear once per potential.
- **The data files are the same at both commits.** `git diff a7e0978 HEAD` over `PRyMrates`, `PRyM`,
  `CosmologyModels/GenericEOS`, `requirements.txt`, `runregistrykit.toml` and `thirdparty` is empty. Of the files a run
  needs, only `exponential.yaml` (the `models` key), `main.py` and `RunDrivers/registered_run.py` (the scope) differ. The
  open issue `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]` would therefore not bite
  one shared cluster. The prompt still has one cluster per tree, `electroweak-eos` 02's practice, and a probe that
  hashes the data files in every process, so that the record shows it rather than infers it (P60).
- **04's forty are cheap.** `runs.log`: 3–32 s per history at laboratory M and below, 3–11 s for n = 1 natural mode at
  10⁻³ M_P (which fails at G2, R5), 48–97 s for n = 2 there; BBN included. Each run of the build is therefore minutes,
  and the whole prompt is bounded by the suites (about 25 minutes) and the readers, not by the builds.
- **04's outputs at the digits they print.** `sample_run.py` prints the tool's own lines, then `sample` and `cand` lines.
  - The `tuning` line prints `normalization`, `V_ref`, `phi_ref`, `phi_0`, `pi_0`, `tracking` and the residuals by
    `repr`, so they compare bit for bit.
  - The `dissipation`, `drain` and `cand` values are printed to about six significant digits, and `bbn` to ten.
  - A failure's reason names the history as the tool names it (`history_and_bbn-beta2-M0.001-inverse-power-n1`), which
    `main.py` does not, so the prompt masks the name (§2.3).
  - The tool's defaults are `main.py`'s: φ* = 5 M_P (its own `PHI_INIT_MP = 5.0`, equal to the parser's
    `DEFAULT_PHI_INIT_MP`), atol = rtol = 10⁻⁸ (the config files' and `integrate_history`'s), T* = 2 × 10⁴ GeV,
    `DEFAULT_TUNING_TOLERANCE` and `DEFAULT_TUNING_MAX_INTEGRATIONS` imported from the same module as the parser's, and
    the small network. Every one of 04's forty `# command:` lines gives only `--model`, `--n`, `--drain-law` and
    `--alpha`.
  - In `inelastic-reflection` 11 and `one-loop-drain` 06 the build and the tool route agreed on every tuning key by
    `float.hex`; the prompt asks the same here.
- **The interruption's window.** In 04, the A = 0.05 histories at laboratory M took 16–32 s (n = 1) and 17–29 s
  (n = 2), the widest spread of any pipeline at that M; A = 1 and 10 took 12–19 s. With six tasks per pipeline on ten
  cores the window may be near 15 s at low load, which is why the prompt keeps `one-loop-drain` 06's "opening + 2 s"
  rule and its single retry.
- **The exports carry no serial.** `extract_common.CSV_COLUMNS` has no serial or timestamp column, so `mixed`'s exports,
  whose serials interleave two potentials, can equal `exp`'s and `pl`'s byte for byte (§2.4). `compare_exports.py`
  reports any column that differs, so a timing column would be named, not hidden.
- **06's `compare_exports.py`** expects the tip's `<label>/` to hold both potentials' directories. In 07 `export-exp`
  holds `exponential/` only, hence the adaptation the prompt asks for (§1.6).
- **The configuration's precedence.** `config/argument_parser.py` reads the config file through `configargparse`'s
  `YAMLConfigFileParser`, so `--models` on the command line overrides the file's `models`, as `mixed` needs.
- **`tools/bbn_from_store.py`** takes `--M-Mp`, `--beta`, `--phi-init-Mp`, `--model`, `--n`, `--drain-law`, `--alpha`,
  `--small-network` and `--variant` (default `prod`). R6 on `base` runs from the base tree, whose tool reads its own
  format.
- **What 07 does not need:** no PRyMordial patch, no change to `.gitignore`, and no file of the main checkout but the
  two ignored directories the registry uses.

**Rulings.** Each is recorded in the log, with its class.
1. **The trees and the main checkout.**
   - The agent works in this worktree, from its root, with `./venv/bin/python` and `PYTHONPATH=.`, and in the base
     worktree for the base's builds and readers.
   - The base worktree is `<scratch>/base-a7e0978`, with `<scratch>` the agent's subdirectory of the session
     scratchpad, and one link, `venv`. Nothing under it is edited. It is removed with `git worktree remove` and
     `git worktree prune` before the commit; `--force` only if the link alone makes it dirty, and the log says so.
   - In the main checkout, only `var/datastores/power-law-wall-07-*` and `var/runs/power-law-wall-07-*` are created,
     by `main.py`, the wrapper and `runregistrykit`. No tracked file there is read or written, and nothing else under
     its `var/`.
   - Never stage `venv` or anything under `var/`. Never name `/tmp/ray-cpbh`.

   *STRUCTURALLY REQUIRED* (§0, first point; README §5).
2. **The clusters.**
   - Each `start` runs alone in its Bash call, from its tree's root, with `PYTHONPATH` unset, through that tree's
     `tools/ray_cluster.sh`, with its default of 10 CPUs. Each `stop` is that tree's.
   - The base's cluster is stopped, and `status` shows no process and the lock free, before the tip's is started.
   - The lock taken by another project between the two clusters gives exit 3 at the second `start`: stop and ask
     (prompt §6), and never wait or retry. The base's store and records stay as they are.
   - **A stop:** prompt §1.2's probe mismatch.

   *IMPLEMENTATION CHOICE*, ruled here (P60).
3. **The manifests and sidecars.** Each base run's manifest records `git_head` `a7e0978` and the script hash of
   `git show a7e0978:main.py`; each tip run's a tip SHA and the hash of the tip's `main.py`. The base store's sidecar is
   format 7, every tip store's format 8. A mismatch is a stop. *STRUCTURALLY REQUIRED.*
4. **§2.3, exactly.**
   - **The map.** `pl`'s forty to 04's files by (n, M, β, law), each recorded with its `# command:` line.
   - **Bit for bit:** the `tuning` line's values that the store holds, matched by name to their columns, and the
     counts of the `history` line that the store holds. A value the store does not hold is listed, not compared.
   - **At the printed digits:** a stored value formatted with the format the tool or `sample_run.py` uses for that
     field (read from the source; name the format in the script) equals the printed text. A `cand` field is compared
     through `to_column(value, kind, Planck_units())` against the stored column, as `one-loop-drain` 06's ruling 2
     compared candidates, and then at the printed digits.
   - **Y_p and D/H:** at full precision against the printed ten digits, a relative difference up to 5 × 10⁻¹⁰ reported
     and not a stop.
   - The count of values compared, per line kind, per history and in total.

   *As the prompt says; made exact* (§0, fifth point).
5. **The interruption.** As the prompt says, with `one-loop-drain` 06's ruling 7: the window from `pl-a`'s beats,
   measured from the A = 0.05 banner; the midpoint rounded down, or the opening + 2 s below a 10 s window; the load at
   both launches recorded. *IMPLEMENTATION CHOICE*, ruled here.
6. **The load.** The machine is shared. Each build's and reader's wall is recorded with the load at its start. The
   60-minute bound per run stands. No run is delayed or repeated for load. *As the prompt says.*
7. **The two worktree failures** of note 01's ruling 7 are expected in the suites. Any other failure is a stop.
   *IMPLEMENTATION CHOICE*, ruled here.
8. **A finding about production** is recorded and reported, not fixed. A defect found in a driver or reader is a stop
   (prompt §6). Anything else may be opened as a §3 issue, with `.documents/OPEN_ISSUES.md` in the same commit.
   *IMPLEMENTATION CHOICE*, ruled here.
9. **Files.** Prompt §7's list, and nothing else. The README is not edited. *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites, before anything else: serially, ComputeTargets in its two halves, the `test_[g-z]`
     half and `tools/tests` in the background, waiting for each notification.
  2. The scripts of §1.6, and the map of §1.3.
  3. The base worktree; the base's cluster and probe; `base`'s three runs and `store create`; the base's readers
     (R1, R2, R6, R7 on `base`); the base's cluster stopped.
  4. The tip's cluster and probe; `exp`, `pl`, `pl-resumed` (with the interruption), `mixed`, each with `store create`.
  5. The tip's readers R1–R4 and R7; the tip's cluster stopped; R5 and R6.
  6. The comparisons of §2; the base worktree removed.
  7. The counts by discovery; the documents; the records; the commit.
- **Scratch.** Run outputs go under `$L`. Probes and suite outputs go under `<scratch>`. Nothing goes in `/tmp` but the
  clusters' own directory, which only `ray_cluster.sh` names.
- **Ray.** Through `tools/ray_cluster.sh` alone. No `ray stop`, ever. A Ray CLI command takes `RAY_AUTH_MODE=token`.
  The token is never printed.
- **Waiting.** Launch, one check, then one background wait per run for its end line. No polling from the turn.
- **Stop conditions are real:** prompt §6's, whole; ruling 3's mismatch; ruling 4's bitwise items; a suite count that
  differs. A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`. At a stop the
  agent leaves the cluster up and the stores where they are, and reports what is running, unless the stop is the lock's.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note, or a later one.
2. **`runregistrykit list`**, `tools/ray_cluster.sh status`, the rehearsal lock
   (`./venv/bin/python -m raypoolkit.rehearsal_lock status`) and the load average. If another project holds the lock,
   do not dispatch: tell the user.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly prompt §7's files. Its log is `logs/07-rehearse.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Nothing it starts outlives its turn** but the five stores (six, after a miss) and their sidecars. The clusters are
  stopped, the lock is released, the base worktree is removed, and no pid recorded under `$L` answers `kill -0`.
- **Stop and ask** on any of the conventions' stop conditions. A stop is a reply headed **STOP**, with the work left
  uncommitted.
- **At the end it replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before, and the counts after, with times and `uptime`;
  - each tree's probe output;
  - the per-run table (§1.4's record), and each manifest's `git_head`, `git_dirty` and script hash;
  - the interruption: the window, the delay, when the signal fell, what was stored and pruned;
  - the gate R item by item (§2.1–§2.5), with the counts compared per table and per line kind;
  - the readers R1–R7, with exits, walls and `shasum`s, and `plot_quanta_grid`'s refusal;
  - the costs: per store, and per history the power law's beside the exponential's;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only prompt §7's files. Its parent is this note's commit, or a later commit that is
     not the agent's.
   - `git status --porcelain` in the worktree shows `?? venv` only, and the main checkout is clean at its own `HEAD`.
     `git worktree list` shows the main checkout and this worktree. Nothing is pushed.
   - No line of code, test, config file, `CLAUDE.md`, `.gitignore` or `PRyM/` changes. Nothing under `var/` is tracked.
     No PDF and no PNG is committed.
   - `$MAIN/var` gained only `datastores/power-law-wall-07-*` and `runs/power-law-wall-07-*`.
2. **The counts,** by discovery, myself: 26, 783 (226 + 557), 175, 312, 79.
3. **The cluster, the registry and the trees.**
   - `ray_cluster.sh status` and the lock's `status`: free, no process.
   - `runregistrykit list`: every run of this prompt `done` or `killed`, as the log says.
   - Each manifest's `git_head` and script hash against `git show <sha>:main.py`, by my own `shasum`.
   - Each probe output as committed: module files under their own tree, the labels and formats each tree's, the working
     directories each tree's root, the data files' hashes equal.
   - The sidecars' formats, 7 for `base` and 8 for the others.
4. **Bitwise, independently.** A read-only script of my own (`sqlite3` `mode=ro`) over `base` and `exp`, and over
   `mixed` against each of `exp` and `pl`: the histories matched; every shared column of `ScalarHistory`,
   `ScalarHistoryValue` and `ScalarHistoryBounce`, and the BBN values, by `float.hex`; my counts against the agent's.
5. **R, independently.**
   - `tools/compare_stores.py` on `pl` and `pl-resumed`, and `store fingerprint` on each, run by me.
   - Against 04, at five histories of my choosing (at least one per n, one failure, one at 10⁻³³ M_P): the tuning
     line's values by `float.hex`, ΔN_eff, Y_p and D/H, and the first and last `cand` rows.
6. **The readers and the exports.**
   - `export-base` against `export-exp/…/exponential/` by `cmp` of my own, with `histories.csv`'s `model` removed.
   - `export-mixed` against `export-exp` and `export-pl` (n = 1) by `cmp`.
   - `tools/quanta_measures.py` at c_a = 1 re-run by me on `export-pl`'s root, byte-identical to the agent's, timing
     fields aside, with each row's `spectrum_model` by its n.
   - R4's logs and the absence of their outputs; R6's lines against the stored values; the `shasum`s unchanged.
   - One of `plot-pl`'s figures looked at myself.
7. **The records.**
   - The log is in §5.1's form, every deviation classed and §0's rulings among them. Its §7 gives 08 the stores, the
     exports, the gate and the costs, as prompt §5 asks.
   - The verification document's §1–§5 are unchanged, and §6 follows prompt §3 with the commands verbatim.
   - The board: row 07, its items, the gate R in §2, an entry in §5, and **Last updated**.
     `.documents/OPEN_ISSUES.md` matches anything opened.
   - The token grep over the commit finds nothing.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 07 landed, the gate R held, and this note marked "used for 07";
- an *Orchestrator review of prompt 07* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3 and in
`.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate R: the exponential base to tip, the power law resumed, against 04, both potentials in one store;
- the readers, the refusals, the quanta tools and `plot_quanta_grid`'s refusal;
- the costs measured, for the science run's handover;
- anything opened, and the stores left in `$MAIN/var/datastores`;
- that 08 (the close and the handover) is written only when the user asks for it.
