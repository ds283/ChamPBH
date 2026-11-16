# Prompt 07 — the rehearsal

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-16, after 06 landed (`fda2df2`, reviewed `c7ba5ca`), at the user's request: "Please write 07 and its
orchestration note." It is written from:
- README §2 row 07, §2.1 (the gate R, as P64–P68 make it exact), §3, §5, §6.2 U6–U7, §6.6 R9–R10, §6.10 (R12) and §6.11
  (P59–P72);
- log 04 (the forty and their outputs) and log 06 §6 and §7 (the commands, the export roots, what 07 compares);
- `one-loop-drain` prompt 06, its orchestrator note, its log and its verification §6
  (`.documents/one-loop-drain/rehearse-06/`), the last rehearsal, whose protocol this prompt follows;
- `electroweak-eos` prompt 02 and its open issue `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`
  (`.documents/electroweak-eos/effect/`), the last build from two trees;
- the planner's survey at `c7ba5ca` (orchestrator note 07 §0).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, from its root, with
`PYTHONPATH=.` and `./venv/bin/python`. The base builds run from a scratch worktree of `a7e0978` (§1.1). Throughout:
- `ADDRESS=127.0.0.1:6380`;
- `MAIN=/Users/ds283/Documents/Code/ChamPBH`, the main checkout, whose `var/` the registry uses (P61);
- `S=$MAIN/var/datastores`, where the stores go;
- `L=<worktree>/var/power-law-wall/07`, where the launch logs, the exports and the comparisons go.

**Gate:**
- in the worktree, on branch `power-law-wall`, at or after the commit that adds this prompt; `fda2df2` and `c7ba5ca` are
  ancestors of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `git worktree list`: the main checkout and this worktree, nothing else;
- `tools/ray_cluster.sh status`: the lock free, no ChamPBH Ray process, the four ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- none of `$S/power-law-wall-07-*`, `$L` and `.documents/power-law-wall/rehearse-07/` exists;
- `.documents/power-law-wall/sample-04/runs/` holds the forty outputs and `runs.log`, and `sample-04/tool-route/` the
  three `exp-*.txt`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus** (README §2). Real builds under real Ray from two trees, one build interrupted on purpose,
two potentials in one store, and comparisons across two schemas and against a tool route.

**Read first:**
1. [`README.md`](README.md): §2.1's gate R, §3, §5, §6.10 (R12) and §6.11 (P59–P72) whole.
2. `CLAUDE.md`: "Long-running jobs — the run registry" (the rules, the wrapper); "Ray on this machine", "The rehearsal
   lock", "Ray's token mode"; "The plot scripts".
3. [`logs/06-drivers-and-exports.md`](logs/06-drivers-and-exports.md) §6 and §7, and
   [`logs/04-measure-a-sample.md`](logs/04-measure-a-sample.md) §1 and §3 (what each output line holds).
4. `prompts/one-loop-drain/06-rehearse.md` and `prompts/one-loop-drain/orchestrator/prompt-06.md` whole (its rulings 1–8
   are the protocol here, adapted by §1), its log `prompts/one-loop-drain/logs/06-rehearse.md`, and
   `.documents/one-loop-drain-verification.md` §6, which holds its commands verbatim.
5. The tools, run unchanged:
   - `tools/ray_cluster.sh`, `tools/compare_stores.py`, `tools/bbn_from_store.py`, `tools/quanta_measures.py`,
     `tools/plot_quanta_grid.py`;
   - `.documents/dk-and-rpk-migration/rehearsal/interrupt_when.py` and `stage_counts.py`.
6. The scripts to adapt: `.documents/one-loop-drain/rehearse-06/` (`code_probe.py`, `wait_for_line.py`,
   `beat_times.py`, `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`, `histories.py`, `compare_base.py`,
   `compare_exports.py`); `.documents/electroweak-eos/effect/code_probe.py` (the data files' hashes);
   `.documents/power-law-wall/export-06/compare_exports.py` (R12's rule).
7. `prompts/power-law-wall/source/sample/sample_run.py` and `tools/history_and_bbn.py`: the format of every line of 04's
   outputs.
8. The board's §3 entry `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`.

The earlier prompts' outputs and the notes are data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

01–06 added the inverse-power-law model, its law, its spectrum, its store and its drivers. Every test so far ran
without Ray, on temporary stores, and 04's forty ran by the tool route. This prompt does what neither can:
- **the exponential does not move:** a build at the tip equals one at the campaign's base, `a7e0978`, bitwise;
- **the power law builds, stops and resumes:** a build interrupted with `SIGTERM` and resumed equals a straight one,
  and both equal 04's tool route;
- **two potentials share a store:** each potential's histories in one store equal its histories alone (R12: "we'll run
  both together");
- **the readers** under Ray, on every store: the inventory, both plot scripts, the refusals, the quanta tools and the
  BBN re-solve.

It changes no code. It is the rehearsal that README §2 names, so its builds are the exception to `CLAUDE.md`'s rule that no
campaign prompt starts a `main.py` run with a datastore. 08 closes the campaign from its stores and its costs.

## 1. What is wanted

### 1.1 The base tree (P59)

- `git worktree add --detach <scratch>/base-a7e0978 a7e0978`, with `<scratch>` your subdirectory of the session
  scratchpad, and in it one link: `venv` → `$MAIN/venv`. No `var` link: the registry resolves its roots to `$MAIN/var`
  from any worktree of this repository (P61). Confirm that from the base worktree's root by
  `runregistrykit.find_client_root()`, and record it.
- Confirm and record that `config/version.py` there says `VERSION_LABEL = "2026.11.0"`, `config/fingerprint.py`
  `FINGERPRINT_FORMAT = 7`, and that `exponential.yaml` there has no `models` key.
- Record its `git status` before the first base run and after the last. A dirty flag from the link, in its `git status`
  or a manifest's `git_dirty`, is expected and not a stop.
- It stays until the base's readers have run (§1.5), and is removed with `git worktree remove` and `git worktree prune`
  before the commit.

### 1.2 The clusters and the probe (P60)

- **Two clusters, one at a time.** The first is started from the base worktree's root, with its own
  `tools/ray_cluster.sh start "power-law-wall 07 (rehearsal, base a7e0978)"`; it serves the base's builds and the base's
  plot runs, and is stopped with that tree's `stop`. The second is started from this worktree's root,
  `tools/ray_cluster.sh start "power-law-wall 07 (rehearsal, tip)"`, and serves everything else. Each `start` is alone in
  its Bash call, with `PYTHONPATH` unset. Record `status` and the lock's `status` after each `start` and each `stop`.
- **The probe,** `rehearse-07/code_probe.py`, adapted from `one-loop-drain` 06's and `electroweak-eos` 02's. It runs as
  `main.py` runs (`PYTHONPATH=.`, the venv's python, from a tree's root, `config.ray_auth` imported first, then
  `ray.init(address=ADDRESS)`), and prints from the driver, from one task and from one actor:
  - `ComputeTargets.ScalarHistory.__file__` and `ObjectFactories.__file__`;
  - `config.version.VERSION_LABEL` and `config.fingerprint.FINGERPRINT_FORMAT`;
  - `os.getcwd()`, `os.environ.get("PYTHONPATH")` and the first five entries of `sys.path`;
  - the SHA-256 of `CosmologyModels/GenericEOS/Xav_EOS_data.csv` and of `PRyMrates/` (its files, sorted, digested
    together), each as read by its relative path.

  It exits 1 if a worker's files, labels or hashes differ from the driver's.
- **When it runs:** from the base worktree once its cluster is up, before any base build; from this worktree once its
  cluster is up, before any tip build.
- **What must hold.** Every module file under the tree that runs the driver, at that tree's label and format (2026.11.0
  and 7 for the base, 2026.12.0 and 8 for the tip); every working directory that tree's root; the data files' hashes
  equal in every process and in both trees. **Anything else is a stop.**

### 1.3 The grid (P62)

Every run gets `--database $S/power-law-wall-07-<store>.db --shards 16 --phi-init-Mp 5 --ray-address $ADDRESS
--job-name power-law-wall-07-<store>-<run>`, its store's config and potentials, and then:

| run | grid |
|---|---|
| `a` | `--M-values-Mp 4e-31 --beta-values 1.2 2.0 2.9 --alpha-values 0.01 0.1 --A-values 0.05 1 10` |
| `b` | `--M-values-Mp 1e-3 --beta-values 1.2 2.0 2.9 --alpha-values 0` |
| `c` | `--M-values-Mp 1e-33 1e-15 --beta-values 2.0 --A-values 1 --no-alpha-values` |

| store | tree | config and potentials | n | histories |
|---|---|---|---|---|
| `base` | `a7e0978` | `--config exponential.yaml` | 1 | 20 |
| `exp` | tip | `--config exponential.yaml` (the command is the base's) | 1 | 20 |
| `pl` | tip | `--config inverse_power.yaml --n-values 1 2` | 1, 2 | 40 |
| `pl-resumed` | tip | the same as `pl` | 1, 2 | 40 |
| `mixed` | tip | `--config exponential.yaml --models exponential inverse-power` | 1 | 40 |

- **The histories are U7's.** `pl`'s forty are 04's forty, and each store's exponential twenty are their counterparts.
  Before any build, map each of `pl`'s forty to its 04 output (`ip-n<N>-M<M>-b<β>-<law>.txt`) and check from the
  output's `# command:` line that the tool ran with no option but `--model`, `--n`, `--drain-law` and `--alpha`, so at
  `main.py`'s defaults. Record the map.
- **Slugs:** `<store>-<run>`, for example `base-a`, `pl-c`, `mixed-b`; the resume of §1.4 is `pl-resumed-a2`.
- **Purposes** say the prompt, the store, the run, the tree and the grid, for example
  `"power-law-wall 07 R: run a of the pl store at the tip (inverse-power n 1 2; beta 1.2 2.0 2.9 at M 4e-31 Mp, phi* 5,
  alpha 0.01 0.1, A 0.05 1 10)"`.
- **The counts per store** are the table's. A history that fails is a row, not a stop (04: n = 1 at 10⁻³ M_P fails at
  G2, R5).

### 1.4 The builds

- **In this order:** `base` (runs `a`, `b`, `c`, then `runregistrykit store create --purpose …` from the base worktree's
  root, so that the sidecar is format 7); the base's readers (§1.5); the base's cluster stopped; the tip's cluster and
  probe; `exp`, `pl`, `pl-resumed`, `mixed`, each its three runs then `store create` from this worktree's root.
- **Through the wrapper:** `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign power-law-wall --prompt
  07 --register <slug> --purpose "…" -- <main.py's arguments>`, the base's runs through the base tree's own wrapper.
- **The interruption** (P63), in `pl-resumed`'s run `a`:
  - `interrupt_when.py` sends `SIGTERM` with `--banner "RUNNING PIPELINE FOR MODEL QCD_Cosmology, A = 0.05
    (one-loop-measured-pl-v1)"`; confirm the banner's text from `pl-a`'s log first;
  - its `--after` is read from `pl-a`'s beats: the window opens at the first line that shows an A = 0.05 history
    stored and closes at the line that ends that pipeline's scalar stage. `--after` is the window's midpoint, rounded
    down to a whole second, or the opening + 2 s if the window is shorter than 10 s. Name both lines and the loads at
    `pl-a`'s and `pl-resumed-a`'s launches;
  - the run is recorded `killed`. Then `pl-resumed-a2`, run `a` again with `--prune-unvalidated`; then runs `b` and `c`;
  - **a miss.** If the signal falls outside the window, keep the store, name it in the log, and make one further
    attempt into `power-law-wall-07-pl-resumed2.db` (slugs `pl-resumed2-*`). A second miss is a stop.
- **Every run** is launched detached through `sh`, as `one-loop-drain` 06's verification §6 gives the command, with its
  log and pid file under `$L/runs/`. Make **one** check that it started (`check.sh`), then wait in the background for its
  `** launcher saw exit status` line (`wait_for_line.py`). Never poll. A recorder (`beat_times.py`) runs beside each run.
- **Record per run:** its id, slug, store, pid, state, exit status, launch and end, wall time, the load at launch, the
  fingerprint at finish, and `git_head`, `git_dirty` and the script hash from its manifest. Each base manifest records
  `git_head` `a7e0978` and the hash of `git show a7e0978:main.py`; each tip manifest a tip SHA and the tip's hash. A
  mismatch is a stop.
- **The bound.** A run not ended 60 minutes after its launch is a stop. Do not kill it unasked.

### 1.5 The readers (P68)

Unregistered. Before and after each, take `shasum -a 256` of the store's files; a change is a stop. The readers of the
base store run from the base worktree with its cluster up; all others from this worktree with its cluster up, but R7.
Each takes `--config <its store's config> --database $S/power-law-wall-07-<store>.db --shards 16 --phi-init-Mp 5`, the
store's potentials and n, and the plot scripts `--ray-address $ADDRESS` and the alpha rows the store holds
(`--alpha-values 0 0.01 0.1 --A-values 0.05 1 10`).
- **R1 — the inventory,** `main.py --inventory`, on every store, each by its own tree. Each must print `problems: none`.
- **R2 — the exports without figures:** `plot_by_beta.py` and `plot_ScalarHistory.py` with `--no-plots` on every store,
  `--output $L/export-<store>`. `mixed` is exported in one plot run with `--models exponential inverse-power`.
- **R3 — the figures, on `pl`:** both plot scripts without `--no-plots`, `--output $L/plot-pl`. The power law's figures
  are drawn under Ray for the first time.
- **R4 — the refusals.** Each must exit 2, name the option and the value, and write nothing under its `--output`:
  - `plot_by_beta.py --models inverse-power` on `exp`;
  - `plot_by_beta.py --A-values 0.5` on `pl`;
  - `main.py --config inverse_power.yaml --n-values 3 --A-values 1 …` at parse, before Ray, against a database path
    that does not exist (and is not created).
- **R5 — the quanta tools,** no cluster needed: `tools/quanta_measures.py` at c_a = 1 on every root the tip wrote
  (`export-exp/QCD_Cosmology/exponential`, `export-pl/…/inverse-power`, `export-pl-resumed/…/inverse-power`, and
  `export-mixed`'s two). Then `tools/plot_quanta_grid.py` on `export-pl`'s root: the board's §3 issue expects it to
  refuse every figure (two n at one cell). Record its exit and message. That is a measurement, not a stop.
- **R6 — the BBN re-solve,** `tools/bbn_from_store.py --small-network --variant prod`, no Ray, one PRyMordial solve each:
  - on `base` by the base tree, and on `exp`: M = 4 × 10⁻³¹ M_P, β = 2.0, α = 0.1;
  - on `pl`: the same M and β, `--model inverse-power --drain-law one_loop --alpha 1`, at `--n 1` and at `--n 2`.

  Each prints the stored Y_p and D/H, and its `prod` line equals them to the ten significant digits it prints.
- **R7 — the power law's histories, read:** `histories.py` on every store (§1.6).

Stop each cluster after its tree's last reader, with that tree's `ray_cluster.sh stop`.

### 1.6 The scripts (P71)

Under `.documents/power-law-wall/rehearse-07/`. Each has a docstring that says what it does, how it is run, what it reads,
and that it writes nothing but stdout or files under `$L`. Each reads a store only through `sqlite3` `file:…?mode=ro` URIs,
never through `datastorekit` or Ray.
- **`code_probe.py`** (§1.2).
- **The helpers,** copied from `one-loop-drain` 06's and adapted to this prompt's paths: `wait_for_line.py`,
  `beat_times.py`, `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`. Each adaptation is shown by a diff in `out/`.
- **`histories.py`**: one line per stored history with its key (the potential, M, n, β, φ*, α and drain model), its
  outcome and failure reason, the tuning's outputs and iterations, and for a one-loop history the drain record's counts
  and shares and its bounce rows by source and regime.
- **`compare_histories.py A_STEM B_STEM [--model …] [--n …]`** (§2.1, §2.4), adapted from `one-loop-drain` 06's
  `compare_base.py`. It matches the histories of A to those of B by (potential, M, n, β, φ*, α, drain model) through the
  model's table (selected by `model_type`), `M_value`, `beta_value`, `phi_value` and `alpha_value`, restricted to the
  given potential and n, and compares them as that script did: every column both schemas hold, floats by `float.hex`,
  NULL only with NULL, `extra_data` key by key, `ScalarHistoryBounce` row by row; `serial`, every `*_serial`, `version`,
  `label`, `timestamp` and the timing columns left out by name and printed. Every history in the selection is matched
  exactly once each way. It prints the count of values compared per table, and exits 1 on any difference.
- **`compare_fingerprints.py SIDECAR_A SIDECAR_B`** (§2.1): the two sidecars' fingerprints class by class, count and
  digest, naming each class one names and the other does not.
- **`compare_04.py STORE_STEM`** (§2.3): each history of the store against its 04 output.
- **`compare_exports.py`**: `export-06/compare_exports.py` adapted to a tip export of one potential (§2.5), and a mode
  that compares two roots of the tip file by file (§2.2, §2.4). Where files differ it names the columns that differ and
  says whether each holds a timing.

No test module is added, and no suite's count changes.

## 2. The gate R (README §2.1, as P64–P68 make it exact)

### 2.1 The exponential bitwise, base to tip (P64)

- `compare_histories.py` on `base` and `exp`: the twenty matched, every shared column equal. **Any difference is a
  stop.** No exponential code path changed between `a7e0978` and the tip (README §5, P2), so no exception is expected;
  a history that fails at both with equal reasons is a match.
- `compare_fingerprints.py` on the two sidecars: every class both name equal in count and digest, and `exp`'s
  `InversePowerModel` with count 0, as P40 found for the golden. Any other difference is reported and is a stop unless
  the log explains it from the two trees' `inventory_spec()`s.

### 2.2 The power law resumed (P65)

- `tools/compare_stores.py` on `pl` and `pl-resumed` prints `RESULT: equal` and `problems: none`, with
  `InversePowerModel` and `ScalarHistoryBounce` in its table list.
- `runregistrykit store fingerprint` on each gives the same fingerprint, at format 8.
- `compare_exports.py` on `export-pl` and `export-pl-resumed`, and on R5's two `quanta_measures` outputs: byte-identical,
  or differing only in named timing columns.

### 2.3 The power law against 04 (P66)

`compare_04.py` on `pl` and on `pl-resumed`, for each of the forty against its 04 output; and on `exp` for its three
histories that 04 ran by the plain tool (`sample-04/tool-route/exp-*.txt`):
- **the outcome:** success or failure as 04's `sample outcome` line, and a failure's stored reason equal to the tool's
  once the history's name in it is masked (the tool names it `history_and_bbn-beta…`, `main.py` otherwise); every number
  in the reason compared as printed;
- **bit for bit, a difference a stop:** every value of the `tuning` line, which the tool prints by `repr`
  (`normalization`, `V_ref`, `phi_ref`, `phi_0`, `pi_0`, `tracking`, the residuals and the rest it prints), and every
  count of the `history` line that the store holds (the integrations, the reflections, the steps);
- **at the digits the line prints, a difference a stop:** every value of the `dissipation`, `drain`, `fixed_T` and
  `bounce` lines that the store holds, and every `cand` row against the stored bounce row of `source` = `candidate` with
  the same `candidate_index` (their number equal to the line's `candidates`), through the store's own conversion
  (`ObjectFactories.ScalarHistory.to_column` in `Planck_units`, as `main.py` builds);
- **Y_p and D/H** (`bbn` line, ten significant digits): a relative difference up to 5 × 10⁻¹⁰ is reported and is not a
  stop (`one-loop-drain` 06, ruling 3: the build's BBN reads the history back from the store); a larger one is a stop.

For each line, list the fields compared and the fields the store does not hold. The count of values compared, per
history and in total.

**A measurement, not a gate:** `exp`'s export `histories.csv` against 04's `exponential_counterparts.csv` (the
2026.11.0 export, built with that run's own options), column by column over the columns both hold. Report what
differs and by how much.

### 2.4 Two potentials in one store (P67)

- `compare_histories.py` on `mixed` and `exp` with `--model exponential`, and on `mixed` and `pl` with
  `--model inverse-power --n 1`: twenty matched each, every shared column equal. **Any difference is a stop.**
- `compare_exports.py` on the roots of `export-mixed` and `export-exp` (`exponential/`): byte-identical.
- `compare_exports.py` on the roots of `export-mixed` and `export-pl` (`inverse-power/`): every file of the former equal
  byte for byte to the same path of the latter; `histories.csv` equal to the latter's rows with n = 1, in their order;
  the latter's other files are those of n = 2 only.

### 2.5 The readers (P68)

- R1: every inventory `problems: none`.
- `compare_exports.py` on `export-base/QCD_Cosmology` and `export-exp/QCD_Cosmology/exponential`, by 06's rule: every
  file equal byte for byte but `histories.csv`, which equals the base's once its `model` column is removed, with `model`
  after `n` holding `exponential`.
- R4's three refusals as §1.5 says; R6's re-solves equal to the stored values; every store's `shasum` unchanged by every
  reader.

**The gate R holds** when §2.1–§2.5 hold, item by item.

## 3. Measurement

In `.documents/power-law-wall-verification.md` §6 (new), as `one-loop-drain` 06's verification §6:
- **The clusters and the probe:** each tree's probe output.
- **The builds:** the per-run table of §1.4; `histories.py` for each store, and for `pl-resumed` after `pl-resumed-a` and
  at the end.
- **The interruption:** the window, the delay and why; when the signal fell; what was stored by then (`stage_counts.py`);
  the Ray processes after it (`ray_state.py`, `ray_cluster.sh status`); what the resume pruned and recomputed.
- **The silences:** each run's longest heartbeat silence, against the 900 s window.
- **The costs:** each store's wall and its stages; each history's tuning integrations and wall, the power law's at each n
  beside the exponential's at the same (M, β, law); the bounce rows per one-loop history and in total; each store's size
  on disk; the `mixed` store's wall against `exp`'s and `pl`'s.
- **The readers:** each reader's exit, wall and output paths; R4's messages; R5's summaries, each row's
  `spectrum_model` by its potential and n, and `plot_quanta_grid`'s refusal; R6's values.
- **The figures:** look at `$L/plot-pl/QCD_Cosmology/inverse-power/`'s `Delta_N_eff.pdf` and at one one-loop history's
  figures at each n (as PNG), and say what they show.
- **The 2026.11.0 counterparts** (§2.3's measurement).

## 4. Verification

1. **Before anything else:** the five suites as `CLAUDE.md` says, ComputeTargets in its two halves and `tools/tests`
   alone, serially, with the time and the load at each start. Expect 26, 783 (226 + 557), 175, 312 and 79, with the two
   worktree failures of note 01's ruling 7 and no other. A different count, or another failure, is a stop.
2. **After:** no code changed, so the counts are taken by discovery (P70) and must equal the first.
3. `git status` shows only §5's files. Nothing under `var/` is committed. `git worktree list` shows the main checkout
   and this worktree only.
4. No ChamPBH Ray process outlives the last `ray_cluster.sh stop`, and the lock is released. `runregistrykit list` shows
   every run of this prompt `done` or `killed`, none `running` or stale.
5. `grep -r -F -l -f ~/.ray/auth_token` over everything this prompt commits finds nothing. Never print the token.

## 5. Acceptance

1. **The gate R** (§2) item by item, in the verification document and the log.
2. **The measurement** of §3.
3. **The verification document:** `.documents/power-law-wall-verification.md`, a new §6, additive (invariant 6), with the
   gate, the measurement, the commands verbatim, and "Observed, not acted on". §1–§5 stay as they are.
4. **`.documents/power-law-wall/rehearse-07/`:** the scripts of §1.6 and an `out/` of small text only: the runs' logs
   with ANSI codes removed; the manifests, status files and sidecars; the recorders' and the probe's outputs; the
   comparisons; each store's `histories.py` output and each export's `histories.csv`; R5's outputs; the inventories, the
   loads and the helpers' diffs. No PDF and no PNG; the figures stay under `$L`, and the log names them.
5. **The log,** `logs/07-rehearse.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - the suite counts before and after;
   - the gate item by item;
   - every deviation, classified;
   - "Observations not acted on";
   - "State handed to the next prompt", for 08: the stores and where they are, the exports, the gate, the costs per
     history and per store for the science run's handover, and every observation that bears on the science run (the
     grid figure, `--n-values` across potentials).
6. **The board:** row 07 and its items; the gate R in §2; an entry in §5; **Last updated**; anything opened, in §3 and in
   `.documents/OPEN_ISSUES.md` in the same commit.
7. **One commit,** in `CLAUDE.md`'s form, staged by explicit path, with `git diff --cached --name-status` shown before
   committing. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 6. Stop conditions: stop and ask

- An item of the Gate at the top fails, or a baseline count differs, or a suite fails other than as §4 expects.
- **The lock.** `ray_cluster.sh` or the lock gives exit 3: stop. Never wait, retry or remove the lock. Exit 4: run
  `stop`, then stop and ask.
- **The probe** (§1.2) shows a worker running another tree's code, another label or format, another working directory,
  or other data files.
- **A manifest** whose `git_head` or script hash is not its tree's.
- **A gate item fails** (§2).
- **A driver or reader fails:** it errors, refuses where it should run, runs where it should refuse, or changes a store's
  `shasum`. That is a defect of 05's or 06's work. Record it; a lettered prompt fixes it, not this one.
- **The interruption misses twice** (§1.4).
- **A run has not ended 60 minutes after its launch.**
- **The work seems to need a change** to any code, any test, any config file, `.gitignore` or the git configuration.

A history that fails with the same reason in every store that holds it, and in 04, is a row, not a stop.

## 7. What this prompt does not do

- **No code changes.** No test, golden or config file is added or edited.
- **No `ray stop`**, and no run but the builds of §1.4, the probe and the readers of §1.5.
- **No fix** for anything found. It is recorded, and opened on the board if it is a defect.
- **No change** to `CLAUDE.md`, the architecture summary, `numerical-strategies.md` or `science-runs/`. Those are 08's.
- **No tracked file of the main checkout** is read or written; only the registry's records and the stores are added to
  its ignored `var/` (P61). No merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched.
- **Files it changes:**
  - `.documents/power-law-wall-verification.md` (§6, new);
  - `.documents/power-law-wall/rehearse-07/` (new: the scripts and `out/`);
  - the log, this board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Nothing under `var/` is committed. Any other file is a deviation to classify.
