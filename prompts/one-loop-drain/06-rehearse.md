# Prompt 06 — the rehearsal

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-06, on branch `one-loop-drain` in the main checkout, after 05 landed and was reviewed (`9830738`,
`20972cc`). It is written from:
- README §8.4 (row 06), §8.5 (the gates Bitwise and R) and §8.14 (the review before this prompt, P50–P57);
- 04's log §7 (the options, the paths and the exports) and 05's log §7 (the tool on a rehearsal's exports);
- `inelastic-reflection` prompt 11, its log and its verification document §2 and §5, and `jordan-normalization` prompt
  09 and its log: the rehearsal protocol and its commands, which this prompt follows;
- the tree's code at the commit that adds this prompt.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. The base build runs from a scratch worktree (§1.1). The
stores go under `var/datastores/`, and everything else the runs write under `var/one-loop-drain/06/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `tools/ray_cluster.sh status`: the lock free, no ChamPBH process, the four ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 665 = 219 + 446, 163, 191, 72), all passing;
- `var/one-loop-drain/02d/sample.jsonl` (240 lines) and its `tasks/` exist, and so does
  `var/one-loop-drain/05/exports/QCD_Cosmology/`;
- none of `var/datastores/one-loop-drain-06-*`, `var/one-loop-drain/06/` and `.documents/one-loop-drain/rehearse-06/`
  exists;
- `git worktree list` shows the main checkout only.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus**, as README §8.4 plans. Real builds under real Ray at two commits, one build interrupted on
purpose, and comparisons across two schemas.

**Read first:**
1. [`README.md`](README.md) §8.14 whole; §8.5's gates Bitwise and R; §8.12's P33–P40 (the options and the paths).
2. `CLAUDE.md`: "Long-running jobs — the run registry" (the rules, the wrapper); "Ray on this machine", "The rehearsal
   lock", "Ray's token mode"; "The plot scripts".
3. [`logs/04-drivers-and-exports.md`](logs/04-drivers-and-exports.md) §7 and [`logs/05-derivation-tool.md`](logs/05-derivation-tool.md)
   §7, whole.
4. [`logs/03-the-store.md`](logs/03-the-store.md) §1.1 and §7: what the fixed law stores in the new columns, and the
   sample masses.
5. `prompts/inelastic-reflection/11-measure.md` §1.2, §2.6–§2.8, its log, and `.documents/inelastic-reflection-verification.md`
   §2 and §5. §5 holds the commands verbatim; follow them.
6. The rehearsal tools, run unchanged:
   - `tools/ray_cluster.sh`, `tools/compare_stores.py`;
   - `.documents/dk-and-rpk-migration/rehearsal/interrupt_when.py` and `stage_counts.py`;
   - `inelastic-reflection` 11's helpers, committed as `.documents/inelastic-reflection/out/11-*.py.txt` and `11-*.sh.txt`
     (`wait_for_line.py`, `beat_times.py`, `ray_state.py`, `silences.py`, `compare_tool.py`, `check.sh`, `suite.sh`), and
     `.documents/inelastic-reflection/measure/compare_jn09.py` and `histories.py`.
7. `.documents/one-loop-drain/derive-05/run_05.py`: how 05 compared a history's candidates with its 02d line by `repr`.
8. `ray/_private/worker.py` in the venv, around `_py_driver_sys_path`: which paths a driver gives its workers (P51).

---

## 0. Where this prompt sits

01–05 put the one-loop law into the integration, the store, the drivers, the exports and a tool. Every test so far ran
without Ray, on temporary stores. This prompt does what the tests cannot (README §8.14):
- **Bitwise:** a fixed-law build at the tip equals one at the campaign's base, `1561b60`;
- **R:** a one-loop build interrupted with `SIGTERM` and resumed equals a straight one, and both equal 02d's tool route;
- **the readers** under Ray: the inventory, both plot scripts with `--A-values`, the refusal of an unstored A, and 05's tool
  on the exports.

It changes no code. It is a rehearsal that README §8.4 names, so its builds are the exception to `CLAUDE.md`'s rule that no
campaign prompt starts a `main.py` run with a datastore. 07 renders its figures on this prompt's exports. 08 closes the
campaign.

## 1. What is wanted

Throughout, `ADDRESS=127.0.0.1:6380`, `S=$PWD/var/datastores` and `L=$PWD/var/one-loop-drain/06`, with `$PWD` the main
checkout's root.

### 1.1 The base tree (P50)

- **The worktree.** `git worktree add --detach <scratch>/base-1561b60 1561b60`, with `<scratch>` your subdirectory of the
  session scratchpad. In it, two symbolic links to this checkout: `venv` and `var`.
  - Through `var`, the base's wrapper writes its run record to this repository's `var/runs` (its `runregistrykit.toml`
    names `var/runs` relative to its own root), and `runregistrykit list` from the main checkout shows it.
  - Record what `git status` in the worktree says about the two links, and the `git_dirty` the base run's manifest
    records. A dirty flag from the links is expected, not a stop.
- **Its code.** Confirm and record:
  - `git diff 1561b60 9616e04` touches no code;
  - `config/version.py` there says `VERSION_LABEL = "2026.9.0"`, and `config/fingerprint.py` `FINGERPRINT_FORMAT = 6`.
- **Afterwards.** It is removed with `git worktree remove` (then `git worktree prune`) once the base store's sidecar is
  written (§1.4). Nothing after that runs from it: the comparisons and R7 read the base store with the tip's code. It is
  gone before the commit.

### 1.2 The cluster and the probe (P51)

- **The cluster.** One cluster for the whole prompt: `tools/ray_cluster.sh start "one-loop-drain 06 (rehearsal)"` from the
  main checkout, with its default of 10 CPUs. Record `status` and the lock's `status` after `start` and after `stop`.
- **The probe,** `rehearse-06/code_probe.py`. It runs as `main.py` runs: `PYTHONPATH=.`, the venv's python, from a tree's
  root, importing `config.ray_auth` first, then `ray.init(address=ADDRESS)`. It prints, from the driver, from one task and
  from one actor:
  - `ComputeTargets.ScalarHistory.__file__` and `ObjectFactories.__file__`;
  - `config.version.VERSION_LABEL` and `config.fingerprint.FINGERPRINT_FORMAT`;
  - the first five entries of `sys.path`.
  It exits 1 if a worker's files or labels differ from the driver's.
- **When it runs:**
  - from the worktree and from the main checkout, before any build;
  - from the main checkout again after the base build ends, before the first tip build.
- **What must hold.** From the worktree every file is under the worktree, at 2026.9.0 and format 6. From the main checkout
  every file is under the main checkout, at 2026.10.0 and format 7.
- **Anything else is a stop.** The two trees agree bitwise under the fixed law, so a base build that ran the tip's code on
  its workers would pass §2.1 and prove nothing.

### 1.3 The grid (P52)

Each store is built by three registered runs, in this order. Every run gets `--config exponential.yaml --database
$S/one-loop-drain-06-<store>.db --shards 16 --job-name one-loop-drain-06-<store>-<run> --ray-address $ADDRESS
--phi-init-Mp 5`, and then:

| run | grid | rows at the base | rows at the tip |
|---|---|---|---|
| `a` | `--M-values-Mp 4e-31 --beta-values 1.06 1.185 2 10` | `--alpha-values 0.01 0.1` | `--alpha-values 0.01 0.1 --A-values 0.1 1` |
| `b` | `--M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5` | `--alpha-values 0 0.1` | the same |
| `c` | `--M-values-Mp 1e-12 --beta-values 1` | `--alpha-values 0.15` | `--alpha-values 0.15 --A-values 1` |

- **The counts.** The base store holds 17 fixed-law histories. The tip's stores hold 26: the same 17, and 9 one-loop
  histories.
- **The one-loop histories** are 02d tasks: P-2188, P-2197, R-865 and R-611 at A = 0.1 and 1, and C-3233 at A = 1.
  Confirm from `var/one-loop-drain/02d/tasks.json` that each one's (β, M, A) is the build's, and that its `argv` gives
  no option other than `--drain-law one_loop --alpha A`, so that the tool ran at `main.py`'s defaults (φ* = 5, the
  tolerances, the network).
- **Slugs:** `base-a`, `base-b`, `base-c`, `reference-a`, …, `resumed-a`, `resumed-a2`, `resumed-b`, `resumed-c`.
- **Purposes** say the store, the run and the grid, as `inelastic-reflection` 11's did, for example
  `"one-loop-drain 06 Bitwise: run a of the base build at 1561b60 (beta 1.06 1.185 2 10 at M 4e-31 Mp, phi* 5, alpha 0.01 0.1)"`.

### 1.4 The builds

- **B0 — `base`,** from the worktree's root, through its own `RunDrivers/registered_run.py` (`--campaign one-loop-drain
  --prompt 06`). Runs `a`, `b`, `c` into `$S/one-loop-drain-06-base.db`. Then `runregistrykit store create --purpose …`
  from the worktree's root, so that the sidecar's fingerprint is the base's format 6.
- **B1 — `reference`,** from the main checkout. Runs `a`, `b`, `c` into `…-reference.db`, then `store create`.
- **B2 — `resumed`** (P55), from the main checkout:
  - run `a` into `…-resumed.db`, sent `SIGTERM` by `interrupt_when.py` with `--banner "RUNNING PIPELINE FOR MODEL
    QCD_Cosmology, A = 0.1 (one-loop-measured-v1)"`;
  - its `--after` is taken from `reference-a`'s log: after at least one A = 0.1 history is stored and before that
    pipeline's scalar stage ends. Say how it was chosen;
  - the run is recorded `killed`. Then run `a` again (`resumed-a2`) with `--prune-unvalidated`, then runs `b` and `c`,
    then `store create`;
  - **a miss.** If the signal falls outside that window, keep the store, name it in the log, and make one further attempt
    into `…-resumed2.db`, with slugs `resumed2-*`. A second miss is a stop.
- **Every run** is launched detached through `sh`, as in `inelastic-reflection` 11's verification §5, with its log and pid
  file under `$L/runs/`. Make **one** check that it started (`check.sh`), then wait in the background for its `** launcher
  saw exit status` line with `wait_for_line.py`. Never poll. A recorder (`beat_times.py`) runs beside each run.
- **Record per run:** its id, slug, store, pid, state, exit status, launch and end, wall time, the load at launch, the
  fingerprint at finish, and `git_head` and `git_dirty` from its manifest.
- **The bound.** If a single run has not ended 60 minutes after its launch, stop and ask. Do not kill it unasked.

### 1.5 The readers (P56)

Unregistered, on the reference store unless stated, with the cluster up. Before and after each, take `shasum -a 256` of the
store's files.
- **R1** — `main.py --inventory` (no Ray).
- **R2** — `plot_by_beta.py --phi-init-Mp 5 --alpha-values 0 0.01 0.1 0.15 --A-values 0.1 1 --output $L/plot-reference`.
- **R3** — `plot_ScalarHistory.py` with the same options and `--output $L/plot-reference`.
- **R4** — `plot_by_beta.py --phi-init-Mp 5 --A-values 0.5 --output $L/plot-unstored`. It must exit 2, name `--A-values`
  and `0.5`, and write nothing under its output.
- **R5** — R2 and R3 on the resumed store, `--output $L/plot-resumed`.
- **R6** — `tools/quanta_measures.py` on `$L/plot-reference/QCD_Cosmology` and on `$L/plot-resumed/QCD_Cosmology`, at
  c_a = 1, 0 and 10 (05's log §7). It needs no cluster.
- **R7** — `tools/bbn_from_store.py` (no Ray, one PRyMordial solve each):
  - on the base store and on the reference store, the same fixed-law history: M = 4 × 10⁻³¹ M_P, β = 1.185, α = 0.1;
  - on the reference store, the one-loop history at β = 1.185, A = 1 (`--drain-law one_loop --alpha 1`).
  Each must print the stored Y_p and D/H. The base store is format 6, with no `drain_model` column (P39).

Stop the cluster after R5 with `ray_cluster.sh stop`, and record its `status` and the lock's `status`.

### 1.6 The scripts

Under `.documents/one-loop-drain/rehearse-06/`. Each has a docstring that says what it does, how it is run, what it reads
and that it writes nothing but stdout or files under `var/one-loop-drain/06/`. Each reads a store only through `sqlite3`
`file:…?mode=ro` URIs, never through `datastorekit` or Ray.
- **`code_probe.py`** (§1.2).
- **The helpers**, copied from `inelastic-reflection` 11 and adapted to this prompt's paths: `wait_for_line.py`,
  `beat_times.py`, `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`. Each adaptation is shown by a diff in `out/`.
- **`histories.py`**: one line per stored history with its key, the tuning's outputs and iterations, the tuning wall,
  and, for a one-loop history, the drain record's counts and shares and its bounce rows by source and regime. It is
  adapted from `inelastic-reflection` 11's.
- **`compare_base.py BASE_STEM TIP_STEM`** (P53; §2.1). It is adapted from `compare_jn09.py`.
- **`compare_02d.py STORE_STEM`** (P54 (ii); §2.2). It is adapted from `compare_tool.py` and from `run_05.py`'s
  comparison of candidates.
- **`compare_exports.py ROOT_A ROOT_B`** (P54 (iii); §2.2). It compares every CSV file under two export roots byte
  for byte. Where two files differ it names the columns that differ. A column that holds a timing is named as such, and
  any other difference is reported as one.

No test module is added, and no suite's count changes.

## 2. The gates

### 2.1 Bitwise (P53)

`compare_base.py` on the base store and the reference store:
- **Matched:** each of the base's 17 histories, by (M, β, φ*, α) through its model, `beta_value`, `phi_value` and
  `alpha_value` rows, and at the tip under `drain_model` = `fixed`. A sample is matched by ln(1 + z). Every history must
  be matched exactly once in each store.
- **Compared bitwise:** every column both schemas hold in `ScalarHistory`, `ScalarHistoryValue`, `AdiabaticHistory`,
  `AdiabaticHistoryValue`, `BBNData` and `BBNDataValue`, and in the key tables. Floats are compared by `float.hex`, the
  rest exactly, and NULL equals only NULL. `extra_data` is compared key by key over the keys both hold.
- **Left out, by name** (and printed): `serial` and every `*_serial` column, `version`, `label`, `timestamp`, and the timing
  columns of `compare_jn09.py`.
- **The tip's new columns,** at each matched history, against 03's rules:
  - `alpha_value.drain_model` is `fixed`;
  - every drain-record column of `ScalarHistory` is NULL, and `bounce_rows` is what 03 sets under the fixed law;
  - `ScalarHistoryBounce` holds no row of it;
  - `log_m_eff` and `log_m_tracker` at every sample: count the filled and the NULL. Each NULL must be one that 03's
    `sample_masses` gives as None; say how that was checked.
- **The count:** the number of values compared, per table.
- **The exceptions** (README §8.14). A history that the base stores as a failure, with a reason that is the wall search's
  raise (P21) or G2's refusal (ruling (b)), and that the tip stores as a success, is listed with both reasons. It is not
  a difference. A history that fails at both with equal reasons is a match.
- **The gate holds** when every compared value is equal and every new column obeys 03's rules. Any other difference is a
  stop.

### 2.2 R (P54)

- **(i) The stores agree.** `tools/compare_stores.py` on the reference and resumed stores prints `RESULT: equal` and
  `problems: none`. Its table list includes `ScalarHistoryBounce` with the rows matched; if it does not, stop and ask.
  `runregistrykit store fingerprint` on each closed store gives the same fingerprint, at format 7.
- **(ii) Against 02d's tool route.** `compare_02d.py` on the reference store, and on the resumed store, for each of the
  nine one-loop histories against its 02d line in `var/one-loop-drain/02d/sample.jsonl`:
  - **bitwise, a difference a stop:**
    - the tuning's fifteen keys (`normalization`, `V_ref`, `phi_ref`, `phi_0`, `pi_0`, `tracking`, the two residuals,
      `tuning_iterations`, the three step counts, `seed_kind`, `seed_normalization`, `seed_phi_ref`);
    - every field of the stored drain record against the line's `accepted.drain_values`;
    - every candidate's bounce row against the `DrainCandidate` of the same ordinal in the last line of
      `var/one-loop-drain/02d/tasks/<id>.partial.jsonl`, by `repr`, field by field, as `run_05.py` compared them;
  - **to the digits the line prints:** the dissipation values, ΔN_eff, and Y_p and D/H. A difference is reported with
    its size. It is a stop unless the log explains it.
- **(iii) The exports.** `compare_exports.py` on `$L/plot-reference/QCD_Cosmology` and `$L/plot-resumed/QCD_Cosmology`,
  after R6:
  - histories.csv, every fields.csv and bounces.csv, and the tool's three files at the three scales are byte-identical,
    or differ only in named timing columns;
  - each of the nine bounces.csv against 05's export of the same 02d task under `var/one-loop-drain/05/exports/QCD_Cosmology/`:
    byte-identical. A difference is a stop;
  - the nine histories' rows of quanta_histories.csv against 05's rows for the same tasks: report what differs and why.
    That is a measurement, not a gate. 05's fields.csv had no adiabatic or BBN columns behind it, so E and the K check
    may legitimately differ there.
- **The gate holds** when (i), (ii)'s bitwise items and (iii)'s byte-identical items all pass.

## 3. Measurement

As `inelastic-reflection` 11's verification §2, in the verification document:
- **The cluster and the probe:** the probe's output from both trees, before and after the base build.
- **The builds:** the per-run table of §1.4; the histories by `histories.py` for each store, and, for the resumed store,
  after `resumed-a` and at the end.
- **The interruption:** the delay chosen and why; when the signal fell; what was stored by then (`stage_counts.py`); the
  Ray processes after it (`ray_state.py`, `ray_cluster.sh status`); what the resume recomputed and what it pruned.
- **The silences:** the longest heartbeat silence of each run, against the 900 s window.
- **The costs:** each build's wall and its stages; each history's tuning integrations and wall; the bounce rows written
  per one-loop history and in total; the stores' sizes on disk, the base's beside the tip's.
- **The readers:** R1's report (the one-loop values and their labels); R2's and R3's products at their paths
  (`<label>/alpha=<α>/`, `<label>/one-loop-measured-v1/A=<A>/`, histories.csv with 26 rows in P36's sort, and a
  bounces.csv for each one-loop history and none for a fixed-law one); R4's exit and message; R6's summaries at the three
  scales; R7's values; each reader's wall; and the store's `shasum` unchanged by every reader.
- **The figures:** look at `Delta_N_eff.pdf` and at one one-loop history's figures (as PNG), and say what they show.
- **What the base build shows of the exceptions**, if any arose: which histories, and their reasons.

## 4. Verification

1. **Before anything else:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves and
   `tools/tests` alone. Expect 26 / 665 (219 + 446) / 163 / 191 / 72. A different count, or a failure, is a stop.
2. **After the cluster is stopped:** the same five suites, unchanged in count. No code changed, so nothing else is
   expected. Record the counts, the times and `uptime`.
3. `git status` shows only the files of §6. Nothing under `var/` is committed, and `git worktree list` shows the main
   checkout only.
4. No ChamPBH Ray process outlives `ray_cluster.sh stop`, and the lock is released. `runregistrykit list` shows every run
   of this prompt `done` or `killed`, none `running` or stale.
5. `grep -r -F -l -f ~/.ray/auth_token` over everything this prompt commits finds nothing. Never print the token.

## 5. Acceptance

1. **The gate Bitwise** (§2.1) and **the gate R** (§2.2) hold, item by item, in the verification document and the log.
2. **The measurement** of §3.
3. **The verification document:** `.documents/one-loop-drain-verification.md`, a new §6, additive (invariant 6). It holds
   the gates, the measurement, the commands verbatim, and "Observed, not acted on". §1–§5 stay as they are.
4. **`.documents/one-loop-drain/rehearse-06/`:** the scripts of §1.6 and an `out/` of small text only:
   - the runs' logs, with ANSI codes removed;
   - the manifests, status files and sidecars;
   - the recorders' outputs and the probe's;
   - the comparisons, histories.csv and the tool's quanta_histories.csv at the three scales;
   - the inventory, the loads, and the helpers' diffs.

   No PDF and no PNG; the figures stay under `$L`, and the log names them.
5. **The log**, `logs/06-rehearse.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - the suite counts before and after;
   - the gates item by item;
   - every deviation, classified;
   - "Observations not acted on";
   - "State handed to the next prompt". For 07: where the exports and the tool's outputs are, which histories they hold,
     and what in them differs from 05's exports. For 08: the stores and where they are, the gates, and the costs measured,
     for the rebuild's handover.
6. **The board:**
   - row 06 and its items;
   - the gates Bitwise and R in §2;
   - an entry in §5;
   - **Last updated**;
   - anything opened, in §3 and in `.documents/OPEN_ISSUES.md`, in the same commit (`CLAUDE.md`).
7. **One commit**, in `CLAUDE.md`'s form, staged by explicit path, with `git diff --cached --name-status` shown before
   committing. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 6. Stop conditions: stop and ask

- An item of the Gate at the top fails, or a baseline count differs or fails.
- **The lock.** `ray_cluster.sh` or the lock gives exit 3: stop. Never wait, retry or remove the lock. Exit 4: run
  `stop`, then stop and ask.
- **The probe** (§1.2) shows a worker running another tree's code, or a label other than its tree's.
- **A gate item fails** (§2.1, §2.2), or a one-loop history of the build is not the 02d task §1.3 names.
- **A driver or reader fails.** It errors, refuses where it should run, runs where it should refuse, or changes the store's
  `shasum`. That is a defect of 04's or 05's work. Record it; a lettered prompt fixes it, not this one.
- **The interruption misses twice** (§1.4).
- **A run has not ended 60 minutes after its launch.**
- **The work seems to need a change** to any code, any test, `.gitignore` or the git configuration.

A history that fails in both builds with equal reasons is a row, not a stop. Report it with its reason.

## 7. What this prompt does not do

- **No code changes.** No test is added or edited, and no golden is written.
- **No `ray stop`**, and no run but the builds of §1.4, the probe and the readers of §1.5.
- **No fix** for anything found. It is recorded, and opened if it is a defect (README P15).
- **No change** to `CLAUDE.md`, the architecture summary or `numerical-strategies.md`. Those are 08's.
- **No checkout** of another commit in the main checkout, and no merge or push. `PRyM/`, `thirdparty/` and the packages
  are not touched.
- **Files it changes:**
  - `.documents/one-loop-drain-verification.md` (§6, new);
  - `.documents/one-loop-drain/rehearse-06/` (new: the scripts and `out/`);
  - the log, this board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Nothing under `var/` is committed. Any other file is a deviation to classify.
