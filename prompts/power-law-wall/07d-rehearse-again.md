# Prompt 07d — the rehearsal again, at one k_ex

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-18, after 07c landed (`c8461ab`, reviewed `2e3c551`), at the user's request: "Please write 07d (and
an orchestration note, if needed) now". It is written from:
- README §2 row 07d, §2.1 (the gate R2), §5, §6.12 (P87–P89), §6.14 (P109) and §6.15 (P111–P124);
- prompt 07, its orchestrator note and its log, and `.documents/power-law-wall-verification.md` §6 (07's commands
  verbatim), whose protocol this prompt repeats at the tip;
- 07c's log §7 and verification §9 (what moves, and by how much, in the tool route);
- the planner's survey at `2e3c551` (orchestrator note 07d §0).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Amended 2026-11-19, after its first attempt stopped** (README §6.16, P133; the user's ruling: "Try (a)"). The first
attempt stopped at §6 and P114: five power-wall histories at n = 1, β = 2.9, M = 4 × 10⁻³¹ M_P, built by 07, failed at
the tip, the pulse rate overflowing. 07c2 forms the rate in closed form. This prompt then runs again as written, with
these substitutions, which govern where the text below differs:
- **The Gate:** `HEAD` at or after the commit that records the orchestrator's review of 07c2; `VERSION_LABEL =
  "2026.14.0"`; the suites' counts are those 07c2's review records, before and after (P123's "07c's" counts are
  replaced by them).
- **The names:** the stores `$S/power-law-wall-07d-r2-{exp,pl,pl-resumed,mixed}.db` (`power-law-wall-07d-r2-pl-resumed2`
  on a miss); the job names `power-law-wall-07d-r2-<store>-<run>`; the slugs `r2-<store>-<run>` (the resume
  `r2-pl-resumed-a2`); `L=<worktree>/var/power-law-wall/07d-r2`. The Gate's "none exists" is about these names, and
  about `.documents/power-law-wall/rehearse-07d/`.
- **The stopped attempt's things** (P134): `$S/power-law-wall-07d-{exp,pl}*`, its run records and
  `var/power-law-wall/07d/` stay where they are and are read by nothing of this prompt, except that its scripts under
  `var/power-law-wall/07d/rehearse-07d-attempt1/` may be the starting point for `rehearse-07d/`, read as data, each
  adaptation still shown by a diff against `rehearse-07/`.
- **The probe** (§1.1) expects 2026.14.0. **P120's third hook** is reported against the end of the range 07c2 measured
  (where V″ leaves float range), not against (ln V)‴'s.
- **The verification document's section** is §11, not §10 (07c2 wrote §10); §1–§10 stay as they are.
- **The commit** lists `.documents/power-law-wall-verification.md` (§11, new) in place of §10.

**Where it runs.** In the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, from its root, with
`PYTHONPATH=.` and `./venv/bin/python`. One tree builds; there is no base worktree. Throughout:
- `ADDRESS=127.0.0.1:6380`;
- `MAIN=/Users/ds283/Documents/Code/ChamPBH`, the main checkout, whose `var/` the registry uses (P61);
- `S=$MAIN/var/datastores`, where the stores go and where 07's are;
- `L=<worktree>/var/power-law-wall/07d`, where the launch logs, the exports and the comparisons go;
- `L07=<worktree>/var/power-law-wall/07`, 07's exports, read only.

**Gate:**
- in the worktree, on branch `power-law-wall`, at or after the commit that adds this prompt; `c8461ab` and `2e3c551` are
  ancestors of `HEAD`; `config/version.py` has `VERSION_LABEL = "2026.13.0"`;
- `git status`: no tracked change; the untracked `venv` and nothing else; `git worktree list`: the main checkout and this
  worktree, nothing else;
- `tools/ray_cluster.sh status`: the lock free, no ChamPBH Ray process, the four ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- 07's four tip stores, `$S/power-law-wall-07-{exp,pl,pl-resumed,mixed}.db`, with their shards and sidecars;
  `runregistrykit store fingerprint` (without `--write`) on each gives 07's digest: `2092b200…` (exp), `9d4fcf02…` (pl and
  pl-resumed), `7d7a1a4c…` (mixed), in full in `.documents/power-law-wall/rehearse-07/out/07-<store>-store.sidecar.json`;
- `$L07` holds `export-{exp,pl,pl-resumed,mixed}/`;
- none of `$S/power-law-wall-07d-*`, `$L` and `.documents/power-law-wall/rehearse-07d/` exists.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus** (README §2). Real builds under real Ray, one interrupted on purpose, compared across a
change of physics with a named set of columns allowed to move.

**Read first:**
1. [`README.md`](README.md): §2.1's gate R2, §5, §6.12 (P87–P89), §6.14 (P109) and §6.15 (P111–P124) whole.
2. `CLAUDE.md`: "Long-running jobs — the run registry"; "Ray on this machine", "The rehearsal lock", "Ray's token mode";
   "The plot scripts".
3. [`07-rehearse.md`](07-rehearse.md) whole, and [`logs/07-rehearse.md`](logs/07-rehearse.md) (its deviations, D1 onward,
   are what went wrong last time) and `.documents/power-law-wall-verification.md` §6, which holds 07's commands verbatim.
4. [`logs/07c-k-ex-in-production.md`](logs/07c-k-ex-in-production.md) §6 and §7, and the verification document §9.7
   (the tool route's moved columns).
5. The scripts to copy and adapt: `.documents/power-law-wall/rehearse-07/` (all of them).
6. The tools, run unchanged: `tools/ray_cluster.sh`, `tools/compare_stores.py`, `tools/bbn_from_store.py`,
   `tools/quanta_measures.py`, `tools/plot_quanta_grid.py`; `.documents/dk-and-rpk-migration/rehearsal/interrupt_when.py`
   and `stage_counts.py`.
7. `ObjectFactories/ScalarHistory.py`'s `ScalarHistory` and `ScalarHistoryBounce` columns, and
   `ComputeTargets/ScalarHistory.py`'s `DrainHistoryValues` and `build_extra_data`: where each k_ex column of P114 is
   written from.

The earlier prompts' outputs and the notes are data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

07c put one k_ex, ½E^{1/2}|V‴/V″|, at every use, and the exponential's one-loop law `one-loop-exponential-v2`, into
production (`c8461ab`). Its tool route showed, on five histories, that a history whose law reads no ℓ keeps its
integration and BBN bit for bit, and that only the columns k_ex defines move. This prompt shows the same through the
whole pipeline, on 07's grid, in stores built under Ray:
- **no ℓ, no change:** every fixed-law, natural-mode and power-wall history of 07's grid, built at the tip, equals 07's
  in every stored value but the k_ex columns (P114), which are listed with how far each moved;
- **the exponential's one-loop histories, measured:** under v2 at the tip beside v1 in 07's stores (P115);
- **the tip still resumes and mixes:** an interrupted build equals a straight one; two potentials in one store equal each
  alone (P116);
- **the readers**, on every store, and the label refusing a 2026.12.0 store (P117).

It changes no code. It is the rehearsal README §2 names, so its builds are the exception to `CLAUDE.md`'s rule that no
campaign prompt starts a `main.py` run with a datastore. 08 closes the campaign from its stores and its costs (P124).

## 1. What is wanted

### 1.1 The cluster and the probe (P112)

- One cluster, from this worktree's root: `tools/ray_cluster.sh start "power-law-wall 07d (rehearsal, tip)"`, alone in its
  Bash call, with `PYTHONPATH` unset. Record `status` and the lock's `status` after `start` and after `stop`.
- 07's `code_probe.py`, copied to `rehearse-07d/` and changed only where its expectations name 07's labels, run once the
  cluster is up and before the first build. **What must hold:** every module file under this worktree, the label
  2026.13.0 and the format 8 in the driver, the task and the actor; every working directory this worktree's root; the
  data files' hashes equal in every process and equal to those in 07's probe output (`rehearse-07/out/`). **Anything else
  is a stop.**

### 1.2 The grid (P111)

07's grid exactly (prompt 07 §1.3): runs `a`, `b` and `c`; the stores `exp`, `pl`, `pl-resumed` and `mixed` with 07's
configs, potentials and n. Every run gets `--database $S/power-law-wall-07d-<store>.db --shards 16 --phi-init-Mp 5
--ray-address $ADDRESS --job-name power-law-wall-07d-<store>-<run>`, and otherwise the arguments 07's run of the same
slug had, as `.documents/power-law-wall-verification.md` §6 gives them. Before the first build, write each run's command
beside 07's and show that they differ only in `--database` and `--job-name`.

- **Slugs:** `<store>-<run>`, as 07's; the resume is `pl-resumed-a2`.
- **Purposes** say the prompt, the store, the run and the grid, as 07's, with `07d` and "at the tip, one k_ex".
- **The counts per store** are 07's: 20, 40, 40 and 40. A history that fails is a row, not a stop.

### 1.3 The builds

- **In this order:** `exp`, `pl`, `pl-resumed` (with the interruption), `mixed`, each its three runs and then
  `runregistrykit store create --purpose …` from this worktree's root.
- **Through the wrapper:** `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign power-law-wall --prompt
  07d --register <slug> --purpose "…" -- <main.py's arguments>`, launched detached through `sh` as 07 launched it, with
  its log and pid file under `$L/runs/`; **one** check that it started (`check.sh`), then a background wait for its
  `** launcher saw exit status` line (`wait_for_line.py`), never a poll; a recorder (`beat_times.py`) beside each run.
- **The interruption** (P113), in `pl-resumed`'s run `a`, as 07's §1.4:
  - `interrupt_when.py` with `--banner "RUNNING PIPELINE FOR MODEL QCD_Cosmology, A = 0.05 (one-loop-inverse-power-v1)"`;
    confirm the banner's text from 07d's `pl-a` log first;
  - `--after` from 07d's `pl-a` beats: the window opens at the first line showing an A = 0.05 history stored and closes at
    the line ending that pipeline's scalar stage; the midpoint rounded down to a whole second, or the opening + 2 s if the
    window is shorter than 10 s. Name both lines and the loads at both launches;
  - the run is recorded `killed`; then `pl-resumed-a2` with `--prune-unvalidated`; then runs `b` and `c`;
  - **a miss:** keep the store, name it, and make one further attempt into `power-law-wall-07d-pl-resumed2.db` (slugs
    `pl-resumed2-*`). A second miss is a stop.
- **Record per run:** its id, slug, store, pid, state, exit status, launch and end, wall time, the load at launch, the
  fingerprint at finish, and `git_head`, `git_dirty` and the script hash from its manifest. Each manifest's `git_head` is
  a commit of this branch at or after this prompt's, and its script hash that of the tip's `main.py`
  (`git show <git_head>:main.py | shasum -a 256`). A mismatch is a stop.
- **The bound.** A run not ended 60 minutes after its launch is a stop. Do not kill it unasked.

### 1.4 The readers (P117)

Unregistered. Before and after each, `shasum -a 256` of the store's files; a change is a stop. Each takes `--config <its
store's config> --database $S/power-law-wall-07d-<store>.db --shards 16 --phi-init-Mp 5`, the store's potentials and n,
and the plot scripts `--ray-address $ADDRESS` and the alpha rows the store holds, as 07's readers did.
- **R1 — the inventory,** `main.py --inventory`, on each of 07d's stores: `problems: none`.
- **R2 — the exports without figures:** both plot scripts with `--no-plots`, `--output $L/export-<store>`; `mixed` in one
  plot run with `--models exponential inverse-power`.
- **R3 — the figures, on `exp`:** both plot scripts without `--no-plots`, `--output $L/plot-exp`.
- **R4 — the refusals.** Each must exit 2, name the option and the value, and write nothing under its `--output`:
  - `plot_by_beta.py --models inverse-power` on `exp`;
  - `plot_by_beta.py --A-values 0.5` on `pl`;
  - `main.py --config inverse_power.yaml --n-values 3 --A-values 1 …` at parse, before Ray, against a database path
    that does not exist (and is not created);
  - `main.py --config exponential.yaml --n-values 3 --A-values 1 …` likewise (U14).
- **R5 — the quanta tools,** no cluster needed: `tools/quanta_measures.py` at c_a = 1 on every root of R2 (one per
  potential, `mixed`'s two); each row's `spectrum_model` must be `R8-<wall>-v2` for its potential and n. Then
  `tools/plot_quanta_grid.py` on `export-pl`'s root; the open issue `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`
  expects it to refuse. Its exit and message are a measurement, not a stop.
- **R6 — the BBN re-solve,** `tools/bbn_from_store.py --small-network --variant prod`, no Ray, at M = 4 × 10⁻³¹ M_P and
  β = 2.0: on `exp` at `--alpha 0.1`, and at `--drain-law one_loop --alpha 1`; on `pl` with `--model inverse-power
  --drain-law one_loop --alpha 1` at `--n 1` and at `--n 2`. Each `prod` line equals the stored Y_p and D/H to the ten
  significant digits it prints.
- **R7 — the histories, read:** `histories.py` on each of 07d's stores.
- **R8 — U12's guard,** last of all, after every comparison of §2: `plot_by_beta.py --no-plots` from this worktree on
  07's `exp` store (`$S/power-law-wall-07-exp.db`), with 07's R2 options for that store and `--output $L/refuse-07-exp`.
  It must exit without writing any export file, and leave the store's `shasum` unchanged. Record its exit and what it
  prints. A run that exports 07's rows is a stop.

Stop the cluster after the last reader that needs it, with `ray_cluster.sh stop`.

### 1.5 The scripts (P122)

Under `.documents/power-law-wall/rehearse-07d/`, each with a docstring saying what it does, how it is run, what it reads,
and that it writes nothing but stdout or files under `$L`. Each reads a store only through `sqlite3` `file:…?mode=ro`
URIs, never through `datastorekit` or Ray. 07's scripts are copied, not edited in place; each adaptation is shown by a
diff in `out/`.
- **The helpers** from `rehearse-07/`: `code_probe.py`, `wait_for_line.py`, `beat_times.py`, `ray_state.py`,
  `silences.py`, `check.sh`, `suite.sh`, `histories.py`.
- **`compare_histories.py`**, 07's, with a second mode, `--across-k-ex` (P114): the drain-model map of 07's names to
  07d's (`one-loop-measured-pl-v1` → `one-loop-inverse-power-v1`; with `--include-exponential-one-loop`,
  `one-loop-measured-v1` → `one-loop-exponential-v2`, used only by P115's selection); the k_ex columns of P114 compared
  and reported (per column, the count of values that differ and the largest change), not failed; every other column as
  07's mode, exit 1 on any difference. Under the power wall's law it also checks that each history's
  `drained_share_<regime>` sum and every candidate's `delta_K_Mp4` are bitwise equal, and that no candidate assessed in A
  is `unassessed` in B. Without the flag it is 07's script.
- **`compare_v2.py A_STEM B_STEM`** (new, P115): the exponential's one-loop histories of 07's store (A) against 07d's
  (B), matched by key across the map; per history, P115's quantities, A beside B, and the difference.
- **`compare_exports.py`**, 07's, with a mode for P118: two roots of different trees, a one-loop directory under 07's
  drain-model name mapped to 07d's, and per file the columns that differ, each with its largest change and whether P114
  lets it move.
- **`margins.py`** (new, P119) and **`domain.py`** (new, P120), read-only on the stores.

No test module is added, and no suite's count changes.

## 2. The gate R2 (README §2.1, as P114–P118 make it exact)

### 2.1 No ℓ, no change (P114)

`compare_histories.py --across-k-ex`, 07's store as A and 07d's as B:
- `exp` against `exp`, restricted to the fixed law and natural mode (the drain model `fixed`): nine histories (run `a`'s
  six α rows and run `b`'s three) matched;
- `pl` against `pl`, all forty; `pl-resumed` against `pl-resumed`, all forty;
- `mixed` against `mixed`, the power wall's twenty and the exponential's fixed-law and natural-mode nine.

Every history matched exactly once each way; every column outside P114's list equal by `float.hex`; each k_ex column
that moves listed. **Any other difference is a stop.** A history that fails in both with equal reasons (masked as 07's
ruling (a) says) is a match.

### 2.2 The exponential's one-loop histories, measured (P115)

`compare_v2.py` on `exp` (07 against 07d) and on `mixed`'s exponential: the eleven one-loop histories of each (run `a`'s
nine A rows and run `c`'s two), P115's table. Measured, not judged; a history that fails in one store only is reported
with its reason.

### 2.3 The tip's own identities (P116)

- `tools/compare_stores.py` on 07d's `pl` and `pl-resumed`: `RESULT: equal`, `problems: none`, with `InversePowerModel` and
  `ScalarHistoryBounce` in its table list; `store fingerprint` on each the same, at format 8; their exports and R5 outputs
  byte-identical, or differing only in named timing columns.
- `compare_histories.py` (07's mode) on 07d's `mixed` against 07d's `exp` with `--model exponential`, and against 07d's
  `pl` with `--model inverse-power --n 1`: twenty matched each, every shared column equal; the exports as 07's §2.4.

**Any difference is a stop.**

### 2.4 The exports against 07's (P118)

`compare_exports.py` (P118's mode) on each of 07d's roots against 07's same root. Where the law reads no ℓ, only the
columns P114 lets move may differ, and the drain-model column and directory under U11; anything else is a stop. The
exponential's one-loop files and every quanta file are measured: which columns differ, and by how much.

### 2.5 The readers (P117)

R1 `problems: none` on every store; R4's four refusals; R5's `spectrum_model`s; R6's re-solves equal to the stored
values; R8's refusal; every store's `shasum` unchanged by every reader, 07's `exp` included.

**The gate R2 holds** when §2.1 and §2.3–§2.5 hold, item by item, and §2.2 is measured.

## 3. Measurement

In `.documents/power-law-wall-verification.md` §10 (new), as §6 is laid out:
- **The cluster and the probe:** the probe's output, beside 07's hashes.
- **The builds:** the per-run table of §1.3; `histories.py` for each store, and for `pl-resumed` after `pl-resumed-a` and
  at the end.
- **The interruption:** the window, the delay and why; when the signal fell; what was stored by then (`stage_counts.py`);
  the Ray processes after it; what the resume pruned and recomputed.
- **The silences:** each run's longest heartbeat silence, against the 900 s window.
- **No ℓ, no change:** for each store, every k_ex column that moved, with its count and its largest change, by potential,
  n and law; beside 07c's tool route (verification §9.7).
- **The exponential under v2** (P115's table), with the drained share and ΔN_eff by β and A at each M.
- **The margins** (P119): the failing shares and worst margins, 07 beside 07d, and how many rose, fell or held.
- **The domain and the third hook** (P120).
- **The costs** (P121): each store's wall and its stages, each history's tuning integrations and wall, beside 07's.
- **The readers:** each reader's exit, wall and output paths; R4's messages; R5's summaries; R6's values; R8's output.
- **The figures:** look at `$L/plot-exp/QCD_Cosmology/exponential/`'s `Delta_N_eff.pdf` beside 07's `export-exp`
  values, and at one exponential one-loop history's figures (as PNG), and say what they show.

## 4. Verification

1. **Before anything else:** the five suites as `CLAUDE.md` says, ComputeTargets in its two halves and `tools/tests`
   alone, serially, with the time and the load at each start. Expect 26, 805 (226 + 579), 175, 312 and 79, with the two
   worktree failures (`test_registered_run … test_the_script_is_main_py…` and `test_registry_config …
   test_roots_and_distributions`) and no other. A different count, or another failure, is a stop.
2. **After:** no code changed, so the counts are taken by discovery (P123) and must equal the first.
3. `git status` shows only §5's files. Nothing under `var/` is committed. `git worktree list` shows the main checkout and
   this worktree only.
4. No ChamPBH Ray process outlives the `ray_cluster.sh stop`, and the lock is released. `runregistrykit list` shows every
   run of this prompt `done` or `killed`, none `running` or stale.
5. `grep -r -F -l -f ~/.ray/auth_token` over everything this prompt commits finds nothing. Never print the token.

## 5. Acceptance

1. **The gate R2** (§2) item by item, in the verification document and the log.
2. **The measurement** of §3.
3. **The verification document:** a new §10, additive (invariant 6), with the gate, the measurement, the commands
   verbatim, and "Observed, not acted on". §1–§9 stay as they are.
4. **`.documents/power-law-wall/rehearse-07d/`:** the scripts of §1.5 and an `out/` of small text only: the runs' logs with
   ANSI codes removed; the manifests, status files and sidecars; the recorders' and the probe's outputs; the comparisons;
   each store's `histories.py` output and each export's `histories.csv`; R5's outputs; the inventories, the loads and
   the helpers' diffs. No PDF and no PNG; the figures stay under `$L`, and the log names them.
5. **The log,** `logs/07d-rehearse-again.md`, in the template of `dk-and-rpk-migration` README §5.1, with the suite
   counts before and after, the gate item by item, every deviation classified, "Observations not acted on", and "State
   handed to the next prompt", for 08: the stores and where they are, the exports, the gate, what moved, the costs, and
   every observation that bears on the science run.
6. **The board:** row 07d and its items; the gate R2 in §2; an entry in §5; **Last updated**; anything opened, in §3 and
   in `.documents/OPEN_ISSUES.md` in the same commit.
7. **One commit,** in `CLAUDE.md`'s form, staged by explicit path, with `git diff --cached --name-status` shown before
   committing. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 6. Stop conditions: stop and ask

- An item of the Gate fails, or a baseline count differs, or a suite fails other than as §4 expects.
- **The lock.** `ray_cluster.sh` or the lock gives exit 3: stop. Never wait, retry or remove the lock. Exit 4: run `stop`,
  then stop and ask.
- **The probe** (§1.1) shows a worker running other code, another label or format, another working directory, or other
  data files.
- **A manifest** whose `git_head` or script hash is not the tip's.
- **A gate item fails** (§2.1, §2.3–§2.5): in particular any value outside P114's list that differs in a history whose
  law reads no ℓ, or a power-wall candidate that 07 assessed and 07d does not.
- **A driver or reader fails:** it errors, refuses where it should run, runs where it should refuse, or changes a store's
  `shasum`. Record it; a lettered prompt fixes it, not this one.
- **The interruption misses twice** (§1.3).
- **A run has not ended 60 minutes after its launch.**
- **The work seems to need a change** to any code, any test, any config file, `.gitignore` or the git configuration.

A history that fails with the same reason in 07's store and 07d's is a row, not a stop. The exponential's one-loop
histories are measured (§2.2), and nothing in them is a stop.

## 7. What this prompt does not do

- **No code changes.** No test, golden or config file is added or edited.
- **No `ray stop`**, and no run but the builds of §1.3, the probe and the readers of §1.4.
- **No fix** for anything found. It is recorded, and opened on the board if it is a defect.
- **07's stores and exports are not written,** retired or moved; they are read through `sqlite3` `mode=ro` and by R8 only.
- **No change** to `CLAUDE.md`, the architecture summary, `numerical-strategies.md` or `science-runs/`. Those are 08's.
- **No tracked file of the main checkout** is read or written; only the registry's records and the stores are added to its
  ignored `var/` (P61). No merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched.
- **Files it changes:**
  - `.documents/power-law-wall-verification.md` (§10, new);
  - `.documents/power-law-wall/rehearse-07d/` (new: the scripts and `out/`);
  - the log, this board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Nothing under `var/` is committed. Any other file is a deviation to classify.
