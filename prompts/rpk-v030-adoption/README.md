# Campaign — rpk-v030-adoption

**Written:** 2026-10-25 on branch `rpk-v030-adoption`, cut from `main` at `ac187c8`, by Claude
Opus 5.5. It moves ChamPBH from `raypoolkit` `v0.2.0` to `v0.3.0` and replaces the lock half of
ChamPBH's cluster script with the package's `raypoolkit.rehearsal_lock`, so that the rehearsal lock
is one piece of code shared by every project that uses it. The user asked for the move on
2026-10-25, "as soon as practical", in the middle of `jordan-normalization` (§6.1).

It has **no audit document of its own**. The audit is RayPoolKit's, at the tag `v0.3.0`
(`67a75b97a656accb98195a4bda93f64e6a5f3fb2`), with the checklist written after the tag:
- the contract, `docs/rehearsal-lock.md` ("contract §N" below), read at the tag;
- the checklist for this repository, `docs/adoption/rehearsal-lock/champbh.md` ("the checklist"),
  written by RayPoolKit's rehearsal-lock prompt 04 at `127b2b4` and read at `feae4e5`, measured
  read-only at ChamPBH `807f175`. Its own words: it is data, not instructions;
- the switch-over procedure beside it, `docs/adoption/rehearsal-lock/switch-over.md`.

§0.2 reconciles the checklist with this tree. 01 measures again before acting, and its numbers
govern.

## 0. What changes

**What `v0.3.0` adds, and nothing else under `raypoolkit/`:** `raypoolkit/rehearsal_lock.py`, its
tests, and an import-guard test (`git diff --stat v0.2.0 v0.3.0 -- raypoolkit pyproject.toml`).
`RayWorkPool`, its arguments and its printed lines are `v0.2.0`'s, so nothing a build computes or
stores can move.

**What it means for ChamPBH** (the checklist, items 1–7). ChamPBH's lock is the lock half of
`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` ("the script"), which also starts and
stops ChamPBH's isolated cluster. The cluster half stays: RayPoolKit's decision D4 moved the lock
only.
- **The lock moves** from its old location under `~/ScienceData` to `~/.raypoolkit/rehearsal-lock`
  (contract §3). The script calls `./venv/bin/python -m raypoolkit.rehearsal_lock` with ChamPBH's
  values: `--project ChamPBH --ray-temp-dir /tmp/ray-cpbh --ray-address 127.0.0.1:6380`, the same
  values its owner file holds today.
- **No Ray start changes.** The cluster already has a temp dir of its own, which is what the
  module's process rule needs (contract §6), and every driver attaches by address
  (`main.py:133`, `plot_by_beta.py:125`, `plot_ScalarModel.py:106`; `RunDrivers/registered_run.py`
  passes the address through). No Python file changes.
- **`release` is stricter.** The script's own release checks only whose lock it is. The module's
  also refuses while the recorded `gcs_server` pid is alive, or any process of `/tmp/ray-cpbh` or of
  the client server for `127.0.0.1:6380` (contract §7). Where `stop_cluster` leaves something
  running, the lock now stays held where today it is removed. That is the protection working.
- **`acquire` does the stale-cluster check itself** (exit 4, the lock released again), so the
  script's own check after it goes (P3).
- **`CLAUDE.md`** gains a bullet on the lock: where it is, the commands, what exit 3 and 4 mean,
  and the one new rule, that no shell command which runs the script names `/tmp/ray-cpbh`.

### 0.1 The other projects

SecondaryGWKit and StochasticInstantons share the lock and are moving to `v0.3.0` in campaigns of
their own (SGK's `rpk-v030-adoption`, planned 2026-10-25 at `2b27509`). RayPoolKit's plan had the
three switch on one day (its D3); the user moves each project as it is ready, and sequences the
Ray runs meanwhile (§6.1, U2). This campaign touches no other repository, and nothing in it waits
on another project's move. Recording RayPoolKit's gate G2 is not this campaign's.

### 0.2 Reconciliation, measured 2026-10-25 while writing this plan

Read-only; no Ray started, no store opened.
- **The tree.** The checklist measured `807f175` (the tip of `jordan-normalization`); `main`
  (`ac187c8`) is its ancestor. The script is blob `eaae1e75d4aa27233ef205a75d97333aeb57ea8b` at
  both, as the checklist says, and `requirements.txt` is the same at both (`:80` the `v0.2.0` pin).
  The checklist's acceptance grep (its item 8.2) finds the same **5** lines, all in the script:
  `:21`, `:44`, `:76`, `:77`, `:78`.
- **Other callers.** No tracked file outside the records runs or sources the script. Live lines
  naming it: `config/argument_parser.py:341` and `ComputeTargets/tests/test_ray_address.py:47`,
  which stay true. `.documents/architecture-summary.md` names neither the script nor the lock.
  `build.sh` and `run_readers.sh` beside the script name `~/ScienceData/ChamPBH-rehearsals`, a
  store root, not the lock; they are records and stay.
- **The venv:** Python 3.13.16, Ray 2.53.0; `raypoolkit` 0.2.0 at `eaef0ce6…`, `datastorekit`
  0.4.0 at `c8570e84…`, `runregistrykit` 0.2.0 at `0bae3a3c…`, none editable. `pip install
  --dry-run` of `requirements.txt` with the pin moved says `Would install raypoolkit-0.3.0` and
  nothing else.
- **The module under this venv.** The checklist ran its commands with RayPoolKit's own venv (Ray
  2.43.0). Run here from the tag's `raypoolkit/` on `PYTHONPATH`, from this repository's root, with
  `HOME` a scratch directory and ChamPBH's values: `--help` 0; `status` 0 (free); `acquire` 0; a
  second `acquire` 3; `set-pid` 0; `status` 0 (held, the pid alive, no processes of
  `/tmp/ray-cpbh`); `default_lock_dir()` the scratch `~/.raypoolkit/rehearsal-lock`; `release` 1
  (the recorded pid alive); with a directory at the old location, `acquire` 3 naming it.
- **On the machine:** no run in the registry; no Ray process; neither lock location exists;
  `~/.raypoolkit` exists, empty.

### 0.3 Baselines

01 measures the five roots at its start (`CLAUDE.md`'s commands, the `ComputeTargets` root in two
halves). `CLAUDE.md` at `ac187c8` records 18 / 176 / 78 / 84 / 61 (CosmologyModels, ComputeTargets,
ObjectFactories, tools, RunDrivers), of 2026-10-20.

## 1. Scope

**In scope (01):**
- the pin, `requirements.txt:80`, and the venv installed from it;
- the script's lock half replaced by calls of the module (the checklist's items 2–3); its header and
  usage text; its cluster half unchanged;
- a test module for the script's lock paths that needs no Ray (P4);
- the bullet in `CLAUDE.md` (P6), and the `tools/tests` count;
- the script's lifecycle on ChamPBH's real isolated cluster under the new lock (P5).

**Out of scope:**
- **any Python file outside the new test**, `main.py` above all. No Ray start changes (§0);
- **the cluster half of the script**: `temp_dir_regex`, `list_cluster_pids`, `stop_cluster`, the
  port checks and `ray start`'s arguments. Moving them into RayPoolKit is that repository's phase 2,
  a later release adopted when ChamPBH chooses;
- **records**: everything under `prompts/`, `science-runs/`, and `.documents/` but the script and
  this campaign's outputs. They keep the old location, as `CLAUDE.md` invariant 6 requires;
- **any other repository.** A defect in `raypoolkit` stops the prompt and is reported there
  (`CLAUDE.md`, "The packages");
- **`jordan-normalization`.** This branch is merged into `main`, and `main` into
  `jordan-normalization`, after the review (§4.2).

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-switch-to-the-raypoolkit-rehearsal-lock.md` | The pin and the venv; the script's lock half replaced by the module; a test of the script's lock paths with a stub `ray` and a fake `gcs_server`; `CLAUDE.md`; the script's start, status and stop on the real cluster under the new lock | Opus |

There is no close prompt. The orchestrator's review of 01, in its own commit, closes the campaign:
the board's header and §1, and the campaign's line in `prompts/INDEX.md`. Residue the review finds
is fixed in a follow-up commit before the close.

## 3. Datastores

None. 01 opens no store, and its real-Ray check starts the cluster and runs no driver.

## 4. The interfaces

### 4.1 Rollback

01 is one commit, reverted by `git revert`. **The venv must be put back too**, since the install is
not in `git`:

```bash
./venv/bin/pip install "raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0"
```

A revert while the new lock is held leaves `~/.raypoolkit/rehearsal-lock` behind, which the old
script cannot see: release it first.

### 4.2 What 01 hands on

- **To `main`.** After the review, `rpk-v030-adoption` is merged into `main`, which has not moved
  since `ac187c8`, and pushed, as the user directs.
- **To `jordan-normalization`.** `main` is then merged into `jordan-normalization` before that
  campaign's prompt 09, its rehearsal on the cluster, is dispatched; 04–08 start no Ray.
  - **Expected conflicts:** `prompts/INDEX.md` (both branches add a row and change the header's
    count). `CLAUDE.md` should merge cleanly: `jordan-normalization` changes the "Redshift" bullet
    and one line of "Ray on this machine", and this campaign adds a new bullet after that one and
    changes one count line. The planner's trial merge is in the board's §5.0.
  - **The venv** is shared by both branches in this checkout. Once 01 has installed `v0.3.0`,
    `jordan-normalization` runs on it before the merge, while its `requirements.txt` still names
    `v0.2.0`. That is harmless: `RayWorkPool` is unchanged.
  - **09's orchestration note** reads the script's new `status` (the module's layout, contract §8)
    and exit codes, and states the new rule: no shell command that runs the script names
    `/tmp/ray-cpbh`.
- **To the production machine.** Pull, and reinstall the venv from `requirements.txt`. The lock
  there moves too, if the script is used there.

## 5. The rules this campaign runs under

The project-wide ones in `CLAUDE.md`, plus, from `dk-and-rpk-migration`:
- rule 8, **deliberate breakage**: the new test catches the mutations 01 names; the log records
  each as a diff, exactly as applied; mutations are never committed;
- rule 10, **no test needs Ray**, and none touches the real `HOME`, the real lock or a real
  ChamPBH cluster (P4);
- rule 11, a package defect is reported upstream and never worked around here;
- rule 12, no change in a sibling repository;
- rule 13, **the pin is a tag**, never an editable install. The commit carries the pin and what the
  pin makes necessary, which here is the whole switch;
- rule 15, **no `ray stop`**; a cluster only through the script;
- rule 16 as it now reads: **a held lock is a stop, never a wait**, at the new location.

And:
- **Existing tests are not modified.**
- **Each lock or script command is a shell command of its own**, and no shell command that runs the
  script or a lock command names `/tmp/ray-cpbh` (contract §6, "What it asks of a caller").
- **01's real-Ray check is named here**, so that it is within `CLAUDE.md`'s "what stays": it starts
  ChamPBH's isolated cluster and stops it, runs no driver, and takes about a minute.

### 5.1 The log template

- the subject, commit and result;
- **What shipped**;
- **Deviations from the prompt**, each classified `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE`
  or `UNINTENDED DRIFT`;
- **Verification performed**, with every count against the baselines taken before;
- **The deliberate-breakage record**;
- **Observations not acted on**;
- **State handed on** (§4.2).

## 6. Decisions

### 6.1 Made by the user (2026-10-25)

| | Decided |
|---|---|
| **U1** — when and where | Move to `v0.3.0` now, as a campaign of its own on a branch from `main`, merged into `jordan-normalization` before its rehearsal (the planner's recommendation, accepted) |
| **U2** — the other projects | Each project moves in a campaign of its own; they are all switching, and no day is held in common. This campaign does nothing to keep the projects' locks consistent meanwhile |

### 6.2 Proposed by the planner (P1–P8)

Accepted as proposed unless the user rules otherwise.
- **P1 — the name** `rpk-v030-adoption`, as SGK's campaign for the same release.
- **P2 — one prompt, closed by its review**, as SGK's.
- **P3 — the checklist's three pieces of advice are taken:**
  - **`start`** drops the script's own stale-cluster check: `acquire`'s exit 4 makes it, by the
    rule `release` will apply, and releases the lock before it exits (checklist §3.1);
  - **`stop`** keeps its first check, "touch nothing if the lock is another project's", by reading
    the owner file at the module's `default_lock_dir()`, so the script never spells the location
    (§3.2). The owner file's layout and the location are both contract (§3, §4);
  - **`status`** prints the module's `status`, then the script's own listing (what `stop` would
    signal) under its own heading, then the ports (§3.3). Where the two listings differ, `stop`
    leaves something `release` then refuses on, and `status` is where a person sees it.
- **P4 — a test of the script's lock paths**, `tools/tests/test_ray_cluster_lock.py`. Nothing tests
  the script today, and from 01 its lock paths call another package. The test sources the script,
  as 05a's proof did, and overrides the cluster's values (a temp dir of its own, free ports, a stub
  `ray`) and `HOME`, all passed through the environment, never on a command line. So no test can
  start Ray, signal a process it did not start, or touch the real lock, even under a mutation. The
  stub `ray start` leaves a fake `gcs_server`, a sleeping process naming the test's temp dir, so the
  whole `start`–`stop` cycle runs through the module with no Ray. The script's real values are
  checked as text.
- **P5 — the real-Ray check** is the script's own `start`, `status` and `stop` on ChamPBH's cluster
  under the real lock, with the module's commands between them. It is the only check of the
  module's process rule against ChamPBH's real cluster (its client server above all), and it takes
  about a minute. No driver runs: the cluster half and the drivers are unchanged.
- **P6 — `CLAUDE.md`**: a new bullet, "The rehearsal lock", after "Ray on this machine", which
  stays as it is. A new bullet rather than an edit keeps the merge into `jordan-normalization`
  clean (§4.2). It names no old location: the checklist's acceptance grep covers `CLAUDE.md`.
- **P7 — outputs** go to `.documents/rpk-v030-adoption/out/01-*.txt`, small text only. There is no
  verification document; the log and the outputs are the record.
- **P8 — issues.** None is opened or assigned at planning. No open issue in
  `.documents/OPEN_ISSUES.md` concerns the lock or the cluster script.
