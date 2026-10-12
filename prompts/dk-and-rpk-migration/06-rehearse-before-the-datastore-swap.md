# Prompt 06 — rehearse before the DatastoreKit swap (R1)

**Campaign:** [`README.md`](README.md) · **Board items:** **L1**–**L4** (R1), **V2**; gate **R1** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompt 05, after the reviews of 02 (`70184f1`) and 04 (`cb0c675`).
**Gate:**
- 05 landed and reviewed (board §1);
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- the venv holds `raypoolkit` 0.2.0 at `eaef0ce6…`, not editable, and no `datastorekit`;
- **no Ray cluster is up** (`./venv/bin/ray status` finds none). If one is, stop and ask: it may be
  the user's.

**Closes:** nothing. **Opens:** anything you find, **without fixing it**.
**Recommended model:** **Opus**. It runs three builds and the readers under a real Ray cluster on
two real stores, the first run of `raypoolkit` under Ray, and the work is in reading what they did.

**This prompt runs `main.py` on a store**, which `CLAUDE.md` reserves to the user. The user's
ruling **U2** (README §6.1) makes a one-off exception for prompts 02, 06 and 10, within README §4.1
and no further.

**Read first:**

1. `CLAUDE.md`, in full, and in particular "Long-running jobs" and "Run everything from the
   repository root".
2. [`README.md`](README.md): §2.2 (06's charter), §3, **§4.1** (the protocol), **§4.2**, **§4.3**,
   §5 and §6.
3. [`02-rehearse-the-start-tree.md`](02-rehearse-the-start-tree.md): R0's prompt. 06 is R0's protocol
   at a later tree, and its §1 is this prompt's §1 but for the names.
4. Prompt 02's log, and `.documents/dk-and-rpk-migration-verification.md` §1, **in full**: R0's
   numbers are what you compare against, and its "Observations not acted on" and §1.8 say what to
   expect and how `S` was found.
5. Prompt 04's and 05's logs, "State handed to the next prompt": what changed since R0.
6. RayPoolKit's README at the tag, "The printed lines are part of the interface"
   (`git -C /Users/ds283/Documents/Code/RayPoolKit show v0.2.0:README.md`). Read only.

---

## 0. What this prompt is

R1 is the rehearsal at 05's tree: **`raypoolkit` and the data move, still on ChamPBH's layer.** It
is the first run of the packaged pool under real Ray (at Ray 2.53.0, which neither package tests),
and the last rehearsal before 08 replaces the layer. It answers one question: **did 03–05 change
anything a build stores, reads or prints?**

R0 was bitwise reproducible (log 02; README P4). So the criterion is **bitwise equality**: every R1
store equals `02-reference.db` by `tools/compare_stores.py`, `RESULT: equal`, exit 0. Bookkeeping
columns (labels, timestamps, timings) differ, and are expected to. **One differing bit in a science
value is a miss.** No tolerance is chosen by an agent.

---

## 1. The protocol, with this prompt's names

### 1.1 Names

- NN = `06`. Rehearsal root: `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`
  (`$REHEARSAL_ROOT`), which exists and holds R0's stores. **Never write to, move or delete an `02-*`
  file**; R0's stores are opened only by `tools/compare_stores.py`, which reads `mode=ro`.
- Stores: `$REHEARSAL_ROOT/06-reference.db` and `$REHEARSAL_ROOT/06-resumed.db`, each with 16
  shards. Neither may exist before you start, nor `$REHEARSAL_ROOT/06-readers/`; if one does, stop and
  ask.
- Logs: `$REHEARSAL_ROOT/logs/06-<build>.log`. Readers: `$REHEARSAL_ROOT/06-readers/`.
- Outputs kept in the repository: `.documents/dk-and-rpk-migration/out/06-*` (small text only, as
  02's). No store, log or figure is committed.

### 1.2 The cluster

1. Record `uptime`, the number of CPUs, and any process using much CPU that is not yours (`ps -Ao
   pid,pcpu,comm -r | head`). The orchestrator asks other sessions to hold their suites and Ray jobs
   off the machine while you build; if one runs anyway, record it and continue (R0's result held
   under a foreign suite, log 02 Observation 4).
2. `./venv/bin/ray start --head --disable-usage-stats`, from the repository root. **At Ray 2.53.0 it
   prints no address line.** The address is `127.0.0.1:6379` by default; read it from
   `/tmp/ray/ray_current_cluster`, confirm with `./venv/bin/ray status`, and pass it to every driver
   as `--ray-address`.
3. At the end, and after any stop, `./venv/bin/ray stop`, **as a command on its own**: it kills a
   shell whose command line names Ray's processes (log 02 Observation 6). Check afterwards, in a
   separate command, that `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. You stop only the
   cluster you started.

### 1.3 The builds

Each with prompt 01's `build.sh`, **unchanged** (README §4), run as a background task, so that you are
notified when it ends. Wait for each to end before the next. **Do not poll.**

1. **`reference`:** `build.sh 06 reference reference <address>`.
2. **`resumed-a`:** `build.sh 06 resumed resumed-a <address>`, with
   `interrupt_when.py $REHEARSAL_ROOT/logs/06-resumed-a.log $REHEARSAL_ROOT/logs/06-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after <S>`
   started beside it. **Choose `S` from `06-reference.db`'s own timestamps**, by the method of the
   verification document §1.8: read each `AdiabaticHistory` row's `timestamp` (`mode=ro`), and the
   adiabatic banner's time from the log, and pick `S` inside the spread so that some rows are stored
   and some are not. At R0 the stage took 12.6 s and `S` was 5 s; do not reuse it without checking.
   Record `S` and why.
3. **Between `resumed-a` and `resumed-b`:** a driver killed by `SIGTERM` under Ray 2.53.0 **exits
   134**, not 143: Ray's absl failure-signal handler prints `*** SIGTERM received …` and a native
   stack, twice, then aborts. That dump is not a Python traceback and contains neither `Traceback`
   nor `Error` (log 02 Observation 1). Confirm that the killed driver's actors are gone
   (`./venv/bin/ray list actors --filter "state=ALIVE"`), and record what you saw. If an actor of the
   killed job is still alive after a minute, stop and ask. Record what `resumed-a` kept (`mode=ro`:
   which `AdiabaticHistory` rows, validated or not).
4. **`resumed-b`:** `build.sh 06 resumed resumed-b <address> --prune-unvalidated`.

### 1.4 After the builds

1. **The readers:** `run_readers.sh 06 resumed <address>`.
2. `ray stop` (§1.2.3). Nothing below needs the cluster.
3. **The comparisons**, each with `--json .documents/dk-and-rpk-migration/out/06-compare-<a>-<b>.json`
   and its text output saved beside it:
   - **`02-reference.db` against `06-reference.db`**: the criterion;
   - **`02-reference.db` against `06-resumed.db`**: the criterion, for the resumed store;
   - `06-reference.db` against `06-resumed.db`: §4.3 again, at this tree.
4. **The schema snapshot** of `06-reference.db`, compared with
   `.documents/dk-and-rpk-migration/out/02-reference-schema.json` (`--schema … --against …`), and
   saved as `out/06-reference-schema.json`.
5. **The stage counts:** `stage_counts.py` on all three logs, saved, text and `--json`.
6. **The printed lines** (§2, L4): the pool's lines from `02-reference.log` and `06-reference.log`,
   normalised and compared, saved as `out/06-printed-lines.txt`.

---

## 2. What to verify

- **L1 — every build ends as it should.** `reference` and `resumed-b` end with
  `** driver exit status 0` and every stage's `ALL WORK ITEMS COMPLETE`; `resumed-a` ends by the
  signal (134), in the adiabatic stage. No `Traceback` in any log. **Each build log, and each Ray
  reader's log, carries the provenance line**
  `>> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
  It prints no `editable` flag; the helper's `editable: False` is checked once, by
  `utilities.installed_package_provenance()["raypoolkit"]`, before `ray start`.
- **L2 — the resume.** From the logs, the label counts (`mode=ro`) and `stage_counts.py`:
  - what `resumed-b`'s open pruned (by `_validate_on_startup`'s report), and that it was only
    unvalidated rows;
  - that `resumed-b` computed exactly what `resumed-a` had not kept, by label counts per table;
  - the §4.3 comparison of §1.4.3.
- **L3 — every reader** exits 0, and the store is unchanged by them: `run_readers.sh`'s last step is
  `RESULT: equal`, with no difference but bookkeeping. In addition:
  - `--inventory` lists 5 validated rows of each of `ScalarModel`, `AdiabaticHistory`, `BBNData`,
    version `2026.6.0`, as R0's;
  - **`histories.csv` is byte-identical to R0's**: SHA-256
    `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, and `cmp` against
    `$REHEARSAL_ROOT/02-readers/plot_by_beta/QCD_Cosmology/histories.csv`;
  - `bbn_from_store.py` for β = 1.15 prints the same stored and re-solved Yp and D/H as R0's (stored
    `Yp=0.24687349447579363 DoH=2.4859924812264325`; re-solve `0.2468734945`, `2.485992481`).
- **L4 — against R0.**
  - **Counts.** `reference`: `compute` 5 / 5 / 5, as R0's. The resume's *pattern*, not its numbers,
    since what `resumed-a` keeps depends on the signal's timing: `resumed-b` computes exactly what
    was not kept and prunes only unvalidated rows. Its scalar-stage lookups are 5 with compute 0,
    because the scalar stage looks up every stored model again (board §3,
    `[02-scalar-stage-missing-filter-keeps-every-model]`, unchanged); its adiabatic lookups equal
    its adiabatic computes. Give R0's figures beside yours.
  - **The printed lines keep their layout** (RayPoolKit's "The printed lines are part of the
    interface"). The pool prints, for a titled pool, the stage banner, progress lines
    (`   -- <time> (<elapsed> running): …` and the `inflight: … | completed: …` line beneath),
    `   -- ALL WORK ITEMS COMPLETE in time …` and `      Queue summary: …`. Extract those lines from
    `02-reference.log` and `06-reference.log`, replace every number, time and rate unit with a
    placeholder, and compare: the banners and completion lines must agree in text and order, and
    every progress-line shape in 06's log must occur in 02's (how many progress lines appear depends
    on timing). `stage_counts.py` parsing 06's logs is part of the evidence. A difference is recorded
    and explained; it is a finding, not by itself a miss.
- **The criterion (README §4.3, P4).** Both `02-reference` comparisons give `RESULT: equal`, exit 0,
  and the `06-reference`/`06-resumed` one too. The schema snapshot agrees with R0's.

---

## 3. Acceptance

1. L1–L3 hold, each shown.
2. Every provenance line names `raypoolkit` 0.2.0 at `eaef0ce6…` and `datastorekit` not installed;
   the helper gives `editable: False`.
3. **Against R0:** `06-reference.db` and `06-resumed.db` each equal `02-reference.db`, bitwise
   (`RESULT: equal`, exit 0); the schema agrees with R0's snapshot.
4. **L4:** the counts and the resume's pattern are R0's; the printed lines keep their layout.
5. **No deliberate breakage** (README §5 rule 8): 06 changes no code. Its checks are each one a bad
   build would fail; the log says which.
6. **The records**, in the same commit:
   - `.documents/dk-and-rpk-migration-verification.md` **§2 R1**, added after §1 and in its shape
     (cluster and load; the builds; L1; L2; L3; the criterion; L4; the commands verbatim). §1 is not
     edited (`CLAUDE.md` invariant 6);
   - `.documents/dk-and-rpk-migration/out/06-*`;
   - this board: the §1 row for 06, items L1–L4 (R1) and V2, **gate R1** in §2, and
     `### 5.8 After prompt 06 (<date>)`;
   - a §3 entry for anything opened, with `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- A Ray cluster is already up, or a store or directory of §1.1 already exists.
- A build fails, raises, or does not end as §2 says.
- An actor of the killed build is still alive after a minute.
- A reader fails, or changes the store beyond bookkeeping.
- **A comparison against `02-reference.db` is not `RESULT: equal`.** Record `compare_stores`'s full
  output, text and JSON, with its near-miss pairs, and which stage's tables differ. Do not look for
  the cause by changing anything, and do not delete or rebuild a store. The change is in 03–05, all
  on ChamPBH's layer (README §2.2); the user may have a reference build run at 04's tree to tell
  `raypoolkit` from the data move. Nothing after 06 is dispatched until the user has ruled.
- The schema snapshot differs from R0's.
- Anything would need a change to code, to a script of 01's, or to the tool. A script that does not
  do what its usage says is a finding: record it, and stop rather than patch it, since 10 must run
  the same scripts.

---

## 5. What this prompt does not do

- **Files it changes:** `.documents/dk-and-rpk-migration-verification.md` (§2 added),
  `.documents/dk-and-rpk-migration/out/06-*`, the log and this board. No code.
- It opens no store but its own two (and the readers' copy), and R0's two read-only through
  `compare_stores`. Not the pilot store in the repository root; not `~/ChamPBH-stores/` or
  `~/ChamPBH-stores-bt02/`.
- It installs nothing, and leaves no Ray cluster running.

---

## 6. The log

`logs/06-rehearse-before-the-datastore-swap.md`, using README §5.1. It holds the cluster's address and
the load, each command verbatim, each build's start, end and exit status, `S` and why, the actor
check, what `resumed-a` kept, L1–L4 with R0's figures beside each, the three comparisons in full,
the printed-lines comparison, and the three suites at dispatch, run before `ray start` (no change
expected: 06 changes no code, but record them, as `CLAUDE.md` asks).

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
