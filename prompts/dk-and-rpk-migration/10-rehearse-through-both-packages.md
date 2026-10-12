# Prompt 10 — rehearse through both packages (R2)

**Campaign:** [`README.md`](README.md) · **Board items:** **L1**–**L4** (R2), **V3**, **H1**;
gates **R2** and **DatastoreKit G3** · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompts 07–09, after 06's review (`fa874a5`). It is 06's protocol at
09's tree; read 06 beside it.
**Gate:**
- 09 landed and reviewed (board §1);
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- the venv holds `datastorekit` 0.3.0 at `2f5417ea…` and `raypoolkit` 0.2.0 at `eaef0ce6…`, neither
  editable;
- **`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status` shows the rehearsal lock
  free, no ChamPBH process (`/tmp/ray-cpbh`) and ChamPBH's four ports free.** If the lock is held,
  stop and ask: another project is rehearsing. Other projects' clusters may be up; they are
  isolated, and you leave them alone. Do not use `ray status` for this check.

**Closes:** nothing. **Opens:** anything you find, **without fixing it**.
**Recommended model:** **Opus**. Three builds and the readers under a real Ray cluster, the first run
of the package's actor and pool under Ray at 2.53.0, and the package's store tools.

**This prompt runs `main.py` on a store**, which `CLAUDE.md` reserves to the user. The user's ruling
**U2** (README §6.1) makes a one-off exception for prompts 02, 06 and 10, within README §4.1 and no
further.

**Read first:**

1. `CLAUDE.md`, in full, and in particular "Long-running jobs" and "Run everything from the
   repository root".
2. [`README.md`](README.md): §2.2 (10's charter), §3, **§4.1** (the protocol), **§4.2**, **§4.3**,
   §5 and §6.
3. [`06-rehearse-before-the-datastore-swap.md`](06-rehearse-before-the-datastore-swap.md): R1's
   prompt. **10 is 06 at a later tree; its §1 is 06's §1 but for the names, and its §2 is 06's but
   for what this prompt says.**
4. Prompt 06's log and `.documents/dk-and-rpk-migration-verification.md` §2, **in full**: R1's
   numbers are what you compare against, and its observations and the board's §5.10 review say what
   to expect: exit 134, the actor check, how `S` was found and why 6 s was too late, and Ray's log
   deduplication of the prune reports.
5. Prompt 07's log (the provenance line it recorded), 08's (the schema differences, the U4 refusal,
   the store tools) and 09's (the new inventory report).
6. DatastoreKit **at the tag** (read, never edit): `datastorekit/tools/shard_key_audit.py` and
   `datastorekit/tools/sharded_store.py` (their usage), and `ShardedPool.copy_store` and
   `delete_store` (`datastorekit/SQL/ShardedPool.py:2649-2753`).

---

## 0. What this prompt is

R2 is the rehearsal at 09's tree: **both packages, ChamPBH's layer gone.** It answers one question:
**did 07–09 change anything a build stores, reads or prints?** It is also ChamPBH's evidence, at Ray
2.53.0, that its registry of instances crosses into the package's actors intact (README §0.3,
point 2), and the last part of DatastoreKit's G3: a pilot store rebuilt through the package.

R0 and R1 were bitwise equal (verification §1, §2). So the criterion is **bitwise equality**: every
R2 store equals `06-reference.db` (and so `02-reference.db`) by `tools/compare_stores.py`, `RESULT:
equal`, exit 0. The tool reads the package's stores (bare shard records, `replication_in_flight`) as
it reads ChamPBH's (01's tests). Bookkeeping columns differ, and are expected to. **One differing bit
in a science value is a miss.** No tolerance is chosen by an agent.

**What is expected to differ from R1, and is not a finding:**
- the provenance line names `datastorekit` 0.3.0 at `2f5417ea…`, as 07's log records it;
- the store's primary is the package's: shards recorded by bare name, a `replication_in_flight`
  table, replicated rows stamped with one time on every shard;
- the schema snapshot differs from R0's exactly as 08 listed (`out/08-standin-schema.json` is the
  package's schema at this registry);
- `--inventory` (`run_readers.sh` step 1) is 09's read-only report, run before Ray, and prints
  09's layout, not R1's.

**What is expected to be the same:** the `Created`/`Opened existing sharded datastore` lines, the
actors' `!! INTEGRITY WARNING` header and the factories' prune lines (the package's actor calls the
factory's `validate_on_startup` and prints what it returns, as ChamPBH's did), the actor names (16
`shardNNNN-store` and one `SerialPoolBroker` per job), and every line the pool prints (RayPoolKit is
unchanged since R1).

---

## 1. The protocol, with this prompt's names

### 1.1 Names

- NN = `10`. Rehearsal root: `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`
  (`$REHEARSAL_ROOT`), holding R0's and R1's stores. **Never write to, move or delete an `02-*` or
  `06-*` file**; they are opened only by `tools/compare_stores.py` (`mode=ro`) and `sqlite3`
  `mode=ro`.
- Stores: `$REHEARSAL_ROOT/10-reference.db` and `$REHEARSAL_ROOT/10-resumed.db`, 16 shards each, and
  for §1.4.6 only, `$REHEARSAL_ROOT/10-reference-copy.db`. None may exist before you start, nor
  `$REHEARSAL_ROOT/10-readers/`; if one does, stop and ask.
- Logs: `$REHEARSAL_ROOT/logs/10-<build>.log`. Readers: `$REHEARSAL_ROOT/10-readers/`.
- Outputs kept in the repository: `.documents/dk-and-rpk-migration/out/10-*` (small text only). No
  store, log or figure is committed.

### 1.2 The cluster

As 06's §1.2, with purpose `"dk-and-rpk-migration 10 (R2)"`: record the load; `ray_cluster.sh start`
(exit 3: the lock is held, stop and ask); every driver gets `--ray-address 127.0.0.1:6380`, every
`ray` CLI call `--address 127.0.0.1:6380`; at the end and after any stop, `ray_cluster.sh stop` as a
command on its own, then `status` in another. **Never `ray stop`.** No shell or background task
naming `/tmp/ray-cpbh` left running across a stop, no parallel Bash calls while it runs, and no
`stop` while a build or reader runs.

### 1.3 The builds

Each with 01's `build.sh`, **unchanged**, as a background task; wait for each by notification.
1. **`reference`:** `build.sh 10 reference reference <address>`.
2. **`resumed-a`:** `build.sh 10 resumed resumed-a <address>`, with
   `interrupt_when.py $REHEARSAL_ROOT/logs/10-resumed-a.log $REHEARSAL_ROOT/logs/10-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after <S>`
   beside it. **Choose `S` from `10-reference.db`'s own `AdiabaticHistory` timestamps**, as 06 did,
   **allowing for `resumed-a` running ahead of its `reference`**: at R0 `resumed-a`'s rows came 2.0–4.0
   s after the banner was seen against `reference`'s 4.6–7.6 s, at R1 2.4–5.2 s against 5.1–9.8 s. R1's
   `S` = 6 s fell after all five, so `resumed-a` stored every row (validating two). Aim the signal at
   the earliest part of `reference`'s spread less about 3 s (at R1's figures, `S` ≈ 3 s), so that
   some `AdiabaticHistory` rows are stored and some are not. Record `S` and why.
3. **Between the two:** a `SIGTERM`ed driver exits **134** (Ray's absl handler). Confirm the killed
   job's actors are gone (`./venv/bin/ray list actors --address 127.0.0.1:6380 --filter
   "state=ALIVE"`, then `--limit 200` and `ray list jobs --address …`); if one is alive after a
   minute, stop and ask. Record what `resumed-a` kept (`mode=ro`: which `AdiabaticHistory` rows,
   validated or not).
4. **`resumed-b`:** `build.sh 10 resumed resumed-b <address> --prune-unvalidated`. **Before the
   cluster stops**, read every prune report in full: Ray deduplicates repeated lines across actors in
   the driver's log, so read the actors' own logs as well
   (`/tmp/ray-cpbh/session_*/logs/old/worker-*-<job id>-*.out` for `resumed-b`'s job), in one command
   that ends before anything else runs.

### 1.4 After the builds

1. **The readers:** `run_readers.sh 10 resumed <address>`. Then, separately, `main.py --config
   exponential.yaml --database $REHEARSAL_ROOT/10-reference.db --inventory`, its output to
   `$REHEARSAL_ROOT/10-readers/1b-inventory-reference.log` (09's `--inventory` needs no cluster).
2. `ray_cluster.sh stop` (§1.2). Nothing below needs the cluster.
3. **The comparisons**, each with `--json .documents/dk-and-rpk-migration/out/10-compare-<a>-<b>.json`
   and its text output beside it:
   - **`06-reference.db` against `10-reference.db`**: the criterion;
   - **`06-reference.db` against `10-resumed.db`**: the criterion, for the resumed store;
   - `02-reference.db` against `10-reference.db`: R0 directly;
   - `10-reference.db` against `10-resumed.db`: §4.3 again, at this tree.
4. **The schema snapshot** of `10-reference.db`, saved as `out/10-reference-schema.json`, and
   compared with `out/08-standin-schema.json` (`--schema … --against …`: they must agree) and with
   `out/02-reference-schema.json` (the differences must be 08's list, no more).
5. **The stage counts:** `stage_counts.py` on the three logs, text and `--json`. **The printed
   lines** (L4): the pool's lines of `06-reference.log` and `10-reference.log`, and of the two
   `resumed-b` logs, normalised and compared as 06 did, saved as `out/10-printed-lines.txt`.
6. **H1 — the store by the package's tools**, on closed stores, no cluster:
   1. `./venv/bin/python -m datastorekit.tools.shard_key_audit $REHEARSAL_ROOT/10-reference.db`, and
      the same on `10-resumed.db`: each `VERDICT: OK`, exit 0.
   2. `./venv/bin/python -m datastorekit.tools.sharded_store copy $REHEARSAL_ROOT/10-reference.db
      $REHEARSAL_ROOT/10-reference-copy.db`: exit 0, 16 shards.
   3. The copy **opens read-only**: `main.py --database …/10-reference-copy.db --inventory` exits 0 and
      reports no problem (the package's `read_inventory` opens it through `open_read_only`). Its
      shard records name its own shards (a bare name each; `sqlite3 mode=ro` on its `shards` table).
   4. `tools/compare_stores.py 10-reference.db 10-reference-copy.db`: `RESULT: equal`, exit 0.
   5. **The copy is deleted with the package's own `ShardedPool.delete_store`**, from a throwaway
      script in your scratch, on the copy's primary alone; the files it returns are exactly the copy's
      17, and none remains. This is the one deletion under `$REHEARSAL_ROOT` the campaign allows
      (README §3).

---

## 2. What to verify

- **L1 — every build ends as it should.** As 06's: `reference` and `resumed-b` end with `** driver exit
  status 0` and every stage's `ALL WORK ITEMS COMPLETE`; `resumed-a` ends by the signal (134), in the
  adiabatic stage. No `Traceback` in any log. **Each build log and each Ray reader's log carries the
  provenance line 07's log records**, naming `datastorekit 0.3.0 (…@2f5417ea…, requested v0.3.0)`
  and `raypoolkit 0.2.0 (…@eaef0ce6…, requested v0.2.0)`. Every driver connects to `127.0.0.1:6380`.
- **L2 — the resume.** What `resumed-b`'s open pruned (by the actors' reports, read in full), and that
  it was only unvalidated rows; that `resumed-b` computed exactly what `resumed-a` had not kept, by
  label counts per table; the §4.3 comparison of §1.4.3. **The prune at open is the package's code
  now**; its report's text is the factories'.
- **L3 — every reader** exits 0, and the store is unchanged by them (`run_readers.sh`'s last step
  `RESULT: equal`, no difference but bookkeeping). In addition:
  - `--inventory`, on `10-resumed.db` (step 1) and on `10-reference.db` (§1.4.1), **reports no named
    problem**, and lists 5 validated rows of each of `ScalarModel`, `AdiabaticHistory`, `BBNData`,
    version `2026.6.0`, with the physical labels and `PRyM_version` of 09's report;
  - **`histories.csv` is byte-identical to R0's and R1's**: SHA-256
    `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, and `cmp` against
    `$REHEARSAL_ROOT/06-readers/plot_by_beta/QCD_Cosmology/histories.csv`;
  - `bbn_from_store.py` for β = 1.15 prints R0's stored and re-solved Yp and D/H (stored
    `Yp=0.24687349447579363 DoH=2.4859924812264325`; re-solve `0.2468734945`, `2.485992481`).
- **L4 — against R1.** `reference`'s counts 5 / 5 / 5; the resume's *pattern* (`resumed-b` computes
  exactly what was not kept; prunes only unvalidated rows; scalar-stage lookups 5 with compute 0,
  board §3 `[02-scalar-stage-missing-filter-keeps-every-model]`, unchanged unless the board says it
  was fixed); the printed lines in R1's layout, the progress shapes included (or none, said plainly).
  Give R1's figures beside yours.
- **H1** (§1.4.6) holds.
- **The criterion (README §4.3, P4).** The four comparisons of §1.4.3 each give `RESULT: equal`,
  exit 0. The schema agrees with 08's snapshot.

---

## 3. Acceptance

1. L1–L3 hold, each shown.
2. Every provenance line names both tags' commits, neither editable.
3. **Against R1 and R0:** `10-reference.db` and `10-resumed.db` each equal `06-reference.db`, and
   `10-reference.db` equals `02-reference.db`, bitwise (`RESULT: equal`, exit 0); the schema agrees
   with `out/08-standin-schema.json`, and differs from R0's only as 08 listed.
4. **L4:** the counts and the resume's pattern are R1's; the printed lines keep their layout.
5. **H1:** the audit is clean on both stores; the copy opens read-only, compares equal, and is gone.
6. **No deliberate breakage** (README §5 rule 8): 10 changes no code. Its checks are each one a bad
   build would fail; the log says which.
7. **The records**, in the same commit:
   - `.documents/dk-and-rpk-migration-verification.md` **§3 R2**, after §2 and in its shape, plus H1;
     §1 and §2 are not edited (`CLAUDE.md` invariant 6);
   - `.documents/dk-and-rpk-migration/out/10-*`;
   - this board: the §1 row for 10, items L1–L4 (R2), V3 and H1, **gate R2** and **DatastoreKit G3**
     in §2 (by the checklist's item 9: the layer deleted and the pin imported (08), the suites pass,
     and a pilot store rebuilt through the package and compared, here), and
     `### 5.15 After prompt 10 (<date>)`;
   - **the G3 record's text** in the log's "State handed to the next prompt", for 11 to carry to
     DatastoreKit's board (P16), as 04's log carries RayPoolKit's;
   - a §3 entry for anything opened, with `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- The rehearsal lock is held, or a ChamPBH process is already running, or a store or directory of
  §1.1 already exists.
- `ray_cluster.sh stop` exits non-zero, or leaves a ChamPBH process or port behind.
- A build fails, raises, or does not end as §2 says; a driver connects anywhere but `127.0.0.1:6380`.
- An actor of the killed build is still alive after a minute.
- A reader fails, or changes the store beyond bookkeeping; `--inventory` reports a problem.
- **A comparison against `06-reference.db` or `02-reference.db` is not `RESULT: equal`.** Record
  `compare_stores`'s full output, text and JSON, with its near-miss pairs, and which stage's tables
  differ. Do not look for the cause by changing anything, and do not delete or rebuild a store. The
  change is in 07–09, or in the package; a defect in the package is reported to DatastoreKit by the
  user, never worked around here (README §5 rule 11). Nothing after 10 is dispatched until the user
  has ruled. Finish §1.4 first (the stop of the cluster included), write the log and the
  verification document, and **do not commit**.
- The schema snapshot differs from 08's.
- A store tool reports a problem, or `delete_store` names a file that is not the copy's.
- Anything would need a change to code, to a script of 01's or 05a's, or to the tool.

---

## 5. What this prompt does not do

- **Files it changes:** `.documents/dk-and-rpk-migration-verification.md` (§3 added),
  `.documents/dk-and-rpk-migration/out/10-*`, the log and this board (and the index if an issue
  opens). No code.
- It opens no store but its own two, the copy and the readers' copy, and R0's and R1's read-only
  through `compare_stores`. Not the pilot store in the repository root; not `~/ChamPBH-stores/` or
  `~/ChamPBH-stores-bt02/`.
- It deletes nothing but the copy of §1.4.6, by the package's `delete_store`.
- It installs nothing, leaves no ChamPBH cluster running and the lock free, and never runs
  `ray stop`. Other projects' clusters are not touched.
- It records DatastoreKit's G3 on this board only; the record on DatastoreKit's board is 11's draft
  and the user's commit (P16).

---

## 6. The log

`logs/10-rehearse-through-both-packages.md`, using README §5.1. It holds the cluster's address and
the load, each command verbatim, each build's start, end and exit status, `S` and why, the actor
check, what `resumed-a` kept, every prune report in full, L1–L4 with R1's figures beside each, the
four comparisons in full, the schema comparisons, the printed-lines comparison, H1, the G3 record's
text, and the three suites at dispatch, run before the cluster starts.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`, and made with `git commit -- <paths>`.
