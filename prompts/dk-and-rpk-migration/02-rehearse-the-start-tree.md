# Prompt 02 — rehearse the start tree (R0)

**Campaign:** [`README.md`](README.md) · **Board items:** **L1**–**L4** (R0), **V1** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-11, with prompts 01, 03 and 04, after the user reviewed the plan (`97f50f8`).
**Gate:**
- 01 landed and reviewed (board §1), and its scripts and tool are as its log describes;
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- the venv holds neither `datastorekit` nor `raypoolkit`;
- **no Ray cluster is up** (`./venv/bin/ray status` finds none). If one is, stop and ask: it may be
  the user's.

**Closes:** nothing. **Opens:** anything you find, **without fixing it**.
**Recommended model:** **Opus**. It runs three builds and the readers under a real Ray cluster on
two real stores, and the work is in reading what they did.

**This prompt runs `main.py` on a store**, which `CLAUDE.md` reserves to the user. The user's
ruling **U2** (README §6.1) makes a one-off exception for prompts 02, 06 and 10, within README §4.1
and no further.

**Read first:**

1. `CLAUDE.md`, in full, and in particular "Long-running jobs" and "Run everything from the
   repository root".
2. [`README.md`](README.md): §0.2, §2.2 (02's charter), §3, **§4.1** (the protocol), **§4.2**,
   **§4.3**, §5 and §6.
3. Prompt 01's log: the provenance line's format, the bookkeeping list, and each script's usage.
4. `pilot-main.log` (untracked; read it as text): how the same grid ran on 2026-10-02, about 9
   minutes in all, and what its banners and summaries look like.
5. `Datastore/SQL/Datastore.py:441-470` (`_validate_on_startup`: what `--prune-unvalidated` prints
   and prunes) and `main.py`'s stage loop, so that you can read the resume's output.

---

## 0. What this prompt is

R0 is the campaign's baseline, on the code as it stands: ChamPBH's own layer and `RayTools/`. It
answers two questions before anything changes:
- **does the start tree build, resume and read the pilot cleanly?** (L1–L3);
- **do two builds of one grid agree bit for bit?** (README §4.3). That answer sets the criterion
  for R1 (06) and R2 (10). It is measured here, not assumed.

---

## 1. The protocol, with this prompt's names

### 1.1 Names

- NN = `02`. Rehearsal root: `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`
  (`$REHEARSAL_ROOT`, P2). `~/ScienceData` does not exist on 2026-10-11; create the root with
  `mkdir -p`.
- Stores: `$REHEARSAL_ROOT/02-reference.db` and `$REHEARSAL_ROOT/02-resumed.db`, each with 16 shards.
  Neither may exist before you start; if one does, stop and ask.
- Logs: `$REHEARSAL_ROOT/logs/02-<build>.log`. Readers: `$REHEARSAL_ROOT/02-readers/`.
- Outputs kept in the repository: `.documents/dk-and-rpk-migration/out/02-*.txt` (small text only:
  comparison results, stage counts, the schema snapshot, abridged reader output). No store, log or
  figure is committed.

### 1.2 The cluster

1. Record `uptime` and the number of CPUs. The builds are best run on an otherwise idle machine
   (README §4.3: a BBN solve that meets its wall-clock limit under load stores a failure). If the
   load average is above the CPU count, say so in the log and continue.
2. `./venv/bin/ray start --head --disable-usage-stats`, from the repository root. Record the address
   it prints, and pass it to every driver as `--ray-address`.
3. At the end, and after any stop, `./venv/bin/ray stop`. You stop only the cluster you started.

### 1.3 The builds

Each with prompt 01's `build.sh`, run as a background task, so that you are notified when it ends.
Wait for each to end before the next. **Do not poll.**

1. **`reference`:** `build.sh 02 reference reference <address>`.
2. **`resumed-a`:** `build.sh 02 resumed resumed-a <address>`, with
   `interrupt_when.py $REHEARSAL_ROOT/logs/02-resumed-a.log $REHEARSAL_ROOT/logs/02-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after <S>`
   started beside it. Choose `S` from `reference`'s log so that some `AdiabaticHistory` rows are
   stored and some are not: on the pilot the stage took 2 min 19 s for five histories, so about
   half that. Record `S` and why.
3. **Between `resumed-a` and `resumed-b`:** confirm that the killed driver's actors are gone
   (`./venv/bin/ray list actors --filter "state=ALIVE"`, or the equivalent your Ray offers), and
   record what you saw. If an actor of the killed job is still alive after a minute, stop and ask.
4. **`resumed-b`:** `build.sh 02 resumed resumed-b <address> --prune-unvalidated`.

### 1.4 After the builds

1. **The readers:** `run_readers.sh 02 resumed <address>`.
2. **The comparison:** `tools/compare_stores.py $REHEARSAL_ROOT/02-reference.db
   $REHEARSAL_ROOT/02-resumed.db --json .documents/dk-and-rpk-migration/out/02-compare-reference-resumed.json`,
   with its text output saved beside it.
3. **The schema snapshot** of `02-reference.db`, saved as
   `.documents/dk-and-rpk-migration/out/02-reference-schema.json`. 05 and later prompts compare with
   it.
4. **The stage counts:** `stage_counts.py` on all three logs, saved.
5. `ray stop`.

---

## 2. What to verify

- **L1 — every build ends as it should.** `reference` and `resumed-b` end with
  `** driver exit status 0` and every stage's `ALL WORK ITEMS COMPLETE`; `resumed-a` ends by the
  signal, at the stage you meant. No traceback in any log. Each log carries the provenance line,
  both packages not installed.
- **L2 — the resume.** From the logs and `stage_counts.py`:
  - what `resumed-b`'s open pruned (by `_validate_on_startup`'s report), and that it was only
    unvalidated rows;
  - that `resumed-b` computed only what `resumed-a` had not stored: its compute counts per stage
    against `reference`'s;
  - and the comparison of §1.4.2, which is also §4.3's measurement.
- **L3 — every reader** exits 0, and the store is unchanged by them: `run_readers.sh`'s last step,
  `compare_stores` of `before/` against the store, is empty but for bookkeeping columns. Record what
  `--inventory` printed, the five rows of `histories.csv`, and whether `bbn_from_store.py`'s Yp and
  D/H for β = 1.15 equal the stored row's (its header prints the stored values). If they do not,
  that is a finding about the tool or the network flag, not about the store: record it and continue.
- **L4 — the baseline:** each stage's counts in all three builds, and what the resume pruned and
  recomputed, recorded as the campaign's baseline.

**§4.3, the reproducibility.** `reference` and `resumed` compute every history independently.
- If `compare_stores` reports **nothing** but bookkeeping: R0 is **bitwise reproducible**, and R1 and
  R2 are held to bitwise equality (P4). Say so plainly in the log and the verification document.
- If it reports differences: record every one, with `compare_stores`'s near-miss pairs, and look for
  the cause without changing anything (a BBN failure in one store and a result in the other, under
  the wall-clock limit, is the first candidate; the logs say). **Then stop.** The user rules on R1's
  criterion before 05–06 are written (README §2.1, §4.3). No tolerance is chosen by an agent.

---

## 3. Acceptance

1. L1–L3 hold, each shown.
2. §4.3 is measured and recorded, either way.
3. L4's baseline is recorded.
4. The schema snapshot is saved.
5. **No deliberate breakage** (README §5 rule 8): 02 changes no code. Its checks are each a check a
   bad build would fail.
6. **The records**, in the same commit:
   - `.documents/dk-and-rpk-migration-verification.md`, created, with an introduction (what the
     document is, pointing at README §4) and **§1 R0**: the cluster and load, the three builds with
     their times and exit statuses, L1–L4, §4.3's result, and the commands verbatim;
   - `.documents/dk-and-rpk-migration/out/02-*`;
   - this board: the §1 row for 02, items L1–L4 (R0) and V1, gate R0 in §2, and
     `### 5.3 After prompt 02 (<date>)`;
   - a §3 entry for anything opened, with `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- A Ray cluster is already up, or a store of §1.1 already exists.
- A build fails, raises, or does not end as §2 says. The start tree is ChamPBH's: the failure is
  not this campaign's to fix, and nothing after 02 is dispatched until the user has ruled.
- An actor of the killed build is still alive after a minute.
- A reader fails, or changes the store beyond bookkeeping.
- §4.3 finds a difference (the user rules on the criterion).
- Anything would need a change to code, to a script of 01's, or to the tool. A script that does not
  do what its usage says is a finding: record it, and stop rather than patch it, since 06 and 10 must
  run the same scripts.

---

## 5. What this prompt does not do

- **Files it changes:** `.documents/dk-and-rpk-migration-verification.md`,
  `.documents/dk-and-rpk-migration/out/02-*`, the log and this board. No code.
- It opens no store but its own two (and the readers' copy). Not the pilot store; not the science
  store or its refresh copy.
- It installs nothing, and leaves no Ray cluster running.

---

## 6. The log

`logs/02-rehearse-the-start-tree.md`, using README §5.1. It holds the cluster's address and the
load, each command verbatim, each build's start, end and exit status, `S` and why, the actor check,
L1–L4, §4.3's result in full, and the three suites at dispatch (no change expected: 02 changes no
code, but record them, as `CLAUDE.md` asks).

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
