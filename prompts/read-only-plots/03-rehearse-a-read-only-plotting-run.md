# Prompt 03 — rehearse a read-only plotting run

**Campaign:** [`README.md`](README.md) · **Board items:** **H1**–**H4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-13, after the user's rulings (README §6.1 U5: rehearse). It follows
`dk-and-rpk-migration`'s rehearsal protocol (that README §4.1), cut down: one build, no resume, the
readers.
**Gate:**
- 01 and 02 landed and reviewed (board §1);
- on branch `read-only-plots`, no tracked change in `git status`;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: the lock free, no ChamPBH
  process, the four ports free;
- `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` does not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. A real build under real Ray, and the proof that a real plotting run
writes nothing.

**Read first:**

1. [`README.md`](README.md): §0.3, §2.2 (03's charter), §3, §5 (rules 15 and 16, and the one-off
   exception for this prompt).
2. `prompts/dk-and-rpk-migration/README.md` §4.1, the protocol this prompt cuts down: the cluster,
   the grid, the builds, L1 and L3.
3. `.documents/dk-and-rpk-migration-verification.md` §3 (R2): what a build and its readers printed,
   and how long they took. Its §3.10 has the commands verbatim.
4. The rehearsal scripts, run **unchanged** (`.documents/dk-and-rpk-migration/rehearsal/`):
   - `ray_cluster.sh` (`start`, `stop`, `status`);
   - `build.sh` and `run_readers.sh`, each with `REHEARSAL_ROOT` set in its environment (both
     read it: `build.sh:85`, `run_readers.sh:73`);
   - `stage_counts.py`.

   Read each one's `--help`.
5. Logs 01 and 02 of this campaign: the address check's message, and `plot_inputs.py`'s messages.

---

## 1. What is wanted

The one proof that 02's tests cannot give: under real Ray, on a store `main.py` built, the plot
scripts and `--inventory` run to completion, draw what they drew before, and change **no byte** of
any file of the store. And 01's refusal and 02's stop, each shown once under real Ray.

This prompt is a one-off exception to `CLAUDE.md`'s "no campaign prompt starts" a `main.py` run with
a datastore, by the user's ruling U5 (README §5). It is about four minutes of build at the load R2
saw, a few minutes of readers, and one PRyMordial solve.

### 1.1 The protocol

Throughout: `R=~/ScienceData/ChamPBH-rehearsals/read-only-plots`, and `ADDRESS=127.0.0.1:6380`.
- Every driver is given `--ray-address $ADDRESS`.
- Every `ray` CLI call is given `--address $ADDRESS`.
- **No `ray stop`, ever.**
- Wait on each background build by its notification, or by a `Monitor` on its log's
  `** driver exit status` line, never a `sleep` loop.
- No command naming `/tmp/ray-cpbh` is left running across a stop.

**Steps:**
1. **The suites**, before `start`, serially, never with a cluster up: 01's and 02's counts.
2. `ray_cluster.sh start "read-only-plots 03 (R)"`. If the lock is held, it exits 3: **stop and ask**;
   never wait on it, never break it. Record `start`'s output, and `ray status --address $ADDRESS`.
3. **H1, the build.** Run
   `REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/build.sh 03 reference reference $ADDRESS`,
   in the background, waited on. It runs `main.py` on the pilot grid of
   `dk-and-rpk-migration` §4.1. Its job name will read `dk-and-rpk-migration-03-reference`, because
   the script is run unchanged: say so in the log. It must end `** driver exit status 0`.
   Then `stage_counts.py` on its log: `compute` 5 / 5 / 5.
4. **The checksums before.** `shasum -a 256` of all 17 files of `$R/03-reference*.db`, saved, with
   each file's size and mtime (`/usr/bin/stat -f '%m %z %N'`).
5. **H2, the readers.** Run `REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/run_readers.sh
   03 reference $ADDRESS`. Every step must exit 0:
   - `--inventory`;
   - `plot_by_beta.py --phi-init-Mp 5`;
   - `plot_ScalarModel.py`;
   - `bbn_from_store.py`;
   - `compare_stores.py` of the copy `before/` against the store.
6. **The checksums after**: the same 17 files, **byte-identical**, with mtimes and sizes unchanged.
   No `-journal`, `-wal` or `-shm` file beside any of them.
7. **H3, the refusals under real Ray.** Each is run on the same store, with its output to
   `$R/03-refusals/`, and then step 6's checksums are taken again.
   - `plot_by_beta.py` with `--ray-address auto`: exit 2, 01's message, before Ray. Confirm that no
     new Ray driver appears in `ray list jobs --address $ADDRESS`.
   - `plot_by_beta.py --phi-init-Mp 5 --T-stop-GeV 1e-7` (a `T_stop` the build did not use): exit 2
     with 02's message, before any figure. This is the parameter-miss path, a `ReadOnlyMiss` coming
     through `ray.get` from a real actor.
   - `plot_by_beta.py` with `--drop bbn-data`: exit 2, before Ray.
8. `ray_cluster.sh stop`, as a command on its own; then `status`, which must show the lock free and
   no process.

### 1.2 The items

- **H1 — the build**: exit 0; the stage counts; the provenance line naming both packages' tags.
- **H2 — the readers**: every step exits 0, and every store file is byte-identical before and after.
  `5-compare_stores.log` reads `RESULT: equal` and **no bookkeeping difference**: with nothing
  written, not even a timestamp moves.
  - `histories.csv`'s SHA-256 equals R0's, R1's and R2's:
    `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`
    (`.documents/dk-and-rpk-migration/out/10-readers.txt:116`). Nothing that computes has changed
    since R2, and 02 kept every payload.
  - The β = 1.15 Yp and D/H of `bbn_from_store` equal R2's.
- **H3 — the refusals** of step 7, each with its exit status and its message, and the store still
  byte-identical after them.
- **H4 — the record.** A new `.documents/read-only-plots-verification.md`, §1:
  - the cluster and the load;
  - the build;
  - the readers;
  - the checksums before and after, as a table or a committed file;
  - `histories.csv`;
  - the refusals;
  - the commands, verbatim.

  Small text outputs go under `.documents/read-only-plots/out/03-*`. A file over about 1 MB is a
  finding to report before committing.

---

## 2. Acceptance

1. H1–H4 as §1.2 says.
2. The cluster was started and stopped by the script alone, under the lock; the log shows `start`'s
   output, `ray status`, and `stop`'s exit status with its timing. Nothing is left running.
3. **Nothing outside `$R`** is opened or written: not the user's pilot store, not `~/ChamPBH-stores*`,
   not the Trash. The venv is unchanged (`pip freeze` before and after).
4. **One commit**, with the log `logs/03-rehearse-a-read-only-plotting-run.md`. The commit holds:
   - the verification document;
   - `.documents/read-only-plots/out/03-*`;
   - the board: row 03, H1–H4, the gate "R", and a `### 5.4 After prompt 03 (<date>)`.

   No code, no test, no script. Write "this commit" where the SHA goes.

## 3. Stop conditions: stop and ask (after `ray_cluster.sh stop`, unless the stop is what failed)

- The lock is held, or the cluster will not start.
- The build fails. 02 changed no write path, so that is not this campaign's to fix here.
- **Any store file changes** across the readers or the refusals.
- A reader fails, or `histories.csv` differs from R0's.
- A refusal does not come before Ray or before any figure, or comes with another message.
- An exception reaches a reader from the package other than the ones 02 handles.

## 4. What this prompt does not do

- It changes no code, test or script.
- It runs no resume and no second build.
- It keeps its store. Removing `$R` is the user's (README §3).

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
