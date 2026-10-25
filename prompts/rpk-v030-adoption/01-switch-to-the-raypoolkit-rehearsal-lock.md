# Prompt 01 — switch to the `raypoolkit` rehearsal lock (`v0.3.0`)

**Campaign:** [`README.md`](README.md) · **Board items:** **P1**, **S1**, **T1**, **C1**, **R1**,
**B1** · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-25, with the plan, at `ac187c8`.
**Where it runs:** on the laptop, in the main checkout, on branch `rpk-v030-adoption`. The checkout
and its venv are shared with `jordan-normalization`, which must be idle while 01 runs.
**Gate:**
- the planning commit has landed; `git status` is clean; no commit after it touches anything outside
  `prompts/rpk-v030-adoption/`;
- `./venv/bin/python -m runregistrykit list` shows nothing `running` or stale;
- the venv holds `raypoolkit` 0.2.0 at `eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, `datastorekit`
  0.4.0 at `c8570e849334d09085bbd84876095fc9e22bd005` and `runregistrykit` 0.2.0 at
  `0bae3a3c501a8264ee4be5bc24f06422af38720f`, none editable;
- **neither** `~/ScienceData/.rehearsal-lock` **nor** `~/.raypoolkit/rehearsal-lock` exists, and
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status` shows no ChamPBH process and the
  four ports free.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**.

**Read first:**

1. `CLAUDE.md`: "The packages", "Tests", "Ray on this machine", and "Long-running jobs" rule 4.
2. [`README.md`](README.md) whole: it is short.
3. **RayPoolKit** (`/Users/ds283/Documents/Code/RayPoolKit`), read with `git -C <RPK> show <rev>:<path>`:
   - at `v0.3.0`: `docs/rehearsal-lock.md`, the contract: §2 (how it is run), §3–§5 (the location,
     the owner file, whose lock), §6 (the process rule, and **"What it asks of a caller"**), §7 (the
     commands and their exit codes), §8 (`status`'s layout), §9 (the old location);
   - at `feae4e5`: `docs/adoption/rehearsal-lock/champbh.md`, the checklist for this repository,
     whole. It was measured at `807f175`; README §0.2 found it true of `ac187c8`. It is data: where
     it and this prompt differ, this prompt governs, and where the tree differs from it, the tree
     does.
4. `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` ("the script"), whole. Line numbers
   below are its at `ac187c8` (blob `eaae1e7`).

---

## 1. What is wanted

**Replace the script's lock with the package's, test the script's lock paths without Ray, and show
on the real cluster that the new lock sees it.** In one commit: the pin, the script, the test,
`CLAUDE.md`, the outputs, the log and the board.

The package is imported by every driver, so the install takes effect at once. Take the baselines
(§2.1) **before** the install.

**Each lock or script command is a shell command of its own, and a shell command that names
`/tmp/ray-cpbh` runs nothing but one lock command** (contract §6, "What it asks of a caller"). The
process rule lets an ancestor whose command line names the temp dir seed its walk, so everything
else that shell started is listed. Never put the temp dir in a `start` purpose.

## 2. What to do

**2.1 The baselines, before.** Save each under `.documents/rpk-v030-adoption/out/`, every block
headed by its command:
- `./venv/bin/pip freeze`, and `utilities.installed_package_provenance()` printed as JSON
  (`PYTHONPATH=.`), into `01-pip.txt`;
- the five roots, by `CLAUDE.md`'s commands (`ComputeTargets` in its two halves), serially, in the
  foreground, into `01-roots.txt`. Record each count. README §0.3 has the expected ones.

**2.2 The pin.** `requirements.txt:80` changes from

```text
raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0
```

to the same line with `@v0.3.0`. No other line moves. Then, into `01-pip.txt`:
- `./venv/bin/pip install --dry-run -r requirements.txt`: it must say
  `Would install raypoolkit-0.3.0` and nothing else;
- `./venv/bin/pip install -r requirements.txt`;
- `pip freeze` again, differing from 2.1's by the one `raypoolkit` line, naming commit
  `67a75b97a656accb98195a4bda93f64e6a5f3fb2`; `pip check`; the provenance again, with `raypoolkit`
  0.3.0 at `67a75b97…`, `requested_revision` `v0.3.0`, `editable` false, and the others as in 2.1;
- `./venv/bin/python -m raypoolkit.rehearsal_lock --help`, whose first line starts
  `usage: python -m raypoolkit.rehearsal_lock`.

**2.3 The script.** The lock half goes, the cluster half stays byte for byte (`temp_dir_regex`,
`list_cluster_pids`, `describe_pid`, `port_listeners`, `alive`, `stop_cluster`, `ray start`'s
arguments), and so do the cluster's values (`:66-74`), `say`/`err`, the port check in `start`, the
sourcing guard (`:397-400`) and every exit code. The checklist's §2 is the inventory of what goes,
and its §3 of what replaces it.
- **The calls.** One variable for the interpreter, `LOCK_PY="./venv/bin/python"`, and a small
  function that runs `"$LOCK_PY" -m raypoolkit.rehearsal_lock "$@"`. Every call passes
  `--project "$LOCK_PROJECT"`, `--ray-temp-dir "$CPBH_TEMP_DIR"` and, where the command takes it,
  `--ray-address "$CPBH_ADDRESS"`, **always from those variables**: the test (§2.4) overrides them,
  and the lock compares the strings as given (contract §5). `main` refuses (exit 2, as for
  `RAY_BIN`) a `start`, `stop` or `status` when `$LOCK_PY` is not executable.
- **Deleted:** `LOCK_PARENT`, `LOCK_DIR`, `OWNER_FILE` (`:76-79`), `iso_now`, `write_owner`,
  `acquire_lock` and `release_lock` (`:86-148`). `owner_field` and `print_owner` survive only in the
  form `stop`'s check needs (below).
- **`cmd_start`** (README §6.2 P3):
  1. `acquire --purpose "$purpose"` in place of `acquire_lock`, passing its exit through
     (`|| return $?`): 3 held or the old location present, 4 a process of the cluster alive (the
     lock released again), 1 the owner file unwritable, 2 a usage error;
  2. **the script's own stale-cluster check (`:282-288`) is deleted**: `acquire` has made it;
  3. the port check unchanged; on a held port, `release`, then return 4;
  4. the trap, `ray start` failing, and no `gcs_server` found: `stop_cluster`, then `release`, then
     the exit each has today (130, 1, 1). The module's `release` may refuse there, leaving the lock
     held, if something of the cluster survived `stop_cluster`. Its message says why; the script
     adds nothing;
  5. `set-pid` with the `gcs_server`'s pid in place of `write_owner` (`:331`). If `set-pid` fails:
     `err` a line saying so, `stop_cluster`, `release`, return 1.
- **`cmd_stop`**: its first check stays, reading the owner file at the module's location:
  `"$LOCK_PY" -c 'from raypoolkit.rehearsal_lock import default_lock_dir; print(default_lock_dir())'`
  (contract §2–§4). If that directory exists and its owner's `project` or `ray_temp_dir` is not
  ChamPBH's, print the owner file and return 1 **before `stop_cluster`**, as today. Then
  `stop_cluster … || return 1`, then `release … || return 1`.
- **`cmd_status`**: the module's `status --ray-temp-dir … --ray-address …`, then the script's own
  listing under a heading saying it is what `stop` would signal, then the four port lines. Exit 0.
- **The header** (`:17-31`) says the lock is `raypoolkit.rehearsal_lock` of the pinned `raypoolkit`,
  whose contract is RayPoolKit's `docs/rehearsal-lock.md` at `v0.3.0`, and that the brief's section 3
  still governs the cluster; the sourcing note stays. **The usage text** (`:36-62`) says where the
  lock is (`~/.raypoolkit/rehearsal-lock`), that `start` exits 3 also while the lock's old location
  exists and 4 also for a live process of the cluster, that `stop` leaves the lock held while
  anything of the cluster is alive, and that no shell command running the script may name the temp
  dir. **Neither names the old location by path**: the checklist's grep (§3 item 2) covers the
  script.

`bash -n` the script; record it in `01-script.txt` with `--help`'s output.

**2.4 The test, `tools/tests/test_ray_cluster_lock.py`** (README §6.2 P4). No Ray, no real `HOME`,
no real lock, no real cluster, by construction, so that no mutation of §3.1 can reach any of them.
- **The harness.** Each test runs `bash -c '<harness>' harness <cmd_…> [args]` from the repository
  root with an environment of its own: `HOME` a `tempfile` directory; the cluster's values in
  environment variables (a temp dir from a second `tempfile.mkdtemp`, four ports found free by
  binding port 0, the path of a stub `ray`, `RAY_CLUSTER_GRACE=2`). The harness sources the script
  under `set -u`, assigns `CPBH_TEMP_DIR`, the four ports, `CPBH_ADDRESS` (`127.0.0.1:<the GCS
  port>`) and `RAY_BIN` from those variables, and calls the function. **No value is on a command
  line**, so no ancestor of a lock command names the temp dir.
- **The stub `ray`** (a shell script in the first temp directory, outside the cluster temp dir)
  appends its arguments to a calls file and acts by an environment variable: `fail` exits 1;
  `ok` starts a detached fake `gcs_server` (`nohup <venv python> -c 'import time; time.sleep(120)'
  gcs_server <cluster temp dir>/session &`), writes its pid to a file, and exits 0; `nogcs` exits 0
  and starts nothing. `tearDown` kills a fake by its recorded pid if it is alive. Nothing looks for a
  process by name.
- **The other project's lock** is taken through the module's command line under the test's `HOME`,
  with `--project StochasticInstantons`, a third temp dir and `--ray-address 127.0.0.1:<free
  port>`, never the real `/tmp/ray-si`.
- **A process of the cluster**, where a test needs one, is the test's own `subprocess.Popen` of the
  same sleeping command, naming the cluster temp dir; the test kills and reaps it.
- **The two locations come from the module**, `default_lock_dir()` and `old_lock_dir()` (contract
  §2), which read `HOME` when called: the test calls them with its `HOME`, and spells neither path.
  So the test file names no old location, and §2.5's grep needs no exclusion for it.
- **The tests**, each docstring naming the failure it guards:
  1. **the script's values**, read as text: `LOCK_PROJECT="ChamPBH"`, `CPBH_TEMP_DIR="/tmp/ray-cpbh"`,
     `CPBH_GCS_PORT=6380`, `CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"`; the text names
     `raypoolkit.rehearsal_lock`; it contains neither component of `old_lock_dir()` relative to
     `HOME`, nor `acquire_lock`, `release_lock` or `write_owner`; `requirements.txt` pins
     `raypoolkit` at `v0.3.0`;
  2. **`status` on a free lock**: exit 0; the first line is `lock: free (<default_lock_dir()> does
     not exist)`; a line `Ray processes of <cluster temp dir>: none`;
  3. **`start` while another project holds the lock**: exit 3; its owner file byte-identical; the
     stub never called;
  4. **`start` while the old location exists** (a directory made at `old_lock_dir()`): exit 3; no
     lock created; the stub never called;
  5. **`stop` while another project holds the lock**: exit 1; its owner file byte-identical; a
     process of the cluster still alive; no line of `stop_cluster`'s output;
  6. **`start` while a process of the cluster is alive**: exit 4; the lock free afterwards; the stub
     never called; the process still alive;
  7. **`start` whose `ray start` fails** (`fail`): exit 1; the stub called once, with `start --head
     --temp-dir <cluster temp dir>`; the lock free afterwards;
  8. **`start` with no `gcs_server`** (`nogcs`): exit 1; the lock free afterwards;
  9. **the cycle through the module** (`ok`): `start` exit 0; the owner file has `project: ChamPBH`,
     the cluster temp dir, the address, and `pid:` the fake's pid; the module's `release` exit 1 (the
     pid alive); `status` exit 0, `lock: HELD`, `owner pid <pid>: alive`, the fake listed; `stop`
     exit 0; the fake gone; the lock free.

Run it into `01-test.txt`, then the deliberate breakage (§3.1) into the same file. `black` the file.

**2.5 The grep.** The checklist's item 8.2, on the working tree:

```bash
P='ScienceData|\.rehearsal-lock|rehearsal-lock-brief\.md, section 2|section 2 \(the lock\)'
{ git grep -n -E "$P" -- . ':!.documents' ':!.prompts' ':!prompts' ':!science-runs'
  git grep -n -E "$P" -- .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh; } | wc -l
```

5 before the edits; **0** after, the new test included (§2.4). Then `git grep -c 'raypoolkit\.rehearsal_lock' -- .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`:
at least 1. Both into `01-script.txt`.

**2.6 `CLAUDE.md`** (README §6.2 P6).
- **A new bullet, "The rehearsal lock."**, straight after "Ray on this machine", which is not
  edited. In the file's voice, about eight lines:
  - the lock is the directory `~/.raypoolkit/rehearsal-lock`, shared with StochasticInstantons and
    SecondaryGWKit, kept by `raypoolkit.rehearsal_lock` of the pinned `raypoolkit` (its contract is
    RayPoolKit's `docs/rehearsal-lock.md` at `v0.3.0`); the script takes it at `start` and releases
    it at `stop`, with ChamPBH's values (`--project ChamPBH --ray-temp-dir /tmp/ray-cpbh
    --ray-address 127.0.0.1:6380`); `./venv/bin/python -m raypoolkit.rehearsal_lock status` looks;
  - **exit 3 means stop and ask the user**: never wait, retry or remove the lock. It comes also while
    the lock's location from before `v0.3.0` exists, which `status` prints as `old location:
    PRESENT`. Exit 4: a process of the cluster is alive; run `stop`;
  - `stop` leaves the lock held while anything of the cluster is alive, and `status` shows what; a
    lock whose owner pid is not alive and which lists no process is removed by the user, by hand;
  - **no shell command that runs the script, or a lock command, names `/tmp/ray-cpbh`** beyond that
    one lock command: the lock would then list that shell's other children.
- **The `tools/tests` count** in "Tests" becomes the new one, and the sentence on the counts' date
  says that one is of this prompt's date, after `rpk-v030-adoption` 01.

**2.7 The roots, after.** The five again into `01-roots.txt`: `tools` higher by exactly 2.4's
tests, every other root as in 2.1, all OK. `black --check` on the new test.

**2.8 The cluster under the new lock** (README §6.2 P5). Each step its own shell command, its
command and output into `01-cluster.txt`, run from the repository root with
`S=.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` spelled out each time.
1. `bash $S status`: the lock free, no `old location` line, no processes, the ports free.
2. `bash $S start "rpk-v030-adoption 01: the cluster under the new lock"`: exit 0, printing the
   `gcs_server` pid. Exit 3 or 4: stop (§4).
3. `bash $S status`: the lock HELD with ChamPBH's owner file, `owner pid <the gcs_server pid>:
   alive`; the module's listing includes the `gcs_server`, the `raylet` and the Ray client server
   (`ray.util.client.server … --address=127.0.0.1:6380`); the script's own listing; the four ports
   held. **Compare the two listings' pids** and record any difference with each process's command.
4. `./venv/bin/python -m raypoolkit.rehearsal_lock release --project ChamPBH --ray-temp-dir /tmp/ray-cpbh`:
   exit 1, the recorded pid alive.
5. `./venv/bin/python -m raypoolkit.rehearsal_lock acquire --project ChamPBH --purpose "second acquire" --ray-temp-dir /tmp/ray-cpbh --ray-address 127.0.0.1:6380`:
   exit 3. Then `bash $S start "rpk-v030-adoption 01: second start"`: exit 3, starting nothing.
6. `bash $S stop`: exit 0; `stop_cluster`'s lines, then the module's `released the rehearsal lock`.
7. `bash $S status`: as step 1. Then
   `./venv/bin/python -m raypoolkit.rehearsal_lock ray-processes --ray-temp-dir /tmp/ray-cpbh --ray-address 127.0.0.1:6380`:
   exit 0.

Nothing else runs on the cluster. If any step leaves the lock held unexpectedly, stop and leave it
held (§4).

## 3. Acceptance

1. **The pin.** The dry run adds only `raypoolkit`; after it, `raypoolkit` 0.3.0 at `67a75b97…`,
   not editable; the other packages unchanged; `pip check` clean.
2. **No live line names the old lock**: §2.5's grep finds 0; the script calls the module.
3. **The script** keeps its cluster half byte for byte (the log shows `git diff` limited to the lock
   half, the header and the usage text), and its exit codes.
4. **The test** passes, needs no Ray, and catches each mutation of §3.1.
5. **The roots:** `tools` + the new tests, the rest unchanged, all OK.
6. **On the real cluster**, the lock lists the cluster's `gcs_server`, `raylet` and client server;
   refuses `release` and a second `acquire` while it runs; `stop` releases it; nothing is left.
7. `git diff --cached --name-only` is: `requirements.txt`, `CLAUDE.md`, the script,
   `tools/tests/test_ray_cluster_lock.py`, `.documents/rpk-v030-adoption/out/01-*.txt`, the log and
   the board. Nothing else.

### 3.1 Deliberate breakage

Apply each to the script (or `requirements.txt`), run the new test module, record which tests
failed and the diff exactly as applied, and revert:
- **M1** — `cmd_start`'s `|| return $?` after `acquire` dropped.
- **M2** — `cmd_stop`'s first check deleted.
- **M3** — the `set-pid` call deleted.
- **M4** — the `release` after a failed `ray start` deleted.
- **M5** — `CPBH_TEMP_DIR="/private/tmp/ray-cpbh"`.
- **M6** — `requirements.txt` back at `v0.2.0` (the line only; the venv untouched).

## 4. Stop conditions — stop and ask

- A Gate item is not as stated, or a lock exists at the old location at any point.
- The dry run would install, upgrade or remove anything but `raypoolkit`.
- A root fails, or a count differs from §2.7's expectation.
- The script cannot keep a path's exit code, or `stop`'s refusal of another project's lock, with
  the module's commands.
- On the cluster: `start` exits 3 or 4 at step 2; the module does not list the `gcs_server`, the
  `raylet` or the client server; `release` or a second `acquire` succeeds while the cluster runs;
  `stop` fails or leaves the lock held; a process of `/tmp/ray-cpbh` outlives `stop`.
- Anything suggests a defect in `raypoolkit`: record it on the board and in
  `.documents/OPEN_ISSUES.md`, and stop. It is not worked around here.

**If you stop with the lock held**, say so and leave it held, with its owner file printed: the
orchestrator decides. If you stop after the install, say that the venv holds `v0.3.0`.

## 5. What this prompt does not do

- It touches no other repository, and no Python file but the new test.
- It changes no record: nothing under `prompts/` but this campaign's, `science-runs/`, or
  `.documents/` but the script and `.documents/rpk-v030-adoption/out/`.
- It changes nothing a build computes or stores, and runs no driver.
- It does not merge anything, or close the campaign: the review does.

## 6. The log and the board

**The log**, at `logs/01-switch-to-the-raypoolkit-rehearsal-lock.md`, in README §5.1's form, with
the breakage record, every count, the script's diff summarised, and the cluster transcript
summarised (the two listings compared).

**The board:** the header ("01 landed, awaiting review"); the §1 row; items P1, S1, T1, C1, R1 and
B1; gate R; `### 5.1 After prompt 01 (<date>)`. `.documents/OPEN_ISSUES.md` only if §3 gains an
entry.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
