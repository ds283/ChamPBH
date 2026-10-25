# Log 01 — switch to the `raypoolkit` rehearsal lock (`v0.3.0`)

**Subject:** Switch the cluster script to the raypoolkit v0.3.0 rehearsal lock
**Commit:** this commit, on branch `rpk-v030-adoption`, on top of `5478c6e`
**Date:** 2026-10-25, by Claude Opus 5.5
**Result:** **COMPLETE.** P1, S1, T1, C1, R1 and B1 shipped; gate R holds. The venv holds
`raypoolkit` 0.3.0 at `67a75b97…`. The roots were 18, 61 + 115, 78, 84 and 61 before, and are 18,
61 + 115, 78, **93** and 61 after, all OK. The new test catches all six mutations, each failing
exactly the tests the orchestrator's note expected. No §4 stop condition was met. No issue was
opened or closed, so `.documents/OPEN_ISSUES.md` is unchanged.

The orchestrator's note for 01 (§0, six rulings) governed where it differs from the prompt. Each
ruling is classified below as the note says.

## What shipped

- **P1 — the pin.** `requirements.txt:80` now reads `raypoolkit @
  git+https://github.com/ds283/RayPoolKit@v0.3.0`. No other line moved. The dry run printed
  `Would install raypoolkit-0.3.0` and nothing else. The install uninstalled 0.2.0 and installed
  0.3.0. The `pip freeze` diff is the one `raypoolkit` line, from `eaef0ce6…` to
  `67a75b97a656accb98195a4bda93f64e6a5f3fb2`, and `pip check` finds no broken requirements.
  `installed_package_provenance()` reports `raypoolkit` 0.3.0 at `67a75b97…`, `requested_revision`
  `v0.3.0`, `editable` false. It reports `datastorekit` unchanged (0.4.0 at `c8570e84…`, `v0.4.0`,
  not editable). It does not report `runregistrykit` (ruling 6); `pip freeze` shows that pin unchanged at
  `0bae3a3c…`. `python -m raypoolkit.rehearsal_lock --help` exits 0, and its first line starts
  `usage: python -m raypoolkit.rehearsal_lock`. Recorded in `01-pip.txt`.
- **S1 — the script**, `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`:
  `git diff --stat` shows 86 insertions and 107 deletions, and the file is now 379 lines (400 before).
  The changes, by function:
  - **Header** (`:17-27`): the lock is `raypoolkit.rehearsal_lock` of the pinned `raypoolkit`,
    whose contract is RayPoolKit's `docs/rehearsal-lock.md` at `v0.3.0`, and section 3 of SI's
    brief still governs the cluster. The paragraphs on `ray stop` and sourcing are unchanged.
  - **`usage`**: `start` names `~/.raypoolkit/rehearsal-lock` and `raypoolkit.rehearsal_lock`. It
    says `start` exits 3 also while the location from before `v0.3.0` exists, and 4 also for a live
    process of the cluster (the lock released again). `stop` now says that the lock stays held
    while anything of the cluster is alive. `status` describes its three parts, and a closing
    sentence says that no shell command running the script may name the temp dir. Neither the
    header nor the usage text names the old location by path.
  - **Deleted:** `LOCK_PARENT`, `LOCK_DIR`, `OWNER_FILE`, `iso_now`, `write_owner`, `acquire_lock`
    and `release_lock`.
  - **New:** `LOCK_PY="./venv/bin/python"`; `lock_cmd` (runs `"$LOCK_PY" -m
    raypoolkit.rehearsal_lock "$@"`); `lock_release` (`lock_cmd release --project "$LOCK_PROJECT"
    --ray-temp-dir "$CPBH_TEMP_DIR"`). `owner_field LOCK_DIR KEY` and `print_owner LOCK_DIR`
    survive in the form `stop`'s check needs, taking the lock directory as an argument.
  - **`cmd_start`**: `lock_cmd acquire --project … --purpose "$purpose" --ray-temp-dir … --ray-address
    … || return $?` replaces `acquire_lock`. The script's own stale-cluster check is deleted. The
    port check is unchanged, and `lock_release` then `return 4` on a held port. The trap, the
    failed-`ray start` path and the no-`gcs_server` path call `lock_release` where they called
    `release_lock`, with the exits (130, 1, 1) unchanged. `lock_cmd set-pid … "$gcs"` replaces
    `write_owner`. If it fails, an `err` line is printed, then `stop_cluster`, `lock_release`,
    `trap - INT TERM` and `return 1`.
  - **`cmd_stop`**: the owner check stays. The lock directory is asked of the module (`"$LOCK_PY" -c
    'from raypoolkit.rehearsal_lock import default_lock_dir; print(default_lock_dir())'`). If that
    call fails or prints nothing, `stop` prints an `err` line and returns 1 before `stop_cluster`
    (ruling 4). If the directory exists and its owner's `project` or `ray_temp_dir` is not ChamPBH's,
    `stop` prints the owner file and returns 1 before `stop_cluster`, as before. Then
    `stop_cluster … || return 1` and `lock_release || return 1`.
  - **`cmd_status`**: prints the module's `status --ray-temp-dir … --ray-address …`, then the
    script's own listing under the heading `what stop would signal (this script's rule, <dir>):`,
    then the four port lines. It exits 0.
  - **`main`**: for `start`, `stop` or `status`, it refuses with exit 2 when `$LOCK_PY` is not
    executable, as `cmd_start` does for `RAY_BIN`.
  - **Unchanged, checked by `diff` against `HEAD`** (`01-script.txt`): the cluster's values
    (`:66-74` before), `say`/`err`, the cluster half (`temp_dir_regex`, `list_cluster_pids`,
    `describe_pid`, `port_listeners`, `alive` and `stop_cluster`), `ray start`'s arguments,
    `start`'s port check and the sourcing guard. `bash -n` passes.
- **T1 — `tools/tests/test_ray_cluster_lock.py`**: nine tests (404 lines, black-clean) that start no
  Ray and touch no real `HOME`, real lock or real cluster. The module ran in **11.5 s**
  (`01-test.txt`; 12.1 s in a first run). What each asserts:
  1. **The script's values and the pin**, read as text: the four lines
     `LOCK_PROJECT="ChamPBH"`, `CPBH_TEMP_DIR="/tmp/ray-cpbh"`, `CPBH_GCS_PORT=6380` and
     `CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"`. The text names `raypoolkit.rehearsal_lock`, and
     contains neither component of `old_lock_dir()` relative to `HOME`, nor `acquire_lock`,
     `release_lock` or `write_owner`. The one `raypoolkit` line of `requirements.txt` pins `v0.3.0`.
  2. **`status` on a free lock**: exit 0; the first line is `lock: free (<default_lock_dir()> does
     not exist)`; and the line `Ray processes of <temp dir>: none` appears.
  3. **`start` while another project holds the lock**: exit 3; the owner file byte-identical; the
     stub never called.
  4. **`start` while the old location exists**: exit 3; nothing at `default_lock_dir()`; the stub
     never called.
  5. **`stop` while another project holds the lock**: exit 1; the owner file byte-identical; the
     test's process of the cluster still running (`poll()` is `None`); none of `stop_cluster`'s
     phrases (`no process names`, `SIGTERM to`, `grace period`, `are free`) in the output.
  6. **`start` while a process of the cluster is alive**: exit 4; the lock free; the stub never
     called; the process still running.
  7. **`start` whose `ray start` fails**: exit 1; the stub called once, its arguments starting
     `start --head --temp-dir <temp dir> `; the lock free.
  8. **`start` with no `gcs_server`**: exit 1; the lock free.
  9. **The cycle through the module**: `start` exits 0. The owner file has `project: ChamPBH`, the
     temp dir, the address, and `pid:` the fake's pid. The module's `release` exits 1, saying that
     the recorded pid is alive. `status` exits 0, with `lock: HELD (<lock>)`, `owner pid <pid>:
     alive`, `Ray processes of <temp dir>: 1` and a listed line for the pid. `stop` exits 0 and
     prints `released the rehearsal lock`. The fake is then gone (`os.kill(pid, 0)` fails within
     the grace period plus 5 s), and the lock is free.
- **C1 — `CLAUDE.md`**: a new bullet, "The rehearsal lock.", straight after "Ray on this machine",
  which is not edited. It says where the lock is, who shares it and whose module keeps it, with
  ChamPBH's values and the `status` command. Exit 3 means stop and ask the user, and it also
  comes while the location from before `v0.3.0` exists (`old location: PRESENT`). Exit 4 means run
  `stop`. `stop` leaves the lock held while anything of the cluster is alive. A dead lock that lists
  no process is the user's to remove, by hand. And the rule on shell commands naming
  `/tmp/ray-cpbh`. The bullet names no old location. Separately, `tools/tests` `# 84` becomes
  `# 93`, and the parenthesis reads "(The counts are those of 2026-10-20; `tools/tests`'s is of
  2026-10-25, after `rpk-v030-adoption` 01.)". A three-way `git merge-file` of this `CLAUDE.md`,
  `ac187c8`'s and `jordan-normalization`'s reports no conflict.
- **R1 — the real cluster**, `01-cluster.txt` (below, "Verification").
- **B1 — the baselines**, `01-pip.txt` and `01-roots.txt`.
- **Outputs**, `.documents/rpk-v030-adoption/out/`: `01-pip.txt`, `01-roots.txt`, `01-script.txt`,
  `01-test.txt` and `01-cluster.txt`.

## Deviations from the prompt

1. **The fake `gcs_server` is `/usr/bin/perl -e 'sleep 120' gcs_server <temp dir>/session`**, in the
   stub and as the test's own process of the cluster (tests 5 and 6), not the venv's Python.
   **STRUCTURALLY REQUIRED**, by the orchestrator's note, ruling 1. Under MacPorts' framework Python,
   `gcs_server` falls past `describe_pid`'s 150-column cut, and `cmd_start` would never find it.
2. **No fake holds the test's pipes.** The stub starts the fake as `nohup … </dev/null >/dev/null
   2>&1 &`. The test's `Popen` passes `stdin`, `stdout` and `stderr` as `subprocess.DEVNULL`. Every
   `subprocess.run` has a 60 s `timeout`. **STRUCTURALLY REQUIRED**, ruling 2.
3. **"Still alive"** is `Popen.poll() is None` for the test's own child, and for the stub's fake
   (re-parented to `launchd`) "gone" is `os.kill(pid, 0)` failing, allowed up to the grace period
   plus 5 s. `tearDown` kills and reaps the test's children, and kills the stub's fake by its
   recorded pid if it is alive. **IMPLEMENTATION CHOICE**, ruling 3. One addition: `tearDown` kills
   the recorded pid only if `ps -o command= -p <pid>` still names the test's temp dir, so a reused
   pid is never signalled (**IMPLEMENTATION CHOICE**).
4. **Three choices the prompt left open** (ruling 4), each an **IMPLEMENTATION CHOICE**:
   - the stub with no mode records its arguments and exits 1, starting nothing. A test that says
     "never called" reads the calls file as absent or empty;
   - `cmd_stop` prints an `err` line and returns 1 before `stop_cluster` when the module cannot be
     asked for `default_lock_dir()`;
   - the tests compute both locations with `mock.patch.dict(os.environ, {"HOME": <test HOME>})`
     around `default_lock_dir()` and `old_lock_dir()`, importing the module normally.
5. **The `tools/tests` count after** is 84 + 9 = **93**: one method for each item of §2.4 (ruling 5).
   Not a deviation; recorded because the note fixes it.
6. **The provenance after** lists `datastorekit` and `raypoolkit` only, and `pip freeze` shows
   `runregistrykit`'s unchanged pin (ruling 6). Not a deviation; the note says how to read "the
   others as in 2.1".
7. **The stub's `ok` waits until its fork runs perl** (up to 10 s, checking `ps -o command= -p
   <pid>`) before it exits 0, as `ray start` returns only once its `gcs_server` runs. Without the
   wait, `cmd_start`'s search could catch the fork while it is still the stub's subshell, whose
   command names the temp dir but not `gcs_server`, and exit 1. **IMPLEMENTATION CHOICE.**
8. **The stub takes the temp dir from its own `--temp-dir` argument**, so the fake names what the
   script passed to `ray start`, not a value of the test's. **IMPLEMENTATION CHOICE.**
9. **`lock_release`**, a second small function beside `lock_cmd`, so that the six release calls are
   one spelling. **IMPLEMENTATION CHOICE.**
10. **`status`'s heading for the script's listing** is `what stop would signal (this script's rule,
    <dir>): …`, in place of `ChamPBH cluster (<dir>): …`. **IMPLEMENTATION CHOICE** (the prompt
    asks for a heading saying so).
11. **Assertions beyond §2.4's words**, each within its item, and each an **IMPLEMENTATION CHOICE**:
    - test 9 checks that the module's `release` names the live pid, the count line `Ray processes of
      <dir>: 1` and the pid's listed line, and that `stop` printed `released the rehearsal lock`;
    - test 5 checks four of `stop_cluster`'s phrases;
    - test 7 checks the prefix of the stub's arguments.
12. **M2 deletes the whole first check of `cmd_stop`**, the `default_lock_dir()` lookup included,
    and leaves the `local … lock_dir` declaration. **IMPLEMENTATION CHOICE** (how to apply M2).
13. **The outputs.** `01-pip.txt` gives the install's output without its 99 "Requirement already
    satisfied" lines, and says so; the dry run is whole. `01-cluster.txt` has Ray's ANSI colour codes
    removed from step 2, and says so. **IMPLEMENTATION CHOICE.**
14. **Steps 4 and 5a of §2.8 ran as the lone command of their Bash calls.** Their output was copied
    into `01-cluster.txt` from the tool's transcript after step 7. **STRUCTURALLY REQUIRED** by the
    note's convention: while the cluster ran, no shell command could name `/tmp/ray-cpbh` except
    those two.
15. **`CLAUDE.md`'s last sentence** reads "No shell command that runs the script names
    `/tmp/ray-cpbh`, and one that runs a lock command naming it runs nothing else". The prompt
    words this rule differently; the wording here is §1's ("a shell command that names
    `/tmp/ray-cpbh` runs nothing but one lock command"). **IMPLEMENTATION CHOICE.**

No **UNINTENDED DRIFT**.

## Verification performed

- **The gate**, checked again before acting:
  - `git status` clean at `5478c6e`, whose only change after `51c47b7` is
    `prompts/rpk-v030-adoption/orchestrator/prompt-01.md`;
  - `runregistrykit list` shows no runs;
  - `pip freeze` shows `raypoolkit` at `eaef0ce6…`, `datastorekit` at `c8570e84…` and
    `runregistrykit` at `0bae3a3c…`, none editable, with `ray==2.53.0`;
  - neither lock location exists, and `~/.raypoolkit` exists, empty;
  - the script's `status` showed the lock free, no processes and the four ports free, and
    `pgrep -lf 'gcs_server|raylet'` found nothing;
  - `.documents/rpk-v030-adoption/` did not exist; the script's blob was `eaae1e7`.
- **The roots** (`01-roots.txt`; serial, foreground, `ComputeTargets` in halves, `uptime` beside
  each):

  | Root | Before (v0.2.0) | After |
  |---|---|---|
  | CosmologyModels | 18 OK (157 s, load 15–49) | 18 OK (68 s, load 12) |
  | ComputeTargets `[a-f]` | 61 OK (163 s, load 45–48) | 61 OK (96 s, load 12) |
  | ComputeTargets `[g-z]` | 115 OK (106 s, load 27–45) | 115 OK (102 s, load 12) |
  | ObjectFactories | 78 OK | 78 OK |
  | tools | 84 OK | **93** OK |
  | RunDrivers | 61 OK | 61 OK |

  `tools` rises by exactly the nine new tests, and every other count is as before.
  `black --check` on the new test: unchanged.
- **§2.5's grep** (`01-script.txt`) found **5** lines before the edits (`:21`, `:44`, `:76`, `:77`,
  `:78`, all in the script). After the edits it finds **0**, and still 0 with `--untracked`, which
  takes in the new test. `git grep -c 'raypoolkit\.rehearsal_lock'` on the script gives **10**.
- **§2.8, the real cluster** (`01-cluster.txt`, 02:13–02:14 GMT, one-minute load 11 then 9):
  1. `status`: exit 0. The lock is free at `~/.raypoolkit/rehearsal-lock`, there is no
     `old location` line, both listings are empty, and the four ports are free.
  2. `start "rpk-v030-adoption 01: the cluster under the new lock"`: exit 0 in 6 s. The module
     printed `acquired the rehearsal lock … for ChamPBH` and `recorded pid 89972`; the
     `gcs_server` pid is 89972.
  3. `status`: exit 0. The lock is HELD with ChamPBH's owner file (`ray_temp_dir: /tmp/ray-cpbh`,
     `ray_address: 127.0.0.1:6380`, `pid: 89972`), with `owner pid 89972: alive`. **The module lists
     18 processes and the script lists 18, and they are the same pids, 89972–89989**; no process
     is in one listing and not the other. Among them are the `gcs_server` (89972), the `raylet`
     (89987) and the Ray client server (89974, `-m ray.util.client.server --address=127.0.0.1:`, cut
     at 150 columns). All four ports are held. **In the real `gcs_server`'s `describe_pid` line,
     `gcs_server` starts at column 104** of the 150 (ruling 1).
  4. The module's `release`: exit 1, `release: the recorded pid 89972 is alive; not releasing`.
  5. A second `acquire`: exit 3, printing the owner file. A second `start`: exit 3, starting
     nothing.
  6. `stop`: exit 0 in 19 s. It sent SIGTERM to the 18 pids, and SIGKILL to 89972 when the 15 s
     grace ran out. Then `no process names /tmp/ray-cpbh; ports … are free`, and `released the
     rehearsal lock (~/.raypoolkit/rehearsal-lock)`.
  7. `status`: as step 1. The module's `ray-processes`: exit 0, no output.

  Afterwards neither lock location exists, `~/.raypoolkit` is empty, and `pgrep -lf
  'gcs_server|raylet'` finds nothing. **Gate R holds.**

## The deliberate-breakage record

Each mutation, recorded in `01-test.txt` with its diff exactly as applied, was checked with `git
apply --check`, applied, checked with `git apply -R --check` and run against the new module. It
was then reverted with `git apply -R`. After all six, `shasum` of the script and `requirements.txt`
matched the values taken before, and `git status` showed only the work. Every failure matches the
note's expected record.

| Mutation | Diff, in short | Tests failed | Why |
|---|---|---|---|
| **M1** | `cmd_start`: ` \|\| return $?` dropped after `acquire` | 3, 4, 6 | each exits 1, not 3/3/4, after calling the stub; in 6, `stop_cluster` then signals the test's process |
| **M2** | `cmd_stop`: the first check deleted (`:317-334`, the lookup and the owner check) | 5 | `stop_cluster` SIGTERMs the test's process (`-15 is not None`) |
| **M3** | `cmd_start`: the `set-pid` block deleted (`:300-306`) | 9 | the owner file's `pid:` is `-` |
| **M4** | `cmd_start`: `lock_release` after a failed `ray start` deleted (`:284`) | 7 | the lock is left held |
| **M5** | `CPBH_TEMP_DIR="/private/tmp/ray-cpbh"` | 1 | the value read as text |
| **M6** | `requirements.txt:80` back at `@v0.2.0` (the venv untouched) | 1 | the pin read as text |

## Observations not acted on

- **`describe_pid` cuts at 150 columns** (cluster half, out of scope). In the real `gcs_server`'s
  line, `gcs_server` starts at column 104, which leaves 46 columns of room. A `gcs_server`
  started from a longer venv path, or one that MacPorts' framework Python re-executes under its own
  path, would lose the word and make `start` exit 1 at "no gcs_server" (ruling 1). Under the note, no
  issue is opened; the orchestrator decides at the review.
- **The real `gcs_server` outlived the 15 s SIGTERM grace** in step 6 and was SIGKILLed, after which
  `stop` succeeded. The same happens in 05a's log and in `read-only-plots` 03's and `rrk-adoption`
  04's cluster transcripts. That behaviour is the cluster half's and is already on record, so no issue.
- **pip's "Resolved … to commit `caf00765…`"** names `v0.3.0`'s annotated tag object. The commit it
  points at, `67a75b97…` (`refs/tags/v0.3.0^{}` in `git ls-remote`), is what `pip freeze` and the
  provenance record.
- **The module's `status` cuts each listed command at 150 columns** (contract §7), so the client
  server's `--address=127.0.0.1:6380` shows only as `--address=127.0.0.1:` there. The script's
  listing, by its own rule, needs the whole address and found the same pid.
- **Test 1's `assertIn` on the script's text prints the whole script** in its failure message, as
  under M5. This is cosmetic.

## State handed on

- **The venv holds `raypoolkit` 0.3.0 at `67a75b97…`**, installed from this commit's
  `requirements.txt`. `jordan-normalization` shares this venv while its `requirements.txt` still
  names `v0.2.0`, which is harmless because `RayWorkPool` is unchanged (README §4.2). A revert of
  this commit needs `./venv/bin/pip install "raypoolkit @
  git+https://github.com/ds283/RayPoolKit@v0.2.0"` too (README §4.1).
- **The lock is free at both locations**, and no Ray process of `/tmp/ray-cpbh` is running.
- **For `jordan-normalization` 09's note:** the script's `status` now prints the module's layout
  (contract §8), then `what stop would signal (this script's rule, /tmp/ray-cpbh): …`, then the
  ports. `start` exits 3 also for the old location, and 4 also for a live process of the cluster.
  `stop` exits 1 with the lock left held if `release` refuses. And no shell command that runs the
  script may name `/tmp/ray-cpbh`.
- **Merge:** a three-way `git merge-file` of `CLAUDE.md` against `jordan-normalization` (base
  `ac187c8`) is clean. `prompts/INDEX.md` is untouched here.
- Nothing is committed beyond this commit; the review closes the campaign.
