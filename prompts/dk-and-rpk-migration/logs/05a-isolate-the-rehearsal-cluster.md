# Log — prompt 05a, isolate the rehearsal cluster, and take the shared rehearsal lock

**Prompt:** [`05a-isolate-the-rehearsal-cluster.md`](../05a-isolate-the-rehearsal-cluster.md) ·
**Campaign:** `dk-and-rpk-migration` · **Board items:** C1–C3 · **Model:** Claude Sonnet 5.5 ·
**Date:** 2026-10-12 · **Commit:** `babd9d5` (on `dk-and-rpk-migration`, parent `b114692`) ·
**Result:** the script is written and proved on a real cluster beside a decoy; C2's five facts
measured on Ray 2.53.0; four breakages caught. No Python, no venv change, no store opened; no
Ray process, port or lock left behind.

## 1. What shipped

- **New** `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` (mode `100755`):
  `start PURPOSE [--num-cpus N]`, `stop`, `status`, `--help`. It is SI's script at `3556148` with
  prompt 05a's changes 1–4 (§2). It never runs `ray stop`.
- This log, and the board (§1 row, C1–C3, `### 5.9`). No issue opened, so
  `.documents/OPEN_ISSUES.md` is unchanged.

## 2. `diff -u` against SI's file, every hunk classified

SI's file is `git -C /Users/ds283/Documents/Code/StochasticInstantons show
3556148:.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` (363 lines). 12 hunks:

| # | Hunk (lines of the diff) | Class |
|---|---|---|
| 1 | header comment: ChamPBH, prompt 05a, U5, "adapted from SI's at `3556148`", the brief at `ed9399a`; licence header untouched and complete | change 1 |
| 2 | usage: ChamPBH's temp dir and ports, default 10 CPUs and why, the exit 4 on a held port, the four-port confirmation | changes 1, 2, 3, 4 |
| 3 | identity block: `LOCK_PROJECT="ChamPBH"`, `CPBH_*` variables, `/tmp/ray-cpbh`, 6380 / 6381 / 8267 / 52367 | change 1 |
| 4 | `write_owner`: the two variable names only | change 1 (names) |
| 5 | `release_lock`: comment, the check's variable names and the message's wording ("SI" to "ChamPBH") only; the condition is the same | change 1 (names) |
| 6 | `temp_dir_regex`'s comment (`ray-simple` to `ray-cpbhx`) | change 1 (names) |
| 7 | `stop_cluster`: five arguments; the port check loops over all four, reports each held port, then returns 1; the success line names the four | change 3 |
| 8 | `cmd_start` `ncpus=4` to `ncpus=10` | change 2 |
| 9 | `cmd_start`: names; the new held-port refusal between the stale-process guard and the trap (releases the lock, exit 4) | change 1 (names), change 4 |
| 10 | `cmd_start`: the trap and the two failure paths call `stop_cluster` with four ports; names | changes 1, 3 |
| 11 | `cmd_stop`: names; `stop_cluster` with four ports | changes 1, 3 |
| 12 | `cmd_status`: names; the four ports reported | changes 1, 3 |

**The lock functions** (`acquire_lock`, `release_lock`, `write_owner`, `owner_field`,
`print_owner`) differ from SI's by variable names and by the word "ChamPBH" in `release_lock`'s
messages and comment; `acquire_lock`, `owner_field` and `print_owner` are byte-identical. The
match (`list_cluster_pids`, `temp_dir_regex`'s body), the client-server seed, the descendants,
the exemptions, `SIGTERM`/grace/`SIGKILL`, the trap, the `>> ray_cluster:` lines and the ability
to be sourced are unchanged. `list_cluster_pids` keeps its two-argument signature, as the prompt's
step 4 calls it.

```diff
--- /private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/0ff176ad-e3a5-409e-abee-a307071401fe/scratchpad/agent-05a/si_ray_cluster.sh	2026-10-12 11:50:00
+++ .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh	2026-10-12 11:50:31
@@ -14,18 +14,21 @@
 # See the License for the specific language governing permissions and
 # limitations under the License.
 #
-# StochasticInstanton's rehearsal cluster, and the rehearsal lock it shares with ChamPBH and
-# SecondaryGWKit. Written for dk-and-rpk-migration prompt 01a, which implements
-# .prompts/dk-and-rpk-migration/rehearsal-lock-brief.md: section 2 (the lock) and section 3 (the
-# cluster) are followed exactly, because the contract is shared with the other projects.
+# ChamPBH's rehearsal cluster, and the rehearsal lock it shares with StochasticInstantons (SI) and
+# SecondaryGWKit. Written for dk-and-rpk-migration prompt 05a, under U5 (README section 4.1 and
+# section 6.6 of this campaign). Adapted from SI's script at StochasticInstantons commit 3556148
+# (.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh), and it keeps the contract of SI's
+# .prompts/dk-and-rpk-migration/rehearsal-lock-brief.md at ed9399a: section 2 (the lock) and
+# section 3 (the cluster) are followed exactly, because the contract is shared with the other
+# projects.
 #
-# This script NEVER runs `ray stop`, which stops every Ray process on the machine. It stops SI's
-# cluster by signalling the processes whose command line names SI's temp dir, and their
-# descendants (Ray's workers carry only their process title, so they are reached through their
-# parent). It never waits on a held lock, and never removes a lock that is not SI's.
+# This script NEVER runs `ray stop`, which stops every Ray process on the machine. It stops
+# ChamPBH's cluster by signalling the processes whose command line names ChamPBH's temp dir, and
+# their descendants (Ray's workers carry only their process title, so they are reached through
+# their parent). It never waits on a held lock, and never removes a lock that is not ChamPBH's.
 #
-# It may be sourced (its functions used, its main skipped): the proof of prompt 01a stops a
-# decoy cluster by calling stop_cluster with the decoy's temp dir.
+# It may be sourced (its functions used, its main skipped): the proof of prompt 05a stops a decoy
+# cluster by calling stop_cluster with the decoy's temp dir and ports.
 
 set -u
 
@@ -38,31 +41,35 @@
 
 Run from the repository root.
 
-  start PURPOSE   take the rehearsal lock (~/ScienceData/.rehearsal-lock), then start SI's Ray
-                  head on /tmp/ray-si: GCS 6390, client 6391, dashboard 8266, dashboard agent
-                  52366, N CPUs (default 4). PURPOSE is written to the lock's owner file, e.g.
-                  "dk-and-rpk-migration 02 (RH0)". If the lock is held, prints its owner file and
-                  exits 3, starting nothing. Prints the cluster's address and the command that
-                  lists its actors.
-  stop            stop SI's processes (SIGTERM to every process naming /tmp/ray-si, and to their
-                  descendants; a grace period of $RAY_CLUSTER_GRACE seconds, default 15; then
-                  SIGKILL), confirm none is left and port 6390 is free, then release the lock if
-                  its owner is SI's. Touches nothing if the lock is another project's.
-  status          the lock, its owner file, whether the owner's pid is alive, and SI's processes.
+  start PURPOSE   take the rehearsal lock (~/ScienceData/.rehearsal-lock), then start ChamPBH's Ray
+                  head on /tmp/ray-cpbh: GCS 6380, client 6381, dashboard 8267, dashboard agent
+                  52367, N CPUs (default 10: R0's cluster had 10, Ray's default on this machine,
+                  and R1 and R2 are compared with R0, wall-clock limits included). PURPOSE is
+                  written to the lock's owner file, e.g. "dk-and-rpk-migration 06 (R1)". If the
+                  lock is held, prints its owner file and exits 3, starting nothing. If any of the
+                  four ports is already held, releases the lock and exits 4, starting nothing.
+                  Prints the cluster's address and the command that lists its actors.
+  stop            stop ChamPBH's processes (SIGTERM to every process naming /tmp/ray-cpbh, to
+                  Ray's client server on 6380, and to their descendants; a grace period of
+                  $RAY_CLUSTER_GRACE seconds, default 15; then SIGKILL), confirm none is left and
+                  all four ports (6380, 6381, 8267, 52367) are free, then release the lock if its
+                  owner is ChamPBH's. Touches nothing if the lock is another project's.
+  status          the lock, its owner file, whether the owner's pid is alive, ChamPBH's processes
+                  and its four ports.
 
 Never runs `ray stop`. Never waits on a held lock.
 EOF2
 }
 
-# ---- SI's cluster (rehearsal-lock-brief.md, section 3) ----
-LOCK_PROJECT="StochasticInstantons"
-SI_TEMP_DIR="/tmp/ray-si"
-SI_GCS_PORT=6390
-SI_CLIENT_PORT=6391
-SI_DASHBOARD_PORT=8266
-SI_AGENT_PORT=52366
-SI_ADDRESS="127.0.0.1:${SI_GCS_PORT}"
-SI_DASHBOARD="http://127.0.0.1:${SI_DASHBOARD_PORT}"
+# ---- ChamPBH's cluster (rehearsal-lock-brief.md, section 3; README section 4.1, U5) ----
+LOCK_PROJECT="ChamPBH"
+CPBH_TEMP_DIR="/tmp/ray-cpbh"
+CPBH_GCS_PORT=6380
+CPBH_CLIENT_PORT=6381
+CPBH_DASHBOARD_PORT=8267
+CPBH_AGENT_PORT=52367
+CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"
+CPBH_DASHBOARD="http://127.0.0.1:${CPBH_DASHBOARD_PORT}"
 RAY_BIN="./venv/bin/ray"
 
 # ---- the shared lock (rehearsal-lock-brief.md, section 2) ----
@@ -93,8 +100,8 @@
         echo "purpose: $1"
         echo "acquired: $(iso_now)"
         echo "host: $(hostname -s)"
-        echo "ray_temp_dir: $SI_TEMP_DIR"
-        echo "ray_address: $SI_ADDRESS"
+        echo "ray_temp_dir: $CPBH_TEMP_DIR"
+        echo "ray_address: $CPBH_ADDRESS"
         echo "pid: $2"
     } > "$LOCK_DIR/owner.tmp" && mv "$LOCK_DIR/owner.tmp" "$OWNER_FILE"
 }
@@ -121,7 +128,7 @@
 }
 
 release_lock() {
-    # Release only if the owner's project and ray_temp_dir are SI's (brief section 2).
+    # Release only if the owner's project and ray_temp_dir are ChamPBH's (brief section 2).
     if [ ! -d "$LOCK_DIR" ]; then
         say "no lock to release"
         return 0
@@ -129,8 +136,8 @@
     local proj tdir
     proj="$(owner_field project)"
     tdir="$(owner_field ray_temp_dir)"
-    if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$SI_TEMP_DIR" ]; then
-        err "the lock is not SI's; not releasing it. owner has project='$proj' ray_temp_dir='$tdir'; SI is project='$LOCK_PROJECT' ray_temp_dir='$SI_TEMP_DIR'"
+    if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$CPBH_TEMP_DIR" ]; then
+        err "the lock is not ChamPBH's; not releasing it. owner has project='$proj' ray_temp_dir='$tdir'; ChamPBH is project='$LOCK_PROJECT' ray_temp_dir='$CPBH_TEMP_DIR'"
         return 1
     fi
     rm -f "$OWNER_FILE" "$LOCK_DIR/owner.tmp"
@@ -143,7 +150,7 @@
 
 temp_dir_regex() {
     # temp_dir_regex DIR: an ERE matching DIR in its /tmp and /private/tmp spellings, as a whole
-    # path component (so /tmp/ray-si does not match /tmp/ray-simple).
+    # path component (so /tmp/ray-cpbh does not match /tmp/ray-cpbhx).
     local d="${1#/private}"
     d="$(printf '%s' "$d" | sed 's/[.]/[.]/g')"
     printf '(/private)?%s([/ =]|$)' "$d"
@@ -192,8 +199,9 @@
 alive() { kill -0 "$1" 2>/dev/null; }
 
 stop_cluster() {
-    # stop_cluster DIR GCS_PORT. Returns 0 when nothing is left and the port is free.
-    local dir="$1" port="$2" pids p remaining left i n
+    # stop_cluster DIR GCS_PORT CLIENT_PORT DASHBOARD_PORT AGENT_PORT. Returns 0 when nothing is
+    # left and all four ports are free. (Prompt 05a, change 3: SI's checks the GCS port alone.)
+    local dir="$1" port="$2" ports="$2 $3 $4 $5" pids p remaining left i n q held
     pids="$(list_cluster_pids "$dir" "$port" | tr '\n' ' ')"
     pids="${pids% }"
     if [ -z "$pids" ]; then
@@ -234,12 +242,16 @@
         err "processes still alive after SIGKILL:$left"
         return 1
     fi
-    if [ -n "$(port_listeners "$port")" ]; then
-        err "port $port is still held:"
-        port_listeners "$port" >&2
-        return 1
-    fi
-    say "no process names $dir; port $port is free"
+    held=""
+    for q in $ports; do
+        if [ -n "$(port_listeners "$q")" ]; then
+            err "port $q is still held:"
+            port_listeners "$q" >&2
+            held="$held $q"
+        fi
+    done
+    [ -z "$held" ] || return 1
+    say "no process names $dir; ports $ports are free"
     return 0
 }
 
@@ -247,7 +259,7 @@
 
 cmd_start() {
     if [ $# -lt 1 ]; then usage >&2; return 2; fi
-    local purpose="$1" ncpus=4
+    local purpose="$1" ncpus=10
     shift
     while [ $# -gt 0 ]; do
         case "$1" in
@@ -260,45 +272,60 @@
 
     acquire_lock "$purpose" || return $?
 
-    # The lock is free but SI's temp dir is named by a live process: a stale cluster. Not ours to
+    # The lock is free but ChamPBH's temp dir is named by a live process: a stale cluster. Not ours to
     # guess about; release the lock and say so.
-    if [ -n "$(list_cluster_pids "$SI_TEMP_DIR" "$SI_GCS_PORT")" ]; then
-        err "processes naming $SI_TEMP_DIR are already running (a stale cluster?); not starting. Run 'stop' first."
+    if [ -n "$(list_cluster_pids "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT")" ]; then
+        err "processes naming $CPBH_TEMP_DIR are already running (a stale cluster?); not starting. Run 'stop' first."
         release_lock
         return 4
     fi
 
-    trap 'err "interrupted; stopping what was started"; stop_cluster "$SI_TEMP_DIR" "$SI_GCS_PORT"; release_lock; exit 130' INT TERM
+    # Prompt 05a, change 4: a held port is not ours to take. Without this, a ray start that fails
+    # on a foreign listener leaves a stop whose port check cannot pass, and the lock held.
+    local q held=""
+    for q in "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; do
+        if [ -n "$(port_listeners "$q")" ]; then
+            err "port $q is already held; not starting:"
+            port_listeners "$q" >&2
+            held="$held $q"
+        fi
+    done
+    if [ -n "$held" ]; then
+        release_lock
+        return 4
+    fi
 
+    trap 'err "interrupted; stopping what was started"; stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; release_lock; exit 130' INT TERM
+
     local t0=$SECONDS
-    "$RAY_BIN" start --head --temp-dir "$SI_TEMP_DIR" --port "$SI_GCS_PORT" \
-        --ray-client-server-port "$SI_CLIENT_PORT" --dashboard-port "$SI_DASHBOARD_PORT" \
-        --dashboard-agent-listen-port "$SI_AGENT_PORT" --num-cpus "$ncpus" --disable-usage-stats
+    "$RAY_BIN" start --head --temp-dir "$CPBH_TEMP_DIR" --port "$CPBH_GCS_PORT" \
+        --ray-client-server-port "$CPBH_CLIENT_PORT" --dashboard-port "$CPBH_DASHBOARD_PORT" \
+        --dashboard-agent-listen-port "$CPBH_AGENT_PORT" --num-cpus "$ncpus" --disable-usage-stats
     local rc=$?
     if [ "$rc" -ne 0 ]; then
         err "ray start failed (exit $rc); stopping what it started and releasing the lock"
-        stop_cluster "$SI_TEMP_DIR" "$SI_GCS_PORT"
+        stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"
         release_lock
         trap - INT TERM
         return 1
     fi
 
     local gcs
-    gcs="$(list_cluster_pids "$SI_TEMP_DIR" "$SI_GCS_PORT" | while read -r p; do
+    gcs="$(list_cluster_pids "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" | while read -r p; do
         describe_pid "$p" | grep -q 'gcs_server' && echo "$p"
     done | head -n 1)"
     if [ -z "$gcs" ]; then
-        err "ray start succeeded but no gcs_server names $SI_TEMP_DIR; stopping"
-        stop_cluster "$SI_TEMP_DIR" "$SI_GCS_PORT"
+        err "ray start succeeded but no gcs_server names $CPBH_TEMP_DIR; stopping"
+        stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"
         release_lock
         trap - INT TERM
         return 1
     fi
     write_owner "$purpose" "$gcs"
     trap - INT TERM
-    say "address $SI_ADDRESS"
-    say "dashboard $SI_DASHBOARD"
-    say "actors: $RAY_BIN list actors --address $SI_ADDRESS"
+    say "address $CPBH_ADDRESS"
+    say "dashboard $CPBH_DASHBOARD"
+    say "actors: $RAY_BIN list actors --address $CPBH_ADDRESS"
     say "started in $((SECONDS - t0))s; gcs_server pid $gcs"
     return 0
 }
@@ -309,13 +336,13 @@
         local proj tdir
         proj="$(owner_field project)"
         tdir="$(owner_field ray_temp_dir)"
-        if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$SI_TEMP_DIR" ]; then
+        if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$CPBH_TEMP_DIR" ]; then
             err "the rehearsal lock is another project's (project='$proj' ray_temp_dir='$tdir'); touching nothing. Its owner file:"
             print_owner >&2
             return 1
         fi
     fi
-    stop_cluster "$SI_TEMP_DIR" "$SI_GCS_PORT" || return 1
+    stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT" || return 1
     release_lock || return 1
     say "stopped in $((SECONDS - t0))s"
     return 0
@@ -335,15 +362,18 @@
         echo "lock: free ($LOCK_DIR does not exist)"
     fi
     local pids
-    pids="$(list_cluster_pids "$SI_TEMP_DIR" "$SI_GCS_PORT")"
+    pids="$(list_cluster_pids "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT")"
     if [ -z "$pids" ]; then
-        echo "SI cluster ($SI_TEMP_DIR): no processes"
+        echo "ChamPBH cluster ($CPBH_TEMP_DIR): no processes"
     else
-        echo "SI cluster ($SI_TEMP_DIR): $(echo "$pids" | wc -l | tr -d ' ') process(es)"
+        echo "ChamPBH cluster ($CPBH_TEMP_DIR): $(echo "$pids" | wc -l | tr -d ' ') process(es)"
         local p
         for p in $pids; do describe_pid "$p" | sed 's/^/    /'; done
     fi
-    if [ -n "$(port_listeners "$SI_GCS_PORT")" ]; then echo "port $SI_GCS_PORT: held"; else echo "port $SI_GCS_PORT: free"; fi
+    local q
+    for q in "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; do
+        if [ -n "$(port_listeners "$q")" ]; then echo "port $q: held"; else echo "port $q: free"; fi
+    done
     return 0
 }
 
```

## 3. Deviations from the prompt

None of substance. Two notes, neither a departure from the prompt's text:
- **IMPLEMENTATION CHOICE.** The prompt says `stop_cluster` takes "the temp dir and the four
  ports". It is `stop_cluster DIR GCS CLIENT DASHBOARD AGENT`, and the GCS port is also the one
  `list_cluster_pids` needs for the client-server seed.
- **IMPLEMENTATION CHOICE.** The held-port check in `start` lists every held port (not the first)
  and prints each listener, so the refusal shows what holds what.
- **Scratch-only, not a deviation of the script.** My first "unbroken" listing in B1 sourced a copy
  named `pristine.sh`, which is not exempt by the script's `ray_cluster[.]sh` rule, and so counted
  my own `bash -c` subshells (4 pids). Re-run from a copy named `ray_cluster.sh`: 0. The rule is
  SI's and is unchanged; the point is that a sourcing shell must run a file with that name.
- **UNINTENDED DRIFT: none.**

**An observation about the tree, not the work.** At dispatch `git status` showed
`A prompts/jordan-normalization/source/phiref_iter.py` (staged). When I came to commit (about
12:07) it was no longer staged: `prompts/jordan-normalization/` appeared as untracked, and
`git diff --cached --name-only` before my `git add` was empty. I did not stage, unstage, restore or
touch it; another session or the user changed the index meanwhile. The commit is by pathspec in
any case.

## 4. Verification performed

Load at the start: `uptime` `66.76 85.01 65.71` (11:49), falling to `20.94 37.74 50.54` at 11:59 and
about 25 at 12:06; 10 CPUs; host `mps026088`. The cause was macOS's Time Machine and Spotlight, as
the orchestrator measured, not another Ray project: no Ray process was running at any gate.

### 4.1 The suites (before step 0, no cluster up; nothing changed in Python)

| Suite | Tests | Result | Time |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | OK | 222 s |
| `ComputeTargets/tests`, `-p 'test_[a-f]*.py'` | 61 | OK | 135 s |
| `ComputeTargets/tests`, `-p 'test_[g-z]*.py'` | 53 | OK | 136 s |
| `Datastore/tests` | 73 | OK | 4.8 s |

18 / 114 / 73, the same as 05's. The CosmologyModels and ComputeTargets times are long because of the load.

### 4.2 Usage and syntax, both shells

`/bin/bash` is 3.2.57, `bash` on `PATH` is 5.3.20 (`/opt/local/bin/bash`).
- `bash -n ray_cluster.sh`: passes under both.
- `ray_cluster.sh --help`: exit 0 under both; with no argument: usage printed, exit 2 under both.
  The `--help` outputs are byte-identical across the shells, and the no-argument outputs too.
- Executed as `./…/ray_cluster.sh` (shebang `#!/usr/bin/env bash`), it runs under 5.3.

### 4.3 The proof, run under `/bin/bash` 3.2, from the repository root

Scratch driver `worker.py` (`ray.init(address=…, namespace="cpbh05a")`, four tasks, one detached
named actor, then exits), run with `./venv/bin/python -I`; it and every captured output are in the
session scratchpad, not in the repository. Each `stop` was a Bash call of its own, and no Bash
call ran in parallel with a cluster up.

**Step 0, the machine.** `ps -axww` filtered for `gcs_server|raylet|ray::|ray[.]util[.]client|dashboard|log_monitor|ray-cpbh`: no process. Ports 6380, 6381, 8267, 52367, 6379, 10001, 8265, 52365: none held. `~/ScienceData` held only `ChamPBH-rehearsals`, no lock. `/tmp/ray` existed (mtime 10:56); `/tmp/ray/ray_current_cluster` did not. `/tmp/ray-cpbh` and `/tmp/ray-cpbh-decoy` did not exist.

**Step 1, `status`.**
```
lock: free (/Users/ds283/ScienceData/.rehearsal-lock does not exist)
ChamPBH cluster (/tmp/ray-cpbh): no processes
port 6380: free
port 6381: free
port 8267: free
port 52367: free
```

**Step 2, `start "dk-and-rpk-migration 05a (dry run)"`**, exit 0, 13 s (12.9 s wall, under load ~66):
```
>> ray_cluster: address 127.0.0.1:6380
>> ray_cluster: dashboard http://127.0.0.1:8267
>> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380
>> ray_cluster: started in 13s; gcs_server pid 84950
```
Owner file, the brief's seven keys in order:
```
project: ChamPBH
purpose: dk-and-rpk-migration 05a (dry run)
acquired: 2026-10-12T12:00:07+01:00
host: mps026088
ray_temp_dir: /tmp/ray-cpbh
ray_address: 127.0.0.1:6380
pid: 84950
```
(Later starts, with the load falling: 6 s, 3 s, 4 s.) `status` then: lock HELD, `owner pid 84950: alive`, 18 processes, all four ports held.

**Step 3, a second `start`:** exit 3, the owner file printed (as above), "stop and ask the user; this
script never waits on, or breaks, the lock". The list of Ray pids before and after: identical, 17
pids by the filter (`cmp` empty); the owner file unchanged.

**Step 4, the decoy:** `./venv/bin/ray start --head --temp-dir /tmp/ray-cpbh-decoy --num-cpus 1
--disable-usage-stats`, exit 0, 3.9 s, on 6379 / 10001 / 8265 / 52365. By `list_cluster_pids` (the
script sourced), 18 pids each, `comm -12` of the two: empty (disjoint):
- ChamPBH `/tmp/ray-cpbh` 6380: 84950 84952 84953 84954 84966–84975 85014 85015 85032 85033;
- decoy `/tmp/ray-cpbh-decoy` 6379: 85625–85638 85651–85654.

**Step 5, workers.** `127.0.0.1:6380`: tasks `[0, 1, 4, 9]`, actor `cpbh_actor`, worker pid 86031.
`127.0.0.1:6379`: the same, actor `decoy_actor`, pid 86072. Then, with the decoy up:
```
$ ./venv/bin/ray list actors --address 127.0.0.1:6380      Total: 1   cpbh_actor   ALIVE   pid 86031   namespace cpbh05a
$ ./venv/bin/ray list actors --address 127.0.0.1:6379      Total: 1   decoy_actor  ALIVE   pid 86072   namespace cpbh05a
$ ./venv/bin/ray list actors                               ConnectionError: Found multiple active Ray instances: {'127.0.0.1:6380', '127.0.0.1:6379'}. Please specify the one to connect to by setting the `--address` flag or `RAY_ADDRESS` environment variable.
```
Process sets with the workers: ChamPBH 24 (the 18, plus six workers under the raylet: five `ray::IDLE` and `ray::Pinger` 86031, pids 86027 86029–86033); decoy 19 (the 18 plus `ray::Pinger` 86072). Disjoint again; each actor's pid is in its own cluster's set only.

**Step 6, `stop`:** exit 0, 19.7 s wall ("stopped in 19s"):
```
>> ray_cluster: SIGTERM to 24 process(es) naming /tmp/ray-cpbh, and their descendants: 84950 84952 …
>> ray_cluster: grace period of 15s over; SIGKILL to: 84950 84952
>> ray_cluster: no process names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free
>> ray_cluster: released the rehearsal lock
>> ray_cluster: stopped in 19s
```
All 24 pids dead (checked with `kill -0`), the actor's worker 86031 among them. The lock directory is gone. Ports 6380 / 6381 / 8267 / 52367 free (`lsof`). **The decoy's set is unchanged pid for pid** (`cmp` of the 19-line lists: identical), its four ports still held, and its actor still answers: `actor decoy_actor answers, pid: 86072`. `ray_current_cluster` of `/tmp/ray-cpbh` was kept by the stop (stale), as SI found.

**Step 7, the decoy stopped the same way:** `source ray_cluster.sh; stop_cluster /tmp/ray-cpbh-decoy 6379 10001 8265 52365`, exit 0, 18 s: SIGTERM to its processes, "grace period of 15s over; SIGKILL to: 85625 85626" (gcs_server and `monitor.py`), "no process names /tmp/ray-cpbh-decoy; ports 6379 10001 8265 52365 are free"; `release_lock` then said "no lock to release". Afterwards: no Ray process, all eight ports free, the lock absent.

**Step 8, a lock held by another project.** `mkdir ~/ScienceData/.rehearsal-lock` (no `-p`) succeeded; stand-in owner file: `project: StochasticInstantons`, `purpose: ChamPBH 05a stand-in`, `ray_temp_dir: /tmp/ray-si`, `ray_address: 127.0.0.1:6390`, `pid: -`, with `acquired` and `host` as the brief gives them.
- `start x`: exit 3, the stand-in's owner file printed, nothing started;
- `stop`: exit 1, "the rehearsal lock is another project's … touching nothing", the owner file printed; no process signalled (none existed); the lock left;
- `status`: lock HELD, the stand-in's owner file, "owner pid: - (no cluster pid recorded yet)", no ChamPBH processes, four ports free.
Removed by hand after `grep -x 'purpose: ChamPBH 05a stand-in'` matched.

**Step 9, `status`:** lock free, no ChamPBH process, ports 6380 / 6381 / 8267 / 52367 free.

**Bash 5.3, steps 1–3 and stop.** `/opt/local/bin/bash ray_cluster.sh status` (free), `./ray_cluster.sh start "dk-and-rpk-migration 05a (bash 5.3)"` via the shebang (exit 0, 4 s, gcs_server 90802), a second `start` under 5.3 (exit 3), then `/opt/local/bin/bash ray_cluster.sh stop` (exit 0, 17 s; SIGKILL to 90802 90803; ports free; lock released).

**Change 4, which no breakage shows.** With `nc -l 127.0.0.1 6381` holding the client port and no cluster:
```
>> ray_cluster: port 6381 is already held; not starting:
nc      89723 ds283    3u  IPv4 …  TCP 127.0.0.1:6381 (LISTEN)
>> ray_cluster: released the rehearsal lock
```
exit 4, no `gcs_server` started, the lock gone. The `nc` was killed afterwards.

**After everything:** no Ray process; the eight ports free; `~/ScienceData` holds only `ChamPBH-rehearsals`; `/tmp/ray/ray_current_cluster` still absent; the venv untouched.

### 4.4 C2 — Ray 2.53.0's behaviour, measured

1. **The client server's command line**, in full (from `ps`, on the real cluster):
   `/opt/local/Library/Frameworks/Python.framework/Versions/3.13/Resources/Python.app/Contents/MacOS/Python -m ray.util.client.server --address=127.0.0.1:6380 --host=127.0.0.1 --port=6381 --mode=proxy --runtime-env-agent-address=http://127.0.0.1:59875`.
   Its parent is pid 1 (`launchd`) and it names no temp dir. The script's seed
   `ray[.]util[.]client[.]server --address=127[.]0[.]0[.]1:6380( |$)` matches it (pid 84953 is in the set). The decoy's is the same with `:6379` and `--port=10001`.
2. **`ray_current_cluster`.** Written to `/tmp/ray-cpbh/ray_current_cluster` (14 bytes, `127.0.0.1:6380`, 12:00) and to `/tmp/ray-cpbh-decoy/ray_current_cluster` (`127.0.0.1:6379`). `/tmp/ray/ray_current_cluster` **did not exist before step 2, and does not exist after step 6, after step 7, or at the end**; `/tmp/ray` itself kept its mtime (10:56) until the last check. The file survives the stop.
3. **`./venv/bin/ray status --address 127.0.0.1:6380`:** one active node, `0.0/10.0 CPU`, `8.33GiB` memory, `2.00GiB` object store, no pending nodes or failures.
4. **The actor command.** `./venv/bin/ray list actors --address 127.0.0.1:6380` reaches ChamPBH's cluster alone (`cpbh_actor`, not `decoy_actor`) with the decoy up; `--address 127.0.0.1:6379` the reverse. The bare `ray list actors` with both up fails ("Found multiple active Ray instances"), as SI found. With the cluster up and no actor, the command prints `No resource in the cluster`.
5. **`gcs_server` and `SIGTERM`.** `gcs_server` ignores it: every stop spent the full 15 s grace and `SIGKILL`ed the `gcs_server` (84950, 89196, 89905, 90802; 85625 for the decoy), and in the first stop and the decoy's also `monitor.py` (84952, 85626). The rest of the set ended on `SIGTERM`. A stop takes 17-20 s.

## 5. The deliberate-breakage record

Each applied to the script in place, shown caught, reverted (`cmp` against the saved copy
confirmed) and never committed. The diffs are against the script at `babd9d5`, as applied, and
replay by `git apply` (checked on a scratch repository holding the script: each `git apply
--check`, `git apply`, `git apply -R --check`, `git apply -R`, and a clean `git diff`).

### B1 — the whole-component guard removed from the temp-dir pattern

```diff
--- a/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
+++ b/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
@@ -153,7 +153,7 @@
     # path component (so /tmp/ray-cpbh does not match /tmp/ray-cpbhx).
     local d="${1#/private}"
     d="$(printf '%s' "$d" | sed 's/[.]/[.]/g')"
-    printf '(/private)?%s([/ =]|$)' "$d"
+    printf '(/private)?%s' "$d"
 }
 
 list_cluster_pids() {
```
With only the decoy up: `list_cluster_pids /tmp/ray-cpbh 6380` on the **unbroken** script gives 0
pids; on the broken script it gives 18, all of them the decoy's (it misses only the decoy's client
server, 88161, which only the port-6379 seed finds). Shown by listing; nothing signalled. The
decoy was then stopped by the unbroken function.

### B2 — the lock's `mkdir` given `-p`

```diff
--- a/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
+++ b/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
@@ -117,7 +117,7 @@
 acquire_lock() {
     # acquire_lock PURPOSE. mkdir WITHOUT -p: the atomic step. Returns 3 when the lock is held.
     mkdir -p "$LOCK_PARENT" || return 1
-    if mkdir "$LOCK_DIR" 2>/dev/null; then
+    if mkdir -p "$LOCK_DIR" 2>/dev/null; then
         write_owner "$1" "-"
         return 0
     fi
```
With a real cluster up under the lock: a second `start` on the broken script was **not** refused by
the lock. `acquire_lock` succeeded and rewrote the live cluster's owner file (`pid: -`). What caught
it was the stale-process guard: "processes naming /tmp/ray-cpbh are already running (a stale
cluster?); not starting", exit 4. But the guard then calls `release_lock`, and the owner was
ChamPBH's, so **the live cluster's lock was removed**: after B2 `~/ScienceData/.rehearsal-lock` did
not exist and the cluster ran unlocked. (Change 4's held-port check would have caught it second.)
The unbroken `stop` then stopped the cluster (18 s, SIGKILL to the gcs_server 89196) and said
"no lock to release", as expected; the lock was free afterwards. So `mkdir`'s atomicity is the
only thing that keeps a second starter from overwriting the owner file and then removing the lock.

### B3 — the owner checks removed (in `stop` and in the release, both)

```diff
--- a/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
+++ b/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
@@ -136,10 +136,6 @@
     local proj tdir
     proj="$(owner_field project)"
     tdir="$(owner_field ray_temp_dir)"
-    if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$CPBH_TEMP_DIR" ]; then
-        err "the lock is not ChamPBH's; not releasing it. owner has project='$proj' ray_temp_dir='$tdir'; ChamPBH is project='$LOCK_PROJECT' ray_temp_dir='$CPBH_TEMP_DIR'"
-        return 1
-    fi
     rm -f "$OWNER_FILE" "$LOCK_DIR/owner.tmp"
     rmdir "$LOCK_DIR" || { err "could not rmdir $LOCK_DIR"; return 1; }
     say "released the rehearsal lock"
@@ -332,16 +328,6 @@
 
 cmd_stop() {
     local t0=$SECONDS
-    if [ -d "$LOCK_DIR" ]; then
-        local proj tdir
-        proj="$(owner_field project)"
-        tdir="$(owner_field ray_temp_dir)"
-        if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$CPBH_TEMP_DIR" ]; then
-            err "the rehearsal lock is another project's (project='$proj' ray_temp_dir='$tdir'); touching nothing. Its owner file:"
-            print_owner >&2
-            return 1
-        fi
-    fi
     stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT" || return 1
     release_lock || return 1
     say "stopped in $((SECONDS - t0))s"
```
With the stand-in lock and nothing of ChamPBH's running, `stop` on the broken script exited 0 and
**released the stand-in** ("released the rehearsal lock"): the directory was gone. Caught. (Either
check alone would have left the other to refuse; SI found the same.) The stand-in was re-created by
hand, the unbroken `stop` was shown to refuse again (exit 1, lock kept), and the stand-in removed
after checking its `purpose`.

### B4 — the client-server seed removed

```diff
--- a/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
+++ b/.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
@@ -174,7 +174,7 @@
             for (p in all) {
                 if (p in safe) continue
                 if (cmd[p] ~ /ray_cluster[.]sh/) continue
-                if (cmd[p] ~ re || cmd[p] ~ re2) mark[p]=1
+                if (cmd[p] ~ re) mark[p]=1
             }
             do {
                 changed=0
```
On a fresh `start` (gcs_server 89905), the broken `stop` signalled 17 processes (the client server
not among them), `SIGKILL`led 89905 and 89906, and then failed: "port 6381 is still held:
`Python 89907 … TCP 127.0.0.1:6381 (LISTEN)`", exit 1, **the lock kept** (`owner` still there). This
is SI's D1, and what change 3 is for: SI's GCS-only check would have passed. The unbroken `stop`
then found and stopped 89907 (1 s), confirmed all four ports free and released the lock.

## 6. Observations not acted on

1. **SI's script checks only its GCS port when it stops.** ChamPBH's checks four (change 3); the
   difference is SI's to take up (README §6.6).
2. **A stop kills any process whose command line names `/tmp/ray-cpbh`** (including a shell or a
   `tail -f`), as the brief says; the caller must not leave one. 06 §1.2.3 already says so.
3. **B2 shows `release_lock` after a refused start removes a lock the refused starter did not
   take**, when the lock was acquired by overwriting. It is only reachable if `mkdir` is not
   atomic, which it is; noted because the guard order (stale-process guard releasing the lock)
   is SI's and shared.
4. The orchestrator's note and the prompt say the user's `phiref_iter.py` is staged. It was not,
   by the time of the commit (§3). No action.

## 7. State handed to the next prompt

- **`ray_cluster.sh` exists** at `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`
  (mode `100755`). Prompt 06 calls it from the repository root:
  `ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"` (exit 3 if the lock is held: stop and
  ask; exit 4 if a ChamPBH process or one of the four ports is already there: stop and ask),
  then `stop` as a command on its own, then `status` in another.
- **Its output for 06 to read**: `>> ray_cluster: address 127.0.0.1:6380`,
  `>> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380`,
  `>> ray_cluster: started in Ns; gcs_server pid P`. Pass `127.0.0.1:6380` to every driver as
  `--ray-address` and to every `ray` CLI call as `--address`. Confirm with `./venv/bin/ray status
  --address 127.0.0.1:6380`: one node, **10 CPUs**.
- **Timings, for 06's expectations:** a start takes 3-13 s (13 s at a load of 66, 3-6 s at a load
  of 25); a stop takes 17-20 s, because `gcs_server` ignores `SIGTERM` and the 15 s grace always
  runs. `RAY_CLUSTER_GRACE` changes the grace.
- **Where things went:** `ray_current_cluster` is in `/tmp/ray-cpbh/`, not `/tmp/ray/`; it survives
  the stop. Nothing of ChamPBH's touches `/tmp/ray`.
- **The brief's contract needed no change** for Ray 2.53.0: the client server's command line, the
  temp dir's `ray_current_cluster`, the `--address` form of the actor command and the `gcs_server`
  ignoring `SIGTERM` all agree with SI's measurements on 2.55.1. The differences from SI's script
  are changes 1–4 only; none is `STRUCTURALLY REQUIRED`. (Change 3 and change 4 are ChamPBH's
  additions that SI's script does not have; the brief asks for the four-port check in §3.)
- **State at the end:** no Ray process; the eight ports free; no lock; no file under
  `~/ScienceData` but `ChamPBH-rehearsals`; the venv, the stores and the siblings untouched; the
  suites 18 / 114 / 73.
- **A caution for anyone sourcing the script:** `list_cluster_pids` exempts any process whose
  command line names `ray_cluster.sh`; source the file under that name or the sourcing shell's own
  subshells will be counted as the cluster's.
