#!/usr/bin/env bash
# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ChamPBH's rehearsal cluster, and the rehearsal lock it shares with StochasticInstantons (SI) and
# SecondaryGWKit. Written for dk-and-rpk-migration prompt 05a, under U5 (README section 4.1 and
# section 6.6 of this campaign). Adapted from SI's script at StochasticInstantons commit 3556148
# (.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh).
#
# The lock is raypoolkit.rehearsal_lock of the pinned raypoolkit (requirements.txt), whose contract
# is RayPoolKit's docs/rehearsal-lock.md at v0.3.0; this script calls its commands and keeps no
# lock of its own (rpk-v030-adoption prompt 01). The cluster follows section 3 of SI's
# .prompts/dk-and-rpk-migration/rehearsal-lock-brief.md at ed9399a exactly, because that contract
# is shared with the other projects.
#
# This script NEVER runs `ray stop`, which stops every Ray process on the machine. It stops
# ChamPBH's cluster by signalling the processes whose command line names ChamPBH's temp dir, and
# their descendants (Ray's workers carry only their process title, so they are reached through
# their parent). It never waits on a held lock, and never removes a lock that is not ChamPBH's.
#
# It may be sourced (its functions used, its main skipped): the proof of prompt 05a stops a decoy
# cluster by calling stop_cluster with the decoy's temp dir and ports.

set -u

usage() {
    cat <<'EOF2'
usage: ray_cluster.sh start PURPOSE [--num-cpus N]
       ray_cluster.sh stop
       ray_cluster.sh status
       ray_cluster.sh --help

Run from the repository root.

  start PURPOSE   take the rehearsal lock (~/.raypoolkit/rehearsal-lock, through
                  raypoolkit.rehearsal_lock), then start ChamPBH's Ray head on /tmp/ray-cpbh:
                  GCS 6380, client 6381, dashboard 8267, dashboard agent 52367, N CPUs (default
                  10: R0's cluster had 10, Ray's default on this machine, and R1 and R2 are
                  compared with R0, wall-clock limits included). PURPOSE is written to the lock's
                  owner file, e.g. "dk-and-rpk-migration 06 (R1)", and the gcs_server's pid once
                  it is up. Exits 3, starting nothing, if the lock is held or the lock's location
                  from before raypoolkit v0.3.0 still exists, printing the owner file found.
                  Exits 4, starting nothing and releasing the lock again, if a process of the
                  cluster is already alive or any of the four ports is already held. Prints the
                  cluster's address and the command that lists its actors.
  stop            stop ChamPBH's processes (SIGTERM to every process naming /tmp/ray-cpbh, to
                  the Ray client server whose address is 127.0.0.1:6380, and to their
                  descendants; a grace period of $RAY_CLUSTER_GRACE seconds, default 15; then
                  SIGKILL), confirm none is left and all four ports (6380, 6381, 8267, 52367) are
                  free, then release the lock if its owner is ChamPBH's. Touches nothing if the
                  lock is another project's. The lock stays held while anything of the cluster
                  is alive: the gcs_server, or any process raypoolkit.rehearsal_lock lists.
  status          the lock, its owner file, whether the owner's pid is alive, and the cluster's
                  processes, as raypoolkit.rehearsal_lock sees them; then the processes `stop`
                  would signal, by this script's rule; then the four ports.

Never runs `ray stop`. Never waits on a held lock. No shell command that runs this script may
name the temp dir (/tmp/ray-cpbh), in PURPOSE or anywhere else: the lock would then list every
other process that shell started.
EOF2
}

# ---- ChamPBH's cluster (rehearsal-lock-brief.md, section 3; README section 4.1, U5) ----
LOCK_PROJECT="ChamPBH"
CPBH_TEMP_DIR="/tmp/ray-cpbh"
CPBH_GCS_PORT=6380
CPBH_CLIENT_PORT=6381
CPBH_DASHBOARD_PORT=8267
CPBH_AGENT_PORT=52367
CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"
CPBH_DASHBOARD="http://127.0.0.1:${CPBH_DASHBOARD_PORT}"
RAY_BIN="./venv/bin/ray"

# ---- the shared lock: raypoolkit.rehearsal_lock (RayPoolKit's docs/rehearsal-lock.md, v0.3.0) ----
# Every call passes ChamPBH's values from the variables above, never spelled on a command line of
# this script's own (the contract, section 6, "What it asks of a caller"). The lock compares the
# strings as given (section 5), so the same spellings are passed every time.
LOCK_PY="./venv/bin/python"

GRACE="${RAY_CLUSTER_GRACE:-15}"

say() { echo ">> ray_cluster: $*"; }
err() { echo ">> ray_cluster: $*" >&2; }

lock_cmd() {
    # lock_cmd COMMAND [OPTION...]: a command of raypoolkit.rehearsal_lock, its exit code passed on.
    "$LOCK_PY" -m raypoolkit.rehearsal_lock "$@"
}

lock_release() {
    # Refused (exit 1, the lock left held) while anything of the cluster is alive (contract section 7).
    lock_cmd release --project "$LOCK_PROJECT" --ray-temp-dir "$CPBH_TEMP_DIR"
}

owner_field() {
    # owner_field LOCK_DIR KEY: the value of KEY in LOCK_DIR's owner file ("" if absent or no file).
    # The owner file's layout is the contract's, section 4.
    [ -f "$1/owner" ] || return 0
    sed -n "s/^$2: //p" "$1/owner" | head -n 1
}

print_owner() {
    # print_owner LOCK_DIR: its owner file, indented.
    if [ -f "$1/owner" ]; then
        sed 's/^/    /' "$1/owner"
    else
        echo "    (no owner file)"
    fi
}

# ---- the targeted stop (brief section 3) ----

temp_dir_regex() {
    # temp_dir_regex DIR: an ERE matching DIR in its /tmp and /private/tmp spellings, as a whole
    # path component (so /tmp/ray-cpbh does not match /tmp/ray-cpbhx).
    local d="${1#/private}"
    d="$(printf '%s' "$d" | sed 's/[.]/[.]/g')"
    printf '(/private)?%s([/ =]|$)' "$d"
}

list_cluster_pids() {
    # list_cluster_pids DIR GCS_PORT: one pid per line: every process whose command line names DIR,
    # and every descendant of those. Also the cluster's Ray client server, found on a real
    # cluster (prompt 01a's proof): it has no Ray ancestor in the process table (its parent is
    # launchd) and carries no temp dir, only `-m ray.util.client.server --address=127.0.0.1:GCS_PORT`.
    # Never this script's own process or its ancestors, nor any process running this script
    # (whose arguments may name DIR).
    local re re2
    re="$(temp_dir_regex "$1")"
    re2="ray[.]util[.]client[.]server --address=127[.]0[.]0[.]1:$2( |\$)"
    ps -axww -o pid= -o ppid= -o command= | awk -v re="$re" -v re2="$re2" -v self="$$" '
        { pid=$1; ppid=$2; par[pid]=ppid; cmd[pid]=$0; sub(/^[ ]*[0-9]+[ ]+[0-9]+[ ]+/, "", cmd[pid]); all[pid]=1 }
        END {
            # self and its ancestors are never ours to signal
            p=self; while (p in par && p > 1) { safe[p]=1; p=par[p] }
            for (p in all) {
                if (p in safe) continue
                if (cmd[p] ~ /ray_cluster[.]sh/) continue
                if (cmd[p] ~ re || cmd[p] ~ re2) mark[p]=1
            }
            do {
                changed=0
                for (p in all) {
                    if ((p in mark) || (p in safe)) continue
                    if ((par[p] in mark) && !(cmd[p] ~ /ray_cluster[.]sh/)) { mark[p]=1; changed=1 }
                }
            } while (changed)
            for (p in mark) print p
        }' | sort -n
}

describe_pid() {
    ps -ww -o pid= -o ppid= -o command= -p "$1" 2>/dev/null | cut -c1-150
}

port_listeners() {
    # port_listeners PORT: the lsof lines of the listeners on 127.0.0.1/any, empty when free.
    lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | sed 1d
}

alive() { kill -0 "$1" 2>/dev/null; }

stop_cluster() {
    # stop_cluster DIR GCS_PORT CLIENT_PORT DASHBOARD_PORT AGENT_PORT. Returns 0 when nothing is
    # left and all four ports are free. (Prompt 05a, change 3: SI's checks the GCS port alone.)
    # All five arguments are required, as in SI's script at 8e04a7c: without the check, set -u
    # makes a shorter call abort the shell that sourced this script, not return.
    if [ $# -ne 5 ]; then
        err "usage: stop_cluster DIR GCS_PORT CLIENT_PORT DASHBOARD_PORT AGENT_PORT"
        return 2
    fi
    local dir="$1" port="$2" ports="$2 $3 $4 $5" pids p remaining left i n q held
    pids="$(list_cluster_pids "$dir" "$port" | tr '\n' ' ')"
    pids="${pids% }"
    if [ -z "$pids" ]; then
        say "no process names $dir"
    else
        n="$(echo $pids | wc -w | tr -d ' ')"
        say "SIGTERM to $n process(es) naming $dir, and their descendants: $pids"
        for p in $pids; do describe_pid "$p" | sed 's/^/    /'; done
        for p in $pids; do kill -TERM "$p" 2>/dev/null; done
        # The captured set is kept: workers re-parented to launchd once their raylet dies would
        # escape a second descendant search.
        i=0
        while [ "$i" -lt $((GRACE * 2)) ]; do
            remaining=""
            for p in $pids; do alive "$p" && remaining="$remaining $p"; done
            [ -z "$remaining" ] && break
            sleep 0.5
            i=$((i + 1))
        done
        remaining=""
        for p in $pids $(list_cluster_pids "$dir" "$port"); do
            alive "$p" && remaining="$remaining $p"
        done
        remaining="$(echo $remaining | tr ' ' '\n' | sort -nu | tr '\n' ' ')"
        remaining="${remaining% }"
        if [ -n "$remaining" ]; then
            say "grace period of ${GRACE}s over; SIGKILL to: $remaining"
            for p in $remaining; do kill -KILL "$p" 2>/dev/null; done
            sleep 1
        fi
    fi
    # Confirm.
    left=""
    for p in $pids $(list_cluster_pids "$dir" "$port"); do
        alive "$p" && left="$left $p"
    done
    if [ -n "$left" ]; then
        err "processes still alive after SIGKILL:$left"
        return 1
    fi
    held=""
    for q in $ports; do
        if [ -n "$(port_listeners "$q")" ]; then
            err "port $q is still held:"
            port_listeners "$q" >&2
            held="$held $q"
        fi
    done
    [ -z "$held" ] || return 1
    say "no process names $dir; ports $ports are free"
    return 0
}

# ---- commands ----

cmd_start() {
    if [ $# -lt 1 ]; then usage >&2; return 2; fi
    local purpose="$1" ncpus=10
    shift
    while [ $# -gt 0 ]; do
        case "$1" in
            --num-cpus) [ $# -ge 2 ] || { usage >&2; return 2; }; ncpus="$2"; shift 2 ;;
            *) usage >&2; return 2 ;;
        esac
    done
    case "$ncpus" in ''|*[!0-9]*) err "--num-cpus must be a positive integer"; return 2 ;; esac
    [ -x "$RAY_BIN" ] || { err "$RAY_BIN not found: run from the repository root"; return 2; }

    # 0 held; 3 held by anyone, or the old location present (nothing created); 4 a process of the
    # cluster alive, a stale cluster (the lock released again); 1 the owner file unwritable; 2 usage.
    # acquire makes the stale-cluster check itself, by the rule release will apply.
    lock_cmd acquire --project "$LOCK_PROJECT" --purpose "$purpose" \
        --ray-temp-dir "$CPBH_TEMP_DIR" --ray-address "$CPBH_ADDRESS" || return $?

    # Prompt 05a, change 4: a held port is not ours to take. Without this, a ray start that fails
    # on a foreign listener leaves a stop whose port check cannot pass, and the lock held.
    local q held=""
    for q in "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; do
        if [ -n "$(port_listeners "$q")" ]; then
            err "port $q is already held; not starting:"
            port_listeners "$q" >&2
            held="$held $q"
        fi
    done
    if [ -n "$held" ]; then
        lock_release
        return 4
    fi

    trap 'err "interrupted; stopping what was started"; stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; lock_release; exit 130' INT TERM

    local t0=$SECONDS
    "$RAY_BIN" start --head --temp-dir "$CPBH_TEMP_DIR" --port "$CPBH_GCS_PORT" \
        --ray-client-server-port "$CPBH_CLIENT_PORT" --dashboard-port "$CPBH_DASHBOARD_PORT" \
        --dashboard-agent-listen-port "$CPBH_AGENT_PORT" --num-cpus "$ncpus" --disable-usage-stats
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        err "ray start failed (exit $rc); stopping what it started and releasing the lock"
        stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"
        lock_release
        trap - INT TERM
        return 1
    fi

    local gcs
    gcs="$(list_cluster_pids "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" | while read -r p; do
        describe_pid "$p" | grep -q 'gcs_server' && echo "$p"
    done | head -n 1)"
    if [ -z "$gcs" ]; then
        err "ray start succeeded but no gcs_server names $CPBH_TEMP_DIR; stopping"
        stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"
        lock_release
        trap - INT TERM
        return 1
    fi
    if ! lock_cmd set-pid --project "$LOCK_PROJECT" --ray-temp-dir "$CPBH_TEMP_DIR" "$gcs"; then
        err "could not record the gcs_server's pid $gcs in the lock; stopping"
        stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"
        lock_release
        trap - INT TERM
        return 1
    fi
    trap - INT TERM
    say "address $CPBH_ADDRESS"
    say "dashboard $CPBH_DASHBOARD"
    say "actors: $RAY_BIN list actors --address $CPBH_ADDRESS"
    say "started in $((SECONDS - t0))s; gcs_server pid $gcs"
    return 0
}

cmd_stop() {
    local t0=$SECONDS lock_dir
    # Touch nothing if the lock is another project's. The module has no command for "whose lock is
    # it", so its owner file is read where the pinned package says the lock is (contract sections
    # 2-4); a stop that cannot ask touches nothing either.
    if ! lock_dir="$("$LOCK_PY" -c 'from raypoolkit.rehearsal_lock import default_lock_dir; print(default_lock_dir())')" \
        || [ -z "$lock_dir" ]; then
        err "could not ask raypoolkit.rehearsal_lock where the lock is ($LOCK_PY); touching nothing"
        return 1
    fi
    if [ -d "$lock_dir" ]; then
        local proj tdir
        proj="$(owner_field "$lock_dir" project)"
        tdir="$(owner_field "$lock_dir" ray_temp_dir)"
        if [ "$proj" != "$LOCK_PROJECT" ] || [ "$tdir" != "$CPBH_TEMP_DIR" ]; then
            err "the rehearsal lock is another project's (project='$proj' ray_temp_dir='$tdir'); touching nothing. Its owner file:"
            print_owner "$lock_dir" >&2
            return 1
        fi
    fi
    stop_cluster "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT" || return 1
    lock_release || return 1
    say "stopped in $((SECONDS - t0))s"
    return 0
}

cmd_status() {
    # The lock and the cluster's processes as raypoolkit.rehearsal_lock sees them (its layout,
    # contract section 8); then what stop would signal, by this script's rule. Where the two
    # listings differ, stop leaves something that release then refuses on.
    lock_cmd status --ray-temp-dir "$CPBH_TEMP_DIR" --ray-address "$CPBH_ADDRESS"
    local pids
    pids="$(list_cluster_pids "$CPBH_TEMP_DIR" "$CPBH_GCS_PORT")"
    if [ -z "$pids" ]; then
        echo "what stop would signal (this script's rule, $CPBH_TEMP_DIR): no processes"
    else
        echo "what stop would signal (this script's rule, $CPBH_TEMP_DIR): $(echo "$pids" | wc -l | tr -d ' ') process(es)"
        local p
        for p in $pids; do describe_pid "$p" | sed 's/^/    /'; done
    fi
    local q
    for q in "$CPBH_GCS_PORT" "$CPBH_CLIENT_PORT" "$CPBH_DASHBOARD_PORT" "$CPBH_AGENT_PORT"; do
        if [ -n "$(port_listeners "$q")" ]; then echo "port $q: held"; else echo "port $q: free"; fi
    done
    return 0
}

main() {
    case "${1:-}" in
        start|stop|status)
            [ -x "$LOCK_PY" ] || { err "$LOCK_PY not found: run from the repository root"; return 2; } ;;
    esac
    case "${1:-}" in
        start) shift; cmd_start "$@" ;;
        stop) shift; cmd_stop ;;
        status) shift; cmd_status ;;
        -h|--help|help|"") usage; [ -z "${1:-}" ] && return 2; return 0 ;;
        *) usage >&2; return 2 ;;
    esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
    exit $?
fi
