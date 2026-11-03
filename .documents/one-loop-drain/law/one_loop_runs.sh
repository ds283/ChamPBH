#!/bin/sh
# one-loop-drain prompt 01 §2.2 and §1.7: the one-loop runs, one process each, one at a time, from the
# repository root; outputs in var/one-loop-drain/01/oneloop/ (JSON and each run's stdout/stderr). Launched
# detached (nohup), its pid in oneloop/runs.pid; its last line in oneloop/runs.log is "** runs done".
#
#     nohup sh .documents/one-loop-drain/law/one_loop_runs.sh [OUT] > OUT/runs.nohup 2>&1 &
#
# OUT defaults to var/one-loop-drain/01/oneloop (the first run, before the user's ruling (b)); the run on the
# final tree is var/one-loop-drain/01/final/oneloop.
set -u
OUT=${1:-var/one-loop-drain/01/oneloop}
mkdir -p "$OUT"
echo $$ > "$OUT/runs.pid"
log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >> "$OUT/runs.log"; }
log "** runs start pid=$$ $(uptime)"
run() {
    # SERIAL A GAP GATE
    name="$1-A$2-$3-$4"
    log "start $name"
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/one_loop_run.py "$1" "$2" "$3" "$4" "$OUT/$name.json" > "$OUT/$name.log" 2>&1
    log "end $name rc=$?"
}
# §2.2.1: the probe's table, the probe's law (gate patched to 1.0) and then the production code
for serial in 2197 2188; do
    for A in 1 0.1; do
        run $serial $A zero probe
        run $serial $A zero production
    done
done
# §2.2.2: both prescriptions
run 2197 1 edge production
# §2.2.3 and §2.2.4: large M
for serial in 3016 3233 3873; do
    run $serial 1 zero probe
    run $serial 1 zero production
done
# §1.7: 3873 observed, under the one-loop law at A = 1 and the fixed law at its stored alpha
log "start 3873-inside-wall-one_loop"
PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/inside_wall_3873.py one_loop 1 "$OUT/3873-inside-wall-one_loop-A1.json" > "$OUT/3873-inside-wall-one_loop-A1.log" 2>&1
log "end 3873-inside-wall-one_loop rc=$?"
log "start 3873-inside-wall-fixed"
PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/inside_wall_3873.py fixed 0.03 "$OUT/3873-inside-wall-fixed-a0.03.json" > "$OUT/3873-inside-wall-fixed-a0.03.log" 2>&1
log "end 3873-inside-wall-fixed rc=$?"
log "** runs done $(uptime)"
