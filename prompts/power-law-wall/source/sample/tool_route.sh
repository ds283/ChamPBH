#!/bin/bash
# power-law-wall prompt 04 §1.2 (P25): the exponential by the tool route (tools/history_and_bbn.py, plain, tuned with
# its defaults; no Ray, no store) at three of the twenty counterparts, one at a time, so that each can be checked against
# its 2026.11.0 export row (tables.py export-check). Run from the repository root:
#
#     bash prompts/power-law-wall/source/sample/tool_route.sh
#
# Each run writes .documents/power-law-wall/sample-04/tool-route/<name>.txt: the command, the start time and `uptime`,
# the tool's stdout and stderr, and the exit status, the wall time and `uptime`.
set -u
cd "$(dirname "$0")/../../../.." || exit 1
OUT=.documents/power-law-wall/sample-04/tool-route
mkdir -p "$OUT"

one() {
    name=$1
    shift
    file="$OUT/$name.txt"
    {
        echo "# command: PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py $*"
        echo "# start: $(date '+%F %T') $(uptime)"
    } >"$file"
    t0=$(date +%s)
    PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py "$@" >>"$file" 2>&1
    rc=$?
    t1=$(date +%s)
    echo "# end: $(date '+%F %T') exit=$rc wall=$((t1 - t0)) s $(uptime)" >>"$file"
    echo "$name: exit=$rc wall=$((t1 - t0)) s"
}

one exp-M4e-31-b2.0-oneloop1 2.0 4e-31 --drain-law one_loop --alpha 1
one exp-M1e-15-b2.0-oneloop1 2.0 1e-15 --drain-law one_loop --alpha 1
one exp-M1e-3-b2.0-natural 2.0 1e-3
