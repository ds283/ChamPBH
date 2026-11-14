#!/bin/bash
# power-law-wall prompt 01 §1.5: the twelve inverse-power-law runs and their exponential counterparts, by
# the tool route (tools/history_and_bbn.py, tuned, no Ray, no store), three processes at a time. Run from
# the repository root:
#
#     bash .documents/power-law-wall/model-01/runs.sh
#
# Each run writes .documents/power-law-wall/model-01/out/<name>.txt: a header with the command, the start
# time and `uptime`, the tool's stdout and stderr, and a footer with the exit status, the wall time and
# `uptime`. The exponential counterpart of each run is the same command without --model and --n
# (orchestrator note 01, ruling 4), so the exponential at n = 1 on this tree.
set -u
cd "$(dirname "$0")/../../.." || exit 1
OUT=.documents/power-law-wall/model-01/out
mkdir -p "$OUT"

one() {
    name=$1; shift
    file="$OUT/$name.txt"
    {
        echo "# command: PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py $*"
        echo "# start: $(date '+%F %T') $(uptime)"
    } > "$file"
    t0=$(date +%s)
    PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py "$@" >> "$file" 2>&1
    rc=$?
    t1=$(date +%s)
    echo "# end: $(date '+%F %T') exit=$rc wall=$((t1 - t0)) s $(uptime)" >> "$file"
}
export -f one
export OUT

{
    for beta in 1.2 2.0 2.9; do
        for n in 1 2; do
            echo "ip-n$n-M4e-31-beta$beta-fixed0.1 $beta 4e-31 --alpha 0.1 --model inverse-power --n $n"
            echo "ip-n$n-M1e-3-beta$beta-natural $beta 1e-3 --model inverse-power --n $n"
        done
        echo "exp-M4e-31-beta$beta-fixed0.1 $beta 4e-31 --alpha 0.1"
        echo "exp-M1e-3-beta$beta-natural $beta 1e-3"
    done
} | xargs -P 3 -L 1 bash -c 'one "$@"' _
