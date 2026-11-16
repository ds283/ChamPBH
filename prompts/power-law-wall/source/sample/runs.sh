#!/bin/bash
# power-law-wall prompt 04 §1.1 (U7; P9, P24): the forty histories, by the tool route through the runner
# (sample_run.py: tools/history_and_bbn.py's own main, in-process, then the instrumentation of prompt §1.3), tuned with
# the tool's defaults (phi* = 5 M_P, tolerance 1e-5, at most 30 integrations, the small network), ONE AT A TIME
# (orchestrator note 04, ruling 6). No Ray, no store. Run from the repository root:
#
#     bash prompts/power-law-wall/source/sample/runs.sh [NAME ...]
#
# With names, only those histories run (a history is named as its file is, without .txt). Each run writes
# .documents/power-law-wall/sample-04/runs/<name>.txt: a header with the command, the start time and `uptime`, the
# runner's stdout and stderr (the tool's lines, then the `sample` and `cand` lines), and a footer with the exit status,
# the wall time and `uptime`, as 01's runs.sh and 03's tool_route_*.txt do. This script's own output, one line per
# history at its start and end, is appended to runs/runs.log, the record of how the forty ran.
#
# Names: ip-n<N>-M<M>-b<beta>-<law>, <law> fixed<alpha>, oneloop<A>, or natural.
set -u
cd "$(dirname "$0")/../../../.." || exit 1
OUT=.documents/power-law-wall/sample-04/runs
LOG="$OUT/runs.log"
mkdir -p "$OUT"

one() {
    name=$1
    shift
    file="$OUT/$name.txt"
    {
        echo "# command: PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/sample/sample_run.py $*"
        echo "# start: $(date '+%F %T') $(uptime)"
    } >"$file"
    echo "start $name: $(date '+%F %T') $(uptime)" | tee -a "$LOG"
    t0=$(date +%s)
    PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/sample/sample_run.py "$@" >>"$file" 2>&1
    rc=$?
    t1=$(date +%s)
    echo "# end: $(date '+%F %T') exit=$rc wall=$((t1 - t0)) s $(uptime)" >>"$file"
    echo "end   $name: exit=$rc wall=$((t1 - t0)) s $(date '+%F %T') $(uptime)" | tee -a "$LOG"
}

# the forty, in prompt §1.1's order: per n, laboratory M (beta x laws), 1e-33, 1e-15, then 1e-3 in natural mode
list() {
    for n in 1 2; do
        for beta in 1.2 2.0 2.9; do
            for a in 0.01 0.1; do
                echo "ip-n$n-M4e-31-b$beta-fixed$a $beta 4e-31 --alpha $a --model inverse-power --n $n"
            done
            for A in 0.05 1 10; do
                echo "ip-n$n-M4e-31-b$beta-oneloop$A $beta 4e-31 --drain-law one_loop --alpha $A --model inverse-power --n $n"
            done
        done
        echo "ip-n$n-M1e-33-b2.0-oneloop1 2.0 1e-33 --drain-law one_loop --alpha 1 --model inverse-power --n $n"
        echo "ip-n$n-M1e-15-b2.0-oneloop1 2.0 1e-15 --drain-law one_loop --alpha 1 --model inverse-power --n $n"
        for beta in 1.2 2.0 2.9; do
            echo "ip-n$n-M1e-3-b$beta-natural $beta 1e-3 --model inverse-power --n $n"
        done
    done
}

echo "runs.sh: begin $(date '+%F %T') HEAD $(git rev-parse HEAD) $(uptime)" | tee -a "$LOG"
list | while read -r name args; do
    if [ $# -gt 0 ]; then
        case " $* " in
        *" $name "*) ;;
        *) continue ;;
        esac
    fi
    # shellcheck disable=SC2086
    one "$name" $args </dev/null
done
echo "runs.sh: end $(date '+%F %T') $(uptime)" | tee -a "$LOG"
