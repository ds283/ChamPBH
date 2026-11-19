#!/bin/bash
# power-law-wall prompt 02 §1.4: the twenty-five mapped histories, by path_map.py, three at a time (orchestrator note
# 02, ruling 5), from the repository root. Each output file holds its own `uptime` at the end; this script prints the
# load at its start and end.
#   prompts/power-law-wall/source/wall/runs_map.sh
set -u
OUT=.documents/power-law-wall/wall-02/map
mkdir -p "$OUT"
echo "start $(date '+%F %T') $(uptime)"
{
  for n in 1 2; do
    for a in 0.01 0.1; do
      for bm in "1.2 4e-31" "2.0 4e-31" "2.9 4e-31" "2.0 1e-33" "2.0 1e-15"; do
        set -- $bm
        echo "$1 $2 inverse-power $n $a ip-n$n-M$2-b$1-a$a"
      done
    done
  done
  for b in 2.0 2.9 3.05; do echo "$b 4e-31 exponential 1 0.1 exp-n1-M4e-31-b$b-a0.1"; done
  for n in 1 2; do echo "3.05 4e-31 inverse-power $n 0.1 ip-n$n-M4e-31-b3.05-a0.1"; done
} | xargs -P 3 -L 1 sh -c 'PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/wall/path_map.py "$0" "$1" --model "$2" --n "$3" --alpha "$4" > '"$OUT"'/"$5".txt 2>&1; echo "$5 exit $?"'
echo "end $(date '+%F %T') $(uptime)"
