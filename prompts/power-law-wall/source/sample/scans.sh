#!/bin/bash
# power-law-wall prompt 04 §1.5 (U7; P29): one surfing scan by tools/surfing_scan.py, at M = 1e-3 M_P, phi* = 5 M_P and the
# tool's default tuning, as electroweak-eos 02 ran its scans. One process per scan; at most three run at once and none
# while the forty run (orchestrator note 04, ruling 5). Run from the repository root:
#
#     bash prompts/power-law-wall/source/sample/scans.sh NAME [BETA_MIN BETA_MAX]
#
# NAME is c_n1, c_n2 (1.10-3.50 in 0.05), c_n1_ext, c_n2_ext (3.55-5.00 in 0.05, into the same CSV as c_n<N>, only if
# no row of C up to 3.50 captures EW), q_exp (the exponential, 1.100-1.150 in 0.001), f_new_check (the exponential at
# 2.975, 2.976 and 3.003), or q_n1, q_n2, e_n1, e_n2 with the grid's ends as read by edges.py grids from C's CSV (0.001
# steps). The CSV goes to .documents/power-law-wall/sample-04/surfing/<csv>.csv with its .meta.json, and the console to
# <NAME>.console.txt beside it, opening with the command, the start time and `uptime` and ending with the exit status,
# the wall time and `uptime`. A scan cut off is resumed by running it again (surfing_scan.py skips the beta it holds).
set -u
cd "$(dirname "$0")/../../../.." || exit 1
OUT=.documents/power-law-wall/sample-04/surfing
mkdir -p "$OUT"
name=$1
csv=$name
case $name in
c_n1 | c_n2)
    n=${name#c_n}
    args=(--model inverse-power --n "$n" --beta-min 1.10 --beta-max 3.50 --beta-step 0.05)
    ;;
c_n1_ext | c_n2_ext)
    n=${name#c_n}
    n=${n%_ext}
    csv=c_n$n
    args=(--model inverse-power --n "$n" --beta-min 3.55 --beta-max 5.00 --beta-step 0.05)
    ;;
q_n1 | q_n2 | e_n1 | e_n2)
    n=${name#?_n}
    args=(--model inverse-power --n "$n" --beta-min "$2" --beta-max "$3" --beta-step 0.001)
    ;;
q_exp)
    args=(--beta-min 1.100 --beta-max 1.150 --beta-step 0.001)
    ;;
f_new_check)
    args=(--beta-values 2.975 2.976 3.003)
    ;;
*)
    echo "scans.sh: unknown scan $name" >&2
    exit 2
    ;;
esac
console="$OUT/$name.console.txt"
cmd="PYTHONPATH=. ./venv/bin/python tools/surfing_scan.py --out $OUT/$csv.csv --M 1e-3 --phi-init-Mp 5 ${args[*]}"
{
    echo "# command: $cmd"
    echo "# start: $(date '+%F %T') $(uptime)"
} >>"$console"
t0=$(date +%s)
PYTHONPATH=. ./venv/bin/python tools/surfing_scan.py --out "$OUT/$csv.csv" --M 1e-3 --phi-init-Mp 5 "${args[@]}" >>"$console" 2>&1
rc=$?
t1=$(date +%s)
echo "# end: $(date '+%F %T') exit=$rc wall=$((t1 - t0)) s $(uptime)" >>"$console"
echo "$name: exit=$rc wall=$((t1 - t0)) s"
