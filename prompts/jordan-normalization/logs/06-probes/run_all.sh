#!/bin/bash
# jordan-normalization prompt 06, ruling 13: compare.py on README §0.3's thirteen points, with the
# secant and with the plain update: 26 tunings in at most eight parallel lanes, the longest first.
# Run from the repository root, detached:
#
#     nohup bash run_all.sh OUTDIR > OUTDIR/driver.log 2>&1 &
#
# Each tuning's output goes to OUTDIR/b<beta>_M<M>_phi<phi*>_<method>.log; driver.log gets a
# start and an end line per tuning, with the machine's load.
set -u
OUT=$1
SCRIPT="$(cd "$(dirname "$0")" && pwd)/compare.py"
mkdir -p "$OUT"
export OUT SCRIPT

run_one() {
    local b=$1 m=$2 p=$3 meth=$4
    local log="$OUT/b${b}_M${m}_phi${p}_${meth}.log"
    echo "start $b $m $p $meth $(date +%T) $(uptime | sed 's/.*load averages*: //')"
    PYTHONPATH=. ./venv/bin/python "$SCRIPT" "$b" "$m" "$p" "$meth" > "$log" 2>&1
    echo "end   $b $m $p $meth $(date +%T) exit=$? $(uptime | sed 's/.*load averages*: //')"
}
export -f run_one

cat <<'EOF' | xargs -P 8 -L 1 bash -c 'run_one "$@"' _
25 1e-3 5 plain
25 1e-3 5 secant
5.5 1e-3 5 plain
5.5 1e-3 5 secant
3 1e-5 5 plain
3 1e-5 5 secant
3 0.5 5 plain
3 0.5 5 secant
3 1e-3 5 plain
3 1e-3 5 secant
3 0.1 5 plain
3 0.1 5 secant
3 0.01 5 plain
3 0.01 5 secant
1.1 1e-3 2 plain
1.1 1e-3 2 secant
1.5 1e-3 5 plain
1.5 1e-3 5 secant
1 1e-5 5 plain
1 1e-5 5 secant
0.5 1e-3 1 plain
0.5 1e-3 1 secant
1 0.5 5 plain
1 0.5 5 secant
1 0.03 5 plain
1 0.03 5 secant
EOF
echo "ALL DONE $(date +%T)"
