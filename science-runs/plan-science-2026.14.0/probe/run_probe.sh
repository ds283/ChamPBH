#!/usr/bin/env bash
# The probe of the 2026.14.0 run (PLAN.md section 3), on the Mac Studio, from its ChamPBH checkout's root.
#
#   Part 1  the same tree on another host: 07d's r2-exp and r2-pl commands rebuilt here, each store compared by value
#           (compare_histories.py, failure_reason's label masked) with its laptop reference. A difference stops the
#           probe before Part 2.
#   Part 2  coverage of the power wall: 768 histories (8 M, 12 beta, alpha 0.01 and A 0.05 1 10, n 1 and 2), and 72
#           exponential histories at block B's six masses at A 1 (law v2 where it is extrapolated, P4).
#
# Every main.py block is registered as --campaign science-2026.14.0 --prompt Q. The references, copied from the laptop
# into var/datastores/ before the probe starts (shards, primary and sidecar):
#   science-2026.14.0-probe-ref-exp   the laptop's rebuild of 07d's r2-exp (PLAN.md section 3, amended 2026-11-20)
#   power-law-wall-07d-r2-pl          07d's power-law store, kept
#
#   bash science-runs/plan-science-2026.14.0/probe/run_probe.sh            # Part 1, then Part 2 if Part 1 is equal
#   START_AT=2 bash science-runs/plan-science-2026.14.0/probe/run_probe.sh # Part 2 alone, after Part 1 is ruled
set -euo pipefail

RUN=science-2026.14.0
HERE=science-runs/plan-science-2026.14.0/probe
LOG=$HERE/logs
OUT=$HERE/out
DS=$PWD/var/datastores
RAY_ADDR=127.0.0.1:6380
NUM_CPUS=${NUM_CPUS:-8}                 # P5, amended 2026-11-20
START_AT=${START_AT:-1}
MIN_FREE_GB=${MIN_FREE_GB:-20}

P1_EXP=$DS/$RUN-probe-p1-exp.db
P1_PL=$DS/$RUN-probe-p1-pl.db
REF_EXP=$DS/$RUN-probe-ref-exp
REF_PL=$DS/power-law-wall-07d-r2-pl
P2_PL=$DS/$RUN-probe-pl.db
P2_EXP=$DS/$RUN-probe-exp.db

# Part 2's grid (PLAN.md section 3)
P2_M_PL="4e-31 1e-30 1e-32 1e-33 1e-36 1e-40 1e-42 1e-16"
P2_M_EXP="1e-30 1e-32 1e-33 1e-36 1e-40 1e-42"
P2_BETA="0.5 1.0 1.1 1.2 1.5 2.0 2.9 2.975 3.0 3.04 5 20"

[ -e CLAUDE.md ] && [ -d RunDrivers ] || { echo "run from the repository root" >&2; exit 2; }
[ -z "$(git status --porcelain)" ] || { echo "the tree is dirty; the probe runs on a clean, committed tree" >&2; exit 2; }
mkdir -p "$LOG" "$OUT"

need_free() {  # need_free <GB> <what>
    local free
    free=$(df -g "$DS" | awk 'NR==2 {print $4}')
    [ "$free" -ge "$1" ] || { echo "!! $2: $free GB free, under $1 GB; stopping" | tee -a "$LOG/probe.log" >&2; exit 3; }
}

run() {  # run <store> <job> <config> <purpose> <main.py arguments...>
    local db="$1" job="$2" config="$3" purpose="$4"
    shift 4
    need_free "$MIN_FREE_GB" "$job"
    echo ">> $(date '+%F %T') $job" | tee -a "$LOG/$job.log" "$LOG/probe.log"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt Q \
        --register "$job" --purpose "$purpose" -- --config "$config" --database "$db" \
        --shards 16 --phi-init-Mp 5 --ray-address "$RAY_ADDR" --job-name "$job" "$@" 2>&1 | tee -a "$LOG/$job.log"
    echo "<< $(date '+%F %T') $job" | tee -a "$LOG/probe.log"
}

sidecar() {  # sidecar <store> <purpose>: once, after the store's first block
    [ -e "${1%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create --purpose "$2" "$1"
}

tools/ray_cluster.sh start "$RUN probe (PLAN.md section 3)" --num-cpus "$NUM_CPUS"
trap 'tools/ray_cluster.sh stop' EXIT

if [ "$START_AT" -le 1 ]; then
    for ref in "$REF_EXP" "$REF_PL"; do
        [ -e "$ref.db" ] && ls "$ref"-shard*.db >/dev/null 2>&1 || { echo "the reference $ref is not here" >&2; exit 2; }
    done

    # Part 1: 07d's commands (.documents/power-law-wall/rehearse-07d/out/07d-commands.txt), --database and --job-name changed
    run "$P1_EXP" probe-p1-exp-a exponential.yaml "$RUN probe Part 1: 07d r2-exp-a on the Mac Studio" \
        --M-values-Mp 4e-31 --beta-values 1.2 2.0 2.9 --alpha-values 0.01 0.1 --A-values 0.05 1 10
    sidecar "$P1_EXP" "$RUN probe Part 1: 07d's r2-exp store rebuilt on the Mac Studio"
    run "$P1_EXP" probe-p1-exp-b exponential.yaml "$RUN probe Part 1: 07d r2-exp-b on the Mac Studio" \
        --M-values-Mp 1e-3 --beta-values 1.2 2.0 2.9 --alpha-values 0
    run "$P1_EXP" probe-p1-exp-c exponential.yaml "$RUN probe Part 1: 07d r2-exp-c on the Mac Studio" \
        --M-values-Mp 1e-33 1e-15 --beta-values 2.0 --A-values 1 --no-alpha-values

    run "$P1_PL" probe-p1-pl-a inverse_power.yaml "$RUN probe Part 1: 07d r2-pl-a on the Mac Studio" \
        --n-values 1 2 --M-values-Mp 4e-31 --beta-values 1.2 2.0 2.9 --alpha-values 0.01 0.1 --A-values 0.05 1 10
    sidecar "$P1_PL" "$RUN probe Part 1: 07d's r2-pl store rebuilt on the Mac Studio"
    run "$P1_PL" probe-p1-pl-b inverse_power.yaml "$RUN probe Part 1: 07d r2-pl-b on the Mac Studio" \
        --n-values 1 2 --M-values-Mp 1e-3 --beta-values 1.2 2.0 2.9 --alpha-values 0
    run "$P1_PL" probe-p1-pl-c inverse_power.yaml "$RUN probe Part 1: 07d r2-pl-c on the Mac Studio" \
        --n-values 1 2 --M-values-Mp 1e-33 1e-15 --beta-values 2.0 --A-values 1 --no-alpha-values

    status=0
    for pair in "exp $REF_EXP ${P1_EXP%.db}" "pl $REF_PL ${P1_PL%.db}"; do
        set -- $pair
        echo ">> $(date '+%F %T') compare $1" | tee -a "$LOG/probe.log"
        if ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_histories.py "$2" "$3" \
            > "$OUT/p1-compare-$1.txt" 2>&1; then
            echo "   $1: equal (compare_histories.py exit 0)" | tee -a "$LOG/probe.log"
        else
            echo "   $1: NOT EQUAL; see $OUT/p1-compare-$1.txt" | tee -a "$LOG/probe.log"
            status=1
        fi
    done
    [ "$status" -eq 0 ] || { echo "!! Part 1 differs; the probe stops for the user before Part 2" | tee -a "$LOG/probe.log"; exit 1; }
fi

if [ "$START_AT" -le 2 ]; then
    # Part 2: the power wall, one block per M (each block's wall time is read per M)
    first=1
    for M in $P2_M_PL; do
        run "$P2_PL" "probe-pl-M$M" inverse_power.yaml \
            "$RUN probe Part 2: power wall at M $M Mp, n 1 2, 12 beta, alpha 0.01, A 0.05 1 10" \
            --bbn-wall-clock-limit 600 --n-values 1 2 --M-values-Mp "$M" --beta-values $P2_BETA \
            --alpha-values 0.01 --A-values 0.05 1 10
        [ "$first" -eq 0 ] || sidecar "$P2_PL" "$RUN probe Part 2: coverage of the power wall (PLAN.md section 3)"
        first=0
    done

    # Part 2: the exponential at block B's masses, A 1 alone (law v2 extrapolated, P4)
    first=1
    for M in $P2_M_EXP; do
        run "$P2_EXP" "probe-exp-M$M" exponential.yaml \
            "$RUN probe Part 2: exponential at M $M Mp, 12 beta, A 1, no fixed alpha (law v2 extrapolated)" \
            --bbn-wall-clock-limit 600 --M-values-Mp "$M" --beta-values $P2_BETA --A-values 1 --no-alpha-values
        [ "$first" -eq 0 ] || sidecar "$P2_EXP" "$RUN probe Part 2: the exponential at block B's masses (PLAN.md section 3, P4)"
        first=0
    done

    for s in pl exp; do
        stem=$DS/$RUN-probe-$s
        ./venv/bin/python .documents/power-law-wall/rehearse-07d/histories.py "$stem" > "$OUT/p2-histories-$s.txt"
    done
    ./venv/bin/python "$HERE/summarise.py" "$OUT/p2-histories-pl.txt" "$OUT/p2-histories-exp.txt" \
        --logs "$LOG" --stores "$DS/$RUN-probe-pl" "$DS/$RUN-probe-exp" > "$OUT/p2-summary.txt"
    echo ">> $(date '+%F %T') Part 2 done; $OUT/p2-summary.txt" | tee -a "$LOG/probe.log"
fi
