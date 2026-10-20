#!/usr/bin/env bash
# ChamPBH full science run, VERSION_LABEL 2026.6.0 (tree 4ae25b4 or later).
# Run from the ChamPBH repository root, with ChamPBH's Ray cluster already up:
#   .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "<purpose>"
# and stop it afterwards with that script's stop (never `ray stop`). Every driver that starts Ray
# needs --ray-address; RAY_ADDR overrides the default, ChamPBH's isolated cluster.
# One fresh store, 16 shards, filled in seven main.py invocations, cheapest and most
# important first. Every block is restartable: main.py looks up each (beta, M, phi*)
# before computing it, so rerunning a block recomputes only what is missing.
# After an interrupted invocation, add --prune-unvalidated to the next one.
set -euo pipefail

STORE_DIR="${STORE_DIR:-$HOME/ChamPBH-stores-bt02}"      # outside the repository
DB="$STORE_DIR/science-2026.6.0.db"
LOG="$STORE_DIR/logs"
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
mkdir -p "$LOG"
export PYTHONPATH=.

# betas a/d, (a+s)/d, ..., b/d from integers, so the same beta is the same double in every block
B() { ./venv/bin/python -c 'import sys; a,b,s,d=map(int,sys.argv[1:]); print(" ".join(f"{i/d:g}" for i in range(a,b+1,s)))' "$@"; }

DROP="--drop bbn-data"
run() {  # run <job-name> <args...>
    local job="$1"; shift
    echo ">> $(date '+%F %T') $job"
    ./venv/bin/python main.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
        --ray-address "$RAY_ADDR" --job-name "$job" --bbn-wall-clock-limit 600 "$@" $DROP 2>&1 | tee "$LOG/$job.log"
    DROP=""
}

# C1  main map: beta 1.00-3.00, step 0.01, M = 1e-3                         201 histories
run C1-main-M1e-3          --M-values-Mp 1e-3 --beta-values $(B 100 300 1 100)

# C4  threshold zoom: beta 1.100-1.700, step 0.005, M = 1e-3                 121 (60 new)
run C4-threshold-M1e-3     --M-values-Mp 1e-3 --beta-values $(B 1100 1700 5 1000)

# L   large beta: 3.25-5.00 step 0.25, then 5.5-25 step 0.5, M = 1e-3      48 histories
#     (beta >~ 6.5 starts super-Planckian at phi* = 5: warned and computed, by the ruling)
run L-large-beta-M1e-3     --M-values-Mp 1e-3 --beta-values $(B 325 500 25 100) $(B 55 250 5 10)

# C5  non-surfing region: beta 0.50-1.10, step 0.05, M = 1e-3, phi* = 1, 2, 5   39 (36 new)
for PHI in 1 2 5; do
    run C5-nonsurfing-phi${PHI}-M1e-3 --M-values-Mp 1e-3 --phi-init-Mp $PHI --beta-values $(B 50 110 5 100)
done

# C3  approach to the M-independent limit: beta 1.00-3.00, step 0.05       164 histories
run C3-approach            --M-values-Mp 0.5 0.1 0.03 0.01 --beta-values $(B 100 300 5 100)

# C2  confirmation of the limit: beta 1.00-3.00, step 0.01, M = 1e-5        201 histories (slowest)
run C2-confirm-M1e-5       --M-values-Mp 1e-5 --beta-values $(B 100 300 1 100)

./venv/bin/python main.py --config exponential.yaml --database "$DB" --shards "$SHARDS" --inventory \
    2>&1 | tee "$LOG/inventory.log"

# Figures and histories.csv: one plot_by_beta run per phi* (it reads --phi-init-Mp).
# Rows whose failure_reasons read "no ScalarModel row in the store" are (beta, M) pairs that
# no block computed (plot_by_beta takes the product of every beta and M in the store); drop them.
for PHI in 5 2 1; do
    ./venv/bin/python plot_by_beta.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
        --ray-address "$RAY_ADDR" --phi-init-Mp $PHI --output "$STORE_DIR/science-out-phi$PHI" 2>&1 | tee "$LOG/plot-phi$PHI.log"
done
