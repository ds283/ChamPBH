#!/usr/bin/env bash
# ChamPBH science run, VERSION_LABEL 2026.8.0: the plan is PLAN.md beside this file, section 8.
#
# Run from the ChamPBH repository root, on a committed tree (the script refuses a dirty one, so
# that every block's manifest records git_dirty false), with ChamPBH's Ray cluster already up:
#   .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "science-2026.8.0"
# and stop it afterwards with that script's stop (never `ray stop`). Launch detached:
#   nohup bash science-runs/<folder>/run_science_2026.8.0.sh > var/science-2026.8.0/run.out 2>&1 &
# then check once that the first block started (./venv/bin/python -m runregistrykit list).
#
# Two stores under var/datastores/: the main store (blocks A, A2, B, C, D) and the reflection
# store (blocks R), which plot_ScalarHistory.py reads whole. Every main.py block is registered
# through RunDrivers/registered_run.py as --campaign science-2026.8.0 --prompt <tier>, and
# every block can be restarted: main.py looks up each history before computing it, so
# re-running the script recomputes only what is missing. After a SIGKILL, end the stale run
# first (runregistrykit end RUN_ID --reason ..., CLAUDE.md rule 5).
set -euo pipefail

if [ ! -f main.py ] || [ ! -d RunDrivers ]; then
    echo "!! run from the ChamPBH repository root" >&2
    exit 2
fi
if [ -n "$(git status --porcelain)" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "!! the tree is dirty; commit first (or set ALLOW_DIRTY_TREE=1 to run anyway)" >&2
    exit 2
fi

RUN=science-2026.8.0
STORE_DIR=var/datastores
DB="$STORE_DIR/science-2026.8.0.db"
DB_R="$STORE_DIR/science-2026.8.0-reflections.db"
LOG=var/science-2026.8.0/logs
OUT=var/science-2026.8.0/out
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
mkdir -p "$STORE_DIR" "$LOG" "$OUT"
export PYTHONPATH=.

# G spec...: the sorted distinct betas of its specs, each the double of a ratio of integers, so
# that the same beta is the same double in every block. A spec lo:hi:step:den is
# lo/den, (lo+step)/den, ..., hi/den; a spec num/den is that one value.
G() {
    ./venv/bin/python -c '
import sys
values = set()
for spec in sys.argv[1:]:
    if ":" in spec:
        lo, hi, step, den = map(int, spec.split(":"))
        values.update(i / den for i in range(lo, hi + 1, step))
    else:
        num, den = map(int, spec.split("/"))
        values.add(num / den)
print(" ".join(repr(v) for v in sorted(values)))' "$@"
}

# the beta grids of PLAN.md section 8.2
NS="50/100 75/100 90/100 100/100"                                       # non-surfing
W="10600:12000:25:10000 120:150:1:100"                                   # the transition window
TAIL="325/100 350/100 4/1 5/1 6/1 8/1 10/1 15/1 20/1"
BETA_DENSE=$(G $NS $W 150:300:2:100 2980:3040:5:1000 $TAIL)              # 186
BETA_SPARSE=$(G $NS 110:300:10:100 115/100 125/100 350/100 5/1 10/1 20/1) # 30
BETA_LOW=$(G $W 160:300:10:100 350/100 5/1 10/1 20/1)                    # 106: W + sparse-out
BETA_COARSE=$(G $NS 110:300:5:100 108:120:1:100 $TAIL)                   # 62
BETA_R="1.2 2.0 2.9"

# the alpha grids
ALPHA_MAIN="0.15 0.1 0.08 0.05 0.03 0.02 0.01"
ALPHA_LOW="0.007 0.005 0.003 0.002 0.0015 0.001"
ALPHA_R="0.15 0.08 0.01 0.001"
ALPHA_PLOT_MAIN="0 0.001 0.0015 0.002 0.003 0.005 0.007 0.01 0.02 0.03 0.05 0.08 0.1 0.15"
ALPHA_PLOT_R="0 0.001 0.01 0.08 0.15"

count() { echo $#; }
echo ">> $(date '+%F %T') $RUN on $(git rev-parse HEAD), β grids: dense $(count $BETA_DENSE), sparse $(count $BETA_SPARSE), low $(count $BETA_LOW), coarse $(count $BETA_COARSE)"

run() {  # run <store> <tier> <job> <purpose> <main.py arguments...>
    local db="$1" tier="$2" job="$3" purpose="$4"
    shift 4
    echo ">> $(date '+%F %T') $job"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt "$tier" \
        --register "$job" --purpose "$purpose" -- --config exponential.yaml --database "$db" \
        --shards "$SHARDS" --ray-address "$RAY_ADDR" --job-name "$job" \
        --bbn-wall-clock-limit 600 --phi-init-Mp 5 "$@" 2>&1 | tee "$LOG/$job.log"
}

sidecar() {  # sidecar <store> <purpose>: the store's fingerprint sidecar, once, after its first block
    local db="$1" purpose="$2"
    [ -e "${db%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create \
        --purpose "$purpose" "$db"
}

# A   Figure 6: laboratory M, the dense beta grid, the main alpha grid          1302 histories
run "$DB" A A-lab-main \
    "$RUN A: Figure 6 at laboratory M 4e-31 Mp, dense beta 0.5-20, alpha 0.01-0.15, phi* 5" \
    --M-values-Mp 4e-31 --beta-values $BETA_DENSE --alpha-values $ALPHA_MAIN
sidecar "$DB" "$RUN main store: Figure 6 at laboratory M and its comparisons at larger M (PLAN.md section 8)"

# A2  Figure 6: low alpha, dense in the transition window, sparse above it      636 histories
run "$DB" A2 A2-lab-low-alpha \
    "$RUN A2: Figure 6 low alpha 0.001-0.007 at M 4e-31 Mp, transition window 1.06-1.50 and sparse beta above" \
    --M-values-Mp 4e-31 --beta-values $BETA_LOW --alpha-values $ALPHA_LOW

# B   M around and below the laboratory bound                                    420 histories
run "$DB" B B-lab-neighbourhood \
    "$RUN B: M 1e-30 and 1e-33 Mp on the sparse beta grid, alpha 0.01-0.15, to show independence of M" \
    --M-values-Mp 1e-30 1e-33 --beta-values $BETA_SPARSE --alpha-values $ALPHA_MAIN

# C   around M_crit                                                              1302 histories
run "$DB" C C-Mcrit \
    "$RUN C: M 1e-15, 1e-13, 1e-12 Mp around M_crit, coarse beta, alpha 0.01-0.15" \
    --M-values-Mp 1e-15 1e-13 1e-12 --beta-values $BETA_COARSE --alpha-values $ALPHA_MAIN

# D   large M, natural mode only (alpha = 0)                                     186 histories
run "$DB" D D-large-M-natural \
    "$RUN D: natural mode (alpha 0) at M 1e-3, 0.1, 0.5 Mp, coarse beta" \
    --M-values-Mp 1e-3 0.1 0.5 --beta-values $BETA_COARSE --alpha-values 0

# R   the reflection store: a few histories drawn whole by plot_ScalarHistory    42 histories
run "$DB_R" R R-reflections-dissipation \
    "$RUN R: reflection structure, beta 1.2 2 2.9 at M 4e-31, 1e-15, 1e-12 Mp, alpha 0.001-0.15" \
    --M-values-Mp 4e-31 1e-15 1e-12 --beta-values $BETA_R --alpha-values $ALPHA_R
sidecar "$DB_R" "$RUN reflection store: a few histories for the reflection-structure figure (PLAN.md section 8.3)"
run "$DB_R" R R-reflections-natural \
    "$RUN R: reflection structure, natural mode (alpha 0), beta 1.2 2 2.9 at M 1e-3 and 0.5 Mp" \
    --M-values-Mp 1e-3 0.5 --beta-values $BETA_R --alpha-values 0

# the inventories: read-only, unregistered, no Ray
for STORE in "$DB" "$DB_R"; do
    NAME=$(basename "${STORE%.db}")
    ./venv/bin/python main.py --config exponential.yaml --database "$STORE" --shards "$SHARDS" \
        --inventory 2>&1 | tee "$LOG/inventory-$NAME.log"
done

# the plots: read-only, unregistered. plot_by_beta writes histories.csv (every alpha) under
# $OUT/main/QCD_Cosmology/; plot_ScalarHistory writes fields.csv per history under $OUT/reflections/
echo ">> $(date '+%F %T') plot_by_beta (main store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT_MAIN \
    --output "$OUT/main" 2>&1 | tee "$LOG/plot_by_beta-main.log"
echo ">> $(date '+%F %T') plot_ScalarHistory (reflection store)"
./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database "$DB_R" \
    --shards "$SHARDS" --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT_R \
    --output "$OUT/reflections" 2>&1 | tee "$LOG/plot_ScalarHistory-reflections.log"

echo ">> $(date '+%F %T') $RUN done"
