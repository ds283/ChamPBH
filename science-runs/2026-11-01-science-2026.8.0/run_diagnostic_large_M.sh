#!/usr/bin/env bash
# The 2026.8.0 run's large-M diagnostic, added 2026-11-01 at the user's request after the run.
#
# Two things, neither changing the run's two stores:
#   1. plot_by_beta.py on the main store again, into out/main-export/, so that histories.csv carries
#      the dissipation columns added to extract_common.CSV_COLUMNS after the run (parking and
#      release temperatures, injections, failing margin shares). Read-only.
#   2. dissipation-mode histories at large M, at the alpha of the reflection figure's row 1, into a
#      store of their own, so that the reflection figure can compare dark radiation at the same alpha
#      across M (the run computed large M in natural mode only, by the user's ruling for science
#      output; these are a diagnostic of the figure). Then plot_ScalarHistory.py on that store.
#
# Run from the ChamPBH repository root, on a committed tree, with the cluster up:
#   tools/ray_cluster.sh start "science-2026.8.0 diagnostic"
#   bash science-runs/2026-11-01-science-2026.8.0/run_diagnostic_large_M.sh > var/science-2026.8.0/diagnostic/run.out 2>&1
# and stop it afterwards with tools/ray_cluster.sh stop.
set -euo pipefail

if [ ! -f main.py ] || [ ! -d RunDrivers ]; then
    echo "!! run from the ChamPBH repository root" >&2
    exit 2
fi
if [ -n "$(git status --porcelain --untracked-files=no)" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "!! the tree is dirty; commit first (or set ALLOW_DIRTY_TREE=1 to run anyway)" >&2
    exit 2
fi

RUN=science-2026.8.0
DB="var/datastores/science-2026.8.0.db"
DB_X="var/datastores/science-2026.8.0-large-M-dissipation.db"
LOG=var/science-2026.8.0/diagnostic/logs
OUT=var/science-2026.8.0/diagnostic/out
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
mkdir -p "$LOG" "$OUT"
export PYTHONPATH=.

M_X="1e-9 1e-6 1e-3 0.5"
BETA_X="1.2 2.0 2.9"
ALPHA_X="0.08"
ALPHA_PLOT_MAIN="0 0.001 0.0015 0.002 0.003 0.005 0.007 0.01 0.02 0.03 0.05 0.08 0.1 0.15"

echo ">> $(date '+%F %T') $RUN diagnostic on $(git rev-parse HEAD)"

# 1. the main store's histories.csv with the dissipation columns (read-only, unregistered)
echo ">> $(date '+%F %T') plot_by_beta (main store, export)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT_MAIN \
    --output "$OUT/main-export" 2>&1 | tee "$LOG/plot_by_beta-main-export.log"

# 2. dissipation at large M, registered
job=X-large-M-dissipation
echo ">> $(date '+%F %T') $job" | tee -a "$LOG/$job.log"
./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt X \
    --register "$job" \
    --purpose "$RUN X: diagnostic, dissipation (alpha $ALPHA_X) at M $M_X Mp, beta $BETA_X, for the reflection figure" \
    -- --config exponential.yaml --database "$DB_X" --shards "$SHARDS" --ray-address "$RAY_ADDR" \
    --job-name "$job" --bbn-wall-clock-limit 600 --phi-init-Mp 5 \
    --M-values-Mp $M_X --beta-values $BETA_X --alpha-values $ALPHA_X 2>&1 | tee -a "$LOG/$job.log"
[ -e "${DB_X%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create \
    --purpose "$RUN diagnostic store: dissipation at large M for the reflection figure" "$DB_X"

./venv/bin/python main.py --config exponential.yaml --database "$DB_X" --shards "$SHARDS" \
    --inventory 2>&1 | tee "$LOG/inventory-large-M-dissipation.log"

echo ">> $(date '+%F %T') plot_ScalarHistory (diagnostic store)"
./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database "$DB_X" \
    --shards "$SHARDS" --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_X \
    --output "$OUT/large-M-dissipation" 2>&1 | tee "$LOG/plot_ScalarHistory-large-M-dissipation.log"

echo ">> $(date '+%F %T') $RUN diagnostic done"
