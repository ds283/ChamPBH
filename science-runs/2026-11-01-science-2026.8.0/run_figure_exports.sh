#!/usr/bin/env bash
# The 2026.8.0 run's figure exports, added 2026-11-02 at the user's request: plot_by_beta.py on the
# main store and on the diagnostic store, read-only, so that histories.csv carries log_R_first_impact
# (c95650a) as well as the parking temperatures (3cb4374). The figures (fig6/, reflections/) copy
# their inputs from these exports. Neither store changes.
#
# Run from the ChamPBH repository root, on a committed tree, with the cluster up:
#   tools/ray_cluster.sh start "science-2026.8.0 figure exports"
#   bash science-runs/2026-11-01-science-2026.8.0/run_figure_exports.sh > var/science-2026.8.0/figure-exports/run.out 2>&1
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
LOG=var/science-2026.8.0/figure-exports/logs
OUT=var/science-2026.8.0/figure-exports/out
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
mkdir -p "$LOG" "$OUT"
export PYTHONPATH=.

ALPHA_MAIN="0 0.001 0.0015 0.002 0.003 0.005 0.007 0.01 0.02 0.03 0.05 0.08 0.1 0.15"
ALPHA_X="0.08"

echo ">> $(date '+%F %T') $RUN figure exports on $(git rev-parse HEAD)"

echo ">> $(date '+%F %T') plot_by_beta (main store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_MAIN \
    --output "$OUT/main" 2>&1 | tee "$LOG/plot_by_beta-main.log"

echo ">> $(date '+%F %T') plot_by_beta (diagnostic store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB_X" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_X \
    --output "$OUT/large-M-dissipation" 2>&1 | tee "$LOG/plot_by_beta-large-M-dissipation.log"

echo ">> $(date '+%F %T') $RUN figure exports done"
