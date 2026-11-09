#!/usr/bin/env bash
# ChamPBH science run 2026.10.0: the post-build steps, in place of those at the end of
# run_science_2026.10.0.sh, which stopped at the derived measures (2026-11-09).
#
# Why: tools/quanta_measures.py reads <root>/histories.csv, which plot_by_beta.py writes, and each
# one-loop history's fields.csv and bounces.csv, which plot_ScalarHistory.py writes, from one
# export root. The run script ran plot_by_beta.py on the main store only and plot_ScalarHistory.py
# on the reflection store only, so every main-store history was "missing" and the reflection
# store had no histories.csv (exit 2). The builds and the exports it did make are unaffected.
#
# This script adds the missing export to each root (the main store's through --no-plots, 5e3b0f8:
# the CSV files alone, byte-identical to those written with figures), then runs the derived
# measures at c_a = 1, 0 and 10 on both roots, the grid figure on the main store and the
# per-history figures on the reflection store. Read-only on both stores; unregistered, as the run
# script's plot steps were.
#
# Run from the ChamPBH repository root, on a committed tree, with ChamPBH's Ray cluster up:
#   nohup bash science-runs/2026-11-08-science-2026.10.0/run_postbuild_2026.10.0.sh > var/science-2026.10.0/postbuild.out 2>&1 &
set -euo pipefail

if [ ! -f main.py ] || [ ! -d RunDrivers ]; then
    echo "!! run from the ChamPBH repository root" >&2
    exit 2
fi
if [ -n "$(git status --porcelain)" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "!! the tree is dirty; commit first (or set ALLOW_DIRTY_TREE=1 to run anyway)" >&2
    exit 2
fi

RUN=science-2026.10.0
DB=var/datastores/science-2026.10.0.db
DB_R=var/datastores/science-2026.10.0-reflections.db
LOG=var/science-2026.10.0/logs
OUT=var/science-2026.10.0/out
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
export PYTHONPATH=.

ALPHA_PLOT="0 0.01 0.1"
A_ALL="0.05 0.1 0.2 0.5 1 1.5 2 3 5 10"
A_PLOT_R="0.05 0.1 1 10"

echo ">> $(date '+%F %T') $RUN post-build on $(git rev-parse HEAD)"

# the main store's per-history CSV files, beside the run's histories.csv: the one-loop rows alone
# (--no-alpha-values, f8f185a), since the measures skip fixed-law histories
echo ">> $(date '+%F %T') plot_ScalarHistory --no-plots (main store, one-loop rows)"
./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --no-alpha-values --A-values $A_ALL --no-plots \
    --output "$OUT/main" 2>&1 | tee "$LOG/postbuild-plot_ScalarHistory-main.log"

# the reflection store's histories.csv (and its figures), beside the run's fields.csv
echo ">> $(date '+%F %T') plot_by_beta (reflection store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB_R" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT --A-values $A_PLOT_R \
    --output "$OUT/reflections" 2>&1 | tee "$LOG/postbuild-plot_by_beta-reflections.log"

# the derived measures (no store, no Ray) at c_a = 1, then 0 and 10 for the sensitivity. quanta_measures
# exits 1 when any history is refused or fails its K check; that is reported, and the rest still run
for ROOT in "$OUT/main/QCD_Cosmology" "$OUT/reflections/QCD_Cosmology"; do
    NAME=$(basename "$(dirname "$ROOT")")
    for SCALE in 1 0 10; do
        echo ">> $(date '+%F %T') quanta_measures $NAME --assoc-scale $SCALE"
        ./venv/bin/python -u tools/quanta_measures.py "$ROOT" --assoc-scale "$SCALE" 2>&1 \
            | tee "$LOG/postbuild-quanta_measures-$NAME-assoc$SCALE.log" \
            || echo "!! quanta_measures $NAME --assoc-scale $SCALE exited non-zero"
    done
done

# the figure tools at c_a = 1
echo ">> $(date '+%F %T') plot_quanta_grid (main store)"
./venv/bin/python -u tools/plot_quanta_grid.py "$OUT/main/QCD_Cosmology" 2>&1 \
    | tee "$LOG/postbuild-plot_quanta_grid-main.log" || echo "!! plot_quanta_grid exited non-zero"
echo ">> $(date '+%F %T') plot_quanta_history (reflection store)"
./venv/bin/python -u tools/plot_quanta_history.py "$OUT/reflections/QCD_Cosmology" 2>&1 \
    | tee "$LOG/postbuild-plot_quanta_history-reflections.log" \
    || echo "!! plot_quanta_history exited non-zero"

echo ">> $(date '+%F %T') $RUN post-build done"
