#!/usr/bin/env bash
# The run script export section, tested on the probe Part 1 stores (PLAN.md P9, section 4), one wall at a time.
set -u
T=var/science-2026.14.0/probe/export-test
DS=$PWD/var/datastores
tools/ray_cluster.sh start "science-2026.14.0 export test on the probe stores (P9)" --num-cpus 8 || exit $?
trap "tools/ray_cluster.sh stop" EXIT
for spec in "exp $DS/science-2026.14.0-probe-p1-exp.db" "pl1 $DS/science-2026.14.0-probe-p1-pl.db" "pl2 $DS/science-2026.14.0-probe-p1-pl.db"; do
  set -- $spec
  echo "=== wall $1 $(date +%T)"
  env DB_EXP=$DS/science-2026.14.0-probe-p1-exp.db DB_PL=$DS/science-2026.14.0-probe-p1-pl.db DB_R=$2 OUT=$T/out LOG=$T/logs \
      A_ALL="0.05 1 10" A_PLOT_R="0.05 1 10" START_AT=X EXPORT_WALLS=$1 MIN_FREE_GB_EXPORT=20 \
      bash science-runs/plan-science-2026.14.0/run_science_2026.14.0.sh > $T/wall-$1.out 2>&1
  echo "    exit $? $(date +%T)"
  grep -E "^!!|Traceback|exited non-zero" $T/wall-$1.out | grep -v "shards read\|dropped because\|not stored\|no ExponentialModel\|no InversePowerModel\|were skipped" | head -8
  grep -E "wall $1 done" $T/wall-$1.out
done
echo "=== all done $(date +%T)"
