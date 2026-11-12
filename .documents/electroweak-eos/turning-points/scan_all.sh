#!/bin/zsh
# electroweak-eos prompt 01a: run scan_turning_points.py over its 36 histories, one process per
# history, P at a time, into OUTDIR/<beta>_<M>.json (one history each); compare_scans.py reads them.
# usage (from the repository root): scan_all.sh OUTDIR [TABLE.csv] [P]
export SCAN_OUT=$1 SCAN_TABLE=$2
P=${3:-5}
mkdir -p $SCAN_OUT
{ uptime; date -u +%H:%M:%S; } > $SCAN_OUT/uptime.txt
PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/scan_turning_points.py --list |
  xargs -P $P -n 1 zsh -c '
    f=${1//,/_}
    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/scan_turning_points.py \
      --only $1 --out $SCAN_OUT/$f.json ${SCAN_TABLE:+--table} ${SCAN_TABLE:+$SCAN_TABLE} > $SCAN_OUT/$f.log 2>&1
  ' _
{ uptime; date -u +%H:%M:%S; } >> $SCAN_OUT/uptime.txt
