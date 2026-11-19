#!/bin/sh
# power-law-wall 07d (copied from power-law-wall 07's rehearse-07/check.sh, with only its paths changed; 07's was
# adapted from one-loop-drain 06's check.sh): the one check after a launch (as
# jordan-normalization 09; the orchestrator's note for 09, ruling 4). Run from this worktree's root, for a base run too
# (its log is under this worktree's var/power-law-wall/07d-r2/runs/ and names its manifest by an absolute path).
# Reads the launch log, the run's manifest.json and status.json, the pid file, and `list`.
# Starts nothing and signals nothing. Appends to var/power-law-wall/07d-r2/runs/07d-checks.txt, and writes nothing else.
#   sh .documents/power-law-wall/rehearse-07d/check.sh SLUG
L=var/power-law-wall/07d-r2/runs
slug=$1
log=$L/$slug.log
{
    echo "### $(date '+%F %T %z') one check, $slug"
    m=$(grep -m1 '^\*\*   manifest ' "$log" | sed 's/^\*\*   manifest //')
    echo "manifest path: $m"
    ls -l "$m"
    d=$(dirname "$m")
    ./venv/bin/python -c '
import json, sys
d = sys.argv[1]
s = json.load(open(d + "/status.json"))
m = json.load(open(d + "/manifest.json"))
print("status state", s.get("state"), "pid", s.get("pid"), "heartbeat", s.get("heartbeat"), "stage", s.get("stage"))
print("created", m.get("created"), "results_store_id", m.get("results_store_id"))
' "$d"
    echo "pid file: $(cat $L/$slug.pid)"
    echo "log birth: $(/usr/bin/stat -f '%SB' -t '%F %T %z' "$log")"
    echo "manifest birth: $(/usr/bin/stat -f '%SB' -t '%F %T %z' "$m")"
    echo '$ list'
    ./venv/bin/python -m runregistrykit list
    echo "list exit $?"
    echo '$ head of log'
    head -4 "$log" | grep -n ''
    echo
} >> $L/07d-checks.txt 2>&1
