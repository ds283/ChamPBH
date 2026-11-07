#!/bin/sh
# one-loop-drain 06 (adapted from inelastic-reflection 11's suite.sh): run one CLAUDE.md suite command
# from the repository root, its output to $SUITES/<point>-<name>.txt, and a summary (the command,
# uptime before, the wall time, uptime after, Ran, the verdict, the exit) appended to $SUITES/06-suites.txt.
# $SUITES is a directory given in the environment (the session scratchpad, by the orchestrator's note for
# 06, "Scratch"); the script writes nothing else, and reads only the suite's own output.
#   SUITES=<dir> sh .documents/one-loop-drain/rehearse-06/suite.sh POINT NAME DIR [PATTERN]
L=${SUITES:?set SUITES to the output directory}
mkdir -p "$L"
point=$1; name=$2; dir=$3; pat=$4
out=$L/$point-$name.txt
if [ -n "$pat" ]; then
    cmd="PYTHONPATH=. ./venv/bin/python -m unittest discover -s $dir -t . -p '$pat'"
else
    cmd="PYTHONPATH=. ./venv/bin/python -m unittest discover -s $dir -t ."
fi
{
    echo "## $point $name: $(date '+%F %T %z')"
    echo "\$ $cmd"
    echo "uptime: $(uptime)"
} >> $L/06-suites.txt
t0=$(date +%s)
if [ -n "$pat" ]; then
    PYTHONPATH=. ./venv/bin/python -m unittest discover -s "$dir" -t . -p "$pat" > "$out" 2>&1
else
    PYTHONPATH=. ./venv/bin/python -m unittest discover -s "$dir" -t . > "$out" 2>&1
fi
rc=$?
t1=$(date +%s)
{
    echo "wall: $((t1 - t0)) s"
    echo "uptime after: $(uptime)"
    grep -E '^Ran [0-9]+ tests? in' "$out"
    echo "verdict: $(grep -E '^(OK|FAILED)' "$out" | tail -1)"
    echo "exit: $rc"
    echo
} >> $L/06-suites.txt
tail -6 $L/06-suites.txt
