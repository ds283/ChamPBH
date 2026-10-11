#!/usr/bin/env bash
# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# One build of the dk-and-rpk-migration rehearsal protocol (prompts/dk-and-rpk-migration/
# README.md section 4.1): README 4.1's main.py command, verbatim but for its parameters.
# Written for dk-and-rpk-migration prompt 01; prompts 02, 06 and 10 run it unchanged.
#
# It starts and stops no Ray cluster: the caller starts one and passes its address. It runs in
# the foreground (the driver is waited on); the caller backgrounds it.

set -u

usage() {
    cat <<'EOF'
usage: build.sh NN STORE BUILD ADDRESS [--prune-unvalidated]

  NN        the prompt's number, two digits (02, 06, 10)
  STORE     reference | resumed
  BUILD     reference | resumed-a | resumed-b   (the slug)
  ADDRESS   the Ray cluster's address, passed to main.py as --ray-address

Runs, from the repository root:
  PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml \
      --database $REHEARSAL_ROOT/NN-STORE.db --shards 16 \
      --job-name dk-and-rpk-migration-NN-BUILD --M-values-Mp 1e-3 \
      --beta-values 1.1 1.15 1.2 10 25 --ray-address ADDRESS [--prune-unvalidated]

Output goes to $REHEARSAL_ROOT/logs/NN-BUILD.log, the driver's pid to
$REHEARSAL_ROOT/logs/NN-BUILD.pid as soon as it starts, and "** driver exit status N" is
appended to the log when it ends. The script exits with the driver's status. It refuses to
overwrite an existing log.

REHEARSAL_ROOT defaults to ~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration.
EOF
    exit "${1:-2}"
}

if [[ $# -ge 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage 0
fi
if [[ $# -lt 4 || $# -gt 5 ]]; then
    usage 2
fi

NN=$1
STORE=$2
BUILD=$3
ADDRESS=$4
PRUNE=${5:-}

if [[ ! "$NN" =~ ^[0-9][0-9]$ ]]; then
    echo "!! build.sh: NN must be two digits, not '$NN'" >&2
    exit 2
fi
case "$STORE" in
    reference|resumed) ;;
    *) echo "!! build.sh: STORE must be reference or resumed, not '$STORE'" >&2; exit 2 ;;
esac
case "$BUILD" in
    reference|resumed-a|resumed-b) ;;
    *) echo "!! build.sh: BUILD must be reference, resumed-a or resumed-b, not '$BUILD'" >&2; exit 2 ;;
esac
if [[ -n "$PRUNE" && "$PRUNE" != "--prune-unvalidated" ]]; then
    echo "!! build.sh: the fifth argument may only be --prune-unvalidated, not '$PRUNE'" >&2
    exit 2
fi
if [[ ! -f main.py || ! -d PRyMrates || ! -x venv/bin/python ]]; then
    echo "!! build.sh: run from the repository root (main.py, PRyMrates/ and venv/ are read from it)" >&2
    exit 2
fi

ROOT=${REHEARSAL_ROOT:-$HOME/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration}
LOGS="$ROOT/logs"
LOG="$LOGS/$NN-$BUILD.log"
PIDFILE="$LOGS/$NN-$BUILD.pid"

if [[ -e "$LOG" ]]; then
    echo "!! build.sh: $LOG already exists; refusing to overwrite it" >&2
    exit 2
fi
mkdir -p "$LOGS"

ARGS=(--config exponential.yaml
      --database "$ROOT/$NN-$STORE.db" --shards 16
      --job-name "dk-and-rpk-migration-$NN-$BUILD" --M-values-Mp 1e-3
      --beta-values 1.1 1.15 1.2 10 25 --ray-address "$ADDRESS")
if [[ -n "$PRUNE" ]]; then
    ARGS+=("$PRUNE")
fi

PYTHONPATH=. ./venv/bin/python -u main.py "${ARGS[@]}" > "$LOG" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"
echo ">> build.sh: $NN-$BUILD started, driver pid $PID, log $LOG"

wait "$PID"
STATUS=$?
echo "** driver exit status $STATUS" >> "$LOG"
echo ">> build.sh: $NN-$BUILD ended, driver exit status $STATUS"
exit "$STATUS"
