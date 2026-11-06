#!/bin/bash
# one-loop-drain prompt 05 §1.6 (orchestrator note 05, ruling 11): launch run_05.py's scheduler once, detached (nohup), from
# the repository root. Its pid goes in var/one-loop-drain/05/scheduler.pid and its output in scheduler.out; it writes the
# end marker var/one-loop-drain/05/scheduler.done after its last child and the merge. Nothing is registered, no Ray is
# started, and no store is opened but the children's temporary ones under var/one-loop-drain/05/stores/.
set -euo pipefail
cd "$(dirname "$0")/../../.."
VAR=var/one-loop-drain/05
mkdir -p "$VAR"
rm -f "$VAR/scheduler.done" "$VAR/scheduler.json" "$VAR/scheduler.log"
PYTHONPATH=. nohup ./venv/bin/python .documents/one-loop-drain/derive-05/run_05.py scheduler \
  > "$VAR/scheduler.out" 2>&1 < /dev/null &
echo $! > "$VAR/scheduler.pid"
disown
echo "launched: scheduler pid $(cat $VAR/scheduler.pid)"
