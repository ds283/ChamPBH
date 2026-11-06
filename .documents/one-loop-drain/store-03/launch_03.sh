#!/bin/bash
# one-loop-drain prompt 03, Store gate §2.4: launch cost_03.py's six children detached (orchestrator note 03, ruling 10),
# from the repository root. Each: nohup, its pid in var/one-loop-drain/03/<id>.pid, its output in <id>.log, and the
# end marker <id>.done written by the child. At most six at once (all six at once). Nothing is registered and no
# store is opened but the children's own under var/one-loop-drain/03/stores/.
set -euo pipefail
cd "$(dirname "$0")/../../.."
VAR=var/one-loop-drain/03
mkdir -p "$VAR"
for id in P-2197-A1 W-2187-A0.05 L-M8e-18-A0.5 L-M5e-16-A1 C-3016-A1 C-3233-A1; do
  rm -rf "$VAR/stores/$id" "$VAR/$id.done" "$VAR/$id.result.json"
  PYTHONPATH=. nohup ./venv/bin/python .documents/one-loop-drain/store-03/cost_03.py child "$id" \
    > "$VAR/$id.log" 2>&1 < /dev/null &
  echo $! > "$VAR/$id.pid"
  disown
done
echo "launched: $(cat $VAR/*.pid | tr '\n' ' ')"
