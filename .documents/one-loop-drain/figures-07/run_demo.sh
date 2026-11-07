#!/bin/bash
# The demonstration of one-loop-drain prompt 07 §3: every command, with its printed output, its exit and its wall time, in out/.
# Run from the repository root:  .documents/one-loop-drain/figures-07/run_demo.sh
set -u
cd "$(git rev-parse --show-toplevel)"
export PYTHONPATH=.
PY=./venv/bin/python
F=.documents/one-loop-drain/figures-07
T6=var/one-loop-drain/06/plot-reference/QCD_Cosmology
T5=var/one-loop-drain/05/exports/QCD_Cosmology
O6=var/one-loop-drain/07/plot-reference/QCD_Cosmology
O5=var/one-loop-drain/07/exports-05/QCD_Cosmology
mkdir -p $F/out
run() { # name, command...
  name=$1; shift
  t0=$(date +%s)
  { echo "# $*"; echo "# load at start: $(uptime | sed 's/.*load averages: //')"; "$@"; rc=$?; echo "exit $rc"; echo "wall $(( $(date +%s) - t0 )) s"; } > $F/out/$name 2>&1
}
run grid-06.txt $PY tools/plot_quanta_grid.py $T6 --output $O6
run history-06.txt $PY tools/plot_quanta_history.py $T6 --output $O6
run grid-05.txt $PY tools/plot_quanta_grid.py $T5 --output $O5
run grid-05-assoc0.txt $PY tools/plot_quanta_grid.py $T5 --A-values 1 --assoc-scale 0 --output $O5
run grid-05-assoc10.txt $PY tools/plot_quanta_grid.py $T5 --A-values 1 --assoc-scale 10 --output $O5
$PY $F/pick_histories.py $T5 > $F/out/picks.txt
k=0
while read -r alpha beta M n bar cols; do
  k=$((k+1))
  run history-05-$k.txt $PY tools/plot_quanta_history.py $T5 --A-values $alpha --beta-values $beta --M-Mp-values $M --output $O5
done < $F/out/picks.txt
