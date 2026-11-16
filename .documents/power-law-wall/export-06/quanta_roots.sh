#!/bin/bash
# The quanta tools on one exponential root (power-law-wall prompt 06, §2.2; README §6.10 P54), from the root of the tree
# whose tools are wanted, with this worktree's interpreter:
#
#     cd TREE && bash /path/to/quanta_roots.sh VENV_PYTHON EXPORT_ROOT V NAME
#
# EXPORT_ROOT is copied to V/qroot, one path for both trees, so that the grid figure's footer (which prints its root)
# and the printed lines are comparable; tools/quanta_measures.py, tools/plot_quanta_grid.py and
# tools/plot_quanta_history.py are run on it (the figure tools with --output V/figures-NAME), their stdout and stderr
# into V/runs-NAME/<tool>.stdout with the exit status appended; then V/qroot is moved to V/quanta-NAME.
# The base ran it on export-base/QCD_Cosmology (NAME base); the tip on export-tip/QCD_Cosmology/exponential (NAME tip).
set -u
PY="$1"; ROOT="$2"; V="$3"; NAME="$4"
mkdir -p "$V/runs-$NAME"
cp -R "$ROOT" "$V/qroot"
PYTHONPATH=. "$PY" tools/quanta_measures.py "$V/qroot" > "$V/runs-$NAME/quanta_measures.stdout" 2>&1
echo "exit $?" >> "$V/runs-$NAME/quanta_measures.stdout"
for t in plot_quanta_grid plot_quanta_history; do
  MPLBACKEND=Agg PYTHONPATH=. "$PY" tools/$t.py "$V/qroot" --output "$V/figures-$NAME" > "$V/runs-$NAME/$t.stdout" 2>&1
  echo "exit $?" >> "$V/runs-$NAME/$t.stdout"
done
mv "$V/qroot" "$V/quanta-$NAME"
