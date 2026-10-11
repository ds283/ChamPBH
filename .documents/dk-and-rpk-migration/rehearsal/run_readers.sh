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
# Every reader of a closed rehearsal store: the dk-and-rpk-migration protocol's L3
# (prompts/dk-and-rpk-migration/README.md section 4.1). Written for dk-and-rpk-migration
# prompt 01; prompts 02, 06 and 10 run it unchanged.
#
# It starts and stops no Ray cluster: the caller starts one and passes its address.

set -u

usage() {
    cat <<'EOF'
usage: run_readers.sh NN STORE ADDRESS

  NN        the prompt's number, two digits (02, 06, 10)
  STORE     reference | resumed: the store is $REHEARSAL_ROOT/NN-STORE.db
  ADDRESS   the Ray cluster's address, passed to each driver as --ray-address

Run from the repository root, on a closed store. Into $REHEARSAL_ROOT/NN-readers/:
  0. a copy of the store's files into before/, for comparison only; no layer opens it
  1. main.py --inventory                                    -> 1-inventory.log
  2. plot_by_beta.py --phi-init-Mp 5 --output plot_by_beta  -> 2-plot_by_beta.log
  3. plot_ScalarModel.py --output plot_ScalarModel          -> 3-plot_ScalarModel.log
  4. tools/bbn_from_store.py --beta 1.15 --M-Mp 1e-3 --small-network --variant prod
                                                            -> 4-bbn_from_store.log
  5. tools/compare_stores.py before/NN-STORE.db NN-STORE.db -> 5-compare_stores.log, .json
Each step's exit status is appended to status.txt. The script exits 1 if any step did not
exit 0. It refuses to run if NN-readers/ already exists.

REHEARSAL_ROOT defaults to ~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration.
EOF
    exit "${1:-2}"
}

if [[ $# -ge 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage 0
fi
if [[ $# -ne 3 ]]; then
    usage 2
fi

NN=$1
STORE=$2
ADDRESS=$3

if [[ ! "$NN" =~ ^[0-9][0-9]$ ]]; then
    echo "!! run_readers.sh: NN must be two digits, not '$NN'" >&2
    exit 2
fi
case "$STORE" in
    reference|resumed) ;;
    *) echo "!! run_readers.sh: STORE must be reference or resumed, not '$STORE'" >&2; exit 2 ;;
esac
if [[ ! -f main.py || ! -d PRyMrates || ! -x venv/bin/python ]]; then
    echo "!! run_readers.sh: run from the repository root (main.py, PRyMrates/ and venv/ are read from it)" >&2
    exit 2
fi

ROOT=${REHEARSAL_ROOT:-$HOME/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration}
STEM="$ROOT/$NN-$STORE"
DB="$STEM.db"
OUT="$ROOT/$NN-readers"

if [[ ! -f "$DB" ]]; then
    echo "!! run_readers.sh: no store at $DB" >&2
    exit 2
fi
if [[ -e "$OUT" ]]; then
    echo "!! run_readers.sh: $OUT already exists; refusing to overwrite it" >&2
    exit 2
fi
mkdir -p "$OUT/before"

FAILED=0
record() {
    # record STEP NAME STATUS
    echo "step $1 $2: exit $3" >> "$OUT/status.txt"
    echo ">> run_readers.sh: step $1 $2: exit $3"
    if [[ "$3" -ne 0 ]]; then
        FAILED=1
    fi
}

# 0. the store's files as they are before any reader, for step 5 only
shopt -s nullglob
SHARDS=("$STEM"-shard*.db)
shopt -u nullglob
if [[ ${#SHARDS[@]} -eq 0 ]]; then
    echo "!! run_readers.sh: no shards match $STEM-shard*.db" >&2
    exit 2
fi
cp -p "$DB" "${SHARDS[@]}" "$OUT/before/"
record 0 copy-before $?

# 1. the inventory
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml --database "$DB" \
    --shards 16 --inventory --ray-address "$ADDRESS" > "$OUT/1-inventory.log" 2>&1
record 1 inventory $?

# 2. plot_by_beta.py at phi* = 5 M_P
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB" \
    --shards 16 --phi-init-Mp 5 --output "$OUT/plot_by_beta" --ray-address "$ADDRESS" \
    > "$OUT/2-plot_by_beta.log" 2>&1
record 2 plot_by_beta $?

# 3. plot_ScalarModel.py
PYTHONPATH=. ./venv/bin/python -u plot_ScalarModel.py --config exponential.yaml --database "$DB" \
    --shards 16 --output "$OUT/plot_ScalarModel" --ray-address "$ADDRESS" \
    > "$OUT/3-plot_ScalarModel.log" 2>&1
record 3 plot_ScalarModel $?

# 4. one PRyMordial solve on one stored history, on the network production uses
PYTHONPATH=. ./venv/bin/python -u tools/bbn_from_store.py "$STEM" --beta 1.15 --M-Mp 1e-3 \
    --small-network --variant prod > "$OUT/4-bbn_from_store.log" 2>&1
record 4 bbn_from_store $?

# 5. the store after the readers against the copy made before them
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py "$OUT/before/$NN-$STORE.db" "$DB" \
    --json "$OUT/5-compare_stores.json" > "$OUT/5-compare_stores.log" 2>&1
record 5 compare_stores $?

exit "$FAILED"
