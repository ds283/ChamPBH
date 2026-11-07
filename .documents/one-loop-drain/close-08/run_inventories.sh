#!/usr/bin/env bash
# one-loop-drain prompt 08, §2 item 4 and orchestrator note 08 ruling 3: main.py --inventory, read-only and with no Ray,
#   (1) on var/datastores/one-loop-drain-06-reference.db in place, and
#   (2) on a copy of var/datastores/one-loop-drain-06-base (all 18 files, cp -p) in a scratch directory,
# each with shasum -a 256 of the store's 18 files (16 shards, the primary, the manifest) before and after.
# Usage, from the repository root:  bash .documents/one-loop-drain/close-08/run_inventories.sh SCRATCH OUT
# SCRATCH is a directory outside the repository (the copy goes in SCRATCH/basecopy); OUT receives the two records.
# The records name SCRATCH as written; the committed copies write it <scratch>.
set -u
SCRATCH="$1"
OUT="$2"
STORES=var/datastores

files() {  # files <dir> <stem>: the store's 18 files, sorted
    ls "$1" | grep -E "^$2(\.db|-shard[0-9]{4}\.db|\.manifest\.json)$" | sort | sed "s|^|$1/|"
}

run_inventory() {  # run_inventory <db>
    echo "\$ PYTHONPATH=. ./venv/bin/python main.py --config exponential.yaml --database $1 --shards 16 --inventory"
    echo "# start $(date '+%F %T %z'); $(uptime)"
    local t0 t1 status
    t0=$(./venv/bin/python -c 'import time; print(time.time())')
    PYTHONPATH=. ./venv/bin/python main.py --config exponential.yaml --database "$1" --shards 16 --inventory 2>&1
    status=$?
    t1=$(./venv/bin/python -c 'import time; print(time.time())')
    echo "# exit $status; wall $(./venv/bin/python -c "print(f'{$t1 - $t0:.1f}')") s; end $(date '+%F %T %z')"
}

# (1) the reference store, in place
{
    echo "# one-loop-drain 08: main.py --inventory on the reference store, in place (orchestrator note 08, ruling 3)"
    echo "# git HEAD $(git rev-parse --short HEAD)"
    echo "## shasum -a 256, before"
    shasum -a 256 $(files "$STORES" one-loop-drain-06-reference) | tee "$SCRATCH/ref-before.sha"
    echo "## the inventory"
    run_inventory "$STORES/one-loop-drain-06-reference.db"
    echo "## shasum -a 256, after"
    shasum -a 256 $(files "$STORES" one-loop-drain-06-reference) | tee "$SCRATCH/ref-after.sha"
    echo "## before against after"
    if cmp -s "$SCRATCH/ref-before.sha" "$SCRATCH/ref-after.sha"; then
        echo "identical: $(wc -l < "$SCRATCH/ref-after.sha" | tr -d ' ') files"
    else
        echo "DIFFERENT"; diff "$SCRATCH/ref-before.sha" "$SCRATCH/ref-after.sha"
    fi
} > "$OUT/inventory-reference.txt" 2>&1

# (2) a copy of the base store
{
    echo "# one-loop-drain 08: main.py --inventory on a copy of the base store (format 6, 2026.9.0) (orchestrator note 08, ruling 3)"
    echo "# git HEAD $(git rev-parse --short HEAD)"
    mkdir -p "$SCRATCH/basecopy"
    echo "## cp -p of the 18 files into $SCRATCH/basecopy"
    for f in $(files "$STORES" one-loop-drain-06-base); do cp -p "$f" "$SCRATCH/basecopy/"; done
    ls -l "$SCRATCH/basecopy" | tail -n +2 | wc -l | sed 's/^ */files copied: /'
    echo "## shasum -a 256 of the originals"
    (cd "$STORES" && shasum -a 256 $(files . one-loop-drain-06-base | sed 's|^\./||')) | tee "$SCRATCH/base-orig.sha"
    echo "## shasum -a 256 of the copy, before"
    (cd "$SCRATCH/basecopy" && shasum -a 256 $(files . one-loop-drain-06-base | sed 's|^\./||')) | tee "$SCRATCH/base-before.sha"
    echo "## originals against the copy"
    if cmp -s "$SCRATCH/base-orig.sha" "$SCRATCH/base-before.sha"; then echo "identical"; else echo "DIFFERENT"; diff "$SCRATCH/base-orig.sha" "$SCRATCH/base-before.sha"; exit 1; fi
    echo "## the inventory"
    run_inventory "$SCRATCH/basecopy/one-loop-drain-06-base.db"
    echo "## shasum -a 256 of the copy, after"
    (cd "$SCRATCH/basecopy" && shasum -a 256 $(files . one-loop-drain-06-base | sed 's|^\./||')) | tee "$SCRATCH/base-after.sha"
    echo "## the copy, before against after"
    if cmp -s "$SCRATCH/base-before.sha" "$SCRATCH/base-after.sha"; then
        echo "identical: $(wc -l < "$SCRATCH/base-after.sha" | tr -d ' ') files"
    else
        echo "DIFFERENT"; diff "$SCRATCH/base-before.sha" "$SCRATCH/base-after.sha"
    fi
    echo "## files in the copy's directory after"
    ls "$SCRATCH/basecopy" | wc -l | sed 's/^ */files: /'
} > "$OUT/inventory-basecopy.txt" 2>&1
