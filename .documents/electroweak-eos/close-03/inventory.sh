#!/bin/bash
# electroweak-eos prompt 03, §2 item 3: main.py --inventory on a scratch copy of the old side's store.
# Usage: inventory.sh SCRATCH_DIR   (run from the repository root; starts no Ray, writes nothing to var/)
set -u
SCRATCH="$1"
SRC=var/datastores
NAME=electroweak-eos-02-old
OUT=.documents/electroweak-eos/close-03/out
mkdir -p "$SCRATCH/copy"
rm -f "$SCRATCH/copy/$NAME"*
cp -p "$SRC/$NAME".db "$SRC/$NAME"-shard00*.db "$SRC/$NAME".manifest.json "$SCRATCH/copy/"
ls "$SCRATCH/copy" | wc -l > "$OUT/copy_file_count.txt"
( cd "$SCRATCH/copy" && shasum -a 256 $NAME* ) > "$OUT/sha256_source_copy_before.txt"
( cd "$SRC" && shasum -a 256 $NAME* ) > "$OUT/sha256_source_var.txt"
start=$(date +%s)
PYTHONPATH=. ./venv/bin/python main.py --config exponential.yaml --database "$SCRATCH/copy/$NAME.db" --shards 16 --inventory > "$OUT/inventory_stdout.txt" 2> "$OUT/inventory_stderr.txt"
echo "exit=$? seconds=$(( $(date +%s) - start ))" > "$OUT/inventory_exit.txt"
( cd "$SCRATCH/copy" && shasum -a 256 $NAME* ) > "$OUT/sha256_source_copy_after.txt"
cmp "$OUT/sha256_source_copy_before.txt" "$OUT/sha256_source_copy_after.txt" && echo "copy unchanged" >> "$OUT/inventory_exit.txt" || echo "COPY CHANGED" >> "$OUT/inventory_exit.txt"
cmp "$OUT/sha256_source_copy_before.txt" "$OUT/sha256_source_var.txt" && echo "copy equals var store" >> "$OUT/inventory_exit.txt"
