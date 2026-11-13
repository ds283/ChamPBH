#!/usr/bin/env bash
# ChamPBH science run, VERSION_LABEL 2026.11.0: the plan is PLAN.md beside this file. It rebuilds the
# 2026.10.0 run (science-runs/2026-11-08-science-2026.10.0/) on the electroweak-eos w table, with the dense
# beta grid's fine segment moved to 2.950-3.040 (PLAN.md section 0.1), and makes every export and derived
# measure that run's analyses read (its run script and its post-build script in one; PLAN.md P3).
#
# Run from the ChamPBH repository root, on a committed tree (the script refuses a dirty one, so
# that every block's manifest records git_dirty false), with ChamPBH's Ray cluster already up and
# started from this same root (its workers take the cwd of the start, and read the w table and
# PRyMordial's rates by relative paths):
#   tools/ray_cluster.sh start "science-2026.11.0"
# and stop it afterwards with that script's stop (never `ray stop`). Launch detached:
#   mkdir -p var/science-2026.11.0
#   nohup bash science-runs/<folder>/run_science_2026.11.0.sh > var/science-2026.11.0/run.out 2>&1 &
# then check once that the first block started (./venv/bin/python -m runregistrykit list).
#
# Two stores under var/datastores/: the main store (blocks A, B, D, C) and the reflection store
# (the R blocks). Every main.py block is registered through RunDrivers/registered_run.py as
# --campaign science-2026.11.0 --prompt <tier>, and every block can be restarted: main.py looks up
# each history before computing it, so re-running the script recomputes only what is missing.
# After a SIGKILL, end the stale run first (runregistrykit end RUN_ID --reason ..., CLAUDE.md rule 5).
#
# Slots run in the order A B D R C (PLAN.md section 1). Slot C, last, holds C-Mcrit and R-Mcrit,
# whose histories of beta ~ 1.1-1.4 at M_crit may each tune for one to two hours. A pipeline of
# such histories prints nothing for longer than the registry's 900 s window and reads stale while
# it works: read it with `runregistrykit list --stale-after 4000`, or by its pid.
#
# START_AT=<slot> (A, B, D, R, C or X; default A) resumes the script at that slot after a run that
# ended early, skipping the slots before it; X skips every build slot and runs the inventories,
# exports and measures alone. Each block's log is appended to, so an earlier attempt's stays above
# the resume's:
#   START_AT=C nohup bash science-runs/<folder>/run_science_2026.11.0.sh >> var/science-2026.11.0/run.out 2>&1 &
#
# The disk (PLAN.md P4): Ray stops writing at 95% of the disk. A build slot is not started with less
# than MIN_FREE_GB_BUILD (default 55) GB free, nor the exports with less than MIN_FREE_GB_EXPORT
# (default 70); the script then exits 3, and is resumed with START_AT once space is freed.
set -euo pipefail

if [ ! -f main.py ] || [ ! -d RunDrivers ]; then
    echo "!! run from the ChamPBH repository root" >&2
    exit 2
fi
if [ -n "$(git status --porcelain)" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "!! the tree is dirty; commit first (or set ALLOW_DIRTY_TREE=1 to run anyway)" >&2
    exit 2
fi

RUN=science-2026.11.0
STORE_DIR=var/datastores
DB="$STORE_DIR/science-2026.11.0.db"
DB_R="$STORE_DIR/science-2026.11.0-reflections.db"
LOG=var/science-2026.11.0/logs
OUT=var/science-2026.11.0/out
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
MIN_FREE_GB_BUILD="${MIN_FREE_GB_BUILD:-55}"
MIN_FREE_GB_EXPORT="${MIN_FREE_GB_EXPORT:-70}"
mkdir -p "$STORE_DIR" "$LOG" "$OUT"
export PYTHONPATH=.

SLOTS="A B D R C X"
START_AT="${START_AT:-A}"
case " $SLOTS " in
    *" $START_AT "*) ;;
    *) echo "!! START_AT=$START_AT is not one of: $SLOTS" >&2; exit 2 ;;
esac

due() {  # due <slot>: whether this invocation runs <slot>, i.e. it is START_AT or comes after it
    local s
    for s in $SLOTS; do
        [ "$s" = "$START_AT" ] && return 0
        [ "$s" = "$1" ] && return 1
    done
}

need_free() {  # need_free <GB> <what>: exit 3 if the disk holding the stores has less than <GB> GB free
    local free
    free=$(df -Pk "$STORE_DIR" | awk 'NR == 2 { print int($4 / 1048576) }')
    echo ">> $(date '+%F %T') disk: $free GB free before $2 (needs $1)"
    if [ "$free" -lt "$1" ]; then
        echo "!! $free GB free is less than the $1 GB that $2 needs; free space and resume with START_AT" >&2
        exit 3
    fi
}

# G spec...: the sorted distinct betas of its specs, each the double of a ratio of integers, so
# that the same beta is the same double in every block. A spec lo:hi:step:den is
# lo/den, (lo+step)/den, ..., hi/den; a spec num/den is that one value.
G() {
    ./venv/bin/python -c '
import sys
values = set()
for spec in sys.argv[1:]:
    if ":" in spec:
        lo, hi, step, den = map(int, spec.split(":"))
        values.update(i / den for i in range(lo, hi + 1, step))
    else:
        num, den = map(int, spec.split("/"))
        values.add(num / den)
print(" ".join(repr(v) for v in sorted(values)))' "$@"
}

# the beta grids of PLAN.md section 1: 2026.10.0's, with the fine segment 2.950-3.040 (was 2.980-3.040)
NS="50/100 75/100 90/100 100/100"                                       # non-surfing
W="10600:12000:25:10000 120:150:1:100"                                   # the transition window
TAIL="325/100 350/100 4/1 5/1 6/1 8/1 10/1 15/1 20/1"
BETA_DENSE=$(G $NS $W 150:300:2:100 2950:3040:5:1000 $TAIL)              # 191
BETA_SPARSE=$(G $NS 110:300:10:100 115/100 125/100 350/100 5/1 10/1 20/1) # 30
BETA_COARSE=$(G $NS 110:300:5:100 108:120:1:100 $TAIL)                   # 62
BETA_R="1.2 2.0 2.9"

# the fixed-law alpha rows and the one-loop A rows (PLAN.md section 1)
ALPHA_FIXED="0.01 0.1"
A_ALL="0.05 0.1 0.2 0.5 1 1.5 2 3 5 10"
A_R_LAB="0.05 1 10"
A_R_LADDER="0.1 1 10"
ALPHA_PLOT="0 0.01 0.1"
A_PLOT_R="0.05 0.1 1 10"

count() { echo $#; }
echo ">> $(date '+%F %T') $RUN on $(git rev-parse HEAD), starting at slot $START_AT, β grids: dense $(count $BETA_DENSE), sparse $(count $BETA_SPARSE), coarse $(count $BETA_COARSE)"

run() {  # run <store> <tier> <job> <purpose> <main.py arguments...>
    local db="$1" tier="$2" job="$3" purpose="$4"
    shift 4
    need_free "$MIN_FREE_GB_BUILD" "$job"
    echo ">> $(date '+%F %T') $job" | tee -a "$LOG/$job.log"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt "$tier" \
        --register "$job" --purpose "$purpose" -- --config exponential.yaml --database "$db" \
        --shards "$SHARDS" --ray-address "$RAY_ADDR" --job-name "$job" \
        --bbn-wall-clock-limit 600 --phi-init-Mp 5 "$@" 2>&1 | tee -a "$LOG/$job.log"
}

sidecar() {  # sidecar <store> <purpose>: the store's fingerprint sidecar, once, after its first block
    local db="$1" purpose="$2"
    [ -e "${db%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create \
        --purpose "$purpose" "$db"
}

# A   Figure 6: laboratory M, the dense beta grid, every A                      2292 histories
if due A; then
    run "$DB" A A-lab \
        "$RUN A: Figure 6 at laboratory M 4e-31 Mp, dense beta 0.5-20, one-loop A 0.05-10, fixed alpha 0.01 0.1, phi* 5" \
        --M-values-Mp 4e-31 --beta-values $BETA_DENSE --alpha-values $ALPHA_FIXED --A-values $A_ALL
    sidecar "$DB" "$RUN main store: Figure 6 at laboratory M, its trend in M, M_crit and large M (PLAN.md section 1)"
fi

# B   the trend in M at and below the laboratory bound                          2160 histories
if due B; then
    run "$DB" B B-lab-M-trend \
        "$RUN B: M 1e-30 to 1e-42 Mp on the sparse beta grid, one-loop A 0.05-10, fixed alpha 0.01 0.1, to show the trend in M" \
        --M-values-Mp 1e-30 1e-32 1e-33 1e-36 1e-40 1e-42 --beta-values $BETA_SPARSE \
        --alpha-values $ALPHA_FIXED --A-values $A_ALL
fi

# D   large M, natural mode only (alpha = 0)                                     186 histories
if due D; then
    run "$DB" D D-large-M-natural \
        "$RUN D: natural mode (alpha 0) at M 1e-3, 0.1, 0.5 Mp, coarse beta" \
        --M-values-Mp 1e-3 0.1 0.5 --beta-values $BETA_COARSE --alpha-values 0
fi

# R   the reflection store, without R-Mcrit (slot C)                              26 histories
if due R; then
    run "$DB_R" R R-lab \
        "$RUN R: reflection structure at laboratory M 4e-31 Mp, beta 1.2 2 2.9, one-loop A 0.05 1 10, fixed alpha 0.01 0.1" \
        --M-values-Mp 4e-31 --beta-values $BETA_R --alpha-values $ALPHA_FIXED --A-values $A_R_LAB
    sidecar "$DB_R" "$RUN reflection store: a few histories drawn whole (PLAN.md section 1)"
    run "$DB_R" R R-ladder \
        "$RUN R: the three drain regimes at 1e-16 Mp, beta 2, one-loop A 0.1 1 10, fixed alpha 0.01 0.1" \
        --M-values-Mp 1e-16 --beta-values 2.0 --alpha-values $ALPHA_FIXED --A-values $A_R_LADDER
    run "$DB_R" R R-natural \
        "$RUN R: reflection structure, natural mode (alpha 0), beta 1.2 2 2.9 at M 1e-3 and 0.5 Mp" \
        --M-values-Mp 1e-3 0.5 --beta-values $BETA_R --alpha-values 0
fi

# C   M_crit, last: C-Mcrit (main store, 180) and R-Mcrit (reflection store, 18)
if due C; then
    run "$DB" C C-Mcrit \
        "$RUN C: M 1e-15 and 1e-12 Mp around M_crit, sparse beta, one-loop A 1, fixed alpha 0.01 0.1" \
        --M-values-Mp 1e-15 1e-12 --beta-values $BETA_SPARSE --alpha-values $ALPHA_FIXED --A-values 1
    run "$DB_R" R R-Mcrit \
        "$RUN R: reflection structure at M_crit, 1e-15 and 1e-12 Mp, beta 1.2 2 2.9, one-loop A 1, fixed alpha 0.01 0.1" \
        --M-values-Mp 1e-15 1e-12 --beta-values $BETA_R --alpha-values $ALPHA_FIXED --A-values 1
fi

# the inventories: read-only, unregistered, no Ray
for STORE in "$DB" "$DB_R"; do
    NAME=$(basename "${STORE%.db}")
    ./venv/bin/python main.py --config exponential.yaml --database "$STORE" --shards "$SHARDS" \
        --inventory 2>&1 | tee "$LOG/inventory-$NAME.log"
done

need_free "$MIN_FREE_GB_EXPORT" "the exports"

# the exports: read-only, unregistered. Each store's root gets both exporters, since
# tools/quanta_measures.py reads <root>/histories.csv (plot_by_beta.py) and each one-loop history's
# fields.csv and bounces.csv (plot_ScalarHistory.py) from one root: 2026.10.0's run script gave each
# root one exporter, and its post-build script added the other (PLAN.md P3)
echo ">> $(date '+%F %T') plot_by_beta (main store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT --A-values $A_ALL \
    --output "$OUT/main" 2>&1 | tee "$LOG/plot_by_beta-main.log"
# the main store's per-history CSV files: the one-loop rows alone, with no figures, since the
# measures skip fixed-law histories and the figures of 3770 histories are not wanted
echo ">> $(date '+%F %T') plot_ScalarHistory --no-plots (main store, one-loop rows)"
./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database "$DB" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --no-alpha-values --A-values $A_ALL --no-plots \
    --output "$OUT/main" 2>&1 | tee "$LOG/plot_ScalarHistory-main.log"
echo ">> $(date '+%F %T') plot_ScalarHistory (reflection store)"
./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database "$DB_R" \
    --shards "$SHARDS" --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT \
    --A-values $A_PLOT_R --output "$OUT/reflections" 2>&1 | tee "$LOG/plot_ScalarHistory-reflections.log"
echo ">> $(date '+%F %T') plot_by_beta (reflection store)"
./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database "$DB_R" --shards "$SHARDS" \
    --ray-address "$RAY_ADDR" --phi-init-Mp 5 --alpha-values $ALPHA_PLOT --A-values $A_PLOT_R \
    --output "$OUT/reflections" 2>&1 | tee "$LOG/plot_by_beta-reflections.log"

# the derived measures (no store, no Ray) at c_a = 1, then 0 and 10 for the sensitivity. quanta_measures
# exits 1 when any history is refused or fails its K check; that is reported, and the rest still run
for ROOT in "$OUT/main/QCD_Cosmology" "$OUT/reflections/QCD_Cosmology"; do
    NAME=$(basename "$(dirname "$ROOT")")
    for SCALE in 1 0 10; do
        echo ">> $(date '+%F %T') quanta_measures $NAME --assoc-scale $SCALE"
        ./venv/bin/python -u tools/quanta_measures.py "$ROOT" --assoc-scale "$SCALE" 2>&1 \
            | tee "$LOG/quanta_measures-$NAME-assoc$SCALE.log" \
            || echo "!! quanta_measures $NAME --assoc-scale $SCALE exited non-zero"
    done
done

# the figure tools at c_a = 1
echo ">> $(date '+%F %T') plot_quanta_grid (main store)"
./venv/bin/python -u tools/plot_quanta_grid.py "$OUT/main/QCD_Cosmology" 2>&1 \
    | tee "$LOG/plot_quanta_grid-main.log" || echo "!! plot_quanta_grid exited non-zero"
echo ">> $(date '+%F %T') plot_quanta_history (reflection store)"
./venv/bin/python -u tools/plot_quanta_history.py "$OUT/reflections/QCD_Cosmology" 2>&1 \
    | tee "$LOG/plot_quanta_history-reflections.log" || echo "!! plot_quanta_history exited non-zero"

echo ">> $(date '+%F %T') $RUN done"
