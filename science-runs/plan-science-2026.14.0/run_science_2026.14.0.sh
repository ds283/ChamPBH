#!/usr/bin/env bash
# ChamPBH science run, VERSION_LABEL 2026.14.0: the plan is PLAN.md beside this file. It replaces the 2026.11.0 run
# (science-runs/2026-11-13-science-2026.11.0/) and covers both potentials: the exponential at n = 1 on 2026.11.0's grid
# (P1), and the inverse power law at n = 1 and 2 on the same grid less blocks C and D, R-natural and R-Mcrit (P2, and
# the user's rulings of PLAN.md section 0). It builds every block, then makes every export and derived measure the
# analyses read, one wall at a time (P10, P11). The script is 2026.11.0's with PLAN.md section 4's changes.
#
# Run on the Mac Studio (P5) from the ChamPBH repository root, on a committed tree (the script refuses a dirty one, so
# that every block's manifest records git_dirty false), with ChamPBH's Ray cluster already up and started from this
# same root (its workers take the cwd of the start, and read the w table and PRyMordial's rates by relative paths):
#   tools/ray_cluster.sh start "science-2026.14.0" --num-cpus 8
# and stop it afterwards with that script's stop (never `ray stop`). Launch detached:
#   mkdir -p var/science-2026.14.0
#   nohup bash science-runs/<folder>/run_science_2026.14.0.sh > var/science-2026.14.0/run.out 2>&1 < /dev/null & disown
# then check once that the first block started (./venv/bin/python -m runregistrykit list).
#
# Three stores under var/datastores/ (P3): the exponential's main store (A-exp, B-exp, D-exp, C-Mcrit), the power
# law's (A-pl, B-pl) and the reflection store (the R blocks of both potentials, and R-Mcrit). Every main.py block is
# registered through RunDrivers/registered_run.py as --campaign science-2026.14.0 --prompt <slot>, and every block can
# be restarted: main.py looks up each history before computing it, so re-running the script recomputes only what is
# missing. After a SIGKILL, end the stale run first (runregistrykit end RUN_ID --reason ..., CLAUDE.md rule 5).
#
# Slots run in the order A-exp A-pl B-exp B-pl D-exp R C-exp X (P13). Slot C-exp, last, holds C-Mcrit and R-Mcrit,
# whose histories of beta ~ 1.1-1.4 at M_crit may each tune for one to two hours. A pipeline of such histories prints
# nothing for longer than the registry's 900 s window and reads stale while it works: read it with
# `runregistrykit list --stale-after 4000`, or by its pid. The yield points to SecondaryGWKit (P7) are the slot
# boundaries after A-pl and after B-pl, and before X.
#
# START_AT=<slot> (one of SLOTS below; default A-exp) resumes the script at that slot after a run that ended early,
# skipping the slots before it; X skips every build slot and runs the inventories, exports and measures alone.
# EXPORT_WALLS (default "exp pl1 pl2") names the walls X exports, in that order, so that X can be resumed at a wall.
# Each block's log is appended to, so an earlier attempt's stays above the resume's:
#   START_AT=C-exp nohup bash science-runs/<folder>/run_science_2026.14.0.sh >> var/science-2026.14.0/run.out 2>&1 &
#
# The export section's test (P9): DB_EXP, DB_PL, DB_R, LOG, OUT, A_ALL and A_PLOT_R may be set in the environment, so
# that START_AT=X can be run against the probe's Part 1 stores before the launch, one wall at a time (EXPORT_WALLS).
# The run itself sets none of them.
#
# The disk (P11): Ray stops writing at 95% of the disk. A build slot is not started with less than MIN_FREE_GB_BUILD
# (default 55) GB free, nor a wall's exports with less than MIN_FREE_GB_EXPORT (default 70); the script then exits 3,
# and is resumed with START_AT (and EXPORT_WALLS) once space is freed. Once a main-store wall's measures and grid
# figure have run, its per-history fields.csv files are compressed with gzip, which no analysis reads afterwards.
set -euo pipefail

if [ ! -f main.py ] || [ ! -d RunDrivers ]; then
    echo "!! run from the ChamPBH repository root" >&2
    exit 2
fi
if [ -n "$(git status --porcelain)" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "!! the tree is dirty; commit first (or set ALLOW_DIRTY_TREE=1 to run anyway)" >&2
    exit 2
fi

RUN=science-2026.14.0
STORE_DIR=var/datastores
DB_EXP="${DB_EXP:-$STORE_DIR/$RUN-exponential.db}"
DB_PL="${DB_PL:-$STORE_DIR/$RUN-inverse-power.db}"
DB_R="${DB_R:-$STORE_DIR/$RUN-reflections.db}"
LOG="${LOG:-var/$RUN/logs}"
OUT="${OUT:-var/$RUN/out}"
SHARDS=16
RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"     # never auto, which the drivers refuse
MIN_FREE_GB_BUILD="${MIN_FREE_GB_BUILD:-55}"
MIN_FREE_GB_EXPORT="${MIN_FREE_GB_EXPORT:-70}"
EXPORT_WALLS="${EXPORT_WALLS:-exp pl1 pl2}"
mkdir -p "$STORE_DIR" "$LOG" "$OUT"
export PYTHONPATH=.

SLOTS="A-exp A-pl B-exp B-pl D-exp R C-exp X"
START_AT="${START_AT:-A-exp}"
case " $SLOTS " in
    *" $START_AT "*) ;;
    *) echo "!! START_AT=$START_AT is not one of: $SLOTS" >&2; exit 2 ;;
esac
for w in $EXPORT_WALLS; do
    case "$w" in
        exp|pl1|pl2) ;;
        *) echo "!! EXPORT_WALLS: $w is not one of: exp pl1 pl2" >&2; exit 2 ;;
    esac
done

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

# the beta grids of PLAN.md section 1: 2026.11.0's, unchanged
NS="50/100 75/100 90/100 100/100"                                       # non-surfing
W="10600:12000:25:10000 120:150:1:100"                                   # the transition window
TAIL="325/100 350/100 4/1 5/1 6/1 8/1 10/1 15/1 20/1"
BETA_DENSE=$(G $NS $W 150:300:2:100 2950:3040:5:1000 $TAIL)              # 191
BETA_SPARSE=$(G $NS 110:300:10:100 115/100 125/100 350/100 5/1 10/1 20/1) # 30
BETA_COARSE=$(G $NS 110:300:5:100 108:120:1:100 $TAIL)                   # 62
BETA_R="1.2 2.0 2.9"

# the masses, the fixed-law alpha rows and the one-loop A rows (PLAN.md section 1)
M_LAB="4e-31"
M_TREND="1e-30 1e-32 1e-33 1e-36 1e-40 1e-42"
ALPHA_FIXED="0.01 0.1"
A_ALL="${A_ALL:-0.05 0.1 0.2 0.5 1 1.5 2 3 5 10}"
A_R_LAB="0.05 1 10"
A_R_LADDER="0.1 1 10"
A_PLOT_R="${A_PLOT_R:-0.05 0.1 1 10}"

# the two potentials' main.py options: the config names the potential (models: [...]); n is the power law's grid
EXP="--config exponential.yaml"
PL="--config inverse_power.yaml --n-values 1 2"

count() { echo $#; }
echo ">> $(date '+%F %T') $RUN on $(git rev-parse HEAD), starting at slot $START_AT, β grids: dense $(count $BETA_DENSE), sparse $(count $BETA_SPARSE), coarse $(count $BETA_COARSE)"

run() {  # run <store> <slot> <job> <purpose> <main.py arguments, the potential's options first...>
    local db="$1" slot="$2" job="$3" purpose="$4"
    shift 4
    need_free "$MIN_FREE_GB_BUILD" "$job"
    echo ">> $(date '+%F %T') $job" | tee -a "$LOG/$job.log"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt "$slot" \
        --register "$job" --purpose "$purpose" -- --database "$db" \
        --shards "$SHARDS" --ray-address "$RAY_ADDR" --job-name "$job" \
        --bbn-wall-clock-limit 600 --phi-init-Mp 5 "$@" 2>&1 | tee -a "$LOG/$job.log"
}

sidecar() {  # sidecar <store> <purpose>: the store's fingerprint sidecar, once, after its first block
    local db="$1" purpose="$2"
    [ -e "${db%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create \
        --purpose "$purpose" "$db"
}

# A-exp   Figure 6 of the exponential: laboratory M, the dense beta grid, every A           2292 histories
if due A-exp; then
    run "$DB_EXP" A-exp A-exp-lab \
        "$RUN A-exp: exponential n 1, Figure 6 at laboratory M 4e-31 Mp, dense beta 0.5-20, one-loop A 0.05-10, fixed alpha 0.01 0.1, phi* 5" \
        $EXP --M-values-Mp $M_LAB --beta-values $BETA_DENSE --alpha-values $ALPHA_FIXED --A-values $A_ALL
    sidecar "$DB_EXP" "$RUN exponential store: Figure 6 at laboratory M, its trend in M, M_crit and large M (PLAN.md section 1)"
fi

# A-pl    Figure 6 of the power law, at n 1 and 2                                           4584 histories
if due A-pl; then
    run "$DB_PL" A-pl A-pl-lab \
        "$RUN A-pl: inverse power n 1 2, Figure 6 at laboratory M 4e-31 Mp, dense beta 0.5-20, one-loop A 0.05-10, fixed alpha 0.01 0.1, phi* 5" \
        $PL --M-values-Mp $M_LAB --beta-values $BETA_DENSE --alpha-values $ALPHA_FIXED --A-values $A_ALL
    sidecar "$DB_PL" "$RUN inverse-power store: Figure 6 at laboratory M and its trend in M, n 1 and 2 (PLAN.md section 1)"
fi

# B-exp   the exponential's trend in M; its one-loop rows use law v2 beyond its fit (P4)     2160 histories
if due B-exp; then
    run "$DB_EXP" B-exp B-exp-lab-M-trend \
        "$RUN B-exp: exponential n 1, M 1e-30 to 1e-42 Mp on the sparse beta grid, one-loop A 0.05-10 (law v2 extrapolated), fixed alpha 0.01 0.1" \
        $EXP --M-values-Mp $M_TREND --beta-values $BETA_SPARSE --alpha-values $ALPHA_FIXED --A-values $A_ALL
fi

# B-pl    the power law's trend in M, at n 1 and 2                                          4320 histories
if due B-pl; then
    run "$DB_PL" B-pl B-pl-lab-M-trend \
        "$RUN B-pl: inverse power n 1 2, M 1e-30 to 1e-42 Mp on the sparse beta grid, one-loop A 0.05-10, fixed alpha 0.01 0.1" \
        $PL --M-values-Mp $M_TREND --beta-values $BETA_SPARSE --alpha-values $ALPHA_FIXED --A-values $A_ALL
fi

# D-exp   large M, natural mode only (alpha = 0), the exponential alone                      186 histories
if due D-exp; then
    run "$DB_EXP" D-exp D-exp-large-M-natural \
        "$RUN D-exp: exponential n 1, natural mode (alpha 0) at M 1e-3, 0.1, 0.5 Mp, coarse beta" \
        $EXP --M-values-Mp 1e-3 0.1 0.5 --beta-values $BETA_COARSE --alpha-values 0
fi

# R       the reflection store: R-lab and R-ladder of both potentials, R-natural of the exponential   15+30, 5+10, 6
if due R; then
    run "$DB_R" R R-exp-lab \
        "$RUN R: exponential n 1, reflection structure at laboratory M 4e-31 Mp, beta 1.2 2 2.9, one-loop A 0.05 1 10, fixed alpha 0.01 0.1" \
        $EXP --M-values-Mp $M_LAB --beta-values $BETA_R --alpha-values $ALPHA_FIXED --A-values $A_R_LAB
    sidecar "$DB_R" "$RUN reflection store: a few histories of both potentials drawn whole (PLAN.md section 1)"
    run "$DB_R" R R-pl-lab \
        "$RUN R: inverse power n 1 2, reflection structure at laboratory M 4e-31 Mp, beta 1.2 2 2.9, one-loop A 0.05 1 10, fixed alpha 0.01 0.1" \
        $PL --M-values-Mp $M_LAB --beta-values $BETA_R --alpha-values $ALPHA_FIXED --A-values $A_R_LAB
    run "$DB_R" R R-exp-ladder \
        "$RUN R: exponential n 1, the three drain regimes at 1e-16 Mp, beta 2, one-loop A 0.1 1 10, fixed alpha 0.01 0.1" \
        $EXP --M-values-Mp 1e-16 --beta-values 2.0 --alpha-values $ALPHA_FIXED --A-values $A_R_LADDER
    run "$DB_R" R R-pl-ladder \
        "$RUN R: inverse power n 1 2, the drain regimes at 1e-16 Mp, beta 2, one-loop A 0.1 1 10, fixed alpha 0.01 0.1" \
        $PL --M-values-Mp 1e-16 --beta-values 2.0 --alpha-values $ALPHA_FIXED --A-values $A_R_LADDER
    run "$DB_R" R R-exp-natural \
        "$RUN R: exponential n 1, reflection structure, natural mode (alpha 0), beta 1.2 2 2.9 at M 1e-3 and 0.5 Mp" \
        $EXP --M-values-Mp 1e-3 0.5 --beta-values $BETA_R --alpha-values 0
fi

# C-exp   M_crit, last, the exponential alone: C-Mcrit (its store, 180) and R-Mcrit (reflection store, 18)
if due C-exp; then
    run "$DB_EXP" C-exp C-exp-Mcrit \
        "$RUN C-exp: exponential n 1, M 1e-15 and 1e-12 Mp around M_crit, sparse beta, one-loop A 1, fixed alpha 0.01 0.1" \
        $EXP --M-values-Mp 1e-15 1e-12 --beta-values $BETA_SPARSE --alpha-values $ALPHA_FIXED --A-values 1
    run "$DB_R" C-exp R-exp-Mcrit \
        "$RUN C-exp: exponential n 1, reflection structure at M_crit, 1e-15 and 1e-12 Mp, beta 1.2 2 2.9, one-loop A 1, fixed alpha 0.01 0.1" \
        $EXP --M-values-Mp 1e-15 1e-12 --beta-values $BETA_R --alpha-values $ALPHA_FIXED --A-values 1
fi

# X       the inventories, then the exports and measures, one wall at a time (P10, P11)
# the inventories: read-only, unregistered, no Ray; the reflection store under each potential
inventory() {  # inventory <store> <name> <potential's options...>
    local db="$1" name="$2"
    shift 2
    ./venv/bin/python main.py "$@" --database "$db" --shards "$SHARDS" --inventory 2>&1 | tee "$LOG/inventory-$name.log"
}
inventory "$DB_EXP" exponential $EXP
inventory "$DB_PL" inverse-power $PL
inventory "$DB_R" reflections-exponential $EXP
inventory "$DB_R" reflections-inverse-power $PL

# export_wall <wall> <main store> <alpha rows of the main store> <alpha rows of the reflection store> <options...>
# Each wall's root is <OUT>/<wall>/ and its potential's root <OUT>/<wall>/QCD_Cosmology/<model>/; the reflection
# store's is <OUT>/<wall>-reflections/. Both exporters write into one root, since tools/quanta_measures.py reads
# <root>/histories.csv (plot_by_beta.py) and each one-loop history's fields.csv and bounces.csv (plot_ScalarHistory.py).
export_wall() {
    local wall="$1" db="$2" alpha_main="$3" alpha_r="$4"
    shift 4
    local model
    case "$wall" in exp) model=exponential ;; *) model=inverse-power ;; esac
    local root="$OUT/$wall" root_r="$OUT/$wall-reflections"
    need_free "$MIN_FREE_GB_EXPORT" "the exports of wall $wall"

    echo ">> $(date '+%F %T') plot_by_beta ($wall)"
    ./venv/bin/python -u plot_by_beta.py "$@" --database "$db" --shards "$SHARDS" --ray-address "$RAY_ADDR" \
        --phi-init-Mp 5 --alpha-values $alpha_main --A-values $A_ALL --output "$root" \
        2>&1 | tee "$LOG/plot_by_beta-$wall.log"
    # the per-history CSV files of the one-loop rows alone, with no figures: the measures skip fixed-law histories
    echo ">> $(date '+%F %T') plot_ScalarHistory --no-plots ($wall, one-loop rows)"
    ./venv/bin/python -u plot_ScalarHistory.py "$@" --database "$db" --shards "$SHARDS" --ray-address "$RAY_ADDR" \
        --phi-init-Mp 5 --no-alpha-values --A-values $A_ALL --no-plots --output "$root" \
        2>&1 | tee "$LOG/plot_ScalarHistory-$wall.log"
    echo ">> $(date '+%F %T') plot_ScalarHistory ($wall-reflections)"
    ./venv/bin/python -u plot_ScalarHistory.py "$@" --database "$DB_R" --shards "$SHARDS" --ray-address "$RAY_ADDR" \
        --phi-init-Mp 5 --alpha-values $alpha_r --A-values $A_PLOT_R --output "$root_r" \
        2>&1 | tee "$LOG/plot_ScalarHistory-$wall-reflections.log"
    echo ">> $(date '+%F %T') plot_by_beta ($wall-reflections)"
    ./venv/bin/python -u plot_by_beta.py "$@" --database "$DB_R" --shards "$SHARDS" --ray-address "$RAY_ADDR" \
        --phi-init-Mp 5 --alpha-values $alpha_r --A-values $A_PLOT_R --output "$root_r" \
        2>&1 | tee "$LOG/plot_by_beta-$wall-reflections.log"

    # the derived measures (no store, no Ray) at c_a = 1, then 0 and 10 for the sensitivity. quanta_measures exits 1
    # when any history is refused or fails its K check; that is reported, and the rest still run
    local r name scale
    for r in "$root" "$root_r"; do
        name=$(basename "$r")
        for scale in 1 0 10; do
            echo ">> $(date '+%F %T') quanta_measures $name --assoc-scale $scale"
            ./venv/bin/python -u tools/quanta_measures.py "$r/QCD_Cosmology/$model" --assoc-scale "$scale" 2>&1 \
                | tee "$LOG/quanta_measures-$name-assoc$scale.log" \
                || echo "!! quanta_measures $name --assoc-scale $scale exited non-zero"
        done
    done

    # the figure tools at c_a = 1
    echo ">> $(date '+%F %T') plot_quanta_grid ($wall)"
    ./venv/bin/python -u tools/plot_quanta_grid.py "$root/QCD_Cosmology/$model" 2>&1 \
        | tee "$LOG/plot_quanta_grid-$wall.log" || echo "!! plot_quanta_grid $wall exited non-zero"
    echo ">> $(date '+%F %T') plot_quanta_history ($wall-reflections)"
    ./venv/bin/python -u tools/plot_quanta_history.py "$root_r/QCD_Cosmology/$model" 2>&1 \
        | tee "$LOG/plot_quanta_history-$wall-reflections.log" || echo "!! plot_quanta_history $wall exited non-zero"

    # P11: the main store's per-history fields.csv, which no analysis reads once the measures have run
    echo ">> $(date '+%F %T') gzip fields.csv ($wall)"
    find "$root" -name fields.csv -type f -exec gzip -f {} +
    echo ">> $(date '+%F %T') wall $wall done: $(du -sh "$root" | cut -f1) main, $(du -sh "$root_r" | cut -f1) reflections"
}

for w in $EXPORT_WALLS; do
    case "$w" in
        exp) export_wall exp "$DB_EXP" "0 $ALPHA_FIXED" "0 $ALPHA_FIXED" $EXP ;;
        pl1) export_wall pl1 "$DB_PL" "$ALPHA_FIXED" "$ALPHA_FIXED" --config inverse_power.yaml --n-values 1 ;;
        pl2) export_wall pl2 "$DB_PL" "$ALPHA_FIXED" "$ALPHA_FIXED" --config inverse_power.yaml --n-values 2 ;;
    esac
done

echo ">> $(date '+%F %T') $RUN done"
