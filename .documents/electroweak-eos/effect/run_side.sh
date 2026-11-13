#!/bin/sh
# electroweak-eos prompt 02 §1.4 (README §9.2 P14, P15): build one side of the sample, old or new, by six registered
# runs of main.py in sequence, through the side's own RunDrivers/registered_run.py, into
# var/datastores/electroweak-eos-02-<side>.db. It stops at the first run that fails.
#
# Run from the side's tree root (the old worktree at ec6bca8 for "old", the main checkout for "new"), with ChamPBH's
# isolated cluster up (tools/ray_cluster.sh start), detached, as one-loop-drain 06 §1.4 launched its runs:
#   cd <tree root> && nohup sh <main checkout>/.documents/electroweak-eos/effect/run_side.sh <side> \
#       > <main checkout>/var/electroweak-eos/02/runs/<side>.log 2>&1 & echo $! > .../runs/<side>.pid
# Each run's own output goes to <main checkout>/var/electroweak-eos/02/runs/<side>-<run>.log, with the load at its launch and the wrapper's exit status. The last line of this script's own
# output is "** run_side <side>: ended, exit <status>", which wait_for_line.py waits for.
#
# The betas are the run's own values, made as run_science_2026.10.0.sh makes them (repr(i / den)): the 13 of the dense
# grid's 2980:3040:5:1000, and 1.2 (120/100), 2.0 (200/100), 3.0 (300/100) and 5.0 (5/1).

side="$1"
case "$side" in
    old) want_label=2026.10.0 ;;
    new) want_label=2026.11.0 ;;
    *) echo "!! run_side: give old or new" >&2; exit 2 ;;
esac
if [ ! -f main.py ] || [ ! -f RunDrivers/registered_run.py ]; then
    echo "!! run_side: run from a ChamPBH tree root" >&2
    exit 2
fi
label=$(./venv/bin/python -c 'import config.version as v; print(v.VERSION_LABEL)')
if [ "$label" != "$want_label" ]; then
    echo "!! run_side: this tree is $label; side $side wants $want_label" >&2
    exit 2
fi

ADDRESS=127.0.0.1:6380
# the main checkout's root, from this script's own place in it: S and L are always the main checkout's (prompt §1)
MAIN=$(cd "$(dirname "$0")/../../.." && pwd -P)
S=$MAIN/var/datastores
L=$MAIN/var/electroweak-eos/02/runs
DB=$S/electroweak-eos-02-$side.db
HEAD=$(git rev-parse --short HEAD)
mkdir -p "$L"
export PYTHONPATH=.

EDGE=$(./venv/bin/python -c 'print(" ".join(repr(i / 1000) for i in range(2980, 3041, 5)))')

echo "** run_side $side: tree $PWD at $HEAD, VERSION_LABEL $label, store $DB"
echo "** run_side $side: a-edge betas $EDGE"
echo "** run_side $side: started $(date '+%F %T %z')"

# one <run> <grid summary> <main.py grid arguments...>
one() {
    run="$1"
    summary="$2"
    shift 2
    log=$L/$side-$run.log
    {
        echo "** run_side $side-$run: launch $(date '+%F %T %z')"
        echo "** load at launch: $(uptime)"
    } > "$log"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign electroweak-eos --prompt 02 \
        --register "$side-$run" \
        --purpose "electroweak-eos 02 Effect: $side side ($HEAD, $label), run $run: $summary, phi* 5" \
        -- --config exponential.yaml --database "$DB" --shards 16 \
        --job-name "electroweak-eos-02-$side-$run" --ray-address "$ADDRESS" --phi-init-Mp 5 \
        --bbn-wall-clock-limit 600 "$@" >> "$log" 2>&1
    status=$?
    echo "** run_side $side-$run: end $(date '+%F %T %z'), exit $status" >> "$log"
    echo "** run_side $side-$run: exit $status ($(date '+%F %T %z'))"
    return $status
}

rc=0
one a-edge "M 4e-31 Mp, the 13 beta of the dense grid's 2.980-3.040, one-loop A 1, no fixed alpha" \
    --M-values-Mp 4e-31 --beta-values $EDGE --A-values 1 --no-alpha-values || rc=$?
[ $rc -eq 0 ] && { one a-spread "M 4e-31 Mp, beta 1.2 2.0 5.0, fixed alpha 0.1, one-loop A 0.05 1 10" \
    --M-values-Mp 4e-31 --beta-values 1.2 2.0 5.0 --alpha-values 0.1 --A-values 0.05 1 10 || rc=$?; }
[ $rc -eq 0 ] && { one b "M 1e-30 1e-32 1e-33 1e-36 1e-40 1e-42 Mp, beta 2.0 3.0, one-loop A 1, no fixed alpha" \
    --M-values-Mp 1e-30 1e-32 1e-33 1e-36 1e-40 1e-42 --beta-values 2.0 3.0 --A-values 1 --no-alpha-values || rc=$?; }
[ $rc -eq 0 ] && { one c "M 1e-15 1e-12 Mp, beta 2.0, fixed alpha 0.1, one-loop A 1" \
    --M-values-Mp 1e-15 1e-12 --beta-values 2.0 --alpha-values 0.1 --A-values 1 || rc=$?; }
[ $rc -eq 0 ] && { one d "M 1e-3 0.1 0.5 Mp, beta 2.0 3.0, natural mode alpha 0" \
    --M-values-Mp 1e-3 0.1 0.5 --beta-values 2.0 3.0 --alpha-values 0 || rc=$?; }
[ $rc -eq 0 ] && { one r "M 1e-16 Mp, beta 2.0, one-loop A 1, no fixed alpha" \
    --M-values-Mp 1e-16 --beta-values 2.0 --A-values 1 --no-alpha-values || rc=$?; }

echo "** run_side $side: ended, exit $rc ($(date '+%F %T %z'))"
exit $rc
